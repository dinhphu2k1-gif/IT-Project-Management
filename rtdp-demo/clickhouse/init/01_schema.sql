-- ClickHouse: nạp CDC từ Kafka (định dạng JSON của GoldenGate Kafka Handler, mode=op).
-- Message mẫu:
--   {"table":"CORE.TXN","op_type":"I","op_ts":"2026-10-17 03:00:00.000000",
--    "current_ts":"2026-10-17 03:00:01.120000","pos":"00000000000000001234",
--    "after":{"TXN_ID":1,"ACCOUNT_ID":1001,...}}
-- Insert/Update lấy dữ liệu ở "after"; Delete lấy ở "before".
-- Bảng đích dùng ReplacingMergeTree(version, is_deleted): bản ghi mới nhất theo khoá chính thắng,
-- bản ghi bị xoá được loại khi đọc qua các view v_* (FINAL).

CREATE DATABASE IF NOT EXISTS rtdp;

-- ---------------------------------------------------------------------------
-- Hàm tiện ích
-- ---------------------------------------------------------------------------

-- Giá trị một khoá trong object JSON dưới dạng chuỗi (số hay chuỗi đều được, null -> '')
CREATE FUNCTION IF NOT EXISTS gg_val AS (obj, key) ->
    multiIf(JSONType(obj, key) = 'String', JSONExtractString(obj, key),
            JSONType(obj, key) = 'Null', '',
            JSONExtractRaw(obj, key));

-- Giá trị cột: ưu tiên ảnh "after", nếu không có (delete / update dạng compact) thì lấy "before"
CREATE FUNCTION IF NOT EXISTS gg_col AS (raw, key) ->
    if(JSONHas(raw, 'after', key),
       gg_val(JSONExtractRaw(raw, 'after'), key),
       gg_val(JSONExtractRaw(raw, 'before'), key));

-- Thời gian dạng '2026-10-17 03:00:00.123456' hoặc '2026-10-17T03:00:00.123456789' (UTC)
CREATE FUNCTION IF NOT EXISTS gg_ts AS (s) ->
    parseDateTime64BestEffortOrZero(substring(replaceOne(s, 'T', ' '), 1, 26), 6, 'UTC');

-- Phiên bản bản ghi: tăng dần theo thứ tự nhận (Kafka 1 partition giữ đúng thứ tự commit)
CREATE FUNCTION IF NOT EXISTS gg_version AS () ->
    toUInt64(toUnixTimestamp64Micro(now64(6))) * 1000 + rowNumberInBlock();

-- ---------------------------------------------------------------------------
-- Bảng đích
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS rtdp.branch
(
    branch_id    UInt32,
    branch_code  String,
    branch_name  String,
    city         String,
    version      UInt64,
    is_deleted   UInt8,
    ingested_at  DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(version, is_deleted)
ORDER BY branch_id;

CREATE TABLE IF NOT EXISTS rtdp.customer
(
    customer_id  UInt64,
    full_name    String,
    segment      LowCardinality(String),
    branch_id    UInt32,
    created_at   DateTime64(6, 'UTC'),
    version      UInt64,
    is_deleted   UInt8,
    ingested_at  DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(version, is_deleted)
ORDER BY customer_id;

CREATE TABLE IF NOT EXISTS rtdp.account
(
    account_id   UInt64,
    account_no   String,
    customer_id  UInt64,
    branch_id    UInt32,
    currency     LowCardinality(String),
    balance      Decimal(18, 2),
    status       LowCardinality(String),
    updated_at   DateTime64(6, 'UTC'),
    version      UInt64,
    is_deleted   UInt8,
    ingested_at  DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(version, is_deleted)
ORDER BY account_id;

CREATE TABLE IF NOT EXISTS rtdp.txn
(
    txn_id       UInt64,
    account_id   UInt64,
    branch_id    UInt32,
    channel      LowCardinality(String),
    txn_type     LowCardinality(String),
    amount       Decimal(18, 2),
    currency     LowCardinality(String),
    created_at   DateTime64(6, 'UTC'),
    description  String,
    op_ts        DateTime64(6, 'UTC'),
    version      UInt64,
    is_deleted   UInt8,
    ingested_at  DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(version, is_deleted)
ORDER BY txn_id;

-- Nhật ký mọi message CDC (để demo "đi theo dòng dữ liệu" và gỡ lỗi), giữ 3 ngày
CREATE TABLE IF NOT EXISTS rtdp.cdc_events
(
    topic        LowCardinality(String),
    kafka_offset UInt64,
    source_table LowCardinality(String),
    op_type      LowCardinality(String),
    op_ts        DateTime64(6, 'UTC'),
    pos          String,
    raw          String,
    ingested_at  DateTime64(3, 'UTC')
)
ENGINE = MergeTree
ORDER BY ingested_at
TTL toDateTime(ingested_at) + INTERVAL 3 DAY;

-- ---------------------------------------------------------------------------
-- Hàng đợi Kafka (mỗi topic một bảng). kafka_flush_interval_ms=1000 để độ trễ thấp
-- (mặc định ~7,5 giây).
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS rtdp.q_branch (raw String)
ENGINE = Kafka
SETTINGS kafka_broker_list = 'kafka:29092', kafka_topic_list = 'CORE.BRANCH',
         kafka_group_name = 'clickhouse_rtdp_branch', kafka_format = 'JSONAsString',
         kafka_flush_interval_ms = 1000, kafka_skip_broken_messages = 1000;

CREATE TABLE IF NOT EXISTS rtdp.q_customer (raw String)
ENGINE = Kafka
SETTINGS kafka_broker_list = 'kafka:29092', kafka_topic_list = 'CORE.CUSTOMER',
         kafka_group_name = 'clickhouse_rtdp_customer', kafka_format = 'JSONAsString',
         kafka_flush_interval_ms = 1000, kafka_skip_broken_messages = 1000;

CREATE TABLE IF NOT EXISTS rtdp.q_account (raw String)
ENGINE = Kafka
SETTINGS kafka_broker_list = 'kafka:29092', kafka_topic_list = 'CORE.ACCOUNT',
         kafka_group_name = 'clickhouse_rtdp_account', kafka_format = 'JSONAsString',
         kafka_flush_interval_ms = 1000, kafka_skip_broken_messages = 1000;

CREATE TABLE IF NOT EXISTS rtdp.q_txn (raw String)
ENGINE = Kafka
SETTINGS kafka_broker_list = 'kafka:29092', kafka_topic_list = 'CORE.TXN',
         kafka_group_name = 'clickhouse_rtdp_txn', kafka_format = 'JSONAsString',
         kafka_flush_interval_ms = 1000, kafka_skip_broken_messages = 1000;

-- ---------------------------------------------------------------------------
-- Materialized view: parse JSON -> bảng đích
-- ---------------------------------------------------------------------------

CREATE MATERIALIZED VIEW IF NOT EXISTS rtdp.mv_branch TO rtdp.branch AS
SELECT
    toUInt32OrZero(gg_col(raw, 'BRANCH_ID'))  AS branch_id,
    gg_col(raw, 'BRANCH_CODE')                AS branch_code,
    gg_col(raw, 'BRANCH_NAME')                AS branch_name,
    gg_col(raw, 'CITY')                       AS city,
    gg_version()                              AS version,
    JSONExtractString(raw, 'op_type') = 'D'   AS is_deleted,
    now64(3)                                  AS ingested_at
FROM rtdp.q_branch;

CREATE MATERIALIZED VIEW IF NOT EXISTS rtdp.mv_customer TO rtdp.customer AS
SELECT
    toUInt64OrZero(gg_col(raw, 'CUSTOMER_ID')) AS customer_id,
    gg_col(raw, 'FULL_NAME')                   AS full_name,
    gg_col(raw, 'SEGMENT')                     AS segment,
    toUInt32OrZero(gg_col(raw, 'BRANCH_ID'))   AS branch_id,
    gg_ts(gg_col(raw, 'CREATED_AT'))           AS created_at,
    gg_version()                               AS version,
    JSONExtractString(raw, 'op_type') = 'D'    AS is_deleted,
    now64(3)                                   AS ingested_at
FROM rtdp.q_customer;

CREATE MATERIALIZED VIEW IF NOT EXISTS rtdp.mv_account TO rtdp.account AS
SELECT
    toUInt64OrZero(gg_col(raw, 'ACCOUNT_ID'))  AS account_id,
    gg_col(raw, 'ACCOUNT_NO')                  AS account_no,
    toUInt64OrZero(gg_col(raw, 'CUSTOMER_ID')) AS customer_id,
    toUInt32OrZero(gg_col(raw, 'BRANCH_ID'))   AS branch_id,
    trim(gg_col(raw, 'CURRENCY'))              AS currency,
    toDecimal64OrZero(gg_col(raw, 'BALANCE'), 2) AS balance,
    gg_col(raw, 'STATUS')                      AS status,
    gg_ts(gg_col(raw, 'UPDATED_AT'))           AS updated_at,
    gg_version()                               AS version,
    JSONExtractString(raw, 'op_type') = 'D'    AS is_deleted,
    now64(3)                                   AS ingested_at
FROM rtdp.q_account;

CREATE MATERIALIZED VIEW IF NOT EXISTS rtdp.mv_txn TO rtdp.txn AS
SELECT
    toUInt64OrZero(gg_col(raw, 'TXN_ID'))      AS txn_id,
    toUInt64OrZero(gg_col(raw, 'ACCOUNT_ID'))  AS account_id,
    toUInt32OrZero(gg_col(raw, 'BRANCH_ID'))   AS branch_id,
    gg_col(raw, 'CHANNEL')                     AS channel,
    gg_col(raw, 'TXN_TYPE')                    AS txn_type,
    toDecimal64OrZero(gg_col(raw, 'AMOUNT'), 2) AS amount,
    trim(gg_col(raw, 'CURRENCY'))              AS currency,
    gg_ts(gg_col(raw, 'CREATED_AT'))           AS created_at,
    gg_col(raw, 'DESCRIPTION')                 AS description,
    gg_ts(JSONExtractString(raw, 'op_ts'))     AS op_ts,
    gg_version()                               AS version,
    JSONExtractString(raw, 'op_type') = 'D'    AS is_deleted,
    now64(3)                                   AS ingested_at
FROM rtdp.q_txn;

-- Nhật ký CDC cho cả 4 topic
CREATE MATERIALIZED VIEW IF NOT EXISTS rtdp.mv_events_branch TO rtdp.cdc_events AS
SELECT _topic AS topic, _offset AS kafka_offset, JSONExtractString(raw, 'table') AS source_table,
       JSONExtractString(raw, 'op_type') AS op_type, gg_ts(JSONExtractString(raw, 'op_ts')) AS op_ts,
       JSONExtractString(raw, 'pos') AS pos, raw, now64(3) AS ingested_at
FROM rtdp.q_branch;

CREATE MATERIALIZED VIEW IF NOT EXISTS rtdp.mv_events_customer TO rtdp.cdc_events AS
SELECT _topic AS topic, _offset AS kafka_offset, JSONExtractString(raw, 'table') AS source_table,
       JSONExtractString(raw, 'op_type') AS op_type, gg_ts(JSONExtractString(raw, 'op_ts')) AS op_ts,
       JSONExtractString(raw, 'pos') AS pos, raw, now64(3) AS ingested_at
FROM rtdp.q_customer;

CREATE MATERIALIZED VIEW IF NOT EXISTS rtdp.mv_events_account TO rtdp.cdc_events AS
SELECT _topic AS topic, _offset AS kafka_offset, JSONExtractString(raw, 'table') AS source_table,
       JSONExtractString(raw, 'op_type') AS op_type, gg_ts(JSONExtractString(raw, 'op_ts')) AS op_ts,
       JSONExtractString(raw, 'pos') AS pos, raw, now64(3) AS ingested_at
FROM rtdp.q_account;

CREATE MATERIALIZED VIEW IF NOT EXISTS rtdp.mv_events_txn TO rtdp.cdc_events AS
SELECT _topic AS topic, _offset AS kafka_offset, JSONExtractString(raw, 'table') AS source_table,
       JSONExtractString(raw, 'op_type') AS op_type, gg_ts(JSONExtractString(raw, 'op_ts')) AS op_ts,
       JSONExtractString(raw, 'pos') AS pos, raw, now64(3) AS ingested_at
FROM rtdp.q_txn;

-- ---------------------------------------------------------------------------
-- Ngưỡng cảnh báo (Grafana đọc mỗi lần đánh giá luật). Đổi bằng scripts/set-threshold.sh
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS rtdp.alert_config
(
    name        String,
    value       Float64,
    updated_at  DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY name;

INSERT INTO rtdp.alert_config (name, value) VALUES
    ('withdraw_count', 5),
    ('withdraw_window_min', 5),
    ('large_amount', 1000000000);

-- ---------------------------------------------------------------------------
-- View đọc trạng thái hiện tại (đã áp dụng update/delete). Dashboard và đối soát dùng các view này.
-- ---------------------------------------------------------------------------

CREATE VIEW IF NOT EXISTS rtdp.v_branch AS
SELECT branch_id, branch_code, branch_name, city, ingested_at
FROM rtdp.branch FINAL WHERE is_deleted = 0;

CREATE VIEW IF NOT EXISTS rtdp.v_customer AS
SELECT customer_id, full_name, segment, branch_id, created_at, ingested_at
FROM rtdp.customer FINAL WHERE is_deleted = 0;

CREATE VIEW IF NOT EXISTS rtdp.v_account AS
SELECT account_id, account_no, customer_id, branch_id, currency, balance, status, updated_at, ingested_at
FROM rtdp.account FINAL WHERE is_deleted = 0;

CREATE VIEW IF NOT EXISTS rtdp.v_txn AS
SELECT txn_id, account_id, branch_id, channel, txn_type, amount, currency, created_at, description,
       op_ts, ingested_at,
       (toUnixTimestamp64Milli(ingested_at) - toUnixTimestamp64Milli(created_at)) / 1000 AS latency_sec
FROM rtdp.txn FINAL WHERE is_deleted = 0;
