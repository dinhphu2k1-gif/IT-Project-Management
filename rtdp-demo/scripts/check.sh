#!/usr/bin/env bash
# Kiểm tra nhanh từng chặng của luồng dữ liệu: Oracle -> Kafka -> ClickHouse -> Grafana.
set -uo pipefail
cd "$(dirname "$0")/.."
[ -f .env ] && set -a && . ./.env && set +a
CH_USER="${CLICKHOUSE_USER:-rtdp}"; CH_PASS="${CLICKHOUSE_PASSWORD:-rtdp}"

section() { printf '\n== %s\n' "$1"; }

section "Container"
docker compose ps --format 'table {{.Name}}\t{{.Status}}'

section "Oracle (chế độ log, supplemental log, số dòng CORE)"
docker exec -i rtdp-oracle sqlplus -s / as sysdba <<'SQL'
SET HEADING ON FEEDBACK OFF PAGESIZE 50 LINESIZE 120
SELECT log_mode, supplemental_log_data_min AS supp_log FROM v$database;
ALTER SESSION SET CONTAINER = FREEPDB1;
SELECT (SELECT COUNT(*) FROM core.branch) branch, (SELECT COUNT(*) FROM core.customer) customer,
       (SELECT COUNT(*) FROM core.account) account, (SELECT COUNT(*) FROM core.txn) txn FROM dual;
SQL

section "Kafka (số message mỗi topic)"
docker exec rtdp-kafka /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server kafka:29092 \
  --topic-partitions 'CORE.BRANCH:0,CORE.CUSTOMER:0,CORE.ACCOUNT:0,CORE.TXN:0'

section "ClickHouse (consumer Kafka và số dòng)"
docker exec rtdp-clickhouse clickhouse-client --user "$CH_USER" --password "$CH_PASS" --multiquery -q "
SELECT table AS consumer, num_messages_read AS da_doc, last_poll_time AS lan_doc_cuoi,
       if(length(exceptions.text) = 0, '', concat(toString(exceptions.time[-1]), ' ', substring(exceptions.text[-1], 1, 50))) AS loi_gan_nhat
FROM system.kafka_consumers WHERE database = 'rtdp' ORDER BY table FORMAT PrettyCompact;
SELECT (SELECT count() FROM rtdp.v_branch) AS branch, (SELECT count() FROM rtdp.v_customer) AS customer,
       (SELECT count() FROM rtdp.v_account) AS account, (SELECT count() FROM rtdp.v_txn) AS txn,
       (SELECT max(ingested_at) FROM rtdp.cdc_events) AS message_moi_nhat
FORMAT PrettyCompact"

section "Grafana"
curl -s http://localhost:3000/api/health && echo
echo "Dashboard: http://localhost:3000/d/rtdp-realtime"
