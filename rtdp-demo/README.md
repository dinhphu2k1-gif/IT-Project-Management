# Demo RTDP: giao dịch ngân hàng thời gian thực

Một giao dịch ghi vào core banking (Oracle) xuất hiện trên dashboard Grafana sau khoảng 1–2 giây, thay vì T+1.

```
 generator.py ──► Oracle 23ai Free ──► GoldenGate Extract ──► GoldenGate DAA ──► Kafka ──► ClickHouse ──► Grafana
 (sinh giao dịch)  schema CORE          (Integrated, trail ct)  (Kafka Handler)   CORE.*    Kafka engine   dashboard
                   ARCHIVELOG                                    JSON, mode=op     topic     + MV           + cảnh báo
```

Oracle, Kafka, ClickHouse và Grafana chạy bằng Docker Compose. GoldenGate chạy bằng hai container build từ
bộ cài của bạn (mục 3.0), hoặc dùng bản cài sẵn với cấu hình mẫu ở thư mục `goldengate/` (mục 3.1, 3.2).

![Dashboard RTDP](docs/dashboard.png)

## 1. Yêu cầu máy

- Ubuntu 22.04/24.04, RAM 16 GB, ổ trống ≥ 40 GB
- Docker Engine + Docker Compose v2, Python 3.10+
- Lần chạy đầu cần Internet để tải image (~5 GB) và plugin ClickHouse cho Grafana

## 2. Khởi động hạ tầng

```bash
cd rtdp-demo
cp .env.example .env          # tuỳ chọn: đổi mật khẩu, địa chỉ
docker compose up -d
docker compose ps             # đợi rtdp-oracle chuyển sang (healthy), thường 1–2 phút
scripts/check.sh              # kiểm tra từng chặng
```

Khi Oracle khởi tạo lần đầu, script trong `oracle/setup/` tự chạy:

- bật ARCHIVELOG, FORCE LOGGING, supplemental log, `ENABLE_GOLDENGATE_REPLICATION=TRUE`, `streams_pool_size=256M`;
- tạo user GoldenGate `ggadmin` (trong PDB) và `c##ggadmin` (trong CDB), cấp quyền `OGG_CAPTURE`;
- tạo schema `CORE` gồm 4 bảng `BRANCH`, `CUSTOMER`, `ACCOUNT`, `TXN`.

| Thành phần | Địa chỉ | Tài khoản mặc định |
| --- | --- | --- |
| Oracle (PDB) | `localhost:1521/FREEPDB1` | `core/core`, `ggadmin/ggadmin`, CDB: `c##ggadmin/ggadmin` (service `FREE`) |
| Kafka | `localhost:9092` (từ máy host), `kafka:29092` (trong mạng docker) | — |
| ClickHouse | http://localhost:8123, native `localhost:9000` | `rtdp/rtdp` |
| Grafana | http://localhost:3000 | `admin/admin` (người xem ẩn danh được xem dashboard) |
| GoldenGate for Oracle (Service Manager) | http://localhost:9011 (Admin Service: 9012) | `oggadmin/Oggadmin_123` (đổi trong `.env`) |
| GoldenGate DAA (Service Manager) | http://localhost:9021 (Admin Service: 9022) | như trên |

### Truy cập từng thành phần

Chạy trong thư mục `rtdp-demo`. Nếu đã đổi mật khẩu trong `.env` thì thay giá trị tương ứng.

**Grafana**: mở http://localhost:3000/d/rtdp-realtime. Đăng nhập `admin/admin` để sửa dashboard và xem luật cảnh báo (Alerting → Alert rules).

**Oracle**: dùng `sqlplus` có sẵn trong container.

```bash
docker exec -it rtdp-oracle sqlplus core/core@localhost:1521/FREEPDB1      # dữ liệu core banking (schema CORE)
docker exec -it rtdp-oracle sqlplus ggadmin/ggadmin@localhost:1521/FREEPDB1 # user của GoldenGate
docker exec -it rtdp-oracle sqlplus / as sysdba                             # quản trị (gõ ALTER SESSION SET CONTAINER = FREEPDB1;)
```

Từ máy host có thể dùng client bất kỳ với `localhost:1521/FREEPDB1` (SQL Developer, DBeaver…).

**Kafka**: xem topic và đọc message.

```bash
docker exec rtdp-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:29092 --list
docker exec rtdp-kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:29092 \
  --topic CORE.TXN --partition 0 --offset latest --max-messages 1     # message mới nhất, định dạng như mục 3.2
```

**ClickHouse**: bảng `rtdp.*`, nên đọc qua các view `v_*` (đã áp dụng update/delete).

```bash
docker exec -it rtdp-clickhouse clickhouse-client --user rtdp --password rtdp
curl -s -u rtdp:rtdp 'http://localhost:8123/?query=SELECT+count()+FROM+rtdp.v_txn'      # qua HTTP
```

**GoldenGate** (mục 3.0). Có ba cách:

1. *Giao diện web*: http://localhost:9011 (Oracle) và http://localhost:9021 (DAA), tài khoản `oggadmin`.
   Vào Service Manager, chọn Administration Service để xem Extract `EXTCORE` và Replicat `RKAFKA`: trạng thái, độ trễ,
   thống kê, tham số, báo cáo.
2. *Dòng lệnh `adminclient`* trong container:

   ```bash
   docker exec -it rtdp-ogg-oracle adminclient
   OGG> CONNECT http://ogg-oracle:9011 DEPLOYMENT ORA_DEPLOY AS oggadmin PASSWORD Oggadmin_123
   OGG> INFO EXTRACT EXTCORE, DETAIL
   OGG> STATS EXTRACT EXTCORE, TOTAL

   docker exec -it rtdp-ogg-daa adminclient
   OGG> CONNECT http://ogg-daa:9021 DEPLOYMENT DAA_DEPLOY AS oggadmin PASSWORD Oggadmin_123
   OGG> INFO REPLICAT RKAFKA, DETAIL
   OGG> STATS REPLICAT RKAFKA, TOTAL
   ```

3. *REST API* (cổng 9012 cho Extract, 9022 cho Replicat):

   ```bash
   curl -s -u oggadmin:Oggadmin_123 http://localhost:9012/services/v2/extracts/EXTCORE
   curl -s -u oggadmin:Oggadmin_123 http://localhost:9022/services/v2/replicats/RKAFKA
   ```

Nhật ký GoldenGate:

```bash
docker exec rtdp-ogg-oracle tail -50 /u01/app/oracle/deployments/ORA_DEPLOY/var/log/ggserr.log
docker exec rtdp-ogg-daa    tail -50 /u01/app/oracle/deployments/DAA_DEPLOY/var/log/ggserr.log
docker exec rtdp-ogg-daa    ls /u01/app/oracle/deployments/DAA_DEPLOY/var/lib/report/   # báo cáo RKAFKA.rpt
```

## 3. Cấu hình GoldenGate

**3.0. Chạy bằng Docker (khuyến nghị)**

Cần hai bộ cài Oracle (tải từ Oracle, không nằm trong repo), đặt chung một thư mục, mặc định `~/Documents/goldengate`
(đổi bằng `OGG_INSTALLER_DIR` trong `.env`):

- `V1054774-01.zip`: GoldenGate 26ai for Oracle (Microservices), chạy Extract;
- `V1054834-01.zip`: GoldenGate for Big Data (DAA), chạy Replicat Kafka.

```bash
docker compose -f docker-compose.yml -f docker-compose.goldengate.yml up -d --build   # lần đầu build image ~10 phút
scripts/setup-goldengate.sh        # tạo Extract EXTCORE, Replicat RKAFKA, bật Auto Restart (chạy lại được)
```

Hai container `rtdp-ogg-oracle` và `rtdp-ogg-daa` dùng chung volume trail `ogg-trail`. Giao diện web ở cổng 9011 và 9021.
Nếu đang chạy `scripts/gg_simulator.py` thì tắt đi, để mỗi topic chỉ có một nguồn.
Mỗi container cần khoảng 1–1,5 GB RAM, nên máy 16 GB sẽ khá chật khi chạy cùng stack chính.

Sau khi xoá volume GoldenGate (`down -v`), `setup-goldengate.sh` tự gỡ đăng ký Extract cũ còn sót trong Oracle.

Các mục 3.1 và 3.2 dưới đây dành cho trường hợp dùng bản GoldenGate cài sẵn, không dùng Docker.

Các file đều có chỗ `<...>` cần thay theo bản cài (cổng, tên deployment, đường dẫn).

**3.1. Extract (GoldenGate for Oracle):** thư mục `goldengate/extract/`

1. Chép `EXTCORE.prm` vào thư mục tham số của deployment (`$OGG_ETC_HOME/conf/ogg/`).
2. Chạy `adminclient` rồi gõ `OBEY .../setup-extract.oby`. Script này tạo credential, chạy `ADD SCHEMATRANDATA CORE ALLCOLS`,
   đăng ký và khởi động Integrated Extract `EXTCORE` ghi ra trail `ct`.
3. Kiểm tra: `python generator/generator.py once`, sau đó `STATS EXTRACT EXTCORE` phải tăng.

**3.2. Replicat Kafka (GoldenGate DAA):** thư mục `goldengate/daa/`

1. Tải thư viện Kafka client: `$OGG_HOME/opt/DependencyDownloader/kafka.sh 3.8.1`.
2. Chép `RKAFKA.prm`, `kafka.props`, `custom_kafka_producer.properties` vào `$OGG_ETC_HOME/conf/ogg/` của deployment DAA,
   sửa `gg.classpath` và đường dẫn `property=` cho đúng.
3. Đưa trail `ct` sang DAA: dùng Distribution Path, hoặc trỏ đường dẫn tuyệt đối nếu hai bản cài cùng máy (xem chú thích trong `setup-daa.oby`).
4. `OBEY .../setup-daa.oby` để tạo và khởi động Replicat `RKAFKA`.
5. Bật **Auto Restart** cho Extract và Replicat (giao diện web: Managed Options; bản Classic: `AUTORESTART ER *, RETRIES 10, WAITMINUTES 1` trong `mgr.prm`).
   Cần bước này để ca TC05 (tắt/bật Kafka) tự phục hồi.
6. Kiểm tra: `scripts/check.sh`, số message ở các topic `CORE.*` phải tăng khi có giao dịch.

Định dạng message mà ClickHouse chờ đọc (Kafka Handler mặc định, `format=json`, `mode=op`):

```json
{"table":"CORE.TXN","op_type":"I","op_ts":"2026-10-17 03:00:00.000000","current_ts":"2026-10-17 03:00:01.215000","pos":"00000000000000238653",
 "after":{"TXN_ID":1,"ACCOUNT_ID":1000001,"AMOUNT":2500000.00,"CREATED_AT":"2026-10-17 03:00:00.120000000", ...}}
```

GoldenGate 26ai mặc định chỉ xuất `before`/`after`. Thông tin `table`, `op_type`, `pos`... có được nhờ dòng
`format.metaColumnsTemplate` trong `kafka.props`. Thiếu dòng này thì ClickHouse không nhận ra lệnh DELETE.
Trail của Per-PDB Extract mang tên 2 phần (`CORE.TXN`), nên `RKAFKA.prm` dùng `MAP CORE.*, TARGET CORE.*`.

> **Chưa có GoldenGate mà muốn thử phần sau Kafka?** Dùng script giả lập (chỉ để thử, demo thật thì tắt đi):
> `pip install confluent-kafka && python scripts/gg_simulator.py`. Script đọc Oracle định kỳ và gửi message
> cùng định dạng vào Kafka. Script không bắt được lệnh DELETE; ca TC03 thì dùng `--delete-txn <id>`.

## 4. Sinh dữ liệu

```bash
python3 -m venv .venv && . .venv/bin/activate
pip install -r generator/requirements.txt
set -a; . ./.env; set +a       # nếu có file .env

python generator/generator.py seed               # 10 chi nhánh, 200 khách hàng, 300 tài khoản
python generator/generator.py run --rate 3       # ~3 giao dịch/giây, Ctrl+C để dừng
```

**Chạy `seed` sau khi Extract đã chạy**, để dữ liệu gốc cũng đi qua GoldenGate. Nếu lỡ seed trước,
chạy `python generator/generator.py resync` để gửi lại toàn bộ.

Các lệnh khác: `once`, `anomaly`, `update`, `delete`, `stats`; xem `python generator/generator.py <lệnh> --help`.

Dashboard: http://localhost:3000/d/rtdp-realtime (tự làm mới mỗi 5 giây).

## 5. Kịch bản demo 10 phút

Trước giờ demo 30 phút: `docker compose up -d`, khởi động Extract/Replicat, `scripts/check.sh`,
chạy `generator.py run --rate 3` ở một terminal để dashboard đã có số liệu.

| Phút | Việc làm | Lệnh / màn hình |
| --- | --- | --- |
| 0–2 | Bài toán T+1 và kiến trúc | Sơ đồ ở đầu README |
| 2–4 | Một giao dịch thủ công | `python generator/generator.py once --amount 50000000 --wait`: in ra số giây đến ClickHouse; giao dịch hiện ở bảng "Giao dịch mới nhất" |
| 4–6 | Đi theo dòng dữ liệu | `STATS EXTRACT EXTCORE` / `STATS REPLICAT RKAFKA` → `scripts/check.sh` (offset Kafka) → panel "Dòng dữ liệu CDC mới nhất" |
| 6–8 | Giao dịch bất thường | `python generator/generator.py anomaly`: panel "Cảnh báo" bật đỏ trong ~10–30 giây |
| 8–10 | Đối soát, hỏi đáp | Dừng `run`, chạy `python generator/reconcile.py`: Oracle và ClickHouse khớp 100%; panel "Độ trễ end-to-end" |

Phương án dự phòng: quay sẵn video demo hoàn chỉnh.

## 6. Ca kiểm thử

| Mã | Cách chạy | Tiêu chí đạt |
| --- | --- | --- |
| TC01 | `generator.py once --wait` | In "Đã có ở ClickHouse sau x giây", x < 10 |
| TC02 | `generator.py update --account-id 1000001 --balance 123456789` | `SELECT balance FROM rtdp.v_account WHERE account_id=1000001` ra giá trị mới |
| TC03 | `generator.py delete` (xoá giao dịch mới nhất) | Message `op_type = D` trong `rtdp.cdc_events`; giao dịch biến mất khỏi `rtdp.v_txn` |
| TC04 | Dừng `run`, chạy `reconcile.py` | "ĐẠT - Oracle và ClickHouse khớp 100%" |
| TC05 | Đang `run`: `docker stop rtdp-kafka`, đợi 20 giây, `docker start rtdp-kafka`, rồi `reconcile.py` | Khớp 100% (Replicat phải bật Auto Restart) |
| TC06 | `generator.py anomaly` | 2 cảnh báo "Rút tiền liên tục", "Giao dịch giá trị lớn" bật trong vòng 1 phút |
| TC07 | `generator.py run --rate 5 --duration 1800`, song song `docker stats` | RAM toàn máy < 13 GB, không container nào bị kill |

Kết quả chạy thử (không có GoldenGate, dùng `gg_simulator.py`): TC01 khoảng 0,6–1,3 giây; TC02–TC06 đạt.
Lúc đó Oracle dùng 2,35 GB, ClickHouse 0,42 GB, Kafka 0,41 GB, Grafana 85 MB.

### Kiểm thử tự động

```bash
pip install -r requirements-dev.txt
python -m pyflakes generator scripts tests
python -m pytest -m "not e2e"                 # unit test, không cần CSDL
RTDP_E2E=1 python -m pytest -m e2e            # end-to-end, cần hệ thống đang chạy và có luồng CDC
```

CI trên GitHub Actions (`.github/workflows/rtdp-ci.yml`) chạy kiểm tra tĩnh, unit test, rồi dựng Oracle,
Kafka và ClickHouse bằng Docker Compose để chạy kiểm thử end-to-end với bộ giả lập GoldenGate.

Với GoldenGate thật (container ở mục 3.0), TC01–TC06 đã chạy đạt: TC01 khoảng 4–5 giây (cao hơn bộ giả lập vì
Integrated Extract), TC03 nhận message `D` thật, TC05 Replicat không bị lỗi khi Kafka tắt rồi tự gửi tiếp. TC07 chưa chạy.
Khi chạy trên máy có GoldenGate cài sẵn, cần kiểm chứng thêm các ca TC-GG01–TC-GG05: Extract bắt thay đổi,
độ trễ p95 qua GoldenGate, UPDATE/DELETE, đối soát, và tự phục hồi khi Kafka khởi động lại.

## 7. Ngưỡng cảnh báo

Ngưỡng nằm trong bảng ClickHouse `rtdp.alert_config` và có hiệu lực ở lần đánh giá kế tiếp (10 giây):

```bash
scripts/set-threshold.sh                          # xem ngưỡng hiện tại
scripts/set-threshold.sh withdraw_count 5         # cảnh báo khi rút > 5 lần ...
scripts/set-threshold.sh withdraw_window_min 5    # ... trong 5 phút
scripts/set-threshold.sh large_amount 1000000000  # giao dịch > 1 tỷ VND
```

Lệnh `generator.py anomaly` tự đọc ngưỡng, nên đợt bất thường luôn vượt ngưỡng.

## 8. Thiết kế phía ClickHouse

- Mỗi topic có một bảng `Kafka` engine (`q_*`). Materialized view parse JSON vào bảng đích, dùng ảnh `after` cho insert/update và `before` cho delete.
- Bảng đích dùng `ReplacingMergeTree(version, is_deleted)`. Update/delete được áp dụng theo khoá chính, và message nhận trùng
  (Kafka giao "ít nhất một lần", ví dụ khi Kafka khởi động lại) không làm lệch số liệu.
  Dashboard và đối soát đọc qua các view `v_*` (đã `FINAL`, bỏ dòng đã xoá).
- `kafka_flush_interval_ms = 1000`: ClickHouse ghi mỗi giây; mặc định là 7,5 giây.
- `rtdp.cdc_events` lưu mọi message thô trong 3 ngày, để trình diễn và gỡ lỗi.
- Độ trễ end-to-end = `ingested_at` (lúc vào ClickHouse) − `CREATED_AT` (Oracle ghi theo giờ UTC).

## 9. Xử lý sự cố

| Triệu chứng | Nguyên nhân thường gặp | Xử lý |
| --- | --- | --- |
| Extract không bắt được thay đổi | Chưa chạy `ADD SCHEMATRANDATA` hoặc Extract đăng ký sai PDB | `INFO SCHEMATRANDATA CORE`; kiểm tra alias trỏ `FREEPDB1` |
| Replicat báo lỗi kết nối Kafka | Sai `bootstrap.servers` | Từ máy host dùng `localhost:9092`; trong container docker dùng `kafka:29092`. Máy khác: đặt `KAFKA_EXTERNAL_HOST` trong `.env` |
| Topic có message nhưng ClickHouse trống | Tên topic khác `CORE.*` hoặc JSON khác mẫu | `scripts/check.sh`; đọc thử: `docker exec rtdp-kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:29092 --topic CORE.TXN --from-beginning --max-messages 1` |
| Dashboard thiếu tên chi nhánh / đối soát lệch ở branch, account | Seed trước khi Extract chạy | `generator.py resync` |
| Grafana không có datasource ClickHouse | Không tải được plugin (mất mạng lần đầu) | Có mạng rồi `docker compose restart grafana` |
| Container bị kill | Hết RAM | `docker stats`; giảm heap Java của GoldenGate |
| Ổ đầy dần | Archive log của Oracle | `docker exec -it rtdp-oracle rman target /` → `DELETE NOPROMPT ARCHIVELOG ALL COMPLETED BEFORE 'SYSDATE-1';` (chỉ khi Extract đã đọc qua) |

## 10. Dừng và dọn dẹp

```bash
docker compose stop        # dừng, giữ dữ liệu
docker compose down        # xoá container, giữ volume dữ liệu
docker compose down -v     # xoá cả dữ liệu (Oracle khởi tạo lại từ đầu lần sau)
```

Sau `down -v`, hãy xoá và tạo lại Extract/Replicat, vì checkpoint GoldenGate còn trỏ vào database cũ
(với container GoldenGate: `scripts/setup-goldengate.sh`).

## Khác biệt so với tài liệu triển khai ban đầu

- Image Oracle: `gvenzl/oracle-free:23` (Oracle Database 23ai Free trên Docker Hub, tải không cần đăng nhập, hỗ trợ script khởi tạo).
- Topic theo `${schemaName}.${tableName}`: `CORE.TXN`, `CORE.ACCOUNT`, `CORE.CUSTOMER`, `CORE.BRANCH` (thêm 2 bảng danh mục để hiển thị tên chi nhánh).
- ClickHouse dùng `ReplacingMergeTree` + view `FINAL` thay cho `MergeTree`, để TC02/TC03/TC04 đúng. Bảng tổng hợp theo phút
  được thay bằng truy vấn trực tiếp trên view, vì dữ liệu demo nhỏ, luôn đúng khi có update/delete.
- Ngưỡng cảnh báo cấu hình được, không cần sửa file.
