# BÀI TẬP LỚN: NỀN TẢNG DỮ LIỆU THỜI GIAN THỰC (RTDP) - AGRIBANK
> **Đề tài:** Xây dựng hệ thống giám sát giao dịch và cảnh báo rủi ro thời gian thực bằng công nghệ CDC (Change Data Capture)  
> **Mục tiêu:** Chuyển đổi báo cáo giao dịch ngân hàng từ chế độ trễ **T+1 (theo ngày)** sang **thời gian thực (độ trễ < 3 giây)**, không gây áp lực lên hệ thống Core Banking.

---

## 1. Kiến trúc hệ thống

```
┌─────────────────┐       ┌────────────────────┐       ┌────────────────────┐
│   Core Banking  │  CDC  │  Oracle GoldenGate │ Trail │  OGG for Big Data  │
│  Oracle 23ai    ├──────►│  Integrated Extract├──────►│  Replicat (DAA)    │
│  (Bảng CORE.*)  │       │     (EXTCORE)      │       │     (RKAFKA)       │
└─────────────────┘       └────────────────────┘       └─────────┬──────────┘
                                                                 │ JSON (mode=op)
                                                                 ▼
┌─────────────────┐       ┌────────────────────┐       ┌────────────────────┐
│ Grafana 11.3    │ Query │  ClickHouse 24.8   │ Stream│  Apache Kafka 3.8  │
│ Realtime Board  │◄──────┤  ReplacingMergeTree│◄──────┤  Topics: CORE.*    │
│ + Cảnh báo gian │       │  Materialized View │       │  (1 partition/FIFO)│
└─────────────────┘       └────────────────────┘       └────────────────────┘
```

### Nguyên lý luồng dữ liệu:
1. **Sinh giao dịch (Core Banking - Oracle):** Ứng dụng ghi nhận giao dịch vào các bảng `BRANCH`, `CUSTOMER`, `ACCOUNT`, `TXN`. Redo log ghi nhận mọi thay đổi vật lý.
2. **Bắt dữ liệu thay đổi (CDC - Oracle GoldenGate):** 
   - Tiến trình **Integrated Extract (`EXTCORE`)** đọc trực tiếp Redo log trong Oracle (không query vào bảng nghiệp vụ nên không ảnh hưởng tải OLTP), ghi thay đổi ra file Trail `ct`.
   - Tiến trình **Replicat (`RKAFKA`)** đọc file Trail, chuẩn hóa thành thông điệp JSON chứa trạng thái trước (`before`) và sau (`after`), gửi đến Apache Kafka.
3. **Phân phối & Xếp thứ tự (Apache Kafka):** Mỗi bảng tương ứng một topic (`CORE.TXN`, `CORE.ACCOUNT`...). Cấu hình 1 partition giúp bảo đảm tuyệt đối thứ tự thời gian của các giao dịch.
4. **Lưu trữ & Phân tích (ClickHouse):** Bảng Kafka Engine đọc dữ liệu streaming từ Kafka, Materialized View bóc tách JSON và ghi vào các bảng `ReplacingMergeTree`. Tự động khử trùng lặp và xử lý các thao tác Sửa/Xóa.
5. **Trực quan hóa & Cảnh báo (Grafana):** Đọc trực tiếp từ các view tổng hợp của ClickHouse, cập nhật tự động mỗi 5 giây với độ trễ ghi nhận chỉ khoảng 1–3 giây.

---

## 2. Hướng dẫn khởi chạy nhanh

Hệ thống đã được đóng gói hoàn toàn bằng Docker Compose:

```bash
cd rtdp-demo

# 1. Khởi động toàn bộ container (Oracle, Kafka, ClickHouse, Grafana, GoldenGate)
docker compose -f docker-compose.yml -f docker-compose.goldengate.yml up -d

# 2. Khởi tạo pipeline GoldenGate (chỉ cần chạy 1 lần)
scripts/setup-goldengate.sh

# 3. Kích hoạt môi trường Python và sinh dữ liệu mẫu ban đầu (seed)
source .venv/bin/activate
set -a; . ./.env; set +a
python generator/generator.py seed
```

*Dashboard theo dõi:* Mở trình duyệt truy cập: **http://localhost:3000/d/rtdp-realtime**

---

## 3. Kịch bản Demo thuyết trình (5 - 7 phút)

| Bước | Nội dung trình bày | Thao tác thực hiện | Kết quả quan sát trên Dashboard |
| :--- | :--- | :--- | :--- |
| **1** | **Tổng quan bài toán** | Mở trang Grafana: `http://localhost:3000/d/rtdp-realtime` | Giới thiệu các chỉ số KPI: Doanh số, số GD, cơ cấu kênh giao dịch (ATM, Counter, Mobile...). |
| **2** | **Demo giao dịch đơn lẻ & đo độ trễ** | `python generator/generator.py once --amount 50000000 --wait` | Dòng lệnh in ra: *Đã có ở ClickHouse sau ~2-3 giây*. Dashboard ngay lập tức hiện giao dịch 50 triệu ở bảng "Giao dịch mới nhất". |
| **3** | **Demo luồng giao dịch liên tục** | `python generator/generator.py run --rate 3` | Biểu đồ biến động giao dịch nhảy số liên tục theo thời gian thực (~3 GD/s). Nhấn `Ctrl + C` để dừng. |
| **4** | **Phát hiện gian lận / Bất thường** | `python generator/generator.py anomaly` | Kích hoạt kịch bản: rút tiền ATM liên tiếp 7 lần và chuyển khoản đột biến > 1.5 tỷ. Panel **"Cảnh báo giao dịch bất thường"** trên Grafana lập tức bật màu đỏ. |
| **5** | **Đối soát dữ liệu (Reconcile)** | `python generator/reconcile.py` | Kiểm tra tính toàn vẹn giữa Oracle và ClickHouse: **Khớp 100%** cả về số lượng bản ghi và tổng số dư tài khoản/tiền giao dịch. |

---

## 4. CẨM NANG THAO TÁC KHI GIẢNG VIÊN VẤN ĐÁP (CHEAT SHEET)

> *Dành cho sinh viên khi thầy/cô yêu cầu show dữ liệu trực tiếp trong từng thành phần.*

### Tình huống 1: "Thầy/Cô muốn xem dữ liệu gốc được lưu trong Oracle Core Banking như thế nào?"
Chạy lệnh truy cập `sqlplus` vào thẳng container Oracle:
```bash
docker exec -it rtdp-oracle sqlplus core/core@localhost:1521/FREEPDB1
```
Gõ các câu truy vấn SQL để show:
```sql
-- Xem 5 giao dịch mới nhất:
SELECT txn_id, account_id, txn_type, amount, channel, created_at 
FROM txn ORDER BY txn_id DESC FETCH FIRST 5 ROWS ONLY;

-- Xem số dư tài khoản:
SELECT account_id, account_no, balance, status FROM account WHERE ROWNUM <= 5;

-- Thoát sqlplus:
EXIT;
```

---

### Tình huống 2: "GoldenGate bắt dữ liệu ở đâu? Cho thầy/cô xem tiến trình CDC đang chạy?"
Có 2 cách trình bày cho giảng viên:

- **Cách 1 - Xem qua Giao diện Web (Khuyên dùng):**
  - Mở trình duyệt vào: **http://localhost:9011** (Tài khoản: `oggadmin` / `Oggadmin_123`)
  - Bấm vào **Administration Service** -> Chỉ cho giảng viên thấy tiến trình Extract `EXTCORE` đang ở trạng thái **Running**, kèm số lượng bản ghi đã capture từ Redo log.
  - Mở tiếp: **http://localhost:9021** -> Xem tiến trình Replicat `RKAFKA` đang đẩy dữ liệu sang Kafka.

- **Cách 2 - Xem dòng lệnh terminal:**
  ```bash
  # Xem trạng thái Extract:
  docker exec -i rtdp-ogg-oracle adminclient <<EOF
  CONNECT http://ogg-oracle:9011 DEPLOYMENT ORA_DEPLOY AS oggadmin PASSWORD Oggadmin_123
  INFO EXTRACT EXTCORE, DETAIL
  STATS EXTRACT EXTCORE, TOTAL
  EOF
  ```

---

### Tình huống 3: "Dữ liệu được đẩy qua Kafka trông như thế nào? Bắn thử 1 message xem format?"
Chạy lệnh console consumer của Kafka để đọc ngay 1 message CDC mẫu:
```bash
docker exec rtdp-kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka:29092 --topic CORE.TXN --partition 0 --offset latest --max-messages 1
```
**Giải thích cho giảng viên:**
- Format là JSON dạng **Op-mode** chuẩn của Oracle GoldenGate.
- Trường `"op_type"`: `I` (Insert), `U` (Update), `D` (Delete).
- Trường `"before"`: Lưu giá trị cũ trước khi sửa/xóa.
- Trường `"after"`: Lưu giá trị mới được thêm/sửa.
- Nhờ có cả `before` và `after`, ClickHouse xử lý được toàn bộ các thao tác nghiệp vụ ngân hàng mà không bị sai lệch số dư.

---

### Tình huống 4: "ClickHouse nhận dữ liệu và xử lý như thế nào? Tại sao lại dùng ReplacingMergeTree?"
Mở giao diện dòng lệnh ClickHouse:
```bash
docker exec -it rtdp-clickhouse clickhouse-client --user rtdp --password rtdp
```
Chạy các câu lệnh:
```sql
-- 1. Xem dữ liệu bảng giao dịch:
SELECT txn_id, account_id, amount, channel, created_at, ingested_at, latency_sec 
FROM rtdp.v_txn ORDER BY txn_id DESC LIMIT 5;

-- 2. Xem bảng nhật ký toàn bộ sự kiện CDC nhận từ Kafka:
SELECT topic, op_type, source_table, ingested_at FROM rtdp.cdc_events ORDER BY ingested_at DESC LIMIT 5;

-- Thoát:
EXIT;
```

**Câu trả lời chuẩn khi giảng viên hỏi "Tại sao dùng ReplacingMergeTree và view có FINAL?":**
> *"Thưa thầy/cô, Kafka có cơ chế phân phối `At-least-once` (ít nhất một lần), nên khi mạng chập chờn có thể sinh ra bản ghi trùng. Đồng thời, nghiệp vụ ngân hàng có các thao tác sửa thông tin tài khoản hoặc xóa giao dịch sai. Động cơ `ReplacingMergeTree` của ClickHouse cùng với `FINAL` giúp tự động khử trùng lặp bản ghi theo Primary Key và áp dụng trạng thái mới nhất theo trường `version`, đảm bảo dữ liệu phân tích luôn khớp 100% với Oracle."*

---

### Tình huống 5: "Làm thế nào để chứng minh hệ thống không làm mất hay sai lệch dữ liệu?"
Chạy công cụ đối soát tự động:
```bash
python generator/reconcile.py
```
Hệ thống sẽ đồng thời truy vấn tổng số dòng và tổng tiền của cả 4 bảng (`branch`, `customer`, `account`, `txn`) trên cả Oracle và ClickHouse, so sánh và in ra kết quả: **ĐẠT - Oracle và ClickHouse khớp 100%**.

---

### Tình huống 6: "Kiểm tra độ trễ (Latency) của toàn bộ đường ống?"
- Trên Grafana: Chỉ vào 2 panel **"Độ trễ trung bình"** và **"Độ trễ P95"**.
- Bản chất cách tính: `Độ trễ = ingested_at (thời điểm ghi vào ClickHouse) - created_at (thời điểm commit ở Oracle)`. Độ trễ thông thường đạt từ **1.5 đến 3.5 giây**, hoàn toàn đáp ứng tiêu chuẩn thời gian thực cho nghiệp vụ giám sát.

---

## 5. Cổng dịch vụ & Thông tin đăng nhập

| Dịch vụ | Địa chỉ | Tài khoản | Ghi chú |
| :--- | :--- | :--- | :--- |
| **Grafana** | `http://localhost:3000` | `admin` / `admin` | Dashboard chính |
| **GoldenGate Oracle** | `http://localhost:9011` | `oggadmin` / `Oggadmin_123` | Quản lý Extract CDC |
| **GoldenGate DAA** | `http://localhost:9021` | `oggadmin` / `Oggadmin_123` | Quản lý Kafka Replicat |
| **ClickHouse HTTP** | `http://localhost:8123` | `rtdp` / `rtdp` | CSDL OLAP |
| **Oracle Database** | `localhost:1521/FREEPDB1` | `core` / `core` | CSDL Core Banking |
| **Kafka Broker** | `localhost:9092` | *(Không mật khẩu)* | Message Broker |

---

## 6. Lệnh quản lý hệ thống
- **Kiểm tra trạng thái toàn diện:** `scripts/check.sh`
- **Tạm dừng hệ thống (giữ nguyên dữ liệu):**  
  `docker compose -f docker-compose.yml -f docker-compose.goldengate.yml stop`
- **Khởi động lại:**  
  `docker compose -f docker-compose.yml -f docker-compose.goldengate.yml start`
