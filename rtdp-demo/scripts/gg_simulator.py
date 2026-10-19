#!/usr/bin/env python3
"""GIẢ LẬP GoldenGate -> Kafka, CHỈ ĐỂ THỬ phần sau Kafka (ClickHouse, Grafana) khi GoldenGate chưa sẵn sàng.

Đọc thay đổi trong Oracle bằng cách hỏi định kỳ (không phải CDC thật: không thấy lệnh DELETE)
rồi gửi message JSON cùng định dạng với GoldenGate Kafka Handler (format=json, mode=op)
vào các topic CORE.*. Khi demo thật, TẮT script này và dùng GoldenGate.

  pip install confluent-kafka
  python scripts/gg_simulator.py            # chạy liên tục
  python scripts/gg_simulator.py --delete-txn 123   # gửi một message DELETE cho TC03
"""

import argparse
import datetime as dt
import json
import os
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "generator"))
from common import oracle_connect  # noqa: E402

import oracledb  # noqa: E402
from confluent_kafka import Producer  # noqa: E402

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TABLES = {
    # bảng: (khoá chính, cột theo dõi thay đổi hoặc None)
    "BRANCH": ("BRANCH_ID", None),
    "CUSTOMER": ("CUSTOMER_ID", None),
    "ACCOUNT": ("ACCOUNT_ID", "UPDATED_AT"),
    "TXN": ("TXN_ID", None),
}
_pos = 0


def fmt(v):
    if isinstance(v, dt.datetime):
        return v.strftime("%Y-%m-%d %H:%M:%S.%f")
    if v is None or isinstance(v, (int, str)):
        return v
    return float(v)


def message(table, op_type, row):
    global _pos
    _pos += 1
    now = dt.datetime.now(dt.timezone.utc)
    msg = {
        "table": f"FREEPDB1.CORE.{table}",
        "op_type": op_type,
        "op_ts": now.strftime("%Y-%m-%d %H:%M:%S.%f"),
        "current_ts": now.strftime("%Y-%m-%dT%H:%M:%S.%f"),
        "pos": f"{_pos:020d}",
    }
    msg["before" if op_type == "D" else "after"] = {k: fmt(v) for k, v in row.items()}
    return json.dumps(msg, ensure_ascii=False)


def send(producer, table, op_type, row, key):
    producer.produce(f"CORE.{table}", key=str(row[key]), value=message(table, op_type, row).encode("utf-8"))


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--interval", type=float, default=0.5)
    parser.add_argument("--delete-txn", type=int, help="chỉ gửi một message DELETE cho txn_id này rồi thoát")
    args = parser.parse_args()

    producer = Producer({"bootstrap.servers": BOOTSTRAP, "linger.ms": 0})
    if args.delete_txn:
        send(producer, "TXN", "D", {"TXN_ID": args.delete_txn}, "TXN_ID")
        producer.flush(10)
        print(f"Đã gửi DELETE txn_id={args.delete_txn}")
        return

    conn = oracle_connect()
    cur = conn.cursor()
    last_id = {t: 0 for t in TABLES}
    last_ts = {t: dt.datetime(1970, 1, 1) for t in TABLES}
    print(f"Giả lập GoldenGate -> {BOOTSTRAP}. Ctrl+C để dừng.")
    try:
        while True:
            sent = 0
            for table, (pk, ts_col) in TABLES.items():
                if ts_col:
                    cur.setinputsizes(oracledb.DB_TYPE_TIMESTAMP)  # giữ phần lẻ của giây khi so sánh
                    cur.execute(f"SELECT * FROM {table} WHERE {ts_col} > :1 ORDER BY {ts_col}", [last_ts[table]])
                else:
                    cur.execute(f"SELECT * FROM {table} WHERE {pk} > :1 ORDER BY {pk}", [last_id[table]])
                cols = [d[0] for d in cur.description]
                for values in cur.fetchall():
                    row = dict(zip(cols, values))
                    is_new = row[pk] > last_id[table]
                    send(producer, table, "I" if is_new else "U", row, pk)
                    last_id[table] = max(last_id[table], row[pk])
                    if ts_col:
                        last_ts[table] = max(last_ts[table], row[ts_col])
                    sent += 1
            producer.flush(10)
            if sent:
                print(f"{time.strftime('%H:%M:%S')} gửi {sent} message")
            time.sleep(args.interval)
    except KeyboardInterrupt:
        producer.flush(10)


if __name__ == "__main__":
    main()
