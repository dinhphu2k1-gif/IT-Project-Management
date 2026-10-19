#!/usr/bin/env python3
"""Sinh dữ liệu core banking giả lập vào Oracle (schema CORE).

Các lệnh:
  seed        tạo chi nhánh, khách hàng, tài khoản (chạy SAU khi GoldenGate đã bắt thay đổi)
  run         sinh giao dịch liên tục (chế độ thường, có thể chèn bất thường định kỳ)
  anomaly     bắn một đợt giao dịch bất thường để kích hoạt cảnh báo
  once        tạo một giao dịch thủ công, tuỳ chọn đo thời gian đến ClickHouse (--wait)
  update      cập nhật số dư một tài khoản (ca kiểm thử TC02)
  delete      xoá một giao dịch (ca kiểm thử TC03)
  resync      gửi lại toàn bộ dữ liệu qua GoldenGate (khi đã seed trước khi bật Extract)
  stats       in số liệu tổng hợp trong Oracle
"""

import argparse
import math
import random
import signal
import sys
import time

import oracledb

from common import ch_query, oracle_connect

CHANNELS = ["ATM", "POS", "MOBILE", "INTERNET", "COUNTER"]
CHANNEL_WEIGHTS = [20, 25, 35, 12, 8]
TXN_TYPES = ["DEPOSIT", "WITHDRAW", "TRANSFER_OUT", "TRANSFER_IN", "PAYMENT"]
TXN_TYPE_WEIGHTS = [20, 25, 20, 15, 20]
CREDIT_TYPES = {"DEPOSIT", "TRANSFER_IN"}

DEFAULT_THRESHOLDS = {"withdraw_count": 5, "withdraw_window_min": 5, "large_amount": 1_000_000_000}

BRANCHES = [
    (1, "HN001", "Chi nhánh Hoàn Kiếm", "Hà Nội"),
    (2, "HN002", "Chi nhánh Cầu Giấy", "Hà Nội"),
    (3, "HN003", "Chi nhánh Long Biên", "Hà Nội"),
    (4, "HCM01", "Chi nhánh Quận 1", "TP. Hồ Chí Minh"),
    (5, "HCM02", "Chi nhánh Thủ Đức", "TP. Hồ Chí Minh"),
    (6, "HCM03", "Chi nhánh Tân Bình", "TP. Hồ Chí Minh"),
    (7, "DN001", "Chi nhánh Hải Châu", "Đà Nẵng"),
    (8, "HP001", "Chi nhánh Lê Chân", "Hải Phòng"),
    (9, "CT001", "Chi nhánh Ninh Kiều", "Cần Thơ"),
    (10, "KH001", "Chi nhánh Nha Trang", "Khánh Hòa"),
]
HO = ["Nguyễn", "Trần", "Lê", "Phạm", "Hoàng", "Huỳnh", "Phan", "Vũ", "Võ", "Đặng", "Bùi", "Đỗ", "Ngô", "Dương"]
DEM = ["Văn", "Thị", "Minh", "Đức", "Thu", "Ngọc", "Quang", "Thanh", "Hữu", "Gia", "Anh", "Bảo"]
TEN = ["An", "Bình", "Châu", "Dũng", "Giang", "Hà", "Hải", "Hùng", "Lan", "Linh", "Long", "Mai",
       "Nam", "Phúc", "Quân", "Sơn", "Tâm", "Thảo", "Trang", "Tuấn", "Vy", "Yến"]

_stop = False


def _handle_sigint(signum, frame):
    global _stop
    _stop = True


def thresholds():
    """Ngưỡng cảnh báo hiện tại trong ClickHouse (để đợt bất thường chắc chắn vượt ngưỡng)."""
    try:
        rows = ch_query("SELECT name, value FROM rtdp.alert_config FINAL")
        return {**DEFAULT_THRESHOLDS, **{r["name"]: float(r["value"]) for r in rows}}
    except OSError:
        return DEFAULT_THRESHOLDS


def money(mu, sigma, lo, hi):
    """Số tiền VND ngẫu nhiên theo phân phối log-normal, làm tròn nghìn đồng."""
    value = random.lognormvariate(math.log(mu), sigma)
    return float(round(min(max(value, lo), hi), -3))


# ---------------------------------------------------------------------------
# Thao tác dữ liệu
# ---------------------------------------------------------------------------

def insert_txn(cur, account_id, branch_id, channel, txn_type, amount, description=None):
    """Ghi một giao dịch và cập nhật số dư trong cùng transaction. Trả về (txn_id, created_at)."""
    sign = 1 if txn_type in CREDIT_TYPES else -1
    txn_id = cur.var(int)
    created_at = cur.var(oracledb.DB_TYPE_TIMESTAMP)
    cur.execute(
        """INSERT INTO txn (account_id, branch_id, channel, txn_type, amount, description)
           VALUES (:1, :2, :3, :4, :5, :6)
           RETURNING txn_id, created_at INTO :7, :8""",
        [account_id, branch_id, channel, txn_type, amount,
         description or f"{txn_type} qua {channel}", txn_id, created_at],
    )
    cur.execute(
        """UPDATE account SET balance = balance + :1, updated_at = SYS_EXTRACT_UTC(SYSTIMESTAMP)
           WHERE account_id = :2""",
        [sign * amount, account_id],
    )
    return txn_id.getvalue()[0], created_at.getvalue()[0]


def load_accounts(cur):
    cur.execute("SELECT account_id, branch_id, balance FROM account WHERE status = 'ACTIVE'")
    return [list(r) for r in cur.fetchall()]


def random_txn(accounts):
    acc = random.choice(accounts)
    account_id, branch_id, balance = acc
    channel = random.choices(CHANNELS, CHANNEL_WEIGHTS)[0]
    txn_type = random.choices(TXN_TYPES, TXN_TYPE_WEIGHTS)[0]
    if txn_type == "WITHDRAW" and channel in ("POS", "INTERNET"):
        channel = random.choice(["ATM", "COUNTER"])
    amount = money(2_000_000, 1.0, 10_000, 200_000_000)
    if txn_type not in CREDIT_TYPES and amount > float(balance):
        txn_type = "DEPOSIT"
    acc[2] = float(balance) + (amount if txn_type in CREDIT_TYPES else -amount)
    return account_id, branch_id, channel, txn_type, amount


def anomaly_burst(conn, accounts, account_id=None, count=None, large=True, gap=1.5):
    """Một tài khoản rút tiền liên tục (> ngưỡng) và một giao dịch giá trị rất lớn."""
    cur = conn.cursor()
    limits = thresholds()
    count = count or int(limits["withdraw_count"]) + 2
    if account_id is None:
        acc = random.choice([a for a in accounts if float(a[2]) > 50_000_000] or accounts)
    else:
        acc = next(a for a in accounts if a[0] == account_id)
    print(f"[bất thường] tài khoản {acc[0]}: rút {count} lần liên tiếp tại ATM")
    for i in range(count):
        amount = float(random.choice([3_000_000, 4_000_000, 5_000_000]))
        txn_id, _ = insert_txn(cur, acc[0], acc[1], "ATM", "WITHDRAW", amount, "Rút tiền ATM liên tục")
        conn.commit()
        print(f"  #{i + 1} txn_id={txn_id} số tiền={amount:,.0f}")
        time.sleep(gap)
    if large:
        rich = max(accounts, key=lambda a: float(a[2]))
        amount = float(round(limits["large_amount"] * random.uniform(1.2, 2.0), -6))
        txn_id, _ = insert_txn(cur, rich[0], rich[1], "INTERNET", "TRANSFER_OUT", amount,
                               "Chuyển khoản giá trị lớn")
        conn.commit()
        print(f"[bất thường] tài khoản {rich[0]}: chuyển khoản lớn {amount:,.0f} VND (txn_id={txn_id})")


# ---------------------------------------------------------------------------
# Lệnh
# ---------------------------------------------------------------------------

def cmd_seed(args):
    conn = oracle_connect()
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM branch")
    if cur.fetchone()[0] > 0 and not args.force:
        print("Đã có dữ liệu gốc, bỏ qua (dùng --force để thêm khách hàng/tài khoản mới).")
        return
    random.seed(args.seed)
    cur.executemany(
        """MERGE INTO branch b USING (SELECT :1 id, :2 code, :3 name, :4 city FROM dual) s
           ON (b.branch_id = s.id)
           WHEN NOT MATCHED THEN INSERT VALUES (s.id, s.code, s.name, s.city)""",
        BRANCHES,
    )
    cur.execute("SELECT NVL(MAX(customer_id), 100000) FROM customer")
    next_customer = cur.fetchone()[0] + 1
    cur.execute("SELECT NVL(MAX(account_id), 1000000) FROM account")
    next_account = cur.fetchone()[0] + 1

    customers = []
    for i in range(args.customers):
        name = f"{random.choice(HO)} {random.choice(DEM)} {random.choice(TEN)}"
        segment = random.choices(["CA_NHAN", "PRIORITY", "DOANH_NGHIEP"], [80, 12, 8])[0]
        customers.append((next_customer + i, name, segment, random.randint(1, len(BRANCHES))))
    cur.executemany("INSERT INTO customer (customer_id, full_name, segment, branch_id) VALUES (:1, :2, :3, :4)",
                    customers)

    accounts = []
    for i in range(args.accounts):
        cust = customers[i % len(customers)]
        if cust[2] == "CA_NHAN":
            balance = money(50_000_000, 1.0, 1_000_000, 2_000_000_000)
        else:
            balance = money(3_000_000_000, 0.8, 500_000_000, 50_000_000_000)
        account_id = next_account + i
        accounts.append((account_id, f"0{account_id:011d}", cust[0], cust[3], balance))
    cur.executemany(
        "INSERT INTO account (account_id, account_no, customer_id, branch_id, balance) VALUES (:1, :2, :3, :4, :5)",
        accounts,
    )
    conn.commit()
    print(f"Đã tạo {len(BRANCHES)} chi nhánh, {len(customers)} khách hàng, {len(accounts)} tài khoản.")


def cmd_run(args):
    signal.signal(signal.SIGINT, _handle_sigint)
    signal.signal(signal.SIGTERM, _handle_sigint)
    conn = oracle_connect()
    cur = conn.cursor()
    accounts = load_accounts(cur)
    if not accounts:
        sys.exit("Chưa có tài khoản. Chạy: python generator.py seed")
    print(f"Sinh giao dịch ~{args.rate}/giây trên {len(accounts)} tài khoản. Ctrl+C để dừng.")
    started = time.time()
    last_anomaly = started
    total = 0
    while not _stop:
        if args.duration and time.time() - started >= args.duration:
            break
        txn_id, _ = insert_txn(cur, *random_txn(accounts))
        conn.commit()
        total += 1
        if total % 20 == 0:
            print(f"{time.strftime('%H:%M:%S')} đã sinh {total} giao dịch (txn_id mới nhất {txn_id})")
        if args.anomaly_every and time.time() - last_anomaly >= args.anomaly_every:
            anomaly_burst(conn, accounts)
            last_anomaly = time.time()
        time.sleep(random.expovariate(args.rate))
    print(f"Dừng. Tổng cộng {total} giao dịch.")


def cmd_anomaly(args):
    conn = oracle_connect()
    accounts = load_accounts(conn.cursor())
    anomaly_burst(conn, accounts, args.account_id, args.count, large=not args.no_large, gap=args.gap)


def cmd_once(args):
    conn = oracle_connect()
    cur = conn.cursor()
    if args.account_id:
        cur.execute("SELECT account_id, branch_id FROM account WHERE account_id = :1", [args.account_id])
    else:
        cur.execute("SELECT account_id, branch_id FROM account ORDER BY DBMS_RANDOM.VALUE FETCH FIRST 1 ROWS ONLY")
    row = cur.fetchone()
    if not row:
        sys.exit("Không tìm thấy tài khoản.")
    txn_id, created_at = insert_txn(cur, row[0], row[1], args.channel, args.type, float(args.amount),
                                    "Giao dịch demo thủ công")
    conn.commit()
    committed = time.time()
    print(f"Đã commit txn_id={txn_id} tài khoản={row[0]} {args.type} {float(args.amount):,.0f} VND "
          f"lúc {created_at:%H:%M:%S.%f} UTC")
    if not args.wait:
        return
    print("Chờ giao dịch xuất hiện ở ClickHouse...", flush=True)
    deadline = committed + args.timeout
    while time.time() < deadline:
        rows = ch_query(f"SELECT latency_sec FROM rtdp.v_txn WHERE txn_id = {int(txn_id)}")
        if rows:
            print(f"Đã có ở ClickHouse sau {time.time() - committed:.2f} giây "
                  f"(độ trễ ghi nhận: {float(rows[0]['latency_sec']):.2f} giây)")
            return
        time.sleep(0.2)
    sys.exit(f"Sau {args.timeout} giây vẫn chưa thấy giao dịch ở ClickHouse.")


def cmd_update(args):
    conn = oracle_connect()
    cur = conn.cursor()
    cur.execute(
        "UPDATE account SET balance = :1, updated_at = SYS_EXTRACT_UTC(SYSTIMESTAMP) WHERE account_id = :2",
        [float(args.balance), args.account_id],
    )
    conn.commit()
    print(f"Cập nhật {cur.rowcount} tài khoản: account_id={args.account_id} balance={float(args.balance):,.2f}")


def cmd_delete(args):
    conn = oracle_connect()
    cur = conn.cursor()
    txn_id = args.txn_id
    if txn_id is None:
        cur.execute("SELECT MAX(txn_id) FROM txn")
        txn_id = cur.fetchone()[0]
    cur.execute("DELETE FROM txn WHERE txn_id = :1", [txn_id])
    conn.commit()
    print(f"Đã xoá {cur.rowcount} giao dịch: txn_id={txn_id}")


def cmd_resync(args):
    """UPDATE "không đổi giá trị" mọi dòng để GoldenGate gửi lại toàn bộ dữ liệu sang Kafka.
    Dùng khi đã seed dữ liệu TRƯỚC khi Extract chạy (ClickHouse thiếu dữ liệu gốc)."""
    conn = oracle_connect()
    cur = conn.cursor()
    for table, column in (("branch", "city"), ("customer", "segment"), ("account", "status"), ("txn", "currency")):
        cur.execute(f"UPDATE {table} SET {column} = {column}")
        conn.commit()
        print(f"{table:9s} gửi lại {cur.rowcount:,} dòng")


def cmd_stats(args):
    conn = oracle_connect()
    cur = conn.cursor()
    for table in ("branch", "customer", "account", "txn"):
        cur.execute(f"SELECT COUNT(*) FROM {table}")
        print(f"{table:9s} {cur.fetchone()[0]:>10,}")
    cur.execute("SELECT NVL(SUM(amount), 0), NVL(MAX(txn_id), 0) FROM txn")
    total, max_id = cur.fetchone()
    print(f"Tổng tiền giao dịch: {float(total):,.2f} VND, txn_id lớn nhất: {max_id}")


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="command", required=True)

    p = sub.add_parser("seed", help="tạo dữ liệu gốc")
    p.add_argument("--customers", type=int, default=200)
    p.add_argument("--accounts", type=int, default=300)
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--force", action="store_true")
    p.set_defaults(func=cmd_seed)

    p = sub.add_parser("run", help="sinh giao dịch liên tục")
    p.add_argument("--rate", type=float, default=3.0, help="số giao dịch trung bình mỗi giây")
    p.add_argument("--duration", type=int, default=0, help="chạy bao nhiêu giây (0 = đến khi Ctrl+C)")
    p.add_argument("--anomaly-every", type=int, default=0, help="chèn đợt bất thường mỗi N giây (0 = tắt)")
    p.set_defaults(func=cmd_run)

    p = sub.add_parser("anomaly", help="bắn một đợt giao dịch bất thường")
    p.add_argument("--account-id", type=int)
    p.add_argument("--count", type=int, help="số lần rút (mặc định: ngưỡng withdraw_count + 2)")
    p.add_argument("--gap", type=float, default=1.5, help="giây giữa hai lần rút")
    p.add_argument("--no-large", action="store_true", help="không tạo giao dịch giá trị lớn")
    p.set_defaults(func=cmd_anomaly)

    p = sub.add_parser("once", help="một giao dịch thủ công")
    p.add_argument("--account-id", type=int)
    p.add_argument("--amount", default="2500000")
    p.add_argument("--type", default="DEPOSIT", choices=TXN_TYPES)
    p.add_argument("--channel", default="COUNTER", choices=CHANNELS)
    p.add_argument("--wait", action="store_true", help="chờ đến khi thấy ở ClickHouse và in độ trễ")
    p.add_argument("--timeout", type=int, default=30)
    p.set_defaults(func=cmd_once)

    p = sub.add_parser("update", help="cập nhật số dư tài khoản (TC02)")
    p.add_argument("--account-id", type=int, required=True)
    p.add_argument("--balance", required=True)
    p.set_defaults(func=cmd_update)

    p = sub.add_parser("delete", help="xoá một giao dịch (TC03)")
    p.add_argument("--txn-id", type=int, help="mặc định: giao dịch mới nhất")
    p.set_defaults(func=cmd_delete)

    p = sub.add_parser("resync", help="gửi lại toàn bộ dữ liệu qua GoldenGate (khi seed trước khi bật Extract)")
    p.set_defaults(func=cmd_resync)

    p = sub.add_parser("stats", help="số liệu trong Oracle")
    p.set_defaults(func=cmd_stats)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
