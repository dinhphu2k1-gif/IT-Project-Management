#!/usr/bin/env python3
"""Đối soát Oracle và ClickHouse (ca kiểm thử TC04).

So sánh số bản ghi và tổng tiền của từng bảng. Nên dừng script sinh giao dịch trước khi chạy;
script tự chờ tối đa --timeout giây để ClickHouse bắt kịp.
"""

import argparse
import sys
import time
from decimal import Decimal

from common import ch_query, oracle_connect

CHECKS = [
    # (tên, câu Oracle, câu ClickHouse)
    ("branch", "SELECT COUNT(*), 0 FROM branch", "SELECT count(), 0 FROM rtdp.v_branch"),
    ("customer", "SELECT COUNT(*), 0 FROM customer", "SELECT count(), 0 FROM rtdp.v_customer"),
    ("account", "SELECT COUNT(*), NVL(SUM(balance), 0) FROM account",
     "SELECT count(), sum(balance) FROM rtdp.v_account"),
    ("txn", "SELECT COUNT(*), NVL(SUM(amount), 0) FROM txn",
     "SELECT count(), sum(amount) FROM rtdp.v_txn"),
]


def snapshot():
    conn = oracle_connect()
    cur = conn.cursor()
    rows = []
    for name, ora_sql, ch_sql in CHECKS:
        cur.execute(ora_sql)
        ora_count, ora_sum = cur.fetchone()
        ch = list(ch_query(ch_sql)[0].values())
        rows.append((name, int(ora_count), Decimal(str(ora_sum)), int(ch[0]), Decimal(str(ch[1]))))
    return rows


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--timeout", type=int, default=30)
    args = parser.parse_args()

    deadline = time.time() + args.timeout
    while True:
        rows = snapshot()
        ok = all(r[1] == r[3] and r[2] == r[4] for r in rows)
        if ok or time.time() >= deadline:
            break
        time.sleep(1)

    print(f"{'Bảng':10s} {'Oracle (số dòng)':>17s} {'ClickHouse':>12s} {'Oracle (tổng tiền)':>24s} {'ClickHouse':>24s}  KQ")
    for name, oc, osum, cc, csum in rows:
        mark = "KHỚP" if oc == cc and osum == csum else "LỆCH"
        print(f"{name:10s} {oc:>17,} {cc:>12,} {osum:>24,.2f} {csum:>24,.2f}  {mark}")
    print("\nKết quả:", "ĐẠT - Oracle và ClickHouse khớp 100%" if ok else "KHÔNG ĐẠT - còn chênh lệch")
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
