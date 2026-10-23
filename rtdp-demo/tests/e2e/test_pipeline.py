"""Kiểm thử end-to-end trên hệ thống đang chạy (docker compose + bộ giả lập hoặc GoldenGate thật).

Chạy:  RTDP_E2E=1 pytest -m e2e tests/e2e
Yêu cầu: đã `docker compose up -d`, đã `generator.py seed`, và có luồng CDC (GoldenGate hoặc gg_simulator.py).
"""
import os
import subprocess
import sys
import time

import pytest

from common import ch_query, oracle_connect

pytestmark = [pytest.mark.e2e,
              pytest.mark.skipif(os.getenv("RTDP_E2E") != "1", reason="đặt RTDP_E2E=1 để chạy kiểm thử end-to-end")]
ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def wait_for(sql, predicate, timeout=30):
    deadline = time.time() + timeout
    while time.time() < deadline:
        rows = ch_query(sql)
        if predicate(rows):
            return rows
        time.sleep(0.3)
    pytest.fail(f"Hết {timeout}s vẫn chưa thoả điều kiện: {sql}")


def run(*args):
    return subprocess.run([sys.executable, os.path.join(ROOT, "generator", "generator.py"), *args],
                          capture_output=True, text=True, check=True).stdout


def test_tc01_insert_arrives_within_10_seconds():
    out = run("once", "--amount", "1234000", "--wait", "--timeout", "10")
    assert "Đã có ở ClickHouse" in out


def test_tc02_update_is_applied():
    conn = oracle_connect()
    cur = conn.cursor()
    cur.execute("SELECT MIN(account_id) FROM account")
    account_id = cur.fetchone()[0]
    run("update", "--account-id", str(account_id), "--balance", "987654321.25")
    wait_for(f"SELECT balance FROM rtdp.v_account WHERE account_id = {account_id}",
             lambda r: r and float(r[0]["balance"]) == 987654321.25)


def test_tc04_reconcile_matches():
    result = subprocess.run([sys.executable, os.path.join(ROOT, "generator", "reconcile.py"), "--timeout", "30"],
                            capture_output=True, text=True)
    assert result.returncode == 0, result.stdout


def test_latency_p95_under_10_seconds():
    rows = ch_query("SELECT quantile(0.95)(latency_sec) AS p95 FROM rtdp.v_txn")
    assert float(rows[0]["p95"]) < 10
