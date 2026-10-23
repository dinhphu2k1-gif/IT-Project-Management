"""Unit test cho logic sinh dữ liệu (không cần Oracle hay ClickHouse)."""
import random

import pytest

import generator as g


def test_money_within_bounds_and_rounded_to_thousand():
    random.seed(1)
    for _ in range(2000):
        v = g.money(2_000_000, 1.0, 10_000, 200_000_000)
        assert 10_000 <= v <= 200_000_000
        assert v % 1000 == 0


def test_random_txn_never_overdraws_account():
    random.seed(2)
    accounts = [[1, 1, 50_000.0], [2, 2, 1_000_000_000.0]]
    for _ in range(3000):
        account_id, branch_id, channel, txn_type, amount = g.random_txn(accounts)
        assert channel in g.CHANNELS and txn_type in g.TXN_TYPES and amount > 0
    assert all(a[2] >= 0 for a in accounts)


def test_random_txn_withdraw_only_on_atm_or_counter():
    random.seed(3)
    accounts = [[1, 1, 1e12]]
    for _ in range(3000):
        _, _, channel, txn_type, _ = g.random_txn(accounts)
        if txn_type == "WITHDRAW":
            assert channel in ("ATM", "COUNTER")


def test_random_txn_updates_cached_balance():
    random.seed(4)
    acc = [[7, 3, 10_000_000.0]]
    before = acc[0][2]
    _, _, _, txn_type, amount = g.random_txn(acc)
    sign = 1 if txn_type in g.CREDIT_TYPES else -1
    assert acc[0][2] == pytest.approx(before + sign * amount)


def test_thresholds_fall_back_to_defaults_when_clickhouse_down(monkeypatch):
    def boom(_sql):
        raise OSError("connection refused")
    monkeypatch.setattr(g, "ch_query", boom)
    assert g.thresholds() == g.DEFAULT_THRESHOLDS


def test_thresholds_override_from_clickhouse(monkeypatch):
    monkeypatch.setattr(g, "ch_query", lambda _sql: [{"name": "withdraw_count", "value": 3}])
    t = g.thresholds()
    assert t["withdraw_count"] == 3.0
    assert t["large_amount"] == g.DEFAULT_THRESHOLDS["large_amount"]
