"""Định dạng message của bộ giả lập phải giống GoldenGate Kafka Handler (format=json, mode=op)."""
import datetime as dt
import json
from decimal import Decimal

import gg_simulator as sim


def test_insert_message_has_after_image_and_metadata():
    msg = json.loads(sim.message("TXN", "I", {"TXN_ID": 1, "AMOUNT": Decimal("2500000.00"),
                                              "CREATED_AT": dt.datetime(2026, 10, 17, 3, 0, 0, 123456)}))
    assert msg["table"] == "FREEPDB1.CORE.TXN"
    assert msg["op_type"] == "I"
    assert set(msg) >= {"op_ts", "current_ts", "pos", "after"}
    assert "before" not in msg
    assert msg["after"]["AMOUNT"] == 2500000.0
    assert msg["after"]["CREATED_AT"] == "2026-10-17 03:00:00.123456"


def test_delete_message_uses_before_image():
    msg = json.loads(sim.message("TXN", "D", {"TXN_ID": 9}))
    assert msg["op_type"] == "D" and msg["before"] == {"TXN_ID": 9} and "after" not in msg


def test_position_is_monotonic_and_zero_padded():
    a = json.loads(sim.message("ACCOUNT", "U", {"ACCOUNT_ID": 1}))["pos"]
    b = json.loads(sim.message("ACCOUNT", "U", {"ACCOUNT_ID": 1}))["pos"]
    assert len(a) == 20 and int(b) == int(a) + 1
