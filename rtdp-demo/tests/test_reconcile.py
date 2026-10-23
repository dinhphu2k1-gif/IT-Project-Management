from decimal import Decimal

from reconcile import all_match


def test_all_match_true_when_counts_and_sums_equal():
    rows = [("txn", 10, Decimal("100.50"), 10, Decimal("100.50")), ("branch", 3, Decimal(0), 3, Decimal(0))]
    assert all_match(rows)


def test_all_match_false_on_count_mismatch():
    assert not all_match([("txn", 10, Decimal("1"), 9, Decimal("1"))])


def test_all_match_false_on_amount_mismatch():
    assert not all_match([("account", 5, Decimal("10.00"), 5, Decimal("10.01"))])
