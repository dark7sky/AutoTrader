import pytest

from kis_ai_scalper.cli import main
from kis_ai_scalper.paper import (
    PaperFill,
    PaperLedger,
    PaperOrderIntent,
    PaperOrderStatus,
    PaperSide,
)


def order(order_id, side, quantity, price):
    return PaperOrderIntent(order_id, "005930", side, quantity, price)


def test_buy_updates_weighted_average_price():
    ledger = PaperLedger()
    ledger.submit_order(order("b1", PaperSide.BUY, 10, 100))
    ledger.fill_order(PaperFill("f1", "b1", 10, 100))
    ledger.submit_order(order("b2", PaperSide.BUY, 10, 110))
    ledger.fill_order(PaperFill("f2", "b2", 10, 110))
    assert ledger.positions["005930"].quantity == 20
    assert ledger.positions["005930"].avg_price == 105


def test_partial_sell_realizes_pnl_at_average_cost():
    ledger = PaperLedger()
    ledger.submit_order(order("b1", PaperSide.BUY, 10, 100))
    ledger.fill_order(PaperFill("f1", "b1", 10, 100))
    ledger.submit_order(order("s1", PaperSide.SELL, 4, 110))
    ledger.fill_order(PaperFill("f2", "s1", 4, 110))
    assert ledger.realized_pnl == 40
    assert ledger.positions["005930"].quantity == 6


def test_oversell_is_rejected_without_short_position():
    ledger = PaperLedger()
    rejected = ledger.submit_order(order("s1", PaperSide.SELL, 1, 100))
    assert rejected.status is PaperOrderStatus.REJECTED
    assert ledger.positions == {}


def test_duplicate_fill_is_idempotent():
    ledger = PaperLedger()
    ledger.submit_order(order("b1", PaperSide.BUY, 2, 100))
    fill = PaperFill("f1", "b1", 2, 100)
    assert ledger.fill_order(fill) == fill
    assert ledger.fill_order(fill) == fill
    assert ledger.positions["005930"].quantity == 2


def test_conflicting_duplicate_fill_id_is_rejected():
    ledger = PaperLedger()
    ledger.submit_order(order("b1", PaperSide.BUY, 3, 100))
    ledger.fill_order(PaperFill("f1", "b1", 2, 100))
    with pytest.raises(ValueError, match="conflicting paper fill id"):
        ledger.fill_order(PaperFill("f1", "b1", 1, 100))


def test_conflicting_duplicate_order_id_is_rejected():
    ledger = PaperLedger()
    ledger.submit_order(order("b1", PaperSide.BUY, 2, 100))
    with pytest.raises(ValueError, match="conflicting paper order id"):
        ledger.submit_order(order("b1", PaperSide.BUY, 3, 100))


def test_submit_order_does_not_mutate_input_intent():
    ledger = PaperLedger()
    intent = order("s1", PaperSide.SELL, 1, 100)
    rejected = ledger.submit_order(intent)

    assert rejected.status is PaperOrderStatus.REJECTED
    assert intent.status is PaperOrderStatus.PENDING


def test_invalid_trade_values_are_rejected():
    with pytest.raises(ValueError):
        PaperLedger().submit_order(order("b1", PaperSide.BUY, 0, 100))
    with pytest.raises(ValueError):
        PaperLedger().submit_order(order("b1", PaperSide.BUY, 1, 0))
    with pytest.raises(ValueError):
        PaperLedger().submit_order(order("", PaperSide.BUY, 1, 100))


def test_paper_cli_sample_has_no_external_side_effects(capsys):
    assert main(["paper-check-sample"]) == 0
    output = capsys.readouterr().out
    assert "paper trade check: OK" in output
    assert "realized_pnl=80" in output
    assert "broker_calls=none orders=none account_queries=none ai_calls=none" in output
