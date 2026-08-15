from datetime import date, datetime

from kis_ai_scalper.broker.kis_fill_notice import FillNotice, FillNoticeKind
from kis_ai_scalper.market.clock import KST
from kis_ai_scalper.pipeline.fill_notice_reconciliation import apply_fill_notice
from kis_ai_scalper.storage import connect_database


RECEIVED = datetime(2026, 8, 18, 10, 0, tzinfo=KST)


def notice(*, kind=FillNoticeKind.FILLED, order_no="broker-1", symbol="005930", side="02", qty=10, fill_qty=10, price=100, fill_time="095900"):
    return FillNotice(
        kind=kind, order_no=order_no, original_order_no="", order_qty=qty,
        side=side, symbol=symbol, fill_qty=fill_qty, fill_price=price,
        fill_time=fill_time, customer_id="customer-secret", account_no="account-secret",
        receipt_type="", order_kind="00", reject_flag="Y" if kind is FillNoticeKind.REJECTED else "N",
        accepted_flag="Y" if kind is FillNoticeKind.ACCEPTED else "N", order_price=price,
        exchange_id="KRX",
    )


def seed_buy(database, *, client="client-1", broker="broker-1", signal="decision-1", qty=10):
    assert database.claim_order_intent(
        client_order_id=client, signal_id=signal, symbol="005930", side="BUY",
        requested_qty=qty, requested_price=100,
    )
    assert database.record_order_submission(client, broker)
    assert database.record_ai_decision(
        decision_id=signal, symbol="005930", action="BUY", confidence=.9,
        entry_price=100, take_profit_price=105, stop_loss_price=95,
        risk_level="NORMAL", requires_operator_approval=False, rationale="test",
        max_holding_seconds=900, created_at=RECEIVED,
    )


def seed_sell(database, *, client="client-sell", broker="broker-sell", qty=10):
    database.claim_order_intent(
        client_order_id=client, signal_id="sell-signal", symbol="005930", side="SELL",
        requested_qty=qty, requested_price=100,
    )
    database.record_order_submission(client, broker)


def test_duplicate_filled_notice_is_applied_once_and_opens_buy_position(tmp_path):
    with connect_database(tmp_path / "ledger.sqlite3") as database:
        database.init_schema()
        seed_buy(database)
        event = notice()
        first = apply_fill_notice(database, event, trading_date=date(2026, 8, 18), received_at=RECEIVED)
        second = apply_fill_notice(database, event, trading_date=date(2026, 8, 18), received_at=RECEIVED)

        assert first.applied and not first.blocked
        assert second.duplicate and not second.blocked
        assert len(database.list_broker_fills("client-1")) == 1
        position = database.list_open_live_positions("005930")
        assert len(position) == 1 and position[0]["quantity"] == 10


def test_sell_fill_reduces_and_then_closes_position(tmp_path):
    with connect_database(tmp_path / "ledger.sqlite3") as database:
        database.init_schema()
        seed_buy(database)
        apply_fill_notice(database, notice(), trading_date=date(2026, 8, 18), received_at=RECEIVED)
        seed_sell(database, qty=10)
        partial = apply_fill_notice(
            database, notice(order_no="broker-sell", side="01", qty=10, fill_qty=4, price=101),
            trading_date=date(2026, 8, 18), received_at=RECEIVED,
        )
        assert partial.applied
        assert database.list_open_live_positions("005930")[0]["quantity"] == 6

        seed_sell(database, client="client-sell-2", broker="broker-sell-2", qty=6)
        remaining = apply_fill_notice(
            database, notice(order_no="broker-sell-2", side="01", qty=6, fill_qty=6, price=102),
            trading_date=date(2026, 8, 18), received_at=RECEIVED,
        )
        assert remaining.applied
        assert database.list_open_live_positions("005930") == []


def test_unknown_order_sets_review_without_adopting(tmp_path):
    with connect_database(tmp_path / "ledger.sqlite3") as database:
        database.init_schema()
        result = apply_fill_notice(database, notice(order_no="broker-unknown"), received_at=RECEIVED)
        assert result.blocked and result.reason == "broker_order_not_found"
        assert database.get_runtime_metadata("operator_review") == "true"
        assert database.get_runtime_metadata("block_new_entries") == "true"
        assert database.connection.execute("SELECT COUNT(*) FROM broker_orders").fetchone()[0] == 0


def test_ambiguous_order_sets_review_without_adopting(tmp_path):
    with connect_database(tmp_path / "ledger.sqlite3") as database:
        database.init_schema()
        seed_buy(database, client="client-a")
        seed_buy(database, client="client-b", signal="decision-2")
        result = apply_fill_notice(database, notice(), received_at=RECEIVED)
        assert result.blocked and result.reason == "broker_order_ambiguous"
        assert database.connection.execute("SELECT COUNT(*) FROM broker_fills").fetchone()[0] == 0


def test_symbol_and_side_mismatch_set_review(tmp_path):
    with connect_database(tmp_path / "ledger.sqlite3") as database:
        database.init_schema()
        seed_buy(database)
        wrong_symbol = apply_fill_notice(database, notice(symbol="000660"), received_at=RECEIVED)
        wrong_side = apply_fill_notice(database, notice(side="01"), received_at=RECEIVED)
        assert wrong_symbol.blocked and wrong_symbol.reason == "symbol_mismatch"
        assert wrong_side.blocked and wrong_side.reason == "side_mismatch"
        assert database.connection.execute("SELECT COUNT(*) FROM broker_fills").fetchone()[0] == 0


def test_rejected_order_is_marked_only_when_unfilled(tmp_path):
    with connect_database(tmp_path / "ledger.sqlite3") as database:
        database.init_schema()
        seed_buy(database)
        result = apply_fill_notice(
            database, notice(kind=FillNoticeKind.REJECTED, fill_qty=0), received_at=RECEIVED,
        )
        assert result.outcome == "rejected"
        assert database.get_broker_order("client-1")["status"] == "REJECTED"


def test_rejection_after_fill_sets_review_and_does_not_regress(tmp_path):
    with connect_database(tmp_path / "ledger.sqlite3") as database:
        database.init_schema()
        seed_buy(database)
        apply_fill_notice(database, notice(fill_qty=4, qty=10), received_at=RECEIVED)
        result = apply_fill_notice(
            database, notice(kind=FillNoticeKind.REJECTED, fill_qty=0), received_at=RECEIVED,
        )
        assert result.blocked and result.reason == "rejected_order_has_fills"
        assert database.get_broker_order("client-1")["status"] == "PARTIALLY_FILLED"


def test_future_or_invalid_fill_date_sets_review(tmp_path):
    with connect_database(tmp_path / "ledger.sqlite3") as database:
        database.init_schema()
        seed_buy(database)
        future = apply_fill_notice(
            database, notice(), trading_date=date(2026, 8, 19), received_at=RECEIVED,
        )
        invalid = apply_fill_notice(
            database, notice(fill_time="256000"), trading_date=date(2026, 8, 18), received_at=RECEIVED,
        )
        assert future.blocked and future.reason == "future_trading_date"
        assert invalid.blocked and invalid.reason == "invalid_fill_time"
