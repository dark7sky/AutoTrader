from datetime import datetime, timezone

from kis_ai_scalper.broker.kis_account import (
    KisAccountPosition,
    KisAccountSnapshot,
    KisAccountSummary,
)
from kis_ai_scalper.broker.kis_order import KisOrderSide
from kis_ai_scalper.broker.kis_order_status import KisOrderStatus, KisOrderStatusRecord
from kis_ai_scalper.pipeline.broker_reconciliation import reconcile_broker_state
from kis_ai_scalper.storage import connect_database


NOW = datetime(2026, 8, 18, 9, 30, tzinfo=timezone.utc)


class FakeOrders:
    def __init__(self, orders):
        self.orders = tuple(orders)
        self.calls = 0

    def get_today_orders(self):
        self.calls += 1
        return self.orders


class FakeAccount:
    def __init__(self, positions=()):
        self.positions = tuple(positions)
        self.calls = 0

    def get_snapshot(self):
        self.calls += 1
        return KisAccountSnapshot(
            self.positions,
            KisAccountSummary(1_000_000, 1_000_000, 1_000_000, 0),
        )


def status(*, number="broker-1", filled=0, remaining=10, state=KisOrderStatus.UNFILLED):
    return KisOrderStatusRecord(
        order_number=number,
        symbol="005930",
        side=KisOrderSide.BUY,
        ordered_quantity=10,
        filled_quantity=filled,
        remaining_quantity=remaining,
        order_price=100.0,
        average_fill_price=100.0 if filled else None,
        status=state,
        order_time="093000",
        order_date="20260818",
    )


def seed_order(database, *, status="ACKNOWLEDGED", signal_id="decision-1"):
    database.claim_order_intent(
        client_order_id="client-1", signal_id=signal_id, symbol="005930",
        side="BUY", requested_qty=10, requested_price=100,
    )
    database.record_order_submission("client-1", "broker-1")
    if status != "ACKNOWLEDGED":
        database.update_broker_order_status("client-1", status)
    database.record_ai_decision(
        decision_id=signal_id, symbol="005930", action="BUY", confidence=0.9,
        entry_price=100, take_profit_price=104, stop_loss_price=99,
        risk_level="NORMAL", requires_operator_approval=False,
        rationale="test", created_at=NOW,
    )


def account_position(qty):
    return KisAccountPosition("005930", qty, qty, 100, 100, 0)


def test_partial_fill_is_stored_and_materialized_once(tmp_path):
    with connect_database(tmp_path / "reconcile.db") as database:
        database.init_schema()
        seed_order(database)
        result = reconcile_broker_state(
            database, FakeOrders([status(filled=3, remaining=7, state=KisOrderStatus.PARTIALLY_FILLED)]),
            FakeAccount([account_position(3)]), current_time=NOW,
        )
        order = database.get_broker_order("client-1")
        position = database.list_open_live_positions("005930")

    assert result.operator_review is False
    assert result.new_fills == 1
    assert result.materialized_fills == 1
    assert order["status"] == "PARTIALLY_FILLED"
    assert order["filled_qty"] == 3
    assert position[0]["quantity"] == 3


def test_reconciliation_restart_and_duplicate_response_do_not_duplicate_fill(tmp_path):
    path = tmp_path / "restart.db"
    with connect_database(path) as database:
        database.init_schema()
        seed_order(database)
        orders = FakeOrders([status(filled=3, remaining=7, state=KisOrderStatus.PARTIALLY_FILLED)])
        account = FakeAccount([account_position(3)])
        first = reconcile_broker_state(database, orders, account, current_time=NOW)
        assert first.new_fills == 1
    with connect_database(path) as database:
        database.init_schema()
        second = reconcile_broker_state(
            database, FakeOrders([status(filled=3, remaining=7, state=KisOrderStatus.PARTIALLY_FILLED)]),
            FakeAccount([account_position(3)]), current_time=NOW,
        )
        assert second.new_fills == 0
        assert second.materialized_fills == 0
        assert database.get_broker_order("client-1")["filled_qty"] == 3
        assert len(database.list_broker_fills("client-1")) == 1


def test_later_partial_fill_derives_incremental_price_from_cumulative_average(tmp_path):
    with connect_database(tmp_path / "weighted.db") as database:
        database.init_schema()
        seed_order(database)
        first = status(filled=3, remaining=7, state=KisOrderStatus.PARTIALLY_FILLED)
        reconcile_broker_state(
            database, FakeOrders([first]), FakeAccount([account_position(3)]), current_time=NOW,
        )
        second = status(filled=5, remaining=5, state=KisOrderStatus.PARTIALLY_FILLED)
        second = KisOrderStatusRecord(
            **{**second.__dict__, "average_fill_price": 102.0}
        )
        reconcile_broker_state(
            database, FakeOrders([second]), FakeAccount([account_position(5)]), current_time=NOW,
        )
        fills = database.list_broker_fills("client-1")
        position = database.list_open_live_positions("005930")[0]

    assert [row["price"] for row in fills] == [100.0, 105.0]
    assert position["entry_price"] == 102.0


def test_cancelled_order_updates_status_after_preserving_partial_fill(tmp_path):
    with connect_database(tmp_path / "cancel.db") as database:
        database.init_schema()
        seed_order(database)
        result = reconcile_broker_state(
            database, FakeOrders([status(filled=3, remaining=0, state=KisOrderStatus.CANCELLED)]),
            FakeAccount([account_position(3)]), current_time=NOW,
        )
        order = database.get_broker_order("client-1")

    assert result.operator_review is False
    assert order["status"] == "CANCELLED"
    assert order["filled_qty"] == 3


def test_unknown_and_unconfirmed_local_orders_block_entries(tmp_path):
    with connect_database(tmp_path / "unknown.db") as database:
        database.init_schema()
        database.claim_order_intent(
            client_order_id="client-unknown", signal_id="signal-unknown", symbol="005930",
            side="BUY", requested_qty=1, requested_price=100,
        )
        database.mark_order_unknown("client-unknown", "timeout")
        result = reconcile_broker_state(database, FakeOrders([]), FakeAccount(), current_time=NOW)

    assert result.operator_review is True
    assert result.block_new_entries is True
    assert any(reason.startswith("local_unknown:") for reason in result.reasons)
    assert result.reasons


def test_unknown_order_is_still_reconciled_when_broker_id_is_known(tmp_path):
    with connect_database(tmp_path / "unknown-known-broker.db") as database:
        database.init_schema()
        seed_order(database)
        database.mark_order_unknown("client-1", "submission timeout")
        result = reconcile_broker_state(
            database,
            FakeOrders([status(filled=2, remaining=8, state=KisOrderStatus.PARTIALLY_FILLED)]),
            FakeAccount([account_position(2)]),
            current_time=NOW,
        )
        order = database.get_broker_order("client-1")

    assert result.operator_review is True
    assert any(reason.startswith("local_unknown:") for reason in result.reasons)
    assert order["status"] == "PARTIALLY_FILLED"
    assert order["filled_qty"] == 2


def test_broker_only_open_order_requires_operator_review(tmp_path):
    with connect_database(tmp_path / "external-order.db") as database:
        database.init_schema()
        result = reconcile_broker_state(
            database, FakeOrders([status(number="external-1")]), FakeAccount(), current_time=NOW
        )

    assert result.operator_review is True
    assert "broker_only_open_order:external-1" in result.reasons
    assert database.get_runtime_metadata("block_new_entries") == "true"


def test_position_mismatch_does_not_import_or_close_positions(tmp_path):
    with connect_database(tmp_path / "position-mismatch.db") as database:
        database.init_schema()
        seed_order(database)
        result = reconcile_broker_state(
            database, FakeOrders([status(filled=3, remaining=7, state=KisOrderStatus.PARTIALLY_FILLED)]),
            FakeAccount([account_position(9)]), current_time=NOW,
        )
        local = database.list_open_live_positions("005930")

    assert result.operator_review is True
    assert any(reason.startswith("position_mismatch:") for reason in result.reasons)
    assert local[0]["quantity"] == 3
