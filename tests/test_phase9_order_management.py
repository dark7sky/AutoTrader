from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

import pytest

from kis_ai_scalper.broker.kis_order import KisOrderSide
from kis_ai_scalper.broker.kis_order_status import (
    KisCancelResult,
    KisOrderStatus,
    KisOrderStatusRecord,
)
from kis_ai_scalper.pipeline.order_management import (
    OrderManagementConfig,
    can_re_evaluate_entry,
    manage_stale_orders,
)
from kis_ai_scalper.market.clock import KST
from kis_ai_scalper.storage import connect_database


UTC = timezone.utc


@dataclass
class FakeStatusClient:
    orders: tuple[KisOrderStatusRecord, ...]
    cancel_error: Exception | None = None

    def __post_init__(self):
        self.cancel_calls = []

    def get_today_orders(self):
        return self.orders

    def cancel_order(self, original_order_number, krx_forward_order_orgno, **kwargs):
        self.cancel_calls.append((original_order_number, krx_forward_order_orgno, kwargs))
        if self.cancel_error is not None:
            raise self.cancel_error
        return KisCancelResult(
            order_number="cancel-ack",
            original_order_number=original_order_number,
            status="cancel_requested",
            tr_id="VTTC0013U",
            raw={},
        )


def broker_order(
    *,
    status=KisOrderStatus.UNFILLED,
    side=KisOrderSide.BUY,
    remaining=10,
    order_number="broker-1",
    branch="001",
    filled=0,
):
    return KisOrderStatusRecord(
        order_number=order_number,
        symbol="005930",
        side=side,
        ordered_quantity=10,
        filled_quantity=filled,
        remaining_quantity=remaining,
        order_price=100.0,
        average_fill_price=100.0 if filled else None,
        status=status,
        order_time="090000",
        order_date="20260818",
        order_branch=branch,
        raw={"rmn_qty": str(remaining)},
    )


def seed_order(database, *, side="BUY", submitted_at):
    assert database.claim_order_intent(
        client_order_id="client-1",
        signal_id="signal-1",
        symbol="005930",
        side=side,
        requested_qty=10,
        requested_price=100.0,
        created_at=submitted_at,
    )
    assert database.record_order_submission("client-1", "broker-1", submitted_at=submitted_at)


def test_stale_buy_cancel_is_requested_once_and_uses_remaining_qty(tmp_path):
    now = datetime(2026, 8, 18, 9, 2, tzinfo=KST)
    with connect_database(tmp_path / "orders.sqlite3") as database:
        database.init_schema()
        seed_order(database, submitted_at=now - timedelta(seconds=61))
        client = FakeStatusClient((broker_order(remaining=6),))

        first = manage_stale_orders(database, client, current_time=now)
        second = manage_stale_orders(database, client, current_time=now + timedelta(seconds=1))

        assert first.cancel_requested == 1
        assert second.cancel_requested == 0
        assert len(client.cancel_calls) == 1
        assert client.cancel_calls[0][2]["quantity"] == 6
        assert database.get_broker_order("client-1")["status"] == "CANCEL_PENDING"


def test_partial_fill_sell_uses_30_second_ttl_and_remaining_quantity(tmp_path):
    now = datetime(2026, 8, 18, 10, 0, tzinfo=KST)
    with connect_database(tmp_path / "orders.sqlite3") as database:
        database.init_schema()
        seed_order(database, side="SELL", submitted_at=now - timedelta(seconds=31))
        client = FakeStatusClient((broker_order(side=KisOrderSide.SELL, remaining=4, filled=6),))

        report = manage_stale_orders(database, client, current_time=now)

        assert report.cancel_requested == 1
        assert client.cancel_calls[0][2]["quantity"] == 4


def test_buy_is_cancelled_after_entry_window_even_before_ttl(tmp_path):
    now = datetime(2026, 8, 18, 15, 5, tzinfo=KST)
    with connect_database(tmp_path / "orders.sqlite3") as database:
        database.init_schema()
        seed_order(database, submitted_at=now - timedelta(seconds=1))
        client = FakeStatusClient((broker_order(),))

        report = manage_stale_orders(database, client, current_time=now)

        assert report.cancel_requested == 1
        assert report.actions[0].reason == "entry_window_closed"


def test_missing_branch_marks_unknown_and_blocks_entries(tmp_path):
    now = datetime(2026, 8, 18, 9, 2, tzinfo=KST)
    with connect_database(tmp_path / "orders.sqlite3") as database:
        database.init_schema()
        seed_order(database, submitted_at=now - timedelta(seconds=61))
        client = FakeStatusClient((broker_order(branch=None),))

        report = manage_stale_orders(database, client, current_time=now)

        assert report.unknown == 1
        assert client.cancel_calls == []
        assert database.get_broker_order("client-1")["status"] == "UNKNOWN"
        assert database.get_runtime_metadata("block_new_entries") == "true"
        assert database.get_runtime_metadata("operator_review") == "true"


def test_missing_broker_order_number_marks_unknown_and_blocks_entries(tmp_path):
    now = datetime(2026, 8, 18, 9, 2, tzinfo=KST)
    with connect_database(tmp_path / "orders.sqlite3") as database:
        database.init_schema()
        assert database.claim_order_intent(
            client_order_id="client-1",
            signal_id="signal-1",
            symbol="005930",
            side="BUY",
            requested_qty=10,
            requested_price=100.0,
            created_at=now - timedelta(seconds=61),
        )
        database.update_broker_order_status(
            "client-1", "ACKNOWLEDGED", updated_at=now - timedelta(seconds=61)
        )
        client = FakeStatusClient((broker_order(),))

        report = manage_stale_orders(database, client, current_time=now)

        assert report.unknown == 1
        assert client.cancel_calls == []
        assert database.get_broker_order("client-1")["status"] == "UNKNOWN"
        assert database.get_runtime_metadata("block_new_entries") == "true"


def test_cancel_timeout_becomes_unknown_and_is_not_retried(tmp_path):
    now = datetime(2026, 8, 18, 9, 2, tzinfo=KST)
    with connect_database(tmp_path / "orders.sqlite3") as database:
        database.init_schema()
        seed_order(database, submitted_at=now - timedelta(seconds=61))
        client = FakeStatusClient((broker_order(),), cancel_error=TimeoutError("timeout"))

        first = manage_stale_orders(database, client, current_time=now)
        second = manage_stale_orders(database, client, current_time=now + timedelta(seconds=1))

        assert first.unknown == 1
        assert second.cancel_requested == 0
        assert len(client.cancel_calls) == 1
        assert database.get_broker_order("client-1")["status"] == "UNKNOWN"


def test_cancel_pending_waits_for_reconciliation_then_requires_new_bar(tmp_path):
    now = datetime(2026, 8, 18, 9, 2, tzinfo=KST)
    with connect_database(tmp_path / "orders.sqlite3") as database:
        database.init_schema()
        seed_order(database, submitted_at=now - timedelta(seconds=61))
        client = FakeStatusClient((broker_order(),))
        manage_stale_orders(database, client, current_time=now, entry_bar_key="bar-1")

        pending_state = can_re_evaluate_entry(database, "005930", "bar-1")
        assert pending_state.allowed is False

        client.orders = (broker_order(status=KisOrderStatus.CANCELLED, remaining=0),)
        report = manage_stale_orders(database, client, current_time=now + timedelta(minutes=1))

        assert report.confirmed == 1
        assert database.get_broker_order("client-1")["status"] == "CANCELLED"
        assert can_re_evaluate_entry(database, "005930", "bar-1").allowed is False
        assert can_re_evaluate_entry(database, "005930", "bar-2").allowed is True
        assert len(client.cancel_calls) == 1


def test_market_closed_does_not_cancel(tmp_path):
    now = datetime(2026, 8, 16, 10, 0, tzinfo=UTC)
    with connect_database(tmp_path / "orders.sqlite3") as database:
        database.init_schema()
        seed_order(database, submitted_at=now - timedelta(minutes=5))
        client = FakeStatusClient((broker_order(),))

        report = manage_stale_orders(database, client, current_time=now)

        assert report.cancel_requested == 0
        assert client.cancel_calls == []
