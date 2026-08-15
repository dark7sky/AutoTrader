from datetime import datetime

import pytest

from kis_ai_scalper.broker.kis_order import KisOrderRequest, KisOrderResult, KisOrderSide, KisOrderType
from kis_ai_scalper.execution.guarded_submitter import GuardedSubmitter, OrderSafetyGateError
from kis_ai_scalper.storage import connect_database


NOW = datetime(2026, 8, 18, 10, 0)
SYMBOL = "005930"


class RecordingSubmitter:
    environment = "demo"

    def __init__(self):
        self.requests = []

    def submit_order(self, request):
        self.requests.append(request)
        return KisOrderResult(
            request.symbol,
            KisOrderSide(request.side),
            request.quantity,
            request.price,
            KisOrderType(request.order_type),
            "VTTC0012U" if KisOrderSide(request.side) is KisOrderSide.BUY else "VTTC0011U",
            "broker-1",
            {},
        )


def setup_runtime(path, *, paused=False, environment="demo", owner="owner-1"):
    with connect_database(path) as database:
        database.init_schema()
        database.set_runtime_paused(paused, "test", "test")
        if environment != "demo":
            database.set_runtime_paused(True, "test-env", "test")
            database.set_runtime_environment(environment, "test-env", "test")
            database.set_runtime_paused(paused, "test", "test")
        assert database.acquire_service_lease(
            "trading-service", owner, 120, now=datetime(2026, 8, 18, 9, 59)
        )


def make_guard(path, submitter=None, *, environment="demo", owner="owner-1"):
    return GuardedSubmitter(
        submitter or RecordingSubmitter(),
        path,
        environment,
        owner,
        now_fn=lambda: NOW,
    )


def request(side, quantity=1):
    return KisOrderRequest(SYMBOL, side, quantity, 70_000)


def test_pause_race_is_rechecked_from_fresh_sqlite_connection(tmp_path):
    path = tmp_path / "runtime.sqlite3"
    setup_runtime(path)
    submitter = RecordingSubmitter()
    guard = make_guard(path, submitter)

    with connect_database(path) as database:
        database.init_schema()
        database.set_runtime_paused(True, "telegram_operator", "telegram")

    with pytest.raises(OrderSafetyGateError, match="runtime_paused"):
        guard.submit_order(request(KisOrderSide.BUY))
    assert submitter.requests == []


def test_runtime_environment_mismatch_blocks_without_submission(tmp_path):
    path = tmp_path / "runtime.sqlite3"
    setup_runtime(path, environment="real")
    submitter = RecordingSubmitter()

    with pytest.raises(OrderSafetyGateError, match="runtime_environment_mismatch"):
        make_guard(path, submitter, environment="demo").submit_order(request("buy"))
    assert submitter.requests == []


@pytest.mark.parametrize("metadata_key", ["emergency_stop", "telegram.emergency_stop"])
def test_emergency_stop_blocks_both_metadata_keys(tmp_path, metadata_key):
    path = tmp_path / "runtime.sqlite3"
    setup_runtime(path)
    with connect_database(path) as database:
        database.init_schema()
        database.set_runtime_metadata(metadata_key, "true")
    submitter = RecordingSubmitter()

    with pytest.raises(OrderSafetyGateError, match="emergency_stop"):
        make_guard(path, submitter).submit_order(request("buy"))
    assert submitter.requests == []


def test_buy_is_blocked_when_operator_review_or_new_entries_flag_is_set(tmp_path):
    path = tmp_path / "runtime.sqlite3"
    setup_runtime(path)
    with connect_database(path) as database:
        database.init_schema()
        database.set_runtime_metadata("operator_review", "true")
        database.set_runtime_metadata("block_new_entries", "true")
    submitter = RecordingSubmitter()

    with pytest.raises(OrderSafetyGateError, match="operator_review"):
        make_guard(path, submitter).submit_order(request("buy"))
    assert submitter.requests == []


def test_sell_quantity_cannot_exceed_known_local_open_position(tmp_path):
    path = tmp_path / "runtime.sqlite3"
    setup_runtime(path)
    with connect_database(path) as database:
        database.init_schema()
        assert database.open_live_position(
            position_id="position-1",
            signal_id="signal-1",
            symbol=SYMBOL,
            quantity=2,
            entry_price=69_000,
            stop_loss_price=68_000,
            take_profit_price=71_000,
            opened_at=datetime(2026, 8, 18, 9, 30),
            entry_broker_order_id="entry-1",
        )
    submitter = RecordingSubmitter()

    with pytest.raises(OrderSafetyGateError, match="sell_quantity_exceeds_local_position"):
        make_guard(path, submitter).submit_order(request("sell", 3))
    assert submitter.requests == []


def test_normal_order_is_submitted_once(tmp_path):
    path = tmp_path / "runtime.sqlite3"
    setup_runtime(path)
    submitter = RecordingSubmitter()

    result = make_guard(path, submitter).submit_order(request("buy"))

    assert result.broker_order_id == "broker-1"
    assert len(submitter.requests) == 1


def test_unresolved_prior_buy_blocks_new_buy(tmp_path):
    path = tmp_path / "runtime.sqlite3"
    setup_runtime(path)
    with connect_database(path) as database:
        database.init_schema()
        assert database.claim_order_intent(
            client_order_id="prior-buy",
            signal_id="prior-signal",
            symbol=SYMBOL,
            side="BUY",
            requested_qty=1,
            requested_price=70_000,
            created_at=NOW,
        )
        assert database.record_order_submission("prior-buy", "broker-prior", submitted_at=NOW)
    submitter = RecordingSubmitter()

    with pytest.raises(OrderSafetyGateError, match="unresolved_buy_order"):
        make_guard(path, submitter).submit_order(request("buy"))
    assert submitter.requests == []
