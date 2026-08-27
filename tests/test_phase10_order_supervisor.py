import json
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

import pytest

from kis_ai_scalper.broker.kis_auth import KisHttpError
from kis_ai_scalper.broker.kis_order import KisOrderSide
from kis_ai_scalper.broker.kis_order_status import (
    KisCancelResult,
    KisOrderStatus,
    KisOrderStatusRecord,
)
from kis_ai_scalper.ops import order_supervisor as supervisor
from kis_ai_scalper.storage import connect_database


UTC = timezone.utc


@dataclass
class FakeOrderClient:
    orders: tuple[KisOrderStatusRecord, ...] = ()
    cancel_error: Exception | None = None

    def __post_init__(self):
        self.cancel_calls = []
        self.get_orders_calls = 0

    def get_today_orders(self):
        self.get_orders_calls += 1
        return self.orders

    def cancel_order(self, original_order_number, krx_forward_order_orgno, **kwargs):
        self.cancel_calls.append((original_order_number, krx_forward_order_orgno, kwargs))
        if self.cancel_error:
            raise self.cancel_error
        return KisCancelResult(
            order_number="cancel-ack",
            original_order_number=original_order_number,
            status="cancel_requested",
            tr_id="VTTC0013U",
            raw={},
        )


class FakeAccountClient:
    def get_snapshot(self):
        return type("Snapshot", (), {"positions": ()})()


def make_db(tmp_path, *, environment="demo", owner="owner-1"):
    path = tmp_path / "runtime.sqlite3"
    now = datetime.now(UTC)
    with connect_database(path) as database:
        database.init_schema()
        database.set_runtime_paused(True, "test", "test")
        if environment != "demo":
            database.set_runtime_environment(environment, "test", "test")
        assert database.acquire_service_lease("trading-service", owner, 60, now=now)
    return path, now


def noop_reconciliation(*args, **kwargs):
    return type(
        "Reconciliation",
        (),
        {"operator_review": False, "block_new_entries": False, "reasons": ()},
    )()


def noop_management(*args, **kwargs):
    return type(
        "Management",
        (),
        {"operator_review": False, "block_new_entries": False},
    )()


def management_with_action(reason="cancel_ambiguous"):
    return type(
        "Management",
        (),
        {
            "operator_review": True,
            "block_new_entries": True,
            "actions": (type("Action", (), {"reason": reason})(),),
        },
    )()


def broker_buy(*, status=KisOrderStatus.UNFILLED, remaining=10):
    return KisOrderStatusRecord(
        order_number="broker-1",
        symbol="005930",
        side=KisOrderSide.BUY,
        ordered_quantity=10,
        filled_quantity=0,
        remaining_quantity=remaining,
        order_price=100.0,
        average_fill_price=None,
        status=status,
        order_time="090000",
        order_date="20260818",
        order_branch="001",
        raw={},
    )


def seed_acknowledged_buy(path, now):
    with connect_database(path) as database:
        database.init_schema()
        assert database.claim_order_intent(
            client_order_id="client-1",
            signal_id="signal-1",
            symbol="005930",
            side="BUY",
            requested_qty=10,
            requested_price=100.0,
            created_at=now,
        )
        assert database.record_order_submission("client-1", "broker-1", submitted_at=now)
        database.set_runtime_metadata(supervisor.CANCEL_REQUEST_KEY, "true")


def test_paused_state_still_reconciles(tmp_path, monkeypatch):
    path, now = make_db(tmp_path)
    calls = []
    monkeypatch.setattr(
        supervisor,
        "reconcile_broker_state",
        lambda *args, **kwargs: calls.append("reconcile") or noop_reconciliation(),
    )
    monkeypatch.setattr(supervisor, "manage_stale_orders", lambda *args, **kwargs: noop_management())
    client = FakeOrderClient()
    result = supervisor.one_iteration(
        "config/settings.yaml",
        path,
        expected_owner_id="owner-1",
        client_factory=lambda environment, refresh_token=False: (client, FakeAccountClient()),
        now=now,
    )
    assert result.status == "reconciled"
    assert result.paused is True
    assert calls == ["reconcile"]


def test_one_iteration_reuses_one_broker_snapshot_for_reconcile_cancel_and_stale(tmp_path, monkeypatch):
    path, now = make_db(tmp_path)
    seed_acknowledged_buy(path, now)
    client = FakeOrderClient((broker_buy(),))

    result = supervisor.one_iteration(
        "config/settings.yaml",
        path,
        expected_owner_id="owner-1",
        client_factory=lambda environment, refresh_token=False: (client, FakeAccountClient()),
        now=now,
        buy_ttl_seconds=3600,
        sell_ttl_seconds=3600,
    )

    assert result.ok
    assert client.get_orders_calls == 1
    assert client.cancel_calls == [("broker-1", "001", {"quantity": 10, "order_price": 100})]


def test_normal_reconciled_is_silent_on_start_and_repeated_passes(tmp_path, monkeypatch):
    path, now = make_db(tmp_path)
    monkeypatch.setattr(supervisor, "reconcile_broker_state", lambda *a, **k: noop_reconciliation())
    monkeypatch.setattr(supervisor, "manage_stale_orders", lambda *a, **k: noop_management())
    messages = []
    state = supervisor.SupervisorState()
    factory = lambda environment, refresh_token=False: (FakeOrderClient(), FakeAccountClient())

    supervisor.one_iteration(
        "config/settings.yaml", path, notifier=messages.append, state=state,
        expected_owner_id="owner-1", client_factory=factory, now=now,
    )
    supervisor.one_iteration(
        "config/settings.yaml", path, notifier=messages.append, state=state,
        expected_owner_id="owner-1", client_factory=factory, now=now + timedelta(seconds=1),
    )

    assert messages == []
    with connect_database(path) as database:
        payload = json.loads(database.get_runtime_metadata(supervisor.STATUS_KEY))
    assert payload["status"] == "reconciled"
    assert payload["reasons"] == []


def test_dependency_review_backoff_and_three_healthy_iterations(tmp_path, monkeypatch):
    path, now = make_db(tmp_path)
    monkeypatch.setattr(supervisor, "reconcile_broker_state", lambda *a, **k: type(
        "Reconciliation", (), {
            "operator_review": True,
            "block_new_entries": True,
            "reasons": ("order_status_unavailable:KisHttpError:http_200:rt_cd_-1:msg_cd_EGW00123",),
        },
    )())
    monkeypatch.setattr(supervisor, "manage_stale_orders", lambda *a, **k: noop_management())
    messages = []
    state = supervisor.SupervisorState()

    result = supervisor.one_iteration(
        "config/settings.yaml", path, notifier=messages.append, state=state,
        expected_owner_id="owner-1",
        client_factory=lambda environment, refresh_token=False: (FakeOrderClient(), FakeAccountClient()),
        now=now,
    )

    assert result.status == "dependency_unavailable"
    with connect_database(path) as database:
        payload = json.loads(database.get_runtime_metadata(supervisor.STATUS_KEY))
    assert payload["status"] == "dependency_unavailable"
    assert payload["failure_streak"] == 1
    assert payload["healthy_streak"] == 0
    assert payload["next_retry_seconds"] == 5
    assert payload["safe_kis_error"] == {
        "http_status": "200",
        "rt_cd": "-1",
        "msg_cd": "EGW00123",
    }

    reports = iter([noop_reconciliation(), noop_reconciliation(), noop_reconciliation()])
    monkeypatch.setattr(supervisor, "reconcile_broker_state", lambda *a, **k: next(reports))
    for offset in range(1, 4):
        supervisor.one_iteration(
            "config/settings.yaml", path, notifier=messages.append, state=state,
            expected_owner_id="owner-1",
            client_factory=lambda environment, refresh_token=False: (FakeOrderClient(), FakeAccountClient()),
            now=now + timedelta(seconds=offset),
        )

    with connect_database(path) as database:
        assert database.get_runtime_metadata("operator_review") == "false"
        assert database.get_runtime_metadata("block_new_entries") == "false"
        recovered = json.loads(database.get_runtime_metadata(supervisor.STATUS_KEY))
    assert recovered["status"] == "reconciled"
    assert recovered["healthy_streak"] == 3
    assert any("recovered" in message for message in messages)


def test_successful_broker_reads_are_spaced_before_account_snapshot(tmp_path, monkeypatch):
    path, now = make_db(tmp_path)
    monkeypatch.setattr(supervisor, "reconcile_broker_state", lambda *a, **k: noop_reconciliation())
    monkeypatch.setattr(supervisor, "manage_stale_orders", lambda *a, **k: noop_management())
    sleeps = []

    result = supervisor.one_iteration(
        "config/settings.yaml",
        path,
        expected_owner_id="owner-1",
        client_factory=lambda environment, refresh_token=False: (FakeOrderClient(), FakeAccountClient()),
        now=now,
        broker_read_throttle_seconds=0.25,
        sleeper=sleeps.append,
    )

    assert result.status == "reconciled"
    assert sleeps == [0.25]


def test_kis_rate_limit_dependency_uses_longer_initial_backoff(tmp_path, monkeypatch):
    path, now = make_db(tmp_path)

    class RateLimitedAccountClient:
        def get_snapshot(self):
            raise KisHttpError(
                500,
                {"rt_cd": "1", "msg_cd": "EGW00201", "msg1": "초당 거래건수를 초과"},
            )

    def reconcile(*args, **kwargs):
        assert isinstance(kwargs["account_snapshot_error"], KisHttpError)
        return type(
            "Reconciliation",
            (),
            {
                "operator_review": True,
                "block_new_entries": True,
                "reasons": (
                    "account_snapshot_unavailable:KisHttpError:http_500:rt_cd_1:msg_cd_EGW00201",
                ),
            },
        )()

    monkeypatch.setattr(supervisor, "reconcile_broker_state", reconcile)
    monkeypatch.setattr(supervisor, "manage_stale_orders", lambda *a, **k: noop_management())

    result = supervisor.one_iteration(
        "config/settings.yaml",
        path,
        expected_owner_id="owner-1",
        client_factory=lambda environment, refresh_token=False: (
            FakeOrderClient(),
            RateLimitedAccountClient(),
        ),
        now=now,
    )

    assert result.status == "dependency_unavailable"
    with connect_database(path) as database:
        payload = json.loads(database.get_runtime_metadata(supervisor.STATUS_KEY))
    assert payload["next_retry_seconds"] == 15
    assert payload["safe_kis_error"] == {
        "http_status": "500",
        "rt_cd": "1",
        "msg_cd": "EGW00201",
    }


def test_operator_review_is_throttled_and_recovery_notifies_once(tmp_path, monkeypatch):
    path, now = make_db(tmp_path)
    reports = iter([
        type("Reconciliation", (), {"operator_review": True, "block_new_entries": True,
                                     "reasons": ("broker_order_missing:secret-order",)})(),
        noop_reconciliation(),
        noop_reconciliation(),
        noop_reconciliation(),
        noop_reconciliation(),
    ])
    monkeypatch.setattr(supervisor, "reconcile_broker_state", lambda *a, **k: next(reports))
    monkeypatch.setattr(supervisor, "manage_stale_orders", lambda *a, **k: noop_management())
    messages = []
    state = supervisor.SupervisorState()
    factory = lambda environment, refresh_token=False: (FakeOrderClient(), FakeAccountClient())

    for offset in range(5):
        supervisor.one_iteration(
            "config/settings.yaml", path, notifier=messages.append, state=state,
            expected_owner_id="owner-1", client_factory=factory,
            now=now + timedelta(seconds=offset),
        )

    assert len(messages) == 2
    assert "reconciled_operator_review" in messages[0]
    assert "secret-order" not in messages[0]
    assert "recovered" in messages[1]


def test_flapping_review_does_not_emit_recovery_or_repeat_warning(tmp_path, monkeypatch):
    path, now = make_db(tmp_path)
    review = type(
        "Reconciliation", (), {
            "operator_review": True,
            "block_new_entries": True,
            "reasons": ("account_snapshot_unavailable:TimeoutError",),
        },
    )()
    reports = iter([review, noop_reconciliation(), review, noop_reconciliation(), review])
    monkeypatch.setattr(supervisor, "reconcile_broker_state", lambda *a, **k: next(reports))
    monkeypatch.setattr(supervisor, "manage_stale_orders", lambda *a, **k: noop_management())
    messages = []
    state = supervisor.SupervisorState()
    factory = lambda environment, refresh_token=False: (FakeOrderClient(), FakeAccountClient())

    for offset in range(5):
        supervisor.one_iteration(
            "config/settings.yaml", path, notifier=messages.append, state=state,
            expected_owner_id="owner-1", client_factory=factory,
            now=now + timedelta(seconds=offset),
        )

    assert len(messages) == 1
    assert "dependency_unavailable" in messages[0]


def test_status_reasons_include_safe_reconciliation_stale_and_cancel_causes(tmp_path, monkeypatch):
    path, now = make_db(tmp_path)
    seed_acknowledged_buy(path, now)
    monkeypatch.setattr(
        supervisor,
        "reconcile_broker_state",
        lambda *a, **k: type(
            "Reconciliation", (), {"operator_review": True, "block_new_entries": True,
                                     "reasons": ("position_mismatch:005930:local=1:broker=2",)}
        )(),
    )
    monkeypatch.setattr(supervisor, "manage_stale_orders", lambda *a, **k: management_with_action())
    client = FakeOrderClient((broker_buy(),), cancel_error=TimeoutError("account=secret"))
    supervisor.one_iteration(
        "config/settings.yaml", path, expected_owner_id="owner-1",
        client_factory=lambda environment, refresh_token=False: (client, FakeAccountClient()), now=now,
    )

    with connect_database(path) as database:
        payload = json.loads(database.get_runtime_metadata(supervisor.STATUS_KEY))
    assert payload["reasons"] == [
        "reconciliation:position_mismatch",
        "stale_order:cancel_ambiguous",
        "cancel:TimeoutError",
    ]
    assert all("005930" not in reason and "secret" not in reason for reason in payload["reasons"])


def test_environment_change_rebuilds_clients(tmp_path, monkeypatch):
    path, now = make_db(tmp_path)
    monkeypatch.setattr(supervisor, "reconcile_broker_state", lambda *a, **k: noop_reconciliation())
    monkeypatch.setattr(supervisor, "manage_stale_orders", lambda *a, **k: noop_management())
    environments = []

    def factory(environment, refresh_token=False):
        environments.append(environment.value)
        return FakeOrderClient(), FakeAccountClient()

    state = supervisor.SupervisorState()
    first = supervisor.one_iteration(
        "config/settings.yaml", path, expected_owner_id="owner-1",
        client_factory=factory, state=state, now=now,
    )
    with connect_database(path) as database:
        database.set_runtime_environment("real", "test", "test")
    second = supervisor.one_iteration(
        "config/settings.yaml", path, expected_owner_id="owner-1",
        client_factory=factory, state=state, now=now + timedelta(seconds=1),
    )
    assert first.status == second.status == "reconciled"
    assert environments == ["demo", "real"]


def test_invalid_lease_makes_no_broker_call(tmp_path):
    path, _ = make_db(tmp_path, owner="actual-owner")
    called = []

    def factory(*args, **kwargs):
        called.append(True)
        raise AssertionError("broker clients must not be constructed")

    result = supervisor.one_iteration(
        "config/settings.yaml", path, expected_owner_id="wrong-owner", client_factory=factory
    )
    assert result.status == "lease_invalid"
    assert called == []
    with connect_database(path) as database:
        assert database.get_runtime_metadata(supervisor.LAST_ERROR_KEY) == "invalid_service_lease"


def test_exception_blocks_entries_and_records_sanitized_error(tmp_path, monkeypatch):
    path, _ = make_db(tmp_path)

    def fail(*args, **kwargs):
        raise RuntimeError("token=super-secret account=12345678")

    monkeypatch.setattr(supervisor, "reconcile_broker_state", fail)
    result = supervisor.one_iteration(
        "config/settings.yaml", path, expected_owner_id="owner-1",
        client_factory=lambda environment, refresh_token=False: (FakeOrderClient(), FakeAccountClient()),
    )
    assert result.status == "error"
    assert result.operator_review is True
    assert result.block_new_entries is True
    with connect_database(path) as database:
        assert database.get_runtime_metadata("operator_review") == "true"
        assert database.get_runtime_metadata("block_new_entries") == "true"
        assert database.get_runtime_metadata(supervisor.LAST_ERROR_KEY) == "RuntimeError"
        assert "super-secret" not in (database.get_runtime_metadata(supervisor.STATUS_KEY) or "")


@pytest.mark.parametrize(
    ("status_code", "refresh_expected"),
    [(403, False), (401, True)],
)
def test_auth_refresh_only_requested_for_401(tmp_path, monkeypatch, status_code, refresh_expected):
    path, _ = make_db(tmp_path)
    error = KisHttpError(
        status_code,
        {"rt_cd": "-1", "msg_cd": "EGW00123", "msg1": "auth failure"},
    )
    monkeypatch.setattr(supervisor, "reconcile_broker_state", lambda *a, **k: (_ for _ in ()).throw(error))

    state = supervisor.SupervisorState()
    result = supervisor.one_iteration(
        "config/settings.yaml",
        path,
        expected_owner_id="owner-1",
        client_factory=lambda environment, refresh_token=False: (FakeOrderClient(), FakeAccountClient()),
        state=state,
    )

    assert result.status == "error"
    assert result.auth_refresh_requested is refresh_expected
    assert state.force_refresh is refresh_expected


def test_kis_http_error_exposes_safe_metadata_but_runtime_error_does_not(tmp_path, monkeypatch):
    path, _ = make_db(tmp_path)
    kis_error = KisHttpError(
        403,
        {
            "rt_cd": "-1",
            "msg_cd": "EGW00123",
            "msg1": "forbidden request",
            "error_description": "rate limited",
        },
    )
    monkeypatch.setattr(supervisor, "reconcile_broker_state", lambda *a, **k: (_ for _ in ()).throw(kis_error))

    result = supervisor.one_iteration(
        "config/settings.yaml",
        path,
        expected_owner_id="owner-1",
        client_factory=lambda environment, refresh_token=False: (FakeOrderClient(), FakeAccountClient()),
    )
    assert result.error == (
        "KisHttpError: KIS HTTP 403 (rt_cd=-1 msg_cd=EGW00123 "
        "msg1=forbidden request error_description=rate limited)"
    )

    secret = "token=super-secret account=12345678"
    monkeypatch.setattr(
        supervisor,
        "reconcile_broker_state",
        lambda *a, **k: (_ for _ in ()).throw(RuntimeError(secret)),
    )
    runtime_result = supervisor.one_iteration(
        "config/settings.yaml",
        path,
        expected_owner_id="owner-1",
        client_factory=lambda environment, refresh_token=False: (FakeOrderClient(), FakeAccountClient()),
    )
    assert runtime_result.error == "RuntimeError"
    assert secret not in runtime_result.error


def test_retry_backoff_is_capped_and_interruptible(monkeypatch):
    waits = []
    event = threading.Event()
    result = supervisor.OrderSupervisorResult("error", error="RuntimeError")

    monkeypatch.setattr(supervisor, "one_iteration", lambda *args, **kwargs: result)
    original_wait = threading.Event.wait

    def fake_wait(self, timeout=None):
        waits.append(timeout)
        if len(waits) >= 8:
            event.set()
        return event.is_set()

    monkeypatch.setattr(threading.Event, "wait", fake_wait)
    try:
        supervisor.run_order_supervisor("config/settings.yaml", "db.sqlite3", event)
    finally:
        monkeypatch.setattr(threading.Event, "wait", original_wait)
    assert waits == [5.0, 10.0, 20.0, 40.0, 60.0, 60.0, 60.0, 60.0]


def test_stop_event_interrupts_long_interval():
    event = threading.Event()
    event.set()
    started = time.monotonic()
    result = supervisor.run_order_supervisor(
        "config/settings.yaml", "db.sqlite3", event, interval_seconds=60
    )
    assert result is None
    assert time.monotonic() - started < 1


def test_cancel_open_buys_consumes_flag_and_preserves_cancel_pending(tmp_path, monkeypatch):
    path, now = make_db(tmp_path)
    seed_acknowledged_buy(path, now)
    order_client = FakeOrderClient((broker_buy(),))
    monkeypatch.setattr(supervisor, "reconcile_broker_state", lambda *a, **k: noop_reconciliation())
    monkeypatch.setattr(supervisor, "manage_stale_orders", lambda *a, **k: noop_management())

    result = supervisor.one_iteration(
        "config/settings.yaml", path, expected_owner_id="owner-1",
        client_factory=lambda environment, refresh_token=False: (order_client, FakeAccountClient()),
        now=now,
    )
    assert result.cancel_requested == 1
    assert len(order_client.cancel_calls) == 1
    with connect_database(path) as database:
        assert database.get_runtime_metadata(supervisor.CANCEL_REQUEST_KEY) == "false"
        status = json.loads(database.get_runtime_metadata(supervisor.CANCEL_STATUS_KEY))
        assert status["status"] == "completed"
        assert status["requested"] == 1
        assert database.get_broker_order("client-1")["status"] == "CANCEL_PENDING"

    # A second pass does not re-submit the same cancel without a new operator request.
    second = supervisor.one_iteration(
        "config/settings.yaml", path, expected_owner_id="owner-1",
        client_factory=lambda environment, refresh_token=False: (order_client, FakeAccountClient()),
        now=now + timedelta(seconds=1),
    )
    assert second.cancel_requested == 0
    assert len(order_client.cancel_calls) == 1


def test_cancel_open_buys_error_is_sanitized_and_blocks_entries(tmp_path, monkeypatch):
    path, now = make_db(tmp_path)
    seed_acknowledged_buy(path, now)
    order_client = FakeOrderClient((broker_buy(),), cancel_error=TimeoutError("account=secret"))
    monkeypatch.setattr(supervisor, "reconcile_broker_state", lambda *a, **k: noop_reconciliation())
    monkeypatch.setattr(supervisor, "manage_stale_orders", lambda *a, **k: noop_management())

    result = supervisor.one_iteration(
        "config/settings.yaml", path, expected_owner_id="owner-1",
        client_factory=lambda environment, refresh_token=False: (order_client, FakeAccountClient()),
        now=now,
    )
    assert result.cancel_errors == 1
    with connect_database(path) as database:
        assert database.get_runtime_metadata(supervisor.CANCEL_REQUEST_KEY) == "false"
        status = json.loads(database.get_runtime_metadata(supervisor.CANCEL_STATUS_KEY))
        assert status["status"] == "error"
        assert status["error"] == "TimeoutError"
        assert database.get_runtime_metadata("operator_review") == "true"
        assert database.get_runtime_metadata("block_new_entries") == "true"
        assert "secret" not in database.get_runtime_metadata(supervisor.CANCEL_STATUS_KEY)
