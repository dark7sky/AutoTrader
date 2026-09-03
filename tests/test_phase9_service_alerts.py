from datetime import datetime, timedelta, timezone

import pytest

from kis_ai_scalper import cli
from kis_ai_scalper.pipeline.order_management import (
    OrderManagementAction,
    OrderManagementReport,
)
from kis_ai_scalper.risk.models import PortfolioState
from kis_ai_scalper.risk.portfolio_snapshot import PortfolioRiskSnapshot
from kis_ai_scalper.storage import connect_database


UTC = timezone.utc


def test_service_parser_removes_pause_bypass_and_defaults_collection_to_10(monkeypatch):
    monkeypatch.delenv("AUTO_TRADE_COLLECT_SECONDS", raising=False)
    args = cli.build_parser().parse_args(["service-loop"])

    assert args.collect_seconds == 10
    assert args.cycle_interval_seconds == 20
    assert not hasattr(args, "pause_on_start")
    with pytest.raises(SystemExit):
        cli.build_parser().parse_args(["service-loop", "--no-pause-on-start"])


def test_service_loop_auto_resumes_on_restart_without_emergency_stop(tmp_path, monkeypatch):
    db_path = tmp_path / "service.sqlite3"
    with connect_database(db_path) as database:
        database.init_schema()
        database.set_runtime_paused(False, "test", "test")

    def stop_preflight(*_args):
        raise KeyboardInterrupt

    monkeypatch.setattr(cli, "_telegram_notifier_from_env", lambda *_args: None)
    monkeypatch.setattr(cli, "_runtime_preflight", stop_preflight)

    with pytest.raises(KeyboardInterrupt):
        cli.service_loop(
            "config/settings.yaml", "005930", str(db_path), "rule", 1, 0,
            1, 0, 0, False,
        )

    with connect_database(db_path) as database:
        control = database.get_runtime_control()
        assert control.paused is False
        assert control.reason == "service_start_auto_resume"


def test_service_loop_preserves_emergency_stop_across_restart(tmp_path, monkeypatch):
    db_path = tmp_path / "service.sqlite3"
    with connect_database(db_path) as database:
        database.init_schema()
        database.set_runtime_paused(True, "telegram_emergency_stop", "telegram")
        database.set_runtime_metadata("telegram.emergency_stop", "true")

    def stop_preflight(*_args):
        raise KeyboardInterrupt

    monkeypatch.setattr(cli, "_telegram_notifier_from_env", lambda *_args: None)
    monkeypatch.setattr(cli, "_runtime_preflight", stop_preflight)

    with pytest.raises(KeyboardInterrupt):
        cli.service_loop(
            "config/settings.yaml", "005930", str(db_path), "rule", 1, 0,
            1, 0, 0, False,
        )

    with connect_database(db_path) as database:
        control = database.get_runtime_control()
        assert control.paused is True
        assert control.reason == "telegram_emergency_stop"


def test_service_cycle_error_sets_automatic_pause_without_operator_pause(tmp_path, monkeypatch):
    class InvalidMessage(Exception):
        pass

    db_path = tmp_path / "service.sqlite3"
    monkeypatch.setattr(cli, "_telegram_notifier_from_env", lambda *_args: None)
    monkeypatch.setattr(cli, "optional_env_value", lambda _name: None)
    monkeypatch.setattr(cli, "_runtime_preflight", lambda *_args: [])
    monkeypatch.setattr(cli, "is_regular_market_open", lambda *_args: True)
    monkeypatch.setattr(
        cli,
        "_collect_service_market_window",
        lambda *_args: (_ for _ in ()).throw(InvalidMessage()),
    )
    monkeypatch.setattr(cli, "run_order_supervisor", lambda **_kwargs: None)
    monkeypatch.setattr(cli, "_notify_operator_if_possible", lambda *args, **kwargs: False)
    monkeypatch.setattr(
        cli,
        "_sleep_remaining",
        lambda *_args: (_ for _ in ()).throw(KeyboardInterrupt()),
    )

    with pytest.raises(KeyboardInterrupt):
        cli.service_loop(
            "config/settings.yaml", "005930", str(db_path), "rule", 1, 1,
            1, 0, 0, False,
        )

    with connect_database(db_path) as database:
        assert database.get_runtime_control().paused is False
        assert database.get_runtime_metadata("runtime.auto_paused") == "true"
        assert database.get_runtime_metadata("runtime.auto_pause_reason") == (
            "service_cycle_error:InvalidMessage"
        )


def test_service_skips_duplicate_reads_while_supervisor_dependency_is_unavailable(
    tmp_path, monkeypatch,
):
    db_path = tmp_path / "dependency-gate.sqlite3"
    with connect_database(db_path) as database:
        database.init_schema()
        database.set_runtime_metadata(
            "order-supervisor.status",
            '{"status":"dependency_unavailable","reasons":["order_status_unavailable:ReadTimeout"]}',
        )
        database.set_runtime_metadata("block_new_entries", "true")

    broker_calls = []
    monkeypatch.setattr(cli, "_telegram_notifier_from_env", lambda *_args: None)
    monkeypatch.setattr(cli, "optional_env_value", lambda _name: None)
    monkeypatch.setattr(cli, "_runtime_preflight", lambda *_args: [])
    monkeypatch.setattr(cli, "is_regular_market_open", lambda *_args: True)
    monkeypatch.setattr(cli, "run_order_supervisor", lambda **_kwargs: None)
    monkeypatch.setattr(
        cli,
        "_collect_service_market_window",
        lambda *_args: broker_calls.append("collect"),
    )
    monkeypatch.setattr(
        cli,
        "_make_broker_cycle_state",
        lambda *_args, **_kwargs: broker_calls.append("broker-state"),
    )
    monkeypatch.setattr(
        cli,
        "_sleep_remaining",
        lambda *_args: (_ for _ in ()).throw(KeyboardInterrupt()),
    )

    with pytest.raises(KeyboardInterrupt):
        cli.service_loop(
            "config/settings.yaml", "005930", str(db_path), "rule", 1, 1,
            1, 0, 0, False,
        )

    assert broker_calls == []
    with connect_database(db_path) as database:
        assert database.get_runtime_metadata("runtime.auto_paused") == "true"
        assert database.get_runtime_metadata("runtime.auto_pause_reason") == (
            "supervisor_dependency_unavailable"
        )


def test_preflight_alert_throttle_persists_across_restart_and_changes_alert_immediately(
    tmp_path, monkeypatch,
):
    db_path = tmp_path / "alerts.sqlite3"
    sent = []
    def record_alert(message):
        sent.append(message)
        return True

    monkeypatch.setattr(cli, "_notify_operator_if_possible", record_alert)
    first = datetime(2026, 8, 16, 1, 0, tzinfo=UTC)
    same_message = "auto-trade paused: setup issue\n- config: missing"
    changed_message = "auto-trade paused: setup issue\n- config: malformed"

    with connect_database(db_path) as database:
        database.init_schema()
        assert cli._send_throttled_service_alert(
            database, same_message, now=first,
            fingerprint_key=cli.PREFLIGHT_ALERT_FINGERPRINT_KEY,
            sent_at_key=cli.PREFLIGHT_ALERT_AT_KEY,
        ) is True
        assert cli._send_throttled_service_alert(
            database, same_message, now=first + timedelta(minutes=5),
            fingerprint_key=cli.PREFLIGHT_ALERT_FINGERPRINT_KEY,
            sent_at_key=cli.PREFLIGHT_ALERT_AT_KEY,
        ) is False

    with connect_database(db_path) as database:
        database.init_schema()
        assert cli._send_throttled_service_alert(
            database, same_message, now=first + timedelta(minutes=10),
            fingerprint_key=cli.PREFLIGHT_ALERT_FINGERPRINT_KEY,
            sent_at_key=cli.PREFLIGHT_ALERT_AT_KEY,
        ) is False
        assert cli._send_throttled_service_alert(
            database, changed_message, now=first + timedelta(minutes=10),
            fingerprint_key=cli.PREFLIGHT_ALERT_FINGERPRINT_KEY,
            sent_at_key=cli.PREFLIGHT_ALERT_AT_KEY,
        ) is True
        assert cli._send_throttled_service_alert(
            database, changed_message, now=first + timedelta(minutes=26),
            fingerprint_key=cli.PREFLIGHT_ALERT_FINGERPRINT_KEY,
            sent_at_key=cli.PREFLIGHT_ALERT_AT_KEY,
        ) is True

    assert sent == [same_message, changed_message, changed_message]


def test_order_management_alert_is_sanitized_and_throttled_separately(tmp_path, monkeypatch):
    db_path = tmp_path / "order-alerts.sqlite3"
    sent = []
    def record_alert(message):
        sent.append(message)
        return True

    monkeypatch.setattr(cli, "_notify_operator_if_possible", record_alert)
    now = datetime(2026, 8, 16, 1, 0, tzinfo=UTC)
    report = OrderManagementReport(actions=(OrderManagementAction(
        "client-order-secret", "005930", "CANCEL_PENDING", "entry_window_closed", 7,
    ),))

    with connect_database(db_path) as database:
        database.init_schema()
        assert cli._notify_order_management_alert(database, report, now=now) is True
        assert cli._notify_order_management_alert(
            database, report, now=now + timedelta(minutes=1),
        ) is False
        assert cli._notify_order_management_alert(
            database,
            OrderManagementReport(operator_review=True),
            now=now + timedelta(minutes=1),
        ) is True

    assert len(sent) == 2
    assert "client-order-secret" not in sent[0]
    assert "remaining_quantity" not in sent[0]
    assert "7" not in sent[0]
    assert "CANCEL_PENDING" in sent[0]
    assert "OPERATOR_REVIEW" in sent[1]


@pytest.mark.parametrize("failure", [False, True])
def test_alert_metadata_is_recorded_only_after_telegram_send(tmp_path, monkeypatch, failure):
    db_path = tmp_path / "delivery.sqlite3"
    now = datetime(2026, 8, 16, 1, 0, tzinfo=UTC)
    message = "auto-trade paused: setup issue\n- config: missing"

    def fake_send(_message):
        if failure:
            return False
        return True

    monkeypatch.setattr(cli, "_notify_operator_if_possible", fake_send)
    with connect_database(db_path) as database:
        database.init_schema()
        expected = not failure
        assert cli._send_throttled_service_alert(
            database, message, now=now,
            fingerprint_key=cli.PREFLIGHT_ALERT_FINGERPRINT_KEY,
            sent_at_key=cli.PREFLIGHT_ALERT_AT_KEY,
        ) is expected
        if failure:
            assert database.get_runtime_metadata(cli.PREFLIGHT_ALERT_FINGERPRINT_KEY) is None
            assert database.get_runtime_metadata(cli.PREFLIGHT_ALERT_AT_KEY) is None
        else:
            assert database.get_runtime_metadata(cli.PREFLIGHT_ALERT_FINGERPRINT_KEY)
            assert database.get_runtime_metadata(cli.PREFLIGHT_ALERT_AT_KEY) == now.isoformat()


def test_operator_notification_reports_missing_or_failed_telegram(monkeypatch):
    monkeypatch.setattr(cli, "_telegram_notifier_from_env", lambda: None)
    assert cli._notify_operator_if_possible("message") is False

    class FailingNotifier:
        def send(self, _message):
            raise RuntimeError("network down")

    monkeypatch.setattr(cli, "_telegram_notifier_from_env", lambda: FailingNotifier())
    assert cli._notify_operator_if_possible("message") is False


def test_auto_trade_uses_time_after_stream_collection(tmp_path, monkeypatch):
    db_path = tmp_path / "cycle-time.sqlite3"
    with connect_database(db_path) as database:
        database.init_schema()
        database.set_runtime_paused(False, "test", "test")

    before_collection = datetime(2026, 8, 16, 1, 0, tzinfo=UTC)
    after_collection = before_collection + timedelta(seconds=50)
    times = iter((before_collection, after_collection))
    captured = {}

    class FakeConfig:
        def kis_api_for(self, _environment):
            return type("Api", (), {"app_key": "key", "app_secret": "secret"})()

        def kis_account_for(self, _environment):
            return type("Account", (), {"account_no": "12345678", "account_product_code": "01"})()

    class FakeAuth:
        def __init__(self, *_args, **_kwargs):
            pass

        def authenticate_read_only(self, **_kwargs):
            return type("Auth", (), {"access_token": "token", "approval_key": "approval"})()

    class FakeCollector:
        def __init__(self, **_kwargs):
            pass

        async def run(self, *, deadline):
            captured["deadline"] = deadline

    monkeypatch.setattr(cli, "kst_now", lambda: next(times))
    monkeypatch.setattr(cli, "_lease_is_active", lambda *_args, **_kwargs: False)
    monkeypatch.setattr(cli, "exchange_calendar_available", lambda: True)
    monkeypatch.setattr(cli, "is_regular_market_open", lambda _now: True)
    monkeypatch.setattr(cli, "load_config", lambda _path: FakeConfig())
    monkeypatch.setattr(cli, "_assert_broker_order_allowed", lambda *_args: None)
    monkeypatch.setattr(cli, "KisAuthClient", FakeAuth)
    monkeypatch.setattr(cli, "KisOrderClient", lambda *_args, **_kwargs: object())
    monkeypatch.setattr(cli, "websocket_url", lambda _environment: "wss://example.test")
    monkeypatch.setattr(cli, "StreamingCollector", FakeCollector)
    monkeypatch.setattr(cli, "run_auto_trade_cycle", lambda *args, **kwargs: (
        captured.update(current_time=kwargs["current_time"])
        or type("Report", (), {"submitted_count": 0, "results": (), "ai_call_count": 0})()
    ))

    assert cli.auto_trade_cycle(
        "config/settings.yaml", "demo", "005930", str(db_path), "rule", 1, 50,
        "AUTO_TRADE", False,
        portfolio=PortfolioRiskSnapshot(PortfolioState()),
        buying_power_client=object(),
    ) == 0
    assert captured["current_time"] == after_collection


@pytest.mark.parametrize("gate_change", ["pause", "emergency", "environment"])
def test_auto_trade_rechecks_runtime_gate_after_collection(tmp_path, monkeypatch, gate_change):
    db_path = tmp_path / f"gate-{gate_change}.sqlite3"
    with connect_database(db_path) as database:
        database.init_schema()
        database.set_runtime_paused(False, "test", "test")

    class FakeConfig:
        def kis_api_for(self, _environment):
            return type("Api", (), {"app_key": "key", "app_secret": "secret"})()

        def kis_account_for(self, _environment):
            return type("Account", (), {"account_no": "12345678", "account_product_code": "01"})()

    class FakeAuth:
        def __init__(self, *_args, **_kwargs):
            pass

        def authenticate_read_only(self, **_kwargs):
            return type("Auth", (), {"access_token": "token", "approval_key": "approval"})()

    class FakeCollector:
        def __init__(self, **_kwargs):
            pass

        async def run(self, *, deadline):
            with connect_database(db_path) as database:
                database.init_schema()
                if gate_change == "pause":
                    database.set_runtime_paused(True, "telegram_operator", "telegram")
                elif gate_change == "emergency":
                    database.set_runtime_metadata("emergency_stop", "true")
                else:
                    database.set_runtime_paused(True, "telegram_operator", "telegram")
                    database.set_runtime_environment("real", "telegram_operator", "telegram")

    called = []
    monkeypatch.setattr(cli, "kst_now", lambda: datetime(2026, 8, 16, 1, 0, tzinfo=UTC))
    monkeypatch.setattr(cli, "_lease_is_active", lambda *_args, **_kwargs: False)
    monkeypatch.setattr(cli, "exchange_calendar_available", lambda: True)
    monkeypatch.setattr(cli, "is_regular_market_open", lambda _now: True)
    monkeypatch.setattr(cli, "load_config", lambda _path: FakeConfig())
    monkeypatch.setattr(cli, "_assert_broker_order_allowed", lambda *_args: None)
    monkeypatch.setattr(cli, "KisAuthClient", FakeAuth)
    monkeypatch.setattr(cli, "KisOrderClient", lambda *_args, **_kwargs: object())
    monkeypatch.setattr(cli, "websocket_url", lambda _environment: "wss://example.test")
    monkeypatch.setattr(cli, "StreamingCollector", FakeCollector)
    monkeypatch.setattr(cli, "run_auto_trade_cycle", lambda *args, **kwargs: called.append(kwargs))

    result = cli.auto_trade_cycle(
        "config/settings.yaml", "demo", "005930", str(db_path), "rule", 1, 50,
        "AUTO_TRADE", False,
        portfolio=PortfolioRiskSnapshot(PortfolioState()),
        buying_power_client=object(),
    )

    assert result == 3
    assert called == []
