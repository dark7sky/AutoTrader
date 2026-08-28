from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

import kis_ai_scalper.cli as cli
from kis_ai_scalper.ops.telegram import (
    CANCEL_OPEN_BUYS_KEY,
    EMERGENCY_STOP_KEY,
    handle_update,
)
from kis_ai_scalper.storage import connect_database


class FakeTelegram:
    def __init__(self) -> None:
        self.sent: list[tuple[str, str, object]] = []

    def send_message(self, chat_id, text, reply_markup=None):
        self.sent.append((str(chat_id), str(text), reply_markup))


def private(text: str, chat_id: int = 42) -> dict:
    return {
        "message": {
            "chat": {"id": chat_id, "type": "private"},
            "text": text,
        }
    }


def seed_acknowledged_buy(path: Path) -> None:
    with connect_database(str(path)) as database:
        database.init_schema()
        database.claim_order_intent(
            client_order_id="client-buy-1",
            signal_id="signal-buy-1",
            symbol="005930",
            side="BUY",
            requested_qty=2,
            requested_price=100,
        )
        database.record_order_submission("client-buy-1", "broker-buy-1")
        assert database.get_broker_order("client-buy-1")["status"] == "ACKNOWLEDGED"


def _patch_service_dependencies(monkeypatch, *, preflight_errors, worker_calls):
    monkeypatch.setattr(cli, "_telegram_notifier_from_env", lambda *_args: None)
    monkeypatch.setattr(cli, "optional_env_value", lambda name: None)
    monkeypatch.setattr(cli, "_runtime_preflight", lambda *args: list(preflight_errors))
    monkeypatch.setattr(cli, "_sleep_remaining", lambda *args: (_ for _ in ()).throw(KeyboardInterrupt))
    monkeypatch.setattr(cli, "_notify_operator_if_possible", lambda *args: False)

    def fake_fill_notice_worker(**kwargs):
        worker_calls["fill"].append(kwargs)

    def fake_order_supervisor(**kwargs):
        worker_calls["order"].append(kwargs)

    monkeypatch.setattr(cli, "run_fill_notice_worker", fake_fill_notice_worker)
    monkeypatch.setattr(cli, "run_order_supervisor", fake_order_supervisor)


def test_service_starts_workers_only_after_successful_preflight(tmp_path, monkeypatch):
    config_path = tmp_path / "config" / "settings.yaml"
    config_path.parent.mkdir()
    config_path.write_text("{}\n", encoding="utf-8")
    db_path = tmp_path / "service.sqlite3"
    worker_calls = {"fill": [], "order": []}
    _patch_service_dependencies(monkeypatch, preflight_errors=["missing credential"], worker_calls=worker_calls)

    with pytest.raises(KeyboardInterrupt):
        cli.service_loop(
            str(config_path), None, str(db_path), "rule", 1, 0, 1, 10, 0, True,
        )

    assert worker_calls == {"fill": [], "order": []}


def test_service_passes_owner_to_supervisor_and_joins_workers(tmp_path, monkeypatch):
    config_path = tmp_path / "config" / "settings.yaml"
    config_path.parent.mkdir()
    config_path.write_text("{}\n", encoding="utf-8")
    db_path = tmp_path / "service.sqlite3"
    worker_calls = {"fill": [], "order": []}
    _patch_service_dependencies(monkeypatch, preflight_errors=[], worker_calls=worker_calls)
    joins: list[tuple[str, float | None]] = []
    original_thread = cli.threading.Thread

    class TrackingThread(original_thread):
        def join(self, timeout=None):
            joins.append((self.name, timeout))
            return super().join(timeout)

    monkeypatch.setattr(cli.threading, "Thread", TrackingThread)

    with pytest.raises(KeyboardInterrupt):
        cli.service_loop(
            str(config_path), None, str(db_path), "rule", 1, 0, 1, 10, 0, True,
        )

    assert len(worker_calls["fill"]) == 1
    assert len(worker_calls["order"]) == 1
    owner_id = worker_calls["order"][0]["expected_owner_id"]
    assert isinstance(owner_id, str) and owner_id
    assert worker_calls["order"][0]["broker_read_throttle_seconds"] == 0.25
    assert worker_calls["fill"][0]["stop_event"].is_set()
    assert worker_calls["order"][0]["stop_event"].is_set()
    assert {name for name, _ in joins} == {"fill-notice", "order-supervisor"}


def test_service_reserves_single_websocket_for_market_stream(tmp_path, monkeypatch):
    config_path = tmp_path / "config" / "settings.yaml"
    config_path.parent.mkdir()
    config_path.write_text("{}\n", encoding="utf-8")
    db_path = tmp_path / "service.sqlite3"
    worker_calls = {"fill": [], "order": []}
    _patch_service_dependencies(monkeypatch, preflight_errors=[], worker_calls=worker_calls)

    with pytest.raises(KeyboardInterrupt):
        cli.service_loop(
            str(config_path), None, str(db_path), "rule", 1, 65, 1, 10, 0, True,
        )

    assert worker_calls["fill"] == []
    assert len(worker_calls["order"]) == 1
    with connect_database(str(db_path)) as database:
        assert database.get_runtime_metadata("fill-notice:status") == "rest_reconciliation"


def test_emergency_stop_pauses_and_requests_cancel_for_known_buy(tmp_path):
    path = tmp_path / "telegram.sqlite3"
    seed_acknowledged_buy(path)
    fake = FakeTelegram()

    assert handle_update(private("/emergency-stop"), str(path), fake, "42") is True

    with connect_database(str(path)) as database:
        assert database.get_runtime_control().paused is True
        assert database.get_runtime_metadata(EMERGENCY_STOP_KEY) == "true"
        assert database.get_runtime_metadata(CANCEL_OPEN_BUYS_KEY) == "true"


def test_cancel_open_buys_without_order_leaves_request_false(tmp_path):
    path = tmp_path / "telegram.sqlite3"
    fake = FakeTelegram()

    assert handle_update(private("/cancel-open-buys"), str(path), fake, "42") is True

    with connect_database(str(path)) as database:
        assert database.get_runtime_control().paused is True
        assert database.get_runtime_metadata(CANCEL_OPEN_BUYS_KEY) == "false"


def test_resume_is_blocked_while_cancel_request_is_pending(tmp_path):
    path = tmp_path / "telegram.sqlite3"
    fake = FakeTelegram()
    with connect_database(str(path)) as database:
        database.init_schema()
        database.set_runtime_paused(True, "test", "test")
        database.set_runtime_metadata(CANCEL_OPEN_BUYS_KEY, "true")

    assert handle_update(private("/resume"), str(path), fake, "42") is True
    assert "cancellation is still pending" in fake.sent[-1][1]
    with connect_database(str(path)) as database:
        assert database.get_runtime_control().paused is True


def test_status_reports_worker_states_without_exposing_raw_metadata(tmp_path):
    path = tmp_path / "telegram.sqlite3"
    fake = FakeTelegram()
    now = datetime.now(timezone.utc).isoformat()
    with connect_database(str(path)) as database:
        database.init_schema()
        database.record_heartbeat("trading-service", heartbeat_at=datetime.now(timezone.utc))
        database.record_heartbeat("fill-notice", heartbeat_at=datetime.now(timezone.utc))
        database.record_heartbeat("order-supervisor", heartbeat_at=datetime.now(timezone.utc))
        database.set_runtime_metadata(
            "fill-notice:status",
            json.dumps({"status": "connected", "token": "secret-token"}),
        )
        database.set_runtime_metadata(
            "order-supervisor.status",
            json.dumps({"status": "reconciled", "account": "secret-account"}),
        )
        database.set_runtime_metadata("status-test-timestamp", now)

    assert handle_update(private("/status"), str(path), fake, "42") is True
    text = fake.sent[-1][1]
    assert "fill_notice: connected" in text
    assert "order_supervisor: reconciled" in text
    assert "secret-token" not in text
    assert "secret-account" not in text
