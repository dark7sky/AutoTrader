import asyncio
from datetime import datetime, timezone

from kis_ai_scalper import cli
from kis_ai_scalper.market.collector import CollectorResult
from kis_ai_scalper.ops.telegram import env_value, handle_update
from kis_ai_scalper.pipeline import run_paper_session
from kis_ai_scalper.storage import connect_database


class FakeTelegram:
    def __init__(self):
        self.sent = []
        self.answered = []

    def send_message(self, chat_id, text, reply_markup=None):
        self.sent.append((str(chat_id), text, reply_markup))

    def answer_callback_query(self, callback_query_id, text=None):
        self.answered.append((callback_query_id, text))


def test_runtime_control_defaults_paused_and_round_trips(tmp_path):
    path = tmp_path / "control.sqlite3"
    with connect_database(path) as database:
        database.init_schema()
        assert database.get_runtime_control().paused is True
        database.set_runtime_paused(False, "operator_test", "test")
        assert database.get_runtime_control().paused is False
        database.set_runtime_paused(True, "maintenance", "telegram")
        control = database.get_runtime_control()
        assert (control.paused, control.reason, control.source) == (True, "maintenance", "telegram")


def test_paused_paper_session_skips_collector(tmp_path):
    calls = []

    async def collector(*args):
        calls.append(args)
        return CollectorResult("005930", 1, 1, True, 100.0)

    report = asyncio.run(run_paper_session(
        "fake", "approval", "005930", str(tmp_path / "paused.sqlite3"),
        iterations=2, collector=collector,
    ))
    assert calls == []
    assert report.exit_code == 3
    assert all(item.risk_reason == "runtime_paused" and item.exit_code == 3 for item in report.iterations)


def test_paused_paper_session_cli_skips_auth(tmp_path, monkeypatch, capsys):
    def fail_load_config(*args, **kwargs):
        raise AssertionError("paused paper-session should not load KIS config")

    monkeypatch.setattr("kis_ai_scalper.cli.load_config", fail_load_config)

    assert cli.main([
        "paper-session",
        "--config", str(tmp_path / "settings.yaml"),
        "--db", str(tmp_path / "paused.sqlite3"),
        "--iterations", "1",
        "--collect-seconds", "1",
    ]) == 3
    output = capsys.readouterr().out
    assert "risk_reason=runtime_paused" in output
    assert "broker_calls=none broker_orders=none account_queries=none ai_calls=none" in output


def test_cli_can_pause_and_resume_runtime(tmp_path, capsys):
    path = tmp_path / "control.sqlite3"

    assert cli.main(["control-resume", "--db", str(path), "--reason", "test_resume"]) == 0
    assert "paused=false" in capsys.readouterr().out
    assert cli.main(["control-pause", "--db", str(path), "--reason", "test_pause"]) == 0
    output = capsys.readouterr().out
    assert "paused=true" in output
    assert "test_pause" in output


def test_telegram_env_value_can_read_local_dotenv(tmp_path, monkeypatch):
    (tmp_path / ".env").write_text("TELEGRAM_BOT_TOKEN=token-from-dotenv\n", encoding="utf-8")
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("TELEGRAM_BOT_TOKEN", raising=False)

    assert env_value("TELEGRAM_BOT_TOKEN") == "token-from-dotenv"


def test_unauthorized_telegram_is_ignored(tmp_path):
    fake = FakeTelegram()
    handled = handle_update(
        {"update_id": 1, "message": {"chat": {"id": 99}, "text": "/resume"}},
        str(tmp_path / "control.sqlite3"), fake, "42",
    )
    assert handled is False
    assert fake.sent == []


def test_commands_send_status_and_report(tmp_path):
    path = tmp_path / "control.sqlite3"
    fake = FakeTelegram()
    update = lambda text: {"message": {"chat": {"id": 42}, "text": text}}
    assert handle_update(update("/resume"), str(path), fake, "42") is True
    assert "runtime resumed" in fake.sent[-1][1]
    assert handle_update(update("/status"), str(path), fake, "42") is True
    assert "runtime: running" in fake.sent[-1][1]
    assert handle_update(update("/report"), str(path), fake, "42") is True
    assert "paper report" in fake.sent[-1][1]


def test_callback_is_acknowledged_and_controls_pause(tmp_path):
    path = tmp_path / "control.sqlite3"
    fake = FakeTelegram()
    update = {"callback_query": {
        "id": "callback-1",
        "data": "control:pause",
        "message": {"chat": {"id": 42}},
    }}
    assert handle_update(update, str(path), fake, "42") is True
    assert fake.answered == [("callback-1", None)]
    with connect_database(path) as database:
        database.init_schema()
        assert database.get_runtime_control().paused is True


def test_telegram_environment_switch_is_pause_gated(tmp_path):
    path = tmp_path / "control.sqlite3"
    fake = FakeTelegram()
    update = lambda text: {"message": {"chat": {"id": 42}, "text": text}}

    assert handle_update(update("/env real"), str(path), fake, "42") is True
    assert "runtime environment: real" in fake.sent[-1][1]
    assert handle_update(update("/resume"), str(path), fake, "42") is True
    assert handle_update(update("/env demo"), str(path), fake, "42") is True
    assert "only be changed while paused" in fake.sent[-1][1]
    assert handle_update(update("/pause"), str(path), fake, "42") is True
    assert handle_update(update("/env demo"), str(path), fake, "42") is True

    with connect_database(path) as database:
        database.init_schema()
        assert database.get_runtime_control().environment == "demo"


def test_telegram_status_and_positions_show_leftovers(tmp_path):
    path = tmp_path / "control.sqlite3"
    fake = FakeTelegram()
    with connect_database(path) as database:
        database.init_schema()
        database.open_live_position(
            position_id="position-1", signal_id="signal-1", symbol="005930",
            quantity=2, entry_price=100, stop_loss_price=90,
            take_profit_price=110, opened_at=datetime.now(timezone.utc),
            entry_broker_order_id="broker-1",
        )
    update = lambda text: {"message": {"chat": {"id": 42}, "text": text}}
    assert handle_update(update("/status"), str(path), fake, "42") is True
    assert "environment: demo" in fake.sent[-1][1]
    assert "open_live_positions: 1" in fake.sent[-1][1]
    assert handle_update(update("/positions"), str(path), fake, "42") is True
    assert "live 005930 qty=2" in fake.sent[-1][1]
