import asyncio
from datetime import datetime, timezone

from kis_ai_scalper import cli
from kis_ai_scalper.ops import telegram as telegram_module
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

    def set_my_commands(self, commands):
        self.commands = commands

    def set_chat_menu_button(self, chat_id, menu_button=None):
        self.chat_menu_button = (chat_id, menu_button)


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


def test_commands_send_status_and_report(tmp_path, monkeypatch):
    path = tmp_path / "control.sqlite3"
    fake = FakeTelegram()
    _ready_demo_env(monkeypatch)
    _ready_demo_db(path)
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


def test_start_and_menu_send_main_menu_keyboard(tmp_path):
    path = tmp_path / "control.sqlite3"
    fake = FakeTelegram()
    expected = telegram_module.MAIN_MENU_KEYBOARD
    update = lambda text: {"message": {"chat": {"id": 42}, "text": text}}

    assert handle_update(update("/start"), str(path), fake, "42") is True
    assert fake.sent[-1][2] == expected
    assert handle_update(update("/menu"), str(path), fake, "42") is True
    assert fake.sent[-1][2] == expected


def test_menu_callbacks_render_submenus_with_main_return_button(tmp_path):
    path = tmp_path / "control.sqlite3"
    fake = FakeTelegram()
    for callback_name in ("status", "trading", "control", "environment", "ai"):
        update = {"callback_query": {
            "id": f"menu-{callback_name}",
            "from": {"id": 42},
            "data": f"menu:{callback_name}",
            "message": {"chat": {"id": 42, "type": "private"}},
        }}
        assert handle_update(update, str(path), fake, "42") is True
        markup = fake.sent[-1][2]
        callbacks = [
            button["callback_data"]
            for row in markup["inline_keyboard"]
            for button in row
        ]
        assert "menu:main" in callbacks


def test_menu_callback_requires_authorization(tmp_path):
    fake = FakeTelegram()
    update = {"callback_query": {
        "id": "unauthorized-menu",
        "from": {"id": 99},
        "data": "menu:status",
        "message": {"chat": {"id": 42, "type": "private"}},
    }}
    assert handle_update(update, str(tmp_path / "control.sqlite3"), fake, "42", "42") is False
    assert fake.sent == []
    assert fake.answered == []


def test_telegram_environment_switch_is_pause_gated(tmp_path, monkeypatch):
    path = tmp_path / "control.sqlite3"
    fake = FakeTelegram()
    _ready_demo_env(monkeypatch)
    _ready_demo_db(path)
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


def _ready_demo_env(monkeypatch):
    monkeypatch.setenv("LIVE_TRADING_ENABLED", "true")
    monkeypatch.setenv("AUTO_TRADE_AI", "rule")
    monkeypatch.setenv("KIS_DEMO_APP_KEY", "demo-key")
    monkeypatch.setenv("KIS_DEMO_APP_SECRET", "demo-secret")
    monkeypatch.setenv("KIS_DEMO_ACCOUNT_NO", "12345678")
    monkeypatch.setattr(telegram_module, "exchange_calendar_available", lambda: True)


def _ready_demo_db(path):
    with connect_database(path) as database:
        database.init_schema()
        database.add_watchlist_symbol("005930")
        database.record_heartbeat("trading-service")
        database.record_heartbeat("order-supervisor")


def test_readiness_reports_blockers_and_market_closed_is_not_one(tmp_path, monkeypatch):
    path = str(tmp_path / "control.sqlite3")
    fake = FakeTelegram()
    _ready_demo_env(monkeypatch)
    monkeypatch.setattr(telegram_module, "exchange_calendar_available", lambda: True)
    monkeypatch.setattr(telegram_module, "is_regular_market_open", lambda _: False)
    with connect_database(path) as database:
        database.init_schema()
        database.add_watchlist_symbol("005930")
        database.record_heartbeat("trading-service")
        database.record_heartbeat("order-supervisor")
    assert handle_update({"message": {"chat": {"id": 42}, "text": "/readiness"}}, path, fake, "42")
    text = fake.sent[-1][1]
    assert "krx_market_open=false" in text
    assert "resume_ready=true" in text
    assert "blockers: none" in text


def test_status_and_readiness_show_operator_review_reasons(tmp_path, monkeypatch):
    path = str(tmp_path / "control.sqlite3")
    fake = FakeTelegram()
    _ready_demo_env(monkeypatch)
    monkeypatch.setattr(telegram_module, "is_regular_market_open", lambda _: False)
    _ready_demo_db(path)
    with connect_database(path) as database:
        database.set_runtime_metadata("operator_review", "true")
        database.set_runtime_metadata(
            "order-supervisor.status",
            '{"status":"reconciled_operator_review","reasons":["reconciliation:position_mismatch"]}',
        )

    update = lambda text: {"message": {"chat": {"id": 42}, "text": text}}
    assert handle_update(update("/status"), path, fake, "42")
    assert "operator_review_reasons: reconciliation:position_mismatch" in fake.sent[-1][1]
    assert handle_update(update("/readiness"), path, fake, "42")
    assert "operator_review=true (reconciliation:position_mismatch)" in fake.sent[-1][1]


def test_readiness_shows_supervisor_dependency_details(tmp_path, monkeypatch):
    path = str(tmp_path / "control.sqlite3")
    fake = FakeTelegram()
    _ready_demo_env(monkeypatch)
    monkeypatch.setattr(telegram_module, "is_regular_market_open", lambda _: False)
    _ready_demo_db(path)
    with connect_database(path) as database:
        database.set_runtime_metadata("operator_review", "true")
        database.set_runtime_metadata("block_new_entries", "true")
        database.set_runtime_metadata(
            "order-supervisor.status",
            (
                '{"status":"dependency_unavailable","environment":"demo",'
                '"failure_streak":2,"healthy_streak":0,"next_retry_seconds":10,'
                '"safe_kis_error":{"http_status":"200","rt_cd":"-1","msg_cd":"EGW00123"},'
                '"updated_at":"2026-08-25T06:59:00+00:00",'
                '"reasons":["dependency:order_status_unavailable:KisHttpError:http_200:rt_cd_-1:msg_cd_EGW00123"]}'
            ),
        )

    assert handle_update({"message": {"chat": {"id": 42}, "text": "/readiness"}}, path, fake, "42")
    text = fake.sent[-1][1]
    assert "order_supervisor_status=dependency_unavailable" in text
    assert "failure_streak=2 healthy_streak=0 next_retry_seconds=10" in text
    assert "safe_kis_error=http_200 rt_cd=-1 msg_cd=EGW00123" in text
    assert "order_supervisor_status_age=" in text


def test_performance_command_and_ai_menu_button(tmp_path):
    path = str(tmp_path / "control.sqlite3")
    fake = FakeTelegram()
    update = lambda text: {"message": {"chat": {"id": 42}, "text": text}}

    assert "performance" in {item["command"] for item in telegram_module.BOT_COMMANDS}
    callbacks = [
        button["callback_data"]
        for row in telegram_module.AI_MENU_KEYBOARD["inline_keyboard"]
        for button in row
    ]
    assert "control:performance" in callbacks
    assert handle_update(update("/performance"), path, fake, "42")
    assert "performance report" in fake.sent[-1][1]


def test_watchlist_commands_validate_and_reactivate_symbols(tmp_path):
    path = str(tmp_path / "control.sqlite3")
    fake = FakeTelegram()
    update = lambda text: {"message": {"chat": {"id": 42}, "text": text}}
    assert handle_update(update("/watchlist_add 005930,000660"), path, fake, "42")
    assert "005930,000660" in fake.sent[-1][1]
    assert handle_update(update("/watchlist_remove 005930"), path, fake, "42")
    assert handle_update(update("/watchlist_add 005930"), path, fake, "42")
    assert "005930" in fake.sent[-1][1]
    assert handle_update(update("/watchlist_add ABC"), path, fake, "42")
    assert "6자리" in fake.sent[-1][1]


def test_watchlist_menu_add_prompt_and_symbol_remove_callbacks(tmp_path):
    path = str(tmp_path / "control.sqlite3")
    fake = FakeTelegram()
    with connect_database(path) as database:
        database.init_schema()
        database.add_watchlist_symbol("005930")
        database.add_watchlist_symbol("000660")

    menu_update = {"callback_query": {
        "id": "watchlist-menu",
        "from": {"id": 42},
        "data": "control:watchlist",
        "message": {"chat": {"id": 42, "type": "private"}},
    }}
    assert handle_update(menu_update, path, fake, "42")
    markup = fake.sent[-1][2]
    callbacks = [
        button["callback_data"]
        for row in markup["inline_keyboard"]
        for button in row
    ]
    assert "watchlist:add" in callbacks
    assert "watchlist:remove:005930" in callbacks

    add_update = {"callback_query": {
        "id": "watchlist-add",
        "from": {"id": 42},
        "data": "watchlist:add",
        "message": {"chat": {"id": 42, "type": "private"}},
    }}
    assert handle_update(add_update, path, fake, "42")
    assert "/watchlist_add 005930" in fake.sent[-1][1]

    remove_update = {"callback_query": {
        "id": "watchlist-remove",
        "from": {"id": 42},
        "data": "watchlist:remove:005930",
        "message": {"chat": {"id": 42, "type": "private"}},
    }}
    assert handle_update(remove_update, path, fake, "42")
    assert "관심종목 제거: 005930" in fake.sent[-1][1]
    with connect_database(path) as database:
        database.init_schema()
        assert database.list_watchlist_symbols() == ["000660"]
