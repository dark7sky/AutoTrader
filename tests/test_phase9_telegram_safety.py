from datetime import datetime, timedelta, timezone

import pytest
import requests

from kis_ai_scalper import cli
from kis_ai_scalper.ops import telegram as telegram_module
from kis_ai_scalper.ops.telegram import (
    EMERGENCY_STOP_KEY,
    REAL_CHALLENGE_EXPIRES_KEY,
    REAL_CHALLENGE_KEY,
    REAL_RESUME_ARM_EXPIRES_KEY,
    TelegramClient,
    handle_update,
    poll_telegram,
)
from kis_ai_scalper.ops.control import set_paused
from kis_ai_scalper.storage import connect_database


class FakeTelegram:
    def __init__(self, updates=None):
        self.updates = updates or []
        self.get_calls = []
        self.sent = []
        self.answered = []

    def get_updates(self, **kwargs):
        self.get_calls.append(kwargs)
        return self.updates

    def send_message(self, chat_id, text, reply_markup=None):
        self.sent.append((str(chat_id), text, reply_markup))

    def answer_callback_query(self, callback_query_id, text=None):
        self.answered.append((callback_query_id, text))

    def set_my_commands(self, commands):
        self.commands = commands

    def set_chat_menu_button(self, chat_id, menu_button=None):
        self.chat_menu_button = (chat_id, menu_button)


def private(text, update_id=None, chat_id=42, user_id=None):
    message = {"chat": {"id": chat_id, "type": "private"}, "text": text}
    if user_id is not None:
        message["from"] = {"id": user_id}
    update = {"message": message}
    if update_id is not None:
        update["update_id"] = update_id
    return update


def test_poll_persists_offset_in_update_id_order_and_consumes_unauthorized(tmp_path):
    path = str(tmp_path / "telegram.sqlite3")
    fake = FakeTelegram([
        private("/status", update_id=12, chat_id=999),
        {"update_id": 10, "edited_message": {"text": "ignored"}},
        private("/pause", update_id=11),
    ])

    assert poll_telegram(path, "token", "42", client=fake) == 0
    assert fake.get_calls == [{"offset": None, "limit": 10, "timeout": 0}]
    with connect_database(path) as database:
        assert database.get_telegram_update_offset() == 13
        assert database.get_runtime_control().paused is True

    second = FakeTelegram([])
    poll_telegram(path, "token", "42", client=second)
    assert second.get_calls[0]["offset"] == 13


def test_group_requires_allowed_user_and_callback_checks_callback_actor(tmp_path):
    path = str(tmp_path / "telegram.sqlite3")
    fake = FakeTelegram()
    group = {
        "message": {
            "chat": {"id": -100, "type": "supergroup"},
            "from": {"id": 7},
            "text": "/pause",
        }
    }
    assert handle_update(group, path, fake, "-100", "8") is False
    assert fake.sent == []

    callback = {
        "callback_query": {
            "id": "cb-1",
            "from": {"id": 7},
            "data": "control:pause",
            "message": {"chat": {"id": -100, "type": "group"}},
        }
    }
    assert handle_update(callback, path, fake, "-100", "8") is False
    assert fake.answered == []
    callback["callback_query"]["from"]["id"] = 8
    assert handle_update(callback, path, fake, "-100", "8") is True
    assert fake.answered == [("cb-1", None)]


def test_menu_main_callback_exposes_all_operator_sections(tmp_path):
    path = str(tmp_path / "telegram.sqlite3")
    fake = FakeTelegram()
    callback = {
        "callback_query": {
            "id": "menu-main",
            "from": {"id": 42},
            "data": "menu:main",
            "message": {"chat": {"id": 42, "type": "private"}},
        }
    }
    assert handle_update(callback, path, fake, "42", "42") is True
    buttons = [
        button["callback_data"]
        for row in fake.sent[-1][2]["inline_keyboard"]
        for button in row
    ]
    assert {"menu:status", "menu:trading", "menu:control", "menu:environment", "menu:ai"} <= set(buttons)


def test_menu_callback_preserves_existing_control_and_approval_callbacks(tmp_path):
    path = str(tmp_path / "telegram.sqlite3")
    fake = FakeTelegram()
    control = {
        "callback_query": {
            "id": "control-pause",
            "from": {"id": 42},
            "data": "control:pause",
            "message": {"chat": {"id": 42, "type": "private"}},
        }
    }
    assert handle_update(control, path, fake, "42", "42") is True
    assert fake.answered[-1] == ("control-pause", None)


def test_telegram_client_registers_commands_and_chat_menu_button():
    calls = []

    class Response:
        def raise_for_status(self):
            return None

        def json(self):
            return {"ok": True, "result": {}}

    class Session:
        def post(self, url, **kwargs):
            calls.append((url, kwargs["json"]))
            return Response()

    client = TelegramClient("token", session=Session())
    client.set_my_commands([
        {"command": "menu", "description": "메뉴"},
    ])
    client.set_chat_menu_button("42")
    assert [url.rsplit("/", 1)[1] for url, _ in calls] == ["setMyCommands", "setChatMenuButton"]


def test_service_start_notification_includes_main_menu_markup(monkeypatch):
    sent = []

    class FakeClient:
        def __init__(self, token):
            self.token = token

        def send_message(self, chat_id, text, reply_markup=None):
            sent.append((chat_id, text, reply_markup))

    monkeypatch.setattr(telegram_module, "TelegramClient", FakeClient)
    notifier = cli._TelegramNotifier("token", "42")
    notifier.send_menu("auto-trade service started\nruntime: paused")
    assert sent[-1][2] == telegram_module.MAIN_MENU_KEYBOARD


def test_real_environment_requires_challenge_confirmation_and_one_time_arm(tmp_path, monkeypatch):
    path = str(tmp_path / "telegram.sqlite3")
    fake = FakeTelegram()
    now = datetime(2026, 8, 16, 1, 0, tzinfo=timezone.utc)
    monkeypatch.setattr("kis_ai_scalper.ops.telegram._utcnow", lambda: now)

    assert handle_update(private("/env real"), path, fake, "42") is True
    challenge_text = fake.sent[-1][1]
    code = challenge_text.split("challenge: ", 1)[1].split("\n", 1)[0]
    assert len(code) == 6 and code.isdigit()
    with connect_database(path) as database:
        assert database.get_runtime_control().environment == "demo"
        assert database.get_runtime_metadata(REAL_CHALLENGE_KEY) == code
        assert database.get_runtime_metadata(REAL_CHALLENGE_EXPIRES_KEY)

    assert handle_update(private("/resume"), path, fake, "42") is True
    assert "runtime resumed" in fake.sent[-1][1]
    assert handle_update(private("/pause"), path, fake, "42") is True
    assert handle_update(private(f"/confirm-real {code}"), path, fake, "42") is True
    assert "runtime environment: real" in fake.sent[-1][1]
    with connect_database(path) as database:
        assert database.get_runtime_control().environment == "real"
        assert database.get_runtime_metadata(REAL_RESUME_ARM_EXPIRES_KEY)

    assert handle_update(private("/resume"), path, fake, "42") is True
    assert "runtime resumed" in fake.sent[-1][1]
    assert handle_update(private("/pause"), path, fake, "42") is True
    assert handle_update(private("/resume"), path, fake, "42") is True
    assert "rejected" in fake.sent[-1][1]


def test_real_challenge_expires(tmp_path, monkeypatch):
    path = str(tmp_path / "telegram.sqlite3")
    fake = FakeTelegram()
    start = datetime(2026, 8, 16, 1, 0, tzinfo=timezone.utc)
    current = [start]
    monkeypatch.setattr("kis_ai_scalper.ops.telegram._utcnow", lambda: current[0])
    handle_update(private("/env real"), path, fake, "42")
    code = fake.sent[-1][1].split("challenge: ", 1)[1].split("\n", 1)[0]
    current[0] = start + timedelta(minutes=5)
    handle_update(private(f"/confirm-real {code}"), path, fake, "42")
    assert "invalid or expired" in fake.sent[-1][1]


def test_real_callback_only_issues_challenge_and_uses_five_minutes(tmp_path, monkeypatch):
    path = str(tmp_path / "telegram.sqlite3")
    fake = FakeTelegram()
    start = datetime(2026, 8, 16, 1, 0, tzinfo=timezone.utc)
    monkeypatch.setattr("kis_ai_scalper.ops.telegram._utcnow", lambda: start)
    callback = {
        "callback_query": {
            "id": "cb-real",
            "from": {"id": 1},
            "data": "control:environment:real",
            "message": {"chat": {"id": 42, "type": "private"}},
        }
    }
    assert handle_update(callback, path, fake, "42") is True
    assert fake.sent[-1][1].startswith("runtime environment: real pending")
    with connect_database(path) as database:
        assert database.get_runtime_control().environment == "demo"
        expires = datetime.fromisoformat(database.get_runtime_metadata(REAL_CHALLENGE_EXPIRES_KEY))
        assert expires == start + timedelta(minutes=5)


def test_emergency_stop_blocks_resume_until_cleared_while_paused(tmp_path):
    path = str(tmp_path / "telegram.sqlite3")
    fake = FakeTelegram()
    assert handle_update(private("/emergency-stop"), path, fake, "42") is True
    with connect_database(path) as database:
        assert database.get_runtime_metadata(EMERGENCY_STOP_KEY) == "true"
    assert handle_update(private("/resume"), path, fake, "42") is True
    assert "emergency stop is active" in fake.sent[-1][1]
    assert handle_update(private("/clear-emergency"), path, fake, "42") is True
    with connect_database(path) as database:
        assert database.get_runtime_metadata(EMERGENCY_STOP_KEY) == "false"
        assert database.get_runtime_control().paused is True
    assert handle_update(private("/resume"), path, fake, "42") is True
    assert "runtime resumed" in fake.sent[-1][1]


def test_poll_uses_env_allowed_user_id_when_argument_is_omitted(tmp_path, monkeypatch):
    path = str(tmp_path / "telegram.sqlite3")
    monkeypatch.setenv("TELEGRAM_ALLOWED_USER_ID", "8")
    set_paused(path, False, "test", "test")
    fake = FakeTelegram([private("/pause", update_id=1, chat_id=42, user_id=7)])
    assert poll_telegram(path, "token", "42", client=fake) == 0
    with connect_database(path) as database:
        assert database.get_runtime_control().paused is False


def test_emergency_orders_and_fills_are_available_without_sensitive_ids(tmp_path):
    path = str(tmp_path / "telegram.sqlite3")
    with connect_database(path) as database:
        database.init_schema()
        database.claim_order_intent(
            client_order_id="secret-client-id", signal_id="signal-1", symbol="005930",
            side="BUY", requested_qty=2, requested_price=100,
        )
        database.record_order_submission("secret-client-id", "secret-broker-id")
        database.apply_broker_fill(
            fill_id="fill-1", client_order_id="secret-client-id", quantity=1,
            price=100, filled_at=datetime.now(timezone.utc), broker_order_id="secret-broker-id",
        )
    fake = FakeTelegram()
    assert handle_update(private("/orders"), path, fake, "42") is True
    assert "005930" in fake.sent[-1][1]
    assert "secret-client-id" not in fake.sent[-1][1]
    assert handle_update(private("/fills"), path, fake, "42") is True
    assert "005930" in fake.sent[-1][1]
    assert handle_update(private("/emergency-stop"), path, fake, "42") is True
    with connect_database(path) as database:
        assert database.get_runtime_control().paused is True
        assert database.get_runtime_metadata(REAL_RESUME_ARM_EXPIRES_KEY) == ""


def test_telegram_network_errors_do_not_expose_token():
    class FailingSession:
        def post(self, *args, **kwargs):
            raise requests.ConnectionError("request failed")

    with pytest.raises(RuntimeError) as error:
        TelegramClient("super-secret-token", session=FailingSession()).get_updates()
    assert "super-secret-token" not in str(error.value)
