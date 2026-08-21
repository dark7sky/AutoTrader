import json
from pathlib import Path

import pytest

from kis_ai_scalper.cli import _TelegramNotifier
from kis_ai_scalper.storage.database import connect_database


class FakeTelegramClient:
    instances = []
    next_message_id = 100
    fail_next_edit = False

    def __init__(self, token):
        self.token = token
        self.calls = []
        type(self).instances.append(self)

    def send_message(self, chat_id, text, reply_markup=None):
        message_id = type(self).next_message_id
        type(self).next_message_id += 1
        self.calls.append(("sendMessage", chat_id, message_id, text, reply_markup))
        return {"ok": True, "result": {"message_id": message_id}}

    def edit_message_text(self, chat_id, message_id, text, reply_markup=None):
        self.calls.append(("editMessageText", chat_id, message_id, text, reply_markup))
        if type(self).fail_next_edit:
            type(self).fail_next_edit = False
            raise RuntimeError("edit failed")
        return {"ok": True, "result": {"message_id": message_id}}

    def delete_message(self, chat_id, message_id):
        self.calls.append(("deleteMessage", chat_id, message_id))
        return {"ok": True, "result": True}


@pytest.fixture
def fake_telegram(monkeypatch):
    FakeTelegramClient.instances = []
    FakeTelegramClient.next_message_id = 100
    FakeTelegramClient.fail_next_edit = False
    monkeypatch.setattr("kis_ai_scalper.ops.telegram.TelegramClient", FakeTelegramClient)
    return FakeTelegramClient


def _calls(fake_client, method):
    return [call for call in fake_client.calls if call[0] == method]


def _metadata(db_path: Path) -> dict[str, str]:
    with connect_database(db_path) as database:
        database.init_schema()
        rows = database.connection.execute(
            "SELECT key, value FROM runtime_metadata WHERE key LIKE 'telegram%notification%'"
        ).fetchall()
    return {row["key"]: row["value"] for row in rows}


def test_general_notifications_are_compacted_to_latest_five(fake_telegram, tmp_path):
    db_path = tmp_path / "telegram.sqlite3"
    notifier = _TelegramNotifier("token", "chat", db_path=str(db_path))

    for index in range(1, 7):
        notifier.send(f"notice-{index}")

    client = fake_telegram.instances[0]
    sends = _calls(client, "sendMessage")
    edits = _calls(client, "editMessageText")

    assert len(sends) == 1
    assert len(edits) == 5
    assert edits[-1][2] == sends[0][2]
    rendered = edits[-1][3]
    assert all(f"notice-{index}" in rendered for index in range(2, 7))
    assert "notice-1" not in edits[-1][3]

    metadata = _metadata(db_path)
    assert metadata
    decoded = {key: json.loads(value) for key, value in metadata.items()}
    assert any(value == sends[0][2] for value in decoded.values() if isinstance(value, int))
    assert any(isinstance(value, list) and len(value) == 5 for value in decoded.values())


def test_notification_message_is_reused_after_process_recreation(fake_telegram, tmp_path):
    db_path = tmp_path / "telegram.sqlite3"
    first = _TelegramNotifier("token", "chat", db_path=str(db_path))
    first.send("before-restart")
    message_id = _calls(fake_telegram.instances[0], "sendMessage")[0][2]

    second = _TelegramNotifier("token", "chat", db_path=str(db_path))
    second.send("after-restart")

    sends = _calls(fake_telegram.instances[1], "sendMessage")
    edits = _calls(fake_telegram.instances[1], "editMessageText")
    assert sends == []
    assert len(edits) == 1
    assert edits[0][2] == message_id
    assert "before-restart" in edits[0][3]
    assert "after-restart" in edits[0][3]


def test_edit_failure_deletes_old_message_and_starts_new_history(fake_telegram, tmp_path):
    db_path = tmp_path / "telegram.sqlite3"
    notifier = _TelegramNotifier("token", "chat", db_path=str(db_path))
    notifier.send("first")
    old_message_id = _calls(fake_telegram.instances[0], "sendMessage")[0][2]

    fake_telegram.fail_next_edit = True
    notifier.send("second")

    client = fake_telegram.instances[0]
    assert _calls(client, "deleteMessage") == [("deleteMessage", "chat", old_message_id)]
    replacement = _calls(client, "sendMessage")[1]
    assert "first" in replacement[3]
    assert "second" in replacement[3]

    notifier.send("third")
    latest_edit = _calls(client, "editMessageText")[-1]
    assert latest_edit[2] == replacement[2]
    assert all(value in latest_edit[3] for value in ("first", "second", "third"))


def test_approval_notification_is_separate_from_general_history(fake_telegram, tmp_path):
    db_path = tmp_path / "telegram.sqlite3"
    notifier = _TelegramNotifier("token", "chat", db_path=str(db_path))

    notifier.send_approval("approval-1", "approval request")
    notifier.send("normal-1")
    notifier.send("normal-2")

    client = fake_telegram.instances[0]
    sends = _calls(client, "sendMessage")
    edits = _calls(client, "editMessageText")
    assert sends[0][3] == "approval request"
    assert "normal-1" in sends[1][3]
    assert len(edits) == 1
    assert "normal-1" in edits[0][3]
    assert "normal-2" in edits[0][3]
