from dataclasses import dataclass

from kis_ai_scalper.ops.openai_usage import (
    fetch_openai_cost_summary,
    openai_cost_summary_from_env,
)
from kis_ai_scalper.ops.telegram import handle_update


@dataclass
class FakeResponse:
    payload: dict
    status_code: int = 200

    def raise_for_status(self):
        return None

    def json(self):
        return self.payload


class FakeSession:
    def __init__(self):
        self.gets = []

    def get(self, url, **kwargs):
        self.gets.append((url, kwargs))
        return FakeResponse({
            "data": [{
                "results": [
                    {"amount": {"value": 0.12, "currency": "usd"}},
                    {"amount": {"value": 0.03, "currency": "usd"}},
                ],
            }],
        })


class FakeTelegram:
    def __init__(self):
        self.sent = []
        self.answered = []

    def send_message(self, chat_id, text, reply_markup=None):
        self.sent.append((str(chat_id), text, reply_markup))

    def answer_callback_query(self, callback_query_id, text=None):
        self.answered.append((callback_query_id, text))


def test_fetch_openai_cost_summary_uses_costs_endpoint():
    session = FakeSession()
    summary = fetch_openai_cost_summary("usage-key", days=7, session=session)

    assert summary.available is True
    assert summary.total == 0.15
    assert summary.text() == "openai cost: 0.1500 USD last_7d"
    url, kwargs = session.gets[0]
    assert url == "https://api.openai.com/v1/organization/costs"
    assert kwargs["headers"]["authorization"] == "Bearer usage-key"
    assert kwargs["params"]["limit"] == 7


def test_cost_summary_from_env_reports_missing_admin_key(tmp_path, monkeypatch):
    (tmp_path / ".env").write_text("OPENAI_API_KEY=project-key\n", encoding="utf-8")
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("OPENAI_ADMIN_KEY", raising=False)
    monkeypatch.delenv("OPENAI_USAGE_API_KEY", raising=False)

    summary = openai_cost_summary_from_env(cwd=tmp_path)

    assert summary.available is False
    assert "OPENAI_ADMIN_KEY missing" in summary.text()


def test_telegram_cost_command_and_button_send_cost_text(tmp_path, monkeypatch):
    monkeypatch.setattr(
        "kis_ai_scalper.ops.telegram.openai_cost_summary_from_env",
        lambda: type("Summary", (), {"text": lambda self: "openai cost: 1.2300 USD last_30d"})(),
    )
    fake = FakeTelegram()

    assert handle_update(
        {"message": {"chat": {"id": 42}, "text": "/cost"}},
        str(tmp_path / "control.sqlite3"),
        fake,
        "42",
    ) is True
    assert "openai cost: 1.2300 USD" in fake.sent[-1][1]

    callback = {"callback_query": {
        "id": "cb-1",
        "data": "control:cost",
        "message": {"chat": {"id": 42}},
    }}
    assert handle_update(callback, str(tmp_path / "control.sqlite3"), fake, "42") is True
    assert fake.answered == [("cb-1", None)]
    assert "openai cost: 1.2300 USD" in fake.sent[-1][1]
