"""Bounded Telegram operator control for the local paper runtime."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import requests

from kis_ai_scalper.ops.control import set_environment, set_paused
from kis_ai_scalper.ops.openai_usage import openai_cost_summary_from_env
from kis_ai_scalper.paper import report_from_database
from kis_ai_scalper.storage import connect_database


class TelegramClient:
    """Small requests-only Telegram Bot API client."""

    def __init__(self, token: str, *, session: Any | None = None, timeout: float = 20.0) -> None:
        if not token:
            raise ValueError("Telegram bot token is required")
        self._token = token
        self._session = session or requests.Session()
        self._timeout = timeout

    def _call(self, method: str, payload: dict[str, Any]) -> dict[str, Any]:
        try:
            response = self._session.post(
                f"https://api.telegram.org/bot{self._token}/{method}",
                json=payload,
                timeout=self._timeout,
            )
            response.raise_for_status()
        except requests.RequestException:
            raise RuntimeError(f"Telegram API request failed: {method}") from None
        body = response.json()
        if not body.get("ok"):
            raise RuntimeError(f"Telegram API call failed: {method}")
        return body

    def send_message(self, chat_id: str | int, text: str, reply_markup: dict[str, Any] | None = None) -> dict[str, Any]:
        payload: dict[str, Any] = {"chat_id": chat_id, "text": text}
        if reply_markup is not None:
            payload["reply_markup"] = reply_markup
        return self._call("sendMessage", payload)

    def get_updates(self, *, offset: int | None = None, limit: int = 10, timeout: int = 0) -> list[dict[str, Any]]:
        payload: dict[str, Any] = {"limit": limit, "timeout": timeout}
        if offset is not None:
            payload["offset"] = offset
        return self._call("getUpdates", payload).get("result", [])

    def answer_callback_query(self, callback_query_id: str, text: str | None = None) -> dict[str, Any]:
        payload: dict[str, Any] = {"callback_query_id": callback_query_id}
        if text:
            payload["text"] = text
        return self._call("answerCallbackQuery", payload)


KEYBOARD = {
    "inline_keyboard": [[
        {"text": "Pause", "callback_data": "control:pause"},
        {"text": "Resume", "callback_data": "control:resume"},
    ], [
        {"text": "Status", "callback_data": "control:status"},
        {"text": "Paper report", "callback_data": "control:report"},
    ], [
        {"text": "OpenAI cost", "callback_data": "control:cost"},
    ], [
        {"text": "Demo env", "callback_data": "control:environment:demo"},
        {"text": "Real env", "callback_data": "control:environment:real"},
    ], [
        {"text": "Positions", "callback_data": "control:positions"},
    ]]
}


def _status_text(db_path: str) -> str:
    with connect_database(db_path) as database:
        database.init_schema()
        control = database.get_runtime_control()
        environment = control.environment
        live_positions = database.list_open_live_positions()
        paper_positions = database.paper_positions()
    state = "paused" if control.paused else "running"
    return (
        f"runtime: {state}\n"
        f"environment: {environment}\n"
        f"open_live_positions: {len(live_positions)}\n"
        f"open_paper_positions: {len(paper_positions)}\n"
        f"reason: {control.reason}\nsource: {control.source}\nupdated_at: {control.updated_at}"
    )


def _positions_text(db_path: str) -> str:
    with connect_database(db_path) as database:
        database.init_schema()
        live = database.list_open_live_positions()
        paper = database.paper_positions()
    lines = ["open positions"]
    lines.extend(f"live {row['symbol']} qty={row['quantity']}" for row in live)
    lines.extend(f"paper {position.symbol} qty={position.quantity}" for position in paper)
    return "\n".join(lines) if len(lines) > 1 else "open positions\nnone"


def _report_text(db_path: str) -> str:
    with connect_database(db_path) as database:
        database.init_schema()
        report = report_from_database(database)
    return (
        "paper report\n"
        f"orders={report.total_paper_orders} fills={report.total_paper_fills} "
        f"open_positions={len(report.open_positions)} realized_pnl={report.realized_pnl:g}\n"
        f"symbols={','.join(report.symbols) or 'none'}\n"
        f"{_cost_text()}"
    )


def _cost_text() -> str:
    return openai_cost_summary_from_env().text()


def _command(update: dict[str, Any]) -> tuple[str, str | int] | None:
    message = update.get("message") or {}
    text = str(message.get("text") or "").strip()
    chat_id = (message.get("chat") or {}).get("id")
    if not text or chat_id is None or not text.startswith("/"):
        return None
    parts = text.split(maxsplit=1)
    command = parts[0].split("@", 1)[0].lower()
    argument = parts[1].strip() if len(parts) > 1 else ""
    if command in {"/pause", "/resume"}:
        argument = argument or "telegram_operator"
    return command + (" " + argument if argument and command in {"/pause", "/resume", "/env", "/environment"} else ""), chat_id


def _callback(update: dict[str, Any]) -> tuple[str, str | int, str] | None:
    callback = update.get("callback_query") or {}
    message = callback.get("message") or {}
    chat_id = (message.get("chat") or {}).get("id")
    data = str(callback.get("data") or "")
    callback_id = callback.get("id")
    if chat_id is None or not callback_id or not data.startswith("control:"):
        return None
    return data.split(":", 1)[1], chat_id, callback_id


def handle_update(update: dict[str, Any], db_path: str, client: Any, allowed_chat_id: str | int) -> bool:
    """Handle one authorized update and return whether it was actionable."""
    command = _command(update)
    callback = _callback(update)
    chat_id = command[1] if command else callback[1] if callback else None
    if chat_id is None or str(chat_id) != str(allowed_chat_id):
        return False
    if callback:
        action, chat_id, callback_id = callback
        client.answer_callback_query(callback_id)
        command_name = "/" + action
    else:
        assert command is not None
        command_name, chat_id = command[0].split(" ", 1)[0], command[1]
    if command_name in {"/pause", "pause"}:
        reason = "telegram_operator"
        if command and " " in command[0]:
            reason = command[0].split(" ", 1)[1]
        set_paused(db_path, True, reason, "telegram")
        text = "runtime paused"
    elif command_name in {"/resume", "resume"}:
        reason = "telegram_operator"
        if command and " " in command[0]:
            reason = command[0].split(" ", 1)[1]
        set_paused(db_path, False, reason, "telegram")
        text = "runtime resumed"
    elif command_name in {"/status", "status", "/control", "control"}:
        text = _status_text(db_path)
    elif command_name in {"/env", "env", "/environment", "environment"}:
        value = command[0].split(" ", 1)[1] if command and " " in command[0] else None
        if value is None:
            text = _status_text(db_path)
        elif value not in {"demo", "real"}:
            text = "usage: /env demo|real"
        else:
            try:
                selected = set_environment(db_path, value, "telegram_operator", "telegram")
            except ValueError as exc:
                text = f"environment unchanged: {exc}"
            else:
                text = f"runtime environment: {selected.environment}"
    elif command_name in {"/positions", "positions"}:
        text = _positions_text(db_path)
    elif command_name in {"/environment:demo", "/environment:real"}:
        value = command_name.split(":", 1)[1]
        try:
            selected = set_environment(db_path, value, "telegram_operator", "telegram")
        except ValueError as exc:
            text = f"environment unchanged: {exc}"
        else:
            text = f"runtime environment: {selected.environment}"
    elif command_name in {"/report", "report"}:
        text = _report_text(db_path)
    elif command_name in {"/cost", "cost"}:
        text = _cost_text()
    else:
        return False
    client.send_message(chat_id, text, reply_markup=KEYBOARD if command_name in {"/control", "control", "/environment:demo", "/environment:real"} else None)
    return True


def poll_telegram(db_path: str, token: str, allowed_chat_id: str | int, *, limit: int = 10, timeout_seconds: int = 0, client: Any | None = None) -> int:
    if not 1 <= limit <= 100:
        raise ValueError("limit must be between 1 and 100")
    if not 0 <= timeout_seconds <= 60:
        raise ValueError("timeout_seconds must be between 0 and 60")
    telegram = client or TelegramClient(token)
    updates = telegram.get_updates(limit=limit, timeout=timeout_seconds)
    for update in updates[:limit]:
        handle_update(update, db_path, telegram, allowed_chat_id)
    return 0


def env_value(name: str) -> str:
    value = os.getenv(name)
    if not value:
        dotenv = Path.cwd() / ".env"
        if dotenv.exists():
            for raw_line in dotenv.read_text(encoding="utf-8").splitlines():
                line = raw_line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                key, candidate = line.split("=", 1)
                if key.strip() == name:
                    value = candidate.strip().strip('"').strip("'")
                    break
    if not value:
        raise ValueError(f"required environment variable is missing: {name}")
    return value


def optional_env_value(name: str) -> str | None:
    try:
        return env_value(name)
    except ValueError:
        return None
