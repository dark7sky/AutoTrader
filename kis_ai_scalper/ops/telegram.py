"""Bounded Telegram operator control for the local paper runtime."""

from __future__ import annotations

import os
import json
import json
import secrets
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import requests

from kis_ai_scalper.ops.control import set_environment, set_paused
from kis_ai_scalper.ops.openai_usage import openai_cost_summary_from_env
from kis_ai_scalper.paper import report_from_database
from kis_ai_scalper.storage import connect_database


REAL_CHALLENGE_KEY = "telegram.real_challenge"
REAL_CHALLENGE_EXPIRES_KEY = "telegram.real_challenge_expires"
REAL_RESUME_ARM_EXPIRES_KEY = "telegram.real_resume_arm_expires"
REAL_RESUME_ARM_USES_KEY = "telegram.real_resume_arm_uses"
EMERGENCY_STOP_KEY = "telegram.emergency_stop"
LIVE_REPORT_SNAPSHOT_KEY = "live_report_snapshot"
HEARTBEAT_MAX_AGE_SECONDS = 180.0


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
        {"text": "Live report", "callback_data": "control:live-report"},
    ], [
        {"text": "Orders", "callback_data": "control:orders"},
        {"text": "Fills", "callback_data": "control:fills"},
        {"text": "Approvals", "callback_data": "control:approvals"},
    ], [
        {"text": "Emergency stop", "callback_data": "control:emergency-stop"},
        {"text": "Clear emergency", "callback_data": "control:clear-emergency"},
    ]]
}


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _metadata(db_path: str, key: str) -> str | None:
    with connect_database(db_path) as database:
        database.init_schema()
        return database.get_runtime_metadata(key)


def _set_metadata(db_path: str, key: str, value: str) -> None:
    with connect_database(db_path) as database:
        database.init_schema()
        database.set_runtime_metadata(key, value)


def _real_challenge(db_path: str) -> str:
    code = f"{secrets.randbelow(1_000_000):06d}"
    expires = (_utcnow() + timedelta(minutes=5)).isoformat()
    with connect_database(db_path) as database:
        database.init_schema()
        database.set_runtime_metadata(REAL_CHALLENGE_KEY, code)
        database.set_runtime_metadata(REAL_CHALLENGE_EXPIRES_KEY, expires)
    return code


def _confirm_real(db_path: str, code: str) -> str:
    with connect_database(db_path) as database:
        database.init_schema()
        control = database.get_runtime_control()
        if not control.paused:
            return "real environment can only be confirmed while paused"
        expected = database.get_runtime_metadata(REAL_CHALLENGE_KEY) or ""
        expires_text = database.get_runtime_metadata(REAL_CHALLENGE_EXPIRES_KEY) or ""
        try:
            expired = not expires_text or _utcnow() >= datetime.fromisoformat(expires_text)
        except ValueError:
            expired = True
        if not code or not secrets.compare_digest(code, expected) or expired:
            return "real challenge invalid or expired"
        try:
            set_environment(db_path, "real", "telegram_operator", "telegram")
        except ValueError as exc:
            return f"runtime environment unchanged: {exc}"
        database.set_runtime_metadata(REAL_CHALLENGE_KEY, "")
        database.set_runtime_metadata(REAL_CHALLENGE_EXPIRES_KEY, "")
        database.set_runtime_metadata(REAL_RESUME_ARM_EXPIRES_KEY, (_utcnow() + timedelta(minutes=15)).isoformat())
        database.set_runtime_metadata(REAL_RESUME_ARM_USES_KEY, "1")
    return "runtime environment: real (armed for one resume)"


def _real_resume_allowed(db_path: str) -> bool:
    with connect_database(db_path) as database:
        database.init_schema()
        control = database.get_runtime_control()
        if control.environment != "real":
            return True
        expires_text = database.get_runtime_metadata(REAL_RESUME_ARM_EXPIRES_KEY) or ""
        uses_text = database.get_runtime_metadata(REAL_RESUME_ARM_USES_KEY) or "0"
        try:
            valid = _utcnow() < datetime.fromisoformat(expires_text) and int(uses_text) > 0
        except (ValueError, TypeError):
            valid = False
        if valid:
            database.set_runtime_metadata(REAL_RESUME_ARM_USES_KEY, str(int(uses_text) - 1))
        return valid


def _emergency_stop_active(db_path: str) -> bool:
    return (_metadata(db_path, EMERGENCY_STOP_KEY) or "").lower() == "true"


def _status_text(db_path: str) -> str:
    with connect_database(db_path) as database:
        database.init_schema()
        control = database.get_runtime_control()
        environment = control.environment
        live_positions = database.list_open_live_positions()
        paper_positions = database.paper_positions()
        heartbeat = database.get_heartbeat("trading-service")
        operator_review = database.get_runtime_metadata("operator_review")
        block_entries = database.get_runtime_metadata("block_new_entries")
        emergency = database.get_runtime_metadata(EMERGENCY_STOP_KEY) or database.get_runtime_metadata("emergency_stop")
    state = "paused" if control.paused else "running"
    heartbeat_text, heartbeat_healthy = _heartbeat_status(heartbeat)
    return (
        f"runtime: {state}\n"
        f"environment: {environment}\n"
        f"open_live_positions: {len(live_positions)}\n"
        f"open_paper_positions: {len(paper_positions)}\n"
        f"reason: {control.reason}\nsource: {control.source}\nupdated_at: {control.updated_at}\n"
        f"trading_service: {heartbeat_text} healthy={heartbeat_healthy}\n"
        f"operator_review: {_safe_flag(operator_review)}\n"
        f"block_new_entries: {_safe_flag(block_entries)}\n"
        f"emergency_stop: {_safe_flag(emergency)}"
    )


def _positions_text(db_path: str) -> str:
    with connect_database(db_path) as database:
        database.init_schema()
        snapshot = _live_snapshot(database)
        if snapshot is not None:
            return _truncate(_snapshot_positions_text(snapshot) + "\n" + _cost_text())
        live = database.list_open_live_positions()
        paper = database.paper_positions()
    lines = ["open positions"]
    lines.extend(f"live {row['symbol']} qty={row['quantity']}" for row in live)
    lines.extend(f"paper {position.symbol} qty={position.quantity}" for position in paper)
    return _truncate(("\n".join(lines) if len(lines) > 1 else "open positions\nnone") + "\n" + _cost_text())


def _report_text(db_path: str) -> str:
    with connect_database(db_path) as database:
        database.init_schema()
        snapshot = _live_snapshot(database)
        if snapshot is not None:
            return _truncate("live broker report\n" + _snapshot_report_text(snapshot) + "\n" + _cost_text())
        report = report_from_database(database)
    return (
        "paper report\n"
        f"orders={report.total_paper_orders} fills={report.total_paper_fills} "
        f"open_positions={len(report.open_positions)} realized_pnl={report.realized_pnl:g}\n"
        f"symbols={','.join(report.symbols) or 'none'}\n"
        f"{_cost_text()}"
    )


def _live_report_text(db_path: str) -> str:
    with connect_database(db_path) as database:
        database.init_schema()
        raw = database.get_runtime_metadata("live_report_snapshot")
    if not raw:
        return "live report\nnone"
    try:
        payload = json.loads(raw)
    except (TypeError, ValueError):
        return "live report\nunavailable"
    return "live report\n" + json.dumps(payload, ensure_ascii=False, indent=2)


def _safe_flag(value: str | None) -> str:
    if value is None:
        return "unavailable"
    return "true" if value.strip().lower() in {"1", "true", "yes", "on"} else "false"


def _heartbeat_status(value: str | None) -> tuple[str, bool]:
    if not value:
        return "unavailable", False
    try:
        timestamp = datetime.fromisoformat(value)
        if timestamp.tzinfo is None:
            timestamp = timestamp.replace(tzinfo=timezone.utc)
        age = (_utcnow() - timestamp.astimezone(timezone.utc)).total_seconds()
        return (f"age={age:.1f}s" if age >= 0 else "future"), 0 <= age <= HEARTBEAT_MAX_AGE_SECONDS
    except (TypeError, ValueError):
        return "unavailable", False


def _live_snapshot(database: Any) -> dict[str, Any] | None:
    raw = database.get_runtime_metadata(LIVE_REPORT_SNAPSHOT_KEY)
    if not raw:
        return None
    try:
        value = json.loads(raw)
    except (TypeError, ValueError):
        return None
    return value if isinstance(value, dict) else None


def _snapshot_positions_text(snapshot: dict[str, Any]) -> str:
    positions = snapshot.get("positions") or snapshot.get("account_positions") or []
    lines = ["live broker positions (metadata snapshot)"]
    if isinstance(positions, list):
        for item in positions[:50]:
            if isinstance(item, dict) and str(item.get("symbol", "")).isdigit():
                lines.append(
                    f"{item['symbol']} qty={item.get('qty', item.get('quantity', 'unknown'))} "
                    f"avg={item.get('avg_price', item.get('entry_price', 'unknown'))}"
                )
    if len(lines) == 1:
        lines.append("none")
    lines.append(f"reconciliation={_safe_flag(str(snapshot.get('reconciliation_ok', '')))}")
    return _truncate("\n".join(lines))


def _snapshot_report_text(snapshot: dict[str, Any]) -> str:
    summary = snapshot.get("summary") or snapshot.get("account") or {}
    if not isinstance(summary, dict):
        summary = {}
    lines = [
        f"environment={snapshot.get('environment', 'live')}",
        f"cash={summary.get('orderable_cash', summary.get('cash', 'unknown'))}",
        f"total_eval={summary.get('total_eval', summary.get('total_evaluation', 'unknown'))}",
        f"daily_pnl={summary.get('daily_pnl', 'unknown')}",
        f"positions={len(snapshot.get('positions') or snapshot.get('account_positions') or [])}",
        f"operator_review={_safe_flag(str(snapshot.get('operator_review', '')))}",
        f"block_new_entries={_safe_flag(str(snapshot.get('block_new_entries', '')))}",
    ]
    return "\n".join(lines)


def _truncate(text: str, limit: int = 3500) -> str:
    return text if len(text) <= limit else text[: limit - 20] + "\n...[truncated]"


def _approval_markup(rows: list[Any]) -> dict[str, Any] | None:
    buttons = []
    for row in rows[:10]:
        request_id = str(row["request_id"])
        buttons.append([
            {"text": f"Approve {request_id}", "callback_data": f"approval:approve:{request_id}"},
            {"text": f"Reject {request_id}", "callback_data": f"approval:reject:{request_id}"},
        ])
    return {"inline_keyboard": buttons} if buttons else None


def _approvals_text(db_path: str) -> tuple[str, dict[str, Any] | None]:
    with connect_database(db_path) as database:
        database.init_schema()
        database.expire_approval_requests(now=_utcnow())
        rows = database.list_approval_requests(status="PENDING", limit=10)
    lines = ["pending approvals"]
    for row in rows:
        lines.append(
            f"{row['request_id']} {row['symbol']} qty={row['quantity'] or 'unknown'} "
            f"entry={row['entry_price'] or 'unknown'} expires={row['expires_at'] or 'unknown'}"
        )
    if len(lines) == 1:
        lines.append("none")
    return _truncate("\n".join(lines)), _approval_markup(rows)


def _cost_text() -> str:
    return openai_cost_summary_from_env().text()


def _orders_text(db_path: str) -> str:
    with connect_database(db_path) as database:
        database.init_schema()
        rows = database.connection.execute(
            "SELECT symbol, side, requested_qty, filled_qty, status, requested_price "
            "FROM broker_orders ORDER BY created_at DESC LIMIT 20"
        ).fetchall()
    lines = ["broker orders"]
    lines.extend(
        f"{row['symbol']} {row['side']} qty={row['requested_qty']} filled={row['filled_qty']} "
        f"price={row['requested_price']:g} status={row['status']}" for row in rows
    )
    return "\n".join(lines) if len(lines) > 1 else "broker orders\nnone"


def _fills_text(db_path: str) -> str:
    with connect_database(db_path) as database:
        database.init_schema()
        rows = database.connection.execute(
            "SELECT symbol, side, quantity, price, filled_at "
            "FROM broker_fills ORDER BY filled_at DESC LIMIT 20"
        ).fetchall()
    lines = ["broker fills"]
    lines.extend(
        f"{row['symbol']} {row['side']} qty={row['quantity']} price={row['price']:g} "
        f"at={row['filled_at']}" for row in rows
    )
    return "\n".join(lines) if len(lines) > 1 else "broker fills\nnone"


def _command(update: dict[str, Any]) -> tuple[str, str | int, str | int | None] | None:
    message = update.get("message") or {}
    text = str(message.get("text") or "").strip()
    chat_id = (message.get("chat") or {}).get("id")
    if not text or chat_id is None or not text.startswith("/"):
        return None
    parts = text.split(maxsplit=1)
    command = parts[0].split("@", 1)[0].lower()
    argument = parts[1].strip() if len(parts) > 1 else ""
    user_id = (message.get("from") or {}).get("id")
    if command in {"/pause", "/resume"}:
        argument = argument or "telegram_operator"
    return command + (" " + argument if argument and command in {
        "/pause", "/resume", "/env", "/environment", "/confirm-real", "/approve", "/reject",
    } else ""), chat_id, user_id


def _callback(update: dict[str, Any]) -> tuple[str, str | int, str, str | int | None] | None:
    callback = update.get("callback_query") or {}
    message = callback.get("message") or {}
    chat_id = (message.get("chat") or {}).get("id")
    data = str(callback.get("data") or "")
    callback_id = callback.get("id")
    if chat_id is None or not callback_id or not (
        data.startswith("control:") or data.startswith("approval:")
    ):
        return None
    return data, chat_id, callback_id, (callback.get("from") or {}).get("id")


def handle_update(
    update: dict[str, Any], db_path: str, client: Any,
    allowed_chat_id: str | int, allowed_user_id: str | int | None = None,
) -> bool:
    """Handle one authorized update and return whether it was actionable."""
    command = _command(update)
    callback = _callback(update)
    chat_id = command[1] if command else callback[1] if callback else None
    user_id = command[2] if command else callback[3] if callback else None
    if chat_id is None or str(chat_id) != str(allowed_chat_id):
        return False
    if allowed_user_id is not None and str(user_id) != str(allowed_user_id):
        return False
    if callback:
        action, chat_id, callback_id, _ = callback
        client.answer_callback_query(callback_id)
        command_name = "/approval-callback" if action.startswith("approval:") else "/" + action.split(":", 1)[1]
    else:
        assert command is not None
        command_name, chat_id = command[0].split(" ", 1)[0], command[1]
    reply_markup = None
    if command_name == "/approval-callback":
        _, decision, request_id = action.split(":", 2)
        with connect_database(db_path) as database:
            database.init_schema()
            changed = database.resolve_approval_request(
                request_id, "APPROVED" if decision == "approve" else "REJECTED",
                resolved_by=str(user_id) if user_id is not None else None,
                now=_utcnow(),
            )
            row = database.get_approval_request(request_id)
        text = (
            f"approval {request_id}: {'approved' if changed and decision == 'approve' else 'rejected' if changed else row['status'].lower() if row else 'not found'}"
        )
    elif command_name in {"/approve", "approve", "/reject", "reject"}:
        request_id = command[0].split(" ", 1)[1].strip() if command and " " in command[0] else ""
        decision = "APPROVED" if command_name in {"/approve", "approve"} else "REJECTED"
        if not request_id:
            text = f"usage: {'/approve' if decision == 'APPROVED' else '/reject'} <request_id>"
        else:
            with connect_database(db_path) as database:
                database.init_schema()
                changed = database.resolve_approval_request(
                    request_id, decision, resolved_by=str(user_id) if user_id is not None else None,
                    now=_utcnow(),
                )
                row = database.get_approval_request(request_id)
            text = (
                f"approval {request_id}: {'approved' if changed and decision == 'APPROVED' else 'rejected' if changed else row['status'].lower() if row else 'not found'}"
            )
    elif command_name in {"/approvals", "approvals"}:
        text, reply_markup = _approvals_text(db_path)
    elif command_name in {"/pause", "pause"}:
        reason = "telegram_operator"
        if command and " " in command[0]:
            reason = command[0].split(" ", 1)[1]
        set_paused(db_path, True, reason, "telegram")
        text = "runtime paused"
    elif command_name in {"/resume", "resume"}:
        reason = "telegram_operator"
        if command and " " in command[0]:
            reason = command[0].split(" ", 1)[1]
        if _emergency_stop_active(db_path):
            text = "resume rejected: emergency stop is active; use /clear-emergency while paused"
        elif not _real_resume_allowed(db_path):
            text = "resume rejected: real environment requires a fresh challenge confirmation"
        else:
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
        elif value == "real":
            code = _real_challenge(db_path)
            text = f"runtime environment: real pending\nchallenge: {code}\nconfirm with /confirm-real {code}"
        else:
            try:
                selected = set_environment(db_path, value, "telegram_operator", "telegram")
            except ValueError as exc:
                text = f"environment unchanged: {exc}"
            else:
                text = f"runtime environment: {selected.environment}"
    elif command_name in {"/confirm-real", "confirm-real"}:
        code = command[0].split(" ", 1)[1] if command and " " in command[0] else ""
        text = _confirm_real(db_path, code)
    elif command_name in {"/positions", "positions"}:
        text = _positions_text(db_path)
    elif command_name in {"/orders", "orders"}:
        text = _orders_text(db_path)
    elif command_name in {"/fills", "fills"}:
        text = _fills_text(db_path)
    elif command_name in {"/emergency-stop", "emergency-stop"}:
        set_paused(db_path, True, "telegram_emergency_stop", "telegram")
        _set_metadata(db_path, EMERGENCY_STOP_KEY, "true")
        _set_metadata(db_path, REAL_RESUME_ARM_EXPIRES_KEY, "")
        _set_metadata(db_path, REAL_RESUME_ARM_USES_KEY, "0")
        text = "emergency stop active; runtime paused"
    elif command_name in {"/environment:demo", "/environment:real"}:
        value = command_name.split(":", 1)[1]
        if value == "real":
            code = _real_challenge(db_path)
            text = f"runtime environment: real pending\nchallenge: {code}\nconfirm with /confirm-real {code}"
        else:
            try:
                selected = set_environment(db_path, value, "telegram_operator", "telegram")
            except ValueError as exc:
                text = f"environment unchanged: {exc}"
            else:
                text = f"runtime environment: {selected.environment}"
    elif command_name in {"/clear-emergency", "clear-emergency"}:
        with connect_database(db_path) as database:
            database.init_schema()
            control = database.get_runtime_control()
            if not control.paused:
                text = "clear emergency rejected: runtime must be paused"
            else:
                database.set_runtime_metadata(EMERGENCY_STOP_KEY, "false")
                text = "emergency stop cleared; runtime remains paused"
    elif command_name in {"/report", "report"}:
        text = _report_text(db_path)
    elif command_name in {"/live-report", "live-report"}:
        text = _live_report_text(db_path)
    elif command_name in {"/cost", "cost"}:
        text = _cost_text()
    else:
        return False
    client.send_message(
        chat_id, _truncate(text),
        reply_markup=reply_markup or (
            KEYBOARD if command_name in {"/control", "control", "/environment:demo", "/environment:real"}
            else None
        ),
    )
    return True


def poll_telegram(
    db_path: str,
    token: str,
    allowed_chat_id: str | int,
    allowed_user_id: str | int | None = None,
    *,
    limit: int = 10,
    timeout_seconds: int = 0,
    client: Any | None = None,
) -> int:
    if not 1 <= limit <= 100:
        raise ValueError("limit must be between 1 and 100")
    if not 0 <= timeout_seconds <= 60:
        raise ValueError("timeout_seconds must be between 0 and 60")
    if allowed_user_id is None:
        allowed_user_id = optional_env_value("TELEGRAM_ALLOWED_USER_ID")
    telegram = client or TelegramClient(token)
    with connect_database(db_path) as database:
        database.init_schema()
        offset = database.get_telegram_update_offset()
    updates = telegram.get_updates(offset=offset, limit=limit, timeout=timeout_seconds)
    max_update_id = offset - 1 if offset is not None else None
    for update in sorted(updates[:limit], key=lambda item: int(item.get("update_id", -1))):
        handle_update(update, db_path, telegram, allowed_chat_id, allowed_user_id)
        if "update_id" in update:
            max_update_id = max(max_update_id if max_update_id is not None else -1, int(update["update_id"]))
    if max_update_id is not None:
        with connect_database(db_path) as database:
            database.init_schema()
            database.set_telegram_update_offset(max_update_id + 1)
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
