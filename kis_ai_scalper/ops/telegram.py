"""Bounded Telegram operator control for the local paper runtime."""

from __future__ import annotations

import os
import json
import secrets
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import requests

from kis_ai_scalper.ops.control import set_environment, set_paused
from kis_ai_scalper.ops.openai_usage import openai_cost_summary_from_env
from kis_ai_scalper.ops.performance import performance_report_from_database
from kis_ai_scalper.ops.trading_frequency import (
    FREQUENCY_PRESETS,
    apply_trade_frequency_preset,
    read_trade_frequency,
)
from kis_ai_scalper.paper import report_from_database
from kis_ai_scalper.storage import connect_database
from kis_ai_scalper.market.schedule import exchange_calendar_available, is_regular_market_open


REAL_CHALLENGE_KEY = "telegram.real_challenge"
REAL_CHALLENGE_EXPIRES_KEY = "telegram.real_challenge_expires"
REAL_RESUME_ARM_EXPIRES_KEY = "telegram.real_resume_arm_expires"
REAL_RESUME_ARM_USES_KEY = "telegram.real_resume_arm_uses"
EMERGENCY_STOP_KEY = "telegram.emergency_stop"
CANCEL_OPEN_BUYS_KEY = "operator.cancel_open_buys_requested"
AUTO_PAUSE_KEY = "runtime.auto_paused"
AUTO_PAUSE_REASON_KEY = "runtime.auto_pause_reason"
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

    def edit_message_text(
        self,
        chat_id: str | int,
        message_id: int,
        text: str,
        reply_markup: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "chat_id": chat_id,
            "message_id": message_id,
            "text": text,
        }
        if reply_markup is not None:
            payload["reply_markup"] = reply_markup
        return self._call("editMessageText", payload)

    def delete_message(self, chat_id: str | int, message_id: int) -> dict[str, Any]:
        return self._call("deleteMessage", {"chat_id": chat_id, "message_id": message_id})

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

    def set_my_commands(self, commands: list[dict[str, str]]) -> dict[str, Any]:
        return self._call("setMyCommands", {"commands": commands})

    def set_chat_menu_button(
        self,
        chat_id: str | int,
        menu_button: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        return self._call(
            "setChatMenuButton",
            {"chat_id": chat_id, "menu_button": menu_button or {"type": "commands"}},
        )


BOT_COMMANDS = [
    {"command": "menu", "description": "메인 메뉴"},
    {"command": "readiness", "description": "모의매매 준비 점검"},
    {"command": "watchlist", "description": "관심종목 조회"},
    {"command": "watchlist_add", "description": "관심종목 추가"},
    {"command": "watchlist_remove", "description": "관심종목 제거"},
    {"command": "decisions", "description": "최근 AI 판단"},
    {"command": "performance", "description": "AI 성과"},
    {"command": "frequency", "description": "거래 빈도 설정"},
    {"command": "status", "description": "서비스 상태"},
    {"command": "control", "description": "운용 제어"},
]

MAIN_MENU_KEYBOARD = {
    "inline_keyboard": [
        [{"text": "모의매매 준비 점검", "callback_data": "control:readiness"}],
        [
            {"text": "상태·리포트", "callback_data": "menu:status"},
            {"text": "계좌·거래", "callback_data": "menu:trading"},
        ],
        [
            {"text": "운용 제어", "callback_data": "menu:control"},
            {"text": "환경 설정", "callback_data": "menu:environment"},
        ],
        [{"text": "AI·비용", "callback_data": "menu:ai"}],
    ]
}

STATUS_MENU_KEYBOARD = {
    "inline_keyboard": [
        [
            {"text": "현재 상태", "callback_data": "control:status"},
            {"text": "라이브 리포트", "callback_data": "control:live-report"},
        ],
        [{"text": "페이퍼 리포트", "callback_data": "control:report"}],
        [{"text": "메인 메뉴", "callback_data": "menu:main"}],
    ]
}

TRADING_MENU_KEYBOARD = {
    "inline_keyboard": [
        [
            {"text": "보유 포지션", "callback_data": "control:positions"},
            {"text": "주문", "callback_data": "control:orders"},
        ],
        [
            {"text": "체결", "callback_data": "control:fills"},
            {"text": "승인 대기", "callback_data": "control:approvals"},
        ],
        [
            {"text": "관심종목 관리", "callback_data": "control:watchlist"},
            {"text": "최근 AI 판단", "callback_data": "control:decisions"},
        ],
        [{"text": "메인 메뉴", "callback_data": "menu:main"}],
    ]
}

CONTROL_MENU_KEYBOARD = {
    "inline_keyboard": [
        [{"text": "미체결 매수 취소", "callback_data": "control:cancel-open-buys"}],
        [
            {"text": "긴급 정지", "callback_data": "control:emergency-stop"},
            {"text": "긴급 정지 해제", "callback_data": "control:clear-emergency"},
        ],
        [{"text": "거래 빈도", "callback_data": "control:frequency"}],
        [{"text": "메인 메뉴", "callback_data": "menu:main"}],
    ]
}

ENVIRONMENT_MENU_KEYBOARD = {
    "inline_keyboard": [
        [
            {"text": "모의투자", "callback_data": "control:environment:demo"},
            {"text": "실전투자", "callback_data": "control:environment:real"},
        ],
        [{"text": "메인 메뉴", "callback_data": "menu:main"}],
    ]
}

AI_MENU_KEYBOARD = {
    "inline_keyboard": [
        [
            {"text": "OpenAI 사용금액", "callback_data": "control:cost"},
            {"text": "성과", "callback_data": "control:performance"},
        ],
        [{"text": "메인 메뉴", "callback_data": "menu:main"}],
    ]
}

# Backwards-compatible name for callers that imported the original keyboard.
KEYBOARD = MAIN_MENU_KEYBOARD


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


def _request_open_buy_cancellation(db_path: str) -> bool:
    with connect_database(db_path) as database:
        database.init_schema()
        pending = database.connection.execute(
            """SELECT 1 FROM broker_orders
               WHERE side='BUY'
                 AND status IN ('ACKNOWLEDGED','PARTIALLY_FILLED','CANCEL_PENDING','UNKNOWN')
               LIMIT 1"""
        ).fetchone() is not None
        database.set_runtime_metadata(CANCEL_OPEN_BUYS_KEY, "true" if pending else "false")
    return pending


def _resume_safety_reason(db_path: str) -> str | None:
    with connect_database(db_path) as database:
        database.init_schema()
        if _safe_flag(database.get_runtime_metadata("operator_review")) == "true":
            return "operator review is active"
        if _safe_flag(database.get_runtime_metadata("block_new_entries")) == "true":
            return "new entries are blocked"
        row = database.connection.execute(
            """SELECT status FROM broker_orders
               WHERE status IN ('CANCEL_PENDING','UNKNOWN') LIMIT 1"""
        ).fetchone()
        if row is not None:
            return f"broker order is unresolved ({row['status']})"
    return None


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


def _frequency_settings(db_path: str):
    with connect_database(db_path) as database:
        database.init_schema()
        return read_trade_frequency(
            database,
            default_ai_min_confidence=float(os.getenv("AI_MIN_CONFIDENCE", "0.75")),
        )


def _frequency_keyboard() -> dict[str, Any]:
    labels = {
        "conservative": "보수",
        "normal": "표준",
        "aggressive": "적극",
    }
    return {
        "inline_keyboard": [
            [
                {"text": labels[name], "callback_data": f"frequency:set:{name}"}
                for name in ("conservative", "normal", "aggressive")
            ],
            [{"text": "운용 제어", "callback_data": "menu:control"}],
            [{"text": "메인 메뉴", "callback_data": "menu:main"}],
        ]
    }


def _frequency_text(db_path: str) -> str:
    settings = _frequency_settings(db_path)
    presets = "\n".join(
        f"- {name}: ai_min_confidence={preset.ai_min_confidence:.2f} "
        f"candidate_sensitivity={name}"
        for name, preset in FREQUENCY_PRESETS.items()
    )
    return (
        "거래 빈도\n"
        f"profile={settings.profile}\n"
        f"ai_min_confidence={settings.ai_min_confidence:.2f}\n"
        f"candidate_sensitivity={settings.profile}\n"
        "daily_trade_limit=none\n"
        "presets:\n"
        f"{presets}"
    )


def _set_frequency_text(db_path: str, profile: str) -> str:
    with connect_database(db_path) as database:
        database.init_schema()
        try:
            settings = apply_trade_frequency_preset(database, profile)
        except ValueError:
            return "usage: /frequency conservative|normal|aggressive"
    return (
        "거래 빈도 설정 완료\n"
        f"profile={settings.profile}\n"
        f"ai_min_confidence={settings.ai_min_confidence:.2f}\n"
        f"candidate_sensitivity={settings.profile}\n"
        "daily_trade_limit=none"
    )


def _status_text(db_path: str) -> str:
    with connect_database(db_path) as database:
        database.init_schema()
        control = database.get_runtime_control()
        environment = control.environment
        live_positions = database.list_open_live_positions()
        paper_positions = database.paper_positions()
        heartbeat = database.get_heartbeat("trading-service")
        fill_heartbeat = database.get_heartbeat("fill-notice")
        supervisor_heartbeat = database.get_heartbeat("order-supervisor")
        fill_status = database.get_runtime_metadata("fill-notice:status")
        supervisor_status = database.get_runtime_metadata("order-supervisor.status")
        operator_review = database.get_runtime_metadata("operator_review")
        block_entries = database.get_runtime_metadata("block_new_entries")
        emergency = database.get_runtime_metadata(EMERGENCY_STOP_KEY) or database.get_runtime_metadata("emergency_stop")
        auto_paused = database.get_runtime_metadata(AUTO_PAUSE_KEY)
        auto_pause_reason = database.get_runtime_metadata(AUTO_PAUSE_REASON_KEY)
        cancel_open_buys = database.get_runtime_metadata(CANCEL_OPEN_BUYS_KEY)
    frequency = _frequency_settings(db_path)
    automatically_blocked = (
        _safe_flag(auto_paused) == "true"
        or _safe_flag(operator_review) == "true"
        or _safe_flag(block_entries) == "true"
    )
    state = "paused" if control.paused else "auto_paused" if automatically_blocked else "running"
    heartbeat_text, heartbeat_healthy = _heartbeat_status(heartbeat)
    fill_heartbeat_text, fill_healthy = _heartbeat_status(fill_heartbeat)
    supervisor_heartbeat_text, supervisor_healthy = _heartbeat_status(supervisor_heartbeat)
    supervisor_reasons = _supervisor_reasons(supervisor_status)
    return (
        f"runtime: {state}\n"
        f"environment: {environment}\n"
        f"open_live_positions: {len(live_positions)}\n"
        f"open_paper_positions: {len(paper_positions)}\n"
        f"reason: {control.reason}\nsource: {control.source}\nupdated_at: {control.updated_at}\n"
        f"trading_service: {heartbeat_text} healthy={heartbeat_healthy}\n"
        f"fill_notice: {_worker_status(fill_status)} heartbeat={fill_heartbeat_text} healthy={fill_healthy}\n"
        f"order_supervisor: {_worker_status(supervisor_status)} heartbeat={supervisor_heartbeat_text} healthy={supervisor_healthy}\n"
        f"operator_review_reasons: {','.join(supervisor_reasons) or 'none'}\n"
        f"operator_review: {_safe_flag(operator_review)}\n"
        f"block_new_entries: {_safe_flag(block_entries)}\n"
        f"emergency_stop: {_safe_flag(emergency)}\n"
        f"automatic_pause: {_safe_flag(auto_paused)} reason={auto_pause_reason or 'none'}\n"
        f"cancel_open_buys_pending: {_safe_flag(cancel_open_buys)}\n"
        f"trade_frequency: profile={frequency.profile} "
        f"ai_min_confidence={frequency.ai_min_confidence:.2f} "
        f"candidate_sensitivity={frequency.profile} "
        "daily_trade_limit=none"
    )


def _required_env_status(names: tuple[str, ...]) -> tuple[bool, str]:
    missing = [name for name in names if optional_env_value(name) is None]
    return not missing, ",".join(missing) if missing else "ok"


def _readiness(db_path: str) -> tuple[str, bool]:
    """Return a compact preflight view; a closed market is informational only."""
    now = _utcnow()
    blockers: list[str] = []
    with connect_database(db_path) as database:
        database.init_schema()
        control = database.get_runtime_control()
        environment = control.environment
        watchlist = database.list_watchlist_symbols()
        operator_review = _safe_flag(database.get_runtime_metadata("operator_review"))
        block_entries = _safe_flag(database.get_runtime_metadata("block_new_entries"))
        emergency = _safe_flag(
            database.get_runtime_metadata(EMERGENCY_STOP_KEY)
            or database.get_runtime_metadata("emergency_stop")
        )
        cancel_pending = _safe_flag(database.get_runtime_metadata(CANCEL_OPEN_BUYS_KEY))
        supervisor_status = database.get_runtime_metadata("order-supervisor.status")
        unresolved = database.connection.execute(
            "SELECT 1 FROM broker_orders WHERE status IN ('CANCEL_PENDING','UNKNOWN') LIMIT 1"
        ).fetchone() is not None
        trading_heartbeat = _heartbeat_status(database.get_heartbeat("trading-service"))[1]
        supervisor_heartbeat = _heartbeat_status(database.get_heartbeat("order-supervisor"))[1]
    supervisor_reasons = _supervisor_reasons(supervisor_status)
    supervisor_detail = _supervisor_detail(supervisor_status)

    gate = (optional_env_value("LIVE_TRADING_ENABLED") or "").strip().lower() == "true"
    if not gate:
        blockers.append("LIVE_TRADING_ENABLED=true 필요")
    prefix = "KIS_DEMO" if environment == "demo" else "KIS_REAL"
    kis_ok, kis_detail = _required_env_status((f"{prefix}_APP_KEY", f"{prefix}_APP_SECRET", f"{prefix}_ACCOUNT_NO"))
    if not kis_ok:
        blockers.append(f"{environment} KIS 누락: {kis_detail}")
    ai = (optional_env_value("AUTO_TRADE_AI") or "openai").strip().lower()
    if ai == "openai" and optional_env_value("OPENAI_API_KEY") is None:
        blockers.append("OPENAI_API_KEY 필요(AUTO_TRADE_AI=openai)")
    if not watchlist:
        blockers.append("관심종목 없음")
    if operator_review == "true":
        detail = ",".join(supervisor_reasons)
        blockers.append("operator_review=true" + (f" ({detail})" if detail else ""))
    if block_entries == "true":
        blockers.append("block_new_entries=true")
    if emergency == "true":
        blockers.append("emergency_stop=true")
    if cancel_pending == "true" or unresolved:
        blockers.append("cancellation is still pending (미해결 주문/취소 대기)")
    if not trading_heartbeat:
        blockers.append("trading-service heartbeat 없음/오래됨")
    if not supervisor_heartbeat:
        blockers.append("order-supervisor heartbeat 없음/오래됨")
    calendar_ok = exchange_calendar_available()
    market_open = is_regular_market_open(now) if calendar_ok else False
    if not calendar_ok:
        blockers.append("KRX 캘린더 사용 불가")
    lines = [
        "모의매매 준비 점검",
        f"environment={environment} paused={str(control.paused).lower()}",
        f"resume_ready={str(not blockers).lower()}",
        f"LIVE_TRADING_ENABLED={str(gate).lower()} KIS={str(kis_ok).lower()} AI={ai}",
        f"watchlist={','.join(watchlist) or 'none'}",
        f"operator_review={operator_review} block_new_entries={block_entries} emergency={emergency} cancel_pending={str(cancel_pending == 'true' or unresolved).lower()}",
        f"worker_heartbeat=trading-service:{str(trading_heartbeat).lower()} order-supervisor:{str(supervisor_heartbeat).lower()}",
        f"order_supervisor_status={supervisor_detail['status']} order_supervisor_status_age={supervisor_detail['age']}",
        (
            f"failure_streak={supervisor_detail['failure_streak']} "
            f"healthy_streak={supervisor_detail['healthy_streak']} "
            f"next_retry_seconds={supervisor_detail['next_retry_seconds']}"
        ),
        f"safe_kis_error={supervisor_detail['safe_kis_error']}",
        f"krx_market_open={str(market_open).lower()} (시장 휴장은 resume blocker 아님)",
    ]
    if blockers:
        lines.append("blockers:")
        lines.extend(f"- {item}" for item in blockers)
    else:
        lines.append("blockers: none")
    return _truncate("\n".join(lines)), not blockers


def _watchlist_text(db_path: str) -> str:
    with connect_database(db_path) as database:
        database.init_schema()
        symbols = database.list_watchlist_symbols()
    return "관심종목\n" + ("\n".join(symbols) if symbols else "none")


def _watchlist_keyboard(db_path: str) -> dict[str, Any]:
    with connect_database(db_path) as database:
        database.init_schema()
        symbols = database.list_watchlist_symbols()
    rows: list[list[dict[str, str]]] = [
        [{"text": "추가", "callback_data": "watchlist:add"}],
    ]
    rows.extend(
        [{"text": f"{symbol} 삭제", "callback_data": f"watchlist:remove:{symbol}"}]
        for symbol in symbols
    )
    rows.append([{"text": "계좌·거래", "callback_data": "menu:trading"}])
    rows.append([{"text": "메인 메뉴", "callback_data": "menu:main"}])
    return {"inline_keyboard": rows}


def _watchlist_add_prompt_text(db_path: str) -> str:
    return (
        _watchlist_text(db_path)
        + "\n\n추가할 종목코드를 메시지로 보내세요.\n"
        + "예: /watchlist_add 005930,000660"
    )


def _watchlist_remove_symbol_text(db_path: str, symbol: str) -> str:
    if not symbol.isdigit() or len(symbol) != 6:
        return "잘못된 종목코드: " + symbol + " (6자리 숫자 필요)"
    with connect_database(db_path) as database:
        database.init_schema()
        changed = database.set_watchlist_enabled(symbol, False)
        current = database.list_watchlist_symbols()
    return f"관심종목 제거: {symbol if changed else '변경 없음'}\n현재: {','.join(current) or 'none'}"


def _symbols_argument(command: tuple[str, str | int, str | int | None] | None) -> list[str]:
    if not command or " " not in command[0]:
        return []
    raw = command[0].split(" ", 1)[1]
    return [item for item in raw.replace(",", " ").replace(";", " ").split() if item]


def _watchlist_change_text(db_path: str, command: tuple[str, str | int, str | int | None], *, add: bool) -> str:
    symbols = _symbols_argument(command)
    if not symbols:
        return f"usage: {'/watchlist-add' if add else '/watchlist-remove'} <6자리코드들>"
    invalid = [symbol for symbol in symbols if not symbol.isdigit() or len(symbol) != 6]
    if invalid:
        return "잘못된 종목코드: " + ",".join(invalid) + " (각각 6자리 숫자 필요)"
    with connect_database(db_path) as database:
        database.init_schema()
        changed = []
        for symbol in symbols:
            if add:
                database.add_watchlist_symbol(symbol, True)
                changed.append(symbol)
            elif database.set_watchlist_enabled(symbol, False):
                changed.append(symbol)
        current = database.list_watchlist_symbols()
    action = "추가" if add else "제거"
    return f"관심종목 {action}: {','.join(changed) or '변경 없음'}\n현재: {','.join(current) or 'none'}"


def _decisions_text(db_path: str) -> str:
    with connect_database(db_path) as database:
        database.init_schema()
        cycle_raw = database.get_runtime_metadata("auto_trade:last_cycle")
        rows = database.connection.execute(
            "SELECT created_at,symbol,action,confidence,entry_price,take_profit_price,stop_loss_price,risk_level "
            "FROM ai_decision_audits ORDER BY created_at DESC LIMIT 10"
        ).fetchall()
    lines = ["최근 AI 판단"]
    if cycle_raw:
        try:
            cycle = json.loads(cycle_raw)
        except (TypeError, ValueError):
            cycle = None
        if isinstance(cycle, dict):
            lines.append(f"cycle observed_at={cycle.get('observed_at', 'unknown')} environment={cycle.get('environment', 'unknown')} ai={cycle.get('ai', 'unknown')}")
            results = cycle.get("results") or []
            for result in results[:10]:
                if isinstance(result, dict):
                    lines.append(
                        f"cycle {result.get('symbol','?')} action={result.get('action','?')} reason={str(result.get('reason','?'))[:80]} "
                        f"submitted={str(result.get('submitted', False)).lower()} qty={result.get('quantity', 0)}"
                    )
    else:
        lines.append("아직 장중 사이클 없음")
    if rows:
        lines.append("AI audit:")
        for row in rows:
            def price(value: Any) -> str:
                return "none" if value is None else f"{value:g}"
            lines.append(
                f"{row['created_at']} {row['symbol']} action={row['action']} conf={row['confidence']:g} "
                f"entry={price(row['entry_price'])} target={price(row['take_profit_price'])} stop={price(row['stop_loss_price'])} risk={row['risk_level']}"
            )
    else:
        lines.append("AI audit: none")
    return _truncate("\n".join(lines))


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


def _performance_text(db_path: str) -> str:
    with connect_database(db_path) as database:
        database.init_schema()
        report = performance_report_from_database(database)
    return report.text()


def _safe_flag(value: str | None) -> str:
    if value is None:
        return "unavailable"
    return "true" if value.strip().lower() in {"1", "true", "yes", "on"} else "false"


def _worker_status(value: str | None) -> str:
    if not value:
        return "unavailable"
    candidate = value
    try:
        payload = json.loads(value)
        if isinstance(payload, dict):
            candidate = str(payload.get("status") or "unavailable")
    except (TypeError, ValueError):
        pass
    return candidate if candidate.replace("_", "").replace("-", "").isalnum() else "unavailable"


def _supervisor_reasons(value: str | None) -> list[str]:
    if not value:
        return []
    try:
        payload = json.loads(value)
    except (TypeError, ValueError):
        return []
    if not isinstance(payload, dict) or not isinstance(payload.get("reasons"), list):
        return []
    reasons = []
    for item in payload["reasons"][:5]:
        reason = str(item).replace("\n", " ").replace("\r", " ").strip()[:80]
        if reason:
            reasons.append(reason)
    return reasons


def _supervisor_detail(value: str | None) -> dict[str, str]:
    detail = {
        "status": "unavailable",
        "age": "unavailable",
        "failure_streak": "0",
        "healthy_streak": "0",
        "next_retry_seconds": "0",
        "safe_kis_error": "none",
    }
    if not value:
        return detail
    try:
        payload = json.loads(value)
    except (TypeError, ValueError):
        return detail
    if not isinstance(payload, dict):
        return detail
    status = str(payload.get("status") or "unavailable")
    detail["status"] = status if status.replace("_", "").replace("-", "").isalnum() else "unavailable"
    updated_at = payload.get("updated_at")
    if updated_at:
        try:
            timestamp = datetime.fromisoformat(str(updated_at))
            if timestamp.tzinfo is None:
                timestamp = timestamp.replace(tzinfo=timezone.utc)
            age = (_utcnow() - timestamp.astimezone(timezone.utc)).total_seconds()
            detail["age"] = f"{age:.1f}s" if age >= 0 else "future"
        except (TypeError, ValueError):
            pass
    for key in ("failure_streak", "healthy_streak", "next_retry_seconds"):
        try:
            detail[key] = str(max(0, int(payload.get(key, 0))))
        except (TypeError, ValueError):
            detail[key] = "0"
    safe = payload.get("safe_kis_error")
    if isinstance(safe, dict):
        http = _safe_token(safe.get("http_status"))
        rt_cd = _safe_token(safe.get("rt_cd"))
        msg_cd = _safe_token(safe.get("msg_cd"))
        parts = []
        if http:
            parts.append(f"http_{http}")
        if rt_cd:
            parts.append(f"rt_cd={rt_cd}")
        if msg_cd:
            parts.append(f"msg_cd={msg_cd}")
        if parts:
            detail["safe_kis_error"] = " ".join(parts)
    return detail


def _safe_token(value: Any) -> str:
    text = str(value or "").strip()
    return text if text and all(character.isalnum() or character in {"-", "_"} for character in text) else ""


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
    buttons.append([
        {"text": "계좌·거래", "callback_data": "menu:trading"},
        {"text": "메인 메뉴", "callback_data": "menu:main"},
    ])
    return {"inline_keyboard": buttons}


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
        "/watchlist-add", "/watchlist-remove", "/watchlist_add", "/watchlist_remove",
        "/frequency",
    } else ""), chat_id, user_id


def _callback(
    update: dict[str, Any],
) -> tuple[str, str | int, str, str | int | None, int | None] | None:
    callback = update.get("callback_query") or {}
    message = callback.get("message") or {}
    chat_id = (message.get("chat") or {}).get("id")
    data = str(callback.get("data") or "")
    callback_id = callback.get("id")
    if chat_id is None or not callback_id or not (
        data.startswith("control:")
        or data.startswith("approval:")
        or data.startswith("menu:")
        or data.startswith("watchlist:")
        or data.startswith("frequency:")
    ):
        return None
    message_id = message.get("message_id")
    return (
        data,
        chat_id,
        callback_id,
        (callback.get("from") or {}).get("id"),
        int(message_id) if message_id is not None else None,
    )


def _default_keyboard(command_name: str) -> dict[str, Any]:
    if command_name in {"/readiness", "readiness", "/decisions", "decisions"}:
        return MAIN_MENU_KEYBOARD if command_name in {"/readiness", "readiness"} else TRADING_MENU_KEYBOARD
    if command_name in {
        "/status", "status", "/report", "report", "/live-report", "live-report",
    }:
        return STATUS_MENU_KEYBOARD
    if command_name in {
        "/positions", "positions", "/orders", "orders", "/fills", "fills",
        "/approvals", "approvals", "/approval-callback", "/approve", "approve",
        "/reject", "reject",
    }:
        return TRADING_MENU_KEYBOARD
    if command_name in {
        "/pause", "pause", "/resume", "resume", "/control", "control",
        "/emergency-stop", "emergency-stop", "/clear-emergency", "clear-emergency",
        "/cancel-open-buys", "cancel-open-buys",
    }:
        return CONTROL_MENU_KEYBOARD
    if command_name in {
        "/frequency", "frequency",
        "/frequency:set:conservative", "/frequency:set:normal", "/frequency:set:aggressive",
    }:
        return _frequency_keyboard()
    if command_name in {
        "/env", "env", "/environment", "environment", "/confirm-real", "confirm-real",
        "/environment:demo", "/environment:real",
    }:
        return ENVIRONMENT_MENU_KEYBOARD
    if command_name in {"/cost", "cost", "/performance", "performance"}:
        return AI_MENU_KEYBOARD
    if command_name in {
        "/watchlist", "watchlist", "/watchlist-add", "watchlist-add", "/watchlist-remove", "watchlist-remove",
        "/watchlist_add", "watchlist_add", "/watchlist_remove", "watchlist_remove",
        "/watchlist:add", "/watchlist:remove",
    }:
        return TRADING_MENU_KEYBOARD
    return MAIN_MENU_KEYBOARD


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
        action, chat_id, callback_id, _, callback_message_id = callback
        client.answer_callback_query(callback_id)
        if action.startswith("approval:"):
            command_name = "/approval-callback"
        elif action.startswith("menu:"):
            command_name = "/" + action
        elif action.startswith("watchlist:"):
            command_name = "/" + action
        elif action.startswith("frequency:"):
            command_name = "/" + action
        else:
            command_name = "/" + action.split(":", 1)[1]
    else:
        assert command is not None
        command_name, chat_id = command[0].split(" ", 1)[0], command[1]
        callback_message_id = None
    reply_markup = None
    if command_name in {"/start", "/menu", "/menu:main"}:
        text = "메인 메뉴"
        reply_markup = MAIN_MENU_KEYBOARD
    elif command_name == "/menu:status":
        text = _status_text(db_path)
        reply_markup = STATUS_MENU_KEYBOARD
    elif command_name == "/menu:trading":
        text = "계좌·거래 메뉴"
        reply_markup = TRADING_MENU_KEYBOARD
    elif command_name == "/menu:control":
        text = _status_text(db_path)
        reply_markup = CONTROL_MENU_KEYBOARD
    elif command_name == "/menu:environment":
        text = _status_text(db_path)
        reply_markup = ENVIRONMENT_MENU_KEYBOARD
    elif command_name == "/menu:ai":
        text = "AI·비용 메뉴"
        reply_markup = AI_MENU_KEYBOARD
    elif command_name == "/readiness":
        text, _ = _readiness(db_path)
    elif command_name == "/watchlist":
        text = _watchlist_text(db_path)
        reply_markup = _watchlist_keyboard(db_path)
    elif command_name == "/watchlist:add":
        text = _watchlist_add_prompt_text(db_path)
        reply_markup = _watchlist_keyboard(db_path)
    elif command_name.startswith("/watchlist:remove:"):
        text = _watchlist_remove_symbol_text(db_path, command_name.rsplit(":", 1)[1])
        reply_markup = _watchlist_keyboard(db_path)
    elif command_name in {"/watchlist-add", "/watchlist-remove", "/watchlist_add", "/watchlist_remove"}:
        text = _watchlist_change_text(db_path, command, add=command_name in {"/watchlist-add", "/watchlist_add"})
        reply_markup = _watchlist_keyboard(db_path)
    elif command_name == "/frequency":
        profile = command[0].split(" ", 1)[1].strip() if command and " " in command[0] else ""
        text = _set_frequency_text(db_path, profile) if profile else _frequency_text(db_path)
        reply_markup = _frequency_keyboard()
    elif command_name.startswith("/frequency:set:"):
        text = _set_frequency_text(db_path, command_name.rsplit(":", 1)[1])
        reply_markup = _frequency_keyboard()
    elif command_name == "/decisions":
        text = _decisions_text(db_path)
    elif command_name == "/approval-callback":
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
        _set_metadata(db_path, EMERGENCY_STOP_KEY, "true")
        set_paused(db_path, True, "telegram_emergency_stop", "telegram")
        cancellation_requested = _request_open_buy_cancellation(db_path)
        text = (
            "emergency stop active; runtime paused; open BUY cancellation requested"
            if cancellation_requested
            else "emergency stop active; runtime paused; no open BUY orders"
        )
    elif command_name in {"/resume", "resume"}:
        reason = "telegram_operator"
        if command and " " in command[0]:
            reason = command[0].split(" ", 1)[1]
        readiness_text, resume_ready = _readiness(db_path)
        resume_safety_reason = _resume_safety_reason(db_path)
        if _emergency_stop_active(db_path):
            text = "resume rejected: emergency stop is active; use /clear-emergency while paused"
        elif not resume_ready:
            blockers = readiness_text.split("blockers:\n", 1)[-1].strip()
            text = "resume rejected: readiness blockers\n" + blockers
        elif (_metadata(db_path, CANCEL_OPEN_BUYS_KEY) or "").strip().lower() in {"1", "true", "yes", "on"}:
            text = "resume rejected: open BUY cancellation is still pending"
        elif resume_safety_reason is not None:
            text = f"resume rejected: {resume_safety_reason}"
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
        _set_metadata(db_path, EMERGENCY_STOP_KEY, "true")
        set_paused(db_path, True, "telegram_emergency_stop", "telegram")
        cancellation_requested = _request_open_buy_cancellation(db_path)
        _set_metadata(db_path, REAL_RESUME_ARM_EXPIRES_KEY, "")
        _set_metadata(db_path, REAL_RESUME_ARM_USES_KEY, "0")
        text = (
            "emergency stop active; runtime paused; open BUY cancellation requested"
            if cancellation_requested
            else "emergency stop active; runtime paused; no open BUY orders"
        )
    elif command_name in {"/cancel-open-buys", "cancel-open-buys"}:
        text = (
            "open BUY cancellation requested; new entries temporarily blocked"
            if _request_open_buy_cancellation(db_path)
            else "no open BUY orders"
        )
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
        if not _real_resume_allowed(db_path):
            text = "clear emergency rejected: real environment requires a fresh challenge confirmation"
        else:
            with connect_database(db_path) as database:
                database.init_schema()
                database.set_runtime_metadata(EMERGENCY_STOP_KEY, "false")
                database.set_runtime_metadata("emergency_stop", "false")
                database.set_runtime_paused(
                    False, "emergency_stop_cleared", "telegram"
                )
            text = "emergency stop cleared; automatic runtime control restored"
    elif command_name in {"/report", "report"}:
        text = _report_text(db_path)
    elif command_name in {"/live-report", "live-report"}:
        text = _live_report_text(db_path)
    elif command_name in {"/cost", "cost"}:
        text = _cost_text()
    elif command_name in {"/performance", "performance"}:
        text = _performance_text(db_path)
    else:
        return False
    response_text = _truncate(text)
    response_markup = reply_markup or _default_keyboard(command_name)
    if callback and callback_message_id is not None and hasattr(client, "edit_message_text"):
        try:
            client.edit_message_text(
                chat_id,
                callback_message_id,
                response_text,
                reply_markup=response_markup,
            )
            return True
        except RuntimeError:
            if hasattr(client, "delete_message"):
                try:
                    client.delete_message(chat_id, callback_message_id)
                except RuntimeError:
                    pass
    client.send_message(chat_id, response_text, reply_markup=response_markup)
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
