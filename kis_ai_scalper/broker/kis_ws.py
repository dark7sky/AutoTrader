"""Read-only KIS domestic-stock realtime price WebSocket helpers."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
import json
import time
from datetime import date, datetime, time as datetime_time
from typing import Any

from kis_ai_scalper.market.tick import MarketTick

TR_REALTIME_PRICE = "H0STCNT0"


@dataclass(frozen=True)
class RealtimePrice:
    symbol: str
    timestamp: str
    price: float
    volume: int
    total_volume: int


def realtime_price_to_market_tick(price: RealtimePrice, trading_date: date) -> MarketTick:
    """Convert KIS HHMMSS trade time to a naive KST market timestamp."""
    if len(price.timestamp) != 6 or not price.timestamp.isdigit():
        raise ValueError("KIS realtime timestamp must be HHMMSS")
    parsed = datetime_time(
        int(price.timestamp[0:2]), int(price.timestamp[2:4]), int(price.timestamp[4:6])
    )
    return MarketTick(price.symbol, datetime.combine(trading_date, parsed), price.price, price.volume)


@dataclass(frozen=True)
class WebSocketSmokeResult:
    acknowledged: bool
    ticks: tuple[RealtimePrice, ...]


def build_subscription(approval_key: str, symbol: str, tr_id: str = TR_REALTIME_PRICE) -> str:
    if not approval_key:
        raise ValueError("approval_key is required")
    if not symbol.isdigit() or len(symbol) != 6:
        raise ValueError("domestic stock symbol must be a six-digit code")
    if tr_id != TR_REALTIME_PRICE:
        raise ValueError("read-only smoke supports H0STCNT0 only")
    return json.dumps({
        "header": {
            "approval_key": approval_key,
            "custtype": "P",
            "tr_type": "1",
            "content-type": "utf-8",
        },
        "body": {"input": {"tr_id": tr_id, "tr_key": symbol}},
    }, separators=(",", ":"))


def raw_to_text(raw: str | bytes) -> str:
    return raw.decode("utf-8", errors="replace") if isinstance(raw, bytes) else raw


def parse_system_message(raw: str | bytes) -> dict[str, Any] | None:
    """Parse JSON ack/heartbeat messages; return None for realtime pipe data."""
    text = raw_to_text(raw).strip()
    if not text.startswith("{"):
        return None
    try:
        payload = json.loads(text)
    except json.JSONDecodeError:
        return None
    if not isinstance(payload, dict):
        return None
    header = payload.get("header")
    body = payload.get("body")
    if not isinstance(header, dict):
        return None
    result: dict[str, Any] = {"header": header}
    if isinstance(body, dict):
        result["body"] = body
    return result


def is_pingpong(message: dict[str, Any] | None) -> bool:
    if not message:
        return False
    header = message.get("header", {})
    return str(header.get("tr_id", "")).upper() == "PINGPONG"


def is_subscription_ack(message: dict[str, Any] | None) -> bool:
    if not message:
        return False
    text = json.dumps(message, ensure_ascii=False).upper()
    return "SUBSCRIBE SUCCESS" in text or "SUBSCRIBE SUCCESS" in text.replace("_", " ")


def parse_realtime_price(raw: str | bytes) -> RealtimePrice | None:
    """Parse KIS 0|H0STCNT0|count|symbol^time^price^... payload."""
    text = raw_to_text(raw).strip()
    parts = text.split("|", 3)
    if len(parts) != 4 or parts[0] != "0" or parts[1] != TR_REALTIME_PRICE:
        return None
    fields = parts[3].split("^")
    if len(fields) < 14:
        return None
    try:
        symbol = fields[0]
        timestamp = fields[1]
        price = float(fields[2])
        volume = int(fields[12])
        total_volume = int(fields[13])
    except (TypeError, ValueError):
        return None
    if not symbol.isdigit() or len(symbol) != 6 or not timestamp or price <= 0:
        return None
    if volume < 0 or total_volume < 0:
        return None
    return RealtimePrice(symbol, timestamp, price, volume, total_volume)


async def smoke_realtime_price(
    endpoint: str,
    approval_key: str,
    symbol: str,
    seconds: int = 10,
) -> WebSocketSmokeResult:
    """Subscribe to H0STCNT0 for a bounded period; never sends order messages."""
    if seconds < 1 or seconds > 60:
        raise ValueError("seconds must be between 1 and 60")
    try:
        import websockets
    except ImportError as exc:
        raise RuntimeError("websockets package is required for smoke-ws") from exc

    ticks: list[RealtimePrice] = []
    acknowledged = False
    deadline = time.monotonic() + seconds
    async with websockets.connect(endpoint, open_timeout=15, close_timeout=5) as socket:
        await socket.send(build_subscription(approval_key, symbol))
        while time.monotonic() < deadline:
            remaining = max(0.05, deadline - time.monotonic())
            try:
                raw = await asyncio.wait_for(socket.recv(), timeout=remaining)
            except (asyncio.TimeoutError, websockets.exceptions.ConnectionClosed):
                break
            system = parse_system_message(raw)
            if is_pingpong(system):
                await socket.send(raw_to_text(raw))
                continue
            if is_subscription_ack(system):
                acknowledged = True
                continue
            tick = parse_realtime_price(raw)
            if tick is not None and tick.symbol == symbol:
                ticks.append(tick)
    return WebSocketSmokeResult(acknowledged=acknowledged, ticks=tuple(ticks))
