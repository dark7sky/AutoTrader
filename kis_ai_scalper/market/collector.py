"""Bounded, read-only KIS realtime tick collector."""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import date
import time
from typing import Any, AsyncIterator, Callable

from kis_ai_scalper.broker.kis_ws import (
    build_subscription,
    is_pingpong,
    is_subscription_ack,
    parse_realtime_price,
    parse_system_message,
    raw_to_text,
    realtime_price_to_market_tick,
)
from kis_ai_scalper.market.bar_builder import MinuteBarBuilder
from kis_ai_scalper.market.clock import kst_today
from kis_ai_scalper.storage import connect_database


@dataclass(frozen=True)
class CollectorResult:
    symbol: str
    ticks_saved: int
    bars_saved: int
    subscribe_ack: bool
    last_price: float | None


def _validate_seconds(seconds: int) -> None:
    if seconds < 1 or seconds > 3600:
        raise ValueError("seconds must be between 1 and 3600")


@asynccontextmanager
async def _real_socket(endpoint: str) -> AsyncIterator[Any]:
    try:
        import websockets
    except ImportError as exc:
        raise RuntimeError("websockets package is required for collect-market") from exc
    async with websockets.connect(endpoint, open_timeout=15, close_timeout=5) as socket:
        yield socket


async def collect_realtime_prices(
    endpoint: str,
    approval_key: str,
    symbol: str,
    db_path: str,
    seconds: int = 60,
    trading_date: date | None = None,
    socket_context_factory: Callable[[str], Any] | None = None,
) -> CollectorResult:
    """Collect H0STCNT0 ticks and completed bars; never sends order messages."""
    _validate_seconds(seconds)
    if not symbol.isdigit() or len(symbol) != 6:
        raise ValueError("domestic stock symbol must be a six-digit code")
    collection_date = trading_date or kst_today()
    builder = MinuteBarBuilder()
    ticks_saved = 0
    bars_saved = 0
    acknowledged = False
    last_price: float | None = None
    deadline = time.monotonic() + seconds
    context_factory = socket_context_factory or _real_socket

    with connect_database(db_path) as database:
        database.init_schema()
        async with context_factory(endpoint) as socket:
            await socket.send(build_subscription(approval_key, symbol))
            while time.monotonic() < deadline:
                remaining = max(0.05, deadline - time.monotonic())
                try:
                    raw = await asyncio.wait_for(socket.recv(), timeout=remaining)
                except asyncio.TimeoutError:
                    break
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    if exc.__class__.__name__ == "ConnectionClosed":
                        break
                    raise
                system = parse_system_message(raw)
                if is_pingpong(system):
                    await socket.send(raw_to_text(raw))
                    continue
                if is_subscription_ack(system):
                    acknowledged = True
                    continue
                realtime = parse_realtime_price(raw)
                if realtime is None or realtime.symbol != symbol:
                    continue
                tick = realtime_price_to_market_tick(realtime, collection_date)
                database.save_tick(tick)
                ticks_saved += 1
                last_price = tick.price
                completed = builder.update(tick)
                if completed is not None:
                    database.save_bar(completed)
                    bars_saved += 1
    return CollectorResult(symbol, ticks_saved, bars_saved, acknowledged, last_price)
