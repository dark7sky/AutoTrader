"""Multi-symbol KIS realtime collector with bounded reconnects."""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import date, datetime
import random
import time
from typing import Any, AsyncContextManager, Awaitable, Callable, Iterable

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
from kis_ai_scalper.market.clock import KST, kst_today
from kis_ai_scalper.market.completed_bars import materialize_completed_bars


TRANSIENT_WEBSOCKET_CLOSES = {"ConnectionClosed", "ConnectionClosedError", "ConnectionClosedOK"}


@dataclass(frozen=True)
class HealthSnapshot:
    acknowledged: bool
    last_tick_at: datetime | None
    reconnect_count: int


@dataclass(frozen=True)
class StreamingCollectorResult:
    ticks_saved: int
    bars_saved: int
    health: HealthSnapshot


def _symbols(values: Iterable[str]) -> tuple[str, ...]:
    result: list[str] = []
    seen: set[str] = set()
    for value in values:
        if not isinstance(value, str) or not value.isdigit() or len(value) != 6:
            raise ValueError("domestic stock symbols must be six-digit codes")
        if value not in seen:
            result.append(value)
            seen.add(value)
    if not result:
        raise ValueError("at least one symbol is required")
    return tuple(result)


@asynccontextmanager
async def _real_socket(endpoint: str) -> AsyncContextManager[Any]:
    try:
        import websockets
    except ImportError as exc:
        raise RuntimeError("websockets package is required for streaming collection") from exc
    async with websockets.connect(endpoint, open_timeout=15, close_timeout=5) as socket:
        yield socket


class StreamingCollector:
    """Collect multiple symbols over one KIS WebSocket connection.

    ``socket_context_factory`` and ``sleeper`` are injectable so reconnect and
    stop behavior can be tested without a network connection or real delays.
    """

    def __init__(
        self,
        *,
        endpoint: str,
        approval_key: str,
        symbols: Iterable[str],
        database: Any,
        trading_date: date | None = None,
        socket_context_factory: Callable[[str], AsyncContextManager[Any]] | None = None,
        sleeper: Callable[[float], Awaitable[None]] = asyncio.sleep,
        monotonic: Callable[[], float] = time.monotonic,
        random_source: Callable[[], float] = random.random,
        subscription_throttle: float = 0.05,
        reconnect_base: float = 0.25,
        reconnect_max: float = 5.0,
        reconnect_jitter: float = 0.2,
    ) -> None:
        if not endpoint:
            raise ValueError("endpoint is required")
        if not approval_key:
            raise ValueError("approval_key is required")
        if subscription_throttle < 0 or reconnect_base < 0 or reconnect_max < reconnect_base:
            raise ValueError("invalid throttle or reconnect bounds")
        if reconnect_jitter < 0:
            raise ValueError("reconnect_jitter must not be negative")
        self.endpoint = endpoint
        self.approval_key = approval_key
        self.symbols = _symbols(symbols)
        self.database = database
        self.trading_date = trading_date or kst_today()
        self.socket_context_factory = socket_context_factory or _real_socket
        self.sleeper = sleeper
        self.monotonic = monotonic
        self.random_source = random_source
        self.subscription_throttle = subscription_throttle
        self.reconnect_base = reconnect_base
        self.reconnect_max = reconnect_max
        self.reconnect_jitter = reconnect_jitter
        self._acknowledged = False
        self._last_tick_at: datetime | None = None
        self._reconnect_count = 0
        self._ticks_saved = 0
        self._bars_saved = 0

    @property
    def health(self) -> HealthSnapshot:
        return HealthSnapshot(self._acknowledged, self._last_tick_at, self._reconnect_count)

    def _backoff(self, reconnect_attempt: int) -> float:
        delay = min(self.reconnect_max, self.reconnect_base * (2 ** max(0, reconnect_attempt - 1)))
        return min(self.reconnect_max, delay + self.random_source() * self.reconnect_jitter)

    async def _sleep_until(self, seconds: float, deadline: float | None, stop_event: Any) -> bool:
        if stop_event is not None and stop_event.is_set():
            return False
        if deadline is not None:
            seconds = min(seconds, max(0.0, deadline - self.monotonic()))
        if seconds <= 0:
            return deadline is None or self.monotonic() < deadline
        await self.sleeper(seconds)
        return not (
            (stop_event is not None and stop_event.is_set())
            or (deadline is not None and self.monotonic() >= deadline)
        )

    async def _recv(self, socket: Any, deadline: float | None) -> Any:
        if deadline is None:
            return await socket.recv()
        remaining = deadline - self.monotonic()
        if remaining <= 0:
            raise asyncio.TimeoutError
        return await asyncio.wait_for(socket.recv(), timeout=remaining)

    async def _subscribe(self, socket: Any, deadline: float | None, stop_event: Any) -> bool:
        for index, symbol in enumerate(self.symbols):
            if index and not await self._sleep_until(self.subscription_throttle, deadline, stop_event):
                return False
            await socket.send(build_subscription(self.approval_key, symbol))
        return True

    async def _consume(self, socket: Any, builders: dict[str, MinuteBarBuilder], deadline: float | None, stop_event: Any) -> tuple[int, int, bool]:
        ticks_saved = bars_saved = 0
        while not ((stop_event is not None and stop_event.is_set()) or (deadline is not None and self.monotonic() >= deadline)):
            try:
                raw = await self._recv(socket, deadline)
            except asyncio.TimeoutError:
                break
            system = parse_system_message(raw)
            if is_pingpong(system):
                await socket.send(raw_to_text(raw))
                continue
            if is_subscription_ack(system):
                self._acknowledged = True
                continue
            realtime = parse_realtime_price(raw)
            if realtime is None or realtime.symbol not in builders:
                continue
            tick = realtime_price_to_market_tick(realtime, self.trading_date)
            self.database.save_tick(tick)
            ticks_saved += 1
            self._ticks_saved += 1
            self._last_tick_at = tick.timestamp
            completed = builders[realtime.symbol].update(tick)
            if completed is not None:
                self.database.save_bar(completed)
                bars_saved += 1
                self._bars_saved += 1
        return ticks_saved, bars_saved, True

    async def run(self, *, stop_event: Any = None, deadline: float | None = None, max_reconnects: int = 3) -> StreamingCollectorResult:
        if max_reconnects < 0:
            raise ValueError("max_reconnects must not be negative")
        self.database.init_schema()
        builders = {symbol: MinuteBarBuilder() for symbol in self.symbols}
        reconnect_attempt = 0
        try:
            while not ((stop_event is not None and stop_event.is_set()) or (deadline is not None and self.monotonic() >= deadline)):
                try:
                    async with self.socket_context_factory(self.endpoint) as socket:
                        if not await self._subscribe(socket, deadline, stop_event):
                            break
                        await self._consume(socket, builders, deadline, stop_event)
                        if stop_event is not None and stop_event.is_set():
                            break
                        if deadline is not None and self.monotonic() >= deadline:
                            break
                        raise ConnectionError("WebSocket ended before deadline")
                except asyncio.CancelledError:
                    raise
                except (asyncio.TimeoutError, ConnectionError, OSError) as exc:
                    if reconnect_attempt >= max_reconnects:
                        break
                    reconnect_attempt += 1
                    self._reconnect_count += 1
                    if not await self._sleep_until(self._backoff(reconnect_attempt), deadline, stop_event):
                        break
                except Exception as exc:
                    if exc.__class__.__name__ not in TRANSIENT_WEBSOCKET_CLOSES:
                        raise
                    if reconnect_attempt >= max_reconnects:
                        break
                    reconnect_attempt += 1
                    self._reconnect_count += 1
                    if not await self._sleep_until(self._backoff(reconnect_attempt), deadline, stop_event):
                        break
        finally:
            # Rebuild the completed minutes that were left in a builder when
            # this bounded window or process ended.
            if hasattr(self.database, "connection"):
                watermark = self._last_tick_at
                if watermark is not None and watermark.tzinfo is None:
                    watermark = watermark.replace(tzinfo=KST)
                materialize_completed_bars(self.database, self.symbols, as_of=watermark)
        return StreamingCollectorResult(self._ticks_saved, self._bars_saved, self.health)


async def collect_streaming_prices(**kwargs: Any) -> StreamingCollectorResult:
    """Convenience wrapper around :class:`StreamingCollector`."""
    return await StreamingCollector(**kwargs).run()
