"""Pure market-data freshness and connection health checks."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum

from .tick import MarketTick, MinuteBar


class MarketHealthStatus(StrEnum):
    """Overall status used by safety checks before trading."""

    OK = "OK"
    STALE = "STALE"
    DISCONNECTED = "DISCONNECTED"
    NO_DATA = "NO_DATA"


@dataclass(frozen=True)
class MarketHealth:
    """Result of a market-data health evaluation."""

    status: MarketHealthStatus
    trading_blocked: bool
    enter_safe_mode: bool
    reason: str
    tick_age_seconds: float | None = None
    bar_age_seconds: float | None = None


def _age_seconds(current_time: datetime, timestamp: datetime) -> float:
    """Return a non-negative age, treating a future timestamp as fresh."""

    return max(0.0, (current_time - timestamp).total_seconds())


def evaluate_market_health(
    current_time: datetime,
    *,
    max_tick_age_seconds: float = 5.0,
    max_bar_age_seconds: float = 90.0,
    websocket_acknowledged: bool,
    latest_tick: MarketTick | None = None,
    latest_bar: MinuteBar | None = None,
) -> MarketHealth:
    """Evaluate whether the latest market data is safe to use.

    A missing subscription acknowledgement is disconnected even when a cached
    tick or bar exists. With an acknowledged connection, either a fresh tick
    or a fresh bar is sufficient for an ``OK`` result.
    """

    if max_tick_age_seconds <= 0 or max_bar_age_seconds <= 0:
        raise ValueError("maximum data ages must be positive")

    tick_age = _age_seconds(current_time, latest_tick.timestamp) if latest_tick else None
    bar_age = _age_seconds(current_time, latest_bar.start) if latest_bar else None

    if not websocket_acknowledged:
        return MarketHealth(
            status=MarketHealthStatus.DISCONNECTED,
            trading_blocked=True,
            enter_safe_mode=True,
            reason="websocket subscription is not acknowledged",
            tick_age_seconds=tick_age,
            bar_age_seconds=bar_age,
        )

    if latest_tick is None and latest_bar is None:
        return MarketHealth(
            status=MarketHealthStatus.NO_DATA,
            trading_blocked=True,
            enter_safe_mode=True,
            reason="no tick or bar data is available",
        )

    tick_fresh = tick_age is not None and tick_age <= max_tick_age_seconds
    bar_fresh = bar_age is not None and bar_age <= max_bar_age_seconds
    if tick_fresh or bar_fresh:
        return MarketHealth(
            status=MarketHealthStatus.OK,
            trading_blocked=False,
            enter_safe_mode=False,
            reason="market data is fresh",
            tick_age_seconds=tick_age,
            bar_age_seconds=bar_age,
        )

    return MarketHealth(
        status=MarketHealthStatus.STALE,
        trading_blocked=True,
        enter_safe_mode=True,
        reason="latest market data is stale",
        tick_age_seconds=tick_age,
        bar_age_seconds=bar_age,
    )


__all__ = ["MarketHealth", "MarketHealthStatus", "evaluate_market_health"]
