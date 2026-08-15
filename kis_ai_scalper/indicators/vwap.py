"""Volume-weighted average price helpers."""

from __future__ import annotations

from collections.abc import Sequence


def vwap(prices: Sequence[float], volumes: Sequence[float]) -> float | None:
    if len(prices) != len(volumes):
        raise ValueError("prices and volumes must have equal length")
    if not prices:
        return None
    total_volume = sum(volumes)
    if total_volume <= 0:
        return None
    return sum(price * volume for price, volume in zip(prices, volumes)) / total_volume


def vwap_bars(bars: Sequence[object]) -> float | None:
    """Use OHLC4 as the bar's representative price."""
    prices = [(bar.open + bar.high + bar.low + bar.close) / 4 for bar in bars]
    return vwap(prices, [bar.volume for bar in bars])
