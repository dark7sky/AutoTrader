"""Small, deterministic moving-average helpers."""

from __future__ import annotations

from collections.abc import Sequence


def _validate(values: Sequence[float], period: int) -> None:
    if period <= 0:
        raise ValueError("period must be positive")
    if any(value != value for value in values):
        raise ValueError("values must not contain NaN")


def sma(values: Sequence[float], period: int) -> float | None:
    """Return the latest simple moving average, or None if history is short."""
    _validate(values, period)
    if len(values) < period:
        return None
    return sum(values[-period:]) / period


def sma_series(values: Sequence[float], period: int) -> list[float | None]:
    _validate(values, period)
    return [sma(values[:index + 1], period) for index in range(len(values))]


def ema_series(values: Sequence[float], period: int) -> list[float | None]:
    """Return an EMA series seeded by the first complete SMA window."""
    _validate(values, period)
    result: list[float | None] = [None] * len(values)
    if len(values) < period:
        return result
    current = sum(values[:period]) / period
    result[period - 1] = current
    multiplier = 2 / (period + 1)
    for index in range(period, len(values)):
        current = (values[index] - current) * multiplier + current
        result[index] = current
    return result


def ema(values: Sequence[float], period: int) -> float | None:
    """Return the latest exponential moving average, or None if short."""
    series = ema_series(values, period)
    return series[-1] if series else None
