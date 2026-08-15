"""Deterministic volume comparison helpers."""

from __future__ import annotations

from collections.abc import Sequence


def volume_average(volumes: Sequence[float], period: int) -> float | None:
    if period <= 0:
        raise ValueError("period must be positive")
    if len(volumes) < period:
        return None
    return sum(volumes[-period:]) / period


def volume_ratio(volumes: Sequence[float], period: int = 20) -> float | None:
    if not volumes:
        return None
    average = volume_average(volumes[:-1], period)
    if average is None:
        average = volume_average(volumes, period)
    if average is None or average <= 0:
        return None
    return volumes[-1] / average
