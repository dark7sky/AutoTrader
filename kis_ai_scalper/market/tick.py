"""Small market-data value objects used by replay and live adapters."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True)
class MarketTick:
    symbol: str
    timestamp: datetime
    price: float
    volume: int


@dataclass(frozen=True)
class MinuteBar:
    symbol: str
    start: datetime
    open: float
    high: float
    low: float
    close: float
    volume: int
