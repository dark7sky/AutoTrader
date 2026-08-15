"""Deterministic one-minute OHLCV aggregation."""

from __future__ import annotations

from .tick import MarketTick, MinuteBar


class MinuteBarBuilder:
    def __init__(self) -> None:
        self._bar: MinuteBar | None = None

    @staticmethod
    def _start(tick: MarketTick):
        return tick.timestamp.replace(second=0, microsecond=0)

    def update(self, tick: MarketTick) -> MinuteBar | None:
        start = self._start(tick)
        if self._bar is None:
            self._bar = MinuteBar(tick.symbol, start, tick.price, tick.price, tick.price, tick.price, tick.volume)
            return None
        if tick.symbol != self._bar.symbol:
            raise ValueError("bar builder accepts one symbol")
        if start < self._bar.start:
            raise ValueError("ticks must be chronological")
        if start == self._bar.start:
            self._bar = MinuteBar(self._bar.symbol, self._bar.start, self._bar.open,
                                  max(self._bar.high, tick.price), min(self._bar.low, tick.price),
                                  tick.price, self._bar.volume + tick.volume)
            return None
        completed = self._bar
        self._bar = MinuteBar(tick.symbol, start, tick.price, tick.price, tick.price, tick.price, tick.volume)
        return completed

    def flush(self) -> MinuteBar | None:
        completed, self._bar = self._bar, None
        return completed
