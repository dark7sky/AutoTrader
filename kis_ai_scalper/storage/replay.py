"""Offline OHLCV CSV replay input."""

from __future__ import annotations

import csv
from datetime import datetime, timedelta
from pathlib import Path

from kis_ai_scalper.market.tick import MinuteBar


REQUIRED_COLUMNS = ("symbol", "start", "open", "high", "low", "close", "volume")


def parse_bars_csv(path: str | Path) -> list[MinuteBar]:
    with Path(path).open(newline="", encoding="utf-8-sig") as handle:
        reader = csv.DictReader(handle)
        if reader.fieldnames is None or any(column not in reader.fieldnames for column in REQUIRED_COLUMNS):
            raise ValueError(f"CSV must contain columns: {','.join(REQUIRED_COLUMNS)}")
        bars = []
        for line, row in enumerate(reader, start=2):
            try:
                start = datetime.fromisoformat(row["start"].strip().replace("Z", "+00:00"))
                symbol = row["symbol"].strip()
                open_price = float(row["open"])
                high = float(row["high"])
                low = float(row["low"])
                close = float(row["close"])
                volume = int(row["volume"])
                if not symbol.isdigit() or len(symbol) != 6:
                    raise ValueError("symbol must be a six-digit domestic stock code")
                if volume < 0:
                    raise ValueError("volume must be non-negative")
                if min(open_price, high, low, close) <= 0:
                    raise ValueError("OHLC prices must be positive")
                if high < max(open_price, close) or low > min(open_price, close) or low > high:
                    raise ValueError("OHLC prices are inconsistent")
                bars.append(MinuteBar(symbol, start, open_price, high, low, close, volume))
            except (AttributeError, TypeError, ValueError) as exc:
                raise ValueError(f"invalid bar at CSV line {line}: {exc}") from exc
    return bars


def sample_bars(symbol: str = "005930") -> list[MinuteBar]:
    start = datetime(2026, 8, 15, 9, 0)
    closes = [100_000 + index * 1_000 for index in range(20)] + [121_000]
    return [MinuteBar(symbol, start + timedelta(minutes=index), close, close + 500,
                      close - 500, close, 100 if index < 20 else 150)
            for index, close in enumerate(closes)]
