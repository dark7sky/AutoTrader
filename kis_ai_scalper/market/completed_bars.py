"""Materialize completed one-minute bars from persisted market ticks."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Iterable

from kis_ai_scalper.market.clock import KST
from kis_ai_scalper.market.tick import MinuteBar


def _aware_kst(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        # Older collectors persisted naive KST timestamps. Keep them readable
        # while ensuring every materialized bar has an explicit timezone.
        return value.replace(tzinfo=KST)
    return value.astimezone(KST)


def _minute_start(value: datetime) -> datetime:
    return _aware_kst(value).replace(second=0, microsecond=0)


def _parse_timestamp(value: str) -> datetime:
    return _aware_kst(datetime.fromisoformat(value))


def _validate_symbols(symbols: Iterable[str] | None) -> tuple[str, ...] | None:
    if symbols is None:
        return None
    result: list[str] = []
    seen: set[str] = set()
    for symbol in symbols:
        if not isinstance(symbol, str) or not symbol.isdigit() or len(symbol) != 6:
            raise ValueError("domestic stock symbols must be six-digit codes")
        if symbol not in seen:
            result.append(symbol)
            seen.add(symbol)
    if not result:
        raise ValueError("symbols must not be empty")
    return tuple(result)


def _query_parameters(
    symbols: tuple[str, ...] | None,
    *,
    range_start: datetime,
    range_end: datetime,
) -> tuple[str, tuple[Any, ...]]:
    # ISO strings with different offsets do not sort chronologically in SQLite.
    # Pad by a day on both sides, then apply the actual aware-time filter in
    # Python. The indexed date range still bounds the rows read from SQLite.
    sql_start = (range_start.date() - timedelta(days=1)).isoformat()
    sql_end = (range_end.date() + timedelta(days=2)).isoformat()
    clauses = ["timestamp >= ?", "timestamp < ?"]
    parameters: list[Any] = [sql_start, sql_end]
    if symbols:
        placeholders = ",".join("?" for _ in symbols)
        clauses.append(f"symbol IN ({placeholders})")
        parameters.extend(symbols)
    return " WHERE " + " AND ".join(clauses), tuple(parameters)


@dataclass
class _MinuteAggregate:
    symbol: str
    start: datetime
    open: float
    high: float
    low: float
    close: float
    volume: int
    first_order: tuple[datetime, int]
    last_order: tuple[datetime, int]


def _range_parameters(
    symbols: tuple[str, ...] | None,
    *,
    range_start: datetime,
    range_end: datetime,
) -> tuple[str, tuple[Any, ...]]:
    """Keep the old helper name private while making its range explicit."""
    return _query_parameters(symbols, range_start=range_start, range_end=range_end)


def materialize_completed_bars(
    database: Any,
    symbols: Iterable[str] | None = None,
    *,
    as_of: datetime | None = None,
    batch_size: int = 1000,
    lookback_minutes: int = 360,
) -> list[MinuteBar]:
    """Upsert every completed OHLCV minute represented by ``market_ticks``.

    The query is restricted to a date-padded lookback range and streamed in
    bounded batches. Exact duplicate ticks are removed in SQL; timezone-aware
    ordering and the precise range filter happen in Python because SQLite text
    ordering is not chronological across different ISO offsets.
    """
    selected = _validate_symbols(symbols)
    if as_of is not None and (as_of.tzinfo is None or as_of.utcoffset() is None):
        raise ValueError("as_of must be timezone-aware")
    if isinstance(batch_size, bool) or not isinstance(batch_size, int) or batch_size <= 0:
        raise ValueError("batch_size must be a positive integer")
    if isinstance(lookback_minutes, bool) or not isinstance(lookback_minutes, int) or lookback_minutes <= 0:
        raise ValueError("lookback_minutes must be a positive integer")

    database.init_schema()
    watermark = _aware_kst(as_of) if as_of is not None else datetime.now(KST)
    range_start = watermark - timedelta(minutes=lookback_minutes)
    where, parameters = _range_parameters(
        selected, range_start=range_start, range_end=watermark
    )
    query = (
        "SELECT symbol,timestamp,price,volume,MIN(rowid) AS first_rowid "
        f"FROM market_ticks{where} "
        "GROUP BY symbol,timestamp,price,volume "
        "ORDER BY symbol"
    )
    cursor = database.connection.execute(query, parameters)
    aggregates: dict[tuple[str, datetime], _MinuteAggregate] = {}

    while True:
        rows = cursor.fetchmany(batch_size)
        if not rows:
            break
        for row in rows:
            symbol = str(row["symbol"])
            timestamp = _parse_timestamp(str(row["timestamp"]))
            if timestamp < range_start or timestamp > watermark:
                continue
            start = _minute_start(timestamp)
            if start + timedelta(minutes=1) > watermark:
                continue
            price = float(row["price"])
            volume = int(row["volume"])
            order = (timestamp.astimezone(timezone.utc), int(row["first_rowid"]))
            key = (symbol, start)
            aggregate = aggregates.get(key)
            if aggregate is None:
                aggregates[key] = _MinuteAggregate(
                    symbol, start, price, price, price, price, volume, order, order
                )
            else:
                aggregate.high = max(aggregate.high, price)
                aggregate.low = min(aggregate.low, price)
                aggregate.volume += volume
                if order < aggregate.first_order:
                    aggregate.open = price
                    aggregate.first_order = order
                if order > aggregate.last_order:
                    aggregate.close = price
                    aggregate.last_order = order
    bars = [
        MinuteBar(item.symbol, item.start, item.open, item.high, item.low, item.close, item.volume)
        for item in sorted(aggregates.values(), key=lambda value: (value.symbol, value.start))
    ]
    for bar in bars:
        database.save_bar(bar)
    return bars


__all__ = ["materialize_completed_bars"]
