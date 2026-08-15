import asyncio
from contextlib import asynccontextmanager
from datetime import date, datetime, timedelta, timezone

from kis_ai_scalper.market.completed_bars import materialize_completed_bars
from kis_ai_scalper.market.streaming_collector import StreamingCollector
from kis_ai_scalper.market.tick import MarketTick
from kis_ai_scalper.storage import connect_database


KST = timezone(timedelta(hours=9))


def tick(symbol, timestamp, price, volume):
    return MarketTick(symbol, timestamp, price, volume)


def add_ticks(database, values):
    for value in values:
        database.save_tick(value)


def test_materializes_only_completed_minutes_and_is_idempotent(tmp_path):
    path = tmp_path / "bars.sqlite3"
    with connect_database(path) as database:
        database.init_schema()
        add_ticks(database, [
            tick("005930", datetime(2026, 8, 18, 9, 0, 1, tzinfo=KST), 100, 2),
            tick("005930", datetime(2026, 8, 18, 9, 0, 1, tzinfo=KST), 100, 2),
            tick("005930", datetime(2026, 8, 18, 9, 0, 15, tzinfo=KST), 101, 7),
            tick("005930", datetime(2026, 8, 18, 9, 0, 30, tzinfo=KST), 105, 3),
            tick("005930", datetime(2026, 8, 18, 9, 1, 5, tzinfo=KST), 103, 4),
        ])
        first = materialize_completed_bars(database, as_of=datetime(2026, 8, 18, 9, 1, 30, tzinfo=KST))
        second = materialize_completed_bars(database, as_of=datetime(2026, 8, 18, 9, 1, 30, tzinfo=KST))
        bars = database.load_bars("005930")

    assert len(first) == 1
    assert len(second) == 1
    assert len(bars) == 1
    assert bars[0].start.tzinfo is None
    assert (bars[0].open, bars[0].high, bars[0].low, bars[0].close, bars[0].volume) == (100, 105, 100, 105, 12)


def test_restart_watermark_materializes_previous_window_for_multiple_symbols(tmp_path):
    path = tmp_path / "restart.sqlite3"
    with connect_database(path) as database:
        database.init_schema()
        add_ticks(database, [
            tick("005930", datetime(2026, 8, 18, 9, 0, 1, tzinfo=KST), 100, 1),
            tick("000660", datetime(2026, 8, 18, 9, 0, 2, tzinfo=KST), 200, 2),
        ])
        assert materialize_completed_bars(
            database, as_of=datetime(2026, 8, 18, 9, 0, 30, tzinfo=KST)
        ) == []
        add_ticks(database, [
            tick("005930", datetime(2026, 8, 18, 9, 1, 1, tzinfo=KST), 101, 3),
            tick("000660", datetime(2026, 8, 18, 9, 1, 2, tzinfo=KST), 198, 4),
        ])
        materialize_completed_bars(
            database, batch_size=1, as_of=datetime(2026, 8, 18, 9, 1, 30, tzinfo=KST)
        )
        assert [(bar.symbol, bar.start.minute, bar.volume) for bar in database.load_bars("005930")] == [("005930", 0, 1)]
        assert [(bar.symbol, bar.start.minute, bar.volume) for bar in database.load_bars("000660")] == [("000660", 0, 2)]


def test_naive_legacy_tick_is_interpreted_as_kst_and_as_of_must_be_aware(tmp_path):
    path = tmp_path / "timezone.sqlite3"
    with connect_database(path) as database:
        database.init_schema()
        add_ticks(database, [
            tick("005930", datetime(2026, 8, 18, 9, 0, 1), 100, 1),
            tick("005930", datetime(2026, 8, 18, 9, 1, 1), 101, 1),
        ])
        materialize_completed_bars(
            database, as_of=datetime(2026, 8, 18, 9, 2, 0, tzinfo=KST)
        )
        assert database.load_bars("005930")[0].start.tzinfo is None

        try:
            materialize_completed_bars(database, as_of=datetime(2026, 8, 18, 9, 2))
        except ValueError as exc:
            assert "timezone-aware" in str(exc)
        else:
            raise AssertionError("naive as_of should be rejected")


class Socket:
    def __init__(self, messages):
        self.messages = list(messages)

    async def send(self, _message):
        return None

    async def recv(self):
        if self.messages:
            message = self.messages.pop(0)
            if isinstance(message, BaseException):
                raise message
            return message
        raise ConnectionError("closed")


def raw_tick(symbol, hhmmss, price, volume):
    fields = [symbol, hhmmss, str(price), "2", "100", "0.1", str(price), str(price), str(price), str(price), str(price), str(price), str(volume), "100"]
    return "0|H0STCNT0|001|" + "^".join(fields)


def socket_factory(socket):
    @asynccontextmanager
    async def context(_endpoint):
        yield socket
    return context


async def no_sleep(_seconds):
    return None


def test_bounded_collector_materializes_ticks_across_restart(tmp_path):
    path = tmp_path / "collector.sqlite3"
    first = Socket([
        '{"header":{"tr_id":"H0STCNT0","msg":"SUBSCRIBE SUCCESS"}}',
        raw_tick("005930", "090001", 100, 2),
        raw_tick("005930", "090030", 105, 3),
        ConnectionError(),
    ])
    asyncio.run(StreamingCollector(
        endpoint="ws://fake", approval_key="approval", symbols=["005930"],
        database=connect_database(path), trading_date=date(2026, 8, 18),
        socket_context_factory=socket_factory(first), sleeper=no_sleep,
        subscription_throttle=0, reconnect_base=0, reconnect_max=0,
    ).run(max_reconnects=0))

    second = Socket([
        '{"header":{"tr_id":"H0STCNT0","msg":"SUBSCRIBE SUCCESS"}}',
        raw_tick("005930", "090100", 103, 4),
        ConnectionError(),
    ])
    asyncio.run(StreamingCollector(
        endpoint="ws://fake", approval_key="approval", symbols=["005930"],
        database=connect_database(path), trading_date=date(2026, 8, 18),
        socket_context_factory=socket_factory(second), sleeper=no_sleep,
        subscription_throttle=0, reconnect_base=0, reconnect_max=0,
    ).run(max_reconnects=0))

    with connect_database(path) as database:
        bars = database.load_bars("005930")
    assert len(bars) == 1
    assert (bars[0].open, bars[0].high, bars[0].low, bars[0].close, bars[0].volume) == (100, 105, 100, 105, 5)


def test_range_is_bounded_and_offset_timestamps_are_chronological(tmp_path):
    path = tmp_path / "range.sqlite3"
    with connect_database(path) as database:
        database.init_schema()
        add_ticks(database, [
            tick("005930", datetime(2026, 8, 17, 9, 0, 1, tzinfo=KST), 1, 99),
            # Insert the later instant first so SQLite text ordering cannot
            # accidentally define the OHLC open/close values.
            tick("005930", datetime(2026, 8, 18, 9, 0, 20, tzinfo=KST), 105, 3),
            tick("005930", datetime(2026, 8, 18, 0, 0, 10, tzinfo=timezone.utc), 100, 2),
            tick("005930", datetime(2026, 8, 18, 0, 1, 5, tzinfo=timezone.utc), 103, 4),
        ])
        statements = []
        database.connection.set_trace_callback(statements.append)
        materialize_completed_bars(
            database,
            as_of=datetime(2026, 8, 18, 9, 1, 30, tzinfo=KST),
            lookback_minutes=3,
            batch_size=1,
        )
        bars = database.load_bars("005930")

    assert any("timestamp >=" in statement and "timestamp <" in statement for statement in statements)
    assert len(bars) == 1
    assert (bars[0].open, bars[0].high, bars[0].low, bars[0].close, bars[0].volume) == (100, 105, 100, 105, 5)
