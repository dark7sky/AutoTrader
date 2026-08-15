import asyncio
import json
from contextlib import asynccontextmanager
from datetime import date

from kis_ai_scalper.market.streaming_collector import StreamingCollector


class FakeDatabase:
    def __init__(self):
        self.ticks = []
        self.bars = []
        self.initialized = False

    def init_schema(self):
        self.initialized = True

    def save_tick(self, tick):
        self.ticks.append(tick)

    def save_bar(self, bar):
        self.bars.append(bar)


def raw_tick(symbol, hhmmss, price, volume):
    fields = [symbol, hhmmss, str(price), "2", "100", "0.1", str(price), str(price), str(price), str(price), str(price), str(price), str(volume), "100"]
    return "0|H0STCNT0|001|" + "^".join(fields)


class FakeSocket:
    def __init__(self, messages):
        self.messages = list(messages)
        self.sent = []

    async def send(self, message):
        self.sent.append(message)

    async def recv(self):
        if not self.messages:
            raise ConnectionError("closed")
        message = self.messages.pop(0)
        if isinstance(message, BaseException):
            raise message
        return message


class SocketFactory:
    def __init__(self, sockets):
        self.sockets = list(sockets)
        self.used = []

    def __call__(self, endpoint):
        socket = self.sockets.pop(0)
        self.used.append(socket)

        @asynccontextmanager
        async def context():
            yield socket
        return context()


def ack():
    return '{"header":{"tr_id":"H0STCNT0","msg":"SUBSCRIBE SUCCESS"}}'


def ping():
    return '{"header":{"tr_id":"PINGPONG"}}'


def collector(factory, db, **kwargs):
    return StreamingCollector(
        endpoint="ws://fake",
        approval_key="approval",
        symbols=["005930", "000660", "005930"],
        database=db,
        trading_date=date(2026, 8, 18),
        socket_context_factory=factory,
        sleeper=kwargs.pop("sleeper", _no_sleep),
        subscription_throttle=kwargs.pop("subscription_throttle", 0),
        reconnect_base=kwargs.pop("reconnect_base", 0),
        reconnect_max=kwargs.pop("reconnect_max", 0.5),
        **kwargs,
    )


async def _no_sleep(_seconds):
    return None


def test_multi_symbol_subscribes_once_per_unique_symbol_and_saves_ticks():
    first = FakeSocket([ack(), raw_tick("005930", "090001", 100, 2), raw_tick("000660", "090002", 200, 3), ConnectionError()])
    db = FakeDatabase()
    result = asyncio.run(collector(SocketFactory([first]), db).run(max_reconnects=0))
    subscriptions = [json.loads(item)["body"]["input"]["tr_key"] for item in first.sent]
    assert subscriptions == ["005930", "000660"]
    assert result.ticks_saved == 2
    assert [tick.symbol for tick in db.ticks] == ["005930", "000660"]


def test_completed_minute_bars_are_saved_per_symbol():
    first = FakeSocket([ack(), raw_tick("005930", "090001", 100, 2), raw_tick("005930", "090100", 105, 3), raw_tick("000660", "090001", 200, 1), raw_tick("000660", "090100", 190, 2), ConnectionError()])
    db = FakeDatabase()
    result = asyncio.run(collector(SocketFactory([first]), db).run(max_reconnects=0))
    assert result.bars_saved == 2
    assert [(bar.symbol, bar.open, bar.close, bar.volume) for bar in db.bars] == [("005930", 100, 100, 2), ("000660", 200, 200, 1)]


def test_pingpong_is_echoed_and_ack_updates_health():
    first = FakeSocket([ping(), ack(), raw_tick("005930", "090001", 100, 1), ConnectionError()])
    db = FakeDatabase()
    result = asyncio.run(collector(SocketFactory([first]), db).run(max_reconnects=0))
    assert ping() in first.sent
    assert result.health.acknowledged is True
    assert result.health.last_tick_at is not None


def test_reconnect_uses_bounded_backoff_and_counts_reconnects():
    first = FakeSocket([ack(), ConnectionError()])
    second = FakeSocket([ack(), raw_tick("005930", "090002", 101, 1), ConnectionError()])
    waits = []

    async def sleeper(seconds):
        waits.append(seconds)

    db = FakeDatabase()
    result = asyncio.run(collector(SocketFactory([first, second]), db, sleeper=sleeper, random_source=lambda: 1.0, reconnect_jitter=0.1).run(max_reconnects=1))
    assert result.health.reconnect_count == 1
    assert waits == [0.1]
    assert result.ticks_saved == 1


def test_stop_event_prevents_connect_and_stops_after_current_message():
    stop = asyncio.Event()

    class StoppingSocket(FakeSocket):
        def __init__(self, messages):
            super().__init__(messages)
            self.receives = 0

        async def recv(self):
            value = await super().recv()
            self.receives += 1
            if self.receives == 2:
                stop.set()
            return value

    socket = StoppingSocket([ack(), raw_tick("005930", "090001", 100, 1), raw_tick("000660", "090001", 200, 1)])
    db = FakeDatabase()
    result = asyncio.run(collector(SocketFactory([socket]), db).run(stop_event=stop, max_reconnects=0))
    assert result.ticks_saved == 1
    assert len(socket.sent) == 2


def test_deadline_stops_without_reconnect_when_clock_expires():
    class Clock:
        def __init__(self):
            self.now = 0

        def __call__(self):
            self.now += 1
            return self.now

    first = FakeSocket([ack(), ConnectionError()])
    db = FakeDatabase()
    clock = Clock()
    result = asyncio.run(StreamingCollector(
        endpoint="ws://fake", approval_key="approval", symbols=["005930"], database=db,
        socket_context_factory=SocketFactory([first]), sleeper=_no_sleep, monotonic=clock,
        reconnect_base=0, reconnect_max=0,
    ).run(deadline=2, max_reconnects=2))
    assert result.health.reconnect_count == 0


def test_open_position_symbols_can_be_passed_with_watchlist_and_are_deduplicated():
    first = FakeSocket([ConnectionError()])
    db = FakeDatabase()
    c = StreamingCollector(endpoint="ws://fake", approval_key="approval", symbols=["005930", "000660", "005930"], database=db, socket_context_factory=SocketFactory([first]))
    assert c.symbols == ("005930", "000660")
