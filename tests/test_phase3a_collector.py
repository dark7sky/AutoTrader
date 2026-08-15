import asyncio
from datetime import date

import pytest

from kis_ai_scalper.broker.safety import contains_forbidden_kis_endpoint
from kis_ai_scalper.market.collector import collect_realtime_prices
from kis_ai_scalper.storage import connect_database


class FakeSocket:
    def __init__(self):
        self.sent = []
        self.messages = [
            '{"header":{"tr_id":"H0STCNT0","msg":"SUBSCRIBE SUCCESS"}}',
            "0|H0STCNT0|001|005930^090000^100^2^0^0^0^0^0^0^0^0^2^10",
            "0|H0STCNT0|001|005930^090030^101^2^0^0^0^0^0^0^0^0^3^13",
            "0|H0STCNT0|001|005930^090100^99^2^0^0^0^0^0^0^0^0^4^17",
        ]

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_args):
        return None

    async def send(self, message):
        self.sent.append(message)

    async def recv(self):
        if self.messages:
            return self.messages.pop(0)
        raise asyncio.TimeoutError


def test_collector_saves_ticks_and_only_completed_bars(tmp_path):
    socket = FakeSocket()
    result = asyncio.run(collect_realtime_prices(
        "ws://test", "approval", "005930", str(tmp_path / "market.sqlite3"),
        seconds=1, trading_date=date(2026, 8, 15), socket_context_factory=lambda _: socket,
    ))
    assert result.subscribe_ack is True
    assert result.ticks_saved == 3
    assert result.bars_saved == 1
    assert result.last_price == 99
    assert len(socket.sent) == 1
    with connect_database(tmp_path / "market.sqlite3") as database:
        assert len(database.load_ticks("005930")) == 3
        bars = database.load_bars("005930")
        assert len(bars) == 1
        assert (bars[0].open, bars[0].high, bars[0].low, bars[0].close, bars[0].volume) == (100, 101, 100, 101, 5)


def test_collector_seconds_are_bounded():
    with pytest.raises(ValueError, match="between 1 and 3600"):
        asyncio.run(collect_realtime_prices("ws://test", "approval", "005930", ":memory:", seconds=3601))


def test_collector_source_has_no_forbidden_endpoint():
    from pathlib import Path
    source = (Path(__file__).parents[1] / "kis_ai_scalper" / "market" / "collector.py").read_text(encoding="utf-8")
    assert not contains_forbidden_kis_endpoint(source)
