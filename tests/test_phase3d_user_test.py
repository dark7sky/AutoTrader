import asyncio
from datetime import datetime

from kis_ai_scalper.market.collector import CollectorResult
from kis_ai_scalper.market.clock import kst_now
from kis_ai_scalper.market.tick import MarketTick, MinuteBar
from kis_ai_scalper.pipeline import run_user_test
from kis_ai_scalper.storage import connect_database


def test_user_test_injected_collector_uses_same_db_and_ack(tmp_path):
    db_path = tmp_path / "user-test.sqlite3"
    calls = []

    async def fake_collector(endpoint, approval_key, symbol, path, seconds):
        calls.append((endpoint, approval_key, symbol, path, seconds))
        with connect_database(path) as database:
            database.init_schema()
            database.save_tick(MarketTick(symbol, kst_now(), 121_000, 1))
            database.save_bar(MinuteBar(symbol, kst_now(), 120_000, 122_000, 119_000, 121_000, 10))
        return CollectorResult(symbol, 1, 0, True, 121_000)

    report = asyncio.run(run_user_test(
        "ws://fake", "approval", "005930", str(db_path), seconds=2,
        collector=fake_collector,
    ))

    assert calls == [("ws://fake", "approval", "005930", str(db_path), 2)]
    assert report.collector.subscribe_ack is True
    assert report.shadow.health_status == "OK"
    assert report.shadow.trading_blocked is False
    assert report.shadow.bars_count == 1
    assert report.exit_code == 0


def test_user_test_returns_blocked_for_no_data_or_missing_ack(tmp_path):
    db_path = tmp_path / "blocked.sqlite3"

    async def fake_collector(endpoint, approval_key, symbol, path, seconds):
        return CollectorResult(symbol, 0, 0, False, None)

    report = asyncio.run(run_user_test(
        "ws://fake", "approval", "005930", str(db_path),
        collector=fake_collector,
    ))

    assert report.no_data is True
    assert report.shadow.health_status == "DISCONNECTED"
    assert report.shadow.trading_blocked is True
    assert report.exit_code == 3


def test_user_test_blocks_when_only_tick_arrived_without_completed_bar(tmp_path):
    db_path = tmp_path / "tick-only.sqlite3"

    async def fake_collector(endpoint, approval_key, symbol, path, seconds):
        with connect_database(path) as database:
            database.init_schema()
            database.save_tick(MarketTick(symbol, kst_now(), 121_000, 1))
        return CollectorResult(symbol, 1, 0, True, 121_000)

    report = asyncio.run(run_user_test(
        "ws://fake", "approval", "005930", str(db_path),
        collector=fake_collector,
    ))

    assert report.shadow.health_status == "OK"
    assert report.shadow.bars_count == 0
    assert report.exit_code == 3
