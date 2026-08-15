from datetime import datetime, timedelta

from kis_ai_scalper.cli import main
from kis_ai_scalper.market.clock import kst_now
from kis_ai_scalper.market.tick import MarketTick, MinuteBar
from kis_ai_scalper.pipeline import ShadowCycleConfig, record_shadow_paper_buy, run_paper_shadow_cycle
from kis_ai_scalper.storage import connect_database


def make_bars():
    start = datetime(2026, 8, 15, 9, 0)
    closes = [100_000 + index * 1_000 for index in range(20)] + [121_000]
    return [
        MinuteBar(
            "005930", start + timedelta(minutes=i), close, close + 500,
            close - 500, close, 150 if i == len(closes) - 1 else 100,
        )
        for i, close in enumerate(closes)
    ]


def seed_database(path, *, stale=False):
    now = kst_now()
    with connect_database(path) as database:
        database.init_schema()
        tick_time = now - timedelta(minutes=5) if stale else now
        database.save_tick(MarketTick("005930", tick_time, 121_000, 1))
        for index, bar in enumerate(make_bars()):
            database.save_bar(
                MinuteBar(
                    bar.symbol, now - timedelta(minutes=21 - index), bar.open,
                    bar.high, bar.low, bar.close, bar.volume,
                )
            )


def test_approved_shadow_records_one_paper_buy(tmp_path):
    path = tmp_path / "paper.sqlite3"
    seed_database(path)
    with connect_database(path) as database:
        result = run_paper_shadow_cycle(
            "005930", database=database,
            config=ShadowCycleConfig(websocket_acknowledged=True),
        )
        assert result.recorded is True
        assert result.duplicate_skipped is False
        assert len(database.list_paper_orders()) == 1
        assert len(database.list_paper_fills()) == 1


def test_duplicate_signal_record_is_skipped_without_new_rows(tmp_path):
    path = tmp_path / "paper.sqlite3"
    seed_database(path)
    with connect_database(path) as database:
        config = ShadowCycleConfig(websocket_acknowledged=True)
        first = run_paper_shadow_cycle("005930", database=database, config=config)
        second = record_shadow_paper_buy(first.shadow, database)
        assert first.recorded is True
        assert second.duplicate_skipped is True
        assert len(database.list_paper_orders()) == 1
        assert len(database.list_paper_fills()) == 1


def test_existing_paper_position_blocks_new_same_symbol_entry(tmp_path):
    path = tmp_path / "paper.sqlite3"
    seed_database(path)
    with connect_database(path) as database:
        config = ShadowCycleConfig(websocket_acknowledged=True)
        first = run_paper_shadow_cycle("005930", database=database, config=config)
        assert first.recorded is True

        fresh_signal_bar = database.latest_bar("005930")
        assert fresh_signal_bar is not None
        database.save_bar(MinuteBar(
            fresh_signal_bar.symbol,
            fresh_signal_bar.start + timedelta(minutes=1),
            fresh_signal_bar.open,
            fresh_signal_bar.high + 1_000,
            fresh_signal_bar.low,
            fresh_signal_bar.close + 1_000,
            fresh_signal_bar.volume + 50,
        ))
        database.save_tick(MarketTick("005930", kst_now(), 122_000, 1))
        second = run_paper_shadow_cycle("005930", database=database, config=config)

        assert second.recorded is False
        assert second.shadow.risk_reason == "existing_position_same_symbol"
        assert len(database.list_paper_orders()) == 1


def test_blocked_shadow_creates_no_paper_record(tmp_path):
    path = tmp_path / "paper.sqlite3"
    seed_database(path, stale=True)
    with connect_database(path) as database:
        result = run_paper_shadow_cycle("005930", database=database)
        assert result.recorded is False
        assert result.duplicate_skipped is False
        assert database.list_paper_orders() == []
        assert database.list_paper_fills() == []


def test_paper_shadow_cli_reports_local_only_execution(tmp_path, capsys):
    path = tmp_path / "paper.sqlite3"
    seed_database(path)
    assert main([
        "paper-shadow-cycle", "--symbol", "005930", "--db", str(path),
        "--websocket-acknowledged",
    ]) == 0
    output = capsys.readouterr().out
    assert "paper shadow cycle: RECORDED" in output
    assert "paper_orders=1" in output
    assert "broker_calls=none orders=none account_queries=none ai_calls=none" in output
