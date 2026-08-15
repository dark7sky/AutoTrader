import json
import sqlite3
from datetime import datetime, timedelta

from kis_ai_scalper.cli import analyze_bars
from kis_ai_scalper.market.tick import MinuteBar
from kis_ai_scalper.market.tick import MarketTick
from kis_ai_scalper.storage import connect_database
from kis_ai_scalper.storage.replay import parse_bars_csv
from kis_ai_scalper.strategies.candidate import CandidateSignal


def make_bar(index: int, symbol: str = "005930") -> MinuteBar:
    start = datetime(2026, 8, 15, 9, 0) + timedelta(minutes=index)
    return MinuteBar(symbol, start, 100 + index, 101 + index, 99 + index, 100 + index, 100)


def test_schema_save_and_load_bars(tmp_path):
    with connect_database(tmp_path / "analysis.sqlite3") as database:
        database.init_schema()
        database.save_bar(make_bar(1))
        database.save_bar(make_bar(1))
        assert database.load_bars("005930") == [make_bar(1)]
        tables = {row[0] for row in database.connection.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        assert {"market_ticks", "bars_1m", "candidate_signals", "system_events"} <= tables


def test_save_and_load_ticks_preserves_same_timestamp_rows(tmp_path):
    timestamp = datetime(2026, 8, 15, 9, 1, 2)
    first = MarketTick("005930", timestamp, 100.0, 2)
    second = MarketTick("005930", timestamp, 100.5, 3)
    with connect_database(tmp_path / "ticks.sqlite3") as database:
        database.init_schema()
        database.save_tick(first, datetime(2026, 8, 15, 9, 1, 3))
        database.save_tick(second, datetime(2026, 8, 15, 9, 1, 4))
        assert database.load_ticks("005930") == [first, second]
        assert database.latest_tick("005930") == second


def test_latest_bar_returns_most_recent_symbol_bar(tmp_path):
    with connect_database(tmp_path / "latest.sqlite3") as database:
        database.init_schema()
        database.save_bar(make_bar(1))
        database.save_bar(make_bar(3))
        database.save_bar(make_bar(2, symbol="000660"))
        assert database.latest_bar("005930") == make_bar(3)
        assert database.latest_bar("035420") is None


def test_candidate_features_are_stored_as_json(tmp_path):
    candidate = CandidateSignal("005930", "PULLBACK_WATCH", 0.7, "test", {"vwap": 101.2})
    with connect_database(tmp_path / "analysis.sqlite3") as database:
        database.init_schema()
        database.save_candidate(candidate, make_bar(1).start)
        row = database.connection.execute("SELECT features_json FROM candidate_signals").fetchone()
        assert json.loads(row[0]) == {"vwap": 101.2}


def test_schema_has_no_secret_or_token_columns(tmp_path):
    with connect_database(tmp_path / "analysis.sqlite3") as database:
        database.init_schema()
        for table in ("market_ticks", "bars_1m", "candidate_signals", "system_events", "paper_orders", "paper_fills"):
            columns = {row[1].lower() for row in database.connection.execute(f"PRAGMA table_info({table})")}
            assert not columns & {"token", "access_token", "secret", "app_secret", "password"}


def test_csv_parser(tmp_path):
    path = tmp_path / "bars.csv"
    path.write_text(
        "symbol,start,open,high,low,close,volume\n"
        "005930,2026-08-15T09:00:00+09:00,100,101,99,100.5,120\n",
        encoding="utf-8",
    )
    bars = parse_bars_csv(path)
    assert bars[0].symbol == "005930"
    assert bars[0].close == 100.5
    assert bars[0].volume == 120


def test_csv_parser_rejects_invalid_ohlcv(tmp_path):
    path = tmp_path / "bad_bars.csv"
    path.write_text(
        "symbol,start,open,high,low,close,volume\n"
        "005930,2026-08-15T09:00:00+09:00,100,99,98,100.5,120\n",
        encoding="utf-8",
    )
    try:
        parse_bars_csv(path)
    except ValueError as exc:
        assert "OHLC" in str(exc)
    else:
        raise AssertionError("invalid OHLC row should fail")


def test_analyze_bars_sample(tmp_path, capsys):
    assert analyze_bars(None, "005930", str(tmp_path / "sample.sqlite3")) == 0
    output = capsys.readouterr().out
    assert "rows=21" in output
    assert "candidates=1" in output
    assert "BREAKOUT_WATCH" in output
