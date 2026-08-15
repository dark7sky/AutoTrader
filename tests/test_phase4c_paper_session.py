import asyncio
from datetime import timedelta

import pytest

from kis_ai_scalper import cli
from kis_ai_scalper.market.clock import kst_now
from kis_ai_scalper.market.collector import CollectorResult
from kis_ai_scalper.market.tick import MarketTick, MinuteBar
from kis_ai_scalper.pipeline import PaperSessionIteration, PaperSessionReport, run_paper_session
from kis_ai_scalper.storage import connect_database


def _seed_market(path):
    now = kst_now()
    with connect_database(path) as database:
        database.init_schema()
        database.save_tick(MarketTick("005930", now, 121_000, 1))
        for index in range(21):
            close = 100_000 + index * 1_000
            database.save_bar(MinuteBar(
                "005930", now - timedelta(minutes=20 - index), close,
                close + 500, close - 500, close, 150 if index == 20 else 100,
            ))


def test_paper_session_validates_bounds(tmp_path):
    async def collector(*args):
        return CollectorResult("005930", 0, 0, False, None)

    for kwargs in (
        {"iterations": 0}, {"iterations": 101},
        {"collect_seconds": 0}, {"collect_seconds": 3601},
        {"sleep_seconds": -1}, {"sleep_seconds": 3601},
        {"max_tick_age_seconds": 0}, {"max_bar_age_seconds": 0},
    ):
        with pytest.raises(ValueError):
            asyncio.run(run_paper_session(
                "fake", "approval", "005930", str(tmp_path / "x.db"),
                collector=collector, **kwargs,
            ))


def test_blocked_session_returns_three(tmp_path):
    async def collector(*args):
        return CollectorResult("005930", 0, 0, False, None)

    report = asyncio.run(run_paper_session(
        "fake", "approval", "005930", str(tmp_path / "blocked.db"),
        iterations=2, collector=collector,
    ))
    assert report.exit_code == 3
    assert len(report.iterations) == 2
    assert all(item.blocked and item.exit_code == 3 for item in report.iterations)


def test_session_records_once_and_does_not_add_second_same_symbol_position(tmp_path):
    path = tmp_path / "paper-session.db"
    _seed_market(path)
    calls = []
    with connect_database(path) as database:
        database.init_schema()
        database.set_runtime_paused(False, "test_active", "test")

    async def collector(endpoint, approval_key, symbol, db_path, seconds):
        calls.append(seconds)
        return CollectorResult(symbol, 1, 1, True, 121_000)

    report = asyncio.run(run_paper_session(
        "fake", "approval", "005930", str(path), iterations=2,
        collector=collector, sleep_seconds=0,
    ))
    assert report.exit_code == 0
    assert report.iterations[0].recorded is True
    assert report.iterations[1].recorded is False
    assert report.iterations[1].duplicate_skipped is False
    with connect_database(path) as database:
        assert len(database.list_paper_orders("005930")) == 1
        assert len(database.paper_positions()) == 1
    assert calls == [60, 60]


def test_paper_session_cli_prints_safety_banner_without_auth_or_network(tmp_path, monkeypatch, capsys):
    class FakeConfig:
        kis_api = type("Credentials", (), {"app_key": "key", "app_secret": "secret"})()

    class FakeAuth:
        def __init__(self, *args):
            pass

        def authenticate_read_only(self, **kwargs):
            return type("AuthResult", (), {"approval_key": "approval"})()

    async def fake_session(*args, **kwargs):
        return PaperSessionReport((PaperSessionIteration(
            1, True, 2, 1, "OK", True, "approved", True, False, False, 0,
        ),))

    monkeypatch.setattr(cli, "load_config", lambda path: FakeConfig())
    monkeypatch.setattr(cli, "KisAuthClient", FakeAuth)
    monkeypatch.setattr(cli, "run_paper_session", fake_session)
    monkeypatch.setattr(cli, "websocket_url", lambda env: "ws://fake")

    assert cli.main([
        "paper-session", "--config", str(tmp_path / "settings.yaml"),
        "--iterations", "1", "--collect-seconds", "1", "--db", str(tmp_path / "x.db"),
    ]) == 0
    output = capsys.readouterr().out
    assert "iteration=1" in output
    assert "local_paper_recorded=true" in output
    assert "broker_calls=none broker_orders=none account_queries=none ai_calls=none" in output
