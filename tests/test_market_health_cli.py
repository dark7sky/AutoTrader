from datetime import datetime, timedelta

from kis_ai_scalper.cli import market_health_check
from kis_ai_scalper.market.clock import kst_now
from kis_ai_scalper.market.tick import MarketTick
from kis_ai_scalper.storage import connect_database


def test_market_health_cli_blocks_when_websocket_ack_is_missing(tmp_path, capsys):
    db_path = tmp_path / "health.sqlite3"
    with connect_database(db_path) as database:
        database.init_schema()
        database.save_tick(MarketTick("005930", kst_now(), 100.0, 1))

    assert market_health_check("005930", str(db_path), False, 5.0, 90.0) == 3
    output = capsys.readouterr().out
    assert "status=DISCONNECTED" in output
    assert "trading_blocked=true" in output


def test_market_health_cli_allows_fresh_acknowledged_data(tmp_path, capsys):
    db_path = tmp_path / "health.sqlite3"
    with connect_database(db_path) as database:
        database.init_schema()
        database.save_tick(MarketTick("005930", kst_now() - timedelta(seconds=1), 100.0, 1))

    assert market_health_check("005930", str(db_path), True, 5.0, 90.0) == 0
    output = capsys.readouterr().out
    assert "status=OK" in output
    assert "safe_mode=false" in output
