from datetime import datetime, timedelta

from kis_ai_scalper.cli import main
from kis_ai_scalper.market.clock import kst_now
from kis_ai_scalper.market.tick import MarketTick, MinuteBar
from kis_ai_scalper.pipeline import ShadowCycleConfig, run_shadow_cycle
from kis_ai_scalper.storage import connect_database


def make_bars(closes, volumes=None):
    volumes = volumes or [100] * len(closes)
    start = datetime(2026, 8, 15, 9, 0)
    return [
        MinuteBar("005930", start + timedelta(minutes=i), close, close + 500, close - 500, close, volume)
        for i, (close, volume) in enumerate(zip(closes, volumes))
    ]


def test_blocked_health_skips_candidates_and_risk():
    bars = make_bars([100_000 + index * 1_000 for index in range(20)] + [121_000], [100] * 20 + [150])
    report = run_shadow_cycle(
        "005930",
        bars=bars,
        current_time=datetime(2026, 8, 15, 9, 21),
        config=ShadowCycleConfig(websocket_acknowledged=False),
    )
    assert report.health_status == "DISCONNECTED"
    assert report.trading_blocked is True
    assert report.candidates_count == 0
    assert report.risk_reason == "websocket subscription is not acknowledged"
    assert report.lifecycle_final_state == "SAFE_MODE"


def test_ok_shadow_cycle_scans_candidate_and_evaluates_risk():
    bars = make_bars([100_000 + index * 1_000 for index in range(20)] + [121_000], [100] * 20 + [150])
    report = run_shadow_cycle(
        "005930",
        bars=bars,
        current_time=datetime(2026, 8, 15, 9, 21),
        config=ShadowCycleConfig(websocket_acknowledged=True),
    )
    assert report.health_status == "OK"
    assert report.candidates_count == 1
    assert report.selected_strategy == "BREAKOUT_WATCH"
    assert report.risk_approved is True
    assert report.risk_quantity == 2
    assert report.lifecycle_final_state == "LONG"


def test_shadow_cycle_cli_prints_no_side_effects(tmp_path, capsys):
    db_path = tmp_path / "shadow.sqlite3"
    now = kst_now()
    with connect_database(db_path) as database:
        database.init_schema()
        database.save_tick(MarketTick("005930", now, 121_000, 1))
        for index, bar in enumerate(make_bars([100_000 + index * 1_000 for index in range(20)] + [121_000], [100] * 20 + [150])):
            database.save_bar(MinuteBar(bar.symbol, now - timedelta(minutes=21 - index), bar.open, bar.high, bar.low, bar.close, bar.volume))

    assert main(["shadow-cycle", "--symbol", "005930", "--db", str(db_path), "--websocket-acknowledged"]) == 0
    output = capsys.readouterr().out
    assert "shadow cycle: OK" in output
    assert "orders=none account_queries=none ai_calls=none" in output


def test_shadow_cycle_cli_returns_blocked_code_for_stale_data(tmp_path, capsys):
    db_path = tmp_path / "shadow.sqlite3"
    old = kst_now() - timedelta(minutes=5)
    with connect_database(db_path) as database:
        database.init_schema()
        database.save_tick(MarketTick("005930", old, 121_000, 1))

    assert main(["shadow-cycle", "--symbol", "005930", "--db", str(db_path), "--websocket-acknowledged"]) == 3
    output = capsys.readouterr().out
    assert "trading_blocked=true" in output
    assert "lifecycle_final_state=SAFE_MODE" in output
