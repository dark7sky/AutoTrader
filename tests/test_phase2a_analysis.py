from datetime import datetime, timedelta

from kis_ai_scalper.indicators.moving_average import ema, sma
from kis_ai_scalper.indicators.volume import volume_average, volume_ratio
from kis_ai_scalper.indicators.vwap import vwap
from kis_ai_scalper.market.features import build_feature_snapshot
from kis_ai_scalper.market.tick import MinuteBar
from kis_ai_scalper.strategies.candidate import scan_candidates


def make_bars(closes, volumes=None):
    volumes = volumes or [100] * len(closes)
    start = datetime(2026, 8, 15, 9, 0)
    return [MinuteBar("005930", start + timedelta(minutes=i), close, close + 1, close - 1, close, volume)
            for i, (close, volume) in enumerate(zip(closes, volumes))]


def test_moving_averages_are_deterministic():
    assert sma([1, 2, 3, 4], 3) == 3
    assert ema([1, 2, 3, 4], 3) == 3.0
    assert sma([1, 2], 3) is None
    assert ema([1, 2], 3) is None


def test_vwap_and_volume_ratio():
    assert vwap([10, 20], [1, 3]) == 17.5
    assert volume_average([10, 20, 30], 2) == 25
    assert volume_ratio([10, 10, 20], 2) == 2


def test_feature_snapshot_allows_short_history():
    snapshot = build_feature_snapshot(make_bars([100, 101, 102], [100, 100, 150]))
    assert snapshot is not None
    assert snapshot.symbol == "005930"
    assert snapshot.latest_close == 102
    assert snapshot.ema5 is None
    assert snapshot.ma60 is None
    assert snapshot.volume_ratio == 1.5
    assert snapshot.high_n == 102


def test_pullback_and_breakout_candidates():
    rising = list(range(100, 120))
    pullback = rising + [119]
    pullback_snapshot = build_feature_snapshot(make_bars(pullback, [100] * 20 + [120]))
    assert pullback_snapshot is not None
    pullback_candidates = scan_candidates(pullback_snapshot)
    assert any(
        candidate.strategy == "PULLBACK_WATCH" and candidate.score >= 0.75
        for candidate in pullback_candidates
    )

    breakout = rising + [121]
    breakout_snapshot = build_feature_snapshot(make_bars(breakout, [100] * 20 + [150]))
    assert breakout_snapshot is not None
    assert any(candidate.strategy == "BREAKOUT_WATCH" for candidate in scan_candidates(breakout_snapshot))


def test_near_breakout_momentum_candidate_is_actionable():
    closes = [100 + index for index in range(20)] + [119.8]
    snapshot = build_feature_snapshot(make_bars(closes, [100] * 20 + [130]))
    assert snapshot is not None

    candidates = scan_candidates(snapshot)

    assert any(
        candidate.strategy == "MOMENTUM_CONTINUATION" and candidate.score >= 0.75
        for candidate in candidates
    )


def test_short_history_near_breakout_can_be_actionable_after_restart():
    start = datetime(2026, 8, 15, 9, 0)
    closes = [248_000, 253_000, 250_000, 253_500, 253_500, 253_500, 253_500, 253_500, 252_750]
    bars = [
        MinuteBar(
            "005930",
            start + timedelta(minutes=index),
            close,
            253_500 if index < 8 else 253_000,
            close - 500,
            close,
            100 if index < 8 else 60,
        )
        for index, close in enumerate(closes)
    ]
    snapshot = build_feature_snapshot(bars)
    assert snapshot is not None
    assert snapshot.ema20 is None
    assert snapshot.volume_ratio == 0.6
    assert snapshot.ema5 is not None
    assert snapshot.latest_close < snapshot.ema5
    assert snapshot.latest_close / snapshot.ema5 > 0.999

    candidates = scan_candidates(snapshot)

    assert any(
        candidate.strategy == "MOMENTUM_CONTINUATION" and candidate.score >= 0.75
        for candidate in candidates
    )


def test_no_signal_case():
    snapshot = build_feature_snapshot(make_bars([100] * 25, [100] * 25))
    assert snapshot is not None
    assert scan_candidates(snapshot) == []
