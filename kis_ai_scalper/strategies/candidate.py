"""Deterministic watch-list candidates; these are not trade instructions."""

from __future__ import annotations

from dataclasses import dataclass

from kis_ai_scalper.market.features import BarFeatureSnapshot


@dataclass(frozen=True)
class CandidateSignal:
    symbol: str
    strategy: str
    score: float
    reason: str
    features: dict[str, object]


@dataclass(frozen=True)
class CandidateThresholds:
    pullback_distance_min: float
    pullback_distance_max: float
    pullback_volume_ratio: float
    momentum_distance_min: float
    momentum_volume_ratio: float
    breakout_volume_ratio: float


CANDIDATE_PROFILES = {
    "conservative": CandidateThresholds(-1.0, -0.4, 1.3, -0.2, 0.9, 1.5),
    "normal": CandidateThresholds(-1.5, -0.3, 1.0, -0.3, 0.6, 1.2),
    "aggressive": CandidateThresholds(-3.0, -0.1, 0.5, -0.9, 0.35, 0.7),
}


def scan_candidates(
    snapshot: BarFeatureSnapshot,
    *,
    profile: str = "normal",
) -> list[CandidateSignal]:
    """Return zero or more deterministic candidates for a completed snapshot."""
    try:
        thresholds = CANDIDATE_PROFILES[profile]
    except KeyError as exc:
        raise ValueError(f"unknown candidate profile: {profile}") from exc
    candidates: list[CandidateSignal] = []
    common = snapshot.as_dict()
    long_trend_confirmed = (
        snapshot.ema20 is not None
        and snapshot.ema20_slope_pct is not None
        and snapshot.ema20_slope_pct > 0
        and snapshot.ema5 is not None
        and snapshot.latest_close > snapshot.ema5 > snapshot.ema20
    )
    short_trend_confirmed = (
        snapshot.ema20 is None
        and snapshot.ema5 is not None
        and snapshot.latest_close >= snapshot.ema5 * 0.999
    )
    if (
        snapshot.ema20 is not None
        and snapshot.ema20_slope_pct is not None
        and snapshot.ema20_slope_pct > 0
        and snapshot.vwap is not None
        and snapshot.latest_close > snapshot.vwap
        and snapshot.distance_from_high_pct is not None
        and thresholds.pullback_distance_min
        <= snapshot.distance_from_high_pct
        <= thresholds.pullback_distance_max
        and snapshot.volume_ratio is not None
        and snapshot.volume_ratio >= thresholds.pullback_volume_ratio
    ):
        candidates.append(CandidateSignal(
            snapshot.symbol,
            "PULLBACK_WATCH",
            0.76,
            "close above VWAP, rising EMA20, controlled pullback from recent high",
            common,
        ))
    if (
        snapshot.high_n is not None
        and snapshot.latest_close < snapshot.high_n
        and snapshot.distance_from_high_pct is not None
        and thresholds.momentum_distance_min < snapshot.distance_from_high_pct <= 0
        and (long_trend_confirmed or short_trend_confirmed)
        and snapshot.vwap is not None
        and snapshot.latest_close > snapshot.vwap
        and snapshot.volume_ratio is not None
        and snapshot.volume_ratio >= thresholds.momentum_volume_ratio
    ):
        candidates.append(CandidateSignal(
            snapshot.symbol,
            "MOMENTUM_CONTINUATION",
            0.76,
            "close is pressing the recent high with rising EMA stack and above-normal volume",
            common,
        ))
    if (
        snapshot.high_n is not None
        and snapshot.latest_close >= snapshot.high_n
        and snapshot.vwap is not None
        and snapshot.latest_close > snapshot.vwap
        and snapshot.volume_ratio is not None
        and snapshot.volume_ratio >= thresholds.breakout_volume_ratio
    ):
        candidates.append(CandidateSignal(
            snapshot.symbol,
            "BREAKOUT_WATCH",
            0.8,
            "close at recent high, above VWAP, with elevated volume",
            common,
        ))
    return candidates
