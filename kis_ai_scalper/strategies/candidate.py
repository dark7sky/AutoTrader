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


def scan_candidates(snapshot: BarFeatureSnapshot) -> list[CandidateSignal]:
    """Return zero or more deterministic candidates for a completed snapshot."""
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
        and -1.5 <= snapshot.distance_from_high_pct <= -0.3
        and snapshot.volume_ratio is not None
        and snapshot.volume_ratio >= 1.0
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
        and -0.3 < snapshot.distance_from_high_pct <= 0
        and (long_trend_confirmed or short_trend_confirmed)
        and snapshot.vwap is not None
        and snapshot.latest_close > snapshot.vwap
        and snapshot.volume_ratio is not None
        and snapshot.volume_ratio >= 0.6
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
        and snapshot.volume_ratio >= 1.2
    ):
        candidates.append(CandidateSignal(
            snapshot.symbol,
            "BREAKOUT_WATCH",
            0.8,
            "close at recent high, above VWAP, with elevated volume",
            common,
        ))
    return candidates
