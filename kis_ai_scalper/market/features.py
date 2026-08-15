"""Read-only feature snapshots derived from completed bars."""

from __future__ import annotations

from dataclasses import asdict, dataclass

from kis_ai_scalper.indicators.moving_average import ema, ema_series, sma
from kis_ai_scalper.indicators.volume import volume_ratio
from kis_ai_scalper.indicators.vwap import vwap_bars
from .tick import MinuteBar


@dataclass(frozen=True)
class BarFeatureSnapshot:
    symbol: str
    latest_close: float
    ema5: float | None
    ema10: float | None
    ema20: float | None
    ma60: float | None
    vwap: float | None
    volume_ratio: float | None
    high_n: float | None
    low_n: float | None
    distance_from_high_pct: float | None
    ema20_slope_pct: float | None

    def as_dict(self) -> dict[str, object]:
        return asdict(self)


def build_feature_snapshot(bars: list[MinuteBar], lookback: int = 20) -> BarFeatureSnapshot | None:
    if not bars:
        return None
    if lookback <= 0:
        raise ValueError("lookback must be positive")
    closes = [bar.close for bar in bars]
    volumes = [bar.volume for bar in bars]
    recent = bars[-lookback - 1:-1] if len(bars) > 1 else bars
    if not recent:
        recent = bars
    high_n = max(bar.high for bar in recent)
    low_n = min(bar.low for bar in recent)
    distance = (closes[-1] - high_n) / high_n * 100 if high_n else None
    ema20_values = ema_series(closes, 20)
    ema20_slope_pct = None
    if len(ema20_values) >= 2 and ema20_values[-1] is not None and ema20_values[-2] is not None:
        previous = ema20_values[-2]
        if previous:
            ema20_slope_pct = (ema20_values[-1] - previous) / previous * 100
    return BarFeatureSnapshot(
        symbol=bars[-1].symbol,
        latest_close=closes[-1],
        ema5=ema(closes, 5),
        ema10=ema(closes, 10),
        ema20=ema20_values[-1],
        ma60=sma(closes, 60),
        vwap=vwap_bars(bars),
        volume_ratio=volume_ratio(volumes, min(20, max(1, len(volumes) - 1))),
        high_n=high_n,
        low_n=low_n,
        distance_from_high_pct=distance,
        ema20_slope_pct=ema20_slope_pct,
    )
