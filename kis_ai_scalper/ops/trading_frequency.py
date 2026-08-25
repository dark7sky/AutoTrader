"""Runtime trade-frequency controls shared by Telegram and auto trading."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol


PROFILE_KEY = "trade_frequency.profile"
MAX_TRADES_PER_DAY_KEY = "trade_frequency.max_trades_per_day"
AI_MIN_CONFIDENCE_KEY = "trade_frequency.ai_min_confidence"


class RuntimeMetadataStore(Protocol):
    def get_runtime_metadata(self, key: str) -> str | None:
        ...

    def set_runtime_metadata(self, key: str, value: str) -> None:
        ...


@dataclass(frozen=True)
class TradeFrequencySettings:
    profile: str
    max_trades_per_day: int
    ai_min_confidence: float


FREQUENCY_PRESETS: dict[str, TradeFrequencySettings] = {
    "conservative": TradeFrequencySettings("conservative", 1, 0.82),
    "normal": TradeFrequencySettings("normal", 3, 0.75),
    "aggressive": TradeFrequencySettings("aggressive", 5, 0.70),
}


def _parse_max_trades(raw: str | None, default: int) -> int:
    if raw is None:
        return default
    try:
        value = int(raw)
    except ValueError:
        return default
    return value if value >= 1 else default


def _parse_confidence(raw: str | None, default: float) -> float:
    if raw is None:
        return default
    try:
        value = float(raw)
    except ValueError:
        return default
    return value if 0 <= value <= 1 else default


def read_trade_frequency(
    store: RuntimeMetadataStore,
    *,
    default_max_trades_per_day: int,
    default_ai_min_confidence: float,
) -> TradeFrequencySettings:
    profile = (store.get_runtime_metadata(PROFILE_KEY) or "env").strip() or "env"
    max_trades = _parse_max_trades(
        store.get_runtime_metadata(MAX_TRADES_PER_DAY_KEY),
        default_max_trades_per_day,
    )
    confidence = _parse_confidence(
        store.get_runtime_metadata(AI_MIN_CONFIDENCE_KEY),
        default_ai_min_confidence,
    )
    return TradeFrequencySettings(profile, max_trades, confidence)


def apply_trade_frequency_preset(
    store: RuntimeMetadataStore,
    profile: str,
) -> TradeFrequencySettings:
    try:
        settings = FREQUENCY_PRESETS[profile]
    except KeyError as exc:
        raise ValueError("unknown frequency profile") from exc
    store.set_runtime_metadata(PROFILE_KEY, settings.profile)
    store.set_runtime_metadata(MAX_TRADES_PER_DAY_KEY, str(settings.max_trades_per_day))
    store.set_runtime_metadata(AI_MIN_CONFIDENCE_KEY, f"{settings.ai_min_confidence:.2f}")
    return settings
