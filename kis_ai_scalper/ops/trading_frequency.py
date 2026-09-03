"""Runtime trade-frequency controls shared by Telegram and auto trading."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol


PROFILE_KEY = "trade_frequency.profile"
AI_MIN_CONFIDENCE_KEY = "trade_frequency.ai_min_confidence"


class RuntimeMetadataStore(Protocol):
    def get_runtime_metadata(self, key: str) -> str | None:
        ...

    def set_runtime_metadata(self, key: str, value: str) -> None:
        ...


@dataclass(frozen=True)
class TradeFrequencySettings:
    profile: str
    ai_min_confidence: float


FREQUENCY_PRESETS: dict[str, TradeFrequencySettings] = {
    "conservative": TradeFrequencySettings("conservative", 0.82),
    "normal": TradeFrequencySettings("normal", 0.75),
    "aggressive": TradeFrequencySettings("aggressive", 0.65),
}


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
    default_ai_min_confidence: float,
) -> TradeFrequencySettings:
    profile = (store.get_runtime_metadata(PROFILE_KEY) or "env").strip() or "env"
    confidence = _parse_confidence(
        store.get_runtime_metadata(AI_MIN_CONFIDENCE_KEY),
        default_ai_min_confidence,
    )
    return TradeFrequencySettings(profile, confidence)


def apply_trade_frequency_preset(
    store: RuntimeMetadataStore,
    profile: str,
) -> TradeFrequencySettings:
    try:
        settings = FREQUENCY_PRESETS[profile]
    except KeyError as exc:
        raise ValueError("unknown frequency profile") from exc
    store.set_runtime_metadata(PROFILE_KEY, settings.profile)
    store.set_runtime_metadata(AI_MIN_CONFIDENCE_KEY, f"{settings.ai_min_confidence:.2f}")
    return settings
