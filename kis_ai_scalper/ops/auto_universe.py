"""Cached runtime universe selection for the long-running trading service."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import json
from typing import Callable, Protocol


AUTO_UNIVERSE_SYMBOLS_KEY = "auto_universe.symbols"
AUTO_UNIVERSE_REFRESHED_AT_KEY = "auto_universe.refreshed_at"
AUTO_UNIVERSE_ATTEMPTED_AT_KEY = "auto_universe.attempted_at"
AUTO_UNIVERSE_ERROR_KEY = "auto_universe.last_error"


class RuntimeMetadataStore(Protocol):
    def get_runtime_metadata(self, key: str) -> str | None:
        ...

    def set_runtime_metadata(
        self, key: str, value: str, *, updated_at: datetime | None = None,
    ) -> None:
        ...


@dataclass(frozen=True)
class AutoUniverseSettings:
    enabled: bool = True
    size: int = 5
    refresh_seconds: int = 1800
    failure_retry_seconds: int = 60
    min_price: float = 2_000
    max_price: float = 200_000
    min_volume: int = 100_000
    min_change_pct: float = 0.5
    max_change_pct: float = 20.0

    def __post_init__(self) -> None:
        if self.size <= 0:
            raise ValueError("auto universe size must be positive")
        if self.refresh_seconds <= 0 or self.failure_retry_seconds <= 0:
            raise ValueError("auto universe refresh intervals must be positive")
        if self.min_price < 0 or self.max_price <= self.min_price:
            raise ValueError("auto universe price range is invalid")
        if self.min_volume < 0:
            raise ValueError("auto universe minimum volume must not be negative")
        if self.max_change_pct <= self.min_change_pct:
            raise ValueError("auto universe change range is invalid")


def _timestamp(raw: str | None) -> datetime | None:
    if not raw:
        return None
    try:
        value = datetime.fromisoformat(raw)
    except ValueError:
        return None
    return value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)


def _cached_symbols(raw: str | None, size: int) -> list[str]:
    try:
        values = json.loads(raw or "[]")
    except (TypeError, ValueError):
        return []
    if not isinstance(values, list):
        return []
    return list(dict.fromkeys(
        str(value) for value in values
        if str(value).isdigit() and len(str(value)) == 6
    ))[:size]


def _is_due(database: RuntimeMetadataStore, settings: AutoUniverseSettings, now: datetime) -> bool:
    error = database.get_runtime_metadata(AUTO_UNIVERSE_ERROR_KEY)
    timestamp_key = AUTO_UNIVERSE_ATTEMPTED_AT_KEY if error else AUTO_UNIVERSE_REFRESHED_AT_KEY
    previous = _timestamp(database.get_runtime_metadata(timestamp_key))
    if previous is None:
        return True
    interval = settings.failure_retry_seconds if error else settings.refresh_seconds
    return now >= previous + timedelta(seconds=interval)


def resolve_service_symbols(
    database: RuntimeMetadataStore,
    *,
    base_symbols: list[str],
    open_position_symbols: list[str],
    discover: Callable[[AutoUniverseSettings], list[str]],
    settings: AutoUniverseSettings,
    now: datetime,
) -> list[str]:
    dynamic = _cached_symbols(
        database.get_runtime_metadata(AUTO_UNIVERSE_SYMBOLS_KEY), settings.size,
    )
    if settings.enabled and _is_due(database, settings, now):
        database.set_runtime_metadata(
            AUTO_UNIVERSE_ATTEMPTED_AT_KEY, now.isoformat(), updated_at=now,
        )
        try:
            discovered = _cached_symbols(json.dumps(discover(settings)), settings.size)
        except Exception as exc:
            database.set_runtime_metadata(
                AUTO_UNIVERSE_ERROR_KEY, type(exc).__name__, updated_at=now,
            )
        else:
            dynamic = discovered
            database.set_runtime_metadata(
                AUTO_UNIVERSE_SYMBOLS_KEY, json.dumps(dynamic), updated_at=now,
            )
            database.set_runtime_metadata(
                AUTO_UNIVERSE_REFRESHED_AT_KEY, now.isoformat(), updated_at=now,
            )
            database.set_runtime_metadata(AUTO_UNIVERSE_ERROR_KEY, "", updated_at=now)
    if not settings.enabled:
        dynamic = []
    return list(dict.fromkeys([*base_symbols, *dynamic, *open_position_symbols]))
