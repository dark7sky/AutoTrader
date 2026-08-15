"""In-memory idempotency guard for deterministic candidate signals."""

from __future__ import annotations

import hashlib
import re
from collections.abc import Iterable
from datetime import date, datetime


class DuplicateSignalError(ValueError):
    """Raised when a signal ID is recorded more than once."""


def _canonical_timestamp(timestamp: datetime | date | str) -> str:
    if isinstance(timestamp, datetime):
        value = timestamp.isoformat(timespec="seconds")
    elif isinstance(timestamp, date):
        value = timestamp.isoformat()
    else:
        value = str(timestamp).strip()
    if not value:
        raise ValueError("timestamp must not be empty")
    return value


def _prefix(value: str) -> str:
    normalized = re.sub(r"[^A-Za-z0-9]+", "_", str(value).strip()).strip("_")
    if not normalized:
        raise ValueError("strategy and symbol must not be empty")
    return normalized.upper()


def build_signal_id(
    strategy: str,
    symbol: str,
    timestamp: datetime | date | str,
) -> str:
    """Build a stable, human-searchable signal ID with a short hash suffix."""

    canonical = f"{strategy.strip()}|{symbol.strip()}|{_canonical_timestamp(timestamp)}"
    suffix = hashlib.sha256(canonical.encode("utf-8")).hexdigest()[:12]
    return f"{_prefix(strategy)}:{_prefix(symbol)}:{suffix}"


class SignalLedger:
    """Process-local set of signal IDs; it never touches a broker or database."""

    def __init__(self, signal_ids: Iterable[str] = ()) -> None:
        self._seen: set[str] = set(signal_ids)

    def record(self, signal_id: str) -> bool:
        signal_id = str(signal_id).strip()
        if not signal_id:
            raise ValueError("signal_id must not be empty")
        if signal_id in self._seen:
            raise DuplicateSignalError(f"duplicate signal_id: {signal_id}")
        self._seen.add(signal_id)
        return True

    def seen(self, signal_id: str) -> bool:
        return str(signal_id).strip() in self._seen

    def __len__(self) -> int:
        return len(self._seen)
