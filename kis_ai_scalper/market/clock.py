"""KST clock helpers for KIS market data timestamps."""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone


KST = timezone(timedelta(hours=9), "KST")


def kst_now() -> datetime:
    """Return a naive KST datetime matching KIS domestic-market timestamps."""
    return datetime.now(KST).replace(tzinfo=None)


def kst_today() -> date:
    return kst_now().date()
