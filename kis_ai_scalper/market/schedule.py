"""Korea Exchange session helpers for broker-order safety gates."""

from __future__ import annotations

from datetime import datetime, time

try:
    import pandas as pd
    from exchange_calendars import get_calendar
except ImportError:
    pd = None
    get_calendar = None

from kis_ai_scalper.market.clock import KST


REGULAR_ORDER_START = time(9, 0)
REGULAR_ORDER_END = time(15, 20)
_XKRX_CALENDAR = None


def as_kst(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value
    return value.astimezone(KST).replace(tzinfo=None)


def is_regular_market_open(value: datetime) -> bool:
    now = as_kst(value)
    calendar = _calendar()
    if calendar is not None and pd is not None:
        timestamp = pd.Timestamp(now, tz=KST)
        return bool(calendar.is_open_on_minute(timestamp))
    if now.weekday() >= 5:
        return False
    return REGULAR_ORDER_START <= now.time() <= REGULAR_ORDER_END


def is_previous_trading_day_position(opened_at: datetime, now: datetime) -> bool:
    return as_kst(opened_at).date() < as_kst(now).date()


def _calendar():
    global _XKRX_CALENDAR
    if _XKRX_CALENDAR is False:
        return None
    if _XKRX_CALENDAR is None:
        if get_calendar is None:
            _XKRX_CALENDAR = False
            return None
        try:
            _XKRX_CALENDAR = get_calendar("XKRX")
        except Exception:
            _XKRX_CALENDAR = False
            return None
    return _XKRX_CALENDAR
