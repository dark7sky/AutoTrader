from datetime import datetime

import kis_ai_scalper.market.schedule as schedule


class AlwaysOpenCalendar:
    def is_open_on_minute(self, _timestamp):
        return True


def test_calendar_failure_is_visible_to_broker_preflight(monkeypatch):
    monkeypatch.setattr(schedule, "_XKRX_CALENDAR", False)

    assert schedule.exchange_calendar_available() is False
    assert schedule.is_regular_market_open(datetime(2026, 8, 18, 10, 0)) is True


def test_entry_and_forced_exit_windows(monkeypatch):
    monkeypatch.setattr(schedule, "_XKRX_CALENDAR", AlwaysOpenCalendar())

    assert schedule.is_new_entry_window(datetime(2026, 8, 18, 15, 0)) is True
    assert schedule.is_new_entry_window(datetime(2026, 8, 18, 15, 1)) is False
    assert schedule.is_forced_exit_window(datetime(2026, 8, 18, 15, 9)) is False
    assert schedule.is_forced_exit_window(datetime(2026, 8, 18, 15, 10)) is True
    assert schedule.is_forced_exit_window(datetime(2026, 8, 18, 15, 21)) is False
