from datetime import datetime

import pytest

from kis_ai_scalper.execution import (
    Command,
    DuplicateSignalError,
    OrderState,
    SignalLedger,
    build_signal_id,
    transition,
)


def test_full_dry_run_lifecycle():
    state = OrderState.FLAT
    for command, expected in (
        (Command.WATCH, OrderState.WATCHING),
        (Command.ARM, OrderState.ARMED),
        (Command.SUBMIT_ENTRY, OrderState.ENTRY_PENDING),
        (Command.MARK_ENTRY_FILLED, OrderState.LONG),
        (Command.MARK_TP1_FILLED, OrderState.PARTIAL_LONG),
        (Command.SUBMIT_EXIT, OrderState.EXIT_PENDING),
        (Command.MARK_EXIT_FILLED, OrderState.FLAT),
    ):
        state = transition(state, command)
        assert state is expected


def test_invalid_exit_from_flat_is_rejected():
    with pytest.raises(ValueError, match="invalid transition"):
        transition(OrderState.FLAT, Command.SUBMIT_EXIT)


def test_flat_can_enter_cooldown_after_completed_cycle():
    assert transition(OrderState.FLAT, Command.COOLDOWN) is OrderState.COOLDOWN


def test_safe_mode_is_global_and_terminal():
    for state in OrderState:
        assert transition(state, Command.SAFE_MODE) is OrderState.SAFE_MODE
    with pytest.raises(ValueError):
        transition(OrderState.SAFE_MODE, Command.WATCH)


def test_signal_id_is_stable_and_searchable():
    timestamp = datetime(2026, 8, 15, 9, 1)
    first = build_signal_id("breakout-watch", "005930", timestamp)
    second = build_signal_id("breakout-watch", "005930", timestamp)
    assert first == second
    assert first.startswith("BREAKOUT_WATCH:005930:")


def test_ledger_rejects_duplicate_signal_id():
    ledger = SignalLedger()
    assert ledger.record("BREAKOUT_WATCH:005930:abc123") is True
    assert ledger.seen("BREAKOUT_WATCH:005930:abc123")
    with pytest.raises(DuplicateSignalError):
        ledger.record("BREAKOUT_WATCH:005930:abc123")
    assert len(ledger) == 1
