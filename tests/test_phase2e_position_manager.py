from datetime import datetime, timedelta

import pytest

from kis_ai_scalper.execution import (
    ManagedPosition,
    PositionAction,
    apply_position_decision,
    evaluate_position,
)


OPENED = datetime(2026, 8, 15, 9, 0)


def position(**overrides):
    values = dict(
        symbol="005930",
        quantity=10,
        entry_price=100.0,
        stop_loss=98.0,
        tp1_price=101.0,
        tp2_price=102.0,
        opened_at=OPENED,
    )
    values.update(overrides)
    return ManagedPosition(**values)


def test_stop_loss_exits_immediately():
    decision = evaluate_position(position(), 97.5, OPENED + timedelta(minutes=1))
    assert (decision.action, decision.reason, decision.quantity) == (
        PositionAction.EXIT,
        "stop_loss",
        10,
    )


def test_tp1_partially_exits_and_moves_stop_to_break_even():
    current = position(quantity=9)
    decision = evaluate_position(current, 101.0, OPENED + timedelta(minutes=1))
    assert decision.action is PositionAction.PARTIAL_EXIT
    assert decision.reason == "take_profit_1"
    assert decision.quantity == 5
    assert decision.new_stop_loss == 100.0
    updated = apply_position_decision(current, decision)
    assert updated.quantity == 4
    assert updated.tp1_filled is True
    assert updated.stop_loss >= updated.entry_price


def test_tp1_break_even_buffer_is_supported():
    decision = evaluate_position(
        position(), 101.0, OPENED + timedelta(minutes=1), break_even_buffer_pct=0.1
    )
    assert decision.new_stop_loss == pytest.approx(100.1)


def test_tp1_single_share_exits_instead_of_partial_exit():
    current = position(quantity=1)
    decision = evaluate_position(current, 101.0, OPENED + timedelta(minutes=1))
    assert decision.action is PositionAction.EXIT
    assert decision.reason == "take_profit_1_single_share"
    assert decision.quantity == 1


def test_tp2_exits_remaining_position():
    decision = evaluate_position(
        position(tp1_filled=True, quantity=4), 102.0, OPENED + timedelta(minutes=2)
    )
    assert decision.action is PositionAction.EXIT
    assert decision.reason == "take_profit_2"
    assert decision.quantity == 4


def test_tp2_has_priority_over_trailing_exit():
    current = position(tp1_filled=True, trailing_active=True, highest_price=103.0)
    decision = evaluate_position(current, 102.0, OPENED + timedelta(minutes=2))
    assert decision.reason == "take_profit_2"


def test_filled_tp1_does_not_emit_duplicate_partial_exit():
    current = position(tp1_filled=True, quantity=5)
    decision = evaluate_position(current, 101.0, OPENED + timedelta(minutes=1))
    assert decision.action is PositionAction.HOLD
    assert decision.quantity == 0


def test_trailing_activates_and_updates_high_without_exiting():
    decision = evaluate_position(position(), 100.6, OPENED + timedelta(minutes=1))
    assert decision.action is PositionAction.HOLD
    assert decision.reason == "hold"
    assert decision.trailing_active is True
    assert decision.highest_price == 100.6


def test_trailing_exits_after_drop_from_high():
    current = position(highest_price=101.0, trailing_active=True)
    decision = evaluate_position(current, 100.7, OPENED + timedelta(minutes=2))
    assert decision.action is PositionAction.EXIT
    assert decision.reason == "trailing_stop"
    assert decision.quantity == 10


def test_time_stop_exits_before_tp1():
    current = position(max_holding_seconds=60)
    decision = evaluate_position(current, 100.2, OPENED + timedelta(seconds=60))
    assert decision.action is PositionAction.EXIT
    assert decision.reason == "time_stop"


def test_hold_updates_high_without_mutating_input():
    current = position(highest_price=100.4)
    decision = evaluate_position(current, 100.5, OPENED + timedelta(minutes=1))
    assert decision.action is PositionAction.HOLD
    assert decision.highest_price == 100.5
    assert current.highest_price == 100.4
    updated = apply_position_decision(current, decision)
    assert updated.highest_price == 100.5


def test_invalid_position_and_time_are_rejected():
    with pytest.raises(ValueError, match="quantity"):
        evaluate_position(position(quantity=0), 100.0, OPENED)
    with pytest.raises(ValueError, match="price ladder"):
        evaluate_position(position(stop_loss=101.0), 100.0, OPENED)
    with pytest.raises(ValueError, match="tp1_ratio"):
        evaluate_position(position(tp1_ratio=1.0), 100.0, OPENED)
    with pytest.raises(ValueError, match="now"):
        evaluate_position(position(), 100.0, OPENED - timedelta(seconds=1))
