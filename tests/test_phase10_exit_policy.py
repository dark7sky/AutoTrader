from datetime import datetime, timedelta

import pytest

from kis_ai_scalper.execution.exit_policy import (
    ExitOrderState,
    ExitPolicy,
    ExitPolicyConfig,
    ExitPolicyError,
    ExitQuote,
)


QUOTE = ExitQuote(bid=99_900, ask=100_000, current_price=100_000)
NOW = datetime(2026, 8, 18, 14, 59)


def policy(**overrides):
    values = {
        "max_slippage_bps": 20,
        "max_requotes": 2,
        "requote_interval_seconds": 30,
        "close_urgency_seconds": 10,
        "max_quantity": 100,
    }
    values.update(overrides)
    return ExitPolicy(ExitPolicyConfig(**values))


def test_crash_is_rejected_when_bid_breaks_slippage_cap():
    with pytest.raises(ExitPolicyError, match="max slippage"):
        policy(max_slippage_bps=50).plan_sell_limit(
            ExitQuote(bid=99_000, ask=99_100, current_price=100_000)
        )


def test_sell_price_is_floored_to_krx_tick_and_stays_marketable():
    plan = policy(max_slippage_bps=10).plan_sell_limit(
        ExitQuote(bid=12_345, ask=12_350, current_price=12_345)
    )

    assert plan.price == 12_340
    assert plan.tick_size == 10
    assert plan.price <= 12_345
    assert plan.slippage_bps < 10


def test_stale_sell_enters_cancel_pending_without_replacement():
    p = policy()
    lifecycle = p.initial_lifecycle(order_id="sell-1", symbol="005930", quantity=10, now=NOW)

    stale = lifecycle.mark_stale()
    pending = stale.lifecycle.mark_cancel_pending()
    replacement = p.replacement_intent(pending.lifecycle, QUOTE, now=NOW + timedelta(minutes=1))

    assert stale.state is ExitOrderState.STALE
    assert pending.state is ExitOrderState.CANCEL_PENDING
    assert replacement.intent is None
    assert replacement.reason == "terminal_confirmation_required"


def test_partial_fill_terminal_cancel_replaces_only_remaining_quantity():
    p = policy()
    lifecycle = p.initial_lifecycle(order_id="sell-2", symbol="005930", quantity=10, now=NOW)
    pending = lifecycle.mark_stale().lifecycle.mark_cancel_pending().lifecycle
    partial = pending.observe_broker_status(
        "PARTIALLY_FILLED", filled_quantity=6, remaining_quantity=4
    )
    cancelled = partial.lifecycle.observe_broker_status("CANCELLED")

    replacement = p.replacement_intent(
        cancelled.lifecycle, QUOTE, now=NOW + timedelta(minutes=1)
    )

    assert cancelled.state is ExitOrderState.CANCELLED
    assert replacement.intent is not None
    assert replacement.intent.quantity == 4
    assert replacement.intent.replacement_of == "sell-2"
    assert replacement.state is ExitOrderState.REPLACEMENT_INTENT_CREATED


def test_terminal_cancel_is_required_before_replacement_and_interval_is_checked():
    p = policy()
    lifecycle = p.initial_lifecycle(order_id="sell-3", symbol="005930", quantity=2, now=NOW)
    pending = lifecycle.mark_stale().lifecycle.mark_cancel_pending().lifecycle
    pending_result = p.replacement_intent(pending, QUOTE, now=NOW + timedelta(seconds=31))

    assert pending_result.intent is None
    assert pending_result.state is ExitOrderState.CANCEL_PENDING

    cancelled = pending.observe_broker_status("CANCELLED", remaining_quantity=2)
    too_soon = p.replacement_intent(cancelled.lifecycle, QUOTE, now=NOW + timedelta(seconds=10))
    assert too_soon.intent is None
    assert too_soon.reason == "requote_interval_not_elapsed"


def test_unknown_status_never_creates_duplicate_replacement():
    p = policy()
    lifecycle = p.initial_lifecycle(order_id="sell-4", symbol="005930", quantity=2, now=NOW)
    pending = lifecycle.mark_stale().lifecycle.mark_cancel_pending().lifecycle
    unknown = pending.observe_broker_status("UNKNOWN")

    first = p.replacement_intent(unknown.lifecycle, QUOTE, now=NOW + timedelta(minutes=2))
    second = p.replacement_intent(first.lifecycle, QUOTE, now=NOW + timedelta(minutes=3))

    assert unknown.state is ExitOrderState.UNKNOWN
    assert first.intent is None and second.intent is None
    assert first.operator_review and second.operator_review


def test_close_urgency_can_clear_interval_but_not_terminal_or_price_guards():
    p = policy(close_urgency_seconds=10)
    lifecycle = p.initial_lifecycle(order_id="sell-5", symbol="005930", quantity=2, now=NOW)
    pending = lifecycle.mark_stale().lifecycle.mark_cancel_pending().lifecycle
    cancelled = pending.observe_broker_status("REJECTED", remaining_quantity=2)

    result = p.replacement_intent(
        cancelled.lifecycle,
        QUOTE,
        now=NOW + timedelta(seconds=1),
        seconds_to_close=5,
    )

    assert result.intent is not None
    assert result.intent.urgent is True


@pytest.mark.parametrize("quantity", [0, -1, 101, True])
def test_quantity_bounds_fail_closed(quantity):
    with pytest.raises(ExitPolicyError):
        policy().create_sell_intent(symbol="005930", quantity=quantity, quote=QUOTE)


def test_market_order_is_disabled_by_default():
    with pytest.raises(ExitPolicyError, match="market orders"):
        policy().create_sell_intent(
            symbol="005930", quantity=1, quote=QUOTE, order_type="MARKET"
        )


def test_unknown_and_cancel_pending_block_duplicate_cancel_or_replacement():
    p = policy()
    lifecycle = p.initial_lifecycle(order_id="sell-6", symbol="005930", quantity=1, now=NOW)
    pending = lifecycle.mark_stale().lifecycle.mark_cancel_pending()
    assert pending.lifecycle.mark_cancel_pending().reason == "cancel_pending_no_duplicate_cancel"
    unknown = pending.lifecycle.observe_broker_status("TIMEOUT")
    assert unknown.lifecycle.mark_stale().operator_review is True
