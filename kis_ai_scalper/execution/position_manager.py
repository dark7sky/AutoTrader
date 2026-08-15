"""Pure position-lifecycle evaluation.

The manager produces local exit intents only.  It has no broker, database,
network, order, account, or AI dependency, and it never mutates a position.
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import datetime
from enum import StrEnum
from math import isfinite, floor


class PositionAction(StrEnum):
    HOLD = "HOLD"
    PARTIAL_EXIT = "PARTIAL_EXIT"
    EXIT = "EXIT"


@dataclass(frozen=True)
class ManagedPosition:
    symbol: str
    quantity: int
    entry_price: float
    stop_loss: float
    tp1_price: float
    tp2_price: float
    opened_at: datetime
    tp1_ratio: float = 0.5
    tp1_filled: bool = False
    highest_price: float | None = None
    trailing_active: bool = False
    max_holding_seconds: int | None = None


@dataclass(frozen=True)
class PositionDecision:
    action: PositionAction
    reason: str
    quantity: int
    new_stop_loss: float | None = None
    trailing_active: bool | None = None
    highest_price: float | None = None


def _validate(position: ManagedPosition, current_price: float, now: datetime) -> None:
    if not position.symbol.strip():
        raise ValueError("symbol must not be empty")
    if position.quantity < 1:
        raise ValueError("quantity must be positive")
    prices = (
        current_price,
        position.entry_price,
        position.stop_loss,
        position.tp1_price,
        position.tp2_price,
    )
    if not all(isfinite(price) and price > 0 for price in prices):
        raise ValueError("prices must be finite and positive")
    if not 0 < position.tp1_ratio < 1:
        raise ValueError("tp1_ratio must be greater than 0 and less than 1")
    if not position.stop_loss < position.entry_price < position.tp1_price <= position.tp2_price:
        raise ValueError("price ladder must satisfy stop_loss < entry_price < tp1_price <= tp2_price")
    if position.max_holding_seconds is not None and position.max_holding_seconds < 0:
        raise ValueError("max_holding_seconds must not be negative")
    if position.highest_price is not None and (
        not isfinite(position.highest_price) or position.highest_price <= 0
    ):
        raise ValueError("highest_price must be finite and positive")
    if now < position.opened_at:
        raise ValueError("now must not precede opened_at")
    # Aware and naive datetimes cannot be compared safely.
    if (now.tzinfo is None) != (position.opened_at.tzinfo is None):
        raise ValueError("opened_at and now must use compatible timezone information")


def evaluate_position(
    position: ManagedPosition,
    current_price: float,
    now: datetime,
    *,
    trailing_start_pct: float = 0.5,
    trailing_drop_pct: float = 0.25,
    break_even_buffer_pct: float = 0.0,
) -> PositionDecision:
    """Evaluate a position and return a broker-independent intent.

    Priority is hard stop, time stop, TP2, TP1, trailing activation/exit, then
    hold.  The returned fields let the caller persist lifecycle state without
    allowing this pure function to mutate the supplied position.
    """

    _validate(position, current_price, now)
    if trailing_start_pct < 0 or trailing_drop_pct < 0 or break_even_buffer_pct < 0:
        raise ValueError("trailing and break-even percentages must not be negative")

    highest_price = max(current_price, position.highest_price or current_price)
    trailing_active = position.trailing_active or (
        current_price >= position.entry_price * (1 + trailing_start_pct / 100)
    )
    state = dict(trailing_active=trailing_active, highest_price=highest_price)

    if current_price <= position.stop_loss:
        return PositionDecision(PositionAction.EXIT, "stop_loss", position.quantity, **state)

    holding_seconds = (now - position.opened_at).total_seconds()
    if (
        position.max_holding_seconds is not None
        and holding_seconds >= position.max_holding_seconds
        and not position.tp1_filled
        and current_price < position.tp1_price
    ):
        return PositionDecision(PositionAction.EXIT, "time_stop", position.quantity, **state)

    if current_price >= position.tp2_price:
        return PositionDecision(PositionAction.EXIT, "take_profit_2", position.quantity, **state)

    if not position.tp1_filled and current_price >= position.tp1_price:
        if position.quantity == 1:
            return PositionDecision(PositionAction.EXIT, "take_profit_1_single_share", 1, **state)
        # Use conventional half-up rounding; Python's round uses ties-to-even.
        partial_quantity = min(
            position.quantity - 1,
            max(1, floor(position.quantity * position.tp1_ratio + 0.5)),
        )
        break_even = position.entry_price * (1 + break_even_buffer_pct / 100)
        return PositionDecision(
            PositionAction.PARTIAL_EXIT,
            "take_profit_1",
            min(partial_quantity, position.quantity),
            new_stop_loss=break_even,
            **state,
        )

    if trailing_active and current_price <= highest_price * (1 - trailing_drop_pct / 100):
        return PositionDecision(PositionAction.EXIT, "trailing_stop", position.quantity, **state)

    return PositionDecision(PositionAction.HOLD, "hold", 0, **state)


def apply_position_decision(
    position: ManagedPosition, decision: PositionDecision
) -> ManagedPosition:
    """Return the local position state after applying a pure decision."""

    if decision.action is PositionAction.PARTIAL_EXIT:
        remaining = position.quantity - decision.quantity
        if remaining < 1:
            raise ValueError("partial exit must leave at least one share")
        return replace(
            position,
            quantity=remaining,
            tp1_filled=True,
            stop_loss=decision.new_stop_loss or position.stop_loss,
            highest_price=decision.highest_price or position.highest_price,
            trailing_active=(
                position.trailing_active
                if decision.trailing_active is None
                else decision.trailing_active
            ),
        )
    if decision.action is PositionAction.EXIT:
        return replace(
            position,
            quantity=0,
            highest_price=decision.highest_price or position.highest_price,
            trailing_active=(
                position.trailing_active
                if decision.trailing_active is None
                else decision.trailing_active
            ),
        )
    return replace(
        position,
        highest_price=decision.highest_price or position.highest_price,
        trailing_active=(
            position.trailing_active
            if decision.trailing_active is None
            else decision.trailing_active
        ),
    )
