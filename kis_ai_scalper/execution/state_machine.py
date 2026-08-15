"""Pure dry-run order lifecycle state machine.

This module deliberately knows nothing about brokers, orders, accounts, or
network clients. It only validates the local lifecycle of a trading idea.
"""

from __future__ import annotations

from enum import StrEnum


class OrderState(StrEnum):
    FLAT = "FLAT"
    WATCHING = "WATCHING"
    ARMED = "ARMED"
    ENTRY_PENDING = "ENTRY_PENDING"
    LONG = "LONG"
    PARTIAL_LONG = "PARTIAL_LONG"
    EXIT_PENDING = "EXIT_PENDING"
    COOLDOWN = "COOLDOWN"
    SAFE_MODE = "SAFE_MODE"


class Command(StrEnum):
    WATCH = "WATCH"
    ARM = "ARM"
    SUBMIT_ENTRY = "SUBMIT_ENTRY"
    MARK_ENTRY_FILLED = "MARK_ENTRY_FILLED"
    MARK_TP1_FILLED = "MARK_TP1_FILLED"
    SUBMIT_EXIT = "SUBMIT_EXIT"
    MARK_EXIT_FILLED = "MARK_EXIT_FILLED"
    COOLDOWN = "COOLDOWN"
    SAFE_MODE = "SAFE_MODE"


_TRANSITIONS: dict[tuple[OrderState, Command], OrderState] = {
    (OrderState.FLAT, Command.WATCH): OrderState.WATCHING,
    (OrderState.WATCHING, Command.ARM): OrderState.ARMED,
    (OrderState.ARMED, Command.SUBMIT_ENTRY): OrderState.ENTRY_PENDING,
    (OrderState.ENTRY_PENDING, Command.MARK_ENTRY_FILLED): OrderState.LONG,
    (OrderState.LONG, Command.MARK_TP1_FILLED): OrderState.PARTIAL_LONG,
    (OrderState.LONG, Command.SUBMIT_EXIT): OrderState.EXIT_PENDING,
    (OrderState.PARTIAL_LONG, Command.SUBMIT_EXIT): OrderState.EXIT_PENDING,
    (OrderState.EXIT_PENDING, Command.MARK_EXIT_FILLED): OrderState.FLAT,
    (OrderState.FLAT, Command.COOLDOWN): OrderState.COOLDOWN,
    (OrderState.WATCHING, Command.COOLDOWN): OrderState.COOLDOWN,
    (OrderState.ARMED, Command.COOLDOWN): OrderState.COOLDOWN,
    (OrderState.COOLDOWN, Command.WATCH): OrderState.WATCHING,
}


def transition(state: OrderState, command: Command) -> OrderState:
    """Apply one command or raise for an impossible lifecycle transition."""

    state = OrderState(state)
    command = Command(command)
    if command is Command.SAFE_MODE:
        return OrderState.SAFE_MODE
    if state is OrderState.SAFE_MODE:
        raise ValueError(f"invalid transition: {state} + {command}")
    try:
        return _TRANSITIONS[(state, command)]
    except KeyError as exc:
        raise ValueError(f"invalid transition: {state} + {command}") from exc
