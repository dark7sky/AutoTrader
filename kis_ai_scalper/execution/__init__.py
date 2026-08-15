"""Broker-independent dry-run execution safety primitives."""

from .idempotency import DuplicateSignalError, SignalLedger, build_signal_id
from .position_manager import (
    ManagedPosition,
    PositionAction,
    PositionDecision,
    apply_position_decision,
    evaluate_position,
)
from .state_machine import Command, OrderState, transition
from .guarded_submitter import GuardedOrderSubmitter, OrderSafetyGateError
from .exit_policy import ExitPolicy, ExitPolicyConfig, ExitPolicyError, ExitQuote

__all__ = [
    "Command",
    "DuplicateSignalError",
    "ExitPolicy",
    "ExitPolicyConfig",
    "ExitPolicyError",
    "ExitQuote",
    "GuardedOrderSubmitter",
    "OrderState",
    "OrderSafetyGateError",
    "ManagedPosition",
    "PositionAction",
    "PositionDecision",
    "SignalLedger",
    "apply_position_decision",
    "build_signal_id",
    "evaluate_position",
    "transition",
]
