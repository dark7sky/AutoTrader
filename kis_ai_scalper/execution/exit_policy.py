"""Fail-closed, broker-independent policy for protective SELL execution.

The policy only creates limit-order intents.  In particular, a stale order is
never replaced from ``STALE`` or ``CANCEL_PENDING``: the broker must first
confirm ``CANCELLED``, ``REJECTED``, or ``FILLED``.
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import datetime
from decimal import Decimal, InvalidOperation, ROUND_FLOOR
from enum import StrEnum
import math
from typing import Any, Callable

from kis_ai_scalper.broker.kis_market_rules import krx_tick_size


class ExitPolicyError(ValueError):
    """Raised when an exit would violate a fail-closed policy boundary."""


class ExitOrderState(StrEnum):
    SUBMITTED = "SUBMITTED"
    STALE = "STALE"
    CANCEL_PENDING = "CANCEL_PENDING"
    CANCELLED = "CANCELLED"
    REJECTED = "REJECTED"
    FILLED = "FILLED"
    UNKNOWN = "UNKNOWN"
    OPERATOR_REVIEW = "OPERATOR_REVIEW"
    REPLACEMENT_INTENT_CREATED = "REPLACEMENT_INTENT_CREATED"


TERMINAL_STATES = frozenset({
    ExitOrderState.CANCELLED,
    ExitOrderState.REJECTED,
    ExitOrderState.FILLED,
})


def _decimal(value: Any, name: str, *, positive: bool = True) -> Decimal:
    if isinstance(value, bool):
        raise ExitPolicyError(f"{name} must be a finite number")
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise ExitPolicyError(f"{name} must be a finite number") from exc
    if not parsed.is_finite() or (parsed <= 0 if positive else parsed < 0):
        qualifier = "positive" if positive else "non-negative"
        raise ExitPolicyError(f"{name} must be a finite {qualifier} number")
    return parsed


def _whole_positive(value: Any, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ExitPolicyError(f"{name} must be a positive integer")
    return value


def _symbol(value: Any) -> str:
    symbol = str(value).strip()
    if len(symbol) != 6 or not symbol.isdigit():
        raise ExitPolicyError("symbol must be a six-digit domestic stock code")
    return symbol


def _tick_floor(price: Decimal, tick_size_fn: Callable[[Any], Any]) -> tuple[int, int]:
    try:
        tick = tick_size_fn(price)
    except Exception as exc:
        raise ExitPolicyError("KRX tick rule failed") from exc
    if isinstance(tick, bool) or not isinstance(tick, int) or tick <= 0:
        raise ExitPolicyError("KRX tick rule returned an invalid tick")
    normalized = (price / Decimal(tick)).to_integral_value(rounding=ROUND_FLOOR) * Decimal(tick)
    if normalized <= 0 or normalized != normalized.to_integral_value():
        raise ExitPolicyError("normalized SELL price is invalid")
    return int(normalized), tick


def _validate_order_type(order_type: Any, *, allow_market_order: bool) -> str:
    normalized = str(order_type).strip().upper()
    if normalized == "LIMIT":
        return normalized
    if normalized == "MARKET" and allow_market_order:
        return normalized
    if normalized == "MARKET":
        raise ExitPolicyError("market orders are disabled by exit policy")
    raise ExitPolicyError("exit order type must be LIMIT")


@dataclass(frozen=True)
class ExitPolicyConfig:
    """Hard limits for a single exit chain."""

    max_slippage_bps: float = 50.0
    max_requotes: int = 3
    requote_interval_seconds: float = 5.0
    close_urgency_seconds: float = 30.0
    min_quantity: int = 1
    max_quantity: int = 1_000_000
    allow_market_order: bool = False

    def __post_init__(self) -> None:
        slippage = _decimal(self.max_slippage_bps, "max_slippage_bps", positive=False)
        if slippage >= Decimal("10000"):
            raise ExitPolicyError("max_slippage_bps must be below 10000")
        if isinstance(self.max_requotes, bool) or not isinstance(self.max_requotes, int) or self.max_requotes < 0:
            raise ExitPolicyError("max_requotes must be a non-negative integer")
        interval = _decimal(self.requote_interval_seconds, "requote_interval_seconds", positive=False)
        if interval <= 0:
            raise ExitPolicyError("requote_interval_seconds must be positive")
        _decimal(self.close_urgency_seconds, "close_urgency_seconds", positive=False)
        if isinstance(self.min_quantity, bool) or not isinstance(self.min_quantity, int) or self.min_quantity <= 0:
            raise ExitPolicyError("min_quantity must be a positive integer")
        if isinstance(self.max_quantity, bool) or not isinstance(self.max_quantity, int):
            raise ExitPolicyError("max_quantity must be a positive integer")
        if self.max_quantity < self.min_quantity:
            raise ExitPolicyError("max_quantity must be at least min_quantity")
        if not isinstance(self.allow_market_order, bool):
            raise ExitPolicyError("allow_market_order must be boolean")


@dataclass(frozen=True)
class ExitQuote:
    """The latest quote used for one deterministic SELL price decision."""

    bid: Any | None
    ask: Any | None
    current_price: Any | None


@dataclass(frozen=True)
class SellLimitPlan:
    price: int
    tick_size: int
    slippage_bps: float
    source: str
    urgent: bool


@dataclass(frozen=True)
class SellOrderIntent:
    symbol: str
    quantity: int
    price: int
    side: str = "SELL"
    order_type: str = "LIMIT"
    reason: str = "exit"
    requote_count: int = 0
    replacement_of: str | None = None
    urgent: bool = False

    def __post_init__(self) -> None:
        if _symbol(self.symbol) != self.symbol:
            raise ExitPolicyError("symbol must be a six-digit domestic stock code")
        _whole_positive(self.quantity, "quantity")
        _whole_positive(self.price, "price")
        if self.side != "SELL" or self.order_type != "LIMIT":
            raise ExitPolicyError("exit intents must be SELL LIMIT orders")
        if isinstance(self.requote_count, bool) or not isinstance(self.requote_count, int) or self.requote_count < 0:
            raise ExitPolicyError("requote_count must be a non-negative integer")


@dataclass(frozen=True)
class ExitTransition:
    lifecycle: "ExitOrderLifecycle"
    action: str
    reason: str
    intent: SellOrderIntent | None = None
    operator_review: bool = False

    @property
    def state(self) -> ExitOrderState:
        return self.lifecycle.state

    @property
    def replacement_intent(self) -> SellOrderIntent | None:
        return self.intent


@dataclass(frozen=True)
class ExitOrderLifecycle:
    """Immutable state for one original order and its replacement chain."""

    order_id: str
    symbol: str
    requested_quantity: int
    state: ExitOrderState = ExitOrderState.SUBMITTED
    filled_quantity: int = 0
    remaining_quantity: int | None = None
    requote_count: int = 0
    last_intent_at: Any | None = None

    def __post_init__(self) -> None:
        if not str(self.order_id).strip():
            raise ExitPolicyError("order_id is required")
        _symbol(self.symbol)
        _whole_positive(self.requested_quantity, "requested_quantity")
        if self.remaining_quantity is None:
            object.__setattr__(self, "remaining_quantity", self.requested_quantity - self.filled_quantity)
        for name, value in (("filled_quantity", self.filled_quantity), ("remaining_quantity", self.remaining_quantity)):
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise ExitPolicyError(f"{name} must be a non-negative integer")
        if self.filled_quantity + self.remaining_quantity != self.requested_quantity:
            raise ExitPolicyError("filled and remaining quantities must reconcile")
        if isinstance(self.requote_count, bool) or not isinstance(self.requote_count, int) or self.requote_count < 0:
            raise ExitPolicyError("requote_count must be a non-negative integer")
        object.__setattr__(self, "state", ExitOrderState(self.state))

    def mark_stale(self) -> ExitTransition:
        if self.state in {ExitOrderState.UNKNOWN, ExitOrderState.OPERATOR_REVIEW}:
            return ExitTransition(self, "OPERATOR_REVIEW", "unknown_order_no_action", operator_review=True)
        if self.state is ExitOrderState.CANCEL_PENDING:
            return ExitTransition(self, "WAIT", "cancel_pending_no_duplicate_cancel")
        if self.state in TERMINAL_STATES:
            return ExitTransition(self, "WAIT", "terminal_order_no_action")
        if self.state is ExitOrderState.STALE:
            return ExitTransition(self, "WAIT", "stale_cancel_not_yet_requested")
        return ExitTransition(replace(self, state=ExitOrderState.STALE), "CANCEL_REQUIRED", "stale_sell")

    def mark_cancel_pending(self) -> ExitTransition:
        if self.state is ExitOrderState.CANCEL_PENDING:
            return ExitTransition(self, "WAIT", "cancel_pending_no_duplicate_cancel")
        if self.state is not ExitOrderState.STALE:
            return ExitTransition(self, "OPERATOR_REVIEW", "cancel_requires_stale_state", operator_review=True)
        return ExitTransition(replace(self, state=ExitOrderState.CANCEL_PENDING), "CANCEL", "cancel_requested")

    def observe_broker_status(
        self,
        status: Any,
        *,
        filled_quantity: int | None = None,
        remaining_quantity: int | None = None,
    ) -> ExitTransition:
        normalized = str(status).strip().upper()
        if normalized in {"TIMEOUT", "UNKNOWN", "ERROR", "UNAVAILABLE"}:
            unknown = replace(self, state=ExitOrderState.UNKNOWN)
            return ExitTransition(unknown, "OPERATOR_REVIEW", "broker_status_unknown", operator_review=True)
        if normalized in {"UNFILLED", "PARTIALLY_FILLED", "ACKNOWLEDGED", "CANCEL_PENDING"}:
            if filled_quantity is None and remaining_quantity is None:
                return ExitTransition(self, "WAIT", "terminal_confirmation_required")
            if filled_quantity is None:
                remaining = remaining_quantity
                filled = self.requested_quantity - remaining_quantity
            elif remaining_quantity is None:
                filled = filled_quantity
                remaining = self.requested_quantity - filled_quantity
            else:
                filled = filled_quantity
                remaining = remaining_quantity
            if (
                isinstance(filled, bool) or not isinstance(filled, int) or filled < 0
                or isinstance(remaining, bool) or not isinstance(remaining, int) or remaining < 0
                or filled + remaining != self.requested_quantity
            ):
                unknown = replace(self, state=ExitOrderState.UNKNOWN)
                return ExitTransition(unknown, "OPERATOR_REVIEW", "broker_quantity_mismatch", operator_review=True)
            updated = replace(self, filled_quantity=filled, remaining_quantity=remaining)
            return ExitTransition(updated, "WAIT", "terminal_confirmation_required")
        if normalized not in {"CANCELLED", "REJECTED", "FILLED"}:
            unknown = replace(self, state=ExitOrderState.UNKNOWN)
            return ExitTransition(unknown, "OPERATOR_REVIEW", "unrecognized_broker_status", operator_review=True)

        if filled_quantity is None and remaining_quantity is None:
            filled = self.requested_quantity if normalized == "FILLED" else self.filled_quantity
            remaining = 0 if normalized == "FILLED" else self.remaining_quantity
        elif filled_quantity is None:
            remaining = remaining_quantity
            filled = self.requested_quantity - remaining_quantity
        elif remaining_quantity is None:
            filled = filled_quantity
            remaining = self.requested_quantity - filled_quantity
        else:
            filled = filled_quantity
            remaining = remaining_quantity
        if (
            isinstance(filled, bool) or not isinstance(filled, int) or filled < 0
            or isinstance(remaining, bool) or not isinstance(remaining, int) or remaining < 0
            or filled + remaining != self.requested_quantity
            or (normalized == "FILLED" and remaining != 0)
        ):
            unknown = replace(self, state=ExitOrderState.UNKNOWN)
            return ExitTransition(unknown, "OPERATOR_REVIEW", "broker_quantity_mismatch", operator_review=True)
        terminal = replace(
            self,
            state=ExitOrderState(normalized),
            filled_quantity=filled,
            remaining_quantity=remaining,
        )
        return ExitTransition(terminal, "TERMINAL_CONFIRMED", f"{normalized.lower()}_confirmed")

    def replacement(
        self,
        policy: "ExitPolicy",
        quote: ExitQuote,
        *,
        now: Any | None = None,
        seconds_to_close: Any | None = None,
        reason: str = "exit_requote",
    ) -> ExitTransition:
        if self.state in {ExitOrderState.UNKNOWN, ExitOrderState.OPERATOR_REVIEW}:
            return ExitTransition(self, "OPERATOR_REVIEW", "unknown_order_no_replacement", operator_review=True)
        if self.state is ExitOrderState.CANCEL_PENDING or self.state is ExitOrderState.STALE:
            return ExitTransition(self, "WAIT", "terminal_confirmation_required")
        if self.state is ExitOrderState.REPLACEMENT_INTENT_CREATED:
            return ExitTransition(self, "WAIT", "duplicate_replacement_blocked")
        if self.state is ExitOrderState.FILLED or self.remaining_quantity == 0:
            return ExitTransition(self, "DONE", "no_remaining_quantity")
        if self.state not in {ExitOrderState.CANCELLED, ExitOrderState.REJECTED}:
            return ExitTransition(self, "WAIT", "replacement_requires_terminal_cancel_or_reject")
        if self.requote_count >= policy.config.max_requotes:
            return ExitTransition(self, "OPERATOR_REVIEW", "max_requotes_reached", operator_review=True)
        urgent = policy.is_close_urgent(seconds_to_close)
        if not urgent and not policy.interval_elapsed(self.last_intent_at, now):
            return ExitTransition(self, "WAIT", "requote_interval_not_elapsed")
        intent = policy.create_sell_intent(
            symbol=self.symbol,
            quantity=self.remaining_quantity,
            quote=quote,
            reason=reason,
            requote_count=self.requote_count + 1,
            replacement_of=self.order_id,
            seconds_to_close=seconds_to_close,
        )
        next_lifecycle = replace(
            self,
            state=ExitOrderState.REPLACEMENT_INTENT_CREATED,
            requested_quantity=self.remaining_quantity,
            filled_quantity=0,
            last_intent_at=now,
            requote_count=self.requote_count + 1,
        )
        return ExitTransition(next_lifecycle, "CREATE_REPLACEMENT_INTENT", "terminal_cancel_confirmed", intent)


class ExitPolicy:
    def __init__(self, config: ExitPolicyConfig | None = None, *, tick_size_fn: Callable[[Any], Any] = krx_tick_size) -> None:
        self.config = config or ExitPolicyConfig()
        self.tick_size_fn = tick_size_fn

    def is_close_urgent(self, seconds_to_close: Any | None) -> bool:
        if seconds_to_close is None:
            return False
        return _decimal(seconds_to_close, "seconds_to_close", positive=False) <= Decimal(str(self.config.close_urgency_seconds))

    def interval_elapsed(self, last_intent_at: Any | None, now: Any | None) -> bool:
        if last_intent_at is None or now is None:
            return False
        try:
            if isinstance(last_intent_at, datetime) and isinstance(now, datetime):
                elapsed = (now - last_intent_at).total_seconds()
            else:
                elapsed = float(now) - float(last_intent_at)
        except (TypeError, ValueError, OverflowError):
            return False
        return math.isfinite(elapsed) and elapsed >= self.config.requote_interval_seconds

    def plan_sell_limit(self, quote: ExitQuote, *, seconds_to_close: Any | None = None) -> SellLimitPlan:
        if quote.bid is None:
            raise ExitPolicyError("latest bid is required for a marketable SELL limit")
        bid = _decimal(quote.bid, "bid")
        current = _decimal(quote.current_price, "current_price") if quote.current_price is not None else bid
        ask = _decimal(quote.ask, "ask") if quote.ask is not None else None
        if ask is not None and bid > ask:
            raise ExitPolicyError("crossed quote: bid must not exceed ask")
        price, tick = _tick_floor(bid, self.tick_size_fn)
        max_loss_price = current * (Decimal("1") - Decimal(str(self.config.max_slippage_bps)) / Decimal("10000"))
        if Decimal(price) < max_loss_price:
            raise ExitPolicyError("SELL price exceeds max slippage")
        actual_slippage = max(Decimal("0"), (current - Decimal(price)) / current * Decimal("10000"))
        urgent = self.is_close_urgent(seconds_to_close)
        return SellLimitPlan(price, tick, float(actual_slippage), "bid", urgent)

    def create_sell_intent(
        self,
        *,
        symbol: str,
        quantity: int,
        quote: ExitQuote,
        reason: str = "exit",
        requote_count: int = 0,
        replacement_of: str | None = None,
        seconds_to_close: Any | None = None,
        order_type: str = "LIMIT",
    ) -> SellOrderIntent:
        _symbol(symbol)
        if isinstance(quantity, bool) or not isinstance(quantity, int):
            raise ExitPolicyError("quantity must be a positive integer")
        if quantity < self.config.min_quantity or quantity > self.config.max_quantity:
            raise ExitPolicyError("quantity is outside exit policy bounds")
        if isinstance(requote_count, bool) or not isinstance(requote_count, int) or not 0 <= requote_count <= self.config.max_requotes:
            raise ExitPolicyError("requote_count is outside exit policy bounds")
        _validate_order_type(order_type, allow_market_order=self.config.allow_market_order)
        plan = self.plan_sell_limit(quote, seconds_to_close=seconds_to_close)
        return SellOrderIntent(
            symbol=symbol,
            quantity=quantity,
            price=plan.price,
            reason=str(reason).strip() or "exit",
            requote_count=requote_count,
            replacement_of=replacement_of,
            urgent=plan.urgent,
        )

    def initial_lifecycle(
        self,
        *,
        order_id: str,
        symbol: str,
        quantity: int,
        now: Any | None = None,
    ) -> ExitOrderLifecycle:
        if quantity < self.config.min_quantity or quantity > self.config.max_quantity:
            raise ExitPolicyError("quantity is outside exit policy bounds")
        return ExitOrderLifecycle(order_id, symbol, quantity, last_intent_at=now)

    def replacement_intent(self, lifecycle: ExitOrderLifecycle, quote: ExitQuote, **kwargs: Any) -> ExitTransition:
        return lifecycle.replacement(self, quote, **kwargs)


def calculate_sell_limit_price(
    *,
    bid: Any,
    ask: Any | None,
    current_price: Any,
    max_slippage_bps: float = 50.0,
    tick_size_fn: Callable[[Any], Any] = krx_tick_size,
) -> int:
    """Return a KRX-tick-normalized, marketable SELL limit price."""
    policy = ExitPolicy(ExitPolicyConfig(max_slippage_bps=max_slippage_bps), tick_size_fn=tick_size_fn)
    return policy.plan_sell_limit(ExitQuote(bid, ask, current_price)).price


def build_sell_intent(*args: Any, **kwargs: Any) -> SellOrderIntent:
    """Functional entry point for callers that do not need a policy object."""
    policy = kwargs.pop("policy", None) or ExitPolicy()
    return policy.create_sell_intent(*args, **kwargs)


def transition_exit_order(lifecycle: ExitOrderLifecycle, event: str, **kwargs: Any) -> ExitTransition:
    """Apply a lifecycle event without allowing implicit replacement."""
    normalized = str(event).strip().upper()
    if normalized == "STALE":
        return lifecycle.mark_stale()
    if normalized in {"CANCEL", "CANCEL_REQUESTED", "CANCEL_PENDING"}:
        return lifecycle.mark_cancel_pending()
    if normalized in {"STATUS", "BROKER_STATUS", "TERMINAL"}:
        if "status" not in kwargs:
            raise ExitPolicyError("broker status is required")
        return lifecycle.observe_broker_status(**kwargs)
    raise ExitPolicyError("unsupported exit lifecycle event")


# Small aliases make the policy usable from services that call the result an
# order request or a state transition rather than an intent.
ExitOrderIntent = SellOrderIntent
ExitLifecycle = ExitOrderLifecycle
build_marketable_sell_limit = calculate_sell_limit_price


__all__ = [
    "ExitLifecycle",
    "ExitOrderIntent",
    "ExitOrderLifecycle",
    "ExitOrderState",
    "ExitPolicy",
    "ExitPolicyConfig",
    "ExitPolicyError",
    "ExitQuote",
    "ExitTransition",
    "SellLimitPlan",
    "SellOrderIntent",
    "build_marketable_sell_limit",
    "build_sell_intent",
    "calculate_sell_limit_price",
    "transition_exit_order",
]
