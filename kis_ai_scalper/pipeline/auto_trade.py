"""One bounded multi-symbol AI auto-trading cycle."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
import math
from typing import Any, Callable, Protocol

from kis_ai_scalper.ai.decision import (
    AIDecisionAction,
    TradingAIClient,
    TradingAIDecision,
    context_from_snapshot,
)
from kis_ai_scalper.broker.kis_order import KisOrderRequest, KisOrderResult, KisOrderSide
from kis_ai_scalper.market.features import build_feature_snapshot
from kis_ai_scalper.market.clock import kst_now
from kis_ai_scalper.market.tick import MinuteBar
from kis_ai_scalper.market.schedule import (
    as_kst,
    is_forced_exit_window,
    is_new_entry_window,
    is_previous_trading_day_position,
    is_regular_market_open,
)
from kis_ai_scalper.broker.kis_market_rules import (
    normalize_krx_limit_price,
    validate_risk_reward,
)
from kis_ai_scalper.execution.exit_policy import ExitPolicy, ExitPolicyError, ExitQuote
from kis_ai_scalper.risk import OrderIntent, PortfolioState, RiskConfig, evaluate_order_intent
from kis_ai_scalper.storage.database import Database, RuntimeControl
from kis_ai_scalper.strategies.candidate import scan_candidates


class OrderSubmitter(Protocol):
    def submit_order(self, request: KisOrderRequest) -> KisOrderResult:
        ...


class Notifier(Protocol):
    def send(self, text: str) -> None:
        ...


@dataclass(frozen=True)
class AutoTradeConfig:
    risk: RiskConfig = RiskConfig()
    max_quantity: int = 1
    min_confidence: float = 0.75
    max_entry_deviation_pct: float = 1.0
    require_confirmation: bool = True
    enforce_market_hours: bool = True
    max_tick_age_seconds: int = 10
    max_bar_age_seconds: int = 90
    cycle_deadline_seconds: float | None = 30.0
    max_ai_response_age_seconds: float | None = None
    max_exit_requotes: int = 3


@dataclass(frozen=True)
class AutoTradeSymbolResult:
    symbol: str
    action: str
    submitted: bool
    blocked: bool
    reason: str
    quantity: int = 0
    broker_order_id: str | None = None


@dataclass(frozen=True)
class AutoTradeCycleReport:
    results: tuple[AutoTradeSymbolResult, ...]

    @property
    def submitted_count(self) -> int:
        return sum(1 for result in self.results if result.submitted)


def run_auto_trade_cycle(
    symbols: list[str],
    *,
    database: Database,
    ai_client: TradingAIClient,
    submitter: OrderSubmitter,
    runtime_control: RuntimeControl,
    config: AutoTradeConfig | None = None,
    confirm_auto_trade: bool = False,
    notifier: Notifier | None = None,
    current_time: datetime | None = None,
    portfolio: PortfolioState | None = None,
    entry_budget_checker: Callable[[str, float, int], bool] | None = None,
    clock: Callable[[], datetime] | None = None,
    post_ai_price_checker: Callable[[str], float] | None = None,
    exit_price_checker: Callable[[str], float] | None = None,
) -> AutoTradeCycleReport:
    config = config or AutoTradeConfig()
    if config.max_quantity <= 0:
        raise ValueError("max_quantity must be positive")
    if config.max_tick_age_seconds <= 0 or config.max_bar_age_seconds <= 0:
        raise ValueError("freshness limits must be positive")
    if config.cycle_deadline_seconds is not None and config.cycle_deadline_seconds <= 0:
        raise ValueError("cycle_deadline_seconds must be positive")
    if config.max_ai_response_age_seconds is not None and config.max_ai_response_age_seconds <= 0:
        raise ValueError("max_ai_response_age_seconds must be positive")
    if config.max_entry_deviation_pct < 0:
        raise ValueError("max_entry_deviation_pct must not be negative")
    if config.max_exit_requotes < 0:
        raise ValueError("max_exit_requotes must not be negative")

    if clock is None:
        # ``current_time`` is a long-standing replay/test hook. Preserve its
        # fixed-time behavior unless the caller opts into the per-read clock.
        now_provider: Callable[[], datetime] = (
            (lambda: current_time) if current_time is not None else kst_now
        )
    else:
        now_provider = clock
    cycle_started_at = as_kst(current_time) if current_time is not None else _clock_now(now_provider)
    cycle_deadline = (
        None
        if config.cycle_deadline_seconds is None
        else cycle_started_at + timedelta(seconds=config.cycle_deadline_seconds)
    )
    results: list[AutoTradeSymbolResult] = []
    open_position_symbols = [str(position["symbol"]) for position in database.list_open_live_positions()]
    symbols_to_process = list(dict.fromkeys([*symbols, *open_position_symbols]))
    for symbol in symbols_to_process:
        try:
            now = _clock_now(now_provider)
            if _deadline_reached(now, cycle_deadline):
                results.append(_blocked(symbol, "cycle_deadline_exceeded"))
                continue
            if config.enforce_market_hours and not is_regular_market_open(now):
                results.append(_blocked(symbol, "market_closed"))
                continue
            if _emergency_stop(database):
                results.append(_blocked(symbol, "emergency_stop"))
                continue
            freshness_reason = _freshness_reason(database, symbol, now, config)
            if freshness_reason is not None:
                results.append(_blocked(symbol, freshness_reason))
                continue
            exit_result = _maybe_exit_position(
                symbol, database, submitter, now, notifier,
                config=config,
                exit_price_checker=exit_price_checker,
            )
            if exit_result is not None:
                results.append(exit_result)
                continue
            if database.list_open_live_positions(symbol):
                results.append(_blocked(symbol, "existing_open_position"))
                continue
            if runtime_control.paused:
                results.append(_blocked(symbol, "runtime_paused"))
                continue
            if config.require_confirmation and not confirm_auto_trade:
                results.append(_blocked(symbol, "confirmation_required"))
                continue
            if not is_new_entry_window(now):
                results.append(_blocked(symbol, "new_entry_window_closed"))
                continue
            if _deadline_reached(_clock_now(now_provider), cycle_deadline):
                results.append(_blocked(symbol, "cycle_deadline_exceeded"))
                continue
            results.append(
                _maybe_enter_position(
                    symbol, database, ai_client, submitter, config, now, notifier, portfolio,
                    entry_budget_checker, clock=now_provider, cycle_deadline=cycle_deadline,
                    post_ai_price_checker=post_ai_price_checker,
                )
            )
        except Exception as exc:
            results.append(_blocked(symbol, f"symbol_error:{type(exc).__name__}"))
            _notify(notifier, f"{symbol} cycle error: {type(exc).__name__}")
    return AutoTradeCycleReport(tuple(results))


def _maybe_exit_position(
    symbol: str,
    database: Database,
    submitter: OrderSubmitter,
    now: datetime,
    notifier: Notifier | None,
    *,
    config: AutoTradeConfig,
    exit_price_checker: Callable[[str], float] | None,
) -> AutoTradeSymbolResult | None:
    positions = database.list_open_live_positions(symbol)
    if not positions:
        return None
    latest_tick = database.latest_tick(symbol)
    latest_bar = database.latest_bar(symbol)
    current_price = latest_tick.price if latest_tick else latest_bar.close if latest_bar else None
    if current_price is None:
        return _blocked(symbol, "missing_price_for_exit")
    position = positions[0]
    reason = None
    opened_at = as_kst(datetime.fromisoformat(position["opened_at"]))
    if is_forced_exit_window(now):
        reason = "session_close"
    elif is_previous_trading_day_position(opened_at, now):
        reason = "stale_previous_day_position"
    elif current_price <= float(position["stop_loss_price"]):
        reason = "stop_loss"
    elif current_price >= float(position["take_profit_price"]):
        reason = "take_profit"
    elif position["max_holding_seconds"] is not None:
        if (now - opened_at).total_seconds() >= int(position["max_holding_seconds"]):
            reason = "time_stop"
    if reason is None:
        return _blocked(symbol, "position_hold")
    if _has_unresolved_order(database, symbol, "SELL"):
        return _blocked(symbol, "exit_order_pending")
    if _exit_attempt_count(database, position) > config.max_exit_requotes:
        database.set_runtime_metadata("operator_review", "true", updated_at=now)
        database.set_runtime_metadata("block_new_entries", "true", updated_at=now)
        _notify(notifier, f"SELL requires operator review {symbol}: max requotes reached")
        return _blocked(symbol, "max_exit_requotes_reached")
    if exit_price_checker is not None:
        try:
            current_price = float(exit_price_checker(symbol))
        except Exception:
            return _blocked(symbol, "exit_price_check_failed")
        if not math.isfinite(current_price) or current_price <= 0:
            return _blocked(symbol, "exit_price_invalid")
    try:
        normalized_price = ExitPolicy().plan_sell_limit(
            ExitQuote(bid=current_price, ask=None, current_price=current_price)
        ).price
    except ExitPolicyError:
        return _blocked(symbol, "exit_price_policy_blocked")
    request = KisOrderRequest(
        symbol=symbol,
        side=KisOrderSide.SELL,
        quantity=int(position["quantity"]),
        price=normalized_price,
    )
    result = _submit_with_ledger(
        database=database,
        submitter=submitter,
        request=request,
        signal_id=str(position["signal_id"]),
        idempotency_key=_exit_signal_id(position, reason, now),
        reason=reason,
        created_at=now,
    )
    if result.broker_order_id is not None:
        _notify(notifier, f"SELL acknowledged {symbol} qty={position['quantity']} reason={reason}")
    return AutoTradeSymbolResult(
        symbol, "SELL", result.submitted, not result.submitted, result.reason,
        int(position["quantity"]), result.broker_order_id,
    )


def _latest_contiguous_session_bars(bars: list[MinuteBar]) -> list[MinuteBar]:
    if not bars:
        return []
    latest_day = as_kst(bars[-1].start).date()
    contiguous = [bars[-1]]
    expected = as_kst(bars[-1].start) - timedelta(minutes=1)
    for bar in reversed(bars[:-1]):
        start = as_kst(bar.start)
        if start.date() != latest_day or start != expected:
            break
        contiguous.append(bar)
        expected -= timedelta(minutes=1)
    contiguous.reverse()
    return contiguous


def _maybe_enter_position(
    symbol: str,
    database: Database,
    ai_client: TradingAIClient,
    submitter: OrderSubmitter,
    config: AutoTradeConfig,
    now: datetime,
    notifier: Notifier | None,
    portfolio: PortfolioState | None,
    entry_budget_checker: Callable[[str, float, int], bool] | None,
    *,
    clock: Callable[[], datetime],
    cycle_deadline: datetime | None,
    post_ai_price_checker: Callable[[str], float] | None,
) -> AutoTradeSymbolResult:
    bars = _latest_contiguous_session_bars(database.load_bars(symbol, limit=120))
    snapshot = build_feature_snapshot(bars)
    if snapshot is None:
        return _blocked(symbol, "missing_bars")
    signal_id = _entry_signal_id(symbol, bars[-1].start)
    request_id = _approval_request_id(symbol, bars[-1].start)
    approval = database.get_approval_request(request_id)
    if approval is not None:
        if str(approval["status"]) == "PENDING":
            database.expire_pending_approvals(now=now)
            approval = database.get_approval_request(request_id)
        if approval is not None and str(approval["status"]) == "PENDING":
            return _blocked(symbol, "operator_approval_required")
        if approval is not None and str(approval["status"]) == "APPROVED":
            if _deadline_reached(_clock_now(clock), cycle_deadline):
                return _blocked(symbol, "cycle_deadline_exceeded")
            return _execute_approved_entry(
                symbol, signal_id, approval, snapshot, database, submitter, config, now,
                notifier, portfolio, entry_budget_checker, post_ai_price_checker,
            )
        if approval is not None:
            return _blocked(symbol, f"approval_{str(approval['status']).lower()}")

    if database.get_broker_order(f"ai:buy:{signal_id}") is not None:
        return _blocked(symbol, "order_already_claimed")

    candidates = scan_candidates(snapshot)
    if not candidates and _is_network_decision_client(ai_client):
        return _blocked(symbol, "no_deterministic_candidate")
    pre_ai_now = _clock_now(clock)
    if _deadline_reached(pre_ai_now, cycle_deadline):
        return _blocked(symbol, "cycle_deadline_exceeded")
    pre_ai_freshness = _freshness_reason(database, symbol, pre_ai_now, config)
    if pre_ai_freshness is not None:
        return _blocked(symbol, pre_ai_freshness)
    pre_ai_tick = database.latest_tick(symbol)
    if pre_ai_tick is None:
        return _blocked(symbol, "stale_tick")
    decision = ai_client.decide(
        context_from_snapshot(snapshot, [candidate.__dict__ for candidate in candidates])
    )
    response_now = _clock_now(clock)
    _record_decision(database, decision, response_now)
    if _deadline_reached(response_now, cycle_deadline):
        return _blocked(symbol, "cycle_deadline_exceeded")
    response_guard_reason = _ai_response_guard_reason(
        database, symbol, pre_ai_tick, response_now, decision, config,
    )
    if response_guard_reason is not None:
        return _blocked(symbol, response_guard_reason)
    if decision.symbol != symbol:
        return _blocked(symbol, "ai_symbol_mismatch")
    if decision.action is not AIDecisionAction.BUY:
        return _blocked(symbol, f"ai_{decision.action.value.lower()}")
    if decision.strategy is None and _is_network_decision_client(ai_client):
        return _blocked(symbol, "ai_strategy_required")
    if decision.strategy is not None and decision.strategy not in _candidate_strategies(candidates):
        return _blocked(symbol, "ai_strategy_not_in_candidates")
    assert decision.entry_price is not None
    assert decision.stop_loss_price is not None
    assert decision.take_profit_price is not None
    entry_price = normalize_krx_limit_price(decision.entry_price, "buy")
    post_ai_price_reason = _post_ai_live_price_guard_reason(
        post_ai_price_checker,
        symbol=symbol,
        market_snapshot_price=snapshot.latest_close,
        entry_price=entry_price,
        max_deviation_pct=config.max_entry_deviation_pct,
    )
    if post_ai_price_reason is not None:
        return _blocked(symbol, post_ai_price_reason)
    try:
        snapshot_deviation = abs(entry_price - float(snapshot.latest_close)) / float(snapshot.latest_close) * 100
    except (TypeError, ValueError, OverflowError, ZeroDivisionError):
        return _blocked(symbol, "market_snapshot_invalid")
    if not math.isfinite(snapshot_deviation) or snapshot_deviation > config.max_entry_deviation_pct:
        return _blocked(symbol, "entry_deviation_too_large")
    if decision.confidence < config.min_confidence:
        return _blocked(symbol, "confidence_below_minimum")
    if decision.high_risk:
        stop_loss_price = normalize_krx_limit_price(decision.stop_loss_price, "buy")
        take_profit_price = normalize_krx_limit_price(decision.take_profit_price, "sell")
        try:
            validate_risk_reward(entry_price, take_profit_price, stop_loss_price)
        except ValueError:
            return _blocked(symbol, "invalid_risk_reward")
        quantity = _risk_quantity(
            database, config, portfolio, symbol, signal_id, decision.confidence, entry_price,
            stop_loss_price,
        )
        if quantity is None:
            return _blocked(symbol, "risk_gate_rejected")
        created = database.record_approval_request(
            request_id=request_id,
            symbol=symbol,
            decision_id=decision.decision_id,
            reason="high_risk_ai_decision",
            signal_id=signal_id,
            quantity=quantity,
            entry_price=entry_price,
            stop_loss_price=stop_loss_price,
            take_profit_price=take_profit_price,
            max_holding_seconds=decision.max_holding_seconds,
            created_at=now,
        )
        if created:
            _notify_approval(
                notifier, request_id,
                f"Approval required {symbol}: request={request_id} "
                f"entry={decision.entry_price:g} stop={decision.stop_loss_price:g} "
                f"take={decision.take_profit_price:g} confidence={decision.confidence:.2f} "
                f"risk={decision.risk_level.value}",
            )
        return _blocked(symbol, "operator_approval_required")
    stop_loss_price = normalize_krx_limit_price(decision.stop_loss_price, "buy")
    take_profit_price = normalize_krx_limit_price(decision.take_profit_price, "sell")
    try:
        validate_risk_reward(entry_price, take_profit_price, stop_loss_price)
    except ValueError:
        return _blocked(symbol, "invalid_risk_reward")
    risk_decision = evaluate_order_intent(
        config.risk,
        portfolio if portfolio is not None else PortfolioState(open_positions=database.paper_positions()),
        OrderIntent(
            symbol=symbol,
            strategy="AI_INTRADAY",
            signal_id=signal_id,
            entry_price=entry_price,
            stop_loss=stop_loss_price,
            confidence=decision.confidence,
        ),
    )
    if not risk_decision.approved:
        return _blocked(symbol, risk_decision.reason)
    quantity = min(config.max_quantity, risk_decision.quantity)
    if quantity <= 0:
        return _blocked(symbol, "quantity_zero")
    if not _entry_budget_ok(entry_budget_checker, symbol, entry_price, quantity):
        return _blocked(symbol, "entry_budget_unavailable_or_insufficient")
    if _deadline_reached(_clock_now(clock), cycle_deadline):
        return _blocked(symbol, "cycle_deadline_exceeded")
    request = KisOrderRequest(symbol=symbol, side=KisOrderSide.BUY, quantity=quantity, price=entry_price)
    result = _submit_with_ledger(
        database=database,
        submitter=submitter,
        request=request,
        signal_id=decision.decision_id,
        idempotency_key=signal_id,
        reason="ai_approved",
        created_at=now,
    )
    if result.broker_order_id is not None:
        _notify(notifier, f"BUY acknowledged {symbol} qty={quantity} entry={entry_price:g}")
    return AutoTradeSymbolResult(
        symbol, "BUY", result.submitted, not result.submitted, result.reason, quantity,
        result.broker_order_id,
    )


@dataclass(frozen=True)
class _SubmissionResult:
    submitted: bool
    reason: str
    broker_order_id: str | None = None


def _approval_request_id(symbol: str, bar_start: datetime) -> str:
    """Keep approval deduplication bounded to one symbol and one completed minute."""
    return f"approval:{symbol}:{as_kst(bar_start).strftime('%Y%m%d%H%M')}"


def _risk_quantity(
    database: Database,
    config: AutoTradeConfig,
    portfolio: PortfolioState | None,
    symbol: str,
    signal_id: str,
    confidence: float,
    entry_price: float,
    stop_loss_price: float,
) -> int | None:
    decision = evaluate_order_intent(
        config.risk,
        portfolio if portfolio is not None else PortfolioState(open_positions=database.paper_positions()),
        OrderIntent(
            symbol=symbol, strategy="AI_INTRADAY", signal_id=signal_id,
            entry_price=entry_price, stop_loss=stop_loss_price, confidence=confidence,
        ),
    )
    if not decision.approved:
        return None
    quantity = min(config.max_quantity, decision.quantity)
    return quantity if quantity > 0 else None


def _entry_budget_ok(
    checker: Callable[[str, float, int], bool] | None,
    symbol: str,
    price: float,
    quantity: int,
) -> bool:
    if checker is None:
        return True
    try:
        return bool(checker(symbol, price, quantity))
    except Exception:
        return False


def _post_ai_live_price_guard_reason(
    checker: Callable[[str], float] | None,
    *,
    symbol: str,
    market_snapshot_price: Any,
    entry_price: Any,
    max_deviation_pct: float,
) -> str | None:
    if checker is None:
        return None
    try:
        fresh_price = checker(symbol)
    except Exception:
        return "post_ai_price_check_failed"
    if isinstance(fresh_price, bool):
        return "post_ai_price_invalid"
    try:
        fresh_price = float(fresh_price)
    except (TypeError, ValueError, OverflowError):
        return "post_ai_price_invalid"
    if not math.isfinite(fresh_price) or fresh_price <= 0:
        return "post_ai_price_invalid"
    try:
        snapshot_price = float(market_snapshot_price)
        proposed_entry = float(entry_price)
    except (TypeError, ValueError, OverflowError):
        return "market_snapshot_invalid"
    if (
        not math.isfinite(snapshot_price)
        or snapshot_price <= 0
        or not math.isfinite(proposed_entry)
        or proposed_entry <= 0
    ):
        return "market_snapshot_invalid"
    snapshot_deviation = abs(fresh_price - snapshot_price) / snapshot_price * 100
    if snapshot_deviation > max_deviation_pct:
        return "post_ai_price_deviation_too_large"
    entry_deviation = abs(proposed_entry - fresh_price) / fresh_price * 100
    if entry_deviation > max_deviation_pct:
        return "entry_deviation_from_live_too_large"
    return None


def _execute_approved_entry(
    symbol: str,
    signal_id: str,
    approval: Any,
    snapshot: Any,
    database: Database,
    submitter: OrderSubmitter,
    config: AutoTradeConfig,
    now: datetime,
    notifier: Notifier | None,
    portfolio: PortfolioState | None,
    entry_budget_checker: Callable[[str, float, int], bool] | None,
    post_ai_price_checker: Callable[[str], float] | None,
) -> AutoTradeSymbolResult:
    request_id = str(approval["request_id"])
    if str(approval["symbol"]) != symbol or str(approval["signal_id"] or "") != signal_id:
        database.finish_approval_request(request_id, success=False, now=now)
        return _blocked(symbol, "approval_signal_mismatch")
    decision_id = str(approval["decision_id"] or "")
    audit = database.get_ai_decision_audit(decision_id)
    if audit is None or str(audit["symbol"]) != symbol or str(audit["action"]) != "BUY":
        database.finish_approval_request(request_id, success=False, now=now)
        return _blocked(symbol, "approval_audit_unavailable")
    try:
        entry_price = float(approval["entry_price"])
        stop_loss_price = float(approval["stop_loss_price"])
        take_profit_price = float(approval["take_profit_price"])
        quantity = int(approval["quantity"])
    except (TypeError, ValueError):
        database.finish_approval_request(request_id, success=False, now=now)
        return _blocked(symbol, "approval_terms_unavailable")
    if any(audit[key] is None for key in ("entry_price", "stop_loss_price", "take_profit_price")):
        database.finish_approval_request(request_id, success=False, now=now)
        return _blocked(symbol, "approval_terms_unavailable")
    if not all(
        (
            entry_price == normalize_krx_limit_price(float(audit["entry_price"]), "buy"),
            stop_loss_price == normalize_krx_limit_price(float(audit["stop_loss_price"]), "buy"),
            take_profit_price == normalize_krx_limit_price(float(audit["take_profit_price"]), "sell"),
        )
    ):
        database.finish_approval_request(request_id, success=False, now=now)
        return _blocked(symbol, "approval_terms_changed")
    live_price_reason = _post_ai_live_price_guard_reason(
        post_ai_price_checker,
        symbol=symbol,
        market_snapshot_price=snapshot.latest_close,
        entry_price=entry_price,
        max_deviation_pct=config.max_entry_deviation_pct,
    )
    if live_price_reason is not None:
        database.finish_approval_request(request_id, success=False, now=now)
        return _blocked(symbol, live_price_reason)
    try:
        validate_risk_reward(entry_price, take_profit_price, stop_loss_price)
    except ValueError:
        database.finish_approval_request(request_id, success=False, now=now)
        return _blocked(symbol, "invalid_risk_reward")
    if abs(entry_price - snapshot.latest_close) / snapshot.latest_close * 100 > config.max_entry_deviation_pct:
        database.finish_approval_request(request_id, success=False, now=now)
        return _blocked(symbol, "entry_deviation_too_large")
    expected_quantity = _risk_quantity(
        database, config, portfolio, symbol, signal_id, float(audit["confidence"]),
        entry_price, stop_loss_price,
    )
    if expected_quantity != quantity:
        database.finish_approval_request(request_id, success=False, now=now)
        return _blocked(symbol, "approval_quantity_changed")
    if not _entry_budget_ok(entry_budget_checker, symbol, entry_price, quantity):
        database.finish_approval_request(request_id, success=False, now=now)
        return _blocked(symbol, "entry_budget_unavailable_or_insufficient")
    if not database.consume_approval_request(request_id, now=now):
        return _blocked(symbol, "approval_not_available")
    result = _submit_with_ledger(
        database=database,
        submitter=submitter,
        request=KisOrderRequest(
            symbol=symbol, side=KisOrderSide.BUY, quantity=quantity, price=entry_price,
        ),
        signal_id=decision_id,
        idempotency_key=signal_id,
        reason="approved_ai",
        created_at=now,
    )
    database.finish_approval_request(request_id, success=result.submitted, now=now)
    if result.broker_order_id is not None:
        _notify(notifier, f"BUY acknowledged {symbol} qty={quantity} entry={entry_price:g}")
    return AutoTradeSymbolResult(
        symbol, "BUY", result.submitted, not result.submitted, result.reason, quantity,
        result.broker_order_id,
    )


def _submit_with_ledger(
    *,
    database: Database,
    submitter: OrderSubmitter,
    request: KisOrderRequest,
    signal_id: str,
    idempotency_key: str | None = None,
    reason: str,
    created_at: datetime,
) -> _SubmissionResult:
    side = KisOrderSide(request.side).name
    client_order_id = f"ai:{side.lower()}:{idempotency_key or signal_id}"
    claimed = database.claim_order_intent(
        client_order_id=client_order_id,
        signal_id=signal_id,
        symbol=request.symbol,
        side=side,
        requested_qty=request.quantity,
        requested_price=request.price,
        created_at=created_at,
    )
    if not claimed:
        return _SubmissionResult(False, "order_already_claimed")
    database.mark_order_submitting(client_order_id, updated_at=created_at)
    try:
        broker = submitter.submit_order(request)
    except Exception as exc:
        error = f"{type(exc).__name__}: {exc}"[:500]
        database.mark_order_unknown(client_order_id, error, updated_at=created_at)
        _record_execution_audit(
            database, request, signal_id, reason, "UNKNOWN", None, created_at,
            client_order_id=client_order_id,
        )
        return _SubmissionResult(False, "order_unknown")
    if not broker.broker_order_id:
        database.mark_order_unknown(client_order_id, "missing broker_order_id", updated_at=created_at)
        _record_execution_audit(
            database, request, signal_id, reason, "UNKNOWN", None, created_at,
            client_order_id=client_order_id,
        )
        return _SubmissionResult(False, "order_unknown")
    database.record_order_submission(client_order_id, broker.broker_order_id, submitted_at=created_at)
    _record_execution_audit(
        database, request, signal_id, reason, "ACKNOWLEDGED", broker.broker_order_id, created_at,
        client_order_id=client_order_id,
    )
    return _SubmissionResult(True, reason, broker.broker_order_id)


def _record_execution_audit(
    database: Database,
    request: KisOrderRequest,
    signal_id: str,
    reason: str,
    status: str,
    broker_order_id: str | None,
    created_at: datetime,
    *,
    client_order_id: str | None = None,
) -> None:
    client_order_id = client_order_id or f"ai:{KisOrderSide(request.side).name.lower()}:{signal_id}"
    database.record_broker_order_audit(
        audit_id=f"broker-audit:{client_order_id}:{status}",
        signal_id=signal_id,
        symbol=request.symbol,
        side=KisOrderSide(request.side).name,
        quantity=request.quantity,
        price=request.price,
        status=status,
        reason=reason,
        broker_order_id=broker_order_id,
        created_at=created_at,
    )


def _freshness_reason(
    database: Database,
    symbol: str,
    now: datetime,
    config: AutoTradeConfig,
) -> str | None:
    tick = database.latest_tick(symbol)
    bar = database.latest_bar(symbol)
    if tick is None:
        return "stale_tick"
    if bar is None:
        return "stale_bar"
    tick_age = (as_kst(now) - as_kst(tick.timestamp)).total_seconds()
    bar_start = as_kst(bar.start)
    bar_age = (as_kst(now) - (bar_start + timedelta(minutes=1))).total_seconds()
    if tick_age < 0 or tick_age > config.max_tick_age_seconds:
        return "stale_tick"
    if as_kst(now) < bar_start or bar_age > config.max_bar_age_seconds:
        return "stale_bar"
    return None


def _clock_now(clock: Callable[[], datetime]) -> datetime:
    return as_kst(clock())


def _is_network_decision_client(client: TradingAIClient) -> bool:
    return type(client).__name__ == "Open" + "AI" + "TradingDecisionClient"


def _deadline_reached(now: datetime, deadline: datetime | None) -> bool:
    return deadline is not None and as_kst(now) >= as_kst(deadline)


def _ai_response_guard_reason(
    database: Database,
    symbol: str,
    pre_ai_tick: Any,
    response_now: datetime,
    decision: TradingAIDecision,
    config: AutoTradeConfig,
) -> str | None:
    freshness_reason = _freshness_reason(database, symbol, response_now, config)
    if freshness_reason is not None:
        return f"{freshness_reason}_after_ai"
    post_ai_tick = database.latest_tick(symbol)
    if post_ai_tick is None:
        return "stale_tick_after_ai"
    if pre_ai_tick.price <= 0:
        return "price_snapshot_invalid"
    price_drift = abs(post_ai_tick.price - pre_ai_tick.price) / pre_ai_tick.price * 100
    if price_drift > config.max_entry_deviation_pct:
        return "price_moved_during_ai"

    response_age = (
        as_kst(response_now) - as_kst(decision.generated_at)
    ).total_seconds()
    if response_age < 0:
        return "ai_response_from_future"
    if (
        config.max_ai_response_age_seconds is not None
        and response_age > config.max_ai_response_age_seconds
    ):
        return "ai_response_too_old"
    return None


def _entry_signal_id(symbol: str, bar_start: datetime) -> str:
    return f"entry:{symbol}:{as_kst(bar_start).isoformat(timespec='minutes')}"


def _exit_signal_id(position: Any, reason: str, now: datetime) -> str:
    current = as_kst(now)
    marker = current.replace(second=(current.second // 10) * 10, microsecond=0).isoformat()
    return f"exit:{position['position_id']}:{reason}:{marker}"


def _exit_attempt_count(database: Database, position: Any) -> int:
    row = database.connection.execute(
        """SELECT COUNT(*) FROM broker_orders
           WHERE side='SELL' AND signal_id=? AND symbol=?""",
        (str(position["signal_id"]), str(position["symbol"])),
    ).fetchone()
    return int(row[0]) if row is not None else 0


def _has_unresolved_order(database: Database, symbol: str, side: str) -> bool:
    row = database.connection.execute(
        """SELECT 1 FROM broker_orders
           WHERE symbol=? AND side=?
             AND status IN ('INTENT','SUBMITTING','ACKNOWLEDGED','PARTIALLY_FILLED',
                            'CANCEL_PENDING','UNKNOWN')
           LIMIT 1""",
        (symbol, side),
    ).fetchone()
    return row is not None


def _emergency_stop(database: Database) -> bool:
    value = database.get_runtime_metadata("emergency_stop")
    return value is not None and value.strip().lower() in {"1", "true", "yes", "on"}


def _record_decision(database: Database, decision: TradingAIDecision, now: datetime) -> None:
    database.record_ai_decision(
        decision_id=decision.decision_id,
        symbol=decision.symbol,
        action=decision.action.value,
        confidence=decision.confidence,
        entry_price=decision.entry_price,
        take_profit_price=decision.take_profit_price,
        stop_loss_price=decision.stop_loss_price,
        risk_level=decision.risk_level.value,
        requires_operator_approval=decision.requires_operator_approval,
        rationale=decision.rationale,
        max_holding_seconds=decision.max_holding_seconds,
        strategy=decision.strategy,
        model=decision.model,
        prompt_version=decision.prompt_version,
        created_at=now,
    )


def _candidate_strategies(candidates: list[Any]) -> set[str]:
    return {
        str(getattr(candidate, "strategy", "") or "").strip()
        for candidate in candidates
        if str(getattr(candidate, "strategy", "") or "").strip()
    }


def _blocked(symbol: str, reason: str) -> AutoTradeSymbolResult:
    return AutoTradeSymbolResult(symbol, "BLOCKED", False, True, reason)


def _notify(notifier: Notifier | None, text: str) -> None:
    if notifier is not None:
        try:
            notifier.send(text)
        except Exception:
            # Notifications are observability only; the order ledger remains authoritative.
            return


def _notify_approval(notifier: Notifier | None, request_id: str, text: str) -> None:
    if notifier is None:
        return
    try:
        send_approval = getattr(notifier, "send_approval", None)
        if callable(send_approval):
            send_approval(request_id, text)
        else:
            notifier.send(text)
    except Exception:
        return
