"""One bounded multi-symbol AI auto-trading cycle."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Protocol
from uuid import uuid4

from kis_ai_scalper.ai.decision import (
    AIDecisionAction,
    TradingAIClient,
    TradingAIDecision,
    context_from_snapshot,
)
from kis_ai_scalper.broker.kis_order import KisOrderRequest, KisOrderResult, KisOrderSide
from kis_ai_scalper.market.features import build_feature_snapshot
from kis_ai_scalper.market.clock import kst_now
from kis_ai_scalper.market.schedule import (
    as_kst,
    is_previous_trading_day_position,
    is_regular_market_open,
)
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
) -> AutoTradeCycleReport:
    config = config or AutoTradeConfig()
    if config.max_quantity <= 0:
        raise ValueError("max_quantity must be positive")
    now = current_time or kst_now()
    results: list[AutoTradeSymbolResult] = []
    open_position_symbols = [str(position["symbol"]) for position in database.list_open_live_positions()]
    symbols_to_process = list(dict.fromkeys([*symbols, *open_position_symbols]))
    for symbol in symbols_to_process:
        if runtime_control.paused:
            results.append(_blocked(symbol, "runtime_paused"))
            continue
        if config.enforce_market_hours and not is_regular_market_open(now):
            results.append(_blocked(symbol, "market_closed"))
            continue
        if config.require_confirmation and not confirm_auto_trade:
            results.append(_blocked(symbol, "confirmation_required"))
            continue
        exit_result = _maybe_exit_position(symbol, database, submitter, now, notifier)
        if exit_result is not None:
            results.append(exit_result)
            continue
        if database.list_open_live_positions(symbol):
            results.append(_blocked(symbol, "existing_open_position"))
            continue
        results.append(_maybe_enter_position(symbol, database, ai_client, submitter, config, now, notifier))
    return AutoTradeCycleReport(tuple(results))


def _maybe_exit_position(
    symbol: str,
    database: Database,
    submitter: OrderSubmitter,
    now: datetime,
    notifier: Notifier | None,
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
    if is_previous_trading_day_position(opened_at, now):
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
    request = KisOrderRequest(
        symbol=symbol,
        side=KisOrderSide.SELL,
        quantity=int(position["quantity"]),
        price=current_price,
    )
    broker = submitter.submit_order(request)
    database.close_live_position(
        position_id=str(position["position_id"]),
        exit_broker_order_id=broker.broker_order_id,
        close_reason=reason,
        closed_at=now,
    )
    database.record_broker_order_audit(
        audit_id=f"broker-audit:{position['signal_id']}:exit:{uuid4().hex}",
        signal_id=str(position["signal_id"]),
        symbol=symbol,
        side="SELL",
        quantity=int(position["quantity"]),
        price=current_price,
        status="SUBMITTED",
        reason=reason,
        broker_order_id=broker.broker_order_id,
        created_at=now,
    )
    _notify(notifier, f"SELL submitted {symbol} qty={position['quantity']} reason={reason}")
    return AutoTradeSymbolResult(
        symbol, "SELL", True, False, reason, int(position["quantity"]), broker.broker_order_id,
    )


def _maybe_enter_position(
    symbol: str,
    database: Database,
    ai_client: TradingAIClient,
    submitter: OrderSubmitter,
    config: AutoTradeConfig,
    now: datetime,
    notifier: Notifier | None,
) -> AutoTradeSymbolResult:
    bars = database.load_bars(symbol)
    snapshot = build_feature_snapshot(bars)
    if snapshot is None:
        return _blocked(symbol, "missing_bars")
    candidates = scan_candidates(snapshot)
    decision = ai_client.decide(context_from_snapshot(snapshot, candidates))
    _record_decision(database, decision, now)
    if decision.action is not AIDecisionAction.BUY:
        return _blocked(symbol, f"ai_{decision.action.value.lower()}")
    if decision.confidence < config.min_confidence:
        return _blocked(symbol, "confidence_below_minimum")
    if decision.high_risk:
        request_id = f"approval:{decision.decision_id}"
        database.record_approval_request(
            request_id=request_id,
            symbol=symbol,
            decision_id=decision.decision_id,
            reason="high_risk_ai_decision",
            created_at=now,
        )
        _notify(
            notifier,
            f"Approval required {symbol}: confidence={decision.confidence:.2f} "
            f"risk={decision.risk_level.value}",
        )
        return _blocked(symbol, "operator_approval_required")
    assert decision.entry_price is not None
    assert decision.stop_loss_price is not None
    assert decision.take_profit_price is not None
    deviation = abs(decision.entry_price - snapshot.latest_close) / snapshot.latest_close * 100
    if deviation > config.max_entry_deviation_pct:
        return _blocked(symbol, "entry_deviation_too_large")
    risk_decision = evaluate_order_intent(
        config.risk,
        PortfolioState(open_positions=database.paper_positions()),
        OrderIntent(
            symbol=symbol,
            strategy="AI_INTRADAY",
            signal_id=decision.decision_id,
            entry_price=decision.entry_price,
            stop_loss=decision.stop_loss_price,
            confidence=decision.confidence,
        ),
    )
    if not risk_decision.approved:
        return _blocked(symbol, risk_decision.reason)
    quantity = min(config.max_quantity, risk_decision.quantity)
    if quantity <= 0:
        return _blocked(symbol, "quantity_zero")
    broker = submitter.submit_order(
        KisOrderRequest(symbol=symbol, side=KisOrderSide.BUY, quantity=quantity, price=decision.entry_price)
    )
    database.record_broker_order_audit(
        audit_id=f"broker-audit:{decision.decision_id}:entry:{uuid4().hex}",
        signal_id=decision.decision_id,
        symbol=symbol,
        side="BUY",
        quantity=quantity,
        price=decision.entry_price,
        status="SUBMITTED",
        reason="ai_approved",
        broker_order_id=broker.broker_order_id,
        created_at=now,
    )
    database.open_live_position(
        position_id=f"live-position:{decision.decision_id}",
        signal_id=decision.decision_id,
        symbol=symbol,
        quantity=quantity,
        entry_price=decision.entry_price,
        stop_loss_price=decision.stop_loss_price,
        take_profit_price=decision.take_profit_price,
        opened_at=now,
        entry_broker_order_id=broker.broker_order_id,
        max_holding_seconds=decision.max_holding_seconds,
    )
    _notify(notifier, f"BUY submitted {symbol} qty={quantity} entry={decision.entry_price:g}")
    return AutoTradeSymbolResult(symbol, "BUY", True, False, "ai_approved", quantity, broker.broker_order_id)


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
        created_at=now,
    )


def _blocked(symbol: str, reason: str) -> AutoTradeSymbolResult:
    return AutoTradeSymbolResult(symbol, "BLOCKED", False, True, reason)


def _notify(notifier: Notifier | None, text: str) -> None:
    if notifier is not None:
        notifier.send(text)
