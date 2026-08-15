"""End-to-end deterministic replay of the local trading safety pipeline."""

from __future__ import annotations

from dataclasses import dataclass, field

from kis_ai_scalper.execution import (
    Command,
    ManagedPosition,
    OrderState,
    PositionAction,
    SignalLedger,
    apply_position_decision,
    build_signal_id,
    evaluate_position,
    transition,
)
from kis_ai_scalper.market.features import build_feature_snapshot
from kis_ai_scalper.market.tick import MinuteBar
from kis_ai_scalper.risk import OrderIntent, PortfolioState, RiskConfig, evaluate_order_intent
from kis_ai_scalper.strategies.candidate import CandidateSignal, scan_candidates


@dataclass(frozen=True)
class DryRunConfig:
    """Local-only parameters for one deterministic replay."""

    risk: RiskConfig = field(default_factory=RiskConfig)
    lookback: int = 20


@dataclass(frozen=True)
class DryRunReport:
    symbol: str
    bars_count: int
    candidates_count: int
    selected_strategy: str | None
    risk_approved: bool
    risk_reason: str
    risk_quantity: int
    lifecycle_final_state: str
    position_action: str | None
    position_reason: str | None
    signal_id: str | None


def _empty_report(symbol: str, bars_count: int, *, reason: str) -> DryRunReport:
    return DryRunReport(
        symbol=symbol,
        bars_count=bars_count,
        candidates_count=0,
        selected_strategy=None,
        risk_approved=False,
        risk_reason=reason,
        risk_quantity=0,
        lifecycle_final_state=OrderState.FLAT.value,
        position_action=None,
        position_reason=None,
        signal_id=None,
    )


def _select_candidate(candidates: list[CandidateSignal]) -> CandidateSignal:
    return max(candidates, key=lambda candidate: (candidate.score, candidate.strategy))


def run_offline_dry_run(
    bars: list[MinuteBar], config: DryRunConfig | None = None
) -> DryRunReport:
    """Run analysis, risk approval, local lifecycle, and position evaluation.

    Price levels in this function are replay placeholders.  They are passed
    only to pure local modules and are never emitted to an external adapter.
    """

    config = config or DryRunConfig()
    if not bars:
        return _empty_report("unknown", 0, reason="no_bars")
    symbols = {bar.symbol for bar in bars}
    if len(symbols) != 1:
        raise ValueError("dry-run requires bars for exactly one symbol")
    symbol = bars[0].symbol
    snapshot = build_feature_snapshot(bars, lookback=config.lookback)
    candidates = scan_candidates(snapshot) if snapshot is not None else []
    if not candidates:
        return _empty_report(symbol, len(bars), reason="no_candidate")

    selected = _select_candidate(candidates)
    signal_id = build_signal_id(selected.strategy, symbol, bars[-1].start)
    SignalLedger().record(signal_id)
    entry_price = bars[-1].close
    stop_loss = entry_price * 0.99
    intent = OrderIntent(
        symbol=symbol,
        strategy=selected.strategy,
        signal_id=signal_id,
        entry_price=entry_price,
        stop_loss=stop_loss,
        confidence=selected.score,
    )
    decision = evaluate_order_intent(config.risk, PortfolioState(), intent)
    if not decision.approved:
        return DryRunReport(
            symbol=symbol,
            bars_count=len(bars),
            candidates_count=len(candidates),
            selected_strategy=selected.strategy,
            risk_approved=False,
            risk_reason=decision.reason,
            risk_quantity=decision.quantity,
            lifecycle_final_state=OrderState.FLAT.value,
            position_action=None,
            position_reason=None,
            signal_id=signal_id,
        )

    state = OrderState.FLAT
    for command in (
        Command.WATCH,
        Command.ARM,
        Command.SUBMIT_ENTRY,
        Command.MARK_ENTRY_FILLED,
    ):
        state = transition(state, command)

    opened_at = bars[-1].start
    position = ManagedPosition(
        symbol=symbol,
        quantity=decision.quantity,
        entry_price=entry_price,
        stop_loss=stop_loss,
        tp1_price=entry_price * 1.005,
        tp2_price=entry_price * 1.01,
        opened_at=opened_at,
    )
    position_decision = evaluate_position(position, entry_price, opened_at)
    # Applying HOLD records the local high-water mark without changing the
    # report semantics; this keeps the pure manager's update path exercised.
    apply_position_decision(position, position_decision)
    return DryRunReport(
        symbol=symbol,
        bars_count=len(bars),
        candidates_count=len(candidates),
        selected_strategy=selected.strategy,
        risk_approved=True,
        risk_reason=decision.reason,
        risk_quantity=decision.quantity,
        lifecycle_final_state=state.value,
        position_action=position_decision.action.value,
        position_reason=position_decision.reason,
        signal_id=signal_id,
    )
