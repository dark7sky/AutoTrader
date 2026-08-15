"""Bounded, read-only shadow evaluation of the local trading pipeline."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime

from kis_ai_scalper.execution import Command, OrderState, build_signal_id, transition
from kis_ai_scalper.market.clock import kst_now
from kis_ai_scalper.market.features import build_feature_snapshot
from kis_ai_scalper.market.health import evaluate_market_health
from kis_ai_scalper.market.tick import MarketTick, MinuteBar
from kis_ai_scalper.risk import OrderIntent, PortfolioState, RiskConfig, evaluate_order_intent
from kis_ai_scalper.storage.database import Database
from kis_ai_scalper.strategies.candidate import CandidateSignal, scan_candidates


@dataclass(frozen=True)
class ShadowCycleConfig:
    risk: RiskConfig = field(default_factory=RiskConfig)
    lookback: int = 20
    max_tick_age_seconds: float = 5.0
    max_bar_age_seconds: float = 90.0
    websocket_acknowledged: bool = False


@dataclass(frozen=True)
class ShadowCycleReport:
    symbol: str
    bars_count: int
    health_status: str
    trading_blocked: bool
    safe_mode: bool
    candidates_count: int
    selected_strategy: str | None
    risk_approved: bool
    risk_reason: str
    risk_quantity: int
    lifecycle_final_state: str
    signal_id: str | None = None
    entry_price: float | None = None


def _select_candidate(candidates: list[CandidateSignal]) -> CandidateSignal:
    return max(candidates, key=lambda candidate: (candidate.score, candidate.strategy))


def _clock_for_data(current_time: datetime | None, latest: MinuteBar | MarketTick | None) -> datetime:
    if current_time is not None:
        return current_time
    timestamp = latest.start if isinstance(latest, MinuteBar) else latest.timestamp if latest else None
    if timestamp is not None and timestamp.tzinfo is not None:
        timezone = timestamp.tzinfo
        return datetime.now(timezone)
    return kst_now()


def run_shadow_cycle(
    symbol: str,
    *,
    bars: list[MinuteBar] | None = None,
    ticks: list[MarketTick] | None = None,
    database: Database | None = None,
    current_time: datetime | None = None,
    config: ShadowCycleConfig | None = None,
    portfolio: PortfolioState | None = None,
) -> ShadowCycleReport:
    """Evaluate one symbol using only supplied or persisted market data.

    This function has no broker, account, AI, or database-write side effects.
    ``current_time`` is injectable so replay tests can remain deterministic.
    """
    config = config or ShadowCycleConfig()
    latest_bar: MinuteBar | None = None
    latest_tick: MarketTick | None = None
    if database is not None:
        bars = database.load_bars(symbol)
        latest_tick = database.latest_tick(symbol)
        latest_bar = database.latest_bar(symbol)
    bars = sorted((bar for bar in (bars or []) if bar.symbol == symbol), key=lambda bar: bar.start)
    if latest_bar is None:
        latest_bar = bars[-1] if bars else None
    if latest_tick is None:
        ticks = sorted(
            (tick for tick in (ticks or []) if tick.symbol == symbol),
            key=lambda tick: tick.timestamp,
        )
        latest_tick = ticks[-1] if ticks else None
    now = _clock_for_data(current_time, latest_tick or latest_bar)
    health = evaluate_market_health(
        now,
        websocket_acknowledged=config.websocket_acknowledged,
        latest_tick=latest_tick,
        latest_bar=latest_bar,
        max_tick_age_seconds=config.max_tick_age_seconds,
        max_bar_age_seconds=config.max_bar_age_seconds,
    )
    if health.trading_blocked:
        return ShadowCycleReport(
            symbol, len(bars), health.status.value, True, health.enter_safe_mode, 0, None,
            False, health.reason, 0, OrderState.SAFE_MODE.value,
        )

    snapshot = build_feature_snapshot(bars, lookback=config.lookback)
    candidates = scan_candidates(snapshot) if snapshot is not None else []
    if not candidates:
        return ShadowCycleReport(
            symbol, len(bars), health.status.value, False, False, 0, None,
            False, "no_candidate", 0, OrderState.FLAT.value,
        )

    selected = _select_candidate(candidates)
    entry_price = bars[-1].close
    stop_loss = entry_price * 0.99
    intent = OrderIntent(
        symbol=symbol,
        strategy=selected.strategy,
        signal_id=build_signal_id(selected.strategy, symbol, bars[-1].start),
        entry_price=entry_price,
        stop_loss=stop_loss,
        confidence=selected.score,
    )
    decision = evaluate_order_intent(config.risk, portfolio or PortfolioState(), intent)
    if not decision.approved:
        state = OrderState.FLAT
    else:
        state = OrderState.FLAT
        for command in (Command.WATCH, Command.ARM, Command.SUBMIT_ENTRY, Command.MARK_ENTRY_FILLED):
            state = transition(state, command)
    return ShadowCycleReport(
        symbol, len(bars), health.status.value, False, False, len(candidates), selected.strategy,
        decision.approved, decision.reason, decision.quantity, state.value,
        intent.signal_id, intent.entry_price,
    )
