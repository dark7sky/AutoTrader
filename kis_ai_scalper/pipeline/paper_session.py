"""Bounded read-only market collection followed by local paper shadow cycles."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Awaitable, Callable

from kis_ai_scalper.market.collector import CollectorResult, collect_realtime_prices
from kis_ai_scalper.pipeline.paper_shadow import PaperShadowResult, run_paper_shadow_cycle
from kis_ai_scalper.pipeline.shadow_cycle import ShadowCycleConfig
from kis_ai_scalper.storage import connect_database


@dataclass(frozen=True)
class PaperSessionIteration:
    iteration: int
    subscribe_ack: bool
    ticks_saved: int
    bars_saved: int
    health_status: str
    risk_approved: bool
    risk_reason: str
    recorded: bool
    duplicate_skipped: bool
    blocked: bool
    exit_code: int


@dataclass(frozen=True)
class PaperSessionReport:
    iterations: tuple[PaperSessionIteration, ...]

    @property
    def exit_code(self) -> int:
        return 0 if any(item.recorded or item.duplicate_skipped for item in self.iterations) else 3


def _validate_bounds(iterations: int, collect_seconds: int, sleep_seconds: int) -> None:
    if not 1 <= iterations <= 100:
        raise ValueError("iterations must be between 1 and 100")
    if not 1 <= collect_seconds <= 3600:
        raise ValueError("collect_seconds must be between 1 and 3600")
    if not 0 <= sleep_seconds <= 3600:
        raise ValueError("sleep_seconds must be between 0 and 3600")


async def run_paper_session(
    endpoint: str,
    approval_key: str,
    symbol: str,
    db_path: str,
    iterations: int = 1,
    collect_seconds: int = 60,
    sleep_seconds: int = 0,
    max_tick_age_seconds: float = 5.0,
    max_bar_age_seconds: float = 90.0,
    *,
    collector: Callable[..., Awaitable[CollectorResult]] = collect_realtime_prices,
    shadow_runner: Callable[..., PaperShadowResult] = run_paper_shadow_cycle,
    sleeper: Callable[[float], Awaitable[None]] = asyncio.sleep,
) -> PaperSessionReport:
    """Run a bounded paper session using read-only KIS market data only."""
    _validate_bounds(iterations, collect_seconds, sleep_seconds)
    if max_tick_age_seconds <= 0 or max_bar_age_seconds <= 0:
        raise ValueError("market data ages must be positive")

    summaries: list[PaperSessionIteration] = []
    for iteration in range(1, iterations + 1):
        with connect_database(db_path) as database:
            database.init_schema()
            runtime = database.get_runtime_control()
        if runtime.paused:
            summaries.append(PaperSessionIteration(
                iteration=iteration,
                subscribe_ack=False,
                ticks_saved=0,
                bars_saved=0,
                health_status="PAUSED",
                risk_approved=False,
                risk_reason="runtime_paused",
                recorded=False,
                duplicate_skipped=False,
                blocked=True,
                exit_code=3,
            ))
            if iteration < iterations and sleep_seconds:
                await sleeper(sleep_seconds)
            continue
        collected = await collector(endpoint, approval_key, symbol, db_path, collect_seconds)
        with connect_database(db_path) as database:
            database.init_schema()
            result = shadow_runner(
                symbol,
                database=database,
                config=ShadowCycleConfig(
                    websocket_acknowledged=collected.subscribe_ack,
                    max_tick_age_seconds=max_tick_age_seconds,
                    max_bar_age_seconds=max_bar_age_seconds,
                ),
            )
        report = result.shadow
        eligible = (
            not report.trading_blocked
            and report.risk_approved
            and report.risk_quantity > 0
            and bool(report.signal_id)
            and report.entry_price is not None
        )
        recorded_or_duplicate = result.recorded or result.duplicate_skipped
        summaries.append(PaperSessionIteration(
            iteration=iteration,
            subscribe_ack=collected.subscribe_ack,
            ticks_saved=collected.ticks_saved,
            bars_saved=collected.bars_saved,
            health_status=report.health_status,
            risk_approved=report.risk_approved,
            risk_reason=report.risk_reason,
            recorded=result.recorded,
            duplicate_skipped=result.duplicate_skipped,
            blocked=not eligible,
            exit_code=0 if recorded_or_duplicate else 3,
        ))
        if iteration < iterations and sleep_seconds:
            await sleeper(sleep_seconds)
    return PaperSessionReport(tuple(summaries))
