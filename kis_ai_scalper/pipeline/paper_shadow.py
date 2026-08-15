"""Persistent local paper execution for approved shadow-cycle signals."""

from __future__ import annotations

from dataclasses import dataclass

from kis_ai_scalper.pipeline.shadow_cycle import ShadowCycleConfig, ShadowCycleReport, run_shadow_cycle
from kis_ai_scalper.risk import PortfolioState
from kis_ai_scalper.storage.database import Database


@dataclass(frozen=True)
class PaperShadowResult:
    shadow: ShadowCycleReport
    recorded: bool = False
    duplicate_skipped: bool = False
    order_id: str | None = None
    fill_id: str | None = None


def record_shadow_paper_buy(
    report: ShadowCycleReport, database: Database, *, current_time=None,
) -> PaperShadowResult:
    """Record a deterministic local BUY only when the shadow report is fully approved."""
    if (
        report.trading_blocked
        or not report.risk_approved
        or report.risk_quantity <= 0
        or not report.signal_id
        or report.entry_price is None
    ):
        return PaperShadowResult(report)

    order_id = f"paper-order:{report.signal_id}"
    fill_id = f"paper-fill:{report.signal_id}"
    recorded = database.record_paper_buy(
        order_id=order_id,
        fill_id=fill_id,
        signal_id=report.signal_id,
        symbol=report.symbol,
        quantity=report.risk_quantity,
        price=report.entry_price,
        created_at=current_time,
    )
    return PaperShadowResult(
        report, recorded=recorded, duplicate_skipped=not recorded,
        order_id=order_id, fill_id=fill_id,
    )


def run_paper_shadow_cycle(
    symbol: str, *, database: Database, config: ShadowCycleConfig | None = None,
    current_time=None,
) -> PaperShadowResult:
    portfolio = PortfolioState(open_positions=database.paper_positions())
    report = run_shadow_cycle(
        symbol,
        database=database,
        config=config,
        current_time=current_time,
        portfolio=portfolio,
    )
    return record_shadow_paper_buy(report, database, current_time=current_time)
