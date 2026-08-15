"""Conservative bridge from an approved shadow signal to one broker order."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Protocol
from uuid import uuid4

from kis_ai_scalper.broker.kis_order import (
    KisOrderRequest,
    KisOrderResult,
    KisOrderSide,
    KisOrderType,
)
from kis_ai_scalper.pipeline.shadow_cycle import ShadowCycleReport
from kis_ai_scalper.storage.database import Database, RuntimeControl


class OrderSubmitter(Protocol):
    def submit_order(self, request: KisOrderRequest) -> KisOrderResult:
        ...


@dataclass(frozen=True)
class LiveOrderExecutionResult:
    shadow: ShadowCycleReport
    submitted: bool
    blocked: bool
    reason: str
    quantity: int = 0
    broker_order_id: str | None = None


def submit_shadow_live_buy(
    report: ShadowCycleReport,
    *,
    runtime_control: RuntimeControl,
    submitter: OrderSubmitter,
    database: Database | None = None,
    confirm_submit: bool = False,
    max_quantity: int = 1,
    current_time: datetime | None = None,
) -> LiveOrderExecutionResult:
    """Submit at most one BUY for a fully approved shadow report.

    This function is deliberately narrow: it supports entry BUY orders only,
    requires a live operator confirmation, honors the runtime pause gate, and
    caps quantity before creating a broker request.
    """
    if max_quantity <= 0:
        raise ValueError("max_quantity must be positive")
    if runtime_control.paused:
        return _blocked(report, "runtime_paused", database, current_time)
    if not confirm_submit:
        return _blocked(report, "confirmation_required", database, current_time)
    if report.trading_blocked:
        return _blocked(report, report.risk_reason or "market_health_blocked", database, current_time)
    if not report.risk_approved:
        return _blocked(report, report.risk_reason or "risk_rejected", database, current_time)
    if not report.signal_id:
        return _blocked(report, "missing_signal_id", database, current_time)
    if report.entry_price is None or report.entry_price <= 0:
        return _blocked(report, "missing_entry_price", database, current_time)
    if report.risk_quantity <= 0:
        return _blocked(report, "risk_quantity_zero", database, current_time)
    if database is not None and database.broker_signal_submitted(report.signal_id):
        return _blocked(report, "duplicate_signal", database, current_time)

    quantity = min(report.risk_quantity, max_quantity)
    request = KisOrderRequest(
        symbol=report.symbol,
        side=KisOrderSide.BUY,
        quantity=quantity,
        price=report.entry_price,
        order_type=KisOrderType.LIMIT,
    )
    broker_result = submitter.submit_order(request)
    if database is not None:
        database.record_broker_order_audit(
            audit_id=_audit_id(report.signal_id, "submitted"),
            signal_id=report.signal_id,
            symbol=report.symbol,
            side=KisOrderSide.BUY.name,
            quantity=quantity,
            price=report.entry_price,
            status="SUBMITTED",
            reason="approved",
            broker_order_id=broker_result.broker_order_id,
            created_at=current_time,
        )
    return LiveOrderExecutionResult(
        report,
        submitted=True,
        blocked=False,
        reason="approved",
        quantity=quantity,
        broker_order_id=broker_result.broker_order_id,
    )


def _blocked(
    report: ShadowCycleReport,
    reason: str,
    database: Database | None,
    current_time: datetime | None,
) -> LiveOrderExecutionResult:
    quantity = max(1, report.risk_quantity or 1)
    price = report.entry_price if report.entry_price and report.entry_price > 0 else 1.0
    if database is not None:
        database.record_broker_order_audit(
            audit_id=_audit_id(report.signal_id, f"blocked-{reason}"),
            signal_id=report.signal_id,
            symbol=report.symbol,
            side=KisOrderSide.BUY.name,
            quantity=quantity,
            price=price,
            status="BLOCKED",
            reason=reason,
            broker_order_id=None,
            created_at=current_time,
        )
    return LiveOrderExecutionResult(
        report, submitted=False, blocked=True, reason=reason,
    )


def _audit_id(signal_id: str | None, suffix: str) -> str:
    base = signal_id or "no-signal"
    return f"broker-audit:{base}:{suffix}:{uuid4().hex}"
