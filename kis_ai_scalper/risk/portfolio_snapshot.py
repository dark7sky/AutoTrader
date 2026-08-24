"""Build a fail-closed risk view from the broker account and order ledger."""

from __future__ import annotations

from collections import defaultdict, deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any

from kis_ai_scalper.broker.kis_account import KisAccountSnapshot
from kis_ai_scalper.broker.kis_buying_power import KisBuyingPowerSnapshot
from kis_ai_scalper.broker.kis_realized_pnl import KisRealizedPnlSnapshot
from kis_ai_scalper.ops.performance import build_performance_report, cost_bps_from_env
from kis_ai_scalper.risk.models import PortfolioState, PositionState
from kis_ai_scalper.storage.database import Database


KST = timezone(timedelta(hours=9), "KST")


@dataclass(frozen=True)
class PortfolioRiskSnapshot:
    """Portfolio state plus fields that prevent a trustworthy new entry."""

    portfolio: PortfolioState
    unknown_fields: frozenset[str] = field(default_factory=frozenset)
    broker_orders_today: int = 0
    broker_fills_today: int = 0
    orderable_cash: float | None = None

    @property
    def fail_closed(self) -> bool:
        return bool(self.unknown_fields)

    @property
    def can_enter(self) -> bool:
        return not self.fail_closed

    def entry_allowed(self) -> bool:
        return self.can_enter


def _today(value: datetime) -> str:
    return _as_aware_kst(value).date().isoformat()


def _as_aware_kst(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=KST)
    return value.astimezone(KST)


def _rows_for_day(database: Database, table: str, day: str) -> list[Any]:
    if table not in {"broker_orders", "broker_fills"}:
        raise ValueError("unsupported broker ledger table")
    column = "created_at" if table == "broker_orders" else "filled_at"
    rows = database.connection.execute(
        f"SELECT * FROM {table} ORDER BY {column}"
    ).fetchall()
    result: list[Any] = []
    for row in rows:
        try:
            row_day = _as_aware_kst(datetime.fromisoformat(str(row[column]))).date().isoformat()
        except (TypeError, ValueError):
            continue
        if row_day == day:
            result.append(row)
    return result


def _consecutive_losses(database: Database) -> int | None:
    rows = database.connection.execute(
        """SELECT f.*, COALESCE(a.strategy, 'UNKNOWN') AS strategy
           FROM broker_fills f
           JOIN broker_orders o ON o.client_order_id=f.client_order_id
           LEFT JOIN ai_decision_audits a ON a.decision_id=o.signal_id
           ORDER BY f.filled_at, f.fill_id"""
    ).fetchall()
    try:
        report = build_performance_report(rows)
    except ValueError:
        return None
    losses = 0
    for outcome in reversed(report.outcomes):
        if outcome < 0:
            losses += 1
        else:
            break
    return losses


def calculate_local_daily_realized_pnl(
    database: Database,
    *,
    now: datetime | None = None,
) -> float | None:
    """Return KST-day estimated net realized P/L from reconciled fills using FIFO.

    It fails closed unless the latest reconciliation completed without operator review.
    Historical fills are consumed to establish FIFO cost basis, while only
    sells on the current Asia/Seoul trading date contribute to the result.
    """
    timestamp = now or datetime.now(timezone.utc)
    timestamp = _as_aware_kst(timestamp)
    target_date = timestamp.date()
    metadata = database.connection.execute(
        """SELECT key, value, updated_at FROM runtime_metadata
           WHERE key IN ('operator_review', 'block_new_entries')"""
    ).fetchall()
    states = {str(row["key"]): row for row in metadata}
    reconciliation_times: list[datetime] = []
    for key in ("operator_review", "block_new_entries"):
        row = states.get(key)
        if row is None or str(row["value"]).lower() != "false":
            return None
        try:
            reconciled_at = datetime.fromisoformat(str(row["updated_at"]))
        except ValueError:
            return None
        reconciled_at = _as_aware_kst(reconciled_at)
        if reconciled_at.date() != target_date:
            return None
        if reconciled_at.astimezone(timezone.utc) > timestamp.astimezone(timezone.utc):
            return None
        reconciliation_times.append(reconciled_at.astimezone(timezone.utc))
    reconciled_through = min(reconciliation_times)

    rows = database.connection.execute(
        """SELECT f.*, COALESCE(a.strategy, 'UNKNOWN') AS strategy
           FROM broker_fills f
           JOIN broker_orders o ON o.client_order_id=f.client_order_id
           LEFT JOIN ai_decision_audits a ON a.decision_id=o.signal_id
           ORDER BY f.filled_at, f.fill_id"""
    ).fetchall()
    buy_cost_bps, sell_cost_bps = cost_bps_from_env()
    lots: dict[str, deque[tuple[int, float]]] = defaultdict(deque)
    realized = 0.0
    for row in rows:
        symbol = str(row["symbol"] or "")
        side = str(row["side"] or "").upper()
        try:
            quantity = int(row["quantity"])
            price = float(row["price"])
            filled_at = datetime.fromisoformat(str(row["filled_at"]))
        except (TypeError, ValueError):
            return None
        if not symbol or quantity <= 0 or price <= 0 or side not in {"BUY", "SELL"}:
            return None
        filled_at = _as_aware_kst(filled_at)
        if filled_at.astimezone(timezone.utc) > timestamp.astimezone(timezone.utc):
            return None
        if filled_at.astimezone(timezone.utc) > reconciled_through:
            return None
        if side == "BUY":
            lots[symbol].append((quantity, price))
            continue

        remaining = quantity
        sell_realized = 0.0
        while remaining:
            if not lots[symbol]:
                return None
            lot_quantity, lot_price = lots[symbol][0]
            matched = min(remaining, lot_quantity)
            buy_notional = lot_price * matched
            sell_notional = price * matched
            sell_realized += (
                sell_notional
                - buy_notional
                - buy_notional * buy_cost_bps / 10_000
                - sell_notional * sell_cost_bps / 10_000
            )
            remaining -= matched
            if matched == lot_quantity:
                lots[symbol].popleft()
            else:
                lots[symbol][0] = (lot_quantity - matched, lot_price)
        if filled_at.astimezone(KST).date() == target_date:
            realized += sell_realized
    return round(realized, 10)


def build_portfolio_risk_snapshot(
    account: KisAccountSnapshot,
    database: Database,
    *,
    now: datetime | None = None,
    buying_power: KisBuyingPowerSnapshot | None = None,
    realized_pnl: KisRealizedPnlSnapshot | float | None = None,
) -> PortfolioRiskSnapshot:
    """Convert a KIS snapshot and the persisted broker ledger into risk inputs."""
    timestamp = now or datetime.now(timezone.utc)
    unknown: set[str] = set()
    positions: list[PositionState] = []
    exposure = 0.0

    for position in account.positions:
        avg_price = position.avg_price
        if avg_price is None:
            unknown.add(f"position:{position.symbol}.average_price")
            avg_price = 0.0
        current_price = position.current_price
        if current_price is None:
            unknown.add(f"position:{position.symbol}.current_price")
        else:
            exposure += position.qty * current_price
        positions.append(PositionState(position.symbol, position.qty, avg_price))

    orderable_cash = (
        buying_power.orderable_cash
        if buying_power is not None
        else account.summary.orderable_cash_estimate
    )
    if orderable_cash is None:
        unknown.add("orderable_cash")
    if isinstance(realized_pnl, KisRealizedPnlSnapshot):
        daily_pnl = realized_pnl.daily_realized_pnl
    elif isinstance(realized_pnl, (int, float)) and not isinstance(realized_pnl, bool):
        daily_pnl = float(realized_pnl)
    else:
        daily_pnl = None
    if daily_pnl is None:
        daily_pnl = calculate_local_daily_realized_pnl(database, now=timestamp)
    if daily_pnl is None:
        unknown.add("daily_pnl")
    consecutive_losses = _consecutive_losses(database)
    if consecutive_losses is None:
        unknown.add("consecutive_losses")

    day = _today(timestamp)
    orders = _rows_for_day(database, "broker_orders", day)
    fills = _rows_for_day(database, "broker_fills", day)
    orders_by_symbol: dict[str, int] = {}
    for row in orders:
        symbol = str(row["symbol"])
        orders_by_symbol[symbol] = orders_by_symbol.get(symbol, 0) + 1

    portfolio = PortfolioState(
        current_exposure_krw=exposure,
        open_positions=tuple(positions),
        daily_pnl_krw=daily_pnl or 0.0,
        consecutive_losses=consecutive_losses or 0,
        trades_today=len(orders),
        orders_by_symbol=orders_by_symbol,
    )
    return PortfolioRiskSnapshot(
        portfolio=portfolio,
        unknown_fields=frozenset(unknown),
        broker_orders_today=len(orders),
        broker_fills_today=len(fills),
        orderable_cash=orderable_cash,
    )


def validate_entry_budget(
    requested_price: float,
    quantity: int,
    *,
    orderable_cash: float | None,
    allocated_cap: float,
) -> bool:
    """Return whether a new order is affordable under both cash and allocation caps."""
    if orderable_cash is None:
        return False
    if requested_price <= 0 or quantity <= 0 or allocated_cap <= 0:
        return False
    if orderable_cash < 0:
        return False
    amount = requested_price * quantity
    return amount <= orderable_cash and amount <= allocated_cap


__all__ = [
    "PortfolioRiskSnapshot",
    "build_portfolio_risk_snapshot",
    "calculate_local_daily_realized_pnl",
    "validate_entry_budget",
]
