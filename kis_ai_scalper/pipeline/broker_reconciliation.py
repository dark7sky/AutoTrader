"""Restart-safe reconciliation between the local order ledger and KIS."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Iterable

from kis_ai_scalper.broker.kis_account import KisAccountClient
from kis_ai_scalper.broker.kis_auth import KisHttpError
from kis_ai_scalper.broker.kis_order_status import (
    KisOrderStatus,
    KisOrderStatusClient,
    KisOrderStatusRecord,
)
from kis_ai_scalper.storage.database import Database


ACTIVE_LOCAL_STATUSES = frozenset({
    "INTENT",
    "SUBMITTING",
    "ACKNOWLEDGED",
    "PARTIALLY_FILLED",
    "CANCEL_PENDING",
    "UNKNOWN",
})


@dataclass(frozen=True)
class ReconciliationReport:
    """Outcome of one bounded, repeatable reconciliation pass."""

    updated_orders: int = 0
    new_fills: int = 0
    materialized_fills: int = 0
    operator_review: bool = False
    block_new_entries: bool = False
    reasons: tuple[str, ...] = ()

    @property
    def entries_blocked(self) -> bool:
        return self.block_new_entries


def reconcile_broker_state(
    database: Database,
    order_status_client: KisOrderStatusClient,
    account_client: KisAccountClient,
    *,
    current_time: datetime | None = None,
    broker_orders: Iterable[KisOrderStatusRecord] | None = None,
    broker_orders_error: BaseException | None = None,
    account_snapshot: Any | None = None,
    account_snapshot_error: BaseException | None = None,
) -> ReconciliationReport:
    """Reconcile local active orders and positions with same-day KIS state.

    This function only adopts fills belonging to a known local order. It never
    creates a local position from a broker-only holding and never closes a local
    position because a balance result is missing or different.
    """
    now = current_time or datetime.now().astimezone()
    reasons: list[str] = []
    updated_orders = 0
    new_fills = 0
    materialized_fills = 0

    if broker_orders_error is not None:
        reasons.append(f"order_status_unavailable:{_exception_code(broker_orders_error)}")
        return _finish(database, now, updated_orders, new_fills, materialized_fills, reasons)
    if broker_orders is None:
        try:
            broker_orders = tuple(order_status_client.get_today_orders())
        except Exception as exc:
            reasons.append(f"order_status_unavailable:{_exception_code(exc)}")
            return _finish(database, now, updated_orders, new_fills, materialized_fills, reasons)
    else:
        broker_orders = tuple(broker_orders)

    by_broker_id = {order.order_number: order for order in broker_orders if order.order_number}
    local_rows = database.connection.execute(
        """SELECT * FROM broker_orders
           WHERE status IN ({})
              OR EXISTS (
                   SELECT 1 FROM broker_fills f
                   LEFT JOIN live_position_fill_applications a ON a.fill_id=f.fill_id
                   WHERE f.client_order_id=broker_orders.client_order_id
                     AND a.fill_id IS NULL
              )
           ORDER BY created_at, client_order_id""".format(
            ",".join("?" for _ in ACTIVE_LOCAL_STATUSES)
        ),
        tuple(sorted(ACTIVE_LOCAL_STATUSES)),
    ).fetchall()

    matched_broker_ids: set[str] = set()
    for local in local_rows:
        client_order_id = str(local["client_order_id"])
        local_status = str(local["status"])
        broker_order_id = str(local["broker_order_id"] or "")
        if local_status == "UNKNOWN":
            reasons.append(f"local_unknown:{client_order_id}")
        if not broker_order_id:
            reasons.append(f"local_order_unconfirmed:{client_order_id}")
            continue
        broker = by_broker_id.get(broker_order_id)
        if broker is None:
            reasons.append(f"broker_order_missing:{client_order_id}:{broker_order_id}")
            continue
        matched_broker_ids.add(broker_order_id)
        if not _same_order(local, broker):
            reasons.append(f"order_identity_mismatch:{client_order_id}:{broker_order_id}")
            continue
        if broker.filled_quantity < int(local["filled_qty"]):
            reasons.append(f"fill_regression:{client_order_id}")
            continue
        if broker.ordered_quantity and broker.ordered_quantity != int(local["requested_qty"]):
            reasons.append(f"order_quantity_mismatch:{client_order_id}")
            continue

        delta = broker.filled_quantity - int(local["filled_qty"])
        if delta:
            fill_price = _incremental_fill_price(local, broker, delta)
            fill_id = f"reconcile:{client_order_id}:cum:{broker.filled_quantity}"
            try:
                inserted = database.apply_broker_fill(
                    fill_id=fill_id,
                    client_order_id=client_order_id,
                    quantity=delta,
                    price=fill_price,
                    filled_at=now,
                    broker_order_id=broker.order_number,
                    symbol=broker.symbol,
                    side=broker.side.name if broker.side is not None else None,
                )
            except Exception as exc:
                reasons.append(f"fill_apply_failed:{client_order_id}:{type(exc).__name__}")
                continue
            if inserted:
                new_fills += 1

        status = _local_status_for_broker(broker.status)
        if status is None:
            reasons.append(f"broker_status_unknown:{client_order_id}")
            continue
        # A cancel acknowledgement is not a confirmed cancellation. Preserve
        # the local pending state until KIS reports a terminal order status.
        if local_status == "CANCEL_PENDING" and status in {
            "ACKNOWLEDGED",
            "PARTIALLY_FILLED",
        }:
            status = "CANCEL_PENDING"
        if database.update_broker_order_status(
            client_order_id,
            status,
            broker_order_id=broker.order_number,
            filled_qty=broker.filled_quantity,
            avg_fill_price=broker.average_fill_price,
            updated_at=now,
            completed_at=now if status in {"FILLED", "CANCELLED", "REJECTED"} else None,
        ):
            updated_orders += 1

        try:
            materialized_fills += _materialize_local_fills(database, local, now)
        except Exception as exc:
            reasons.append(f"materialize_failed:{client_order_id}:{type(exc).__name__}")

    for broker in broker_orders:
        if broker.order_number in matched_broker_ids:
            continue
        if broker.remaining_quantity > 0 or broker.status in {
            KisOrderStatus.UNFILLED,
            KisOrderStatus.PARTIALLY_FILLED,
        }:
            reasons.append(f"broker_only_open_order:{broker.order_number}")

    if account_snapshot_error is not None:
        reasons.append(f"account_snapshot_unavailable:{_exception_code(account_snapshot_error)}")
        return _finish(database, now, updated_orders, new_fills, materialized_fills, reasons)
    if account_snapshot is None:
        try:
            account = account_client.get_snapshot()
        except Exception as exc:
            reasons.append(f"account_snapshot_unavailable:{_exception_code(exc)}")
            return _finish(database, now, updated_orders, new_fills, materialized_fills, reasons)
    else:
        account = account_snapshot

    local_positions = _local_position_quantities(database)
    broker_positions = {position.symbol: int(position.qty) for position in account.positions}
    for symbol in sorted(set(local_positions) | set(broker_positions)):
        local_qty = local_positions.get(symbol, 0)
        broker_qty = broker_positions.get(symbol, 0)
        if local_qty != broker_qty:
            reasons.append(f"position_mismatch:{symbol}:local={local_qty}:broker={broker_qty}")

    return _finish(database, now, updated_orders, new_fills, materialized_fills, reasons)


def reconcile(
    database: Database,
    order_status_client: KisOrderStatusClient,
    account_client: KisAccountClient,
    *,
    current_time: datetime | None = None,
) -> ReconciliationReport:
    """Short alias for callers wiring the service loop."""
    return reconcile_broker_state(
        database, order_status_client, account_client, current_time=current_time
    )


def _same_order(local: Any, broker: KisOrderStatusRecord) -> bool:
    local_side = str(local["side"]).upper()
    broker_side = broker.side.name if broker.side is not None else ""
    return str(local["symbol"]) == broker.symbol and local_side == broker_side


def _exception_code(exc: BaseException) -> str:
    if not isinstance(exc, KisHttpError):
        return type(exc).__name__
    parts = ["KisHttpError", f"http_{exc.status_code}"]
    for key in ("rt_cd", "msg_cd"):
        value = str(exc.details.get(key, "")).strip()
        if value and all(character.isalnum() or character in {"-", "_"} for character in value):
            parts.append(f"{key}_{value}")
    return ":".join(parts)


def _local_status_for_broker(status: KisOrderStatus) -> str | None:
    return {
        KisOrderStatus.UNFILLED: "ACKNOWLEDGED",
        KisOrderStatus.PARTIALLY_FILLED: "PARTIALLY_FILLED",
        KisOrderStatus.FILLED: "FILLED",
        KisOrderStatus.CANCELLED: "CANCELLED",
        KisOrderStatus.REJECTED: "REJECTED",
    }.get(status)


def _incremental_fill_price(local: Any, broker: KisOrderStatusRecord, delta: int) -> float:
    """Derive the new fill price from KIS cumulative quantity and average price."""
    if delta <= 0:
        raise ValueError("fill delta must be positive")
    broker_average = broker.average_fill_price
    if broker_average is None:
        return float(local["requested_price"])
    previous_quantity = int(local["filled_qty"])
    previous_average = local["avg_fill_price"]
    if previous_quantity <= 0 or previous_average is None:
        return float(broker_average)
    cumulative_notional = float(broker_average) * broker.filled_quantity
    previous_notional = float(previous_average) * previous_quantity
    incremental_price = (cumulative_notional - previous_notional) / delta
    if incremental_price <= 0:
        raise ValueError("derived incremental fill price must be positive")
    return incremental_price


def _materialize_local_fills(database: Database, local: Any, now: datetime) -> int:
    if str(local["side"]).upper() == "BUY":
        decision = database.connection.execute(
            """SELECT stop_loss_price, take_profit_price, max_holding_seconds
               FROM ai_decision_audits WHERE decision_id=?""",
            (local["signal_id"],),
        ).fetchone()
        if decision is None:
            raise ValueError("missing entry risk metadata")
        return database.materialize_order_fills(
            str(local["client_order_id"]),
            stop_loss_price=decision["stop_loss_price"],
            take_profit_price=decision["take_profit_price"],
            max_holding_seconds=decision["max_holding_seconds"] or 900,
        )
    return database.materialize_order_fills(
        str(local["client_order_id"]), close_reason="broker_fill"
    )


def _local_position_quantities(database: Database) -> dict[str, int]:
    rows = database.connection.execute(
        """SELECT symbol, SUM(quantity) AS quantity
           FROM live_positions WHERE status='OPEN' GROUP BY symbol"""
    ).fetchall()
    return {str(row["symbol"]): int(row["quantity"] or 0) for row in rows}


def _finish(
    database: Database,
    now: datetime,
    updated_orders: int,
    new_fills: int,
    materialized_fills: int,
    reasons: Iterable[str],
) -> ReconciliationReport:
    unique_reasons = tuple(dict.fromkeys(reasons))
    review = bool(unique_reasons)
    database.set_runtime_metadata("operator_review", "true" if review else "false", updated_at=now)
    database.set_runtime_metadata(
        "block_new_entries", "true" if review else "false", updated_at=now
    )
    return ReconciliationReport(
        updated_orders=updated_orders,
        new_fills=new_fills,
        materialized_fills=materialized_fills,
        operator_review=review,
        block_new_entries=review,
        reasons=unique_reasons,
    )


__all__ = ["ReconciliationReport", "reconcile", "reconcile_broker_state"]
