"""Restart-safe management of stale KIS orders.

This module owns cancellation calls only. It never retries a cancellation
request and never treats the broker's acknowledgement as a confirmed cancel.
The next reconciliation pass must observe a terminal broker state first.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
import json
from typing import Any

from kis_ai_scalper.broker.kis_order_status import (
    KisOrderStatus,
    KisOrderStatusClient,
    KisOrderStatusRecord,
)
from kis_ai_scalper.market.schedule import (
    as_kst,
    is_new_entry_window,
    is_regular_market_open,
)
from kis_ai_scalper.storage.database import Database


ACTIVE_STATUSES = frozenset({"ACKNOWLEDGED", "PARTIALLY_FILLED"})
TERMINAL_STATUSES = frozenset({"FILLED", "CANCELLED", "REJECTED"})
DO_NOT_RECALL_STATUSES = frozenset({"CANCEL_PENDING", "UNKNOWN", *TERMINAL_STATUSES})


@dataclass(frozen=True)
class OrderManagementConfig:
    buy_ttl_seconds: float = 60.0
    sell_ttl_seconds: float = 30.0

    def __post_init__(self) -> None:
        if self.buy_ttl_seconds < 0 or self.sell_ttl_seconds < 0:
            raise ValueError("order TTLs must be non-negative")


@dataclass(frozen=True)
class OrderManagementAction:
    client_order_id: str
    symbol: str
    action: str
    reason: str
    remaining_quantity: int = 0


@dataclass(frozen=True)
class OrderManagementReport:
    inspected: int = 0
    cancel_requested: int = 0
    pending: int = 0
    unknown: int = 0
    confirmed: int = 0
    operator_review: bool = False
    block_new_entries: bool = False
    actions: tuple[OrderManagementAction, ...] = ()

    @property
    def entries_blocked(self) -> bool:
        return self.block_new_entries


@dataclass(frozen=True)
class EntryReevaluationState:
    allowed: bool
    reason: str
    last_confirmed_bar_key: str | None = None


def manage_stale_orders(
    database: Database,
    order_status_client: KisOrderStatusClient,
    *,
    current_time: datetime | None = None,
    config: OrderManagementConfig | None = None,
    entry_bar_key: str | None = None,
    broker_orders: Any | None = None,
    broker_orders_error: BaseException | None = None,
) -> OrderManagementReport:
    """Cancel stale unfilled orders once and leave them pending until reconcile.

    A broker status query failure is intentionally fail-closed. A malformed
    order or an ambiguous cancellation failure becomes ``UNKNOWN`` and blocks
    new entries through runtime metadata.
    """

    now = current_time or datetime.now().astimezone()
    policy = config or OrderManagementConfig()
    actions: list[OrderManagementAction] = []
    if broker_orders_error is not None:
        _block_new_entries(database, f"order_status_unavailable:{type(broker_orders_error).__name__}")
        return OrderManagementReport(
            operator_review=True,
            block_new_entries=True,
            actions=(OrderManagementAction("", "", "UNKNOWN", "order_status_unavailable"),),
        )
    if broker_orders is None:
        try:
            broker_orders = tuple(order_status_client.get_today_orders())
        except Exception as exc:
            _block_new_entries(database, f"order_status_unavailable:{type(exc).__name__}")
            return OrderManagementReport(
                operator_review=True,
                block_new_entries=True,
                actions=(OrderManagementAction("", "", "UNKNOWN", "order_status_unavailable"),),
            )
    else:
        broker_orders = tuple(broker_orders)

    by_broker_id = {str(order.order_number): order for order in broker_orders if order.order_number}
    rows = database.connection.execute(
        """SELECT * FROM broker_orders
           WHERE status NOT IN ('FILLED','CANCELLED','REJECTED')
           ORDER BY created_at, client_order_id"""
    ).fetchall()
    pending = 0
    unknown = 0
    confirmed = 0
    cancel_requested = 0
    review = False

    for local in rows:
        client_id = str(local["client_order_id"])
        local_status = str(local["status"])
        marker = _pending_marker(database, client_id)
        broker = by_broker_id.get(str(local["broker_order_id"] or ""))

        if marker is not None:
            result = _reconcile_pending_marker(database, local, broker, now, marker)
            if result == "confirmed":
                confirmed += 1
            elif result == "pending":
                pending += 1
            continue

        if local_status in DO_NOT_RECALL_STATUSES:
            if local_status == "CANCEL_PENDING":
                pending += 1
            continue
        if local_status not in ACTIVE_STATUSES:
            continue
        if not str(local["broker_order_id"] or "").strip():
            unknown += 1
            review = True
            reason = "missing_order_number"
            _mark_unknown(database, client_id, reason, now)
            actions.append(OrderManagementAction(client_id, str(local["symbol"]), "UNKNOWN", reason))
            continue
        if broker is None:
            continue

        if broker.status not in {KisOrderStatus.UNFILLED, KisOrderStatus.PARTIALLY_FILLED}:
            continue
        if not _same_identity(local, broker):
            unknown += 1
            review = True
            _mark_unknown(database, client_id, "order_identity_missing_or_mismatch", now)
            actions.append(OrderManagementAction(client_id, str(local["symbol"]), "UNKNOWN", "identity_mismatch"))
            continue

        stale, reason = _is_stale(local, broker, now, policy)
        if not stale:
            continue
        valid, validation_reason = _valid_cancel_fields(local, broker)
        if not valid:
            unknown += 1
            review = True
            _mark_unknown(database, client_id, validation_reason, now)
            actions.append(OrderManagementAction(client_id, str(local["symbol"]), "UNKNOWN", validation_reason))
            continue

        try:
            order_price = int(broker.order_price or float(local["requested_price"]))
            database.update_broker_order_status(
                client_id,
                "CANCEL_PENDING",
                broker_order_id=broker.order_number,
                filled_qty=broker.filled_quantity,
                avg_fill_price=broker.average_fill_price,
                updated_at=now,
            )
            order_status_client.cancel_order(
                broker.order_number,
                str(broker.order_branch),
                quantity=broker.remaining_quantity,
                order_price=order_price,
            )
            # A successful HTTP response is only a cancel request acknowledgement.
            _store_pending_marker(database, client_id, entry_bar_key, now)
            cancel_requested += 1
            pending += 1
            actions.append(OrderManagementAction(
                client_id, str(local["symbol"]), "CANCEL_PENDING", reason, broker.remaining_quantity
            ))
        except Exception as exc:
            unknown += 1
            review = True
            _mark_unknown(database, client_id, f"cancel_ambiguous:{type(exc).__name__}", now)
            actions.append(OrderManagementAction(
                client_id, str(local["symbol"]), "UNKNOWN", "cancel_ambiguous"
            ))

    if review:
        _block_new_entries(database, "order_management_operator_review")
    return OrderManagementReport(
        inspected=len(rows),
        cancel_requested=cancel_requested,
        pending=pending,
        unknown=unknown,
        confirmed=confirmed,
        operator_review=review,
        block_new_entries=review or pending > 0,
        actions=tuple(actions),
    )


def run_order_management_cycle(*args: Any, **kwargs: Any) -> OrderManagementReport:
    """Service-loop alias kept explicit for callers wiring pipeline stages."""
    return manage_stale_orders(*args, **kwargs)


def can_re_evaluate_entry(
    database: Database,
    symbol: str,
    bar_key: str,
) -> EntryReevaluationState:
    """Return whether an entry may be considered for this new bar key."""

    rows = database.connection.execute(
        """SELECT client_order_id, status FROM broker_orders
           WHERE symbol=? AND status IN ('INTENT','SUBMITTING','ACKNOWLEDGED',
                                         'PARTIALLY_FILLED','CANCEL_PENDING','UNKNOWN')""",
        (symbol,),
    ).fetchall()
    if rows:
        return EntryReevaluationState(False, "unresolved_order")
    marker = database.get_runtime_metadata(f"order_management.confirmed.{symbol}")
    if marker:
        try:
            payload = json.loads(marker)
            confirmed_bar = payload.get("bar_key")
            if confirmed_bar == bar_key:
                return EntryReevaluationState(False, "same_bar_after_cancel", confirmed_bar)
            return EntryReevaluationState(True, "cancel_confirmed_new_bar", confirmed_bar)
        except (TypeError, ValueError):
            return EntryReevaluationState(False, "invalid_confirmation_state")
    return EntryReevaluationState(True, "no_unresolved_order")


def entry_reevaluation_state(database: Database, symbol: str, bar_key: str) -> EntryReevaluationState:
    return can_re_evaluate_entry(database, symbol, bar_key)


def _is_stale(local: Any, broker: KisOrderStatusRecord, now: datetime, config: OrderManagementConfig) -> tuple[bool, str]:
    if not is_regular_market_open(now):
        return False, "market_closed"
    side = str(local["side"]).upper()
    age = _age_seconds(local, now)
    ttl = config.buy_ttl_seconds if side == "BUY" else config.sell_ttl_seconds
    if age >= ttl:
        return True, f"ttl_{side.lower()}"
    if side == "BUY" and not is_new_entry_window(now) and is_regular_market_open(now):
        return True, "entry_window_closed"
    return False, "fresh"


def _age_seconds(local: Any, now: datetime) -> float:
    timestamp = local["submitted_at"] or local["updated_at"] or local["created_at"]
    try:
        started = datetime.fromisoformat(str(timestamp))
    except (TypeError, ValueError):
        return float("inf")
    return max(0.0, (as_kst(now) - as_kst(started)).total_seconds())


def _same_identity(local: Any, broker: KisOrderStatusRecord) -> bool:
    broker_side = broker.side.name if broker.side is not None else ""
    return str(local["symbol"]) == broker.symbol and str(local["side"]).upper() == broker_side


def _valid_cancel_fields(local: Any, broker: KisOrderStatusRecord) -> tuple[bool, str]:
    if not broker.order_number:
        return False, "missing_order_number"
    if not broker.order_branch:
        return False, "missing_order_branch"
    if broker.remaining_quantity <= 0:
        return False, "missing_remaining_quantity"
    return True, ""


def _pending_marker(database: Database, client_id: str) -> dict[str, Any] | None:
    value = database.get_runtime_metadata(f"order_management.pending.{client_id}")
    if not value:
        return None
    try:
        parsed = json.loads(value)
        return parsed if isinstance(parsed, dict) else None
    except (TypeError, ValueError):
        return {"invalid": True}


def _reconcile_pending_marker(
    database: Database,
    local: Any,
    broker: KisOrderStatusRecord | None,
    now: datetime,
    marker: dict[str, Any],
) -> str:
    client_id = str(local["client_order_id"])
    if marker.get("invalid") or broker is None:
        return "pending"
    if broker.status in {KisOrderStatus.CANCELLED, KisOrderStatus.FILLED, KisOrderStatus.REJECTED}:
        status = {
            KisOrderStatus.CANCELLED: "CANCELLED",
            KisOrderStatus.FILLED: "FILLED",
            KisOrderStatus.REJECTED: "REJECTED",
        }[broker.status]
        database.update_broker_order_status(
            client_id,
            status,
            broker_order_id=broker.order_number,
            filled_qty=broker.filled_quantity,
            avg_fill_price=broker.average_fill_price,
            updated_at=now,
            completed_at=now,
        )
        database.set_runtime_metadata(
            f"order_management.confirmed.{local['symbol']}",
            json.dumps({"bar_key": marker.get("bar_key"), "client_order_id": client_id}),
            updated_at=now,
        )
        database.set_runtime_metadata(f"order_management.pending.{client_id}", "confirmed", updated_at=now)
        return "confirmed"
    database.update_broker_order_status(
        client_id,
        "CANCEL_PENDING",
        broker_order_id=broker.order_number,
        filled_qty=broker.filled_quantity,
        avg_fill_price=broker.average_fill_price,
        updated_at=now,
    )
    return "pending"


def _store_pending_marker(database: Database, client_id: str, bar_key: str | None, now: datetime) -> None:
    database.set_runtime_metadata(
        f"order_management.pending.{client_id}",
        json.dumps({"bar_key": bar_key, "requested_at": now.isoformat()}),
        updated_at=now,
    )


def _mark_unknown(database: Database, client_id: str, reason: str, now: datetime) -> None:
    database.mark_order_unknown(client_id, reason, updated_at=now)
    _block_new_entries(database, reason)


def _block_new_entries(database: Database, reason: str) -> None:
    database.set_runtime_metadata("operator_review", "true")
    database.set_runtime_metadata("block_new_entries", "true")
    database.set_runtime_metadata("order_management.last_reason", reason)


__all__ = [
    "EntryReevaluationState",
    "OrderManagementAction",
    "OrderManagementConfig",
    "OrderManagementReport",
    "can_re_evaluate_entry",
    "entry_reevaluation_state",
    "manage_stale_orders",
    "run_order_management_cycle",
]
