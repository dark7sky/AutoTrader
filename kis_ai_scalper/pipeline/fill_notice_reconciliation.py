"""Apply KIS realtime fill notices to the local order and position ledger.

This module is deliberately bounded: a notice can only update one already
known local order.  It never creates a local order from broker data.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
import hashlib
import re
from typing import Any

from kis_ai_scalper.broker.kis_fill_notice import FillNotice, FillNoticeKind
from kis_ai_scalper.market.clock import KST
from kis_ai_scalper.storage.database import Database


OPERATOR_REVIEW_KEY = "operator_review"
BLOCK_NEW_ENTRIES_KEY = "block_new_entries"
LAST_REASON_KEY = "fill_notice.last_reason"

_BUY_SIDE_CODE = "02"
_SELL_SIDE_CODE = "01"
_NONTERMINAL = frozenset({"INTENT", "SUBMITTING", "ACKNOWLEDGED", "PARTIALLY_FILLED", "CANCEL_PENDING", "UNKNOWN"})
_TERMINAL = frozenset({"FILLED", "CANCELLED", "REJECTED"})
_SAFE_CODE = re.compile(r"^[a-z0-9_]+$")


@dataclass(frozen=True)
class FillNoticeApplyResult:
    """Small, non-sensitive summary of one fill-notice application."""

    outcome: str
    blocked: bool
    client_order_id: str | None = None
    fill_id: str | None = None
    materialized: int = 0
    reason: str | None = None

    @property
    def applied(self) -> bool:
        return self.outcome == "applied"

    @property
    def duplicate(self) -> bool:
        return self.outcome == "duplicate"

    @property
    def status(self) -> str:
        return self.outcome


# A descriptive alias keeps the public name discoverable without duplicating
# the result type or exposing any broker/customer identifiers.
FillNoticeReconciliationResult = FillNoticeApplyResult
FillNoticeResult = FillNoticeApplyResult


def apply_fill_notice(
    database: Database,
    notice: FillNotice,
    *,
    trading_date: date | None = None,
    received_at: datetime | None = None,
) -> FillNoticeApplyResult:
    """Apply one validated KIS notice, fail-closed on any identity conflict."""
    if not isinstance(notice, FillNotice):
        raise TypeError("notice must be a FillNotice")

    received = _received_at(received_at)
    filled_at, date_error = _filled_at(notice.fill_time, trading_date, received)
    if date_error is not None:
        return _blocked(database, date_error, now=received)

    side = {_BUY_SIDE_CODE: "BUY", _SELL_SIDE_CODE: "SELL"}.get(str(notice.side))
    if side is None:
        return _blocked(database, "invalid_side_code", now=received)

    order_no = _public_identifier(notice.order_no)
    if order_no is None:
        return _blocked(database, "invalid_broker_order_id", now=received)

    rows = database.connection.execute(
        "SELECT * FROM broker_orders WHERE broker_order_id=?", (order_no,)
    ).fetchall()
    if len(rows) != 1:
        return _blocked(
            database,
            "broker_order_not_found" if not rows else "broker_order_ambiguous",
            now=received,
        )
    local = rows[0]
    client_order_id = str(local["client_order_id"])

    mismatch = _identity_mismatch(local, notice, side)
    if mismatch is not None:
        return _blocked(database, mismatch, now=received, client_order_id=client_order_id)

    local_status = str(local["status"])
    local_filled = int(local["filled_qty"] or 0)
    existing_fills = int(
        database.connection.execute(
            "SELECT COALESCE(SUM(quantity), 0) FROM broker_fills WHERE client_order_id=?",
            (client_order_id,),
        ).fetchone()[0]
    )

    if notice.kind is FillNoticeKind.ACCEPTED:
        return _apply_accepted(database, local, local_status, local_filled, existing_fills, received)
    if notice.kind is FillNoticeKind.REJECTED:
        return _apply_rejected(database, local, local_status, local_filled, existing_fills, received)

    fill_id = _fill_id(notice)
    existing = database.connection.execute(
        "SELECT 1 FROM broker_fills WHERE fill_id=?", (fill_id,)
    ).fetchone()
    if existing is not None:
        materialized = _materialize_if_needed(
            database, local, side, client_order_id, received, fill_id=fill_id
        )
        if materialized is None:
            return _blocked(
                database, "missing_entry_risk_metadata", now=received,
                client_order_id=client_order_id, fill_id=fill_id,
            )
        return FillNoticeApplyResult(
            "duplicate", False, client_order_id, fill_id, materialized, None
        )

    if notice.fill_qty <= 0 or notice.fill_price <= 0:
        return _blocked(database, "invalid_fill_values", now=received, client_order_id=client_order_id)
    if local_filled + int(notice.fill_qty) > int(local["requested_qty"]):
        return _blocked(database, "fill_quantity_overflow", now=received, client_order_id=client_order_id)
    if local_status in {"CANCELLED", "REJECTED"}:
        return _blocked(database, "fill_after_terminal_order", now=received, client_order_id=client_order_id)

    risk = _entry_risk(database, local) if side == "BUY" else None
    if side == "BUY" and risk is None:
        return _blocked(
            database, "missing_entry_risk_metadata", now=received,
            client_order_id=client_order_id, fill_id=fill_id,
        )

    try:
        inserted = database.apply_broker_fill(
            fill_id=fill_id,
            client_order_id=client_order_id,
            quantity=int(notice.fill_qty),
            price=float(notice.fill_price),
            filled_at=filled_at,
            broker_order_id=order_no,
            symbol=str(notice.symbol),
            side=side,
            created_at=received,
        )
        if not inserted:
            return FillNoticeApplyResult("duplicate", False, client_order_id, fill_id, 0, None)
        materialized = database.materialize_order_fills(
            client_order_id,
            **(risk if risk is not None else {}),
            close_reason="broker_fill",
        )
    except Exception as exc:
        # Persist only a fixed category, never exception text from broker data.
        return _blocked(
            database, _safe_reason(exc), now=received,
            client_order_id=client_order_id, fill_id=fill_id,
        )
    return FillNoticeApplyResult("applied", False, client_order_id, fill_id, materialized, None)


def _apply_accepted(
    database: Database,
    local: Any,
    status: str,
    filled_qty: int,
    existing_fills: int,
    now: datetime,
) -> FillNoticeApplyResult:
    client_order_id = str(local["client_order_id"])
    if filled_qty < 0 or existing_fills < 0 or filled_qty < existing_fills:
        return _blocked(database, "fill_ledger_inconsistent", now=now, client_order_id=client_order_id)
    if status == "CANCEL_PENDING":
        return FillNoticeApplyResult("ignored", False, client_order_id, None, 0, None)
    if status in {"PARTIALLY_FILLED", "FILLED"} and filled_qty > 0:
        return FillNoticeApplyResult("ignored", False, client_order_id, None, 0, None)
    if status == "UNKNOWN":
        return _blocked(database, "accepted_after_unknown_order", now=now, client_order_id=client_order_id)
    if status in _TERMINAL:
        return _blocked(database, "accepted_after_terminal_order", now=now, client_order_id=client_order_id)
    if status not in _NONTERMINAL:
        return _blocked(database, "invalid_local_order_status", now=now, client_order_id=client_order_id)
    if filled_qty != 0:
        return _blocked(database, "accepted_fill_regression", now=now, client_order_id=client_order_id)
    if status != "ACKNOWLEDGED":
        try:
            database.update_broker_order_status(client_order_id, "ACKNOWLEDGED", updated_at=now)
        except Exception:
            return _blocked(database, "accepted_status_update_failed", now=now, client_order_id=client_order_id)
    return FillNoticeApplyResult("acknowledged", False, client_order_id, None, 0, None)


def _apply_rejected(
    database: Database,
    local: Any,
    status: str,
    filled_qty: int,
    existing_fills: int,
    now: datetime,
) -> FillNoticeApplyResult:
    client_order_id = str(local["client_order_id"])
    if filled_qty != 0 or existing_fills != 0:
        return _blocked(database, "rejected_order_has_fills", now=now, client_order_id=client_order_id)
    if status == "REJECTED":
        return FillNoticeApplyResult("rejected", False, client_order_id, None, 0, None)
    if status in _TERMINAL or status == "UNKNOWN":
        return _blocked(database, "rejected_after_terminal_order", now=now, client_order_id=client_order_id)
    try:
        database.update_broker_order_status(client_order_id, "REJECTED", updated_at=now)
    except Exception:
        return _blocked(database, "rejected_status_update_failed", now=now, client_order_id=client_order_id)
    return FillNoticeApplyResult("rejected", False, client_order_id, None, 0, None)


def _identity_mismatch(local: Any, notice: FillNotice, side: str) -> str | None:
    if str(local["symbol"]) != str(notice.symbol):
        return "symbol_mismatch"
    if str(local["side"]).upper() != side:
        return "side_mismatch"
    if int(local["requested_qty"]) != int(notice.order_qty):
        return "order_quantity_mismatch"
    if notice.fill_qty < 0 or (notice.kind is not FillNoticeKind.FILLED and notice.fill_qty != 0):
        return "invalid_notice_quantity"
    return None


def _entry_risk(database: Database, local: Any) -> dict[str, Any] | None:
    signal_id = local["signal_id"]
    if not signal_id:
        return None
    row = database.connection.execute(
        """SELECT stop_loss_price, take_profit_price, max_holding_seconds
           FROM ai_decision_audits WHERE decision_id=?""",
        (str(signal_id),),
    ).fetchone()
    if row is None or any(row[key] is None for key in ("stop_loss_price", "take_profit_price", "max_holding_seconds")):
        return None
    try:
        stop = float(row["stop_loss_price"])
        take = float(row["take_profit_price"])
        holding = int(row["max_holding_seconds"])
    except (TypeError, ValueError, OverflowError):
        return None
    if not (stop > 0 and take > 0 and stop < take and holding > 0):
        return None
    return {
        "stop_loss_price": stop,
        "take_profit_price": take,
        "max_holding_seconds": holding,
    }


def _materialize_if_needed(
    database: Database,
    local: Any,
    side: str,
    client_order_id: str,
    now: datetime,
    *,
    fill_id: str,
) -> int | None:
    applied = database.connection.execute(
        "SELECT 1 FROM live_position_fill_applications WHERE fill_id=?", (fill_id,)
    ).fetchone()
    if applied is not None:
        return 0
    risk = _entry_risk(database, local) if side == "BUY" else {}
    if side == "BUY" and risk is None:
        return None
    try:
        return database.materialize_order_fills(client_order_id, **(risk or {}), close_reason="broker_fill")
    except Exception:
        return None


def _fill_id(notice: FillNotice) -> str:
    fields = (
        str(notice.kind), str(notice.order_no), str(notice.original_order_no),
        str(notice.order_qty), str(notice.side), str(notice.symbol),
        str(notice.fill_qty), str(notice.fill_price), str(notice.fill_time),
        str(notice.receipt_type), str(notice.order_kind), str(notice.reject_flag),
        str(notice.accepted_flag), str(notice.order_price), str(notice.exchange_id),
    )
    digest = hashlib.sha256("|".join(fields).encode("utf-8")).hexdigest()
    return f"kis:fill:{digest}"


def _filled_at(
    fill_time: str,
    trading_date: date | None,
    received_at: datetime,
) -> tuple[datetime | None, str | None]:
    if trading_date is not None and (
        isinstance(trading_date, datetime) or not isinstance(trading_date, date)
    ):
        return None, "invalid_trading_date"
    if trading_date is None:
        trading_date = received_at.astimezone(KST).date()
    if trading_date > received_at.astimezone(KST).date():
        return None, "future_trading_date"
    if not isinstance(fill_time, str) or len(fill_time) != 6 or not fill_time.isdigit():
        return None, "invalid_fill_time"
    hour, minute, second = int(fill_time[:2]), int(fill_time[2:4]), int(fill_time[4:])
    try:
        value = datetime.combine(trading_date, time(hour, minute, second), tzinfo=KST)
    except ValueError:
        return None, "invalid_fill_time"
    if value > received_at.astimezone(KST):
        return None, "future_fill_time"
    return value, None


def _received_at(value: datetime | None) -> datetime:
    if value is None:
        return datetime.now(timezone.utc)
    if not isinstance(value, datetime):
        raise TypeError("received_at must be a datetime")
    if value.tzinfo is None:
        return value.replace(tzinfo=KST)
    return value


def _public_identifier(value: Any) -> str | None:
    if not isinstance(value, str) or not value or len(value) > 64:
        return None
    if any(ord(char) < 32 or ord(char) == 127 for char in value):
        return None
    return value


def _safe_reason(exc: Exception) -> str:
    name = type(exc).__name__.lower()
    return f"fill_apply_{name}" if _SAFE_CODE.fullmatch(f"fill_apply_{name}") else "fill_apply_failed"


def _blocked(
    database: Database,
    reason: str,
    *,
    now: datetime,
    client_order_id: str | None = None,
    fill_id: str | None = None,
) -> FillNoticeApplyResult:
    if not _SAFE_CODE.fullmatch(reason):
        reason = "fill_notice_blocked"
    database.set_runtime_metadata(OPERATOR_REVIEW_KEY, "true", updated_at=now)
    database.set_runtime_metadata(BLOCK_NEW_ENTRIES_KEY, "true", updated_at=now)
    database.set_runtime_metadata(LAST_REASON_KEY, reason, updated_at=now)
    return FillNoticeApplyResult("blocked", True, client_order_id, fill_id, 0, reason)


__all__ = [
    "BLOCK_NEW_ENTRIES_KEY", "FillNoticeApplyResult",
    "FillNoticeReconciliationResult", "FillNoticeResult", "LAST_REASON_KEY",
    "OPERATOR_REVIEW_KEY", "apply_fill_notice",
]
