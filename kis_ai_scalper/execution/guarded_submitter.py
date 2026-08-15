"""Last-moment safety checks for broker order submission.

The guard deliberately stores only the SQLite path.  Every submission opens a
fresh connection, so a connection created by one worker thread is never used
by another worker thread.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Protocol

from kis_ai_scalper.broker.kis_endpoints import KisEnvironment
from kis_ai_scalper.broker.kis_order import KisOrderRequest, KisOrderResult, KisOrderSide
from kis_ai_scalper.market.schedule import is_regular_market_open
from kis_ai_scalper.storage import connect_database


SERVICE_LEASE_NAME = "trading-service"
KST = timezone(timedelta(hours=9), "KST")
_TRUE_VALUES = frozenset({"1", "true", "yes", "on"})


class OrderSafetyGateError(RuntimeError):
    """Raised when an order fails the final pre-submission safety gate."""


class OrderSubmitter(Protocol):
    def submit_order(self, request: KisOrderRequest) -> KisOrderResult:
        ...


class GuardedSubmitter:
    """Wrap a broker submitter with fail-closed, no-retry order checks."""

    def __init__(
        self,
        submitter: OrderSubmitter,
        db_path: str | Path | None = None,
        environment: KisEnvironment | str | None = None,
        service_owner_id: str | None = None,
        *,
        database_path: str | Path | None = None,
        expected_environment: KisEnvironment | str | None = None,
        owner_id: str | None = None,
        lease_name: str = SERVICE_LEASE_NAME,
        now_fn: Callable[[], datetime] | None = None,
    ) -> None:
        if db_path is not None and database_path is not None and Path(db_path) != Path(database_path):
            raise ValueError("db_path and database_path must identify the same database")
        path = db_path if db_path is not None else database_path
        if path is None:
            raise ValueError("db_path is required")
        if environment is not None and expected_environment is not None:
            if KisEnvironment.parse(str(environment)) != KisEnvironment.parse(str(expected_environment)):
                raise ValueError("environment and expected_environment must match")
        requested_environment = environment if environment is not None else expected_environment
        if requested_environment is None:
            requested_environment = getattr(submitter, "environment", None)
        if requested_environment is None:
            raise ValueError("expected KIS environment is required")
        selected_environment = KisEnvironment.parse(str(requested_environment))
        submitter_environment = getattr(submitter, "environment", None)
        if submitter_environment is not None:
            actual_environment = KisEnvironment.parse(str(submitter_environment))
            if actual_environment != selected_environment:
                raise ValueError(
                    "guard environment must match the underlying submitter environment"
                )

        selected_owner = service_owner_id if service_owner_id is not None else owner_id
        if service_owner_id is not None and owner_id is not None and service_owner_id != owner_id:
            raise ValueError("service_owner_id and owner_id must match")
        if not isinstance(selected_owner, str) or not selected_owner.strip():
            raise ValueError("service_owner_id is required")
        if not isinstance(lease_name, str) or not lease_name.strip():
            raise ValueError("lease_name is required")

        self.submitter = submitter
        self.db_path = Path(path)
        self.environment = selected_environment
        self.service_owner_id = selected_owner
        self.lease_name = lease_name
        self._now_fn = now_fn or (lambda: datetime.now(KST))

    def submit_order(self, request: KisOrderRequest) -> KisOrderResult:
        """Check current runtime state and submit exactly once if approved.

        There is intentionally no retry or exception recovery around the
        underlying submitter.  A rejected gate never reaches it.
        """
        try:
            now = _aware_now(self._now_fn())
            side = KisOrderSide(request.side)
            quantity = _positive_quantity(request)
            if str(getattr(request, "exchange_id", "")).upper() != "KRX":
                raise OrderSafetyGateError("order blocked by safety gate: exchange_not_krx")

            with connect_database(self.db_path) as database:
                database.init_schema()
                _check_runtime_state(
                    database, self.environment, self.lease_name, self.service_owner_id, now
                )

                if not is_regular_market_open(now):
                    raise OrderSafetyGateError("order blocked by safety gate: market_closed")

                if side is KisOrderSide.BUY:
                    blocked_flags = tuple(
                        key
                        for key in ("block_new_entries", "operator_review")
                        if _is_true(database.get_runtime_metadata(key))
                    )
                    if blocked_flags:
                        raise OrderSafetyGateError(
                            "order blocked by safety gate: " + ", ".join(blocked_flags)
                        )
                    unresolved = database.connection.execute(
                        """SELECT status FROM broker_orders
                           WHERE side='BUY'
                             AND status IN ('ACKNOWLEDGED','PARTIALLY_FILLED',
                                            'CANCEL_PENDING','UNKNOWN')
                           LIMIT 1"""
                    ).fetchone()
                    if unresolved is not None:
                        raise OrderSafetyGateError(
                            "order blocked by safety gate: unresolved_buy_order "
                            f"status={unresolved['status']}"
                        )
                else:
                    row = database.connection.execute(
                        """SELECT COALESCE(SUM(quantity), 0) AS quantity
                           FROM live_positions
                           WHERE symbol=? AND status='OPEN'""",
                        (request.symbol,),
                    ).fetchone()
                    open_quantity = int(row["quantity"] or 0)
                    if open_quantity <= 0:
                        raise OrderSafetyGateError(
                            "order blocked by safety gate: sell_without_local_open_position "
                            f"symbol={request.symbol}"
                        )
                    if quantity > open_quantity:
                        raise OrderSafetyGateError(
                            "order blocked by safety gate: sell_quantity_exceeds_local_position "
                            f"requested={quantity} available={open_quantity} symbol={request.symbol}"
                        )

                # Runtime controls may change while the side-specific checks
                # run. Re-read them immediately before allowing submission.
                _check_runtime_state(
                    database, self.environment, self.lease_name, self.service_owner_id, now
                )
        except OrderSafetyGateError:
            raise
        except Exception as exc:
            raise OrderSafetyGateError(
                f"order safety gate check failed: {type(exc).__name__}: {exc}"
            ) from exc

        # This is the only call to the wrapped submitter.  In particular, a
        # transport exception is propagated without attempting a second order.
        return self.submitter.submit_order(request)


def _positive_quantity(request: Any) -> int:
    quantity = getattr(request, "quantity", None)
    if isinstance(quantity, bool) or not isinstance(quantity, int) or quantity <= 0:
        raise OrderSafetyGateError("order blocked by safety gate: invalid_quantity")
    return quantity


def _is_true(value: str | None) -> bool:
    return value is not None and value.strip().lower() in _TRUE_VALUES


def _aware_now(value: datetime) -> datetime:
    if not isinstance(value, datetime):
        raise TypeError("now_fn must return datetime")
    return value if value.tzinfo is not None else value.replace(tzinfo=KST)


def _lease_belongs_to_owner(row: Any, owner_id: str, now: datetime) -> bool:
    if row is None or str(row["owner_id"]) != owner_id:
        return False
    try:
        expires_at = datetime.fromisoformat(str(row["expires_at"]))
    except (TypeError, ValueError):
        return False
    if expires_at.tzinfo is None:
        expires_at = expires_at.replace(tzinfo=KST)
    return expires_at > now


def _check_runtime_state(
    database: Any,
    environment: KisEnvironment,
    lease_name: str,
    owner_id: str,
    now: datetime,
) -> None:
    control = database.get_runtime_control()
    if control.paused:
        raise OrderSafetyGateError("order blocked by safety gate: runtime_paused")
    if control.environment != environment.value:
        raise OrderSafetyGateError(
            "order blocked by safety gate: runtime_environment_mismatch "
            f"runtime={control.environment} expected={environment.value}"
        )

    emergency_keys = tuple(
        key
        for key in ("emergency_stop", "telegram.emergency_stop")
        if _is_true(database.get_runtime_metadata(key))
    )
    if emergency_keys:
        raise OrderSafetyGateError(
            "order blocked by safety gate: emergency_stop "
            f"({', '.join(emergency_keys)})"
        )

    lease = database.get_service_lease(lease_name)
    if not _lease_belongs_to_owner(lease, owner_id, now):
        raise OrderSafetyGateError(
            "order blocked by safety gate: service_lease_invalid "
            f"lease={lease_name} owner={owner_id}"
        )


GuardedOrderSubmitter = GuardedSubmitter


__all__ = [
    "GuardedOrderSubmitter",
    "GuardedSubmitter",
    "OrderSafetyGateError",
    "SERVICE_LEASE_NAME",
]
