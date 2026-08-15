"""Conservative background reconciliation and cancellation supervisor.

This module intentionally has no dependency on the trading cycle.  It keeps
the local ledger current while paused, and its only broker write is a cancel
request for an already-known local BUY order.
"""

from __future__ import annotations

import inspect
import json
import os
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

from kis_ai_scalper.broker.kis_account import KisAccountClient
from kis_ai_scalper.broker.kis_auth import KisAuthClient
from kis_ai_scalper.broker.kis_endpoints import KisEnvironment
from kis_ai_scalper.broker.kis_order import KisOrderSide
from kis_ai_scalper.broker.kis_order_status import (
    KisOrderStatus,
    KisOrderStatusClient,
)
from kis_ai_scalper.config import load_config
from kis_ai_scalper.pipeline.broker_reconciliation import reconcile_broker_state
from kis_ai_scalper.pipeline.order_management import (
    OrderManagementConfig,
    manage_stale_orders,
)
from kis_ai_scalper.storage import Database, connect_database


SERVICE_LEASE_NAME = "trading-service"
SUPERVISOR_COMPONENT = "order-supervisor"
HEARTBEAT_KEY = f"heartbeat:{SUPERVISOR_COMPONENT}"
STATUS_KEY = f"{SUPERVISOR_COMPONENT}.status"
LAST_ERROR_KEY = f"{SUPERVISOR_COMPONENT}.last_error"
CANCEL_REQUEST_KEY = "operator.cancel_open_buys_requested"
CANCEL_STATUS_KEY = "operator.cancel_open_buys_status"
DEFAULT_NOTIFY_THROTTLE_SECONDS = 300.0


ClientFactory = Callable[..., Any]


@dataclass
class SupervisorState:
    """Objects that may safely be reused between iterations of one thread."""

    environment: KisEnvironment | None = None
    clients: tuple[KisOrderStatusClient, KisAccountClient] | None = None
    force_refresh: bool = False
    last_notification_state: str | None = None
    last_notification_at: float = 0.0


@dataclass(frozen=True)
class OrderSupervisorResult:
    status: str
    environment: str | None = None
    paused: bool | None = None
    operator_review: bool = False
    block_new_entries: bool = False
    cancel_requested: int = 0
    cancel_errors: int = 0
    error: str | None = None
    auth_refresh_requested: bool = False

    @property
    def ok(self) -> bool:
        return self.status in {"reconciled", "reconciled_operator_review"}


@dataclass(frozen=True)
class _CancelReport:
    requested: int = 0
    errors: int = 0
    skipped: int = 0
    error_type: str | None = None


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _parse_timestamp(value: Any) -> datetime | None:
    try:
        parsed = datetime.fromisoformat(str(value))
    except (TypeError, ValueError):
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _expected_owner(expected_owner_id: str | None, owner_id: str | None) -> str | None:
    if expected_owner_id and owner_id and expected_owner_id != owner_id:
        raise ValueError("expected_owner_id and owner_id disagree")
    return expected_owner_id or owner_id or os.getenv("TRADING_SERVICE_OWNER_ID") or None


def _lease_is_valid(database: Database, expected_owner_id: str | None, now: datetime) -> bool:
    lease = database.get_service_lease(SERVICE_LEASE_NAME)
    if lease is None:
        return False
    if expected_owner_id is not None and str(lease["owner_id"]) != expected_owner_id:
        return False
    expires_at = _parse_timestamp(lease["expires_at"])
    return expires_at is not None and expires_at > now.astimezone(timezone.utc)


def _environment(database: Database) -> KisEnvironment:
    value = database.get_runtime_control().environment
    return KisEnvironment.parse(value)


def _cache_path(config_path: str | Path, environment: KisEnvironment) -> Path:
    project_root = Path(config_path).resolve().parent.parent
    return project_root / "data" / "auth" / f"kis_token_{environment.value}.json"


def _build_clients(
    config_path: str | Path,
    environment: KisEnvironment,
    *,
    refresh_token: bool,
) -> tuple[KisOrderStatusClient, KisAccountClient]:
    config = load_config(Path(config_path))
    api = config.kis_api_for(environment.value)
    account = config.kis_account_for(environment.value)
    if api is None or account is None or not account.account_no:
        raise ValueError(f"KIS {environment.value} credentials/account are unavailable")
    account_no, account_product_code = _account_components(
        account.account_no, account.account_product_code
    )
    auth = KisAuthClient(environment, api.app_key, api.app_secret)
    auth_result = auth.authenticate_read_only(
        cache_path=_cache_path(config_path, environment),
        refresh_token=refresh_token,
    )
    common = {
        "environment": environment,
        "app_key": api.app_key,
        "app_secret": api.app_secret,
        "access_token": auth_result.access_token,
        "account_no": account_no,
        "account_product_code": account_product_code,
    }
    return KisOrderStatusClient(**common), KisAccountClient(**common)


def _account_components(account_no: str, product_code: str) -> tuple[str, str]:
    raw = account_no.strip()
    if "-" in raw:
        raw, suffix = raw.split("-", 1)
        product_code = suffix.strip() or product_code
    digits = "".join(character for character in raw if character.isdigit())
    if len(digits) == 10 and product_code == "01":
        return digits[:8], digits[8:]
    if len(digits) != 8:
        raise ValueError("KIS account number must contain eight account digits")
    return digits, product_code


def _call_factory(
    factory: ClientFactory,
    config_path: str | Path,
    environment: KisEnvironment,
    refresh_token: bool,
) -> tuple[Any, Any]:
    """Allow small test factories without weakening the production contract."""
    try:
        signature = inspect.signature(factory)
    except (TypeError, ValueError):
        signature = None
    if signature is not None:
        names = set(signature.parameters)
        if {"config_path", "environment"}.issubset(names):
            result = factory(
                config_path=config_path,
                environment=environment,
                refresh_token=refresh_token,
            )
        elif "environment" in names:
            result = factory(environment, refresh_token=refresh_token)
        else:
            result = factory(environment, refresh_token)
    else:
        result = factory(config_path, environment, refresh_token)
    if isinstance(result, tuple) and len(result) == 2:
        return result
    order_client = getattr(result, "order_status", None)
    account_client = getattr(result, "account", None)
    if order_client is None or account_client is None:
        raise TypeError("client_factory must return (order_status_client, account_client)")
    return order_client, account_client


def _sanitized_error(exc: BaseException) -> str:
    return type(exc).__name__


def _looks_like_auth_error(value: Any) -> bool:
    text = str(value).lower()
    return any(
        token in text
        for token in (
            "401",
            "403",
            "unauthorized",
            "forbidden",
            "kishttperror",
            "token",
            "auth",
            "접근",
            "인증",
        )
    )


def _report_needs_refresh(report: Any) -> bool:
    reasons = getattr(report, "reasons", ()) or ()
    return any(_looks_like_auth_error(reason) for reason in reasons)


def _notify(
    notifier: Any,
    state: SupervisorState,
    message_state: str,
    message: str,
    *,
    now_monotonic: float | None = None,
    throttle_seconds: float = DEFAULT_NOTIFY_THROTTLE_SECONDS,
) -> None:
    if notifier is None:
        return
    current = time.monotonic() if now_monotonic is None else now_monotonic
    changed = message_state != state.last_notification_state
    throttled_due = current - state.last_notification_at >= throttle_seconds
    if not changed and not throttled_due:
        return
    try:
        if callable(notifier):
            notifier(message)
        elif hasattr(notifier, "notify"):
            notifier.notify(message)
        elif hasattr(notifier, "send_message"):
            notifier.send_message(message)
        elif hasattr(notifier, "send"):
            notifier.send(message)
        else:
            raise TypeError("notifier must be callable or expose notify/send_message")
    except Exception:
        # Notification is advisory and must never interrupt reconciliation.
        pass
    state.last_notification_state = message_state
    state.last_notification_at = current


def _write_status(
    database: Database,
    *,
    status: str,
    environment: str | None,
    paused: bool | None,
    operator_review: bool = False,
    block_new_entries: bool = False,
    error: str | None = None,
    now: datetime,
) -> None:
    payload = {
        "status": status,
        "environment": environment,
        "paused": paused,
        "operator_review": bool(operator_review),
        "block_new_entries": bool(block_new_entries),
        "updated_at": now.isoformat(),
    }
    database.set_runtime_metadata(STATUS_KEY, json.dumps(payload, sort_keys=True), updated_at=now)
    database.set_runtime_metadata(LAST_ERROR_KEY, error or "none", updated_at=now)
    database.record_heartbeat(SUPERVISOR_COMPONENT, heartbeat_at=now)


def _mark_exception(database: Database, exc: BaseException, now: datetime) -> None:
    error = _sanitized_error(exc)
    database.set_runtime_metadata("operator_review", "true", updated_at=now)
    database.set_runtime_metadata("block_new_entries", "true", updated_at=now)
    _write_status(
        database,
        status="error",
        environment=None,
        paused=None,
        operator_review=True,
        block_new_entries=True,
        error=error,
        now=now,
    )


def _set_cancel_status(
    database: Database,
    *,
    status: str,
    requested: int,
    errors: int,
    skipped: int,
    error_type: str | None,
    now: datetime,
) -> None:
    database.set_runtime_metadata(
        CANCEL_STATUS_KEY,
        json.dumps(
            {
                "status": status,
                "requested": requested,
                "errors": errors,
                "skipped": skipped,
                "error": error_type or "none",
                "updated_at": now.isoformat(),
            },
            sort_keys=True,
        ),
        updated_at=now,
    )


def _cancel_requested_buys(
    database: Database,
    order_status_client: KisOrderStatusClient,
    *,
    now: datetime,
) -> _CancelReport:
    if (database.get_runtime_metadata(CANCEL_REQUEST_KEY) or "").strip().lower() != "true":
        return _CancelReport()

    requested = errors = skipped = 0
    error_type: str | None = None
    # Consume the operator request before the broker call.  A restart cannot
    # accidentally replay it; unresolved orders remain CANCEL_PENDING.
    database.set_runtime_metadata(CANCEL_REQUEST_KEY, "false", updated_at=now)
    rows = database.connection.execute(
        """SELECT * FROM broker_orders
           WHERE side='BUY' AND status IN ('ACKNOWLEDGED','PARTIALLY_FILLED')
           ORDER BY created_at, client_order_id"""
    ).fetchall()
    try:
        broker_orders = tuple(order_status_client.get_today_orders()) if rows else ()
    except Exception as exc:
        error_type = _sanitized_error(exc)
        database.set_runtime_metadata("operator_review", "true", updated_at=now)
        database.set_runtime_metadata("block_new_entries", "true", updated_at=now)
        _set_cancel_status(
            database, status="error", requested=0, errors=1, skipped=0,
            error_type=error_type, now=now,
        )
        return _CancelReport(errors=1, error_type=error_type)

    by_id = {str(order.order_number): order for order in broker_orders if order.order_number}
    for local in rows:
        broker = by_id.get(str(local["broker_order_id"] or ""))
        if broker is None or broker.side is not KisOrderSide.BUY:
            errors += 1
            error_type = "order_identity_mismatch"
            database.mark_order_unknown(
                str(local["client_order_id"]), error_type, updated_at=now
            )
            database.set_runtime_metadata("operator_review", "true", updated_at=now)
            database.set_runtime_metadata("block_new_entries", "true", updated_at=now)
            continue
        if broker.status not in {KisOrderStatus.UNFILLED, KisOrderStatus.PARTIALLY_FILLED}:
            skipped += 1
            continue
        if not broker.order_branch or broker.remaining_quantity <= 0:
            errors += 1
            error_type = "missing_cancel_fields"
            database.mark_order_unknown(
                str(local["client_order_id"]), error_type, updated_at=now
            )
            database.set_runtime_metadata("operator_review", "true", updated_at=now)
            database.set_runtime_metadata("block_new_entries", "true", updated_at=now)
            continue
        client_id = str(local["client_order_id"])
        database.update_broker_order_status(
            client_id,
            "CANCEL_PENDING",
            broker_order_id=broker.order_number,
            filled_qty=broker.filled_quantity,
            avg_fill_price=broker.average_fill_price,
            updated_at=now,
        )
        try:
            order_price = int(broker.order_price or float(local["requested_price"]))
            order_status_client.cancel_order(
                broker.order_number,
                str(broker.order_branch),
                quantity=broker.remaining_quantity,
                order_price=order_price,
            )
        except Exception as exc:
            errors += 1
            error_type = _sanitized_error(exc)
            database.mark_order_unknown(client_id, f"cancel_ambiguous:{error_type}", updated_at=now)
            database.set_runtime_metadata("operator_review", "true", updated_at=now)
            database.set_runtime_metadata("block_new_entries", "true", updated_at=now)
            continue
        database.set_runtime_metadata(
            f"order_management.pending.{client_id}",
            json.dumps({"reason": "operator_cancel_open_buys", "requested_at": now.isoformat()}),
            updated_at=now,
        )
        requested += 1

    _set_cancel_status(
        database,
        status="error" if errors else "completed",
        requested=requested,
        errors=errors,
        skipped=skipped,
        error_type=error_type,
        now=now,
    )
    return _CancelReport(requested, errors, skipped, error_type)


def one_iteration(
    config_path: str | Path,
    db_path: str | Path,
    stop_event: threading.Event | None = None,
    notifier: Any | None = None,
    interval_seconds: float = 5.0,
    buy_ttl_seconds: float = 60.0,
    sell_ttl_seconds: float = 30.0,
    refresh_token: bool = False,
    *,
    expected_owner_id: str | None = None,
    owner_id: str | None = None,
    client_factory: ClientFactory | None = None,
    state: SupervisorState | None = None,
    now: datetime | None = None,
) -> OrderSupervisorResult:
    """Run one bounded pass; ``client_factory`` is injectable for unit tests."""
    del stop_event, interval_seconds
    if buy_ttl_seconds < 0 or sell_ttl_seconds < 0:
        raise ValueError("order TTLs must be non-negative")
    state = state or SupervisorState()
    expected = _expected_owner(expected_owner_id, owner_id)
    current = (now or _now()).astimezone(timezone.utc)
    try:
        with connect_database(db_path) as database:
            database.init_schema()
            if not _lease_is_valid(database, expected, current):
                _write_status(
                    database, status="lease_invalid", environment=None, paused=None,
                    operator_review=True, block_new_entries=True,
                    error="invalid_service_lease", now=current,
                )
                _notify(notifier, state, "lease_invalid", "order-supervisor: service lease invalid")
                return OrderSupervisorResult("lease_invalid", error="invalid_service_lease")

            control = database.get_runtime_control()
            environment = _environment(database)
            if state.clients is None or state.environment is not environment or state.force_refresh:
                factory = client_factory or _build_clients
                state.clients = _call_factory(
                    factory, config_path, environment, refresh_token or state.force_refresh
                )
                state.environment = environment
                state.force_refresh = False
            order_status_client, account_client = state.clients

            reconciliation = reconcile_broker_state(
                database, order_status_client, account_client, current_time=current
            )
            if _report_needs_refresh(reconciliation):
                state.force_refresh = True
            cancel_report = _cancel_requested_buys(
                database, order_status_client, now=current
            )
            if _looks_like_auth_error(cancel_report.error_type):
                state.force_refresh = True
            management = manage_stale_orders(
                database,
                order_status_client,
                current_time=current,
                config=OrderManagementConfig(
                    buy_ttl_seconds=buy_ttl_seconds,
                    sell_ttl_seconds=sell_ttl_seconds,
                ),
            )
            review = bool(reconciliation.operator_review or management.operator_review or cancel_report.errors)
            blocked = bool(reconciliation.block_new_entries or management.block_new_entries or cancel_report.errors)
            status = "reconciled_operator_review" if review else "reconciled"
            _write_status(
                database,
                status=status,
                environment=environment.value,
                paused=control.paused,
                operator_review=review,
                block_new_entries=blocked,
                now=current,
            )
            _notify(
                notifier,
                state,
                status,
                f"order-supervisor: {status} environment={environment.value} paused={str(control.paused).lower()}",
            )
            return OrderSupervisorResult(
                status,
                environment.value,
                control.paused,
                review,
                blocked,
                cancel_report.requested,
                cancel_report.errors,
                auth_refresh_requested=state.force_refresh,
            )
    except Exception as exc:
        current = (now or _now()).astimezone(timezone.utc)
        try:
            with connect_database(db_path) as database:
                database.init_schema()
                _mark_exception(database, exc, current)
        except Exception:
            pass
        if _looks_like_auth_error(exc):
            state.force_refresh = True
        _notify(notifier, state, f"error:{_sanitized_error(exc)}", f"order-supervisor: error={_sanitized_error(exc)}")
        return OrderSupervisorResult(
            "error", error=_sanitized_error(exc), operator_review=True,
            block_new_entries=True, auth_refresh_requested=state.force_refresh,
        )


def run_order_supervisor(
    config_path: str | Path,
    db_path: str | Path,
    stop_event: threading.Event,
    notifier: Any | None = None,
    interval_seconds: float = 5.0,
    buy_ttl_seconds: float = 60.0,
    sell_ttl_seconds: float = 30.0,
    refresh_token: bool = False,
    *,
    expected_owner_id: str | None = None,
    owner_id: str | None = None,
    client_factory: ClientFactory | None = None,
) -> OrderSupervisorResult | None:
    """Daemon-thread entry point with bounded, interruptible retry backoff."""
    if not isinstance(stop_event, threading.Event):
        raise TypeError("stop_event must be threading.Event")
    if interval_seconds <= 0 or buy_ttl_seconds < 0 or sell_ttl_seconds < 0:
        raise ValueError("interval must be positive and TTLs must be non-negative")
    state = SupervisorState()
    backoff = 1.0
    last: OrderSupervisorResult | None = None
    while not stop_event.is_set():
        started = time.monotonic()
        last = one_iteration(
            config_path,
            db_path,
            stop_event,
            notifier,
            interval_seconds,
            buy_ttl_seconds,
            sell_ttl_seconds,
            refresh_token,
            expected_owner_id=expected_owner_id,
            owner_id=owner_id,
            client_factory=client_factory,
            state=state,
        )
        refresh_token = False
        if last.status == "lease_invalid":
            return last
        if last.status == "error":
            if last.auth_refresh_requested:
                state.force_refresh = True
            if stop_event.wait(min(backoff, 60.0)):
                break
            backoff = min(backoff * 2.0, 60.0)
            continue
        backoff = 1.0
        remaining = max(0.0, interval_seconds - (time.monotonic() - started))
        if stop_event.wait(remaining):
            break
    return last


__all__ = [
    "HEARTBEAT_KEY",
    "LAST_ERROR_KEY",
    "STATUS_KEY",
    "CANCEL_REQUEST_KEY",
    "CANCEL_STATUS_KEY",
    "OrderSupervisorResult",
    "SupervisorState",
    "one_iteration",
    "run_order_supervisor",
]
