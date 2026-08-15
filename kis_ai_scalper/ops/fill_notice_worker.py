"""Reconnectable, read-only KIS realtime fill-notice worker.

The worker owns no broker order client.  Its only broker operation is the
WebSocket subscription used to receive acceptance, rejection, and fill
events.  Every ledger application uses a fresh SQLite connection so the
service loop and the notification stream cannot share thread-affine state.
"""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from datetime import datetime, timezone
import inspect
from pathlib import Path
import re
import time
from typing import Any, AsyncIterator, Callable, Protocol

from kis_ai_scalper.broker.kis_auth import KisAuthClient
from kis_ai_scalper.broker.kis_endpoints import KisEnvironment, websocket_url
from kis_ai_scalper.broker.kis_fill_notice import (
    FillNotice,
    FillNoticeAck,
    KisFillNoticeClient,
)
from kis_ai_scalper.broker.kis_ws import is_pingpong, parse_system_message, raw_to_text
from kis_ai_scalper.config import load_config
from kis_ai_scalper.config.loader import _environment_with_dotenv
from kis_ai_scalper.pipeline.fill_notice_reconciliation import apply_fill_notice
from kis_ai_scalper.storage import connect_database


class StopEvent(Protocol):
    def is_set(self) -> bool: ...


class FillNoticeNotifier(Protocol):
    def send(self, message: str) -> Any: ...


class _WorkerFault(RuntimeError):
    """An internal, secret-free category used to drive reconnect handling."""


class _MissingHTS(_WorkerFault):
    pass


class _EnvironmentChanged(_WorkerFault):
    pass


_SAFE_CODE = re.compile(r"^[a-z0-9_.:-]{1,80}$")
_DEFAULT_RECONNECT_MIN = 1.0
_DEFAULT_RECONNECT_MAX = 30.0
_DEFAULT_RECEIVE_TIMEOUT = 1.0
_DEFAULT_ACK_TIMEOUT = 15.0
_DEFAULT_NOTIFY_THROTTLE = 60.0


def _is_stopped(stop_event: StopEvent) -> bool:
    try:
        return bool(stop_event.is_set())
    except Exception:
        return True


def _clock_value(clock: Callable[[], float] | Any) -> float:
    value = clock.monotonic() if hasattr(clock, "monotonic") else clock()
    return float(value)


async def _maybe_await(value: Any) -> Any:
    if inspect.isawaitable(value):
        return await value
    return value


async def _default_sleep(stop_event: StopEvent, seconds: float) -> None:
    # Waiting on the threading event makes service shutdown prompt even when
    # the reconnect ceiling is configured above the default value.
    if hasattr(stop_event, "wait"):
        await asyncio.to_thread(stop_event.wait, max(0.0, seconds))
    else:
        await asyncio.sleep(max(0.0, seconds))


async def _sleep(
    stop_event: StopEvent,
    seconds: float,
    sleeper: Callable[[float], Any] | None,
) -> None:
    if sleeper is None:
        await _default_sleep(stop_event, seconds)
        return
    await _maybe_await(sleeper(max(0.0, seconds)))


def _safe_code(value: str, fallback: str = "worker_error") -> str:
    candidate = str(value).lower().replace(" ", "_")
    return candidate if _SAFE_CODE.fullmatch(candidate) else fallback


def _exception_code(exc: BaseException) -> str:
    if isinstance(exc, _MissingHTS):
        return "missing_hts_id"
    if isinstance(exc, _EnvironmentChanged):
        return "environment_changed"
    if isinstance(exc, TimeoutError | asyncio.TimeoutError):
        return "timeout"
    name = type(exc).__name__.lower()
    return _safe_code(name, "connection_error")


def _hts_id(config_path: Path) -> str:
    """Read the non-YAML HTS identifier without ever exposing its value."""
    values = _environment_with_dotenv(config_path)
    value = values.get("KIS_HTS_ID", "")
    if not isinstance(value, str):
        raise _MissingHTS("KIS_HTS_ID is unavailable")
    value = value.strip()
    if not value or len(value) > 64 or any(ord(char) < 32 or ord(char) == 127 for char in value):
        raise _MissingHTS("KIS_HTS_ID is unavailable")
    return value


def _runtime_environment(db_path: str | Path) -> KisEnvironment:
    with connect_database(db_path) as database:
        database.init_schema()
        control = database.get_runtime_control()
    return KisEnvironment.parse(control.environment)


def _cache_path(config_path: Path, environment: KisEnvironment) -> Path:
    root = config_path.resolve().parent.parent
    return root / "data" / "auth" / f"kis_token_{environment.value}.json"


def _connection_material(config_path: Path, environment: KisEnvironment) -> tuple[str, str, str, Path]:
    config = load_config(config_path)
    credentials = config.kis_api_for(environment.value)
    if credentials is None:
        raise _WorkerFault("KIS credentials are unavailable")
    hts_id = _hts_id(config_path)
    return credentials.app_key, credentials.app_secret, hts_id, _cache_path(config_path, environment)


def _record_runtime(
    db_path: str | Path,
    *,
    status: str,
    environment: KisEnvironment | str | None = None,
    last_error: str = "",
) -> None:
    safe_status = _safe_code(status, "unavailable")
    safe_error = _safe_code(last_error, "worker_error") if last_error else ""
    env_value = environment.value if isinstance(environment, KisEnvironment) else str(environment or "")
    if env_value not in {"", "demo", "real"}:
        env_value = ""
    now = datetime.now(timezone.utc)
    with connect_database(db_path) as database:
        database.init_schema()
        database.record_heartbeat("fill-notice", heartbeat_at=now)
        database.set_runtime_metadata("fill-notice:status", safe_status, updated_at=now)
        database.set_runtime_metadata("fill-notice:environment", env_value, updated_at=now)
        database.set_runtime_metadata("fill-notice:last_error", safe_error, updated_at=now)


def _record_apply_result(db_path: str | Path, result: Any) -> None:
    outcome = _safe_code(getattr(result, "outcome", "unknown"), "unknown")
    reason = getattr(result, "reason", None)
    error = ""
    if outcome == "blocked":
        error = "ledger_blocked"
    elif reason:
        error = _safe_code(str(reason), "ledger_error")
    now = datetime.now(timezone.utc)
    with connect_database(db_path) as database:
        database.init_schema()
        database.record_heartbeat("fill-notice", heartbeat_at=now)
        database.set_runtime_metadata("fill-notice:last_apply", outcome, updated_at=now)
        if error:
            database.set_runtime_metadata("fill-notice:last_error", error, updated_at=now)


async def _dispatch_notice(db_path: str | Path, notice: FillNotice) -> None:
    """Apply one notice using a connection that is never reused by the socket."""
    try:
        with connect_database(db_path) as database:
            database.init_schema()
            result = apply_fill_notice(
                database,
                notice,
                received_at=datetime.now(timezone.utc),
            )
        _record_apply_result(db_path, result)
    except Exception:
        # A malformed or locally inconsistent notice must not terminate the
        # receive loop.  The reconciliation adapter is itself fail-closed.
        try:
            _record_runtime(db_path, status="connected", last_error="ledger_apply_error")
        except Exception:
            pass


@asynccontextmanager
async def _default_socket_factory(endpoint: str) -> AsyncIterator[Any]:
    try:
        import websockets
    except ImportError as exc:
        raise RuntimeError("websockets package is required") from exc
    async with websockets.connect(
        endpoint,
        open_timeout=15,
        close_timeout=5,
        ping_interval=None,
    ) as socket:
        yield socket


@asynccontextmanager
async def _socket_context(factory: Callable[[str], Any] | None, endpoint: str) -> AsyncIterator[Any]:
    candidate = (factory or _default_socket_factory)(endpoint)
    candidate = await _maybe_await(candidate)
    if hasattr(candidate, "__aenter__") and hasattr(candidate, "__aexit__"):
        async with candidate as socket:
            yield socket
    else:
        yield candidate


async def _auth_result(
    environment: KisEnvironment,
    app_key: str,
    app_secret: str,
    cache_path: Path,
    auth_factory: Callable[..., Any] | None,
) -> Any:
    auth = await _maybe_await((auth_factory or KisAuthClient)(environment, app_key, app_secret))
    result = auth.authenticate_read_only(cache_path=cache_path)
    return await _maybe_await(result)


async def _send(socket: Any, message: str) -> None:
    await _maybe_await(socket.send(message))


class _StateReporter:
    def __init__(self, notifier: Any, clock: Callable[[], float] | Any, throttle: float) -> None:
        self.notifier = notifier
        self.clock = clock
        self.throttle = throttle
        self.last_state: tuple[str, str, str] | None = None
        self.last_sent_at = float("-inf")

    async def set(
        self,
        db_path: str | Path,
        status: str,
        environment: KisEnvironment | str | None,
        error: str = "",
    ) -> None:
        env_value = environment.value if isinstance(environment, KisEnvironment) else str(environment or "")
        safe_status = _safe_code(status, "unavailable")
        safe_error = _safe_code(error, "worker_error") if error else ""
        try:
            _record_runtime(db_path, status=safe_status, environment=env_value, last_error=safe_error)
        except Exception:
            return
        state = (safe_status, env_value, safe_error)
        now = _clock_value(self.clock)
        should_send = (
            self.notifier is not None
            and (state != self.last_state or (safe_error and now - self.last_sent_at >= self.throttle))
        )
        self.last_state = state
        if not should_send:
            return
        message = f"fill-notice: {safe_status}"
        if env_value:
            message += f" environment={env_value}"
        if safe_error:
            message += f" error={safe_error}"
        try:
            target = self.notifier.send if hasattr(self.notifier, "send") else self.notifier
            await _maybe_await(target(message))
            self.last_sent_at = now
        except Exception:
            # Notification failure must never affect KIS receive/reconnect.
            pass


async def _session(
    config_path: Path,
    db_path: str | Path,
    stop_event: StopEvent,
    environment: KisEnvironment,
    approval_key: str,
    hts_id: str,
    socket_factory: Callable[[str], Any] | None,
    receive_timeout_seconds: float,
    ack_timeout_seconds: float,
    clock: Callable[[], float] | Any,
    reporter: _StateReporter,
) -> str:
    endpoint = websocket_url(environment)
    async with _socket_context(socket_factory, endpoint) as socket:
        client = KisFillNoticeClient(environment, approval_key, hts_id, socket)
        await client.subscribe()
        ack_deadline = _clock_value(clock) + ack_timeout_seconds
        await reporter.set(db_path, "subscribing", environment)
        while not _is_stopped(stop_event):
            if _runtime_environment(db_path) is not environment:
                raise _EnvironmentChanged("runtime environment changed")
            try:
                raw = await asyncio.wait_for(socket.recv(), timeout=receive_timeout_seconds)
            except asyncio.TimeoutError:
                _record_runtime(db_path, status="subscribing" if client.ack is None else "connected", environment=environment)
                if client.ack is None and _clock_value(clock) >= ack_deadline:
                    raise _WorkerFault("subscription acknowledgement timed out")
                continue
            if raw is None:
                raise _WorkerFault("socket closed")
            system = parse_system_message(raw)
            if is_pingpong(system) or raw_to_text(raw).strip().upper() == "PINGPONG":
                await _send(socket, raw_to_text(raw))
                continue
            handled = client.handle_message(raw)
            if isinstance(handled, FillNoticeAck):
                if not handled.ready_for_data:
                    raise _WorkerFault("subscription acknowledgement rejected")
                await reporter.set(db_path, "connected", environment)
                continue
            if isinstance(handled, tuple):
                for notice in handled:
                    if isinstance(notice, FillNotice):
                        await _dispatch_notice(db_path, notice)
                _record_runtime(db_path, status="connected", environment=environment)
        return "stopped"


async def _run_fill_notice_worker(
    config_path: str | Path,
    db_path: str | Path,
    stop_event: StopEvent,
    notifier: Any,
    *,
    reconnect_min_seconds: float,
    reconnect_max_seconds: float,
    socket_factory: Callable[[str], Any] | None,
    auth_factory: Callable[..., Any] | None,
    clock: Callable[[], float] | Any,
    sleeper: Callable[[float], Any] | None,
    receive_timeout_seconds: float,
    ack_timeout_seconds: float,
    notify_throttle_seconds: float,
) -> None:
    config_file = Path(config_path)
    reporter = _StateReporter(notifier, clock, notify_throttle_seconds)
    delay = reconnect_min_seconds
    current_environment: KisEnvironment | None = None
    try:
        await reporter.set(db_path, "starting", None)
    except Exception:
        pass
    while not _is_stopped(stop_event):
        try:
            environment = _runtime_environment(db_path)
            if current_environment is not environment:
                current_environment = environment
                delay = reconnect_min_seconds
            app_key, app_secret, hts_id, cache_path = _connection_material(config_file, environment)
            auth = await _auth_result(environment, app_key, app_secret, cache_path, auth_factory)
            approval_key = getattr(auth, "approval_key", "")
            if not isinstance(approval_key, str) or not approval_key:
                raise _WorkerFault("KIS websocket approval key unavailable")
            await reporter.set(db_path, "connecting", environment)
            outcome = await _session(
                config_file,
                db_path,
                stop_event,
                environment,
                approval_key,
                hts_id,
                socket_factory,
                receive_timeout_seconds,
                ack_timeout_seconds,
                clock,
                reporter,
            )
            if outcome == "stopped":
                break
            delay = reconnect_min_seconds
        except _EnvironmentChanged:
            delay = reconnect_min_seconds
            try:
                await reporter.set(db_path, "reconnecting", current_environment, "environment_changed")
            except Exception:
                pass
            continue
        except Exception as exc:
            error = _exception_code(exc)
            try:
                environment = _runtime_environment(db_path)
            except Exception:
                environment = current_environment
            try:
                await reporter.set(db_path, "unavailable", environment, error)
            except Exception:
                pass
            if _is_stopped(stop_event):
                break
            try:
                await _sleep(stop_event, delay, sleeper)
            except Exception:
                await asyncio.sleep(min(delay, reconnect_max_seconds))
            delay = min(reconnect_max_seconds, max(reconnect_min_seconds, delay * 2))
    try:
        previous_error = reporter.last_state[2] if reporter.last_state is not None else ""
        await reporter.set(db_path, "stopped", current_environment, previous_error)
    except Exception:
        pass


def run_fill_notice_worker(
    config_path: str | Path,
    db_path: str | Path,
    stop_event: StopEvent,
    notifier: Any = None,
    *,
    reconnect_min_seconds: float = _DEFAULT_RECONNECT_MIN,
    reconnect_max_seconds: float = _DEFAULT_RECONNECT_MAX,
    socket_factory: Callable[[str], Any] | None = None,
    auth_factory: Callable[..., Any] | None = None,
    clock: Callable[[], float] | Any = time.monotonic,
    sleeper: Callable[[float], Any] | None = None,
    receive_timeout_seconds: float = _DEFAULT_RECEIVE_TIMEOUT,
    ack_timeout_seconds: float = _DEFAULT_ACK_TIMEOUT,
    notify_throttle_seconds: float = _DEFAULT_NOTIFY_THROTTLE,
) -> None:
    """Run the worker synchronously, suitable as a daemon thread target."""
    if reconnect_min_seconds <= 0 or reconnect_max_seconds < reconnect_min_seconds:
        raise ValueError("invalid reconnect bounds")
    if receive_timeout_seconds <= 0 or ack_timeout_seconds <= 0:
        raise ValueError("receive and ACK timeouts must be positive")
    if notify_throttle_seconds < 0:
        raise ValueError("notify throttle must be non-negative")
    asyncio.run(
        _run_fill_notice_worker(
            config_path,
            db_path,
            stop_event,
            notifier,
            reconnect_min_seconds=reconnect_min_seconds,
            reconnect_max_seconds=reconnect_max_seconds,
            socket_factory=socket_factory,
            auth_factory=auth_factory,
            clock=clock,
            sleeper=sleeper,
            receive_timeout_seconds=receive_timeout_seconds,
            ack_timeout_seconds=ack_timeout_seconds,
            notify_throttle_seconds=notify_throttle_seconds,
        )
    )


__all__ = ["run_fill_notice_worker"]
