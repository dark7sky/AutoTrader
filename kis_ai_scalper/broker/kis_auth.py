"""Read-only KIS OAuth helpers."""

from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import json
import os
from pathlib import Path
import threading
import time
from typing import Any

import requests

from .kis_endpoints import KisEnvironment, api_url
from .safety import redact


@dataclass(frozen=True)
class KisAuthResult:
    access_token: str
    approval_key: str
    cache_hit: bool = False


class KisHttpError(RuntimeError):
    """Safe KIS error containing response metadata but never request secrets."""

    def __init__(self, status_code: int, details: dict[str, str]) -> None:
        summary = " ".join(f"{key}={value}" for key, value in details.items() if value)
        super().__init__(f"KIS HTTP {status_code}" + (f" ({summary})" if summary else ""))
        self.status_code = status_code
        self.details = details


_CACHE_LOCK_TIMEOUT_SECONDS = 45.0
_CACHE_LOCK_STALE_SECONDS = 120.0
_CREDENTIAL_RATE_LIMIT_COOLDOWN_SECONDS = 60.0


def _raise_for_kis_response(response: Any, secret_values: tuple[str, ...] = ()) -> None:
    status_code = int(getattr(response, "status_code", 200))
    if status_code < 400:
        response.raise_for_status()
        return
    details: dict[str, str] = {}
    try:
        payload = response.json()
    except (ValueError, TypeError, AttributeError):
        payload = {}
    if isinstance(payload, dict):
        for key in ("rt_cd", "msg_cd", "msg1", "error_description"):
            value = payload.get(key)
            if value not in (None, ""):
                safe_value = str(value)
                for secret in secret_values:
                    if secret:
                        safe_value = safe_value.replace(secret, "[redacted]")
                details[key] = safe_value[:300]
    raise KisHttpError(status_code, details)


def _parse_expiry(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def _cache_valid(payload: dict[str, Any], now: datetime) -> bool:
    return bool(payload.get("approval_key") and _token_cache_valid(payload, now))


def _token_cache_valid(payload: dict[str, Any], now: datetime) -> bool:
    expires_at = _parse_expiry(payload.get("expires_at"))
    return bool(
        payload.get("access_token")
        and expires_at
        and expires_at > now + timedelta(seconds=60)
    )


def _raise_if_credential_cooldown(payload: dict[str, Any], now: datetime) -> None:
    retry_after = _parse_expiry(payload.get("credential_retry_after"))
    if retry_after is None or retry_after <= now:
        return
    try:
        status_code = int(payload.get("last_error_status", 429))
    except (TypeError, ValueError):
        status_code = 429
    details = payload.get("last_error_details")
    safe_details = details if isinstance(details, dict) else {}
    raise KisHttpError(
        status_code,
        {
            **{str(key): str(value) for key, value in safe_details.items()},
            "retry_after": retry_after.isoformat(),
        },
    )


def _cache_credential_rate_limit(
    path: Path,
    exc: KisHttpError,
    now: datetime,
    *,
    preserve_token: bool = False,
) -> None:
    if exc.status_code not in {403, 429}:
        return
    details = {
        key: value
        for key, value in exc.details.items()
        if key in {"rt_cd", "msg_cd", "error_description"}
    }
    cached = _read_cache(path) if preserve_token else {}
    base = (
        {
            key: cached[key]
            for key in ("access_token", "issued_at", "expires_at")
            if key in cached
        }
        if _token_cache_valid(cached, now) else {}
    )
    _write_cache(path, {
        **base,
        "credential_retry_after": (
            now + timedelta(seconds=_CREDENTIAL_RATE_LIMIT_COOLDOWN_SECONDS)
        ).isoformat(),
        "last_error_status": exc.status_code,
        "last_error_details": details,
    })


def _read_cache(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError, TypeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _write_cache(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(
        f".{path.name}.{os.getpid()}.{threading.get_ident()}.tmp"
    )
    try:
        temporary.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        deadline = time.monotonic() + 2.0
        while True:
            try:
                os.replace(temporary, path)
                break
            except PermissionError:
                if time.monotonic() >= deadline:
                    raise
                time.sleep(0.02)
    finally:
        temporary.unlink(missing_ok=True)


@contextmanager
def _cache_issue_lock(cache_path: Path):
    """Serialize KIS credential issuance across threads and containers."""

    cache_path.parent.mkdir(parents=True, exist_ok=True)
    lock_path = cache_path.with_name(f".{cache_path.name}.lock")
    deadline = time.monotonic() + _CACHE_LOCK_TIMEOUT_SECONDS
    descriptor: int | None = None
    while descriptor is None:
        try:
            descriptor = os.open(lock_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        except FileExistsError:
            try:
                age = time.time() - lock_path.stat().st_mtime
                if age >= _CACHE_LOCK_STALE_SECONDS:
                    lock_path.unlink(missing_ok=True)
                    continue
            except FileNotFoundError:
                continue
            if time.monotonic() >= deadline:
                raise TimeoutError("timed out waiting for the KIS credential cache lock")
            time.sleep(0.05)
    try:
        os.write(descriptor, str(os.getpid()).encode("ascii"))
        yield
    finally:
        os.close(descriptor)
        lock_path.unlink(missing_ok=True)


class KisAuthClient:
    def __init__(self, environment: KisEnvironment | str, app_key: str, app_secret: str,
                 session: requests.Session | Any | None = None, timeout: float = 15.0) -> None:
        if not app_key or not app_secret:
            raise ValueError("KIS app key and app secret are required")
        self.environment = KisEnvironment.parse(environment) if isinstance(environment, str) else environment
        self.app_key = app_key
        self.app_secret = app_secret
        self.session = session or requests.Session()
        self.timeout = timeout
        self.last_token_expires_in: int | None = None
        self.last_token_expired_at: str | None = None

    def issue_access_token(self) -> str:
        response = self.session.post(
            api_url(self.environment, "/oauth2/tokenP"),
            json={"grant_type": "client_credentials", "appkey": self.app_key, "appsecret": self.app_secret},
            timeout=self.timeout,
        )
        _raise_for_kis_response(response, (self.app_key, self.app_secret))
        payload = response.json()
        self.last_token_expires_in = _positive_int(payload.get("expires_in"))
        self.last_token_expired_at = str(payload.get("access_token_token_expired") or "") or None
        token = payload.get("access_token")
        if not token:
            raise ValueError("KIS token response did not contain access_token")
        return str(token)

    def issue_websocket_approval_key(self) -> str:
        response = self.session.post(
            api_url(self.environment, "/oauth2/Approval"),
            json={"grant_type": "client_credentials", "appkey": self.app_key, "secretkey": self.app_secret},
            timeout=self.timeout,
        )
        _raise_for_kis_response(response, (self.app_key, self.app_secret))
        approval_key = response.json().get("approval_key")
        if not approval_key:
            raise ValueError("KIS approval response did not contain approval_key")
        return str(approval_key)

    def authenticate_read_only(self, cache_path: Path | None = None, refresh_token: bool = False) -> KisAuthResult:
        if cache_path is None:
            return self._issue_credentials()
        with _cache_issue_lock(cache_path):
            now = datetime.now(timezone.utc)
            payload = _read_cache(cache_path)
            if not refresh_token and _cache_valid(payload, now):
                return KisAuthResult(
                    str(payload["access_token"]), str(payload["approval_key"]), True
                )
            _raise_if_credential_cooldown(payload, now)

            if not refresh_token and _token_cache_valid(payload, now):
                access_token = str(payload["access_token"])
                issued_at = str(payload.get("issued_at") or now.isoformat())
                expires_at = str(payload["expires_at"])
            else:
                try:
                    access_token = self.issue_access_token()
                except KisHttpError as exc:
                    _cache_credential_rate_limit(cache_path, exc, now)
                    raise
                issued_at = now.isoformat()
                expires_at = self._token_expires_at(now).isoformat()
                _write_cache(cache_path, {
                    "access_token": access_token,
                    "issued_at": issued_at,
                    "expires_at": expires_at,
                })

            try:
                approval_key = self.issue_websocket_approval_key()
            except KisHttpError as exc:
                _cache_credential_rate_limit(
                    cache_path, exc, now, preserve_token=True
                )
                raise
            _write_cache(cache_path, {
                "access_token": access_token,
                "approval_key": approval_key,
                "issued_at": issued_at,
                "expires_at": expires_at,
            })
            return KisAuthResult(access_token, approval_key, False)

    def _issue_credentials(self) -> KisAuthResult:
        access_token = self.issue_access_token()
        approval_key = self.issue_websocket_approval_key()
        return KisAuthResult(access_token, approval_key, False)

    def _token_expires_at(self, issued_at: datetime) -> datetime:
        if self.last_token_expires_in is not None:
            return issued_at + timedelta(seconds=max(0, self.last_token_expires_in))
        parsed = _parse_expiry(self.last_token_expired_at)
        return parsed or (issued_at + timedelta(hours=23))


def _positive_int(value: Any) -> int | None:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return None
    return parsed if parsed > 0 else None
