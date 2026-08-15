"""Read-only KIS OAuth helpers."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
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
    expires_at = _parse_expiry(payload.get("expires_at"))
    return bool(payload.get("access_token") and payload.get("approval_key") and expires_at and expires_at > now + timedelta(seconds=60))


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
        now = datetime.now(timezone.utc)
        if cache_path is not None and not refresh_token and cache_path.exists():
            try:
                payload = json.loads(cache_path.read_text(encoding="utf-8"))
            except (OSError, ValueError, TypeError):
                payload = {}
            if isinstance(payload, dict) and _cache_valid(payload, now):
                return KisAuthResult(str(payload["access_token"]), str(payload["approval_key"]), True)

        access_token = self.issue_access_token()
        approval_key = self.issue_websocket_approval_key()
        if cache_path is not None:
            cache_path.parent.mkdir(parents=True, exist_ok=True)
            cache_path.write_text(json.dumps({
                "access_token": access_token,
                "approval_key": approval_key,
                "issued_at": now.isoformat(),
                "expires_at": self._token_expires_at(now).isoformat(),
            }, indent=2), encoding="utf-8")
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
