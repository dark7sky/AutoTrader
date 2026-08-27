"""Process-wide pacing for KIS REST API calls."""

from __future__ import annotations

import threading
import time
from collections.abc import Mapping
from typing import Any, Callable

import requests

from .kis_endpoints import KisEnvironment


KIS_REST_INTERVAL_SECONDS = {
    KisEnvironment.DEMO: 0.5,
    KisEnvironment.REAL: 0.05,
}
KIS_RATE_LIMIT_CODE = "EGW00201"
KIS_RATE_LIMIT_COOLDOWN_SECONDS = 1.0
KIS_READ_MAX_RETRIES = 2


class KisRateLimiter:
    """Serialize REST starts and enforce the official environment interval."""

    def __init__(
        self,
        *,
        clock: Callable[[], float] = time.monotonic,
        sleeper: Callable[[float], None] = time.sleep,
    ) -> None:
        self._clock = clock
        self._sleeper = sleeper
        self._lock = threading.Lock()
        self._last_call_at: dict[KisEnvironment, float] = {}
        self._not_before: dict[KisEnvironment, float] = {}

    def wait(self, environment: KisEnvironment | str) -> None:
        env = (
            KisEnvironment.parse(environment)
            if isinstance(environment, str)
            else environment
        )
        with self._lock:
            now = self._clock()
            previous = self._last_call_at.get(env)
            interval_remaining = (
                0.0
                if previous is None
                else KIS_REST_INTERVAL_SECONDS[env] - (now - previous)
            )
            cooldown_remaining = self._not_before.get(env, 0.0) - now
            remaining = max(0.0, interval_remaining, cooldown_remaining)
            if remaining > 0:
                self._sleeper(remaining)
            self._last_call_at[env] = self._clock()

    def penalize(
        self,
        environment: KisEnvironment | str,
        seconds: float = KIS_RATE_LIMIT_COOLDOWN_SECONDS,
    ) -> None:
        env = (
            KisEnvironment.parse(environment)
            if isinstance(environment, str)
            else environment
        )
        with self._lock:
            self._not_before[env] = max(
                self._not_before.get(env, 0.0),
                self._clock() + seconds,
            )


def _is_rate_limited(response: Any) -> bool:
    try:
        payload = response.json()
    except (AttributeError, TypeError, ValueError):
        return False
    return (
        isinstance(payload, Mapping)
        and str(payload.get("msg_cd", "")) == KIS_RATE_LIMIT_CODE
    )


class KisRateLimitedSession:
    """requests-compatible session that shares one process-wide limiter."""

    def __init__(
        self,
        environment: KisEnvironment | str,
        session: Any | None = None,
        *,
        limiter: KisRateLimiter | None = None,
    ) -> None:
        self.environment = (
            KisEnvironment.parse(environment)
            if isinstance(environment, str)
            else environment
        )
        self._session = session if session is not None else requests.Session()
        self._limiter = limiter or GLOBAL_KIS_RATE_LIMITER

    def get(self, url: str, **kwargs: Any) -> Any:
        for attempt in range(KIS_READ_MAX_RETRIES + 1):
            self._limiter.wait(self.environment)
            response = self._session.get(url, **kwargs)
            if not _is_rate_limited(response) or attempt >= KIS_READ_MAX_RETRIES:
                return response
            self._limiter.penalize(self.environment)
        raise AssertionError("unreachable")

    def post(self, url: str, **kwargs: Any) -> Any:
        self._limiter.wait(self.environment)
        return self._session.post(url, **kwargs)

    def request(self, method: str, url: str, **kwargs: Any) -> Any:
        self._limiter.wait(self.environment)
        return self._session.request(method, url, **kwargs)

    def __getattr__(self, name: str) -> Any:
        return getattr(self._session, name)


GLOBAL_KIS_RATE_LIMITER = KisRateLimiter()


def new_rate_limited_session(
    environment: KisEnvironment | str,
) -> KisRateLimitedSession:
    return KisRateLimitedSession(environment)


__all__ = [
    "GLOBAL_KIS_RATE_LIMITER",
    "KIS_RATE_LIMIT_CODE",
    "KIS_RATE_LIMIT_COOLDOWN_SECONDS",
    "KIS_READ_MAX_RETRIES",
    "KIS_REST_INTERVAL_SECONDS",
    "KisRateLimitedSession",
    "KisRateLimiter",
    "new_rate_limited_session",
]
