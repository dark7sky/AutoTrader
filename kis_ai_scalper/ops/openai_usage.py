"""OpenAI organization cost reporting helpers."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import requests


@dataclass(frozen=True)
class OpenAICostSummary:
    available: bool
    total: float = 0.0
    currency: str = "usd"
    days: int = 30
    reason: str = "ok"

    def text(self) -> str:
        if not self.available:
            return f"openai cost: unavailable ({self.reason})"
        return f"openai cost: {self.total:.4f} {self.currency.upper()} last_{self.days}d"


def env_value_optional(name: str, *, cwd: Path | None = None) -> str | None:
    import os

    value = os.getenv(name)
    if value:
        return value
    dotenv = (cwd or Path.cwd()) / ".env"
    if dotenv.exists():
        for raw_line in dotenv.read_text(encoding="utf-8").splitlines():
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, candidate = line.split("=", 1)
            if key.strip() == name:
                candidate = candidate.strip().strip('"').strip("'")
                return candidate or None
    return None


def openai_cost_summary_from_env(
    *,
    days: int = 30,
    session: Any | None = None,
    cwd: Path | None = None,
    timeout: float = 8.0,
) -> OpenAICostSummary:
    key = (
        env_value_optional("OPENAI_ADMIN_KEY", cwd=cwd)
        or env_value_optional("OPENAI_USAGE_API_KEY", cwd=cwd)
    )
    if not key:
        return OpenAICostSummary(False, days=days, reason="OPENAI_ADMIN_KEY missing")
    try:
        return fetch_openai_cost_summary(key, days=days, session=session, timeout=timeout)
    except requests.HTTPError as exc:
        status = exc.response.status_code if exc.response is not None else "unknown"
        return OpenAICostSummary(False, days=days, reason=f"HTTP {status}")
    except requests.RequestException:
        return OpenAICostSummary(False, days=days, reason="request_failed")
    except (KeyError, TypeError, ValueError):
        return OpenAICostSummary(False, days=days, reason="unexpected_response")


def fetch_openai_cost_summary(
    api_key: str,
    *,
    days: int = 30,
    session: Any | None = None,
    timeout: float = 8.0,
) -> OpenAICostSummary:
    if not 1 <= days <= 180:
        raise ValueError("days must be between 1 and 180")
    now = datetime.now(timezone.utc)
    start = now - timedelta(days=days)
    http = session or requests.Session()
    response = http.get(
        "https://api.openai.com/v1/organization/costs",
        headers={
            "authorization": f"Bearer {api_key}",
            "content-type": "application/json",
        },
        params={
            "start_time": int(start.timestamp()),
            "end_time": int(now.timestamp()),
            "limit": min(days, 180),
        },
        timeout=timeout,
    )
    response.raise_for_status()
    payload = response.json()
    total = 0.0
    currency = "usd"
    for bucket in payload.get("data", []):
        results = bucket.get("results", bucket.get("result", [])) or []
        for item in results:
            amount = item.get("amount") or {}
            total += float(amount.get("value") or 0)
            currency = str(amount.get("currency") or currency)
    return OpenAICostSummary(True, total=total, currency=currency, days=days)
