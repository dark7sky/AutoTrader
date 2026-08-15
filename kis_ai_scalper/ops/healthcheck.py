"""Container healthcheck based on the persisted service heartbeat."""

from __future__ import annotations

import argparse
import math
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path

from kis_ai_scalper.storage.database import Database


DEFAULT_COMPONENT = "trading-service"
DEFAULT_MAX_AGE_SECONDS = 180.0


def _parse_timestamp(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.strip())
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def check_heartbeat(
    db_path: str | Path,
    *,
    component: str = DEFAULT_COMPONENT,
    max_age_seconds: float = DEFAULT_MAX_AGE_SECONDS,
    now: datetime | None = None,
) -> tuple[bool, str]:
    """Return health and a short diagnostic without mutating the database."""
    if not component.strip():
        return False, "invalid component"
    if not math.isfinite(max_age_seconds) or max_age_seconds < 0:
        return False, "invalid max age"
    database: Database | None = None
    try:
        database = Database(db_path).connect()
        heartbeat = database.get_heartbeat(component)
        if heartbeat is None or not heartbeat.strip():
            return False, f"missing heartbeat: {component}"
        heartbeat_at = _parse_timestamp(heartbeat)
        current = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
        age = (current - heartbeat_at).total_seconds()
        if age < 0:
            return False, f"heartbeat is in the future: {component}"
        if age > max_age_seconds:
            return False, f"stale heartbeat: {component} age={age:.1f}s"
        return True, f"healthy: {component} age={age:.1f}s"
    except (OSError, sqlite3.Error, ValueError, TypeError) as exc:
        return False, f"heartbeat check failed: {type(exc).__name__}"
    finally:
        if database is not None and database._connection is not None:
            database._connection.close()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Check a persisted service heartbeat")
    parser.add_argument("--db", default="data/kis_ai_scalper.sqlite3")
    parser.add_argument("--component", default=DEFAULT_COMPONENT)
    parser.add_argument("--max-age-seconds", type=float, default=DEFAULT_MAX_AGE_SECONDS)
    args = parser.parse_args(argv)
    healthy, message = check_heartbeat(
        args.db,
        component=args.component,
        max_age_seconds=args.max_age_seconds,
    )
    print(message, file=sys.stdout if healthy else sys.stderr)
    return 0 if healthy else 1


if __name__ == "__main__":
    raise SystemExit(main())
