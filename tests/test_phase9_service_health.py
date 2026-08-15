from datetime import datetime, timedelta, timezone

from kis_ai_scalper.ops.healthcheck import check_heartbeat, main
from kis_ai_scalper.storage.database import Database


UTC = timezone.utc


def make_database(tmp_path):
    database = Database(tmp_path / "service.sqlite3").connect()
    database.init_schema()
    return database


def test_fresh_heartbeat_is_healthy_even_when_paused(tmp_path):
    database = make_database(tmp_path)
    now = datetime(2026, 8, 18, 1, 0, tzinfo=UTC)
    database.record_heartbeat("trading-service", heartbeat_at=now - timedelta(seconds=20))
    ok, message = check_heartbeat(database.path, now=now, max_age_seconds=60)
    assert ok is True
    assert "healthy" in message


def test_missing_stale_and_invalid_heartbeats_are_unhealthy(tmp_path):
    database = make_database(tmp_path)
    now = datetime(2026, 8, 18, 1, 0, tzinfo=UTC)
    ok, _ = check_heartbeat(database.path, now=now, max_age_seconds=60)
    assert ok is False
    database.record_heartbeat("trading-service", heartbeat_at=now - timedelta(seconds=61))
    ok, _ = check_heartbeat(database.path, now=now, max_age_seconds=60)
    assert ok is False
    database.set_runtime_metadata("heartbeat:trading-service", "not-a-time")
    ok, _ = check_heartbeat(database.path, now=now, max_age_seconds=60)
    assert ok is False


def test_cli_exit_codes(tmp_path, capsys):
    database = make_database(tmp_path)
    now = datetime.now(UTC)
    database.record_heartbeat("trading-service", heartbeat_at=now)
    assert main(["--db", str(database.path), "--max-age-seconds", "60"]) == 0
    assert "healthy" in capsys.readouterr().out
    assert main(["--db", str(tmp_path / "missing.sqlite3")]) != 0


def test_database_error_is_unhealthy(tmp_path):
    broken = tmp_path / "broken.sqlite3"
    broken.write_text("not a sqlite database", encoding="utf-8")
    ok, message = check_heartbeat(broken)
    assert ok is False
    assert "failed" in message


def test_non_finite_max_age_is_unhealthy(tmp_path):
    database = make_database(tmp_path)
    ok, message = check_heartbeat(database.path, max_age_seconds=float("nan"))
    assert ok is False
    assert "invalid max age" in message
