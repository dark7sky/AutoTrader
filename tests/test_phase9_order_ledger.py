import sqlite3
from datetime import datetime, timedelta, timezone

from kis_ai_scalper.market.tick import MarketTick, MinuteBar
from kis_ai_scalper.market.clock import KST
from kis_ai_scalper.storage import connect_database


UTC = timezone.utc


def make_bar(index: int, symbol: str = "005930") -> MinuteBar:
    start = datetime(2026, 8, 18, 9, 0, tzinfo=UTC) + timedelta(minutes=index)
    return MinuteBar(symbol, start, 100 + index, 101 + index, 99 + index, 100 + index, 100)


def test_connection_pragmas_and_new_schema(tmp_path):
    path = tmp_path / "ledger.sqlite3"
    with connect_database(path) as database:
        database.init_schema()
        assert database.connection.execute("PRAGMA journal_mode").fetchone()[0].lower() == "wal"
        assert database.connection.execute("PRAGMA foreign_keys").fetchone()[0] == 1
        assert database.connection.execute("PRAGMA busy_timeout").fetchone()[0] >= 1000
        tables = {
            row[0]
            for row in database.connection.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            )
        }
        assert {"broker_orders", "broker_fills", "runtime_metadata", "service_leases"} <= tables


def test_memory_database_does_not_request_wal_and_foreign_keys_are_enabled():
    with connect_database(":memory:") as database:
        database.init_schema()
        assert database.connection.execute("PRAGMA journal_mode").fetchone()[0].lower() != "wal"
        assert database.connection.execute("PRAGMA foreign_keys").fetchone()[0] == 1


def test_init_schema_migrates_legacy_runtime_control(tmp_path):
    path = tmp_path / "legacy.sqlite3"
    connection = sqlite3.connect(path)
    connection.execute(
        """CREATE TABLE runtime_control (
           control_id INTEGER PRIMARY KEY CHECK (control_id = 1),
           paused INTEGER NOT NULL,
           updated_at TEXT NOT NULL,
           reason TEXT NOT NULL,
           source TEXT NOT NULL
        )"""
    )
    connection.execute(
        "INSERT INTO runtime_control VALUES (1, 1, '2026-08-18T00:00:00+00:00', 'legacy', 'test')"
    )
    connection.commit()
    connection.close()

    with connect_database(path) as database:
        database.init_schema()
        database.init_schema()
        control = database.get_runtime_control()
        assert control.paused is True
        assert control.environment == "demo"
        assert database.get_broker_order("missing") is None


def test_order_claim_status_and_restart_idempotent(tmp_path):
    path = tmp_path / "orders.sqlite3"
    first = connect_database(path)
    first.init_schema()
    second = connect_database(path)
    second.init_schema()
    kwargs = dict(
        client_order_id="client-1",
        signal_id="signal-1",
        symbol="005930",
        side="BUY",
        requested_qty=10,
        requested_price=100.0,
    )
    assert first.claim_order_intent(**kwargs) is True
    assert second.claim_order_intent(**kwargs) is False
    assert first.mark_order_submitting("client-1") is True
    assert first.record_order_submission("client-1", "broker-1") is True
    assert first.mark_order_unknown("client-1", "response timeout") is True
    assert first.update_broker_order_status(
        "client-1", "ACKNOWLEDGED", broker_order_id="broker-1"
    ) is True
    assert first.get_broker_order("client-1")["status"] == "ACKNOWLEDGED"

    first.close()
    second.close()
    with connect_database(path) as restarted:
        restarted.init_schema()
        assert restarted.claim_order_intent(**kwargs) is False
        assert restarted.get_broker_order("client-1")["broker_order_id"] == "broker-1"


def test_partial_fills_are_weighted_and_duplicate_fill_is_noop(tmp_path):
    path = tmp_path / "fills.sqlite3"
    fill_time = datetime(2026, 8, 18, 9, 1, tzinfo=UTC)
    with connect_database(path) as database:
        database.init_schema()
        assert database.claim_order_intent(
            client_order_id="client-2",
            signal_id="signal-2",
            symbol="005930",
            side="BUY",
            requested_qty=10,
            requested_price=100.0,
        )
        assert database.apply_broker_fill(
            fill_id="fill-1",
            client_order_id="client-2",
            quantity=4,
            price=100.0,
            filled_at=fill_time,
            broker_order_id="broker-2",
        ) is True
        assert database.get_broker_order("client-2")["status"] == "PARTIALLY_FILLED"
        assert database.apply_broker_fill(
            fill_id="fill-2",
            client_order_id="client-2",
            quantity=6,
            price=110.0,
            filled_at=fill_time + timedelta(seconds=1),
            broker_order_id="broker-2",
        ) is True
        order = database.get_broker_order("client-2")
        assert order["status"] == "FILLED"
        assert order["filled_qty"] == 10
        assert order["avg_fill_price"] == 106.0
        assert database.apply_broker_fill(
            fill_id="fill-2",
            client_order_id="client-2",
            quantity=6,
            price=110.0,
            filled_at=fill_time + timedelta(seconds=1),
            broker_order_id="broker-2",
        ) is False
        assert len(database.list_broker_fills("client-2")) == 2


def test_runtime_metadata_telegram_offset_and_heartbeat(tmp_path):
    with connect_database(tmp_path / "metadata.sqlite3") as database:
        database.init_schema()
        assert database.get_runtime_metadata("missing") is None
        database.set_runtime_metadata("test.key", "value")
        assert database.get_runtime_metadata("test.key") == "value"
        database.set_telegram_update_offset(42)
        assert database.get_telegram_update_offset() == 42
        heartbeat_at = datetime(2026, 8, 18, 9, 0, tzinfo=UTC)
        database.record_heartbeat("trading-service", heartbeat_at=heartbeat_at)
        assert database.get_heartbeat("trading-service") == heartbeat_at.isoformat()


def test_service_lease_is_owner_checked_and_expiry_aware(tmp_path):
    start = datetime(2026, 8, 18, 9, 0, tzinfo=UTC)
    with connect_database(tmp_path / "leases.sqlite3") as database:
        database.init_schema()
        assert database.acquire_service_lease("trading", "owner-a", 30, now=start) is True
        assert database.acquire_service_lease("trading", "owner-b", 30, now=start) is False
        assert database.renew_service_lease("trading", "owner-b", 30, now=start) is False
        assert database.renew_service_lease(
            "trading", "owner-a", 30, now=start + timedelta(seconds=10)
        ) is True
        assert database.release_service_lease("trading", "owner-b") is False
        assert database.acquire_service_lease(
            "trading", "owner-b", 30, now=start + timedelta(seconds=20)
        ) is False
        assert database.acquire_service_lease(
            "trading", "owner-b", 30, now=start + timedelta(seconds=41)
        ) is True
        assert database.release_service_lease("trading", "owner-b") is True


def test_recent_bars_and_retention_cleanup(tmp_path):
    with connect_database(tmp_path / "retention.sqlite3") as database:
        database.init_schema()
        for index in range(3):
            database.save_bar(make_bar(index))
            database.save_tick(
                MarketTick("005930", make_bar(index).start, 100 + index, 10),
                received_at=make_bar(index).start,
            )
        assert [bar.start for bar in database.load_bars("005930", limit=2)] == [
            make_bar(1).start.astimezone(KST).replace(tzinfo=None),
            make_bar(2).start.astimezone(KST).replace(tzinfo=None),
        ]
        cutoff = make_bar(1).start
        assert database.delete_old_bars(cutoff) == 1
        assert database.delete_old_ticks(cutoff) == 1
        assert len(database.load_recent_bars("005930", 10)) == 2
        assert len(database.load_ticks("005930")) == 2
