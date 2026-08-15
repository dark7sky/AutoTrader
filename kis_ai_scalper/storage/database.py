"""SQLite persistence for raw ticks, completed bars, and analysis results."""

from __future__ import annotations

import json
import sqlite3
from datetime import datetime
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from kis_ai_scalper.market.tick import MinuteBar
from kis_ai_scalper.market.tick import MarketTick
from kis_ai_scalper.risk import PositionState
from kis_ai_scalper.strategies.candidate import CandidateSignal


SCHEMA = """
CREATE TABLE IF NOT EXISTS market_ticks (
    symbol TEXT NOT NULL,
    timestamp TEXT NOT NULL,
    price REAL NOT NULL,
    volume INTEGER NOT NULL,
    received_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_market_ticks_symbol_timestamp
    ON market_ticks(symbol, timestamp);
CREATE TABLE IF NOT EXISTS bars_1m (
    symbol TEXT NOT NULL,
    start TEXT NOT NULL,
    open REAL NOT NULL,
    high REAL NOT NULL,
    low REAL NOT NULL,
    close REAL NOT NULL,
    volume INTEGER NOT NULL,
    UNIQUE(symbol, start)
);
CREATE TABLE IF NOT EXISTS candidate_signals (
    symbol TEXT NOT NULL,
    bar_start TEXT NOT NULL,
    strategy TEXT NOT NULL,
    score REAL NOT NULL,
    reason TEXT NOT NULL,
    features_json TEXT NOT NULL,
    created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS system_events (
    created_at TEXT NOT NULL,
    level TEXT NOT NULL,
    component TEXT NOT NULL,
    message TEXT NOT NULL,
    details_json TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS paper_orders (
    order_id TEXT PRIMARY KEY,
    signal_id TEXT NOT NULL UNIQUE,
    symbol TEXT NOT NULL,
    side TEXT NOT NULL,
    quantity INTEGER NOT NULL,
    price REAL NOT NULL,
    status TEXT NOT NULL,
    created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS paper_fills (
    fill_id TEXT PRIMARY KEY,
    order_id TEXT NOT NULL UNIQUE,
    signal_id TEXT NOT NULL UNIQUE,
    symbol TEXT NOT NULL,
    side TEXT NOT NULL,
    quantity INTEGER NOT NULL,
    price REAL NOT NULL,
    filled_at TEXT NOT NULL,
    FOREIGN KEY(order_id) REFERENCES paper_orders(order_id)
);
CREATE TABLE IF NOT EXISTS runtime_control (
    control_id INTEGER PRIMARY KEY CHECK (control_id = 1),
    paused INTEGER NOT NULL CHECK (paused IN (0, 1)),
    runtime_env TEXT NOT NULL DEFAULT 'demo',
    updated_at TEXT NOT NULL,
    reason TEXT NOT NULL,
    source TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS broker_order_audits (
    audit_id TEXT PRIMARY KEY,
    signal_id TEXT,
    symbol TEXT NOT NULL,
    side TEXT NOT NULL,
    quantity INTEGER NOT NULL,
    price REAL NOT NULL,
    status TEXT NOT NULL,
    reason TEXT NOT NULL,
    broker_order_id TEXT,
    created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_broker_order_audits_signal_status
    ON broker_order_audits(signal_id, status);
CREATE TABLE IF NOT EXISTS watchlist (
    symbol TEXT PRIMARY KEY,
    enabled INTEGER NOT NULL CHECK (enabled IN (0, 1)),
    added_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS ai_decision_audits (
    decision_id TEXT PRIMARY KEY,
    symbol TEXT NOT NULL,
    action TEXT NOT NULL,
    confidence REAL NOT NULL,
    entry_price REAL,
    take_profit_price REAL,
    stop_loss_price REAL,
    risk_level TEXT NOT NULL,
    requires_operator_approval INTEGER NOT NULL CHECK (requires_operator_approval IN (0, 1)),
    rationale TEXT NOT NULL,
    created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS approval_requests (
    request_id TEXT PRIMARY KEY,
    symbol TEXT NOT NULL,
    decision_id TEXT,
    reason TEXT NOT NULL,
    status TEXT NOT NULL,
    created_at TEXT NOT NULL,
    resolved_at TEXT
);
CREATE TABLE IF NOT EXISTS live_positions (
    position_id TEXT PRIMARY KEY,
    signal_id TEXT NOT NULL UNIQUE,
    symbol TEXT NOT NULL,
    quantity INTEGER NOT NULL,
    entry_price REAL NOT NULL,
    stop_loss_price REAL NOT NULL,
    take_profit_price REAL NOT NULL,
    opened_at TEXT NOT NULL,
    status TEXT NOT NULL,
    entry_broker_order_id TEXT,
    exit_broker_order_id TEXT,
    closed_at TEXT,
    close_reason TEXT,
    max_holding_seconds INTEGER
);
CREATE INDEX IF NOT EXISTS idx_live_positions_symbol_status
    ON live_positions(symbol, status);
INSERT OR IGNORE INTO runtime_control(control_id, paused, runtime_env, updated_at, reason, source)
VALUES (1, 1, 'demo', '1970-01-01T00:00:00+00:00', 'default_paused', 'database_init');
"""


def _iso(value: datetime) -> str:
    return value.isoformat()


def _validate_paper_record(symbol: str, quantity: int, price: float) -> None:
    _validate_symbol(symbol)
    if isinstance(quantity, bool) or quantity <= 0:
        raise ValueError("quantity must be positive")
    if price <= 0:
        raise ValueError("price must be positive")


def _validate_symbol(symbol: str) -> None:
    if not symbol.strip():
        raise ValueError("symbol must not be empty")
    if not symbol.isdigit() or len(symbol) != 6:
        raise ValueError("symbol must be a six-digit domestic stock code")


@dataclass(frozen=True)
class RuntimeControl:
    paused: bool
    updated_at: str
    reason: str
    source: str
    environment: str = "demo"


class Database:
    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        self._connection: sqlite3.Connection | None = None

    def connect(self) -> "Database":
        if self._connection is None:
            if self.path != Path(":memory:"):
                self.path.parent.mkdir(parents=True, exist_ok=True)
            self._connection = sqlite3.connect(str(self.path))
            self._connection.row_factory = sqlite3.Row
        return self

    @property
    def connection(self) -> sqlite3.Connection:
        if self._connection is None:
            self.connect()
        assert self._connection is not None
        return self._connection

    def init_schema(self) -> None:
        self.connection.executescript(SCHEMA)
        self._migrate_schema()
        self.connection.commit()

    def _migrate_schema(self) -> None:
        runtime_columns = {
            str(row["name"])
            for row in self.connection.execute("PRAGMA table_info(runtime_control)").fetchall()
        }
        if "runtime_env" not in runtime_columns:
            self.connection.execute(
                "ALTER TABLE runtime_control ADD COLUMN runtime_env TEXT NOT NULL DEFAULT 'demo'"
            )

    def set_runtime_paused(self, paused: bool, reason: str, source: str) -> RuntimeControl:
        """Persist the operator gate; this never talks to a broker."""
        if not isinstance(paused, bool):
            raise TypeError("paused must be a bool")
        if not reason or not source:
            raise ValueError("reason and source are required")
        updated_at = datetime.now().astimezone().isoformat()
        with self.connection:
            self.connection.execute(
                """INSERT INTO runtime_control(control_id,paused,runtime_env,updated_at,reason,source)
                   VALUES(1,?,'demo',?,?,?)
                   ON CONFLICT(control_id) DO UPDATE SET
                   paused=excluded.paused, updated_at=excluded.updated_at,
                   reason=excluded.reason, source=excluded.source""",
                (int(paused), updated_at, reason, source),
            )
        control = self.get_runtime_control()
        return RuntimeControl(paused, updated_at, reason, source, control.environment)

    def set_runtime_environment(self, environment: str, reason: str, source: str) -> RuntimeControl:
        """Switch the target broker environment only while the runtime is paused."""
        if environment not in {"demo", "real"}:
            raise ValueError("runtime environment must be demo or real")
        if not reason or not source:
            raise ValueError("reason and source are required")
        control = self.get_runtime_control()
        if not control.paused:
            raise ValueError("runtime environment can only be changed while paused")
        updated_at = datetime.now().astimezone().isoformat()
        with self.connection:
            self.connection.execute(
                """INSERT INTO runtime_control(control_id,paused,runtime_env,updated_at,reason,source)
                   VALUES(1,1,?,?,?,?)
                   ON CONFLICT(control_id) DO UPDATE SET
                   runtime_env=excluded.runtime_env, updated_at=excluded.updated_at,
                   reason=excluded.reason, source=excluded.source""",
                (environment, updated_at, reason, source),
            )
        return RuntimeControl(True, updated_at, reason, source, environment)

    def get_runtime_control(self) -> RuntimeControl:
        row = self.connection.execute(
            "SELECT paused, runtime_env, updated_at, reason, source FROM runtime_control WHERE control_id=1"
        ).fetchone()
        if row is None:
            return RuntimeControl(True, "1970-01-01T00:00:00+00:00", "default_paused", "database_init")
        return RuntimeControl(
            bool(row["paused"]),
            row["updated_at"],
            row["reason"],
            row["source"],
            str(row["runtime_env"] or "demo"),
        )

    def record_broker_order_audit(
        self, *, audit_id: str, signal_id: str | None, symbol: str, side: str,
        quantity: int, price: float, status: str, reason: str,
        broker_order_id: str | None = None, created_at: datetime | None = None,
    ) -> bool:
        """Persist one broker-order decision without storing credentials or tokens."""
        _validate_paper_record(symbol, quantity, price)
        if not audit_id or not side or not status or not reason:
            raise ValueError("audit_id, side, status, and reason are required")
        timestamp = _iso(created_at or datetime.now().astimezone())
        with self.connection:
            cursor = self.connection.execute(
                """INSERT OR IGNORE INTO broker_order_audits
                   (audit_id,signal_id,symbol,side,quantity,price,status,reason,broker_order_id,created_at)
                   VALUES(?,?,?,?,?,?,?,?,?,?)""",
                (
                    audit_id, signal_id, symbol, side, quantity, price,
                    status, reason, broker_order_id, timestamp,
                ),
            )
        return cursor.rowcount > 0

    def broker_signal_submitted(self, signal_id: str) -> bool:
        row = self.connection.execute(
            """SELECT 1 FROM broker_order_audits
               WHERE signal_id=? AND status='SUBMITTED' LIMIT 1""",
            (signal_id,),
        ).fetchone()
        return row is not None

    def list_broker_order_audits(self, symbol: str | None = None) -> list[sqlite3.Row]:
        query = "SELECT * FROM broker_order_audits"
        params: tuple[str, ...] = ()
        if symbol is not None:
            query += " WHERE symbol=?"
            params = (symbol,)
        return self.connection.execute(query + " ORDER BY created_at, audit_id", params).fetchall()

    def add_watchlist_symbol(self, symbol: str, enabled: bool = True,
                             added_at: datetime | None = None) -> bool:
        _validate_symbol(symbol)
        timestamp = _iso(added_at or datetime.now().astimezone())
        with self.connection:
            cursor = self.connection.execute(
                """INSERT OR IGNORE INTO watchlist(symbol,enabled,added_at)
                   VALUES(?,?,?)""",
                (symbol, int(enabled), timestamp),
            )
            if cursor.rowcount == 0:
                self.connection.execute(
                    "UPDATE watchlist SET enabled=? WHERE symbol=?",
                    (int(enabled), symbol),
                )
                return False
        return True

    def set_watchlist_enabled(self, symbol: str, enabled: bool) -> bool:
        _validate_symbol(symbol)
        with self.connection:
            cursor = self.connection.execute(
                "UPDATE watchlist SET enabled=? WHERE symbol=?",
                (int(enabled), symbol),
            )
        return cursor.rowcount > 0

    def list_watchlist_symbols(self, enabled_only: bool = True) -> list[str]:
        query = "SELECT symbol FROM watchlist"
        if enabled_only:
            query += " WHERE enabled=1"
        rows = self.connection.execute(query + " ORDER BY symbol").fetchall()
        return [str(row["symbol"]) for row in rows]

    def record_ai_decision(
        self, *, decision_id: str, symbol: str, action: str, confidence: float,
        entry_price: float | None, take_profit_price: float | None,
        stop_loss_price: float | None, risk_level: str,
        requires_operator_approval: bool, rationale: str,
        created_at: datetime | None = None,
    ) -> bool:
        _validate_symbol(symbol)
        if not decision_id or not action or not risk_level or not rationale:
            raise ValueError("decision_id, action, risk_level, and rationale are required")
        timestamp = _iso(created_at or datetime.now().astimezone())
        with self.connection:
            cursor = self.connection.execute(
                """INSERT OR IGNORE INTO ai_decision_audits
                   (decision_id,symbol,action,confidence,entry_price,take_profit_price,
                    stop_loss_price,risk_level,requires_operator_approval,rationale,created_at)
                   VALUES(?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    decision_id, symbol, action, confidence, entry_price,
                    take_profit_price, stop_loss_price, risk_level,
                    int(requires_operator_approval), rationale, timestamp,
                ),
            )
        return cursor.rowcount > 0

    def record_approval_request(
        self, *, request_id: str, symbol: str, decision_id: str | None,
        reason: str, status: str = "PENDING",
        created_at: datetime | None = None,
    ) -> bool:
        _validate_symbol(symbol)
        if not request_id or not reason or not status:
            raise ValueError("request_id, reason, and status are required")
        timestamp = _iso(created_at or datetime.now().astimezone())
        with self.connection:
            cursor = self.connection.execute(
                """INSERT OR IGNORE INTO approval_requests
                   (request_id,symbol,decision_id,reason,status,created_at,resolved_at)
                   VALUES(?,?,?,?,?,?,NULL)""",
                (request_id, symbol, decision_id, reason, status, timestamp),
            )
        return cursor.rowcount > 0

    def open_live_position(
        self, *, position_id: str, signal_id: str, symbol: str, quantity: int,
        entry_price: float, stop_loss_price: float, take_profit_price: float,
        opened_at: datetime, entry_broker_order_id: str | None,
        max_holding_seconds: int | None = None,
    ) -> bool:
        _validate_paper_record(symbol, quantity, entry_price)
        if not position_id or not signal_id:
            raise ValueError("position_id and signal_id are required")
        if not stop_loss_price < entry_price < take_profit_price:
            raise ValueError("position prices must satisfy stop < entry < take_profit")
        with self.connection:
            cursor = self.connection.execute(
                """INSERT OR IGNORE INTO live_positions
                   (position_id,signal_id,symbol,quantity,entry_price,stop_loss_price,
                    take_profit_price,opened_at,status,entry_broker_order_id,
                    exit_broker_order_id,closed_at,close_reason,max_holding_seconds)
                   VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    position_id, signal_id, symbol, quantity, entry_price,
                    stop_loss_price, take_profit_price, _iso(opened_at), "OPEN",
                    entry_broker_order_id, None, None, None, max_holding_seconds,
                ),
            )
        return cursor.rowcount > 0

    def close_live_position(
        self, *, position_id: str, exit_broker_order_id: str | None,
        close_reason: str, closed_at: datetime | None = None,
    ) -> bool:
        if not position_id or not close_reason:
            raise ValueError("position_id and close_reason are required")
        timestamp = _iso(closed_at or datetime.now().astimezone())
        with self.connection:
            cursor = self.connection.execute(
                """UPDATE live_positions
                   SET status='CLOSED', exit_broker_order_id=?, closed_at=?, close_reason=?
                   WHERE position_id=? AND status='OPEN'""",
                (exit_broker_order_id, timestamp, close_reason, position_id),
            )
        return cursor.rowcount > 0

    def list_open_live_positions(self, symbol: str | None = None) -> list[sqlite3.Row]:
        query = "SELECT * FROM live_positions WHERE status='OPEN'"
        params: tuple[str, ...] = ()
        if symbol is not None:
            query += " AND symbol=?"
            params = (symbol,)
        return self.connection.execute(query + " ORDER BY opened_at, position_id", params).fetchall()

    def save_bar(self, bar: MinuteBar) -> None:
        self.connection.execute(
            """INSERT INTO bars_1m(symbol,start,open,high,low,close,volume)
               VALUES(?,?,?,?,?,?,?)
               ON CONFLICT(symbol,start) DO UPDATE SET
               open=excluded.open, high=excluded.high, low=excluded.low,
               close=excluded.close, volume=excluded.volume""",
            (bar.symbol, _iso(bar.start), bar.open, bar.high, bar.low, bar.close, bar.volume),
        )
        self.connection.commit()

    def save_tick(self, tick: MarketTick, received_at: datetime | None = None) -> None:
        received = received_at or datetime.now().astimezone()
        self.connection.execute(
            """INSERT INTO market_ticks(symbol,timestamp,price,volume,received_at)
               VALUES(?,?,?,?,?)""",
            (tick.symbol, _iso(tick.timestamp), tick.price, tick.volume, _iso(received)),
        )
        self.connection.commit()

    def save_candidate(self, candidate: CandidateSignal, bar_start: datetime) -> None:
        self.connection.execute(
            """INSERT INTO candidate_signals
               (symbol,bar_start,strategy,score,reason,features_json,created_at)
               VALUES(?,?,?,?,?,?,?)""",
            (candidate.symbol, _iso(bar_start), candidate.strategy, candidate.score,
             candidate.reason, json.dumps(candidate.features, sort_keys=True),
             datetime.now().astimezone().isoformat()),
        )
        self.connection.commit()

    def log_event(self, level: str, component: str, message: str,
                  details: dict[str, Any] | None = None) -> None:
        self.connection.execute(
            "INSERT INTO system_events(created_at,level,component,message,details_json) VALUES(?,?,?,?,?)",
            (datetime.now().astimezone().isoformat(), level, component, message,
             json.dumps(details or {}, sort_keys=True)),
        )
        self.connection.commit()

    def paper_signal_exists(self, signal_id: str) -> bool:
        row = self.connection.execute(
            "SELECT 1 FROM paper_orders WHERE signal_id=? LIMIT 1", (signal_id,)
        ).fetchone()
        return row is not None

    def record_paper_buy(
        self, *, order_id: str, fill_id: str, signal_id: str, symbol: str,
        quantity: int, price: float, created_at: datetime | None = None,
    ) -> bool:
        """Persist one simulated BUY order and fill, returning False for a duplicate signal."""
        _validate_paper_record(symbol, quantity, price)
        timestamp = _iso(created_at or datetime.now().astimezone())
        with self.connection:
            cursor = self.connection.execute(
                """INSERT OR IGNORE INTO paper_orders
                   (order_id,signal_id,symbol,side,quantity,price,status,created_at)
                   VALUES(?,?,?,?,?,?,?,?)""",
                (order_id, signal_id, symbol, "BUY", quantity, price, "FILLED", timestamp),
            )
            if cursor.rowcount == 0:
                return False
            self.connection.execute(
                """INSERT INTO paper_fills
                   (fill_id,order_id,signal_id,symbol,side,quantity,price,filled_at)
                   VALUES(?,?,?,?,?,?,?,?)""",
                (fill_id, order_id, signal_id, symbol, "BUY", quantity, price, timestamp),
            )
        return True

    def record_paper_sell(
        self, *, order_id: str, fill_id: str, signal_id: str, symbol: str,
        quantity: int, price: float, created_at: datetime | None = None,
    ) -> bool:
        """Persist one simulated SELL order and fill for local journal replay."""
        _validate_paper_record(symbol, quantity, price)
        if self.paper_signal_exists(signal_id):
            return False
        position = next((item for item in self.paper_positions() if item.symbol == symbol), None)
        if position is None or quantity > position.quantity:
            raise ValueError(f"paper sell exceeds long position for {symbol}")
        timestamp = _iso(created_at or datetime.now().astimezone())
        with self.connection:
            cursor = self.connection.execute(
                """INSERT OR IGNORE INTO paper_orders
                   (order_id,signal_id,symbol,side,quantity,price,status,created_at)
                   VALUES(?,?,?,?,?,?,?,?)""",
                (order_id, signal_id, symbol, "SELL", quantity, price, "FILLED", timestamp),
            )
            if cursor.rowcount == 0:
                return False
            self.connection.execute(
                """INSERT INTO paper_fills
                   (fill_id,order_id,signal_id,symbol,side,quantity,price,filled_at)
                   VALUES(?,?,?,?,?,?,?,?)""",
                (fill_id, order_id, signal_id, symbol, "SELL", quantity, price, timestamp),
            )
        return True

    def list_paper_orders(self, symbol: str | None = None) -> list[sqlite3.Row]:
        query = "SELECT * FROM paper_orders"
        params: tuple[str, ...] = ()
        if symbol is not None:
            query += " WHERE symbol=?"
            params = (symbol,)
        return self.connection.execute(query + " ORDER BY created_at, order_id", params).fetchall()

    def list_paper_fills(self, symbol: str | None = None) -> list[sqlite3.Row]:
        query = "SELECT * FROM paper_fills"
        params: tuple[str, ...] = ()
        if symbol is not None:
            query += " WHERE symbol=?"
            params = (symbol,)
        return self.connection.execute(query + " ORDER BY filled_at, fill_id", params).fetchall()

    def paper_positions(self) -> tuple[PositionState, ...]:
        rows = self.connection.execute(
            """SELECT
                   symbol,
                   SUM(CASE WHEN side='BUY' THEN quantity ELSE -quantity END) AS quantity,
                   SUM(CASE WHEN side='BUY' THEN quantity * price ELSE 0 END) AS buy_value,
                   SUM(CASE WHEN side='BUY' THEN quantity ELSE 0 END) AS buy_quantity
               FROM paper_fills
               GROUP BY symbol
               HAVING quantity > 0
               ORDER BY symbol"""
        ).fetchall()
        positions: list[PositionState] = []
        for row in rows:
            buy_quantity = row["buy_quantity"] or 0
            avg_price = (row["buy_value"] / buy_quantity) if buy_quantity else 0
            positions.append(PositionState(row["symbol"], int(row["quantity"]), avg_price))
        return tuple(positions)

    def load_bars(self, symbol: str) -> list[MinuteBar]:
        rows = self.connection.execute(
            "SELECT symbol,start,open,high,low,close,volume FROM bars_1m WHERE symbol=? ORDER BY start",
            (symbol,),
        ).fetchall()
        return [MinuteBar(row["symbol"], datetime.fromisoformat(row["start"]),
                          row["open"], row["high"], row["low"], row["close"], row["volume"])
                for row in rows]

    def load_ticks(self, symbol: str) -> list[MarketTick]:
        rows = self.connection.execute(
            "SELECT symbol,timestamp,price,volume FROM market_ticks "
            "WHERE symbol=? ORDER BY timestamp, rowid",
            (symbol,),
        ).fetchall()
        return [MarketTick(row["symbol"], datetime.fromisoformat(row["timestamp"]),
                           row["price"], row["volume"]) for row in rows]

    def latest_tick(self, symbol: str) -> MarketTick | None:
        row = self.connection.execute(
            "SELECT symbol,timestamp,price,volume FROM market_ticks "
            "WHERE symbol=? ORDER BY timestamp DESC, rowid DESC LIMIT 1",
            (symbol,),
        ).fetchone()
        if row is None:
            return None
        return MarketTick(
            row["symbol"], datetime.fromisoformat(row["timestamp"]), row["price"], row["volume"]
        )

    def latest_bar(self, symbol: str) -> MinuteBar | None:
        row = self.connection.execute(
            "SELECT symbol,start,open,high,low,close,volume FROM bars_1m "
            "WHERE symbol=? ORDER BY start DESC LIMIT 1",
            (symbol,),
        ).fetchone()
        if row is None:
            return None
        return MinuteBar(
            row["symbol"],
            datetime.fromisoformat(row["start"]),
            row["open"],
            row["high"],
            row["low"],
            row["close"],
            row["volume"],
        )

    def close(self) -> None:
        if self._connection is not None:
            self._connection.close()
            self._connection = None

    def __enter__(self) -> "Database":
        return self.connect()

    def __exit__(self, *_: object) -> None:
        self.close()


def connect_database(path: str | Path = "data/kis_ai_scalper.sqlite3") -> Database:
    return Database(path).connect()
