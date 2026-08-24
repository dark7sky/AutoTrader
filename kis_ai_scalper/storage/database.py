"""SQLite persistence for raw ticks, completed bars, and analysis results."""

from __future__ import annotations

import json
import math
import sqlite3
from datetime import datetime, timedelta
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from kis_ai_scalper.market.tick import MinuteBar
from kis_ai_scalper.market.tick import MarketTick
from kis_ai_scalper.market.clock import KST
from kis_ai_scalper.risk import PositionState
from kis_ai_scalper.strategies.candidate import CandidateSignal


BUSY_TIMEOUT_MS = 5_000
BROKER_ORDER_STATUSES = frozenset({
    "INTENT",
    "SUBMITTING",
    "ACKNOWLEDGED",
    "PARTIALLY_FILLED",
    "FILLED",
    "CANCEL_PENDING",
    "CANCELLED",
    "REJECTED",
    "UNKNOWN",
})
TERMINAL_BROKER_ORDER_STATUSES = frozenset({"FILLED", "CANCELLED", "REJECTED"})
TELEGRAM_UPDATE_OFFSET_KEY = "telegram.update_offset"


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
    created_at TEXT NOT NULL,
    strategy TEXT NOT NULL DEFAULT 'UNKNOWN',
    model TEXT NOT NULL DEFAULT 'UNKNOWN',
    prompt_version TEXT NOT NULL DEFAULT 'UNKNOWN',
    max_holding_seconds INTEGER
);
CREATE TABLE IF NOT EXISTS approval_requests (
    request_id TEXT PRIMARY KEY,
    symbol TEXT NOT NULL,
    decision_id TEXT,
    reason TEXT NOT NULL,
    status TEXT NOT NULL,
    created_at TEXT NOT NULL,
    resolved_at TEXT,
    expires_at TEXT,
    resolved_by TEXT,
    consumed_at TEXT,
    signal_id TEXT,
    quantity INTEGER,
    entry_price REAL,
    take_profit_price REAL,
    stop_loss_price REAL,
    max_holding_seconds INTEGER
);
CREATE INDEX IF NOT EXISTS idx_approval_requests_status_created
    ON approval_requests(status, created_at);
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
CREATE TABLE IF NOT EXISTS broker_orders (
    client_order_id TEXT PRIMARY KEY,
    signal_id TEXT,
    symbol TEXT NOT NULL,
    side TEXT NOT NULL CHECK (side IN ('BUY', 'SELL')),
    requested_qty INTEGER NOT NULL CHECK (requested_qty > 0),
    requested_price REAL NOT NULL CHECK (requested_price > 0),
    status TEXT NOT NULL CHECK (status IN (
        'INTENT', 'SUBMITTING', 'ACKNOWLEDGED', 'PARTIALLY_FILLED', 'FILLED',
        'CANCEL_PENDING', 'CANCELLED', 'REJECTED', 'UNKNOWN'
    )),
    broker_order_id TEXT,
    filled_qty INTEGER NOT NULL DEFAULT 0 CHECK (filled_qty >= 0),
    avg_fill_price REAL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    submitted_at TEXT,
    completed_at TEXT,
    error TEXT
);
CREATE INDEX IF NOT EXISTS idx_broker_orders_broker_order_id
    ON broker_orders(broker_order_id);
CREATE INDEX IF NOT EXISTS idx_broker_orders_signal_id
    ON broker_orders(signal_id);
CREATE TABLE IF NOT EXISTS broker_fills (
    fill_id TEXT PRIMARY KEY,
    client_order_id TEXT NOT NULL,
    broker_order_id TEXT,
    symbol TEXT NOT NULL,
    side TEXT NOT NULL CHECK (side IN ('BUY', 'SELL')),
    quantity INTEGER NOT NULL CHECK (quantity > 0),
    price REAL NOT NULL CHECK (price > 0),
    filled_at TEXT NOT NULL,
    created_at TEXT NOT NULL,
    FOREIGN KEY(client_order_id) REFERENCES broker_orders(client_order_id)
);
CREATE INDEX IF NOT EXISTS idx_broker_fills_client_order_id
    ON broker_fills(client_order_id, filled_at);
CREATE TABLE IF NOT EXISTS live_position_fill_applications (
    fill_id TEXT PRIMARY KEY,
    client_order_id TEXT NOT NULL,
    applied_at TEXT NOT NULL,
    FOREIGN KEY(fill_id) REFERENCES broker_fills(fill_id),
    FOREIGN KEY(client_order_id) REFERENCES broker_orders(client_order_id)
);
CREATE TABLE IF NOT EXISTS runtime_metadata (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS service_leases (
    lease_name TEXT PRIMARY KEY,
    owner_id TEXT NOT NULL,
    acquired_at TEXT NOT NULL,
    renewed_at TEXT NOT NULL,
    expires_at TEXT NOT NULL
);
"""


def _iso(value: datetime) -> str:
    return value.isoformat()


def _canonical_market_time(value: datetime) -> datetime:
    """Store exchange timestamps as naive KST; audit timestamps keep their own contract."""
    if value.tzinfo is not None:
        return value.astimezone(KST).replace(tzinfo=None)
    return value


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


def _require_nonempty(value: str, name: str) -> None:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{name} must not be empty")


def _validate_order_status(status: str) -> None:
    if status not in BROKER_ORDER_STATUSES:
        allowed = ", ".join(sorted(BROKER_ORDER_STATUSES))
        raise ValueError(f"invalid broker order status {status!r}; expected one of: {allowed}")


def _validate_positive_number(value: int | float, name: str) -> None:
    if (
        isinstance(value, bool)
        or not isinstance(value, (int, float))
        or not math.isfinite(value)
        or value <= 0
    ):
        raise ValueError(f"{name} must be positive")


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
            is_memory = self.path == Path(":memory:")
            if not is_memory:
                self.path.parent.mkdir(parents=True, exist_ok=True)
            self._connection = sqlite3.connect(str(self.path))
            self._connection.row_factory = sqlite3.Row
            self._connection.execute(f"PRAGMA busy_timeout={BUSY_TIMEOUT_MS}")
            self._connection.execute("PRAGMA foreign_keys=ON")
            if not is_memory:
                self._connection.execute("PRAGMA journal_mode=WAL")
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
        self.connection.execute(
            """INSERT OR IGNORE INTO runtime_control
               (control_id,paused,runtime_env,updated_at,reason,source)
               VALUES (1,1,'demo','1970-01-01T00:00:00+00:00',
                       'default_paused','database_init')"""
        )
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
        decision_columns = {
            str(row["name"])
            for row in self.connection.execute("PRAGMA table_info(ai_decision_audits)").fetchall()
        }
        if "max_holding_seconds" not in decision_columns:
            self.connection.execute(
                "ALTER TABLE ai_decision_audits ADD COLUMN max_holding_seconds INTEGER"
            )
        decision_additions = {
            "strategy": "TEXT NOT NULL DEFAULT 'UNKNOWN'",
            "model": "TEXT NOT NULL DEFAULT 'UNKNOWN'",
            "prompt_version": "TEXT NOT NULL DEFAULT 'UNKNOWN'",
        }
        for name, type_name in decision_additions.items():
            if name not in decision_columns:
                self.connection.execute(
                    f"ALTER TABLE ai_decision_audits ADD COLUMN {name} {type_name}"
                )
        approval_columns = {
            str(row["name"])
            for row in self.connection.execute("PRAGMA table_info(approval_requests)").fetchall()
        }
        approval_additions = {
            "expires_at": "TEXT",
            "resolved_by": "TEXT",
            "consumed_at": "TEXT",
            "signal_id": "TEXT",
            "quantity": "INTEGER",
            "entry_price": "REAL",
            "take_profit_price": "REAL",
            "stop_loss_price": "REAL",
            "max_holding_seconds": "INTEGER",
        }
        for name, type_name in approval_additions.items():
            if name not in approval_columns:
                self.connection.execute(
                    f"ALTER TABLE approval_requests ADD COLUMN {name} {type_name}"
                )
        legacy_approvals = self.connection.execute(
            "SELECT request_id, created_at FROM approval_requests WHERE expires_at IS NULL"
        ).fetchall()
        for row in legacy_approvals:
            try:
                created = datetime.fromisoformat(str(row["created_at"]))
            except ValueError:
                continue
            self.connection.execute(
                "UPDATE approval_requests SET expires_at=? WHERE request_id=? AND expires_at IS NULL",
                (_iso(created + timedelta(minutes=2)), row["request_id"]),
            )

    def claim_order_intent(
        self,
        *,
        client_order_id: str,
        signal_id: str | None,
        symbol: str,
        side: str,
        requested_qty: int,
        requested_price: float,
        created_at: datetime | None = None,
    ) -> bool:
        """Atomically claim a client order id before making any broker request."""
        _require_nonempty(client_order_id, "client_order_id")
        if signal_id is not None:
            _require_nonempty(signal_id, "signal_id")
        _validate_symbol(symbol)
        _require_nonempty(side, "side")
        normalized_side = side.upper()
        if normalized_side not in {"BUY", "SELL"}:
            raise ValueError("side must be BUY or SELL")
        _validate_positive_number(requested_qty, "requested_qty")
        if not isinstance(requested_qty, int):
            raise ValueError("requested_qty must be an integer")
        _validate_positive_number(requested_price, "requested_price")
        timestamp = _iso(created_at or datetime.now().astimezone())
        with self.connection:
            cursor = self.connection.execute(
                """INSERT OR IGNORE INTO broker_orders
                   (client_order_id,signal_id,symbol,side,requested_qty,requested_price,
                    status,broker_order_id,filled_qty,avg_fill_price,created_at,updated_at,
                    submitted_at,completed_at,error)
                   VALUES(?,?,?,?,?,?,'INTENT',NULL,0,NULL,?,?,NULL,NULL,NULL)""",
                (
                    client_order_id,
                    signal_id,
                    symbol,
                    normalized_side,
                    requested_qty,
                    requested_price,
                    timestamp,
                    timestamp,
                ),
            )
        return cursor.rowcount > 0

    def mark_order_submitting(
        self, client_order_id: str, *, updated_at: datetime | None = None
    ) -> bool:
        _require_nonempty(client_order_id, "client_order_id")
        timestamp = _iso(updated_at or datetime.now().astimezone())
        with self.connection:
            cursor = self.connection.execute(
                """UPDATE broker_orders
                   SET status='SUBMITTING', updated_at=?, error=NULL
                   WHERE client_order_id=?""",
                (timestamp, client_order_id),
            )
        return cursor.rowcount > 0

    def record_order_submission(
        self,
        client_order_id: str,
        broker_order_id: str,
        *,
        submitted_at: datetime | None = None,
    ) -> bool:
        _require_nonempty(client_order_id, "client_order_id")
        _require_nonempty(broker_order_id, "broker_order_id")
        timestamp = _iso(submitted_at or datetime.now().astimezone())
        with self.connection:
            cursor = self.connection.execute(
                """UPDATE broker_orders
                   SET status='ACKNOWLEDGED', broker_order_id=?, submitted_at=?,
                       updated_at=?, error=NULL
                   WHERE client_order_id=?""",
                (broker_order_id, timestamp, timestamp, client_order_id),
            )
        return cursor.rowcount > 0

    def mark_order_unknown(
        self,
        client_order_id: str,
        error: str | None = None,
        *,
        updated_at: datetime | None = None,
    ) -> bool:
        _require_nonempty(client_order_id, "client_order_id")
        if error is not None:
            _require_nonempty(error, "error")
        timestamp = _iso(updated_at or datetime.now().astimezone())
        with self.connection:
            cursor = self.connection.execute(
                """UPDATE broker_orders
                   SET status='UNKNOWN', updated_at=?, error=?
                   WHERE client_order_id=?""",
                (timestamp, error, client_order_id),
            )
        return cursor.rowcount > 0

    def update_broker_order_status(
        self,
        client_order_id: str,
        status: str,
        *,
        broker_order_id: str | None = None,
        filled_qty: int | None = None,
        avg_fill_price: float | None = None,
        error: str | None = None,
        updated_at: datetime | None = None,
        completed_at: datetime | None = None,
    ) -> bool:
        """Update reconciled broker state while preserving omitted broker fields."""
        _require_nonempty(client_order_id, "client_order_id")
        _validate_order_status(status)
        if broker_order_id is not None:
            _require_nonempty(broker_order_id, "broker_order_id")
        if filled_qty is not None and (
            isinstance(filled_qty, bool) or not isinstance(filled_qty, int) or filled_qty < 0
        ):
            raise ValueError("filled_qty must be a non-negative integer")
        if avg_fill_price is not None:
            _validate_positive_number(avg_fill_price, "avg_fill_price")
        if error is not None:
            _require_nonempty(error, "error")
        timestamp = _iso(updated_at or datetime.now().astimezone())
        completion = None
        if status in TERMINAL_BROKER_ORDER_STATUSES:
            completion = _iso(completed_at or updated_at or datetime.now().astimezone())
        with self.connection:
            cursor = self.connection.execute(
                """UPDATE broker_orders
                   SET status=?, broker_order_id=COALESCE(?,broker_order_id),
                       filled_qty=COALESCE(?,filled_qty),
                       avg_fill_price=COALESCE(?,avg_fill_price),
                       updated_at=?, completed_at=?, error=?
                   WHERE client_order_id=?
                     AND (? IS NULL OR ? <= requested_qty)""",
                (
                    status,
                    broker_order_id,
                    filled_qty,
                    avg_fill_price,
                    timestamp,
                    completion,
                    error,
                    client_order_id,
                    filled_qty,
                    filled_qty,
                ),
            )
        return cursor.rowcount > 0

    def apply_broker_fill(
        self,
        *,
        fill_id: str,
        client_order_id: str,
        quantity: int,
        price: float,
        filled_at: datetime,
        broker_order_id: str | None = None,
        symbol: str | None = None,
        side: str | None = None,
        created_at: datetime | None = None,
    ) -> bool:
        """Insert one fill and atomically roll it into its order, once per fill id."""
        _require_nonempty(fill_id, "fill_id")
        _require_nonempty(client_order_id, "client_order_id")
        _validate_positive_number(quantity, "quantity")
        if not isinstance(quantity, int):
            raise ValueError("quantity must be an integer")
        _validate_positive_number(price, "price")
        if broker_order_id is not None:
            _require_nonempty(broker_order_id, "broker_order_id")
        if symbol is not None:
            _validate_symbol(symbol)
        if side is not None:
            _require_nonempty(side, "side")
        normalized_side = side.upper() if side is not None else None
        if normalized_side is not None and normalized_side not in {"BUY", "SELL"}:
            raise ValueError("side must be BUY or SELL")
        fill_timestamp = _iso(filled_at)
        created_timestamp = _iso(created_at or datetime.now().astimezone())

        with self.connection:
            order = self.connection.execute(
                "SELECT * FROM broker_orders WHERE client_order_id=?",
                (client_order_id,),
            ).fetchone()
            if order is None:
                raise ValueError(f"unknown client_order_id: {client_order_id}")
            if symbol is not None and symbol != order["symbol"]:
                raise ValueError("fill symbol does not match order")
            if normalized_side is not None and normalized_side != order["side"]:
                raise ValueError("fill side does not match order")
            if (
                broker_order_id is not None
                and order["broker_order_id"] is not None
                and broker_order_id != order["broker_order_id"]
            ):
                raise ValueError("fill broker_order_id does not match order")

            effective_broker_id = broker_order_id or order["broker_order_id"]
            cursor = self.connection.execute(
                """INSERT OR IGNORE INTO broker_fills
                   (fill_id,client_order_id,broker_order_id,symbol,side,quantity,price,
                    filled_at,created_at)
                   VALUES(?,?,?,?,?,?,?,?,?)""",
                (
                    fill_id,
                    client_order_id,
                    effective_broker_id,
                    order["symbol"],
                    order["side"],
                    quantity,
                    price,
                    fill_timestamp,
                    created_timestamp,
                ),
            )
            if cursor.rowcount == 0:
                return False
            updated = self.connection.execute(
                """UPDATE broker_orders
                   SET filled_qty=filled_qty+?,
                       avg_fill_price=((filled_qty * COALESCE(avg_fill_price,0)) + (? * ?))
                                      / (filled_qty + ?),
                       status=CASE WHEN filled_qty+? = requested_qty
                                   THEN 'FILLED' ELSE 'PARTIALLY_FILLED' END,
                       broker_order_id=COALESCE(broker_order_id,?),
                       updated_at=?,
                       completed_at=CASE WHEN filled_qty+? = requested_qty
                                         THEN ? ELSE NULL END,
                       error=NULL
                   WHERE client_order_id=? AND filled_qty+? <= requested_qty""",
                (
                    quantity,
                    quantity,
                    price,
                    quantity,
                    quantity,
                    effective_broker_id,
                    fill_timestamp,
                    quantity,
                    fill_timestamp,
                    client_order_id,
                    quantity,
                ),
            )
            if updated.rowcount == 0:
                raise ValueError("fill quantity exceeds requested order quantity")
        return True

    def materialize_order_fills(
        self,
        client_order_id: str,
        *,
        stop_loss_price: float | None = None,
        take_profit_price: float | None = None,
        max_holding_seconds: int | None = None,
        close_reason: str = "broker_fill",
    ) -> int:
        """Apply unmaterialized broker fills to live positions exactly once."""
        _require_nonempty(client_order_id, "client_order_id")
        if stop_loss_price is not None:
            _validate_positive_number(stop_loss_price, "stop_loss_price")
        if take_profit_price is not None:
            _validate_positive_number(take_profit_price, "take_profit_price")
        if max_holding_seconds is not None and (
            isinstance(max_holding_seconds, bool)
            or not isinstance(max_holding_seconds, int)
            or max_holding_seconds <= 0
        ):
            raise ValueError("max_holding_seconds must be positive")
        if not close_reason:
            raise ValueError("close_reason must not be empty")

        with self.connection:
            order = self.connection.execute(
                "SELECT * FROM broker_orders WHERE client_order_id=?",
                (client_order_id,),
            ).fetchone()
            if order is None:
                raise ValueError(f"unknown client_order_id: {client_order_id}")
            fills = self.connection.execute(
                """SELECT f.* FROM broker_fills f
                   LEFT JOIN live_position_fill_applications a ON a.fill_id=f.fill_id
                   WHERE f.client_order_id=? AND a.fill_id IS NULL
                   ORDER BY f.filled_at, f.fill_id""",
                (client_order_id,),
            ).fetchall()
            applied = 0
            for fill in fills:
                symbol = str(fill["symbol"])
                filled_at = datetime.fromisoformat(fill["filled_at"])
                position = self.connection.execute(
                    """SELECT * FROM live_positions
                       WHERE symbol=? AND status='OPEN'
                       ORDER BY opened_at, position_id LIMIT 1""",
                    (symbol,),
                ).fetchone()
                if fill["side"] == "BUY":
                    if position is None:
                        if (
                            stop_loss_price is None
                            or take_profit_price is None
                            or not stop_loss_price < float(fill["price"]) < take_profit_price
                        ):
                            raise ValueError("BUY fill requires a valid stop/take price ladder")
                        signal_id = str(order["signal_id"] or f"fill:{client_order_id}")
                        position_id = f"live-position:{signal_id}"
                        self.connection.execute(
                            """INSERT INTO live_positions
                               (position_id,signal_id,symbol,quantity,entry_price,
                                stop_loss_price,take_profit_price,opened_at,status,
                                entry_broker_order_id,exit_broker_order_id,closed_at,
                                close_reason,max_holding_seconds)
                               VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                            (
                                position_id,
                                signal_id,
                                symbol,
                                int(fill["quantity"]),
                                float(fill["price"]),
                                stop_loss_price,
                                take_profit_price,
                                _iso(filled_at),
                                "OPEN",
                                fill["broker_order_id"] or order["broker_order_id"],
                                None,
                                None,
                                None,
                                max_holding_seconds,
                            ),
                        )
                    else:
                        old_quantity = int(position["quantity"])
                        new_quantity = old_quantity + int(fill["quantity"])
                        new_average = (
                            old_quantity * float(position["entry_price"])
                            + int(fill["quantity"]) * float(fill["price"])
                        ) / new_quantity
                        self.connection.execute(
                            """UPDATE live_positions
                               SET quantity=?, entry_price=?
                               WHERE position_id=? AND status='OPEN'""",
                            (new_quantity, new_average, position["position_id"]),
                        )
                else:
                    if position is None:
                        raise ValueError("SELL fill has no open live position")
                    old_quantity = int(position["quantity"])
                    sell_quantity = int(fill["quantity"])
                    if sell_quantity > old_quantity:
                        raise ValueError("SELL fill exceeds open live position")
                    new_quantity = old_quantity - sell_quantity
                    if new_quantity == 0:
                        self.connection.execute(
                            """UPDATE live_positions
                               SET quantity=0, status='CLOSED',
                                   exit_broker_order_id=?, closed_at=?, close_reason=?
                               WHERE position_id=? AND status='OPEN'""",
                            (
                                fill["broker_order_id"] or order["broker_order_id"],
                                _iso(filled_at),
                                close_reason,
                                position["position_id"],
                            ),
                        )
                    else:
                        self.connection.execute(
                            """UPDATE live_positions
                               SET quantity=?, exit_broker_order_id=?
                               WHERE position_id=? AND status='OPEN'""",
                            (
                                new_quantity,
                                fill["broker_order_id"] or order["broker_order_id"],
                                position["position_id"],
                            ),
                        )
                self.connection.execute(
                    """INSERT INTO live_position_fill_applications
                       (fill_id,client_order_id,applied_at) VALUES(?,?,?)""",
                    (fill["fill_id"], client_order_id, _iso(datetime.now().astimezone())),
                )
                applied += 1
            return applied

    def get_broker_order(self, client_order_id: str) -> sqlite3.Row | None:
        _require_nonempty(client_order_id, "client_order_id")
        return self.connection.execute(
            "SELECT * FROM broker_orders WHERE client_order_id=?", (client_order_id,)
        ).fetchone()

    def list_broker_fills(self, client_order_id: str) -> list[sqlite3.Row]:
        _require_nonempty(client_order_id, "client_order_id")
        return self.connection.execute(
            """SELECT * FROM broker_fills WHERE client_order_id=?
               ORDER BY filled_at, fill_id""",
            (client_order_id,),
        ).fetchall()

    def get_runtime_metadata(self, key: str) -> str | None:
        _require_nonempty(key, "key")
        row = self.connection.execute(
            "SELECT value FROM runtime_metadata WHERE key=?", (key,)
        ).fetchone()
        return None if row is None else str(row["value"])

    def set_runtime_metadata(
        self, key: str, value: str, *, updated_at: datetime | None = None
    ) -> str:
        _require_nonempty(key, "key")
        if not isinstance(value, str):
            raise TypeError("value must be a string")
        timestamp = _iso(updated_at or datetime.now().astimezone())
        with self.connection:
            self.connection.execute(
                """INSERT INTO runtime_metadata(key,value,updated_at) VALUES(?,?,?)
                   ON CONFLICT(key) DO UPDATE SET
                   value=excluded.value, updated_at=excluded.updated_at""",
                (key, value, timestamp),
            )
        return timestamp

    def get_telegram_update_offset(self) -> int | None:
        value = self.get_runtime_metadata(TELEGRAM_UPDATE_OFFSET_KEY)
        return None if value is None else int(value)

    def set_telegram_update_offset(
        self, offset: int, *, updated_at: datetime | None = None
    ) -> str:
        if isinstance(offset, bool) or not isinstance(offset, int) or offset < 0:
            raise ValueError("Telegram update offset must be a non-negative integer")
        return self.set_runtime_metadata(
            TELEGRAM_UPDATE_OFFSET_KEY, str(offset), updated_at=updated_at
        )

    def set_heartbeat(
        self, key: str, *, heartbeat_at: datetime | None = None
    ) -> str:
        """Store an ISO heartbeat timestamp under an explicit metadata key."""
        heartbeat_time = heartbeat_at or datetime.now().astimezone()
        timestamp = _iso(heartbeat_time)
        self.set_runtime_metadata(key, timestamp, updated_at=heartbeat_time)
        return timestamp

    def record_heartbeat(
        self, component: str, *, heartbeat_at: datetime | None = None
    ) -> str:
        _require_nonempty(component, "component")
        return self.set_heartbeat(f"heartbeat:{component}", heartbeat_at=heartbeat_at)

    def get_heartbeat(self, component: str) -> str | None:
        _require_nonempty(component, "component")
        return self.get_runtime_metadata(f"heartbeat:{component}")

    def acquire_service_lease(
        self,
        lease_name: str,
        owner_id: str,
        ttl_seconds: int | float,
        *,
        now: datetime | None = None,
    ) -> bool:
        """Acquire an absent/expired lease, or refresh one already owned by this owner."""
        _require_nonempty(lease_name, "lease_name")
        _require_nonempty(owner_id, "owner_id")
        _validate_positive_number(ttl_seconds, "ttl_seconds")
        acquired = now or datetime.now().astimezone()
        acquired_at = _iso(acquired)
        expires_at = _iso(acquired + timedelta(seconds=ttl_seconds))
        with self.connection:
            cursor = self.connection.execute(
                """INSERT INTO service_leases
                   (lease_name,owner_id,acquired_at,renewed_at,expires_at)
                   VALUES(?,?,?,?,?)
                   ON CONFLICT(lease_name) DO UPDATE SET
                   owner_id=excluded.owner_id,
                   acquired_at=excluded.acquired_at,
                   renewed_at=excluded.renewed_at,
                   expires_at=excluded.expires_at
                   WHERE service_leases.owner_id=excluded.owner_id
                      OR julianday(service_leases.expires_at)
                         <= julianday(excluded.acquired_at)""",
                (lease_name, owner_id, acquired_at, acquired_at, expires_at),
            )
        return cursor.rowcount > 0

    def renew_service_lease(
        self,
        lease_name: str,
        owner_id: str,
        ttl_seconds: int | float,
        *,
        now: datetime | None = None,
    ) -> bool:
        _require_nonempty(lease_name, "lease_name")
        _require_nonempty(owner_id, "owner_id")
        _validate_positive_number(ttl_seconds, "ttl_seconds")
        renewed = now or datetime.now().astimezone()
        renewed_at = _iso(renewed)
        expires_at = _iso(renewed + timedelta(seconds=ttl_seconds))
        with self.connection:
            cursor = self.connection.execute(
                """UPDATE service_leases
                   SET renewed_at=?, expires_at=?
                   WHERE lease_name=? AND owner_id=?
                     AND julianday(expires_at) > julianday(?)""",
                (renewed_at, expires_at, lease_name, owner_id, renewed_at),
            )
        return cursor.rowcount > 0

    def release_service_lease(self, lease_name: str, owner_id: str) -> bool:
        _require_nonempty(lease_name, "lease_name")
        _require_nonempty(owner_id, "owner_id")
        with self.connection:
            cursor = self.connection.execute(
                "DELETE FROM service_leases WHERE lease_name=? AND owner_id=?",
                (lease_name, owner_id),
            )
        return cursor.rowcount > 0

    def get_service_lease(self, lease_name: str) -> sqlite3.Row | None:
        _require_nonempty(lease_name, "lease_name")
        return self.connection.execute(
            "SELECT * FROM service_leases WHERE lease_name=?", (lease_name,)
        ).fetchone()

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
        """Switch target broker only while paused and with no local broker state."""
        if environment not in {"demo", "real"}:
            raise ValueError("runtime environment must be demo or real")
        if not reason or not source:
            raise ValueError("reason and source are required")
        control = self.get_runtime_control()
        if not control.paused:
            raise ValueError("runtime environment can only be changed while paused")
        if environment != control.environment:
            open_position = self.connection.execute(
                "SELECT 1 FROM live_positions WHERE status='OPEN' LIMIT 1"
            ).fetchone()
            active_order = self.connection.execute(
                """SELECT 1 FROM broker_orders
                   WHERE status IN ('INTENT','SUBMITTING','ACKNOWLEDGED','PARTIALLY_FILLED',
                                    'CANCEL_PENDING','UNKNOWN') LIMIT 1"""
            ).fetchone()
            if open_position is not None or active_order is not None:
                raise ValueError(
                    "runtime environment cannot change with local open positions "
                    "or active/unknown broker orders"
                )
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
        max_holding_seconds: int | None = None,
        strategy: str | None = None,
        model: str | None = None,
        prompt_version: str | None = None,
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
                    stop_loss_price,risk_level,requires_operator_approval,rationale,created_at,
                    max_holding_seconds,strategy,model,prompt_version)
                   VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    decision_id, symbol, action, confidence, entry_price,
                    take_profit_price, stop_loss_price, risk_level,
                    int(requires_operator_approval), rationale, timestamp,
                    max_holding_seconds,
                    (strategy or "UNKNOWN").strip() or "UNKNOWN",
                    (model or "UNKNOWN").strip() or "UNKNOWN",
                    (prompt_version or "UNKNOWN").strip() or "UNKNOWN",
                ),
            )
        return cursor.rowcount > 0

    def get_ai_decision_audit(self, decision_id: str) -> sqlite3.Row | None:
        _require_nonempty(decision_id, "decision_id")
        return self.connection.execute(
            "SELECT * FROM ai_decision_audits WHERE decision_id=?", (decision_id,)
        ).fetchone()

    def record_approval_request(
        self, *, request_id: str, symbol: str, decision_id: str | None,
        reason: str, status: str = "PENDING",
        signal_id: str | None = None,
        quantity: int | None = None,
        entry_price: float | None = None,
        take_profit_price: float | None = None,
        stop_loss_price: float | None = None,
        max_holding_seconds: int | None = None,
        expires_at: datetime | None = None,
        created_at: datetime | None = None,
    ) -> bool:
        _validate_symbol(symbol)
        if not request_id or not reason or not status:
            raise ValueError("request_id, reason, and status are required")
        base_time = created_at or datetime.now().astimezone()
        timestamp = _iso(base_time)
        expiry = _iso(expires_at or (base_time + timedelta(minutes=2)))
        if signal_id is not None:
            _require_nonempty(signal_id, "signal_id")
        if quantity is not None:
            if isinstance(quantity, bool) or not isinstance(quantity, int) or quantity <= 0:
                raise ValueError("quantity must be positive integer")
        for value, name in (
            (entry_price, "entry_price"),
            (take_profit_price, "take_profit_price"),
            (stop_loss_price, "stop_loss_price"),
        ):
            if value is not None:
                _validate_positive_number(value, name)
        with self.connection:
            cursor = self.connection.execute(
                """INSERT OR IGNORE INTO approval_requests
                   (request_id,symbol,decision_id,reason,status,created_at,resolved_at,
                    expires_at,resolved_by,consumed_at,signal_id,quantity,entry_price,
                    take_profit_price,stop_loss_price,max_holding_seconds)
                   VALUES(?,?,?,?,?,?,NULL,?,NULL,NULL,?,?,?,?,?,?)""",
                (request_id, symbol, decision_id, reason, status, timestamp, expiry,
                 signal_id, quantity, entry_price, take_profit_price, stop_loss_price,
                 max_holding_seconds),
            )
        return cursor.rowcount > 0

    def get_approval_request(self, request_id: str) -> sqlite3.Row | None:
        _require_nonempty(request_id, "request_id")
        return self.connection.execute(
            "SELECT * FROM approval_requests WHERE request_id=?", (request_id,)
        ).fetchone()

    def list_approval_requests(
        self, *, status: str | None = None, limit: int = 20,
    ) -> list[sqlite3.Row]:
        if limit <= 0:
            raise ValueError("limit must be positive")
        if status is None:
            return self.connection.execute(
                "SELECT * FROM approval_requests ORDER BY created_at DESC LIMIT ?", (limit,)
            ).fetchall()
        return self.connection.execute(
            "SELECT * FROM approval_requests WHERE status=? ORDER BY created_at DESC LIMIT ?",
            (status, limit),
        ).fetchall()

    def expire_approval_requests(self, *, now: datetime | None = None) -> int:
        timestamp = _iso(now or datetime.now().astimezone())
        with self.connection:
            cursor = self.connection.execute(
                """UPDATE approval_requests SET status='EXPIRED', resolved_at=?
                   WHERE status IN ('PENDING','APPROVED') AND expires_at IS NOT NULL
                     AND expires_at <= ?""",
                (timestamp, timestamp),
            )
        return cursor.rowcount

    def expire_pending_approvals(self, *, now: datetime | None = None) -> int:
        """Expire approval requests whose deadlines have passed."""
        return self.expire_approval_requests(now=now)

    def resolve_approval_request(
        self, request_id: str, decision: str, *, resolved_by: str | None = None,
        now: datetime | None = None,
    ) -> bool:
        _require_nonempty(request_id, "request_id")
        normalized = decision.upper()
        if normalized not in {"APPROVED", "REJECTED"}:
            raise ValueError("decision must be APPROVED or REJECTED")
        timestamp = _iso(now or datetime.now().astimezone())
        with self.connection:
            self.connection.execute(
                """UPDATE approval_requests SET status='EXPIRED', resolved_at=?
                   WHERE request_id=? AND status='PENDING' AND expires_at IS NOT NULL
                     AND expires_at <= ?""",
                (timestamp, request_id, timestamp),
            )
            cursor = self.connection.execute(
                """UPDATE approval_requests
                   SET status=?, resolved_at=?, resolved_by=?
                   WHERE request_id=? AND status='PENDING'
                     AND (expires_at IS NULL OR expires_at > ?)""",
                (normalized, timestamp, resolved_by, request_id, timestamp),
            )
        return cursor.rowcount > 0

    def consume_approval_request(self, request_id: str, *, now: datetime | None = None) -> bool:
        _require_nonempty(request_id, "request_id")
        timestamp = _iso(now or datetime.now().astimezone())
        with self.connection:
            self.connection.execute(
                """UPDATE approval_requests SET status='EXPIRED', resolved_at=?
                   WHERE request_id=? AND status='APPROVED' AND expires_at IS NOT NULL
                     AND expires_at <= ?""",
                (timestamp, request_id, timestamp),
            )
            cursor = self.connection.execute(
                """UPDATE approval_requests SET status='CONSUMING', consumed_at=?
                   WHERE request_id=? AND status='APPROVED'
                     AND (expires_at IS NULL OR expires_at > ?)""",
                (timestamp, request_id, timestamp),
            )
        return cursor.rowcount > 0

    def finish_approval_request(
        self, request_id: str, *, success: bool, now: datetime | None = None,
    ) -> bool:
        _require_nonempty(request_id, "request_id")
        timestamp = _iso(now or datetime.now().astimezone())
        status = "EXECUTED" if success else "FAILED"
        with self.connection:
            cursor = self.connection.execute(
                """UPDATE approval_requests SET status=?, resolved_at=?
                   WHERE request_id=? AND status='CONSUMING'""",
                (status, timestamp, request_id),
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
        start = _canonical_market_time(bar.start)
        self.connection.execute(
            """INSERT INTO bars_1m(symbol,start,open,high,low,close,volume)
               VALUES(?,?,?,?,?,?,?)
               ON CONFLICT(symbol,start) DO UPDATE SET
               open=excluded.open, high=excluded.high, low=excluded.low,
               close=excluded.close, volume=excluded.volume""",
            (bar.symbol, _iso(start), bar.open, bar.high, bar.low, bar.close, bar.volume),
        )
        self.connection.commit()

    def save_tick(self, tick: MarketTick, received_at: datetime | None = None) -> None:
        received = received_at or datetime.now().astimezone()
        timestamp = _canonical_market_time(tick.timestamp)
        self.connection.execute(
            """INSERT INTO market_ticks(symbol,timestamp,price,volume,received_at)
               VALUES(?,?,?,?,?)""",
            (tick.symbol, _iso(timestamp), tick.price, tick.volume, _iso(received)),
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

    def load_bars(self, symbol: str, limit: int | None = None) -> list[MinuteBar]:
        if limit is not None and (
            isinstance(limit, bool) or not isinstance(limit, int) or limit <= 0
        ):
            raise ValueError("limit must be a positive integer")
        if limit is None:
            rows = self.connection.execute(
                """SELECT symbol,start,open,high,low,close,volume
                   FROM bars_1m WHERE symbol=? ORDER BY start""",
                (symbol,),
            ).fetchall()
        else:
            rows = self.connection.execute(
                """SELECT symbol,start,open,high,low,close,volume FROM (
                       SELECT symbol,start,open,high,low,close,volume
                       FROM bars_1m WHERE symbol=? ORDER BY start DESC LIMIT ?
                   ) ORDER BY start""",
                (symbol, limit),
            ).fetchall()
        return [MinuteBar(row["symbol"], datetime.fromisoformat(row["start"]),
                          row["open"], row["high"], row["low"], row["close"], row["volume"])
                for row in rows]

    def load_recent_bars(self, symbol: str, limit: int) -> list[MinuteBar]:
        return self.load_bars(symbol, limit=limit)

    def delete_old_ticks(self, cutoff: datetime) -> int:
        if not isinstance(cutoff, datetime):
            raise TypeError("cutoff must be a datetime")
        with self.connection:
            cursor = self.connection.execute(
                "DELETE FROM market_ticks WHERE timestamp < ?",
                (_iso(_canonical_market_time(cutoff)),),
            )
        return cursor.rowcount

    def delete_old_bars(self, cutoff: datetime) -> int:
        if not isinstance(cutoff, datetime):
            raise TypeError("cutoff must be a datetime")
        with self.connection:
            cursor = self.connection.execute(
                "DELETE FROM bars_1m WHERE start < ?",
                (_iso(_canonical_market_time(cutoff)),),
            )
        return cursor.rowcount

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
