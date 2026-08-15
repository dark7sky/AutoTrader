"""Operator-readable reports for the local SQLite paper journal."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable


@dataclass(frozen=True)
class PaperPositionReport:
    symbol: str
    quantity: int
    average_cost: float


@dataclass(frozen=True)
class PaperReport:
    total_paper_orders: int
    total_paper_fills: int
    open_positions: tuple[PaperPositionReport, ...]
    gross_buy_value: float
    realized_pnl: float
    symbols: tuple[str, ...]
    first_fill_timestamp: str | None
    last_fill_timestamp: str | None

    @property
    def empty(self) -> bool:
        return self.total_paper_orders == 0 and self.total_paper_fills == 0


@dataclass
class _Inventory:
    quantity: int = 0
    cost: float = 0.0

    @property
    def average_cost(self) -> float:
        return self.cost / self.quantity if self.quantity else 0.0


def build_paper_report(
    orders: Iterable[object],
    fills: Iterable[object],
) -> PaperReport:
    """Build a report from SQLite rows, replaying fills in weighted-average order."""
    order_rows = list(orders)
    fill_rows = sorted(
        list(fills),
        key=lambda row: (str(row["filled_at"]), str(row["fill_id"])),
    )
    inventory: dict[str, _Inventory] = {}
    realized_pnl = 0.0
    gross_buy_value = 0.0
    symbols = {row["symbol"] for row in fill_rows}

    for row in fill_rows:
        symbol = row["symbol"]
        side = str(row["side"]).upper()
        quantity = int(row["quantity"])
        price = float(row["price"])
        position = inventory.setdefault(symbol, _Inventory())
        if side == "BUY":
            position.quantity += quantity
            position.cost += quantity * price
            gross_buy_value += quantity * price
        elif side == "SELL":
            if quantity > position.quantity:
                raise ValueError(f"paper sell exceeds long position for {symbol}")
            realized_pnl += (price - position.average_cost) * quantity
            position.cost -= position.average_cost * quantity
            position.quantity -= quantity

    open_positions = tuple(
        PaperPositionReport(symbol, position.quantity, position.average_cost)
        for symbol, position in sorted(inventory.items())
        if position.quantity > 0
    )
    timestamps = [str(row["filled_at"]) for row in fill_rows]
    return PaperReport(
        total_paper_orders=len(order_rows),
        total_paper_fills=len(fill_rows),
        open_positions=open_positions,
        gross_buy_value=gross_buy_value,
        realized_pnl=realized_pnl,
        symbols=tuple(sorted(symbols)),
        first_fill_timestamp=min(timestamps) if timestamps else None,
        last_fill_timestamp=max(timestamps) if timestamps else None,
    )


def report_from_database(database: object, symbol: str | None = None) -> PaperReport:
    """Read only local paper tables from a Database-like object."""
    return build_paper_report(
        database.list_paper_orders(symbol),
        database.list_paper_fills(symbol),
    )
