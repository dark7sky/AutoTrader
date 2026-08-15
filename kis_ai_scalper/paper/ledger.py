"""Small deterministic in-memory paper-trade ledger.

This module records local order intents and simulated fills only. It has no
broker, account, network, or AI dependencies.
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from enum import StrEnum
from math import isfinite


class PaperSide(StrEnum):
    BUY = "BUY"
    SELL = "SELL"


class PaperOrderStatus(StrEnum):
    PENDING = "PENDING"
    FILLED = "FILLED"
    REJECTED = "REJECTED"
    CANCELLED = "CANCELLED"


@dataclass
class PaperOrderIntent:
    order_id: str
    symbol: str
    side: PaperSide
    quantity: int
    price: float
    status: PaperOrderStatus = PaperOrderStatus.PENDING
    filled_quantity: int = 0


@dataclass(frozen=True)
class PaperFill:
    fill_id: str
    order_id: str
    quantity: int
    price: float


@dataclass(frozen=True)
class PaperPosition:
    symbol: str
    quantity: int
    avg_price: float


@dataclass(frozen=True)
class PaperTradeReport:
    realized_pnl: float
    positions: tuple[PaperPosition, ...]
    open_orders: tuple[PaperOrderIntent, ...]


def _validate_trade_values(symbol: str, quantity: int, price: float) -> None:
    if not symbol.strip():
        raise ValueError("symbol must not be empty")
    if isinstance(quantity, bool) or quantity <= 0:
        raise ValueError("quantity must be positive")
    if not isfinite(price) or price <= 0:
        raise ValueError("price must be finite and positive")


def _validate_identifier(value: str, label: str) -> None:
    if not value.strip():
        raise ValueError(f"{label} must not be empty")


class PaperLedger:
    """In-memory long-only ledger for simulated orders and fills."""

    def __init__(self) -> None:
        self._orders: dict[str, PaperOrderIntent] = {}
        self._fills: dict[str, PaperFill] = {}
        self._positions: dict[str, PaperPosition] = {}
        self._realized_pnl = 0.0

    def submit_order(self, intent: PaperOrderIntent) -> PaperOrderIntent:
        _validate_identifier(intent.order_id, "order_id")
        _validate_trade_values(intent.symbol, intent.quantity, intent.price)
        side = PaperSide(intent.side)
        if intent.order_id in self._orders:
            existing = self._orders[intent.order_id]
            if (
                existing.symbol != intent.symbol
                or existing.side is not side
                or existing.quantity != intent.quantity
                or existing.price != intent.price
            ):
                raise ValueError(f"conflicting paper order id: {intent.order_id}")
            return existing
        stored = replace(intent, side=side, status=PaperOrderStatus.PENDING, filled_quantity=0)
        position = self._positions.get(intent.symbol)
        if stored.side is PaperSide.SELL and (
            position is None or intent.quantity > position.quantity
        ):
            stored.status = PaperOrderStatus.REJECTED
        self._orders[stored.order_id] = stored
        return stored

    def fill_order(self, fill: PaperFill) -> PaperFill:
        _validate_identifier(fill.fill_id, "fill_id")
        _validate_identifier(fill.order_id, "order_id")
        _validate_trade_values("fill", fill.quantity, fill.price)
        existing = self._fills.get(fill.fill_id)
        if existing is not None:
            if (
                existing.order_id != fill.order_id
                or existing.quantity != fill.quantity
                or existing.price != fill.price
            ):
                raise ValueError(f"conflicting paper fill id: {fill.fill_id}")
            return existing
        try:
            order = self._orders[fill.order_id]
        except KeyError as exc:
            raise KeyError(f"unknown paper order: {fill.order_id}") from exc
        if order.status in {PaperOrderStatus.REJECTED, PaperOrderStatus.CANCELLED}:
            raise ValueError(f"cannot fill order in {order.status} status")
        if order.filled_quantity + fill.quantity > order.quantity:
            raise ValueError("fill quantity exceeds order quantity")
        position = self._positions.get(order.symbol)
        if order.side is PaperSide.SELL and (
            position is None or fill.quantity > position.quantity
        ):
            raise ValueError("sell fill exceeds current long position")

        self._apply_fill(order, fill, position)
        self._fills[fill.fill_id] = fill
        order.filled_quantity += fill.quantity
        if order.filled_quantity == order.quantity:
            order.status = PaperOrderStatus.FILLED
        return fill

    def _apply_fill(
        self,
        order: PaperOrderIntent,
        fill: PaperFill,
        position: PaperPosition | None,
    ) -> None:
        if order.side is PaperSide.BUY:
            old_quantity = position.quantity if position else 0
            old_avg = position.avg_price if position else 0.0
            quantity = old_quantity + fill.quantity
            average = ((old_quantity * old_avg) + (fill.quantity * fill.price)) / quantity
            self._positions[order.symbol] = PaperPosition(order.symbol, quantity, average)
            return

        assert position is not None
        self._realized_pnl += (fill.price - position.avg_price) * fill.quantity
        remaining = position.quantity - fill.quantity
        if remaining:
            self._positions[order.symbol] = PaperPosition(
                order.symbol, remaining, position.avg_price
            )
        else:
            self._positions.pop(order.symbol, None)

    @property
    def positions(self) -> dict[str, PaperPosition]:
        return dict(self._positions)

    @property
    def realized_pnl(self) -> float:
        return self._realized_pnl

    @property
    def open_orders(self) -> tuple[PaperOrderIntent, ...]:
        return tuple(
            order for order in self._orders.values()
            if order.status is PaperOrderStatus.PENDING
        )

    def report(self) -> PaperTradeReport:
        return PaperTradeReport(
            realized_pnl=self.realized_pnl,
            positions=tuple(self._positions.values()),
            open_orders=self.open_orders,
        )
