"""Net performance reporting from broker fills."""

from __future__ import annotations

from collections import defaultdict, deque
from dataclasses import dataclass, field
import os
from typing import Any, Iterable


@dataclass(frozen=True)
class StrategyPerformance:
    strategy: str
    closed_trades: int = 0
    gross_realized_pnl: float = 0.0
    estimated_costs: float = 0.0
    net_realized_pnl: float = 0.0


@dataclass(frozen=True)
class PerformanceReport:
    closed_trades: int
    gross_realized_pnl: float
    estimated_costs: float
    net_realized_pnl: float
    wins: int
    losses: int
    win_rate: float
    average_win: float
    average_loss: float
    profit_factor: float
    max_drawdown: float
    buy_cost_bps: float
    sell_cost_bps: float
    outcomes: tuple[float, ...] = ()
    by_strategy: dict[str, StrategyPerformance] = field(default_factory=dict)

    def text(self) -> str:
        lines = [
            "performance report",
            (
                f"closed_trades={self.closed_trades} wins={self.wins} losses={self.losses} "
                f"win_rate={self.win_rate:.2%}"
            ),
            (
                f"gross_realized_pnl={self.gross_realized_pnl:g} "
                f"estimated_costs={self.estimated_costs:g} "
                f"net_realized_pnl={self.net_realized_pnl:g}"
            ),
            (
                f"average_win={self.average_win:g} average_loss={self.average_loss:g} "
                f"profit_factor={self.profit_factor:g} max_drawdown={self.max_drawdown:g}"
            ),
            f"cost_bps=buy:{self.buy_cost_bps:g} sell:{self.sell_cost_bps:g}",
            "by_strategy:",
        ]
        if not self.by_strategy:
            lines.append("- none")
        else:
            for strategy, item in sorted(self.by_strategy.items()):
                lines.append(
                    f"- {strategy} trades={item.closed_trades} "
                    f"gross={item.gross_realized_pnl:g} costs={item.estimated_costs:g} "
                    f"net={item.net_realized_pnl:g}"
                )
        return "\n".join(lines)


@dataclass
class _Lot:
    quantity: int
    price: float
    strategy: str


@dataclass
class _StrategyAccumulator:
    closed_trades: int = 0
    gross: float = 0.0
    costs: float = 0.0
    net: float = 0.0

    def report(self, strategy: str) -> StrategyPerformance:
        return StrategyPerformance(
            strategy=strategy,
            closed_trades=self.closed_trades,
            gross_realized_pnl=round(self.gross, 10),
            estimated_costs=round(self.costs, 10),
            net_realized_pnl=round(self.net, 10),
        )


def cost_bps_from_env() -> tuple[float, float]:
    return _env_bps("ESTIMATED_BUY_COST_BPS", 15.0), _env_bps("ESTIMATED_SELL_COST_BPS", 15.0)


def build_performance_report(
    fills: Iterable[Any],
    *,
    buy_cost_bps: float | None = None,
    sell_cost_bps: float | None = None,
) -> PerformanceReport:
    buy_bps, sell_bps = cost_bps_from_env()
    if buy_cost_bps is not None:
        buy_bps = float(buy_cost_bps)
    if sell_cost_bps is not None:
        sell_bps = float(sell_cost_bps)
    if buy_bps < 0 or sell_bps < 0:
        raise ValueError("cost bps must be non-negative")

    lots: dict[str, deque[_Lot]] = defaultdict(deque)
    outcomes: list[float] = []
    gross_total = 0.0
    cost_total = 0.0
    by_strategy: dict[str, _StrategyAccumulator] = defaultdict(_StrategyAccumulator)

    for row in sorted(fills, key=lambda item: (str(_get(item, "filled_at", "")), str(_get(item, "fill_id", "")))):
        symbol = str(_get(row, "symbol", "") or "")
        side = str(_get(row, "side", "") or "").upper()
        quantity = int(_get(row, "quantity", 0) or 0)
        price = float(_get(row, "price", 0) or 0)
        if not symbol or side not in {"BUY", "SELL"} or quantity <= 0 or price <= 0:
            raise ValueError("invalid broker fill row")
        if side == "BUY":
            strategy = str(_get(row, "strategy", "") or "UNKNOWN").strip() or "UNKNOWN"
            lots[symbol].append(_Lot(quantity, price, strategy))
            continue

        remaining = quantity
        while remaining:
            if not lots[symbol]:
                raise ValueError(f"sell exceeds FIFO inventory for {symbol}")
            lot = lots[symbol][0]
            matched = min(remaining, lot.quantity)
            buy_notional = lot.price * matched
            sell_notional = price * matched
            gross = sell_notional - buy_notional
            costs = buy_notional * buy_bps / 10_000 + sell_notional * sell_bps / 10_000
            net = gross - costs
            outcomes.append(net)
            gross_total += gross
            cost_total += costs
            accumulator = by_strategy[lot.strategy]
            accumulator.closed_trades += 1
            accumulator.gross += gross
            accumulator.costs += costs
            accumulator.net += net
            remaining -= matched
            if matched == lot.quantity:
                lots[symbol].popleft()
            else:
                lot.quantity -= matched

    wins = [value for value in outcomes if value > 0]
    losses = [value for value in outcomes if value < 0]
    win_base = len(wins) + len(losses)
    profit_factor = (sum(wins) / abs(sum(losses))) if losses else (float("inf") if wins else 0.0)
    return PerformanceReport(
        closed_trades=len(outcomes),
        gross_realized_pnl=round(gross_total, 10),
        estimated_costs=round(cost_total, 10),
        net_realized_pnl=round(gross_total - cost_total, 10),
        wins=len(wins),
        losses=len(losses),
        win_rate=(len(wins) / win_base) if win_base else 0.0,
        average_win=round(sum(wins) / len(wins), 10) if wins else 0.0,
        average_loss=round(sum(losses) / len(losses), 10) if losses else 0.0,
        profit_factor=round(profit_factor, 10) if profit_factor != float("inf") else profit_factor,
        max_drawdown=round(_max_drawdown(outcomes), 10),
        buy_cost_bps=buy_bps,
        sell_cost_bps=sell_bps,
        outcomes=tuple(round(value, 10) for value in outcomes),
        by_strategy={strategy: item.report(strategy) for strategy, item in by_strategy.items()},
    )


def performance_report_from_database(database: Any) -> PerformanceReport:
    rows = database.connection.execute(
        """SELECT f.*, COALESCE(a.strategy, 'UNKNOWN') AS strategy
           FROM broker_fills f
           JOIN broker_orders o ON o.client_order_id=f.client_order_id
           LEFT JOIN ai_decision_audits a ON a.decision_id=o.signal_id
           ORDER BY f.filled_at, f.fill_id"""
    ).fetchall()
    return build_performance_report(rows)


def _max_drawdown(outcomes: list[float]) -> float:
    equity = 0.0
    peak = 0.0
    drawdown = 0.0
    for outcome in outcomes:
        equity += outcome
        peak = max(peak, equity)
        drawdown = max(drawdown, peak - equity)
    return drawdown


def _get(row: Any, key: str, default: Any = None) -> Any:
    try:
        return row[key]
    except (KeyError, IndexError, TypeError):
        return getattr(row, key, default)


def _env_bps(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, str(default)))
    except (TypeError, ValueError):
        return default


__all__ = [
    "PerformanceReport",
    "StrategyPerformance",
    "build_performance_report",
    "cost_bps_from_env",
    "performance_report_from_database",
]
