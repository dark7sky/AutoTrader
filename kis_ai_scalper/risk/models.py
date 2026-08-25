"""Pure data models used by the deterministic risk engine."""

from dataclasses import dataclass, field


@dataclass(frozen=True)
class RiskConfig:
    allocated_krw: float = 3_000_000
    risk_per_trade_pct: float = 0.3
    max_position_pct: float = 10
    max_total_exposure_pct: float = 20
    max_positions: int = 2
    max_daily_loss_pct: float = 1.0
    consecutive_loss_limit: int = 3
    max_trades_per_day: int | None = None
    max_orders_per_symbol: int = 3
    minimum_confidence: float = 0.75


@dataclass(frozen=True)
class PositionState:
    symbol: str
    quantity: int
    avg_price: float
    stop_loss: float | None = None


@dataclass
class PortfolioState:
    current_exposure_krw: float = 0
    open_positions: tuple[PositionState, ...] = ()
    daily_pnl_krw: float = 0
    consecutive_losses: int = 0
    trades_today: int = 0
    orders_by_symbol: dict[str, int] = field(default_factory=dict)


@dataclass(frozen=True)
class OrderIntent:
    symbol: str
    strategy: str
    signal_id: str
    entry_price: float
    stop_loss: float
    confidence: float | None = None


@dataclass(frozen=True)
class RiskDecision:
    approved: bool
    reason: str
    quantity: int = 0
    max_loss_krw: float = 0
