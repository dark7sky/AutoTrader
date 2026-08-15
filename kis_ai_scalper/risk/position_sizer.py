"""Deterministic position sizing. This module has no broker dependencies."""

from .models import RiskConfig


def calculate_quantity(config: RiskConfig, entry_price: float, stop_loss: float) -> int:
    """Return whole shares sized by risk budget and capped by position value."""
    if entry_price <= 0 or stop_loss <= 0 or stop_loss >= entry_price:
        return 0
    per_share_risk = entry_price - stop_loss
    risk_budget = config.allocated_krw * config.risk_per_trade_pct / 100
    position_cap = config.allocated_krw * config.max_position_pct / 100
    if risk_budget <= 0 or position_cap <= 0:
        return 0
    by_risk = int(risk_budget // per_share_risk)
    by_capital = int(position_cap // entry_price)
    return max(0, min(by_risk, by_capital))
