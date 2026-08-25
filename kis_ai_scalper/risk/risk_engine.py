"""Pure deterministic approval gate for proposed entry intents."""

from .models import OrderIntent, PortfolioState, RiskConfig, RiskDecision
from .position_sizer import calculate_quantity


def evaluate_order_intent(
    config: RiskConfig, portfolio: PortfolioState, intent: OrderIntent
) -> RiskDecision:
    if not intent.symbol or not intent.strategy or not intent.signal_id:
        return RiskDecision(False, "invalid_intent")
    if intent.entry_price <= 0 or intent.stop_loss <= 0 or intent.stop_loss >= intent.entry_price:
        return RiskDecision(False, "invalid_entry_or_stop")
    if intent.confidence is not None and intent.confidence < config.minimum_confidence:
        return RiskDecision(False, "confidence_below_minimum")
    if any(position.symbol == intent.symbol for position in portfolio.open_positions):
        return RiskDecision(False, "existing_position_same_symbol")
    if len(portfolio.open_positions) >= config.max_positions:
        return RiskDecision(False, "max_positions_reached")
    if portfolio.daily_pnl_krw <= -(config.allocated_krw * config.max_daily_loss_pct / 100):
        return RiskDecision(False, "daily_loss_limit_reached")
    if portfolio.consecutive_losses >= config.consecutive_loss_limit:
        return RiskDecision(False, "consecutive_loss_limit_reached")
    if config.max_trades_per_day is not None and portfolio.trades_today >= config.max_trades_per_day:
        return RiskDecision(False, "max_trades_per_day_reached")
    if portfolio.orders_by_symbol.get(intent.symbol, 0) >= config.max_orders_per_symbol:
        return RiskDecision(False, "max_orders_per_symbol_reached")

    quantity = calculate_quantity(config, intent.entry_price, intent.stop_loss)
    if quantity <= 0:
        return RiskDecision(False, "calculated_quantity_zero")
    new_exposure = portfolio.current_exposure_krw + quantity * intent.entry_price
    exposure_cap = config.allocated_krw * config.max_total_exposure_pct / 100
    if new_exposure > exposure_cap:
        return RiskDecision(False, "max_total_exposure_reached")
    max_loss = quantity * (intent.entry_price - intent.stop_loss)
    return RiskDecision(True, "approved", quantity, max_loss)
