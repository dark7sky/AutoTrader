from kis_ai_scalper.risk import (
    OrderIntent,
    PortfolioState,
    PositionState,
    RiskConfig,
    calculate_quantity,
    evaluate_order_intent,
)


def make_intent(**overrides):
    values = {
        "symbol": "005930",
        "strategy": "BREAKOUT_WATCH",
        "signal_id": "signal-1",
        "entry_price": 100_000,
        "stop_loss": 99_000,
        "confidence": 0.8,
    }
    values.update(overrides)
    return OrderIntent(**values)


def test_position_sizing_uses_risk_budget():
    assert calculate_quantity(RiskConfig(), 100_000, 99_000) == 3


def test_position_sizing_is_capped_by_position_value():
    config = RiskConfig(risk_per_trade_pct=10, max_position_pct=1)
    assert calculate_quantity(config, 10_000, 9_900) == 3


def test_rejects_invalid_stop():
    decision = evaluate_order_intent(RiskConfig(), PortfolioState(), make_intent(stop_loss=100_000))
    assert (decision.approved, decision.reason) == (False, "invalid_entry_or_stop")


def test_rejects_low_confidence():
    decision = evaluate_order_intent(RiskConfig(), PortfolioState(), make_intent(confidence=0.74))
    assert decision.reason == "confidence_below_minimum"


def test_confidence_threshold_is_configurable():
    config = RiskConfig(minimum_confidence=0.9)
    decision = evaluate_order_intent(config, PortfolioState(), make_intent(confidence=0.85))
    assert decision.reason == "confidence_below_minimum"


def test_rejects_existing_position_same_symbol():
    portfolio = PortfolioState(open_positions=(PositionState("005930", 1, 90_000),))
    decision = evaluate_order_intent(RiskConfig(), portfolio, make_intent())
    assert decision.reason == "existing_position_same_symbol"


def test_rejects_each_portfolio_limit():
    config = RiskConfig()
    cases = [
        (PortfolioState(open_positions=(PositionState("000001", 1, 1), PositionState("000002", 1, 1))), "max_positions_reached"),
        (PortfolioState(current_exposure_krw=599_000), "max_total_exposure_reached"),
        (PortfolioState(daily_pnl_krw=-30_000), "daily_loss_limit_reached"),
        (PortfolioState(consecutive_losses=3), "consecutive_loss_limit_reached"),
        (PortfolioState(trades_today=10), "max_trades_per_day_reached"),
        (PortfolioState(orders_by_symbol={"005930": 3}), "max_orders_per_symbol_reached"),
    ]
    for portfolio, reason in cases:
        assert evaluate_order_intent(config, portfolio, make_intent()).reason == reason


def test_approves_normal_intent_and_reports_max_loss():
    decision = evaluate_order_intent(RiskConfig(), PortfolioState(), make_intent())
    assert decision.approved is True
    assert decision.reason == "approved"
    assert decision.quantity == 3
    assert decision.max_loss_krw == 3_000


def test_risk_modules_have_no_network_or_broker_calls():
    from pathlib import Path

    root = Path(__file__).parents[1] / "kis_ai_scalper" / "risk"
    source = "\n".join(path.read_text(encoding="utf-8") for path in root.glob("*.py"))
    assert "requests" not in source
    assert "websocket" not in source
    assert "oauth2" not in source
