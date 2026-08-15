from datetime import datetime, timedelta

from kis_ai_scalper.market.health import MarketHealthStatus, evaluate_market_health
from kis_ai_scalper.market.tick import MarketTick, MinuteBar


NOW = datetime(2026, 8, 15, 9, 30, 0)


def make_tick(age_seconds: int) -> MarketTick:
    return MarketTick("005930", NOW - timedelta(seconds=age_seconds), 100.0, 1)


def make_bar(age_seconds: int) -> MinuteBar:
    return MinuteBar("005930", NOW - timedelta(seconds=age_seconds), 100, 101, 99, 100, 10)


def test_fresh_acknowledged_tick_is_ok_and_tradable():
    health = evaluate_market_health(NOW, websocket_acknowledged=True, latest_tick=make_tick(2))

    assert health.status is MarketHealthStatus.OK
    assert health.trading_blocked is False
    assert health.enter_safe_mode is False
    assert health.tick_age_seconds == 2.0


def test_stale_tick_blocks_trading_and_enters_safe_mode():
    health = evaluate_market_health(NOW, websocket_acknowledged=True, latest_tick=make_tick(6))

    assert health.status is MarketHealthStatus.STALE
    assert health.trading_blocked is True
    assert health.enter_safe_mode is True


def test_acknowledged_connection_without_data_is_no_data():
    health = evaluate_market_health(NOW, websocket_acknowledged=True)

    assert health.status is MarketHealthStatus.NO_DATA
    assert health.trading_blocked is True
    assert health.enter_safe_mode is True


def test_missing_subscription_ack_is_disconnected_even_with_cached_data():
    health = evaluate_market_health(NOW, websocket_acknowledged=False, latest_tick=make_tick(1))

    assert health.status is MarketHealthStatus.DISCONNECTED
    assert health.trading_blocked is True
    assert health.enter_safe_mode is True


def test_stale_bar_blocks_trading():
    health = evaluate_market_health(NOW, websocket_acknowledged=True, latest_bar=make_bar(91))

    assert health.status is MarketHealthStatus.STALE
    assert health.bar_age_seconds == 91.0
    assert health.trading_blocked is True
