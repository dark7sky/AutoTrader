from datetime import datetime, timezone

from kis_ai_scalper.ops.performance import build_performance_report, performance_report_from_database
from kis_ai_scalper.storage import connect_database


NOW = datetime(2026, 8, 25, 9, 30, tzinfo=timezone.utc)


def _order(database, client_id, signal_id, symbol, side, quantity, price, at):
    assert database.claim_order_intent(
        client_order_id=client_id,
        signal_id=signal_id,
        symbol=symbol,
        side=side,
        requested_qty=quantity,
        requested_price=price,
        created_at=at,
    )
    database.record_order_submission(client_id, "broker-" + client_id, submitted_at=at)
    assert database.apply_broker_fill(
        fill_id="fill-" + client_id,
        client_order_id=client_id,
        quantity=quantity,
        price=price,
        filled_at=at,
        broker_order_id="broker-" + client_id,
        symbol=symbol,
        side=side,
    )


def test_performance_report_uses_fifo_net_costs_and_drawdown(tmp_path, monkeypatch):
    monkeypatch.setenv("ESTIMATED_BUY_COST_BPS", "15")
    monkeypatch.setenv("ESTIMATED_SELL_COST_BPS", "15")
    with connect_database(tmp_path / "perf.db") as database:
        database.init_schema()
        database.record_ai_decision(
            decision_id="decision-win",
            symbol="005930",
            action="BUY",
            confidence=0.9,
            entry_price=100.0,
            take_profit_price=110.0,
            stop_loss_price=95.0,
            risk_level="NORMAL",
            requires_operator_approval=False,
            rationale="candidate",
            strategy="PULLBACK_WATCH",
            model="gpt-test",
            prompt_version="trade-decision-v2",
            created_at=NOW,
        )
        _order(database, "buy-win", "decision-win", "005930", "BUY", 10, 100.0, NOW)
        _order(database, "sell-win", "exit-win", "005930", "SELL", 10, 110.0, NOW)
        _order(database, "buy-loss", "missing-decision", "000660", "BUY", 5, 200.0, NOW)
        _order(database, "sell-loss", "exit-loss", "000660", "SELL", 5, 190.0, NOW)

        report = performance_report_from_database(database)

    assert report.closed_trades == 2
    assert report.gross_realized_pnl == 50.0
    assert report.estimated_costs == 6.075
    assert report.net_realized_pnl == 43.925
    assert report.win_rate == 0.5
    assert report.average_win == 96.85
    assert report.average_loss == -52.925
    assert round(report.profit_factor, 4) == round(96.85 / 52.925, 4)
    assert report.max_drawdown == 52.925
    assert report.by_strategy["PULLBACK_WATCH"].net_realized_pnl == 96.85
    assert report.by_strategy["UNKNOWN"].net_realized_pnl == -52.925


def test_performance_report_text_is_operator_readable():
    report = build_performance_report([], buy_cost_bps=15, sell_cost_bps=15)
    text = report.text()
    assert "performance report" in text
    assert "closed_trades=0" in text
    assert "cost_bps=buy:15 sell:15" in text
