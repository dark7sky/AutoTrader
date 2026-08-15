from datetime import datetime, timedelta

from kis_ai_scalper.ai.decision import AIDecisionAction, AIRiskLevel, TradingAIDecision
from kis_ai_scalper.broker.kis_order import KisOrderResult, KisOrderSide, KisOrderType
from kis_ai_scalper.market.tick import MarketTick, MinuteBar
from kis_ai_scalper.pipeline.auto_trade import AutoTradeConfig, run_auto_trade_cycle
from kis_ai_scalper.risk import PortfolioState, PositionState
from kis_ai_scalper.storage import connect_database
from kis_ai_scalper.storage.database import RuntimeControl


NOW = datetime(2026, 8, 18, 9, 30)
SYMBOL = "005930"


def seed_market(database, symbol=SYMBOL, *, price=100_000, tick_at=NOW, bar_at=NOW - timedelta(minutes=1)):
    for index in range(21):
        start = bar_at - timedelta(minutes=20 - index)
        database.save_bar(MinuteBar(symbol, start, price, price + 500, price - 500, price, 100))
    database.save_tick(MarketTick(symbol, tick_at, price, 1))


def buy_decision(symbol=SYMBOL, *, decision_id="decision-1", entry=100_000):
    return TradingAIDecision(
        decision_id=decision_id,
        symbol=symbol,
        action=AIDecisionAction.BUY,
        confidence=0.9,
        entry_price=entry,
        stop_loss_price=entry - 1_000,
        take_profit_price=entry + 2_000,
        risk_level=AIRiskLevel.NORMAL,
        rationale="test setup",
    )


class FixedAI:
    def __init__(self, decision):
        self.decision = decision
        self.calls = 0

    def decide(self, context):
        self.calls += 1
        return self.decision


class Submitter:
    def __init__(self, *, broker_order_id="broker-1", error=None):
        self.requests = []
        self.broker_order_id = broker_order_id
        self.error = error

    def submit_order(self, request):
        self.requests.append(request)
        if self.error is not None:
            raise self.error
        return KisOrderResult(
            request.symbol, KisOrderSide(request.side), request.quantity, request.price,
            KisOrderType(request.order_type), "VTTC0012U", self.broker_order_id, {},
        )


def control(*, paused=False):
    return RuntimeControl(paused, NOW.isoformat(), "test", "test")


def run(database, ai, submitter, *, runtime=None, portfolio=None, now=NOW, config=None):
    return run_auto_trade_cycle(
        [SYMBOL], database=database, ai_client=ai, submitter=submitter,
        runtime_control=runtime or control(),
        config=config or AutoTradeConfig(max_quantity=1),
        confirm_auto_trade=True, current_time=now, portfolio=portfolio,
    )


def add_position(database, *, quantity=2, opened_at=NOW - timedelta(minutes=5)):
    database.open_live_position(
        position_id="position-1", signal_id="signal-1", symbol=SYMBOL, quantity=quantity,
        entry_price=100_000, stop_loss_price=99_000, take_profit_price=102_000,
        opened_at=opened_at, entry_broker_order_id="entry-1", max_holding_seconds=900,
    )


def test_failed_submit_is_unknown_and_second_cycle_does_not_resubmit(tmp_path):
    with connect_database(tmp_path / "safety.db") as database:
        database.init_schema()
        seed_market(database)
        ai = FixedAI(buy_decision())
        submitter = Submitter(error=RuntimeError("transport failure"))
        first = run(database, ai, submitter)
        second = run(database, ai, submitter)
        order = database.connection.execute(
            "SELECT * FROM broker_orders WHERE symbol=? AND side='BUY'", (SYMBOL,)
        ).fetchone()

    assert first.results[0].reason == "order_unknown"
    assert second.results[0].reason == "order_already_claimed"
    assert len(submitter.requests) == 1
    assert order["status"] == "UNKNOWN"


def test_pause_blocks_buy_but_allows_exit_submission_without_closing_position(tmp_path):
    with connect_database(tmp_path / "pause.db") as database:
        database.init_schema()
        seed_market(database, price=98_900)
        add_position(database)
        submitter = Submitter()
        report = run(database, FixedAI(buy_decision()), submitter, runtime=control(paused=True))
        still_open = bool(database.list_open_live_positions(SYMBOL))

    assert report.results[0].action == "SELL"
    assert report.results[0].submitted is True
    assert still_open


def test_emergency_stop_blocks_existing_exit_and_new_buy(tmp_path):
    with connect_database(tmp_path / "emergency.db") as database:
        database.init_schema()
        database.set_runtime_metadata("emergency_stop", "true")
        seed_market(database, price=98_900)
        add_position(database)
        submitter = Submitter()
        report = run(database, FixedAI(buy_decision()), submitter)

    assert report.results[0].reason == "emergency_stop"
    assert submitter.requests == []


def test_stale_data_blocks_ai_and_exit(tmp_path):
    with connect_database(tmp_path / "stale.db") as database:
        database.init_schema()
        seed_market(database, tick_at=NOW - timedelta(seconds=11))
        ai = FixedAI(buy_decision())
        submitter = Submitter()
        buy_report = run(database, ai, submitter)
        add_position(database)
        exit_report = run(database, ai, submitter)

    assert buy_report.results[0].reason == "stale_tick"
    assert exit_report.results[0].reason == "stale_tick"
    assert ai.calls == 0
    assert submitter.requests == []


def test_mismatched_ai_symbol_is_blocked(tmp_path):
    with connect_database(tmp_path / "symbol.db") as database:
        database.init_schema()
        seed_market(database)
        report = run(database, FixedAI(buy_decision("000660")), Submitter())

    assert report.results[0].reason == "ai_symbol_mismatch"


def test_session_close_forces_exit_reason_and_ack_only(tmp_path):
    close_time = datetime(2026, 8, 18, 15, 10)
    with connect_database(tmp_path / "close.db") as database:
        database.init_schema()
        seed_market(database, tick_at=close_time, bar_at=close_time - timedelta(minutes=1))
        add_position(database)
        report = run(
            database, FixedAI(buy_decision()), Submitter(), now=close_time,
        )
        position = database.list_open_live_positions(SYMBOL)

    assert report.results[0].reason == "session_close"
    assert len(position) == 1


def test_portfolio_argument_is_used_for_risk_evaluation(tmp_path):
    with connect_database(tmp_path / "portfolio.db") as database:
        database.init_schema()
        seed_market(database)
        portfolio = PortfolioState(
            open_positions=(PositionState("000660", 1, 100_000), PositionState("035420", 1, 100_000)),
        )
        report = run(database, FixedAI(buy_decision()), Submitter(), portfolio=portfolio)

    assert report.results[0].reason == "max_positions_reached"


def test_materialize_buy_and_sell_fills_is_weighted_and_idempotent(tmp_path):
    fill_time = NOW
    with connect_database(tmp_path / "materialize.db") as database:
        database.init_schema()
        assert database.claim_order_intent(
            client_order_id="ai:buy:decision-fill", signal_id="decision-fill",
            symbol=SYMBOL, side="BUY", requested_qty=4, requested_price=100_000,
        )
        assert database.record_order_submission("ai:buy:decision-fill", "broker-buy")
        database.apply_broker_fill(
            fill_id="buy-fill-1", client_order_id="ai:buy:decision-fill", quantity=2,
            price=100_000, filled_at=fill_time, broker_order_id="broker-buy",
        )
        database.apply_broker_fill(
            fill_id="buy-fill-2", client_order_id="ai:buy:decision-fill", quantity=2,
            price=102_000, filled_at=fill_time + timedelta(seconds=1), broker_order_id="broker-buy",
        )
        assert database.materialize_order_fills(
            "ai:buy:decision-fill", stop_loss_price=99_000,
            take_profit_price=104_000, max_holding_seconds=900,
        ) == 2
        position = database.list_open_live_positions(SYMBOL)[0]
        assert position["quantity"] == 4
        assert position["entry_price"] == 101_000
        assert database.materialize_order_fills(
            "ai:buy:decision-fill", stop_loss_price=99_000, take_profit_price=104_000,
        ) == 0

        assert database.claim_order_intent(
            client_order_id="ai:sell:signal-fill", signal_id="signal-fill",
            symbol=SYMBOL, side="SELL", requested_qty=4, requested_price=101_000,
        )
        database.record_order_submission("ai:sell:signal-fill", "broker-sell")
        database.apply_broker_fill(
            fill_id="sell-fill-1", client_order_id="ai:sell:signal-fill", quantity=1,
            price=101_000, filled_at=fill_time + timedelta(seconds=2), broker_order_id="broker-sell",
        )
        database.apply_broker_fill(
            fill_id="sell-fill-2", client_order_id="ai:sell:signal-fill", quantity=3,
            price=101_000, filled_at=fill_time + timedelta(seconds=3), broker_order_id="broker-sell",
        )
        assert database.materialize_order_fills("ai:sell:signal-fill", close_reason="stop_loss") == 2
        assert database.list_open_live_positions(SYMBOL) == []
        assert database.materialize_order_fills("ai:sell:signal-fill") == 0
