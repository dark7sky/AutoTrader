from dataclasses import dataclass
from datetime import datetime, timedelta

import pytest
from pydantic import ValidationError

from kis_ai_scalper.ai.decision import (
    AIDecisionAction,
    AIDecisionContext,
    AIRiskLevel,
    OpenAITradingDecisionClient,
    TradingAIDecision,
    trading_ai_decision_schema,
)
from kis_ai_scalper.broker.kis_order import KisOrderResult, KisOrderSide, KisOrderType
from kis_ai_scalper.market.tick import MarketTick, MinuteBar
from kis_ai_scalper.pipeline.auto_trade import AutoTradeConfig, run_auto_trade_cycle
from kis_ai_scalper.risk.models import RiskConfig
from kis_ai_scalper.storage import connect_database
from kis_ai_scalper.storage.database import RuntimeControl


OPEN_MARKET_TIME = datetime(2026, 8, 18, 9, 30)


def seed_bars(database, symbol="005930", *, close=100_000):
    start = datetime(2026, 8, 18, 9, 9)
    for index in range(21):
        price = close - (20 - index) * 200
        database.save_bar(MinuteBar(
            symbol, start + timedelta(minutes=index), price,
            price + 500, price - 500, price, 100 + index * 5,
        ))
    database.save_tick(MarketTick(symbol, start + timedelta(minutes=21), close, 1))


class FixedAI:
    def __init__(self, decision):
        self.decision = decision
        self.calls = []

    def decide(self, context):
        self.calls.append(context)
        return self.decision


class FakeSubmitter:
    def __init__(self):
        self.requests = []

    def submit_order(self, request):
        self.requests.append(request)
        return KisOrderResult(
            request.symbol,
            KisOrderSide(request.side),
            request.quantity,
            request.price,
            KisOrderType(request.order_type),
            "VTTC0012U" if request.side == KisOrderSide.BUY else "VTTC0011U",
            f"broker-{len(self.requests)}",
            {},
        )


class FakeNotifier:
    def __init__(self):
        self.messages = []

    def send(self, text):
        self.messages.append(text)


def active_control():
    return RuntimeControl(False, "2026-08-15T09:00:00+09:00", "test", "test")


def test_default_daily_trade_cap_targets_two_to_three_entries():
    assert RiskConfig().max_trades_per_day == 3


def test_watchlist_add_disable_and_list(tmp_path):
    with connect_database(tmp_path / "auto.db") as database:
        database.init_schema()
        assert database.add_watchlist_symbol("005930") is True
        assert database.add_watchlist_symbol("005930") is False
        assert database.add_watchlist_symbol("000660") is True
        assert database.list_watchlist_symbols() == ["000660", "005930"]
        assert database.set_watchlist_enabled("005930", False) is True
        assert database.list_watchlist_symbols() == ["000660"]
        assert database.list_watchlist_symbols(enabled_only=False) == ["000660", "005930"]
        assert database.set_watchlist_enabled("123456", False) is False
        with pytest.raises(ValueError):
            database.add_watchlist_symbol("ABC")


def test_ai_buy_decision_requires_valid_price_ladder():
    decision = TradingAIDecision(
        symbol="005930",
        generated_at=OPEN_MARKET_TIME,
        action=AIDecisionAction.BUY,
        confidence=0.8,
        entry_price=100_000,
        stop_loss_price=99_000,
        take_profit_price=101_500,
        risk_level=AIRiskLevel.NORMAL,
        rationale="valid setup",
    )
    assert decision.high_risk is False

    with pytest.raises(ValidationError):
        TradingAIDecision(
            symbol="005930",
            generated_at=OPEN_MARKET_TIME,
            action=AIDecisionAction.BUY,
            confidence=0.8,
            entry_price=100_000,
            stop_loss_price=100_500,
            take_profit_price=101_500,
            rationale="invalid ladder",
        )
    with pytest.raises(ValidationError):
        TradingAIDecision(
            symbol="005930", generated_at=OPEN_MARKET_TIME, action=AIDecisionAction.HOLD,
            confidence=1.2, rationale="bad confidence",
        )


def test_normal_ai_buy_ack_does_not_open_local_position(tmp_path):
    decision = TradingAIDecision(
        symbol="005930",
        generated_at=OPEN_MARKET_TIME,
        action=AIDecisionAction.BUY,
        confidence=0.9,
        entry_price=100_000,
        stop_loss_price=99_000,
        take_profit_price=101_500,
        rationale="breakout",
    )
    ai = FixedAI(decision)
    submitter = FakeSubmitter()
    notifier = FakeNotifier()
    with connect_database(tmp_path / "auto.db") as database:
        database.init_schema()
        seed_bars(database)
        report = run_auto_trade_cycle(
            ["005930"],
            database=database,
            ai_client=ai,
            submitter=submitter,
            runtime_control=active_control(),
            config=AutoTradeConfig(max_quantity=1),
            confirm_auto_trade=True,
            notifier=notifier,
            current_time=OPEN_MARKET_TIME,
        )
        positions = database.list_open_live_positions("005930")
        audits = database.list_broker_order_audits("005930")
        order = database.connection.execute(
            "SELECT * FROM broker_orders WHERE symbol=? AND side='BUY'", ("005930",)
        ).fetchone()

    assert report.submitted_count == 1
    assert submitter.requests[0].side == KisOrderSide.BUY
    assert submitter.requests[0].quantity == 1
    assert positions == []
    assert order["status"] == "ACKNOWLEDGED"
    assert audits[0]["side"] == "BUY"
    assert notifier.messages and "BUY acknowledged" in notifier.messages[0]


def test_ai_buy_strategy_must_match_deterministic_candidate_and_is_audited(tmp_path):
    decision = TradingAIDecision(
        symbol="005930",
        generated_at=OPEN_MARKET_TIME,
        action=AIDecisionAction.BUY,
        strategy="NOT_A_CANDIDATE",
        model="gpt-test",
        prompt_version="trade-decision-v2",
        confidence=0.9,
        entry_price=100_000,
        stop_loss_price=99_000,
        take_profit_price=101_500,
        rationale="invalid strategy",
    )
    with connect_database(tmp_path / "auto.db") as database:
        database.init_schema()
        seed_bars(database)
        report = run_auto_trade_cycle(
            ["005930"],
            database=database,
            ai_client=FixedAI(decision),
            submitter=FakeSubmitter(),
            runtime_control=active_control(),
            config=AutoTradeConfig(max_quantity=1),
            confirm_auto_trade=True,
            current_time=OPEN_MARKET_TIME,
        )
        audit = database.get_ai_decision_audit(decision.decision_id)

    assert report.results[0].reason == "ai_strategy_not_in_candidates"
    assert audit["strategy"] == "NOT_A_CANDIDATE"
    assert audit["model"] == "gpt-test"
    assert audit["prompt_version"] == "trade-decision-v2"


def test_high_risk_ai_buy_requests_approval_without_order(tmp_path):
    decision = TradingAIDecision(
        symbol="005930",
        generated_at=OPEN_MARKET_TIME,
        action=AIDecisionAction.BUY,
        confidence=0.85,
        entry_price=100_000,
        stop_loss_price=99_000,
        take_profit_price=101_500,
        risk_level=AIRiskLevel.HIGH,
        requires_operator_approval=True,
        rationale="volatile",
    )
    submitter = FakeSubmitter()
    notifier = FakeNotifier()
    with connect_database(tmp_path / "auto.db") as database:
        database.init_schema()
        seed_bars(database)
        report = run_auto_trade_cycle(
            ["005930"],
            database=database,
            ai_client=FixedAI(decision),
            submitter=submitter,
            runtime_control=active_control(),
            config=AutoTradeConfig(max_quantity=1),
            confirm_auto_trade=True,
            notifier=notifier,
            current_time=OPEN_MARKET_TIME,
        )
        approvals = database.connection.execute("SELECT * FROM approval_requests").fetchall()

    assert report.results[0].reason == "operator_approval_required"
    assert submitter.requests == []
    assert approvals[0]["status"] == "PENDING"
    assert notifier.messages and "Approval required" in notifier.messages[0]


def test_stop_loss_and_take_profit_sell_are_automatic(tmp_path):
    submitter = FakeSubmitter()
    opened_at = datetime(2026, 8, 18, 9, 0)
    with connect_database(tmp_path / "auto.db") as database:
        database.init_schema()
        database.open_live_position(
            position_id="pos-1",
            signal_id="signal-1",
            symbol="005930",
            quantity=2,
            entry_price=100_000,
            stop_loss_price=99_000,
            take_profit_price=101_500,
            opened_at=opened_at,
            entry_broker_order_id="entry-1",
        )
        seed_bars(database)
        database.save_tick(MarketTick("005930", OPEN_MARKET_TIME, 98_900, 1))
        report = run_auto_trade_cycle(
            ["005930"],
            database=database,
            ai_client=FixedAI(TradingAIDecision(
                symbol="005930", generated_at=OPEN_MARKET_TIME, action=AIDecisionAction.HOLD,
                confidence=0.5, rationale="unused",
            )),
            submitter=submitter,
            runtime_control=active_control(),
            confirm_auto_trade=True,
            current_time=OPEN_MARKET_TIME,
        )
        positions = database.list_open_live_positions("005930")
        order = database.connection.execute(
            "SELECT * FROM broker_orders WHERE symbol=? AND side='SELL'", ("005930",)
        ).fetchone()
        client_order_id = str(order["client_order_id"])
        assert database.apply_broker_fill(
            fill_id="sell-fill-1", client_order_id=client_order_id, quantity=2,
            price=99_000, filled_at=OPEN_MARKET_TIME, broker_order_id="broker-1",
        ) is True
        assert database.materialize_order_fills(
            client_order_id, close_reason="stop_loss"
        ) == 1
        closed_positions = database.list_open_live_positions("005930")

    assert report.results[0].action == "SELL"
    assert report.results[0].reason == "stop_loss"
    assert submitter.requests[0].side == KisOrderSide.SELL
    assert submitter.requests[0].quantity == 2
    assert order["status"] == "ACKNOWLEDGED"
    assert len(positions) == 1
    assert closed_positions == []


def test_auto_trade_confirmation_required_blocks_before_ai_call(tmp_path):
    ai = FixedAI(TradingAIDecision(
        symbol="005930", generated_at=OPEN_MARKET_TIME, action=AIDecisionAction.HOLD,
        confidence=0.5, rationale="unused",
    ))
    with connect_database(tmp_path / "auto.db") as database:
        database.init_schema()
        seed_bars(database)
        report = run_auto_trade_cycle(
            ["005930"],
            database=database,
            ai_client=ai,
            submitter=FakeSubmitter(),
            runtime_control=active_control(),
            confirm_auto_trade=False,
            current_time=OPEN_MARKET_TIME,
        )

    assert report.results[0].reason == "confirmation_required"
    assert ai.calls == []


def test_market_closed_blocks_before_ai_call(tmp_path):
    ai = FixedAI(TradingAIDecision(
        symbol="005930", generated_at=OPEN_MARKET_TIME, action=AIDecisionAction.HOLD,
        confidence=0.5, rationale="unused",
    ))
    with connect_database(tmp_path / "auto.db") as database:
        database.init_schema()
        seed_bars(database)
        report = run_auto_trade_cycle(
            ["005930"],
            database=database,
            ai_client=ai,
            submitter=FakeSubmitter(),
            runtime_control=active_control(),
            confirm_auto_trade=True,
            current_time=datetime(2026, 8, 16, 9, 30),
        )

    assert report.results[0].reason == "market_closed"
    assert ai.calls == []


def test_previous_day_live_position_is_sold_before_new_entries(tmp_path):
    submitter = FakeSubmitter()
    opened_at = datetime(2026, 8, 14, 9, 0)
    with connect_database(tmp_path / "auto.db") as database:
        database.init_schema()
        database.open_live_position(
            position_id="pos-1",
            signal_id="signal-1",
            symbol="005930",
            quantity=2,
            entry_price=100_000,
            stop_loss_price=99_000,
            take_profit_price=101_500,
            opened_at=opened_at,
            entry_broker_order_id="entry-1",
        )
        seed_bars(database)
        database.save_tick(MarketTick("005930", OPEN_MARKET_TIME, 100_500, 1))
        report = run_auto_trade_cycle(
            [],
            database=database,
            ai_client=FixedAI(TradingAIDecision(
                symbol="005930", generated_at=OPEN_MARKET_TIME, action=AIDecisionAction.HOLD,
                confidence=0.5, rationale="unused",
            )),
            submitter=submitter,
            runtime_control=active_control(),
            confirm_auto_trade=True,
            current_time=OPEN_MARKET_TIME,
        )

    assert report.results[0].action == "SELL"
    assert report.results[0].reason == "stale_previous_day_position"
    assert submitter.requests[0].side == KisOrderSide.SELL
    assert len(database.list_open_live_positions("005930")) == 1


def test_openai_client_uses_injected_session_and_structured_schema():
    class FakeResponse:
        def raise_for_status(self):
            return None

        def json(self):
            return {
                "choices": [{
                    "message": {
                        "content": (
                            '{"symbol":"005930","action":"HOLD","confidence":0.55,'
                            '"entry_price":null,"take_profit_price":null,'
                            '"stop_loss_price":null,"max_holding_seconds":null,'
                            '"risk_level":"LOW","requires_operator_approval":false,'
                            '"rationale":"No clear setup."}'
                        )
                    }
                }]
            }

    class FakeSession:
        def __init__(self):
            self.posts = []

        def post(self, url, **kwargs):
            self.posts.append((url, kwargs))
            return FakeResponse()

    session = FakeSession()
    client = OpenAITradingDecisionClient("test-key", session=session)
    decision = client.decide(AIDecisionContext("005930", {}, [], 100_000))

    assert decision.action is AIDecisionAction.HOLD
    assert len(session.posts) == 1
    url, kwargs = session.posts[0]
    assert url == "https://api.openai.com/v1/chat/completions"
    assert kwargs["headers"]["authorization"] == "Bearer test-key"
    assert kwargs["json"]["response_format"]["json_schema"]["schema"] == trading_ai_decision_schema()
