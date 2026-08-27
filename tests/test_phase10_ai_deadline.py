from datetime import datetime, timedelta

from kis_ai_scalper.ai.decision import (
    AIDecisionAction,
    OpenAITradingDecisionClient,
    TradingAIDecision,
)
from kis_ai_scalper.broker.kis_order import KisOrderResult, KisOrderSide, KisOrderType
from kis_ai_scalper.market.tick import MarketTick, MinuteBar
from kis_ai_scalper.pipeline.auto_trade import AutoTradeConfig, run_auto_trade_cycle
from kis_ai_scalper.storage import connect_database
from kis_ai_scalper.storage.database import RuntimeControl


NOW = datetime(2026, 8, 18, 9, 30)
SYMBOLS = ("005930", "000660")


def active_control() -> RuntimeControl:
    return RuntimeControl(False, NOW.isoformat(), "test", "test")


def seed_market(database, symbol: str, *, price: float = 121_000, tick_at: datetime = NOW) -> None:
    start = NOW - timedelta(minutes=21)
    for index in range(20):
        close = 100_000 + index * 1_000
        database.save_bar(MinuteBar(
            symbol, start + timedelta(minutes=index), close, close + 500, close - 500, close, 100,
        ))
    database.save_bar(MinuteBar(
        symbol, NOW - timedelta(minutes=1), price, price + 500, price - 500, price, 150,
    ))
    database.save_tick(MarketTick(symbol, tick_at, price, 1))


def buy_decision(symbol: str, *, generated_at: datetime = NOW) -> TradingAIDecision:
    return TradingAIDecision(
        symbol=symbol,
        action=AIDecisionAction.BUY,
        confidence=0.9,
        entry_price=121_000,
        stop_loss_price=120_000,
        take_profit_price=123_000,
        rationale="deterministic candidate",
        generated_at=generated_at,
    )


class CountingAI:
    def __init__(self, decision_factory=buy_decision):
        self.calls = []
        self.decision_factory = decision_factory

    def decide(self, context):
        self.calls.append(context.symbol)
        return self.decision_factory(context.symbol)


class Submitter:
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
            "VTTC0012U",
            f"broker-{len(self.requests)}",
            {},
        )


class Session:
    def __init__(self):
        self.posts = 0

    def post(self, *_args, **_kwargs):
        self.posts += 1
        raise AssertionError("the OpenAI endpoint must not be called")


def run(database, ai, *, config=None, clock=None, symbols=(SYMBOLS[0],), submitter=None):
    return run_auto_trade_cycle(
        list(symbols),
        database=database,
        ai_client=ai,
        submitter=submitter or Submitter(),
        runtime_control=active_control(),
        config=config or AutoTradeConfig(max_quantity=1),
        confirm_auto_trade=True,
        current_time=NOW,
        clock=clock,
    )


def test_empty_deterministic_scan_blocks_openai_before_http_call(tmp_path):
    session = Session()
    ai = OpenAITradingDecisionClient("test-key", session=session, max_retries=0)
    with connect_database(tmp_path / "empty-candidate.db") as database:
        database.init_schema()
        for index in range(21):
            start = NOW - timedelta(minutes=21 - index)
            database.save_bar(MinuteBar("005930", start, 100, 101, 99, 100, 100))
        database.save_tick(MarketTick("005930", NOW, 100, 1))
        report = run(database, ai)

    assert report.results[0].reason == "no_deterministic_candidate"
    assert session.posts == 0


def test_previous_session_bars_do_not_create_current_session_candidate(tmp_path):
    session = Session()
    ai = OpenAITradingDecisionClient("test-key", session=session, max_retries=0)
    with connect_database(tmp_path / "cross-session-candidate.db") as database:
        database.init_schema()
        previous_session = NOW - timedelta(days=1, minutes=21)
        for index in range(20):
            close = 100_000 + index * 1_000
            database.save_bar(MinuteBar(
                "005930",
                previous_session + timedelta(minutes=index),
                close,
                close + 500,
                close - 500,
                close,
                100,
            ))
        database.save_bar(MinuteBar(
            "005930",
            NOW - timedelta(minutes=1),
            121_000,
            121_500,
            120_500,
            121_000,
            150,
        ))
        database.save_tick(MarketTick("005930", NOW, 121_000, 1))

        report = run(database, ai)

    assert report.results[0].reason == "no_deterministic_candidate"
    assert session.posts == 0


def test_old_ai_response_is_blocked_before_order(tmp_path):
    ai = CountingAI(lambda symbol: buy_decision(symbol, generated_at=NOW - timedelta(seconds=6)))
    submitter = Submitter()
    with connect_database(tmp_path / "old-response.db") as database:
        database.init_schema()
        seed_market(database, SYMBOLS[0])
        report = run(
            database,
            ai,
            config=AutoTradeConfig(max_quantity=1, max_ai_response_age_seconds=5),
            clock=lambda: NOW,
            submitter=submitter,
        )

    assert report.results[0].reason == "ai_response_too_old"
    assert submitter.requests == []


def test_price_drift_during_ai_is_blocked_before_order(tmp_path):
    submitter = Submitter()

    class MovingAI(CountingAI):
        def decide(self, context):
            self.calls.append(context.symbol)
            database.save_tick(MarketTick(context.symbol, NOW, 124_000, 1))
            return buy_decision(context.symbol)

    with connect_database(tmp_path / "price-drift.db") as database:
        database.init_schema()
        seed_market(database, SYMBOLS[0])
        ai = MovingAI()
        report = run(database, ai, clock=lambda: NOW, submitter=submitter)

    assert report.results[0].reason == "price_moved_during_ai"
    assert submitter.requests == []


def test_stale_tick_after_ai_is_blocked_before_order(tmp_path):
    submitter = Submitter()

    class StalingAI(CountingAI):
        def decide(self, context):
            self.calls.append(context.symbol)
            database.save_tick(MarketTick(
                context.symbol, NOW + timedelta(seconds=1), 121_000, 1,
            ))
            return buy_decision(context.symbol)

    with connect_database(tmp_path / "stale-after-ai.db") as database:
        database.init_schema()
        seed_market(database, SYMBOLS[0])
        ai = StalingAI()
        report = run(database, ai, clock=lambda: NOW, submitter=submitter)

    assert report.results[0].reason == "stale_tick_after_ai"
    assert submitter.requests == []


def test_cycle_deadline_blocks_remaining_symbols_without_ai_call(tmp_path):
    current = [NOW]

    def clock():
        return current[0]

    class SlowFirstAI(CountingAI):
        def decide(self, context):
            self.calls.append(context.symbol)
            current[0] = NOW + timedelta(seconds=2)
            return TradingAIDecision(
                symbol=context.symbol,
                action=AIDecisionAction.HOLD,
                confidence=0.5,
                rationale="deadline test",
                generated_at=NOW,
            )

    with connect_database(tmp_path / "deadline.db") as database:
        database.init_schema()
        for symbol in SYMBOLS:
            seed_market(database, symbol)
        ai = SlowFirstAI()
        report = run(
            database,
            ai,
            config=AutoTradeConfig(max_quantity=1, cycle_deadline_seconds=1),
            clock=clock,
            symbols=SYMBOLS,
        )

    assert ai.calls == [SYMBOLS[0]]
    assert [result.reason for result in report.results] == [
        "cycle_deadline_exceeded",
        "cycle_deadline_exceeded",
    ]
