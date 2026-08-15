from datetime import datetime, timedelta
import math

import pytest

from kis_ai_scalper.ai.decision import AIDecisionAction, TradingAIDecision
from kis_ai_scalper.broker.kis_order import KisOrderResult, KisOrderSide, KisOrderType
from kis_ai_scalper.market.tick import MarketTick, MinuteBar
from kis_ai_scalper.pipeline.auto_trade import AutoTradeConfig, run_auto_trade_cycle
from kis_ai_scalper.storage import connect_database
from kis_ai_scalper.storage.database import RuntimeControl


NOW = datetime(2026, 8, 18, 9, 30)
SYMBOL = "005930"


def seed_market(database, *, price: float = 121_000) -> None:
    start = NOW - timedelta(minutes=21)
    for index in range(20):
        close = 100_000 + index * 1_000
        database.save_bar(MinuteBar(
            SYMBOL, start + timedelta(minutes=index), close, close + 500, close - 500, close, 100,
        ))
    database.save_bar(MinuteBar(
        SYMBOL, NOW - timedelta(minutes=1), price, price + 500, price - 500, price, 150,
    ))
    database.save_tick(MarketTick(SYMBOL, NOW, price, 1))


def active_control() -> RuntimeControl:
    return RuntimeControl(False, NOW.isoformat(), "test", "test")


def buy_decision(*, entry_price: float = 121_000) -> TradingAIDecision:
    return TradingAIDecision(
        symbol=SYMBOL,
        action=AIDecisionAction.BUY,
        confidence=0.9,
        entry_price=entry_price,
        stop_loss_price=120_000,
        take_profit_price=123_000,
        rationale="live price validation test",
        generated_at=NOW,
    )


class FixedAI:
    def __init__(self, decision: TradingAIDecision):
        self.decision = decision
        self.calls = 0

    def decide(self, context):
        self.calls += 1
        return self.decision


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


def run_cycle(database, ai, submitter, *, checker=None, decision=None):
    return run_auto_trade_cycle(
        [SYMBOL],
        database=database,
        ai_client=ai or FixedAI(decision or buy_decision()),
        submitter=submitter,
        runtime_control=active_control(),
        config=AutoTradeConfig(max_quantity=1),
        confirm_auto_trade=True,
        current_time=NOW,
        post_ai_price_checker=checker,
    )


def test_post_ai_price_checker_runs_for_buy_and_allows_current_entry(tmp_path):
    checked = []
    submitter = Submitter()
    with connect_database(tmp_path / "valid.db") as database:
        database.init_schema()
        seed_market(database)
        report = run_cycle(
            database,
            FixedAI(buy_decision()),
            submitter,
            checker=lambda symbol: checked.append(symbol) or 121_000,
        )

    assert checked == [SYMBOL]
    assert report.submitted_count == 1
    assert len(submitter.requests) == 1


def test_post_ai_price_checker_exception_blocks_with_sanitized_reason(tmp_path):
    submitter = Submitter()

    def broken(_symbol):
        raise RuntimeError("broker-secret-must-not-escape")

    with connect_database(tmp_path / "exception.db") as database:
        database.init_schema()
        seed_market(database)
        report = run_cycle(database, FixedAI(buy_decision()), submitter, checker=broken)

    assert report.results[0].reason == "post_ai_price_check_failed"
    assert "broker-secret" not in report.results[0].reason
    assert submitter.requests == []


@pytest.mark.parametrize("price", [0.0, -1.0, math.nan, math.inf, -math.inf])
def test_invalid_post_ai_price_blocks_fail_closed(tmp_path, price):
    submitter = Submitter()
    with connect_database(tmp_path / "invalid-price.db") as database:
        database.init_schema()
        seed_market(database)
        report = run_cycle(database, FixedAI(buy_decision()), submitter, checker=lambda _symbol: price)

    assert report.results[0].reason == "post_ai_price_invalid"
    assert submitter.requests == []


def test_fresh_price_deviation_from_ai_snapshot_blocks_entry(tmp_path):
    submitter = Submitter()
    with connect_database(tmp_path / "snapshot-drift.db") as database:
        database.init_schema()
        seed_market(database)
        report = run_cycle(database, FixedAI(buy_decision()), submitter, checker=lambda _symbol: 123_000)

    assert report.results[0].reason == "post_ai_price_deviation_too_large"
    assert submitter.requests == []


def test_entry_must_stay_within_deviation_of_fresh_price(tmp_path):
    submitter = Submitter()
    with connect_database(tmp_path / "entry-drift.db") as database:
        database.init_schema()
        seed_market(database)
        report = run_cycle(
            database,
            FixedAI(buy_decision(entry_price=121_500)),
            submitter,
            checker=lambda _symbol: 120_000,
        )

    assert report.results[0].reason == "entry_deviation_from_live_too_large"
    assert submitter.requests == []
