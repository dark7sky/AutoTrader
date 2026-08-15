from datetime import datetime, timedelta, timezone
import json

import pytest

from kis_ai_scalper.ai.decision import AIDecisionAction, AIRiskLevel, TradingAIDecision
from kis_ai_scalper.broker.kis_order import KisOrderResult, KisOrderSide, KisOrderType
from kis_ai_scalper.broker.kis_order_status import KisOrderStatus, KisOrderStatusRecord
from kis_ai_scalper.market.tick import MarketTick, MinuteBar
from kis_ai_scalper.ops.telegram import handle_update
from kis_ai_scalper.pipeline.auto_trade import AutoTradeConfig, run_auto_trade_cycle
from kis_ai_scalper.pipeline.broker_reconciliation import reconcile_broker_state
from kis_ai_scalper.risk import PortfolioState
from kis_ai_scalper.storage import connect_database
from kis_ai_scalper.storage.database import RuntimeControl


NOW = datetime(2026, 8, 18, 9, 30)
SYMBOL = "005930"


class FakeTelegram:
    def __init__(self):
        self.sent = []
        self.answered = []

    def send_message(self, chat_id, text, reply_markup=None):
        self.sent.append((str(chat_id), text, reply_markup))

    def answer_callback_query(self, callback_query_id, text=None):
        self.answered.append((callback_query_id, text))


class FixedAI:
    def __init__(self, decision):
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
            request.symbol, KisOrderSide(request.side), request.quantity, request.price,
            KisOrderType(request.order_type), "VTTC0012U", "broker-1", {},
        )


class BadNotifier:
    def send(self, text):
        raise RuntimeError("telegram is unavailable")


def private(text, chat_id=42, user_id=8):
    return {"message": {
        "chat": {"id": chat_id, "type": "private"},
        "from": {"id": user_id}, "text": text,
    }}


def seed_market(database):
    for index in range(21):
        start = NOW - timedelta(minutes=20 - index + 1)
        database.save_bar(MinuteBar(SYMBOL, start, 100_000, 100_500, 99_500, 100_000, 100))
    database.save_tick(MarketTick(SYMBOL, NOW, 100_000, 1))


def high_risk_decision():
    return TradingAIDecision(
        decision_id="decision-high-1", symbol=SYMBOL, action=AIDecisionAction.BUY,
        confidence=0.9, entry_price=100_000, stop_loss_price=99_000,
        take_profit_price=102_000, max_holding_seconds=321,
        risk_level=AIRiskLevel.HIGH, requires_operator_approval=True,
        rationale="high risk test",
    )


def control():
    return RuntimeControl(False, NOW.isoformat(), "test", "test", "demo")


def test_approval_transitions_are_atomic_one_shot_and_expire(tmp_path):
    path = tmp_path / "approval.db"
    with connect_database(path) as database:
        database.init_schema()
        assert database.record_approval_request(
            request_id="approval:005930:202608180930", symbol=SYMBOL,
            decision_id="decision-1", signal_id="entry-1", reason="high risk",
            quantity=1, entry_price=100_000, take_profit_price=102_000,
            stop_loss_price=99_000, created_at=NOW,
        )
        assert database.resolve_approval_request(
            "approval:005930:202608180930", "APPROVED", resolved_by="8", now=NOW
        )
        assert not database.resolve_approval_request(
            "approval:005930:202608180930", "REJECTED", resolved_by="8", now=NOW
        )
        assert database.consume_approval_request("approval:005930:202608180930", now=NOW)
        assert not database.consume_approval_request("approval:005930:202608180930", now=NOW)
        assert database.finish_approval_request(
            "approval:005930:202608180930", success=True, now=NOW
        )
        assert not database.finish_approval_request(
            "approval:005930:202608180930", success=False, now=NOW
        )
        assert database.get_approval_request("approval:005930:202608180930")["status"] == "EXECUTED"

        assert database.record_approval_request(
            request_id="approval:005930:202608180931", symbol=SYMBOL,
            decision_id="decision-2", reason="expired", created_at=NOW,
            expires_at=NOW + timedelta(minutes=2),
        )
        assert not database.resolve_approval_request(
            "approval:005930:202608180931", "APPROVED", now=NOW + timedelta(minutes=2)
        )
        assert database.get_approval_request("approval:005930:202608180931")["status"] == "EXPIRED"


def test_telegram_approval_commands_buttons_and_authentication(tmp_path):
    path = str(tmp_path / "telegram.db")
    with connect_database(path) as database:
        database.init_schema()
        database.record_approval_request(
            request_id="approval:005930:202608180930", symbol=SYMBOL,
            decision_id="decision-1", reason="high risk", created_at=NOW,
        )
    telegram = FakeTelegram()
    assert handle_update(private("/approvals"), path, telegram, "42", "8")
    assert telegram.sent[-1][2]["inline_keyboard"]
    assert not handle_update(private("/approve approval:005930:202608180930", user_id=7), path, telegram, "42", "8")
    assert handle_update(private("/approve approval:005930:202608180930"), path, telegram, "42", "8")
    with connect_database(path) as database:
        assert database.get_approval_request("approval:005930:202608180930")["status"] == "APPROVED"
    assert "approved" in telegram.sent[-1][1]
    assert handle_update(private("/approve approval:005930:202608180930"), path, telegram, "42", "8")
    assert "approved" in telegram.sent[-1][1]


def test_high_risk_reuses_original_audit_and_budget_gate_consumes_once(tmp_path):
    path = tmp_path / "auto.db"
    ai = FixedAI(high_risk_decision())
    submitter = Submitter()
    with connect_database(path) as database:
        database.init_schema()
        seed_market(database)
        first = run_auto_trade_cycle(
            [SYMBOL], database=database, ai_client=ai, submitter=submitter,
            runtime_control=control(), config=AutoTradeConfig(max_quantity=1),
            confirm_auto_trade=True, current_time=NOW,
        )
        request_id = "approval:005930:202608180929"
        request = database.list_approval_requests()[0]
        assert first.results[0].reason == "operator_approval_required"
        assert request["max_holding_seconds"] == 321
        assert database.resolve_approval_request(request["request_id"], "APPROVED", now=NOW)
        second = run_auto_trade_cycle(
            [SYMBOL], database=database, ai_client=ai, submitter=submitter,
            runtime_control=control(), config=AutoTradeConfig(max_quantity=1),
            confirm_auto_trade=True, current_time=NOW,
            entry_budget_checker=lambda symbol, price, quantity: True,
        )
        assert second.submitted_count == 1
        assert ai.calls == 1
        assert database.get_approval_request(request["request_id"])["status"] == "EXECUTED"
        assert len(submitter.requests) == 1


def test_approved_entry_budget_failure_is_fail_closed(tmp_path):
    path = tmp_path / "budget.db"
    with connect_database(path) as database:
        database.init_schema()
        seed_market(database)
        ai = FixedAI(high_risk_decision())
        submitter = Submitter()
        run_auto_trade_cycle(
            [SYMBOL], database=database, ai_client=ai, submitter=submitter,
            runtime_control=control(), config=AutoTradeConfig(max_quantity=1),
            confirm_auto_trade=True, current_time=NOW,
        )
        request = database.list_approval_requests()[0]
        database.resolve_approval_request(request["request_id"], "APPROVED", now=NOW)
        report = run_auto_trade_cycle(
            [SYMBOL], database=database, ai_client=ai, submitter=submitter,
            runtime_control=control(), config=AutoTradeConfig(max_quantity=1),
            confirm_auto_trade=True, current_time=NOW,
            entry_budget_checker=lambda symbol, price, quantity: False,
        )
    assert report.results[0].reason == "entry_budget_unavailable_or_insufficient"
    assert submitter.requests == []


def test_reconciliation_restores_max_holding_from_ai_audit(tmp_path):
    with connect_database(tmp_path / "reconcile.db") as database:
        database.init_schema()
        database.claim_order_intent(
            client_order_id="client-1", signal_id="decision-1", symbol=SYMBOL,
            side="BUY", requested_qty=1, requested_price=100,
        )
        database.record_order_submission("client-1", "broker-1")
        database.record_ai_decision(
            decision_id="decision-1", symbol=SYMBOL, action="BUY", confidence=0.9,
            entry_price=100, take_profit_price=104, stop_loss_price=99,
            risk_level="NORMAL", requires_operator_approval=False, rationale="test",
            max_holding_seconds=321, created_at=NOW,
        )
        order = KisOrderStatusRecord(
            order_number="broker-1", symbol=SYMBOL, side=KisOrderSide.BUY,
            ordered_quantity=1, filled_quantity=1, remaining_quantity=0,
            order_price=100, average_fill_price=100, status=KisOrderStatus.FILLED,
            order_time="093000", order_date="20260818",
        )
        class Orders:
            def get_today_orders(self): return (order,)
        class Account:
            def get_snapshot(self):
                from kis_ai_scalper.broker.kis_account import KisAccountSnapshot, KisAccountSummary, KisAccountPosition
                return KisAccountSnapshot((KisAccountPosition(SYMBOL, 1, 1, 100, 100, 0),), KisAccountSummary(1, 1, 1, 0))
        reconcile_broker_state(database, Orders(), Account(), current_time=NOW)
        position = database.list_open_live_positions(SYMBOL)[0]
    assert position["max_holding_seconds"] == 321


def test_environment_switch_and_notifier_fail_closed_without_breaking_order(tmp_path):
    with connect_database(tmp_path / "env.db") as database:
        database.init_schema()
        database.open_live_position(
            position_id="p1", signal_id="s1", symbol=SYMBOL, quantity=1,
            entry_price=100, stop_loss_price=99, take_profit_price=102,
            opened_at=NOW, entry_broker_order_id="broker-1",
        )
        with pytest.raises(ValueError, match="open positions"):
            database.set_runtime_environment("real", "test", "test")
    telegram = FakeTelegram()
    assert handle_update(private("/env real"), str(tmp_path / "env.db"), telegram, "42", "8")
    code = telegram.sent[-1][1].split("challenge: ", 1)[1].split("\n", 1)[0]
    assert handle_update(private(f"/confirm-real {code}"), str(tmp_path / "env.db"), telegram, "42", "8")
    assert "open positions" in telegram.sent[-1][1]

    with connect_database(tmp_path / "notify.db") as database:
        database.init_schema()
        seed_market(database)
        report = run_auto_trade_cycle(
            [SYMBOL], database=database, ai_client=FixedAI(TradingAIDecision(
                symbol=SYMBOL, action=AIDecisionAction.BUY, confidence=0.9,
                entry_price=100_000, stop_loss_price=99_000, take_profit_price=102_000,
                rationale="normal",
            )), submitter=Submitter(), runtime_control=control(),
            config=AutoTradeConfig(max_quantity=1), confirm_auto_trade=True,
            current_time=NOW, notifier=BadNotifier(),
        )
    assert report.submitted_count == 1


def test_telegram_live_snapshot_status_report_and_positions_are_bounded(tmp_path):
    path = str(tmp_path / "snapshot.db")
    with connect_database(path) as database:
        database.init_schema()
        database.record_heartbeat("trading-service", heartbeat_at=datetime.now(timezone.utc))
        database.set_runtime_metadata("operator_review", "true")
        database.set_runtime_metadata("block_new_entries", "true")
        database.set_runtime_metadata("emergency_stop", "false")
        database.set_runtime_metadata("live_report_snapshot", json.dumps({
            "environment": "real",
            "summary": {"orderable_cash": 123456, "total_eval": 456789, "daily_pnl": -10},
            "positions": [{"symbol": SYMBOL, "qty": 2, "avg_price": 100000}],
            "operator_review": True, "block_new_entries": True,
        }))
    telegram = FakeTelegram()
    assert handle_update(private("/status"), path, telegram, "42", "8")
    assert "trading_service:" in telegram.sent[-1][1]
    assert "emergency_stop: false" in telegram.sent[-1][1]
    assert handle_update(private("/report"), path, telegram, "42", "8")
    assert "live broker report" in telegram.sent[-1][1]
    assert handle_update(private("/positions"), path, telegram, "42", "8")
    assert "005930 qty=2" in telegram.sent[-1][1]
    assert len(telegram.sent[-1][1]) <= 3500


def test_market_storage_canonicalizes_aware_and_naive_kst_keys(tmp_path):
    aware_kst = datetime(2026, 8, 18, 9, 29, tzinfo=timezone(timedelta(hours=9)))
    naive_kst = datetime(2026, 8, 18, 9, 29)
    with connect_database(tmp_path / "market-time.db") as database:
        database.init_schema()
        database.save_bar(MinuteBar(SYMBOL, aware_kst, 1, 2, 1, 2, 1))
        database.save_bar(MinuteBar(SYMBOL, naive_kst, 3, 4, 3, 4, 2))
        database.save_tick(MarketTick(SYMBOL, aware_kst, 10, 1))
        bar = database.latest_bar(SYMBOL)
        tick = database.latest_tick(SYMBOL)
        count = database.connection.execute(
            "SELECT COUNT(*) FROM bars_1m WHERE symbol=?", (SYMBOL,)
        ).fetchone()[0]
    assert count == 1
    assert bar.start == naive_kst and bar.close == 4
    assert bar.start.tzinfo is None
    assert tick.timestamp == naive_kst and tick.timestamp.tzinfo is None
