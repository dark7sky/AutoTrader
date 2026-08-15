from datetime import datetime, timedelta
from types import SimpleNamespace

import pytest

from kis_ai_scalper import cli
from kis_ai_scalper.broker.kis_account import KisAccountSnapshot, KisAccountSummary
from kis_ai_scalper.broker.kis_buying_power import KisBuyingPowerSnapshot
from kis_ai_scalper.broker.kis_order_status import KisOrderStatus
from kis_ai_scalper.market.clock import KST
from kis_ai_scalper.pipeline.broker_reconciliation import reconcile_broker_state
from kis_ai_scalper.pipeline.order_management import (
    can_re_evaluate_entry,
    manage_stale_orders,
)
from kis_ai_scalper.storage import connect_database
from tests.test_phase9_broker_reconciliation import (
    FakeAccount as ReconciliationAccount,
    FakeOrders as ReconciliationOrders,
    NOW as RECONCILIATION_NOW,
    account_position,
    seed_order as seed_reconciliation_order,
    status as kis_order_status,
)
from tests.test_phase9_execution_safety import (
    FixedAI,
    Submitter,
    SYMBOL,
    buy_decision,
    run as run_auto_trade,
    seed_market,
)
from tests.test_phase9_order_management import (
    FakeStatusClient,
    broker_order,
    seed_order as seed_managed_order,
)


@pytest.mark.parametrize(
    ("broker_state", "filled", "remaining", "broker_position"),
    [
        (KisOrderStatus.UNFILLED, 0, 10, ()),
        (KisOrderStatus.PARTIALLY_FILLED, 3, 7, (3,)),
    ],
    ids=["unfilled", "partially-filled"],
)
def test_cancel_pending_survives_nonterminal_kis_states_until_terminal(
    tmp_path, broker_state, filled, remaining, broker_position
):
    with connect_database(tmp_path / "reconciliation.db") as database:
        database.init_schema()
        seed_reconciliation_order(database, status="CANCEL_PENDING")

        pending_order = kis_order_status(
            filled=filled, remaining=remaining, state=broker_state
        )
        pending_result = reconcile_broker_state(
            database,
            ReconciliationOrders([pending_order]),
            ReconciliationAccount(
                [account_position(broker_position[0])] if broker_position else []
            ),
            current_time=RECONCILIATION_NOW,
        )

        order_while_pending = database.get_broker_order("client-1")
        assert pending_result.operator_review is False
        assert order_while_pending["status"] == "CANCEL_PENDING"
        assert order_while_pending["completed_at"] is None

        terminal_order = kis_order_status(
            filled=filled, remaining=0, state=KisOrderStatus.CANCELLED
        )
        terminal_result = reconcile_broker_state(
            database,
            ReconciliationOrders([terminal_order]),
            ReconciliationAccount(
                [account_position(broker_position[0])] if broker_position else []
            ),
            current_time=RECONCILIATION_NOW,
        )

        completed_order = database.get_broker_order("client-1")

    assert terminal_result.operator_review is False
    assert completed_order["status"] == "CANCELLED"
    assert completed_order["completed_at"] is not None


def test_same_completed_bar_submits_once_when_ai_decision_id_changes(tmp_path):
    with connect_database(tmp_path / "idempotency.db") as database:
        database.init_schema()
        seed_market(database)
        completed_bar = database.latest_bar(SYMBOL)
        submitter = Submitter()

        first = run_auto_trade(
            database,
            FixedAI(buy_decision(decision_id="decision-1")),
            submitter,
        )
        second = run_auto_trade(
            database,
            FixedAI(buy_decision(decision_id="decision-2")),
            submitter,
        )

        orders = database.connection.execute(
            "SELECT * FROM broker_orders WHERE symbol=? AND side='BUY'",
            (SYMBOL,),
        ).fetchall()
        decisions = database.connection.execute(
            "SELECT decision_id FROM ai_decision_audits WHERE symbol=? ORDER BY decision_id",
            (SYMBOL,),
        ).fetchall()

    assert completed_bar is not None
    assert first.results[0].submitted is True
    assert second.results[0].submitted is False
    assert second.results[0].reason == "order_already_claimed"
    assert len(submitter.requests) == 1
    assert len(orders) == 1
    assert [row["decision_id"] for row in decisions] == ["decision-1", "decision-2"]


def test_confirmed_cancel_blocks_same_bar_but_allows_only_new_bar(tmp_path):
    now = datetime(2026, 8, 18, 9, 2, tzinfo=KST)
    with connect_database(tmp_path / "bar-reentry.db") as database:
        database.init_schema()
        seed_managed_order(database, submitted_at=now - timedelta(seconds=61))
        client = FakeStatusClient((broker_order(),))

        pending = manage_stale_orders(
            database, client, current_time=now, entry_bar_key="bar-1"
        )
        same_bar_while_pending = can_re_evaluate_entry(database, SYMBOL, "bar-1")

        client.orders = (broker_order(status=KisOrderStatus.CANCELLED, remaining=0),)
        confirmed = manage_stale_orders(
            database, client, current_time=now + timedelta(minutes=1)
        )
        same_bar_after_cancel = can_re_evaluate_entry(database, SYMBOL, "bar-1")
        new_bar_after_cancel = can_re_evaluate_entry(database, SYMBOL, "bar-2")

        order = database.get_broker_order("client-1")

    assert pending.cancel_requested == 1
    assert same_bar_while_pending.allowed is False
    assert confirmed.confirmed == 1
    assert order["status"] == "CANCELLED"
    assert same_bar_after_cancel.allowed is False
    assert same_bar_after_cancel.reason == "same_bar_after_cancel"
    assert new_bar_after_cancel.allowed is True
    assert new_bar_after_cancel.reason == "cancel_confirmed_new_bar"
    assert len(client.cancel_calls) == 1


def test_initial_broker_cycle_uses_first_watchlist_quote_for_buying_power_without_order(
    tmp_path, monkeypatch
):
    account = KisAccountSnapshot(
        positions=(),
        summary=KisAccountSummary(1_000_000, 1_000_000, 1_000_000, 0),
    )
    quote_calls = []
    buying_power_calls = []

    class FakeAuth:
        def __init__(self, *args, **kwargs):
            pass

        def authenticate_read_only(self, **kwargs):
            return SimpleNamespace(access_token="token", approval_key="approval")

    class FakeAccountClient:
        def __init__(self, *args, **kwargs):
            pass

        def get_snapshot(self):
            return account

    class FakeOrderStatusClient:
        def __init__(self, *args, **kwargs):
            pass

        def get_today_orders(self):
            return ()

    class FakeBuyingPowerClient:
        def __init__(self, *args, **kwargs):
            pass

        def get_snapshot(self, symbol, price, *args):
            buying_power_calls.append((symbol, price, args))
            return KisBuyingPowerSnapshot(
                symbol, price, 1_000_000, 1_000_000, 1_000_000, 10, {}
            )

    class FakeOrderClient:
        def __init__(self, *args, **kwargs):
            self.submit_calls = 0

        def submit_order(self, request):
            self.submit_calls += 1
            raise AssertionError("initial broker cycle must not submit orders")

    class FakeRestClient:
        def __init__(self, *args, **kwargs):
            pass

        def get_current_price(self, symbol):
            quote_calls.append(symbol)
            return SimpleNamespace(symbol=symbol, price=70_123.0)

    monkeypatch.setattr(cli, "load_config", lambda _: object())
    monkeypatch.setattr(
        cli, "_kis_api_for", lambda *_: SimpleNamespace(app_key="key", app_secret="secret")
    )
    monkeypatch.setattr(
        cli,
        "_kis_account_for",
        lambda *_: SimpleNamespace(account_no="12345678", account_product_code="01"),
    )
    monkeypatch.setattr(cli, "KisAuthClient", FakeAuth)
    monkeypatch.setattr(cli, "KisAccountClient", FakeAccountClient)
    monkeypatch.setattr(cli, "KisOrderStatusClient", FakeOrderStatusClient)
    monkeypatch.setattr(cli, "KisBuyingPowerClient", FakeBuyingPowerClient)
    monkeypatch.setattr(cli, "KisOrderClient", FakeOrderClient)
    monkeypatch.setattr(cli, "KisRestClient", FakeRestClient)
    monkeypatch.setattr(cli, "reconcile_broker_state", lambda *args, **kwargs: cli.ReconciliationReport())
    monkeypatch.setattr(cli, "manage_stale_orders", lambda *args, **kwargs: cli.OrderManagementReport())

    db_path = tmp_path / "initial-cycle.db"
    state = cli._make_broker_cycle_state(
        "config/settings.yaml", cli.KisEnvironment.DEMO, str(db_path), ["005930", "000660"], False
    )

    assert quote_calls == ["005930"]
    assert buying_power_calls == [("005930", 70_123.0, ())]
    assert "buying_power" not in state.portfolio.unknown_fields
    assert state.order_client.submit_calls == 0
    with connect_database(db_path) as database:
        database.init_schema()
        assert database.connection.execute("SELECT COUNT(*) FROM broker_orders").fetchone()[0] == 0
