from datetime import datetime, timezone
from dataclasses import dataclass

import pytest

from kis_ai_scalper.broker.kis_account import (
    KisAccountPosition,
    KisAccountSnapshot,
    KisAccountSummary,
)
from kis_ai_scalper.broker.kis_buying_power import KisBuyingPowerSnapshot
from kis_ai_scalper.broker.kis_realized_pnl import (
    KisRealizedPnlClient,
    KisRealizedPnlSnapshot,
    KisRealizedPnlUnsupportedError,
    parse_realized_pnl,
)
from kis_ai_scalper.broker.kis_auth import KisHttpError
from kis_ai_scalper.risk.portfolio_snapshot import (
    build_portfolio_risk_snapshot,
    calculate_local_daily_realized_pnl,
    validate_entry_budget,
)
from kis_ai_scalper.storage import connect_database


NOW = datetime(2026, 8, 18, 10, 0, tzinfo=timezone.utc)


@dataclass
class FakeResponse:
    payload: dict
    headers: dict[str, str] | None = None
    status_code: int = 200

    def raise_for_status(self):
        return None

    def json(self):
        return self.payload


class FakeSession:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []

    def get(self, url, **kwargs):
        self.calls.append((url, kwargs))
        return self.responses.pop(0)


def account(*, cash=1_000_000, price=100.0, pnl=None):
    return KisAccountSnapshot(
        positions=(KisAccountPosition("005930", 10, 10, 90.0, price, 100.0),),
        summary=KisAccountSummary(cash, cash, 1_001_000, pnl),
    )


def test_realized_pnl_parser_ignores_evaluation_pnl():
    result = parse_realized_pnl({
        "output1": [{"evlu_pfls_amt": "999999"}],
        "output2": [{"rlzt_pfls": "-1250", "evlu_pfls_smtl_amt": "999999"}],
    })
    assert result.daily_realized_pnl == -1250
    assert parse_realized_pnl({"output2": [{"evlu_pfls_smtl_amt": "999999"}]}).daily_realized_pnl is None


def test_realized_pnl_client_rejects_undocumented_demo_before_network():
    session = FakeSession([FakeResponse(
        {"rt_cd": "0", "output1": [], "output2": [{"rlzt_pfls": "-1250"}]},
        {"tr_cont": "N"},
    )])
    client = KisRealizedPnlClient(
        "demo", "app-key", "app-secret", "access-token", "12345678", "01", session=session
    )
    with pytest.raises(KisRealizedPnlUnsupportedError, match="not officially documented"):
        client.get_snapshot()
    assert session.calls == []


def test_realized_pnl_client_uses_official_real_tr_id_and_params():
    session = FakeSession([FakeResponse(
        {"rt_cd": "0", "output1": [], "output2": [{"rlzt_pfls": "-1250"}]},
        {"tr_cont": "N"},
    )])
    client = KisRealizedPnlClient(
        "real", "app-key", "app-secret", "access-token", "12345678", "01", session=session
    )
    assert client.get_snapshot().daily_realized_pnl == -1250
    _, kwargs = session.calls[0]
    assert kwargs["headers"]["tr_id"] == "TTTC8494R"
    assert kwargs["params"]["PRCS_DVSN"] == "01"
    assert kwargs["params"]["COST_ICLD_YN"] == "N"


def test_realized_pnl_client_missing_value_and_http_error_fail_closed_safely():
    missing = FakeSession([FakeResponse(
        {"rt_cd": "0", "output1": [], "output2": [{"evlu_pfls_smtl_amt": "100"}]},
        {"tr_cont": "N"},
    )])
    result = KisRealizedPnlClient(
        "real", "a", "b", "c", "12345678", "01", session=missing
    ).get_snapshot()
    assert result.daily_realized_pnl is None

    failed = FakeSession([FakeResponse(
        {"msg1": "b c"}, {"tr_cont": "N"}, 403
    )])
    with pytest.raises(KisHttpError) as caught:
        KisRealizedPnlClient(
            "real", "a", "b", "c", "12345678", "01", session=failed
        ).get_snapshot()
    assert " b " not in str(caught.value)
    assert " c" not in str(caught.value)


def add_order(database, client, symbol="005930", when="2026-08-18T09:30:00+00:00", side="BUY"):
    assert database.claim_order_intent(
        client_order_id=client,
        signal_id=client,
        symbol=symbol,
        side=side,
        requested_qty=2,
        requested_price=100.0,
        created_at=datetime.fromisoformat(when),
    )


def test_builds_real_positions_and_market_exposure():
    with connect_database(":memory:") as database:
        database.init_schema()
        snapshot = build_portfolio_risk_snapshot(account(pnl=10), database, now=NOW)
        assert snapshot.portfolio.open_positions[0].symbol == "005930"
        assert snapshot.portfolio.current_exposure_krw == 1000


def test_counts_today_orders_and_groups_by_symbol():
    with connect_database(":memory:") as database:
        database.init_schema()
        add_order(database, "a")
        add_order(database, "b", "000660")
        add_order(database, "old", when="2026-08-17T09:30:00+00:00")
        result = build_portfolio_risk_snapshot(account(pnl=10), database, now=NOW)
        assert result.broker_orders_today == 2
        assert result.portfolio.orders_by_symbol == {"005930": 1, "000660": 1}


def test_counts_today_fills_from_broker_fills():
    with connect_database(":memory:") as database:
        database.init_schema()
        add_order(database, "a")
        database.apply_broker_fill(
            fill_id="f1", client_order_id="a", quantity=1, price=100,
            filled_at=NOW,
        )
        result = build_portfolio_risk_snapshot(account(pnl=10), database, now=NOW)
        assert result.broker_fills_today == 1


def test_unknown_cash_and_pnl_fail_closed():
    with connect_database(":memory:") as database:
        database.init_schema()
        summary = KisAccountSummary(100, None, 100, None)
        result = build_portfolio_risk_snapshot(
            KisAccountSnapshot((), summary), database, now=NOW
        )
        assert {"orderable_cash", "daily_pnl"} <= result.unknown_fields
        assert result.portfolio.consecutive_losses == 0
        assert result.fail_closed is True
        assert result.can_enter is False


def test_missing_evaluation_price_is_unknown_and_fail_closed():
    with connect_database(":memory:") as database:
        database.init_schema()
        result = build_portfolio_risk_snapshot(account(price=None, pnl=0), database, now=NOW)
        assert "position:005930.current_price" in result.unknown_fields
        assert result.fail_closed


def test_explicit_broker_snapshots_and_fill_history_clear_unknowns():
    with connect_database(":memory:") as database:
        database.init_schema()
        buying_power = KisBuyingPowerSnapshot(
            "005930", 100.0, 500_000.0, 500_000.0, 500_000.0, 5, {}
        )
        realized = KisRealizedPnlSnapshot(1_000.0, {"rlzt_pfls": "1000"})
        result = build_portfolio_risk_snapshot(
            account(pnl=None), database, now=NOW,
            buying_power=buying_power, realized_pnl=realized,
        )
        assert result.unknown_fields == frozenset()
        assert result.orderable_cash == 500_000
        assert result.portfolio.daily_pnl_krw == 1_000


def test_consecutive_losses_come_from_fifo_fill_history():
    with connect_database(":memory:") as database:
        database.init_schema()
        add_order(database, "buy-1", when="2026-08-18T09:00:00+00:00", side="BUY")
        database.apply_broker_fill(
            fill_id="fill-buy-1", client_order_id="buy-1", quantity=1, price=100,
            filled_at=datetime(2026, 8, 18, 9, 0, tzinfo=timezone.utc),
        )
        add_order(database, "sell-1", when="2026-08-18T09:01:00+00:00", side="SELL")
        database.apply_broker_fill(
            fill_id="fill-sell-1", client_order_id="sell-1", quantity=1, price=90,
            filled_at=datetime(2026, 8, 18, 9, 1, tzinfo=timezone.utc),
        )
        result = build_portfolio_risk_snapshot(
            account(pnl=None), database, now=NOW,
            buying_power=KisBuyingPowerSnapshot("005930", 100, 500, 500, 500, 5, {}),
            realized_pnl=KisRealizedPnlSnapshot(-10, {"rlzt_pfls": "-10"}),
        )
        assert result.portfolio.consecutive_losses == 1
        assert "consecutive_losses" not in result.unknown_fields


def _add_fill(database, client, side, quantity, price, filled_at, symbol="005930"):
    add_order(
        database, client, symbol=symbol, when=filled_at.isoformat(), side=side
    )
    database.apply_broker_fill(
        fill_id=f"fill-{client}", client_order_id=client, quantity=quantity,
        price=price, filled_at=filled_at,
    )


def _mark_reconciliation_complete(database, at=NOW):
    database.set_runtime_metadata("operator_review", "false", updated_at=at)
    database.set_runtime_metadata("block_new_entries", "false", updated_at=at)


def test_local_daily_realized_pnl_requires_completed_reconciliation():
    with connect_database(":memory:") as database:
        database.init_schema()
        _add_fill(database, "buy", "BUY", 1, 100, datetime(2026, 8, 17, 6, tzinfo=timezone.utc))
        _add_fill(database, "sell", "SELL", 1, 110, datetime(2026, 8, 18, 0, 30, tzinfo=timezone.utc))
        assert calculate_local_daily_realized_pnl(database, now=NOW) is None
        database.set_runtime_metadata("operator_review", "false", updated_at=NOW)
        database.set_runtime_metadata("block_new_entries", "true", updated_at=NOW)
        assert calculate_local_daily_realized_pnl(database, now=NOW) is None


def test_local_daily_realized_pnl_uses_kst_day_fifo_net_with_estimated_costs():
    now = datetime(2026, 8, 18, 1, 0, tzinfo=timezone.utc)  # 10:00 KST
    with connect_database(":memory:") as database:
        database.init_schema()
        _mark_reconciliation_complete(database, at=now)
        _add_fill(database, "buy-older", "BUY", 2, 100, datetime(2026, 8, 16, 6, tzinfo=timezone.utc))
        _add_fill(database, "buy-newer", "BUY", 1, 120, datetime(2026, 8, 17, 6, tzinfo=timezone.utc))
        # 2026-08-17 23:59 KST: consumes one old FIFO share but is not today's P/L.
        _add_fill(database, "sell-prior-kst", "SELL", 1, 105, datetime(2026, 8, 17, 14, 59, tzinfo=timezone.utc))
        # 2026-08-18 09:30 KST: -40 gross minus estimated 15bp/15bp costs.
        _add_fill(database, "sell-today-kst", "SELL", 2, 90, datetime(2026, 8, 18, 0, 30, tzinfo=timezone.utc))
        assert calculate_local_daily_realized_pnl(database, now=now) == -40.6


def test_local_daily_realized_pnl_rejects_stale_reconciliation_day():
    now = datetime(2026, 8, 18, 1, 0, tzinfo=timezone.utc)
    with connect_database(":memory:") as database:
        database.init_schema()
        _mark_reconciliation_complete(
            database, at=datetime(2026, 8, 17, 1, 0, tzinfo=timezone.utc)
        )
        _add_fill(database, "buy", "BUY", 1, 100, datetime(2026, 8, 17, 6, tzinfo=timezone.utc))
        _add_fill(database, "sell", "SELL", 1, 110, datetime(2026, 8, 18, 0, 30, tzinfo=timezone.utc))
        assert calculate_local_daily_realized_pnl(database, now=now) is None


def test_local_daily_realized_pnl_rejects_fill_newer_than_reconciliation():
    now = datetime(2026, 8, 18, 1, 0, tzinfo=timezone.utc)
    with connect_database(":memory:") as database:
        database.init_schema()
        _mark_reconciliation_complete(
            database, at=datetime(2026, 8, 18, 0, 0, tzinfo=timezone.utc)
        )
        _add_fill(database, "buy", "BUY", 1, 100, datetime(2026, 8, 17, 6, tzinfo=timezone.utc))
        _add_fill(database, "sell", "SELL", 1, 110, datetime(2026, 8, 18, 0, 30, tzinfo=timezone.utc))
        assert calculate_local_daily_realized_pnl(database, now=now) is None


def test_local_daily_realized_pnl_fails_closed_without_fifo_cost_basis():
    with connect_database(":memory:") as database:
        database.init_schema()
        _mark_reconciliation_complete(database)
        _add_fill(database, "sell", "SELL", 1, 110, datetime(2026, 8, 18, 0, 30, tzinfo=timezone.utc))
        assert calculate_local_daily_realized_pnl(database, now=NOW) is None


def test_portfolio_uses_reconciled_local_realized_pnl_fallback():
    now = datetime(2026, 8, 18, 1, 0, tzinfo=timezone.utc)
    with connect_database(":memory:") as database:
        database.init_schema()
        _mark_reconciliation_complete(database, at=now)
        _add_fill(database, "buy", "BUY", 1, 100, datetime(2026, 8, 17, 6, tzinfo=timezone.utc))
        _add_fill(database, "sell", "SELL", 1, 90, datetime(2026, 8, 18, 0, 30, tzinfo=timezone.utc))
        result = build_portfolio_risk_snapshot(
            account(pnl=None), database, now=now,
            buying_power=KisBuyingPowerSnapshot("005930", 100, 500, 500, 500, 5, {}),
        )
        assert result.portfolio.daily_pnl_krw == -10.285
        assert "daily_pnl" not in result.unknown_fields


def test_budget_requires_both_cash_and_allocated_cap():
    assert validate_entry_budget(100, 5, orderable_cash=1000, allocated_cap=500)
    assert not validate_entry_budget(100, 6, orderable_cash=1000, allocated_cap=500)
    assert not validate_entry_budget(100, 5, orderable_cash=None, allocated_cap=500)


def test_budget_rejects_invalid_values():
    assert not validate_entry_budget(0, 1, orderable_cash=100, allocated_cap=100)
    assert not validate_entry_budget(100, 0, orderable_cash=100, allocated_cap=100)
    assert not validate_entry_budget(100, 1, orderable_cash=-1, allocated_cap=100)


def test_local_daily_pnl_accepts_project_naive_kst_clock(tmp_path):
    from kis_ai_scalper.risk.portfolio_snapshot import calculate_local_daily_realized_pnl

    naive_now = datetime(2026, 8, 18, 10, 0)
    with connect_database(tmp_path / "naive-kst.db") as database:
        database.init_schema()
        database.set_runtime_metadata("operator_review", "false", updated_at=naive_now)
        database.set_runtime_metadata("block_new_entries", "false", updated_at=naive_now)

        assert calculate_local_daily_realized_pnl(database, now=naive_now) == 0.0
