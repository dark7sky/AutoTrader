import json
from datetime import datetime, timezone
from types import SimpleNamespace

from kis_ai_scalper import cli
from kis_ai_scalper.pipeline.auto_trade import AutoTradeCycleReport, AutoTradeSymbolResult
from kis_ai_scalper.risk.models import PortfolioState
from kis_ai_scalper.risk.portfolio_snapshot import PortfolioRiskSnapshot
from kis_ai_scalper.storage import connect_database


def test_auto_trade_cycle_persists_last_cycle_observability_metadata(tmp_path, monkeypatch):
    db_path = tmp_path / "auto-trade.sqlite3"
    observed_at = datetime(2026, 8, 21, 10, 15, tzinfo=timezone.utc)
    report = AutoTradeCycleReport((
        AutoTradeSymbolResult(
            symbol="005930",
            action="BUY",
            submitted=True,
            blocked=False,
            reason="acknowledged",
            quantity=2,
        ),
        AutoTradeSymbolResult(
            symbol="000660",
            action="HOLD",
            submitted=False,
            blocked=True,
            reason="operator_approval_required",
            quantity=0,
        ),
    ))

    with connect_database(db_path) as database:
        database.init_schema()
        database.set_runtime_paused(False, "test", "test")

    monkeypatch.setattr(cli, "kst_now", lambda: observed_at)
    monkeypatch.setattr(cli, "is_regular_market_open", lambda _now: True)
    monkeypatch.setattr(cli, "exchange_calendar_available", lambda: True)
    monkeypatch.setattr(cli, "_assert_broker_order_allowed", lambda: None)
    monkeypatch.setattr(cli, "load_config", lambda _path: SimpleNamespace(
        kis_api_for=lambda _environment: SimpleNamespace(app_key="app", app_secret="secret"),
        kis_account_for=lambda _environment: SimpleNamespace(
            account_no="12345678", account_product_code="01"
        ),
    ))

    class FakeAuth:
        def __init__(self, *_args, **_kwargs):
            pass

        def authenticate_read_only(self, **_kwargs):
            return SimpleNamespace(access_token="token", approval_key="approval")

    monkeypatch.setattr(cli, "KisAuthClient", FakeAuth)
    monkeypatch.setattr(cli, "KisOrderClient", lambda *_args, **_kwargs: object())
    monkeypatch.setattr(cli, "KisRestClient", lambda *_args, **_kwargs: object())
    monkeypatch.setattr(cli, "run_auto_trade_cycle", lambda *_args, **_kwargs: report)

    result = cli.auto_trade_cycle(
        "config/settings.yaml",
        "demo",
        "005930,000660",
        str(db_path),
        "rule",
        2,
        0,
        "AUTO_TRADE",
        False,
        portfolio=PortfolioRiskSnapshot(PortfolioState()),
        buying_power_client=object(),
    )

    assert result == 0
    with connect_database(db_path) as database:
        stored = database.get_runtime_metadata(cli.AUTO_TRADE_LAST_CYCLE_KEY)

    assert stored is not None
    payload = json.loads(stored)
    assert payload == {
        "ai": "rule",
        "environment": "demo",
        "observed_at": observed_at.isoformat(),
        "results": [
            {
                "symbol": "005930",
                "action": "BUY",
                "submitted": True,
                "blocked": False,
                "reason": "acknowledged",
                "quantity": 2,
            },
            {
                "symbol": "000660",
                "action": "HOLD",
                "submitted": False,
                "blocked": True,
                "reason": "operator_approval_required",
                "quantity": 0,
            },
        ],
    }
