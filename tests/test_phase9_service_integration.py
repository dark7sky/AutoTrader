from datetime import datetime, timezone, timedelta

import pytest

from kis_ai_scalper import cli
from kis_ai_scalper.broker.kis_account import (
    KisAccountPosition,
    KisAccountSnapshot,
    KisAccountSummary,
)
from kis_ai_scalper.risk.portfolio_snapshot import PortfolioRiskSnapshot
from kis_ai_scalper.risk.models import PortfolioState
from kis_ai_scalper.storage import connect_database


def test_risk_environment_values_are_strictly_parsed(monkeypatch):
    monkeypatch.setenv("AUTO_TRADE_ALLOCATED_KRW", "1250000")
    monkeypatch.setenv("MAX_POSITIONS", "4")
    monkeypatch.setenv("AI_MIN_CONFIDENCE", "0.81")
    config = cli._risk_config_from_env()
    assert config.allocated_krw == 1_250_000
    assert config.max_positions == 4
    assert config.minimum_confidence == 0.81

    monkeypatch.setenv("MAX_POSITIONS", "1.5")
    with pytest.raises(ValueError, match="MAX_POSITIONS"):
        cli._risk_config_from_env()


def test_auto_trade_config_uses_runtime_confidence_frequency_override(tmp_path, monkeypatch):
    db_path = tmp_path / "frequency.sqlite3"
    monkeypatch.setenv("MAX_TRADES_PER_DAY", "10")
    monkeypatch.setenv("AI_MIN_CONFIDENCE", "0.82")
    with connect_database(db_path) as database:
        database.init_schema()
        database.set_runtime_metadata("trade_frequency.profile", "aggressive")
        # Existing deployments stored the previous aggressive preset as 0.70.
        database.set_runtime_metadata("trade_frequency.ai_min_confidence", "0.70")

    config = cli._auto_trade_config_from_env(1, db_path=str(db_path))

    assert config.risk.minimum_confidence == 0.65
    assert config.min_confidence == 0.65
    assert config.candidate_profile == "aggressive"


def test_standalone_cycle_blocks_environment_mismatch_before_auth(tmp_path, capsys):
    db_path = tmp_path / "runtime.sqlite3"
    with connect_database(db_path) as database:
        database.init_schema()
        database.set_runtime_paused(True, "test", "test")
        database.set_runtime_environment("real", "test", "test")

    result = cli.auto_trade_cycle(
        "config/settings.yaml", "demo", "005930", str(db_path), "rule", 1, 0,
        "AUTO_TRADE", False,
    )
    output = capsys.readouterr().out
    assert result == 3
    assert "runtime_environment_mismatch" in output
    assert "broker_orders=none" in output


def test_service_lease_is_only_active_for_unexpired_other_owner(tmp_path):
    db_path = tmp_path / "lease.sqlite3"
    now = datetime(2026, 8, 16, 9, 0, tzinfo=timezone.utc)
    with connect_database(db_path) as database:
        database.init_schema()
        assert database.acquire_service_lease("trading-service", "owner-a", 60, now=now)
    assert cli._lease_is_active(str(db_path), owner_id="owner-b", now=now)
    assert not cli._lease_is_active(str(db_path), owner_id="owner-a", now=now)
    assert not cli._lease_is_active(
        str(db_path), owner_id="owner-b", now=now + timedelta(seconds=61)
    )


def test_market_retention_runs_once_per_day(tmp_path):
    db_path = tmp_path / "retention.sqlite3"
    now = datetime(2026, 8, 16, 9, 0)
    with connect_database(db_path) as database:
        database.init_schema()
        first = cli._cleanup_market_data(database, now, tick_days=7, bar_days=365)
        second = cli._cleanup_market_data(database, now, tick_days=7, bar_days=365)
    assert first == (0, 0)
    assert second is None


def test_live_report_is_sanitized_and_contains_reconciliation_reasons():
    account = KisAccountSnapshot(
        positions=(KisAccountPosition("005930", 2, 2, 70000, 71000, 2000),),
        summary=KisAccountSummary(100000, 90000, 242000, 2000),
    )
    portfolio = PortfolioRiskSnapshot(PortfolioState(daily_pnl_krw=2000))
    report = cli._sanitized_live_report(
        cli.KisEnvironment.DEMO, account, portfolio, None,
        datetime(2026, 8, 16, 9, 0),
    )
    assert "005930" in report
    assert "app_secret" not in report
    assert "access_token" not in report


def test_cli_openai_client_receives_dotenv_model_and_budget_explicitly(tmp_path, monkeypatch):
    db_path = tmp_path / "openai.sqlite3"
    with connect_database(db_path) as database:
        database.init_schema()
        database.set_runtime_paused(False, "test", "test")
    monkeypatch.setenv("OPENAI_API_KEY", "key")
    monkeypatch.setenv("OPENAI_MODEL", "cheap-model")
    monkeypatch.setenv("OPENAI_MAX_DAILY_CALLS", "7")
    monkeypatch.setenv("OPENAI_MAX_DAILY_COST_USD", "1.25")
    monkeypatch.setattr(cli, "is_regular_market_open", lambda _: True)
    monkeypatch.setattr(cli, "exchange_calendar_available", lambda: True)
    monkeypatch.setattr(cli, "exchange_calendar_available", lambda: True)
    monkeypatch.setattr(cli, "_assert_broker_order_allowed", lambda *_: None)
    monkeypatch.setattr(cli, "load_config", lambda _: type(
        "Config", (), {
            "kis_api_for": lambda self, _: type("Api", (), {"app_key": "a", "app_secret": "s"})(),
            "kis_account_for": lambda self, _: type("Account", (), {"account_no": "12345678", "account_product_code": "01"})(),
        }
    )())

    class FakeAuth:
        def __init__(self, *args, **kwargs):
            pass

        def authenticate_read_only(self, **kwargs):
            return type("Auth", (), {"access_token": "token", "approval_key": "approval"})()

    class FakeAI:
        def __init__(self, *args, **kwargs):
            self.args = args
            self.kwargs = kwargs

    monkeypatch.setattr(cli, "KisAuthClient", FakeAuth)
    monkeypatch.setattr(cli, "KisOrderClient", lambda *args, **kwargs: object())
    monkeypatch.setattr(cli, "OpenAITradingDecisionClient", FakeAI)
    monkeypatch.setattr(cli, "run_auto_trade_cycle", lambda *args, **kwargs: type(
        "Report", (), {"submitted_count": 0, "results": (), "ai_call_count": 0}
    )())

    captured = {}
    original = cli.OpenAITradingDecisionClient

    def capture(*args, **kwargs):
        instance = original(*args, **kwargs)
        captured["model"] = instance.kwargs["model"]
        captured["budget"] = instance.kwargs["budget"].snapshot()
        return instance

    monkeypatch.setattr(cli, "OpenAITradingDecisionClient", capture)
    buying_power_client = type(
        "BuyingPower", (), {"get_snapshot": lambda self, *_: None}
    )()
    assert cli.auto_trade_cycle(
        "config/settings.yaml", "demo", "005930", str(db_path), "openai", 1, 0,
        "AUTO_TRADE", False,
        portfolio=PortfolioRiskSnapshot(PortfolioState()),
        buying_power_client=buying_power_client,
    ) == 0
    assert captured["model"] == "cheap-model"
    assert captured["budget"].daily_cost_usd == 0
    assert captured["budget"].daily_reserved_cost_usd == 0
