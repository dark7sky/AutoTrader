from datetime import UTC, datetime
from pathlib import Path

import pytest
from pydantic import ValidationError

from kis_ai_scalper.config import load_config
from kis_ai_scalper.schemas import AIAction, TradeDecision


ROOT = Path(__file__).parents[1]
CONFIG = ROOT / "config" / "settings.yaml"


def test_config_loads_api_credentials_without_account_and_has_no_execution_mode_fields():
    config = load_config(
        CONFIG,
        {
            "KIS_DEMO_APP_KEY": "test-key",
            "KIS_DEMO_APP_SECRET": "test-secret",
        },
    )
    assert not hasattr(config, "mode")
    assert not hasattr(config, "live_trading_enabled")
    assert config.kis_api_for("demo") is not None
    assert config.kis_api_for("demo").app_secret == "test-secret"
    assert config.kis_account_for("demo") is None


def test_account_info_is_separate_and_optional():
    config = load_config(
        CONFIG,
        {
            "KIS_DEMO_APP_KEY": "test-key",
            "KIS_DEMO_APP_SECRET": "test-secret",
            "KIS_DEMO_ACCOUNT_NO": "test-account",
        },
    )
    assert config.kis_api_for("demo") is not None
    assert config.kis_account_for("demo") is not None
    assert config.kis_account_for("demo").account_no == "test-account"


def test_environment_specific_kis_credentials_and_accounts_are_selected():
    config = load_config(
        CONFIG,
        {
            "KIS_DEMO_APP_KEY": "demo-key",
            "KIS_DEMO_APP_SECRET": "demo-secret",
            "KIS_DEMO_ACCOUNT_NO": "11111111",
            "KIS_DEMO_ACCOUNT_PRODUCT_CODE": "01",
            "KIS_REAL_APP_KEY": "real-key",
            "KIS_REAL_APP_SECRET": "real-secret",
            "KIS_REAL_ACCOUNT_NO": "22222222",
            "KIS_REAL_ACCOUNT_PRODUCT_CODE": "02",
        },
    )

    demo_api = config.kis_api_for("demo")
    real_api = config.kis_api_for("real")
    demo_account = config.kis_account_for("demo")
    real_account = config.kis_account_for("real")

    assert demo_api is not None
    assert demo_api.app_key == "demo-key"
    assert real_api is not None
    assert real_api.app_key == "real-key"
    assert demo_account is not None
    assert demo_account.account_no == "11111111"
    assert real_account is not None
    assert real_account.account_no == "22222222"
    assert real_account.account_product_code == "02"


def test_config_can_load_local_dotenv_without_account(tmp_path, monkeypatch):
    monkeypatch.delenv("KIS_DEMO_APP_KEY", raising=False)
    monkeypatch.delenv("KIS_DEMO_APP_SECRET", raising=False)
    monkeypatch.delenv("KIS_DEMO_ACCOUNT_NO", raising=False)
    monkeypatch.delenv("KIS_REAL_APP_KEY", raising=False)
    monkeypatch.delenv("KIS_REAL_APP_SECRET", raising=False)
    monkeypatch.delenv("KIS_REAL_ACCOUNT_NO", raising=False)
    project = tmp_path / "project"
    config_dir = project / "config"
    config_dir.mkdir(parents=True)
    config_path = config_dir / "settings.yaml"
    config_path.write_text("mode: shadow\n", encoding="utf-8")
    (project / ".env").write_text(
        "KIS_DEMO_APP_KEY=dotenv-key\nKIS_DEMO_APP_SECRET=dotenv-secret\n",
        encoding="utf-8",
    )
    monkeypatch.chdir(project)

    config = load_config(config_path)

    assert config.kis_api_for("demo") is not None
    assert config.kis_api_for("demo").app_key == "dotenv-key"
    assert config.kis_account_for("demo") is None


def test_yaml_cannot_contain_kis_secret_fields(tmp_path):
    path = tmp_path / "unsafe.yaml"
    path.write_text("mode: shadow\napp_secret: accidentally-inline\n", encoding="utf-8")
    with pytest.raises(ValueError, match="must not be stored"):
        load_config(path, {})


def test_trade_decision_validation():
    decision = TradeDecision(
        action=AIAction.WAIT,
        symbol="005930",
        confidence=0.5,
        rationale="No deterministic setup is armed.",
        generated_at=datetime.now(UTC),
    )
    assert decision.action is AIAction.WAIT
    with pytest.raises(ValidationError):
        TradeDecision.model_validate({**decision.model_dump(), "confidence": 1.5})
    with pytest.raises(ValidationError):
        TradeDecision.model_validate({**decision.model_dump(), "quantity": 1})


def test_legacy_mode_and_live_gate_inputs_are_ignored(tmp_path):
    config_path = tmp_path / "legacy-settings.yaml"
    config_path.write_text(
        "mode: this-is-not-a-trading-mode\nlive_trading_enabled: true\n",
        encoding="utf-8",
    )

    config = load_config(
        config_path,
        {
            "TRADING_MODE": "also-invalid-and-ignored",
            "CONFIG_LIVE_TRADING_ENABLED": "false",
        },
    )

    assert not hasattr(config, "mode")
    assert not hasattr(config, "live_trading_enabled")


@pytest.mark.parametrize("enabled", [None, "false"])
def test_broker_orders_are_blocked_without_single_live_gate(monkeypatch, enabled):
    if enabled is None:
        monkeypatch.delenv("LIVE_TRADING_ENABLED", raising=False)
    else:
        monkeypatch.setenv("LIVE_TRADING_ENABLED", enabled)
    monkeypatch.setenv("TRADING_MODE", "live")
    monkeypatch.setenv("CONFIG_LIVE_TRADING_ENABLED", "true")

    from kis_ai_scalper import cli

    with pytest.raises(ValueError, match="LIVE_TRADING_ENABLED"):
        cli._assert_broker_order_allowed()


def test_broker_orders_use_only_single_live_gate(monkeypatch):
    monkeypatch.setenv("LIVE_TRADING_ENABLED", "true")
    monkeypatch.setenv("TRADING_MODE", "invalid-and-ignored")
    monkeypatch.setenv("CONFIG_LIVE_TRADING_ENABLED", "false")

    from kis_ai_scalper import cli

    cli._assert_broker_order_allowed()
