from datetime import UTC, datetime
from pathlib import Path

import pytest
from pydantic import ValidationError

from kis_ai_scalper.config import load_config
from kis_ai_scalper.schemas import AIAction, TradeDecision, TradingMode


ROOT = Path(__file__).parents[1]
CONFIG = ROOT / "config" / "settings.yaml"


def test_config_defaults_to_shadow_and_loads_api_credentials_without_account():
    config = load_config(
        CONFIG,
        {
            "KIS_DEMO_APP_KEY": "test-key",
            "KIS_DEMO_APP_SECRET": "test-secret",
        },
    )
    assert config.mode is TradingMode.SHADOW
    assert config.live_trading_enabled is False
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


def test_live_mode_requires_explicit_gate(tmp_path):
    with pytest.raises(ValueError, match="live mode"):
        load_config(CONFIG, {"TRADING_MODE": "live", "LIVE_TRADING_ENABLED": "false"})

    with pytest.raises(ValueError, match="YAML"):
        load_config(CONFIG, {"TRADING_MODE": "live", "LIVE_TRADING_ENABLED": "true"})

    live_config = tmp_path / "live-settings.yaml"
    live_config.write_text(
        "mode: live\nlive_trading_enabled: true\n",
        encoding="utf-8",
    )
    config = load_config(
        live_config,
        {"TRADING_MODE": "live", "LIVE_TRADING_ENABLED": "true"},
    )
    assert config.mode is TradingMode.LIVE


def test_config_live_trading_enabled_can_be_overridden_by_environment():
    enabled = load_config(CONFIG, {"CONFIG_LIVE_TRADING_ENABLED": "true"})
    disabled = load_config(CONFIG, {"CONFIG_LIVE_TRADING_ENABLED": "false"})

    assert enabled.live_trading_enabled is True
    assert disabled.live_trading_enabled is False
