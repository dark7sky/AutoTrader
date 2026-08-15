"""Configuration loading with an explicit separation for secrets."""

from __future__ import annotations

import os
from pathlib import Path

import yaml
from pydantic import BaseModel, ConfigDict, Field

from kis_ai_scalper.schemas.types import TradingMode


class KisApiCredentials(BaseModel):
    """KIS app credentials needed for authentication and market data."""

    app_key: str = Field(min_length=1)
    app_secret: str = Field(min_length=1)


class KisAccountInfo(BaseModel):
    """Optional account metadata reserved for future account features."""

    account_no: str | None = None
    account_product_code: str = Field(default="01", min_length=1)


class AppConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    mode: TradingMode = TradingMode.SHADOW
    log_level: str = "INFO"
    decision_schema_path: str = "schemas/trade_decision.schema.json"
    live_trading_enabled: bool = False
    kis_demo_api: KisApiCredentials | None = None
    kis_real_api: KisApiCredentials | None = None
    kis_demo_account: KisAccountInfo | None = None
    kis_real_account: KisAccountInfo | None = None

    def assert_execution_allowed(self, env_live_trading_enabled: bool = False) -> None:
        if self.mode is TradingMode.LIVE and not (
            self.live_trading_enabled and env_live_trading_enabled
        ):
            raise ValueError(
                "live mode requires YAML live_trading_enabled=true and "
                "LIVE_TRADING_ENABLED=true"
            )

    def kis_api_for(self, environment: str) -> KisApiCredentials | None:
        if environment == "real":
            return self.kis_real_api
        return self.kis_demo_api

    def kis_account_for(self, environment: str) -> KisAccountInfo | None:
        if environment == "real":
            return self.kis_real_account
        return self.kis_demo_account


def _read_dotenv(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    if not path.exists():
        return values
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key:
            values[key] = value
    return values


def _environment_with_dotenv(config_path: Path) -> dict[str, str]:
    env = dict(os.environ)
    candidates = []
    try:
        candidates.append(config_path.resolve().parent.parent / ".env")
    except IndexError:
        pass
    candidates.append(Path.cwd() / ".env")
    for candidate in dict.fromkeys(candidates):
        for key, value in _read_dotenv(candidate).items():
            env.setdefault(key, value)
    return env


def load_config(path: str | Path, environ: dict[str, str] | None = None) -> AppConfig:
    """Load non-secret YAML and optional KIS secrets from environment only."""
    config_path = Path(path)
    env = _environment_with_dotenv(config_path) if environ is None else environ
    raw = yaml.safe_load(config_path.read_text(encoding="utf-8")) or {}
    if not isinstance(raw, dict):
        raise ValueError("configuration root must be a mapping")
    forbidden = {
        "kis", "kis_api", "kis_account", "app_key", "app_secret",
        "account_no", "account_product_code",
    }
    if forbidden.intersection(raw):
        raise ValueError("KIS secrets must not be stored in YAML")
    raw = dict(raw)
    raw["mode"] = env.get("TRADING_MODE", raw.get("mode", TradingMode.SHADOW.value))
    yaml_live_trading_enabled = raw.get("live_trading_enabled", False)
    config_live_trading_enabled = env.get("CONFIG_LIVE_TRADING_ENABLED")
    env_live_trading_enabled = env.get("LIVE_TRADING_ENABLED", "false").lower() == "true"
    raw["live_trading_enabled"] = (
        config_live_trading_enabled.lower() == "true"
        if config_live_trading_enabled is not None
        else yaml_live_trading_enabled
    )
    demo_secret_names = ("KIS_DEMO_APP_KEY", "KIS_DEMO_APP_SECRET")
    real_secret_names = ("KIS_REAL_APP_KEY", "KIS_REAL_APP_SECRET")
    if all(env.get(name) for name in demo_secret_names):
        raw["kis_demo_api"] = KisApiCredentials(
            app_key=env["KIS_DEMO_APP_KEY"],
            app_secret=env["KIS_DEMO_APP_SECRET"],
        )
    if all(env.get(name) for name in real_secret_names):
        raw["kis_real_api"] = KisApiCredentials(
            app_key=env["KIS_REAL_APP_KEY"],
            app_secret=env["KIS_REAL_APP_SECRET"],
        )
    if env.get("KIS_DEMO_ACCOUNT_NO"):
        raw["kis_demo_account"] = KisAccountInfo(
            account_no=env["KIS_DEMO_ACCOUNT_NO"],
            account_product_code=env.get("KIS_DEMO_ACCOUNT_PRODUCT_CODE", "01"),
        )
    if env.get("KIS_REAL_ACCOUNT_NO"):
        raw["kis_real_account"] = KisAccountInfo(
            account_no=env["KIS_REAL_ACCOUNT_NO"],
            account_product_code=env.get("KIS_REAL_ACCOUNT_PRODUCT_CODE", "01"),
        )
    config = AppConfig.model_validate(raw)
    config.assert_execution_allowed(env_live_trading_enabled)
    return config
