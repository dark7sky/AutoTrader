"""Command-line entry points for safe connectivity checks."""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import math
import os
import sys
import threading
import time
from dataclasses import dataclass, replace
from datetime import datetime, timedelta, timezone
from pathlib import Path
from uuid import uuid4

from kis_ai_scalper.broker.kis_auth import KisAuthClient, redact
from kis_ai_scalper.broker.kis_balance import KisBalanceClient
from kis_ai_scalper.broker.kis_endpoints import KisEnvironment
from kis_ai_scalper.broker.kis_order import KisOrderClient
from kis_ai_scalper.broker.kis_order import KisOrderRequest, KisOrderSide
from kis_ai_scalper.broker.kis_order_status import KisOrderStatusClient
from kis_ai_scalper.broker.kis_account import KisAccountClient, KisAccountSnapshot
from kis_ai_scalper.broker.kis_buying_power import KisBuyingPowerClient, KisBuyingPowerSnapshot
from kis_ai_scalper.broker.kis_realized_pnl import (
    KisRealizedPnlClient,
    KisRealizedPnlSnapshot,
    KisRealizedPnlUnsupportedError,
)
from kis_ai_scalper.broker.kis_rest import KisRestClient
from kis_ai_scalper.broker.kis_endpoints import websocket_url
from kis_ai_scalper.broker.kis_ws import smoke_realtime_price
from kis_ai_scalper.broker.kis_fill_notice import smoke_fill_notice as smoke_kis_fill_notice
from kis_ai_scalper.config import load_config
from kis_ai_scalper.ai.decision import OpenAITradingDecisionClient, RuleBasedAIClient
from kis_ai_scalper.market.clock import kst_now
from kis_ai_scalper.market.collector import collect_realtime_prices
from kis_ai_scalper.market.streaming_collector import StreamingCollector
from kis_ai_scalper.market.features import build_feature_snapshot
from kis_ai_scalper.market.health import evaluate_market_health
from kis_ai_scalper.market.schedule import exchange_calendar_available, is_regular_market_open
from kis_ai_scalper.storage import connect_database
from kis_ai_scalper.storage.replay import parse_bars_csv, sample_bars
from kis_ai_scalper.strategies.candidate import scan_candidates
from kis_ai_scalper.risk import OrderIntent, PortfolioState, RiskConfig, evaluate_order_intent
from kis_ai_scalper.execution import (
    Command,
    GuardedOrderSubmitter,
    OrderState,
    SignalLedger,
    build_signal_id,
    transition,
)
from kis_ai_scalper.execution import ManagedPosition, evaluate_position
from kis_ai_scalper.pipeline import (
    run_offline_dry_run,
    ShadowCycleConfig,
    run_shadow_cycle,
    run_paper_shadow_cycle,
    run_user_test,
    run_paper_session,
    submit_shadow_live_buy,
    AutoTradeConfig,
    run_auto_trade_cycle,
)
from kis_ai_scalper.paper import PaperFill, PaperLedger, PaperOrderIntent, PaperSide
from kis_ai_scalper.paper import report_from_database
from kis_ai_scalper.ops.control import control_status, set_paused
from kis_ai_scalper.ops.openai_usage import openai_cost_summary_from_env
from kis_ai_scalper.ops.telegram import TelegramClient, env_value, optional_env_value, poll_telegram
from kis_ai_scalper.ops.trading_frequency import read_trade_frequency
from kis_ai_scalper.ops.fill_notice_worker import run_fill_notice_worker
from kis_ai_scalper.ops.order_supervisor import run_order_supervisor
from kis_ai_scalper.ai.reliable import UsageBudget
from kis_ai_scalper.pipeline.broker_reconciliation import (
    ReconciliationReport,
    reconcile_broker_state,
)
from kis_ai_scalper.pipeline.order_management import (
    OrderManagementConfig,
    OrderManagementReport,
    manage_stale_orders,
)
from kis_ai_scalper.risk.portfolio_snapshot import (
    PortfolioRiskSnapshot,
    build_portfolio_risk_snapshot,
)


def _kis_api_for(config, environment: KisEnvironment):
    credentials = config.kis_api_for(environment.value)
    if credentials is None:
        if environment is KisEnvironment.REAL:
            raise ValueError(
                "KIS_REAL_APP_KEY and KIS_REAL_APP_SECRET environment variables "
                "are required for real KIS environment"
            )
        raise ValueError(
            "KIS_DEMO_APP_KEY and KIS_DEMO_APP_SECRET are required for demo KIS environment"
        )
    return credentials


def _kis_account_for(config, environment: KisEnvironment):
    account = config.kis_account_for(environment.value)
    if account is None or not account.account_no:
        if environment is KisEnvironment.REAL:
            raise ValueError("KIS_REAL_ACCOUNT_NO is required for real broker order submission")
        raise ValueError("KIS_DEMO_ACCOUNT_NO is required for demo broker order submission")
    return account


def smoke_kis(config_path: str, environment: str, symbol: str, refresh_token: bool = False) -> int:
    config = load_config(Path(config_path))
    env = KisEnvironment.parse(environment)
    kis_api = _kis_api_for(config, env)
    auth = KisAuthClient(env, kis_api.app_key, kis_api.app_secret)
    project_root = Path(config_path).resolve().parent.parent
    cache_path = project_root / "data" / "auth" / f"kis_token_{env.value}.json"
    result = auth.authenticate_read_only(cache_path=cache_path, refresh_token=refresh_token)
    quote = KisRestClient(
        env, kis_api.app_key, kis_api.app_secret, result.access_token
    ).get_current_price(symbol)
    print("KIS smoke test: OK")
    print(f"environment={env.value} symbol={quote.symbol} current_price={quote.price:g}")
    print(f"cache_hit={str(result.cache_hit).lower()} access_token={redact(result.access_token)} approval_key={redact(result.approval_key)}")
    print("orders=none account_queries=none websocket_subscriptions=none")
    return 0


def smoke_broker_state(config_path: str, environment: str, refresh_token: bool = False) -> int:
    order_status_client, account_client = _broker_clients(
        config_path, KisEnvironment.parse(environment), refresh_token=refresh_token,
    )
    orders = tuple(order_status_client.get_today_orders())
    account = account_client.get_snapshot()
    print("KIS broker state smoke: OK")
    print(
        f"environment={KisEnvironment.parse(environment).value} orders={len(orders)} "
        f"positions={len(account.positions)}"
    )
    print("broker_writes=none current_price_queries=none websocket_subscriptions=none")
    return 0


def smoke_ws(config_path: str, environment: str, symbol: str, seconds: int,
             refresh_token: bool = False) -> int:
    if seconds < 1 or seconds > 60:
        raise ValueError("seconds must be between 1 and 60")
    config = load_config(Path(config_path))
    env = KisEnvironment.parse(environment)
    kis_api = _kis_api_for(config, env)
    auth = KisAuthClient(env, kis_api.app_key, kis_api.app_secret)
    project_root = Path(config_path).resolve().parent.parent
    cache_path = project_root / "data" / "auth" / f"kis_token_{env.value}.json"
    result = auth.authenticate_read_only(cache_path=cache_path, refresh_token=refresh_token)
    ws_result = asyncio.run(smoke_realtime_price(
        websocket_url(env), result.approval_key, symbol, seconds,
    ))
    ticks = ws_result.ticks
    first = f"{ticks[0].price:g}" if ticks else "none"
    last = f"{ticks[-1].price:g}" if ticks else "none"
    succeeded = ws_result.acknowledged
    print("KIS WebSocket smoke: OK" if succeeded else "KIS WebSocket smoke: FAILED")
    print(f"environment={env.value} symbol={symbol} ws_url={websocket_url(env)}")
    print(
        f"cache_hit={str(result.cache_hit).lower()} "
        f"subscribe_ack={str(ws_result.acknowledged).lower()} tick_count={len(ticks)} "
        f"first_price={first} last_price={last} "
        f"error_code={ws_result.error_code or 'none'}"
    )
    print("orders=none account_queries=none execution_notices=none")
    return 0 if succeeded else 3


def smoke_fill_notice(
    config_path: str,
    environment: str,
    seconds: int,
    refresh_token: bool = False,
) -> int:
    env = KisEnvironment.parse(environment)
    config = load_config(Path(config_path))
    kis_api = _kis_api_for(config, env)
    project_root = Path(config_path).resolve().parent.parent
    cache_path = project_root / "data" / "auth" / f"kis_token_{env.value}.json"
    auth_result = KisAuthClient(
        env, kis_api.app_key, kis_api.app_secret,
    ).authenticate_read_only(cache_path=cache_path, refresh_token=refresh_token)
    result = asyncio.run(smoke_kis_fill_notice(
        websocket_url(env),
        auth_result.approval_key,
        env_value("KIS_HTS_ID"),
        env,
        seconds,
    ))
    print("KIS fill-notice smoke: OK" if result.acknowledged else "KIS fill-notice smoke: FAILED")
    print(
        f"environment={env.value} acknowledged={str(result.acknowledged).lower()} "
        f"events={result.event_count} orders=none account_queries=none"
    )
    return 0 if result.acknowledged else 3


def collect_market(config_path: str, environment: str, symbol: str, seconds: int,
                   db_path: str, refresh_token: bool = False) -> int:
    if seconds < 1 or seconds > 3600:
        raise ValueError("seconds must be between 1 and 3600")
    config = load_config(Path(config_path))
    env = KisEnvironment.parse(environment)
    kis_api = _kis_api_for(config, env)
    auth = KisAuthClient(env, kis_api.app_key, kis_api.app_secret)
    project_root = Path(config_path).resolve().parent.parent
    cache_path = project_root / "data" / "auth" / f"kis_token_{env.value}.json"
    result = auth.authenticate_read_only(cache_path=cache_path, refresh_token=refresh_token)
    collected = asyncio.run(collect_realtime_prices(
        websocket_url(env), result.approval_key, symbol, db_path, seconds,
    ))
    last_price = f"{collected.last_price:g}" if collected.last_price is not None else "none"
    print("read-only collector: OK")
    print(f"environment={env.value} symbol={symbol} seconds={seconds}")
    print(
        f"subscribe_ack={str(collected.subscribe_ack).lower()} "
        f"ticks_saved={collected.ticks_saved} bars_saved={collected.bars_saved} "
        f"last_price={last_price}"
    )
    print("broker_calls=none orders=none account_queries=none ai_calls=none")
    return 0


def user_test(
    config_path: str,
    environment: str,
    symbol: str,
    seconds: int,
    db_path: str,
    max_tick_age_seconds: float,
    max_bar_age_seconds: float,
    refresh_token: bool = False,
) -> int:
    if seconds < 1 or seconds > 3600:
        raise ValueError("seconds must be between 1 and 3600")
    config = load_config(Path(config_path))
    env = KisEnvironment.parse(environment)
    kis_api = _kis_api_for(config, env)
    auth = KisAuthClient(env, kis_api.app_key, kis_api.app_secret)
    project_root = Path(config_path).resolve().parent.parent
    cache_path = project_root / "data" / "auth" / f"kis_token_{env.value}.json"
    result = auth.authenticate_read_only(cache_path=cache_path, refresh_token=refresh_token)
    report = asyncio.run(run_user_test(
        websocket_url(env), result.approval_key, symbol, db_path, seconds,
        max_tick_age_seconds, max_bar_age_seconds,
    ))
    collected = report.collector
    shadow = report.shadow
    last_price = f"{collected.last_price:g}" if collected.last_price is not None else "none"
    print("user-test: OK" if report.exit_code == 0 else "user-test: BLOCKED")
    print("shadow cycle: OK" if not shadow.trading_blocked else "shadow cycle: BLOCKED")
    print(f"environment={env.value} symbol={symbol} seconds={seconds}")
    print(
        f"collector subscribe_ack={str(collected.subscribe_ack).lower()} "
        f"ticks_saved={collected.ticks_saved} bars_saved={collected.bars_saved} "
        f"last_price={last_price}"
    )
    print(
        f"shadow health_status={shadow.health_status} "
        f"trading_blocked={str(shadow.trading_blocked).lower()} "
        f"safe_mode={str(shadow.safe_mode).lower()} "
        f"bars_count={shadow.bars_count} "
        f"candidates_count={shadow.candidates_count} "
        f"risk_approved={str(shadow.risk_approved).lower()}"
    )
    print("orders=none account_queries=none ai_calls=none")
    return report.exit_code


def paper_session(
    config_path: str,
    environment: str,
    symbol: str,
    iterations: int,
    collect_seconds: int,
    sleep_seconds: int,
    db_path: str,
    max_tick_age_seconds: float,
    max_bar_age_seconds: float,
    refresh_token: bool = False,
) -> int:
    with connect_database(db_path) as database:
        database.init_schema()
        control = database.get_runtime_control()
    if control.paused:
        report = asyncio.run(run_paper_session(
            "", "", symbol, db_path,
            iterations, collect_seconds, sleep_seconds,
            max_tick_age_seconds, max_bar_age_seconds,
        ))
        print("paper-session: BLOCKED")
        print(f"environment={environment} symbol={symbol} iterations={iterations}")
        for item in report.iterations:
            print(
                f"iteration={item.iteration} collector ack={str(item.subscribe_ack).lower()} "
                f"ticks={item.ticks_saved} bars={item.bars_saved} "
                f"shadow health={item.health_status} risk_approved={str(item.risk_approved).lower()} "
                f"risk_reason={item.risk_reason} local_paper_recorded={str(item.recorded).lower()} "
                f"duplicate={str(item.duplicate_skipped).lower()} blocked={str(item.blocked).lower()} "
                f"exit_code={item.exit_code}"
            )
        print("broker_calls=none broker_orders=none account_queries=none ai_calls=none")
        return report.exit_code
    config = load_config(Path(config_path))
    env = KisEnvironment.parse(environment)
    kis_api = _kis_api_for(config, env)
    auth = KisAuthClient(env, kis_api.app_key, kis_api.app_secret)
    project_root = Path(config_path).resolve().parent.parent
    cache_path = project_root / "data" / "auth" / f"kis_token_{env.value}.json"
    result = auth.authenticate_read_only(cache_path=cache_path, refresh_token=refresh_token)
    report = asyncio.run(run_paper_session(
        websocket_url(env), result.approval_key, symbol, db_path,
        iterations, collect_seconds, sleep_seconds,
        max_tick_age_seconds, max_bar_age_seconds,
    ))
    print("paper-session: OK" if report.exit_code == 0 else "paper-session: BLOCKED")
    print(f"environment={env.value} symbol={symbol} iterations={iterations}")
    for item in report.iterations:
        print(
            f"iteration={item.iteration} collector ack={str(item.subscribe_ack).lower()} "
            f"ticks={item.ticks_saved} bars={item.bars_saved} "
            f"shadow health={item.health_status} risk_approved={str(item.risk_approved).lower()} "
            f"risk_reason={item.risk_reason} local_paper_recorded={str(item.recorded).lower()} "
            f"duplicate={str(item.duplicate_skipped).lower()} blocked={str(item.blocked).lower()} "
            f"exit_code={item.exit_code}"
        )
    print("broker_calls=none broker_orders=none account_queries=none ai_calls=none")
    return report.exit_code


def analyze_bars(csv_path: str | None, symbol: str, db_path: str) -> int:
    bars = parse_bars_csv(csv_path) if csv_path else sample_bars(symbol)
    bars = [bar for bar in bars if bar.symbol == symbol]
    if not bars:
        raise ValueError(f"no bars found for symbol {symbol}")
    with connect_database(db_path) as database:
        database.init_schema()
        for bar in bars:
            database.save_bar(bar)
        snapshot = build_feature_snapshot(bars)
        candidates = scan_candidates(snapshot) if snapshot is not None else []
        for candidate in candidates:
            database.save_candidate(candidate, bars[-1].start)
    strategies = ",".join(candidate.strategy for candidate in candidates) or "none"
    print("offline analyze: OK")
    print(f"symbol={symbol} rows={len(bars)} candidates={len(candidates)} strategies={strategies}")
    return 0


def paper_report(db_path: str, symbol: str | None = None) -> int:
    with connect_database(db_path) as database:
        database.init_schema()
        report = report_from_database(database, symbol)
    print(f"paper-report: empty={str(report.empty).lower()}")
    print(
        f"total_paper_orders={report.total_paper_orders} "
        f"total_paper_fills={report.total_paper_fills} "
        f"open_paper_positions={len(report.open_positions)} "
        f"gross_buy_value={report.gross_buy_value:g} "
        f"realized_pnl={report.realized_pnl:g}"
    )
    print(f"symbols={','.join(report.symbols) or 'none'}")
    print(
        f"first_fill_timestamp={report.first_fill_timestamp or 'none'} "
        f"last_fill_timestamp={report.last_fill_timestamp or 'none'}"
    )
    print(openai_cost_summary_from_env().text())
    for position in report.open_positions:
        print(
            f"position symbol={position.symbol} quantity={position.quantity} "
            f"average_cost={position.average_cost:g}"
        )
    print("broker_calls=none broker_orders=none account_queries=none ai_calls=none")
    return 0


def runtime_control_status(db_path: str) -> int:
    control = control_status(db_path)
    print(
        f"runtime-control: paused={str(control.paused).lower()} "
        f"environment={control.environment} "
        f"updated_at={control.updated_at} reason={control.reason} source={control.source}"
    )
    return 0


def runtime_control_set(db_path: str, paused: bool, reason: str) -> int:
    control = set_paused(db_path, paused, reason, "cli")
    print(
        f"runtime-control: paused={str(control.paused).lower()} "
        f"environment={control.environment} "
        f"updated_at={control.updated_at} reason={control.reason} source={control.source}"
    )
    return 0


def telegram_poll(db_path: str, bot_token_env: str, allowed_chat_id_env: str,
                  limit: int, timeout_seconds: int) -> int:
    return poll_telegram(
        db_path,
        env_value(bot_token_env),
        env_value(allowed_chat_id_env),
        limit=limit,
        timeout_seconds=timeout_seconds,
    )


SERVICE_LEASE_NAME = "trading-service"
AUTO_TRADE_LAST_CYCLE_KEY = "auto_trade:last_cycle"
TELEGRAM_NOTIFICATION_HISTORY_KEY = "telegram.notification_history"
TELEGRAM_NOTIFICATION_MESSAGE_ID_KEY = "telegram.notification_message_id"
TELEGRAM_NOTIFICATION_HISTORY_LIMIT = 5
TELEGRAM_NOTIFICATION_ENTRY_LIMIT = 650
SERVICE_LEASE_TTL_SECONDS = 180
RETENTION_LAST_DAY_KEY = "market_retention:last_cleanup_day"
LIVE_REPORT_KEY = "live_report_snapshot"
SERVICE_ALERT_THROTTLE = timedelta(minutes=15)
PREFLIGHT_ALERT_FINGERPRINT_KEY = "service:preflight_alert:fingerprint"
PREFLIGHT_ALERT_AT_KEY = "service:preflight_alert:at"
ORDER_MANAGEMENT_ALERT_FINGERPRINT_KEY = "service:order_management_alert:fingerprint"
ORDER_MANAGEMENT_ALERT_AT_KEY = "service:order_management_alert:at"


def _env_number(name: str, default: float, *, integer: bool = False,
                minimum: float | None = None, maximum: float | None = None) -> float | int:
    raw = optional_env_value(name)
    if raw is None:
        return int(default) if integer else float(default)
    try:
        value = int(raw, 10) if integer else float(raw)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{name} must be a {'integer' if integer else 'number'}") from exc
    if isinstance(value, float) and not math.isfinite(value):
        raise ValueError(f"{name} must be finite")
    if minimum is not None and value < minimum:
        raise ValueError(f"{name} must be >= {minimum:g}")
    if maximum is not None and value > maximum:
        raise ValueError(f"{name} must be <= {maximum:g}")
    return value


def _risk_config_from_env(db_path: str | None = None):
    defaults = RiskConfig()
    config = RiskConfig(
        allocated_krw=float(_env_number("AUTO_TRADE_ALLOCATED_KRW", defaults.allocated_krw, minimum=0.01)),
        risk_per_trade_pct=float(_env_number("RISK_PER_TRADE_PCT", defaults.risk_per_trade_pct, minimum=0.01, maximum=100)),
        max_position_pct=float(_env_number("MAX_POSITION_PCT", defaults.max_position_pct, minimum=0.01, maximum=100)),
        max_total_exposure_pct=float(_env_number("MAX_TOTAL_EXPOSURE_PCT", defaults.max_total_exposure_pct, minimum=0.01, maximum=100)),
        max_positions=int(_env_number("MAX_POSITIONS", defaults.max_positions, integer=True, minimum=1)),
        max_daily_loss_pct=float(_env_number("MAX_DAILY_LOSS_PCT", defaults.max_daily_loss_pct, minimum=0, maximum=100)),
        consecutive_loss_limit=int(_env_number("CONSECUTIVE_LOSS_LIMIT", defaults.consecutive_loss_limit, integer=True, minimum=1)),
        max_trades_per_day=None,
        max_orders_per_symbol=int(_env_number("MAX_ORDERS_PER_SYMBOL", defaults.max_orders_per_symbol, integer=True, minimum=1)),
        minimum_confidence=float(_env_number("AI_MIN_CONFIDENCE", defaults.minimum_confidence, minimum=0, maximum=1)),
    )
    if db_path is None:
        return config
    with connect_database(db_path) as database:
        database.init_schema()
        frequency = read_trade_frequency(
            database,
            default_ai_min_confidence=config.minimum_confidence,
        )
    return replace(
        config,
        minimum_confidence=frequency.ai_min_confidence,
    )


def _auto_trade_config_from_env(max_quantity: int, db_path: str | None = None) -> AutoTradeConfig:
    risk = _risk_config_from_env(db_path)
    candidate_profile = "normal"
    if db_path is not None:
        with connect_database(db_path) as database:
            database.init_schema()
            frequency = read_trade_frequency(
                database,
                default_ai_min_confidence=risk.minimum_confidence,
            )
        if frequency.profile in {"conservative", "normal", "aggressive"}:
            candidate_profile = frequency.profile
    return AutoTradeConfig(
        risk=risk,
        max_quantity=max_quantity,
        min_confidence=risk.minimum_confidence,
        candidate_profile=candidate_profile,
        cycle_deadline_seconds=float(
            _env_number("AUTO_TRADE_DECISION_DEADLINE_SECONDS", 25, minimum=1)
        ),
        max_ai_response_age_seconds=float(
            _env_number("OPENAI_MAX_RESPONSE_AGE_SECONDS", 20, minimum=1)
        ),
    )


def _usage_budget_from_env() -> UsageBudget:
    return UsageBudget(
        max_process_calls=int(_env_number("OPENAI_MAX_PROCESS_CALLS", 500, integer=True, minimum=0)),
        max_daily_calls=int(_env_number("OPENAI_MAX_DAILY_CALLS", 1000, integer=True, minimum=0)),
        max_process_cost_usd=float(_env_number("OPENAI_MAX_PROCESS_COST_USD", 10.0, minimum=0)),
        max_daily_cost_usd=float(_env_number("OPENAI_MAX_DAILY_COST_USD", 25.0, minimum=0)),
    )


def _lease_is_active(db_path: str, *, owner_id: str | None = None,
                     now: datetime | None = None) -> bool:
    current = now or datetime.now().astimezone()
    with connect_database(db_path) as database:
        database.init_schema()
        lease = database.get_service_lease(SERVICE_LEASE_NAME)
    if lease is None or str(lease["owner_id"]) == owner_id:
        return False
    try:
        expires_at = datetime.fromisoformat(str(lease["expires_at"]))
    except ValueError:
        return True
    if expires_at.tzinfo is None:
        return True
    return expires_at > current


def _cleanup_market_data(database, now: datetime, *, tick_days: int = 7,
                         bar_days: int = 365) -> tuple[int, int] | None:
    """Run retention once per KST day; a failed cleanup never reaches orders."""
    kst = timezone(timedelta(hours=9), "KST")
    aware_now = now if now.tzinfo is not None else now.replace(tzinfo=kst)
    day = aware_now.astimezone(kst).date().isoformat()
    if database.get_runtime_metadata(RETENTION_LAST_DAY_KEY) == day:
        return None
    tick_count = database.delete_old_ticks(aware_now - timedelta(days=tick_days))
    bar_count = database.delete_old_bars(aware_now - timedelta(days=bar_days))
    database.set_runtime_metadata(RETENTION_LAST_DAY_KEY, day, updated_at=now)
    return tick_count, bar_count


def _sanitized_live_report(environment: KisEnvironment, account: KisAccountSnapshot,
                           portfolio: PortfolioRiskSnapshot | None,
                           reconciliation: ReconciliationReport | None,
                           observed_at: datetime) -> str:
    positions = []
    for position in account.positions:
        positions.append({
            "symbol": position.symbol,
            "qty": position.qty,
            "sellable_qty": position.sellable_qty,
            "current_price": position.current_price,
            "evaluation_pnl": position.evaluation_pnl,
        })
    payload = {
        "environment": environment.value,
        "observed_at": observed_at.isoformat(),
        "positions": positions,
        "cash": account.summary.deposit,
        "orderable_cash": account.summary.orderable_cash_estimate,
        "total_evaluation": account.summary.total_evaluation,
        "evaluation_pnl": account.summary.evaluation_pnl,
        "daily_pnl": None if portfolio is None else portfolio.portfolio.daily_pnl_krw,
        "unknown_fields": [] if portfolio is None else sorted(portfolio.unknown_fields),
        "reconcile_reasons": [] if reconciliation is None else list(reconciliation.reasons),
    }
    return json.dumps(payload, ensure_ascii=True, sort_keys=True, separators=(",", ":"))


def _runtime_preflight(config_path: str, environment: KisEnvironment, ai: str) -> list[str]:
    errors: list[str] = []
    try:
        config = load_config(Path(config_path))
    except Exception as exc:
        return [f"config: {exc}"]
    for label, check in (
        ("kis_api", lambda: _kis_api_for(config, environment)),
        ("kis_account", lambda: _kis_account_for(config, environment)),
        ("broker_gate", _assert_broker_order_allowed),
        ("exchange_calendar", lambda: _assert_exchange_calendar_available()),
        ("risk_config", _risk_config_from_env),
    ):
        try:
            check()
        except Exception as exc:
            errors.append(f"{label}: {exc}")
    if ai == "openai" and optional_env_value("OPENAI_API_KEY") is None:
        errors.append("openai: OPENAI_API_KEY is required when AUTO_TRADE_AI=openai")
    return errors


def _assert_exchange_calendar_available() -> None:
    if not exchange_calendar_available():
        raise RuntimeError("exchange calendar is unavailable; broker trading is fail-closed")


def _reconcile_broker_positions(
    config_path: str,
    environment: KisEnvironment,
    db_path: str,
    refresh_token: bool,
) -> tuple[bool, list[str]]:
    config = load_config(Path(config_path))
    kis_api = _kis_api_for(config, environment)
    kis_account = _kis_account_for(config, environment)
    account_no, account_product_code = _account_components(
        kis_account.account_no,
        kis_account.account_product_code,
    )
    project_root = Path(config_path).resolve().parent.parent
    cache_path = project_root / "data" / "auth" / f"kis_token_{environment.value}.json"
    auth = KisAuthClient(environment, kis_api.app_key, kis_api.app_secret)
    auth_result = auth.authenticate_read_only(cache_path=cache_path, refresh_token=refresh_token)
    broker_positions = KisBalanceClient(
        environment,
        kis_api.app_key,
        kis_api.app_secret,
        auth_result.access_token,
        account_no,
        account_product_code,
    ).get_positions()
    broker = {position.symbol: position.quantity for position in broker_positions}
    with connect_database(db_path) as database:
        database.init_schema()
        local = {
            str(position["symbol"]): int(position["quantity"])
            for position in database.list_open_live_positions()
        }
    messages: list[str] = []
    broker_only = sorted(set(broker) - set(local))
    local_only = sorted(set(local) - set(broker))
    mismatched = sorted(
        symbol for symbol in set(broker).intersection(local)
        if broker[symbol] != local[symbol]
    )
    if local_only:
        messages.append("operator_review_local_position_only=" + ",".join(
            f"{symbol}:{local[symbol]}" for symbol in local_only
        ))
    if broker_only:
        messages.append("operator_review_broker_only=" + ",".join(
            f"{symbol}:{broker[symbol]}" for symbol in broker_only
        ))
    if mismatched:
        messages.append("operator_review_quantity_mismatch=" + ",".join(
            f"{symbol}:broker={broker[symbol]}:local={local[symbol]}" for symbol in mismatched
        ))
    requires_operator = bool(local_only or broker_only or mismatched)
    return not requires_operator, messages


def service_loop(
    config_path: str,
    symbols_text: str | None,
    db_path: str,
    ai: str,
    max_quantity: int,
    collect_seconds: int,
    cycle_interval_seconds: int,
    telegram_limit: int,
    telegram_timeout_seconds: int,
    pause_on_start: bool,
    refresh_token: bool = False,
) -> int:
    if cycle_interval_seconds < 1:
        raise ValueError("cycle_interval_seconds must be positive")
    owner_id = uuid4().hex
    notifier = _telegram_notifier_from_env(db_path)
    telegram_poll_stop = threading.Event()
    telegram_poll_failed = threading.Event()
    telegram_poll_thread: threading.Thread | None = None
    service_workers_stop = threading.Event()
    fill_notice_thread: threading.Thread | None = None
    order_supervisor_thread: threading.Thread | None = None
    with connect_database(db_path) as lease_database:
        lease_database.init_schema()
        lease_database.record_heartbeat("trading-service", heartbeat_at=datetime.now().astimezone())
        if not lease_database.acquire_service_lease(
            SERVICE_LEASE_NAME, owner_id, SERVICE_LEASE_TTL_SECONDS,
        ):
            raise RuntimeError("another trading-service instance already holds the service lease")
        # A service restart always requires an explicit operator resume.
        lease_database.set_runtime_paused(True, "service_start_default_pause", "service")
        try:
            if notifier is not None:
                install_menu = getattr(notifier, "install_menu", None)
                if callable(install_menu):
                    try:
                        install_menu()
                    except Exception as exc:
                        print(
                            f"telegram menu setup warning: {type(exc).__name__}",
                            file=sys.stderr,
                        )
                try:
                    startup_text = (
                        "auto-trade service started\n"
                        "runtime: paused\n"
                        "Use the menu after env/positions are checked."
                    )
                    send_menu = getattr(notifier, "send_menu", None)
                    if callable(send_menu):
                        send_menu(startup_text)
                    else:
                        notifier.send(startup_text)
                except Exception as exc:
                    print(f"service startup notification warning: {type(exc).__name__}", file=sys.stderr)

            token = optional_env_value("TELEGRAM_BOT_TOKEN")
            chat_id = optional_env_value("TELEGRAM_ALLOWED_CHAT_ID")
            if token and chat_id:
                telegram_poll_thread = threading.Thread(
                    target=_telegram_poll_worker,
                    kwargs={
                        "db_path": db_path,
                        "token": token,
                        "chat_id": chat_id,
                        "limit": telegram_limit,
                        "timeout_seconds": telegram_timeout_seconds,
                        "stop_event": telegram_poll_stop,
                        "failed_event": telegram_poll_failed,
                        "notifier": notifier,
                    },
                    name="telegram-poll",
                    daemon=True,
                )
                telegram_poll_thread.start()

            shared_ai_client = None
            if ai == "openai":
                try:
                    shared_ai_client = OpenAITradingDecisionClient(
                        env_value("OPENAI_API_KEY"),
                        model=optional_env_value("OPENAI_MODEL") or "gpt-4o-mini",
                        budget=_usage_budget_from_env(),
                        timeout=float(_env_number("OPENAI_TIMEOUT_SECONDS", 8, minimum=1)),
                        max_retries=int(_env_number("OPENAI_MAX_RETRIES", 1, integer=True, minimum=0)),
                    )
                except Exception as exc:
                    _notify_operator_if_possible(
                        f"AI setup warning: {type(exc).__name__}", notifier=notifier,
                    )

            print("service-loop: started")
            last_alert = ""
            last_alert_at = 0.0
            while True:
                started = time.monotonic()
                poll_failed = telegram_poll_failed.is_set()
                try:
                    now = kst_now()
                    lease_database.record_heartbeat("trading-service", heartbeat_at=datetime.now().astimezone())
                    if not lease_database.renew_service_lease(
                        SERVICE_LEASE_NAME, owner_id, SERVICE_LEASE_TTL_SECONDS,
                    ):
                        raise RuntimeError("trading-service lease renewal failed")
                    try:
                        tick_days = int(_env_number("MARKET_TICK_RETENTION_DAYS", 7, integer=True, minimum=1))
                        bar_days = int(_env_number("MARKET_BAR_RETENTION_DAYS", 365, integer=True, minimum=1))
                        _cleanup_market_data(lease_database, now, tick_days=tick_days, bar_days=bar_days)
                    except Exception as exc:
                        warning = f"market retention warning: {type(exc).__name__}"
                        lease_database.set_runtime_metadata("market_retention:last_error", warning, updated_at=now)
                        print(warning, file=sys.stderr)

                    control = control_status(db_path)
                    env = KisEnvironment.parse(control.environment)
                    errors = _runtime_preflight(config_path, env, ai)
                    if errors:
                        set_paused(db_path, True, "preflight_failed", "service")
                        message = "auto-trade paused: setup issue\n" + "\n".join(f"- {item}" for item in errors)
                        print(message)
                        _send_throttled_service_alert(
                            lease_database,
                            message,
                            now=now,
                            fingerprint_key=PREFLIGHT_ALERT_FINGERPRINT_KEY,
                            sent_at_key=PREFLIGHT_ALERT_AT_KEY,
                            notifier=notifier,
                        )
                        _sleep_remaining(started, cycle_interval_seconds)
                        continue
                    if collect_seconds > 0:
                        fill_heartbeat_at = datetime.now(timezone.utc)
                        lease_database.record_heartbeat(
                            "fill-notice", heartbeat_at=fill_heartbeat_at,
                        )
                        lease_database.set_runtime_metadata(
                            "fill-notice:status", "rest_reconciliation",
                            updated_at=fill_heartbeat_at,
                        )
                        lease_database.set_runtime_metadata(
                            "fill-notice:environment", env.value,
                            updated_at=fill_heartbeat_at,
                        )
                        lease_database.set_runtime_metadata(
                            "fill-notice:last_error", "", updated_at=fill_heartbeat_at,
                        )
                    elif fill_notice_thread is None or not fill_notice_thread.is_alive():
                        fill_notice_thread = threading.Thread(
                            target=run_fill_notice_worker,
                            kwargs={
                                "config_path": config_path,
                                "db_path": db_path,
                                "stop_event": service_workers_stop,
                                "notifier": notifier,
                            },
                            name="fill-notice",
                            daemon=True,
                        )
                        fill_notice_thread.start()
                    if order_supervisor_thread is None or not order_supervisor_thread.is_alive():
                        order_supervisor_thread = threading.Thread(
                            target=run_order_supervisor,
                            kwargs={
                                "config_path": config_path,
                                "db_path": db_path,
                                "stop_event": service_workers_stop,
                                "notifier": notifier,
                                "interval_seconds": float(_env_number(
                                    "ORDER_SUPERVISOR_INTERVAL_SECONDS", 5, minimum=1,
                                )),
                                "buy_ttl_seconds": float(_env_number(
                                    "BUY_ORDER_TTL_SECONDS", 60, minimum=0,
                                )),
                                "sell_ttl_seconds": float(_env_number(
                                    "SELL_ORDER_TTL_SECONDS", 30, minimum=0,
                                )),
                                "broker_read_throttle_seconds": float(_env_number(
                                    "ORDER_SUPERVISOR_BROKER_READ_THROTTLE_SECONDS",
                                    0.25,
                                    minimum=0,
                                )),
                                "expected_owner_id": owner_id,
                                "refresh_token": refresh_token,
                            },
                            name="order-supervisor",
                            daemon=True,
                        )
                        order_supervisor_thread.start()
                    if control.paused:
                        print(f"service-loop: paused environment={control.environment}")
                        _sleep_remaining(started, cycle_interval_seconds)
                        continue
                    if not is_regular_market_open(now):
                        print(f"service-loop: market_closed environment={control.environment}")
                        _sleep_remaining(started, cycle_interval_seconds)
                        continue

                    symbols = _parse_symbols(symbols_text) if symbols_text else []
                    if not symbols:
                        with connect_database(db_path) as database:
                            database.init_schema()
                            symbols = database.list_watchlist_symbols()
                    with connect_database(db_path) as database:
                        database.init_schema()
                        symbols = list(dict.fromkeys([
                            *symbols,
                            *(str(row["symbol"]) for row in database.list_open_live_positions()),
                        ]))
                    _collect_service_market_window(
                        config_path,
                        env,
                        db_path,
                        symbols,
                        collect_seconds,
                        refresh_token,
                    )
                    # Account, fills and open orders must be read after the
                    # bounded market-data window so the risk snapshot used by
                    # this decision cycle is current.
                    latest_control = control_status(db_path)
                    if latest_control.paused or latest_control.environment != env.value:
                        print("service-loop: control_changed_during_collection")
                        _sleep_remaining(started, cycle_interval_seconds)
                        continue
                    state = _make_broker_cycle_state(
                        config_path, env, db_path, symbols, refresh_token,
                        manage_orders=False,
                    )
                    _notify_order_management_alert(
                        lease_database, state.order_management, now=now, notifier=notifier,
                    )
                    if state.reconciliation.reasons:
                        message = "broker reconciliation\n" + "\n".join(
                            f"- {reason}" for reason in state.reconciliation.reasons
                        )
                        if message != last_alert or time.monotonic() - last_alert_at > 300:
                            last_alert, last_alert_at = message, time.monotonic()
                            _notify_operator_if_possible(
                                message + "\nNew entries are blocked; runtime is not auto-paused.",
                                notifier=notifier,
                            )
                    cycle_control = control
                    if telegram_poll_failed.is_set() or state.portfolio.fail_closed:
                        cycle_control = replace(control, paused=True, reason="entry_gate_blocked")
                    auto_trade_cycle(
                        config_path, env.value, symbols_text, db_path, ai,
                        max_quantity, 0, "AUTO_TRADE",
                        notify_telegram=notifier is not None,
                        refresh_token=refresh_token,
                        shared_ai_client=shared_ai_client,
                        service_owner_id=owner_id,
                        portfolio=state.portfolio,
                        buying_power_client=state.buying_power_client,
                        runtime_control_override=cycle_control,
                    )
                except KeyboardInterrupt:
                    raise
                except Exception as exc:
                    message = f"service cycle warning: {type(exc).__name__}"
                    print(message, file=sys.stderr)
                    lease_database.set_runtime_metadata("service:last_error", message, updated_at=kst_now())
                    _notify_operator_if_possible(message, notifier=notifier)
                _sleep_remaining(started, cycle_interval_seconds)
        finally:
            telegram_poll_stop.set()
            service_workers_stop.set()
            if telegram_poll_thread is not None:
                telegram_poll_thread.join(timeout=max(1.0, telegram_timeout_seconds + 1.0))
            if fill_notice_thread is not None:
                fill_notice_thread.join(timeout=5.0)
            if order_supervisor_thread is not None:
                order_supervisor_thread.join(timeout=5.0)
            lease_database.release_service_lease(SERVICE_LEASE_NAME, owner_id)


def _collect_service_market_window(
    config_path: str,
    environment: KisEnvironment,
    db_path: str,
    symbols: list[str],
    collect_seconds: int,
    refresh_token: bool,
) -> None:
    if collect_seconds <= 0 or not symbols:
        return
    config = load_config(Path(config_path))
    kis_api = _kis_api_for(config, environment)
    project_root = Path(config_path).resolve().parent.parent
    cache_path = project_root / "data" / "auth" / f"kis_token_{environment.value}.json"
    auth_result = KisAuthClient(
        environment, kis_api.app_key, kis_api.app_secret,
    ).authenticate_read_only(cache_path=cache_path, refresh_token=refresh_token)
    with connect_database(db_path) as database:
        database.init_schema()
        collector = StreamingCollector(
            endpoint=websocket_url(environment),
            approval_key=auth_result.approval_key,
            symbols=symbols,
            database=database,
        )
        asyncio.run(collector.run(deadline=time.monotonic() + collect_seconds))


def _sleep_remaining(started: float, interval_seconds: int) -> None:
    elapsed = time.monotonic() - started
    time.sleep(max(0.0, interval_seconds - elapsed))


class _TelegramNotifier:
    def __init__(self, token: str, chat_id: str, db_path: str | None = None) -> None:
        from kis_ai_scalper.ops.telegram import TelegramClient

        self.client = TelegramClient(token)
        self.chat_id = chat_id
        self.db_path = db_path
        self._lock = threading.Lock()
        self._history: list[dict[str, str]] = []
        self._message_id: int | None = None
        self._state_loaded = False

    def _load_state(self) -> None:
        if self._state_loaded:
            return
        self._state_loaded = True
        if self.db_path is None:
            return
        with connect_database(self.db_path) as database:
            database.init_schema()
            raw_history = database.get_runtime_metadata(TELEGRAM_NOTIFICATION_HISTORY_KEY)
            raw_message_id = database.get_runtime_metadata(TELEGRAM_NOTIFICATION_MESSAGE_ID_KEY)
        try:
            parsed = json.loads(raw_history or "[]")
        except (TypeError, ValueError):
            parsed = []
        if isinstance(parsed, list):
            for item in parsed[-TELEGRAM_NOTIFICATION_HISTORY_LIMIT:]:
                if not isinstance(item, dict):
                    continue
                timestamp = str(item.get("timestamp") or "").strip()[:32]
                text = str(item.get("text") or "").strip()[:TELEGRAM_NOTIFICATION_ENTRY_LIMIT]
                if timestamp and text:
                    self._history.append({"timestamp": timestamp, "text": text})
        try:
            message_id = int(raw_message_id or "")
        except (TypeError, ValueError):
            message_id = 0
        self._message_id = message_id if message_id > 0 else None

    def _persist_state(self, now: datetime) -> None:
        if self.db_path is None:
            return
        with connect_database(self.db_path) as database:
            database.init_schema()
            database.set_runtime_metadata(
                TELEGRAM_NOTIFICATION_HISTORY_KEY,
                json.dumps(self._history, ensure_ascii=True, separators=(",", ":")),
                updated_at=now,
            )
            database.set_runtime_metadata(
                TELEGRAM_NOTIFICATION_MESSAGE_ID_KEY,
                str(self._message_id or ""),
                updated_at=now,
            )

    def _render_history(self) -> str:
        lines = [f"최근 운영 알림 ({len(self._history)}/{TELEGRAM_NOTIFICATION_HISTORY_LIMIT})"]
        for item in self._history:
            lines.append(f"\n[{item['timestamp']}]\n{item['text']}")
        return "\n".join(lines)

    def send(self, text: str) -> None:
        from kis_ai_scalper.ops.telegram import MAIN_MENU_KEYBOARD

        now = kst_now()
        entry = {
            "timestamp": now.strftime("%m-%d %H:%M:%S"),
            "text": str(text).strip()[:TELEGRAM_NOTIFICATION_ENTRY_LIMIT],
        }
        with self._lock:
            self._load_state()
            self._history.append(entry)
            self._history = self._history[-TELEGRAM_NOTIFICATION_HISTORY_LIMIT:]
            rendered = self._render_history()
            if self._message_id is not None:
                try:
                    self.client.edit_message_text(
                        self.chat_id,
                        self._message_id,
                        rendered,
                        reply_markup=MAIN_MENU_KEYBOARD,
                    )
                    self._persist_state(now)
                    return
                except RuntimeError:
                    try:
                        self.client.delete_message(self.chat_id, self._message_id)
                    except RuntimeError:
                        pass
                    self._message_id = None
            result = self.client.send_message(
                self.chat_id,
                rendered,
                reply_markup=MAIN_MENU_KEYBOARD,
            )
            if isinstance(result, dict):
                message = result.get("result")
                if isinstance(message, dict) and isinstance(message.get("message_id"), int):
                    self._message_id = message["message_id"]
            self._persist_state(now)

    def install_menu(self) -> None:
        from kis_ai_scalper.ops.telegram import BOT_COMMANDS

        self.client.set_my_commands(BOT_COMMANDS)
        self.client.set_chat_menu_button(self.chat_id)

    def send_menu(self, text: str) -> None:
        self.send(text)

    def send_approval(self, request_id: str, text: str) -> None:
        reply_markup = {
            "inline_keyboard": [
                [
                    {"text": "승인", "callback_data": f"approval:approve:{request_id}"},
                    {"text": "거절", "callback_data": f"approval:reject:{request_id}"},
                ],
                [{"text": "메인 메뉴", "callback_data": "menu:main"}],
            ]
        }
        self.client.send_message(self.chat_id, text, reply_markup=reply_markup)


def _telegram_poll_worker(
    *,
    db_path: str,
    token: str,
    chat_id: str,
    limit: int,
    timeout_seconds: int,
    stop_event: threading.Event,
    failed_event: threading.Event,
    notifier: _TelegramNotifier | None = None,
) -> None:
    """Poll Telegram independently so operator controls are responsive during collection."""

    while not stop_event.is_set():
        try:
            poll_telegram(
                db_path,
                token,
                chat_id,
                limit=limit,
                timeout_seconds=timeout_seconds,
                client=TelegramClient(token),
            )
            failed_event.clear()
            if timeout_seconds == 0:
                stop_event.wait(0.1)
        except Exception as exc:
            failed_event.set()
            warning = f"telegram poll warning: {type(exc).__name__}"
            print(warning, file=sys.stderr)
            try:
                with connect_database(db_path) as database:
                    database.init_schema()
                    database.set_runtime_metadata("telegram:last_error", warning, updated_at=kst_now())
                    _send_throttled_service_alert(
                        database,
                        warning,
                        now=kst_now(),
                        fingerprint_key="service:telegram_poll_alert:fingerprint",
                        sent_at_key="service:telegram_poll_alert:at",
                        notifier=notifier,
                    )
            except Exception as metadata_exc:
                print(
                    f"telegram poll state warning: {type(metadata_exc).__name__}",
                    file=sys.stderr,
                )
            stop_event.wait(1.0)


def _env_live_trading_enabled() -> bool:
    return env_value("LIVE_TRADING_ENABLED").lower() == "true"


def _telegram_notifier_from_env(db_path: str | None = None) -> _TelegramNotifier | None:
    token = optional_env_value("TELEGRAM_BOT_TOKEN")
    chat_id = optional_env_value("TELEGRAM_ALLOWED_CHAT_ID")
    if not token or not chat_id:
        return None
    return _TelegramNotifier(token, chat_id, db_path)


def _notify_operator_if_possible(text: str, *, notifier: _TelegramNotifier | None = None) -> bool:
    target = notifier or _telegram_notifier_from_env()
    if target is None:
        return False
    try:
        target.send(text)
    except Exception as exc:
        print(f"telegram notification warning: {type(exc).__name__}", file=sys.stderr)
        return False
    return True


def _send_throttled_service_alert(
    database,
    message: str,
    *,
    now: datetime,
    fingerprint_key: str,
    sent_at_key: str,
    notifier: _TelegramNotifier | None = None,
) -> bool:
    """Send one service alert per message every 15 minutes, across restarts."""

    fingerprint = hashlib.sha256(message.encode("utf-8")).hexdigest()
    previous_fingerprint = database.get_runtime_metadata(fingerprint_key)
    previous_at = database.get_runtime_metadata(sent_at_key)
    if previous_fingerprint == fingerprint and previous_at:
        try:
            parsed_at = datetime.fromisoformat(previous_at)
            if parsed_at.tzinfo is None:
                parsed_at = parsed_at.replace(tzinfo=timezone.utc)
            current_at = now if now.tzinfo is not None else now.replace(tzinfo=timezone.utc)
            if current_at.astimezone(timezone.utc) - parsed_at.astimezone(timezone.utc) < SERVICE_ALERT_THROTTLE:
                return False
        except ValueError:
            pass

    delivered = (
        _notify_operator_if_possible(message)
        if notifier is None
        else _notify_operator_if_possible(message, notifier=notifier)
    )
    if not delivered:
        return False
    database.set_runtime_metadata(fingerprint_key, fingerprint, updated_at=now)
    database.set_runtime_metadata(sent_at_key, now.isoformat(), updated_at=now)
    return True


def _notify_order_management_alert(
    database,
    report: OrderManagementReport,
    *,
    now: datetime,
    notifier: _TelegramNotifier | None = None,
) -> bool:
    """Alert on order actions without exposing order identifiers or quantities."""

    events = {
        (action.action, action.symbol or "unknown", action.reason)
        for action in report.actions
        if action.action in {"CANCEL_PENDING", "UNKNOWN", "OPERATOR_REVIEW"}
    }
    if report.operator_review:
        events.add(("OPERATOR_REVIEW", "unknown", "manual_review_required"))
    if not events:
        return False
    lines = [
        f"- action={action} symbol={symbol} reason={reason}"
        for action, symbol, reason in sorted(events)
    ]
    message = (
        "order management alert\n"
        + "\n".join(lines)
        + "\nNew entries are blocked until operator review."
    )
    return _send_throttled_service_alert(
        database,
        message,
        now=now,
        fingerprint_key=ORDER_MANAGEMENT_ALERT_FINGERPRINT_KEY,
        sent_at_key=ORDER_MANAGEMENT_ALERT_AT_KEY,
        notifier=notifier,
    )


def _assert_broker_order_allowed() -> None:
    if not _env_live_trading_enabled():
        raise ValueError("broker order submission requires LIVE_TRADING_ENABLED=true")


def _account_components(account_no: str, account_product_code: str) -> tuple[str, str]:
    raw = account_no.strip()
    if "-" in raw:
        left, right = raw.split("-", 1)
        raw = left.strip()
        account_product_code = right.strip() or account_product_code
    digits = "".join(char for char in raw if char.isdigit())
    if len(digits) == 10 and account_product_code == "01":
        return digits[:8], digits[8:]
    if len(digits) != 8:
        raise ValueError("KIS account number must contain eight account digits")
    return digits, account_product_code


def _broker_clients(
    config_path: str,
    environment: KisEnvironment,
    *,
    refresh_token: bool = False,
) -> tuple[KisOrderStatusClient, KisAccountClient]:
    config = load_config(Path(config_path))
    kis_api = _kis_api_for(config, environment)
    kis_account = _kis_account_for(config, environment)
    account_no, account_product_code = _account_components(
        kis_account.account_no, kis_account.account_product_code,
    )
    project_root = Path(config_path).resolve().parent.parent
    cache_path = project_root / "data" / "auth" / f"kis_token_{environment.value}.json"
    auth_result = KisAuthClient(
        environment, kis_api.app_key, kis_api.app_secret,
    ).authenticate_read_only(cache_path=cache_path, refresh_token=refresh_token)
    common = {
        "environment": environment,
        "app_key": kis_api.app_key,
        "app_secret": kis_api.app_secret,
        "access_token": auth_result.access_token,
        "account_no": account_no,
        "account_product_code": account_product_code,
    }
    return KisOrderStatusClient(**common), KisAccountClient(**common)


def _supervisor_gate_unknown_fields(database: object, now: datetime) -> set[str]:
    heartbeat = database.get_heartbeat("order-supervisor")
    if heartbeat is None:
        return {"order_supervisor"}
    try:
        heartbeat_at = datetime.fromisoformat(str(heartbeat))
    except ValueError:
        return {"order_supervisor"}
    if heartbeat_at.tzinfo is None:
        heartbeat_at = heartbeat_at.replace(tzinfo=timezone.utc)
    max_age = float(_env_number("SERVICE_HEARTBEAT_MAX_AGE_SECONDS", 180, minimum=1))
    if (now.astimezone(timezone.utc) - heartbeat_at.astimezone(timezone.utc)).total_seconds() > max_age:
        return {"order_supervisor"}
    raw_status = database.get_runtime_metadata("order-supervisor.status")
    try:
        payload = json.loads(raw_status or "{}")
    except (TypeError, ValueError):
        return {"order_supervisor"}
    if not isinstance(payload, dict) or payload.get("status") != "reconciled":
        return {"order_supervisor"}
    return set()


@dataclass(frozen=True)
class _BrokerCycleState:
    order_client: KisOrderClient
    account_client: KisAccountClient
    buying_power_client: KisBuyingPowerClient
    account: KisAccountSnapshot
    portfolio: PortfolioRiskSnapshot
    reconciliation: ReconciliationReport
    order_management: OrderManagementReport
    realized_pnl: KisRealizedPnlSnapshot | None = None


def _make_broker_cycle_state(
    config_path: str,
    environment: KisEnvironment,
    db_path: str,
    symbols: list[str],
    refresh_token: bool,
    *,
    manage_orders: bool = True,
) -> _BrokerCycleState:
    config = load_config(Path(config_path))
    kis_api = _kis_api_for(config, environment)
    kis_account = _kis_account_for(config, environment)
    account_no, account_product_code = _account_components(
        kis_account.account_no, kis_account.account_product_code,
    )
    project_root = Path(config_path).resolve().parent.parent
    cache_path = project_root / "data" / "auth" / f"kis_token_{environment.value}.json"
    auth_result = KisAuthClient(
        environment, kis_api.app_key, kis_api.app_secret,
    ).authenticate_read_only(cache_path=cache_path, refresh_token=refresh_token)
    account_client = KisAccountClient(
        environment, kis_api.app_key, kis_api.app_secret, auth_result.access_token,
        account_no, account_product_code,
    )
    order_status_client = KisOrderStatusClient(
        environment, kis_api.app_key, kis_api.app_secret, auth_result.access_token,
        account_no, account_product_code,
    )
    buying_power_client = KisBuyingPowerClient(
        environment, kis_api.app_key, kis_api.app_secret, auth_result.access_token,
        account_no, account_product_code,
    )
    order_client = KisOrderClient(
        environment, kis_api.app_key, kis_api.app_secret, auth_result.access_token,
        account_no, account_product_code,
    )
    with connect_database(db_path) as database:
        database.init_schema()
        cycle_time = kst_now()
        if manage_orders:
            broker_orders = tuple(order_status_client.get_today_orders())
            account = account_client.get_snapshot()
            reconciliation = reconcile_broker_state(
                database,
                order_status_client,
                account_client,
                current_time=cycle_time,
                broker_orders=broker_orders,
                account_snapshot=account,
            )
            order_management = manage_stale_orders(
                database,
                order_status_client,
                current_time=cycle_time,
                config=OrderManagementConfig(
                    buy_ttl_seconds=_env_number(
                        "BUY_ORDER_TTL_SECONDS", 60, minimum=0,
                    ),
                    sell_ttl_seconds=_env_number(
                        "SELL_ORDER_TTL_SECONDS", 30, minimum=0,
                    ),
                ),
                broker_orders=broker_orders,
            )
            supervisor_unknown = set()
        else:
            account = account_client.get_snapshot()
            reconciliation = ReconciliationReport()
            order_management = OrderManagementReport()
            supervisor_unknown = _supervisor_gate_unknown_fields(database, cycle_time)
        buying_power: KisBuyingPowerSnapshot | None = None
        prices: list[tuple[str, float]] = []
        for symbol in dict.fromkeys([*symbols, *(position.symbol for position in account.positions)]):
            tick = database.latest_tick(symbol)
            bar = database.latest_bar(symbol)
            position = next((item for item in account.positions if item.symbol == symbol), None)
            price = tick.price if tick is not None else bar.close if bar is not None else None
            price = price if price is not None else (position.current_price if position else None)
            if price is not None and price > 0:
                prices.append((symbol, float(price)))
        if not prices and symbols:
            try:
                quote = KisRestClient(
                    environment, kis_api.app_key, kis_api.app_secret, auth_result.access_token,
                ).get_current_price(symbols[0])
                prices.append((quote.symbol, float(quote.price)))
            except Exception:
                pass
        buying_power_error = False
        if prices:
            try:
                buying_power = buying_power_client.get_snapshot(*prices[0])
            except Exception:
                buying_power_error = True
        else:
            buying_power_error = True

        realized: KisRealizedPnlSnapshot | None = None
        realized_error = False
        if environment is KisEnvironment.REAL:
            try:
                realized = KisRealizedPnlClient(
                    environment, kis_api.app_key, kis_api.app_secret,
                    auth_result.access_token, account_no, account_product_code,
                ).get_snapshot()
            except Exception:
                # A real-account P/L failure is deliberately an entry block.
                realized_error = True

        portfolio = build_portfolio_risk_snapshot(
            account, database, now=kst_now(),
            buying_power=buying_power, realized_pnl=realized,
        )
        unknown = set(portfolio.unknown_fields)
        if buying_power_error or buying_power is None:
            unknown.add("buying_power")
        elif buying_power.orderable_cash is None or buying_power.orderable_quantity is None:
            unknown.add("buying_power_fields")
        if environment is KisEnvironment.REAL and realized_error:
            unknown.add("realized_pnl")
        unknown.update(supervisor_unknown)
        if reconciliation.block_new_entries:
            unknown.add("broker_reconciliation")
        if order_management.entries_blocked:
            unknown.add("order_management")
        if unknown != set(portfolio.unknown_fields):
            portfolio = replace(portfolio, unknown_fields=frozenset(unknown))
        database.set_runtime_metadata(
            LIVE_REPORT_KEY,
            _sanitized_live_report(environment, account, portfolio, reconciliation, kst_now()),
            updated_at=kst_now(),
        )
    return _BrokerCycleState(
        order_client=order_client,
        account_client=account_client,
        buying_power_client=buying_power_client,
        account=account,
        portfolio=portfolio,
        reconciliation=reconciliation,
        order_management=order_management,
        realized_pnl=realized,
    )


def _entry_budget_checker(client: KisBuyingPowerClient):
    def check(symbol: str, price: float, quantity: int) -> bool:
        snapshot = client.get_snapshot(symbol, price)
        if snapshot.orderable_cash is None or snapshot.orderable_quantity is None:
            return False
        return (
            snapshot.orderable_quantity >= quantity
            and snapshot.orderable_cash >= price * quantity
        )
    return check


def submit_live_shadow_order(
    config_path: str,
    environment: str,
    symbol: str,
    db_path: str,
    max_quantity: int,
    websocket_acknowledged: bool,
    confirm: str,
    refresh_token: bool = False,
) -> int:
    with connect_database(db_path) as database:
        database.init_schema()
        control = database.get_runtime_control()
        if control.paused:
            report = run_shadow_cycle(
                symbol,
                database=database,
                config=ShadowCycleConfig(websocket_acknowledged=websocket_acknowledged),
            )
            result = submit_shadow_live_buy(
                report,
                runtime_control=control,
                submitter=_BlockedSubmitter(),
                database=database,
                confirm_submit=confirm == "SUBMIT_KIS_ORDER",
                max_quantity=max_quantity,
            )
            print("submit-live-shadow: BLOCKED")
            print(f"environment={environment} symbol={symbol} reason={result.reason}")
            print("broker_orders=none account_queries=none ai_calls=none")
            return 3
        if confirm != "SUBMIT_KIS_ORDER":
            report = run_shadow_cycle(
                symbol,
                database=database,
                config=ShadowCycleConfig(websocket_acknowledged=websocket_acknowledged),
            )
            result = submit_shadow_live_buy(
                report,
                runtime_control=control,
                submitter=_BlockedSubmitter(),
                database=database,
                confirm_submit=False,
                max_quantity=max_quantity,
            )
            print("submit-live-shadow: BLOCKED")
            print(f"environment={environment} symbol={symbol} reason={result.reason}")
            print("broker_orders=none account_queries=none ai_calls=none")
            return 3

    env = KisEnvironment.parse(environment)
    config = load_config(Path(config_path))
    kis_api = _kis_api_for(config, env)
    kis_account = _kis_account_for(config, env)
    _assert_broker_order_allowed()
    account_no, account_product_code = _account_components(
        kis_account.account_no,
        kis_account.account_product_code,
    )
    project_root = Path(config_path).resolve().parent.parent
    cache_path = project_root / "data" / "auth" / f"kis_token_{env.value}.json"
    auth = KisAuthClient(env, kis_api.app_key, kis_api.app_secret)
    auth_result = auth.authenticate_read_only(cache_path=cache_path, refresh_token=refresh_token)
    client = KisOrderClient(
        env,
        kis_api.app_key,
        kis_api.app_secret,
        auth_result.access_token,
        account_no,
        account_product_code,
    )
    with connect_database(db_path) as database:
        database.init_schema()
        control = database.get_runtime_control()
        report = run_shadow_cycle(
            symbol,
            database=database,
            config=ShadowCycleConfig(websocket_acknowledged=websocket_acknowledged),
            portfolio=PortfolioState(open_positions=database.paper_positions()),
        )
        result = submit_shadow_live_buy(
            report,
            runtime_control=control,
            submitter=client,
            database=database,
            confirm_submit=True,
            max_quantity=max_quantity,
        )
    print("submit-live-shadow: SUBMITTED" if result.submitted else "submit-live-shadow: BLOCKED")
    print(
        f"environment={env.value} symbol={symbol} reason={result.reason} "
        f"quantity={result.quantity} broker_order_id={result.broker_order_id or 'none'}"
    )
    print(
        "broker_orders=1 account_queries=none ai_calls=none"
        if result.submitted
        else "broker_orders=none account_queries=none ai_calls=none"
    )
    return 0 if result.submitted else 3


class _BlockedSubmitter:
    def submit_order(self, request):
        raise RuntimeError("blocked submitter must not be called")


def watchlist_add(db_path: str, symbols: str) -> int:
    parsed = _parse_symbols(symbols)
    with connect_database(db_path) as database:
        database.init_schema()
        for symbol in parsed:
            database.add_watchlist_symbol(symbol, True)
    print(f"watchlist-add: OK symbols={','.join(parsed)}")
    return 0


def watchlist_remove(db_path: str, symbols: str) -> int:
    parsed = _parse_symbols(symbols)
    with connect_database(db_path) as database:
        database.init_schema()
        for symbol in parsed:
            database.set_watchlist_enabled(symbol, False)
    print(f"watchlist-remove: OK symbols={','.join(parsed)}")
    return 0


def watchlist_list(db_path: str, all_symbols: bool = False) -> int:
    with connect_database(db_path) as database:
        database.init_schema()
        symbols = database.list_watchlist_symbols(enabled_only=not all_symbols)
    print(f"watchlist: symbols={','.join(symbols) or 'none'}")
    return 0


def auto_trade_cycle(
    config_path: str,
    environment: str,
    symbols_text: str | None,
    db_path: str,
    ai: str,
    max_quantity: int,
    collect_seconds: int,
    confirm: str,
    notify_telegram: bool,
    refresh_token: bool = False,
    shared_ai_client=None,
    service_owner_id: str | None = None,
    portfolio: PortfolioRiskSnapshot | None = None,
    buying_power_client: KisBuyingPowerClient | None = None,
    runtime_control_override=None,
) -> int:
    if confirm != "AUTO_TRADE":
        print("auto-trade-cycle: BLOCKED")
        print("reason=confirmation_required broker_orders=none ai_calls=none")
        return 3
    if collect_seconds < 0 or collect_seconds > 3600:
        raise ValueError("collect_seconds must be between 0 and 3600")
    with connect_database(db_path) as database:
        database.init_schema()
        symbols = _parse_symbols(symbols_text) if symbols_text else database.list_watchlist_symbols()
        open_position_symbols = [
            str(position["symbol"]) for position in database.list_open_live_positions()
        ]
        control = runtime_control_override or database.get_runtime_control()
        requested_environment = KisEnvironment.parse(environment).value
        if control.environment != requested_environment:
            print("auto-trade-cycle: BLOCKED")
            print(
                "reason=runtime_environment_mismatch "
                f"runtime={control.environment} requested={requested_environment} "
                "broker_orders=none ai_calls=none"
            )
            return 3
    if not symbols and not open_position_symbols:
        raise ValueError("no symbols supplied and watchlist is empty")
    if _lease_is_active(db_path, owner_id=service_owner_id):
        print("auto-trade-cycle: BLOCKED")
        print("reason=trading_service_lease_active broker_orders=none ai_calls=none")
        return 3
    if control.paused:
        print("auto-trade-cycle: BLOCKED")
        print("reason=runtime_paused broker_orders=none ai_calls=none")
        return 3
    now = kst_now()
    if not exchange_calendar_available():
        print("auto-trade-cycle: BLOCKED")
        print("reason=exchange_calendar_unavailable broker_orders=none ai_calls=none")
        return 3
    if not is_regular_market_open(now):
        print("auto-trade-cycle: BLOCKED")
        print("reason=market_closed broker_orders=none ai_calls=none")
        return 3

    env = KisEnvironment.parse(environment)
    config = load_config(Path(config_path))
    kis_api = _kis_api_for(config, env)
    kis_account = _kis_account_for(config, env)
    _assert_broker_order_allowed()
    account_no, account_product_code = _account_components(
        kis_account.account_no,
        kis_account.account_product_code,
    )
    if portfolio is None or buying_power_client is None:
        state = _make_broker_cycle_state(
            config_path, env, db_path, list(dict.fromkeys([*symbols, *open_position_symbols])),
            refresh_token,
        )
        if state.portfolio.fail_closed:
            print("auto-trade-cycle: BLOCKED")
            print(
                "reason=broker_risk_snapshot_unavailable "
                f"unknown={','.join(sorted(state.portfolio.unknown_fields)) or 'none'} "
                "broker_orders=none ai_calls=none"
            )
            return 3
        portfolio = state.portfolio
        buying_power_client = state.buying_power_client
    project_root = Path(config_path).resolve().parent.parent
    cache_path = project_root / "data" / "auth" / f"kis_token_{env.value}.json"
    auth = KisAuthClient(env, kis_api.app_key, kis_api.app_secret)
    auth_result = auth.authenticate_read_only(cache_path=cache_path, refresh_token=refresh_token)
    if collect_seconds:
        collect_symbols = list(dict.fromkeys([*symbols, *open_position_symbols]))
        with connect_database(db_path) as database:
            database.init_schema()
            collector = StreamingCollector(
                endpoint=websocket_url(env),
                approval_key=auth_result.approval_key,
                symbols=collect_symbols,
                database=database,
            )
            asyncio.run(collector.run(deadline=time.monotonic() + collect_seconds))
    # Telegram controls may change while the stream is collecting. Re-read the
    # gate on a separate connection immediately before creating/submitting orders.
    with connect_database(db_path) as database:
        database.init_schema()
        latest_control = database.get_runtime_control()
        emergency_value = (
            database.get_runtime_metadata("emergency_stop")
            or database.get_runtime_metadata("telegram.emergency_stop")
            or ""
        )
    emergency_active = emergency_value.strip().lower() in {"1", "true", "yes", "on"}
    gate_reasons = []
    if latest_control.paused:
        gate_reasons.append("runtime_paused")
    if latest_control.environment != env.value:
        gate_reasons.append("runtime_environment_mismatch")
    if emergency_active:
        gate_reasons.append("emergency_stop")
    if gate_reasons:
        print("auto-trade-cycle: BLOCKED")
        print(
            f"reason=post_collection_gate:{','.join(gate_reasons)} "
            "broker_orders=none ai_calls=none"
        )
        return 3
    if runtime_control_override is not None and runtime_control_override.paused:
        control = replace(
            latest_control,
            paused=True,
            reason=runtime_control_override.reason,
            source=runtime_control_override.source,
        )
    else:
        control = latest_control
    auto_config = _auto_trade_config_from_env(max_quantity, db_path=db_path)
    # Collection can outlive the preflight time used for market-open checks.
    cycle_time = kst_now()
    order_client = KisOrderClient(
        env,
        kis_api.app_key,
        kis_api.app_secret,
        auth_result.access_token,
        account_no,
        account_product_code,
    )
    order_submitter = (
        GuardedOrderSubmitter(
            order_client,
            db_path=db_path,
            environment=env,
            service_owner_id=service_owner_id,
        )
        if service_owner_id is not None
        else order_client
    )
    post_ai_quote_client = KisRestClient(
        env,
        kis_api.app_key,
        kis_api.app_secret,
        auth_result.access_token,
    )
    ai_client = shared_ai_client
    if ai_client is None:
        if ai == "rule":
            ai_client = RuleBasedAIClient(buy_threshold=auto_config.min_confidence)
        else:
            ai_client = OpenAITradingDecisionClient(
                env_value("OPENAI_API_KEY"),
                model=optional_env_value("OPENAI_MODEL") or "gpt-4o-mini",
                budget=_usage_budget_from_env(),
                timeout=float(_env_number("OPENAI_TIMEOUT_SECONDS", 8, minimum=1)),
                max_retries=int(_env_number("OPENAI_MAX_RETRIES", 1, integer=True, minimum=0)),
            )
    notifier = None
    if notify_telegram:
        notifier = _TelegramNotifier(env_value("TELEGRAM_BOT_TOKEN"), env_value("TELEGRAM_ALLOWED_CHAT_ID"))
    with connect_database(db_path) as database:
        database.init_schema()
        report = run_auto_trade_cycle(
            symbols,
            database=database,
            ai_client=ai_client,
            submitter=order_submitter,
            runtime_control=control,
            config=auto_config,
            confirm_auto_trade=True,
            notifier=notifier,
            current_time=cycle_time,
            clock=kst_now,
            portfolio=None if portfolio is None else portfolio.portfolio,
            entry_budget_checker=(
                None if buying_power_client is None else _entry_budget_checker(buying_power_client)
            ),
            post_ai_price_checker=lambda symbol: float(
                post_ai_quote_client.get_current_price(symbol).price
            ),
            exit_price_checker=lambda symbol: float(
                post_ai_quote_client.get_current_price(symbol).price
            ),
        )
        database.set_runtime_metadata(
            AUTO_TRADE_LAST_CYCLE_KEY,
            json.dumps(
                {
                    "observed_at": cycle_time.isoformat(),
                    "environment": env.value,
                    "ai": ai,
                    "ai_calls": report.ai_call_count,
                    "results": [
                        {
                            "symbol": result.symbol,
                            "action": result.action,
                            "submitted": result.submitted,
                            "blocked": result.blocked,
                            "reason": result.reason,
                            "quantity": result.quantity,
                        }
                        for result in report.results
                    ],
                },
                ensure_ascii=True,
                sort_keys=True,
                separators=(",", ":"),
            ),
            updated_at=cycle_time,
        )
    print("auto-trade-cycle: OK" if report.submitted_count else "auto-trade-cycle: NO_ORDERS")
    for result in report.results:
        print(
            f"symbol={result.symbol} action={result.action} submitted={str(result.submitted).lower()} "
            f"reason={result.reason} quantity={result.quantity} broker_order_id={result.broker_order_id or 'none'}"
        )
    print(f"broker_orders={report.submitted_count} ai_calls={report.ai_call_count}")
    return 0


def _parse_symbols(symbols: str | None) -> list[str]:
    if not symbols:
        return []
    parsed = [item.strip() for item in symbols.replace(";", ",").split(",") if item.strip()]
    for symbol in parsed:
        if not symbol.isdigit() or len(symbol) != 6:
            raise ValueError("symbols must be six-digit domestic stock codes")
    return parsed


def risk_check_sample() -> int:
    intent = OrderIntent("005930", "BREAKOUT_WATCH", "sample-signal", 100_000, 99_000, 0.8)
    decision = evaluate_order_intent(RiskConfig(), PortfolioState(), intent)
    print("offline risk check: OK")
    print(f"approved={str(decision.approved).lower()} reason={decision.reason} quantity={decision.quantity} max_loss_krw={decision.max_loss_krw:g}")
    return 0


def state_machine_sample() -> int:
    state = OrderState.FLAT
    for command in (
        Command.WATCH,
        Command.ARM,
        Command.SUBMIT_ENTRY,
        Command.MARK_ENTRY_FILLED,
        Command.MARK_TP1_FILLED,
        Command.SUBMIT_EXIT,
        Command.MARK_EXIT_FILLED,
    ):
        state = transition(state, command)
    signal_id = build_signal_id("BREAKOUT_WATCH", "005930", "2026-08-15T09:01:00")
    ledger = SignalLedger()
    ledger.record(signal_id)
    duplicate_rejected = False
    try:
        ledger.record(signal_id)
    except ValueError:
        duplicate_rejected = True
    if not duplicate_rejected:
        raise RuntimeError("sample duplicate signal was not rejected")
    print("offline state machine: OK")
    print(f"final_state={state.value} signal_id={signal_id} duplicate_rejected=true")
    return 0


def position_check_sample() -> int:
    opened_at = datetime(2026, 8, 15, 9, 0)
    position = ManagedPosition("005930", 10, 100.0, 98.0, 101.0, 102.0, opened_at)
    decision = evaluate_position(position, 101.0, datetime(2026, 8, 15, 9, 1))
    print("offline position manager: OK")
    print(
        f"action={decision.action.value} reason={decision.reason} "
        f"quantity={decision.quantity} new_stop_loss={decision.new_stop_loss:g}"
    )
    print("broker_calls=none order_submission=none account_queries=none")
    return 0


def paper_check_sample() -> int:
    ledger = PaperLedger()
    ledger.submit_order(PaperOrderIntent("paper-buy-1", "005930", PaperSide.BUY, 10, 100.0))
    ledger.fill_order(PaperFill("paper-fill-buy-1", "paper-buy-1", 10, 100.0))
    ledger.submit_order(PaperOrderIntent("paper-sell-1", "005930", PaperSide.SELL, 4, 105.0))
    ledger.fill_order(PaperFill("paper-fill-sell-1", "paper-sell-1", 4, 105.0))
    ledger.submit_order(PaperOrderIntent("paper-sell-2", "005930", PaperSide.SELL, 6, 110.0))
    ledger.fill_order(PaperFill("paper-fill-sell-2", "paper-sell-2", 6, 110.0))
    print("paper trade check: OK")
    print(f"realized_pnl={ledger.realized_pnl:g} open_positions={len(ledger.positions)}")
    print("broker_calls=none orders=none account_queries=none ai_calls=none")
    return 0


def dry_run_pipeline(csv_path: str | None, symbol: str, db_path: str | None) -> int:
    bars = parse_bars_csv(csv_path) if csv_path else sample_bars(symbol)
    bars = [bar for bar in bars if bar.symbol == symbol]
    if not bars:
        raise ValueError(f"no bars found for symbol {symbol}")
    report = run_offline_dry_run(bars)
    if db_path:
        with connect_database(db_path) as database:
            database.init_schema()
            for bar in bars:
                database.save_bar(bar)
            snapshot = build_feature_snapshot(bars)
            candidates = scan_candidates(snapshot) if snapshot is not None else []
            for candidate in candidates:
                database.save_candidate(candidate, bars[-1].start)
    print("offline dry-run: OK")
    print(
        f"symbol={report.symbol} bars={report.bars_count} "
        f"candidates={report.candidates_count} "
        f"selected_strategy={report.selected_strategy or 'none'}"
    )
    print(
        f"risk_approved={str(report.risk_approved).lower()} "
        f"risk_reason={report.risk_reason} risk_quantity={report.risk_quantity}"
    )
    print(
        f"lifecycle_final_state={report.lifecycle_final_state} "
        f"position_action={report.position_action or 'none'} "
        f"position_reason={report.position_reason or 'none'}"
    )
    print("broker_calls=none ai_calls=none")
    return 0


def market_health_check(
    symbol: str,
    db_path: str,
    websocket_acknowledged: bool,
    max_tick_age_seconds: float,
    max_bar_age_seconds: float,
) -> int:
    with connect_database(db_path) as database:
        database.init_schema()
        latest_tick = database.latest_tick(symbol)
        latest_bar = database.latest_bar(symbol)
    health = evaluate_market_health(
        kst_now(),
        websocket_acknowledged=websocket_acknowledged,
        latest_tick=latest_tick,
        latest_bar=latest_bar,
        max_tick_age_seconds=max_tick_age_seconds,
        max_bar_age_seconds=max_bar_age_seconds,
    )
    print("market health check: OK")
    print(
        f"symbol={symbol} status={health.status.value} "
        f"trading_blocked={str(health.trading_blocked).lower()} "
        f"safe_mode={str(health.enter_safe_mode).lower()}"
    )
    print(
        f"reason={health.reason} "
        f"tick_age_seconds={health.tick_age_seconds if health.tick_age_seconds is not None else 'none'} "
        f"bar_age_seconds={health.bar_age_seconds if health.bar_age_seconds is not None else 'none'}"
    )
    print("broker_calls=none orders=none account_queries=none ai_calls=none")
    return 0 if not health.trading_blocked else 3


def shadow_cycle_check(
    symbol: str,
    db_path: str,
    websocket_acknowledged: bool,
    max_tick_age_seconds: float,
    max_bar_age_seconds: float,
) -> int:
    with connect_database(db_path) as database:
        database.init_schema()
        report = run_shadow_cycle(
            symbol,
            database=database,
            config=ShadowCycleConfig(
                websocket_acknowledged=websocket_acknowledged,
                max_tick_age_seconds=max_tick_age_seconds,
                max_bar_age_seconds=max_bar_age_seconds,
            ),
        )
    print("shadow cycle: OK")
    print(
        f"symbol={report.symbol} health_status={report.health_status} "
        f"trading_blocked={str(report.trading_blocked).lower()} "
        f"safe_mode={str(report.safe_mode).lower()}"
    )
    print(
        f"bars_count={report.bars_count} "
        f"candidates_count={report.candidates_count} "
        f"selected_strategy={report.selected_strategy or 'none'} "
        f"risk_approved={str(report.risk_approved).lower()} "
        f"risk_reason={report.risk_reason} risk_quantity={report.risk_quantity}"
    )
    print(f"lifecycle_final_state={report.lifecycle_final_state}")
    print("orders=none account_queries=none ai_calls=none")
    return 0 if not report.trading_blocked else 3


def paper_shadow_cycle_check(
    symbol: str,
    db_path: str,
    websocket_acknowledged: bool,
    max_tick_age_seconds: float,
    max_bar_age_seconds: float,
) -> int:
    with connect_database(db_path) as database:
        database.init_schema()
        result = run_paper_shadow_cycle(
            symbol,
            database=database,
            config=ShadowCycleConfig(
                websocket_acknowledged=websocket_acknowledged,
                max_tick_age_seconds=max_tick_age_seconds,
                max_bar_age_seconds=max_bar_age_seconds,
            ),
        )
    report = result.shadow
    eligible = (
        not report.trading_blocked
        and report.risk_approved
        and report.risk_quantity > 0
        and report.signal_id
        and report.entry_price is not None
    )
    print("paper shadow cycle: RECORDED" if result.recorded else "paper shadow cycle: DUPLICATE" if result.duplicate_skipped else "paper shadow cycle: BLOCKED")
    print(
        f"symbol={report.symbol} health_status={report.health_status} "
        f"trading_blocked={str(report.trading_blocked).lower()} "
        f"risk_approved={str(report.risk_approved).lower()} "
        f"risk_quantity={report.risk_quantity} signal_id={report.signal_id or 'none'} "
        f"entry_price={report.entry_price if report.entry_price is not None else 'none'}"
    )
    print(
        f"paper_orders={'1' if result.recorded else '0'} "
        f"paper_orders_are_local_only=true broker_orders=none "
        f"duplicate_skipped={str(result.duplicate_skipped).lower()} eligible={str(bool(eligible)).lower()}"
    )
    print("broker_calls=none orders=none account_queries=none ai_calls=none")
    return 0 if eligible else 3


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="kis-ai-scalper")
    subparsers = parser.add_subparsers(dest="command", required=True)
    smoke = subparsers.add_parser("smoke-kis", help="read-only KIS auth and current-price check")
    smoke.add_argument("--config", default="config/settings.yaml")
    smoke.add_argument("--env", choices=[env.value for env in KisEnvironment], default=os.getenv("KIS_ENV", "demo"))
    smoke.add_argument("--symbol", default="005930")
    smoke.add_argument("--refresh-token", action="store_true", help="ignore the local token cache")
    broker_state = subparsers.add_parser("smoke-broker-state", help="read-only KIS order/account state check")
    broker_state.add_argument("--config", default="config/settings.yaml")
    broker_state.add_argument("--env", choices=[env.value for env in KisEnvironment], default=os.getenv("KIS_ENV", "demo"))
    broker_state.add_argument("--refresh-token", action="store_true", help="ignore the local token cache")
    ws = subparsers.add_parser("smoke-ws", help="read-only KIS realtime price subscription")
    ws.add_argument("--config", default="config/settings.yaml")
    ws.add_argument("--env", choices=[env.value for env in KisEnvironment], default=os.getenv("KIS_ENV", "demo"))
    ws.add_argument("--symbol", default="005930")
    ws.add_argument("--seconds", type=int, default=10)
    ws.add_argument("--refresh-token", action="store_true", help="ignore the local token cache")
    fill_ws = subparsers.add_parser(
        "smoke-fill-notice", help="bounded read-only KIS fill-notice subscription smoke"
    )
    fill_ws.add_argument("--config", default="config/settings.yaml")
    fill_ws.add_argument("--env", choices=[env.value for env in KisEnvironment], default=os.getenv("KIS_ENV", "demo"))
    fill_ws.add_argument("--seconds", type=int, default=10)
    fill_ws.add_argument("--refresh-token", action="store_true", help="ignore the local token cache")
    collector = subparsers.add_parser("collect-market", help="bounded read-only KIS tick/bar collector")
    collector.add_argument("--config", default="config/settings.yaml")
    collector.add_argument("--env", choices=[env.value for env in KisEnvironment], default=os.getenv("KIS_ENV", "demo"))
    collector.add_argument("--symbol", default="005930")
    collector.add_argument("--seconds", type=int, default=60)
    collector.add_argument("--db", default="data/kis_ai_scalper.sqlite3")
    collector.add_argument("--refresh-token", action="store_true", help="ignore the local token cache")
    user = subparsers.add_parser("user-test", help="bounded read-only collection and shadow-cycle test")
    user.add_argument("--config", default="config/settings.yaml")
    user.add_argument("--env", choices=[env.value for env in KisEnvironment], default=os.getenv("KIS_ENV", "demo"))
    user.add_argument("--symbol", default="005930")
    user.add_argument("--seconds", type=int, default=60)
    user.add_argument("--db", default="data/kis_ai_scalper.sqlite3")
    user.add_argument("--max-tick-age-seconds", type=float, default=5.0)
    user.add_argument("--max-bar-age-seconds", type=float, default=90.0)
    user.add_argument("--refresh-token", action="store_true", help="ignore the local token cache")
    session = subparsers.add_parser("paper-session", help="bounded read-only collection and local paper shadow session")
    session.add_argument("--config", default="config/settings.yaml")
    session.add_argument("--env", choices=[env.value for env in KisEnvironment], default=os.getenv("KIS_ENV", "demo"))
    session.add_argument("--symbol", default="005930")
    session.add_argument("--iterations", type=int, default=1)
    session.add_argument("--collect-seconds", type=int, default=60)
    session.add_argument("--sleep-seconds", type=int, default=0)
    session.add_argument("--db", default="data/kis_ai_scalper.sqlite3")
    session.add_argument("--max-tick-age-seconds", type=float, default=5.0)
    session.add_argument("--max-bar-age-seconds", type=float, default=90.0)
    session.add_argument("--refresh-token", action="store_true", help="ignore the local token cache")
    analyze = subparsers.add_parser("analyze-bars", help="offline OHLCV replay and candidate analysis")
    analyze.add_argument("--csv", default=None)
    analyze.add_argument("--symbol", default="005930")
    analyze.add_argument("--db", default="data/kis_ai_scalper.sqlite3")
    paper_report_parser = subparsers.add_parser("paper-report", help="report local SQLite paper journal")
    paper_report_parser.add_argument("--db", default="data/kis_ai_scalper.sqlite3")
    paper_report_parser.add_argument("--symbol", default=None)
    control = subparsers.add_parser("control-status", help="show the local runtime pause gate")
    control.add_argument("--db", default="data/kis_ai_scalper.sqlite3")
    control_pause = subparsers.add_parser("control-pause", help="pause local paper runtime")
    control_pause.add_argument("--db", default="data/kis_ai_scalper.sqlite3")
    control_pause.add_argument("--reason", default="cli_operator")
    control_resume = subparsers.add_parser("control-resume", help="resume local paper runtime")
    control_resume.add_argument("--db", default="data/kis_ai_scalper.sqlite3")
    control_resume.add_argument("--reason", default="cli_operator")
    telegram = subparsers.add_parser("telegram-poll", help="bounded Telegram operator poll")
    telegram.add_argument("--db", default="data/kis_ai_scalper.sqlite3")
    telegram.add_argument("--bot-token-env", default="TELEGRAM_BOT_TOKEN")
    telegram.add_argument("--allowed-chat-id-env", default="TELEGRAM_ALLOWED_CHAT_ID")
    telegram.add_argument("--limit", type=int, default=10)
    telegram.add_argument("--timeout-seconds", type=int, default=0)
    wl_add = subparsers.add_parser("watchlist-add", help="add one or more symbols to the local watchlist")
    wl_add.add_argument("--db", default="data/kis_ai_scalper.sqlite3")
    wl_add.add_argument("--symbols", required=True)
    wl_remove = subparsers.add_parser("watchlist-remove", help="disable one or more watchlist symbols")
    wl_remove.add_argument("--db", default="data/kis_ai_scalper.sqlite3")
    wl_remove.add_argument("--symbols", required=True)
    wl_list = subparsers.add_parser("watchlist-list", help="list local watchlist symbols")
    wl_list.add_argument("--db", default="data/kis_ai_scalper.sqlite3")
    wl_list.add_argument("--all", action="store_true")
    subparsers.add_parser("risk-check-sample", help="offline deterministic risk calculation")
    subparsers.add_parser("state-machine-sample", help="offline dry-run lifecycle and idempotency check")
    subparsers.add_parser("position-check-sample", help="offline pure position lifecycle check")
    subparsers.add_parser("paper-check-sample", help="offline local paper-trade ledger check")
    pipeline = subparsers.add_parser("dry-run-pipeline", help="offline end-to-end safety pipeline")
    pipeline.add_argument("--csv", default=None)
    pipeline.add_argument("--symbol", default="005930")
    pipeline.add_argument("--db", default=None)
    health = subparsers.add_parser("market-health", help="offline DB freshness and safety gate check")
    health.add_argument("--symbol", default="005930")
    health.add_argument("--db", default="data/kis_ai_scalper.sqlite3")
    health.add_argument("--websocket-acknowledged", action="store_true")
    health.add_argument("--max-tick-age-seconds", type=float, default=5.0)
    health.add_argument("--max-bar-age-seconds", type=float, default=90.0)
    shadow = subparsers.add_parser("shadow-cycle", help="bounded offline DB shadow analysis")
    shadow.add_argument("--symbol", default="005930")
    shadow.add_argument("--db", default="data/kis_ai_scalper.sqlite3")
    shadow.add_argument("--websocket-acknowledged", action="store_true")
    shadow.add_argument("--max-tick-age-seconds", type=float, default=5.0)
    shadow.add_argument("--max-bar-age-seconds", type=float, default=90.0)
    paper_shadow = subparsers.add_parser("paper-shadow-cycle", help="persist one approved local paper BUY")
    paper_shadow.add_argument("--symbol", default="005930")
    paper_shadow.add_argument("--db", default="data/kis_ai_scalper.sqlite3")
    paper_shadow.add_argument("--websocket-acknowledged", action="store_true")
    paper_shadow.add_argument("--max-tick-age-seconds", type=float, default=5.0)
    paper_shadow.add_argument("--max-bar-age-seconds", type=float, default=90.0)
    live_order = subparsers.add_parser(
        "submit-live-shadow",
        help="submit one gated KIS BUY from an approved local shadow signal",
    )
    live_order.add_argument("--config", default="config/settings.yaml")
    live_order.add_argument("--env", choices=[env.value for env in KisEnvironment], default=os.getenv("KIS_ENV", "demo"))
    live_order.add_argument("--symbol", default="005930")
    live_order.add_argument("--db", default="data/kis_ai_scalper.sqlite3")
    live_order.add_argument("--max-quantity", type=int, default=1)
    live_order.add_argument("--websocket-acknowledged", action="store_true")
    live_order.add_argument(
        "--confirm",
        default="",
        help="must be exactly SUBMIT_KIS_ORDER before any broker order is attempted",
    )
    live_order.add_argument("--refresh-token", action="store_true", help="ignore the local token cache")
    auto = subparsers.add_parser("auto-trade-cycle", help="bounded multi-symbol AI auto-trading cycle")
    auto.add_argument("--config", default="config/settings.yaml")
    auto.add_argument("--env", choices=[env.value for env in KisEnvironment], default=os.getenv("KIS_ENV", "demo"))
    auto.add_argument("--symbols", default=None, help="comma-separated six-digit symbols; defaults to watchlist")
    auto.add_argument("--db", default="data/kis_ai_scalper.sqlite3")
    auto.add_argument("--ai", choices=["rule", "openai"], default="openai")
    auto.add_argument("--max-quantity", type=int, default=1)
    auto.add_argument("--collect-seconds", type=int, default=0)
    auto.add_argument("--confirm", default="", help="must be exactly AUTO_TRADE before any broker order is attempted")
    auto.add_argument("--notify-telegram", action="store_true")
    auto.add_argument("--refresh-token", action="store_true", help="ignore the local token cache")
    service = subparsers.add_parser("service-loop", help="long-running Docker operator loop")
    service.add_argument("--config", default="config/settings.yaml")
    service.add_argument("--symbols", default=None, help="comma-separated six-digit symbols; defaults to watchlist")
    service.add_argument("--db", default="data/kis_ai_scalper.sqlite3")
    service.add_argument("--ai", choices=["rule", "openai"], default=os.getenv("AUTO_TRADE_AI", "openai"))
    service.add_argument("--max-quantity", type=int, default=int(os.getenv("AUTO_TRADE_MAX_QUANTITY", "1")))
    service.add_argument("--collect-seconds", type=int, default=int(os.getenv("AUTO_TRADE_COLLECT_SECONDS", "10")))
    service.add_argument("--cycle-interval-seconds", type=int, default=int(os.getenv("AUTO_TRADE_CYCLE_INTERVAL_SECONDS", "20")))
    service.add_argument("--telegram-limit", type=int, default=10)
    service.add_argument("--telegram-timeout-seconds", type=int, default=5)
    service.add_argument("--refresh-token", action="store_true", help="ignore the local token cache")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        if args.command == "smoke-kis":
            return smoke_kis(args.config, args.env, args.symbol, args.refresh_token)
        if args.command == "smoke-broker-state":
            return smoke_broker_state(args.config, args.env, args.refresh_token)
        if args.command == "smoke-ws":
            return smoke_ws(args.config, args.env, args.symbol, args.seconds, args.refresh_token)
        if args.command == "smoke-fill-notice":
            return smoke_fill_notice(args.config, args.env, args.seconds, args.refresh_token)
        if args.command == "collect-market":
            return collect_market(args.config, args.env, args.symbol, args.seconds, args.db, args.refresh_token)
        if args.command == "user-test":
            return user_test(
                args.config, args.env, args.symbol, args.seconds, args.db,
                args.max_tick_age_seconds, args.max_bar_age_seconds, args.refresh_token,
            )
        if args.command == "paper-session":
            return paper_session(
                args.config, args.env, args.symbol, args.iterations,
                args.collect_seconds, args.sleep_seconds, args.db,
                args.max_tick_age_seconds, args.max_bar_age_seconds, args.refresh_token,
            )
        if args.command == "analyze-bars":
            return analyze_bars(args.csv, args.symbol, args.db)
        if args.command == "paper-report":
            return paper_report(args.db, args.symbol)
        if args.command == "control-status":
            return runtime_control_status(args.db)
        if args.command == "control-pause":
            return runtime_control_set(args.db, True, args.reason)
        if args.command == "control-resume":
            return runtime_control_set(args.db, False, args.reason)
        if args.command == "telegram-poll":
            return telegram_poll(args.db, args.bot_token_env, args.allowed_chat_id_env, args.limit, args.timeout_seconds)
        if args.command == "watchlist-add":
            return watchlist_add(args.db, args.symbols)
        if args.command == "watchlist-remove":
            return watchlist_remove(args.db, args.symbols)
        if args.command == "watchlist-list":
            return watchlist_list(args.db, args.all)
        if args.command == "risk-check-sample":
            return risk_check_sample()
        if args.command == "state-machine-sample":
            return state_machine_sample()
        if args.command == "position-check-sample":
            return position_check_sample()
        if args.command == "paper-check-sample":
            return paper_check_sample()
        if args.command == "dry-run-pipeline":
            return dry_run_pipeline(args.csv, args.symbol, args.db)
        if args.command == "market-health":
            return market_health_check(
                args.symbol,
                args.db,
                args.websocket_acknowledged,
                args.max_tick_age_seconds,
                args.max_bar_age_seconds,
            )
        if args.command == "shadow-cycle":
            return shadow_cycle_check(
                args.symbol, args.db, args.websocket_acknowledged,
                args.max_tick_age_seconds, args.max_bar_age_seconds,
            )
        if args.command == "paper-shadow-cycle":
            return paper_shadow_cycle_check(
                args.symbol, args.db, args.websocket_acknowledged,
                args.max_tick_age_seconds, args.max_bar_age_seconds,
            )
        if args.command == "submit-live-shadow":
            return submit_live_shadow_order(
                args.config, args.env, args.symbol, args.db, args.max_quantity,
                args.websocket_acknowledged, args.confirm, args.refresh_token,
            )
        if args.command == "auto-trade-cycle":
            return auto_trade_cycle(
                args.config, args.env, args.symbols, args.db, args.ai,
                args.max_quantity, args.collect_seconds, args.confirm,
                args.notify_telegram, args.refresh_token,
            )
        if args.command == "service-loop":
            return service_loop(
                args.config, args.symbols, args.db, args.ai,
                args.max_quantity, args.collect_seconds,
                args.cycle_interval_seconds,
                args.telegram_limit, args.telegram_timeout_seconds,
                True, args.refresh_token,
            )
    except Exception as exc:
        if args.command in {
            "smoke-kis", "smoke-broker-state", "smoke-ws", "smoke-fill-notice", "collect-market", "user-test",
            "paper-session", "submit-live-shadow", "auto-trade-cycle",
            "service-loop",
        }:
            symbol = getattr(args, "symbol", None) or getattr(args, "symbols", None) or "unknown"
            environment = getattr(args, "env", "runtime")
            print(f"environment={environment} symbol={symbol} cache_hit=unknown token=<not-issued> approval_key=<not-issued>", file=sys.stderr)
        print(f"{args.command} failed ({type(exc).__name__}: {exc})", file=sys.stderr)
        return 1
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
