"""Command-line entry points for safe connectivity checks."""

from __future__ import annotations

import argparse
import asyncio
import os
import sys
import time
from datetime import datetime
from pathlib import Path

from kis_ai_scalper.broker.kis_auth import KisAuthClient, redact
from kis_ai_scalper.broker.kis_balance import KisBalanceClient
from kis_ai_scalper.broker.kis_endpoints import KisEnvironment
from kis_ai_scalper.broker.kis_order import KisOrderClient
from kis_ai_scalper.broker.kis_rest import KisRestClient
from kis_ai_scalper.broker.kis_endpoints import websocket_url
from kis_ai_scalper.broker.kis_ws import smoke_realtime_price
from kis_ai_scalper.config import load_config
from kis_ai_scalper.schemas.types import TradingMode
from kis_ai_scalper.ai.decision import OpenAITradingDecisionClient, RuleBasedAIClient
from kis_ai_scalper.market.clock import kst_now
from kis_ai_scalper.market.collector import collect_realtime_prices
from kis_ai_scalper.market.features import build_feature_snapshot
from kis_ai_scalper.market.health import evaluate_market_health
from kis_ai_scalper.market.schedule import is_regular_market_open
from kis_ai_scalper.storage import connect_database
from kis_ai_scalper.storage.replay import parse_bars_csv, sample_bars
from kis_ai_scalper.strategies.candidate import scan_candidates
from kis_ai_scalper.risk import OrderIntent, PortfolioState, RiskConfig, evaluate_order_intent
from kis_ai_scalper.execution import Command, OrderState, SignalLedger, build_signal_id, transition
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
    print("KIS WebSocket smoke: OK")
    print(f"environment={env.value} symbol={symbol} ws_url={websocket_url(env)}")
    print(f"cache_hit={str(result.cache_hit).lower()} subscribe_ack={str(ws_result.acknowledged).lower()} tick_count={len(ticks)} first_price={first} last_price={last}")
    print("orders=none account_queries=none execution_notices=none")
    return 0


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


def _runtime_preflight(config_path: str, environment: KisEnvironment, ai: str) -> list[str]:
    errors: list[str] = []
    try:
        config = load_config(Path(config_path))
    except Exception as exc:
        return [f"config: {exc}"]
    for label, check in (
        ("kis_api", lambda: _kis_api_for(config, environment)),
        ("kis_account", lambda: _kis_account_for(config, environment)),
        ("broker_gate", lambda: _assert_broker_order_allowed(config, environment)),
    ):
        try:
            check()
        except Exception as exc:
            errors.append(f"{label}: {exc}")
    if ai == "openai" and optional_env_value("OPENAI_API_KEY") is None:
        errors.append("openai: OPENAI_API_KEY is required when AUTO_TRADE_AI=openai")
    return errors


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
        with connect_database(db_path) as database:
            database.init_schema()
            for symbol in local_only:
                for position in database.list_open_live_positions(symbol):
                    database.close_live_position(
                        position_id=str(position["position_id"]),
                        exit_broker_order_id=None,
                        close_reason="broker_position_missing",
                    )
        messages.append("local_positions_closed=" + ",".join(
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
    requires_operator = bool(broker_only or mismatched)
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
    if pause_on_start:
        set_paused(db_path, True, "service_start_default_pause", "service")
    notifier = _telegram_notifier_from_env()
    last_reconciliation_message = ""
    last_reconciliation_alert_at = 0.0
    if notifier is not None:
        notifier.send(
            "auto-trade service started\n"
            "runtime: paused\n"
            "Use /control then Resume after env/positions are checked."
        )
    print("service-loop: started")
    while True:
        started = time.monotonic()
        try:
            token = optional_env_value("TELEGRAM_BOT_TOKEN")
            chat_id = optional_env_value("TELEGRAM_ALLOWED_CHAT_ID")
            if token and chat_id:
                poll_telegram(
                    db_path, token, chat_id,
                    limit=telegram_limit,
                    timeout_seconds=telegram_timeout_seconds,
                    client=TelegramClient(token),
                )
            control = control_status(db_path)
            if control.paused:
                print(f"service-loop: paused environment={control.environment}")
                _sleep_remaining(started, cycle_interval_seconds)
                continue
            now = kst_now()
            if not is_regular_market_open(now):
                print(f"service-loop: market_closed environment={control.environment}")
                _sleep_remaining(started, cycle_interval_seconds)
                continue
            env = KisEnvironment.parse(control.environment)
            errors = _runtime_preflight(config_path, env, ai)
            if errors:
                set_paused(db_path, True, "preflight_failed", "service")
                message = "auto-trade paused: setup issue\n" + "\n".join(f"- {item}" for item in errors)
                print(message)
                _notify_operator_if_possible(message)
                _sleep_remaining(started, cycle_interval_seconds)
                continue
            reconciliation_ok, reconciliation_messages = _reconcile_broker_positions(
                config_path, env, db_path, refresh_token,
            )
            if reconciliation_messages:
                message = (
                    "position reconciliation\n"
                    + "\n".join(f"- {item}" for item in reconciliation_messages)
                )
                print(message)
                if (
                    message != last_reconciliation_message
                    or time.monotonic() - last_reconciliation_alert_at > 300
                ):
                    last_reconciliation_message = message
                    last_reconciliation_alert_at = time.monotonic()
                    _notify_operator_if_possible(
                        message
                        + "\nIf operator_review is shown, decide manually. "
                        "Use /positions and broker app, then /pause if you want to stop service."
                    )
            if not reconciliation_ok:
                print("service-loop: reconciliation_pending broker_orders=none")
                _sleep_remaining(started, cycle_interval_seconds)
                continue
            auto_trade_cycle(
                config_path, env.value, symbols_text, db_path, ai,
                max_quantity, collect_seconds, "AUTO_TRADE",
                notify_telegram=notifier is not None,
                refresh_token=refresh_token,
            )
        except Exception as exc:
            try:
                set_paused(db_path, True, "service_error", "service")
            except Exception:
                pass
            message = f"auto-trade paused: service error\n{type(exc).__name__}: {exc}"
            print(message, file=sys.stderr)
            _notify_operator_if_possible(message)
        _sleep_remaining(started, cycle_interval_seconds)


def _sleep_remaining(started: float, interval_seconds: int) -> None:
    elapsed = time.monotonic() - started
    time.sleep(max(0.0, interval_seconds - elapsed))


class _TelegramNotifier:
    def __init__(self, token: str, chat_id: str) -> None:
        from kis_ai_scalper.ops.telegram import TelegramClient

        self.client = TelegramClient(token)
        self.chat_id = chat_id

    def send(self, text: str) -> None:
        self.client.send_message(self.chat_id, text)


def _env_live_trading_enabled() -> bool:
    return env_value("LIVE_TRADING_ENABLED").lower() == "true"


def _telegram_notifier_from_env() -> _TelegramNotifier | None:
    token = optional_env_value("TELEGRAM_BOT_TOKEN")
    chat_id = optional_env_value("TELEGRAM_ALLOWED_CHAT_ID")
    if not token or not chat_id:
        return None
    return _TelegramNotifier(token, chat_id)


def _notify_operator_if_possible(text: str) -> None:
    notifier = _telegram_notifier_from_env()
    if notifier is not None:
        notifier.send(text)


def _assert_broker_order_allowed(config, environment: KisEnvironment) -> None:
    if not (config.live_trading_enabled and _env_live_trading_enabled()):
        raise ValueError(
            "broker order submission requires YAML live_trading_enabled=true "
            "and LIVE_TRADING_ENABLED=true"
        )
    if environment is KisEnvironment.REAL and config.mode is not TradingMode.LIVE:
        raise ValueError("real broker orders require TRADING_MODE=live")
    if environment is KisEnvironment.DEMO and config.mode not in {
        TradingMode.MICRO_LIVE,
        TradingMode.LIVE,
    }:
        raise ValueError("demo broker orders require TRADING_MODE=micro_live or live")


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
    _assert_broker_order_allowed(config, env)
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
        control = database.get_runtime_control()
    if not symbols and not open_position_symbols:
        raise ValueError("no symbols supplied and watchlist is empty")
    if control.paused:
        print("auto-trade-cycle: BLOCKED")
        print("reason=runtime_paused broker_orders=none ai_calls=none")
        return 3
    now = kst_now()
    if not is_regular_market_open(now):
        print("auto-trade-cycle: BLOCKED")
        print("reason=market_closed broker_orders=none ai_calls=none")
        return 3

    env = KisEnvironment.parse(environment)
    config = load_config(Path(config_path))
    kis_api = _kis_api_for(config, env)
    kis_account = _kis_account_for(config, env)
    _assert_broker_order_allowed(config, env)
    account_no, account_product_code = _account_components(
        kis_account.account_no,
        kis_account.account_product_code,
    )
    project_root = Path(config_path).resolve().parent.parent
    cache_path = project_root / "data" / "auth" / f"kis_token_{env.value}.json"
    auth = KisAuthClient(env, kis_api.app_key, kis_api.app_secret)
    auth_result = auth.authenticate_read_only(cache_path=cache_path, refresh_token=refresh_token)
    if collect_seconds:
        for symbol in symbols:
            asyncio.run(collect_realtime_prices(
                websocket_url(env), auth_result.approval_key, symbol, db_path, collect_seconds,
            ))
    order_client = KisOrderClient(
        env,
        kis_api.app_key,
        kis_api.app_secret,
        auth_result.access_token,
        account_no,
        account_product_code,
    )
    ai_client = RuleBasedAIClient() if ai == "rule" else OpenAITradingDecisionClient(env_value("OPENAI_API_KEY"))
    notifier = None
    if notify_telegram:
        notifier = _TelegramNotifier(env_value("TELEGRAM_BOT_TOKEN"), env_value("TELEGRAM_ALLOWED_CHAT_ID"))
    with connect_database(db_path) as database:
        database.init_schema()
        report = run_auto_trade_cycle(
            symbols,
            database=database,
            ai_client=ai_client,
            submitter=order_client,
            runtime_control=database.get_runtime_control(),
            config=AutoTradeConfig(max_quantity=max_quantity),
            confirm_auto_trade=True,
            notifier=notifier,
            current_time=now,
        )
    print("auto-trade-cycle: OK" if report.submitted_count else "auto-trade-cycle: NO_ORDERS")
    for result in report.results:
        print(
            f"symbol={result.symbol} action={result.action} submitted={str(result.submitted).lower()} "
            f"reason={result.reason} quantity={result.quantity} broker_order_id={result.broker_order_id or 'none'}"
        )
    print(f"broker_orders={report.submitted_count} ai_calls={'0' if ai == 'rule' else len(symbols)}")
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
    ws = subparsers.add_parser("smoke-ws", help="read-only KIS realtime price subscription")
    ws.add_argument("--config", default="config/settings.yaml")
    ws.add_argument("--env", choices=[env.value for env in KisEnvironment], default=os.getenv("KIS_ENV", "demo"))
    ws.add_argument("--symbol", default="005930")
    ws.add_argument("--seconds", type=int, default=10)
    ws.add_argument("--refresh-token", action="store_true", help="ignore the local token cache")
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
    service.add_argument("--cycle-interval-seconds", type=int, default=int(os.getenv("AUTO_TRADE_CYCLE_INTERVAL_SECONDS", "60")))
    service.add_argument("--telegram-limit", type=int, default=10)
    service.add_argument("--telegram-timeout-seconds", type=int, default=5)
    service.add_argument("--pause-on-start", action=argparse.BooleanOptionalAction, default=True)
    service.add_argument("--refresh-token", action="store_true", help="ignore the local token cache")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        if args.command == "smoke-kis":
            return smoke_kis(args.config, args.env, args.symbol, args.refresh_token)
        if args.command == "smoke-ws":
            return smoke_ws(args.config, args.env, args.symbol, args.seconds, args.refresh_token)
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
                args.pause_on_start, args.refresh_token,
            )
    except Exception as exc:
        if args.command in {
            "smoke-kis", "smoke-ws", "collect-market", "user-test",
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
