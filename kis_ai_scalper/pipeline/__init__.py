"""Offline pipeline entry points."""

from .dry_run import DryRunConfig, DryRunReport, run_offline_dry_run
from .shadow_cycle import ShadowCycleConfig, ShadowCycleReport, run_shadow_cycle
from .paper_shadow import PaperShadowResult, record_shadow_paper_buy, run_paper_shadow_cycle
from .user_test import UserTestReport, run_user_test
from .paper_session import PaperSessionIteration, PaperSessionReport, run_paper_session
from .live_execution import LiveOrderExecutionResult, submit_shadow_live_buy
from .auto_trade import (
    AutoTradeConfig,
    AutoTradeCycleReport,
    AutoTradeSymbolResult,
    run_auto_trade_cycle,
)

__all__ = [
    "DryRunConfig", "DryRunReport", "run_offline_dry_run",
    "ShadowCycleConfig", "ShadowCycleReport", "run_shadow_cycle",
    "PaperShadowResult", "record_shadow_paper_buy", "run_paper_shadow_cycle",
    "UserTestReport", "run_user_test",
    "PaperSessionIteration", "PaperSessionReport", "run_paper_session",
    "LiveOrderExecutionResult", "submit_shadow_live_buy",
    "AutoTradeConfig", "AutoTradeCycleReport", "AutoTradeSymbolResult",
    "run_auto_trade_cycle",
]
