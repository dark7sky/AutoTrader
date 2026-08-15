"""Bounded read-only collection followed by one local shadow-cycle evaluation."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Awaitable, Callable

from kis_ai_scalper.market.collector import CollectorResult, collect_realtime_prices
from kis_ai_scalper.pipeline.shadow_cycle import (
    ShadowCycleConfig,
    ShadowCycleReport,
    run_shadow_cycle,
)
from kis_ai_scalper.storage import connect_database


@dataclass(frozen=True)
class UserTestReport:
    collector: CollectorResult
    shadow: ShadowCycleReport

    @property
    def no_data(self) -> bool:
        return self.collector.ticks_saved == 0 and self.collector.bars_saved == 0

    @property
    def exit_code(self) -> int:
        if (
            self.collector.subscribe_ack
            and not self.no_data
            and self.shadow.bars_count > 0
            and not self.shadow.trading_blocked
            and not self.shadow.safe_mode
            and self.shadow.health_status not in {"STALE", "NO_DATA"}
        ):
            return 0
        return 3


async def run_user_test(
    endpoint: str,
    approval_key: str,
    symbol: str,
    db_path: str,
    seconds: int = 60,
    max_tick_age_seconds: float = 5.0,
    max_bar_age_seconds: float = 90.0,
    *,
    collector: Callable[..., Awaitable[CollectorResult]] = collect_realtime_prices,
    shadow_runner: Callable[..., ShadowCycleReport] = run_shadow_cycle,
) -> UserTestReport:
    """Collect into ``db_path`` and evaluate that same database once."""
    collected = await collector(endpoint, approval_key, symbol, db_path, seconds)
    with connect_database(db_path) as database:
        database.init_schema()
        shadow = shadow_runner(
            symbol,
            database=database,
            config=ShadowCycleConfig(
                websocket_acknowledged=collected.subscribe_ack,
                max_tick_age_seconds=max_tick_age_seconds,
                max_bar_age_seconds=max_bar_age_seconds,
            ),
        )
    return UserTestReport(collected, shadow)
