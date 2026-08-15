"""Conservative KRX domestic-stock price and trade-risk rules."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, InvalidOperation, ROUND_CEILING, ROUND_FLOOR
import math
from typing import Any


def _decimal(value: Any, name: str) -> Decimal:
    if isinstance(value, bool):
        raise ValueError(f"{name} must be a finite positive number")
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError) as exc:
        raise ValueError(f"{name} must be a finite positive number") from exc
    if not parsed.is_finite() or parsed <= 0:
        raise ValueError(f"{name} must be a finite positive number")
    return parsed


def krx_tick_size(price: Any) -> int:
    value = _decimal(price, "price")
    if value < 2_000:
        return 1
    if value < 5_000:
        return 5
    if value < 20_000:
        return 10
    if value < 50_000:
        return 50
    if value < 200_000:
        return 100
    if value < 500_000:
        return 500
    return 1_000


def normalize_krx_limit_price(price: Any, side: str = "buy") -> int:
    value = _decimal(price, "price")
    side_value = str(side).lower()
    if side_value not in {"buy", "sell", "b", "s"}:
        raise ValueError("side must be buy or sell")
    tick = Decimal(krx_tick_size(value))
    rounding = ROUND_FLOOR if side_value in {"buy", "b"} else ROUND_CEILING
    normalized = (value / tick).to_integral_value(rounding=rounding) * tick
    if normalized <= 0:
        raise ValueError("normalized price must be positive")
    return int(normalized)


def is_valid_krx_limit_price(price: Any) -> bool:
    try:
        value = _decimal(price, "price")
        if value != value.to_integral_value():
            return False
        tick = Decimal(krx_tick_size(value))
        return value % tick == 0
    except ValueError:
        return False


def validate_krx_limit_price(price: Any) -> int:
    normalized = normalize_krx_limit_price(price, "buy")
    if not is_valid_krx_limit_price(price):
        raise ValueError(f"price is not a valid KRX tick price; expected tick={krx_tick_size(price)}")
    return normalized


@dataclass(frozen=True)
class RiskRewardCheck:
    risk_per_share: float
    reward_per_share: float
    ratio: float
    stop_distance_pct: float


def validate_risk_reward(
    entry_price: Any,
    take_profit_price: Any,
    stop_loss_price: Any,
    *,
    minimum_ratio: float = 1.5,
    minimum_stop_distance_pct: float = 0.005,
) -> RiskRewardCheck:
    entry = _decimal(entry_price, "entry_price")
    take_profit = _decimal(take_profit_price, "take_profit_price")
    stop_loss = _decimal(stop_loss_price, "stop_loss_price")
    if take_profit <= entry or stop_loss >= entry:
        raise ValueError("long trade prices must satisfy stop_loss < entry < take_profit")
    if not math.isfinite(float(minimum_ratio)) or minimum_ratio <= 0:
        raise ValueError("minimum_ratio must be positive")
    if not math.isfinite(float(minimum_stop_distance_pct)) or minimum_stop_distance_pct < 0:
        raise ValueError("minimum_stop_distance_pct must be non-negative")
    risk = entry - stop_loss
    reward = take_profit - entry
    ratio = reward / risk
    stop_distance_pct = risk / entry
    if stop_distance_pct < Decimal(str(minimum_stop_distance_pct)):
        raise ValueError("stop-loss distance is below the minimum")
    if ratio < Decimal(str(minimum_ratio)):
        raise ValueError("risk-reward ratio is below the minimum")
    return RiskRewardCheck(float(risk), float(reward), float(ratio), float(stop_distance_pct))


def is_valid_risk_reward(*args: Any, **kwargs: Any) -> bool:
    try:
        validate_risk_reward(*args, **kwargs)
    except ValueError:
        return False
    return True
