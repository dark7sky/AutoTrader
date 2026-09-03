"""Structured AI trading decisions and OpenAI-compatible client."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from enum import StrEnum
import json
import os
import random
import time
from typing import Any, Protocol
from uuid import uuid4

import requests
from pydantic import BaseModel, ConfigDict, Field, model_validator

from kis_ai_scalper.market.features import BarFeatureSnapshot
from kis_ai_scalper.broker.kis_market_rules import (
    normalize_krx_limit_price,
    validate_risk_reward,
)
from .reliable import AIBudgetExceededError, AIUsage, UsageBudget


class AIDecisionAction(StrEnum):
    HOLD = "HOLD"
    BUY = "BUY"
    SELL = "SELL"


class AIRiskLevel(StrEnum):
    LOW = "LOW"
    NORMAL = "NORMAL"
    HIGH = "HIGH"


class TradingAIDecision(BaseModel):
    model_config = ConfigDict(extra="forbid")

    decision_id: str = Field(default_factory=lambda: f"ai-decision:{uuid4().hex}")
    symbol: str = Field(pattern=r"^\d{6}$")
    action: AIDecisionAction
    strategy: str | None = Field(default=None, min_length=1, max_length=80)
    model: str | None = Field(default=None, min_length=1, max_length=120)
    prompt_version: str | None = Field(default=None, min_length=1, max_length=80)
    confidence: float = Field(ge=0, le=1)
    entry_price: float | None = Field(default=None, gt=0)
    take_profit_price: float | None = Field(default=None, gt=0)
    stop_loss_price: float | None = Field(default=None, gt=0)
    max_holding_seconds: int | None = Field(default=900, ge=30, le=7200)
    risk_level: AIRiskLevel = AIRiskLevel.NORMAL
    requires_operator_approval: bool = False
    rationale: str = Field(min_length=1, max_length=1000)
    generated_at: datetime = Field(default_factory=lambda: datetime.now().astimezone())

    @model_validator(mode="after")
    def validate_price_ladder(self) -> "TradingAIDecision":
        if self.action is AIDecisionAction.BUY:
            if (
                self.entry_price is None
                or self.take_profit_price is None
                or self.stop_loss_price is None
            ):
                raise ValueError("BUY requires entry, take_profit, and stop_loss prices")
            if not self.stop_loss_price < self.entry_price < self.take_profit_price:
                raise ValueError("BUY prices must satisfy stop_loss < entry < take_profit")
        return self

    @property
    def high_risk(self) -> bool:
        return self.risk_level is AIRiskLevel.HIGH or self.requires_operator_approval


@dataclass(frozen=True)
class AIDecisionContext:
    symbol: str
    features: dict[str, Any]
    candidates: list[dict[str, Any]]
    latest_price: float
    open_position: dict[str, Any] | None = None
    market_snapshot_at: datetime | None = None
    trade_profile: str = "normal"


class TradingAIClient(Protocol):
    def decide(self, context: AIDecisionContext) -> TradingAIDecision:
        ...


class RuleBasedAIClient:
    """Deterministic local stand-in for unit tests and API-key-free dry runs."""

    def __init__(self, buy_threshold: float = 0.75) -> None:
        if not 0 <= buy_threshold <= 1:
            raise ValueError("buy_threshold must be between 0 and 1")
        self.buy_threshold = buy_threshold

    def decide(self, context: AIDecisionContext) -> TradingAIDecision:
        best = max(
            context.candidates,
            key=lambda item: float(item.get("score", 0)),
            default=None,
        )
        score = float(best.get("score", 0)) if best is not None else 0.0
        if context.open_position is not None:
            return TradingAIDecision(
                symbol=context.symbol,
                action=AIDecisionAction.HOLD,
                strategy=None,
                model="rule",
                prompt_version=AI_DECISION_PROMPT_VERSION,
                confidence=0.6,
                risk_level=AIRiskLevel.LOW,
                rationale="Existing position is managed by deterministic exits.",
            )
        if score >= self.buy_threshold:
            entry = context.latest_price
            return TradingAIDecision(
                symbol=context.symbol,
                action=AIDecisionAction.BUY,
                strategy=str(best.get("strategy") or "UNKNOWN"),
                model="rule",
                prompt_version=AI_DECISION_PROMPT_VERSION,
                confidence=min(0.95, score),
                entry_price=entry,
                stop_loss_price=round(entry * 0.992),
                take_profit_price=round(entry * 1.015),
                max_holding_seconds=900,
                risk_level=AIRiskLevel.NORMAL,
                requires_operator_approval=False,
                rationale="Rule-based candidate score passed the entry threshold.",
            )
        return TradingAIDecision(
            symbol=context.symbol,
            action=AIDecisionAction.HOLD,
            strategy=None,
            model="rule",
            prompt_version=AI_DECISION_PROMPT_VERSION,
            confidence=0.5,
            risk_level=AIRiskLevel.LOW,
            rationale="No candidate passed the deterministic threshold.",
        )


class AIDecisionError(RuntimeError):
    """Base class for a decision request that cannot be used safely."""


class AIDecisionTransportError(AIDecisionError):
    """A timeout or retryable HTTP failure exhausted its bounded retries."""


class AIDecisionRequestError(AIDecisionError):
    """A non-retryable HTTP request failure."""


class AIDecisionResponseError(AIDecisionError):
    """The response was not a valid decision for the requested symbol."""


class OpenAITradingDecisionClient:
    """Reliable requests-based structured-output decision client.

    Only transient HTTP failures (429/5xx) and request timeouts are retried.
    This class never submits broker orders, so its retry policy cannot repeat an
    order. The injected ``UsageBudget`` is shared safely by concurrent callers.
    """

    def __init__(
        self,
        api_key: str,
        *,
        model: str | None = None,
        session: Any | None = None,
        timeout: float = 20.0,
        max_retries: int = 2,
        retry_base_delay: float = 0.25,
        retry_max_delay: float = 2.0,
        sleep: Any = time.sleep,
        random_fn: Any = random.random,
        clock: Any = lambda: datetime.now(timezone.utc),
        max_snapshot_age_seconds: float | None = 90.0,
        require_snapshot_timestamp: bool = False,
        budget: UsageBudget | None = None,
        estimated_cost_per_call_usd: float = 0.01,
        input_cost_per_million_tokens: float | None = None,
        output_cost_per_million_tokens: float | None = None,
        on_budget_exceeded: str = "hold",
    ) -> None:
        if not api_key:
            raise ValueError("OPENAI_API_KEY is required")
        if timeout <= 0:
            raise ValueError("timeout must be positive")
        if max_retries < 0:
            raise ValueError("max_retries must be non-negative")
        if retry_base_delay < 0 or retry_max_delay < 0:
            raise ValueError("retry delays must be non-negative")
        if retry_max_delay < retry_base_delay:
            raise ValueError("retry_max_delay must be >= retry_base_delay")
        if max_snapshot_age_seconds is not None and max_snapshot_age_seconds < 0:
            raise ValueError("max_snapshot_age_seconds must be non-negative")
        if on_budget_exceeded not in {"hold", "raise"}:
            raise ValueError("on_budget_exceeded must be 'hold' or 'raise'")
        self.api_key = api_key
        self.model = model or os.getenv("OPENAI_MODEL") or "gpt-4o-mini"
        self.session = session or requests.Session()
        self.timeout = timeout
        self.max_retries = max_retries
        self.retry_base_delay = retry_base_delay
        self.retry_max_delay = retry_max_delay
        self.sleep = sleep
        self.random_fn = random_fn
        self.clock = clock
        self.max_snapshot_age_seconds = max_snapshot_age_seconds
        self.require_snapshot_timestamp = require_snapshot_timestamp
        self.budget = budget or UsageBudget.from_env(clock=clock)
        self.estimated_cost_per_call_usd = float(estimated_cost_per_call_usd)
        if self.estimated_cost_per_call_usd < 0:
            raise ValueError("estimated_cost_per_call_usd must be non-negative")
        default_input, default_output = _model_cost_rates(self.model)
        self.input_cost_per_million_tokens = (
            default_input if input_cost_per_million_tokens is None else float(input_cost_per_million_tokens)
        )
        self.output_cost_per_million_tokens = (
            default_output if output_cost_per_million_tokens is None else float(output_cost_per_million_tokens)
        )
        if self.input_cost_per_million_tokens < 0 or self.output_cost_per_million_tokens < 0:
            raise ValueError("token costs must be non-negative")
        self.on_budget_exceeded = on_budget_exceeded
        self.last_usage = AIUsage()
        self.total_usage = AIUsage()

    def decide(self, context: AIDecisionContext) -> TradingAIDecision:
        self._validate_snapshot(context)
        for attempt in range(self.max_retries + 1):
            reservation = None
            try:
                reservation = self.budget.reserve_call(self.estimated_cost_per_call_usd)
                response = self.session.post(
                    "https://api.openai.com/v1/chat/completions",
                    headers={
                        "authorization": f"Bearer {self.api_key}",
                        "content-type": "application/json",
                    },
                    json={
                        "model": self.model,
                        "messages": [
                            {"role": "system", "content": _SYSTEM_PROMPT},
                            {"role": "user", "content": json.dumps(_context_payload(context), sort_keys=True)},
                        ],
                        "response_format": {
                            "type": "json_schema",
                            "json_schema": {
                                "name": "trading_ai_decision",
                                "strict": True,
                                "schema": trading_ai_decision_schema(),
                            },
                        },
                        "temperature": 0,
                    },
                    timeout=self.timeout,
                )
                status_code = getattr(response, "status_code", None)
                if status_code == 429 or (status_code is not None and 500 <= status_code <= 599):
                    raise _RetryableHTTPError(f"OpenAI transient HTTP status {status_code}")
                if status_code is not None and 400 <= status_code <= 499:
                    self.budget.cancel(reservation)
                    raise AIDecisionRequestError(f"OpenAI request rejected with HTTP {status_code}")
                response.raise_for_status()
                payload = response.json()
                usage = self._extract_usage(payload)
                self.last_usage = usage
                self.total_usage = AIUsage(
                    self.total_usage.prompt_tokens + usage.prompt_tokens,
                    self.total_usage.completion_tokens + usage.completion_tokens,
                    self.total_usage.total_tokens + usage.total_tokens,
                    self.total_usage.estimated_cost_usd + usage.estimated_cost_usd,
                )
                self.budget.settle(reservation, usage.estimated_cost_usd)
                reservation = None
                content = payload["choices"][0]["message"]["content"]
                if isinstance(content, dict):
                    decision = TradingAIDecision.model_validate(content)
                else:
                    decision = TradingAIDecision.model_validate_json(content)
                decision = decision.model_copy(
                    update={
                        "model": decision.model or self.model,
                        "prompt_version": decision.prompt_version or AI_DECISION_PROMPT_VERSION,
                    }
                )
                decision = _normalize_buy_risk_plan(decision)
                if decision.symbol != context.symbol:
                    raise AIDecisionResponseError(
                        f"OpenAI response symbol mismatch: expected {context.symbol}, got {decision.symbol}"
                    )
                return decision
            except AIBudgetExceededError as exc:
                if reservation is not None:
                    self.budget.cancel(reservation)
                return self._budget_fallback(context, exc)
            except (requests.exceptions.Timeout, _RetryableHTTPError) as exc:
                if reservation is not None:
                    self.budget.cancel(reservation)
                if attempt >= self.max_retries:
                    raise AIDecisionTransportError(str(exc)) from exc
                delay = min(self.retry_max_delay, self.retry_base_delay * (2 ** attempt))
                self.sleep(delay * (0.5 + self.random_fn()))
            except (KeyError, TypeError, ValueError) as exc:
                if reservation is not None:
                    self.budget.cancel(reservation)
                raise AIDecisionResponseError("OpenAI response did not match the decision schema") from exc
            except Exception:
                if reservation is not None:
                    self.budget.cancel(reservation)
                raise

        raise AssertionError("bounded retry loop did not return")

    def _extract_usage(self, payload: dict[str, Any]) -> AIUsage:
        raw = payload.get("usage") or {}
        prompt_tokens = int(raw.get("prompt_tokens", raw.get("input_tokens", 0)) or 0)
        completion_tokens = int(raw.get("completion_tokens", raw.get("output_tokens", 0)) or 0)
        total_tokens = int(raw.get("total_tokens", prompt_tokens + completion_tokens) or 0)
        cost = (
            prompt_tokens * self.input_cost_per_million_tokens
            + completion_tokens * self.output_cost_per_million_tokens
        ) / 1_000_000
        return AIUsage(prompt_tokens, completion_tokens, total_tokens, cost)

    def _validate_snapshot(self, context: AIDecisionContext) -> None:
        snapshot_at = context.market_snapshot_at or _snapshot_timestamp(context.features)
        if snapshot_at is None:
            if self.require_snapshot_timestamp:
                raise AIDecisionResponseError("market snapshot timestamp is required")
            return
        if isinstance(snapshot_at, str):
            try:
                snapshot_at = datetime.fromisoformat(snapshot_at)
            except ValueError as exc:
                raise AIDecisionResponseError("market snapshot timestamp is invalid") from exc
        snapshot_at = _aware(snapshot_at)
        age = (self.clock() - snapshot_at).total_seconds()
        if age < -5:
            raise AIDecisionResponseError("market snapshot timestamp is in the future")
        if self.max_snapshot_age_seconds is not None and age > self.max_snapshot_age_seconds:
            raise AIDecisionResponseError("market snapshot is stale")

    def _budget_fallback(
        self, context: AIDecisionContext, error: AIBudgetExceededError
    ) -> TradingAIDecision:
        if self.on_budget_exceeded == "raise":
            raise error
        return TradingAIDecision(
            symbol=context.symbol,
            action=AIDecisionAction.HOLD,
            confidence=0.0,
            risk_level=AIRiskLevel.HIGH,
            requires_operator_approval=True,
            rationale=f"AI call blocked by safety budget: {error}",
        )


def context_from_snapshot(
    snapshot: BarFeatureSnapshot,
    candidates: list[Any],
    *,
    open_position: dict[str, Any] | None = None,
    market_snapshot_at: datetime | None = None,
    trade_profile: str = "normal",
) -> AIDecisionContext:
    return AIDecisionContext(
        symbol=snapshot.symbol,
        features=snapshot.as_dict(),
        candidates=[getattr(candidate, "__dict__", dict(candidate)) for candidate in candidates],
        latest_price=snapshot.latest_close,
        open_position=open_position,
        market_snapshot_at=market_snapshot_at,
        trade_profile=trade_profile,
    )


def _context_payload(context: AIDecisionContext) -> dict[str, Any]:
    payload = {
        "symbol": context.symbol,
        "features": context.features,
        "candidates": context.candidates,
        "latest_price": context.latest_price,
        "open_position": context.open_position,
        "trade_profile": context.trade_profile,
    }
    if context.market_snapshot_at is not None:
        payload["market_snapshot_at"] = _aware(context.market_snapshot_at).isoformat()
    return payload


def _snapshot_timestamp(features: dict[str, Any]) -> datetime | str | None:
    for key in (
        "market_snapshot_at",
        "snapshot_at",
        "timestamp",
        "bar_timestamp",
        "bar_start",
        "latest_bar_timestamp",
    ):
        value = features.get(key)
        if value is not None:
            return value
    return None


def _aware(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _model_cost_rates(model: str) -> tuple[float, float]:
    """Return configurable estimates; unknown models use a conservative default."""
    rates = {
        "gpt-4o-mini": (0.15, 0.60),
        "gpt-4o": (2.50, 10.00),
    }
    return rates.get(model.lower(), (0.15, 0.60))


def _normalize_buy_risk_plan(decision: TradingAIDecision) -> TradingAIDecision:
    if decision.action is not AIDecisionAction.BUY:
        return decision
    assert decision.entry_price is not None
    entry = normalize_krx_limit_price(decision.entry_price, "buy")
    stop = normalize_krx_limit_price(entry * 0.992, "buy")
    risk_per_share = entry - stop
    take = normalize_krx_limit_price(entry + risk_per_share * 1.6, "sell")
    validate_risk_reward(entry, take, stop)
    return decision.model_copy(
        update={
            "entry_price": float(entry),
            "stop_loss_price": float(stop),
            "take_profit_price": float(take),
        }
    )


class _RetryableHTTPError(RuntimeError):
    pass


def trading_ai_decision_schema() -> dict[str, Any]:
    return {
        "type": "object",
        "additionalProperties": False,
        "properties": {
            "symbol": {"type": "string", "pattern": "^\\d{6}$"},
            "action": {"type": "string", "enum": [item.value for item in AIDecisionAction]},
            "strategy": {"type": ["string", "null"], "minLength": 1, "maxLength": 80},
            "confidence": {"type": "number", "minimum": 0, "maximum": 1},
            "entry_price": {"type": ["number", "null"], "exclusiveMinimum": 0},
            "take_profit_price": {"type": ["number", "null"], "exclusiveMinimum": 0},
            "stop_loss_price": {"type": ["number", "null"], "exclusiveMinimum": 0},
            "max_holding_seconds": {"type": ["integer", "null"], "minimum": 30, "maximum": 7200},
            "risk_level": {"type": "string", "enum": [item.value for item in AIRiskLevel]},
            "requires_operator_approval": {"type": "boolean"},
            "rationale": {"type": "string", "minLength": 1, "maxLength": 1000},
        },
        "required": [
            "symbol",
            "action",
            "strategy",
            "confidence",
            "entry_price",
            "take_profit_price",
            "stop_loss_price",
            "max_holding_seconds",
            "risk_level",
            "requires_operator_approval",
            "rationale",
        ],
    }


AI_DECISION_PROMPT_VERSION = "trade-decision-v4"


_SYSTEM_PROMPT = (
    "You are an intraday Korean equity trading decision engine. "
    "Return only the requested JSON schema. The candidates already passed deterministic "
    "trend, VWAP, price-position, and volume filters. Use trade_profile to calibrate entry: "
    "for aggressive, accept a coherent candidate and do not require an exceptional setup; "
    "use HOLD only for a concrete contradiction, stale/ambiguous data, or unsafe risk plan. "
    "For BUY, set strategy to one of the provided deterministic candidate strategy values. "
    "For BUY, provide a limit entry near the latest price, take-profit above entry, "
    "and stop-loss at least 0.5% below entry. The take-profit reward must be "
    "at least 1.5 times the per-share risk after KRX tick-size rounding, so leave "
    "a conservative rounding buffer. Use a short max holding time. Mark HIGH risk or "
    "requires_operator_approval when volatility, weak confidence, or ambiguous data "
    "makes automatic entry inappropriate. Never ask questions."
)
