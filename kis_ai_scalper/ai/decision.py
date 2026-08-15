"""Structured AI trading decisions and OpenAI-compatible client."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
import json
from typing import Any, Protocol
from uuid import uuid4

import requests
from pydantic import BaseModel, ConfigDict, Field, model_validator

from kis_ai_scalper.market.features import BarFeatureSnapshot


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


class TradingAIClient(Protocol):
    def decide(self, context: AIDecisionContext) -> TradingAIDecision:
        ...


class RuleBasedAIClient:
    """Deterministic local stand-in for unit tests and API-key-free dry runs."""

    def decide(self, context: AIDecisionContext) -> TradingAIDecision:
        score = max((float(item.get("score", 0)) for item in context.candidates), default=0.0)
        if context.open_position is not None:
            return TradingAIDecision(
                symbol=context.symbol,
                action=AIDecisionAction.HOLD,
                confidence=0.6,
                risk_level=AIRiskLevel.LOW,
                rationale="Existing position is managed by deterministic exits.",
            )
        if score >= 0.75:
            entry = context.latest_price
            return TradingAIDecision(
                symbol=context.symbol,
                action=AIDecisionAction.BUY,
                confidence=min(0.95, score),
                entry_price=entry,
                stop_loss_price=round(entry * 0.99),
                take_profit_price=round(entry * 1.015),
                max_holding_seconds=900,
                risk_level=AIRiskLevel.NORMAL,
                requires_operator_approval=False,
                rationale="Rule-based candidate score passed the entry threshold.",
            )
        return TradingAIDecision(
            symbol=context.symbol,
            action=AIDecisionAction.HOLD,
            confidence=0.5,
            risk_level=AIRiskLevel.LOW,
            rationale="No candidate passed the deterministic threshold.",
        )


class OpenAITradingDecisionClient:
    """Minimal requests-based OpenAI Chat Completions structured-output client."""

    def __init__(
        self,
        api_key: str,
        *,
        model: str = "gpt-4o-mini",
        session: Any | None = None,
        timeout: float = 20.0,
    ) -> None:
        if not api_key:
            raise ValueError("OPENAI_API_KEY is required")
        self.api_key = api_key
        self.model = model
        self.session = session or requests.Session()
        self.timeout = timeout

    def decide(self, context: AIDecisionContext) -> TradingAIDecision:
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
                    {"role": "user", "content": json.dumps(context.__dict__, sort_keys=True)},
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
        response.raise_for_status()
        payload = response.json()
        content = payload["choices"][0]["message"]["content"]
        return TradingAIDecision.model_validate_json(content)


def context_from_snapshot(
    snapshot: BarFeatureSnapshot,
    candidates: list[Any],
    *,
    open_position: dict[str, Any] | None = None,
) -> AIDecisionContext:
    return AIDecisionContext(
        symbol=snapshot.symbol,
        features=snapshot.as_dict(),
        candidates=[getattr(candidate, "__dict__", dict(candidate)) for candidate in candidates],
        latest_price=snapshot.latest_close,
        open_position=open_position,
    )


def trading_ai_decision_schema() -> dict[str, Any]:
    return {
        "type": "object",
        "additionalProperties": False,
        "properties": {
            "symbol": {"type": "string", "pattern": "^\\d{6}$"},
            "action": {"type": "string", "enum": [item.value for item in AIDecisionAction]},
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


_SYSTEM_PROMPT = (
    "You are an intraday Korean equity trading decision engine. "
    "Return only the requested JSON schema. Prefer HOLD unless the setup is clear. "
    "For BUY, provide a limit entry near the latest price, take-profit above entry, "
    "stop-loss below entry, and a short max holding time. Mark HIGH risk or "
    "requires_operator_approval when volatility, weak confidence, or ambiguous data "
    "makes automatic entry inappropriate. Never ask questions."
)
