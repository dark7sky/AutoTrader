from datetime import datetime
from enum import StrEnum

from pydantic import BaseModel, ConfigDict, Field


class TradingMode(StrEnum):
    BACKTEST = "backtest"
    REPLAY = "replay"
    SHADOW = "shadow"
    PAPER = "paper"
    MICRO_LIVE = "micro_live"
    LIVE = "live"


class AIAction(StrEnum):
    WAIT = "WAIT"
    ARM_LONG = "ARM_LONG"
    ENTER_LONG = "ENTER_LONG"
    HOLD = "HOLD"
    PARTIAL_EXIT = "PARTIAL_EXIT"
    EXIT = "EXIT"
    CANCEL = "CANCEL"


class MarketSnapshot(BaseModel):
    model_config = ConfigDict(extra="forbid")

    symbol: str = Field(min_length=1)
    timestamp: datetime
    last_price: float = Field(gt=0)
    volume: int = Field(ge=0)


class TradeDecision(BaseModel):
    model_config = ConfigDict(extra="forbid")

    action: AIAction
    symbol: str = Field(min_length=1)
    confidence: float = Field(ge=0, le=1)
    rationale: str = Field(min_length=1, max_length=2000)
    generated_at: datetime
