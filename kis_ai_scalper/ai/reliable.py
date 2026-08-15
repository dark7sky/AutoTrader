"""Thread-safe reliability primitives for AI decision calls."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timezone
import os
from threading import Lock
from typing import Callable
from uuid import uuid4


class AIBudgetExceededError(RuntimeError):
    """Raised when an AI request would exceed a configured safety budget."""


@dataclass(frozen=True)
class AIUsage:
    """Usage returned by one OpenAI response."""

    prompt_tokens: int = 0
    completion_tokens: int = 0
    total_tokens: int = 0
    estimated_cost_usd: float = 0.0


@dataclass(frozen=True)
class UsageBudgetSnapshot:
    process_calls: int
    daily_calls: int
    process_cost_usd: float
    daily_cost_usd: float
    process_reserved_cost_usd: float
    daily_reserved_cost_usd: float


@dataclass(frozen=True)
class _Reservation:
    token: str
    day: date
    estimated_cost_usd: float


class UsageBudget:
    """Atomically limits request attempts and estimated spend.

    A reservation is made before every HTTP attempt, including retries. This
    prevents concurrent workers from passing the same limit simultaneously.
    """

    def __init__(
        self,
        *,
        max_process_calls: int = 500,
        max_daily_calls: int = 1000,
        max_process_cost_usd: float = 10.0,
        max_daily_cost_usd: float = 25.0,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self.max_process_calls = _non_negative_int(max_process_calls, "max_process_calls")
        self.max_daily_calls = _non_negative_int(max_daily_calls, "max_daily_calls")
        self.max_process_cost_usd = _non_negative_float(
            max_process_cost_usd, "max_process_cost_usd"
        )
        self.max_daily_cost_usd = _non_negative_float(
            max_daily_cost_usd, "max_daily_cost_usd"
        )
        self._clock = clock or (lambda: datetime.now(timezone.utc))
        self._lock = Lock()
        self._day = self._today()
        self._process_calls = 0
        self._daily_calls = 0
        self._process_cost_usd = 0.0
        self._daily_cost_usd = 0.0
        self._process_reserved_cost_usd = 0.0
        self._daily_reserved_cost_usd = 0.0
        self._reservations: dict[str, _Reservation] = {}

    @classmethod
    def from_env(cls, *, clock: Callable[[], datetime] | None = None) -> "UsageBudget":
        """Build a budget from optional environment overrides."""

        return cls(
            max_process_calls=_env_int("OPENAI_MAX_PROCESS_CALLS", 500),
            max_daily_calls=_env_int("OPENAI_MAX_DAILY_CALLS", 1000),
            max_process_cost_usd=_env_float("OPENAI_MAX_PROCESS_COST_USD", 10.0),
            max_daily_cost_usd=_env_float("OPENAI_MAX_DAILY_COST_USD", 25.0),
            clock=clock,
        )

    def reserve_call(self, estimated_cost_usd: float) -> _Reservation:
        estimated_cost_usd = _non_negative_float(estimated_cost_usd, "estimated_cost_usd")
        with self._lock:
            self._roll_day_locked()
            if self._process_calls >= self.max_process_calls:
                raise AIBudgetExceededError("process AI call limit exceeded")
            if self._daily_calls >= self.max_daily_calls:
                raise AIBudgetExceededError("daily AI call limit exceeded")
            if self._process_cost_usd + self._process_reserved_cost_usd + estimated_cost_usd > self.max_process_cost_usd:
                raise AIBudgetExceededError("process AI cost limit exceeded")
            if self._daily_cost_usd + self._daily_reserved_cost_usd + estimated_cost_usd > self.max_daily_cost_usd:
                raise AIBudgetExceededError("daily AI cost limit exceeded")

            reservation = _Reservation(uuid4().hex, self._day, estimated_cost_usd)
            self._reservations[reservation.token] = reservation
            self._process_calls += 1
            self._daily_calls += 1
            self._process_reserved_cost_usd += estimated_cost_usd
            self._daily_reserved_cost_usd += estimated_cost_usd
            return reservation

    def settle(self, reservation: _Reservation, actual_cost_usd: float) -> None:
        actual_cost_usd = _non_negative_float(actual_cost_usd, "actual_cost_usd")
        with self._lock:
            current = self._reservations.pop(reservation.token, None)
            if current is None:
                return
            self._process_reserved_cost_usd -= current.estimated_cost_usd
            if current.day == self._day:
                self._daily_reserved_cost_usd -= current.estimated_cost_usd
            self._process_cost_usd += actual_cost_usd
            if current.day == self._day:
                self._daily_cost_usd += actual_cost_usd
            if self._process_cost_usd > self.max_process_cost_usd:
                raise AIBudgetExceededError("process AI cost limit exceeded")
            if self._daily_cost_usd > self.max_daily_cost_usd:
                raise AIBudgetExceededError("daily AI cost limit exceeded")

    def cancel(self, reservation: _Reservation) -> None:
        with self._lock:
            current = self._reservations.pop(reservation.token, None)
            if current is None:
                return
            self._process_reserved_cost_usd -= current.estimated_cost_usd
            if current.day == self._day:
                self._daily_reserved_cost_usd -= current.estimated_cost_usd

    def snapshot(self) -> UsageBudgetSnapshot:
        with self._lock:
            self._roll_day_locked()
            return UsageBudgetSnapshot(
                self._process_calls,
                self._daily_calls,
                self._process_cost_usd,
                self._daily_cost_usd,
                self._process_reserved_cost_usd,
                self._daily_reserved_cost_usd,
            )

    def _today(self) -> date:
        # The injected clock defines the application's reporting day.
        return self._clock().date()

    def _roll_day_locked(self) -> None:
        today = self._today()
        if today == self._day:
            return
        self._day = today
        self._daily_calls = 0
        self._daily_cost_usd = 0.0
        self._daily_reserved_cost_usd = sum(
            value.estimated_cost_usd
            for value in self._reservations.values()
            if value.day == today
        )


def _non_negative_int(value: int, name: str) -> int:
    value = int(value)
    if value < 0:
        raise ValueError(f"{name} must be non-negative")
    return value


def _non_negative_float(value: float, name: str) -> float:
    value = float(value)
    if value < 0:
        raise ValueError(f"{name} must be non-negative")
    return value


def _env_int(name: str, default: int) -> int:
    value = os.getenv(name)
    return default if value in (None, "") else int(value)


def _env_float(name: str, default: float) -> float:
    value = os.getenv(name)
    return default if value in (None, "") else float(value)
