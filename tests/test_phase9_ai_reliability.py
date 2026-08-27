from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone

import pytest
import requests

from kis_ai_scalper.ai.decision import (
    AIDecisionRequestError,
    AIDecisionResponseError,
    AIDecisionAction,
    AIDecisionContext,
    AIDecisionTransportError,
    OpenAITradingDecisionClient,
)
from kis_ai_scalper.ai.reliable import UsageBudget
from kis_ai_scalper.broker.kis_market_rules import (
    normalize_krx_limit_price,
    validate_risk_reward,
)


GOOD_CONTENT = (
    '{"symbol":"005930","action":"HOLD","confidence":0.55,'
    '"entry_price":null,"take_profit_price":null,"stop_loss_price":null,'
    '"max_holding_seconds":null,"risk_level":"LOW",'
    '"requires_operator_approval":false,"rationale":"No clear setup."}'
)


class FakeResponse:
    def __init__(self, payload=None, status_code=200):
        self.payload = payload or {
            "choices": [{"message": {"content": GOOD_CONTENT}}],
            "usage": {"prompt_tokens": 100, "completion_tokens": 20, "total_tokens": 120},
        }
        self.status_code = status_code

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError(f"HTTP {self.status_code}")

    def json(self):
        return self.payload


class FakeSession:
    def __init__(self, actions):
        self.actions = list(actions)
        self.posts = []

    def post(self, url, **kwargs):
        self.posts.append((url, kwargs))
        action = self.actions.pop(0)
        if isinstance(action, BaseException):
            raise action
        return action


def context(**kwargs):
    return AIDecisionContext("005930", {}, [], 100_000, **kwargs)


def test_model_timeout_and_usage_are_injected_and_extracted(monkeypatch):
    monkeypatch.setenv("OPENAI_MODEL", "gpt-test-model")
    session = FakeSession([FakeResponse()])
    client = OpenAITradingDecisionClient("key", session=session, timeout=3.5)

    decision = client.decide(context())

    assert decision.action is AIDecisionAction.HOLD
    assert session.posts[0][1]["timeout"] == 3.5
    assert session.posts[0][1]["json"]["model"] == "gpt-test-model"
    assert client.last_usage.prompt_tokens == 100
    assert client.last_usage.completion_tokens == 20
    assert client.last_usage.total_tokens == 120
    assert client.total_usage.total_tokens == 120


def test_prompt_requires_executable_risk_reward_prices():
    session = FakeSession([FakeResponse()])
    client = OpenAITradingDecisionClient("key", session=session)

    decision = client.decide(context())

    system_prompt = session.posts[0][1]["json"]["messages"][0]["content"]
    assert "at least 0.5% below entry" in system_prompt
    assert "at least 1.5 times the per-share risk" in system_prompt
    assert decision.prompt_version == "trade-decision-v3"


def test_buy_response_prices_are_normalized_to_executable_risk_plan():
    content = (
        '{"symbol":"005930","action":"BUY","strategy":"BREAKOUT_WATCH",'
        '"confidence":0.8,"entry_price":267000,"take_profit_price":268500,'
        '"stop_loss_price":265500,"max_holding_seconds":900,'
        '"risk_level":"NORMAL","requires_operator_approval":false,'
        '"rationale":"clear breakout"}'
    )
    response = FakeResponse({"choices": [{"message": {"content": content}}]})
    client = OpenAITradingDecisionClient(
        "key",
        session=FakeSession([response]),
    )

    decision = client.decide(context())

    entry = normalize_krx_limit_price(decision.entry_price, "buy")
    stop = normalize_krx_limit_price(decision.stop_loss_price, "buy")
    take = normalize_krx_limit_price(decision.take_profit_price, "sell")
    check = validate_risk_reward(entry, take, stop)
    assert check.stop_distance_pct >= 0.005
    assert check.ratio >= 1.5


def test_only_transient_failures_are_retried_with_bounded_backoff():
    session = FakeSession([
        FakeResponse(status_code=429),
        requests.exceptions.Timeout("slow"),
        FakeResponse(),
    ])
    sleeps = []
    client = OpenAITradingDecisionClient(
        "key", session=session, max_retries=2, retry_base_delay=1, retry_max_delay=1,
        sleep=sleeps.append, random_fn=lambda: 0,
    )

    client.decide(context())

    assert len(session.posts) == 3
    assert sleeps == [0.5, 0.5]


def test_retryable_failure_exhaustion_is_a_clear_transport_error():
    session = FakeSession([requests.exceptions.Timeout("slow") for _ in range(3)])
    client = OpenAITradingDecisionClient("key", session=session, sleep=lambda _: None)

    with pytest.raises(AIDecisionTransportError):
        client.decide(context())
    assert len(session.posts) == 3


def test_http_4xx_is_not_retried():
    session = FakeSession([FakeResponse(status_code=400)])
    client = OpenAITradingDecisionClient("key", session=session)

    with pytest.raises(AIDecisionRequestError):
        client.decide(context())
    assert len(session.posts) == 1


def test_schema_error_is_not_retried():
    response = FakeResponse({"choices": [{"message": {"content": "{}"}}]})
    session = FakeSession([response])
    client = OpenAITradingDecisionClient("key", session=session)

    with pytest.raises(AIDecisionResponseError):
        client.decide(context())
    assert len(session.posts) == 1


def test_symbol_mismatch_is_rejected_without_retry():
    response = FakeResponse({
        "choices": [{"message": {"content": GOOD_CONTENT.replace("005930", "000660")}}],
    })
    session = FakeSession([response])
    client = OpenAITradingDecisionClient("key", session=session)

    with pytest.raises(AIDecisionResponseError, match="symbol mismatch"):
        client.decide(context())
    assert len(session.posts) == 1


def test_stale_market_snapshot_is_rejected_before_http_call():
    now = datetime(2026, 8, 18, 10, 0, tzinfo=timezone.utc)
    session = FakeSession([FakeResponse()])
    client = OpenAITradingDecisionClient(
        "key", session=session, clock=lambda: now, max_snapshot_age_seconds=30,
    )

    with pytest.raises(AIDecisionResponseError, match="stale"):
        client.decide(context(market_snapshot_at=now - timedelta(seconds=31)))
    assert session.posts == []


def test_budget_excess_fails_closed_to_hold():
    budget = UsageBudget(
        max_process_calls=1, max_daily_calls=10,
        max_process_cost_usd=0.000001, max_daily_cost_usd=1,
    )
    session = FakeSession([FakeResponse()])
    client = OpenAITradingDecisionClient("key", session=session, budget=budget)

    decision = client.decide(context())

    assert decision.action is AIDecisionAction.HOLD
    assert decision.requires_operator_approval is True
    assert "budget" in decision.rationale


def test_usage_budget_is_thread_safe_for_concurrent_reservations():
    budget = UsageBudget(
        max_process_calls=1, max_daily_calls=1,
        max_process_cost_usd=1, max_daily_cost_usd=1,
    )

    def reserve():
        try:
            reservation = budget.reserve_call(0.01)
        except RuntimeError:
            return "blocked"
        budget.cancel(reservation)
        return "reserved"

    with ThreadPoolExecutor(max_workers=8) as executor:
        results = list(executor.map(lambda _: reserve(), range(8)))

    assert results.count("reserved") == 1
