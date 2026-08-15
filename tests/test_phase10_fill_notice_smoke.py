import asyncio
import json
import inspect

import pytest

from kis_ai_scalper.broker.kis_endpoints import KisEnvironment
from kis_ai_scalper.broker.kis_fill_notice import (
    FILL_NOTICE_DEMO_TR_ID,
    smoke_fill_notice,
)


def ack(*, tr_id=FILL_NOTICE_DEMO_TR_ID, tr_key="HTS-TEST", rt_cd="0"):
    body = {"rt_cd": rt_cd, "msg1": "SUBSCRIBE SUCCESS" if rt_cd == "0" else "rejected"}
    if rt_cd == "0":
        body["output"] = {"key": "a" * 32, "iv": "b" * 16}
    return json.dumps({"header": {"tr_id": tr_id, "tr_key": tr_key}, "body": body})


class FakeSocket:
    def __init__(self, messages):
        self.messages = list(messages)
        self.sent = []

    async def send(self, message):
        self.sent.append(message)

    async def recv(self):
        item = self.messages.pop(0)
        if isinstance(item, BaseException):
            raise item
        return item


class SocketContext:
    def __init__(self, socket):
        self.socket = socket

    async def __aenter__(self):
        return self.socket

    async def __aexit__(self, exc_type, exc, tb):
        return False


def run(messages, *, seconds=1, clock=None):
    socket = FakeSocket(messages)
    result = asyncio.run(
        smoke_fill_notice(
            "ws://test.invalid",
            "approval-secret",
            "HTS-TEST",
            KisEnvironment.DEMO,
            seconds,
            socket_factory=lambda _: SocketContext(socket),
            clock=clock,
        )
    )
    return result, socket


def advancing_clock():
    value = 0.0

    def now():
        nonlocal value
        value += 0.25
        return value

    return now


def test_acknowledges_read_only_subscription_and_sanitizes_result():
    result, socket = run([ack(), asyncio.TimeoutError()], clock=advancing_clock())

    assert result.acknowledged is True
    assert result.message == "SUBSCRIBE SUCCESS"
    assert result.event_count == 0
    assert len(socket.sent) == 1
    sent = json.loads(socket.sent[0])
    assert sent["body"]["input"]["tr_id"] == FILL_NOTICE_DEMO_TR_ID
    assert sent["body"]["input"]["tr_key"] == "HTS-TEST"


def test_answers_pingpong_and_continues_until_timeout():
    ping = json.dumps({"header": {"tr_id": "PINGPONG"}})
    result, socket = run([ping, ack(), asyncio.TimeoutError()], clock=advancing_clock())

    assert result.acknowledged is True
    assert any(json.loads(item).get("header", {}).get("tr_id") == "PINGPONG" for item in socket.sent[1:])


def test_timeout_returns_unacknowledged_without_leaking_credentials():
    result, socket = run([asyncio.TimeoutError()], clock=advancing_clock())

    assert result.acknowledged is False
    assert result.message is None
    assert result.event_count == 0
    assert "approval-secret" not in repr(result)
    assert "HTS-TEST" not in repr(result)


def test_wrong_ack_is_ignored_and_does_not_count_events():
    result, _ = run([ack(tr_id="H0STCNI0", tr_key="OTHER-HTS"), asyncio.TimeoutError()], clock=advancing_clock())

    assert result.acknowledged is False
    assert result.event_count == 0


def test_bounded_seconds_and_no_order_or_account_api_surface():
    for seconds in (0, 61, True):
        with pytest.raises(ValueError, match="between 1 and 60"):
            asyncio.run(smoke_fill_notice("ws://test.invalid", "a", "h", "demo", seconds))

    source = inspect.getsource(smoke_fill_notice)
    assert "KisOrderClient" not in source
    assert "KisAccountClient" not in source
    assert "apply_fill_notice" not in source
