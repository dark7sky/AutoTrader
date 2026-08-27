from kis_ai_scalper.broker.kis_endpoints import KisEnvironment
from kis_ai_scalper.broker.kis_rate_limit import (
    KisRateLimitedSession,
    KisRateLimiter,
)


class FakeClock:
    def __init__(self):
        self.now = 0.0

    def __call__(self):
        return self.now


class FakeSession:
    def __init__(self, clock):
        self.clock = clock
        self.calls = []

    def get(self, url, **kwargs):
        self.calls.append(("GET", url, self.clock()))
        return object()

    def post(self, url, **kwargs):
        self.calls.append(("POST", url, self.clock()))
        return object()


def test_demo_rest_calls_share_half_second_minimum_interval():
    clock = FakeClock()
    sleeps = []

    def sleep(seconds):
        sleeps.append(seconds)
        clock.now += seconds

    limiter = KisRateLimiter(clock=clock, sleeper=sleep)
    first_raw = FakeSession(clock)
    second_raw = FakeSession(clock)
    first = KisRateLimitedSession(KisEnvironment.DEMO, first_raw, limiter=limiter)
    second = KisRateLimitedSession(KisEnvironment.DEMO, second_raw, limiter=limiter)

    first.get("https://example.test/orders")
    second.post("https://example.test/account")

    assert sleeps == [0.5]
    assert first_raw.calls == [("GET", "https://example.test/orders", 0.0)]
    assert second_raw.calls == [("POST", "https://example.test/account", 0.5)]


def test_real_rest_calls_use_official_shorter_interval():
    clock = FakeClock()
    sleeps = []

    def sleep(seconds):
        sleeps.append(seconds)
        clock.now += seconds

    limiter = KisRateLimiter(clock=clock, sleeper=sleep)
    session = KisRateLimitedSession(
        KisEnvironment.REAL,
        FakeSession(clock),
        limiter=limiter,
    )

    session.get("https://example.test/first")
    session.get("https://example.test/second")

    assert sleeps == [0.05]
