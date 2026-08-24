from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import threading
import time

import pytest

from kis_ai_scalper import cli
from kis_ai_scalper.broker.kis_auth import KisAuthClient, KisHttpError, redact
from kis_ai_scalper.broker.kis_endpoints import base_url
from kis_ai_scalper.broker.kis_rest import KisRestClient


@dataclass
class FakeResponse:
    payload: dict
    status_code: int = 200

    def raise_for_status(self):
        return None

    def json(self):
        return self.payload


class FakeSession:
    def __init__(self):
        self.posts = []
        self.gets = []

    def post(self, url, **kwargs):
        self.posts.append((url, kwargs))
        if url.endswith("/tokenP"):
            return FakeResponse({"access_token": "access-token-value", "expires_in": 3600})
        return FakeResponse({"approval_key": "approval-key-value"})

    def get(self, url, **kwargs):
        self.gets.append((url, kwargs))
        return FakeResponse({"rt_cd": "0", "output": {"stck_prpr": "71200", "stck_oprc": "70000"}})


def test_auth_uses_correct_secret_field_and_environment_url():
    session = FakeSession()
    client = KisAuthClient("demo", "app-key", "app-secret", session=session)
    result = client.authenticate_read_only()

    assert base_url("demo") == "https://openapivts.koreainvestment.com:29443"
    assert session.posts[0][1]["json"]["appsecret"] == "app-secret"
    assert session.posts[1][1]["json"]["secretkey"] == "app-secret"
    assert result.access_token == "access-token-value"
    assert result.approval_key == "approval-key-value"


def test_current_price_request_is_read_only_and_has_required_headers():
    session = FakeSession()
    client = KisRestClient("demo", "app-key", "app-secret", "token", session=session)
    quote = client.get_current_price("005930")

    url, kwargs = session.gets[0]
    assert url.endswith("/uapi/domestic-stock/v1/quotations/inquire-price")
    assert kwargs["headers"]["authorization"] == "Bearer token"
    assert kwargs["headers"]["tr_id"] == "FHKST01010100"
    assert kwargs["params"] == {"FID_COND_MRKT_DIV_CODE": "J", "FID_INPUT_ISCD": "005930"}
    assert quote.price == 71200


def test_redaction_never_returns_complete_secret():
    assert redact("short") == "*****"
    assert redact("abcdefghijkl") == "abcd...ijkl"


def test_cache_hit_skips_token_and_approval_posts(tmp_path: Path):
    cache = tmp_path / "data" / "kis_token_demo.json"
    now = datetime.now(timezone.utc)
    cache.parent.mkdir(parents=True)
    cache.write_text(json.dumps({
        "access_token": "cached-token",
        "approval_key": "cached-approval",
        "issued_at": now.isoformat(),
        "expires_at": (now + timedelta(hours=1)).isoformat(),
    }), encoding="utf-8")
    session = FakeSession()

    result = KisAuthClient("demo", "app-key", "app-secret", session=session).authenticate_read_only(cache)

    assert result.cache_hit is True
    assert result.access_token == "cached-token"
    assert session.posts == []


def test_cache_miss_and_refresh_issue_both_credentials(tmp_path: Path):
    cache = tmp_path / "kis_token_demo.json"
    first_session = FakeSession()
    first = KisAuthClient("demo", "app-key", "app-secret", session=first_session).authenticate_read_only(cache)
    assert first.cache_hit is False
    assert len(first_session.posts) == 2

    refresh_session = FakeSession()
    refreshed = KisAuthClient("demo", "app-key", "app-secret", session=refresh_session).authenticate_read_only(
        cache, refresh_token=True
    )
    assert refreshed.cache_hit is False
    assert len(refresh_session.posts) == 2
    assert json.loads(cache.read_text(encoding="utf-8"))["expires_at"]


def test_concurrent_cache_miss_issues_credentials_once(tmp_path: Path):
    cache = tmp_path / "kis_token_demo.json"
    calls = 0
    calls_lock = threading.Lock()

    class SlowSharedSession(FakeSession):
        def post(self, url, **kwargs):
            nonlocal calls
            with calls_lock:
                calls += 1
            time.sleep(0.05)
            return super().post(url, **kwargs)

    clients = [
        KisAuthClient("demo", "app-key", "app-secret", session=SlowSharedSession())
        for _ in range(2)
    ]
    start = threading.Barrier(2)
    results = []

    def authenticate(client):
        start.wait()
        results.append(client.authenticate_read_only(cache))

    threads = [threading.Thread(target=authenticate, args=(client,)) for client in clients]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=2)

    assert all(not thread.is_alive() for thread in threads)
    assert calls == 2
    assert {result.access_token for result in results} == {"access-token-value"}
    assert {result.approval_key for result in results} == {"approval-key-value"}
    assert sorted(result.cache_hit for result in results) == [False, True]


def test_approval_failure_preserves_access_token_for_retry(tmp_path: Path):
    cache = tmp_path / "kis_token_demo.json"

    class ApprovalFailureSession(FakeSession):
        def post(self, url, **kwargs):
            if url.endswith("/Approval"):
                return FakeResponse(
                    {"rt_cd": "-1", "msg_cd": "WS_ERROR"}, status_code=503
                )
            return super().post(url, **kwargs)

    with pytest.raises(KisHttpError):
        KisAuthClient(
            "demo", "app-key", "app-secret", session=ApprovalFailureSession()
        ).authenticate_read_only(cache)

    partial = json.loads(cache.read_text(encoding="utf-8"))
    assert partial["access_token"] == "access-token-value"
    assert "approval_key" not in partial

    retry_session = FakeSession()
    result = KisAuthClient(
        "demo", "app-key", "app-secret", session=retry_session
    ).authenticate_read_only(cache)

    assert result.access_token == "access-token-value"
    assert result.approval_key == "approval-key-value"
    assert len(retry_session.posts) == 1
    assert retry_session.posts[0][0].endswith("/Approval")


def test_access_token_rate_limit_is_cached_without_repeating_http_call(tmp_path: Path):
    cache = tmp_path / "kis_token_demo.json"

    class RateLimitedSession:
        def __init__(self):
            self.posts = 0

        def post(self, url, **kwargs):
            self.posts += 1
            return FakeResponse(
                {
                    "rt_cd": "-1",
                    "msg_cd": "EGW_RATE_LIMIT",
                    "error_description": "one access token per minute",
                },
                status_code=403,
            )

    session = RateLimitedSession()
    client = KisAuthClient("demo", "app-key", "app-secret", session=session)

    with pytest.raises(KisHttpError, match="HTTP 403"):
        client.authenticate_read_only(cache)
    with pytest.raises(KisHttpError, match="retry_after"):
        client.authenticate_read_only(cache)

    assert session.posts == 1
    cached = json.loads(cache.read_text(encoding="utf-8"))
    assert cached["last_error_status"] == 403
    assert "access_token" not in cached


def test_http_403_body_becomes_safe_error_without_secrets():
    app_key = "app-key-secret"
    app_secret = "app-secret-value"

    class ErrorSession:
        def post(self, url, **kwargs):
            return FakeResponse({
                "rt_cd": "-1",
                "msg_cd": "EGW00123",
                "msg1": f"invalid {app_secret} {app_key}",
                "error_description": "forbidden",
            }, status_code=403)

    with pytest.raises(KisHttpError) as exc_info:
        KisAuthClient("demo", app_key, app_secret, session=ErrorSession()).issue_access_token()
    message = str(exc_info.value)
    assert "HTTP 403" in message
    assert "EGW00123" in message
    assert app_key not in message
    assert app_secret not in message


def test_broker_state_smoke_reads_orders_and_account_without_writes(monkeypatch, capsys):
    calls = []

    class Orders:
        def get_today_orders(self):
            calls.append("orders")
            return []

    class Account:
        def get_snapshot(self):
            calls.append("account")
            return type("Snapshot", (), {"positions": (), "summary": type("Summary", (), {})()})()

    monkeypatch.setattr(cli, "_broker_clients", lambda *a, **k: (Orders(), Account()))

    assert cli.main([
        "smoke-broker-state",
        "--config", "config/settings.yaml",
        "--env", "demo",
    ]) == 0
    output = capsys.readouterr().out
    assert "KIS broker state smoke: OK" in output
    assert "orders=0" in output
    assert calls == ["orders", "account"]
