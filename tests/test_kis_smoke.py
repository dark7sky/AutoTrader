from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path

import pytest

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
