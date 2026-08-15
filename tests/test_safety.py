from pathlib import Path

from kis_ai_scalper.broker.safety import (
    contains_forbidden_kis_endpoint,
    contains_token_like_value,
    redact,
)


ROOT = Path(__file__).parents[1]


def test_forbidden_kis_endpoint_detector_catches_order_and_account_paths():
    assert contains_forbidden_kis_endpoint("/uapi/domestic-stock/v1/trading/order-cash")
    assert contains_forbidden_kis_endpoint("https://example.test/api/orders")
    assert contains_forbidden_kis_endpoint("/uapi/domestic-stock/v1/trading/inquire-balance")
    assert not contains_forbidden_kis_endpoint("/uapi/domestic-stock/v1/quotations/inquire-price")


def test_read_only_smoke_sources_have_no_forbidden_endpoint_paths():
    files = (
        ROOT / "kis_ai_scalper" / "broker" / "kis_auth.py",
        ROOT / "kis_ai_scalper" / "broker" / "kis_rest.py",
        ROOT / "kis_ai_scalper" / "cli.py",
    )
    for path in files:
        assert not contains_forbidden_kis_endpoint(path.read_text(encoding="utf-8")), path


def test_redaction_does_not_return_complete_tokens_or_secrets():
    app_secret = "app-secret-value-123456789"
    access_token = "access-token-value-abcdefghijklmnopqrstuvwxyz"
    for secret in (app_secret, access_token):
        masked = redact(secret)
        assert secret not in masked
        assert "app-secret" not in masked
        assert "access-token" not in masked


def test_docs_and_readme_have_no_jwt_shaped_token():
    for path in (ROOT / "README.md", ROOT / "docs" / "kis-ai-extensions-review.md"):
        assert not contains_token_like_value(path.read_text(encoding="utf-8")), path
