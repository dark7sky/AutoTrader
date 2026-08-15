from dataclasses import dataclass

import pytest

from kis_ai_scalper.broker.kis_order import (
    KisOrderClient,
    KisOrderRequest,
    KisOrderSide,
    KisOrderType,
    build_order_body,
    order_tr_id,
)


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

    def post(self, url, **kwargs):
        self.posts.append((url, kwargs))
        if url.endswith("/uapi/hashkey"):
            return FakeResponse({"HASH": "hash-value"})
        return FakeResponse({"rt_cd": "0", "output": {"ODNO": "1234567890"}})


def test_order_tr_ids_match_kis_demo_and_real_cash_order_sample():
    assert order_tr_id("demo", "buy") == "VTTC0012U"
    assert order_tr_id("demo", "sell") == "VTTC0011U"
    assert order_tr_id("real", "buy") == "TTTC0012U"
    assert order_tr_id("real", "sell") == "TTTC0011U"


def test_build_limit_order_body_uses_required_kis_fields():
    body = build_order_body(
        "12345678",
        "01",
        KisOrderRequest("005930", KisOrderSide.BUY, 3, 71200, KisOrderType.LIMIT),
    )

    assert body == {
        "CANO": "12345678",
        "ACNT_PRDT_CD": "01",
        "PDNO": "005930",
        "ORD_DVSN": "00",
        "ORD_QTY": "3",
        "ORD_UNPR": "71200",
        "EXCG_ID_DVSN_CD": "KRX",
        "SLL_TYPE": "",
        "CNDT_PRIC": "",
    }


def test_market_order_body_uses_zero_order_price():
    body = build_order_body(
        "12345678",
        "01",
        KisOrderRequest("005930", "buy", 1, 0, "market"),
    )

    assert body["ORD_DVSN"] == "01"
    assert body["ORD_UNPR"] == "0"


def test_submit_order_issues_hashkey_and_posts_cash_order():
    session = FakeSession()
    client = KisOrderClient(
        "demo", "app-key", "app-secret", "token", "12345678", "01",
        session=session,
    )
    result = client.submit_order(
        KisOrderRequest("005930", KisOrderSide.BUY, 2, 70000)
    )

    assert len(session.posts) == 2
    hash_url, hash_kwargs = session.posts[0]
    order_url, order_kwargs = session.posts[1]
    assert hash_url.endswith("/uapi/hashkey")
    assert order_url.endswith("/uapi/domestic-stock/v1/trading/order-cash")
    assert hash_kwargs["headers"]["appkey"] == "app-key"
    assert order_kwargs["headers"]["authorization"] == "Bearer token"
    assert order_kwargs["headers"]["tr_id"] == "VTTC0012U"
    assert order_kwargs["headers"]["hashkey"] == "hash-value"
    assert order_kwargs["json"]["ORD_QTY"] == "2"
    assert result.broker_order_id == "1234567890"
    assert result.tr_id == "VTTC0012U"


def test_order_request_rejects_invalid_values():
    with pytest.raises(ValueError, match="six-digit"):
        KisOrderRequest("ABC", "buy", 1, 1000)
    with pytest.raises(ValueError, match="quantity"):
        KisOrderRequest("005930", "buy", 0, 1000)
    with pytest.raises(ValueError, match="positive price"):
        KisOrderRequest("005930", "buy", 1, 0)
    with pytest.raises(ValueError, match="account_no"):
        build_order_body("123", "01", KisOrderRequest("005930", "buy", 1, 1000))


def test_kis_business_error_raises_without_success_result():
    class ErrorSession(FakeSession):
        def post(self, url, **kwargs):
            self.posts.append((url, kwargs))
            if url.endswith("/uapi/hashkey"):
                return FakeResponse({"HASH": "hash-value"})
            return FakeResponse({"rt_cd": "-1", "msg1": "order rejected"})

    client = KisOrderClient(
        "demo", "app-key", "app-secret", "token", "12345678", "01",
        session=ErrorSession(),
    )

    with pytest.raises(RuntimeError, match="order rejected"):
        client.submit_order(KisOrderRequest("005930", "buy", 1, 70000))
