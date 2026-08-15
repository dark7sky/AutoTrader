from dataclasses import dataclass

import pytest

from kis_ai_scalper.broker.kis_auth import KisHttpError
from kis_ai_scalper.broker.kis_market_rules import (
    is_valid_krx_limit_price,
    is_valid_risk_reward,
    krx_tick_size,
    normalize_krx_limit_price,
    validate_krx_limit_price,
    validate_risk_reward,
)
from kis_ai_scalper.broker.kis_order import KisOrderSide
from kis_ai_scalper.broker.kis_order_status import (
    DAILY_ORDER_PATH,
    KisOrderStatus,
    KisOrderStatusClient,
    KisPaginationContext,
    ORDER_REVISE_CANCEL_PATH,
    parse_order_status,
)


@dataclass
class FakeResponse:
    payload: dict
    headers: dict | None = None
    status_code: int = 200

    def raise_for_status(self):
        return None

    def json(self):
        return self.payload


class FakeSession:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []

    def get(self, url, **kwargs):
        self.calls.append(("GET", url, kwargs))
        return self.responses.pop(0)

    def post(self, url, **kwargs):
        self.calls.append(("POST", url, kwargs))
        return self.responses.pop(0)


def client(session):
    return KisOrderStatusClient("demo", "app-secret-key", "app-secret-value", "access-token", "12345678", "01", session=session)


def row(**overrides):
    value = {
        "odno": "0000123456",
        "pdno": "005930",
        "sll_buy_dvsn_cd": "02",
        "ord_qty": "10",
        "tot_ccld_qty": "4",
        "rmn_qty": "6",
        "ord_unpr": "70,000",
        "avg_prvs": "69,900",
        "ord_tmd": "091530",
        "ord_dt": "20260818",
        "ord_gno_brno": "001",
    }
    value.update(overrides)
    return value


def test_parse_order_status_is_conservative_and_includes_requested_fields():
    parsed = parse_order_status(row())
    assert parsed is not None
    assert parsed.side is KisOrderSide.BUY
    assert parsed.ordered_quantity == 10
    assert parsed.filled_quantity == 4
    assert parsed.remaining_quantity == 6
    assert parsed.order_price == 70000
    assert parsed.average_fill_price == 69900
    assert parsed.status is KisOrderStatus.PARTIALLY_FILLED
    assert parsed.order_time == "091530"


def test_daily_order_query_uses_demo_tr_id_and_paginates():
    session = FakeSession([
        FakeResponse({"rt_cd": "0", "output1": [row(odno="1")], "ctx_area_fk100": "FK1", "ctx_area_nk100": "NK1"}, {"tr_cont": "M"}),
        FakeResponse({"rt_cd": "0", "output1": [row(odno="2", tot_ccld_qty="10", rmn_qty="0")], "ctx_area_fk100": "", "ctx_area_nk100": ""}, {"tr_cont": "N"}),
    ])
    orders = client(session).get_orders("20260818")
    assert [order.order_number for order in orders] == ["1", "2"]
    first = session.calls[0][2]
    second = session.calls[1][2]
    assert first["headers"]["tr_id"] == "VTTC0081R"
    assert first["params"]["CTX_AREA_FK100"] == ""
    assert second["headers"]["tr_cont"] == "N"
    assert second["params"]["CTX_AREA_NK100"] == "NK1"


def test_unfilled_filter_uses_api_filter_and_local_remaining_guard():
    session = FakeSession([FakeResponse({"rt_cd": "0", "output1": [row(), row(odno="2", ord_qty="0", rmn_qty="0")]})])
    orders = client(session).get_unfilled_orders(inquiry_date="20260818")
    assert [order.order_number for order in orders] == ["0000123456"]
    assert session.calls[0][2]["params"]["CCLD_DVSN"] == "02"


def test_cancel_uses_demo_tr_id_and_all_remaining_body():
    session = FakeSession([FakeResponse({"rt_cd": "0", "output": {"ODNO": "999"}})])
    result = client(session).cancel_order("0000123456", "000001", order_price=70000)
    method, url, kwargs = session.calls[0]
    assert method == "POST"
    assert url.endswith(ORDER_REVISE_CANCEL_PATH)
    assert kwargs["headers"]["tr_id"] == "VTTC0013U"
    assert kwargs["json"]["RVSE_CNCL_DVSN_CD"] == "02"
    assert kwargs["json"]["QTY_ALL_ORD_YN"] == "Y"
    assert result.order_number == "999"


def test_http_error_uses_safe_kis_error_without_secret_in_message():
    session = FakeSession([FakeResponse({"rt_cd": "1", "msg1": "app-secret-key access-token"}, status_code=403)])
    with pytest.raises(KisHttpError) as error:
        client(session).get_orders("20260818")
    assert "app-secret-key" not in str(error.value)
    assert "access-token" not in str(error.value)


@pytest.mark.parametrize("price,tick", [(1999, 1), (2000, 5), (4999, 5), (5000, 10), (19999, 10), (20000, 50), (49999, 50), (50000, 100), (199999, 100), (200000, 500), (499999, 500), (500000, 1000)])
def test_2026_krx_price_bands(price, tick):
    assert krx_tick_size(price) == tick


def test_krx_normalization_is_directional_and_validation_is_strict():
    assert normalize_krx_limit_price(70123, "buy") == 70100
    assert normalize_krx_limit_price(70123, "sell") == 70200
    assert is_valid_krx_limit_price(70200)
    assert not is_valid_krx_limit_price(70123)
    with pytest.raises(ValueError):
        validate_krx_limit_price(70123)


def test_risk_reward_and_stop_width_are_enforced():
    result = validate_risk_reward(100, 110, 99, minimum_ratio=1.5, minimum_stop_distance_pct=0.005)
    assert result.ratio == 10
    assert is_valid_risk_reward(100, 101, 99, minimum_ratio=1.5, minimum_stop_distance_pct=0.005) is False
    with pytest.raises(ValueError):
        validate_risk_reward(100, 101, 99.9)


def test_pagination_context_has_next_only_with_broker_continuation_and_key():
    assert KisPaginationContext("M", "FK", "NK").has_next
    assert not KisPaginationContext("M", "", "").has_next
    assert KisPaginationContext("M", "FK", "NK").next_request.tr_cont == "N"
