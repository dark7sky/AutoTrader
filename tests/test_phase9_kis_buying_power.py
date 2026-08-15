from dataclasses import dataclass

import pytest

from kis_ai_scalper.broker.kis_buying_power import (
    BUYING_POWER_PATH,
    KisBuyingPowerClient,
    parse_buying_power,
)
from kis_ai_scalper.broker.kis_auth import KisHttpError


@dataclass
class FakeResponse:
    payload: dict
    headers: dict[str, str] | None = None
    status_code: int = 200

    def raise_for_status(self):
        return None

    def json(self):
        return self.payload


class FakeSession:
    def __init__(self, response):
        self.response = response
        self.calls = []

    def get(self, url, **kwargs):
        self.calls.append((url, kwargs))
        return self.response


def payload(**output):
    return {"rt_cd": "0", "output": output}


def test_prefers_official_no_margin_buy_amount_for_cash():
    result = parse_buying_power(payload(
        pdno="005930",
        psbl_qty_calc_unpr="70,000",
        ord_psbl_cash="900000",
        nrcvb_buy_amt="875000",
        nrcvb_buy_qty="12",
        max_buy_amt="1200000",
    ))

    assert result.orderable_cash == 875000.0
    assert result.no_margin_buy_amount == 875000.0
    assert result.orderable_quantity == 12


def test_missing_cash_stays_none_instead_of_being_invented():
    result = parse_buying_power(payload(pdno="005930", nrcvb_buy_qty="bad"))
    assert result.orderable_cash is None
    assert result.orderable_quantity is None


def test_demo_query_uses_official_read_only_request_shape():
    session = FakeSession(FakeResponse(payload(
        pdno="005930", psbl_qty_calc_unpr="70000", nrcvb_buy_amt="875000"
    )))
    client = KisBuyingPowerClient(
        "demo", "app-key", "app-secret", "access-token", "12345678", "01", session=session
    )

    result = client.get_snapshot("005930", 70000)
    _, kwargs = session.calls[0]
    assert result.orderable_cash == 875000
    assert kwargs["headers"]["tr_id"] == "VTTC8908R"
    assert kwargs["params"] == {
        "CANO": "12345678",
        "ACNT_PRDT_CD": "01",
        "PDNO": "005930",
        "ORD_UNPR": "70000",
        "ORD_DVSN": "01",
        "CMA_EVLU_AMT_ICLD_YN": "N",
        "OVRS_ICLD_YN": "N",
    }
    assert kwargs["timeout"] == 15.0


def test_real_query_and_http_error_are_secret_safe():
    ok = FakeSession(FakeResponse(payload(nrcvb_buy_amt="100")))
    client = KisBuyingPowerClient(
        "real", "app-secret", "secret-value", "token-value", "12345678", "01", session=ok
    )
    assert client.get_snapshot("005930", 100).orderable_cash == 100
    assert ok.calls[0][1]["headers"]["tr_id"] == "TTTC8908R"

    failed = FakeSession(FakeResponse(
        {"msg1": "secret-value token-value"}, status_code=403
    ))
    with pytest.raises(KisHttpError) as caught:
        KisBuyingPowerClient(
            "real", "app-secret", "secret-value", "token-value", "12345678", "01", session=failed
        ).get_snapshot("005930", 100)
    assert "secret-value" not in str(caught.value)
    assert "token-value" not in str(caught.value)


def test_invalid_inputs_fail_before_network():
    session = FakeSession(FakeResponse(payload()))
    client = KisBuyingPowerClient(
        "demo", "a", "b", "c", "12345678", "01", session=session
    )
    with pytest.raises(ValueError):
        client.get_snapshot("bad", 100)
    with pytest.raises(ValueError):
        client.get_snapshot("005930", 0)
    assert session.calls == []
