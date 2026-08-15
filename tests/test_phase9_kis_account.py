from dataclasses import dataclass

import pytest

from kis_ai_scalper.broker.kis_account import (
    KisAccountClient,
    KisAccountSummary,
    parse_account_position,
    parse_account_summary,
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
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []

    def get(self, url, **kwargs):
        self.calls.append((url, kwargs))
        return self.responses.pop(0)


def account_row(symbol="005930"):
    return {
        "pdno": symbol,
        "prdt_name": "Samsung",
        "hldg_qty": "2",
        "ord_psbl_qty": "1",
        "pchs_avg_pric": "70,000",
        "prpr": "71,000",
        "evlu_pfls_amt": "2,000",
        "evlu_amt": "142,000",
    }


def page(row, *, cursor="", continuation="N", summary=True):
    payload = {
        "rt_cd": "0",
        "output1": [row] if row else [],
        "output2": [{
            "dnca_tot_amt": "1,000,000",
            "ord_psbl_cash": "900,000",
            "tot_evlu_amt": "1,142,000",
            "evlu_pfls_smtl_amt": "2,000",
        }] if summary else [],
        "ctx_area_fk100": cursor,
        "ctx_area_nk100": cursor,
    }
    return FakeResponse(payload, {"tr_cont": continuation})


def client(session, environment="demo", **kwargs):
    return KisAccountClient(
        environment, "app-key", "app-secret", "access-token", "12345678", "01",
        session=session, **kwargs,
    )


def test_parses_position_and_summary_fields_conservatively():
    position = parse_account_position(account_row())
    summary = parse_account_summary(page(None).payload["output2"][0])

    assert position.symbol == "005930"
    assert (position.qty, position.sellable_qty) == (2, 1)
    assert (position.avg_price, position.current_price, position.evaluation_pnl) == (70000.0, 71000.0, 2000.0)
    assert summary == KisAccountSummary(1000000.0, 900000.0, 1142000.0, 2000.0)


def test_invalid_position_and_missing_optional_summary_are_safe():
    assert parse_account_position({"pdno": "BAD", "hldg_qty": "2"}) is None
    position = parse_account_position({**account_row(), "ord_psbl_qty": "-1", "prpr": "bad"})
    assert position.sellable_qty is None
    assert position.current_price is None
    assert parse_account_summary({}).deposit is None
    assert parse_account_summary({}).orderable_cash_estimate is None


def test_queries_demo_with_official_tr_id_and_account_params():
    session = FakeSession([page(account_row())])
    snapshot = client(session).get_snapshot()

    assert snapshot.positions[0].symbol == "005930"
    _, kwargs = session.calls[0]
    assert kwargs["headers"]["tr_id"] == "VTTC8434R"
    assert kwargs["params"]["CANO"] == "12345678"
    assert kwargs["params"]["ACNT_PRDT_CD"] == "01"


def test_queries_real_with_official_tr_id():
    session = FakeSession([page(None, summary=False)])
    snapshot = client(session, "real").get_snapshot()
    assert snapshot.positions == ()
    assert session.calls[0][1]["headers"]["tr_id"] == "TTTC8434R"
    assert snapshot.summary.total_evaluation is None


def test_follows_continuation_and_merges_positions():
    session = FakeSession([page(account_row(), cursor="FK1", continuation="M"), page(account_row("000660"))])
    snapshot = client(session).get_snapshot()

    assert [position.symbol for position in snapshot.positions] == ["005930", "000660"]
    assert session.calls[1][1]["params"]["CTX_AREA_FK100"] == "FK1"
    assert session.calls[1][1]["headers"]["tr_cont"] == "N"


def test_pagination_is_hard_limited_to_ten_pages():
    responses = [page(account_row(str(590000 + index)), cursor=f"{index}", continuation="M") for index in range(10)]
    with pytest.raises(RuntimeError, match="exceeded max_pages"):
        client(FakeSession(responses)).get_snapshot()


def test_stalled_cursor_is_rejected_and_http_error_is_secret_safe():
    stalled = FakeSession([page(account_row(), cursor="SAME", continuation="M"), page(account_row(), cursor="SAME", continuation="M")])
    with pytest.raises(RuntimeError, match="cursor did not advance"):
        client(stalled).get_snapshot()

    error = FakeResponse({"msg1": "bad app-secret access-token"}, status_code=403)
    with pytest.raises(KisHttpError) as caught:
        client(FakeSession([error])).get_snapshot()
    assert "app-secret" not in str(caught.value)
    assert "access-token" not in str(caught.value)
