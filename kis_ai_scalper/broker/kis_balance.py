"""Read-only KIS domestic stock balance adapter."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import requests

from .kis_auth import _raise_for_kis_response
from .kis_endpoints import KisEnvironment, api_url
from .kis_rate_limit import new_rate_limited_session


BALANCE_PATH = "/uapi/domestic-stock/v1/trading/inquire-balance"
BALANCE_TR_IDS = {
    KisEnvironment.REAL: "TTTC8434R",
    KisEnvironment.DEMO: "VTTC8434R",
}


@dataclass(frozen=True)
class KisBalancePosition:
    symbol: str
    name: str
    quantity: int
    average_price: float
    current_price: float | None = None


class KisBalanceClient:
    def __init__(
        self,
        environment: KisEnvironment | str,
        app_key: str,
        app_secret: str,
        access_token: str,
        account_no: str,
        account_product_code: str,
        session: requests.Session | Any | None = None,
        timeout: float = 15.0,
    ) -> None:
        self.environment = (
            KisEnvironment.parse(environment) if isinstance(environment, str) else environment
        )
        self.app_key = app_key
        self.app_secret = app_secret
        self.access_token = access_token
        self.account_no = account_no
        self.account_product_code = account_product_code
        self.session = session or new_rate_limited_session(self.environment)
        self.timeout = timeout

    def get_positions(self) -> tuple[KisBalancePosition, ...]:
        if not self.account_no.isdigit() or len(self.account_no) != 8:
            raise ValueError("KIS account_no must be exactly eight digits")
        if not self.account_product_code.isdigit() or len(self.account_product_code) != 2:
            raise ValueError("KIS account_product_code must be exactly two digits")
        response = self.session.get(
            api_url(self.environment, BALANCE_PATH),
            headers={
                "content-type": "application/json; charset=utf-8",
                "authorization": f"Bearer {self.access_token}",
                "appkey": self.app_key,
                "appsecret": self.app_secret,
                "tr_id": BALANCE_TR_IDS[self.environment],
                "custtype": "P",
            },
            params={
                "CANO": self.account_no,
                "ACNT_PRDT_CD": self.account_product_code,
                "AFHR_FLPR_YN": "N",
                "OFL_YN": "",
                "INQR_DVSN": "02",
                "UNPR_DVSN": "01",
                "FUND_STTL_ICLD_YN": "N",
                "FNCG_AMT_AUTO_RDPT_YN": "N",
                "PRCS_DVSN": "01",
                "CTX_AREA_FK100": "",
                "CTX_AREA_NK100": "",
            },
            timeout=self.timeout,
        )
        _raise_for_kis_response(response, (self.app_key, self.app_secret, self.access_token))
        payload = response.json()
        if str(payload.get("rt_cd", "0")) != "0":
            raise RuntimeError(f"KIS balance request failed: {payload.get('msg1', 'unknown error')}")
        rows = payload.get("output1") or []
        if not isinstance(rows, list):
            rows = []
        return tuple(position for row in rows if (position := _parse_position(row)) is not None)


def _parse_position(row: Any) -> KisBalancePosition | None:
    if not isinstance(row, dict):
        return None
    symbol = str(row.get("pdno") or row.get("PDNO") or "").strip()
    if not symbol.isdigit() or len(symbol) != 6:
        return None
    quantity = _int_text(row.get("hldg_qty") or row.get("HLDG_QTY"))
    if quantity <= 0:
        return None
    return KisBalancePosition(
        symbol=symbol,
        name=str(row.get("prdt_name") or row.get("PRDT_NAME") or ""),
        quantity=quantity,
        average_price=_float_text(row.get("pchs_avg_pric") or row.get("PCHS_AVG_PRIC")),
        current_price=_optional_float_text(row.get("prpr") or row.get("PRPR")),
    )


def _int_text(value: Any) -> int:
    if value in (None, ""):
        return 0
    return int(float(str(value).replace(",", "")))


def _float_text(value: Any) -> float:
    return float(str(value or "0").replace(",", ""))


def _optional_float_text(value: Any) -> float | None:
    if value in (None, ""):
        return None
    return _float_text(value)
