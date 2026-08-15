"""Read-only KIS domestic-stock buying-power adapter.

The request shape and transaction IDs follow KIS's official
``inquire_psbl_order`` sample.  This client never submits an order.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping

import requests

from .kis_auth import _raise_for_kis_response
from .kis_endpoints import KisEnvironment, api_url


BUYING_POWER_PATH = "/uapi/domestic-stock/v1/trading/inquire-psbl-order"
BUYING_POWER_TR_IDS = {
    KisEnvironment.REAL: "TTTC8908R",
    KisEnvironment.DEMO: "VTTC8908R",
}


@dataclass(frozen=True)
class KisBuyingPowerSnapshot:
    """A conservative, read-only result for one price/symbol query."""

    symbol: str
    order_price: float
    orderable_cash: float | None
    no_margin_buy_amount: float | None
    max_buy_amount: float | None
    orderable_quantity: int | None
    raw: Mapping[str, Any]

    @property
    def available_cash(self) -> float | None:
        return self.orderable_cash

    @property
    def no_margin_cash(self) -> float | None:
        return self.no_margin_buy_amount


def _first(row: Mapping[str, Any], *names: str) -> Any:
    for name in names:
        value = row.get(name)
        if value not in (None, ""):
            return value
    return None


def _number(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        parsed = float(str(value).replace(",", "").strip())
    except (TypeError, ValueError):
        return None
    return parsed if parsed == parsed and parsed >= 0 else None


def _integer(value: Any) -> int | None:
    parsed = _number(value)
    if parsed is None or not parsed.is_integer():
        return None
    return int(parsed)


def _output(payload: Mapping[str, Any]) -> Mapping[str, Any]:
    value = payload.get("output")
    return value if isinstance(value, Mapping) else {}


def parse_buying_power(payload: Mapping[str, Any]) -> KisBuyingPowerSnapshot:
    """Parse an official KIS ``output`` object without inventing missing data."""
    if not isinstance(payload, Mapping):
        raise TypeError("KIS buying-power payload must be a mapping")
    output = _output(payload) if "output" in payload else payload
    symbol = str(_first(output, "pdno", "PDNO", "symbol", "SYMBOL") or "")
    order_price = _number(_first(output, "psbl_qty_calc_unpr", "PSBL_QTY_CALC_UNPR", "ord_unpr"))
    no_margin_amount = _number(
        _first(output, "nrcvb_buy_amt", "NRCVB_BUY_AMT", "no_margin_buy_amount")
    )
    orderable_cash = _number(_first(output, "ord_psbl_cash", "ORD_PSBL_CASH"))
    # KIS documents NRCVB_BUY_AMT as the amount to use when margin is not used.
    trusted_cash = no_margin_amount if no_margin_amount is not None else orderable_cash
    return KisBuyingPowerSnapshot(
        symbol=symbol,
        order_price=order_price if order_price is not None else 0.0,
        orderable_cash=trusted_cash,
        no_margin_buy_amount=no_margin_amount,
        max_buy_amount=_number(_first(output, "max_buy_amt", "MAX_BUY_AMT")),
        orderable_quantity=_integer(
            _first(output, "nrcvb_buy_qty", "NRCVB_BUY_QTY", "ord_psbl_qty", "ORD_PSBL_QTY")
        ),
        raw=dict(output),
    )


class KisBuyingPowerClient:
    """Read-only client for real and simulated KIS accounts."""

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
        self.environment = KisEnvironment.parse(environment) if isinstance(environment, str) else environment
        self.app_key = app_key
        self.app_secret = app_secret
        self.access_token = access_token
        self.account_no = account_no
        self.account_product_code = account_product_code
        self.session = session if session is not None else requests.Session()
        self.timeout = timeout

    def _validate(self, symbol: str, price: float) -> None:
        if not symbol.isdigit() or len(symbol) != 6:
            raise ValueError("symbol must be a six-digit domestic stock code")
        if isinstance(price, bool) or price <= 0:
            raise ValueError("price must be positive")
        if not self.account_no.isdigit() or len(self.account_no) != 8:
            raise ValueError("KIS account_no must be exactly eight digits")
        if not self.account_product_code.isdigit() or len(self.account_product_code) != 2:
            raise ValueError("KIS account_product_code must be exactly two digits")

    def get_snapshot(self, symbol: str, price: float, order_type: str = "01") -> KisBuyingPowerSnapshot:
        self._validate(symbol, price)
        if not order_type:
            raise ValueError("order_type must not be empty")
        response = self.session.get(
            api_url(self.environment, BUYING_POWER_PATH),
            headers={
                "content-type": "application/json; charset=utf-8",
                "authorization": f"Bearer {self.access_token}",
                "appkey": self.app_key,
                "appsecret": self.app_secret,
                "tr_id": BUYING_POWER_TR_IDS[self.environment],
                "custtype": "P",
            },
            params={
                "CANO": self.account_no,
                "ACNT_PRDT_CD": self.account_product_code,
                "PDNO": symbol,
                "ORD_UNPR": str(int(price)) if float(price).is_integer() else str(price),
                "ORD_DVSN": order_type,
                "CMA_EVLU_AMT_ICLD_YN": "N",
                "OVRS_ICLD_YN": "N",
            },
            timeout=self.timeout,
        )
        _raise_for_kis_response(response, (self.app_key, self.app_secret, self.access_token))
        payload = response.json()
        if not isinstance(payload, Mapping) or str(payload.get("rt_cd", "0")) != "0":
            raise RuntimeError("KIS buying-power request failed")
        snapshot = parse_buying_power(payload)
        return KisBuyingPowerSnapshot(
            symbol=symbol,
            order_price=snapshot.order_price or price,
            orderable_cash=snapshot.orderable_cash,
            no_margin_buy_amount=snapshot.no_margin_buy_amount,
            max_buy_amount=snapshot.max_buy_amount,
            orderable_quantity=snapshot.orderable_quantity,
            raw=snapshot.raw,
        )

    fetch = get_snapshot
    get = get_snapshot


__all__ = [
    "BUYING_POWER_PATH",
    "BUYING_POWER_TR_IDS",
    "KisBuyingPowerClient",
    "KisBuyingPowerSnapshot",
    "parse_buying_power",
]
