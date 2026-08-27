"""KIS domestic-stock cash order adapter with injectable HTTP transport."""

from __future__ import annotations

from dataclasses import dataclass
import json
from enum import StrEnum
from typing import Any

import requests

from .kis_auth import _raise_for_kis_response
from .kis_endpoints import KisEnvironment, api_url
from .kis_rate_limit import new_rate_limited_session


ORDER_CASH_PATH = "/uapi/domestic-stock/v1/trading/order-cash"
HASHKEY_PATH = "/uapi/hashkey"


class KisOrderSide(StrEnum):
    BUY = "buy"
    SELL = "sell"


class KisOrderType(StrEnum):
    LIMIT = "limit"
    MARKET = "market"


TR_IDS = {
    (KisEnvironment.REAL, KisOrderSide.BUY): "TTTC0012U",
    (KisEnvironment.REAL, KisOrderSide.SELL): "TTTC0011U",
    (KisEnvironment.DEMO, KisOrderSide.BUY): "VTTC0012U",
    (KisEnvironment.DEMO, KisOrderSide.SELL): "VTTC0011U",
}

ORDER_DIVISIONS = {
    KisOrderType.LIMIT: "00",
    KisOrderType.MARKET: "01",
}


@dataclass(frozen=True)
class KisOrderRequest:
    symbol: str
    side: KisOrderSide | str
    quantity: int
    price: float
    order_type: KisOrderType | str = KisOrderType.LIMIT
    exchange_id: str = "KRX"
    sell_type: str = ""
    condition_price: str = ""

    def __post_init__(self) -> None:
        side = KisOrderSide(self.side)
        order_type = KisOrderType(self.order_type)
        if not self.symbol.isdigit() or len(self.symbol) != 6:
            raise ValueError("symbol must be a six-digit domestic stock code")
        if isinstance(self.quantity, bool) or self.quantity <= 0:
            raise ValueError("quantity must be a positive integer")
        if order_type is KisOrderType.LIMIT and self.price <= 0:
            raise ValueError("limit orders require a positive price")
        if order_type is KisOrderType.MARKET and self.price < 0:
            raise ValueError("market order price must be zero or positive")
        if not self.exchange_id:
            raise ValueError("exchange_id is required")
        object.__setattr__(self, "side", side)
        object.__setattr__(self, "order_type", order_type)


@dataclass(frozen=True)
class KisOrderResult:
    symbol: str
    side: KisOrderSide
    quantity: int
    price: float
    order_type: KisOrderType
    tr_id: str
    broker_order_id: str | None
    raw: dict[str, Any]


def order_tr_id(environment: KisEnvironment | str, side: KisOrderSide | str) -> str:
    env = KisEnvironment.parse(environment) if isinstance(environment, str) else environment
    return TR_IDS[(env, KisOrderSide(side))]


def _format_int(value: float | int, field_name: str) -> str:
    if isinstance(value, bool) or int(value) != value:
        raise ValueError(f"{field_name} must be a whole number")
    return str(int(value))


def build_order_body(account_no: str, account_product_code: str,
                     request: KisOrderRequest) -> dict[str, str]:
    if not account_no.isdigit() or len(account_no) != 8:
        raise ValueError("KIS account_no must be exactly eight digits")
    if not account_product_code.isdigit() or len(account_product_code) != 2:
        raise ValueError("KIS account_product_code must be exactly two digits")
    order_price = 0 if request.order_type is KisOrderType.MARKET else request.price
    return {
        "CANO": account_no,
        "ACNT_PRDT_CD": account_product_code,
        "PDNO": request.symbol,
        "ORD_DVSN": ORDER_DIVISIONS[request.order_type],
        "ORD_QTY": _format_int(request.quantity, "quantity"),
        "ORD_UNPR": _format_int(order_price, "price"),
        "EXCG_ID_DVSN_CD": request.exchange_id,
        "SLL_TYPE": request.sell_type,
        "CNDT_PRIC": request.condition_price,
    }


class KisOrderClient:
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
        include_hashkey: bool = True,
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
        self.include_hashkey = include_hashkey

    def build_headers(self, request: KisOrderRequest, hashkey: str | None = None) -> dict[str, str]:
        headers = {
            "content-type": "application/json; charset=utf-8",
            "authorization": f"Bearer {self.access_token}",
            "appkey": self.app_key,
            "appsecret": self.app_secret,
            "tr_id": order_tr_id(self.environment, request.side),
            "custtype": "P",
        }
        if hashkey:
            headers["hashkey"] = hashkey
        return headers

    def issue_hashkey(self, body: dict[str, str]) -> str:
        response = self.session.post(
            api_url(self.environment, HASHKEY_PATH),
            headers={
                "content-type": "application/json; charset=utf-8",
                "appkey": self.app_key,
                "appsecret": self.app_secret,
            },
            data=json.dumps(body),
            timeout=self.timeout,
        )
        _raise_for_kis_response(response, (self.app_key, self.app_secret, self.access_token))
        hashkey = response.json().get("HASH")
        if not hashkey:
            raise ValueError("KIS hashkey response did not contain HASH")
        return str(hashkey)

    def submit_order(self, request: KisOrderRequest) -> KisOrderResult:
        body = build_order_body(self.account_no, self.account_product_code, request)
        hashkey = self.issue_hashkey(body) if self.include_hashkey else None
        headers = self.build_headers(request, hashkey)
        response = self.session.post(
            api_url(self.environment, ORDER_CASH_PATH),
            headers=headers,
            json=body,
            timeout=self.timeout,
        )
        _raise_for_kis_response(response, (self.app_key, self.app_secret, self.access_token))
        payload = response.json()
        if str(payload.get("rt_cd", "0")) != "0":
            raise RuntimeError(f"KIS order request failed: {payload.get('msg1', 'unknown error')}")
        output = payload.get("output") or {}
        if not isinstance(output, dict):
            output = {}
        broker_order_id = output.get("ODNO") or output.get("odno")
        return KisOrderResult(
            symbol=request.symbol,
            side=KisOrderSide(request.side),
            quantity=request.quantity,
            price=request.price,
            order_type=KisOrderType(request.order_type),
            tr_id=headers["tr_id"],
            broker_order_id=str(broker_order_id) if broker_order_id else None,
            raw=output,
        )
