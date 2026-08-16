"""KIS domestic-stock order status and cancellation adapter.

The implementation follows the official ``inquire-daily-ccld`` and
``order-rvsecncl`` examples.  It deliberately keeps the HTTP session
injectable so the read and write paths can be tested without a broker call.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from enum import StrEnum
from typing import Any, Mapping

import requests

from .kis_auth import _raise_for_kis_response
from .kis_endpoints import KisEnvironment, api_url
from .kis_order import KisOrderSide


DAILY_ORDER_PATH = "/uapi/domestic-stock/v1/trading/inquire-daily-ccld"
ORDER_REVISE_CANCEL_PATH = "/uapi/domestic-stock/v1/trading/order-rvsecncl"
REVISION_CANCELABLE_PATH = "/uapi/domestic-stock/v1/trading/inquire-psbl-rvsecncl"

DAILY_ORDER_TR_IDS = {
    (KisEnvironment.REAL, "inner"): "TTTC0081R",
    (KisEnvironment.DEMO, "inner"): "VTTC0081R",
    (KisEnvironment.REAL, "before"): "CTSC9215R",
    (KisEnvironment.DEMO, "before"): "VTSC9215R",
}
REVISION_CANCEL_TR_IDS = {
    KisEnvironment.REAL: "TTTC0013U",
    KisEnvironment.DEMO: "VTTC0013U",
}
REVISION_CANCELABLE_TR_IDS = {
    KisEnvironment.REAL: "TTTC0084R",
    KisEnvironment.DEMO: "VTTC0084R",
}


class KisOrderStatus(StrEnum):
    UNKNOWN = "unknown"
    UNFILLED = "unfilled"
    PARTIALLY_FILLED = "partially_filled"
    FILLED = "filled"
    CANCELLED = "cancelled"
    REJECTED = "rejected"


@dataclass(frozen=True)
class KisPaginationContext:
    tr_cont: str = ""
    ctx_area_fk100: str = ""
    ctx_area_nk100: str = ""

    @property
    def has_next(self) -> bool:
        return self.tr_cont in {"M", "F"} and bool(self.ctx_area_fk100 or self.ctx_area_nk100)

    @property
    def next_request(self) -> "KisPaginationContext":
        return KisPaginationContext("N", self.ctx_area_fk100, self.ctx_area_nk100)


@dataclass(frozen=True)
class KisOrderStatusRecord:
    order_number: str
    symbol: str
    side: KisOrderSide | None
    ordered_quantity: int
    filled_quantity: int
    remaining_quantity: int
    order_price: float | None
    average_fill_price: float | None
    status: KisOrderStatus
    order_time: str | None
    order_date: str | None = None
    order_branch: str | None = None
    original_order_number: str | None = None
    exchange_id: str | None = None
    raw: Mapping[str, Any] | None = None


@dataclass(frozen=True)
class KisOrderStatusPage:
    orders: tuple[KisOrderStatusRecord, ...]
    pagination: KisPaginationContext


@dataclass(frozen=True)
class KisCancelResult:
    order_number: str | None
    original_order_number: str
    status: str
    tr_id: str
    raw: Mapping[str, Any]


def _first(row: Mapping[str, Any], *names: str) -> Any:
    for name in names:
        value = row.get(name)
        if value not in (None, ""):
            return value
    return None


def _text(value: Any) -> str | None:
    if value in (None, ""):
        return None
    return str(value).strip() or None


def _int_value(value: Any) -> int:
    if value in (None, ""):
        return 0
    try:
        parsed = int(float(str(value).replace(",", "").strip()))
    except (TypeError, ValueError):
        return 0
    return max(0, parsed)


def _float_value(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(str(value).replace(",", "").strip())
    except (TypeError, ValueError):
        return None


def _flag(value: Any) -> bool:
    return str(value or "").strip().upper() in {"Y", "1", "TRUE", "REJECT", "REJECTED"}


def _parse_side(row: Mapping[str, Any]) -> KisOrderSide | None:
    value = str(_first(row, "sll_buy_dvsn_cd", "SLL_BUY_DVSN_CD", "side", "SIDE") or "").lower()
    if value in {"01", "sell", "s", "매도"}:
        return KisOrderSide.SELL
    if value in {"02", "buy", "b", "매수"}:
        return KisOrderSide.BUY
    return None


def _parse_status(row: Mapping[str, Any], ordered: int, filled: int, remaining: int) -> KisOrderStatus:
    direct = str(_first(row, "order_status", "ord_status", "status", "STATUS") or "").lower()
    direct_status = {
        "filled": KisOrderStatus.FILLED,
        "체결": KisOrderStatus.FILLED,
        "partial": KisOrderStatus.PARTIALLY_FILLED,
        "partially_filled": KisOrderStatus.PARTIALLY_FILLED,
        "부분체결": KisOrderStatus.PARTIALLY_FILLED,
        "cancelled": KisOrderStatus.CANCELLED,
        "취소": KisOrderStatus.CANCELLED,
        "rejected": KisOrderStatus.REJECTED,
        "거부": KisOrderStatus.REJECTED,
    }
    if direct in direct_status:
        return direct_status[direct]
    if _int_value(_first(row, "rjct_qty", "reject_qty", "RJCT_QTY")) > 0 or _flag(_first(row, "rjct_yn", "reject_yn")):
        return KisOrderStatus.REJECTED
    if _int_value(_first(row, "cncl_cfrm_qty", "cancel_qty", "CNCL_CFRM_QTY")) > 0 and remaining == 0:
        return KisOrderStatus.CANCELLED
    if ordered > 0 and filled >= ordered and remaining == 0:
        return KisOrderStatus.FILLED
    if filled > 0:
        return KisOrderStatus.PARTIALLY_FILLED
    if remaining > 0 or ordered > 0:
        return KisOrderStatus.UNFILLED
    return KisOrderStatus.UNKNOWN


def parse_order_status(row: Any) -> KisOrderStatusRecord | None:
    """Parse one KIS output row; malformed rows are ignored conservatively."""
    if not isinstance(row, Mapping):
        return None
    order_number = _text(_first(row, "odno", "ODNO", "order_number"))
    symbol = _text(_first(row, "pdno", "PDNO", "symbol"))
    if not order_number or not symbol or not symbol.isdigit() or len(symbol) != 6:
        return None
    ordered = _int_value(_first(row, "ord_qty", "ORD_QTY", "ordered_quantity"))
    filled = _int_value(_first(row, "tot_ccld_qty", "ccld_qty", "filled_qty", "TOT_CCLD_QTY"))
    explicit_remaining = _first(row, "rmn_qty", "remaining_qty", "RMN_QTY")
    remaining = _int_value(explicit_remaining) if explicit_remaining not in (None, "") else max(0, ordered - filled)
    return KisOrderStatusRecord(
        order_number=order_number,
        symbol=symbol,
        side=_parse_side(row),
        ordered_quantity=ordered,
        filled_quantity=filled,
        remaining_quantity=remaining,
        order_price=_float_value(_first(row, "ord_unpr", "order_price", "ORD_UNPR")),
        average_fill_price=_float_value(_first(row, "avg_prvs", "avg_fill_price", "avg_pric", "AVG_PRVS")),
        status=_parse_status(row, ordered, filled, remaining),
        order_time=_text(_first(row, "ord_tmd", "order_time", "ORD_TMD")),
        order_date=_text(_first(row, "ord_dt", "order_date", "ORD_DT")),
        order_branch=_text(_first(row, "ord_gno_brno", "order_branch", "ORD_GNO_BRNO")),
        original_order_number=_text(_first(row, "orgn_odno", "original_order_number", "ORGN_ODNO")),
        exchange_id=_text(_first(row, "excg_id_dvsn_cd", "exchange_id", "EXCG_ID_DVSN_CD")),
        raw=dict(row),
    )


def _header(response: Any, name: str) -> str:
    headers = getattr(response, "headers", {}) or {}
    if hasattr(headers, "get"):
        return str(headers.get(name, headers.get(name.lower(), "")) or "")
    return ""


def _payload(response: Any, secret_values: tuple[str, ...]) -> dict[str, Any]:
    _raise_for_kis_response(response, secret_values)
    try:
        payload = response.json()
    except (ValueError, TypeError, AttributeError) as exc:
        raise RuntimeError("KIS response was not valid JSON") from exc
    if not isinstance(payload, dict):
        raise RuntimeError("KIS response was not an object")
    if str(payload.get("rt_cd", "1")) != "0":
        raise RuntimeError("KIS order status request failed")
    return payload


def _today() -> str:
    kst = timezone(timedelta(hours=9))
    return datetime.now(kst).strftime("%Y%m%d")


class KisOrderStatusClient:
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

    def _validate_account(self) -> None:
        if not self.account_no.isdigit() or len(self.account_no) != 8:
            raise ValueError("KIS account_no must be exactly eight digits")
        if not self.account_product_code.isdigit() or len(self.account_product_code) != 2:
            raise ValueError("KIS account_product_code must be exactly two digits")

    def _headers(self, tr_id: str, tr_cont: str = "") -> dict[str, str]:
        headers = {
            "content-type": "application/json; charset=utf-8",
            "authorization": f"Bearer {self.access_token}",
            "appkey": self.app_key,
            "appsecret": self.app_secret,
            "tr_id": tr_id,
            "custtype": "P",
        }
        if tr_cont:
            headers["tr_cont"] = tr_cont
        return headers

    def get_orders_page(
        self,
        inquiry_date: str | None = None,
        *,
        unfilled_only: bool = False,
        pagination: KisPaginationContext | None = None,
        pd_dv: str = "inner",
        exchange_id: str = "KRX",
    ) -> KisOrderStatusPage:
        self._validate_account()
        inquiry_date = inquiry_date or _today()
        if not inquiry_date.isdigit() or len(inquiry_date) != 8:
            raise ValueError("inquiry_date must be YYYYMMDD")
        if pd_dv not in {"inner", "before"}:
            raise ValueError("pd_dv must be 'inner' or 'before'")
        context = pagination or KisPaginationContext()
        params = {
            "CANO": self.account_no,
            "ACNT_PRDT_CD": self.account_product_code,
            "INQR_STRT_DT": inquiry_date,
            "INQR_END_DT": inquiry_date,
            "SLL_BUY_DVSN_CD": "00",
            "PDNO": "",
            "CCLD_DVSN": "02" if unfilled_only else "00",
            "INQR_DVSN": "00",
            "INQR_DVSN_3": "00",
            "ORD_GNO_BRNO": "",
            "ODNO": "",
            "INQR_DVSN_1": "",
            "CTX_AREA_FK100": context.ctx_area_fk100,
            "CTX_AREA_NK100": context.ctx_area_nk100,
            "EXCG_ID_DVSN_CD": exchange_id,
        }
        response = self.session.get(
            api_url(self.environment, DAILY_ORDER_PATH),
            headers=self._headers(DAILY_ORDER_TR_IDS[(self.environment, pd_dv)], context.tr_cont),
            params=params,
            timeout=self.timeout,
        )
        payload = _payload(
            response,
            (self.app_key, self.app_secret, self.access_token, self.account_no),
        )
        rows = payload.get("output1") or []
        if not isinstance(rows, list):
            rows = []
        orders = tuple(parsed for row in rows if (parsed := parse_order_status(row)) is not None)
        next_context = KisPaginationContext(
            tr_cont=_header(response, "tr_cont"),
            ctx_area_fk100=str(_first(payload, "ctx_area_fk100", "CTX_AREA_FK100") or ""),
            ctx_area_nk100=str(_first(payload, "ctx_area_nk100", "CTX_AREA_NK100") or ""),
        )
        if unfilled_only:
            orders = tuple(order for order in orders if order.remaining_quantity > 0)
        return KisOrderStatusPage(orders, next_context)

    def get_orders(
        self,
        inquiry_date: str | None = None,
        *,
        unfilled_only: bool = False,
        pd_dv: str = "inner",
        exchange_id: str = "KRX",
        max_pages: int = 10,
    ) -> tuple[KisOrderStatusRecord, ...]:
        if max_pages <= 0:
            raise ValueError("max_pages must be positive")
        context: KisPaginationContext | None = None
        result: list[KisOrderStatusRecord] = []
        for _ in range(max_pages):
            page = self.get_orders_page(
                inquiry_date,
                unfilled_only=unfilled_only,
                pagination=context,
                pd_dv=pd_dv,
                exchange_id=exchange_id,
            )
            result.extend(page.orders)
            if not page.pagination.has_next:
                return tuple(result)
            next_context = page.pagination.next_request
            if next_context == context:
                raise RuntimeError("KIS pagination did not advance")
            context = next_context
        raise RuntimeError("KIS order status pagination exceeded max_pages")

    def get_today_orders(self, *, unfilled_only: bool = False, max_pages: int = 10) -> tuple[KisOrderStatusRecord, ...]:
        return self.get_orders(unfilled_only=unfilled_only, max_pages=max_pages)

    def get_unfilled_orders(self, *, inquiry_date: str | None = None, max_pages: int = 10) -> tuple[KisOrderStatusRecord, ...]:
        return self.get_orders(inquiry_date, unfilled_only=True, max_pages=max_pages)

    def get_cancelable_orders_page(
        self,
        *,
        side: str = "0",
        pagination: KisPaginationContext | None = None,
    ) -> KisOrderStatusPage:
        self._validate_account()
        if side not in {"0", "1", "2"}:
            raise ValueError("side must be 0, 1, or 2")
        context = pagination or KisPaginationContext()
        response = self.session.get(
            api_url(self.environment, REVISION_CANCELABLE_PATH),
            headers=self._headers(REVISION_CANCELABLE_TR_IDS[self.environment], context.tr_cont),
            params={
                "CANO": self.account_no,
                "ACNT_PRDT_CD": self.account_product_code,
                "INQR_DVSN_1": "0",
                "INQR_DVSN_2": side,
                "CTX_AREA_FK100": context.ctx_area_fk100,
                "CTX_AREA_NK100": context.ctx_area_nk100,
            },
            timeout=self.timeout,
        )
        payload = _payload(
            response,
            (self.app_key, self.app_secret, self.access_token, self.account_no),
        )
        rows = payload.get("output") or payload.get("output1") or []
        if not isinstance(rows, list):
            rows = []
        next_context = KisPaginationContext(
            _header(response, "tr_cont"),
            str(_first(payload, "ctx_area_fk100", "CTX_AREA_FK100") or ""),
            str(_first(payload, "ctx_area_nk100", "CTX_AREA_NK100") or ""),
        )
        return KisOrderStatusPage(
            tuple(parsed for row in rows if (parsed := parse_order_status(row)) is not None),
            next_context,
        )

    def cancel_order(
        self,
        original_order_number: str,
        krx_forward_order_orgno: str,
        *,
        quantity: int | None = None,
        order_price: int = 0,
        order_division: str = "00",
        exchange_id: str = "KRX",
        condition_price: int = 0,
    ) -> KisCancelResult:
        self._validate_account()
        if not original_order_number or not krx_forward_order_orgno:
            raise ValueError("original_order_number and krx_forward_order_orgno are required")
        if quantity is not None and (isinstance(quantity, bool) or quantity <= 0):
            raise ValueError("quantity must be positive when supplied")
        if order_price < 0 or condition_price < 0:
            raise ValueError("prices must be non-negative")
        all_remaining = quantity is None
        body = {
            "CANO": self.account_no,
            "ACNT_PRDT_CD": self.account_product_code,
            "KRX_FWDG_ORD_ORGNO": krx_forward_order_orgno,
            "ORGN_ODNO": original_order_number,
            "ORD_DVSN": order_division,
            "RVSE_CNCL_DVSN_CD": "02",
            "ORD_QTY": str(quantity if quantity is not None else 0),
            "ORD_UNPR": str(int(order_price)),
            "QTY_ALL_ORD_YN": "Y" if all_remaining else "N",
            "EXCG_ID_DVSN_CD": exchange_id,
        }
        if condition_price:
            body["CNDT_PRIC"] = str(int(condition_price))
        tr_id = REVISION_CANCEL_TR_IDS[self.environment]
        response = self.session.post(
            api_url(self.environment, ORDER_REVISE_CANCEL_PATH),
            headers=self._headers(tr_id),
            json=body,
            timeout=self.timeout,
        )
        payload = _payload(
            response,
            (self.app_key, self.app_secret, self.access_token, self.account_no),
        )
        output = payload.get("output") or {}
        if not isinstance(output, dict):
            output = {}
        return KisCancelResult(
            order_number=_text(_first(output, "ODNO", "odno")),
            original_order_number=original_order_number,
            status="cancel_requested",
            tr_id=tr_id,
            raw=dict(output),
        )

    def cancel_unfilled_order(self, original_order_number: str, krx_forward_order_orgno: str, **kwargs: Any) -> KisCancelResult:
        return self.cancel_order(original_order_number, krx_forward_order_orgno, **kwargs)
