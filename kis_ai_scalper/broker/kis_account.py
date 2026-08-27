"""Read-only KIS domestic-stock account snapshot adapter."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping

import requests

from .kis_auth import _raise_for_kis_response
from .kis_endpoints import KisEnvironment, api_url
from .kis_rate_limit import new_rate_limited_session


BALANCE_PATH = "/uapi/domestic-stock/v1/trading/inquire-balance"
BALANCE_TR_IDS = {KisEnvironment.REAL: "TTTC8434R", KisEnvironment.DEMO: "VTTC8434R"}


@dataclass(frozen=True)
class KisAccountPosition:
    symbol: str
    qty: int
    sellable_qty: int | None
    avg_price: float | None
    current_price: float | None
    evaluation_pnl: float | None
    evaluation_amount: float | None = None
    name: str = ""

    @property
    def quantity(self) -> int:
        return self.qty

    @property
    def sellable_quantity(self) -> int | None:
        return self.sellable_qty

    @property
    def average_price(self) -> float | None:
        return self.avg_price


@dataclass(frozen=True)
class KisAccountSummary:
    deposit: float | None
    orderable_cash_estimate: float | None
    total_evaluation: float | None
    evaluation_pnl: float | None

    @property
    def cash_balance(self) -> float | None:
        return self.deposit

    @property
    def orderable_cash(self) -> float | None:
        return self.orderable_cash_estimate


@dataclass(frozen=True)
class KisAccountPagination:
    tr_cont: str = ""
    ctx_area_fk100: str = ""
    ctx_area_nk100: str = ""

    @property
    def has_next(self) -> bool:
        return self.tr_cont.upper() in {"M", "F"} and bool(
            self.ctx_area_fk100 or self.ctx_area_nk100
        )


@dataclass(frozen=True)
class KisAccountPage:
    positions: tuple[KisAccountPosition, ...]
    summary: KisAccountSummary
    pagination: KisAccountPagination


@dataclass(frozen=True)
class KisAccountSnapshot:
    positions: tuple[KisAccountPosition, ...]
    summary: KisAccountSummary


def _first(row: Mapping[str, Any], *names: str) -> Any:
    for name in names:
        value = row.get(name)
        if value not in (None, ""):
            return value
    return None


def _text(value: Any) -> str | None:
    if value in (None, ""):
        return None
    text = str(value).strip()
    return text or None


def _int_value(value: Any) -> int | None:
    if value in (None, ""):
        return None
    try:
        return int(float(str(value).replace(",", "").strip()))
    except (TypeError, ValueError):
        return None


def _float_value(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        parsed = float(str(value).replace(",", "").strip())
        return parsed if parsed == parsed else None
    except (TypeError, ValueError):
        return None


def _as_rows(value: Any) -> list[Mapping[str, Any]]:
    if isinstance(value, Mapping):
        return [value]
    if not isinstance(value, list):
        return []
    return [row for row in value if isinstance(row, Mapping)]


def parse_account_position(row: Any) -> KisAccountPosition | None:
    if not isinstance(row, Mapping):
        return None
    symbol = _text(_first(row, "pdno", "PDNO", "symbol", "SYMBOL"))
    qty = _int_value(_first(row, "hldg_qty", "HLDG_QTY", "qty", "quantity"))
    if not symbol or not symbol.isdigit() or len(symbol) != 6 or qty is None or qty <= 0:
        return None
    sellable_qty = _int_value(
        _first(row, "ord_psbl_qty", "ORD_PSBL_QTY", "sellable_qty", "sellable_quantity")
    )
    if sellable_qty is not None and sellable_qty < 0:
        sellable_qty = None
    avg_price = _float_value(_first(row, "pchs_avg_pric", "PCHS_AVG_PRIC", "avg_price"))
    current_price = _float_value(_first(row, "prpr", "PRPR", "current_price"))
    if avg_price is not None and avg_price < 0:
        avg_price = None
    if current_price is not None and current_price < 0:
        current_price = None
    return KisAccountPosition(
        symbol=symbol,
        qty=qty,
        sellable_qty=sellable_qty,
        avg_price=avg_price,
        current_price=current_price,
        evaluation_pnl=_float_value(
            _first(row, "evlu_pfls_amt", "EVLU_PFLS_AMT", "evaluation_pnl")
        ),
        evaluation_amount=_float_value(
            _first(row, "evlu_amt", "EVLU_AMT", "evaluation_amount")
        ),
        name=str(_first(row, "prdt_name", "PRDT_NAME", "name") or ""),
    )


def parse_account_summary(row: Any) -> KisAccountSummary:
    row = row if isinstance(row, Mapping) else {}
    return KisAccountSummary(
        deposit=_float_value(_first(row, "dnca_tot_amt", "DNCA_TOT_AMT", "deposit")),
        orderable_cash_estimate=_float_value(
            _first(row, "ord_psbl_cash", "ORD_PSBL_CASH", "ord_psbl_amt", "ORD_PSBL_AMT", "orderable_cash")
        ),
        total_evaluation=_float_value(
            _first(row, "tot_evlu_amt", "TOT_EVLU_AMT", "evlu_amt_smtl", "total_evaluation")
        ),
        evaluation_pnl=_float_value(
            _first(row, "evlu_pfls_smtl_amt", "EVLU_PFLS_SMTL_AMT", "evaluation_pnl")
        ),
    )


def _response_payload(response: Any, secrets: tuple[str, ...]) -> dict[str, Any]:
    _raise_for_kis_response(response, secrets)
    try:
        payload = response.json()
    except (ValueError, TypeError, AttributeError) as exc:
        raise RuntimeError("KIS account response was not valid JSON") from exc
    if not isinstance(payload, dict):
        raise RuntimeError("KIS account response was not an object")
    if str(payload.get("rt_cd", "1")) != "0":
        raise RuntimeError("KIS account request failed")
    return payload


def _header(response: Any, name: str) -> str:
    headers = getattr(response, "headers", {}) or {}
    return str(headers.get(name, headers.get(name.lower(), "")) or "")


def _pagination(payload: Mapping[str, Any], response: Any) -> KisAccountPagination:
    return KisAccountPagination(
        tr_cont=_header(response, "tr_cont"),
        ctx_area_fk100=str(payload.get("ctx_area_fk100") or payload.get("CTX_AREA_FK100") or ""),
        ctx_area_nk100=str(payload.get("ctx_area_nk100") or payload.get("CTX_AREA_NK100") or ""),
    )


class KisAccountClient:
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
        max_pages: int = 10,
    ) -> None:
        self.environment = KisEnvironment.parse(environment) if isinstance(environment, str) else environment
        self.app_key = app_key
        self.app_secret = app_secret
        self.access_token = access_token
        self.account_no = account_no
        self.account_product_code = account_product_code
        self.session = (
            session if session is not None else new_rate_limited_session(self.environment)
        )
        self.timeout = timeout
        self.max_pages = min(max_pages, 10)

    def _validate_account(self) -> None:
        if not self.account_no.isdigit() or len(self.account_no) != 8:
            raise ValueError("KIS account_no must be exactly eight digits")
        if not self.account_product_code.isdigit() or len(self.account_product_code) != 2:
            raise ValueError("KIS account_product_code must be exactly two digits")
        if self.max_pages <= 0:
            raise ValueError("max_pages must be positive")

    def get_page(self, pagination: KisAccountPagination | None = None) -> KisAccountPage:
        self._validate_account()
        pagination = pagination or KisAccountPagination()
        response = self.session.get(
            api_url(self.environment, BALANCE_PATH),
            headers={
                "content-type": "application/json; charset=utf-8",
                "authorization": f"Bearer {self.access_token}",
                "appkey": self.app_key,
                "appsecret": self.app_secret,
                "tr_id": BALANCE_TR_IDS[self.environment],
                "custtype": "P",
                "tr_cont": "N" if pagination.ctx_area_fk100 or pagination.ctx_area_nk100 else "",
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
                "CTX_AREA_FK100": pagination.ctx_area_fk100,
                "CTX_AREA_NK100": pagination.ctx_area_nk100,
            },
            timeout=self.timeout,
        )
        payload = _response_payload(
            response,
            (self.app_key, self.app_secret, self.access_token, self.account_no),
        )
        positions = tuple(
            position
            for row in _as_rows(payload.get("output1"))
            if (position := parse_account_position(row)) is not None
        )
        summary_rows = _as_rows(payload.get("output2"))
        summary = parse_account_summary(summary_rows[0] if summary_rows else {})
        return KisAccountPage(positions, summary, _pagination(payload, response))

    def get_snapshot(self) -> KisAccountSnapshot:
        positions: list[KisAccountPosition] = []
        summary: KisAccountSummary | None = None
        pagination = KisAccountPagination()
        seen: set[tuple[str, str]] = set()
        for _ in range(self.max_pages):
            page = self.get_page(pagination)
            positions.extend(page.positions)
            summary = summary or page.summary
            if not page.pagination.has_next:
                break
            marker = (page.pagination.ctx_area_fk100, page.pagination.ctx_area_nk100)
            if marker in seen:
                raise RuntimeError("KIS account pagination cursor did not advance")
            seen.add(marker)
            pagination = page.pagination
        else:
            raise RuntimeError("KIS account pagination exceeded max_pages")
        return KisAccountSnapshot(tuple(positions), summary or parse_account_summary({}))

    fetch_snapshot = get_snapshot
    get_account_snapshot = get_snapshot


__all__ = [
    "BALANCE_PATH",
    "BALANCE_TR_IDS",
    "KisAccountClient",
    "KisAccountPage",
    "KisAccountPagination",
    "KisAccountPosition",
    "KisAccountSummary",
    "KisAccountSnapshot",
    "parse_account_position",
    "parse_account_summary",
]
