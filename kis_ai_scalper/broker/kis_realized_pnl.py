"""Read-only KIS domestic-stock realized-P/L adapter."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping

import requests

from .kis_auth import _raise_for_kis_response
from .kis_endpoints import KisEnvironment, api_url
from .kis_rate_limit import new_rate_limited_session


REALIZED_PNL_PATH = "/uapi/domestic-stock/v1/trading/inquire-balance-rlz-pl"
REALIZED_PNL_TR_IDS = {
    KisEnvironment.REAL: "TTTC8494R",
}


class KisRealizedPnlUnsupportedError(RuntimeError):
    """Raised when KIS does not document this query for an environment."""


@dataclass(frozen=True)
class KisRealizedPnlSnapshot:
    daily_realized_pnl: float | None
    raw: Mapping[str, Any]

    @property
    def realized_pnl(self) -> float | None:
        return self.daily_realized_pnl


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
    return parsed if parsed == parsed else None


def _rows(value: Any) -> list[Mapping[str, Any]]:
    if isinstance(value, Mapping):
        return [value]
    if isinstance(value, list):
        return [item for item in value if isinstance(item, Mapping)]
    return []


def parse_realized_pnl(payload: Mapping[str, Any]) -> KisRealizedPnlSnapshot:
    """Parse only explicit realized-P/L fields; evaluation P/L is never a fallback."""
    if not isinstance(payload, Mapping):
        raise TypeError("KIS realized-P/L payload must be a mapping")
    output2 = _rows(payload.get("output2"))
    summary = output2[0] if output2 else {}
    value = _number(
        _first(summary, "rlzt_pfls", "RLZT_PFLS", "tot_rlzt_pfls", "TOT_RLZT_PFLS", "realized_pnl")
    )
    if value is None:
        # Some response variants expose the explicit aggregate in output1.
        values = [
            _number(_first(row, "rlzt_pfls", "RLZT_PFLS", "realized_pnl"))
            for row in _rows(payload.get("output1"))
        ]
        if values and all(item is not None for item in values):
            value = sum(item for item in values if item is not None)
    return KisRealizedPnlSnapshot(value, dict(summary))


def _header(response: Any, name: str) -> str:
    headers = getattr(response, "headers", {}) or {}
    return str(headers.get(name, headers.get(name.lower(), "")) or "")


class KisRealizedPnlClient:
    """Read-only daily realized-P/L query for documented KIS environments."""

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

    def _validate(self) -> None:
        if self.environment not in REALIZED_PNL_TR_IDS:
            raise KisRealizedPnlUnsupportedError(
                "KIS realized-P/L query is not officially documented for demo accounts"
            )
        if not self.account_no.isdigit() or len(self.account_no) != 8:
            raise ValueError("KIS account_no must be exactly eight digits")
        if not self.account_product_code.isdigit() or len(self.account_product_code) != 2:
            raise ValueError("KIS account_product_code must be exactly two digits")
        if self.max_pages <= 0:
            raise ValueError("max_pages must be positive")

    def get_snapshot(self) -> KisRealizedPnlSnapshot:
        self._validate()
        fk100 = nk100 = ""
        seen: set[tuple[str, str]] = set()
        values: list[float] = []
        raw: Mapping[str, Any] = {}
        for _ in range(self.max_pages):
            response = self.session.get(
                api_url(self.environment, REALIZED_PNL_PATH),
                headers={
                    "content-type": "application/json; charset=utf-8",
                    "authorization": f"Bearer {self.access_token}",
                    "appkey": self.app_key,
                    "appsecret": self.app_secret,
                    "tr_id": REALIZED_PNL_TR_IDS[self.environment],
                    "custtype": "P",
                    "tr_cont": "N" if fk100 or nk100 else "",
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
                    "COST_ICLD_YN": "N",
                    "CTX_AREA_FK100": fk100,
                    "CTX_AREA_NK100": nk100,
                },
                timeout=self.timeout,
            )
            _raise_for_kis_response(response, (self.app_key, self.app_secret, self.access_token))
            payload = response.json()
            if not isinstance(payload, Mapping) or str(payload.get("rt_cd", "0")) != "0":
                raise RuntimeError("KIS realized-P/L request failed")
            page = parse_realized_pnl(payload)
            if page.daily_realized_pnl is not None:
                values.append(page.daily_realized_pnl)
            raw = page.raw
            next_cursor = (
                str(payload.get("ctx_area_fk100") or ""),
                str(payload.get("ctx_area_nk100") or ""),
            )
            if _header(response, "tr_cont").upper() not in {"M", "F"}:
                break
            if next_cursor in seen or not any(next_cursor):
                raise RuntimeError("KIS realized-P/L pagination cursor did not advance")
            seen.add(next_cursor)
            fk100, nk100 = next_cursor
        else:
            raise RuntimeError("KIS realized-P/L pagination exceeded max_pages")
        # output2 is an aggregate. If a broker returns it on multiple pages,
        # summing the explicit aggregates would double-count; use the last one.
        return KisRealizedPnlSnapshot(values[-1] if values else None, raw)

    fetch = get_snapshot
    get = get_snapshot


__all__ = [
    "KisRealizedPnlClient",
    "KisRealizedPnlSnapshot",
    "KisRealizedPnlUnsupportedError",
    "REALIZED_PNL_PATH",
    "REALIZED_PNL_TR_IDS",
    "parse_realized_pnl",
]
