"""Read-only KIS ranking adapter used to discover liquid intraday stocks."""

from __future__ import annotations

from dataclasses import dataclass
import math
from typing import Any, Mapping

import requests

from .kis_auth import KisHttpError, _raise_for_kis_response
from .kis_endpoints import KisEnvironment, api_url
from .kis_rate_limit import new_rate_limited_session


VOLUME_RANK_PATH = "/uapi/domestic-stock/v1/quotations/volume-rank"
VOLUME_RANK_TR_ID = "FHPST01710000"


@dataclass(frozen=True)
class RankedStock:
    symbol: str
    name: str
    price: float
    change_pct: float
    volume: int
    trade_amount: float


def _number(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        parsed = float(str(value).replace(",", "").strip())
    except (TypeError, ValueError):
        return None
    return parsed if math.isfinite(parsed) else None


def _safe_payload(response: Any, secrets: tuple[str, ...]) -> dict[str, Any]:
    _raise_for_kis_response(response, secrets)
    try:
        payload = response.json()
    except (ValueError, TypeError, AttributeError) as exc:
        raise RuntimeError("KIS ranking response was not valid JSON") from exc
    if not isinstance(payload, dict):
        raise RuntimeError("KIS ranking response was not an object")
    if str(payload.get("rt_cd", "1")) != "0":
        details: dict[str, str] = {}
        for key in ("rt_cd", "msg_cd", "msg1", "error_description"):
            value = payload.get(key)
            if value in (None, ""):
                continue
            safe_value = str(value)
            for secret in secrets:
                if secret:
                    safe_value = safe_value.replace(secret, "[redacted]")
            details[key] = safe_value[:300]
        raise KisHttpError(int(getattr(response, "status_code", 200)), details)
    return payload


def _parse_ranked_stock(row: Any) -> RankedStock | None:
    if not isinstance(row, Mapping):
        return None
    symbol = str(row.get("mksc_shrn_iscd") or row.get("stck_shrn_iscd") or "").strip()
    price = _number(row.get("stck_prpr"))
    change_pct = _number(row.get("prdy_ctrt"))
    volume = _number(row.get("acml_vol"))
    trade_amount = _number(row.get("acml_tr_pbmn")) or 0.0
    if not symbol.isdigit() or len(symbol) != 6:
        return None
    if price is None or change_pct is None or volume is None:
        return None
    return RankedStock(
        symbol=symbol,
        name=str(row.get("hts_kor_isnm") or symbol).strip() or symbol,
        price=price,
        change_pct=change_pct,
        volume=max(0, int(volume)),
        trade_amount=max(0.0, trade_amount),
    )


class KisVolumeRankingClient:
    def __init__(
        self,
        environment: KisEnvironment | str,
        app_key: str,
        app_secret: str,
        access_token: str,
        session: requests.Session | Any | None = None,
        timeout: float = 15.0,
    ) -> None:
        self.environment = (
            KisEnvironment.parse(environment) if isinstance(environment, str) else environment
        )
        self.app_key = app_key
        self.app_secret = app_secret
        self.access_token = access_token
        self.session = session or new_rate_limited_session(self.environment)
        self.timeout = timeout

    def get_active_stocks(
        self,
        *,
        limit: int = 5,
        min_price: float = 2_000,
        max_price: float = 200_000,
        min_volume: int = 100_000,
        min_change_pct: float = 0.5,
        max_change_pct: float = 20.0,
    ) -> tuple[RankedStock, ...]:
        if limit <= 0:
            raise ValueError("limit must be positive")
        if min_price < 0 or max_price <= min_price:
            raise ValueError("price range is invalid")
        if min_volume < 0:
            raise ValueError("min_volume must not be negative")
        if max_change_pct <= min_change_pct:
            raise ValueError("change range is invalid")
        response = self.session.get(
            api_url(self.environment, VOLUME_RANK_PATH),
            headers={
                "content-type": "application/json; charset=utf-8",
                "authorization": f"Bearer {self.access_token}",
                "appkey": self.app_key,
                "appsecret": self.app_secret,
                "tr_id": VOLUME_RANK_TR_ID,
                "custtype": "P",
            },
            params={
                "FID_COND_MRKT_DIV_CODE": "J",
                "FID_COND_SCR_DIV_CODE": "20171",
                "FID_INPUT_ISCD": "0000",
                "FID_DIV_CLS_CODE": "1",
                "FID_BLNG_CLS_CODE": "3",
                "FID_TRGT_CLS_CODE": "111111111",
                "FID_TRGT_EXLS_CLS_CODE": "1111111111",
                "FID_INPUT_PRICE_1": str(int(min_price)),
                "FID_INPUT_PRICE_2": str(int(max_price)),
                "FID_VOL_CNT": str(int(min_volume)),
                "FID_INPUT_DATE_1": "",
            },
            timeout=self.timeout,
        )
        payload = _safe_payload(
            response, (self.app_key, self.app_secret, self.access_token),
        )
        rows = payload.get("output") or payload.get("output1") or []
        if not isinstance(rows, list):
            return ()
        selected: list[RankedStock] = []
        seen: set[str] = set()
        for row in rows:
            stock = _parse_ranked_stock(row)
            if stock is None or stock.symbol in seen:
                continue
            if not min_price <= stock.price <= max_price:
                continue
            if stock.volume < min_volume:
                continue
            if not min_change_pct <= stock.change_pct <= max_change_pct:
                continue
            seen.add(stock.symbol)
            selected.append(stock)
            if len(selected) >= limit:
                break
        return tuple(selected)
