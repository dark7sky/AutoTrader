"""Read-only KIS REST market-data adapter."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import requests

from .kis_endpoints import KisEnvironment, api_url
from .kis_rate_limit import new_rate_limited_session


@dataclass(frozen=True)
class CurrentPrice:
    symbol: str
    price: float
    raw: dict[str, Any]


class KisRestClient:
    def __init__(self, environment: KisEnvironment | str, app_key: str, app_secret: str,
                 access_token: str, session: requests.Session | Any | None = None,
                 timeout: float = 15.0) -> None:
        self.environment = KisEnvironment.parse(environment) if isinstance(environment, str) else environment
        self.app_key = app_key
        self.app_secret = app_secret
        self.access_token = access_token
        self.session = session or new_rate_limited_session(self.environment)
        self.timeout = timeout

    def get_current_price(self, symbol: str) -> CurrentPrice:
        if not symbol.isdigit() or len(symbol) != 6:
            raise ValueError("domestic stock symbol must be a six-digit code")
        response = self.session.get(
            api_url(self.environment, "/uapi/domestic-stock/v1/quotations/inquire-price"),
            headers={"authorization": f"Bearer {self.access_token}", "appkey": self.app_key,
                     "appsecret": self.app_secret, "tr_id": "FHKST01010100", "custtype": "P"},
            params={"FID_COND_MRKT_DIV_CODE": "J", "FID_INPUT_ISCD": symbol},
            timeout=self.timeout,
        )
        response.raise_for_status()
        payload = response.json()
        if str(payload.get("rt_cd", "0")) != "0":
            raise RuntimeError(f"KIS quote request failed: {payload.get('msg1', 'unknown error')}")
        output = payload.get("output") or {}
        price_text = output.get("stck_prpr")
        if price_text in (None, ""):
            raise ValueError("KIS quote response did not contain output.stck_prpr")
        return CurrentPrice(symbol=symbol, price=float(price_text), raw=output)
