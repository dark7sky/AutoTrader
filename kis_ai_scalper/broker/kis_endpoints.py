"""KIS API environments and endpoint construction."""

from __future__ import annotations

from enum import StrEnum


class KisEnvironment(StrEnum):
    DEMO = "demo"
    REAL = "real"

    @classmethod
    def parse(cls, value: str) -> "KisEnvironment":
        try:
            return cls(value.lower())
        except ValueError as exc:
            raise ValueError("KIS environment must be 'demo' or 'real'") from exc


BASE_URLS = {
    KisEnvironment.REAL: "https://openapi.koreainvestment.com:9443",
    KisEnvironment.DEMO: "https://openapivts.koreainvestment.com:29443",
}

WEBSOCKET_URLS = {
    KisEnvironment.REAL: "ws://ops.koreainvestment.com:21000",
    KisEnvironment.DEMO: "ws://ops.koreainvestment.com:31000",
}


def base_url(environment: KisEnvironment | str) -> str:
    env = environment if isinstance(environment, KisEnvironment) else KisEnvironment.parse(environment)
    return BASE_URLS[env]


def api_url(environment: KisEnvironment | str, path: str) -> str:
    return f"{base_url(environment).rstrip('/')}/{path.lstrip('/')}"


def websocket_url(environment: KisEnvironment | str) -> str:
    env = environment if isinstance(environment, KisEnvironment) else KisEnvironment.parse(environment)
    return WEBSOCKET_URLS[env]
