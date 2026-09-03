from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

import pytest

from kis_ai_scalper import cli
from kis_ai_scalper.broker.kis_auth import KisHttpError
from kis_ai_scalper.broker.kis_rankings import (
    VOLUME_RANK_PATH,
    VOLUME_RANK_TR_ID,
    KisVolumeRankingClient,
)
from kis_ai_scalper.ops.auto_universe import (
    AUTO_UNIVERSE_ERROR_KEY,
    AUTO_UNIVERSE_SYMBOLS_KEY,
    AutoUniverseSettings,
    resolve_service_symbols,
)
from kis_ai_scalper.storage import connect_database


@dataclass
class FakeResponse:
    payload: dict
    status_code: int = 200

    def raise_for_status(self):
        return None

    def json(self):
        return self.payload


class FakeSession:
    def __init__(self, response):
        self.response = response
        self.calls = []

    def get(self, url, **kwargs):
        self.calls.append((url, kwargs))
        return self.response


def ranking_row(symbol, *, change="3.2", price="25,000", volume="1,200,000"):
    return {
        "mksc_shrn_iscd": symbol,
        "hts_kor_isnm": f"stock-{symbol}",
        "stck_prpr": price,
        "prdy_ctrt": change,
        "acml_vol": volume,
        "acml_tr_pbmn": "35000000000",
    }


def ranking_client(session):
    return KisVolumeRankingClient(
        "demo", "app-secret-key", "app-secret-value", "access-token", session=session,
    )


def test_volume_ranking_requests_trade_value_leaders_and_filters_unsafe_movers():
    session = FakeSession(FakeResponse({
        "rt_cd": "0",
        "output": [
            ranking_row("005930"),
            ranking_row("000660", change="0.2"),
            ranking_row("035420", change="21.0"),
            ranking_row("123456", price="900"),
            ranking_row("not-a-symbol"),
            ranking_row("005930", change="4.0"),
            ranking_row("068270", change="1.1"),
        ],
    }))

    ranked = ranking_client(session).get_active_stocks(limit=2)

    assert [stock.symbol for stock in ranked] == ["005930", "068270"]
    assert ranked[0].name == "stock-005930"
    assert ranked[0].change_pct == 3.2
    url, kwargs = session.calls[0]
    assert url.endswith(VOLUME_RANK_PATH)
    assert kwargs["headers"]["tr_id"] == VOLUME_RANK_TR_ID
    assert kwargs["params"]["FID_DIV_CLS_CODE"] == "1"
    assert kwargs["params"]["FID_BLNG_CLS_CODE"] == "3"
    assert kwargs["params"]["FID_TRGT_EXLS_CLS_CODE"] == "1111111111"


def test_volume_ranking_http_200_business_error_is_safely_redacted():
    session = FakeSession(FakeResponse({
        "rt_cd": "-1",
        "msg_cd": "EGW00123",
        "msg1": "app-secret-value access-token denied",
    }))

    with pytest.raises(KisHttpError) as caught:
        ranking_client(session).get_active_stocks()

    assert caught.value.status_code == 200
    assert caught.value.details["msg_cd"] == "EGW00123"
    assert "app-secret-value" not in str(caught.value)
    assert "access-token" not in str(caught.value)


def test_auto_universe_is_cached_merged_and_retained_during_refresh_failure(tmp_path):
    path = tmp_path / "universe.sqlite3"
    now = datetime(2026, 9, 3, 1, 0, tzinfo=timezone.utc)
    settings = AutoUniverseSettings(enabled=True, size=2, refresh_seconds=1800)
    calls = []

    def discover(_settings):
        calls.append("called")
        return ["005930", "068270"]

    with connect_database(path) as database:
        database.init_schema()
        first = resolve_service_symbols(
            database,
            base_symbols=["000660"],
            open_position_symbols=["035420"],
            discover=discover,
            settings=settings,
            now=now,
        )
        cached = resolve_service_symbols(
            database,
            base_symbols=["000660"],
            open_position_symbols=[],
            discover=lambda _settings: (_ for _ in ()).throw(AssertionError("cache missed")),
            settings=settings,
            now=now + timedelta(minutes=10),
        )
        failed = resolve_service_symbols(
            database,
            base_symbols=["000660"],
            open_position_symbols=[],
            discover=lambda _settings: (_ for _ in ()).throw(RuntimeError("network secret")),
            settings=settings,
            now=now + timedelta(minutes=31),
        )
        stored = database.get_runtime_metadata(AUTO_UNIVERSE_SYMBOLS_KEY)
        error = database.get_runtime_metadata(AUTO_UNIVERSE_ERROR_KEY)

    assert first == ["000660", "005930", "068270", "035420"]
    assert cached == ["000660", "005930", "068270"]
    assert failed == cached
    assert calls == ["called"]
    assert stored == '["005930", "068270"]'
    assert error == "RuntimeError"


def test_auto_universe_cache_accepts_service_naive_kst_clock(tmp_path):
    path = tmp_path / "naive-kst.sqlite3"
    now = datetime(2026, 9, 3, 10, 0)
    settings = AutoUniverseSettings(enabled=True, refresh_seconds=1800)
    calls = []

    with connect_database(path) as database:
        database.init_schema()
        first = resolve_service_symbols(
            database,
            base_symbols=[],
            open_position_symbols=[],
            discover=lambda _settings: calls.append("called") or ["005930"],
            settings=settings,
            now=now,
        )
        second = resolve_service_symbols(
            database,
            base_symbols=[],
            open_position_symbols=[],
            discover=lambda _settings: (_ for _ in ()).throw(AssertionError("cache missed")),
            settings=settings,
            now=now + timedelta(seconds=20),
        )

    assert first == second == ["005930"]
    assert calls == ["called"]


def test_disabled_auto_universe_never_calls_discovery(tmp_path):
    path = tmp_path / "disabled.sqlite3"
    settings = AutoUniverseSettings(enabled=False)
    with connect_database(path) as database:
        database.init_schema()
        symbols = resolve_service_symbols(
            database,
            base_symbols=["000660"],
            open_position_symbols=["035420"],
            discover=lambda _settings: (_ for _ in ()).throw(AssertionError("called")),
            settings=settings,
            now=datetime(2026, 9, 3, tzinfo=timezone.utc),
        )
    assert symbols == ["000660", "035420"]


def test_cli_resolves_watchlist_dynamic_and_open_positions_for_same_cycle(tmp_path, monkeypatch):
    path = tmp_path / "service-symbols.sqlite3"
    now = datetime(2026, 9, 3, 1, 0, tzinfo=timezone.utc)
    with connect_database(path) as database:
        database.init_schema()
        database.add_watchlist_symbol("000660")
        database.connection.execute(
            """INSERT INTO live_positions
               (position_id,signal_id,symbol,quantity,entry_price,stop_loss_price,
                take_profit_price,opened_at,status)
               VALUES (?,?,?,?,?,?,?,?,?)""",
            ("p1", "s1", "035420", 1, 100, 99, 102, now.isoformat(), "OPEN"),
        )
        database.connection.commit()

    monkeypatch.setattr(cli, "_discover_auto_universe", lambda *_args, **_kwargs: ["005930"])
    monkeypatch.setattr(
        cli,
        "_auto_universe_settings_from_env",
        lambda: AutoUniverseSettings(enabled=True, size=2, refresh_seconds=1800),
    )

    symbols = cli._resolve_service_symbols_for_cycle(
        "config/settings.yaml", "demo", str(path), None, False, now,
    )

    assert symbols == ["000660", "005930", "035420"]
