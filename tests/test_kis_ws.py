import json
from datetime import datetime
from datetime import date
import pytest

import kis_ai_scalper.cli as cli
from kis_ai_scalper.broker.kis_endpoints import websocket_url
from kis_ai_scalper.broker.kis_ws import (
    TR_REALTIME_PRICE,
    build_subscription,
    is_pingpong,
    is_subscription_ack,
    parse_realtime_price,
    parse_system_message,
    realtime_price_to_market_tick,
    RealtimePrice,
    WebSocketSmokeResult,
)
from kis_ai_scalper.market.bar_builder import MinuteBarBuilder
from kis_ai_scalper.market.tick import MarketTick
from kis_ai_scalper.cli import smoke_ws


def test_subscription_message_shape_and_endpoint():
    payload = json.loads(build_subscription("approval-secret", "005930"))
    assert payload == {
        "header": {"approval_key": "approval-secret", "custtype": "P", "tr_type": "1", "content-type": "utf-8"},
        "body": {"input": {"tr_id": TR_REALTIME_PRICE, "tr_key": "005930"}},
    }
    assert websocket_url("demo") == "ws://ops.koreainvestment.com:31000"


def test_realtime_price_parser_extracts_standard_fields():
    fields = [
        "005930", "123456", "71200", "2", "100", "0.14", "71100",
        "70000", "71300", "69900", "71250", "71150", "37", "123456",
    ]
    raw = "0|H0STCNT0|001|" + "^".join(fields)
    tick = parse_realtime_price(raw)
    assert tick is not None
    assert (tick.symbol, tick.timestamp, tick.price, tick.volume, tick.total_volume) == ("005930", "123456", 71200.0, 37, 123456)


def test_realtime_price_converts_hhmmss_using_collection_date():
    tick = realtime_price_to_market_tick(RealtimePrice("005930", "091530", 71200, 4, 20), date(2026, 8, 15))
    assert tick.timestamp == datetime(2026, 8, 15, 9, 15, 30)


def test_malformed_realtime_and_system_messages_are_safe():
    assert parse_realtime_price("0|H0STCNT0|001|005930^bad") is None
    assert parse_realtime_price("1|H0STCNT0|001|005930^123456^71200") is None
    ack = parse_system_message('{"header":{"tr_id":"H0STCNT0","msg":"SUBSCRIBE SUCCESS"}}')
    ping = parse_system_message('{"header":{"tr_id":"PINGPONG"}}')
    assert is_subscription_ack(ack)
    assert is_pingpong(ping)
    assert parse_system_message("not-json") is None


def test_minute_bar_builder_is_deterministic():
    builder = MinuteBarBuilder()
    assert builder.update(MarketTick("005930", datetime(2026, 8, 15, 9, 0, 1), 100, 2)) is None
    assert builder.update(MarketTick("005930", datetime(2026, 8, 15, 9, 0, 30), 105, 3)) is None
    bar = builder.update(MarketTick("005930", datetime(2026, 8, 15, 9, 1, 0), 98, 4))
    assert bar is not None
    assert (bar.open, bar.high, bar.low, bar.close, bar.volume) == (100, 105, 100, 105, 5)
    assert builder.flush().close == 98


def test_cli_rejects_seconds_outside_bounded_smoke_window():
    with pytest.raises(ValueError, match="between 1 and 60"):
        smoke_ws("config/settings.yaml", "demo", "005930", 61)


def test_cli_reports_rejected_subscription_as_failure(tmp_path, monkeypatch, capsys):
    config_path = tmp_path / "config" / "settings.yaml"
    config_path.parent.mkdir()
    config_path.write_text("{}\n", encoding="utf-8")

    monkeypatch.setattr(cli, "load_config", lambda _path: type(
        "Config", (), {
            "kis_api_for": lambda self, _env: type(
                "Api", (), {"app_key": "key", "app_secret": "secret"},
            )(),
        },
    )())
    monkeypatch.setattr(
        cli.KisAuthClient,
        "authenticate_read_only",
        lambda self, **kwargs: type(
            "Auth", (), {"approval_key": "approval", "cache_hit": True},
        )(),
    )

    async def rejected(*args, **kwargs):
        return WebSocketSmokeResult(False, (), error_code="OPSP8996")

    monkeypatch.setattr(cli, "smoke_realtime_price", rejected)

    assert cli.smoke_ws(str(config_path), "demo", "005930", 10) == 3
    output = capsys.readouterr().out
    assert "KIS WebSocket smoke: FAILED" in output
    assert "error_code=OPSP8996" in output
