from kis_ai_scalper import cli
from kis_ai_scalper.broker.kis_fill_notice import FillNoticeSmokeResult


def test_fill_notice_smoke_cli_is_bounded_and_read_only(monkeypatch, capsys):
    monkeypatch.setenv("KIS_HTS_ID", "test-hts")
    monkeypatch.setattr(cli, "load_config", lambda _path: object())
    monkeypatch.setattr(
        cli,
        "_kis_api_for",
        lambda _config, _env: type("Api", (), {"app_key": "key", "app_secret": "secret"})(),
    )

    class Auth:
        def __init__(self, *_args, **_kwargs):
            pass

        def authenticate_read_only(self, **_kwargs):
            return type("Result", (), {"approval_key": "approval"})()

    calls = []

    async def smoke(endpoint, approval_key, hts_id, environment, seconds):
        calls.append((endpoint, approval_key, hts_id, environment.value, seconds))
        return FillNoticeSmokeResult(True, "SUBSCRIBE SUCCESS", 0)

    monkeypatch.setattr(cli, "KisAuthClient", Auth)
    monkeypatch.setattr(cli, "smoke_kis_fill_notice", smoke)
    monkeypatch.setattr(cli, "websocket_url", lambda _env: "ws://example.test")

    result = cli.main([
        "smoke-fill-notice", "--config", "config/settings.yaml",
        "--env", "demo", "--seconds", "3",
    ])

    assert result == 0
    assert calls == [("ws://example.test", "approval", "test-hts", "demo", 3)]
    output = capsys.readouterr().out
    assert "orders=none account_queries=none" in output
    assert "approval" not in output
    assert "test-hts" not in output
