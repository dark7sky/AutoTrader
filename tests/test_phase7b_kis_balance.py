from dataclasses import dataclass

from kis_ai_scalper.broker.kis_balance import KisBalanceClient
from kis_ai_scalper import cli
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
    def __init__(self):
        self.gets = []

    def get(self, url, **kwargs):
        self.gets.append((url, kwargs))
        return FakeResponse({
            "rt_cd": "0",
            "output1": [
                {
                    "pdno": "005930",
                    "prdt_name": "Samsung",
                    "hldg_qty": "2",
                    "pchs_avg_pric": "70000",
                    "prpr": "71000",
                },
                {"pdno": "000660", "hldg_qty": "0"},
            ],
        })


def test_balance_client_queries_demo_positions_read_only():
    session = FakeSession()
    client = KisBalanceClient(
        "demo", "app-key", "app-secret", "token", "12345678", "01",
        session=session,
    )

    positions = client.get_positions()

    assert len(positions) == 1
    assert positions[0].symbol == "005930"
    assert positions[0].quantity == 2
    url, kwargs = session.gets[0]
    assert url.endswith("/uapi/domestic-stock/v1/trading/inquire-balance")
    assert kwargs["headers"]["tr_id"] == "VTTC8434R"
    assert kwargs["params"]["CANO"] == "12345678"
    assert kwargs["params"]["ACNT_PRDT_CD"] == "01"


def test_reconciliation_keeps_local_only_position_for_operator_review(tmp_path, monkeypatch):
    _patch_reconciliation_dependencies(monkeypatch, broker_positions=())
    db_path = tmp_path / "positions.db"
    with connect_database(db_path) as database:
        database.init_schema()
        database.open_live_position(
            position_id="position-1",
            signal_id="signal-1",
            symbol="005930",
            quantity=2,
            entry_price=100,
            stop_loss_price=90,
            take_profit_price=110,
            opened_at=cli.kst_now(),
            entry_broker_order_id="broker-1",
        )

    ok, messages = cli._reconcile_broker_positions(
        "config/settings.yaml", cli.KisEnvironment.DEMO, str(db_path), False,
    )

    assert ok is False
    assert messages == ["operator_review_local_position_only=005930:2"]
    with connect_database(db_path) as database:
        database.init_schema()
        assert len(database.list_open_live_positions("005930")) == 1


def test_reconciliation_requests_operator_review_for_broker_only_position(tmp_path, monkeypatch):
    _patch_reconciliation_dependencies(
        monkeypatch,
        broker_positions=(type("Position", (), {"symbol": "005930", "quantity": 2})(),),
    )

    ok, messages = cli._reconcile_broker_positions(
        "config/settings.yaml", cli.KisEnvironment.DEMO, str(tmp_path / "positions.db"), False,
    )

    assert ok is False
    assert messages == ["operator_review_broker_only=005930:2"]


def _patch_reconciliation_dependencies(monkeypatch, broker_positions):
    monkeypatch.setattr(cli, "load_config", lambda _: object())
    monkeypatch.setattr(
        cli,
        "_kis_api_for",
        lambda *_: type("Api", (), {"app_key": "key", "app_secret": "secret"})(),
    )
    monkeypatch.setattr(
        cli,
        "_kis_account_for",
        lambda *_: type("Account", (), {"account_no": "12345678", "account_product_code": "01"})(),
    )

    class FakeAuth:
        def __init__(self, *args, **kwargs):
            pass

        def authenticate_read_only(self, **kwargs):
            return type("AuthResult", (), {"access_token": "token"})()

    class FakeBalance:
        def __init__(self, *args, **kwargs):
            pass

        def get_positions(self):
            return broker_positions

    monkeypatch.setattr(cli, "KisAuthClient", FakeAuth)
    monkeypatch.setattr(cli, "KisBalanceClient", FakeBalance)
