import asyncio
import base64
import json
from pathlib import Path
from threading import Event, Thread

from Crypto.Cipher import AES
from Crypto.Util.Padding import pad

import kis_ai_scalper.ops.fill_notice_worker as worker
from kis_ai_scalper.broker.kis_fill_notice import FILL_NOTICE_COLUMNS
from kis_ai_scalper.storage import connect_database


KEY = "0123456789abcdef0123456789abcdef"
IV = "abcdef9876543210"
HTS_ID = "test-hts-id"


def ack() -> str:
    return json.dumps({
        "header": {"tr_id": "H0STCNI9", "tr_key": HTS_ID},
        "body": {"rt_cd": "0", "msg1": "SUBSCRIBE SUCCESS", "output": {"key": KEY, "iv": IV}},
    })


def fill_frame() -> str:
    values = [
        "customer", "account", "0001234567", "0000000000", "02", "00", "00", "",
        "005930", "2", "71200", "091530", "N", "2", "Y", "001", "2", "name", "",
        "KRX", "N", "", "00", "", "", "71200",
    ]
    assert len(values) == len(FILL_NOTICE_COLUMNS)
    plaintext = "^".join(values).encode()
    ciphertext = AES.new(KEY.encode(), AES.MODE_CBC, IV.encode()).encrypt(pad(plaintext, AES.block_size))
    return "1|H0STCNI9|001|" + base64.b64encode(ciphertext).decode()


class FakeSocket:
    def __init__(self, messages, stop: Event | None = None):
        self.messages = list(messages)
        self.sent = []
        self.stop = stop

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_):
        return False

    async def send(self, message):
        self.sent.append(message)

    async def recv(self):
        if self.messages:
            return self.messages.pop(0)
        if self.stop is not None:
            self.stop.set()
        await asyncio.sleep(0)
        return None


class FakeAuth:
    def __init__(self, *_):
        self.approval_key = "approval"

    def authenticate_read_only(self, *, cache_path):
        return type("Auth", (), {"approval_key": self.approval_key})()


def setup_config(tmp_path: Path, monkeypatch, *, environment="demo"):
    config_path = tmp_path / "config" / "settings.yaml"
    config_path.parent.mkdir()
    config_path.write_text("mode: shadow\n", encoding="utf-8")
    monkeypatch.setenv("KIS_DEMO_APP_KEY", "demo-key")
    monkeypatch.setenv("KIS_DEMO_APP_SECRET", "demo-secret")
    monkeypatch.setenv("KIS_HTS_ID", HTS_ID)
    with connect_database(tmp_path / "state.sqlite3") as database:
        database.init_schema()
        database.set_runtime_environment(environment, "test", "test")
    return config_path, tmp_path / "state.sqlite3"


def test_subscribe_ack_and_fill_dispatch_use_read_only_worker(tmp_path, monkeypatch):
    config_path, db_path = setup_config(tmp_path, monkeypatch)
    stop = Event()
    sockets = []
    applied = []

    async def socket_factory(endpoint):
        socket = FakeSocket([
            json.dumps({"header": {"tr_id": "PINGPONG"}}),
            ack(),
            fill_frame(),
        ], stop)
        sockets.append(socket)
        return socket

    def apply(database, notice, **kwargs):
        applied.append((notice.symbol, notice.fill_qty, kwargs["received_at"]))
        return type("Result", (), {"outcome": "applied", "reason": None})()

    monkeypatch.setattr(worker, "apply_fill_notice", apply)
    worker.run_fill_notice_worker(
        config_path, db_path, stop, socket_factory=socket_factory,
        auth_factory=FakeAuth, receive_timeout_seconds=0.01, ack_timeout_seconds=0.1,
    )

    assert json.loads(sockets[0].sent[0])["body"]["input"] == {"tr_id": "H0STCNI9", "tr_key": HTS_ID}
    assert json.loads(sockets[0].sent[1])["header"]["tr_id"] == "PINGPONG"
    assert applied and applied[0][0:2] == ("005930", 2)
    with connect_database(db_path) as database:
        assert database.get_heartbeat("fill-notice")
        assert database.get_runtime_metadata("fill-notice:status") == "stopped"


def test_reconnects_with_bounded_backoff(tmp_path, monkeypatch):
    config_path, db_path = setup_config(tmp_path, monkeypatch)
    stop = Event()
    attempts = []
    delays = []

    async def socket_factory(endpoint):
        attempts.append(endpoint)
        socket = FakeSocket([ack()])
        if len(attempts) >= 3:
            stop.set()
        return socket

    async def sleeper(seconds):
        delays.append(seconds)

    worker.run_fill_notice_worker(
        config_path, db_path, stop, socket_factory=socket_factory,
        auth_factory=FakeAuth, sleeper=sleeper, receive_timeout_seconds=0.01,
        ack_timeout_seconds=0.02, reconnect_min_seconds=0.25, reconnect_max_seconds=0.5,
    )
    assert len(attempts) == 3
    assert delays[:2] == [0.25, 0.5]


def test_missing_hts_is_safe_and_does_not_authenticate(tmp_path, monkeypatch):
    config_path, db_path = setup_config(tmp_path, monkeypatch)
    monkeypatch.delenv("KIS_HTS_ID")
    (tmp_path / ".env").write_text("KIS_HTS_ID=\n", encoding="utf-8")
    stop = Event()
    calls = []

    def auth_factory(*args, **kwargs):
        calls.append(True)
        return FakeAuth()

    async def sleeper(seconds):
        stop.set()

    worker.run_fill_notice_worker(
        config_path, db_path, stop, socket_factory=lambda endpoint: None,
        auth_factory=auth_factory, sleeper=sleeper, reconnect_min_seconds=0.1,
        reconnect_max_seconds=0.2,
    )
    assert calls == []
    with connect_database(db_path) as database:
        assert database.get_runtime_metadata("fill-notice:status") == "stopped"
        assert database.get_runtime_metadata("fill-notice:last_error") == "missing_hts_id"


def test_runtime_environment_switch_reconnects(tmp_path, monkeypatch):
    config_path, db_path = setup_config(tmp_path, monkeypatch)
    monkeypatch.setenv("KIS_REAL_APP_KEY", "real-key")
    monkeypatch.setenv("KIS_REAL_APP_SECRET", "real-secret")
    stop = Event()
    seen = []

    async def socket_factory(endpoint):
        seen.append(endpoint)
        if len(seen) == 1:
            with connect_database(db_path) as database:
                database.set_runtime_environment("real", "test-switch", "test")
            return FakeSocket([ack()])
        stop.set()
        return FakeSocket([])

    worker.run_fill_notice_worker(
        config_path, db_path, stop, socket_factory=socket_factory,
        auth_factory=FakeAuth, receive_timeout_seconds=0.01, ack_timeout_seconds=0.1,
    )
    assert seen == ["ws://ops.koreainvestment.com:31000", "ws://ops.koreainvestment.com:21000"]
