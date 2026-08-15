"""KIS domestic-stock realtime fill-notice client.

The KIS feed uses an encrypted pipe-delimited WebSocket frame.  This module
keeps transport and order submission separate: it only subscribes and parses
notifications received from an injected transport.
"""

from __future__ import annotations

import base64
import asyncio
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from enum import StrEnum
import inspect
import json
import time
from typing import Any, Protocol

from Crypto.Cipher import AES
from Crypto.Util.Padding import unpad

from .kis_endpoints import KisEnvironment


FILL_NOTICE_REAL_TR_ID = "H0STCNI0"
FILL_NOTICE_DEMO_TR_ID = "H0STCNI9"
TR_FILL_NOTICE_REAL = FILL_NOTICE_REAL_TR_ID
TR_FILL_NOTICE_DEMO = FILL_NOTICE_DEMO_TR_ID
TR_FILL_NOTICE_PAPER = TR_FILL_NOTICE_DEMO
FILL_NOTICE_TR_IDS = {
    KisEnvironment.REAL: FILL_NOTICE_REAL_TR_ID,
    KisEnvironment.DEMO: FILL_NOTICE_DEMO_TR_ID,
}

# Official domestic-stock realtime fill-notice column order.
FILL_NOTICE_COLUMNS = (
    "CUST_ID", "ACNT_NO", "ODER_NO", "OODER_NO", "SELN_BYOV_CLS",
    "RCTF_CLS", "ODER_KIND", "ODER_COND", "STCK_SHRN_ISCD", "CNTG_QTY",
    "CNTG_UNPR", "STCK_CNTG_HOUR", "RFUS_YN", "CNTG_YN", "ACPT_YN",
    "BRNC_NO", "ODER_QTY", "ACNT_NAME", "ORD_COND_PRC", "ORD_EXG_GB",
    "POPUP_YN", "FILLER", "CRDT_CLS", "CRDT_LOAN_DATE", "CNTG_ISNM40",
    "ODER_PRC",
)


class FillNoticeKind(StrEnum):
    FILLED = "filled"
    ACCEPTED = "accepted"
    REJECTED = "rejected"


FillNoticeStatus = FillNoticeKind


@dataclass(frozen=True, repr=False)
class FillNotice:
    """A validated order acceptance, rejection, or execution notification."""

    kind: FillNoticeKind
    order_no: str
    original_order_no: str
    order_qty: int
    side: str
    symbol: str
    fill_qty: int
    fill_price: int
    fill_time: str
    customer_id: str = field(repr=False)
    account_no: str = field(repr=False)
    receipt_type: str = ""
    order_kind: str = ""
    reject_flag: str = "N"
    accepted_flag: str = ""
    order_price: int = 0
    exchange_id: str = ""

    @property
    def is_fill(self) -> bool:
        return self.kind is FillNoticeKind.FILLED

    @property
    def is_rejected(self) -> bool:
        return self.kind is FillNoticeKind.REJECTED

    def __repr__(self) -> str:
        # Do not let account or customer identifiers enter logs by accident.
        return (
            "FillNotice(kind={!r}, order_no={!r}, order_qty={!r}, side={!r}, "
            "symbol={!r}, fill_qty={!r}, fill_price={!r}, fill_time={!r})"
        ).format(
            self.kind, self.order_no, self.order_qty, self.side, self.symbol,
            self.fill_qty, self.fill_price, self.fill_time,
        )


@dataclass(frozen=True, repr=False)
class FillNoticeAck:
    """Validated subscription response and its per-subscription crypto state."""

    tr_id: str
    success: bool
    message: str
    hts_id: str = field(repr=False)
    aes_key: str | None = field(default=None, repr=False)
    aes_iv: str | None = field(default=None, repr=False)

    @property
    def ready_for_data(self) -> bool:
        return self.success and self.aes_key is not None and self.aes_iv is not None

    def __repr__(self) -> str:
        return f"FillNoticeAck(tr_id={self.tr_id!r}, success={self.success!r})"


@dataclass(frozen=True, repr=False)
class FillNoticeSmokeResult:
    """Safe summary of a bounded, read-only fill-notice connection."""

    acknowledged: bool
    message: str | None = None
    event_count: int = 0

    def __post_init__(self) -> None:
        if self.event_count < 0:
            raise ValueError("event_count must not be negative")

    def __repr__(self) -> str:
        return (
            f"FillNoticeSmokeResult(acknowledged={self.acknowledged!r}, "
            f"event_count={self.event_count!r})"
        )


def _sanitize_smoke_message(value: str | None) -> str | None:
    if not value:
        return None
    cleaned = " ".join(value.split())
    return cleaned[:120] or None


def fill_notice_tr_id(environment: KisEnvironment | str) -> str:
    env = environment if isinstance(environment, KisEnvironment) else KisEnvironment.parse(environment)
    return FILL_NOTICE_TR_IDS[env]


def _public_text(value: Any, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"invalid {field_name}")
    value = value.strip()
    if any(ord(char) < 32 or ord(char) == 127 for char in value):
        raise ValueError(f"invalid {field_name}")
    return value


def _secret_text(value: Any, field_name: str) -> str:
    # Keep secret validation errors generic: never interpolate the value.
    try:
        return _public_text(value, field_name)
    except ValueError as exc:
        raise ValueError(f"invalid {field_name}") from exc


def build_fill_notice_subscription(
    approval_key: str,
    hts_id: str,
    environment: KisEnvironment | str = KisEnvironment.REAL,
    *,
    tr_type: str = "1",
) -> str:
    """Build the official JSON registration/unregistration message."""
    approval_key = _secret_text(approval_key, "approval_key")
    hts_id = _secret_text(hts_id, "hts_id")
    if tr_type not in {"0", "1", "2"}:
        raise ValueError("tr_type must be '1', '0', or '2'")
    payload = {
        "header": {
            "approval_key": approval_key,
            "custtype": "P",
            "tr_type": tr_type,
            "content-type": "utf-8",
        },
        "body": {"input": {"tr_id": fill_notice_tr_id(environment), "tr_key": hts_id}},
    }
    return json.dumps(payload, ensure_ascii=False, separators=(",", ":"))


# Short alias matching the existing kis_ws helper naming.
build_subscription = build_fill_notice_subscription


def decrypt_aes_cbc_base64(ciphertext: str, aes_key: str, aes_iv: str) -> str:
    """Decrypt one KIS AES-256-CBC/base64 payload with strict validation."""
    try:
        key = _secret_text(aes_key, "aes_key").encode("ascii")
        iv = _secret_text(aes_iv, "aes_iv").encode("ascii")
        encoded = _public_text(ciphertext, "ciphertext").encode("ascii")
        if len(key) != 32 or len(iv) != AES.block_size:
            raise ValueError("invalid AES parameters")
        encrypted = base64.b64decode(encoded, validate=True)
        if not encrypted or len(encrypted) % AES.block_size:
            raise ValueError("invalid ciphertext")
        plaintext = AES.new(key, AES.MODE_CBC, iv).decrypt(encrypted)
        return unpad(plaintext, AES.block_size).decode("utf-8", errors="strict")
    except (ValueError, TypeError, UnicodeError, base64.binascii.Error) as exc:
        raise ValueError("invalid encrypted fill-notice payload") from exc


def aes_cbc_base64_dec(aes_key: str, aes_iv: str, ciphertext: str) -> str:
    """Official-sample-compatible AES helper argument order."""

    return decrypt_aes_cbc_base64(ciphertext, aes_key, aes_iv)


def _json_object(raw: str | bytes) -> dict[str, Any] | None:
    if isinstance(raw, bytes):
        try:
            raw = raw.decode("utf-8")
        except UnicodeDecodeError:
            return None
    if not isinstance(raw, str) or not raw.strip():
        return None
    try:
        value = json.loads(raw)
    except (TypeError, ValueError):
        return None
    return value if isinstance(value, dict) else None


def parse_fill_notice_ack(
    raw: str | bytes,
    environment: KisEnvironment | str | None = None,
    *,
    hts_id: str = "",
) -> FillNoticeAck | None:
    """Parse a subscription ACK; malformed or unrelated JSON returns ``None``."""
    payload = _json_object(raw)
    if payload is None:
        return None
    header = payload.get("header")
    body = payload.get("body")
    if not isinstance(header, Mapping) or not isinstance(body, Mapping):
        return None
    tr_id = header.get("tr_id")
    if not isinstance(tr_id, str) or tr_id not in FILL_NOTICE_TR_IDS.values():
        return None
    tr_key = header.get("tr_key")
    if hts_id and tr_key != hts_id:
        return None
    if environment is not None and tr_id != fill_notice_tr_id(environment):
        return None
    message = body.get("msg1")
    rt_cd = body.get("rt_cd")
    if not isinstance(message, str) or not isinstance(rt_cd, str) or rt_cd not in {"0", "1"}:
        return None
    if rt_cd != "0":
        return FillNoticeAck(tr_id, False, message, hts_id=hts_id)
    output = body.get("output")
    if not isinstance(output, Mapping):
        return None
    key = output.get("key")
    iv = output.get("iv")
    if not isinstance(key, str) or not isinstance(iv, str):
        return None
    try:
        # Validate shape now so a successful ACK can never arm bad crypto state.
        if len(key.encode("ascii")) != 32 or len(iv.encode("ascii")) != AES.block_size:
            return None
    except UnicodeEncodeError:
        return None
    return FillNoticeAck(tr_id, True, message, hts_id=hts_id, aes_key=key, aes_iv=iv)


def _int_field(value: str, name: str, *, positive: bool = False) -> int:
    if not value or (value[0] == "+") or not value.isdigit():
        raise ValueError(f"invalid {name}")
    result = int(value)
    if positive and result <= 0:
        raise ValueError(f"invalid {name}")
    return result


def _parse_fields(fields: list[str]) -> FillNotice:
    if len(fields) != len(FILL_NOTICE_COLUMNS):
        raise ValueError("invalid fill-notice field count")
    if any(not isinstance(value, str) for value in fields):
        raise ValueError("invalid fill-notice fields")
    customer_id, account_no, order_no = fields[0:3]
    order_qty = _int_field(fields[16], "order_qty", positive=True)
    side = fields[4]
    if side not in {"01", "02"}:
        raise ValueError("unknown order side")
    symbol = fields[8]
    if len(symbol) != 6 or not symbol.isdigit():
        raise ValueError("invalid symbol")
    fill_qty = _int_field(fields[9], "fill_qty")
    fill_price = _int_field(fields[10], "fill_price")
    fill_time = fields[11]
    if len(fill_time) != 6 or not fill_time.isdigit() or int(fill_time[:2]) > 23 or int(fill_time[2:4]) > 59 or int(fill_time[4:]) > 59:
        raise ValueError("invalid fill time")
    reject_flag = fields[12].upper()
    event_code = fields[13]
    accepted_flag = fields[14].upper()
    if reject_flag not in {"Y", "N", ""} or event_code not in {"1", "2"}:
        raise ValueError("unknown fill-notice status")
    if accepted_flag not in {"Y", "N", ""}:
        raise ValueError("unknown acceptance status")
    if event_code == "2":
        if reject_flag == "Y" or fill_qty <= 0 or fill_price <= 0:
            raise ValueError("invalid fill event")
        kind = FillNoticeKind.FILLED
    elif reject_flag == "Y":
        if fill_qty != 0:
            raise ValueError("invalid rejected event")
        kind = FillNoticeKind.REJECTED
    else:
        if accepted_flag != "Y" or fill_qty != 0:
            raise ValueError("invalid accepted event")
        kind = FillNoticeKind.ACCEPTED
    order_price = _int_field(fields[25], "order_price") if fields[25] else 0
    return FillNotice(
        kind=kind,
        order_no=_public_text(order_no, "order_no"),
        original_order_no=_public_text(fields[3], "original_order_no") if fields[3] else "",
        order_qty=order_qty,
        side=side,
        symbol=symbol,
        fill_qty=fill_qty,
        fill_price=fill_price,
        fill_time=fill_time,
        customer_id=_secret_text(customer_id, "customer_id"),
        account_no=_secret_text(account_no, "account_no"),
        receipt_type=fields[5],
        order_kind=fields[6],
        reject_flag=reject_flag or "N",
        accepted_flag=accepted_flag,
        order_price=order_price,
        exchange_id=fields[19],
    )


def parse_fill_notice_events(
    raw: str | bytes,
    aes_key: str,
    aes_iv: str,
    environment: KisEnvironment | str | None = None,
) -> tuple[FillNotice, ...]:
    """Decrypt and parse all records in one realtime frame.

    An empty tuple is returned for every malformed, encrypted-without-key,
    wrong-TR, or unknown-status frame.  No partial records are returned.
    """
    if isinstance(raw, bytes):
        try:
            raw = raw.decode("utf-8")
        except UnicodeDecodeError:
            return ()
    if not isinstance(raw, str):
        return ()
    parts = raw.split("|", 3)
    if len(parts) != 4 or parts[0] != "1":
        return ()
    tr_id = parts[1]
    if tr_id not in FILL_NOTICE_TR_IDS.values():
        return ()
    if environment is not None and tr_id != fill_notice_tr_id(environment):
        return ()
    try:
        count = _int_field(parts[2], "record count", positive=True)
        if count > 100:
            return ()
        plaintext = decrypt_aes_cbc_base64(parts[3], aes_key, aes_iv)
        fields = plaintext.split("^")
        if len(fields) != count * len(FILL_NOTICE_COLUMNS):
            return ()
        return tuple(
            _parse_fields(fields[index:index + len(FILL_NOTICE_COLUMNS)])
            for index in range(0, len(fields), len(FILL_NOTICE_COLUMNS))
        )
    except (ValueError, OverflowError):
        return ()


parse_fill_notice_records = parse_fill_notice_events


def parse_fill_notice(
    raw: str | bytes,
    aes_key: str,
    aes_iv: str,
    environment: KisEnvironment | str | None = None,
) -> FillNotice | None:
    events = parse_fill_notice_events(raw, aes_key, aes_iv, environment)
    return events[0] if len(events) == 1 else None


class FillNoticeTransport(Protocol):
    async def send(self, message: str) -> Any: ...

    async def recv(self) -> str | bytes: ...


class KisFillNoticeClient:
    """Transport-agnostic subscription client; it never submits orders."""

    def __init__(
        self,
        environment: KisEnvironment | str,
        approval_key: str,
        hts_id: str,
        transport: FillNoticeTransport,
        *,
        on_notice: Callable[[FillNotice], Any] | None = None,
    ) -> None:
        self.environment = environment if isinstance(environment, KisEnvironment) else KisEnvironment.parse(environment)
        self._approval_key = _secret_text(approval_key, "approval_key")
        self._hts_id = _secret_text(hts_id, "hts_id")
        self._transport = transport
        self._on_notice = on_notice
        self._ack: FillNoticeAck | None = None

    def __repr__(self) -> str:
        return f"KisFillNoticeClient(environment={self.environment.value!r}, tr_id={fill_notice_tr_id(self.environment)!r})"

    @property
    def subscription_message(self) -> str:
        return build_fill_notice_subscription(self._approval_key, self._hts_id, self.environment)

    @property
    def ack(self) -> FillNoticeAck | None:
        return self._ack

    async def subscribe(self) -> None:
        result = self._transport.send(self.subscription_message)
        if inspect.isawaitable(result):
            await result

    def handle_message(self, raw: str | bytes) -> FillNoticeAck | tuple[FillNotice, ...] | None:
        """Handle one raw frame; bad frames are ignored and never surfaced as events."""
        ack = parse_fill_notice_ack(raw, self.environment, hts_id=self._hts_id)
        if ack is not None:
            self._ack = ack if ack.ready_for_data else None
            return ack
        if self._ack is None or not self._ack.ready_for_data:
            return None
        events = parse_fill_notice_events(raw, self._ack.aes_key or "", self._ack.aes_iv or "", self.environment)
        if not events:
            return None
        if self._on_notice is not None:
            for event in events:
                result = self._on_notice(event)
                if inspect.isawaitable(result):
                    raise TypeError("async on_notice requires receive_async")
        return events

    async def receive(self) -> FillNoticeAck | tuple[FillNotice, ...] | None:
        raw = self._transport.recv()
        if inspect.isawaitable(raw):
            raw = await raw
        return self.handle_message(raw)

    async def receive_async(self) -> FillNoticeAck | tuple[FillNotice, ...] | None:
        raw = self._transport.recv()
        if inspect.isawaitable(raw):
            raw = await raw
        ack = parse_fill_notice_ack(raw, self.environment, hts_id=self._hts_id)
        if ack is not None:
            self._ack = ack if ack.ready_for_data else None
            return ack
        if self._ack is None or not self._ack.ready_for_data:
            return None
        events = parse_fill_notice_events(raw, self._ack.aes_key or "", self._ack.aes_iv or "", self.environment)
        if self._on_notice is not None:
            for event in events:
                result = self._on_notice(event)
                if inspect.isawaitable(result):
                    await result
        return events or None


async def smoke_fill_notice(
    endpoint: str,
    approval_key: str,
    hts_id: str,
    environment: KisEnvironment | str,
    seconds: int = 10,
    *,
    socket_factory: Callable[[str], Any] | None = None,
    clock: Callable[[], float] | None = None,
) -> FillNoticeSmokeResult:
    """Run a bounded, read-only KIS fill-notice WebSocket smoke check.

    The helper only subscribes, answers KIS PINGPONG frames, validates the
    subscription ACK, and counts parseable notices. It never touches the
    ledger or imports/calls an order or account client. ``socket_factory`` is
    an async-context-manager factory for deterministic unit tests.
    """
    if not isinstance(seconds, int) or isinstance(seconds, bool) or not 1 <= seconds <= 60:
        raise ValueError("seconds must be between 1 and 60")
    env = environment if isinstance(environment, KisEnvironment) else KisEnvironment.parse(environment)
    # Validate credentials before opening a socket, while keeping values out
    # of all exception text.
    _secret_text(approval_key, "approval_key")
    _secret_text(hts_id, "hts_id")
    if not isinstance(endpoint, str) or not endpoint.strip():
        raise ValueError("endpoint is required")

    if socket_factory is None:
        try:
            import websockets
        except ImportError as exc:
            raise RuntimeError("websockets package is required for fill-notice smoke") from exc
        socket_factory = lambda url: websockets.connect(url, open_timeout=15, close_timeout=5)

    now = clock or time.monotonic
    deadline = now() + seconds
    acknowledged = False
    message: str | None = None
    event_count = 0
    connector = socket_factory(endpoint)
    try:
        async with connector as socket:
            await socket.send(build_fill_notice_subscription(approval_key, hts_id, env))
            ack_state: FillNoticeAck | None = None
            while now() < deadline:
                remaining = max(0.05, deadline - now())
                try:
                    raw = await asyncio.wait_for(socket.recv(), timeout=remaining)
                except asyncio.TimeoutError:
                    break
                except Exception as exc:
                    # Do not relay transport details, which may contain URLs
                    # or provider-specific credential material.
                    name = type(exc).__name__
                    if name in {"ConnectionClosed", "ConnectionClosedError", "ConnectionClosedOK"}:
                        break
                    raise RuntimeError("fill-notice smoke transport error") from None

                ack = parse_fill_notice_ack(raw, env, hts_id=hts_id)
                if ack is not None:
                    if ack.ready_for_data:
                        ack_state = ack
                        acknowledged = True
                        message = _sanitize_smoke_message(ack.message)
                    else:
                        # A valid rejection is terminal for this bounded check.
                        message = _sanitize_smoke_message(ack.message)
                    continue

                system = _json_object(raw)
                if system is not None:
                    header = system.get("header")
                    if isinstance(header, Mapping) and str(header.get("tr_id", "")).upper() == "PINGPONG":
                        await socket.send(raw.decode("utf-8") if isinstance(raw, bytes) else raw)
                    continue

                if ack_state is not None:
                    events = parse_fill_notice_events(
                        raw, ack_state.aes_key or "", ack_state.aes_iv or "", env
                    )
                    event_count += len(events)
    except (ValueError, TypeError):
        raise
    except Exception as exc:
        if type(exc).__name__ in {"ConnectionClosed", "ConnectionClosedError", "ConnectionClosedOK"}:
            pass
        else:
            raise RuntimeError("fill-notice smoke failed") from None
    return FillNoticeSmokeResult(acknowledged, message, event_count)


__all__ = [
    "FILL_NOTICE_COLUMNS", "FILL_NOTICE_DEMO_TR_ID", "FILL_NOTICE_REAL_TR_ID",
    "FILL_NOTICE_TR_IDS", "FillNotice", "FillNoticeAck", "FillNoticeKind",
    "FillNoticeStatus", "TR_FILL_NOTICE_DEMO", "TR_FILL_NOTICE_PAPER",
    "TR_FILL_NOTICE_REAL",
    "FillNoticeTransport", "KisFillNoticeClient", "FillNoticeSmokeResult",
    "aes_cbc_base64_dec", "smoke_fill_notice",
    "build_fill_notice_subscription", "build_subscription", "decrypt_aes_cbc_base64",
    "fill_notice_tr_id", "parse_fill_notice", "parse_fill_notice_ack",
    "parse_fill_notice_events", "parse_fill_notice_records",
]
