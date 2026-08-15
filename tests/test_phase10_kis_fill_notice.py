import asyncio
import base64
import json

from Crypto.Cipher import AES
from Crypto.Util.Padding import pad

from kis_ai_scalper.broker.kis_fill_notice import (
    FILL_NOTICE_COLUMNS,
    FILL_NOTICE_DEMO_TR_ID,
    FILL_NOTICE_REAL_TR_ID,
    FillNoticeKind,
    KisFillNoticeClient,
    aes_cbc_base64_dec,
    build_fill_notice_subscription,
    decrypt_aes_cbc_base64,
    parse_fill_notice,
    parse_fill_notice_ack,
    parse_fill_notice_events,
)


KEY = "0123456789abcdef0123456789abcdef"
IV = "abcdef9876543210"
ACCOUNT = "1234567801"
HTS_ID = "private-hts-id"


def encrypted_frame(tr_id: str, records: list[list[str]], *, count: int | None = None) -> str:
    plaintext = "^".join(value for record in records for value in record)
    ciphertext = AES.new(KEY.encode(), AES.MODE_CBC, IV.encode()).encrypt(pad(plaintext.encode(), AES.block_size))
    encoded = base64.b64encode(ciphertext).decode()
    return f"1|{tr_id}|{count if count is not None else len(records):03d}|{encoded}"


def record(*, event_code: str, reject: str = "N", fill_qty: str = "0", fill_price: str = "0") -> list[str]:
    values = [
        "customer-secret", ACCOUNT, "0001234567", "0000000000", "02", "00", "00", "",
        "005930", fill_qty, fill_price, "091530", reject, event_code, "Y",
        "001", "10", "account-name", "", "KRX", "N", "", "00", "", "", "71200",
    ]
    assert len(values) == len(FILL_NOTICE_COLUMNS)
    return values


def ack(tr_id: str, *, success: bool = True) -> str:
    body = {
        "rt_cd": "0" if success else "1",
        "msg1": "SUBSCRIBE SUCCESS" if success else "SUBSCRIBE FAILED",
    }
    if success:
        body["output"] = {"key": KEY, "iv": IV}
    return json.dumps({"header": {"tr_id": tr_id, "tr_key": HTS_ID}, "body": body})


def test_tr_ids_and_subscription_use_hts_id_not_symbol():
    real = json.loads(build_fill_notice_subscription("approval", HTS_ID, "real"))
    demo = json.loads(build_fill_notice_subscription("approval", HTS_ID, "demo"))
    assert real["body"]["input"] == {"tr_id": FILL_NOTICE_REAL_TR_ID, "tr_key": HTS_ID}
    assert demo["body"]["input"] == {"tr_id": FILL_NOTICE_DEMO_TR_ID, "tr_key": HTS_ID}
    assert real["header"] == {
        "approval_key": "approval", "custtype": "P", "tr_type": "1", "content-type": "utf-8",
    }


def test_aes_cbc_base64_round_trip_and_alias():
    plaintext = "secret-free fill payload"
    ciphertext = AES.new(KEY.encode(), AES.MODE_CBC, IV.encode()).encrypt(pad(plaintext.encode(), AES.block_size))
    encoded = base64.b64encode(ciphertext).decode()
    assert decrypt_aes_cbc_base64(encoded, KEY, IV) == plaintext
    assert aes_cbc_base64_dec(KEY, IV, encoded) == plaintext


def test_aes_rejects_bad_key_iv_base64_and_padding():
    for args in [("bad", KEY, IV), ("bad", "short", IV), ("bad", KEY, "short"), ("%%%", KEY, IV)]:
        try:
            decrypt_aes_cbc_base64(*args)
        except ValueError:
            pass
        else:
            raise AssertionError("malformed crypto input must fail closed")


def test_ack_extracts_per_subscription_aes_key_and_iv_without_repr_leak():
    parsed = parse_fill_notice_ack(ack(FILL_NOTICE_REAL_TR_ID), "real", hts_id=HTS_ID)
    assert parsed is not None
    assert parsed.ready_for_data is True
    assert parsed.aes_key == KEY
    assert parsed.aes_iv == IV
    representation = repr(parsed)
    assert KEY not in representation
    assert IV not in representation
    assert HTS_ID not in representation


def test_failed_or_malformed_ack_is_not_crypto_ready():
    failed = parse_fill_notice_ack(ack(FILL_NOTICE_DEMO_TR_ID, success=False), "demo", hts_id=HTS_ID)
    assert failed is not None and failed.success is False and failed.ready_for_data is False
    assert parse_fill_notice_ack("not-json") is None
    assert parse_fill_notice_ack(ack(FILL_NOTICE_REAL_TR_ID), "demo", hts_id=HTS_ID) is None
    mismatched = json.loads(ack(FILL_NOTICE_REAL_TR_ID))
    mismatched["header"]["tr_key"] = "another-hts-id"
    assert parse_fill_notice_ack(json.dumps(mismatched), "real", hts_id=HTS_ID) is None
    missing_key = json.loads(ack(FILL_NOTICE_REAL_TR_ID))
    del missing_key["body"]["output"]["key"]
    assert parse_fill_notice_ack(json.dumps(missing_key), "real") is None
    assert parse_fill_notice_ack(ack(FILL_NOTICE_REAL_TR_ID), "real", hts_id="another-hts") is None


def test_fill_acceptance_and_rejection_events_parse():
    fill = parse_fill_notice(
        encrypted_frame(FILL_NOTICE_REAL_TR_ID, [record(event_code="2", fill_qty="3", fill_price="71200")]),
        KEY, IV, "real",
    )
    accepted = parse_fill_notice(
        encrypted_frame(FILL_NOTICE_REAL_TR_ID, [record(event_code="1")]), KEY, IV, "real"
    )
    rejected = parse_fill_notice(
        encrypted_frame(FILL_NOTICE_REAL_TR_ID, [record(event_code="1", reject="Y")]), KEY, IV, "real"
    )
    assert fill is not None and fill.kind is FillNoticeKind.FILLED and fill.is_fill
    assert (fill.fill_qty, fill.fill_price, fill.symbol, fill.side) == (3, 71200, "005930", "02")
    assert accepted is not None and accepted.kind is FillNoticeKind.ACCEPTED
    assert rejected is not None and rejected.kind is FillNoticeKind.REJECTED and rejected.is_rejected


def test_multiple_records_are_atomic_and_sensitive_fields_are_not_in_repr():
    raw = encrypted_frame(
        FILL_NOTICE_DEMO_TR_ID,
        [record(event_code="2", fill_qty="3", fill_price="71200"), record(event_code="1")],
    )
    events = parse_fill_notice_events(raw, KEY, IV, "demo")
    assert [event.kind for event in events] == [FillNoticeKind.FILLED, FillNoticeKind.ACCEPTED]
    assert ACCOUNT not in repr(events[0])
    assert "customer-secret" not in repr(events[0])


def test_unknown_malformed_and_unencrypted_frames_return_no_event():
    valid = encrypted_frame(FILL_NOTICE_REAL_TR_ID, [record(event_code="2", fill_qty="1", fill_price="100")])
    assert parse_fill_notice_events(valid.replace("|001|", "|002|"), KEY, IV) == ()
    assert parse_fill_notice_events(valid.replace("1|H0STCNI0", "0|H0STCNI0"), KEY, IV) == ()
    assert parse_fill_notice_events(valid.replace("H0STCNI0", "UNKNOWN0"), KEY, IV) == ()
    bad_status = record(event_code="9", fill_qty="1", fill_price="100")
    assert parse_fill_notice_events(encrypted_frame(FILL_NOTICE_REAL_TR_ID, [bad_status]), KEY, IV) == ()
    bad_count = record(event_code="2", fill_qty="1", fill_price="100")[:-1]
    assert parse_fill_notice_events(encrypted_frame(FILL_NOTICE_REAL_TR_ID, [bad_count]), KEY, IV) == ()


def test_invalid_time_and_missing_acceptance_flag_fail_closed():
    invalid_time = record(event_code="2", fill_qty="1", fill_price="100")
    invalid_time[11] = "996099"
    assert parse_fill_notice_events(encrypted_frame(FILL_NOTICE_REAL_TR_ID, [invalid_time]), KEY, IV) == ()
    missing_acceptance = record(event_code="1")
    missing_acceptance[14] = "N"
    assert parse_fill_notice_events(encrypted_frame(FILL_NOTICE_REAL_TR_ID, [missing_acceptance]), KEY, IV) == ()


class FakeTransport:
    def __init__(self, incoming):
        self.incoming = list(incoming)
        self.sent = []

    async def send(self, message):
        self.sent.append(message)

    async def recv(self):
        return self.incoming.pop(0)


def test_client_injects_transport_waits_for_ack_and_never_calls_order_api():
    transport = FakeTransport([
        ack(FILL_NOTICE_REAL_TR_ID),
        encrypted_frame(FILL_NOTICE_REAL_TR_ID, [record(event_code="2", fill_qty="1", fill_price="71200")]),
    ])
    seen = []
    client = KisFillNoticeClient("real", "approval-secret", HTS_ID, transport, on_notice=seen.append)

    async def scenario():
        await client.subscribe()
        first = await client.receive()
        second = await client.receive()
        return first, second

    first, second = asyncio.run(scenario())
    sent = json.loads(transport.sent[0])
    assert sent["body"]["input"]["tr_key"] == HTS_ID
    assert first.ready_for_data is True
    assert second and second[0].kind is FillNoticeKind.FILLED
    assert seen == list(second)
    safe_repr = repr(client)
    assert "approval-secret" not in safe_repr
    assert HTS_ID not in safe_repr
    assert KEY not in safe_repr
