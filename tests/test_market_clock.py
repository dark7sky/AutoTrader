from kis_ai_scalper.market.clock import KST, kst_now, kst_today


def test_kst_now_is_naive_korean_market_time():
    now = kst_now()

    assert now.tzinfo is None
    assert kst_today() == now.date()


def test_kst_constant_is_utc_plus_nine():
    assert KST.utcoffset(None).total_seconds() == 9 * 60 * 60
    assert KST.tzname(None) == "KST"
