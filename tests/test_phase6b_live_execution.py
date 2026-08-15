from datetime import datetime

from kis_ai_scalper import cli
from kis_ai_scalper.broker.kis_order import KisOrderResult, KisOrderSide, KisOrderType
from kis_ai_scalper.pipeline.live_execution import submit_shadow_live_buy
from kis_ai_scalper.pipeline.shadow_cycle import ShadowCycleReport
from kis_ai_scalper.storage.database import RuntimeControl, connect_database


def approved_report() -> ShadowCycleReport:
    return ShadowCycleReport(
        symbol="005930",
        bars_count=21,
        health_status="OK",
        trading_blocked=False,
        safe_mode=False,
        candidates_count=1,
        selected_strategy="BREAKOUT_WATCH",
        risk_approved=True,
        risk_reason="approved",
        risk_quantity=7,
        lifecycle_final_state="LONG",
        signal_id="signal-1",
        entry_price=71200,
    )


class FakeSubmitter:
    def __init__(self):
        self.requests = []

    def submit_order(self, request):
        self.requests.append(request)
        return KisOrderResult(
            request.symbol,
            KisOrderSide(request.side),
            request.quantity,
            request.price,
            KisOrderType(request.order_type),
            "VTTC0012U",
            "broker-1",
            {"ODNO": "broker-1"},
        )


def active_control() -> RuntimeControl:
    return RuntimeControl(False, "2026-08-15T09:00:00+09:00", "test", "test")


def paused_control() -> RuntimeControl:
    return RuntimeControl(True, "2026-08-15T09:00:00+09:00", "test", "test")


def test_confirmation_missing_blocks_without_submit(tmp_path):
    submitter = FakeSubmitter()
    with connect_database(tmp_path / "live.db") as database:
        database.init_schema()
        result = submit_shadow_live_buy(
            approved_report(),
            runtime_control=active_control(),
            submitter=submitter,
            database=database,
            confirm_submit=False,
        )
        audits = database.list_broker_order_audits("005930")

    assert result.blocked is True
    assert result.reason == "confirmation_required"
    assert submitter.requests == []
    assert audits[0]["status"] == "BLOCKED"


def test_runtime_pause_blocks_before_submit(tmp_path):
    submitter = FakeSubmitter()
    with connect_database(tmp_path / "live.db") as database:
        database.init_schema()
        result = submit_shadow_live_buy(
            approved_report(),
            runtime_control=paused_control(),
            submitter=submitter,
            database=database,
            confirm_submit=True,
        )

    assert result.blocked is True
    assert result.reason == "runtime_paused"
    assert submitter.requests == []


def test_approved_signal_submits_capped_buy_and_records_audit(tmp_path):
    submitter = FakeSubmitter()
    now = datetime(2026, 8, 15, 9, 0)
    with connect_database(tmp_path / "live.db") as database:
        database.init_schema()
        result = submit_shadow_live_buy(
            approved_report(),
            runtime_control=active_control(),
            submitter=submitter,
            database=database,
            confirm_submit=True,
            max_quantity=2,
            current_time=now,
        )
        audits = database.list_broker_order_audits("005930")

    assert result.submitted is True
    assert result.quantity == 2
    assert result.broker_order_id == "broker-1"
    assert submitter.requests[0].quantity == 2
    assert submitter.requests[0].price == 71200
    assert audits[0]["status"] == "SUBMITTED"
    assert audits[0]["broker_order_id"] == "broker-1"


def test_duplicate_submitted_signal_blocks_second_order(tmp_path):
    submitter = FakeSubmitter()
    with connect_database(tmp_path / "live.db") as database:
        database.init_schema()
        first = submit_shadow_live_buy(
            approved_report(),
            runtime_control=active_control(),
            submitter=submitter,
            database=database,
            confirm_submit=True,
        )
        second = submit_shadow_live_buy(
            approved_report(),
            runtime_control=active_control(),
            submitter=submitter,
            database=database,
            confirm_submit=True,
        )

    assert first.submitted is True
    assert second.blocked is True
    assert second.reason == "duplicate_signal"
    assert len(submitter.requests) == 1


def test_risk_rejected_report_blocks_without_submit():
    submitter = FakeSubmitter()
    report = approved_report()
    report = ShadowCycleReport(
        report.symbol, report.bars_count, report.health_status, False, False,
        report.candidates_count, report.selected_strategy, False,
        "max_total_exposure_reached", 0, "FLAT", report.signal_id, report.entry_price,
    )

    result = submit_shadow_live_buy(
        report,
        runtime_control=active_control(),
        submitter=submitter,
        confirm_submit=True,
    )

    assert result.blocked is True
    assert result.reason == "max_total_exposure_reached"
    assert submitter.requests == []


def test_submit_live_shadow_cli_missing_confirmation_does_not_auth(tmp_path, monkeypatch, capsys):
    path = tmp_path / "live.db"
    with connect_database(path) as database:
        database.init_schema()
        database.set_runtime_paused(False, "test_active", "test")

    def fail_load_config(path):
        raise AssertionError("load_config should not be called without confirmation")

    monkeypatch.setattr(cli, "load_config", fail_load_config)

    exit_code = cli.main([
        "submit-live-shadow",
        "--db", str(path),
        "--symbol", "005930",
        "--websocket-acknowledged",
    ])

    output = capsys.readouterr().out
    assert exit_code == 3
    assert "submit-live-shadow: BLOCKED" in output
    assert "reason=confirmation_required" in output
    assert "broker_orders=none" in output
