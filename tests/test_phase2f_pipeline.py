from datetime import datetime, timedelta

from kis_ai_scalper.cli import main
from kis_ai_scalper.market.tick import MinuteBar
from kis_ai_scalper.pipeline import DryRunConfig, run_offline_dry_run
from kis_ai_scalper.risk import RiskConfig


def make_bars(closes, volumes=None):
    volumes = volumes or [100] * len(closes)
    start = datetime(2026, 8, 15, 9, 0)
    return [
        MinuteBar("005930", start + timedelta(minutes=i), close, close + 500, close - 500, close, volume)
        for i, (close, volume) in enumerate(zip(closes, volumes))
    ]


def test_no_candidate_report_is_flat():
    report = run_offline_dry_run(make_bars([100] * 25))
    assert report.candidates_count == 0
    assert report.risk_reason == "no_candidate"
    assert report.lifecycle_final_state == "FLAT"
    assert report.signal_id is None


def test_candidate_flows_through_risk_lifecycle_and_position_manager():
    bars = make_bars([100_000 + index * 1_000 for index in range(20)] + [121_000], [100] * 20 + [150])
    report = run_offline_dry_run(bars)
    assert report.candidates_count == 1
    assert report.selected_strategy == "BREAKOUT_WATCH"
    assert report.risk_approved is True
    assert report.risk_quantity == 2
    assert report.lifecycle_final_state == "LONG"
    assert report.position_action == "HOLD"
    assert report.position_reason == "hold"
    assert report.signal_id is not None


def test_low_confidence_candidate_is_denied_by_risk_engine():
    bars = make_bars([100_000 + index * 1_000 for index in range(20)] + [121_000], [100] * 20 + [150])
    report = run_offline_dry_run(
        bars, DryRunConfig(risk=RiskConfig(minimum_confidence=0.9))
    )
    assert report.risk_approved is False
    assert report.risk_reason == "confidence_below_minimum"
    assert report.lifecycle_final_state == "FLAT"
    assert report.risk_quantity == 0


def test_cli_sample_output(capsys):
    assert main(["dry-run-pipeline"]) == 0
    output = capsys.readouterr().out
    assert "offline dry-run: OK" in output
    assert "selected_strategy=BREAKOUT_WATCH" in output
    assert "risk_approved=true" in output
    assert "lifecycle_final_state=LONG" in output
    assert "broker_calls=none ai_calls=none" in output


def test_pipeline_has_no_external_execution_dependencies():
    from pathlib import Path

    root = Path(__file__).parents[1] / "kis_ai_scalper" / "pipeline"
    source = "\n".join(path.read_text(encoding="utf-8") for path in root.glob("*.py"))
    for forbidden in ("requests", "websockets", "oauth2", "OrderManager", "OpenAI", "Codex"):
        assert forbidden not in source
