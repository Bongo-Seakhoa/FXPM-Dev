from tools.promotion_review import _final_verdict, _gate_failures, _validate_completion_marker
from tools.regime_winner_review import _holdout_gate


def test_gate_failures_preserve_secondary_blockers():
    row = {
        "holdout_status": "INSUFFICIENT",
        "historical_status": "PASS",
        "pbo_timeframe": 0.64,
    }
    stress = {
        "stress": "KILL",
        "provenance": "non_fx_spread_review",
    }

    verdict, why = _final_verdict(row, stress, "stress_max")
    failures = _gate_failures(row, stress, "stress_max")
    codes = {failure["code"] for failure in failures}

    assert verdict == "REJECT"
    assert why == "fails stress_max (spread-spike fragile)"
    assert codes == {
        "stress_fragile",
        "holdout_insufficient",
        "pbo_overfit",
        "provenance_non_fx_spread_review",
    }


def test_gate_failures_promote_only_when_all_gates_clean():
    row = {
        "holdout_status": "PASS",
        "holdout_trades": 5,
        "holdout_return_pct": 1.0,
        "holdout_pf": 1.2,
        "historical_status": "PASS",
        "historical_trades": 20,
        "historical_return_pct": 1.0,
        "historical_pf": 1.2,
        "promotion_status": "PROMOTED",
        "pbo_timeframe": 0.17,
    }
    stress = {
        "stress": "SURVIVE",
        "provenance": "fx_historical_spread",
    }

    assert _gate_failures(row, stress, "stress_max") == []
    assert _final_verdict(row, stress, "stress_max") == ("PROMOTE", "all gates clean")


def test_gate_failures_reject_blocked_promotion_status():
    row = {
        "holdout_status": "PASS",
        "holdout_trades": 5,
        "holdout_return_pct": 1.0,
        "holdout_pf": 1.2,
        "historical_status": "PASS",
        "historical_trades": 20,
        "historical_return_pct": 1.0,
        "historical_pf": 1.2,
        "promotion_status": "BLOCKED",
        "promotion_reasons": ["profitability failed"],
        "pbo_timeframe": 0.17,
    }
    stress = {
        "stress": "SURVIVE",
        "provenance": "fx_historical_spread",
    }

    verdict, why = _final_verdict(row, stress, "stress_max")
    codes = {failure["code"] for failure in _gate_failures(row, stress, "stress_max")}

    assert verdict == "REJECT"
    assert "promotion_status BLOCKED" in why
    assert "promotion_blocked" in codes


def test_gate_failures_review_promote_small_status():
    row = {
        "holdout_status": "PASS",
        "holdout_trades": 5,
        "holdout_return_pct": 1.0,
        "holdout_pf": 1.2,
        "historical_status": "PASS",
        "historical_trades": 20,
        "historical_return_pct": 1.0,
        "historical_pf": 1.2,
        "promotion_status": "PROMOTE_SMALL",
        "promotion_reasons": ["holdout trades 4 < 5"],
        "pbo_timeframe": 0.17,
    }
    stress = {
        "stress": "SURVIVE",
        "provenance": "fx_historical_spread",
    }

    verdict, why = _final_verdict(row, stress, "stress_max")

    assert verdict == "REVIEW"
    assert "PROMOTE_SMALL" in why


def test_gate_failures_reject_numeric_historical_fail_even_when_status_passes():
    row = {
        "holdout_status": "PASS",
        "holdout_trades": 5,
        "holdout_return_pct": 1.0,
        "holdout_pf": 1.2,
        "historical_status": "PASS",
        "historical_trades": 20,
        "historical_return_pct": -0.1,
        "historical_pf": 0.98,
        "promotion_status": "PROMOTED",
        "pbo_timeframe": 0.17,
    }
    stress = {
        "stress": "SURVIVE",
        "provenance": "fx_historical_spread",
    }

    verdict, why = _final_verdict(row, stress, "stress_max")
    codes = {failure["code"] for failure in _gate_failures(row, stress, "stress_max")}

    assert verdict == "REJECT"
    assert "historical PF" in why
    assert {"historical_pf_lt_1", "historical_return_negative"} <= codes


def test_gate_failures_reject_validation_fail_even_when_statuses_pass():
    row = {
        "val_trades": 12,
        "val_return_pct": -0.5,
        "val_pf": 0.95,
        "holdout_status": "PASS",
        "holdout_trades": 5,
        "holdout_return_pct": 1.0,
        "holdout_pf": 1.2,
        "historical_status": "PASS",
        "historical_trades": 20,
        "historical_return_pct": 1.0,
        "historical_pf": 1.2,
        "historical_max_dd_pct": 10.0,
        "promotion_status": "PROMOTED",
        "pbo_timeframe": 0.17,
    }
    stress = {
        "stress": "SURVIVE",
        "provenance": "fx_historical_spread",
    }

    verdict, why = _final_verdict(row, stress, "stress_max")
    codes = {failure["code"] for failure in _gate_failures(row, stress, "stress_max")}

    assert verdict == "REJECT"
    assert "validation PF" in why
    assert {"validation_pf_lt_1", "validation_return_negative"} <= codes


def test_gate_failures_reject_historical_drawdown_breach():
    row = {
        "val_trades": 12,
        "val_return_pct": 1.0,
        "val_pf": 1.2,
        "holdout_status": "PASS",
        "holdout_trades": 5,
        "holdout_return_pct": 1.0,
        "holdout_pf": 1.2,
        "historical_status": "PASS",
        "historical_trades": 20,
        "historical_return_pct": 1.0,
        "historical_pf": 1.2,
        "historical_max_dd_pct": 41.0,
        "promotion_status": "PROMOTED",
        "pbo_timeframe": 0.17,
    }
    stress = {
        "stress": "SURVIVE",
        "provenance": "fx_historical_spread",
    }

    verdict, why = _final_verdict(row, stress, "stress_max")
    codes = {failure["code"] for failure in _gate_failures(row, stress, "stress_max")}

    assert verdict == "REJECT"
    assert "historical DD" in why
    assert "historical_dd_gt_40" in codes


def test_holdout_gate_uses_configurable_min_trades():
    metrics = {
        "total_trades": 4,
        "total_return_pct": 1.0,
        "profit_factor": 1.2,
    }

    assert _holdout_gate(metrics, min_trades=5) == "INSUFFICIENT"
    assert _holdout_gate(metrics, min_trades=4) == "PASS"


def test_completion_marker_validates_exited_pid_and_ledger(tmp_path):
    ledger = tmp_path / "pm_configs.json"
    ledger.write_text("{}", encoding="utf-8")
    marker = tmp_path / "done.json"
    marker.write_text(
        f'{{"review_ready": true, "pid_alive": false, "ledger": "{str(ledger).replace(chr(92), chr(92) + chr(92))}"}}',
        encoding="utf-8",
    )

    ok, reason = _validate_completion_marker(marker, ledger)

    assert ok is True
    assert reason == "completion marker valid"


def test_completion_marker_rejects_active_pid(tmp_path):
    ledger = tmp_path / "pm_configs.json"
    ledger.write_text("{}", encoding="utf-8")
    marker = tmp_path / "done.json"
    marker.write_text('{"review_ready": true, "pid_alive": true}', encoding="utf-8")

    ok, reason = _validate_completion_marker(marker, ledger)

    assert ok is False
    assert "pid_alive=true" in reason


def test_completion_marker_rejects_ledger_mismatch(tmp_path):
    ledger = tmp_path / "pm_configs.json"
    other = tmp_path / "other.json"
    ledger.write_text("{}", encoding="utf-8")
    other.write_text("{}", encoding="utf-8")
    marker = tmp_path / "done.json"
    marker.write_text(
        f'{{"review_ready": true, "pid_alive": false, "ledger": "{str(other).replace(chr(92), chr(92) + chr(92))}"}}',
        encoding="utf-8",
    )

    ok, reason = _validate_completion_marker(marker, ledger)

    assert ok is False
    assert "ledger mismatch" in reason


def test_completion_marker_rejects_non_review_ready_marker(tmp_path):
    ledger = tmp_path / "pm_configs.json"
    ledger.write_text("{}", encoding="utf-8")
    marker = tmp_path / "done.json"
    marker.write_text('{"pid_alive": false}', encoding="utf-8")

    ok, reason = _validate_completion_marker(marker, ledger)

    assert ok is False
    assert "review_ready" in reason


def test_completion_marker_rejects_memory_pressure_stop(tmp_path):
    ledger = tmp_path / "pm_configs.json"
    ledger.write_text("{}", encoding="utf-8")
    marker = tmp_path / "done.json"
    marker.write_text(
        '{"review_ready": true, "pid_alive": false, "stopped_for_memory_pressure": true}',
        encoding="utf-8",
    )

    ok, reason = _validate_completion_marker(marker, ledger)

    assert ok is False
    assert "stopped_for_memory_pressure" in reason


def test_completion_marker_rejects_ledger_count_mismatch(tmp_path):
    ledger = tmp_path / "pm_configs.json"
    ledger.write_text('{"EURUSD": {}, "GBPUSD": {}}', encoding="utf-8")
    marker = tmp_path / "done.json"
    marker.write_text(
        '{"review_ready": true, "pid_alive": false, "ledger_count": 1}',
        encoding="utf-8",
    )

    ok, reason = _validate_completion_marker(marker, ledger)

    assert ok is False
    assert "ledger_count mismatch" in reason


def test_completion_marker_rejects_oos_coverage_below_ledger_count(tmp_path):
    ledger = tmp_path / "pm_configs.json"
    ledger.write_text('{"EURUSD": {}, "GBPUSD": {}}', encoding="utf-8")
    marker = tmp_path / "done.json"
    marker.write_text(
        '{"review_ready": true, "pid_alive": false, "ledger_count": 2, "oos_manifests": 1}',
        encoding="utf-8",
    )

    ok, reason = _validate_completion_marker(marker, ledger)

    assert ok is False
    assert "oos_manifests" in reason
