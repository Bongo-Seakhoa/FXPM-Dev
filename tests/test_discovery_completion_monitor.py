import json
from pathlib import Path

import tools.discovery_completion_monitor as monitor


def test_completion_report_ignores_stale_cell_safety_when_current_audit_failed(tmp_path, monkeypatch):
    monkeypatch.setattr(monitor, "REPO", tmp_path)
    label = "unit_stale_safety"
    experiments = tmp_path / "pm_outputs" / "experiments"
    experiments.mkdir(parents=True)
    (experiments / f"{label}_promotion.json").write_text(
        json.dumps({"counts": {"PROMOTE": 1, "REVIEW": 0, "REJECT": 0}, "rows": []}),
        encoding="utf-8",
    )

    output_dir = tmp_path / "run"
    benchmarks = output_dir / "benchmarks"
    benchmarks.mkdir(parents=True)
    # This stale clean safety artifact must not rescue a failed current cell audit.
    (benchmarks / f"{label}_promotion_cell_safety.json").write_text(
        json.dumps({"navigation_counts": {"final_promotes": 1, "unsafe_promotes": 0}}),
        encoding="utf-8",
    )

    ledger = output_dir / "pm_configs.json"
    ledger.write_text(json.dumps({"EURUSD": {"regime_configs": {}}}), encoding="utf-8")
    evidence_dir = output_dir / "oos_evidence"
    evidence_dir.mkdir()

    verdict, report = monitor._build_report(
        label,
        tmp_path / "config.json",
        output_dir,
        ledger,
        evidence_dir,
        {
            "promotion_review": {"returncode": 0, "output": "ok"},
            "specialist_cell_audit": {"returncode": 1, "output": "failed"},
        },
        {"completed_at": "2026-06-07T00:00:00"},
    )

    assert verdict == "REVIEW_INCOMPLETE"
    assert "specialist-cell safety cross-check is unavailable" in report
