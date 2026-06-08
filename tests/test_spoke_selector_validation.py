import hashlib
import json
import sys
from pathlib import Path

import numpy as np


REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO / "tools"))

from spoke_selector_validation import (
    ENGINE_GATED_SPOKES,
    _candidate_is_gate_survivor,
    _paired_against_baseline,
    run_validation,
)


def _write_manifest(tmp_path, returns, records, symbol="TEST"):
    evidence_dir = tmp_path / "oos_evidence"
    evidence_dir.mkdir()
    candidate_ids = np.asarray([r["candidate_id"] for r in records]).astype(str)
    returns = np.asarray(returns, dtype=np.float32)
    matrix_path = evidence_dir / f"{symbol}_oos_returns_synth.npz"
    np.savez(matrix_path, returns=returns, candidate_ids=candidate_ids)
    digest_payload = returns.tobytes() + json.dumps(
        candidate_ids.tolist(),
        separators=(",", ":"),
    ).encode("utf-8")
    digest = hashlib.sha1(digest_payload).hexdigest()
    manifest = {
        "format_version": "oos_candidate_matrix_v1",
        "symbol": symbol,
        "record_count": len(records),
        "matrix_path": str(matrix_path),
        "matrix_digest_sha1": digest,
        "matrix_contract": {"returns_shape": list(returns.shape)},
        "records": records,
    }
    (evidence_dir / f"{symbol}_oos_manifest.json").write_text(
        json.dumps(manifest),
        encoding="utf-8",
    )
    return evidence_dir


def _record(
    candidate_id,
    col,
    *,
    train_trades=30,
    train_return=5.0,
    metrics_trades=999,
    execution_cost_survival=None,
    parameter_stability_evidence=None,
):
    rec = {
        "candidate_id": candidate_id,
        "symbol": "TEST",
        "timeframe": "H1",
        "strategy_name": f"Strategy{candidate_id}",
        "tuned_for_regime": "RANGE",
        "matrix_column": col,
        "metrics": {
            "total_trades": metrics_trades,
            "max_drawdown_pct": 1.0,
            "profit_factor": 9.0,
            "total_return_pct": 99.0,
        },
        "train_metrics": {
            "total_trades": train_trades,
            "total_return_pct": train_return,
        },
    }
    if execution_cost_survival is not None:
        rec["execution_cost_survival"] = execution_cost_survival
    if parameter_stability_evidence is not None:
        rec["parameter_stability_evidence"] = parameter_stability_evidence
    return rec


def test_gate_uses_train_and_selection_only_not_full_oos_metrics():
    rec = _record("low_train", 0, train_trades=3, metrics_trades=999)
    selection = np.array([0.01, 0.01, -0.002, 0.01], dtype=np.float64)

    assert not _candidate_is_gate_survivor(
        rec,
        selection,
        min_train_trades=20,
        dd_ceiling=35.0,
    )

    rec = _record("ok_train", 0, train_trades=30, metrics_trades=1)
    assert _candidate_is_gate_survivor(
        rec,
        selection,
        min_train_trades=20,
        dd_ceiling=35.0,
    )


def test_run_validation_excludes_full_oos_metric_leakage_and_reports_missing_spokes(tmp_path):
    returns = np.array(
        [
            [0.020, 0.005],
            [0.020, -0.002],
            [0.020, 0.005],
            [-0.001, -0.002],
            [-0.030, 0.010],
            [-0.020, 0.010],
            [-0.010, 0.010],
            [-0.010, 0.010],
        ],
        dtype=np.float32,
    )
    records = [
        _record("fake_full_oos_winner", 0, train_trades=2, metrics_trades=999),
        _record("real_train_survivor", 1, train_trades=30, metrics_trades=1),
    ]
    evidence_dir = _write_manifest(tmp_path, returns, records)

    result = run_validation(
        evidence_dir,
        min_train_trades=20,
        dd_ceiling=35.0,
        min_survivors=1,
        n_stages=4,
    )

    assert result["cells_evaluated"] == 1
    assert result["cells"][0]["gate_survivors"] == 1
    for rule_pick in result["cells"][0]["picks"].values():
        assert rule_pick["candidate_id"] == "real_train_survivor"
    assert result["engine_gated_spokes_missing"] == ENGINE_GATED_SPOKES
    assert result["spoke_availability"]["execution_cost_survival"]["available"] == 0
    assert result["spoke_availability"]["parameter_stability"]["available"] == 0


def test_run_validation_activates_engine_gated_spokes_when_v3_evidence_exists(tmp_path):
    returns = np.array(
        [
            [0.010, 0.004],
            [0.008, 0.003],
            [-0.002, 0.002],
            [0.006, -0.001],
            [0.005, 0.004],
            [0.005, 0.004],
            [-0.001, 0.002],
            [0.005, 0.003],
        ],
        dtype=np.float32,
    )
    cost_a = {"status": "available", "stress_return_pct": 1.5}
    cost_b = {"status": "available", "stress_return_pct": 0.8}
    stability_a = {"status": "available", "nearest_neighbor_median_score_ratio": 0.92}
    stability_b = {"status": "available", "nearest_neighbor_median_score_ratio": 0.77}
    records = [
        _record(
            "eight_spoke_a",
            0,
            execution_cost_survival=cost_a,
            parameter_stability_evidence=stability_a,
        ),
        _record(
            "eight_spoke_b",
            1,
            execution_cost_survival=cost_b,
            parameter_stability_evidence=stability_b,
        ),
    ]
    evidence_dir = _write_manifest(tmp_path, returns, records)

    result = run_validation(
        evidence_dir,
        min_train_trades=20,
        dd_ceiling=35.0,
        min_survivors=2,
        n_stages=4,
    )

    assert result["cells_evaluated"] == 1
    assert result["engine_gated_spokes_missing"] == {}
    assert result["engine_gated_spokes_partial"] == {}
    assert result["spoke_availability"]["execution_cost_survival"]["available_pct"] == 100.0
    assert result["spoke_availability"]["parameter_stability"]["available_pct"] == 100.0


def test_forward_split_scores_after_selection(tmp_path):
    returns = np.array(
        [
            [0.020, 0.004, 0.006],
            [0.020, -0.002, 0.006],
            [0.020, 0.004, -0.003],
            [-0.001, -0.002, 0.006],
            [-0.030, 0.010, 0.005],
            [-0.020, 0.010, 0.005],
            [-0.010, 0.010, 0.005],
            [-0.010, 0.010, 0.005],
        ],
        dtype=np.float32,
    )
    records = [
        _record("high_pf_forward_loser", 0),
        _record("balanced_forward_winner", 1),
        _record("steady_middle", 2),
    ]
    evidence_dir = _write_manifest(tmp_path, returns, records)

    result = run_validation(
        evidence_dir,
        min_train_trades=20,
        dd_ceiling=35.0,
        min_survivors=3,
        n_stages=4,
    )

    assert result["cells_evaluated"] == 1
    assert result["rule_stats"]["baseline_max_pf"]["median_forward_return_pct"] < 0.0
    assert result["rule_stats"]["baseline_max_pf"]["cells"] == 1
    assert "dd_aware_reliability" in result["rule_stats"]["spoke_face_geomean"]
    assert "paired_against_baseline_max_pf" in result
    assert "spoke_face_geomean" in result["rule_stats"]


def test_paired_comparison_reports_identical_and_different_picks():
    cells = [
        {
            "picks": {
                "baseline_max_pf": {
                    "candidate_id": "a",
                    "forward_return_pct": 1.0,
                    "forward_dd_pct": 3.0,
                },
                "spoke_face_geomean": {
                    "candidate_id": "a",
                    "forward_return_pct": 1.0,
                    "forward_dd_pct": 3.0,
                },
            },
        },
        {
            "picks": {
                "baseline_max_pf": {
                    "candidate_id": "b",
                    "forward_return_pct": -2.0,
                    "forward_dd_pct": 8.0,
                },
                "spoke_face_geomean": {
                    "candidate_id": "c",
                    "forward_return_pct": 4.0,
                    "forward_dd_pct": 4.0,
                },
            },
        },
        {
            "picks": {
                "baseline_max_pf": {
                    "candidate_id": "d",
                    "forward_return_pct": 3.0,
                    "forward_dd_pct": 2.0,
                },
                "spoke_face_geomean": {
                    "candidate_id": "e",
                    "forward_return_pct": 0.0,
                    "forward_dd_pct": 1.0,
                },
            },
        },
    ]

    paired = _paired_against_baseline(cells)["spoke_face_geomean"]

    assert paired["cells_compared"] == 3
    assert paired["identical_pick_count"] == 1
    assert paired["different_pick_count"] == 2
    assert paired["better_on_different_picks"] == 1
    assert paired["worse_on_different_picks"] == 1
    assert paired["sign_test_p_value"] == 1.0
