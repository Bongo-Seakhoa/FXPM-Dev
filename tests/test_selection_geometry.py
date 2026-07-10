import sys
from pathlib import Path

import numpy as np


REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO / "tools"))

from pareto_select import path_features, TRACE_FEATURE_NAMES
from selection_validation import (_candidate_is_gate_survivor,
                                  _pick, _shape_survivors, _trace_survivors,
                                  _trace_features_from_normalized, _pool_trace_features)


def test_path_features_penalize_one_large_dent():
    smooth = np.array([0.01] * 12, dtype=np.float64)
    dented = np.array([0.02, 0.02, -0.25, 0.02, 0.02, 0.02, 0.01, 0.01], dtype=np.float64)

    smooth_features = path_features(smooth)
    dented_features = path_features(dented)

    assert smooth_features is not None
    assert dented_features is not None
    assert smooth_features["dent_health"] > dented_features["dent_health"]
    assert smooth_features["expansion"] > dented_features["expansion"]


def test_selection_validation_gate_ignores_full_oos_metrics():
    rec = {
        "train_metrics": {"total_trades": 1},
        "metrics": {"total_trades": 999, "max_drawdown_pct": 0.0},
    }
    selection_returns = np.array([0.01, -0.50, 0.01], dtype=np.float64)

    assert not _candidate_is_gate_survivor(
        rec,
        selection_returns,
        min_trades=20,
        dd_ceiling=35.0,
    )


def test_shape_gate_rejects_path_dominated_high_pf_candidate():
    # Candidate 1 has the highest PF but is strictly worse on every path-shape axis.
    sel_axes = np.array(
        [
            [4.0, 1.5, 1.0, 1.0, -0.01, 80.0, 70.0, -2.0],
            [8.0, 9.0, 0.5, 0.7, -0.04, 30.0, 40.0, -8.0],
            [5.0, 2.2, 1.2, 1.1, -0.02, 75.0, 65.0, -3.0],
        ],
        dtype=np.float64,
    )
    sel_path = np.array(
        [
            [0.90, 0.90, 0.90, 0.90],
            [0.20, 0.20, 0.20, 0.20],
            [0.80, 0.95, 0.85, 0.95],
        ],
        dtype=np.float64,
    )

    survivors = _shape_survivors(sel_path)

    assert 1 not in survivors.tolist()
    sel_trace = np.ones((3, 5), dtype=np.float64)
    assert _pick("max_pf", sel_axes, sel_path, sel_trace) == 1
    assert _pick("shape_gate_pf", sel_axes, sel_path, sel_trace) in {0, 2}


def test_pool_normalized_trace_penalizes_one_axis_dent_without_collapsing_flat_axes():
    smooth = np.array(
        [
            [0.55, 0.60, 0.50, 0.65, 0.60],
            [0.60, 0.60, 0.55, 0.65, 0.65],
            [0.70, 0.60, 0.60, 0.65, 0.70],
            [0.80, 0.60, 0.70, 0.65, 0.80],
        ],
        dtype=np.float64,
    )
    dented = smooth.copy()
    dented[:, 0] = [0.55, 0.90, 0.35, 0.80]

    smooth_trace = _trace_features_from_normalized(smooth)
    dented_trace = _trace_features_from_normalized(dented)

    assert smooth_trace[2] > dented_trace[2]  # worst-axis dent health
    assert smooth_trace[3] > dented_trace[3]  # dent area / duration health
    assert smooth_trace[2] == 1.0             # flat non-problem axes stay healthy


def test_pool_trace_features_are_cross_candidate_stage_axis_relative():
    raw_traces = np.array(
        [
            [[2.0, 1.0], [5.0, 1.0], [6.0, 1.0], [7.0, 1.0]],
            [[1.0, 1.0], [4.0, 1.0], [1.0, 1.0], [4.5, 1.0]],
            [[0.5, 1.0], [1.0, 1.0], [2.0, 1.0], [3.0, 1.0]],
        ],
        dtype=np.float64,
    )

    features = _pool_trace_features(raw_traces)

    assert features.shape == (3, len(TRACE_FEATURE_NAMES))
    assert features[0, 2] > features[1, 2]
    assert features[0, 3] > features[1, 3]


def test_trace_gate_rejects_axis_trace_dominated_high_pf_candidate():
    sel_axes = np.array(
        [
            [4.0, 1.5, 1.0, 1.0, -0.01, 80.0, 70.0, -2.0],
            [8.0, 9.0, 0.5, 0.7, -0.04, 30.0, 40.0, -8.0],
            [5.0, 2.2, 1.2, 1.1, -0.02, 75.0, 65.0, -3.0],
        ],
        dtype=np.float64,
    )
    sel_path = np.ones((3, 4), dtype=np.float64)
    base_trace = np.array(
        [
            [0.90] * len(TRACE_FEATURE_NAMES),
            [0.10] * len(TRACE_FEATURE_NAMES),
            [0.80, 0.95, 0.85, 0.95, 0.90, 0.95, 0.90, 0.90, 0.95, 0.95, 0.95],
        ],
        dtype=np.float64,
    )
    sel_trace = base_trace[:, :len(TRACE_FEATURE_NAMES)]

    survivors = _trace_survivors(sel_trace)

    assert 1 not in survivors.tolist()
    assert _pick("max_pf", sel_axes, sel_path, sel_trace) == 1
    assert _pick("trace_gate_pf", sel_axes, sel_path, sel_trace) in {0, 2}


def test_evidence_geometry_penalizes_late_rescue_dependence():
    steady = np.array(
        [
            [0.40, 0.45, 0.45],
            [0.50, 0.55, 0.55],
            [0.60, 0.65, 0.65],
            [0.70, 0.75, 0.75],
        ],
        dtype=np.float64,
    )
    late_rescue = np.array(
        [
            [0.10, 0.45, 0.45],
            [0.10, 0.45, 0.45],
            [0.10, 0.45, 0.45],
            [0.70, 0.75, 0.75],
        ],
        dtype=np.float64,
    )

    steady_features = _trace_features_from_normalized(steady)
    late_features = _trace_features_from_normalized(late_rescue)

    late_rescue_idx = TRACE_FEATURE_NAMES.index("trace_late_rescue_independence")
    avg_path_idx = TRACE_FEATURE_NAMES.index("trace_average_path_quality")
    assert steady_features[late_rescue_idx] > late_features[late_rescue_idx]
    assert steady_features[avg_path_idx] > late_features[avg_path_idx]
