"""Regression test for audit-002 H1: PBO must partition by aligned (timeframe, window).

CSCV assumes all matrix columns share one calendar row axis. The engine emits one matrix per
symbol mixing timeframes, whose relative-block rows are NOT coincident across timeframes. The
PBO consumer must group columns into aligned (timeframe, start_ts, end_ts) sets and run CSCV
within each. This test fails if the timeframe-aware grouping regresses.
"""
import sys
from pathlib import Path

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "tools"))
from pbo import pbo_by_aligned_group  # noqa: E402


def _two_tf_manifest():
    return {"records": [
        {"matrix_column": 0, "timeframe": "H1", "start_ts": "a", "end_ts": "b"},
        {"matrix_column": 1, "timeframe": "H1", "start_ts": "a", "end_ts": "b"},
        {"matrix_column": 2, "timeframe": "H1", "start_ts": "a", "end_ts": "b"},
        {"matrix_column": 3, "timeframe": "M5", "start_ts": "c", "end_ts": "d"},
        {"matrix_column": 4, "timeframe": "M5", "start_ts": "c", "end_ts": "d"},
        {"matrix_column": 5, "timeframe": "M5", "start_ts": "c", "end_ts": "d"},
    ]}


def test_pbo_partitions_by_timeframe():
    rng = np.random.RandomState(2)
    T = 2000
    M = np.zeros((T, 6))
    M[:, :3] = rng.randn(T, 3) * 0.01
    M[:, 0] += 0.002          # H1 group: one genuine persistent edge -> low PBO
    M[:, 3:] = rng.randn(T, 3) * 0.01  # M5 group: pure noise -> high PBO
    res = pbo_by_aligned_group(M, _two_tf_manifest(), n_blocks=16,
                               candidate_ids=[f"c{i}" for i in range(6)])
    assert res["alignment"] == "per_timeframe_window"
    assert res["n_groups"] == 2
    by_tf = {g["timeframe"]: g for g in res["groups"]}
    assert by_tf["H1"]["pbo"] < 0.25, "genuine-edge timeframe group should have low PBO"
    assert by_tf["M5"]["pbo"] > 0.4, "noise timeframe group should have high PBO"
    # Worst-group is the conservative gate read and must surface the noise group.
    assert res["worst_group_pbo"] == by_tf["M5"]["pbo"]


def test_missing_window_metadata_flags_unverified():
    rng = np.random.RandomState(5)
    M = rng.randn(800, 4) * 0.01
    res = pbo_by_aligned_group(M, {"records": []}, n_blocks=16)
    assert res["alignment"] == "UNVERIFIED"
