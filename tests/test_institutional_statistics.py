from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pytest

from pm_statistics import (
    LockboxAccessRegistry,
    aligned_cell_pbo,
    block_bootstrap_evidence,
    deflated_sharpe_evidence,
    effective_sample_size,
)


def test_dsr_uses_daily_observations_actual_trials_and_non_normal_moments():
    rng = np.random.default_rng(7)
    strong = rng.normal(0.0020, 0.0040, size=756)
    result = deflated_sharpe_evidence(
        strong,
        attempted_trials=500,
        min_observations=252,
        min_effective_observations=60,
        min_dsr=0.95,
    )
    assert result["return_frequency"] == "daily_realized"
    assert result["observations"] == 756
    assert result["attempted_trials"] == 500
    assert result["kurtosis"] > 0.0
    assert result["status"] == "PASS"
    assert result["deflated_sharpe"] >= 0.95


def test_dsr_fails_closed_for_noise_or_insufficient_history():
    rng = np.random.default_rng(11)
    noise = rng.normal(0.0, 0.01, size=756)
    noisy = deflated_sharpe_evidence(
        noise,
        attempted_trials=1000,
        min_observations=252,
        min_effective_observations=60,
        min_dsr=0.95,
    )
    short = deflated_sharpe_evidence(
        noise[:30],
        attempted_trials=1,
        min_observations=252,
    )
    assert noisy["status"] == "FAIL"
    assert not noisy["passed"]
    assert short["status"] == "UNAVAILABLE"
    assert not short["passed"]


def _candidate(column, dates, index):
    return {
        "strategy_name": f"S{index}",
        "val_regime_metrics": {
            "TREND": {
                "daily_realized_returns": column.tolist(),
                "daily_return_dates": dates,
            }
        },
        "oos_evidence": {
            "candidate_id": f"candidate-{index}",
            "timeframe": "H1",
            "window": {"start_ts": dates[0], "end_ts": dates[-1]},
        },
    }


def test_aligned_exact_regime_pbo_passes_persistent_edge():
    rng = np.random.default_rng(13)
    observations = 500
    matrix = rng.normal(0.0, 0.01, size=(observations, 6))
    matrix[:, 0] += 0.004
    dates = [f"2024-{(index // 28) + 1:02d}-{(index % 28) + 1:02d}" for index in range(observations)]
    candidates = [_candidate(matrix[:, index], dates, index) for index in range(6)]
    result = aligned_cell_pbo(
        candidates,
        "TREND",
        min_candidates=5,
        min_observations=252,
        max_pbo=0.50,
        blocks=10,
    )
    assert result["alignment"] == "exact_regime_identical_daily_axis"
    assert result["status"] == "PASS"
    assert result["pbo"] <= 0.50


def test_pbo_rejects_misaligned_candidate_axis():
    rng = np.random.default_rng(17)
    matrix = rng.normal(0.0, 0.01, size=(300, 5))
    dates = [str(index) for index in range(300)]
    candidates = [_candidate(matrix[:, index], dates, index) for index in range(5)]
    candidates[-1]["val_regime_metrics"]["TREND"]["daily_return_dates"][-1] = "different"
    candidates[-1]["oos_evidence"]["window"]["end_ts"] = "different"
    result = aligned_cell_pbo(candidates, "TREND", min_candidates=5, min_observations=60)
    assert result["status"] == "UNAVAILABLE"
    assert not result["passed"]
    assert "axes" in result["reason"]


def test_pbo_rejects_interior_calendar_mismatch_with_same_endpoints_and_count():
    rng = np.random.default_rng(18)
    matrix = rng.normal(0.0, 0.01, size=(300, 5))
    dates = [f"day-{index:03d}" for index in range(300)]
    candidates = [_candidate(matrix[:, index], dates, index) for index in range(5)]
    candidates[-1]["val_regime_metrics"]["TREND"]["daily_return_dates"] = list(dates)
    candidates[-1]["val_regime_metrics"]["TREND"]["daily_return_dates"][150] = "different-interior-day"

    result = aligned_cell_pbo(candidates, "TREND", min_candidates=5, min_observations=60)

    assert result["status"] == "UNAVAILABLE"
    assert not result["passed"]
    assert "axes" in result["reason"]


def test_pbo_counts_unique_effective_candidate_ids_only():
    rng = np.random.default_rng(19)
    matrix = rng.normal(0.0, 0.01, size=(300, 4))
    dates = [f"day-{index:03d}" for index in range(300)]
    candidates = [_candidate(matrix[:, index], dates, index) for index in range(4)]
    candidates.append(json.loads(json.dumps(candidates[-1])))

    result = aligned_cell_pbo(candidates, "TREND", min_candidates=5, min_observations=60)

    assert result["status"] == "UNAVAILABLE"
    assert not result["passed"]
    assert "unique aligned candidates 4 < 5" in result["reason"]


def test_dsr_and_bootstrap_do_not_silently_drop_ruin_or_nan_observations():
    rng = np.random.default_rng(20)
    corrupted = rng.normal(0.0025, 0.0020, size=500)
    corrupted[100] = -1.0
    dsr = deflated_sharpe_evidence(
        corrupted,
        attempted_trials=10,
        min_observations=252,
        min_effective_observations=60,
    )
    corrupted[100] = np.nan
    bootstrap = block_bootstrap_evidence(
        corrupted,
        repetitions=100,
        min_observations=252,
        min_effective_observations=60,
    )

    assert dsr["status"] == "UNAVAILABLE"
    assert bootstrap["status"] == "UNAVAILABLE"
    assert dsr["observations"] == 0
    assert bootstrap["observations"] == 0


def test_block_bootstrap_reports_effective_sample_and_confidence_gates():
    rng = np.random.default_rng(19)
    strong = rng.normal(0.0025, 0.0020, size=756)
    result = block_bootstrap_evidence(
        strong,
        repetitions=200,
        confidence=0.95,
        min_observations=252,
        min_effective_observations=60,
        min_profit_factor_lower=1.0,
        max_drawdown_upper=0.20,
    )
    assert result["status"] == "PASS"
    assert result["daily_mean_interval"][0] > 0.0
    assert result["profit_factor_interval"][0] > 1.0
    assert result["max_drawdown_fraction_interval"][2] <= 0.20
    assert 1.0 <= result["effective_observations"] <= 756
    assert effective_sample_size(np.ones(20)) == 0.0


def test_lockbox_registry_allows_exactly_one_access_across_restart(tmp_path):
    state = tmp_path / "lockbox.json"
    audit = tmp_path / "lockbox.jsonl"
    first_registry = LockboxAccessRegistry(state, audit)
    cell = {
        "symbol": "EURUSD",
        "timeframe": "H1",
        "regime": "TREND",
        "strategy_name": "Demo",
        "parameter_hash": "abc",
    }
    lineage = {
        "source_sha256": "deadbeef",
        "window_start": "2024-01-01",
        "window_end": "2026-01-01",
    }
    first = first_registry.consume(cell=cell, lineage=lineage)
    second = LockboxAccessRegistry(state, audit).consume(cell=cell, lineage=lineage)
    assert first.allowed
    assert not second.allowed
    assert first.access_id == second.access_id
    assert second.metadata["conflict_type"] == "EXACT_DUPLICATE"
    assert len(audit.read_text(encoding="utf-8").splitlines()) == 1
    payload = json.loads(state.read_text(encoding="utf-8"))
    assert first.access_id in payload["accesses"]


def test_lockbox_registry_rejects_partial_overlap_after_source_append(tmp_path):
    registry = LockboxAccessRegistry(tmp_path / "lockbox.json")
    cell = {"symbol": "EURUSD", "timeframe": "H1", "regime": "TREND"}
    first = registry.consume(
        cell={**cell, "strategy_name": "Original", "parameter_hash": "old"},
        lineage={
            "source_sha256": "source-before-append",
            "window_start": "2024-01-01T00:00:00Z",
            "window_end": "2026-01-01T00:00:00Z",
        },
    )
    overlap = registry.consume(
        cell={**cell, "strategy_name": "Reoptimized", "parameter_hash": "new"},
        lineage={
            "source_sha256": "source-after-append",
            "window_start": "2024-02-01T00:00:00Z",
            "window_end": "2026-02-01T00:00:00Z",
        },
    )

    assert first.allowed
    assert not overlap.allowed
    assert overlap.metadata["conflict_type"] == "INTERVAL_OVERLAP"
    assert overlap.metadata["prior_interval"]["window_start"].startswith("2024-01-01")
    assert "overlaps" in overlap.reason


def test_lockbox_registry_allows_disjoint_future_interval(tmp_path):
    registry = LockboxAccessRegistry(tmp_path / "lockbox.json")
    cell = {"symbol": "EURUSD", "timeframe": "H1", "regime": "TREND"}
    first = registry.consume(
        cell=cell,
        lineage={"source_sha256": "a", "window_start": "2024-01-01", "window_end": "2025-01-01"},
    )
    adjacent = registry.consume(
        cell=cell,
        lineage={"source_sha256": "b", "window_start": "2025-01-01", "window_end": "2026-01-01"},
    )

    assert first.allowed
    assert adjacent.allowed


def test_lockbox_registry_scopes_overlap_to_exact_experiment_cell(tmp_path):
    registry = LockboxAccessRegistry(tmp_path / "lockbox.json")
    lineage = {
        "source_sha256": "same",
        "window_start": "2024-01-01",
        "window_end": "2026-01-01",
    }
    trend = registry.consume(
        cell={"symbol": "EURUSD", "timeframe": "H1", "regime": "TREND"},
        lineage=lineage,
    )
    range_cell = registry.consume(
        cell={"symbol": "EURUSD", "timeframe": "H1", "regime": "RANGE"},
        lineage=lineage,
    )

    assert trend.allowed
    assert range_cell.allowed


def test_lockbox_registry_blocks_per_cell_then_overlapping_global_access(tmp_path):
    registry = LockboxAccessRegistry(tmp_path / "lockbox.json")
    prior = registry.consume(
        cell={"symbol": "EURUSD", "timeframe": "H1", "regime": "TREND"},
        lineage={
            "source_sha256": "cell",
            "window_start": "2024-01-01",
            "window_end": "2026-01-01",
        },
    )
    global_access = registry.consume(
        cell={"symbol": "__GLOBAL_PORTFOLIO__", "timeframe": "ALL", "regime": "ALL"},
        lineage={
            "source_sha256": "roster",
            "window_start": "2025-01-01",
            "window_end": "2027-01-01",
        },
    )
    assert prior.allowed
    assert not global_access.allowed
    assert global_access.metadata["conflict_type"] == "GLOBAL_INTERVAL_OVERLAP"


def test_lockbox_registry_blocks_global_then_overlapping_per_cell_access(tmp_path):
    registry = LockboxAccessRegistry(tmp_path / "lockbox.json")
    global_access = registry.consume(
        cell={"symbol": "__GLOBAL_PORTFOLIO__", "timeframe": "ALL", "regime": "ALL"},
        lineage={
            "source_sha256": "roster",
            "window_start": "2024-01-01",
            "window_end": "2026-01-01",
        },
    )
    cell_access = registry.consume(
        cell={"symbol": "GBPUSD", "timeframe": "D1", "regime": "CHOP"},
        lineage={
            "source_sha256": "cell",
            "window_start": "2025-01-01",
            "window_end": "2027-01-01",
        },
    )
    assert global_access.allowed
    assert not cell_access.allowed
    assert cell_access.metadata["conflict_type"] == "GLOBAL_INTERVAL_OVERLAP"


def test_lockbox_registry_allows_disjoint_global_and_cell_access(tmp_path):
    registry = LockboxAccessRegistry(tmp_path / "lockbox.json")
    global_access = registry.consume(
        cell={"symbol": "__GLOBAL_PORTFOLIO__", "timeframe": "ALL", "regime": "ALL"},
        lineage={
            "source_sha256": "roster",
            "window_start": "2024-01-01",
            "window_end": "2025-01-01",
        },
    )
    adjacent_cell = registry.consume(
        cell={"symbol": "EURUSD", "timeframe": "H1", "regime": "TREND"},
        lineage={
            "source_sha256": "cell",
            "window_start": "2025-01-01",
            "window_end": "2026-01-01",
        },
    )
    assert global_access.allowed
    assert adjacent_cell.allowed


def test_corrupt_lockbox_registry_fails_closed(tmp_path):
    state = tmp_path / "lockbox.json"
    state.write_text("not-json", encoding="utf-8")
    registry = LockboxAccessRegistry(state)
    with pytest.raises(RuntimeError, match="registry corrupt"):
        registry.consume(cell={"symbol": "EURUSD"}, lineage={"source_sha256": "x"})
