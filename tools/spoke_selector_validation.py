"""Validate Bongo's specialist-spoke evidence geometry against forward OOS evidence.

This is a read-only discovery tool. It does not change optimizer defaults and it does
not promote candidates. Its job is to answer a narrower, testable question:

    If each symbol x timeframe x regime cell selected one specialist using the
    available spider/spoke evidence path, would that selector beat simple baselines
    such as max PF on the unseen forward half of the OOS evidence?

The locked 8-spoke design is:

  edge_survival, profit_structure_quality, capital_preservation,
  tail_risk_control, temporal_consistency, generalization_survival,
  execution_cost_survival, parameter_stability

Only six spokes are currently available for every candidate in the OOS manifest.
The two engine-gated spokes are reported as missing evidence, not faked:

  execution_cost_survival: needs per-candidate stress/cost replay evidence
  parameter_stability: needs Optuna neighborhood/plateau evidence

Run:
  python tools/spoke_selector_validation.py
  python tools/spoke_selector_validation.py --symbols EURAUD USDJPY
"""

from __future__ import annotations

import argparse
import json
import math
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any

import numpy as np

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO / "tools"))

from pareto_select import pareto_mask, percentile_normalize, rank_pool
from pbo import load_oos_manifest_matrix
from specialist_spokes import (
    capital_preservation,
    edge_survival,
    generalization_survival,
    profit_structure_quality,
    tail_risk_control,
    temporal_consistency,
)


AVAILABLE_SPOKES = (
    "edge_survival",
    "profit_structure_quality",
    "capital_preservation",
    "tail_risk_control",
    "temporal_consistency",
    "generalization_survival",
    "execution_cost_survival",
    "parameter_stability",
)

TRACE_SPOKES = AVAILABLE_SPOKES[:6]

ENGINE_GATED_SPOKES = {
    "execution_cost_survival": "needs per-candidate stress/cost replay evidence",
    "parameter_stability": "needs Optuna neighborhood/plateau evidence",
}

TRACE_FEATURES = (
    "final_face_quality",
    "average_path_quality",
    "worst_axis_dent_health",
    "dent_area_health",
    "dent_recovery",
    "path_smoothness",
    "metric_stability",
    "cross_axis_balance",
    "risk_axis_integrity",
    "late_rescue_independence",
)

RULES = (
    "baseline_max_pf",
    "baseline_max_edge",
    "baseline_edge_per_ulcer",
    "spoke_face_geomean",
    "spoke_face_topsis",
    "spoke_trace_geomean",
    "spoke_face_trace_geomean",
    "spoke_trace_gate_pf",
    "spoke_trace_gate_edge",
    "ablation_no_tail_risk",
    "ablation_no_generalization",
    "ablation_core4_no_tail_no_gen",
)

_EPS = 1e-12


def _as_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except Exception:
        return default


def _as_int(value: Any, default: int = 0) -> int:
    try:
        if value is None:
            return default
        return int(value)
    except Exception:
        return default


def _active_count(r: np.ndarray) -> int:
    arr = np.asarray(r, dtype=np.float64)
    arr = arr[np.isfinite(arr)]
    return int((np.abs(arr) > _EPS).sum())


def _compound_return_pct(r: np.ndarray) -> float:
    arr = np.asarray(r, dtype=np.float64)
    arr = arr[np.isfinite(arr)]
    if not np.any(np.abs(arr) > _EPS):
        return 0.0
    return float((np.cumprod(1.0 + arr)[-1] - 1.0) * 100.0)


def _max_drawdown_pct(r: np.ndarray) -> float:
    arr = np.asarray(r, dtype=np.float64)
    arr = arr[np.isfinite(arr)]
    if not np.any(np.abs(arr) > _EPS):
        return 0.0
    eq = np.cumprod(1.0 + arr)
    peak = np.maximum.accumulate(eq)
    dd = np.where(peak > 0, (peak - eq) / peak, 0.0) * 100.0
    return float(dd.max())


def _train_return_pct(rec: dict[str, Any]) -> float:
    train = rec.get("train_metrics") or {}
    if not isinstance(train, dict):
        return 0.0
    return _as_float(train.get("total_return_pct"), 0.0)


def _execution_cost_survival(rec: dict[str, Any]) -> float:
    evidence = rec.get("execution_cost_survival") or {}
    if not isinstance(evidence, dict) or evidence.get("status") != "available":
        return float("nan")
    return _as_float(evidence.get("stress_return_pct"), float("nan"))


def _parameter_stability(rec: dict[str, Any]) -> float:
    evidence = rec.get("parameter_stability_evidence") or {}
    if not isinstance(evidence, dict) or evidence.get("status") != "available":
        return float("nan")
    return _as_float(evidence.get("nearest_neighbor_median_score_ratio"), float("nan"))


def _base_spoke_vector(block_returns: np.ndarray, rec: dict[str, Any]) -> np.ndarray:
    """Six block/path-derivable spokes, all higher-is-better."""

    oos_return = edge_survival(block_returns)
    return np.array(
        [
            oos_return,
            profit_structure_quality(block_returns),
            capital_preservation(block_returns),
            tail_risk_control(block_returns),
            temporal_consistency(block_returns),
            generalization_survival(oos_return, _train_return_pct(rec)),
        ],
        dtype=np.float64,
    )


def _spoke_vector(block_returns: np.ndarray, rec: dict[str, Any]) -> np.ndarray:
    """Locked eight-spoke final face. Missing engine evidence stays NaN."""

    return np.concatenate(
        [
            _base_spoke_vector(block_returns, rec),
            np.array([
                _execution_cost_survival(rec),
                _parameter_stability(rec),
            ], dtype=np.float64),
        ]
    )


def _spoke_trace(
    selection_returns: np.ndarray,
    rec: dict[str, Any],
    *,
    n_stages: int,
) -> np.ndarray | None:
    """Cumulative evidence_step x spoke trace for one candidate."""

    arr = np.asarray(selection_returns, dtype=np.float64)
    arr = arr[np.isfinite(arr)]
    if _active_count(arr) < max(2, n_stages // 2):
        return None

    stages = [stage for stage in np.array_split(arr, n_stages) if stage.size]
    rows: list[np.ndarray] = []
    for idx in range(1, len(stages) + 1):
        prefix = np.concatenate(stages[:idx])
        rows.append(_base_spoke_vector(prefix, rec))
    if len(rows) < 2:
        return None
    return np.asarray(rows, dtype=np.float64)


def _face_quality_balance(face: np.ndarray) -> tuple[float, float]:
    face = np.asarray(face, dtype=np.float64)
    face = np.where(np.isfinite(face), face, 0.0)
    floored = np.clip(face, 1e-9, None)
    quality = float(np.exp(np.mean(np.log(floored))))
    mean = float(floored.mean())
    balance = float(quality / mean) if mean > 0 else 0.0
    return quality, balance


def _trace_features_from_normalized(trace: np.ndarray) -> np.ndarray:
    """Shape diagnostics for one pool-normalized candidate x evidence x spoke tensor."""

    trace = np.asarray(trace, dtype=np.float64)
    trace = np.where(np.isfinite(trace), trace, 0.0)
    if trace.ndim != 2 or trace.shape[0] < 2:
        return np.zeros(len(TRACE_FEATURES), dtype=np.float64)

    face_quality = np.array([_face_quality_balance(row)[0] for row in trace], dtype=np.float64)
    face_balance = np.array([_face_quality_balance(row)[1] for row in trace], dtype=np.float64)
    terminal = trace[-1]

    peak = np.maximum.accumulate(trace, axis=0)
    dents = np.clip(peak - trace, 0.0, 1.0)
    worst_axis_dent_health = float(1.0 - np.max(dents))
    dent_area_health = float(1.0 - np.mean(dents))

    peak_final = peak[-1]
    retention = np.ones_like(terminal)
    active_peak = peak_final > 1e-9
    retention[active_peak] = np.clip(terminal[active_peak] / peak_final[active_peak], 0.0, 1.0)
    dent_recovery = float(retention.mean())

    moves = np.diff(trace, axis=0)
    total_variation = np.abs(moves).sum(axis=0)
    direct_move = np.abs(terminal - trace[0])
    excess_chop = np.maximum(0.0, total_variation - direct_move)
    path_smoothness = float(1.0 / (1.0 + np.mean(excess_chop)))
    metric_stability = (
        float(1.0 / (1.0 + np.mean(np.std(moves, axis=0))))
        if moves.shape[0] > 1
        else 1.0
    )

    risk_indices = [
        TRACE_SPOKES.index("capital_preservation"),
        TRACE_SPOKES.index("tail_risk_control"),
    ]
    risk_axis_integrity = float(1.0 - np.max(dents[:, risk_indices]))

    quality_positive_progress = float(np.maximum(np.diff(face_quality), 0.0).sum())
    last_quality_jump = float(max(0.0, face_quality[-1] - face_quality[-2]))
    quality_late_share = (
        last_quality_jump / quality_positive_progress
        if quality_positive_progress > _EPS
        else 0.0
    )

    edge_idx = AVAILABLE_SPOKES.index("edge_survival")
    edge = trace[:, edge_idx]
    edge_positive_progress = float(np.maximum(np.diff(edge), 0.0).sum())
    last_edge_jump = float(max(0.0, edge[-1] - edge[-2]))
    edge_late_share = (
        last_edge_jump / edge_positive_progress
        if edge_positive_progress > _EPS
        else 0.0
    )

    final_minus_path = max(0.0, float(face_quality[-1] - face_quality.mean()))
    final_gap_share = final_minus_path / max(float(face_quality[-1]), _EPS)
    late_rescue_independence = float(
        1.0 - np.clip(max(quality_late_share, edge_late_share, final_gap_share), 0.0, 1.0)
    )

    return np.array(
        [
            float(face_quality[-1]),
            float(face_quality.mean()),
            worst_axis_dent_health,
            dent_area_health,
            dent_recovery,
            path_smoothness,
            metric_stability,
            float(face_balance.mean()),
            risk_axis_integrity,
            late_rescue_independence,
        ],
        dtype=np.float64,
    )


def _pool_normalized_traces(raw_traces: list[np.ndarray]) -> np.ndarray:
    tensor = np.asarray(raw_traces, dtype=np.float64)
    if tensor.ndim != 3:
        raise ValueError("raw_traces must have shape [candidates, stages, spokes]")
    norm = np.zeros_like(tensor, dtype=np.float64)
    for stage_idx in range(tensor.shape[1]):
        norm[:, stage_idx, :] = percentile_normalize(tensor[:, stage_idx, :])
    return norm


def _trace_features_for_pool(raw_traces: list[np.ndarray]) -> np.ndarray:
    norm = _pool_normalized_traces(raw_traces)
    return np.asarray([_trace_features_from_normalized(norm[i]) for i in range(norm.shape[0])])


def _choose_with_rank_pool(matrix: np.ndarray, *, method: str = "geomean") -> int:
    order, _scores, _mask, _volume, _balance = rank_pool(matrix, method=method)
    return int(order[0])


def _face_without(final_face: np.ndarray, *spoke_names: str) -> np.ndarray:
    drop = set(spoke_names)
    keep = [idx for idx, name in enumerate(AVAILABLE_SPOKES) if name not in drop]
    if not keep:
        raise ValueError("cannot drop every spoke")
    return final_face[:, keep]


def _rankable_face(final_face: np.ndarray, *drop_spokes: str) -> np.ndarray:
    matrix = _face_without(final_face, *drop_spokes) if drop_spokes else np.asarray(final_face, dtype=np.float64)
    keep = np.all(np.isfinite(matrix), axis=0)
    if not keep.any():
        # Defensive fallback: preserve the original six-spoke behavior for old manifests.
        keep = np.zeros(matrix.shape[1], dtype=bool)
        keep[: min(6, matrix.shape[1])] = True
    return matrix[:, keep]


def _pick(rule: str, final_face: np.ndarray, trace_features: np.ndarray) -> int:
    """Pick one candidate using only selection-half information."""

    edge_idx = AVAILABLE_SPOKES.index("edge_survival")
    pf_idx = AVAILABLE_SPOKES.index("profit_structure_quality")
    cap_idx = AVAILABLE_SPOKES.index("capital_preservation")

    if rule == "baseline_max_pf":
        return int(np.argmax(final_face[:, pf_idx]))
    if rule == "baseline_max_edge":
        return int(np.argmax(final_face[:, edge_idx]))
    if rule == "baseline_edge_per_ulcer":
        ulcer = np.abs(final_face[:, cap_idx])
        score = np.divide(final_face[:, edge_idx], np.maximum(ulcer, 1e-9))
        return int(np.argmax(score))
    if rule == "spoke_face_geomean":
        return _choose_with_rank_pool(_rankable_face(final_face), method="geomean")
    if rule == "spoke_face_topsis":
        return _choose_with_rank_pool(_rankable_face(final_face), method="topsis")
    if rule == "spoke_trace_geomean":
        return _choose_with_rank_pool(trace_features, method="geomean")
    if rule == "spoke_face_trace_geomean":
        return _choose_with_rank_pool(np.hstack([_rankable_face(final_face), trace_features]), method="geomean")
    if rule == "spoke_trace_gate_pf":
        idx = np.flatnonzero(pareto_mask(trace_features))
        if idx.size == 0:
            idx = np.arange(final_face.shape[0])
        return int(idx[int(np.argmax(final_face[idx, pf_idx]))])
    if rule == "spoke_trace_gate_edge":
        idx = np.flatnonzero(pareto_mask(trace_features))
        if idx.size == 0:
            idx = np.arange(final_face.shape[0])
        return int(idx[int(np.argmax(final_face[idx, edge_idx]))])
    if rule == "ablation_no_tail_risk":
        return _choose_with_rank_pool(_rankable_face(final_face, "tail_risk_control"), method="geomean")
    if rule == "ablation_no_generalization":
        return _choose_with_rank_pool(_rankable_face(final_face, "generalization_survival"), method="geomean")
    if rule == "ablation_core4_no_tail_no_gen":
        return _choose_with_rank_pool(
            _rankable_face(final_face, "tail_risk_control", "generalization_survival"),
            method="geomean",
        )
    raise ValueError(f"unknown rule: {rule}")


def _regime_key(rec: dict[str, Any]) -> str:
    return str(rec.get("tuned_for_regime") or "BASE")


def _train_trades(rec: dict[str, Any]) -> int:
    train = rec.get("train_metrics") or {}
    if not isinstance(train, dict):
        return 0
    return _as_int(train.get("total_trades"), 0)


def _candidate_is_gate_survivor(
    rec: dict[str, Any],
    selection_returns: np.ndarray,
    *,
    min_train_trades: int,
    dd_ceiling: float,
) -> bool:
    """Hard survivor gate using only pre-forward evidence.

    Do not read `rec["metrics"]` here: those metrics cover the full OOS matrix and
    would leak the forward half into the selector validation.
    """

    train_trades = _train_trades(rec)
    selection_drawdown = _max_drawdown_pct(selection_returns)
    return train_trades >= min_train_trades and selection_drawdown <= dd_ceiling


def _cell_candidate_payload(
    returns: np.ndarray,
    records: dict[int, dict[str, Any]],
    cols: list[int],
    *,
    min_train_trades: int,
    dd_ceiling: float,
    n_stages: int,
) -> list[dict[str, Any]]:
    periods = returns.shape[0]
    half = periods // 2
    out: list[dict[str, Any]] = []
    for col in cols:
        rec = records.get(int(col))
        if rec is None:
            continue
        selection = np.asarray(returns[:half, col], dtype=np.float64)
        forward = np.asarray(returns[half:, col], dtype=np.float64)
        if not _candidate_is_gate_survivor(
            rec,
            selection,
            min_train_trades=min_train_trades,
            dd_ceiling=dd_ceiling,
        ):
            continue
        if _active_count(selection) < 2:
            continue
        trace = _spoke_trace(selection, rec, n_stages=n_stages)
        if trace is None:
            continue
        final_face = _spoke_vector(selection, rec)
        out.append(
            {
                "col": int(col),
                "record": rec,
                "final_face": final_face,
                "trace": trace,
                "forward_return_pct": _compound_return_pct(forward),
                "forward_dd_pct": _max_drawdown_pct(forward),
            }
        )
    return out


def _evaluate_symbol(
    manifest_path: Path,
    *,
    min_train_trades: int,
    dd_ceiling: float,
    min_survivors: int,
    n_stages: int,
) -> tuple[
    list[dict[str, Any]],
    dict[str, list[float]],
    dict[str, list[float]],
    dict[str, dict[str, list[float]]],
]:
    returns, manifest, _candidate_ids = load_oos_manifest_matrix(manifest_path)
    records_by_col: dict[int, dict[str, Any]] = {}
    cells: dict[tuple[str, str, str], list[int]] = defaultdict(list)

    for rec in manifest.get("records", []):
        col = rec.get("matrix_column")
        if not isinstance(col, int) or col < 0 or col >= returns.shape[1]:
            continue
        records_by_col[int(col)] = rec
        symbol = str(rec.get("symbol") or manifest.get("symbol") or manifest_path.stem)
        tf = str(rec.get("timeframe") or "")
        regime = _regime_key(rec)
        cells[(symbol, tf, regime)].append(int(col))

    cell_results: list[dict[str, Any]] = []
    forward_by_rule = {rule: [] for rule in RULES}
    drawdown_by_rule = {rule: [] for rule in RULES}
    spoke_samples = {name: {"spoke": [], "forward_return": [], "forward_capital": []} for name in AVAILABLE_SPOKES}

    for (symbol, tf, regime), cols in sorted(cells.items()):
        payload = _cell_candidate_payload(
            returns,
            records_by_col,
            cols,
            min_train_trades=min_train_trades,
            dd_ceiling=dd_ceiling,
            n_stages=n_stages,
        )
        if len(payload) < min_survivors:
            continue

        final_face = np.asarray([item["final_face"] for item in payload], dtype=np.float64)
        trace_features = _trace_features_for_pool([item["trace"] for item in payload])
        forward_returns = np.asarray([item["forward_return_pct"] for item in payload], dtype=np.float64)
        forward_dds = np.asarray([item["forward_dd_pct"] for item in payload], dtype=np.float64)

        for axis_idx, name in enumerate(AVAILABLE_SPOKES):
            spoke_samples[name]["spoke"].extend(final_face[:, axis_idx].tolist())
            spoke_samples[name]["forward_return"].extend(forward_returns.tolist())
            spoke_samples[name]["forward_capital"].extend((-forward_dds).tolist())

        picks: dict[str, dict[str, Any]] = {}
        for rule in RULES:
            pick_idx = _pick(rule, final_face, trace_features)
            picked = payload[pick_idx]["record"]
            forward_by_rule[rule].append(float(forward_returns[pick_idx]))
            drawdown_by_rule[rule].append(float(forward_dds[pick_idx]))
            picks[rule] = {
                "candidate_id": str(picked.get("candidate_id", "")),
                "strategy_name": str(picked.get("strategy_name", "")),
                "forward_return_pct": round(float(forward_returns[pick_idx]), 6),
                "forward_dd_pct": round(float(forward_dds[pick_idx]), 6),
            }

        cell_results.append(
            {
                "symbol": symbol,
                "timeframe": tf,
                "regime": regime,
                "candidate_count": len(cols),
                "gate_survivors": len(payload),
                "picks": picks,
            }
        )

    return cell_results, forward_by_rule, drawdown_by_rule, spoke_samples


def _rule_stats(forward: list[float], drawdowns: list[float]) -> dict[str, Any]:
    arr = np.asarray(forward, dtype=np.float64)
    dd = np.asarray(drawdowns, dtype=np.float64)
    if arr.size == 0:
        return {
            "cells": 0,
            "mean_forward_return_pct": None,
            "median_forward_return_pct": None,
            "profitable_pct": None,
            "mean_forward_dd_pct": None,
            "reliability": None,
            "dd_aware_reliability": None,
        }
    profitable = float((arr > 0).mean())
    median = float(np.median(arr))
    mean_dd = float(np.mean(dd))
    reliability = float(median * profitable)
    return {
        "cells": int(arr.size),
        "mean_forward_return_pct": round(float(np.mean(arr)), 6),
        "median_forward_return_pct": round(median, 6),
        "profitable_pct": round(profitable * 100.0, 6),
        "mean_forward_dd_pct": round(mean_dd, 6),
        "worst_forward_return_pct": round(float(np.min(arr)), 6),
        "reliability": round(reliability, 6),
        "dd_aware_reliability": round(float(reliability / (1.0 + max(mean_dd, 0.0))), 6),
    }


def _corr(x: list[float], y: list[float]) -> float | None:
    a = np.asarray(x, dtype=np.float64)
    b = np.asarray(y, dtype=np.float64)
    mask = np.isfinite(a) & np.isfinite(b)
    if int(mask.sum()) < 20:
        return None
    a = a[mask]
    b = b[mask]
    if float(np.std(a)) <= _EPS or float(np.std(b)) <= _EPS:
        return None
    return round(float(np.corrcoef(a, b)[0, 1]), 6)


def _sign_test_p_value(better: int, worse: int) -> float | None:
    """Two-sided exact sign-test p-value for paired non-tie outcomes."""

    n = int(better) + int(worse)
    if n == 0:
        return None
    k = min(int(better), int(worse))
    prob = 0.5 ** n
    tail = prob
    current = prob
    for i in range(0, k):
        current = current * (n - i) / (i + 1)
        tail += current
    return round(float(min(1.0, 2.0 * tail)), 6)


def _paired_against_baseline(
    cells: list[dict[str, Any]],
    *,
    baseline_rule: str = "baseline_max_pf",
) -> dict[str, dict[str, Any]]:
    """Compare each rule's pick to the same cell's baseline pick."""

    out: dict[str, dict[str, Any]] = {}
    for rule in RULES:
        diffs: list[float] = []
        dd_diffs: list[float] = []
        identical = better = worse = ties = 0
        compared = 0
        for cell in cells:
            picks = cell.get("picks") or {}
            base = picks.get(baseline_rule)
            pick = picks.get(rule)
            if not isinstance(base, dict) or not isinstance(pick, dict):
                continue
            compared += 1
            same_candidate = str(base.get("candidate_id", "")) == str(pick.get("candidate_id", ""))
            if same_candidate:
                identical += 1
            diff = _as_float(pick.get("forward_return_pct"), 0.0) - _as_float(
                base.get("forward_return_pct"),
                0.0,
            )
            dd_diff = _as_float(pick.get("forward_dd_pct"), 0.0) - _as_float(
                base.get("forward_dd_pct"),
                0.0,
            )
            diffs.append(diff)
            dd_diffs.append(dd_diff)
            if not same_candidate:
                if diff > 1e-9:
                    better += 1
                elif diff < -1e-9:
                    worse += 1
                else:
                    ties += 1
        arr = np.asarray(diffs, dtype=np.float64)
        dd = np.asarray(dd_diffs, dtype=np.float64)
        out[rule] = {
            "baseline_rule": baseline_rule,
            "cells_compared": int(compared),
            "identical_pick_count": int(identical),
            "different_pick_count": int(compared - identical),
            "better_on_different_picks": int(better),
            "worse_on_different_picks": int(worse),
            "tie_on_different_picks": int(ties),
            "sign_test_p_value": _sign_test_p_value(better, worse),
            "median_forward_return_delta_pct": round(float(np.median(arr)), 6) if arr.size else None,
            "mean_forward_return_delta_pct": round(float(np.mean(arr)), 6) if arr.size else None,
            "median_forward_dd_delta_pct": round(float(np.median(dd)), 6) if dd.size else None,
            "mean_forward_dd_delta_pct": round(float(np.mean(dd)), 6) if dd.size else None,
        }
    return out


def _default_evidence_dir() -> Path:
    candidates = [
        REPO / "pm_outputs" / "production_live" / "oos_evidence",
    ]
    for path in candidates:
        if path.exists() and list(path.glob("*_oos_manifest.json")):
            return path
    return candidates[-1]


def run_validation(
    evidence_dir: Path,
    *,
    symbols: list[str] | None = None,
    min_train_trades: int = 20,
    dd_ceiling: float = 35.0,
    min_survivors: int = 4,
    n_stages: int = 4,
) -> dict[str, Any]:
    evidence_dir = Path(evidence_dir)
    manifest_paths = sorted(evidence_dir.glob("*_oos_manifest.json"))
    if symbols:
        allowed = {s.upper() for s in symbols}
        manifest_paths = [p for p in manifest_paths if p.name.split("_oos_manifest")[0].upper() in allowed]

    all_cells: list[dict[str, Any]] = []
    forward_by_rule = {rule: [] for rule in RULES}
    drawdown_by_rule = {rule: [] for rule in RULES}
    spoke_samples = {
        name: {"spoke": [], "forward_return": [], "forward_capital": []}
        for name in AVAILABLE_SPOKES
    }
    skipped: list[dict[str, str]] = []

    for manifest_path in manifest_paths:
        try:
            cells, fwd, dd, samples = _evaluate_symbol(
                manifest_path,
                min_train_trades=min_train_trades,
                dd_ceiling=dd_ceiling,
                min_survivors=min_survivors,
                n_stages=n_stages,
            )
        except Exception as exc:
            skipped.append({"path": str(manifest_path), "error": f"{type(exc).__name__}: {exc}"})
            continue
        all_cells.extend(cells)
        for rule in RULES:
            forward_by_rule[rule].extend(fwd[rule])
            drawdown_by_rule[rule].extend(dd[rule])
        for name in AVAILABLE_SPOKES:
            spoke_samples[name]["spoke"].extend(samples[name]["spoke"])
            spoke_samples[name]["forward_return"].extend(samples[name]["forward_return"])
            spoke_samples[name]["forward_capital"].extend(samples[name]["forward_capital"])

    rule_stats = {rule: _rule_stats(forward_by_rule[rule], drawdown_by_rule[rule]) for rule in RULES}
    ranked_rules = sorted(
        RULES,
        key=lambda rule: (
            rule_stats[rule]["reliability"] is not None,
            rule_stats[rule]["reliability"] if rule_stats[rule]["reliability"] is not None else -math.inf,
            -(rule_stats[rule]["mean_forward_dd_pct"] or math.inf),
        ),
        reverse=True,
    )
    ranked_rules_by_dd_aware = sorted(
        RULES,
        key=lambda rule: (
            rule_stats[rule]["dd_aware_reliability"] is not None,
            (
                rule_stats[rule]["dd_aware_reliability"]
                if rule_stats[rule]["dd_aware_reliability"] is not None
                else -math.inf
            ),
        ),
        reverse=True,
    )
    spoke_corr = {
        name: {
            "forward_return_corr": _corr(spoke_samples[name]["spoke"], spoke_samples[name]["forward_return"]),
            "forward_capital_preservation_corr": _corr(
                spoke_samples[name]["spoke"],
                spoke_samples[name]["forward_capital"],
            ),
        }
        for name in AVAILABLE_SPOKES
    }
    spoke_availability = {}
    for name in AVAILABLE_SPOKES:
        values = np.asarray(spoke_samples[name]["spoke"], dtype=np.float64)
        finite = np.isfinite(values)
        spoke_availability[name] = {
            "observations": int(values.size),
            "available": int(finite.sum()),
            "available_pct": round(float(finite.mean() * 100.0), 6) if values.size else 0.0,
        }
    engine_gated_missing = {
        name: reason
        for name, reason in ENGINE_GATED_SPOKES.items()
        if spoke_availability.get(name, {}).get("available", 0) == 0
    }
    engine_gated_partial = {
        name: {
            "reason": ENGINE_GATED_SPOKES[name],
            **spoke_availability.get(name, {}),
        }
        for name in ENGINE_GATED_SPOKES
        if 0 < int(spoke_availability.get(name, {}).get("available", 0)) < int(
            spoke_availability.get(name, {}).get("observations", 0)
        )
    }

    return {
        "artifact": "spoke_selector_validation",
        "artifact_version": "2026-06-02-eight-spoke-discovery-v3",
        "evidence_dir": str(evidence_dir),
        "manifest_count": len(manifest_paths),
        "skipped_manifests": skipped,
        "granularity": "symbol/timeframe/regime",
        "selection_split": "first_half_oos_blocks",
        "forward_split": "second_half_oos_blocks",
        "available_spokes": list(AVAILABLE_SPOKES),
        "engine_gated_spokes_missing": engine_gated_missing,
        "engine_gated_spokes_partial": engine_gated_partial,
        "spoke_availability": spoke_availability,
        "shape_features": list(TRACE_FEATURES),
        "experimental_ablation_rules": [
            "ablation_no_tail_risk",
            "ablation_no_generalization",
            "ablation_core4_no_tail_no_gen",
        ],
        "params": {
            "symbols": symbols,
            "min_train_trades": min_train_trades,
            "dd_ceiling": dd_ceiling,
            "min_survivors": min_survivors,
            "n_stages": n_stages,
        },
        "cells_evaluated": len(all_cells),
        "rule_stats": rule_stats,
        "ranked_rules_by_reliability": ranked_rules,
        "ranked_rules_by_dd_aware_reliability": ranked_rules_by_dd_aware,
        "paired_against_baseline_max_pf": _paired_against_baseline(all_cells),
        "spoke_forward_correlations": spoke_corr,
        "cells": all_cells,
        "interpretation_guardrails": [
            "This validates selector behavior on an existing OOS surface; it is not a promotion gate.",
            "Engine-gated spokes are used only when finite evidence is present; missing evidence is never treated as passed.",
            "Portfolio balance is intentionally absent; this is specialist-first per cell.",
            "Aggregate reliability is distributional; paired rule-vs-baseline results must be checked before claiming dominance.",
            "Defaults should not change unless this beats the W4 baseline after cross-audit.",
        ],
    }


def _print_summary(result: dict[str, Any]) -> None:
    print("=== AVAILABLE-SPOKE SPECIALIST SELECTOR VALIDATION ===")
    print(f"evidence_dir={result['evidence_dir']}")
    print(
        f"manifests={result['manifest_count']} cells={result['cells_evaluated']} "
        f"split={result['selection_split']} -> {result['forward_split']}"
    )
    if result["skipped_manifests"]:
        print(f"skipped_manifests={len(result['skipped_manifests'])}")
    print("\navailable spokes:")
    print("  " + ", ".join(result["available_spokes"]))
    missing = result.get("engine_gated_spokes_missing", {})
    partial = result.get("engine_gated_spokes_partial", {})
    print("missing engine-gated spokes:")
    if missing:
        for name, reason in missing.items():
            print(f"  {name}: {reason}")
    else:
        print("  none")
    if partial:
        print("partial engine-gated spokes:")
        for name, info in partial.items():
            print(
                f"  {name}: {info.get('available', 0)}/{info.get('observations', 0)} "
                f"({float(info.get('available_pct', 0.0) or 0.0):.1f}%)"
            )

    print(
        f"\n{'rule':28} {'median%':>9} {'%prof':>8} {'mean%':>9} "
        f"{'fwd_dd%':>9} {'reliab':>9} {'dd_rel':>9}"
    )
    for rule in result["ranked_rules_by_reliability"]:
        st = result["rule_stats"][rule]
        if st["cells"] == 0:
            continue
        print(
            f"{rule:28} {st['median_forward_return_pct']:>9.3f} "
            f"{st['profitable_pct']:>7.1f}% {st['mean_forward_return_pct']:>9.3f} "
            f"{st['mean_forward_dd_pct']:>9.3f} {st['reliability']:>9.3f} "
            f"{st['dd_aware_reliability']:>9.3f}"
        )

    paired = result.get("paired_against_baseline_max_pf") or {}
    if "spoke_face_geomean" in paired:
        p = paired["spoke_face_geomean"]
        print("\npaired vs baseline_max_pf (spoke_face_geomean):")
        print(
            f"  identical={p['identical_pick_count']} different={p['different_pick_count']} "
            f"better={p['better_on_different_picks']} worse={p['worse_on_different_picks']} "
            f"ties={p['tie_on_different_picks']} sign_p={p['sign_test_p_value']}"
        )
        print(
            f"  median_return_delta={p['median_forward_return_delta_pct']:.3f}% "
            f"mean_return_delta={p['mean_forward_return_delta_pct']:.3f}% "
            f"mean_dd_delta={p['mean_forward_dd_delta_pct']:.3f}%"
        )

    print("\nspoke forward correlations (selection-half spoke vs forward outcome):")
    for name, values in result["spoke_forward_correlations"].items():
        ret = values["forward_return_corr"]
        cap = values["forward_capital_preservation_corr"]
        ret_text = "n/a" if ret is None else f"{ret:+.3f}"
        cap_text = "n/a" if cap is None else f"{cap:+.3f}"
        availability = result.get("spoke_availability", {}).get(name, {})
        available_pct = float(availability.get("available_pct", 0.0) or 0.0)
        print(f"  {name:28} return={ret_text:>7}  capital={cap_text:>7}  available={available_pct:6.1f}%")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--evidence-dir", default=str(_default_evidence_dir()))
    parser.add_argument("--symbols", nargs="*")
    parser.add_argument(
        "--min-train-trades",
        type=int,
        default=20,
        help="Minimum training trades. Uses train_metrics only to avoid forward-OOS leakage.",
    )
    parser.add_argument("--dd-ceiling", type=float, default=35.0)
    parser.add_argument("--min-survivors", type=int, default=4)
    parser.add_argument("--n-stages", type=int, default=4)
    parser.add_argument(
        "--output",
        default=str(REPO / "pm_outputs" / "experiments" / "production_live_spoke_selector_validation.json"),
        help="Stable, overwritten discovery artifact path.",
    )
    parser.add_argument("--no-write", action="store_true")
    args = parser.parse_args(argv)

    result = run_validation(
        Path(args.evidence_dir),
        symbols=args.symbols,
        min_train_trades=args.min_train_trades,
        dd_ceiling=args.dd_ceiling,
        min_survivors=args.min_survivors,
        n_stages=args.n_stages,
    )
    _print_summary(result)

    if not args.no_write:
        out = Path(args.output)
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(json.dumps(result, indent=2, sort_keys=True), encoding="utf-8")
        print(f"\nwrote {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
