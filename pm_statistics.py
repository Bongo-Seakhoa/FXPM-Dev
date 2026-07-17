"""Institutional promotion statistics and one-shot lockbox lineage.

All Sharpe-family tests use the versioned dated daily net mark-to-market equity
return series. Trade count is never substituted for return observations. The
functions are deterministic and fail closed when alignment, depth, or
provenance is absent.
"""

from __future__ import annotations

import hashlib
import itertools
import json
import math
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from statistics import NormalDist
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

import numpy as np

from pm_sample_dependence import effective_sample_evidence


STATISTICS_CONTRACT_VERSION = (
    "2026-07-14-institutional-promotion-statistics-v4-diagnostic-dsr"
)
DAILY_NET_MTM_CONTRACT_VERSION = "daily_net_mtm_v1"
DSR_POPULATION_CONTRACT_VERSION = "aligned_validation_dsr_population_v1"
ENTRY_SLIPPAGE_TIMING_CONTRACT_VERSION = "entry_slippage_parity_v1"
SWAP_ACCRUAL_TIMING_CONTRACT_VERSION = "causal_swap_accrual_v1"
EXECUTION_COST_TIMING_CONTRACT = {
    "entry_slippage": ENTRY_SLIPPAGE_TIMING_CONTRACT_VERSION,
    "swap_accrual": SWAP_ACCRUAL_TIMING_CONTRACT_VERSION,
}
LOCKBOX_REGISTRY_VERSION = "2026-07-11-nonoverlapping-lockbox-v2"
_NORMAL = NormalDist()
_EULER_GAMMA = 0.5772156649015329


def _finite_returns(values: Iterable[Any]) -> np.ndarray:
    """Return a validated simple-return vector, or an empty vector on corruption.

    Statistical promotion evidence must never improve because a non-finite or
    ruin observation was silently discarded.  A simple return at or below
    ``-1`` is also outside the admissible compounded-wealth domain used by the
    bootstrap and therefore invalidates the entire vector.
    """
    output: List[float] = []
    source = [] if values is None else values
    for value in source:
        try:
            number = float(value)
        except (TypeError, ValueError, OverflowError):
            return np.asarray([], dtype=np.float64)
        if not math.isfinite(number) or number <= -1.0:
            return np.asarray([], dtype=np.float64)
        output.append(number)
    return np.asarray(output, dtype=np.float64)


def effective_sample_size(values: Sequence[float], max_lag: Optional[int] = None) -> float:
    """Conservative ACF/HAC-adjusted observation count.

    The compatibility scalar now delegates to FxPM's versioned dependence
    contract.  Promotion callers needing diagnostics should persist the full
    :func:`pm_sample_dependence.effective_sample_evidence` result.
    """
    array = _finite_returns(values)
    count = int(array.size)
    if count < 3:
        return float(count)
    evidence = effective_sample_evidence(
        array,
        max_acf_lag=max_lag,
        minimum_observations=3,
    )
    return float(evidence.effective_observations if evidence.passed else 0.0)


def return_moments(values: Sequence[float]) -> Dict[str, float]:
    array = _finite_returns(values)
    count = int(array.size)
    if count < 2:
        return {
            "observations": count,
            "mean": 0.0,
            "std": 0.0,
            "skew": 0.0,
            "kurtosis": 3.0,
            "sharpe_daily": 0.0,
            "sharpe_annualized": 0.0,
            "effective_observations": effective_sample_size(array),
        }
    mean = float(array.mean())
    std = float(array.std(ddof=1))
    population_std = float(array.std(ddof=0))
    if population_std > 1e-18:
        normalized = (array - mean) / population_std
        skew = float(np.mean(normalized ** 3))
        kurtosis = float(np.mean(normalized ** 4))
    else:
        skew, kurtosis = 0.0, 3.0
    sharpe_daily = mean / std if std > 1e-18 else 0.0
    return {
        "observations": count,
        "mean": mean,
        "std": std,
        "skew": skew,
        "kurtosis": kurtosis,
        "sharpe_daily": sharpe_daily,
        "sharpe_annualized": sharpe_daily * math.sqrt(252.0),
        "effective_observations": effective_sample_size(array),
    }


def deflated_sharpe_evidence(
    values: Sequence[float],
    *,
    attempted_trials: int,
    candidate_id: str = "",
    population_evidence: Optional[Mapping[str, Any]] = None,
    min_observations: int = 60,
    min_effective_observations: float = 30.0,
    min_dsr: float = 0.95,
) -> Dict[str, Any]:
    """Compute DSR using aligned cross-trial daily Sharpe dispersion.

    The Bailey/Lopez de Prado expected-maximum benchmark is scaled by the
    observed cross-trial Sharpe standard deviation and the correlation-adjusted
    effective number of independent frozen validation alternatives. Candidate
    PSR then uses its own non-normal moments and dependence-adjusted sample.
    """
    moments = return_moments(values)
    count = int(moments["observations"])
    effective = float(moments["effective_observations"])
    trials = attempted_trials if type(attempted_trials) is int else 0
    population = dict(population_evidence or {})
    base = {
        "contract_version": STATISTICS_CONTRACT_VERSION,
        "return_frequency": "daily_net_mtm",
        "attempted_trials": trials,
        **moments,
        "minimum_observations": int(min_observations),
        "minimum_effective_observations": float(min_effective_observations),
        "minimum_dsr": float(min_dsr),
        "execution_cost_timing_contract": dict(EXECUTION_COST_TIMING_CONTRACT),
    }
    population_ids = [str(value) for value in (population.get("candidate_ids") or [])]
    try:
        population_count = int(population.get("raw_frozen_candidates", 0))
        trial_sharpes = np.asarray(
            population.get("trial_sharpe_daily") or [], dtype=np.float64
        )
    except (TypeError, ValueError, OverflowError):
        population_count = 0
        trial_sharpes = np.asarray([], dtype=np.float64)
    population_valid = (
        trials > 0
        and
        str(population.get("contract_version") or "")
        == DSR_POPULATION_CONTRACT_VERSION
        and str(population.get("status") or "").upper() == "AVAILABLE"
        and population_count == trials
        and len(population_ids) == trials
        and len(set(population_ids)) == trials
        and trial_sharpes.shape == (trials,)
        and bool(np.isfinite(trial_sharpes).all())
        and bool(candidate_id)
        and str(candidate_id) in population_ids
    )
    if not population_valid:
        return {
            **base,
            "status": "UNAVAILABLE",
            "passed": False,
            "reason": "canonical aligned validation DSR population is unavailable",
            "deflated_sharpe": 0.0,
            "population_evidence": population,
        }
    if count < int(min_observations):
        return {**base, "status": "UNAVAILABLE", "passed": False, "reason": "insufficient daily net-MTM observations", "deflated_sharpe": 0.0}
    if effective < float(min_effective_observations):
        return {**base, "status": "UNAVAILABLE", "passed": False, "reason": "insufficient effective observations", "deflated_sharpe": 0.0}
    sharpe = float(moments["sharpe_daily"])
    skew = float(moments["skew"])
    kurtosis = float(moments["kurtosis"])
    variance_term = 1.0 - skew * sharpe + ((kurtosis - 1.0) / 4.0) * sharpe * sharpe
    if variance_term <= 0.0 or not math.isfinite(variance_term):
        return {**base, "status": "UNAVAILABLE", "passed": False, "reason": "invalid Sharpe variance", "deflated_sharpe": 0.0}
    try:
        independent_trials = float(
            population.get("effective_independent_trials", 0.0)
        )
        cross_trial_std = float(
            population.get("cross_trial_sharpe_std_daily", 0.0)
        )
        average_correlation = float(
            population.get("average_off_diagonal_return_correlation", 0.0)
        )
    except (TypeError, ValueError, OverflowError):
        return {
            **base,
            "status": "UNAVAILABLE",
            "passed": False,
            "reason": "canonical DSR population dispersion is invalid",
            "deflated_sharpe": 0.0,
            "population_evidence": population,
        }
    recomputed_std = float(np.std(trial_sharpes, ddof=1))
    recomputed_independent = max(
        1.0,
        min(float(trials), 1.0 + (trials - 1.0) * (1.0 - average_correlation)),
    )
    if (
        not math.isfinite(independent_trials)
        or not 1.0 <= independent_trials <= float(trials)
        or not math.isfinite(cross_trial_std)
        or cross_trial_std < 0.0
        or not math.isfinite(average_correlation)
        or not -1.0 <= average_correlation <= 1.0
        or not math.isclose(cross_trial_std, recomputed_std, rel_tol=1e-12, abs_tol=1e-15)
        or not math.isclose(
            independent_trials,
            recomputed_independent,
            rel_tol=1e-12,
            abs_tol=1e-12,
        )
    ):
        return {
            **base,
            "status": "UNAVAILABLE",
            "passed": False,
            "reason": "canonical DSR population dispersion is invalid",
            "deflated_sharpe": 0.0,
            "population_evidence": population,
        }
    if independent_trials > 1.0:
        upper_one = min(
            1.0 - 1e-12,
            1.0 - (1.0 / independent_trials),
        )
        upper_two = min(
            1.0 - 1e-12,
            1.0 - (1.0 / (independent_trials * math.e)),
        )
        expected_max = cross_trial_std * (
            (1.0 - _EULER_GAMMA) * _NORMAL.inv_cdf(upper_one)
            + _EULER_GAMMA * _NORMAL.inv_cdf(upper_two)
        )
    else:
        expected_max = 0.0
    sharpe_se = math.sqrt(variance_term / max(effective - 1.0, 1.0))
    if sharpe_se <= 0.0 or not math.isfinite(sharpe_se):
        dsr = 0.0
    else:
        dsr = float(_NORMAL.cdf((sharpe - expected_max) / sharpe_se))
    dsr = max(0.0, min(1.0, dsr))
    diagnostic_passed = bool(dsr >= float(min_dsr) and sharpe > 0.0)
    return {
        **base,
        "status": "DIAGNOSTIC_ONLY",
        "passed": False,
        "authoritative": False,
        "diagnostic_threshold_passed": diagnostic_passed,
        "reason": "unrecomputable population summaries cannot authorize promotion",
        "candidate_id": str(candidate_id),
        "raw_frozen_candidates": int(trials),
        "effective_independent_trials": independent_trials,
        "average_off_diagonal_return_correlation": average_correlation,
        "cross_trial_sharpe_variance_daily": float(
            population.get("cross_trial_sharpe_variance_daily", 0.0) or 0.0
        ),
        "cross_trial_sharpe_std_daily": cross_trial_std,
        "expected_max_selection_sharpe_daily": expected_max,
        "sharpe_standard_error": sharpe_se,
        "deflated_sharpe": dsr,
        "population_contract_version": DSR_POPULATION_CONTRACT_VERSION,
    }


def _performance(matrix: np.ndarray) -> np.ndarray:
    mean = matrix.mean(axis=0)
    std = matrix.std(axis=0, ddof=1)
    return np.divide(mean, std, out=np.zeros_like(mean), where=std > 1e-18)


def cscv_pbo(
    matrix: np.ndarray,
    *,
    candidate_ids: Optional[Sequence[str]] = None,
    blocks: int = 10,
    max_combinations: int = 2048,
) -> Dict[str, Any]:
    """Combinatorially symmetric cross-validation PBO on one aligned matrix."""
    data = np.asarray(matrix, dtype=np.float64)
    if data.ndim != 2 or not np.isfinite(data).all():
        return {"status": "UNAVAILABLE", "passed": False, "reason": "matrix is not finite 2D", "pbo": None}
    observations, candidates = data.shape
    block_count = max(2, int(blocks or 10))
    block_count -= block_count % 2
    if candidates < 2 or observations < block_count * 2:
        return {"status": "UNAVAILABLE", "passed": False, "reason": f"insufficient matrix T={observations} N={candidates}", "pbo": None}
    identifiers = [str(value) for value in (candidate_ids or range(candidates))]
    if len(identifiers) != candidates:
        return {"status": "UNAVAILABLE", "passed": False, "reason": "candidate id count mismatch", "pbo": None}
    segments = [data[index, :] for index in np.array_split(np.arange(observations), block_count)]
    combinations = list(itertools.combinations(range(block_count), block_count // 2))
    if len(combinations) > int(max_combinations):
        # Deterministic even spacing preserves complements without RNG state.
        indices = np.linspace(0, len(combinations) - 1, int(max_combinations), dtype=int)
        combinations = [combinations[index] for index in sorted(set(indices.tolist()))]
    logits: List[float] = []
    for selection in combinations:
        inside = set(selection)
        in_sample = np.vstack([segments[index] for index in range(block_count) if index in inside])
        out_sample = np.vstack([segments[index] for index in range(block_count) if index not in inside])
        selected = int(np.argmax(_performance(in_sample)))
        oos = _performance(out_sample)
        ordered = np.sort(oos)
        rank = float(np.searchsorted(ordered, oos[selected], side="left") / max(1, candidates - 1))
        clipped = min(max(rank, 1.0 / (candidates + 1.0)), 1.0 - 1.0 / (candidates + 1.0))
        logits.append(math.log(clipped / (1.0 - clipped)))
    if not logits:
        return {"status": "UNAVAILABLE", "passed": False, "reason": "no CSCV combinations", "pbo": None}
    pbo = float(np.mean(np.asarray(logits) < 0.0))
    return {
        "contract_version": STATISTICS_CONTRACT_VERSION,
        "status": "MEASURED",
        "pbo": pbo,
        "observations": observations,
        "candidates": candidates,
        "blocks": block_count,
        "combinations": len(logits),
        "logit_median": float(np.median(logits)),
        "candidate_ids": identifiers,
    }


def aligned_cell_pbo(
    candidates: Sequence[Mapping[str, Any]],
    regime: str,
    *,
    min_candidates: int = 5,
    min_observations: int = 60,
    max_pbo: float = 0.50,
    blocks: int = 10,
) -> Dict[str, Any]:
    """Build an exact-regime matrix only from identical calendar axes."""
    columns: List[np.ndarray] = []
    identifiers: List[str] = []
    alignment_keys: List[Tuple[str, str, str, int, str]] = []
    seen_identifiers: Dict[str, Tuple[np.ndarray, Tuple[str, str, str, int, str]]] = {}
    target = str(regime or "").upper()
    for index, candidate in enumerate(candidates or []):
        metrics = dict((candidate.get("val_regime_metrics") or {}).get(target, {}) or {})
        contract = dict(metrics.get("daily_net_mtm_contract") or {})
        swap_contract = dict(contract.get("swap_accrual_contract") or {})
        swap_status = str(swap_contract.get("status") or "").upper()
        raw_returns = list(metrics.get("daily_net_mtm_returns", []) or [])
        dates = [str(value) for value in (metrics.get("daily_net_mtm_dates", []) or [])]
        if not raw_returns and not dates:
            return {
                "contract_version": STATISTICS_CONTRACT_VERSION,
                "status": "UNAVAILABLE",
                "passed": False,
                "reason": f"candidate {index} is missing its daily-net-MTM surface",
                "pbo": None,
            }
        if (
            str(contract.get("contract_version") or "")
            != DAILY_NET_MTM_CONTRACT_VERSION
            or str(contract.get("status") or "").upper() != "AVAILABLE"
            or str(contract.get("partition") or "") != "tuning_validation"
            or str(contract.get("origin") or "")
            != "backtester_final_post_cost_equity_curve"
            or dict(contract.get("execution_cost_timing_contract") or {})
            != EXECUTION_COST_TIMING_CONTRACT
            or swap_status not in {"AVAILABLE", "DISABLED"}
            or not bool(swap_contract.get("execution_state_compounding", False))
            or (
                swap_status == "AVAILABLE"
                and str(swap_contract.get("timestamp_source") or "")
                != "_BAR_CLOSE_TS"
            )
        ):
            return {
                "contract_version": STATISTICS_CONTRACT_VERSION,
                "status": "UNAVAILABLE",
                "passed": False,
                "reason": f"candidate {index} lacks an admissible validation daily-net-MTM contract",
                "pbo": None,
            }
        returns = _finite_returns(raw_returns)
        if returns.size == 0:
            return {
                "contract_version": STATISTICS_CONTRACT_VERSION,
                "status": "UNAVAILABLE",
                "passed": False,
                "reason": f"candidate {index} has an invalid daily-net-MTM vector",
                "pbo": None,
            }
        if len(dates) != len(raw_returns) or any(not value.strip() for value in dates):
            return {
                "contract_version": STATISTICS_CONTRACT_VERSION,
                "status": "UNAVAILABLE",
                "passed": False,
                "reason": f"candidate {index} daily-net-MTM dates do not match its return vector",
                "pbo": None,
            }
        if len(set(dates)) != len(dates):
            return {
                "contract_version": STATISTICS_CONTRACT_VERSION,
                "status": "UNAVAILABLE",
                "passed": False,
                "reason": f"candidate {index} daily-net-MTM axis contains duplicate dates",
                "pbo": None,
            }
        date_axis_sha256 = hashlib.sha256(
            json.dumps(dates, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
        ).hexdigest()
        key = (
            str(candidate.get("timeframe") or contract.get("timeframe") or ""),
            str(contract.get("scored_start_ts") or dates[0]),
            str(contract.get("scored_end_ts") or dates[-1]),
            int(returns.size),
            date_axis_sha256,
        )
        identifier = str(candidate.get("candidate_id") or "")
        if not identifier:
            return {
                "contract_version": STATISTICS_CONTRACT_VERSION,
                "status": "UNAVAILABLE",
                "passed": False,
                "reason": f"candidate {index} is missing exact-cell identity",
                "pbo": None,
            }
        prior = seen_identifiers.get(identifier)
        if prior is not None:
            prior_returns, prior_key = prior
            if prior_key != key or not np.array_equal(prior_returns, returns):
                return {
                    "contract_version": STATISTICS_CONTRACT_VERSION,
                    "status": "UNAVAILABLE",
                    "passed": False,
                    "reason": f"duplicate candidate id {identifier!r} has conflicting evidence",
                    "pbo": None,
                }
            # The same effective model is one candidate, not extra multiplicity
            # that can manufacture the minimum pool size or distort ranks.
            continue
        seen_identifiers[identifier] = (returns.copy(), key)
        columns.append(returns)
        identifiers.append(identifier)
        alignment_keys.append(key)
    if len(columns) < int(min_candidates):
        return {"contract_version": STATISTICS_CONTRACT_VERSION, "status": "UNAVAILABLE", "passed": False, "reason": f"unique aligned candidates {len(columns)} < {int(min_candidates)}", "pbo": None}
    if len(set(alignment_keys)) != 1 or len({column.size for column in columns}) != 1:
        return {"contract_version": STATISTICS_CONTRACT_VERSION, "status": "UNAVAILABLE", "passed": False, "reason": "candidate daily-net-MTM axes are not identical", "pbo": None}
    matrix = np.column_stack(columns)
    if matrix.shape[0] < int(min_observations):
        return {"contract_version": STATISTICS_CONTRACT_VERSION, "status": "UNAVAILABLE", "passed": False, "reason": f"daily net-MTM observations {matrix.shape[0]} < {int(min_observations)}", "pbo": None}
    result = cscv_pbo(matrix, candidate_ids=identifiers, blocks=blocks)
    value = result.get("pbo")
    passed = value is not None and float(value) <= float(max_pbo)
    result.update({
        "alignment": "exact_regime_identical_daily_axis",
        "alignment_key": list(alignment_keys[0]),
        "maximum_pbo": float(max_pbo),
        "passed": bool(passed),
        "status": "PASS" if passed else ("FAIL" if value is not None else "UNAVAILABLE"),
        "execution_cost_timing_contract": dict(EXECUTION_COST_TIMING_CONTRACT),
    })
    return result


def aligned_validation_dsr_population(
    candidates: Sequence[Mapping[str, Any]],
    regime: str,
    *,
    min_observations: int = 60,
) -> Dict[str, Any]:
    """Measure correlated frozen-model multiplicity on one validation axis."""
    columns: List[np.ndarray] = []
    identifiers: List[str] = []
    alignment_keys: List[Tuple[str, str, str, int, str]] = []
    seen: Dict[str, Tuple[np.ndarray, Tuple[str, str, str, int, str]]] = {}
    target = str(regime or "").upper()
    for index, candidate in enumerate(candidates or []):
        metrics = dict(
            (candidate.get("val_regime_metrics") or {}).get(target, {}) or {}
        )
        contract = dict(metrics.get("daily_net_mtm_contract") or {})
        swap_contract = dict(contract.get("swap_accrual_contract") or {})
        swap_status = str(swap_contract.get("status") or "").upper()
        raw_returns = list(metrics.get("daily_net_mtm_returns", []) or [])
        dates = [
            str(value) for value in (metrics.get("daily_net_mtm_dates", []) or [])
        ]
        identifier = str(candidate.get("candidate_id") or "")
        if not identifier:
            return {
                "contract_version": DSR_POPULATION_CONTRACT_VERSION,
                "status": "UNAVAILABLE",
                "reason": f"candidate {index} is missing exact-cell identity",
            }
        if (
            str(contract.get("contract_version") or "")
            != DAILY_NET_MTM_CONTRACT_VERSION
            or str(contract.get("status") or "").upper() != "AVAILABLE"
            or str(contract.get("partition") or "") != "tuning_validation"
            or str(contract.get("origin") or "")
            != "backtester_final_post_cost_equity_curve"
            or dict(contract.get("execution_cost_timing_contract") or {})
            != EXECUTION_COST_TIMING_CONTRACT
            or swap_status not in {"AVAILABLE", "DISABLED"}
            or not bool(swap_contract.get("execution_state_compounding", False))
            or (
                swap_status == "AVAILABLE"
                and str(swap_contract.get("timestamp_source") or "")
                != "_BAR_CLOSE_TS"
            )
        ):
            return {
                "contract_version": DSR_POPULATION_CONTRACT_VERSION,
                "status": "UNAVAILABLE",
                "reason": f"candidate {index} lacks admissible validation daily-net-MTM",
            }
        returns = _finite_returns(raw_returns)
        if (
            returns.size == 0
            or len(dates) != len(raw_returns)
            or len(dates) != len(set(dates))
            or any(not value.strip() for value in dates)
        ):
            return {
                "contract_version": DSR_POPULATION_CONTRACT_VERSION,
                "status": "UNAVAILABLE",
                "reason": f"candidate {index} daily-net-MTM vector/axis is invalid",
            }
        date_axis_sha256 = hashlib.sha256(
            json.dumps(
                dates,
                separators=(",", ":"),
                ensure_ascii=False,
            ).encode("utf-8")
        ).hexdigest()
        key = (
            str(candidate.get("timeframe") or contract.get("timeframe") or ""),
            str(contract.get("scored_start_ts") or dates[0]),
            str(contract.get("scored_end_ts") or dates[-1]),
            int(returns.size),
            date_axis_sha256,
        )
        prior = seen.get(identifier)
        if prior is not None:
            if prior[1] != key or not np.array_equal(prior[0], returns):
                return {
                    "contract_version": DSR_POPULATION_CONTRACT_VERSION,
                    "status": "UNAVAILABLE",
                    "reason": f"duplicate candidate id {identifier!r} conflicts",
                }
            continue
        seen[identifier] = (returns.copy(), key)
        identifiers.append(identifier)
        columns.append(returns)
        alignment_keys.append(key)
    raw_count = len(columns)
    if raw_count < 2:
        return {
            "contract_version": DSR_POPULATION_CONTRACT_VERSION,
            "status": "UNAVAILABLE",
            "reason": "cross-trial Sharpe dispersion requires at least two frozen models",
            "raw_frozen_candidates": raw_count,
        }
    if len(set(alignment_keys)) != 1 or len({column.size for column in columns}) != 1:
        return {
            "contract_version": DSR_POPULATION_CONTRACT_VERSION,
            "status": "UNAVAILABLE",
            "reason": "candidate daily-net-MTM axes are not identical",
            "raw_frozen_candidates": raw_count,
        }
    matrix = np.column_stack(columns)
    if matrix.shape[0] < int(min_observations):
        return {
            "contract_version": DSR_POPULATION_CONTRACT_VERSION,
            "status": "UNAVAILABLE",
            "reason": (
                f"daily net-MTM observations {matrix.shape[0]} < "
                f"{int(min_observations)}"
            ),
            "raw_frozen_candidates": raw_count,
        }
    correlation = np.eye(raw_count, dtype=np.float64)
    for left in range(raw_count):
        left_values = matrix[:, left]
        left_constant = bool(np.std(left_values, ddof=0) <= 1e-18)
        for right in range(left + 1, raw_count):
            right_values = matrix[:, right]
            right_constant = bool(np.std(right_values, ddof=0) <= 1e-18)
            if left_constant or right_constant:
                value = (
                    1.0
                    if left_constant
                    and right_constant
                    and np.array_equal(left_values, right_values)
                    else 0.0
                )
            else:
                value = float(np.corrcoef(left_values, right_values)[0, 1])
            if not math.isfinite(value):
                return {
                    "contract_version": DSR_POPULATION_CONTRACT_VERSION,
                    "status": "UNAVAILABLE",
                    "reason": "cross-trial return correlation is non-finite",
                    "raw_frozen_candidates": raw_count,
                }
            correlation[left, right] = value
            correlation[right, left] = value
    off_diagonal = correlation[np.triu_indices(raw_count, k=1)]
    average_correlation = float(np.mean(off_diagonal))
    average_correlation = max(-1.0, min(1.0, average_correlation))
    independent_trials = 1.0 + (raw_count - 1.0) * (1.0 - average_correlation)
    independent_trials = max(1.0, min(float(raw_count), independent_trials))
    sharpe_estimates = _performance(matrix)
    if not np.isfinite(sharpe_estimates).all():
        return {
            "contract_version": DSR_POPULATION_CONTRACT_VERSION,
            "status": "UNAVAILABLE",
            "reason": "cross-trial Sharpe estimates are non-finite",
            "raw_frozen_candidates": raw_count,
        }
    cross_trial_variance = float(np.var(sharpe_estimates, ddof=1))
    cross_trial_std = math.sqrt(max(0.0, cross_trial_variance))
    return {
        "contract_version": DSR_POPULATION_CONTRACT_VERSION,
        "status": "AVAILABLE",
        "population_contract": (
            "predeclared_train_eligible_frozen_validation_roster_v1"
        ),
        "candidate_ids": identifiers,
        "raw_frozen_candidates": int(raw_count),
        "observations": int(matrix.shape[0]),
        "average_off_diagonal_return_correlation": average_correlation,
        "effective_independent_trials": independent_trials,
        "cross_trial_sharpe_variance_daily": cross_trial_variance,
        "cross_trial_sharpe_std_daily": cross_trial_std,
        "trial_sharpe_daily": [float(value) for value in sharpe_estimates.tolist()],
        "alignment": "exact_regime_identical_daily_axis",
        "alignment_key": list(alignment_keys[0]),
        "execution_cost_timing_contract": dict(EXECUTION_COST_TIMING_CONTRACT),
    }


def _max_drawdown_fraction(returns: np.ndarray) -> float:
    wealth = np.cumprod(1.0 + returns)
    if wealth.size == 0:
        return 0.0
    peaks = np.maximum.accumulate(np.concatenate(([1.0], wealth)))[1:]
    return float(np.max(np.where(peaks > 0.0, (peaks - wealth) / peaks, 0.0)))


def _profit_factor(returns: np.ndarray) -> float:
    gain = float(returns[returns > 0.0].sum())
    loss = float(-returns[returns < 0.0].sum())
    if loss > 1e-18:
        return gain / loss
    return float("inf") if gain > 0.0 else 0.0


def block_bootstrap_evidence(
    values: Sequence[float],
    *,
    repetitions: int = 500,
    confidence: float = 0.95,
    block_length: int = 0,
    seed: int = 1729,
    min_observations: int = 60,
    min_effective_observations: float = 30.0,
    min_mean_lower: float = 0.0,
    min_profit_factor_lower: float = 1.0,
    max_drawdown_upper: float = 0.20,
) -> Dict[str, Any]:
    """Moving-block bootstrap CIs for daily mean, PF, and path drawdown."""
    array = _finite_returns(values)
    count = int(array.size)
    effective = effective_sample_size(array)
    base = {
        "contract_version": STATISTICS_CONTRACT_VERSION,
        "return_frequency": "daily_net_mtm",
        "observations": count,
        "effective_observations": effective,
        "execution_cost_timing_contract": dict(EXECUTION_COST_TIMING_CONTRACT),
    }
    if count < int(min_observations) or effective < float(min_effective_observations):
        return {**base, "status": "UNAVAILABLE", "passed": False, "reason": "insufficient daily/effective observations"}
    reps = max(100, int(repetitions or 500))
    length = int(block_length or max(5, round(count ** (1.0 / 3.0))))
    length = max(2, min(length, count))
    rng = np.random.default_rng(int(seed))
    means = np.empty(reps, dtype=np.float64)
    factors = np.empty(reps, dtype=np.float64)
    drawdowns = np.empty(reps, dtype=np.float64)
    starts_needed = int(math.ceil(count / length))
    for iteration in range(reps):
        starts = rng.integers(0, count, size=starts_needed)
        sample = np.concatenate([
            array[(start + np.arange(length)) % count]
            for start in starts
        ])[:count]
        means[iteration] = float(sample.mean())
        factor = _profit_factor(sample)
        factors[iteration] = factor if math.isfinite(factor) else 1e6
        drawdowns[iteration] = _max_drawdown_fraction(sample)
    alpha = max(0.001, min(0.499, (1.0 - float(confidence)) / 2.0))
    mean_interval = np.quantile(means, [alpha, 0.5, 1.0 - alpha])
    pf_interval = np.quantile(factors, [alpha, 0.5, 1.0 - alpha])
    dd_interval = np.quantile(drawdowns, [alpha, 0.5, 1.0 - alpha])
    passed = bool(
        mean_interval[0] > float(min_mean_lower)
        and pf_interval[0] > float(min_profit_factor_lower)
        and dd_interval[2] <= float(max_drawdown_upper)
    )
    return {
        **base,
        "status": "PASS" if passed else "FAIL",
        "passed": passed,
        "reason": "all bootstrap confidence gates pass" if passed else "one or more bootstrap confidence gates fail",
        "repetitions": reps,
        "confidence": float(confidence),
        "block_length": length,
        "daily_mean_interval": mean_interval.tolist(),
        "profit_factor_interval": pf_interval.tolist(),
        "max_drawdown_fraction_interval": dd_interval.tolist(),
        "thresholds": {
            "min_daily_mean_lower": float(min_mean_lower),
            "min_profit_factor_lower": float(min_profit_factor_lower),
            "max_drawdown_fraction_upper": float(max_drawdown_upper),
        },
    }


@dataclass(frozen=True)
class LockboxDecision:
    allowed: bool
    access_id: str
    reason: str
    prior: Optional[Dict[str, Any]] = None
    metadata: Optional[Dict[str, Any]] = None


class LockboxAccessRegistry:
    """Atomic registry permitting one non-overlapping experiment per cell.

    Appending data changes a source hash and shifts a rolling lockbox boundary,
    but it does not make the overlapping observations unseen again.  Therefore
    the consumed unit is the timestamp interval for a
    symbol×timeframe×regime experiment, irrespective of strategy identity,
    parameters, or source-file hash.
    """

    _GLOBAL_EXPERIMENT_CELL = ("__GLOBAL_PORTFOLIO__", "ALL", "ALL")

    def __init__(self, state_path: Path, audit_path: Optional[Path] = None) -> None:
        self.state_path = Path(state_path)
        self.audit_path = Path(audit_path or self.state_path.with_suffix(".jsonl"))

    @staticmethod
    def _fingerprint(payload: Mapping[str, Any]) -> str:
        encoded = json.dumps(dict(payload), sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")
        return hashlib.sha256(encoded).hexdigest()

    @staticmethod
    def _experiment_cell(cell: Mapping[str, Any]) -> Tuple[str, str, str]:
        return (
            str(cell.get("symbol", "") or "").strip().upper(),
            str(cell.get("timeframe", "") or "").strip().upper(),
            str(cell.get("regime", "") or "").strip().upper(),
        )

    @staticmethod
    def _timestamp(value: Any) -> Optional[datetime]:
        if value in (None, ""):
            return None
        try:
            parsed = datetime.fromisoformat(str(value).strip().replace("Z", "+00:00"))
        except (TypeError, ValueError, OverflowError):
            return None
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)

    @classmethod
    def _interval(
        cls,
        lineage: Mapping[str, Any],
    ) -> Optional[Tuple[datetime, datetime]]:
        start = cls._timestamp(lineage.get("window_start"))
        end = cls._timestamp(lineage.get("window_end"))
        if start is None or end is None or start >= end:
            return None
        return start, end

    @staticmethod
    def _interval_payload(interval: Tuple[datetime, datetime]) -> Dict[str, str]:
        return {
            "window_start": interval[0].isoformat(),
            "window_end": interval[1].isoformat(),
        }

    def _load(self) -> Dict[str, Any]:
        if not self.state_path.exists():
            return {"schema_version": LOCKBOX_REGISTRY_VERSION, "accesses": {}}
        try:
            payload = json.loads(self.state_path.read_text(encoding="utf-8"))
        except Exception as exc:
            raise RuntimeError(f"lockbox registry corrupt: {exc}") from exc
        if payload.get("schema_version") != LOCKBOX_REGISTRY_VERSION or not isinstance(payload.get("accesses"), dict):
            raise RuntimeError("lockbox registry schema invalid")
        return payload

    def _atomic_write(self, payload: Dict[str, Any]) -> None:
        self.state_path.parent.mkdir(parents=True, exist_ok=True)
        temporary = self.state_path.with_name(f".{self.state_path.name}.tmp.{os.getpid()}")
        with temporary.open("w", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=2, sort_keys=True, default=str)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, self.state_path)

    def consume(self, *, cell: Mapping[str, Any], lineage: Mapping[str, Any]) -> LockboxDecision:
        access_payload = {
            "cell": dict(cell),
            "lineage": dict(lineage),
            "contract_version": LOCKBOX_REGISTRY_VERSION,
        }
        access_id = self._fingerprint(access_payload)
        requested_interval = self._interval(lineage)
        lock_path = self.state_path.with_suffix(self.state_path.suffix + ".lock")
        lock_path.parent.mkdir(parents=True, exist_ok=True)
        descriptor = os.open(str(lock_path), os.O_RDWR | os.O_CREAT, 0o600)
        try:
            if os.path.getsize(lock_path) == 0:
                os.write(descriptor, b"0")
                os.fsync(descriptor)
            os.lseek(descriptor, 0, os.SEEK_SET)
            if os.name == "nt":
                import msvcrt

                msvcrt.locking(descriptor, msvcrt.LK_LOCK, 1)
            else:
                import fcntl

                fcntl.flock(descriptor, fcntl.LOCK_EX)
            try:
                payload = self._load()
                accesses = payload["accesses"]
                if requested_interval is None:
                    return LockboxDecision(
                        False,
                        access_id,
                        "lockbox interval is missing, invalid, or non-positive",
                        metadata={
                            "conflict_type": "INVALID_INTERVAL",
                            "requested_interval": {
                                "window_start": lineage.get("window_start"),
                                "window_end": lineage.get("window_end"),
                            },
                        },
                    )
                if access_id in accesses:
                    return LockboxDecision(
                        False,
                        access_id,
                        "lockbox lineage/cell already consumed",
                        dict(accesses[access_id]),
                        {
                            "conflict_type": "EXACT_DUPLICATE",
                            "requested_interval": self._interval_payload(requested_interval),
                            "prior_interval": self._interval_payload(requested_interval),
                        },
                    )
                requested_cell = self._experiment_cell(cell)
                for prior in accesses.values():
                    prior_cell = self._experiment_cell(prior.get("cell", {}))
                    global_scope = (
                        requested_cell == self._GLOBAL_EXPERIMENT_CELL
                        or prior_cell == self._GLOBAL_EXPERIMENT_CELL
                    )
                    if prior_cell != requested_cell and not global_scope:
                        continue
                    prior_interval = self._interval(prior.get("lineage", {}))
                    if prior_interval is None:
                        return LockboxDecision(
                            False,
                            access_id,
                            "prior lockbox interval for this experiment is invalid; fail closed",
                            dict(prior),
                            {
                                "conflict_type": "INVALID_PRIOR_INTERVAL",
                                "requested_interval": self._interval_payload(requested_interval),
                            },
                        )
                    overlaps = (
                        max(requested_interval[0], prior_interval[0])
                        < min(requested_interval[1], prior_interval[1])
                    )
                    if overlaps:
                        return LockboxDecision(
                            False,
                            access_id,
                            (
                                "lockbox interval overlaps a previously consumed "
                                "experiment in the same evidence scope"
                            ),
                            dict(prior),
                            {
                                "conflict_type": (
                                    "GLOBAL_INTERVAL_OVERLAP"
                                    if global_scope
                                    else "INTERVAL_OVERLAP"
                                ),
                                "requested_interval": self._interval_payload(requested_interval),
                                "prior_interval": self._interval_payload(prior_interval),
                            },
                        )
                record = {**access_payload, "access_id": access_id}
                accesses[access_id] = record
                self._atomic_write(payload)
                self.audit_path.parent.mkdir(parents=True, exist_ok=True)
                with self.audit_path.open("a", encoding="utf-8") as handle:
                    handle.write(
                        json.dumps(record, sort_keys=True, separators=(",", ":"), default=str)
                        + "\n"
                    )
                    handle.flush()
                    os.fsync(handle.fileno())
                return LockboxDecision(True, access_id, "first and only access recorded")
            finally:
                os.lseek(descriptor, 0, os.SEEK_SET)
                if os.name == "nt":
                    msvcrt.locking(descriptor, msvcrt.LK_UNLCK, 1)
                else:
                    fcntl.flock(descriptor, fcntl.LOCK_UN)
        finally:
            os.close(descriptor)
