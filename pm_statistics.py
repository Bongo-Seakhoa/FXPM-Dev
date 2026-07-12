"""Institutional promotion statistics and one-shot lockbox lineage.

All Sharpe-family tests use a dated daily realized-return series. Trade count
is never substituted for the number of return observations. The functions are
deterministic and fail closed when alignment, depth, or provenance is absent.
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


STATISTICS_CONTRACT_VERSION = "2026-07-10-institutional-promotion-statistics-v1"
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
    """Autocorrelation-adjusted observation count using the initial positive sequence."""
    array = _finite_returns(values)
    count = int(array.size)
    if count < 3:
        return float(count)
    centered = array - float(array.mean())
    variance = float(np.dot(centered, centered) / count)
    if variance <= 1e-18:
        return 0.0
    lag_cap = min(count - 1, int(max_lag or max(1, round(math.sqrt(count)))))
    positive_sum = 0.0
    for lag in range(1, lag_cap + 1):
        covariance = float(np.dot(centered[:-lag], centered[lag:]) / (count - lag))
        rho = covariance / variance
        if not math.isfinite(rho) or rho <= 0.0:
            break
        positive_sum += rho
    return float(max(1.0, min(count, count / (1.0 + 2.0 * positive_sum))))


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
    min_observations: int = 60,
    min_effective_observations: float = 30.0,
    min_dsr: float = 0.95,
) -> Dict[str, Any]:
    """Compute DSR from daily returns, non-normal moments, and actual multiplicity.

    The expected maximum null Sharpe uses the Bailey/Lopez de Prado extreme-
    value approximation with the null standard error of a daily Sharpe. This
    keeps the benchmark and observed Sharpe on the same (daily) clock.
    """
    moments = return_moments(values)
    count = int(moments["observations"])
    effective = float(moments["effective_observations"])
    trials = max(1, int(attempted_trials or 0))
    base = {
        "contract_version": STATISTICS_CONTRACT_VERSION,
        "return_frequency": "daily_realized",
        "attempted_trials": trials,
        **moments,
        "minimum_observations": int(min_observations),
        "minimum_effective_observations": float(min_effective_observations),
        "minimum_dsr": float(min_dsr),
    }
    if count < int(min_observations):
        return {**base, "status": "UNAVAILABLE", "passed": False, "reason": "insufficient daily observations", "deflated_sharpe": 0.0}
    if effective < float(min_effective_observations):
        return {**base, "status": "UNAVAILABLE", "passed": False, "reason": "insufficient effective observations", "deflated_sharpe": 0.0}
    sharpe = float(moments["sharpe_daily"])
    skew = float(moments["skew"])
    kurtosis = float(moments["kurtosis"])
    variance_term = 1.0 - skew * sharpe + ((kurtosis - 1.0) / 4.0) * sharpe * sharpe
    if variance_term <= 0.0 or not math.isfinite(variance_term):
        return {**base, "status": "UNAVAILABLE", "passed": False, "reason": "invalid Sharpe variance", "deflated_sharpe": 0.0}
    null_sigma = 1.0 / math.sqrt(max(effective - 1.0, 1.0))
    if trials > 1:
        upper_one = min(1.0 - 1e-12, 1.0 - (1.0 / trials))
        upper_two = min(1.0 - 1e-12, 1.0 - (1.0 / (trials * math.e)))
        expected_max = null_sigma * (
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
    passed = bool(dsr >= float(min_dsr) and sharpe > 0.0)
    return {
        **base,
        "status": "PASS" if passed else "FAIL",
        "passed": passed,
        "reason": "DSR clears threshold" if passed else "DSR below threshold or non-positive Sharpe",
        "expected_max_null_sharpe_daily": expected_max,
        "sharpe_standard_error": sharpe_se,
        "deflated_sharpe": dsr,
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
        raw_returns = list(metrics.get("daily_realized_returns", []) or [])
        dates = [str(value) for value in (metrics.get("daily_return_dates", []) or [])]
        if not raw_returns and not dates:
            continue
        returns = _finite_returns(raw_returns)
        if returns.size == 0:
            return {
                "contract_version": STATISTICS_CONTRACT_VERSION,
                "status": "UNAVAILABLE",
                "passed": False,
                "reason": f"candidate {index} has an invalid daily-return vector",
                "pbo": None,
            }
        if len(dates) != len(raw_returns) or any(not value.strip() for value in dates):
            return {
                "contract_version": STATISTICS_CONTRACT_VERSION,
                "status": "UNAVAILABLE",
                "passed": False,
                "reason": f"candidate {index} daily-return dates do not match its return vector",
                "pbo": None,
            }
        if len(set(dates)) != len(dates):
            return {
                "contract_version": STATISTICS_CONTRACT_VERSION,
                "status": "UNAVAILABLE",
                "passed": False,
                "reason": f"candidate {index} daily-return axis contains duplicate dates",
                "pbo": None,
            }
        evidence = dict(candidate.get("oos_evidence") or {})
        window = dict(evidence.get("window") or {})
        date_axis_sha256 = hashlib.sha256(
            json.dumps(dates, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
        ).hexdigest()
        key = (
            str(candidate.get("timeframe") or evidence.get("timeframe") or ""),
            str(window.get("start_ts") or dates[0]),
            str(window.get("end_ts") or dates[-1]),
            int(returns.size),
            date_axis_sha256,
        )
        identifier = str(
            evidence.get("candidate_id")
            or candidate.get("candidate_id")
            or f"candidate-{index}"
        )
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
        return {"contract_version": STATISTICS_CONTRACT_VERSION, "status": "UNAVAILABLE", "passed": False, "reason": "candidate daily-return axes are not identical", "pbo": None}
    matrix = np.column_stack(columns)
    if matrix.shape[0] < int(min_observations):
        return {"contract_version": STATISTICS_CONTRACT_VERSION, "status": "UNAVAILABLE", "passed": False, "reason": f"daily observations {matrix.shape[0]} < {int(min_observations)}", "pbo": None}
    result = cscv_pbo(matrix, candidate_ids=identifiers, blocks=blocks)
    value = result.get("pbo")
    passed = value is not None and float(value) <= float(max_pbo)
    result.update({
        "alignment": "exact_regime_identical_daily_axis",
        "alignment_key": list(alignment_keys[0]),
        "maximum_pbo": float(max_pbo),
        "passed": bool(passed),
        "status": "PASS" if passed else ("FAIL" if value is not None else "UNAVAILABLE"),
    })
    return result


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
        "return_frequency": "daily_realized",
        "observations": count,
        "effective_observations": effective,
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
