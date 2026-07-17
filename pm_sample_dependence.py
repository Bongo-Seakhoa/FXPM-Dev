"""Dependence-aware effective sample evidence for FxPM.

Raw bars, daily returns and trades are not interchangeable independent draws.
This module provides one deterministic, fail-closed contract for estimating
how much information remains after serial dependence and overlapping holding
intervals are considered.  It contains no strategy or profitability logic.
"""

from __future__ import annotations

import math
from dataclasses import asdict, dataclass, field
from typing import Any, Iterable, Mapping, Optional, Sequence, Tuple

import numpy as np
import pandas as pd


SAMPLE_DEPENDENCE_SCHEMA_VERSION = "fxpm-effective-sample-v2"


@dataclass(frozen=True)
class EffectiveSampleEvidence:
    status: str
    reason: str
    raw_observations: int
    effective_observations: float
    acf_method: str = "geyer_initial_monotone"
    acf_lags_used: int = 0
    integrated_autocorrelation_time: Optional[float] = None
    effective_observations_acf: Optional[float] = None
    hac_kernel: str = "bartlett"
    hac_bandwidth_method: str = "newey_west_automatic"
    hac_bandwidth: int = 0
    hac_long_run_variance: Optional[float] = None
    effective_observations_hac: Optional[float] = None
    overlap_adjustment: str = "holding_interval_components"
    overlap_cluster_count: Optional[int] = None
    effective_observations_overlap: Optional[float] = None
    reason_codes: Tuple[str, ...] = field(default_factory=tuple)
    schema_version: str = SAMPLE_DEPENDENCE_SCHEMA_VERSION

    @property
    def passed(self) -> bool:
        return self.status == "MEASURED"

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def _finite_vector(values: Iterable[Any]) -> Optional[np.ndarray]:
    try:
        vector = np.asarray(list(values), dtype=np.float64)
    except (TypeError, ValueError, OverflowError):
        return None
    if vector.ndim != 1 or vector.size == 0 or not np.isfinite(vector).all():
        return None
    return vector


def _autocovariances(centered: np.ndarray, max_lag: int) -> np.ndarray:
    count = int(centered.size)
    # The biased (1/n) autocovariance convention keeps the Bartlett HAC
    # estimator positive-semi-definite under its usual finite-sample form.
    return np.asarray(
        [
            float(np.dot(centered[: count - lag], centered[lag:]) / count)
            for lag in range(max_lag + 1)
        ],
        dtype=np.float64,
    )


def _automatic_hac_bandwidth(count: int) -> int:
    """Newey-West style automatic planning bandwidth for a sample mean."""
    if count < 2:
        return 0
    # A deterministic rate rule is preferable to a hidden fixed lag.  It grows
    # slowly enough to keep the estimator stable on the 252-1,000 daily-return
    # samples used by promotion gates.
    return int(max(1, min(count - 1, math.floor(4.0 * (count / 100.0) ** (2.0 / 9.0)))))


def _geyer_initial_monotone(
    autocovariances: np.ndarray,
) -> tuple[float, int]:
    """Return integrated autocorrelation time and the last retained lag.

    Adjacent positive autocorrelation pairs are retained until their sum is no
    longer positive.  Pair sums are forced non-increasing (Geyer's initial
    monotone sequence), preventing noisy high-lag estimates from inflating the
    claimed information.  Negative dependence never raises ESS above ``n``.
    """
    gamma0 = float(autocovariances[0])
    if gamma0 <= 0.0 or not math.isfinite(gamma0):
        return float("nan"), 0
    rhos = autocovariances / gamma0
    retained_sum = 0.0
    previous_pair = float("inf")
    last_lag = 0
    lag = 1
    while lag < len(rhos):
        pair = float(rhos[lag])
        pair_end = lag
        if lag + 1 < len(rhos):
            pair += float(rhos[lag + 1])
            pair_end = lag + 1
        if not math.isfinite(pair) or pair <= 0.0:
            break
        pair = min(pair, previous_pair)
        if pair <= 0.0:
            break
        retained_sum += pair
        previous_pair = pair
        last_lag = pair_end
        lag += 2
    tau = max(1.0, 1.0 + 2.0 * retained_sum)
    return float(tau), int(last_lag)


def _event_order(value: Any, *, field_name: str) -> int:
    if value is None:
        return 0
    if type(value) is not int or value < 0:
        raise ValueError(f"{field_name} must be a nonnegative integer")
    return value


def _coerce_interval(
    value: Any,
) -> tuple[tuple[pd.Timestamp, int], tuple[pd.Timestamp, int]]:
    if isinstance(value, Mapping):
        start = value.get("start", value.get("entry", value.get("open")))
        end = value.get("end", value.get("exit", value.get("close")))
        start_order = _event_order(
            value.get("start_event_order", value.get("entry_event_order")),
            field_name="start_event_order",
        )
        end_order = _event_order(
            value.get("end_event_order", value.get("exit_event_order")),
            field_name="end_event_order",
        )
    elif isinstance(value, Sequence) and not isinstance(value, (str, bytes)) and len(value) == 2:
        start, end = value
        start_order = 0
        end_order = 0
    else:
        raise ValueError("holding interval must be a mapping or a two-item sequence")
    start_ts = pd.Timestamp(start)
    end_ts = pd.Timestamp(end)
    if pd.isna(start_ts) or pd.isna(end_ts):
        raise ValueError("holding interval contains an invalid timestamp")
    if start_ts.tzinfo is None:
        start_ts = start_ts.tz_localize("UTC")
    else:
        start_ts = start_ts.tz_convert("UTC")
    if end_ts.tzinfo is None:
        end_ts = end_ts.tz_localize("UTC")
    else:
        end_ts = end_ts.tz_convert("UTC")
    start_event = (start_ts, start_order)
    end_event = (end_ts, end_order)
    if end_event < start_event:
        raise ValueError("holding interval ends before it starts")
    return start_event, end_event


def overlap_component_count(intervals: Sequence[Any]) -> int:
    """Count connected components of half-open holding intervals."""
    normalized = sorted((_coerce_interval(value) for value in intervals), key=lambda item: item[0])
    if not normalized:
        return 0
    clusters = 1
    current_end = normalized[0][1]
    for start, end in normalized[1:]:
        if start < current_end:
            current_end = max(current_end, end)
        else:
            clusters += 1
            current_end = end
    return clusters


def effective_sample_evidence(
    values: Iterable[Any],
    *,
    holding_intervals: Optional[Sequence[Any]] = None,
    max_acf_lag: Optional[int] = None,
    hac_bandwidth: Optional[int] = None,
    minimum_observations: int = 8,
) -> EffectiveSampleEvidence:
    """Measure conservative effective observations, or fail closed.

    ``values`` should be one ordered inference surface (for example daily net
    MTM returns or per-trade net R).  If supplied, ``holding_intervals`` must
    align one-for-one with the values; overlap components cap independent trade
    information without pretending simultaneous positions are separate bets.
    """
    vector = _finite_vector(values)
    if vector is None:
        return EffectiveSampleEvidence(
            status="UNAVAILABLE",
            reason="input is empty, non-finite or not one-dimensional",
            raw_observations=0,
            effective_observations=0.0,
            reason_codes=("INVALID_OBSERVATION_VECTOR",),
        )
    count = int(vector.size)
    minimum = max(3, int(minimum_observations or 0))
    if count < minimum:
        return EffectiveSampleEvidence(
            status="UNAVAILABLE",
            reason=f"raw observations {count} < {minimum}",
            raw_observations=count,
            effective_observations=0.0,
            reason_codes=("INSUFFICIENT_RAW_OBSERVATIONS",),
        )

    centered = vector - float(vector.mean())
    gamma0 = float(np.dot(centered, centered) / count)
    if not math.isfinite(gamma0) or gamma0 <= np.finfo(np.float64).eps:
        return EffectiveSampleEvidence(
            status="UNAVAILABLE",
            reason="sample variance is zero or invalid",
            raw_observations=count,
            effective_observations=0.0,
            reason_codes=("VARIANCE_UNAVAILABLE",),
        )

    if max_acf_lag is None:
        acf_lag = min(count - 1, max(2, int(math.floor(math.sqrt(count)))))
    else:
        try:
            acf_lag = int(max_acf_lag)
        except (TypeError, ValueError, OverflowError):
            acf_lag = 0
        if acf_lag <= 0:
            return EffectiveSampleEvidence(
                status="UNAVAILABLE",
                reason="ACF lag is invalid",
                raw_observations=count,
                effective_observations=0.0,
                reason_codes=("ACF_LAG_INVALID",),
            )
        acf_lag = min(count - 1, acf_lag)

    if hac_bandwidth is None:
        bandwidth = _automatic_hac_bandwidth(count)
    else:
        try:
            bandwidth = int(hac_bandwidth)
        except (TypeError, ValueError, OverflowError):
            bandwidth = 0
        if bandwidth <= 0:
            return EffectiveSampleEvidence(
                status="UNAVAILABLE",
                reason="HAC bandwidth is invalid",
                raw_observations=count,
                effective_observations=0.0,
                reason_codes=("HAC_BANDWIDTH_INVALID",),
            )
        bandwidth = min(count - 1, bandwidth)

    autocov = _autocovariances(centered, max(acf_lag, bandwidth))
    tau, lags_used = _geyer_initial_monotone(autocov[: acf_lag + 1])
    if not math.isfinite(tau) or tau < 1.0:
        return EffectiveSampleEvidence(
            status="UNAVAILABLE",
            reason="integrated autocorrelation time is unavailable",
            raw_observations=count,
            effective_observations=0.0,
            reason_codes=("ACF_ESTIMATE_UNAVAILABLE",),
        )
    n_eff_acf = float(max(1.0, min(count, count / tau)))

    long_run_variance = gamma0
    for lag in range(1, bandwidth + 1):
        weight = 1.0 - lag / float(bandwidth + 1)
        long_run_variance += 2.0 * weight * float(autocov[lag])
    if not math.isfinite(long_run_variance) or long_run_variance <= 0.0:
        return EffectiveSampleEvidence(
            status="UNAVAILABLE",
            reason="HAC long-run variance is not positive and finite",
            raw_observations=count,
            effective_observations=0.0,
            acf_lags_used=lags_used,
            integrated_autocorrelation_time=tau,
            effective_observations_acf=n_eff_acf,
            hac_bandwidth=bandwidth,
            reason_codes=("HAC_ESTIMATE_UNAVAILABLE",),
        )
    n_eff_hac = float(max(1.0, min(count, count * gamma0 / long_run_variance)))

    overlap_clusters: Optional[int] = None
    n_eff_overlap: Optional[float] = None
    if holding_intervals is not None:
        if len(holding_intervals) != count:
            return EffectiveSampleEvidence(
                status="UNAVAILABLE",
                reason="holding interval count does not match observations",
                raw_observations=count,
                effective_observations=0.0,
                acf_lags_used=lags_used,
                integrated_autocorrelation_time=tau,
                effective_observations_acf=n_eff_acf,
                hac_bandwidth=bandwidth,
                hac_long_run_variance=float(long_run_variance),
                effective_observations_hac=n_eff_hac,
                reason_codes=("HOLDING_INTERVAL_COUNT_MISMATCH",),
            )
        try:
            overlap_clusters = overlap_component_count(holding_intervals)
        except (TypeError, ValueError, OverflowError) as exc:
            return EffectiveSampleEvidence(
                status="UNAVAILABLE",
                reason=f"holding intervals are invalid: {exc}",
                raw_observations=count,
                effective_observations=0.0,
                acf_lags_used=lags_used,
                integrated_autocorrelation_time=tau,
                effective_observations_acf=n_eff_acf,
                hac_bandwidth=bandwidth,
                hac_long_run_variance=float(long_run_variance),
                effective_observations_hac=n_eff_hac,
                reason_codes=("HOLDING_INTERVALS_INVALID",),
            )
        if overlap_clusters <= 0:
            return EffectiveSampleEvidence(
                status="UNAVAILABLE",
                reason="holding overlap clusters are unavailable",
                raw_observations=count,
                effective_observations=0.0,
                reason_codes=("OVERLAP_ESTIMATE_UNAVAILABLE",),
            )
        n_eff_overlap = float(min(count, overlap_clusters))

    components = [float(count), n_eff_acf, n_eff_hac]
    if n_eff_overlap is not None:
        components.append(n_eff_overlap)
    effective = float(max(1.0, min(components)))
    return EffectiveSampleEvidence(
        status="MEASURED",
        reason="dependence-adjusted effective observations measured",
        raw_observations=count,
        effective_observations=effective,
        acf_lags_used=lags_used,
        integrated_autocorrelation_time=tau,
        effective_observations_acf=n_eff_acf,
        hac_bandwidth=bandwidth,
        hac_long_run_variance=float(long_run_variance),
        effective_observations_hac=n_eff_hac,
        overlap_cluster_count=overlap_clusters,
        effective_observations_overlap=n_eff_overlap,
    )


__all__ = [
    "EffectiveSampleEvidence",
    "SAMPLE_DEPENDENCE_SCHEMA_VERSION",
    "effective_sample_evidence",
    "overlap_component_count",
]
