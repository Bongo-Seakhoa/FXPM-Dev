import numpy as np
import pandas as pd

from pm_sample_dependence import effective_sample_evidence, overlap_component_count


def _ar1(count: int, phi: float, seed: int = 7) -> np.ndarray:
    rng = np.random.default_rng(seed)
    noise = rng.normal(size=count)
    values = np.zeros(count, dtype=np.float64)
    for index in range(1, count):
        values[index] = phi * values[index - 1] + noise[index]
    return values


def test_positive_serial_correlation_reduces_effective_sample():
    independent = effective_sample_evidence(_ar1(2_000, 0.0))
    persistent = effective_sample_evidence(_ar1(2_000, 0.85))

    assert independent.passed
    assert persistent.passed
    assert 1 <= persistent.effective_observations < independent.effective_observations
    assert persistent.integrated_autocorrelation_time > 1.0


def test_overlap_components_cap_trade_information():
    values = _ar1(12, 0.0)
    start = pd.Timestamp("2026-01-01", tz="UTC")
    intervals = []
    for index in range(12):
        # Four groups of three trades share a holding/risk interval.
        group = index // 3
        group_start = start + pd.Timedelta(days=group * 10)
        intervals.append((group_start + pd.Timedelta(hours=index % 3), group_start + pd.Timedelta(days=2)))

    evidence = effective_sample_evidence(
        values,
        holding_intervals=intervals,
        minimum_observations=8,
    )

    assert evidence.passed
    assert evidence.overlap_cluster_count == 4
    assert evidence.effective_observations <= 4.0


def test_overlap_component_count_keeps_disjoint_intervals_independent():
    start = pd.Timestamp("2026-01-01", tz="UTC")
    intervals = [
        (start + pd.Timedelta(days=index * 2), start + pd.Timedelta(days=index * 2 + 1))
        for index in range(10)
    ]

    assert overlap_component_count(intervals) == 10


def test_effective_count_never_exceeds_raw_count():
    evidence = effective_sample_evidence(_ar1(500, -0.75))

    assert evidence.passed
    assert 1.0 <= evidence.effective_observations <= evidence.raw_observations
    assert evidence.effective_observations_acf <= evidence.raw_observations
    assert evidence.effective_observations_hac <= evidence.raw_observations


def test_invalid_or_constant_surfaces_fail_closed():
    corrupt = effective_sample_evidence([0.1, 0.2, float("nan"), 0.3])
    constant = effective_sample_evidence([0.0] * 20)
    short = effective_sample_evidence([0.1, -0.1, 0.2])

    assert not corrupt.passed
    assert corrupt.reason_codes == ("INVALID_OBSERVATION_VECTOR",)
    assert not constant.passed
    assert constant.reason_codes == ("VARIANCE_UNAVAILABLE",)
    assert not short.passed
    assert short.reason_codes == ("INSUFFICIENT_RAW_OBSERVATIONS",)


def test_holding_interval_alignment_and_geometry_fail_closed():
    values = _ar1(10, 0.0)
    mismatch = effective_sample_evidence(values, holding_intervals=[("2026-01-01", "2026-01-02")])
    backwards = [("2026-01-02", "2026-01-01")] * 10
    invalid = effective_sample_evidence(values, holding_intervals=backwards)

    assert mismatch.reason_codes == ("HOLDING_INTERVAL_COUNT_MISMATCH",)
    assert invalid.reason_codes == ("HOLDING_INTERVALS_INVALID",)
