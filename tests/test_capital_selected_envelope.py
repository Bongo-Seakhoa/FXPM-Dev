"""Attack tests for the Capital V8 selected-envelope deadlock fix (SEAM 1-3).

Before the fix, the verifier and both readiness paths required the selected
seven-axis envelope to equal the legacy static config tuple (8/2/1/200), so any
genuinely replay-selected point (e.g. 3/2/1, or the higher 33.25/50/75/90
totals) was force-rejected.  These tests prove a non-legacy selection now
authorizes on internal coherence alone, and that mutated/incoherent envelopes
are still rejected - static config is no longer a post-selection veto.
"""

from __future__ import annotations

import pm_demo_readiness as demo_readiness
from pm_optimization_authority import _capital_config_errors


def _coherent_envelope(
    *,
    total: float,
    factor: float,
    same_symbol: float,
    margin_util: float,
) -> dict[str, float]:
    """Build a coherent seven-axis envelope from the four free axes."""

    return {
        "max_total_stop_risk_pct": total,
        "max_factor_stop_risk_pct": factor,
        "max_same_symbol_stop_risk_pct": same_symbol,
        "max_margin_utilization_pct": margin_util,
        "min_margin_level_pct": 10_000.0 / margin_util,
        "min_free_margin_pct": 100.0 - margin_util,
        "gap_stress_multiple": 2.5,
    }


# The exact first candidate from config.production_capital_authority.
_DEMO_CANDIDATE = {
    "max_total_stop_risk_pct": 3.0,
    "max_factor_stop_risk_pct": 2.0,
    "max_same_symbol_stop_risk_pct": 1.0,
    "max_margin_utilization_pct": 50.0,
    "min_margin_level_pct": 200.0,
    "min_free_margin_pct": 50.0,
    "gap_stress_multiple": 2.5,
}


def test_non_legacy_demo_candidate_authorizes_in_verifier() -> None:
    # 3/2/1 is not the legacy 8/2/1 tuple; it must authorize on coherence.
    assert _capital_config_errors(dict(_DEMO_CANDIDATE)) == []


def test_high_total_candidate_authorizes_in_verifier() -> None:
    # A deliberately high precommitted point (90% total) must not be vetoed by
    # the static 8% ceiling - only its own internal coherence matters.
    high = _coherent_envelope(
        total=90.0, factor=60.0, same_symbol=30.0, margin_util=90.0
    )
    assert _capital_config_errors(high) == []


def test_non_legacy_candidate_authorizes_in_demo_readiness() -> None:
    # Readiness must likewise accept the selected point (config is ignored).
    assert demo_readiness.demo_capital_envelope_errors(dict(_DEMO_CANDIDATE), None) == []


def test_incoherent_free_margin_is_rejected() -> None:
    bad = dict(_DEMO_CANDIDATE)
    bad["min_free_margin_pct"] = 40.0  # should be 100 - 50 = 50
    errors = _capital_config_errors(bad)
    assert "CAPITAL_SELECTED_ENVELOPE_FREE_MARGIN_MISMATCH" in errors


def test_incoherent_margin_level_is_rejected() -> None:
    bad = dict(_DEMO_CANDIDATE)
    bad["min_margin_level_pct"] = 150.0  # should be 10000 / 50 = 200
    errors = _capital_config_errors(bad)
    assert "CAPITAL_SELECTED_ENVELOPE_MARGIN_LEVEL_MISMATCH" in errors


def test_missing_axis_is_schema_rejected() -> None:
    bad = dict(_DEMO_CANDIDATE)
    del bad["gap_stress_multiple"]
    assert _capital_config_errors(bad) == ["CAPITAL_SELECTED_ENVELOPE_SCHEMA_INVALID"]


def test_extra_axis_is_schema_rejected() -> None:
    bad = dict(_DEMO_CANDIDATE)
    bad["unexpected_axis"] = 1.0
    assert _capital_config_errors(bad) == ["CAPITAL_SELECTED_ENVELOPE_SCHEMA_INVALID"]


def test_nonpositive_axis_is_rejected() -> None:
    bad = dict(_DEMO_CANDIDATE)
    bad["max_total_stop_risk_pct"] = 0.0
    errors = _capital_config_errors(bad)
    assert any(e.startswith("CAPITAL_SELECTED_ENVELOPE_AXIS_INVALID") for e in errors)


def test_boolean_axis_is_rejected() -> None:
    # bool is a subclass of int; it must not slip through as a numeric axis.
    bad = dict(_DEMO_CANDIDATE)
    bad["gap_stress_multiple"] = True
    errors = _capital_config_errors(bad)
    assert any(e.startswith("CAPITAL_SELECTED_ENVELOPE_AXIS_INVALID") for e in errors)


def test_sub_unit_gap_stress_is_rejected() -> None:
    bad = _coherent_envelope(
        total=3.0, factor=2.0, same_symbol=1.0, margin_util=50.0
    )
    bad["gap_stress_multiple"] = 0.5
    errors = _capital_config_errors(bad)
    assert "CAPITAL_SELECTED_ENVELOPE_GAP_STRESS_INVALID" in errors
