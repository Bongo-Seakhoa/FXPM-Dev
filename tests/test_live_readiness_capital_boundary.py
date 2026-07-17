import math

import pytest

import pm_cap_calibration
from pm_live_readiness import (
    _capital_numeric_authority_available,
    _capital_stop_risk_config_issues,
    _quarter_percent_units,
)


@pytest.mark.parametrize("total", [0.25, 3.0, 8.25, 33.25, 50.0, 75.0, 90.0])
def test_structural_gate_does_not_impose_an_arbitrary_low_total_cap(total):
    assert _capital_stop_risk_config_issues(total, min(2.0, total)) == []


@pytest.mark.parametrize(
    "total", [0.0, -0.25, 33.33, 100.0, float("inf"), True, "90.0"]
)
def test_total_cap_must_remain_finite_solvent_and_quarter_percent_aligned(total):
    codes = {
        finding.code for finding in _capital_stop_risk_config_issues(total, 0.25)
    }
    assert "total_stop_risk_cap_contract_mismatch" in codes


@pytest.mark.parametrize(
    "factor", [0.0, -0.25, 1.1, 50.25, float("inf"), True, "2.0"]
)
def test_factor_cap_must_be_positive_aligned_and_not_exceed_total(factor):
    codes = {
        finding.code for finding in _capital_stop_risk_config_issues(50.0, factor)
    }
    assert "factor_group_stop_risk_cap_contract_mismatch" in codes


def test_signed_v8_capital_adapter_is_available_but_not_self_authorizing():
    assert _capital_numeric_authority_available() is True


@pytest.mark.parametrize(
    "value",
    [
        1e-13,
        math.nextafter(0.0, 1.0),
        math.nextafter(0.25, 0.0),
        math.nextafter(0.25, 1.0),
        math.nextafter(100.0, 0.0),
        10**1000,
    ],
)
def test_exact_quarter_unit_boundary_rejects_nearby_and_oversized_values(value):
    assert _quarter_percent_units(value) is None
    assert _capital_stop_risk_config_issues(value, 0.25)


@pytest.mark.parametrize("value, units", [(0.25, 1), (33.25, 133), (99.75, 399)])
def test_exact_quarter_unit_boundary_accepts_only_lattice_points(value, units):
    assert _quarter_percent_units(value) == units


@pytest.mark.parametrize(
    "forged",
    [
        {"numeric_cap_authority": True},
        {
            "schema_version": "wrong",
            "implementation_status": "UNAVAILABLE",
            "authority_scope": "BLOCKED_NO_NUMERIC_AUTHORITY",
            "numeric_cap_authority": True,
        },
    ],
)
def test_forged_adapter_flags_cannot_enable_blocked_v7(monkeypatch, forged):
    monkeypatch.setattr(
        pm_cap_calibration,
        "current_book_authority_adapter_contract",
        lambda: forged,
    )
    assert _capital_numeric_authority_available() is False
