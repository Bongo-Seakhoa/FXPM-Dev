from __future__ import annotations

import math

from pm_risk_admission import (
    GrossRiskLimits,
    StopRiskExposure,
    conservative_risk_capital,
    evaluate_gross_stop_risk,
    evaluate_projected_margin,
    infer_factor_groups,
)


def limits(total: float = 8.0, factor: float = 2.0) -> GrossRiskLimits:
    return GrossRiskLimits(total, factor)


def test_factor_groups_are_deterministic_across_asset_classes():
    assert infer_factor_groups("EURUSD", "fx") == (
        "CURRENCY:EUR",
        "CURRENCY:USD",
    )
    assert infer_factor_groups("XAUUSD", "metal") == (
        "CURRENCY:USD",
        "PRECIOUS_METALS",
    )
    assert infer_factor_groups("BTCUSD", "crypto") == (
        "CRYPTO_BETA",
        "CURRENCY:USD",
    )
    assert infer_factor_groups("US500", "index") == (
        "GLOBAL_EQUITY_BETA",
        "US_EQUITY_BETA",
    )
    assert infer_factor_groups("APPLE", "equity") == (
        "GLOBAL_EQUITY_BETA",
        "SINGLE_STOCK_BETA",
    )
    assert infer_factor_groups("BROKEREXOTIC", "unknown") == (
        "UNCLASSIFIED_MARKET_FACTOR",
    )


def test_distinct_fx_factors_can_aggregate_until_total_cap():
    decision = evaluate_gross_stop_risk(
        [
            StopRiskExposure("EURUSD", 0.25, "fx"),
            StopRiskExposure("AUDCAD", 0.25, "fx"),
        ],
        StopRiskExposure("GBPJPY", 0.25, "fx"),
        limits(total=0.75, factor=0.50),
    )
    assert decision.allow
    assert decision.prospective_total_stop_risk_pct == 0.75


def test_gross_currency_factor_does_not_net_opposing_trades():
    # Direction is deliberately absent from StopRiskExposure: long EURUSD and
    # short GBPUSD both consume gross USD shock capacity.
    decision = evaluate_gross_stop_risk(
        [StopRiskExposure("EURUSD", 1.0, "fx")],
        StopRiskExposure("GBPUSD", 1.01, "fx"),
        limits(total=8.0, factor=2.0),
    )
    assert not decision.allow
    assert decision.code == "FACTOR_GROUP_STOP_RISK_CAP"
    assert decision.prospective_candidate_group_stop_risk_pct["CURRENCY:USD"] == 2.01


def test_total_stop_risk_cap_remains_authoritative():
    decision = evaluate_gross_stop_risk(
        [StopRiskExposure("EURUSD", 1.5, "fx")],
        StopRiskExposure("AUDCAD", 0.51, "fx"),
        limits(total=2.0, factor=2.0),
    )
    assert not decision.allow
    assert decision.code == "TOTAL_STOP_RISK_CAP"


def test_existing_policy_breach_blocks_new_risk_until_reduced():
    decision = evaluate_gross_stop_risk(
        [
            StopRiskExposure("EURUSD", 1.1, "fx"),
            StopRiskExposure("GBPUSD", 1.0, "fx"),
        ],
        StopRiskExposure("AUDCAD", 0.25, "fx"),
        limits(total=8.0, factor=2.0),
    )
    assert not decision.allow
    assert decision.code == "EXISTING_FACTOR_GROUP_STOP_RISK_CAP"


def test_missing_open_symbol_or_risk_fails_closed():
    missing_symbol = evaluate_gross_stop_risk(
        [StopRiskExposure("", 0.25, "fx", "ticket-1")],
        StopRiskExposure("EURUSD", 0.25, "fx"),
        limits(),
    )
    assert not missing_symbol.allow
    assert missing_symbol.code == "RISK_INPUT_INVALID"

    missing_risk = evaluate_gross_stop_risk(
        [StopRiskExposure("EURUSD", math.nan, "fx", "ticket-2")],
        StopRiskExposure("GBPUSD", 0.25, "fx"),
        limits(),
    )
    assert not missing_risk.allow
    assert missing_risk.code == "RISK_INPUT_INVALID"


def test_invalid_or_inverted_policy_fails_closed():
    decision = evaluate_gross_stop_risk(
        [],
        StopRiskExposure("EURUSD", 0.25, "fx"),
        limits(total=1.0, factor=2.0),
    )
    assert not decision.allow
    assert decision.code == "RISK_POLICY_INVALID"


def test_conservative_capital_blocks_risk_that_fresh_equity_math_would_admit():
    snapshot = conservative_risk_capital(
        balance=10_000.0,
        equity=10_000.0,
        open_stop_risk_amount=700.0,
    )
    assert snapshot.allow
    assert snapshot.risk_capital == 9_300.0

    fresh_equity_decision = evaluate_gross_stop_risk(
        [StopRiskExposure("EURUSD", 7.0, "fx")],
        StopRiskExposure("AUDCAD", 1.0, "fx"),
        limits(total=8.0, factor=8.0),
    )
    assert fresh_equity_decision.allow

    conservative_decision = evaluate_gross_stop_risk(
        [StopRiskExposure("EURUSD", 700.0 / snapshot.risk_capital * 100.0, "fx")],
        StopRiskExposure("AUDCAD", 100.0 / snapshot.risk_capital * 100.0, "fx"),
        limits(total=8.0, factor=8.0),
    )
    assert not conservative_decision.allow
    assert conservative_decision.code == "TOTAL_STOP_RISK_CAP"


def test_conservative_capital_missing_balance_or_exhausted_stop_equity_fails_closed():
    missing = conservative_risk_capital(
        balance=None,
        equity=10_000.0,
        open_stop_risk_amount=0.0,
    )
    assert not missing.allow
    assert missing.code == "RISK_CAPITAL_INPUT_INVALID"

    exhausted = conservative_risk_capital(
        balance=1_000.0,
        equity=1_100.0,
        open_stop_risk_amount=1_000.0,
    )
    assert not exhausted.allow
    assert exhausted.code == "NO_POSITIVE_CONSERVATIVE_RISK_CAPITAL"


def test_projected_margin_is_separate_and_capped_at_utilization_boundary():
    exact = evaluate_projected_margin(
        equity=10_000.0,
        current_used_margin=4_000.0,
        candidate_required_margin=1_000.0,
        max_margin_utilization_pct=50.0,
    )
    assert exact.allow
    assert exact.projected_margin_utilization_pct == 50.0
    assert exact.projected_margin_level_pct == 200.0
    assert exact.projected_free_margin_pct == 50.0

    breach = evaluate_projected_margin(
        equity=10_000.0,
        current_used_margin=4_000.0,
        candidate_required_margin=1_000.01,
        max_margin_utilization_pct=50.0,
    )
    assert not breach.allow
    assert breach.code == "PROJECTED_MARGIN_UTILIZATION_CAP"


def test_projected_margin_missing_candidate_requirement_fails_closed():
    decision = evaluate_projected_margin(
        equity=10_000.0,
        current_used_margin=0.0,
        candidate_required_margin=None,
        max_margin_utilization_pct=50.0,
    )
    assert not decision.allow
    assert decision.code == "MARGIN_INPUT_INVALID"
