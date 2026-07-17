from __future__ import annotations

import copy
from dataclasses import replace
from datetime import datetime, timedelta, timezone

import pm_current_refit_search_producer as current_producer
from pm_portfolio_replay import (
    PORTFOLIO_REPLAY_CANDIDATE_SCHEMA_VERSION,
    PORTFOLIO_REPLAY_SCHEMA_VERSION,
    MarkPoint,
    build_current_refit_replay_successor,
    portfolio_mark_cost_contract,
    seal_portfolio_replay_evidence,
    validate_funded_portfolio_replay_evidence,
)
from tests.test_synchronized_portfolio_replay import (
    _strict_current_refit_roster,
    candidate,
    policy,
)


def _case() -> tuple[dict, dict, dict, dict, dict]:
    roster = _strict_current_refit_roster()
    start = datetime(2024, 1, 1, tzinfo=timezone.utc)
    marks = tuple(
        MarkPoint(start + timedelta(days=day), day * 0.1) for day in range(1, 502)
    )
    selected = replace(
        candidate(
            "selected",
            status="PROCESS_VALIDATED_CURRENT_REFIT",
            entry=start.isoformat(),
            exit=(start + timedelta(days=502)).isoformat(),
            marks=marks,
        ),
        cell_id="EURUSD|H1|TREND",
        source_schema=PORTFOLIO_REPLAY_CANDIDATE_SCHEMA_VERSION,
        margin_model="offline_notional_asset_rate_buffered_v1",
    )
    expected_policy = policy(
        allowed_promotion_statuses=("PROCESS_VALIDATED_CURRENT_REFIT",)
    ).__dict__
    policies = {10_000.0: expected_policy}
    lineage = {
        "native_activation_id": "activation",
        "native_activation_manifest_sha256": "a" * 64,
        "native_activation_roster_sha256": "b" * 64,
        "native_bars_sha256_by_source": {
            "EURUSD|H1": "c" * 64,
            "EURUSD|M5": "d" * 64,
        },
        "native_sidecar_sha256_by_source": {
            "EURUSD|H1": "e" * 64,
            "EURUSD|M5": "f" * 64,
        },
    }
    costs = {
        "use_spread": True,
        "backtest_spread_mode": "conservative_max",
        "use_commission": True,
        "use_slippage": True,
        "slippage_pips": 0.5,
        "swap_model_enabled": True,
        "swap_model_min_holding_hours": 20.0,
        "swap_model_triple_rollover_day": "wednesday",
    }
    successor, scenarios = build_current_refit_replay_successor(
        [selected],
        roster,
        replayed_cell_ids=["EURUSD|H1|TREND"],
        expected_lineage=lineage,
        expected_cost_contract=costs,
        expected_policies=policies,
    )
    native_common = {
        "source_kind": "broker_native",
        "provenance_valid": True,
        "timestamp_semantics": "bar_open_utc",
        "activation_id": "activation",
        "activation_manifest_sha256": "a" * 64,
    }
    evidence = seal_portfolio_replay_evidence(
        {
            "verdict": "PASS",
            "failures": [],
            "lineage": lineage,
            "roster": roster,
            "replay": {
                "candidate_schema_version": PORTFOLIO_REPLAY_CANDIDATE_SCHEMA_VERSION,
                "portfolio_schema_version": PORTFOLIO_REPLAY_SCHEMA_VERSION,
                "window": {
                    "mode": "common_intersection",
                    "partition": "current_refit_management_replay",
                    "evaluation_start": start.isoformat(),
                    "evaluation_end": (start + timedelta(days=502)).isoformat(),
                },
                "partial_run": False,
                "allow_cell_errors": False,
                "cell_generation_errors": [],
                "prelockbox_source_status": "PROCESS_VALIDATED_CURRENT_REFIT",
                "evidence_only_status_allowlist": ["PROCESS_VALIDATED_CURRENT_REFIT"],
                "current_refit_execution_access_id": "4" * 64,
                "replayed_cell_ids": ["EURUSD|H1|TREND"],
                "current_refit_roster_sha256": roster["roster_sha256"],
                "candidate_cell_ids": ["EURUSD|H1|TREND"],
                "cost_contract": costs,
                "mark_cost_contract": portfolio_mark_cost_contract(costs),
                "native_sources": {
                    "EURUSD|H1|TREND": {
                        **native_common,
                        "source_sha256": "c" * 64,
                        "sidecar_sha256": "e" * 64,
                    }
                },
                "valuation_sources": {
                    "EURUSD": {
                        **native_common,
                        "symbol": "EURUSD",
                        "timeframe": "M5",
                        "source_sha256": "d" * 64,
                        "sidecar_sha256": "f" * 64,
                    }
                },
                "scenarios": scenarios,
                "current_refit_successor": successor,
            },
        }
    )
    return evidence, lineage, roster, costs, policies


def _validate(case: tuple[dict, dict, dict, dict, dict]) -> list[str]:
    evidence, lineage, roster, costs, policies = case
    return validate_funded_portfolio_replay_evidence(
        evidence,
        expected_lineage=lineage,
        expected_roster=roster,
        expected_cost_contract=costs,
        expected_policies=policies,
        minimum_calendar_days=500,
        minimum_accepted_positions=1,
        minimum_profit_factor=1.0,
        minimum_return_pct=0.0,
        maximum_drawdown_pct=20.0,
        require_mtm=True,
        maximum_bootstrap_ruin_upper_95pct=1.0,
        maximum_bootstrap_p95_drawdown_pct=20.0,
        minimum_bootstrap_daily_return_observations=500,
    )


def test_authoritative_current_refit_successor_passes_raw_recomputation() -> None:
    case = _case()
    assert _validate(case) == []


def test_real_producer_projection_passes_the_standalone_replay_verifier() -> None:
    evidence, lineage, roster, costs, policies = _case()
    changed = copy.deepcopy(evidence)
    original_window = changed["replay"]["window"]
    start = datetime.fromisoformat(original_window["evaluation_start"])
    end = datetime.fromisoformat(original_window["evaluation_end"])
    _, management_window = current_producer._current_replay_windows(  # noqa: SLF001
        {
            "start_date": start.date().isoformat(),
            "end_date": end.date().isoformat(),
        },
        current_cutoff=end.isoformat(),
    )

    def identity(timeframe: str, bars: str, sidecar: str, suffix: str) -> dict:
        return {
            "symbol": "EURUSD",
            "timeframe": timeframe,
            "activation_id": lineage["native_activation_id"],
            "activation_manifest_sha256": lineage["native_activation_manifest_sha256"],
            "source_bars_sha256": bars,
            "source_audit_sidecar_sha256": sidecar,
            "index_sha256": suffix * 64,
            "source_identity_sha256": suffix.lower() * 64,
        }

    changed["replay"]["window"] = management_window
    changed["replay"]["native_sources"] = {
        "EURUSD|H1|TREND": current_producer._management_native_source_contract(  # noqa: SLF001
            identity("H1", "c" * 64, "e" * 64, "1")
        )
    }
    changed["replay"]["valuation_sources"] = {
        "EURUSD": current_producer._management_native_source_contract(  # noqa: SLF001
            identity("M5", "d" * 64, "f" * 64, "2")
        )
    }
    changed = seal_portfolio_replay_evidence(changed)
    assert _validate((changed, lineage, roster, costs, policies)) == []


def test_successor_tampering_fails_reconciliation() -> None:
    evidence, lineage, roster, costs, policies = _case()
    changed = copy.deepcopy(evidence)
    changed["replay"]["current_refit_successor"]["scenario_reconciliations"][0][
        "fill_ledger_sha256"
    ] = "0" * 64
    changed = seal_portfolio_replay_evidence(changed)
    errors = _validate((changed, lineage, roster, costs, policies))
    assert "CURRENT_REFIT_SUCCESSOR_SEAL_INVALID" in errors
    assert "CURRENT_REFIT_SUCCESSOR_RECONCILIATION_MISMATCH:10000.00" in errors
