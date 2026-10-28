from __future__ import annotations

import hashlib
import json
from dataclasses import asdict
from pathlib import Path

import pandas as pd
import pytest

import pm_portfolio_replay as replay
from agent_audit.performance.scripts.run_synchronized_portfolio_replay import (
    _native_replay_frame,
    _partition_pair,
)
from pm_core import PipelineConfig
from pm_live_readiness import validate_live_readiness
from pm_portfolio_replay import (
    PORTFOLIO_REPLAY_CANDIDATE_SCHEMA_VERSION,
    PORTFOLIO_REPLAY_REQUIRED_REGIMES,
    PORTFOLIO_REPLAY_REQUIRED_TIMEFRAMES,
    PORTFOLIO_REPLAY_SCHEMA_VERSION,
    PortfolioPolicy,
    active_native_portfolio_contract,
    build_book_possible_slot_accounting,
    build_current_refit_replay_roster,
    build_ledger_cell_roster,
    canonical_json_sha256,
    seal_portfolio_replay_evidence,
    portfolio_mark_cost_contract,
    validate_causal_process_authority,
    validate_current_refit_execution_authority,
    validate_funded_portfolio_replay_evidence,
    write_immutable_portfolio_replay_evidence,
)
from tests.native_v5_support import activate_v5_run, write_v5_run
from pm_two_phase_lockbox import (
    build_prelockbox_roster_artifact,
    consume_global_final_lockbox_once,
    critical_module_code_contract,
    materialize_joint_pass_ledger,
    validate_prelockbox_roster_artifact,
    validate_released_ledger_against_prelockbox,
)


@pytest.mark.parametrize("symbol", ("Platinum", "Palladium"))
def test_candidate_and_sparse_ledger_preserve_exact_mt5_symbol(symbol: str) -> None:
    candidate = replay.CandidateTrade(
        candidate_id="candidate-1",
        cell_id=f"{symbol}|H4|TREND",
        symbol=symbol,
        timeframe="H4",
        regime="TREND",
        strategy_name="EMACrossoverStrategy",
        promotion_status="PROCESS_VALIDATED_CURRENT_REFIT",
        asset_class="metal",
        risk_groups=(),
        entry_time="2026-01-01T00:00:00Z",
        direction=1,
        stop_risk_per_lot=100.0,
        margin_per_lot=1_000.0,
        volume_min=0.01,
        volume_max=10.0,
        volume_step=0.01,
        priority_score=1.0,
        legs=(
            replay.ExitLeg(
                exit_time="2026-01-02T00:00:00Z",
                fraction=1.0,
                net_pnl_per_entry_lot=25.0,
            ),
        ),
    ).normalized()
    projection, errors = replay._sparse_physical_winner_rows(  # noqa: SLF001
        {
            symbol: {
                "symbol": symbol,
                "regime_configs": {
                    "H4": {
                        "TREND": {
                            "strategy_name": "EMACrossoverStrategy",
                            "promotion_status": "PROMOTED",
                            "artifact_meta": {},
                        }
                    }
                },
            }
        },
        (symbol,),
    )

    assert candidate.symbol == symbol
    assert errors == []
    assert projection["symbols"] == [symbol]
    assert projection["tradeable_cell_ids"] == [f"{symbol}|H4|TREND"]


@pytest.mark.parametrize("wrong_case", ("PLATINUM", "platinum"))
def test_sparse_ledger_rejects_wrong_case_mt5_key(wrong_case: str) -> None:
    projection, errors = replay._sparse_physical_winner_rows(  # noqa: SLF001
        {
            wrong_case: {
                "symbol": "Platinum",
                "regime_configs": {},
            }
        },
        ("Platinum",),
    )

    assert projection["tradeable_cell_ids"] == []
    assert (
        f"LEDGER_SYMBOL_IDENTITY_MISMATCH:{wrong_case}:Platinum"
        in errors
    )


def _ledger() -> dict:
    grid = {}
    for timeframe in PORTFOLIO_REPLAY_REQUIRED_TIMEFRAMES:
        grid[timeframe] = {}
        for regime in PORTFOLIO_REPLAY_REQUIRED_REGIMES:
            promoted = timeframe == "H1" and regime == "TREND"
            grid[timeframe][regime] = {
                "strategy_name": "Momentum" if promoted else "NO_TRADE",
                "promotion_status": "PROMOTED" if promoted else "NO_TRADE",
                "artifact_meta": {
                    "no_trade": not promoted,
                    "artifact_version": "test-v1",
                },
            }
    return {"EURUSD": {"symbol": "EURUSD", "regime_configs": grid}}


def _policy(balance: float = 10_000.0) -> dict:
    return asdict(
        PortfolioPolicy(
            starting_balance=balance,
            allowed_promotion_statuses=("FROZEN_PRELOCKBOX",),
        )
    )


def _valid_evidence() -> tuple[dict, dict, dict, dict]:
    roster, errors = build_ledger_cell_roster(_ledger(), ["EURUSD"])
    assert errors[-1] == "BOOK_CURRENT_REFIT_REPLAY_AUTHORITY_REQUIRED"
    assert (
        sum(
            error.startswith("LEDGER_NON_WINNER_PLACEHOLDER_FORBIDDEN:")
            for error in errors
        )
        == 23
    )
    lineage = {
        "artifact_contract_sha256": "a" * 64,
        "deployment_contract_sha256": "b" * 64,
        "ledger_sha256": "c" * 64,
        "broker_specs_sha256": "d" * 64,
        "native_activation_id": "activation-test",
        "native_activation_manifest_sha256": "e" * 64,
        "native_activation_roster_sha256": "f" * 64,
        "native_bars_sha256_by_source": {
            "EURUSD|H1": "1" * 64,
            "EURUSD|M5": "3" * 64,
        },
        "native_sidecar_sha256_by_source": {
            "EURUSD|H1": "2" * 64,
            "EURUSD|M5": "4" * 64,
        },
    }
    costs = {
        "use_spread": True,
        "backtest_spread_mode": "conservative_max",
        "swap_model_enabled": True,
        "swap_model_min_holding_hours": 20.0,
        "swap_model_triple_rollover_day": "wednesday",
    }
    cell_id = "EURUSD|H1|TREND"
    summary = {
        "schema_version": PORTFOLIO_REPLAY_SCHEMA_VERSION,
        "starting_balance": 10_000.0,
        "accepted_positions": 120,
        "profit_factor": 1.20,
        "return_pct": 8.0,
        "max_mark_to_market_drawdown_pct": 8.0,
        "max_initial_stop_stress_drawdown_pct": 8.0,
        "observed_ruin": False,
        "mtm_candidate_coverage_pct": 100.0,
        "max_open_risk_pct": 1.0,
        "max_admission_total_stop_risk_pct": 1.0,
        "max_admission_factor_group_stop_risk_pct": 1.0,
        "max_margin_utilization_pct": 10.0,
        "exploratory_sequence_bootstrap": {
            "daily_return_observations": 500,
            "ruin_probability_wilson_95pct": [0.0, 1.0],
            "p95_max_drawdown_pct": 12.0,
        },
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
                    "partition": "final_lockbox",
                    "evaluation_start": "2024-01-01T00:00:00",
                    "evaluation_end": "2026-01-01T00:00:00",
                },
                "partial_run": False,
                "allow_cell_errors": False,
                "cell_generation_errors": [],
                "prelockbox_source_status": "FROZEN_PRELOCKBOX",
                "evidence_only_status_allowlist": ["FROZEN_PRELOCKBOX"],
                "global_lockbox_access_id": "access-test",
                "replayed_cell_ids": [cell_id],
                "cost_contract": costs,
                "mark_cost_contract": portfolio_mark_cost_contract(costs),
                "native_sources": {
                    cell_id: {
                        "source_kind": "broker_native",
                        "source_sha256": "1" * 64,
                        "sidecar_sha256": "2" * 64,
                        "provenance_valid": True,
                        "timestamp_semantics": "bar_open_utc",
                        "activation_id": "activation-test",
                        "activation_manifest_sha256": "e" * 64,
                    }
                },
                "valuation_sources": {
                    "EURUSD": {
                        "symbol": "EURUSD",
                        "timeframe": "M5",
                        "source_kind": "broker_native",
                        "source_sha256": "3" * 64,
                        "sidecar_sha256": "4" * 64,
                        "provenance_valid": True,
                        "timestamp_semantics": "bar_open_utc",
                        "activation_id": "activation-test",
                        "activation_manifest_sha256": "e" * 64,
                    }
                },
                "scenarios": [
                    {
                        "starting_balance": 10_000.0,
                        "policy": _policy(),
                        "summary": summary,
                    }
                ],
            },
        }
    )
    return evidence, lineage, roster, costs


def _validate(
    evidence: dict,
    lineage: dict,
    roster: dict,
    costs: dict,
    *,
    maximum_bootstrap_ruin_upper_95pct: float = 1.0,
    maximum_bootstrap_p95_drawdown_pct: float = 20.0,
) -> list[str]:
    return validate_funded_portfolio_replay_evidence(
        evidence,
        expected_lineage=lineage,
        expected_roster=roster,
        expected_cost_contract=costs,
        expected_policies={10_000.0: _policy()},
        minimum_calendar_days=730,
        minimum_accepted_positions=100,
        minimum_profit_factor=1.05,
        minimum_return_pct=0.0,
        maximum_drawdown_pct=20.0,
        require_mtm=True,
        maximum_bootstrap_ruin_upper_95pct=maximum_bootstrap_ruin_upper_95pct,
        maximum_bootstrap_p95_drawdown_pct=maximum_bootstrap_p95_drawdown_pct,
    )


def test_sealed_exact_contract_passes_and_tampering_fails() -> None:
    evidence, lineage, roster, costs = _valid_evidence()
    assert _validate(evidence, lineage, roster, costs) == []

    evidence["replay"]["scenarios"][0]["summary"]["return_pct"] = -50.0
    errors = _validate(evidence, lineage, roster, costs)
    assert "PORTFOLIO_EVIDENCE_SEAL_INVALID" in errors
    assert "PORTFOLIO_EVIDENCE_RETURN_TOO_LOW:10000.00" in errors


def test_bootstrap_daily_observation_depth_is_required() -> None:
    evidence, lineage, roster, costs = _valid_evidence()
    evidence["replay"]["scenarios"][0]["summary"]["exploratory_sequence_bootstrap"][
        "daily_return_observations"
    ] = 499
    evidence = seal_portfolio_replay_evidence(evidence)
    errors = _validate(evidence, lineage, roster, costs)
    assert "PORTFOLIO_EVIDENCE_BOOTSTRAP_DAILY_OBSERVATIONS_TOO_LOW:10000.00" in errors


def test_mtm_mark_cost_contract_requires_causal_swap_accrual() -> None:
    evidence, lineage, roster, costs = _valid_evidence()
    evidence["replay"]["mark_cost_contract"]["swap_accrued_through_mark"] = False
    evidence = seal_portfolio_replay_evidence(evidence)
    errors = _validate(evidence, lineage, roster, costs)
    assert "PORTFOLIO_EVIDENCE_MARK_COST_CONTRACT_MISMATCH" in errors


def test_funded_evidence_requires_authenticated_native_m5_valuation_source() -> None:
    evidence, lineage, roster, costs = _valid_evidence()
    evidence["replay"]["valuation_sources"]["EURUSD"]["source_sha256"] = "0" * 64
    evidence = seal_portfolio_replay_evidence(evidence)

    errors = _validate(evidence, lineage, roster, costs)

    assert "PORTFOLIO_EVIDENCE_VALUATION_BARS_HASH_MISMATCH:EURUSD" in errors


def test_changed_native_or_ledger_lineage_makes_evidence_stale() -> None:
    evidence, lineage, roster, costs = _valid_evidence()
    changed = dict(lineage)
    changed["native_activation_manifest_sha256"] = "0" * 64
    changed["ledger_sha256"] = "1" * 64
    errors = _validate(evidence, changed, roster, costs)
    assert (
        "PORTFOLIO_EVIDENCE_LINEAGE_MISMATCH:native_activation_manifest_sha256"
        in errors
    )
    assert "PORTFOLIO_EVIDENCE_LINEAGE_MISMATCH:ledger_sha256" in errors


def test_final_replay_is_stale_when_frozen_cap_artifact_changes() -> None:
    evidence, lineage, roster, costs = _valid_evidence()
    lineage = {
        **lineage,
        "cap_calibration_sha256": "3" * 64,
        "cap_calibration_selected_max_total_stop_risk_pct": 12.25,
    }
    evidence["lineage"] = dict(lineage)
    evidence = seal_portfolio_replay_evidence(evidence)
    assert _validate(evidence, lineage, roster, costs) == []

    changed = {**lineage, "cap_calibration_sha256": "4" * 64}
    errors = _validate(evidence, changed, roster, costs)
    assert "PORTFOLIO_EVIDENCE_LINEAGE_MISMATCH:cap_calibration_sha256" in errors


def test_final_evidence_uses_admission_cap_and_binds_summary_balance() -> None:
    evidence, lineage, roster, costs = _valid_evidence()
    summary = evidence["replay"]["scenarios"][0]["summary"]
    # Post-entry MTM drift is not a new allocation decision; the exact
    # conservative-capital admission statistic remains authoritative.
    summary["max_open_risk_pct"] = 50.0
    evidence = seal_portfolio_replay_evidence(evidence)
    assert _validate(evidence, lineage, roster, costs) == []

    evidence["replay"]["scenarios"][0]["summary"]["starting_balance"] = 9_999.0
    evidence = seal_portfolio_replay_evidence(evidence)
    errors = _validate(evidence, lineage, roster, costs)
    assert "PORTFOLIO_EVIDENCE_SUMMARY_BALANCE_MISMATCH:10000.00" in errors


def test_final_evidence_requires_stop_stress_bootstrap_and_admission_metrics() -> None:
    evidence, lineage, roster, costs = _valid_evidence()
    summary = evidence["replay"]["scenarios"][0]["summary"]
    summary["max_initial_stop_stress_drawdown_pct"] = 25.0
    summary["exploratory_sequence_bootstrap"]["p95_max_drawdown_pct"] = 25.0
    summary.pop("max_admission_total_stop_risk_pct")
    evidence = seal_portfolio_replay_evidence(evidence)
    errors = _validate(evidence, lineage, roster, costs)
    assert "PORTFOLIO_EVIDENCE_STOP_STRESS_DRAWDOWN_TOO_HIGH:10000.00" in errors
    assert "PORTFOLIO_EVIDENCE_BOOTSTRAP_DRAWDOWN_TOO_HIGH:10000.00" in errors
    assert "PORTFOLIO_EVIDENCE_ADMISSION_RISK_CAP_BREACH:10000.00" in errors


def test_final_evidence_enforces_frozen_cap_artifacts_exact_ruin_threshold() -> None:
    evidence, lineage, roster, costs = _valid_evidence()
    evidence["replay"]["scenarios"][0]["summary"]["exploratory_sequence_bootstrap"][
        "ruin_probability_wilson_95pct"
    ] = [0.0, 0.75]
    evidence = seal_portfolio_replay_evidence(evidence)
    errors = _validate(
        evidence,
        lineage,
        roster,
        costs,
        maximum_bootstrap_ruin_upper_95pct=0.5,
    )
    assert "PORTFOLIO_EVIDENCE_BOOTSTRAP_RUIN_TOO_HIGH:10000.00" in errors


def test_partial_or_short_replay_cannot_pass() -> None:
    evidence, lineage, roster, costs = _valid_evidence()
    evidence["replay"]["partial_run"] = True
    evidence["replay"]["window"]["evaluation_end"] = "2024-06-01T00:00:00"
    evidence = seal_portfolio_replay_evidence(evidence)
    errors = _validate(evidence, lineage, roster, costs)
    assert "PORTFOLIO_EVIDENCE_PARTIAL_RUN" in errors
    assert "PORTFOLIO_EVIDENCE_CALENDAR_HORIZON_TOO_SHORT" in errors


def test_authoritative_roster_bindings_cannot_be_omitted_from_evidence() -> None:
    evidence, lineage, roster, costs = _valid_evidence()
    expected = {
        **roster,
        "authoritative": True,
        "authority_status": "PROCESS_VALIDATED_CURRENT_REFIT",
        "physical_ledger_sha256": "5" * 64,
        "book_accounting": {"accounting_sha256": "6" * 64},
        "current_refit_lineage": {"current_refit_receipt_sha256": "7" * 64},
    }

    errors = _validate(evidence, lineage, expected, costs)

    assert "PORTFOLIO_EVIDENCE_ROSTER_MISMATCH:authoritative" in errors
    assert "PORTFOLIO_EVIDENCE_ROSTER_MISMATCH:book_accounting" in errors
    assert "PORTFOLIO_EVIDENCE_CURRENT_REFIT_ROSTER_MISMATCH" in errors
    assert "PORTFOLIO_EVIDENCE_CANDIDATE_CELL_ROSTER_INVALID" in errors
    assert "CURRENT_REFIT_SUCCESSOR_CONTRACT_INVALID" in errors


def test_physical_ledger_is_sparse_and_does_not_force_four_regime_leaves() -> None:
    ledger = _ledger()
    ledger["EURUSD"]["regime_configs"] = {
        "H1": {"TREND": ledger["EURUSD"]["regime_configs"]["H1"]["TREND"]}
    }
    roster, errors = build_ledger_cell_roster(ledger, ["EURUSD"])

    assert roster["expected_cell_count"] == 24
    assert roster["physical_winner_cell_count"] == 1
    assert roster["tradeable_cell_ids"] == ["EURUSD|H1|TREND"]
    assert errors == ["BOOK_CURRENT_REFIT_REPLAY_AUTHORITY_REQUIRED"]


def test_no_trade_placeholders_are_not_physical_regime_evidence() -> None:
    roster, errors = build_ledger_cell_roster(_ledger(), ["EURUSD"])

    assert roster["physical_winner_cell_count"] == 1
    assert any(
        error.startswith("LEDGER_NON_WINNER_PLACEHOLDER_FORBIDDEN:") for error in errors
    )


def test_book_and_runtime_pin_same_winner_v14() -> None:
    import pm_book_multiplicity as book
    import pm_winner_selection as winner

    accounting, errors = build_book_possible_slot_accounting(
        object(), evidence_family=()
    )

    assert book.PINNED_WINNER_SELECTION_CONTRACT_VERSION == (
        "fxpm.winner-selection.v14"
    )
    assert winner.WINNER_SELECTION_CONTRACT_VERSION == "fxpm.winner-selection.v14"
    assert errors == ["BOOK_DECISION_INVALID_OR_FORGED"]
    assert accounting["possible_cell_count"] == 0


def test_current_refit_roster_stops_at_invalid_book_decision() -> None:
    winner = _ledger()["EURUSD"]["regime_configs"]["H1"]["TREND"]
    winner["promotion_status"] = "PROCESS_VALIDATED_CURRENT_REFIT"
    ledger = {
        "EURUSD": {
            "symbol": "EURUSD",
            "regime_configs": {"H1": {"TREND": winner}},
        }
    }

    roster, errors = build_current_refit_replay_roster(
        ledger,
        ["EURUSD"],
        current_book_decision=object(),
        book_evidence_family=(),
        historical_process_decision={},
        causal_precommit_payload={},
        causal_evidence_payload={},
        causal_reconciliation_payload={},
        accepted_book_generation_receipt={},
        current_refit_receipt={},
        as_of_utc="2026-07-14T00:00:00Z",
    )

    assert roster["authority_status"] == "BOOK_BLOCKED"
    assert roster["authoritative"] is False
    assert roster["current_refit_execution_reverified"] is False
    assert errors == [
        "CAUSAL_PROCESS_DECISION_INVALID_OR_FORGED",
        "CURRENT_REFIT_EXECUTION_REVERIFIER_REQUIRED",
        "BOOK_DECISION_INVALID_OR_FORGED",
    ]


def test_book_decision_replays_complete_sparse_non_economic_states(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import pm_book_multiplicity as book
    import pm_winner_selection as winner
    from tests import test_book_multiplicity as book_fixture
    from tests.test_book_multiplicity import (
        _available_group_with_counts,
        _family,
    )

    monkeypatch.setattr(
        book,
        "PINNED_WINNER_SELECTION_CONTRACT_VERSION",
        winner.WINNER_SELECTION_CONTRACT_VERSION,
    )
    expected_family = _family()
    expected = book.evaluate_historical_book_multiplicity(expected_family)
    accounting, errors = build_book_possible_slot_accounting(
        expected,
        evidence_family=expected_family,
    )
    assert errors == []
    assert len(accounting["source_unavailable_slot_ids"]) == 2_400
    assert accounting["insufficient_evidence_slot_ids"] == []
    assert accounting["terminal_technical_slot_ids"] == []

    technical_family = _family(
        overrides={
            ordinal: book_fixture._technical_terminal_cell(ordinal)  # noqa: SLF001
            for ordinal in range(len(book.REGIME_ORDER))
        }
    )
    technical = book.evaluate_historical_book_multiplicity(technical_family)
    accounting, errors = build_book_possible_slot_accounting(
        technical,
        evidence_family=technical_family,
    )
    assert errors == []
    assert len(accounting["terminal_technical_slot_ids"]) == 4

    insufficient_family = _family(
        overrides=book_fixture._insufficient_group(  # noqa: SLF001
            postsearch=False,
            sibling_winner=False,
        )
    )
    insufficient = book.evaluate_historical_book_multiplicity(insufficient_family)
    accounting, errors = build_book_possible_slot_accounting(
        insufficient,
        evidence_family=insufficient_family,
    )
    assert errors == []
    assert len(accounting["insufficient_evidence_slot_ids"]) == 1

    alternate_family = _family(overrides=_available_group_with_counts(0, (1, 0, 0, 0)))
    forged = book.evaluate_historical_book_multiplicity(alternate_family)
    assert book.verify_historical_book_multiplicity_decision(forged)
    _, errors = build_book_possible_slot_accounting(
        forged,
        evidence_family=expected_family,
    )
    assert errors == ["BOOK_DECISION_INVALID_OR_FORGED"]


def test_causal_pass_checksum_is_replayed_from_raw_payloads() -> None:
    from tests.test_causal_process import (
        _build_corpus,
        _evaluate,
        _owned,
        _refresh_anchor_hashes,
        _set_group_regime_support,
    )

    expected = _build_corpus()
    alternate = _owned(expected)
    anchor = alternate.evidence_payload["anchors"][0]
    _set_group_regime_support(
        anchor,
        0,
        {"TREND": 4, "RANGE": 3, "BREAKOUT": 2, "CHOP": 0},
    )
    _refresh_anchor_hashes(anchor)
    alternate.refresh_reconciliation()
    forged_pass = _evaluate(alternate).body()
    assert forged_pass["decision"] == "OFFLINE_PROCESS_VALIDATION_PASS"

    assert validate_causal_process_authority(
        forged_pass,
        precommit_payload=expected.precommit_payload,
        evidence_payload=expected.evidence_payload,
        reconciliation_payload=expected.reconciliation_payload,
    ) == ["CAUSAL_PROCESS_DECISION_INVALID_OR_FORGED"]


def test_self_consistent_current_refit_receipt_has_no_raw_execution_authority() -> None:
    from pm_model_lifecycle import build_process_validated_current_refit_lineage
    from tests.test_model_lifecycle import _process_refit_receipts

    process_decision, book_receipt, forged_refit = _process_refit_receipts(
        cell_change=lambda cells: cells[0].update(
            {
                "parameter_sha256": "8" * 64,
                "output_artifact_sha256": "9" * 64,
            }
        )
    )
    lineage = build_process_validated_current_refit_lineage(
        historical_process_decision=process_decision,
        accepted_book_generation_receipt=book_receipt,
        current_refit_receipt=forged_refit,
        as_of_utc="2026-07-13T00:00:00Z",
    )
    assert lineage["contract_status"] == "PROBATION_ONLY"
    assert validate_current_refit_execution_authority(forged_refit) == [
        "CURRENT_REFIT_EXECUTION_REVERIFIER_REQUIRED"
    ]


def _frozen_ledger(*, two_candidates: bool = False) -> dict:
    ledger = _ledger()
    symbol = ledger["EURUSD"]
    symbol["holdout_metrics"] = {}
    symbol["edge_challenger_book"] = {"regime_configs": {}}
    symbol["data_window_meta"] = {
        "windows": {
            "tuning_validation": {
                "start_ts": "2022-01-01T00:00:00Z",
                "end_ts": "2024-01-01T00:00:00Z",
            },
            "final_lockbox": {
                "start_ts": "2024-01-08T00:00:00Z",
                "end_ts": "2026-01-08T00:00:00Z",
            },
        }
    }
    first = symbol["regime_configs"]["H1"]["TREND"]
    first["promotion_status"] = "FROZEN_PRELOCKBOX"
    first["holdout_metrics"] = {}
    first["validation_evidence"] = {
        "selection_score_raw": 0.75,
        "temporal_lineage": {
            "parameter_selection_cutoff_utc": "2024-01-01T00:00:00Z",
            "performance_evidenced_through_utc": "2024-01-01T00:00:00Z",
        },
    }
    if two_candidates:
        second = symbol["regime_configs"]["M5"]["RANGE"]
        second.update(
            {
                "strategy_name": "MeanReversion",
                "promotion_status": "FROZEN_PRELOCKBOX",
                "artifact_meta": {"no_trade": False},
                "holdout_metrics": {},
                "validation_evidence": {"selection_score_raw": 0.5},
            }
        )
    return ledger


def test_canonical_prelockbox_artifact_binds_effective_priority_and_rejects_final_metrics() -> (
    None
):
    ledger = _frozen_ledger()
    artifact = build_prelockbox_roster_artifact(
        ledger=ledger,
        symbols=["EURUSD"],
        lineage={"critical_module_code_contract_sha256": "a" * 64},
    )
    assert (
        validate_prelockbox_roster_artifact(artifact, expected_symbols=["EURUSD"]) == []
    )
    stale_code_errors = validate_prelockbox_roster_artifact(
        artifact,
        expected_symbols=["EURUSD"],
        expected_lineage={"critical_module_code_contract_sha256": "b" * 64},
    )
    assert (
        "PRELOCKBOX_LINEAGE_MISMATCH:critical_module_code_contract_sha256"
        in stale_code_errors
    )

    # The whole cell is frozen, including selection_score_raw used by the
    # replay priority fallback.  Any edit breaks both seal and roster rebuild.
    artifact["frozen_ledger"]["EURUSD"]["regime_configs"]["H1"]["TREND"][
        "validation_evidence"
    ]["selection_score_raw"] = 999.0
    errors = validate_prelockbox_roster_artifact(artifact, expected_symbols=["EURUSD"])
    assert "PRELOCKBOX_SEAL_INVALID" in errors
    assert "PRELOCKBOX_ROSTER_REBUILD_MISMATCH" in errors

    clean = _frozen_ledger()
    clean["EURUSD"]["regime_configs"]["H1"]["TREND"]["holdout_metrics"] = {
        "profit_factor": 2.0
    }
    with pytest.raises(ValueError, match="PRELOCKBOX_CELL_HAS_FINAL_OUTCOMES"):
        build_prelockbox_roster_artifact(ledger=clean, symbols=["EURUSD"], lineage={})


def test_prelockbox_code_contract_binds_live_admission_and_readiness_modules() -> None:
    contract = critical_module_code_contract(Path(__file__).resolve().parents[1])
    assert contract["missing"] == []
    for required in (
        "pm_main.py",
        "pm_mt5.py",
        "pm_live_readiness.py",
        "pm_institutional_supervisor.py",
        "pm_risk_admission.py",
        "pm_promotion_authorization.py",
    ):
        assert required in contract["files"]


def test_joint_pass_release_is_all_or_none_and_cannot_prune_survivors() -> None:
    artifact = build_prelockbox_roster_artifact(
        ledger=_frozen_ledger(two_candidates=True),
        symbols=["EURUSD"],
        lineage={},
    )
    released = materialize_joint_pass_ledger(
        artifact, portfolio_evidence_sha256="f" * 64
    )
    assert validate_released_ledger_against_prelockbox(released, artifact) == []
    assert (
        released["EURUSD"]["regime_configs"]["H1"]["TREND"]["promotion_status"]
        == "PROMOTED"
    )
    assert (
        released["EURUSD"]["regime_configs"]["M5"]["RANGE"]["promotion_status"]
        == "PROMOTED"
    )
    released["EURUSD"]["regime_configs"]["M5"]["RANGE"]["promotion_status"] = "BLOCKED"
    errors = validate_released_ledger_against_prelockbox(released, artifact)
    assert "RELEASED_CELL_NOT_PROMOTED:EURUSD|M5|RANGE" in errors
    assert "RELEASED_ROSTER_NOT_ALL_OR_NONE" in errors


def test_global_final_lockbox_access_is_burned_before_rerun(tmp_path: Path) -> None:
    cfg = PipelineConfig(
        output_dir=tmp_path / "out",
        lockbox_access_registry_path=str(tmp_path / "joint_registry.json"),
    )
    frozen = _frozen_ledger()
    first = consume_global_final_lockbox_once(
        config=cfg,
        config_path=tmp_path / "config.json",
        frozen_ledger=frozen,
        roster_sha256="a" * 64,
        prelockbox_file_sha256="b" * 64,
    )
    assert first
    with pytest.raises(RuntimeError, match="FINAL_LOCKBOX_ALREADY_CONSUMED"):
        consume_global_final_lockbox_once(
            config=cfg,
            config_path=tmp_path / "config.json",
            frozen_ledger=frozen,
            roster_sha256="a" * 64,
            prelockbox_file_sha256="b" * 64,
        )


def test_active_native_contract_rejects_non_authoritative_partial_fixture(
    tmp_path: Path,
) -> None:
    native = tmp_path / "data" / "native"
    native.mkdir(parents=True)
    write_v5_run(native, "portfolio-contract-v5")
    activation = activate_v5_run(
        native,
        "portfolio-contract-v5",
        "native-test-v5",
    )
    assert activation["status"] == "failed"
    assert activation["active_pointer_updated"] is False
    assert activation["production_authoritative"] is False
    assert activation["counts"] == {
        "evidence_candidate": 0,
        "pending": 0,
        "source_unavailable": 0,
        "symbols_with_evidence_candidates": 0,
        "technical_error": 6,
        "terminal": 6,
        "total": 6,
    }
    assert not (native / "ACTIVE.json").exists()

    contract, errors = active_native_portfolio_contract(tmp_path / "data", ["EURUSD"])
    assert contract["symbols"] == ["EURUSD"]
    assert errors == ["NATIVE_ACTIVE_POINTER_UNREADABLE:FileNotFoundError"]


def test_active_native_contract_binds_v5_terminal_cells_without_laundering(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from pm_native_activation import (
        ACTIVE_POINTER_SCHEMA,
        ACTIVATION_MANIFEST_SCHEMA,
        NATIVE_EVIDENCE_CANDIDATE,
        NATIVE_SOURCE_UNAVAILABLE,
        NATIVE_TECHNICAL_ERROR,
        UNIVERSE_AUTHENTICITY_SCHEMA,
    )

    native = tmp_path / "data" / "native"
    activation_id = "v5-terminal-contract"
    manifest_path = native / "activations" / f"{activation_id}.json"
    manifest_path.parent.mkdir(parents=True)
    entries = {
        "EURUSD": {
            "M5": {
                "status": NATIVE_EVIDENCE_CANDIDATE,
                "symbol": "EURUSD",
                "timeframe": "M5",
                "bars_sha256": "1" * 64,
                "sidecar_sha256": "2" * 64,
                "reason_codes": [],
            },
            "M15": {
                "status": NATIVE_TECHNICAL_ERROR,
                "symbol": "EURUSD",
                "timeframe": "M15",
                "bars_sha256": None,
                "sidecar_sha256": "3" * 64,
                "reason_codes": ["SOURCE_TERMINAL_TECHNICAL:HISTORY_GAP"],
            },
            "M30": {
                "status": NATIVE_SOURCE_UNAVAILABLE,
                "symbol": "EURUSD",
                "timeframe": "M30",
                "bars_sha256": None,
                "sidecar_sha256": "4" * 64,
                "reason_codes": ["SOURCE_UNAVAILABLE:SYMBOL_NOT_OFFERED"],
            },
        }
    }
    manifest = {
        "schema_version": ACTIVATION_MANIFEST_SCHEMA,
        "status": "active",
        "activation_id": activation_id,
        "entries": entries,
    }
    manifest_raw = json.dumps(manifest, sort_keys=True).encode("utf-8")
    manifest_path.write_bytes(manifest_raw)
    (native / "ACTIVE.json").write_text(
        json.dumps(
            {
                "schema_version": ACTIVE_POINTER_SCHEMA,
                "activation_id": activation_id,
                "manifest_relative_path": f"activations/{activation_id}.json",
                "manifest_sha256": hashlib.sha256(manifest_raw).hexdigest(),
            }
        ),
        encoding="utf-8",
    )

    class Universe:
        errors: list[str] = []
        eligible_symbols = ("EURUSD",)
        excluded_symbols: dict[str, dict] = {}
        eligibility_schema = UNIVERSE_AUTHENTICITY_SCHEMA
        explicit_contract = True
        exact_symbol_identity = True

        @staticmethod
        def cell_status(symbol: str, timeframe: str) -> str:
            return str(entries[symbol][timeframe]["status"])

    monkeypatch.setattr(
        "pm_native_activation.resolve_active_native_universe",
        lambda *_args, **_kwargs: Universe(),
    )
    contract, errors = active_native_portfolio_contract(
        tmp_path / "data",
        ["EURUSD"],
        timeframes=("M5", "M15", "M30"),
    )

    assert errors == []
    assert contract["activation_entry_count"] == 3
    assert contract["bars_sha256_by_source"] == {"EURUSD|M5": "1" * 64}
    assert contract["sidecar_sha256_by_source"] == {"EURUSD|M5": "2" * 64}
    expected_rows = [
        {
            "cell_id": "EURUSD|M5",
            "status": NATIVE_EVIDENCE_CANDIDATE,
            "bars_sha256": "1" * 64,
            "sidecar_sha256": "2" * 64,
            "reason_codes": [],
        },
        {
            "cell_id": "EURUSD|M15",
            "status": NATIVE_TECHNICAL_ERROR,
            "bars_sha256": "",
            "sidecar_sha256": "3" * 64,
            "reason_codes": ["SOURCE_TERMINAL_TECHNICAL:HISTORY_GAP"],
        },
        {
            "cell_id": "EURUSD|M30",
            "status": NATIVE_SOURCE_UNAVAILABLE,
            "bars_sha256": "",
            "sidecar_sha256": "4" * 64,
            "reason_codes": ["SOURCE_UNAVAILABLE:SYMBOL_NOT_OFFERED"],
        },
    ]
    assert contract["activation_roster_sha256"] == canonical_json_sha256(
        sorted(expected_rows, key=lambda row: row["cell_id"])
    )


def test_active_native_contract_preserves_v5_symbol_order_and_broker_case(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from pm_native_activation import (
        ACTIVE_POINTER_SCHEMA,
        ACTIVATION_MANIFEST_SCHEMA,
        NATIVE_EVIDENCE_CANDIDATE,
        UNIVERSE_AUTHENTICITY_SCHEMA,
    )

    activation_id = "v5-exact-symbol-identity"
    native = tmp_path / "data" / "native"
    manifest_path = native / "activations" / f"{activation_id}.json"
    manifest_path.parent.mkdir(parents=True)
    configured = ("EURUSD", "Platinum")
    entries = {
        symbol: {
            "D1": {
                "status": NATIVE_EVIDENCE_CANDIDATE,
                "symbol": symbol,
                "timeframe": "D1",
                "bars_sha256": str(offset) * 64,
                "sidecar_sha256": str(offset + 2) * 64,
                "reason_codes": [],
            }
        }
        for offset, symbol in enumerate(configured, start=1)
    }
    manifest = {
        "schema_version": ACTIVATION_MANIFEST_SCHEMA,
        "status": "active",
        "activation_id": activation_id,
        "entries": entries,
    }
    manifest_raw = json.dumps(manifest, sort_keys=True).encode("utf-8")
    manifest_path.write_bytes(manifest_raw)
    (native / "ACTIVE.json").write_text(
        json.dumps(
            {
                "schema_version": ACTIVE_POINTER_SCHEMA,
                "activation_id": activation_id,
                "manifest_relative_path": f"activations/{activation_id}.json",
                "manifest_sha256": hashlib.sha256(manifest_raw).hexdigest(),
            }
        ),
        encoding="utf-8",
    )
    captured: dict[str, tuple[str, ...]] = {}

    class Universe:
        errors: list[str] = []
        eligible_symbols = configured
        excluded_symbols: dict[str, dict] = {}
        eligibility_schema = UNIVERSE_AUTHENTICITY_SCHEMA
        explicit_contract = True
        exact_symbol_identity = True

        @staticmethod
        def cell_status(symbol: str, timeframe: str) -> str:
            return str(entries[symbol][timeframe]["status"])

    def resolve(_data_dir: Path, symbols: list[str], **_kwargs: object) -> Universe:
        captured["symbols"] = tuple(symbols)
        return Universe()

    monkeypatch.setattr("pm_native_activation.resolve_active_native_universe", resolve)
    contract, errors = active_native_portfolio_contract(
        tmp_path / "data",
        configured,
        timeframes=("D1",),
    )

    assert errors == []
    assert captured["symbols"] == configured
    assert contract["symbols"] == list(configured)
    assert set(contract["bars_sha256_by_source"]) == {
        "EURUSD|D1",
        "Platinum|D1",
    }


def test_synchronized_generator_accepts_only_exact_activated_native_source() -> None:
    frame = pd.DataFrame(
        {"Open": [1.0], "High": [1.1], "Low": [0.9], "Close": [1.0]},
        index=pd.DatetimeIndex(["2025-01-01"]),
    )
    meta = {
        "source_kind": "broker_native",
        "symbol": "EURUSD",
        "timeframe": "H1",
        "sha256": "a" * 64,
        "provenance_valid": True,
        "provenance_errors": [],
        "timestamp_semantics": "bar_open_utc",
        "activation_id": "native-test",
        "activation_manifest_sha256": "b" * 64,
        "activation_entry": {
            "status": "active",
            "bars_sha256": "a" * 64,
            "sidecar_sha256": "c" * 64,
        },
        "rows": 1,
        "common_calendar_boundaries": {
            "final_lockbox": {
                "start_ts": "2024-01-01T00:00:00Z",
                "end_ts": "2026-01-01T00:00:00Z",
            }
        },
    }

    class Loader:
        def load_native_timeframe(self, symbol: str, timeframe: str):
            return frame, meta

    loaded, report, error = _native_replay_frame(Loader(), "EURUSD", "H1")
    assert error == ""
    assert loaded is not None and len(loaded) == 1
    assert _partition_pair(report, "final_lockbox") == (
        pd.Timestamp("2024-01-01"),
        pd.Timestamp("2026-01-01"),
    )

    class ResampledLoader:
        def load_native_timeframe(self, symbol: str, timeframe: str):
            return frame, {**meta, "source_kind": "resampled_from_m5"}

    loaded, _, error = _native_replay_frame(ResampledLoader(), "EURUSD", "H1")
    assert loaded is None
    assert error == "native_source_not_broker_native"


def test_final_price_source_cannot_open_without_global_access_registration() -> None:
    class GuardedLoader:
        called = False

        def load_native_timeframe(self, symbol: str, timeframe: str):
            self.called = True
            raise AssertionError("price source must remain unopened")

    loader = GuardedLoader()
    loaded, report, error = _native_replay_frame(
        loader,
        "EURUSD",
        "H1",
        partition="final_lockbox",
        global_lockbox_access_id=None,
    )
    assert loaded is None
    assert report == {}
    assert error == "global_final_lockbox_access_not_registered"
    assert loader.called is False


def test_content_addressed_evidence_never_overwrites(tmp_path: Path) -> None:
    evidence, _, _, _ = _valid_evidence()
    first = write_immutable_portfolio_replay_evidence(tmp_path, evidence)
    second = write_immutable_portfolio_replay_evidence(tmp_path, evidence)
    assert first == second
    assert first.name.startswith("portfolio_replay_evidence_")


def test_live_readiness_fails_closed_when_evidence_path_is_empty(
    tmp_path: Path,
) -> None:
    config_path = tmp_path / "config.json"
    config_path.write_text("{}", encoding="utf-8")
    cfg = PipelineConfig(
        data_dir=tmp_path / "data",
        output_dir=tmp_path / "out",
        log_dir=tmp_path / "logs",
        production_portfolio_replay_evidence_path="",
    )
    report = validate_live_readiness(
        cfg,
        symbols=["EURUSD"],
        config_path=str(config_path),
    )
    assert not report.ok
    assert any(
        finding.code == "synchronized_portfolio_evidence_unproven"
        for finding in report.errors
    )


def test_funded_config_keeps_shared_capital_gate_mandatory() -> None:
    root = Path(__file__).resolve().parents[1]
    payload = json.loads((root / "config.json").read_text(encoding="utf-8"))
    pipeline = payload["pipeline"]
    assert pipeline["production_portfolio_replay_evidence_required"] is True
    assert pipeline["production_portfolio_replay_evidence_path"] == ""
    assert pipeline["production_cap_calibration_path"] == ""
    assert pipeline["production_cap_calibration_sha256"] == ""
    assert pipeline["production_prelockbox_roster_path"] == ""
    assert pipeline["production_prelockbox_roster_sha256"] == ""
    assert pipeline["production_portfolio_replay_required_starting_balances"] == [
        1000.0,
        10000.0,
    ]
    assert pipeline["production_portfolio_replay_min_calendar_days"] >= 730
    assert pipeline["production_portfolio_replay_min_positions"] >= 100
    assert pipeline["production_portfolio_replay_min_profit_factor"] >= 1.05
    assert pipeline["production_portfolio_replay_min_return_pct"] >= 0.0
    assert pipeline["production_portfolio_replay_max_drawdown_pct"] <= 20.0
    assert pipeline["production_portfolio_replay_require_mtm"] is True
