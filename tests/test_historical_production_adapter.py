from __future__ import annotations

import copy
import csv
from dataclasses import asdict, replace
from datetime import date, datetime, timedelta, timezone
import hashlib
import json
from pathlib import Path
import struct
import time

import pandas as pd
import pytest

import pm_authority_spool as spool
import pm_authoritative_search_execution as search_execution
import pm_book_multiplicity as book
import pm_causal_process as causal
import pm_current_refit_artifacts as current_refit
import pm_historical_anchor_orchestration as orchestration
import pm_historical_production_adapter as adapter
import pm_native_prefix_production as native_prefix
import pm_training_horizon as training_horizon
from pm_core import (
    BACKTEST_INSTRUMENT_SPEC_SCHEMA_VERSION,
    Backtester,
    InstrumentSpec,
    PipelineConfig,
    build_backtest_cost_ladder_contract,
    build_backtest_position_events,
    build_backtest_trade_identity_pnl_evidence,
)
from pm_native_activation import ActiveNativeResolution
from pm_native_request_contract import PRODUCTION_ARTIFACT_COLUMNS, TIMEFRAME_SECONDS
from pm_strategies import StrategyRegistry
from pm_trade_power import (
    build_numerical_execution_cost_attestation,
    build_trade_sample_evidence,
)
import pm_winner_raw_evidence as raw_winner
import pm_winner_selection as winner
from tests import test_training_horizon as horizon_fixture
from tests import test_native_prefix_production as native_prefix_fixture


PROCESS_PRECOMMIT = "a" * 64
OBSERVED_TIMEFRAME = "D1"
OBSERVED_SYMBOL = book.PINNED_SYMBOL_ORDER[0]


def _canonical(value):
    return json.dumps(
        value,
        allow_nan=False,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("ascii")


def _sha(value):
    if isinstance(value, str):
        value = {"label": value}
    return hashlib.sha256(_canonical(value)).hexdigest()


def test_precommitted_evidence_roles_come_only_from_horizon_status() -> None:
    assert adapter._precommitted_evidence_roles(
        {
            "attempts": [
                {"status": "PLANNED"},
                {"status": "PROCESS_REJECTED"},
            ]
        },
        2,
    ) == ("EVALUATION_REQUIRED", "PRESEARCH_PROCESS_REJECTED")
    with pytest.raises(
        adapter.HistoricalProductionAdapterError,
        match="PRECOMMITTED_EVIDENCE_ROLE_ROSTER_INVALID",
    ):
        adapter._precommitted_evidence_roles({"attempts": [{"status": "PLANNED"}]}, 2)


def _state_context_admission_inputs(
    *,
    candidate_boundary_limited: bool = False,
) -> tuple[dict[str, object], dict[str, object], dict[str, object]]:
    registry_order = ["StrategyA", "StrategyB", "StrategyC"]
    attempt_policy = {
        "one_full_default_per_strategy": True,
        "default_attempt_count": 3,
        "additional_nondefault_attempt_count": 1,
        "attempt_count_per_observed_cell": 4,
    }
    semantics = {
        "registry_order": registry_order,
        "registry_order_sha256": _sha(registry_order),
        "strategy_domain_count": len(registry_order),
        "attempt_policy": attempt_policy,
    }
    process = {
        "schema_version": "test-production-search-process",
        "semantics": semantics,
        "process_semantics_sha256": _sha(semantics),
    }
    process_sha256 = _sha(process)
    candidate = {
        "candidate_id": "candidate-1",
        "candidate_definition_sha256": "c" * 64,
        "folds": [] if candidate_boundary_limited else [{"fold_id": "fold-1"}],
    }
    selected_context = 5
    strategy_ids = ["StrategyA", "StrategyB", "StrategyC", "StrategyA"]
    required_contexts = [3, 4, 7, 6]
    trials = []
    trial_registry = []
    attempts = []
    disposition_hashes = []
    for ordinal, (strategy_id, required_context) in enumerate(
        zip(strategy_ids, required_contexts, strict=True)
    ):
        admitted = required_context <= selected_context
        disposition_body = {
            "schema_version": "fxpm.production-trial-state-context-disposition.v1",
            "candidate_id": candidate["candidate_id"],
            "candidate_definition_sha256": candidate["candidate_definition_sha256"],
            "state_requirement_sha256": _sha({"requirement": ordinal}),
            "required_context_bars": required_context,
            "selected_context_bars": selected_context,
            "status": "PLANNED" if admitted else "PROCESS_REJECTED",
            "reason_code": (
                None if admitted else "STATE_CONTEXT_SOURCE_BOUNDARY_SHORTFALL"
            ),
        }
        disposition = {
            **disposition_body,
            "disposition_sha256": _sha(disposition_body),
        }
        disposition_hashes.append(disposition["disposition_sha256"])
        trial_id = f"trial-{ordinal}"
        strategy_release_sha256 = _sha({"strategy_release": strategy_id})
        parameter_set_sha256 = _sha({"parameters": ordinal})
        trial_definition_sha256 = _sha({"trial_definition": ordinal})
        trials.append(
            {
                "trial_id": trial_id,
                "strategy_id": strategy_id,
                "strategy_release_sha256": strategy_release_sha256,
                "parameter_set_sha256": parameter_set_sha256,
                "trial_definition_sha256": trial_definition_sha256,
                "state_context_disposition": disposition,
            }
        )
        trial_registry.append(
            {
                "trial_id": trial_id,
                "strategy_id": strategy_id,
                "strategy_release_sha256": strategy_release_sha256,
                "parameter_set_sha256": parameter_set_sha256,
                "trial_definition_sha256": trial_definition_sha256,
                "state_context_disposition_sha256": disposition["disposition_sha256"],
            }
        )
        if candidate_boundary_limited:
            execution_status = "PROCESS_REJECTED"
            execution_reason = "SOURCE_BOUNDARY_SHORTFALL"
        else:
            execution_status = disposition["status"]
            execution_reason = disposition["reason_code"]
        attempts.append(
            {
                "attempt_id": _sha({"attempt": ordinal}),
                "trial_id": trial_id,
                "trial_definition_sha256": trial_definition_sha256,
                "strategy_id": strategy_id,
                "strategy_release_sha256": strategy_release_sha256,
                "parameter_set_sha256": parameter_set_sha256,
                "candidate_id": candidate["candidate_id"],
                "candidate_definition_sha256": candidate["candidate_definition_sha256"],
                "state_context_disposition_sha256": disposition["disposition_sha256"],
                "status": execution_status,
                "reason_code": execution_reason,
            }
        )
    selection_body = {
        "schema_version": "fxpm.production-cell-state-context-selection.v1",
        "selection_rule": (
            "LARGEST_SOURCE_GEOMETRY_FEASIBLE_COMMON_CONTEXT_DESCENDING"
        ),
        "selected_common_context_bars": selected_context,
        "candidate_boundary_limited": candidate_boundary_limited,
        "required_context_roster_sha256": _sha(required_contexts),
        "disposition_sha256": disposition_hashes,
        "disposition_roster_sha256": _sha(disposition_hashes),
        "within_selected_context_count": 2,
        "exceeds_selected_context_count": 2,
    }
    selection = {
        **selection_body,
        "receipt_sha256": _sha(selection_body),
    }
    cell_identity = {
        "symbol": "EURUSD",
        "timeframe": "D1",
        "regime": "TREND",
    }
    horizon = {
        "cell": cell_identity,
        "candidates": [candidate],
        "trials": trials,
        "trial_registry": trial_registry,
        "trial_registry_sha256": _sha(trial_registry),
        "attempts": attempts,
        "attempt_roster_sha256": _sha(attempts),
        "plan_status": (
            "HISTORICAL_BOUNDARY_LIMITED"
            if candidate_boundary_limited
            else "READY_FOR_HISTORICAL_EVIDENCE"
        ),
    }
    horizon_receipt_sha256 = _sha(horizon)
    cell_body = {
        "schema_version": "fxpm.production-search-cell-plan-receipt.v2",
        "mode": "BOUNDED_RAW_WINNER_CANARY",
        "search_process_receipt_sha256": process_sha256,
        "attempt_count": len(attempts),
        "cell": cell_identity,
        "boundary_limited": candidate_boundary_limited,
        "dependency_payload": {"feature_lookback_bars": selected_context},
        "state_context_selection": selection,
        "horizon_plan_binding": {
            "cell": cell_identity,
            "plan_receipt_sha256": horizon_receipt_sha256,
            "trial_registry_sha256": horizon["trial_registry_sha256"],
            "attempt_roster_sha256": horizon["attempt_roster_sha256"],
        },
    }
    cell_receipt = {**cell_body, "receipt_sha256": _sha(cell_body)}
    return process, cell_receipt, horizon


def test_canary_state_context_admission_reports_defaults_and_variants() -> None:
    process, cell_receipt, horizon = _state_context_admission_inputs()

    summary = adapter._build_canary_state_context_admission_summary(
        process_body=process,
        cell_receipt=cell_receipt,
        horizon_plan_body=horizon,
    )

    assert summary["attempt_count_exact"] == 4
    assert summary["state_context_admitted_attempt_count"] == 2
    assert summary["state_context_shortfall_attempt_count"] == 2
    assert summary["registry_default_strategy_count_exact"] == 3
    assert summary["registry_default_state_context_admitted_count"] == 2
    assert summary["registry_default_state_context_shortfall_count"] == 1
    assert summary["strategy_with_any_state_context_admitted_count"] == 2
    assert summary["strategy_fully_state_context_admitted_count"] == 1
    assert summary["strategy_partially_state_context_admitted_count"] == 1
    assert summary["strategy_fully_state_context_shortfall_count"] == 1
    assert (
        adapter._verified_canary_state_context_admission_summary(
            summary,
            cell_receipt=cell_receipt,
        )
        == summary
    )


def test_canary_state_context_admission_rejects_coherent_status_swap() -> None:
    process, cell_receipt, horizon = _state_context_admission_inputs()
    summary = adapter._build_canary_state_context_admission_summary(
        process_body=process,
        cell_receipt=cell_receipt,
        horizon_plan_body=horizon,
    )
    tampered = copy.deepcopy(summary)
    for ordinal, state_status, execution_status, execution_reason in (
        (
            1,
            "PROCESS_REJECTED",
            "PROCESS_REJECTED",
            "STATE_CONTEXT_SOURCE_BOUNDARY_SHORTFALL",
        ),
        (3, "PLANNED", "PLANNED", None),
    ):
        row = tampered["attempt_classification_roster"][ordinal]
        row["state_context_status"] = state_status
        row["execution_status"] = execution_status
        row["execution_reason_code"] = execution_reason
    tampered["registry_default_state_context_admitted_count"] = 1
    tampered["registry_default_state_context_shortfall_count"] = 2
    tampered["strategy_with_any_state_context_admitted_count"] = 1
    tampered["strategy_fully_state_context_admitted_count"] = 1
    tampered["strategy_partially_state_context_admitted_count"] = 0
    tampered["strategy_fully_state_context_shortfall_count"] = 2
    tampered["attempt_classification_roster_sha256"] = _sha(
        tampered["attempt_classification_roster"]
    )
    tampered_body = dict(tampered)
    tampered_body.pop("receipt_sha256")
    tampered["receipt_sha256"] = _sha(tampered_body)

    with pytest.raises(
        adapter.HistoricalProductionAdapterError,
        match="CANARY_STATE_CONTEXT_CLASSIFICATION_CHANGED",
    ):
        adapter._verified_canary_state_context_admission_summary(
            tampered,
            cell_receipt=cell_receipt,
        )


def test_canary_state_context_admission_rejects_coherent_variant_owner_swap() -> None:
    process, cell_receipt, horizon = _state_context_admission_inputs()
    summary = adapter._build_canary_state_context_admission_summary(
        process_body=process,
        cell_receipt=cell_receipt,
        horizon_plan_body=horizon,
    )
    tampered = copy.deepcopy(summary)
    tampered["horizon_trial_registry"][3]["strategy_id"] = "StrategyB"
    tampered["horizon_attempt_roster"][3]["strategy_id"] = "StrategyB"
    tampered["attempt_classification_roster"][3]["strategy_id"] = "StrategyB"
    tampered["horizon_trial_registry_sha256"] = _sha(tampered["horizon_trial_registry"])
    tampered["horizon_attempt_roster_sha256"] = _sha(tampered["horizon_attempt_roster"])
    tampered["attempt_classification_roster_sha256"] = _sha(
        tampered["attempt_classification_roster"]
    )
    tampered_body = dict(tampered)
    tampered_body.pop("receipt_sha256")
    tampered["receipt_sha256"] = _sha(tampered_body)

    with pytest.raises(
        adapter.HistoricalProductionAdapterError,
        match="CANARY_STATE_CONTEXT_ADMISSION_BINDING_INVALID",
    ):
        adapter._verified_canary_state_context_admission_summary(
            tampered,
            cell_receipt=cell_receipt,
        )


def test_canary_state_context_admission_keeps_source_boundary_separate() -> None:
    process, cell_receipt, horizon = _state_context_admission_inputs(
        candidate_boundary_limited=True
    )

    summary = adapter._build_canary_state_context_admission_summary(
        process_body=process,
        cell_receipt=cell_receipt,
        horizon_plan_body=horizon,
    )

    assert summary["state_context_admitted_attempt_count"] == 2
    assert summary["state_context_shortfall_attempt_count"] == 2
    assert summary["execution_planned_attempt_count"] == 0
    assert summary["execution_state_context_rejected_attempt_count"] == 0
    assert summary["execution_source_boundary_rejected_attempt_count"] == 4
    assert (
        adapter._verified_canary_state_context_admission_summary(
            summary,
            cell_receipt=cell_receipt,
        )
        == summary
    )


def _canary_failure_body(*, schema_version: str) -> dict[str, object]:
    body: dict[str, object] = {
        "schema_version": schema_version,
        "authority_scope": orchestration.BOUNDED_RAW_WINNER_CANARY_SCOPE,
        "request_sha256": "1" * 64,
        "anchor_id": "canary-anchor",
        "symbol": "EURUSD",
        "timeframe": "H1",
        "regime": "TREND",
        "failure_code": "FROZEN_PATH_COST_LADDER_FAILED",
        "attempted_trials_exact": 128,
        "frozen_receipt_sha256": "2" * 64,
        "search_outcome": None,
        "economic_no_trade_or_no_winner": False,
        "downstream_authority": False,
    }
    if schema_version == adapter.BOUNDED_RAW_WINNER_CANARY_FAILURE_SCHEMA:
        body.update(
            {
                "exception_type": "FrozenPathCostLadderError",
                "failure_details": [
                    "LADDER_TRADE_PNL_IMPROVEMENT",
                    "UNIFORM_ALL_FILLS_SINGLE_FILL_UPLIFT_P75",
                    "genuine ladder evidence is required",
                ],
                "attempt_ordinal": 0,
                "fold_ordinal": 0,
            }
        )
    return body


def _verify_canary_failure_body(body: dict[str, object]) -> dict[str, object]:
    return adapter._verified_canary_failure_body(
        body,
        request_sha256="1" * 64,
        anchor_id="canary-anchor",
        symbol="EURUSD",
        timeframe="H1",
        regime="TREND",
        failure_code="FROZEN_PATH_COST_LADDER_FAILED",
        attempted_trials_exact=128,
        frozen_receipt_sha256="2" * 64,
    )


def test_canary_failure_v2_seals_exact_bounded_diagnostic() -> None:
    body = _canary_failure_body(
        schema_version=adapter.BOUNDED_RAW_WINNER_CANARY_FAILURE_SCHEMA
    )

    reopened = _verify_canary_failure_body(body)

    assert reopened == body
    assert reopened["exception_type"] == "FrozenPathCostLadderError"
    assert reopened["attempt_ordinal"] == 0
    assert reopened["fold_ordinal"] == 0
    assert reopened["failure_details"] == body["failure_details"]

    with_extra_field = {**body, "unsealed_diagnostic": "forged"}
    with pytest.raises(
        adapter.HistoricalProductionAdapterError,
        match="CANARY_TECHNICAL_FAILURE_FIELDS_INVALID",
    ):
        _verify_canary_failure_body(with_extra_field)


def test_canary_failure_v1_artifact_remains_verifiable() -> None:
    body = _canary_failure_body(
        schema_version="fxpm.bounded-raw-winner-canary-failure.v1"
    )

    assert _verify_canary_failure_body(body) == body

    with_v2_field = {**body, "exception_type": "CoherentReseal"}
    with pytest.raises(
        adapter.HistoricalProductionAdapterError,
        match="CANARY_TECHNICAL_FAILURE_FIELDS_INVALID",
    ):
        _verify_canary_failure_body(with_v2_field)


@pytest.mark.parametrize(
    ("overrides", "expected_code"),
    [
        (
            {
                "failure_details": ["detail"]
                * (adapter.BOUNDED_RAW_WINNER_CANARY_FAILURE_DETAIL_LIMIT + 1)
            },
            "CANARY_FAILURE_DETAILS_INVALID",
        ),
        (
            {
                "failure_details": [
                    "x" * (adapter.BOUNDED_RAW_WINNER_CANARY_FAILURE_DETAIL_BYTES + 1)
                ]
            },
            "CANARY_FAILURE_DETAILS_INVALID",
        ),
        (
            {
                "exception_type": "X"
                * (adapter.BOUNDED_RAW_WINNER_CANARY_EXCEPTION_TYPE_BYTES + 1)
            },
            "CANARY_FAILURE_EXCEPTION_TYPE_INVALID",
        ),
        # Out of bounds is one past the last valid ordinal, whatever the bound
        # is. Hardcoding 128 tied this to a payload limit rather than to the
        # contract it is actually asserting.
        (
            {"attempt_ordinal": winner.MAX_ATTEMPTS},
            "CANARY_FAILURE_ATTEMPT_ORDINAL_INVALID",
        ),
        (
            {"attempt_ordinal": None, "fold_ordinal": 0},
            "CANARY_FAILURE_FOLD_ORDINAL_INVALID",
        ),
        (
            {"attempt_ordinal": 0, "fold_ordinal": 12},
            "CANARY_FAILURE_FOLD_ORDINAL_INVALID",
        ),
    ],
)
def test_canary_failure_diagnostic_rejects_unbounded_or_unbound_values(
    overrides: dict[str, object],
    expected_code: str,
) -> None:
    diagnostic = {
        "exception_type": "FrozenPathCostLadderError",
        "failure_details": ["LADDER_TRADE_PNL_IMPROVEMENT"],
        "attempt_ordinal": 0,
        "fold_ordinal": 0,
        **overrides,
    }

    with pytest.raises(adapter.HistoricalProductionAdapterError, match=expected_code):
        adapter._bounded_canary_failure_diagnostic(
            **diagnostic,
            attempted_trials_exact=128,
        )


_CURRENT_NATIVE_CREATION_RECORD = "synthetic_fixture_creation.json"


def _current_native_generator_inputs(*, selected_owner=False):
    cutoff = pd.Timestamp("2024-07-14T08:05:00Z")
    dense_end = cutoff.normalize()
    inputs = {
        "write_v5_run": {
            "run_id": "synthetic-current-native-run", "corrupt_timeframe": None,
            "context_only": False, "price_offset": 0.0, "production_grid": True,
            "information_cutoff_utc": cutoff.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "dense_symbol": OBSERVED_SYMBOL,
            "dense_band_utc": [(dense_end - pd.Timedelta(days=1500)).strftime("%Y-%m-%dT%H:%M:%SZ"),
                               dense_end.strftime("%Y-%m-%dT%H:%M:%SZ")],
            "oscillation_period_days": 30.0, "spread_points": 12,
            "instrument_snapshot": {"status": "available", "symbol": OBSERVED_SYMBOL,
                "asset_class": "fx", "digits": 5, "point": 0.00001,
                "path": f"Forex\\Main\\{OBSERVED_SYMBOL}",
                "trade_contract_size": 100_000.0, "trade_tick_size": 0.00001,
                "trade_tick_value": 1.0, "parity_tick_tolerance": 0.00001},
        },
        "activate_v5_run": {
            "activation_id": "synthetic-current-native-activation",
            "production_only_pointer": True, "now_utc": "2024-08-01T00:00:00Z",
        },
        "activate_broker_terms": {
            "activation_id": "synthetic-native-candidate-terms", "now_utc": "2024-08-02T00:00:00Z",
            "snapshot_fixture": "tests.test_broker_terms._full_receipt_terms_snapshot",
            "financing_policy": "BUILD_EXPLICIT_STRESS_SNAPSHOT",
            "d1_anchor_policy": "FIRST_ACTUAL_NATIVE_D1_OPEN_AND_SERVER_WEEKDAY",
        },
    }
    if selected_owner:
        inputs["write_v5_run"].update({
            "run_id": "synthetic-selected-owner-native-run",
            "information_cutoff_utc": "2026-07-14T08:05:00Z",
            "dense_band_utc": ["2020-10-23T08:05:00Z", "2024-10-12T08:05:00Z"],
            "oscillation_period_days": 2.0,
            "spread_points": 0,
            "spread_cycle_points": [8] * 10 + [12] * 7 + [20] * 3,
            "canonical_broker_symbols": True,
        })
        inputs["activate_v5_run"].update({
            "activation_id": "synthetic-selected-owner-native-activation",
            "now_utc": "OBSERVED_RUNTIME_CLOCK",
        })
        inputs["activate_broker_terms"].update({
            "activation_id": "synthetic-selected-owner-native-terms",
            "now_utc": "OBSERVED_RUNTIME_CLOCK",
        })
    return inputs


def _validated_current_native_creation(record, *, native_root, helper_sha256, generator_inputs):
    if (not isinstance(record, dict)
        or record.get("schema_version") != "fxpm.synthetic-native-fixture-creation.v1"
        or record.get("fixture_provenance") != "SYNTHETIC_NATIVE_ENGINEERING_INPUT_NO_MARKET_OR_TRADING_AUTHORITY"
        or record.get("qualification_authority") is not False
        or record.get("native_root") != str(native_root)):
        raise ValueError("RETAINED_NATIVE_CREATION_RECORD_INVALID")
    if record.get("status") != "GENERATION_AUDIT_AND_TERMS_COMPLETE":
        raise ValueError("RETAINED_NATIVE_CREATION_INCOMPLETE")
    if record.get("helper_sha256") != helper_sha256:
        raise ValueError("RETAINED_NATIVE_GENERATOR_IDENTITY_CHANGED")
    if _canonical(record.get("generator_inputs")) != _canonical(generator_inputs):
        raise ValueError("RETAINED_NATIVE_GENERATOR_INPUTS_CHANGED")
    return record


@pytest.mark.parametrize("change,expected", [
    ("helper", "RETAINED_NATIVE_GENERATOR_IDENTITY_CHANGED"),
    ("inputs", "RETAINED_NATIVE_GENERATOR_INPUTS_CHANGED"),
    ("incomplete", "RETAINED_NATIVE_CREATION_INCOMPLETE"),
])
def test_retained_native_creation_refuses_changed_generation(tmp_path, change, expected):
    inputs = _current_native_generator_inputs()
    record = {"schema_version": "fxpm.synthetic-native-fixture-creation.v1",
        "fixture_provenance": "SYNTHETIC_NATIVE_ENGINEERING_INPUT_NO_MARKET_OR_TRADING_AUTHORITY",
        "qualification_authority": False, "native_root": str(tmp_path), "helper_sha256": "a" * 64,
        "generator_inputs": copy.deepcopy(inputs), "status": "GENERATION_AUDIT_AND_TERMS_COMPLETE"}
    arguments = {"native_root": tmp_path, "helper_sha256": "a" * 64, "generator_inputs": inputs}
    assert _validated_current_native_creation(record, **arguments) is record
    if change == "helper":
        arguments["helper_sha256"] = "b" * 64
    elif change == "inputs":
        arguments["generator_inputs"] = copy.deepcopy(inputs)
        arguments["generator_inputs"]["write_v5_run"]["oscillation_period_days"] = 31.0
    else:
        record["status"] = "STARTED"
    with pytest.raises(ValueError, match=expected):
        _validated_current_native_creation(record, **arguments)


@pytest.mark.parametrize("canonical", [False, True])
def test_synthetic_native_page_diagnostics_bind_declared_broker_symbol(canonical):
    from tests import native_v5_support as helper
    from pm_native_request_contract import _mt5_v4_diagnostics_errors

    start = pd.Timestamp("2022-01-01T00:00Z")
    seconds = helper.TIMEFRAME_SECONDS["H1"]
    slots = min(max(1, helper.DEFAULT_PAGE_DAYS["H1"] * 86400 // seconds),
                int(helper.SOURCE_ENVIRONMENT["terminal"]["max_bars"] * 0.90))
    end = start + pd.Timedelta(seconds=3 * slots * seconds)
    frame = pd.DataFrame({"Open": [1.0, 1.0]}, index=pd.DatetimeIndex([
        start + pd.Timedelta(hours=2), end - pd.Timedelta(hours=5)]))
    _, pagination = helper._pagination(OBSERVED_SYMBOL, "H1", {
        "source_request_interval": {"start_utc": start.isoformat(), "end_exclusive_utc": end.isoformat()}},
        frame, canonical_broker_symbols=canonical)
    assert pagination["empty_pages"] and pagination["pages_with_data"]
    broker_symbol = OBSERVED_SYMBOL if canonical else f"{OBSERVED_SYMBOL}.a"
    for page in pagination["page_attempts"]:
        diagnostics = page["connector_diagnostics"]
        assert diagnostics["broker_symbol"] == broker_symbol
        assert _mt5_v4_diagnostics_errors(diagnostics,
            interval=(pd.Timestamp(page["start_utc"]).to_pydatetime(),
                      pd.Timestamp(page["end_exclusive_utc"]).to_pydatetime()),
            expected_symbol=OBSERVED_SYMBOL, expected_broker_symbol=broker_symbol, timeframe="H1",
            copy_attempts=page["copy_rates_range_attempts"],
            category="data" if diagnostics["admitted_count"] else "empty") == []
    assert all(page["connector_diagnostics"]["broker_symbol"] == broker_symbol
               for page in pagination["empty_pages"])


def _selected_owner_bound_inputs(root, *, retained_native_root, selected_owner,
                                generation_context, frozen_config, selection_cutoff_utc):
    supplied = (generation_context, frozen_config, selection_cutoff_utc)
    if all(value is None for value in supplied):
        return False
    if any(value is None for value in supplied):
        raise ValueError("SELECTED_OWNER_BOUND_INPUTS_REQUIRED_TOGETHER")
    if not selected_owner or retained_native_root is None:
        raise ValueError("SELECTED_OWNER_BOUND_INPUTS_REQUIRE_RETAINED_SELECTED_SOURCE")
    from pm_production_config_binding import FrozenProductionPipelineConfig

    if (type(generation_context) is not spool.GenerationSpoolContext
            or type(frozen_config) is not FrozenProductionPipelineConfig):
        raise ValueError("SELECTED_OWNER_BOUND_INPUT_TYPES_INVALID")
    if not generation_context.root.resolve().is_relative_to(Path(root).resolve()):
        raise ValueError("SELECTED_OWNER_BOUND_CONTEXT_OUTSIDE_FRESH_CASE")
    initial_files = {spool.CONTEXT_RELATIVE_PATH,
        spool.spool_quota.QUOTA_STATE_NAME, spool.spool_quota.QUOTA_LOCK_NAME}
    if {path.relative_to(generation_context.root).as_posix()
            for path in generation_context.root.rglob("*") if path.is_file()} != initial_files:
        raise ValueError("SELECTED_OWNER_BOUND_CONTEXT_NOT_FRESH")
    quota = spool.spool_quota._read_state(generation_context.root)
    if (quota is None or quota["status"] != "CLEAN" or quota["reservations"]
            or quota["committed_bytes"] != (generation_context.root / spool.CONTEXT_RELATIVE_PATH).stat().st_size
            or {path.relative_to(generation_context.root).as_posix()
                for path in generation_context.root.rglob("*") if path.is_dir()} != {"tmp"}):
        raise ValueError("SELECTED_OWNER_BOUND_CONTEXT_NOT_FRESH")
    cutoff = pd.Timestamp(selection_cutoff_utc)
    if (cutoff.tzinfo is None or cutoff > pd.Timestamp("2024-07-14T08:05:00Z")):
        raise ValueError("SELECTED_OWNER_BOUND_CUTOFF_INVALID")
    frozen_config.verify_current(generation_context)
    return True


def _current_v5_native_prefixes(root, *, retained_native_root=None, selected_owner=False,
                              generation_context=None, frozen_config=None, selection_cutoff_utc=None):
    """Invented source bars pass the real V5 audit and public terms activation."""
    import pm_broker_terms
    from tests.native_v5_support import (
        SOURCE_ENVIRONMENT, activate_v5_run, write_v5_run,
    )
    from tests.test_broker_terms import _full_receipt_terms_snapshot

    root = Path(root)
    injected = _selected_owner_bound_inputs(root, retained_native_root=retained_native_root,
        selected_owner=selected_owner, generation_context=generation_context,
        frozen_config=frozen_config, selection_cutoff_utc=selection_cutoff_utc)
    root.mkdir(parents=True, exist_ok=True)
    generator_inputs = _current_native_generator_inputs(selected_owner=selected_owner)
    writer_inputs = generator_inputs["write_v5_run"]
    acquisition_asof = pd.Timestamp(writer_inputs["information_cutoff_utc"])
    if injected:
        cutoff = pd.Timestamp(selection_cutoff_utc)
    else:
        cutoff = pd.Timestamp("2024-07-14T08:05:00Z") if selected_owner else acquisition_asof
    target_timeframe = "H1" if selected_owner else "D1"
    run_id = writer_inputs["run_id"]
    helper_sha256 = hashlib.sha256(Path(__file__).with_name("native_v5_support.py").read_bytes()).hexdigest()
    if retained_native_root is None:
        native_root = (root / "data" / "native").resolve()
        creation_path = native_root / _CURRENT_NATIVE_CREATION_RECORD
        if creation_path.exists():
            raise ValueError("SYNTHETIC_NATIVE_CREATION_RECORD_ALREADY_EXISTS")
        if (native_root / "runs" / run_id).exists():
            raise ValueError("SYNTHETIC_NATIVE_SOURCE_ALREADY_EXISTS_NO_CREATION_BACKFILL")
        native_root.mkdir(parents=True, exist_ok=True)
        creation = {"schema_version": "fxpm.synthetic-native-fixture-creation.v1",
            "fixture_provenance": "SYNTHETIC_NATIVE_ENGINEERING_INPUT_NO_MARKET_OR_TRADING_AUTHORITY",
            "qualification_authority": False, "native_root": str(native_root),
            "helper_sha256": helper_sha256, "generator_inputs": copy.deepcopy(generator_inputs),
            "status": "STARTED", "started_at_utc": datetime.now(timezone.utc).isoformat(),
            "completed_at_utc": None}
        creation_path.write_bytes(_canonical(creation))
        write_v5_run(native_root, **copy.deepcopy(writer_inputs))
        activation_inputs = dict(generator_inputs["activate_v5_run"])
        activation_inputs["now_utc"] = (datetime.now(timezone.utc) if selected_owner
            else datetime.fromisoformat(activation_inputs["now_utc"]))
        if selected_owner:
            creation["observed_native_activation_at_utc"] = activation_inputs["now_utc"].isoformat()
            creation_path.write_bytes(_canonical(creation))
        activate_v5_run(native_root, run_id, **activation_inputs)

        terms_inputs = generator_inputs["activate_broker_terms"]
        now = datetime.now(timezone.utc) if selected_owner else datetime.fromisoformat(terms_inputs["now_utc"])
        if selected_owner:
            creation["observed_terms_snapshot_at_utc"] = now.isoformat()
            creation_path.write_bytes(_canonical(creation))
        snapshot = _full_receipt_terms_snapshot(now)
        snapshot["broker_identity"] = {
            "broker_server": SOURCE_ENVIRONMENT["account"]["server"],
            "terminal_company": SOURCE_ENVIRONMENT["terminal"]["company"],
            "account_currency": SOURCE_ENVIRONMENT["account"]["currency"],
            "terminal_build": SOURCE_ENVIRONMENT["terminal"]["build"]}
        for symbol in book.PINNED_SYMBOL_ORDER:
            first = pd.Timestamp(pd.read_csv(native_root / "runs" / run_id / symbol / "D1" / "bars.csv",
                usecols=["time"], nrows=1)["time"].iloc[0])
            snapshot["symbols"][symbol.upper()]["d1_anchors"] = [{
                "open_epoch": int(first.timestamp()), "server_weekday": (first.dayofweek + 1) % 7}]
        snapshot.pop("content_sha256")
        snapshot["content_sha256"] = pm_broker_terms.canonical_sha256(snapshot)
        stressed = pm_broker_terms.build_explicit_stress_snapshot(snapshot)
        activation_now = datetime.now(timezone.utc) if selected_owner else now
        if selected_owner:
            creation["observed_terms_activation_at_utc"] = activation_now.isoformat()
            creation_path.write_bytes(_canonical(creation))
        pm_broker_terms.activate_broker_terms(native_root, stressed,
            activation_id=terms_inputs["activation_id"], now_utc=activation_now)
        creation.update({"status": "GENERATION_AUDIT_AND_TERMS_COMPLETE",
            "completed_at_utc": datetime.now(timezone.utc).isoformat()})
        creation_path.write_bytes(_canonical(creation))
    else:
        native_root = Path(retained_native_root).resolve(strict=True)
        approved = (Path(__file__).resolve().parents[1] / "audit_reports" / "fxpm_workshop_20261007"
            / ("selected_owner_fixture_verification/runtime" if selected_owner
               else "frozen_tier_candidate_verification")).resolve(strict=True)
        relative = native_root.relative_to(approved) if native_root.is_relative_to(approved) else Path()
        parts = relative.parts
        case_prefix = "selected-owner-candidate" if selected_owner else "current-native-candidate"
        if (len(parts) != 5 or not parts[0].startswith("attempt_")
            or not parts[0].endswith("_pytest_tmp") or not parts[0][8:-11].isdecimal()
            or not parts[1].startswith(case_prefix)
            or not parts[1][len(case_prefix):].isdecimal() or parts[2:] != ("source", "data", "native")):
            raise ValueError("RETAINED_NATIVE_SOURCE_NOT_EXPLICIT_QA90_ROOT" if selected_owner
                             else "RETAINED_NATIVE_SOURCE_NOT_EXPLICIT_QA86_ROOT")
        creation_path = native_root / _CURRENT_NATIVE_CREATION_RECORD
        if not creation_path.is_file():
            raise ValueError("RETAINED_NATIVE_CREATION_RECORD_REQUIRED")
        _validated_current_native_creation(json.loads(creation_path.read_text(encoding="utf-8")),
            native_root=native_root, helper_sha256=helper_sha256, generator_inputs=generator_inputs)
        retained_run = json.loads((native_root / "runs" / run_id / "run_manifest.json").read_text(encoding="utf-8"))
        assert retained_run["fixture_provenance"] == (
            "SYNTHETIC_NATIVE_ENGINEERING_INPUT_NO_MARKET_OR_TRADING_AUTHORITY")
        assert pd.Timestamp(retained_run["request"]["requested_information_cutoff_utc"]) == acquisition_asof
    resolved = pm_broker_terms.resolve_active_swap_contract(native_root, OBSERVED_SYMBOL, target_timeframe)
    assert resolved["schedule_provenance_status"] == pm_broker_terms.STRESS_APPROVED
    if injected:
        context = generation_context
    else:
        context = native_prefix_fixture._context(root)
    products = {}
    accessed_at = _observed_utc_after() if selected_owner else "2025-01-01T00:00:00Z"
    for timeframe in (target_timeframe, "M5"):
        products[timeframe] = native_prefix.produce_activated_native_prefix(
            data_dir=native_root.parent, context=context, symbol=OBSERVED_SYMBOL,
            timeframe=timeframe, cutoff_utc=cutoff.strftime("%Y-%m-%dT%H:%M:%SZ"),
            source_accessed_at_utc=accessed_at, production_contract_required=True)
        assert products[timeframe].source_state == native_prefix.AVAILABLE, (
            _read_descriptor_json(context, products[timeframe].index["terminal_receipt"]))
        if injected:
            assert pd.Timestamp(products[timeframe].native_prefix["prefix_end_exclusive_utc"]) == cutoff
    assert resolved["target_bars_sha256"] == products[target_timeframe].index["source"]["bars_sha256"]
    return products[target_timeframe], products["M5"]


def _make_observed_native_prefix(root):
    """Invented native bars, independently sealed by the canonical verifier."""
    root = Path(root)
    root.mkdir(parents=True, exist_ok=True)
    data_dir = root / "data"
    symbol = OBSERVED_SYMBOL
    row_count = 500
    # Pass the symbol explicitly. _write_activation declares
    # `symbol: str = SYMBOL`, and Python binds that default once at definition
    # time, so reassigning the module global here never reached it: the helper
    # wrote runs/run-prefix/EURUSD/... while this fixture read
    # runs/run-prefix/AUDUSD/..., and every observed-cell test errored at setup.
    facts = native_prefix_fixture._write_activation(
        data_dir, OBSERVED_TIMEFRAME, symbol=symbol
    )

    start = datetime(2018, 1, 1, tzinfo=timezone.utc)
    cadence = TIMEFRAME_SECONDS[OBSERVED_TIMEFRAME]
    rows = []
    for ordinal in range(row_count):
        opened = start + timedelta(seconds=ordinal * cadence)
        closed = opened + timedelta(seconds=cadence)
        price = 1.1 + ordinal * 0.00001
        rows.append(
            [
                opened.strftime("%Y-%m-%dT%H:%M:%SZ"),
                f"{price:.6f}",
                f"{price + 0.002:.6f}",
                f"{price - 0.002:.6f}",
                f"{price + 0.001:.6f}",
                "100",
                "12",
                "80",
                opened.strftime("%Y-%m-%dT%H:%M:%SZ"),
                opened.strftime("%Y-%m-%dT%H:%M:%SZ"),
                closed.strftime("%Y-%m-%dT%H:%M:%SZ"),
            ]
        )
    bars_path = Path(facts["bars_path"])
    with bars_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle, lineterminator="\n")
        writer.writerow(("time", *PRODUCTION_ARTIFACT_COLUMNS))
        writer.writerows(rows)
    bars_raw = bars_path.read_bytes()
    bars_sha256 = hashlib.sha256(bars_raw).hexdigest()
    cutoff = start + timedelta(seconds=row_count * cadence)
    cutoff_utc = cutoff.strftime("%Y-%m-%dT%H:%M:%SZ")

    native_root = data_dir / "native"
    provenance_path = (
        native_root
        / "runs"
        / "run-prefix"
        / symbol
        / OBSERVED_TIMEFRAME
        / "provenance.json"
    )
    provenance = json.loads(provenance_path.read_text(encoding="utf-8"))
    provenance["artifact"].update(
        {
            "sha256": bars_sha256,
            "bytes": len(bars_raw),
            "rows": row_count,
        }
    )
    provenance.update(
        {
            "sha256": bars_sha256,
            "rows": row_count,
            "start_ts": rows[0][0],
            "end_ts": rows[-1][0],
        }
    )
    provenance["request"]["source_request_interval"] = {
        "start_utc": rows[0][0],
        "end_exclusive_utc": cutoff_utc,
    }
    provenance_sha256 = native_prefix_fixture._write_json(provenance_path, provenance)

    sidecar_path = (
        native_root
        / "audits"
        / native_prefix_fixture.ACTIVATION_ID
        / symbol
        / OBSERVED_TIMEFRAME
        / "sidecar.json"
    )
    sidecar = json.loads(sidecar_path.read_text(encoding="utf-8"))
    sidecar["source_provenance_sha256"] = provenance_sha256
    sidecar_sha256 = native_prefix_fixture._write_json(sidecar_path, sidecar)

    run_manifest_path = native_root / "runs" / "run-prefix" / "run_manifest.json"
    run_manifest = json.loads(run_manifest_path.read_text(encoding="utf-8"))
    run_manifest["results"][0].update({"rows": row_count, "sha256": bars_sha256})
    run_manifest_sha256 = native_prefix_fixture._write_json(
        run_manifest_path, run_manifest
    )

    manifest_path = (
        native_root / "activations" / f"{native_prefix_fixture.ACTIVATION_ID}.json"
    )
    activation = json.loads(manifest_path.read_text(encoding="utf-8"))
    activation["run_manifest_sha256"] = run_manifest_sha256
    activation_entry = activation["entries"][symbol][OBSERVED_TIMEFRAME]
    activation_entry.update(
        {
            "bars_sha256": bars_sha256,
            "sidecar_sha256": sidecar_sha256,
        }
    )
    manifest_sha256 = native_prefix_fixture._write_json(manifest_path, activation)
    pointer_path = native_root / "ACTIVE.json"
    pointer = json.loads(pointer_path.read_text(encoding="utf-8"))
    pointer["manifest_sha256"] = manifest_sha256
    native_prefix_fixture._write_json(pointer_path, pointer)

    context = native_prefix_fixture._context(root)
    product = native_prefix.produce_activated_native_prefix(
        data_dir=data_dir,
        context=context,
        symbol=symbol,
        timeframe=OBSERVED_TIMEFRAME,
        cutoff_utc=cutoff_utc,
        source_accessed_at_utc="2020-01-01T00:00:00Z",
        production_contract_required=False,
    )
    assert product.source_state == native_prefix.AVAILABLE, _read_descriptor_json(
        context, product.index["terminal_receipt"]
    )
    frame = native_prefix.open_verified_prefix_frame(product)
    assert len(frame) == row_count
    assert product.native_prefix["prefix_rows"] == row_count
    return product



@pytest.fixture(scope="module")
def observed_native_prefix(tmp_path_factory):
    return _make_observed_native_prefix(tmp_path_factory.mktemp("historical-adapter-native-prefix"))


def _observed_utc_after(previous=None):
    """Observe a distinct second because raw contracts discard microseconds."""
    boundary = None if previous is None else datetime.fromisoformat(previous.replace("Z", "+00:00"))
    while True:
        now = datetime.now(timezone.utc).replace(microsecond=0)
        if boundary is None or now > boundary:
            return now.isoformat().replace("+00:00", "Z")
        time.sleep(0.05)


def _install_selected_owner_profile(product, *, retained_native_root):
    from tools.build_spread_profile import build_native_profile

    native_root = product._data_dir / "native"
    creation_path = native_root / _CURRENT_NATIVE_CREATION_RECORD
    creation = json.loads(creation_path.read_text(encoding="utf-8"))
    profile_path = native_root / "terms" / "spread_profile.json"
    if retained_native_root is None:
        assert not profile_path.exists(), "SYNTHETIC_PROFILE_ALREADY_EXISTS"
        body = build_native_profile(product._data_dir, cutoff=product.index["cutoff_utc"])
        body["fixture_provenance"] = "SYNTHETIC_SELECTED_OWNER_ENGINEERING_ONLY_NO_TRADING_AUTHORITY"
        encoded = _canonical(body)
        profile_path.parent.mkdir(parents=True, exist_ok=True)
        profile_path.write_bytes(encoded)
        creation["frozen_profile"] = {
            "relative_path": "terms/spread_profile.json",
            "sha256": hashlib.sha256(encoded).hexdigest(),
            "bytes": len(encoded),
            "created_at_utc": body["created_at_utc"],
            "builder": "tools.build_spread_profile.build_native_profile",
        }
        creation["declared_forward_source"] = {
            "start_utc": "2024-07-14T08:05:00Z",
            "end_exclusive_utc": "2024-10-12T08:05:00Z",
            "purpose": "DECLARED_SYNTHETIC_SOURCE_ONLY_NO_FORWARD_REPLAY_OR_ISSUANCE",
        }
        creation_path.write_bytes(_canonical(creation))
    else:
        encoded = profile_path.read_bytes()
        assert hashlib.sha256(encoded).hexdigest() == creation["frozen_profile"]["sha256"]
        assert len(encoded) == creation["frozen_profile"]["bytes"]
        body = json.loads(encoded)
    assert body["fixture_provenance"] == "SYNTHETIC_SELECTED_OWNER_ENGINEERING_ONLY_NO_TRADING_AUTHORITY"
    assert body["production_authority"] is False and len(body["cells"]) == 600
    target = next(cell for cell in body["cells"]
                  if cell["symbol"] == OBSERVED_SYMBOL and cell["timeframe"] == "H1")
    assert target["status"] == "CALCULATED" and target["source_timeframe"] == "M5"
    assert {tier: target["tiers"][tier]["spread_points"] for tier in ("P75", "P95")} == {"P75": 12, "P95": 20}
    return creation["frozen_profile"]


def build_current_native_case(tmp_path, *, retained_native_root=None, selected_owner=False,
                             generation_context=None, frozen_config=None, selection_cutoff_utc=None,
                             winner_generation=winner.CURRENT_GENERATION):
    """UNISSUED synthetic engineering carrier, never production authority or profitability."""
    if winner_generation is not winner.CURRENT_GENERATION and winner_generation is not winner.V16_GENERATION:
        raise ValueError("SYNTHETIC_WINNER_GENERATION_UNSUPPORTED")
    if selected_owner and (retained_native_root is None
            or (isinstance(retained_native_root, str) and not retained_native_root.strip())):
        raise ValueError("SELECTED_OWNER_CONTROL_REQUIRES_RETAINED_SOURCE")
    import pm_spread_profile

    injected = _selected_owner_bound_inputs(Path(tmp_path) / "source",
        retained_native_root=retained_native_root, selected_owner=selected_owner,
        generation_context=generation_context, frozen_config=frozen_config,
        selection_cutoff_utc=selection_cutoff_utc)
    product, fine_product = _current_v5_native_prefixes(Path(tmp_path) / "source",
        retained_native_root=retained_native_root, selected_owner=selected_owner,
        generation_context=generation_context, frozen_config=frozen_config,
        selection_cutoff_utc=selection_cutoff_utc)
    timeframe = "H1" if selected_owner else "D1"
    descriptor = dict(product.prefix_descriptor)
    relative = Path(descriptor["relative_path"])
    root = product.prefix_path.parents[len(relative.parts) - 1]
    bound_details = {}
    if injected:
        context = spool.reopen_generation_context(root,
            expected_generation_id=generation_context.generation_id,
            expected_native_universe_sha256=generation_context.native_universe_sha256,
            expected_config_sha256=generation_context.config_sha256)
        assert context.root.resolve() == generation_context.root.resolve()
        assert context.context_sha256 == generation_context.context_sha256
        config = frozen_config.materialize(context)
        assert Path(config._deployment_config_path).resolve() == frozen_config.active_config_path
        assert Path(config.data_dir).resolve() == Path(retained_native_root).resolve().parent
        broker_path = Path(frozen_config.payload()["broker_specs_path"])
        if not broker_path.is_absolute():
            broker_path = frozen_config.active_config_path.parent / broker_path
        broker_path = broker_path.resolve(strict=True)
        bound_details = {"frozen_pipeline_config": frozen_config,
            "effective_config_sha256": frozen_config.effective_config_sha256,
            "selection_config_snapshot": asdict(config), "bound_broker_path": broker_path,
            "bound_config_bytes_sha256": hashlib.sha256(frozen_config.active_config_path.read_bytes()).hexdigest(),
            "bound_broker_sha256": hashlib.sha256(broker_path.read_bytes()).hexdigest(),
            "selection_cutoff_utc": pd.Timestamp(selection_cutoff_utc).isoformat()}
    else:
        context = spool.reopen_generation_context(root)
        config_path = native_prefix_fixture._execution_config(Path(tmp_path) / "execution", symbol=OBSERVED_SYMBOL)
        config = PipelineConfig(research_mode=True, data_dir=str(product._data_dir), backtest_spread_mode="conservative_max")
        config._deployment_config_path = str(config_path)
    spec, _ = search_execution._freeze_bound_execution_spec(product=product, config=config)
    frozen_profile = None
    if selected_owner:
        frozen_profile = _install_selected_owner_profile(product, retained_native_root=retained_native_root)
    else:
        source = fine_product.index["source"]
        body = {"schema_version": pm_spread_profile.PROFILE_SCHEMA,
            "status": "COMPLETE_CALIBRATION_WITH_EXPLICIT_UNAVAILABLE_GROUPS",
            "calculation_policy": "ONE_TIME_OFFLINE_NO_RUNTIME_RECALCULATION",
            "source_convention": "FINEST_ACTIVATED_CANDIDATE_FIXED_PER_SYMBOL_EXACT_PRIOR_CLOSE_AT_TARGET_OPEN",
            "production_authority": False, "fixture_provenance": "UNISSUED_SYNTHETIC_NATIVE_CANDIDATE_ENGINEERING_ONLY",
            "activation": {"id": product.index["activation"]["activation_id"],
                           "manifest_sha256": product.index["activation"]["manifest_sha256"]},
            "protected_cutoff_utc": product.index["cutoff_utc"],
            "cells": [{"symbol": OBSERVED_SYMBOL, "timeframe": "D1", "source_timeframe": "M5",
                "status": "CALCULATED", "point_price": spec.point,
                "source_identity": {"identity_sha256": source["source_identity_sha256"]},
                "source_binding": {"bars_sha256": source["bars_sha256"], "sidecar_sha256": source["audit_sidecar_sha256"],
                    "provenance_sha256": source["provenance_sha256"],
                    "target_bars_sha256": product.index["source"]["bars_sha256"]},
                "tiers": {tier: {"usability": "POSITIVE_QUANTILE", "spread_points": points,
                    "full_spread_price": points * spec.point, "half_spread_price": points * spec.point / 2}
                    for tier, points in (("P75", 9), ("P95", 16))}}]}
        profile_path = product._data_dir / "native" / "terms" / "spread_profile.json"
        if retained_native_root is None:
            profile_path.parent.mkdir(parents=True, exist_ok=True)
            profile_path.write_bytes(_canonical(body))
        else:
            assert profile_path.read_bytes() == _canonical(body), "RETAINED_SPREAD_PROFILE_MISMATCH"
    fitted = search_execution.fit_native_timeframe_for_search(product=product, config=config)
    prefix_end = pd.Timestamp(product.native_prefix["prefix_end_exclusive_utc"])
    if selected_owner:
        if injected:
            assert fitted.train_regime_support["counts"]["TREND"] > 0
            bound_details["derived_train_regime_support"] = copy.deepcopy(fitted.train_regime_support)
        else:
            assert fitted.train_regime_support["receipt_sha256"] == (
                "780a80df065390eeb6681dd9aa27a054f5f0e2132ca36547f8f62e21e4554fbb")
        regime = "TREND"
        strategy_ids = ("EMACrossoverStrategy", "VolumeSpikeMomentumStrategy")
        attempt_artifacts = tuple(_attempt_artifact(OBSERVED_SYMBOL, timeframe,
            regime, native_prefix_product=product, strategy_id=identity,
            lookback_value=500) for identity in strategy_ids)
    else:
        # This fixed synthetic owner is input to the engineering case, not an outcome-selected strategy.
        strategy_id = "EMACrossoverStrategy"
        features = search_execution._build_full_prefix_strategy_surface(frame=fitted._frame,
            strategy=StrategyRegistry.get(strategy_id), symbol=OBSERVED_SYMBOL, timeframe="D1")
        labels = features.features["REGIME_LIVE"]
        signal_labels = labels.loc[features.raw_signals.shift(1).fillna(0).ne(0)].value_counts()
        regime = str(signal_labels.index[0])
        prefix_end = pd.Timestamp(product.native_prefix["prefix_end_exclusive_utc"])
        lookback = 500
        start = prefix_end - pd.Timedelta(days=lookback)
        built = current_refit.build_current_refit_search_artifacts_v2(symbol=OBSERVED_SYMBOL, timeframe="D1",
            regime=regime, strategy_id=strategy_id, parameters=StrategyRegistry.get(strategy_id).get_params(),
            lookback_kind="CALENDAR_INTERVAL", lookback_value=lookback, fit_start_utc=native_prefix_fixture._iso(start),
            fit_end_utc=native_prefix_fixture._iso(prefix_end), latest_closed_bar_utc=product.native_prefix["prefix_last_close_utc"],
            source_snapshot_sha256=product.native_prefix["prefix_content_sha256"], source_bar_count=lookback, seed=7)
        artifacts = {name: getattr(built, name) for name in ("winner_process", "horizon_process", "trial_definition", "refit_recipe")}
        strategy_ids = (strategy_id,)
        attempt_artifacts = (artifacts,)
    request = horizon_fixture._request()
    request["schema_version"] = "fxpm.historical-horizon-request.v3"
    run_id = "unissued-selected-owner-flat-native-control" if selected_owner else "unissued-synthetic-native"
    anchor_id = "unissued-selected-owner-flat-anchor-control" if selected_owner else "unissued-native-anchor"
    request["run_id"] = run_id
    cutoff = product.index["cutoff_utc"]
    forward_start = (pd.Timestamp("2024-07-15T00:00:00Z") if selected_owner
                     else prefix_end + pd.Timedelta(days=35))
    forward_end = (pd.Timestamp("2024-10-12T00:00:00Z") if selected_owner
                   else prefix_end + pd.Timedelta(days=400))
    accessed_at = _observed_utc_after() if selected_owner else "2025-01-01T00:00:00Z"
    request["anchor"] = {"anchor_id": anchor_id, "ordinal": 0,
        "selection_cutoff_utc": cutoff, "forward_start_utc": native_prefix_fixture._iso(forward_start),
        "forward_end_exclusive_utc": native_prefix_fixture._iso(forward_end),
        "evidence_accessed_at_utc": accessed_at, "process_precommit_receipt_sha256": "a" * 64}
    if injected:
        assert pd.Timestamp(request["anchor"]["selection_cutoff_utc"]) == pd.Timestamp(selection_cutoff_utc)
        assert prefix_end == pd.Timestamp(selection_cutoff_utc) < forward_start
    request["cell"] = {"symbol": OBSERVED_SYMBOL, "timeframe": timeframe, "regime": regime}
    request["native_prefix"] = dict(product.native_prefix)
    dependency = request["dependency_policy"]["payload"]
    dependency["regime_warmup_bars"] = 115
    request["dependency_policy"]["payload_sha256"] = _sha(dependency)
    if selected_owner:
        native_frame = native_prefix.open_verified_prefix_frame(product)
        native_opens = pd.DatetimeIndex(native_frame.index)
        native_closes = pd.DatetimeIndex(pd.to_datetime(native_frame["_BAR_CLOSE_TS"], utc=True))
        context_bars = max(115, *(int(item["refit_recipe"]["data_requirements"]["warmup_bars"])
                                  for item in attempt_artifacts))
    validation_end = prefix_end.normalize() if selected_owner else prefix_end
    folds = []
    for ordinal in range(4):
        if selected_owner:
            validation_start = validation_end - pd.Timedelta(days=256 - 64 * ordinal)
        else:
            validation_start = validation_end - pd.Timedelta(days=252 - 63 * ordinal)
        fit_end = validation_start - pd.Timedelta(days=180)
        fit_start = fit_end - pd.Timedelta(days=500)
        context_start = (validation_start - pd.Timedelta(hours=context_bars) if selected_owner
                         else validation_start - pd.Timedelta(days=115))
        validation_finish = validation_start + pd.Timedelta(days=63)
        if selected_owner:
            validation_finish = validation_start + pd.Timedelta(days=64)
        fold = horizon_fixture._fold(f"ROLL-{ordinal:03d}", "CAUSAL_ROLLING_ORIGIN_INNER",
            native_prefix_fixture._iso(fit_start), native_prefix_fixture._iso(fit_end),
            native_prefix_fixture._iso(context_start), native_prefix_fixture._iso(validation_start),
            native_prefix_fixture._iso(validation_finish))
        if selected_owner:
            fold.update({"fit_rows": int(((native_opens >= fit_start) & (native_opens < fit_end)
                                        & (native_closes <= fit_end)).sum()),
                "validation_rows": int(((native_opens >= validation_start) & (native_opens < validation_finish)
                                        & (native_closes <= validation_finish)).sum())})
        else:
            fold.update({"fit_rows": 500, "validation_rows": 63})
        fold.pop("fold_definition_sha256")
        fold["fold_definition_sha256"] = _sha(fold)
        folds.append(fold)
    request["candidates"] = [horizon_fixture._candidate("native-500", "CALENDAR_INTERVAL", 500,
        min(fold["fit_start_utc"] for fold in folds), folds)]
    trial_names = ("selected-flat-control-ema", "selected-flat-control-volume") if selected_owner else ("native-ema",)
    request["trials"] = [horizon_fixture._trial(trial_name, identity,
        item["winner_process"]["strategy_code_sha256"], _sha(item["trial_definition"]["parameters"]))
        for trial_name, identity, item in zip(trial_names, strategy_ids, attempt_artifacts, strict=True)]
    pair = fitted.frozen_tier_pair_contract
    request["horizon_policy"] = {"schema_version": "fxpm.historical-horizon-policy.v3",
        "policy_id": "UNISSUED_SYNTHETIC_ENGINEERING_ONLY", "policy_release_sha256": "b" * 64,
        "cost_contract_sha256": _sha({tier["tier"]: _sha(tier["numerical_execution_cost_policy"]) for tier in pair["tiers"]}),
        "frozen_tier_pair_contract_sha256": _sha(pair),
        "evaluator_release_sha256": search_execution.AUTHORITATIVE_SEARCH_EXECUTION_RELEASE_SHA256}
    plan = training_horizon.build_historical_horizon_plan(request)
    plan_body, verified = search_execution._verify_attempt_roster(plan=plan, attempt_artifacts=attempt_artifacts, fitted=fitted, regime=regime)
    fold_plan = search_execution._winner_fold_plan(verified)
    partition_precommit = search_execution._partition_cost_precommitments(attempts=verified, fold_plan=fold_plan,
        plan_body=plan_body, fitted=fitted, config=config)
    sink = spool.open_symbol_sink(context, OBSERVED_SYMBOL)
    normalized, execution_contracts, count_receipts = adapter._strict_attempt_artifacts(attempt_artifacts, symbol=OBSERVED_SYMBOL,
        timeframe=timeframe, regime=regime, plan_body=plan_body, plan_receipt_sha256=plan.receipt_sha256,
        native_prefix_product=product)
    precommitted_at = _observed_utc_after() if selected_owner else "2025-01-02T00:00:00Z"
    attempts_frozen_at = _observed_utc_after(precommitted_at) if selected_owner else "2025-01-03T00:00:00Z"
    freeze = (raw_winner.freeze_winner_search_plan_v5 if winner_generation is winner.V16_GENERATION
              else raw_winner.freeze_winner_search_plan_v4)
    frozen = freeze(sink, symbol=OBSERVED_SYMBOL, timeframe=timeframe, regime=regime,
        precommitted_at_utc=precommitted_at, attempts_frozen_at_utc=attempts_frozen_at,
        fold_plan=fold_plan, attempt_artifacts=normalized,
        precommitted_evidence_roles=search_execution._precommitted_evidence_roles(verified),
        horizon_execution_contracts=execution_contracts, source_contract_seeds=fitted.source_contract_seeds,
        frozen_tier_pair_contract=pair, partition_cost_precommitments=partition_precommit,
        risk_unit_contract=raw_winner.build_precommitted_risk_unit_contract(),
        artifact_namespace=f"historical-anchor/{run_id}/{anchor_id}")
    cross = adapter._cross_binding_body(plan_body=plan_body, plan_receipt_sha256=plan.receipt_sha256,
        frozen=frozen, attempt_artifacts=normalized, producer_request_sha256="c" * 64,
        anchor_id=anchor_id, artifact_namespace=frozen.artifact_namespace,
        source_group_decision_sha256=fitted.source_group_decision.receipt_sha256,
        regime_fit_decision_sha256=fitted.regime_fit_decision.receipt_sha256,
        horizon_execution_contracts=execution_contracts, source_count_receipts=count_receipts)
    raw_ids = search_execution._raw_attempt_ids(frozen, verified)
    evidence_started = None if selected_owner else "2025-01-04T00:00:00Z"
    evidence = [] if selected_owner else [search_execution._build_planned_attempt_evidence(attempt=attempt, raw_attempt_id=raw_id,
        plan_body=plan_body, plan_receipt_sha256=plan.receipt_sha256, fitted=fitted, config=config,
        partition_cost_precommitments=partition_precommit, source_observation_references=None)
        for attempt, raw_id in zip(verified, raw_ids, strict=True)]
    evidence_completed = None if selected_owner else "2025-01-05T00:00:00Z"
    return {"context": context, "sink": sink, "fitted": fitted, "config": config, "plan": plan,
        "attempt_artifacts": attempt_artifacts, "verified_attempts": verified, "fold_plan": fold_plan,
        "horizon_request": copy.deepcopy(request),
        "frozen": frozen, "evidence": evidence, "cross_binding_bytes": _canonical(cross),
        "partition_cost_precommitments": partition_precommit, "native_product": product,
        "provenance": "UNISSUED_SYNTHETIC_CONTROL_CARRIER", "qualification_authority": False,
        "retained_native_root": None if retained_native_root is None else str(Path(retained_native_root).resolve()),
        "selected_owner_case": selected_owner, "timeframe": timeframe, "frozen_profile": frozen_profile,
        "evidence_deferred": selected_owner,
        "evidence_started_at_utc": evidence_started, "evidence_completed_at_utc": evidence_completed,
        **bound_details}


def pytest_addoption(parser):
    parser.addoption("--fxpm-retained-native-root", action="store", default=None,
        help="Explicit read-only reuse of a QA86 synthetic source with matching completed creation metadata.")
    parser.addoption("--fxpm-selected-owner-native-root", action="store", default=None,
        help="Reuse the declared QA90 synthetic source and frozen profile, without regeneration.")


@pytest.fixture(scope="session")
def current_native_case(tmp_path_factory, pytestconfig):
    # Collection and the plugin import this module under different names.
    if not hasattr(pytestconfig, "_fxpm_current_native_case"):
        pytestconfig._fxpm_current_native_case = build_current_native_case(
            tmp_path_factory.mktemp("current-native-candidate"),
            retained_native_root=pytestconfig.getoption("--fxpm-retained-native-root", default=None))
    return pytestconfig._fxpm_current_native_case


def test_current_native_case_cache_belongs_to_shared_pytest_session(tmp_path, monkeypatch):
    import importlib
    from types import SimpleNamespace

    monkeypatch.syspath_prepend(str(Path(__file__).parent))
    modules = (importlib.import_module("test_historical_production_adapter"),
               importlib.import_module("tests.test_historical_production_adapter"))
    assert modules[0] is not modules[1]
    builds, directories = [], []

    def build(path, *, retained_native_root):
        case = object()
        builds.append((path, retained_native_root, case))
        return case

    def mktemp(name):
        path = tmp_path / f"{name}-{len(directories)}"
        directories.append(path)
        return path

    for module in modules:
        monkeypatch.setattr(module, "build_current_native_case", build)
    factory = SimpleNamespace(mktemp=mktemp)
    for ordinal, order in enumerate((modules, modules[::-1])):
        retained = f"declared-source-session-{ordinal}"

        def getoption(name, default=None):
            assert name == "--fxpm-retained-native-root"
            return retained

        config = SimpleNamespace(getoption=getoption)
        cases = [module.current_native_case.__wrapped__(factory, config)
                 for module in (*order, *order)]
        assert all(case is cases[0] for case in cases)
        assert len(builds) == len(directories) == ordinal + 1
        assert builds[-1] == (directories[-1], retained, cases[0])
    assert builds[0][2] is not builds[1][2]


@pytest.fixture(scope="session")
def selected_owner_native_case(tmp_path_factory, pytestconfig):
    retained = pytestconfig.getoption("--fxpm-selected-owner-native-root", default=None)
    if retained is None:
        retained = (Path(__file__).resolve().parents[1] / "audit_reports/fxpm_workshop_20261007"
            / "selected_owner_fixture_verification/runtime/attempt_003_pytest_tmp"
            / "selected-owner-candidate0/source/data/native")
    return build_current_native_case(tmp_path_factory.mktemp("selected-owner-flat-control"),
        selected_owner=True, retained_native_root=retained)

def _sha1(value):
    if isinstance(value, str):
        value = {"label": value}
    return hashlib.sha1(_canonical(value)).hexdigest()


def _read_descriptor_json(context, descriptor):
    path = context.root.joinpath(*descriptor["relative_path"].split("/"))
    payload = path.read_bytes()
    assert len(payload) == descriptor["size_bytes"]
    assert hashlib.sha256(payload).hexdigest() == descriptor["sha256"]
    return json.loads(payload.decode("utf-8"))


def _plan(
    anchor,
    symbol,
    timeframe,
    regime,
    *,
    source_available,
    commitments=None,
    current=False,
    horizon_anchor=None,
):
    label = f"{anchor['anchor_id']}:{symbol}:{timeframe}:{regime}"
    request = (
        horizon_fixture._request()
        if source_available
        else horizon_fixture._terminal_source_request("SOURCE_UNAVAILABLE")
    )
    suffix = anchor["anchor_id"].rsplit("-", 1)[-1]
    ordinal = anchor.get("ordinal", int(suffix) if suffix.isdecimal() else 0)
    request["run_id"] = "historical-run-001"
    request["anchor"] = {
        "anchor_id": anchor["anchor_id"],
        "ordinal": ordinal,
        "selection_cutoff_utc": anchor["selection_cutoff_utc"],
        "forward_start_utc": f"{anchor['forward_start_date']}T00:00:00Z",
        "forward_end_exclusive_utc": f"{anchor['forward_end_date']}T00:00:00Z",
        "evidence_accessed_at_utc": "2026-07-13T12:00:00Z",
        "process_precommit_receipt_sha256": PROCESS_PRECOMMIT,
    }
    if horizon_anchor is not None:
        request["anchor"] = copy.deepcopy(horizon_anchor)
    request["cell"] = {
        "symbol": symbol,
        "timeframe": timeframe,
        "regime": regime,
    }
    prefix = request["native_prefix"]
    prefix["symbol"] = symbol
    prefix["timeframe"] = timeframe
    if source_available:
        prefix["artifact_rows"] = 1_000
        prefix["prefix_rows"] = 700
        dependency = request["dependency_policy"]["payload"]
        for name in (
            "feature_lookback_bars",
            "label_lookahead_bars",
            "max_holding_bars",
            "execution_delay_bars",
            "regime_warmup_bars",
            "cost_lookback_bars",
            "max_holding_seconds",
        ):
            dependency[name] = 0
        request["dependency_policy"]["payload_sha256"] = _sha(dependency)
    prefix["source_terminal_receipt_sha256"] = (
        None if source_available else _sha(f"{label}:source-terminal")
    )
    if current:
        assert not source_available
        absence = {
            "schema_version": "fxpm.terminal-search-non-evaluation-contract.v1",
            "source_state": prefix["source_state"],
            "source_terminal_receipt_sha256": prefix["source_terminal_receipt_sha256"],
            "economic_evaluation": "NOT_APPLICABLE",
            "fixture_provenance": "UNISSUED_SYNTHETIC_SOURCE_ABSENCE",
        }
        request["schema_version"] = "fxpm.historical-horizon-request.v3"
        request["horizon_policy"] = {
            "schema_version": "fxpm.historical-horizon-policy.v3",
            "policy_id": "UNISSUED_SYNTHETIC_SOURCE_ABSENCE",
            "policy_release_sha256": "b" * 64,
            "cost_contract_sha256": _sha({**absence,
                "binding_domain": "fxpm.terminal-cost-not-applicable.v1"}),
            "frozen_tier_pair_contract_sha256": _sha({**absence,
                "binding_domain": "fxpm.terminal-frozen-tier-evaluation-not-applicable.v1"}),
            "evaluator_release_sha256": search_execution.AUTHORITATIVE_SEARCH_EXECUTION_RELEASE_SHA256,
        }
    # Registry commitments are deliberately derived by Horizon's public builder.
    # The legacy helper argument remains only so older call sites stay compact.
    assert commitments is None or type(commitments) is dict
    return training_horizon.build_historical_horizon_plan(request)


def _plans(
    anchor,
    symbol,
    timeframe,
    *,
    source_available,
    commitments=None,
    current=False,
    horizon_anchor=None,
):
    return [
        _plan(
            anchor,
            symbol,
            timeframe,
            regime,
            source_available=source_available,
            commitments=commitments,
            current=current,
            horizon_anchor=horizon_anchor,
        )
        for regime in book.REGIME_ORDER
    ]


def _terminal_authority(anchor_id, symbol, timeframe):
    label = f"{anchor_id}:{symbol}:{timeframe}"
    source = book.create_historical_source_group_decision(
        symbol=symbol,
        timeframe=timeframe,
        availability=book.TERMINAL_UNAVAILABLE,
        reason_code="BROKER_ARCHIVE_UNAVAILABLE",
        source_contract_sha256=_sha(f"{label}:source-contract"),
        source_group_evidence_sha256=_sha(f"{label}:source-evidence"),
        acquisition_attempts_exact=3,
        terminal_proof_sha256=_sha(f"{label}:terminal-proof"),
    )
    assert source is not None
    fit = book.create_regime_fit_decision(
        source_group_decision=source,
        fit_state=book.REGIME_NOT_APPLICABLE,
        reason_code="SOURCE_TERMINAL_UNAVAILABLE",
        regime_fit_policy_sha256=_sha(f"{label}:regime-policy"),
    )
    assert fit is not None
    return source, fit


def _terminal_technical_authority(product):
    return search_execution.fit_terminal_native_timeframe(
        product=product,
        config=PipelineConfig(),
    )


def _terminal_technical_plans(
    anchor,
    symbol,
    timeframe,
    *,
    native_product,
    activation_id=None,
):
    plans = []
    for regime in book.REGIME_ORDER:
        request = horizon_fixture._terminal_source_request("TECHNICAL_TERMINAL")
        request["run_id"] = "historical-run-001"
        request["anchor"] = {
            "anchor_id": anchor["anchor_id"],
            "ordinal": int(anchor["anchor_id"].rsplit("-", 1)[-1]),
            "selection_cutoff_utc": anchor["selection_cutoff_utc"],
            "forward_start_utc": f"{anchor['forward_start_date']}T00:00:00Z",
            "forward_end_exclusive_utc": (f"{anchor['forward_end_date']}T00:00:00Z"),
            "evidence_accessed_at_utc": "2026-07-13T12:00:00Z",
            "process_precommit_receipt_sha256": PROCESS_PRECOMMIT,
        }
        request["cell"] = {
            "symbol": symbol,
            "timeframe": timeframe,
            "regime": regime,
        }
        request["native_prefix"] = dict(native_product.native_prefix)
        if activation_id is not None:
            request["native_prefix"]["activation_id"] = activation_id
        plans.append(training_horizon.build_historical_horizon_plan(request))
    return plans


def _genuine_terminal_technical_products(
    tmp_path,
    context,
    anchor,
    symbol,
    monkeypatch,
):
    data_dir = (tmp_path / "terminal-native-data").resolve()
    manifest_path = data_dir / "native" / "activations" / "terminal.json"
    manifest_path.parent.mkdir(parents=True)
    entries = {
        timeframe: {
            "symbol": symbol,
            "timeframe": timeframe,
            "status": "technical_error",
            "source_provenance_sha256": _sha(f"terminal:{timeframe}:provenance"),
        }
        for timeframe in book.TIMEFRAME_ORDER
    }
    manifest = {
        "activation_id": "terminal-activation-genuine",
        "entries": {symbol: entries},
        "run_manifest_relative_path": "runs/terminal/run_manifest.json",
        "run_manifest_sha256": _sha("terminal-run-manifest"),
    }
    manifest_path.write_bytes(_canonical(manifest))
    manifest_sha256 = hashlib.sha256(manifest_path.read_bytes()).hexdigest()

    def terminal_resolution(_data_dir, requested_symbol, timeframe, **_kwargs):
        assert requested_symbol == symbol
        assert timeframe in book.TIMEFRAME_ORDER
        return ActiveNativeResolution(
            symbol=requested_symbol,
            timeframe=timeframe,
            activation_id=manifest["activation_id"],
            activation_manifest_path=manifest_path,
            activation_manifest_sha256=manifest_sha256,
            errors=["NATIVE_ACTIVE_CELL_TERMINAL_TECHNICAL"],
        )

    monkeypatch.setattr(
        native_prefix,
        "resolve_active_native_artifact",
        terminal_resolution,
    )
    products = {}
    for timeframe in book.TIMEFRAME_ORDER:
        product = native_prefix.produce_activated_native_prefix(
            data_dir=data_dir,
            context=context,
            symbol=symbol,
            timeframe=timeframe,
            cutoff_utc=anchor["selection_cutoff_utc"],
            source_accessed_at_utc="2026-07-13T12:00:00Z",
            production_contract_required=False,
        )
        assert product.source_state == native_prefix.TECHNICAL_TERMINAL
        assert product._data_dir == data_dir
        products[timeframe] = product
    return products


def _request(context, anchor, *, bind_sinks=True):
    body = {
        "schema_version": orchestration.PRODUCER_REQUEST_SCHEMA,
        "authority_scope": orchestration.AUTHORITY_SCOPE,
        "run_id": "historical-run-001",
        "generation_id": context.generation_id,
        "precommit_receipt_sha256": _sha("precommit"),
        "process_precommit_receipt_sha256": PROCESS_PRECOMMIT,
        "optimizer_producer_release_sha256": (
            adapter.HISTORICAL_PRODUCER_RELEASE_SHA256
        ),
        "anchor": anchor,
        "symbols": list(book.PINNED_SYMBOL_ORDER),
        "timeframes": list(book.TIMEFRAME_ORDER),
        "regimes": list(book.REGIME_ORDER),
        "requires_exact_sparse_state_roster": True,
        "possible_regime_slots_are_forced_observations": False,
        "confers_causal_or_current_authority": False,
    }
    sinks = None
    if bind_sinks:
        sinks = {
            symbol: spool.open_symbol_sink(context, symbol)
            for symbol in book.PINNED_SYMBOL_ORDER
        }
    return orchestration.AnchorProducerRequest(_canonical(body), sinks)


def _context(tmp_path):
    return spool.freeze_generation_context(
        tmp_path / "authority",
        generation_id="historical-adapter-generation-001",
        native_universe_sha256="b" * 64,
        config_sha256="c" * 64,
    )


def test_transport_worker_request_cold_reopen_preserves_exact_v16_bytes(tmp_path):
    """SYNTHETIC custody/selector witness, not a native raw-production proof."""
    from tests.test_book_multiplicity import _transport_candidate_family
    first = _transport_candidate_family()[0]
    request, decision = first.winner_request, first.winner_decision
    context = _context(tmp_path)
    sink = spool.open_symbol_sink(context, first.symbol)
    request_entry = sink.put_opaque("synthetic/v16/request", "winner_v16_request",
        (_canonical(request.to_dict()),), format=raw_winner.ASCII_JSON_FORMAT)
    decision_entry = sink.put_opaque("synthetic/v16/decision", "winner_v16_decision",
        (_canonical(decision.to_dict()),), format=raw_winner.ASCII_JSON_FORMAT)
    receipt = sink.seal()
    manifest = spool.verify_symbol_receipt(context.root, receipt,
        expected_generation_id=context.generation_id, expected_symbol=first.symbol,
        expected_native_universe_sha256=context.native_universe_sha256,
        expected_config_sha256=context.config_sha256)
    assert {row["artifact_type"] for row in manifest["artifacts"]} == {"winner_v16_request", "winner_v16_decision"}
    restored_request, restored_decision = adapter._rebuild_winner_pair(context,
        {"request": request_entry["descriptor"], "request_sha256": request.request_sha256,
         "decision": decision_entry["descriptor"], "decision_sha256": decision.receipt_sha256},
        symbol=first.symbol, timeframe=first.timeframe, regime=first.regime)
    assert restored_request.to_dict() == request.to_dict()
    assert restored_decision.to_dict() == decision.to_dict()
    assert winner.MAX_REQUEST_BYTES == 16 * 1024 * 1024
    assert winner.MAX_SUPPORTED_WINNER_REQUEST_BYTES == 64 * 1024 * 1024


@pytest.mark.parametrize("edition,schema,accepted", [
    ("v16", "fxpm.winner-raw-evidence-production.v5", False),
    ("v16", "fxpm.winner-raw-evidence-production.v6", True),
    ("v14", "fxpm.winner-raw-evidence-production.v4", True),
    ("v14", "fxpm.winner-raw-evidence-production.v5", True),
    ("v15", "fxpm.winner-raw-evidence-production.v4", True),
    ("v15", "fxpm.winner-raw-evidence-production.v5", True),
])
def test_transport_worker_production_tuple_refuses_v15_relabel(tmp_path, edition, schema, accepted):
    """SYNTHETIC metadata cross-binding only; account/raw economics are not replayed."""
    from types import SimpleNamespace
    from tests import test_winner_selection as fixtures
    from tests.test_winner_transport_capacity import build_v16_payload, synthetic_pair
    payload = (build_v16_payload() if edition == "v16" else fixtures._current_payload_request(
        pair_contract=synthetic_pair()) if edition == "v15" else fixtures._payload_request())
    request = winner.create_winner_selection_request(**payload)
    assert request is not None
    decision = winner.evaluate_historical_cell_winner(request)
    first = SimpleNamespace(symbol="AUDUSD", timeframe="M5", regime="TREND")
    context = _context(tmp_path)
    sink = spool.open_symbol_sink(context, first.symbol)
    req_entry = sink.put_opaque(f"synthetic/{edition}/request", f"winner_{edition}_request",
        (_canonical(request.to_dict()),), format=raw_winner.ASCII_JSON_FORMAT)
    dec_entry = sink.put_opaque(f"synthetic/{edition}/decision", f"winner_{edition}_decision",
        (_canonical(decision.to_dict()),), format=raw_winner.ASCII_JSON_FORMAT)
    cross_bytes = _canonical({"fixture_provenance": "SYNTHETIC_METADATA_ONLY"})
    cross_entry = sink.put_opaque("synthetic/v16/cross", "historical_horizon_winner_cross_binding",
        (cross_bytes,), format=raw_winner.ASCII_JSON_FORMAT)
    body = {"schema_version": schema, "generation_context_sha256": context.context_sha256,
        "symbol": first.symbol, "timeframe": first.timeframe, "regime": first.regime,
        "request_sha256": request.request_sha256, "decision_receipt_sha256": decision.receipt_sha256,
        "provenance_cross_binding_sha256": hashlib.sha256(cross_bytes).hexdigest(),
        "artifact_roster": {"winner-request": req_entry["descriptor"], "winner-decision": dec_entry["descriptor"]}}
    production = sink.put_opaque("synthetic/v16/production", "winner_raw_evidence_production_receipt",
        (_canonical(body),), format=raw_winner.ASCII_JSON_FORMAT)
    args = dict(context=context, descriptor=production["descriptor"], cross_descriptor=cross_entry["descriptor"],
        cross_bytes=cross_bytes, request_descriptor=req_entry["descriptor"], decision_descriptor=dec_entry["descriptor"],
        request=request, decision=decision, symbol=first.symbol, timeframe=first.timeframe, regime=first.regime)
    if not accepted:
        with pytest.raises(adapter.HistoricalProductionAdapterError, match="WORKER_PRODUCTION_RECEIPT_SPLICE"):
            adapter._verify_worker_production_receipt(**args)
    else:
        assert adapter._verify_worker_production_receipt(**args) == _canonical(body)


def test_transport_worker_wraps_v16_builder_value_error(tmp_path):
    """Real owned descriptor carrying deliberately malformed deep V16 JSON."""
    from tests.test_winner_transport_capacity import build_v16_payload
    payload = build_v16_payload()
    nested = 0
    for _ in range(winner.MAX_JSON_DEPTH + 1):
        nested = [nested]
    payload["precommitment_payload"]["SYNTHETIC_INVALID_DEPTH"] = nested
    body = {"contract_version": winner.V16_GENERATION.request_version}
    for name in ("precommitment", "attempt_ledger", "evidence_ledger"):
        value = payload[name + "_payload"]
        body[name] = {"payload": value, "payload_sha256": _sha(value)}
    context = _context(tmp_path)
    sink = spool.open_symbol_sink(context, "AUDUSD")
    entry = sink.put_opaque("synthetic/malformed-depth", "winner_v16_request",
        (_canonical(body),), format=raw_winner.ASCII_JSON_FORMAT)
    with pytest.raises(adapter.HistoricalProductionAdapterError) as caught:
        adapter._rebuild_winner_pair(context, {"request": entry["descriptor"],
            "request_sha256": entry["descriptor"]["sha256"], "decision": {}, "decision_sha256": "a" * 64},
            symbol="AUDUSD", timeframe="M5", regime="TREND")
    assert str(caught.value) == "WORKER_WINNER_REQUEST_INVALID"
    assert isinstance(caught.value.__cause__, ValueError)
    assert str(caught.value.__cause__) == "INPUT_STRUCTURE_LIMIT_EXCEEDED"


def test_transport_capture_entrypoints_are_explicit_and_keep_legacy_defaults(monkeypatch):
    observed = []
    marker = object()
    monkeypatch.setattr(adapter, "HistoricalSymbolSearchCapture",
        lambda *args, **kwargs: observed.append((args, kwargs)) or marker)
    assert adapter.open_historical_symbol_capture("context", "request", "AUDUSD") is marker
    assert observed[-1][1] == {}
    assert adapter.open_transport_historical_symbol_capture("context", "request", "AUDUSD") is marker
    assert observed[-1][1] == {"_winner_generation": winner.V16_GENERATION}
    monkeypatch.setattr(adapter, "open_declared_slot_capture",
        lambda *args, **kwargs: observed.append((args, kwargs)) or marker)
    assert adapter.open_transport_declared_slot_capture("context", request="request", process="process",
        plan="plan", source_session="session") is marker
    assert observed[-1][1]["_winner_generation"] is winner.V16_GENERATION


def _anchor(anchor_id="anchor-01", cutoff=date(2020, 1, 1)):
    return {
        "anchor_id": anchor_id,
        "selection_cutoff_utc": f"{cutoff.isoformat()}T00:00:00Z",
        "forward_start_date": (cutoff + timedelta(days=35)).isoformat(),
        "forward_end_date": (cutoff + timedelta(days=399)).isoformat(),
        "purge_calendar_days": 35,
        "horizon_binding_count": book.FAMILY_SIZE,
        "horizon_binding_roster_sha256": "d" * 64,
    }


def _bind_terminal_symbol(capture, anchor):
    for timeframe in book.TIMEFRAME_ORDER:
        source, fit = _terminal_authority(
            anchor["anchor_id"], capture.symbol, timeframe
        )
        capture.bind_timeframe(
            timeframe,
            horizon_plans=_plans(
                anchor, capture.symbol, timeframe, source_available=False
            ),
            source_group_decision=source,
            regime_fit_decision=fit,
        )


def _bind_terminal_technical_symbol(capture, anchor, products):
    for timeframe in book.TIMEFRAME_ORDER:
        native_product = products[timeframe]
        plans = _terminal_technical_plans(
            anchor,
            capture.symbol,
            timeframe,
            native_product=native_product,
        )
        authority = _terminal_technical_authority(native_product)
        capture.bind_timeframe(
            timeframe,
            horizon_plans=plans,
            source_group_decision=authority.source_group_decision,
            regime_fit_decision=authority.regime_fit_decision,
            terminal_native_timeframe=authority,
        )


def _detector_params():
    values = {
        "k_confirm": 2,
        "gap_min": 0.1,
        "k_hold": 3,
        "adx_period": 14,
        "atr_period": 14,
        "bb_period": 20,
        "bb_std": 2.0,
        "efficiency_period": 10,
        "bb_squeeze_lookback": 100,
        "bb_squeeze_percentile": 20.0,
        "atr_lookback": 100,
        "structure_period": 20,
        "structure_atr_mult": 1.0,
        "adx_trend_threshold": 25.0,
    }
    for prefix in ("trend", "range", "breakout", "chop"):
        for suffix in (
            ("adx", "efficiency", "slope")
            if prefix == "trend"
            else (
                ("adx", "efficiency", "containment")
                if prefix == "range"
                else (
                    ("squeeze", "atr", "structure")
                    if prefix == "breakout"
                    else ("adx", "efficiency", "whipsaw")
                )
            )
        ):
            values[f"{prefix}_{suffix}_weight"] = 1.0 / 3.0
    return values


def _available_authority(
    symbol, timeframe, source_contract_sha, *, native_prefix_body=None
):
    native_prefix_body = native_prefix_body or {}
    activation_id = native_prefix_body.get("activation_id", "activation-001")
    activation_manifest_sha256 = native_prefix_body.get(
        "activation_manifest_sha256", "1" * 64
    )
    native_source_sha256 = native_prefix_body.get("artifact_sha256", "2" * 64)
    support_first_ts = native_prefix_body.get(
        "prefix_first_open_utc", "2018-01-01T00:00:00+00:00"
    )
    support_last_ts = native_prefix_body.get(
        "prefix_last_open_utc", "2019-12-31T00:00:00+00:00"
    )
    source = book.create_historical_source_group_decision(
        symbol=symbol,
        timeframe=timeframe,
        availability=book.AVAILABLE,
        reason_code="AUTHENTICATED_NATIVE_SOURCE",
        source_contract_sha256=source_contract_sha,
        source_group_evidence_sha256=_sha("available-source-evidence"),
        acquisition_attempts_exact=1,
        activation_id=activation_id,
        activation_manifest_sha256=activation_manifest_sha256,
        native_source_sha256=native_source_sha256,
    )
    assert source is not None
    code_sha = "3" * 64
    detector = book.create_regime_detector_record(
        source_group_decision=source,
        fit_cutoff_utc="2020-01-01T00:00:00+00:00",
        detector_version="regime-detector-v1",
        detector_code_sha256=code_sha,
        parameters=_detector_params(),
    )
    assert detector is not None
    counts = {regime: 0 for regime in book.REGIME_ORDER}
    counts["TREND"] = 100
    support = {
        "schema_version": book.REGIME_TRAIN_SUPPORT_SCHEMA_VERSION,
        "symbol": symbol,
        "timeframe": timeframe,
        "surface": "REGIME_LIVE",
        "warmup_policy": book.REGIME_TRAIN_SUPPORT_WARMUP_POLICY,
        "label_order": list(book.REGIME_ORDER),
        "eligible_rows": 100,
        "counts": counts,
        "observed_regimes": ["TREND"],
        "unobserved_regimes": ["RANGE", "BREAKOUT", "CHOP"],
        "support_state_by_regime": {
            regime: ("EMPIRICALLY_OBSERVED" if count else "UNOBSERVED")
            for regime, count in counts.items()
        },
        "first_ts": support_first_ts,
        "last_ts": support_last_ts,
        "vector_encoding": book.REGIME_TRAIN_SUPPORT_VECTOR_ENCODING,
        "vector_sha256": _sha("regime-vector"),
        "bindings": {
            "symbol": symbol,
            "timeframe": timeframe,
            "activation_id": activation_id,
            "activation_manifest_sha256": activation_manifest_sha256,
            "source_sha256": native_source_sha256,
            "train_window_sha256": "4" * 64,
            "params_sha256": detector["parameters_sha256"],
            "detector_code_sha256": code_sha,
        },
    }
    support["receipt_sha256"] = _sha(support)
    fit = book.create_regime_fit_decision(
        source_group_decision=source,
        fit_state=book.REGIME_FITTED,
        reason_code="TRAIN_ONLY_REGIME_FIT_COMPLETE",
        regime_fit_policy_sha256="5" * 64,
        detector_process_sha256=code_sha,
        regime_fit_evidence_sha256="6" * 64,
        four_regime_mapping_sha256="7" * 64,
        regime_params_sha256=detector["parameters_sha256"],
        regime_detector_record=detector,
        train_regime_support=support,
    )
    assert fit is not None
    return source, fit


def _bind_single_observed_capture(
    capture,
    anchor,
    source_sha,
    *,
    commitments=None,
    observed_plan=None,
):
    observed_plan_body = observed_plan.to_dict() if observed_plan is not None else {}
    native_prefix_body = observed_plan_body.get("native_prefix")
    for timeframe in book.TIMEFRAME_ORDER:
        if timeframe == OBSERVED_TIMEFRAME:
            source, fit = _available_authority(
                capture.symbol,
                timeframe,
                source_sha,
                native_prefix_body=native_prefix_body,
            )
            available = True
        else:
            source, fit = _terminal_authority(
                anchor["anchor_id"], capture.symbol, timeframe
            )
            available = False
        horizon_plans = _plans(
            anchor,
            capture.symbol,
            timeframe,
            source_available=available,
            commitments=commitments if timeframe == OBSERVED_TIMEFRAME else None,
        )
        if timeframe == OBSERVED_TIMEFRAME and observed_plan is not None:
            horizon_plans[0] = observed_plan
            for ordinal, regime in enumerate(book.REGIME_ORDER[1:], start=1):
                horizon_plans[ordinal] = _real_unobserved_horizon_plan(
                    anchor,
                    capture.symbol,
                    timeframe,
                    regime,
                    native_prefix_body=native_prefix_body,
                    source_decision=source,
                    fit_decision=fit,
                )
        capture.bind_timeframe(
            timeframe,
            horizon_plans=horizon_plans,
            source_group_decision=source,
            regime_fit_decision=fit,
        )


def _finish_through_worker_bridge(context, anchor, capture):
    descriptor = adapter.spool_historical_symbol_worker_bundle(capture)
    parent_request = capture.request
    return adapter.reopen_historical_symbol_worker_bundle(
        context,
        parent_request,
        descriptor,
    )


TEST_INSTRUMENT_SPEC = InstrumentSpec(
    symbol=OBSERVED_SYMBOL,
    pip_position=4,
    pip_value=10.0,
    spread_avg=1.0,
)
TEST_INSTRUMENT_SPEC_SHA256 = Backtester._cost_attestation_json_sha256(
    {
        "schema_version": BACKTEST_INSTRUMENT_SPEC_SCHEMA_VERSION,
        "instrument_spec": asdict(TEST_INSTRUMENT_SPEC),
    }
)
BASE_SPREAD_META = {
    "bar_column": "SpreadMax",
    "entry_column": "SpreadOpen",
    "entry_model": "open_spread",
    "fallback_to_fixed": False,
    "mode": "conservative_max",
    "source": "historical_spread_points",
    "units": "price",
}
COST_LADDER_CONTRACT = build_backtest_cost_ladder_contract(
    BASE_SPREAD_META,
    use_spread=True,
    instrument_spec_sha256=TEST_INSTRUMENT_SPEC_SHA256,
)
_COST_LEVELS = {
    level["level_id"]: level for level in COST_LADDER_CONTRACT["levels"]
}
RISK_CONTRACT = raw_winner.build_precommitted_risk_unit_contract()


def _swap(label):
    return {
        "contract_version": "causal_swap_accrual_v1",
        "status": "AVAILABLE",
        "provenance_status": "STRESS_APPROVED",
        "execution_state_compounding": True,
        "historical_rates_authenticated": False,
        "stress_policy_explicitly_approved": True,
        "stress_policy_immutable": True,
        "stress_policy_sha256": "a" * 64,
        "timestamp_source": "_BAR_CLOSE_TS",
        "price_activation_manifest_sha256": "1" * 64,
        "terms_activation_manifest_sha256": "2" * 64,
        "terms_snapshot_sha256": "3" * 64,
        "schedule_sha256": "4" * 64,
        "target_bars_sha256": "5" * 64,
        "target_sidecar_sha256": "6" * 64,
        "projection_contract": "fxpm-swap-preopen-close-projection-v1",
        "projection_sha256": _sha(label),
    }


def _trades(start, values=None):
    values = values or [0.30 + 0.01 * (index % 2) for index in range(12)]
    rows = []
    for index, value in enumerate(values):
        opened = datetime.combine(
            start + timedelta(days=index), datetime.min.time(), timezone.utc
        ) + timedelta(hours=10)
        rows.append(
            {
                "entry_bar": index * 2 + 1,
                "exit_bar": index * 2 + 2,
                "signal_bar": index * 2,
                "entry_time": opened.isoformat(),
                "exit_time": (opened + timedelta(hours=1)).isoformat(),
                "direction": "LONG",
                "position_size": 1.0,
                "pnl_dollars": value * 100.0,
                "risk_amount": 100.0,
                "exit_reason": "SIGNAL_EXIT",
            }
        )
    return rows


def _partition_cost_attestation(level_id, namespace):
    """Materialize the genuine partition-local vector under frozen policy facts."""

    policy = _COST_LEVELS[level_id]["numerical_execution_cost_policy"]
    return build_numerical_execution_cost_attestation(
        use_spread=policy["use_spread"],
        spread_mode=policy["spread_mode"],
        entry_half_spread_values_sha256=_sha(
            f"{namespace}:{level_id}:entry-half-spread-values"
        ),
        intrabar_half_spread_values_sha256=_sha(
            f"{namespace}:intrabar-half-spread-values"
        ),
        instrument_spec_sha256=policy["instrument_spec_sha256"],
    )


def _result(
    start,
    end,
    namespace,
    *,
    cost_level="BASE",
    values=None,
):
    trades = _trades(start, values)
    position_namespace = "|".join(
        (
            TEST_INSTRUMENT_SPEC.symbol.upper(),
            OBSERVED_TIMEFRAME,
            "EMACrossoverStrategy",
        )
    )
    events = build_backtest_position_events(trades, namespace=position_namespace)
    trade_identity_pnl = build_backtest_trade_identity_pnl_evidence(trades)
    surface = raw_winner.build_flat_daily_r_surface(
        events,
        start_date_utc=start.isoformat(),
        end_date_utc=end.isoformat(),
        risk_unit_contract=RISK_CONTRACT,
    )
    axis = {
        "schema_version": "fxpm.backtest-partition-axis.v1",
        "timeframe": OBSERVED_TIMEFRAME,
        "start_utc": datetime.combine(
            start, datetime.min.time(), timezone.utc
        ).isoformat(),
        "end_utc": datetime.combine(
            end + timedelta(days=1), datetime.min.time(), timezone.utc
        ).isoformat(),
        "scored_bars": (end - start).days + 1,
        "warmup_bars": 0,
        "event_axis_sha256": _sha(f"{namespace}:axis"),
    }
    axis["horizon_receipt_sha256"] = _sha(axis)
    return {
        "trade_identity_pnl": trade_identity_pnl,
        "position_events": events,
        "winner_daily_r_surface": surface,
        "trade_power_partition_axis": axis,
        "execution_cost_timing_contract": {
            "swap_accrual": "causal_swap_accrual_v1",
        },
        "numerical_execution_cost_attestation": _partition_cost_attestation(
            cost_level, namespace
        ),
        "swap_accrual_contract": _swap(f"{namespace}:projection"),
    }


def _attempt_artifact(
    symbol,
    timeframe=OBSERVED_TIMEFRAME,
    regime="TREND",
    *,
    native_prefix_product,
    strategy_id="EMACrossoverStrategy",
    lookback_kind="CALENDAR_INTERVAL",
    lookback_value=120,
    fit_start_utc=None,
    declared_source_bar_count=None,
):
    prefix = native_prefix_product.native_prefix
    frame = native_prefix.open_verified_prefix_frame(native_prefix_product)
    opens = pd.DatetimeIndex(pd.to_datetime(frame.index, utc=True, errors="coerce"))
    closes = pd.DatetimeIndex(
        pd.to_datetime(frame["_BAR_CLOSE_TS"], utc=True, errors="coerce")
    )
    expected_start, expected_count = search_execution._realize_cutoff_refit_window(
        opens=opens,
        closes=closes,
        fit_end=pd.Timestamp(prefix["prefix_end_exclusive_utc"]),
        lookback_kind=lookback_kind,
        lookback_value=lookback_value,
        state_context_bars=0,
        boundary_limited=True,
    )
    artifact_start = (
        expected_start
        if fit_start_utc is None
        else pd.Timestamp(fit_start_utc).tz_convert("UTC")
    )
    artifact_count = int(
        (
            (opens >= artifact_start)
            & (opens < pd.Timestamp(prefix["prefix_end_exclusive_utc"]))
            & (closes <= pd.Timestamp(prefix["prefix_end_exclusive_utc"]))
        ).sum()
    )
    source_bar_count = (
        artifact_count
        if declared_source_bar_count is None
        else declared_source_bar_count
    )
    assert expected_count >= 2
    artifacts = current_refit.build_current_refit_search_artifacts_v2(
        symbol=symbol,
        timeframe=timeframe,
        regime=regime,
        strategy_id=strategy_id,
        parameters=StrategyRegistry.get(strategy_id).get_params(),
        lookback_kind=lookback_kind,
        lookback_value=lookback_value,
        fit_start_utc=artifact_start.isoformat(),
        fit_end_utc=prefix["prefix_end_exclusive_utc"],
        latest_closed_bar_utc=prefix["prefix_last_close_utc"],
        source_snapshot_sha256=prefix["prefix_content_sha256"],
        source_bar_count=source_bar_count,
        seed=7,
    )
    return {
        "winner_process": artifacts.winner_process,
        "horizon_process": artifacts.horizon_process,
        "trial_definition": artifacts.trial_definition,
        "refit_recipe": artifacts.refit_recipe,
    }


def _real_horizon_plan(
    anchor,
    symbol,
    regime,
    *,
    native_prefix_product,
    attempt_artifact,
    fold_plan,
    additional_attempt_artifacts=(),
    boundary_limited=False,
    actual_fold_row_counts=None,
):
    emitted_fold_plan = () if boundary_limited else fold_plan
    if actual_fold_row_counts is not None:
        if (type(actual_fold_row_counts) not in (list, tuple)
                or len(actual_fold_row_counts) != len(emitted_fold_plan)
                or any(type(pair) not in (list, tuple) or len(pair) != 2
                       or any(type(count) is not int or count < 1 for count in pair)
                       for pair in actual_fold_row_counts)):
            raise ValueError("actual fold row counts must cover every emitted fold")
    request = copy.deepcopy(horizon_fixture._request())
    request["run_id"] = "historical-run-001"
    request["anchor"] = {
        "anchor_id": anchor["anchor_id"],
        "ordinal": 0,
        "selection_cutoff_utc": anchor["selection_cutoff_utc"],
        "forward_start_utc": f"{anchor['forward_start_date']}T00:00:00Z",
        "forward_end_exclusive_utc": (f"{anchor['forward_end_date']}T00:00:00Z"),
        "evidence_accessed_at_utc": "2025-01-01T00:00:00Z",
        "process_precommit_receipt_sha256": PROCESS_PRECOMMIT,
    }
    request["cell"] = {
        "symbol": symbol,
        "timeframe": OBSERVED_TIMEFRAME,
        "regime": regime,
    }
    request["native_prefix"] = dict(native_prefix_product.native_prefix)
    artifact_roster = [attempt_artifact, *additional_attempt_artifacts]
    candidates = []
    for artifact in artifact_roster:
        horizon_process = artifact["horizon_process"]
        discovery_start_utc = native_prefix_product.native_prefix[
            "prefix_start_utc"
        ].replace("+00:00", "Z")
        folds = []
        for ordinal, planned in enumerate(emitted_fold_plan):
            validation_start = date.fromisoformat(planned["oos_start_date_utc"])
            validation_end = date.fromisoformat(
                planned["oos_end_date_utc"]
            ) + timedelta(days=1)
            fit_end = validation_start - timedelta(days=60)
            value = horizon_fixture._fold(
                f"ROLL-{ordinal + 1:03d}",
                "CAUSAL_ROLLING_ORIGIN_INNER",
                discovery_start_utc,
                f"{fit_end.isoformat()}T00:00:00Z",
                f"{(validation_start - timedelta(days=5)).isoformat()}T00:00:00Z",
                f"{validation_start.isoformat()}T00:00:00Z",
                f"{validation_end.isoformat()}T00:00:00Z",
            )
            fit_rows, validation_rows = ((50, 30) if actual_fold_row_counts is None
                                         else actual_fold_row_counts[ordinal])
            value["fit_rows"] = fit_rows
            value["validation_rows"] = validation_rows
            value.pop("fold_definition_sha256")
            value["fold_definition_sha256"] = _sha(value)
            folds.append(value)
        realized_start_utc = discovery_start_utc
        if boundary_limited:
            realized_start_utc = (
                (
                    datetime.fromisoformat(discovery_start_utc.replace("Z", "+00:00"))
                    - timedelta(days=1)
                )
                .isoformat(timespec="seconds")
                .replace("+00:00", "Z")
            )
        candidate_definition = {
            "candidate_id": (
                f"{horizon_process['lookback_kind']}-"
                f"{horizon_process['lookback_value']}"
            ),
            "lookback_kind": horizon_process["lookback_kind"],
            "lookback_value": horizon_process["lookback_value"],
            "realized_start_utc": realized_start_utc,
        }
        candidates.append(
            {
                **candidate_definition,
                "candidate_definition_sha256": _sha(candidate_definition),
                "folds": folds,
            }
        )
    request["candidates"] = candidates
    trials_by_identity = {}
    for artifact in artifact_roster:
        process = artifact["winner_process"]
        raw_trial = artifact["trial_definition"]
        identity = (
            process["strategy_id"],
            process["strategy_code_sha256"],
            _sha(raw_trial["parameters"]),
        )
        if identity in trials_by_identity:
            continue
        trial_definition = {
            "trial_id": f"TRIAL-{len(trials_by_identity) + 1:03d}",
            "strategy_id": identity[0],
            "strategy_release_sha256": identity[1],
            "parameter_set_sha256": identity[2],
        }
        trials_by_identity[identity] = {
            **trial_definition,
            "trial_definition_sha256": _sha(trial_definition),
        }
    request["trials"] = list(trials_by_identity.values())
    request["horizon_policy"]["cost_contract_sha256"] = _sha(
        COST_LADDER_CONTRACT["levels"][0]["numerical_execution_cost_policy"]
    )
    request["horizon_policy"]["cost_ladder_contract_sha256"] = _sha(
        COST_LADDER_CONTRACT
    )
    return training_horizon.build_historical_horizon_plan(request)


def _real_unobserved_horizon_plan(
    anchor,
    symbol,
    timeframe,
    regime,
    *,
    native_prefix_body,
    source_decision,
    fit_decision,
    frozen_tier_pair_contract=None,
    evidence_accessed_at_utc="2025-01-01T00:00:00Z",
    horizon_anchor=None,
):
    request = copy.deepcopy(horizon_fixture._unobserved_request())
    request["run_id"] = "historical-run-001"
    request["anchor"] = {
        "anchor_id": anchor["anchor_id"],
        "ordinal": 0,
        "selection_cutoff_utc": anchor["selection_cutoff_utc"],
        "forward_start_utc": f"{anchor['forward_start_date']}T00:00:00Z",
        "forward_end_exclusive_utc": f"{anchor['forward_end_date']}T00:00:00Z",
        "evidence_accessed_at_utc": evidence_accessed_at_utc,
        "process_precommit_receipt_sha256": PROCESS_PRECOMMIT,
    }
    if horizon_anchor is not None:
        request["anchor"] = copy.deepcopy(horizon_anchor)
    request["cell"] = {
        "symbol": symbol,
        "timeframe": timeframe,
        "regime": regime,
    }
    request["native_prefix"] = copy.deepcopy(native_prefix_body)
    fit_body = fit_decision.to_dict()
    support = copy.deepcopy(fit_body["train_regime_support"])
    binding_body = {
        "schema_version": "fxpm.historical-horizon-regime-support-binding.v1",
        "source_group_decision_sha256": source_decision.receipt_sha256,
        "regime_fit_decision_sha256": fit_decision.receipt_sha256,
        "regime_detector_record_sha256": fit_body["regime_detector_record_sha256"],
        "train_regime_support_receipt_sha256": support["receipt_sha256"],
        "target_regime": regime,
        "target_regime_count": 0,
        "train_regime_support": support,
    }
    request["regime_support_binding"] = {
        **binding_body,
        "binding_sha256": _sha(binding_body),
    }
    if frozen_tier_pair_contract is not None:
        request["schema_version"] = "fxpm.historical-horizon-unobserved-request.v3"
        request["horizon_policy"] = {"schema_version": "fxpm.historical-horizon-policy.v3",
            "policy_id": "UNISSUED_SYNTHETIC_ENGINEERING_ONLY", "policy_release_sha256": "b" * 64,
            "cost_contract_sha256": _sha({tier["tier"]: _sha(tier["numerical_execution_cost_policy"])
                for tier in frozen_tier_pair_contract["tiers"]}),
            "frozen_tier_pair_contract_sha256": _sha(frozen_tier_pair_contract),
            "evaluator_release_sha256": search_execution.AUTHORITATIVE_SEARCH_EXECUTION_RELEASE_SHA256}
    return training_horizon.build_historical_horizon_plan(request)


def _fold_plan():
    starts = [date(2018, 5, 1) + timedelta(days=63 * index) for index in range(4)]
    return [
        {
            "anchor_as_of_utc": f"{start.isoformat()}T00:00:00+00:00",
            "oos_start_date_utc": start.isoformat(),
            "oos_end_date_utc": (start + timedelta(days=62)).isoformat(),
        }
        for start in starts
    ]


def _current_capture_from_case(case, tmp_path, *, anchor_id=None, freeze=True):
    """Actual adapter/raw mechanics under an explicitly unissued control carrier."""
    from tests import test_winner_raw_evidence as raw_fixture

    cloned = raw_fixture.clone_current_native_case(case, tmp_path)
    context = cloned["context"]
    plan = cloned["plan"]
    fitted = cloned["fitted"]
    plan_body = plan.to_dict()
    anchor = _anchor(anchor_id or plan_body["anchor"]["anchor_id"],
                     pd.Timestamp(plan_body["anchor"]["selection_cutoff_utc"]).date())
    anchor["selection_cutoff_utc"] = plan_body["anchor"]["selection_cutoff_utc"]
    selected_owner = case.get("selected_owner_case", False)
    target_timeframe = case.get("timeframe", "D1")
    if selected_owner:
        start = pd.Timestamp(plan_body["anchor"]["forward_start_utc"])
        finish = pd.Timestamp(plan_body["anchor"]["forward_end_exclusive_utc"])
        anchor.update({"forward_start_date": start.date().isoformat(),
            "forward_end_date": (finish.date() - timedelta(days=1)).isoformat(),
            "purge_calendar_days": (start.date() - pd.Timestamp(anchor["selection_cutoff_utc"]).date()).days})
        cloned["control_carrier_geometry"] = "UNISSUED_INELIGIBLE_FOR_OLD_ISSUED_ANCHOR_POLICY"
    request = _request(context, anchor)
    request_body = request.to_dict()
    request_body["search_process_receipt_sha256"] = "d" * 64
    request = orchestration.AnchorProducerRequest(_canonical(request_body), request._symbol_sinks)
    capture = adapter.open_historical_symbol_capture(context, request, OBSERVED_SYMBOL)
    target_regime = plan_body["cell"]["regime"]
    support = fitted.train_regime_support["counts"]
    for timeframe in book.TIMEFRAME_ORDER:
        if timeframe != target_timeframe:
            source, fit = _terminal_authority(anchor["anchor_id"], OBSERVED_SYMBOL, timeframe)
            plans = _plans(anchor, OBSERVED_SYMBOL, timeframe, source_available=False, current=True,
                horizon_anchor=plan_body["anchor"] if selected_owner else None)
        else:
            source, fit = fitted.source_group_decision, fitted.regime_fit_decision
            plans = []
            for regime in book.REGIME_ORDER:
                if regime == target_regime:
                    plans.append(plan)
                elif support[regime] == 0:
                    plans.append(_real_unobserved_horizon_plan(anchor, OBSERVED_SYMBOL, timeframe, regime,
                        native_prefix_body=dict(fitted.product.native_prefix), source_decision=source,
                        fit_decision=fit, frozen_tier_pair_contract=fitted.frozen_tier_pair_contract,
                        evidence_accessed_at_utc=plan_body["anchor"]["evidence_accessed_at_utc"],
                        horizon_anchor=plan_body["anchor"] if selected_owner else None))
                else:
                    boundary = copy.deepcopy(case["horizon_request"])
                    boundary["cell"]["regime"] = regime
                    boundary["candidates"] = [horizon_fixture._candidate("unavailable-deep-history", "CALENDAR_INTERVAL", 999999,
                        native_prefix_fixture._iso(pd.Timestamp(fitted.product.native_prefix["prefix_start_utc"])
                            - pd.Timedelta(days=1)), [])]
                    plans.append(training_horizon.build_historical_horizon_plan(boundary))
        capture.bind_timeframe(timeframe, horizon_plans=plans,
            source_group_decision=source, regime_fit_decision=fit)
    if not freeze:
        return cloned, request, capture, None, None
    precommitted_at = _observed_utc_after() if selected_owner else "2025-01-02T00:00:00Z"
    frozen_at = _observed_utc_after(precommitted_at) if selected_owner else "2025-01-03T00:00:00Z"
    handle = capture.freeze_observed_cell(target_timeframe, target_regime, native_prefix_product=case["native_product"],
        precommitted_at_utc=precommitted_at, attempts_frozen_at_utc=frozen_at,
        fold_plan=case["fold_plan"], attempt_artifacts=case["attempt_artifacts"],
        source_contract_seeds=fitted.source_contract_seeds, frozen_tier_pair_contract=fitted.frozen_tier_pair_contract,
        partition_cost_precommitments=case["partition_cost_precommitments"], risk_unit_contract=RISK_CONTRACT)
    if case.get("evidence_deferred", False):
        started = _observed_utc_after(frozen_at)
        raw_ids = search_execution._raw_attempt_ids(handle.frozen, case["verified_attempts"])
        if "frozen_pipeline_config" in case:
            frozen_config = case["frozen_pipeline_config"]
            assert cloned["frozen_pipeline_config"] is frozen_config
            assert frozen_config.effective_config_sha256 == case["effective_config_sha256"]
            config_bytes_before = hashlib.sha256(frozen_config.active_config_path.read_bytes()).hexdigest()
            assert config_bytes_before == case["bound_config_bytes_sha256"]
            broker_before = hashlib.sha256(case["bound_broker_path"].read_bytes()).hexdigest()
            assert broker_before == case["bound_broker_sha256"]
            original_config = frozen_config.materialize(case["context"])
            config = frozen_config.materialize(cloned["context"])
            assert asdict(original_config) == asdict(config) == case["selection_config_snapshot"]
            assert asdict(case["config"]) == case["selection_config_snapshot"]
            assert frozen_config.effective_config_sha256 == case["effective_config_sha256"]
            broker_after = hashlib.sha256(case["bound_broker_path"].read_bytes()).hexdigest()
            assert broker_before == broker_after
            assert hashlib.sha256(frozen_config.active_config_path.read_bytes()).hexdigest() == config_bytes_before
            cloned.update({"config": config, "deferred_broker_sha256": broker_after,
                "deferred_config_bytes_sha256": config_bytes_before,
                "deferred_effective_config_sha256": frozen_config.effective_config_sha256})
            evidence = [search_execution._build_planned_attempt_evidence(attempt=attempt, raw_attempt_id=raw_id,
                plan_body=plan_body, plan_receipt_sha256=plan.receipt_sha256, fitted=fitted, config=config,
                partition_cost_precommitments=case["partition_cost_precommitments"], source_observation_references=None)
                for attempt, raw_id in zip(case["verified_attempts"], raw_ids, strict=True)]
        else:
            evidence = [search_execution._build_planned_attempt_evidence(attempt=attempt, raw_attempt_id=raw_id,
                plan_body=plan_body, plan_receipt_sha256=plan.receipt_sha256, fitted=fitted, config=case["config"],
                partition_cost_precommitments=case["partition_cost_precommitments"], source_observation_references=None)
                for attempt, raw_id in zip(case["verified_attempts"], raw_ids, strict=True)]
        cloned.update({"evidence_started_at_utc": started,
            "evidence_completed_at_utc": datetime.now(timezone.utc).isoformat(), "evidence": evidence})
    else:
        evidence = copy.deepcopy(case["evidence"])
    for item, attempt in zip(evidence, handle.frozen.attempt_ledger["attempts"], strict=True):
        item["attempt_id"] = attempt["attempt_id"]
    return cloned, request, capture, handle, evidence


def test_symbol_capture_uses_raw_winner_and_empirical_unobserved_states(tmp_path, current_native_case):
    case, request, capture, handle, evidence = _current_capture_from_case(current_native_case, tmp_path)
    assert case["qualification_authority"] is False
    assert handle._horizon_execution_contracts[0]["source_bar_count"] == 500
    count = handle._source_count_receipts[0]
    assert count["native_prefix_index_sha256"] == case["native_product"].index_sha256
    assert count["fit_start_utc"] != case["native_product"].native_prefix["prefix_start_utc"]
    production = capture.complete_observed_cell(handle, attempt_evidence=evidence,
        evidence_access_started_at_utc=case["evidence_started_at_utc"],
        evidence_access_completed_at_utc=case["evidence_completed_at_utc"])
    assert production.request.to_dict()["contract_version"] == winner.CURRENT_WINNER_SELECTION_REQUEST_VERSION
    assert production.decision.status == "NO_PROCESS_WINNER"
    product = _finish_through_worker_bridge(case["context"], capture.anchor, capture)
    assert len(product.horizon_plans) == len(product.book_evidence) == 24
    assert all(training_horizon._valid_plan(plan) for plan in product.horizon_plans)
    assert {plan.to_dict()["schema_version"] for plan in product.horizon_plans} == {
        "fxpm.historical-horizon-plan.v3"}
    bindings = [orchestration.historical_horizon_plan_binding(plan) for plan in product.horizon_plans]
    assert {binding["schema_version"] for binding in bindings} == {
        orchestration.CURRENT_HORIZON_BINDING_SCHEMA}
    for plan in product.horizon_plans:
        body = plan.to_dict()
        if body["cell"]["timeframe"] != "D1":
            assert body["plan_status"] == "HISTORICAL_SOURCE_UNAVAILABLE"
            assert body["attempts"] == body["expected_evaluations"] == []
            assert body["horizon_policy"]["cost_contract_sha256"] != (
                body["horizon_policy"]["frozen_tier_pair_contract_sha256"])
    payload = product.optimizer_payload
    assert payload["cell_count"] == 24
    assert payload["confers_causal_current_live_or_capital_authority"] is False
    assert payload["raw_artifact_namespace"] == capture.raw_artifact_namespace
    manifest = spool.verify_symbol_receipt(case["context"].root, request.symbol_sink(OBSERVED_SYMBOL).seal(),
        expected_generation_id=case["context"].generation_id, expected_symbol=OBSERVED_SYMBOL,
        expected_native_universe_sha256=case["context"].native_universe_sha256,
        expected_config_sha256=case["context"].config_sha256)
    by_id = {row["artifact_id"]: row for row in manifest["artifacts"]}
    root_graph = _read_descriptor_json(case["context"], payload["raw_descriptor_graph"])
    for row in root_graph["artifacts"]:
        assert by_id[row["artifact_id"]] == row
        if row["artifact_type"] == "historical_raw_winner_descriptor_graph":
            graph = _read_descriptor_json(case["context"], row["descriptor"])
            assert all(by_id[leaf["artifact_id"]] == leaf for leaf in graph["artifacts"])
    selected = next(cell for cell in product.book_evidence if cell.regime == handle.regime and cell.timeframe == "D1")
    assert selected.winner_decision.to_dict() == production.decision.to_dict()
    assert selected.winner_request.request_sha256 == production.request.request_sha256

    # Remaining symbols are invented terminal test carriers, not measured universe absence.
    from tests import test_book_multiplicity as book_fixture
    family = tuple(product.book_evidence) + tuple(
        book_fixture._terminal_cell(ordinal) for ordinal in range(24, book.FAMILY_SIZE))
    assert family[:24] == tuple(product.book_evidence)
    result = book.evaluate_frozen_tier_historical_book_multiplicity(family)
    assert result.valid, result.reason_codes
    assert book.verify_historical_book_multiplicity_decision(result, family)
    result_body = result.to_dict()
    assert result_body["contract_version"] == "fxpm.book-multiplicity.v15"
    assert result_body["status"] == "NO_LOCAL_WINNERS"
    assert result_body["family_size"] == book.FAMILY_SIZE == 2400
    cell = next(item for item in result_body["cells"]
        if (item["symbol"], item["timeframe"], item["regime"]) == (
            OBSERVED_SYMBOL, "D1", handle.regime))
    assert cell["winner_request_sha256"] == production.request.request_sha256
    assert cell["winner_decision_sha256"] == production.decision.receipt_sha256
    for key in ("causal_process_validated", "live_authorized", "order_authorized",
                "capital_sizing_authorized", "profitability_claimed", "production_integrated"):
        assert result_body[key] is False


def test_declared_native_two_trial_selected_owner_feasibility(tmp_path, selected_owner_native_case, monkeypatch):
    """One synthetic path control; no qualification evidence or PBO power claim."""
    import pm_forward_oos_replay_producer as forward
    from tests import test_book_multiplicity as book_fixture

    observations = []
    execute = search_execution._run_tier_partition

    def observe_actual_partition(**arguments):
        output = execute(**arguments)
        result, features, signals, _ = output
        partition = arguments["partition"]
        raw = arguments["full_prefix_surface"].raw_signals.loc[features.index]
        scored = raw.iloc[partition.warmup_bars:]
        lane = f"{partition.role}_{arguments['spread_tier'].tier.lower()}"
        normalized = search_execution._decision_relevant_result(result, lane)
        relevant = json.loads(_canonical(raw_winner._decision_relevant_lane(normalized, lane=lane)))
        observations.append({"strategy_id": type(arguments["strategy"]).__name__, "lane": lane,
            "scored_start_utc": partition.scored_start_utc,
            "end_exclusive_utc": partition.end_exclusive_utc,
            "scored_raw_signal_count": len(scored), "scored_raw_nonzero": int(scored.ne(0).sum()),
            "scored_raw_signal_sha256": hashlib.sha256(scored.to_numpy(dtype="<i4").tobytes()).hexdigest(),
            "engine_signal_count": len(signals), "engine_nonzero": int(signals.ne(0).sum()),
            "engine_signal_sha256": hashlib.sha256(signals.to_numpy(dtype="<i4").tobytes()).hexdigest(),
            "completed_trades": len(result["trades"]),
            "position_events": len(result["position_events"]), "raw_lane": relevant})
        return output

    try:
        with monkeypatch.context() as observer:
            observer.setattr(search_execution, "_run_tier_partition", observe_actual_partition)
            case, request, capture, handle, evidence = _current_capture_from_case(selected_owner_native_case, tmp_path)
    except Exception as exc:
        actual = {"fixture_provenance": "SYNTHETIC_SELECTED_OWNER_PARTIAL_ENGINEERING_OBSERVATIONS",
            "qualification_authority": False, "capture_completed": False,
            "failure_observed_at_utc": datetime.now(timezone.utc).isoformat(),
            "error_type": type(exc).__name__, "error": str(exc), "observed_lanes": observations,
            "input_generation_id": selected_owner_native_case["context"].generation_id,
            "input_plan_receipt_sha256": selected_owner_native_case["plan"].receipt_sha256}
        (tmp_path / "selected-owner-result.json").write_bytes(_canonical(actual))
        raise
    actual_evidence = {"schema_version": "fxpm.selected-owner-actual-attempt-evidence.v1",
        "fixture_provenance": "SYNTHETIC_ENGINEERING_ONLY_NO_QUALIFICATION_OR_TRADING_AUTHORITY",
        "qualification_authority": False,
        "generation_id": case["context"].generation_id,
        "generation_context_sha256": case["context"].context_sha256,
        "native_universe_sha256": case["context"].native_universe_sha256,
        "config_sha256": case["context"].config_sha256,
        "plan_receipt_sha256": case["plan"].receipt_sha256,
        "evidence_access_started_at_utc": case["evidence_started_at_utc"],
        "evidence_access_completed_at_utc": case["evidence_completed_at_utc"],
        "attempt_evidence": evidence, "observed_lanes": observations}
    actual_evidence_bytes = _canonical(actual_evidence)
    actual_evidence_path = tmp_path / "actual-attempt-evidence.json"
    actual_evidence_path.write_bytes(actual_evidence_bytes)
    assert case["qualification_authority"] is False
    assert case["control_carrier_geometry"] == "UNISSUED_INELIGIBLE_FOR_OLD_ISSUED_ANCHOR_POLICY"
    plan = case["plan"].to_dict()
    assert plan["cell"] == {"symbol": OBSERVED_SYMBOL, "timeframe": "H1", "regime": "TREND"}
    assert len(case["verified_attempts"]) == len(evidence) == 2
    assert {item["winner_process"]["strategy_id"] for item in case["attempt_artifacts"]} == {
        "EMACrossoverStrategy", "VolumeSpikeMomentumStrategy"}
    assert all(attempt["precommitted_evidence_role"] == "EVALUATION_REQUIRED"
               for attempt in handle.frozen.attempt_ledger["attempts"])
    required = {f"{partition}_{tier}" for partition in (
        "optimizer_train", "tuning_validation", "final_lockbox") for tier in ("p75", "p95")}
    diagnostic = {"fixture_provenance": "SYNTHETIC_SELECTED_OWNER_FLAT_PATH_CONTROL_NO_TRADING_AUTHORITY",
        "qualification_authority": False, "profitability_claimed": False, "pbo_discrimination_claimed": False,
        "frozen_profile": case["frozen_profile"], "source_root": str(case["native_product"]._data_dir / "native"),
        "control_carrier_geometry": case["control_carrier_geometry"], "anchor": capture.anchor,
        "horizon_plan_sha256": case["plan"].receipt_sha256,
        "native_product_index": dict(case["native_product"].index_descriptor),
        "native_product_index_sha256": case["native_product"].index_sha256,
        "native_prefix": dict(case["native_product"].native_prefix),
        "source_group_decision_sha256": case["fitted"].source_group_decision.receipt_sha256,
        "regime_fit_decision_sha256": case["fitted"].regime_fit_decision.receipt_sha256,
        "train_regime_support_receipt_sha256": case["fitted"].train_regime_support["receipt_sha256"],
        "regime_fit_policy": "ONE_EXISTING_DETERMINISTIC_FIT_RECOMPUTED_AND_PINNED_NOT_REHYDRATED",
        "prospective_run_id": plan["run_id"], "precommitted_attempts": handle.frozen.attempt_ledger["attempts"],
        "observed_lanes": [{key: value for key, value in row.items() if key != "raw_lane"}
                           for row in observations],
        "actual_attempt_evidence": {"path": str(actual_evidence_path),
            "sha256": hashlib.sha256(actual_evidence_bytes).hexdigest(), "bytes": len(actual_evidence_bytes)},
        "failures": []}
    diagnostic_path = tmp_path / "selected-owner-result.json"

    def save_diagnostic():
        diagnostic_path.write_bytes(_canonical(diagnostic))

    def differing_paths(left, right, prefix=""):
        if type(left) is not type(right):
            return [prefix or "$type"]
        if type(left) is dict:
            changed = []
            for key in sorted(set(left) | set(right)):
                path = f"{prefix}.{key}" if prefix else key
                changed.extend([path] if key not in left or key not in right else
                               differing_paths(left[key], right[key], path))
            return changed
        if type(left) is list:
            if len(left) != len(right):
                return [f"{prefix}.length"]
            return [path for ordinal, (a, b) in enumerate(zip(left, right, strict=True))
                    for path in differing_paths(a, b, f"{prefix}[{ordinal}]")]
        return [] if left == right else [prefix]

    ema_rows = [row for row in observations if row["strategy_id"] == "EMACrossoverStrategy"]
    flat_rows = [row for row in observations if row["strategy_id"] == "VolumeSpikeMomentumStrategy"]
    if len(observations) != 48 or len(ema_rows) != 24 or len(flat_rows) != 24:
        diagnostic["failures"].append("ACTUAL_ENGINE_LANE_ROSTER_INVALID")
    for rows in (ema_rows, flat_rows):
        expected = [f"{partition}_{tier}" for partition in (
            "optimizer_train", "tuning_validation", "final_lockbox") for tier in ("p75", "p95")] * 4
        if [row["lane"] for row in rows] != expected:
            diagnostic["failures"].append("ACTUAL_ENGINE_PARTITION_ORDER_INVALID")
    diagnostic["flat_comparator_proofs"] = []
    for ordinal, row in enumerate(flat_rows):
        diagnostic["flat_comparator_proofs"].append({"fold": ordinal // 6, "lane": row["lane"],
            "scored_raw_nonzero": row["scored_raw_nonzero"], "engine_nonzero": row["engine_nonzero"],
            "completed_trades": row["completed_trades"], "position_events": row["position_events"]})
        if (row["scored_raw_signal_count"] <= 0 or row["scored_raw_nonzero"]
                or row["engine_nonzero"] or row["completed_trades"] or row["position_events"]):
            diagnostic["failures"].append(f"FLAT_COMPARATOR_NOT_FLAT:{ordinal}")
    save_diagnostic()
    assert not diagnostic["failures"], diagnostic["failures"]

    original = (Path(__file__).resolve().parents[1] / "audit_reports/fxpm_workshop_20261007"
        / "selected_owner_geometry_control/runtime/attempt_002_pytest_tmp"
        / "test_declared_native_two_trial0/spool")
    manifest_relative = ("symbols/af1c64037373198aa9f2805a63b3d1ae4cfddc9a70b971e23425e06658c2ed6f"
        "/domains/b15f584799e6d595cd91f88524a58fb6d0b4ea073d79cc381048d1f3ba660225/manifest.json")
    original_manifest_bytes = (original / manifest_relative).read_bytes()
    original_manifest_sha = hashlib.sha256(original_manifest_bytes).hexdigest()
    diagnostic["ema_reference_manifest_sha256"] = original_manifest_sha
    save_diagnostic()
    assert original_manifest_sha == "dfb6f8d125c80aedb094eae93b9e2f0782b4e3d6c24af7ca1c75a81823566a82"
    original_manifest = json.loads(original_manifest_bytes)
    original_context = spool.reopen_generation_context(original,
        expected_generation_id=original_manifest["generation_id"],
        expected_native_universe_sha256=original_manifest["native_universe_sha256"],
        expected_config_sha256=original_manifest["config_sha256"])
    original_receipt = {key: original_manifest[key] for key in (
        "generation_id", "symbol", "manifest_domain", "native_universe_sha256",
        "config_sha256", "generation_context_sha256")}
    original_receipt.update({"schema_version": spool.COMPACT_RECEIPT_SCHEMA,
        "symbol_manifest": {"schema_version": spool.ARTIFACT_DESCRIPTOR_SCHEMA,
            "relative_path": manifest_relative, "sha256": original_manifest_sha,
            "size_bytes": len(original_manifest_bytes), "format": spool.CANONICAL_JSON_FORMAT}})
    _, original_entries = forward._verified_manifest(original_context, original_receipt, OBSERVED_SYMBOL)
    reference_prefix = ("historical-anchor/historical-run-001/"
        "unissued-selected-owner-anchor-64d-control/winner-search/H1/TREND/attempt-0000")
    comparisons = []
    for ordinal, row in enumerate(ema_rows):
        artifact_id = f"{reference_prefix}/fold-{ordinal // 6:03d}/raw/{row['lane']}"
        reference = forward._manifest_body(original_context, original_entries, artifact_id,
            code="RETAINED_QA97_EMA_RAW_LANE")
        actual = row["raw_lane"]
        changes = differing_paths(reference, actual)
        comparison = {"fold": ordinal // 6, "lane": row["lane"],
            "reference_descriptor": original_entries[artifact_id]["descriptor"],
            "actual_raw_lane_sha256": _sha(actual), "unexpected_changed_paths": changes}
        if changes:
            comparison.update({"reference_raw_lane": reference, "actual_raw_lane": actual})
            diagnostic["failures"].append(f"EMA_REFERENCE_CHANGED:{ordinal}:{','.join(changes)}")
        comparisons.append(comparison)
    diagnostic["ema_raw_lane_comparison"] = comparisons
    save_diagnostic()
    assert not diagnostic["failures"], diagnostic["failures"]
    assert len(comparisons) == 24

    admissions = []
    for attempt, declared in zip(evidence, handle.frozen.attempt_ledger["attempts"], strict=True):
        assert len(attempt["folds"]) == 4
        for fold in attempt["folds"]:
            assert set(fold["lanes"]) == required
            signals = fold["provenance"]["attempt_replay_receipt"]["signal_vector_receipts"]
            for partition in ("optimizer_train", "tuning_validation", "final_lockbox"):
                p75, p95 = (fold["lanes"][f"{partition}_{tier}"] for tier in ("p75", "p95"))
                assert p75 is not p95
                if declared["strategy_id"] == "VolumeSpikeMomentumStrategy":
                    assert signals[partition]["observation_count"] > 0
                    assert all(value == 0 for value in signals[partition]["values"])
                    assert all(lane["trade_identity_pnl"] == lane["position_events"] == [] for lane in (p75, p95))
                for tier, lane in (("P75", p75), ("P95", p95)):
                    assert lane["spread_admission"]["missing_executed_legs"] == 0
                    admissions.append({"attempt_id": attempt["attempt_id"], "strategy_id": declared["strategy_id"],
                        "fold": fold["fold_ordinal"], "partition": partition, "tier": tier,
                        "admission": lane["spread_admission"], "completed_trades": len(lane["trade_identity_pnl"])})
                assert (p75["spread_admission"]["entry_observations_sha256"]
                        == p95["spread_admission"]["entry_observations_sha256"])
                assert (p75["spread_admission"]["entry_admission_mask_sha256"]
                        != p95["spread_admission"]["entry_admission_mask_sha256"])
    production = capture.complete_observed_cell(handle, attempt_evidence=evidence,
        evidence_access_started_at_utc=case["evidence_started_at_utc"],
        evidence_access_completed_at_utc=case["evidence_completed_at_utc"])
    assert production.request.to_dict()["contract_version"] == winner.CURRENT_WINNER_SELECTION_REQUEST_VERSION
    assert all(row["evidence_status"] == "AUTHENTICATED" for row in production.evidence_ledger["entries"])
    product = _finish_through_worker_bridge(case["context"], capture.anchor, capture)
    assert len(product.horizon_plans) == len(product.book_evidence) == 24
    assert all(item.to_dict()["anchor"] == plan["anchor"] for item in product.horizon_plans)
    receipt = request.symbol_sink(OBSERVED_SYMBOL).seal()
    manifest = spool.verify_symbol_receipt(case["context"].root, receipt,
        expected_generation_id=case["context"].generation_id, expected_symbol=OBSERVED_SYMBOL,
        expected_native_universe_sha256=case["context"].native_universe_sha256,
        expected_config_sha256=case["context"].config_sha256)
    assert any(row["artifact_type"] == "winner_v15_request" for row in manifest["artifacts"])
    observed = next(cell for cell in product.book_evidence if cell.timeframe == "H1" and cell.regime == "TREND")
    assert observed.winner_request.request_sha256 == production.request.request_sha256
    assert observed.winner_decision.to_dict() == production.decision.to_dict()
    family = tuple(product.book_evidence) + tuple(
        book_fixture._terminal_cell(ordinal) for ordinal in range(24, book.FAMILY_SIZE))
    result = book.evaluate_frozen_tier_historical_book_multiplicity(family)
    book_body = result.to_dict()
    target_cell_id = book.compute_cell_id(OBSERVED_SYMBOL, "H1", "TREND")
    target = next(cell for cell in book_body["cells"] if cell["cell_id"] == target_cell_id)
    ema_attempt = next(attempt for attempt in production.request.attempt_ledger.to_dict()["attempts"]
                       if attempt["strategy_id"] == "EMACrossoverStrategy")
    diagnostic.update({"admissions": admissions, "winner_decision": production.decision.to_dict(),
        "winner_request_sha256": production.request.request_sha256,
        "attempt_ledger_sha256": _sha(production.request.attempt_ledger.to_dict()),
        "evidence_ledger_sha256": _sha(production.request.evidence_ledger.to_dict()),
        "book_decision": book_body, "book_valid": result.valid,
        "book_verifier_valid": book.verify_historical_book_multiplicity_decision(result, family),
        "target_book_cell": target, "selected_ema_attempt": ema_attempt,
        "symbol_receipt": receipt, "manifest_artifact_count": len(manifest["artifacts"]),
        "adapter_index_descriptor": product.optimizer_payload["production_index"],
        "adapter_anchor": capture.anchor, "owner_reopen": {"status": "NOT_ATTEMPTED"}})
    save_diagnostic()
    assert result.valid, result.reason_codes
    assert diagnostic["book_verifier_valid"]
    assert book_body["contract_version"] == "fxpm.book-multiplicity.v15"
    for key in ("causal_process_validated", "live_authorized", "order_authorized",
                "capital_sizing_authorized", "profitability_claimed", "production_integrated"):
        assert book_body[key] is False
    assert production.decision.status == "PROCESS_WINNER_PENDING_BOOK_MULTIPLICITY", production.decision.to_dict()
    assert book_body["status"] == "HISTORICAL_LOCAL_CANDIDATES_AVAILABLE"
    assert target["historical_state"] == "WINNER"
    assert target["local_process_candidate_id"] == ema_attempt["process_candidate_id"]
    assert target["local_process_sha256"] == ema_attempt["process_sha256"]
    assert target["winner_request_sha256"] == production.request.request_sha256
    assert target["winner_decision_sha256"] == production.decision.receipt_sha256
    assert target["winner_attempt_ledger_payload_sha256"] == diagnostic["attempt_ledger_sha256"]
    assert target["winner_evidence_ledger_payload_sha256"] == diagnostic["evidence_ledger_sha256"]
    detector_record = case["fitted"].regime_fit_decision.to_dict()["regime_detector_record"]
    cutoff_proof = {"detector_record": detector_record,
        "fit_cutoff_utc": detector_record["fit_cutoff_utc"],
        "selection_cutoff_utc": capture.anchor["selection_cutoff_utc"],
        "native_prefix_end_exclusive_utc": case["native_product"].native_prefix["prefix_end_exclusive_utc"]}
    diagnostic["owner_cutoff_proof"] = cutoff_proof
    save_diagnostic()
    try:
        specialist = forward._reopen_owner_capture(context=case["context"], anchor=capture.anchor,
            book_body=book_body, cell_id=target_cell_id, symbol_receipt=receipt,
            adapter_index_descriptor=product.optimizer_payload["production_index"])
    except forward.ForwardOOSReplayProductionError as exc:
        diagnostic["owner_reopen"] = {"status": "REFUSED", "code": exc.code, "details": list(exc.details)}
        save_diagnostic()
        raise
    diagnostic["owner_reopen"] = {"status": "VERIFIED", "full_success_condition_met": True, "cell_id": specialist.cell_id,
        "strategy_id": specialist.strategy_id, "process_candidate_id": specialist.process_candidate_id,
        "process_sha256": specialist.process_sha256, "anchor_id": specialist.anchor_id,
        "source_count_receipt": specialist.source_count_receipt,
        "detector_parameters_sha256": specialist.detector_parameters_sha256}
    save_diagnostic()
    assert (specialist.cell_id, specialist.symbol, specialist.timeframe, specialist.regime,
            specialist.strategy_id, specialist.process_candidate_id, specialist.process_sha256, specialist.anchor_id) == (
        target_cell_id, OBSERVED_SYMBOL, "H1", "TREND", "EMACrossoverStrategy",
        ema_attempt["process_candidate_id"], ema_attempt["process_sha256"], capture.anchor["anchor_id"])
    assert (pd.Timestamp(specialist.detector_record["fit_cutoff_utc"])
            == pd.Timestamp(specialist.source_count_receipt["fit_end_exclusive_utc"])
            <= pd.Timestamp(capture.anchor["selection_cutoff_utc"]))
    assert any(row["tier"] == "P75" and row["admission"]["known_cap_refused_opportunities"] > 0
               for row in admissions)
    assert any(row["strategy_id"] == "EMACrossoverStrategy" and row["tier"] == "P95"
               and row["completed_trades"] > 0 for row in admissions)



def test_zero_support_cannot_be_inferred_from_a_ready_plan(
    tmp_path, observed_native_prefix
):
    context = _context(tmp_path)
    anchor = _anchor()
    request = _request(context, anchor)
    symbol = OBSERVED_SYMBOL
    capture = adapter.open_historical_symbol_capture(context, request, symbol)
    for timeframe in book.TIMEFRAME_ORDER[:-1]:
        source, fit = _terminal_authority(anchor["anchor_id"], symbol, timeframe)
        capture.bind_timeframe(
            timeframe,
            horizon_plans=_plans(anchor, symbol, timeframe, source_available=False),
            source_group_decision=source,
            regime_fit_decision=fit,
        )
    source, fit = _available_authority(
        symbol,
        OBSERVED_TIMEFRAME,
        _sha("ready-plan-source"),
        native_prefix_body=observed_native_prefix.native_prefix,
    )
    with pytest.raises(
        adapter.HistoricalProductionAdapterError,
        match="UNOBSERVED_REGIME_HORIZON_STATUS_INVALID",
    ):
        capture.bind_timeframe(
            OBSERVED_TIMEFRAME,
            horizon_plans=_plans(
                anchor, symbol, OBSERVED_TIMEFRAME, source_available=True
            ),
            source_group_decision=source,
            regime_fit_decision=fit,
        )


def test_boundary_limited_observed_cell_becomes_bound_insufficient_evidence(
    tmp_path, observed_native_prefix
):
    context = _context(tmp_path)
    anchor = _anchor()
    request = _request(context, anchor)
    symbol = OBSERVED_SYMBOL
    capture = adapter.open_historical_symbol_capture(context, request, symbol)
    artifact = _attempt_artifact(symbol, native_prefix_product=observed_native_prefix)
    boundary_plan = _real_horizon_plan(
        anchor,
        symbol,
        "TREND",
        native_prefix_product=observed_native_prefix,
        attempt_artifact=artifact,
        fold_plan=_fold_plan(),
        boundary_limited=True,
    )
    assert boundary_plan.to_dict()["plan_status"] == "HISTORICAL_BOUNDARY_LIMITED"
    for timeframe in book.TIMEFRAME_ORDER:
        if timeframe == OBSERVED_TIMEFRAME:
            source, fit = _available_authority(
                symbol,
                timeframe,
                _sha("boundary-source"),
                native_prefix_body=observed_native_prefix.native_prefix,
            )
            plans = _plans(anchor, symbol, timeframe, source_available=True)
            plans[0] = boundary_plan
            for ordinal, regime in enumerate(book.REGIME_ORDER[1:], start=1):
                plans[ordinal] = _real_unobserved_horizon_plan(
                    anchor,
                    symbol,
                    timeframe,
                    regime,
                    native_prefix_body=observed_native_prefix.native_prefix,
                    source_decision=source,
                    fit_decision=fit,
                )
        else:
            source, fit = _terminal_authority(anchor["anchor_id"], symbol, timeframe)
            plans = _plans(anchor, symbol, timeframe, source_available=False)
        capture.bind_timeframe(
            timeframe,
            horizon_plans=plans,
            source_group_decision=source,
            regime_fit_decision=fit,
        )
    product = _finish_through_worker_bridge(context, anchor, capture)
    trend = next(
        cell
        for cell in product.book_evidence
        if cell.timeframe == OBSERVED_TIMEFRAME and cell.regime == "TREND"
    )
    state = trend.process_state_decision.to_dict()
    binding = state["insufficient_evidence_binding"]
    assert state["state_kind"] == book.INSUFFICIENT_EVIDENCE
    assert state["attempted_trials_exact"] == 0
    assert binding["terminal_stage"] == book.INSUFFICIENT_PRESEARCH_STAGE
    assert binding["horizon_plan_receipt_sha256"] == boundary_plan.receipt_sha256
    assert binding["process_rejected_attempt_count"] == 0
    assert product.optimizer_payload["state_counts"][book.INSUFFICIENT_EVIDENCE] == 1


@pytest.mark.parametrize("generation,unobserved", [
    ("legacy", False), ("legacy", True), ("current", False), ("current", True),
], ids=["legacy-observed", "legacy-unobserved", "current-observed", "current-unobserved"])
def test_worker_horizon_rebuild_preserves_request_generation(tmp_path, generation, unobserved):
    # Mechanical synthetic transport only; no native fitting or source authority.
    request = horizon_fixture._unobserved_request() if unobserved else horizon_fixture._request()
    if generation == "current":
        request["schema_version"] = (training_horizon._CURRENT_UNOBSERVED_REQUEST_SCHEMA
            if unobserved else training_horizon._CURRENT_REQUEST_SCHEMA)
        policy = request["horizon_policy"]
        policy["schema_version"] = training_horizon._CURRENT_HORIZON_POLICY_SCHEMA
        policy["frozen_tier_pair_contract_sha256"] = policy.pop("cost_ladder_contract_sha256")
    plan = training_horizon.build_historical_horizon_plan(request)
    context = _context(tmp_path)
    descriptor = adapter._spool_plan_descriptor(context, plan)
    rebuilt = adapter._rebuild_horizon_plan(context, descriptor)
    assert rebuilt.to_dict() == plan.to_dict()
    assert rebuilt.receipt_sha256 == plan.receipt_sha256
    assert training_horizon._valid_plan(rebuilt) is True


def test_worker_horizon_rebuild_refuses_unknown_generation(tmp_path):
    plan = training_horizon.build_historical_horizon_plan(horizon_fixture._request())
    body = plan.to_dict()
    body["schema_version"] = "fxpm.historical-horizon-plan.v999"
    context = _context(tmp_path)
    descriptor = spool.write_canonical_json_artifact(context, body)
    with pytest.raises(adapter.HistoricalProductionAdapterError) as caught:
        adapter._rebuild_horizon_plan(context, {"descriptor": descriptor, "receipt_sha256": _sha(body)})
    assert caught.value.args == ("WORKER_HORIZON_PLAN_INVALID",)


def test_genuine_current_candidate_failure_stays_valid_no_winner(tmp_path, current_native_case):
    # Genuine required-tier refusal now replaces fabricated truncated position events.
    case, _request_value, capture, handle, evidence = _current_capture_from_case(current_native_case, tmp_path)
    assert all(not fold["lanes"]["optimizer_train_p75"]["position_events"]
               for row in evidence for fold in row["folds"])
    production = capture.complete_observed_cell(handle, attempt_evidence=evidence,
        evidence_access_started_at_utc=case["evidence_started_at_utc"],
        evidence_access_completed_at_utc=case["evidence_completed_at_utc"])
    assert production.decision.to_dict()["validated"] is True
    assert production.decision.status == "NO_PROCESS_WINNER"
    body = production.decision.to_dict()
    assert body.get("selected_process") is None
    assert all(entry["evidence_status"] == "AUTHENTICATED" for entry in production.evidence_ledger["entries"])
    assert not any(entry.get("process_rejection") for entry in production.evidence_ledger["entries"])
    product = _finish_through_worker_bridge(case["context"], capture.anchor, capture)
    cell = next(value for value in product.book_evidence if value.timeframe == "D1" and value.regime == handle.regime)
    assert cell.winner_decision.status == "NO_PROCESS_WINNER"
    assert product.optimizer_payload["confers_causal_current_live_or_capital_authority"] is False



def test_raw_production_receipt_reopen_pins_bounded_spool_primitive(
    tmp_path, monkeypatch
):
    context = _context(tmp_path)
    sink = spool.open_symbol_sink(context, OBSERVED_SYMBOL)
    payload = _canonical(
        {
            "schema_version": raw_winner.PRODUCTION_SCHEMA,
            "probe": "adapter-bounded-reopen",
        }
    )
    entry = sink.put_opaque(
        "adapter/reopen-probe",
        "winner_raw_evidence_production_receipt",
        (payload,),
        format=raw_winner.ASCII_JSON_FORMAT,
    )
    descriptor = entry["descriptor"]
    assert adapter._reopen_winner_raw_production_receipt(context, descriptor) == payload

    original_stream = spool._stream_file

    def drifted_stream(*args, **kwargs):
        digest, size, encoded = original_stream(*args, **kwargs)
        if kwargs.get("collect") is True:
            digest = "0" * 64
        return digest, size, encoded

    monkeypatch.setattr(spool, "_stream_file", drifted_stream)
    with pytest.raises(
        adapter.HistoricalProductionAdapterError,
        match="WINNER_RAW_PRODUCTION_RECEIPT_REOPEN_FAILED",
    ):
        adapter._reopen_winner_raw_production_receipt(context, descriptor)


def test_missing_observed_cell_cannot_be_defaulted(tmp_path):
    context = _context(tmp_path)
    anchor = _anchor()
    request = _request(context, anchor)
    symbol = OBSERVED_SYMBOL
    capture = adapter.open_historical_symbol_capture(context, request, symbol)
    _bind_terminal_symbol(capture, anchor)
    first_key = (book.TIMEFRAME_ORDER[0], book.REGIME_ORDER[0])
    capture._evidence.pop(first_key)
    with pytest.raises(
        adapter.HistoricalProductionAdapterError,
        match="BOOK_EVIDENCE_ROSTER_INCOMPLETE",
    ):
        capture.finish()


def test_authenticated_terminal_technical_timeframes_finish_as_complete_abstentions(
    tmp_path,
    monkeypatch,
):
    context = _context(tmp_path)
    anchor = _anchor()
    request = _request(context, anchor)
    symbol = OBSERVED_SYMBOL
    products = _genuine_terminal_technical_products(
        tmp_path, context, anchor, symbol, monkeypatch
    )
    capture = adapter.open_historical_symbol_capture(context, request, symbol)

    _bind_terminal_technical_symbol(capture, anchor, products)
    product = capture.finish()

    assert product.optimizer_payload["cell_count"] == 24
    assert product.optimizer_payload["state_counts"] == {book.TERMINAL_TECHNICAL: 24}
    assert all(
        cell.process_state_decision.state_kind == book.TERMINAL_TECHNICAL
        and cell.process_state_decision.to_dict()["terminal"] is True
        and cell.process_state_decision.to_dict()["complete"] is True
        and cell.process_state_decision.to_dict()["attempted_trials_exact"] == 0
        for cell in product.book_evidence
    )


def test_terminal_technical_rejects_spliced_activation_and_arbitrary_reason(
    tmp_path,
    monkeypatch,
):
    context = _context(tmp_path)
    anchor = _anchor()
    request = _request(context, anchor)
    symbol = OBSERVED_SYMBOL
    products = _genuine_terminal_technical_products(
        tmp_path, context, anchor, symbol, monkeypatch
    )
    timeframe = book.TIMEFRAME_ORDER[0]
    native_product = products[timeframe]
    plans = _terminal_technical_plans(
        anchor,
        symbol,
        timeframe,
        native_product=native_product,
    )
    authority = _terminal_technical_authority(native_product)

    unbound_product = replace(native_product, _data_dir=None)
    unbound_authority = replace(authority, product=unbound_product)
    unbound_capture = adapter.open_historical_symbol_capture(context, request, symbol)
    with pytest.raises(
        adapter.HistoricalProductionAdapterError,
        match="TERMINAL_TECHNICAL_AUTHORITY_REVERIFY_FAILED",
    ):
        unbound_capture.bind_timeframe(
            timeframe,
            horizon_plans=plans,
            source_group_decision=authority.source_group_decision,
            regime_fit_decision=authority.regime_fit_decision,
            terminal_native_timeframe=unbound_authority,
        )

    activation_splice = adapter.open_historical_symbol_capture(context, request, symbol)
    with pytest.raises(
        adapter.HistoricalProductionAdapterError,
        match="TERMINAL_TECHNICAL_AUTHORITY_BINDING_MISMATCH",
    ):
        activation_splice.bind_timeframe(
            timeframe,
            horizon_plans=_terminal_technical_plans(
                anchor,
                symbol,
                timeframe,
                native_product=native_product,
                activation_id="spliced-activation",
            ),
            source_group_decision=authority.source_group_decision,
            regime_fit_decision=authority.regime_fit_decision,
            terminal_native_timeframe=authority,
        )

    source_body = authority.source_group_decision.to_dict()
    forged_source = book.create_historical_source_group_decision(
        symbol=symbol,
        timeframe=timeframe,
        availability=book.TERMINAL_TECHNICAL,
        reason_code="SPLICED_ACTIVATION",
        source_contract_sha256=source_body["source_contract_sha256"],
        source_group_evidence_sha256=source_body["source_group_evidence_sha256"],
        acquisition_attempts_exact=source_body["acquisition_attempts_exact"],
        activation_id=source_body["activation_id"],
        activation_manifest_sha256=source_body["activation_manifest_sha256"],
        activation_entry_sha256=source_body["activation_entry_sha256"],
        terminal_proof_sha256=source_body["terminal_proof_sha256"],
    )
    assert forged_source is not None
    forged_fit = book.create_regime_fit_decision(
        source_group_decision=forged_source,
        fit_state=book.REGIME_NOT_APPLICABLE_TECHNICAL_TERMINAL,
        reason_code="SPLICED_ACTIVATION",
        regime_fit_policy_sha256=(
            authority.regime_fit_decision.to_dict()["regime_fit_policy_sha256"]
        ),
    )
    assert forged_fit is not None
    reason_splice = adapter.open_historical_symbol_capture(context, request, symbol)
    with pytest.raises(
        adapter.HistoricalProductionAdapterError,
        match="TERMINAL_TECHNICAL_AUTHORITY_BINDING_MISMATCH",
    ):
        reason_splice.bind_timeframe(
            timeframe,
            horizon_plans=plans,
            source_group_decision=forged_source,
            regime_fit_decision=forged_fit,
            terminal_native_timeframe=replace(
                authority,
                source_group_decision=forged_source,
                regime_fit_decision=forged_fit,
            ),
        )


def test_terminal_technical_persisted_adapter_graph_replays_exact_cell_bijection(
    tmp_path,
    monkeypatch,
):
    context = _context(tmp_path)
    anchor = _anchor()
    request = _request(context, anchor)
    symbol = OBSERVED_SYMBOL
    products = _genuine_terminal_technical_products(
        tmp_path, context, anchor, symbol, monkeypatch
    )
    capture = adapter.open_historical_symbol_capture(context, request, symbol)
    _bind_terminal_technical_symbol(capture, anchor, products)
    product = capture.finish()
    sink = request.symbol_sink(symbol)
    manifest = spool.verify_symbol_receipt(
        context.root,
        sink.seal(),
        expected_generation_id=context.generation_id,
        expected_symbol=symbol,
        expected_native_universe_sha256=context.native_universe_sha256,
        expected_config_sha256=context.config_sha256,
    )
    artifacts = {entry["artifact_id"]: entry for entry in manifest["artifacts"]}
    bindings = [
        orchestration.historical_horizon_plan_binding(plan)
        for plan in product.horizon_plans
    ]
    precommit_body = {
        "run_id": "historical-run-001",
        "optimizer_producer_release_sha256": (
            adapter.HISTORICAL_PRODUCER_RELEASE_SHA256
        ),
        "process_precommit_receipt_sha256": PROCESS_PRECOMMIT,
    }
    referenced = orchestration._verify_adapter_manifest_graph(
        context,
        artifacts,
        product.optimizer_payload,
        precommit_body=precommit_body,
        anchor=anchor,
        symbol=symbol,
        producer_request_sha256=request.receipt_sha256,
        horizon_bindings=bindings,
    )
    assert referenced == set(artifacts)

    hostile_payload = copy.deepcopy(product.optimizer_payload)
    hostile_artifacts = copy.deepcopy(artifacts)
    index_id = f"{anchor['anchor_id']}/adapter/production-index"
    hostile_index = _read_descriptor_json(context, hostile_payload["production_index"])
    hostile_index["cell_records"][0]["historical_state"] = book.TERMINAL_SOURCE_ABSENCE
    hostile_descriptor = spool.write_canonical_json_artifact(context, hostile_index)
    hostile_payload["production_index"] = hostile_descriptor
    hostile_artifacts[index_id]["descriptor"] = hostile_descriptor
    with pytest.raises(
        orchestration.HistoricalAnchorOrchestrationError,
        match="ADAPTER_TERMINAL_TECHNICAL_BIJECTION_INVALID",
    ):
        orchestration._verify_adapter_manifest_graph(
            context,
            hostile_artifacts,
            hostile_payload,
            precommit_body=precommit_body,
            anchor=anchor,
            symbol=symbol,
            producer_request_sha256=request.receipt_sha256,
            horizon_bindings=bindings,
        )


def _anchor_inputs():
    anchors = []
    plans = {}
    cutoff = date(2010, 1, 1)
    cells = tuple(causal.expected_cell_roster())
    for index in range(4):
        anchor_id = f"anchor-{index + 1:02d}"
        anchor = _anchor(anchor_id, cutoff)
        roster = []
        per_symbol = {}
        for symbol in book.PINNED_SYMBOL_ORDER:
            symbol_plans = {}
            for timeframe in book.TIMEFRAME_ORDER:
                values = _plans(anchor, symbol, timeframe, source_available=False)
                symbol_plans[timeframe] = values
                roster.extend(
                    orchestration.historical_horizon_plan_binding(plan)
                    for plan in values
                )
            per_symbol[symbol] = symbol_plans
        assert len(roster) == len(cells)
        anchor["horizon_binding_roster_sha256"] = (
            orchestration.historical_horizon_binding_roster_sha256(roster)
        )
        anchors.append(anchor)
        plans[anchor_id] = per_symbol
        cutoff = date.fromisoformat(anchor["forward_end_date"]) + timedelta(days=36)
    return anchors, plans


def test_orchestration_consumes_exact_100_by_6_by_4_adapter_stream(tmp_path):
    context = _context(tmp_path)
    anchors, plans = _anchor_inputs()
    precommit = orchestration.precommit_historical_anchor_run(
        context,
        run_id="historical-run-001",
        process_precommit_receipt_sha256=PROCESS_PRECOMMIT,
        optimizer_producer_release_sha256=(adapter.HISTORICAL_PRODUCER_RELEASE_SHA256),
        anchors=anchors,
    )
    producers = {}
    for anchor in anchors:
        anchor_id = anchor["anchor_id"]

        def producer(request, anchor=anchor, anchor_id=anchor_id):
            for symbol in book.PINNED_SYMBOL_ORDER:
                capture = adapter.open_historical_symbol_capture(
                    context, request, symbol
                )
                for timeframe in book.TIMEFRAME_ORDER:
                    source, fit = _terminal_authority(anchor_id, symbol, timeframe)
                    capture.bind_timeframe(
                        timeframe,
                        horizon_plans=plans[anchor_id][symbol][timeframe],
                        source_group_decision=source,
                        regime_fit_decision=fit,
                    )
                yield capture.finish()

        producers[anchor_id] = producer

    receipt = orchestration.execute_historical_anchor_run(
        context,
        precommit,
        producers,
    )
    assert orchestration.verify_historical_anchor_run_receipt(
        context, precommit, receipt
    )
    body = receipt.to_dict()
    assert body["symbol_count"] == 100
    assert len(body["anchors"]) == 4
    assert all(
        item["accounting_slot_count"] == 100 * 6 * 4
        and item["historical_state_counts"]["SOURCE_UNAVAILABLE"] == 100 * 6 * 4
        for item in body["anchors"]
    )


def test_adapter_release_contract_rejects_unbound_request(tmp_path):
    context = _context(tmp_path)
    anchor = _anchor()
    request = _request(context, anchor)
    body = request.to_dict()
    body["optimizer_producer_release_sha256"] = "f" * 64
    forged = orchestration.AnchorProducerRequest(_canonical(body))
    with pytest.raises(
        adapter.HistoricalProductionAdapterError,
        match="ANCHOR_REQUEST_CONTRACT_INVALID",
    ):
        adapter.open_historical_symbol_capture(
            context, forged, OBSERVED_SYMBOL
        )


def test_capture_uses_exact_request_sink_and_never_opens_private_sink(
    tmp_path, monkeypatch
):
    context = _context(tmp_path)
    anchor = _anchor()
    request = _request(context, anchor)
    symbol = OBSERVED_SYMBOL
    expected = request.symbol_sink(symbol)

    def forbidden(*args, **kwargs):
        raise AssertionError("adapter attempted to open a private sink")

    monkeypatch.setattr(spool, "open_symbol_sink", forbidden)
    capture = adapter.open_historical_symbol_capture(context, request, symbol)
    assert capture.sink is expected


def test_direct_request_without_orchestration_sinks_fails_typed(tmp_path):
    context = _context(tmp_path)
    anchor = _anchor()
    request = _request(context, anchor, bind_sinks=False)
    with pytest.raises(
        adapter.HistoricalProductionAdapterError,
        match="ORCHESTRATION_SYMBOL_SINK_UNAVAILABLE",
    ):
        adapter.open_historical_symbol_capture(
            context, request, OBSERVED_SYMBOL
        )


def test_incomplete_executable_artifact_fails_before_raw_freeze(
    tmp_path, monkeypatch, observed_native_prefix
):
    context = _context(tmp_path)
    anchor = _anchor()
    request = _request(context, anchor)
    symbol = OBSERVED_SYMBOL
    capture = adapter.open_historical_symbol_capture(context, request, symbol)
    fold_plan = _fold_plan()
    seed_result = _result(date(2017, 1, 1), date(2017, 3, 1), "SYNTHETIC_PREFREEZE_GUARD_ONLY")
    source_seed = raw_winner.build_trade_source_contract_from_backtester_result(
        seed_result
    )
    sample = build_trade_sample_evidence(
        [], source_contract=source_seed, minimum_observations=8
    )
    assert sample.source_contract is not None
    artifact = _attempt_artifact(symbol, native_prefix_product=observed_native_prefix)
    plan = _real_horizon_plan(
        anchor,
        symbol,
        "TREND",
        native_prefix_product=observed_native_prefix,
        attempt_artifact=artifact,
        fold_plan=fold_plan,
    )
    _bind_single_observed_capture(
        capture,
        anchor,
        sample.source_contract.source_contract_sha256,
        observed_plan=plan,
    )
    artifact["refit_recipe"].pop("seed")
    called = False

    def forbidden(*args, artifact_namespace, **kwargs):
        nonlocal called
        called = True
        raise AssertionError("raw freeze received an unverified artifact")

    monkeypatch.setattr(raw_winner, "freeze_winner_search_plan_v4", forbidden)
    with pytest.raises(
        adapter.HistoricalProductionAdapterError,
        match="ATTEMPT_0000_EXECUTABLE_ARTIFACT_INVALID",
    ):
        capture.freeze_observed_cell(
            OBSERVED_TIMEFRAME,
            "TREND",
            native_prefix_product=observed_native_prefix,
            precommitted_at_utc="2020-01-01T00:00:00Z",
            attempts_frozen_at_utc="2020-01-02T00:00:00Z",
            fold_plan=fold_plan,
            attempt_artifacts=[artifact],
            source_contract_seeds={"P75": source_seed, "P95": source_seed},
            frozen_tier_pair_contract={},
            partition_cost_precommitments={},
            risk_unit_contract=RISK_CONTRACT,
        )
    assert not called


def test_attempt_source_count_is_recomputed_from_sealed_prefix(
    tmp_path, monkeypatch, observed_native_prefix
):
    context = _context(tmp_path)
    anchor = _anchor()
    request = _request(context, anchor)
    symbol = OBSERVED_SYMBOL
    capture = adapter.open_historical_symbol_capture(context, request, symbol)
    fold_plan = _fold_plan()
    seed_result = _result(date(2017, 1, 1), date(2017, 3, 1), "SYNTHETIC_PREFREEZE_GUARD_ONLY")
    source_seed = raw_winner.build_trade_source_contract_from_backtester_result(
        seed_result
    )
    sample = build_trade_sample_evidence(
        [], source_contract=source_seed, minimum_observations=8
    )
    assert sample.source_contract is not None
    caller_asserted = _attempt_artifact(
        symbol,
        native_prefix_product=observed_native_prefix,
        declared_source_bar_count=499,
    )
    plan = _real_horizon_plan(
        anchor,
        symbol,
        "TREND",
        native_prefix_product=observed_native_prefix,
        attempt_artifact=caller_asserted,
        fold_plan=fold_plan,
    )
    _bind_single_observed_capture(
        capture,
        anchor,
        sample.source_contract.source_contract_sha256,
        observed_plan=plan,
    )
    raw_called = False

    def forbidden(*args, **kwargs):
        nonlocal raw_called
        raw_called = True
        raise AssertionError("raw freeze received a caller-asserted source count")

    monkeypatch.setattr(raw_winner, "freeze_winner_search_plan_v4", forbidden)
    with pytest.raises(
        adapter.HistoricalProductionAdapterError,
        match="ATTEMPT_SOURCE_CUTOFF_REFIT_WINDOW_INVALID",
    ):
        capture.freeze_observed_cell(
            OBSERVED_TIMEFRAME,
            "TREND",
            native_prefix_product=observed_native_prefix,
            precommitted_at_utc="2020-01-01T00:00:00Z",
            attempts_frozen_at_utc="2020-01-02T00:00:00Z",
            fold_plan=fold_plan,
            attempt_artifacts=[caller_asserted],
            source_contract_seeds={"P75": source_seed, "P95": source_seed},
            frozen_tier_pair_contract={},
            partition_cost_precommitments={},
            risk_unit_contract=RISK_CONTRACT,
        )
    assert not raw_called


def test_coherent_cutoff_window_splice_fails_against_authenticated_prefix(
    observed_native_prefix,
):
    symbol = OBSERVED_SYMBOL
    anchor = _anchor()
    fold_plan = _fold_plan()
    valid = _attempt_artifact(
        symbol,
        native_prefix_product=observed_native_prefix,
    )
    valid_start = pd.Timestamp(valid["horizon_process"]["fit_start_utc"])
    spliced = _attempt_artifact(
        symbol,
        native_prefix_product=observed_native_prefix,
        fit_start_utc=(valid_start + pd.Timedelta(days=1)).isoformat(),
    )
    assert (
        spliced["horizon_process"]["source_bar_count"]
        == valid["horizon_process"]["source_bar_count"] - 1
    )
    plan = _real_horizon_plan(
        anchor,
        symbol,
        "TREND",
        native_prefix_product=observed_native_prefix,
        attempt_artifact=spliced,
        fold_plan=fold_plan,
    )

    with pytest.raises(
        adapter.HistoricalProductionAdapterError,
        match="ATTEMPT_SOURCE_CUTOFF_REFIT_WINDOW_INVALID",
    ):
        adapter._strict_attempt_artifacts(
            [spliced],
            symbol=symbol,
            timeframe=OBSERVED_TIMEFRAME,
            regime="TREND",
            plan_body=plan.to_dict(),
            plan_receipt_sha256=plan.receipt_sha256,
            native_prefix_product=observed_native_prefix,
        )


def test_128_attempts_hash_one_current_window_per_candidate(
    monkeypatch,
    observed_native_prefix,
):
    symbol = OBSERVED_SYMBOL
    base = _attempt_artifact(
        symbol,
        native_prefix_product=observed_native_prefix,
    )
    process = base["winner_process"]
    horizon_process = base["horizon_process"]
    prefix = dict(observed_native_prefix.native_prefix)
    candidate_definition = {
        "candidate_id": "CALENDAR-120",
        "lookback_kind": horizon_process["lookback_kind"],
        "lookback_value": horizon_process["lookback_value"],
        "realized_start_utc": prefix["prefix_start_utc"],
    }
    candidate = {
        **candidate_definition,
        "candidate_definition_sha256": _sha(candidate_definition),
        "folds": [],
    }
    trials = []
    planned_attempts = []
    raw_attempts = []
    for ordinal in range(128):
        parameters = {"test_ordinal": ordinal}
        trial_definition = {
            **base["trial_definition"],
            "parameters": parameters,
            "parameter_count": 1,
        }
        trial_definition["horizon_process_sha256"] = _sha(horizon_process)
        trial_definition["process_sha256"] = _sha(process)
        trial = {
            "trial_id": f"TRIAL-{ordinal:03d}",
            "strategy_id": process["strategy_id"],
            "strategy_release_sha256": process["strategy_code_sha256"],
            "parameter_set_sha256": _sha(parameters),
        }
        trials.append(
            {
                **trial,
                "trial_definition_sha256": _sha(trial),
            }
        )
        planned_attempts.append(
            {
                "attempt_id": f"ATTEMPT-{ordinal:03d}",
                "candidate_id": candidate["candidate_id"],
                "trial_id": trial["trial_id"],
                "strategy_id": process["strategy_id"],
                "status": "PLANNED",
            }
        )
        raw_attempts.append(
            {
                "winner_process": copy.deepcopy(process),
                "horizon_process": copy.deepcopy(horizon_process),
                "trial_definition": trial_definition,
                "refit_recipe": copy.deepcopy(base["refit_recipe"]),
            }
        )
    plan_body = {
        "anchor": {"anchor_id": "anchor-128-attempt-window-cache"},
        "cell": {
            "symbol": symbol,
            "timeframe": OBSERVED_TIMEFRAME,
            "regime": "TREND",
        },
        "native_prefix": prefix,
        "candidates": [candidate],
        "trials": trials,
        "attempts": planned_attempts,
    }

    def accepted_artifacts(**kwargs):
        bodies = {
            "winner_process": json.loads(kwargs["winner_process_bytes"]),
            "horizon_process": json.loads(kwargs["horizon_process_bytes"]),
            "trial_definition": json.loads(kwargs["trial_definition_bytes"]),
            "refit_recipe": json.loads(kwargs["refit_recipe_bytes"]),
        }
        return current_refit.CurrentRefitSearchArtifacts(
            process_candidate_id=bodies["winner_process"]["process_candidate_id"],
            winner_process=bodies["winner_process"],
            winner_process_sha256=_sha(bodies["winner_process"]),
            horizon_process=bodies["horizon_process"],
            horizon_process_sha256=_sha(bodies["horizon_process"]),
            trial_definition=bodies["trial_definition"],
            trial_definition_sha256=_sha(bodies["trial_definition"]),
            refit_recipe=bodies["refit_recipe"],
            refit_recipe_sha256=_sha(bodies["refit_recipe"]),
        )

    monkeypatch.setattr(
        current_refit,
        "verify_current_refit_search_artifacts_bytes_v2",
        accepted_artifacts,
    )
    frame_hash_calls = []
    original_frame_sha256 = adapter._frame_sha256

    def counted_frame_sha256(frame):
        frame_hash_calls.append(len(frame))
        return original_frame_sha256(frame)

    monkeypatch.setattr(adapter, "_frame_sha256", counted_frame_sha256)
    normalized, contracts, receipts = adapter._strict_attempt_artifacts(
        raw_attempts,
        symbol=symbol,
        timeframe=OBSERVED_TIMEFRAME,
        regime="TREND",
        plan_body=plan_body,
        plan_receipt_sha256=_sha("128-attempt-plan"),
        native_prefix_product=observed_native_prefix,
    )

    assert len(normalized) == len(contracts) == len(receipts) == 128
    assert frame_hash_calls == [horizon_process["source_bar_count"]]
    assert len({receipt["fit_frame_sha256"] for receipt in receipts}) == 1


def test_duplicate_depth_same_trial_is_distinct_when_identity_is_horizon_bound(tmp_path, observed_native_prefix):
    anchor = _anchor()
    artifacts = tuple(_attempt_artifact(OBSERVED_SYMBOL, native_prefix_product=observed_native_prefix,
        lookback_kind="CALENDAR_INTERVAL", lookback_value=depth) for depth in (500, 120))
    assert len({_sha(value["horizon_process"]) for value in artifacts}) == 2
    assert len({value["winner_process"]["process_candidate_id"] for value in artifacts}) == 2
    plan = _real_horizon_plan(anchor, OBSERVED_SYMBOL, "TREND", native_prefix_product=observed_native_prefix,
        attempt_artifact=artifacts[0], additional_attempt_artifacts=artifacts[1:], fold_plan=_fold_plan())
    normalized, contracts, receipts = adapter._strict_attempt_artifacts(artifacts,
        symbol=OBSERVED_SYMBOL, timeframe="D1", regime="TREND", plan_body=plan.to_dict(),
        plan_receipt_sha256=plan.receipt_sha256, native_prefix_product=observed_native_prefix)
    assert len(normalized) == len(contracts) == len(receipts) == 2
    assert [row["lookback_value"] for row in contracts] == [500, 120]
    assert [row["source_bar_count"] for row in receipts] == [500, 120]
    assert len({row["horizon_candidate_id"] for row in receipts}) == 2



def test_horizon_registry_splice_fails_before_winner_evidence(
    tmp_path, observed_native_prefix
):
    context = _context(tmp_path)
    anchor = _anchor()
    request = _request(context, anchor)
    symbol = OBSERVED_SYMBOL
    capture = adapter.open_historical_symbol_capture(context, request, symbol)
    fold_plan = _fold_plan()
    artifact = _attempt_artifact(symbol, native_prefix_product=observed_native_prefix)
    genuine_plan = _real_horizon_plan(
        anchor,
        symbol,
        "TREND",
        native_prefix_product=observed_native_prefix,
        attempt_artifact=artifact,
        fold_plan=fold_plan,
    )
    spliced_body = genuine_plan.to_dict()
    spliced_body["candidate_registry_sha256"] = "f" * 64

    class DuckPlan:
        receipt_sha256 = _sha(spliced_body)

        @staticmethod
        def to_dict():
            return spliced_body

    source, fit = _available_authority(
        symbol,
        OBSERVED_TIMEFRAME,
        _sha("registry-splice-source"),
        native_prefix_body=observed_native_prefix.native_prefix,
    )
    for timeframe in book.TIMEFRAME_ORDER[:-1]:
        terminal_source, terminal_fit = _terminal_authority(
            anchor["anchor_id"], symbol, timeframe
        )
        capture.bind_timeframe(
            timeframe,
            horizon_plans=_plans(anchor, symbol, timeframe, source_available=False),
            source_group_decision=terminal_source,
            regime_fit_decision=terminal_fit,
        )
    plans = [DuckPlan()]
    plans.extend(
        _real_unobserved_horizon_plan(
            anchor,
            symbol,
            OBSERVED_TIMEFRAME,
            regime,
            native_prefix_body=observed_native_prefix.native_prefix,
            source_decision=source,
            fit_decision=fit,
        )
        for regime in book.REGIME_ORDER[1:]
    )
    with pytest.raises(
        orchestration.HistoricalAnchorOrchestrationError,
        match="HORIZON_PLAN_OBJECT_INVALID",
    ):
        capture.bind_timeframe(
            OBSERVED_TIMEFRAME,
            horizon_plans=plans,
            source_group_decision=source,
            regime_fit_decision=fit,
        )


def test_cross_anchor_raw_artifacts_share_sink_without_id_collision(tmp_path, current_native_case):
    from tests import test_winner_raw_evidence as raw_fixture

    case = raw_fixture.clone_current_native_case(current_native_case, tmp_path)
    frozen_plans = []
    for anchor_id in ("anchor-01", "anchor-02"):
        namespace = f"historical-anchor/historical-run-001/{anchor_id}"
        frozen_plans.append(raw_winner.freeze_winner_search_plan_v4(case["sink"], symbol=OBSERVED_SYMBOL,
            timeframe="D1", regime=case["plan"].to_dict()["cell"]["regime"],
            precommitted_at_utc="2025-01-02T00:00:00Z", attempts_frozen_at_utc="2025-01-03T00:00:00Z",
            fold_plan=case["fold_plan"], attempt_artifacts=case["attempt_artifacts"],
            precommitted_evidence_roles=search_execution._precommitted_evidence_roles(case["verified_attempts"]),
            horizon_execution_contracts=case["frozen"].horizon_execution_contracts,
            source_contract_seeds=case["fitted"].source_contract_seeds,
            frozen_tier_pair_contract=case["fitted"].frozen_tier_pair_contract,
            partition_cost_precommitments=case["partition_cost_precommitments"],
            risk_unit_contract=RISK_CONTRACT, artifact_namespace=namespace))
    assert frozen_plans[0].artifact_namespace != frozen_plans[1].artifact_namespace
    ids = {row["artifact_id"] for row in case["sink"].artifact_roster()}
    for frozen in frozen_plans:
        assert f"{frozen.artifact_namespace}/winner-search/D1/{frozen.regime}/frozen/precommitment" in ids



def _worker_bridge_product_surface(product):
    def sealed(value):
        if value is None:
            return None
        return {
            "body": value.to_dict(),
            "receipt_sha256": getattr(
                value,
                "receipt_sha256",
                getattr(value, "request_sha256", None),
            ),
        }

    plans = list(product.horizon_plans)
    cells = list(product.book_evidence)
    return {
        "plans": [sealed(plan) for plan in plans],
        "cells": [
            {
                "identity": (cell.symbol, cell.timeframe, cell.regime),
                "source": sealed(cell.source_group_decision),
                "fit": sealed(cell.regime_fit_decision),
                "winner_request": sealed(cell.winner_request),
                "winner_decision": sealed(cell.winner_decision),
                "process_state": sealed(cell.process_state_decision),
                "insufficient_horizon": sealed(cell.insufficient_horizon_plan),
                "insufficient_rejection_request": sealed(
                    cell.insufficient_process_rejection_request
                ),
                "insufficient_rejection_decision": sealed(
                    cell.insufficient_process_rejection_decision
                ),
                "insufficient_cross_binding": (
                    cell.insufficient_horizon_winner_cross_binding_bytes
                ),
                "insufficient_raw_receipt": (
                    cell.insufficient_winner_raw_production_receipt_bytes
                ),
            }
            for cell in cells
        ],
        "optimizer_payload": product.optimizer_payload,
    }


def _spool_terminal_worker_bundle(tmp_path, monkeypatch):
    context = _context(tmp_path)
    anchor = _anchor()
    worker_request = _request(context, anchor)
    symbol = OBSERVED_SYMBOL
    capture = adapter.open_historical_symbol_capture(context, worker_request, symbol)
    _bind_terminal_symbol(capture, anchor)

    finished_products = []
    original_finish = adapter.HistoricalSymbolSearchCapture.finish

    def finish_spy(self):
        product = original_finish(self)
        finished_products.append(product)
        return product

    monkeypatch.setattr(
        adapter.HistoricalSymbolSearchCapture,
        "finish",
        finish_spy,
    )
    descriptor = adapter.spool_historical_symbol_worker_bundle(capture)
    assert len(finished_products) == 1
    return (
        context,
        anchor,
        worker_request,
        symbol,
        descriptor,
        finished_products[0],
    )


def test_symbol_worker_bundle_reopens_parent_local_exact_product_and_roster(
    tmp_path,
    monkeypatch,
):
    (
        context,
        anchor,
        worker_request,
        symbol,
        descriptor,
        worker_product,
    ) = _spool_terminal_worker_bundle(tmp_path, monkeypatch)
    worker_roster = worker_request.symbol_sink(symbol).artifact_roster()
    assert worker_roster
    assert all(entry["descriptor"] != descriptor for entry in worker_roster)

    parent_request = _request(context, anchor)
    parent_sink = parent_request.symbol_sink(symbol)
    assert parent_sink.artifact_roster() == []
    reopened = adapter.reopen_historical_symbol_worker_bundle(
        context,
        parent_request,
        descriptor,
    )

    assert reopened.symbol == symbol
    assert _worker_bridge_product_surface(reopened) == (
        _worker_bridge_product_surface(worker_product)
    )
    assert parent_sink.artifact_roster() == worker_roster
    worker_plans = list(worker_product.horizon_plans)
    reopened_plans = list(reopened.horizon_plans)
    worker_cells = list(worker_product.book_evidence)
    reopened_cells = list(reopened.book_evidence)
    assert (
        len(reopened_plans)
        == len(worker_plans)
        == book.FAMILY_SIZE // len(book.PINNED_SYMBOL_ORDER)
    )
    assert len(reopened_cells) == len(worker_cells) == len(reopened_plans)
    assert all(training_horizon._valid_plan(plan) for plan in reopened_plans)
    assert all(
        reopened_plan is not worker_plan
        for reopened_plan, worker_plan in zip(
            reopened_plans,
            worker_plans,
            strict=True,
        )
    )
    assert all(
        reopened_cell.process_state_decision is not worker_cell.process_state_decision
        for reopened_cell, worker_cell in zip(
            reopened_cells,
            worker_cells,
            strict=True,
        )
    )


def test_symbol_worker_bundle_coherent_symbol_tamper_fails_before_registration(
    tmp_path,
    monkeypatch,
):
    (
        context,
        anchor,
        _worker_request,
        symbol,
        descriptor,
        _worker_product,
    ) = _spool_terminal_worker_bundle(tmp_path, monkeypatch)
    bundle = _read_descriptor_json(context, descriptor)
    assert bundle["symbol"] == symbol
    bundle["symbol"] = book.PINNED_SYMBOL_ORDER[1]
    coherently_resealed = spool.write_canonical_json_artifact(context, bundle)

    parent_request = _request(context, anchor)
    parent_sink = parent_request.symbol_sink(symbol)
    with pytest.raises(adapter.HistoricalProductionAdapterError):
        adapter.reopen_historical_symbol_worker_bundle(
            context,
            parent_request,
            coherently_resealed,
        )
    assert parent_sink.artifact_roster() == []
