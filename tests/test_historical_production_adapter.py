from __future__ import annotations

import copy
import csv
from dataclasses import asdict, replace
from datetime import date, datetime, timedelta, timezone
import hashlib
import json
from pathlib import Path
import struct

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
from pm_core import Backtester, InstrumentSpec, PipelineConfig
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
        "failure_code": "FROZEN_PATH_COST_STRESS_FAILED",
        "attempted_trials_exact": 128,
        "frozen_receipt_sha256": "2" * 64,
        "search_outcome": None,
        "economic_no_trade_or_no_winner": False,
        "downstream_authority": False,
    }
    if schema_version == adapter.BOUNDED_RAW_WINNER_CANARY_FAILURE_SCHEMA:
        body.update(
            {
                "exception_type": "FrozenPathCostStressError",
                "failure_details": [
                    "STRESS_NOT_NUMERICALLY_ADVERSE",
                    "stress_p95",
                    (
                        "genuine SpreadP90/SpreadP95/SpreadMax evidence or an "
                        "independently preregistered numerical stress is required"
                    ),
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
        failure_code="FROZEN_PATH_COST_STRESS_FAILED",
        attempted_trials_exact=128,
        frozen_receipt_sha256="2" * 64,
    )


def test_canary_failure_v2_seals_exact_bounded_diagnostic() -> None:
    body = _canary_failure_body(
        schema_version=adapter.BOUNDED_RAW_WINNER_CANARY_FAILURE_SCHEMA
    )

    reopened = _verify_canary_failure_body(body)

    assert reopened == body
    assert reopened["exception_type"] == "FrozenPathCostStressError"
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
        "exception_type": "FrozenPathCostStressError",
        "failure_details": ["STRESS_NOT_NUMERICALLY_ADVERSE"],
        "attempt_ordinal": 0,
        "fold_ordinal": 0,
        **overrides,
    }

    with pytest.raises(adapter.HistoricalProductionAdapterError, match=expected_code):
        adapter._bounded_canary_failure_diagnostic(
            **diagnostic,
            attempted_trials_exact=128,
        )


@pytest.fixture(scope="module")
def observed_native_prefix(tmp_path_factory):
    """Build one real sealed 500-row D1 prefix shared by observed-cell tests."""

    root = tmp_path_factory.mktemp("historical-adapter-native-prefix")
    data_dir = root / "data"
    symbol = book.PINNED_SYMBOL_ORDER[0]
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
    row_count = 500
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
    frame = native_prefix.open_verified_prefix_frame(product)
    assert product.source_state == native_prefix.AVAILABLE
    assert len(frame) == row_count
    assert product.native_prefix["prefix_rows"] == row_count
    return product


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
):
    label = f"{anchor['anchor_id']}:{symbol}:{timeframe}:{regime}"
    request = (
        horizon_fixture._request()
        if source_available
        else horizon_fixture._terminal_source_request("SOURCE_UNAVAILABLE")
    )
    request["run_id"] = "historical-run-001"
    request["anchor"] = {
        "anchor_id": anchor["anchor_id"],
        "ordinal": int(anchor["anchor_id"].rsplit("-", 1)[-1]),
        "selection_cutoff_utc": anchor["selection_cutoff_utc"],
        "forward_start_utc": f"{anchor['forward_start_date']}T00:00:00Z",
        "forward_end_exclusive_utc": f"{anchor['forward_end_date']}T00:00:00Z",
        "evidence_accessed_at_utc": "2026-07-13T12:00:00Z",
        "process_precommit_receipt_sha256": PROCESS_PRECOMMIT,
    }
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
):
    return [
        _plan(
            anchor,
            symbol,
            timeframe,
            regime,
            source_available=source_available,
            commitments=commitments,
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
    parent_request = _request(context, anchor)
    return adapter.reopen_historical_symbol_worker_bundle(
        context,
        parent_request,
        descriptor,
    )


TEST_INSTRUMENT_SPEC = InstrumentSpec(
    symbol=book.PINNED_SYMBOL_ORDER[0],
    pip_position=4,
    pip_value=10.0,
    spread_avg=1.0,
    commission_per_lot=7.0,
)
TEST_INSTRUMENT_SPEC_SHA256 = Backtester._cost_attestation_json_sha256(
    {
        "schema_version": "fxpm-backtester-instrument-spec-v1",
        "instrument_spec": asdict(TEST_INSTRUMENT_SPEC),
    }
)
BASE_SPREAD_POLICY = json.dumps(
    {
        "bar_column": "SpreadMax",
        "entry_column": "SpreadOpen",
        "entry_model": "open_spread",
        "fallback_to_fixed": False,
        "mode": "conservative_max",
        "source": "historical_spread_points",
        "stress_entry_fallback": False,
        "units": "price",
    },
    ensure_ascii=True,
    separators=(",", ":"),
    sort_keys=True,
)
STRESS_SPREAD_POLICY = json.dumps(
    {
        "bar_column": "SpreadMax",
        "entry_column": "SpreadP95",
        "entry_model": "p95_spread_stress",
        "fallback_to_fixed": False,
        "mode": "stress_p95",
        "source": "historical_spread_points",
        "stress_entry_fallback": False,
        "units": "price",
    },
    ensure_ascii=True,
    separators=(",", ":"),
    sort_keys=True,
)


BASE_COST = build_numerical_execution_cost_attestation(
    use_spread=True,
    use_slippage=True,
    use_commission=True,
    spread_mode=BASE_SPREAD_POLICY,
    spread_values_sha256="7" * 64,
    commission_inputs_sha256="8" * 64,
    instrument_spec_sha256=TEST_INSTRUMENT_SPEC_SHA256,
    slippage_pips=0.5,
    slippage_price=0.00005,
)
STRESS_COST = build_numerical_execution_cost_attestation(
    use_spread=True,
    use_slippage=True,
    use_commission=True,
    spread_mode=STRESS_SPREAD_POLICY,
    spread_values_sha256="6" * 64,
    commission_inputs_sha256="8" * 64,
    instrument_spec_sha256=TEST_INSTRUMENT_SPEC_SHA256,
    slippage_pips=0.5,
    slippage_price=0.00005,
)


def _frozen_cost_policy(lane, attestation):
    return raw_winner.build_frozen_numerical_execution_cost_policy(
        lane=lane,
        use_spread=attestation["use_spread"],
        use_slippage=attestation["use_slippage"],
        use_commission=attestation["use_commission"],
        spread_mode=attestation["spread_mode"],
        commission_inputs_sha256=attestation["commission_inputs_sha256"],
        instrument_spec_sha256=attestation["instrument_spec_sha256"],
        slippage_pips=attestation["slippage_pips_hex"],
        slippage_price=attestation["slippage_price_hex"],
    )


BASE_COST_POLICY = _frozen_cost_policy("BASE", BASE_COST)
STRESS_COST_POLICY = _frozen_cost_policy("STRESS", STRESS_COST)
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


def _events(start, namespace, values=None):
    values = values or [0.30 + 0.01 * (index % 2) for index in range(12)]
    rows = []
    for index, value in enumerate(values):
        opened = datetime.combine(
            start + timedelta(days=index), datetime.min.time(), timezone.utc
        ) + timedelta(hours=10)
        rows.append(
            {
                "trade_record_schema_version": "2026-07-14-position-events-v2",
                "position_id": f"{namespace}:{index:04d}",
                "entry_time": opened.isoformat(),
                "exit_time": (opened + timedelta(hours=1)).isoformat(),
                "entry_event_order": index * 4 + 1,
                "exit_event_order": index * 4 + 3,
                "pnl_dollars": value * 100.0,
                "risk_amount": 100.0,
            }
        )
    return rows


def _partition_cost_attestation(policy_seed, namespace):
    """Materialize the genuine partition-local vector under frozen policy facts."""

    return build_numerical_execution_cost_attestation(
        use_spread=policy_seed["use_spread"],
        use_slippage=policy_seed["use_slippage"],
        use_commission=policy_seed["use_commission"],
        spread_mode=policy_seed["spread_mode"],
        spread_values_sha256=_sha(f"{namespace}:spread-values"),
        commission_inputs_sha256=policy_seed["commission_inputs_sha256"],
        instrument_spec_sha256=policy_seed["instrument_spec_sha256"],
        slippage_pips=float.fromhex(policy_seed["slippage_pips_hex"]),
        slippage_price=float.fromhex(policy_seed["slippage_price_hex"]),
    )


def _result(
    start,
    end,
    namespace,
    *,
    cost=BASE_COST,
    cost_namespace=None,
    values=None,
):
    events = _events(start, namespace, values)
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
        "position_events": events,
        "winner_daily_r_surface": surface,
        "trade_power_partition_axis": axis,
        "execution_cost_timing_contract": {
            "entry_slippage": "entry_slippage_parity_v1",
            "swap_accrual": "causal_swap_accrual_v1",
        },
        "numerical_execution_cost_attestation": _partition_cost_attestation(
            cost, namespace if cost_namespace is None else cost_namespace
        ),
        "swap_accrual_contract": _swap(f"{namespace}:projection"),
    }


def _attempt_artifact(
    symbol,
    timeframe=OBSERVED_TIMEFRAME,
    regime="TREND",
    *,
    native_prefix_product,
    lookback_kind="CALENDAR_INTERVAL",
    lookback_value=120,
    fit_start_utc=None,
    declared_source_bar_count=None,
):
    strategy_id = "EMACrossoverStrategy"
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
):
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
        for ordinal, planned in enumerate(() if boundary_limited else fold_plan):
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
            value["fit_rows"] = 50
            value["validation_rows"] = 30
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
    request["horizon_policy"]["cost_contract_sha256"] = _sha(BASE_COST_POLICY)
    request["horizon_policy"]["cost_stress_contract_sha256"] = _sha(STRESS_COST_POLICY)
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
):
    request = copy.deepcopy(horizon_fixture._unobserved_request())
    request["run_id"] = "historical-run-001"
    request["anchor"] = {
        "anchor_id": anchor["anchor_id"],
        "ordinal": 0,
        "selection_cutoff_utc": anchor["selection_cutoff_utc"],
        "forward_start_utc": f"{anchor['forward_start_date']}T00:00:00Z",
        "forward_end_exclusive_utc": f"{anchor['forward_end_date']}T00:00:00Z",
        "evidence_accessed_at_utc": "2025-01-01T00:00:00Z",
        "process_precommit_receipt_sha256": PROCESS_PRECOMMIT,
    }
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


def _cost_stress_adversity_plan(
    *,
    horizon_plan,
    fold_plan,
    symbol: str = "EURUSD",
    timeframe: str = OBSERVED_TIMEFRAME,
    regime: str = "TREND",
):
    folds, _latest = raw_winner._normalize_fold_plan(
        fold_plan,
        symbol=symbol,
        timeframe=timeframe,
        regime=regime,
    )
    horizon_attempts = horizon_plan.to_dict()["attempts"]
    fold_receipts = []
    for fold_ordinal, planned in enumerate(fold_plan):
        lanes = _raw_fold(fold_ordinal, planned)["lanes"]
        geometry = _partition_geometry(fold_ordinal, lanes)
        base_cost = lanes["final_lockbox_base"]["numerical_execution_cost_attestation"]
        stress_cost = lanes["final_lockbox_stress"][
            "numerical_execution_cost_attestation"
        ]
        fold_receipts.append((geometry[2], base_cost, stress_cost))
    rows = [
        {
            "attempt_ordinal": attempt_ordinal,
            "horizon_attempt_id": horizon_attempts[attempt_ordinal]["attempt_id"],
            "fold_ordinal": fold["fold_ordinal"],
            "fold_id": fold["fold_id"],
            "final_partition_geometry_receipt_sha256": fold_receipts[
                fold["fold_ordinal"]
            ][0]["receipt_sha256"],
            "final_partition_frame_sha256": fold_receipts[fold["fold_ordinal"]][0][
                "frame_sha256"
            ],
            "base_cost_attestation_sha256": fold_receipts[fold["fold_ordinal"]][1][
                "attestation_sha256"
            ],
            "stress_cost_attestation_sha256": fold_receipts[fold["fold_ordinal"]][2][
                "attestation_sha256"
            ],
            "base_spread_values_sha256": fold_receipts[fold["fold_ordinal"]][1][
                "spread_values_sha256"
            ],
            "stress_spread_values_sha256": fold_receipts[fold["fold_ordinal"]][2][
                "spread_values_sha256"
            ],
            "expected_adversity_status": (
                raw_winner.COST_STRESS_ADVERSITY_EXPECTED_STRICT
            ),
        }
        for attempt_ordinal in range(len(horizon_attempts))
        for fold in folds
    ]
    return {
        "schema_version": raw_winner.COST_STRESS_ADVERSITY_PLAN_SCHEMA,
        "symbol": symbol,
        "timeframe": timeframe,
        "regime": regime,
        "row_count": len(rows),
        "rows_sha256": _sha(rows),
        "rows": rows,
    }


def _provenance_receipt(role, common, **specific):
    body = {
        "schema_version": "fxpm.authoritative-search-provenance.v1",
        **common,
        "receipt_role": role,
        **specific,
    }
    return {**body, "receipt_sha256": _sha(body)}


def _partition_geometry(fold_ordinal, lanes):
    geometry = []
    for index, (role, lane_name) in enumerate(
        (
            ("optimizer_train", "optimizer_train_base"),
            ("tuning_validation", "tuning_validation_base"),
            ("final_lockbox", "final_lockbox_base"),
        )
    ):
        axis = lanes[lane_name]["trade_power_partition_axis"]
        body = {
            "schema_version": "fxpm.winner-partition-geometry.v1",
            "role": role,
            "frame_start_utc": axis["start_utc"],
            "scored_start_utc": axis["start_utc"],
            "end_exclusive_utc": axis["end_utc"],
            "warmup_bars": axis["warmup_bars"],
            "scored_bars": axis["scored_bars"],
            "frame_rows": axis["warmup_bars"] + axis["scored_bars"],
            "frame_sha256": _sha(f"frame:{fold_ordinal}:{index}"),
        }
        geometry.append({**body, "receipt_sha256": _sha(body)})
    return geometry


def _execution_spec_receipt(frozen, cross):
    body = {
        "schema_version": "fxpm.execution-instrument-spec-receipt.v1",
        "symbol": frozen.symbol,
        "authority_timing": "FROZEN_BEFORE_FIRST_SEARCH_EVALUATION",
        "deployment_scope": "TEST_PRODUCTION_SCOPE",
        "deployment_config_sha256": _sha("deployment-config"),
        "deployment_config_size_bytes": 1024,
        "broker_specs_relative_path": "broker_specs.json",
        "broker_specs_sha256": _sha("broker-specs"),
        "broker_specs_size_bytes": 2048,
        "broker_record_key": frozen.symbol,
        "broker_record_sha256": _sha("broker-record"),
        "configured_instrument_record_sha256": _sha("configured-instrument"),
        "native_prefix_index_sha256": cross["native_prefix_index_sha256"],
        "native_provenance_sha256": _sha("native-provenance"),
        "native_instrument_snapshot_sha256": _sha("native-instrument"),
        "tick_value_source": "ACTIVATED_ACQUISITION_INSTRUMENT_SNAPSHOT",
        "commission_source": "FROZEN_DEPLOYMENT_CONFIG_INSTRUMENT_SURFACE",
        "volume_source": "FROZEN_BROKER_SPECS_RECORD",
        "instrument_spec": asdict(TEST_INSTRUMENT_SPEC),
        "instrument_spec_sha256": TEST_INSTRUMENT_SPEC_SHA256,
        "grants_search_authority": False,
        "grants_trading_authority": False,
    }
    return {**body, "receipt_sha256": _sha(body)}


def _raw_fold(fold_ordinal, planned, handle=None):
    final_start = date.fromisoformat(planned["oos_start_date_utc"])
    final_end = date.fromisoformat(planned["oos_end_date_utc"])
    train_start = final_start - timedelta(days=300)
    tuning_start = final_start - timedelta(days=150)
    prefix = f"fold-{fold_ordinal}"
    final = _result(final_start, final_end, f"{prefix}:final")
    stress_values = [
        event["pnl_dollars"] / event["risk_amount"] - 0.02
        for event in final["position_events"]
    ]
    stress = _result(
        final_start,
        final_end,
        f"{prefix}:final",
        cost=STRESS_COST,
        cost_namespace=f"{prefix}:final:stress",
        values=stress_values,
    )
    lanes = {
        "optimizer_train_base": _result(
            train_start,
            train_start + timedelta(days=62),
            f"{prefix}:train",
        ),
        "tuning_validation_base": _result(
            tuning_start,
            tuning_start + timedelta(days=62),
            f"{prefix}:tuning",
        ),
        "final_lockbox_base": final,
        "final_lockbox_stress": stress,
    }
    provenance = {}
    if handle is not None:
        frozen = handle.frozen
        attempt = frozen.attempt_ledger["attempts"][0]
        artifacts = frozen.attempt_artifacts_bundle[0]
        cross = json.loads(handle._cross_binding_bytes.decode("ascii"))
        mapped = cross["mapping"][0]
        mapped_fold = mapped["fold_mapping"][fold_ordinal]
        geometry = _partition_geometry(fold_ordinal, lanes)
        final_axis = final["trade_power_partition_axis"]
        signal_values = [0] * (final_axis["warmup_bars"] + final_axis["scored_bars"])
        signal_sha256 = hashlib.sha256(
            b"".join(struct.pack("<i", value) for value in signal_values)
        ).hexdigest()
        signal_body = {
            "schema_version": "fxpm.authoritative-search-signal-vector.v1",
            "encoding": "little-endian-signed-int32",
            "observation_count": len(signal_values),
            "values": signal_values,
            "signal_vector_sha256": signal_sha256,
        }
        signal_receipt = {**signal_body, "receipt_sha256": _sha(signal_body)}
        common = {
            "authority_scope": "PRECOMMITTED_HISTORICAL_INNER_FOLD_ONLY",
            "anchor_id": cross["anchor_id"],
            "symbol": frozen.symbol,
            "timeframe": frozen.timeframe,
            "regime": frozen.regime,
            "attempt_ordinal": 0,
            "horizon_attempt_id": mapped["horizon_attempt_id"],
            "fold_ordinal": fold_ordinal,
            "fold_id": mapped_fold["horizon_fold_id"],
            "confers_final_oos_or_trading_authority": False,
        }
        stress_body = {
            "schema_version": raw_winner.FROZEN_PATH_COST_STRESS_SCHEMA,
            "symbol": frozen.symbol,
            "timeframe": frozen.timeframe,
            "strategy_name": artifacts["winner_process"]["strategy_id"],
            "stress_mode": "stress_p95",
            "warmup_bars": final["trade_power_partition_axis"]["warmup_bars"],
            "partition_axis_sha256": final["trade_power_partition_axis"][
                "horizon_receipt_sha256"
            ],
            "signal_vector_sha256": signal_sha256,
            "base_cost_attestation_sha256": final[
                "numerical_execution_cost_attestation"
            ]["attestation_sha256"],
            "stress_cost_attestation_sha256": stress[
                "numerical_execution_cost_attestation"
            ]["attestation_sha256"],
            "base_spread_values_sha256": final["numerical_execution_cost_attestation"][
                "spread_values_sha256"
            ],
            "stress_spread_values_sha256": stress[
                "numerical_execution_cost_attestation"
            ]["spread_values_sha256"],
            "base_position_roster_sha256": Backtester._cost_attestation_json_sha256(
                {"position_events": final["position_events"]}
            ),
            "stress_position_roster_sha256": Backtester._cost_attestation_json_sha256(
                {"position_events": stress["position_events"]}
            ),
            "position_count": len(stress["position_events"]),
            "daily_observation_count": len(
                stress["winner_daily_r_surface"]["daily_net_mtm_r"]
            ),
            "path_identity_preserved": True,
            "stress_never_improves": True,
            "strictly_worse_trade_count": len(stress["position_events"]),
            "strictly_worse_daily_observation_count": sum(
                right < left
                for left, right in zip(
                    final["winner_daily_r_surface"]["daily_net_mtm_r"],
                    stress["winner_daily_r_surface"]["daily_net_mtm_r"],
                    strict=True,
                )
            ),
            "realized_adversity_status": (
                raw_winner.BACKTEST_FROZEN_PATH_COST_STRESS_ADVERSITY_STRICT
            ),
        }
        stress_receipt = {**stress_body, "receipt_sha256": _sha(stress_body)}
        source_binding = frozen.source_contract_seed["source_binding"]
        provenance = {
            "anchor_selection_receipt": _provenance_receipt(
                "anchor_selection_receipt",
                common,
                process_precommit_receipt_sha256=PROCESS_PRECOMMIT,
                horizon_plan_receipt_sha256=cross["horizon_roster"][
                    "plan_receipt_sha256"
                ],
                selection_cutoff_utc="2020-01-01T00:00:00Z",
            ),
            "anchor_horizon_decision": _provenance_receipt(
                "anchor_horizon_decision",
                common,
                candidate_id=mapped["horizon_candidate_id"],
                candidate_definition_sha256=mapped[
                    "horizon_candidate_definition_sha256"
                ],
                horizon_process_sha256=attempt["horizon_process_sha256"],
                fold_definition_sha256=mapped_fold["horizon_fold_definition_sha256"],
                partition_geometry=geometry,
                partition_geometry_receipts=[
                    item["receipt_sha256"] for item in geometry
                ],
            ),
            "anchor_trial_ledger": _provenance_receipt(
                "anchor_trial_ledger",
                common,
                trial_registry_sha256=cross["horizon_roster"]["trial_registry_sha256"],
                attempt_roster_sha256=cross["horizon_roster"]["attempt_roster_sha256"],
                attempted_trials_exact=frozen.attempt_ledger["attempt_count"],
            ),
            "anchor_selected_trial": _provenance_receipt(
                "anchor_selected_trial",
                common,
                selection_scope="PRECOMMITTED_ATTEMPT_NOT_POSTHOC_WINNER",
                trial_id=mapped["horizon_trial_id"],
                trial_definition_sha256=mapped["horizon_trial_definition_sha256"],
                winner_process_sha256=attempt["process_sha256"],
                executable_trial_sha256=attempt["trial_definition_sha256"],
            ),
            "anchor_refit_receipt": _provenance_receipt(
                "anchor_refit_receipt",
                common,
                refit_recipe_sha256=attempt["refit_recipe_sha256"],
                fit_start_utc=f"{train_start.isoformat()}T00:00:00Z",
                fit_end_exclusive_utc=(
                    f"{(final_start - timedelta(days=1)).isoformat()}T00:00:00Z"
                ),
                post_selection_search=False,
            ),
            "attempt_replay_receipt": _provenance_receipt(
                "attempt_replay_receipt",
                common,
                lane_sha256={name: _sha(value) for name, value in lanes.items()},
                stress_method_receipt_sha256=stress_receipt["receipt_sha256"],
                stress_method_receipt=stress_receipt,
                signal_vector_receipt=signal_receipt,
                position_events_are_genuine_backtester_v2=True,
            ),
            "oos_source_receipt": _provenance_receipt(
                "oos_source_receipt",
                common,
                native_prefix_index_sha256=cross["native_prefix_index_sha256"],
                native_prefix_content_sha256=artifacts["horizon_process"][
                    "source_snapshot_sha256"
                ],
                source_contract_sha256=frozen.precommitment["source_contract_sha256"],
                native_source_sha256=source_binding["target_bars_sha256"],
                activation_manifest_sha256=source_binding[
                    "price_activation_manifest_sha256"
                ],
                source_group_decision_sha256=cross["source_group_decision_sha256"],
                regime_fit_decision_sha256=cross["regime_fit_decision_sha256"],
                execution_spec_receipt=_execution_spec_receipt(frozen, cross),
                partition_frame_sha256=[item["frame_sha256"] for item in geometry],
            ),
        }
    return {
        "fold_ordinal": fold_ordinal,
        "provenance": provenance,
        "lanes": lanes,
    }


def test_symbol_capture_uses_raw_winner_and_empirical_unobserved_states(
    tmp_path, observed_native_prefix
):
    context = _context(tmp_path)
    anchor = _anchor()
    request = _request(context, anchor)
    symbol = book.PINNED_SYMBOL_ORDER[0]
    capture = adapter.open_historical_symbol_capture(context, request, symbol)
    fold_plan = _fold_plan()
    seed_result = _raw_fold(0, fold_plan[0])["lanes"]["optimizer_train_base"]
    source_seed = raw_winner.build_trade_source_contract_from_backtester_result(
        seed_result
    )
    sample = build_trade_sample_evidence(
        [], source_contract=source_seed, minimum_observations=8
    )
    assert sample.source_contract is not None
    source_sha = sample.source_contract.source_contract_sha256
    attempt_artifacts = [
        _attempt_artifact(symbol, native_prefix_product=observed_native_prefix)
    ]
    real_horizon_plan = _real_horizon_plan(
        anchor,
        symbol,
        "TREND",
        native_prefix_product=observed_native_prefix,
        attempt_artifact=attempt_artifacts[0],
        fold_plan=fold_plan,
    )

    for timeframe in book.TIMEFRAME_ORDER:
        if timeframe == OBSERVED_TIMEFRAME:
            source, fit = _available_authority(
                symbol,
                timeframe,
                source_sha,
                native_prefix_body=observed_native_prefix.native_prefix,
            )
            available = True
        else:
            source, fit = _terminal_authority(anchor["anchor_id"], symbol, timeframe)
            available = False
        horizon_plans = _plans(
            anchor,
            symbol,
            timeframe,
            source_available=available,
        )
        if timeframe == OBSERVED_TIMEFRAME:
            horizon_plans[0] = real_horizon_plan
            for ordinal, regime in enumerate(book.REGIME_ORDER[1:], start=1):
                horizon_plans[ordinal] = _real_unobserved_horizon_plan(
                    anchor,
                    symbol,
                    timeframe,
                    regime,
                    native_prefix_body=observed_native_prefix.native_prefix,
                    source_decision=source,
                    fit_decision=fit,
                )
        capture.bind_timeframe(
            timeframe,
            horizon_plans=horizon_plans,
            source_group_decision=source,
            regime_fit_decision=fit,
        )

    handle = capture.freeze_observed_cell(
        OBSERVED_TIMEFRAME,
        "TREND",
        native_prefix_product=observed_native_prefix,
        precommitted_at_utc="2020-01-01T00:00:00Z",
        attempts_frozen_at_utc="2020-01-02T00:00:00Z",
        fold_plan=fold_plan,
        attempt_artifacts=attempt_artifacts,
        source_contract_seed=source_seed,
        base_cost_contract=BASE_COST_POLICY,
        stress_cost_contract=STRESS_COST_POLICY,
        cost_stress_adversity_plan=_cost_stress_adversity_plan(
            horizon_plan=real_horizon_plan,
            fold_plan=fold_plan,
            symbol=symbol,
        ),
        risk_unit_contract=RISK_CONTRACT,
    )
    plan_candidate = real_horizon_plan.to_dict()["candidates"][0]
    horizon_process = attempt_artifacts[0]["horizon_process"]
    assert plan_candidate["realized_start_utc"] != horizon_process["fit_start_utc"]
    assert handle._horizon_execution_contracts == (
        {
            "lookback_kind": "CALENDAR_INTERVAL",
            "lookback_value": 120,
            "source_bar_count": 120,
        },
    )
    source_count_receipt = handle._source_count_receipts[0]
    receipt_body = dict(source_count_receipt)
    assert receipt_body.pop("receipt_sha256") == _sha(receipt_body)
    assert source_count_receipt["native_prefix_index_sha256"] == (
        observed_native_prefix.index_sha256
    )
    assert source_count_receipt["fit_start_utc"] == horizon_process[
        "fit_start_utc"
    ].replace("+00:00", "Z")
    assert source_count_receipt["fit_start_utc"] != plan_candidate["realized_start_utc"]
    assert (
        source_count_receipt["fit_frame_sha256"]
        != (observed_native_prefix.native_prefix["prefix_content_sha256"])
    )
    production = capture.complete_observed_cell(
        handle,
        attempt_evidence=[
            {
                "attempt_id": handle.frozen.attempt_ledger["attempts"][0]["attempt_id"],
                "folds": [
                    _raw_fold(index, planned, handle)
                    for index, planned in enumerate(fold_plan)
                ],
            }
        ],
        evidence_access_started_at_utc="2025-01-01T00:00:00Z",
        evidence_access_completed_at_utc="2025-01-01T00:00:01Z",
    )
    assert production.request.precommitment.to_dict()["regime"] == "TREND"
    product = _finish_through_worker_bridge(context, anchor, capture)
    returned_plans = list(product.horizon_plans)
    assert len(returned_plans) == 24
    assert all(training_horizon._valid_plan(plan) for plan in returned_plans)
    assert [
        (
            plan.to_dict()["cell"]["timeframe"],
            plan.to_dict()["cell"]["regime"],
        )
        for plan in returned_plans
    ] == [
        (timeframe, regime)
        for timeframe in book.TIMEFRAME_ORDER
        for regime in book.REGIME_ORDER
    ]
    payload = product.optimizer_payload
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
    root_id = f"{anchor['anchor_id']}/adapter/raw-winner-descriptor-graph"
    index_id = f"{anchor['anchor_id']}/adapter/production-index"
    assert artifacts[root_id]["descriptor"] == payload["raw_descriptor_graph"]
    assert artifacts[index_id]["descriptor"] == payload["production_index"]
    assert (
        f"{anchor['anchor_id']}/adapter/{OBSERVED_TIMEFRAME}/TREND/frozen-receipt"
        not in artifacts
    )
    root_graph = _read_descriptor_json(context, payload["raw_descriptor_graph"])
    for root_entry in root_graph["artifacts"]:
        assert artifacts[root_entry["artifact_id"]] == root_entry
        if root_entry["artifact_type"] != "historical_raw_winner_descriptor_graph":
            continue
        phase_graph = _read_descriptor_json(context, root_entry["descriptor"])
        for leaf_entry in phase_graph["artifacts"]:
            assert artifacts[leaf_entry["artifact_id"]] == leaf_entry
    assert payload["cell_count"] == 24
    assert payload["state_counts"] == {
        "REGIME_UNOBSERVED": 3,
        "TERMINAL_SOURCE_ABSENCE": 20,
        "WINNER_DECISION": 1,
    }
    assert payload["raw_artifact_namespace"] == (
        "historical-anchor/historical-run-001/anchor-01"
    )
    assert payload["raw_descriptor_graph_root_count"] == 3
    cells = list(product.book_evidence)
    assert len(cells) == 24
    observed = [cell for cell in cells if cell.timeframe == OBSERVED_TIMEFRAME]
    assert observed[0].winner_decision is not production.decision
    assert observed[0].winner_decision.to_dict() == production.decision.to_dict()
    assert [cell.process_state_decision.state_kind for cell in observed[1:]] == [
        book.REGIME_UNOBSERVED,
        book.REGIME_UNOBSERVED,
        book.REGIME_UNOBSERVED,
    ]


def test_zero_support_cannot_be_inferred_from_a_ready_plan(
    tmp_path, observed_native_prefix
):
    context = _context(tmp_path)
    anchor = _anchor()
    request = _request(context, anchor)
    symbol = book.PINNED_SYMBOL_ORDER[0]
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
    symbol = book.PINNED_SYMBOL_ORDER[0]
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


def test_all_process_rejected_raw_winner_becomes_bound_insufficient_evidence(
    tmp_path, observed_native_prefix
):
    context = _context(tmp_path)
    anchor = _anchor()
    request = _request(context, anchor)
    symbol = book.PINNED_SYMBOL_ORDER[0]
    capture = adapter.open_historical_symbol_capture(context, request, symbol)
    fold_plan = _fold_plan()
    seed_result = _raw_fold(0, fold_plan[0])["lanes"]["optimizer_train_base"]
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
    handle = capture.freeze_observed_cell(
        OBSERVED_TIMEFRAME,
        "TREND",
        native_prefix_product=observed_native_prefix,
        precommitted_at_utc="2020-01-01T00:00:00Z",
        attempts_frozen_at_utc="2020-01-02T00:00:00Z",
        fold_plan=fold_plan,
        attempt_artifacts=[artifact],
        source_contract_seed=source_seed,
        base_cost_contract=BASE_COST_POLICY,
        stress_cost_contract=STRESS_COST_POLICY,
        cost_stress_adversity_plan=_cost_stress_adversity_plan(
            horizon_plan=plan,
            fold_plan=fold_plan,
            symbol=symbol,
        ),
        risk_unit_contract=RISK_CONTRACT,
    )
    attempt = {
        "attempt_id": handle.frozen.attempt_ledger["attempts"][0]["attempt_id"],
        "folds": [
            _raw_fold(index, planned, handle) for index, planned in enumerate(fold_plan)
        ],
    }
    attempt["folds"][0]["lanes"]["optimizer_train_base"]["position_events"] = attempt[
        "folds"
    ][0]["lanes"]["optimizer_train_base"]["position_events"][:2]
    replay_receipt = attempt["folds"][0]["provenance"]["attempt_replay_receipt"]
    replay_receipt["lane_sha256"]["optimizer_train_base"] = _sha(
        attempt["folds"][0]["lanes"]["optimizer_train_base"]
    )
    replay_receipt_body = dict(replay_receipt)
    replay_receipt_body.pop("receipt_sha256")
    replay_receipt["receipt_sha256"] = _sha(replay_receipt_body)
    cross_binding = json.loads(handle._cross_binding_bytes.decode("ascii"))
    assert (
        cross_binding["schema_version"] == adapter.HORIZON_WINNER_CROSS_BINDING_SCHEMA
    )
    assert cross_binding["mapping_count"] == 1
    assert cross_binding["mapping_sha256"] == _sha(cross_binding["mapping"])
    mapping = cross_binding["mapping"][0]
    assert (
        mapping["horizon_trial_definition_sha256"] != mapping["trial_definition_sha256"]
    )
    assert (
        handle.cross_binding_descriptor["sha256"]
        == hashlib.sha256(handle._cross_binding_bytes).hexdigest()
    )
    production = capture.complete_observed_cell(
        handle,
        attempt_evidence=[attempt],
        evidence_access_started_at_utc="2025-01-01T00:00:00Z",
        evidence_access_completed_at_utc="2025-01-01T00:00:01Z",
    )
    assert production.decision.status == "NO_PROCESS_WINNER"
    product = _finish_through_worker_bridge(context, anchor, capture)
    trend = next(
        cell
        for cell in product.book_evidence
        if cell.timeframe == OBSERVED_TIMEFRAME and cell.regime == "TREND"
    )
    state = trend.process_state_decision.to_dict()
    binding = state["insufficient_evidence_binding"]
    receipt_descriptor = production.entries["production-receipt"]["descriptor"]
    assert state["state_kind"] == book.INSUFFICIENT_EVIDENCE
    assert state["attempted_trials_exact"] == 1
    assert binding["terminal_stage"] == book.INSUFFICIENT_POSTSEARCH_STAGE
    assert binding["process_rejection_request_sha256"] == (
        production.request.request_sha256
    )
    assert binding["process_rejection_decision_sha256"] == (
        production.decision.receipt_sha256
    )
    assert binding["horizon_winner_cross_binding_schema_version"] == (
        adapter.HORIZON_WINNER_CROSS_BINDING_SCHEMA
    )
    assert (
        binding["horizon_winner_cross_binding_content_sha256"]
        == (handle.cross_binding_descriptor["sha256"])
    )
    assert (
        binding["horizon_winner_cross_binding_mapping_sha256"]
        == (cross_binding["mapping_sha256"])
    )
    assert binding["winner_raw_production_schema_version"] == (
        raw_winner.PRODUCTION_SCHEMA
    )
    assert (
        binding["winner_raw_production_receipt_sha256"] == receipt_descriptor["sha256"]
    )
    assert product.optimizer_payload["state_counts"][book.INSUFFICIENT_EVIDENCE] == 1


def test_raw_production_receipt_reopen_pins_bounded_spool_primitive(
    tmp_path, monkeypatch
):
    context = _context(tmp_path)
    sink = spool.open_symbol_sink(context, book.PINNED_SYMBOL_ORDER[0])
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
    symbol = book.PINNED_SYMBOL_ORDER[0]
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
    symbol = book.PINNED_SYMBOL_ORDER[0]
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
    symbol = book.PINNED_SYMBOL_ORDER[0]
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
    symbol = book.PINNED_SYMBOL_ORDER[0]
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
            context, forged, book.PINNED_SYMBOL_ORDER[0]
        )


def test_capture_uses_exact_request_sink_and_never_opens_private_sink(
    tmp_path, monkeypatch
):
    context = _context(tmp_path)
    anchor = _anchor()
    request = _request(context, anchor)
    symbol = book.PINNED_SYMBOL_ORDER[0]
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
            context, request, book.PINNED_SYMBOL_ORDER[0]
        )


def test_incomplete_executable_artifact_fails_before_raw_freeze(
    tmp_path, monkeypatch, observed_native_prefix
):
    context = _context(tmp_path)
    anchor = _anchor()
    request = _request(context, anchor)
    symbol = book.PINNED_SYMBOL_ORDER[0]
    capture = adapter.open_historical_symbol_capture(context, request, symbol)
    fold_plan = _fold_plan()
    seed_result = _raw_fold(0, fold_plan[0])["lanes"]["optimizer_train_base"]
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

    monkeypatch.setattr(raw_winner, "freeze_winner_search_plan_v3", forbidden)
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
            source_contract_seed=source_seed,
            base_cost_contract=BASE_COST_POLICY,
            stress_cost_contract=STRESS_COST_POLICY,
            cost_stress_adversity_plan=_cost_stress_adversity_plan(
                horizon_plan=plan,
                fold_plan=fold_plan,
                symbol=symbol,
            ),
            risk_unit_contract=RISK_CONTRACT,
        )
    assert not called


def test_attempt_source_count_is_recomputed_from_sealed_prefix(
    tmp_path, monkeypatch, observed_native_prefix
):
    context = _context(tmp_path)
    anchor = _anchor()
    request = _request(context, anchor)
    symbol = book.PINNED_SYMBOL_ORDER[0]
    capture = adapter.open_historical_symbol_capture(context, request, symbol)
    fold_plan = _fold_plan()
    seed_result = _raw_fold(0, fold_plan[0])["lanes"]["optimizer_train_base"]
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

    monkeypatch.setattr(raw_winner, "freeze_winner_search_plan_v3", forbidden)
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
            source_contract_seed=source_seed,
            base_cost_contract=BASE_COST_POLICY,
            stress_cost_contract=STRESS_COST_POLICY,
            cost_stress_adversity_plan=_cost_stress_adversity_plan(
                horizon_plan=plan,
                fold_plan=fold_plan,
                symbol=symbol,
            ),
            risk_unit_contract=RISK_CONTRACT,
        )
    assert not raw_called


def test_coherent_cutoff_window_splice_fails_against_authenticated_prefix(
    observed_native_prefix,
):
    symbol = book.PINNED_SYMBOL_ORDER[0]
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
    symbol = book.PINNED_SYMBOL_ORDER[0]
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


def test_duplicate_depth_same_trial_is_distinct_when_identity_is_horizon_bound(
    tmp_path, observed_native_prefix
):
    context = _context(tmp_path)
    anchor = _anchor()
    request = _request(context, anchor)
    symbol = book.PINNED_SYMBOL_ORDER[0]
    capture = adapter.open_historical_symbol_capture(context, request, symbol)
    fold_plan = _fold_plan()
    seed_result = _raw_fold(0, fold_plan[0])["lanes"]["optimizer_train_base"]
    source_seed = raw_winner.build_trade_source_contract_from_backtester_result(
        seed_result
    )
    sample = build_trade_sample_evidence(
        [], source_contract=source_seed, minimum_observations=8
    )
    assert sample.source_contract is not None
    attempts = [
        _attempt_artifact(
            symbol,
            native_prefix_product=observed_native_prefix,
            lookback_kind="CALENDAR_INTERVAL",
            lookback_value=500,
        ),
        _attempt_artifact(
            symbol,
            native_prefix_product=observed_native_prefix,
            lookback_kind="CALENDAR_INTERVAL",
            lookback_value=120,
        ),
    ]
    assert (
        attempts[0]["winner_process"]["process_candidate_id"]
        != attempts[1]["winner_process"]["process_candidate_id"]
    )
    assert _sha(attempts[0]["horizon_process"]) != _sha(attempts[1]["horizon_process"])
    plan = _real_horizon_plan(
        anchor,
        symbol,
        "TREND",
        native_prefix_product=observed_native_prefix,
        attempt_artifact=attempts[0],
        additional_attempt_artifacts=attempts[1:],
        fold_plan=fold_plan,
    )
    assert len(plan.to_dict()["attempts"]) == 2
    _bind_single_observed_capture(
        capture,
        anchor,
        sample.source_contract.source_contract_sha256,
        observed_plan=plan,
    )

    handle = capture.freeze_observed_cell(
        OBSERVED_TIMEFRAME,
        "TREND",
        native_prefix_product=observed_native_prefix,
        precommitted_at_utc="2020-01-01T00:00:00Z",
        attempts_frozen_at_utc="2020-01-02T00:00:00Z",
        fold_plan=fold_plan,
        attempt_artifacts=attempts,
        source_contract_seed=source_seed,
        base_cost_contract=BASE_COST_POLICY,
        stress_cost_contract=STRESS_COST_POLICY,
        cost_stress_adversity_plan=_cost_stress_adversity_plan(
            horizon_plan=plan,
            fold_plan=fold_plan,
            symbol=symbol,
        ),
        risk_unit_contract=RISK_CONTRACT,
    )
    frozen_attempts = handle.frozen.attempt_ledger["attempts"]
    assert len(frozen_attempts) == 2
    assert len({attempt["process_candidate_id"] for attempt in frozen_attempts}) == 2
    cross_binding = _read_descriptor_json(context, handle.cross_binding_descriptor)
    assert cross_binding["mapping_count"] == 2
    assert len({item["horizon_candidate_id"] for item in cross_binding["mapping"]}) == 2


def test_horizon_registry_splice_fails_before_winner_evidence(
    tmp_path, observed_native_prefix
):
    context = _context(tmp_path)
    anchor = _anchor()
    request = _request(context, anchor)
    symbol = book.PINNED_SYMBOL_ORDER[0]
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


def test_cross_anchor_raw_artifacts_share_sink_without_id_collision(
    tmp_path, observed_native_prefix
):
    context = _context(tmp_path)
    first_anchor = _anchor("anchor-01", date(2020, 1, 1))
    second_anchor = _anchor("anchor-02", date(2022, 1, 1))
    first_request = _request(context, first_anchor)
    unbound_second = _request(context, second_anchor, bind_sinks=False)
    second_request = orchestration.AnchorProducerRequest(
        unbound_second._body_json, first_request._symbol_sinks
    )
    symbol = book.PINNED_SYMBOL_ORDER[0]
    fold_plan = _fold_plan()
    seed_result = _raw_fold(0, fold_plan[0])["lanes"]["optimizer_train_base"]
    source_seed = raw_winner.build_trade_source_contract_from_backtester_result(
        seed_result
    )
    sample = build_trade_sample_evidence(
        [], source_contract=source_seed, minimum_observations=8
    )
    assert sample.source_contract is not None
    source_sha = sample.source_contract.source_contract_sha256
    handles = []
    for anchor, request in (
        (first_anchor, first_request),
        (second_anchor, second_request),
    ):
        capture = adapter.open_historical_symbol_capture(context, request, symbol)
        attempt_artifact = _attempt_artifact(
            symbol, native_prefix_product=observed_native_prefix
        )
        observed_plan = _real_horizon_plan(
            anchor,
            symbol,
            "TREND",
            native_prefix_product=observed_native_prefix,
            attempt_artifact=attempt_artifact,
            fold_plan=fold_plan,
        )
        _bind_single_observed_capture(
            capture,
            anchor,
            source_sha,
            observed_plan=observed_plan,
        )
        handles.append(
            capture.freeze_observed_cell(
                OBSERVED_TIMEFRAME,
                "TREND",
                native_prefix_product=observed_native_prefix,
                precommitted_at_utc="2020-01-01T00:00:00Z",
                attempts_frozen_at_utc="2020-01-02T00:00:00Z",
                fold_plan=fold_plan,
                attempt_artifacts=[attempt_artifact],
                source_contract_seed=source_seed,
                base_cost_contract=BASE_COST_POLICY,
                stress_cost_contract=STRESS_COST_POLICY,
                cost_stress_adversity_plan=_cost_stress_adversity_plan(
                    horizon_plan=observed_plan,
                    fold_plan=fold_plan,
                    symbol=symbol,
                ),
                risk_unit_contract=RISK_CONTRACT,
            )
        )
    assert handles[0].artifact_namespace != handles[1].artifact_namespace
    sink = first_request.symbol_sink(symbol)
    receipt = sink.seal()
    manifest = spool.verify_symbol_receipt(
        context.root,
        receipt,
        expected_generation_id=context.generation_id,
        expected_symbol=symbol,
        expected_native_universe_sha256=context.native_universe_sha256,
        expected_config_sha256=context.config_sha256,
    )
    ids = {entry["artifact_id"] for entry in manifest["artifacts"]}
    assert (
        "historical-anchor/historical-run-001/anchor-01/"
        f"winner-search/{OBSERVED_TIMEFRAME}/TREND/frozen/precommitment"
    ) in ids
    assert (
        "historical-anchor/historical-run-001/anchor-02/"
        f"winner-search/{OBSERVED_TIMEFRAME}/TREND/frozen/precommitment"
    ) in ids


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
    symbol = book.PINNED_SYMBOL_ORDER[0]
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
