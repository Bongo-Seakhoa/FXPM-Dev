from __future__ import annotations

from copy import deepcopy
from dataclasses import replace
from datetime import date, datetime, timedelta, timezone
from functools import lru_cache
import hashlib
import importlib.util
import inspect
import json
import math
from pathlib import Path
import random
from typing import Any

import pytest

import pm_book_multiplicity as book
import pm_training_horizon as horizon
import pm_winner_selection as winner
from pm_book_multiplicity import (
    AVAILABLE,
    BOOK_MULTIPLICITY_CONTRACT_VERSION,
    BOOK_MULTIPLICITY_SCOPE,
    BookMultiplicityDecision,
    FAMILY_SIZE,
    HistoricalBookCellEvidence,
    HistoricalProcessStateDecision,
    INSUFFICIENT_ALL_ATTEMPTS_REJECTED_REASON,
    INSUFFICIENT_EVIDENCE,
    INSUFFICIENT_PRESEARCH_BOUNDARY_REASON,
    PINNED_SYMBOL_ORDER,
    PINNED_WINNER_ATTEMPT_LEDGER_VERSION,
    PINNED_WINNER_EVIDENCE_LEDGER_VERSION,
    PINNED_WINNER_SELECTION_CONTRACT_VERSION,
    PINNED_WINNER_SELECTION_REQUEST_VERSION,
    PINNED_WINNER_TRADE_POWER_BINDING_VERSION,
    REGIME_FITTED,
    REGIME_NOT_APPLICABLE,
    REGIME_NOT_APPLICABLE_TECHNICAL_TERMINAL,
    REGIME_ORDER,
    REGIME_UNOBSERVED,
    REGIME_UNOBSERVED_REASON,
    RegimeFitDecision,
    SELECTION_RULE,
    SOURCE_GROUP_SIZE,
    TARGET_FDR,
    TECHNICAL_FAILURE,
    TERMINAL_SOURCE_ABSENCE,
    TERMINAL_TECHNICAL,
    TERMINAL_UNAVAILABLE,
    TIMEFRAME_ORDER,
    UNSEARCHED,
    adjust_p_values_by,
    compute_cell_id,
    compute_source_group_id,
    create_historical_process_state_decision,
    create_historical_source_group_decision,
    create_regime_detector_record,
    create_regime_fit_decision,
    evaluate_historical_book_multiplicity,
    pinned_policy_snapshot,
    process_state_cell_evidence,
    verify_historical_book_multiplicity_decision,
    winner_cell_evidence,
)


def _canonical(value: Any) -> bytes:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("ascii")


def _sha(label: str) -> str:
    return hashlib.sha256(label.encode("utf-8")).hexdigest()


def _sha_json(value: Any) -> str:
    return hashlib.sha256(_canonical(value)).hexdigest()


def _detector_parameters(seed: int = 0) -> dict[str, int | float]:
    return {
        "k_confirm": 3 + seed,
        "gap_min": 0.1 + seed * 0.001,
        "k_hold": 5 + seed,
        "adx_period": 14 + seed,
        "atr_period": 14 + seed,
        "bb_period": 20 + seed,
        "bb_std": 2.0 + seed * 0.01,
        "efficiency_period": 10 + seed,
        "bb_squeeze_lookback": 50 + seed,
        "bb_squeeze_percentile": 20.0 + seed * 0.01,
        "atr_lookback": 100 + seed,
        "structure_period": 20 + seed,
        "structure_atr_mult": 0.1 + seed * 0.001,
        "trend_adx_weight": 0.4,
        "trend_efficiency_weight": 0.35,
        "trend_slope_weight": 0.25,
        "range_adx_weight": 0.35,
        "range_efficiency_weight": 0.3,
        "range_containment_weight": 0.35,
        "breakout_squeeze_weight": 0.35,
        "breakout_atr_weight": 0.3,
        "breakout_structure_weight": 0.35,
        "chop_adx_weight": 0.3,
        "chop_efficiency_weight": 0.3,
        "chop_whipsaw_weight": 0.4,
        "adx_trend_threshold": 25.0 + seed * 0.01,
    }


def _detector_record(
    source: Any,
    detector_sha256: str,
    *,
    seed: int = 0,
    cutoff: str = "2025-01-01T00:00:00+00:00",
) -> dict[str, Any]:
    record = create_regime_detector_record(
        source_group_decision=source,
        fit_cutoff_utc=cutoff,
        detector_version="fxpm.market-regime-detector/v1",
        detector_code_sha256=detector_sha256,
        parameters=_detector_parameters(seed),
    )
    assert record is not None
    return record


def _cell_tuple(ordinal: int) -> tuple[str, str, str]:
    symbol_block, regime_index = divmod(ordinal, len(REGIME_ORDER))
    symbol_index, timeframe_index = divmod(symbol_block, len(TIMEFRAME_ORDER))
    return (
        PINNED_SYMBOL_ORDER[symbol_index],
        TIMEFRAME_ORDER[timeframe_index],
        REGIME_ORDER[regime_index],
    )


def _group_tuple(group_ordinal: int) -> tuple[str, str]:
    symbol_index, timeframe_index = divmod(group_ordinal, len(TIMEFRAME_ORDER))
    return PINNED_SYMBOL_ORDER[symbol_index], TIMEFRAME_ORDER[timeframe_index]


def _train_support(
    symbol: str,
    timeframe: str,
    counts: tuple[int, int, int, int],
    *,
    activation_id: str,
    activation_manifest_sha256: str,
    native_source_sha256: str,
    params_sha256: str,
    detector_code_sha256: str,
    tag: str = "base",
) -> dict[str, Any]:
    count_map = dict(zip(REGIME_ORDER, counts, strict=True))
    observed = [regime for regime in REGIME_ORDER if count_map[regime] > 0]
    unobserved = [regime for regime in REGIME_ORDER if count_map[regime] == 0]
    body = {
        "schema_version": "fxpm.regime-train-support/v1",
        "symbol": symbol,
        "timeframe": timeframe,
        "surface": "REGIME_LIVE",
        "warmup_policy": "CURRENT_AND_SOURCE_BAR_NOT_WARMUP",
        "label_order": list(REGIME_ORDER),
        "eligible_rows": sum(counts),
        "counts": count_map,
        "observed_regimes": observed,
        "unobserved_regimes": unobserved,
        "support_state_by_regime": {
            regime: ("EMPIRICALLY_OBSERVED" if count_map[regime] > 0 else "UNOBSERVED")
            for regime in REGIME_ORDER
        },
        "first_ts": (
            "2020-01-01T00:00:00+00:00" if sum(counts) > 0 else None
        ),
        "last_ts": (
            "2024-12-31T23:55:00+00:00" if sum(counts) > 0 else None
        ),
        "vector_encoding": "fxpm.regime-live-labels-nul/v1",
        "vector_sha256": _sha(f"support-vector:{symbol}:{timeframe}:{counts}:{tag}"),
        "bindings": {
            "symbol": symbol,
            "timeframe": timeframe,
            "activation_id": activation_id,
            "activation_manifest_sha256": activation_manifest_sha256,
            "source_sha256": native_source_sha256,
            "train_window_sha256": _sha(f"train-window:{symbol}:{timeframe}:{tag}"),
            "params_sha256": params_sha256,
            "detector_code_sha256": detector_code_sha256,
        },
    }
    body["receipt_sha256"] = hashlib.sha256(_canonical(body)).hexdigest()
    return body


@lru_cache(maxsize=1)
def _winner_fixture_module() -> Any:
    fixture_path = Path(__file__).with_name("test_winner_selection.py")
    spec = importlib.util.spec_from_file_location(
        "_fxpm_book_winner_v11_fixture", fixture_path
    )
    if spec is None or spec.loader is None:
        raise RuntimeError("WINNER_FIXTURE_IMPORT_FAILED")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@lru_cache(maxsize=1)
def _horizon_fixture_module() -> Any:
    fixture_path = Path(__file__).with_name("test_training_horizon.py")
    spec = importlib.util.spec_from_file_location(
        "_fxpm_book_horizon_fixture", fixture_path
    )
    if spec is None or spec.loader is None:
        raise RuntimeError("HORIZON_FIXTURE_IMPORT_FAILED")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@lru_cache(maxsize=1200)
def _source_and_fit(
    symbol: str,
    timeframe: str,
    availability: str,
    counts: tuple[int, int, int, int] = (4, 3, 2, 1),
) -> tuple[Any, RegimeFitDecision]:
    activation_id = f"activation:{symbol}:{timeframe}"
    activation_manifest_sha256 = _sha("activation-manifest")
    native_source_sha256 = _sha(f"native-source:{symbol}:{timeframe}")
    detector_sha256 = _sha(f"detector-process:{symbol}:{timeframe}")
    parameters = _detector_parameters()
    params_sha256 = hashlib.sha256(_canonical(parameters)).hexdigest()
    available = availability == AVAILABLE
    technical_terminal = availability == TERMINAL_TECHNICAL
    source = create_historical_source_group_decision(
        symbol=symbol,
        timeframe=timeframe,
        availability=availability,
        reason_code=(
            "ACTIVATED_NATIVE_SOURCE_AVAILABLE"
            if available
            else (
                "AUTHENTICATED_NATIVE_PREFIX_TERMINAL_TECHNICAL"
                if technical_terminal
                else "BROKER_HISTORY_TERMINALLY_UNAVAILABLE"
            )
        ),
        source_contract_sha256=_sha("source-contract"),
        source_group_evidence_sha256=_sha(
            f"source-group-evidence:{symbol}:{timeframe}:{availability}"
        ),
        activation_id=(activation_id if available or technical_terminal else None),
        activation_manifest_sha256=(
            activation_manifest_sha256 if available or technical_terminal else None
        ),
        activation_entry_sha256=(
            _sha(f"activation-entry:{symbol}:{timeframe}")
            if technical_terminal
            else None
        ),
        native_source_sha256=(native_source_sha256 if available else None),
        acquisition_attempts_exact=3,
        terminal_proof_sha256=(
            None if available else _sha(f"terminal-proof:{symbol}:{timeframe}")
        ),
    )
    assert source is not None
    detector_record = _detector_record(source, detector_sha256) if available else None
    if available:
        assert detector_record is not None
    fit = create_regime_fit_decision(
        source_group_decision=source,
        fit_state=(
            REGIME_FITTED
            if available
            else (
                REGIME_NOT_APPLICABLE_TECHNICAL_TERMINAL
                if technical_terminal
                else REGIME_NOT_APPLICABLE
            )
        ),
        reason_code=(
            "ANCHOR_LOCAL_SHARED_REGIME_FIT_COMPLETE"
            if available
            else (
                "AUTHENTICATED_NATIVE_PREFIX_TERMINAL_TECHNICAL"
                if technical_terminal
                else "SOURCE_UNAVAILABLE_REGIME_FIT_NOT_APPLICABLE"
            )
        ),
        regime_fit_policy_sha256=_sha("regime-fit-policy"),
        detector_process_sha256=(detector_sha256 if available else None),
        regime_fit_evidence_sha256=(
            _sha(f"regime-fit-evidence:{symbol}:{timeframe}") if available else None
        ),
        four_regime_mapping_sha256=(
            _sha(f"four-regime-map:{symbol}:{timeframe}") if available else None
        ),
        regime_params_sha256=(params_sha256 if available else None),
        regime_detector_record=detector_record,
        train_regime_support=(
            _train_support(
                symbol,
                timeframe,
                counts,
                activation_id=activation_id,
                activation_manifest_sha256=activation_manifest_sha256,
                native_source_sha256=native_source_sha256,
                params_sha256=params_sha256,
                detector_code_sha256=detector_sha256,
            )
            if available
            else None
        ),
    )
    assert fit is not None
    return source, fit


def _insufficient_horizon_plan(
    source: Any,
    *,
    boundary: bool,
    winner_fold_plan: list[dict[str, Any]] | None = None,
    winner_attempts: list[dict[str, Any]] | None = None,
) -> Any:
    fixture = _horizon_fixture_module()
    request = fixture._request()
    source_body = source.to_dict()
    source_body.pop("receipt_sha256")
    request["run_id"] = "sparse-evidence-anchor-run"
    request["anchor"] = {
        "anchor_id": "anchor-sparse-evidence",
        "ordinal": 1,
        "selection_cutoff_utc": "2025-06-01T00:00:00Z",
        "forward_start_utc": "2025-07-06T00:00:00Z",
        "forward_end_exclusive_utc": "2025-08-06T00:00:00Z",
        "evidence_accessed_at_utc": "2026-07-13T12:00:00Z",
        "process_precommit_receipt_sha256": _sha("causal-precommit"),
    }
    request["cell"] = {
        "symbol": "AUDUSD",
        "timeframe": "M5",
        "regime": "TREND",
    }
    prefix = request["native_prefix"]
    prefix.update(
        {
            "symbol": "AUDUSD",
            "timeframe": "M5",
            "activation_id": source_body["activation_id"],
            "activation_manifest_sha256": source_body["activation_manifest_sha256"],
            "artifact_sha256": source_body["native_source_sha256"],
            "request_start_utc": "2017-01-01T00:00:00Z",
            "request_end_exclusive_utc": "2026-01-01T00:00:00Z",
            "prefix_start_utc": "2018-01-01T00:00:00Z",
            "prefix_end_exclusive_utc": "2025-06-01T00:00:00Z",
            "prefix_first_open_utc": "2018-01-01T00:00:00Z",
            "prefix_last_open_utc": "2025-05-31T23:50:00Z",
            "prefix_last_close_utc": "2025-05-31T23:55:00Z",
            "artifact_rows": 500_000,
            "prefix_rows": 400_000,
        }
    )
    if boundary:
        request["candidates"] = [
            fixture._candidate(
                "BOUNDARY-LIMITED",
                "CALENDAR_INTERVAL",
                365,
                "2017-01-01T00:00:00Z",
                [],
            )
        ]
    else:
        assert winner_fold_plan is not None
        folds: list[dict[str, Any]] = []
        for ordinal, raw in enumerate(winner_fold_plan):
            validation_start = datetime.fromisoformat(
                raw["anchor_as_of_utc"].replace("Z", "+00:00")
            )
            validation_end = datetime.combine(
                date.fromisoformat(raw["oos_end_date_utc"]) + timedelta(days=1),
                datetime.min.time(),
                tzinfo=timezone.utc,
            )
            fit_start = datetime(2018 + ordinal, 1, 1, tzinfo=timezone.utc)
            fit_end = validation_start - timedelta(days=1)
            warmup_start = validation_start - timedelta(hours=1)

            def iso(value: datetime) -> str:
                return value.isoformat(timespec="seconds").replace("+00:00", "Z")

            folds.append(
                fixture._fold(
                    f"CAUSAL-{ordinal + 1:02d}",
                    "CAUSAL_ROLLING_ORIGIN_INNER",
                    iso(fit_start),
                    iso(fit_end),
                    iso(warmup_start),
                    iso(validation_start),
                    iso(validation_end),
                )
            )
        request["candidates"] = [
            fixture._candidate(
                "ACTIVE-400",
                "ACTIVE_SESSIONS",
                400,
                "2018-01-01T00:00:00Z",
                folds,
            )
        ]
    if winner_attempts is None:
        request["trials"] = [
            fixture._trial("trial-001", "simple-beta", _sha("release"), _sha("params"))
        ]
    else:
        request["trials"] = [
            fixture._trial(
                f"trial-{ordinal + 1:03d}",
                attempt["strategy_id"],
                _sha(f"release:{ordinal}"),
                _sha(f"params:{ordinal}"),
            )
            for ordinal, attempt in enumerate(winner_attempts)
        ]
    return horizon.build_historical_horizon_plan(request)


def _all_process_rejected_request_and_plan(
    source: Any, *, splice_first_evidence_role: bool = False
) -> tuple[Any, Any, Any]:
    fixture = _winner_fixture_module()
    raw = fixture._payload_request("AUDUSD", "M5", "TREND")
    attempts = raw["attempt_ledger_payload"]["attempts"]
    fold_plan = raw["precommitment_payload"]["causal_fold_plan"]
    plan = _insufficient_horizon_plan(
        source,
        boundary=False,
        winner_fold_plan=fold_plan,
        winner_attempts=attempts,
    )
    _horizon_process, horizon_process_sha256 = _postsearch_horizon_process(
        plan.to_dict()
    )
    for attempt in attempts:
        attempt["horizon_process_sha256"] = horizon_process_sha256
    if splice_first_evidence_role:
        attempts[0]["precommitted_evidence_role"] = "PRESEARCH_PROCESS_REJECTED"
    entries = raw["evidence_ledger_payload"]["entries"]
    for attempt, entry in zip(attempts, entries, strict=True):
        entry["horizon_process_sha256"] = attempt["horizon_process_sha256"]
        entry["evidence_status"] = "PROCESS_REJECTED"
        if attempt["precommitted_evidence_role"] == "EVALUATION_REQUIRED":
            entry["status_reason"] = "EVALUATED_OOS_PROCESS_REJECTED"
        else:
            assert attempt["precommitted_evidence_role"] == (
                "PRESEARCH_PROCESS_REJECTED"
            )
            entry["status_reason"] = "PRESEARCH_PROCESS_REJECTED"
            entry["pbo_evidence_source"] = "NOT_APPLICABLE_PRESEARCH_REJECTION"
            entry["pbo_fold_count"] = 0
            entry["pbo_folds"] = []
            entry["pbo_folds_sha256"] = fixture._sha_json([])
        entry["fold_count"] = 0
        entry["folds"] = []
        entry["folds_sha256"] = fixture._sha_json([])
    trial_registry = fixture._trial_registry(attempts)
    process_registry = fixture._process_registry(attempts)
    horizon_registry = fixture._horizon_registry(attempts)
    for payload in (
        raw["precommitment_payload"],
        raw["attempt_ledger_payload"],
        raw["evidence_ledger_payload"],
    ):
        payload["process_registry_sha256"] = process_registry
        payload["horizon_registry_sha256"] = horizon_registry
        payload["trial_registry_sha256"] = trial_registry
    raw["attempt_ledger_payload"]["attempts_sha256"] = fixture._sha_json(attempts)
    precommit_sha = fixture._sha_json(raw["precommitment_payload"])
    raw["attempt_ledger_payload"]["precommitment_payload_sha256"] = precommit_sha
    attempt_sha = fixture._sha_json(raw["attempt_ledger_payload"])
    evidence = raw["evidence_ledger_payload"]
    evidence["precommitment_payload_sha256"] = precommit_sha
    evidence["attempt_ledger_payload_sha256"] = attempt_sha
    evidence["entries_sha256"] = fixture._sha_json(entries)
    request = fixture._typed_request(raw)
    decision = winner.evaluate_historical_cell_winner(request)
    assert decision.status == "NO_PROCESS_WINNER", decision.reason_codes
    assert decision.to_dict()["winner"] is None
    assert winner.verify_historical_cell_winner_decision(decision, request)
    return request, decision, plan


def _postsearch_horizon_process(
    plan_body: dict[str, Any],
) -> tuple[dict[str, Any], str]:
    candidate = plan_body["candidates"][0]
    prefix = plan_body["native_prefix"]
    return book.current_refit.build_horizon_process_artifact_v2(
        symbol=plan_body["cell"]["symbol"],
        timeframe=plan_body["cell"]["timeframe"],
        lookback_kind=candidate["lookback_kind"],
        lookback_value=candidate["lookback_value"],
        fit_start_utc="2024-06-01T00:00:00Z",
        fit_end_utc=prefix["prefix_end_exclusive_utc"],
        latest_closed_bar_utc=prefix["prefix_last_close_utc"],
        source_snapshot_sha256=prefix["prefix_content_sha256"],
        source_bar_count=400,
    )


def _postsearch_authority_bytes(
    source: Any,
    fit: Any,
    request: Any,
    decision: Any,
    plan: Any,
) -> tuple[bytes, bytes]:
    plan_body = plan.to_dict()
    precommitment = request.precommitment.to_dict()
    attempt_ledger = request.attempt_ledger.to_dict()
    evidence_ledger = request.evidence_ledger.to_dict()
    candidates = {item["candidate_id"]: item for item in plan_body["candidates"]}
    trials = {item["trial_id"]: item for item in plan_body["trials"]}
    expected_by_attempt: dict[str, list[dict[str, Any]]] = {}
    for item in plan_body["expected_evaluations"]:
        expected_by_attempt.setdefault(item["attempt_id"], []).append(item)
    mapping: list[dict[str, Any]] = []
    prefix = plan_body["native_prefix"]
    horizon_process, horizon_process_sha256 = _postsearch_horizon_process(plan_body)

    def utc_seconds(value: str) -> str:
        return (
            datetime.fromisoformat(value.replace("Z", "+00:00"))
            .astimezone(timezone.utc)
            .isoformat(timespec="seconds")
            .replace("+00:00", "Z")
        )

    for ordinal, (planned, attempted) in enumerate(
        zip(plan_body["attempts"], attempt_ledger["attempts"], strict=True)
    ):
        candidate = candidates[planned["candidate_id"]]
        trial = trials[planned["trial_id"]]
        execution = {
            "lookback_kind": horizon_process["lookback_kind"],
            "lookback_value": horizon_process["lookback_value"],
            "source_bar_count": horizon_process["source_bar_count"],
        }
        assert attempted["horizon_process_sha256"] == horizon_process_sha256
        source_count = {
            "schema_version": "fxpm.historical-attempt-source-count.v1",
            "anchor_id": plan_body["anchor"]["anchor_id"],
            "symbol": plan_body["cell"]["symbol"],
            "timeframe": plan_body["cell"]["timeframe"],
            "regime": plan_body["cell"]["regime"],
            "horizon_plan_receipt_sha256": plan.receipt_sha256,
            "native_prefix_index_sha256": prefix["prefix_index_sha256"],
            "native_prefix_content_sha256": prefix["prefix_content_sha256"],
            "horizon_attempt_id": planned["attempt_id"],
            "horizon_candidate_id": planned["candidate_id"],
            "horizon_candidate_definition_sha256": candidate[
                "candidate_definition_sha256"
            ],
            **execution,
            "fit_start_utc": utc_seconds(horizon_process["fit_start_utc"]),
            "fit_end_exclusive_utc": utc_seconds(horizon_process["fit_end_utc"]),
            "latest_closed_bar_utc": utc_seconds(
                horizon_process["latest_closed_bar_utc"]
            ),
            "fit_frame_sha256": _sha(f"fit-frame:{ordinal}"),
        }
        source_count["receipt_sha256"] = _sha_json(source_count)
        causal_expected = [
            item
            for item in expected_by_attempt[planned["attempt_id"]]
            if item["downstream_causal_eligible"] is True
        ]
        fold_mapping = []
        for expected, winner_fold in zip(
            causal_expected,
            precommitment["causal_fold_plan"],
            strict=True,
        ):
            candidate_fold = next(
                item
                for item in candidate["folds"]
                if item["fold_id"] == expected["fold_id"]
            )
            fold_mapping.append(
                {
                    "horizon_fold_id": expected["fold_id"],
                    "horizon_fold_definition_sha256": candidate_fold[
                        "fold_definition_sha256"
                    ],
                    "horizon_context_sha256": expected["context_sha256"],
                    "winner_fold_id": winner_fold["fold_id"],
                    "validation_start_utc": expected["validation_start_utc"],
                    "validation_end_exclusive_utc": expected[
                        "validation_end_exclusive_utc"
                    ],
                }
            )
        mapping.append(
            {
                "ordinal": ordinal,
                "horizon_attempt_id": planned["attempt_id"],
                "horizon_candidate_id": planned["candidate_id"],
                "horizon_candidate_definition_sha256": candidate[
                    "candidate_definition_sha256"
                ],
                "horizon_trial_id": planned["trial_id"],
                "horizon_trial_definition_sha256": trial["trial_definition_sha256"],
                "winner_attempt_id": attempted["attempt_id"],
                "winner_process_candidate_id": attempted["process_candidate_id"],
                "process_sha256": attempted["process_sha256"],
                "horizon_process_sha256": attempted["horizon_process_sha256"],
                "trial_definition_sha256": attempted["trial_definition_sha256"],
                "refit_recipe_sha256": attempted["refit_recipe_sha256"],
                "horizon_execution_contract": execution,
                "source_count_receipt": source_count,
                "fold_mapping": fold_mapping,
                "fold_mapping_sha256": _sha_json(fold_mapping),
            }
        )
    binding = {
        "schema_version": book.HORIZON_WINNER_CROSS_BINDING_SCHEMA_VERSION,
        "producer_request_sha256": _sha("historical-producer-request"),
        "anchor_id": plan_body["anchor"]["anchor_id"],
        "selection_cutoff_utc": plan_body["anchor"]["selection_cutoff_utc"],
        "process_precommit_receipt_sha256": plan_body["anchor"][
            "process_precommit_receipt_sha256"
        ],
        "source_group_decision_sha256": source.receipt_sha256,
        "regime_fit_decision_sha256": fit.receipt_sha256,
        "native_prefix_index_sha256": prefix["prefix_index_sha256"],
        **plan_body["cell"],
        "artifact_namespace": "historical-anchor/anchor-sparse-evidence",
        "horizon_roster": {
            "plan_receipt_sha256": plan.receipt_sha256,
            "source_prefix_receipt_sha256": plan_body["source_prefix_receipt_sha256"],
            "candidate_registry_sha256": plan_body["candidate_registry_sha256"],
            "trial_registry_sha256": plan_body["trial_registry_sha256"],
            "attempt_roster_sha256": plan_body["attempt_roster_sha256"],
            "evidence_roster_sha256": plan_body["evidence_roster_sha256"],
        },
        "winner_roster": {
            "precommitment_sha256": _sha_json(precommitment),
            "attempt_ledger_sha256": _sha_json(attempt_ledger),
            "frozen_receipt_sha256": _sha("frozen-winner-plan-receipt"),
            "process_registry_sha256": precommitment["process_registry_sha256"],
            "horizon_registry_sha256": precommitment["horizon_registry_sha256"],
            "trial_registry_sha256": precommitment["trial_registry_sha256"],
            "attempts_sha256": attempt_ledger["attempts_sha256"],
            "causal_fold_plan_sha256": precommitment["causal_fold_plan_sha256"],
            "source_contract_sha256": precommitment["source_contract_sha256"],
            "cost_contract_sha256": precommitment["cost_contract_sha256"],
            "cost_stress_contract_sha256": precommitment["cost_stress_contract_sha256"],
        },
        "mapping_count": len(mapping),
        "mapping_sha256": _sha_json(mapping),
        "mapping": mapping,
        "bijective_coverage": True,
        "created_before_winner_evidence_access": True,
        "confers_causal_current_live_or_capital_authority": False,
    }
    cross_binding_bytes = _canonical(binding)

    def descriptor(name: str, payload: Any) -> dict[str, Any]:
        encoded = _canonical(payload)
        digest = hashlib.sha256(encoded).hexdigest()
        return {
            "schema_version": "fxpm.authority-spool-artifact-descriptor.v1",
            "relative_path": f"objects/{digest[:2]}/{digest}-{name}.json",
            "sha256": digest,
            "size_bytes": len(encoded),
            "format": "fxpm-ascii-json-v1",
        }

    production = {
        "schema_version": book.WINNER_RAW_EVIDENCE_PRODUCTION_SCHEMA_VERSION,
        "generation_context_sha256": _sha("generation-context"),
        "run_id": precommitment["run_id"],
        "artifact_namespace": binding["artifact_namespace"],
        **plan_body["cell"],
        "precommitment_sha256": _sha_json(precommitment),
        "attempt_ledger_sha256": _sha_json(attempt_ledger),
        "provenance_cross_binding_sha256": hashlib.sha256(
            cross_binding_bytes
        ).hexdigest(),
        "evidence_ledger_sha256": _sha_json(evidence_ledger),
        "request_sha256": request.request_sha256,
        "decision_receipt_sha256": decision.receipt_sha256,
        "artifact_roster": {
            "evidence-ledger": descriptor("evidence-ledger", evidence_ledger),
            "winner-request": descriptor("winner-request", request.to_dict()),
            "winner-decision": descriptor("winner-decision", decision.to_dict()),
        },
    }
    return cross_binding_bytes, _canonical(production)


def _insufficient_cell(*, postsearch: bool) -> HistoricalBookCellEvidence:
    source, fit = _source_and_fit("AUDUSD", "M5", AVAILABLE)
    if postsearch:
        request, decision, plan = _all_process_rejected_request_and_plan(source)
        cross_binding_bytes, raw_production_receipt_bytes = _postsearch_authority_bytes(
            source, fit, request, decision, plan
        )
        reason = INSUFFICIENT_ALL_ATTEMPTS_REJECTED_REASON
        attempts = request.attempt_ledger.to_dict()["attempt_count"]
    else:
        request = decision = None
        cross_binding_bytes = raw_production_receipt_bytes = None
        plan = _insufficient_horizon_plan(source, boundary=True)
        reason = INSUFFICIENT_PRESEARCH_BOUNDARY_REASON
        attempts = 0
    state = create_historical_process_state_decision(
        symbol="AUDUSD",
        timeframe="M5",
        regime="TREND",
        state_kind=INSUFFICIENT_EVIDENCE,
        reason_code=reason,
        source_group_decision=source,
        regime_fit_decision=fit,
        attempted_trials_exact=attempts,
        horizon_plan=plan,
        process_rejection_request=request,
        process_rejection_decision=decision,
        horizon_winner_cross_binding_bytes=cross_binding_bytes,
        winner_raw_production_receipt_bytes=raw_production_receipt_bytes,
    )
    assert state is not None
    evidence = process_state_cell_evidence(
        process_state_decision=state,
        source_group_decision=source,
        regime_fit_decision=fit,
        horizon_plan=plan,
        process_rejection_request=request,
        process_rejection_decision=decision,
        horizon_winner_cross_binding_bytes=cross_binding_bytes,
        winner_raw_production_receipt_bytes=raw_production_receipt_bytes,
    )
    assert evidence is not None
    return evidence


def _typed_request(
    symbol: str,
    timeframe: str,
    regime: str,
    *,
    no_winner: bool,
) -> winner.WinnerSelectionRequest:
    fixture = _winner_fixture_module()
    raw = fixture._payload_request(symbol, timeframe, regime)
    if no_winner:
        entries = raw["evidence_ledger_payload"]["entries"]
        fold_means = (0.08, 0.08, 0.08, -0.01)
        for fold_index, fold in enumerate(entries[0]["folds"]):
            base = [
                round(
                    fold_means[fold_index]
                    + 0.05 * math.sin((index + fold_index * 7) * 1.70),
                    12,
                )
                for index in range(63)
            ]
            fold["daily_net_mtm_r"] = base
            fold["daily_net_mtm_r_cost_stress"] = [
                round(value - 0.005, 12) for value in base
            ]
            fixture._refresh_fold_accounting(fold)
        fixture._sync_entry_pbo(entries[0])
        entries[0]["folds_sha256"] = fixture._sha_json(entries[0]["folds"])
        for entry in entries[1:]:
            entry["evidence_status"] = "PROCESS_REJECTED"
            entry["status_reason"] = "EVALUATED_OOS_PROCESS_REJECTED"
            entry["fold_count"] = 0
            entry["folds"] = []
            entry["folds_sha256"] = fixture._sha_json([])
        raw["evidence_ledger_payload"]["entries_sha256"] = fixture._sha_json(entries)
    typed = fixture._typed_request(raw)
    assert type(typed) is winner.WinnerSelectionRequest
    return typed


def _typed_moderate_winner_request(
    symbol: str,
    timeframe: str,
    regime: str,
) -> winner.WinnerSelectionRequest:
    """Build a real V13-local winner whose flat-family BY q-value is unresolved."""

    fixture = _winner_fixture_module()
    raw = fixture._payload_request(symbol, timeframe, regime)
    evidence = raw["evidence_ledger_payload"]
    entries = evidence["entries"]
    for fold_index, fold in enumerate(entries[0]["folds"]):
        base = [
            round(
                0.02 + 0.15 * math.sin((index + fold_index * 7) * 1.70),
                12,
            )
            for index in range(63)
        ]
        fold["daily_net_mtm_r"] = base
        fold["daily_net_mtm_r_cost_stress"] = [
            round(value - 0.002, 12) for value in base
        ]
        fixture._refresh_fold_accounting(fold)
    fixture._sync_entry_pbo(entries[0])
    entries[0]["folds_sha256"] = fixture._sha_json(entries[0]["folds"])
    # Winner v13 requires a genuine multi-column search matrix for CSCV PBO.
    # Keep one authenticated losing process candidate so the preferred column
    # wins the precommitted PBO/bootstrap gauntlet without inventing evidence.
    fixture._make_authenticated_pbo_floor(entries[1])
    for entry in entries[2:]:
        entry["evidence_status"] = "PROCESS_REJECTED"
        entry["status_reason"] = "EVALUATED_OOS_PROCESS_REJECTED"
        entry["fold_count"] = 0
        entry["folds"] = []
        entry["folds_sha256"] = fixture._sha_json([])
    evidence["entries_sha256"] = fixture._sha_json(entries)
    typed = fixture._typed_request(raw)
    assert type(typed) is winner.WinnerSelectionRequest
    return typed


@lru_cache(maxsize=64)
def _verified_winner(
    symbol: str = "AUDUSD",
    timeframe: str = "M5",
    regime: str = "TREND",
) -> HistoricalBookCellEvidence:
    request = _typed_request(symbol, timeframe, regime, no_winner=False)
    decision = winner.evaluate_historical_cell_winner(request)
    assert decision.status == "PROCESS_WINNER_PENDING_BOOK_MULTIPLICITY"
    assert winner.verify_historical_cell_winner_decision(decision, request)
    source, fit = _source_and_fit(symbol, timeframe, AVAILABLE)
    evidence = winner_cell_evidence(decision, request, source, fit)
    assert evidence is not None
    return evidence


@lru_cache(maxsize=8)
def _verified_moderate_winner(
    symbol: str = "AUDUSD",
    timeframe: str = "M5",
    regime: str = "TREND",
) -> HistoricalBookCellEvidence:
    request = _typed_moderate_winner_request(symbol, timeframe, regime)
    decision = winner.evaluate_historical_cell_winner(request)
    assert decision.status == "PROCESS_WINNER_PENDING_BOOK_MULTIPLICITY"
    assert winner.verify_historical_cell_winner_decision(decision, request)
    source, fit = _source_and_fit(symbol, timeframe, AVAILABLE)
    evidence = winner_cell_evidence(decision, request, source, fit)
    assert evidence is not None
    return evidence


@lru_cache(maxsize=64)
def _verified_no_winner(
    symbol: str = "AUDUSD",
    timeframe: str = "M5",
    regime: str = "TREND",
) -> HistoricalBookCellEvidence:
    request = _typed_request(symbol, timeframe, regime, no_winner=True)
    decision = winner.evaluate_historical_cell_winner(request)
    assert decision.status == "NO_PROCESS_WINNER", decision.reason_codes
    assert winner.verify_historical_cell_winner_decision(decision, request)
    source, fit = _source_and_fit(symbol, timeframe, AVAILABLE)
    evidence = winner_cell_evidence(decision, request, source, fit)
    assert evidence is not None
    return evidence


@lru_cache(maxsize=FAMILY_SIZE)
def _terminal_cell(ordinal: int) -> HistoricalBookCellEvidence:
    symbol, timeframe, regime = _cell_tuple(ordinal)
    source, fit = _source_and_fit(symbol, timeframe, TERMINAL_UNAVAILABLE)
    state = create_historical_process_state_decision(
        symbol=symbol,
        timeframe=timeframe,
        regime=regime,
        state_kind=TERMINAL_SOURCE_ABSENCE,
        reason_code="BROKER_HISTORY_TERMINALLY_UNAVAILABLE",
        source_group_decision=source,
        regime_fit_decision=fit,
        attempted_trials_exact=0,
    )
    assert state is not None
    evidence = process_state_cell_evidence(
        process_state_decision=state,
        source_group_decision=source,
        regime_fit_decision=fit,
    )
    assert evidence is not None
    return evidence


@lru_cache(maxsize=FAMILY_SIZE)
def _technical_terminal_cell(ordinal: int) -> HistoricalBookCellEvidence:
    symbol, timeframe, regime = _cell_tuple(ordinal)
    source, fit = _source_and_fit(symbol, timeframe, TERMINAL_TECHNICAL)
    state = create_historical_process_state_decision(
        symbol=symbol,
        timeframe=timeframe,
        regime=regime,
        state_kind=TERMINAL_TECHNICAL,
        reason_code="AUTHENTICATED_NATIVE_PREFIX_TERMINAL_TECHNICAL",
        source_group_decision=source,
        regime_fit_decision=fit,
        attempted_trials_exact=0,
    )
    assert state is not None
    evidence = process_state_cell_evidence(
        process_state_decision=state,
        source_group_decision=source,
        regime_fit_decision=fit,
    )
    assert evidence is not None
    return evidence


def _available_state_cell(
    ordinal: int,
    kind: str,
    *,
    attempts: int,
) -> HistoricalBookCellEvidence:
    symbol, timeframe, regime = _cell_tuple(ordinal)
    source, fit = _source_and_fit(symbol, timeframe, AVAILABLE)
    state = create_historical_process_state_decision(
        symbol=symbol,
        timeframe=timeframe,
        regime=regime,
        state_kind=kind,
        reason_code=(
            "OPTIMIZER_TECHNICAL_FAILURE"
            if kind == TECHNICAL_FAILURE
            else "OPTIMIZATION_NOT_RUN"
        ),
        source_group_decision=source,
        regime_fit_decision=fit,
        attempted_trials_exact=attempts,
    )
    assert state is not None
    evidence = process_state_cell_evidence(
        process_state_decision=state,
        source_group_decision=source,
        regime_fit_decision=fit,
    )
    assert evidence is not None
    return evidence


def _available_group_with_counts(
    group_ordinal: int,
    counts: tuple[int, int, int, int],
    *,
    winner_offset: int | None = None,
) -> dict[int, HistoricalBookCellEvidence]:
    symbol, timeframe = _group_tuple(group_ordinal)
    source, fit = _source_and_fit(symbol, timeframe, AVAILABLE, counts)
    result: dict[int, HistoricalBookCellEvidence] = {}
    start = group_ordinal * len(REGIME_ORDER)
    for offset, regime in enumerate(REGIME_ORDER):
        ordinal = start + offset
        if counts[offset] == 0:
            state = create_historical_process_state_decision(
                symbol=symbol,
                timeframe=timeframe,
                regime=regime,
                state_kind=REGIME_UNOBSERVED,
                reason_code=REGIME_UNOBSERVED_REASON,
                source_group_decision=source,
                regime_fit_decision=fit,
                attempted_trials_exact=0,
            )
            assert state is not None
            evidence = process_state_cell_evidence(
                process_state_decision=state,
                source_group_decision=source,
                regime_fit_decision=fit,
            )
        else:
            base = (
                _verified_winner(symbol, timeframe, regime)
                if winner_offset == offset
                else _verified_no_winner(symbol, timeframe, regime)
            )
            evidence = winner_cell_evidence(
                base.winner_decision,
                base.winner_request,
                source,
                fit,
            )
        assert evidence is not None
        result[ordinal] = evidence
    return result


@lru_cache(maxsize=1)
def _terminal_family() -> tuple[HistoricalBookCellEvidence, ...]:
    return tuple(_terminal_cell(ordinal) for ordinal in range(FAMILY_SIZE))


def _family(
    *,
    available_groups: tuple[int, ...] = (),
    overrides: dict[int, HistoricalBookCellEvidence] | None = None,
) -> list[HistoricalBookCellEvidence]:
    values = list(_terminal_family())
    for group_ordinal in available_groups:
        symbol, timeframe = _group_tuple(group_ordinal)
        start = group_ordinal * len(REGIME_ORDER)
        for regime_offset, regime in enumerate(REGIME_ORDER):
            values[start + regime_offset] = _verified_no_winner(
                symbol, timeframe, regime
            )
    for ordinal, evidence in (overrides or {}).items():
        values[ordinal] = evidence
    return values


def _insufficient_group(
    *,
    postsearch: bool,
    sibling_winner: bool,
) -> dict[int, HistoricalBookCellEvidence]:
    first = _insufficient_cell(postsearch=postsearch)
    source = first.source_group_decision
    fit = first.regime_fit_decision
    result = {0: first}
    for ordinal, regime in enumerate(REGIME_ORDER[1:], start=1):
        base = (
            _verified_winner("AUDUSD", "M5", regime)
            if sibling_winner and ordinal == 1
            else _verified_no_winner("AUDUSD", "M5", regime)
        )
        rebound = winner_cell_evidence(
            base.winner_decision,
            base.winner_request,
            source,
            fit,
        )
        assert rebound is not None
        result[ordinal] = rebound
    return result


def _assert_rejected(decision: BookMultiplicityDecision, reason: str) -> None:
    assert decision.status == "REJECTED"
    assert decision.reason_codes == (reason,)
    assert decision.valid is False
    assert not verify_historical_book_multiplicity_decision(decision)


def _forge_book(body: dict[str, Any]) -> BookMultiplicityDecision:
    body.pop("receipt_sha256", None)
    encoded = _canonical(body)
    return BookMultiplicityDecision(encoded, hashlib.sha256(encoded).hexdigest())


def test_architecture_is_exact_configured_100_by_6_by_4() -> None:
    config = json.loads(Path("config.json").read_text(encoding="utf-8"))
    assert tuple(config["symbols"]) == PINNED_SYMBOL_ORDER
    assert len(PINNED_SYMBOL_ORDER) == len(set(PINNED_SYMBOL_ORDER)) == 100
    assert TIMEFRAME_ORDER == ("M5", "M15", "M30", "H1", "H4", "D1")
    assert REGIME_ORDER == ("TREND", "RANGE", "BREAKOUT", "CHOP")
    assert SOURCE_GROUP_SIZE == 600
    assert FAMILY_SIZE == 2400


def test_cross_product_order_and_source_group_identity_are_exact() -> None:
    for ordinal in range(FAMILY_SIZE):
        symbol, timeframe, regime = _cell_tuple(ordinal)
        assert compute_cell_id(symbol, timeframe, regime) == winner.compute_cell_id(
            symbol, timeframe, regime
        )
        assert compute_source_group_id(symbol, timeframe) is not None
    assert compute_cell_id("missing", "M5", "TREND") is None
    assert compute_source_group_id("AUDUSD", "M1") is None


def test_typed_source_regime_and_process_receipts_hash_exact_payloads() -> None:
    source, fit = _source_and_fit("AUDUSD", "M5", AVAILABLE)
    state_evidence = _available_state_cell(0, TECHNICAL_FAILURE, attempts=3)
    state = state_evidence.process_state_decision
    assert state is not None
    for decision in (source, fit, state):
        payload = decision.to_dict()
        receipt = payload.pop("receipt_sha256")
        assert receipt == hashlib.sha256(_canonical(payload)).hexdigest()
    state_payload = state.to_dict()
    assert state_payload["cell"]["cell_id"] == compute_cell_id("AUDUSD", "M5", "TREND")
    assert state_payload["source_group_id"] == compute_source_group_id("AUDUSD", "M5")
    assert state_payload["source_group_decision_sha256"] == source.receipt_sha256
    assert state_payload["regime_receipt_sha256"] == fit.receipt_sha256
    assert state_payload["attempted_trials_exact"] == 3
    assert state_payload["terminal"] is False
    assert state_payload["complete"] is False


def test_real_winner_v11_is_historical_candidate_even_when_by_is_flat() -> None:
    family = _family(available_groups=(0,), overrides={0: _verified_moderate_winner()})
    decision = evaluate_historical_book_multiplicity(family)
    assert decision.status == "HISTORICAL_LOCAL_CANDIDATES_AVAILABLE", (
        decision.reason_codes
    )
    assert decision.valid
    assert verify_historical_book_multiplicity_decision(decision, family)
    body = decision.to_dict()
    first = body["cells"][0]
    assert first["historical_state"] == "WINNER"
    assert first["marginal_p_value"] < 0.05
    assert first["adjusted_q_value"] > TARGET_FDR
    assert first["by_qualified"] is False
    assert first["historical_local_candidate_pending_causal"] is True
    assert body["historical_local_candidate_count"] == 1
    assert body["diagnostic_by_qualified_count"] == 0
    assert body["by_is_diagnostic_only"] is True
    assert body["individual_family_wide_fdr_claimed"] is False


def test_consumer_pins_released_winner_v11_constructible_api() -> None:
    assert (
        PINNED_WINNER_SELECTION_CONTRACT_VERSION
        == (winner.WINNER_SELECTION_CONTRACT_VERSION)
        == "fxpm.winner-selection.v13"
    )
    assert (
        PINNED_WINNER_SELECTION_REQUEST_VERSION
        == (winner.WINNER_SELECTION_REQUEST_VERSION)
        == "fxpm.winner-selection-request.v1"
    )
    assert (
        PINNED_WINNER_ATTEMPT_LEDGER_VERSION
        == winner.ATTEMPT_LEDGER_VERSION
        == "fxpm.winner-attempt-ledger.v2"
    )
    assert (
        PINNED_WINNER_EVIDENCE_LEDGER_VERSION
        == winner.EVIDENCE_LEDGER_VERSION
        == "fxpm.winner-evidence-ledger.v4"
    )
    assert (
        PINNED_WINNER_TRADE_POWER_BINDING_VERSION
        == winner.TRADE_POWER_BINDING_VERSION
        == "fxpm.winner-trade-power-binding.v1"
    )
    assert tuple(
        inspect.signature(winner.evaluate_historical_cell_winner).parameters
    ) == ("request",)
    assert tuple(
        inspect.signature(winner.verify_historical_cell_winner_decision).parameters
    ) == ("decision", "request")
    assert tuple(
        inspect.signature(winner.historical_book_marginal_receipt).parameters
    ) == (
        "decision",
        "request",
    )
    evidence = _verified_winner()
    assert type(evidence.winner_request) is winner.WinnerSelectionRequest
    assert type(evidence.winner_decision) is winner.WinnerSelectionDecision
    assert winner.verify_historical_cell_winner_decision(
        evidence.winner_decision, evidence.winner_request
    )
    assert (
        winner_cell_evidence(
            evidence.winner_decision,
            evidence.winner_request,
            evidence.source_group_decision,
            evidence.regime_fit_decision,
        )
        == evidence
    )


def test_winner_v11_marginal_and_book_identity_chain_is_exact() -> None:
    evidence = _verified_winner()
    marginal = winner.historical_book_marginal_receipt(
        evidence.winner_decision, evidence.winner_request
    )
    assert marginal is not None
    assert set(marginal) == {
        "cell_id",
        "effective_trials",
        "evidence_candidate_id",
        "evidence_scope",
        "local_admissibility_passed",
        "marginal_p_value",
        "p_value_contract_sha256",
        "p_value_kind",
        "parameter_evidence_inherited",
        "process_sha256",
        "source_evidence_sha256",
        "upstream_evidence_receipt_sha256",
        "within_cell_attempted_trials_exact",
        "within_cell_multiplicity_adjusted",
    }
    book_decision = evaluate_historical_book_multiplicity(
        _family(available_groups=(0,), overrides={0: evidence})
    )
    assert book_decision.status == "HISTORICAL_LOCAL_CANDIDATES_AVAILABLE"
    first = book_decision.to_dict()["cells"][0]
    assert first["cell_id"] == marginal["cell_id"]
    assert first["winner_request_sha256"] == evidence.winner_request.request_sha256
    assert first["winner_attempt_ledger_payload_sha256"] == (
        evidence.winner_request.attempt_ledger.payload_sha256
    )
    assert first["winner_decision_sha256"] == evidence.winner_decision.receipt_sha256
    assert first["historical_outcome_receipt_sha256"] == (
        evidence.winner_decision.receipt_sha256
    )
    assert first["local_process_candidate_id"] == marginal["evidence_candidate_id"]
    assert first["local_process_sha256"] == marginal["process_sha256"]
    assert (
        first["winner_evidence_ledger_payload_sha256"]
        == marginal["source_evidence_sha256"]
    )
    assert first["winner_evidence_ledger_payload_sha256"] == (
        evidence.winner_request.evidence_ledger.payload_sha256
    )
    assert first["historical_book_entry_sha256"]
    assert first["historical_local_candidate_pending_causal"] is True


def test_candidate_before_causal_has_no_live_order_capital_or_profit_authority() -> (
    None
):
    body = evaluate_historical_book_multiplicity(
        _family(available_groups=(0,), overrides={0: _verified_winner()})
    ).to_dict()
    assert body["causal_process_validation_required"] is True
    assert body["causal_process_validated"] is False
    assert body["historical_only"] is True
    assert body["live_authorized"] is False
    assert body["order_authorized"] is False
    assert body["capital_sizing_authorized"] is False
    assert body["profitability_claimed"] is False
    assert body["production_integrated"] is False
    serialized = json.dumps(body, sort_keys=True).lower()
    for forbidden in ("selected", "refit", "downstream", "current_candidate"):
        assert forbidden not in serialized


def test_complete_family_without_local_winner_has_unambiguous_status() -> None:
    decision = evaluate_historical_book_multiplicity(_family(available_groups=(0,)))
    assert decision.status == "NO_LOCAL_WINNERS"
    assert decision.reason_codes == ("COMPLETE_FAMILY_HAS_NO_LOCAL_WINNERS",)
    assert decision.valid
    body = decision.to_dict()
    assert body["cells"][0]["historical_state"] == "NO_WINNER"
    assert body["cells"][0]["marginal_p_value"] == 1.0
    assert body["historical_local_candidate_count"] == 0


def test_zero_label_regime_is_complete_p_one_without_winner_leaf() -> None:
    overrides = _available_group_with_counts(0, (4, 3, 0, 3), winner_offset=0)
    family = _family(overrides=overrides)
    decision = evaluate_historical_book_multiplicity(family)
    assert decision.status == "HISTORICAL_LOCAL_CANDIDATES_AVAILABLE"
    assert decision.valid
    assert verify_historical_book_multiplicity_decision(decision, family)
    body = decision.to_dict()
    absent = body["cells"][2]
    assert absent["historical_state"] == REGIME_UNOBSERVED
    assert absent["within_cell_attempted_trials_exact"] == 0
    assert absent["marginal_p_value"] == 1.0
    assert absent["winner_request_sha256"] is None
    assert absent["winner_attempt_ledger_payload_sha256"] is None
    assert absent["winner_decision_sha256"] is None
    assert absent["winner_evidence_ledger_payload_sha256"] is None
    assert absent["local_process_candidate_id"] is None
    support_receipt = overrides[2].regime_fit_decision.to_dict()[
        "train_regime_support"
    ]["receipt_sha256"]
    assert absent["train_regime_support_receipt_sha256"] == support_receipt
    assert (
        absent["process_state_payload"]["train_regime_support_receipt_sha256"]
        == support_receipt
    )
    assert absent["historical_local_candidate_pending_causal"] is False
    assert body["counts"][REGIME_UNOBSERVED] == 1
    assert body["optimization_run_complete"] is True


def test_one_observed_label_is_not_unobserved_or_thresholded() -> None:
    overrides = _available_group_with_counts(0, (1, 3, 2, 4))
    family = _family(overrides=overrides)
    decision = evaluate_historical_book_multiplicity(family)
    assert decision.status == "NO_LOCAL_WINNERS"
    assert decision.to_dict()["cells"][0]["historical_state"] == "NO_WINNER"
    source, fit = _source_and_fit("AUDUSD", "M5", AVAILABLE, (1, 3, 2, 4))
    assert (
        create_historical_process_state_decision(
            symbol="AUDUSD",
            timeframe="M5",
            regime="TREND",
            state_kind=REGIME_UNOBSERVED,
            reason_code=REGIME_UNOBSERVED_REASON,
            source_group_decision=source,
            regime_fit_decision=fit,
            attempted_trials_exact=0,
        )
        is None
    )


def test_presearch_insufficient_evidence_is_complete_p_one_and_does_not_veto_sibling() -> (
    None
):
    overrides = _insufficient_group(postsearch=False, sibling_winner=True)
    family = _family(overrides=overrides)
    decision = evaluate_historical_book_multiplicity(family)
    assert decision.status == "HISTORICAL_LOCAL_CANDIDATES_AVAILABLE"
    assert decision.valid
    assert verify_historical_book_multiplicity_decision(decision, family)
    body = decision.to_dict()
    cell = body["cells"][0]
    assert cell["historical_state"] == INSUFFICIENT_EVIDENCE
    assert cell["state_reason"] == INSUFFICIENT_PRESEARCH_BOUNDARY_REASON
    assert cell["within_cell_attempted_trials_exact"] == 0
    assert cell["marginal_p_value"] == 1.0
    assert cell["effective_trials"] == 0.0
    assert cell["historical_local_candidate_pending_causal"] is False
    assert cell["winner_request_sha256"] is None
    assert cell["winner_attempt_ledger_payload_sha256"] is None
    assert cell["winner_decision_sha256"] is None
    assert cell["winner_evidence_ledger_payload_sha256"] is None
    assert cell["local_process_candidate_id"] is None
    assert cell["local_process_sha256"] is None
    binding = cell["process_state_payload"]["insufficient_evidence_binding"]
    assert binding["terminal_stage"] == "PRE_SEARCH_BOUNDARY"
    assert binding["causal_fold_count"] == 0
    assert binding["process_rejected_attempt_count"] == 0
    assert binding["planned_attempt_count"] > 0
    assert body["counts"][INSUFFICIENT_EVIDENCE] == 1
    assert body["historical_local_candidate_cell_ids"] == [
        compute_cell_id("AUDUSD", "M5", "RANGE")
    ]
    assert body["optimization_run_complete"] is True


def test_postsearch_all_process_rejected_binds_exact_count_without_winner_leaf() -> (
    None
):
    overrides = _insufficient_group(postsearch=True, sibling_winner=False)
    family = _family(overrides=overrides)
    decision = evaluate_historical_book_multiplicity(family)
    assert decision.status == "NO_LOCAL_WINNERS"
    assert decision.valid
    assert verify_historical_book_multiplicity_decision(decision, family)
    cell = decision.to_dict()["cells"][0]
    binding = cell["process_state_payload"]["insufficient_evidence_binding"]
    assert cell["historical_state"] == INSUFFICIENT_EVIDENCE
    assert cell["state_reason"] == INSUFFICIENT_ALL_ATTEMPTS_REJECTED_REASON
    assert cell["within_cell_attempted_trials_exact"] == 3
    assert binding["terminal_stage"] == "POST_SEARCH_ALL_PROCESS_REJECTED"
    assert binding["planned_attempt_count"] == 3
    assert binding["process_rejected_attempt_count"] == 3
    assert binding["causal_fold_count"] == 4
    assert binding["process_rejection_request_sha256"] is not None
    assert binding["process_rejection_attempt_ledger_sha256"] is not None
    assert binding["process_rejection_evidence_ledger_sha256"] is not None
    assert binding["process_rejection_decision_sha256"] is not None
    assert (
        binding["horizon_winner_cross_binding_schema_version"]
        == book.HORIZON_WINNER_CROSS_BINDING_SCHEMA_VERSION
    )
    assert binding["horizon_winner_cross_binding_content_sha256"] is not None
    assert binding["horizon_winner_cross_binding_mapping_sha256"] is not None
    assert (
        binding["winner_raw_production_schema_version"]
        == book.WINNER_RAW_EVIDENCE_PRODUCTION_SCHEMA_VERSION
    )
    assert binding["winner_raw_production_receipt_sha256"] is not None
    evidence = overrides[0]
    cross = json.loads(
        evidence.insufficient_horizon_winner_cross_binding_bytes.decode("ascii")
    )
    assert (
        cross["mapping"][0]["horizon_trial_definition_sha256"]
        != cross["mapping"][0]["trial_definition_sha256"]
    )
    assert cell["winner_request_sha256"] is None
    assert cell["winner_decision_sha256"] is None
    assert cell["historical_local_candidate_pending_causal"] is False


def test_postsearch_insufficiency_requires_exact_cross_binding_and_raw_receipt() -> (
    None
):
    source, fit = _source_and_fit("AUDUSD", "M5", AVAILABLE)
    request, decision, plan = _all_process_rejected_request_and_plan(source)
    cross_bytes, production_bytes = _postsearch_authority_bytes(
        source, fit, request, decision, plan
    )
    common = {
        "symbol": "AUDUSD",
        "timeframe": "M5",
        "regime": "TREND",
        "state_kind": INSUFFICIENT_EVIDENCE,
        "reason_code": INSUFFICIENT_ALL_ATTEMPTS_REJECTED_REASON,
        "source_group_decision": source,
        "regime_fit_decision": fit,
        "attempted_trials_exact": request.attempt_ledger.to_dict()["attempt_count"],
        "horizon_plan": plan,
        "process_rejection_request": request,
        "process_rejection_decision": decision,
    }
    assert (
        create_historical_process_state_decision(
            **common,
            horizon_winner_cross_binding_bytes=cross_bytes,
            winner_raw_production_receipt_bytes=production_bytes,
        )
        is not None
    )
    assert create_historical_process_state_decision(**common) is None

    cross = json.loads(cross_bytes.decode("ascii"))
    assert (
        cross["mapping"][0]["source_count_receipt"]["fit_start_utc"]
        != plan.to_dict()["candidates"][0]["realized_start_utc"]
    )
    cross["mapping"][0]["horizon_trial_definition_sha256"] = cross["mapping"][0][
        "trial_definition_sha256"
    ]
    cross["mapping_sha256"] = _sha_json(cross["mapping"])
    mutated_cross_bytes = _canonical(cross)
    production = json.loads(production_bytes.decode("ascii"))
    production["provenance_cross_binding_sha256"] = hashlib.sha256(
        mutated_cross_bytes
    ).hexdigest()
    assert (
        create_historical_process_state_decision(
            **common,
            horizon_winner_cross_binding_bytes=mutated_cross_bytes,
            winner_raw_production_receipt_bytes=_canonical(production),
        )
        is None
    )

    cross = json.loads(cross_bytes.decode("ascii"))
    source_count = cross["mapping"][0]["source_count_receipt"]
    source_count["fit_start_utc"] = (
        (
            datetime.fromisoformat(source_count["fit_start_utc"].replace("Z", "+00:00"))
            + timedelta(days=1)
        )
        .isoformat(timespec="seconds")
        .replace("+00:00", "Z")
    )
    source_count["receipt_sha256"] = _sha_json(
        {key: value for key, value in source_count.items() if key != "receipt_sha256"}
    )
    cross["mapping_sha256"] = _sha_json(cross["mapping"])
    mutated_cross_bytes = _canonical(cross)
    production = json.loads(production_bytes.decode("ascii"))
    production["provenance_cross_binding_sha256"] = hashlib.sha256(
        mutated_cross_bytes
    ).hexdigest()
    with pytest.raises(
        book._BookError,
        match="INSUFFICIENT_CROSS_BINDING_HORIZON_PROCESS_SPLICE",
    ):
        book._horizon_winner_cross_binding_identity(
            mutated_cross_bytes,
            plan_body=plan.to_dict(),
            source_body=book._source_body(source),
            fit_body=book._regime_fit_body(fit, source),
            process_rejection_request=request,
            process_rejection_decision=decision,
            winner_raw_production_receipt_bytes=_canonical(production),
        )
    assert (
        create_historical_process_state_decision(
            **common,
            horizon_winner_cross_binding_bytes=mutated_cross_bytes,
            winner_raw_production_receipt_bytes=_canonical(production),
        )
        is None
    )

    production = json.loads(production_bytes.decode("ascii"))
    production["artifact_roster"]["winner-request"]["sha256"] = _sha("spliced-request")
    assert (
        create_historical_process_state_decision(
            **common,
            horizon_winner_cross_binding_bytes=cross_bytes,
            winner_raw_production_receipt_bytes=_canonical(production),
        )
        is None
    )


def test_postsearch_insufficiency_rejects_horizon_to_winner_role_splice() -> None:
    source, fit = _source_and_fit("AUDUSD", "M5", AVAILABLE)
    request, decision, plan = _all_process_rejected_request_and_plan(
        source, splice_first_evidence_role=True
    )
    cross_bytes, production_bytes = _postsearch_authority_bytes(
        source, fit, request, decision, plan
    )
    assert (
        create_historical_process_state_decision(
            symbol="AUDUSD",
            timeframe="M5",
            regime="TREND",
            state_kind=INSUFFICIENT_EVIDENCE,
            reason_code=INSUFFICIENT_ALL_ATTEMPTS_REJECTED_REASON,
            source_group_decision=source,
            regime_fit_decision=fit,
            attempted_trials_exact=request.attempt_ledger.to_dict()["attempt_count"],
            horizon_plan=plan,
            process_rejection_request=request,
            process_rejection_decision=decision,
            horizon_winner_cross_binding_bytes=cross_bytes,
            winner_raw_production_receipt_bytes=production_bytes,
        )
        is None
    )


def test_insufficient_evidence_rejects_reason_count_source_regime_and_economic_laundering() -> (
    None
):
    source, fit = _source_and_fit("AUDUSD", "M5", AVAILABLE)
    boundary_plan = _insufficient_horizon_plan(source, boundary=True)
    common = {
        "symbol": "AUDUSD",
        "timeframe": "M5",
        "regime": "TREND",
        "state_kind": INSUFFICIENT_EVIDENCE,
        "source_group_decision": source,
        "regime_fit_decision": fit,
        "horizon_plan": boundary_plan,
    }
    assert (
        create_historical_process_state_decision(
            **common,
            reason_code=INSUFFICIENT_PRESEARCH_BOUNDARY_REASON,
            attempted_trials_exact=1,
        )
        is None
    )
    assert (
        create_historical_process_state_decision(
            **common,
            reason_code="NO_EDGE_FOUND",
            attempted_trials_exact=0,
        )
        is None
    )
    terminal_source, terminal_fit = _source_and_fit(
        "AUDUSD", "M5", TERMINAL_UNAVAILABLE
    )
    assert (
        create_historical_process_state_decision(
            symbol="AUDUSD",
            timeframe="M5",
            regime="TREND",
            state_kind=INSUFFICIENT_EVIDENCE,
            reason_code=INSUFFICIENT_PRESEARCH_BOUNDARY_REASON,
            source_group_decision=terminal_source,
            regime_fit_decision=terminal_fit,
            attempted_trials_exact=0,
            horizon_plan=boundary_plan,
        )
        is None
    )
    sparse_source, sparse_fit = _source_and_fit("AUDUSD", "M5", AVAILABLE, (0, 3, 2, 1))
    sparse_plan = _insufficient_horizon_plan(sparse_source, boundary=True)
    assert (
        create_historical_process_state_decision(
            symbol="AUDUSD",
            timeframe="M5",
            regime="TREND",
            state_kind=INSUFFICIENT_EVIDENCE,
            reason_code=INSUFFICIENT_PRESEARCH_BOUNDARY_REASON,
            source_group_decision=sparse_source,
            regime_fit_decision=sparse_fit,
            attempted_trials_exact=0,
            horizon_plan=sparse_plan,
        )
        is None
    )
    economic_request = _typed_request("AUDUSD", "M5", "TREND", no_winner=True)
    economic_decision = winner.evaluate_historical_cell_winner(economic_request)
    assert economic_decision.status == "NO_PROCESS_WINNER"
    post_plan = _insufficient_horizon_plan(
        source,
        boundary=False,
        winner_fold_plan=economic_request.precommitment.to_dict()["causal_fold_plan"],
        winner_attempts=economic_request.attempt_ledger.to_dict()["attempts"],
    )
    assert (
        create_historical_process_state_decision(
            **{key: value for key, value in common.items() if key != "horizon_plan"},
            reason_code=INSUFFICIENT_ALL_ATTEMPTS_REJECTED_REASON,
            attempted_trials_exact=3,
            horizon_plan=post_plan,
            process_rejection_request=economic_request,
            process_rejection_decision=economic_decision,
        )
        is None
    )


def test_insufficient_binding_mutation_reseal_and_relabel_are_rejected() -> None:
    evidence = _insufficient_cell(postsearch=False)
    state = evidence.process_state_decision
    assert state is not None
    raw = state.to_dict()
    raw.pop("receipt_sha256")
    raw["insufficient_evidence_binding"]["source_prefix_rows"] += 1
    encoded = _canonical(raw)
    forged_state = HistoricalProcessStateDecision(
        encoded, hashlib.sha256(encoded).hexdigest()
    )
    assert (
        process_state_cell_evidence(
            process_state_decision=forged_state,
            source_group_decision=evidence.source_group_decision,
            regime_fit_decision=evidence.regime_fit_decision,
            horizon_plan=evidence.insufficient_horizon_plan,
        )
        is None
    )
    raw = state.to_dict()
    raw.pop("receipt_sha256")
    raw["insufficient_evidence_binding"]["selection_cutoff_utc"] = raw[
        "insufficient_evidence_binding"
    ]["selection_cutoff_utc"].replace("Z", ".000Z")
    encoded = _canonical(raw)
    noncanonical_cutoff = HistoricalProcessStateDecision(
        encoded, hashlib.sha256(encoded).hexdigest()
    )
    assert (
        process_state_cell_evidence(
            process_state_decision=noncanonical_cutoff,
            source_group_decision=evidence.source_group_decision,
            regime_fit_decision=evidence.regime_fit_decision,
            horizon_plan=evidence.insufficient_horizon_plan,
        )
        is None
    )
    raw = state.to_dict()
    raw.pop("receipt_sha256")
    raw["state_kind"] = UNSEARCHED
    raw["terminal"] = False
    raw["complete"] = False
    encoded = _canonical(raw)
    relabelled = HistoricalProcessStateDecision(
        encoded, hashlib.sha256(encoded).hexdigest()
    )
    assert (
        process_state_cell_evidence(
            process_state_decision=relabelled,
            source_group_decision=evidence.source_group_decision,
            regime_fit_decision=evidence.regime_fit_decision,
        )
        is None
    )


def test_unobserved_slot_cannot_be_omitted_or_relabelled() -> None:
    overrides = _available_group_with_counts(0, (4, 3, 0, 3))
    family = _family(overrides=overrides)
    _assert_rejected(
        evaluate_historical_book_multiplicity(family[:2] + family[3:]),
        "CELL_0002_ROSTER_MISMATCH",
    )
    absent = family[2]
    assert absent.process_state_decision is not None
    raw = absent.process_state_decision.to_dict()
    raw.pop("receipt_sha256")
    raw["state_kind"] = UNSEARCHED
    raw["reason_code"] = "OPTIMIZATION_NOT_RUN"
    raw["terminal"] = False
    raw["complete"] = False
    encoded = _canonical(raw)
    relabelled = replace(
        absent,
        process_state_decision=HistoricalProcessStateDecision(
            encoded, hashlib.sha256(encoded).hexdigest()
        ),
    )
    family[2] = relabelled
    _assert_rejected(
        evaluate_historical_book_multiplicity(family),
        "UNSEARCHED_REGIME_FIT_INVALID",
    )


def test_unobserved_state_is_bound_to_exact_regime_fit_receipt() -> None:
    overrides = _available_group_with_counts(0, (4, 3, 0, 3))
    family = _family(overrides=overrides)
    source = family[0].source_group_decision
    source_body = source.to_dict()
    detector_sha256 = _sha("changed-detector")
    detector_record = _detector_record(source, detector_sha256, seed=1)
    params_sha256 = detector_record["parameters_sha256"]
    changed_fit = create_regime_fit_decision(
        source_group_decision=source,
        fit_state=REGIME_FITTED,
        reason_code="ANCHOR_LOCAL_SHARED_REGIME_FIT_COMPLETE",
        regime_fit_policy_sha256=_sha("regime-fit-policy"),
        detector_process_sha256=detector_sha256,
        regime_fit_evidence_sha256=_sha("changed-fit-evidence"),
        four_regime_mapping_sha256=_sha("changed-map"),
        regime_params_sha256=params_sha256,
        regime_detector_record=detector_record,
        train_regime_support=_train_support(
            "AUDUSD",
            "M5",
            (4, 3, 0, 3),
            activation_id=source_body["activation_id"],
            activation_manifest_sha256=source_body["activation_manifest_sha256"],
            native_source_sha256=source_body["native_source_sha256"],
            params_sha256=params_sha256,
            detector_code_sha256=detector_sha256,
            tag="changed",
        ),
    )
    assert changed_fit is not None
    for index in range(4):
        family[index] = replace(family[index], regime_fit_decision=changed_fit)
    _assert_rejected(
        evaluate_historical_book_multiplicity(family),
        "PROCESS_PROVENANCE_SPLICE",
    )


def test_terminal_source_decision_is_one_group_fact_for_all_four_regimes() -> None:
    decision = evaluate_historical_book_multiplicity(_family())
    assert decision.status == "NO_LOCAL_WINNERS"
    assert decision.valid
    body = decision.to_dict()
    siblings = body["cells"][:4]
    assert {cell["historical_state"] for cell in siblings} == {"SOURCE_UNAVAILABLE"}
    assert len({cell["source_group_decision_sha256"] for cell in siblings}) == 1
    assert len({cell["regime_receipt_sha256"] for cell in siblings}) == 1
    assert body["source_group_counts"] == {
        AVAILABLE: 0,
        TERMINAL_TECHNICAL: 0,
        TERMINAL_UNAVAILABLE: SOURCE_GROUP_SIZE,
    }


@pytest.mark.parametrize("kind", [TECHNICAL_FAILURE, UNSEARCHED])
def test_technical_or_unsearched_cell_invalidates_candidate_book(kind: str) -> None:
    failure = _available_state_cell(
        1,
        kind,
        attempts=3 if kind == TECHNICAL_FAILURE else 0,
    )
    decision = evaluate_historical_book_multiplicity(
        _family(
            available_groups=(0,),
            overrides={0: _verified_winner(), 1: failure},
        )
    )
    assert decision.status == "INCOMPLETE_HISTORICAL_BOOK"
    assert decision.valid is False
    assert verify_historical_book_multiplicity_decision(decision)
    body = decision.to_dict()
    assert body["optimization_run_complete"] is False
    assert body["historical_local_candidate_count"] == 0
    assert not any(
        cell["historical_local_candidate_pending_causal"] for cell in body["cells"]
    )


def test_authenticated_terminal_technical_group_is_complete_and_does_not_veto_winner() -> (
    None
):
    overrides = {index: _technical_terminal_cell(index) for index in range(4, 8)}
    overrides[0] = _verified_winner()
    decision = evaluate_historical_book_multiplicity(
        _family(available_groups=(0,), overrides=overrides)
    )

    assert decision.valid
    assert decision.status == "HISTORICAL_LOCAL_CANDIDATES_AVAILABLE"
    body = decision.to_dict()
    assert body["optimization_run_complete"] is True
    assert body["multiplicity_tested_family_size"] == FAMILY_SIZE - 4
    assert body["multiplicity_excluded_terminal_technical_count"] == 4
    tested_ids = [
        cell["cell_id"]
        for cell in body["cells"]
        if cell["historical_state"] != TERMINAL_TECHNICAL
    ]
    assert body["multiplicity_tested_cell_ids_sha256"] == _sha_json(tested_ids)
    assert {cell["historical_state"] for cell in body["cells"][4:8]} == {
        TERMINAL_TECHNICAL
    }
    assert all(cell["marginal_p_value"] == 1.0 for cell in body["cells"][4:8])
    assert all(cell["adjusted_q_value"] == 1.0 for cell in body["cells"][4:8])
    assert all(
        cell["within_cell_attempted_trials_exact"] == 0 for cell in body["cells"][4:8]
    )
    assert body["source_group_counts"][TERMINAL_TECHNICAL] == 1
    assert body["regime_fit_counts"][REGIME_NOT_APPLICABLE_TECHNICAL_TERMINAL] == 1
    assert body["historical_local_candidate_count"] == 1


def test_hostile_technical_relabel_to_terminal_absence_fails_closed() -> None:
    technical = _available_state_cell(0, TECHNICAL_FAILURE, attempts=2)
    assert technical.process_state_decision is not None
    raw = technical.process_state_decision.to_dict()
    raw.pop("receipt_sha256")
    raw["state_kind"] = TERMINAL_SOURCE_ABSENCE
    raw["attempted_trials_exact"] = 0
    raw["terminal"] = True
    raw["complete"] = True
    encoded = _canonical(raw)
    forged = HistoricalProcessStateDecision(
        encoded, hashlib.sha256(encoded).hexdigest()
    )
    relabeled = replace(technical, process_state_decision=forged)
    _assert_rejected(
        evaluate_historical_book_multiplicity(
            _family(available_groups=(0,), overrides={0: relabeled})
        ),
        "TERMINAL_PROCESS_PROVENANCE_INVALID",
    )


def test_hostile_generic_technical_cannot_relabel_to_terminal_technical() -> None:
    technical = _available_state_cell(0, TECHNICAL_FAILURE, attempts=2)
    assert technical.process_state_decision is not None
    raw = technical.process_state_decision.to_dict()
    raw.pop("receipt_sha256")
    raw["state_kind"] = TERMINAL_TECHNICAL
    raw["attempted_trials_exact"] = 0
    raw["terminal"] = True
    raw["complete"] = True
    encoded = _canonical(raw)
    forged = HistoricalProcessStateDecision(
        encoded, hashlib.sha256(encoded).hexdigest()
    )
    relabeled = replace(technical, process_state_decision=forged)
    _assert_rejected(
        evaluate_historical_book_multiplicity(
            _family(available_groups=(0,), overrides={0: relabeled})
        ),
        "TERMINAL_TECHNICAL_PROCESS_PROVENANCE_INVALID",
    )


def test_one_regime_cannot_claim_terminal_source_while_siblings_are_available() -> None:
    family = _family(available_groups=(0,))
    family[1] = _terminal_cell(1)
    _assert_rejected(
        evaluate_historical_book_multiplicity(family),
        "CELL_0001_CONTRADICTORY_SOURCE_GROUP",
    )


def test_four_regimes_must_share_exact_same_regime_fit_receipt() -> None:
    family = _family(available_groups=(0,))
    source, _ = _source_and_fit("AUDUSD", "M5", AVAILABLE)
    source_body = source.to_dict()
    detector_sha256 = _sha("different-detector")
    detector_record = _detector_record(source, detector_sha256, seed=2)
    params_sha256 = detector_record["parameters_sha256"]
    different_fit = create_regime_fit_decision(
        source_group_decision=source,
        fit_state=REGIME_FITTED,
        reason_code="ANCHOR_LOCAL_SHARED_REGIME_FIT_COMPLETE",
        regime_fit_policy_sha256=_sha("different-regime-fit-policy"),
        detector_process_sha256=detector_sha256,
        regime_fit_evidence_sha256=_sha("different-fit-evidence"),
        four_regime_mapping_sha256=_sha("different-map"),
        regime_params_sha256=params_sha256,
        regime_detector_record=detector_record,
        train_regime_support=_train_support(
            "AUDUSD",
            "M5",
            (4, 3, 2, 1),
            activation_id=source_body["activation_id"],
            activation_manifest_sha256=source_body["activation_manifest_sha256"],
            native_source_sha256=source_body["native_source_sha256"],
            params_sha256=params_sha256,
            detector_code_sha256=detector_sha256,
            tag="different",
        ),
    )
    assert different_fit is not None
    family[2] = replace(family[2], regime_fit_decision=different_fit)
    _assert_rejected(
        evaluate_historical_book_multiplicity(family),
        "CELL_0002_CONTRADICTORY_REGIME_FIT",
    )


def test_regime_fit_roster_is_exact_600_ordered_and_bound_to_every_entry() -> None:
    body = evaluate_historical_book_multiplicity(
        _family(available_groups=(0,))
    ).to_dict()
    assert len(body["regime_fits"]) == SOURCE_GROUP_SIZE
    assert len(body["source_groups"]) == SOURCE_GROUP_SIZE
    for group_ordinal in (0, 1, 37, SOURCE_GROUP_SIZE - 1):
        symbol, timeframe = _group_tuple(group_ordinal)
        fit = body["regime_fits"][group_ordinal]
        assert (fit["payload"]["symbol"], fit["payload"]["timeframe"]) == (
            symbol,
            timeframe,
        )
        siblings = body["cells"][group_ordinal * 4 : (group_ordinal + 1) * 4]
        assert {cell["regime_receipt_sha256"] for cell in siblings} == {
            fit["receipt_sha256"]
        }
        assert {cell["regime_detector_record_sha256"] for cell in siblings} == {
            fit["payload"]["regime_detector_record_sha256"]
        }
        assert all(
            cell["regime_receipt_sha256"] in json.dumps(cell, sort_keys=True)
            for cell in siblings
        )


def test_available_group_embeds_one_exact_detector_record_without_forcing_regimes() -> (
    None
):
    family = _family(overrides=_available_group_with_counts(0, (4, 3, 0, 3)))
    body = evaluate_historical_book_multiplicity(family).to_dict()
    fit = body["regime_fits"][0]
    record = fit["payload"]["regime_detector_record"]
    assert json.loads(record["parameters_canonical_json"]) == _detector_parameters()
    assert (
        record["parameters_sha256"]
        == hashlib.sha256(
            record["parameters_canonical_json"].encode("ascii")
        ).hexdigest()
    )
    record_body = dict(record)
    record_sha = record_body.pop("record_sha256")
    assert record_sha == hashlib.sha256(_canonical(record_body)).hexdigest()
    assert fit["payload"]["regime_detector_record_sha256"] == record_sha
    siblings = body["cells"][:4]
    assert {cell["regime_detector_record_sha256"] for cell in siblings} == {record_sha}
    assert siblings[2]["historical_state"] == REGIME_UNOBSERVED
    assert siblings[2]["within_cell_attempted_trials_exact"] == 0


def test_winner_request_splice_and_decision_replay_fail_closed() -> None:
    real = _verified_winner("AUDUSD", "M5", "TREND")
    wrong_request = _typed_request("AUDUSD", "M5", "RANGE", no_winner=False)
    spliced = replace(real, winner_request=wrong_request)
    _assert_rejected(
        evaluate_historical_book_multiplicity(
            _family(available_groups=(0,), overrides={0: spliced})
        ),
        "CELL_0000_WINNER_REPLAY_REJECTED",
    )


def test_stale_winner_ledger_relabel_cannot_enter_book() -> None:
    real = _verified_winner()
    request = real.winner_request
    decision = real.winner_decision
    assert request is not None and decision is not None
    payload = request.evidence_ledger.to_dict()
    payload["contract_version"] = "fxpm.winner-evidence-ledger.v2"
    encoded = _canonical(payload)
    stale_ledger = winner.WinnerEvidenceLedgerPayload(
        encoded, hashlib.sha256(encoded).hexdigest()
    )
    stale_request = winner.WinnerSelectionRequest(
        request.precommitment,
        request.attempt_ledger,
        stale_ledger,
        request.request_sha256,
    )
    assert not winner.verify_historical_cell_winner_decision(decision, stale_request)
    assert (
        winner_cell_evidence(
            decision,
            stale_request,
            real.source_group_decision,
            real.regime_fit_decision,
        )
        is None
    )


def test_source_contract_request_splice_fails_before_candidate_creation() -> None:
    real = _verified_winner()
    activation_id = "activation:AUDUSD:M5:wrong"
    activation_manifest_sha256 = _sha("wrong-activation-manifest")
    native_source_sha256 = _sha("wrong-native-source")
    wrong_source = create_historical_source_group_decision(
        symbol="AUDUSD",
        timeframe="M5",
        availability=AVAILABLE,
        reason_code="ACTIVATED_NATIVE_SOURCE_AVAILABLE",
        source_contract_sha256=_sha("different-source-contract"),
        source_group_evidence_sha256=_sha("different-source-evidence"),
        activation_id=activation_id,
        activation_manifest_sha256=activation_manifest_sha256,
        native_source_sha256=native_source_sha256,
        acquisition_attempts_exact=1,
    )
    assert wrong_source is not None
    wrong_detector_sha256 = _sha("detector")
    wrong_detector_record = _detector_record(
        wrong_source, wrong_detector_sha256, seed=3
    )
    wrong_params_sha256 = wrong_detector_record["parameters_sha256"]
    wrong_fit = create_regime_fit_decision(
        source_group_decision=wrong_source,
        fit_state=REGIME_FITTED,
        reason_code="ANCHOR_LOCAL_SHARED_REGIME_FIT_COMPLETE",
        regime_fit_policy_sha256=_sha("regime-fit-policy"),
        detector_process_sha256=wrong_detector_sha256,
        regime_fit_evidence_sha256=_sha("fit-evidence"),
        four_regime_mapping_sha256=_sha("mapping"),
        regime_params_sha256=wrong_params_sha256,
        regime_detector_record=wrong_detector_record,
        train_regime_support=_train_support(
            "AUDUSD",
            "M5",
            (4, 3, 2, 1),
            activation_id=activation_id,
            activation_manifest_sha256=activation_manifest_sha256,
            native_source_sha256=native_source_sha256,
            params_sha256=wrong_params_sha256,
            detector_code_sha256=wrong_detector_sha256,
            tag="wrong",
        ),
    )
    assert wrong_fit is not None
    spliced = replace(
        real,
        source_group_decision=wrong_source,
        regime_fit_decision=wrong_fit,
    )
    _assert_rejected(
        evaluate_historical_book_multiplicity(
            _family(available_groups=(0,), overrides={0: spliced})
        ),
        "CELL_0000_SOURCE_CONTRACT_SPLICE",
    )


def test_fake_winner_without_exact_decision_and_request_cannot_be_candidate() -> None:
    source, fit = _source_and_fit("AUDUSD", "M5", AVAILABLE)
    fake = HistoricalBookCellEvidence(
        symbol="AUDUSD",
        timeframe="M5",
        regime="TREND",
        source_group_decision=source,
        regime_fit_decision=fit,
    )
    _assert_rejected(
        evaluate_historical_book_multiplicity(
            _family(available_groups=(0,), overrides={0: fake})
        ),
        "CELL_0000_PROCESS_STATE_INPUT_INVALID",
    )


def test_family_must_be_exact_complete_ordered_2400_grid() -> None:
    family = _family()
    _assert_rejected(
        evaluate_historical_book_multiplicity(family[:-1]), "FAMILY_INCOMPLETE"
    )
    _assert_rejected(
        evaluate_historical_book_multiplicity([*family, family[-1]]),
        "FAMILY_OVERSIZED",
    )
    swapped = list(family)
    swapped[0], swapped[1] = swapped[1], swapped[0]
    _assert_rejected(
        evaluate_historical_book_multiplicity(swapped),
        "CELL_0000_ROSTER_MISMATCH",
    )


def test_process_state_factory_rejects_bool_float_aliases_and_false_terminal() -> None:
    source, fit = _source_and_fit("AUDUSD", "M5", AVAILABLE)
    for alias in (True, 1.0):
        assert (
            create_historical_process_state_decision(
                symbol="AUDUSD",
                timeframe="M5",
                regime="TREND",
                state_kind=TECHNICAL_FAILURE,
                reason_code="OPTIMIZER_TECHNICAL_FAILURE",
                source_group_decision=source,
                regime_fit_decision=fit,
                attempted_trials_exact=alias,
            )
            is None
        )
    assert (
        create_historical_process_state_decision(
            symbol="AUDUSD",
            timeframe="M5",
            regime="TREND",
            state_kind=TERMINAL_SOURCE_ABSENCE,
            reason_code="FAKE_TERMINAL",
            source_group_decision=source,
            regime_fit_decision=fit,
            attempted_trials_exact=0,
        )
        is None
    )


def test_regime_fit_rejects_incomplete_or_inconsistent_support_counts() -> None:
    source, _ = _source_and_fit("AUDUSD", "M5", AVAILABLE)
    source_body = source.to_dict()
    detector_sha256 = _sha("detector")
    detector_record = _detector_record(source, detector_sha256, seed=4)
    params_sha256 = detector_record["parameters_sha256"]
    common = {
        "source_group_decision": source,
        "fit_state": REGIME_FITTED,
        "reason_code": "ANCHOR_LOCAL_SHARED_REGIME_FIT_COMPLETE",
        "regime_fit_policy_sha256": _sha("policy"),
        "detector_process_sha256": detector_sha256,
        "regime_fit_evidence_sha256": _sha("evidence"),
        "four_regime_mapping_sha256": _sha("mapping"),
        "regime_params_sha256": params_sha256,
        "regime_detector_record": detector_record,
    }
    missing = _train_support(
        "AUDUSD",
        "M5",
        (4, 3, 2, 1),
        activation_id=source_body["activation_id"],
        activation_manifest_sha256=source_body["activation_manifest_sha256"],
        native_source_sha256=source_body["native_source_sha256"],
        params_sha256=params_sha256,
        detector_code_sha256=detector_sha256,
    )
    missing["counts"].pop("CHOP")
    missing["receipt_sha256"] = hashlib.sha256(
        _canonical(
            {key: value for key, value in missing.items() if key != "receipt_sha256"}
        )
    ).hexdigest()
    assert create_regime_fit_decision(**common, train_regime_support=missing) is None
    inconsistent = _train_support(
        "AUDUSD",
        "M5",
        (4, 3, 2, 1),
        activation_id=source_body["activation_id"],
        activation_manifest_sha256=source_body["activation_manifest_sha256"],
        native_source_sha256=source_body["native_source_sha256"],
        params_sha256=params_sha256,
        detector_code_sha256=detector_sha256,
    )
    inconsistent["counts"]["CHOP"] = 2
    inconsistent["receipt_sha256"] = hashlib.sha256(
        _canonical(
            {
                key: value
                for key, value in inconsistent.items()
                if key != "receipt_sha256"
            }
        )
    ).hexdigest()
    assert (
        create_regime_fit_decision(**common, train_regime_support=inconsistent) is None
    )
    spliced = _train_support(
        "AUDUSD",
        "M5",
        (4, 3, 2, 1),
        activation_id=source_body["activation_id"],
        activation_manifest_sha256=source_body["activation_manifest_sha256"],
        native_source_sha256=source_body["native_source_sha256"],
        params_sha256=params_sha256,
        detector_code_sha256=detector_sha256,
    )
    spliced["bindings"]["source_sha256"] = _sha("spliced-native-source")
    spliced["receipt_sha256"] = hashlib.sha256(
        _canonical(
            {key: value for key, value in spliced.items() if key != "receipt_sha256"}
        )
    ).hexdigest()
    assert create_regime_fit_decision(**common, train_regime_support=spliced) is None


def test_regime_fit_accepts_authenticated_zero_label_support() -> None:
    source, fit = _source_and_fit(
        "XAUGBP",
        "D1",
        AVAILABLE,
        counts=(0, 0, 0, 0),
    )
    support = fit.to_dict()["train_regime_support"]

    assert support["eligible_rows"] == 0
    assert support["observed_regimes"] == []
    assert support["unobserved_regimes"] == list(REGIME_ORDER)
    assert support["first_ts"] is None
    assert support["last_ts"] is None
    for regime in REGIME_ORDER:
        state = create_historical_process_state_decision(
            symbol="XAUGBP",
            timeframe="D1",
            regime=regime,
            state_kind=REGIME_UNOBSERVED,
            reason_code=REGIME_UNOBSERVED_REASON,
            source_group_decision=source,
            regime_fit_decision=fit,
            attempted_trials_exact=0,
        )
        assert state is not None
        assert state.to_dict()["state_kind"] == REGIME_UNOBSERVED

    forged = deepcopy(support)
    forged["first_ts"] = "2022-01-24T00:00:00+00:00"
    forged["last_ts"] = "2022-01-24T00:00:00+00:00"
    forged["receipt_sha256"] = hashlib.sha256(
        _canonical(
            {key: value for key, value in forged.items() if key != "receipt_sha256"}
        )
    ).hexdigest()
    fit_body = fit.to_dict()
    assert (
        create_regime_fit_decision(
            source_group_decision=source,
            fit_state=fit_body["fit_state"],
            reason_code=fit_body["reason_code"],
            regime_fit_policy_sha256=fit_body["regime_fit_policy_sha256"],
            detector_process_sha256=fit_body["detector_process_sha256"],
            regime_fit_evidence_sha256=fit_body["regime_fit_evidence_sha256"],
            four_regime_mapping_sha256=fit_body["four_regime_mapping_sha256"],
            regime_params_sha256=fit_body["regime_params_sha256"],
            regime_detector_record=fit_body["regime_detector_record"],
            train_regime_support=forged,
        )
        is None
    )


def test_detector_record_rejects_omission_source_splice_cutoff_and_numeric_alias() -> (
    None
):
    source, _ = _source_and_fit("AUDUSD", "M5", AVAILABLE)
    source_body = source.to_dict()
    detector_sha = _sha("record-detector")
    record = _detector_record(source, detector_sha, seed=5)
    params_sha = record["parameters_sha256"]
    support = _train_support(
        "AUDUSD",
        "M5",
        (4, 3, 2, 1),
        activation_id=source_body["activation_id"],
        activation_manifest_sha256=source_body["activation_manifest_sha256"],
        native_source_sha256=source_body["native_source_sha256"],
        params_sha256=params_sha,
        detector_code_sha256=detector_sha,
    )
    common = {
        "source_group_decision": source,
        "fit_state": REGIME_FITTED,
        "reason_code": "ANCHOR_LOCAL_SHARED_REGIME_FIT_COMPLETE",
        "regime_fit_policy_sha256": _sha("record-policy"),
        "detector_process_sha256": detector_sha,
        "regime_fit_evidence_sha256": _sha("record-evidence"),
        "four_regime_mapping_sha256": _sha("record-mapping"),
        "regime_params_sha256": params_sha,
        "train_regime_support": support,
    }
    assert create_regime_fit_decision(**common, regime_detector_record=None) is None

    mutated = deepcopy(record)
    hostile_parameters = json.loads(mutated["parameters_canonical_json"])
    hostile_parameters["k_confirm"] = True
    mutated["parameters_canonical_json"] = _canonical(hostile_parameters).decode(
        "ascii"
    )
    assert create_regime_fit_decision(**common, regime_detector_record=mutated) is None

    stale_cutoff = _detector_record(
        source,
        detector_sha,
        seed=5,
        cutoff="2024-01-01T00:00:00+00:00",
    )
    assert (
        create_regime_fit_decision(**common, regime_detector_record=stale_cutoff)
        is None
    )

    other_source, _ = _source_and_fit("AUDUSD", "M15", AVAILABLE)
    spliced = _detector_record(other_source, detector_sha, seed=5)
    assert create_regime_fit_decision(**common, regime_detector_record=spliced) is None

    parameters = _detector_parameters(5)
    parameters["k_hold"] = False
    assert (
        create_regime_detector_record(
            source_group_decision=source,
            fit_cutoff_utc="2025-01-01T00:00:00+00:00",
            detector_version="fxpm.market-regime-detector/v1",
            detector_code_sha256=detector_sha,
            parameters=parameters,
        )
        is None
    )


@pytest.mark.parametrize(
    ("field", "bad"),
    (
        ("k_confirm", 65),
        ("k_hold", 257),
        ("adx_period", 513),
        ("bb_squeeze_lookback", 10_001),
        ("gap_min", 1.0001),
        ("bb_squeeze_percentile", 100.1),
        ("adx_trend_threshold", -0.1),
        ("bb_std", 0.0),
        ("structure_atr_mult", 10.1),
        ("trend_adx_weight", 1.1),
        ("trend_adx_weight", 0.39),
        ("gap_min", float("inf")),
    ),
)
def test_detector_record_rejects_runtime_invalid_parameter_domains(
    field: str, bad: int | float
) -> None:
    source, _ = _source_and_fit("AUDUSD", "M5", AVAILABLE)
    parameters = _detector_parameters()
    parameters[field] = bad
    assert (
        create_regime_detector_record(
            source_group_decision=source,
            fit_cutoff_utc="2025-01-01T00:00:00+00:00",
            detector_version="fxpm.market-regime-detector/v1",
            detector_code_sha256=_sha("domain-detector"),
            parameters=parameters,
        )
        is None
    )


def test_detector_record_accepts_runtime_boundary_parameter_domains() -> None:
    source, _ = _source_and_fit("AUDUSD", "M5", AVAILABLE)
    parameters = _detector_parameters()
    parameters.update(book.REGIME_DETECTOR_INTEGER_MAXIMA)
    parameters.update(
        {
            "gap_min": 1.0,
            "bb_squeeze_percentile": 100.0,
            "adx_trend_threshold": 100.0,
            "bb_std": 10.0,
            "structure_atr_mult": 10.0,
        }
    )
    assert (
        create_regime_detector_record(
            source_group_decision=source,
            fit_cutoff_utc="2025-01-01T00:00:00+00:00",
            detector_version="fxpm.market-regime-detector/v1",
            detector_code_sha256=_sha("boundary-detector"),
            parameters=parameters,
        )
        is not None
    )


def test_source_factory_requires_canonical_terminal_proof_and_exact_attempts() -> None:
    common = {
        "symbol": "AUDUSD",
        "timeframe": "M5",
        "availability": TERMINAL_UNAVAILABLE,
        "reason_code": "BROKER_HISTORY_TERMINALLY_UNAVAILABLE",
        "source_contract_sha256": _sha("source-contract"),
        "source_group_evidence_sha256": _sha("source-evidence"),
    }
    assert (
        create_historical_source_group_decision(
            **common,
            acquisition_attempts_exact=1,
            terminal_proof_sha256=None,
        )
        is None
    )
    for alias in (True, 1.0):
        assert (
            create_historical_source_group_decision(
                **common,
                acquisition_attempts_exact=alias,
                terminal_proof_sha256=_sha("proof"),
            )
            is None
        )


def test_decision_replay_detects_exact_family_input_splice() -> None:
    family = _family(available_groups=(0,), overrides={0: _verified_winner()})
    decision = evaluate_historical_book_multiplicity(family)
    assert verify_historical_book_multiplicity_decision(decision, family)
    spliced = list(family)
    spliced[1] = _available_state_cell(1, TECHNICAL_FAILURE, attempts=1)
    assert not verify_historical_book_multiplicity_decision(decision, spliced)


@pytest.mark.parametrize(
    "mutation",
    [
        "family_size_float",
        "candidate_count_bool",
        "counts_float",
        "cell_ordinal_bool",
        "cell_attempt_float",
        "cell_candidate_int",
        "winner_attempt_bool",
        "source_attempt_bool",
        "source_contract_drift",
    ],
)
def test_rehashed_bool_and_float_aliases_never_verify(mutation: str) -> None:
    genuine = evaluate_historical_book_multiplicity(
        _family(available_groups=(0,), overrides={0: _verified_winner()})
    )
    body = deepcopy(genuine.to_dict())
    if mutation == "family_size_float":
        body["family_size"] = float(FAMILY_SIZE)
    elif mutation == "candidate_count_bool":
        body["historical_local_candidate_count"] = True
    elif mutation == "counts_float":
        body["counts"]["WINNER"] = 1.0
    elif mutation == "cell_ordinal_bool":
        body["cells"][0]["ordinal"] = False
    elif mutation == "cell_attempt_float":
        body["cells"][0]["within_cell_attempted_trials_exact"] = 3.0
    elif mutation == "cell_candidate_int":
        body["cells"][0]["historical_local_candidate_pending_causal"] = 1
    elif mutation == "winner_attempt_bool":
        body["cells"][0]["winner_attempt_ledger_payload_sha256"] = True
    elif mutation == "source_attempt_bool":
        body["source_groups"][0]["payload"]["acquisition_attempts_exact"] = True
        payload = body["source_groups"][0]["payload"]
        body["source_groups"][0]["receipt_sha256"] = hashlib.sha256(
            _canonical(payload)
        ).hexdigest()
    else:
        body["source_groups"][0]["payload"]["contract_version"] = "drifted"
        payload = body["source_groups"][0]["payload"]
        body["source_groups"][0]["receipt_sha256"] = hashlib.sha256(
            _canonical(payload)
        ).hexdigest()
    assert not verify_historical_book_multiplicity_decision(_forge_book(body))


@pytest.mark.parametrize(
    ("field", "bad"),
    [
        ("adjusted_q_value", 0.0),
        ("historical_book_entry_sha256", "0" * 64),
        ("local_process_sha256", "1" * 64),
        ("regime_receipt_sha256", "2" * 64),
        ("regime_detector_record_sha256", "3" * 64),
    ],
)
def test_rehashed_cell_lineage_tampering_never_verifies(field: str, bad: Any) -> None:
    genuine = evaluate_historical_book_multiplicity(
        _family(available_groups=(0,), overrides={0: _verified_winner()})
    )
    body = genuine.to_dict()
    body["cells"][0][field] = bad
    assert not verify_historical_book_multiplicity_decision(_forge_book(body))


def test_by_math_is_exact_ordered_and_diagnostic_only() -> None:
    values = [0.01, 0.04, 0.03, 0.002]
    assert adjust_p_values_by(values) == pytest.approx((1 / 24, 1 / 12, 1 / 12, 1 / 60))
    sparse_strong = adjust_p_values_by([0.000638] + [1.0] * (FAMILY_SIZE - 1))
    assert sparse_strong is not None
    assert sparse_strong[0] == 1.0
    snapshot = pinned_policy_snapshot()
    assert snapshot["by_is_diagnostic_only"] is True
    assert snapshot["individual_family_wide_fdr_claimed"] is False
    assert snapshot["regime_detector_exact_parameters_are_embedded"] is True
    assert snapshot["all_sibling_cells_bind_shared_regime_detector_record"] is True


def test_by_monotonicity_property() -> None:
    randomizer = random.Random(2400)
    for _ in range(30):
        values = [randomizer.random() for _ in range(randomizer.randint(1, 200))]
        adjusted = adjust_p_values_by(values)
        assert adjusted is not None
        order = sorted(range(len(values)), key=lambda index: (values[index], index))
        assert [adjusted[index] for index in order] == sorted(
            adjusted[index] for index in order
        )


@pytest.mark.parametrize(
    "values",
    [
        [],
        [True],
        [0],
        ["0.1"],
        [float("nan")],
        [float("inf")],
        [-0.1],
        [1.1],
        {"p": 0.1},
    ],
)
def test_by_helper_is_total_exact_typed_and_bounded(values: Any) -> None:
    assert adjust_p_values_by(values) is None
    assert adjust_p_values_by([1.0] * (FAMILY_SIZE + 1)) is None


@pytest.mark.parametrize(
    "hostile",
    [None, True, 1, 1.0, "x", b"x", object(), {"cells": []}],
)
def test_public_evaluator_is_total_for_hostile_root_inputs(hostile: Any) -> None:
    decision = evaluate_historical_book_multiplicity(hostile)
    assert decision.status == "REJECTED"
    assert decision.valid is False
    assert not verify_historical_book_multiplicity_decision(decision)


def test_decision_is_deterministic_bounded_and_historical_only() -> None:
    family = _family(available_groups=(0,), overrides={0: _verified_winner()})
    first = evaluate_historical_book_multiplicity(family)
    second = evaluate_historical_book_multiplicity(family)
    assert first.receipt_sha256 == second.receipt_sha256
    assert first._body_json == second._body_json
    assert len(first._body_json) < 8 * 1024 * 1024
    assert first.to_dict()["selection_scope"] == BOOK_MULTIPLICITY_SCOPE


def test_policy_snapshot_is_defensive_and_has_no_live_authority() -> None:
    snapshot = pinned_policy_snapshot()
    assert snapshot["contract_version"] == BOOK_MULTIPLICITY_CONTRACT_VERSION
    assert snapshot["family_size"] == FAMILY_SIZE
    assert snapshot["source_group_size"] == SOURCE_GROUP_SIZE
    assert snapshot["selection_rule"] == SELECTION_RULE
    assert snapshot["one_source_decision_per_symbol_timeframe"] is True
    assert snapshot["one_regime_fit_decision_per_symbol_timeframe"] is True
    assert snapshot["causal_process_validation_required"] is True
    assert snapshot["live_authorized"] is False
    assert snapshot["order_authorized"] is False
    assert snapshot["capital_sizing_authorized"] is False
    assert snapshot["profitability_claimed"] is False
    snapshot["symbols"].clear()
    assert len(pinned_policy_snapshot()["symbols"]) == 100


def test_source_contains_no_signer_key_or_legacy_authority_surface() -> None:
    source = inspect.getsource(book)
    for forbidden in (
        "Ed25519",
        "HMAC",
        ".sign(",
        "TrustBundle",
        "winner_trust_bundle",
        "downstream_refit_blocked",
        "current_refit_authorized",
        '"selected"',
        "place_order",
    ):
        assert forbidden not in source
