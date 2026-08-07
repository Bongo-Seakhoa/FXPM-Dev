from __future__ import annotations

import ast
import copy
import hashlib
import inspect
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

import pm_training_horizon as horizon


def _canonical(value: object) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def _hash(value: object) -> str:
    return hashlib.sha256(_canonical(value)).hexdigest()


def _utc(value: str) -> datetime:
    return datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)


def _fold(
    fold_id: str,
    role: str,
    fit_start: str,
    fit_end: str,
    warmup_start: str,
    validation_start: str,
    validation_end: str,
    right_start: str | None = None,
    right_end: str | None = None,
) -> dict[str, object]:
    value: dict[str, object] = {
        "fold_id": fold_id,
        "role": role,
        "fit_start_utc": fit_start,
        "fit_end_exclusive_utc": fit_end,
        "validation_warmup_start_utc": warmup_start,
        "validation_start_utc": validation_start,
        "validation_end_exclusive_utc": validation_end,
        "right_fit_start_utc": right_start,
        "right_fit_end_exclusive_utc": right_end,
        "fit_rows": 100,
        "validation_rows": 30,
        "source_slice_sha256": "1" * 64,
        "final_oos_authority": False,
    }
    value["fold_definition_sha256"] = _hash(value)
    return value


def _candidate(
    candidate_id: str,
    lookback_kind: str,
    lookback_value: int,
    realized_start: str,
    folds: list[dict[str, object]],
) -> dict[str, object]:
    definition = {
        "candidate_id": candidate_id,
        "lookback_kind": lookback_kind,
        "lookback_value": lookback_value,
        "realized_start_utc": realized_start,
    }
    return {
        **definition,
        "candidate_definition_sha256": _hash(definition),
        "folds": folds,
    }


def _trial(
    trial_id: str, strategy_id: str, release: str, parameters: str
) -> dict[str, str]:
    definition = {
        "trial_id": trial_id,
        "strategy_id": strategy_id,
        "strategy_release_sha256": release,
        "parameter_set_sha256": parameters,
    }
    return {**definition, "trial_definition_sha256": _hash(definition)}


def _with_state_context(
    trial: dict[str, str],
    *,
    candidate: dict[str, object],
    required: int,
    selected: int,
) -> dict[str, object]:
    disposition_body = {
        "schema_version": "fxpm.production-trial-state-context-disposition.v1",
        "candidate_id": candidate["candidate_id"],
        "candidate_definition_sha256": candidate[
            "candidate_definition_sha256"
        ],
        "state_requirement_sha256": f"{required + 1:064x}",
        "required_context_bars": required,
        "selected_context_bars": selected,
        "status": "PLANNED" if required <= selected else "PROCESS_REJECTED",
        "reason_code": (
            None
            if required <= selected
            else "STATE_CONTEXT_SOURCE_BOUNDARY_SHORTFALL"
        ),
    }
    disposition = {
        **disposition_body,
        "disposition_sha256": _hash(disposition_body),
    }
    definition = {
        key: value
        for key, value in trial.items()
        if key != "trial_definition_sha256"
    }
    definition["state_context_disposition"] = disposition
    return {
        **definition,
        "trial_definition_sha256": _hash(definition),
    }


def _request() -> dict[str, object]:
    dependency_payload = {
        "schema_version": "fxpm.historical-dependency-policy-payload.v1",
        "feature_lookback_bars": 4,
        "label_lookahead_bars": 2,
        "max_holding_bars": 3,
        "execution_delay_bars": 1,
        "regime_warmup_bars": 5,
        "cost_lookback_bars": 2,
        "max_holding_seconds": 18_000,
    }
    causal_a = _fold(
        "ROLL-001",
        "CAUSAL_ROLLING_ORIGIN_INNER",
        "2018-06-01T00:00:00Z",
        "2019-03-01T00:00:00Z",
        "2019-03-31T18:00:00Z",
        "2019-04-01T00:00:00Z",
        "2019-05-01T00:00:00Z",
    )
    interior_a = _fold(
        "MIDDLE-001",
        "INTERPOLATION_ONLY",
        "2018-06-01T00:00:00Z",
        "2019-06-01T00:00:00Z",
        "2019-06-30T18:00:00Z",
        "2019-07-01T00:00:00Z",
        "2019-08-01T00:00:00Z",
        "2019-09-01T00:00:00Z",
        "2019-12-01T00:00:00Z",
    )
    causal_b = _fold(
        "ROLL-001",
        "CAUSAL_ROLLING_ORIGIN_INNER",
        "2018-02-01T00:00:00Z",
        "2019-03-01T00:00:00Z",
        "2019-03-31T18:00:00Z",
        "2019-04-01T00:00:00Z",
        "2019-05-01T00:00:00Z",
    )
    interior_b = _fold(
        "MIDDLE-001",
        "INTERPOLATION_ONLY",
        "2018-02-01T00:00:00Z",
        "2019-06-01T00:00:00Z",
        "2019-06-30T18:00:00Z",
        "2019-07-01T00:00:00Z",
        "2019-08-01T00:00:00Z",
        "2019-09-01T00:00:00Z",
        "2019-12-01T00:00:00Z",
    )
    candidates = [
        _candidate(
            "OBS-100",
            "NATIVE_OBSERVATIONS",
            100,
            "2018-06-01T00:00:00Z",
            [causal_a, interior_a],
        ),
        _candidate(
            "SESSIONS-400",
            "ACTIVE_SESSIONS",
            400,
            "2018-02-01T00:00:00Z",
            [causal_b, interior_b],
        ),
        _candidate(
            "CALENDAR-TOO-LONG",
            "CALENDAR_INTERVAL",
            7,
            "2017-01-01T00:00:00Z",
            [],
        ),
    ]
    return {
        "schema_version": "fxpm.historical-horizon-request.v1",
        "run_id": "anchor-run-001",
        "anchor": {
            "anchor_id": "anchor-2020-01",
            "ordinal": 7,
            "selection_cutoff_utc": "2020-01-01T00:00:00Z",
            "forward_start_utc": "2020-02-01T00:00:00Z",
            "forward_end_exclusive_utc": "2020-03-01T00:00:00Z",
            "evidence_accessed_at_utc": "2026-07-13T12:00:00Z",
            "process_precommit_receipt_sha256": "2" * 64,
        },
        "cell": {
            "symbol": "eUrUsd.a",
            "timeframe": "H1",
            "regime": "TREND",
        },
        "native_prefix": {
            "source_state": "AVAILABLE",
            "source_reason": None,
            "symbol": "eUrUsd.a",
            "timeframe": "H1",
            "activation_id": "native-activation-001",
            "activation_manifest_sha256": "3" * 64,
            "activation_entry_sha256": "4" * 64,
            "artifact_sha256": "5" * 64,
            "provenance_sha256": "6" * 64,
            "request_map_sha256": "7" * 64,
            "source_identity_sha256": "8" * 64,
            "continuity_policy_sha256": "9" * 64,
            "continuity_receipt_sha256": "a" * 64,
            "continuity_geometry_sha256": "b" * 64,
            "source_terminal_receipt_sha256": None,
            "source_accessed_at_utc": "2026-07-13T12:00:00Z",
            "request_start_utc": "2017-01-01T00:00:00Z",
            "request_end_exclusive_utc": "2026-07-14T00:00:00Z",
            "prefix_start_utc": "2018-01-01T00:00:00Z",
            "prefix_end_exclusive_utc": "2020-01-01T00:00:00Z",
            "prefix_first_open_utc": "2018-01-01T00:00:00Z",
            "prefix_last_open_utc": "2019-12-31T22:00:00Z",
            "prefix_last_close_utc": "2019-12-31T23:00:00Z",
            "artifact_rows": 80_000,
            "prefix_rows": 17_000,
            "prefix_schema_sha256": "c" * 64,
            "prefix_index_sha256": "d" * 64,
            "prefix_content_sha256": "e" * 64,
            "internal_status": "CONTINUOUS",
            "boundary_status": "LEFT_TRUNCATED",
            "unexplained_discontinuity_count": 0,
            "segments_sha256": "f" * 64,
            "requires_preserved_segment_ledger": True,
        },
        "dependency_policy": {
            "schema_version": "fxpm.historical-dependency-policy.v1",
            "authority_key_id": "dependency-policy-key-001",
            "signed_policy_receipt_sha256": "0" * 64,
            "payload_sha256": _hash(dependency_payload),
            "payload": dependency_payload,
        },
        "horizon_policy": {
            "schema_version": "fxpm.historical-horizon-policy.v1",
            "policy_id": "historical-horizon-policy-001",
            "policy_release_sha256": "a" * 64,
            "cost_contract_sha256": "b" * 64,
            "cost_stress_contract_sha256": "c" * 64,
            "evaluator_release_sha256": "d" * 64,
        },
        "candidates": candidates,
        "trials": [
            _trial("trial-alpha-001", "strategy-alpha", "1" * 64, "2" * 64),
            _trial("trial-beta-001", "strategy-beta", "3" * 64, "4" * 64),
        ],
    }


def _unobserved_request() -> dict[str, object]:
    request = _request()
    request["schema_version"] = "fxpm.historical-horizon-unobserved-request.v1"
    request["candidates"] = []
    request["trials"] = []
    counts = {"TREND": 0, "RANGE": 8_000, "BREAKOUT": 4_000, "CHOP": 3_000}
    support_body = {
        "schema_version": "fxpm.regime-train-support/v1",
        "symbol": "eUrUsd.a",
        "timeframe": "H1",
        "surface": "REGIME_LIVE",
        "warmup_policy": "CURRENT_AND_SOURCE_BAR_NOT_WARMUP",
        "label_order": ["TREND", "RANGE", "BREAKOUT", "CHOP"],
        "eligible_rows": 15_000,
        "counts": counts,
        "observed_regimes": ["RANGE", "BREAKOUT", "CHOP"],
        "unobserved_regimes": ["TREND"],
        "support_state_by_regime": {
            "TREND": "UNOBSERVED",
            "RANGE": "EMPIRICALLY_OBSERVED",
            "BREAKOUT": "EMPIRICALLY_OBSERVED",
            "CHOP": "EMPIRICALLY_OBSERVED",
        },
        "first_ts": "2018-01-02T00:00:00+00:00",
        "last_ts": "2019-12-31T22:00:00+00:00",
        "vector_encoding": "fxpm.regime-live-labels-nul/v1",
        "vector_sha256": "0" * 64,
        "bindings": {
            "symbol": "eUrUsd.a",
            "timeframe": "H1",
            "activation_id": "native-activation-001",
            "activation_manifest_sha256": "3" * 64,
            "source_sha256": "5" * 64,
            "train_window_sha256": "6" * 64,
            "params_sha256": "7" * 64,
            "detector_code_sha256": "8" * 64,
        },
    }
    support = {**support_body, "receipt_sha256": _hash(support_body)}
    binding_body = {
        "schema_version": "fxpm.historical-horizon-regime-support-binding.v1",
        "source_group_decision_sha256": "1" * 64,
        "regime_fit_decision_sha256": "2" * 64,
        "regime_detector_record_sha256": "3" * 64,
        "train_regime_support_receipt_sha256": support["receipt_sha256"],
        "target_regime": "TREND",
        "target_regime_count": 0,
        "train_regime_support": support,
    }
    request["regime_support_binding"] = {
        **binding_body,
        "binding_sha256": _hash(binding_body),
    }
    return request


def _reseal_unobserved_support(request: dict[str, object]) -> None:
    binding = request["regime_support_binding"]
    support = binding["train_regime_support"]
    support["receipt_sha256"] = _hash(
        {name: value for name, value in support.items() if name != "receipt_sha256"}
    )
    binding["train_regime_support_receipt_sha256"] = support["receipt_sha256"]
    binding["binding_sha256"] = _hash(
        {name: value for name, value in binding.items() if name != "binding_sha256"}
    )


def _shards(plan: object) -> list[dict[str, object]]:
    body = plan.to_dict()
    result: list[dict[str, object]] = []
    for expected in body["expected_evaluations"]:
        terminal = expected["expected_status"] == "PROCESS_REJECTED"
        if terminal:
            dates: list[str] = []
            daily: list[str] = []
            stress: list[str] = []
            closes: list[str] = []
            trades: list[str] = []
            status = "PROCESS_REJECTED"
            reason = expected["expected_reason_code"]
        else:
            validation_start = _utc(expected["validation_start_utc"])
            observation_time = validation_start + timedelta(days=1)
            dates = [observation_time.strftime("%Y-%m-%d")]
            daily = ["0.1"]
            stress = ["-0.2"]
            closes = [observation_time.strftime("%Y-%m-%dT%H:%M:%SZ")]
            trades = ["0.05"]
            status = "AUTHENTICATED"
            reason = None
        result.append(
            {
                "schema_version": "fxpm.historical-horizon-evidence-shard.v1",
                "attempt_id": expected["attempt_id"],
                "fold_id": expected["fold_id"],
                "context_sha256": expected["context_sha256"],
                "status": status,
                "reason_code": reason,
                "source_prefix_sha256": body["source_prefix_receipt_sha256"],
                "evaluator_release_sha256": body["horizon_policy"][
                    "evaluator_release_sha256"
                ],
                "cost_contract_sha256": body["horizon_policy"]["cost_contract_sha256"],
                "cost_stress_contract_sha256": body["horizon_policy"][
                    "cost_stress_contract_sha256"
                ],
                "evidence_role": expected["role"],
                "final_oos_authority": False,
                "daily_dates_utc": dates,
                "daily_net_mtm_r": daily,
                "daily_net_mtm_r_cost_stress": stress,
                "completed_trade_close_utc": closes,
                "completed_trade_net_mtm_r": trades,
            }
        )
    return result


@pytest.fixture(scope="module")
def valid_plan() -> object:
    return horizon.build_historical_horizon_plan(_request())


@pytest.fixture(scope="module")
def valid_shards(valid_plan: object) -> list[dict[str, object]]:
    return _shards(valid_plan)


@pytest.fixture(scope="module")
def valid_evidence(valid_plan: object, valid_shards: list[dict[str, object]]) -> object:
    return horizon.evaluate_historical_horizon_plan(
        valid_plan, copy.deepcopy(valid_shards)
    )


def test_public_surface_is_exact_and_has_no_injected_defaults() -> None:
    assert horizon.__all__ == (
        "build_historical_horizon_plan",
        "evaluate_historical_horizon_plan",
        "verify_historical_horizon_evidence",
    )
    expected = {
        "build_historical_horizon_plan": ["request"],
        "evaluate_historical_horizon_plan": ["plan", "evidence_shards"],
        "verify_historical_horizon_evidence": ["plan", "evidence"],
    }
    for name, parameter_names in expected.items():
        function = getattr(horizon, name)
        parameters = list(inspect.signature(function).parameters.values())
        assert [parameter.name for parameter in parameters] == parameter_names
        assert all(
            parameter.default is inspect.Parameter.empty for parameter in parameters
        )
        assert all(
            parameter.kind is inspect.Parameter.POSITIONAL_OR_KEYWORD
            for parameter in parameters
        )


def test_module_has_no_forbidden_authority_imports_or_storage_transport() -> None:
    path = Path(horizon.__file__)
    source = path.read_text(encoding="utf-8")
    tree = ast.parse(source)
    imported: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            imported.add(node.module or "")
    forbidden_modules = {
        "pm_winner",
        "pm_book",
        "pm_causal_process",
        "pm_current_refit",
        "pm_live",
        "pm_capital",
        "pm_promotion",
        "hmac",
        "pickle",
        "pandas",
    }
    assert imported.isdisjoint(forbidden_modules)
    lowered = source.lower()
    assert "dataframe" not in lowered
    assert "import pickle" not in lowered
    assert "0.05" not in source
    assert "sharpe" not in lowered


def test_plan_binds_prefix_and_builds_complete_cartesian_roster(
    valid_plan: object,
) -> None:
    body = valid_plan.to_dict()
    assert body["authority_scope"] == "HISTORICAL_ANCHOR_LOCAL_HORIZON_EVIDENCE_ONLY"
    assert body["trust_class"] == "LOCAL_SINGLE_USER_DETERMINISTIC_OFFLINE_EVIDENCE"
    assert body["requires_external_exact_artifact_verification"] is False
    assert body["eligible_for_downstream_offline_selection"] is True
    assert body["confers_trading_or_capital_authority"] is False
    assert body["historical_anchor_local_only"] is True
    assert body["selects_within_cell_process"] is False
    assert body["confers_final_oos_authority"] is False
    assert body["cell"] == {"symbol": "eUrUsd.a", "timeframe": "H1", "regime": "TREND"}
    assert body["native_prefix"]["activation_id"] == "native-activation-001"
    assert body["native_prefix"]["continuity_receipt_sha256"] == "a" * 64
    assert body["source_prefix_receipt_sha256"] == _hash(
        {
            "cell": body["cell"],
            "anchor_id": body["anchor"]["anchor_id"],
            "selection_cutoff_utc": body["anchor"]["selection_cutoff_utc"],
            "native_prefix": body["native_prefix"],
        }
    )
    assert len(body["candidate_registry"]) == 3
    assert len(body["trial_registry"]) == 2
    assert len(body["attempts"]) == 6
    assert len({item["attempt_id"] for item in body["attempts"]}) == 6
    assert len(body["expected_evaluations"]) == 10
    assert body["candidate_registry_sha256"] == _hash(body["candidate_registry"])
    assert body["trial_registry_sha256"] == _hash(body["trial_registry"])
    assert body["attempt_roster_sha256"] == _hash(body["attempts"])
    assert body["evidence_roster_sha256"] == _hash(body["expected_evaluations"])
    resource_contract = body["resource_contract"]
    assert resource_contract["expected_evaluation_ceiling"] == 100_000
    assert all(
        "timeout" not in key.lower() and "deadline" not in key.lower()
        for key in resource_contract
    )
    assert horizon._TECHNICAL_REASONS == frozenset(  # noqa: SLF001
        {
            "EVALUATOR_OOM",
            "SOURCE_INTEGRITY_FAILURE",
            "EVALUATOR_EXCEPTION",
        }
    )


def test_dependency_geometry_is_derived_from_bound_policy(valid_plan: object) -> None:
    geometry = valid_plan.to_dict()["derived_dependency_geometry"]
    assert geometry == {
        "timeframe_seconds": 3_600,
        "warmup_bars": 5,
        "left_purge_bars": 4,
        "right_purge_bars": 5,
        "warmup_seconds": 18_000,
        "left_purge_seconds": 21_600,
        "right_purge_seconds": 21_600,
    }
    weakened = _request()
    fold = weakened["candidates"][0]["folds"][0]
    fold["fit_end_exclusive_utc"] = "2019-03-31T20:00:00Z"
    fold["fold_definition_sha256"] = _hash(
        {key: value for key, value in fold.items() if key != "fold_definition_sha256"}
    )
    with pytest.raises(ValueError, match="purge|embargo"):
        horizon.build_historical_horizon_plan(weakened)


def test_execution_delay_and_holding_are_additive_purge_dependencies() -> None:
    request = _request()
    payload = request["dependency_policy"]["payload"]
    payload["execution_delay_bars"] = 4
    payload["max_holding_bars"] = 4
    payload["max_holding_seconds"] = 0
    request["dependency_policy"]["payload_sha256"] = _hash(payload)
    plan = horizon.build_historical_horizon_plan(request)
    geometry = plan.to_dict()["derived_dependency_geometry"]
    assert geometry["left_purge_bars"] == 8
    assert geometry["left_purge_seconds"] == 8 * 3_600

    overlapped = _request()
    payload = overlapped["dependency_policy"]["payload"]
    payload["execution_delay_bars"] = 4
    payload["max_holding_bars"] = 4
    payload["max_holding_seconds"] = 0
    overlapped["dependency_policy"]["payload_sha256"] = _hash(payload)
    for candidate in overlapped["candidates"][:2]:
        fold = candidate["folds"][0]
        fold["fit_end_exclusive_utc"] = "2019-03-31T20:00:00Z"
        fold["fold_definition_sha256"] = _hash(
            {
                key: value
                for key, value in fold.items()
                if key != "fold_definition_sha256"
            }
        )
    with pytest.raises(ValueError, match="purge|embargo"):
        horizon.build_historical_horizon_plan(overlapped)


def test_native_observation_horizon_requires_actual_prefix_rows_and_fit_rows() -> None:
    impossible = _request()
    candidate = impossible["candidates"][0]
    candidate["candidate_id"] = "OBS-50000"
    candidate["lookback_value"] = 50_000
    candidate["candidate_definition_sha256"] = _hash(
        {
            key: value
            for key, value in candidate.items()
            if key not in ("candidate_definition_sha256", "folds")
        }
    )
    for fold in candidate["folds"]:
        fold["fit_rows"] = 50_000
        fold["fold_definition_sha256"] = _hash(
            {
                key: value
                for key, value in fold.items()
                if key != "fold_definition_sha256"
            }
        )
    assert impossible["native_prefix"]["prefix_rows"] == 17_000
    with pytest.raises(ValueError, match="boundary-trimmed"):
        horizon.build_historical_horizon_plan(impossible)

    mechanically_rejected = copy.deepcopy(impossible)
    mechanically_rejected["candidates"][0]["folds"] = []
    plan = horizon.build_historical_horizon_plan(mechanically_rejected).to_dict()
    attempts = [
        item for item in plan["attempts"] if item["candidate_id"] == "OBS-50000"
    ]
    assert attempts
    assert {item["status"] for item in attempts} == {"PROCESS_REJECTED"}
    assert {item["reason_code"] for item in attempts} == {"SOURCE_BOUNDARY_SHORTFALL"}

    mismatched_fit_rows = _request()
    mismatched_fit_rows["candidates"][0]["folds"][0]["fit_rows"] = 99
    fold = mismatched_fit_rows["candidates"][0]["folds"][0]
    fold["fold_definition_sha256"] = _hash(
        {key: value for key, value in fold.items() if key != "fold_definition_sha256"}
    )
    with pytest.raises(ValueError, match="native-observation lookback"):
        horizon.build_historical_horizon_plan(mismatched_fit_rows)

    impossible_window = _request()
    candidate = impossible_window["candidates"][0]
    candidate["lookback_value"] = 10_000
    candidate["candidate_definition_sha256"] = _hash(
        {
            key: value
            for key, value in candidate.items()
            if key not in ("candidate_definition_sha256", "folds")
        }
    )
    for fold in candidate["folds"]:
        fold["fit_rows"] = 10_000
        fold["fold_definition_sha256"] = _hash(
            {
                key: value
                for key, value in fold.items()
                if key != "fold_definition_sha256"
            }
        )
    with pytest.raises(ValueError, match="fit-window capacity"):
        horizon.build_historical_horizon_plan(impossible_window)


def test_realized_start_and_paired_information_boundaries_are_exact() -> None:
    mismatched_start = _request()
    candidate = mismatched_start["candidates"][0]
    candidate["realized_start_utc"] = "2018-05-01T00:00:00Z"
    candidate["candidate_definition_sha256"] = _hash(
        {
            key: value
            for key, value in candidate.items()
            if key not in ("candidate_definition_sha256", "folds")
        }
    )
    with pytest.raises(ValueError, match="realized start"):
        horizon.build_historical_horizon_plan(mismatched_start)

    unsynchronized = _request()
    fold = unsynchronized["candidates"][1]["folds"][0]
    fold["fit_end_exclusive_utc"] = "2019-02-01T00:00:00Z"
    fold["fold_definition_sha256"] = _hash(
        {key: value for key, value in fold.items() if key != "fold_definition_sha256"}
    )
    with pytest.raises(ValueError, match="paired information"):
        horizon.build_historical_horizon_plan(unsynchronized)


def test_candidate_horizon_bounds_warmup_and_causal_windows_advance() -> None:
    outside_horizon = _request()
    fold = outside_horizon["candidates"][0]["folds"][0]
    fold["validation_warmup_start_utc"] = "2018-05-31T18:00:00Z"
    fold["fold_definition_sha256"] = _hash(
        {key: value for key, value in fold.items() if key != "fold_definition_sha256"}
    )
    with pytest.raises(ValueError, match="warmup"):
        horizon.build_historical_horizon_plan(outside_horizon)

    non_rolling = _request()
    second_causal = _fold(
        "ROLL-002",
        "CAUSAL_ROLLING_ORIGIN_INNER",
        "2018-06-01T00:00:00Z",
        "2019-03-01T00:00:00Z",
        "2019-09-30T18:00:00Z",
        "2019-10-01T00:00:00Z",
        "2019-11-01T00:00:00Z",
    )
    non_rolling["candidates"] = [
        non_rolling["candidates"][0],
        non_rolling["candidates"][2],
    ]
    non_rolling["candidates"][0]["folds"].append(second_causal)
    with pytest.raises(ValueError, match="do not advance"):
        horizon.build_historical_horizon_plan(non_rolling)


def test_semantic_candidate_and_strategy_parameter_duplicates_are_rejected() -> None:
    duplicate_candidate = _request()
    copied_candidate = copy.deepcopy(duplicate_candidate["candidates"][0])
    copied_candidate["candidate_id"] = "OBS-100-ALIAS"
    copied_candidate["realized_start_utc"] = "2018-05-01T00:00:00Z"
    for fold in copied_candidate["folds"]:
        fold["fit_start_utc"] = "2018-05-01T00:00:00Z"
        fold["fold_definition_sha256"] = _hash(
            {
                key: value
                for key, value in fold.items()
                if key != "fold_definition_sha256"
            }
        )
    candidate_definition = {
        key: value
        for key, value in copied_candidate.items()
        if key not in ("candidate_definition_sha256", "folds")
    }
    copied_candidate["candidate_definition_sha256"] = _hash(candidate_definition)
    duplicate_candidate["candidates"].append(copied_candidate)
    with pytest.raises(ValueError, match="duplicate candidate identity"):
        horizon.build_historical_horizon_plan(duplicate_candidate)

    duplicate_trial = _request()
    copied_trial = copy.deepcopy(duplicate_trial["trials"][0])
    copied_trial["trial_id"] = "trial-alpha-alias"
    copied_trial["trial_definition_sha256"] = _hash(
        {
            key: value
            for key, value in copied_trial.items()
            if key != "trial_definition_sha256"
        }
    )
    duplicate_trial["trials"].append(copied_trial)
    with pytest.raises(ValueError, match="duplicate strategy-parameter identity"):
        horizon.build_historical_horizon_plan(duplicate_trial)


def test_authenticated_discontinuities_require_preserved_segment_ledger() -> None:
    request = _request()
    request["native_prefix"]["internal_status"] = (
        "EXPECTED_AUTHENTICATED_DISCONTINUITIES"
    )
    request["native_prefix"]["requires_preserved_segment_ledger"] = False
    with pytest.raises(ValueError, match="preserved segment ledger"):
        horizon.build_historical_horizon_plan(request)


def test_interior_fold_is_interpolation_only_and_never_final_oos(
    valid_plan: object,
) -> None:
    body = valid_plan.to_dict()
    interiors = [
        item
        for item in body["expected_evaluations"]
        if item["role"] == "INTERPOLATION_ONLY"
    ]
    assert interiors
    assert all(item["downstream_causal_eligible"] is False for item in interiors)
    assert all(item["final_oos_authority"] is False for item in interiors)

    final_oos = _request()
    fold = final_oos["candidates"][0]["folds"][1]
    fold["final_oos_authority"] = True
    fold["fold_definition_sha256"] = _hash(
        {key: value for key, value in fold.items() if key != "fold_definition_sha256"}
    )
    with pytest.raises(ValueError, match="final OOS"):
        horizon.build_historical_horizon_plan(final_oos)

    weak_right = _request()
    fold = weak_right["candidates"][0]["folds"][1]
    fold["right_fit_start_utc"] = "2019-08-01T04:00:00Z"
    fold["fold_definition_sha256"] = _hash(
        {key: value for key, value in fold.items() if key != "fold_definition_sha256"}
    )
    with pytest.raises(ValueError, match="right purge"):
        horizon.build_historical_horizon_plan(weak_right)


def test_causal_fold_rejects_right_side_training() -> None:
    request = _request()
    fold = request["candidates"][0]["folds"][0]
    fold["right_fit_start_utc"] = "2019-05-02T00:00:00Z"
    fold["right_fit_end_exclusive_utc"] = "2019-06-01T00:00:00Z"
    fold["fold_definition_sha256"] = _hash(
        {key: value for key, value in fold.items() if key != "fold_definition_sha256"}
    )
    with pytest.raises(ValueError, match="right-side"):
        horizon.build_historical_horizon_plan(request)


def test_future_acquisition_clock_is_allowed_but_future_market_data_is_not() -> None:
    original = horizon.build_historical_horizon_plan(_request())
    later_access = _request()
    later_access["anchor"]["evidence_accessed_at_utc"] = "2035-01-01T00:00:00Z"
    later_access["native_prefix"]["source_accessed_at_utc"] = "2035-01-01T00:00:00Z"
    later = horizon.build_historical_horizon_plan(later_access)
    assert original.receipt_sha256 != later.receipt_sha256
    assert (
        original.to_dict()["source_prefix_receipt_sha256"]
        != later.to_dict()["source_prefix_receipt_sha256"]
    )

    leaked = _request()
    leaked["native_prefix"]["prefix_end_exclusive_utc"] = "2020-01-02T00:00:00Z"
    leaked["native_prefix"]["prefix_last_open_utc"] = "2020-01-01T22:00:00Z"
    leaked["native_prefix"]["prefix_last_close_utc"] = "2020-01-01T23:00:00Z"
    with pytest.raises(ValueError, match="leaks beyond"):
        horizon.build_historical_horizon_plan(leaked)

    impossible_clock = _request()
    impossible_clock["native_prefix"]["source_accessed_at_utc"] = "2026-07-13T12:00:01Z"
    with pytest.raises(ValueError, match="source access clock"):
        horizon.build_historical_horizon_plan(impossible_clock)


def _terminal_source_request(state: str) -> dict[str, object]:
    request = _request()
    prefix = request["native_prefix"]
    prefix["source_state"] = state
    prefix["source_reason"] = (
        "BROKER_ARCHIVE_UNAVAILABLE"
        if state == "SOURCE_UNAVAILABLE"
        else "SOURCE_IO_ERROR"
    )
    prefix["source_terminal_receipt_sha256"] = "5" * 64
    for key in (
        "artifact_sha256",
        "provenance_sha256",
        "prefix_start_utc",
        "prefix_end_exclusive_utc",
        "prefix_first_open_utc",
        "prefix_last_open_utc",
        "prefix_last_close_utc",
        "artifact_rows",
        "prefix_rows",
        "prefix_schema_sha256",
        "prefix_index_sha256",
        "prefix_content_sha256",
    ):
        prefix[key] = None
    request["candidates"] = []
    request["trials"] = []
    return request


def test_boundary_shortfall_is_local_and_does_not_veto_siblings(
    valid_plan: object,
) -> None:
    body = valid_plan.to_dict()
    assert body["plan_status"] == "READY_FOR_HISTORICAL_EVIDENCE"
    rejected = [
        item
        for item in body["attempts"]
        if item["reason_code"] == "SOURCE_BOUNDARY_SHORTFALL"
    ]
    planned = [item for item in body["attempts"] if item["status"] == "PLANNED"]
    assert len(rejected) == 2
    assert len(planned) == 4
    assert {item["candidate_id"] for item in rejected} == {"CALENDAR-TOO-LONG"}

    unavailable = horizon.build_historical_horizon_plan(
        _terminal_source_request("SOURCE_UNAVAILABLE")
    )
    assert unavailable.to_dict()["plan_status"] == "HISTORICAL_SOURCE_UNAVAILABLE"
    independent = horizon.build_historical_horizon_plan(_request())
    assert independent.to_dict()["plan_status"] == "READY_FOR_HISTORICAL_EVIDENCE"
    assert independent.receipt_sha256 == valid_plan.receipt_sha256


def _mixed_state_context_request() -> dict[str, object]:
    request = _request()
    candidate = request["candidates"][0]
    request["candidates"] = [candidate]
    request["trials"] = [
        _with_state_context(
            request["trials"][0],
            candidate=candidate,
            required=4,
            selected=4,
        ),
        _with_state_context(
            request["trials"][1],
            candidate=candidate,
            required=5,
            selected=4,
        ),
    ]
    return request


def test_state_context_shortfall_is_trial_local_and_evidence_reason_is_exact() -> None:
    plan = horizon.build_historical_horizon_plan(
        _mixed_state_context_request()
    )
    body = plan.to_dict()

    assert body["plan_status"] == "READY_FOR_HISTORICAL_EVIDENCE"
    assert [item["status"] for item in body["attempts"]] == [
        "PLANNED",
        "PROCESS_REJECTED",
    ]
    assert body["attempts"][1]["reason_code"] == (
        "STATE_CONTEXT_SOURCE_BOUNDARY_SHORTFALL"
    )
    assert len(body["expected_evaluations"]) == 3
    terminal = [
        item
        for item in body["expected_evaluations"]
        if item["expected_status"] == "PROCESS_REJECTED"
    ]
    assert len(terminal) == 1
    assert terminal[0]["expected_reason_code"] == (
        "STATE_CONTEXT_SOURCE_BOUNDARY_SHORTFALL"
    )
    assert all(
        "state_context_disposition_sha256" in item
        for item in body["trial_registry"]
    )

    evidence = horizon.evaluate_historical_horizon_plan(
        plan,
        _shards(plan),
    )
    assert horizon.verify_historical_horizon_evidence(plan, evidence) is True


def test_state_context_disposition_roster_and_inequality_fail_closed() -> None:
    missing = _mixed_state_context_request()
    legacy = _trial(
        "trial-beta",
        "breakout",
        "d" * 64,
        "e" * 64,
    )
    missing["trials"][1] = legacy
    with pytest.raises(ValueError, match="exact trial roster"):
        horizon.build_historical_horizon_plan(missing)

    invalid = _mixed_state_context_request()
    trial = invalid["trials"][1]
    disposition = trial["state_context_disposition"]
    disposition["status"] = "PLANNED"
    disposition["reason_code"] = None
    disposition["disposition_sha256"] = _hash(
        {
            key: value
            for key, value in disposition.items()
            if key != "disposition_sha256"
        }
    )
    trial["trial_definition_sha256"] = _hash(
        {
            key: value
            for key, value in trial.items()
            if key != "trial_definition_sha256"
        }
    )
    with pytest.raises(ValueError, match="inequality mismatch"):
        horizon.build_historical_horizon_plan(invalid)


def test_state_context_disposition_must_bind_dependency_and_candidate() -> None:
    for field, value, match in (
        ("selected_context_bars", 3, "candidate or dependency mismatch"),
        ("candidate_definition_sha256", "0" * 64, "candidate or dependency mismatch"),
    ):
        request = _mixed_state_context_request()
        for trial in request["trials"]:
            disposition = trial["state_context_disposition"]
            disposition[field] = value
            required = disposition["required_context_bars"]
            selected = disposition["selected_context_bars"]
            disposition["status"] = (
                "PLANNED" if required <= selected else "PROCESS_REJECTED"
            )
            disposition["reason_code"] = (
                None
                if required <= selected
                else "STATE_CONTEXT_SOURCE_BOUNDARY_SHORTFALL"
            )
            disposition["disposition_sha256"] = _hash(
                {
                    key: item
                    for key, item in disposition.items()
                    if key != "disposition_sha256"
                }
            )
            trial["trial_definition_sha256"] = _hash(
                {
                    key: item
                    for key, item in trial.items()
                    if key != "trial_definition_sha256"
                }
            )
        with pytest.raises(ValueError, match=match):
            horizon.build_historical_horizon_plan(request)


def test_authenticated_unobserved_regime_has_exact_zero_work_roster() -> None:
    request = _unobserved_request()
    plan = horizon.build_historical_horizon_plan(request)
    body = plan.to_dict()
    assert body["schema_version"] == "fxpm.historical-horizon-plan.v1"
    assert body["plan_status"] == "HISTORICAL_REGIME_UNOBSERVED"
    assert body["native_prefix"]["source_state"] == "AVAILABLE"
    assert body["source_prefix_receipt_sha256"] is not None
    assert body["candidates"] == []
    assert body["trials"] == []
    assert body["candidate_registry"] == []
    assert body["trial_registry"] == []
    assert body["attempts"] == []
    assert body["expected_evaluations"] == []
    assert body["candidate_registry_sha256"] == _hash([])
    assert body["trial_registry_sha256"] == _hash([])
    assert body["attempt_roster_sha256"] == _hash([])
    assert body["evidence_roster_sha256"] == _hash([])
    assert body["regime_support_binding"] == request["regime_support_binding"]
    assert body["regime_support_binding"]["target_regime_count"] == 0
    assert (
        body["regime_support_binding"]["train_regime_support"]["counts"]["TREND"] == 0
    )

    evidence = horizon.evaluate_historical_horizon_plan(plan, [])
    evidence_body = evidence.to_dict()
    assert evidence_body["evidence_status"] == "HISTORICAL_REGIME_UNOBSERVED"
    assert evidence_body["shard_count"] == 0
    assert evidence_body["authenticated_shard_count"] == 0
    assert horizon.verify_historical_horizon_evidence(plan, evidence) is True
    with pytest.raises(ValueError, match="unobserved.*cannot accept"):
        horizon.evaluate_historical_horizon_plan(plan, [{}])


def test_all_zero_regime_support_has_exact_zero_work_roster() -> None:
    request = _unobserved_request()
    binding = request["regime_support_binding"]
    support = binding["train_regime_support"]
    support["eligible_rows"] = 0
    support["counts"] = {
        "TREND": 0,
        "RANGE": 0,
        "BREAKOUT": 0,
        "CHOP": 0,
    }
    support["observed_regimes"] = []
    support["unobserved_regimes"] = ["TREND", "RANGE", "BREAKOUT", "CHOP"]
    support["support_state_by_regime"] = {
        regime: "UNOBSERVED" for regime in support["label_order"]
    }
    support["first_ts"] = None
    support["last_ts"] = None
    _reseal_unobserved_support(request)

    plan = horizon.build_historical_horizon_plan(request)
    body = plan.to_dict()

    assert body["plan_status"] == "HISTORICAL_REGIME_UNOBSERVED"
    assert body["candidates"] == []
    assert body["trials"] == []
    assert body["attempts"] == []
    assert body["expected_evaluations"] == []
    assert body["regime_support_binding"] == binding

    forged = _unobserved_request()
    forged_binding = forged["regime_support_binding"]
    forged_support = forged_binding["train_regime_support"]
    forged_support.update(support)
    forged_support["first_ts"] = "2022-01-24T00:00:00+00:00"
    _reseal_unobserved_support(forged)
    with pytest.raises(
        ValueError,
        match="empty regime support must not claim a label time range",
    ):
        horizon.build_historical_horizon_plan(forged)


def test_unobserved_regime_rejects_support_count_and_partition_tampering() -> None:
    observed_substitution = _unobserved_request()
    binding = observed_substitution["regime_support_binding"]
    support = binding["train_regime_support"]
    support["counts"]["TREND"] = 1
    support["counts"]["RANGE"] = 7_999
    support["observed_regimes"] = ["TREND", "RANGE", "BREAKOUT", "CHOP"]
    support["unobserved_regimes"] = []
    support["support_state_by_regime"]["TREND"] = "EMPIRICALLY_OBSERVED"
    binding["target_regime_count"] = 1
    _reseal_unobserved_support(observed_substitution)
    with pytest.raises(ValueError, match="observed regime.*zero-attempt"):
        horizon.build_historical_horizon_plan(observed_substitution)

    top_level_count_splice = _unobserved_request()
    top_binding = top_level_count_splice["regime_support_binding"]
    top_binding["target_regime_count"] = 1
    top_binding["binding_sha256"] = _hash(
        {name: value for name, value in top_binding.items() if name != "binding_sha256"}
    )
    with pytest.raises(ValueError, match="count does not match"):
        horizon.build_historical_horizon_plan(top_level_count_splice)

    partition_splice = _unobserved_request()
    partition_binding = partition_splice["regime_support_binding"]
    partition_binding["train_regime_support"]["unobserved_regimes"] = []
    _reseal_unobserved_support(partition_splice)
    with pytest.raises(ValueError, match="partition mismatch"):
        horizon.build_historical_horizon_plan(partition_splice)

    receipt_splice = _unobserved_request()
    receipt_splice["regime_support_binding"]["train_regime_support"][
        "vector_sha256"
    ] = "f" * 64
    with pytest.raises(ValueError, match="support receipt mismatch"):
        horizon.build_historical_horizon_plan(receipt_splice)


def test_unobserved_regime_rejects_cross_cell_source_and_observed_zero_work() -> None:
    source_splice = _unobserved_request()
    source_splice["regime_support_binding"]["train_regime_support"]["bindings"][
        "activation_id"
    ] = "different-activation"
    _reseal_unobserved_support(source_splice)
    with pytest.raises(ValueError, match="native source binding mismatch"):
        horizon.build_historical_horizon_plan(source_splice)

    target_splice = _unobserved_request()
    target_splice["regime_support_binding"]["target_regime"] = "RANGE"
    target_splice["regime_support_binding"]["binding_sha256"] = _hash(
        {
            name: value
            for name, value in target_splice["regime_support_binding"].items()
            if name != "binding_sha256"
        }
    )
    with pytest.raises(ValueError, match="target mismatch"):
        horizon.build_historical_horizon_plan(target_splice)

    observed_v1 = _request()
    observed_v1["candidates"] = []
    observed_v1["trials"] = []
    with pytest.raises(ValueError, match="available source requires candidate roster"):
        horizon.build_historical_horizon_plan(observed_v1)


def test_technical_source_integrity_cannot_be_laundered_as_process_rejection(
    valid_shards: list[dict[str, object]],
) -> None:
    request = _request()
    request["native_prefix"]["internal_status"] = "UNEXPLAINED"
    request["native_prefix"]["unexplained_discontinuity_count"] = 1
    plan = horizon.build_historical_horizon_plan(request)
    body = plan.to_dict()
    assert body["plan_status"] == "HISTORICAL_TECHNICAL_INCOMPLETE"
    assert body["expected_evaluations"] == []
    assert body["attempts"]
    assert {item["status"] for item in body["attempts"]} == {"TECHNICAL_INCOMPLETE"}
    with pytest.raises(ValueError, match="terminal historical source|cannot accept"):
        horizon.evaluate_historical_horizon_plan(plan, [copy.deepcopy(valid_shards[0])])

    unavailable = horizon.build_historical_horizon_plan(
        _terminal_source_request("SOURCE_UNAVAILABLE")
    )
    technical = horizon.build_historical_horizon_plan(
        _terminal_source_request("TECHNICAL_FAILURE")
    )
    assert unavailable.to_dict()["plan_status"] == "HISTORICAL_SOURCE_UNAVAILABLE"
    assert technical.to_dict()["plan_status"] == "HISTORICAL_TECHNICAL_INCOMPLETE"


def test_authenticated_terminal_technical_is_distinct_complete_zero_work_plan() -> None:
    request = _terminal_source_request("TECHNICAL_TERMINAL")
    request["native_prefix"]["source_reason"] = (
        "AUTHENTICATED_NATIVE_PREFIX_TERMINAL_TECHNICAL"
    )
    plan = horizon.build_historical_horizon_plan(request)
    body = plan.to_dict()

    assert body["plan_status"] == "HISTORICAL_TERMINAL_TECHNICAL"
    assert body["native_prefix"]["source_state"] == "TECHNICAL_TERMINAL"
    assert body["candidates"] == []
    assert body["trials"] == []
    assert body["attempts"] == []
    assert body["expected_evaluations"] == []
    assert body["confers_trading_or_capital_authority"] is False

    evidence = horizon.evaluate_historical_horizon_plan(plan, [])
    assert evidence.to_dict()["evidence_status"] == ("HISTORICAL_TERMINAL_TECHNICAL")
    assert horizon.verify_historical_horizon_evidence(plan, evidence) is True


def test_exact_evidence_is_complete_replayable_and_non_decisional(
    valid_plan: object,
    valid_evidence: object,
) -> None:
    body = valid_evidence.to_dict()
    assert body["evidence_status"] == "HISTORICAL_EVIDENCE_COMPLETE"
    assert body["trust_class"] == "LOCAL_SINGLE_USER_DETERMINISTIC_OFFLINE_EVIDENCE"
    assert body["requires_external_exact_artifact_verification"] is False
    assert body["eligible_for_downstream_offline_selection"] is True
    assert body["confers_trading_or_capital_authority"] is False
    assert body["evidence_completeness_scope"] == "ROSTER_AND_SCHEMA_ONLY"
    assert body["authenticated_shard_count"] == 8
    assert body["process_rejected_shard_count"] == 2
    assert body["technical_failure_shard_count"] == 0
    assert body["shard_count"] == 10
    assert body["shard_receipts_sha256"] == _hash(body["shard_receipts"])
    assert (
        horizon.verify_historical_horizon_evidence(valid_plan, valid_evidence) is True
    )
    all_keys = set(body)
    assert not any(
        "selected" in key or "profit" in key or "verdict" in key for key in all_keys
    )
    assert all(
        receipt["final_oos_authority"] is False for receipt in body["shard_receipts"]
    )


def test_tampered_sealed_evidence_fails_replay(
    valid_plan: object, valid_evidence: object
) -> None:
    body = valid_evidence.to_dict()
    body["shards"][0]["daily_net_mtm_r"][0] = "9"
    forged = object.__new__(type(valid_evidence))
    forged_bytes = _canonical(body)
    object.__setattr__(forged, "_body_json", forged_bytes)
    object.__setattr__(
        forged, "_receipt_sha256", hashlib.sha256(forged_bytes).hexdigest()
    )
    object.__setattr__(forged, "_seal", valid_evidence._seal)
    object.__setattr__(forged, "_locked", True)
    assert horizon.verify_historical_horizon_evidence(valid_plan, forged) is False
    assert horizon.verify_historical_horizon_evidence(valid_plan, body) is False


def test_evidence_roster_omission_reorder_and_duplication_fail_closed(
    valid_plan: object,
    valid_shards: list[dict[str, object]],
) -> None:
    variants = [
        valid_shards[:-1],
        [valid_shards[1], valid_shards[0], *valid_shards[2:]],
        [*valid_shards, copy.deepcopy(valid_shards[-1])],
    ]
    for variant in variants:
        with pytest.raises(ValueError, match="roster|attempt|fold"):
            horizon.evaluate_historical_horizon_plan(valid_plan, copy.deepcopy(variant))


def test_technical_shard_remains_technical_incomplete(
    valid_plan: object,
    valid_shards: list[dict[str, object]],
) -> None:
    shards = copy.deepcopy(valid_shards)
    shard = next(item for item in shards if item["status"] == "AUTHENTICATED")
    shard["status"] = "TECHNICAL_FAILURE"
    shard["reason_code"] = "EVALUATOR_OOM"
    for key in (
        "daily_dates_utc",
        "daily_net_mtm_r",
        "daily_net_mtm_r_cost_stress",
        "completed_trade_close_utc",
        "completed_trade_net_mtm_r",
    ):
        shard[key] = []
    evidence = horizon.evaluate_historical_horizon_plan(valid_plan, shards)
    assert (
        evidence.to_dict()["evidence_status"]
        == "HISTORICAL_EVIDENCE_TECHNICAL_INCOMPLETE"
    )
    assert evidence.to_dict()["technical_failure_shard_count"] == 1
    assert horizon.verify_historical_horizon_evidence(valid_plan, evidence) is True


def test_zero_declared_rows_and_empty_authenticated_vectors_fail_closed(
    valid_plan: object,
    valid_shards: list[dict[str, object]],
) -> None:
    zero_rows = _request()
    fold = zero_rows["candidates"][0]["folds"][0]
    fold["fit_rows"] = 0
    fold["validation_rows"] = 0
    fold["fold_definition_sha256"] = _hash(
        {key: value for key, value in fold.items() if key != "fold_definition_sha256"}
    )
    with pytest.raises(ValueError, match="fit rows|validation rows"):
        horizon.build_historical_horizon_plan(zero_rows)

    empty = copy.deepcopy(valid_shards)
    for shard in empty:
        if shard["status"] != "AUTHENTICATED":
            continue
        for key in (
            "daily_dates_utc",
            "daily_net_mtm_r",
            "daily_net_mtm_r_cost_stress",
            "completed_trade_close_utc",
            "completed_trade_net_mtm_r",
        ):
            shard[key] = []
    with pytest.raises(ValueError, match="nonempty daily MTM"):
        horizon.evaluate_historical_horizon_plan(valid_plan, empty)


def test_same_second_trade_closes_remain_lossless(
    valid_plan: object,
    valid_shards: list[dict[str, object]],
) -> None:
    shards = copy.deepcopy(valid_shards)
    shard = next(item for item in shards if item["status"] == "AUTHENTICATED")
    close = shard["completed_trade_close_utc"][0]
    shard["completed_trade_close_utc"] = [close, close]
    shard["completed_trade_net_mtm_r"] = ["0.05", "-0.01"]
    evidence = horizon.evaluate_historical_horizon_plan(valid_plan, shards)
    stored = next(
        item
        for item in evidence.to_dict()["shards"]
        if item["attempt_id"] == shard["attempt_id"]
        and item["fold_id"] == shard["fold_id"]
    )
    assert stored["completed_trade_close_utc"] == [close, close]
    assert stored["completed_trade_net_mtm_r"] == ["0.05", "-0.01"]


def test_roles_survive_exact_evidence_without_interior_authority(
    valid_evidence: object,
) -> None:
    receipts = valid_evidence.to_dict()["shard_receipts"]
    causal = [
        item
        for item in receipts
        if item["evidence_role"] == "CAUSAL_ROLLING_ORIGIN_INNER"
    ]
    interior = [
        item for item in receipts if item["evidence_role"] == "INTERPOLATION_ONLY"
    ]
    terminal = [
        item
        for item in receipts
        if item["evidence_role"] == "TERMINAL_MECHANICAL_REJECTION"
    ]
    assert causal and interior and terminal
    assert all(item["downstream_causal_eligible"] is True for item in causal)
    assert all(
        item["downstream_causal_eligible"] is False for item in interior + terminal
    )
    assert all(item["final_oos_authority"] is False for item in receipts)


def test_unbounded_and_non_json_inputs_fail_before_planning() -> None:
    floating = _request()
    floating["anchor"]["ordinal"] = 0.5
    with pytest.raises(ValueError, match="exact JSON"):
        horizon.build_historical_horizon_plan(floating)

    huge_integer = _request()
    huge_integer["anchor"]["ordinal"] = 1 << 1_000
    with pytest.raises(ValueError, match="integer bit ceiling"):
        horizon.build_historical_horizon_plan(huge_integer)

    deep: object = "leaf"
    for _ in range(55):
        deep = [deep]
    nested = _request()
    nested["unexpected"] = deep
    with pytest.raises(ValueError, match="depth"):
        horizon.build_historical_horizon_plan(nested)

    huge = _request()
    huge["run_id"] = "x" * 1_000_001
    with pytest.raises(ValueError, match="string ceiling"):
        horizon.build_historical_horizon_plan(huge)

    injected = _request()
    injected["unexpected"] = lambda: None
    with pytest.raises(ValueError, match="exact JSON"):
        horizon.build_historical_horizon_plan(injected)


def test_expected_evaluation_product_is_bounded_before_materialization() -> None:
    request = _request()
    first = request["candidates"][0]
    second = request["candidates"][1]
    boundary = request["candidates"][2]
    aliases: list[dict[str, object]] = []
    for index in range(48):
        alias = copy.deepcopy(first)
        alias["candidate_id"] = f"OBS-ALIAS-{index:02d}"
        alias["lookback_kind"] = "ACTIVE_SESSIONS"
        alias["lookback_value"] = 60_000 + index
        alias["candidate_definition_sha256"] = _hash(
            {
                key: value
                for key, value in alias.items()
                if key not in ("candidate_definition_sha256", "folds")
            }
        )
        aliases.append(alias)
    request["candidates"] = [first, second, *aliases, boundary]
    request["trials"] = [
        _trial(
            f"trial-{index:04d}",
            f"strategy-{index:04d}",
            "1" * 64,
            _hash({"parameter_index": index}),
        )
        for index in range(1_000)
    ]
    with pytest.raises(ValueError, match="expected evaluation ceiling"):
        horizon.build_historical_horizon_plan(request)


def test_raw_evidence_requires_aligned_finite_canonical_vectors(
    valid_plan: object,
    valid_shards: list[dict[str, object]],
) -> None:
    shards = copy.deepcopy(valid_shards)
    shard = next(item for item in shards if item["status"] == "AUTHENTICATED")
    shard["daily_net_mtm_r"][0] = "1e999999999"
    with pytest.raises(ValueError, match="non-canonical|invalid"):
        horizon.evaluate_historical_horizon_plan(valid_plan, shards)


def test_daily_vectors_cannot_blur_partial_utc_days() -> None:
    request = _request()
    for candidate in request["candidates"][:2]:
        fold = candidate["folds"][0]
        fold["validation_start_utc"] = "2019-04-01T00:30:00Z"
        fold["fold_definition_sha256"] = _hash(
            {
                key: value
                for key, value in fold.items()
                if key != "fold_definition_sha256"
            }
        )
    plan = horizon.build_historical_horizon_plan(request)
    with pytest.raises(ValueError, match="UTC-midnight"):
        horizon.evaluate_historical_horizon_plan(plan, _shards(plan))


@pytest.mark.parametrize(
    "target_name",
    [
        "_normalize_request",
        "_evidence_worker",
        "_build_plan_body",
        "_digest",
    ],
)
def test_runtime_helper_rebinding_fails_closed(
    monkeypatch: pytest.MonkeyPatch, target_name: str
) -> None:
    captured = horizon.build_historical_horizon_plan
    monkeypatch.setattr(horizon, target_name, lambda *args, **kwargs: None)
    with pytest.raises(ValueError, match="runtime authority rebound"):
        captured(_request())


def test_public_rebinding_and_resource_constant_rebinding_fail_closed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured = horizon.build_historical_horizon_plan
    monkeypatch.setattr(
        horizon, "build_historical_horizon_plan", lambda request: request
    )
    with pytest.raises(ValueError, match="public authority rebound"):
        captured(_request())
    monkeypatch.undo()

    captured = horizon.build_historical_horizon_plan
    monkeypatch.setattr(horizon, "_MAX_VECTOR_POINTS", 99_999_999)
    with pytest.raises(ValueError, match="runtime constant changed"):
        captured(_request())


def test_default_constant_seal_and_class_rebinding_attacks_fail_closed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured = horizon.build_historical_horizon_plan
    monkeypatch.setattr(captured, "__defaults__", (_request(),))
    with pytest.raises(ValueError, match="public authority defaults changed"):
        captured()
    monkeypatch.undo()

    captured = horizon.build_historical_horizon_plan
    monkeypatch.setattr(horizon, "_TECHNICAL_REASONS", frozenset({"ANYTHING"}))
    with pytest.raises(ValueError, match="runtime constant changed"):
        captured(_request())
    monkeypatch.undo()

    captured = horizon.build_historical_horizon_plan
    monkeypatch.setattr(horizon, "_PLAN_SEAL", object())
    with pytest.raises(ValueError, match="runtime identity rebound"):
        captured(_request())
    monkeypatch.undo()

    captured = horizon.build_historical_horizon_plan
    monkeypatch.setattr(horizon._HistoricalHorizonPlan, "to_dict", lambda self: {})
    with pytest.raises(ValueError, match="sealed class rebound"):
        captured(_request())


def test_plan_tampering_and_cross_plan_evidence_fail_verification(
    valid_evidence: object,
) -> None:
    plan = horizon.build_historical_horizon_plan(_request())
    object.__setattr__(plan, "_body_json", b"{}")
    assert horizon.verify_historical_horizon_evidence(plan, valid_evidence) is False

    other_request = _request()
    other_request["run_id"] = "anchor-run-002"
    other_plan = horizon.build_historical_horizon_plan(other_request)
    assert (
        horizon.verify_historical_horizon_evidence(other_plan, valid_evidence) is False
    )


@pytest.mark.parametrize(
    ("path", "replacement", "message"),
    [
        (("native_prefix", "symbol"), "EURUSD", "symbol mismatch"),
        (("native_prefix", "timeframe"), "M5", "timeframe mismatch"),
        (("native_prefix", "activation_entry_sha256"), "bad", "activation entry"),
        (("native_prefix", "continuity_receipt_sha256"), "bad", "continuity receipt"),
    ],
)
def test_source_identity_cross_bindings_fail_closed(
    path: tuple[str, str],
    replacement: str,
    message: str,
) -> None:
    request = _request()
    request[path[0]][path[1]] = replacement
    with pytest.raises(ValueError, match=message):
        horizon.build_historical_horizon_plan(request)


def test_full_prefix_geometry_is_bound_against_cross_plan_shard_reuse(
    valid_plan: object,
    valid_shards: list[dict[str, object]],
) -> None:
    changed = _request()
    changed["native_prefix"]["boundary_status"] = "RIGHT_TRUNCATED"
    changed_plan = horizon.build_historical_horizon_plan(changed)
    assert (
        changed_plan.to_dict()["source_prefix_receipt_sha256"]
        != valid_plan.to_dict()["source_prefix_receipt_sha256"]
    )
    assert (
        changed_plan.to_dict()["attempt_roster_sha256"]
        != valid_plan.to_dict()["attempt_roster_sha256"]
    )
    with pytest.raises(ValueError, match="attempt|context|source prefix"):
        horizon.evaluate_historical_horizon_plan(
            changed_plan, copy.deepcopy(valid_shards)
        )


def test_terminal_source_cannot_hide_an_experiment_roster() -> None:
    request = _terminal_source_request("SOURCE_UNAVAILABLE")
    request["trials"] = [_trial("hidden-trial", "hidden-strategy", "1" * 64, "2" * 64)]
    with pytest.raises(ValueError, match="cannot carry experiment roster"):
        horizon.build_historical_horizon_plan(request)


def test_plan_objects_are_immutable(valid_plan: object, valid_evidence: object) -> None:
    with pytest.raises(AttributeError, match="sealed"):
        valid_plan._receipt_sha256 = "0" * 64
    with pytest.raises(AttributeError, match="sealed"):
        valid_evidence._receipt_sha256 = "0" * 64
