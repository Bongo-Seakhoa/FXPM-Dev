from __future__ import annotations

from copy import deepcopy
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
import hashlib
import inspect
import json
import math
import random
import tracemalloc
from typing import Any

import pytest

import pm_winner_selection as winner


def _canonical(value: Any) -> bytes:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("ascii")


def _sha_json(value: Any) -> str:
    return hashlib.sha256(_canonical(value)).hexdigest()


def _sha(label: str) -> str:
    return hashlib.sha256(label.encode("utf-8")).hexdigest()


def _sha1(label: str) -> str:
    return hashlib.sha1(label.encode("utf-8")).hexdigest()


def _iso(moment: datetime) -> str:
    return (
        moment.astimezone(timezone.utc)
        .isoformat(timespec="seconds")
        .replace("+00:00", "Z")
    )


def _attempts() -> list[dict[str, Any]]:
    definitions = [
        ("complex-alpha", "trend", 7),
        ("simple-beta", "trend", 3),
        ("losing-gamma", "reversion", 2),
    ]
    return [
        {
            "ordinal": ordinal,
            "attempt_id": _sha1(f"attempt:{name}"),
            "process_candidate_id": _sha1(f"process-candidate:{name}"),
            "strategy_family": family,
            "strategy_id": name,
            "parameter_count": parameter_count,
            "precommitted_evidence_role": "EVALUATION_REQUIRED",
            "process_sha256": _sha(f"process:{name}"),
            "horizon_process_sha256": _sha(f"horizon:{name}"),
            "trial_definition_sha256": _sha(f"trial-definition:{name}"),
            "refit_recipe_sha256": _sha(f"refit-recipe:{name}"),
            "source_contract_sha256": _sha("source-contract"),
            "cost_contract_sha256": _sha("cost-contract"),
            "cost_stress_contract_sha256": _sha("cost-stress-contract"),
            "risk_unit_contract_sha256": _sha("cell-risk-unit-contract"),
        }
        for ordinal, (name, family, parameter_count) in enumerate(definitions)
    ]


def _process_registry(attempts: list[dict[str, Any]]) -> str:
    return _sha_json(
        {
            "processes": [
                {
                    "attempt_id": value["attempt_id"],
                    "process_candidate_id": value["process_candidate_id"],
                    "process_sha256": value["process_sha256"],
                    "strategy_family": value["strategy_family"],
                    "strategy_id": value["strategy_id"],
                    "precommitted_evidence_role": value["precommitted_evidence_role"],
                }
                for value in attempts
            ]
        }
    )


def _horizon_registry(attempts: list[dict[str, Any]]) -> str:
    return _sha_json(
        {
            "horizons": [
                {
                    "attempt_id": value["attempt_id"],
                    "horizon_process_sha256": value["horizon_process_sha256"],
                    "refit_recipe_sha256": value["refit_recipe_sha256"],
                }
                for value in attempts
            ]
        }
    )


def _trial_registry(attempts: list[dict[str, Any]]) -> str:
    return _sha_json(
        {
            "trials": [
                {
                    "attempt_id": value["attempt_id"],
                    "trial_definition_sha256": value["trial_definition_sha256"],
                    "parameter_count": value["parameter_count"],
                }
                for value in attempts
            ]
        }
    )


def _fold(
    candidate_index: int,
    fold_index: int,
    *,
    symbol: str = "AUDUSD",
    timeframe: str = "M5",
    regime: str = "TREND",
) -> dict[str, Any]:
    start = date(2021 + fold_index, 2, 1)
    dates = [(start + timedelta(days=index)).isoformat() for index in range(63)]
    base: list[float] = []
    for index in range(63):
        common = 0.055 * math.sin((index + fold_index * 7) * 1.70)
        paired = 0.012 * math.sin((index + fold_index * 3) * 0.77)
        if candidate_index == 0:
            value = 0.105 + common + paired
        elif candidate_index == 1:
            value = 0.105 + common - paired
        else:
            value = -0.025 + common
        base.append(round(value, 12))
    stress = [round(value - 0.015, 12) for value in base]
    trade_count = 120
    trade_times = [
        _iso(
            datetime.combine(
                start,
                datetime.min.time(),
                tzinfo=timezone.utc,
            )
            + timedelta(hours=12 * index + 1)
        )
        for index in range(trade_count)
    ]
    trade_open_times = [
        _iso(datetime.fromisoformat(value.replace("Z", "+00:00")) - timedelta(hours=1))
        for value in trade_times
    ]
    provisional_trade_values = [
        round((0.35 if index % 3 else -0.2) + candidate_index * -0.01, 12)
        for index in range(trade_count - 1)
    ]
    daily_total = sum((Decimal(str(value)) for value in base), Decimal(0))
    final_trade = daily_total - sum(
        (Decimal(str(value)) for value in provisional_trade_values), Decimal(0)
    )
    trade_values = [*provisional_trade_values, float(final_trade)]
    stress_total = sum((Decimal(str(value)) for value in stress), Decimal(0))
    trade_cost = (daily_total - stress_total) / Decimal(trade_count)
    provisional_stress = [
        float(Decimal(str(value)) - trade_cost) for value in trade_values[:-1]
    ]
    final_stress = stress_total - sum(
        (Decimal(str(value)) for value in provisional_stress), Decimal(0)
    )
    trade_stress_values = [*provisional_stress, float(final_stress)]
    position_ids = [
        f"position:{candidate_index}:{fold_index}:{index:04d}"
        for index in range(trade_count)
    ]
    open_event_orders = [2 * index for index in range(trade_count)]
    close_event_orders = [2 * index + 1 for index in range(trade_count)]
    power_body = {
        "schema_version": winner.TRADE_POWER_BINDING_VERSION,
        "attempt_ledger_payload_sha256": "0" * 64,
        "attempted_trials_exact": 3,
        "train_requirement_sha256": _sha(
            f"train-power-requirement:{candidate_index}:{fold_index}"
        ),
        "train_calibration_sample_sha256": _sha(
            f"train-power-sample:{candidate_index}:{fold_index}"
        ),
        "train_partition_contract_sha256": _sha(
            f"train-power-partition:{candidate_index}:{fold_index}"
        ),
        "validation_decision_sha256": _sha(
            f"validation-power-decision:{candidate_index}:{fold_index}"
        ),
        "validation_partition_contract_sha256": _sha(
            f"validation-power-partition:{candidate_index}:{fold_index}"
        ),
        "required_effective_trades": 20,
    }
    power_binding = {**power_body, "binding_sha256": _sha_json(power_body)}
    end = start + timedelta(days=62)
    daily_hash_body = {
        "daily_dates_utc": dates,
        "daily_net_mtm_r": base,
        "daily_net_mtm_r_cost_stress": stress,
    }
    pbo_trade_hash_body = {
        "accounting_identity_kind": (
            "EXACT_DECIMAL_SUM_DAILY_NET_MTM_EQUALS_COMPLETED_TRADE_NET_MTM_"
            "IN_SAME_PRECOMMITTED_CELL_RISK_UNIT"
        ),
        "completed_trade_open_utc": trade_open_times,
        "completed_trade_close_utc": trade_times,
        "completed_trade_position_ids": position_ids,
        "completed_trade_open_event_order": open_event_orders,
        "completed_trade_close_event_order": close_event_orders,
        "completed_trade_net_mtm_r": trade_values,
        "completed_trade_net_mtm_r_cost_stress": trade_stress_values,
        "completed_trade_unit": "PRECOMMITTED_CELL_RISK_UNIT",
        "inventory_boundary_kind": "FLAT_TO_FLAT_LIQUIDATED_POST_COST",
        "position_activity_ledger_sha256": _sha(
            f"position-activity:{candidate_index}:{fold_index}"
        ),
        "risk_unit_contract_sha256": _sha("cell-risk-unit-contract"),
    }
    trade_hash_body = {
        **pbo_trade_hash_body,
        "trade_power_binding": power_binding,
    }
    end = start + timedelta(days=62)
    cell_id = winner.compute_cell_id(symbol, timeframe, regime)
    assert cell_id is not None
    shared_body = {
        "schema_version": winner.SHARED_OOS_PARTITION_VERSION,
        "cell_id": cell_id,
        "symbol": symbol,
        "timeframe": timeframe,
        "regime": regime,
        "fold_ordinal": fold_index,
        "fold_id": _sha1(f"shared-fold:{fold_index}"),
        "anchor_as_of_utc": _iso(
            datetime(2021 + fold_index, 2, 1, 0, tzinfo=timezone.utc)
        ),
        "oos_start_date_utc": start.isoformat(),
        "oos_end_date_utc": end.isoformat(),
        "native_prefix_index_sha256": _sha("native-prefix-index"),
        "native_prefix_content_sha256": _sha("native-prefix-content"),
        "source_contract_sha256": _sha("source-contract"),
        "native_source_sha256": _sha("native-source"),
        "activation_manifest_sha256": _sha("activation-manifest"),
        "source_group_decision_sha256": _sha("source-group-decision"),
        "regime_fit_decision_sha256": _sha("regime-fit-decision"),
        "execution_spec_receipt_sha256": _sha("execution-spec-receipt"),
        "cost_contract_sha256": _sha("cost-contract"),
        "cost_stress_contract_sha256": _sha("cost-stress-contract"),
        "risk_unit_contract_sha256": _sha("cell-risk-unit-contract"),
    }
    shared_partition = {**shared_body, "receipt_sha256": _sha_json(shared_body)}
    return {
        "fold_ordinal": fold_index,
        "fold_id": _sha1(f"shared-fold:{fold_index}"),
        "anchor_as_of_utc": _iso(
            datetime(2021 + fold_index, 2, 1, 0, tzinfo=timezone.utc)
        ),
        "oos_start_date_utc": start.isoformat(),
        "oos_end_date_utc": end.isoformat(),
        "anchor_selection_receipt_sha256": _sha(
            f"selection:{candidate_index}:{fold_index}"
        ),
        "anchor_horizon_decision_sha256": _sha(
            f"horizon-decision:{candidate_index}:{fold_index}"
        ),
        "anchor_trial_ledger_sha256": _sha(
            f"anchor-trials:{candidate_index}:{fold_index}"
        ),
        "anchor_selected_trial_sha256": _sha(
            f"anchor-selected-trial:{candidate_index}:{fold_index}"
        ),
        "anchor_refit_receipt_sha256": _sha(
            f"anchor-refit:{candidate_index}:{fold_index}"
        ),
        "attempt_replay_receipt_sha256": _sha(
            f"attempt-replay:{candidate_index}:{fold_index}"
        ),
        "attempt_oos_source_receipt_sha256": _sha(
            f"oos-source:{candidate_index}:{fold_index}"
        ),
        "shared_oos_partition": shared_partition,
        "position_activity_ledger_sha256": trade_hash_body[
            "position_activity_ledger_sha256"
        ],
        "cost_contract_sha256": _sha("cost-contract"),
        "cost_stress_contract_sha256": _sha("cost-stress-contract"),
        "risk_unit_contract_sha256": _sha("cell-risk-unit-contract"),
        "oos_evidence_accessed_at_utc": "2025-01-01T00:00:00Z",
        "fold_evaluated_at_utc": "2025-01-01T00:00:00Z",
        "daily_axis_kind": "COMPLETE_UTC_CALENDAR_DAY_AXIS",
        "daily_evidence_kind": "POST_COST_NET_MTM",
        "daily_unit": "PRECOMMITTED_CELL_RISK_UNIT",
        "completed_trade_unit": "PRECOMMITTED_CELL_RISK_UNIT",
        "accounting_identity_kind": trade_hash_body["accounting_identity_kind"],
        "inventory_boundary_kind": trade_hash_body["inventory_boundary_kind"],
        **daily_hash_body,
        "daily_vector_sha256": _sha_json(daily_hash_body),
        **trade_hash_body,
        "pbo_trade_vector_sha256": _sha_json(pbo_trade_hash_body),
        "trade_vector_sha256": _sha_json(trade_hash_body),
    }


def _pbo_projection(fold: dict[str, Any]) -> dict[str, Any]:
    return json.loads(
        json.dumps(
            {key: fold[key] for key in winner._PBO_FOLD_KEYS},
            allow_nan=False,
            separators=(",", ":"),
            sort_keys=True,
        )
    )


def _sync_entry_pbo(entry: dict[str, Any]) -> None:
    if entry["evidence_status"] != "AUTHENTICATED":
        return
    for fold in entry["folds"]:
        shared = dict(fold["shared_oos_partition"])
        for field in (
            "fold_ordinal",
            "fold_id",
            "anchor_as_of_utc",
            "oos_start_date_utc",
            "oos_end_date_utc",
        ):
            shared[field] = fold[field]
        shared_body = {
            key: value for key, value in shared.items() if key != "receipt_sha256"
        }
        fold["shared_oos_partition"] = {
            **shared_body,
            "receipt_sha256": _sha_json(shared_body),
        }
    entry["pbo_evidence_source"] = "COMPLETE_EVALUATION_REQUIRED_FOLDS"
    entry["pbo_folds"] = [_pbo_projection(fold) for fold in entry["folds"]]
    entry["pbo_fold_count"] = len(entry["pbo_folds"])
    entry["pbo_folds_sha256"] = _sha_json(entry["pbo_folds"])


def _align_rejected_pbo_axes(entry: dict[str, Any], reference: dict[str, Any]) -> None:
    for target, source in zip(entry["pbo_folds"], reference["pbo_folds"], strict=True):
        old_count = len(target["daily_dates_utc"])
        new_count = len(source["daily_dates_utc"])
        if new_count < old_count:
            raise AssertionError("test helper cannot truncate genuine PBO evidence")
        for field in (
            "fold_ordinal",
            "fold_id",
            "anchor_as_of_utc",
            "oos_start_date_utc",
            "oos_end_date_utc",
            "shared_oos_partition",
            "oos_evidence_accessed_at_utc",
            "fold_evaluated_at_utc",
        ):
            target[field] = json.loads(json.dumps(source[field]))
        target["daily_dates_utc"] = list(source["daily_dates_utc"])
        target["daily_net_mtm_r"] = [
            *target["daily_net_mtm_r"],
            *([0.0] * (new_count - old_count)),
        ]
        target["daily_net_mtm_r_cost_stress"] = [
            *target["daily_net_mtm_r_cost_stress"],
            *([0.0] * (new_count - old_count)),
        ]
        target["daily_vector_sha256"] = _sha_json(
            {
                "daily_dates_utc": target["daily_dates_utc"],
                "daily_net_mtm_r": target["daily_net_mtm_r"],
                "daily_net_mtm_r_cost_stress": target["daily_net_mtm_r_cost_stress"],
            }
        )
    entry["pbo_fold_count"] = len(entry["pbo_folds"])
    entry["pbo_folds_sha256"] = _sha_json(entry["pbo_folds"])


def _refresh_fold_accounting(fold: dict[str, Any]) -> None:
    base = fold["daily_net_mtm_r"]
    stress = fold["daily_net_mtm_r_cost_stress"]
    fold["daily_vector_sha256"] = _sha_json(
        {
            "daily_dates_utc": fold["daily_dates_utc"],
            "daily_net_mtm_r": base,
            "daily_net_mtm_r_cost_stress": stress,
        }
    )
    count = len(fold["completed_trade_close_utc"])
    base_total = sum((Decimal(str(value)) for value in base), Decimal(0))
    stress_total = sum((Decimal(str(value)) for value in stress), Decimal(0))
    offsets = [
        Decimal("0.01") if index % 2 == 0 else Decimal("-0.01")
        for index in range(count - 1)
    ]
    base_values = [
        (base_total / Decimal(count) + offset).quantize(Decimal("0.000000000001"))
        for offset in offsets
    ]
    base_values.append(base_total - sum(base_values, Decimal(0)))
    stress_values = [
        (stress_total / Decimal(count) + offset).quantize(Decimal("0.000000000001"))
        for offset in offsets
    ]
    stress_values.append(stress_total - sum(stress_values, Decimal(0)))
    fold["completed_trade_net_mtm_r"] = [float(value) for value in base_values]
    fold["completed_trade_net_mtm_r_cost_stress"] = [
        float(value) for value in stress_values
    ]
    _rehash_trade_vector(fold)


def _rehash_trade_vector(fold: dict[str, Any]) -> None:
    pbo_body = {
        "accounting_identity_kind": fold["accounting_identity_kind"],
        "completed_trade_open_utc": fold["completed_trade_open_utc"],
        "completed_trade_close_utc": fold["completed_trade_close_utc"],
        "completed_trade_position_ids": fold["completed_trade_position_ids"],
        "completed_trade_open_event_order": fold["completed_trade_open_event_order"],
        "completed_trade_close_event_order": fold["completed_trade_close_event_order"],
        "completed_trade_net_mtm_r": fold["completed_trade_net_mtm_r"],
        "completed_trade_net_mtm_r_cost_stress": fold[
            "completed_trade_net_mtm_r_cost_stress"
        ],
        "completed_trade_unit": fold["completed_trade_unit"],
        "inventory_boundary_kind": fold["inventory_boundary_kind"],
        "position_activity_ledger_sha256": fold["position_activity_ledger_sha256"],
        "risk_unit_contract_sha256": fold["risk_unit_contract_sha256"],
    }
    fold["pbo_trade_vector_sha256"] = _sha_json(pbo_body)
    fold["trade_vector_sha256"] = _sha_json(
        {**pbo_body, "trade_power_binding": fold["trade_power_binding"]}
    )


def _bind_trade_power(
    fold: dict[str, Any], attempt_ledger_sha256: str, attempted_trials: int
) -> None:
    body = {
        key: value
        for key, value in fold["trade_power_binding"].items()
        if key != "binding_sha256"
    }
    body["attempt_ledger_payload_sha256"] = attempt_ledger_sha256
    body["attempted_trials_exact"] = attempted_trials
    fold["trade_power_binding"] = {
        **body,
        "binding_sha256": _sha_json(body),
    }
    _rehash_trade_vector(fold)


def _base_payloads(
    symbol: str = "AUDUSD",
    timeframe: str = "M5",
    regime: str = "TREND",
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    attempts = _attempts()
    run_id = _sha(f"run:{symbol}:{timeframe}:{regime}")
    cell_id = winner.compute_cell_id(symbol, timeframe, regime)
    assert cell_id is not None
    process_registry = _process_registry(attempts)
    horizon_registry = _horizon_registry(attempts)
    trial_registry = _trial_registry(attempts)
    common = {
        "cell_id": cell_id,
        "symbol": symbol,
        "timeframe": timeframe,
        "regime": regime,
        "source_contract_sha256": _sha("source-contract"),
        "cost_contract_sha256": _sha("cost-contract"),
        "cost_stress_contract_sha256": _sha("cost-stress-contract"),
        "risk_unit_contract_sha256": _sha("cell-risk-unit-contract"),
        "process_registry_sha256": process_registry,
        "horizon_registry_sha256": horizon_registry,
        "trial_registry_sha256": trial_registry,
    }
    policy = {
        "contract_version": winner.PRECOMMITMENT_VERSION,
        "run_id": run_id,
        "precommitted_at_utc": "2020-01-01T00:00:00Z",
        **common,
        "selection_policy_sha256": winner.PINNED_SELECTION_POLICY_SHA256,
        "planned_attempt_count": len(attempts),
    }
    entries: list[dict[str, Any]] = []
    for ordinal, attempt in enumerate(attempts):
        folds = [
            _fold(
                ordinal,
                fold_index,
                symbol=symbol,
                timeframe=timeframe,
                regime=regime,
            )
            for fold_index in range(4)
        ]
        pbo_folds = [_pbo_projection(fold) for fold in folds]
        entries.append(
            {
                "ordinal": ordinal,
                "attempt_id": attempt["attempt_id"],
                "process_candidate_id": attempt["process_candidate_id"],
                "process_sha256": attempt["process_sha256"],
                "horizon_process_sha256": attempt["horizon_process_sha256"],
                "trial_definition_sha256": attempt["trial_definition_sha256"],
                "refit_recipe_sha256": attempt["refit_recipe_sha256"],
                "source_contract_sha256": attempt["source_contract_sha256"],
                "cost_contract_sha256": attempt["cost_contract_sha256"],
                "cost_stress_contract_sha256": attempt["cost_stress_contract_sha256"],
                "risk_unit_contract_sha256": attempt["risk_unit_contract_sha256"],
                "evidence_status": "AUTHENTICATED",
                "status_reason": None,
                "fold_count": len(folds),
                "folds_sha256": _sha_json(folds),
                "folds": folds,
                "pbo_evidence_source": "COMPLETE_EVALUATION_REQUIRED_FOLDS",
                "pbo_fold_count": len(pbo_folds),
                "pbo_folds_sha256": _sha_json(pbo_folds),
                "pbo_folds": pbo_folds,
            }
        )
    fold_plan = [
        {
            field: entries[0]["folds"][fold_index][field]
            for field in (
                "fold_ordinal",
                "fold_id",
                "anchor_as_of_utc",
                "oos_start_date_utc",
                "oos_end_date_utc",
            )
        }
        for fold_index in range(len(entries[0]["folds"]))
    ]
    latest_boundary = datetime.combine(
        date.fromisoformat(fold_plan[-1]["oos_end_date_utc"]) + timedelta(days=1),
        datetime.min.time(),
        tzinfo=timezone.utc,
    )
    policy.update(
        {
            "planned_fold_count": len(fold_plan),
            "causal_fold_plan_sha256": _sha_json(fold_plan),
            "causal_fold_plan": fold_plan,
            "latest_evidence_boundary_utc": _iso(latest_boundary),
        }
    )
    policy_sha = _sha_json(policy)
    attempt_ledger = {
        "contract_version": winner.ATTEMPT_LEDGER_VERSION,
        "run_id": run_id,
        "frozen_at_utc": "2020-01-02T00:00:00Z",
        "precommitment_payload_sha256": policy_sha,
        **common,
        "attempt_count": len(attempts),
        "attempts_sha256": _sha_json(attempts),
        "attempts": attempts,
    }
    attempt_sha = _sha_json(attempt_ledger)
    for entry in entries:
        for fold in entry["folds"]:
            _bind_trade_power(fold, attempt_sha, len(attempts))
        entry["pbo_folds"] = [_pbo_projection(fold) for fold in entry["folds"]]
        entry["pbo_fold_count"] = len(entry["pbo_folds"])
        entry["pbo_folds_sha256"] = _sha_json(entry["pbo_folds"])
        entry["folds_sha256"] = _sha_json(entry["folds"])
    evidence = {
        "contract_version": winner.EVIDENCE_LEDGER_VERSION,
        "run_id": run_id,
        "generated_at_utc": "2025-01-01T00:00:00Z",
        "evidence_access_started_at_utc": "2025-01-01T00:00:00Z",
        "evidence_access_completed_at_utc": "2025-01-01T00:00:00Z",
        "latest_evidence_boundary_utc": _iso(latest_boundary),
        "precommitment_payload_sha256": policy_sha,
        "attempt_ledger_payload_sha256": attempt_sha,
        **common,
        "attempt_count": len(attempts),
        "entries_sha256": _sha_json(entries),
        "entries": entries,
        "evidence_scope": "PROCESS_LEVEL_ONLY",
        "parameter_evidence_inherited": False,
    }
    return policy, attempt_ledger, evidence


def _payload_request(
    symbol: str = "AUDUSD",
    timeframe: str = "M5",
    regime: str = "TREND",
) -> dict[str, Any]:
    policy, attempts, evidence = _base_payloads(symbol, timeframe, regime)
    return {
        "precommitment_payload": policy,
        "attempt_ledger_payload": attempts,
        "evidence_ledger_payload": evidence,
    }


def _typed_request(request: dict[str, Any]) -> winner.WinnerSelectionRequest:
    typed = winner.create_winner_selection_request(
        request["precommitment_payload"],
        request["attempt_ledger_payload"],
        request["evidence_ledger_payload"],
    )
    assert typed is not None
    return typed


def _evaluate(request: dict[str, Any]) -> winner.WinnerSelectionDecision:
    return winner.evaluate_historical_cell_winner(_typed_request(request))


def _verify(decision: winner.WinnerSelectionDecision, request: dict[str, Any]) -> bool:
    return winner.verify_historical_cell_winner_decision(
        decision, _typed_request(request)
    )


def _marginal(
    decision: winner.WinnerSelectionDecision, request: dict[str, Any]
) -> dict[str, Any] | None:
    return winner.historical_book_marginal_receipt(decision, _typed_request(request))


def _refresh_complete_chain(request: dict[str, Any]) -> None:
    policy = request["precommitment_payload"]
    attempt_ledger = request["attempt_ledger_payload"]
    evidence = request["evidence_ledger_payload"]
    authenticated = next(
        entry
        for entry in evidence["entries"]
        if entry["evidence_status"] == "AUTHENTICATED"
    )
    fold_plan = [
        {
            field: fold[field]
            for field in (
                "fold_ordinal",
                "fold_id",
                "anchor_as_of_utc",
                "oos_start_date_utc",
                "oos_end_date_utc",
            )
        }
        for fold in authenticated["folds"]
    ]
    latest_boundary = datetime.combine(
        date.fromisoformat(fold_plan[-1]["oos_end_date_utc"]) + timedelta(days=1),
        datetime.min.time(),
        tzinfo=timezone.utc,
    )
    access_time = latest_boundary + timedelta(days=1)
    for entry in evidence["entries"]:
        for pbo_fold in entry["pbo_folds"]:
            pbo_fold["oos_evidence_accessed_at_utc"] = _iso(access_time)
            pbo_fold["fold_evaluated_at_utc"] = _iso(access_time)
        entry["pbo_folds_sha256"] = _sha_json(entry["pbo_folds"])
        if entry["evidence_status"] == "AUTHENTICATED":
            for fold in entry["folds"]:
                fold["oos_evidence_accessed_at_utc"] = _iso(access_time)
                fold["fold_evaluated_at_utc"] = _iso(access_time)
            entry["pbo_folds"] = [_pbo_projection(fold) for fold in entry["folds"]]
            entry["pbo_fold_count"] = len(entry["pbo_folds"])
            entry["pbo_folds_sha256"] = _sha_json(entry["pbo_folds"])
            entry["folds_sha256"] = _sha_json(entry["folds"])
    policy["planned_fold_count"] = len(fold_plan)
    policy["causal_fold_plan"] = fold_plan
    policy["causal_fold_plan_sha256"] = _sha_json(fold_plan)
    policy["latest_evidence_boundary_utc"] = _iso(latest_boundary)
    attempt_ledger["precommitment_payload_sha256"] = _sha_json(policy)
    attempt_sha = _sha_json(attempt_ledger)
    for entry in evidence["entries"]:
        if entry["evidence_status"] == "AUTHENTICATED":
            for fold in entry["folds"]:
                _bind_trade_power(fold, attempt_sha, len(attempt_ledger["attempts"]))
            entry["pbo_folds"] = [_pbo_projection(fold) for fold in entry["folds"]]
            entry["pbo_fold_count"] = len(entry["pbo_folds"])
            entry["pbo_folds_sha256"] = _sha_json(entry["pbo_folds"])
            entry["folds_sha256"] = _sha_json(entry["folds"])
    evidence["precommitment_payload_sha256"] = _sha_json(policy)
    evidence["attempt_ledger_payload_sha256"] = attempt_sha
    evidence["latest_evidence_boundary_utc"] = _iso(latest_boundary)
    evidence["evidence_access_started_at_utc"] = _iso(access_time)
    evidence["evidence_access_completed_at_utc"] = _iso(access_time)
    evidence["generated_at_utc"] = _iso(access_time)
    evidence["entries_sha256"] = _sha_json(evidence["entries"])


def _make_authenticated_pbo_floor(entry: dict[str, Any]) -> None:
    """Keep one genuine losing column so CSCV cannot be bypassed in unit fixtures."""

    for fold_index, fold in enumerate(entry["folds"]):
        base = [
            round(-0.20 + 0.01 * math.sin((index + fold_index * 7) * 1.70), 12)
            for index in range(63)
        ]
        fold["daily_net_mtm_r"] = base
        fold["daily_net_mtm_r_cost_stress"] = [
            round(value - 0.01, 12) for value in base
        ]
        _refresh_fold_accounting(fold)
    entry["pbo_folds"] = [_pbo_projection(fold) for fold in entry["folds"]]
    entry["pbo_fold_count"] = len(entry["pbo_folds"])
    entry["pbo_folds_sha256"] = _sha_json(entry["pbo_folds"])
    _sync_entry_pbo(entry)
    entry["folds_sha256"] = _sha_json(entry["folds"])


def test_valid_exact_evidence_uses_identity_for_unresolved_processes() -> None:
    request = _payload_request()
    typed_request = _typed_request(request)
    decision = _evaluate(request)
    assert decision.status == "PROCESS_WINNER_PENDING_BOOK_MULTIPLICITY", (
        decision.reason_codes
    )
    assert (
        decision.winner_process_candidate_id == _attempts()[0]["process_candidate_id"]
    )
    assert winner.verify_historical_cell_winner_decision(decision, typed_request)
    body = decision.to_dict()
    for summary in body["candidate_summaries"]:
        if summary["conservative_primary_utility_lcb_daily_log_growth"] is None:
            continue
        assert summary["conservative_primary_utility_lcb_daily_log_growth"] == min(
            summary["dependence_robust_lcb_daily_log_growth"],
            summary["cost_stress_dependence_robust_lcb_daily_log_growth"],
        )
        assert summary["primary_cell_risk_fraction"] == 0.0025
    assert decision.selection_scope == "OFFLINE_BOOK_SELECTION"
    assert body["selection_scope"] == winner.WINNER_SELECTION_SCOPE
    assert body["live_authorized"] is False
    assert body["capital_sizing_authorized"] is False
    assert body["profitability_claimed"] is False
    assert body["parameter_evidence_inherited"] is False
    assert body["latest_evidence_boundary_utc"] == "2024-04-04T00:00:00Z"
    assert body["evidence_access_delay_seconds"] > 0
    marginal = winner.historical_book_marginal_receipt(decision, typed_request)
    assert marginal is not None
    assert (
        marginal["p_value_contract_sha256"]
        == winner.PINNED_BOOK_P_VALUE_CONTRACT_SHA256
    )
    assert marginal["within_cell_attempted_trials_exact"] == 3
    assert marginal["effective_trials"] == 3.0
    assert marginal["within_cell_multiplicity_adjusted"] is True
    assert marginal["parameter_evidence_inherited"] is False
    assert 0.0 <= marginal["marginal_p_value"] <= 0.05
    assert marginal["upstream_evidence_receipt_sha256"] == decision.receipt_sha256
    forbidden = {
        "current_refit_candidate_id",
        "parameters_sha256",
        "upstream_refit_receipt_sha256",
        "refit_as_of_utc",
    }
    assert forbidden.isdisjoint(marginal)
    assert "current_refit_payload_sha256" not in body
    assert "authenticated_request" not in body
    assert body["request_contract_version"] == winner.WINNER_SELECTION_REQUEST_VERSION
    assert body["request_sha256"] == typed_request.request_sha256
    assert (
        body["precommitment_payload_sha256"]
        == typed_request.precommitment.payload_sha256
    )
    assert (
        body["attempt_ledger_payload_sha256"]
        == typed_request.attempt_ledger.payload_sha256
    )
    assert (
        body["evidence_ledger_payload_sha256"]
        == typed_request.evidence_ledger.payload_sha256
    )


def test_v13_pbo_and_bootstrap_are_exact_fixed_risk_admission_evidence() -> None:
    request = _payload_request()
    decision = _evaluate(request)
    body = decision.to_dict()
    pbo = body["cell_pbo_evidence"]
    attempts = request["attempt_ledger_payload"]["attempts"]
    assert pbo["contract_version"] == "fxpm.winner-cscv-pbo.v2"
    assert pbo["status"] == "PASS"
    assert pbo["passed"] is True
    assert pbo["attempted_trials_exact"] == len(attempts) == 3
    assert [record["process_candidate_id"] for record in pbo["attempt_roster"]] == [
        attempt["process_candidate_id"] for attempt in attempts
    ]
    assert pbo["evaluation_required_candidate_ids"] == sorted(
        attempt["process_candidate_id"] for attempt in attempts
    )
    assert pbo["presearch_rejected_attempts"] == []
    assert pbo["base"]["complete_combination_count"] == 70
    assert pbo["cost_stress"]["complete_combination_count"] == 70
    assert pbo["conservative_pbo"] <= 0.50
    assert pbo["evidence_sha256"] == _sha_json(
        {key: value for key, value in pbo.items() if key != "evidence_sha256"}
    )

    entries = request["evidence_ledger_payload"]["entries"]
    for summary in body["candidate_summaries"]:
        if summary["evidence_status"] != "AUTHENTICATED":
            continue
        evidence = summary["moving_block_bootstrap_evidence"]
        assert evidence["contract_version"] == ("fxpm.winner-moving-block-bootstrap.v1")
        assert evidence["daily_unit"] == ("FIXED_0.25_PERCENT_RISK_DAILY_LOG_GROWTH")
        assert evidence["attempted_trials_exact"] == 3
        assert evidence["resampled_block_draw_count"] <= (
            winner.MAX_BOOTSTRAP_BLOCK_DRAWS // 3
        )
        entry = entries[summary["ordinal"]]
        expected_base_mean = (
            math.fsum(
                math.log1p(0.0025 * float(value))
                for fold in entry["folds"]
                for value in fold["daily_net_mtm_r"]
            )
            / summary["daily_count"]
        )
        assert evidence["base"]["primary"][
            "observed_mean_daily_log_growth"
        ] == pytest.approx(expected_base_mean, abs=1e-18)
        assert evidence["base"]["primary"]["repetitions"] == 1024
        assert (
            evidence["base"]["primary"]["seed_sha256"]
            != evidence["cost_stress"]["primary"]["seed_sha256"]
        )
        assert evidence["evidence_sha256"] == _sha_json(
            {key: value for key, value in evidence.items() if key != "evidence_sha256"}
        )
    rejected = next(
        summary
        for summary in body["candidate_summaries"]
        if summary["strategy_id"] == "losing-gamma"
    )
    assert rejected["moving_block_bootstrap_evidence"]["status"] == "FAIL"
    assert (
        "MOVING_BLOCK_BOOTSTRAP_LOWER_BOUND_FAILED"
        in rejected["admissibility_failures"]
    )
    unsigned_body = {
        key: value for key, value in body.items() if key != "receipt_sha256"
    }
    assert decision.receipt_sha256 == _sha_json(unsigned_body)


def test_moving_block_bootstrap_resamples_between_fold_outcome_variation() -> None:
    # Conditioning on exactly one copy of every fold would make every bootstrap
    # mean +0.0005 and manufacture a pass. Pooling only within-fold block starts
    # preserves the no-cross-boundary rule while retaining anchor-to-anchor risk.
    folds = [
        [0.01] * 63,
        [0.01] * 63,
        [-0.009] * 63,
        [-0.009] * 63,
    ]
    evidence = winner._moving_block_bootstrap_scale(
        folds,
        process_candidate_id=_sha1("bootstrap-between-fold-variation"),
        lane="BASE",
        scale_role="PRIMARY",
        block_days=5,
    )
    assert evidence["observation_count"] == 252
    assert evidence["moving_block_start_count"] == 4 * (63 - 5 + 1)
    assert evidence["resampled_block_draw_count"] == 1024 * math.ceil(252 / 5)
    assert evidence["observed_mean_daily_log_growth"] == pytest.approx(0.0005)
    assert evidence["lower_confidence_bound_daily_log_growth"] < 0.0
    assert evidence["status"] == "FAIL"
    assert evidence["passed"] is False


def test_pbo_cannot_hide_an_evaluated_attempt_or_narrow_matrix() -> None:
    request = _payload_request()
    baseline_pbo = _evaluate(request).to_dict()["cell_pbo_evidence"]
    excluded = request["evidence_ledger_payload"]["entries"][2]
    excluded.update(
        {
            "evidence_status": "PROCESS_REJECTED",
            "status_reason": "EVALUATED_OOS_PROCESS_REJECTED",
            "fold_count": 0,
            "folds": [],
            "folds_sha256": _sha_json([]),
        }
    )
    _refresh_complete_chain(request)
    body = _evaluate(request).to_dict()
    pbo = body["cell_pbo_evidence"]
    assert body["status"] == "PROCESS_WINNER_PENDING_BOOK_MULTIPLICITY"
    assert pbo["status"] == "PASS"
    assert pbo["evaluation_required_candidate_count"] == 3
    assert pbo["authenticated_candidate_count"] == 2
    assert pbo["base"]["matrix_sha256"] == baseline_pbo["base"]["matrix_sha256"]
    assert (
        pbo["cost_stress"]["matrix_sha256"]
        == baseline_pbo["cost_stress"]["matrix_sha256"]
    )

    second = request["evidence_ledger_payload"]["entries"][1]
    second.update(
        {
            "evidence_status": "PROCESS_REJECTED",
            "status_reason": "EVALUATED_OOS_PROCESS_REJECTED",
            "fold_count": 0,
            "folds": [],
            "folds_sha256": _sha_json([]),
        }
    )
    _refresh_complete_chain(request)
    retained = _evaluate(request).to_dict()
    assert retained["cell_pbo_evidence"]["status"] == "PASS"
    assert retained["cell_pbo_evidence"]["evaluation_required_candidate_count"] == 3
    assert retained["cell_pbo_evidence"]["authenticated_candidate_count"] == 1

    excluded["pbo_folds"] = []
    excluded["pbo_fold_count"] = 0
    excluded["pbo_folds_sha256"] = _sha_json([])
    request["evidence_ledger_payload"]["entries_sha256"] = _sha_json(
        request["evidence_ledger_payload"]["entries"]
    )
    rejected = _evaluate(request)
    assert rejected.status == "REJECTED"
    assert rejected.reason_codes == (
        "EVIDENCE_0002_EVALUATION_REQUIRED_PBO_ROSTER_INCOMPLETE",
    )


def test_pair_inference_is_exactly_invariant_to_candidate_order() -> None:
    request = _payload_request()
    entries = request["evidence_ledger_payload"]["entries"]
    for fold in entries[1]["folds"]:
        start = datetime.fromisoformat(fold["anchor_as_of_utc"].replace("Z", "+00:00"))
        fold["completed_trade_open_utc"] = [
            _iso(
                max(
                    start,
                    datetime.fromisoformat(close.replace("Z", "+00:00"))
                    - timedelta(days=9),
                )
            )
            for close in fold["completed_trade_close_utc"]
        ]
    left = {"ordinal": 0}
    right = {"ordinal": 1}

    forward = winner._pair_inference(left, right, entries)
    reverse = winner._pair_inference(right, left, entries)

    assert forward["raw_one_sided_left_superior_p"] == pytest.approx(
        reverse["raw_one_sided_right_superior_p"], abs=1e-15
    )
    assert forward["raw_one_sided_right_superior_p"] == pytest.approx(
        reverse["raw_one_sided_left_superior_p"], abs=1e-15
    )
    assert forward["raw_two_sided_difference_p"] == pytest.approx(
        reverse["raw_two_sided_difference_p"], abs=1e-15
    )
    assert forward["paired_mean_difference_daily_log_growth"] == pytest.approx(
        -reverse["paired_mean_difference_daily_log_growth"], abs=1e-18
    )


def test_log_growth_not_arithmetic_r_owns_newest_fold_gate() -> None:
    request = _payload_request()
    evidence = request["evidence_ledger_payload"]
    entry = evidence["entries"][0]
    for fold_index, fold in enumerate(entry["folds"]):
        if fold_index < 3:
            base = [
                round(9.0 + 0.1 * math.sin((index + fold_index) * 1.3), 12)
                for index in range(63)
            ]
            stress = [round(value - 0.1, 12) for value in base]
        else:
            base = [49.0] * 32 + [-50.0] * 31
            stress = [48.9] * 32 + [-50.0] * 31
        fold["daily_net_mtm_r"] = base
        fold["daily_net_mtm_r_cost_stress"] = stress
        _refresh_fold_accounting(fold)
    _sync_entry_pbo(entry)
    entry["folds_sha256"] = _sha_json(entry["folds"])
    for rejected in evidence["entries"][1:]:
        rejected["evidence_status"] = "PROCESS_REJECTED"
        rejected["status_reason"] = "EVALUATED_OOS_PROCESS_REJECTED"
        rejected["fold_count"] = 0
        rejected["folds"] = []
        rejected["folds_sha256"] = _sha_json([])
    evidence["entries_sha256"] = _sha_json(evidence["entries"])

    decision = _evaluate(request)
    summary = decision.to_dict()["candidate_summaries"][0]

    assert summary["newest_fold_mean_daily_r"] > 0.0
    assert summary["newest_fold_mean_daily_log_growth"] < 0.0
    assert "NONPOSITIVE_NEWEST_FOLD" in summary["admissibility_failures"]
    assert decision.status == "NO_PROCESS_WINNER"


def test_overlapping_positions_cap_completed_trade_power() -> None:
    request = _payload_request()
    evidence = request["evidence_ledger_payload"]
    entry = evidence["entries"][0]
    for fold in entry["folds"]:
        fold["completed_trade_open_utc"] = [
            fold["anchor_as_of_utc"] for _ in fold["completed_trade_open_utc"]
        ]
        _rehash_trade_vector(fold)
    _sync_entry_pbo(entry)
    entry["folds_sha256"] = _sha_json(entry["folds"])
    for rejected in evidence["entries"][1:]:
        rejected["evidence_status"] = "PROCESS_REJECTED"
        rejected["status_reason"] = "EVALUATED_OOS_PROCESS_REJECTED"
        rejected["fold_count"] = 0
        rejected["folds"] = []
        rejected["folds_sha256"] = _sha_json([])
    evidence["entries_sha256"] = _sha_json(evidence["entries"])

    decision = _evaluate(request)
    summary = decision.to_dict()["candidate_summaries"][0]

    assert summary["completed_trade_count"] == 480
    assert summary["completed_trade_overlap_component_count"] == 4
    assert summary["completed_trade_hac_effective_count"] <= 4.0
    assert "INSUFFICIENT_COMPLETED_TRADE_HAC_POWER" in summary["admissibility_failures"]


def test_declared_parameter_count_cannot_flip_unresolved_winner() -> None:
    request = _payload_request()
    attempt_ledger = request["attempt_ledger_payload"]
    attempt_ledger["attempts"][1]["parameter_count"] = 0
    attempt_ledger["attempts_sha256"] = _sha_json(attempt_ledger["attempts"])
    trial_registry = _trial_registry(attempt_ledger["attempts"])
    for payload in (
        request["precommitment_payload"],
        attempt_ledger,
        request["evidence_ledger_payload"],
    ):
        payload["trial_registry_sha256"] = trial_registry
    _refresh_complete_chain(request)

    decision = _evaluate(request)

    assert decision.status == "PROCESS_WINNER_PENDING_BOOK_MULTIPLICITY"
    assert (
        decision.winner_process_candidate_id == _attempts()[0]["process_candidate_id"]
    )


def test_trade_power_binding_must_match_exact_attempt_ledger() -> None:
    request = _payload_request()
    evidence = request["evidence_ledger_payload"]
    entry = evidence["entries"][0]
    fold = entry["folds"][0]
    body = {
        key: value
        for key, value in fold["trade_power_binding"].items()
        if key != "binding_sha256"
    }
    body["attempted_trials_exact"] = 2
    fold["trade_power_binding"] = {
        **body,
        "binding_sha256": _sha_json(body),
    }
    _rehash_trade_vector(fold)
    _sync_entry_pbo(entry)
    entry["folds_sha256"] = _sha_json(entry["folds"])
    evidence["entries_sha256"] = _sha_json(evidence["entries"])

    decision = _evaluate(request)

    assert decision.status == "REJECTED"
    assert "TRADE_POWER_BINDING_INVALID" in decision.reason_codes[0]


def test_same_timestamp_position_events_are_causally_ordered() -> None:
    request = _payload_request()
    evidence = request["evidence_ledger_payload"]
    entry = evidence["entries"][0]
    fold = entry["folds"][0]
    fold["completed_trade_open_utc"][0] = fold["completed_trade_close_utc"][0]
    _rehash_trade_vector(fold)
    _sync_entry_pbo(entry)
    entry["folds_sha256"] = _sha_json(entry["folds"])
    evidence["entries_sha256"] = _sha_json(evidence["entries"])

    decision = _evaluate(request)

    assert decision.status == "PROCESS_WINNER_PENDING_BOOK_MULTIPLICITY"


def test_trade_record_permutation_is_not_valid_evidence_order() -> None:
    request = _payload_request()
    evidence = request["evidence_ledger_payload"]
    entry = evidence["entries"][0]
    fold = entry["folds"][0]
    for field in (
        "completed_trade_open_utc",
        "completed_trade_close_utc",
        "completed_trade_position_ids",
        "completed_trade_open_event_order",
        "completed_trade_close_event_order",
        "completed_trade_net_mtm_r",
        "completed_trade_net_mtm_r_cost_stress",
    ):
        fold[field][0], fold[field][1] = fold[field][1], fold[field][0]
    _rehash_trade_vector(fold)
    entry["folds_sha256"] = _sha_json(entry["folds"])
    evidence["entries_sha256"] = _sha_json(evidence["entries"])

    decision = _evaluate(request)

    assert decision.status == "REJECTED"
    assert "TRADE_TIME_ORDER_INVALID" in decision.reason_codes[0]


def test_arbitration_generation_roster_inputs_are_stable_pre_book_identities() -> None:
    request = _payload_request()
    decision = _evaluate(request)
    body = decision.to_dict()
    selected = body["winner"]
    roster_inputs = {
        "symbol": body["cell"]["symbol"],
        "timeframe": body["cell"]["timeframe"],
        "regime": body["cell"]["regime"],
        "strategy": selected["strategy_id"],
        "winner_process_candidate_id": selected["process_candidate_id"],
        "winner_process_sha256": selected["process_sha256"],
        "winner_decision_sha256": decision.receipt_sha256,
    }
    assert roster_inputs == {
        "symbol": "AUDUSD",
        "timeframe": "M5",
        "regime": "TREND",
        "strategy": "complex-alpha",
        "winner_process_candidate_id": _attempts()[0]["process_candidate_id"],
        "winner_process_sha256": _attempts()[0]["process_sha256"],
        "winner_decision_sha256": decision.receipt_sha256,
    }
    assert all(roster_inputs.values())


def test_per_fold_hac_lcbs_are_diagnostics_not_hidden_admission_gates() -> None:
    """A real moderate edge must not pass four extra significance tests.

    Four 63-day folds with true mean 0.02R/day and sinusoidal 0.15R noise
    clear the frozen overall attempt-aware DSR gate, every base/stress fold
    mean is positive, but at least one individual fold HAC LCB is negative.
    The old undeclared conjunction rejected this otherwise admissible process.
    """

    request = _payload_request()
    evidence = request["evidence_ledger_payload"]
    entries = evidence["entries"]
    for fold_index, fold in enumerate(entries[0]["folds"]):
        base = [
            round(
                0.02 + 0.15 * math.sin((index + fold_index * 7) * 1.70),
                12,
            )
            for index in range(63)
        ]
        stress = [round(value - 0.002, 12) for value in base]
        fold["daily_net_mtm_r"] = base
        fold["daily_net_mtm_r_cost_stress"] = stress
        _refresh_fold_accounting(fold)
    _sync_entry_pbo(entries[0])
    entries[0]["folds_sha256"] = _sha_json(entries[0]["folds"])
    _make_authenticated_pbo_floor(entries[1])
    for entry in entries[2:]:
        entry["evidence_status"] = "PROCESS_REJECTED"
        entry["status_reason"] = "EVALUATED_OOS_PROCESS_REJECTED"
        entry["fold_count"] = 0
        entry["folds"] = []
        entry["folds_sha256"] = _sha_json([])
    evidence["entries_sha256"] = _sha_json(entries)

    decision = _evaluate(request)
    assert decision.status == "PROCESS_WINNER_PENDING_BOOK_MULTIPLICITY", (
        decision.reason_codes
    )
    summary = decision.to_dict()["candidate_summaries"][0]
    assert summary["admissible"] is True
    assert summary["within_cell_adjusted_p_value"] <= 0.05
    assert summary["attempt_aware_dsr_p_value"] == pytest.approx(
        max(
            summary["primary_hac_dsr_p_value"],
            summary["sensitivity_hac_dsr_p_value"],
            summary["cost_stress_dependence_p_value"],
        )
    )
    assert summary["precommitted_block_student_t_p_value"] not in (
        summary["primary_hac_dsr_p_value"],
        summary["sensitivity_hac_dsr_p_value"],
    )
    assert summary["worst_fold_mean_daily_r"] > 0.0
    assert summary["newest_fold_mean_daily_r"] > 0.0
    assert summary["cost_stress_worst_fold_mean_daily_r"] > 0.0
    assert summary["cost_stress_newest_fold_mean_daily_r"] > 0.0
    assert summary["worst_fold_hac_lcb_daily_r"] < 0.0
    assert summary["cost_stress_worst_fold_hac_lcb_daily_r"] < 0.0
    assert not any("HAC_LCB" in code for code in summary["admissibility_failures"])


def test_fold_lcb_false_negative_power_regression_is_pinned() -> None:
    """Document the false-negative rate avoided by the V5 selector."""

    def normal_cdf(value: float) -> float:
        return 0.5 * (1.0 + math.erf(value / math.sqrt(2.0)))

    def simultaneous_power(annual_sharpe: float, fold_days: int) -> float:
        per_fold = normal_cdf(
            annual_sharpe * math.sqrt(fold_days / 252.0) - 1.6448536269514722
        )
        return per_fold**4

    assert simultaneous_power(1.0, 63) == pytest.approx(0.000253, abs=1e-6)
    assert simultaneous_power(2.0, 63) == pytest.approx(0.004535, abs=1e-6)
    assert simultaneous_power(1.0, 252) == pytest.approx(0.004535, abs=1e-6)
    assert simultaneous_power(2.0, 252) == pytest.approx(0.166476, abs=1e-6)

    policy = winner.pinned_policy_snapshot()
    assert policy["per_fold_hac_lcb_gate"] is False
    assert set(policy["diagnostic_only_fields"]) == {
        "worst_fold_hac_lcb_daily_r",
        "newest_fold_hac_lcb_daily_r",
        "cost_stress_worst_fold_hac_lcb_daily_r",
        "cost_stress_newest_fold_hac_lcb_daily_r",
        "tail_cvar_daily_r",
        "max_path_drawdown_r",
        "cost_stress_tail_cvar_daily_r",
        "cost_stress_max_path_drawdown_r",
    }


def test_v13_primary_utility_and_inference_policy_are_exact() -> None:
    policy = winner.pinned_policy_snapshot()
    assert policy["policy_version"] == "fxpm.winner-statistical-policy.v10"
    assert policy["trade_power_minimum_economic_edge_r"] == 0.25
    assert policy["trade_power_maximum_ci_half_width_r"] == 0.25
    assert policy["primary_utility"] == "MEAN_DAILY_LOG_GROWTH_AT_FIXED_CELL_RISK"
    assert policy["primary_cell_risk_fraction"] == 0.0025
    assert policy["primary_transform"] == "LOG1P(0.0025_TIMES_DAILY_NET_MTM_R)"
    assert policy["primary_lcb_confidence"] == 0.95
    assert policy["selection_dominance_fields"] == [
        "conservative_primary_utility_lcb_daily_log_growth"
    ]
    assert policy["attempt_aware_dsr_reapplications_per_block_scale"] == 0
    assert policy["least_favorable_dependence_p_value_binds"] is False
    assert policy["block_student_t_p_value_role"] == "DIAGNOSTIC_ONLY_NON_BINDING"
    assert policy["block_lcb_participates_in_candidate_score"] is True
    assert policy["pbo_contiguous_block_count"] == 8
    assert policy["pbo_complete_combinations"] == 70
    assert policy["pbo_min_evaluation_candidates"] == 2
    assert policy["pbo_max_probability"] == 0.50
    assert policy["moving_block_bootstrap_repetitions"] == 1024
    assert policy["moving_block_bootstrap_lower_tail_numerator"] == 5
    assert policy["moving_block_bootstrap_lower_tail_denominator"] == 100
    assert (
        policy["minimum_total_days_role"]
        == "STRUCTURAL_ELIGIBILITY_FLOOR_NOT_A_POWER_GUARANTEE"
    )
    assert 1.0 - policy["primary_cell_risk_fraction"] * winner.MAX_ABS_DAILY_R > 0.0
    for daily_r in (-winner.MAX_ABS_DAILY_R, -1.0, 0.0, 2.0, 50.0):
        assert winner._daily_log_growth_from_r(float(daily_r)) == pytest.approx(
            math.log1p(0.0025 * daily_r), abs=1e-18
        )
    with pytest.raises(ValueError, match="PRIMARY_UTILITY_LOG_DOMAIN_INVALID"):
        winner._daily_log_growth_from_r(-400.0)
    with pytest.raises(ValueError, match="PRIMARY_UTILITY_INPUT_INVALID"):
        winner._daily_log_growth_from_r(math.nan)
    assert winner._student_t_one_sided_critical(3, 0.95) == pytest.approx(
        2.3533634348018273, abs=1e-12
    )
    assert winner._student_t_one_sided_critical.cache_info().maxsize == 16_384


def _calibration_log_growth_folds(
    seed: int,
    days_per_fold: int,
    daily_sharpe: float,
    *,
    ar1: float = 0.0,
) -> list[list[float]]:
    generator = random.Random(seed)
    scale = 0.0025
    folds: list[list[float]] = []
    for _ in range(4):
        values: list[float] = []
        state = 0.0
        for _ in range(days_per_fold):
            if ar1:
                state = ar1 * state + generator.gauss(
                    daily_sharpe * (1.0 - ar1),
                    math.sqrt(1.0 - ar1**2),
                )
                value = state
            else:
                value = generator.gauss(daily_sharpe, 1.0)
            values.append(scale * value)
        folds.append(values)
    return folds


@pytest.mark.parametrize("seed", (1, 2))
def test_v10_formal_power_defect_counterexamples_now_pass(seed: int) -> None:
    """The V9 all-scale four-block DSR false negatives cannot recur."""

    folds = _calibration_log_growth_folds(seed, 504, 0.30)
    result_100 = winner._dependence_robust_inference(folds, None, 100)
    result_1000 = winner._dependence_robust_inference(folds, None, 1000)
    assert result_100["p"] < 0.05
    assert result_1000["p"] < 0.05
    assert result_100["p"] == max(
        result_100["primary_hac_p"], result_100["sensitivity_hac_p"]
    )
    assert result_100["precommitted_primary_block_days"] <= 14
    assert result_100["precommitted_sensitivity_block_days"] <= 28
    assert result_100["precommitted_minimum_block_count"] >= 4


def test_v10_pinned_power_calibration_grid() -> None:
    policy = winner.pinned_policy_snapshot()
    assert policy["power_calibration_version"] == ("fxpm.winner-power-calibration.v1")
    assert policy["power_calibration_seed_panel"] == "INTEGER_SEEDS_1_THROUGH_32"
    attempted_trials_seen: set[int] = set()
    horizons_seen: set[int] = set()
    effects_seen: set[float] = set()
    for (
        days_per_fold,
        daily_sharpe,
        attempted_trials,
        minimum_passes,
        maximum_passes,
    ) in policy["power_calibration_expectations"]:
        passes = sum(
            winner._dependence_robust_inference(
                _calibration_log_growth_folds(seed, days_per_fold, daily_sharpe),
                None,
                attempted_trials,
            )["p"]
            <= 0.05
            for seed in range(1, 33)
        )
        assert minimum_passes <= passes <= maximum_passes, (
            days_per_fold,
            daily_sharpe,
            attempted_trials,
            passes,
        )
        attempted_trials_seen.add(attempted_trials)
        horizons_seen.add(days_per_fold)
        effects_seen.add(daily_sharpe)
    assert attempted_trials_seen == {1, 100, 1000}
    assert horizons_seen == {63, 252, 504}
    assert effects_seen == {0.10, 0.20, 0.30}
    assert policy["high_trial_count_power_warning"] == (
        "252_TOTAL_DAYS_DOES_NOT_IMPLY_USEFUL_POWER_AT_1000_ATTEMPTS"
    )


def test_v10_null_size_panel_and_exact_zero_edge_are_controlled() -> None:
    nominal_false_positives = 0
    search_adjusted_false_positives = 0
    for seed in range(1, 33):
        folds = _calibration_log_growth_folds(seed, 504, 0.0)
        nominal_false_positives += (
            winner._dependence_robust_inference(folds, None, 1)["p"] <= 0.05
        )
        search_adjusted_false_positives += (
            winner._dependence_robust_inference(folds, None, 100)["p"] <= 0.05
        )
    assert nominal_false_positives <= 3
    assert search_adjusted_false_positives == 0

    exact_zero = [[0.0] * 504 for _ in range(4)]
    assert winner._dependence_robust_inference(exact_zero, None, 1)["p"] == 1.0
    assert winner._dependence_robust_inference(exact_zero, None, 1000)["p"] == 1.0


def test_v10_trial_penalty_is_monotone_and_block_never_recharges_it() -> None:
    folds = _calibration_log_growth_folds(20260714, 252, 0.10)
    p_values = [
        winner._dependence_robust_inference(folds, None, trials)["p"]
        for trials in (1, 10, 100, 1000)
    ]
    assert p_values == sorted(p_values)
    assert (
        "attempted_trials"
        not in inspect.signature(winner._precommitted_block_inference).parameters
    )
    assert (
        winner.pinned_policy_snapshot()[
            "attempt_aware_dsr_reapplications_per_block_scale"
        ]
        == 0
    )


def test_v10_ar1_dependence_calibration_fails_insufficient_evidence() -> None:
    policy = winner.pinned_policy_snapshot()
    assert policy["serial_dependence_calibration_expectation"] == [
        "AR1",
        0.90,
        0.10,
        4,
        252,
        100,
        "FAIL_INSUFFICIENT_INDEPENDENT_EVIDENCE",
    ]
    folds = _calibration_log_growth_folds(6, 252, 0.10, ar1=0.90)
    result = winner._dependence_robust_inference(folds, None, 100)
    assert result["p"] > 0.05
    assert result["ess"] < winner.pinned_policy_snapshot()["min_hac_effective_days"]
    assert result["maximum_detected_autocorrelation_lag_days"] >= 40


def test_risk_metrics_do_not_become_unauthenticated_numeric_admission_oracles() -> None:
    request = _payload_request()
    evidence = request["evidence_ledger_payload"]
    entries = evidence["entries"]
    for fold_index, fold in enumerate(entries[0]["folds"]):
        base = [
            round(2.0 + 0.1 * math.sin((index + fold_index * 7) * 1.70), 12)
            for index in range(63)
        ]
        if fold_index == 0:
            base[:6] = [-4.0] * 6
        elif fold_index == 1:
            for index in (5, 15, 25):
                base[index] = -4.0
        elif fold_index == 2:
            for index in (8, 28):
                base[index] = -4.0
        else:
            for index in (12, 42):
                base[index] = -4.0
        stress = [round(value - 0.01, 12) for value in base]
        fold["daily_net_mtm_r"] = base
        fold["daily_net_mtm_r_cost_stress"] = stress
        _refresh_fold_accounting(fold)
    _sync_entry_pbo(entries[0])
    entries[0]["folds_sha256"] = _sha_json(entries[0]["folds"])
    _make_authenticated_pbo_floor(entries[1])
    for entry in entries[2:]:
        entry["evidence_status"] = "PROCESS_REJECTED"
        entry["status_reason"] = "EVALUATED_OOS_PROCESS_REJECTED"
        entry["fold_count"] = 0
        entry["folds"] = []
        entry["folds_sha256"] = _sha_json([])
    evidence["entries_sha256"] = _sha_json(entries)

    decision = _evaluate(request)
    assert decision.status == "PROCESS_WINNER_PENDING_BOOK_MULTIPLICITY", (
        decision.reason_codes
    )
    summary = decision.to_dict()["candidate_summaries"][0]
    assert summary["admissible"] is True
    assert summary["tail_cvar_daily_r"] < -3.0
    assert summary["max_path_drawdown_r"] > 20.0
    assert summary["cost_stress_tail_cvar_daily_r"] < -3.0
    assert summary["cost_stress_max_path_drawdown_r"] > 20.0
    assert not any(
        "TAIL_CVAR" in code or "DRAWDOWN" in code
        for code in summary["admissibility_failures"]
    )


def test_seq809_growth_superiority_survives_one_diagnostic_drawdown_tradeoff() -> None:
    """The executable V8 counterexample must produce a V9 Winner, not an empty cell."""

    request = _payload_request()
    evidence = request["evidence_ledger_payload"]
    complex_entry = evidence["entries"][0]
    for fold in complex_entry["folds"]:
        fold["daily_net_mtm_r"] = [
            round(value + 0.04, 12) for value in fold["daily_net_mtm_r"]
        ]
        fold["daily_net_mtm_r_cost_stress"] = [
            round(value + 0.04, 12) for value in fold["daily_net_mtm_r_cost_stress"]
        ]
        # One fold-local shock recreates seq809: materially higher growth and
        # better tail mean, but a slightly worse maximum drawdown than beta.
        fold["daily_net_mtm_r"][10] = round(fold["daily_net_mtm_r"][10] - 0.15, 12)
        fold["daily_net_mtm_r_cost_stress"][10] = round(
            fold["daily_net_mtm_r_cost_stress"][10] - 0.15, 12
        )
        _refresh_fold_accounting(fold)
    _sync_entry_pbo(complex_entry)
    complex_entry["folds_sha256"] = _sha_json(complex_entry["folds"])
    evidence["entries_sha256"] = _sha_json(evidence["entries"])

    decision = _evaluate(request)
    body = decision.to_dict()
    assert decision.status == "PROCESS_WINNER_PENDING_BOOK_MULTIPLICITY"
    assert decision.reason_codes == (
        "UNIQUE_SIGNIFICANT_PRIMARY_UTILITY_NONDOMINATED_PROCESS",
    )
    assert (
        decision.winner_process_candidate_id == _attempts()[0]["process_candidate_id"]
    )
    complex_summary, simple_summary = body["candidate_summaries"][:2]
    assert complex_summary["admissible"] is True
    assert simple_summary["admissible"] is True
    assert complex_summary["mean_daily_r"] == pytest.approx(0.14258292550877777)
    assert simple_summary["mean_daily_r"] == pytest.approx(0.10479247850500793)
    assert complex_summary["tail_cvar_daily_r"] > simple_summary["tail_cvar_daily_r"]
    assert (
        complex_summary["max_path_drawdown_r"] > simple_summary["max_path_drawdown_r"]
    )
    assert (
        complex_summary["conservative_primary_utility_lcb_daily_log_growth"]
        > simple_summary["conservative_primary_utility_lcb_daily_log_growth"]
    )
    comparison = body["paired_comparisons"][0]
    assert comparison["holm_adjusted_left_superior_p"] < 0.01
    assert comparison["left_primary_utility_dominates"] is True
    assert comparison["right_primary_utility_dominates"] is False


def test_positive_overall_edge_with_negative_newest_fold_still_fails() -> None:
    """Restoring power must not erase the precommitted recency guard."""

    request = _payload_request()
    evidence = request["evidence_ledger_payload"]
    entries = evidence["entries"]
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
        stress = [round(value - 0.005, 12) for value in base]
        fold["daily_net_mtm_r"] = base
        fold["daily_net_mtm_r_cost_stress"] = stress
        _refresh_fold_accounting(fold)
    _sync_entry_pbo(entries[0])
    entries[0]["folds_sha256"] = _sha_json(entries[0]["folds"])
    for entry in entries[1:]:
        entry["evidence_status"] = "PROCESS_REJECTED"
        entry["status_reason"] = "EVALUATED_OOS_PROCESS_REJECTED"
        entry["fold_count"] = 0
        entry["folds"] = []
        entry["folds_sha256"] = _sha_json([])
    evidence["entries_sha256"] = _sha_json(entries)

    decision = _evaluate(request)
    assert decision.status == "NO_PROCESS_WINNER"
    summary = decision.to_dict()["candidate_summaries"][0]
    assert summary["mean_daily_r"] > 0.0
    assert summary["newest_fold_mean_daily_r"] < 0.0
    assert "NONPOSITIVE_NEWEST_FOLD" in summary["admissibility_failures"]
    assert "COST_STRESS_NONPOSITIVE_NEWEST_FOLD" in summary["admissibility_failures"]


def test_no_robust_positive_edge_remains_an_explicit_no_winner() -> None:
    request = _payload_request()
    evidence = request["evidence_ledger_payload"]
    entries = evidence["entries"]
    for fold_index, fold in enumerate(entries[0]["folds"]):
        base = [
            round(
                -0.02 + 0.04 * math.sin((day_index + fold_index * 7) * 1.31),
                12,
            )
            for day_index in range(63)
        ]
        fold["daily_net_mtm_r"] = base
        fold["daily_net_mtm_r_cost_stress"] = [
            round(value - 0.01, 12) for value in base
        ]
        _refresh_fold_accounting(fold)
    _sync_entry_pbo(entries[0])
    entries[0]["folds_sha256"] = _sha_json(entries[0]["folds"])
    for entry in entries[1:]:
        entry["evidence_status"] = "PROCESS_REJECTED"
        entry["status_reason"] = "EVALUATED_OOS_PROCESS_REJECTED"
        entry["fold_count"] = 0
        entry["folds"] = []
        entry["folds_sha256"] = _sha_json([])
    evidence["entries_sha256"] = _sha_json(entries)

    decision = _evaluate(request)
    assert decision.status == "NO_PROCESS_WINNER"
    assert decision.winner_process_candidate_id is None
    assert decision.reason_codes == ("NO_ADMISSIBLE_PROCESS_CANDIDATE",)
    summary = decision.to_dict()["candidate_summaries"][0]
    assert summary["admissible"] is False
    assert summary["mean_daily_r"] < 0.0
    assert "NONPOSITIVE_MEAN" in summary["admissibility_failures"]
    assert "COST_STRESS_NONPOSITIVE_MEAN" in summary["admissibility_failures"]


def test_decision_is_immutable_and_requires_exact_request() -> None:
    request = _payload_request()
    decision = _evaluate(request)
    with pytest.raises(AttributeError):
        decision._body_json = b"{}"  # type: ignore[misc]
    other_request = _payload_request(symbol="EURUSD")
    assert not _verify(decision, other_request)
    assert _marginal(decision, other_request) is None


def test_scalar_metric_fabrication_is_not_an_accepted_evidence_language() -> None:
    request = _payload_request()
    fold = request["evidence_ledger_payload"]["entries"][0]["folds"][0]
    fold["claimed_sharpe"] = 999.0
    entries = request["evidence_ledger_payload"]["entries"]
    entries[0]["folds_sha256"] = _sha_json(entries[0]["folds"])
    request["evidence_ledger_payload"]["entries_sha256"] = _sha_json(entries)

    decision = _evaluate(request)
    assert decision.status == "REJECTED"
    assert "SCHEMA_INVALID" in decision.reason_codes[0]


def test_authenticated_missing_attempt_cannot_hide_multiplicity() -> None:
    request = _payload_request()
    evidence = request["evidence_ledger_payload"]
    evidence["entries"].pop()
    evidence["entries_sha256"] = _sha_json(evidence["entries"])

    decision = _evaluate(request)
    assert decision.status == "REJECTED"
    assert decision.reason_codes == ("EVIDENCE_ATTEMPT_ROSTER_INCOMPLETE",)


def test_historical_api_rejects_current_refit_and_is_deterministic() -> None:
    request = _payload_request()
    baseline = _evaluate(request)
    replay = _evaluate(deepcopy(request))
    assert replay.receipt_sha256 == baseline.receipt_sha256
    assert replay.to_dict() == baseline.to_dict()

    with pytest.raises(TypeError):
        winner.create_winner_selection_request(
            request["precommitment_payload"],
            request["attempt_ledger_payload"],
            request["evidence_ledger_payload"],
            current_refit_payload={"attacker": "current-state-bytes"},
        )


def test_legacy_noncausal_api_names_are_deleted() -> None:
    request = _payload_request()
    historical = _evaluate(request)
    assert _verify(historical, request)
    for name in (
        "evaluate_cell_winner",
        "verify_winner_selection_decision",
        "book_marginal_receipt",
    ):
        assert not hasattr(winner, name)
        assert name not in winner.__all__


@pytest.mark.parametrize("distortion", ["unit", "risk_contract", "horizon"])
def test_authenticated_unit_or_horizon_distortion_fails_closed(distortion: str) -> None:
    request = _payload_request()
    evidence = request["evidence_ledger_payload"]
    if distortion == "unit":
        evidence["entries"][1]["folds"][0]["daily_unit"] = "PERCENT_RETURN"
        evidence["entries"][1]["folds_sha256"] = _sha_json(
            evidence["entries"][1]["folds"]
        )
    elif distortion == "risk_contract":
        evidence["entries"][1]["folds"][0]["risk_unit_contract_sha256"] = _sha(
            "substituted-risk-unit"
        )
        evidence["entries"][1]["folds_sha256"] = _sha_json(
            evidence["entries"][1]["folds"]
        )
    else:
        evidence["entries"][1]["horizon_process_sha256"] = _sha("substituted-horizon")
    evidence["entries_sha256"] = _sha_json(evidence["entries"])

    decision = _evaluate(request)
    assert decision.status == "REJECTED"
    assert any(
        token in decision.reason_codes[0]
        for token in (
            "DAILY_UNIT_INVALID",
            "RISK_UNIT_CONTRACT_MISMATCH",
            "HORIZON_PROCESS_SHA256_MISMATCH",
        )
    )


def test_payload_splicing_and_unbound_tampering_fail_closed() -> None:
    request = _payload_request()
    spliced = deepcopy(request)
    spliced["evidence_ledger_payload"] = deepcopy(spliced["attempt_ledger_payload"])
    assert _evaluate(spliced).status == "REJECTED"
    tampered = deepcopy(request)
    tampered["evidence_ledger_payload"]["entries"][0]["folds"][0]["daily_net_mtm_r"][
        0
    ] = 49.0
    assert _evaluate(tampered).status == "REJECTED"


def test_global_receipt_namespace_includes_root_request_payloads() -> None:
    request = _payload_request()
    evidence = request["evidence_ledger_payload"]
    fold = evidence["entries"][0]["folds"][0]
    fold["anchor_selection_receipt_sha256"] = _sha_json(
        request["precommitment_payload"]
    )
    evidence["entries"][0]["folds_sha256"] = _sha_json(evidence["entries"][0]["folds"])
    evidence["entries_sha256"] = _sha_json(evidence["entries"])
    decision = _evaluate(request)
    assert decision.status == "REJECTED"
    assert "REPLAYED_ACROSS_EVIDENCE_DOMAINS" in decision.reason_codes[0]


def test_exact_builtin_schema_types_and_request_binding_fail_closed() -> None:
    request = _payload_request()
    request["precommitment_payload"]["planned_attempt_count"] = True
    typed = _typed_request(request)
    decision = winner.evaluate_historical_cell_winner(typed)
    assert decision.status == "REJECTED"
    assert decision.reason_codes == ("PLANNED_ATTEMPT_COUNT_INVALID",)

    class DictionarySubclass(dict[str, Any]):
        pass

    assert (
        winner.create_winner_selection_request(
            DictionarySubclass(request["precommitment_payload"]),
            request["attempt_ledger_payload"],
            request["evidence_ledger_payload"],
        )
        is None
    )
    forged = winner.WinnerSelectionRequest(
        typed.precommitment,
        typed.attempt_ledger,
        typed.evidence_ledger,
        "0" * 64,
    )
    rejected = winner.evaluate_historical_cell_winner(forged)
    assert rejected.status == "REJECTED"
    assert rejected.reason_codes == ("REQUEST_BINDING_INVALID",)


def test_technical_failure_is_infrastructure_rejection_not_no_trade() -> None:
    request = _payload_request()
    evidence = request["evidence_ledger_payload"]
    entry = evidence["entries"][2]
    entry["evidence_status"] = "TECHNICAL_FAILURE"
    entry["status_reason"] = "EVALUATOR_TECHNICAL_FAILURE"
    entry["folds"] = []
    entry["fold_count"] = 0
    entry["folds_sha256"] = _sha_json([])
    evidence["entries_sha256"] = _sha_json(evidence["entries"])

    decision = _evaluate(request)
    assert decision.status == "REJECTED"
    assert decision.reason_codes == ("ATTEMPT_EVIDENCE_INCOMPLETE_TECHNICAL_FAILURE",)


def test_replayed_fold_receipts_cannot_inflate_nominal_fold_count() -> None:
    request = _payload_request()
    evidence = request["evidence_ledger_payload"]
    entry = evidence["entries"][0]
    source_fold = entry["folds"][0]
    replayed_fold = entry["folds"][1]
    replayed_fold["daily_net_mtm_r"] = list(source_fold["daily_net_mtm_r"])
    replayed_fold["daily_net_mtm_r_cost_stress"] = list(
        source_fold["daily_net_mtm_r_cost_stress"]
    )
    for field in (
        "anchor_selection_receipt_sha256",
        "anchor_horizon_decision_sha256",
        "anchor_trial_ledger_sha256",
        "anchor_selected_trial_sha256",
        "anchor_refit_receipt_sha256",
        "attempt_replay_receipt_sha256",
        "position_activity_ledger_sha256",
    ):
        replayed_fold[field] = source_fold[field]
    _refresh_fold_accounting(replayed_fold)
    entry["folds_sha256"] = _sha_json(entry["folds"])
    evidence["entries_sha256"] = _sha_json(evidence["entries"])

    decision = _evaluate(request)
    assert decision.status == "REJECTED"
    assert "REPLAYED" in decision.reason_codes[0]


def test_source_receipt_must_be_unique_across_nonoverlapping_fold_axes() -> None:
    request = _payload_request()
    evidence = request["evidence_ledger_payload"]
    for entry in evidence["entries"]:
        entry["folds"][1]["attempt_oos_source_receipt_sha256"] = entry["folds"][0][
            "attempt_oos_source_receipt_sha256"
        ]
        _sync_entry_pbo(entry)
        entry["folds_sha256"] = _sha_json(entry["folds"])
    evidence["entries_sha256"] = _sha_json(evidence["entries"])

    decision = _evaluate(request)
    assert decision.status == "REJECTED"
    assert "REPLAYED" in decision.reason_codes[0]


@pytest.mark.parametrize(
    "reuse_case",
    (
        "activity_equals_source",
        "attempt_equals_source",
        "anchor_equals_activity",
        "refit_reused_across_candidates",
    ),
)
def test_candidate_evidence_identities_are_single_use_across_domains(
    reuse_case: str,
) -> None:
    request = _payload_request()
    evidence = request["evidence_ledger_payload"]
    entries = evidence["entries"]
    first = entries[0]["folds"][0]
    if reuse_case == "activity_equals_source":
        first["position_activity_ledger_sha256"] = first[
            "attempt_oos_source_receipt_sha256"
        ]
        _rehash_trade_vector(first)
    elif reuse_case == "attempt_equals_source":
        first["attempt_replay_receipt_sha256"] = first[
            "attempt_oos_source_receipt_sha256"
        ]
    elif reuse_case == "anchor_equals_activity":
        first["anchor_selection_receipt_sha256"] = first[
            "position_activity_ledger_sha256"
        ]
    else:
        entries[1]["folds"][0]["anchor_refit_receipt_sha256"] = first[
            "anchor_refit_receipt_sha256"
        ]
    for entry in entries:
        _sync_entry_pbo(entry)
        entry["folds_sha256"] = _sha_json(entry["folds"])
    evidence["entries_sha256"] = _sha_json(entries)

    decision = _evaluate(request)
    assert decision.status == "REJECTED"
    assert "REPLAYED" in decision.reason_codes[0]


def test_equal_numeric_evidence_with_distinct_receipts_is_not_identity_replay() -> None:
    request = _payload_request()
    evidence = request["evidence_ledger_payload"]
    source_entry = evidence["entries"][0]
    target_entry = evidence["entries"][1]
    for source_fold, target_fold in zip(
        source_entry["folds"], target_entry["folds"], strict=True
    ):
        target_fold["daily_net_mtm_r"] = list(source_fold["daily_net_mtm_r"])
        target_fold["daily_net_mtm_r_cost_stress"] = list(
            source_fold["daily_net_mtm_r_cost_stress"]
        )
        _refresh_fold_accounting(target_fold)
    _sync_entry_pbo(target_entry)
    target_entry["folds_sha256"] = _sha_json(target_entry["folds"])
    evidence["entries_sha256"] = _sha_json(evidence["entries"])

    decision = _evaluate(request)
    assert decision.status != "REJECTED", decision.reason_codes


def test_primary_dominance_requires_base_and_cost_stress_inference() -> None:
    request = _payload_request()
    evidence = request["evidence_ledger_payload"]
    complex_entry = evidence["entries"][0]
    simple_entry = evidence["entries"][1]
    base_deltas = (0.025, 0.030, 0.035, 0.040)
    for fold_index, (complex_fold, simple_fold) in enumerate(
        zip(complex_entry["folds"], simple_entry["folds"], strict=True)
    ):
        baseline = [
            round(
                0.10
                + 0.025 * math.sin((day_index + fold_index * 5) * 1.173)
                + 0.005 * math.cos((day_index + 3) * 0.419),
                12,
            )
            for day_index in range(len(simple_fold["daily_dates_utc"]))
        ]
        simple_stress = [round(value - 0.020, 12) for value in baseline]
        complex_base = [
            round(value + base_deltas[fold_index], 12) for value in baseline
        ]
        complex_stress = [
            round(
                value + 0.0001 + 0.02 * math.sin((day_index + fold_index * 11) * 1.337),
                12,
            )
            for day_index, value in enumerate(simple_stress)
        ]
        simple_fold["daily_net_mtm_r"] = baseline
        simple_fold["daily_net_mtm_r_cost_stress"] = simple_stress
        complex_fold["daily_net_mtm_r"] = complex_base
        complex_fold["daily_net_mtm_r_cost_stress"] = complex_stress
        _refresh_fold_accounting(simple_fold)
        _refresh_fold_accounting(complex_fold)
    _sync_entry_pbo(complex_entry)
    _sync_entry_pbo(simple_entry)
    complex_entry["folds_sha256"] = _sha_json(complex_entry["folds"])
    simple_entry["folds_sha256"] = _sha_json(simple_entry["folds"])
    evidence["entries_sha256"] = _sha_json(evidence["entries"])

    decision = _evaluate(request)
    assert decision.status == "PROCESS_WINNER_PENDING_BOOK_MULTIPLICITY"
    assert (
        decision.winner_process_candidate_id == _attempts()[0]["process_candidate_id"]
    )
    body = decision.to_dict()
    complex_summary, simple_summary = body["candidate_summaries"][:2]
    assert (
        complex_summary["dependence_robust_lcb_daily_log_growth"]
        > simple_summary["dependence_robust_lcb_daily_log_growth"]
    )
    assert (
        complex_summary["conservative_primary_utility_lcb_daily_log_growth"]
        < simple_summary["conservative_primary_utility_lcb_daily_log_growth"]
    )
    comparison = body["paired_comparisons"][0]
    assert comparison["raw_base_one_sided_left_superior_p"] < 0.05
    assert comparison["raw_cost_stress_one_sided_left_superior_p"] > 0.05
    assert comparison["raw_one_sided_left_superior_p"] == max(
        comparison["raw_base_one_sided_left_superior_p"],
        comparison["raw_cost_stress_one_sided_left_superior_p"],
    )
    assert comparison["holm_adjusted_left_superior_p"] > 0.05
    assert comparison["left_primary_utility_dominates"] is False


def test_inverse_base_and_stress_superiority_cannot_manufacture_dominance() -> None:
    request = _payload_request()
    evidence = request["evidence_ledger_payload"]
    complex_entry, simple_entry = evidence["entries"][:2]
    for fold_index, (complex_fold, simple_fold) in enumerate(
        zip(complex_entry["folds"], simple_entry["folds"], strict=True)
    ):
        baseline = [
            round(
                0.10
                + 0.025 * math.sin((day_index + fold_index * 5) * 1.173)
                + 0.005 * math.cos((day_index + 3) * 0.419),
                12,
            )
            for day_index in range(63)
        ]
        simple_stress = [round(value - 0.02, 12) for value in baseline]
        base_advantage = [
            0.04 + 0.01 * math.sin((day_index + fold_index) * 0.73)
            for day_index in range(63)
        ]
        stress_disadvantage = [
            -0.04 + 0.01 * math.cos((day_index + fold_index) * 0.61)
            for day_index in range(63)
        ]
        complex_fold["daily_net_mtm_r"] = [
            round(value + delta, 12)
            for value, delta in zip(baseline, base_advantage, strict=True)
        ]
        complex_fold["daily_net_mtm_r_cost_stress"] = [
            round(value + delta, 12)
            for value, delta in zip(simple_stress, stress_disadvantage, strict=True)
        ]
        simple_fold["daily_net_mtm_r"] = baseline
        simple_fold["daily_net_mtm_r_cost_stress"] = simple_stress
        _refresh_fold_accounting(complex_fold)
        _refresh_fold_accounting(simple_fold)
    _sync_entry_pbo(complex_entry)
    _sync_entry_pbo(simple_entry)
    complex_entry["folds_sha256"] = _sha_json(complex_entry["folds"])
    simple_entry["folds_sha256"] = _sha_json(simple_entry["folds"])
    evidence["entries_sha256"] = _sha_json(evidence["entries"])

    decision = _evaluate(request)
    body = decision.to_dict()
    assert decision.status == "UNRESOLVED_NONDOMINATED_SET"
    assert decision.winner_process_candidate_id is None
    assert decision.reason_codes == (
        "STATISTICALLY_DIFFERENT_PRIMARY_UTILITY_ORDER_CONFLICT",
    )
    comparison = body["paired_comparisons"][0]
    assert comparison["raw_base_one_sided_left_superior_p"] < 0.01
    assert comparison["raw_cost_stress_one_sided_right_superior_p"] < 0.01
    assert comparison["holm_adjusted_left_superior_p"] > 0.05
    assert comparison["holm_adjusted_right_superior_p"] > 0.05
    assert comparison["holm_adjusted_two_sided_difference_p"] < 0.01
    assert comparison["left_primary_utility_dominates"] is False
    assert comparison["right_primary_utility_dominates"] is False


def test_noisy_sixty_day_memory_binds_precommitted_autocorrelation_plan() -> None:
    request = _payload_request()
    evidence = request["evidence_ledger_payload"]
    entries = evidence["entries"]
    generator = random.Random(13)
    shocks = [generator.gauss(0.1, 1.0) for _ in range(24)]
    for fold_index, fold in enumerate(entries[0]["folds"]):
        start = date(2021 + fold_index, 2, 1)
        base = [
            round(
                shocks[fold_index * 6 + day_index // 60]
                + 0.002 * math.sin((day_index + 1) * 1.371),
                12,
            )
            for day_index in range(360)
        ]
        stress = [round(value - 0.01, 12) for value in base]
        fold["daily_dates_utc"] = [
            (start + timedelta(days=index)).isoformat() for index in range(360)
        ]
        fold["oos_start_date_utc"] = start.isoformat()
        fold["oos_end_date_utc"] = (start + timedelta(days=359)).isoformat()
        fold["anchor_as_of_utc"] = _iso(
            datetime.combine(start, datetime.min.time(), tzinfo=timezone.utc)
        )
        fold["daily_net_mtm_r"] = base
        fold["daily_net_mtm_r_cost_stress"] = stress
        _refresh_fold_accounting(fold)
    _sync_entry_pbo(entries[0])
    entries[0]["folds_sha256"] = _sha_json(entries[0]["folds"])
    for entry in entries[1:]:
        entry["evidence_status"] = "PROCESS_REJECTED"
        entry["status_reason"] = "EVALUATED_OOS_PROCESS_REJECTED"
        entry["fold_count"] = 0
        entry["folds"] = []
        entry["folds_sha256"] = _sha_json([])
        _align_rejected_pbo_axes(entry, entries[0])
    evidence["entries_sha256"] = _sha_json(entries)
    _refresh_complete_chain(request)

    daily_folds = [fold["daily_net_mtm_r"] for fold in entries[0]["folds"]]
    naive = winner._fold_aware_inference(daily_folds)
    naive_p = winner._one_sided_dsr_p(
        [value for fold in daily_folds for value in fold], naive["ess"], 1
    )
    assert naive_p < 0.05

    decision = _evaluate(request)
    assert decision.status == "NO_PROCESS_WINNER"
    summary = decision.to_dict()["candidate_summaries"][0]
    assert summary["primary_hac_dsr_p_value"] > 0.05
    assert summary["sensitivity_hac_dsr_p_value"] > 0.05
    assert summary["maximum_detected_autocorrelation_lag_days"] >= 50
    assert summary["precommitted_primary_block_days"] >= 50
    assert summary["precommitted_sensitivity_block_days"] >= 100
    assert (
        summary["precommitted_minimum_block_count"]
        >= winner.pinned_policy_snapshot()["min_dependence_blocks"]
    )
    assert summary["precommitted_block_student_t_p_value"] > 0.05
    assert "WITHIN_CELL_ATTEMPT_AWARE_DSR_FAILED" in summary["admissibility_failures"]


def test_noisy_ar1_memory_cannot_evade_with_nonconstant_returns() -> None:
    request = _payload_request()
    evidence = request["evidence_ledger_payload"]
    entries = evidence["entries"]
    generator = random.Random(6)
    ar_folds: list[list[float]] = []
    for fold_index, fold in enumerate(entries[0]["folds"]):
        state = 0.0
        base: list[float] = []
        for _ in range(252):
            state = 0.9 * state + generator.gauss(
                0.02 * (1.0 - 0.9),
                0.2 * math.sqrt(1.0 - 0.9**2),
            )
            base.append(round(state, 12))
        ar_folds.append(base)
        stress = [round(value - 0.001, 12) for value in base]
        start = date(2021 + fold_index, 2, 1)
        fold["daily_dates_utc"] = [
            (start + timedelta(days=index)).isoformat() for index in range(252)
        ]
        fold["oos_start_date_utc"] = start.isoformat()
        fold["oos_end_date_utc"] = (start + timedelta(days=251)).isoformat()
        fold["anchor_as_of_utc"] = _iso(
            datetime.combine(start, datetime.min.time(), tzinfo=timezone.utc)
        )
        fold["daily_net_mtm_r"] = base
        fold["daily_net_mtm_r_cost_stress"] = stress
        _refresh_fold_accounting(fold)
    assert min(sum(values) / len(values) for values in ar_folds) > 0.0
    _sync_entry_pbo(entries[0])
    entries[0]["folds_sha256"] = _sha_json(entries[0]["folds"])
    for entry in entries[1:]:
        entry["evidence_status"] = "PROCESS_REJECTED"
        entry["status_reason"] = "EVALUATED_OOS_PROCESS_REJECTED"
        entry["fold_count"] = 0
        entry["folds"] = []
        entry["folds_sha256"] = _sha_json([])
        _align_rejected_pbo_axes(entry, entries[0])
    evidence["entries_sha256"] = _sha_json(entries)
    _refresh_complete_chain(request)

    naive = winner._fold_aware_inference(ar_folds)
    assert (
        winner._one_sided_dsr_p(
            [value for fold in ar_folds for value in fold], naive["ess"], 1
        )
        < 0.05
    )
    decision = _evaluate(request)
    assert decision.status == "NO_PROCESS_WINNER"
    summary = decision.to_dict()["candidate_summaries"][0]
    assert summary["worst_fold_mean_daily_r"] > 0.0
    assert summary["cost_stress_worst_fold_mean_daily_r"] > 0.0
    assert summary["primary_hac_dsr_p_value"] < 0.05
    assert summary["sensitivity_hac_dsr_p_value"] > 0.05
    assert summary["maximum_detected_autocorrelation_lag_days"] >= 40
    assert summary["precommitted_sensitivity_block_days"] >= 80
    assert summary["precommitted_block_student_t_p_value"] > 0.05
    assert "WITHIN_CELL_ATTEMPT_AWARE_DSR_FAILED" in summary["admissibility_failures"]


def test_precommitted_anchor_is_exact_market_boundary_not_stale_age_guess() -> None:
    request = _payload_request()
    evidence = request["evidence_ledger_payload"]
    for entry in evidence["entries"]:
        entry["folds"][0]["anchor_as_of_utc"] = "2017-02-01T00:00:00Z"
        entry["folds_sha256"] = _sha_json(entry["folds"])
    evidence["entries_sha256"] = _sha_json(evidence["entries"])
    _refresh_complete_chain(request)

    decision = _evaluate(request)
    assert decision.status == "REJECTED"
    assert "ANCHOR_NOT_EXACT_OOS_BOUNDARY" in decision.reason_codes[0]


def test_precommitted_fold_intervals_cannot_overlap() -> None:
    request = _payload_request()
    evidence = request["evidence_ledger_payload"]
    for entry in evidence["entries"]:
        fold = entry["folds"][1]
        fold["anchor_as_of_utc"] = "2021-03-01T00:00:00Z"
        fold["oos_start_date_utc"] = "2021-03-01"
        fold["oos_end_date_utc"] = "2021-05-02"
        entry["folds_sha256"] = _sha_json(entry["folds"])
    evidence["entries_sha256"] = _sha_json(evidence["entries"])
    _refresh_complete_chain(request)

    decision = _evaluate(request)
    assert decision.status == "REJECTED"
    assert decision.reason_codes == ("CAUSAL_FOLD_PLAN_INTERVALS_OVERLAP",)


def test_evidence_access_cannot_precede_latest_closed_market_boundary() -> None:
    request = _payload_request()
    evidence = request["evidence_ledger_payload"]
    early = "2024-01-01T00:00:00Z"
    evidence["evidence_access_started_at_utc"] = early
    evidence["evidence_access_completed_at_utc"] = early
    evidence["generated_at_utc"] = early
    for entry in evidence["entries"]:
        for fold in entry["folds"]:
            fold["oos_evidence_accessed_at_utc"] = early
            fold["fold_evaluated_at_utc"] = early
        entry["folds_sha256"] = _sha_json(entry["folds"])
    evidence["entries_sha256"] = _sha_json(evidence["entries"])

    decision = _evaluate(request)
    assert decision.status == "REJECTED"
    assert decision.reason_codes == ("EVIDENCE_ACCESS_PRECEDES_LATEST_MARKET_BOUNDARY",)


def test_access_completion_and_evidence_freeze_clock_must_be_exact() -> None:
    request = _payload_request()
    evidence = request["evidence_ledger_payload"]
    evidence["generated_at_utc"] = "2099-01-01T00:00:00Z"

    decision = _evaluate(request)
    assert decision.status == "REJECTED"
    assert decision.reason_codes == ("EVIDENCE_FREEZE_NOT_EXACT_ACCESS_COMPLETION",)


def test_self_consistent_future_evidence_clock_is_not_trusted_today() -> None:
    request = _payload_request()
    evidence = request["evidence_ledger_payload"]
    future = "2099-01-01T00:00:00Z"
    evidence["evidence_access_started_at_utc"] = future
    evidence["evidence_access_completed_at_utc"] = future
    evidence["generated_at_utc"] = future
    for entry in evidence["entries"]:
        for fold in entry["folds"]:
            fold["oos_evidence_accessed_at_utc"] = future
            fold["fold_evaluated_at_utc"] = future
        entry["folds_sha256"] = _sha_json(entry["folds"])
    evidence["entries_sha256"] = _sha_json(evidence["entries"])

    decision = _evaluate(request)
    assert decision.status == "REJECTED"
    assert decision.reason_codes == ("EVIDENCE_TRUSTED_CLOCK_IN_FUTURE",)


def test_flat_to_flat_additive_r_accounting_rejects_trade_mtm_contradiction() -> None:
    request = _payload_request()
    evidence = request["evidence_ledger_payload"]
    entry = evidence["entries"][0]
    assert sum(
        sum(fold["daily_net_mtm_r"]) for fold in entry["folds"]
    ) == pytest.approx(26.45, abs=0.2)
    for fold in entry["folds"]:
        count = len(fold["completed_trade_close_utc"])
        fold["completed_trade_net_mtm_r"] = [-1.1] * count
        fold["completed_trade_net_mtm_r_cost_stress"] = [-1.2] * count
        _rehash_trade_vector(fold)
    assert sum(
        sum(fold["completed_trade_net_mtm_r"]) for fold in entry["folds"]
    ) == pytest.approx(-528.0)
    entry["folds_sha256"] = _sha_json(entry["folds"])
    evidence["entries_sha256"] = _sha_json(evidence["entries"])

    decision = _evaluate(request)
    assert decision.status == "REJECTED"
    assert "BASE_FLAT_TO_FLAT_ACCOUNTING_MISMATCH" in decision.reason_codes[0]


def test_precommitted_dependence_plan_has_fail_before_growth_bound() -> None:
    assert winner._dependence_operation_count([63] * 4) == 4_620
    maximum_daily_fold_layout = [winner.MAX_DAYS_PER_FOLD] * winner.MAX_FOLDS
    assert winner._dependence_operation_count(maximum_daily_fold_layout) == 2_824_704
    assert (
        winner._dependence_operation_count(maximum_daily_fold_layout)
        < winner.MAX_DEPENDENCE_BLOCK_OPERATIONS
    )
    maximum_pair_count = winner.MAX_ATTEMPTS * (winner.MAX_ATTEMPTS - 1) // 2
    maximum_pair_product = (
        4
        * maximum_pair_count
        * winner._dependence_operation_count(maximum_daily_fold_layout)
    )
    assert maximum_pair_product > winner.MAX_DEPENDENCE_BLOCK_OPERATIONS
    assert (
        winner.pinned_policy_snapshot()["pairwise_dependence_operation_limit"]
        == winner.MAX_DEPENDENCE_BLOCK_OPERATIONS
    )
    policy = winner.pinned_policy_snapshot()
    assert "ALL_FEASIBLE" not in policy["block_inference_rule"]
    assert policy["attempt_aware_dsr_reapplications_per_block_scale"] == 0


def test_streamed_block_mean_uses_small_sample_student_t_not_normal_shortcut() -> None:
    # Construct moments with t=.862 and 23 degrees of freedom.  The exact
    # one-sided Student-t tail is ~.198793 (normal would report ~.1944).
    moments = winner._RunningMoments(
        count=24,
        mean=0.862 / math.sqrt(24.0),
        m2=23.0,
    )
    assert winner._student_t_mean_one_sided_p(moments) == pytest.approx(
        0.19879306509968825,
        abs=1e-14,
    )


def test_v13_public_offline_apis_have_no_dependency_injection_surface() -> None:
    expected = {
        "create_winner_selection_request": (
            "precommitment_payload",
            "attempt_ledger_payload",
            "evidence_ledger_payload",
        ),
        "evaluate_historical_cell_winner": ("request",),
        "verify_historical_cell_winner_decision": ("decision", "request"),
        "historical_book_marginal_receipt": ("decision", "request"),
    }
    assert winner.WINNER_SELECTION_CONTRACT_VERSION == "fxpm.winner-selection.v13"
    assert winner.WINNER_SELECTION_REQUEST_VERSION == "fxpm.winner-selection-request.v1"
    assert winner.WINNER_SELECTION_SCOPE == "OFFLINE_BOOK_SELECTION"
    for name, parameter_names in expected.items():
        function = getattr(winner, name)
        signature = inspect.signature(function)
        assert tuple(signature.parameters) == parameter_names
        assert function.__defaults__ is None
        assert function.__kwdefaults__ is None
        assert all(
            parameter.kind
            in {
                inspect.Parameter.POSITIONAL_ONLY,
                inspect.Parameter.POSITIONAL_OR_KEYWORD,
            }
            for parameter in signature.parameters.values()
        )


def test_public_apis_reject_hidden_dependency_keywords() -> None:
    request = _payload_request()
    typed_request = _typed_request(request)
    decision = _evaluate(request)
    with pytest.raises(TypeError):
        winner.evaluate_historical_cell_winner(typed_request, _impl=lambda: None)
    with pytest.raises(TypeError):
        winner.verify_historical_cell_winner_decision(
            decision,
            typed_request,
            _impl=lambda: True,
        )
    with pytest.raises(TypeError):
        winner.historical_book_marginal_receipt(
            decision, typed_request, _impl=lambda: {}
        )
    with pytest.raises(TypeError):
        winner.create_winner_selection_request(
            request["precommitment_payload"],
            request["attempt_ledger_payload"],
            request["evidence_ledger_payload"],
            _impl=lambda: None,
        )


def test_local_result_scope_documents_the_semantic_authority_boundary() -> None:
    """Same-process code mutation is outside the isolated-user threat model.

    The enforceable boundary here is semantic: this local result can feed the
    historical Book, but contains no capital, funded-live, promotion, final
    process, or profitability authorization.  Downstream gates own those
    decisions.
    """

    request = _payload_request()
    decision = _evaluate(request)
    body = decision.to_dict()
    assert decision.selection_scope == "OFFLINE_BOOK_SELECTION"
    assert body["selection_scope"] == winner.WINNER_SELECTION_SCOPE
    assert body["validated"] is True
    assert body["live_authorized"] is False
    assert body["capital_sizing_authorized"] is False
    assert body["profitability_claimed"] is False
    assert body["production_integrated"] is False
    assert "authority_scope" not in body
    assert "verified" not in body


def test_no_output_signer_or_promotion_api_is_exposed() -> None:
    forbidden = {
        "sign_winner_selection_decision",
        "create_trust_bundle",
        "WinnerSelectionTrustBundle",
        "PRECOMMITMENT_SIGNATURE_DOMAIN",
        "ATTEMPT_SIGNATURE_DOMAIN",
        "EVIDENCE_SIGNATURE_DOMAIN",
        "issue_live_winner",
        "authorize_capital",
        "authorize_promotion",
        "promote_winner",
    }
    assert forbidden.isdisjoint(set(winner.__all__))
    assert all(not hasattr(winner, name) for name in forbidden)
    source = inspect.getsource(winner)
    for token in (
        "cryptography",
        "Ed25519",
        "signature_base64",
        "TrustBundle",
        "create_trust_bundle",
    ):
        assert token not in source


def test_valid_request_is_constructible_from_exported_production_api_only() -> None:
    precommitment, attempt_ledger, evidence_ledger = _base_payloads()
    request = winner.create_winner_selection_request(
        precommitment,
        attempt_ledger,
        evidence_ledger,
    )
    assert type(request) is winner.WinnerSelectionRequest
    assert type(request.precommitment) is winner.WinnerPrecommitmentPayload
    assert type(request.attempt_ledger) is winner.WinnerAttemptLedgerPayload
    assert type(request.evidence_ledger) is winner.WinnerEvidenceLedgerPayload
    original_run_id = request.precommitment.to_dict()["run_id"]
    precommitment["run_id"] = "0" * 64
    assert request.precommitment.to_dict()["run_id"] == original_run_id
    decision = winner.evaluate_historical_cell_winner(request)
    assert decision.status == "PROCESS_WINNER_PENDING_BOOK_MULTIPLICITY"
    assert winner.verify_historical_cell_winner_decision(decision, request)
    assert winner.historical_book_marginal_receipt(decision, request) is not None


def test_attempt_multiplicity_penalizes_identical_statistical_evidence() -> None:
    values = [0.03 + 0.08 * math.sin(index * 1.7) for index in range(4 * 63)]
    effective = winner._fold_aware_inference(
        [values[index : index + 63] for index in range(0, len(values), 63)]
    )["ess"]
    one_attempt = winner._one_sided_dsr_p(values, effective, 1)
    many_attempts = winner._one_sided_dsr_p(values, effective, 128)
    assert 0.0 <= one_attempt < many_attempts <= 1.0


def test_selection_dominance_is_exactly_one_primary_utility_dimension() -> None:
    field = "conservative_primary_utility_lcb_daily_log_growth"
    baseline = {
        field: 0.0001,
        "tail_cvar_daily_r": 5.0,
        "max_path_drawdown_r": 0.0,
        "worst_fold_hac_lcb_daily_r": 5.0,
    }
    growth_superior_with_worse_diagnostics = {
        field: 0.0002,
        "tail_cvar_daily_r": -50.0,
        "max_path_drawdown_r": 50.0,
        "worst_fold_hac_lcb_daily_r": -50.0,
    }
    assert winner._primary_utility_better(
        growth_superior_with_worse_diagnostics, baseline
    )
    assert not winner._primary_utility_better(
        baseline, growth_superior_with_worse_diagnostics
    )
    assert winner.pinned_policy_snapshot()["selection_dominance_fields"] == [field]
    assert not hasattr(winner, "_metric_pareto_better")
    assert not hasattr(winner, "_PARETO_HIGHER_FIELDS")
    assert not hasattr(winner, "_PARETO_LOWER_FIELDS")


@pytest.mark.parametrize("nonfinite", [math.nan, math.inf, -math.inf])
def test_nonfinite_request_numbers_fail_before_evaluation(nonfinite: float) -> None:
    request = _payload_request()
    request["precommitment_payload"]["planned_attempt_count"] = nonfinite
    typed = winner.create_winner_selection_request(
        request["precommitment_payload"],
        request["attempt_ledger_payload"],
        request["evidence_ledger_payload"],
    )
    assert typed is None
    decision = winner.evaluate_historical_cell_winner(typed)
    assert decision.status == "REJECTED"
    assert decision.reason_codes == ("REQUEST_TYPE_INVALID",)


def test_cyclic_deep_wide_and_huge_inputs_are_total_and_bounded() -> None:
    request = _payload_request()
    cyclic: dict[str, Any] = {}
    cyclic["self"] = cyclic
    deep: dict[str, Any] = {}
    cursor = deep
    for _ in range(40):
        child: dict[str, Any] = {}
        cursor["next"] = child
        cursor = child
    wide = {str(index): index for index in range(winner.MAX_CONTAINER_ITEMS + 1)}
    huge = {"blob": "x" * (winner.MAX_STRING_CHARS + 1)}
    tracemalloc.start()
    try:
        for hostile in (cyclic, deep, wide, huge):
            typed = winner.create_winner_selection_request(
                hostile,
                request["attempt_ledger_payload"],
                request["evidence_ledger_payload"],
            )
            assert typed is None
            decision = winner.evaluate_historical_cell_winner(hostile)
            assert decision.status == "REJECTED"
            assert not _verify(decision, request)
        _, peak = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()
    assert peak < 80 * 1024 * 1024


def test_public_helpers_are_total_on_hostile_types() -> None:
    assert winner.compute_cell_id([], "M5", "TREND") is None
    request = _payload_request()
    assert winner.create_winner_selection_request([], object(), None) is None
    rejected = winner.evaluate_historical_cell_winner(object())
    assert rejected.status == "REJECTED"
    typed_request = _typed_request(request)
    assert not winner.verify_historical_cell_winner_decision(object(), typed_request)
    assert winner.historical_book_marginal_receipt(object(), typed_request) is None


@pytest.mark.parametrize(
    "name",
    [
        "CurrentRefitBindingDecision",
        "CurrentRefitBindingTrustBundle",
        "bind_current_refits_after_book",
        "create_current_refit_binding_trust_bundle",
        "verify_current_refit_binding_decision",
    ],
)
def test_post_book_refit_authority_is_not_exposed(name: str) -> None:
    assert not hasattr(winner, name)
    assert name not in winner.__all__
