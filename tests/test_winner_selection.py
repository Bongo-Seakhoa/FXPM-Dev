from __future__ import annotations

import base64
from copy import deepcopy
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
import hashlib
import inspect
import json
import math
import random
import struct
import tracemalloc
from typing import Any

import pytest

import pm_winner_selection as winner

RISK_UNIT_CONTRACT_SHA256 = winner.PINNED_RISK_UNIT_CONTRACT_SHA256
VALIDATED_P75_TOLERANCE = {
    "level_id": "UNIFORM_ALL_FILLS_SINGLE_FILL_UPLIFT_P75",
    "single_fill_uplift_percentile": 75,
    "uniform_entry_half_spread_multiplier_float64_hex": "0x1.a000000000000p+2",
}


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


def _fold_bounds(axis_row: dict[str, Any]) -> tuple[datetime, datetime]:
    start = datetime.combine(
        date.fromisoformat(axis_row["oos_start_date_utc"]),
        datetime.min.time(),
        tzinfo=timezone.utc,
    )
    end = datetime.combine(
        date.fromisoformat(axis_row["oos_end_date_utc"]) + timedelta(days=1),
        datetime.min.time(),
        tzinfo=timezone.utc,
    )
    return start, end


def _r_e12(values: list[float], maximum_abs_r: float) -> list[int]:
    return [
        winner._encode_r_e12(value, maximum_abs_r, "TEST_R_E12_INVALID")
        for value in values
    ]


def _packed_r_e12(values: list[float], maximum_abs_r: float) -> str:
    return winner._encode_r_e12_i64be_base64(
        _r_e12(values, maximum_abs_r),
        maximum_abs_r,
        "TEST_R_I64BE_BASE64_INVALID",
    )


def _unpacked_r_e12(
    fold: dict[str, Any],
    field: str,
    *,
    expected_count: int,
    maximum_abs_r: float,
) -> list[int]:
    return winner._decode_r_e12_i64be_base64(
        fold[field],
        expected_count=expected_count,
        maximum_abs_r=maximum_abs_r,
        code="TEST_R_I64BE_BASE64_INVALID",
    )


def _set_packed_r_e12(
    fold: dict[str, Any],
    field: str,
    values: list[int],
    maximum_abs_r: float,
) -> None:
    fold[field] = winner._encode_r_e12_i64be_base64(
        values,
        maximum_abs_r,
        "TEST_R_I64BE_BASE64_INVALID",
    )


def _decoded_position_ids(fold: dict[str, Any]) -> list[str]:
    count = len(fold["completed_trade_open_offset_seconds"])
    return winner._decode_position_id_digests(
        fold["completed_trade_position_id_digests_base64"],
        count,
        "TEST_POSITION_ID_DIGESTS_INVALID",
    )


def _set_daily_lanes(
    fold: dict[str, Any],
    base: list[float],
    p75: list[float],
    p95: list[float],
) -> None:
    fold["daily_net_mtm_r_e12_i64be_base64"] = _packed_r_e12(
        base, winner.MAX_ABS_DAILY_R
    )
    fold["daily_net_mtm_r_p75_e12_i64be_base64"] = _packed_r_e12(
        p75, winner.MAX_ABS_DAILY_R
    )
    fold["daily_net_mtm_r_p95_e12_i64be_base64"] = _packed_r_e12(
        p95, winner.MAX_ABS_DAILY_R
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
            "cost_ladder_contract_sha256": _sha("cost-ladder-contract"),
            "risk_unit_contract_sha256": RISK_UNIT_CONTRACT_SHA256,
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


def _fold_plan() -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for fold_index in range(4):
        start = date(2021 + fold_index, 2, 1)
        end = start + timedelta(days=62)
        rows.append(
            {
                "fold_ordinal": fold_index,
                "fold_id": _sha1(f"shared-fold:{fold_index}"),
                "anchor_as_of_utc": _iso(
                    datetime.combine(start, datetime.min.time(), tzinfo=timezone.utc)
                ),
                "oos_start_date_utc": start.isoformat(),
                "oos_end_date_utc": end.isoformat(),
            }
        )
    return rows


def _capacity_fold_plan() -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    start = date(2021, 2, 1)
    for fold_index, day_count in enumerate((63, 63, 63, 381)):
        end = start + timedelta(days=day_count - 1)
        rows.append(
            {
                "fold_ordinal": fold_index,
                "fold_id": _sha1(f"capacity-shared-fold:{fold_index}"),
                "anchor_as_of_utc": _iso(
                    datetime.combine(
                        start,
                        datetime.min.time(),
                        tzinfo=timezone.utc,
                    )
                ),
                "oos_start_date_utc": start.isoformat(),
                "oos_end_date_utc": end.isoformat(),
            }
        )
        start = end + timedelta(days=1)
    return rows


def _shared_axis(
    fold_plan: list[dict[str, Any]],
    *,
    symbol: str = "AUDUSD",
    timeframe: str = "M5",
    regime: str = "TREND",
) -> list[dict[str, Any]]:
    cell_id = winner.compute_cell_id(symbol, timeframe, regime)
    assert cell_id is not None
    rows: list[dict[str, Any]] = []
    for planned in fold_plan:
        fold_index = planned["fold_ordinal"]
        start = date.fromisoformat(planned["oos_start_date_utc"])
        end = date.fromisoformat(planned["oos_end_date_utc"])
        partition_body = {
            "schema_version": winner.SHARED_OOS_PARTITION_VERSION,
            "cell_id": cell_id,
            "symbol": symbol,
            "timeframe": timeframe,
            "regime": regime,
            "causal_fold_plan_row_sha256": _sha_json(planned),
            "native_prefix_index_sha256": _sha(
                f"native-prefix-index:{fold_index}"
            ),
            "native_prefix_content_sha256": _sha(
                f"native-prefix-content:{fold_index}"
            ),
            "source_contract_sha256": _sha("source-contract"),
            "native_source_sha256": _sha("native-source"),
            "activation_manifest_sha256": _sha("activation-manifest"),
            "source_group_decision_sha256": _sha("source-group-decision"),
            "regime_fit_decision_sha256": _sha("regime-fit-decision"),
            "execution_spec_receipt_sha256": _sha("execution-spec-receipt"),
            "cost_contract_sha256": _sha("cost-contract"),
            "cost_ladder_contract_sha256": _sha("cost-ladder-contract"),
            "risk_unit_contract_sha256": RISK_UNIT_CONTRACT_SHA256,
        }
        rows.append(
            {
                **planned,
                "daily_dates_utc": [
                    (start + timedelta(days=index)).isoformat()
                    for index in range((end - start).days + 1)
                ],
                "shared_oos_partition": {
                    **partition_body,
                    "receipt_sha256": _sha_json(partition_body),
                },
            }
        )
    return rows


def _fold(
    candidate_index: int,
    fold_index: int,
    axis_row: dict[str, Any],
) -> dict[str, Any]:
    start = date.fromisoformat(axis_row["oos_start_date_utc"])
    base: list[float] = []
    for index in range(len(axis_row["daily_dates_utc"])):
        common = 0.055 * math.sin((index + fold_index * 7) * 1.70)
        paired = 0.012 * math.sin((index + fold_index * 3) * 0.77)
        if candidate_index == 0:
            value = 0.105 + common + paired
        elif candidate_index == 1:
            value = 0.105 + common - paired
        else:
            value = -0.025 + common
        base.append(round(value, 12))
    p75 = [round(value - 0.010, 12) for value in base]
    p95 = [round(value - 0.020, 12) for value in base]
    trade_count = 120
    close_times = [
        _iso(
            datetime.combine(start, datetime.min.time(), tzinfo=timezone.utc)
            + timedelta(hours=12 * index + 1)
        )
        for index in range(trade_count)
    ]
    open_times = [
        _iso(datetime.fromisoformat(value.replace("Z", "+00:00")) - timedelta(hours=1))
        for value in close_times
    ]
    position_ids = [
        "btp:" + _sha(
            f"position:{candidate_index}:{fold_index}:{index:04d}"
        )
        for index in range(trade_count)
    ]
    fold_start, fold_end = _fold_bounds(axis_row)
    open_orders = [2 * index for index in range(trade_count)]
    close_orders = [2 * index + 1 for index in range(trade_count)]
    order_anchor, open_order_offsets, close_order_offsets = (
        winner._encode_event_order_offsets(
            open_orders,
            close_orders,
            "TEST_EVENT_ORDER_INVALID",
        )
    )
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
    fold = {
        "fold_ordinal": fold_index,
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
        "position_activity_ledger_sha256": _sha(
            f"position-activity:{candidate_index}:{fold_index}"
        ),
        "daily_net_mtm_r_e12_i64be_base64": _packed_r_e12(
            base, winner.MAX_ABS_DAILY_R
        ),
        "daily_net_mtm_r_p75_e12_i64be_base64": _packed_r_e12(
            p75, winner.MAX_ABS_DAILY_R
        ),
        "daily_net_mtm_r_p95_e12_i64be_base64": _packed_r_e12(
            p95, winner.MAX_ABS_DAILY_R
        ),
        "daily_ladder_sha256": "0" * 64,
        "completed_trade_open_offset_seconds": (
            winner._encode_utc_second_offsets(
                open_times,
                fold_start_utc=fold_start,
                fold_end_exclusive_utc=fold_end,
                code="TEST_OPEN_OFFSET_INVALID",
            )
        ),
        "completed_trade_close_offset_seconds": (
            winner._encode_utc_second_offsets(
                close_times,
                fold_start_utc=fold_start,
                fold_end_exclusive_utc=fold_end,
                code="TEST_CLOSE_OFFSET_INVALID",
            )
        ),
        "completed_trade_position_id_digests_base64": (
            winner._encode_position_id_digests(
                position_ids,
                "TEST_POSITION_ID_INVALID",
            )
        ),
        "completed_trade_event_order_anchor": order_anchor,
        "completed_trade_open_event_order_offsets": open_order_offsets,
        "completed_trade_close_event_order_offsets": close_order_offsets,
        "completed_trade_net_mtm_r_e12_i64be_base64": "",
        "completed_trade_net_mtm_r_p75_e12_i64be_base64": "",
        "completed_trade_net_mtm_r_p95_e12_i64be_base64": "",
        "trade_power_binding": {
            **power_body,
            "binding_sha256": _sha_json(power_body),
        },
        "trade_vector_sha256": "0" * 64,
    }
    _refresh_fold_accounting(fold, axis_row)
    return fold


def _rehash_trade_vector(
    fold: dict[str, Any],
    axis_row: dict[str, Any] | None = None,
) -> None:
    if axis_row is None:
        axis_row = _shared_axis(_fold_plan())[fold["fold_ordinal"]]
    body = {
        "shared_fold_axis_row_sha256": _sha_json(axis_row),
        "accounting_identity_kind": winner.PINNED_SELECTION_POLICY[
            "accounting_identity"
        ],
        "completed_trade_unit": winner.PINNED_SELECTION_POLICY["daily_unit"],
        "inventory_boundary_kind": winner.PINNED_SELECTION_POLICY[
            "inventory_boundary"
        ],
        "completed_trade_open_offset_seconds": fold[
            "completed_trade_open_offset_seconds"
        ],
        "completed_trade_close_offset_seconds": fold[
            "completed_trade_close_offset_seconds"
        ],
        "completed_trade_position_id_digests_base64": fold[
            "completed_trade_position_id_digests_base64"
        ],
        "completed_trade_event_order_anchor": fold[
            "completed_trade_event_order_anchor"
        ],
        "completed_trade_open_event_order_offsets": fold[
            "completed_trade_open_event_order_offsets"
        ],
        "completed_trade_close_event_order_offsets": fold[
            "completed_trade_close_event_order_offsets"
        ],
        "completed_trade_net_mtm_r_e12_i64be_base64": fold[
            "completed_trade_net_mtm_r_e12_i64be_base64"
        ],
        "completed_trade_net_mtm_r_p75_e12_i64be_base64": fold[
            "completed_trade_net_mtm_r_p75_e12_i64be_base64"
        ],
        "completed_trade_net_mtm_r_p95_e12_i64be_base64": fold[
            "completed_trade_net_mtm_r_p95_e12_i64be_base64"
        ],
        "position_activity_ledger_sha256": fold[
            "position_activity_ledger_sha256"
        ],
        "risk_unit_contract_sha256": RISK_UNIT_CONTRACT_SHA256,
        "trade_power_binding": fold["trade_power_binding"],
    }
    fold["trade_vector_sha256"] = _sha_json(body)


def _refresh_fold_accounting(
    fold: dict[str, Any],
    axis_row: dict[str, Any] | None = None,
) -> None:
    if axis_row is None:
        axis_row = _shared_axis(_fold_plan())[fold["fold_ordinal"]]
    _rehash_daily_ladder(fold, axis_row)
    count = len(fold["completed_trade_close_offset_seconds"])
    for field in winner._SERIALIZED_TRADE_LANE_FIELDS.values():
        daily_field = {
            "completed_trade_net_mtm_r_e12_i64be_base64": (
                "daily_net_mtm_r_e12_i64be_base64"
            ),
            "completed_trade_net_mtm_r_p75_e12_i64be_base64": (
                "daily_net_mtm_r_p75_e12_i64be_base64"
            ),
            "completed_trade_net_mtm_r_p95_e12_i64be_base64": (
                "daily_net_mtm_r_p95_e12_i64be_base64"
            ),
        }[field]
        daily_values = _unpacked_r_e12(
            fold,
            daily_field,
            expected_count=len(axis_row["daily_dates_utc"]),
            maximum_abs_r=winner.MAX_ABS_DAILY_R,
        )
        total = sum(daily_values)
        if count == 0:
            values: list[int] = []
        else:
            base_value = total // count
            values = [
                base_value + (10_000_000_000 if index % 2 == 0 else -10_000_000_000)
                for index in range(max(0, count - 1))
            ]
            values.append(total - sum(values))
        _set_packed_r_e12(fold, field, values, winner.MAX_ABS_TRADE_R)
    _rehash_trade_vector(fold, axis_row)


def _rehash_daily_ladder(
    fold: dict[str, Any],
    axis_row: dict[str, Any] | None = None,
) -> None:
    if axis_row is None:
        axis_row = _shared_axis(_fold_plan())[fold["fold_ordinal"]]
    fold["daily_ladder_sha256"] = _sha_json(
        {
            "shared_fold_axis_row_sha256": _sha_json(axis_row),
            **{
                field: fold[field]
                for field in winner._SERIALIZED_DAILY_LANE_FIELDS.values()
            },
        }
    )


def _bind_trade_power(
    fold: dict[str, Any],
    attempt_ledger_sha256: str,
    attempted_trials: int,
    axis_row: dict[str, Any] | None = None,
) -> None:
    binding = fold["trade_power_binding"]
    assert type(binding) is dict
    body = {
        key: value for key, value in binding.items() if key != "binding_sha256"
    }
    body["attempt_ledger_payload_sha256"] = attempt_ledger_sha256
    body["attempted_trials_exact"] = attempted_trials
    fold["trade_power_binding"] = {
        **body,
        "binding_sha256": _sha_json(body),
    }
    _rehash_trade_vector(fold, axis_row)


def _refresh_entry_hashes(entry: dict[str, Any]) -> None:
    entry["fold_count"] = len(entry["folds"])
    entry["folds_sha256"] = _sha_json(entry["folds"])


def _base_payloads(
    symbol: str = "AUDUSD",
    timeframe: str = "M5",
    regime: str = "TREND",
    *,
    fold_plan_override: list[dict[str, Any]] | None = None,
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    attempts = _attempts()
    for attempt in attempts:
        attempt["source_contract_sha256"] = _sha("source-contract")
        attempt["cost_contract_sha256"] = _sha("cost-contract")
        attempt["cost_ladder_contract_sha256"] = _sha("cost-ladder-contract")
        attempt["risk_unit_contract_sha256"] = RISK_UNIT_CONTRACT_SHA256
    run_id = _sha(f"run:{symbol}:{timeframe}:{regime}")
    cell_id = winner.compute_cell_id(symbol, timeframe, regime)
    assert cell_id is not None
    common = {
        "cell_id": cell_id,
        "symbol": symbol,
        "timeframe": timeframe,
        "regime": regime,
        "source_contract_sha256": _sha("source-contract"),
        "cost_contract_sha256": _sha("cost-contract"),
        "cost_ladder_contract_sha256": _sha("cost-ladder-contract"),
        "risk_unit_contract_sha256": RISK_UNIT_CONTRACT_SHA256,
        "process_registry_sha256": _process_registry(attempts),
        "horizon_registry_sha256": _horizon_registry(attempts),
        "trial_registry_sha256": _trial_registry(attempts),
    }
    fold_plan = (
        _fold_plan()
        if fold_plan_override is None
        else deepcopy(fold_plan_override)
    )
    latest_boundary = datetime.combine(
        date.fromisoformat(fold_plan[-1]["oos_end_date_utc"]) + timedelta(days=1),
        datetime.min.time(),
        tzinfo=timezone.utc,
    )
    policy = {
        "contract_version": winner.PRECOMMITMENT_VERSION,
        "run_id": run_id,
        "precommitted_at_utc": "2020-01-01T00:00:00Z",
        **common,
        "selection_policy_sha256": winner.PINNED_SELECTION_POLICY_SHA256,
        "planned_attempt_count": len(attempts),
        "planned_fold_count": len(fold_plan),
        "causal_fold_plan_sha256": _sha_json(fold_plan),
        "causal_fold_plan": fold_plan,
        "latest_evidence_boundary_utc": _iso(latest_boundary),
    }
    attempt_ledger = {
        "contract_version": winner.ATTEMPT_LEDGER_VERSION,
        "run_id": run_id,
        "frozen_at_utc": "2020-01-02T00:00:00Z",
        "precommitment_payload_sha256": _sha_json(policy),
        **common,
        "attempt_count": len(attempts),
        "attempts_sha256": _sha_json(attempts),
        "attempts": attempts,
    }
    attempt_sha = _sha_json(attempt_ledger)
    shared_axis = _shared_axis(
        fold_plan,
        symbol=symbol,
        timeframe=timeframe,
        regime=regime,
    )
    entries: list[dict[str, Any]] = []
    for ordinal, attempt in enumerate(attempts):
        folds = [
            _fold(ordinal, fold_index, shared_axis[fold_index])
            for fold_index in range(len(shared_axis))
        ]
        for fold_index, fold in enumerate(folds):
            _bind_trade_power(
                fold,
                attempt_sha,
                len(attempts),
                shared_axis[fold_index],
            )
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
                "cost_ladder_contract_sha256": attempt[
                    "cost_ladder_contract_sha256"
                ],
                "risk_unit_contract_sha256": attempt["risk_unit_contract_sha256"],
                "evidence_status": "AUTHENTICATED",
                "status_reason": None,
                "fold_count": len(folds),
                "folds_sha256": _sha_json(folds),
                "folds": folds,
            }
        )
    evidence = {
        "contract_version": winner.EVIDENCE_LEDGER_VERSION,
        "run_id": run_id,
        "generated_at_utc": "2025-01-01T00:00:00Z",
        "evidence_access_started_at_utc": "2025-01-01T00:00:00Z",
        "evidence_access_completed_at_utc": "2025-01-01T00:00:00Z",
        "latest_evidence_boundary_utc": _iso(latest_boundary),
        "precommitment_payload_sha256": _sha_json(policy),
        "attempt_ledger_payload_sha256": attempt_sha,
        **common,
        "shared_fold_axis_count": len(shared_axis),
        "shared_fold_axis_sha256": _sha_json(shared_axis),
        "shared_fold_axis": shared_axis,
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
    *,
    fold_plan_override: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    policy, attempts, evidence = _base_payloads(
        symbol,
        timeframe,
        regime,
        fold_plan_override=fold_plan_override,
    )
    return {
        "precommitment_payload": policy,
        "attempt_ledger_payload": attempts,
        "evidence_ledger_payload": evidence,
    }


_CAPACITY_IDENTITY_FIELDS = (
    "anchor_selection_receipt_sha256",
    "anchor_horizon_decision_sha256",
    "anchor_trial_ledger_sha256",
    "anchor_selected_trial_sha256",
    "anchor_refit_receipt_sha256",
    "attempt_replay_receipt_sha256",
    "attempt_oos_source_receipt_sha256",
    "position_activity_ledger_sha256",
)


def _capacity_attempt(
    ordinal: int,
    *,
    evaluated_attempt_count: int,
) -> dict[str, Any]:
    return {
        "ordinal": ordinal,
        "attempt_id": _sha1(f"capacity-attempt:{ordinal}"),
        "process_candidate_id": _sha1(f"capacity-candidate:{ordinal}"),
        "strategy_family": "f" * 128,
        "strategy_id": "s" * 128,
        "parameter_count": 100_000,
        "precommitted_evidence_role": (
            "PRESEARCH_PROCESS_REJECTED"
            if ordinal >= evaluated_attempt_count
            else "EVALUATION_REQUIRED"
        ),
        "process_sha256": _sha(f"capacity-process:{ordinal}"),
        "horizon_process_sha256": _sha(f"capacity-horizon:{ordinal}"),
        "trial_definition_sha256": _sha(f"capacity-trial:{ordinal}"),
        "refit_recipe_sha256": _sha(f"capacity-refit:{ordinal}"),
        "source_contract_sha256": _sha("source-contract"),
        "cost_contract_sha256": _sha("cost-contract"),
        "cost_ladder_contract_sha256": _sha("cost-ladder-contract"),
        "risk_unit_contract_sha256": RISK_UNIT_CONTRACT_SHA256,
    }


def _set_capacity_trade_lanes(
    fold: dict[str, Any],
    axis_row: dict[str, Any],
) -> None:
    trade_count = len(fold["completed_trade_open_offset_seconds"])

    def exact_partition(total: int) -> list[int]:
        quotient, remainder = divmod(total, trade_count)
        return [
            quotient + int(index < remainder) for index in range(trade_count)
        ]

    for lane in ("BASE", "P75", "P95"):
        daily_field = winner._SERIALIZED_DAILY_LANE_FIELDS[lane]
        trade_field = winner._SERIALIZED_TRADE_LANE_FIELDS[lane]
        daily_values = _unpacked_r_e12(
            fold,
            daily_field,
            expected_count=len(axis_row["daily_dates_utc"]),
            maximum_abs_r=winner.MAX_ABS_DAILY_R,
        )
        _set_packed_r_e12(
            fold,
            trade_field,
            exact_partition(sum(daily_values)),
            winner.MAX_ABS_TRADE_R,
        )
    _rehash_trade_vector(fold, axis_row)


def _capacity_fold(
    template: dict[str, Any],
    axis_row: dict[str, Any],
    *,
    attempt_ordinal: int,
    trade_count: int,
) -> dict[str, Any]:
    fold = deepcopy(template)
    fold_ordinal = fold["fold_ordinal"]
    for field in _CAPACITY_IDENTITY_FIELDS:
        fold[field] = _sha(f"capacity:{field}:{attempt_ordinal}:{fold_ordinal}")
    binding = dict(fold["trade_power_binding"])
    binding_body = {
        key: value for key, value in binding.items() if key != "binding_sha256"
    }
    binding_body.update(
        {
            "train_requirement_sha256": _sha(
                f"capacity-power-requirement:{attempt_ordinal}:{fold_ordinal}"
            ),
            "train_calibration_sample_sha256": _sha(
                f"capacity-power-sample:{attempt_ordinal}:{fold_ordinal}"
            ),
            "train_partition_contract_sha256": _sha(
                f"capacity-train-partition:{attempt_ordinal}:{fold_ordinal}"
            ),
            "validation_decision_sha256": _sha(
                f"capacity-validation:{attempt_ordinal}:{fold_ordinal}"
            ),
            "validation_partition_contract_sha256": _sha(
                f"capacity-validation-partition:{attempt_ordinal}:{fold_ordinal}"
            ),
            "required_effective_trades": winner.MAX_TOTAL_COMPLETED_TRADES,
        }
    )
    fold["trade_power_binding"] = {
        **binding_body,
        "binding_sha256": _sha_json(binding_body),
    }
    day_count = len(axis_row["daily_dates_utc"])
    daily_values = (
        -4_000_000_000_000,
        -5_000_000_000_000,
        -6_000_000_000_000,
    )
    for lane, value in zip(("BASE", "P75", "P95"), daily_values, strict=True):
        _set_packed_r_e12(
            fold,
            winner._SERIALIZED_DAILY_LANE_FIELDS[lane],
            [value] * day_count,
            winner.MAX_ABS_DAILY_R,
        )
    fold_seconds = day_count * 86_400
    fold["completed_trade_close_offset_seconds"] = [
        fold_seconds - 2 * (trade_count - index)
        for index in range(trade_count)
    ]
    fold["completed_trade_open_offset_seconds"] = [
        value - 1 for value in fold["completed_trade_close_offset_seconds"]
    ]
    position_ids = [
        "btp:" + _sha(
            f"capacity-position:{attempt_ordinal}:{fold_ordinal}:{index}"
        )
        for index in range(trade_count)
    ]
    fold["completed_trade_position_id_digests_base64"] = (
        winner._encode_position_id_digests(position_ids, "CAPACITY_POSITION_ID")
    )
    fold["completed_trade_event_order_anchor"] = 1_000_000_000
    fold["completed_trade_open_event_order_offsets"] = [
        0 if index == 0 else 1_147_000_000 + 2 * index
        for index in range(trade_count)
    ]
    fold["completed_trade_close_event_order_offsets"] = [
        1_147_000_001 + 2 * index for index in range(trade_count)
    ]
    _refresh_fold_accounting(fold, axis_row)
    return fold


def _capacity_request(
    total_trades: int,
    *,
    evaluated_attempt_count: int = winner.MAX_ATTEMPTS,
    long_fold_trade_counts: list[int] | None = None,
    fold_plan_override: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    assert 1 <= evaluated_attempt_count <= winner.MAX_ATTEMPTS
    request = _payload_request(
        "S" * 32,
        "M15",
        "BREAKOUT",
        fold_plan_override=_capacity_fold_plan() if fold_plan_override is None else fold_plan_override,
    )
    evidence = request["evidence_ledger_payload"]
    axes = evidence["shared_fold_axis"]
    attempts = [
        _capacity_attempt(
            ordinal,
            evaluated_attempt_count=evaluated_attempt_count,
        )
        for ordinal in range(winner.MAX_ATTEMPTS)
    ]
    short_fold_count = len(axes) - 1
    minimum = int(winner.PINNED_SELECTION_POLICY["min_completed_trades_per_fold"])
    short_trade_total = evaluated_attempt_count * short_fold_count * minimum
    if long_fold_trade_counts is None:
        long_quotient, long_remainder = divmod(
            total_trades - short_trade_total,
            evaluated_attempt_count,
        )
        long_fold_trade_counts = [
            long_quotient + int(ordinal < long_remainder)
            for ordinal in range(evaluated_attempt_count)
        ]
    assert len(long_fold_trade_counts) == evaluated_attempt_count
    assert short_trade_total + sum(long_fold_trade_counts) == total_trades
    assert all(
        minimum <= count <= winner.MAX_TRADES_PER_FOLD
        for count in long_fold_trade_counts
    )
    templates = [_fold(0, ordinal, axis) for ordinal, axis in enumerate(axes)]
    entries: list[dict[str, Any]] = []
    for attempt in attempts:
        ordinal = attempt["ordinal"]
        if attempt["precommitted_evidence_role"] == "PRESEARCH_PROCESS_REJECTED":
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
                    "cost_ladder_contract_sha256": attempt[
                        "cost_ladder_contract_sha256"
                    ],
                    "risk_unit_contract_sha256": attempt[
                        "risk_unit_contract_sha256"
                    ],
                    "evidence_status": "PROCESS_REJECTED",
                    "status_reason": "PRESEARCH_PROCESS_REJECTED",
                    "fold_count": 0,
                    "folds_sha256": _sha_json([]),
                    "folds": [],
                }
            )
            continue
        folds = []
        for fold_ordinal, axis in enumerate(axes):
            count = (
                minimum
                if fold_ordinal < short_fold_count
                else long_fold_trade_counts[ordinal]
            )
            folds.append(
                _capacity_fold(
                    templates[fold_ordinal],
                    axis,
                    attempt_ordinal=ordinal,
                    trade_count=count,
                )
            )
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
                "cost_ladder_contract_sha256": attempt[
                    "cost_ladder_contract_sha256"
                ],
                "risk_unit_contract_sha256": attempt["risk_unit_contract_sha256"],
                "evidence_status": "AUTHENTICATED",
                "status_reason": None,
                "fold_count": len(folds),
                "folds_sha256": _sha_json(folds),
                "folds": folds,
            }
        )
    request["attempt_ledger_payload"]["attempts"] = attempts
    evidence["entries"] = entries
    _refresh_complete_chain(request)
    for entry in evidence["entries"]:
        for fold_ordinal, fold in enumerate(entry["folds"]):
            _set_capacity_trade_lanes(fold, axes[fold_ordinal])
        _refresh_entry_hashes(entry)
    _refresh_evidence_hashes(request)
    return request


def _json_node_count(value: Any) -> int:
    count = 0
    stack = [value]
    while stack:
        current = stack.pop()
        count += 1
        if type(current) is dict:
            stack.extend(current.values())
        elif type(current) is list:
            stack.extend(current)
    return count


def _validate_capacity_transport(
    request: winner.WinnerSelectionRequest,
) -> tuple[datetime, list[dict[str, Any]], list[dict[str, Any]]]:
    (
        policy,
        policy_sha,
        attempt_payload,
        attempt_sha,
        evidence_payload,
        _evidence_sha,
        request_sha,
    ) = winner._request_payloads(request)
    (
        policy_time,
        cell,
        planned_attempts,
        fold_plan,
        latest_boundary,
    ) = winner._validate_precommitment(policy)
    frozen_at, attempts = winner._validate_attempt_ledger(
        attempt_payload,
        policy,
        policy_sha,
        policy_time,
        cell,
        planned_attempts,
    )
    return winner._validate_evidence_ledger(
        evidence_payload,
        policy,
        policy_sha,
        attempt_sha,
        frozen_at,
        cell,
        attempts,
        fold_plan,
        latest_boundary,
        datetime.now(timezone.utc),
        request_sha,
    )


def _append_capacity_trade(
    fold: dict[str, Any],
    axis_row: dict[str, Any],
) -> None:
    ids = _decoded_position_ids(fold)
    ids.append("btp:" + _sha("capacity-plus-one-position"))
    fold["completed_trade_position_id_digests_base64"] = (
        winner._encode_position_id_digests(ids, "CAPACITY_PLUS_ONE_POSITION")
    )
    fold_seconds = len(axis_row["daily_dates_utc"]) * 86_400
    fold["completed_trade_open_offset_seconds"].append(fold_seconds - 2)
    fold["completed_trade_close_offset_seconds"].append(fold_seconds - 1)
    fold["completed_trade_open_event_order_offsets"].append(1_147_483_646)
    fold["completed_trade_close_event_order_offsets"].append(1_147_483_647)
    _refresh_fold_accounting(fold, axis_row)


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
    shared_axis = evidence["shared_fold_axis"]
    attempts = attempt_ledger["attempts"]
    registries = {
        "process_registry_sha256": _process_registry(attempts),
        "horizon_registry_sha256": _horizon_registry(attempts),
        "trial_registry_sha256": _trial_registry(attempts),
    }
    for payload in (policy, attempt_ledger, evidence):
        payload.update(registries)
    policy["planned_attempt_count"] = len(attempts)
    attempt_ledger["attempt_count"] = len(attempts)
    attempt_ledger["attempts_sha256"] = _sha_json(attempts)
    evidence["attempt_count"] = len(attempts)
    fold_plan = [
        {field: axis[field] for field in winner._FOLD_PLAN_KEYS}
        for axis in shared_axis
    ]
    for fold_index, (planned, axis) in enumerate(
        zip(fold_plan, shared_axis, strict=True)
    ):
        partition = axis["shared_oos_partition"]
        partition_body = {
            key: value for key, value in partition.items() if key != "receipt_sha256"
        }
        partition_body["causal_fold_plan_row_sha256"] = _sha_json(planned)
        axis["shared_oos_partition"] = {
            **partition_body,
            "receipt_sha256": _sha_json(partition_body),
        }
        start = date.fromisoformat(axis["oos_start_date_utc"])
        end = date.fromisoformat(axis["oos_end_date_utc"])
        axis["daily_dates_utc"] = [
            (start + timedelta(days=index)).isoformat()
            for index in range((end - start).days + 1)
        ]
        axis["fold_ordinal"] = fold_index
    latest_boundary = datetime.combine(
        date.fromisoformat(fold_plan[-1]["oos_end_date_utc"]) + timedelta(days=1),
        datetime.min.time(),
        tzinfo=timezone.utc,
    )
    policy["planned_fold_count"] = len(fold_plan)
    policy["causal_fold_plan"] = fold_plan
    policy["causal_fold_plan_sha256"] = _sha_json(fold_plan)
    policy["latest_evidence_boundary_utc"] = _iso(latest_boundary)
    attempt_ledger["precommitment_payload_sha256"] = _sha_json(policy)
    attempt_sha = _sha_json(attempt_ledger)
    for entry in evidence["entries"]:
        for fold_index, fold in enumerate(entry["folds"]):
            _refresh_fold_accounting(fold, shared_axis[fold_index])
            if entry["evidence_status"] == "AUTHENTICATED":
                _bind_trade_power(
                    fold,
                    attempt_sha,
                    len(attempt_ledger["attempts"]),
                    shared_axis[fold_index],
                )
            else:
                fold["trade_power_binding"] = None
                _rehash_trade_vector(fold, shared_axis[fold_index])
        _refresh_entry_hashes(entry)
    access_time = latest_boundary + timedelta(days=1)
    evidence["precommitment_payload_sha256"] = _sha_json(policy)
    evidence["attempt_ledger_payload_sha256"] = attempt_sha
    evidence["latest_evidence_boundary_utc"] = _iso(latest_boundary)
    evidence["evidence_access_started_at_utc"] = _iso(access_time)
    evidence["evidence_access_completed_at_utc"] = _iso(access_time)
    evidence["generated_at_utc"] = _iso(access_time)
    evidence["shared_fold_axis_count"] = len(shared_axis)
    evidence["shared_fold_axis_sha256"] = _sha_json(shared_axis)
    evidence["entries_sha256"] = _sha_json(evidence["entries"])


def _make_authenticated_ladder_floor(
    entry: dict[str, Any],
    shared_axis: list[dict[str, Any]] | None = None,
) -> None:
    """Keep one genuine losing column so CSCV cannot be bypassed in fixtures."""

    axis = shared_axis or _shared_axis(_fold_plan())
    for fold_index, fold in enumerate(entry["folds"]):
        base = [
            round(-0.20 + 0.01 * math.sin((index + fold_index * 7) * 1.70), 12)
            for index in range(len(axis[fold_index]["daily_dates_utc"]))
        ]
        _set_daily_lanes(
            fold,
            base,
            [round(value - 0.01, 12) for value in base],
            [round(value - 0.02, 12) for value in base],
        )
        _refresh_fold_accounting(fold, axis[fold_index])
    _refresh_entry_hashes(entry)

def _refresh_evidence_hashes(request: dict[str, Any]) -> None:
    evidence = request["evidence_ledger_payload"]
    evidence["shared_fold_axis_count"] = len(evidence["shared_fold_axis"])
    evidence["shared_fold_axis_sha256"] = _sha_json(evidence["shared_fold_axis"])
    for entry in evidence["entries"]:
        _refresh_entry_hashes(entry)
    evidence["entries_sha256"] = _sha_json(evidence["entries"])


def _reject_evaluated_attempt(request: dict[str, Any], ordinal: int) -> None:
    evidence = request["evidence_ledger_payload"]
    entry = evidence["entries"][ordinal]
    entry["evidence_status"] = "PROCESS_REJECTED"
    entry["status_reason"] = "EVALUATED_OOS_PROCESS_REJECTED"
    for fold_index, fold in enumerate(entry["folds"]):
        fold["trade_power_binding"] = None
        _rehash_trade_vector(fold, evidence["shared_fold_axis"][fold_index])
    _refresh_evidence_hashes(request)


def _calibration_log_growth_folds(
    seed: int,
    days_per_fold: int,
    daily_sharpe: float,
    *,
    ar1: float = 0.0,
) -> list[list[float]]:
    generator = random.Random(seed)
    scale = winner.pinned_policy_snapshot()["primary_cell_risk_fraction"]
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


def test_v14_valid_request_selects_p95_and_emits_tiered_evidence() -> None:
    request = _payload_request()
    typed = _typed_request(request)
    decision = _evaluate(request)
    assert decision.status == "PROCESS_WINNER_PENDING_BOOK_MULTIPLICITY"
    assert decision.winner_process_candidate_id == _attempts()[0][
        "process_candidate_id"
    ]
    assert winner.verify_historical_cell_winner_decision(decision, typed)
    body = decision.to_dict()
    ladder = body["cost_ladder_selection"]
    assert ladder["selected_tier"] == "P95_PREFERENCE"
    assert ladder["p95_admitted_candidate_count"] == 2
    assert ladder["p75_required_decision"]["winner_process_candidate_id"]
    assert ladder["p95_preference_decision"]["winner_process_candidate_id"]
    assert "paired_comparisons" not in body
    for summary in body["candidate_summaries"][:2]:
        assert tuple(summary["lane_statistics"]) == ("BASE", "P75", "P95")
        assert tuple(summary["tier_statistics"]) == (
            "P75_REQUIRED",
            "P95_PREFERENCE",
        )
        for tier, lanes in winner._TIER_LANES.items():
            tier_lcb = summary["tier_statistics"][tier][
                winner._PRIMARY_UTILITY_FIELD
            ]
            assert tier_lcb == min(
                summary["lane_statistics"][lane][
                    "dependence_robust_lcb_daily_log_growth"
                ]
                for lane in lanes
            )
    marginal = winner.historical_book_marginal_receipt(decision, typed)
    assert marginal is not None
    assert marginal["contract_version"] == "fxpm.winner-book-marginal.v1"
    assert marginal["selected_cost_ladder_tier"] == "P95_PREFERENCE"
    assert marginal["validated_cost_ladder_tolerance"] == VALIDATED_P75_TOLERANCE
    assert set(marginal["validated_cost_ladder_tolerance"]) == set(
        VALIDATED_P75_TOLERANCE
    )
    assert marginal["within_cell_attempted_trials_exact"] == 3
    assert marginal["within_cell_multiplicity_adjusted"] is True
    assert body["live_authorized"] is False
    assert body["capital_sizing_authorized"] is False


def test_v14_schema_versions_and_policy_boundary_are_exact() -> None:
    assert winner.WINNER_SELECTION_CONTRACT_VERSION == "fxpm.winner-selection.v14"
    assert winner.WINNER_BOOK_MARGINAL_CONTRACT_VERSION == (
        "fxpm.winner-book-marginal.v1"
    )
    assert winner.WINNER_SELECTION_REQUEST_VERSION.endswith(".v2")
    assert winner.PRECOMMITMENT_VERSION.endswith(".v3")
    assert winner.ATTEMPT_LEDGER_VERSION.endswith(".v3")
    assert winner.EVIDENCE_LEDGER_VERSION == "fxpm.winner-evidence-ledger.v6"
    assert winner.MAX_TRADES_PER_FOLD == winner.MAX_CONTAINER_ITEMS == 4_096
    assert winner.MAX_TOTAL_COMPLETED_TRADES == 65_536
    assert winner.MAX_JSON_NODES == 1_000_000
    assert tuple(winner._SERIALIZED_DAILY_LANE_FIELDS.values()) == (
        "daily_net_mtm_r_e12_i64be_base64",
        "daily_net_mtm_r_p75_e12_i64be_base64",
        "daily_net_mtm_r_p95_e12_i64be_base64",
    )
    assert tuple(winner._SERIALIZED_TRADE_LANE_FIELDS.values()) == (
        "completed_trade_net_mtm_r_e12_i64be_base64",
        "completed_trade_net_mtm_r_p75_e12_i64be_base64",
        "completed_trade_net_mtm_r_p95_e12_i64be_base64",
    )
    assert winner.SHARED_OOS_PARTITION_VERSION.endswith(".v2")
    assert winner.TRADE_POWER_BINDING_VERSION.endswith(".v2")
    policy = winner.pinned_policy_snapshot()
    assert policy["policy_version"] == "fxpm.winner-statistical-policy.v11"
    assert policy["cost_ladder_lane_order"] == ["BASE", "P75", "P95"]
    assert policy["p75_required_lanes"] == ["BASE", "P75"]
    assert policy["p95_preference_lanes"] == ["BASE", "P75", "P95"]
    assert policy["pbo_contract_version"].endswith(".v3")
    assert policy["moving_block_bootstrap_contract_version"].endswith(".v2")


def test_book_marginal_predecessors_and_malformed_tolerance_fail_closed() -> None:
    request = _payload_request()
    typed = _typed_request(request)
    decision = _evaluate(request)
    assert winner.verify_historical_cell_winner_decision(decision, typed)
    genuine = decision.to_dict()
    genuine.pop("receipt_sha256")

    mutations = []
    missing_version = deepcopy(genuine)
    missing_version["historical_book_marginal_receipt"].pop("contract_version")
    mutations.append(missing_version)
    predecessor_version = deepcopy(genuine)
    predecessor_version["historical_book_marginal_receipt"]["contract_version"] = (
        "fxpm.winner-book-marginal.v0"
    )
    mutations.append(predecessor_version)
    missing_tolerance = deepcopy(genuine)
    missing_tolerance["historical_book_marginal_receipt"].pop(
        "validated_cost_ladder_tolerance"
    )
    mutations.append(missing_tolerance)
    fourth_tolerance_field = deepcopy(genuine)
    fourth_tolerance_field["historical_book_marginal_receipt"][
        "validated_cost_ladder_tolerance"
    ]["extra"] = "forbidden"
    mutations.append(fourth_tolerance_field)

    for mutated in mutations:
        encoded = _canonical(mutated)
        forged = winner.WinnerSelectionDecision(
            encoded, hashlib.sha256(encoded).hexdigest()
        )
        assert not winner.verify_historical_cell_winner_decision(forged, typed)
        assert winner.historical_book_marginal_receipt(forged, typed) is None


def test_winner_v14_primary_utility_and_inference_policy_are_exact() -> None:
    policy = winner.pinned_policy_snapshot()
    assert policy["policy_version"] == "fxpm.winner-statistical-policy.v11"
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


def test_per_fold_hac_lcbs_remain_diagnostic_only() -> None:
    request = _payload_request()
    evidence = request["evidence_ledger_payload"]
    axis = evidence["shared_fold_axis"]
    entries = evidence["entries"]
    for fold_index, fold in enumerate(entries[0]["folds"]):
        base = [
            round(
                0.02 + 0.15 * math.sin((index + fold_index * 7) * 1.70),
                12,
            )
            for index in range(len(axis[fold_index]["daily_dates_utc"]))
        ]
        _set_daily_lanes(
            fold,
            base,
            [round(value - 0.001, 12) for value in base],
            [round(value - 0.002, 12) for value in base],
        )
        _refresh_fold_accounting(fold, axis[fold_index])
    _refresh_entry_hashes(entries[0])
    _make_authenticated_ladder_floor(entries[1], axis)
    _refresh_evidence_hashes(request)

    decision = _evaluate(request)
    assert decision.status == "PROCESS_WINNER_PENDING_BOOK_MULTIPLICITY", (
        decision.reason_codes
    )
    summary = decision.to_dict()["candidate_summaries"][0]
    for tier in summary["tier_statistics"].values():
        assert tier["admissible"] is True
        assert not any("HAC_LCB" in code for code in tier["admissibility_failures"])
    assert summary["lane_statistics"]["BASE"]["worst_fold_mean_daily_r"] > 0.0
    assert summary["lane_statistics"]["P95"]["newest_fold_mean_daily_r"] > 0.0
    assert summary["lane_statistics"]["BASE"]["worst_fold_hac_lcb_daily_r"] < 0.0
    assert summary["lane_statistics"]["P95"]["worst_fold_hac_lcb_daily_r"] < 0.0


def test_fold_lcb_false_negative_power_regression_remains_pinned() -> None:
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
    assert policy["diagnostic_only_fields"] == [
        "worst_fold_hac_lcb_daily_r",
        "newest_fold_hac_lcb_daily_r",
        "p75_worst_fold_hac_lcb_daily_r",
        "p75_newest_fold_hac_lcb_daily_r",
        "p95_worst_fold_hac_lcb_daily_r",
        "p95_newest_fold_hac_lcb_daily_r",
        "tail_cvar_daily_r",
        "max_path_drawdown_r",
        "p75_tail_cvar_daily_r",
        "p75_max_path_drawdown_r",
        "p95_tail_cvar_daily_r",
        "p95_max_path_drawdown_r",
    ]


@pytest.mark.parametrize("seed", (1, 2))
def test_dsr_formal_power_calibration_counterexamples(seed: int) -> None:
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


def test_dsr_pinned_power_calibration_grid() -> None:
    policy = winner.pinned_policy_snapshot()
    assert policy["power_calibration_version"] == "fxpm.winner-power-calibration.v1"
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


def test_dsr_null_size_and_exact_zero_edge_are_controlled() -> None:
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


def test_dsr_trial_penalty_is_monotone_and_block_never_recharges_it() -> None:
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


def test_dsr_ar1_calibration_fails_insufficient_independent_evidence() -> None:
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
    result = winner._dependence_robust_inference(
        _calibration_log_growth_folds(6, 252, 0.10, ar1=0.90),
        None,
        100,
    )
    assert result["p"] > 0.05
    assert result["ess"] < policy["min_hac_effective_days"]
    assert result["maximum_detected_autocorrelation_lag_days"] >= 40


def test_sixty_day_memory_binds_precommitted_hac_plan() -> None:
    generator = random.Random(13)
    shocks = [generator.gauss(0.1, 1.0) for _ in range(24)]
    scale = winner.pinned_policy_snapshot()["primary_cell_risk_fraction"]
    folds = [
        [
            scale
            * (
                shocks[fold_index * 6 + day_index // 60]
                + 0.002 * math.sin((day_index + 1) * 1.371)
            )
            for day_index in range(360)
        ]
        for fold_index in range(4)
    ]
    flattened = [value for fold in folds for value in fold]
    naive = winner._fold_aware_inference(folds)
    assert winner._one_sided_dsr_p(flattened, naive["ess"], 1) < 0.05

    robust = winner._dependence_robust_inference(folds, None, 3)
    assert robust["primary_hac_p"] > 0.05
    assert robust["sensitivity_hac_p"] > 0.05
    assert robust["maximum_detected_autocorrelation_lag_days"] >= 50
    assert robust["precommitted_primary_block_days"] >= 50
    assert robust["precommitted_sensitivity_block_days"] >= 100
    assert robust["precommitted_minimum_block_count"] >= 4
    assert robust["precommitted_block_diagnostic_p"] > 0.05


def test_ar1_memory_cannot_evade_sensitivity_hac() -> None:
    folds = _calibration_log_growth_folds(6, 252, 0.10, ar1=0.90)
    assert min(sum(fold) / len(fold) for fold in folds) > 0.0
    flattened = [value for fold in folds for value in fold]
    naive = winner._fold_aware_inference(folds)
    assert winner._one_sided_dsr_p(flattened, naive["ess"], 1) < 0.05

    robust = winner._dependence_robust_inference(folds, None, 3)
    assert robust["primary_hac_p"] < 0.05
    assert robust["sensitivity_hac_p"] > 0.05
    assert robust["p"] > 0.05
    assert robust["maximum_detected_autocorrelation_lag_days"] >= 40
    assert robust["precommitted_sensitivity_block_days"] >= 80
    assert robust["precommitted_block_diagnostic_p"] > 0.05


def test_attempt_multiplicity_penalizes_identical_daily_evidence() -> None:
    values = [0.03 + 0.08 * math.sin(index * 1.7) for index in range(4 * 63)]
    effective = winner._fold_aware_inference(
        [values[index : index + 63] for index in range(0, len(values), 63)]
    )["ess"]
    one_attempt = winner._one_sided_dsr_p(values, effective, 1)
    many_attempts = winner._one_sided_dsr_p(values, effective, 128)
    assert 0.0 <= one_attempt < many_attempts <= 1.0


def test_retired_pbo_transport_and_singular_cost_names_are_deleted() -> None:
    source = inspect.getsource(winner)
    for retired in (
        "_PBO_FOLD_KEYS",
        "_validate_pbo_fold",
        "_pbo_fold_projection",
        "pbo_" + "folds",
        "pbo_" + "fold_count",
        "cost_" + "stress_contract_sha256",
        "daily_net_mtm_r_" + "cost_stress",
        "completed_trade_net_mtm_r_" + "cost_stress",
        "pbo_trade_vector_sha256",
    ):
        assert retired not in source
    for retired_field in (
        "daily_net_mtm_r_e12",
        "daily_net_mtm_r_p75_e12",
        "daily_net_mtm_r_p95_e12",
        "completed_trade_net_mtm_r_e12",
        "completed_trade_net_mtm_r_p75_e12",
        "completed_trade_net_mtm_r_p95_e12",
    ):
        assert f'"{retired_field}"' not in source


def test_shared_root_axis_is_single_and_attempt_folds_are_compact() -> None:
    request = _payload_request()
    evidence = request["evidence_ledger_payload"]
    assert evidence["shared_fold_axis_count"] == 4
    assert evidence["shared_fold_axis_sha256"] == _sha_json(
        evidence["shared_fold_axis"]
    )
    for entry in evidence["entries"]:
        for fold in entry["folds"]:
            assert frozenset(fold) == winner._FOLD_KEYS
            assert "daily_dates_utc" not in fold
            assert "shared_oos_partition" not in fold
            assert "daily_net_mtm_r" not in fold
            assert "completed_trade_open_utc" not in fold
            assert "completed_trade_position_ids" not in fold


def test_compact_fold_codecs_round_trip_exact_semantics() -> None:
    assert winner.R_QUANTUM == Decimal(1).scaleb(
        -winner.PINNED_RISK_UNIT_CONTRACT["r_decimal_places"]
    )
    assert winner._R_SCALE_DECIMAL == Decimal(1) / winner.R_QUANTUM
    assert RISK_UNIT_CONTRACT_SHA256 == _sha_json(
        dict(winner.PINNED_RISK_UNIT_CONTRACT)
    )
    values = [-100.0, -0.000000000001, 0.0, 1.234567890123, 100.0]
    encoded_values = [
        winner._encode_r_e12(value, winner.MAX_ABS_TRADE_R, "TEST_R")
        for value in values
    ]
    assert encoded_values == [
        -100_000_000_000_000,
        -1,
        0,
        1_234_567_890_123,
        100_000_000_000_000,
    ]
    assert [
        winner._decode_r_e12(value, winner.MAX_ABS_TRADE_R, "TEST_R")
        for value in encoded_values
    ] == values
    canonical_zero = winner._decode_r_e12(
        winner._encode_r_e12(0.0, winner.MAX_ABS_TRADE_R, "TEST_R"),
        winner.MAX_ABS_TRADE_R,
        "TEST_R",
    )
    assert math.copysign(1.0, canonical_zero) == 1.0
    with pytest.raises(ValueError, match="TEST_NON_GRID"):
        winner._encode_r_e12(
            1.0000000000005,
            winner.MAX_ABS_TRADE_R,
            "TEST_NON_GRID",
        )
    packed_r = winner._encode_r_e12_i64be_base64(
        [0, 1, -1, 100_000_000_000_000, -100_000_000_000_000],
        winner.MAX_ABS_TRADE_R,
        "TEST_PACKED_R",
    )
    assert packed_r == (
        "AAAAAAAAAAAAAAAAAAAAAf//////////AABa8xB6QAD//6UM74XAAA=="
    )
    assert winner._decode_r_e12_i64be_base64(
        packed_r,
        expected_count=5,
        maximum_abs_r=winner.MAX_ABS_TRADE_R,
        code="TEST_PACKED_R",
    ) == [0, 1, -1, 100_000_000_000_000, -100_000_000_000_000]


def test_unknown_risk_unit_contract_hash_fails_before_vector_decode() -> None:
    request = _payload_request()
    request["precommitment_payload"]["risk_unit_contract_sha256"] = "0" * 64

    decision = _evaluate(request)

    assert decision.reason_codes == ("RISK_UNIT_CONTRACT_MISMATCH",)


@pytest.mark.parametrize(
    "value",
    (
        "",
        "AAAAAAAAAA*=",
        "AAAAAAAAAAB=",
        base64.b64encode(struct.pack(">q", 50_000_000_000_001)).decode(
            "ascii"
        ),
    ),
)
def test_packed_r_codec_rejects_wrong_length_alphabet_canonicality_and_domain(
    value: str,
) -> None:
    with pytest.raises(ValueError, match="TEST_PACKED_R_INVALID"):
        winner._decode_r_e12_i64be_base64(
            value,
            expected_count=1,
            maximum_abs_r=winner.MAX_ABS_DAILY_R,
            code="TEST_PACKED_R_INVALID",
        )

    ids = ["btp:" + "00" * 32, "btp:" + "ff" * 32]
    encoded_ids = winner._encode_position_id_digests(ids, "TEST_POSITION_IDS")
    assert encoded_ids == (
        "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAD"
        "//////////////////////////////////////////w=="
    )
    assert winner._decode_position_id_digests(
        encoded_ids, 2, "TEST_POSITION_IDS"
    ) == ids

    axis = _shared_axis(_fold_plan())[0]
    fold_start, fold_end = _fold_bounds(axis)
    timestamps = [
        _iso(fold_start),
        _iso(fold_start + timedelta(days=62, seconds=86_399)),
    ]
    offsets = winner._encode_utc_second_offsets(
        timestamps,
        fold_start_utc=fold_start,
        fold_end_exclusive_utc=fold_end,
        code="TEST_TIME",
    )
    assert winner._decode_utc_second_offsets(
        offsets,
        fold_start_utc=fold_start,
        fold_end_exclusive_utc=fold_end,
        code="TEST_TIME",
    ) == timestamps
    anchor, opens, closes = winner._encode_event_order_offsets(
        [1_000_000_000, 2_147_000_000],
        [2_146_999_999, 2_147_000_001],
        "TEST_ORDER",
    )
    assert anchor == 1_000_000_000
    assert winner._decode_event_order_offsets(
        anchor, opens, closes, "TEST_ORDER"
    ) == ([1_000_000_000, 2_147_000_000], [2_146_999_999, 2_147_000_001])


def test_compact_fold_reconstructs_exact_selector_semantics() -> None:
    request = _payload_request()
    evidence = request["evidence_ledger_payload"]
    raw_fold = evidence["entries"][0]["folds"][0]
    axis = evidence["shared_fold_axis"][0]
    validated = winner._validate_fold(
        raw_fold,
        ordinal=0,
        shared_axis=axis,
        policy=request["precommitment_payload"],
        attempt_payload_sha=_sha_json(request["attempt_ledger_payload"]),
        attempted_trials=3,
        authenticated=True,
    )
    assert validated["daily_net_mtm_r"] == [
        winner._decode_r_e12(value, winner.MAX_ABS_DAILY_R, "TEST_R")
        for value in _unpacked_r_e12(
            raw_fold,
            "daily_net_mtm_r_e12_i64be_base64",
            expected_count=len(axis["daily_dates_utc"]),
            maximum_abs_r=winner.MAX_ABS_DAILY_R,
        )
    ]
    assert validated["completed_trade_position_ids"] == _decoded_position_ids(
        raw_fold
    )
    assert validated["completed_trade_open_utc"][0].endswith("Z")
    assert "daily_net_mtm_r_e12_i64be_base64" not in validated
    assert "completed_trade_position_id_digests_base64" not in validated


def test_predecessor_evidence_schema_and_retired_fold_shape_fail_closed() -> None:
    request = _payload_request()
    request["evidence_ledger_payload"]["contract_version"] = (
        "fxpm.winner-evidence-ledger.v5"
    )
    assert _evaluate(request).reason_codes == ("EVIDENCE_LEDGER_VERSION_INVALID",)

    request = _payload_request()
    evidence = request["evidence_ledger_payload"]
    fold = evidence["entries"][0]["folds"][0]
    packed = fold.pop("daily_net_mtm_r_e12_i64be_base64")
    fold["daily_net_mtm_r_e12"] = winner._decode_r_e12_i64be_base64(
        packed,
        expected_count=len(evidence["shared_fold_axis"][0]["daily_dates_utc"]),
        maximum_abs_r=winner.MAX_ABS_DAILY_R,
        code="TEST_RETIRED_LIST",
    )
    _refresh_evidence_hashes(request)
    assert _evaluate(request).reason_codes == ("FOLD_000_SCHEMA_INVALID",)


@pytest.mark.parametrize("mutation", ("LENGTH", "ALPHABET", "DOMAIN"))
def test_packed_daily_r_wire_tamper_fails_closed(mutation: str) -> None:
    request = _payload_request()
    evidence = request["evidence_ledger_payload"]
    fold = evidence["entries"][0]["folds"][0]
    field = "daily_net_mtm_r_e12_i64be_base64"
    if mutation == "LENGTH":
        fold[field] = fold[field][:-4]
    elif mutation == "ALPHABET":
        fold[field] = "*" + fold[field][1:]
    else:
        decoded = base64.b64decode(fold[field], validate=True)
        fold[field] = base64.b64encode(
            struct.pack(">q", 50_000_000_000_001) + decoded[8:]
        ).decode("ascii")
    _rehash_daily_ladder(fold, evidence["shared_fold_axis"][0])
    _refresh_evidence_hashes(request)
    assert _evaluate(request).reason_codes == (
        "FOLD_000_BASE_DAILY_R_I64BE_BASE64_INVALID",
    )


@pytest.mark.parametrize(
    ("field", "value", "expected_code"),
    (
        (
            "completed_trade_open_offset_seconds",
            -1,
            "TRADE_OPEN_OFFSET_INVALID",
        ),
        (
            "completed_trade_open_offset_seconds",
            63 * 86_400,
            "TRADE_OPEN_OFFSET_INVALID",
        ),
        (
            "completed_trade_open_event_order_offsets",
            2_147_483_648,
            "EVENT_ORDER_OFFSETS_INVALID",
        ),
    ),
)
def test_compact_scalar_bounds_fail_closed(
    field: str, value: Any, expected_code: str
) -> None:
    request = _payload_request()
    evidence = request["evidence_ledger_payload"]
    fold = evidence["entries"][0]["folds"][0]
    fold[field][0] = value
    _rehash_trade_vector(fold, evidence["shared_fold_axis"][0])
    _refresh_evidence_hashes(request)
    decision = _evaluate(request)
    assert decision.reason_codes == (f"FOLD_000_{expected_code}",)


def test_compact_order_and_base64_encodings_are_unique_and_canonical() -> None:
    request = _payload_request()
    evidence = request["evidence_ledger_payload"]
    fold = evidence["entries"][0]["folds"][0]
    fold["completed_trade_open_event_order_offsets"] = [
        value + 1 for value in fold["completed_trade_open_event_order_offsets"]
    ]
    fold["completed_trade_close_event_order_offsets"] = [
        value + 1 for value in fold["completed_trade_close_event_order_offsets"]
    ]
    _rehash_trade_vector(fold, evidence["shared_fold_axis"][0])
    _refresh_evidence_hashes(request)
    assert _evaluate(request).reason_codes == (
        "FOLD_000_EVENT_ORDER_OFFSETS_INVALID",
    )

    one_id = winner._encode_position_id_digests(
        ["btp:" + "00" * 32], "TEST_POSITION_IDS"
    )
    assert one_id.endswith("=")
    noncanonical = one_id[:-2] + ("B" if one_id[-2] == "A" else "A") + "="
    with pytest.raises(ValueError, match="TEST_POSITION_IDS"):
        winner._decode_position_id_digests(
            noncanonical, 1, "TEST_POSITION_IDS"
        )
    with pytest.raises(ValueError, match="TEST_POSITION_IDS"):
        winner._decode_position_id_digests(
            one_id[:-1] + "_", 1, "TEST_POSITION_IDS"
        )

    request = _payload_request()
    evidence = request["evidence_ledger_payload"]
    fold = evidence["entries"][0]["folds"][0]
    ids = _decoded_position_ids(fold)
    raw = b"".join(
        bytes.fromhex((ids[0] if index == 1 else value)[4:])
        for index, value in enumerate(ids)
    )
    fold["completed_trade_position_id_digests_base64"] = base64.b64encode(
        raw
    ).decode("ascii")
    _rehash_trade_vector(fold, evidence["shared_fold_axis"][0])
    _refresh_evidence_hashes(request)
    assert _evaluate(request).reason_codes == ("FOLD_000_POSITION_ID_DUPLICATED",)


def test_subsecond_timestamp_cannot_enter_compact_transport() -> None:
    axis = _shared_axis(_fold_plan())[0]
    fold_start, fold_end = _fold_bounds(axis)
    with pytest.raises(ValueError, match="TEST_SUBSECOND"):
        winner._encode_utc_second_offsets(
            ["2021-02-01T00:00:00.000001Z"],
            fold_start_utc=fold_start,
            fold_end_exclusive_utc=fold_end,
            code="TEST_SUBSECOND",
        )


def test_compact_complete_root_capacity_is_bounded_at_projected_and_hard_cap() -> None:
    projected_request = _typed_request(
        _capacity_request(
            56_640,
            evaluated_attempt_count=winner.MAX_ATTEMPTS - 1,
        )
    )
    projected_body = projected_request.to_dict()
    projected_bytes = len(_canonical(projected_body))
    projected_nodes = _json_node_count(projected_body)
    assert (projected_bytes, projected_nodes) == (15_457_553, 279_094)
    assert projected_bytes <= winner.MAX_REQUEST_BYTES
    assert projected_nodes <= winner.MAX_JSON_NODES
    _validate_capacity_transport(projected_request)

    # Four base64 trade vectors are widest when 299 long-fold counts have
    # residue 2 modulo 3 and the remaining count has residue 0.
    padding_worst_long_counts = [206] * 45 + [203] * 254 + [204]
    at_cap_payload = _capacity_request(
        winner.MAX_TOTAL_COMPLETED_TRADES,
        long_fold_trade_counts=padding_worst_long_counts,
    )
    at_cap_request = _typed_request(at_cap_payload)
    at_cap_body = at_cap_request.to_dict()
    at_cap_bytes = len(_canonical(at_cap_body))
    at_cap_nodes = _json_node_count(at_cap_body)
    assert (at_cap_bytes, at_cap_nodes) == (16_507_724, 314_818)
    assert at_cap_bytes <= winner.MAX_REQUEST_BYTES
    assert at_cap_nodes <= winner.MAX_JSON_NODES
    _validate_capacity_transport(at_cap_request)
    at_cap_decision = winner.evaluate_historical_cell_winner(at_cap_request)
    assert at_cap_decision.status == "NO_PROCESS_WINNER"
    assert at_cap_decision.reason_codes == (
        "NO_P75_ADMISSIBLE_PROCESS_CANDIDATE",
    )

    evidence = at_cap_payload["evidence_ledger_payload"]
    last_entry = evidence["entries"][-1]
    last_fold = last_entry["folds"][-1]
    _append_capacity_trade(last_fold, evidence["shared_fold_axis"][-1])
    _refresh_entry_hashes(last_entry)
    _refresh_evidence_hashes(at_cap_payload)
    with pytest.raises(ValueError, match="GLOBAL_COMPLETED_TRADE_LIMIT_EXCEEDED"):
        _validate_capacity_transport(_typed_request(at_cap_payload))


def test_per_fold_trade_width_accepts_4096_and_rejects_4097_canonically() -> None:
    request = _payload_request()
    evidence = request["evidence_ledger_payload"]
    axis = evidence["shared_fold_axis"][0]
    entry = evidence["entries"][0]
    fold = _capacity_fold(
        entry["folds"][0],
        axis,
        attempt_ordinal=entry["ordinal"],
        trade_count=winner.MAX_TRADES_PER_FOLD,
    )
    _set_capacity_trade_lanes(fold, axis)
    entry["folds"][0] = fold
    _refresh_entry_hashes(entry)
    _refresh_evidence_hashes(request)

    accepted = _typed_request(request)
    _validate_capacity_transport(accepted)

    fold["completed_trade_open_offset_seconds"].append(0)
    assert winner.create_winner_selection_request(
        request["precommitment_payload"],
        request["attempt_ledger_payload"],
        request["evidence_ledger_payload"],
    ) is None


def test_componentwise_daily_improvement_is_allowed_when_prefixes_degrade() -> None:
    request = _payload_request()
    evidence = request["evidence_ledger_payload"]
    fold = evidence["entries"][0]["folds"][0]
    day_count = len(evidence["shared_fold_axis"][0]["daily_dates_utc"])
    base = _unpacked_r_e12(
        fold,
        "daily_net_mtm_r_e12_i64be_base64",
        expected_count=day_count,
        maximum_abs_r=winner.MAX_ABS_DAILY_R,
    )
    p75 = _unpacked_r_e12(
        fold,
        "daily_net_mtm_r_p75_e12_i64be_base64",
        expected_count=day_count,
        maximum_abs_r=winner.MAX_ABS_DAILY_R,
    )
    p75[0] -= 100_000_000_000
    p75[1] += 50_000_000_000
    assert p75[1] > base[1]
    _set_packed_r_e12(
        fold,
        "daily_net_mtm_r_p75_e12_i64be_base64",
        p75,
        winner.MAX_ABS_DAILY_R,
    )
    _set_packed_r_e12(
        fold,
        "daily_net_mtm_r_p95_e12_i64be_base64",
        [value - 10_000_000_000 for value in p75],
        winner.MAX_ABS_DAILY_R,
    )
    _refresh_fold_accounting(fold, evidence["shared_fold_axis"][0])
    _refresh_evidence_hashes(request)
    assert _evaluate(request).status != "REJECTED"


@pytest.mark.parametrize(
    ("lane", "prior_lane", "expected_code"),
    (
        (
            "daily_net_mtm_r_p75_e12_i64be_base64",
            "daily_net_mtm_r_e12_i64be_base64",
            "P75_CUMULATIVE_PREFIX",
        ),
        (
            "daily_net_mtm_r_p95_e12_i64be_base64",
            "daily_net_mtm_r_p75_e12_i64be_base64",
            "P95_CUMULATIVE_PREFIX",
        ),
    ),
)
def test_adjacent_cumulative_prefix_improvement_fails_closed(
    lane: str, prior_lane: str, expected_code: str
) -> None:
    request = _payload_request()
    evidence = request["evidence_ledger_payload"]
    fold = evidence["entries"][0]["folds"][0]
    day_count = len(evidence["shared_fold_axis"][0]["daily_dates_utc"])
    lane_values = _unpacked_r_e12(
        fold,
        lane,
        expected_count=day_count,
        maximum_abs_r=winner.MAX_ABS_DAILY_R,
    )
    prior_values = _unpacked_r_e12(
        fold,
        prior_lane,
        expected_count=day_count,
        maximum_abs_r=winner.MAX_ABS_DAILY_R,
    )
    lane_values[0] = prior_values[0] + 10_000_000_000
    _set_packed_r_e12(
        fold,
        lane,
        lane_values,
        winner.MAX_ABS_DAILY_R,
    )
    _rehash_daily_ladder(fold, evidence["shared_fold_axis"][0])
    _refresh_evidence_hashes(request)
    decision = _evaluate(request)
    assert decision.status == "REJECTED"
    assert expected_code in decision.reason_codes[0]


@pytest.mark.parametrize(
    ("lane", "prior_lane", "expected_code"),
    (
        (
            "completed_trade_net_mtm_r_p75_e12_i64be_base64",
            "completed_trade_net_mtm_r_e12_i64be_base64",
            "P75_IMPROVES_TRADE_RETURN",
        ),
        (
            "completed_trade_net_mtm_r_p95_e12_i64be_base64",
            "completed_trade_net_mtm_r_p75_e12_i64be_base64",
            "P95_IMPROVES_TRADE_RETURN",
        ),
    ),
)
def test_adjacent_completed_trade_improvement_fails_closed(
    lane: str, prior_lane: str, expected_code: str
) -> None:
    request = _payload_request()
    evidence = request["evidence_ledger_payload"]
    fold = evidence["entries"][0]["folds"][0]
    trade_count = len(fold["completed_trade_open_offset_seconds"])
    lane_values = _unpacked_r_e12(
        fold,
        lane,
        expected_count=trade_count,
        maximum_abs_r=winner.MAX_ABS_TRADE_R,
    )
    prior_values = _unpacked_r_e12(
        fold,
        prior_lane,
        expected_count=trade_count,
        maximum_abs_r=winner.MAX_ABS_TRADE_R,
    )
    delta = prior_values[0] + 10_000_000_000 - lane_values[0]
    lane_values[0] += delta
    lane_values[1] -= delta
    _set_packed_r_e12(
        fold,
        lane,
        lane_values,
        winner.MAX_ABS_TRADE_R,
    )
    _rehash_trade_vector(fold, evidence["shared_fold_axis"][0])
    _refresh_evidence_hashes(request)
    decision = _evaluate(request)
    assert decision.status == "REJECTED"
    assert decision.reason_codes == (f"FOLD_000_{expected_code}",)


def test_three_lane_flat_to_flat_accounting_is_exact() -> None:
    request = _payload_request()
    evidence = request["evidence_ledger_payload"]
    fold = evidence["entries"][0]["folds"][0]
    field = "completed_trade_net_mtm_r_p95_e12_i64be_base64"
    values = _unpacked_r_e12(
        fold,
        field,
        expected_count=len(fold["completed_trade_open_offset_seconds"]),
        maximum_abs_r=winner.MAX_ABS_TRADE_R,
    )
    values[0] -= 10_000_000_000
    _set_packed_r_e12(fold, field, values, winner.MAX_ABS_TRADE_R)
    _rehash_trade_vector(fold, evidence["shared_fold_axis"][0])
    _refresh_evidence_hashes(request)
    decision = _evaluate(request)
    assert decision.reason_codes == (
        "FOLD_000_P95_FLAT_TO_FLAT_ACCOUNTING_MISMATCH",
    )


def test_shared_axis_hash_tamper_fails_before_selection() -> None:
    request = _payload_request()
    evidence = request["evidence_ledger_payload"]
    evidence["shared_fold_axis"][0]["daily_dates_utc"][0] = "2021-01-31"
    decision = _evaluate(request)
    assert decision.reason_codes == ("SHARED_FOLD_AXIS_BINDING_INVALID",)


def test_shared_partition_splice_across_folds_fails_closed() -> None:
    request = _payload_request()
    evidence = request["evidence_ledger_payload"]
    evidence["shared_fold_axis"][1]["shared_oos_partition"] = deepcopy(
        evidence["shared_fold_axis"][0]["shared_oos_partition"]
    )
    evidence["shared_fold_axis_sha256"] = _sha_json(evidence["shared_fold_axis"])
    decision = _evaluate(request)
    assert decision.status == "REJECTED"
    assert "CAUSAL_FOLD_PLAN_ROW_MISMATCH" in decision.reason_codes[0]


def test_evaluated_process_rejection_retains_complete_unified_folds() -> None:
    request = _payload_request()
    _reject_evaluated_attempt(request, 2)
    decision = _evaluate(request)
    assert decision.status == "PROCESS_WINNER_PENDING_BOOK_MULTIPLICITY"
    body = decision.to_dict()
    rejected = body["candidate_summaries"][2]
    assert rejected["evidence_status"] == "PROCESS_REJECTED"
    assert rejected["lane_statistics"] == {}
    assert (
        body["cell_pbo_evidence"]["evaluation_required_candidate_count"] == 3
    )


def test_evaluated_process_rejection_cannot_drop_its_fold_matrix() -> None:
    request = _payload_request()
    _reject_evaluated_attempt(request, 2)
    entry = request["evidence_ledger_payload"]["entries"][2]
    entry["folds"] = []
    _refresh_evidence_hashes(request)
    decision = _evaluate(request)
    assert decision.reason_codes == (
        "EVIDENCE_0002_PRECOMMITTED_FOLD_ROSTER_MISMATCH",
    )


def test_only_presearch_rejection_may_have_no_folds() -> None:
    request = _payload_request()
    attempt = request["attempt_ledger_payload"]["attempts"][2]
    attempt["precommitted_evidence_role"] = "PRESEARCH_PROCESS_REJECTED"
    entry = request["evidence_ledger_payload"]["entries"][2]
    entry["evidence_status"] = "PROCESS_REJECTED"
    entry["status_reason"] = "PRESEARCH_PROCESS_REJECTED"
    entry["folds"] = []
    _refresh_complete_chain(request)
    decision = _evaluate(request)
    assert decision.status != "REJECTED"
    assert (
        decision.to_dict()["cell_pbo_evidence"][
            "presearch_rejected_attempt_count"
        ]
        == 1
    )


def test_authenticated_fold_requires_exact_trade_power_binding() -> None:
    request = _payload_request()
    evidence = request["evidence_ledger_payload"]
    fold = evidence["entries"][0]["folds"][0]
    fold["trade_power_binding"] = None
    _rehash_trade_vector(fold, evidence["shared_fold_axis"][0])
    _refresh_evidence_hashes(request)
    assert _evaluate(request).reason_codes == (
        "FOLD_000_TRADE_POWER_BINDING_SCHEMA_INVALID",
    )


def test_trade_power_binding_must_match_exact_attempt_ledger() -> None:
    request = _payload_request()
    evidence = request["evidence_ledger_payload"]
    entry = evidence["entries"][0]
    fold = entry["folds"][0]
    binding_body = {
        key: value
        for key, value in fold["trade_power_binding"].items()
        if key != "binding_sha256"
    }
    binding_body["attempted_trials_exact"] = 2
    fold["trade_power_binding"] = {
        **binding_body,
        "binding_sha256": _sha_json(binding_body),
    }
    _rehash_trade_vector(fold, evidence["shared_fold_axis"][0])
    _refresh_entry_hashes(entry)
    _refresh_evidence_hashes(request)

    decision = _evaluate(request)
    assert decision.status == "REJECTED"
    assert decision.reason_codes == ("FOLD_000_TRADE_POWER_BINDING_INVALID",)


def test_overlapping_positions_cap_completed_trade_power() -> None:
    request = _payload_request()
    evidence = request["evidence_ledger_payload"]
    entry = evidence["entries"][0]
    for fold_index, fold in enumerate(entry["folds"]):
        fold["completed_trade_open_offset_seconds"] = [
            0 for _ in fold["completed_trade_open_offset_seconds"]
        ]
        _rehash_trade_vector(fold, evidence["shared_fold_axis"][fold_index])
    _refresh_entry_hashes(entry)
    _refresh_evidence_hashes(request)

    summary = _evaluate(request).to_dict()["candidate_summaries"][0]
    assert summary["completed_trade_count"] == 480
    assert summary["completed_trade_overlap_component_count"] == 4
    for lane in summary["lane_statistics"].values():
        assert lane["completed_trade_hac_effective_count"] == 4.0
    for tier in summary["tier_statistics"].values():
        assert "INSUFFICIENT_COMPLETED_TRADE_HAC_POWER" in tier[
            "admissibility_failures"
        ]


def test_zero_p95_admissions_is_the_only_p75_fallback() -> None:
    request = _payload_request()
    evidence = request["evidence_ledger_payload"]
    for entry in evidence["entries"]:
        for fold_index, fold in enumerate(entry["folds"]):
            day_count = len(
                evidence["shared_fold_axis"][fold_index]["daily_dates_utc"]
            )
            p75 = _unpacked_r_e12(
                fold,
                "daily_net_mtm_r_p75_e12_i64be_base64",
                expected_count=day_count,
                maximum_abs_r=winner.MAX_ABS_DAILY_R,
            )
            _set_packed_r_e12(
                fold,
                "daily_net_mtm_r_p95_e12_i64be_base64",
                [
                value - 200_000_000_000
                    for value in p75
                ],
                winner.MAX_ABS_DAILY_R,
            )
            _refresh_fold_accounting(
                fold, evidence["shared_fold_axis"][fold_index]
            )
    _refresh_evidence_hashes(request)
    decision = _evaluate(request)
    assert decision.status == "PROCESS_WINNER_PENDING_BOOK_MULTIPLICITY"
    ladder = decision.to_dict()["cost_ladder_selection"]
    assert ladder["p95_admitted_candidate_count"] == 0
    assert ladder["selected_tier"] == "P75_REQUIRED"
    assert decision.reason_codes == ("P95_ZERO_ADMITTED_P75_FALLBACK",)
    marginal = _marginal(decision, request)
    assert marginal is not None
    assert marginal["selected_cost_ladder_tier"] == "P75_REQUIRED"
    assert marginal["validated_cost_ladder_tolerance"] == VALIDATED_P75_TOLERANCE


@pytest.mark.parametrize("selected_tier", ["P75_REQUIRED", "P95_PREFERENCE"])
def test_dominant_winner_marginal_keeps_selected_identity(
    selected_tier: str,
) -> None:
    request = _payload_request()
    evidence = request["evidence_ledger_payload"]
    for candidate_index, entry in enumerate(evidence["entries"]):
        for fold_index, fold in enumerate(entry["folds"]):
            axis = evidence["shared_fold_axis"][fold_index]
            day_count = len(axis["daily_dates_utc"])
            if candidate_index == 0:
                # Synthetic higher-growth evidence with more variance than beta.
                base = [
                    round(
                        0.25
                        + 0.20 * math.sin((index + fold_index * 7) * 1.70)
                        + 0.012 * math.sin((index + fold_index * 3) * 0.77),
                        12,
                    )
                    for index in range(day_count)
                ]
                _set_daily_lanes(
                    fold,
                    base,
                    [round(value - 0.010, 12) for value in base],
                    [round(value - 0.020, 12) for value in base],
                )
            if selected_tier == "P75_REQUIRED":
                p75 = _unpacked_r_e12(
                    fold,
                    "daily_net_mtm_r_p75_e12_i64be_base64",
                    expected_count=day_count,
                    maximum_abs_r=winner.MAX_ABS_DAILY_R,
                )
                _set_packed_r_e12(
                    fold,
                    "daily_net_mtm_r_p95_e12_i64be_base64",
                    [value - 500_000_000_000 for value in p75],
                    winner.MAX_ABS_DAILY_R,
                )
            _refresh_fold_accounting(fold, axis)
    _refresh_evidence_hashes(request)

    decision = _evaluate(request)
    body = decision.to_dict()
    selected, loser = body["candidate_summaries"][:2]
    selected_stats = selected["tier_statistics"][selected_tier]
    loser_stats = loser["tier_statistics"][selected_tier]
    assert selected_stats["admissible"] is True
    assert loser_stats["admissible"] is True
    assert loser_stats["within_cell_adjusted_p_value"] < selected_stats[
        "within_cell_adjusted_p_value"
    ]
    assert selected_stats[
        "conservative_primary_utility_lcb_daily_log_growth"
    ] > loser_stats["conservative_primary_utility_lcb_daily_log_growth"]
    assert decision.status == "PROCESS_WINNER_PENDING_BOOK_MULTIPLICITY"
    assert decision.winner_process_candidate_id == selected["process_candidate_id"]
    ladder = body["cost_ladder_selection"]
    assert ladder["selected_tier"] == selected_tier
    tier_decision = ladder[
        "p75_required_decision"
        if selected_tier == "P75_REQUIRED"
        else "p95_preference_decision"
    ]
    assert tier_decision["reason_codes"] == [
        "UNIQUE_SIGNIFICANT_PRIMARY_UTILITY_NONDOMINATED_PROCESS"
    ]

    marginal = _marginal(decision, request)
    assert marginal is not None
    assert marginal["evidence_candidate_id"] == selected["process_candidate_id"]
    assert marginal["process_sha256"] == selected["process_sha256"]
    assert marginal["marginal_p_value"] == selected_stats[
        "within_cell_adjusted_p_value"
    ]
    assert tier_decision["reporting_process_candidate_id"] == selected[
        "process_candidate_id"
    ]


def test_nonempty_p95_pbo_failure_never_falls_back(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original = winner._cell_pbo_evidence

    def failed_p95(*args: Any, **kwargs: Any) -> dict[str, Any]:
        evidence = original(*args, **kwargs)
        evidence["tier_evidence"]["P95_PREFERENCE"].update(
            {
                "status": "FAIL",
                "passed": False,
                "reason_code": "TIER_LANE_PBO_EXCEEDS_LIMIT",
            }
        )
        return evidence

    monkeypatch.setattr(winner, "_cell_pbo_evidence", failed_p95)
    request = _payload_request()
    decision = _evaluate(request)
    ladder = decision.to_dict()["cost_ladder_selection"]
    assert ladder["p95_admitted_candidate_count"] > 0
    assert ladder["selected_tier"] is None
    assert decision.status == "NO_PROCESS_WINNER"
    assert decision.reason_codes == (
        "P95_CELL_PBO_EXCEEDS_PRECOMMITTED_LIMIT",
    )
    marginal = _marginal(decision, request)
    assert marginal is not None
    assert marginal["selected_cost_ladder_tier"] is None
    assert marginal["validated_cost_ladder_tolerance"] is None


def test_p95_admission_count_survives_p75_pbo_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original = winner._cell_pbo_evidence

    def failed_p75(*args: Any, **kwargs: Any) -> dict[str, Any]:
        evidence = original(*args, **kwargs)
        evidence["tier_evidence"]["P75_REQUIRED"].update(
            {
                "status": "FAIL",
                "passed": False,
                "reason_code": "TIER_LANE_PBO_EXCEEDS_LIMIT",
            }
        )
        return evidence

    monkeypatch.setattr(winner, "_cell_pbo_evidence", failed_p75)
    decision = _evaluate(_payload_request())
    ladder = decision.to_dict()["cost_ladder_selection"]
    assert decision.status == "NO_PROCESS_WINNER"
    assert ladder["p95_admitted_candidate_count"] == 2
    assert ladder["p95_preference_decision"]["admitted_candidate_count"] == 2
    assert ladder["p95_preference_decision"]["status"] == "NOT_EVALUATED"


def test_nonempty_p95_unresolved_set_never_falls_back(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original = winner._resolve_tier

    def unresolved_p95(*args: Any, **kwargs: Any) -> tuple[Any, ...]:
        if kwargs["tier"] != "P95_PREFERENCE":
            return original(*args, **kwargs)
        admitted = kwargs["admissible"]
        reporting = winner._reporting_candidate(admitted, "P95_PREFERENCE")
        return (
            {
                "status": "UNRESOLVED_NONDOMINATED_SET",
                "reason_codes": [
                    "STATISTICALLY_DIFFERENT_PRIMARY_UTILITY_ORDER_CONFLICT"
                ],
                "admitted_candidate_count": len(admitted),
                "winner_process_candidate_id": None,
                "reporting_process_candidate_id": reporting[
                    "process_candidate_id"
                ],
                "paired_comparisons": [],
            },
            None,
            reporting,
        )

    monkeypatch.setattr(winner, "_resolve_tier", unresolved_p95)
    decision = _evaluate(_payload_request())
    ladder = decision.to_dict()["cost_ladder_selection"]
    assert ladder["p95_admitted_candidate_count"] > 0
    assert ladder["selected_tier"] is None
    assert decision.status == "UNRESOLVED_NONDOMINATED_SET"
    assert decision.reason_codes == (
        "P95_STATISTICALLY_DIFFERENT_PRIMARY_UTILITY_ORDER_CONFLICT",
    )


def test_p95_preference_can_select_a_different_p75_admitted_winner(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original = winner._resolve_tier

    def divergent_p95(*args: Any, **kwargs: Any) -> tuple[Any, ...]:
        decision, selected, reporting = original(*args, **kwargs)
        if kwargs["tier"] != "P95_PREFERENCE":
            return decision, selected, reporting
        chosen = kwargs["admissible"][-1]
        return (
            {
                **decision,
                "status": "PROCESS_WINNER_PENDING_BOOK_MULTIPLICITY",
                "reason_codes": ["FORCED_TEST_P95_PREFERENCE"],
                "winner_process_candidate_id": chosen["process_candidate_id"],
                "reporting_process_candidate_id": chosen[
                    "process_candidate_id"
                ],
            },
            chosen,
            chosen,
        )

    monkeypatch.setattr(winner, "_resolve_tier", divergent_p95)
    decision = _evaluate(_payload_request())
    ladder = decision.to_dict()["cost_ladder_selection"]
    assert ladder["selected_tier"] == "P95_PREFERENCE"
    assert ladder["p75_p95_winner_diverged"] is True
    assert (
        decision.winner_process_candidate_id
        == ladder["p95_preference_decision"]["winner_process_candidate_id"]
    )


def test_raw_three_lane_pair_inference_runs_once_per_unordered_pair(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[str, str]] = []
    original = winner._raw_pair_inference

    def counted(
        left: dict[str, Any],
        right: dict[str, Any],
        entries: list[dict[str, Any]],
        **kwargs: Any,
    ) -> dict[str, Any]:
        calls.append(
            (left["process_candidate_id"], right["process_candidate_id"])
        )
        return original(left, right, entries, **kwargs)

    monkeypatch.setattr(winner, "_raw_pair_inference", counted)
    body = _evaluate(_payload_request()).to_dict()
    assert calls == [
        (
            _attempts()[0]["process_candidate_id"],
            _attempts()[1]["process_candidate_id"],
        )
    ]
    p75_comparison = body["cost_ladder_selection"][
        "p75_required_decision"
    ]["paired_comparisons"][0]
    p95_comparison = body["cost_ladder_selection"][
        "p95_preference_decision"
    ]["paired_comparisons"][0]
    assert p75_comparison["lanes"] == ["BASE", "P75"]
    assert p95_comparison["lanes"] == ["BASE", "P75", "P95"]
    assert p75_comparison is not p95_comparison


def test_cell_pbo_computes_each_lane_once(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[str] = []
    original = winner._pbo_lane_evidence

    def counted(*args: Any, **kwargs: Any) -> dict[str, Any]:
        calls.append(kwargs["lane"])
        return original(*args, **kwargs)

    monkeypatch.setattr(winner, "_pbo_lane_evidence", counted)
    _evaluate(_payload_request())
    assert calls == ["BASE", "P75", "P95"]


def test_moving_block_bootstrap_is_lane_once_then_tier_composed() -> None:
    body = _evaluate(_payload_request()).to_dict()
    evidence = body["candidate_summaries"][0][
        "moving_block_bootstrap_evidence"
    ]
    assert tuple(evidence["lane_evidence"]) == ("BASE", "P75", "P95")
    assert tuple(evidence["tier_evidence"]) == (
        "P75_REQUIRED",
        "P95_PREFERENCE",
    )
    assert evidence["resampled_block_draw_count"] == sum(
        lane["resampled_block_draw_count"]
        for lane in evidence["lane_evidence"].values()
    )


def test_operation_contracts_cover_full_ladder_without_double_counting() -> None:
    lengths = [63, 63, 63, 381]
    daily = winner._dependence_operation_count(lengths, [1, 1, 1, 1])
    trade = winner._dependence_operation_count([5, 5, 5, 25])
    assert 3 * daily + trade == 111_669
    assert 299 * (3 * daily + trade) == 33_389_031
    assert 6 * (300 * 299 // 2) * daily == 9_983_610_000
    assert winner.MAX_BOOTSTRAP_BLOCK_DRAWS == 3 * 2 * 1024 * 76 * 300
    assert 3 * 2 * 1024 * 76 * 299 == 139_616_256


def test_pair_operation_overflow_is_unresolved_not_rejected(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original = winner._pair_operation_diagnostic

    def exceeded(*args: Any, **kwargs: Any) -> dict[str, int]:
        diagnostic = original(*args, **kwargs)
        return {**diagnostic,
                "expected_dependence_operations": diagnostic["operation_limit"] + 1}

    monkeypatch.setattr(winner, "_pair_operation_diagnostic", exceeded)
    decision = _evaluate(_payload_request())
    assert decision.status == "UNRESOLVED_NONDOMINATED_SET"
    assert decision.reason_codes == (
        "P75_PAIRWISE_DEPENDENCE_PRODUCT_EXCEEDS_BOUNDED_EVALUATION",
    )
    ladder = decision.to_dict()["cost_ladder_selection"]
    assert ladder["p95_admitted_candidate_count"] == 2
    assert ladder["p95_preference_decision"]["admitted_candidate_count"] == 2


def test_p95_admission_count_survives_p75_pairwise_nonresolution(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original = winner._resolve_tier

    def unresolved_p75(*args: Any, **kwargs: Any) -> tuple[Any, ...]:
        if kwargs["tier"] != "P75_REQUIRED":
            return original(*args, **kwargs)
        admitted = kwargs["admissible"]
        reporting = winner._reporting_candidate(admitted, "P75_REQUIRED")
        return (
            {
                "status": "UNRESOLVED_NONDOMINATED_SET",
                "reason_codes": [
                    "STATISTICALLY_DIFFERENT_PRIMARY_UTILITY_ORDER_CONFLICT"
                ],
                "admitted_candidate_count": len(admitted),
                "winner_process_candidate_id": None,
                "reporting_process_candidate_id": reporting[
                    "process_candidate_id"
                ],
                "paired_comparisons": [],
            },
            None,
            reporting,
        )

    monkeypatch.setattr(winner, "_resolve_tier", unresolved_p75)
    decision = _evaluate(_payload_request())
    ladder = decision.to_dict()["cost_ladder_selection"]
    assert decision.status == "UNRESOLVED_NONDOMINATED_SET"
    assert ladder["p95_admitted_candidate_count"] == 2
    assert ladder["p95_preference_decision"]["admitted_candidate_count"] == 2
    assert ladder["p95_preference_decision"]["status"] == "NOT_EVALUATED"


def test_trade_event_order_and_roster_are_causal_and_unique() -> None:
    request = _payload_request()
    evidence = request["evidence_ledger_payload"]
    fold = evidence["entries"][0]["folds"][0]
    fold["completed_trade_close_offset_seconds"][0] = fold[
        "completed_trade_open_offset_seconds"
    ][0]
    fold["completed_trade_close_event_order_offsets"][0] = fold[
        "completed_trade_open_event_order_offsets"
    ][0]
    _rehash_trade_vector(fold, evidence["shared_fold_axis"][0])
    _refresh_evidence_hashes(request)
    assert _evaluate(request).reason_codes == ("FOLD_000_TRADE_TIME_ORDER_INVALID",)


def test_duplicate_position_across_folds_cannot_inflate_power() -> None:
    request = _payload_request()
    evidence = request["evidence_ledger_payload"]
    entry = evidence["entries"][0]
    first_ids = _decoded_position_ids(entry["folds"][0])
    second_ids = _decoded_position_ids(entry["folds"][1])
    second_ids[0] = first_ids[0]
    entry["folds"][1]["completed_trade_position_id_digests_base64"] = (
        winner._encode_position_id_digests(second_ids, "TEST_POSITION_ID_INVALID")
    )
    _rehash_trade_vector(entry["folds"][1], evidence["shared_fold_axis"][1])
    _refresh_evidence_hashes(request)
    assert _evaluate(request).reason_codes == (
        "EVIDENCE_0000_POSITION_ID_REUSED_ACROSS_FOLDS",
    )


def test_exact_request_binding_and_decision_are_immutable() -> None:
    request = _payload_request()
    typed = _typed_request(request)
    decision = winner.evaluate_historical_cell_winner(typed)
    assert winner.verify_historical_cell_winner_decision(decision, typed)
    request["precommitment_payload"]["run_id"] = _sha("tampered")
    assert winner.verify_historical_cell_winner_decision(
        decision, _typed_request(request)
    ) is False
    with pytest.raises(Exception):
        decision._receipt_sha256 = "0" * 64  # type: ignore[misc]


def test_technical_failure_is_infrastructure_rejection() -> None:
    request = _payload_request()
    entry = request["evidence_ledger_payload"]["entries"][0]
    entry["evidence_status"] = "TECHNICAL_FAILURE"
    entry["status_reason"] = "SOURCE_TECHNICAL_FAILURE"
    entry["folds"] = []
    _refresh_evidence_hashes(request)
    decision = _evaluate(request)
    assert decision.status == "REJECTED"
    assert decision.reason_codes == (
        "ATTEMPT_EVIDENCE_INCOMPLETE_TECHNICAL_FAILURE",
    )


def test_public_offline_api_has_no_dependency_injection_or_authority_surface() -> None:
    assert tuple(inspect.signature(winner.create_winner_selection_request).parameters) == (
        "precommitment_payload",
        "attempt_ledger_payload",
        "evidence_ledger_payload",
    )
    assert tuple(inspect.signature(winner.evaluate_historical_cell_winner).parameters) == (
        "request",
    )
    assert tuple(
        inspect.signature(winner.verify_historical_cell_winner_decision).parameters
    ) == ("decision", "request")
    for name in (
        "sign",
        "promote",
        "authorize_live",
        "size_capital",
        "current_refit",
    ):
        assert not hasattr(winner, name)


@pytest.mark.parametrize("nonfinite", [float("nan"), float("inf"), float("-inf")])
def test_nonfinite_request_numbers_fail_before_evaluation(nonfinite: float) -> None:
    request = _payload_request()
    request["evidence_ledger_payload"]["entries"][0]["folds"][0][
        "completed_trade_open_offset_seconds"
    ][0] = nonfinite
    assert winner.create_winner_selection_request(
        request["precommitment_payload"],
        request["attempt_ledger_payload"],
        request["evidence_ledger_payload"],
    ) is None


def test_cyclic_and_deep_inputs_are_total_and_bounded() -> None:
    request = _payload_request()
    cyclic: dict[str, Any] = {}
    cyclic["self"] = cyclic
    assert winner.create_winner_selection_request(
        cyclic,
        request["attempt_ledger_payload"],
        request["evidence_ledger_payload"],
    ) is None
    deep: Any = 1
    for _ in range(winner.MAX_JSON_DEPTH + 2):
        deep = [deep]
    assert winner.create_winner_selection_request(
        {"deep": deep},
        request["attempt_ledger_payload"],
        request["evidence_ledger_payload"],
    ) is None


def test_valid_request_uses_exported_payload_api_only() -> None:
    precommitment, attempt_ledger, evidence_ledger = _base_payloads()
    request = winner.create_winner_selection_request(
        precommitment,
        attempt_ledger,
        evidence_ledger,
    )
    assert type(request) is winner.WinnerSelectionRequest
    decision = winner.evaluate_historical_cell_winner(request)
    assert type(decision) is winner.WinnerSelectionDecision
    assert winner.verify_historical_cell_winner_decision(decision, request)


def test_selection_dominance_is_one_tier_primary_utility_dimension() -> None:
    assert winner.PINNED_SELECTION_POLICY["selection_dominance_fields"] == (
        winner._PRIMARY_UTILITY_FIELD,
    )


def test_source_documents_offline_non_authority_boundary() -> None:
    assert "live-trading" in winner.__doc__
    decision = _evaluate(_payload_request()).to_dict()
    for key in (
        "live_authorized",
        "capital_sizing_authorized",
        "profitability_claimed",
        "production_integrated",
    ):
        assert decision[key] is False


def _current_pair_contract(symbol: str, timeframe: str) -> dict[str, Any]:
    """Nontrading probes over synthetic bars; no issued source or market result."""
    import pandas as pd
    import pm_core
    from pm_spread_profile import load_spread_profile

    profile = load_spread_profile()
    cell = profile._cells[(symbol, timeframe)]
    point = cell["point_price"]
    instrument = pm_core.InstrumentSpec(
        symbol,
        round(-math.log10(point)) - 1,
        pip_value=10,
        point=point,
        tick_size=point,
        tick_value=1,
        min_lot=0.01,
        max_lot=100,
        volume_step=0.01,
        asset_class="fx",
    )
    minutes = {"M5": 5, "M15": 15, "M30": 30, "H1": 60, "H4": 240, "D1": 1440}[
        timeframe
    ]
    index = pd.date_range("2020-01-01", periods=5, freq=f"{minutes}min", tz="UTC")
    features = pd.DataFrame(
        {
            "Open": 1.0,
            "High": 1.0001,
            "Low": 0.9999,
            "Close": 1.0,
            "Spread": 0.0,
            "ATR_14": 0.001,
            "_BAR_CLOSE_TS": index + pd.Timedelta(minutes=minutes),
        },
        index=index,
    )
    runner = pm_core.Backtester(
        pm_core.PipelineConfig(
            use_spread=True,
            backtest_spread_mode="conservative_max",
            swap_model_enabled=False,
            research_mode=True,
        )
    )
    probes = {
        name: runner.build_evidence_source_seed_result(
            features,
            symbol,
            spec=instrument,
            timeframe=timeframe,
            spread_tier=profile.resolve(
                symbol,
                timeframe,
                name,
                point=point,
                source_identity_sha256=cell["source_identity"]["identity_sha256"],
            ),
            entry_spread_observations=pd.Series(0.0, index=index),
        )
        for name in ("P75", "P95")
    }
    return pm_core.build_backtest_frozen_tier_pair_contract(
        probes, spec=instrument, use_spread=True
    )


def _rehash_current_tier(row, axis, policy):
    lineage = (
        "spread_tier",
        "source_contract_sha256",
        "cost_contract_sha256",
        "entry_spread_admission_policy_sha256",
    )
    row["daily_surface_sha256"] = _sha_json(
        {
            "shared_fold_axis_row_sha256": _sha_json(axis),
            **{field: row[field] for field in lineage},
            "daily_net_mtm_r_e12_i64be_base64": row["daily_net_mtm_r_e12_i64be_base64"],
        }
    )
    row["trade_vector_sha256"] = _sha_json(
        {
            "shared_fold_axis_row_sha256": _sha_json(axis),
            "accounting_identity_kind": winner.CURRENT_SELECTION_POLICY[
                "accounting_identity"
            ],
            "completed_trade_unit": winner.CURRENT_SELECTION_POLICY["daily_unit"],
            "inventory_boundary_kind": winner.CURRENT_SELECTION_POLICY[
                "inventory_boundary"
            ],
            **{
                field: row[field] for field in (*lineage, *winner._TIER_IDENTITY_FIELDS)
            },
            "completed_trade_net_mtm_r_e12_i64be_base64": row[
                "completed_trade_net_mtm_r_e12_i64be_base64"
            ],
            "position_activity_ledger_sha256": row["position_activity_ledger_sha256"],
            "risk_unit_contract_sha256": policy["risk_unit_contract_sha256"],
            "trade_power_binding": row["trade_power_binding"],
        }
    )


def _refresh_current_request(request):
    policy = request["precommitment_payload"]
    attempts = request["attempt_ledger_payload"]
    evidence = request["evidence_ledger_payload"]
    policy_sha = _sha_json(policy)
    attempts["precommitment_payload_sha256"] = policy_sha
    attempts["attempts_sha256"] = _sha_json(attempts["attempts"])
    attempts_sha = _sha_json(attempts)
    evidence["precommitment_payload_sha256"] = policy_sha
    evidence["attempt_ledger_payload_sha256"] = attempts_sha
    for axis in evidence["shared_fold_axis"]:
        partition = axis["shared_oos_partition"]
        partition["receipt_sha256"] = _sha_json(
            {key: value for key, value in partition.items() if key != "receipt_sha256"}
        )
    evidence["shared_fold_axis_sha256"] = _sha_json(evidence["shared_fold_axis"])
    for entry in evidence["entries"]:
        if not entry["folds"]:
            _refresh_entry_hashes(entry)
            continue
        for fold, axis in zip(
            entry["folds"], evidence["shared_fold_axis"], strict=True
        ):
            for row in fold["tier_evidence"].values():
                binding = row["trade_power_binding"]
                if binding is not None:
                    binding["attempt_ledger_payload_sha256"] = attempts_sha
                    binding["binding_sha256"] = _sha_json(
                        {
                            key: value
                            for key, value in binding.items()
                            if key != "binding_sha256"
                        }
                    )
                _rehash_current_tier(row, axis, policy)
        _refresh_entry_hashes(entry)
    evidence["entries_sha256"] = _sha_json(evidence["entries"])


def _current_payload_request(
    symbol="AUDUSD", timeframe="M5", regime="TREND", *, p95_trade_step=2, p95_power=True,
    base_request=None, pair_contract=None,
):
    """Fresh synthetic v15 unit evidence, not a relabelled archived account."""
    request = _payload_request(symbol, timeframe, regime) if base_request is None else deepcopy(base_request)
    symbol, timeframe, regime = (request["precommitment_payload"][key] for key in ("symbol", "timeframe", "regime"))
    pair = _current_pair_contract(symbol, timeframe) if pair_contract is None else deepcopy(pair_contract)
    source_hashes = {
        tier: _sha(f"SYNTHETIC-CURRENT-SOURCE:{symbol}:{timeframe}:{tier}")
        for tier in ("P75", "P95")
    }
    cost_hashes = {
        item["tier"]: _sha_json(item["numerical_execution_cost_policy"])
        for item in pair["tiers"]
    }
    pair_sha = _sha_json(pair)
    common_source, common_cost = _sha_json(source_hashes), _sha_json(cost_hashes)

    def bind(value):
        if type(value) is dict:
            if "cost_ladder_contract_sha256" in value:
                value.pop("cost_ladder_contract_sha256")
                value["frozen_tier_pair_contract_sha256"] = pair_sha
            if "source_contract_sha256" in value:
                value["source_contract_sha256"] = common_source
            if "cost_contract_sha256" in value:
                value["cost_contract_sha256"] = common_cost
            for child in value.values():
                bind(child)
        elif type(value) is list:
            for child in value:
                bind(child)

    bind(request)
    policy = request["precommitment_payload"]
    attempts = request["attempt_ledger_payload"]
    evidence = request["evidence_ledger_payload"]
    policy.update(
        contract_version=winner.CURRENT_GENERATION.precommitment_version,
        selection_policy_sha256=winner.CURRENT_GENERATION.policy_sha256,
        frozen_tier_pair_contract=pair,
        partition_cost_precommitments_sha256=_sha(
            "SYNTHETIC-PREOUTCOME-PARTITION-COSTS"
        ),
    )
    for ledger in (attempts, evidence):
        ledger["partition_cost_precommitments_sha256"] = policy[
            "partition_cost_precommitments_sha256"
        ]
    evidence["contract_version"] = winner.CURRENT_GENERATION.evidence_version
    for axis in evidence["shared_fold_axis"]:
        axis["shared_oos_partition"]["schema_version"] = (
            winner.CURRENT_GENERATION.shared_partition_version
        )
    admission_sha = _sha_json(
        {
            "schema_version": pair["admission_schema"],
            "entry_admission_policy": pair["admission_policy"],
        }
    )
    for entry in evidence["entries"]:
        if (entry.get("evidence_status") == "PROCESS_REJECTED"
                and entry.get("status_reason") == "PRESEARCH_PROCESS_REJECTED"
                and type(entry.get("fold_count")) is int and entry["fold_count"] == 0
                and entry["folds"] == []
                and attempts["attempts"][entry["ordinal"]]["precommitted_evidence_role"]
                    == "PRESEARCH_PROCESS_REJECTED"):
            continue
        current_folds = []
        for old, axis in zip(entry["folds"], evidence["shared_fold_axis"], strict=True):
            fold = {field: old[field] for field in winner._COMMON_FOLD_KEYS}
            fold["tier_evidence"] = {}
            for tier in ("P75", "P95"):
                step = 1 if tier == "P75" else p95_trade_step
                count = len(old["completed_trade_close_offset_seconds"])
                groups = [
                    list(range(start, min(start + step, count)))
                    for start in range(0, count, step)
                ]
                integers = _unpacked_r_e12(
                    old,
                    winner._SERIALIZED_TRADE_LANE_FIELDS[tier],
                    expected_count=count,
                    maximum_abs_r=winner.MAX_ABS_TRADE_R,
                )
                orders_open, orders_close = winner._decode_event_order_offsets(
                    old["completed_trade_event_order_anchor"],
                    old["completed_trade_open_event_order_offsets"],
                    old["completed_trade_close_event_order_offsets"],
                    "SYNTHETIC-CURRENT-ORDERS",
                )
                anchor, opens, closes = winner._encode_event_order_offsets(
                    [orders_open[group[0]] for group in groups],
                    [orders_close[group[-1]] for group in groups],
                    "SYNTHETIC-CURRENT-ORDERS",
                )
                row = {
                    "spread_tier": tier,
                    "source_contract_sha256": source_hashes[tier],
                    "cost_contract_sha256": cost_hashes[tier],
                    "entry_spread_admission_policy_sha256": admission_sha,
                    "position_activity_ledger_sha256": _sha(
                        f"SYNTHETIC-CURRENT-ACTIVITY:{symbol}:{timeframe}:{regime}:{entry['ordinal']}:{old['fold_ordinal']}:{tier}"
                    ),
                    "daily_net_mtm_r_e12_i64be_base64": old[
                        winner._SERIALIZED_DAILY_LANE_FIELDS[tier]
                    ],
                    "completed_trade_open_offset_seconds": [
                        old["completed_trade_open_offset_seconds"][group[0]]
                        for group in groups
                    ],
                    "completed_trade_close_offset_seconds": [
                        old["completed_trade_close_offset_seconds"][group[-1]]
                        for group in groups
                    ],
                    "completed_trade_position_id_digests_base64": winner._encode_position_id_digests(
                        [
                            "btp:"
                            + _sha(
                                f"SYNTHETIC-CURRENT-POSITION:{entry['ordinal']}:{old['fold_ordinal']}:{tier}:{index}"
                            )
                            for index in range(len(groups))
                        ],
                        "SYNTHETIC-CURRENT-POSITIONS",
                    ),
                    "completed_trade_event_order_anchor": anchor,
                    "completed_trade_open_event_order_offsets": opens,
                    "completed_trade_close_event_order_offsets": closes,
                    "completed_trade_net_mtm_r_e12_i64be_base64": winner._encode_r_e12_i64be_base64(
                        [sum(integers[index] for index in group) for group in groups],
                        winner.MAX_ABS_TRADE_R,
                        "SYNTHETIC-CURRENT-R",
                    ),
                    "trade_power_binding": deepcopy(old["trade_power_binding"])
                    if tier == "P75" or p95_power
                    else None,
                }
                if row["trade_power_binding"] is not None:
                    for key in (
                        "train_requirement_sha256",
                        "train_calibration_sample_sha256",
                        "train_partition_contract_sha256",
                        "validation_decision_sha256",
                        "validation_partition_contract_sha256",
                    ):
                        row["trade_power_binding"][key] = _sha(
                            f"SYNTHETIC-CURRENT:{key}:{entry['ordinal']}:{old['fold_ordinal']}:{tier}"
                        )
                fold["tier_evidence"][tier] = row
            current_folds.append(fold)
        entry["folds"] = current_folds
    _refresh_current_request(request)
    return request


def test_current_tiers_keep_independent_axes_and_original_daily_standards():
    request = _current_payload_request()
    typed = _typed_request(request)
    decision = winner.evaluate_historical_cell_winner(typed)
    body = decision.to_dict()
    assert body["validated"] is True
    assert body["contract_version"] == winner.CURRENT_GENERATION.selection_version
    assert winner.verify_historical_cell_winner_decision(decision, typed)
    for entry in request["evidence_ledger_payload"]["entries"]:
        for fold in entry["folds"]:
            assert (
                len(
                    fold["tier_evidence"]["P75"]["completed_trade_close_offset_seconds"]
                )
                == 120
            )
            assert (
                len(
                    fold["tier_evidence"]["P95"]["completed_trade_close_offset_seconds"]
                )
                == 60
            )
    assert all(
        set(summary["lane_statistics"]) == {"P75", "P95"}
        for summary in body["candidate_summaries"]
    )
    for key in (
        "min_total_days",
        "min_hac_effective_days",
        "min_completed_trades",
        "min_completed_trades_per_fold",
        "within_cell_alpha",
        "pbo_max_probability",
        "moving_block_bootstrap_repetitions",
    ):
        assert (
            winner.CURRENT_SELECTION_POLICY[key]
            == winner.ARCHIVED_SELECTION_POLICY[key]
        )
    assert body["live_authorized"] is False
    assert body["profitability_claimed"] is False


def test_failed_p95_power_does_not_erase_a_qualified_p75_candidate():
    decision = _evaluate(_current_payload_request(p95_power=False)).to_dict()
    expected_winner = _attempts()[0]["process_candidate_id"]
    selection = decision["cost_ladder_selection"]
    assert decision["validated"] is True
    assert decision["status"] == "PROCESS_WINNER_PENDING_BOOK_MULTIPLICITY"
    assert decision["reason_codes"] == ["P95_ZERO_ADMITTED_P75_FALLBACK"]
    assert decision["winner"]["process_candidate_id"] == expected_winner
    assert selection["p75_required_decision"]["winner_process_candidate_id"] == expected_winner
    assert selection["selected_tier"] == "P75_REQUIRED"
    assert selection["p95_admitted_candidate_count"] == 0
    assert selection["p95_preference_decision"]["reason_codes"] == [
        "P95_ZERO_ADMITTED_P75_FALLBACK"
    ]
    for summary in decision["candidate_summaries"]:
        assert not summary["tier_statistics"]["P95_PREFERENCE"]["admissible"]
        assert (
            "INSUFFICIENT_COMPLETED_TRADE_HAC_POWER"
            in summary["tier_statistics"]["P95_PREFERENCE"]["admissibility_failures"]
        )


@pytest.mark.parametrize(
    "mutation",
    [
        "missing_tier",
        "mixed_shape",
        "tier_tag",
        "admission_policy",
        "daily_sum",
        "trade_axis",
        "power_binding",
        "precommit_cost",
        "cost_binding",
        "source_binding",
    ],
)
def test_current_resealed_evidence_rejects_material_tampering(mutation):
    request = _current_payload_request()
    fold = request["evidence_ledger_payload"]["entries"][0]["folds"][0]
    row = fold["tier_evidence"]["P95"]
    if mutation == "missing_tier":
        fold["tier_evidence"].pop("P95")
    elif mutation == "mixed_shape":
        fold["daily_net_mtm_r_e12_i64be_base64"] = row[
            "daily_net_mtm_r_e12_i64be_base64"
        ]
    elif mutation == "tier_tag":
        row["spread_tier"] = "P75"
    elif mutation == "admission_policy":
        row["entry_spread_admission_policy_sha256"] = _sha("WRONG-ADMISSION-POLICY")
    elif mutation == "daily_sum":
        values = _unpacked_r_e12(
            row,
            "daily_net_mtm_r_e12_i64be_base64",
            expected_count=63,
            maximum_abs_r=winner.MAX_ABS_DAILY_R,
        )
        values[0] += 1
        _set_packed_r_e12(
            row, "daily_net_mtm_r_e12_i64be_base64", values, winner.MAX_ABS_DAILY_R
        )
    elif mutation == "trade_axis":
        row["completed_trade_close_offset_seconds"].pop()
    elif mutation == "power_binding":
        row["trade_power_binding"]["attempted_trials_exact"] += 1
    elif mutation == "cost_binding":
        row["cost_contract_sha256"] = _sha("WRONG-COST-CONTRACT")
    elif mutation == "source_binding":
        row["source_contract_sha256"] = _sha("WRONG-SOURCE-CONTRACT")
    else:
        request["precommitment_payload"]["frozen_tier_pair_contract"][
            "qualification_authority"
        ] = True
    _refresh_current_request(request)
    decision = _evaluate(request).to_dict()
    assert decision["status"] == "REJECTED"
    assert decision["live_authorized"] is False


@pytest.mark.parametrize("failure", ["pbo", "work_bound", "unresolved"])
def test_current_nonempty_p95_failure_preserves_qualified_p75_fallback(
    monkeypatch: pytest.MonkeyPatch,
    failure: str,
) -> None:
    # Force the selection branch only; candidate arithmetic and policy stay real.
    request = _current_payload_request(p95_trade_step=1)
    if failure == "pbo":
        original = winner._cell_pbo_evidence

        def failed_p95(*args: Any, **kwargs: Any) -> dict[str, Any]:
            evidence = original(*args, **kwargs)
            evidence["tier_evidence"]["P95_PREFERENCE"].update(
                status="FAIL",
                passed=False,
                reason_code="TIER_LANE_PBO_EXCEEDS_LIMIT",
            )
            return evidence

        monkeypatch.setattr(winner, "_cell_pbo_evidence", failed_p95)
    elif failure == "work_bound":
        original = winner._pair_operation_diagnostic
        calls = 0

        def exceeded_p95(*args: Any, **kwargs: Any) -> dict[str, int]:
            nonlocal calls
            calls += 1
            diagnostic = original(*args, **kwargs)
            if calls % 2 == 0:
                diagnostic["expected_dependence_operations"] = (
                    diagnostic["operation_limit"] + 1
                )
            return diagnostic

        monkeypatch.setattr(winner, "_pair_operation_diagnostic", exceeded_p95)
    else:
        original = winner._resolve_tier

        def unresolved_p95(*args: Any, **kwargs: Any) -> tuple[Any, ...]:
            decision, selected, reporting = original(*args, **kwargs)
            if kwargs["tier"] == "P95_PREFERENCE":
                return (
                    {
                        **decision,
                        "status": "UNRESOLVED_NONDOMINATED_SET",
                        "winner_process_candidate_id": None,
                        "reason_codes": [
                            "STATISTICALLY_DIFFERENT_PRIMARY_UTILITY_ORDER_CONFLICT"
                        ],
                    },
                    None,
                    reporting,
                )
            return decision, selected, reporting

        monkeypatch.setattr(winner, "_resolve_tier", unresolved_p95)

    decision = _evaluate(request)
    body = decision.to_dict()
    selection = body["cost_ladder_selection"]
    assert body["validated"] is True
    assert body["status"] == "PROCESS_WINNER_PENDING_BOOK_MULTIPLICITY"
    assert body["reason_codes"] == ["NO_QUALIFIED_P95_WINNER_P75_FALLBACK"]
    assert selection["p95_admitted_candidate_count"] > 0
    assert selection["p95_preference_decision"]["winner_process_candidate_id"] is None
    assert selection["selected_tier"] == "P75_REQUIRED"
    assert (
        body["winner"]["process_candidate_id"]
        == selection["p75_required_decision"]["winner_process_candidate_id"]
    )
    marginal = _marginal(decision, request)
    assert marginal["validated_cost_ladder_tolerance"]["tier"] == "P75"
    assert body["live_authorized"] is False


def test_current_p75_failure_cannot_be_replaced_by_a_p95_winner(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original = winner._cell_pbo_evidence

    def failed_p75(*args: Any, **kwargs: Any) -> dict[str, Any]:
        evidence = original(*args, **kwargs)
        evidence["tier_evidence"]["P75_REQUIRED"].update(
            status="FAIL",
            passed=False,
            reason_code="TIER_LANE_PBO_EXCEEDS_LIMIT",
        )
        return evidence

    monkeypatch.setattr(winner, "_cell_pbo_evidence", failed_p75)
    body = _evaluate(_current_payload_request(p95_trade_step=1)).to_dict()
    selection = body["cost_ladder_selection"]
    assert body["validated"] is True
    assert body["winner"] is None
    assert selection["selected_tier"] is None
    assert selection["p95_admitted_candidate_count"] > 0
    assert selection["p95_preference_decision"]["status"] == "NOT_EVALUATED"


def test_public_archive_names_stay_pinned_and_current_request_dispatch_is_explicit():
    assert winner.WINNER_SELECTION_CONTRACT_VERSION == "fxpm.winner-selection.v14"
    assert winner.PRECOMMITMENT_VERSION == "fxpm.winner-precommitment.v3"
    assert winner.EVIDENCE_LEDGER_VERSION == "fxpm.winner-evidence-ledger.v6"
    assert winner.pinned_policy_snapshot() == json.loads(
        winner._canonical_bytes(dict(winner.ARCHIVED_SELECTION_POLICY))
    )
    assert winner.current_policy_snapshot() == json.loads(
        winner._canonical_bytes(dict(winner.CURRENT_SELECTION_POLICY))
    )
    assert (
        winner.CURRENT_WINNER_SELECTION_CONTRACT_VERSION == "fxpm.winner-selection.v15"
    )
    assert (
        winner.current_policy_snapshot()["p95_fallback_rule"]
        == "QUALIFIED_P75_WHEN_NO_QUALIFIED_P95_WINNER"
    )


@pytest.mark.parametrize(
    "host_generation,foreign_generation",
    [("archive", "current"), ("current", "archive")],
    ids=["archive-host-current-evidence", "current-host-archive-evidence"],
)
def test_winner_rejects_mixed_generation_evidence_at_public_boundary(
    host_generation: str, foreign_generation: str,
) -> None:
    factories = {"archive": _payload_request, "current": _current_payload_request}
    host = _typed_request(factories[host_generation]())
    foreign = _typed_request(factories[foreign_generation]())
    control = winner.evaluate_historical_cell_winner(host).to_dict()
    assert control["validated"] is True
    assert host.evidence_ledger.to_dict()["contract_version"] != (
        foreign.evidence_ledger.to_dict()["contract_version"]
    )

    # The public factory rebinds the exact foreign bytes, isolating generation refusal.
    mixed = winner.create_winner_selection_request(
        host.precommitment.to_dict(), host.attempt_ledger.to_dict(),
        foreign.evidence_ledger.to_dict(),
    )
    assert mixed is not None
    assert mixed.request_sha256 != host.request_sha256
    decision = winner.evaluate_historical_cell_winner(mixed)
    body = decision.to_dict()
    assert body["status"] == "REJECTED"
    assert body["validated"] is False
    assert body["reason_codes"] == ["EVIDENCE_LEDGER_SCHEMA_INVALID"]
    assert body["contract_version"] == control["contract_version"]
    assert body["request_contract_version"] == host.to_dict()["contract_version"]
    assert body["request_sha256"] == mixed.request_sha256
    assert body["winner"] is None
    assert winner.verify_historical_cell_winner_decision(decision, mixed) is False
    assert winner.historical_book_marginal_receipt(decision, mixed) is None
    for field in ("live_authorized", "capital_sizing_authorized", "profitability_claimed"):
        assert body[field] is False


def test_current_p95_candidate_must_also_pass_its_own_required_p75_gates():
    request = _current_payload_request(p95_trade_step=1)
    entries = request["evidence_ledger_payload"]["entries"]
    second = entries[1]
    for fold in second["folds"]:
        fold["tier_evidence"]["P75"]["trade_power_binding"] = None
    _refresh_current_request(request)
    body = _evaluate(request).to_dict()
    assert body["validated"] is True
    statistics = body["candidate_summaries"][1]["tier_statistics"]
    assert statistics["P75_REQUIRED"]["admissible"] is False
    assert statistics["P95_PREFERENCE"]["admissible"] is True
    first_statistics = body["candidate_summaries"][0]["tier_statistics"]
    assert first_statistics["P75_REQUIRED"]["admissible"] is True
    assert first_statistics["P95_PREFERENCE"]["admissible"] is True
    selection = body["cost_ladder_selection"]
    expected_winner = entries[0]["process_candidate_id"]
    assert selection["p95_admitted_candidate_count"] == 1
    preference = selection["p95_preference_decision"]
    assert preference["admitted_candidate_count"] == 1
    assert preference["winner_process_candidate_id"] == expected_winner
    assert preference["reporting_process_candidate_id"] == expected_winner
    assert preference["paired_comparisons"] == []
    assert second["process_candidate_id"] not in {
        preference["winner_process_candidate_id"], preference["reporting_process_candidate_id"],
    }
    assert selection["selected_tier"] == "P95_PREFERENCE"
    assert body["winner"]["process_candidate_id"] == expected_winner
    assert body["live_authorized"] is False


def test_current_terminal_request_binds_pair_cost_before_any_fold_exists():
    request = _current_payload_request()
    attempts = request["attempt_ledger_payload"]["attempts"]
    entries = request["evidence_ledger_payload"]["entries"]
    for attempt, entry in zip(attempts, entries, strict=True):
        attempt["precommitted_evidence_role"] = "PRESEARCH_PROCESS_REJECTED"
        entry.update(
            evidence_status="PROCESS_REJECTED",
            status_reason="PRESEARCH_PROCESS_REJECTED",
            folds=[],
        )
    _refresh_complete_chain(request)
    original = _evaluate(request).to_dict()
    assert original["validated"] is True
    assert original["winner"] is None

    def alter_cost(value):
        if type(value) is dict:
            if "cost_contract_sha256" in value:
                value["cost_contract_sha256"] = _sha("UNBOUND-TERMINAL-COST")
            for child in value.values():
                alter_cost(child)
        elif type(value) is list:
            for child in value:
                alter_cost(child)

    alter_cost(request)
    _refresh_current_request(request)
    refused = _evaluate(request).to_dict()
    assert refused["status"] == "REJECTED"
    assert refused["reason_codes"] == ["FROZEN_TIER_PAIR_COST_CONTRACT_MISMATCH"]
