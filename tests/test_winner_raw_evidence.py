from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, replace
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

import pm_authority_spool as spool
import pm_production_search_planner as planner
import pm_winner_selection as winner
from pm_core import (
    BACKTEST_INSTRUMENT_SPEC_SCHEMA_VERSION,
    BACKTEST_FROZEN_PATH_COST_STRESS_ADVERSITY_INSUFFICIENT,
    BACKTEST_FROZEN_PATH_COST_STRESS_ADVERSITY_STRICT,
    BACKTEST_FROZEN_PATH_COST_STRESS_SCHEMA_VERSION,
    Backtester,
    InstrumentSpec,
    PipelineConfig,
)
from pm_current_refit_artifacts import build_current_refit_search_artifacts_v2
from pm_strategies import StrategyRegistry
from pm_search_outcomes import SearchOutcomeKind, SearchStage, TrialOutcome
from pm_trade_power import build_numerical_execution_cost_attestation
from pm_winner_raw_evidence import (
    COST_STRESS_ADVERSITY_EXPECTED_NO_SHOCK,
    COST_STRESS_ADVERSITY_EXPECTED_STRICT,
    COST_STRESS_ADVERSITY_PLAN_SCHEMA,
    EXECUTION_SPEC_RECEIPT_SCHEMA,
    FROZEN_COST_POLICY_SCHEMA,
    WinnerRawEvidenceError,
    WinnerRawEvidenceIncomplete,
    _event_vectors,
    build_flat_daily_r_surface,
    build_frozen_numerical_execution_cost_policy,
    build_precommitted_risk_unit_contract,
    build_trade_source_contract_from_backtester_result,
    freeze_winner_search_plan_v3,
    produce_winner_v11_raw_evidence,
)


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


def _sha1(value):
    if isinstance(value, str):
        value = {"label": value}
    return hashlib.sha1(_canonical(value)).hexdigest()


COST_SPEC = InstrumentSpec(
    symbol="EURUSD",
    pip_position=4,
    pip_value=10.0,
    spread_avg=1.0,
)
BASE_COST_BACKTESTER = Backtester(
    PipelineConfig(
        use_spread=True,
        backtest_spread_mode="conservative_max",
        swap_model_enabled=False,
    )
)
STRESS_COST_BACKTESTER = Backtester(
    PipelineConfig(
        use_spread=True,
        backtest_spread_mode="stress_p95",
        swap_model_enabled=False,
    )
)


def _genuine_partition_cost(*, label: str, bars: int, stress: bool):
    """Use Backtester itself so each causal partition seals its own arrays."""

    if bars <= 0:
        raise AssertionError("partition must contain bars")
    # The label fraction makes equal-length partitions byte-distinct too;
    # bar count remains encoded by Backtester's own array hasher.
    offset = int(_sha(f"spread:{label}")[:12], 16) / float(16**12)
    spread_open = np.asarray(
        [12.0 + offset + index % 3 for index in range(bars)], dtype=np.float64
    )
    spread_max = spread_open + 4.0
    spread_p95 = spread_open + 3.0
    frame = pd.DataFrame(
        {
            "Spread": spread_open,
            "SpreadOpen": spread_open,
            "SpreadMax": spread_max,
            "SpreadP95": spread_p95,
        }
    )
    backtester = STRESS_COST_BACKTESTER if stress else BASE_COST_BACKTESTER
    entry, intrabar, meta = backtester._build_half_spread_arrays(frame, COST_SPEC)
    return backtester._numerical_execution_cost_attestation(
        entry_half_spread=entry,
        intrabar_half_spread=intrabar,
        spread_meta=meta,
        spec=COST_SPEC,
    )


def _frozen_policy(attestation, lane):
    return build_frozen_numerical_execution_cost_policy(
        lane=lane,
        use_spread=attestation["use_spread"],
        spread_mode=attestation["spread_mode"],
        instrument_spec_sha256=attestation["instrument_spec_sha256"],
    )


BASE_COST = _genuine_partition_cost(label="policy-base", bars=1, stress=False)
STRESS_COST = _genuine_partition_cost(label="policy-stress", bars=1, stress=True)
BASE_COST_POLICY = _frozen_policy(BASE_COST, "BASE")
STRESS_COST_POLICY = _frozen_policy(STRESS_COST, "STRESS")
RISK_CONTRACT = build_precommitted_risk_unit_contract()


def _rebuilt_cost(attestation, **changes):
    values = dict(attestation)
    values.update(changes)
    return build_numerical_execution_cost_attestation(
        use_spread=values["use_spread"],
        spread_mode=values["spread_mode"],
        spread_values_sha256=values["spread_values_sha256"],
        instrument_spec_sha256=values["instrument_spec_sha256"],
    )


def _events(
    start: date,
    *,
    namespace: str,
    values: list[float] | None = None,
):
    observed = values or [0.30 + 0.01 * (index % 2) for index in range(12)]
    rows = []
    for index, value in enumerate(observed):
        opened = datetime.combine(
            start + timedelta(days=index),
            datetime.min.time(),
            tzinfo=timezone.utc,
        ) + timedelta(hours=10)
        rows.append(
            {
                "trade_record_schema_version": "2026-07-14-position-events-v2",
                "position_id": f"{namespace}:position:{index:04d}",
                "entry_time": opened.isoformat(),
                "exit_time": (opened + timedelta(hours=1)).isoformat(),
                "entry_event_order": index * 4 + 1,
                "exit_event_order": index * 4 + 3,
                "pnl_dollars": value * 100.0,
                "risk_amount": 100.0,
            }
        )
    return rows


def _swap(*, projection: str):
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
        "projection_sha256": projection,
    }


def _result(
    *,
    start: date,
    end: date,
    namespace: str,
    axis_label: str,
    cost=None,
    stress_cost_lane: bool = False,
    values: list[float] | None = None,
):
    events = _events(start, namespace=namespace, values=values)
    surface = build_flat_daily_r_surface(
        events,
        start_date_utc=start.isoformat(),
        end_date_utc=end.isoformat(),
        risk_unit_contract=RISK_CONTRACT,
    )
    axis = {
        "schema_version": "fxpm.backtest-partition-axis.v1",
        "timeframe": "H1",
        "start_utc": datetime.combine(
            start, datetime.min.time(), tzinfo=timezone.utc
        ).isoformat(),
        "end_utc": datetime.combine(
            end + timedelta(days=1), datetime.min.time(), tzinfo=timezone.utc
        ).isoformat(),
        "scored_bars": (end - start).days + 1,
        "warmup_bars": 0,
        "event_axis_sha256": _sha(f"axis:{axis_label}"),
    }
    axis["horizon_receipt_sha256"] = _sha(axis)
    partition_bars = (end - start).days + 1
    partition_cost = cost or _genuine_partition_cost(
        label=axis_label,
        bars=partition_bars,
        stress=stress_cost_lane,
    )
    return {
        "position_events": events,
        "winner_daily_r_surface": surface,
        "trade_power_partition_axis": axis,
        "execution_cost_timing_contract": {
            "swap_accrual": "causal_swap_accrual_v1",
        },
        "numerical_execution_cost_attestation": partition_cost,
        "swap_accrual_contract": _swap(projection=_sha(f"projection:{axis_label}")),
    }


def _attempt_artifact(
    ordinal: int,
    strategy_id: str = "EMACrossoverStrategy",
    *,
    symbol: str = "EURUSD",
):
    """Build a GENUINE executable attempt via the accepted v2 producer."""
    strategy = StrategyRegistry.get(strategy_id)
    parameters = strategy.get_params()
    if strategy_id == "EMACrossoverStrategy":
        choices = strategy.get_param_grid()["fast_period"]
        parameters["fast_period"] = choices[ordinal % len(choices)]
    built = build_current_refit_search_artifacts_v2(
        symbol=symbol,
        timeframe="H1",
        regime="TREND",
        strategy_id=strategy_id,
        parameters=parameters,
        lookback_kind="NATIVE_OBSERVATIONS",
        lookback_value=5_000,
        fit_start_utc="2015-01-01T00:00:00Z",
        fit_end_utc="2020-01-01T00:00:00Z",
        latest_closed_bar_utc="2019-12-31T23:00:00Z",
        source_snapshot_sha256="a" * 64,
        source_bar_count=5_000,
        seed=20260714,
    )
    return {
        "winner_process": built.winner_process,
        "horizon_process": built.horizon_process,
        "trial_definition": built.trial_definition,
        "refit_recipe": built.refit_recipe,
    }


def _fold_plan():
    starts = [date(2021 + index, 2, 1) for index in range(4)]
    return [
        {
            "anchor_as_of_utc": datetime.combine(
                start, datetime.min.time(), tzinfo=timezone.utc
            ).isoformat(),
            "oos_start_date_utc": start.isoformat(),
            "oos_end_date_utc": (start + timedelta(days=62)).isoformat(),
        }
        for start in starts
    ]


def _receipt(role: str, common: dict, **specific):
    body = {
        "schema_version": "fxpm.authoritative-search-provenance.v1",
        **common,
        "receipt_role": role,
        **specific,
    }
    return {**body, "receipt_sha256": _sha(body)}


def _partition_geometry(*, attempt_ordinal, fold_ordinal, lanes):
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
            "frame_sha256": _sha(f"frame:{attempt_ordinal}:{fold_ordinal}:{index}"),
        }
        geometry.append({**body, "receipt_sha256": _sha(body)})
    return geometry


def _execution_spec_receipt(frozen):
    instrument_spec = asdict(COST_SPEC)
    instrument_spec_sha256 = Backtester._cost_attestation_json_sha256(
        {
            "schema_version": BACKTEST_INSTRUMENT_SPEC_SCHEMA_VERSION,
            "instrument_spec": instrument_spec,
        }
    )
    body = {
        "schema_version": EXECUTION_SPEC_RECEIPT_SCHEMA,
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
        "native_prefix_index_sha256": _sha("native-prefix-index"),
        "native_provenance_sha256": _sha("native-provenance"),
        "native_instrument_snapshot_sha256": _sha("native-instrument"),
        "tick_value_source": "ACTIVATED_ACQUISITION_INSTRUMENT_SNAPSHOT",
        "volume_source": "FROZEN_BROKER_SPECS_RECORD",
        "instrument_spec": instrument_spec,
        "instrument_spec_sha256": instrument_spec_sha256,
        "grants_search_authority": False,
        "grants_trading_authority": False,
    }
    return {**body, "receipt_sha256": _sha(body)}


def _production_provenance(*, frozen, attempt_ordinal, fold_ordinal, planned, lanes):
    attempt = frozen.attempt_ledger["attempts"][attempt_ordinal]
    artifacts = frozen.attempt_artifacts_bundle[attempt_ordinal]
    common = {
        "authority_scope": "PRECOMMITTED_HISTORICAL_INNER_FOLD_ONLY",
        "anchor_id": "anchor-test-001",
        "symbol": frozen.symbol,
        "timeframe": frozen.timeframe,
        "regime": frozen.regime,
        "attempt_ordinal": attempt_ordinal,
        "horizon_attempt_id": _sha(f"horizon-attempt:{attempt_ordinal}"),
        "fold_ordinal": fold_ordinal,
        "fold_id": planned["fold_id"],
        "confers_final_oos_or_trading_authority": False,
    }
    base = lanes["final_lockbox_base"]
    stress = lanes["final_lockbox_stress"]
    base_axis = base["trade_power_partition_axis"]
    stress_surface = stress["winner_daily_r_surface"]
    signal_values = [0] * (base_axis["warmup_bars"] + base_axis["scored_bars"])
    signal_sha256 = hashlib.sha256(
        np.asarray(signal_values, dtype="<i4").tobytes(order="C")
    ).hexdigest()
    signal_body = {
        "schema_version": "fxpm.authoritative-search-signal-vector.v1",
        "encoding": "little-endian-signed-int32",
        "observation_count": len(signal_values),
        "values": signal_values,
        "signal_vector_sha256": signal_sha256,
    }
    signal_receipt = {**signal_body, "receipt_sha256": _sha(signal_body)}
    geometry = _partition_geometry(
        attempt_ordinal=attempt_ordinal,
        fold_ordinal=fold_ordinal,
        lanes=lanes,
    )
    stress_body = {
        "schema_version": BACKTEST_FROZEN_PATH_COST_STRESS_SCHEMA_VERSION,
        "symbol": frozen.symbol,
        "timeframe": frozen.timeframe,
        "strategy_name": artifacts["winner_process"]["strategy_id"],
        "stress_mode": "stress_p95",
        "warmup_bars": base_axis["warmup_bars"],
        "partition_axis_sha256": base_axis["horizon_receipt_sha256"],
        "signal_vector_sha256": signal_sha256,
        "base_cost_attestation_sha256": base["numerical_execution_cost_attestation"][
            "attestation_sha256"
        ],
        "stress_cost_attestation_sha256": stress[
            "numerical_execution_cost_attestation"
        ]["attestation_sha256"],
        "base_spread_values_sha256": base["numerical_execution_cost_attestation"][
            "spread_values_sha256"
        ],
        "stress_spread_values_sha256": stress[
            "numerical_execution_cost_attestation"
        ]["spread_values_sha256"],
        "base_position_roster_sha256": Backtester._cost_attestation_json_sha256(
            {"position_events": base["position_events"]}
        ),
        "stress_position_roster_sha256": Backtester._cost_attestation_json_sha256(
            {"position_events": stress["position_events"]}
        ),
        "position_count": len(stress["position_events"]),
        "daily_observation_count": len(stress_surface["daily_net_mtm_r"]),
        "path_identity_preserved": True,
        "stress_never_improves": True,
        "strictly_worse_trade_count": sum(
            stress_event["pnl_dollars"] < base_event["pnl_dollars"]
            for base_event, stress_event in zip(
                base["position_events"],
                stress["position_events"],
                strict=True,
            )
        ),
        "strictly_worse_daily_observation_count": sum(
            stress_value < base_value
            for base_value, stress_value in zip(
                base["winner_daily_r_surface"]["daily_net_mtm_r"],
                stress_surface["daily_net_mtm_r"],
                strict=True,
            )
        ),
        "stress_daily_improvement_observation_count": sum(
            stress_value > base_value
            for base_value, stress_value in zip(
                base["winner_daily_r_surface"]["daily_net_mtm_r"],
                stress_surface["daily_net_mtm_r"],
                strict=True,
            )
        ),
        "realized_adversity_status": (
            BACKTEST_FROZEN_PATH_COST_STRESS_ADVERSITY_STRICT
        ),
    }
    stress_receipt = {**stress_body, "receipt_sha256": _sha(stress_body)}
    source_binding = frozen.source_contract_seed["source_binding"]
    return {
        "anchor_selection_receipt": _receipt(
            "anchor_selection_receipt",
            common,
            process_precommit_receipt_sha256=_sha("process-precommit"),
            horizon_plan_receipt_sha256=_sha("horizon-plan"),
            selection_cutoff_utc="2020-01-01T00:00:00Z",
        ),
        "anchor_horizon_decision": _receipt(
            "anchor_horizon_decision",
            common,
            candidate_id=f"candidate-{attempt_ordinal}",
            candidate_definition_sha256=_sha(f"candidate:{attempt_ordinal}"),
            horizon_process_sha256=attempt["horizon_process_sha256"],
            fold_definition_sha256=_sha(f"fold:{attempt_ordinal}:{fold_ordinal}"),
            partition_geometry=geometry,
            partition_geometry_receipts=[item["receipt_sha256"] for item in geometry],
        ),
        "anchor_trial_ledger": _receipt(
            "anchor_trial_ledger",
            common,
            trial_registry_sha256=frozen.precommitment["trial_registry_sha256"],
            attempt_roster_sha256=_sha("horizon-attempt-roster"),
            attempted_trials_exact=frozen.attempt_ledger["attempt_count"],
        ),
        "anchor_selected_trial": _receipt(
            "anchor_selected_trial",
            common,
            selection_scope="PRECOMMITTED_ATTEMPT_NOT_POSTHOC_WINNER",
            trial_id=f"trial-{attempt_ordinal}",
            trial_definition_sha256=_sha(f"horizon-trial:{attempt_ordinal}"),
            winner_process_sha256=attempt["process_sha256"],
            executable_trial_sha256=attempt["trial_definition_sha256"],
        ),
        "anchor_refit_receipt": _receipt(
            "anchor_refit_receipt",
            common,
            refit_recipe_sha256=attempt["refit_recipe_sha256"],
            fit_start_utc=(
                date.fromisoformat(planned["oos_start_date_utc"]) - timedelta(days=300)
            ).isoformat()
            + "T00:00:00Z",
            fit_end_exclusive_utc=(
                date.fromisoformat(planned["oos_start_date_utc"]) - timedelta(days=1)
            ).isoformat()
            + "T00:00:00Z",
            post_selection_search=False,
        ),
        "attempt_replay_receipt": _receipt(
            "attempt_replay_receipt",
            common,
            lane_sha256={name: _sha(lanes[name]) for name in lanes},
            stress_method_receipt_sha256=stress_receipt["receipt_sha256"],
            stress_method_receipt=stress_receipt,
            signal_vector_receipt=signal_receipt,
            position_events_are_genuine_backtester_v2=True,
        ),
        "oos_source_receipt": _receipt(
            "oos_source_receipt",
            common,
            native_prefix_index_sha256=_sha("native-prefix-index"),
            native_prefix_content_sha256=artifacts["horizon_process"][
                "source_snapshot_sha256"
            ],
            source_contract_sha256=frozen.precommitment["source_contract_sha256"],
            native_source_sha256=source_binding["target_bars_sha256"],
            activation_manifest_sha256=source_binding[
                "price_activation_manifest_sha256"
            ],
            source_group_decision_sha256=_sha("source-group-decision"),
            regime_fit_decision_sha256=_sha("regime-fit-decision"),
            execution_spec_receipt=_execution_spec_receipt(frozen),
            partition_frame_sha256=[item["frame_sha256"] for item in geometry],
        ),
    }


def _raw_fold(*, attempt_ordinal: int, fold_ordinal: int, planned, frozen=None):
    final_start = date.fromisoformat(planned["oos_start_date_utc"])
    final_end = date.fromisoformat(planned["oos_end_date_utc"])
    train_start = final_start - timedelta(days=300)
    train_end = train_start + timedelta(days=62 + fold_ordinal)
    tuning_start = final_start - timedelta(days=150)
    tuning_end = tuning_start + timedelta(days=63 + fold_ordinal * 2)
    prefix = f"a{attempt_ordinal}:f{fold_ordinal}"
    final = _result(
        start=final_start,
        end=final_end,
        namespace=f"{prefix}:final",
        axis_label=f"{prefix}:final",
    )
    stress_values = [
        event["pnl_dollars"] / event["risk_amount"] - 0.02
        for event in final["position_events"]
    ]
    stress = _result(
        start=final_start,
        end=final_end,
        namespace=f"{prefix}:final",
        axis_label=f"{prefix}:final",
        stress_cost_lane=True,
        values=stress_values,
    )
    lanes = {
        "optimizer_train_base": _result(
            start=train_start,
            end=train_end,
            namespace=f"{prefix}:train",
            axis_label=f"{prefix}:train",
        ),
        "tuning_validation_base": _result(
            start=tuning_start,
            end=tuning_end,
            namespace=f"{prefix}:tuning",
            axis_label=f"{prefix}:tuning",
        ),
        "final_lockbox_base": final,
        "final_lockbox_stress": stress,
    }
    provenance = (
        {}
        if frozen is None
        else _production_provenance(
            frozen=frozen,
            attempt_ordinal=attempt_ordinal,
            fold_ordinal=fold_ordinal,
            planned=planned,
            lanes=lanes,
        )
    )
    return {
        "fold_ordinal": fold_ordinal,
        "provenance": provenance,
        "lanes": lanes,
    }


def _context(tmp_path: Path, *, symbol: str = "EURUSD"):
    context = spool.freeze_generation_context(
        tmp_path / "authority",
        generation_id="generation-raw-winner-001",
        native_universe_sha256="b" * 64,
        config_sha256="c" * 64,
    )
    return context, spool.open_symbol_sink(context, symbol)


def _normalized_test_fold_plan(*, symbol: str = "EURUSD"):
    normalized = []
    for ordinal, raw in enumerate(_fold_plan()):
        anchor = (
            datetime.fromisoformat(
                raw["anchor_as_of_utc"].replace("Z", "+00:00")
            )
            .astimezone(timezone.utc)
            .isoformat(timespec="seconds")
            .replace("+00:00", "Z")
        )
        body = {
            "schema_version": "fxpm.winner-fold-identity.v1",
            "symbol": symbol,
            "timeframe": "H1",
            "regime": "TREND",
            "fold_ordinal": ordinal,
            "anchor_as_of_utc": anchor,
            "oos_start_date_utc": raw["oos_start_date_utc"],
            "oos_end_date_utc": raw["oos_end_date_utc"],
        }
        normalized.append(
            {
                "fold_ordinal": ordinal,
                "fold_id": _sha1(body),
                "anchor_as_of_utc": anchor,
                "oos_start_date_utc": raw["oos_start_date_utc"],
                "oos_end_date_utc": raw["oos_end_date_utc"],
            }
        )
    return normalized


def _cost_stress_adversity_plan(
    *,
    attempts: int,
    roles,
    no_shock_pairs=frozenset(),
    symbol: str = "EURUSD",
):
    rows = []
    normalized_folds = _normalized_test_fold_plan(symbol=symbol)
    for attempt_ordinal in range(attempts):
        if roles[attempt_ordinal] != "EVALUATION_REQUIRED":
            continue
        for planned in normalized_folds:
            fold_ordinal = planned["fold_ordinal"]
            lanes = _raw_fold(
                attempt_ordinal=attempt_ordinal,
                fold_ordinal=fold_ordinal,
                planned=planned,
            )["lanes"]
            geometry = _partition_geometry(
                attempt_ordinal=attempt_ordinal,
                fold_ordinal=fold_ordinal,
                lanes=lanes,
            )[-1]
            base_cost = lanes["final_lockbox_base"][
                "numerical_execution_cost_attestation"
            ]
            stress_cost = lanes["final_lockbox_stress"][
                "numerical_execution_cost_attestation"
            ]
            if (attempt_ordinal, fold_ordinal) in no_shock_pairs:
                stress_cost = _rebuilt_cost(
                    stress_cost,
                    spread_values_sha256=base_cost["spread_values_sha256"],
                )
            equal_vectors = (
                base_cost["spread_values_sha256"]
                == stress_cost["spread_values_sha256"]
            )
            rows.append(
                {
                    "attempt_ordinal": attempt_ordinal,
                    "horizon_attempt_id": _sha(
                        f"horizon-attempt:{attempt_ordinal}"
                    ),
                    "fold_ordinal": fold_ordinal,
                    "fold_id": planned["fold_id"],
                    "final_partition_geometry_receipt_sha256": geometry[
                        "receipt_sha256"
                    ],
                    "final_partition_frame_sha256": geometry["frame_sha256"],
                    "base_cost_attestation_sha256": base_cost[
                        "attestation_sha256"
                    ],
                    "stress_cost_attestation_sha256": stress_cost[
                        "attestation_sha256"
                    ],
                    "base_spread_values_sha256": base_cost[
                        "spread_values_sha256"
                    ],
                    "stress_spread_values_sha256": stress_cost[
                        "spread_values_sha256"
                    ],
                    "expected_adversity_status": (
                        COST_STRESS_ADVERSITY_EXPECTED_NO_SHOCK
                        if equal_vectors
                        else COST_STRESS_ADVERSITY_EXPECTED_STRICT
                    ),
                }
            )
    return {
        "schema_version": COST_STRESS_ADVERSITY_PLAN_SCHEMA,
        "symbol": symbol,
        "timeframe": "H1",
        "regime": "TREND",
        "row_count": len(rows),
        "rows_sha256": _sha(rows),
        "rows": rows,
    }


def _freeze(
    tmp_path: Path,
    *,
    attempts=1,
    symbol="EURUSD",
    artifact_namespace=None,
    sink=None,
    context=None,
    base_cost_policy=BASE_COST_POLICY,
    stress_cost_policy=STRESS_COST_POLICY,
    precommitted_evidence_roles=None,
    no_shock_pairs=frozenset(),
):
    if sink is None:
        context, sink = _context(tmp_path, symbol=symbol)
    roles = (
        ["EVALUATION_REQUIRED" for _ in range(attempts)]
        if precommitted_evidence_roles is None
        else list(precommitted_evidence_roles)
    )
    first_plan = _fold_plan()[0]
    seed_result = _raw_fold(attempt_ordinal=0, fold_ordinal=0, planned=first_plan)[
        "lanes"
    ]["optimizer_train_base"]
    seed_result["numerical_execution_cost_attestation"] = _genuine_partition_cost(
        label="source-seed-precommit",
        bars=17,
        stress=False,
    )
    frozen = freeze_winner_search_plan_v3(
        sink,
        symbol=symbol,
        timeframe="H1",
        regime="TREND",
        precommitted_at_utc="2020-01-01T00:00:00Z",
        attempts_frozen_at_utc="2020-01-02T00:00:00Z",
        fold_plan=_fold_plan(),
        attempt_artifacts=[
            _attempt_artifact(index, symbol=symbol) for index in range(attempts)
        ],
        precommitted_evidence_roles=roles,
        horizon_execution_contracts=[
            {
                "lookback_kind": "NATIVE_OBSERVATIONS",
                "lookback_value": 5_000,
                "source_bar_count": 5_000,
            }
            for _ in range(attempts)
        ],
        source_contract_seed=build_trade_source_contract_from_backtester_result(
            seed_result
        ),
        base_cost_contract=base_cost_policy,
        stress_cost_contract=stress_cost_policy,
        risk_unit_contract=RISK_CONTRACT,
        cost_stress_adversity_plan=_cost_stress_adversity_plan(
            attempts=attempts,
            roles=roles,
            no_shock_pairs=no_shock_pairs,
            symbol=symbol,
        ),
        artifact_namespace=artifact_namespace,
    )
    return context, sink, frozen


def test_frozen_cost_policy_v2_is_exact_and_rejects_full_attestation(
    tmp_path: Path,
) -> None:
    assert BASE_COST_POLICY["schema_version"] == FROZEN_COST_POLICY_SCHEMA
    assert set(BASE_COST_POLICY) == {
        "schema_version",
        "lane",
        "attestation_schema_version",
        "use_spread",
        "spread_mode",
        "instrument_spec_sha256",
        "numerical_execution_cost_policy_sha256",
        "policy_identity_sha256",
    }
    with pytest.raises(WinnerRawEvidenceError, match="BASE_COST_POLICY_INVALID"):
        _freeze(tmp_path, base_cost_policy=BASE_COST)


@pytest.mark.parametrize("symbol", ("Platinum", "Palladium"))
def test_frozen_plan_preserves_exact_mt5_symbol(
    tmp_path: Path,
    symbol: str,
) -> None:
    _context_value, _sink, frozen = _freeze(tmp_path, symbol=symbol)

    assert frozen.symbol == symbol
    assert frozen.precommitment["symbol"] == symbol
    assert frozen.attempt_ledger["symbol"] == symbol
    assert json.loads(frozen.frozen_receipt_bytes)["symbol"] == symbol


@pytest.mark.parametrize("wrong_case", ("PLATINUM", "platinum"))
def test_frozen_plan_rejects_wrong_case_mt5_symbol(
    tmp_path: Path,
    wrong_case: str,
) -> None:
    context, sink = _context(tmp_path, symbol="Platinum")

    with pytest.raises(
        WinnerRawEvidenceError,
        match="AUTHORITY_SINK_SYMBOL_MISMATCH",
    ):
        _freeze(
            tmp_path,
            symbol=wrong_case,
            context=context,
            sink=sink,
        )


def _attempt_evidence(frozen, ordinal=0):
    attempt = frozen.attempt_ledger["attempts"][ordinal]
    return {
        "attempt_id": attempt["attempt_id"],
        "folds": [
            _raw_fold(
                attempt_ordinal=ordinal,
                fold_ordinal=fold["fold_ordinal"],
                planned=fold,
                frozen=frozen,
            )
            for fold in frozen.precommitment["causal_fold_plan"]
        ],
    }


def _cross_binding_bytes(frozen):
    precommitment = frozen.precommitment
    ledger = frozen.attempt_ledger
    mapping = []
    for attempt in ledger["attempts"]:
        ordinal = attempt["ordinal"]
        artifacts = frozen.attempt_artifacts_bundle[ordinal]
        horizon = artifacts["horizon_process"]
        horizon_contract = frozen.horizon_execution_contracts[ordinal]
        candidate_id = f"candidate-{ordinal}"
        candidate_sha256 = _sha(f"candidate:{ordinal}")
        source_count_body = {
            "schema_version": "fxpm.historical-attempt-source-count.v1",
            "anchor_id": "anchor-test-001",
            "symbol": frozen.symbol,
            "timeframe": frozen.timeframe,
            "regime": frozen.regime,
            "horizon_plan_receipt_sha256": _sha("horizon-plan"),
            "native_prefix_index_sha256": _sha("native-prefix-index"),
            "native_prefix_content_sha256": horizon["source_snapshot_sha256"],
            "horizon_attempt_id": _sha(f"horizon-attempt:{ordinal}"),
            "horizon_candidate_id": candidate_id,
            "horizon_candidate_definition_sha256": candidate_sha256,
            **horizon_contract,
            "fit_start_utc": datetime.fromisoformat(
                horizon["fit_start_utc"].replace("Z", "+00:00")
            )
            .astimezone(timezone.utc)
            .isoformat(timespec="seconds")
            .replace("+00:00", "Z"),
            "fit_end_exclusive_utc": datetime.fromisoformat(
                horizon["fit_end_utc"].replace("Z", "+00:00")
            )
            .astimezone(timezone.utc)
            .isoformat(timespec="seconds")
            .replace("+00:00", "Z"),
            "latest_closed_bar_utc": datetime.fromisoformat(
                horizon["latest_closed_bar_utc"].replace("Z", "+00:00")
            )
            .astimezone(timezone.utc)
            .isoformat(timespec="seconds")
            .replace("+00:00", "Z"),
            "fit_frame_sha256": _sha(f"fit-frame:{ordinal}"),
        }
        source_count_receipt = {
            **source_count_body,
            "receipt_sha256": _sha(source_count_body),
        }
        folds = []
        planned_fold_roster = (
            precommitment["causal_fold_plan"]
            if attempt["precommitted_evidence_role"] == "EVALUATION_REQUIRED"
            else []
        )
        for planned in planned_fold_roster:
            end = date.fromisoformat(planned["oos_end_date_utc"]) + timedelta(days=1)
            folds.append(
                {
                    "horizon_fold_id": planned["fold_id"],
                    "horizon_fold_definition_sha256": _sha(
                        f"fold:{ordinal}:{planned['fold_ordinal']}"
                    ),
                    "horizon_context_sha256": _sha(
                        f"horizon-context:{ordinal}:{planned['fold_ordinal']}"
                    ),
                    "winner_fold_id": planned["fold_id"],
                    "validation_start_utc": planned["anchor_as_of_utc"],
                    "validation_end_exclusive_utc": end.isoformat() + "T00:00:00Z",
                }
            )
        mapping.append(
            {
                "ordinal": ordinal,
                "horizon_attempt_id": _sha(f"horizon-attempt:{ordinal}"),
                "horizon_candidate_id": candidate_id,
                "horizon_candidate_definition_sha256": candidate_sha256,
                "horizon_trial_id": f"trial-{ordinal}",
                "horizon_trial_definition_sha256": _sha(f"horizon-trial:{ordinal}"),
                "winner_attempt_id": attempt["attempt_id"],
                "winner_process_candidate_id": attempt["process_candidate_id"],
                "process_sha256": attempt["process_sha256"],
                "horizon_process_sha256": attempt["horizon_process_sha256"],
                "trial_definition_sha256": attempt["trial_definition_sha256"],
                "refit_recipe_sha256": attempt["refit_recipe_sha256"],
                "horizon_execution_contract": horizon_contract,
                "source_count_receipt": source_count_receipt,
                "fold_mapping": folds,
                "fold_mapping_sha256": _sha(folds),
            }
        )
    horizon_roster = {
        "plan_receipt_sha256": _sha("horizon-plan"),
        "source_prefix_receipt_sha256": _sha("source-prefix"),
        "candidate_registry_sha256": _sha("candidate-registry"),
        "trial_registry_sha256": precommitment["trial_registry_sha256"],
        "attempt_roster_sha256": _sha("horizon-attempt-roster"),
        "evidence_roster_sha256": _sha("evidence-roster"),
    }
    winner_roster = {
        "precommitment_sha256": _sha(precommitment),
        "attempt_ledger_sha256": _sha(ledger),
        "frozen_receipt_sha256": hashlib.sha256(
            frozen.frozen_receipt_bytes
        ).hexdigest(),
        "process_registry_sha256": precommitment["process_registry_sha256"],
        "horizon_registry_sha256": precommitment["horizon_registry_sha256"],
        "trial_registry_sha256": precommitment["trial_registry_sha256"],
        "attempts_sha256": ledger["attempts_sha256"],
        "causal_fold_plan_sha256": precommitment["causal_fold_plan_sha256"],
        "source_contract_sha256": precommitment["source_contract_sha256"],
        "cost_contract_sha256": precommitment["cost_contract_sha256"],
        "cost_stress_contract_sha256": precommitment["cost_stress_contract_sha256"],
    }
    binding = {
        "schema_version": "fxpm.historical-horizon-winner-cross-binding.v1",
        "producer_request_sha256": _sha("producer-request"),
        "anchor_id": "anchor-test-001",
        "selection_cutoff_utc": "2020-01-01T00:00:00Z",
        "process_precommit_receipt_sha256": _sha("process-precommit"),
        "source_group_decision_sha256": _sha("source-group-decision"),
        "regime_fit_decision_sha256": _sha("regime-fit-decision"),
        "native_prefix_index_sha256": _sha("native-prefix-index"),
        "symbol": frozen.symbol,
        "timeframe": frozen.timeframe,
        "regime": frozen.regime,
        "artifact_namespace": frozen.artifact_namespace,
        "horizon_roster": horizon_roster,
        "winner_roster": winner_roster,
        "mapping_count": len(mapping),
        "mapping_sha256": _sha(mapping),
        "mapping": mapping,
        "bijective_coverage": True,
        "created_before_winner_evidence_access": True,
        "confers_causal_current_live_or_capital_authority": False,
    }
    return _canonical(binding)


class _CurrentHorizonPlan:
    def __init__(self, body):
        self._body = json.loads(_canonical(body).decode("ascii"))

    def to_dict(self):
        return json.loads(_canonical(self._body).decode("ascii"))


def _current_cross_binding_fixture(frozen):
    """Make a one-attempt CURRENT plan congruent with the baseline raw fixture."""

    binding = json.loads(_cross_binding_bytes(frozen).decode("ascii"))
    row = binding["mapping"][0]
    artifacts = frozen.attempt_artifacts_bundle[0]
    horizon = artifacts["horizon_process"]
    process_artifact = artifacts["winner_process"]
    trial_definition = artifacts["trial_definition"]
    candidate = {
        "candidate_id": row["horizon_candidate_id"],
        "candidate_definition_sha256": row["horizon_candidate_definition_sha256"],
        "lookback_kind": horizon["lookback_kind"],
        "lookback_value": horizon["lookback_value"],
        # Historical discovery geometry is intentionally distinct from the
        # latest-cutoff current-refit interval carried by the artifact.
        "realized_start_utc": (
            datetime.fromisoformat(horizon["fit_start_utc"].replace("Z", "+00:00"))
            - timedelta(days=90)
        )
        .astimezone(timezone.utc)
        .isoformat(timespec="seconds")
        .replace("+00:00", "Z"),
        "folds": [
            {
                "fold_id": fold["horizon_fold_id"],
                "fold_definition_sha256": fold["horizon_fold_definition_sha256"],
            }
            for fold in row["fold_mapping"]
        ],
    }
    trial = {
        "trial_id": row["horizon_trial_id"],
        "trial_definition_sha256": row["horizon_trial_definition_sha256"],
        "strategy_id": process_artifact["strategy_id"],
        "strategy_release_sha256": process_artifact["strategy_code_sha256"],
        "parameter_set_sha256": _sha(trial_definition["parameters"]),
    }
    plan_attempt = {
        "attempt_id": row["horizon_attempt_id"],
        "candidate_id": candidate["candidate_id"],
        "trial_id": trial["trial_id"],
        "strategy_id": process_artifact["strategy_id"],
        "status": "PLANNED",
    }
    expected_evaluations = [
        {
            "attempt_id": plan_attempt["attempt_id"],
            "fold_id": fold["horizon_fold_id"],
            "context_sha256": fold["horizon_context_sha256"],
            "validation_start_utc": fold["validation_start_utc"],
            "validation_end_exclusive_utc": fold["validation_end_exclusive_utc"],
            "expected_status": "EVALUATION_REQUIRED",
            "downstream_causal_eligible": True,
        }
        for fold in row["fold_mapping"]
    ]
    candidates = [candidate]
    trials = [trial]
    plan_attempts = [plan_attempt]
    horizon_roster = {
        "plan_receipt_sha256": binding["horizon_roster"]["plan_receipt_sha256"],
        "source_prefix_receipt_sha256": binding["horizon_roster"][
            "source_prefix_receipt_sha256"
        ],
        "candidate_registry_sha256": _sha(candidates),
        "trial_registry_sha256": _sha(trials),
        "attempt_roster_sha256": _sha(plan_attempts),
        "evidence_roster_sha256": _sha(expected_evaluations),
    }
    binding["horizon_roster"] = horizon_roster
    plan_body = {
        "anchor": {
            "anchor_id": "anchor-test-001",
            "selection_cutoff_utc": "2020-01-01T00:00:00Z",
            "process_precommit_receipt_sha256": _sha("process-precommit"),
        },
        "cell": {
            "symbol": frozen.symbol,
            "timeframe": frozen.timeframe,
            "regime": frozen.regime,
        },
        "native_prefix": {
            "source_state": "AVAILABLE",
            "prefix_index_sha256": binding["native_prefix_index_sha256"],
            "prefix_content_sha256": horizon["source_snapshot_sha256"],
            "prefix_end_exclusive_utc": horizon["fit_end_utc"],
            "prefix_last_close_utc": horizon["latest_closed_bar_utc"],
        },
        "horizon_policy": {
            "cost_contract_sha256": frozen.precommitment["cost_contract_sha256"],
            "cost_stress_contract_sha256": frozen.precommitment[
                "cost_stress_contract_sha256"
            ],
        },
        "source_prefix_receipt_sha256": horizon_roster["source_prefix_receipt_sha256"],
        "candidate_registry_sha256": horizon_roster["candidate_registry_sha256"],
        "trial_registry_sha256": horizon_roster["trial_registry_sha256"],
        "attempt_roster_sha256": horizon_roster["attempt_roster_sha256"],
        "evidence_roster_sha256": horizon_roster["evidence_roster_sha256"],
        "candidates": candidates,
        "trials": trials,
        "attempts": plan_attempts,
        "expected_evaluations": expected_evaluations,
        "plan_status": "READY_FOR_HISTORICAL_EVIDENCE",
    }
    process = planner.FrozenProductionSearchProcess(
        _canonical({"run_id": frozen.precommitment["run_id"]}),
        _canonical({"test_spool_receipt": True}),
        planner._PROCESS_SEAL,
    )
    horizon_plan_binding = {
        "anchor_id": plan_body["anchor"]["anchor_id"],
        "selection_cutoff_utc": plan_body["anchor"]["selection_cutoff_utc"],
        "process_precommit_receipt_sha256": plan_body["anchor"][
            "process_precommit_receipt_sha256"
        ],
        "plan_status": plan_body["plan_status"],
        **horizon_roster,
    }
    receipt_body = {
        "artifact_namespace": frozen.artifact_namespace,
        "cell": plan_body["cell"],
        "causal_process_precommit_receipt_sha256": plan_body["anchor"][
            "process_precommit_receipt_sha256"
        ],
        "search_process_receipt_sha256": process.receipt_sha256,
        "native_prefix": {
            "index_sha256": binding["native_prefix_index_sha256"],
            "content_sha256": horizon["source_snapshot_sha256"],
        },
        "source_group_decision_sha256": binding["source_group_decision_sha256"],
        "regime_fit_decision_sha256": binding["regime_fit_decision_sha256"],
        "horizon_plan_binding": horizon_plan_binding,
        "attempts_frozen_before_search": True,
        "state_replay_required_before_scored_partition": True,
        "confers_winner_causal_current_capital_publication_or_live_authority": False,
    }
    receipt = {**receipt_body, "receipt_sha256": _sha(receipt_body)}
    plan = planner.ProductionCellSearchPlan(
        mode="CURRENT_REFIT",
        symbol=frozen.symbol,
        timeframe=frozen.timeframe,
        regime=frozen.regime,
        causal_precommit_receipt_sha256=plan_body["anchor"][
            "process_precommit_receipt_sha256"
        ],
        search_process_receipt_sha256=process.receipt_sha256,
        horizon_plan=_CurrentHorizonPlan(plan_body),
        _attempt_artifacts_json=tuple(
            _canonical(value) for value in frozen.attempt_artifacts_bundle
        ),
        _state_requirements_json=(),
        _cell_receipt_json=_canonical(receipt),
        _spool_entry_json=_canonical({"test_spool_entry": True}),
        boundary_limited=False,
        _seal=planner._CELL_PLAN_SEAL,
    )
    for name in (
        "producer_request_sha256",
        "anchor_id",
        "selection_cutoff_utc",
        "process_precommit_receipt_sha256",
    ):
        binding.pop(name)
    binding.update(
        {
            "schema_version": "fxpm.horizon-winner-cross-binding.v2",
            "mode": "CURRENT",
            "cell_plan_receipt_sha256": plan.receipt_sha256,
            "causal_precommit_receipt_sha256": (plan.causal_precommit_receipt_sha256),
            "search_process_receipt_sha256": process.receipt_sha256,
        }
    )
    return _canonical(binding), plan, process


def _current_attempt_evidence(frozen, binding_bytes):
    binding = json.loads(binding_bytes.decode("ascii"))
    evidence = _copied_attempt_evidence(frozen)
    for fold in evidence["folds"]:
        selection = fold["provenance"]["anchor_selection_receipt"]
        selection["horizon_plan_receipt_sha256"] = binding["horizon_roster"][
            "plan_receipt_sha256"
        ]
        fold["provenance"]["anchor_selection_receipt"] = _reseal(selection)
        trial = fold["provenance"]["anchor_trial_ledger"]
        trial["trial_registry_sha256"] = binding["horizon_roster"][
            "trial_registry_sha256"
        ]
        trial["attempt_roster_sha256"] = binding["horizon_roster"][
            "attempt_roster_sha256"
        ]
        fold["provenance"]["anchor_trial_ledger"] = _reseal(trial)
    return evidence


def _produce(sink, frozen, evidence, *, artifact_namespace=None):
    return produce_winner_v11_raw_evidence(
        sink,
        frozen=frozen,
        attempt_evidence=evidence,
        evidence_access_started_at_utc="2025-01-01T00:00:00Z",
        evidence_access_completed_at_utc="2025-01-01T00:00:01Z",
        provenance_cross_binding_bytes=_cross_binding_bytes(frozen),
        artifact_namespace=artifact_namespace,
    )


def _produce_current(
    sink,
    frozen,
    evidence,
    *,
    binding_bytes,
    plan,
    process,
):
    return produce_winner_v11_raw_evidence(
        sink,
        frozen=frozen,
        attempt_evidence=evidence,
        evidence_access_started_at_utc="2025-01-01T00:00:00Z",
        evidence_access_completed_at_utc="2025-01-01T00:00:01Z",
        provenance_cross_binding_bytes=binding_bytes,
        production_cell_search_plan=plan,
        production_search_process=process,
        artifact_namespace=frozen.artifact_namespace,
    )


def _reseal(receipt):
    body = dict(receipt)
    body.pop("receipt_sha256", None)
    return {**body, "receipt_sha256": _sha(body)}


def _copied_attempt_evidence(frozen, ordinal=0):
    return json.loads(_canonical(_attempt_evidence(frozen, ordinal)).decode("ascii"))


def test_real_raw_chain_round_trips_v11_and_spool(tmp_path: Path) -> None:
    context, sink, frozen = _freeze(tmp_path)
    produced = _produce(sink, frozen, [_attempt_evidence(frozen)])

    assert produced.decision.to_dict()["validated"] is True
    assert produced.evidence_ledger["entries"][0]["evidence_status"] == "AUTHENTICATED"
    assert produced.evidence_ledger["entries"][0]["fold_count"] == 4
    receipt = sink.seal()
    manifest = spool.verify_symbol_receipt(
        context.root,
        receipt,
        expected_generation_id=context.generation_id,
        expected_symbol="EURUSD",
        expected_native_universe_sha256=context.native_universe_sha256,
        expected_config_sha256=context.config_sha256,
    )
    assert manifest["artifact_count"] > 40
    assert any(
        entry["artifact_type"] == "winner_v11_request"
        for entry in manifest["artifacts"]
    )


def test_two_evaluated_candidates_share_only_candidate_neutral_oos_axis(
    tmp_path: Path,
) -> None:
    _, sink, frozen = _freeze(tmp_path, attempts=2)
    produced = _produce(
        sink,
        frozen,
        [_attempt_evidence(frozen, ordinal) for ordinal in range(2)],
    )

    entries = produced.evidence_ledger["entries"]
    assert all(entry["pbo_fold_count"] == 4 for entry in entries)
    for fold_ordinal in range(4):
        left = entries[0]["pbo_folds"][fold_ordinal]
        right = entries[1]["pbo_folds"][fold_ordinal]
        assert (
            left["shared_oos_partition"]["receipt_sha256"]
            == right["shared_oos_partition"]["receipt_sha256"]
        )
        assert (
            left["attempt_oos_source_receipt_sha256"]
            != right["attempt_oos_source_receipt_sha256"]
        )
    assert produced.decision.to_dict()["validated"] is True


def test_evaluation_required_outcome_only_cannot_remove_pbo_column(
    tmp_path: Path,
) -> None:
    _, sink, frozen = _freeze(tmp_path)
    attempt = frozen.attempt_ledger["attempts"][0]
    outcome = TrialOutcome(
        kind=SearchOutcomeKind.EXPECTED_INELIGIBLE,
        stage=SearchStage.PARAMETER_VALIDATION,
        reason_code="POSTHOC_OUTCOME_ONLY",
        reason="evaluated attempt cannot discard its daily column",
        symbol="EURUSD",
        timeframe="H1",
        regime="TREND",
        strategy_family=attempt["strategy_family"],
    )
    with pytest.raises(
        WinnerRawEvidenceIncomplete,
        match="EVALUATION_REQUIRED_ATTEMPT_OUTCOME_ONLY",
    ):
        _produce(
            sink,
            frozen,
            [{"attempt_id": attempt["attempt_id"], "outcome": outcome}],
        )


def test_v2_historical_mode_replays_v1_semantics_exactly(tmp_path: Path) -> None:
    _, sink, frozen = _freeze(tmp_path)
    binding = json.loads(_cross_binding_bytes(frozen).decode("ascii"))
    binding["schema_version"] = "fxpm.horizon-winner-cross-binding.v2"
    binding["mode"] = "HISTORICAL"

    produced = produce_winner_v11_raw_evidence(
        sink,
        frozen=frozen,
        attempt_evidence=[_attempt_evidence(frozen)],
        evidence_access_started_at_utc="2025-01-01T00:00:00Z",
        evidence_access_completed_at_utc="2025-01-01T00:00:01Z",
        provenance_cross_binding_bytes=_canonical(binding),
    )

    assert produced.decision.to_dict()["validated"] is True


def test_current_mode_reverifies_sealed_plan_before_evidence_access(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _, sink, frozen = _freeze(tmp_path)
    binding_bytes, plan, process = _current_cross_binding_fixture(frozen)
    events = []

    def verify(context, *, process, sink, plan):
        events.append(("PLAN_REVERIFIED", context.context_sha256, sink.symbol))
        return True

    monkeypatch.setattr(planner, "verify_production_cell_search_plan", verify)

    def evidence_stream():
        events.append(("EVIDENCE_ACCESSED",))
        yield _current_attempt_evidence(frozen, binding_bytes)

    produced = _produce_current(
        sink,
        frozen,
        evidence_stream(),
        binding_bytes=binding_bytes,
        plan=plan,
        process=process,
    )

    plan_body = plan.horizon_plan.to_dict()
    assert (
        plan_body["candidates"][0]["realized_start_utc"]
        != frozen.attempt_artifacts_bundle[0]["horizon_process"]["fit_start_utc"]
    )
    assert events[0][0] == "PLAN_REVERIFIED"
    assert events[1][0] == "EVIDENCE_ACCESSED"
    assert produced.decision.to_dict()["validated"] is True


def test_current_mode_rejects_historical_field_mixing_before_plan_reopen(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _, sink, frozen = _freeze(tmp_path)
    binding_bytes, plan, process = _current_cross_binding_fixture(frozen)
    binding = json.loads(binding_bytes.decode("ascii"))
    binding["anchor_id"] = "posthoc-field-injection"
    called = []
    monkeypatch.setattr(
        planner,
        "verify_production_cell_search_plan",
        lambda *_args, **_kwargs: called.append(True) or True,
    )

    with pytest.raises(
        WinnerRawEvidenceError,
        match="PROVENANCE_CROSS_BINDING_SCHEMA_INVALID",
    ):
        _produce_current(
            sink,
            frozen,
            iter(()),
            binding_bytes=_canonical(binding),
            plan=plan,
            process=process,
        )
    assert called == []


def test_current_mode_rejects_cell_plan_receipt_splice_before_evidence(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _, sink, frozen = _freeze(tmp_path)
    binding_bytes, plan, process = _current_cross_binding_fixture(frozen)
    binding = json.loads(binding_bytes.decode("ascii"))
    binding["cell_plan_receipt_sha256"] = "f" * 64
    monkeypatch.setattr(
        planner, "verify_production_cell_search_plan", lambda *_a, **_k: True
    )
    touched = []

    def evidence_stream():
        touched.append(True)
        yield _current_attempt_evidence(frozen, binding_bytes)

    with pytest.raises(
        WinnerRawEvidenceError,
        match="PROVENANCE_CURRENT_CELL_PLAN_SPLICE",
    ):
        _produce_current(
            sink,
            frozen,
            evidence_stream(),
            binding_bytes=_canonical(binding),
            plan=plan,
            process=process,
        )
    assert touched == []


def test_current_mode_rejects_horizon_mapping_splice_before_evidence(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _, sink, frozen = _freeze(tmp_path)
    binding_bytes, plan, process = _current_cross_binding_fixture(frozen)
    binding = json.loads(binding_bytes.decode("ascii"))
    row = binding["mapping"][0]
    row["horizon_candidate_id"] = "posthoc-candidate"
    count = row["source_count_receipt"]
    count["horizon_candidate_id"] = "posthoc-candidate"
    row["source_count_receipt"] = _reseal(count)
    binding["mapping_sha256"] = _sha(binding["mapping"])
    monkeypatch.setattr(
        planner, "verify_production_cell_search_plan", lambda *_a, **_k: True
    )
    touched = []

    def evidence_stream():
        touched.append(True)
        yield _current_attempt_evidence(frozen, binding_bytes)

    with pytest.raises(
        WinnerRawEvidenceError,
        match="PROVENANCE_CURRENT_PLAN_BIJECTION_SPLICE",
    ):
        _produce_current(
            sink,
            frozen,
            evidence_stream(),
            binding_bytes=_canonical(binding),
            plan=plan,
            process=process,
        )
    assert touched == []


def test_current_mode_rejects_posthoc_flag_and_v1_mode_confusion(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _, sink, frozen = _freeze(tmp_path)
    binding_bytes, plan, process = _current_cross_binding_fixture(frozen)
    binding = json.loads(binding_bytes.decode("ascii"))
    binding["created_before_winner_evidence_access"] = False
    monkeypatch.setattr(
        planner, "verify_production_cell_search_plan", lambda *_a, **_k: True
    )
    with pytest.raises(
        WinnerRawEvidenceError,
        match="PROVENANCE_CROSS_BINDING_SCHEMA_INVALID",
    ):
        _produce_current(
            sink,
            frozen,
            iter(()),
            binding_bytes=_canonical(binding),
            plan=plan,
            process=process,
        )
    with pytest.raises(
        WinnerRawEvidenceError,
        match="HISTORICAL_CROSS_BINDING_CURRENT_PLAN_FORBIDDEN",
    ):
        _produce_current(
            sink,
            frozen,
            iter(()),
            binding_bytes=_cross_binding_bytes(frozen),
            plan=plan,
            process=process,
        )


def test_current_mode_rejects_carried_attempt_or_verifier_splice(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _, sink, frozen = _freeze(tmp_path)
    binding_bytes, plan, process = _current_cross_binding_fixture(frozen)
    changed = list(plan._attempt_artifacts_json)
    body = json.loads(changed[0].decode("ascii"))
    body["horizon_process"]["source_snapshot_sha256"] = "f" * 64
    changed[0] = _canonical(body)
    forged = replace(plan, _attempt_artifacts_json=tuple(changed))
    monkeypatch.setattr(
        planner, "verify_production_cell_search_plan", lambda *_a, **_k: True
    )
    with pytest.raises(
        WinnerRawEvidenceError,
        match="PROVENANCE_CURRENT_CELL_PLAN_ATTEMPT_SPLICE",
    ):
        _produce_current(
            sink,
            frozen,
            iter(()),
            binding_bytes=binding_bytes,
            plan=forged,
            process=process,
        )

    def rejected(*_args, **_kwargs):
        raise planner.ProductionSearchPlannerError("CELL_PLAN_SPOOL_REOPEN_INVALID")

    monkeypatch.setattr(planner, "verify_production_cell_search_plan", rejected)
    with pytest.raises(
        WinnerRawEvidenceError,
        match="CURRENT_CELL_PLAN_REVERIFY_FAILED: CELL_PLAN_SPOOL_REOPEN_INVALID",
    ):
        _produce_current(
            sink,
            frozen,
            iter(()),
            binding_bytes=binding_bytes,
            plan=plan,
            process=process,
        )


def test_current_mode_requires_both_sealed_plan_and_process(tmp_path: Path) -> None:
    _, sink, frozen = _freeze(tmp_path)
    binding_bytes, plan, process = _current_cross_binding_fixture(frozen)
    for missing_plan, missing_process in ((None, process), (plan, None)):
        with pytest.raises(WinnerRawEvidenceError, match="CURRENT_CELL_PLAN_REQUIRED"):
            _produce_current(
                sink,
                frozen,
                iter(()),
                binding_bytes=binding_bytes,
                plan=missing_plan,
                process=missing_process,
            )


def test_arbitrary_provenance_body_is_typed_technical_failure(tmp_path: Path) -> None:
    _, sink, frozen = _freeze(tmp_path)
    raw = _copied_attempt_evidence(frozen)
    raw["folds"][0]["provenance"]["anchor_selection_receipt"] = {
        "schema_version": "fxpm.test-provenance.v1",
        "kind": "anchor_selection_receipt",
    }
    with pytest.raises(
        WinnerRawEvidenceIncomplete,
        match="ANCHOR_SELECTION_RECEIPT_PROVENANCE_SCHEMA_INVALID",
    ):
        _produce(sink, frozen, [raw])


def test_missing_pre_evidence_cross_binding_is_typed_failure(tmp_path: Path) -> None:
    _, sink, frozen = _freeze(tmp_path)
    with pytest.raises(
        WinnerRawEvidenceError,
        match="PROVENANCE_CROSS_BINDING_BYTES_INVALID",
    ):
        produce_winner_v11_raw_evidence(
            sink,
            frozen=frozen,
            attempt_evidence=[_attempt_evidence(frozen)],
            evidence_access_started_at_utc="2025-01-01T00:00:00Z",
            evidence_access_completed_at_utc="2025-01-01T00:00:01Z",
        )


def test_cross_binding_candidate_substitution_fails_closed(tmp_path: Path) -> None:
    _, sink, frozen = _freeze(tmp_path)
    binding = json.loads(_cross_binding_bytes(frozen).decode("ascii"))
    binding["mapping"][0]["horizon_candidate_id"] = "substituted-candidate"
    source_count = binding["mapping"][0]["source_count_receipt"]
    source_count["horizon_candidate_id"] = "substituted-candidate"
    binding["mapping"][0]["source_count_receipt"] = _reseal(source_count)
    binding["mapping_sha256"] = _sha(binding["mapping"])
    with pytest.raises(
        WinnerRawEvidenceIncomplete,
        match="HORIZON_DECISION_PROVENANCE_SPLICE",
    ):
        produce_winner_v11_raw_evidence(
            sink,
            frozen=frozen,
            attempt_evidence=[_attempt_evidence(frozen)],
            evidence_access_started_at_utc="2025-01-01T00:00:00Z",
            evidence_access_completed_at_utc="2025-01-01T00:00:01Z",
            provenance_cross_binding_bytes=_canonical(binding),
        )


def test_resealed_replay_lane_hash_substitution_fails_closed(tmp_path: Path) -> None:
    _, sink, frozen = _freeze(tmp_path)
    raw = _copied_attempt_evidence(frozen)
    replay = raw["folds"][0]["provenance"]["attempt_replay_receipt"]
    replay["lane_sha256"]["optimizer_train_base"] = "f" * 64
    raw["folds"][0]["provenance"]["attempt_replay_receipt"] = _reseal(replay)
    with pytest.raises(
        WinnerRawEvidenceIncomplete,
        match="ATTEMPT_REPLAY_PROVENANCE_SPLICE",
    ):
        _produce(sink, frozen, [raw])


def test_resealed_native_source_substitution_fails_closed(tmp_path: Path) -> None:
    _, sink, frozen = _freeze(tmp_path)
    raw = _copied_attempt_evidence(frozen)
    source = raw["folds"][0]["provenance"]["oos_source_receipt"]
    source["native_source_sha256"] = "f" * 64
    raw["folds"][0]["provenance"]["oos_source_receipt"] = _reseal(source)
    with pytest.raises(
        WinnerRawEvidenceIncomplete,
        match="OOS_SOURCE_PROVENANCE_SPLICE",
    ):
        _produce(sink, frozen, [raw])


def test_resealed_partition_geometry_body_substitution_fails_closed(
    tmp_path: Path,
) -> None:
    _, sink, frozen = _freeze(tmp_path)
    raw = _copied_attempt_evidence(frozen)
    horizon = raw["folds"][0]["provenance"]["anchor_horizon_decision"]
    geometry = horizon["partition_geometry"][0]
    geometry["scored_bars"] += 1
    geometry["frame_rows"] += 1
    geometry = _reseal(geometry)
    horizon["partition_geometry"][0] = geometry
    horizon["partition_geometry_receipts"][0] = geometry["receipt_sha256"]
    raw["folds"][0]["provenance"]["anchor_horizon_decision"] = _reseal(horizon)
    with pytest.raises(
        WinnerRawEvidenceIncomplete,
        match="PARTITION_GEOMETRY_0_LANE_SPLICE",
    ):
        _produce(sink, frozen, [raw])


def test_resealed_signal_vector_content_substitution_fails_closed(
    tmp_path: Path,
) -> None:
    _, sink, frozen = _freeze(tmp_path)
    raw = _copied_attempt_evidence(frozen)
    replay = raw["folds"][0]["provenance"]["attempt_replay_receipt"]
    signal = replay["signal_vector_receipt"]
    signal["values"][0] = 1
    replay["signal_vector_receipt"] = _reseal(signal)
    raw["folds"][0]["provenance"]["attempt_replay_receipt"] = _reseal(replay)
    with pytest.raises(
        WinnerRawEvidenceIncomplete,
        match="SIGNAL_VECTOR_CONTENT_HASH_INVALID",
    ):
        _produce(sink, frozen, [raw])


@pytest.mark.parametrize(
    ("field", "replacement"),
    (
        ("strategy_name", "SubstitutedStrategy"),
        ("stress_mode", "stress_max"),
    ),
)
def test_resealed_stress_executable_identity_substitution_fails_closed(
    tmp_path: Path,
    field: str,
    replacement: str,
) -> None:
    _, sink, frozen = _freeze(tmp_path)
    raw = _copied_attempt_evidence(frozen)
    replay = raw["folds"][0]["provenance"]["attempt_replay_receipt"]
    stress = replay["stress_method_receipt"]
    stress[field] = replacement
    stress = _reseal(stress)
    replay["stress_method_receipt"] = stress
    replay["stress_method_receipt_sha256"] = stress["receipt_sha256"]
    raw["folds"][0]["provenance"]["attempt_replay_receipt"] = _reseal(replay)
    with pytest.raises(
        WinnerRawEvidenceIncomplete,
        match="STRESS_METHOD_RECEIPT_BINDING_INVALID",
    ):
        _produce(sink, frozen, [raw])


def test_resealed_execution_spec_cost_identity_substitution_fails_closed(
    tmp_path: Path,
) -> None:
    _, sink, frozen = _freeze(tmp_path)
    raw = _copied_attempt_evidence(frozen)
    source = raw["folds"][0]["provenance"]["oos_source_receipt"]
    execution_spec = source["execution_spec_receipt"]
    execution_spec["instrument_spec_sha256"] = "f" * 64
    source["execution_spec_receipt"] = _reseal(execution_spec)
    raw["folds"][0]["provenance"]["oos_source_receipt"] = _reseal(source)
    with pytest.raises(
        WinnerRawEvidenceIncomplete,
        match="EXECUTION_SPEC_RECEIPT_BINDING_INVALID",
    ):
        _produce(sink, frozen, [raw])


def test_resealed_execution_spec_accepts_bound_zero_native_tick_fallback(
    tmp_path: Path,
) -> None:
    _, sink, frozen = _freeze(tmp_path)
    raw = _copied_attempt_evidence(frozen)
    source = raw["folds"][0]["provenance"]["oos_source_receipt"]
    execution_spec = source["execution_spec_receipt"]
    execution_spec[
        "tick_value_source"
    ] = "FROZEN_BROKER_SPECS_RECORD_NATIVE_ZERO_UNAVAILABLE"
    source["execution_spec_receipt"] = _reseal(execution_spec)
    raw["folds"][0]["provenance"]["oos_source_receipt"] = _reseal(source)

    product = _produce(sink, frozen, [raw])

    assert product.evidence_ledger["entries"][0]["evidence_status"] == "AUTHENTICATED"


def test_resealed_execution_spec_rejects_unknown_tick_value_source(
    tmp_path: Path,
) -> None:
    _, sink, frozen = _freeze(tmp_path)
    raw = _copied_attempt_evidence(frozen)
    source = raw["folds"][0]["provenance"]["oos_source_receipt"]
    execution_spec = source["execution_spec_receipt"]
    execution_spec["tick_value_source"] = "UNBOUND_FALLBACK"
    source["execution_spec_receipt"] = _reseal(execution_spec)
    raw["folds"][0]["provenance"]["oos_source_receipt"] = _reseal(source)

    with pytest.raises(
        WinnerRawEvidenceIncomplete,
        match="EXECUTION_SPEC_RECEIPT_BINDING_INVALID",
    ):
        _produce(sink, frozen, [raw])


def test_postfreeze_horizon_execution_contract_substitution_fails_closed(
    tmp_path: Path,
) -> None:
    _, sink, frozen = _freeze(tmp_path)
    contracts = frozen.horizon_execution_contracts
    contracts[0]["source_bar_count"] -= 1
    forged = replace(
        frozen,
        horizon_execution_contracts_bytes=_canonical(contracts),
    )
    with pytest.raises(
        WinnerRawEvidenceError,
        match="HORIZON_EXECUTION_CONTRACT_0000_VALUE_INVALID",
    ):
        _produce(sink, forged, [_attempt_evidence(frozen)])


def test_cross_binding_native_prefix_index_substitution_fails_closed(
    tmp_path: Path,
) -> None:
    _, sink, frozen = _freeze(tmp_path)
    binding = json.loads(_cross_binding_bytes(frozen).decode("ascii"))
    binding["native_prefix_index_sha256"] = "f" * 64
    with pytest.raises(
        WinnerRawEvidenceError,
        match="PROVENANCE_CROSS_BINDING_SOURCE_COUNT_SPLICE",
    ):
        produce_winner_v11_raw_evidence(
            sink,
            frozen=frozen,
            attempt_evidence=[_attempt_evidence(frozen)],
            evidence_access_started_at_utc="2025-01-01T00:00:00Z",
            evidence_access_completed_at_utc="2025-01-01T00:00:01Z",
            provenance_cross_binding_bytes=_canonical(binding),
        )


def test_postfreeze_attempt_body_substitution_fails_before_evidence(
    tmp_path: Path,
) -> None:
    _, sink, frozen = _freeze(tmp_path)
    bundle = frozen.attempt_artifacts_bundle
    bundle[0]["horizon_process"]["source_snapshot_sha256"] = "f" * 64
    forged = replace(
        frozen,
        attempt_artifacts_bundle_bytes=_canonical(bundle),
    )
    with pytest.raises(
        WinnerRawEvidenceError,
        match="ATTEMPT_0000_ARTIFACT_UNVERIFIABLE|ATTEMPT_0000_ARTIFACT_BUNDLE_HASH_INVALID",
    ):
        _produce(sink, forged, [_attempt_evidence(frozen)])


def test_label_only_cost_stress_is_rejected_even_when_receipts_are_resealed(
    tmp_path: Path,
) -> None:
    _, sink, frozen = _freeze(tmp_path)
    raw = _copied_attempt_evidence(frozen)
    fold = raw["folds"][0]
    base = fold["lanes"]["final_lockbox_base"]
    stress = fold["lanes"]["final_lockbox_stress"]
    stress["position_events"] = json.loads(_canonical(base["position_events"]))
    stress["winner_daily_r_surface"] = json.loads(
        _canonical(base["winner_daily_r_surface"])
    )
    replay = fold["provenance"]["attempt_replay_receipt"]
    stress_receipt = replay["stress_method_receipt"]
    stress_receipt["stress_position_roster_sha256"] = (
        Backtester._cost_attestation_json_sha256(
            {"position_events": stress["position_events"]}
        )
    )
    stress_receipt["position_count"] = len(stress["position_events"])
    stress_receipt["daily_observation_count"] = len(
        stress["winner_daily_r_surface"]["daily_net_mtm_r"]
    )
    stress_receipt = _reseal(stress_receipt)
    replay["stress_method_receipt"] = stress_receipt
    replay["stress_method_receipt_sha256"] = stress_receipt["receipt_sha256"]
    replay["lane_sha256"]["final_lockbox_stress"] = _sha(stress)
    fold["provenance"]["attempt_replay_receipt"] = _reseal(replay)
    with pytest.raises(
        WinnerRawEvidenceIncomplete,
        match="STRESS_METHOD_ADVERSITY_STATUS_MISMATCH",
    ):
        _produce(sink, frozen, [raw])


def test_resealed_adversity_status_cannot_relabel_genuine_shock(
    tmp_path: Path,
) -> None:
    _, sink, frozen = _freeze(tmp_path)
    raw = _copied_attempt_evidence(frozen)
    replay = raw["folds"][0]["provenance"]["attempt_replay_receipt"]
    stress_receipt = replay["stress_method_receipt"]
    stress_receipt["realized_adversity_status"] = (
        BACKTEST_FROZEN_PATH_COST_STRESS_ADVERSITY_INSUFFICIENT
    )
    stress_receipt["strictly_worse_trade_count"] = 0
    stress_receipt["strictly_worse_daily_observation_count"] = 0
    stress_receipt = _reseal(stress_receipt)
    replay["stress_method_receipt"] = stress_receipt
    replay["stress_method_receipt_sha256"] = stress_receipt["receipt_sha256"]
    raw["folds"][0]["provenance"]["attempt_replay_receipt"] = _reseal(replay)

    with pytest.raises(
        WinnerRawEvidenceIncomplete,
        match="STRESS_METHOD_ADVERSITY_STATUS_MISMATCH",
    ):
        _produce(sink, frozen, [raw])


def _reseal_first_fold_as_no_numerical_shock(
    raw,
    *,
    preserve_changed_spread_vector: bool = False,
):
    fold = raw["folds"][0]
    base = fold["lanes"]["final_lockbox_base"]
    stress = fold["lanes"]["final_lockbox_stress"]
    stress["position_events"] = json.loads(_canonical(base["position_events"]))
    stress["winner_daily_r_surface"] = json.loads(
        _canonical(base["winner_daily_r_surface"])
    )
    if not preserve_changed_spread_vector:
        stress["numerical_execution_cost_attestation"] = _rebuilt_cost(
            stress["numerical_execution_cost_attestation"],
            spread_values_sha256=base["numerical_execution_cost_attestation"][
                "spread_values_sha256"
            ],
        )
    replay = fold["provenance"]["attempt_replay_receipt"]
    stress_receipt = replay["stress_method_receipt"]
    stress_receipt["stress_cost_attestation_sha256"] = stress[
        "numerical_execution_cost_attestation"
    ]["attestation_sha256"]
    stress_receipt["stress_spread_values_sha256"] = stress[
        "numerical_execution_cost_attestation"
    ]["spread_values_sha256"]
    stress_receipt["stress_position_roster_sha256"] = (
        Backtester._cost_attestation_json_sha256(
            {"position_events": stress["position_events"]}
        )
    )
    stress_receipt["position_count"] = len(stress["position_events"])
    stress_receipt["daily_observation_count"] = len(
        stress["winner_daily_r_surface"]["daily_net_mtm_r"]
    )
    stress_receipt["strictly_worse_trade_count"] = 0
    stress_receipt["strictly_worse_daily_observation_count"] = 0
    stress_receipt["stress_daily_improvement_observation_count"] = 0
    stress_receipt["realized_adversity_status"] = (
        BACKTEST_FROZEN_PATH_COST_STRESS_ADVERSITY_INSUFFICIENT
    )
    stress_receipt = _reseal(stress_receipt)
    replay["stress_method_receipt"] = stress_receipt
    replay["stress_method_receipt_sha256"] = stress_receipt["receipt_sha256"]
    replay["lane_sha256"]["final_lockbox_stress"] = _sha(stress)
    fold["provenance"]["attempt_replay_receipt"] = _reseal(replay)
    return raw


def _reseal_first_fold_as_zero_trade(raw):
    raw = _reseal_first_fold_as_no_numerical_shock(raw)
    fold = raw["folds"][0]
    train = fold["lanes"]["optimizer_train_base"]
    base = fold["lanes"]["final_lockbox_base"]
    stress = fold["lanes"]["final_lockbox_stress"]
    train_dates = train["winner_daily_r_surface"]["daily_dates_utc"]
    train_zero_surface = build_flat_daily_r_surface(
        [],
        start_date_utc=train_dates[0],
        end_date_utc=train_dates[-1],
        risk_unit_contract=RISK_CONTRACT,
    )
    final_dates = base["winner_daily_r_surface"]["daily_dates_utc"]
    final_zero_surface = build_flat_daily_r_surface(
        [],
        start_date_utc=final_dates[0],
        end_date_utc=final_dates[-1],
        risk_unit_contract=RISK_CONTRACT,
    )
    train["position_events"] = []
    train["winner_daily_r_surface"] = json.loads(_canonical(train_zero_surface))
    base["position_events"] = []
    base["winner_daily_r_surface"] = json.loads(_canonical(final_zero_surface))
    stress["position_events"] = []
    stress["winner_daily_r_surface"] = json.loads(_canonical(final_zero_surface))

    replay = fold["provenance"]["attempt_replay_receipt"]
    stress_receipt = replay["stress_method_receipt"]
    empty_roster_sha256 = Backtester._cost_attestation_json_sha256(
        {"position_events": []}
    )
    stress_receipt["base_position_roster_sha256"] = empty_roster_sha256
    stress_receipt["stress_position_roster_sha256"] = empty_roster_sha256
    stress_receipt["position_count"] = 0
    stress_receipt["daily_observation_count"] = len(
        final_zero_surface["daily_net_mtm_r"]
    )
    stress_receipt["strictly_worse_trade_count"] = 0
    stress_receipt["strictly_worse_daily_observation_count"] = 0
    stress_receipt["stress_daily_improvement_observation_count"] = 0
    stress_receipt["realized_adversity_status"] = (
        BACKTEST_FROZEN_PATH_COST_STRESS_ADVERSITY_INSUFFICIENT
    )
    stress_receipt = _reseal(stress_receipt)
    replay["stress_method_receipt"] = stress_receipt
    replay["stress_method_receipt_sha256"] = stress_receipt["receipt_sha256"]
    replay["lane_sha256"]["optimizer_train_base"] = _sha(train)
    replay["lane_sha256"]["final_lockbox_base"] = _sha(base)
    replay["lane_sha256"]["final_lockbox_stress"] = _sha(stress)
    fold["provenance"]["attempt_replay_receipt"] = _reseal(replay)
    return raw


def test_changed_spread_without_realized_degradation_is_process_rejected(
    tmp_path: Path,
) -> None:
    _, sink, frozen = _freeze(tmp_path)
    raw = _reseal_first_fold_as_no_numerical_shock(
        _copied_attempt_evidence(frozen),
        preserve_changed_spread_vector=True,
    )

    receipt = raw["folds"][0]["provenance"]["attempt_replay_receipt"][
        "stress_method_receipt"
    ]
    assert receipt["base_spread_values_sha256"] != receipt[
        "stress_spread_values_sha256"
    ]
    assert receipt["realized_adversity_status"] == (
        BACKTEST_FROZEN_PATH_COST_STRESS_ADVERSITY_INSUFFICIENT
    )

    produced = _produce(sink, frozen, [raw])

    entry = produced.evidence_ledger["entries"][0]
    assert entry["evidence_status"] == "PROCESS_REJECTED"
    assert entry["status_reason"] == "EVALUATED_OOS_PROCESS_REJECTED"
    assert produced.decision.to_dict()["validated"] is True
    assert entry["folds"] == []
    assert entry["pbo_fold_count"] == len(frozen.precommitment["causal_fold_plan"])


def test_authenticated_no_numerical_shock_keeps_pbo_and_rejects_attempt(
    tmp_path: Path,
) -> None:
    _, sink, frozen = _freeze(tmp_path, no_shock_pairs={(0, 0)})
    raw = _reseal_first_fold_as_no_numerical_shock(
        _copied_attempt_evidence(frozen)
    )

    produced = _produce(sink, frozen, [raw])

    entry = produced.evidence_ledger["entries"][0]
    assert entry["evidence_status"] == "PROCESS_REJECTED"
    assert entry["status_reason"] == "EVALUATED_OOS_PROCESS_REJECTED"
    assert produced.decision.to_dict()["validated"] is True
    assert entry["fold_count"] == 0
    assert entry["folds"] == []
    assert entry["pbo_fold_count"] == len(frozen.precommitment["causal_fold_plan"])
    assert len(entry["pbo_folds"]) == entry["pbo_fold_count"]


def test_authenticated_zero_trade_fold_is_process_rejected_with_pbo(
    tmp_path: Path,
) -> None:
    _, sink, frozen = _freeze(tmp_path, no_shock_pairs={(0, 0)})
    raw = _reseal_first_fold_as_zero_trade(_copied_attempt_evidence(frozen))

    produced = _produce(sink, frozen, [raw])

    entry = produced.evidence_ledger["entries"][0]
    assert entry["evidence_status"] == "PROCESS_REJECTED"
    assert entry["status_reason"] == "EVALUATED_OOS_PROCESS_REJECTED"
    assert entry["folds"] == []
    assert entry["pbo_fold_count"] == len(frozen.precommitment["causal_fold_plan"])
    zero_fold = entry["pbo_folds"][0]
    assert zero_fold["completed_trade_position_ids"] == []
    assert zero_fold["completed_trade_net_mtm_r"] == []
    assert zero_fold["completed_trade_net_mtm_r_cost_stress"] == []
    assert all(value == 0.0 for value in zero_fold["daily_net_mtm_r"])
    assert all(value == 0.0 for value in zero_fold["daily_net_mtm_r_cost_stress"])


def test_genuine_base_stress_trade_count_mismatch_remains_technical(
    tmp_path: Path,
) -> None:
    _, sink, frozen = _freeze(tmp_path)
    raw = _copied_attempt_evidence(frozen)
    raw["folds"][0]["lanes"]["final_lockbox_stress"]["position_events"].pop()

    with pytest.raises(
        WinnerRawEvidenceIncomplete,
        match="FINAL_BASE_STRESS_TRADE_COUNT_MISMATCH",
    ):
        _produce(sink, frozen, [raw])


def test_event_vectors_keep_mismatch_and_trade_count_bounds_fail_closed() -> None:
    fold_start = datetime(2024, 1, 1, tzinfo=timezone.utc)
    fold_end = datetime(2024, 2, 1, tzinfo=timezone.utc)
    with pytest.raises(
        WinnerRawEvidenceError,
        match="FINAL_BASE_STRESS_TRADE_COUNT_MISMATCH",
    ):
        _event_vectors(
            [],
            [{}],
            fold_start=fold_start,
            fold_end_exclusive=fold_end,
        )

    oversized = [{}] * (winner.MAX_TRADES_PER_FOLD + 1)
    with pytest.raises(
        WinnerRawEvidenceError,
        match="FINAL_TRADE_COUNT_LIMIT_EXCEEDED",
    ):
        _event_vectors(
            oversized,
            oversized,
            fold_start=fold_start,
            fold_end_exclusive=fold_end,
        )


def test_genuine_partition_cost_vectors_vary_under_one_frozen_policy(
    tmp_path: Path,
) -> None:
    _, sink, frozen = _freeze(tmp_path)
    raw = _attempt_evidence(frozen)
    base_attestations = [
        fold["lanes"][lane]["numerical_execution_cost_attestation"]
        for fold in raw["folds"]
        for lane in (
            "optimizer_train_base",
            "tuning_validation_base",
            "final_lockbox_base",
        )
    ]
    base_bar_counts = [
        fold["lanes"][lane]["trade_power_partition_axis"]["scored_bars"]
        for fold in raw["folds"]
        for lane in ("optimizer_train_base", "tuning_validation_base")
    ]
    assert len(set(base_bar_counts)) > 1
    assert len({item["spread_values_sha256"] for item in base_attestations}) == len(
        base_attestations
    )
    assert len({item["attestation_sha256"] for item in base_attestations}) == len(
        base_attestations
    )
    assert {item["policy_sha256"] for item in base_attestations} == {
        BASE_COST_POLICY["numerical_execution_cost_policy_sha256"]
    }
    assert frozen.source_contract_seed["numerical_execution_cost_attestation"][
        "spread_values_sha256"
    ] not in {item["spread_values_sha256"] for item in base_attestations}
    assert "spread_values_sha256" not in frozen.base_cost_policy
    assert "attestation_sha256" not in frozen.base_cost_policy

    produced = _produce(sink, frozen, [raw])
    assert produced.decision.to_dict()["validated"] is True


def test_internally_valid_partition_policy_mutation_fails_closed(
    tmp_path: Path,
) -> None:
    _, sink, frozen = _freeze(tmp_path)
    raw = _attempt_evidence(frozen)
    lane = raw["folds"][0]["lanes"]["optimizer_train_base"]
    lane["numerical_execution_cost_attestation"] = _rebuilt_cost(
        lane["numerical_execution_cost_attestation"],
        instrument_spec_sha256="f" * 64,
    )

    with pytest.raises(
        WinnerRawEvidenceIncomplete, match="BASE_COST_ATTESTATION_INVALID_POLICY_DRIFT"
    ):
        _produce(sink, frozen, [raw])


def test_base_stress_attestation_pair_swap_fails_closed(tmp_path: Path) -> None:
    _, sink, frozen = _freeze(tmp_path)
    raw = _attempt_evidence(frozen)
    lanes = raw["folds"][0]["lanes"]
    base = lanes["final_lockbox_base"]
    stress = lanes["final_lockbox_stress"]
    base_cost = base["numerical_execution_cost_attestation"]
    stress_cost = stress["numerical_execution_cost_attestation"]
    base["numerical_execution_cost_attestation"] = stress_cost
    stress["numerical_execution_cost_attestation"] = base_cost

    with pytest.raises(
        WinnerRawEvidenceIncomplete, match="BASE_COST_ATTESTATION_INVALID_POLICY_DRIFT"
    ):
        _produce(sink, frozen, [raw])


def test_stress_lane_improvement_still_fails_closed(tmp_path: Path) -> None:
    _, sink, frozen = _freeze(tmp_path)
    raw = _attempt_evidence(frozen)
    planned = frozen.precommitment["causal_fold_plan"][0]
    lanes = raw["folds"][0]["lanes"]
    base = lanes["final_lockbox_base"]
    stress = lanes["final_lockbox_stress"]
    for base_event, stress_event in zip(
        base["position_events"], stress["position_events"], strict=True
    ):
        stress_event["pnl_dollars"] = base_event["pnl_dollars"] + 1.0
    stress["winner_daily_r_surface"] = build_flat_daily_r_surface(
        stress["position_events"],
        start_date_utc=planned["oos_start_date_utc"],
        end_date_utc=planned["oos_end_date_utc"],
        risk_unit_contract=RISK_CONTRACT,
    )

    with pytest.raises(
        WinnerRawEvidenceIncomplete, match="COST_STRESS_IMPROVES_DAILY_R"
    ):
        _produce(sink, frozen, [raw])


def test_base_and_stress_execution_policies_must_be_distinct(tmp_path: Path) -> None:
    same_execution_policy = _frozen_policy(BASE_COST, "STRESS")
    with pytest.raises(WinnerRawEvidenceError, match="COST_STRESS_POLICY_NOT_DISTINCT"):
        _freeze(tmp_path, stress_cost_policy=same_execution_policy)


def test_frozen_attempt_omission_fails_before_v11(tmp_path: Path) -> None:
    _, sink, frozen = _freeze(
        tmp_path,
        attempts=2,
        precommitted_evidence_roles=[
            "PRESEARCH_PROCESS_REJECTED",
            "EVALUATION_REQUIRED",
        ],
    )
    first = frozen.attempt_ledger["attempts"][0]
    outcome = TrialOutcome(
        kind=SearchOutcomeKind.EXPECTED_INELIGIBLE,
        stage=SearchStage.PARAMETER_VALIDATION,
        reason_code="PARAMETER_SCREEN_REJECTED",
        reason="predeclared parameter screen rejected this process",
        symbol="EURUSD",
        timeframe="H1",
        regime="TREND",
        strategy_family=first["strategy_family"],
    )
    with pytest.raises(
        WinnerRawEvidenceError, match="ATTEMPT_EVIDENCE_ROSTER_OMISSION"
    ):
        _produce(
            sink,
            frozen,
            [{"attempt_id": first["attempt_id"], "outcome": outcome}],
        )


def test_underpowered_is_process_rejected_not_technical(tmp_path: Path) -> None:
    _, sink, frozen = _freeze(tmp_path)
    raw = _attempt_evidence(frozen)
    raw["folds"][0]["lanes"]["optimizer_train_base"]["position_events"] = raw["folds"][
        0
    ]["lanes"]["optimizer_train_base"]["position_events"][:2]
    replay = raw["folds"][0]["provenance"]["attempt_replay_receipt"]
    replay["lane_sha256"]["optimizer_train_base"] = _sha(
        raw["folds"][0]["lanes"]["optimizer_train_base"]
    )
    raw["folds"][0]["provenance"]["attempt_replay_receipt"] = _reseal(replay)

    produced = _produce(sink, frozen, [raw])

    entry = produced.evidence_ledger["entries"][0]
    assert entry["evidence_status"] == "PROCESS_REJECTED"
    assert entry["status_reason"] == "EVALUATED_OOS_PROCESS_REJECTED"
    assert entry["folds"] == []
    assert entry["pbo_fold_count"] == len(frozen.precommitment["causal_fold_plan"])
    assert len(entry["pbo_folds"]) == entry["pbo_fold_count"]


def test_malformed_positive_evidence_is_technical_not_no_trade(tmp_path: Path) -> None:
    _, sink, frozen = _freeze(tmp_path)
    raw = _attempt_evidence(frozen)
    raw["folds"][0]["lanes"]["optimizer_train_base"]["position_events"][0][
        "trade_record_schema_version"
    ] = "stale"

    with pytest.raises(
        WinnerRawEvidenceIncomplete, match="ATTEMPT_TECHNICAL_EVIDENCE_FAILURE"
    ):
        _produce(sink, frozen, [raw])


def test_cost_stress_identity_tamper_is_technical(tmp_path: Path) -> None:
    _, sink, frozen = _freeze(tmp_path)
    raw = _attempt_evidence(frozen)
    raw["folds"][0]["lanes"]["final_lockbox_stress"]["position_events"][0][
        "position_id"
    ] = "tampered-position"

    with pytest.raises(
        WinnerRawEvidenceIncomplete, match="ATTEMPT_TECHNICAL_EVIDENCE_FAILURE"
    ):
        _produce(sink, frozen, [raw])


class _OneTradeStrategy:
    name = "real-backtester-path"

    def calculate_stops(self, *_args, **_kwargs):
        return 10.0, 10.0


def test_core_returns_authenticated_no_numerical_shock_after_full_replay() -> None:
    config = PipelineConfig(
        use_spread=True,
        swap_model_enabled=False,
        backtest_spread_mode="conservative_max",
    )
    backtester = Backtester(config)
    index = pd.date_range("2024-01-01T00:00:00Z", periods=4, freq="h")
    features = pd.DataFrame(
        {
            "Open": [100.0, 100.0, 100.0, 100.0],
            "High": [100.0, 100.2, 100.0, 100.0],
            "Low": [100.0, 99.95, 100.0, 100.0],
            "Close": [100.0, 100.1, 100.0, 100.0],
            "Volume": [100, 100, 100, 100],
            "Spread": [1.0, 1.0, 1.0, 1.0],
            "_BAR_CLOSE_TS": index + pd.Timedelta(hours=1),
        },
        index=index,
    )
    signals = pd.Series([1, 0, 0, 0], index=index)
    spec = InstrumentSpec(
        symbol="EURUSD",
        pip_position=2,
        pip_value=10.0,
        spread_avg=1.0,
    )
    base = backtester.run(
        features,
        signals,
        "EURUSD",
        _OneTradeStrategy(),
        spec=spec,
        timeframe="H1",
    )
    assert base["position_events"]

    stress = backtester.replay_frozen_path_cost_stress(
        features,
        signals,
        "EURUSD",
        _OneTradeStrategy(),
        base,
        stress_mode="stress_p95",
        spec=spec,
        timeframe="H1",
    )

    receipt = stress["cost_stress_frozen_path_receipt"]
    assert receipt["schema_version"] == (
        BACKTEST_FROZEN_PATH_COST_STRESS_SCHEMA_VERSION
    )
    assert receipt["realized_adversity_status"] == (
        BACKTEST_FROZEN_PATH_COST_STRESS_ADVERSITY_INSUFFICIENT
    )
    assert receipt["base_spread_values_sha256"] == (
        receipt["stress_spread_values_sha256"]
    )
    assert receipt["strictly_worse_trade_count"] == 0
    assert receipt["strictly_worse_daily_observation_count"] == 0
    assert stress["position_events"] == base["position_events"]
    assert stress["winner_daily_r_surface"] == base["winner_daily_r_surface"]


def test_real_backtester_position_events_feed_daily_surface() -> None:
    config = PipelineConfig(
        use_spread=False,
        swap_model_enabled=False,
    )
    backtester = Backtester(config)
    index = pd.date_range("2024-01-01T00:00:00Z", periods=4, freq="h")
    features = pd.DataFrame(
        {
            "Open": [100.0, 100.0, 100.0, 100.0],
            "High": [100.0, 100.2, 100.0, 100.0],
            "Low": [100.0, 99.95, 100.0, 100.0],
            "Close": [100.0, 100.1, 100.0, 100.0],
            "Volume": [100, 100, 100, 100],
            "_BAR_CLOSE_TS": index + pd.Timedelta(hours=1),
        },
        index=index,
    )
    signals = pd.Series([1, 0, 0, 0], index=index)
    result = backtester.run(
        features,
        signals,
        "EURUSD",
        _OneTradeStrategy(),
        spec=InstrumentSpec(
            symbol="EURUSD", pip_position=2, pip_value=10.0, spread_avg=0.0
        ),
        timeframe="H1",
    )

    assert result["position_events"]
    surface = build_flat_daily_r_surface(
        result["position_events"],
        start_date_utc="2024-01-01",
        end_date_utc="2024-01-01",
        risk_unit_contract=RISK_CONTRACT,
    )
    assert surface["origin"] == "backtester_search_time_flat_daily_positions"
    assert sum(surface["daily_net_mtm_r"]) == pytest.approx(
        sum(
            event["pnl_dollars"] / event["risk_amount"]
            for event in result["position_events"]
        )
    )


NAMESPACE = "historical-anchor/run-001/anchor-000"


def test_namespace_prefixes_every_artifact_and_binds_receipt(
    tmp_path: Path,
) -> None:
    context, sink, frozen = _freeze(tmp_path, artifact_namespace=NAMESPACE)
    assert frozen.artifact_namespace == NAMESPACE
    receipt = json.loads(frozen.frozen_receipt_bytes.decode("ascii"))
    assert receipt["artifact_namespace"] == NAMESPACE
    assert all(
        descriptor["relative_path"]
        for descriptor in receipt["artifact_roster"].values()
    )

    produced = _produce(
        sink, frozen, [_attempt_evidence(frozen)], artifact_namespace=NAMESPACE
    )
    assert produced.decision.to_dict()["validated"] is True
    sealed = sink.seal()
    manifest = spool.verify_symbol_receipt(
        context.root,
        sealed,
        expected_generation_id=context.generation_id,
        expected_symbol="EURUSD",
        expected_native_universe_sha256=context.native_universe_sha256,
        expected_config_sha256=context.config_sha256,
    )
    prefixed = f"{NAMESPACE}/winner-search/"
    assert all(
        entry["artifact_id"].startswith(prefixed) for entry in manifest["artifacts"]
    )


def test_second_anchor_namespace_shares_one_sink_without_collision(
    tmp_path: Path,
) -> None:
    context, sink, first = _freeze(
        tmp_path, artifact_namespace="historical-anchor/run-001/anchor-000"
    )
    _, _, second = _freeze(
        tmp_path,
        artifact_namespace="historical-anchor/run-001/anchor-001",
        sink=sink,
        context=context,
    )
    assert first.artifact_namespace != second.artifact_namespace
    roster_first = json.loads(first.frozen_receipt_bytes.decode("ascii"))
    roster_second = json.loads(second.frozen_receipt_bytes.decode("ascii"))
    assert set(roster_first["artifact_roster"]) == set(roster_second["artifact_roster"])


def test_namespace_mismatch_fails_closed(tmp_path: Path) -> None:
    _, sink, frozen = _freeze(tmp_path, artifact_namespace=NAMESPACE)
    with pytest.raises(WinnerRawEvidenceError, match="ARTIFACT_NAMESPACE_MISMATCH"):
        _produce(sink, frozen, [_attempt_evidence(frozen)])
    with pytest.raises(WinnerRawEvidenceError, match="ARTIFACT_NAMESPACE_MISMATCH"):
        _produce(
            sink,
            frozen,
            [_attempt_evidence(frozen)],
            artifact_namespace="historical-anchor/run-001/anchor-999",
        )


def test_invalid_namespace_rejected(tmp_path: Path) -> None:
    with pytest.raises(WinnerRawEvidenceError, match="ARTIFACT_NAMESPACE_INVALID"):
        _freeze(tmp_path, artifact_namespace="bad//segment")
    with pytest.raises(WinnerRawEvidenceError, match="ARTIFACT_NAMESPACE_INVALID"):
        _freeze(tmp_path, artifact_namespace="/leading-slash")


def _freeze_single(tmp_path, artifact):
    context, sink = _context(tmp_path)
    first_plan = _fold_plan()[0]
    seed_result = _raw_fold(attempt_ordinal=0, fold_ordinal=0, planned=first_plan)[
        "lanes"
    ]["optimizer_train_base"]
    seed_result["numerical_execution_cost_attestation"] = _genuine_partition_cost(
        label="single-source-seed-precommit",
        bars=19,
        stress=False,
    )
    return freeze_winner_search_plan_v3(
        sink,
        symbol="EURUSD",
        timeframe="H1",
        regime="TREND",
        precommitted_at_utc="2020-01-01T00:00:00Z",
        attempts_frozen_at_utc="2020-01-02T00:00:00Z",
        fold_plan=_fold_plan(),
        attempt_artifacts=[artifact],
        precommitted_evidence_roles=["EVALUATION_REQUIRED"],
        horizon_execution_contracts=[
            {
                "lookback_kind": "NATIVE_OBSERVATIONS",
                "lookback_value": 5_000,
                "source_bar_count": 5_000,
            }
        ],
        source_contract_seed=build_trade_source_contract_from_backtester_result(
            seed_result
        ),
        base_cost_contract=BASE_COST_POLICY,
        stress_cost_contract=STRESS_COST_POLICY,
        risk_unit_contract=RISK_CONTRACT,
        cost_stress_adversity_plan=_cost_stress_adversity_plan(
            attempts=1,
            roles=["EVALUATION_REQUIRED"],
        ),
    )


def _forged(mutate):
    artifact = _attempt_artifact(0)
    mutate(artifact)
    return artifact


@pytest.mark.parametrize(
    "mutate",
    [
        # caller-chosen candidate identity
        lambda a: a["winner_process"].__setitem__("process_candidate_id", "0" * 40),
        # forged executable code pin
        lambda a: a["winner_process"].__setitem__("strategy_code_sha256", "b" * 64),
        # invented strategy class on a real strategy id
        lambda a: a["winner_process"].__setitem__("strategy_class", "InventedStrategy"),
        # invented strategy id (no executable process exists)
        lambda a: [
            body.__setitem__("strategy_id", "strategy-0")
            for body in (
                a["winner_process"],
                a["trial_definition"],
                a["refit_recipe"],
            )
        ],
        # non-grid fixed default mutated (never searchable)
        lambda a: a["trial_definition"]["parameters"].__setitem__(
            "structural_sl_lookback", 27
        ),
        # forged entry-rule identity
        lambda a: a["winner_process"].__setitem__(
            "entry_rule_identity_sha256", "c" * 64
        ),
        # minimal (keyset-stripped) process body
        lambda a: a.__setitem__(
            "winner_process",
            {
                name: a["winner_process"][name]
                for name in (
                    "schema_version",
                    "process_candidate_id",
                    "strategy_id",
                    "strategy_family",
                )
            },
        ),
    ],
)
def test_forged_attempt_artifacts_are_rejected(tmp_path: Path, mutate) -> None:
    with pytest.raises(WinnerRawEvidenceError, match="ARTIFACT_UNVERIFIABLE"):
        _freeze_single(tmp_path, _forged(mutate))


def test_tuple_and_list_grid_strategy_freezes(tmp_path: Path) -> None:
    frozen = _freeze_single(
        tmp_path, _attempt_artifact(0, strategy_id="RegimeTransitionBreakoutStrategy")
    )
    attempt = frozen.attempt_ledger["attempts"][0]
    assert attempt["strategy_id"] == "RegimeTransitionBreakoutStrategy"


def test_frozen_receipt_entry_is_the_genuine_sink_entry(tmp_path: Path) -> None:
    context, sink, frozen = _freeze(tmp_path, artifact_namespace=NAMESPACE)
    entry = frozen.frozen_receipt_entry
    expected_id = f"{NAMESPACE}/winner-search/H1/TREND/frozen/receipt"
    assert entry["artifact_id"] == expected_id
    assert entry["artifact_type"] == "winner_frozen_search_plan_receipt"
    assert (
        entry["descriptor"]["sha256"]
        == hashlib.sha256(frozen.frozen_receipt_bytes).hexdigest()
    )
    sealed = sink.seal()
    manifest = spool.verify_symbol_receipt(
        context.root,
        sealed,
        expected_generation_id=context.generation_id,
        expected_symbol="EURUSD",
        expected_native_universe_sha256=context.native_universe_sha256,
        expected_config_sha256=context.config_sha256,
    )
    registered = {item["artifact_id"]: item for item in manifest["artifacts"]}
    assert registered[expected_id] == entry


def test_post_freeze_receipt_reseal_conflicts_with_original_sink(
    tmp_path: Path,
) -> None:
    _, sink, frozen = _freeze(tmp_path)
    receipt = json.loads(frozen.frozen_receipt_bytes.decode("ascii"))
    receipt["schema_version"] = "attacker-resealed-after-freeze"
    forged_receipt_bytes = _canonical(receipt)
    forged_entry = json.loads(frozen.frozen_receipt_entry_bytes.decode("ascii"))
    forged_entry["descriptor"]["sha256"] = hashlib.sha256(
        forged_receipt_bytes
    ).hexdigest()
    forged = replace(
        frozen,
        frozen_receipt_bytes=forged_receipt_bytes,
        frozen_receipt_entry_bytes=_canonical(forged_entry),
    )

    with pytest.raises(spool.AuthoritySpoolError, match="ARTIFACT_ID_CONFLICT"):
        _produce(sink, forged, [_attempt_evidence(forged)])


def test_cost_stress_adversity_plan_is_run_bound_and_reopened(
    tmp_path: Path,
) -> None:
    _, sink, frozen = _freeze(tmp_path)
    plan = frozen.cost_stress_adversity_plan
    plan_sha256 = _sha(plan)
    receipt = json.loads(frozen.frozen_receipt_bytes.decode("ascii"))
    (tmp_path / "alternate").mkdir()
    _, _, alternate = _freeze(
        tmp_path / "alternate",
        no_shock_pairs={(0, 0)},
    )

    assert alternate.precommitment["run_id"] != frozen.precommitment["run_id"]
    assert receipt["cost_stress_adversity_plan_sha256"] == plan_sha256
    assert (
        receipt["artifact_roster"]["cost_stress_adversity_plan"]["sha256"]
        == plan_sha256
    )

    plan["rows"][0]["final_partition_frame_sha256"] = "f" * 64
    plan["rows_sha256"] = _sha(plan["rows"])
    forged = replace(
        frozen,
        cost_stress_adversity_plan_bytes=_canonical(plan),
    )
    with pytest.raises(
        WinnerRawEvidenceError,
        match="FROZEN_PLAN_RECEIPT_MISMATCH",
    ):
        _produce(sink, forged, [_attempt_evidence(frozen)])


def test_namespace_maximum_length_bound(tmp_path: Path) -> None:
    segment = "p" * 128
    at_cap = "/".join([segment, segment, segment, "q" * 125])
    assert len(at_cap) == 512
    _, _, frozen = _freeze(tmp_path, artifact_namespace=at_cap)
    assert frozen.artifact_namespace == at_cap
    over_cap = "/".join([segment, segment, segment, "q" * 126])
    with pytest.raises(WinnerRawEvidenceError, match="ARTIFACT_NAMESPACE_INVALID"):
        _freeze(tmp_path, artifact_namespace=over_cap)


def test_out_of_order_final_events_fail_with_typed_code(tmp_path: Path) -> None:
    _, sink, frozen = _freeze(tmp_path)
    raw = _attempt_evidence(frozen)
    lanes = raw["folds"][0]["lanes"]
    for lane in ("final_lockbox_base", "final_lockbox_stress"):
        events = lanes[lane]["position_events"]
        events[0], events[1] = events[1], events[0]
    with pytest.raises(
        WinnerRawEvidenceIncomplete, match="ATTEMPT_TECHNICAL_EVIDENCE_FAILURE"
    ):
        _produce(sink, frozen, [raw])


def test_registry_commitments_derive_from_frozen_bytes(tmp_path: Path) -> None:
    _, _, frozen = _freeze(tmp_path)
    commitments = frozen.registry_commitments()
    precommitment = frozen.precommitment
    assert commitments["run_id"] == precommitment["run_id"]
    assert (
        commitments["process_registry_sha256"]
        == precommitment["process_registry_sha256"]
    )
    assert (
        commitments["causal_fold_plan_sha256"]
        == precommitment["causal_fold_plan_sha256"]
    )
    assert commitments["precommitment_sha256"] == _sha(precommitment)
    receipt = json.loads(frozen.frozen_receipt_bytes.decode("ascii"))
    for name in (
        "process_registry_sha256",
        "horizon_registry_sha256",
        "trial_registry_sha256",
    ):
        assert receipt[name] == commitments[name]
