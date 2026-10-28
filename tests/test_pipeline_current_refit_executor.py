from __future__ import annotations

import hashlib
import json
from datetime import datetime, timedelta
from pathlib import Path

from pm_current_refit_execution import (
    CURRENT_REGIME_PARAMS_BUNDLE_VERSION,
    build_raw_artifact_descriptor,
    build_raw_current_refit_manifest,
    validate_raw_current_refit_manifest,
    verify_raw_current_refit_execution,
)
from pm_book_multiplicity import (
    BOOK_MULTIPLICITY_CONTRACT_VERSION,
    PINNED_SYMBOL_ORDER,
    TIMEFRAME_ORDER,
    compute_cell_id,
)
from pm_model_lifecycle import (
    CURRENT_REFIT_EXECUTION_RECEIPT_TYPE,
    canonical_receipt_sha256,
    compute_current_refit_cell_id,
    open_typed_receipt,
)
from pm_pipeline import (
    CURRENT_PROCESS_BOOK_ARTIFACT_VERSION,
    CURRENT_REFIT_BAR_FILE_VERSION,
    CURRENT_REFIT_BOOK_ENTRY_VERSION,
    CURRENT_REFIT_CELL_TRACE_VERSION,
    CURRENT_REFIT_EXECUTION_TRACE_VERSION,
    CURRENT_REFIT_HORIZON_BUNDLE_VERSION,
    CURRENT_REFIT_HORIZON_VERSION,
    CURRENT_REFIT_OUTPUT_BOOK_VERSION,
    CURRENT_REFIT_PARAMETER_VERSION,
    CURRENT_REFIT_PROCESS_VERSION,
    CURRENT_REFIT_RECIPE_VERSION,
    CURRENT_REFIT_SOURCE_BUNDLE_VERSION,
    CURRENT_REFIT_SOURCE_SNAPSHOT_VERSION,
    CURRENT_REFIT_SPECIALIST_VERSION,
    CURRENT_REFIT_TRIAL_VERSION,
    _CURRENT_REFIT_BAR_COLUMNS,
    _current_refit_callable_sha256,
    _current_refit_core_code_sha256,
    _current_refit_feature_contract,
    _current_refit_process_candidate_id,
    _current_refit_sha256,
    _current_refit_strategy_code_sha256,
    replay_current_refit_execution_from_raw_manifest,
)
from pm_strategies import StrategyRegistry
from tests.test_model_lifecycle import (
    _digest,
    _process_refit_receipts,
    _regime_params_bundle,
)


def _canonical(value: object) -> bytes:
    return json.dumps(
        value,
        allow_nan=False,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


def _production_fixture(
    tmp_path: Path,
    *,
    cutoff: str = "2026-07-13T00:00:00+00:00",
    historical_process_decision: dict | None = None,
    book_multiplicity_decision: dict | None = None,
    book_entries_override: dict[str, dict] | None = None,
    regime_bundle_override: dict | None = None,
    process_candidate_id_override: str | None = None,
    parameter_overrides: dict | None = None,
    return_details: bool = False,
):
    times = {
        "M5": (
            "2024-01-01T00:00:00+00:00",
            "2026-07-12T23:55:00+00:00",
            timedelta(minutes=5),
        ),
        "D1": (
            "2010-01-01T00:00:00+00:00",
            "2026-07-12T00:00:00+00:00",
            timedelta(days=1),
        ),
    }
    descriptors = []

    def add_bytes(relative: str, content: bytes, semantic: str, version: str):
        path = tmp_path / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(content)
        descriptor = build_raw_artifact_descriptor(
            relative_path=relative,
            size_bytes=len(content),
            content_sha256=hashlib.sha256(content).hexdigest(),
            semantic_sha256=semantic,
            format_version=version,
        )
        descriptors.append(descriptor)
        return descriptor

    def add_json(relative: str, value: dict, version: str):
        return add_bytes(relative, _canonical(value), _current_refit_sha256(value), version)

    regime_bundle = regime_bundle_override or _regime_params_bundle(cutoff)
    regime_by_tf = {
        item["payload"]["timeframe"]: item for item in regime_bundle["regime_fits"]
    }
    strategy = StrategyRegistry.get("EMACrossoverStrategy")
    parameters = strategy.get_params()
    parameters.update(parameter_overrides or {})
    materials = {}
    cell_descriptors = {}

    for timeframe, (fit_start, latest, step) in times.items():
        latest_dt = datetime.fromisoformat(latest)
        stamps = [fit_start] + [
            (latest_dt - step * offset).isoformat() for offset in range(58, -1, -1)
        ]
        rows = [",".join(_CURRENT_REFIT_BAR_COLUMNS)]
        rows.extend(
            f"{stamp},1.0000,1.0010,0.9990,1.0005,100" for stamp in stamps
        )
        bar_content = ("\n".join(rows) + "\n").encode("utf-8")
        bar_digest = hashlib.sha256(bar_content).hexdigest()
        bar_descriptor = add_bytes(
            f"bars/{timeframe}.csv",
            bar_content,
            bar_digest,
            CURRENT_REFIT_BAR_FILE_VERSION,
        )
        fit = regime_by_tf[timeframe]
        record = fit["payload"]["regime_detector_record"]
        source = {
            "schema_version": CURRENT_REFIT_SOURCE_SNAPSHOT_VERSION,
            "symbol": "EURUSD",
            "timeframe": timeframe,
            "fit_start_utc": fit_start,
            "fit_end_utc": cutoff,
            "latest_closed_bar_utc": latest,
            "first_bar_utc": fit_start,
            "last_bar_utc": latest,
            "bar_count": len(stamps),
            "bars_artifact_id": bar_descriptor["artifact_id"],
            "bars_content_sha256": bar_digest,
            "activation_id": record["activation_id"],
            "activation_manifest_sha256": record["activation_manifest_sha256"],
            "native_source_sha256": record["native_source_sha256"],
            "source_contract_sha256": record["source_contract_sha256"],
            "continuity_receipt_sha256": _digest(f"continuity-{timeframe}"),
            "columns": list(_CURRENT_REFIT_BAR_COLUMNS),
            "timestamp_encoding": "UTC_ISO8601_PLUS00",
        }
        source_descriptor = add_json(
            f"inputs/{timeframe}-source.json",
            source,
            CURRENT_REFIT_SOURCE_SNAPSHOT_VERSION,
        )
        horizon = {
            "schema_version": CURRENT_REFIT_HORIZON_VERSION,
            "symbol": "EURUSD",
            "timeframe": timeframe,
            "lookback_kind": "NATIVE_OBSERVATIONS",
            "lookback_value": len(stamps),
            "source_bar_count": len(stamps),
            "fit_start_utc": fit_start,
            "fit_end_utc": cutoff,
            "latest_closed_bar_utc": latest,
            "source_snapshot_sha256": _current_refit_sha256(source),
        }
        horizon_sha = _current_refit_sha256(horizon)
        horizon_descriptor = add_json(
            f"inputs/{timeframe}-horizon.json", horizon, CURRENT_REFIT_HORIZON_VERSION
        )
        process = {
            "schema_version": CURRENT_REFIT_PROCESS_VERSION,
            "process_candidate_id": (
                process_candidate_id_override
                or _current_refit_process_candidate_id(
                    symbol="EURUSD",
                    timeframe=timeframe,
                    regime="TREND",
                    strategy_id=strategy.name,
                    parameters=parameters,
                    horizon_process_sha256=horizon_sha,
                )
            ),
            "strategy_id": strategy.name,
            "strategy_family": strategy.category.value,
            "strategy_module": "pm_strategies",
            "strategy_class": type(strategy).__name__,
            "strategy_code_sha256": _current_refit_strategy_code_sha256(strategy.name),
            "entry_rule_identity_sha256": _current_refit_callable_sha256(
                type(strategy).generate_signals
            ),
            "exit_rule_identity_sha256": _current_refit_callable_sha256(
                type(strategy).calculate_stops
            ),
            "regime_conditioning": "EXACT_SYMBOL_TIMEFRAME_REGIME_CELL",
        }
        process_sha = _current_refit_sha256(process)
        process_descriptor = add_json(
            f"inputs/{timeframe}-process.json", process, CURRENT_REFIT_PROCESS_VERSION
        )
        trial = {
            "schema_version": CURRENT_REFIT_TRIAL_VERSION,
            "strategy_id": strategy.name,
            "strategy_family": strategy.category.value,
            "process_sha256": process_sha,
            "horizon_process_sha256": horizon_sha,
            "feature_contract_sha256": _current_refit_feature_contract(strategy),
            "indicator_contract_sha256": _current_refit_core_code_sha256(),
            "parameter_count": len(parameters),
            "parameters": parameters,
        }
        trial_sha = _current_refit_sha256(trial)
        trial_descriptor = add_json(
            f"inputs/{timeframe}-trial.json", trial, CURRENT_REFIT_TRIAL_VERSION
        )
        recipe = {
            "schema_version": CURRENT_REFIT_RECIPE_VERSION,
            "strategy_id": strategy.name,
            "winner_process_sha256": process_sha,
            "winner_horizon_process_sha256": horizon_sha,
            "winner_trial_definition_sha256": trial_sha,
            "refit_mode": "STATELESS_RULE_MATERIALIZATION",
            "parameter_source": "CURRENT_WINNER_TRIAL",
            "source_validation": "FULL_NATIVE_PREFIX_STREAM",
            "fit_window_rule": "HORIZON_NATIVE_PREFIX_THROUGH_CURRENT_CUTOFF",
            "deterministic_fit_procedure_id": "FXPM_STATELESS_RULE_MATERIALIZE_V1",
            "seed": 100 + len(materials),
            "version_pins": {
                "python": __import__("sys").version.split()[0],
                "numpy": __import__("numpy").__version__,
                "pandas": __import__("pandas").__version__,
                "pm_core_sha256": _current_refit_core_code_sha256(),
                "pm_strategies_sha256": _current_refit_strategy_code_sha256(
                    strategy.name
                ),
            },
            "data_requirements": {
                "columns": list(_CURRENT_REFIT_BAR_COLUMNS),
                "minimum_bars": int(strategy.MIN_BARS),
                "warmup_bars": int(strategy.MIN_BARS),
            },
            "attempts": 1,
            "post_selection_search": False,
            "output_schema_version": CURRENT_REFIT_SPECIALIST_VERSION,
        }
        recipe_sha = _current_refit_sha256(recipe)
        recipe_descriptor = add_json(
            f"inputs/{timeframe}-recipe.json", recipe, CURRENT_REFIT_RECIPE_VERSION
        )
        detector = {
            "regime_fit_receipt_sha256": fit["receipt_sha256"],
            "regime_detector_record_sha256": record["record_sha256"],
            "regime_detector_parameters_sha256": record["parameters_sha256"],
            "regime_detector_code_sha256": record["detector_code_sha256"],
        }
        book_entry = {
            "ordinal": (
                PINNED_SYMBOL_ORDER.index("EURUSD") * len(TIMEFRAME_ORDER) * 4
                + TIMEFRAME_ORDER.index(timeframe) * 4
            ),
            "cell_id": compute_cell_id("EURUSD", timeframe, "TREND"),
            "symbol": "EURUSD",
            "timeframe": timeframe,
            "regime": "TREND",
            "source_group_id": fit["payload"]["source_group_id"],
            "source_group_decision_sha256": fit["payload"][
                "source_group_decision_sha256"
            ],
            "regime_receipt_sha256": fit["receipt_sha256"],
            "regime_detector_record_sha256": record["record_sha256"],
            "train_regime_support_receipt_sha256": fit["payload"][
                "train_regime_support"
            ]["receipt_sha256"],
            "historical_state": "WINNER",
            "state_reason": "PROCESS_WINNER_PENDING_BOOK_MULTIPLICITY",
            "historical_outcome_receipt_sha256": _digest(
                f"winner-decision-{timeframe}"
            ),
            "winner_request_sha256": _digest(f"winner-request-{timeframe}"),
            "winner_attempt_ledger_payload_sha256": _digest(
                f"winner-attempts-{timeframe}"
            ),
            "winner_decision_sha256": _digest(f"winner-decision-{timeframe}"),
            "winner_evidence_ledger_payload_sha256": _digest(
                f"winner-evidence-{timeframe}"
            ),
            "local_process_candidate_id": process["process_candidate_id"],
            "local_process_sha256": process_sha,
            "validated_cost_ladder_tolerance": {
                "level_id": "UNIFORM_ALL_FILLS_SINGLE_FILL_UPLIFT_P75",
                "single_fill_uplift_percentile": 75,
                "uniform_entry_half_spread_multiplier_float64_hex": (
                    "0x1.a000000000000p+2"
                ),
            },
            "process_state_payload": None,
            "process_state_receipt_sha256": None,
            "within_cell_attempted_trials_exact": 10,
            "effective_trials": 10.0,
            "marginal_p_value": 0.01,
            "adjusted_q_value": 0.02,
            "by_qualified": True,
            "historical_local_candidate_pending_causal": True,
            "historical_book_entry_sha256": "0" * 64,
        }
        book_identity = {
            name: value
            for name, value in book_entry.items()
            if name
            not in {
                "adjusted_q_value",
                "by_qualified",
                "historical_local_candidate_pending_causal",
                "historical_book_entry_sha256",
            }
        }
        book_entry["historical_book_entry_sha256"] = _current_refit_sha256(
            {
                "book_contract_version": BOOK_MULTIPLICITY_CONTRACT_VERSION,
                **book_identity,
            }
        )
        if book_entries_override is not None:
            book_entry = dict(book_entries_override[timeframe])
        book_descriptor = add_bytes(
            f"inputs/{timeframe}-book-entry.json",
            _canonical(book_entry),
            book_entry["historical_book_entry_sha256"],
            CURRENT_REFIT_BOOK_ENTRY_VERSION,
        )
        materials[timeframe] = {
            "source": source,
            "source_sha": _current_refit_sha256(source),
            "process": process,
            "process_sha": process_sha,
            "horizon_sha": horizon_sha,
            "trial": trial,
            "trial_sha": trial_sha,
            "recipe_sha": recipe_sha,
            "book_entry": book_entry,
            "book_entry_sha": book_entry["historical_book_entry_sha256"],
            "detector": detector,
            "parameters": parameters,
        }
        cell_descriptors[timeframe] = {
            "source_snapshot": source_descriptor,
            "winner_process": process_descriptor,
            "horizon_process": horizon_descriptor,
            "trial_definition": trial_descriptor,
            "refit_recipe": recipe_descriptor,
            "winner_book_entry": book_descriptor,
        }

    source_bundle = {
        "schema_version": CURRENT_REFIT_SOURCE_BUNDLE_VERSION,
        "current_cutoff_utc": cutoff,
        "entries": [
            {
                "symbol": "EURUSD",
                "timeframe": timeframe,
                "source_snapshot_sha256": materials[timeframe]["source_sha"],
            }
            for timeframe in ("M5", "D1")
        ],
    }
    horizon_bundle = {
        "schema_version": CURRENT_REFIT_HORIZON_BUNDLE_VERSION,
        "current_cutoff_utc": cutoff,
        "entries": [
            {
                "symbol": "EURUSD",
                "timeframe": timeframe,
                "regime": "TREND",
                "horizon_process_sha256": materials[timeframe]["horizon_sha"],
            }
            for timeframe in ("M5", "D1")
        ],
    }
    source_bundle_sha = _current_refit_sha256(source_bundle)
    horizon_bundle_sha = _current_refit_sha256(horizon_bundle)
    regime_bundle_sha = canonical_receipt_sha256(regime_bundle)

    def roster_change(roster):
        for winner in roster:
            material = materials[winner["timeframe"]]
            winner.update(
                {
                    "strategy_id": strategy.name,
                    "winner_process_candidate_id": material["process"][
                        "process_candidate_id"
                    ],
                    "winner_process_sha256": material["process_sha"],
                    "winner_horizon_process_sha256": material["horizon_sha"],
                    "winner_trial_definition_sha256": material["trial_sha"],
                    "winner_refit_recipe_sha256": material["recipe_sha"],
                    "winner_decision_sha256": material["book_entry"][
                        "winner_decision_sha256"
                    ],
                    "winner_book_entry_sha256": material["book_entry_sha"],
                    **material["detector"],
                }
            )

    def book_change(book):
        book["current_cutoff_utc"] = cutoff
        book["source_snapshot_bundle_sha256"] = source_bundle_sha
        book["regime_params_sha256"] = regime_bundle_sha
        book["horizon_decisions_sha256"] = horizon_bundle_sha
        if book_multiplicity_decision is not None:
            book["book_multiplicity_receipt_sha256"] = (
                book_multiplicity_decision["receipt_sha256"]
            )
            book["full_cell_outcome_roster_sha256"] = (
                book_multiplicity_decision["book_generation_sha256"]
            )
        if historical_process_decision is not None:
            book["started_at_utc"] = "2026-07-13T10:23:00+00:00"
            book["completed_at_utc"] = "2026-07-13T12:00:00+00:00"

    def process_change(process):
        if historical_process_decision is not None:
            process.clear()
            process.update(historical_process_decision)

    def cell_change(cells):
        for cell in cells:
            cell["fit_end_utc"] = cutoff
            material = materials[cell["timeframe"]]
            parameter_payload = {
                "schema_version": CURRENT_REFIT_PARAMETER_VERSION,
                "strategy_id": strategy.name,
                "parameters": material["parameters"],
            }
            parameter_sha = _current_refit_sha256(parameter_payload)
            output_payload = {
                "schema_version": CURRENT_REFIT_SPECIALIST_VERSION,
                "symbol": cell["symbol"],
                "timeframe": cell["timeframe"],
                "regime": cell["regime"],
                "strategy_id": strategy.name,
                "parameters": material["parameters"],
                "parameter_sha256": parameter_sha,
                "source_snapshot_sha256": material["source_sha"],
                "winner_process_sha256": material["process_sha"],
                "winner_horizon_process_sha256": material["horizon_sha"],
                "winner_trial_definition_sha256": material["trial_sha"],
                "winner_refit_recipe_sha256": material["recipe_sha"],
                "winner_book_entry_sha256": material["book_entry_sha"],
                **material["detector"],
                "fit_start_utc": cell["fit_start_utc"],
                "fit_end_utc": cell["fit_end_utc"],
                "latest_closed_bar_utc": cell["latest_closed_bar_utc"],
            }
            output_sha = _current_refit_sha256(output_payload)
            cell.update(
                {
                    "strategy_id": strategy.name,
                    "winner_process_candidate_id": material["process"][
                        "process_candidate_id"
                    ],
                    "winner_process_sha256": material["process_sha"],
                    "winner_horizon_process_sha256": material["horizon_sha"],
                    "winner_trial_definition_sha256": material["trial_sha"],
                    "winner_refit_recipe_sha256": material["recipe_sha"],
                    "winner_decision_sha256": material["book_entry"][
                        "winner_decision_sha256"
                    ],
                    "winner_book_entry_sha256": material["book_entry_sha"],
                    "source_snapshot_sha256": material["source_sha"],
                    "parameter_sha256": parameter_sha,
                    "output_artifact_sha256": output_sha,
                    **material["detector"],
                }
            )
            identity = {
                name: cell[name]
                for name in (
                    "historical_process_decision_sha256",
                    "accepted_book_generation_receipt_sha256",
                    "symbol",
                    "timeframe",
                    "regime",
                    "strategy_id",
                    "winner_process_candidate_id",
                    "winner_process_sha256",
                    "winner_horizon_process_sha256",
                    "winner_trial_definition_sha256",
                    "winner_refit_recipe_sha256",
                    "winner_decision_sha256",
                    "winner_book_entry_sha256",
                    "regime_fit_receipt_sha256",
                    "regime_detector_record_sha256",
                    "regime_detector_parameters_sha256",
                    "regime_detector_code_sha256",
                    "fit_start_utc",
                    "fit_end_utc",
                    "latest_closed_bar_utc",
                    "source_snapshot_sha256",
                    "parameter_sha256",
                    "output_artifact_sha256",
                )
            }
            cell_id = compute_current_refit_cell_id(identity)
            trace_payload = {
                "schema_version": CURRENT_REFIT_CELL_TRACE_VERSION,
                "current_refit_cell_id": cell_id,
                "input_content_sha256": {
                    role: cell_descriptors[cell["timeframe"]][role]["content_sha256"]
                    for role in sorted(cell_descriptors[cell["timeframe"]])
                },
                "parameter_sha256": parameter_sha,
                "output_artifact_sha256": output_sha,
                "execution_mode": "STATELESS_RULE_MATERIALIZATION",
            }
            cell["current_refit_cell_id"] = cell_id
            cell["execution_trace_sha256"] = _current_refit_sha256(trace_payload)

    def execution_change(execution):
        execution["current_cutoff_utc"] = cutoff
        cells = execution["cell_executions"]
        book_receipt_sha = cells[0]["accepted_book_generation_receipt_sha256"]
        execution["cell_execution_bundle_sha256"] = canonical_receipt_sha256(
            {"cell_executions": cells}
        )
        execution["output_book_sha256"] = _current_refit_sha256(
            {
                "schema_version": CURRENT_REFIT_OUTPUT_BOOK_VERSION,
                "current_cutoff_utc": cutoff,
                "accepted_book_generation_receipt_sha256": book_receipt_sha,
                "specialists": [
                    {
                        "current_refit_cell_id": cell["current_refit_cell_id"],
                        "parameter_sha256": cell["parameter_sha256"],
                        "output_artifact_sha256": cell["output_artifact_sha256"],
                    }
                    for cell in cells
                ],
            }
        )
        execution["execution_trace_sha256"] = _current_refit_sha256(
            {
                "schema_version": CURRENT_REFIT_EXECUTION_TRACE_VERSION,
                "current_cutoff_utc": cutoff,
                "accepted_book_generation_receipt_sha256": book_receipt_sha,
                "source_snapshot_bundle_sha256": source_bundle_sha,
                "regime_params_sha256": regime_bundle_sha,
                "horizon_decisions_sha256": horizon_bundle_sha,
                "cell_execution_traces": [
                    {
                        "current_refit_cell_id": cell["current_refit_cell_id"],
                        "execution_trace_sha256": cell["execution_trace_sha256"],
                    }
                    for cell in cells
                ],
            }
        )
        if historical_process_decision is not None:
            execution["started_at_utc"] = "2026-07-13T12:02:00+00:00"
            execution["completed_at_utc"] = "2026-07-13T13:00:00+00:00"

    def authorization_change(authorization):
        authorization["current_cutoff_utc"] = cutoff
        if historical_process_decision is not None:
            authorization["issued_at_utc"] = "2026-07-13T12:01:00+00:00"

    _process, book, receipt = _process_refit_receipts(
        process_change=process_change,
        roster_change=roster_change,
        book_change=book_change,
        authorization_change=authorization_change,
        cell_change=cell_change,
        execution_change=execution_change,
    )
    payload, _ = open_typed_receipt(
        receipt, expected_receipt_type=CURRENT_REFIT_EXECUTION_RECEIPT_TYPE
    )
    cells = payload["execution"]["cell_executions"]

    accepted_book_descriptor = add_bytes(
        "bundles/accepted-book.json",
        _canonical(book),
        canonical_receipt_sha256(book),
        CURRENT_PROCESS_BOOK_ARTIFACT_VERSION,
    )
    source_bundle_descriptor = add_json(
        "bundles/sources.json", source_bundle, CURRENT_REFIT_SOURCE_BUNDLE_VERSION
    )
    regime_bundle_descriptor = add_bytes(
        "bundles/regimes.json",
        _canonical(regime_bundle),
        regime_bundle_sha,
        CURRENT_REGIME_PARAMS_BUNDLE_VERSION,
    )
    horizon_bundle_descriptor = add_json(
        "bundles/horizons.json", horizon_bundle, CURRENT_REFIT_HORIZON_BUNDLE_VERSION
    )
    bundle_ids = {
        "accepted_book_generation": accepted_book_descriptor["artifact_id"],
        "source_snapshot_bundle": source_bundle_descriptor["artifact_id"],
        "regime_params_bundle": regime_bundle_descriptor["artifact_id"],
        "horizon_decisions_bundle": horizon_bundle_descriptor["artifact_id"],
    }
    requests = []
    for cell in cells:
        request = {
            name: cell[name]
            for name in (
                "current_refit_cell_id",
                "symbol",
                "timeframe",
                "regime",
                "strategy_id",
                "fit_start_utc",
                "fit_end_utc",
                "latest_closed_bar_utc",
                "source_snapshot_sha256",
                "winner_process_sha256",
                "winner_horizon_process_sha256",
                "winner_trial_definition_sha256",
                "winner_refit_recipe_sha256",
                "winner_book_entry_sha256",
                "regime_fit_receipt_sha256",
                "regime_detector_record_sha256",
                "regime_detector_parameters_sha256",
                "regime_detector_code_sha256",
            )
        }
        request["artifact_ids"] = {
            role: descriptor["artifact_id"]
            for role, descriptor in cell_descriptors[cell["timeframe"]].items()
        }
        requests.append(request)
    manifest = build_raw_current_refit_manifest(
        current_refit_receipt=receipt,
        bundle_artifact_ids=bundle_ids,
        artifact_descriptors=descriptors,
        cell_requests=requests,
    )
    if return_details:
        return receipt, manifest, {
            "accepted_book_receipt": book,
            "book_entries": {
                timeframe: material["book_entry"]
                for timeframe, material in materials.items()
            },
            "regime_bundle": regime_bundle,
            "bar_digests": {
                timeframe: material["source"]["bars_content_sha256"]
                for timeframe, material in materials.items()
            },
        }
    return receipt, manifest


def test_raw_current_refit_executor_rederives_complete_book(tmp_path: Path) -> None:
    receipt, manifest, details = _production_fixture(tmp_path, return_details=True)
    assert all(
        entry["validated_cost_ladder_tolerance"]
        == {
            "level_id": "UNIFORM_ALL_FILLS_SINGLE_FILL_UPLIFT_P75",
            "single_fill_uplift_percentile": 75,
            "uniform_entry_half_spread_multiplier_float64_hex": (
                "0x1.a000000000000p+2"
            ),
        }
        for entry in details["book_entries"].values()
    )
    plan, errors = validate_raw_current_refit_manifest(
        receipt, manifest, artifact_root=tmp_path
    )
    assert errors == []
    assert plan is not None
    replay_current_refit_execution_from_raw_manifest(
        plan, artifact_root=tmp_path.resolve()
    )

    result = verify_raw_current_refit_execution(
        receipt, manifest, artifact_root=tmp_path
    )

    assert result["verified"] is True, result["errors"]
    assert result["errors"] == []
    assert result["recomputed_cell_count"] == 2


def test_raw_executor_rejects_self_consistent_caller_chosen_candidate_id(
    tmp_path: Path,
) -> None:
    receipt, manifest = _production_fixture(
        tmp_path, process_candidate_id_override="attacker-chosen-candidate"
    )
    plan, errors = validate_raw_current_refit_manifest(
        receipt, manifest, artifact_root=tmp_path
    )
    assert errors == []
    assert plan is not None

    result = verify_raw_current_refit_execution(
        receipt, manifest, artifact_root=tmp_path
    )
    assert result["verified"] is False
    assert "CURRENT_REFIT_RAW_REPLAY_FAILED:ValueError" in result["errors"]


def test_raw_executor_rejects_non_grid_fixed_default_mutation(tmp_path: Path) -> None:
    receipt, manifest = _production_fixture(
        tmp_path, parameter_overrides={"structural_sl_lookback": 27}
    )
    plan, errors = validate_raw_current_refit_manifest(
        receipt, manifest, artifact_root=tmp_path
    )
    assert errors == []
    assert plan is not None

    result = verify_raw_current_refit_execution(
        receipt, manifest, artifact_root=tmp_path
    )
    assert result["verified"] is False
    assert "CURRENT_REFIT_RAW_REPLAY_FAILED:ValueError" in result["errors"]


def test_declared_semantics_cannot_launder_changed_executable_trial(
    tmp_path: Path,
) -> None:
    receipt, manifest = _production_fixture(tmp_path)
    request = manifest["cell_requests"][0]

    def replace(role: str, value: dict) -> str:
        old_id = request["artifact_ids"][role]
        index = next(
            i
            for i, descriptor in enumerate(manifest["artifact_descriptors"])
            if descriptor["artifact_id"] == old_id
        )
        old = manifest["artifact_descriptors"][index]
        content = _canonical(value)
        (tmp_path / old["relative_path"]).write_bytes(content)
        replacement = build_raw_artifact_descriptor(
            relative_path=old["relative_path"],
            size_bytes=len(content),
            content_sha256=hashlib.sha256(content).hexdigest(),
            semantic_sha256=old["semantic_sha256"],
            format_version=old["format_version"],
        )
        manifest["artifact_descriptors"][index] = replacement
        request["artifact_ids"][role] = replacement["artifact_id"]
        return _current_refit_sha256(value)

    def load(role: str) -> dict:
        artifact_id = request["artifact_ids"][role]
        descriptor = next(
            item
            for item in manifest["artifact_descriptors"]
            if item["artifact_id"] == artifact_id
        )
        return json.loads((tmp_path / descriptor["relative_path"]).read_text())

    trial = load("trial_definition")
    trial["parameters"]["fast_period"] = 8
    changed_trial_sha = replace("trial_definition", trial)
    recipe = load("refit_recipe")
    recipe["winner_trial_definition_sha256"] = changed_trial_sha
    replace("refit_recipe", recipe)
    body = dict(manifest)
    body.pop("manifest_sha256")
    manifest["manifest_sha256"] = canonical_receipt_sha256(body)

    plan, errors = validate_raw_current_refit_manifest(
        receipt, manifest, artifact_root=tmp_path
    )
    assert errors == []
    assert plan is not None
    result = verify_raw_current_refit_execution(
        receipt, manifest, artifact_root=tmp_path
    )
    assert result["verified"] is False
    assert "CURRENT_REFIT_RAW_REPLAY_FAILED:ValueError" in result["errors"]
