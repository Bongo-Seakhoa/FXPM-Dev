import copy
import inspect

from pm_core import PipelineConfig
from pm_model_lifecycle import (
    ACCEPTED_BOOK_GENERATION_RECEIPT_VERSION,
    CURRENT_PROCESS_BOOK_RECEIPT_TYPE,
    CURRENT_REFIT_RECEIPT_VERSION,
    CURRENT_REFIT_EXECUTION_RECEIPT_TYPE,
    EDGE_CHALLENGER_ROLE,
    LOCKBOX_CHAMPION_ROLE,
    PROCESS_VALIDATED_CURRENT_REFIT_ROLE,
    TYPED_RECEIPT_VERSION,
    build_process_validated_current_refit_lineage,
    build_typed_receipt,
    build_empty_edge_challenger_book,
    build_lockbox_champion_lineage,
    canonical_receipt_sha256,
    compute_current_refit_cell_id,
    compute_current_winner_roster_sha256,
    open_typed_receipt,
    temporal_role_block_reason,
)
from pm_book_multiplicity import (
    REGIME_ORDER,
    create_historical_source_group_decision,
    create_regime_detector_record,
    create_regime_fit_decision,
)
from pm_current_refit_execution import build_current_regime_params_bundle
from unittest.mock import Mock, patch

import pandas as pd

from pm_pipeline import (
    OptimizationPipeline,
    RegimeConfig,
    SymbolConfig,
    classify_promotion_status,
)


def _window_meta():
    return {
        "as_of_utc": "2026-01-01T00:00:00Z",
        "windows": {
            "tuning_validation": {
                "start_ts": "2022-12-25T00:00:00Z",
                "end_ts": "2023-12-25T00:00:00Z",
            },
            "final_lockbox": {
                "start_ts": "2024-01-01T00:00:00Z",
                "end_ts": "2026-01-01T00:00:00Z",
            },
        },
    }


def _profitable_config(lineage):
    return RegimeConfig(
        strategy_name="DemoStrategy",
        parameters={"period": 20},
        quality_score=0.8,
        val_metrics={
            "profit_factor": 1.4,
            "total_return_pct": 15.0,
            "max_drawdown_pct": 5.0,
        },
        validation_evidence={"temporal_lineage": lineage},
    )


def test_champion_lineage_exposes_selection_cutoff_and_latest_evidence_age():
    lineage = build_lockbox_champion_lineage(_window_meta())

    assert lineage["contract_status"] == "PASS"
    assert lineage["book_role"] == LOCKBOX_CHAMPION_ROLE
    assert lineage["parameter_selection_cutoff_utc"].startswith("2023-12-25")
    assert lineage["performance_evidenced_through_utc"].startswith("2026-01-01")
    assert lineage["parameter_selection_age_days_at_asof"] > 700
    assert lineage["evidence_lag_days_at_asof"] == 0.0
    assert temporal_role_block_reason({"temporal_lineage": lineage}) is None


def test_champion_lineage_fails_if_selection_touches_lockbox():
    meta = _window_meta()
    meta["windows"]["tuning_validation"]["end_ts"] = "2024-02-01T00:00:00Z"

    lineage = build_lockbox_champion_lineage(meta)

    assert lineage["contract_status"] == "FAIL"
    assert not lineage["funded_role_eligible"]
    assert "overlaps final lockbox" in " ".join(lineage["contract_errors"])


def test_edge_challenger_is_separate_paper_only_book_and_never_live_valid():
    book = build_empty_edge_challenger_book(_window_meta())
    challenger_lineage = {
        "schema_version": book["schema_version"],
        "book_role": EDGE_CHALLENGER_ROLE,
        "contract_status": "PASS",
        "parameters_frozen_during_final_lockbox": False,
    }
    challenger = _profitable_config(challenger_lineage)

    status, reasons = classify_promotion_status(challenger, PipelineConfig())

    assert book["paper_only"]
    assert book["regime_configs"] == {}
    assert status == "PAPER_ONLY"
    assert "future non-overlapping forward evidence" in reasons[0]
    assert not challenger.is_valid_for_live()


def test_symbol_config_roundtrip_preserves_separate_challenger_book():
    lineage = build_lockbox_champion_lineage(_window_meta())
    champion = _profitable_config(lineage)
    book = build_empty_edge_challenger_book(_window_meta())
    config = SymbolConfig(
        symbol="EURUSD",
        regime_configs={"H1": {"TREND": champion}},
        default_config=champion,
        edge_challenger_book=book,
    )

    restored = SymbolConfig.from_dict(config.to_dict())

    assert restored.edge_challenger_book["book_role"] == EDGE_CHALLENGER_ROLE
    assert restored.edge_challenger_book["funded_role_eligible"] is False
    assert restored.get_regime_config("H1", "TREND").is_valid_for_live()


def test_latest_data_refit_writes_only_paper_challenger_book():
    pipeline = OptimizationPipeline.__new__(OptimizationPipeline)
    pipeline.config = PipelineConfig(
        false_discovery_control_enabled=False,
        false_discovery_holdout_policy="off",
        walk_forward_promotion_policy="off",
    )
    pipeline.regime_optimizer = Mock()
    pipeline.regime_optimizer.min_train_trades = 2
    pipeline.regime_optimizer._tune_strategy_params.return_value = [
        {
            "strategy_name": "DemoStrategy",
            "params": {"period": 11},
            "train_regime_metrics": {
                "TREND": {
                    "total_trades": 40,
                    "profit_factor": 1.3,
                    "total_return_pct": 12.0,
                    "max_drawdown_pct": 6.0,
                }
            },
            "tuned_for_regime": "TREND",
            "search_trials": 25,
            "search_phase": "edge_refit_test",
        }
    ]
    pipeline.regime_optimizer._compute_train_regime_score.return_value = 2.0
    pipeline.regime_optimizer._normalize_score.return_value = 0.75

    champion_lineage = build_lockbox_champion_lineage(_window_meta())
    champion = _profitable_config(champion_lineage)
    champion.parameters = {"period": 20}
    index = pd.date_range("2020-01-01", "2026-01-01", freq="D", tz="UTC")
    full_features = pd.DataFrame(
        {"Open": 1.0, "High": 1.1, "Low": 0.9, "Close": 1.0},
        index=index,
    )
    strategy = Mock()
    strategy.get_param_grid.return_value = {"period": [10, 11, 12]}

    with patch("pm_pipeline.StrategyRegistry.get", return_value=strategy):
        challengers, failures = pipeline._build_edge_challengers_for_timeframe(
            symbol="EURUSD",
            timeframe="H1",
            champion_configs={"TREND": champion},
            full_features=full_features,
            fit_start=pd.Timestamp("2021-01-01T00:00:00Z"),
            fit_end=pd.Timestamp("2026-01-01T00:00:00Z"),
            champion_temporal_lineage=champion_lineage,
        )

    assert failures == []
    challenger = challengers["TREND"]
    assert challenger["parameters"] == {"period": 11}
    assert challenger["promotion_status"] == "PAPER_ONLY"
    temporal = challenger["validation_evidence"]["temporal_lineage"]
    assert temporal["book_role"] == EDGE_CHALLENGER_ROLE
    assert temporal["fit_cutoff_utc"].startswith("2026-01-01")
    assert temporal["performance_evidenced_through_utc"] is None
    assert not temporal["champion_lockbox_verdict_inherited"]
    # The funded champion remains immutable and physically separate.
    assert champion.parameters == {"period": 20}


def _digest(label: str) -> str:
    return canonical_receipt_sha256({"label": label})


def _detector_parameters():
    return {
        "k_confirm": 2,
        "gap_min": 0.1,
        "k_hold": 3,
        "adx_period": 14,
        "atr_period": 14,
        "bb_period": 20,
        "bb_std": 2.0,
        "efficiency_period": 10,
        "bb_squeeze_lookback": 50,
        "bb_squeeze_percentile": 0.2,
        "atr_lookback": 50,
        "structure_period": 20,
        "structure_atr_mult": 1.0,
        "trend_adx_weight": 0.4,
        "trend_efficiency_weight": 0.3,
        "trend_slope_weight": 0.3,
        "range_adx_weight": 0.4,
        "range_efficiency_weight": 0.3,
        "range_containment_weight": 0.3,
        "breakout_squeeze_weight": 0.4,
        "breakout_atr_weight": 0.3,
        "breakout_structure_weight": 0.3,
        "chop_adx_weight": 0.4,
        "chop_efficiency_weight": 0.3,
        "chop_whipsaw_weight": 0.3,
        "adx_trend_threshold": 25.0,
    }


def _train_support(symbol, timeframe, *, activation_id, manifest_sha, source_sha, params_sha, code_sha):
    counts = {name: (10 if name == "TREND" else 0) for name in REGIME_ORDER}
    body = {
        "schema_version": "fxpm.regime-train-support/v1",
        "symbol": symbol,
        "timeframe": timeframe,
        "surface": "REGIME_LIVE",
        "warmup_policy": "CURRENT_AND_SOURCE_BAR_NOT_WARMUP",
        "label_order": list(REGIME_ORDER),
        "eligible_rows": 10,
        "counts": counts,
        "observed_regimes": ["TREND"],
        "unobserved_regimes": ["RANGE", "BREAKOUT", "CHOP"],
        "support_state_by_regime": {
            name: "EMPIRICALLY_OBSERVED" if name == "TREND" else "UNOBSERVED"
            for name in REGIME_ORDER
        },
        "first_ts": "2025-01-01T00:00:00+00:00",
        "last_ts": "2026-07-12T23:55:00+00:00",
        "vector_encoding": "fxpm.regime-live-labels-nul/v1",
        "vector_sha256": _digest(f"labels-{symbol}-{timeframe}"),
        "bindings": {
            "symbol": symbol,
            "timeframe": timeframe,
            "activation_id": activation_id,
            "activation_manifest_sha256": manifest_sha,
            "source_sha256": source_sha,
            "train_window_sha256": _digest(f"train-window-{symbol}-{timeframe}"),
            "params_sha256": params_sha,
            "detector_code_sha256": code_sha,
        },
    }
    return {**body, "receipt_sha256": canonical_receipt_sha256(body)}


def _regime_params_bundle(cutoff, groups=(('EURUSD', 'M5'), ('EURUSD', 'D1'))):
    fits = []
    for symbol, timeframe in groups:
        activation_id = f"native-current-{symbol}-{timeframe}"
        manifest_sha = _digest(f"activation-manifest-{symbol}-{timeframe}")
        source_sha = _digest(f"native-source-{symbol}-{timeframe}")
        source = create_historical_source_group_decision(
            symbol=symbol,
            timeframe=timeframe,
            availability="AVAILABLE",
            reason_code="NATIVE_SOURCE_AVAILABLE",
            source_contract_sha256=_digest(f"source-contract-{symbol}-{timeframe}"),
            source_group_evidence_sha256=_digest(f"source-evidence-{symbol}-{timeframe}"),
            acquisition_attempts_exact=1,
            activation_id=activation_id,
            activation_manifest_sha256=manifest_sha,
            native_source_sha256=source_sha,
        )
        assert source is not None
        code_sha = _digest(f"detector-code-{symbol}-{timeframe}")
        record = create_regime_detector_record(
            source_group_decision=source,
            fit_cutoff_utc=cutoff,
            detector_version="fxpm-regime-live-v1",
            detector_code_sha256=code_sha,
            parameters=_detector_parameters(),
        )
        assert record is not None
        fit = create_regime_fit_decision(
            source_group_decision=source,
            fit_state="FITTED",
            reason_code="CURRENT_SHARED_REGIME_FIT_COMPLETE",
            regime_fit_policy_sha256=_digest(f"fit-policy-{symbol}-{timeframe}"),
            detector_process_sha256=code_sha,
            regime_fit_evidence_sha256=_digest(f"fit-evidence-{symbol}-{timeframe}"),
            four_regime_mapping_sha256=_digest(f"regime-map-{symbol}-{timeframe}"),
            regime_params_sha256=record["parameters_sha256"],
            regime_detector_record=record,
            train_regime_support=_train_support(
                symbol,
                timeframe,
                activation_id=activation_id,
                manifest_sha=manifest_sha,
                source_sha=source_sha,
                params_sha=record["parameters_sha256"],
                code_sha=code_sha,
            ),
        )
        assert fit is not None
        payload = fit.to_dict()
        receipt_sha = payload.pop("receipt_sha256")
        fits.append({"payload": payload, "receipt_sha256": receipt_sha})
    return build_current_regime_params_bundle(
        current_cutoff_utc=cutoff,
        regime_fits=fits,
    )


def _process_decision():
    return {
        "schema": "causal-schema-is-not-pinned-here",
        "authority_version": "causal-v3-test",
        "authority_scope": "OFFLINE_PROCESS_VALIDATION_ONLY",
        "permitted_use": "OFFLINE_PROCESS_VALIDATION_EVIDENCE_INPUT",
        "explicit_non_authorities": [
            "CURRENT_REFIT",
            "MODEL_PROMOTION",
            "CAPITAL_ALLOCATION",
            "FUNDED_LIVE_TRADING",
            "LIVE_TRADING",
            "PROFITABILITY_CLAIM",
        ],
        "process_id": "causal-process-v3-accepted",
        "process_definition_sha256": _digest("causal-definition"),
        "policy_sha256": _digest("causal-policy"),
        "precommit_receipt_sha256": _digest("causal-precommit"),
        "evidence_receipt_sha256": _digest("causal-evidence"),
        "reconciliation_receipt_sha256": _digest("causal-reconciliation"),
        "evaluated_at_utc": "2026-07-12T11:00:00+00:00",
        "metrics": {
            "anchor_count": 4,
            "total_calendar_days": 1460,
            "completed_trade_count": 480,
            "effective_dependence_blocks": "42.5",
            "sample_size_hac_floor_calendar_days": 34,
            "primary_hac_bandwidth_calendar_days": 34,
            "sensitivity_hac_bandwidth_calendar_days": 68,
            "most_influential_block_calendar_days": 35,
            "small_cluster_degrees_of_freedom": 3,
            "small_cluster_one_sided_critical_value": "2.353",
        },
        "gates": {"primary_hac": True, "sensitivity_hac": True, "influence": True},
        "decision": "OFFLINE_PROCESS_VALIDATION_PASS",
        "failure_reasons": [],
    }


def _winner_roster():
    return [
        {
            "symbol": "EURUSD",
            "timeframe": timeframe,
            "regime": "TREND",
            "strategy_id": f"Current{timeframe}Expert",
            "winner_process_candidate_id": f"winner-{timeframe.lower()}",
            "winner_process_sha256": _digest(f"winner-process-{timeframe}"),
            "winner_horizon_process_sha256": _digest(
                f"current-horizon-winner-{timeframe}"
            ),
            "winner_trial_definition_sha256": _digest(
                f"current-trial-definition-{timeframe}"
            ),
            "winner_refit_recipe_sha256": _digest(f"current-refit-recipe-{timeframe}"),
            "winner_decision_sha256": _digest(f"winner-decision-{timeframe}"),
            "winner_book_entry_sha256": _digest(f"book-entry-{timeframe}"),
        }
        for timeframe in ("M5", "D1")
    ]


def _book_generation_sha(payload):
    return canonical_receipt_sha256(
        {name: value for name, value in payload.items() if name != "generation_sha256"}
    )


def _process_refit_receipts(
    *,
    process_change=None,
    roster_change=None,
    book_change=None,
    authorization_change=None,
    execution_change=None,
    cell_change=None,
):
    process_decision = _process_decision()
    if process_change:
        process_change(process_decision)
    process_decision_sha = canonical_receipt_sha256(process_decision)
    roster = _winner_roster()
    cutoff = "2026-07-13T00:00:00+00:00"
    regime_bundle = _regime_params_bundle(cutoff)
    fits_by_group = {
        (fit["payload"]["symbol"], fit["payload"]["timeframe"]): fit
        for fit in regime_bundle["regime_fits"]
    }
    for winner in roster:
        fit = fits_by_group[(winner["symbol"], winner["timeframe"])]
        record = fit["payload"]["regime_detector_record"]
        winner.update(
            {
                "regime_fit_receipt_sha256": fit["receipt_sha256"],
                "regime_detector_record_sha256": record["record_sha256"],
                "regime_detector_parameters_sha256": record["parameters_sha256"],
                "regime_detector_code_sha256": record["detector_code_sha256"],
            }
        )
    if roster_change:
        roster_change(roster)
    roster_sha = compute_current_winner_roster_sha256(roster)
    book_payload = {
        "schema_version": ACCEPTED_BOOK_GENERATION_RECEIPT_VERSION,
        "generation_id": "current-full-process-generation-20260713",
        "historical_process_decision_sha256": process_decision_sha,
        "process_id": process_decision["process_id"],
        "process_definition_sha256": process_decision["process_definition_sha256"],
        "generation_sha256": "0" * 64,
        "current_cutoff_utc": cutoff,
        "maximum_input_timestamp_utc": "2026-07-12T23:55:00+00:00",
        "source_snapshot_bundle_sha256": _digest("current-source-bundle"),
        "regime_params_sha256": canonical_receipt_sha256(regime_bundle),
        "horizon_decisions_sha256": _digest("current-horizon-decisions"),
        "full_cell_outcome_roster_sha256": _digest("all-2400-current-cell-outcomes"),
        "selected_winner_roster_sha256": roster_sha,
        "trial_ledger_bundle_sha256": _digest("all-current-trial-ledgers"),
        "winner_bundle_sha256": _digest("winner-bundle"),
        "output_book_sha256": _digest("current-process-winner-book"),
        "book_multiplicity_receipt_sha256": _digest("book-multiplicity"),
        "full_process_execution_trace_sha256": _digest(
            "full-current-process-execution"
        ),
        "started_at_utc": "2026-07-13T00:00:01+00:00",
        "completed_at_utc": "2026-07-13T02:00:00+00:00",
        "status": "COMPLETE",
        "selection_scope": "FULL_CURRENT_PROCESS_ALL_PRECOMMITTED_CELLS",
        "precommitted_cell_count": 2_400,
        "selected_winner_count": len(roster),
        "attempts": 1,
        "identical_frozen_process_executed": True,
        "post_cutoff_data_used": False,
        "complete_precommitted_roster_processed": True,
        "current_winner_selection_completed": True,
        "historical_anchor_winner_inputs_used": False,
        "survivor_only_reoptimization": False,
        "parameter_level_oos_evidence_claimed": False,
        "funded_live_authority": False,
    }
    if book_change:
        book_change(book_payload)
    book_payload["generation_sha256"] = _book_generation_sha(book_payload)
    book_receipt = build_typed_receipt(
        book_payload,
        receipt_type=CURRENT_PROCESS_BOOK_RECEIPT_TYPE,
    )
    book_receipt_sha = canonical_receipt_sha256(book_receipt)
    cells = []
    for winner in roster:
        timeframe = winner["timeframe"]
        identity = {
            "historical_process_decision_sha256": process_decision_sha,
            "accepted_book_generation_receipt_sha256": book_receipt_sha,
            **winner,
            "fit_start_utc": (
                "2024-01-01T00:00:00+00:00"
                if timeframe == "M5"
                else "2010-01-01T00:00:00+00:00"
            ),
            "fit_end_utc": cutoff,
            "latest_closed_bar_utc": (
                "2026-07-12T23:55:00+00:00"
                if timeframe == "M5"
                else "2026-07-12T00:00:00+00:00"
            ),
            "source_snapshot_sha256": _digest(f"current-source-{timeframe}"),
            "parameter_sha256": _digest(f"parameters-{timeframe}"),
            "output_artifact_sha256": _digest(f"artifact-{timeframe}"),
        }
        cell = {
            "current_refit_cell_id": compute_current_refit_cell_id(identity),
            **identity,
            "execution_trace_sha256": _digest(f"trace-{timeframe}"),
            "attempts": 1,
            "status": "COMPLETE",
            "refit_process_frozen": True,
            "parameter_level_oos_evidence_claimed": False,
        }
        cells.append(cell)
    if cell_change:
        cell_change(cells)
        for cell in cells:
            identity_fields = (
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
            if any(name not in cell for name in identity_fields):
                continue
            identity = {name: cell[name] for name in identity_fields}
            cell["current_refit_cell_id"] = compute_current_refit_cell_id(identity)
    authorization = {
        "authorization_id": "current-refit-20260713",
        "historical_process_decision_sha256": process_decision_sha,
        "process_id": process_decision["process_id"],
        "process_definition_sha256": process_decision["process_definition_sha256"],
        "accepted_book_generation_receipt_sha256": book_receipt_sha,
        "accepted_book_generation_sha256": book_payload["generation_sha256"],
        "current_cutoff_utc": cutoff,
        "issued_at_utc": "2026-07-13T02:01:00+00:00",
        "source_snapshot_bundle_sha256": book_payload["source_snapshot_bundle_sha256"],
        "regime_params_sha256": book_payload["regime_params_sha256"],
        "horizon_decisions_sha256": book_payload["horizon_decisions_sha256"],
        "selected_winner_roster_sha256": roster_sha,
        "one_touch_nonce_sha256": _digest("one-touch-nonce"),
    }
    if authorization_change:
        authorization_change(authorization)
    authorization_sha = canonical_receipt_sha256(authorization)
    execution = {
        "authorization_sha256": authorization_sha,
        "process_definition_sha256": process_decision["process_definition_sha256"],
        "accepted_book_generation_sha256": book_payload["generation_sha256"],
        "started_at_utc": "2026-07-13T02:02:00+00:00",
        "completed_at_utc": "2026-07-13T03:00:00+00:00",
        "current_cutoff_utc": cutoff,
        "output_book_sha256": _digest("current-output-book"),
        "cell_execution_bundle_sha256": canonical_receipt_sha256(
            {"cell_executions": cells}
        ),
        "execution_trace_sha256": _digest("current-trace"),
        "cell_executions": cells,
        "attempts": 1,
        "status": "COMPLETE",
        "refit_process_frozen": True,
        "post_authorization_policy_change": False,
        "parameter_level_oos_evidence_claimed": False,
    }
    if execution_change:
        execution_change(execution)
    refit_payload = {
        "schema_version": CURRENT_REFIT_RECEIPT_VERSION,
        "authorization": authorization,
        "authorization_sha256": authorization_sha,
        "execution": execution,
        "execution_sha256": canonical_receipt_sha256(execution),
    }
    refit_receipt = build_typed_receipt(
        refit_payload,
        receipt_type=CURRENT_REFIT_EXECUTION_RECEIPT_TYPE,
    )
    return process_decision, book_receipt, refit_receipt


def _process_refit_lineage(**kwargs):
    process_decision, book_receipt, refit_receipt = _process_refit_receipts(**kwargs)
    return build_process_validated_current_refit_lineage(
        historical_process_decision=process_decision,
        accepted_book_generation_receipt=book_receipt,
        current_refit_receipt=refit_receipt,
        as_of_utc="2026-07-13T00:00:00Z",
    )


def test_process_validated_current_refit_is_one_global_generation_with_independent_cells():
    lineage = _process_refit_lineage()
    assert lineage["contract_status"] == "PROBATION_ONLY"
    assert lineage["book_role"] == PROCESS_VALIDATED_CURRENT_REFIT_ROLE
    assert lineage["current_process_precommitted_cell_count"] == 2_400
    assert lineage["current_process_selected_winner_count"] == 2
    assert lineage["current_process_completed_at_utc"] == "2026-07-13T02:00:00+00:00"
    assert lineage["current_refit_cell_count"] == 2
    cells = lineage["current_refit_cells"]
    assert cells[0]["timeframe"] == "M5" and cells[1]["timeframe"] == "D1"
    assert cells[0]["winner_process_sha256"] != cells[1]["winner_process_sha256"]
    assert cells[0]["fit_start_utc"] != cells[1]["fit_start_utc"]
    assert {cell["fit_end_utc"] for cell in cells} == {"2026-07-13T00:00:00+00:00"}
    assert lineage["process_level_evidence_inherited"] is True
    assert lineage["parameter_level_oos_evidence_inherited"] is False
    assert lineage["probation_candidate_eligible"] is True
    assert lineage["funded_role_eligible"] is False
    assert lineage["production_promotable"] is False
    assert "detached probation" in temporal_role_block_reason(
        {"temporal_lineage": lineage}
    )


def test_full_current_search_may_select_new_strategy_and_horizon_then_refit_them():
    lineage = _process_refit_lineage()
    assert lineage["contract_status"] == "PROBATION_ONLY", lineage
    cells = lineage["current_refit_cells"]
    assert {cell["strategy_id"] for cell in cells} == {
        "CurrentM5Expert",
        "CurrentD1Expert",
    }
    assert {cell["winner_horizon_process_sha256"] for cell in cells} == {
        _digest("current-horizon-winner-M5"),
        _digest("current-horizon-winner-D1"),
    }
    assert all(
        cell["strategy_id"] not in {"HistoricalM5Survivor", "HistoricalD1Survivor"}
        for cell in cells
    )


def test_current_winner_roster_digest_is_order_independent_and_cell_specific():
    _process, _book, receipt = _process_refit_receipts()
    payload, _sha = open_typed_receipt(
        receipt,
        expected_receipt_type=CURRENT_REFIT_EXECUTION_RECEIPT_TYPE,
    )
    roster = payload["execution"]["cell_executions"]
    assert compute_current_winner_roster_sha256(
        roster
    ) == compute_current_winner_roster_sha256(list(reversed(roster)))
    changed = copy.deepcopy(roster)
    changed[0]["winner_refit_recipe_sha256"] = _digest("another-current-recipe")
    assert compute_current_winner_roster_sha256(
        changed
    ) != compute_current_winner_roster_sha256(roster)


def test_causal_decision_schema_label_is_not_pinned_but_exact_body_is_rebound():
    lineage = _process_refit_lineage(
        process_change=lambda decision: decision.update(
            {"schema": "repaired-causal-public-schema"}
        )
    )
    assert lineage["contract_status"] == "PROBATION_ONLY", lineage
    assert lineage["historical_process_decision_sha256"] == canonical_receipt_sha256(
        _process_refit_receipts(
            process_change=lambda decision: decision.update(
                {"schema": "repaired-causal-public-schema"}
            )
        )[0]
    )


def test_causal_failed_gate_and_wrong_decimal_geometry_fail_closed():
    failed_gate = _process_refit_lineage(
        process_change=lambda decision: decision["gates"].update(
            {"sensitivity_hac": False}
        )
    )
    wrong_type = _process_refit_lineage(
        process_change=lambda decision: decision["metrics"].update(
            {"small_cluster_one_sided_critical_value": 2.353}
        )
    )
    assert failed_gate["contract_status"] == "FAIL"
    assert "failed evidence gate" in failed_gate["contract_errors"][0]
    assert wrong_type["contract_status"] == "FAIL"
    assert "decimal string" in wrong_type["contract_errors"][0]


def test_process_definitions_may_repeat_but_cell_specific_winner_receipts_may_not():
    shared_process = _process_refit_lineage(
        roster_change=lambda cells: cells[1].update(
            {
                "winner_process_candidate_id": cells[0]["winner_process_candidate_id"],
                "winner_process_sha256": cells[0]["winner_process_sha256"],
            }
        )
    )
    replayed_decision = _process_refit_lineage(
        roster_change=lambda cells: cells[1].update(
            {"winner_decision_sha256": cells[0]["winner_decision_sha256"]}
        )
    )
    assert shared_process["contract_status"] == "PROBATION_ONLY", shared_process
    assert replayed_decision["contract_status"] == "FAIL"
    assert "reuse winner_decision_sha256" in replayed_decision["contract_errors"][0]


def test_current_refit_rejects_book_generation_receipt_splice():
    process, book, refit = _process_refit_receipts()
    other_book = copy.deepcopy(book)
    other_book["payload_sha256"] = "0" * 64
    lineage = build_process_validated_current_refit_lineage(
        historical_process_decision=process,
        accepted_book_generation_receipt=other_book,
        current_refit_receipt=refit,
        as_of_utc="2026-07-13T00:00:00Z",
    )
    assert lineage["contract_status"] == "FAIL"


def test_current_refit_rejects_survivor_only_or_historical_winner_reuse():
    survivor_only = _process_refit_lineage(
        book_change=lambda payload: payload.update(
            {"survivor_only_reoptimization": True}
        )
    )
    anchor_reuse = _process_refit_lineage(
        book_change=lambda payload: payload.update(
            {"historical_anchor_winner_inputs_used": True}
        )
    )
    assert survivor_only["contract_status"] == "FAIL"
    assert "survivor-only" in survivor_only["contract_errors"][0]
    assert anchor_reuse["contract_status"] == "FAIL"
    assert "anchor winners" in anchor_reuse["contract_errors"][0]


def test_book_and_current_refit_causal_clocks_are_strictly_ordered():
    before_decision = _process_refit_lineage(
        book_change=lambda payload: payload.update(
            {
                "current_cutoff_utc": "2026-07-12T10:00:00+00:00",
                "started_at_utc": "2026-07-12T10:01:00+00:00",
                "completed_at_utc": "2026-07-12T10:02:00+00:00",
            }
        )
    )
    process_before_cutoff = _process_refit_lineage()
    authorization_before_book = _process_refit_lineage(
        authorization_change=lambda authorization: authorization.update(
            {"issued_at_utc": "2026-07-13T01:59:59+00:00"}
        )
    )
    assert before_decision["contract_status"] == "FAIL"
    assert "process decision and data cutoff" in before_decision["contract_errors"][0]
    assert process_before_cutoff["contract_status"] == "PROBATION_ONLY"
    assert authorization_before_book["contract_status"] == "FAIL"
    assert (
        "predates current Book completion"
        in authorization_before_book["contract_errors"][0]
    )


def test_process_book_and_refit_reject_future_trusted_clocks():
    future_process = _process_refit_lineage(
        process_change=lambda decision: decision.update(
            {"evaluated_at_utc": "2099-07-12T23:00:00+00:00"}
        )
    )
    future_book = _process_refit_lineage(
        book_change=lambda payload: payload.update(
            {
                "started_at_utc": "2099-07-13T00:00:01+00:00",
                "completed_at_utc": "2099-07-13T02:00:00+00:00",
            }
        )
    )
    future_refit = _process_refit_lineage(
        execution_change=lambda execution: execution.update(
            {
                "started_at_utc": "2099-07-13T02:02:00+00:00",
                "completed_at_utc": "2099-07-13T03:00:00+00:00",
            }
        )
    )
    assert future_process["contract_status"] == "FAIL"
    assert "future trusted clock" in future_process["contract_errors"][0]
    assert future_book["contract_status"] == "FAIL"
    assert "future trusted completion clock" in future_book["contract_errors"][0]
    assert future_refit["contract_status"] == "FAIL"
    assert "future trusted completion clock" in future_refit["contract_errors"][0]


def test_current_process_requires_full_2400_cell_scope_before_winner_refits():
    partial = _process_refit_lineage(
        book_change=lambda payload: payload.update({"precommitted_cell_count": 2_399})
    )
    wrong_scope = _process_refit_lineage(
        book_change=lambda payload: payload.update(
            {"selection_scope": "HISTORICAL_SURVIVORS_ONLY"}
        )
    )
    repeated = _process_refit_lineage(
        book_change=lambda payload: payload.update({"attempts": 2})
    )
    post_cutoff = _process_refit_lineage(
        book_change=lambda payload: payload.update(
            {"maximum_input_timestamp_utc": "2026-07-13T00:00:01+00:00"}
        )
    )
    assert partial["contract_status"] == "FAIL"
    assert "2,400 cells" in partial["contract_errors"][0]
    assert wrong_scope["contract_status"] == "FAIL"
    assert "full process scope" in wrong_scope["contract_errors"][0]
    assert repeated["contract_status"] == "FAIL"
    assert "exactly once" in repeated["contract_errors"][0]
    assert post_cutoff["contract_status"] == "FAIL"
    assert "maximum input exceeds" in post_cutoff["contract_errors"][0]


def test_current_refit_rejects_shared_history_assumption_only_when_cell_geometry_is_false():
    lineage = _process_refit_lineage(
        cell_change=lambda cells: cells[1].update(
            {"latest_closed_bar_utc": "2009-12-31T00:00:00+00:00"}
        )
    )
    assert lineage["contract_status"] == "FAIL"
    assert "independent history" in lineage["contract_errors"][0]


def test_current_refit_rejects_cell_or_global_parameter_oos_inheritance():
    cell_lineage = _process_refit_lineage(
        cell_change=lambda cells: cells[0].update(
            {"parameter_level_oos_evidence_claimed": True}
        )
    )
    global_lineage = _process_refit_lineage(
        execution_change=lambda execution: execution.update(
            {"parameter_level_oos_evidence_claimed": True}
        )
    )
    assert cell_lineage["contract_status"] == "FAIL"
    assert global_lineage["contract_status"] == "FAIL"


def test_current_refit_rejects_multiple_attempts_and_mixed_current_cutoffs():
    attempts = _process_refit_lineage(
        execution_change=lambda execution: execution.update({"attempts": 2})
    )
    cutoff = _process_refit_lineage(
        cell_change=lambda cells: cells[1].update(
            {"fit_end_utc": "2026-07-12T00:00:00+00:00"}
        )
    )
    assert attempts["contract_status"] == "FAIL"
    assert cutoff["contract_status"] == "FAIL"


def test_current_refit_rejects_roster_omission_even_with_rehashed_execution():
    lineage = _process_refit_lineage(cell_change=lambda cells: cells.pop())
    assert lineage["contract_status"] == "FAIL"
    assert "cell execution roster" in lineage["contract_errors"][0]


def test_current_refit_detector_hashes_are_required_cell_identity():
    missing = _process_refit_lineage(
        cell_change=lambda cells: cells[0].pop("regime_fit_receipt_sha256")
    )
    changed = _process_refit_lineage(
        cell_change=lambda cells: cells[0].update(
            {"regime_detector_record_sha256": _digest("forged-detector-record")}
        )
    )

    assert missing["contract_status"] == "FAIL"
    assert "current refit cell fields" in missing["contract_errors"][0]
    assert changed["contract_status"] == "FAIL"
    assert "current Book roster" in changed["contract_errors"][0]


def test_current_refit_sibling_regimes_must_share_one_detector_fit():
    def create_mismatched_siblings(roster):
        roster[0]["regime"] = "RANGE"
        roster[1]["timeframe"] = "M5"

    lineage = _process_refit_lineage(roster_change=create_mismatched_siblings)

    assert lineage["contract_status"] == "FAIL"
    assert "sibling regimes changed" in lineage["contract_errors"][0]


def test_exported_typed_receipt_api_is_constructible_without_keys_and_binds_type():
    payload = {"schema_version": "example.v1", "status": "COMPLETE"}
    receipt = build_typed_receipt(payload, receipt_type="fxpm.example")
    opened, receipt_sha = open_typed_receipt(
        receipt,
        expected_receipt_type="fxpm.example",
    )
    assert receipt["schema_version"] == TYPED_RECEIPT_VERSION
    assert opened == payload
    assert receipt_sha == canonical_receipt_sha256(receipt)
    assert "signing_key" not in inspect.signature(build_typed_receipt).parameters
    assert "verification_keys" not in inspect.signature(open_typed_receipt).parameters

    process, book, refit = _process_refit_receipts()
    tampered = copy.deepcopy(book)
    tampered["receipt_type"] = "fxpm.another-artifact"
    lineage = build_process_validated_current_refit_lineage(
        historical_process_decision=process,
        accepted_book_generation_receipt=tampered,
        current_refit_receipt=refit,
        as_of_utc="2026-07-13T00:00:00Z",
    )
    assert lineage["contract_status"] == "FAIL"
    assert "typed receipt type" in lineage["contract_errors"][0]


def test_legacy_v1_process_receipt_and_causal_decision_book_splice_cannot_enter():
    process, book, refit = _process_refit_receipts()
    legacy = build_typed_receipt(
        {"schema_version": "fxpm-process-validation-receipt-v1", "status": "PASS"},
        receipt_type="fxpm.legacy-process-validation",
    )
    legacy_result = build_process_validated_current_refit_lineage(
        historical_process_decision=legacy,
        accepted_book_generation_receipt=book,
        current_refit_receipt=refit,
        as_of_utc="2026-07-13T00:00:00Z",
    )
    other_process = copy.deepcopy(process)
    other_process["process_id"] = "another-causal-process"
    splice_result = build_process_validated_current_refit_lineage(
        historical_process_decision=other_process,
        accepted_book_generation_receipt=book,
        current_refit_receipt=refit,
        as_of_utc="2026-07-13T00:00:00Z",
    )
    assert legacy_result["contract_status"] == "FAIL"
    assert splice_result["contract_status"] == "FAIL"
    assert "another historical process decision" in splice_result["contract_errors"][0]
