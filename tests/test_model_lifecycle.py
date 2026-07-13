import copy

from pm_core import PipelineConfig
from pm_model_lifecycle import (
    CURRENT_REFIT_RECEIPT_VERSION,
    EDGE_CHALLENGER_ROLE,
    LOCKBOX_CHAMPION_ROLE,
    PROCESS_VALIDATED_CURRENT_REFIT_ROLE,
    PROCESS_VALIDATION_RECEIPT_VERSION,
    build_process_validated_current_refit_lineage,
    build_empty_edge_challenger_book,
    build_lockbox_champion_lineage,
    canonical_receipt_sha256,
    seal_authenticated_receipt,
    temporal_role_block_reason,
)
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


_RECEIPT_KEY = b"institutional-test-key-material!!"


def _digest(label: str) -> str:
    return canonical_receipt_sha256({"label": label})


def _process_refit_receipts(
    *,
    process_change=None,
    authorization_change=None,
    execution_change=None,
):
    process_contract = {
        name: _digest(name)
        for name in (
            "code_contract_sha256",
            "frozen_config_sha256",
            "candidate_domain_sha256",
            "decision_rule_sha256",
            "source_policy_sha256",
            "regime_policy_sha256",
            "horizon_policy_sha256",
            "scoring_policy_sha256",
            "cost_policy_sha256",
            "risk_policy_sha256",
        )
    }
    process_sha = canonical_receipt_sha256(process_contract)
    anchors = []
    for offset, year in enumerate((2020, 2021, 2022), start=1):
        anchors.append(
            {
                "anchor_id": f"anchor-{offset}",
                "process_sha256": process_sha,
                "fit_start_utc": f"{year - 5}-01-01T00:00:00+00:00",
                "selection_information_cutoff_utc": (
                    f"{year - 1}-12-01T00:00:00+00:00"
                ),
                "purge_end_utc": f"{year - 1}-12-15T00:00:00+00:00",
                "oos_start_utc": f"{year}-01-01T00:00:00+00:00",
                "oos_end_utc": f"{year}-06-30T00:00:00+00:00",
                "selection_max_input_utc": (
                    f"{year - 1}-12-01T00:00:00+00:00"
                ),
                "evaluation_max_input_utc": f"{year}-06-29T00:00:00+00:00",
                "post_oos_data_used_for_selection": False,
                "source_snapshot_sha256": _digest(f"source-{year}"),
                "regime_params_sha256": _digest(f"regime-{year}"),
                "horizon_decisions_sha256": _digest(f"horizon-{year}"),
                "trial_ledger_sha256": _digest(f"trials-{year}"),
                "selected_book_sha256": _digest(f"book-{year}"),
                "cell_roster_sha256": _digest(f"roster-{year}"),
                "cell_evidence_sha256": _digest(f"cells-{year}"),
                "portfolio_replay_sha256": _digest(f"portfolio-{year}"),
                "outcome_sha256": _digest(f"outcome-{year}"),
                "status": "COMPLETE",
            }
        )
    probation_sha = _digest("probation-calibration")
    process_payload = {
        "schema_version": PROCESS_VALIDATION_RECEIPT_VERSION,
        "process_contract": process_contract,
        "process_sha256": process_sha,
        "minimum_required_anchors": 3,
        "precommitted_anchor_ids": [row["anchor_id"] for row in anchors],
        "anchors": anchors,
        "anchor_bundle_sha256": canonical_receipt_sha256({"anchors": anchors}),
        "cell_evidence_bundle_sha256": _digest("all-cell-evidence"),
        "portfolio_replay_bundle_sha256": _digest("all-portfolio-replays"),
        "dependence_correction_sha256": _digest("dependence-correction"),
        "probation_calibration_sha256": probation_sha,
        "decision": {
            "status": "PASS",
            "decision_rule_sha256": process_contract["decision_rule_sha256"],
            "all_precommitted_anchors_consumed": True,
            "interior_holdout_used_as_final_gate": False,
            "dependence_adjusted": True,
            "multiple_testing_adjusted": True,
            "cell_evidence_complete": True,
            "common_portfolio_replay_complete": True,
            "process_level_evidence_only": True,
        },
    }
    if process_change is not None:
        process_change(process_payload)
    process_receipt = seal_authenticated_receipt(
        process_payload,
        key_id="test-process-key",
        signing_key=_RECEIPT_KEY,
    )
    process_receipt_sha = canonical_receipt_sha256(process_receipt)

    authorization = {
        "authorization_id": "current-refit-20260713",
        "process_validation_receipt_sha256": process_receipt_sha,
        "process_sha256": process_sha,
        "fit_start_utc": "2021-01-01T00:00:00+00:00",
        "fit_end_utc": "2026-07-13T00:00:00+00:00",
        "issued_at_utc": "2026-07-13T00:01:00+00:00",
        "source_snapshot_sha256": _digest("current-source"),
        "regime_params_sha256": _digest("current-regime"),
        "horizon_decisions_sha256": _digest("current-horizon"),
        "trial_ledger_sha256": _digest("current-trials"),
        "cell_roster_sha256": _digest("current-roster"),
        "code_contract_sha256": process_contract["code_contract_sha256"],
        "frozen_config_sha256": process_contract["frozen_config_sha256"],
        "probation_calibration_sha256": probation_sha,
        "one_touch_nonce_sha256": _digest("one-touch-nonce"),
    }
    if authorization_change is not None:
        authorization_change(authorization)
    authorization_sha = canonical_receipt_sha256(authorization)
    execution = {
        "authorization_sha256": authorization_sha,
        "process_sha256": process_sha,
        "started_at_utc": "2026-07-13T00:02:00+00:00",
        "completed_at_utc": "2026-07-13T01:00:00+00:00",
        "max_input_timestamp_utc": "2026-07-13T00:00:00+00:00",
        "output_book_sha256": _digest("current-output-book"),
        "selected_candidate_bundle_sha256": _digest("current-candidates"),
        "execution_trace_sha256": _digest("current-trace"),
        "attempts": 1,
        "status": "COMPLETE",
        "refit_process_frozen": True,
        "post_authorization_policy_change": False,
        "parameter_level_oos_evidence_claimed": False,
    }
    if execution_change is not None:
        execution_change(execution)
    refit_payload = {
        "schema_version": CURRENT_REFIT_RECEIPT_VERSION,
        "authorization": authorization,
        "authorization_sha256": authorization_sha,
        "execution": execution,
        "execution_sha256": canonical_receipt_sha256(execution),
    }
    refit_receipt = seal_authenticated_receipt(
        refit_payload,
        key_id="test-process-key",
        signing_key=_RECEIPT_KEY,
    )
    return process_receipt, refit_receipt


def _process_refit_lineage(**kwargs):
    process_receipt, refit_receipt = _process_refit_receipts(**kwargs)
    return build_process_validated_current_refit_lineage(
        process_validation_receipt=process_receipt,
        current_refit_receipt=refit_receipt,
        verification_keys={"test-process-key": _RECEIPT_KEY},
        as_of_utc="2026-07-13T00:00:00Z",
    )


def test_process_validated_current_refit_is_a_distinct_probation_only_lineage():
    lineage = _process_refit_lineage()

    assert lineage["contract_status"] == "PROBATION_ONLY"
    assert lineage["book_role"] == PROCESS_VALIDATED_CURRENT_REFIT_ROLE
    assert lineage["strict_leave_future_out_anchor_count"] == 3
    assert lineage["process_level_evidence_inherited"] is True
    assert lineage["parameter_level_oos_evidence_inherited"] is False
    assert lineage["champion_lockbox_verdict_inherited"] is False
    assert lineage["probation_candidate_eligible"] is True
    assert lineage["probation_role_eligible"] is False
    assert lineage["funded_role_eligible"] is False
    assert lineage["production_promotable"] is False
    assert lineage["parameter_selection_cutoff_utc"].startswith("2026-07-13")
    assert "detached probation" in temporal_role_block_reason(
        {"temporal_lineage": lineage}
    )


def test_process_refit_rejects_a_tampered_authenticated_anchor():
    process_receipt, refit_receipt = _process_refit_receipts()
    tampered = copy.deepcopy(process_receipt)
    tampered["payload"]["anchors"][0]["status"] = "FAIL"

    lineage = build_process_validated_current_refit_lineage(
        process_validation_receipt=tampered,
        current_refit_receipt=refit_receipt,
        verification_keys={"test-process-key": _RECEIPT_KEY},
        as_of_utc="2026-07-13T00:00:00Z",
    )

    assert lineage["contract_status"] == "FAIL"
    assert "digest mismatch" in lineage["contract_errors"][0]


def test_process_refit_rejects_middle_holdout_as_the_final_gate():
    lineage = _process_refit_lineage(
        process_change=lambda payload: payload["decision"].update(
            {"interior_holdout_used_as_final_gate": True}
        )
    )

    assert lineage["contract_status"] == "FAIL"
    assert "interior holdout" in lineage["contract_errors"][0]


def test_process_refit_rejects_future_information_in_historical_selection():
    def use_future(payload):
        payload["anchors"][1]["selection_max_input_utc"] = (
            "2021-02-01T00:00:00+00:00"
        )

    lineage = _process_refit_lineage(process_change=use_future)

    assert lineage["contract_status"] == "FAIL"
    assert "outside its frozen fit interval" in lineage["contract_errors"][0]


def test_process_refit_rejects_survivor_selection_of_anchor_subset():
    def omit_anchor(payload):
        payload["anchors"] = payload["anchors"][:2]

    lineage = _process_refit_lineage(process_change=omit_anchor)

    assert lineage["contract_status"] == "FAIL"
    assert "complete precommitted anchor set" in lineage["contract_errors"][0]


def test_process_refit_rejects_multiple_current_refit_attempts():
    lineage = _process_refit_lineage(
        execution_change=lambda execution: execution.update({"attempts": 2})
    )

    assert lineage["contract_status"] == "FAIL"
    assert "one-touch" in lineage["contract_errors"][0]


def test_process_refit_rejects_authorization_issued_after_execution_started():
    lineage = _process_refit_lineage(
        authorization_change=lambda authorization: authorization.update(
            {"issued_at_utc": "2026-07-13T00:03:00+00:00"}
        )
    )

    assert lineage["contract_status"] == "FAIL"
    assert "authorized before execution" in lineage["contract_errors"][0]


def test_process_refit_rejects_parameter_level_evidence_inheritance_claim():
    lineage = _process_refit_lineage(
        execution_change=lambda execution: execution.update(
            {"parameter_level_oos_evidence_claimed": True}
        )
    )

    assert lineage["contract_status"] == "FAIL"
    assert "parameter-level OOS evidence" in lineage["contract_errors"][0]


def test_authenticated_receipt_binds_key_identity_not_only_payload():
    process_receipt, refit_receipt = _process_refit_receipts()
    tampered = copy.deepcopy(process_receipt)
    tampered["authentication"]["key_id"] = "alternate-key"

    lineage = build_process_validated_current_refit_lineage(
        process_validation_receipt=tampered,
        current_refit_receipt=refit_receipt,
        verification_keys={
            "test-process-key": _RECEIPT_KEY,
            "alternate-key": _RECEIPT_KEY,
        },
        as_of_utc="2026-07-13T00:00:00Z",
    )

    assert lineage["contract_status"] == "FAIL"
    assert "HMAC mismatch" in lineage["contract_errors"][0]


def test_process_refit_rejects_ambiguous_naive_historical_timestamps():
    def remove_timezone(payload):
        payload["anchors"][0]["selection_information_cutoff_utc"] = (
            "2019-12-01T00:00:00"
        )

    lineage = _process_refit_lineage(process_change=remove_timezone)

    assert lineage["contract_status"] == "FAIL"
    assert "explicit UTC timestamp" in lineage["contract_errors"][0]
