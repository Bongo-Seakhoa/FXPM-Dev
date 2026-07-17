from __future__ import annotations

import inspect
import sys
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest


PATCH_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PATCH_ROOT))

from pm_portfolio_replay import (  # noqa: E402
    PORTFOLIO_REPLAY_CANDIDATE_SCHEMA_VERSION,
    PORTFOLIO_REPLAY_ROSTER_SCHEMA_VERSION,
    CandidateTrade,
    ExitLeg,
    MarkPoint,
    PortfolioPolicy,
    SynchronizedPortfolioSimulator,
    TrailingCorrelationBook,
    bootstrap_ruin_probability,
    build_current_refit_replay_successor,
    candidate_trade_from_record,
    canonical_json_sha256,
    current_refit_candidate_record,
    floor_volume,
    portfolio_cost_contract,
    portfolio_policy_from_config_payload,
    validate_current_refit_replay_candidates,
)
from pm_core import Backtester, InstrumentSpec, PipelineConfig  # noqa: E402
from agent_audit.performance.scripts.run_synchronized_portfolio_replay import (  # noqa: E402
    _half_open_evaluation_frame,
    _marks_for_position,
    replay_cell_candidates,
    resolved_live_governance_mode,
    stop_risk_per_lot_including_commission,
)


@pytest.mark.parametrize(
    "removed_args",
    [
        ["--window-mode", "per_symbol_full"],
        ["--start", "2024-01-01"],
        ["--end", "2025-01-01"],
        ["--max-cells", "1"],
        ["--symbol", "EURUSD"],
        ["--no-mtm"],
        ["--allow-cell-errors"],
    ],
)
def test_production_replay_cli_has_no_partial_or_degrading_options(
    monkeypatch: pytest.MonkeyPatch,
    removed_args: list[str],
) -> None:
    from agent_audit.performance.scripts import (
        run_synchronized_portfolio_replay as replay_cli,
    )

    monkeypatch.setattr(
        sys,
        "argv",
        ["run_synchronized_portfolio_replay.py", *removed_args],
    )
    with pytest.raises(SystemExit) as exc_info:
        replay_cli.main()
    assert exc_info.value.code == 2


@pytest.mark.parametrize("partition", ["tuning_validation", "final_lockbox"])
def test_blocked_capital_authority_aborts_before_any_output_or_data_work(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    partition: str,
) -> None:
    from agent_audit.performance.scripts import (
        run_synchronized_portfolio_replay as replay_cli,
    )

    output = tmp_path / "must-not-be-created"
    missing_config = tmp_path / "must-not-be-read.json"
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "run_synchronized_portfolio_replay.py",
            "--partition",
            partition,
            "--config",
            str(missing_config),
            "--out-dir",
            str(output),
        ],
    )

    with pytest.raises(SystemExit, match="CAPITAL_AUTHORITY_UNAVAILABLE"):
        replay_cli.main()

    assert not output.exists()


def policy(**overrides) -> PortfolioPolicy:
    base = PortfolioPolicy(
        starting_balance=10_000.0,
        target_risk_per_trade_pct=1.0,
        max_risk_per_trade_pct=1.0,
        max_open_risk_pct=2.0,
        max_concurrent_positions=5,
        max_factor_group_open_risk_pct=2.0,
        enforce_pair_correlation=True,
        correlation_threshold=0.75,
        max_correlated_cluster_risk_pct=5.0,
        block_unknown_correlation=False,
        max_margin_utilization_pct=90.0,
        min_margin_level_pct=100.0,
        min_free_margin_pct=0.0,
        allowed_promotion_statuses=("PROMOTED",),
    )
    return replace(base, **overrides)


def test_funded_cost_contract_uses_code_owned_defaults_when_json_omits_switches() -> (
    None
):
    resolved = PipelineConfig()
    contract = portfolio_cost_contract(
        {
            "pipeline": {
                # These remain operator-owned values while the enablement
                # switches are deliberately absent from funded JSON.
                "backtest_spread_mode": "conservative_max",
                "slippage_pips": 0.75,
                "swap_model_enabled": True,
            }
        }
    )

    assert contract["use_spread"] is resolved.use_spread is True
    assert contract["use_commission"] is resolved.use_commission is True
    assert contract["use_slippage"] is resolved.use_slippage is True
    assert contract["backtest_spread_mode"] == "conservative_max"
    assert contract["slippage_pips"] == pytest.approx(0.75)


def test_funded_policy_disables_replay_only_pair_correlation_gate() -> None:
    payload = {
        "symbols": ["EURUSD", "XAUUSD"],
        "pipeline": {
            "max_total_open_risk_pct": 8.0,
            "max_factor_group_open_risk_pct": 2.0,
            "max_combined_risk_pct": 1.0,
            "max_new_entries_when_total_open_risk_above_pct": 0.0,
            "open_risk_action": "observe",
            "margin_entry_block_level": 200.0,
            "live_allowed_promotion_statuses": ["PROMOTED"],
        },
        "position": {"risk_per_trade_pct": 0.25, "max_risk_pct": 0.50},
    }

    funded = portfolio_policy_from_config_payload(payload, 10_000.0)

    assert funded.enforce_pair_correlation is False
    assert funded.max_factor_group_open_risk_pct == pytest.approx(2.0)


def test_replay_governance_uses_resolved_funded_default_when_json_omits_key() -> None:
    assert "local_governance_live_mode" not in PipelineConfig().__dict__ or (
        PipelineConfig().local_governance_live_mode == "off"
    )
    assert resolved_live_governance_mode(PipelineConfig()) == "off"


def test_candidate_stop_risk_includes_same_round_trip_commission_as_live():
    spec = SimpleNamespace(
        commission_per_lot=7.0,
        calculate_loss_at_stop=lambda *_args: 100.0,
    )
    assert (
        stop_risk_per_lot_including_commission(
            spec,
            entry_price=1.10,
            stop_price=1.09,
            direction=1,
            use_commission=True,
        )
        == 107.0
    )
    assert (
        stop_risk_per_lot_including_commission(
            spec,
            entry_price=1.10,
            stop_price=1.09,
            direction=1,
            use_commission=False,
        )
        == 100.0
    )


def candidate(
    candidate_id: str,
    *,
    symbol: str = "EURUSD",
    timeframe: str = "H1",
    asset_class: str = "fx",
    entry: str = "2025-01-01",
    exit: str = "2025-01-02",
    net_per_lot: float = 100.0,
    stop_risk_per_lot: float = 100.0,
    margin_per_lot: float = 100.0,
    volume_min: float = 0.1,
    volume_step: float = 0.1,
    direction: int = 1,
    groups=(),
    status: str = "PROMOTED",
    priority: float = 1.0,
    legs=None,
    marks=(),
) -> CandidateTrade:
    if legs is None:
        legs = (ExitLeg(exit, 1.0, net_per_lot, net_per_lot + 7.0, 7.0, 0.0, "exit"),)
    return CandidateTrade(
        candidate_id=candidate_id,
        cell_id=f"{symbol}|{timeframe}|TREND",
        symbol=symbol,
        timeframe=timeframe,
        regime="TREND",
        strategy_name="Test",
        promotion_status=status,
        asset_class=asset_class,
        risk_groups=tuple(groups),
        entry_time=entry,
        direction=direction,
        stop_risk_per_lot=stop_risk_per_lot,
        margin_per_lot=margin_per_lot,
        volume_min=volume_min,
        volume_max=100.0,
        volume_step=volume_step,
        priority_score=priority,
        legs=tuple(legs),
        marks=tuple(marks),
    )


def test_public_candidate_trade_codec_is_exact_and_supplies_no_defaults() -> None:
    original = candidate(
        "candidate-codec",
        marks=(MarkPoint("2025-01-01T12:00:00Z", 12.5, "2025-01-01T12:00:00Z"),),
    ).normalized()
    record = current_refit_candidate_record(original)

    rebuilt = candidate_trade_from_record(record)

    assert rebuilt == original
    assert current_refit_candidate_record(rebuilt) == record
    missing = dict(record)
    missing.pop("margin_model")
    with pytest.raises(ValueError, match="candidate record fields invalid"):
        candidate_trade_from_record(missing)
    changed = dict(record)
    changed["entry_time"] = "not-a-time"
    with pytest.raises((TypeError, ValueError)):
        candidate_trade_from_record(changed)


def _current_refit_roster() -> dict:
    rows = [
        {
            "cell_id": "EURUSD|H1|TREND",
            "strategy_name": "Test",
            "promotion_status": "PROCESS_VALIDATED_CURRENT_REFIT",
        }
    ]
    accounting = {
        "accounting_sha256": "a" * 64,
        "winner_slot_ids": ["EURUSD|H1|TREND"],
    }
    lineage = {"current_refit_receipt_sha256": "b" * 64}
    roster = {
        "schema_version": PORTFOLIO_REPLAY_ROSTER_SCHEMA_VERSION,
        "authoritative": True,
        "current_refit_execution_reverified": True,
        "authority_status": "PROCESS_VALIDATED_CURRENT_REFIT",
        "book_accounting": accounting,
        "current_refit_lineage": lineage,
        "physical_ledger_sha256": "c" * 64,
        "tradeable_cell_ids": ["EURUSD|H1|TREND"],
        "rows": rows,
    }
    roster["roster_sha256"] = canonical_json_sha256(
        {
            "authority_status": roster["authority_status"],
            "current_refit_execution_reverified": True,
            "book_accounting_sha256": accounting["accounting_sha256"],
            "current_refit_lineage": lineage,
            "physical_ledger_sha256": roster["physical_ledger_sha256"],
            "rows": rows,
        }
    )
    return roster


def _strict_current_refit_roster() -> dict:
    lineage = {
        "historical_process_decision_sha256": "1" * 64,
        "accepted_book_generation_receipt_sha256": "2" * 64,
        "current_refit_receipt_sha256": "3" * 64,
        "current_refit_execution_sha256": "4" * 64,
        "current_refit_cell_count": 1,
        "current_selected_winner_roster_sha256": "5" * 64,
        "current_refit_output_book_sha256": "6" * 64,
    }
    verification = {"verified": True, "errors": []}
    rows = [
        {
            "cell_id": "EURUSD|H1|TREND",
            "strategy_name": "Test",
            "promotion_status": "PROCESS_VALIDATED_CURRENT_REFIT",
            "current_refit_binding": {},
            "artifact_sha256": "7" * 64,
            "cell_sha256": "8" * 64,
        }
    ]
    accounting = {
        "accounting_sha256": "9" * 64,
        "winner_slot_ids": ["EURUSD|H1|TREND"],
    }
    roster = {
        "schema_version": PORTFOLIO_REPLAY_ROSTER_SCHEMA_VERSION,
        "authoritative": True,
        "current_refit_execution_reverified": True,
        "authority_status": "PROCESS_VALIDATED_CURRENT_REFIT",
        "book_accounting": accounting,
        "current_refit_lineage": lineage,
        "current_refit_execution_verification": verification,
        "physical_ledger_sha256": "6" * 64,
        "tradeable_cell_ids": ["EURUSD|H1|TREND"],
        "physical_winner_cell_count": 1,
        "rows": rows,
    }
    roster["roster_sha256"] = canonical_json_sha256(
        {
            "authority_status": roster["authority_status"],
            "current_refit_execution_reverified": True,
            "book_accounting_sha256": accounting["accounting_sha256"],
            "current_refit_lineage": lineage,
            "current_refit_execution_verification": verification,
            "physical_ledger_sha256": roster["physical_ledger_sha256"],
            "rows": rows,
        }
    )
    return roster


def test_run_current_refit_accepts_no_caller_supplied_roster() -> None:
    replay_policy = policy(
        allowed_promotion_statuses=("PROCESS_VALIDATED_CURRENT_REFIT",)
    )
    selected = candidate("selected", status="PROCESS_VALIDATED_CURRENT_REFIT")
    assert (
        "roster"
        not in inspect.signature(
            SynchronizedPortfolioSimulator.run_current_refit
        ).parameters
    )
    with pytest.raises(TypeError):
        SynchronizedPortfolioSimulator(replay_policy).run_current_refit(
            [selected],
            _current_refit_roster(),
            replayed_cell_ids=["EURUSD|H1|TREND"],
        )


def test_direct_candidate_validator_rejects_forged_roster() -> None:
    selected = candidate("selected", status="PROCESS_VALIDATED_CURRENT_REFIT")
    forged = _current_refit_roster()
    errors = validate_current_refit_replay_candidates(
        [selected], forged, replayed_cell_ids=["EURUSD|H1|TREND"]
    )
    assert "CURRENT_REFIT_RAW_EXECUTION_NOT_REVERIFIED" in errors
    assert "CURRENT_REFIT_ROSTER_SEAL_INVALID" in errors


def test_current_refit_successor_recomputes_synchronized_result() -> None:
    roster = _strict_current_refit_roster()
    selected = replace(
        candidate("selected", status="PROCESS_VALIDATED_CURRENT_REFIT"),
        cell_id="EURUSD|H1|TREND",
        source_schema=PORTFOLIO_REPLAY_CANDIDATE_SCHEMA_VERSION,
        margin_model="offline_notional_asset_rate_buffered_v1",
    )
    policies = {
        10_000.0: policy(
            allowed_promotion_statuses=("PROCESS_VALIDATED_CURRENT_REFIT",)
        ).__dict__
    }
    successor, scenarios = build_current_refit_replay_successor(
        [selected],
        roster,
        replayed_cell_ids=["EURUSD|H1|TREND"],
        expected_lineage={
            "native_activation_id": "activation",
            "native_activation_manifest_sha256": "a" * 64,
            "native_activation_roster_sha256": "b" * 64,
            "native_bars_sha256_by_source": {},
            "native_sidecar_sha256_by_source": {},
        },
        expected_cost_contract={
            "use_spread": True,
            "use_commission": True,
            "use_slippage": True,
            "swap_model_enabled": True,
        },
        expected_policies=policies,
    )
    assert successor["candidate_records"][0]["candidate_id"] == "selected"
    assert successor["scenario_reconciliations"][0]["fill_count"] == 1
    assert scenarios[0]["summary"]["accepted_positions"] == 1


def test_replay_module_exposes_no_roster_capability_minter() -> None:
    import pm_portfolio_replay as replay

    assert not hasattr(replay, "_VerifiedCurrentRefitReplayRoster")
    assert not hasattr(replay, "_CURRENT_REFIT_ROSTER_CAPABILITY")


def test_generic_replay_cannot_bypass_current_refit_authority() -> None:
    replay_policy = policy(
        allowed_promotion_statuses=("PROCESS_VALIDATED_CURRENT_REFIT",)
    )
    selected = candidate("selected", status="PROCESS_VALIDATED_CURRENT_REFIT")

    with pytest.raises(ValueError, match="CURRENT_REFIT_EXECUTION_REVERIFIER_REQUIRED"):
        SynchronizedPortfolioSimulator(replay_policy).run([selected])


def test_floor_volume_never_clamps_up_to_minimum():
    assert floor_volume(0.009, 0.01, 100.0) == 0.0
    assert floor_volume(0.019, 0.01, 100.0) == 0.01


def test_shared_account_compounds_in_chronological_order():
    first = candidate("first", net_per_lot=1_000.0)
    second = candidate(
        "second", entry="2025-01-03", exit="2025-01-04", net_per_lot=100.0
    )
    result = SynchronizedPortfolioSimulator(policy()).run([first, second])

    # First position: 1.0 lot -> +1000. Second position sizes from 11,000 -> 1.1 lots.
    assert [trade["volume"] for trade in result["closed_positions"]] == [1.0, 1.1]
    assert result["summary"]["final_balance"] == pytest.approx(11_110.0)
    assert (
        result["summary"]["portfolio_return_aggregation"]
        == "chronological_cashflows_not_summed_cell_returns"
    )
    assert result["summary"]["max_admission_total_stop_risk_pct"] == pytest.approx(1.0)
    assert result["summary"][
        "max_admission_factor_group_stop_risk_pct"
    ] == pytest.approx(1.0)


def test_minimum_lot_is_blocked_instead_of_risk_clamped():
    tiny = candidate(
        "tiny", stop_risk_per_lot=20_000.0, volume_min=0.01, volume_step=0.01
    )
    result = SynchronizedPortfolioSimulator(policy()).run([tiny])
    assert result["summary"]["accepted_positions"] == 0
    assert result["summary"]["blocked_by_reason"] == {"MIN_LOT_EXCEEDS_RISK_BUDGET": 1}


def test_same_specialist_cell_overlap_is_rejected():
    first = candidate("first", exit="2025-01-05")
    overlap = candidate("overlap", entry="2025-01-02", exit="2025-01-03")
    result = SynchronizedPortfolioSimulator(policy()).run([first, overlap])
    assert result["summary"]["accepted_positions"] == 1
    assert result["summary"]["blocked_by_reason"] == {"SPECIALIST_CELL_ALREADY_OPEN": 1}


def test_three_unique_same_symbol_specialists_are_admitted_under_envelope():
    higher = candidate("higher", timeframe="H4", exit="2025-01-06")
    fast = candidate("fast", timeframe="M5", entry="2025-01-02", exit="2025-01-05")
    medium = candidate("medium", timeframe="M15", entry="2025-01-03", exit="2025-01-04")
    result = SynchronizedPortfolioSimulator(
        policy(
            target_risk_per_trade_pct=0.5,
            max_risk_per_trade_pct=0.5,
            max_same_symbol_open_risk_pct=2.0,
        )
    ).run([higher, fast, medium])
    assert result["summary"]["accepted_positions"] == 3
    assert result["summary"]["blocked_by_reason"] == {}
    assert result["summary"]["max_concurrent_positions"] == 3


def test_unique_same_symbol_specialists_obey_same_symbol_risk_cap():
    higher = candidate("higher", timeframe="H4", exit="2025-01-05")
    lower = candidate("lower", timeframe="M5", entry="2025-01-02", exit="2025-01-03")
    result = SynchronizedPortfolioSimulator(
        policy(
            max_same_symbol_open_risk_pct=1.5,
        )
    ).run([higher, lower])
    assert result["summary"]["accepted_positions"] == 1
    assert result["summary"]["blocked_by_reason"] == {"SAME_SYMBOL_OPEN_RISK_CAP": 1}


def test_legacy_d1_lower_flag_cannot_reenable_exact_cell_pyramiding():
    first = candidate("first", timeframe="M5", exit="2025-01-06")
    duplicate = candidate(
        "duplicate", timeframe="M5", entry="2025-01-02", exit="2025-01-05"
    )
    result = SynchronizedPortfolioSimulator(
        policy(
            allow_d1_plus_lower_tf=True,
            max_same_symbol_open_risk_pct=2.0,
        )
    ).run([first, duplicate])
    assert result["summary"]["accepted_positions"] == 1
    assert result["summary"]["blocked_by_reason"] == {"SPECIALIST_CELL_ALREADY_OPEN": 1}


def test_specialist_cell_guard_cannot_be_bypassed_by_relabelling_slot():
    first = candidate("first", timeframe="M5", exit="2025-01-06")
    forged = replace(
        candidate("forged", timeframe="M5", entry="2025-01-02", exit="2025-01-05"),
        cell_id="forged-cell-label",
    )
    with pytest.raises(
        ValueError, match="specialist slot maps to multiple candidate cell_id values"
    ):
        SynchronizedPortfolioSimulator(policy()).run([first, forged])


def test_candidate_cell_cannot_be_rebound_to_another_specialist_identity():
    first = candidate("first", timeframe="M5", exit="2025-01-06")
    rebound = replace(
        candidate("rebound", timeframe="M15", entry="2025-01-02", exit="2025-01-05"),
        cell_id=first.cell_id,
    )
    with pytest.raises(
        ValueError, match="candidate cell_id maps to multiple specialist identities"
    ):
        SynchronizedPortfolioSimulator(policy()).run([first, rebound])


def test_funded_policy_has_no_hidden_six_specialist_ceiling():
    symbols = [f"SPECIALIST{i}" for i in range(8)]
    funded_policy = portfolio_policy_from_config_payload(
        {
            "symbols": symbols,
            "pipeline": {
                "max_total_open_risk_pct": 20.0,
                "max_new_entries_when_total_open_risk_above_pct": 20.0,
                "max_combined_risk_pct": 10.0,
                "production_symbol_pool_target_count": 8,
                "allow_d1_plus_lower_tf": False,
                "margin_reopen_level": 300.0,
                "live_allowed_promotion_statuses": ["PROMOTED"],
            },
            "position": {
                "risk_per_trade_pct": 0.25,
                "max_risk_pct": 0.50,
            },
        },
        starting_balance=10_000.0,
    )
    assert funded_policy.max_concurrent_positions == 8 * 6 * 4

    specialists = [
        candidate(
            f"specialist-{idx}",
            symbol=symbol,
            asset_class=f"independent-class-{idx}",
            exit="2025-01-03",
        )
        for idx, symbol in enumerate(symbols)
    ]
    result = SynchronizedPortfolioSimulator(funded_policy).run(specialists)
    assert result["summary"]["accepted_positions"] == 8
    assert "MAX_CONCURRENT_POSITIONS" not in result["summary"]["blocked_by_reason"]
    assert result["summary"]["max_open_risk_pct"] <= funded_policy.max_open_risk_pct


def test_structural_ceiling_is_one_slot_per_symbol_timeframe_regime():
    payload = {
        "symbols": ["EURUSD", "GBPUSD", "USDJPY"],
        "pipeline": {
            "max_total_open_risk_pct": 3.0,
            "max_new_entries_when_total_open_risk_above_pct": 2.0,
            "max_combined_risk_pct": 1.0,
            "production_symbol_pool_target_count": 3,
            "allow_d1_plus_lower_tf": True,
        },
        "position": {"risk_per_trade_pct": 0.25, "max_risk_pct": 0.50},
    }
    funded_policy = portfolio_policy_from_config_payload(payload, 10_000.0)
    assert funded_policy.max_concurrent_positions == 3 * 6 * 4


def test_production_structural_ceiling_covers_exact_100_by_6_by_4_roster():
    symbols = [f"SPECIALIST{i:03d}" for i in range(100)]
    funded_policy = portfolio_policy_from_config_payload(
        {
            "symbols": symbols,
            "pipeline": {
                "max_total_open_risk_pct": 90.0,
                "max_new_entries_when_total_open_risk_above_pct": 90.0,
                "max_combined_risk_pct": 90.0,
                "max_factor_group_open_risk_pct": 90.0,
                "production_symbol_pool_target_count": 100,
                "allow_d1_plus_lower_tf": False,
            },
            "position": {"risk_per_trade_pct": 0.25, "max_risk_pct": 0.50},
        },
        10_000.0,
    )
    assert funded_policy.max_concurrent_positions == 2_400


@pytest.mark.parametrize(
    ("action", "threshold", "expected_cap"),
    (
        ("observe", 2.0, 8.0),
        ("pause_new_entries", 2.0, 2.0),
        ("pause_new_entries", 0.0, 8.0),
    ),
)
def test_replay_open_risk_threshold_semantics_match_live_action(
    action, threshold, expected_cap
):
    payload = {
        "symbols": ["EURUSD"],
        "pipeline": {
            "max_total_open_risk_pct": 8.0,
            "max_factor_group_open_risk_pct": 2.0,
            "max_new_entries_when_total_open_risk_above_pct": threshold,
            "open_risk_action": action,
            "max_combined_risk_pct": 1.0,
            "margin_entry_block_level": 200.0,
        },
        "position": {"risk_per_trade_pct": 0.25, "max_risk_pct": 0.50},
    }
    funded_policy = portfolio_policy_from_config_payload(payload, 10_000.0)
    assert funded_policy.max_open_risk_pct == expected_cap
    assert funded_policy.max_margin_utilization_pct == 50.0
    assert funded_policy.min_margin_level_pct == 200.0
    assert funded_policy.min_free_margin_pct == 50.0


def test_replay_rejects_unmodeled_derate_policy():
    payload = {
        "symbols": ["EURUSD"],
        "pipeline": {
            "max_total_open_risk_pct": 8.0,
            "max_factor_group_open_risk_pct": 2.0,
            "max_new_entries_when_total_open_risk_above_pct": 6.0,
            "open_risk_action": "derate",
            "max_combined_risk_pct": 1.0,
            "margin_entry_block_level": 200.0,
        },
        "position": {"risk_per_trade_pct": 0.25, "max_risk_pct": 0.50},
    }
    with pytest.raises(ValueError, match="does not support.*derate"):
        portfolio_policy_from_config_payload(payload, 10_000.0)


def test_replay_margin_limits_derive_from_one_entry_block_authority():
    payload = {
        "symbols": ["EURUSD"],
        "pipeline": {
            "max_total_open_risk_pct": 8.0,
            "max_factor_group_open_risk_pct": 2.0,
            "max_new_entries_when_total_open_risk_above_pct": 0.0,
            "open_risk_action": "observe",
            "max_combined_risk_pct": 1.0,
            "margin_entry_block_level": 250.0,
        },
        "position": {"risk_per_trade_pct": 0.25, "max_risk_pct": 0.50},
    }
    funded_policy = portfolio_policy_from_config_payload(payload, 10_000.0)
    assert funded_policy.max_margin_utilization_pct == 40.0
    assert funded_policy.min_margin_level_pct == 250.0
    assert funded_policy.min_free_margin_pct == 60.0


def test_exit_is_processed_before_new_entry_at_same_timestamp():
    first = candidate("first", exit="2025-01-02", symbol="EURUSD")
    replacement = candidate(
        "replacement", entry="2025-01-02", exit="2025-01-03", symbol="EURUSD"
    )
    result = SynchronizedPortfolioSimulator(policy()).run([first, replacement])
    assert result["summary"]["accepted_positions"] == 2
    assert "SYMBOL_ALREADY_OPEN" not in result["summary"]["blocked_by_reason"]


def test_partial_exit_releases_risk_before_same_time_admission():
    partial = candidate(
        "partial",
        exit="2025-01-03",
        legs=(
            ExitLeg("2025-01-02", 0.5, 0.0),
            ExitLeg("2025-01-03", 0.5, 0.0),
        ),
    )
    second = candidate("second", symbol="GBPUSD", entry="2025-01-02", exit="2025-01-04")
    result = SynchronizedPortfolioSimulator(
        policy(max_open_risk_pct=1.5, max_factor_group_open_risk_pct=1.5)
    ).run([partial, second])
    assert result["summary"]["accepted_positions"] == 2
    assert result["summary"]["max_open_risk_pct"] <= 1.5 + 1e-8


def test_group_concentration_uses_dollar_risk_not_position_count():
    first = candidate(
        "first", symbol="EURUSD", exit="2025-01-04", groups=("CURRENCY:USD",)
    )
    second = candidate(
        "second",
        symbol="GBPUSD",
        entry="2025-01-02",
        exit="2025-01-03",
        groups=("CURRENCY:USD",),
    )
    result = SynchronizedPortfolioSimulator(
        policy(max_factor_group_open_risk_pct=1.5)
    ).run([first, second])
    assert result["summary"]["blocked_by_reason"]["FACTOR_GROUP_STOP_RISK_CAP"] == 1


def test_signed_correlation_cluster_blocks_only_compounding_direction():
    def lookup(left, right, stamp):
        return 0.90

    first = candidate("first", symbol="EURUSD", exit="2025-01-04", direction=1)
    same_direction = candidate(
        "same", symbol="GBPUSD", entry="2025-01-02", exit="2025-01-03", direction=1
    )
    blocked = SynchronizedPortfolioSimulator(
        policy(max_correlated_cluster_risk_pct=1.5), lookup
    ).run([first, same_direction])
    assert blocked["summary"]["blocked_by_reason"]["CORRELATED_CLUSTER_RISK_CAP"] == 1

    opposite_direction = replace(same_direction, candidate_id="opposite", direction=-1)
    admitted = SynchronizedPortfolioSimulator(
        policy(max_correlated_cluster_risk_pct=1.5), lookup
    ).run([first, opposite_direction])
    assert admitted["summary"]["accepted_positions"] == 2


def test_missing_margin_fails_closed():
    missing = candidate("missing", margin_per_lot=float("nan"))
    with pytest.raises(ValueError, match="candidate margin per lot"):
        SynchronizedPortfolioSimulator(policy()).run([missing])


def test_margin_utilization_cap_is_enforced():
    expensive = candidate("expensive", margin_per_lot=5_000.0)
    result = SynchronizedPortfolioSimulator(
        policy(max_margin_utilization_pct=20.0)
    ).run([expensive])
    assert result["summary"]["blocked_by_reason"] == {
        "PROJECTED_MARGIN_UTILIZATION_CAP": 1
    }


def test_mark_to_market_drawdown_is_not_hidden_by_realized_balance():
    marked = candidate(
        "marked",
        exit="2025-01-03",
        net_per_lot=0.0,
        marks=(MarkPoint("2025-01-02", -1_000.0),),
    )
    result = SynchronizedPortfolioSimulator(policy()).run([marked])
    assert result["summary"]["max_realized_balance_drawdown_pct"] == 0.0
    assert result["summary"]["max_mark_to_market_drawdown_pct"] == pytest.approx(10.0)
    assert result["summary"]["mtm_candidate_coverage_pct"] == 100.0


def test_common_m5_mark_is_applied_before_same_time_admission():
    first_with_m5 = candidate(
        "first-m5",
        symbol="EURUSD",
        exit="2025-01-04",
        marks=(
            MarkPoint(
                "2025-01-02",
                -6_000.0,
                observation_time="2025-01-02",
            ),
        ),
    )
    first_with_stale_h4 = replace(
        first_with_m5,
        candidate_id="first-h4-stale",
        marks=(),
    )
    second = candidate(
        "second",
        symbol="XAUUSD",
        asset_class="metal",
        entry="2025-01-02",
        exit="2025-01-03",
    )
    replay_policy = policy(
        max_open_risk_pct=2.1,
        max_factor_group_open_risk_pct=2.1,
        max_same_symbol_open_risk_pct=2.1,
        enforce_pair_correlation=False,
    )

    stale = SynchronizedPortfolioSimulator(replay_policy).run(
        [first_with_stale_h4, second]
    )
    causal = SynchronizedPortfolioSimulator(replay_policy).run([first_with_m5, second])

    assert stale["summary"]["accepted_positions"] == 2
    assert causal["summary"]["accepted_positions"] == 1
    assert causal["summary"]["blocked_by_reason"] == {"EXISTING_TOTAL_STOP_RISK_CAP": 1}
    assert causal["summary"]["valuation_event_order"] == (
        "close_existing_then_mark_existing_then_admit"
    )


def test_future_observation_cannot_be_backstamped_as_causal_mark():
    with pytest.raises(ValueError, match="observation time cannot be after"):
        candidate(
            "future-mark",
            marks=(
                MarkPoint(
                    "2025-01-02T10:00:00",
                    -100.0,
                    observation_time="2025-01-02T10:05:00",
                ),
            ),
        ).normalized()


def test_promotion_status_fails_closed_by_live_allowlist():
    paper = candidate("paper", status="PROMOTE_SMALL")
    result = SynchronizedPortfolioSimulator(policy()).run([paper])
    assert result["summary"]["blocked_by_reason"] == {"PROMOTION_STATUS_NOT_ALLOWED": 1}


def test_trailing_correlation_never_uses_the_asof_day_or_future():
    index = pd.date_range("2025-01-01", periods=50, freq="D")
    left = pd.Series(range(50), index=index, dtype=float).pct_change().fillna(0.0)
    right = left.copy()
    # Reverse only observations on/after the cutoff; point-in-time correlation must remain positive.
    right.loc[right.index >= pd.Timestamp("2025-02-10")] *= -1.0
    book = TrailingCorrelationBook(
        {"EURUSD": left, "GBPUSD": right}, lookback=30, min_observations=20
    )
    assert book("EURUSD", "GBPUSD", pd.Timestamp("2025-02-10")) > 0.99


def test_bootstrap_ruin_is_seeded_and_labeled_exploratory():
    returns = [0.01, -0.02, 0.015, -0.01] * 10
    first = bootstrap_ruin_probability(
        returns, starting_balance=10_000.0, simulations=100, seed=7
    )
    second = bootstrap_ruin_probability(
        returns, starting_balance=10_000.0, simulations=100, seed=7
    )
    assert first == second
    assert first["status"] == "exploratory_conditional_sequence_stress"


def test_bootstrap_ruin_rejects_nonfinite_daily_returns():
    result = bootstrap_ruin_probability(
        [0.01, float("nan"), -0.02],
        starting_balance=10_000.0,
        simulations=100,
        seed=7,
    )
    assert result["status"] == "invalid_daily_returns"
    assert result["invalid_daily_return_observations"] == 1
    assert result["simulations"] == 0


def test_candidate_normalization_rejects_nonfinite_exit_and_mark_evidence():
    with pytest.raises(ValueError, match="exit leg net P&L"):
        candidate(
            "bad-exit",
            legs=(ExitLeg("2025-01-02", 1.0, float("nan")),),
        ).normalized()
    with pytest.raises(ValueError, match="mark P&L"):
        candidate(
            "bad-mark",
            marks=(MarkPoint("2025-01-01T12:00:00", float("inf")),),
        ).normalized()


def test_evaluation_frame_uses_exclusive_end_boundary():
    index = pd.date_range("2025-01-01", periods=4, freq="h")
    frame = pd.DataFrame({"Close": [1.0, 2.0, 3.0, 4.0]}, index=index)
    bounded = _half_open_evaluation_frame(frame, index[2])
    assert list(bounded.index) == [index[0], index[1]]
    assert index[2] not in bounded.index


def test_candidate_generation_rejects_nonfinite_source_trade_evidence(
    monkeypatch: pytest.MonkeyPatch,
):
    from agent_audit.performance.scripts import (
        run_synchronized_portfolio_replay as replay_cli,
    )

    index = pd.date_range("2025-01-01", periods=220, freq="h")
    frame = pd.DataFrame(
        {
            "Open": 1.0,
            "High": 1.1,
            "Low": 0.9,
            "Close": 1.0,
            "Volume": 1.0,
            "REGIME_LIVE": "TREND",
        },
        index=index,
    )
    signals = pd.Series(0, index=index, dtype=int)
    signals.iloc[10] = 1
    strategy = SimpleNamespace(generate_signals=lambda *_args: signals)
    fake_backtester = SimpleNamespace(
        run=lambda *_args, **_kwargs: {
            "trades": [
                {
                    "entry_bar": 10,
                    "signal_bar": 9,
                    "entry_time": index[10],
                    "exit_time": index[20],
                    "direction": "LONG",
                    "entry_price": 1.0,
                    "exit_price": 1.1,
                    "position_size": 1.0,
                    "entry_to_stop_pips": 10.0,
                    "pnl_dollars": float("nan"),
                    "swap_dollars": 0.0,
                }
            ]
        },
        _build_half_spread_arrays=lambda *_args: (
            np.zeros(len(frame)),
            np.zeros(len(frame)),
            {},
        ),
    )
    monkeypatch.setattr(
        replay_cli.FeatureComputer, "compute_all", lambda *_args, **_kwargs: frame
    )
    monkeypatch.setattr(
        replay_cli.StrategyRegistry, "get", lambda *_args, **_kwargs: strategy
    )
    monkeypatch.setattr(replay_cli, "Backtester", lambda _cfg: fake_backtester)
    cfg = PipelineConfig()
    cell = {
        "symbol": "EURUSD",
        "timeframe": "H1",
        "regime": "TREND",
        "strategy_name": "Test",
        "parameters": {},
        "promotion_status": "PROMOTED",
    }
    candidates, report = replay_cell_candidates(
        cfg,
        cell,
        frame,
        SimpleNamespace(),
        "EURUSD",
        evaluation_start=index[0],
        evaluation_end=index[-1] + pd.Timedelta(hours=1),
        apply_governance=False,
        include_marks=False,
    )
    assert candidates == []
    assert "group_0:nonfinite_trade_evidence" in report["group_errors"]


def test_mtm_marks_accrue_swap_causally_with_broker_triple_day():
    index = pd.date_range("2026-04-06", periods=4, freq="D")  # Monday-Thursday
    features = pd.DataFrame({"Close": [100.0] * 4}, index=index)
    cfg = PipelineConfig(
        swap_model_enabled=True,
        swap_model_min_holding_hours=0,
        swap_model_triple_rollover_day="friday",  # broker spec must win
    )
    backtester = Backtester(cfg)
    spec = InstrumentSpec(
        "TEST",
        pip_position=2,
        commission_per_lot=0.0,
        swap_long=-5.0,
        swap_short=1.0,
        swap_mode=-1,
        swap_rollover3days=3,  # MT5 Wednesday
    )
    spec.calculate_tick_profit = lambda *_args: 0.0

    marks = _marks_for_position(
        features,
        np.zeros(len(features)),
        spec,
        index[0],
        index[-1],
        100.0,
        1,
        0.0,
        backtester,
        "D1",
    )

    # MetaQuotes defines SYMBOL_SWAP_WEDNESDAY as the multiplier for a
    # position rolled FROM Wednesday TO Thursday.  The triple therefore
    # appears at Thursday's boundary, not Wednesday's.
    assert [mark.pnl_per_current_lot for mark in marks] == [0.0, -5.0, -10.0, -25.0]


def test_m5_valuation_uses_observable_open_not_future_close():
    index = pd.date_range("2026-04-06", periods=2, freq="5min")
    valuation = pd.DataFrame(
        {
            "Open": [100.0, 90.0],
            # The second close is not observable at its bar-open timestamp and
            # must never enter the decision-time mark.
            "Close": [100.0, 1_000.0],
        },
        index=index,
    )
    backtester = Backtester(PipelineConfig(swap_model_enabled=False))
    spec = InstrumentSpec("TEST", pip_position=2, commission_per_lot=0.0)
    spec.calculate_tick_profit = lambda entry, exit_price, volume, direction: (
        (exit_price - entry) * volume * direction
    )

    marks = _marks_for_position(
        valuation,
        np.zeros(len(valuation)),
        spec,
        index[0],
        index[-1],
        100.0,
        1,
        0.0,
        backtester,
        "M5",
        valuation_price_field="Open",
    )

    assert [mark.pnl_per_current_lot for mark in marks] == [0.0, -10.0]
    assert all(mark.observation_time == mark.time for mark in marks)


def test_swap_interest_open_mode_uses_open_notional_and_360_day_year():
    spec = InstrumentSpec(
        "APPLE",
        pip_position=2,
        contract_size=100.0,
        swap_long=-6.85,
        swap_mode=6,
    )
    daily = Backtester._swap_value_per_lot_dollars(
        spec, spec.swap_long, entry_price=200.0
    )
    assert daily == pytest.approx(200.0 * 100.0 * (-6.85 / 100.0) / 360.0)


def test_nonzero_unsupported_swap_mode_fails_closed():
    spec = InstrumentSpec(
        "UNKNOWN",
        pip_position=2,
        swap_long=-1.0,
        swap_mode=99,
    )
    with pytest.raises(ValueError, match="unsupported nonzero MT5 swap mode"):
        Backtester._swap_value_per_lot_dollars(spec, spec.swap_long, entry_price=100.0)


def test_swap_rollovers_skip_weekends_and_honor_broker_friday_triple():
    backtester = Backtester(
        PipelineConfig(swap_model_enabled=True, swap_model_min_holding_hours=0)
    )
    spec = InstrumentSpec(
        "INDEX",
        pip_position=2,
        swap_rollover3days=5,  # MT5 Friday
    )
    units = backtester._swap_rollover_units(
        {
            "entry_time": pd.Timestamp("2026-04-06"),  # Monday
            "exit_time": pd.Timestamp("2026-04-13"),  # next Monday
        },
        None,
        timeframe="D1",
        spec=spec,
    )
    # Mon-Thu singles=4, Friday triple=3, weekend=0.
    assert units == 7
