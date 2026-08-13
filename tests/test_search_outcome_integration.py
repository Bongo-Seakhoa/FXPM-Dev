import copy
import hashlib
from types import SimpleNamespace
from unittest.mock import patch

import numpy as np
import pandas as pd
import pytest

from pm_core import PipelineConfig
from pm_optuna import _bounded_cartesian_combinations as _optuna_bounded_cartesian
from pm_pipeline import (
    RegimeOptimizer,
    _bounded_cartesian_combinations as _pipeline_bounded_cartesian,
    _signal_vector_sha256,
)
from pm_search_outcomes import (
    InvalidStrategyParameterError,
    SearchInfrastructureError,
    SearchOutcomeKind,
    SearchStage,
)
from pm_trade_power import (
    REQUIRED_TRADE_RECORD_SCHEMA,
    TRADE_POWER_DECISION_SCHEMA_VERSION,
    TradePowerDecision,
    build_numerical_execution_cost_attestation,
)


class _Strategy:
    name = "IntegrationFamily"

    def get_params(self):
        return {}


def _authenticated_result(*, observations: int, seed: int, start_utc: str):
    rng = np.random.default_rng(seed)
    values = rng.normal(loc=0.30, scale=0.70, size=observations)
    start = pd.Timestamp(start_utc, tz="UTC")
    trades = []
    for index, value in enumerate(values):
        entry = start + pd.Timedelta(hours=3 * index)
        trades.append({
            "position_id": f"fixture-{start.value}-{index:04d}",
            "entry_event_order": index * 2,
            "exit_event_order": index * 2 + 1,
            "pnl_dollars": float(value * 100.0),
            "risk_amount": 100.0,
            "entry_time": entry,
            "exit_time": entry + pd.Timedelta(hours=1),
            "trade_record_schema_version": REQUIRED_TRADE_RECORD_SCHEMA,
        })
    end = start + pd.Timedelta(hours=max(3 * observations, 3))
    return {
        "total_trades": len(trades),
        "trades": trades,
        "trade_power_partition_axis": {
            "start_utc": start.isoformat(),
            "end_utc": end.isoformat(),
            "horizon_receipt_sha256": hashlib.sha256(
                f"search-outcome:horizon:{start.isoformat()}:{end.isoformat()}".encode(
                    "ascii"
                )
            ).hexdigest(),
            "event_axis_sha256": hashlib.sha256(
                "|".join(
                    f"{trade['position_id']}:{trade['entry_time']}:{trade['exit_time']}"
                    for trade in trades
                ).encode("ascii")
            ).hexdigest(),
        },
        "execution_cost_timing_contract": {
            "entry_slippage": "entry_slippage_parity_v1",
            "swap_accrual": "causal_swap_accrual_v1",
        },
        "numerical_execution_cost_attestation": (
            build_numerical_execution_cost_attestation(
                use_spread=True,
                use_slippage=True,
                use_commission=True,
                spread_mode="conservative_max",
                spread_values_sha256="7" * 64,
                commission_inputs_sha256="8" * 64,
                instrument_spec_sha256="9" * 64,
                slippage_pips=0.5,
                slippage_price=0.00005,
            )
        ),
        "swap_accrual_contract": {
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
            "projection_sha256": hashlib.sha256(
                f"search-outcome:{start.isoformat()}".encode("ascii")
            ).hexdigest(),
        },
    }


def _persisted_power_pair(*, train_observations: int, val_observations: int, seed: int):
    optimizer = RegimeOptimizer.__new__(RegimeOptimizer)
    optimizer._trade_power_multiplicity_cache = {}
    train = optimizer._calibrate_train_power_result(
        _authenticated_result(
            observations=train_observations,
            seed=seed,
            start_utc="2020-01-01",
        ),
        effective_trials=1,
        symbol="EURUSD",
        timeframe="H1",
        regime="TREND",
        strategy_family="Fixture",
        parameter_hash="c" * 64,
    )
    assert train.passed
    validation = optimizer._evaluate_partition_power_result(
        _authenticated_result(
            observations=val_observations,
            seed=seed + 1,
            start_utc="2022-01-01",
        ),
        frozen_requirement=train.frozen_requirement,
        symbol="EURUSD",
        timeframe="H1",
        regime="TREND",
        strategy_family="Fixture",
        parameter_hash="c" * 64,
    )
    return copy.deepcopy(train.to_dict()), copy.deepcopy(validation.to_dict())


def _underpowered_decision() -> TradePowerDecision:
    return TradePowerDecision(
        status="EXPECTED_INELIGIBLE",
        outcome_kind=SearchOutcomeKind.EXPECTED_INELIGIBLE,
        partition="optimizer_train",
        reason="effective train trades are underpowered",
        reason_codes=("TRAIN_EFFECTIVE_TRADES_UNDERPOWERED",),
        observed_effective_trades=3.0,
        required_effective_trades=100,
        sample_evidence=None,
        frozen_requirement=None,
    )


def _passing_decision(*, effective_trades: float = 100.0) -> TradePowerDecision:
    return TradePowerDecision(
        status="PASS",
        outcome_kind=SearchOutcomeKind.VALID,
        partition="optimizer_train",
        reason="effective train trades passed",
        reason_codes=(),
        observed_effective_trades=effective_trades,
        required_effective_trades=10,
        sample_evidence=None,
        frozen_requirement=None,
    )


def _probe_optimizer() -> RegimeOptimizer:
    optimizer = RegimeOptimizer.__new__(RegimeOptimizer)
    optimizer.REGIMES = ("TREND",)
    optimizer.family_probe_trials = 1
    optimizer._family_probe_param_specs = lambda *_args, **_kwargs: [({}, None)]
    return optimizer


def test_unexpected_b0_failure_aborts_instead_of_selecting_an_inferior_family():
    optimizer = _probe_optimizer()

    def fail_run(**_kwargs):
        raise RuntimeError("synthetic backtest infrastructure failure")

    optimizer._run_one_exact_regime = fail_run
    with patch("pm_pipeline.StrategyRegistry.get", return_value=_Strategy()):
        with pytest.raises(SearchInfrastructureError) as raised:
            optimizer._collect_family_probe_candidates(
                symbol="EURUSD",
                timeframe="H1",
                train_features=SimpleNamespace(),
                strategies=[_Strategy()],
            )

    assert raised.value.outcome.kind is SearchOutcomeKind.INFRA_FAILURE
    assert raised.value.outcome.reason_code == "UNEXPECTED_SEARCH_EXCEPTION"
    assert raised.value.outcome.stage is SearchStage.TRAIN_BACKTEST


@pytest.mark.parametrize(
    ("failure_site", "expected_stage", "exception"),
    [
        ("signal", SearchStage.SIGNAL_GENERATION, RuntimeError("signal failed")),
        ("signal", SearchStage.SIGNAL_GENERATION, InvalidStrategyParameterError("late invalid")),
        ("backtest", SearchStage.TRAIN_BACKTEST, RuntimeError("backtest failed")),
    ],
)
def test_exact_regime_runtime_failures_preserve_stage_and_never_hide_late_invalid_params(
    failure_site,
    expected_stage,
    exception,
):
    class Strategy:
        name = "StageStrategy"

        def get_params(self):
            return {"p": 1}

        def generate_signals(self, features, _symbol):
            if failure_site == "signal":
                raise exception
            return pd.Series(1.0, index=features.index)

    class Backtester:
        def run(self, *_args, **_kwargs):
            raise exception

    optimizer = RegimeOptimizer.__new__(RegimeOptimizer)
    optimizer.backtester = Backtester()
    features = pd.DataFrame(
        {"Close": np.arange(60.0), "REGIME_LIVE": ["TREND"] * 60},
        index=pd.date_range("2026-01-01", periods=60, freq="h", tz="UTC"),
    )

    with pytest.raises(SearchInfrastructureError) as raised:
        optimizer._run_one_exact_regime(
            symbol="EURUSD",
            timeframe="H1",
            features=features,
            strategy=Strategy(),
            regime="TREND",
            partition="optimizer_train",
        )

    assert raised.value.outcome.kind is SearchOutcomeKind.INFRA_FAILURE
    assert raised.value.outcome.stage is expected_stage
    if isinstance(exception, InvalidStrategyParameterError):
        assert (
            raised.value.outcome.reason_code
            == "INVALID_PARAMETER_EXCEPTION_OUTSIDE_PARAMETER_STAGE"
        )


def test_malformed_signal_values_fail_as_typed_signal_generation():
    class StringSignalStrategy:
        name = "StringSignalStrategy"

        def get_params(self):
            return {}

        def generate_signals(self, features, _symbol):
            return pd.Series("not-numeric", index=features.index)

    optimizer = RegimeOptimizer.__new__(RegimeOptimizer)
    optimizer.backtester = SimpleNamespace(run=lambda *_args, **_kwargs: {})
    features = pd.DataFrame(
        {"Close": np.arange(60.0), "REGIME_LIVE": ["TREND"] * 60},
        index=pd.date_range("2026-01-01", periods=60, freq="h", tz="UTC"),
    )

    with pytest.raises(SearchInfrastructureError) as raised:
        optimizer._run_one_exact_regime(
            symbol="EURUSD",
            timeframe="H1",
            features=features,
            strategy=StringSignalStrategy(),
            regime="TREND",
            partition="optimizer_train",
        )

    assert raised.value.outcome.kind is SearchOutcomeKind.INFRA_FAILURE
    assert raised.value.outcome.stage is SearchStage.SIGNAL_GENERATION


@pytest.mark.parametrize(
    "builder",
    [_pipeline_bounded_cartesian, _optuna_bounded_cartesian],
)
def test_cartesian_sampling_is_bounded_before_large_domain_materialization(builder):
    combinations, total = builder(
        [range(100), range(100), range(100), range(100)],
        limit=7,
        seed=12345,
    )
    assert total == 100_000_000
    assert len(combinations) == 7
    assert len(set(combinations)) == 7
    assert combinations == builder(
        [range(100), range(100), range(100), range(100)],
        limit=7,
        seed=12345,
    )[0]


def test_deep_search_supersedes_identical_b0_identity_and_validates_once():
    class Strategy:
        name = "Family"

        def get_params(self):
            return {"p": 1}

        def get_param_grid(self):
            return {"p": [1]}

    optimizer = RegimeOptimizer.__new__(RegimeOptimizer)
    optimizer.REGIMES = ("TREND",)
    optimizer.enable_hyperparam_tuning = True
    optimizer.hyperparam_top_k = 1
    optimizer._last_tuning_search_meta = {}
    optimizer._last_family_probe_cells = []
    optimizer._last_search_cell_outcomes = []
    b0 = {
        "strategy": Strategy(),
        "strategy_name": "Family",
        "params": {"p": 1},
        "evaluation_regime": "TREND",
        "candidate_id": optimizer._exact_cell_candidate_id(
            "EURUSD",
            "H1",
            "TREND",
            {"strategy_name": "Family", "params": {"p": 1}},
        ),
    }
    probe_cells = [{
        "strategy_family": "Family",
        "regime": "TREND",
        "search_trials": 3,
        "tuning_validation_evaluations": 0,
    }]
    validation_events = []
    optimizer._collect_family_probe_candidates = lambda **_kwargs: ([b0], probe_cells)
    optimizer._apply_training_eligibility_gates = lambda rows, *_args: rows
    optimizer._identify_top_strategies_per_regime = lambda *_args, **_kwargs: {
        "TREND": ["Family"]
    }

    def tune(*_args, **kwargs):
        assert kwargs["prior_effective_trials_by_regime"] == {"TREND": 3}
        validation_events.append("deep")
        optimizer._last_tuning_search_meta = {
            "cell_count": 1,
            "search_trials_total": 1,
            "tuning_validation_evaluations": 1,
            "cells": [{
                "strategy_family": "Family",
                "regime": "TREND",
                "search_trials": 1,
                "tuning_validation_evaluations": 1,
            }],
        }
        return [{
            **b0,
            "is_tuned": True,
            "tuning_validation_evaluations": 1,
        }]

    optimizer._tune_strategy_params = tune

    def validate_probe(**kwargs):
        validation_events.extend(
            f"probe:{candidate['candidate_id']}" for candidate in kwargs["candidates"]
        )

    optimizer._attach_frozen_probe_validation = validate_probe
    optimizer._attach_local_search_multiplicity = lambda *_args, **_kwargs: None
    optimizer._attach_oos_evidence_to_candidates = lambda *_args, **_kwargs: None
    result = optimizer._collect_candidates(
        "EURUSD",
        "H1",
        pd.DataFrame(),
        pd.DataFrame(),
        [Strategy()],
    )

    assert validation_events == ["deep"]
    assert len(result) == 1
    assert result[0]["is_tuned"] is True


def test_governed_oos_reuse_rejects_empty_record():
    optimizer = RegimeOptimizer(PipelineConfig(
        research_mode=True,
        oos_evidence_export_enabled=True,
        oos_evidence_include_execution_cost_survival=False,
    ))
    features = pd.DataFrame(
        {"Close": np.arange(20.0), "REGIME_LIVE": ["TREND"] * 20},
        index=pd.date_range("2026-01-01", periods=20, freq="h", tz="UTC"),
    )
    candidate = {
        "strategy_name": "Fixture",
        "params": {},
        "evaluation_regime": "TREND",
        "trade_power_contract_enforced": True,
        "oos_evidence": {},
    }

    with pytest.raises(SearchInfrastructureError) as raised:
        optimizer._attach_oos_evidence_to_candidates(
            "EURUSD", "H1", features, 0, [candidate]
        )

    assert (
        raised.value.outcome.reason_code
        == "FROZEN_VALIDATION_REPLAY_EVIDENCE_INVALID"
    )


def test_train_only_score_frequency_bonus_uses_effective_not_raw_trade_count():
    optimizer = RegimeOptimizer(PipelineConfig(
        research_mode=True,
        scoring_use_trade_frequency_bonus=True,
        regime_min_train_trades=1,
    ))
    base = {
        "effective_trades": 20.0,
        "win_rate": 55.0,
        "profit_factor": 1.4,
        "total_return_pct": 8.0,
        "max_drawdown_pct": 4.0,
        "sharpe_approx": 1.0,
        "avg_win_pips": 2.0,
        "avg_loss_pips": 1.0,
    }

    low_raw = optimizer._compute_train_regime_score({**base, "total_trades": 2})
    high_raw = optimizer._compute_train_regime_score({**base, "total_trades": 2000})

    assert low_raw == pytest.approx(high_raw, rel=0.0, abs=1e-12)


def test_governed_winner_robustness_is_invariant_to_raw_trade_count():
    optimizer = RegimeOptimizer(PipelineConfig(
        research_mode=True,
        scoring_use_trade_frequency_bonus=True,
        regime_min_val_trades=1,
        regime_min_val_profit_factor=0.0,
        regime_min_val_return_pct=-100.0,
        regime_min_val_return_dd_ratio=0.0,
        fx_val_max_drawdown=100.0,
    ))
    base = {
        "effective_trades": 20.0,
        "win_rate": 55.0,
        "profit_factor": 1.5,
        "total_return_pct": 10.0,
        "max_drawdown_pct": 2.0,
        "sharpe_approx": 1.0,
        "avg_win_pips": 2.0,
        "avg_loss_pips": 1.0,
    }
    validation = {**base, "total_trades": 2}

    low = optimizer._validate_regime_winner(
        {**base, "total_trades": 2},
        validation,
        "TREND",
        trade_power_enforced=True,
    )
    high = optimizer._validate_regime_winner(
        {**base, "total_trades": 2000},
        validation,
        "TREND",
        trade_power_enforced=True,
    )

    assert high == low


def test_oos_reuse_refreshes_final_multiplicity_without_baseline_replay():
    optimizer = RegimeOptimizer(PipelineConfig(
        research_mode=True,
        oos_evidence_export_enabled=True,
        oos_evidence_periods=8,
        oos_evidence_min_periods=2,
        oos_evidence_include_execution_cost_survival=False,
    ))
    features = pd.DataFrame(
        {"Close": np.arange(20.0), "REGIME_LIVE": ["TREND"] * 20},
        index=pd.date_range("2026-01-01", periods=20, freq="h", tz="UTC"),
    )
    signals = pd.Series(1.0, index=features.index)
    val_result = {
        "equity_curve": np.linspace(100.0, 110.0, len(features)).tolist(),
        "total_trades": 10,
        "profit_factor": 1.4,
        "total_return_pct": 10.0,
        "max_drawdown_pct": 2.0,
        "win_rate": 50.0,
        "trades": [],
        "_baseline_gated_signals_sha256": _signal_vector_sha256(signals),
    }
    candidate = {
        "strategy_name": "Fixture",
        "params": {"p": 1},
        "evaluation_regime": "TREND",
        "trade_power_contract_enforced": True,
        "_baseline_gated_signals_sha256": val_result[
            "_baseline_gated_signals_sha256"
        ],
        "search_trials": 1,
        "local_search_trials_total": 0,
        "local_search_cells_total": 0,
        "family_probe_search_trials_total": 0,
        "family_probe_search_cells_total": 0,
        "tuning_search_trials_total": 0,
        "tuning_search_cells_total": 0,
        "strategy_roster_count": 0,
        "train_result": {},
        "train_regime_metrics": {},
        "val_regime_metrics": {"TREND": dict(val_result)},
    }
    candidate["oos_evidence"] = optimizer._build_oos_evidence_record(
        "EURUSD", "H1", candidate, val_result, features, 0
    )
    candidate.update({
        "local_search_trials_total": 77,
        "local_search_cells_total": 9,
        "family_probe_search_trials_total": 30,
        "family_probe_search_cells_total": 4,
        "tuning_search_trials_total": 47,
        "tuning_search_cells_total": 5,
        "strategy_roster_count": 100,
        "multiplicity_scope": "symbol_timeframe_all_regime_cells",
    })

    class NoBaselineReplay:
        def run(self, *_args, **_kwargs):
            raise AssertionError("baseline validation was replayed")

    optimizer.backtester = NoBaselineReplay()
    optimizer._attach_oos_evidence_to_candidates(
        "EURUSD", "H1", features, 0, [candidate]
    )

    record = candidate["oos_evidence"]
    assert record["local_search_trials_total"] == 77
    assert record["local_search_cells_total"] == 9
    assert record["strategy_roster_count"] == 100

    for mutate in (
        lambda payload: payload["period_returns"].__setitem__(0, 100.0),
        lambda payload: payload["metrics"].__setitem__(
            "total_return_pct", 999999.0
        ),
        lambda payload: payload.__setitem__(
            "baseline_gated_signals_sha256", "f" * 64
        ),
    ):
        tampered = copy.deepcopy(candidate)
        mutate(tampered["oos_evidence"])
        with pytest.raises(SearchInfrastructureError) as raised:
            optimizer._attach_oos_evidence_to_candidates(
                "EURUSD", "H1", features, 0, [tampered]
            )
        assert (
            raised.value.outcome.reason_code
            == "FROZEN_VALIDATION_REPLAY_EVIDENCE_INVALID"
        )


def test_all_honestly_underpowered_b0_trials_are_semantic_no_trade():
    optimizer = _probe_optimizer()
    optimizer._run_one_exact_regime = lambda **_kwargs: (
        {
            "total_trades": 3,
            "_trade_power_raw_result": {"trades": []},
        },
        True,
    )
    optimizer._calibrate_train_power_result = (
        lambda *_args, **_kwargs: _underpowered_decision()
    )

    with patch("pm_pipeline.StrategyRegistry.get", return_value=_Strategy()):
        candidates, cells = optimizer._collect_family_probe_candidates(
            symbol="EURUSD",
            timeframe="H1",
            train_features=SimpleNamespace(),
            strategies=[_Strategy()],
        )

    assert candidates == []
    assert len(cells) == 1
    assert cells[0]["family_outcome"]["kind"] == "EXPECTED_INELIGIBLE"
    assert cells[0]["frozen_candidate"] is False
    summary = optimizer._summarize_search_cell_outcomes(cells, regime="TREND")
    assert summary["outcome_kind_counts"] == {"EXPECTED_INELIGIBLE": 1}
    assert summary["reason_code_counts"]
    assert len(summary["cell_outcomes_sha256"]) == 64


def test_mixed_cell_summary_preserves_typed_validation_underpower_reason():
    cells = [
        {
            "phase": "family_probe_b0",
            "strategy_family": "Strong",
            "regime": "TREND",
            "search_trials": 2,
            "completed_trials": 2,
            "eligible_train_trials": 1,
            "family_outcome": {
                "kind": "VALID",
                "reason_code": "VALID_CANDIDATE_AVAILABLE",
            },
            "trade_power_validation_status": "PASS",
            "trade_power_validation_kind": "VALID",
            "trade_power_validation_reason_code": "POWER_PASS",
        },
        {
            "phase": "deep_grid",
            "strategy_family": "UnderpoweredValidation",
            "regime": "TREND",
            "search_trials": 8,
            "completed_trials": 8,
            "eligible_train_trials": 1,
            "family_outcome": {
                "kind": "VALID",
                "reason_code": "VALID_CANDIDATE_AVAILABLE",
            },
            "trade_power_validation_status": "EXPECTED_INELIGIBLE",
            "trade_power_validation_kind": "EXPECTED_INELIGIBLE",
            "trade_power_validation_reason_code": (
                "TUNING_VALIDATION_EFFECTIVE_TRADES_UNDERPOWERED"
            ),
        },
    ]

    summary = RegimeOptimizer._summarize_search_cell_outcomes(
        cells, regime="TREND"
    )

    assert summary["schema_version"] == "fxpm-search-cell-summary-v2"
    assert summary["outcome_kind_counts"] == {"VALID": 2}
    assert summary["validation_kind_counts"] == {
        "EXPECTED_INELIGIBLE": 1,
        "VALID": 1,
    }
    assert summary["validation_reason_code_counts"] == {
        "POWER_PASS": 1,
        "TUNING_VALIDATION_EFFECTIVE_TRADES_UNDERPOWERED": 1,
    }
    assert len(summary["cell_outcomes_sha256"]) == 64


def test_b0_freezes_best_economically_eligible_trial_not_invalid_top_score():
    optimizer = _probe_optimizer()
    optimizer.family_probe_trials = 2
    optimizer.config = SimpleNamespace(
        train_min_profit_factor=1.0,
        train_min_return_pct=0.0,
        train_max_drawdown=20.0,
    )
    optimizer.val_max_drawdown = 20.0
    optimizer._family_probe_param_specs = lambda *_args, **_kwargs: [
        ({"trial": "invalid_top"}, None),
        ({"trial": "eligible_lower"}, None),
    ]

    class _ParamStrategy(_Strategy):
        def __init__(self, trial):
            self.trial = trial

        def get_params(self):
            return {"trial": self.trial}

    def run_one(*, strategy, **_kwargs):
        invalid = strategy.trial == "invalid_top"
        return ({
            "total_trades": 100,
            "profit_factor": 0.5 if invalid else 1.4,
            "total_return_pct": 30.0 if invalid else 8.0,
            "max_drawdown_pct": 2.0,
            "win_rate": 55.0,
            "score_probe": 1_000.0 if invalid else 10.0,
            "_trade_power_raw_result": {"trades": []},
        }, True)

    optimizer._run_one_exact_regime = run_one
    optimizer._calibrate_train_power_result = (
        lambda *_args, **_kwargs: _passing_decision()
    )
    optimizer._compute_train_regime_score = (
        lambda metrics, *_args: float(metrics["score_probe"])
    )

    with patch(
        "pm_pipeline.StrategyRegistry.get",
        side_effect=lambda _name, **params: _ParamStrategy(params["trial"]),
    ):
        candidates, cells = optimizer._collect_family_probe_candidates(
            symbol="EURUSD",
            timeframe="H1",
            train_features=SimpleNamespace(),
            strategies=[_Strategy()],
        )

    assert len(candidates) == 1
    assert candidates[0]["params"] == {"trial": "eligible_lower"}
    assert cells[0]["eligible_train_trials"] == 1


def test_malformed_losing_b0_trial_aborts_instead_of_freezing_valid_runner_up():
    optimizer = _probe_optimizer()
    optimizer.config = SimpleNamespace(
        train_min_profit_factor=0.0,
        train_min_return_pct=-100.0,
        train_max_drawdown=100.0,
    )
    optimizer.val_max_drawdown = 100.0
    optimizer._family_probe_param_specs = lambda *_args, **_kwargs: [
        ({"trial": "malformed_low"}, None),
        ({"trial": "valid_high"}, None),
    ]

    class ParamStrategy(_Strategy):
        def __init__(self, trial):
            self.trial = trial

        def get_params(self):
            return {"trial": self.trial}

    def run_one(*, strategy, **_kwargs):
        malformed = strategy.trial == "malformed_low"
        return ({
            "total_trades": 100,
            "profit_factor": 1.4,
            "total_return_pct": 8.0,
            "max_drawdown_pct": 2.0,
            "win_rate": 200.0 if malformed else 55.0,
            "score_probe": 1.0 if malformed else 100.0,
            "_trade_power_raw_result": {"trades": []},
        }, True)

    optimizer._run_one_exact_regime = run_one
    optimizer._calibrate_train_power_result = (
        lambda *_args, **_kwargs: _passing_decision()
    )
    optimizer._compute_train_regime_score = (
        lambda metrics, *_args: float(metrics["score_probe"])
    )

    with patch(
        "pm_pipeline.StrategyRegistry.get",
        side_effect=lambda _name, **params: ParamStrategy(params["trial"]),
    ):
        with pytest.raises(SearchInfrastructureError) as raised:
            optimizer._collect_family_probe_candidates(
                symbol="EURUSD",
                timeframe="H1",
                train_features=SimpleNamespace(),
                strategies=[_Strategy()],
            )

    assert raised.value.outcome.stage is SearchStage.TRAIN_SCORE
    assert raised.value.outcome.kind is SearchOutcomeKind.INFRA_FAILURE


def test_malformed_economic_metrics_abort_instead_of_freezing_runner_up():
    optimizer = RegimeOptimizer(PipelineConfig(
        regime_min_train_trades=1,
        regime_min_val_trades=1,
        false_discovery_control_enabled=False,
    ))
    with pytest.raises(ValueError, match="TRAIN_SELECTION_SURFACE_INVALID"):
        optimizer._train_trial_economic_ineligibility({
            "total_trades": 100,
            "profit_factor": float("nan"),
            "win_rate": 55.0,
            "total_return_pct": 10.0,
            "max_drawdown_pct": 2.0,
        })

    valid = {
        "total_trades": 20,
        "profit_factor": 1.5,
        "win_rate": 55.0,
        "total_return_pct": 10.0,
        "max_drawdown_pct": 2.0,
    }
    malformed = {
        **valid,
        "strategy_name": "Malformed",
    }
    with pytest.raises(SearchInfrastructureError) as raised:
        optimizer._select_best_for_regime(
            "EURUSD",
            "H1",
            "TREND",
            [
                {
                    "strategy_name": "Malformed",
                    "params": {},
                    "train_regime_metrics": {
                        "TREND": {**valid, "profit_factor": float("nan")}
                    },
                    "val_regime_metrics": {"TREND": dict(valid)},
                },
                {
                    "strategy_name": "RunnerUp",
                    "params": {},
                    "train_regime_metrics": {"TREND": dict(valid)},
                    "val_regime_metrics": {"TREND": dict(valid)},
                },
            ],
        )
    assert raised.value.outcome.reason_code == "TRAIN_SELECTION_SURFACE_INVALID"


def test_governed_score_ties_use_effective_sample_not_raw_trade_count():
    config = PipelineConfig(
        regime_min_train_trades=1,
        regime_min_val_trades=1,
        regime_min_val_profit_factor=0.0,
        regime_min_val_return_pct=-100.0,
        regime_min_val_return_dd_ratio=0.0,
        fx_val_max_drawdown=100.0,
        false_discovery_control_enabled=False,
    )
    optimizer = RegimeOptimizer(config)
    optimizer._compute_regime_score = lambda *_args, **_kwargs: 100.0
    optimizer._validate_regime_winner = (
        lambda *_args, **_kwargs: (True, "forced integration pass")
    )

    def candidate(name, raw_trades, observations, seed):
        metrics = {
            "total_trades": raw_trades,
            "profit_factor": 1.5,
            "win_rate": 55.0,
            "total_return_pct": 10.0,
            "max_drawdown_pct": 2.0,
        }
        train_power, validation_power = _persisted_power_pair(
            train_observations=observations,
            val_observations=observations,
            seed=seed,
        )
        assert validation_power["passed"] is True
        return {
            "strategy_name": name,
            "params": {},
            "trade_power_contract_enforced": True,
            "trade_power_train": train_power,
            "trade_power_validation": validation_power,
            "train_regime_metrics": {"TREND": {
                **metrics,
                "effective_trades": train_power["observed_effective_trades"],
            }},
            "val_regime_metrics": {"TREND": {
                **metrics,
                "effective_trades": validation_power["observed_effective_trades"],
            }},
        }

    winner, validated, _ = optimizer._select_best_for_regime(
        "EURUSD",
        "H1",
        "TREND",
        [
            candidate("ManyRawWeakESS", 1_000, 500, 101),
            candidate("FewerRawStrongESS", 200, 1_000, 201),
        ],
    )

    assert validated is True
    assert winner is not None
    assert winner.strategy_name == "FewerRawStrongESS"


def test_selection_rejects_mixed_authentic_frozen_power_requirements():
    optimizer = RegimeOptimizer(PipelineConfig(
        regime_min_train_trades=1,
        regime_min_val_trades=1,
        regime_min_val_profit_factor=0.0,
        regime_min_val_return_pct=-100.0,
        false_discovery_control_enabled=False,
    ))
    train_a, _validation_a = _persisted_power_pair(
        train_observations=500,
        val_observations=500,
        seed=301,
    )
    _train_b, validation_b = _persisted_power_pair(
        train_observations=500,
        val_observations=500,
        seed=401,
    )
    assert (
        train_a["frozen_requirement"]["calibration_sample_sha256"]
        != validation_b["frozen_requirement"]["calibration_sample_sha256"]
    )
    metrics = {
        "total_trades": 500,
        "effective_trades": 500.0,
        "profit_factor": 1.4,
        "win_rate": 55.0,
        "total_return_pct": 8.0,
        "max_drawdown_pct": 4.0,
    }
    candidate = {
        "strategy_name": "CrossDrift",
        "params": {},
        "trade_power_contract_enforced": True,
        "trade_power_train": train_a,
        "trade_power_validation": validation_b,
        "train_regime_metrics": {"TREND": {
            **metrics,
            "effective_trades": train_a["observed_effective_trades"],
        }},
        "val_regime_metrics": {"TREND": {
            **metrics,
            "effective_trades": validation_b["observed_effective_trades"],
        }},
    }

    with pytest.raises(SearchInfrastructureError) as raised:
        optimizer._select_best_for_regime(
            "EURUSD", "H1", "TREND", [candidate]
        )

    assert (
        raised.value.outcome.reason_code
        == "TRADE_POWER_FROZEN_REQUIREMENT_DRIFT"
    )


def test_malformed_effective_metric_aborts_before_lower_candidate_descent():
    optimizer = RegimeOptimizer(PipelineConfig(
        regime_min_train_trades=1,
        regime_min_val_trades=1,
        regime_min_val_profit_factor=0.0,
        regime_min_val_return_pct=-100.0,
        false_discovery_control_enabled=False,
    ))
    optimizer._compute_regime_score = lambda *_args, **_kwargs: 100.0
    train_power, validation_power = _persisted_power_pair(
        train_observations=500,
        val_observations=500,
        seed=421,
    )
    metrics = {
        "total_trades": 500,
        "profit_factor": 1.4,
        "win_rate": 55.0,
        "total_return_pct": 8.0,
        "max_drawdown_pct": 4.0,
    }

    def candidate(name, *, valid_effective):
        train_metrics = dict(metrics)
        val_metrics = dict(metrics)
        if valid_effective:
            train_metrics["effective_trades"] = train_power[
                "observed_effective_trades"
            ]
            val_metrics["effective_trades"] = validation_power[
                "observed_effective_trades"
            ]
        return {
            "strategy_name": name,
            "params": {},
            "trade_power_contract_enforced": True,
            "trade_power_train": copy.deepcopy(train_power),
            "trade_power_validation": copy.deepcopy(validation_power),
            "train_regime_metrics": {"TREND": train_metrics},
            "val_regime_metrics": {"TREND": val_metrics},
        }

    with pytest.raises(SearchInfrastructureError) as raised:
        optimizer._select_best_for_regime(
            "EURUSD",
            "H1",
            "TREND",
            [
                candidate("MalformedTop", valid_effective=False),
                candidate("LowerValid", valid_effective=True),
            ],
        )

    assert (
        raised.value.outcome.reason_code
        == "TRADE_POWER_METRIC_EFFECTIVE_COUNT_DRIFT"
    )


def test_incomplete_or_nonfinite_persisted_power_pass_is_infrastructure_failure():
    forged = {
        "schema_version": TRADE_POWER_DECISION_SCHEMA_VERSION,
        "status": "PASS",
        "outcome_kind": "VALID",
        "partition": "optimizer_train",
        "passed": True,
        "requirement_recalibrated": False,
        "observed_effective_trades": "not-a-number",
    }
    state, reason = RegimeOptimizer._trade_power_decision_state(
        forged,
        partition="optimizer_train",
    )
    assert state == "INFRA"
    assert reason == "TRADE_POWER_DECISION_CONTRACT_INVALID"


def test_persisted_power_requirement_cannot_forge_lower_required_counts():
    train_power, validation_power = _persisted_power_pair(
        train_observations=500,
        val_observations=12,
        seed=451,
    )
    assert validation_power["passed"] is False
    forged_train = copy.deepcopy(train_power)
    forged_validation = copy.deepcopy(validation_power)
    for decision in (forged_train, forged_validation):
        requirement = decision["frozen_requirement"]
        requirement["required_effective_train_trades"] = 1
        requirement["required_effective_tuning_trades"] = 1
        requirement["required_effective_final_trades"] = 1
        decision["required_effective_trades"] = 1
    forged_validation.update({
        "status": "PASS",
        "outcome_kind": "VALID",
        "passed": True,
        "reason_codes": [],
    })

    train_state = RegimeOptimizer._trade_power_decision_state(
        forged_train,
        partition="optimizer_train",
    )
    validation_state = RegimeOptimizer._trade_power_decision_state(
        forged_validation,
        partition="tuning_validation",
    )

    assert train_state == ("INFRA", "TRADE_POWER_DECISION_REQUIREMENT_INVALID")
    assert validation_state == (
        "INFRA",
        "TRADE_POWER_DECISION_REQUIREMENT_INVALID",
    )


def test_zero_trade_validation_is_honest_ineligible_not_infrastructure_failure():
    _train_power, validation_power = _persisted_power_pair(
        train_observations=500,
        val_observations=0,
        seed=471,
    )

    state = RegimeOptimizer._trade_power_decision_state(
        validation_power,
        partition="tuning_validation",
    )

    assert state == ("INELIGIBLE", "TRADE_POWER_UNDERPOWERED")


def test_persisted_requirement_rejects_forged_swap_provenance():
    train_power, validation_power = _persisted_power_pair(
        train_observations=500,
        val_observations=500,
        seed=481,
    )
    for decision in (train_power, validation_power):
        decision["frozen_requirement"]["swap_provenance_class"] = "FAKE"
        decision["frozen_requirement"][
            "historical_financing_provenance"
        ] = "FAKE"

    assert RegimeOptimizer._trade_power_decision_state(
        train_power,
        partition="optimizer_train",
    ) == ("INFRA", "TRADE_POWER_DECISION_REQUIREMENT_INVALID")
    assert RegimeOptimizer._trade_power_decision_state(
        validation_power,
        partition="tuning_validation",
    ) == ("INFRA", "TRADE_POWER_DECISION_REQUIREMENT_INVALID")


def test_validation_underpower_is_counted_before_frozen_roster_selection():
    config = PipelineConfig(
        regime_min_train_trades=1,
        regime_min_val_trades=1,
        regime_min_val_profit_factor=0.0,
        regime_min_val_return_pct=-100.0,
        regime_min_val_return_dd_ratio=0.0,
        fx_val_max_drawdown=100.0,
        false_discovery_control_enabled=True,
    )
    optimizer = RegimeOptimizer(config)
    optimizer._compute_regime_score = lambda *_args, **_kwargs: 100.0
    optimizer._validate_regime_winner = (
        lambda *_args, **_kwargs: (True, "forced integration pass")
    )
    optimizer._build_false_discovery_evidence = (
        lambda *_args, **_kwargs: {"enabled": False}
    )

    def candidate(name, validation_passed):
        metrics = {
            "total_trades": 100,
            "profit_factor": 1.5,
            "win_rate": 55.0,
            "total_return_pct": 10.0,
            "max_drawdown_pct": 2.0,
        }
        train_power, validation_power = _persisted_power_pair(
            train_observations=500,
            val_observations=500 if validation_passed else 12,
            seed=301 if validation_passed else 401,
        )
        assert validation_power["passed"] is validation_passed
        return {
            "strategy_name": name,
            "params": {},
            "trade_power_contract_enforced": True,
            "trade_power_train": train_power,
            "trade_power_validation": validation_power,
            "train_regime_metrics": {"TREND": {
                **metrics,
                "effective_trades": train_power["observed_effective_trades"],
            }},
            "val_regime_metrics": {"TREND": {
                **metrics,
                "effective_trades": validation_power["observed_effective_trades"],
            }},
        }

    roster_sizes = []

    def record_roster(roster, *_args, **_kwargs):
        roster_sizes.append(len(roster))
        return {"status": "PASS", "passed": True, "pbo": 0.0}

    with patch("pm_pipeline.aligned_cell_pbo", side_effect=record_roster), patch(
        "pm_pipeline.aligned_validation_dsr_population",
        return_value={"status": "PASS"},
    ):
        winner, validated, _ = optimizer._select_best_for_regime(
            "EURUSD",
            "H1",
            "TREND",
            [candidate("Strong", True), candidate("ValidationUnderpowered", False)],
        )

    assert roster_sizes == [2]
    assert validated is True
    assert winner is not None
    assert winner.strategy_name == "Strong"


def test_legacy_score_uses_effective_trades_for_governed_candidates():
    config = PipelineConfig(
        regime_min_train_trades=10,
        regime_min_val_trades=10,
        false_discovery_control_enabled=False,
    )
    optimizer = RegimeOptimizer(config)
    metrics_many_raw = {
        "total_trades": 10_000,
        "profit_factor": 1.5,
        "win_rate": 55.0,
        "total_return_pct": 10.0,
        "max_drawdown_pct": 2.0,
    }

    def governed_candidate(effective):
        return {
            "trade_power_contract_enforced": True,
            "trade_power_train": {"observed_effective_trades": effective},
            "trade_power_validation": {"observed_effective_trades": effective},
        }

    weak_ess = optimizer._compute_regime_score(
        dict(metrics_many_raw),
        dict(metrics_many_raw),
        candidate=governed_candidate(2.0),
    )
    strong_ess = optimizer._compute_regime_score(
        {**metrics_many_raw, "total_trades": 20},
        {**metrics_many_raw, "total_trades": 20},
        candidate=governed_candidate(20.0),
    )

    assert strong_ess > weak_ess


def test_governed_scoring_is_invariant_to_raw_trade_count():
    optimizer = RegimeOptimizer(PipelineConfig(
        regime_min_train_trades=10,
        regime_min_val_trades=10,
        false_discovery_control_enabled=False,
    ))
    train_power, validation_power = _persisted_power_pair(
        train_observations=500,
        val_observations=500,
        seed=501,
    )
    candidate = {
        "trade_power_contract_enforced": True,
        "trade_power_train": train_power,
        "trade_power_validation": validation_power,
    }
    base = {
        "profit_factor": 1.5,
        "win_rate": 55.0,
        "total_return_pct": 10.0,
        "max_drawdown_pct": 2.0,
        "mean_r": 0.2,
        "median_r": 0.1,
        "std_r": 0.7,
        "se_r": 0.01,
        "gross_profit": 500.0,
        "gross_loss": 200.0,
        "total_pnl": 300.0,
    }
    low_raw = {**base, "total_trades": 2, "r_count": 2}
    high_raw = {**base, "total_trades": 2_000, "r_count": 2_000}
    low_score = optimizer._compute_regime_score(
        low_raw, low_raw, candidate=candidate
    )
    high_score = optimizer._compute_regime_score(
        high_raw, high_raw, candidate=candidate
    )
    assert high_score == pytest.approx(low_score, rel=0.0, abs=1e-12)


