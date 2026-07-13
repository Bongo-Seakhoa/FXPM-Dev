from __future__ import annotations

import copy
from unittest.mock import patch

import numpy as np
import pandas as pd

from pm_core import Backtester, InstrumentSpec, PipelineConfig
from pm_pipeline import RegimeOptimizer, StrategyRegistry, build_artifact_meta
from pm_statistics import (
    DAILY_NET_MTM_CONTRACT_VERSION,
    ENTRY_SLIPPAGE_TIMING_CONTRACT_VERSION,
    EXECUTION_COST_TIMING_CONTRACT,
    SWAP_ACCRUAL_TIMING_CONTRACT_VERSION,
    STATISTICS_CONTRACT_VERSION,
)
from pm_trade_power import (
    REQUIRED_ENTRY_SLIPPAGE_CONTRACT,
    REQUIRED_SWAP_ACCRUAL_CONTRACT,
    REQUIRED_TRADE_RECORD_SCHEMA,
    build_numerical_execution_cost_attestation,
)


class _ExactStrategy:
    def __init__(self, name: str, daily_returns):
        self.name = name
        self.params = {"variant": name}
        self.daily_returns = np.asarray(daily_returns, dtype=np.float64)

    def get_params(self):
        return dict(self.params)

    def generate_signals(self, features, _symbol):
        return pd.Series(1, index=features.index, dtype=int)


class _EquityBacktester:
    def __init__(self, initial_capital: float = 10000.0):
        self.initial_capital = float(initial_capital)

    def run(self, features, _signals, _symbol, strategy, **_kwargs):
        returns = np.asarray(strategy.daily_returns, dtype=np.float64)
        assert len(returns) == len(features)
        equity = np.empty(len(returns), dtype=np.float64)
        equity[0] = self.initial_capital
        for index in range(1, len(equity)):
            equity[index] = equity[index - 1] * (1.0 + returns[index])
        running_peak = np.maximum.accumulate(equity)
        drawdown = np.max((running_peak - equity) / running_peak) * 100.0
        total_return = (equity[-1] / self.initial_capital - 1.0) * 100.0
        trades = [
            {
                "signal_bar": max(0, index * 5 - 1),
                "entry_bar": index * 5,
                "exit_bar": min(len(features) - 1, index * 5 + 2),
                "entry_time": features.index[min(len(features) - 1, index * 5)],
                "exit_time": features.index[min(len(features) - 1, index * 5 + 2)],
                "pnl_dollars": 20.0 if index % 3 else -5.0,
                "risk_amount": 100.0,
                "r_multiple": 0.2 if index % 3 else -0.05,
                "trade_record_schema_version": REQUIRED_TRADE_RECORD_SCHEMA,
            }
            for index in range(1, 41)
        ]
        return {
            "total_trades": len(trades),
            "profit_factor": 2.0,
            "win_rate": 66.0,
            "sharpe_ratio": 2.0,
            "sortino_ratio": 2.5,
            "total_return_pct": float(total_return),
            "max_drawdown_pct": float(drawdown),
            "gross_profit": 800.0,
            "gross_loss": 200.0,
            "total_pnl": 600.0,
            "mean_r": 0.12,
            "median_r": 0.10,
            "worst_5pct_r": -0.05,
            "max_consecutive_losses": 1,
            "final_equity": float(equity[-1]),
            "equity_curve": equity.tolist(),
            "trades": trades,
            "execution_cost_timing_contract": {
                "entry_slippage": REQUIRED_ENTRY_SLIPPAGE_CONTRACT,
                "swap_accrual": REQUIRED_SWAP_ACCRUAL_CONTRACT,
            },
            "numerical_execution_cost_attestation": (
                build_numerical_execution_cost_attestation(
                    use_spread=True,
                    use_slippage=True,
                    use_commission=True,
                    spread_mode="conservative_max",
                    spread_values_sha256="9" * 64,
                    commission_inputs_sha256="a" * 64,
                    instrument_spec_sha256="b" * 64,
                    slippage_pips=0.5,
                    slippage_price=0.00005,
                )
            ),
            "swap_accrual_contract": {
                "contract_version": REQUIRED_SWAP_ACCRUAL_CONTRACT,
                "status": "AVAILABLE",
                "provenance_status": "AUTHENTICATED",
                "execution_state_compounding": True,
                "timestamp_source": "_BAR_CLOSE_TS",
                "source_provenance_authenticated": True,
                "historical_rates_authenticated": True,
                "historical_rates_sha256": "8" * 64,
                "price_activation_manifest_sha256": "1" * 64,
                "terms_activation_manifest_sha256": "2" * 64,
                "terms_snapshot_sha256": "3" * 64,
                "schedule_sha256": "4" * 64,
                "target_bars_sha256": "5" * 64,
                "target_sidecar_sha256": "6" * 64,
                "projection_contract": "fxpm-swap-preopen-close-projection-v1",
                "projection_sha256": "7" * 64,
            },
        }


class _OpenAcrossDaysBacktester(_EquityBacktester):
    def run(self, features, signals, symbol, strategy, **kwargs):
        result = super().run(features, signals, symbol, strategy, **kwargs)
        result["total_trades"] = 1
        result["trades"] = [{
            "signal_bar": 0,
            "entry_bar": 1,
            "exit_bar": len(features) - 1,
            "entry_time": features.index[1],
            "exit_time": features.index[-1],
            "pnl_dollars": 100.0,
            "risk_amount": 100.0,
            "r_multiple": 1.0,
            "trade_record_schema_version": REQUIRED_TRADE_RECORD_SCHEMA,
        }]
        return result


class _FrozenOptuna:
    def __init__(self, candidate):
        self.candidate = candidate
        self.last_regime_search_metadata = {
            "search_trials_total": 1500,
            "cell_count": 1,
            "tuning_validation_evaluations": 1,
        }

    def optimize_for_regimes(self, **_kwargs):
        return {"TREND": copy.deepcopy(self.candidate)}


def _features(periods: int) -> pd.DataFrame:
    opens = pd.date_range("2020-01-01", periods=periods, freq="D", tz="UTC")
    closes = opens + pd.Timedelta(days=1)
    return pd.DataFrame(
        {
            "Open": 1.0,
            "High": 1.1,
            "Low": 0.9,
            "Close": 1.0,
            "REGIME_LIVE": "TREND",
            "_BAR_CLOSE_TS": closes,
        },
        index=opens,
    )


def _optimizer(*, export_enabled: bool = False) -> RegimeOptimizer:
    config = PipelineConfig(
        research_mode=True,
        timeframes=["D1"],
        false_discovery_control_enabled=True,
        false_discovery_min_daily_observations=252,
        false_discovery_min_effective_observations=60.0,
        false_discovery_min_dsr=0.95,
        false_discovery_pbo_min_candidates=5,
        false_discovery_max_pbo=0.50,
        false_discovery_pbo_blocks=10,
        false_discovery_bootstrap_repetitions=100,
        regime_min_train_trades=15,
        regime_min_val_trades=15,
        regime_min_val_profit_factor=1.0,
        regime_min_val_return_pct=0.0,
        regime_min_val_return_dd_ratio=0.1,
        fx_val_max_drawdown=20.0,
        fx_min_robustness_ratio=0.0,
        fx_val_sharpe_override=-99.0,
        oos_evidence_export_enabled=export_enabled,
        oos_evidence_periods=32,
        oos_evidence_min_periods=8,
    )
    optimizer = RegimeOptimizer(config)
    optimizer.backtester = _EquityBacktester(config.initial_capital)
    return optimizer


def _candidate_pool(optimizer: RegimeOptimizer, *, export_enabled: bool = False):
    features = _features(320)
    rng = np.random.default_rng(20260713)
    candidates = []
    for index in range(5):
        returns = rng.normal(
            0.0030 - index * 0.00035,
            0.0010 + index * 0.00020,
            size=len(features),
        )
        returns[0] = 0.0
        returns[40] = 0.0
        strategy = _ExactStrategy(f"Persistent{index}", returns)
        train_metrics, train_ran = optimizer._run_one_exact_regime(
            symbol="EURUSD",
            timeframe="D1",
            features=features,
            strategy=strategy,
            regime="TREND",
            partition="optimizer_train",
        )
        val_metrics, val_ran = optimizer._run_one_exact_regime(
            symbol="EURUSD",
            timeframe="D1",
            features=features,
            strategy=strategy,
            regime="TREND",
            partition="tuning_validation",
        )
        assert train_ran and val_ran
        candidate = {
            "strategy": strategy,
            "strategy_name": strategy.name,
            "params": strategy.get_params(),
            "evaluation_regime": "TREND",
            "search_trials": 1500 if index == 0 else 1,
            "local_search_trials_total": 1500 if index == 0 else 1,
            "train_regime_metrics": {"TREND": train_metrics},
            "val_regime_metrics": {"TREND": val_metrics},
        }
        candidates.append(candidate)
    if export_enabled:
        optimizer._attach_oos_evidence_to_candidates(
            "EURUSD",
            "D1",
            features,
            0,
            candidates,
        )
    return features, candidates


def test_artifact_fingerprint_binds_statistics_and_cost_timing_contracts():
    metadata = build_artifact_meta(_optimizer().config)

    assert metadata["statistics_contract_version"] == STATISTICS_CONTRACT_VERSION
    assert metadata["entry_slippage_timing_contract_version"] == (
        ENTRY_SLIPPAGE_TIMING_CONTRACT_VERSION
    )
    assert metadata["swap_accrual_timing_contract_version"] == (
        SWAP_ACCRUAL_TIMING_CONTRACT_VERSION
    )


def test_optuna_frozen_candidate_raw_equity_is_plumbed_before_compaction():
    optimizer = _optimizer()
    features = _features(320)
    returns = np.random.default_rng(77).normal(0.002, 0.001, size=320)
    returns[0] = 0.0
    strategy = _ExactStrategy("OptunaFrozen", returns)
    raw_train = optimizer.backtester.run(
        features,
        strategy.generate_signals(features, "EURUSD"),
        "EURUSD",
        strategy,
        timeframe="D1",
    )
    raw_val = copy.deepcopy(raw_train)
    optimizer.optuna_optimizer = _FrozenOptuna({
        "strategy": strategy,
        "strategy_name": strategy.name,
        "params": strategy.get_params(),
        "train_result": raw_train,
        "train_regime_metrics": {"TREND": raw_train},
        "val_regime_metrics": {"TREND": raw_val},
        "evaluation_regime": "TREND",
        "is_tuned": True,
    })

    candidates = optimizer._tune_strategy_params(
        "EURUSD",
        "D1",
        strategy.name,
        {"variant": [strategy.name]},
        features,
        features,
        regimes=["TREND"],
    )

    assert len(candidates) == 1
    candidate = candidates[0]
    train = candidate["train_regime_metrics"]["TREND"]
    validation = candidate["val_regime_metrics"]["TREND"]
    assert train["daily_net_mtm_contract"]["partition"] == "optimizer_train"
    assert validation["daily_net_mtm_contract"]["partition"] == "tuning_validation"
    assert validation["daily_net_mtm_observations"] == 320
    assert "equity_curve" not in validation
    assert "trades" not in validation
    assert candidate["candidate_id"]


def test_real_optuna_tpe_candidate_reaches_regime_optimizer_daily_mtm():
    strategy_name = "WinnerStatisticsRealOptunaStrategy"
    features = _features(320)
    base_returns = np.random.default_rng(7701).normal(0.002, 0.001, size=320)
    base_returns[0] = 0.0

    class _RealOptunaStrategy:
        name = strategy_name

        def __init__(self, variant=1, **_kwargs):
            self.params = {"variant": int(variant)}
            self.daily_returns = base_returns + (int(variant) - 1) * 0.0001
            self.daily_returns[0] = 0.0

        def get_params(self):
            return dict(self.params)

        def generate_signals(self, frame, _symbol):
            return pd.Series(1, index=frame.index, dtype=int)

    prior = StrategyRegistry._strategies.get(strategy_name)
    StrategyRegistry._strategies[strategy_name] = _RealOptunaStrategy
    try:
        optimizer = _optimizer()
        assert optimizer.optuna_optimizer is not None
        optimizer.optuna_optimizer.backtester = optimizer.backtester

        candidates = optimizer._tune_strategy_params(
            "EURUSD",
            "D1",
            strategy_name,
            {"variant": [1, 2]},
            features,
            features,
            regimes=["TREND"],
        )

        assert len(candidates) == 1
        validation = candidates[0]["val_regime_metrics"]["TREND"]
        assert validation["daily_net_mtm_observations"] == 320
        assert validation["daily_net_mtm_contract"]["status"] == "AVAILABLE"
        assert validation["daily_net_mtm_contract"]["partition"] == (
            "tuning_validation"
        )
        assert "equity_curve" not in validation
    finally:
        if prior is None:
            StrategyRegistry._strategies.pop(strategy_name, None)
        else:
            StrategyRegistry._strategies[strategy_name] = prior


def test_daily_net_mtm_surface_keeps_open_trade_marks_zero_days_and_warmup():
    optimizer = _optimizer()
    optimizer.backtester = _OpenAcrossDaysBacktester(
        optimizer.config.initial_capital
    )
    features = _features(6)
    strategy = _ExactStrategy(
        "OpenAcrossDays",
        [0.0, 0.01, 0.0, 0.02, -0.005, 0.01],
    )

    metrics, ran = optimizer._run_one_exact_regime(
        symbol="EURUSD",
        timeframe="D1",
        features=features,
        strategy=strategy,
        regime="TREND",
        warmup_bars=2,
        partition="tuning_validation",
    )

    assert ran
    assert metrics["daily_net_mtm_observations"] == 4
    assert np.allclose(
        metrics["daily_net_mtm_returns"],
        [0.0, 0.02, -0.005, 0.01],
    )
    assert metrics["daily_net_mtm_dates"][0] == "2020-01-04T00:00:00+00:00"
    contract = metrics["daily_net_mtm_contract"]
    assert contract["contract_version"] == DAILY_NET_MTM_CONTRACT_VERSION
    assert contract["partition"] == "tuning_validation"
    assert contract["scored_start_bar"] == 2
    assert contract["zero_return_days"] == 1
    assert contract["cost_contract"]["entry_slippage"] == (
        ENTRY_SLIPPAGE_TIMING_CONTRACT_VERSION
    )
    assert contract["cost_contract"]["swap_accrual"] == (
        SWAP_ACCRUAL_TIMING_CONTRACT_VERSION
    )
    assert contract["execution_cost_timing_contract"] == (
        EXECUTION_COST_TIMING_CONTRACT
    )


def test_daily_net_mtm_requires_explicit_close_and_cost_attestation():
    optimizer = _optimizer()
    features = _features(6)
    strategy = _ExactStrategy("AttestationProbe", [0.0, 0.01, 0.0, 0.01, 0.0, 0.01])
    raw = optimizer.backtester.run(
        features,
        strategy.generate_signals(features, "EURUSD"),
        "EURUSD",
        strategy,
        timeframe="D1",
    )

    no_close = optimizer._build_daily_net_mtm_surface(
        result=raw,
        features=features.drop(columns=["_BAR_CLOSE_TS"]),
        timeframe="D1",
        warmup_bars=0,
        partition="tuning_validation",
    )
    no_cost = optimizer._build_daily_net_mtm_surface(
        result={
            key: value
            for key, value in raw.items()
            if key != "execution_cost_timing_contract"
        },
        features=features,
        timeframe="D1",
        warmup_bars=0,
        partition="tuning_validation",
    )

    assert no_close["daily_net_mtm_contract"]["status"] == "UNAVAILABLE"
    assert "_BAR_CLOSE_TS" in no_close["daily_net_mtm_contract"]["reason"]
    assert no_cost["daily_net_mtm_contract"]["status"] == "UNAVAILABLE"
    assert "does not attest" in no_cost["daily_net_mtm_contract"]["reason"]


def test_config_disabled_swap_does_not_claim_cost_complete_daily_surface():
    config = PipelineConfig(
        research_mode=True,
        timeframes=["D1"],
        use_spread=False,
        use_slippage=False,
        use_commission=False,
        swap_model_enabled=False,
    )
    optimizer = RegimeOptimizer(config)
    features = _features(8).copy()
    features["ATR_14"] = 0.001
    strategy = _ExactStrategy("RealBacktesterContractProbe", np.zeros(len(features)))
    signals = strategy.generate_signals(features, "ZZZUSD")
    result = Backtester(config).run(
        features,
        signals,
        "ZZZUSD",
        strategy,
        spec=InstrumentSpec(
            symbol="ZZZUSD",
            pip_position=4,
            pip_value=10.0,
            spread_avg=0.0,
            commission_per_lot=0.0,
        ),
        timeframe="D1",
    )

    surface = optimizer._build_daily_net_mtm_surface(
        result=result,
        features=features,
        timeframe="D1",
        warmup_bars=0,
        partition="tuning_validation",
    )

    assert result["execution_cost_timing_contract"] == EXECUTION_COST_TIMING_CONTRACT
    assert result["swap_accrual_contract"]["status"] == "DISABLED"
    assert surface["daily_net_mtm_contract"]["status"] == "UNAVAILABLE"
    assert surface["daily_net_mtm_observations"] == 0
    assert "not authenticated proof" in surface["daily_net_mtm_contract"]["reason"]


def test_default_swap_enabled_unauthenticated_schedule_is_not_statistical_evidence():
    config = PipelineConfig(
        research_mode=True,
        timeframes=["D1"],
        use_spread=False,
        use_slippage=False,
        use_commission=False,
    )
    assert config.swap_model_enabled is True
    optimizer = RegimeOptimizer(config)
    features = _features(8).copy()
    features["ATR_14"] = 0.001
    strategy = _ExactStrategy("RealBacktesterSwapProbe", np.zeros(len(features)))
    result = Backtester(config).run(
        features,
        strategy.generate_signals(features, "ZZZUSD"),
        "ZZZUSD",
        strategy,
        spec=InstrumentSpec(
            symbol="ZZZUSD",
            pip_position=4,
            pip_value=10.0,
            spread_avg=0.0,
            commission_per_lot=0.0,
            swap_long=-1.0,
            swap_short=0.5,
        ),
        timeframe="D1",
    )

    surface = optimizer._build_daily_net_mtm_surface(
        result=result,
        features=features,
        timeframe="D1",
        warmup_bars=0,
        partition="tuning_validation",
    )

    swap_contract = result["swap_accrual_contract"]
    assert swap_contract["status"] == "UNAVAILABLE"
    assert swap_contract["execution_state_compounding"] is True
    assert swap_contract["timestamp_source"] == "_BAR_CLOSE_TS"
    assert swap_contract["source_provenance_authenticated"] is False
    assert surface["daily_net_mtm_contract"]["status"] == "UNAVAILABLE"
    assert surface["daily_net_mtm_observations"] == 0
    assert "swap source contract is unavailable" in (
        surface["daily_net_mtm_contract"]["reason"]
    )


def test_true_exact_regime_runs_supply_default_hard_gates_and_ignore_export_toggle():
    outcomes = []
    for export_enabled in (False, True):
        optimizer = _optimizer(export_enabled=export_enabled)
        _features_frame, candidates = _candidate_pool(
            optimizer,
            export_enabled=export_enabled,
        )

        winner, validated, reason = optimizer._select_best_for_regime(
            "EURUSD",
            "D1",
            "TREND",
            candidates,
        )

        assert validated, reason
        false_discovery = winner.validation_evidence["false_discovery"]
        assert false_discovery["dsr_evidence"]["status"] == "PASS"
        assert false_discovery["pbo_status"] == "pass"
        assert false_discovery["bootstrap_evidence"]["status"] == "PASS"
        assert false_discovery["validation_frozen_candidates"] == 5
        assert 1.0 <= false_discovery["effective_trials"] <= 5.0
        assert false_discovery["daily_net_mtm_observations"] == 320
        assert false_discovery["pbo_candidate_included"] is True
        outcomes.append(
            (
                winner.strategy_name,
                false_discovery["deflated_sharpe"],
                false_discovery["pbo"],
                false_discovery["effective_trials"],
            )
        )
    assert outcomes[0] == outcomes[1]


def test_train_eligible_validation_loser_remains_in_dsr_and_pbo_population():
    optimizer = _optimizer()
    _features_frame, candidates = _candidate_pool(optimizer)
    loser = copy.deepcopy(candidates[0])
    loser["strategy_name"] = "FlatValidationLoser"
    loser["params"] = {"variant": "flat-loser"}
    loser_metrics = loser["val_regime_metrics"]["TREND"]
    loser_metrics["profit_factor"] = 0.1
    loser_metrics["total_return_pct"] = 0.0
    loser_metrics["daily_net_mtm_returns"] = [
        0.0 for _ in loser_metrics["daily_net_mtm_returns"]
    ]
    population = [*copy.deepcopy(candidates), loser]

    winner, validated, reason = optimizer._select_best_for_regime(
        "EURUSD",
        "D1",
        "TREND",
        population,
    )

    assert validated, reason
    false_discovery = winner.validation_evidence["false_discovery"]
    pbo = false_discovery["pbo_evidence"]
    dsr_population = false_discovery["dsr_population_evidence"]
    assert pbo["input_candidate_count"] == 6
    assert pbo["train_predeclared_eligible_count"] == 6
    assert pbo["unique_predeclared_frozen_validation_candidate_count"] == 6
    assert len(pbo["candidate_ids"]) == 6
    assert population[-1]["candidate_id"] in pbo["candidate_ids"]
    assert dsr_population["status"] == "AVAILABLE"
    assert dsr_population["raw_frozen_candidates"] == 6
    assert false_discovery["validation_frozen_candidates"] == 6


def test_train_ineligible_candidate_is_excluded_before_validation_population():
    optimizer = _optimizer()
    _features_frame, candidates = _candidate_pool(optimizer)
    rejected = copy.deepcopy(candidates[0])
    rejected["strategy_name"] = "TrainIneligible"
    rejected["params"] = {"variant": "train-ineligible"}
    rejected["train_regime_metrics"]["TREND"]["total_trades"] = 0
    rejected["val_regime_metrics"]["TREND"] = {}

    winner, validated, reason = optimizer._select_best_for_regime(
        "EURUSD",
        "D1",
        "TREND",
        [*copy.deepcopy(candidates), rejected],
    )

    assert validated, reason
    false_discovery = winner.validation_evidence["false_discovery"]
    pbo = false_discovery["pbo_evidence"]
    assert pbo["input_candidate_count"] == 6
    assert pbo["train_predeclared_eligible_count"] == 5
    assert pbo["unique_predeclared_frozen_validation_candidate_count"] == 5
    assert false_discovery["validation_frozen_candidates"] == 5


def test_post_gate_missing_daily_surface_blocks_cell_instead_of_disappearing():
    optimizer = _optimizer()
    _features_frame, candidates = _candidate_pool(optimizer)
    missing = copy.deepcopy(candidates[0])
    missing["strategy_name"] = "EligibleButEvidenceMissing"
    missing["params"] = {"variant": "missing"}
    missing["val_regime_metrics"]["TREND"].pop("daily_net_mtm_returns")
    missing["val_regime_metrics"]["TREND"].pop("daily_net_mtm_dates")
    missing["val_regime_metrics"]["TREND"].pop("daily_net_mtm_contract")

    winner, validated, reason = optimizer._select_best_for_regime(
        "EURUSD",
        "D1",
        "TREND",
        [*copy.deepcopy(candidates), missing],
    )

    assert winner is None
    assert not validated
    assert "aligned cell PBO UNAVAILABLE" in reason or "daily-net-MTM DSR UNAVAILABLE" in reason


def test_default_false_discovery_gates_still_reject_short_daily_history():
    optimizer = _optimizer()
    _features_frame, candidates = _candidate_pool(optimizer)
    for candidate in candidates:
        metrics = candidate["val_regime_metrics"]["TREND"]
        metrics["daily_net_mtm_returns"] = metrics["daily_net_mtm_returns"][:100]
        metrics["daily_net_mtm_dates"] = metrics["daily_net_mtm_dates"][:100]
        metrics["daily_net_mtm_observations"] = 100
        metrics["daily_net_mtm_contract"]["observations"] = 100
        metrics["daily_net_mtm_contract"]["daily_axis_end"] = (
            metrics["daily_net_mtm_dates"][-1]
        )
        metrics["daily_net_mtm_contract"]["scored_end_ts"] = (
            metrics["daily_net_mtm_dates"][-1]
        )

    winner, validated, reason = optimizer._select_best_for_regime(
        "EURUSD",
        "D1",
        "TREND",
        candidates,
    )

    assert winner is None
    assert not validated
    assert "UNAVAILABLE" in reason


def test_validation_dsr_counts_frozen_models_not_inner_train_trials():
    optimizer = _optimizer()
    values = np.random.default_rng(9).normal(0.002, 0.004, size=320).tolist()
    dates = [
        value.isoformat()
        for value in pd.date_range("2020-01-01", periods=320, freq="D", tz="UTC")
    ]
    metrics = {
        "total_trades": 40,
        "daily_net_mtm_returns": values,
        "daily_net_mtm_dates": dates,
        "daily_net_mtm_contract": {
            "contract_version": DAILY_NET_MTM_CONTRACT_VERSION,
            "status": "AVAILABLE",
            "origin": "backtester_final_post_cost_equity_curve",
            "partition": "tuning_validation",
            "uses_final_lockbox": False,
            "observations": len(values),
            "execution_cost_timing_contract": dict(
                EXECUTION_COST_TIMING_CONTRACT
            ),
            "swap_accrual_contract": {
                "status": "AVAILABLE",
                "provenance_status": "AUTHENTICATED",
                "execution_state_compounding": True,
                "timestamp_source": "_BAR_CLOSE_TS",
                "source_provenance_authenticated": True,
                "historical_rates_authenticated": True,
                "historical_rates_sha256": "8" * 64,
                "price_activation_manifest_sha256": "1" * 64,
                "terms_activation_manifest_sha256": "2" * 64,
                "terms_snapshot_sha256": "3" * 64,
                "schedule_sha256": "4" * 64,
                "target_bars_sha256": "5" * 64,
                "target_sidecar_sha256": "6" * 64,
                "projection_sha256": "7" * 64,
            },
        },
    }
    attempted = []

    def capture_dsr(_values, *, attempted_trials, **_kwargs):
        attempted.append(attempted_trials)
        return {
            "contract_version": "test",
            "status": "PASS",
            "deflated_sharpe": 0.99,
            "sharpe_annualized": 2.0,
        }

    with patch("pm_pipeline.deflated_sharpe_evidence", side_effect=capture_dsr), patch(
        "pm_pipeline.block_bootstrap_evidence",
        return_value={"status": "PASS", "passed": True},
    ):
        for train_trials, validation_trials in ((1, 1), (1500, 1), (1500, 10)):
            candidate = {
                "strategy_name": "Frozen",
                "params": {},
                "search_trials": train_trials,
                "local_search_trials_total": train_trials,
            }
            optimizer._build_false_discovery_evidence(
                metrics,
                [candidate],
                candidate,
                regime="TREND",
                pbo_evidence={"status": "PASS", "passed": True, "pbo": 0.0},
                validation_trials=validation_trials,
            )

    assert attempted == [1, 1, 10]


def test_daily_net_mtm_admits_only_authenticated_or_explicit_stress_swap():
    dates = [
        value.isoformat()
        for value in pd.date_range("2020-01-01", periods=3, freq="D", tz="UTC")
    ]
    metrics = {
        "daily_net_mtm_returns": [0.001, -0.0005, 0.002],
        "daily_net_mtm_dates": dates,
        "daily_net_mtm_contract": {
            "contract_version": DAILY_NET_MTM_CONTRACT_VERSION,
            "status": "AVAILABLE",
            "origin": "backtester_final_post_cost_equity_curve",
            "partition": "tuning_validation",
            "uses_final_lockbox": False,
            "observations": 3,
            "execution_cost_timing_contract": dict(
                EXECUTION_COST_TIMING_CONTRACT
            ),
            "swap_accrual_contract": {
                "status": "AVAILABLE",
                "provenance_status": "AUTHENTICATED",
                "execution_state_compounding": True,
                "timestamp_source": "_BAR_CLOSE_TS",
                "source_provenance_authenticated": False,
                "historical_rates_authenticated": False,
                "historical_rates_sha256": "8" * 64,
                "price_activation_manifest_sha256": "1" * 64,
                "terms_activation_manifest_sha256": "2" * 64,
                "terms_snapshot_sha256": "3" * 64,
                "schedule_sha256": "4" * 64,
                "target_bars_sha256": "5" * 64,
                "target_sidecar_sha256": "6" * 64,
                "projection_sha256": "7" * 64,
            },
        },
    }

    assert RegimeOptimizer._admissible_validation_daily_net_mtm(metrics) == ([], [])

    current_snapshot_only = copy.deepcopy(metrics)
    current_snapshot_only["daily_net_mtm_contract"]["swap_accrual_contract"][
        "source_provenance_authenticated"
    ] = True
    current_snapshot_only["daily_net_mtm_contract"]["swap_accrual_contract"][
        "historical_rates_authenticated"
    ] = False
    assert RegimeOptimizer._admissible_validation_daily_net_mtm(
        current_snapshot_only
    ) == ([], [])

    authenticated = copy.deepcopy(metrics)
    authenticated["daily_net_mtm_contract"]["swap_accrual_contract"][
        "source_provenance_authenticated"
    ] = True
    authenticated["daily_net_mtm_contract"]["swap_accrual_contract"][
        "historical_rates_authenticated"
    ] = True
    observed_returns, observed_dates = (
        RegimeOptimizer._admissible_validation_daily_net_mtm(authenticated)
    )
    assert observed_returns == metrics["daily_net_mtm_returns"]
    assert observed_dates == dates

    stressed = copy.deepcopy(metrics)
    stress_contract = stressed["daily_net_mtm_contract"][
        "swap_accrual_contract"
    ]
    stress_contract["provenance_status"] = "STRESS_APPROVED"
    stress_contract["stress_policy_explicitly_approved"] = True
    stress_contract["stress_policy_immutable"] = True
    stress_contract["stress_policy_sha256"] = "9" * 64
    observed_returns, observed_dates = (
        RegimeOptimizer._admissible_validation_daily_net_mtm(stressed)
    )
    assert observed_returns == metrics["daily_net_mtm_returns"]
    assert observed_dates == dates
