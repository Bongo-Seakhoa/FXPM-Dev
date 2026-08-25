"""Causal-prefix regressions for optimizer strategy evaluation surfaces."""

from __future__ import annotations

from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import Mock, patch

import numpy as np
import pandas as pd

import pm_optuna
from pm_core import Backtester, PipelineConfig, StrategyCategory
from pm_optuna import OptunaConfig, OptunaTPEOptimizer
from pm_pipeline import CALENDAR_SPLIT_MODE, OptimizationPipeline, RegimeOptimizer
from pm_search_outcomes import SearchOutcomeKind
from pm_strategies import BaseStrategy, FractalChannelBreakoutStrategy
from pm_trade_power import (
    TradePowerDecision,
    TradePowerPolicy,
    TradePowerRequirement,
    derive_trade_power_requirement,
)


def _typed_pass_power_decision(
    partition: str,
    *,
    frozen_requirement: TradePowerRequirement | None = None,
) -> TradePowerDecision:
    requirement = frozen_requirement
    if requirement is None:
        requirement = replace(
            derive_trade_power_requirement(
                sigma_r_robust=1.0,
                effective_trials=1,
                policy=TradePowerPolicy(
                    minimum_economic_edge_r=1.0,
                    maximum_ci_half_width_r=2.0,
                ),
            ),
            required_effective_train_trades=1,
            required_effective_tuning_trades=1,
            required_effective_final_trades=1,
            frozen=True,
        )
    return TradePowerDecision(
        status="PASS",
        outcome_kind=SearchOutcomeKind.VALID,
        partition=partition,
        reason="typed test evidence satisfies the frozen requirement",
        reason_codes=("POWER_PASS",),
        observed_effective_trades=1.0,
        required_effective_trades=1,
        sample_evidence=None,
        frozen_requirement=requirement,
    )


class _PrefixStateStrategy:
    """Controlled strategy whose signal requires state from an earlier row."""

    name = "PrefixStateStrategy"

    def __init__(self, **params):
        self.params = {"variant": 1, **params}

    def get_params(self):
        return dict(self.params)

    def get_param_grid(self):
        return {"variant": [1]}

    def generate_signals(self, features: pd.DataFrame, _symbol: str) -> pd.Series:
        armed = features["Arm"].fillna(0).cumsum().gt(0)
        return (armed & features["Trigger"].fillna(False)).astype(int)


class _TradeMarkerStrategy(BaseStrategy):
    @property
    def name(self) -> str:
        return "TradeMarkerStrategy"

    @property
    def category(self) -> StrategyCategory:
        return StrategyCategory.BREAKOUT_MOMENTUM

    def get_default_params(self):
        return {"sl_atr_mult": 1.0, "tp_atr_mult": 1.0}

    def generate_signals(self, features: pd.DataFrame, _symbol: str) -> pd.Series:
        return features["Signal"].astype(int)


class _Registry:
    @staticmethod
    def get(_strategy_name: str, **params):
        return _PrefixStateStrategy(**params)


class _BoundaryBacktester:
    """Backtester double exposing exactly which entries enter the metrics."""

    def __init__(self):
        self.calls = []

    def run(
        self,
        features,
        signals,
        _symbol,
        _strategy,
        *,
        warmup_bars=0,
        timeframe=None,
        **_kwargs,
    ):
        warmup = int(warmup_bars or 0)
        # Entries execute one bar after their signal.  A signal on the final
        # prefix bar therefore belongs to the first scored entry bar.
        signal_start = max(0, warmup - 1)
        scored_entries = int(signals.iloc[signal_start:].abs().sum())
        self.calls.append(
            {
                "surface": features.attrs.get("surface"),
                "warmup_bars": warmup,
                "scored_entries": scored_entries,
                "timeframe": timeframe,
            }
        )
        return {
            "total_trades": scored_entries,
            "max_drawdown_pct": 1.0,
            "total_return_pct": float(scored_entries),
            "trades": [],
        }


def _prefix_features(n: int, *, warm_signal: int, scored_signal: int) -> pd.DataFrame:
    index = pd.date_range("2025-01-01", periods=n, freq="h", tz="UTC")
    frame = pd.DataFrame(
        {
            "Close": np.linspace(100.0, 101.0, n),
            "Arm": 0,
            "Trigger": False,
            "REGIME_LIVE": "TREND",
        },
        index=index,
    )
    frame.iloc[1, frame.columns.get_loc("Arm")] = 1
    frame.iloc[warm_signal, frame.columns.get_loc("Trigger")] = True
    frame.iloc[scored_signal, frame.columns.get_loc("Trigger")] = True
    return frame


def test_authenticated_prefix_preserves_fractal_signal_at_scored_boundary() -> None:
    index = pd.date_range("2025-01-01", periods=120, freq="h", tz="UTC")
    close = np.full(len(index), 100.0)
    high = close + 0.5
    low = close - 0.5
    high[40] = 105.0
    close[80] = 106.0
    high[80] = 106.5
    features = pd.DataFrame(
        {
            "Open": close,
            "High": high,
            "Low": low,
            "Close": close,
            "ATR_14": 1.0,
        },
        index=index,
    )
    strategy = FractalChannelBreakoutStrategy(
        wing_bars=1,
        atr_buffer=0.0,
        pivot_max_age_bars=100,
    )

    context, warmup = OptimizationPipeline._strategy_evaluation_context(
        features,
        context_start=index[20],
        evaluation_start=index[80],
        evaluation_end=index[100],
    )
    full_signals = strategy.generate_signals(features, "EURUSD")
    context_signals = strategy.generate_signals(context, "EURUSD")
    truncated_signals = strategy.generate_signals(
        features.loc[index[80]:index[99]], "EURUSD"
    )

    assert warmup == 60
    pd.testing.assert_series_equal(
        full_signals.loc[index[80]:index[99]],
        context_signals.iloc[warmup:],
    )
    assert context_signals.iloc[warmup] == 1
    assert truncated_signals.iloc[0] == 0


def test_regime_backtest_excludes_warmup_entries_from_metrics() -> None:
    features = _prefix_features(30, warm_signal=5, scored_signal=12)
    backtester = _BoundaryBacktester()
    optimizer = RegimeOptimizer.__new__(RegimeOptimizer)
    optimizer.backtester = backtester

    metrics, ran = optimizer._run_one_exact_regime(
        symbol="EURUSD",
        timeframe="H1",
        features=features,
        strategy=_PrefixStateStrategy(),
        regime="TREND",
        warmup_bars=10,
    )

    assert ran is True
    assert metrics["total_trades"] == 1
    assert backtester.calls == [
        {
            "surface": None,
            "warmup_bars": 10,
            "scored_entries": 1,
            "timeframe": "H1",
        }
    ]


def test_real_backtester_excludes_warmup_trade_from_metrics() -> None:
    n = 40
    index = pd.date_range("2025-01-01", periods=n, freq="h", tz="UTC")
    close = np.full(n, 1.1000)
    high = close + 0.0001
    low = close - 0.0001
    # Signals at bars 5, 9, and 20 enter at bars 6, 10, and 21.  With a
    # 10-bar prefix, bar 9 is the legal signal for the first scored entry.
    high[6] = 1.1020
    high[10] = 1.1020
    high[21] = 1.1020
    features = pd.DataFrame(
        {
            "Open": close,
            "High": high,
            "Low": low,
            "Close": close,
            "ATR_14": 0.0005,
            "Signal": 0,
        },
        index=index,
    )
    features.iloc[5, features.columns.get_loc("Signal")] = 1
    features.iloc[9, features.columns.get_loc("Signal")] = 1
    features.iloc[20, features.columns.get_loc("Signal")] = 1
    strategy = _TradeMarkerStrategy()
    signals = strategy.generate_signals(features, "EURUSD")
    backtester = Backtester(
        PipelineConfig(
            use_spread=False,
            swap_model_enabled=False,
        )
    )

    unbounded = backtester.run(
        features, signals, "EURUSD", strategy, timeframe="H1"
    )
    bounded = backtester.run(
        features,
        signals,
        "EURUSD",
        strategy,
        timeframe="H1",
        warmup_bars=10,
    )

    assert unbounded["total_trades"] == 3
    assert bounded["total_trades"] == 2
    assert [trade["entry_bar"] for trade in bounded["trades"]] == [10, 21]


def test_grid_search_threads_train_and_validation_prefixes() -> None:
    train = _prefix_features(70, warm_signal=5, scored_signal=12)
    validation = _prefix_features(70, warm_signal=5, scored_signal=25)
    train.attrs["surface"] = "train"
    validation.attrs["surface"] = "validation"
    backtester = _BoundaryBacktester()
    optimizer = RegimeOptimizer.__new__(RegimeOptimizer)
    optimizer.backtester = backtester
    optimizer.hyperparam_max_combos = 1
    optimizer.min_train_trades = 1
    optimizer.val_max_drawdown = 10.0
    optimizer._last_tuning_search_meta = {}
    optimizer._compute_train_regime_score = (
        lambda train_metrics, _val_metrics: float(train_metrics["total_return_pct"])
    )
    optimizer._calibrate_train_power_result = (
        lambda _result, **_kwargs: _typed_pass_power_decision("optimizer_train")
    )
    optimizer._evaluate_partition_power_result = (
        lambda _result, **kwargs: _typed_pass_power_decision(
            "tuning_validation",
            frozen_requirement=kwargs["frozen_requirement"],
        )
    )
    optimizer._train_trial_economic_ineligibility = lambda _metrics: None

    with patch("pm_pipeline.StrategyRegistry.get", side_effect=_Registry.get):
        candidates = optimizer._tune_strategy_params_grid(
            "EURUSD",
            "H1",
            "PrefixStateStrategy",
            {"variant": [1]},
            train,
            validation,
            train_warmup_bars=10,
            val_warmup_bars=20,
            regimes=["TREND"],
        )

    assert len(candidates) == 1
    assert candidates[0]["train_regime_metrics"]["TREND"]["total_trades"] == 1
    assert candidates[0]["val_regime_metrics"]["TREND"]["total_trades"] == 1
    assert [(call["surface"], call["warmup_bars"]) for call in backtester.calls] == [
        ("train", 10),
        ("validation", 20),
    ]


def test_regime_optimizer_threads_train_prefix_to_optuna_interface() -> None:
    optimizer = RegimeOptimizer.__new__(RegimeOptimizer)
    optimizer.REGIMES = ("TREND", "RANGE", "BREAKOUT", "CHOP")
    optimizer.optuna_optimizer = Mock()
    optimizer.optuna_optimizer.optimize_for_regimes.return_value = {}
    optimizer.optuna_optimizer.last_regime_search_metadata = {}
    optimizer._last_tuning_search_meta = {}
    optimizer.min_train_trades = 1
    optimizer.val_max_drawdown = 10.0

    optimizer._tune_strategy_params(
        "EURUSD",
        "H1",
        "PrefixStateStrategy",
        {"variant": [1]},
        _prefix_features(70, warm_signal=5, scored_signal=12),
        _prefix_features(70, warm_signal=5, scored_signal=25),
        train_warmup_bars=10,
        val_warmup_bars=20,
        regimes=["TREND"],
    )

    kwargs = optimizer.optuna_optimizer.optimize_for_regimes.call_args.kwargs
    assert kwargs["train_warmup_bars"] == 10
    assert kwargs["val_warmup_bars"] == 20


def test_optuna_grid_fallback_preserves_both_prefix_boundaries() -> None:
    train = _prefix_features(70, warm_signal=5, scored_signal=12)
    validation = _prefix_features(70, warm_signal=5, scored_signal=25)
    train.attrs["surface"] = "train"
    validation.attrs["surface"] = "validation"
    backtester = _BoundaryBacktester()
    optimizer = OptunaTPEOptimizer(
        OptunaConfig(n_trials=1),
        backtester,
        scorer=SimpleNamespace(),
        strategy_registry=_Registry(),
    )

    with patch.object(pm_optuna, "OPTUNA_AVAILABLE", False):
        candidates = optimizer.optimize_for_regimes(
            symbol="EURUSD",
            timeframe="H1",
            strategy_name="PrefixStateStrategy",
            param_grid={"variant": [1]},
            train_features=train,
            val_features=validation,
            regimes=["TREND"],
            bucket_trades_fn=lambda *_args: {},
            compute_score_fn=lambda train_metrics, _val: float(
                train_metrics["total_return_pct"]
            ),
            min_train_trades=1,
            max_drawdown_pct=10.0,
            train_warmup_bars=10,
            val_warmup_bars=20,
            calibrate_train_power_fn=(
                lambda _result, **_kwargs: _typed_pass_power_decision(
                    "optimizer_train"
                )
            ),
            evaluate_validation_power_fn=(
                lambda _result, **kwargs: _typed_pass_power_decision(
                    "tuning_validation",
                    frozen_requirement=kwargs["frozen_requirement"],
                )
            ),
            train_economic_gate_fn=lambda _metrics: None,
        )

    assert candidates["TREND"]["train_regime_metrics"]["TREND"]["total_trades"] == 1
    assert candidates["TREND"]["val_regime_metrics"]["TREND"]["total_trades"] == 1
    assert [(call["surface"], call["warmup_bars"]) for call in backtester.calls] == [
        ("train", 10),
        ("validation", 20),
    ]


def test_phase_a_feature_input_still_excludes_final_lockbox_rows() -> None:
    features = _prefix_features(100, warm_signal=5, scored_signal=12)
    cutoff = features.index[80]

    phase_a = OptimizationPipeline._selection_feature_input(
        features,
        split_mode=CALENDAR_SPLIT_MODE,
        final_lockbox_start=cutoff,
    )

    assert len(phase_a) == 80
    assert phase_a.index.max() < cutoff
