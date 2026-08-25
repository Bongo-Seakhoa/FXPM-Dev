import unittest
import hashlib
from dataclasses import replace
from unittest.mock import patch

import pandas as pd

import pm_optuna
from pm_optuna import OptunaConfig, OptunaTPEOptimizer
from pm_pipeline import RegimeOptimizer
from pm_search_outcomes import SearchOutcomeKind
from pm_trade_power import (
    REQUIRED_TRADE_RECORD_SCHEMA,
    TradePowerDecision,
    TradePowerPolicy,
    build_numerical_execution_cost_attestation,
    derive_trade_power_requirement,
)


def _with_authenticated_trade_evidence(payload, *, start_utc="2024-01-01"):
    result = dict(payload)
    start = pd.Timestamp(start_utc, tz="UTC")
    result["trades"] = [
        {
            "position_id": f"fixture-position-{index:04d}",
            "entry_event_order": index * 2,
            "exit_event_order": index * 2 + 1,
            "pnl_dollars": 100.0 if index % 2 else -50.0,
            "risk_amount": 100.0,
            "entry_time": start + pd.Timedelta(hours=2 * index),
            "exit_time": start + pd.Timedelta(hours=2 * index + 1),
            "trade_record_schema_version": REQUIRED_TRADE_RECORD_SCHEMA,
        }
        for index in range(320)
    ]
    result["total_trades"] = len(result["trades"])
    result["execution_cost_timing_contract"] = {
        "swap_accrual": "causal_swap_accrual_v1",
    }
    result["numerical_execution_cost_attestation"] = (
        build_numerical_execution_cost_attestation(
            use_spread=True,
            spread_mode="conservative_max",
            spread_values_sha256="7" * 64,
            instrument_spec_sha256="9" * 64,
        )
    )
    result["swap_accrual_contract"] = {
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
            f"regime-gate:{start.isoformat()}".encode("ascii")
        ).hexdigest(),
    }
    return result


def _power_callbacks():
    evaluator = RegimeOptimizer.__new__(RegimeOptimizer)
    evaluator._trade_power_multiplicity_cache = {}
    return (
        evaluator._calibrate_train_power_result,
        evaluator._evaluate_partition_power_result,
    )


def _passing_power_decision(
    partition: str = "optimizer_train",
    *,
    frozen_requirement=None,
) -> TradePowerDecision:
    requirement = frozen_requirement or replace(
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
        reason="focused selection fixture passed",
        reason_codes=(),
        observed_effective_trades=100.0,
        required_effective_trades=1,
        sample_evidence=None,
        frozen_requirement=requirement,
    )


class RegimeGatedSelectionTests(unittest.TestCase):
    def test_screening_gate_fails_closed_without_regime_column(self) -> None:
        features = pd.DataFrame({"Close": range(3)})
        gated = RegimeOptimizer._gate_signals_to_exact_regime(
            pd.Series([1, -1, 1]), features, "TREND"
        )
        self.assertEqual(gated.tolist(), [0, 0, 0])

    def test_bucket_conversion_uses_pips_not_dollar_aliases(self) -> None:
        optimizer = RegimeOptimizer.__new__(RegimeOptimizer)
        converted = optimizer._bucket_to_full_metrics(
            {
                "win_rate": 50.0,
                "avg_win": 100.0,
                "avg_loss": 25.0,
                "avg_win_pips": 4.0,
                "avg_loss_pips": 2.0,
                "sharpe_ratio": 1.25,
                "sortino_ratio": 1.75,
            }
        )
        self.assertEqual(converted["expectancy_pips"], 1.0)
        self.assertEqual(converted["sharpe_ratio"], 1.25)
        self.assertEqual(converted["sortino_ratio"], 1.75)

    def test_other_regime_direction_cannot_change_trend_screening_metrics(self) -> None:
        class Strategy:
            def __init__(self, chop_direction):
                self.chop_direction = chop_direction

            def generate_signals(self, features, symbol):
                return pd.Series(
                    [
                        1 if regime == "TREND" else self.chop_direction
                        for regime in features["REGIME_LIVE"]
                    ],
                    index=features.index,
                )

        class Backtester:
            def run(self, features, signals, symbol, strategy, **kwargs):
                total = int(signals.ne(0).sum())
                edge = float(signals.sum())
                return {
                    "total_trades": total,
                    "profit_factor": 1.2,
                    "total_return_pct": edge,
                    "max_drawdown_pct": 1.0,
                    "sharpe_ratio": edge / max(total, 1),
                    "trades": [],
                }

        optimizer = RegimeOptimizer.__new__(RegimeOptimizer)
        optimizer.backtester = Backtester()
        features = pd.DataFrame(
            {
                "Close": range(60),
                "REGIME_LIVE": ["TREND", "CHOP"] * 30,
            }
        )
        _, positive = optimizer._run_exact_regime_screening(
            symbol="EURUSD", timeframe="H1", features=features,
            strategy=Strategy(1),
        )
        _, negative = optimizer._run_exact_regime_screening(
            symbol="EURUSD", timeframe="H1", features=features,
            strategy=Strategy(-1),
        )

        self.assertEqual(positive["TREND"], negative["TREND"])
        self.assertNotEqual(
            positive["CHOP"]["total_return_pct"],
            negative["CHOP"]["total_return_pct"],
        )

    def test_default_screening_runs_independent_exact_regime_paths(self) -> None:
        class Strategy:
            def generate_signals(self, features, symbol):
                return pd.Series(1, index=features.index, dtype=int)

        class Backtester:
            def __init__(self):
                self.active_regimes = []

            def run(self, features, signals, symbol, strategy, **kwargs):
                active = set(features.loc[signals.ne(0), "REGIME_LIVE"].tolist())
                self.active_regimes.append(active)
                return {
                    "total_trades": int(signals.ne(0).sum()),
                    "profit_factor": 1.2,
                    "total_return_pct": 1.0,
                    "max_drawdown_pct": 1.0,
                    "trades": [],
                    "equity_curve": [10000.0, 10100.0],
                }

        optimizer = RegimeOptimizer.__new__(RegimeOptimizer)
        optimizer.backtester = Backtester()
        regimes = ["TREND", "RANGE"] * 30
        features = pd.DataFrame({"Close": range(60), "REGIME_LIVE": regimes})

        _, metrics = optimizer._run_exact_regime_screening(
            symbol="EURUSD",
            timeframe="H1",
            features=features,
            strategy=Strategy(),
        )

        self.assertEqual(optimizer.backtester.active_regimes, [{"TREND"}, {"RANGE"}])
        self.assertEqual(metrics["TREND"]["total_trades"], 30)
        self.assertEqual(metrics["RANGE"]["total_trades"], 30)
        self.assertEqual(metrics["BREAKOUT"]["total_trades"], 0)
        self.assertTrue(metrics["TREND"]["regime_gated"])

    def test_grid_search_backtests_each_regime_with_exact_entry_gate(self) -> None:
        class Strategy:
            def __init__(self, p=1):
                self.p = p

            def get_params(self):
                return {"p": self.p}

            def generate_signals(self, features, symbol):
                return pd.Series(1, index=features.index, dtype=int)

        class Registry:
            @staticmethod
            def get(name, **params):
                return Strategy(**params)

        class Backtester:
            def __init__(self):
                self.calls = []

            def run(self, features, signals, symbol, strategy, **kwargs):
                active = features.loc[signals.ne(0), "REGIME_LIVE"]
                self.calls.append(set(active.tolist()))
                return _with_authenticated_trade_evidence(
                    {
                        "total_trades": int(signals.ne(0).sum()),
                        "max_drawdown_pct": 1.0,
                        "profit_factor": 1.2,
                        "total_return_pct": 1.0,
                        "trades": [],
                    },
                    start_utc=(
                        "2024-01-01"
                        if features.attrs["surface"] == "train"
                        else "2024-06-01"
                    ),
                )

        regimes = (["TREND", "RANGE"] * 30)
        train = pd.DataFrame({"Close": range(60), "REGIME_LIVE": regimes})
        val = pd.DataFrame({"Close": range(60), "REGIME_LIVE": regimes})
        train.attrs["surface"] = "train"
        val.attrs["surface"] = "validation"
        backtester = Backtester()
        optimizer = OptunaTPEOptimizer(
            OptunaConfig(n_trials=1),
            backtester=backtester,
            scorer=None,
            strategy_registry=Registry(),
        )

        calibrate_power, validate_power = _power_callbacks()
        result = optimizer._fallback_regime_search(
            symbol="EURUSD",
            timeframe="H1",
            strategy_name="Strategy",
            param_grid={"p": [1]},
            train_features=train,
            val_features=val,
            regimes=["TREND", "RANGE"],
            bucket_trades_fn=lambda *args, **kwargs: self.fail("post-hoc bucketing must not run"),
            compute_score_fn=lambda train_m, val_m: float(train_m["profit_factor"]),
            min_train_trades=5,
            max_drawdown_pct=10.0,
            calibrate_train_power_fn=calibrate_power,
            evaluate_validation_power_fn=validate_power,
            train_economic_gate_fn=lambda _metrics: None,
        )

        self.assertEqual(set(result), {"TREND", "RANGE"})
        self.assertTrue(all(candidate["regime_gated"] for candidate in result.values()))
        self.assertEqual(backtester.calls, [{"TREND"}, {"TREND"}, {"RANGE"}, {"RANGE"}])
        self.assertEqual(optimizer.last_regime_search_metadata["cell_count"], 2)
        self.assertEqual(optimizer.last_regime_search_metadata["search_trials_total"], 2)
        self.assertEqual(
            optimizer.last_regime_search_metadata["tuning_validation_evaluations"], 2
        )

    @unittest.skipIf(pm_optuna.optuna is None, "optuna not available")
    def test_optuna_uses_four_train_only_studies_then_one_frozen_validation_each(self) -> None:
        class Strategy:
            def __init__(self, p=1):
                self.p = p

            def get_params(self):
                return {"p": self.p}

            def generate_signals(self, features, symbol):
                return pd.Series(1, index=features.index, dtype=int)

        class Registry:
            @staticmethod
            def get(name, **params):
                return Strategy(**params)

        class Backtester:
            def __init__(self):
                self.events = []

            def run(self, features, signals, symbol, strategy, **kwargs):
                active = set(features.loc[signals.ne(0), "REGIME_LIVE"].tolist())
                self.events.append((features.attrs["surface"], active))
                return _with_authenticated_trade_evidence(
                    {
                        "total_trades": int(signals.ne(0).sum()),
                        "max_drawdown_pct": 1.0,
                        "profit_factor": 1.2,
                        "total_return_pct": 1.0,
                        "trades": [],
                    },
                    start_utc=(
                        "2024-01-01"
                        if features.attrs["surface"] == "train"
                        else "2024-06-01"
                    ),
                )

        regime_values = ["TREND", "RANGE", "BREAKOUT", "CHOP"] * 20
        train = pd.DataFrame({"Close": range(80), "REGIME_LIVE": regime_values})
        val = pd.DataFrame({"Close": range(80), "REGIME_LIVE": regime_values})
        train.attrs["surface"] = "train"
        val.attrs["surface"] = "validation"
        backtester = Backtester()
        optimizer = OptunaTPEOptimizer(
            OptunaConfig(n_trials=1, n_startup_trials=0, progressive_rejection=False),
            backtester=backtester,
            scorer=None,
            strategy_registry=Registry(),
        )
        real_create_study = pm_optuna.optuna.create_study
        calibrate_power, validate_power = _power_callbacks()
        with patch("pm_optuna.optuna.create_study", wraps=real_create_study) as create_study:
            result = optimizer.optimize_for_regimes(
                symbol="EURUSD",
                timeframe="H1",
                strategy_name="Strategy",
                param_grid={"p": [1]},
                train_features=train,
                val_features=val,
                regimes=["TREND", "RANGE", "BREAKOUT", "CHOP"],
                bucket_trades_fn=lambda *args, **kwargs: self.fail("no post-hoc buckets"),
                compute_score_fn=lambda train_m, _val_m: float(train_m["profit_factor"]),
                min_train_trades=5,
                max_drawdown_pct=10.0,
                val_warmup_bars=3,
                calibrate_train_power_fn=calibrate_power,
                evaluate_validation_power_fn=validate_power,
                train_economic_gate_fn=lambda _metrics: None,
            )

        self.assertEqual(create_study.call_count, 4)
        self.assertEqual(set(result), {"TREND", "RANGE", "BREAKOUT", "CHOP"})
        self.assertEqual(
            [surface for surface, _active in backtester.events],
            ["train", "validation"] * 4,
        )
        self.assertTrue(all(len(active) == 1 for _surface, active in backtester.events))
        metadata = optimizer.last_regime_search_metadata
        self.assertEqual(metadata["cell_count"], 4)
        self.assertEqual(metadata["search_trials_total"], 4)
        self.assertEqual(metadata["tuning_validation_evaluations"], 4)

    def test_pipeline_grid_cells_freeze_train_best_before_validation(self) -> None:
        class Strategy:
            name = "GridFamily"

            def __init__(self, p=0):
                self.p = int(p)

            def get_params(self):
                return {"p": self.p}

            def generate_signals(self, features, symbol):
                return pd.Series(1, index=features.index, dtype=int)

        class Backtester:
            def __init__(self):
                self.events = []

            def run(self, features, signals, symbol, strategy, **kwargs):
                surface = features.attrs["surface"]
                self.events.append((surface, strategy.p))
                score = float(strategy.p if surface == "train" else -strategy.p)
                return _with_authenticated_trade_evidence({
                    "total_trades": int(signals.ne(0).sum()),
                    "max_drawdown_pct": 1.0,
                    "score": score,
                    "trades": [],
                })

        regimes = ["TREND", "RANGE", "BREAKOUT", "CHOP"] * 20
        train = pd.DataFrame({"Close": range(80), "REGIME_LIVE": regimes})
        val = pd.DataFrame({"Close": range(80), "REGIME_LIVE": regimes})
        train.attrs["surface"] = "train"
        val.attrs["surface"] = "validation"
        optimizer = RegimeOptimizer.__new__(RegimeOptimizer)
        optimizer.backtester = Backtester()
        optimizer.hyperparam_max_combos = 2
        optimizer.min_train_trades = 1
        optimizer.val_max_drawdown = 10.0
        optimizer._last_tuning_search_meta = {}
        optimizer._compute_train_regime_score = lambda train_m, _val_m: float(train_m["score"])
        optimizer._calibrate_train_power_result = (
            lambda *_args, **_kwargs: _passing_power_decision()
        )
        optimizer._evaluate_partition_power_result = (
            lambda *_args, **kwargs: _passing_power_decision(
                "tuning_validation",
                frozen_requirement=kwargs["frozen_requirement"],
            )
        )
        optimizer._train_trial_economic_ineligibility = lambda _metrics: None
        with patch("pm_pipeline.StrategyRegistry.get", side_effect=lambda _name, **params: Strategy(**params)):
            candidates = optimizer._tune_strategy_params_grid(
                "EURUSD",
                "H1",
                "GridFamily",
                {"p": [0, 1]},
                train,
                val,
                regimes=["TREND", "RANGE", "BREAKOUT", "CHOP"],
            )

        self.assertEqual(len(candidates), 4)
        self.assertTrue(all(candidate["params"]["p"] == 1 for candidate in candidates))
        self.assertEqual(
            optimizer.backtester.events,
            [("train", 0), ("train", 1), ("validation", 1)] * 4,
        )
        self.assertEqual(optimizer._last_tuning_search_meta["cell_count"], 4)
        self.assertEqual(optimizer._last_tuning_search_meta["search_trials_total"], 8)
        self.assertEqual(
            optimizer._last_tuning_search_meta["tuning_validation_evaluations"], 4
        )

    def test_equal_budget_b0_probe_rescues_bad_default_strong_tuned_family(self) -> None:
        class Strategy:
            def __init__(self, name, p=0):
                self.name = name
                self.p = int(p)

            def get_params(self):
                return {"p": self.p}

            def get_param_grid(self):
                return {"p": [0, 1]}

            def generate_signals(self, features, symbol):
                return pd.Series(1, index=features.index, dtype=int)

        class Backtester:
            def run(self, features, signals, symbol, strategy, **kwargs):
                if strategy.name == "BadDefaultStrongRegion":
                    score = 0.0 if strategy.p == 0 else 100.0
                else:
                    score = 50.0 if strategy.p == 0 else 40.0
                return _with_authenticated_trade_evidence({
                    "total_trades": int(signals.ne(0).sum()),
                    "max_drawdown_pct": 1.0,
                    "score": score,
                    "trades": [],
                })

        optimizer = RegimeOptimizer.__new__(RegimeOptimizer)
        optimizer.family_probe_trials = 2
        optimizer.screening_atr_anchor_enabled = False
        optimizer.screening_atr_anchor_multipliers = [2.0]
        optimizer.min_train_trades = 1
        optimizer.backtester = Backtester()
        optimizer._compute_train_regime_score = lambda metrics, _val: float(metrics["score"])
        optimizer._calibrate_train_power_result = (
            lambda *_args, **_kwargs: _passing_power_decision()
        )
        optimizer._train_trial_economic_ineligibility = lambda _metrics: None
        features = pd.DataFrame({
            "Close": range(80),
            "REGIME_LIVE": ["TREND", "RANGE", "BREAKOUT", "CHOP"] * 20,
        })
        families = [
            Strategy("BadDefaultStrongRegion", p=0),
            Strategy("GoodDefaultWeakRegion", p=0),
        ]

        def make_strategy(name, **params):
            return Strategy(name, **params)

        with patch("pm_pipeline.StrategyRegistry.get", side_effect=make_strategy):
            candidates, cells = optimizer._collect_family_probe_candidates(
                symbol="EURUSD",
                timeframe="H1",
                train_features=features,
                strategies=families,
            )
        top = optimizer._identify_top_strategies_per_regime(candidates, top_k=1)

        self.assertEqual(len(cells), 8)
        self.assertTrue(all(cell["search_trials"] == 2 for cell in cells))
        self.assertTrue(all(names == ["BadDefaultStrongRegion"] for names in top.values()))
        rescued = [
            candidate for candidate in candidates
            if candidate["strategy_name"] == "BadDefaultStrongRegion"
        ]
        self.assertTrue(rescued)
        self.assertTrue(all(candidate["params"]["p"] == 1 for candidate in rescued))


if __name__ == "__main__":
    unittest.main()
