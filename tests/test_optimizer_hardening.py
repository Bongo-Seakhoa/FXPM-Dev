import unittest

import pandas as pd

from pm_core import PipelineConfig
from pm_optuna import OptunaConfig, OptunaTPEOptimizer, _stable_seed as optuna_stable_seed
from pm_pipeline import RegimeConfig, RegimeOptimizer, SymbolConfig, _stable_seed as pipeline_stable_seed


class OptimizerHardeningTests(unittest.TestCase):
    def test_stable_seed_is_deterministic_and_context_sensitive(self):
        seed_a1 = pipeline_stable_seed("EURUSD", "H1", "TRIXSignalStrategy", "grid")
        seed_a2 = pipeline_stable_seed("EURUSD", "H1", "TRIXSignalStrategy", "grid")
        seed_b = pipeline_stable_seed("EURUSD", "H4", "TRIXSignalStrategy", "grid")

        self.assertEqual(seed_a1, seed_a2)
        self.assertNotEqual(seed_a1, seed_b)
        self.assertEqual(
            optuna_stable_seed("EURUSD", "H1", "TRIXSignalStrategy", "grid"),
            optuna_stable_seed("EURUSD", "H1", "TRIXSignalStrategy", "grid"),
        )

    def test_regime_selection_accounts_for_search_trial_breadth(self):
        cfg = PipelineConfig(
            scoring_mode="fx_backtester",
            regime_min_train_trades=1,
            regime_min_val_trades=1,
            regime_min_val_profit_factor=0.0,
            regime_min_val_return_pct=-100.0,
            fx_val_max_drawdown=50.0,
            fx_min_robustness_ratio=0.0,
            fx_val_sharpe_override=-99.0,
        )
        optimizer = RegimeOptimizer(cfg)

        shared_train = {
            "total_trades": 40,
            "profit_factor": 1.8,
            "win_rate": 55.0,
            "sharpe_approx": 1.8,
            "total_return_pct": 12.0,
            "max_drawdown_pct": 8.0,
            "avg_win": 120.0,
            "avg_loss": -70.0,
        }
        shared_val = {
            "total_trades": 25,
            "profit_factor": 1.6,
            "win_rate": 54.0,
            "sharpe_approx": 2.1,
            "total_return_pct": 9.0,
            "max_drawdown_pct": 7.0,
            "avg_win": 110.0,
            "avg_loss": -65.0,
        }

        candidates = [
            {
                "strategy_name": "LowTrialStrategy",
                "params": {},
                "search_trials": 1,
                "train_regime_metrics": {"TREND": dict(shared_train)},
                "val_regime_metrics": {"TREND": dict(shared_val)},
            },
            {
                "strategy_name": "HighTrialStrategy",
                "params": {},
                "search_trials": 200,
                "train_regime_metrics": {"TREND": dict(shared_train)},
                "val_regime_metrics": {"TREND": dict(shared_val)},
            },
        ]

        best_config, is_valid, _ = optimizer._select_best_for_regime(
            "EURUSD",
            "H1",
            "TREND",
            candidates,
        )

        self.assertTrue(is_valid)
        self.assertIsNotNone(best_config)
        self.assertEqual(best_config.strategy_name, "LowTrialStrategy")

    def test_regime_bucket_uses_signal_bar_not_entry_bar(self):
        cfg = PipelineConfig(
            regime_enable_hyperparam_tuning=False,
            regime_min_train_trades=1,
            regime_min_val_trades=1,
        )
        optimizer = RegimeOptimizer(cfg)
        features = pd.DataFrame({"REGIME_LIVE": ["TREND", "RANGE"]})
        trades = [{
            "signal_bar": 0,
            "entry_bar": 1,
            "exit_bar": 2,
            "pnl_dollars": 50.0,
            "pnl_pips": 5.0,
            "direction": "LONG",
            "exit_reason": "test",
        }]

        buckets = optimizer._bucket_trades_by_regime(trades, features)

        self.assertEqual(buckets["TREND"]["total_trades"], 1)
        self.assertEqual(buckets["RANGE"]["total_trades"], 0)

    def test_no_trade_markers_are_not_counted_as_winners(self):
        winner = RegimeConfig(
            strategy_name="SomeStrategy",
            parameters={},
            quality_score=0.9,
            val_metrics={"profit_factor": 2.0, "total_return_pct": 10.0, "max_drawdown_pct": 4.0},
        )
        marker = RegimeConfig(
            strategy_name="NO_TRADE",
            parameters={},
            quality_score=0.0,
        )
        cfg = SymbolConfig(
            symbol="EURUSD",
            regime_configs={"H1": {"TREND": winner, "RANGE": marker}},
        )

        self.assertEqual(cfg.count_regime_winners(), 1)

    def test_regime_grid_fallback_objective_uses_validation_metrics(self):
        class FakeStrategy:
            def __init__(self, p=0):
                self.p = p

            def get_params(self):
                return {"p": self.p}

            def generate_signals(self, features, symbol):
                return pd.Series([1] * len(features), index=features.index)

        class FakeRegistry:
            @staticmethod
            def get(name, **params):
                return FakeStrategy(**params)

        class FakeBacktester:
            def run(self, features, signals, symbol, strategy, warmup_bars=0):
                is_val = bool(features.attrs.get("is_val"))
                if strategy.p == 0:
                    score = 10.0 if not is_val else 100.0
                else:
                    score = 100.0 if not is_val else -100.0
                return {
                    "total_trades": 20,
                    "max_drawdown_pct": 2.0,
                    "trades": [{"score": score}],
                }

        def bucket_trades(trades, features):
            return {
                "TREND": {
                    "total_trades": 20,
                    "max_drawdown_pct": 2.0,
                    "score": trades[0]["score"],
                }
            }

        train = pd.DataFrame({"Close": range(60)})
        val = pd.DataFrame({"Close": range(60)})
        val.attrs["is_val"] = True

        optimizer = OptunaTPEOptimizer(
            OptunaConfig(n_trials=2),
            backtester=FakeBacktester(),
            scorer=None,
            strategy_registry=FakeRegistry(),
        )
        result = optimizer._fallback_regime_search(
            symbol="EURUSD",
            timeframe="H1",
            strategy_name="Fake",
            param_grid={"p": [0, 1]},
            train_features=train,
            val_features=val,
            regimes=["TREND"],
            bucket_trades_fn=bucket_trades,
            compute_score_fn=lambda train_m, val_m: float(val_m.get("score", -999.0)),
            min_train_trades=10,
            max_drawdown_pct=10.0,
        )

        self.assertEqual(result["TREND"]["params"]["p"], 0)


if __name__ == "__main__":
    unittest.main()
