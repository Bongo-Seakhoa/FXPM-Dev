import unittest

from pm_core import PipelineConfig
from pm_optuna import _stable_seed as optuna_stable_seed
from pm_pipeline import RegimeOptimizer, _stable_seed as pipeline_stable_seed


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


if __name__ == "__main__":
    unittest.main()
