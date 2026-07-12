import unittest
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pandas as pd

from pm_pipeline import OptimizationPipeline, RegimeConfig


class PipelineLockboxTests(unittest.TestCase):
    def test_candidate_screening_cannot_see_validation_or_lockbox(self) -> None:
        base_start = pd.Timestamp("2020-01-01")
        train_end = pd.Timestamp("2024-01-01")
        base_end = pd.Timestamp("2026-01-01")

        bounds = OptimizationPipeline._candidate_eligibility_bounds(
            "chronological_lockbox_v1",
            base_start,
            base_end,
            base_start,
            train_end,
        )

        self.assertEqual(bounds, (base_start, train_end))

        calendar_bounds = OptimizationPipeline._candidate_eligibility_bounds(
            "calendar_lockbox_v2",
            base_start,
            base_end,
            base_start,
            train_end,
        )
        self.assertEqual(calendar_bounds, (base_start, train_end))

    def test_final_lockbox_backtest_is_gated_to_exact_regime(self) -> None:
        pipeline = OptimizationPipeline.__new__(OptimizationPipeline)
        pipeline.config = SimpleNamespace(lockbox_one_shot_registry_enabled=False)
        optimizer = MagicMock()
        optimizer.backtester.run.return_value = {"trades": []}
        optimizer._compact_backtest_result.return_value = {
            "total_trades": 2,
            "profit_factor": 1.25,
            "max_drawdown_pct": 4.0,
        }
        optimizer._min_val_trades_for_timeframe.return_value = 30
        pipeline.regime_optimizer = optimizer

        index = pd.date_range("2026-01-01", periods=4, freq="h")
        features = pd.DataFrame(
            {
                "open": [1.0] * 4,
                "high": [1.1] * 4,
                "low": [0.9] * 4,
                "close": [1.0] * 4,
                "REGIME_LIVE": ["TREND", "RANGE", "TREND", "CHOP"],
            },
            index=index,
        )
        strategy = MagicMock()
        strategy.generate_signals.return_value = pd.Series([1, -1, 1, -1], index=index)
        winner = RegimeConfig(strategy_name="FrozenWinner", parameters={}, quality_score=1.0)

        with patch("pm_pipeline.StrategyRegistry.get", return_value=strategy), patch(
            "pm_pipeline.block_bootstrap_evidence",
            return_value={"status": "PASS", "passed": True},
        ):
            pipeline._attach_holdout_metrics(
                "EURUSD",
                {"H1": {"TREND": winner}},
                {"H1": features},
                {"H1": 1},
            )

        call = optimizer.backtester.run.call_args
        gated_signals = call.args[1]
        self.assertEqual(gated_signals.tolist(), [1, 0, 1, 0])
        self.assertEqual(call.kwargs["warmup_bars"], 1)
        self.assertEqual(call.kwargs["timeframe"], "H1")
        self.assertEqual(winner.holdout_metrics["validation_mode"], "final_lockbox_regime_gated")
        self.assertEqual(winner.holdout_metrics["target_regime"], "TREND")
        self.assertEqual(winner.holdout_metrics["regime_matching_bars"], 2)
        self.assertEqual(winner.holdout_metrics["min_trades_required"], 30)


if __name__ == "__main__":
    unittest.main()
