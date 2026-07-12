import json
import shutil
import unittest
from pathlib import Path
from unittest.mock import patch

from agent_audit.performance.scripts import run_promoted_cell_replay as replay_tools
from pm_core import PipelineConfig
from pm_pipeline import PortfolioManager, RegimeOptimizer


class PipelineReplayAuditIntegrationTests(unittest.TestCase):
    def setUp(self):
        self.tmp = Path("artifact/fxpm_runtime/.tmp_pytest/test_pipeline_replay_audit_integration")
        shutil.rmtree(self.tmp, ignore_errors=True)
        self.tmp.mkdir(parents=True, exist_ok=True)

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def test_post_optimization_replay_audit_blocks_failed_symbol_and_leaf(self):
        ledger_path = self.tmp / "pm_configs.json"
        ledger_path.write_text(
            json.dumps(
                {
                    "FORD": {
                        "symbol": "FORD",
                        "timeframe": "H4",
                        "strategy_name": "DoubleEMATrendStrategy",
                        "promotion_status": "PROMOTED",
                        "regime_configs": {
                            "H4": {
                                "CHOP": {
                                    "strategy_name": "DoubleEMATrendStrategy",
                                    "parameters": {},
                                    "promotion_status": "PROMOTED",
                                }
                            }
                        },
                    }
                }
            ),
            encoding="utf-8",
        )
        cfg = PipelineConfig(
            data_dir=self.tmp / "data",
            output_dir=self.tmp / "out",
            research_mode=True,
            production_split_mode="bleeding_edge",
            production_replay_audit_enabled=True,
            production_replay_audit_mode="apply",
            production_replay_audit_out_dir=str(self.tmp / "audit"),
        )
        manager = PortfolioManager(cfg, ["FORD"], config_file=str(ledger_path))

        replay_row = {
            "error": "",
            "starting_balance": 10000.0,
            "net_profit": -2317.84,
            "return_pct": -23.18,
            "profit_factor": "0.66",
            "max_drawdown_pct": 24.88,
            "total_trades": 166,
            "expectancy_pips": -10.0,
            "avg_mae_r": 0.74,
        }
        with patch.object(replay_tools, "replay_cell", return_value=replay_row):
            summary = manager._run_production_replay_audit()

        self.assertEqual(summary["fail"], 1)
        self.assertEqual(summary["counters"]["blocked_symbol"], 1)
        self.assertEqual(summary["counters"]["blocked_leaf"], 1)
        self.assertTrue(Path(summary["report_path"]).exists())
        self.assertTrue(Path(summary["backup_path"]).exists())

        updated = json.loads(ledger_path.read_text(encoding="utf-8"))
        self.assertEqual(updated["FORD"]["promotion_status"], "BLOCKED")
        leaf = updated["FORD"]["regime_configs"]["H4"]["CHOP"]
        self.assertEqual(leaf["promotion_status"], "BLOCKED")
        self.assertEqual(leaf["replay_audit"]["verdict"], "FAIL")


class TimeframeValidationFloorTests(unittest.TestCase):
    @staticmethod
    def _candidate(val_trades):
        train = {
            "total_trades": 80,
            "profit_factor": 1.6,
            "total_return_pct": 12.0,
            "max_drawdown_pct": 4.0,
            "sharpe_approx": 1.2,
            "win_rate": 60.0,
        }
        val = {
            "total_trades": val_trades,
            "profit_factor": 1.5,
            "total_return_pct": 10.0,
            "max_drawdown_pct": 4.0,
            "sharpe_approx": 1.1,
            "win_rate": 60.0,
        }
        return {
            "strategy_name": "FloorProbeStrategy",
            "params": {},
            "search_trials": 1,
            "train_regime_metrics": {"TREND": train},
            "val_regime_metrics": {"TREND": val},
        }

    def test_timeframe_aware_floor_blocks_intraday_under_30_trades(self):
        cfg = PipelineConfig(
            research_mode=True,
            false_discovery_control_enabled=False,
            regime_enable_hyperparam_tuning=False,
            regime_min_train_trades=1,
            regime_min_val_trades=15,
            regime_timeframe_val_trade_floors_enabled=True,
            fx_val_max_drawdown=30.0,
        )
        optimizer = RegimeOptimizer(cfg)

        self.assertEqual(optimizer._min_val_trades_for_timeframe("D1"), 15)
        self.assertEqual(optimizer._min_val_trades_for_timeframe("H4"), 25)
        self.assertEqual(optimizer._min_val_trades_for_timeframe("M5"), 30)

        blocked, is_valid, reason = optimizer._select_best_for_regime(
            "EURUSD",
            "M5",
            "TREND",
            [self._candidate(29)],
        )
        self.assertIsNone(blocked)
        self.assertFalse(is_valid)
        self.assertIn("minimum trade", reason)

        accepted, is_valid, _ = optimizer._select_best_for_regime(
            "EURUSD",
            "M5",
            "TREND",
            [self._candidate(30)],
        )
        self.assertIsNotNone(accepted)
        self.assertTrue(is_valid)


if __name__ == "__main__":
    unittest.main()
