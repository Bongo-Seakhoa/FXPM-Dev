import shutil
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

from pm_core import PipelineConfig
from pm_main import FXPortfolioManagerApp


class RetrainScheduleTests(unittest.TestCase):
    def setUp(self):
        self.tmp = Path("artifact/fxpm_runtime/.tmp_pytest/test_retrain_schedule_and_app_paths")
        shutil.rmtree(self.tmp, ignore_errors=True)
        self.tmp.mkdir(parents=True, exist_ok=True)

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def test_calendar_retrain_schedule_is_anchor_based(self):
        cfg = PipelineConfig(
            data_dir=self.tmp / "data",
            output_dir=self.tmp / "out",
            production_retrain_interval_weeks=2,
            production_retrain_weekday="sunday",
            production_retrain_time="00:01",
            production_retrain_anchor_date="2026-03-29",
        )

        self.assertEqual(
            cfg.get_next_retrain_at(datetime(2026, 3, 29, 0, 0)),
            datetime(2026, 3, 29, 0, 1),
        )
        self.assertEqual(
            cfg.get_next_retrain_at(datetime(2026, 3, 29, 0, 1)),
            datetime(2026, 4, 12, 0, 1),
        )
        self.assertEqual(
            cfg.get_next_retrain_at(datetime(2026, 4, 1, 12, 0)),
            datetime(2026, 4, 12, 0, 1),
        )

    def test_utc_retrain_schedule_is_aware_and_preserves_legacy_naive_api(self):
        cfg = PipelineConfig(
            data_dir=self.tmp / "utc_data",
            output_dir=self.tmp / "utc_out",
            production_retrain_anchor_date="2026-06-07",
        )

        completed_at = datetime(2026, 7, 22, 15, 0, tzinfo=timezone.utc)
        self.assertEqual(
            cfg.get_next_retrain_at_utc(completed_at),
            datetime(2026, 8, 2, 0, 1, tzinfo=timezone.utc),
        )
        self.assertIsNone(
            cfg.get_next_retrain_at(completed_at.replace(tzinfo=None)).tzinfo
        )
        self.assertEqual(
            cfg.get_next_retrain_at_utc(
                completed_at.astimezone(timezone(timedelta(hours=2)))
            ),
            datetime(2026, 8, 2, 0, 1, tzinfo=timezone.utc),
        )
        with self.assertRaisesRegex(ValueError, "aware datetime"):
            cfg.get_next_retrain_at_utc(completed_at.replace(tzinfo=None))

    def test_app_uses_pipeline_paths_as_source_of_truth(self):
        cfg = PipelineConfig(
            data_dir=self.tmp / "cfg_data",
            output_dir=self.tmp / "cfg_out",
        )

        app = FXPortfolioManagerApp(pipeline_config=cfg)

        self.assertEqual(app.data_dir, cfg.data_dir)
        self.assertEqual(app.output_dir, cfg.output_dir)
        self.assertTrue(app.data_dir.exists())
        self.assertTrue(app.output_dir.exists())

    def test_explicit_path_override_updates_pipeline_config(self):
        cfg = PipelineConfig(
            data_dir=self.tmp / "cfg_data_2",
            output_dir=self.tmp / "cfg_out_2",
        )
        override_data = self.tmp / "override_data"
        override_output = self.tmp / "override_output"

        app = FXPortfolioManagerApp(
            pipeline_config=cfg,
            data_dir=str(override_data),
            output_dir=str(override_output),
        )

        self.assertEqual(app.data_dir, override_data)
        self.assertEqual(app.output_dir, override_output)
        self.assertEqual(cfg.data_dir, override_data)
        self.assertEqual(cfg.output_dir, override_output)

    def _scheduled_native_app(self, *, ledger_changed: bool):
        cfg = PipelineConfig(
            data_dir=self.tmp / "scheduled_data",
            output_dir=self.tmp / "scheduled_out",
        )
        cfg.production_retrain_mode = "auto"
        cfg.production_split_mode = "calendar_lockbox_v2"
        cfg._deployment_config_path = "config.json"
        app = FXPortfolioManagerApp(pipeline_config=cfg)
        app.portfolio_manager = MagicMock()
        app.portfolio_manager.get_symbols_needing_retrain.return_value = ["EURUSD"]
        app.portfolio_manager.retrain_all_needed.return_value = {
            "EURUSD": bool(ledger_changed)
        }
        app.portfolio_manager.last_optimization_generation = SimpleNamespace(
            promoted=bool(ledger_changed),
            ledger_changed=bool(ledger_changed)
        )
        app.trader = MagicMock()
        app.storage_manager = MagicMock()
        app._fetch_historical_data = MagicMock()
        return app

    def test_failed_native_generation_leaves_runtime_side_effects_untouched(self):
        app = self._scheduled_native_app(ledger_changed=False)

        app._check_production_retrain_schedule()

        app._fetch_historical_data.assert_not_called()
        app.trader.invalidate_runtime_caches.assert_not_called()
        app.storage_manager.on_optimization_complete.assert_not_called()

    def test_promoted_native_generation_invalidates_runtime_once(self):
        app = self._scheduled_native_app(ledger_changed=True)

        app._check_production_retrain_schedule()

        app._fetch_historical_data.assert_not_called()
        app.trader.invalidate_runtime_caches.assert_called_once_with()
        app.storage_manager.on_optimization_complete.assert_called_once_with()


if __name__ == "__main__":
    unittest.main()
