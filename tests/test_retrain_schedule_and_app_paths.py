import shutil
import unittest
from datetime import datetime
from pathlib import Path

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


if __name__ == "__main__":
    unittest.main()
