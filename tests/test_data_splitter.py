import unittest
import pandas as pd

from pm_core import PipelineConfig, DataSplitter


class DataSplitterTests(unittest.TestCase):
    def test_split_indices(self):
        cfg = PipelineConfig(train_pct=80.0, overlap_pct=10.0, holdout_pct=10.0)
        splitter = DataSplitter(cfg)
        df = pd.DataFrame({"Open": range(100), "High": range(100), "Low": range(100), "Close": range(100)})
        split = splitter.split(df)
        indices = splitter.get_split_indices(len(df))

        self.assertEqual(indices["train"], (0, 80))
        self.assertEqual(indices["warmup"], (70, 80))
        self.assertEqual(indices["validation"], (80, 90))
        self.assertEqual(indices["validation_with_warmup"], (70, 90))
        self.assertEqual(indices["holdout"], (90, 100))
        self.assertEqual(indices["holdout_with_warmup"], (80, 100))

        self.assertEqual(len(split["train"]), 80)
        self.assertEqual(len(split["validation_with_warmup"]), 20)
        self.assertEqual(len(split["holdout_with_warmup"]), 20)

    def test_val_pct_controls_validation_when_holdout_pct_unset(self):
        cfg = PipelineConfig(train_pct=70.0, val_pct=20.0, overlap_pct=10.0, holdout_pct=None)
        splitter = DataSplitter(cfg)
        indices = splitter.get_split_indices(100)

        self.assertEqual(indices["train"], (0, 70))
        self.assertEqual(indices["validation"], (70, 90))
        self.assertEqual(indices["validation_with_warmup"], (60, 90))
        self.assertEqual(indices["holdout"], (90, 100))
        self.assertEqual(indices["holdout_with_warmup"], (80, 100))

    def test_invalid_val_pct_falls_back_to_balanced_oos_split(self):
        cfg = PipelineConfig(train_pct=80.0, val_pct=30.0, overlap_pct=10.0, holdout_pct=None)
        splitter = DataSplitter(cfg)
        indices = splitter.get_split_indices(100)

        self.assertEqual(indices["validation"], (80, 90))
        self.assertEqual(indices["holdout"], (90, 100))

    def test_active_recent_m5_workflow_indices(self):
        cfg = PipelineConfig(
            max_bars=300000,
            historical_stress_audit_bars=50000,
            active_universe_bars=250000,
            active_stage2_pct=50.0,
            overlap_pct=10.0,
        )
        splitter = DataSplitter(cfg)
        indices = splitter.get_workflow_indices(400000)

        self.assertEqual(indices["workflow_window"], (100000, 400000))
        self.assertEqual(indices["historical_stress_audit"], (100000, 150000))
        self.assertEqual(indices["active_universe"], (150000, 400000))
        self.assertEqual(indices["stage2_selection"], (275000, 400000))
        self.assertEqual(indices["stage2_selection_with_warmup"], (250000, 400000))

    def test_active_recent_m5_does_not_expand_historical_audit_when_max_bars_is_larger(self):
        cfg = PipelineConfig(
            max_bars=500000,
            historical_stress_audit_bars=50000,
            active_universe_bars=250000,
            active_stage2_pct=50.0,
            overlap_pct=10.0,
        )
        splitter = DataSplitter(cfg)
        indices = splitter.get_workflow_indices(600000)

        self.assertEqual(indices["workflow_window"], (300000, 600000))
        self.assertEqual(indices["historical_stress_audit"], (300000, 350000))
        self.assertEqual(indices["active_universe"], (350000, 600000))
        self.assertEqual(indices["stage2_selection"], (475000, 600000))
        self.assertEqual(indices["stage2_selection_with_warmup"], (450000, 600000))

    def test_active_recent_m5_short_history_preserves_newest_active_rows(self):
        cfg = PipelineConfig(
            max_bars=300000,
            historical_stress_audit_bars=50000,
            active_universe_bars=250000,
            active_stage2_pct=50.0,
            overlap_pct=10.0,
        )
        splitter = DataSplitter(cfg)
        indices = splitter.get_workflow_indices(200000)

        self.assertEqual(indices["workflow_window"], (0, 200000))
        self.assertEqual(indices["historical_stress_audit"], (0, 0))
        self.assertEqual(indices["active_universe"], (0, 200000))
        self.assertEqual(indices["stage2_selection"], (100000, 200000))
        self.assertEqual(indices["stage2_selection_with_warmup"], (80000, 200000))


if __name__ == "__main__":
    unittest.main()
