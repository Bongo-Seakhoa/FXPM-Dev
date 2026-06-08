import unittest

import pandas as pd

from pm_core import PipelineConfig, DataSplitter
from pm_pipeline import OptimizationPipeline


class DataSplitterTests(unittest.TestCase):
    def test_active_recent_m5_workflow_indices(self):
        cfg = PipelineConfig(
            max_bars=300000,
            historical_stress_audit_bars=50000,
            active_universe_bars=250000,
            active_stage2_pct=50.0,
            walk_forward_validation_bars=50000,
            oos_embargo_bars=300,
            overlap_pct=10.0,
        )
        splitter = DataSplitter(cfg)
        indices = splitter.get_workflow_indices(400000)

        self.assertEqual(indices["workflow_window"], (100000, 400000))
        self.assertEqual(indices["historical_stress_audit"], (100000, 150000))
        self.assertEqual(indices["active_universe"], (150000, 400000))
        self.assertEqual(indices["base_selection"], (150000, 400000))
        self.assertEqual(indices["walk_forward_validation"], (224700, 274700))
        self.assertEqual(indices["optimization_train"], (275000, 400000))
        self.assertEqual(indices["optimization_train_with_warmup"], (250000, 400000))
        self.assertEqual(indices["stage2_selection"], (275000, 400000))
        self.assertEqual(indices["stage2_selection_with_warmup"], (250000, 400000))
        self.assertEqual(indices["stage2_training_selection"], (275000, 400000))
        self.assertEqual(indices["stage2_training_selection_with_warmup"], (250000, 400000))
        self.assertEqual(indices["clean_holdout"], (400000, 400000))
        self.assertEqual(indices["clean_holdout_with_warmup"], (400000, 400000))

    def test_bleeding_edge_exact_300k_geometry(self):
        cfg = PipelineConfig(
            max_bars=300000,
            historical_stress_audit_bars=50000,
            active_universe_bars=250000,
            active_stage2_pct=50.0,
            walk_forward_validation_bars=50000,
            oos_embargo_bars=300,
            overlap_pct=10.0,
        )
        indices = DataSplitter(cfg).get_workflow_indices(300000)

        self.assertEqual(indices["historical_stress_audit"], (0, 50000))
        self.assertEqual(indices["active_universe"], (50000, 300000))
        self.assertEqual(indices["base_selection"], (50000, 300000))
        self.assertEqual(indices["walk_forward_validation"], (124700, 174700))
        self.assertEqual(indices["optimization_train"], (175000, 300000))
        self.assertEqual(indices["clean_holdout"], (300000, 300000))

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
        self.assertEqual(indices["clean_holdout"], (600000, 600000))

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
        self.assertEqual(indices["base_selection"], (0, 200000))
        self.assertEqual(indices["optimization_train"], (100000, 200000))
        self.assertEqual(indices["stage2_selection"], (100000, 200000))
        self.assertEqual(indices["stage2_selection_with_warmup"], (80000, 200000))

    def test_empty_data_returns_zero_windows(self):
        cfg = PipelineConfig()
        splitter = DataSplitter(cfg)
        indices = splitter.get_workflow_indices(0)
        for key in (
            "workflow_window",
            "historical_stress_audit",
            "active_universe",
            "base_selection",
            "walk_forward_validation",
            "optimization_train",
            "optimization_train_with_warmup",
            "stage2_selection",
            "stage2_selection_with_warmup",
            "stage2_training_selection",
            "stage2_training_selection_with_warmup",
            "clean_holdout",
            "clean_holdout_with_warmup",
        ):
            self.assertEqual(indices[key], (0, 0))

    def test_clean_holdout_reserves_newest_stage2_rows_when_enabled(self):
        cfg = PipelineConfig(
            max_bars=300000,
            historical_stress_audit_bars=50000,
            active_universe_bars=250000,
            active_stage2_pct=50.0,
            production_split_mode="research_clean_holdout",
            clean_holdout_pct=10.0,
            overlap_pct=10.0,
        )
        splitter = DataSplitter(cfg)
        indices = splitter.get_workflow_indices(400000)

        self.assertEqual(indices["stage2_selection"], (275000, 400000))
        self.assertEqual(indices["optimization_train"], (150000, 387499))
        self.assertEqual(indices["stage2_training_selection"], (275000, 387499))
        self.assertEqual(indices["stage2_training_selection_with_warmup"], (250000, 387499))
        self.assertEqual(indices["clean_holdout"], (387500, 400000))
        self.assertEqual(indices["clean_holdout_with_warmup"], (362500, 400000))

    def test_purged_kfold_mode_does_not_reserve_fixed_clean_holdout(self):
        cfg = PipelineConfig(
            max_bars=300000,
            historical_stress_audit_bars=50000,
            active_universe_bars=250000,
            active_stage2_pct=50.0,
            production_split_mode="research_clean_holdout",
            clean_holdout_pct=10.0,
            oos_validation_mode="purged_kfold",
            overlap_pct=10.0,
        )
        splitter = DataSplitter(cfg)
        indices = splitter.get_workflow_indices(400000)

        self.assertEqual(indices["stage2_selection"], (275000, 400000))
        self.assertEqual(indices["optimization_train"], (150000, 400000))
        self.assertEqual(indices["stage2_training_selection"], (275000, 400000))
        self.assertEqual(indices["stage2_training_selection_with_warmup"], (250000, 400000))
        self.assertEqual(indices["clean_holdout"], (400000, 400000))
        self.assertEqual(indices["clean_holdout_with_warmup"], (400000, 400000))

    def test_pipeline_workflow_meta_carries_optimization_train_window(self):
        cfg = PipelineConfig(
            max_bars=1100,
            historical_stress_audit_bars=100,
            active_universe_bars=1000,
            active_stage2_pct=50.0,
            walk_forward_validation_bars=100,
            oos_embargo_bars=5,
            overlap_pct=10.0,
            feature_disk_cache_enabled=False,
        )
        pipeline = OptimizationPipeline(cfg)
        base_m5 = pd.DataFrame(
            {"Open": 1.0, "High": 1.0, "Low": 1.0, "Close": 1.0, "Volume": 1},
            index=pd.date_range("2026-01-01", periods=1100, freq="5min"),
        )

        meta = pipeline._build_m5_workflow_meta(base_m5)
        windows = meta["windows"]

        self.assertIn("optimization_train", windows)
        self.assertEqual(windows["base_selection"]["start_index"], 100)
        self.assertEqual(windows["base_selection"]["end_index"], 1100)
        self.assertEqual(windows["walk_forward_validation"]["start_index"], 495)
        self.assertEqual(windows["walk_forward_validation"]["end_index"], 595)
        self.assertEqual(windows["optimization_train"]["start_index"], 600)
        self.assertEqual(windows["optimization_train"]["end_index"], 1100)
        self.assertEqual(windows["clean_holdout"]["start_index"], 1100)
        self.assertLess(
            windows["walk_forward_validation"]["end_index"],
            windows["optimization_train"]["start_index"],
        )


if __name__ == "__main__":
    unittest.main()
