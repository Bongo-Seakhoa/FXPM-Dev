import hashlib
import tempfile
import unittest
from pathlib import Path

import pandas as pd

from pm_core import DataLoader, PipelineConfig, DataSplitter
from pm_pipeline import OptimizationPipeline


class DataSplitterTests(unittest.TestCase):
    def test_active_recent_m5_workflow_indices(self):
        cfg = PipelineConfig(
            research_mode=True,
            production_split_mode="bleeding_edge",
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
            research_mode=True,
            production_split_mode="bleeding_edge",
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
            research_mode=True,
            production_split_mode="bleeding_edge",
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
            research_mode=True,
            production_split_mode="bleeding_edge",
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
        cfg = PipelineConfig(research_mode=True, production_split_mode="bleeding_edge")
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
            research_mode=True,
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
            research_mode=True,
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
            research_mode=True,
            production_split_mode="bleeding_edge",
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

    def test_chronological_lockbox_exact_300k_geometry(self):
        cfg = PipelineConfig(
            research_mode=True,
            production_split_mode="chronological_lockbox_v1",
            max_bars=300000,
            historical_stress_audit_bars=50000,
            active_universe_bars=250000,
            tuning_validation_bars=50000,
            final_lockbox_bars=50000,
            split_embargo_bars=300,
            overlap_pct=10.0,
        )

        windows = DataSplitter(cfg).get_workflow_indices(300000)

        self.assertEqual(windows["historical_stress_audit"], (0, 50000))
        self.assertEqual(windows["active_universe"], (50000, 300000))
        self.assertEqual(windows["optimizer_train"], (50000, 199400))
        self.assertEqual(windows["embargo_train_validation"], (199400, 199700))
        self.assertEqual(windows["tuning_validation"], (199700, 249700))
        self.assertEqual(windows["embargo_validation_lockbox"], (249700, 250000))
        self.assertEqual(windows["final_lockbox"], (250000, 300000))
        self.assertEqual(windows["tuning_validation_warmup"], (174700, 199700))
        self.assertEqual(windows["tuning_validation_with_warmup"], (174700, 249700))
        self.assertEqual(windows["final_lockbox_warmup"], (225000, 250000))
        self.assertEqual(windows["final_lockbox_with_warmup"], (225000, 300000))
        self.assertEqual(windows["optimization_train"], windows["optimizer_train"])
        self.assertEqual(windows["walk_forward_validation"], windows["tuning_validation"])
        self.assertEqual(windows["clean_holdout"], windows["final_lockbox"])

    def test_chronological_lockbox_uses_latest_workflow_rows(self):
        cfg = PipelineConfig(research_mode=True, production_split_mode="chronological_lockbox_v1")
        windows = DataSplitter(cfg).get_workflow_indices(400000)

        self.assertEqual(windows["optimizer_train"], (150000, 299400))
        self.assertEqual(windows["tuning_validation"], (299700, 349700))
        self.assertEqual(windows["final_lockbox"], (350000, 400000))

    def test_chronological_lockbox_fails_closed_without_training_row(self):
        cfg = PipelineConfig(
            research_mode=True,
            production_split_mode="chronological_lockbox_v1",
            max_bars=1000,
            active_universe_bars=1000,
            historical_stress_audit_bars=0,
            tuning_validation_bars=50,
            final_lockbox_bars=50,
            split_embargo_bars=5,
        )
        # PipelineConfig clamps active_universe_bars to at least 1000, so use
        # the requested 110/111-row histories against that active cap.
        with self.assertRaisesRegex(ValueError, "requires at least one optimizer-train row"):
            DataSplitter(cfg).get_workflow_indices(110)

        self.assertEqual(DataSplitter(cfg).get_workflow_indices(111)["optimizer_train"], (0, 1))

    def test_resample_tracks_source_bar_bounds_and_excludes_straddlers(self):
        index = pd.date_range("2026-01-01", periods=24, freq="5min")
        source = pd.DataFrame(
            {
                "Open": 1.0,
                "High": 1.1,
                "Low": 0.9,
                "Close": 1.0,
                "Volume": 1,
            },
            index=index,
        )
        resampled = DataLoader(Path("."), cache_resampled=False).resample(source, "H1")

        self.assertEqual(resampled.loc[index[0], "_SOURCE_FIRST_TS"], index[0])
        self.assertEqual(resampled.loc[index[0], "_SOURCE_LAST_TS"], index[11])
        straddling = OptimizationPipeline._slice_time_window(
            resampled, index[0], pd.Timestamp("2026-01-01 00:30")
        )
        clean_second_hour = OptimizationPipeline._slice_time_window(
            resampled,
            pd.Timestamp("2026-01-01 01:00"),
            pd.Timestamp("2026-01-01 02:00"),
        )
        self.assertTrue(straddling.empty)
        self.assertEqual(clean_second_hour.index.tolist(), [pd.Timestamp("2026-01-01 01:00")])

    def test_source_content_digest_invalidates_resample_cache_and_provenance_round_trips(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            path = root / "TEST_M5.csv"
            frame = pd.DataFrame(
                {
                    "time": pd.date_range("2026-01-01", periods=24, freq="5min"),
                    "Open": 1.0,
                    "High": 1.1,
                    "Low": 0.9,
                    "Close": 1.0,
                    "Volume": 1,
                }
            )
            frame.to_csv(path, index=False)
            loader = DataLoader(root, cache_resampled=True, cache_dir=root / "cache")
            source = loader.load_symbol("TEST", "M5")
            self.assertIsNotNone(source)
            source_meta = loader._source_meta["TEST_M5"]
            self.assertEqual(source_meta["size"], path.stat().st_size)
            self.assertEqual(source_meta["sha256"], hashlib.sha256(path.read_bytes()).hexdigest())

            resampled = loader.resample(source, "H1")
            loader._save_resample_cache("TEST", "H1", source_meta, resampled)
            cache_meta = dict(loader._resample_meta["TEST_H1"])
            self.assertTrue(loader._is_resample_cache_valid(cache_meta, source_meta))
            altered = dict(source_meta)
            altered["sha256"] = "0" * 64
            self.assertFalse(loader._is_resample_cache_valid(cache_meta, altered))

            loader._resample_cache.clear()
            loader._resample_meta.clear()
            restored = loader._load_resample_cache("TEST", "H1", source_meta)
            self.assertIsNotNone(restored)
            self.assertIn("_SOURCE_FIRST_TS", restored.columns)
            self.assertIn("_SOURCE_LAST_TS", restored.columns)


if __name__ == "__main__":
    unittest.main()
