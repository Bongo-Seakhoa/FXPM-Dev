import unittest

from pm_core import PipelineConfig, DataSplitter


class DataSplitterTests(unittest.TestCase):
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

    def test_empty_data_returns_zero_windows(self):
        cfg = PipelineConfig()
        splitter = DataSplitter(cfg)
        indices = splitter.get_workflow_indices(0)
        for key in (
            "workflow_window",
            "historical_stress_audit",
            "active_universe",
            "stage2_selection",
            "stage2_selection_with_warmup",
        ):
            self.assertEqual(indices[key], (0, 0))


if __name__ == "__main__":
    unittest.main()
