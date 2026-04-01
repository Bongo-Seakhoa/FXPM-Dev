import unittest

from pm_core import PipelineConfig


class PipelineConfigFieldTests(unittest.TestCase):
    def test_actionable_score_margin_default(self):
        cfg = PipelineConfig()
        self.assertAlmostEqual(cfg.actionable_score_margin, 0.92, places=6)

    def test_actionable_score_margin_accepts_config_value(self):
        cfg = PipelineConfig(actionable_score_margin=0.9)
        self.assertAlmostEqual(cfg.actionable_score_margin, 0.9, places=6)

    def test_position_timeframe_overrides_default(self):
        cfg = PipelineConfig()
        self.assertIsInstance(cfg.position_timeframe_overrides, dict)
        self.assertEqual(cfg.position_timeframe_overrides, {})

    def test_position_timeframe_overrides_accepts_config_value(self):
        cfg = PipelineConfig(position_timeframe_overrides={"magic:123": "D1"})
        self.assertEqual(cfg.position_timeframe_overrides.get("magic:123"), "D1")


if __name__ == "__main__":
    unittest.main()
