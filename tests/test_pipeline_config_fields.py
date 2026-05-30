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

    def test_storage_observe_only_default(self):
        cfg = PipelineConfig()
        self.assertTrue(cfg.storage_observe_only)

    def test_storage_observe_only_accepts_config_value(self):
        cfg = PipelineConfig(storage_observe_only=False)
        self.assertFalse(cfg.storage_observe_only)

    def test_storage_live_sync_overlap_bars_default(self):
        cfg = PipelineConfig()
        self.assertEqual(cfg.storage_live_sync_overlap_bars, 100)

    def test_storage_live_sync_overlap_bars_accepts_config_value(self):
        cfg = PipelineConfig(storage_live_sync_overlap_bars=42)
        self.assertEqual(cfg.storage_live_sync_overlap_bars, 42)

    def test_storage_live_cache_max_age_days_default(self):
        cfg = PipelineConfig()
        self.assertEqual(cfg.storage_live_cache_max_age_days, 7)

    def test_storage_live_cache_max_age_days_accepts_config_value(self):
        cfg = PipelineConfig(storage_live_cache_max_age_days=3)
        self.assertEqual(cfg.storage_live_cache_max_age_days, 3)

    def test_log_dir_accepts_config_value(self):
        cfg = PipelineConfig(log_dir="custom_logs")
        self.assertEqual(str(cfg.log_dir).replace("\\", "/"), "custom_logs")

    def test_local_governance_live_mode_default(self):
        cfg = PipelineConfig()
        self.assertEqual(cfg.local_governance_live_mode, "off")

    def test_local_governance_live_mode_invalid_falls_back_to_off(self):
        cfg = PipelineConfig(local_governance_live_mode="enabled")
        self.assertEqual(cfg.local_governance_live_mode, "off")

    def test_local_governance_candidate_policies_are_normalized(self):
        cfg = PipelineConfig(local_governance_candidate_policies=["control", "pure_atr_runner", "bad"])
        self.assertEqual(cfg.local_governance_candidate_policies, ["control_fixed", "pure_atr"])

    def test_daily_loss_advisory_defaults_off(self):
        cfg = PipelineConfig()
        self.assertEqual(cfg.daily_loss_advisory_pct, 0.0)
        self.assertEqual(cfg.session_loss_advisory_pct, 0.0)

    def test_daily_loss_advisory_accepts_config_values(self):
        cfg = PipelineConfig(daily_loss_advisory_pct=1.25, session_loss_advisory_pct=2.5)
        self.assertAlmostEqual(cfg.daily_loss_advisory_pct, 1.25, places=6)
        self.assertAlmostEqual(cfg.session_loss_advisory_pct, 2.5, places=6)

    def test_active_recent_workflow_defaults(self):
        cfg = PipelineConfig()
        self.assertEqual(cfg.max_bars, 300000)
        self.assertEqual(cfg.historical_stress_audit_bars, 50000)
        self.assertEqual(cfg.active_universe_bars, 250000)
        self.assertAlmostEqual(cfg.active_stage2_pct, 50.0, places=6)
        self.assertAlmostEqual(cfg.clean_holdout_pct, 0.0, places=6)

    def test_risk_management_optimization_is_enabled_by_default(self):
        cfg = PipelineConfig()
        self.assertTrue(cfg.risk_management_optimization_enabled)
        self.assertEqual(cfg.risk_management_selection_stage, "stage3")

    def test_live_loop_trigger_defaults_to_bar(self):
        cfg = PipelineConfig()
        self.assertEqual(cfg.live_loop_trigger_mode, "bar")
        self.assertAlmostEqual(cfg.live_bar_poll_seconds, 0.25, places=6)

    def test_live_loop_trigger_maps_quote_alias_to_bar(self):
        cfg = PipelineConfig(live_loop_trigger_mode="quote")
        self.assertEqual(cfg.live_loop_trigger_mode, "bar")

    def test_live_bar_poll_seconds_has_cpu_idle_floor(self):
        cfg = PipelineConfig(live_bar_poll_seconds=-1.0)
        self.assertAlmostEqual(cfg.live_bar_poll_seconds, 0.05, places=6)
        self.assertAlmostEqual(cfg.live_tick_poll_seconds, 0.05, places=6)

    def test_backtest_spread_mode_defaults_to_fixed_spec(self):
        cfg = PipelineConfig()
        self.assertEqual(cfg.backtest_spread_mode, "fixed_spec")

    def test_backtest_spread_mode_invalid_falls_back_to_fixed_spec(self):
        cfg = PipelineConfig(backtest_spread_mode="wishful")
        self.assertEqual(cfg.backtest_spread_mode, "fixed_spec")

    def test_regime_trade_stability_floor_defaults_and_clamps(self):
        self.assertAlmostEqual(PipelineConfig().regime_trade_stability_floor, 0.7, places=6)
        self.assertAlmostEqual(PipelineConfig(regime_trade_stability_floor=0.9).regime_trade_stability_floor, 0.9, places=6)
        self.assertAlmostEqual(PipelineConfig(regime_trade_stability_floor=-1.0).regime_trade_stability_floor, 0.0, places=6)
        self.assertAlmostEqual(PipelineConfig(regime_trade_stability_floor=2.0).regime_trade_stability_floor, 1.0, places=6)

    def test_false_discovery_controls_default_off_and_clamp(self):
        cfg = PipelineConfig(
            false_discovery_min_dsr=2.0,
            false_discovery_confidence_floor=-1.0,
            false_discovery_scope_power=3.0,
            false_discovery_symbol_count=-5,
            false_discovery_holdout_policy="mystery",
            false_discovery_holdout_min_trades=-1,
            false_discovery_holdout_min_profit_factor=-2.0,
        )
        self.assertFalse(cfg.false_discovery_control_enabled)
        self.assertAlmostEqual(cfg.false_discovery_min_dsr, 1.0, places=6)
        self.assertAlmostEqual(cfg.false_discovery_confidence_floor, 0.0, places=6)
        self.assertAlmostEqual(cfg.false_discovery_scope_power, 1.0, places=6)
        self.assertEqual(cfg.false_discovery_symbol_count, 0)
        self.assertEqual(cfg.false_discovery_holdout_policy, "promote_small")
        self.assertEqual(cfg.false_discovery_holdout_min_trades, 0)
        self.assertAlmostEqual(cfg.false_discovery_holdout_min_profit_factor, 0.0, places=6)


if __name__ == "__main__":
    unittest.main()
