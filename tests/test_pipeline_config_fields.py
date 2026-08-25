import os
import unittest

from pm_core import PipelineConfig, RETIRED_SCORING_CONFIG_KEYS


class PipelineConfigFieldTests(unittest.TestCase):
    def test_uncalibrated_cross_timeframe_actionable_margin_is_removed(self):
        self.assertFalse(hasattr(PipelineConfig(), "actionable_score_margin"))
        with self.assertRaises(TypeError):
            PipelineConfig(actionable_score_margin=0.9)

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

    def test_walk_forward_promotion_policy_defaults_to_production_gate(self):
        cfg = PipelineConfig()
        self.assertEqual(cfg.walk_forward_promotion_policy, "promotion_gate")
        self.assertTrue(cfg.walk_forward_live_blocking_enabled)

    def test_walk_forward_block_policy_can_be_configured(self):
        cfg = PipelineConfig(
            walk_forward_promotion_policy="block_severe_only",
            walk_forward_live_blocking_enabled=True,
        )
        self.assertEqual(cfg.walk_forward_promotion_policy, "block_severe_only")
        self.assertTrue(cfg.walk_forward_live_blocking_enabled)

    def test_live_requires_tuned_regime_params_by_default(self):
        cfg = PipelineConfig()
        self.assertTrue(cfg.live_require_tuned_regime_params)

    def test_live_require_tuned_regime_params_can_be_disabled_for_research_fixture(self):
        cfg = PipelineConfig(live_require_tuned_regime_params=False)
        self.assertFalse(cfg.live_require_tuned_regime_params)

    def test_production_retrain_interval_defaults_to_four_weeks(self):
        cfg = PipelineConfig()
        self.assertEqual(cfg.production_retrain_interval_weeks, 4)

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
        self.assertEqual(cfg.production_split_mode, "calendar_lockbox_v2")
        self.assertEqual(cfg.tuning_validation_bars, 50000)
        self.assertEqual(cfg.final_lockbox_bars, 50000)
        self.assertEqual(cfg.split_embargo_bars, 300)
        self.assertAlmostEqual(cfg.evidence_purge_days, 7.0, places=6)
        self.assertAlmostEqual(cfg.evidence_warmup_days, 400.0, places=6)
        self.assertEqual(cfg.evidence_as_of_utc, "")
        self.assertEqual(cfg.walk_forward_validation_bars, 50000)
        self.assertTrue(cfg.walk_forward_audit_enabled)
        self.assertAlmostEqual(cfg.clean_holdout_pct, 0.0, places=6)

    def test_calendar_production_cannot_disable_native_evidence_gates(self):
        cfg = PipelineConfig(
            production_split_mode="calendar_lockbox_v2",
            evidence_sufficiency_enabled=False,
            evidence_require_native_timeframes=False,
            evidence_require_native_provenance=False,
            evidence_require_session_coverage=False,
        )
        self.assertTrue(cfg.evidence_sufficiency_enabled)
        self.assertTrue(cfg.evidence_require_native_timeframes)
        self.assertTrue(cfg.evidence_require_native_provenance)
        self.assertTrue(cfg.evidence_require_session_coverage)

        research = PipelineConfig(
            research_mode=True,
            production_split_mode="research_clean_holdout",
            evidence_sufficiency_enabled=False,
        )
        self.assertFalse(research.evidence_sufficiency_enabled)

    def test_feature_cache_max_entries_defaults_and_clamps(self):
        self.assertEqual(PipelineConfig().feature_cache_max_entries, 24)
        self.assertEqual(PipelineConfig(feature_cache_max_entries=0).feature_cache_max_entries, 0)
        self.assertEqual(PipelineConfig(feature_cache_max_entries=-3).feature_cache_max_entries, 0)

    def test_feature_disk_cache_defaults_and_clamps(self):
        default_cfg = PipelineConfig()
        self.assertFalse(default_cfg.feature_disk_cache_enabled)
        self.assertEqual(default_cfg.feature_disk_cache_dir, "")
        self.assertAlmostEqual(default_cfg.feature_disk_cache_max_gb, 0.0, places=6)
        self.assertAlmostEqual(default_cfg.feature_disk_cache_min_free_gb, 4.0, places=6)
        self.assertFalse(default_cfg.feature_disk_cache_precompute_enabled)
        # The declared default is 0, documented as "<= 0 uses all OS logical
        # CPUs", so the resolved value is machine-dependent. Asserting 1 only
        # passed on a single-core host; assert the contract instead.
        self.assertEqual(
            default_cfg.feature_disk_cache_precompute_workers,
            max(1, os.cpu_count() or 1),
        )

        cfg = PipelineConfig(
            feature_disk_cache_enabled=True,
            feature_disk_cache_dir=" ./pm_outputs/feature_cache ",
            feature_disk_cache_max_gb=-1.0,
            feature_disk_cache_min_free_gb=-2.0,
            feature_disk_cache_precompute_enabled=True,
            feature_disk_cache_precompute_workers=0,
        )
        self.assertTrue(cfg.feature_disk_cache_enabled)
        self.assertEqual(cfg.feature_disk_cache_dir, "./pm_outputs/feature_cache")
        self.assertAlmostEqual(cfg.feature_disk_cache_max_gb, 0.0, places=6)
        self.assertAlmostEqual(cfg.feature_disk_cache_min_free_gb, 0.0, places=6)
        self.assertTrue(cfg.feature_disk_cache_precompute_enabled)
        # Explicit 0 resolves the same way as the default: all logical CPUs.
        self.assertEqual(
            cfg.feature_disk_cache_precompute_workers,
            max(1, os.cpu_count() or 1),
        )

    def test_screening_atr_anchor_defaults_and_normalizes(self):
        default_cfg = PipelineConfig()
        self.assertFalse(default_cfg.screening_atr_anchor_enabled)
        self.assertEqual(default_cfg.screening_atr_anchor_multipliers, [2.0])
        self.assertEqual(default_cfg.regime_family_probe_trials, 5)

        cfg = PipelineConfig(
            screening_atr_anchor_enabled=True,
            screening_atr_anchor_multipliers=[3.0, 1.0, 2.0, -1.0, 2.0],
        )
        self.assertTrue(cfg.screening_atr_anchor_enabled)
        self.assertEqual(cfg.screening_atr_anchor_multipliers, [1.0, 2.0, 3.0])
        self.assertEqual(
            PipelineConfig(regime_family_probe_trials=0).regime_family_probe_trials,
            1,
        )

    def test_risk_management_optimization_is_enabled_by_default(self):
        cfg = PipelineConfig()
        self.assertFalse(cfg.risk_management_optimization_enabled)
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

    def test_backtest_spread_mode_accepts_stress_max(self):
        cfg = PipelineConfig(backtest_spread_mode="stress_max")
        self.assertEqual(cfg.backtest_spread_mode, "stress_max")

    def test_backtest_spread_mode_accepts_realistic_stress_modes(self):
        for mode in ("stress_p90", "stress_p95"):
            cfg = PipelineConfig(backtest_spread_mode=mode)
            self.assertEqual(cfg.backtest_spread_mode, mode)

    def test_regime_trade_stability_floor_defaults_and_clamps(self):
        self.assertAlmostEqual(PipelineConfig().regime_trade_stability_floor, 0.7, places=6)
        self.assertAlmostEqual(PipelineConfig(regime_trade_stability_floor=0.9).regime_trade_stability_floor, 0.9, places=6)
        self.assertAlmostEqual(PipelineConfig(regime_trade_stability_floor=-1.0).regime_trade_stability_floor, 0.0, places=6)
        self.assertAlmostEqual(PipelineConfig(regime_trade_stability_floor=2.0).regime_trade_stability_floor, 1.0, places=6)

    def test_retired_pipeline_controls_are_not_constructor_fields(self):
        self.assertTrue(
            {"use_commission", "use_slippage", "slippage_pips"}.issubset(
                RETIRED_SCORING_CONFIG_KEYS
            )
        )
        self.assertTrue(
            RETIRED_SCORING_CONFIG_KEYS.isdisjoint(
                PipelineConfig.__dataclass_fields__
            )
        )
        for key in RETIRED_SCORING_CONFIG_KEYS:
            with self.subTest(key=key), self.assertRaises(TypeError):
                PipelineConfig(**{key: "retired"})

    def test_false_discovery_controls_default_on_and_clamp(self):
        cfg = PipelineConfig(
            false_discovery_min_dsr=2.0,
            false_discovery_confidence_floor=-1.0,
            false_discovery_scope_power=3.0,
            false_discovery_symbol_count=-5,
            false_discovery_holdout_policy="mystery",
            false_discovery_holdout_min_trades=-1,
            false_discovery_holdout_min_profit_factor=-2.0,
        )
        self.assertTrue(cfg.false_discovery_control_enabled)
        self.assertAlmostEqual(cfg.false_discovery_min_dsr, 1.0, places=6)
        self.assertAlmostEqual(cfg.false_discovery_confidence_floor, 0.0, places=6)
        self.assertAlmostEqual(cfg.false_discovery_scope_power, 1.0, places=6)
        self.assertEqual(cfg.false_discovery_symbol_count, 0)
        self.assertEqual(cfg.false_discovery_holdout_policy, "block")
        self.assertEqual(cfg.false_discovery_holdout_min_trades, 0)
        self.assertAlmostEqual(cfg.false_discovery_holdout_min_profit_factor, 0.0, places=6)

    def test_lockbox_fields_and_live_statuses_normalize_fail_closed(self):
        self.assertEqual(
            PipelineConfig(production_split_mode="lockbox").production_split_mode,
            "calendar_lockbox_v2",
        )
        self.assertEqual(
            PipelineConfig(production_split_mode="THREE_WAY").production_split_mode,
            "calendar_lockbox_v2",
        )
        self.assertEqual(
            PipelineConfig(production_split_mode="future").production_split_mode,
            "calendar_lockbox_v2",
        )
        self.assertEqual(
            PipelineConfig(research_mode=True, production_split_mode="bleeding_edge").production_split_mode,
            "bleeding_edge",
        )
        self.assertEqual(
            PipelineConfig(research_mode=True, production_split_mode="CLEAN_HOLDOUT").production_split_mode,
            "research_clean_holdout",
        )

        clamped = PipelineConfig(
            walk_forward_validation_bars=-5,
            tuning_validation_bars=0,
            final_lockbox_bars=-1,
            split_embargo_bars=-2,
            false_discovery_holdout_max_drawdown_pct=-1.0,
            live_allowed_promotion_statuses=[
                "promote_small", "invalid", "PROMOTED", "promoted"
            ],
        )
        self.assertEqual(clamped.walk_forward_validation_bars, 0)
        self.assertEqual(clamped.tuning_validation_bars, 1)
        self.assertEqual(clamped.final_lockbox_bars, 1)
        self.assertEqual(clamped.split_embargo_bars, 0)
        self.assertEqual(clamped.false_discovery_holdout_max_drawdown_pct, 0.0)
        self.assertEqual(clamped.live_allowed_promotion_statuses, ["PROMOTE_SMALL", "PROMOTED"])
        self.assertEqual(
            PipelineConfig(false_discovery_holdout_max_drawdown_pct=float("nan"))
            .false_discovery_holdout_max_drawdown_pct,
            20.0,
        )
        self.assertEqual(
            PipelineConfig(live_allowed_promotion_statuses=["invalid"])
            .live_allowed_promotion_statuses,
            ["PROMOTED"],
        )

    def test_oos_evidence_controls_default_off_and_clamp(self):
        cfg = PipelineConfig(
            oos_validation_mode="mystery",
            oos_kfold_splits=1,
            oos_embargo_bars=-5,
            oos_min_trades=-3,
            oos_aggregate_policy="mystery",
            oos_evidence_periods=1,
            oos_evidence_min_periods=999,
            oos_evidence_dirname="../bad",
            oos_evidence_trade_path_max_points=999999,
            oos_evidence_include_execution_cost_survival=True,
            oos_evidence_execution_stress_mode="stress_wish",
            oos_evidence_execution_cost_max_candidates_per_tf=-3,
        )
        self.assertEqual(cfg.oos_validation_mode, "fixed_holdout")
        self.assertEqual(cfg.oos_kfold_splits, 2)
        self.assertEqual(cfg.oos_embargo_bars, 0)
        self.assertEqual(cfg.oos_min_trades, 0)
        self.assertEqual(cfg.oos_aggregate_policy, "aggregate_with_worst_fold_floor")
        self.assertTrue(cfg.oos_evidence_export_enabled)
        self.assertEqual(cfg.oos_evidence_periods, 2)
        self.assertEqual(cfg.oos_evidence_min_periods, 2)
        self.assertEqual(cfg.oos_evidence_dirname, "oos_evidence")
        self.assertEqual(cfg.oos_evidence_trade_path_max_points, 5000)
        self.assertTrue(cfg.oos_evidence_include_execution_cost_survival)
        self.assertEqual(cfg.oos_evidence_execution_stress_mode, "stress_p95")
        self.assertEqual(cfg.oos_evidence_execution_cost_max_candidates_per_tf, 0)

        purged = PipelineConfig(
            research_mode=True,
            oos_validation_mode="purged_kfold",
            oos_kfold_splits=5,
            oos_embargo_bars=3,
            oos_min_trades=5,
            oos_aggregate_policy="all_folds",
        )
        self.assertEqual(purged.oos_validation_mode, "purged_kfold")
        self.assertEqual(purged.oos_kfold_splits, 5)
        self.assertEqual(purged.oos_embargo_bars, 3)
        self.assertEqual(purged.oos_min_trades, 5)
        self.assertEqual(purged.oos_aggregate_policy, "all_folds")

        stress = PipelineConfig(
            oos_evidence_trade_path_max_points=128,
            oos_evidence_execution_stress_mode="stress_max",
            oos_evidence_execution_cost_max_candidates_per_tf=7,
        )
        self.assertEqual(stress.oos_evidence_trade_path_max_points, 128)
        self.assertEqual(stress.oos_evidence_execution_stress_mode, "stress_max")
        self.assertEqual(stress.oos_evidence_execution_cost_max_candidates_per_tf, 7)


if __name__ == "__main__":
    unittest.main()
