import unittest
from types import SimpleNamespace

from pm_enhancement_seams import (
    ExecutionQualityContext,
    PortfolioObservationContext,
    PortfolioObservatory,
    RiskScalarContext,
    RiskScalarStack,
    SpreadAwareExecutionOverlay,
    VolatilityTargetScalar,
    StrategyInsertionSpec,
    create_default_enhancement_seams,
)


class DummyStrategy:
    pass


class EnhancementSeamsTests(unittest.TestCase):
    def test_default_risk_scalar_stack_is_no_op(self):
        seams = create_default_enhancement_seams()
        context = RiskScalarContext(
            symbol="EURUSD",
            timeframe="H1",
            regime="TREND",
            base_risk_pct=1.0,
        )
        self.assertEqual(seams.risk_scalar_stack.apply(1.0, context), 1.0)

    def test_strategy_extension_registry_accepts_specs(self):
        seams = create_default_enhancement_seams()
        spec = StrategyInsertionSpec(
            name="DummyExtensionStrategy",
            strategy_cls=DummyStrategy,
            required_features=["ATR_14"],
        )
        seams.strategy_extension_registry.register(spec)
        names = [item.name for item in seams.strategy_extension_registry.list_specs()]
        self.assertIn("DummyExtensionStrategy", names)

    def test_volatility_target_scalar_uses_price_not_equity(self):
        scalar = VolatilityTargetScalar(target_annual_vol=0.10, min_scalar=0.3, max_scalar=2.0)

        high_vol = RiskScalarContext(
            symbol="EURUSD",
            timeframe="H1",
            regime="TREND",
            base_risk_pct=1.0,
            current_atr=0.0100,
            current_price=1.1000,
        )
        low_vol = RiskScalarContext(
            symbol="EURUSD",
            timeframe="H1",
            regime="TREND",
            base_risk_pct=1.0,
            current_atr=0.0002,
            current_price=1.1000,
        )

        self.assertLess(scalar.apply(1.0, high_vol), 1.0)
        self.assertGreaterEqual(scalar.apply(1.0, low_vol), 1.0)

    def test_spread_overlay_soft_penalty_is_reachable(self):
        overlay = SpreadAwareExecutionOverlay(
            min_edge_mult=1.5,
            spike_mult=2.0,
            penalty_start_mult=0.5,
            enabled=True,
        )
        decision = overlay.evaluate(
            ExecutionQualityContext(
                symbol="EURUSD",
                timeframe="H1",
                spread_pips=1.0,
                atr_pips=1.8,
                rolling_spread_median=0.8,
            )
        )

        self.assertTrue(decision.allow_trade)
        self.assertLess(decision.score_multiplier, 1.0)
        self.assertTrue(any("Spread penalty" in note for note in decision.notes))

    def test_spread_overlay_blocks_spread_spike(self):
        overlay = SpreadAwareExecutionOverlay(enabled=True)
        decision = overlay.evaluate(
            ExecutionQualityContext(
                symbol="EURUSD",
                timeframe="H1",
                spread_pips=3.0,
                atr_pips=10.0,
                rolling_spread_median=1.0,
            )
        )

        self.assertFalse(decision.allow_trade)
        self.assertEqual(decision.score_multiplier, 0.0)

    def test_spread_overlay_penalty_does_not_saturate_at_high_ratios(self):
        """Findings.html §9: penalty must keep decaying past the old 0.5 floor.

        Old implementation: max(0.5, 1 - delta) → flat at 0.5 once delta > 0.5.
        New implementation: max(0.25, 1 - 0.4*delta) → decays further until 0.25.
        """
        # Disable min-edge and spike filters so we isolate the soft-penalty path.
        overlay = SpreadAwareExecutionOverlay(
            min_edge_mult=0.05,
            spike_mult=999.0,
            penalty_start_mult=0.5,
            enabled=True,
        )
        # ratio = 0.8 → delta = 0.3 → expected 1 - 0.4*0.3 = 0.88
        moderate = overlay.evaluate(
            ExecutionQualityContext(
                symbol="EURUSD", timeframe="H1",
                spread_pips=1.6, atr_pips=2.0, rolling_spread_median=1.0,
            )
        )
        # ratio = 1.4 → delta = 0.9 → expected 1 - 0.4*0.9 = 0.64
        # Old curve would have returned 0.5 (saturated).
        severe = overlay.evaluate(
            ExecutionQualityContext(
                symbol="EURUSD", timeframe="H1",
                spread_pips=2.8, atr_pips=2.0, rolling_spread_median=1.0,
            )
        )
        self.assertTrue(moderate.allow_trade)
        self.assertTrue(severe.allow_trade)
        self.assertAlmostEqual(moderate.score_multiplier, 0.88, places=2)
        self.assertAlmostEqual(severe.score_multiplier, 0.64, places=2)
        # Severity must continue to decay beyond the old 0.5 floor.
        self.assertLess(severe.score_multiplier, moderate.score_multiplier)

    def test_spread_overlay_penalty_floor_at_quarter(self):
        overlay = SpreadAwareExecutionOverlay(
            min_edge_mult=0.1,  # disable min-edge filter
            spike_mult=999.0,    # disable spike filter
            penalty_start_mult=0.5,
            enabled=True,
        )
        # very high ratio → would be deeply negative without the floor
        decision = overlay.evaluate(
            ExecutionQualityContext(
                symbol="EURUSD", timeframe="H1",
                spread_pips=20.0, atr_pips=2.0, rolling_spread_median=1.0,
            )
        )
        self.assertGreaterEqual(decision.score_multiplier, 0.25 - 1e-9)

    def test_default_seams_respect_configured_spread_thresholds(self):
        config = SimpleNamespace(
            execution_spread_filter_enabled=True,
            execution_spread_min_edge_mult=1.25,
            execution_spread_spike_mult=5.0,
            execution_spread_penalty_start_mult=0.4,
        )
        seams = create_default_enhancement_seams(config)
        overlay = seams.execution_quality_overlay

        self.assertIsInstance(overlay, SpreadAwareExecutionOverlay)
        self.assertTrue(overlay.enabled)
        self.assertEqual(overlay.min_edge_mult, 1.25)
        self.assertEqual(overlay.spike_mult, 5.0)
        self.assertEqual(overlay.penalty_start_mult, 0.4)

    def test_portfolio_observatory_reports_clusters_without_mutation(self):
        positions = [
            SimpleNamespace(symbol="EURUSD"),
            SimpleNamespace(symbol="EURJPY"),
            SimpleNamespace(symbol="XAUUSD"),
        ]
        observatory = PortfolioObservatory(enabled=True)
        snapshot = observatory.snapshot(
            PortfolioObservationContext(
                positions=positions,
                estimated_risk_by_symbol={"EURUSD": 0.5, "EURJPY": 0.75, "XAUUSD": 1.0},
            )
        )
        self.assertTrue(snapshot["enabled"])
        self.assertEqual(snapshot["open_positions"], 3)
        self.assertEqual(snapshot["symbols_with_positions"], 3)
        self.assertTrue(any(item["cluster"] == "EUR" for item in snapshot["clusters"]))
        self.assertEqual([pos.symbol for pos in positions], ["EURUSD", "EURJPY", "XAUUSD"])

    def test_default_seams_build_portfolio_observatory_from_config(self):
        config = SimpleNamespace(portfolio_observatory_enabled=True)
        seams = create_default_enhancement_seams(config)
        self.assertTrue(seams.portfolio_observatory.enabled)


class RiskScalarShadowModeTests(unittest.TestCase):
    """E1 — `live_risk_scalars_mode` ∈ {off, shadow, on}.

    Shadow mode runs every overlay so operators can read the would-be sizing
    delta from the log, but the live `risk_pct` is returned unchanged. This is
    the safe ramp-up step before flipping the stack to authoritative `on`.
    """

    @staticmethod
    def _high_vol_ctx() -> RiskScalarContext:
        return RiskScalarContext(
            symbol="EURUSD", timeframe="H1", regime="TREND",
            base_risk_pct=1.0, current_atr=0.0100, current_price=1.1000,
        )

    def test_off_mode_returns_input_with_no_overlays(self):
        cfg = SimpleNamespace(live_risk_scalars_mode="off")
        seams = create_default_enhancement_seams(cfg)
        self.assertEqual(seams.risk_scalar_stack.overlays, [])
        self.assertFalse(seams.risk_scalar_stack.shadow_mode)
        self.assertEqual(seams.risk_scalar_stack.apply(1.0, self._high_vol_ctx()), 1.0)

    def test_shadow_mode_populates_overlays_but_apply_is_pass_through(self):
        cfg = SimpleNamespace(live_risk_scalars_mode="shadow")
        seams = create_default_enhancement_seams(cfg)
        stack = seams.risk_scalar_stack
        self.assertGreater(len(stack.overlays), 0,
                           "shadow mode must populate the overlay list so compute() is meaningful")
        self.assertTrue(stack.shadow_mode)
        ctx = self._high_vol_ctx()
        # apply() leaves the live target untouched
        self.assertEqual(stack.apply(1.0, ctx), 1.0)
        # compute() runs the overlays — should drop sizing under high vol
        self.assertLess(stack.compute(1.0, ctx), 1.0)

    def test_on_mode_applies_scalars(self):
        cfg = SimpleNamespace(live_risk_scalars_mode="on")
        seams = create_default_enhancement_seams(cfg)
        stack = seams.risk_scalar_stack
        self.assertGreater(len(stack.overlays), 0)
        self.assertFalse(stack.shadow_mode)
        applied = stack.apply(1.0, self._high_vol_ctx())
        self.assertLess(applied, 1.0,
                        "on-mode must shrink risk under high realized vol")

    def test_legacy_enabled_true_falls_back_to_on(self):
        # Old configs without live_risk_scalars_mode but with enabled=True
        # must still light up the stack.
        cfg = SimpleNamespace(live_risk_scalars_enabled=True)
        seams = create_default_enhancement_seams(cfg)
        stack = seams.risk_scalar_stack
        self.assertGreater(len(stack.overlays), 0)
        self.assertFalse(stack.shadow_mode)

    def test_legacy_enabled_false_stays_off(self):
        cfg = SimpleNamespace(live_risk_scalars_enabled=False)
        seams = create_default_enhancement_seams(cfg)
        self.assertEqual(seams.risk_scalar_stack.overlays, [])

    def test_invalid_mode_falls_back_to_off_when_legacy_disabled(self):
        # Garbage mode value with no legacy override → safe default of off.
        cfg = SimpleNamespace(
            live_risk_scalars_mode="enabled",   # not a valid enum value
            live_risk_scalars_enabled=False,
        )
        seams = create_default_enhancement_seams(cfg)
        self.assertEqual(seams.risk_scalar_stack.overlays, [])
        self.assertFalse(seams.risk_scalar_stack.shadow_mode)

    def test_invalid_mode_with_legacy_enabled_falls_back_to_on(self):
        cfg = SimpleNamespace(
            live_risk_scalars_mode="garbage",
            live_risk_scalars_enabled=True,
        )
        seams = create_default_enhancement_seams(cfg)
        self.assertGreater(len(seams.risk_scalar_stack.overlays), 0)
        self.assertFalse(seams.risk_scalar_stack.shadow_mode)

    def test_mode_value_normalized_case_and_whitespace(self):
        cfg = SimpleNamespace(live_risk_scalars_mode="  SHADOW  ")
        seams = create_default_enhancement_seams(cfg)
        self.assertTrue(seams.risk_scalar_stack.shadow_mode)
        self.assertGreater(len(seams.risk_scalar_stack.overlays), 0)

    def test_compute_runs_even_when_apply_is_passthrough(self):
        # Direct unit test on the stack: shadow_mode forces apply() to no-op
        # but compute() must still reflect what overlays *would* have done.
        stack = RiskScalarStack(
            [VolatilityTargetScalar(target_annual_vol=0.10)],
            shadow_mode=True,
        )
        ctx = self._high_vol_ctx()
        self.assertEqual(stack.apply(1.0, ctx), 1.0)
        self.assertLess(stack.compute(1.0, ctx), 1.0)


class PipelineConfigRiskScalarsModeTests(unittest.TestCase):
    """E1 — PipelineConfig.__post_init__ normalizes `live_risk_scalars_mode`."""

    def test_default_is_off(self):
        from pm_core import PipelineConfig
        cfg = PipelineConfig()
        self.assertEqual(cfg.live_risk_scalars_mode, "off")

    def test_explicit_shadow_persists(self):
        from pm_core import PipelineConfig
        cfg = PipelineConfig(live_risk_scalars_mode="shadow")
        self.assertEqual(cfg.live_risk_scalars_mode, "shadow")

    def test_invalid_mode_warns_and_falls_back_to_off(self):
        import logging
        from pm_core import PipelineConfig
        with self.assertLogs("pm_core", level=logging.WARNING) as captured:
            cfg = PipelineConfig(live_risk_scalars_mode="enabled")
        self.assertEqual(cfg.live_risk_scalars_mode, "off")
        self.assertTrue(any("Invalid live_risk_scalars_mode" in r.getMessage()
                            for r in captured.records))

    def test_uppercase_normalized(self):
        from pm_core import PipelineConfig
        cfg = PipelineConfig(live_risk_scalars_mode="ON")
        self.assertEqual(cfg.live_risk_scalars_mode, "on")


if __name__ == "__main__":
    unittest.main()
