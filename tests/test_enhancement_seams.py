import unittest

from pm_enhancement_seams import (
    ExecutionQualityContext,
    RiskScalarContext,
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


if __name__ == "__main__":
    unittest.main()
