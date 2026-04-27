"""Phase D5 — opt-in searchable structural-SL wrapper.

`BaseStrategy.calculate_stops()` is the single source of stop sizing for all
47 strategies. When `use_structural_sl=True` it widens the ATR stop to the
nearest structural swing (lowest Low for longs / highest High for shorts in
a `structural_sl_lookback` window) but never narrows below ATR and never
exceeds `1.5 × ATR_SL`. Discovery decides per-(symbol, TF, regime) whether
the wrapper helps — defaults stay OFF so legacy behavior is preserved.

Scope is uniform across the trend + breakout families (no competitor
privileged); MR strategies do not opt in because their thesis is mean
reversion to a band, not a structural-support continuation.
"""
import unittest

import numpy as np
import pandas as pd

from pm_core import StrategyCategory, InstrumentSpec
from pm_strategies import (
    StrategyRegistry,
    EMACrossoverStrategy,
    DonchianBreakoutStrategy,
    RSIExtremesStrategy,
    _STRUCTURAL_SL_DEFAULT_LOOKBACK,
    _STRUCTURAL_SL_ATR_MAX_MULT,
)


def _eurusd_spec() -> InstrumentSpec:
    """4-digit FX spec — pip_size = 0.0001, so 10 pips ⇄ 0.0010 in price."""
    return InstrumentSpec(symbol='EURUSD', pip_position=4, digits=5)


def _build_features(*,
                    n: int = 60,
                    base_close: float = 1.1000,
                    swing_low: float | None = None,
                    swing_low_offset: int = 5,
                    swing_high: float | None = None,
                    swing_high_offset: int = 5,
                    atr_pips: float = 10.0) -> pd.DataFrame:
    """Build a flat OHLC frame with a controllable swing inside the lookback.

    `atr_pips` is injected directly via the `ATR_14` column so the test does
    not depend on warmup. A pip is 0.0001 for EURUSD, so atr_pips=10 ⇒ ATR=0.0010.
    """
    idx = pd.RangeIndex(n)
    close = np.full(n, base_close, dtype=float)
    high = close + 0.00005
    low = close - 0.00005

    if swing_low is not None:
        pos = n - 1 - swing_low_offset
        low = low.copy()
        low[pos] = swing_low
    if swing_high is not None:
        pos = n - 1 - swing_high_offset
        high = high.copy()
        high[pos] = swing_high

    return pd.DataFrame({
        'Open': close,
        'High': high,
        'Low': low,
        'Close': close,
        'Volume': np.ones(n),
        'ATR_14': np.full(n, atr_pips * 0.0001),
    }, index=idx)


class StructuralSLConstantsTests(unittest.TestCase):

    def test_lookback_default_is_twenty(self):
        self.assertEqual(_STRUCTURAL_SL_DEFAULT_LOOKBACK, 20)

    def test_cap_multiplier_is_one_and_a_half(self):
        self.assertEqual(_STRUCTURAL_SL_ATR_MAX_MULT, 1.5)


class StructuralSLBehaviorTests(unittest.TestCase):
    """Verify the math at the BaseStrategy level using a trend strategy.

    `EMACrossoverStrategy.calculate_stops` is inherited from BaseStrategy, so
    we exercise the wrapper through the public API rather than poking at the
    private helper directly.
    """

    def setUp(self):
        self.spec = _eurusd_spec()
        # ATR = 10 pips, sl_atr_mult default = 2.0 → ATR-based SL = 20 pips
        self.atr_pips = 10.0
        self.expected_atr_sl_pips = self.atr_pips * 2.0  # 20.0

    def _stops(self, strat, features, signal=1):
        return strat.calculate_stops(
            features=features, signal=signal, symbol='EURUSD',
            spec=self.spec, bar_index=len(features) - 1,
        )

    def test_default_off_returns_pure_atr_stop(self):
        """No opt-in — wrapper must be a no-op vs legacy ATR sizing."""
        strat = EMACrossoverStrategy()
        self.assertFalse(strat.params.get('use_structural_sl', False))

        feat = _build_features(swing_low=1.0900)  # very wide swing
        sl_pips, _tp = self._stops(strat, feat, signal=1)
        self.assertAlmostEqual(sl_pips, self.expected_atr_sl_pips, places=6)

    def test_opt_in_widens_to_nearest_swing_long(self):
        """Lowest Low 30 pips below close → SL widens from 20 → 30 pips."""
        strat = EMACrossoverStrategy(use_structural_sl=True)
        # base_close=1.1000, swing_low=1.0970 → 30 pip distance, < cap (30 pips)
        feat = _build_features(swing_low=1.0970)
        sl_pips, _tp = self._stops(strat, feat, signal=1)
        # Cap is 1.5 × 20 = 30 pips, so 30-pip swing is exactly at the cap.
        self.assertAlmostEqual(sl_pips, 30.0, places=6)

    def test_opt_in_widens_to_nearest_swing_short(self):
        """Highest High 28 pips above close → SL widens from 20 → 28 pips."""
        strat = EMACrossoverStrategy(use_structural_sl=True)
        feat = _build_features(swing_high=1.1028)
        sl_pips, _tp = self._stops(strat, feat, signal=-1)
        self.assertAlmostEqual(sl_pips, 28.0, places=6)

    def test_opt_in_does_not_narrow_when_swing_inside_atr(self):
        """Swing closer than ATR (8 pips) — must stay at ATR (20 pips)."""
        strat = EMACrossoverStrategy(use_structural_sl=True)
        feat = _build_features(swing_low=1.0992)  # 8 pips below close
        sl_pips, _tp = self._stops(strat, feat, signal=1)
        self.assertAlmostEqual(sl_pips, self.expected_atr_sl_pips, places=6)

    def test_cap_clips_runaway_swing_at_one_and_a_half_atr(self):
        """Swing 100 pips away — cap kicks in at 1.5 × 20 = 30 pips."""
        strat = EMACrossoverStrategy(use_structural_sl=True)
        feat = _build_features(swing_low=1.0900)  # 100 pips below close
        sl_pips, _tp = self._stops(strat, feat, signal=1)
        cap = self.expected_atr_sl_pips * _STRUCTURAL_SL_ATR_MAX_MULT
        self.assertAlmostEqual(sl_pips, cap, places=6)

    def test_falls_back_to_atr_when_no_valid_swing(self):
        """Flat lows = current close — ref >= close, so wrapper bails to ATR."""
        strat = EMACrossoverStrategy(use_structural_sl=True)
        n = 60
        close = np.full(n, 1.1000)
        feat = pd.DataFrame({
            'Open': close, 'High': close, 'Low': close, 'Close': close,
            'Volume': np.ones(n), 'ATR_14': np.full(n, self.atr_pips * 0.0001),
        })
        sl_pips, _tp = self._stops(strat, feat, signal=1)
        self.assertAlmostEqual(sl_pips, self.expected_atr_sl_pips, places=6)

    def test_no_lookahead_swing_after_bar_index_is_ignored(self):
        """A swing low placed AFTER bar_index must not influence the stop."""
        strat = EMACrossoverStrategy(use_structural_sl=True)
        n = 60
        bar_index = 30  # compute stop at bar 30
        close = np.full(n, 1.1000)
        low = close - 0.00005
        # Plant a deep swing well after bar_index — wrapper must not see it.
        low_arr = low.copy()
        low_arr[55] = 1.0900
        feat = pd.DataFrame({
            'Open': close, 'High': close + 0.00005,
            'Low': low_arr, 'Close': close,
            'Volume': np.ones(n), 'ATR_14': np.full(n, self.atr_pips * 0.0001),
        })
        sl_pips, _tp = strat.calculate_stops(
            features=feat, signal=1, symbol='EURUSD',
            spec=self.spec, bar_index=bar_index,
        )
        # No structural swing inside [bar_index-20, bar_index] beyond the
        # minor low band → falls back to ATR (20 pips).
        self.assertAlmostEqual(sl_pips, self.expected_atr_sl_pips, places=6)

    def test_lookback_window_respected(self):
        """Swing 25 bars back is outside the 20-bar lookback → ignored."""
        strat = EMACrossoverStrategy(use_structural_sl=True)
        # Place the swing 25 bars before the last bar — outside lookback=20.
        feat = _build_features(swing_low=1.0950, swing_low_offset=25)
        sl_pips, _tp = self._stops(strat, feat, signal=1)
        self.assertAlmostEqual(sl_pips, self.expected_atr_sl_pips, places=6)

    def test_custom_shorter_lookback_excludes_distant_swing(self):
        """Shrinking the lookback drops distant swings from consideration."""
        strat = EMACrossoverStrategy(use_structural_sl=True, structural_sl_lookback=5)
        # Swing 10 bars back is outside the 5-bar window
        feat = _build_features(swing_low=1.0970, swing_low_offset=10)
        sl_pips, _tp = self._stops(strat, feat, signal=1)
        self.assertAlmostEqual(sl_pips, self.expected_atr_sl_pips, places=6)

    def test_invalid_lookback_falls_back_to_atr(self):
        """`structural_sl_lookback < 2` is degenerate — must bail to ATR."""
        strat = EMACrossoverStrategy(use_structural_sl=True, structural_sl_lookback=1)
        feat = _build_features(swing_low=1.0970)
        sl_pips, _tp = self._stops(strat, feat, signal=1)
        self.assertAlmostEqual(sl_pips, self.expected_atr_sl_pips, places=6)

    def test_breakout_strategy_inherits_same_behavior(self):
        """DonchianBreakout opts in the same way as trend strategies."""
        strat = DonchianBreakoutStrategy(use_structural_sl=True)
        feat = _build_features(swing_low=1.0975)  # 25 pips
        sl_pips, _tp = strat.calculate_stops(
            features=feat, signal=1, symbol='EURUSD',
            spec=self.spec, bar_index=len(feat) - 1,
        )
        self.assertAlmostEqual(sl_pips, 25.0, places=6)


class StructuralSLOptInScopeTests(unittest.TestCase):
    """D5 is scoped to TREND_FOLLOWING + BREAKOUT_MOMENTUM only.

    MR strategies fade extremes back to a band — a structural-support stop
    contradicts that thesis, so they must not expose `use_structural_sl`.
    """

    OPT_IN_CATEGORIES = {
        StrategyCategory.TREND_FOLLOWING,
        StrategyCategory.BREAKOUT_MOMENTUM,
    }

    def test_trend_and_breakout_strategies_all_expose_grid_key(self):
        missing = []
        for name in StrategyRegistry.list_all():
            strat = StrategyRegistry.get(name)
            if strat.category not in self.OPT_IN_CATEGORIES:
                continue
            defaults = strat.get_default_params()
            grid = strat.get_param_grid()
            if 'use_structural_sl' not in defaults or 'use_structural_sl' not in grid:
                missing.append(name)
        self.assertEqual(missing, [],
                         f"Trend/breakout strategies missing use_structural_sl: {missing}")

    def test_trend_and_breakout_default_is_off(self):
        for name in StrategyRegistry.list_all():
            strat = StrategyRegistry.get(name)
            if strat.category not in self.OPT_IN_CATEGORIES:
                continue
            with self.subTest(strategy=name):
                self.assertFalse(strat.get_default_params().get('use_structural_sl', False),
                                 f"{name}: default must be OFF — discovery decides opt-in")

    def test_trend_and_breakout_grid_searches_both_branches(self):
        for name in StrategyRegistry.list_all():
            strat = StrategyRegistry.get(name)
            if strat.category not in self.OPT_IN_CATEGORIES:
                continue
            with self.subTest(strategy=name):
                values = strat.get_param_grid().get('use_structural_sl', [])
                self.assertEqual(set(values), {True, False},
                                 f"{name}: grid must search both [True, False]")

    def test_mean_reversion_strategies_do_not_opt_in(self):
        """MR thesis = revert to band, not hold structural support — keep stops ATR-only."""
        leaked = []
        for name in StrategyRegistry.list_all():
            strat = StrategyRegistry.get(name)
            if strat.category != StrategyCategory.MEAN_REVERSION:
                continue
            if 'use_structural_sl' in strat.get_default_params() \
                    or 'use_structural_sl' in strat.get_param_grid():
                leaked.append(name)
        self.assertEqual(leaked, [],
                         f"MR strategies must not expose use_structural_sl: {leaked}")

    def test_mr_strategy_opt_in_attempt_is_inert(self):
        """Even if a caller forces use_structural_sl on an MR strategy, the
        wrapper still runs through BaseStrategy and must not crash. It is
        simply not searched by Optuna because it is not in the MR grid.
        """
        strat = RSIExtremesStrategy()
        strat.params['use_structural_sl'] = True
        feat = _build_features(swing_low=1.0975)
        sl_pips, _tp = strat.calculate_stops(
            features=feat, signal=1, symbol='EURUSD',
            spec=_eurusd_spec(), bar_index=len(feat) - 1,
        )
        self.assertGreater(sl_pips, 0.0)


if __name__ == "__main__":
    unittest.main()
