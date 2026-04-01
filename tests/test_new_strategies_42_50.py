"""
Tests for the 8 new strategies (42 → 50 expansion).

Covers:
- Registry presence and count
- Signal contract (-1/0/1, aligned index, no NaN entries)
- Param grid compliance (SL/TP standardized, combo counts)
- Category assignments
- No-lookahead safety for structural/zone strategies
- Default instantiation and basic smoke
"""

import numpy as np
import pandas as pd
import pytest

from pm_strategies import (
    StrategyRegistry,
    StrategyCategory,
    _GLOBAL_SL_GRID,
    _GLOBAL_TP_GRID,
    VortexTrendStrategy,
    ElderRayBullBearStrategy,
    TRIXMomentumStrategy,
    MarketStructureBOSPullbackStrategy,
    LiquiditySweepReversalStrategy,
    FibonacciRetracementPullbackStrategy,
    FractalSRZoneBreakRetestStrategy,
    SupplyDemandImpulseRetestStrategy,
)

# Test symbol for generate_signals / calculate_stops calls
TEST_SYMBOL = 'EURUSD'

NEW_STRATEGY_NAMES = [
    'VortexTrendStrategy',
    'ElderRayBullBearStrategy',
    'TRIXMomentumStrategy',
    'MarketStructureBOSPullbackStrategy',
    'LiquiditySweepReversalStrategy',
    'FibonacciRetracementPullbackStrategy',
    'FractalSRZoneBreakRetestStrategy',
    'SupplyDemandImpulseRetestStrategy',
]

EXPECTED_CATEGORIES = {
    'VortexTrendStrategy': StrategyCategory.TREND_FOLLOWING,
    'ElderRayBullBearStrategy': StrategyCategory.TREND_FOLLOWING,
    'TRIXMomentumStrategy': StrategyCategory.BREAKOUT_MOMENTUM,
    'MarketStructureBOSPullbackStrategy': StrategyCategory.TREND_FOLLOWING,
    'LiquiditySweepReversalStrategy': StrategyCategory.MEAN_REVERSION,
    'FibonacciRetracementPullbackStrategy': StrategyCategory.TREND_FOLLOWING,
    'FractalSRZoneBreakRetestStrategy': StrategyCategory.BREAKOUT_MOMENTUM,
    'SupplyDemandImpulseRetestStrategy': StrategyCategory.BREAKOUT_MOMENTUM,
}

# Strategy-only combo counts from the spec (excluding SL/TP)
EXPECTED_STRATEGY_COMBOS = {
    'VortexTrendStrategy': 720,
    'ElderRayBullBearStrategy': 480,
    'TRIXMomentumStrategy': 360,
    'MarketStructureBOSPullbackStrategy': 324,
    'LiquiditySweepReversalStrategy': 576,
    'FibonacciRetracementPullbackStrategy': 432,
    'FractalSRZoneBreakRetestStrategy': 576,
    'SupplyDemandImpulseRetestStrategy': 720,
}

SL_TP_COMBOS = len(_GLOBAL_SL_GRID) * len(_GLOBAL_TP_GRID)  # 44


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def _make_features(n: int = 500, seed: int = 42) -> pd.DataFrame:
    """Build a synthetic OHLCV DataFrame with trending + mean-reverting segments."""
    rng = np.random.RandomState(seed)
    dates = pd.date_range('2023-01-01', periods=n, freq='4h')

    close = 1.1000 + np.cumsum(rng.randn(n) * 0.001)
    high = close + rng.uniform(0.0005, 0.003, n)
    low = close - rng.uniform(0.0005, 0.003, n)
    open_ = close + rng.randn(n) * 0.001
    volume = rng.randint(100, 10000, n).astype(float)

    df = pd.DataFrame({
        'Open': open_,
        'High': high,
        'Low': low,
        'Close': close,
        'Volume': volume,
    }, index=dates)

    tr = np.maximum(
        df['High'] - df['Low'],
        np.maximum(
            (df['High'] - df['Close'].shift(1)).abs(),
            (df['Low'] - df['Close'].shift(1)).abs()
        )
    )
    df['ATR_14'] = tr.rolling(14).mean()
    return df


def _make_features_with_swings(n: int = 800, seed: int = 99) -> pd.DataFrame:
    """Build features with clear swing structure for structural strategy testing."""
    rng = np.random.RandomState(seed)
    dates = pd.date_range('2023-01-01', periods=n, freq='4h')

    close = np.zeros(n)
    close[0] = 1.1000
    leg_len = 0
    direction = 1
    for i in range(1, n):
        close[i] = close[i - 1] + direction * rng.uniform(0.0001, 0.002)
        leg_len += 1
        if leg_len > rng.randint(30, 60):
            direction *= -1
            leg_len = 0

    noise = rng.randn(n) * 0.0003
    close = close + noise
    high = close + rng.uniform(0.0005, 0.003, n)
    low = close - rng.uniform(0.0005, 0.003, n)
    open_ = close + rng.randn(n) * 0.0008
    volume = rng.randint(100, 10000, n).astype(float)

    df = pd.DataFrame({
        'Open': open_,
        'High': high,
        'Low': low,
        'Close': close,
        'Volume': volume,
    }, index=dates)

    tr = np.maximum(
        df['High'] - df['Low'],
        np.maximum(
            (df['High'] - df['Close'].shift(1)).abs(),
            (df['Low'] - df['Close'].shift(1)).abs()
        )
    )
    df['ATR_14'] = tr.rolling(14).mean()
    return df


# ---------------------------------------------------------------------------
# Registry Tests
# ---------------------------------------------------------------------------

class TestRegistryExpansion:
    def test_total_count_is_50(self):
        assert StrategyRegistry.count() == 50

    def test_all_new_strategies_registered(self):
        names = set(StrategyRegistry.list_all())
        for name in NEW_STRATEGY_NAMES:
            assert name in names, f"{name} missing from registry"

    def test_category_counts(self):
        trend = StrategyRegistry.list_by_category(StrategyCategory.TREND_FOLLOWING)
        mr = StrategyRegistry.list_by_category(StrategyCategory.MEAN_REVERSION)
        breakout = StrategyRegistry.list_by_category(StrategyCategory.BREAKOUT_MOMENTUM)
        assert len(trend) == 18
        assert len(mr) == 18
        assert len(breakout) == 14

    @pytest.mark.parametrize("name", NEW_STRATEGY_NAMES)
    def test_category_assignment(self, name):
        instance = StrategyRegistry.get(name)
        assert instance.category == EXPECTED_CATEGORIES[name]

    @pytest.mark.parametrize("name", NEW_STRATEGY_NAMES)
    def test_instantiation_by_name(self, name):
        instance = StrategyRegistry.get(name)
        assert instance.name == name


# ---------------------------------------------------------------------------
# Signal Contract Tests
# ---------------------------------------------------------------------------

class TestSignalContract:
    """Every strategy must return a Series of {-1, 0, 1} aligned with features index."""

    @pytest.mark.parametrize("name", NEW_STRATEGY_NAMES)
    def test_signal_values_in_range(self, name):
        features = _make_features()
        instance = StrategyRegistry.get(name)
        signals = instance.generate_signals(features, TEST_SYMBOL)

        assert isinstance(signals, pd.Series), f"{name} did not return pd.Series"
        assert len(signals) == len(features), f"{name} signal length mismatch"
        assert signals.index.equals(features.index), f"{name} index mismatch"

        # No NaN entries
        assert not signals.isna().any(), f"{name} has NaN in signals"

        # Values in {-1, 0, 1}
        unique_vals = set(signals.unique())
        assert unique_vals.issubset({-1, 0, 1}), \
            f"{name} has invalid signal values: {unique_vals - {-1, 0, 1}}"

    @pytest.mark.parametrize("name", NEW_STRATEGY_NAMES)
    def test_signal_values_with_swing_data(self, name):
        """Test with swing-rich data that structural strategies can use."""
        features = _make_features_with_swings()
        instance = StrategyRegistry.get(name)
        signals = instance.generate_signals(features, TEST_SYMBOL)

        assert isinstance(signals, pd.Series)
        assert len(signals) == len(features)
        assert not signals.isna().any()
        unique_vals = set(signals.unique())
        assert unique_vals.issubset({-1, 0, 1})

    @pytest.mark.parametrize("name", NEW_STRATEGY_NAMES)
    def test_warmup_zeroed(self, name):
        """First bars should be zero (warmup period)."""
        features = _make_features()
        instance = StrategyRegistry.get(name)
        signals = instance.generate_signals(features, TEST_SYMBOL)

        # At least first 10 bars should be zero (all strategies have warmup > 10)
        assert (signals.iloc[:10] == 0).all(), \
            f"{name} has non-zero signals in warmup zone"


# ---------------------------------------------------------------------------
# Param Grid Tests
# ---------------------------------------------------------------------------

class TestParamGrid:
    @pytest.mark.parametrize("name", NEW_STRATEGY_NAMES)
    def test_sl_tp_standardized(self, name):
        """All grids must include standardized SL/TP from global grid."""
        instance = StrategyRegistry.get(name)
        grid = instance.get_param_grid()

        assert 'sl_atr_mult' in grid, f"{name} missing sl_atr_mult in grid"
        assert 'tp_atr_mult' in grid, f"{name} missing tp_atr_mult in grid"
        assert grid['sl_atr_mult'] == _GLOBAL_SL_GRID
        assert grid['tp_atr_mult'] == _GLOBAL_TP_GRID

    @pytest.mark.parametrize("name", NEW_STRATEGY_NAMES)
    def test_strategy_combo_count(self, name):
        """Strategy-only combos should match spec expectations."""
        instance = StrategyRegistry.get(name)
        grid = instance.get_param_grid()

        # Remove SL/TP to count strategy-only combos
        strategy_grid = {k: v for k, v in grid.items()
                         if k not in ('sl_atr_mult', 'tp_atr_mult')}

        combos = 1
        for values in strategy_grid.values():
            combos *= len(values)

        expected = EXPECTED_STRATEGY_COMBOS[name]
        assert combos == expected, \
            f"{name}: expected {expected} strategy combos, got {combos}"

    @pytest.mark.parametrize("name", NEW_STRATEGY_NAMES)
    def test_total_combo_count(self, name):
        """Total combos = strategy combos x SL/TP combos (44)."""
        instance = StrategyRegistry.get(name)
        grid = instance.get_param_grid()

        strategy_grid = {k: v for k, v in grid.items()
                         if k not in ('sl_atr_mult', 'tp_atr_mult')}
        strategy_combos = 1
        for values in strategy_grid.values():
            strategy_combos *= len(values)

        total = strategy_combos * SL_TP_COMBOS
        expected_total = EXPECTED_STRATEGY_COMBOS[name] * SL_TP_COMBOS
        assert total == expected_total

    @pytest.mark.parametrize("name", NEW_STRATEGY_NAMES)
    def test_default_params_in_grid(self, name):
        """Default param values should be valid grid members."""
        instance = StrategyRegistry.get(name)
        defaults = instance.get_default_params()
        grid = instance.get_param_grid()

        for key, default_val in defaults.items():
            if key in ('sl_atr_mult', 'tp_atr_mult'):
                continue
            if key in grid:
                assert default_val in grid[key], \
                    f"{name}: default {key}={default_val} not in grid {grid[key]}"


# ---------------------------------------------------------------------------
# No-Lookahead Tests
# ---------------------------------------------------------------------------

STRUCTURAL_STRATEGIES = [
    'MarketStructureBOSPullbackStrategy',
    'LiquiditySweepReversalStrategy',
    'FibonacciRetracementPullbackStrategy',
    'FractalSRZoneBreakRetestStrategy',
    'SupplyDemandImpulseRetestStrategy',
]


class TestNoLookahead:
    """
    Verify that signals at bar i depend only on data up to bar i.

    Method: compute signals on full data, then on truncated data (first N bars).
    Signals for bars 0..N-1 must be identical in both runs.
    """

    @pytest.mark.parametrize("name", STRUCTURAL_STRATEGIES)
    def test_no_lookahead_truncation(self, name):
        features = _make_features_with_swings(n=600)
        instance = StrategyRegistry.get(name)

        # Full run
        signals_full = instance.generate_signals(features.copy(), TEST_SYMBOL)

        # Truncated run (first 400 bars)
        cutoff = 400
        features_trunc = features.iloc[:cutoff].copy()
        signals_trunc = instance.generate_signals(features_trunc, TEST_SYMBOL)

        # Signals for first 400 bars must match
        np.testing.assert_array_equal(
            signals_full.iloc[:cutoff].values,
            signals_trunc.values,
            err_msg=f"{name} has lookahead: signals differ on truncated data"
        )

    @pytest.mark.parametrize("name", STRUCTURAL_STRATEGIES)
    def test_no_lookahead_future_change(self, name):
        """Changing future data should not affect past signals."""
        features_a = _make_features_with_swings(n=500, seed=42)
        features_b = features_a.copy()

        # Alter future bars (300+)
        rng = np.random.RandomState(123)
        n_future = len(features_b) - 300
        features_b.iloc[300:, features_b.columns.get_loc('Close')] += rng.randn(n_future) * 0.01
        features_b.iloc[300:, features_b.columns.get_loc('High')] += rng.randn(n_future) * 0.01
        features_b.iloc[300:, features_b.columns.get_loc('Low')] -= rng.randn(n_future) * 0.005

        # Recompute ATR for altered data
        tr_b = np.maximum(
            features_b['High'] - features_b['Low'],
            np.maximum(
                (features_b['High'] - features_b['Close'].shift(1)).abs(),
                (features_b['Low'] - features_b['Close'].shift(1)).abs()
            )
        )
        features_b['ATR_14'] = tr_b.rolling(14).mean()

        instance = StrategyRegistry.get(name)
        signals_a = instance.generate_signals(features_a, TEST_SYMBOL)
        signals_b = instance.generate_signals(features_b, TEST_SYMBOL)

        # First 300 bars should be identical
        np.testing.assert_array_equal(
            signals_a.iloc[:300].values,
            signals_b.iloc[:300].values,
            err_msg=f"{name} lookahead: future data changes affected past signals"
        )


# ---------------------------------------------------------------------------
# Stop Calculation Tests
# ---------------------------------------------------------------------------

class TestStopCalculation:
    @pytest.mark.parametrize("name", NEW_STRATEGY_NAMES)
    def test_calculate_stops_returns_valid(self, name):
        features = _make_features()
        instance = StrategyRegistry.get(name)
        signals = instance.generate_signals(features, TEST_SYMBOL)

        # Find a non-zero signal bar (if any)
        nonzero = signals[signals != 0]
        if len(nonzero) == 0:
            pytest.skip(f"{name} produced no signals on test data")

        idx = nonzero.index[0]
        direction = int(nonzero.iloc[0])
        bar_idx = features.index.get_loc(idx)

        sl, tp = instance.calculate_stops(
            features, direction, TEST_SYMBOL, bar_index=bar_idx
        )

        assert isinstance(sl, float)
        assert isinstance(tp, float)
        assert sl > 0
        assert tp > 0


# ---------------------------------------------------------------------------
# Required Features Tests
# ---------------------------------------------------------------------------

class TestRequiredFeatures:
    @pytest.mark.parametrize("name", NEW_STRATEGY_NAMES)
    def test_required_features_includes_atr(self, name):
        instance = StrategyRegistry.get(name)
        required = instance.get_required_features()
        assert 'ATR_14' in required


# ---------------------------------------------------------------------------
# Smoke Test: Run with non-default params
# ---------------------------------------------------------------------------

class TestNonDefaultParams:
    """Verify strategies work with various grid param settings."""

    def test_vortex_with_ema_and_adx_filters(self):
        features = _make_features()
        strat = VortexTrendStrategy(
            vortex_period=21, min_vi_spread=0.06,
            use_ema_filter=True, ema_filter_period=50,
            use_adx_filter=True, adx_threshold=22,
        )
        signals = strat.generate_signals(features, TEST_SYMBOL)
        assert not signals.isna().any()
        assert set(signals.unique()).issubset({-1, 0, 1})

    def test_elder_ray_threshold_cross(self):
        features = _make_features()
        strat = ElderRayBullBearStrategy(
            ema_period=20, entry_mode='threshold_cross',
            threshold_atr_norm=0.4, require_ema_trend=True,
            trend_ema_period=200, power_smooth=3,
        )
        signals = strat.generate_signals(features, TEST_SYMBOL)
        assert not signals.isna().any()
        assert set(signals.unique()).issubset({-1, 0, 1})

    def test_elder_ray_modes_are_distinct(self):
        features = _make_features(n=600, seed=42)
        zero_mode = ElderRayBullBearStrategy(
            ema_period=13, entry_mode='zero_cross',
            threshold_atr_norm=0.6, require_ema_trend=False,
        )
        threshold_mode = ElderRayBullBearStrategy(
            ema_period=13, entry_mode='threshold_cross',
            threshold_atr_norm=0.6, require_ema_trend=False,
        )
        zero_signals = zero_mode.generate_signals(features.copy(), TEST_SYMBOL)
        threshold_signals = threshold_mode.generate_signals(features.copy(), TEST_SYMBOL)
        assert not zero_signals.equals(threshold_signals)

    def test_trix_zero_cross_mode(self):
        features = _make_features()
        strat = TRIXMomentumStrategy(
            trix_period=20, signal_period=12,
            entry_mode='zero_cross', use_trend_filter=True,
            trend_ema_period=50, min_abs_trix=0.03,
        )
        signals = strat.generate_signals(features, TEST_SYMBOL)
        assert not signals.isna().any()
        assert set(signals.unique()).issubset({-1, 0, 1})

    def test_trix_both_confirm_mode(self):
        features = _make_features()
        strat = TRIXMomentumStrategy(
            trix_period=15, signal_period=9,
            entry_mode='both_confirm', use_trend_filter=False,
            min_abs_trix=0.0,
        )
        signals = strat.generate_signals(features, TEST_SYMBOL)
        assert not signals.isna().any()
        assert set(signals.unique()).issubset({-1, 0, 1})

    def test_bos_without_displacement(self):
        features = _make_features_with_swings()
        strat = MarketStructureBOSPullbackStrategy(
            swing_order=3, bos_buffer_atr=0.05,
            require_displacement=False,
            pullback_tolerance_atr=0.6, pullback_window=6,
        )
        signals = strat.generate_signals(features, TEST_SYMBOL)
        assert not signals.isna().any()
        assert set(signals.unique()).issubset({-1, 0, 1})

    def test_liquidity_sweep_with_reversal_body_gate(self):
        features = _make_features_with_swings()
        strat = LiquiditySweepReversalStrategy(
            swing_order=3, pool_lookback_bars=80,
            min_pool_points=3, sweep_buffer_atr=0.15,
            reclaim_window=3, min_reversal_body_atr=0.4,
        )
        signals = strat.generate_signals(features, TEST_SYMBOL)
        assert not signals.isna().any()
        assert set(signals.unique()).issubset({-1, 0, 1})

    def test_fib_engulfing_confirmation(self):
        features = _make_features_with_swings()
        strat = FibonacciRetracementPullbackStrategy(
            swing_order=3, min_impulse_atr=1.0,
            fib_entry_set='382_618', confirmation_mode='engulfing_like',
            max_pullback_bars=8, invalidation_mode='below_swing_start',
        )
        signals = strat.generate_signals(features, TEST_SYMBOL)
        assert not signals.isna().any()
        assert set(signals.unique()).issubset({-1, 0, 1})

    def test_fractal_sr_wider_zones(self):
        features = _make_features_with_swings()
        strat = FractalSRZoneBreakRetestStrategy(
            fractal_order=9, zone_width_atr=0.9,
            min_zone_touches=4, break_buffer_atr=0.2,
            retest_window=8,
        )
        signals = strat.generate_signals(features, TEST_SYMBOL)
        assert not signals.isna().any()
        assert set(signals.unique()).issubset({-1, 0, 1})

    def test_supply_demand_tight_impulse(self):
        features = _make_features_with_swings()
        strat = SupplyDemandImpulseRetestStrategy(
            impulse_body_atr=1.0, base_lookback=12,
            zone_width_atr=0.8, max_zone_touches=1,
            confirmation_close_pos=0.75,
        )
        signals = strat.generate_signals(features, TEST_SYMBOL)
        assert not signals.isna().any()
        assert set(signals.unique()).issubset({-1, 0, 1})


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
