"""The ATR_14 fallback must return the same stops it always did, far cheaper.

When FeatureComputer does not supply ATR_14, calculate_stops used to slice
features.iloc[:bar_index+1] and recompute the whole true range PER TRADE. On a
106,697-bar frame that turned a 548 ms backtest into 49 s - a silent ~20x,
guarded only by a comment saying it "should be rare".

True range and rolling(14).mean() are causal, so the value at the end of a
prefix equals the value at that position of the full series. This pins that
equivalence exactly, because the whole argument for caching rests on it.
"""

import time
import unittest

import numpy as np
import pandas as pd

import pm_strategies
from pm_strategies import StrategyRegistry


def _frame(n: int, seed: int = 5, with_atr: bool = False) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    close = 1.10 + np.cumsum(rng.normal(0, 1e-4, n))
    idx = pd.date_range("2020-01-01", periods=n, freq="h", tz="UTC")
    df = pd.DataFrame(
        {
            "Open": close + rng.normal(0, 2e-5, n),
            "High": close + np.abs(rng.normal(0, 5e-5, n)),
            "Low": close - np.abs(rng.normal(0, 5e-5, n)),
            "Close": close,
            "Volume": rng.integers(100, 5000, n).astype(float),
        },
        index=idx,
    )
    if with_atr:
        df["ATR_14"] = (df["High"] - df["Low"]).rolling(14).mean().bfill()
    return df


def _reference_prefix_atr(features: pd.DataFrame, bar_index) -> float:
    """The original per-trade prefix recomputation, kept as the oracle."""
    _df = features if bar_index is None else features.iloc[:bar_index + 1]
    high_low = _df["High"] - _df["Low"]
    high_close = abs(_df["High"] - _df["Close"].shift(1))
    low_close = abs(_df["Low"] - _df["Close"].shift(1))
    tr = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
    return float(tr.rolling(14).mean().iloc[-1])


class ATRFallbackGuardTest(unittest.TestCase):
    def setUp(self):
        pm_strategies._ATR_FALLBACK_WARNED.clear()

    def test_cached_series_matches_prefix_recomputation_exactly(self):
        df = _frame(600)
        series = pm_strategies._fallback_atr14(df, "EURUSD")
        for bar_index in (14, 15, 20, 99, 300, 599):
            with self.subTest(bar_index=bar_index):
                self.assertEqual(
                    float(series.iat[bar_index]),
                    _reference_prefix_atr(df, bar_index),
                )

    def test_early_bars_are_nan_in_both(self):
        df = _frame(60)
        series = pm_strategies._fallback_atr14(df, "EURUSD")
        for bar_index in (0, 1, 12):
            with self.subTest(bar_index=bar_index):
                self.assertTrue(np.isnan(series.iat[bar_index]))
                self.assertTrue(np.isnan(_reference_prefix_atr(df, bar_index)))

    def test_bar_index_none_matches_last_value(self):
        df = _frame(200)
        series = pm_strategies._fallback_atr14(df, "EURUSD")
        self.assertEqual(float(series.iloc[-1]), _reference_prefix_atr(df, None))

    def test_series_is_cached_per_frame(self):
        df = _frame(300)
        first = pm_strategies._fallback_atr14(df, "EURUSD")
        second = pm_strategies._fallback_atr14(df, "EURUSD")
        self.assertIs(first, second)

    def test_stale_cache_of_wrong_length_is_rejected(self):
        df = _frame(300)
        pm_strategies._ATR_FALLBACK_CACHE[id(df)] = pd.Series([1.0, 2.0])
        series = pm_strategies._fallback_atr14(df, "EURUSD")
        self.assertEqual(len(series), len(df))

    def test_caching_does_not_poison_the_frame(self):
        """Regression: caching in features.attrs broke pd.concat.

        pandas compares attrs dicts with ==, so a Series stored there makes
        `obj.attrs == attrs` return a Series and any later concat raise
        "truth value of a Series is ambiguous". The cache must live outside
        the frame.
        """
        df = _frame(200)
        pm_strategies._fallback_atr14(df, "EURUSD")
        self.assertEqual(df.attrs, {})
        joined = pd.concat([df, df])           # used to raise ValueError
        self.assertEqual(len(joined), 2 * len(df))
        self.assertIsNotNone(df.copy().describe())

    def test_cache_entry_is_released_with_the_frame(self):
        import gc

        df = _frame(150)
        pm_strategies._fallback_atr14(df, "EURUSD")
        key = id(df)
        self.assertIn(key, pm_strategies._ATR_FALLBACK_CACHE)
        del df
        gc.collect()
        self.assertNotIn(key, pm_strategies._ATR_FALLBACK_CACHE)

    def test_warns_once_per_symbol(self):
        df = _frame(120)
        with self.assertLogs("pm_strategies", level="WARNING") as first:
            pm_strategies._fallback_atr14(df, "GBPUSD")
        self.assertTrue(any("ATR_14 missing" in m for m in first.output))
        other = _frame(120, seed=6)
        with self.assertNoLogs("pm_strategies", level="WARNING"):
            pm_strategies._fallback_atr14(other, "GBPUSD")

    def test_calculate_stops_agrees_with_and_without_atr_column(self):
        """The fast path and the fallback must produce the same stops."""
        strategy = StrategyRegistry.get("MACDTrendStrategy")
        with_atr = _frame(400, with_atr=True)
        without = with_atr.drop(columns=["ATR_14"])
        # Give the fast path the fallback's own definition so the two are
        # comparable; the point is the code path, not the ATR recipe.
        with_atr = without.copy()
        with_atr["ATR_14"] = pm_strategies._fallback_atr14(without.copy(), "EURUSD")
        for bar_index in (50, 120, 399):
            with self.subTest(bar_index=bar_index):
                fast = strategy.calculate_stops(
                    with_atr, 1, "EURUSD", bar_index=bar_index
                )
                slow = strategy.calculate_stops(
                    without, 1, "EURUSD", bar_index=bar_index
                )
                self.assertEqual(fast, slow)

    def test_fallback_is_not_quadratic(self):
        """A 4x longer frame must not cost ~16x. Guards the actual defect."""
        small, large = _frame(2_000), _frame(8_000)
        strategy = StrategyRegistry.get("MACDTrendStrategy")

        def elapsed(df, n_calls):
            pm_strategies._ATR_FALLBACK_CACHE.pop(id(df), None)
            start = time.perf_counter()
            for i in range(n_calls):
                strategy.calculate_stops(
                    df, 1, "EURUSD", bar_index=len(df) - 1 - i
                )
            return time.perf_counter() - start

        elapsed(small, 5)  # warm imports
        t_small, t_large = elapsed(small, 40), elapsed(large, 40)
        # Quadratic would put this near 4x. Cached, it is dominated by one
        # O(n) build, so allow generous headroom and still catch a regression.
        self.assertLess(t_large, max(t_small, 1e-4) * 3.0 + 0.05)


if __name__ == "__main__":
    unittest.main()
