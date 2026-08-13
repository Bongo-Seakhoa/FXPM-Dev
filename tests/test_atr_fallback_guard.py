"""The ATR_14 fallback must behave exactly like the original prefix form.

History worth keeping, because it is the whole reason these tests are strict:

5b0180c memoised the fallback series per frame to remove an O(n) per-trade
cost. CX-084 found two correctness defects in that, both reproduced here as
regressions:

  1. cache identity was id(frame) + len only, so a SAME-LENGTH in-place OHLC
     edit returned a stale ATR - 0.000406 against a true 0.020400, a 50x wrong
     number that would silently produce wrong stop distances
  2. invalid bar indices drifted in BOTH directions: bar_index=-1 raised under
     the prefix form but returned a value from the cache, and an out-of-range
     index returned a value under the prefix form but raised from the cache

The cache was removed rather than repaired. FeatureComputer supplies ATR_14 on
the canonical path, so the fallback must be zero-hit there; caching it bought
nothing real and cost correctness. If it is ever slow, that is a
feature-pipeline defect announcing itself.
"""

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
    """The original implementation, kept verbatim as the oracle."""
    _df = features if bar_index is None else features.iloc[:bar_index + 1]
    high_low = _df["High"] - _df["Low"]
    high_close = abs(_df["High"] - _df["Close"].shift(1))
    low_close = abs(_df["Low"] - _df["Close"].shift(1))
    tr = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
    return float(tr.rolling(14).mean().iloc[-1])


def _outcome(fn):
    """Value or exception type, so drift in either direction is comparable.

    NaN is normalised because NaN != NaN would make a matching pair look like
    drift, which is the opposite of what this asserts.
    """
    try:
        value = fn()
    except Exception as exc:
        return ("raised", type(exc).__name__)
    if isinstance(value, float) and np.isnan(value):
        return ("value", "nan")
    return ("value", value)


class ATRFallbackGuardTest(unittest.TestCase):
    def setUp(self):
        pm_strategies._ATR_FALLBACK_WARNED.clear()

    def test_matches_prefix_recomputation_exactly(self):
        df = _frame(600)
        for bar_index in (14, 15, 20, 99, 300, 599, None):
            with self.subTest(bar_index=bar_index):
                self.assertEqual(
                    pm_strategies._fallback_atr14(df, "EURUSD", bar_index),
                    _reference_prefix_atr(df, bar_index),
                )

    def test_early_bars_are_nan_in_both(self):
        df = _frame(60)
        for bar_index in (0, 1, 12):
            with self.subTest(bar_index=bar_index):
                self.assertTrue(
                    np.isnan(pm_strategies._fallback_atr14(df, "EURUSD", bar_index))
                )
                self.assertTrue(np.isnan(_reference_prefix_atr(df, bar_index)))

    def test_invalid_indices_behave_exactly_as_the_prefix_form(self):
        """CX-084 defect 2: the cache drifted here in both directions."""
        df = _frame(60)
        for bar_index in (-1, -5, -60, 10_000, 59, 60):
            with self.subTest(bar_index=bar_index):
                self.assertEqual(
                    _outcome(lambda: pm_strategies._fallback_atr14(
                        df, "EURUSD", bar_index)),
                    _outcome(lambda: _reference_prefix_atr(df, bar_index)),
                )

    def test_in_place_mutation_is_reflected_immediately(self):
        """CX-084 defect 1: a same-length edit returned a 50x wrong stale value."""
        df = _frame(60)
        before = pm_strategies._fallback_atr14(df, "EURUSD", 30)
        self.assertEqual(before, _reference_prefix_atr(df, 30))

        df.loc[:, "High"] = df["High"] + 0.01
        df.loc[:, "Low"] = df["Low"] - 0.01

        after = pm_strategies._fallback_atr14(df, "EURUSD", 30)
        self.assertEqual(after, _reference_prefix_atr(df, 30))
        self.assertNotEqual(after, before)

    def test_nothing_is_cached_on_or_off_the_frame(self):
        """attrs caching broke pd.concat; id() caching went stale. Neither now."""
        df = _frame(200)
        pm_strategies._fallback_atr14(df, "EURUSD", 100)
        self.assertEqual(df.attrs, {})
        self.assertFalse(hasattr(pm_strategies, "_ATR_FALLBACK_CACHE"))
        joined = pd.concat([df, df])
        self.assertEqual(len(joined), 2 * len(df))

    def test_warns_once_per_symbol(self):
        df = _frame(120)
        with self.assertLogs("pm_strategies", level="WARNING") as first:
            pm_strategies._fallback_atr14(df, "GBPUSD", 60)
        self.assertTrue(any("ATR_14 missing" in m for m in first.output))
        with self.assertNoLogs("pm_strategies", level="WARNING"):
            pm_strategies._fallback_atr14(_frame(120, seed=6), "GBPUSD", 60)

    def test_calculate_stops_agrees_with_and_without_atr_column(self):
        strategy = StrategyRegistry.get("MACDTrendStrategy")
        without = _frame(400)
        with_atr = without.copy()
        with_atr["ATR_14"] = [
            _reference_prefix_atr(without, i) for i in range(len(without))
        ]
        for bar_index in (50, 120, 399):
            with self.subTest(bar_index=bar_index):
                self.assertEqual(
                    strategy.calculate_stops(with_atr, 1, "EURUSD",
                                             bar_index=bar_index),
                    strategy.calculate_stops(without, 1, "EURUSD",
                                             bar_index=bar_index),
                )


if __name__ == "__main__":
    unittest.main()
