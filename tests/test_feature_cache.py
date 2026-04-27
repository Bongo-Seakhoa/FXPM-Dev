import logging
import unittest
import warnings
import pandas as pd

from pm_core import FeatureComputer
from pm_strategies import EMACrossoverStrategy, StrategyRegistry


class FeatureCacheTests(unittest.TestCase):
    def setUp(self):
        FeatureComputer.clear_cache()

    def test_compute_all_cache(self):
        df = pd.DataFrame({
            "Open": [1, 2, 3, 4, 5],
            "High": [2, 3, 4, 5, 6],
            "Low": [0.5, 1.5, 2.5, 3.5, 4.5],
            "Close": [1, 2, 3, 4, 5],
            "Volume": [100, 100, 100, 100, 100],
        })
        f1 = FeatureComputer.compute_all(df, symbol="TEST", timeframe="H1")
        f2 = FeatureComputer.compute_all(df, symbol="TEST", timeframe="H1")
        self.assertTrue(f1.equals(f2))
        f1["MUTATED"] = 1
        f3 = FeatureComputer.compute_all(df, symbol="TEST", timeframe="H1")
        self.assertNotIn("MUTATED", f3.columns)

    def test_cache_key_changes_when_ohlc_changes(self):
        df_a = pd.DataFrame({
            "Open": [1, 2, 3],
            "High": [2, 3, 4],
            "Low": [0.5, 1.5, 2.5],
            "Close": [1, 2, 3],
            "Volume": [100, 100, 100],
        })
        df_b = df_a.copy()
        df_b.loc[1, "Close"] = 9

        key_a = FeatureComputer._make_cache_key(df_a, "TEST", "H1", "regime_params.json")
        key_b = FeatureComputer._make_cache_key(df_b, "TEST", "H1", "regime_params.json")
        self.assertNotEqual(key_a, key_b)

    def test_compute_for_strategy_uses_parameter_aware_feature_request(self):
        df = pd.DataFrame({
            "Open": [1, 2, 3, 4, 5, 6],
            "High": [2, 3, 4, 5, 6, 7],
            "Low": [0.5, 1.5, 2.5, 3.5, 4.5, 5.5],
            "Close": [1, 2, 3, 4, 5, 6],
            "Volume": [100, 100, 100, 100, 100, 100],
        })
        strategy = StrategyRegistry.get(
            "EMAPullbackContinuationStrategy",
            fast_period=13,
            slow_period=50,
        )
        features = FeatureComputer.compute_for_strategy(df, strategy, symbol="TEST", timeframe="H1")

        self.assertIn("ATR_14", features.columns)
        self.assertIn("EMA_13", features.columns)
        self.assertIn("EMA_50", features.columns)
        self.assertNotIn("MACD", features.columns)

    def test_compute_all_cache_returns_isolated_copy(self):
        df = pd.DataFrame({
            "Open": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
            "High": [2, 3, 4, 5, 6, 7, 8, 9, 10, 11],
            "Low": [0.5, 1.5, 2.5, 3.5, 4.5, 5.5, 6.5, 7.5, 8.5, 9.5],
            "Close": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
            "Volume": [100] * 10,
        })
        f1 = FeatureComputer.compute_all(df, symbol="TEST", timeframe="H1")
        f1["TMP_MUTATED"] = 1
        f2 = FeatureComputer.compute_all(df, symbol="TEST", timeframe="H1")
        self.assertNotIn("TMP_MUTATED", f2.columns)

    def test_cache_capacity_is_24(self):
        """D8 — bumped 6 → 24 to fit ≥(5 TFs × 2 splits × ~2 tags) per symbol."""
        self.assertEqual(FeatureComputer._FEATURE_CACHE_MAX, 24)

    def test_cache_evicts_oldest_only_after_capacity_reached(self):
        """LRU-by-insertion: filling cap+1 entries drops the first inserted."""
        # The test does not rely on real OHLC — it pokes the put/get internals
        # directly so it is hermetic to indicator changes.
        FeatureComputer.clear_cache()
        cap = FeatureComputer._FEATURE_CACHE_MAX
        df = pd.DataFrame({"Close": [1.0]})
        for i in range(cap + 1):
            FeatureComputer._cache_put((i,), df)
        # First key should have been evicted; remaining cap keys should be hits.
        self.assertIsNone(FeatureComputer._cache_get((0,)))
        for i in range(1, cap + 1):
            self.assertIsNotNone(FeatureComputer._cache_get((i,)))

    def test_clear_cache_logs_hit_rate_when_counters_nonzero(self):
        """D8 — `clear_cache()` is the per-retrain boundary; INFO must surface
        the cache hit rate so operators see whether the bumped cap is paying off.
        """
        FeatureComputer.clear_cache()  # baseline reset
        df = pd.DataFrame({"Close": [1.0]})
        # 1 miss + 1 hit on the same key → 50% hit rate
        FeatureComputer._cache_get(("k",))
        FeatureComputer._cache_put(("k",), df)
        FeatureComputer._cache_get(("k",))
        with self.assertLogs("pm_core", level=logging.INFO) as captured:
            FeatureComputer.clear_cache()
        info_lines = [r.getMessage() for r in captured.records
                      if "FeatureCache stats" in r.getMessage()]
        self.assertEqual(len(info_lines), 1)
        msg = info_lines[0]
        self.assertIn("hits=1", msg)
        self.assertIn("misses=1", msg)
        self.assertIn("hit_rate=50.0%", msg)
        self.assertIn("cap=24", msg)

    def test_clear_cache_silent_when_no_traffic(self):
        """No accesses since last clear → no INFO line (avoids log spam)."""
        FeatureComputer.clear_cache()  # reset counters
        logger = logging.getLogger("pm_core")
        captured: list[logging.LogRecord] = []

        class _Capture(logging.Handler):
            def emit(self, record):
                captured.append(record)

        handler = _Capture()
        logger.addHandler(handler)
        try:
            FeatureComputer.clear_cache()
        finally:
            logger.removeHandler(handler)
        stats_lines = [r for r in captured if "FeatureCache stats" in r.getMessage()]
        self.assertEqual(stats_lines, [],
                         "No INFO when nothing has touched the cache since last clear")

    def test_clear_cache_resets_counters(self):
        FeatureComputer.clear_cache()
        FeatureComputer._cache_get(("k",))  # +1 miss
        self.assertEqual(FeatureComputer._FEATURE_CACHE_MISSES, 1)
        FeatureComputer.clear_cache()
        self.assertEqual(FeatureComputer._FEATURE_CACHE_HITS, 0)
        self.assertEqual(FeatureComputer._FEATURE_CACHE_MISSES, 0)

    def test_strategy_helpers_do_not_warn_on_slice_views(self):
        df = pd.DataFrame({
            "Open": list(range(1, 80)),
            "High": list(range(2, 81)),
            "Low": [x - 0.5 for x in range(1, 80)],
            "Close": list(range(1, 80)),
            "Volume": [100] * 79,
        })
        features = FeatureComputer.compute_all(df, symbol="TEST", timeframe="H1")
        sliced = features.iloc[:60]
        strategy = EMACrossoverStrategy()

        with warnings.catch_warnings():
            warnings.simplefilter("error", pd.errors.SettingWithCopyWarning)
            signals = strategy.generate_signals(sliced, "EURUSD")
        self.assertEqual(len(signals), len(sliced))


if __name__ == "__main__":
    unittest.main()
