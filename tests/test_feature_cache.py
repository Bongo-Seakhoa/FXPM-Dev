import logging
import os
import json
import shutil
import unittest
import warnings
from pathlib import Path
import pandas as pd

from pm_core import FeatureComputer, PipelineConfig
from pm_pipeline import OptimizationPipeline
from pm_strategies import EMACrossoverStrategy, StrategyRegistry


class FeatureCacheTests(unittest.TestCase):
    def setUp(self):
        FeatureComputer.configure_cache(
            24,
            disk_enabled=False,
            disk_dir=None,
            disk_max_gb=0,
            disk_min_free_gb=0,
        )
        FeatureComputer.clear_cache()
        FeatureComputer.get_cache_stats(reset=True)

    def tearDown(self):
        FeatureComputer.configure_cache(
            24,
            disk_enabled=False,
            disk_dir=None,
            disk_max_gb=0,
            disk_min_free_gb=0,
        )
        FeatureComputer.clear_cache()
        FeatureComputer.get_cache_stats(reset=True)

    def _sample_ohlcv(self, rows=80):
        return pd.DataFrame(
            {
                "Open": [1.0 + i * 0.01 for i in range(rows)],
                "High": [1.1 + i * 0.01 for i in range(rows)],
                "Low": [0.9 + i * 0.01 for i in range(rows)],
                "Close": [1.0 + i * 0.01 for i in range(rows)],
                "Volume": [100] * rows,
            },
            index=pd.date_range("2026-01-01", periods=rows, freq="h"),
        )

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

    def test_disk_feature_cache_round_trips_without_memory_cache(self):
        tmp = Path("artifact/fxpm_runtime/.tmp_pytest/test_feature_disk_cache_roundtrip")
        shutil.rmtree(tmp, ignore_errors=True)
        tmp.mkdir(parents=True, exist_ok=True)
        try:
            FeatureComputer.configure_cache(
                0,
                disk_enabled=True,
                disk_dir=tmp,
                disk_max_gb=0.01,
            )
            df = self._sample_ohlcv()
            first = FeatureComputer.compute_all(df, symbol="TEST", timeframe="H1", regime_params_file="")
            self.assertTrue((tmp).exists())
            FeatureComputer.clear_cache()
            FeatureComputer.get_cache_stats(reset=True)

            second = FeatureComputer.compute_all(df, symbol="TEST", timeframe="H1", regime_params_file="")
            stats = FeatureComputer.get_cache_stats()

            self.assertTrue(first.equals(second))
            self.assertEqual(stats["memory_cap"], 0)
            self.assertEqual(stats["disk_hits"], 1)
            self.assertGreater(stats["disk_bytes_read"], 0)
        finally:
            shutil.rmtree(tmp, ignore_errors=True)

    def test_disk_feature_cache_key_hashes_full_source_not_sample_only(self):
        tmp = Path("artifact/fxpm_runtime/.tmp_pytest/test_feature_disk_cache_full_hash")
        shutil.rmtree(tmp, ignore_errors=True)
        tmp.mkdir(parents=True, exist_ok=True)
        try:
            FeatureComputer.configure_cache(
                0,
                disk_enabled=True,
                disk_dir=tmp,
                disk_max_gb=0.01,
            )
            df = self._sample_ohlcv()
            FeatureComputer.compute_all(df, symbol="TEST", timeframe="H1", regime_params_file="")
            FeatureComputer.clear_cache()
            FeatureComputer.get_cache_stats(reset=True)

            changed = df.copy()
            changed.iloc[len(changed) // 3, changed.columns.get_loc("Close")] = 99.0
            result = FeatureComputer.compute_all(changed, symbol="TEST", timeframe="H1", regime_params_file="")
            stats = FeatureComputer.get_cache_stats()

            self.assertEqual(float(result.iloc[len(changed) // 3]["Close"]), 99.0)
            self.assertEqual(stats["disk_hits"], 0)
            self.assertEqual(stats["disk_misses"], 1)
            self.assertEqual(stats["disk_writes"], 1)
        finally:
            shutil.rmtree(tmp, ignore_errors=True)

    def test_disk_feature_cache_respects_free_space_reserve(self):
        tmp = Path("artifact/fxpm_runtime/.tmp_pytest/test_feature_disk_cache_reserve")
        shutil.rmtree(tmp, ignore_errors=True)
        tmp.mkdir(parents=True, exist_ok=True)
        try:
            FeatureComputer.configure_cache(
                0,
                disk_enabled=True,
                disk_dir=tmp,
                disk_max_gb=0.01,
                disk_min_free_gb=999999.0,
            )
            df = self._sample_ohlcv()
            FeatureComputer.compute_all(df, symbol="TEST", timeframe="H1", regime_params_file="")
            stats = FeatureComputer.get_cache_stats()

            self.assertEqual(stats["disk_writes"], 0)
            self.assertGreaterEqual(stats["disk_skipped_storage"], 1)
            self.assertEqual(list(tmp.glob("feature_*")), [])
        finally:
            shutil.rmtree(tmp, ignore_errors=True)

    def test_regime_params_content_change_invalidates_feature_cache_key(self):
        tmp = Path("artifact/fxpm_runtime/.tmp_pytest/test_feature_regime_fingerprint")
        shutil.rmtree(tmp, ignore_errors=True)
        tmp.mkdir(parents=True, exist_ok=True)
        try:
            regime_file = tmp / "regime_params.json"
            regime_file.write_text('{"a":1}', encoding="utf-8")
            df = self._sample_ohlcv()
            first_key = FeatureComputer._make_cache_key(df, "TEST", "H1", str(regime_file))
            regime_file.write_text('{"a":2}', encoding="utf-8")
            second_key = FeatureComputer._make_cache_key(df, "TEST", "H1", str(regime_file))

            self.assertNotEqual(first_key, second_key)
        finally:
            shutil.rmtree(tmp, ignore_errors=True)

    def test_disk_feature_cache_prunes_whole_entry_groups(self):
        tmp = Path("artifact/fxpm_runtime/.tmp_pytest/test_feature_disk_cache_prune")
        shutil.rmtree(tmp, ignore_errors=True)
        tmp.mkdir(parents=True, exist_ok=True)
        try:
            FeatureComputer.configure_cache(
                0,
                disk_enabled=True,
                disk_dir=tmp,
                disk_max_gb=0.00001,
                disk_min_free_gb=0,
            )
            old_parquet = tmp / "feature_old.parquet"
            old_manifest = tmp / "feature_old.json"
            new_parquet = tmp / "feature_new.parquet"
            new_manifest = tmp / "feature_new.json"
            for path in [old_parquet, old_manifest, new_parquet, new_manifest]:
                path.write_bytes(b"x" * 4096)
            old_time = 1_700_000_000
            new_time = old_time + 100
            for path in [old_parquet, old_manifest]:
                os.utime(path, (old_time, old_time))
            for path in [new_parquet, new_manifest]:
                os.utime(path, (new_time, new_time))

            FeatureComputer._prune_disk_cache()

            self.assertFalse(old_parquet.exists())
            self.assertFalse(old_manifest.exists())
            self.assertTrue(new_parquet.exists() or new_manifest.exists())
        finally:
            shutil.rmtree(tmp, ignore_errors=True)

    def test_disk_feature_cache_prune_ignores_inflight_temp_files(self):
        tmp = Path("artifact/fxpm_runtime/.tmp_pytest/test_feature_disk_cache_prune_temp")
        shutil.rmtree(tmp, ignore_errors=True)
        tmp.mkdir(parents=True, exist_ok=True)
        try:
            FeatureComputer.configure_cache(
                0,
                disk_enabled=True,
                disk_dir=tmp,
                disk_max_gb=0.000001,
                disk_min_free_gb=0,
            )
            temp_file = tmp / "feature_abc.123.tmp.parquet"
            old_file = tmp / "feature_old.parquet"
            temp_file.write_bytes(b"x" * 4096)
            old_file.write_bytes(b"x" * 4096)

            FeatureComputer._prune_disk_cache()

            self.assertTrue(temp_file.exists())
            self.assertFalse(old_file.exists())
        finally:
            shutil.rmtree(tmp, ignore_errors=True)

    def test_disk_feature_cache_invalidates_manifest_shape_mismatch(self):
        tmp = Path("artifact/fxpm_runtime/.tmp_pytest/test_feature_disk_cache_manifest_shape")
        shutil.rmtree(tmp, ignore_errors=True)
        tmp.mkdir(parents=True, exist_ok=True)
        try:
            FeatureComputer.configure_cache(
                0,
                disk_enabled=True,
                disk_dir=tmp,
                disk_max_gb=0.01,
                disk_min_free_gb=0,
            )
            df = self._sample_ohlcv()
            FeatureComputer.compute_all(df, symbol="TEST", timeframe="H1", regime_params_file="")
            FeatureComputer.clear_cache()
            FeatureComputer.get_cache_stats(reset=True)
            manifest_path = next(tmp.glob("feature_*.json"))
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            manifest["rows"] = -1
            manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

            FeatureComputer.compute_all(df, symbol="TEST", timeframe="H1", regime_params_file="")
            stats = FeatureComputer.get_cache_stats()

            self.assertGreaterEqual(stats["disk_invalidations"], 1)
            self.assertEqual(stats["disk_hits"], 0)
        finally:
            shutil.rmtree(tmp, ignore_errors=True)

    def test_pipeline_precompute_feature_cache_warms_disk_cache(self):
        tmp = Path("artifact/fxpm_runtime/.tmp_pytest/test_pipeline_feature_precompute")
        data_dir = tmp / "data"
        cache_dir = tmp / "feature_cache"
        shutil.rmtree(tmp, ignore_errors=True)
        data_dir.mkdir(parents=True, exist_ok=True)
        try:
            df = self._sample_ohlcv(rows=240).reset_index().rename(columns={"index": "time"})
            df.to_csv(data_dir / "TEST_M5.csv", index=False)
            cfg = PipelineConfig(
                research_mode=True,
                production_split_mode="bleeding_edge",
                data_dir=data_dir,
                timeframes=["M5"],
                feature_cache_max_entries=0,
                feature_disk_cache_enabled=True,
                feature_disk_cache_dir=str(cache_dir),
                feature_disk_cache_max_gb=0.01,
                feature_disk_cache_min_free_gb=0,
                regime_params_file="",
            )
            pipeline = OptimizationPipeline(cfg)

            stats = pipeline.precompute_feature_cache(["TEST"])

            self.assertEqual(stats["symbols_completed"], 1)
            self.assertEqual(stats["symbols_failed"], 0)
            self.assertEqual(stats["timeframes_computed_or_retrieved"], 1)
            self.assertGreater(len(list(cache_dir.glob("feature_*"))), 0)
        finally:
            shutil.rmtree(tmp, ignore_errors=True)

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
