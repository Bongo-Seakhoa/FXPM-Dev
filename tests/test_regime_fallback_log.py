"""Tests for findings.html §C5: load_regime_params() emits one INFO log
per (symbol, timeframe) fallback and exposes the record for the startup summary."""

import logging
import os
import tempfile
import unittest

from pm_regime import (
    DEFAULT_PARAMS_BY_TIMEFRAME,
    clear_regime_fallback_log,
    clear_regime_params_cache,
    get_regime_fallback_record,
    load_regime_params,
)


class RegimeFallbackLoggingTests(unittest.TestCase):
    def setUp(self):
        clear_regime_params_cache()  # also clears fallback log

    def tearDown(self):
        clear_regime_params_cache()

    def _missing_params_path(self) -> str:
        # A path that does not exist forces the no-symbol-entry branch every call.
        with tempfile.NamedTemporaryFile(suffix='.json', delete=False) as tf:
            path = tf.name
        os.unlink(path)
        return path

    def test_fallback_logged_once_per_symbol_tf(self):
        """Two calls for the same (symbol, tf) → exactly one INFO record entry."""
        path = self._missing_params_path()
        with self.assertLogs("pm_regime", level="INFO") as cm:
            load_regime_params("EURUSD", "H1", filepath=path)
            load_regime_params("EURUSD", "H1", filepath=path)
        # The one INFO line we care about — additional lines from cache load are tolerated.
        fallback_lines = [m for m in cm.output if "regime_params fallback" in m and "EURUSD/H1" in m]
        self.assertEqual(len(fallback_lines), 1)

    def test_record_tracks_all_distinct_pairs(self):
        path = self._missing_params_path()
        load_regime_params("EURUSD", "H1", filepath=path)
        load_regime_params("EURUSD", "H4", filepath=path)
        load_regime_params("GBPUSD", "H1", filepath=path)
        load_regime_params("EURUSD", "H1", filepath=path)  # duplicate of first
        rec = get_regime_fallback_record()
        self.assertIn(("EURUSD", "H1"), rec)
        self.assertIn(("EURUSD", "H4"), rec)
        self.assertIn(("GBPUSD", "H1"), rec)
        self.assertEqual(len(rec), 3)

    def test_source_label_for_known_timeframe(self):
        """Known TF in DEFAULT_PARAMS_BY_TIMEFRAME → source 'timeframe_default'."""
        path = self._missing_params_path()
        # Pick any TF that is in the defaults table.
        known_tf = next(iter(DEFAULT_PARAMS_BY_TIMEFRAME.keys()))
        load_regime_params("XYZ", known_tf, filepath=path)
        self.assertEqual(get_regime_fallback_record()[("XYZ", known_tf)], "timeframe_default")

    def test_source_label_for_unknown_timeframe(self):
        """TF not in defaults → source 'hardcoded_default'."""
        path = self._missing_params_path()
        load_regime_params("XYZ", "ZZ_BOGUS_TF", filepath=path)
        self.assertEqual(
            get_regime_fallback_record()[("XYZ", "ZZ_BOGUS_TF")], "hardcoded_default"
        )

    def test_clear_resets_log(self):
        path = self._missing_params_path()
        load_regime_params("EURUSD", "H1", filepath=path)
        self.assertTrue(get_regime_fallback_record())
        clear_regime_fallback_log()
        self.assertFalse(get_regime_fallback_record())


class DefaultParamsByTimeframeTests(unittest.TestCase):
    """E5 — D1 hysteresis `k_hold` lifted 2 → 3.

    Findings.html: D1 winners are overnight-holding; the prior `k_hold=2` let
    a single-bar D1 spike flip the held regime before the next evaluation.
    Bumping to `3` blocks the one-bar flip without affecting intraday TFs.
    """

    def test_d1_k_hold_is_three(self):
        self.assertEqual(DEFAULT_PARAMS_BY_TIMEFRAME["D1"].k_hold, 3)

    def test_d1_other_fields_unchanged(self):
        # k_confirm and gap_min stay where they were so the change is scoped
        # purely to the hold window.
        d1 = DEFAULT_PARAMS_BY_TIMEFRAME["D1"]
        self.assertEqual(d1.k_confirm, 1)
        self.assertAlmostEqual(d1.gap_min, 0.08, places=6)

    def test_intraday_timeframes_unaffected(self):
        # Lock the rest of the table so an accidental edit to D1 cannot
        # silently shift another row.
        expected = {
            "M5":  (3, 0.10, 5),
            "M15": (3, 0.10, 4),
            "M30": (2, 0.10, 4),
            "H1":  (2, 0.10, 3),
            "H4":  (2, 0.08, 2),
        }
        for tf, (k_confirm, gap_min, k_hold) in expected.items():
            with self.subTest(timeframe=tf):
                params = DEFAULT_PARAMS_BY_TIMEFRAME[tf]
                self.assertEqual(params.k_confirm, k_confirm)
                self.assertAlmostEqual(params.gap_min, gap_min, places=6)
                self.assertEqual(params.k_hold, k_hold)


class BBSqueezeLookbackTimeframeTests(unittest.TestCase):
    """E4 — BB-squeeze lookback is timeframe-aware.

    Flat 200-bar lookback was ~16h on M5 (dense, slow to flag a squeeze) and
    ~9 months on D1 (stale). Findings.html prescribes M5=50, M15=80, H1=50,
    H4=50, D1=60; M30 stays on the dataclass baseline (200) since findings
    didn't specify it.
    """

    def test_per_tf_lookback_matches_findings_spec(self):
        expected = {
            "M5":  50,
            "M15": 80,
            "H1":  50,
            "H4":  50,
            "D1":  60,
        }
        for tf, lookback in expected.items():
            with self.subTest(timeframe=tf):
                self.assertEqual(
                    DEFAULT_PARAMS_BY_TIMEFRAME[tf].bb_squeeze_lookback, lookback
                )

    def test_m30_keeps_dataclass_default(self):
        # M30 was intentionally not in the findings spec — assert it still
        # carries the dataclass default so a future edit to add M30 is explicit.
        from pm_regime import RegimeParams
        self.assertEqual(
            DEFAULT_PARAMS_BY_TIMEFRAME["M30"].bb_squeeze_lookback,
            RegimeParams().bb_squeeze_lookback,
        )

    def test_dataclass_default_unchanged(self):
        # Symbols / timeframes outside the table still get the 200-bar default.
        from pm_regime import RegimeParams
        self.assertEqual(RegimeParams().bb_squeeze_lookback, 200)


if __name__ == "__main__":
    unittest.main()
