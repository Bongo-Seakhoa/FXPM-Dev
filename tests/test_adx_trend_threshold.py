"""Tests for E3 (findings.html): per-asset-class `adx_trend_threshold`
default + ADX normalization re-anchored at the threshold.

Metals and crypto have different natural ADX trend floors than G10 FX. The
classic Wilder 25 misfires TREND on crypto chop and under-scores metal
trends. E3 re-anchors the normalization curves to an asset-class-derived
threshold while leaving FX / indices exactly at their legacy behavior.
"""
import json
import os
import tempfile
import unittest

import numpy as np

from pm_regime import (
    DEFAULT_ADX_TREND_THRESHOLD_BY_ASSET_CLASS,
    MarketRegimeDetector,
    RegimeParams,
    _infer_asset_class,
    _resolve_adx_trend_threshold,
    clear_regime_params_cache,
    load_regime_params,
)


class AssetClassInferenceTests(unittest.TestCase):
    def test_fx_majors_and_crosses(self):
        for sym in ("EURUSD", "GBPUSD", "USDJPY", "AUDNZD", "EURGBP", "CHFJPY"):
            with self.subTest(symbol=sym):
                self.assertEqual(_infer_asset_class(sym), "fx")

    def test_metals(self):
        for sym in ("XAUUSD", "XAUEUR", "XAGUSD", "XPTUSD", "XPDUSD", "GOLD", "SILVER"):
            with self.subTest(symbol=sym):
                self.assertEqual(_infer_asset_class(sym), "metal")

    def test_crypto_majors(self):
        for sym in ("BTCUSD", "ETHUSD", "LTCUSD", "XRPUSD", "SOLUSD",
                    "ADAUSD", "DOTUSD", "DOGEUSD", "BCHUSD", "BTCETH"):
            with self.subTest(symbol=sym):
                self.assertEqual(_infer_asset_class(sym), "crypto")

    def test_indices(self):
        for sym in ("SP500", "NAS100", "US30", "DAX", "FTSE", "NIKKEI", "JP225"):
            with self.subTest(symbol=sym):
                self.assertEqual(_infer_asset_class(sym), "index")

    def test_unknown_symbol_defaults_to_fx(self):
        # Safest fallback for a broad-discovery engine — unknown names should
        # not silently shift the trend anchor.
        self.assertEqual(_infer_asset_class("SOMETHING_WEIRD"), "fx")

    def test_empty_symbol_defaults_to_fx(self):
        self.assertEqual(_infer_asset_class(""), "fx")
        self.assertEqual(_infer_asset_class(None), "fx")


class AssetClassThresholdDefaultsTests(unittest.TestCase):
    def test_findings_spec_thresholds(self):
        # Spec: FX/index classic 25, metals 22 (slower buildup), crypto 30
        # (higher natural floor).
        self.assertEqual(DEFAULT_ADX_TREND_THRESHOLD_BY_ASSET_CLASS["fx"], 25.0)
        self.assertEqual(DEFAULT_ADX_TREND_THRESHOLD_BY_ASSET_CLASS["metal"], 22.0)
        self.assertEqual(DEFAULT_ADX_TREND_THRESHOLD_BY_ASSET_CLASS["crypto"], 30.0)
        self.assertEqual(DEFAULT_ADX_TREND_THRESHOLD_BY_ASSET_CLASS["index"], 25.0)

    def test_resolver_per_symbol(self):
        self.assertEqual(_resolve_adx_trend_threshold("EURUSD"), 25.0)
        self.assertEqual(_resolve_adx_trend_threshold("XAUUSD"), 22.0)
        self.assertEqual(_resolve_adx_trend_threshold("BTCUSD"), 30.0)
        self.assertEqual(_resolve_adx_trend_threshold("SP500"), 25.0)

    def test_dataclass_default_preserved(self):
        # The dataclass default is the FX anchor; anything else in the table is
        # applied via `load_regime_params` resolution, not at construction.
        self.assertEqual(RegimeParams().adx_trend_threshold, 25.0)


class LoadRegimeParamsAssetClassWiringTests(unittest.TestCase):
    """`load_regime_params` returns the asset-class threshold unless the JSON
    pinned one explicitly."""

    def setUp(self):
        clear_regime_params_cache()

    def tearDown(self):
        clear_regime_params_cache()

    def _missing_params_path(self) -> str:
        with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as tf:
            path = tf.name
        os.unlink(path)
        return path

    def test_metal_fallback_uses_metal_threshold(self):
        path = self._missing_params_path()
        p = load_regime_params("XAUUSD", "H1", filepath=path)
        self.assertEqual(p.adx_trend_threshold, 22.0)

    def test_crypto_fallback_uses_crypto_threshold(self):
        path = self._missing_params_path()
        p = load_regime_params("BTCUSD", "H1", filepath=path)
        self.assertEqual(p.adx_trend_threshold, 30.0)

    def test_fx_fallback_unchanged(self):
        path = self._missing_params_path()
        p = load_regime_params("EURUSD", "H1", filepath=path)
        self.assertEqual(p.adx_trend_threshold, 25.0)

    def test_hardcoded_default_branch_applies_asset_class(self):
        # Unknown timeframe falls through to RegimeParams() — the asset-class
        # override must still apply.
        path = self._missing_params_path()
        p = load_regime_params("ETHUSD", "ZZ_UNKNOWN_TF", filepath=path)
        self.assertEqual(p.adx_trend_threshold, 30.0)

    def test_timeframe_default_preserves_other_fields(self):
        # Re-anchoring should only touch adx_trend_threshold; the per-TF
        # k_confirm / k_hold / bb_squeeze_lookback overrides survive.
        path = self._missing_params_path()
        p = load_regime_params("XAUUSD", "D1", filepath=path)
        self.assertEqual(p.adx_trend_threshold, 22.0)
        self.assertEqual(p.k_confirm, 1)
        self.assertEqual(p.k_hold, 3)
        self.assertEqual(p.bb_squeeze_lookback, 60)

    def test_user_pinned_threshold_wins_over_asset_class(self):
        # Even for a metal symbol, if the user pinned 25.0 in JSON, we keep 25.
        raw = {"XAUUSD": {"H1": {"adx_trend_threshold": 25.0, "k_hold": 4}}}
        with tempfile.NamedTemporaryFile(suffix=".json", delete=False, mode="w") as tf:
            json.dump(raw, tf)
            path = tf.name
        try:
            p = load_regime_params("XAUUSD", "H1", filepath=path)
            self.assertEqual(p.adx_trend_threshold, 25.0)
            self.assertEqual(p.k_hold, 4)
        finally:
            os.unlink(path)

    def test_user_json_without_threshold_applies_asset_class(self):
        # JSON entry exists but doesn't set adx_trend_threshold → still
        # gets the metal default.
        raw = {"XAUUSD": {"H1": {"k_hold": 4}}}
        with tempfile.NamedTemporaryFile(suffix=".json", delete=False, mode="w") as tf:
            json.dump(raw, tf)
            path = tf.name
        try:
            p = load_regime_params("XAUUSD", "H1", filepath=path)
            self.assertEqual(p.adx_trend_threshold, 22.0)
            self.assertEqual(p.k_hold, 4)
        finally:
            os.unlink(path)


class ADXNormalizationAnchoredTests(unittest.TestCase):
    """Both ADX normalization curves must re-scale to `threshold` while
    staying byte-identical to the legacy curves at the classic 25 anchor."""

    def setUp(self):
        self.detector = MarketRegimeDetector(RegimeParams())

    def _legacy_norm(self, a):
        r = np.zeros_like(a, dtype=float)
        m1 = a < 20
        r[m1] = a[m1] / 40
        m2 = (a >= 20) & (a < 40)
        r[m2] = 0.5 + (a[m2] - 20) / 40
        m3 = a >= 40
        r[m3] = np.minimum(1.0, 0.75 + (a[m3] - 40) / 80)
        return r

    def _legacy_mid(self, a):
        r = np.zeros_like(a, dtype=float)
        m1 = a < 15
        r[m1] = a[m1] / 30
        m2 = (a >= 15) & (a < 30)
        r[m2] = 0.5 + (30 - np.abs(a[m2] - 22.5)) / 30
        m3 = a >= 30
        r[m3] = np.maximum(0.0, 1.0 - (a[m3] - 30) / 40)
        return r

    def test_fx_threshold_matches_legacy_curve(self):
        adx = np.linspace(0, 80, 81)
        np.testing.assert_allclose(
            self.detector._normalize_adx_vectorized(adx, 25.0),
            self._legacy_norm(adx),
            rtol=1e-9,
        )

    def test_fx_threshold_matches_legacy_mid_curve(self):
        adx = np.linspace(0, 80, 81)
        np.testing.assert_allclose(
            self.detector._normalize_adx_mid_vectorized(adx, 25.0),
            self._legacy_mid(adx),
            rtol=1e-9,
        )

    def test_crypto_curve_shifts_right(self):
        # Same ADX=25 that's "moderate" (0.625) on FX (T=25) is only "weak"
        # (below 0.5) on crypto (T=30) — because crypto's trend floor is
        # higher. This is the point of E3.
        fx = self.detector._normalize_adx_vectorized(np.array([25.0]), 25.0)[0]
        crypto = self.detector._normalize_adx_vectorized(np.array([25.0]), 30.0)[0]
        self.assertLess(crypto, fx)

    def test_metal_curve_shifts_left(self):
        # Metals have a lower trend floor — the same ADX that's "weak" on FX
        # should score higher on a metal because 22 is the natural anchor.
        fx = self.detector._normalize_adx_vectorized(np.array([22.0]), 25.0)[0]
        metal = self.detector._normalize_adx_vectorized(np.array([22.0]), 22.0)[0]
        self.assertGreater(metal, fx)

    def test_scalar_helpers_match_vectorized(self):
        # _normalize_adx and _normalize_adx_vectorized must agree point-by-point.
        adx_vals = [0.0, 5.0, 15.0, 20.0, 25.0, 30.0, 40.0, 50.0, 60.0, 80.0]
        for T in (22.0, 25.0, 30.0):
            vec = self.detector._normalize_adx_vectorized(np.array(adx_vals), T)
            for i, v in enumerate(adx_vals):
                with self.subTest(threshold=T, adx=v):
                    self.assertAlmostEqual(
                        self.detector._normalize_adx(v, T), float(vec[i]), places=9
                    )

    def test_vectorized_norm_preserves_fractional_scores_for_integer_input(self):
        adx_vals = np.array([5, 20, 25, 40], dtype=int)
        vec = self.detector._normalize_adx_vectorized(adx_vals, 25.0)
        self.assertEqual(vec.dtype.kind, "f")
        self.assertAlmostEqual(float(vec[0]), 0.125, places=9)

    def test_mid_scalar_helpers_match_vectorized(self):
        adx_vals = [0.0, 10.0, 15.0, 22.5, 30.0, 40.0, 70.0]
        for T in (22.0, 25.0, 30.0):
            vec = self.detector._normalize_adx_mid_vectorized(np.array(adx_vals), T)
            for i, v in enumerate(adx_vals):
                with self.subTest(threshold=T, adx=v):
                    self.assertAlmostEqual(
                        self.detector._normalize_adx_mid(v, T), float(vec[i]), places=9
                    )

    def test_vectorized_mid_preserves_fractional_scores_for_integer_input(self):
        adx_vals = np.array([10, 15, 22, 30], dtype=int)
        vec = self.detector._normalize_adx_mid_vectorized(adx_vals, 25.0)
        self.assertEqual(vec.dtype.kind, "f")
        self.assertGreater(float(vec[0]), 0.0)
        self.assertLess(float(vec[0]), 1.0)


if __name__ == "__main__":
    unittest.main()
