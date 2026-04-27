"""Phase C9 — helper short-circuit must gate on the per-DataFrame precomputed-params tag,
not on column presence alone.

Scenario (from findings §C9): a DataFrame carries columns named `KC_MID`, `KC_UPPER`,
`KC_LOWER` but the columns were written with a non-default multiplier. A strategy that
asks `_get_keltner(..., mult=2.0)` must not return those columns unless the DataFrame
explicitly claims they were computed with `(20, 20, 2.0)`.

Same logic covers `_get_bb`, `_get_adx_di`, `_get_stochastic`, `_get_macd`.
"""
import unittest

import numpy as np
import pandas as pd

from pm_core import FeatureComputer
from pm_strategies import (
    _get_adx_di,
    _get_bb,
    _get_keltner,
    _get_macd,
    _get_stochastic,
    _precomputed_matches,
    mark_precomputed,
)


def _make_ohlc(n: int = 300) -> pd.DataFrame:
    rng = np.random.default_rng(0)
    close = 100.0 + np.cumsum(rng.normal(0.0, 0.3, size=n))
    high = close + np.abs(rng.normal(0.2, 0.1, size=n))
    low = close - np.abs(rng.normal(0.2, 0.1, size=n))
    open_ = close + rng.normal(0.0, 0.05, size=n)
    return pd.DataFrame(
        {
            "Open": open_,
            "High": high,
            "Low": low,
            "Close": close,
            "Volume": np.ones(n),
        }
    )


class PrecomputedTagGatesShortCircuit(unittest.TestCase):
    def test_full_compute_stamps_all_default_tags(self):
        df = _make_ohlc()
        features = FeatureComputer.compute_all(df, "EURUSD", "H1")
        self.assertTrue(_precomputed_matches(features, "KC", (20, 20, 2.0)))
        self.assertTrue(_precomputed_matches(features, "BB_20", (20, 2.0)))
        self.assertTrue(_precomputed_matches(features, "ADX", (14,)))
        self.assertTrue(_precomputed_matches(features, "STOCH", (14, 3)))
        self.assertTrue(_precomputed_matches(features, "MACD", (12, 26, 9)))

    def test_keltner_short_circuit_trusts_stamped_frame(self):
        df = _make_ohlc()
        features = FeatureComputer.compute_all(df, "EURUSD", "H1")
        mid, upper, lower = _get_keltner(features, 20, 20, 2.0)
        self.assertTrue(mid.equals(features["KC_MID"]))
        self.assertTrue(upper.equals(features["KC_UPPER"]))
        self.assertTrue(lower.equals(features["KC_LOWER"]))

    def test_keltner_refuses_short_circuit_on_untagged_columns(self):
        # Simulate a DataFrame that carries KC_* columns that were NOT computed
        # with the default params. No tag is set, so the helper must not trust
        # the column presence.
        features = _make_ohlc().copy()
        features["KC_MID"] = 0.0
        features["KC_UPPER"] = 1.0
        features["KC_LOWER"] = -1.0
        mid, upper, lower = _get_keltner(features, 20, 20, 2.0)
        # Helper must compute a real KC, not hand back the injected garbage.
        self.assertFalse((mid == 0.0).all())
        self.assertFalse((upper == 1.0).all())
        self.assertFalse((lower == -1.0).all())

    def test_keltner_refuses_short_circuit_on_mismatched_tag(self):
        # Frame claims KC_* was computed with a DIFFERENT multiplier. Helper must
        # not return the stored columns for a mult=2.0 caller.
        features = _make_ohlc().copy()
        features["KC_MID"] = 0.0
        features["KC_UPPER"] = 1.0
        features["KC_LOWER"] = -1.0
        mark_precomputed(features, KC=(20, 20, 1.5))
        mid, _upper, _lower = _get_keltner(features, 20, 20, 2.0)
        self.assertFalse((mid == 0.0).all())

    def test_bb_short_circuit_refuses_untagged_columns(self):
        features = _make_ohlc().copy()
        features["BB_MID_20"] = 0.0
        features["BB_UPPER_20"] = 1.0
        features["BB_LOWER_20"] = -1.0
        mid, upper, lower = _get_bb(features, 20, 2.0)
        self.assertFalse((mid == 0.0).all())
        self.assertFalse((upper == 1.0).all())
        self.assertFalse((lower == -1.0).all())

    def test_bb_short_circuit_trusts_stamped_frame(self):
        df = _make_ohlc()
        features = FeatureComputer.compute_all(df, "EURUSD", "H1")
        mid, upper, lower = _get_bb(features, 20, 2.0)
        self.assertTrue(mid.equals(features["BB_MID_20"]))
        self.assertTrue(upper.equals(features["BB_UPPER_20"]))
        self.assertTrue(lower.equals(features["BB_LOWER_20"]))

    def test_adx_short_circuit_refuses_untagged_columns(self):
        features = _make_ohlc().copy()
        features["ADX"] = 0.0
        features["PLUS_DI"] = 1.0
        features["MINUS_DI"] = -1.0
        adx, plus_di, minus_di = _get_adx_di(features, 14)
        self.assertFalse((adx == 0.0).all())
        self.assertFalse((plus_di == 1.0).all())
        self.assertFalse((minus_di == -1.0).all())

    def test_adx_short_circuit_trusts_stamped_frame(self):
        df = _make_ohlc()
        features = FeatureComputer.compute_all(df, "EURUSD", "H1")
        adx, plus_di, minus_di = _get_adx_di(features, 14)
        self.assertTrue(adx.equals(features["ADX"]))
        self.assertTrue(plus_di.equals(features["PLUS_DI"]))
        self.assertTrue(minus_di.equals(features["MINUS_DI"]))

    def test_stoch_short_circuit_refuses_untagged_columns(self):
        features = _make_ohlc().copy()
        features["STOCH_K"] = 0.0
        features["STOCH_D"] = 1.0
        k, d = _get_stochastic(features, 14, 3)
        self.assertFalse((k == 0.0).all())
        self.assertFalse((d == 1.0).all())

    def test_macd_short_circuit_refuses_untagged_columns(self):
        features = _make_ohlc().copy()
        features["MACD"] = 0.0
        features["MACD_SIGNAL"] = 1.0
        features["MACD_HIST"] = -1.0
        macd, sig, hist = _get_macd(features, 12, 26, 9)
        self.assertFalse((macd == 0.0).all())
        self.assertFalse((sig == 1.0).all())
        self.assertFalse((hist == -1.0).all())

    def test_non_default_params_never_short_circuit(self):
        # Even with a fully-stamped frame, non-default params must fall through
        # to the cache / compute path (short-circuit is only for exact-default params).
        df = _make_ohlc()
        features = FeatureComputer.compute_all(df, "EURUSD", "H1")
        # KC short-circuit only fires at (20, 20, 2.0); (20, 20, 1.5) must bypass.
        mid, upper, lower = _get_keltner(features, 20, 20, 1.5)
        # Recomputed band width = 1.5 * ATR_20 * 2, different from the stored 2.0 * ATR_20 * 2.
        self.assertFalse(upper.equals(features["KC_UPPER"]))
        self.assertFalse(lower.equals(features["KC_LOWER"]))

    def test_mark_precomputed_is_idempotent_and_accumulates(self):
        features = _make_ohlc().copy()
        mark_precomputed(features, KC=(20, 20, 2.0))
        mark_precomputed(features, BB_20=(20, 2.0))
        self.assertTrue(_precomputed_matches(features, "KC", (20, 20, 2.0)))
        self.assertTrue(_precomputed_matches(features, "BB_20", (20, 2.0)))
        # Overwrite: should reflect the new tuple.
        mark_precomputed(features, KC=(20, 20, 1.5))
        self.assertTrue(_precomputed_matches(features, "KC", (20, 20, 1.5)))
        self.assertFalse(_precomputed_matches(features, "KC", (20, 20, 2.0)))


if __name__ == "__main__":
    unittest.main()
