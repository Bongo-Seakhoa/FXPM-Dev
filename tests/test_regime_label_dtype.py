"""Regime labels are a closed set of four and must stay categorical.

Storing them as pandas object columns keeps 250k interned Python strings alive
per column per frame. Measured on a 250k-bar M5 feature frame, the three label
columns cost 44.0 MB as object against 6.4 MB as category - 37.6 MB, 22% of the
entire frame, paid on every cached frame in every worker process.

The dtype is also a correctness boundary. A closed category set turns an
out-of-vocabulary regime into NaN instead of silently widening the column, and
the authoritative search path fails closed on a missing label rather than
scoring a cell against a regime that does not exist.
"""

import unittest

import pandas as pd

from pm_regime import REGIME_DTYPE, RegimeType, as_regime_category


LABEL_COLUMNS = ("REGIME_RAW", "REGIME", "REGIME_LIVE")


class RegimeLabelDtypeTests(unittest.TestCase):
    def test_category_set_is_exactly_the_four_regimes(self) -> None:
        self.assertEqual(list(REGIME_DTYPE.categories), list(RegimeType.ALL))
        self.assertEqual(len(RegimeType.ALL), 4)

    def test_labels_round_trip_without_altering_values(self) -> None:
        values = ["TREND", "CHOP", "RANGE", "BREAKOUT", "CHOP"]
        cat = as_regime_category(values)
        self.assertEqual([str(v) for v in cat], values)

    def test_out_of_vocabulary_label_becomes_missing_not_a_new_category(self) -> None:
        """A label outside the closed set must not widen the column."""
        cat = as_regime_category(["TREND", "NOT_A_REGIME"])
        self.assertEqual(list(cat.categories), list(RegimeType.ALL))
        self.assertTrue(pd.isna(cat[1]))

    def test_iteration_yields_plain_str_for_the_fail_closed_label_check(self) -> None:
        """pm_authoritative_search_execution rejects any label that is not `str`.

        It iterates ``labels.array`` and applies ``type(label) is not str``. A
        Categorical must yield real Python strings there, or every cell would
        fail closed on REGIME_SUPPORT_LABEL_INVALID.
        """
        series = pd.Series(as_regime_category(list(RegimeType.ALL)))
        seen = set()
        for raw in series.array:
            label = getattr(raw, "value", raw)
            seen.add(type(label))
            self.assertIs(type(label), str)
        self.assertEqual(seen, {str})

    def test_missing_labels_are_detected_by_isna(self) -> None:
        """The search path guards with ``labels.isna().any()``."""
        series = pd.Series(as_regime_category(["TREND", None]))
        self.assertEqual(series.isna().tolist(), [False, True])

    def test_shift_preserves_the_categorical_dtype(self) -> None:
        """REGIME_LIVE is built as REGIME.shift(1); dtype must survive it."""
        series = pd.Series(as_regime_category(["TREND", "CHOP", "RANGE"]))
        shifted = series.shift(1)
        self.assertEqual(str(shifted.dtype), "category")
        self.assertTrue(pd.isna(shifted.iloc[0]))
        self.assertEqual(str(shifted.iloc[1]), "TREND")

    def test_computed_feature_frame_keeps_labels_categorical(self) -> None:
        """Guards the regression on the frame the optimizer actually consumes."""
        import numpy as np

        from pm_core import FeatureComputer

        periods = 900
        idx = pd.date_range("2024-01-01", periods=periods, freq="5min", tz="UTC")
        rng = np.random.default_rng(7)
        close = 1.10 + np.cumsum(rng.normal(0.0, 0.0002, periods))
        frame = pd.DataFrame(
            {
                "Open": close,
                "High": close + 0.0004,
                "Low": close - 0.0004,
                "Close": close,
                "Volume": rng.integers(50, 500, periods).astype(float),
            },
            index=idx,
        )
        features = FeatureComputer.compute_all(frame, symbol="EURUSD", timeframe="M5")
        present = [c for c in LABEL_COLUMNS if c in features.columns]
        self.assertTrue(present, "no regime label columns were produced")
        for column in present:
            self.assertEqual(
                str(features[column].dtype),
                "category",
                f"{column} regressed to a non-categorical dtype; "
                "that costs ~22% of every cached feature frame",
            )


if __name__ == "__main__":
    unittest.main()
