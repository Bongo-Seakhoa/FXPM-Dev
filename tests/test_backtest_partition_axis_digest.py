"""build_backtest_partition_axis must seal the same axis it always has.

The digest was built by a per-bar Python loop that boxed every timestamp
through pandas and called hashlib.update() twice per bar - 30% of every
Backtester.run (CPM-025). Vectorizing it is only safe if the bytes fed to
sha256 are unchanged, so this pins the seal two independent ways:

  * GOLDEN - hex digests captured from the pre-vectorization implementation.
    Catches both implementations drifting together.
  * REFERENCE - the original loop, re-run here over randomized axes.
    Catches divergence the fixed cases happen to miss.
"""

import hashlib
import unittest

import numpy as np
import pandas as pd

import pm_core

_FREQ = {"M5": "5min", "M15": "15min", "M30": "30min", "H1": "h", "H4": "4h", "D1": "D"}
_SECS = {"M5": 300, "M15": 900, "M30": 1800, "H1": 3600, "H4": 14400, "D1": 86400}

# (event_axis_sha256, horizon_receipt_sha256) from the loop implementation.
GOLDEN = {
    "H1_1000_warm100": (
        "41ddc9c0d671cf46fad8b61649b4e5df16976f4f5f227207bb865be679589677",
        "a56d7a21e682589ee939d2facb8acb36415278b194a528550b1f0e9c96216f00",
    ),
    "H1_1000_warm0": (
        "f9c7b12867a595099adb5bdd72d67f81071ed7d6a8e2f0837be1708143e0d814",
        "78036ca93876f9d45fcbada022f9e44d0ba66a420d500eacfe9af9d7f1554447",
    ),
    "M5_500_warm7": (
        "031a7b0bedc9e47c0a74c3af0b65b205a847a1c1e7361f5329cb317428d9c819",
        "fc6f9bdadc92adaaddf4277c928d4ca5dc22d8a6181388defb0c7c886494e494",
    ),
    "D1_250_warm1": (
        "297f94017e83195d25596697fb071241ce8a791f01f14fdb70c4449d51c8593f",
        "b6b1a12a5c66cdfe9d8256db143bf84eef90099ef693acb681fadfb7e137c39e",
    ),
    "H4_64_explicit_close": (
        "f9bc2bf87b4ede607bf1f2975d7caa04a240a262ecaaa75f8d156f53a9a01c13",
        "81f86c023efff2bae6980edcee7e291695a932903739128a6a48b5a96e86f4d3",
    ),
    "H1_2_warm1_minimal": (
        "5c1098e8675e8e971c53df07cce975976fe160ace2d7a1186fb3e4ef771ae473",
        "b79a11f5df14d5309396991d6da8d7958543718c3aea7be01f9373ee846b76ec",
    ),
}

CASES = [
    ("H1_1000_warm100", "H1", 1000, 100, False),
    ("H1_1000_warm0", "H1", 1000, 0, False),
    ("M5_500_warm7", "M5", 500, 7, False),
    ("D1_250_warm1", "D1", 250, 1, False),
    ("H4_64_explicit_close", "H4", 64, 3, True),
    ("H1_2_warm1_minimal", "H1", 2, 1, False),
]


def _frame(timeframe, periods, explicit_close=False, start="2020-01-01"):
    idx = pd.date_range(start, periods=periods, freq=_FREQ[timeframe], tz="UTC")
    df = pd.DataFrame({"Close": np.arange(periods, dtype=float)}, index=idx)
    if explicit_close:
        df["_BAR_CLOSE_TS"] = idx + pd.to_timedelta(_SECS[timeframe], unit="s")
    return df


def _reference_event_axis_sha256(features, timeframe, warmup_bars):
    """The original per-bar loop, kept here as the equivalence oracle."""
    start_offset = max(0, int(warmup_bars or 0))
    opens = pd.DatetimeIndex(pd.to_datetime(features.index, utc=True, errors="coerce"))
    if "_BAR_CLOSE_TS" in features.columns:
        closes = pd.DatetimeIndex(
            pd.to_datetime(features["_BAR_CLOSE_TS"], utc=True, errors="coerce")
        )
    else:
        closes = opens + pd.to_timedelta(_SECS[str(timeframe).upper()], unit="s")
    digest = hashlib.sha256()
    digest.update(b"fxpm.backtest-partition-axis.v1\0")
    for opened, closed in zip(
        opens[start_offset:], closes[start_offset:], strict=True
    ):
        digest.update(int(opened.value).to_bytes(8, "big", signed=True))
        digest.update(int(closed.value).to_bytes(8, "big", signed=True))
    return digest.hexdigest()


class BacktestPartitionAxisDigestTest(unittest.TestCase):
    def test_golden_digests_are_unchanged(self):
        for name, tf, periods, warm, explicit in CASES:
            with self.subTest(case=name):
                body = pm_core.build_backtest_partition_axis(
                    _frame(tf, periods, explicit), timeframe=tf, warmup_bars=warm
                )
                event_gold, horizon_gold = GOLDEN[name]
                self.assertEqual(body["event_axis_sha256"], event_gold)
                self.assertEqual(body["horizon_receipt_sha256"], horizon_gold)

    def test_matches_reference_loop_on_randomized_axes(self):
        rng = np.random.default_rng(4242)
        for trial in range(60):
            tf = str(rng.choice(list(_FREQ)))
            periods = int(rng.integers(2, 900))
            warm = int(rng.integers(0, periods))
            explicit = bool(rng.integers(0, 2))
            start = f"20{rng.integers(10, 26):02d}-0{rng.integers(1, 10)}-1{rng.integers(0, 9)}"
            with self.subTest(trial=trial, tf=tf, periods=periods, warmup=warm):
                df = _frame(tf, periods, explicit, start=start)
                body = pm_core.build_backtest_partition_axis(
                    df, timeframe=tf, warmup_bars=warm
                )
                self.assertEqual(
                    body["event_axis_sha256"],
                    _reference_event_axis_sha256(df, tf, warm),
                )

    def test_rejects_invalid_axes(self):
        with self.assertRaises(ValueError):
            pm_core.build_backtest_partition_axis(
                pd.DataFrame(), timeframe="H1", warmup_bars=0
            )
        df = _frame("H1", 10)
        with self.assertRaises(ValueError):
            pm_core.build_backtest_partition_axis(df, timeframe="H1", warmup_bars=10)
        dupe = pd.concat([df.iloc[:5], df.iloc[4:5], df.iloc[5:]])
        with self.assertRaises(ValueError):
            pm_core.build_backtest_partition_axis(dupe, timeframe="H1", warmup_bars=0)
        with self.assertRaises(ValueError):
            pm_core.build_backtest_partition_axis(
                df, timeframe="NOT_A_TIMEFRAME", warmup_bars=0
            )

    def test_body_fields_describe_the_scored_region(self):
        df = _frame("H1", 500)
        body = pm_core.build_backtest_partition_axis(
            df, timeframe="H1", warmup_bars=120
        )
        self.assertEqual(body["scored_bars"], 380)
        self.assertEqual(body["warmup_bars"], 120)
        self.assertEqual(body["timeframe"], "H1")
        self.assertEqual(body["start_utc"], df.index[120].isoformat())


if __name__ == "__main__":
    unittest.main()
