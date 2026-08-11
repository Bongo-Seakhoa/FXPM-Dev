"""The metrics path must give numerically identical answers after vectorization.

Drawdown and the safe_float conversions ran per bar in Python - 106,697 calls
on an H1 frame. Vectorizing is only safe where the operation is independent of
accumulation order, so this covers exactly those sites and asserts EXACT
equality, not approximate:

  * max/accumulate reductions - np.maximum.accumulate matches a sequential
    running max bit for bit
  * safe_float conversion - a per-element map, no accumulation

Deliberately NOT covered, because vectorizing them would change results:
expectancy_per_trade (Python sum(), pairwise summation reorders the adds) and
trade_sharpe_sortino_from_pnl (per-trade, not per-bar, so not hot anyway).
"""

import math
import unittest

import numpy as np

import pm_core
import pm_metrics


def _reference_safe_float_list(values):
    return [pm_metrics.safe_float(v) for v in values]


def _reference_core_drawdown(equity_curve):
    """pm_core.Backtester._max_drawdown_from_equity_curve, pre-vectorization."""
    peak = None
    max_dd = 0.0
    for raw_value in equity_curve or []:
        try:
            value = float(raw_value)
        except (TypeError, ValueError):
            continue
        if not math.isfinite(value):
            continue
        if peak is None or value > peak:
            peak = value
        if peak and peak > 0.0:
            max_dd = max(max_dd, (peak - value) / peak * 100.0)
    return max_dd


def _reference_metrics_drawdown(equity_curve):
    """pm_metrics.max_drawdown_from_equity, pre-vectorization."""
    values = _reference_safe_float_list(equity_curve)
    if not values:
        return 0.0, 0.0
    peak = values[0]
    max_dd_pct = 0.0
    max_dd_abs = 0.0
    for equity in values:
        if equity > peak:
            peak = equity
        drawdown_abs = max(0.0, peak - equity)
        drawdown_pct = (drawdown_abs / peak * 100.0) if peak > 0.0 else 0.0
        max_dd_abs = max(max_dd_abs, drawdown_abs)
        max_dd_pct = max(max_dd_pct, drawdown_pct)
    return max_dd_pct, max_dd_abs


def _reference_sharpe_sortino(returns, periods_per_year):
    arr = np.asarray(_reference_safe_float_list(returns), dtype=np.float64)
    if arr.size < 2:
        return 0.0, 0.0
    bpy = pm_metrics.safe_float(periods_per_year)
    if bpy <= 0.0:
        bpy = 252.0
    mean_return = float(np.mean(arr))
    std_return = float(np.std(arr, ddof=1)) if arr.size >= 2 else 0.0
    sharpe = (mean_return / std_return) * math.sqrt(bpy) if std_return > 1e-8 else 0.0
    negative = arr[arr < 0.0]
    if negative.size >= 2:
        downside_std = float(np.std(negative, ddof=1))
    elif negative.size == 1:
        downside_std = float(abs(negative[0]))
    else:
        downside_std = 0.0
    if downside_std > 1e-8:
        sortino = (mean_return / downside_std) * math.sqrt(bpy)
    elif mean_return > 0.0 and negative.size == 0:
        sortino = float("inf")
    else:
        sortino = 0.0
    return sharpe, sortino


NASTY = [
    [],
    [100.0],
    [0.0, 0.0, 0.0],
    [100.0, 90.0, 95.0, 80.0, 120.0],
    [100.0, float("nan"), 90.0],
    [100.0, float("inf"), 90.0],
    [100.0, float("-inf"), 90.0],
    [100.0, None, 90.0],
    [100.0, "abc", 90.0],
    [100.0, "95.5", 90.0],
    [-100.0, -200.0, -50.0],
    [0.0, -1.0, -2.0],
    [1e308, -1e308, 1e308],
    [1e-320, 0.0, -1e-320],
    [True, False, 1.0],
    list(np.linspace(100.0, 1.0, 50)),
    list(np.linspace(1.0, 100.0, 50)),
]


class MetricsVectorizationTest(unittest.TestCase):
    def _curves(self):
        rng = np.random.default_rng(20260811)
        for case in NASTY:
            yield case
        for _ in range(40):
            n = int(rng.integers(0, 600))
            yield list(10_000 + np.cumsum(rng.normal(0, 150, n)))
        for _ in range(10):
            n = int(rng.integers(2, 400))
            yield list(rng.normal(0, 1, n) * 10.0 ** int(rng.integers(-12, 12)))

    def test_core_drawdown_is_bit_identical(self):
        for i, curve in enumerate(self._curves()):
            with self.subTest(case=i, n=len(curve)):
                self.assertEqual(
                    pm_core.Backtester._max_drawdown_from_equity_curve(curve),
                    _reference_core_drawdown(curve),
                )

    def test_metrics_drawdown_is_bit_identical(self):
        for i, curve in enumerate(self._curves()):
            with self.subTest(case=i, n=len(curve)):
                self.assertEqual(
                    pm_metrics.max_drawdown_from_equity(curve),
                    _reference_metrics_drawdown(curve),
                )

    def test_sharpe_sortino_is_bit_identical(self):
        for bpy in (252.0, 0.0, -1.0, 6240.0):
            for i, series in enumerate(self._curves()):
                with self.subTest(case=i, bpy=bpy, n=len(series)):
                    self.assertEqual(
                        pm_metrics.sharpe_sortino_from_returns(series, bpy),
                        _reference_sharpe_sortino(series, bpy),
                    )

    def test_core_and_metrics_drawdown_agree_on_clean_curves(self):
        """The two independent drawdown implementations must not disagree."""
        rng = np.random.default_rng(7)
        for _ in range(25):
            curve = list(10_000 + np.cumsum(rng.normal(0, 200, 500)))
            self.assertAlmostEqual(
                pm_core.Backtester._max_drawdown_from_equity_curve(curve),
                pm_metrics.max_drawdown_from_equity(curve)[0],
                places=9,
            )


if __name__ == "__main__":
    unittest.main()
