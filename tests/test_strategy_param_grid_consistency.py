"""Test that all 47 strategies have consistent param grids matching default params."""
import logging
import unittest
from pm_strategies import (
    StrategyRegistry,
    _LARGE_GRID_WARN_EMITTED,
    _LARGE_PARAM_GRID_WARN_THRESHOLD,
)


class StrategyParamGridConsistencyTests(unittest.TestCase):

    def test_all_strategies_grid_keys_match_defaults(self):
        """Every key in get_param_grid() must exist in get_default_params()."""
        for name in StrategyRegistry.list_all():
            with self.subTest(strategy=name):
                strat = StrategyRegistry.get(name)
                defaults = strat.get_default_params()
                grid = strat.get_param_grid()
                for key in grid:
                    self.assertIn(
                        key, defaults,
                        f"{name}: grid key '{key}' not in defaults {list(defaults.keys())}"
                    )

    def test_strategy_count_is_47(self):
        self.assertEqual(StrategyRegistry.count(), 47)

    def test_all_strategies_instantiate(self):
        """Every registered strategy can be instantiated with defaults."""
        for name in StrategyRegistry.list_all():
            with self.subTest(strategy=name):
                strat = StrategyRegistry.get(name)
                self.assertIsNotNone(strat)
                self.assertEqual(strat.name, name)


class CartesianGridSizeObservabilityTests(unittest.TestCase):
    """Phase D4 — `BaseStrategy.cartesian_grid_size()` and large-grid WARN.

    Pure observability — never used to prune or axis-fix. The warn surfaces
    coverage-fairness concerns to operators so they can opt-in to the D3
    family-aware budget or raise the per-strategy cap.
    """

    def setUp(self):
        # Reset the per-process emit cache so each test sees a fresh state
        _LARGE_GRID_WARN_EMITTED.clear()

    def test_cartesian_grid_size_matches_manual_product(self):
        for name in StrategyRegistry.list_all():
            with self.subTest(strategy=name):
                strat = StrategyRegistry.get(name)
                grid = strat.get_param_grid()
                expected = 1
                if not grid:
                    expected = 0
                else:
                    for values in grid.values():
                        n = len(values) if values else 0
                        if n == 0:
                            expected = 0
                            break
                        expected *= n
                self.assertEqual(strat.cartesian_grid_size(), expected)

    def test_warn_threshold_is_meaningful(self):
        self.assertEqual(_LARGE_PARAM_GRID_WARN_THRESHOLD, 1000)

    def test_warn_emitted_once_per_strategy_when_above_threshold(self):
        # Find a strategy whose cartesian exceeds the threshold (EMARibbonADX is ~46k)
        large = None
        for name in StrategyRegistry.list_all():
            strat = StrategyRegistry.get(name)
            if strat.cartesian_grid_size() > _LARGE_PARAM_GRID_WARN_THRESHOLD:
                large = strat
                break
        self.assertIsNotNone(large, "Repo should contain at least one large-cartesian strategy")

        with self.assertLogs("pm_strategies", level="WARNING") as captured:
            size_a = large.warn_if_param_grid_large()
            size_b = large.warn_if_param_grid_large()  # should be deduped
        self.assertEqual(size_a, size_b)
        self.assertGreater(size_a, _LARGE_PARAM_GRID_WARN_THRESHOLD)
        warn_lines = [r for r in captured.records if r.levelno >= logging.WARNING]
        self.assertEqual(len(warn_lines), 1, "WARN must be emitted exactly once per (strategy, size)")
        self.assertIn(large.name, warn_lines[0].getMessage())

    def test_warn_silent_for_small_grids(self):
        # Find a strategy whose cartesian is well under threshold
        small = None
        for name in StrategyRegistry.list_all():
            strat = StrategyRegistry.get(name)
            size = strat.cartesian_grid_size()
            if 0 < size <= _LARGE_PARAM_GRID_WARN_THRESHOLD:
                small = strat
                break
        self.assertIsNotNone(small)

        # assertNoLogs does not exist in older unittest; use assertLogs negative pattern
        logger = logging.getLogger("pm_strategies")
        prior_disabled = logger.disabled
        try:
            handler_records = []

            class _CaptureHandler(logging.Handler):
                def emit(self, record):
                    if record.levelno >= logging.WARNING:
                        handler_records.append(record)

            handler = _CaptureHandler()
            logger.addHandler(handler)
            small.warn_if_param_grid_large()
            logger.removeHandler(handler)
        finally:
            logger.disabled = prior_disabled
        self.assertEqual(handler_records, [])


if __name__ == "__main__":
    unittest.main()
