"""Test that all strategies have consistent param grids matching default params."""
import logging
import unittest
from pm_current_refit_parameters import (
    CurrentRefitParameterCodecError,
    build_parameter_domain,
    validate_parameter_domain_member,
)
from pm_strategies import (
    StrategyRegistry,
    _LARGE_GRID_WARN_EMITTED,
    _LARGE_PARAM_GRID_WARN_THRESHOLD,
    _GLOBAL_SL_GRID,
    _GLOBAL_TP_GRID,
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

    def test_strategy_count_is_100(self):
        self.assertEqual(StrategyRegistry.count(), 100)

    def test_global_atr_exit_grid_contains_initial_anchor_ranges(self):
        for value in (1.0, 2.0, 3.0):
            self.assertIn(value, _GLOBAL_SL_GRID)
            self.assertIn(value, _GLOBAL_TP_GRID)

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

    def test_cartesian_grid_size_matches_legal_product(self):
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
                for group in strat.get_param_couplings():
                    for key in group['names']:
                        expected //= len(grid[key])
                    expected *= len(group['choices'])
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


class ApprovedCoupledDomainsTests(unittest.TestCase):
    EXPECTED_SIZES = {
        'RegimeTransitionBreakoutStrategy': 26730,
        'EMARibbonADXStrategy': 23760,
        'StochRSITrendGateStrategy': 23760,
        'SessionVWAPReversionStrategy': 13200,
        'MACDHistogramMomentumStrategy': 19800,
    }
    EXPECTED_PAIRS = {
        'StochRSITrendGateStrategy': (
            ('lower_band', 'upper_band'), ((10, 90), (15, 85), (20, 80), (30, 70))),
        'SessionVWAPReversionStrategy': (
            ('session_start_hour', 'session_end_hour'), ((0, 23), (7, 20), (8, 16))),
        'MACDHistogramMomentumStrategy': (
            ('macd_fast', 'macd_slow'), ((8, 21), (12, 26), (16, 34))),
    }

    def test_exact_approved_sizes_and_defaults_are_legal(self):
        for name, expected in self.EXPECTED_SIZES.items():
            with self.subTest(strategy=name):
                strategy = StrategyRegistry.get(name)
                domain = build_parameter_domain(
                    strategy.get_default_params(), strategy.get_param_grid(),
                    coupled_groups=strategy.get_param_couplings())
                self.assertEqual(domain['point_count'], expected)
                self.assertEqual(strategy.cartesian_grid_size(), expected)
                validate_parameter_domain_member(domain, strategy.get_default_params())
                self.assertEqual(len(strategy.get_param_grid()['sl_atr_mult']), 5)
                self.assertEqual(len(strategy.get_param_grid()['tp_atr_mult']), 11)

    def test_exact_pairs_and_flat_marginals_cannot_create_cross_pairs(self):
        for name, (names, rows) in self.EXPECTED_PAIRS.items():
            with self.subTest(strategy=name):
                strategy = StrategyRegistry.get(name)
                groups = strategy.get_param_couplings()
                self.assertEqual(groups, ({'names': names, 'choices': rows},))
                domain = build_parameter_domain(
                    strategy.get_default_params(), strategy.get_param_grid(), coupled_groups=groups)
                for row in rows:
                    parameters = {**strategy.get_default_params(), **dict(zip(names, row))}
                    wire = validate_parameter_domain_member(domain, parameters)
                    self.assertEqual(set(wire), set(parameters))
                    self.assertTrue(all(not key.startswith('__') for key in wire))
                invalid = {**strategy.get_default_params(), names[0]: rows[0][0], names[1]: rows[1][1]}
                with self.assertRaises(CurrentRefitParameterCodecError):
                    validate_parameter_domain_member(domain, invalid)

    def test_only_three_strategies_declare_couplings(self):
        self.assertEqual(
            {name for name in StrategyRegistry.list_all() if StrategyRegistry.get(name).get_param_couplings()},
            set(self.EXPECTED_PAIRS))


if __name__ == "__main__":
    unittest.main()
