"""Test that all 42 strategies have consistent param grids matching default params."""
import unittest
from pm_strategies import StrategyRegistry


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

    def test_strategy_count_is_42(self):
        self.assertEqual(StrategyRegistry.count(), 42)

    def test_all_strategies_instantiate(self):
        """Every registered strategy can be instantiated with defaults."""
        for name in StrategyRegistry.list_all():
            with self.subTest(strategy=name):
                strat = StrategyRegistry.get(name)
                self.assertIsNotNone(strat)
                self.assertEqual(strat.name, name)


if __name__ == "__main__":
    unittest.main()
