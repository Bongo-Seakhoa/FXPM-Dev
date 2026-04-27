"""Phase D3 — fairness-preserving family-size-aware Optuna trial budget.

Verifies the search-budget contract that prevents large strategy families (e.g.,
EMARibbonADX, 46k+ cartesian) from receiving a coarser search than a 48-combo
family under a flat `max_param_combos`. Behavior is opt-in (`optuna_family_size_aware_budget`)
so the legacy flat-budget mode remains the default.
"""
import unittest

from pm_core import PipelineConfig
from pm_optuna import OptunaConfig


class FamilySizeAwareBudgetTests(unittest.TestCase):

    def _config(self, **overrides) -> OptunaConfig:
        cfg = OptunaConfig(
            n_trials=100,
            family_size_aware_budget=True,
            min_trials_per_strategy=50,
            max_trials_per_strategy=500,
            target_coverage_pct=0.10,
        )
        for key, val in overrides.items():
            setattr(cfg, key, val)
        return cfg

    def test_pipeline_config_exposes_d3_knobs_with_safe_defaults(self):
        cfg = PipelineConfig()
        self.assertTrue(hasattr(cfg, "optuna_family_size_aware_budget"))
        self.assertFalse(cfg.optuna_family_size_aware_budget,
                         "D3 must default to OFF — opt-in only so legacy behavior is preserved")
        self.assertGreaterEqual(cfg.optuna_min_trials_per_strategy, 1)
        self.assertGreater(cfg.optuna_max_trials_per_strategy, cfg.optuna_min_trials_per_strategy)
        self.assertGreater(cfg.optuna_target_coverage_pct, 0.0)
        self.assertLessEqual(cfg.optuna_target_coverage_pct, 1.0)

    def test_from_pipeline_config_propagates_d3_knobs(self):
        pcfg = PipelineConfig(
            optuna_family_size_aware_budget=True,
            optuna_min_trials_per_strategy=80,
            optuna_max_trials_per_strategy=600,
            optuna_target_coverage_pct=0.15,
        )
        ocfg = OptunaConfig.from_pipeline_config(pcfg)
        self.assertTrue(ocfg.family_size_aware_budget)
        self.assertEqual(ocfg.min_trials_per_strategy, 80)
        self.assertEqual(ocfg.max_trials_per_strategy, 600)
        self.assertAlmostEqual(ocfg.target_coverage_pct, 0.15)

    def test_flat_mode_caps_at_n_trials_and_returns_flat_label(self):
        cfg = OptunaConfig(n_trials=100, family_size_aware_budget=False)
        n, mode = cfg.resolve_trial_budget(48)
        self.assertEqual(mode, "flat")
        self.assertEqual(n, 48)  # capped at search space size
        n, mode = cfg.resolve_trial_budget(46_000)
        self.assertEqual(mode, "flat")
        self.assertEqual(n, 100)  # capped at n_trials

    def test_family_aware_lifts_large_family_above_flat_budget(self):
        """A 46k-combo family must receive >> 100 trials — the core D3 unfairness fix."""
        cfg = self._config()
        n, mode = cfg.resolve_trial_budget(46_000)
        self.assertEqual(mode, "family_aware")
        self.assertGreater(n, 100, "D3: 46k-combo family must get more trials than flat default")
        self.assertEqual(n, 500, "Should hit max_trials_per_strategy cap (10% of 46k = 4600 > 500)")

    def test_family_aware_floors_small_family_at_minimum(self):
        cfg = self._config()
        n, mode = cfg.resolve_trial_budget(48)
        self.assertEqual(mode, "family_aware")
        self.assertEqual(n, 48, "small family should not exceed cartesian size")

        n, mode = cfg.resolve_trial_budget(200)
        self.assertEqual(mode, "family_aware")
        # 10% of 200 = 20 < min(50). Floor lifts to 50, capped at search size 200.
        self.assertEqual(n, 50)

    def test_family_aware_caps_at_max_trials(self):
        cfg = self._config(max_trials_per_strategy=300)
        n, mode = cfg.resolve_trial_budget(10_000)
        self.assertEqual(mode, "family_aware")
        self.assertEqual(n, 300, "max_trials_per_strategy must hard-cap the budget")

    def test_family_aware_coverage_proportional_in_normal_band(self):
        cfg = self._config(min_trials_per_strategy=10, max_trials_per_strategy=10_000,
                           target_coverage_pct=0.20)
        # 1000 cartesian * 20% = 200
        n, mode = cfg.resolve_trial_budget(1000)
        self.assertEqual(mode, "family_aware")
        self.assertEqual(n, 200)


if __name__ == "__main__":
    unittest.main()
