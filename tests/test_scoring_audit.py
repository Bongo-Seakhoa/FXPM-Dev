"""Tests for scoring audit implementation (Steps A-F).

Covers:
- Candidate descent validation (rank-1 fail, rank-2 pass; all fail → no-winner)
- DD hard-gate invariants
- Scoring feature flag behavior
- Continuous DD penalty vs discrete buckets
- Sortino blend, tail risk, consistency, trade frequency
- Weak-train exception tightening
- Optuna objective blend config
- Sigmoid recalibration
"""

import math
import unittest

from pm_core import PipelineConfig, StrategyScorer
from pm_pipeline import RegimeOptimizer


class TestScoringFeatureFlags(unittest.TestCase):
    """Verify feature flags control new scoring terms."""

    def _make_scorer(self, **overrides):
        cfg = PipelineConfig(scoring_mode="fx_backtester", **overrides)
        return StrategyScorer(cfg)

    def _base_metrics(self, **overrides):
        m = {
            'sharpe_ratio': 1.5,
            'profit_factor': 2.0,
            'win_rate': 55.0,
            'total_return_pct': 30.0,
            'max_drawdown_pct': 12.0,
            'expectancy_pips': 5.0,
            'sortino_ratio': 2.0,
            'worst_5pct_r': -1.0,
            'max_consecutive_losses': 4,
            'total_trades': 80,
        }
        m.update(overrides)
        return m

    def test_continuous_dd_flag_on(self):
        scorer = self._make_scorer(scoring_use_continuous_dd=True)
        score = scorer.calculate_fx_selection_score(self._base_metrics())
        # With continuous DD at 12%, multiplier = exp(-0.03*12) ≈ 0.70
        self.assertGreater(score, 0)

    def test_continuous_dd_flag_off_uses_buckets(self):
        scorer = self._make_scorer(scoring_use_continuous_dd=False)
        # DD=12% is below 20% bucket → no penalty in legacy mode
        score_low = scorer.calculate_fx_selection_score(self._base_metrics(max_drawdown_pct=12.0))
        # DD=22% → 0.9 multiplier in legacy mode
        score_high = scorer.calculate_fx_selection_score(self._base_metrics(max_drawdown_pct=22.0))
        self.assertGreater(score_low, score_high)

    def test_continuous_dd_monotonic(self):
        """Higher DD should always produce lower score."""
        scorer = self._make_scorer(scoring_use_continuous_dd=True)
        scores = []
        for dd in [5, 10, 15, 20, 25, 30]:
            s = scorer.calculate_fx_selection_score(self._base_metrics(max_drawdown_pct=dd))
            scores.append(s)
        for i in range(len(scores) - 1):
            self.assertGreater(scores[i], scores[i + 1],
                               f"Score at DD={5 + i*5}% should exceed score at DD={10 + i*5}%")

    def test_sortino_blend_changes_score(self):
        metrics = self._base_metrics(sortino_ratio=3.0, sharpe_ratio=1.0)
        score_on = self._make_scorer(scoring_use_sortino_blend=True).calculate_fx_selection_score(metrics)
        score_off = self._make_scorer(scoring_use_sortino_blend=False).calculate_fx_selection_score(metrics)
        # Sortino > Sharpe → blend-on should boost score
        self.assertNotAlmostEqual(score_on, score_off, places=2)

    def test_tail_risk_penalty_triggers(self):
        """Severe tail risk (worst_5pct_r < -3) should penalize."""
        scorer = self._make_scorer(scoring_use_tail_risk=True)
        score_ok = scorer.calculate_fx_selection_score(self._base_metrics(worst_5pct_r=-1.0))
        score_bad = scorer.calculate_fx_selection_score(self._base_metrics(worst_5pct_r=-5.0))
        self.assertGreater(score_ok, score_bad)

    def test_tail_risk_no_penalty_above_threshold(self):
        """Moderate tail risk should not penalize."""
        scorer = self._make_scorer(scoring_use_tail_risk=True)
        score_a = scorer.calculate_fx_selection_score(self._base_metrics(worst_5pct_r=-1.0))
        score_b = scorer.calculate_fx_selection_score(self._base_metrics(worst_5pct_r=-2.5))
        self.assertAlmostEqual(score_a, score_b, places=5)

    def test_consistency_penalty_triggers(self):
        """Max consecutive losses > 8 should penalize."""
        scorer = self._make_scorer(scoring_use_consistency=True)
        score_ok = scorer.calculate_fx_selection_score(self._base_metrics(max_consecutive_losses=5))
        score_bad = scorer.calculate_fx_selection_score(self._base_metrics(max_consecutive_losses=14))
        self.assertGreater(score_ok, score_bad)

    def test_consistency_no_penalty_below_threshold(self):
        """Low consecutive losses should not penalize."""
        scorer = self._make_scorer(scoring_use_consistency=True)
        score_a = scorer.calculate_fx_selection_score(self._base_metrics(max_consecutive_losses=3))
        score_b = scorer.calculate_fx_selection_score(self._base_metrics(max_consecutive_losses=7))
        self.assertAlmostEqual(score_a, score_b, places=5)

    def test_trade_frequency_bonus_rewards_high_counts(self):
        """High trade count should produce a bonus."""
        scorer = self._make_scorer(scoring_use_trade_frequency_bonus=True)
        score_low = scorer.calculate_fx_selection_score(self._base_metrics(total_trades=25))
        score_high = scorer.calculate_fx_selection_score(self._base_metrics(total_trades=200))
        self.assertGreater(score_high, score_low)

    def test_trade_frequency_bonus_capped(self):
        """Bonus should not exceed 8%."""
        scorer = self._make_scorer(
            scoring_use_trade_frequency_bonus=True,
            scoring_use_continuous_dd=False,
            scoring_use_sortino_blend=False,
            scoring_use_tail_risk=False,
            scoring_use_consistency=False,
        )
        score_base = scorer.calculate_fx_selection_score(self._base_metrics(total_trades=25))
        score_huge = scorer.calculate_fx_selection_score(self._base_metrics(total_trades=10000))
        ratio = score_huge / score_base if score_base > 0 else 999
        self.assertLessEqual(ratio, 1.09)  # Max 8% bonus + small tolerance

    def test_all_flags_off_matches_legacy(self):
        """With all flags off, score should match the legacy bucket behavior."""
        scorer = self._make_scorer(
            scoring_use_continuous_dd=False,
            scoring_use_sortino_blend=False,
            scoring_use_tail_risk=False,
            scoring_use_consistency=False,
            scoring_use_trade_frequency_bonus=False,
        )
        metrics = self._base_metrics(max_drawdown_pct=22.0, total_trades=25)
        score = scorer.calculate_fx_selection_score(metrics)
        # Manually compute legacy: sharpe=1.5, pf=2.0, wr=0.55, return_dd=30/22≈1.36, exp=5
        sharpe = 1.5
        pf = 2.0
        wr = 0.55
        ret_dd = 30.0 / 22.0
        exp = 5.0
        expected = (
            ((sharpe + 2.0) / 7.0) * 25.0 +
            (pf / 10.0) * 20.0 +
            (wr * 15.0) +
            ((ret_dd + 5.0) / 25.0) * 25.0 +
            min(exp / 10.0, 1.0) * 15.0
        ) * 0.9  # DD=22% → bucket 0.9
        self.assertAlmostEqual(score, expected, places=4)


class TestOptScoreCalibration(unittest.TestCase):
    """Test calculate_fx_opt_score calibration extensions."""

    def _make_scorer(self, **overrides):
        cfg = PipelineConfig(scoring_mode="fx_backtester", **overrides)
        return StrategyScorer(cfg)

    def _base_metrics(self, **overrides):
        m = {
            'sharpe_ratio': 1.5,
            'total_return_pct': 30.0,
            'max_drawdown_pct': 12.0,
            'profit_factor': 2.0,
            'win_rate': 55.0,
            'sortino_ratio': 2.0,
        }
        m.update(overrides)
        return m

    def test_opt_score_continuous_dd(self):
        scorer = self._make_scorer(scoring_use_continuous_dd=True)
        score_low_dd = scorer.calculate_fx_opt_score(self._base_metrics(max_drawdown_pct=5.0))
        score_high_dd = scorer.calculate_fx_opt_score(self._base_metrics(max_drawdown_pct=25.0))
        self.assertGreater(score_low_dd, score_high_dd)

    def test_opt_score_includes_pf_and_wr(self):
        """Opt score now includes PF and WR terms."""
        scorer = self._make_scorer(scoring_use_continuous_dd=False, scoring_use_sortino_blend=False)
        score_high_pf = scorer.calculate_fx_opt_score(self._base_metrics(profit_factor=4.0, win_rate=65.0))
        score_low_pf = scorer.calculate_fx_opt_score(self._base_metrics(profit_factor=1.1, win_rate=42.0))
        self.assertGreater(score_high_pf, score_low_pf)

    def test_opt_score_legacy_dd_fallback(self):
        scorer = self._make_scorer(scoring_use_continuous_dd=False, scoring_use_sortino_blend=False)
        # DD=10% → legacy penalty 1.0
        score_a = scorer.calculate_fx_opt_score(self._base_metrics(max_drawdown_pct=10.0))
        # DD=20% → legacy penalty 0.8
        score_b = scorer.calculate_fx_opt_score(self._base_metrics(max_drawdown_pct=20.0))
        self.assertGreater(score_a, score_b)


class TestDDHardGateInvariants(unittest.TestCase):
    """DD safety gates must never be relaxed by scoring changes."""

    def test_exceptional_val_pf_tightened(self):
        cfg = PipelineConfig()
        self.assertGreaterEqual(cfg.exceptional_val_profit_factor, 1.50)

    def test_exceptional_val_return_tightened(self):
        cfg = PipelineConfig()
        self.assertGreaterEqual(cfg.exceptional_val_return_pct, 10.0)

    def test_regime_validation_top_k_default(self):
        cfg = PipelineConfig()
        self.assertEqual(cfg.regime_validation_top_k, 5)

    def test_scoring_flags_default_enabled(self):
        cfg = PipelineConfig()
        self.assertTrue(cfg.scoring_use_continuous_dd)
        self.assertTrue(cfg.scoring_use_sortino_blend)
        self.assertTrue(cfg.scoring_use_tail_risk)
        self.assertTrue(cfg.scoring_use_consistency)
        self.assertTrue(cfg.scoring_use_trade_frequency_bonus)


class TestOptunaBlendConfig(unittest.TestCase):
    """Verify Optuna objective blend config propagates correctly."""

    def test_blend_defaults(self):
        cfg = PipelineConfig()
        self.assertTrue(getattr(cfg, 'optuna_objective_blend_enabled', False))
        self.assertAlmostEqual(getattr(cfg, 'optuna_objective_train_weight', 0), 0.80)
        self.assertAlmostEqual(getattr(cfg, 'optuna_objective_val_weight', 0), 0.20)

    def test_weights_sum_to_one(self):
        cfg = PipelineConfig()
        total = cfg.optuna_objective_train_weight + cfg.optuna_objective_val_weight
        self.assertAlmostEqual(total, 1.0, places=5)

    def test_weights_normalize_when_misconfigured(self):
        cfg = PipelineConfig(
            optuna_objective_train_weight=2.0,
            optuna_objective_val_weight=1.0,
        )
        self.assertAlmostEqual(cfg.optuna_objective_train_weight, 2.0 / 3.0, places=6)
        self.assertAlmostEqual(cfg.optuna_objective_val_weight, 1.0 / 3.0, places=6)

    def test_val_in_objective_default_false(self):
        cfg = PipelineConfig()
        self.assertFalse(cfg.optuna_use_val_in_objective)


class TestSigmoidRecalibration(unittest.TestCase):
    """Verify sigmoid normalization produces good spread for typical scores."""

    def _normalize(self, score):
        try:
            return 1.0 / (1.0 + math.exp(-(score - 45) / 30))
        except OverflowError:
            return 0.0 if score < 0 else 1.0

    def test_center_at_45(self):
        self.assertAlmostEqual(self._normalize(45), 0.5, places=3)

    def test_spread_in_practical_range(self):
        """Scores 20-70 should span at least 0.25-0.70."""
        low = self._normalize(20)
        high = self._normalize(70)
        self.assertGreater(low, 0.25)
        self.assertLess(low, 0.45)
        self.assertGreater(high, 0.65)
        self.assertLess(high, 0.80)

    def test_monotonic(self):
        scores = [0, 15, 30, 45, 60, 80, 100]
        normed = [self._normalize(s) for s in scores]
        for i in range(len(normed) - 1):
            self.assertLess(normed[i], normed[i + 1])

    def test_bounds(self):
        self.assertGreater(self._normalize(-100), 0.0)
        self.assertLess(self._normalize(500), 1.0)


class TestConfigHardening(unittest.TestCase):
    def test_regime_validation_top_k_clamped(self):
        cfg = PipelineConfig(regime_validation_top_k=0)
        self.assertEqual(cfg.regime_validation_top_k, 1)


class TestCandidateDescentSelection(unittest.TestCase):
    """Verify descent picks the best validated candidate by rank order."""

    @staticmethod
    def _candidate(name, score, regime="TREND"):
        train = {
            'total_trades': 40,
            'max_drawdown_pct': 10.0,
            'profit_factor': 1.5,
            'total_return_pct': 12.0,
            'win_rate': 55.0,
        }
        val = {
            'total_trades': 30,
            'max_drawdown_pct': 8.0,
            'profit_factor': 1.4,
            'total_return_pct': 10.0,
            'win_rate': 57.0,
            '_score': score,
        }
        return {
            'strategy_name': name,
            'params': {'p': 1},
            'train_regime_metrics': {regime: train},
            'val_regime_metrics': {regime: val},
        }

    def test_rank1_fail_rank2_pass_selects_rank2(self):
        cfg = PipelineConfig(
            scoring_mode="fx_backtester",
            regime_enable_hyperparam_tuning=False,
            regime_validation_top_k=5,
        )
        opt = RegimeOptimizer(cfg)

        # Deterministic scoring by injected marker.
        opt._compute_regime_score = lambda train_m, val_m: float(val_m.get('_score', 0.0))

        # Force rank #1 to fail validation, rank #2 to pass.
        def fake_validate(train_m, val_m, regime, candidate_name="Unknown"):
            if candidate_name == "S1":
                return False, "forced fail"
            return True, "forced pass"

        opt._validate_regime_winner = fake_validate

        candidates = [
            self._candidate("S1", 100.0),
            self._candidate("S2", 90.0),
            self._candidate("S3", 80.0),
        ]

        cfg_out, is_valid, reason = opt._select_best_for_regime("SYM", "H1", "TREND", candidates)
        self.assertTrue(is_valid)
        self.assertIsNotNone(cfg_out)
        self.assertEqual(cfg_out.strategy_name, "S2")
        self.assertIn("pass", reason)

    def test_top_k_cap_limits_validation_attempts(self):
        cfg = PipelineConfig(
            scoring_mode="fx_backtester",
            regime_enable_hyperparam_tuning=False,
            regime_validation_top_k=1,
        )
        opt = RegimeOptimizer(cfg)
        opt._compute_regime_score = lambda train_m, val_m: float(val_m.get('_score', 0.0))

        # Only S2 would pass, but top_k=1 means only S1 is attempted.
        def fake_validate(train_m, val_m, regime, candidate_name="Unknown"):
            return (candidate_name == "S2"), ("pass" if candidate_name == "S2" else "fail")

        opt._validate_regime_winner = fake_validate

        candidates = [
            self._candidate("S1", 100.0),
            self._candidate("S2", 90.0),
        ]

        cfg_out, is_valid, reason = opt._select_best_for_regime("SYM", "H1", "TREND", candidates)
        self.assertFalse(is_valid)
        self.assertIsNotNone(cfg_out)
        self.assertEqual(cfg_out.strategy_name, "S1")
        self.assertIn("failed validation", reason)


if __name__ == "__main__":
    unittest.main()
