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
from unittest.mock import patch

from pm_core import PipelineConfig, StrategyScorer
from pm_pipeline import RegimeOptimizer
from pm_search_outcomes import SearchInfrastructureError


class TestScoringFeatureFlags(unittest.TestCase):
    """Verify feature flags control new scoring terms."""

    def _make_scorer(self, **overrides):
        cfg = PipelineConfig(**overrides)
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
        # findings.html §4: continuous DD slope tightened to -0.05.
        # At 12% DD, multiplier = exp(-0.05*12) ≈ 0.55.
        self.assertGreater(score, 0)

    def test_continuous_dd_uses_005_slope(self):
        """findings.html §4: penalty slope tightened from -0.03 to -0.05.

        We can't isolate the dd_mult cleanly (the score also includes a
        return/dd term which moves with dd), so we sanity-check that the
        full-score ratio at DD=15 is well below what the old -0.03 slope
        could have produced even with the return/dd shift baked in.
        """
        scorer = self._make_scorer(
            scoring_use_continuous_dd=True,
            scoring_use_sortino_blend=False,
            scoring_use_tail_risk=False,
            scoring_use_consistency=False,
            scoring_use_trade_frequency_bonus=False,
        )
        # Hold all other inputs constant, vary only DD.
        score_dd0 = scorer.calculate_fx_selection_score(self._base_metrics(max_drawdown_pct=0.0001))
        score_dd15 = scorer.calculate_fx_selection_score(self._base_metrics(max_drawdown_pct=15.0))
        ratio = score_dd15 / score_dd0 if score_dd0 > 0 else 0
        # Old slope at DD=15: math.exp(-0.03*15) ≈ 0.64. With the return/dd
        # term shift the empirical ratio under the old slope was around 0.45.
        # New slope brings it well below 0.40.
        self.assertLess(ratio, 0.40)

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
        cfg = PipelineConfig(**overrides)
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
        self.assertFalse(cfg.scoring_use_trade_frequency_bonus)
        self.assertAlmostEqual(cfg.legacy_confidence_bonus_cap, 0.08)


class TestLegacyConfidenceSelector(unittest.TestCase):
    """Legacy-confidence is a post-search regime selector variant, not an Optuna objective."""

    def _optimizer(self, **overrides):
        cfg = PipelineConfig(
            regime_scoring_model="legacy_confidence",
            scoring_use_trade_frequency_bonus=False,
            false_discovery_control_enabled=False,
            regime_min_train_trades=5,
            regime_min_val_trades=5,
            fx_val_max_drawdown=20.0,
            regime_min_val_profit_factor=1.0,
            regime_min_val_return_pct=0.0,
            fx_min_robustness_ratio=0.0,
            regime_min_val_return_dd_ratio=0.1,
            **overrides,
        )
        return RegimeOptimizer(cfg)

    def _bucket(self, **overrides):
        metrics = {
            "total_trades": 50,
            "win_rate": 55.0,
            "profit_factor": 1.5,
            "total_pnl": 100.0,
            "gross_profit": 250.0,
            "gross_loss": 150.0,
            "avg_win": 10.0,
            "avg_loss": 8.0,
            "sharpe_approx": 1.0,
            "sortino_approx": 1.2,
            "calmar_ratio": 2.0,
            "std_weekly_return": 0.01,
            "r_count": 50,
            "mean_r": 0.10,
            "median_r": 0.08,
            "std_r": 0.50,
            "se_r": 0.070711,
            "edge_t_stat_r": 1.414214,
            "pct_positive_r": 55.0,
            "worst_5pct_r": -1.0,
            "r_downside_deviation": 0.2,
            "r_block_downside_deviation": 0.2,
            "r_block_cvar": 0.3,
            "max_consecutive_losses": 3,
            "max_drawdown_pct": 5.0,
            "total_return_pct": 20.0,
        }
        metrics.update(overrides)
        return metrics

    def test_legacy_confidence_evidence_rewards_positive_edge_only(self):
        opt = self._optimizer()
        positive = opt._build_legacy_confidence_evidence(
            self._bucket(r_count=100, mean_r=0.20, std_r=0.40, se_r=0.04)
        )
        negative = opt._build_legacy_confidence_evidence(
            self._bucket(r_count=100, mean_r=-0.20, std_r=0.20, se_r=0.02)
        )
        no_variance = opt._build_legacy_confidence_evidence(
            self._bucket(r_count=1, mean_r=1.0, std_r=0.0, se_r=0.0)
        )

        self.assertGreater(positive["multiplier"], 1.0)
        self.assertLessEqual(positive["multiplier"], 1.08)
        self.assertEqual(negative["multiplier"], 1.0)
        self.assertEqual(no_variance["multiplier"], 1.0)

    def test_legacy_confidence_does_not_change_base_regime_score(self):
        legacy = RegimeOptimizer(PipelineConfig(
            regime_scoring_model="legacy",
            scoring_use_trade_frequency_bonus=False,
            regime_min_train_trades=5,
            regime_min_val_trades=5,
        ))
        confidence = self._optimizer()
        train = self._bucket()
        val = self._bucket(r_count=100, mean_r=0.20, std_r=0.40, se_r=0.04)

        self.assertAlmostEqual(
            confidence._compute_regime_score(train, val),
            legacy._compute_regime_score(train, val),
            places=8,
        )

    def test_legacy_confidence_can_rerank_fixed_candidate_pool(self):
        opt = self._optimizer(regime_validation_top_k=2)
        base_train = self._bucket(total_trades=50)
        weak_conf_val = self._bucket(r_count=100, mean_r=0.01, std_r=1.0, se_r=0.1)
        strong_conf_val = self._bucket(r_count=100, mean_r=0.20, std_r=0.40, se_r=0.04)
        candidates = [
            {
                "strategy_name": "BaseHigher",
                "params": {},
                "search_trials": 10,
                "train_regime_metrics": {"TREND": base_train},
                "val_regime_metrics": {"TREND": weak_conf_val},
            },
            {
                "strategy_name": "ConfidenceHigher",
                "params": {},
                "search_trials": 10,
                "train_regime_metrics": {"TREND": base_train},
                "val_regime_metrics": {"TREND": strong_conf_val},
            },
        ]

        def fake_score(_train, _val, candidate=None):
            return 100.0 if candidate["strategy_name"] == "BaseHigher" else 95.0

        opt._call_regime_score = fake_score
        config, validated, _reason = opt._select_best_for_regime("EURUSD", "H1", "TREND", candidates)

        self.assertTrue(validated)
        self.assertEqual(config.strategy_name, "ConfidenceHigher")
        self.assertIn("legacy_confidence", config.validation_evidence)
        self.assertGreater(config.validation_evidence["legacy_confidence"]["multiplier"], 1.0)


class TestOptunaBlendKnobsRemoved(unittest.TestCase):
    """findings §C7 / §8.3: the blend knobs never propagated past OptunaConfig —
    the actual train/val gap lever is fx_gap_penalty_lambda on PipelineConfig.
    This test guards against silent reintroduction."""

    def test_blend_knobs_absent_from_pipeline_config(self):
        cfg = PipelineConfig()
        for dead_knob in (
            "optuna_objective_blend_enabled",
            "optuna_objective_train_weight",
            "optuna_objective_val_weight",
            "optuna_use_val_in_objective",
        ):
            self.assertFalse(
                hasattr(cfg, dead_knob),
                f"{dead_knob} should have been removed (findings §C7); "
                "fx_gap_penalty_lambda is the real train/val lever.",
            )

    def test_actual_train_val_lever_still_present(self):
        cfg = PipelineConfig()
        self.assertTrue(hasattr(cfg, "fx_gap_penalty_lambda"))
        self.assertGreater(float(cfg.fx_gap_penalty_lambda), 0.0)


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
            research_mode=True,
            false_discovery_control_enabled=False,
            regime_enable_hyperparam_tuning=False,
            regime_validation_top_k=5,
        )
        opt = RegimeOptimizer(cfg)

        # Deterministic scoring by injected marker.
        opt._compute_regime_score = lambda train_m, val_m: float(val_m.get('_score', 0.0))

        # Force rank #1 to fail validation, rank #2 to pass.
        def fake_validate(
            train_m, val_m, regime, candidate_name="Unknown", **_contract
        ):
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

    def test_already_scored_descent_does_not_stop_at_legacy_top_k_cap(self):
        cfg = PipelineConfig(
            research_mode=True,
            false_discovery_control_enabled=False,
            regime_enable_hyperparam_tuning=False,
            regime_validation_top_k=1,
        )
        opt = RegimeOptimizer(cfg)
        opt._compute_regime_score = lambda train_m, val_m: float(val_m.get('_score', 0.0))

        # Both candidates already have validation metrics and scores, so the
        # legacy top_k must not manufacture NO_TRADE after S1 fails.
        attempted = []

        def fake_validate(
            train_m, val_m, regime, candidate_name="Unknown", **_contract
        ):
            attempted.append(candidate_name)
            return (candidate_name == "S2"), ("pass" if candidate_name == "S2" else "fail")

        opt._validate_regime_winner = fake_validate

        candidates = [
            self._candidate("S1", 100.0),
            self._candidate("S2", 90.0),
        ]

        cfg_out, is_valid, reason = opt._select_best_for_regime("SYM", "H1", "TREND", candidates)
        self.assertTrue(is_valid)
        self.assertIsNotNone(cfg_out)
        self.assertEqual(cfg_out.strategy_name, "S2")
        self.assertEqual(attempted, ["S1", "S2"])
        self.assertIn("pass", reason)

    def test_non_finite_score_cannot_sort_ahead_of_a_finite_winner(self):
        cfg = PipelineConfig(
            research_mode=True,
            false_discovery_control_enabled=False,
            regime_enable_hyperparam_tuning=False,
        )
        opt = RegimeOptimizer(cfg)
        opt._compute_regime_score = lambda _train, val: float(val["_score"])
        validated = []

        def validate(
            _train, _val, _regime, candidate_name="Unknown", **_contract
        ):
            validated.append(candidate_name)
            return True, "pass"

        opt._validate_regime_winner = validate
        poisoned = self._candidate("PoisonedNaN", float("nan"))
        finite = self._candidate("Finite", 10.0)

        cfg_out, is_valid, _reason = opt._select_best_for_regime(
            "SYM", "H1", "TREND", [poisoned, finite]
        )

        self.assertTrue(is_valid)
        self.assertEqual(cfg_out.strategy_name, "Finite")
        self.assertEqual(validated, ["Finite"])
        self.assertEqual(opt._normalize_score(float("nan")), 0.0)

    def test_invalid_core_metric_surface_fails_closed_before_ranking(self):
        cfg = PipelineConfig(
            research_mode=True,
            false_discovery_control_enabled=False,
            regime_enable_hyperparam_tuning=False,
        )
        opt = RegimeOptimizer(cfg)
        opt._compute_regime_score = lambda _train, val: float(val["_score"])
        invalid = self._candidate("NegativeDrawdown", 1000.0)
        invalid["val_regime_metrics"]["TREND"]["max_drawdown_pct"] = -1.0
        finite = self._candidate("Finite", 10.0)
        opt._validate_regime_winner = lambda *_args, **_kwargs: (True, "pass")

        with self.assertRaises(SearchInfrastructureError) as raised:
            opt._select_best_for_regime(
                "SYM", "H1", "TREND", [invalid, finite]
            )

        self.assertEqual(
            raised.exception.outcome.reason_code,
            "VALIDATION_SELECTION_SURFACE_INVALID",
        )

    def test_false_discovery_rejection_happens_before_descent_validation(self):
        cfg = PipelineConfig(
            research_mode=True,
            regime_enable_hyperparam_tuning=False,
            regime_validation_top_k=5,
            regime_min_train_trades=1,
            regime_min_val_trades=1,
            regime_min_val_profit_factor=0.0,
            regime_min_val_return_pct=-100.0,
            regime_min_val_return_dd_ratio=0.1,
            fx_val_max_drawdown=50.0,
            fx_min_robustness_ratio=0.0,
            fx_val_sharpe_override=-99.0,
            false_discovery_control_enabled=True,
            false_discovery_symbol_count=40,
            false_discovery_scope_power=1.0,
            false_discovery_min_dsr=0.5,
        )
        opt = RegimeOptimizer(cfg)
        opt._compute_regime_score = lambda train_m, val_m: float(val_m.get('_score', 0.0))
        validated = []

        def fake_validate(
            train_m, val_m, regime, candidate_name="Unknown", **_contract
        ):
            validated.append(candidate_name)
            return True, "forced pass"

        opt._validate_regime_winner = fake_validate

        c1 = self._candidate("C3Failed", 100.0)
        c1["search_trials"] = 100
        c1["local_search_trials_total"] = 100
        c1["train_regime_metrics"]["TREND"]["sharpe_approx"] = 2.0
        c1["val_regime_metrics"]["TREND"]["sharpe_approx"] = 2.0
        c2 = self._candidate("C3Passed", 10.0)
        c2["search_trials"] = 100
        c2["local_search_trials_total"] = 100
        c2["train_regime_metrics"]["TREND"]["sharpe_approx"] = 6.0
        c2["val_regime_metrics"]["TREND"]["sharpe_approx"] = 6.0

        def false_discovery_evidence(_val, _candidates, candidate, **_kwargs):
            passed = candidate["strategy_name"] == "C3Passed"
            return {
                "enabled": True,
                "min_dsr": 0.5,
                "deflated_sharpe": 0.9 if passed else 0.1,
                "dsr_evidence": {"status": "PASS"},
                "pbo_status": "PASS",
                "bootstrap_evidence": {"status": "PASS"},
                "confidence_multiplier": 1.0,
                "effective_trials": 100,
            }

        with patch.object(
            opt,
            "_build_false_discovery_evidence",
            side_effect=false_discovery_evidence,
        ):
            cfg_out, is_valid, _ = opt._select_best_for_regime("SYM", "H1", "TREND", [c1, c2])

        self.assertTrue(is_valid)
        self.assertEqual(cfg_out.strategy_name, "C3Passed")
        self.assertEqual(validated, ["C3Passed"])


class TestStabilityPenalty(unittest.TestCase):
    """findings.html §C4: multiplicative exp(-k * std_weekly_return) on val side
    in fx_generalization_score. Default k=2.0; setting 0 disables the penalty."""

    def _make_scorer(self, **overrides):
        cfg = PipelineConfig(**overrides)
        return StrategyScorer(cfg)

    def _metrics(self, std_weekly_return=0.0, **overrides):
        m = {
            'sharpe_ratio': 1.5,
            'profit_factor': 2.0,
            'win_rate': 55.0,
            'total_return_pct': 30.0,
            'max_drawdown_pct': 8.0,
            'expectancy_pips': 5.0,
            'sortino_ratio': 2.0,
            'worst_5pct_r': -1.0,
            'max_consecutive_losses': 4,
            'total_trades': 80,
            'std_weekly_return': std_weekly_return,
        }
        m.update(overrides)
        return m

    def test_zero_std_means_no_penalty(self):
        """std_weekly_return=0 → multiplier exp(0)=1.0 → penalty inert."""
        scorer = self._make_scorer(fx_stability_penalty_k=2.0)
        train = self._metrics(std_weekly_return=0.0)
        val = self._metrics(std_weekly_return=0.0)
        score_no_var, _, _, _ = scorer.fx_generalization_score(train, val)
        # Reference: same path with k=0 disables the term entirely.
        scorer_off = self._make_scorer(fx_stability_penalty_k=0.0)
        score_off, _, _, _ = scorer_off.fx_generalization_score(train, val)
        self.assertAlmostEqual(score_no_var, score_off, places=6)

    def test_high_std_lowers_score(self):
        """A jagged val curve must score below a smooth one (all else equal)."""
        scorer = self._make_scorer(fx_stability_penalty_k=2.0)
        train = self._metrics()
        smooth_val = self._metrics(std_weekly_return=0.01)  # 1% weekly std
        jagged_val = self._metrics(std_weekly_return=0.20)  # 20% weekly std
        s_smooth, _, _, _ = scorer.fx_generalization_score(train, smooth_val)
        s_jagged, _, _, _ = scorer.fx_generalization_score(train, jagged_val)
        self.assertGreater(s_smooth, s_jagged)

    def test_disable_with_k_zero(self):
        """fx_stability_penalty_k=0 short-circuits: result identical regardless of std."""
        scorer = self._make_scorer(fx_stability_penalty_k=0.0)
        train = self._metrics()
        v1 = self._metrics(std_weekly_return=0.01)
        v2 = self._metrics(std_weekly_return=0.50)
        s1, _, _, _ = scorer.fx_generalization_score(train, v1)
        s2, _, _, _ = scorer.fx_generalization_score(train, v2)
        self.assertAlmostEqual(s1, s2, places=6)

    def test_safe_when_field_missing(self):
        """val_metrics without std_weekly_return must not crash."""
        scorer = self._make_scorer(fx_stability_penalty_k=2.0)
        train = self._metrics()
        val = self._metrics()
        val.pop('std_weekly_return', None)
        score, _, _, _ = scorer.fx_generalization_score(train, val)
        self.assertIsInstance(score, float)

    def test_penalty_magnitude_matches_formula(self):
        """At k=2.0 and std=0.05, multiplier should equal exp(-0.10) ≈ 0.9048."""
        scorer = self._make_scorer(fx_stability_penalty_k=2.0)
        train = self._metrics()
        val_zero = self._metrics(std_weekly_return=0.0)
        val_5pct = self._metrics(std_weekly_return=0.05)
        s_zero, _, _, _ = scorer.fx_generalization_score(train, val_zero)
        s_5pct, _, _, _ = scorer.fx_generalization_score(train, val_5pct)
        if abs(s_zero) > 1e-9:
            ratio = s_5pct / s_zero
            self.assertAlmostEqual(ratio, math.exp(-2.0 * 0.05), places=5)

    def test_config_default_is_2(self):
        cfg = PipelineConfig()
        self.assertAlmostEqual(cfg.fx_stability_penalty_k, 2.0)


if __name__ == "__main__":
    unittest.main()
