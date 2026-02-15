"""Tests for DD-to-Return ratio gate implementation.

Covers Section 6 test plan from dd_to_return_audit.md:
1. Candidate with return < 5% fails (existing behavior preserved).
2. Candidate with return >= 5% but return/DD < 1.0 fails.
3. Candidate with return >= 5% and return/DD >= 1.0 can pass if other gates pass.
4. Ratio gate still applies when regime_allow_losing_winners=True (unconditionality).
5. Weak-train candidate that meets exceptional checks but fails ratio is rejected.
6. In descent, rank-1 fails ratio and rank-2 passes -> rank-2 selected.
7. DD near zero uses epsilon path and remains stable.
"""

import unittest

from pm_core import PipelineConfig
from pm_pipeline import RegimeOptimizer


def _make_optimizer(**overrides):
    cfg = PipelineConfig(
        scoring_mode="fx_backtester",
        regime_enable_hyperparam_tuning=False,
        **overrides,
    )
    return RegimeOptimizer(cfg)


def _make_val_metrics(**overrides):
    """Base passing validation metrics."""
    m = {
        'total_trades': 40,
        'max_drawdown_pct': 10.0,
        'profit_factor': 1.5,
        'total_return_pct': 12.0,
        'win_rate': 58.0,
        'sharpe_approx': 1.2,
    }
    m.update(overrides)
    return m


def _make_train_metrics(**overrides):
    """Base passing training metrics."""
    m = {
        'total_trades': 50,
        'max_drawdown_pct': 8.0,
        'profit_factor': 1.8,
        'total_return_pct': 15.0,
        'win_rate': 55.0,
        'sharpe_approx': 1.5,
    }
    m.update(overrides)
    return m


class TestReturnDDRatioGate(unittest.TestCase):
    """Tests for return/DD ratio hard gate in validation."""

    def test_return_below_5pct_rejected(self):
        """Test 1: return < 5% fails (existing behavior preserved)."""
        opt = _make_optimizer()
        train = _make_train_metrics()
        val = _make_val_metrics(total_return_pct=3.0, max_drawdown_pct=2.0)  # ratio=6.0 but return too low
        is_valid, reason = opt._validate_regime_winner(train, val, "TREND", "TestStrat")
        self.assertFalse(is_valid)
        self.assertIn("return", reason.lower())

    def test_return_above_5pct_but_ratio_below_1_rejected(self):
        """Test 2: return >= 5% but return/DD < 1.0 fails."""
        opt = _make_optimizer()
        train = _make_train_metrics()
        val = _make_val_metrics(total_return_pct=7.0, max_drawdown_pct=12.0)  # ratio=0.58
        is_valid, reason = opt._validate_regime_winner(train, val, "TREND", "TestStrat")
        self.assertFalse(is_valid)
        self.assertIn("return/DD", reason)

    def test_good_ratio_passes(self):
        """Test 3: return >= 5% and return/DD >= 1.0 passes."""
        opt = _make_optimizer()
        train = _make_train_metrics()
        val = _make_val_metrics(total_return_pct=12.0, max_drawdown_pct=10.0)  # ratio=1.2
        is_valid, reason = opt._validate_regime_winner(train, val, "TREND", "TestStrat")
        self.assertTrue(is_valid)
        self.assertIn("Validated", reason)

    def test_ratio_gate_unconditional_with_allow_losing(self):
        """Test 4: ratio gate applies even when allow_losing_winners=True."""
        opt = _make_optimizer(regime_allow_losing_winners=True)
        train = _make_train_metrics()
        val = _make_val_metrics(total_return_pct=6.0, max_drawdown_pct=15.0)  # ratio=0.4
        is_valid, reason = opt._validate_regime_winner(train, val, "TREND", "TestStrat")
        self.assertFalse(is_valid)
        self.assertIn("return/DD", reason)

    def test_weak_train_fails_if_ratio_bad(self):
        """Test 5: weak-train candidate with exceptional stats but bad ratio is rejected."""
        opt = _make_optimizer(
            exceptional_val_profit_factor=1.15,
            exceptional_val_return_pct=8.0,
        )
        # Weak train: PF < 1.0
        train = _make_train_metrics(profit_factor=0.8, total_return_pct=-5.0)
        # Exceptional val: high PF, high return, but DD > return
        val = _make_val_metrics(
            total_return_pct=9.0,
            max_drawdown_pct=12.0,  # ratio=0.75 - fails ratio gate
            profit_factor=1.5,
            total_trades=60,
            win_rate=55.0,
        )
        is_valid, reason = opt._validate_regime_winner(train, val, "TREND", "TestStrat")
        self.assertFalse(is_valid)
        self.assertIn("Weak train rejected", reason)
        self.assertIn("return/DD", reason)

    def test_weak_train_passes_if_ratio_good(self):
        """Weak-train with good ratio and exceptional metrics passes."""
        opt = _make_optimizer(
            exceptional_val_profit_factor=1.15,
            exceptional_val_return_pct=8.0,
        )
        train = _make_train_metrics(profit_factor=0.8, total_return_pct=-5.0)
        val = _make_val_metrics(
            total_return_pct=14.0,
            max_drawdown_pct=10.0,  # ratio=1.4 - passes
            profit_factor=1.5,
            total_trades=60,
            win_rate=55.0,
        )
        is_valid, reason = opt._validate_regime_winner(train, val, "TREND", "TestStrat")
        self.assertTrue(is_valid)

    def test_dd_near_zero_epsilon_stable(self):
        """Test 7: DD near zero uses epsilon and remains stable."""
        opt = _make_optimizer()
        train = _make_train_metrics()
        val = _make_val_metrics(total_return_pct=6.0, max_drawdown_pct=0.1)  # eps=0.5 -> ratio=12.0
        is_valid, reason = opt._validate_regime_winner(train, val, "TREND", "TestStrat")
        self.assertTrue(is_valid)

    def test_dd_zero_epsilon_stable(self):
        """DD exactly zero uses epsilon and does not crash."""
        opt = _make_optimizer()
        ratio = opt._compute_return_dd_ratio(5.0, 0.0)
        self.assertAlmostEqual(ratio, 10.0)  # 5.0 / 0.5

    def test_helper_method_correctness(self):
        """Helper returns correct ratio with and without epsilon."""
        self.assertAlmostEqual(
            RegimeOptimizer._compute_return_dd_ratio(12.0, 10.0), 1.2
        )
        self.assertAlmostEqual(
            RegimeOptimizer._compute_return_dd_ratio(5.0, 0.3), 10.0  # max(0.3, 0.5) = 0.5
        )
        self.assertAlmostEqual(
            RegimeOptimizer._compute_return_dd_ratio(10.0, 8.0), 1.25
        )

    def test_config_default(self):
        """Config default is 1.0."""
        cfg = PipelineConfig()
        self.assertAlmostEqual(cfg.regime_min_val_return_dd_ratio, 1.0)

    def test_config_hardening_rejects_zero(self):
        """Config value <= 0 gets clamped to 1.0."""
        opt = _make_optimizer(regime_min_val_return_dd_ratio=0.0)
        self.assertAlmostEqual(opt.min_val_return_dd_ratio, 1.0)

    def test_config_hardening_rejects_negative(self):
        """Config value < 0 gets clamped to 1.0."""
        opt = _make_optimizer(regime_min_val_return_dd_ratio=-0.5)
        self.assertAlmostEqual(opt.min_val_return_dd_ratio, 1.0)


class TestReturnDDRatioEarlyRejection(unittest.TestCase):
    """Test ratio gate in _select_best_for_regime early rejection loop."""

    @staticmethod
    def _candidate(name, val_return, val_dd, score=50.0, regime="TREND"):
        train = {
            'total_trades': 50,
            'max_drawdown_pct': 8.0,
            'profit_factor': 1.8,
            'total_return_pct': 15.0,
            'win_rate': 55.0,
        }
        val = {
            'total_trades': 35,
            'max_drawdown_pct': val_dd,
            'profit_factor': 1.4,
            'total_return_pct': val_return,
            'win_rate': 57.0,
            '_score': score,
        }
        return {
            'strategy_name': name,
            'params': {'p': 1},
            'train_regime_metrics': {regime: train},
            'val_regime_metrics': {regime: val},
        }

    def test_descent_rank1_fails_ratio_rank2_passes(self):
        """Test 6: rank-1 fails ratio, rank-2 passes -> rank-2 selected."""
        opt = _make_optimizer(regime_validation_top_k=5)

        # Inject deterministic scoring
        opt._compute_regime_score = lambda t, v: float(v.get('_score', 0.0))

        # Always pass validation (ratio gate is in early rejection, not validation here)
        opt._validate_regime_winner = lambda t, v, r, n="": (True, "forced pass")

        candidates = [
            # Rank 1 by score but bad ratio (ret=6, dd=12 -> 0.5)
            self._candidate("BadRatio", val_return=6.0, val_dd=12.0, score=100.0),
            # Rank 2 by score with good ratio (ret=11, dd=9 -> 1.22)
            self._candidate("GoodRatio", val_return=11.0, val_dd=9.0, score=80.0),
        ]

        cfg_out, is_valid, reason = opt._select_best_for_regime("SYM", "H1", "TREND", candidates)
        self.assertTrue(is_valid)
        self.assertIsNotNone(cfg_out)
        self.assertEqual(cfg_out.strategy_name, "GoodRatio")

    def test_all_candidates_fail_ratio_returns_no_winner(self):
        """All candidates fail ratio -> no winner (None returned)."""
        opt = _make_optimizer(regime_validation_top_k=5)

        candidates = [
            self._candidate("A", val_return=6.0, val_dd=12.0),  # ratio=0.5
            self._candidate("B", val_return=5.5, val_dd=11.0),  # ratio=0.5
        ]

        cfg_out, is_valid, reason = opt._select_best_for_regime("SYM", "H1", "TREND", candidates)
        self.assertIsNone(cfg_out)
        self.assertFalse(is_valid)


if __name__ == "__main__":
    unittest.main()
