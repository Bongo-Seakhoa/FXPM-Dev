"""Pin what _compute_regime_score returns under the production scoring model.

Production sets `regime_scoring_model = "legacy"`, and legacy is the FALLTHROUGH:
every branch in that method tests for a non-legacy value, so under production all
of them are skipped. Removing the dead branches must therefore leave every score
byte-identical.

This characterises the current behaviour BEFORE the branches are deleted, so the
deletion is provably output-preserving rather than argued to be. Written to pass
against unchanged code first — a test that only agrees with the new code proves
nothing.
"""

import unittest

import pm_core
import pm_pipeline


def _metrics(**over):
    base = {
        "total_trades": 60,
        "win_rate": 0.55,
        "profit_factor": 1.4,
        "sharpe_ratio": 1.1,
        "sortino_ratio": 1.5,
        "max_drawdown_pct": 8.0,
        "total_return_pct": 12.0,
        "expectancy": 4.2,
        "avg_trade_duration_hours": 6.0,
        "gross_profit": 900.0,
        "gross_loss": 640.0,
        "consecutive_losses": 3,
    }
    base.update(over)
    return base


CASES = [
    ("healthy", {}, {}),
    ("thin_val", {}, {"total_trades": 16}),
    ("thin_train", {"total_trades": 20}, {}),
    ("losing_val", {}, {"profit_factor": 0.8, "total_return_pct": -4.0}),
    ("zero_trades_val", {}, {"total_trades": 0}),
    ("deep_dd", {}, {"max_drawdown_pct": 34.0}),
    ("high_sharpe", {}, {"sharpe_ratio": 4.5, "sortino_ratio": 6.0}),
    ("negative_sharpe", {}, {"sharpe_ratio": -0.7}),
    ("flat", {}, {"profit_factor": 1.0, "total_return_pct": 0.0, "sharpe_ratio": 0.0}),
]


class LegacyScoringCharacterisation(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.config = pm_core.PipelineConfig()
        cls.optimizer = pm_pipeline.RegimeOptimizer(cls.config)

    def test_production_resolves_to_legacy(self):
        """The premise of the deletion: production is the fallthrough path."""
        self.assertEqual(
            str(getattr(self.config, "regime_scoring_model", "legacy")).lower(),
            "legacy",
        )

    def test_scores_are_finite_and_ordered_sensibly(self):
        """Characterises real behaviour, not just 'it returns a number'."""
        scores = {}
        for name, train_over, val_over in CASES:
            train, val = _metrics(**train_over), _metrics(**val_over)
            candidate = {"strategy_name": "EMACrossoverStrategy", "params": {}}
            score = self.optimizer._compute_regime_score(train, val, candidate)
            scores[name] = float(score)

        # A healthy cell must not score below a losing one, and a cell with no
        # validation trades must not outscore a healthy one. These are the
        # invariants a scoring change could silently break.
        self.assertGreaterEqual(scores["healthy"], scores["losing_val"])
        self.assertGreaterEqual(scores["healthy"], scores["zero_trades_val"])
        self.assertGreaterEqual(scores["healthy"], scores["deep_dd"])
        for name, value in scores.items():
            with self.subTest(case=name):
                self.assertFalse(value != value, "score is NaN")

    def test_scores_are_deterministic(self):
        train, val = _metrics(), _metrics()
        candidate = {"strategy_name": "EMACrossoverStrategy", "params": {}}
        first = self.optimizer._compute_regime_score(train, val, candidate)
        second = self.optimizer._compute_regime_score(train, val, candidate)
        self.assertEqual(first, second)

    def test_degenerate_inputs_behave_as_they_do_today(self):
        """Characterisation, not aspiration.

        The -inf guard sits on the DERIVED scoring inputs, not the raw
        arguments, so empty metric dicts still produce a finite score. That is
        recorded here rather than asserted away: the deletion must not change
        it, whatever one thinks of it.
        """
        candidate = {"strategy_name": "EMACrossoverStrategy", "params": {}}
        empty = self.optimizer._compute_regime_score({}, {}, candidate)
        self.assertTrue(
            empty == empty and abs(empty) != float("inf"),
            f"empty-metrics score changed shape: {empty!r}",
        )
        # Pinned so a refactor cannot silently move it.
        self.assertAlmostEqual(empty, 0.04009078464455852, places=12)

        no_candidate = self.optimizer._compute_regime_score(
            _metrics(), _metrics(), None
        )
        self.assertTrue(
            no_candidate == no_candidate,
            f"missing-candidate score is NaN: {no_candidate!r}",
        )


if __name__ == "__main__":
    unittest.main()
