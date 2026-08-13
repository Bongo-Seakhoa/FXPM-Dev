"""Pin the exact outputs of FXPM's one canonical production scorer."""

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

GOLDEN_HEX = {
    "healthy": "0x1.c2ffbda9a7721p+3",
    "thin_val": "0x1.b26df8603c275p+3",
    "thin_train": "0x1.b9d6c75a0b6d3p+3",
    "losing_val": "0x1.46ad7255d4cfdp+3",
    "zero_trades_val": "0x1.f2ce0ca59f151p+2",
    "deep_dd": "-0x1.a3f8f73eecddfp+1",
    "high_sharpe": "0x1.72a9a761c8385p+4",
    "negative_sharpe": "0x1.2dce7f926a159p+3",
    "flat": "0x1.0fd51103f74d1p+3",
}


class LegacyScoringCharacterisation(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.config = pm_core.PipelineConfig()
        cls.optimizer = pm_pipeline.RegimeOptimizer(cls.config)

    def test_all_nine_scores_match_exact_legacy_goldens(self):
        for name, train_over, val_over in CASES:
            train, val = _metrics(**train_over), _metrics(**val_over)
            candidate = {"strategy_name": "EMACrossoverStrategy", "params": {}}
            score = self.optimizer._compute_regime_score(train, val, candidate)
            with self.subTest(case=name):
                self.assertEqual(float(score).hex(), GOLDEN_HEX[name])

    def test_scores_are_deterministic(self):
        train, val = _metrics(), _metrics()
        candidate = {"strategy_name": "EMACrossoverStrategy", "params": {}}
        first = self.optimizer._compute_regime_score(train, val, candidate)
        second = self.optimizer._compute_regime_score(train, val, candidate)
        self.assertEqual(first, second)

    def test_governed_validation_evidence_uses_effective_counts_and_fixed_policy(self):
        captured = {}
        optimizer = pm_pipeline.RegimeOptimizer(pm_core.PipelineConfig())

        def robustness(train, val, *, purpose):
            captured["counts"] = (train["total_trades"], val["total_trades"])
            captured["purpose"] = purpose
            return 1.0

        optimizer.scorer.calculate_fx_score_robustness_ratio = robustness
        train, val = _metrics(total_trades=10_000), _metrics(total_trades=8_000)
        candidate = {
            "candidate_id": "canonical-cell",
            "strategy_name": "MissingFixtureStrategy",
            "params": {},
            "trade_power_contract_enforced": True,
            "trade_power_train": {"observed_effective_trades": 17.0},
            "trade_power_validation": {"observed_effective_trades": 11.0},
        }

        evidence = optimizer._build_validation_evidence(
            train,
            val,
            candidate=candidate,
        )

        self.assertEqual(captured, {"counts": (17.0, 11.0), "purpose": "selection"})
        self.assertEqual(evidence["candidate_id"], "canonical-cell")
        self.assertEqual(
            evidence["fresh_edge_policy"],
            {
                "winner_source": "active_recent_stage3",
                "no_fallback_to_best_train": True,
                "holdout_does_not_displace_latest_surface": True,
            },
        )
        self.assertNotIn("legacy_confidence", evidence)

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
