import unittest
from unittest.mock import patch

import pandas as pd

from pm_core import PipelineConfig
from pm_optuna import (
    OptunaConfig,
    OptunaTPEOptimizer,
    ParameterSpace,
    TrialState,
    _stable_seed as optuna_stable_seed,
)
from pm_pipeline import (
    OptimizationPipeline,
    RegimeConfig,
    RegimeOptimizer,
    SymbolConfig,
    _stable_seed as pipeline_stable_seed,
    classify_promotion_status,
)
from pm_strategies import StrategyRegistry


class OptimizerHardeningTests(unittest.TestCase):
    @staticmethod
    def _walk_forward_audit(
        returns,
        *,
        min_score=0.25,
        avg_profit_factor=1.25,
        reason="",
    ):
        windows = [
            {"total_trades": 3, "total_return_pct": value}
            for value in returns
        ]
        avg_return = sum(returns) / len(returns) if returns else 0.0
        return {
            "enabled": True,
            "window_count": max(1, len(windows)),
            "summary": {
                "reason": reason,
                "min_score": min_score,
                "avg_profit_factor": avg_profit_factor,
                "avg_total_return_pct": avg_return,
            },
            "windows": windows,
            "audited_at": "2026-06-09T00:00:00",
        }

    @unittest.skipIf(TrialState is None, "optuna not available")
    def test_parameter_stability_evidence_requires_local_neighbors(self):
        class FakeTrial:
            def __init__(self, state, params, scores):
                self.state = state
                self.params = params
                self.user_attrs = {
                    "clean_params": params,
                    "regime_scores": scores,
                }

        class FakeStudy:
            trials = [
                FakeTrial(TrialState.COMPLETE, {"a": 2, "b": 20}, {"RANGE": 10.0}),
                FakeTrial(TrialState.COMPLETE, {"a": 1, "b": 20}, {"RANGE": 8.0}),
                FakeTrial(TrialState.COMPLETE, {"a": 3, "b": 20}, {"RANGE": 7.0}),
                FakeTrial(TrialState.COMPLETE, {"a": 2, "b": 10}, {"RANGE": 6.0}),
                FakeTrial(TrialState.COMPLETE, {"a": 2, "b": 30}, {"TREND": 99.0}),
                FakeTrial(TrialState.FAIL, {"a": 1, "b": 10}, {"RANGE": 99.0}),
            ]

        optimizer = OptunaTPEOptimizer(OptunaConfig(), None, None, None)
        param_space = ParameterSpace({"a": [1, 2, 3], "b": [10, 20, 30]}, {})

        evidence = optimizer._build_parameter_stability_evidence(
            FakeStudy(),
            "RANGE",
            {"a": 2, "b": 20},
            10.0,
            param_space,
        )

        self.assertEqual(evidence["status"], "available")
        self.assertEqual(evidence["completed_regime_trials"], 4)
        self.assertEqual(evidence["local_neighbor_count"], 3)
        self.assertEqual(evidence["nearest_neighbor_count"], 3)
        self.assertAlmostEqual(evidence["nearest_neighbor_median_score_ratio"], 0.7)

    @unittest.skipIf(TrialState is None, "optuna not available")
    def test_parameter_stability_evidence_keeps_sparse_neighbors_missing(self):
        class FakeTrial:
            def __init__(self, params, score):
                self.state = TrialState.COMPLETE
                self.params = params
                self.user_attrs = {
                    "clean_params": params,
                    "regime_scores": {"RANGE": score},
                }

        class FakeStudy:
            trials = [
                FakeTrial({"a": 2, "b": 20}, 10.0),
                FakeTrial({"a": 1, "b": 10}, 8.0),
            ]

        optimizer = OptunaTPEOptimizer(OptunaConfig(), None, None, None)
        param_space = ParameterSpace({"a": [1, 2, 3], "b": [10, 20, 30]}, {})

        evidence = optimizer._build_parameter_stability_evidence(
            FakeStudy(),
            "RANGE",
            {"a": 2, "b": 20},
            10.0,
            param_space,
        )

        self.assertEqual(evidence["status"], "insufficient_neighbors")
        self.assertEqual(evidence["local_neighbor_count"], 0)

    def test_stable_seed_is_deterministic_and_context_sensitive(self):
        seed_a1 = pipeline_stable_seed("EURUSD", "H1", "TRIXSignalStrategy", "grid")
        seed_a2 = pipeline_stable_seed("EURUSD", "H1", "TRIXSignalStrategy", "grid")
        seed_b = pipeline_stable_seed("EURUSD", "H4", "TRIXSignalStrategy", "grid")

        self.assertEqual(seed_a1, seed_a2)
        self.assertNotEqual(seed_a1, seed_b)
        self.assertEqual(
            optuna_stable_seed("EURUSD", "H1", "TRIXSignalStrategy", "grid"),
            optuna_stable_seed("EURUSD", "H1", "TRIXSignalStrategy", "grid"),
        )

    def test_regime_selection_accounts_for_search_trial_breadth(self):
        cfg = PipelineConfig(
            research_mode=True,
            false_discovery_control_enabled=False,
            regime_min_train_trades=1,
            regime_min_val_trades=1,
            regime_min_val_profit_factor=0.0,
            regime_min_val_return_pct=-100.0,
            fx_val_max_drawdown=50.0,
            fx_min_robustness_ratio=0.0,
            fx_val_sharpe_override=-99.0,
        )
        optimizer = RegimeOptimizer(cfg)

        shared_train = {
            "total_trades": 40,
            "profit_factor": 1.8,
            "win_rate": 55.0,
            "sharpe_approx": 1.8,
            "total_return_pct": 12.0,
            "max_drawdown_pct": 8.0,
            "avg_win": 120.0,
            "avg_loss": -70.0,
        }
        shared_val = {
            "total_trades": 25,
            "profit_factor": 1.6,
            "win_rate": 54.0,
            "sharpe_approx": 2.1,
            "total_return_pct": 9.0,
            "max_drawdown_pct": 7.0,
            "avg_win": 110.0,
            "avg_loss": -65.0,
        }

        candidates = [
            {
                "strategy_name": "LowTrialStrategy",
                "params": {},
                "search_trials": 1,
                "train_regime_metrics": {"TREND": dict(shared_train)},
                "val_regime_metrics": {"TREND": dict(shared_val)},
            },
            {
                "strategy_name": "HighTrialStrategy",
                "params": {},
                "search_trials": 200,
                "train_regime_metrics": {"TREND": dict(shared_train)},
                "val_regime_metrics": {"TREND": dict(shared_val)},
            },
        ]

        best_config, is_valid, _ = optimizer._select_best_for_regime(
            "EURUSD",
            "H1",
            "TREND",
            candidates,
        )

        self.assertTrue(is_valid)
        self.assertIsNotNone(best_config)
        self.assertEqual(best_config.strategy_name, "LowTrialStrategy")

    def test_candidate_descent_reaches_rank_six_after_top_five_fail(self):
        cfg = PipelineConfig(
            research_mode=True,
            false_discovery_control_enabled=False,
            regime_validation_top_k=5,
            regime_min_train_trades=1,
            regime_min_val_trades=1,
            regime_min_val_profit_factor=0.0,
            regime_min_val_return_pct=-100.0,
            regime_min_val_return_dd_ratio=0.0,
            fx_val_max_drawdown=100.0,
        )
        optimizer = RegimeOptimizer(cfg)
        checked = []
        optimizer._compute_regime_score = (
            lambda train, val, candidate=None: float(candidate["rank_score"])
        )

        def validate(_train, _val, _regime, candidate_name, timeframe=None):
            checked.append(candidate_name)
            if candidate_name == "Rank6Passes":
                return True, "Validated rank six"
            return False, "deliberate final-gate failure"

        optimizer._validate_regime_winner = validate
        shared_metrics = {
            "total_trades": 30,
            "profit_factor": 1.5,
            "total_return_pct": 10.0,
            "max_drawdown_pct": 2.0,
            "sharpe_approx": 1.0,
            "avg_win": 2.0,
            "avg_loss": -1.0,
        }
        candidates = []
        for rank in range(1, 7):
            name = "Rank6Passes" if rank == 6 else f"Rank{rank}Fails"
            candidates.append(
                {
                    "strategy_name": name,
                    "params": {},
                    "rank_score": float(7 - rank),
                    "search_trials": 1,
                    "train_regime_metrics": {"TREND": dict(shared_metrics)},
                    "val_regime_metrics": {"TREND": dict(shared_metrics)},
                }
            )

        winner, validated, reason = optimizer._select_best_for_regime(
            "EURUSD",
            "H1",
            "TREND",
            candidates,
        )

        self.assertTrue(validated, reason)
        self.assertEqual(winner.strategy_name, "Rank6Passes")
        self.assertEqual(len(checked), 6)

    def test_false_discovery_separates_train_trials_from_frozen_validation_trials(self):
        cfg = PipelineConfig(
            regime_min_train_trades=1,
            regime_min_val_trades=1,
            regime_min_val_profit_factor=0.0,
            regime_min_val_return_pct=-100.0,
            regime_min_val_return_dd_ratio=0.1,
            fx_val_max_drawdown=50.0,
            fx_min_robustness_ratio=0.0,
            fx_val_sharpe_override=-99.0,
            false_discovery_control_enabled=True,
            false_discovery_symbol_count=50,
            false_discovery_scope_power=1.0,
            false_discovery_confidence_floor=0.25,
        )
        optimizer = RegimeOptimizer(cfg)
        optimizer._compute_regime_score = lambda train, val: 100.0

        train = {
            "total_trades": 100,
            "profit_factor": 1.8,
            "win_rate": 55.0,
            "sharpe_approx": 2.0,
            "total_return_pct": 20.0,
            "max_drawdown_pct": 5.0,
        }
        val = dict(train)
        val["total_return_pct"] = 12.0
        candidates = [{
            "strategy_name": "CandidateStrategy",
            "params": {},
            "search_trials": 100,
            "local_search_trials_total": 100,
            "train_regime_metrics": {"TREND": train},
            "val_regime_metrics": {"TREND": val},
        }]

        with patch(
            "pm_pipeline.deflated_sharpe_evidence",
            return_value={
                "contract_version": "test",
                "status": "PASS",
                "deflated_sharpe": 0.99,
                "sharpe_annualized": 2.0,
                "effective_independent_trials": 1.0,
            },
        ), patch(
            "pm_pipeline.block_bootstrap_evidence",
            return_value={"status": "PASS", "passed": True},
        ):
            evidence = optimizer._build_false_discovery_evidence(
                val,
                candidates,
                candidates[0],
                regime="TREND",
                pbo_evidence={"status": "PASS", "passed": True, "pbo": 0.1},
            )

        self.assertTrue(evidence["enabled"])
        self.assertEqual(evidence["local_trials"], 100)
        self.assertEqual(evidence["scope_multiplier"], 50 * 6 * 4)
        self.assertEqual(evidence["effective_trials"], 1)
        self.assertEqual(evidence["validation_frozen_candidates"], 1)
        self.assertEqual(
            evidence["multiplicity_scope"]["train_inner_attempted_trials_exact"],
            100,
        )
        self.assertIsNone(
            evidence["multiplicity_scope"]["global_roster_attempted_trials_exact"]
        )
        self.assertLess(evidence["confidence_multiplier"], 1.0)

    def test_false_discovery_confidence_does_not_mutate_val_metric_surface(self):
        cfg = PipelineConfig(
            regime_min_train_trades=1,
            regime_min_val_trades=1,
            regime_min_val_profit_factor=0.0,
            regime_min_val_return_pct=-100.0,
            regime_min_val_return_dd_ratio=0.1,
            fx_val_max_drawdown=50.0,
            fx_min_robustness_ratio=0.0,
            fx_val_sharpe_override=-99.0,
            false_discovery_control_enabled=True,
            false_discovery_symbol_count=50,
            false_discovery_scope_power=1.0,
            false_discovery_confidence_floor=0.25,
        )
        optimizer = RegimeOptimizer(cfg)
        seen = {}

        def capture_score(_train, val, candidate=None):
            seen["sharpe_approx"] = val.get("sharpe_approx")
            seen["sharpe_ratio"] = val.get("sharpe_ratio")
            return 100.0

        optimizer._compute_regime_score = capture_score
        train = {
            "total_trades": 100,
            "profit_factor": 1.8,
            "win_rate": 55.0,
            "sharpe_approx": 2.0,
            "sharpe_ratio": 2.0,
            "total_return_pct": 20.0,
            "max_drawdown_pct": 5.0,
        }
        val = dict(train)
        val["sharpe_approx"] = 0.2
        val["sharpe_ratio"] = 0.2
        candidates = [{
            "strategy_name": "CandidateStrategy",
            "params": {},
            "search_trials": 100,
            "local_search_trials_total": 100,
            "train_regime_metrics": {"TREND": train},
            "val_regime_metrics": {"TREND": val},
        }]

        with patch(
            "pm_pipeline.aligned_cell_pbo",
            return_value={"status": "PASS", "passed": True, "pbo": 0.1},
        ), patch(
            "pm_pipeline.deflated_sharpe_evidence",
            return_value={
                "contract_version": "test",
                "status": "PASS",
                "deflated_sharpe": 0.99,
                "sharpe_annualized": 0.2,
            },
        ), patch(
            "pm_pipeline.block_bootstrap_evidence",
            return_value={"status": "PASS", "passed": True},
        ):
            best_config, is_valid, _ = optimizer._select_best_for_regime(
                "EURUSD",
                "H1",
                "TREND",
                candidates,
            )

        self.assertTrue(is_valid)
        self.assertAlmostEqual(seen["sharpe_approx"], 0.2)
        self.assertAlmostEqual(seen["sharpe_ratio"], 0.2)
        self.assertLess(best_config.validation_evidence["selection_score_raw"], 100.0)

    def test_false_discovery_min_dsr_can_reject_noise_candidates(self):
        cfg = PipelineConfig(
            regime_min_train_trades=1,
            regime_min_val_trades=1,
            regime_min_val_profit_factor=0.0,
            regime_min_val_return_pct=-100.0,
            regime_min_val_return_dd_ratio=0.1,
            fx_val_max_drawdown=50.0,
            false_discovery_control_enabled=True,
            false_discovery_symbol_count=50,
            false_discovery_scope_power=1.0,
            false_discovery_min_dsr=0.5,
        )
        optimizer = RegimeOptimizer(cfg)

        train = {
            "total_trades": 100,
            "profit_factor": 1.8,
            "win_rate": 55.0,
            "sharpe_approx": 0.2,
            "total_return_pct": 20.0,
            "max_drawdown_pct": 5.0,
        }
        val = dict(train)
        val["total_return_pct"] = 12.0
        candidates = [{
            "strategy_name": "NoiseStrategy",
            "params": {},
            "search_trials": 100,
            "local_search_trials_total": 100,
            "train_regime_metrics": {"TREND": train},
            "val_regime_metrics": {"TREND": val},
        }]

        best_config, is_valid, reason = optimizer._select_best_for_regime(
            "EURUSD",
            "H1",
            "TREND",
            candidates,
        )

        self.assertFalse(is_valid)
        self.assertIsNone(best_config)
        self.assertIn("daily-net-MTM DSR UNAVAILABLE", reason)

    def test_false_discovery_rejects_candidates_without_daily_aligned_evidence(self):
        cfg = PipelineConfig(
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
        optimizer = RegimeOptimizer(cfg)
        optimizer._compute_regime_score = lambda train, val: float(val.get("_score", 0.0))

        def candidate(name, raw_score, sharpe):
            train = {
                "total_trades": 500,
                "profit_factor": 1.8,
                "win_rate": 55.0,
                "sharpe_approx": sharpe,
                "total_return_pct": 20.0,
                "max_drawdown_pct": 5.0,
            }
            val = dict(train)
            val.update({"total_return_pct": 12.0, "_score": raw_score})
            return {
                "strategy_name": name,
                "params": {},
                "search_trials": 100,
                "local_search_trials_total": 100,
                "train_regime_metrics": {"TREND": train},
                "val_regime_metrics": {"TREND": val},
            }

        best_config, is_valid, _ = optimizer._select_best_for_regime(
            "EURUSD",
            "H1",
            "TREND",
            [
                candidate("FlashyNoiseStrategy", 100.0, 2.0),
                candidate("DefensibleStrategy", 10.0, 6.0),
            ],
        )

        self.assertFalse(is_valid)
        self.assertIsNone(best_config)

    def test_false_discovery_promotion_blocks_low_dsr(self):
        cfg = PipelineConfig(
            research_mode=True,
            false_discovery_control_enabled=True,
            false_discovery_min_dsr=0.10,
        )
        winner = RegimeConfig(
            strategy_name="SomeStrategy",
            parameters={},
            quality_score=0.5,
            holdout_metrics={
                "total_trades": 5,
                "profit_factor": 1.5,
                "total_return_pct": 1.0,
                "max_drawdown_pct": 1.0,
            },
            validation_evidence={"false_discovery": {"deflated_sharpe": 0.01}},
        )

        status, reasons = classify_promotion_status(winner, cfg)

        self.assertEqual(status, "BLOCKED")
        self.assertTrue(
            any("false-discovery daily-net-MTM DSR" in reason for reason in reasons),
            reasons,
        )

    def test_false_discovery_holdout_policy_blocks_failed_holdout(self):
        cfg = PipelineConfig(
            research_mode=True,
            false_discovery_control_enabled=True,
            false_discovery_holdout_policy="block",
            false_discovery_holdout_min_trades=5,
            false_discovery_holdout_min_profit_factor=1.0,
            false_discovery_holdout_min_return_pct=0.0,
        )
        winner = RegimeConfig(
            strategy_name="SomeStrategy",
            parameters={},
            quality_score=0.5,
            holdout_metrics={
                "total_trades": 5,
                "profit_factor": 0.8,
                "total_return_pct": -1.0,
            },
        )

        status, reasons = classify_promotion_status(winner, cfg)

        self.assertEqual(status, "BLOCKED")
        self.assertTrue(any("holdout PF" in reason for reason in reasons))

    def test_false_discovery_holdout_block_policy_blocks_insufficient_holdout(self):
        cfg = PipelineConfig(
            research_mode=True,
            false_discovery_control_enabled=True,
            false_discovery_holdout_policy="block",
            production_split_mode="research_clean_holdout",
            false_discovery_holdout_min_trades=5,
        )
        winner = RegimeConfig(
            strategy_name="SomeStrategy",
            parameters={},
            quality_score=0.5,
            holdout_metrics={
                "total_trades": 2,
                "profit_factor": float("inf"),
                "total_return_pct": 2.0,
            },
        )

        status, reasons = classify_promotion_status(winner, cfg)

        self.assertEqual(status, "BLOCKED")
        self.assertEqual(reasons, ["holdout trades 2 < 5"])

    def test_bleeding_edge_split_does_not_require_clean_holdout_metrics(self):
        cfg = PipelineConfig(
            research_mode=True,
            false_discovery_control_enabled=True,
            false_discovery_holdout_policy="block",
            production_split_mode="bleeding_edge",
            false_discovery_holdout_min_trades=5,
        )
        winner = RegimeConfig(
            strategy_name="SomeStrategy",
            parameters={},
            quality_score=0.5,
            holdout_metrics={},
            walk_forward_audit=self._walk_forward_audit([0.4, 0.6, 0.5]),
        )

        status, reasons = classify_promotion_status(winner, cfg)

        self.assertEqual(status, "PROMOTED")
        self.assertEqual(reasons, [])

    def test_false_discovery_holdout_promote_small_policy_derates_insufficient_holdout(self):
        cfg = PipelineConfig(
            false_discovery_control_enabled=True,
            false_discovery_holdout_policy="promote_small",
            false_discovery_holdout_min_trades=5,
        )
        winner = RegimeConfig(
            strategy_name="SomeStrategy",
            parameters={},
            quality_score=0.5,
            holdout_metrics={
                "total_trades": 0,
                "profit_factor": 0.0,
                "total_return_pct": 0.0,
            },
        )

        status, reasons = classify_promotion_status(winner, cfg)

        self.assertEqual(status, "PROMOTE_SMALL")
        self.assertEqual(reasons, ["holdout trades 0 < 5"])

    def test_optimizer_train_controls_screening_even_if_broad_surface_looks_better(self):
        cfg = PipelineConfig(
            train_min_profit_factor=1.0,
            train_min_return_pct=0.0,
            train_max_drawdown=20.0,
            regime_min_train_trades=10,
        )
        optimizer = RegimeOptimizer(cfg)
        candidate = {
            "strategy_name": "BroadActiveWinner",
            "train_result": {
                "profit_factor": 0.2,
                "total_return_pct": -5.0,
                "max_drawdown_pct": 10.0,
            },
            "eligibility_result": {
                "profit_factor": 1.4,
                "total_return_pct": 4.0,
                "max_drawdown_pct": 6.0,
            },
            "train_regime_metrics": {
                "TREND": {"total_trades": 2, "profit_factor": 0.2, "total_return_pct": -5.0}
            },
            "eligibility_regime_metrics": {
                "TREND": {
                    "total_trades": 20,
                    "profit_factor": 1.4,
                    "total_return_pct": 4.0,
                    "max_drawdown_pct": 6.0,
                }
            },
        }

        eligible = optimizer._apply_training_eligibility_gates([candidate], "EURUSD", "M5")
        top = optimizer._identify_top_strategies_per_regime(eligible, top_k=1)

        self.assertEqual(eligible, [])
        self.assertEqual(top["TREND"], [])

    def test_walk_forward_observe_only_does_not_block_live_promotion(self):
        cfg = PipelineConfig(
            research_mode=True,
            production_split_mode="research_clean_holdout",
            false_discovery_control_enabled=False,
            false_discovery_holdout_policy="off",
            walk_forward_promotion_policy="observe_only",
        )
        winner = RegimeConfig(
            strategy_name="SomeStrategy",
            parameters={},
            quality_score=0.5,
            walk_forward_audit={
                "summary": {"min_score": -1.0},
                "windows": [
                    {"total_trades": 4, "total_return_pct": -0.5},
                    {"total_trades": 3, "total_return_pct": -0.2},
                ],
            },
        )

        status, reasons = classify_promotion_status(winner, cfg)

        self.assertEqual(status, "PROMOTED")
        self.assertEqual(reasons, [])

    def test_walk_forward_block_policy_remains_available_for_research(self):
        cfg = PipelineConfig(
            research_mode=True,
            production_split_mode="research_clean_holdout",
            false_discovery_control_enabled=False,
            false_discovery_holdout_policy="off",
            walk_forward_promotion_policy="block_severe_only",
            walk_forward_live_blocking_enabled=True,
        )
        winner = RegimeConfig(
            strategy_name="SomeStrategy",
            parameters={},
            quality_score=0.5,
            walk_forward_audit={
                "summary": {"min_score": -1.0},
                "windows": [
                    {"total_trades": 4, "total_return_pct": -0.5},
                    {"total_trades": 3, "total_return_pct": -0.2},
                ],
            },
        )

        status, reasons = classify_promotion_status(winner, cfg)

        self.assertEqual(status, "BLOCKED")
        self.assertTrue(any("walk_forward" in reason for reason in reasons))

    def test_walk_forward_block_policy_without_live_opt_in_is_research_only(self):
        cfg = PipelineConfig(
            research_mode=True,
            production_split_mode="research_clean_holdout",
            false_discovery_control_enabled=False,
            false_discovery_holdout_policy="off",
            walk_forward_promotion_policy="block_severe_only",
            walk_forward_live_blocking_enabled=False,
        )
        winner = RegimeConfig(
            strategy_name="SomeStrategy",
            parameters={},
            quality_score=0.5,
            walk_forward_audit={
                "summary": {"min_score": -1.0},
                "windows": [
                    {"total_trades": 4, "total_return_pct": -0.5},
                    {"total_trades": 3, "total_return_pct": -0.2},
                ],
            },
        )

        status, reasons = classify_promotion_status(winner, cfg)

        self.assertEqual(status, "PROMOTED")
        self.assertEqual(reasons, [])

    def test_walk_forward_promotion_gate_promotes_clean_evidence(self):
        cfg = PipelineConfig(
            research_mode=True,
            production_split_mode="research_clean_holdout",
            false_discovery_control_enabled=False,
            false_discovery_holdout_policy="off",
        )
        winner = RegimeConfig(
            strategy_name="SomeStrategy",
            parameters={},
            quality_score=0.5,
            walk_forward_audit=self._walk_forward_audit([0.4, 0.6, 0.5]),
        )

        status, reasons = classify_promotion_status(winner, cfg)

        self.assertEqual(status, "PROMOTED")
        self.assertEqual(reasons, [])

    def test_walk_forward_promotion_gate_derates_missing_evidence(self):
        cfg = PipelineConfig(
            research_mode=True,
            production_split_mode="research_clean_holdout",
            false_discovery_control_enabled=False,
            false_discovery_holdout_policy="off",
        )
        winner = RegimeConfig(
            strategy_name="SomeStrategy",
            parameters={},
            quality_score=0.5,
            walk_forward_audit={
                "enabled": True,
                "summary": {"reason": "no_regime_trades"},
                "windows": [],
            },
        )

        status, reasons = classify_promotion_status(winner, cfg)

        self.assertEqual(status, "PROMOTE_SMALL")
        self.assertTrue(any("insufficient promotion evidence" in reason for reason in reasons))

    def test_walk_forward_promotion_gate_derates_weak_evidence(self):
        cfg = PipelineConfig(
            research_mode=True,
            production_split_mode="research_clean_holdout",
            false_discovery_control_enabled=False,
            false_discovery_holdout_policy="off",
        )
        winner = RegimeConfig(
            strategy_name="SomeStrategy",
            parameters={},
            quality_score=0.5,
            walk_forward_audit=self._walk_forward_audit([0.3, 0.0, 0.2], avg_profit_factor=0.95),
        )

        status, reasons = classify_promotion_status(winner, cfg)

        self.assertEqual(status, "PROMOTE_SMALL")
        self.assertTrue(any("walk_forward" in reason for reason in reasons))

    def test_walk_forward_promotion_gate_blocks_severe_evidence(self):
        cfg = PipelineConfig(
            research_mode=True,
            production_split_mode="research_clean_holdout",
            false_discovery_control_enabled=False,
            false_discovery_holdout_policy="off",
        )
        winner = RegimeConfig(
            strategy_name="SomeStrategy",
            parameters={},
            quality_score=0.5,
            walk_forward_audit=self._walk_forward_audit(
                [-0.4, -0.1, -0.2],
                min_score=-1.0,
                avg_profit_factor=0.7,
            ),
        )

        status, reasons = classify_promotion_status(winner, cfg)

        self.assertEqual(status, "BLOCKED")
        self.assertTrue(any("walk_forward" in reason for reason in reasons))

    def test_promotion_blocks_nested_regime_profitability_severe_failure(self):
        cfg = PipelineConfig()
        winner = RegimeConfig(
            strategy_name="SomeStrategy",
            parameters={},
            quality_score=0.5,
            historical_audit_metrics={
                "enabled": True,
                "severe_failure": False,
                "failure_reasons": [],
                "regime_profitability_audit": {
                    "severe_failure": True,
                    "warnings": ["RANGE: profitability failed (PF=0.75, return=-1.17%)"],
                },
            },
        )

        status, reasons = classify_promotion_status(winner, cfg)

        self.assertEqual(status, "BLOCKED")
        self.assertTrue(any("RANGE: profitability failed" in reason for reason in reasons))

    def test_specialist_spokes_v2_default_fallback_prefers_promoted_candidate(self):
        cfg = PipelineConfig(regime_scoring_model="specialist_spokes_v2")
        pipeline = OptimizationPipeline(cfg)
        blocked = RegimeConfig(
            strategy_name="BlockedWinner",
            parameters={},
            quality_score=0.9,
            promotion_status="BLOCKED",
            promotion_reasons=["holdout PF 0.14 < 1.00"],
        )
        promoted = RegimeConfig(
            strategy_name="SaferWinner",
            parameters={},
            quality_score=0.6,
            promotion_status="PROMOTED",
        )

        selected, tf, regime, evidence = pipeline._select_v2_symbol_default(
            {"H1": {"RANGE": blocked}, "H4": {"RANGE": promoted}},
            blocked,
        )

        self.assertIs(selected, promoted)
        self.assertEqual((tf, regime), ("H4", "RANGE"))
        self.assertEqual(evidence["rejected_before_selection"][0]["strategy"], "BlockedWinner")
        self.assertEqual(evidence["selection_pass"], "promoted_only")

    def test_specialist_spokes_v2_default_origin_uses_object_identity_first(self):
        cfg = PipelineConfig(regime_scoring_model="specialist_spokes_v2")
        pipeline = OptimizationPipeline(cfg)
        first = RegimeConfig(
            strategy_name="SameStrategy",
            parameters={"period": 14},
            quality_score=0.7,
            holdout_metrics={"total_trades": 1},
        )
        selected = RegimeConfig(
            strategy_name="SameStrategy",
            parameters={"period": 14},
            quality_score=0.8,
            holdout_metrics={"total_trades": 9},
        )

        tf, regime = pipeline._locate_default_origin(
            selected,
            {"M15": {"RANGE": first}, "H1": {"TREND": selected}},
        )

        self.assertEqual((tf, regime), ("H1", "TREND"))

    def test_specialist_spokes_v2_derates_insufficient_holdout_with_research_holdout_policy(self):
        cfg = PipelineConfig(
            regime_scoring_model="specialist_spokes_v2",
            false_discovery_control_enabled=False,
            false_discovery_holdout_policy="promote_small",
            false_discovery_holdout_min_trades=5,
        )
        winner = RegimeConfig(
            strategy_name="ThinHoldout",
            parameters={},
            quality_score=0.5,
            holdout_metrics={"total_trades": 2, "profit_factor": 1.5, "total_return_pct": 1.0},
        )

        status, reasons = classify_promotion_status(winner, cfg)

        self.assertEqual(status, "PROMOTE_SMALL")
        self.assertIn("holdout trades 2 < 5", reasons)

    def test_funded_default_blocks_missing_production_holdout_even_when_false_discovery_disabled(self):
        cfg = PipelineConfig(
            regime_scoring_model="specialist_spokes_v2",
            false_discovery_control_enabled=False,
            false_discovery_holdout_min_trades=5,
        )
        winner = RegimeConfig(
            strategy_name="ProductionWinner",
            parameters={},
            quality_score=0.5,
            walk_forward_audit=self._walk_forward_audit([0.4, 0.6, 0.5]),
        )

        status, reasons = classify_promotion_status(winner, cfg)

        self.assertEqual(status, "BLOCKED")
        self.assertIn("holdout trades 0 < 5", reasons)

    def test_specialist_spokes_v2_uses_promote_small_only_after_clean_pool_exhausted(self):
        cfg = PipelineConfig(
            regime_scoring_model="specialist_spokes_v2",
            specialist_spokes_v2_allow_promote_small_default=True,
        )
        pipeline = OptimizationPipeline(cfg)
        thin_high_score = RegimeConfig(
            strategy_name="ThinHighScore",
            parameters={},
            quality_score=0.9,
            promotion_status="PROMOTE_SMALL",
            promotion_reasons=["holdout trades 2 < 5"],
        )
        clean_lower_score = RegimeConfig(
            strategy_name="CleanLowerScore",
            parameters={},
            quality_score=0.5,
            promotion_status="PROMOTED",
        )

        selected, tf, regime, evidence = pipeline._select_v2_symbol_default(
            {"H1": {"RANGE": thin_high_score}, "H4": {"TREND": clean_lower_score}},
            thin_high_score,
        )

        self.assertIs(selected, clean_lower_score)
        self.assertEqual((tf, regime), ("H4", "TREND"))
        self.assertEqual(evidence["selection_pass"], "promoted_only")

    def test_specialist_spokes_v2_can_fallback_to_promote_small_when_no_clean_default(self):
        cfg = PipelineConfig(
            regime_scoring_model="specialist_spokes_v2",
            specialist_spokes_v2_allow_promote_small_default=True,
        )
        pipeline = OptimizationPipeline(cfg)
        blocked = RegimeConfig(
            strategy_name="BlockedWinner",
            parameters={},
            quality_score=0.9,
            promotion_status="BLOCKED",
        )
        small = RegimeConfig(
            strategy_name="DeratedWinner",
            parameters={},
            quality_score=0.6,
            promotion_status="PROMOTE_SMALL",
            promotion_reasons=["holdout trades 3 < 5"],
        )

        selected, tf, regime, evidence = pipeline._select_v2_symbol_default(
            {"M30": {"CHOP": blocked}, "H4": {"RANGE": small}},
            blocked,
        )

        self.assertIs(selected, small)
        self.assertEqual((tf, regime), ("H4", "RANGE"))
        self.assertEqual(evidence["selection_pass"], "promote_small_allowed")

    def test_specialist_spokes_v2_rejects_regime_severe_only_when_holdout_confirms(self):
        cfg = PipelineConfig(
            regime_scoring_model="specialist_spokes_v2",
            false_discovery_holdout_policy="promote_small",
        )
        pipeline = OptimizationPipeline(cfg)
        severe_clean_holdout = RegimeConfig(
            strategy_name="SevereButCleanHoldout",
            parameters={},
            quality_score=0.8,
            promotion_status="PROMOTED",
            holdout_metrics={
                "total_trades": 8,
                "profit_factor": 1.4,
                "total_return_pct": 1.0,
                "max_drawdown_pct": 5.0,
            },
            historical_audit_metrics={
                "regime_profitability_audit": {
                    "severe_failure": True,
                    "warnings": ["RANGE: profitability failed"],
                },
            },
        )
        severe_bad_holdout = RegimeConfig(
            strategy_name="SevereAndBadHoldout",
            parameters={},
            quality_score=0.9,
            promotion_status="PROMOTE_SMALL",
            holdout_metrics={
                "total_trades": 8,
                "profit_factor": 0.7,
                "total_return_pct": -1.0,
                "max_drawdown_pct": 5.0,
            },
            historical_audit_metrics={
                "regime_profitability_audit": {
                    "severe_failure": True,
                    "warnings": ["RANGE: profitability failed"],
                },
            },
        )

        self.assertEqual(pipeline._v2_default_rejection_reasons(severe_clean_holdout), [])
        reasons = pipeline._v2_default_rejection_reasons(severe_bad_holdout, allow_promote_small=True)
        self.assertTrue(any("regime severe confirmed" in reason for reason in reasons))

    def test_specialist_spokes_v2_classification_confirms_historical_severe_with_holdout(self):
        cfg = PipelineConfig(
            research_mode=True,
            false_discovery_control_enabled=False,
            regime_scoring_model="specialist_spokes_v2",
            false_discovery_holdout_policy="promote_small",
            false_discovery_holdout_min_trades=5,
            false_discovery_holdout_min_profit_factor=1.0,
        )
        severe_clean_holdout = RegimeConfig(
            strategy_name="HistoricalSevereButCleanHoldout",
            parameters={},
            quality_score=0.8,
            holdout_metrics={
                "total_trades": 8,
                "profit_factor": 1.4,
                "total_return_pct": 1.0,
                "max_drawdown_pct": 5.0,
            },
            walk_forward_audit=self._walk_forward_audit([0.4, 0.6, 0.5]),
            historical_audit_metrics={
                "severe_failure": True,
                "failure_reasons": ["historical replay drawdown warning"],
            },
        )
        severe_bad_holdout = RegimeConfig(
            strategy_name="HistoricalSevereAndBadHoldout",
            parameters={},
            quality_score=0.9,
            holdout_metrics={
                "total_trades": 8,
                "profit_factor": 0.7,
                "total_return_pct": -1.0,
                "max_drawdown_pct": 5.0,
            },
            walk_forward_audit=self._walk_forward_audit([0.4, 0.6, 0.5]),
            historical_audit_metrics={
                "severe_failure": True,
                "failure_reasons": ["historical replay drawdown warning"],
            },
        )

        clean_status, clean_reasons = classify_promotion_status(severe_clean_holdout, cfg)
        bad_status, bad_reasons = classify_promotion_status(severe_bad_holdout, cfg)

        self.assertEqual(clean_status, "PROMOTED")
        self.assertEqual(clean_reasons, [])
        self.assertEqual(bad_status, "BLOCKED")
        self.assertTrue(any("historical severe confirmed" in reason for reason in bad_reasons))

    def test_specialist_spokes_v2_rejects_historical_severe_only_when_holdout_confirms(self):
        cfg = PipelineConfig(
            regime_scoring_model="specialist_spokes_v2",
            false_discovery_holdout_policy="promote_small",
        )
        pipeline = OptimizationPipeline(cfg)
        severe_clean_holdout = RegimeConfig(
            strategy_name="HistoricalSevereButCleanHoldout",
            parameters={},
            quality_score=0.8,
            promotion_status="PROMOTED",
            holdout_metrics={"total_trades": 8, "profit_factor": 1.4, "total_return_pct": 1.0},
            historical_audit_metrics={
                "severe_failure": True,
                "failure_reasons": ["historical replay drawdown warning"],
            },
        )
        severe_bad_holdout = RegimeConfig(
            strategy_name="HistoricalSevereAndBadHoldout",
            parameters={},
            quality_score=0.9,
            promotion_status="PROMOTE_SMALL",
            holdout_metrics={"total_trades": 8, "profit_factor": 0.7, "total_return_pct": -1.0},
            historical_audit_metrics={
                "severe_failure": True,
                "failure_reasons": ["historical replay drawdown warning"],
            },
        )

        self.assertEqual(pipeline._v2_default_rejection_reasons(severe_clean_holdout), [])
        reasons = pipeline._v2_default_rejection_reasons(severe_bad_holdout, allow_promote_small=True)
        self.assertTrue(any("historical severe confirmed" in reason for reason in reasons))

    def test_specialist_spokes_v2_optional_sharpe_cap_uses_val_metrics_without_false_discovery(self):
        cfg = PipelineConfig(
            regime_scoring_model="specialist_spokes_v2",
            specialist_spokes_v2_max_validation_sharpe=100.0,
        )
        pipeline = OptimizationPipeline(cfg)
        winner = RegimeConfig(
            strategy_name="ExtremeSharpe",
            parameters={},
            quality_score=0.9,
            promotion_status="PROMOTED",
            val_metrics={"sharpe_ratio": 999.0},
        )

        reasons = pipeline._v2_default_rejection_reasons(winner)

        self.assertTrue(any("validation Sharpe 999.00" in reason for reason in reasons))

    def test_oos_evidence_record_is_compact_and_stable(self):
        cfg = PipelineConfig(
            oos_evidence_export_enabled=True,
            oos_evidence_periods=4,
            oos_evidence_min_periods=2,
        )
        optimizer = RegimeOptimizer(cfg)
        features = pd.DataFrame(index=pd.date_range("2026-01-01", periods=5, freq="h"))
        candidate = {
            "strategy_name": "DemoStrategy",
            "params": {"window": 10},
            "is_tuned": True,
            "tuned_for_regime": "TREND",
            "search_trials": 25,
            "local_search_trials_total": 100,
            "strategy_roster_count": 100,
            "parameter_stability_evidence": {
                "status": "available",
                "nearest_neighbor_median_score_ratio": 0.91,
            },
            "train_result": {
                "equity_curve": [100.0, 102.0],
                "trades": [{"pnl_dollars": 2.0}],
                "total_trades": 10,
                "profit_factor": 1.5,
                "total_return_pct": 4.0,
                "max_drawdown_pct": 0.5,
            },
            "train_regime_metrics": {
                "TREND": {"total_trades": 5, "profit_factor": 1.4, "total_return_pct": 2.0}
            },
            "val_regime_metrics": {
                "TREND": {"total_trades": 3, "profit_factor": 1.2, "total_return_pct": 2.0}
            },
        }
        val_result = {
            "equity_curve": [100.0, 101.0, 100.0, 103.0, 102.0],
            "total_trades": 3,
            "profit_factor": 1.2,
            "total_return_pct": 2.0,
            "max_drawdown_pct": 1.0,
            "win_rate": 66.7,
            "trades": [
                {"pnl_dollars": 10.0, "risk_amount": 10.0},
                {"pnl_dollars": -5.0, "risk_amount": 10.0},
                {"r_multiple": 0.4},
            ],
        }
        stress_result = {
            "total_trades": 3,
            "profit_factor": 1.05,
            "total_return_pct": 1.0,
            "max_drawdown_pct": 1.5,
        }

        first = optimizer._build_oos_evidence_record(
            "EURUSD",
            "H1",
            candidate,
            val_result,
            features,
            val_warmup_bars=1,
            stress_result=stress_result,
            stress_mode="stress_p95",
        )
        second = optimizer._build_oos_evidence_record(
            "EURUSD",
            "H1",
            {**candidate, "params": {"window": 10}},
            val_result,
            features,
            val_warmup_bars=1,
        )
        other_regime = optimizer._build_oos_evidence_record(
            "EURUSD",
            "H1",
            {**candidate, "tuned_for_regime": "RANGE"},
            val_result,
            features,
            val_warmup_bars=1,
        )

        self.assertEqual(first["format_version"], "oos_candidate_returns_v3")
        self.assertEqual(first["candidate_id"], second["candidate_id"])
        self.assertNotEqual(first["candidate_id"], other_regime["candidate_id"])
        self.assertEqual(first["regime"], "TREND")
        self.assertEqual(first["return_index"], "relative_equal_blocks")
        self.assertEqual(first["period_count"], 4)
        self.assertEqual(len(first["period_returns"]), 4)
        self.assertNotIn("equity_curve", first)
        self.assertEqual(first["metrics"]["total_trades"], 3)
        self.assertEqual(first["train_metrics"]["total_trades"], 10)
        self.assertNotIn("equity_curve", first["train_metrics"])
        self.assertNotIn("trades", first["train_metrics"])
        self.assertAlmostEqual(first["generalization_metrics"]["return_retention"], 0.5)
        self.assertIn("TREND", first["train_regime_metrics"])
        self.assertEqual(first["trade_path_evidence"]["status"], "available")
        self.assertEqual(first["trade_path_evidence"]["trade_count"], 3)
        self.assertEqual(first["equity_path_evidence"]["status"], "available")
        self.assertIn("capital_preservation_blocks", first["equity_path_evidence"])
        self.assertEqual(first["execution_cost_survival"]["status"], "available")
        self.assertEqual(first["execution_cost_survival"]["stress_mode"], "stress_p95")
        self.assertAlmostEqual(first["execution_cost_survival"]["return_retention"], 0.5)
        self.assertEqual(first["parameter_stability_evidence"]["status"], "available")
        self.assertAlmostEqual(
            first["parameter_stability_evidence"]["nearest_neighbor_median_score_ratio"],
            0.91,
        )
        self.assertEqual(second["execution_cost_survival"]["status"], "not_requested")
        self.assertEqual(second["parameter_stability_evidence"]["status"], "available")

    def test_oos_evidence_trims_warmup_equity_prefix(self):
        cfg = PipelineConfig(
            oos_evidence_export_enabled=True,
            oos_evidence_periods=4,
            oos_evidence_min_periods=2,
        )
        optimizer = RegimeOptimizer(cfg)
        candidate = {
            "strategy_name": "DemoStrategy",
            "params": {"window": 10},
            "train_result": {"total_return_pct": 3.0, "profit_factor": 1.2},
        }
        trades = [{"pnl_dollars": 3.0, "risk_amount": 10.0, "r_multiple": 0.3}]
        compact_result = {
            "equity_curve": [100.0, 101.0, 99.0, 102.0, 101.0, 103.0],
            "trades": trades,
            "total_trades": 1,
            "profit_factor": 1.2,
            "total_return_pct": 3.0,
            "max_drawdown_pct": 2.0,
            "win_rate": 100.0,
        }
        warmed_result = {
            **compact_result,
            "equity_curve": [100.0] * 50 + [101.0, 99.0, 102.0, 101.0, 103.0],
        }
        compact_features = pd.DataFrame(index=pd.date_range("2026-01-01", periods=6, freq="h"))
        warmed_features = pd.DataFrame(index=pd.date_range("2026-01-01", periods=55, freq="h"))

        compact = optimizer._build_oos_evidence_record(
            "EURUSD",
            "H1",
            candidate,
            compact_result,
            compact_features,
            val_warmup_bars=0,
        )
        warmed = optimizer._build_oos_evidence_record(
            "EURUSD",
            "H1",
            candidate,
            warmed_result,
            warmed_features,
            val_warmup_bars=50,
        )

        self.assertEqual(warmed["period_returns"], compact["period_returns"])
        self.assertEqual(
            warmed["equity_path_evidence"]["return_pct_blocks"],
            compact["equity_path_evidence"]["return_pct_blocks"],
        )

    def test_oos_evidence_record_keeps_no_trade_candidates_as_zero_returns(self):
        cfg = PipelineConfig(
            oos_evidence_export_enabled=True,
            oos_evidence_periods=5,
            oos_evidence_min_periods=2,
        )
        optimizer = RegimeOptimizer(cfg)
        features = pd.DataFrame(index=pd.date_range("2026-01-01", periods=12, freq="h"))

        record = optimizer._build_oos_evidence_record(
            "EURUSD",
            "H1",
            {"strategy_name": "NoTradeStrategy", "params": {}},
            {
                "equity_curve": [100.0],
                "total_trades": 0,
                "profit_factor": 0.0,
                "total_return_pct": 0.0,
                "max_drawdown_pct": 0.0,
                "win_rate": 0.0,
            },
            features,
            val_warmup_bars=0,
        )

        self.assertIsNotNone(record)
        self.assertEqual(record["period_returns"], [0.0] * 5)
        self.assertEqual(record["metrics"]["total_trades"], 0)
        self.assertEqual(record["trade_path_evidence"]["status"], "no_trades")
        self.assertEqual(record["execution_cost_survival"]["status"], "not_requested")
        self.assertEqual(record["parameter_stability_evidence"]["status"], "not_available")

    def test_execution_cost_survival_marks_capped_and_error_explicitly(self):
        cfg = PipelineConfig(oos_evidence_export_enabled=True)
        optimizer = RegimeOptimizer(cfg)
        baseline = {"total_return_pct": 5.0, "profit_factor": 1.5, "max_drawdown_pct": 2.0}

        capped = optimizer._execution_cost_survival_evidence(
            baseline,
            {"status": "capped"},
            "stress_p95",
        )
        errored = optimizer._execution_cost_survival_evidence(
            baseline,
            {"status": "error", "error": "boom"},
            "stress_p95",
        )

        self.assertEqual(capped["status"], "capped")
        self.assertEqual(errored["status"], "error")
        self.assertIn("boom", errored["error"])

    def test_oos_evidence_attach_collects_final_candidate_records(self):
        class FakeStrategy:
            name = "FakeEvidenceStrategy"

            def generate_signals(self, features, symbol):
                return pd.Series([1] * len(features), index=features.index)

        class FakeBacktester:
            def __init__(self):
                self.signal_vectors = []

            def run(self, features, signals, symbol, strategy, timeframe=None, warmup_bars=0):
                self.signal_vectors.append(signals.astype(int).tolist())
                return {
                    "equity_curve": [100.0, 101.0, 100.5, 102.0, 101.5, 103.0],
                    "total_trades": 4,
                    "profit_factor": 1.3,
                    "total_return_pct": 3.0,
                    "max_drawdown_pct": 1.0,
                    "win_rate": 50.0,
                }

        cfg = PipelineConfig(
            research_mode=True,
            oos_evidence_export_enabled=True,
            oos_evidence_periods=5,
            oos_evidence_min_periods=2,
            oos_evidence_include_execution_cost_survival=False,
        )
        optimizer = RegimeOptimizer(cfg)
        backtester = FakeBacktester()
        optimizer.backtester = backtester
        features = pd.DataFrame(
            {
                "Close": [1, 2, 3, 4, 5, 6],
                "REGIME_LIVE": ["TREND"] * 3 + ["RANGE"] * 3,
            },
            index=pd.date_range("2026-01-01", periods=6, freq="h"),
        )
        candidate = {
            "strategy": FakeStrategy(),
            "strategy_name": "FakeEvidenceStrategy",
            "params": {"p": 1},
            "search_trials": 1,
            "evaluation_regime": "TREND",
        }

        optimizer._attach_oos_evidence_to_candidates(
            "EURUSD",
            "H1",
            features,
            0,
            [candidate],
        )

        self.assertIn("oos_evidence", candidate)
        self.assertIn("selection_execution_cost_survival", candidate)
        self.assertEqual(candidate["selection_execution_cost_survival"]["status"], "not_requested")
        records = optimizer.consume_oos_candidate_evidence()
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0]["strategy_name"], "FakeEvidenceStrategy")
        self.assertEqual(records[0]["regime"], "TREND")
        self.assertEqual(backtester.signal_vectors, [[1, 1, 1, 0, 0, 0]])

    def test_purged_kfold_ranges_exclude_test_from_train_segments(self):
        ranges = RegimeOptimizer._build_purged_kfold_ranges(
            20,
            scored_start=4,
            splits=4,
            embargo_bars=2,
            warmup_bars=3,
        )

        self.assertEqual(len(ranges), 4)
        for fold in ranges:
            self.assertGreaterEqual(fold["warmup_start"], 0)
            self.assertEqual(fold["warmup_bars"], fold["test_start"] - fold["warmup_start"])
            excluded_start = fold["purge_start"]
            excluded_end = fold["embargo_end"]
            self.assertLessEqual(excluded_start, fold["test_start"])
            self.assertGreaterEqual(excluded_end, fold["test_end"])
            for segment in fold["train_segments"]:
                self.assertTrue(
                    segment["end"] <= excluded_start or segment["start"] >= excluded_end,
                    f"train segment overlaps purged test span: {segment} vs {fold}",
                )

    def test_purged_kfold_aggregate_blocks_bad_traded_fold(self):
        aggregate = RegimeOptimizer._aggregate_purged_kfold_metrics(
            [
                {
                    "fold": 1,
                    "total_trades": 3,
                    "gross_profit": 120.0,
                    "gross_loss": 20.0,
                    "total_pnl": 100.0,
                    "profit_factor": 6.0,
                    "total_return_pct": 1.0,
                    "max_drawdown_pct": 0.5,
                    "win_rate": 66.7,
                },
                {
                    "fold": 2,
                    "total_trades": 2,
                    "gross_profit": 10.0,
                    "gross_loss": 60.0,
                    "total_pnl": -50.0,
                    "profit_factor": 0.166,
                    "total_return_pct": -0.5,
                    "max_drawdown_pct": 1.2,
                    "win_rate": 50.0,
                },
            ],
            initial_capital=10000.0,
            min_trades=5,
            min_profit_factor=1.0,
            min_return_pct=0.0,
            aggregate_policy="aggregate_with_worst_fold_floor",
        )

        self.assertEqual(aggregate["total_trades"], 5)
        self.assertEqual(aggregate["policy_status"], "FAIL")
        self.assertEqual(aggregate["worst_fold"]["fold"], 2)
        self.assertAlmostEqual(aggregate["capital_preservation"], 0.988)

    def test_purged_kfold_evaluation_records_fold_metrics(self):
        strategy_name = "KFoldUnitTestStrategy"

        class KFoldUnitTestStrategy:
            name = strategy_name

            def __init__(self, **params):
                self.params = dict(params)

            def generate_signals(self, features, symbol):
                return pd.Series([1] * len(features), index=features.index)

        class FakeBacktester:
            def run(self, features, signals, symbol, strategy, timeframe=None, warmup_bars=0):
                return {
                    "trades": [
                        {
                            "signal_bar": int(warmup_bars),
                            "entry_bar": int(warmup_bars),
                            "pnl_dollars": 50.0,
                            "pnl_pips": 5.0,
                        }
                    ],
                }

            def _calculate_metrics(self, trades, final_equity, equity_curve, max_dd, features=None, timeframe="", warmup_bars=0):
                total_pnl = sum(float(trade.get("pnl_dollars", 0.0) or 0.0) for trade in trades)
                gross_profit = sum(max(0.0, float(trade.get("pnl_dollars", 0.0) or 0.0)) for trade in trades)
                gross_loss = abs(sum(min(0.0, float(trade.get("pnl_dollars", 0.0) or 0.0)) for trade in trades))
                return {
                    "total_trades": len(trades),
                    "win_rate": 100.0,
                    "profit_factor": float("inf") if gross_profit > 0 and gross_loss == 0 else 0.0,
                    "total_pnl": total_pnl,
                    "gross_profit": gross_profit,
                    "gross_loss": gross_loss,
                    "avg_win_dollars": gross_profit / max(len(trades), 1),
                    "avg_loss_dollars": 0.0,
                    "sharpe_ratio": 0.0,
                    "max_drawdown_pct": 0.0,
                    "total_return_pct": total_pnl / 10000.0 * 100.0,
                }

        old_strategy = StrategyRegistry._strategies.get(strategy_name)
        StrategyRegistry._strategies[strategy_name] = KFoldUnitTestStrategy
        try:
            cfg = PipelineConfig(
                research_mode=True,
                oos_validation_mode="purged_kfold",
                oos_kfold_splits=3,
                oos_embargo_bars=1,
                oos_min_trades=1,
                false_discovery_holdout_min_profit_factor=1.0,
                false_discovery_holdout_min_return_pct=0.0,
            )
            optimizer = RegimeOptimizer(cfg)
            optimizer.backtester = FakeBacktester()
            features = pd.DataFrame(
                {
                    "Close": [1.0] * 12,
                    "REGIME_LIVE": ["TREND"] * 12,
                },
                index=pd.date_range("2026-01-01", periods=12, freq="h"),
            )
            winner = RegimeConfig(strategy_name=strategy_name, parameters={}, quality_score=0.9)

            evidence = optimizer.evaluate_purged_kfold_oos(
                "EURUSD",
                "H1",
                "TREND",
                winner,
                features,
                scored_start=3,
            )

            self.assertIsNotNone(evidence)
            self.assertEqual(evidence["validation_mode"], "purged_kfold")
            self.assertEqual(evidence["fold_count"], 3)
            self.assertEqual(evidence["total_trades"], 3)
            self.assertEqual(evidence["policy_status"], "PASS")
            self.assertEqual(len(evidence["folds"]), 3)
            self.assertEqual(evidence["folds"][0]["warmup_bars"], 3)
        finally:
            if old_strategy is None:
                StrategyRegistry._strategies.pop(strategy_name, None)
            else:
                StrategyRegistry._strategies[strategy_name] = old_strategy

    def test_purged_kfold_policy_status_blocks_promotion(self):
        cfg = PipelineConfig(
            research_mode=True,
            false_discovery_control_enabled=True,
            false_discovery_holdout_policy="block",
            oos_validation_mode="purged_kfold",
            oos_min_trades=5,
        )
        winner = RegimeConfig(
            strategy_name="DemoStrategy",
            parameters={},
            quality_score=0.5,
            holdout_metrics={
                "validation_mode": "purged_kfold",
                "total_trades": 6,
                "profit_factor": 1.4,
                "total_return_pct": 2.0,
                "policy_status": "FAIL",
                "policy_reason": "aggregate OOS or traded-fold PF/return floor failed",
            },
        )

        status, reasons = classify_promotion_status(winner, cfg)

        self.assertEqual(status, "BLOCKED")
        self.assertEqual(reasons, ["aggregate OOS or traded-fold PF/return floor failed"])

    def test_regime_selection_tie_breaks_on_trade_count(self):
        cfg = PipelineConfig(
            research_mode=True,
            false_discovery_control_enabled=False,
            regime_min_train_trades=1,
            regime_min_val_trades=1,
            regime_min_val_profit_factor=0.0,
            regime_min_val_return_pct=-100.0,
            regime_min_val_return_dd_ratio=0.1,
            fx_val_max_drawdown=50.0,
            fx_min_robustness_ratio=0.0,
            fx_val_sharpe_override=-99.0,
        )
        optimizer = RegimeOptimizer(cfg)
        optimizer._compute_regime_score = lambda train, val: 100.0
        optimizer._validate_regime_winner = lambda train, val, regime, candidate_name="": (True, "forced pass")

        def candidate(name, train_trades):
            train = {
                "total_trades": train_trades,
                "profit_factor": 1.5,
                "win_rate": 55.0,
                "total_return_pct": 10.0,
                "max_drawdown_pct": 5.0,
            }
            val = {
                "total_trades": 20,
                "profit_factor": 1.5,
                "win_rate": 55.0,
                "total_return_pct": 10.0,
                "max_drawdown_pct": 5.0,
            }
            return {
                "strategy_name": name,
                "params": {},
                "train_regime_metrics": {"TREND": train},
                "val_regime_metrics": {"TREND": val},
            }

        best_config, is_valid, _ = optimizer._select_best_for_regime(
            "EURUSD",
            "H1",
            "TREND",
            [
                candidate("AlphaLowerSampleStrategy", 10),
                candidate("ZetaHigherSampleStrategy", 40),
            ],
        )

        self.assertTrue(is_valid)
        self.assertEqual(best_config.strategy_name, "ZetaHigherSampleStrategy")

    def test_regime_score_uses_configured_trade_stability_floor(self):
        def make_optimizer(floor):
            cfg = PipelineConfig(
                regime_min_train_trades=15,
                regime_min_val_trades=15,
                regime_trade_stability_floor=floor,
            )
            return RegimeOptimizer(cfg)

        train = {
            "total_trades": 15,
            "profit_factor": 1.7,
            "win_rate": 55.0,
            "sharpe_approx": 1.2,
            "total_return_pct": 10.0,
            "max_drawdown_pct": 5.0,
            "avg_win": 100.0,
            "avg_loss": -60.0,
        }
        val = {
            "total_trades": 15,
            "profit_factor": 1.6,
            "win_rate": 54.0,
            "sharpe_approx": 1.1,
            "total_return_pct": 8.0,
            "max_drawdown_pct": 5.0,
            "avg_win": 90.0,
            "avg_loss": -55.0,
        }

        legacy_floor_score = make_optimizer(0.7)._compute_regime_score(train, val)
        experiment_floor_score = make_optimizer(0.9)._compute_regime_score(train, val)

        self.assertGreater(experiment_floor_score, legacy_floor_score)

    def test_specialist_spoke_regime_score_rewards_stable_generalized_shape(self):
        cfg = PipelineConfig(
            regime_scoring_model="specialist_spokes",
            regime_min_train_trades=20,
            regime_min_val_trades=15,
            train_max_drawdown=20.0,
            fx_val_max_drawdown=12.0,
        )
        optimizer = RegimeOptimizer(cfg)
        train = {
            "total_trades": 40,
            "profit_factor": 1.8,
            "total_return_pct": 12.0,
            "max_drawdown_pct": 5.0,
            "worst_5pct_r": -0.8,
            "max_consecutive_losses": 3,
        }
        stable_val = {
            "total_trades": 24,
            "profit_factor": 1.6,
            "total_return_pct": 9.0,
            "max_drawdown_pct": 4.0,
            "worst_5pct_r": -0.9,
            "max_consecutive_losses": 3,
        }
        collapsed_val = {
            **stable_val,
            "profit_factor": 0.9,
            "total_return_pct": -1.0,
            "max_drawdown_pct": 10.0,
            "worst_5pct_r": -3.0,
            "max_consecutive_losses": 8,
        }

        stable_score = optimizer._compute_regime_score(train, stable_val)
        collapsed_score = optimizer._compute_regime_score(train, collapsed_val)
        evidence = optimizer._compute_specialist_spoke_evidence(train, stable_val)

        self.assertGreater(stable_score, collapsed_score)
        self.assertGreater(stable_score, 0.0)
        self.assertLess(collapsed_score, 1e-6)
        self.assertEqual(
            list(evidence["spokes"].keys()),
            [
                "edge_survival",
                "profit_structure_quality",
                "capital_preservation",
                "tail_risk_control",
                "temporal_consistency",
                "generalization_survival",
                "execution_cost_survival",
                "parameter_stability",
            ],
        )
        self.assertEqual(evidence["spoke_status"]["execution_cost_survival"], "not_available_neutral")

    def test_specialist_spoke_regime_score_uses_saturating_evidence_confidence(self):
        cfg = PipelineConfig(
            regime_scoring_model="specialist_spokes",
            regime_min_train_trades=20,
            regime_min_val_trades=15,
        )
        optimizer = RegimeOptimizer(cfg)
        train = {
            "total_trades": 60,
            "profit_factor": 1.8,
            "total_return_pct": 12.0,
            "max_drawdown_pct": 5.0,
            "worst_5pct_r": -0.8,
            "max_consecutive_losses": 3,
        }
        val = {
            "total_trades": 30,
            "profit_factor": 1.6,
            "total_return_pct": 9.0,
            "max_drawdown_pct": 4.0,
            "worst_5pct_r": -0.9,
            "max_consecutive_losses": 3,
        }
        many_more_trades = {**val, "total_trades": 120, "max_consecutive_losses": 6}

        sufficient_score = optimizer._compute_regime_score(train, val)
        extra_trade_score = optimizer._compute_regime_score(train, many_more_trades)

        self.assertAlmostEqual(extra_trade_score, sufficient_score, places=9)

    def test_threadspark_selector_uses_post_search_selection_and_historical_faces(self):
        cfg = PipelineConfig(
            regime_scoring_model="specialist_spokes_threadspark",
            regime_min_train_trades=20,
            regime_min_val_trades=15,
            train_max_drawdown=20.0,
            fx_val_max_drawdown=12.0,
            historical_audit_max_drawdown=35.0,
        )
        optimizer = RegimeOptimizer(cfg)
        regime = "TREND"
        train = {
            "total_trades": 80,
            "profit_factor": 1.8,
            "total_return_pct": 12.0,
            "max_drawdown_pct": 5.0,
            "worst_5pct_r": -0.8,
            "r_block_downside_deviation": 0.25,
            "r_block_cvar": 0.45,
            "max_consecutive_losses": 3,
            "gross_profit": 1800.0,
            "total_pnl": 1200.0,
        }
        glamorous_val = {
            "total_trades": 30,
            "profit_factor": 2.2,
            "total_return_pct": 14.0,
            "max_drawdown_pct": 3.0,
            "worst_5pct_r": -0.5,
            "r_block_downside_deviation": 0.20,
            "r_block_cvar": 0.30,
            "max_consecutive_losses": 2,
            "gross_profit": 1900.0,
            "total_pnl": 1400.0,
        }
        broad_holdout = {
            "total_trades": 24,
            "profit_factor": 1.5,
            "total_return_pct": 6.0,
            "max_drawdown_pct": 4.0,
            "worst_5pct_r": -0.7,
            "r_block_downside_deviation": 0.30,
            "r_block_cvar": 0.50,
            "max_consecutive_losses": 3,
            "gross_profit": 900.0,
            "total_pnl": 520.0,
        }
        damaged_holdout = {
            **broad_holdout,
            "profit_factor": 0.8,
            "total_return_pct": -3.0,
            "max_drawdown_pct": 11.0,
            "worst_5pct_r": -3.0,
            "r_block_downside_deviation": 1.8,
            "r_block_cvar": 2.7,
            "max_consecutive_losses": 9,
            "gross_profit": 800.0,
            "total_pnl": -200.0,
        }
        good_candidate = {
            "threadspark_selection_surface": "clean_holdout",
            "threadspark_selection_regime_metrics": {regime: broad_holdout},
            "threadspark_historical_regime_metrics": {regime: broad_holdout},
        }
        bad_candidate = {
            "threadspark_selection_surface": "clean_holdout",
            "threadspark_selection_regime_metrics": {regime: damaged_holdout},
            "threadspark_historical_regime_metrics": {regime: damaged_holdout},
        }

        good_score = optimizer._compute_threadspark_spoke_score(
            train, glamorous_val, candidate=good_candidate, regime=regime
        )
        bad_score = optimizer._compute_threadspark_spoke_score(
            train, glamorous_val, candidate=bad_candidate, regime=regime
        )
        evidence = optimizer._compute_threadspark_spoke_evidence(
            train, glamorous_val, candidate=good_candidate, regime=regime
        )

        self.assertGreater(good_score, bad_score * 5.0)
        self.assertEqual(evidence["selector_model"], "specialist_spokes_threadspark")
        self.assertIn("path_persistence", evidence["spokes"])
        self.assertNotIn("temporal_consistency", evidence["spokes"])
        self.assertGreaterEqual(evidence["derived_shape_features"]["evidence_step_count"], 4)
        self.assertEqual(evidence["derived_shape_features"]["final_surface"], "clean_holdout")

    def test_threadspark_historical_chunks_penalize_full_life_collapse(self):
        cfg = PipelineConfig(
            regime_scoring_model="specialist_spokes_threadspark",
            regime_min_train_trades=20,
            regime_min_val_trades=15,
            train_max_drawdown=20.0,
            fx_val_max_drawdown=12.0,
            historical_audit_max_drawdown=35.0,
        )
        optimizer = RegimeOptimizer(cfg)
        regime = "TREND"
        train = {
            "total_trades": 80,
            "profit_factor": 1.8,
            "total_return_pct": 12.0,
            "max_drawdown_pct": 5.0,
            "worst_5pct_r": -0.8,
            "r_block_downside_deviation": 0.25,
            "r_block_cvar": 0.45,
            "max_consecutive_losses": 3,
            "gross_profit": 1800.0,
            "total_pnl": 1200.0,
            "pct_positive_r": 58.0,
            "mean_r": 0.20,
        }
        val = {
            "total_trades": 30,
            "profit_factor": 2.0,
            "total_return_pct": 10.0,
            "max_drawdown_pct": 3.0,
            "worst_5pct_r": -0.5,
            "r_block_downside_deviation": 0.20,
            "r_block_cvar": 0.30,
            "max_consecutive_losses": 2,
            "gross_profit": 1400.0,
            "total_pnl": 1000.0,
            "pct_positive_r": 60.0,
            "mean_r": 0.25,
        }
        clean_holdout = {
            "total_trades": 25,
            "profit_factor": 1.6,
            "total_return_pct": 6.0,
            "max_drawdown_pct": 4.0,
            "worst_5pct_r": -0.7,
            "r_block_downside_deviation": 0.30,
            "r_block_cvar": 0.50,
            "max_consecutive_losses": 3,
            "gross_profit": 900.0,
            "total_pnl": 520.0,
            "pct_positive_r": 56.0,
            "mean_r": 0.15,
        }
        collapse_chunk = {
            **clean_holdout,
            "total_trades": 40,
            "profit_factor": 0.65,
            "total_return_pct": -18.0,
            "max_drawdown_pct": 30.0,
            "worst_5pct_r": -5.0,
            "r_block_downside_deviation": 2.2,
            "r_block_cvar": 4.0,
            "max_consecutive_losses": 12,
            "gross_profit": 700.0,
            "total_pnl": -1800.0,
            "pct_positive_r": 37.0,
            "mean_r": -0.30,
        }
        clean_candidate = {
            "threadspark_selection_surface": "clean_holdout",
            "threadspark_selection_regime_metrics": {regime: clean_holdout},
            "threadspark_historical_chunk_regime_metrics": [
                {regime: clean_holdout},
                {regime: clean_holdout},
            ],
        }
        collapsed_candidate = {
            "threadspark_selection_surface": "clean_holdout",
            "threadspark_selection_regime_metrics": {regime: clean_holdout},
            "threadspark_historical_chunk_regime_metrics": [
                {regime: clean_holdout},
                {regime: collapse_chunk},
            ],
        }

        clean_score = optimizer._compute_threadspark_spoke_score(train, val, clean_candidate, regime)
        collapsed_score = optimizer._compute_threadspark_spoke_score(train, val, collapsed_candidate, regime)
        collapsed_evidence = optimizer._compute_threadspark_spoke_evidence(train, val, collapsed_candidate, regime)

        self.assertGreater(clean_score, collapsed_score * 3.0)
        self.assertIn(
            "historical_cost_chunk_2",
            [step["surface"] for step in collapsed_evidence["evidence_steps"]],
        )
        self.assertLess(collapsed_evidence["derived_shape_features"]["dent_health"], 0.75)

    def test_threadspark_l1_score_is_volume_neutral_after_evidence_sufficiency(self):
        cfg = PipelineConfig(
            regime_scoring_model="specialist_spokes_threadspark",
            regime_min_train_trades=20,
            regime_min_val_trades=15,
        )
        optimizer = RegimeOptimizer(cfg)
        train = {
            "total_trades": 80,
            "profit_factor": 1.8,
            "total_return_pct": 12.0,
            "max_drawdown_pct": 5.0,
            "worst_5pct_r": -0.8,
            "max_consecutive_losses": 3,
            "pct_positive_r": 58.0,
            "mean_r": 0.2,
        }
        val = {
            "total_trades": 30,
            "profit_factor": 1.6,
            "total_return_pct": 9.0,
            "max_drawdown_pct": 4.0,
            "worst_5pct_r": -0.9,
            "max_consecutive_losses": 4,
            "pct_positive_r": 55.0,
            "mean_r": 0.18,
            "std_weekly_return": 0.03,
        }
        high_volume = {**val, "total_trades": 240, "max_consecutive_losses": 4}

        self.assertAlmostEqual(
            optimizer._compute_threadspark_l1_score(train, val),
            optimizer._compute_threadspark_l1_score(train, high_volume),
            places=9,
        )

    def test_threadspark_selection_attachment_writes_historical_chunk_faces(self):
        class FakeStrategy:
            name = "Fake"

            def __init__(self):
                self.params = {}

            def generate_signals(self, features, symbol):
                return pd.Series([1] * len(features), index=features.index)

            def get_params(self):
                return {}

        class FakeBacktester:
            def run(self, features, signals, symbol, strategy, **kwargs):
                trades = [
                    {"signal_bar": 10, "entry_bar": 11, "exit_bar": 12, "pnl_dollars": 120.0},
                    {"signal_bar": 60, "entry_bar": 61, "exit_bar": 62, "pnl_dollars": -40.0},
                ]
                return {
                    "trades": [
                        trade for trade in trades
                        if int(signals.iloc[int(trade["signal_bar"])]) != 0
                    ]
                }

            def _calculate_metrics(self, trades, final_equity, equity_curve, max_dd, features=None, timeframe="", warmup_bars=0):
                pnl = sum(float(t.get("pnl_dollars", 0.0) or 0.0) for t in trades)
                wins = sum(1 for t in trades if float(t.get("pnl_dollars", 0.0) or 0.0) > 0.0)
                losses = sum(1 for t in trades if float(t.get("pnl_dollars", 0.0) or 0.0) < 0.0)
                gross_profit = sum(max(0.0, float(t.get("pnl_dollars", 0.0) or 0.0)) for t in trades)
                gross_loss = abs(sum(min(0.0, float(t.get("pnl_dollars", 0.0) or 0.0)) for t in trades))
                return {
                    "total_trades": len(trades),
                    "win_rate": 100.0 * wins / len(trades) if trades else 0.0,
                    "profit_factor": gross_profit / gross_loss if gross_loss else (10.0 if gross_profit else 0.0),
                    "total_pnl": pnl,
                    "gross_profit": gross_profit,
                    "gross_loss": gross_loss,
                    "avg_win_dollars": gross_profit / wins if wins else 0.0,
                    "avg_loss_dollars": gross_loss / losses if losses else 0.0,
                    "sharpe_ratio": 0.0,
                    "sortino_ratio": 0.0,
                    "calmar_ratio": 0.0,
                    "std_weekly_return": 0.0,
                    "mean_r": pnl / 100.0,
                    "median_r": pnl / 100.0,
                    "pct_positive_r": 100.0 * wins / len(trades) if trades else 0.0,
                    "worst_5pct_r": -0.4 if losses else 0.0,
                    "r_downside_deviation": 0.4 if losses else 0.0,
                    "r_block_downside_deviation": 0.4 if losses else 0.0,
                    "r_block_cvar": 0.4 if losses else 0.0,
                    "max_consecutive_losses": losses,
                    "max_drawdown_pct": max_dd,
                    "total_return_pct": pnl / 100.0,
                }

        cfg = PipelineConfig(
            regime_scoring_model="specialist_spokes_threadspark",
            threadspark_historical_chunk_count=2,
        )
        optimizer = RegimeOptimizer(cfg)
        optimizer.backtester = FakeBacktester()
        features = pd.DataFrame(
            {
                "Close": range(100),
                "REGIME_LIVE": ["TREND"] * 50 + ["RANGE"] * 50,
            },
            index=pd.date_range("2024-01-01", periods=100, freq="h"),
        )
        candidate = {
            "strategy": FakeStrategy(),
            "strategy_name": "Fake",
            "params": {},
            "evaluation_regime": "TREND",
        }

        optimizer._attach_threadspark_selection_evidence_to_candidates(
            "EURUSD",
            "H1",
            None,
            0,
            features,
            [candidate],
            selection_surface="clean_holdout",
        )

        chunks = candidate.get("threadspark_historical_chunk_regime_metrics")
        self.assertEqual(candidate.get("threadspark_historical_chunk_count"), 1)
        self.assertEqual(len(chunks), 1)
        self.assertEqual(chunks[0]["TREND"]["total_trades"], 1)
        self.assertNotIn("RANGE", candidate["threadspark_historical_regime_metrics"])

    def test_threadspark_selector_reranks_only_l1_shortlist(self):
        cfg = PipelineConfig(
            research_mode=True,
            false_discovery_control_enabled=False,
            regime_scoring_model="specialist_spokes_threadspark",
            regime_validation_top_k=2,
            regime_min_train_trades=10,
            regime_min_val_trades=5,
            regime_min_val_profit_factor=1.0,
            regime_min_val_return_pct=0.0,
            regime_min_val_return_dd_ratio=0.1,
            fx_val_max_drawdown=20.0,
            fx_min_robustness_ratio=0.0,
        )
        optimizer = RegimeOptimizer(cfg)
        regime = "TREND"

        def metrics():
            return {
                "total_trades": 20,
                "profit_factor": 1.5,
                "total_return_pct": 5.0,
                "max_drawdown_pct": 4.0,
                "worst_5pct_r": -0.8,
                "max_consecutive_losses": 2,
            }

        candidates = [
            {
                "strategy_name": "TopBase",
                "params": {},
                "train_regime_metrics": {regime: metrics()},
                "val_regime_metrics": {regime: metrics()},
                "base": 100.0,
                "threadspark": 1.0,
                "search_trials": 1,
            },
            {
                "strategy_name": "SecondBase",
                "params": {},
                "train_regime_metrics": {regime: metrics()},
                "val_regime_metrics": {regime: metrics()},
                "base": 90.0,
                "threadspark": 2.0,
                "search_trials": 1,
            },
            {
                "strategy_name": "DeepHoldoutGlamour",
                "params": {},
                "train_regime_metrics": {regime: metrics()},
                "val_regime_metrics": {regime: metrics()},
                "base": 10.0,
                "threadspark": 999.0,
                "search_trials": 1,
            },
        ]
        optimizer._compute_threadspark_l1_score = lambda train, val, candidate=None: candidate["base"]
        optimizer._compute_threadspark_spoke_score = (
            lambda train, val, candidate=None, regime=None: candidate["threadspark"]
        )

        config, is_valid, reason = optimizer._select_best_for_regime("EURUSD", "H1", regime, candidates)

        self.assertTrue(is_valid, reason)
        self.assertEqual(config.strategy_name, "SecondBase")

    def test_threadspark_tie_break_does_not_prefer_more_trades(self):
        cfg = PipelineConfig(
            research_mode=True,
            false_discovery_control_enabled=False,
            regime_scoring_model="specialist_spokes_threadspark",
            regime_validation_top_k=2,
            regime_min_train_trades=10,
            regime_min_val_trades=5,
            regime_min_val_profit_factor=1.0,
            regime_min_val_return_pct=0.0,
            regime_min_val_return_dd_ratio=0.1,
            fx_val_max_drawdown=20.0,
            fx_min_robustness_ratio=0.0,
        )
        optimizer = RegimeOptimizer(cfg)
        regime = "TREND"

        def metrics(trades):
            return {
                "total_trades": trades,
                "profit_factor": 1.5,
                "total_return_pct": 5.0,
                "max_drawdown_pct": 4.0,
                "worst_5pct_r": -0.8,
                "max_consecutive_losses": 2,
            }

        candidates = [
            {
                "strategy_name": "A_LowTrades",
                "params": {},
                "train_regime_metrics": {regime: metrics(20)},
                "val_regime_metrics": {regime: metrics(20)},
                "search_trials": 1,
            },
            {
                "strategy_name": "Z_HighTrades",
                "params": {},
                "train_regime_metrics": {regime: metrics(200)},
                "val_regime_metrics": {regime: metrics(200)},
                "search_trials": 1,
            },
        ]
        optimizer._compute_threadspark_l1_score = lambda train, val, candidate=None: 1.0
        optimizer._compute_threadspark_spoke_score = lambda train, val, candidate=None, regime=None: 1.0

        config, is_valid, reason = optimizer._select_best_for_regime("EURUSD", "H1", regime, candidates)

        self.assertTrue(is_valid, reason)
        self.assertEqual(config.strategy_name, "A_LowTrades")

    def test_threadspark_requested_clean_holdout_missing_is_flagged_not_validation_fallback(self):
        cfg = PipelineConfig(
            regime_scoring_model="specialist_spokes_threadspark",
            regime_min_train_trades=10,
            regime_min_val_trades=5,
            threadspark_selection_surface="clean_holdout",
        )
        optimizer = RegimeOptimizer(cfg)
        regime = "TREND"
        metrics = {
            "total_trades": 20,
            "profit_factor": 1.5,
            "total_return_pct": 5.0,
            "max_drawdown_pct": 4.0,
            "worst_5pct_r": -0.8,
            "max_consecutive_losses": 2,
        }
        candidate = {
            "strategy_name": "HoldoutMissing",
            "params": {},
            "train_regime_metrics": {regime: metrics},
            "val_regime_metrics": {regime: metrics},
            "threadspark_selection_surface": "clean_holdout_missing",
            "threadspark_selection_missing": True,
            "threadspark_selection_missing_reason": "requested_clean_holdout_unavailable_or_too_thin",
        }

        evidence = optimizer._compute_threadspark_spoke_evidence(
            metrics, metrics, candidate, regime
        )

        self.assertEqual(evidence["derived_shape_features"]["final_surface"], "clean_holdout_missing")
        self.assertFalse(evidence["derived_shape_features"]["selection_surface_available"])
        self.assertEqual(
            evidence["derived_shape_features"]["selection_missing_reason"],
            "requested_clean_holdout_unavailable_or_too_thin",
        )
        self.assertIn(
            "missing_requested_surface",
            [step["status"]["evidence"] for step in evidence["evidence_steps"]],
        )

    def test_specialist_spoke_tail_axis_prefers_block_cvar_when_available(self):
        mild = {
            "total_trades": 40,
            "profit_factor": 1.5,
            "total_return_pct": 5.0,
            "max_drawdown_pct": 4.0,
            "worst_5pct_r": -1.0,
            "r_block_downside_deviation": 0.2,
            "r_block_cvar": 0.4,
        }
        damaged = {
            **mild,
            "r_block_downside_deviation": 2.0,
            "r_block_cvar": 3.0,
        }
        mild_axes = RegimeOptimizer._surface_spoke_axes(mild, drawdown_limit=12.0)
        damaged_axes = RegimeOptimizer._surface_spoke_axes(damaged, drawdown_limit=12.0)

        self.assertGreater(mild_axes["tail_risk_control"], damaged_axes["tail_risk_control"])

    def test_specialist_spoke_score_uses_parameter_stability_when_available(self):
        cfg = PipelineConfig(
            regime_scoring_model="specialist_spokes",
            regime_min_train_trades=20,
            regime_min_val_trades=15,
        )
        optimizer = RegimeOptimizer(cfg)
        train = {
            "total_trades": 60,
            "profit_factor": 1.8,
            "total_return_pct": 12.0,
            "max_drawdown_pct": 5.0,
            "worst_5pct_r": -0.8,
            "max_consecutive_losses": 3,
            "gross_profit": 1500.0,
            "total_pnl": 1100.0,
        }
        val = {
            "total_trades": 30,
            "profit_factor": 1.6,
            "total_return_pct": 9.0,
            "max_drawdown_pct": 4.0,
            "worst_5pct_r": -0.9,
            "max_consecutive_losses": 3,
            "gross_profit": 1000.0,
            "total_pnl": 700.0,
        }
        weak_plateau = {
            "parameter_stability_evidence": {
                "status": "available",
                "nearest_neighbor_median_score_ratio": 0.35,
            }
        }
        stable_plateau = {
            "parameter_stability_evidence": {
                "status": "available",
                "nearest_neighbor_median_score_ratio": 0.95,
            }
        }

        weak_score = optimizer._compute_regime_score(train, val, candidate=weak_plateau)
        stable_score = optimizer._compute_regime_score(train, val, candidate=stable_plateau)

        self.assertGreater(stable_score, weak_score)

    def test_specialist_spokes_v2_penalizes_weak_high_frequency_cost_surface(self):
        cfg = PipelineConfig(
            regime_scoring_model="specialist_spokes_v2",
            regime_min_train_trades=20,
            regime_min_val_trades=15,
            specialist_spokes_v2_cost_frequency_strength=0.50,
        )
        optimizer = RegimeOptimizer(cfg)
        train = {
            "total_trades": 60,
            "profit_factor": 1.8,
            "total_return_pct": 12.0,
            "max_drawdown_pct": 5.0,
            "worst_5pct_r": -0.8,
            "max_consecutive_losses": 3,
            "gross_profit": 1500.0,
            "total_pnl": 1100.0,
            "mean_r": 0.18,
        }
        efficient_val = {
            "total_trades": 30,
            "profit_factor": 1.7,
            "total_return_pct": 10.0,
            "max_drawdown_pct": 4.0,
            "worst_5pct_r": -0.8,
            "max_consecutive_losses": 3,
            "gross_profit": 1000.0,
            "total_pnl": 720.0,
            "mean_r": 0.20,
        }
        churn_val = {
            **efficient_val,
            "total_trades": 120,
            "profit_factor": 1.1,
            "gross_profit": 3000.0,
            "total_pnl": 180.0,
            "mean_r": 0.01,
        }

        efficient = optimizer._compute_specialist_spoke_v2_evidence(train, efficient_val)
        churn = optimizer._compute_specialist_spoke_v2_evidence(train, churn_val)

        self.assertGreater(efficient["cost_frequency_evidence"]["factor"], churn["cost_frequency_evidence"]["factor"])
        self.assertLess(churn["cost_frequency_evidence"]["factor"], 1.0)
        self.assertGreater(efficient["score"], churn["score"])
        self.assertIn("train", churn["cost_frequency_evidence"])
        self.assertIn("validation", churn["cost_frequency_evidence"])

    def test_specialist_spokes_v2_cost_frequency_penalizes_churny_train_surface(self):
        cfg = PipelineConfig(
            regime_scoring_model="specialist_spokes_v2",
            regime_min_train_trades=20,
            regime_min_val_trades=15,
            specialist_spokes_v2_cost_frequency_strength=0.50,
        )
        optimizer = RegimeOptimizer(cfg)
        clean_train = {
            "total_trades": 40,
            "profit_factor": 1.8,
            "total_return_pct": 12.0,
            "max_drawdown_pct": 5.0,
            "gross_profit": 1500.0,
            "total_pnl": 1000.0,
            "mean_r": 0.18,
        }
        churn_train = {
            **clean_train,
            "total_trades": 180,
            "profit_factor": 1.05,
            "gross_profit": 5000.0,
            "total_pnl": 200.0,
            "mean_r": 0.01,
        }
        same_val = {
            "total_trades": 30,
            "profit_factor": 1.7,
            "total_return_pct": 10.0,
            "max_drawdown_pct": 4.0,
            "gross_profit": 1000.0,
            "total_pnl": 720.0,
            "mean_r": 0.20,
        }

        clean = optimizer._specialist_spokes_v2_cost_frequency_factor(clean_train, same_val)
        churn = optimizer._specialist_spokes_v2_cost_frequency_factor(churn_train, same_val)

        self.assertGreater(clean["factor"], churn["factor"])
        self.assertEqual(churn["limiting_surface"], "train")
        self.assertLess(churn["train"]["factor"], churn["validation"]["factor"])

    def test_specialist_spokes_v2_score_ignores_holdout_and_promotion_fields(self):
        cfg = PipelineConfig(
            regime_scoring_model="specialist_spokes_v2",
            regime_min_train_trades=20,
            regime_min_val_trades=15,
        )
        optimizer = RegimeOptimizer(cfg)
        train = {
            "total_trades": 60,
            "profit_factor": 1.8,
            "total_return_pct": 12.0,
            "max_drawdown_pct": 5.0,
            "worst_5pct_r": -0.8,
            "max_consecutive_losses": 3,
            "gross_profit": 1500.0,
            "total_pnl": 1100.0,
            "mean_r": 0.18,
        }
        val = {
            "total_trades": 30,
            "profit_factor": 1.6,
            "total_return_pct": 9.0,
            "max_drawdown_pct": 4.0,
            "worst_5pct_r": -0.9,
            "max_consecutive_losses": 3,
            "gross_profit": 1000.0,
            "total_pnl": 700.0,
            "mean_r": 0.17,
        }
        clean_candidate = {"parameter_stability_evidence": {"status": "not_available"}}
        leaky_candidate = {
            **clean_candidate,
            "holdout_metrics": {"profit_factor": 99.0, "total_return_pct": 999.0},
            "promotion_status": "PROMOTED",
            "future_forward_return_pct": 999.0,
        }

        clean_score = optimizer._compute_regime_score(train, val, candidate=clean_candidate)
        leaky_score = optimizer._compute_regime_score(train, val, candidate=leaky_candidate)

        self.assertAlmostEqual(clean_score, leaky_score, places=12)

    def test_specialist_spoke_score_ignores_holdout_and_promotion_fields(self):
        cfg = PipelineConfig(
            regime_scoring_model="specialist_spokes",
            regime_min_train_trades=20,
            regime_min_val_trades=15,
        )
        optimizer = RegimeOptimizer(cfg)
        train = {
            "total_trades": 60,
            "profit_factor": 1.8,
            "total_return_pct": 12.0,
            "max_drawdown_pct": 5.0,
            "worst_5pct_r": -0.8,
            "max_consecutive_losses": 3,
            "gross_profit": 1500.0,
            "total_pnl": 1100.0,
        }
        val = {
            "total_trades": 30,
            "profit_factor": 1.6,
            "total_return_pct": 9.0,
            "max_drawdown_pct": 4.0,
            "worst_5pct_r": -0.9,
            "max_consecutive_losses": 3,
            "gross_profit": 1000.0,
            "total_pnl": 700.0,
        }
        clean_candidate = {"parameter_stability_evidence": {"status": "not_available"}}
        leaky_candidate = {
            **clean_candidate,
            "holdout_metrics": {"profit_factor": 99.0, "total_return_pct": 999.0},
            "promotion_status": "PROMOTE",
            "future_forward_return_pct": 999.0,
            "oos_evidence": {
                "execution_cost_survival": {
                    "status": "available",
                    "baseline_return_pct": 1.0,
                    "stress_return_pct": 999.0,
                    "baseline_profit_factor": 1.0,
                    "stress_profit_factor": 999.0,
                    "survives_positive_return": True,
                    "survives_pf_ge_1": True,
                }
            },
        }

        clean_score = optimizer._compute_regime_score(train, val, candidate=clean_candidate)
        leaky_score = optimizer._compute_regime_score(train, val, candidate=leaky_candidate)

        self.assertAlmostEqual(clean_score, leaky_score, places=12)

    def test_specialist_spoke_selection_prefers_stable_parameter_plateau(self):
        cfg = PipelineConfig(
            research_mode=True,
            false_discovery_control_enabled=False,
            regime_scoring_model="specialist_spokes",
            regime_min_train_trades=20,
            regime_min_val_trades=15,
            regime_min_val_profit_factor=1.05,
            regime_min_val_return_pct=5.0,
            regime_min_val_return_dd_ratio=1.0,
            fx_val_max_drawdown=12.0,
            fx_min_robustness_ratio=0.0,
            fx_val_sharpe_override=-99.0,
        )
        optimizer = RegimeOptimizer(cfg)
        train = {
            "total_trades": 60,
            "profit_factor": 1.8,
            "win_rate": 55.0,
            "sharpe_approx": 1.4,
            "total_return_pct": 12.0,
            "max_drawdown_pct": 5.0,
            "worst_5pct_r": -0.8,
            "max_consecutive_losses": 3,
            "gross_profit": 1500.0,
            "gross_loss": 500.0,
            "total_pnl": 1000.0,
        }
        val = {
            "total_trades": 30,
            "profit_factor": 1.6,
            "win_rate": 54.0,
            "sharpe_approx": 1.2,
            "total_return_pct": 9.0,
            "max_drawdown_pct": 4.0,
            "worst_5pct_r": -0.9,
            "max_consecutive_losses": 3,
            "gross_profit": 1000.0,
            "gross_loss": 400.0,
            "total_pnl": 600.0,
        }

        def candidate(name, stability_ratio):
            return {
                "strategy_name": name,
                "params": {"plateau": stability_ratio},
                "search_trials": 50,
                "train_regime_metrics": {"TREND": dict(train)},
                "val_regime_metrics": {"TREND": dict(val)},
                "parameter_stability_evidence": {
                    "status": "available",
                    "nearest_neighbor_median_score_ratio": stability_ratio,
                },
            }

        best_config, is_valid, _ = optimizer._select_best_for_regime(
            "EURUSD",
            "H1",
            "TREND",
            [
                candidate("WeakPlateauStrategy", 0.35),
                candidate("StablePlateauStrategy", 0.95),
            ],
        )

        self.assertTrue(is_valid)
        self.assertEqual(best_config.strategy_name, "StablePlateauStrategy")
        self.assertGreater(best_config.validation_evidence["selection_score_raw"], 0.0)

    def test_asym_specialist_spoke_regime_score_reports_validated_primary_spokes(self):
        cfg = PipelineConfig(
            regime_scoring_model="specialist_spokes_asym",
            regime_min_train_trades=20,
            regime_min_val_trades=15,
            train_max_drawdown=20.0,
            fx_val_max_drawdown=12.0,
        )
        optimizer = RegimeOptimizer(cfg)
        train = {
            "total_trades": 80,
            "profit_factor": 1.8,
            "total_return_pct": 18.0,
            "max_drawdown_pct": 5.0,
            "calmar_ratio": 3.6,
            "sortino_approx": 2.0,
            "mean_r": 0.25,
            "median_r": 0.20,
            "pct_positive_r": 58.0,
            "worst_5pct_r": -1.1,
            "max_consecutive_losses": 5,
            "std_weekly_return": 0.02,
            "gross_profit": 2400.0,
            "gross_loss": 1000.0,
            "total_pnl": 1400.0,
            "avg_win": 120.0,
            "avg_loss": -80.0,
        }
        val = {
            **train,
            "total_trades": 40,
            "profit_factor": 1.55,
            "total_return_pct": 12.0,
            "max_drawdown_pct": 4.0,
            "calmar_ratio": 3.0,
            "sortino_approx": 1.8,
            "mean_r": 0.18,
            "median_r": 0.15,
            "pct_positive_r": 56.0,
            "worst_5pct_r": -0.8,
            "max_consecutive_losses": 4,
            "std_weekly_return": 0.015,
            "gross_profit": 1800.0,
            "gross_loss": 850.0,
            "total_pnl": 950.0,
            "avg_win": 110.0,
            "avg_loss": -80.0,
        }

        evidence = optimizer._compute_asym_specialist_spoke_evidence(train, val)

        self.assertEqual(
            list(evidence["spokes"].keys()),
            [
                "risk_adjusted_edge",
                "trade_quality",
                "gain_pain_efficiency",
                "capital_preservation",
                "tail_risk_control",
                "generalization_survival",
                "execution_cost_survival",
                "parameter_stability",
            ],
        )
        self.assertEqual(
            list(evidence["diagnostic_spokes"].keys()),
            [
                "conditional_upside_capture",
                "path_persistence",
            ],
        )
        self.assertGreater(evidence["score"], 0.0)
        self.assertIn("directional_topology_score", evidence["derived_shape_features"])

    def test_asym_specialist_spoke_score_ignores_holdout_and_promotion_fields(self):
        cfg = PipelineConfig(
            regime_scoring_model="specialist_spokes_asym",
            regime_min_train_trades=20,
            regime_min_val_trades=15,
        )
        optimizer = RegimeOptimizer(cfg)
        train = {
            "total_trades": 80,
            "profit_factor": 1.8,
            "total_return_pct": 18.0,
            "max_drawdown_pct": 5.0,
            "calmar_ratio": 3.6,
            "sortino_approx": 2.0,
            "mean_r": 0.25,
            "median_r": 0.20,
            "pct_positive_r": 58.0,
            "worst_5pct_r": -1.1,
            "max_consecutive_losses": 5,
            "std_weekly_return": 0.02,
            "gross_profit": 2400.0,
            "gross_loss": 1000.0,
            "total_pnl": 1400.0,
            "avg_win": 120.0,
            "avg_loss": -80.0,
        }
        val = {
            **train,
            "total_trades": 40,
            "profit_factor": 1.55,
            "total_return_pct": 12.0,
            "max_drawdown_pct": 4.0,
            "calmar_ratio": 3.0,
            "sortino_approx": 1.8,
            "mean_r": 0.18,
            "median_r": 0.15,
            "pct_positive_r": 56.0,
            "worst_5pct_r": -0.8,
            "max_consecutive_losses": 4,
            "std_weekly_return": 0.015,
            "gross_profit": 1800.0,
            "gross_loss": 850.0,
            "total_pnl": 950.0,
            "avg_win": 110.0,
            "avg_loss": -80.0,
        }
        clean_candidate = {
            "parameter_stability_evidence": {
                "status": "available",
                "nearest_neighbor_median_score_ratio": 0.85,
            }
        }
        leaky_candidate = {
            **clean_candidate,
            "holdout_metrics": {"profit_factor": 99.0, "total_return_pct": 999.0},
            "promotion_status": "PROMOTE",
            "future_forward_return_pct": 999.0,
            "oos_evidence": {
                "execution_cost_survival": {
                    "status": "available",
                    "baseline_return_pct": 1.0,
                    "stress_return_pct": 999.0,
                    "baseline_profit_factor": 1.0,
                    "stress_profit_factor": 999.0,
                    "survives_positive_return": True,
                    "survives_pf_ge_1": True,
                }
            },
        }

        clean_score = optimizer._compute_regime_score(train, val, candidate=clean_candidate)
        leaky_score = optimizer._compute_regime_score(train, val, candidate=leaky_candidate)

        self.assertAlmostEqual(clean_score, leaky_score, places=12)

    def test_asym_specialist_spoke_score_blocks_unprotected_upside(self):
        cfg = PipelineConfig(
            regime_scoring_model="specialist_spokes_asym",
            regime_min_train_trades=20,
            regime_min_val_trades=15,
            train_max_drawdown=20.0,
            fx_val_max_drawdown=12.0,
        )
        optimizer = RegimeOptimizer(cfg)
        train = {
            "total_trades": 80,
            "profit_factor": 1.8,
            "total_return_pct": 18.0,
            "max_drawdown_pct": 5.0,
            "calmar_ratio": 3.6,
            "sortino_approx": 2.0,
            "mean_r": 0.25,
            "median_r": 0.20,
            "pct_positive_r": 58.0,
            "worst_5pct_r": -1.1,
            "max_consecutive_losses": 5,
            "std_weekly_return": 0.02,
            "gross_profit": 2400.0,
            "gross_loss": 1000.0,
            "total_pnl": 1400.0,
            "avg_win": 120.0,
            "avg_loss": -80.0,
        }
        protected = {
            **train,
            "total_trades": 40,
            "profit_factor": 1.55,
            "total_return_pct": 12.0,
            "max_drawdown_pct": 4.0,
            "calmar_ratio": 3.0,
            "sortino_approx": 1.8,
            "mean_r": 0.18,
            "median_r": 0.15,
            "pct_positive_r": 56.0,
            "worst_5pct_r": -0.8,
            "max_consecutive_losses": 4,
            "std_weekly_return": 0.015,
            "gross_profit": 1800.0,
            "gross_loss": 850.0,
            "total_pnl": 950.0,
            "avg_win": 110.0,
            "avg_loss": -80.0,
        }
        unprotected_lottery = {
            **protected,
            "profit_factor": 2.4,
            "total_return_pct": 35.0,
            "max_drawdown_pct": 11.0,
            "mean_r": 0.45,
            "median_r": -0.05,
            "pct_positive_r": 45.0,
            "worst_5pct_r": -4.0,
            "max_consecutive_losses": 9,
            "std_weekly_return": 0.08,
            "gross_profit": 4500.0,
            "gross_loss": 1800.0,
            "total_pnl": 2700.0,
            "avg_win": 450.0,
            "avg_loss": -90.0,
        }

        protected_evidence = optimizer._compute_asym_specialist_spoke_evidence(train, protected)
        lottery_evidence = optimizer._compute_asym_specialist_spoke_evidence(train, unprotected_lottery)

        self.assertGreater(protected_evidence["score"], lottery_evidence["score"])
        self.assertLess(
            lottery_evidence["diagnostic_spokes"]["conditional_upside_capture"],
            protected_evidence["diagnostic_spokes"]["conditional_upside_capture"],
        )
        self.assertEqual(lottery_evidence["derived_shape_features"]["risk_gate"], 0.0)

    def test_asym_specialist_spoke_score_does_not_veto_one_moderate_axis(self):
        cfg = PipelineConfig(
            regime_scoring_model="specialist_spokes_asym",
            regime_min_train_trades=20,
            regime_min_val_trades=15,
            train_max_drawdown=20.0,
            fx_val_max_drawdown=12.0,
        )
        optimizer = RegimeOptimizer(cfg)
        train = {
            "total_trades": 80,
            "profit_factor": 1.8,
            "total_return_pct": 18.0,
            "max_drawdown_pct": 5.0,
            "calmar_ratio": 3.6,
            "sortino_approx": 2.0,
            "mean_r": 0.25,
            "median_r": 0.20,
            "pct_positive_r": 58.0,
            "worst_5pct_r": -1.1,
            "max_consecutive_losses": 5,
            "std_weekly_return": 0.02,
            "gross_profit": 2400.0,
            "gross_loss": 1000.0,
            "total_pnl": 1400.0,
            "avg_win": 120.0,
            "avg_loss": -80.0,
        }
        strong_with_one_moderate_tail_axis = {
            **train,
            "total_trades": 40,
            "profit_factor": 1.7,
            "total_return_pct": 16.0,
            "max_drawdown_pct": 4.0,
            "calmar_ratio": 4.0,
            "sortino_approx": 2.2,
            "mean_r": 0.22,
            "median_r": 0.20,
            "pct_positive_r": 56.0,
            "worst_5pct_r": -1.4,
            "max_consecutive_losses": 5,
            "gross_profit": 2200.0,
            "gross_loss": 900.0,
            "total_pnl": 1300.0,
            "avg_win": 120.0,
            "avg_loss": -90.0,
        }
        lower_edge_clean_shape = {
            **train,
            "total_trades": 40,
            "profit_factor": 1.35,
            "total_return_pct": 7.0,
            "max_drawdown_pct": 3.0,
            "calmar_ratio": 2.3,
            "sortino_approx": 1.2,
            "mean_r": 0.08,
            "median_r": 0.07,
            "pct_positive_r": 58.0,
            "worst_5pct_r": -0.8,
            "max_consecutive_losses": 3,
            "std_weekly_return": 0.01,
            "gross_profit": 1400.0,
            "gross_loss": 1040.0,
            "total_pnl": 360.0,
            "avg_win": 75.0,
            "avg_loss": -65.0,
        }

        strong_score = optimizer._compute_regime_score(train, strong_with_one_moderate_tail_axis)
        clean_shape_score = optimizer._compute_regime_score(train, lower_edge_clean_shape)

        self.assertGreater(strong_score, clean_shape_score)

    def test_asym_specialist_spoke_score_uses_stress_cost_survival_evidence(self):
        cfg = PipelineConfig(
            regime_scoring_model="specialist_spokes_asym",
            regime_min_train_trades=20,
            regime_min_val_trades=15,
        )
        optimizer = RegimeOptimizer(cfg)
        train = {
            "total_trades": 80,
            "profit_factor": 1.8,
            "total_return_pct": 18.0,
            "max_drawdown_pct": 5.0,
            "calmar_ratio": 3.6,
            "sortino_approx": 2.0,
            "mean_r": 0.25,
            "median_r": 0.20,
            "pct_positive_r": 58.0,
            "worst_5pct_r": -1.1,
            "max_consecutive_losses": 5,
            "std_weekly_return": 0.02,
            "gross_profit": 2400.0,
            "gross_loss": 1000.0,
            "total_pnl": 1400.0,
            "avg_win": 120.0,
            "avg_loss": -80.0,
        }
        val = {
            **train,
            "total_trades": 40,
            "profit_factor": 1.55,
            "total_return_pct": 12.0,
            "max_drawdown_pct": 4.0,
            "calmar_ratio": 3.0,
            "sortino_approx": 1.8,
            "mean_r": 0.18,
            "median_r": 0.15,
            "pct_positive_r": 56.0,
            "worst_5pct_r": -0.8,
            "max_consecutive_losses": 4,
            "std_weekly_return": 0.015,
            "gross_profit": 1800.0,
            "gross_loss": 850.0,
            "total_pnl": 950.0,
            "avg_win": 110.0,
            "avg_loss": -80.0,
        }
        survives_stress = {
            "selection_execution_cost_survival": {
                "status": "available",
                "baseline_return_pct": 10.0,
                "stress_return_pct": 9.0,
                "baseline_profit_factor": 1.5,
                "stress_profit_factor": 1.4,
                "survives_positive_return": True,
                "survives_pf_ge_1": True,
            }
        }
        fails_stress = {
            "selection_execution_cost_survival": {
                "status": "available",
                "baseline_return_pct": 10.0,
                "stress_return_pct": -2.0,
                "baseline_profit_factor": 1.5,
                "stress_profit_factor": 0.8,
                "survives_positive_return": False,
                "survives_pf_ge_1": False,
            }
        }

        survives = optimizer._compute_asym_specialist_spoke_evidence(train, val, survives_stress)
        fails = optimizer._compute_asym_specialist_spoke_evidence(train, val, fails_stress)

        self.assertGreater(
            survives["spokes"]["execution_cost_survival"],
            fails["spokes"]["execution_cost_survival"],
        )
        self.assertGreater(survives["score"], fails["score"])

    def test_asym_specialist_spoke_zero_metric_values_do_not_fall_back(self):
        cfg = PipelineConfig(
            regime_scoring_model="specialist_spokes_asym",
            regime_min_train_trades=20,
            regime_min_val_trades=15,
        )
        optimizer = RegimeOptimizer(cfg)
        metrics = {
            "total_trades": 40,
            "profit_factor": 1.2,
            "total_return_pct": 3.0,
            "max_drawdown_pct": 4.0,
            "calmar_ratio": 0.8,
            "sortino_approx": 0.5,
            "mean_r": 0.02,
            "median_r": 0.01,
            "pct_positive_r": 0.0,
            "win_rate": 75.0,
            "worst_5pct_r": -0.8,
            "max_consecutive_losses": 3,
            "gross_profit": 1000.0,
            "gross_loss": 900.0,
            "total_pnl": 100.0,
            "avg_win": 90.0,
            "avg_loss": -85.0,
        }

        axes, _ = optimizer._asym_surface_spoke_axes(
            metrics,
            drawdown_limit=12.0,
            train_metrics=None,
        )
        without_pct = dict(metrics)
        without_pct.pop("pct_positive_r")
        fallback_axes, _ = optimizer._asym_surface_spoke_axes(
            without_pct,
            drawdown_limit=12.0,
            train_metrics=None,
        )

        self.assertLess(axes["path_persistence"], fallback_axes["path_persistence"])

    def test_bucket_metrics_emit_block_downside_tail_fields(self):
        cfg = PipelineConfig(initial_capital=10000.0)
        optimizer = RegimeOptimizer(cfg)
        features = pd.DataFrame(index=pd.date_range("2026-01-01", periods=20, freq="h"))
        trades = [
            {
                "signal_bar": idx,
                "entry_bar": idx,
                "exit_bar": idx + 1,
                "pnl_dollars": pnl,
                "pnl_pips": pnl / 10.0,
                "r_multiple": r,
                "risk_amount": 100.0,
                "exit_reason": "test",
                "direction": "LONG",
            }
            for idx, (pnl, r) in enumerate(
                [
                    (120.0, 1.2),
                    (-40.0, -0.4),
                    (80.0, 0.8),
                    (-160.0, -1.6),
                    (90.0, 0.9),
                    (-60.0, -0.6),
                    (70.0, 0.7),
                    (-220.0, -2.2),
                ]
            )
        ]

        metrics = optimizer._compute_bucket_metrics(trades, features=features, timeframe="H1")

        self.assertIn("r_downside_deviation", metrics)
        self.assertIn("r_block_downside_deviation", metrics)
        self.assertIn("r_block_cvar", metrics)
        self.assertGreater(metrics["r_downside_deviation"], 0.0)
        self.assertGreater(metrics["r_block_cvar"], 0.0)

    def test_bucket_sharpe_uses_dated_daily_returns_not_timeframe_label(self):
        cfg = PipelineConfig(initial_capital=10000.0)
        optimizer = RegimeOptimizer(cfg)
        features = pd.DataFrame(index=pd.date_range("2025-01-01", periods=24 * 90, freq="h"))
        pnls = [120.0, -80.0, 100.0, -70.0, 130.0, -60.0, 90.0, -50.0]
        trades = []
        for idx, pnl in enumerate(pnls):
            entry_bar = 24 * (5 + idx * 10)
            trades.append({
                "signal_bar": entry_bar - 1,
                "entry_bar": entry_bar,
                "exit_bar": entry_bar + 2,
                "pnl_dollars": pnl,
                "pnl_pips": pnl / 10.0,
                "r_multiple": pnl / 100.0,
                "risk_amount": 100.0,
                "exit_reason": "test",
                "direction": "LONG",
            })

        as_h1 = optimizer._compute_bucket_metrics(trades, features=features, timeframe="H1")
        as_m5 = optimizer._compute_bucket_metrics(trades, features=features, timeframe="M5")

        self.assertEqual(as_h1["risk_return_frequency"], "daily_realized")
        self.assertEqual(as_h1["sharpe_approx"], as_m5["sharpe_approx"])
        self.assertEqual(as_h1["sortino_approx"], as_m5["sortino_approx"])

    def test_asym_tail_risk_uses_block_downside_metrics(self):
        cfg = PipelineConfig(regime_scoring_model="specialist_spokes_asym")
        optimizer = RegimeOptimizer(cfg)
        base = {
            "total_trades": 40,
            "profit_factor": 1.6,
            "total_return_pct": 10.0,
            "max_drawdown_pct": 4.0,
            "calmar_ratio": 2.5,
            "sortino_approx": 1.5,
            "mean_r": 0.18,
            "median_r": 0.15,
            "pct_positive_r": 55.0,
            "worst_5pct_r": -1.0,
            "max_consecutive_losses": 4,
            "gross_profit": 1800.0,
            "gross_loss": 900.0,
            "total_pnl": 900.0,
            "avg_win": 110.0,
            "avg_loss": -80.0,
        }
        clean_tail = {
            **base,
            "r_downside_deviation": 0.25,
            "r_block_downside_deviation": 0.20,
            "r_block_cvar": 0.35,
        }
        damaged_tail = {
            **base,
            "r_downside_deviation": 1.30,
            "r_block_downside_deviation": 1.80,
            "r_block_cvar": 2.50,
        }

        clean_axes, _ = optimizer._asym_surface_spoke_axes(
            clean_tail,
            drawdown_limit=12.0,
            train_metrics=None,
        )
        damaged_axes, _ = optimizer._asym_surface_spoke_axes(
            damaged_tail,
            drawdown_limit=12.0,
            train_metrics=None,
        )

        self.assertGreater(clean_axes["tail_risk_control"], damaged_axes["tail_risk_control"])

    def test_regime_bucket_uses_signal_bar_not_entry_bar(self):
        cfg = PipelineConfig(
            regime_enable_hyperparam_tuning=False,
            regime_min_train_trades=1,
            regime_min_val_trades=1,
        )
        optimizer = RegimeOptimizer(cfg)
        features = pd.DataFrame({"REGIME_LIVE": ["TREND", "RANGE"]})
        trades = [{
            "signal_bar": 0,
            "entry_bar": 1,
            "exit_bar": 2,
            "pnl_dollars": 50.0,
            "pnl_pips": 5.0,
            "direction": "LONG",
            "exit_reason": "test",
        }]

        buckets = optimizer._bucket_trades_by_regime(trades, features)

        self.assertEqual(buckets["TREND"]["total_trades"], 1)
        self.assertEqual(buckets["RANGE"]["total_trades"], 0)

    def test_screening_atr_anchors_are_tried_before_shortlist(self):
        class AnchorSensitiveStrategy:
            name = "AnchorSensitiveStrategy"

            def __init__(self, **params):
                self.params = {"sl_atr_mult": 2.0, "tp_atr_mult": 2.0}
                self.params.update(params)

            def get_params(self):
                return dict(self.params)

            def get_param_grid(self):
                return {
                    "sl_atr_mult": [1.0, 2.0, 3.0],
                    "tp_atr_mult": [1.0, 2.0, 3.0],
                }

            def generate_signals(self, features, symbol):
                return pd.Series([1] * len(features), index=features.index)

        class FakeBacktester:
            def run(self, features, signals, symbol, strategy, **kwargs):
                anchor = float(strategy.get_params()["sl_atr_mult"])
                pf_by_anchor = {1.0: 0.5, 2.0: 0.7, 3.0: 1.2}
                pf = pf_by_anchor[anchor]
                return {
                    "total_trades": 30,
                    "profit_factor": pf,
                    "win_rate": 52.0,
                    "total_return_pct": 8.0 if pf >= 1.0 else -8.0,
                    "max_drawdown_pct": 4.0,
                    "trades": [{"pf": pf}],
                }

        previous = StrategyRegistry._strategies.get("AnchorSensitiveStrategy")
        StrategyRegistry._strategies["AnchorSensitiveStrategy"] = AnchorSensitiveStrategy
        try:
            cfg = PipelineConfig(
                regime_enable_hyperparam_tuning=False,
                screening_atr_anchor_enabled=True,
                screening_atr_anchor_multipliers=[1.0, 2.0, 3.0],
                regime_family_probe_trials=3,
                regime_min_train_trades=1,
                train_min_profit_factor=1.0,
                train_min_return_pct=-100.0,
                train_max_drawdown=100.0,
            )
            optimizer = RegimeOptimizer(cfg)
            optimizer.backtester = FakeBacktester()

            def bucket_by_anchor(trades, features, timeframe=None):
                pf = float(trades[0]["pf"])
                metrics = {
                    "total_trades": 30,
                    "profit_factor": pf,
                    "win_rate": 52.0,
                    "sharpe_approx": pf,
                    "total_return_pct": 8.0 if pf >= 1.0 else -8.0,
                    "max_drawdown_pct": 4.0,
                    "avg_win": 1.0,
                    "avg_loss": -1.0,
                }
                return {regime: dict(metrics) for regime in optimizer.REGIMES}

            optimizer._bucket_trades_by_regime = bucket_by_anchor
            features = pd.DataFrame(
                {
                    "Close": range(80),
                    "REGIME_LIVE": [
                        optimizer.REGIMES[i % len(optimizer.REGIMES)] for i in range(80)
                    ],
                },
                index=pd.date_range("2026-01-01", periods=80, freq="h"),
            )

            candidates = optimizer._collect_candidates(
                "EURUSD",
                "H1",
                features,
                None,
                [AnchorSensitiveStrategy()],
            )
        finally:
            if previous is None:
                StrategyRegistry._strategies.pop("AnchorSensitiveStrategy", None)
            else:
                StrategyRegistry._strategies["AnchorSensitiveStrategy"] = previous

        self.assertEqual(len(candidates), 4)
        self.assertTrue(all(c["screening_atr_anchor_mult"] == 3.0 for c in candidates))
        self.assertTrue(all(c["params"]["sl_atr_mult"] == 3.0 for c in candidates))
        self.assertTrue(all(c["params"]["tp_atr_mult"] == 3.0 for c in candidates))
        self.assertTrue(all(c["local_search_trials_total"] == 12 for c in candidates))
        self.assertTrue(all(c["local_search_cells_total"] == 4 for c in candidates))

    def test_threadspark_evidence_runs_only_when_selector_consumes_it(self):
        def exercise(scoring_model):
            optimizer = RegimeOptimizer(
                PipelineConfig(
                    regime_scoring_model=scoring_model,
                    regime_enable_hyperparam_tuning=False,
                )
            )
            candidate = {
                "strategy_name": "ProbeStrategy",
                "train_regime_metrics": {},
                "val_regime_metrics": {},
            }
            optimizer._collect_family_probe_candidates = (
                lambda **kwargs: ([candidate], [])
            )
            optimizer._apply_training_eligibility_gates = (
                lambda candidates, symbol, timeframe: candidates
            )
            optimizer._attach_frozen_probe_validation = lambda **kwargs: None
            optimizer._attach_local_search_multiplicity = (
                lambda candidates, **kwargs: None
            )
            optimizer._attach_oos_evidence_to_candidates = (
                lambda *args, **kwargs: None
            )
            calls = []
            optimizer._attach_threadspark_selection_evidence_to_candidates = (
                lambda *args, **kwargs: calls.append((args, kwargs))
            )

            result = optimizer._collect_candidates(
                "EURUSD",
                "H1",
                pd.DataFrame({"Close": [1.0]}),
                None,
                [],
            )
            return result, calls

        legacy_result, legacy_calls = exercise("legacy")
        threadspark_result, threadspark_calls = exercise(
            "specialist_spokes_threadspark"
        )

        self.assertEqual(len(legacy_result), 1)
        self.assertEqual(legacy_calls, [])
        self.assertEqual(len(threadspark_result), 1)
        self.assertEqual(len(threadspark_calls), 1)

    def test_screening_shortlist_dedupes_atr_anchor_variants_by_strategy(self):
        cfg = PipelineConfig(regime_min_train_trades=2)
        optimizer = RegimeOptimizer(cfg)
        optimizer._compute_train_regime_score = lambda train, val: float(train["_score"])

        def candidate(name, score):
            return {
                "strategy_name": name,
                "train_regime_metrics": {
                    "TREND": {
                        "total_trades": 10,
                        "_score": score,
                    }
                },
            }

        top = optimizer._identify_top_strategies_per_regime(
            [
                candidate("SameStrategy", 90.0),
                candidate("SameStrategy", 100.0),
                candidate("OtherStrategy", 80.0),
            ],
            top_k=2,
        )

        self.assertEqual(top["TREND"], ["SameStrategy", "OtherStrategy"])

    def test_no_trade_markers_are_not_counted_as_winners(self):
        winner = RegimeConfig(
            strategy_name="SomeStrategy",
            parameters={},
            quality_score=0.9,
            promotion_status="PROMOTED",
            val_metrics={"profit_factor": 2.0, "total_return_pct": 10.0, "max_drawdown_pct": 4.0},
        )
        marker = RegimeConfig(
            strategy_name="NO_TRADE",
            parameters={},
            quality_score=0.0,
        )
        cfg = SymbolConfig(
            symbol="EURUSD",
            regime_configs={"H1": {"TREND": winner, "RANGE": marker}},
        )

        self.assertEqual(cfg.count_regime_winners(), 1)

    def test_regime_grid_fallback_freezes_train_best_before_one_validation(self):
        class FakeStrategy:
            def __init__(self, p=0):
                self.p = p

            def get_params(self):
                return {"p": self.p}

            def generate_signals(self, features, symbol):
                return pd.Series([1] * len(features), index=features.index)

        class FakeRegistry:
            @staticmethod
            def get(name, **params):
                return FakeStrategy(**params)

        class FakeBacktester:
            def __init__(self):
                self.events = []

            def run(self, features, signals, symbol, strategy, warmup_bars=0, **kwargs):
                is_val = bool(features.attrs.get("is_val"))
                self.events.append(("validation" if is_val else "train", strategy.p))
                if strategy.p == 0:
                    score = 10.0 if not is_val else 100.0
                else:
                    score = 100.0 if not is_val else -100.0
                return {
                    "total_trades": 20,
                    "max_drawdown_pct": 2.0,
                    "score": score,
                    "trades": [{"score": score}],
                }

        def bucket_trades(trades, features):
            return {
                "TREND": {
                    "total_trades": 20,
                    "max_drawdown_pct": 2.0,
                    "score": trades[0]["score"],
                }
            }

        train = pd.DataFrame({"Close": range(60), "REGIME_LIVE": ["TREND"] * 60})
        val = pd.DataFrame({"Close": range(60), "REGIME_LIVE": ["TREND"] * 60})
        val.attrs["is_val"] = True

        backtester = FakeBacktester()
        optimizer = OptunaTPEOptimizer(
            OptunaConfig(n_trials=2),
            backtester=backtester,
            scorer=None,
            strategy_registry=FakeRegistry(),
        )
        result = optimizer._fallback_regime_search(
            symbol="EURUSD",
            timeframe="H1",
            strategy_name="Fake",
            param_grid={"p": [0, 1]},
            train_features=train,
            val_features=val,
            regimes=["TREND"],
            bucket_trades_fn=bucket_trades,
            compute_score_fn=lambda train_m, val_m: float(train_m.get("score", -999.0)),
            min_train_trades=10,
            max_drawdown_pct=10.0,
        )

        self.assertEqual(result["TREND"]["params"]["p"], 1)
        self.assertEqual(result["TREND"]["val_regime_metrics"]["TREND"]["score"], -100.0)
        self.assertEqual(backtester.events, [("train", 0), ("train", 1), ("validation", 1)])
        self.assertEqual(result["TREND"]["search_trials"], 2)
        self.assertEqual(result["TREND"]["tuning_validation_evaluations"], 1)

    @unittest.skipIf(TrialState is None, "optuna not available")
    def test_optuna_regime_objective_passes_timeframe_to_backtester(self):
        class FakeStrategy:
            name = "Fake"

            def __init__(self, **params):
                self.params = params

            def get_default_params(self):
                return {"p": 1}

            def get_params(self):
                return dict(self.params)

            def generate_signals(self, features, symbol):
                return pd.Series([1] * len(features), index=features.index)

        class FakeRegistry:
            @staticmethod
            def get(name, **params):
                return FakeStrategy(**params)

        class RecordingBacktester:
            def __init__(self):
                self.calls = []

            def run(self, features, signals, symbol, strategy, **kwargs):
                self.calls.append(dict(kwargs))
                return {
                    "total_trades": 20,
                    "max_drawdown_pct": 2.0,
                    "trades": [{"score": 1.0}],
                }

        def bucket_trades(trades, features, timeframe=None):
            return {
                "TREND": {
                    "total_trades": 20,
                    "max_drawdown_pct": 2.0,
                    "score": 1.0,
                }
            }

        train = pd.DataFrame({"Close": range(80), "REGIME_LIVE": ["TREND"] * 80})
        val = pd.DataFrame({"Close": range(80), "REGIME_LIVE": ["TREND"] * 80})
        backtester = RecordingBacktester()
        optimizer = OptunaTPEOptimizer(
            OptunaConfig(n_trials=1),
            backtester=backtester,
            scorer=None,
            strategy_registry=FakeRegistry(),
        )

        optimizer.optimize_for_regimes(
            symbol="EURUSD",
            timeframe="M15",
            strategy_name="Fake",
            param_grid={"p": [1]},
            train_features=train,
            val_features=val,
            regimes=["TREND"],
            bucket_trades_fn=bucket_trades,
            compute_score_fn=lambda train_m, val_m: 1.0,
            min_train_trades=10,
            max_drawdown_pct=10.0,
            val_warmup_bars=3,
        )

        self.assertGreaterEqual(len(backtester.calls), 2)
        self.assertTrue(all(call.get("timeframe") == "M15" for call in backtester.calls))
        self.assertIn(3, [call.get("warmup_bars", 0) for call in backtester.calls])

    @unittest.skipIf(TrialState is None, "optuna not available")
    def test_optuna_single_strategy_objective_passes_timeframe_to_backtester(self):
        class FakeStrategy:
            name = "Fake"

            def __init__(self, **params):
                self.params = params

            def get_default_params(self):
                return {"p": 1}

            def get_params(self):
                return dict(self.params)

            def generate_signals(self, features, symbol):
                return pd.Series([1] * len(features), index=features.index)

        class FakeRegistry:
            @staticmethod
            def get(name, **params):
                return FakeStrategy(**params)

        class RecordingBacktester:
            def __init__(self):
                self.calls = []

            def run(self, features, signals, symbol, strategy, **kwargs):
                self.calls.append(dict(kwargs))
                return {
                    "total_trades": 20,
                    "max_drawdown_pct": 2.0,
                    "total_return_pct": 3.0,
                    "sharpe_ratio": 1.0,
                }

        train = pd.DataFrame({"Close": range(80)})
        val = pd.DataFrame({"Close": range(80)})
        backtester = RecordingBacktester()
        optimizer = OptunaTPEOptimizer(
            OptunaConfig(n_trials=1),
            backtester=backtester,
            scorer=None,
            strategy_registry=FakeRegistry(),
        )

        optimizer.optimize(
            symbol="EURUSD",
            timeframe="H4",
            strategy_name="Fake",
            param_grid={"p": [1]},
            train_features=train,
            val_features=val,
            scoring_fn=lambda train_m, val_m: 1.0,
            min_trades=10,
            max_drawdown_pct=10.0,
        )

        self.assertGreaterEqual(len(backtester.calls), 2)
        self.assertTrue(all(call.get("timeframe") == "H4" for call in backtester.calls))

    @unittest.skipIf(TrialState is None, "optuna not available")
    def test_single_strategy_trials_are_train_only_then_validate_frozen_best_once(self):
        class Strategy:
            name = "Fake"

            def __init__(self, p=1):
                self.p = int(p)

            def get_params(self):
                return {"p": self.p}

            def generate_signals(self, features, symbol):
                return pd.Series(1, index=features.index, dtype=int)

        class Registry:
            @staticmethod
            def get(name, **params):
                return Strategy(**params)

        class Backtester:
            def __init__(self):
                self.events = []

            def run(self, features, signals, symbol, strategy, **kwargs):
                surface = features.attrs["surface"]
                self.events.append((surface, strategy.p))
                score = float(strategy.p if surface == "train" else -strategy.p)
                return {
                    "total_trades": 20,
                    "max_drawdown_pct": 1.0,
                    "score": score,
                }

        train = pd.DataFrame({"Close": range(80)})
        val = pd.DataFrame({"Close": range(80)})
        train.attrs["surface"] = "train"
        val.attrs["surface"] = "validation"
        backtester = Backtester()
        optimizer = OptunaTPEOptimizer(
            OptunaConfig(n_trials=2, n_startup_trials=2, progressive_rejection=False),
            backtester=backtester,
            scorer=None,
            strategy_registry=Registry(),
        )
        result = optimizer.optimize(
            symbol="EURUSD",
            timeframe="H1",
            strategy_name="Fake",
            param_grid={"p": [0, 1]},
            train_features=train,
            val_features=val,
            scoring_fn=lambda train_m, _val_m: float(train_m["score"]),
            min_trades=1,
            max_drawdown_pct=10.0,
        )

        self.assertEqual(sum(surface == "validation" for surface, _p in backtester.events), 1)
        self.assertEqual(backtester.events[-1][0], "validation")
        self.assertEqual(result.val_metrics["score"], -result.best_params["p"])


if __name__ == "__main__":
    unittest.main()


class TupleCategoricalSuggestTests(unittest.TestCase):
    @unittest.skipIf(TrialState is None, "optuna not available")
    def test_tuple_choices_suggested_without_warning(self):
        import warnings
        import optuna

        optuna.logging.set_verbosity(optuna.logging.WARNING)
        grid = {
            "allowed_regimes": [("BREAKOUT", "TREND"), ("CHOP",), ("RANGE",)],
            "period": [10, 20, 30],
        }
        space = ParameterSpace(grid, {"allowed_regimes": ("CHOP",), "period": 20})

        def objective(trial):
            params = space.suggest(trial)
            # Tuple choice round-trips to the real tuple, not an index/string.
            self.assertIsInstance(params["allowed_regimes"], tuple)
            self.assertIn(params["allowed_regimes"], grid["allowed_regimes"])
            self.assertIn(params["period"], grid["period"])
            return float(params["period"])

        with warnings.catch_warnings():
            warnings.simplefilter("error", UserWarning)  # tuple-choice warning -> error
            study = optuna.create_study(direction="maximize")
            study.optimize(objective, n_trials=12, show_progress_bar=False)

        self.assertEqual(len(study.trials), 12)


if __name__ == "__main__":
    unittest.main()
