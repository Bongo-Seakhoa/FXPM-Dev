import hashlib
import inspect
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
from pm_trade_power import build_numerical_execution_cost_attestation


def _with_authenticated_trade_evidence(
    payload,
    *,
    partition_start="2024-01-01",
):
    result = dict(payload)
    start = pd.Timestamp(partition_start)
    start = start.tz_localize("UTC") if start.tzinfo is None else start.tz_convert("UTC")
    projection_sha256 = hashlib.sha256(start.isoformat().encode("ascii")).hexdigest()
    metadata = dict((payload.get("trades") or [{}])[0])
    trades = []
    for index in range(320):
        entry = start + pd.Timedelta(hours=2 * index)
        trades.append({**metadata,
            "pnl_dollars": 100.0 if index % 2 else -50.0,
            "risk_amount": 100.0,
            "entry_time": entry,
            "exit_time": entry + pd.Timedelta(hours=1),
            "position_id": f"{projection_sha256[:16]}:{index}",
            "entry_event_order": index * 2,
            "exit_event_order": index * 2 + 1,
            "trade_record_schema_version": "2026-07-14-position-events-v2",
        })
    result["total_trades"] = len(trades)
    result["trades"] = trades
    result["position_events"] = trades
    result["execution_cost_timing_contract"] = {
        "swap_accrual": "causal_swap_accrual_v1",
    }
    result["numerical_execution_cost_attestation"] = (
        build_numerical_execution_cost_attestation(
            use_spread=True,
            spread_mode="conservative_max",
            entry_half_spread_values_sha256="7" * 64,
            intrabar_half_spread_values_sha256="7" * 64,
            instrument_spec_sha256="9" * 64,
        )
    )
    result["swap_accrual_contract"] = {
        "contract_version": "causal_swap_accrual_v1",
        "status": "AVAILABLE",
        "provenance_status": "STRESS_APPROVED",
        "execution_state_compounding": True,
        "source_provenance_authenticated": False,
        "historical_rates_authenticated": False,
        "stress_policy_explicitly_approved": True,
        "stress_policy_immutable": True,
        "stress_policy_sha256": "a" * 64,
        "timestamp_source": "_BAR_CLOSE_TS",
        "price_activation_manifest_sha256": "1" * 64,
        "terms_activation_manifest_sha256": "2" * 64,
        "terms_snapshot_sha256": "3" * 64,
        "schedule_sha256": "4" * 64,
        "target_bars_sha256": "5" * 64,
        "target_sidecar_sha256": "6" * 64,
        "projection_contract": "fxpm-swap-preopen-close-projection-v1",
        "projection_sha256": projection_sha256,
    }
    return result


def _power_callbacks():
    evaluator = RegimeOptimizer(PipelineConfig(oos_evidence_export_enabled=False))
    return (
        evaluator._calibrate_train_power_result,
        evaluator._evaluate_partition_power_result,
    )


class OptimizerHardeningTests(unittest.TestCase):
    def test_optuna_config_contains_no_elapsed_ceiling(self):
        parameters = inspect.signature(OptunaConfig).parameters
        self.assertNotIn("timeout_seconds", parameters)
        self.assertFalse(hasattr(OptunaConfig(), "timeout_seconds"))

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
        param_space = ParameterSpace(
            {"a": [1, 2, 3], "b": [10, 20, 30]}, {"a": 2, "b": 20}, coupled_groups=(),
        )

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
        param_space = ParameterSpace(
            {"a": [1, 2, 3], "b": [10, 20, 30]}, {"a": 2, "b": 20}, coupled_groups=(),
        )

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

        def validate(
            _train,
            _val,
            _regime,
            candidate_name,
            timeframe=None,
            **_contract,
        ):
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

    def test_default_origin_uses_object_identity_before_equal_value_fallback(self):
        pipeline = OptimizationPipeline(PipelineConfig())
        first = RegimeConfig(
            strategy_name="SameStrategy",
            parameters={"period": 14},
            quality_score=0.7,
        )
        selected = RegimeConfig(
            strategy_name="SameStrategy",
            parameters={"period": 14},
            quality_score=0.8,
        )

        tf, regime = pipeline._locate_default_origin(
            selected,
            {"M15": {"RANGE": first}, "H1": {"TREND": selected}},
        )

        self.assertEqual((tf, regime), ("H1", "TREND"))

    def test_funded_default_blocks_missing_production_holdout_even_when_false_discovery_disabled(self):
        cfg = PipelineConfig(
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
            "_baseline_gated_signals_sha256": "c" * 64,
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
        first = optimizer._build_oos_evidence_record(
            "EURUSD",
            "H1",
            candidate,
            val_result,
            features,
            val_warmup_bars=1,
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

        self.assertEqual(first["format_version"], "oos_candidate_returns_v5")
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
        self.assertNotIn("execution_cost_survival", first)
        self.assertEqual(first["parameter_stability_evidence"]["status"], "available")
        self.assertAlmostEqual(
            first["parameter_stability_evidence"]["nearest_neighbor_median_score_ratio"],
            0.91,
        )
        self.assertNotIn("execution_cost_survival", second)
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
            "_baseline_gated_signals_sha256": "d" * 64,
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
                "_baseline_gated_signals_sha256": "e" * 64,
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
        self.assertNotIn("execution_cost_survival", record)
        self.assertEqual(record["parameter_stability_evidence"]["status"], "not_available")

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
        self.assertNotIn("selection_execution_cost_survival", candidate)
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
        optimizer._validate_regime_winner = (
            lambda train, val, regime, candidate_name="", **_contract: (
                True,
                "forced pass",
            )
        )

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

            def get_param_couplings(self):
                return ()

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
                return _with_authenticated_trade_evidence(
                    {
                        "total_trades": 30,
                        "profit_factor": pf,
                        "win_rate": 52.0,
                        "total_return_pct": 8.0 if pf >= 1.0 else -8.0,
                        "max_drawdown_pct": 4.0,
                        "trades": [{"pf": pf}],
                    },
                    partition_start=features.index[0],
                )

        previous = StrategyRegistry._strategies.get("AnchorSensitiveStrategy")
        StrategyRegistry._strategies["AnchorSensitiveStrategy"] = AnchorSensitiveStrategy
        try:
            cfg = PipelineConfig(
                regime_enable_hyperparam_tuning=False,
                oos_evidence_export_enabled=False,
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
                index=pd.date_range(
                    "2026-01-01", periods=80, freq="h", tz="UTC"
                ),
            )
            validation = features.copy()
            validation.index = validation.index + pd.Timedelta(days=365)

            candidates = optimizer._collect_candidates(
                "EURUSD",
                "H1",
                features,
                validation,
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

            def get_param_couplings(self):
                return ()

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
                return _with_authenticated_trade_evidence(
                    {
                        "total_trades": 20,
                        "max_drawdown_pct": 2.0,
                        "score": score,
                        "trades": [{"score": score}],
                    },
                    partition_start=features.index[0],
                )

        def bucket_trades(trades, features):
            return {
                "TREND": {
                    "total_trades": 20,
                    "max_drawdown_pct": 2.0,
                    "score": trades[0]["score"],
                }
            }

        train = pd.DataFrame(
            {"Close": range(60), "REGIME_LIVE": ["TREND"] * 60},
            index=pd.date_range("2024-01-01", periods=60, freq="h", tz="UTC"),
        )
        val = pd.DataFrame(
            {"Close": range(60), "REGIME_LIVE": ["TREND"] * 60},
            index=pd.date_range("2025-01-01", periods=60, freq="h", tz="UTC"),
        )
        val.attrs["is_val"] = True

        backtester = FakeBacktester()
        optimizer = OptunaTPEOptimizer(
            OptunaConfig(n_trials=2),
            backtester=backtester,
            scorer=None,
            strategy_registry=FakeRegistry(),
        )
        calibrate_power, validate_power = _power_callbacks()
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
            calibrate_train_power_fn=calibrate_power,
            evaluate_validation_power_fn=validate_power,
            train_economic_gate_fn=lambda _metrics: None,
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
                self.params = {"p": 1, **params}

            def get_default_params(self):
                return {"p": 1}

            def get_params(self):
                return dict(self.params)

            def get_param_couplings(self):
                return ()

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
                return _with_authenticated_trade_evidence(
                    {
                        "total_trades": 20,
                        "max_drawdown_pct": 2.0,
                        "trades": [{"score": 1.0}],
                    },
                    partition_start=features.index[0],
                )

        def bucket_trades(trades, features, timeframe=None):
            return {
                "TREND": {
                    "total_trades": 20,
                    "max_drawdown_pct": 2.0,
                    "score": 1.0,
                }
            }

        train = pd.DataFrame(
            {"Close": range(80), "REGIME_LIVE": ["TREND"] * 80},
            index=pd.date_range("2024-01-01", periods=80, freq="h", tz="UTC"),
        )
        val = pd.DataFrame(
            {"Close": range(80), "REGIME_LIVE": ["TREND"] * 80},
            index=pd.date_range("2025-01-01", periods=80, freq="h", tz="UTC"),
        )
        backtester = RecordingBacktester()
        optimizer = OptunaTPEOptimizer(
            OptunaConfig(n_trials=1),
            backtester=backtester,
            scorer=None,
            strategy_registry=FakeRegistry(),
        )

        calibrate_power, validate_power = _power_callbacks()
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
            calibrate_train_power_fn=calibrate_power,
            evaluate_validation_power_fn=validate_power,
            train_economic_gate_fn=lambda _metrics: None,
        )

        self.assertGreaterEqual(len(backtester.calls), 2)
        self.assertTrue(all(call.get("timeframe") == "M15" for call in backtester.calls))
        self.assertIn(3, [call.get("warmup_bars", 0) for call in backtester.calls])

    @unittest.skipIf(TrialState is None, "optuna not available")
    def test_optuna_single_strategy_objective_passes_timeframe_to_backtester(self):
        class FakeStrategy:
            name = "Fake"

            def __init__(self, **params):
                self.params = {"p": 1, **params}

            def get_default_params(self):
                return {"p": 1}

            def get_params(self):
                return dict(self.params)

            def get_param_couplings(self):
                return ()

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

            def get_param_couplings(self):
                return ()

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
