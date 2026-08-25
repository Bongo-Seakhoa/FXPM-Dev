import json
import os
import tempfile
import unittest
from pathlib import Path

from pm_core import RETIRED_SCORING_CONFIG_KEYS
from pm_main import load_config_json
from pm_position import PositionConfig
from pm_production_config_binding import load_effective_pipeline_config
from pm_regime import has_tuned_regime_params


class ConfigJsonIntegrityTests(unittest.TestCase):
    repo_root = Path(__file__).resolve().parents[1]

    def _assert_no_duplicate_object_keys(self, config_path: Path) -> None:
        duplicates = []

        def _hook(pairs):
            seen = {}
            for key, value in pairs:
                if key in seen:
                    duplicates.append(key)
                seen[key] = value
            return seen

        json.loads(config_path.read_text(encoding="utf-8"), object_pairs_hook=_hook)
        self.assertEqual(duplicates, [], str(config_path))

    def test_config_json_has_no_duplicate_object_keys(self) -> None:
        self._assert_no_duplicate_object_keys(self.repo_root / "config.json")

    def test_root_runtime_config_and_ledger_are_canonical(self) -> None:
        config_files = sorted(path.name for path in self.repo_root.glob("config*.json"))
        ledger_files = sorted(path.name for path in self.repo_root.glob("pm_configs*.json"))

        self.assertEqual(config_files, ["config.json"])
        self.assertEqual(ledger_files, ["pm_configs.json"])

    def test_runtime_config_and_ledger_use_production_cache_namespace(self) -> None:
        config_text = (self.repo_root / "config.json").read_text(encoding="utf-8")
        ledger_text = (self.repo_root / "pm_configs.json").read_text(encoding="utf-8")
        data = json.loads(config_text)
        stale_namespace = "discovery" + "_honest"

        self.assertNotIn(stale_namespace, config_text)
        self.assertNotIn(stale_namespace, ledger_text)
        self.assertEqual(
            data["pipeline"]["feature_disk_cache_dir"],
            "./pm_outputs/production_live/feature_cache",
        )

    def test_retired_execution_economics_are_absent_from_tracked_configs(self) -> None:
        retired_pipeline_keys = {
            "use_commission",
            "use_slippage",
            "slippage_pips",
        }
        config = json.loads(
            (self.repo_root / "config.json").read_text(encoding="utf-8")
        )
        ledger = json.loads(
            (self.repo_root / "pm_configs.json").read_text(encoding="utf-8")
        )

        def nested_keys(value):
            if isinstance(value, dict):
                for key, item in value.items():
                    yield key
                    yield from nested_keys(item)
            elif isinstance(value, list):
                for item in value:
                    yield from nested_keys(item)

        self.assertTrue(retired_pipeline_keys.issubset(RETIRED_SCORING_CONFIG_KEYS))
        self.assertTrue(retired_pipeline_keys.isdisjoint(config["pipeline"]))
        self.assertTrue(retired_pipeline_keys.isdisjoint(set(nested_keys(ledger))))
        self.assertNotIn("instrument_spec_defaults", config)
        self.assertTrue(
            all(
                "commission_per_lot" not in spec
                for spec in config["instrument_specs"].values()
            )
        )

    def test_config_loader_accepts_utf8_bom(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "config.json"
            path.write_text(
                '{"pipeline": {"initial_capital": 10000.0}}',
                encoding="utf-8-sig",
            )

            data = load_config_json(str(path))

        self.assertEqual(data["pipeline"]["initial_capital"], 10000.0)

    def test_tracked_config_json_does_not_contain_live_mt5_credentials(self) -> None:
        data = json.loads((self.repo_root / "config.json").read_text(encoding="utf-8"))
        mt5 = data.get("mt5") or {}

        self.assertIn(mt5.get("login", 0), (0, "", None))
        self.assertEqual(mt5.get("password", ""), "")
        self.assertEqual(mt5.get("server", ""), "")

    def test_tracked_config_has_one_code_owned_scoring_path(self) -> None:
        data = json.loads((self.repo_root / "config.json").read_text(encoding="utf-8"))
        pipeline = data["pipeline"]
        _, resolved = load_effective_pipeline_config(self.repo_root / "config.json")

        self.assertTrue(RETIRED_SCORING_CONFIG_KEYS.isdisjoint(pipeline))
        self.assertTrue(
            all(not hasattr(resolved, key) for key in RETIRED_SCORING_CONFIG_KEYS)
        )
        self.assertNotIn("scoring_use_trade_frequency_bonus", pipeline)
        self.assertFalse(resolved.scoring_use_trade_frequency_bonus)
        self.assertNotIn("walk_forward_audit_enabled", pipeline)
        self.assertNotIn("walk_forward_promotion_policy", pipeline)
        self.assertNotIn("walk_forward_live_blocking_enabled", pipeline)
        self.assertTrue(resolved.walk_forward_audit_enabled)
        self.assertEqual(resolved.walk_forward_promotion_policy, "promotion_gate")
        self.assertTrue(resolved.walk_forward_live_blocking_enabled)
        self.assertTrue(pipeline["live_require_tuned_regime_params"])
        self.assertEqual(pipeline["production_retrain_mode"], "notify")
        self.assertEqual(pipeline["production_retrain_interval_weeks"], 4)
        self.assertEqual(
            resolved.optimization_max_workers,
            max(1, int(os.cpu_count() or 1)),
        )
        self.assertIsNone(pipeline["storage_pause_entries_below_free_gb"])
        self.assertGreater(pipeline["daily_loss_hard_block_pct"], 0.0)
        self.assertGreater(pipeline["session_loss_hard_block_pct"], 0.0)
        self.assertGreater(pipeline["weekly_loss_hard_block_pct"], 0.0)
        self.assertGreater(pipeline["open_drawdown_hard_block_pct"], 0.0)

    def test_universal_config_requires_institutional_lockbox_and_live_safety_gate(self) -> None:
        data = json.loads((self.repo_root / "config.json").read_text(encoding="utf-8"))
        pipeline = data["pipeline"]
        _, resolved = load_effective_pipeline_config(self.repo_root / "config.json")
        resolved_position = PositionConfig(**data["position"])

        self.assertTrue(RETIRED_SCORING_CONFIG_KEYS.isdisjoint(pipeline))
        self.assertNotIn("scoring_use_trade_frequency_bonus", pipeline)
        self.assertFalse(resolved.scoring_use_trade_frequency_bonus)
        self.assertEqual(pipeline["backtest_spread_mode"], "conservative_max")
        self.assertEqual(pipeline["production_split_mode"], "calendar_lockbox_v2")
        self.assertEqual(pipeline["evidence_purge_days"], 7.0)
        self.assertEqual(pipeline["evidence_warmup_days"], 400.0)
        self.assertEqual(pipeline["evidence_as_of_utc"], "")
        for research_only_key in (
            "historical_stress_audit_bars",
            "active_universe_bars",
            "active_stage2_pct",
            "overlap_pct",
            "walk_forward_validation_bars",
            "clean_holdout_pct",
            "max_bars",
            "research_mode",
            "tuning_validation_bars",
            "final_lockbox_bars",
            "split_embargo_bars",
            "oos_kfold_splits",
            "oos_embargo_bars",
            "oos_aggregate_policy",
            "max_param_combos",
            "min_trades",
            "fx_opt_min_trades",
            "fx_val_min_trades",
            "fx_selection_top_k",
            "fx_opt_top_k",
            "oos_min_trades",
            "promote_small_risk_multiplier",
        ):
            self.assertNotIn(research_only_key, pipeline)
        self.assertEqual(resolved.active_stage2_pct, 50.0)
        self.assertEqual(resolved.walk_forward_validation_bars, 50000)
        self.assertEqual(resolved.tuning_validation_bars, 50000)
        self.assertEqual(resolved.final_lockbox_bars, 50000)
        self.assertEqual(resolved.split_embargo_bars, 300)
        self.assertEqual(resolved.oos_kfold_splits, 5)
        self.assertEqual(resolved.oos_embargo_bars, 1)
        self.assertEqual(
            resolved.oos_aggregate_policy,
            "aggregate_with_worst_fold_floor",
        )
        self.assertEqual(resolved.clean_holdout_pct, 0.0)
        for fixed_true_key in ("use_spread",):
            self.assertNotIn(fixed_true_key, pipeline)
            self.assertTrue(getattr(resolved, fixed_true_key))
        for retired_execution_key in (
            "use_commission",
            "use_slippage",
            "slippage_pips",
        ):
            self.assertNotIn(retired_execution_key, pipeline)
            self.assertFalse(hasattr(resolved, retired_execution_key))
        for fixed_false_key in (
            "local_governance_tournament_enabled",
            "risk_management_optimization_enabled",
            "feature_disk_cache_precompute_enabled",
        ):
            self.assertNotIn(fixed_false_key, pipeline)
            self.assertFalse(getattr(resolved, fixed_false_key))
        self.assertNotIn("local_governance_live_mode", pipeline)
        self.assertEqual(resolved.local_governance_live_mode, "off")
        self.assertNotIn("walk_forward_audit_mode", pipeline)
        self.assertNotIn("walk_forward_do_not_displace_latest_surface", pipeline)
        self.assertEqual(pipeline["false_discovery_holdout_policy"], "block")
        self.assertEqual(pipeline["false_discovery_holdout_min_trades"], 15)
        self.assertEqual(pipeline["false_discovery_holdout_min_profit_factor"], 1.1)
        self.assertEqual(pipeline["false_discovery_holdout_min_return_pct"], 0.0)
        self.assertEqual(pipeline["false_discovery_holdout_max_drawdown_pct"], 20.0)
        self.assertNotIn("walk_forward_audit_enabled", pipeline)
        self.assertTrue(resolved.walk_forward_audit_enabled)
        self.assertTrue(pipeline["production_require_promotion_gate"])
        self.assertEqual(pipeline["production_promotion_gate_mode"], "immutable_exact")
        # No approval is fabricated or checked in: funded entries remain
        # fail-closed until an independently reviewed artifact and its exact
        # whole-file hash are deliberately configured.
        self.assertEqual(pipeline["production_promotion_authorization_path"], "")
        self.assertEqual(pipeline["production_promotion_authorization_sha256"], "")
        self.assertEqual(pipeline["production_promotion_authorization_max_age_days"], 31)
        self.assertEqual(pipeline["live_artifact_drift_policy"], "block")
        self.assertNotIn("production_promotion_review_path", pipeline)
        self.assertNotIn("production_promotion_cell_safety_path", pipeline)
        self.assertNotIn("production_completion_marker_path", pipeline)
        self.assertNotIn("walk_forward_promotion_policy", pipeline)
        self.assertNotIn("walk_forward_live_blocking_enabled", pipeline)
        self.assertEqual(resolved.walk_forward_promotion_policy, "promotion_gate")
        self.assertTrue(resolved.walk_forward_live_blocking_enabled)
        self.assertTrue(pipeline["live_require_tuned_regime_params"])
        self.assertEqual(pipeline["production_retrain_mode"], "notify")
        self.assertEqual(pipeline["production_retrain_interval_weeks"], 4)
        self.assertIsNone(pipeline["storage_pause_entries_below_free_gb"])
        self.assertTrue(pipeline["manual_new_entries_enabled"])
        self.assertEqual(pipeline["live_allowed_promotion_statuses"], ["PROMOTED"])
        self.assertTrue(pipeline["production_replay_audit_enabled"])
        self.assertEqual(pipeline["production_replay_audit_mode"], "apply")
        self.assertTrue(pipeline["production_replay_audit_block_unverified"])
        self.assertEqual(pipeline["daily_loss_hard_block_pct"], 2.0)
        self.assertEqual(pipeline["session_loss_hard_block_pct"], 1.5)
        self.assertEqual(pipeline["weekly_loss_hard_block_pct"], 5.0)
        self.assertEqual(pipeline["open_drawdown_hard_block_pct"], 3.0)
        self.assertEqual(pipeline["max_total_open_risk_pct"], 8.0)
        self.assertEqual(pipeline["production_cap_calibration_path"], "")
        self.assertEqual(pipeline["production_cap_calibration_sha256"], "")
        self.assertEqual(pipeline["production_prelockbox_roster_path"], "")
        self.assertEqual(pipeline["production_prelockbox_roster_sha256"], "")
        self.assertEqual(pipeline["max_factor_group_open_risk_pct"], 2.0)
        self.assertEqual(pipeline["max_new_entries_when_total_open_risk_above_pct"], 0.0)
        self.assertEqual(pipeline["open_risk_action"], "observe")
        self.assertEqual(pipeline["hard_block_reopen_mode"], "when_clear")
        self.assertEqual(pipeline["margin_entry_block_level"], 200.0)
        self.assertEqual(pipeline["margin_recovery_start_level"], 150.0)
        self.assertEqual(pipeline["margin_panic_level"], 120.0)
        self.assertEqual(pipeline["margin_reopen_level"], 300.0)
        self.assertEqual(pipeline["max_combined_risk_pct"], 1.0)
        self.assertEqual(pipeline["secondary_trade_max_risk_pct"], 0.25)
        self.assertTrue(pipeline["allow_d1_plus_lower_tf"])
        self.assertNotIn("production_expected_promote_cells", pipeline)
        self.assertEqual(pipeline["winner_ledger_path"], "pm_configs.json")
        self.assertEqual(len(data["symbols"]), 100)
        self.assertEqual(len(set(data["symbols"])), 100)
        # The runtime derives symbol count from the actual requested universe;
        # the old JSON count and scope-power knob did not affect DSR/PBO gates.
        self.assertNotIn("false_discovery_symbol_count", pipeline)
        self.assertNotIn("false_discovery_scope_power", pipeline)
        # Zero selects all OS logical CPUs at config materialization. Production
        # schedulers add no hidden worker ceiling or RAM admission policy.
        self.assertEqual(pipeline["optimization_max_workers"], 0)
        self.assertNotIn("optimization_min_m5_bars", pipeline)
        self.assertNotIn("historical_fetch_retry_attempts", pipeline)
        self.assertNotIn("historical_fetch_deep_backfill_enabled", pipeline)
        self.assertNotIn("historical_fetch_deep_backfill_years", pipeline)
        self.assertEqual(pipeline["production_symbol_pool_target_count"], 100)
        self.assertTrue(pipeline["production_require_full_symbol_ledger"])
        self.assertFalse(pipeline["live_allow_partial_symbol_ledger"])
        self.assertEqual(pipeline["live_min_account_equity"], 1000.0)
        self.assertEqual(pipeline["live_min_account_equity_currency"], "USD")
        # Live risk has one authority: PositionConfig. Discovery sizing remains
        # a separate, fixed research field in PipelineConfig.
        self.assertNotIn("risk_per_trade_pct", pipeline)
        self.assertEqual(data["position"]["risk_per_trade_pct"], 0.25)
        self.assertEqual(data["position"]["max_risk_pct"], 0.5)
        self.assertEqual(data["position"]["risk_tolerance_pct"], 10.0)
        for fixed_position_key in (
            "auto_widen_sl",
            "allow_min_lot_risk_clamp",
            "regime_tp_multipliers",
        ):
            self.assertNotIn(fixed_position_key, data["position"])
        self.assertFalse(resolved_position.auto_widen_sl)
        self.assertFalse(resolved_position.allow_min_lot_risk_clamp)
        self.assertTrue(
            all(value == 1.0 for value in resolved_position.regime_tp_multipliers.values())
        )
        for symbol in ["AUDUSD", "EURUSD", "USDCAD", "USDJPY"]:
            self.assertIn(symbol, data["symbols"])

    def test_broker_specs_cover_universal_symbol_pool(self) -> None:
        data = json.loads((self.repo_root / "config.json").read_text(encoding="utf-8"))
        specs = json.loads((self.repo_root / "broker_specs.json").read_text(encoding="utf-8"))

        self.assertEqual(len(specs), 100)
        self.assertEqual(set(data["symbols"]), set(specs))

    def test_universal_pm_configs_candidates_are_governed(self) -> None:
        data = json.loads((self.repo_root / "config.json").read_text(encoding="utf-8"))
        configured_symbols = set(data["symbols"])
        ledger = json.loads((self.repo_root / "pm_configs.json").read_text(encoding="utf-8"))
        regime_params = json.loads((self.repo_root / "regime_params.json").read_text(encoding="utf-8"))

        self.assertTrue(set(ledger).issubset(configured_symbols))
        for symbol, payload in ledger.items():
            self.assertEqual(payload.get("symbol"), symbol)
            default_config = payload.get("default_config") or {}
            status = payload.get("promotion_status") or default_config.get("promotion_status")
            if payload.get("strategy_name") != "NO_TRADE":
                self.assertIn(status, {"PROMOTED", "PROMOTE_SMALL", "PAPER_ONLY", "BLOCKED"})
                if status == "PROMOTED":
                    self.assertTrue(payload.get("is_validated") or default_config.get("is_validated"))
                if status == "BLOCKED":
                    self.assertTrue(payload.get("promotion_reasons") or default_config.get("promotion_reasons"))

            for timeframe, regimes in (payload.get("regime_configs") or {}).items():
                self.assertIn(timeframe, data["pipeline"]["timeframes"])
                for regime, cfg in regimes.items():
                    self.assertIn(regime, {"TREND", "RANGE", "BREAKOUT", "CHOP"})
                    if cfg.get("strategy_name") != "NO_TRADE":
                        cell_status = cfg.get("promotion_status")
                        self.assertIn(cell_status, {"PROMOTED", "PROMOTE_SMALL", "PAPER_ONLY", "BLOCKED"})
                        if cell_status == "PROMOTED":
                            evidence = cfg.get("validation_evidence") or {}
                            self.assertTrue(
                                cfg.get("is_validated")
                                or str(evidence.get("validation_reason", "")).startswith("Validated")
                            )
                            # Check exactly what live checks: the regime-param
                            # lookup is case-insensitive, so a promoted mixed-case
                            # broker symbol (e.g. Platinum) resolves against the
                            # materialized row regardless of key casing.
                            self.assertTrue(
                                has_tuned_regime_params(
                                    symbol, timeframe, str(self.repo_root / "regime_params.json")
                                ),
                                f"PROMOTED {symbol}/{timeframe} has no tuned regime params; live would NO_TRADE it",
                            )
                        if cell_status == "BLOCKED":
                            self.assertTrue(cfg.get("promotion_reasons"))

    def test_research_only_defaults_pin_production_breadth(self) -> None:
        """research_only keys resolve from code, so their defaults ARE production.

        Removing a key from config.json hands its value to the PipelineConfig
        default. That is the intended ownership model, but it means a default
        that disagrees with the value config.json used to carry changes
        behaviour silently. max_param_combos did exactly that: config.json
        carried 200, the key moved to code ownership, and search breadth fell
        to the 150 default with no test to notice.
        """
        _, resolved = load_effective_pipeline_config(self.repo_root / "config.json")
        self.assertEqual(
            resolved.max_param_combos,
            200,
            "production search breadth must stay at 200 combinations",
        )

    def test_profitability_gates_sit_above_breakeven(self) -> None:
        """A profit factor below 1.0 loses money.

        The 2026-06-18 audit traced FORD H4 CHOP (PF 0.66, -23.18% over 166
        trades) reaching PROMOTED because the historical stress-audit floor was
        0.60 - beneath breakeven, so incapable of blocking a net loser. Any
        profit-factor gate that can block promotion must sit above 1.0.
        """
        _, resolved = load_effective_pipeline_config(self.repo_root / "config.json")
        for gate in (
            "historical_audit_min_profit_factor",
            "probation_min_profit_factor",
            "production_portfolio_replay_min_profit_factor",
            "regime_min_val_profit_factor",
        ):
            value = float(getattr(resolved, gate))
            self.assertGreaterEqual(
                value,
                1.0,
                f"{gate}={value} sits at or below breakeven; it cannot block an unprofitable strategy",
            )

    def test_historical_audit_sample_and_drawdown_are_meaningful(self) -> None:
        """Guards the other two halves of the same gate."""
        _, resolved = load_effective_pipeline_config(self.repo_root / "config.json")
        self.assertGreaterEqual(
            int(resolved.historical_audit_min_trades),
            20,
            "a handful of trades carries no information about durability",
        )
        self.assertLessEqual(
            float(resolved.historical_audit_max_drawdown),
            25.0,
            "the audit found promoted cells at 40-99% drawdown; the cap must exclude them",
        )


if __name__ == "__main__":
    unittest.main()
