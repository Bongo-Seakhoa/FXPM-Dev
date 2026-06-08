import json
import tempfile
import unittest
from pathlib import Path

from pm_main import load_config_json


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

    def test_config_loader_accepts_utf8_bom(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "config.json"
            path.write_text('{"pipeline": {"regime_scoring_model": "legacy"}}', encoding="utf-8-sig")

            data = load_config_json(str(path))

        self.assertEqual(data["pipeline"]["regime_scoring_model"], "legacy")

    def test_tracked_config_json_does_not_contain_live_mt5_credentials(self) -> None:
        data = json.loads((self.repo_root / "config.json").read_text(encoding="utf-8"))
        mt5 = data.get("mt5") or {}

        self.assertIn(mt5.get("login", 0), (0, "", None))
        self.assertEqual(mt5.get("password", ""), "")
        self.assertEqual(mt5.get("server", ""), "")

    def test_tracked_config_pins_validated_legacy_scoring_flags(self) -> None:
        data = json.loads((self.repo_root / "config.json").read_text(encoding="utf-8"))
        pipeline = data["pipeline"]

        self.assertEqual(pipeline["regime_scoring_model"], "legacy")
        self.assertFalse(pipeline["scoring_use_trade_frequency_bonus"])
        self.assertAlmostEqual(pipeline["legacy_confidence_bonus_cap"], 0.08, places=6)
        self.assertTrue(pipeline["walk_forward_audit_enabled"])
        self.assertEqual(pipeline["walk_forward_promotion_policy"], "observe_only")
        self.assertTrue(pipeline["live_require_tuned_regime_params"])
        self.assertEqual(pipeline["production_retrain_mode"], "notify")
        self.assertEqual(pipeline["production_retrain_interval_weeks"], 4)
        self.assertIsNone(pipeline["storage_pause_entries_below_free_gb"])
        self.assertGreater(pipeline["daily_loss_hard_block_pct"], 0.0)
        self.assertGreater(pipeline["session_loss_hard_block_pct"], 0.0)
        self.assertGreater(pipeline["weekly_loss_hard_block_pct"], 0.0)
        self.assertGreater(pipeline["open_drawdown_hard_block_pct"], 0.0)

    def test_universal_config_requires_corrected_promotion_gate(self) -> None:
        data = json.loads((self.repo_root / "config.json").read_text(encoding="utf-8"))
        pipeline = data["pipeline"]

        self.assertEqual(pipeline["regime_scoring_model"], "legacy")
        self.assertFalse(pipeline["scoring_use_trade_frequency_bonus"])
        self.assertEqual(pipeline["backtest_spread_mode"], "conservative_max")
        self.assertEqual(pipeline["production_split_mode"], "bleeding_edge")
        self.assertEqual(pipeline["active_stage2_pct"], 50.0)
        self.assertEqual(pipeline["walk_forward_validation_bars"], 50000)
        self.assertEqual(pipeline["clean_holdout_pct"], 0.0)
        self.assertEqual(pipeline["false_discovery_holdout_policy"], "off")
        self.assertTrue(pipeline["walk_forward_audit_enabled"])
        self.assertTrue(pipeline["production_require_promotion_gate"])
        self.assertEqual(pipeline["production_promotion_gate_mode"], "ledger_status")
        self.assertEqual(pipeline["production_promotion_review_path"], "")
        self.assertEqual(pipeline["production_promotion_cell_safety_path"], "")
        self.assertEqual(pipeline["production_completion_marker_path"], "")
        self.assertEqual(pipeline["walk_forward_promotion_policy"], "observe_only")
        self.assertFalse(pipeline["walk_forward_live_blocking_enabled"])
        self.assertTrue(pipeline["live_require_tuned_regime_params"])
        self.assertEqual(pipeline["production_retrain_mode"], "notify")
        self.assertEqual(pipeline["production_retrain_interval_weeks"], 4)
        self.assertIsNone(pipeline["storage_pause_entries_below_free_gb"])
        self.assertEqual(pipeline["daily_loss_hard_block_pct"], 50.0)
        self.assertEqual(pipeline["session_loss_hard_block_pct"], 40.0)
        self.assertEqual(pipeline["weekly_loss_hard_block_pct"], 60.0)
        self.assertEqual(pipeline["open_drawdown_hard_block_pct"], 50.0)
        self.assertEqual(pipeline["max_total_open_risk_pct"], 50.0)
        self.assertEqual(pipeline["max_new_entries_when_total_open_risk_above_pct"], 40.0)
        self.assertEqual(pipeline["margin_entry_block_level"], 60.0)
        self.assertEqual(pipeline["margin_recovery_start_level"], 55.0)
        self.assertEqual(pipeline["margin_panic_level"], 50.0)
        self.assertEqual(pipeline["margin_reopen_level"], 80.0)
        self.assertEqual(pipeline["max_combined_risk_pct"], 50.0)
        self.assertEqual(pipeline["secondary_trade_max_risk_pct"], 50.0)
        self.assertEqual(pipeline["production_expected_promote_cells"], [])
        self.assertEqual(pipeline["winner_ledger_path"], "pm_configs.json")
        self.assertEqual(len(data["symbols"]), 100)
        self.assertEqual(len(set(data["symbols"])), 100)
        self.assertEqual(pipeline["false_discovery_symbol_count"], 100)
        self.assertEqual(pipeline["optimization_max_workers"], 4)
        self.assertEqual(pipeline["optimization_min_m5_bars"], 300000)
        self.assertEqual(pipeline["historical_fetch_retry_attempts"], 3)
        self.assertTrue(pipeline["historical_fetch_deep_backfill_enabled"])
        self.assertEqual(pipeline["historical_fetch_deep_backfill_years"], 18)
        self.assertEqual(pipeline["production_symbol_pool_target_count"], 100)
        self.assertTrue(pipeline["production_require_full_symbol_ledger"])
        self.assertTrue(pipeline["live_allow_partial_symbol_ledger"])
        self.assertEqual(pipeline["live_min_account_equity"], 20.0)
        self.assertEqual(pipeline["live_min_account_equity_currency"], "USD")
        self.assertEqual(pipeline["risk_per_trade_pct"], 1.5)
        self.assertEqual(data["position"]["risk_per_trade_pct"], 1.5)
        self.assertEqual(data["position"]["max_risk_pct"], 25.0)
        self.assertEqual(data["position"]["risk_tolerance_pct"], 25.0)
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
                self.assertIn(status, {"PROMOTED", "BLOCKED"})
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
                        self.assertIn(cell_status, {"PROMOTED", "BLOCKED"})
                        if cell_status == "PROMOTED":
                            evidence = cfg.get("validation_evidence") or {}
                            self.assertTrue(
                                cfg.get("is_validated")
                                or str(evidence.get("validation_reason", "")).startswith("Validated")
                            )
                            self.assertIn(symbol, regime_params)
                            self.assertIn(timeframe, regime_params[symbol])
                        if cell_status == "BLOCKED":
                            self.assertTrue(cfg.get("promotion_reasons"))


if __name__ == "__main__":
    unittest.main()
