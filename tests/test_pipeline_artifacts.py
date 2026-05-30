import unittest
import json
from datetime import datetime, timedelta
from pathlib import Path
import shutil

import numpy as np

from pm_core import PipelineConfig
from pm_pipeline import (
    ConfigLedger,
    OptimizationPipeline,
    PortfolioManager,
    RegimeConfig,
    SymbolConfig,
    artifact_contract_matches,
    build_artifact_meta,
)


class PipelineArtifactTests(unittest.TestCase):
    def test_ledger_invalidates_when_artifact_fingerprint_changes(self):
        tmp = Path("artifact/fxpm_runtime/.tmp_pytest/test_pipeline_artifacts")
        shutil.rmtree(tmp, ignore_errors=True)
        tmp.mkdir(parents=True, exist_ok=True)
        try:
            cfg = PipelineConfig(
                data_dir=tmp / "data",
                output_dir=tmp / "out",
            )
            artifact_meta = build_artifact_meta(cfg)
            self.assertEqual(artifact_meta["max_bars"], 300000)
            self.assertEqual(artifact_meta["discovery_risk_per_trade_pct"], 1.0)
            self.assertTrue(artifact_meta["risk_management_optimization_enabled"])
            self.assertEqual(artifact_meta["risk_management_selection_stage"], "stage3")
            ledger_path = tmp / "pm_configs.json"
            ledger = ConfigLedger(str(ledger_path))
            ledger.load()

            symbol_cfg = SymbolConfig(
                symbol="EURUSD",
                is_validated=True,
                valid_until=datetime.now() + timedelta(days=7),
                artifact_meta=artifact_meta,
            )
            self.assertTrue(ledger.update_symbol("EURUSD", symbol_cfg))

            should_optimize, _ = ledger.should_optimize("EURUSD", current_artifact_meta=artifact_meta)
            self.assertFalse(should_optimize)

            changed_meta = dict(artifact_meta)
            changed_meta["artifact_version"] = "changed"
            should_optimize, reason = ledger.should_optimize("EURUSD", current_artifact_meta=changed_meta)
            self.assertTrue(should_optimize)
            self.assertIn("artifact fingerprint changed", reason)
        finally:
            shutil.rmtree(tmp, ignore_errors=True)

    def test_artifact_contract_ignores_volatile_ledger_metadata(self):
        cfg = PipelineConfig()
        current = build_artifact_meta(cfg)
        stored = dict(current)
        stored["ledger_status"] = {"complete_universe": False, "missing_symbols": ["GBPUSD"]}
        stored["operator_note"] = "does not participate in invalidation"

        self.assertTrue(artifact_contract_matches(stored, current))

        changed = dict(current)
        changed["artifact_version"] = "changed"
        self.assertFalse(artifact_contract_matches(stored, changed))

    def test_artifact_meta_keeps_live_tp_overlay_out_of_discovery_contract(self):
        cfg = PipelineConfig(regime_tp_multipliers={"TREND": 1.25})
        current = build_artifact_meta(cfg)

        self.assertEqual(current["exit_surface_contract"], "neutral_discovery_sl_tp_v1")
        self.assertNotIn("regime_tp_multipliers", current)

    def test_artifact_meta_includes_execution_cost_contract(self):
        cfg = PipelineConfig(
            backtest_spread_mode="conservative_max",
            use_spread=True,
            use_commission=True,
            use_slippage=True,
            slippage_pips=0.75,
            swap_model_enabled=True,
            swap_model_min_holding_hours=12.0,
            swap_model_triple_rollover_day="Friday",
        )
        current = build_artifact_meta(cfg)

        self.assertEqual(current["backtest_spread_mode"], "conservative_max")
        self.assertTrue(current["use_spread"])
        self.assertTrue(current["use_commission"])
        self.assertTrue(current["use_slippage"])
        self.assertAlmostEqual(current["slippage_pips"], 0.75)
        self.assertTrue(current["swap_model_enabled"])
        self.assertAlmostEqual(current["swap_model_min_holding_hours"], 12.0)
        self.assertEqual(current["swap_model_triple_rollover_day"], "friday")

        legacy_without_cost_contract = {
            k: v for k, v in current.items()
            if k not in {
                "use_spread",
                "backtest_spread_mode",
                "use_commission",
                "use_slippage",
                "slippage_pips",
                "swap_model_enabled",
                "swap_model_min_holding_hours",
                "swap_model_triple_rollover_day",
            }
        }
        changed = dict(current)
        changed["backtest_spread_mode"] = "fixed_spec"

        self.assertFalse(artifact_contract_matches(legacy_without_cost_contract, current))
        self.assertFalse(artifact_contract_matches(current, changed))

    def test_artifact_meta_includes_false_discovery_contract(self):
        cfg = PipelineConfig(
            false_discovery_control_enabled=True,
            false_discovery_min_dsr=0.05,
            false_discovery_confidence_floor=0.4,
            false_discovery_scope_power=0.5,
            false_discovery_symbol_count=61,
            false_discovery_holdout_policy="block",
        )
        current = build_artifact_meta(cfg)

        self.assertTrue(current["false_discovery_control_enabled"])
        self.assertAlmostEqual(current["false_discovery_min_dsr"], 0.05)
        self.assertAlmostEqual(current["false_discovery_confidence_floor"], 0.4)
        self.assertAlmostEqual(current["false_discovery_scope_power"], 0.5)
        self.assertEqual(current["false_discovery_symbol_count"], 61)
        self.assertEqual(current["false_discovery_holdout_policy"], "block")
        self.assertEqual(current["false_discovery_holdout_min_trades"], 5)
        self.assertAlmostEqual(current["false_discovery_holdout_min_profit_factor"], 1.0)
        self.assertAlmostEqual(current["false_discovery_holdout_min_return_pct"], 0.0)
        self.assertFalse(current["oos_evidence_export_enabled"])
        self.assertEqual(current["oos_evidence_periods"], 64)
        self.assertEqual(current["oos_evidence_min_periods"], 8)
        self.assertEqual(current["oos_evidence_dirname"], "oos_evidence")

        changed = dict(current)
        changed["false_discovery_symbol_count"] = 62
        self.assertFalse(artifact_contract_matches(current, changed))

        changed = dict(current)
        changed["oos_evidence_export_enabled"] = True
        self.assertFalse(artifact_contract_matches(current, changed))

    def test_portfolio_manager_sets_false_discovery_symbol_scope(self):
        tmp = Path("artifact/fxpm_runtime/.tmp_pytest/test_false_discovery_scope")
        shutil.rmtree(tmp, ignore_errors=True)
        tmp.mkdir(parents=True, exist_ok=True)
        try:
            cfg = PipelineConfig(
                data_dir=tmp / "data",
                output_dir=tmp / "out",
                false_discovery_control_enabled=True,
            )
            manager = PortfolioManager(
                cfg,
                ["EURUSD", "GBPUSD", "USDJPY"],
                config_file=str(tmp / "pm_configs.json"),
            )

            self.assertEqual(manager.config.false_discovery_symbol_count, 3)
            self.assertEqual(manager.pipeline.config.false_discovery_symbol_count, 3)
        finally:
            shutil.rmtree(tmp, ignore_errors=True)

    def test_portfolio_manager_leaves_disabled_false_discovery_scope_legacy_neutral(self):
        tmp = Path("artifact/fxpm_runtime/.tmp_pytest/test_false_discovery_scope_disabled")
        shutil.rmtree(tmp, ignore_errors=True)
        tmp.mkdir(parents=True, exist_ok=True)
        try:
            cfg = PipelineConfig(data_dir=tmp / "data", output_dir=tmp / "out")
            manager = PortfolioManager(
                cfg,
                ["EURUSD", "GBPUSD", "USDJPY"],
                config_file=str(tmp / "pm_configs.json"),
            )

            self.assertFalse(manager.config.false_discovery_control_enabled)
            self.assertEqual(manager.config.false_discovery_symbol_count, 0)
        finally:
            shutil.rmtree(tmp, ignore_errors=True)

    def test_oos_candidate_evidence_writer_is_separate_from_ledger(self):
        tmp = Path("artifact/fxpm_runtime/.tmp_pytest/test_oos_evidence_writer")
        shutil.rmtree(tmp, ignore_errors=True)
        tmp.mkdir(parents=True, exist_ok=True)
        try:
            cfg = PipelineConfig(
                data_dir=tmp / "data",
                output_dir=tmp / "out",
                oos_evidence_export_enabled=True,
            )
            pipeline = OptimizationPipeline(cfg)
            artifact_meta = build_artifact_meta(cfg)
            pointer = pipeline._write_oos_candidate_evidence(
                "EURUSD",
                [
                    {
                        "format_version": "oos_candidate_returns_v1",
                        "candidate_id": "abc",
                        "symbol": "EURUSD",
                        "timeframe": "H1",
                        "strategy_name": "Demo",
                        "parameters": {},
                        "period_returns": [0.01, -0.02, 0.03, 0.01, 0.0, 0.02, -0.01, 0.01],
                        "period_count": 8,
                    }
                ],
                artifact_meta=artifact_meta,
                data_window_meta={"data_window_fingerprint": "window-hash"},
            )

            path = pointer["path"]
            payload = json.loads(Path(path).read_text(encoding="utf-8"))
            self.assertEqual(payload["format_version"], "oos_candidate_matrix_v1")
            self.assertEqual(payload["record_count"], 1)
            self.assertEqual(pointer["record_count"], payload["record_count"])
            self.assertEqual(payload["records"][0]["candidate_id"], "abc")
            self.assertNotIn("period_returns", payload["records"][0])
            matrix = np.load(payload["matrix_path"])
            self.assertEqual(matrix["returns"].shape, (8, 1))
            self.assertEqual(matrix["candidate_ids"].tolist(), ["abc"])
            self.assertIn("artifact_meta", payload)
            self.assertFalse((tmp / "pm_configs.json").exists())
        finally:
            shutil.rmtree(tmp, ignore_errors=True)

    def test_valid_config_reoptimizes_when_oos_sidecar_is_missing(self):
        tmp = Path("artifact/fxpm_runtime/.tmp_pytest/test_oos_evidence_cache")
        shutil.rmtree(tmp, ignore_errors=True)
        tmp.mkdir(parents=True, exist_ok=True)
        try:
            cfg = PipelineConfig(
                data_dir=tmp / "data",
                output_dir=tmp / "out",
                oos_evidence_export_enabled=True,
            )
            pipeline = OptimizationPipeline(cfg)
            artifact_meta = build_artifact_meta(cfg)
            pointer = pipeline._write_oos_candidate_evidence(
                "EURUSD",
                [
                    {
                        "format_version": "oos_candidate_returns_v1",
                        "candidate_id": "abc",
                        "symbol": "EURUSD",
                        "timeframe": "H1",
                        "strategy_name": "Demo",
                        "parameters": {},
                        "period_returns": [0.01, -0.02, 0.03, 0.01, 0.0, 0.02, -0.01, 0.01],
                        "period_count": 8,
                    }
                ],
                artifact_meta=artifact_meta,
                data_window_meta={},
            )
            symbol_config = SymbolConfig(
                symbol="EURUSD",
                is_validated=True,
                validation_reason="ok",
                data_window_meta={"oos_candidate_evidence": pointer},
                artifact_meta=artifact_meta,
                valid_until=datetime.now() + timedelta(days=1),
            )
            ledger = ConfigLedger(str(tmp / "pm_configs.json"))
            ledger.configs = {"EURUSD": symbol_config}
            ledger._loaded = True

            should_optimize, reason = ledger.should_optimize(
                "EURUSD",
                current_artifact_meta=artifact_meta,
            )
            self.assertFalse(should_optimize, reason)

            Path(pointer["matrix_path"]).unlink()
            should_optimize, reason = ledger.should_optimize(
                "EURUSD",
                current_artifact_meta=artifact_meta,
            )
            self.assertTrue(should_optimize)
            self.assertIn("OOS evidence", reason)
        finally:
            shutil.rmtree(tmp, ignore_errors=True)

    def test_live_eligible_configs_exclude_expired_and_artifact_drift_when_blocking(self):
        tmp = Path("artifact/fxpm_runtime/.tmp_pytest/test_live_eligibility")
        shutil.rmtree(tmp, ignore_errors=True)
        tmp.mkdir(parents=True, exist_ok=True)
        try:
            cfg = PipelineConfig(
                data_dir=tmp / "data",
                output_dir=tmp / "out",
                live_artifact_drift_policy="block",
            )
            artifact_meta = build_artifact_meta(cfg)
            manager = PortfolioManager(cfg, ["EURUSD", "GBPUSD", "USDJPY"], config_file=str(tmp / "pm_configs.json"))

            valid = SymbolConfig(
                symbol="EURUSD",
                strategy_name="TestStrategy",
                is_validated=True,
                valid_until=datetime.now() + timedelta(days=1),
                artifact_meta={**artifact_meta, "ledger_status": {"complete_universe": False}},
            )
            expired = SymbolConfig(
                symbol="GBPUSD",
                strategy_name="TestStrategy",
                is_validated=True,
                valid_until=datetime.now() - timedelta(minutes=1),
                artifact_meta=artifact_meta,
            )
            drifted_meta = dict(artifact_meta)
            drifted_meta["artifact_version"] = "old"
            drifted = SymbolConfig(
                symbol="USDJPY",
                strategy_name="TestStrategy",
                is_validated=True,
                valid_until=datetime.now() + timedelta(days=1),
                artifact_meta=drifted_meta,
            )

            self.assertTrue(manager.ledger.update_symbol("EURUSD", valid))
            self.assertTrue(manager.ledger.update_symbol("GBPUSD", expired))
            self.assertTrue(manager.ledger.update_symbol("USDJPY", drifted))

            raw_validated = manager.get_validated_configs()
            live_eligible = manager.get_live_eligible_configs(current_artifact_meta=artifact_meta)

            self.assertEqual(set(raw_validated), {"EURUSD", "GBPUSD", "USDJPY"})
            self.assertEqual(set(live_eligible), {"EURUSD"})
            self.assertIn("GBPUSD", manager._last_live_eligibility_rejections)
            self.assertIn("USDJPY", manager._last_live_eligibility_rejections)
        finally:
            shutil.rmtree(tmp, ignore_errors=True)

    def test_live_eligible_configs_can_warn_on_artifact_drift(self):
        tmp = Path("artifact/fxpm_runtime/.tmp_pytest/test_live_eligibility_warn")
        shutil.rmtree(tmp, ignore_errors=True)
        tmp.mkdir(parents=True, exist_ok=True)
        try:
            cfg = PipelineConfig(
                data_dir=tmp / "data",
                output_dir=tmp / "out",
                live_artifact_drift_policy="warn",
            )
            artifact_meta = build_artifact_meta(cfg)
            drifted_meta = dict(artifact_meta)
            drifted_meta["artifact_version"] = "old"
            manager = PortfolioManager(cfg, ["EURUSD"], config_file=str(tmp / "pm_configs.json"))
            symbol_cfg = SymbolConfig(
                symbol="EURUSD",
                strategy_name="TestStrategy",
                is_validated=True,
                valid_until=datetime.now() + timedelta(days=1),
                artifact_meta=drifted_meta,
            )
            self.assertTrue(manager.ledger.update_symbol("EURUSD", symbol_cfg))

            live_eligible = manager.get_live_eligible_configs(current_artifact_meta=artifact_meta)

            self.assertEqual(set(live_eligible), {"EURUSD"})
            self.assertEqual(
                manager._last_live_eligibility_warnings,
                {"EURUSD": "artifact fingerprint changed"},
            )
        finally:
            shutil.rmtree(tmp, ignore_errors=True)

    def test_regime_config_preserves_no_trade_marker_metadata_roundtrip(self):
        cfg = RegimeConfig(
            strategy_name="NO_TRADE",
            parameters={},
            quality_score=0.0,
            artifact_meta={"no_trade": True, "reason": "no validated winner"},
        )
        restored = RegimeConfig.from_dict(cfg.to_dict())
        self.assertTrue(restored.is_no_trade_marker())
        self.assertEqual(restored.artifact_meta["reason"], "no validated winner")

    def test_regime_config_persists_false_discovery_evidence(self):
        cfg = RegimeConfig(
            strategy_name="SomeStrategy",
            parameters={},
            quality_score=0.5,
            validation_evidence={
                "false_discovery": {
                    "enabled": True,
                    "deflated_sharpe": 0.42,
                    "effective_trials": 1234,
                    "pbo_status": "not_available",
                }
            },
        )

        restored = RegimeConfig.from_dict(cfg.to_dict())

        evidence = restored.validation_evidence["false_discovery"]
        self.assertTrue(evidence["enabled"])
        self.assertAlmostEqual(evidence["deflated_sharpe"], 0.42)
        self.assertEqual(evidence["effective_trials"], 1234)
        self.assertEqual(evidence["pbo_status"], "not_available")

    def test_symbol_config_persists_validation_evidence_and_robustness(self):
        cfg = SymbolConfig(
            symbol="EURUSD",
            robustness_ratio=0.87,
            validation_evidence={"robustness_ratio": 0.87, "return_dd_ratio": 2.1},
        )
        restored = SymbolConfig.from_dict(cfg.to_dict())
        self.assertAlmostEqual(restored.robustness_ratio, 0.87)
        self.assertAlmostEqual(restored.validation_evidence["return_dd_ratio"], 2.1)

    def test_ledger_stats_report_artifact_contract_counts(self):
        tmp = Path("artifact/fxpm_runtime/.tmp_pytest/test_ledger_contract_counts")
        shutil.rmtree(tmp, ignore_errors=True)
        tmp.mkdir(parents=True, exist_ok=True)
        try:
            ledger = ConfigLedger(str(tmp / "pm_configs.json"))
            ledger.load()
            cfg = SymbolConfig(
                symbol="EURUSD",
                is_validated=True,
                valid_until=datetime.now() + timedelta(days=7),
                artifact_meta={
                    "artifact_version": "a",
                    "split_contract_version": "b",
                },
            )
            self.assertTrue(ledger.update_symbol("EURUSD", cfg))
            stats = ledger.get_stats()
            self.assertEqual(stats["artifact_contract_counts"], {"a|b": 1})
        finally:
            shutil.rmtree(tmp, ignore_errors=True)


if __name__ == "__main__":
    unittest.main()
