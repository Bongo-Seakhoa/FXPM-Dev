import unittest
from datetime import datetime, timedelta
from pathlib import Path
import shutil

from pm_core import PipelineConfig
from pm_pipeline import (
    ConfigLedger,
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
            self.assertEqual(artifact_meta["data_workflow_mode"], "active_recent_m5")
            self.assertEqual(artifact_meta["max_bars"], 300000)
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
