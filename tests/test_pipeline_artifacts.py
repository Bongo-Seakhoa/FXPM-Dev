import unittest
from datetime import datetime, timedelta
from pathlib import Path
import shutil

from pm_core import PipelineConfig
from pm_pipeline import ConfigLedger, SymbolConfig, build_artifact_meta


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


if __name__ == "__main__":
    unittest.main()
