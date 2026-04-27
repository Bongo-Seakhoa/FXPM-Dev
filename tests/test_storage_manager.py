import json
import os
import shutil
import unittest
from datetime import datetime, timedelta
from pathlib import Path

from pm_core import PipelineConfig
from pm_storage import StorageManager


class StorageManagerTests(unittest.TestCase):
    def setUp(self) -> None:
        self.root = Path(__file__).resolve().parents[1] / ".tmp_storage_manager_tests"
        self.root.mkdir(parents=True, exist_ok=True)
        self.data_dir = self.root / "data"
        self.output_dir = self.root / "pm_outputs"
        self.log_dir = self.root / "logs"
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.log_dir.mkdir(parents=True, exist_ok=True)

    def tearDown(self) -> None:
        if self.root.exists():
            shutil.rmtree(self.root, ignore_errors=True)

    def _make_config(self, **overrides):
        payload = {
            "data_dir": self.data_dir,
            "output_dir": self.output_dir,
            "log_dir": self.log_dir,
            "storage_enabled": True,
            "storage_observe_only": False,
            "storage_logs_keep_days": 1,
            "storage_pm_outputs_keep_days": 1,
            "storage_pm_outputs_keep_count": 1,
            "storage_resample_cache_max_age_days": 1,
            "storage_resample_cache_max_gb": 1.0,
            "storage_write_protect_minutes": 5,
        }
        payload.update(overrides)
        return PipelineConfig(**payload)

    def _touch(self, path: Path, text: str, modified_at: datetime) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text, encoding="utf-8")
        stamp = modified_at.timestamp()
        os.utime(path, (stamp, stamp))

    def test_pipeline_config_storage_defaults(self):
        cfg = PipelineConfig(data_dir=self.data_dir, output_dir=self.output_dir, log_dir=self.log_dir)
        self.assertTrue(cfg.storage_enabled)
        self.assertTrue(cfg.storage_observe_only)
        self.assertTrue(cfg.storage_signal_ledger_enabled)
        self.assertEqual(cfg.storage_write_protect_minutes, 5)
        self.assertEqual(cfg.log_dir, self.log_dir)

    def test_pm_owned_cleanup_prunes_old_files_but_keeps_recent(self):
        now = datetime(2026, 4, 2, 12, 0, 0)
        cfg = self._make_config()
        manager = StorageManager(cfg, self.data_dir, self.output_dir, self.log_dir)

        old_log = self.log_dir / "pm_old.log"
        recent_log = self.log_dir / "pm_recent.log"
        old_snapshot = self.output_dir / "trades_20260301_000000.json"
        recent_snapshot = self.output_dir / "trades_20260402_115900.json"
        cache_dir = self.data_dir / ".cache"
        old_cache = cache_dir / "EURUSD_H1.pkl"
        recent_cache = cache_dir / "GBPUSD_H1.pkl"

        self._touch(old_log, "old-log", now - timedelta(days=10))
        self._touch(recent_log, "recent-log", now - timedelta(minutes=2))
        self._touch(old_snapshot, "[]", now - timedelta(days=10))
        self._touch(recent_snapshot, "[]", now - timedelta(minutes=2))
        self._touch(old_cache, "cache-old", now - timedelta(days=10))
        self._touch(recent_cache, "cache-recent", now - timedelta(minutes=2))

        manager.on_shutdown(now=now)

        self.assertFalse(old_log.exists())
        self.assertTrue(recent_log.exists())
        self.assertFalse(old_snapshot.exists())
        self.assertTrue(recent_snapshot.exists())
        self.assertFalse(old_cache.exists())
        self.assertTrue(recent_cache.exists())
        self.assertTrue((self.output_dir / "storage_state.json").exists())
        self.assertTrue((self.output_dir / "storage_manifest.jsonl").exists())
        state = json.loads((self.output_dir / "storage_state.json").read_text(encoding="utf-8"))
        self.assertIn("state_updated_at", state)
        self.assertIn("freshness", state)
        self.assertIn("last_housekeeping_is_fresh", state["freshness"])

    def test_write_protection_window_prevents_recent_deletes(self):
        now = datetime(2026, 4, 2, 12, 0, 0)
        cfg = self._make_config(storage_logs_keep_days=0, storage_pm_outputs_keep_days=0, storage_resample_cache_max_age_days=0)
        manager = StorageManager(cfg, self.data_dir, self.output_dir, self.log_dir)

        protected_log = self.log_dir / "pm_protected.log"
        protected_snapshot = self.output_dir / "trades_20260402_115800.json"
        protected_cache = self.data_dir / ".cache" / "USDJPY_H1.pkl"

        self._touch(protected_log, "log", now - timedelta(minutes=4))
        self._touch(protected_snapshot, "[]", now - timedelta(minutes=4))
        self._touch(protected_cache, "cache", now - timedelta(minutes=4))

        manager.on_shutdown(now=now)

        self.assertTrue(protected_log.exists())
        self.assertTrue(protected_snapshot.exists())
        self.assertTrue(protected_cache.exists())

    def test_signal_ledger_writes_jsonl(self):
        now = datetime(2026, 4, 2, 14, 30, 0)
        cfg = self._make_config()
        manager = StorageManager(cfg, self.data_dir, self.output_dir, self.log_dir)

        manager.record_actionable(
            {
                "symbol": "XAUUSD",
                "action": "EXECUTED",
                "timeframe": "M15",
                "regime": "CHOP",
            },
            now=now,
        )

        ledger_path = self.output_dir / "signal_ledger_202604.jsonl"
        self.assertTrue(ledger_path.exists())
        payload = json.loads(ledger_path.read_text(encoding="utf-8").strip())
        self.assertEqual(payload["symbol"], "XAUUSD")
        self.assertEqual(payload["action"], "EXECUTED")

    def test_metaquotes_review_finds_demo_and_tester_candidates(self):
        now = datetime(2026, 4, 2, 15, 0, 0)
        metaquotes_root = self.root / "MetaQuotes"
        terminal_demo = metaquotes_root / "Terminal" / "HASH1" / "bases" / "FBS-Demo"
        tester_demo = metaquotes_root / "Tester" / "HASH2" / "bases" / "MetaQuotes-Demo"
        terminal_demo.mkdir(parents=True, exist_ok=True)
        tester_demo.mkdir(parents=True, exist_ok=True)
        self._touch(terminal_demo / "demo.dat", "terminal-demo", now - timedelta(days=40))
        self._touch(tester_demo / "demo.dat", "tester-demo", now - timedelta(days=40))

        cfg = self._make_config(
            storage_observe_only=True,
            storage_metaquotes_root=str(metaquotes_root),
            storage_metaquotes_cleanup_enabled=False,
            storage_metaquotes_demo_servers=["FBS-Demo", "MetaQuotes-Demo"],
            storage_metaquotes_stale_tester_days=14,
        )
        manager = StorageManager(cfg, self.data_dir, self.output_dir, self.log_dir)
        manager.on_optimization_complete(now=now)

        state = json.loads((self.output_dir / "storage_state.json").read_text(encoding="utf-8"))
        meta = state["last_housekeeping"]["metaquotes"]
        demo_servers = {item["server"] for item in meta["demo_candidates"]}
        tester_servers = {item["server"] for item in meta["tester_base_candidates"]}
        self.assertIn("FBS-Demo", demo_servers)
        self.assertIn("MetaQuotes-Demo", tester_servers)
        self.assertTrue(terminal_demo.exists())
        self.assertTrue(tester_demo.exists())

    def test_live_cache_cleanup_prunes_inactive_symbols(self):
        now = datetime(2026, 4, 2, 16, 0, 0)
        cfg = self._make_config(storage_live_cache_max_age_days=30)
        manager = StorageManager(cfg, self.data_dir, self.output_dir, self.log_dir)
        manager.set_active_symbols(["GBPUSD"])

        live_dir = self.data_dir / ".live"
        inactive_live = live_dir / "EURUSD_H1.csv"
        active_live = live_dir / "GBPUSD_H1.csv"
        self._touch(inactive_live, "inactive", now - timedelta(days=2))
        self._touch(active_live, "active", now - timedelta(days=2))

        manager.on_shutdown(now=now)

        self.assertFalse(inactive_live.exists())
        self.assertTrue(active_live.exists())


if __name__ == "__main__":
    unittest.main()
