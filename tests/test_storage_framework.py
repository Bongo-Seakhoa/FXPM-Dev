import os
import shutil
import unittest
from datetime import datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

from pm_core import DataLoader, StorageConfig
from pm_storage import SignalLedger, StorageManager


class StorageFrameworkTests(unittest.TestCase):
    def setUp(self) -> None:
        self.scratch_root = Path(__file__).resolve().parents[1] / ".tmp_storage_tests"
        self.scratch_root.mkdir(parents=True, exist_ok=True)

    def tearDown(self) -> None:
        if self.scratch_root.exists():
            shutil.rmtree(self.scratch_root, ignore_errors=True)

    def _scratch(self, name: str) -> Path:
        path = self.scratch_root / name
        if path.exists():
            shutil.rmtree(path, ignore_errors=True)
        path.mkdir(parents=True, exist_ok=True)
        return path

    def _touch_with_age(self, path: Path, age_days: float = 0.0) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("x", encoding="utf-8")
        age_seconds = max(0, int(age_days * 86400))
        past = datetime.now() - timedelta(seconds=age_seconds)
        os.utime(path, (past.timestamp(), past.timestamp()))

    def _touch_dir_with_age(self, path: Path, age_days: float = 0.0) -> None:
        path.mkdir(parents=True, exist_ok=True)
        age_seconds = max(0, int(age_days * 86400))
        past = datetime.now() - timedelta(seconds=age_seconds)
        os.utime(path, (past.timestamp(), past.timestamp()))

    def test_storage_config_normalizes_defaults(self) -> None:
        cfg = StorageConfig(
            warn_free_gb="15.5",
            critical_free_gb="10",
            pause_new_entries_below_free_gb="2.5",
            metaquotes_active_root_allowlist=["  C:\\Temp\\Demo  ", ""],
        )
        self.assertEqual(cfg.write_protection_seconds, 300)
        self.assertAlmostEqual(cfg.warn_free_gb, 15.5)
        self.assertAlmostEqual(cfg.critical_free_gb, 10.0)
        self.assertAlmostEqual(cfg.pause_new_entries_below_free_gb or 0.0, 2.5)
        self.assertEqual(cfg.metaquotes_active_root_allowlist, ["C:\\Temp\\Demo"])
        self.assertTrue(cfg.storage_signal_ledger_enabled)
        self.assertEqual(cfg.storage_live_cache_max_age_days, 7)

    def test_signal_ledger_writer_appends_and_reads_jsonl(self) -> None:
        tmp = self._scratch("ledger")
        ledger = SignalLedger(tmp, enabled=True)
        path = ledger.append({"symbol": "EURUSD", "action": "EXECUTED"})
        ledger.append({"symbol": "XAUUSD", "action": "SKIPPED_RISK_CAP"})

        self.assertIsNotNone(path)
        with open(path, "r", encoding="utf-8") as handle:
            lines = [line.strip() for line in handle if line.strip()]
        self.assertEqual(len(lines), 2)
        first = __import__("json").loads(lines[0])
        second = __import__("json").loads(lines[1])
        self.assertEqual(first["symbol"], "EURUSD")
        self.assertIn("ledger_recorded_at", first)
        self.assertEqual(second["action"], "SKIPPED_RISK_CAP")

    def test_storage_manager_due_time_and_protection(self) -> None:
        root = self._scratch("pm_root")
        data_dir = self._scratch("data_root")
        output_dir = self._scratch("output_root")
        log_dir = self._scratch("log_root")
        cfg = SimpleNamespace(
            storage_enabled=True,
            storage_observe_only=False,
            storage_signal_ledger_enabled=True,
            storage_measure_interval_seconds=300,
            storage_housekeeping_interval_seconds=900,
            storage_metaquotes_review_interval_seconds=21600,
            storage_warn_free_gb=15.0,
            storage_critical_free_gb=10.0,
            storage_pause_entries_below_free_gb=None,
            storage_write_protect_minutes=5,
            storage_metaquotes_stale_tester_days=14,
            storage_metaquotes_cleanup_enabled=False,
            storage_metaquotes_demo_servers=["FBS-Demo", "MetaQuotes-Demo"],
            storage_metaquotes_active_root_allowlist=[],
        )
        manager = StorageManager(cfg, data_dir, output_dir, log_dir)
        self.assertTrue(manager.is_due())

        now = datetime(2026, 4, 2, 12, 0, 0)
        manager.mark_run(now=now)
        self.assertFalse(manager.is_due(now=now))
        self.assertTrue(manager.is_due(now=now + timedelta(seconds=manager.config.storage_measure_interval_seconds + 1)))

        cache_dir = data_dir / ".cache"
        cache_dir.mkdir(parents=True, exist_ok=True)
        protected_file = cache_dir / "protected.txt"
        self._touch_with_age(protected_file, age_days=0)
        record = manager.prune_path(protected_file, reason="test", dry_run=False, min_age_seconds=1)
        self.assertIsNone(record)
        self.assertTrue(protected_file.exists())

    def test_resample_cache_pruning_respects_age_and_recent_window(self) -> None:
        data_dir = self._scratch("data")
        cache_dir = data_dir / ".cache"
        cache_dir.mkdir(parents=True, exist_ok=True)
        loader = DataLoader(data_dir)

        stale_inactive = cache_dir / "EURUSD_H1.pkl"
        recent_inactive = cache_dir / "GBPJPY_M15.pkl"
        fresh_active = cache_dir / "XAUUSD_H1.pkl"
        self._touch_with_age(stale_inactive, age_days=10)
        self._touch_with_age(recent_inactive, age_days=0)
        self._touch_with_age(fresh_active, age_days=1)

        stats = loader.prune_resample_cache(
            max_age_days=7,
            max_total_bytes=None,
            active_symbols=["XAUUSD"],
            active_timeframes=["H1"],
            dry_run=False,
            recent_window_seconds=300,
        )

        self.assertEqual(stats["files_removed"], 1)
        self.assertGreaterEqual(stats["skipped_recent"], 1)
        self.assertTrue(fresh_active.exists())
        self.assertTrue(recent_inactive.exists())
        self.assertFalse(stale_inactive.exists())

    def test_metaquotes_demo_root_pruning_requires_enable_and_allows_cleanup(self) -> None:
        root = self._scratch("pm_root")
        protected_root = self.scratch_root / "FBS-Real"
        demo_root = self.scratch_root / "FBS-Demo"
        self._touch_with_age(protected_root / "keep.txt", age_days=10)
        self._touch_with_age(demo_root / "old.txt", age_days=10)
        self._touch_dir_with_age(protected_root, age_days=10)
        self._touch_dir_with_age(demo_root, age_days=10)

        disabled_cfg = SimpleNamespace(
            storage_enabled=True,
            storage_observe_only=False,
            storage_signal_ledger_enabled=True,
            storage_measure_interval_seconds=300,
            storage_housekeeping_interval_seconds=900,
            storage_metaquotes_review_interval_seconds=21600,
            storage_warn_free_gb=15.0,
            storage_critical_free_gb=10.0,
            storage_pause_entries_below_free_gb=None,
            storage_write_protect_minutes=5,
            storage_metaquotes_stale_tester_days=14,
            storage_metaquotes_cleanup_enabled=False,
            storage_metaquotes_demo_servers=["FBS-Demo", "MetaQuotes-Demo"],
            storage_metaquotes_active_root_allowlist=[str(protected_root)],
        )
        disabled_manager = StorageManager(disabled_cfg, root, self._scratch("output_disabled"), self._scratch("log_disabled"))
        self.assertEqual(disabled_manager.prune_metaquotes_demo_roots([demo_root], dry_run=False, min_age_seconds=1), [])
        self.assertTrue(demo_root.exists())

        enabled_cfg = SimpleNamespace(
            storage_enabled=True,
            storage_observe_only=False,
            storage_signal_ledger_enabled=True,
            storage_measure_interval_seconds=300,
            storage_housekeeping_interval_seconds=900,
            storage_metaquotes_review_interval_seconds=21600,
            storage_warn_free_gb=15.0,
            storage_critical_free_gb=10.0,
            storage_pause_entries_below_free_gb=None,
            storage_write_protect_minutes=5,
            storage_metaquotes_stale_tester_days=14,
            storage_metaquotes_cleanup_enabled=True,
            storage_metaquotes_demo_servers=["FBS-Demo", "MetaQuotes-Demo"],
            storage_metaquotes_active_root_allowlist=[str(protected_root)],
        )
        manager = StorageManager(enabled_cfg, root, self._scratch("output_enabled"), self._scratch("log_enabled"))
        records = manager.prune_metaquotes_demo_roots([demo_root], dry_run=False, min_age_seconds=1)
        self.assertEqual(len(records), 1)
        self.assertFalse(demo_root.exists())

        protected_records = manager.prune_metaquotes_demo_roots([protected_root], dry_run=False, min_age_seconds=1)
        self.assertEqual(protected_records, [])
        self.assertTrue(protected_root.exists())


if __name__ == "__main__":
    unittest.main()
