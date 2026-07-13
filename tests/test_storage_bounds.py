"""Bounded-growth guarantees for PM-owned storage journals and ledgers.

Covers the use-and-dump storage contract: the manifest journal rotates at a
size cap under thread/process contention, observe-only journal spam is
deduplicated, the dedupe bucket stays bounded, and canonical signal-ledger
history is never silently deleted. None of these paths may touch trading
evidence or decision inputs.
"""

import json
import os
import shutil
import subprocess
import sys
import threading
import unittest
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import patch

from pm_core import PipelineConfig
from pm_storage import StorageManager


class StorageBoundsTests(unittest.TestCase):
    def setUp(self) -> None:
        self.root = Path(__file__).resolve().parents[1] / ".tmp_storage_bounds_tests"
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
            "storage_observe_only": True,
            "storage_write_protect_minutes": 0,
        }
        payload.update(overrides)
        return PipelineConfig(**payload)

    def _manager(self, **overrides) -> StorageManager:
        return StorageManager(
            self._make_config(**overrides),
            data_dir=self.data_dir,
            output_dir=self.output_dir,
            log_dir=self.log_dir,
        )

    def _touch(self, path: Path, text: str, modified_at: datetime) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text, encoding="utf-8")
        stamp = modified_at.timestamp()
        os.utime(path, (stamp, stamp))

    def _manifest_records(self, manager: StorageManager):
        paths = [manager.manifest_path]
        paths.extend(
            path
            for path in manager.output_dir.glob(f"{manager.manifest_path.name}.*")
            if path.name.rsplit(".", 1)[-1].isdigit()
        )
        records = []
        for path in paths:
            if not path.exists():
                continue
            records.extend(
                json.loads(line)
                for line in path.read_text(encoding="utf-8").splitlines()
                if line.strip()
            )
        return records

    # ------------------------------------------------------------------
    # Manifest rotation
    # ------------------------------------------------------------------

    def test_manifest_rotates_at_size_cap_and_bounds_rotations(self):
        manager = self._manager(
            storage_manifest_max_mb=0.0001,  # ~105 bytes: force rotation
            storage_manifest_keep_rotations=2,
            storage_manifest_dedupe_hours=0.0,
        )
        now = datetime(2026, 7, 12, 12, 0, 0)
        for index in range(6):
            manager._append_manifest(
                trigger="test",
                action="delete_log",
                path=self.log_dir / f"file_{index}.log",
                reason="unit",
                size_bytes=1,
                result="deleted",
                now=now + timedelta(seconds=index),
            )
        manifest = manager.manifest_path
        self.assertTrue(manifest.exists())
        rotation_1 = manifest.with_name(f"{manifest.name}.1")
        rotation_3 = manifest.with_name(f"{manifest.name}.3")
        self.assertTrue(rotation_1.exists())
        self.assertFalse(rotation_3.exists(), "rotations must stay bounded at keep_rotations")
        cap_bytes = int(0.0001 * 1024 * 1024)
        for path in (manifest, rotation_1):
            # Each retained file holds at most one over-cap append beyond the cap.
            self.assertLess(path.stat().st_size, cap_bytes + 400)

    def test_nonpositive_or_nonfinite_manifest_cap_falls_back_to_bounded_default(self):
        for invalid in (0.0, -1.0, float("nan"), float("inf")):
            cfg = self._make_config(storage_manifest_max_mb=invalid)
            self.assertEqual(cfg.storage_manifest_max_mb, 16.0)
        clamped = self._make_config(
            storage_manifest_max_mb=1e300,
            storage_manifest_keep_rotations=10_000,
            storage_manifest_dedupe_hours=1e300,
        )
        self.assertEqual(clamped.storage_manifest_max_mb, 1024.0)
        self.assertEqual(clamped.storage_manifest_keep_rotations, 16)
        self.assertEqual(clamped.storage_manifest_dedupe_hours, 24.0 * 365.0)

    def test_manifest_rotation_prunes_stale_generations_above_keep(self):
        manager = self._manager(
            storage_manifest_max_mb=0.0001,
            storage_manifest_keep_rotations=2,
            storage_manifest_dedupe_hours=0.0,
        )
        manager.manifest_path.parent.mkdir(parents=True, exist_ok=True)
        manager.manifest_path.write_text("x" * 200, encoding="utf-8")
        manager.manifest_path.with_name(f"{manager.manifest_path.name}.3").write_text("old")
        manager.manifest_path.with_name(f"{manager.manifest_path.name}.99").write_text("old")
        manager._append_manifest(
            trigger="test", action="delete_log", path=self.log_dir / "x.log",
            reason="unit", size_bytes=1, result="deleted",
            now=datetime(2026, 7, 12, 12, 0, 0),
        )
        retained = {
            path.name for path in manager.output_dir.glob(f"{manager.manifest_path.name}.*")
            if path.name.rsplit(".", 1)[-1].isdigit()
        }
        self.assertNotIn(f"{manager.manifest_path.name}.3", retained)
        self.assertNotIn(f"{manager.manifest_path.name}.99", retained)

    def test_rotation_failure_does_not_suppress_terminal_record(self):
        manager = self._manager(
            storage_manifest_max_mb=0.0001,
            storage_manifest_keep_rotations=2,
            storage_manifest_dedupe_hours=0.0,
        )
        now = datetime(2026, 7, 12, 12, 0, 0)
        manager._append_manifest(
            trigger="test", action="delete_log", path=self.log_dir / "first.log",
            reason="unit", size_bytes=1, result="deleted", now=now,
        )
        with patch("pm_storage.os.replace", side_effect=OSError("simulated rotation failure")):
            manager._append_manifest(
                trigger="test", action="delete_log", path=self.log_dir / "second.log",
                reason="unit", size_bytes=1, result="deleted", now=now + timedelta(seconds=1),
            )
        results = self._manifest_records(manager)
        self.assertEqual(len(results), 2)
        self.assertEqual({item["path"] for item in results}, {
            str(self.log_dir / "first.log"), str(self.log_dir / "second.log")
        })

    def test_manifest_thread_contention_preserves_every_terminal_record(self):
        managers = [
            self._manager(
                storage_manifest_max_mb=0.01,
                storage_manifest_keep_rotations=16,
                storage_manifest_dedupe_hours=0.0,
            )
            for _ in range(6)
        ]
        barrier = threading.Barrier(len(managers))
        errors = []

        def writer(worker: int) -> None:
            try:
                barrier.wait()
                for index in range(50):
                    managers[worker]._append_manifest(
                        trigger="thread", action="delete_log",
                        path=self.log_dir / f"thread_{worker}_{index}.log",
                        reason="unit", size_bytes=1, result="deleted",
                        now=datetime(2026, 7, 12, 12, 0, 0) + timedelta(microseconds=index),
                    )
            except Exception as exc:  # pragma: no cover - asserted below
                errors.append(exc)

        threads = [threading.Thread(target=writer, args=(worker,)) for worker in range(len(managers))]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=30)
        self.assertFalse(errors)
        self.assertTrue(all(not thread.is_alive() for thread in threads))
        records = self._manifest_records(managers[0])
        self.assertEqual(len(records), 300)
        self.assertEqual(len({item["path"] for item in records}), 300)

    def test_manifest_process_contention_preserves_every_terminal_record(self):
        worker_code = """
import sys
from datetime import datetime, timedelta
from pathlib import Path
from pm_core import PipelineConfig
from pm_storage import StorageManager
root = Path(sys.argv[1])
worker = int(sys.argv[2])
cfg = PipelineConfig(
    data_dir=root / 'data', output_dir=root / 'pm_outputs', log_dir=root / 'logs',
    storage_manifest_max_mb=0.005, storage_manifest_keep_rotations=16,
    storage_manifest_dedupe_hours=0.0,
)
manager = StorageManager(cfg, root / 'data', root / 'pm_outputs', root / 'logs')
for index in range(30):
    manager._append_manifest(
        trigger='process', action='delete_log',
        path=root / 'logs' / f'process_{worker}_{index}.log',
        reason='unit', size_bytes=1, result='deleted',
        now=datetime(2026, 7, 12, 12, 0, 0) + timedelta(microseconds=index),
    )
"""
        processes = [
            subprocess.Popen(
                [sys.executable, "-c", worker_code, str(self.root), str(worker)],
                cwd=str(Path(__file__).resolve().parents[1]),
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
            for worker in range(4)
        ]
        failures = []
        for process in processes:
            stdout, stderr = process.communicate(timeout=45)
            if process.returncode != 0:
                failures.append((process.returncode, stdout, stderr))
        self.assertFalse(failures)
        manager = self._manager(
            storage_manifest_max_mb=0.005,
            storage_manifest_keep_rotations=16,
            storage_manifest_dedupe_hours=0.0,
        )
        records = self._manifest_records(manager)
        self.assertEqual(len(records), 120)
        self.assertEqual(len({item["path"] for item in records}), 120)

    # ------------------------------------------------------------------
    # Observe-only journal dedupe
    # ------------------------------------------------------------------

    def test_repeated_candidate_results_are_deduplicated_within_window(self):
        manager = self._manager(storage_manifest_dedupe_hours=24.0)
        now = datetime(2026, 7, 12, 12, 0, 0)
        target = self.log_dir / "old.log"
        for repeat in range(5):
            manager._append_manifest(
                trigger="sweep",
                action="delete_log",
                path=target,
                reason="log_retention>14d",
                size_bytes=100,
                result="candidate",
                now=now + timedelta(minutes=15 * repeat),
            )
        lines = manager.manifest_path.read_text(encoding="utf-8").strip().splitlines()
        self.assertEqual(len(lines), 1, "identical candidate spam must journal once per window")

        # Outside the window the same state is journaled again.
        manager._append_manifest(
            trigger="sweep",
            action="delete_log",
            path=target,
            reason="log_retention>14d",
            size_bytes=100,
            result="candidate",
            now=now + timedelta(hours=25),
        )
        lines = manager.manifest_path.read_text(encoding="utf-8").strip().splitlines()
        self.assertEqual(len(lines), 2)

    def test_changed_state_and_terminal_results_always_journal(self):
        manager = self._manager(storage_manifest_dedupe_hours=24.0)
        now = datetime(2026, 7, 12, 12, 0, 0)
        target = self.log_dir / "old.log"
        manager._append_manifest(
            trigger="sweep", action="delete_log", path=target, reason="r",
            size_bytes=100, result="candidate", now=now,
        )
        # Size change re-journals despite the window.
        manager._append_manifest(
            trigger="sweep", action="delete_log", path=target, reason="r",
            size_bytes=250, result="candidate", now=now + timedelta(minutes=1),
        )
        # Terminal results bypass dedupe entirely, even repeated.
        for repeat in range(2):
            manager._append_manifest(
                trigger="sweep", action="delete_log", path=target, reason="r",
                size_bytes=250, result="deleted", now=now + timedelta(minutes=2 + repeat),
            )
        lines = manager.manifest_path.read_text(encoding="utf-8").strip().splitlines()
        self.assertEqual(len(lines), 4)
        results = [json.loads(line)["result"] for line in lines]
        self.assertEqual(results, ["candidate", "candidate", "deleted", "deleted"])

    def test_semantically_distinct_candidate_reason_is_not_deduplicated(self):
        manager = self._manager(storage_manifest_dedupe_hours=24.0)
        now = datetime(2026, 7, 12, 12, 0, 0)
        target = self.data_dir / ".cache" / "old.pkl"
        manager._append_manifest(
            trigger="scheduled", action="delete_cache_file", path=target,
            reason="cache_age>7d", size_bytes=100, result="candidate", now=now,
        )
        manager._append_manifest(
            trigger="critical", action="delete_cache_file", path=target,
            reason="cache_quota>1000", size_bytes=100, result="candidate",
            now=now + timedelta(minutes=1),
        )
        records = self._manifest_records(manager)
        self.assertEqual(len(records), 2)
        self.assertEqual({item["reason"] for item in records}, {"cache_age>7d", "cache_quota>1000"})

    def test_dedupe_bucket_stays_bounded(self):
        manager = self._manager(storage_manifest_dedupe_hours=24.0)
        now = datetime(2026, 7, 12, 12, 0, 0)
        for index in range(600):
            manager._should_journal(
                self.log_dir / f"file_{index:04d}.log", "candidate", index, now + timedelta(seconds=index)
            )
        bucket = manager._state.get("manifest_dedupe")
        self.assertLessEqual(len(bucket), 512)
        # Oldest entries were evicted first.
        self.assertNotIn(str(self.log_dir / "file_0000.log"), bucket)
        self.assertIn(str(self.log_dir / "file_0599.log"), bucket)

    # ------------------------------------------------------------------
    # Canonical signal-ledger preservation and append integrity
    # ------------------------------------------------------------------

    def test_housekeeping_never_loses_canonical_ledger_content(self):
        # AMENDED (Claude, ledger-compaction lane): the invariant is content
        # preservation, not raw-file identity. A closed month may be archived
        # to .jsonl.gz only when the archive decompresses byte-identical and
        # every reader consumes it transparently; nothing is ever pruned.
        import gzip

        now = datetime(2026, 7, 12, 12, 0, 0)
        manager = self._manager(storage_observe_only=False)
        stale = self.output_dir / "signal_ledger_202401.jsonl"
        self._touch(
            stale, '{"action": "EXECUTED", "symbol": "EURUSD"}\n', now - timedelta(days=900)
        )
        original_bytes = stale.read_bytes()
        manager._cleanup_pm_owned(now, trigger="test")
        archive = self.output_dir / "signal_ledger_202401.jsonl.gz"
        self.assertTrue(archive.exists(), "closed month must be archived, not deleted")
        self.assertFalse(stale.exists())
        self.assertEqual(
            gzip.decompress(archive.read_bytes()),
            original_bytes,
            "canonical trade history must survive byte-identical",
        )

    def test_compaction_disabled_keeps_raw_months(self):
        now = datetime(2026, 7, 12, 12, 0, 0)
        manager = self._manager(
            storage_observe_only=False,
        )
        manager.config.storage_signal_ledger_compress_after_months = 0
        stale = self.output_dir / "signal_ledger_202401.jsonl"
        self._touch(stale, "{}\n", now - timedelta(days=900))
        summary = manager._cleanup_pm_owned(now, trigger="test")
        self.assertTrue(stale.exists())
        self.assertEqual(summary.get("compressed", 0), 0)

    def test_recent_months_and_malformed_names_stay_raw(self):
        now = datetime(2026, 7, 12, 12, 0, 0)
        manager = self._manager(storage_observe_only=False)
        current = self.output_dir / "signal_ledger_202607.jsonl"
        previous = self.output_dir / "signal_ledger_202606.jsonl"
        malformed = self.output_dir / "signal_ledger_notamonth.jsonl"
        for path in (current, previous, malformed):
            self._touch(path, "{}\n", now - timedelta(days=900))
        manager._cleanup_pm_owned(now, trigger="test")
        for path in (current, previous, malformed):
            self.assertTrue(path.exists(), f"{path.name} must stay raw")
            self.assertFalse(path.with_name(f"{path.name}.gz").exists())

    def test_observe_only_journals_candidate_without_archiving(self):
        now = datetime(2026, 7, 12, 12, 0, 0)
        manager = self._manager(storage_observe_only=True)
        stale = self.output_dir / "signal_ledger_202401.jsonl"
        self._touch(stale, "{}\n", now - timedelta(days=900))
        manager._cleanup_pm_owned(now, trigger="test")
        self.assertTrue(stale.exists())
        self.assertFalse((self.output_dir / "signal_ledger_202401.jsonl.gz").exists())

    def test_failed_roundtrip_preserves_raw_month(self):
        now = datetime(2026, 7, 12, 12, 0, 0)
        manager = self._manager(storage_observe_only=False)
        stale = self.output_dir / "signal_ledger_202401.jsonl"
        self._touch(stale, '{"action": "EXECUTED"}\n', now - timedelta(days=900))
        with patch("pm_storage.os.replace", side_effect=OSError("simulated archive failure")):
            summary = manager._cleanup_pm_owned(now, trigger="test")
        self.assertTrue(stale.exists(), "raw month must survive a failed archive")
        self.assertFalse((self.output_dir / "signal_ledger_202401.jsonl.gz").exists())
        self.assertEqual(summary.get("compressed", 0), 0)
        results = [item["result"] for item in self._manifest_records(manager)]
        self.assertTrue(any(str(item).startswith("failed:") for item in results))

    def test_late_append_after_archive_merges_without_loss(self):
        import gzip

        now = datetime(2026, 7, 12, 12, 0, 0)
        manager = self._manager(storage_observe_only=False)
        stale = self.output_dir / "signal_ledger_202401.jsonl"
        self._touch(
            stale, '{"action": "EXECUTED", "index": 1}\n', now - timedelta(days=900)
        )
        first_bytes = stale.read_bytes()
        manager._cleanup_pm_owned(now, trigger="test")
        # A late append recreates the raw file for the archived month.
        manager.signal_ledger.append(
            {"action": "EXECUTED", "index": 2}, when=datetime(2024, 1, 15)
        )
        self.assertTrue(stale.exists())
        stale_stamp = (now - timedelta(days=900)).timestamp()
        os.utime(stale, (stale_stamp, stale_stamp))
        manager._cleanup_pm_owned(now, trigger="test")
        archive = self.output_dir / "signal_ledger_202401.jsonl.gz"
        merged_bytes = gzip.decompress(archive.read_bytes())
        merged = merged_bytes.decode("utf-8")
        self.assertFalse(stale.exists())
        self.assertIn('"index": 1', merged)
        self.assertIn('"index": 2', merged)
        self.assertTrue(
            merged_bytes.startswith(first_bytes), "archived records must precede late ones"
        )

    def test_dashboard_reads_archived_month_identically(self):
        from pm_dashboard.ledger import load_records_from_text
        from pm_dashboard.utils import safe_read_text

        now = datetime(2026, 7, 12, 12, 0, 0)
        manager = self._manager(storage_observe_only=False)
        stale = self.output_dir / "signal_ledger_202401.jsonl"
        lines = "".join(
            json.dumps({"action": "EXECUTED", "symbol": "EURUSD", "index": index}) + "\n"
            for index in range(25)
        )
        self._touch(stale, lines, now - timedelta(days=900))
        raw_records = load_records_from_text(str(stale), safe_read_text(str(stale)))
        manager._cleanup_pm_owned(now, trigger="test")
        archive = self.output_dir / "signal_ledger_202401.jsonl.gz"
        archived_text = safe_read_text(str(archive))
        self.assertEqual(archived_text, lines)
        archived_records = load_records_from_text(str(archive), archived_text)
        self.assertEqual(archived_records, raw_records)

    def test_crash_between_publish_and_unlink_is_idempotent_on_retry(self):
        # Codex review cbfca893: a crash after os.replace(gz) but before
        # raw unlink must NOT concatenate the same records again on retry.
        import gzip

        now = datetime(2026, 7, 12, 12, 0, 0)
        manager = self._manager(storage_observe_only=False)
        month_file = self.output_dir / "signal_ledger_202401.jsonl"
        self._touch(month_file, '{"action": "EXECUTED", "index": 1}\n', now - timedelta(days=900))
        raw_bytes = month_file.read_bytes()

        real_unlink = os.unlink
        def crash_on_raw_unlink(target, *args, **kwargs):
            if str(target) == str(month_file):
                raise OSError("simulated crash before raw unlink")
            return real_unlink(target, *args, **kwargs)

        with patch("pm_storage.Path.unlink", autospec=True) as mocked:
            def side_effect(self_path, *a, **k):
                if str(self_path) == str(month_file):
                    raise OSError("simulated crash before raw unlink")
                return real_unlink(str(self_path))
            mocked.side_effect = side_effect
            with self.assertRaises(OSError):
                manager._compress_ledger_month(month_file)

        archive = self.output_dir / "signal_ledger_202401.jsonl.gz"
        txn = self.output_dir / "signal_ledger_202401.jsonl.gz.txn"
        self.assertTrue(archive.exists(), "archive was published before the crash")
        self.assertTrue(month_file.exists(), "raw stranded by the crash")
        self.assertTrue(txn.exists(), "transaction marker must survive the crash")

        # Retry must complete the transaction without duplicating records.
        manager._compress_ledger_month(month_file)
        self.assertFalse(month_file.exists())
        self.assertFalse(txn.exists())
        self.assertEqual(
            gzip.decompress(archive.read_bytes()),
            raw_bytes,
            "records must appear exactly once after crash + retry",
        )

    def test_crash_then_late_append_merges_only_the_remainder(self):
        import gzip

        now = datetime(2026, 7, 12, 12, 0, 0)
        manager = self._manager(storage_observe_only=False)
        month_file = self.output_dir / "signal_ledger_202401.jsonl"
        self._touch(month_file, '{"action": "EXECUTED", "index": 1}\n', now - timedelta(days=900))
        original_bytes = month_file.read_bytes()

        real_unlink = os.unlink
        with patch("pm_storage.Path.unlink", autospec=True) as mocked:
            def side_effect(self_path, *a, **k):
                if str(self_path) == str(month_file):
                    raise OSError("simulated crash before raw unlink")
                return real_unlink(str(self_path))
            mocked.side_effect = side_effect
            with self.assertRaises(OSError):
                manager._compress_ledger_month(month_file)

        # A late append lands on the stranded raw file before the retry.
        manager.signal_ledger.append(
            {"action": "EXECUTED", "index": 2}, when=datetime(2024, 1, 25)
        )
        late_bytes = month_file.read_bytes()[len(original_bytes):]
        self.assertTrue(late_bytes, "late append must extend the stranded raw")

        manager._compress_ledger_month(month_file)
        archive = self.output_dir / "signal_ledger_202401.jsonl.gz"
        merged = gzip.decompress(archive.read_bytes())
        self.assertFalse(month_file.exists())
        self.assertEqual(
            merged,
            original_bytes + late_bytes,
            "original records exactly once, late records appended after",
        )

    def test_recovery_rejects_foreign_schema_or_wrong_month_marker(self):
        import gzip

        now = datetime(2026, 7, 12, 12, 0, 0)
        manager = self._manager(storage_observe_only=False)
        month_file = self.output_dir / "signal_ledger_202401.jsonl"
        line = '{"action": "EXECUTED", "index": 1}\n'
        self._touch(month_file, line, now - timedelta(days=900))
        raw_bytes = month_file.read_bytes()
        # Fabricate an archive already containing the raw payload plus a
        # marker whose hashes match but whose identity fields are foreign.
        archive = self.output_dir / "signal_ledger_202401.jsonl.gz"
        archive.write_bytes(gzip.compress(raw_bytes))
        import hashlib as _hashlib
        for bad_identity in (
            {"schema": "some-other-schema", "month_file": month_file.name},
            {"schema": "fxpm-ledger-compaction-txn-v1", "month_file": "signal_ledger_209912.jsonl"},
        ):
            marker = {
                **bad_identity,
                "prior_archive_sha256": _hashlib.sha256(b"").hexdigest(),
                "raw_sha256": _hashlib.sha256(raw_bytes).hexdigest(),
                "raw_length": len(raw_bytes),
                "combined_sha256": _hashlib.sha256(raw_bytes).hexdigest(),
            }
            txn = self.output_dir / "signal_ledger_202401.jsonl.gz.txn"
            txn.write_text(json.dumps(marker), encoding="utf-8")
            manager._compress_ledger_month(month_file)
            # Foreign markers must NOT shortcut recovery: the merge runs from
            # current state, so the archive ends with raw appended once more
            # (archive already contained it), never a hash-matched skip that
            # trusts a marker from another schema/month.
            merged = gzip.decompress(archive.read_bytes())
            self.assertTrue(merged.endswith(raw_bytes))
            self.assertFalse(month_file.exists())
            # Reset for the second identity case.
            self._touch(month_file, line, now - timedelta(days=900))
            raw_bytes = month_file.read_bytes()
            archive.write_bytes(gzip.compress(raw_bytes))

    def test_stranded_raw_plus_archive_window_does_not_double_count_in_dashboard(self):
        import gzip

        from pm_dashboard.analytics import load_trade_history

        now = datetime(2026, 7, 12, 12, 0, 0)
        record = {
            "action": "EXECUTED",
            "symbol": "EURUSD",
            "timeframe": "H1",
            "regime": "TREND",
            "strategy": "TestStrategy",
            "direction": "BUY",
            "entry_price": 1.1,
            "stop_loss_price": 1.09,
            "take_profit_price": 1.12,
            "timestamp": "2024-01-10T10:00:00",
            "status": "CLOSED",
            "pnl": 25.0,
        }
        line = json.dumps(record) + "\n"
        month_file = self.output_dir / "signal_ledger_202401.jsonl"
        archive = self.output_dir / "signal_ledger_202401.jsonl.gz"
        # Crash-window state: the archive already holds the record AND the
        # stranded raw file still exists with the identical record.
        self._touch(month_file, line, now - timedelta(days=900))
        archive.write_bytes(gzip.compress(month_file.read_bytes()))
        trades = load_trade_history(str(self.output_dir))
        matching = [
            trade for trade in trades
            if trade.get("symbol") == "EURUSD" and trade.get("pnl") == 25.0
        ]
        self.assertEqual(
            len(matching), 1,
            "identity dedupe must collapse the stranded raw+archive window to one trade",
        )

    def test_concurrent_append_and_compaction_lose_no_records(self):
        import gzip

        now = datetime(2026, 7, 12, 12, 0, 0)
        manager = self._manager(storage_observe_only=False)
        month_file = self.output_dir / "signal_ledger_202401.jsonl"
        self._touch(
            month_file,
            '{"action": "EXECUTED", "index": -1}\n',
            now - timedelta(days=900),
        )
        barrier = threading.Barrier(2)
        errors = []

        def appender():
            barrier.wait(timeout=10)
            try:
                for index in range(40):
                    manager.signal_ledger.append(
                        {"action": "EXECUTED", "index": index},
                        when=datetime(2024, 1, 20),
                    )
            except Exception as exc:  # pragma: no cover - failure surface
                errors.append(exc)

        def compactor():
            barrier.wait(timeout=10)
            try:
                manager._compress_ledger_month(month_file)
            except Exception as exc:  # pragma: no cover - failure surface
                errors.append(exc)

        threads = [threading.Thread(target=appender), threading.Thread(target=compactor)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=30)
        self.assertTrue(all(not thread.is_alive() for thread in threads))
        self.assertEqual(errors, [])
        combined = ""
        archive = self.output_dir / "signal_ledger_202401.jsonl.gz"
        if archive.exists():
            combined += gzip.decompress(archive.read_bytes()).decode("utf-8")
        if month_file.exists():
            combined += month_file.read_text(encoding="utf-8")
        indexes = [
            json.loads(line)["index"]
            for line in combined.splitlines()
            if line.strip()
        ]
        self.assertEqual(sorted(indexes), [-1] + list(range(40)))

    def test_signal_ledger_cross_instance_threads_preserve_all_records(self):
        managers = [self._manager() for _ in range(6)]
        barrier = threading.Barrier(len(managers))

        def writer(worker: int) -> None:
            barrier.wait()
            for index in range(50):
                managers[worker].record_actionable(
                    {"symbol": f"S{worker}", "action": "EXECUTED", "index": index},
                    now=datetime(2026, 7, 12, 12, 0, 0),
                )

        threads = [threading.Thread(target=writer, args=(worker,)) for worker in range(len(managers))]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=30)
        self.assertTrue(all(not thread.is_alive() for thread in threads))
        ledger = self.output_dir / "signal_ledger_202607.jsonl"
        records = [json.loads(line) for line in ledger.read_text(encoding="utf-8").splitlines()]
        self.assertEqual(len(records), 300)
        self.assertEqual(len({(item["symbol"], item["index"]) for item in records}), 300)


if __name__ == "__main__":
    unittest.main()
