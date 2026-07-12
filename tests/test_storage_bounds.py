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

    def test_housekeeping_never_deletes_canonical_ledger_months(self):
        now = datetime(2026, 7, 12, 12, 0, 0)
        manager = self._manager(storage_observe_only=False)
        stale = self.output_dir / "signal_ledger_202401.jsonl"
        self._touch(stale, "{}\n", now - timedelta(days=900))
        manager._cleanup_pm_owned(now, trigger="test")
        self.assertTrue(stale.exists(), "canonical trade history must not be pruned")

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
