"""Owned software I/O fixtures; no native, model, or financial assurance.

All future mutations stay under explicit retained FXPM_DERIVED_QA_ROOT. The
caller fence models a software caller's finish claim, not a native seal.
"""
from __future__ import annotations

from dataclasses import asdict
import hashlib
import json
import os
import shutil
from pathlib import Path
import threading
import unittest
from unittest.mock import patch
import uuid
from contextlib import contextmanager

import pm_authority_spool as spool
import pm_authority_spool_quota as quota
import pm_book_multiplicity as book
import pm_end_grid as grid
import pm_native_storage as storage


LABEL = "INVENTED_OWNED_SOFTWARE_IO_NO_NATIVE_VERIFICATION_NO_FINANCIAL_AUTHORITY"


def _sha(value):
    return hashlib.sha256(grid.canonical_bytes(value)).hexdigest()


def _write(path, body):
    encoded = grid.canonical_bytes(body)
    with path.open("xb") as handle:
        handle.write(encoded)
        handle.flush()
        os.fsync(handle.fileno())
    assert path.read_bytes() == encoded
    return hashlib.sha256(encoded).hexdigest()


class CompletedGenerationRetirementTests(unittest.TestCase):
    def setUp(self):
        value = os.environ.get("FXPM_DERIVED_QA_ROOT")
        if not value:
            self.skipTest("explicit retained FXPM_DERIVED_QA_ROOT is required")
        parent = Path(value)
        self.assertTrue(parent.is_absolute() and parent.is_dir())
        spool._reject_reparse_chain(parent)
        self.parent = parent / ("retirement_" + uuid.uuid4().hex)
        self.parent.mkdir(exist_ok=False)
        (self.parent / "NO_NATIVE_VERIFICATION.txt").write_text(LABEL, encoding="ascii")

    def fixture(self, *, owned=True):
        case = self.parent / uuid.uuid4().hex
        case.mkdir()
        root, retained = case / "working", case / "retained"
        retained.mkdir()
        limits = spool.SpoolLimits(max_artifact_bytes=1024**2, max_symbol_bytes=2 * 1024**2,
            max_artifacts_per_symbol=100, max_manifest_bytes=128 * 1024, max_json_bytes=256 * 1024,
            stream_chunk_bytes=64 * 1024, max_generation_bytes=4 * 1024**2, min_free_bytes=1)
        cell = book.compute_cell_id("XAGUSD", "H1", "TREND")
        kwargs = {"owned_workspace": {"cell_id": cell, "cell_contract_sha256": "c" * 64}} if owned else {}
        context = spool.freeze_generation_context(root, generation_id="SOFTWARE_" + uuid.uuid4().hex,
            native_universe_sha256="a" * 64, config_sha256="b" * 64, limits=limits, **kwargs)
        sink = spool.open_symbol_sink(context, "OWNED_SOFTWARE_IO")
        entries = [sink.put_json("software-" + str(index), "software_fixture",
            {"fixture_provenance": LABEL, "value": index}) for index in range(2)]
        sink.seal()
        owner_sha = spool.read_owned_generation_workspace(context)[1] if owned else "0" * 64
        binding = {"generation_context_sha256": context.context_sha256, "run_id": context.generation_id,
            "cell_id": cell, "fixture_provenance": LABEL}
        row = grid.build_end_grid_row(symbol="XAGUSD", timeframe="H1", regime="TREND",
            cycle_start_utc="2024-01-02T00:00:00+00:00", cycle_end_utc="2024-01-03T00:00:00+00:00",
            information_cutoff_utc="2024-01-01T23:00:00+00:00", outcome="STOP", reason_code="SOFTWARE_IO_ONLY",
            source_kind="m5_derived", source_contract_sha256=None, view_key=None,
            source_identity={}, cost_tier="NONE", process_edition_sha256="d" * 64,
            code_sha256={"software-fixture": "e" * 64}, metrics={}, job_binding_sha256=_sha(binding),
            produced_at_utc="2024-01-03T00:00:00+00:00")
        appended = grid.EndGridWriter(retained).append(row)
        fence = retained / "source-fence.json"
        fence_sha = _write(fence, {"schema_version": "fxpm.declared-m5-development-source-fence.v1",
            "workspace_owner_sha256": owner_sha,
            "source_summary": {"binding": binding, "owned_values_released": True, "release_eligible": True,
                "full_native_context_eligibility": False, "financial_consumer_ready": False,
                "fixture_provenance": LABEL},
            "end_grid_rows": {"terminal": {"path": appended["path"], "row_sha256": row.row_sha256}},
            "qualification_authority": False, "capital_authority": False, "live_authority": False, "order_authority": False})
        canonical = case / "canonical-m5"
        canonical.mkdir()
        (canonical / "input.txt").write_text(LABEL, encoding="ascii")
        edition = case / "shared-9fe-edition"
        edition.mkdir()
        (edition / "profile.txt").write_text(LABEL, encoding="ascii")
        config = case / "config.json"
        _write(config, {"fixture_provenance": LABEL})
        args = {"expected_context_sha256": context.context_sha256, "workspace_owner_sha256": owner_sha,
            "source_fence_path": fence, "source_fence_sha256": fence_sha,
            "retirement_receipt_path": retained / "retirement.json",
            "protected_paths": (canonical, edition, config, retained, Path(spool.__file__), Path(quota.__file__)),
            "owned_direct_files": (), "unit_lock_paths": (),
            "max_paths": 100, "max_nodes": 300, "max_bytes": 4 * 1024**2}
        return context, entries, args

    def native_completion_fixture(self, *, mismatched_run=False):
        from datetime import datetime, timedelta, timezone

        context, entries, args = self.fixture()
        retained = args["source_fence_path"].parent
        grid_root = retained / "native-grid"
        grid_root.mkdir()
        cell = book.compute_cell_id("XAGUSD", "H1", "TREND")
        precommit_sha, process_sha, run_sha = "1" * 64, "2" * 64, "3" * 64
        start = datetime(2023, 6, 24, tzinfo=timezone.utc)
        selected_slots = []
        rows = []
        for ordinal in range(13):
            cycle_start = start + timedelta(days=28 * ordinal)
            cycle_end = cycle_start + timedelta(days=28)
            cutoff = cycle_start - timedelta(hours=1)
            slot_id = f"owner-slot-{ordinal + 1:02d}-{cycle_start:%Y%m%dT%H%M%SZ}"
            winner = ordinal == 0
            outcome = "WINNER" if winner else "NO_TRADE"
            reason = "SOFTWARE_WINNER_ONLY" if winner else "SOFTWARE_NO_TRADE_ONLY"
            selected_slots.append({
                "slot_id": slot_id,
                "cycle_start_utc": cycle_start.isoformat(),
                "cycle_end_utc": cycle_end.isoformat(),
                "information_cutoff_utc": cutoff.isoformat(),
                "outcome": outcome,
            })
            view = {
                "schema_version": "fxpm.broker-native-end-grid-view.v1",
                "symbol": "XAGUSD", "timeframe": "H1",
                "cutoff_utc": cutoff.isoformat(),
                "activation_id": "INVENTED_SOFTWARE_ACTIVATION",
                "activation_manifest_sha256": "4" * 64,
                "activation_entry_sha256": "5" * 64,
                "native_bars_sha256": "6" * 64,
                "continuity_policy_sha256": "7" * 64,
                "continuity_geometry_sha256": "8" * 64,
            }
            result = {
                "precommit_sha256": precommit_sha, "cell_id": cell,
                "slot_id": slot_id, "ordinal": ordinal + 1,
                "cycle_start_utc": cycle_start.isoformat(),
                "cycle_end_utc": cycle_end.isoformat(),
                "completion_status": "OWNER" if winner else "NO_TRADE",
                "reason_code": reason, "request_sha256": "9" * 64,
                "slot_commit_sha256": _sha({"fixture": LABEL, "slot": slot_id}),
                "source_session_sha256": "a" * 64,
                "native_prefix_index_sha256": "b" * 64,
                "source_accessed_at_utc": cutoff.isoformat(),
                "saved_cell_run_receipt_sha256": run_sha,
            }
            tier_evidence = {
                "semantics": "VERIFIED_SELECTED_OWNER_TIER", "tier": "P75",
                "selected_tier_contract": {"fixture_provenance": LABEL},
            } if winner else {
                "semantics": "BOUND_BASELINE_DESCRIPTOR_NO_SELECTED_WINNER", "tier": "P75",
                "bound_baseline_descriptor": {"fixture_provenance": LABEL},
            }
            contract = grid.store_view_contract(grid_root, {
                "schema_version": "fxpm.broker-native-end-grid-reconstruction.v1",
                "view": view, "result": result, "cost_tier_evidence": tier_evidence,
            })
            rows.append(grid.build_end_grid_row(
                symbol="XAGUSD", timeframe="H1", regime="TREND",
                cycle_start_utc=cycle_start.isoformat(), cycle_end_utc=cycle_end.isoformat(),
                information_cutoff_utc=cutoff.isoformat(), outcome=outcome,
                reason_code=reason, source_kind="broker_native",
                source_identity={key: view[key] for key in (
                    "activation_id", "activation_manifest_sha256",
                    "activation_entry_sha256", "native_bars_sha256")},
                strategy_family="SOFTWARE_FIXTURE" if winner else None,
                configuration_sha256="c" * 64 if winner else None,
                source_contract_sha256="d" * 64 if winner else None,
                no_trade_run_receipt_sha256=None if winner else (
                    "e" * 64 if mismatched_run and ordinal == 1 else run_sha),
                view_key=_sha(view), view_contract_sha256=contract,
                cost_tier="P75", process_edition_sha256=process_sha,
                code_sha256={"software-fixture": "f" * 64}, metrics={},
                produced_at_utc=cycle_end.isoformat(),
            ))
        appended = grid.EndGridWriter(grid_root).append_many_same_symbol(rows)
        fence = retained / "native-completion-fence.json"
        fence_sha = _write(fence, {
            "schema_version": spool.NATIVE_COMPLETION_FENCE_SCHEMA,
            "workspace_owner_sha256": args["workspace_owner_sha256"],
            "binding": {
                "generation_context_sha256": context.context_sha256,
                "run_id": context.generation_id,
                "cell_id": cell, "cell_contract_sha256": "c" * 64,
                "search_process_receipt_sha256": process_sha,
                "precommit_sha256": precommit_sha,
            },
            "completion": {"cell_run_receipt_sha256": run_sha,
                           "account_artifact_sha256": "0" * 64},
            "selected_slots": selected_slots,
            "end_grid_rows": {slot["slot_id"]: {"path": item["path"],
                                               "row_sha256": item["row_sha256"]}
                              for slot, item in zip(selected_slots, appended, strict=True)},
            "qualification_authority": False, "capital_authority": False,
            "live_authority": False, "order_authority": False,
        })
        args.update(source_fence_path=fence, source_fence_sha256=fence_sha)
        return context, entries, args, grid_root, tuple(rows)

    def test_native_completion_archives_owned_payload_and_retains_exact_full_grid(self):
        context, entries, args, grid_root, rows = self.native_completion_fixture()
        result = spool.retire_completed_generation(context.root, **args)
        self.assertEqual(result["status"], "ARCHIVED_RECORD_ONLY")
        self.assertIs(result["payloads_retained"], False)
        self.assertIs(result["genuine_cold_reopen_available"], False)
        self.assertEqual(grid.read_end_grid(grid_root, "XAGUSD"), rows)
        self.assertEqual(spool.read_generation_retirement(
            context.root, expected_completed_fence_schema=spool.NATIVE_COMPLETION_FENCE_SCHEMA
        ), result)
        for entry in entries:
            self.assertFalse((context.root / entry["descriptor"]["relative_path"]).exists())
        with self.assertRaisesRegex(spool.AuthoritySpoolError, "GENERATION_WORKSPACE_TERMINAL"):
            spool.reopen_generation_context(context.root)

    def test_native_no_trade_receipt_mismatch_refuses_before_owned_payload_release(self):
        context, entries, args, grid_root, rows = self.native_completion_fixture(mismatched_run=True)
        payloads = {context.root / entry["descriptor"]["relative_path"]:
                    (context.root / entry["descriptor"]["relative_path"]).read_bytes()
                    for entry in entries}
        with self.assertRaisesRegex(spool.AuthoritySpoolError, "RETIREMENT_GRID_READBACK_REQUIRED"):
            spool.retire_completed_generation(context.root, **args)
        self.assertFalse(args["retirement_receipt_path"].exists())
        self.assertFalse((context.root / "generation-archived.json").exists())
        self.assertFalse((context.root / "generation-retirement.json").exists())
        self.assertEqual(grid.read_end_grid(grid_root, "XAGUSD"), rows)
        for path, data in payloads.items():
            self.assertEqual(path.read_bytes(), data)

    def test_default_context_schema_and_hash_are_unchanged(self):
        context, _, _ = self.fixture(owned=False)
        encoded = (context.root / spool.CONTEXT_RELATIVE_PATH).read_bytes()
        expected = {"schema_version": spool.GENERATION_CONTEXT_SCHEMA, "generation_id": context.generation_id,
            "native_universe_sha256": context.native_universe_sha256, "config_sha256": context.config_sha256,
            "limits": asdict(context.limits)}
        self.assertEqual(encoded, grid.canonical_bytes(expected))
        self.assertEqual(hashlib.sha256(encoded).hexdigest(), context.context_sha256)
        self.assertFalse((context.root / spool.WORKSPACE_OWNER_RELATIVE_PATH).exists())
        self.assertEqual(spool.reopen_generation_context(context.root), context)

    def test_nonempty_root_cannot_be_retroactively_claimed(self):
        root = self.parent / "foreign-root"
        root.mkdir()
        foreign = root / "other-owner.txt"
        foreign.write_text(LABEL, encoding="ascii")
        with self.assertRaisesRegex(spool.AuthoritySpoolError, "WORKSPACE_ROOT_NOT_EMPTY"):
            spool.freeze_generation_context(root, generation_id="SOFTWARE", native_universe_sha256="a" * 64,
                config_sha256="b" * 64, owned_workspace={"cell_id": "c" * 64, "cell_contract_sha256": "d" * 64})
        self.assertEqual(foreign.read_text(encoding="ascii"), LABEL)
        self.assertFalse((root / spool.CONTEXT_RELATIVE_PATH).exists())

    def test_cloned_and_replaced_roots_cannot_reuse_an_owned_marker(self):
        for scenario in ("clone", "replace"):
            with self.subTest(scenario=scenario):
                context, entries, args = self.fixture()
                original = context.root
                clone = original.with_name("clone")
                self.assertTrue(original.resolve().is_relative_to(self.parent.resolve()))
                self.assertTrue(clone.resolve().is_relative_to(self.parent.resolve()))
                if scenario == "replace":
                    moved = original.with_name("original-moved")
                    self.assertTrue(moved.resolve().is_relative_to(self.parent.resolve()))
                    self.assertFalse(moved.exists())
                    original.rename(moved)
                    shutil.copytree(moved, original)
                    clone = original
                else:
                    shutil.copytree(original, clone)
                with self.assertRaisesRegex(spool.AuthoritySpoolError, "WORKSPACE_ROOT_IDENTITY_CHANGED"):
                    spool.read_owned_generation_workspace(spool.reopen_generation_context(clone))
                with self.assertRaisesRegex(spool.AuthoritySpoolError, "WORKSPACE_ROOT_IDENTITY_CHANGED"):
                    spool.read_generation_retirement(clone)
                with self.assertRaisesRegex(spool.AuthoritySpoolError, "WORKSPACE_ROOT_IDENTITY_CHANGED"):
                    spool.retire_completed_generation(clone, **args)
                self.assertTrue((clone / entries[0]["descriptor"]["relative_path"]).exists())
                self.assertFalse((clone / spool.RETIREMENT_RELATIVE_PATH).exists())

    def test_real_owned_io_retires_without_payload_read_and_is_idempotent(self):
        context, entries, args = self.fixture()
        protected = {path: path.read_bytes() for path in
            (args["source_fence_path"], args["protected_paths"][2], args["protected_paths"][0] / "input.txt",
             args["protected_paths"][1] / "profile.txt")}
        original = spool._stream_file
        def metadata_only(root, relative, **kwargs):
            self.assertFalse(relative.startswith("objects/"), "retirement must not rehash payloads")
            return original(root, relative, **kwargs)
        with patch.object(spool, "_stream_file", side_effect=metadata_only):
            result = spool.retire_completed_generation(context.root, **args)
        self.assertEqual(result["status"], "ARCHIVED_RECORD_ONLY")
        self.assertIs(result["genuine_cold_reopen_available"], False)
        self.assertIs(result["payloads_retained"], False)
        for entry in entries:
            self.assertFalse((context.root / entry["descriptor"]["relative_path"]).exists())
        self.assertTrue((context.root / quota.QUOTA_LOCK_NAME).exists())
        self.assertEqual(spool.retire_completed_generation(context.root, **args), result)
        self.assertEqual(spool.read_generation_retirement(context.root), result)
        for path, data in protected.items():
            self.assertEqual(path.read_bytes(), data)
        with self.assertRaisesRegex(spool.AuthoritySpoolError, "GENERATION_WORKSPACE_TERMINAL"):
            spool.reopen_generation_context(context.root)
        with self.assertRaisesRegex(quota.AuthoritySpoolQuotaError, "SPOOL_GENERATION_TERMINAL"):
            with quota.publication_lease(context.root, max_generation_bytes=context.limits.max_generation_bytes,
                    min_free_bytes=context.limits.min_free_bytes):
                self.fail("a retired generation admitted a writer")

    def test_active_reservation_is_refused_without_reaping(self):
        context, entries, args = self.fixture()
        with quota.publication_lease(context.root, max_generation_bytes=context.limits.max_generation_bytes,
                min_free_bytes=context.limits.min_free_bytes) as lease:
            with self.assertRaisesRegex(quota.AuthoritySpoolQuotaError, "SPOOL_RETIREMENT_RESERVATIONS_PRESENT"):
                spool.retire_completed_generation(context.root, **args)
            self.assertTrue((context.root / entries[0]["descriptor"]["relative_path"]).exists())
            self.assertFalse((context.root / spool.RETIREMENT_RELATIVE_PATH).exists())
            lease.cancel_after_cleanup()

    def test_unknown_guard_is_preserved_and_refused(self):
        context, _, args = self.fixture()
        guard = context.root / (quota.QUOTA_RESERVATION_PREFIX + "f" * 32 + ".lock")
        guard.write_bytes(b"unknown")
        with self.assertRaisesRegex(quota.AuthoritySpoolQuotaError, "SPOOL_RETIREMENT_GUARDS_PRESENT"):
            spool.retire_completed_generation(context.root, **args)
        self.assertEqual(guard.read_bytes(), b"unknown")
        self.assertFalse((context.root / spool.RETIREMENT_RELATIVE_PATH).exists())

    def test_actual_unit_kernel_lock_is_refused_and_its_inode_is_retained(self):
        context, entries, args = self.fixture()
        relative = "software-units/units.lock"
        path = context.root / relative
        path.parent.mkdir()
        path.write_bytes(b"0")
        identity = path.stat().st_dev, path.stat().st_ino
        args["owned_direct_files"] = (relative,)
        args["unit_lock_paths"] = (relative,)
        with path.open("r+b", buffering=0) as handle:
            self.assertTrue(storage._try_kernel_lock(handle))
            try:
                omitted = dict(args, unit_lock_paths=())
                with self.assertRaisesRegex(spool.AuthoritySpoolError, "RETIREMENT_UNIT_LOCK_PATHS_INVALID"):
                    spool.retire_completed_generation(context.root, **omitted)
                self.assertTrue((context.root / entries[0]["descriptor"]["relative_path"]).exists())
                self.assertFalse((context.root / spool.RETIREMENT_RELATIVE_PATH).exists())
                with self.assertRaisesRegex(spool.AuthoritySpoolError, "RETIREMENT_UNIT_WRITER_ACTIVE"):
                    spool.retire_completed_generation(context.root, **args)
            finally:
                storage._release_kernel_lock(handle)
        self.assertTrue((context.root / entries[0]["descriptor"]["relative_path"]).exists())
        self.assertFalse((context.root / spool.RETIREMENT_RELATIVE_PATH).exists())
        result = spool.retire_completed_generation(context.root, **args)
        self.assertEqual(result["retained_unit_locks"], [relative])
        self.assertEqual((path.stat().st_dev, path.stat().st_ino), identity)
        self.assertEqual(path.read_bytes(), b"0")

    def test_terminal_and_admission_use_the_same_real_lock(self):
        context, _, _ = self.fixture()
        attempted, released = threading.Event(), threading.Event()
        errors = []
        original = quota._exclusive_file_lock
        @contextmanager
        def observed_lock(path):
            if threading.current_thread().name == "software-publisher":
                attempted.set()
            with original(path):
                yield
        def publish():
            try:
                with quota.publication_lease(context.root, max_generation_bytes=context.limits.max_generation_bytes,
                        min_free_bytes=context.limits.min_free_bytes):
                    errors.append("ADMITTED")
            except quota.AuthoritySpoolQuotaError as error:
                errors.append(error.code)
            finally:
                released.set()
        with patch.object(quota, "_exclusive_file_lock", side_effect=observed_lock):
            with quota.generation_retirement_lock(context.root, max_root_entries=100):
                worker = threading.Thread(target=publish, name="software-publisher")
                worker.start()
                self.assertTrue(attempted.wait(5))
                self.assertFalse(any(path.name.startswith(quota.QUOTA_RESERVATION_PREFIX) for path in context.root.iterdir()))
                (context.root / quota.GENERATION_TERMINAL_NAME).write_bytes(b"SOFTWARE_TERMINAL")
            self.assertTrue(released.wait(5))
            worker.join(5)
        self.assertFalse(worker.is_alive())
        self.assertEqual(errors, ["SPOOL_GENERATION_TERMINAL"])

    def test_foreign_file_overlap_and_bounds_stop_before_unlink(self):
        for scenario in ("foreign", "overlap", "paths", "nodes", "bytes"):
            with self.subTest(scenario=scenario):
                context, entries, args = self.fixture()
                if scenario == "foreign":
                    (context.root / "other-owner.py").write_text(LABEL, encoding="ascii")
                elif scenario == "overlap":
                    args["protected_paths"] += (context.root / "objects",)
                else:
                    args[{"paths": "max_paths", "nodes": "max_nodes", "bytes": "max_bytes"}[scenario]] = 1
                expected_error = quota.AuthoritySpoolQuotaError if scenario == "nodes" else spool.AuthoritySpoolError
                with self.assertRaises(expected_error) as caught:
                    spool.retire_completed_generation(context.root, **args)
                if scenario == "nodes":
                    self.assertEqual(caught.exception.code, "SPOOL_RETIREMENT_ENTRY_BOUND_EXCEEDED")
                self.assertTrue((context.root / entries[0]["descriptor"]["relative_path"]).exists())
                self.assertFalse((context.root / spool.RETIREMENT_RELATIVE_PATH).exists())

    def test_interrupted_retirement_resumes_only_its_saved_intent(self):
        context, _, args = self.fixture()
        original = Path.unlink
        calls = []
        def interrupted(path, *a, **kw):
            if path.is_relative_to(context.root) and path.relative_to(context.root).as_posix().startswith("objects/"):
                calls.append(path)
                if len(calls) == 2:
                    raise OSError("INVENTED_IO_INTERRUPTION")
            return original(path, *a, **kw)
        with patch.object(Path, "unlink", interrupted):
            with self.assertRaisesRegex(OSError, "INVENTED_IO_INTERRUPTION"):
                spool.retire_completed_generation(context.root, **args)
        self.assertEqual(spool.read_generation_retirement(context.root)["status"], "RETIRING")
        changed = dict(args, max_nodes=args["max_nodes"] + 1)
        with self.assertRaisesRegex(spool.AuthoritySpoolError, "RETIREMENT_RESUME_BINDING_CHANGED"):
            spool.retire_completed_generation(context.root, **changed)
        self.assertEqual(spool.retire_completed_generation(context.root, **args)["status"], "ARCHIVED_RECORD_ONLY")

    def test_changed_leaf_after_intent_is_preserved_and_refused(self):
        context, entries, args = self.fixture()
        original = Path.unlink
        def interrupted(path, *a, **kw):
            if path.is_relative_to(context.root) and path.relative_to(context.root).as_posix().startswith("objects/"):
                raise OSError("INVENTED_BEFORE_UNLINK")
            return original(path, *a, **kw)
        with patch.object(Path, "unlink", interrupted):
            with self.assertRaisesRegex(OSError, "INVENTED_BEFORE_UNLINK"):
                spool.retire_completed_generation(context.root, **args)
        leaf = context.root / entries[0]["descriptor"]["relative_path"]
        leaf.write_bytes(b"INVENTED_CHANGED_LEAF")
        with self.assertRaisesRegex(spool.AuthoritySpoolError, "RETIREMENT_PAYLOAD_CHANGED"):
            spool.retire_completed_generation(context.root, **args)
        self.assertEqual(leaf.read_bytes(), b"INVENTED_CHANGED_LEAF")

    def test_missing_owner_bad_fence_and_receipt_conflict_preserve_payload(self):
        for scenario in ("owner", "fence", "receipt"):
            with self.subTest(scenario=scenario):
                context, entries, args = self.fixture(owned=scenario != "owner")
                if scenario == "fence":
                    args["source_fence_sha256"] = "0" * 64
                elif scenario == "receipt":
                    args["retirement_receipt_path"].write_bytes(b"OTHER_OWNER")
                with self.assertRaises(spool.AuthoritySpoolError):
                    spool.retire_completed_generation(context.root, **args)
                self.assertTrue((context.root / entries[0]["descriptor"]["relative_path"]).exists())
                self.assertFalse((context.root / spool.RETIREMENT_RELATIVE_PATH).exists())


if __name__ == "__main__":
    unittest.main()
