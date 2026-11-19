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

    def _invented_account_result(self, context, retained, *, native=False, development_scope=None):
        import pm_historical_scheduler as scheduler

        body = {"schema_version": ("fxpm.declared-cell-raw-account.v4" if native
                else "fxpm.declared-development-m5-cell-raw-account.v1"),
            "fixture_provenance": LABEL, "progress_status": "COMPLETE" if native else "DEVELOPMENT_COMPLETE",
            "generation_id": context.generation_id, "generation_context_sha256": context.context_sha256,
            "declared_precommit_sha256": "1" * 64, "search_process_receipt_sha256": "2" * 64,
            "saved_cell_run_receipt_sha256": "3" * 64,
            "cell": {"cell_id": book.compute_cell_id("XAGUSD", "H1", "TREND"),
                "symbol": "XAGUSD", "timeframe": "H1", "regime": "TREND"},
            "account": {"fixture_provenance": LABEL},
            "qualification_authority": False, "capital_authority": False,
            "live_authority": False, "order_authority": False}
        if not native:
            body.update(result_kind="DEVELOPMENT_PARTIAL", source_kind="m5_derived",
                design_contaminated=True, pristine_validation=False, development_scope={"fixture_provenance": LABEL})
        elif development_scope is not None:
            body.update(schema_version="fxpm.declared-development-native-cell-raw-account.v1",
                progress_status="DEVELOPMENT_COMPLETE", result_kind="DEVELOPMENT_PARTIAL", source_kind="broker_native",
                design_contaminated=True, pristine_validation=False, development_scope=development_scope,
                development_scope_sha256=_sha(development_scope))
        sink = spool.open_symbol_sink(context, "INVENTED_ACCOUNT_NATIVE" if native else "INVENTED_ACCOUNT_M5")
        entry = sink.put_json("invented-final-account", "invented_final_account", body)
        sink.seal()
        reference = scheduler._declared_retain_account_result(context, entry["descriptor"], end_grid_root=retained)
        return entry, reference, body

    def fixture(self, *, owned=True, old_archive=False, legacy_fence=False):
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
        entries = []
        if not old_archive:
            sink = spool.open_symbol_sink(context, "OWNED_SOFTWARE_IO")
            entries = [sink.put_json("software-" + str(index), "software_fixture",
                {"fixture_provenance": LABEL, "value": index}) for index in range(2)]
            sink.seal()
        owner_sha = spool.read_owned_generation_workspace(context)[1] if owned else "0" * 64
        binding = {"generation_context_sha256": context.context_sha256, "run_id": context.generation_id,
            "cell_id": cell, "fixture_provenance": LABEL}
        retained_account, view_contract = None, None
        if not old_archive:
            entry, retained_account, account = self._invented_account_result(context, retained)
            entries.append(entry)
            binding.update(precommit_sha256="1" * 64, search_process_receipt_sha256="2" * 64,
                terminal_result_key="terminal")
            identity = {"account_artifact_sha256": entry["descriptor"]["sha256"],
                "cell_run_receipt_sha256": account["saved_cell_run_receipt_sha256"]}
            reconstruction = {
                "schema_version": "fxpm.m5-end-grid-reconstruction.v1", "fixture_provenance": LABEL,
                "development_scope": account["development_scope"], "actual_result_identity": identity,
                "terminal_account_reference": identity}
            if not legacy_fence:
                reconstruction["retained_account_result"] = retained_account
            view_contract = grid.store_view_contract(retained, reconstruction)
        view_key = (_sha({"fixture_provenance": LABEL, "view_contract_sha256": view_contract})
            if view_contract is not None else None)
        row = grid.build_end_grid_row(symbol="XAGUSD", timeframe="H1", regime="TREND",
            cycle_start_utc="2024-01-02T00:00:00+00:00", cycle_end_utc="2024-01-03T00:00:00+00:00",
            information_cutoff_utc="2024-01-01T23:00:00+00:00", outcome="STOP", reason_code="SOFTWARE_IO_ONLY",
            source_kind="m5_derived", source_contract_sha256=None, view_key=view_key, view_contract_sha256=view_contract,
            source_identity={}, cost_tier="NONE", process_edition_sha256="d" * 64,
            code_sha256={"software-fixture": "e" * 64}, metrics={}, job_binding_sha256=_sha(binding),
            produced_at_utc="2024-01-03T00:00:00+00:00")
        appended = grid.EndGridWriter(retained).append(row)
        fence = retained / "source-fence.json"
        summary = {"binding": binding, "owned_values_released": True, "release_eligible": True,
            "full_native_context_eligibility": False, "financial_consumer_ready": False, "fixture_provenance": LABEL}
        if retained_account is not None:
            summary["results"] = [["terminal", {"result_sha256": retained_account["sha256"],
                "result_descriptor_sha256": row.row_sha256}]]
        fence_body = {"schema_version": "fxpm.declared-m5-development-source-fence.v1",
            "workspace_owner_sha256": owner_sha,
            "source_summary": summary,
            "end_grid_rows": {"terminal": {"path": appended["path"], "row_sha256": row.row_sha256}},
            "qualification_authority": False, "capital_authority": False, "live_authority": False, "order_authority": False}
        if retained_account is not None and not legacy_fence:
            fence_body["retained_account_result"] = retained_account
        fence_sha = _write(fence, fence_body)
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

    def native_completion_fixture(self, *, mismatched_run=False, legacy_views=False,
            legacy_fence=False, development_k=None):
        from datetime import datetime, timedelta, timezone

        context, entries, args = self.fixture()
        retained = args["source_fence_path"].parent
        grid_root = retained / "native-grid"
        grid_root.mkdir()
        cell = book.compute_cell_id("XAGUSD", "H1", "TREND")
        precommit_sha, process_sha, run_sha = "1" * 64, "2" * 64, "3" * 64
        start = datetime(2023, 6, 24, tzinfo=timezone.utc)
        accepted = [f"owner-slot-{ordinal + 1:02d}-{start + timedelta(days=28 * ordinal):%Y%m%dT%H%M%SZ}"
            for ordinal in range(13)]
        scope = None
        if development_k is not None:
            import pm_historical_anchor_orchestration as orchestration
            scope = orchestration._declared_development_scope_body(
                {"slots": [{"slot_id": value} for value in accepted]}, accepted[:development_k])
        account_entry, retained_account, _ = self._invented_account_result(context, grid_root,
            native=True, development_scope=scope)
        entries.append(account_entry)
        selected_slots = []
        rows = []
        for ordinal in range(13 if development_k is None else development_k):
            cycle_start = start + timedelta(days=28 * ordinal)
            cycle_end = cycle_start + timedelta(days=28)
            cutoff = cycle_start - timedelta(hours=1)
            slot_id = accepted[ordinal]
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
            reconstruction = {
                "schema_version": "fxpm.broker-native-end-grid-reconstruction.v1",
                "view": view, "result": result, "cost_tier_evidence": tier_evidence}
            if not legacy_views:
                reconstruction["retained_account_result"] = retained_account
            contract = grid.store_view_contract(grid_root, reconstruction)
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
        fence_body = {
            "schema_version": (spool.NATIVE_COMPLETION_FENCE_SCHEMA if scope is None
                else spool.NATIVE_DEVELOPMENT_COMPLETION_FENCE_SCHEMA),
            "workspace_owner_sha256": args["workspace_owner_sha256"],
            "binding": {
                "generation_context_sha256": context.context_sha256,
                "run_id": context.generation_id,
                "cell_id": cell, "cell_contract_sha256": "c" * 64,
                "search_process_receipt_sha256": process_sha,
                "precommit_sha256": precommit_sha,
            },
            "completion": {"cell_run_receipt_sha256": run_sha,
                           "account_artifact_sha256": account_entry["descriptor"]["sha256"]},
            "selected_slots": selected_slots,
            "end_grid_rows": {slot["slot_id"]: {"path": item["path"],
                                               "row_sha256": item["row_sha256"]}
                              for slot, item in zip(selected_slots, appended, strict=True)},
            "qualification_authority": False, "capital_authority": False,
            "live_authority": False, "order_authority": False,
        }
        if not legacy_fence:
            fence_body["retained_account_result"] = retained_account
        if scope is not None:
            fence_body.update(development_scope=scope, development_scope_sha256=_sha(scope), accepted_slot_ids=accepted)
        fence_sha = _write(fence, fence_body)
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

    def test_native_legacy_grid_views_keep_their_bytes_with_new_retained_account_fence(self):
        context, _, args, grid_root, rows = self.native_completion_fixture(legacy_views=True)
        prior = {path: path.read_bytes() for path in grid_root.rglob("*") if path.is_file()}
        self.assertTrue(all("retained_account_result" not in grid.load_view_contract(grid_root, row.view_contract_sha256)
            for row in rows))
        result = spool.retire_completed_generation(context.root, **args)
        self.assertEqual(result["status"], "ARCHIVED_RECORD_ONLY")
        self.assertIn("retained_account_result", result)
        self.assertEqual(grid.read_end_grid(grid_root, "XAGUSD"), rows)
        self.assertEqual({path: path.read_bytes() for path in grid_root.rglob("*") if path.is_file()}, prior)
        self.assertEqual(spool.read_generation_retirement(context.root), result)

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

    def test_invented_old_archive_without_account_reference_remains_readable(self):
        """Invented historical-format metadata; fresh retirement still requires retention."""
        context, entries, args = self.fixture(old_archive=True)
        self.assertEqual(entries, [])
        with self.assertRaisesRegex(spool.AuthoritySpoolError, "RETIREMENT_RETAINED_ACCOUNT_INVALID"):
            spool.retire_completed_generation(context.root, **args)
        self.assertFalse((context.root / spool.RETIREMENT_RELATIVE_PATH).exists())
        fence = json.loads(args["source_fence_path"].read_bytes())
        self.assertNotIn("retained_account_result", fence)
        owner, owner_sha = spool.read_owned_generation_workspace(context)
        intent_sha = _sha({"fixture_provenance": LABEL, "historical_empty_payload_archive": True})
        receipt = {"schema_version": spool.RETIREMENT_SCHEMA, "status": "ARCHIVED_RECORD_ONLY",
            "fixture_provenance": "INVENTED_HISTORICAL_ARCHIVE_WITHOUT_ACCOUNT_REFERENCE",
            "generation_context_sha256": context.context_sha256, "generation_id": context.generation_id,
            "cell_id": owner["cell_id"], "cell_contract_sha256": owner["cell_contract_sha256"],
            "workspace_owner_sha256": owner_sha, "intent_sha256": intent_sha,
            "source_fence": {"path": str(args["source_fence_path"]), "sha256": args["source_fence_sha256"]},
            "precommit_sha256": None, "end_grid_rows": fence["end_grid_rows"],
            "retired_payload_files": 0, "retired_logical_bytes": 0, "payloads_retained": False,
            "genuine_cold_reopen_available": False, "descriptor_assurance": "IDENTITY_ONLY_AFTER_WORKSPACE_RELEASE",
            "retained_unit_locks": [], "qualification_authority": False, "capital_authority": False,
            "live_authority": False, "order_authority": False}
        receipt_sha = _write(args["retirement_receipt_path"], receipt)
        _write(context.root / spool.RETIREMENT_RELATIVE_PATH, {
            "schema_version": spool.RETIREMENT_SCHEMA, "status": "RETIRING",
            "generation_context_sha256": context.context_sha256, "generation_id": context.generation_id,
            "cell_id": owner["cell_id"], "intent_sha256": intent_sha,
            "source_fence_path": str(args["source_fence_path"]), "source_fence_sha256": args["source_fence_sha256"],
            "retirement_receipt_path": str(args["retirement_receipt_path"]), "workspace_owner_sha256": owner_sha})
        _write(context.root / spool.RETIREMENT_COMPLETE_RELATIVE_PATH, {
            "schema_version": spool.RETIREMENT_SCHEMA, "receipt_sha256": receipt_sha,
            "generation_context_sha256": context.context_sha256})
        archived = spool.read_generation_retirement(context.root)
        self.assertEqual(archived["status"], "ARCHIVED_RECORD_ONLY")
        self.assertEqual(archived["retirement_receipt_sha256"], receipt_sha)
        self.assertNotIn("retained_account_result", archived)

    def test_nonterminal_view_with_wrong_retained_account_ref_refuses_before_release(self):
        context, entries, args = self.fixture()
        fence = json.loads(args["source_fence_path"].read_bytes())
        retained = args["source_fence_path"].parent
        original = grid.read_end_grid(retained, "XAGUSD")[0]
        reconstruction = grid.load_view_contract(retained, original.view_contract_sha256)
        reconstruction["terminal_account_reference"] = None
        reconstruction["retained_account_result"] = {**fence["retained_account_result"],
            "size_bytes": fence["retained_account_result"]["size_bytes"] + 1}
        view_sha = grid.store_view_contract(retained, reconstruction)
        row = grid.build_end_grid_row(symbol="XAGUSD", timeframe="H1", regime="TREND",
            cycle_start_utc="2024-01-04T00:00:00+00:00", cycle_end_utc="2024-01-05T00:00:00+00:00",
            information_cutoff_utc="2024-01-03T23:00:00+00:00", outcome="STOP", reason_code="SOFTWARE_IO_ONLY",
            source_kind="m5_derived", source_contract_sha256=None, view_key=None, view_contract_sha256=view_sha,
            source_identity={}, cost_tier="NONE", process_edition_sha256="d" * 64,
            code_sha256={"software-fixture": "e" * 64}, metrics={},
            job_binding_sha256=_sha(fence["source_summary"]["binding"]),
            produced_at_utc="2024-01-05T00:00:00+00:00")
        appended = grid.EndGridWriter(retained).append(row)
        fence["end_grid_rows"]["nonterminal"] = {key: appended[key] for key in ("path", "row_sha256")}
        invalid_fence = retained / "invented-wrong-nonterminal-reference.json"
        args.update(source_fence_path=invalid_fence, source_fence_sha256=_write(invalid_fence, fence))
        with self.assertRaisesRegex(spool.AuthoritySpoolError, "RETIREMENT_RETAINED_ACCOUNT_INVALID"):
            spool.retire_completed_generation(context.root, **args)
        self.assertTrue(all((context.root / item["descriptor"]["relative_path"]).exists() for item in entries))
        self.assertFalse((context.root / spool.RETIREMENT_RELATIVE_PATH).exists())

    def _legacy_completion_case(self, kind):
        """Invented old-format history; real owned artifact/Grid/retirement IO."""
        if kind == "m5":
            context, entries, args = self.fixture(legacy_fence=True)
            grid_root = args["source_fence_path"].parent
        else:
            context, entries, args, grid_root, _ = self.native_completion_fixture(
                legacy_views=True, legacy_fence=True,
                development_k=2 if kind == "native-dev" else None)
        fence = json.loads(args["source_fence_path"].read_bytes())
        self.assertNotIn("retained_account_result", fence)
        account_sha = (fence["source_summary"]["results"][0][1]["result_sha256"] if kind == "m5"
            else fence["completion"]["account_artifact_sha256"])
        entry = next(value for value in entries if value["descriptor"]["sha256"] == account_sha)
        reference = {"path": str(grid_root / ("declared-account-" + account_sha + ".json")),
            "sha256": account_sha, "size_bytes": entry["descriptor"]["size_bytes"]}
        original = context.root / entry["descriptor"]["relative_path"]
        self.assertEqual(Path(reference["path"]).read_bytes(), original.read_bytes())
        return context, entries, args, reference, original

    def test_legacy_completion_fence_retains_account_without_rewriting_history(self):
        for kind in ("native", "native-dev", "m5"):
            with self.subTest(kind=kind):
                context, _, args, reference, original = self._legacy_completion_case(kind)
                retained = args["source_fence_path"].parent
                history = {path: path.read_bytes() for path in retained.rglob("*") if path.is_file()}
                owner, _ = spool.read_owned_generation_workspace(context)
                observed = {"unverified": "MUST_NOT_ESCAPE"}
                body = spool._retirement_fence(args["source_fence_path"], args["source_fence_sha256"],
                    context_sha256=context.context_sha256, generation_id=context.generation_id,
                    cell_id=owner["cell_id"], workspace_sha256=args["workspace_owner_sha256"],
                    working_root=context.root, owner=owner, require_retained_account=True,
                    retained_account_result_out=observed)
                self.assertEqual(body, json.loads(history[args["source_fence_path"]]))
                self.assertNotIn("retained_account_result", body)
                self.assertEqual(observed, reference)
                result = spool.retire_completed_generation(context.root, **args)
                self.assertEqual(result["status"], "ARCHIVED_RECORD_ONLY")
                self.assertEqual(result["retained_account_result"], reference)
                self.assertFalse(original.exists())
                self.assertEqual({path: path.read_bytes() for path in history}, history)
                self.assertEqual(spool.read_generation_retirement(context.root), result)

    def test_legacy_completion_missing_or_changed_copy_preserves_original_account(self):
        for kind in ("native", "native-dev", "m5"):
            for fault in ("missing", "changed"):
                with self.subTest(kind=kind, fault=fault):
                    context, entries, args, reference, original = self._legacy_completion_case(kind)
                    target = Path(reference["path"])
                    raw = original.read_bytes()
                    fence_raw = args["source_fence_path"].read_bytes()
                    if fault == "missing":
                        target.unlink()
                    else:
                        target.write_bytes(b"INVENTED_CHANGED_ACCOUNT_COPY")
                    with self.assertRaises(spool.AuthoritySpoolError):
                        spool.retire_completed_generation(context.root, **args)
                    self.assertEqual(original.read_bytes(), raw)
                    self.assertTrue(all((context.root / item["descriptor"]["relative_path"]).exists() for item in entries))
                    self.assertEqual(args["source_fence_path"].read_bytes(), fence_raw)
                    self.assertFalse((context.root / spool.RETIREMENT_RELATIVE_PATH).exists())
                    self.assertFalse((context.root / spool.RETIREMENT_INTENT_RELATIVE_PATH).exists())
                    # Explicit repair of this test-owned copy, not an automatic production retry.
                    target.write_bytes(raw)
                    result = spool.retire_completed_generation(context.root, **args)
                    self.assertEqual(result["retained_account_result"], reference)
                    self.assertEqual(spool.read_generation_retirement(context.root), result)

    def test_legacy_fence_archive_reverifies_account_and_independent_receipt_reference(self):
        for kind in ("native", "native-dev", "m5"):
            with self.subTest(kind=kind):
                context, _, args, reference, _ = self._legacy_completion_case(kind)
                result = spool.retire_completed_generation(context.root, **args)
                receipt_raw = args["retirement_receipt_path"].read_bytes()
                complete_path = context.root / spool.RETIREMENT_COMPLETE_RELATIVE_PATH
                complete_raw = complete_path.read_bytes()
                wrong = json.loads(receipt_raw)
                wrong["retained_account_result"]["size_bytes"] += 1
                complete = json.loads(complete_raw)
                # Deliberately corrupt only these known synthetic files, then restore their exact bytes below.
                wrong_raw = grid.canonical_bytes(wrong)
                args["retirement_receipt_path"].write_bytes(wrong_raw)
                complete["receipt_sha256"] = hashlib.sha256(wrong_raw).hexdigest()
                complete_path.write_bytes(grid.canonical_bytes(complete))
                with self.assertRaisesRegex(spool.AuthoritySpoolError, "RETIREMENT_COMPLETION_INVALID"):
                    spool.read_generation_retirement(context.root)
                args["retirement_receipt_path"].write_bytes(receipt_raw)
                complete_path.write_bytes(complete_raw)
                self.assertEqual(spool.read_generation_retirement(context.root), result)
                target = Path(reference["path"])
                target.write_bytes(b"INVENTED_CHANGED_ARCHIVED_ACCOUNT")
                with self.assertRaises(spool.AuthoritySpoolError):
                    spool.read_generation_retirement(context.root)
                self.assertEqual(args["retirement_receipt_path"].read_bytes(), receipt_raw)

    def test_new_legacy_intent_keeps_retained_copy_required_on_resume(self):
        for kind in ("native", "m5"):
            for segmented in (False, True):
                for fault in ("missing", "changed"):
                    with self.subTest(kind=kind, segmented=segmented, fault=fault):
                        context, _, args, reference, original = self._legacy_completion_case(kind)
                        if segmented:
                            args["segment_max_records"] = 2
                        raw = original.read_bytes()
                        fence_raw = args["source_fence_path"].read_bytes()
                        original_unlink = Path.unlink
                        def interrupt_account(path, *a, **kw):
                            if path == original:
                                raise OSError("INVENTED_BEFORE_ACCOUNT_UNLINK")
                            return original_unlink(path, *a, **kw)
                        with patch.object(Path, "unlink", interrupt_account):
                            with self.assertRaisesRegex(OSError, "INVENTED_BEFORE_ACCOUNT_UNLINK"):
                                spool.retire_completed_generation(context.root, **args)
                        intent = json.loads((context.root / spool.RETIREMENT_INTENT_RELATIVE_PATH).read_bytes())
                        self.assertEqual(intent["arguments"]["retained_account_result"], reference)
                        self.assertTrue((context.root / spool.RETIREMENT_RELATIVE_PATH).exists())
                        target = Path(reference["path"])
                        if fault == "missing":
                            target.unlink()
                        else:
                            target.write_bytes(b"INVENTED_CHANGED_COPY_DURING_RELEASE")
                        unlinked = []
                        def observe_unlink(path, *a, **kw):
                            if path.is_relative_to(context.root):
                                unlinked.append(path)
                            return original_unlink(path, *a, **kw)
                        with patch.object(Path, "unlink", observe_unlink):
                            with self.assertRaises(spool.AuthoritySpoolError):
                                spool.retire_completed_generation(context.root, **args)
                        self.assertEqual(unlinked, [])
                        self.assertEqual(original.read_bytes(), raw)
                        self.assertEqual(args["source_fence_path"].read_bytes(), fence_raw)
                        target.write_bytes(raw)
                        result = spool.retire_completed_generation(context.root, **args)
                        self.assertEqual(result["retained_account_result"], reference)
                        self.assertEqual(spool.read_generation_retirement(context.root), result)

    def test_new_legacy_retirement_resumes_after_receipt_or_completion_publication(self):
        stages = ((False, "receipt"), (True, "receipt"), (True, "completion"))
        for kind in ("native", "m5"):
            for segmented, stage in stages:
                with self.subTest(kind=kind, segmented=segmented, stage=stage):
                    context, _, args, reference, _ = self._legacy_completion_case(kind)
                    if segmented:
                        args["segment_max_records"] = 2
                    fence_raw = args["source_fence_path"].read_bytes()
                    if stage == "receipt":
                        name = "_retirement_publish_v2" if segmented else "_retirement_publish"
                        original_publish = getattr(spool, name)
                        def before_complete(root, relative, *a, **kw):
                            if root == context.root and relative == spool.RETIREMENT_COMPLETE_RELATIVE_PATH:
                                raise OSError("INVENTED_AFTER_EXTERNAL_RECEIPT")
                            return original_publish(root, relative, *a, **kw)
                        with patch.object(spool, name, before_complete):
                            with self.assertRaisesRegex(OSError, "INVENTED_AFTER_EXTERNAL_RECEIPT"):
                                spool.retire_completed_generation(context.root, **args)
                    else:
                        with patch.object(spool, "_retirement_v2_cleanup",
                                side_effect=OSError("INVENTED_AFTER_COMPLETE_BEFORE_CLEANUP")):
                            with self.assertRaisesRegex(OSError, "INVENTED_AFTER_COMPLETE_BEFORE_CLEANUP"):
                                spool.retire_completed_generation(context.root, **args)
                    receipt_raw = args["retirement_receipt_path"].read_bytes()
                    self.assertEqual(json.loads(receipt_raw)["retained_account_result"], reference)
                    intent = json.loads((context.root / spool.RETIREMENT_INTENT_RELATIVE_PATH).read_bytes())
                    self.assertEqual(intent["arguments"]["retained_account_result"], reference)
                    result = spool.retire_completed_generation(context.root, **args)
                    self.assertEqual(result["retained_account_result"], reference)
                    self.assertEqual(args["retirement_receipt_path"].read_bytes(), receipt_raw)
                    self.assertEqual(args["source_fence_path"].read_bytes(), fence_raw)
                    self.assertEqual(spool.read_generation_retirement(context.root), result)
                    self.assertEqual(spool.retire_completed_generation(context.root, **args), result)

    def _m5_account_refinalization_case(self, *, legacy):
        """Invented source capabilities; real account arithmetic, manifest, finalizer and Grid IO.

        This proves software refinalization only. Precommit, source material,
        original commit authentication and cost capabilities are labelled doubles.
        No market data, fitted model or native source authority is supplied.
        """
        from types import SimpleNamespace
        import pandas as pd
        import pm_core
        import pm_causal_evidence_producer as causal
        import pm_forward_oos_replay_producer as forward
        import pm_historical_anchor_orchestration as orchestration
        import pm_historical_scheduler as scheduler
        import pm_historical_search_producer as producer
        import pm_m5_job_view as views
        import pm_production_anchor_plan as anchors

        case = self.parent / uuid.uuid4().hex
        case.mkdir()
        retained = case / "retained"
        retained.mkdir()
        config_path, broker_path = retained / "invented-config.json", retained / "invented-broker.json"
        _write(broker_path, {"fixture_provenance": LABEL})
        config_sha = _write(config_path, {"_meta": {"scope": LABEL}, "pipeline": {"initial_capital": 10000.0},
            "position": {}, "broker_specs_path": str(broker_path)})
        config = pm_core.PipelineConfig(research_mode=True, initial_capital=10000.0,
            discovery_risk_per_trade_pct=1.0, use_spread=True, backtest_spread_mode="conservative_max",
            swap_model_enabled=False)
        config._deployment_config_path = str(config_path)
        config.data_dir = str(retained / "invented-data")
        config.output_dir = str(retained / "invented-output")
        cell = {"cell_id": book.compute_cell_id("XAGUSD", "H1", "TREND"),
            "symbol": "XAGUSD", "timeframe": "H1", "regime": "TREND"}
        context = spool.freeze_generation_context(case / "working", generation_id="INVENTED_M5_REFINALIZATION",
            native_universe_sha256="a" * 64, config_sha256=config_sha,
            owned_workspace={"cell_id": cell["cell_id"], "cell_contract_sha256": "c" * 64})
        frozen = SimpleNamespace(active_config_path=config_path, config_sha256=config_sha,
            effective_config_sha256="d" * 64)
        launch = "2023-06-24T00:00:00Z"
        roster = anchors.derive_declared_owner_slot_roster_v2(launch_activation_utc=launch,
            frozen_refit_clock={"anchor_activation_utc": "2026-03-29T00:01:00Z", "interval_calendar_days": 28},
            config_identity_refs={"config_sha256": config_sha, "effective_config_sha256": frozen.effective_config_sha256},
            extend_frozen_clock_backward=True, protected_end_exclusive_utc="2024-07-14T08:05:00Z")
        slot = roster["slots"][0]
        scope = orchestration._declared_development_scope_body(roster, [slot["slot_id"]])
        parent = {"schema_version": orchestration.DECLARED_M5_DEVELOPMENT_SLOT_PRECOMMIT_SCHEMA,
            "source_kind": "m5_derived", "cells": [cell], "slot_roster": roster,
            "slot_roster_sha256": _sha(roster), "launch_activation_utc": launch,
            "development_scope": scope, "development_scope_sha256": _sha(scope)}
        precommit = SimpleNamespace(receipt_sha256="1" * 64, to_dict=lambda: dict(parent))
        process = SimpleNamespace(receipt_sha256="2" * 64)
        terminal, view_id = "invented-terminal", "invented-slot-view"
        budgets = views.ViewBudgets(16, 131072, 16, 131072, 32, 262144)
        binding = views.DerivedJobBinding("3" * 64, context.context_sha256, config_sha,
            frozen.effective_config_sha256, process.receipt_sha256, precommit.receipt_sha256,
            context.generation_id, cell["cell_id"], _sha(asdict(budgets)), "4" * 64,
            (slot["slot_id"], terminal), terminal)
        window = {"source_kind": "m5_derived", "m5_view_contract_sha256": binding.m5_view_contract_sha256,
            "view_key": "5" * 64, "fixture_provenance": LABEL}
        baseline = {"fixture_provenance": LABEL, "m5_execution_view_binding": window}

        class InventedLease:
            def __init__(self, view_id):
                self.view_id = view_id
            def __enter__(self):
                return self
            def __exit__(self, *exception):
                return False
        class InventedOwner:
            """Finite upstream capability double, not a source verifier."""
            pending_view_ids = ()
            source_identity = SimpleNamespace(activation_id=LABEL, bars_sha256="6" * 64, point=0.01)
            job_binding_sha256 = _sha(asdict(binding))
            def consumer_bindings_for(self, key):
                return (_sha({"fixture": LABEL, "consumer": key}),)
            def reopen_metadata(self):
                return json.loads(grid.canonical_bytes({"binding": asdict(binding), "budgets": asdict(budgets),
                    "requests": [{"view_id": view_id, "constructor_implementation_sha256": "7" * 64,
                        "gap_findings_sha256": "8" * 64}]}))
            def acquire(self, view, **kwargs):
                assert view in (view_id, "invented-calibration")
                return InventedLease(view)
            def view(self, lease):
                assert lease.view_id == view_id
                return SimpleNamespace(cutoff_utc=slot["native_cutoff_utc"], view_key=window["view_key"])
            def route_at(self, cutoff):
                assert cutoff == slot["native_cutoff_utc"]
                return {"source_identity_sha256": "9" * 64}
            def finish(self, results, *, end_grid_rows):
                return views.JobSourceSummary(binding, budgets, tuple(sorted(results.items())),
                    tuple(sorted(end_grid_rows.items())), (), (), ())
        owner = InventedOwner()
        owner.binding = binding
        class InventedEdition:
            sha256 = "a" * 64
            def bind_view(self, source_owner, **kwargs):
                self_owner = source_owner
                def from_descriptor(value, **bound):
                    assert self_owner is owner and value == baseline
                    assert bound["source_identity_sha256"] == "9" * 64
                return SimpleNamespace(resolve=lambda *a, **kw: SimpleNamespace(descriptor=lambda: dict(baseline)),
                    from_descriptor=from_descriptor)
        edition = InventedEdition()
        run_body = {"fixture_provenance": LABEL, "status": "DEVELOPMENT_COMPLETE"}
        commit = {"slot_id": slot["slot_id"], "fixture_provenance": LABEL,
            "completion": {"status": "NO_TRADE", "request_sha256": "b" * 64}}
        commit_descriptor = spool.write_canonical_json_artifact(context, commit)
        commit_entry = {"artifact_id": "invented-commit", "artifact_type": "invented_unauthenticated_commit",
            "descriptor": commit_descriptor}
        progress = SimpleNamespace(slot_commit_artifacts=(commit_entry,),
            slot_summaries=({"slot_id": slot["slot_id"], "ordinal": 1, "status": "NO_TRADE",
                "commit_sha256": commit_descriptor["sha256"]},), run_receipt=run_body)
        start, end = pd.Timestamp(slot["activation_utc"]), pd.Timestamp(slot["end_exclusive_utc"])
        index = pd.DatetimeIndex([start-pd.Timedelta(hours=1), start, end-pd.Timedelta(hours=1)])
        frame = pd.DataFrame({"Open": 100.0, "High": 100.0, "Low": 100.0, "Close": 100.0,
            "SpreadOpen": 2.0, "SpreadMax": 2.0}, index=index)
        frame["_BAR_CLOSE_TS"] = index + pd.Timedelta(hours=1)
        schedule = pm_core.ContinuousEntrySchedule((),
            (pm_core.ContinuousEntrySlot(slot["slot_id"], start, end, None),))
        inputs = {"parent": parent, "config": config, "progress": progress, "commits": [commit],
            "slot_schedule": [{"slot_id": slot["slot_id"], "ordinal": 1, "start_utc": slot["activation_utc"],
                "end_exclusive_utc": slot["end_exclusive_utc"], "status": "NO_TRADE", "owner_id": None}],
            "owner_records": [], "progress_status": "DEVELOPMENT_COMPLETE", "reason_code": LABEL}
        material = {"frame": frame, "owners": (), "schedule": schedule,
            "observations": pd.Series(2.0, index=index), "matched": pd.Series(index, index=index), "warmup": 1,
            "spec": pm_core.InstrumentSpec("XAGUSD", 2, point=0.01, tick_size=0.01, tick_value=1.0,
                pip_value=1.0, min_lot=0.01, max_lot=100.0, volume_step=0.01, asset_class="fx"),
            "deployment": forward._deployment_config(config, declared_research_account=True), "leases": [],
            "execution_identity": {"fixture_provenance": LABEL}, "spread_identity": {"fixture_provenance": LABEL},
            "spec_receipt": {"fixture_provenance": LABEL}, "source_context": {"account_view_id": "invented-account"},
            "account_bindings": {"fixture_provenance": LABEL}, "window": window, "selected_tier_views": [],
            "terminal_consumers": list(owner.consumer_bindings_for(terminal))}
        grid_root = retained / "grid"
        grid_root.mkdir()
        fence_path = retained / ("legacy-m5-fence.json" if legacy else "own-m5-fence.json")
        row = {"cell": cell, "m5_development": {"terminal_result_key": terminal,
            "development_slot_ids": [slot["slot_id"]], "end_grid_root": str(grid_root),
            "source_fence_path": str(fence_path), "slot_view_ids": {slot["slot_id"]: view_id},
            "calibration_view_id": "invented-calibration", "m5_source_pins": {"constructor_identity": {"fixture": "c" * 64}},
            "cost_group_metadata": {"fixture_provenance": LABEL}}}
        direct = ((), (), {"fixture_provenance": LABEL})
        def reopened_parent(actual_context, **kwargs):
            assert actual_context is context and kwargs["receipt_sha256"] == precommit.receipt_sha256
            assert kwargs["process"] is process and kwargs["frozen_pipeline_config"] is frozen
            return precommit
        def reopened_inputs(actual_context, **kwargs):
            assert actual_context is context and kwargs["precommit"] is precommit
            assert kwargs["process"] is process and kwargs["frozen_pipeline_config"] is frozen
            return inputs
        def source_material(actual_context, **kwargs):
            assert actual_context is context and kwargs["precommit"] is precommit
            assert kwargs["process"] is process and kwargs["owner"] is owner and kwargs["cost_edition"] is edition
            assert kwargs["account_view_id"] == "invented-account" and kwargs["inputs"] is inputs
            return material
        def original_commit(actual_context, relative):
            assert actual_context is context
            assert relative == producer._declared_commit_path(precommit, cell["cell_id"], 1)
            return commit, commit_descriptor["sha256"], commit_descriptor
        def direct_files(actual_context, actual_parent, actual_row, **kwargs):
            assert actual_context is context and actual_parent is precommit and actual_row is row
            assert kwargs["commits"] == {slot["slot_id"]: commit}
            return direct
        def snapshot(paths):
            result = {}
            for path in paths:
                raw = path.read_bytes()
                result[path] = (raw, hashlib.sha256(raw).hexdigest(),
                    spool._file_identity(path.stat(follow_symlinks=False)))
            return result
        with patch.object(orchestration, "reopen_declared_slot_owner_run_precommit", side_effect=reopened_parent), \
                patch.object(forward, "_reopen_declared_development_cell_inputs", side_effect=reopened_inputs), \
                patch.object(forward, "_declared_m5_account_frame_inputs", side_effect=source_material), \
                patch.object(producer, "_read_declared_commit", side_effect=original_commit), \
                patch.object(scheduler, "_declared_m5_owned_direct_files", side_effect=direct_files):
            artifact = forward.produce_declared_m5_development_cell_account_artifact(context,
                precommit=precommit, process=process, frozen_pipeline_config=frozen,
                m5_owner=owner, account_view_id="invented-account", cost_edition=edition)
            account_raw = (context.root / artifact.descriptor["relative_path"]).read_bytes()
            account = causal._read_descriptor_json(context, artifact.descriptor, code="DECLARED_CELL_RAW_ACCOUNT")
            run_descriptor = scheduler._declared_run_descriptor(precommit, row, run_body)
            workspace_sha = spool.read_owned_generation_workspace(context)[1]
            grids, history, fence_sha = None, {}, None
            if legacy:
                grids = scheduler._declared_m5_append_grid(context, precommit=precommit, process=process, row=row,
                    owner=owner, edition=edition, account_body=account, account_descriptor=artifact.descriptor,
                    run_descriptor=run_descriptor, commits={slot["slot_id"]: commit})
                result_bindings = {slot["slot_id"]: views.CallerValidatedResultBinding(commit_descriptor["sha256"],
                    owner.job_binding_sha256, owner.consumer_bindings_for(slot["slot_id"]),
                    result_descriptor_sha256=grids[slot["slot_id"]]["row_sha256"]),
                    terminal: views.CallerValidatedResultBinding(artifact.descriptor["sha256"], owner.job_binding_sha256,
                        owner.consumer_bindings_for(terminal), result_descriptor_sha256=grids[slot["slot_id"]]["row_sha256"])}
                end_grid = {key: views.EndGridResultBinding(value["path"], value["row_sha256"], value["appended"],
                    value["readback_row_sha256"]) for key, value in grids.items()}
                end_grid[terminal] = end_grid[slot["slot_id"]]
                compact = asdict(owner.finish(result_bindings, end_grid_rows=end_grid))
                del compact["end_grid_rows"]
                old_fence = {"schema_version": "fxpm.declared-m5-development-source-fence.v1",
                    "workspace_owner_sha256": workspace_sha,
                    "terminal_end_grid_reference_semantics": "FINAL_SELECTED_SLOT_ROW_WITH_SEPARATE_ACCOUNT_BINDING",
                    "retirement_ownership": direct[2], "end_grid_rows": {key: {name: value[name] for name in ("path", "row_sha256")}
                        for key, value in grids.items()}, "source_summary": compact, **scheduler._DECLARED_M5_SCOPE}
                fence_sha = _write(fence_path, old_fence)
                history = snapshot([path for path in grid_root.rglob("*") if path.is_file()] + [fence_path])
                owner = InventedOwner()
                owner.binding = binding
            with patch.object(forward, "verify_declared_m5_development_cell_account_transport",
                    wraps=forward.verify_declared_m5_development_cell_account_transport) as verified, \
                    patch.object(forward, "_reconcile_declared_cell_account",
                    wraps=forward._reconcile_declared_cell_account) as reconciled, \
                    patch.object(scheduler, "_declared_retained_completion_fence",
                    wraps=scheduler._declared_retained_completion_fence) as compared:
                returned, fence, returned_direct = scheduler._declared_m5_finalize(context,
                    precommit=precommit, process=process, frozen=frozen, contract={}, row=row,
                    owner=owner, edition=edition, run_body=run_body, artifact=artifact)
                self.assertEqual(verified.call_count, 1)
                self.assertEqual(reconciled.call_count, 1)
                self.assertEqual(compared.call_count, 1)
                self.assertIsInstance(compared.call_args.args[1]["source_summary"]["results"], tuple)
            self.assertEqual(returned_direct, direct)
            if legacy:
                self.assertEqual(fence["sha256"], fence_sha)
                self.assertEqual(returned[slot["slot_id"]]["row_sha256"], grids[slot["slot_id"]]["row_sha256"])
                self.assertEqual(snapshot(history), history)
            else:
                grids, fence_sha = returned, fence["sha256"]
                self.assertIn("retained_account_result", json.loads(fence_path.read_bytes()))
            copy_path = grid_root / ("declared-account-" + artifact.descriptor["sha256"] + ".json")
            self.assertEqual(copy_path.read_bytes(), account_raw)
            published = snapshot([path for path in grid_root.rglob("*") if path.is_file()] + [fence_path])
            retirement_args = dict(expected_context_sha256=context.context_sha256,
                workspace_owner_sha256=workspace_sha, source_fence_path=fence_path,
                source_fence_sha256=fence_sha, retirement_receipt_path=retained / "retirement.json",
                protected_paths=(retained, config_path, broker_path, Path(spool.__file__), Path(quota.__file__)),
                owned_direct_files=(), unit_lock_paths=(), max_paths=100, max_nodes=300, max_bytes=4*1024**2)
            original_publish = spool._retirement_publish
            def before_marker(root, relative, *args, **kwargs):
                if root == context.root and relative == spool.RETIREMENT_RELATIVE_PATH:
                    raise OSError("INVENTED_AFTER_M5_PUBLICATION_BEFORE_MARKER")
                return original_publish(root, relative, *args, **kwargs)
            with patch.object(spool, "_retirement_publish", side_effect=before_marker):
                with self.assertRaisesRegex(OSError, "INVENTED_AFTER_M5_PUBLICATION_BEFORE_MARKER"):
                    spool.retire_completed_generation(context.root, **retirement_args)
            self.assertFalse((context.root / spool.RETIREMENT_RELATIVE_PATH).exists())
            self.assertTrue((context.root / spool.RETIREMENT_INTENT_RELATIVE_PATH).exists())
            self.assertEqual((context.root / artifact.descriptor["relative_path"]).read_bytes(), account_raw)
            self.assertEqual(snapshot(published), published)
            previous_owner = owner
            owner = InventedOwner()
            owner.binding = binding
            self.assertIsNot(owner, previous_owner)
            with patch.object(forward, "verify_declared_m5_development_cell_account_transport",
                    wraps=forward.verify_declared_m5_development_cell_account_transport) as verified, \
                    patch.object(forward, "_reconcile_declared_cell_account",
                    wraps=forward._reconcile_declared_cell_account) as reconciled, \
                    patch.object(scheduler, "_declared_retained_completion_fence",
                    wraps=scheduler._declared_retained_completion_fence) as compared:
                resumed, repeated_fence, repeated_direct = scheduler._declared_m5_finalize(context,
                    precommit=precommit, process=process, frozen=frozen, contract={}, row=row,
                    owner=owner, edition=edition, run_body=run_body, artifact=artifact)
                self.assertEqual((verified.call_count, reconciled.call_count, compared.call_count), (1, 1, 1))
                self.assertIsInstance(compared.call_args.args[1]["source_summary"]["results"], tuple)
            self.assertEqual(repeated_direct, direct)
            self.assertEqual(repeated_fence["sha256"], fence_sha)
            self.assertEqual(resumed[slot["slot_id"]]["row_sha256"], grids[slot["slot_id"]]["row_sha256"])
            self.assertEqual(snapshot(published), published)
        result = spool.retire_completed_generation(context.root, **retirement_args)
        self.assertEqual(result["status"], "ARCHIVED_RECORD_ONLY")
        self.assertEqual(result["retained_account_result"]["sha256"], artifact.descriptor["sha256"])
        self.assertEqual(Path(result["retained_account_result"]["path"]).read_bytes(), account_raw)
        self.assertEqual(snapshot(published), published)
        self.assertEqual(spool.read_generation_retirement(context.root), result)

    def test_m5_legacy_account_refinalization_keeps_real_verification_grid_and_retirement(self):
        self._m5_account_refinalization_case(legacy=True)

    def test_m5_own_fence_refinalization_after_interrupted_retirement_preserves_publications(self):
        self._m5_account_refinalization_case(legacy=False)

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

    def _invented_saved_legacy_intent(self, *, segmented, marker):
        """Model the old protocol's saved plan without claiming a current finish."""
        context, _, args = self.fixture(old_archive=True)
        sink = spool.open_symbol_sink(context, "INVENTED_LEGACY_RELEASE")
        entry = sink.put_json("legacy-payload", "software_fixture", {"fixture_provenance": LABEL})
        sink.seal()
        records, _, _, _ = spool._retirement_inventory(context.root, frozenset(args["owned_direct_files"]),
            max_paths=args["max_paths"], max_nodes=args["max_nodes"], max_bytes=args["max_bytes"],
            retained_unit_locks=args["unit_lock_paths"])
        arguments = {"source_fence_path": str(args["source_fence_path"]),
            "source_fence_sha256": args["source_fence_sha256"],
            "retirement_receipt_path": str(args["retirement_receipt_path"]),
            "workspace_owner_sha256": args["workspace_owner_sha256"],
            "protected_paths": [str(Path(value).resolve()) for value in args["protected_paths"]],
            "owned_direct_files": sorted(args["owned_direct_files"]),
            "unit_lock_paths": list(args["unit_lock_paths"]),
            **{key: args[key] for key in ("max_paths", "max_nodes", "max_bytes")}}
        schema = spool.RETIREMENT_SCHEMA
        if segmented:
            args["segment_max_records"] = arguments["segment_max_records"] = 1
            plan = spool.plan_segmented_retirement(records, generation_context_sha256=context.context_sha256,
                arguments=arguments, maximum_bytes=context.limits.max_json_bytes, segment_max_records=1)
            for segment in plan["segments"]:
                path = context.root / segment["relative_path"]
                path.parent.mkdir(exist_ok=True)
                self.assertEqual(_write(path, segment["body"]), segment["sha256"])
            intent = plan["intent"]
            schema = spool.RETIREMENT_SCHEMA_V2
        else:
            intent = {"schema_version": schema, "generation_context_sha256": context.context_sha256,
                "arguments": arguments, "files": records}
        intent_sha = _write(context.root / spool.RETIREMENT_INTENT_RELATIVE_PATH, intent)
        if marker:
            owner, _ = spool.read_owned_generation_workspace(context)
            _write(context.root / spool.RETIREMENT_RELATIVE_PATH, {
                "schema_version": schema, "status": "RETIRING",
                "generation_context_sha256": context.context_sha256,
                "generation_id": context.generation_id, "cell_id": owner["cell_id"],
                "intent_sha256": intent_sha,
                **{key: arguments[key] for key in ("source_fence_path", "source_fence_sha256",
                    "retirement_receipt_path", "workspace_owner_sha256")}})
        return context, entry, args

    def test_saved_legacy_retirement_resumes_after_partial_release_without_inventing_account(self):
        context, entry, args = self._invented_saved_legacy_intent(segmented=False, marker=True)
        fence_bytes = args["source_fence_path"].read_bytes()
        released = context.root / entry["descriptor"]["relative_path"]
        self.assertTrue(released.resolve().is_relative_to(context.root.resolve()))
        released.unlink()
        result = spool.retire_completed_generation(context.root, **args)
        self.assertEqual(result["status"], "ARCHIVED_RECORD_ONLY")
        self.assertNotIn("retained_account_result", result)
        self.assertEqual(args["source_fence_path"].read_bytes(), fence_bytes)
        self.assertEqual(spool.read_generation_retirement(context.root), result)

    def test_saved_legacy_segmented_intent_resumes_before_marker_without_new_account(self):
        context, entry, args = self._invented_saved_legacy_intent(segmented=True, marker=False)
        self.assertFalse((context.root / spool.RETIREMENT_RELATIVE_PATH).exists())
        result = spool.retire_completed_generation(context.root, **args)
        self.assertEqual(result["status"], "ARCHIVED_RECORD_ONLY")
        self.assertNotIn("retained_account_result", result)
        self.assertFalse((context.root / entry["descriptor"]["relative_path"]).exists())
        self.assertEqual(spool.read_generation_retirement(context.root), result)

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
