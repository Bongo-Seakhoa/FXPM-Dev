"""Invented software declarations only; no native or run authority is verified.

Future fixture writes require an explicit owned QA root and remain retained.
These tests never open a recorded cost edition or produce a native prefix.
"""

from __future__ import annotations

from copy import deepcopy
import hashlib
import json
import os
from pathlib import Path
import unittest
import uuid

import pm_historical_scheduler as scheduler
import pm_historical_anchor_orchestration as orchestration
import pm_m5_job_view as views
import pm_authority_spool as spool
import pm_explicit_derived_publication as publication


PROVENANCE = "INVENTED_SOFTWARE_DECLARATION_NO_NATIVE_VERIFICATION_NO_RUN_GRANT"


def _sha(value):
    return hashlib.sha256(json.dumps(value, ensure_ascii=True, sort_keys=True,
        separators=(",", ":"), allow_nan=False).encode("ascii")).hexdigest()


class M5DevelopmentSchedulerTests(unittest.TestCase):
    def setUp(self):
        configured = os.environ.get("FXPM_DERIVED_QA_ROOT")
        if not configured:
            self.skipTest("explicit retained FXPM_DERIVED_QA_ROOT is required")
        parent = Path(configured)
        self.assertTrue(parent.is_absolute() and parent.is_dir())
        self.assertFalse(parent.is_symlink() or parent.is_junction())
        spool._reject_reparse_chain(parent)
        self.root = parent / ("m5_scheduler_" + uuid.uuid4().hex)
        self.root.mkdir(exist_ok=False)
        (self.root / "NO_NATIVE_VERIFICATION.txt").write_text(PROVENANCE, encoding="ascii")

    def declaration(self):
        constructors = orchestration._declared_m5_constructor_identity()
        admission_path = Path(os.environ.get("FXPM_RESEARCH_ADMISSION_RECORD",
            str(Path(__file__).resolve().parent / "fixtures" / "m5_development" / "available_data_research_admission_001.json"))).resolve()
        admission_bytes = admission_path.read_bytes()
        self.assertEqual((len(admission_bytes), hashlib.sha256(admission_bytes).hexdigest()),
            (2016, scheduler._DECLARED_M5_ADMISSION_SHA))
        admission = json.loads(admission_bytes)
        stratum = {"timeframe": "H1", "instrument_session_class": "INVENTED_CLASS",
            "alignment_identity": "INVENTED_FIXED_GRID"}
        interval = {"start_utc": "2024-01-01T00:00:00Z", "end_exclusive_utc": "2024-01-01T02:00:00Z"}
        axes = []
        for view_id, purpose in (("slot-view", "FINANCIAL_INPUT"),
                ("calibration-view", "COST_EDITION_REPRODUCTION_ONLY"), ("account-view", "FINANCIAL_INPUT")):
            axes.append({"view_id": view_id, "symbol": "XAGUSD", "timeframe": "H1",
                "interval": dict(interval), "cutoff_utc": "2024-07-14T08:05:00+00:00",
                "recipe_sha256": scheduler._DECLARED_M5_RECIPE,
                "constructor_implementation_sha256": constructors["pm_explicit_derived_history.py"],
                "gap_findings_sha256": scheduler._DECLARED_M5_LINEAGE_SHA, "purpose": purpose, "source_kind": "m5_derived",
                "stratum": dict(stratum), "clock_template_sha256": None, "clock_axes_sha256": None,
                "spread_identification_sha256": "d" * 64})
        receipt = {"generation_id": "INVENTED_SOFTWARE_ROUTING_ONLY", "native_universe_sha256": "1" * 64,
            "config_sha256": "2" * 64, "fixture_provenance": PROVENANCE}
        identity = {"receipt_sha256": _sha(receipt), "generation_id": receipt["generation_id"],
            "native_universe_sha256": receipt["native_universe_sha256"], "plan_sha256": receipt["config_sha256"],
            "implementation_sha256": "3" * 64, "profile_sha256": scheduler._DECLARED_M5_PROFILE}
        source_pins = {"schema_version": orchestration.DECLARED_M5_SOURCE_PINS_SCHEMA,
            "source_kind": "m5_derived", "base_timeframe": "M5", "view_timeframe": "H1",
            "recipe_sha256": scheduler._DECLARED_M5_RECIPE, "constructor_identity": constructors,
            "view_plan_sha256": views.declared_view_plan_sha256(tuple(axes)),
            "gap_findings_sha256": scheduler._DECLARED_M5_LINEAGE_SHA, "cost_edition_identity": identity}
        row = {"root": str(self.root / "spool"), "run_id": "INVENTED_SCHEDULER_JOB",
            "cell": {"cell_id": "a" * 64, "symbol": "XAGUSD", "timeframe": "H1", "regime": "TREND"},
            "launch_activation_utc": "2024-07-14T08:05:00Z", "frozen_refit_clock": {},
            "protected_end_exclusive_utc": "2024-07-14T08:05:00Z", "extend_frozen_clock_backward": False,
            "source_access_window_policy": {"lead_seconds": 0, "validity_seconds": 172800},
            "m5_development": {"development_slot_ids": ["INVENTED_SLOT_1"], "view_axes": axes,
                "slot_view_ids": {"INVENTED_SLOT_1": "slot-view"}, "calibration_view_id": "calibration-view",
                "account_view_id": "account-view", "required_result_keys": ["INVENTED_SLOT_1", "terminal"],
                "terminal_result_key": "terminal", "m5_source_pins": source_pins,
                "cost_group_metadata": {"symbol": "XAGUSD", "timeframe": "H1", "cost_status": publication.COST_READY,
                    "source_kind": "m5_derived", "fine_source": {"fixture_provenance": PROVENANCE},
                    "financial_consumer_ready": False, "full_native_context_eligibility": False,
                    "spread_identification_sha256": "d" * 64, "available_interval": interval},
                "gap_provenance": {"status": "AVAILABLE_DATA_ONLY_RESEARCH_ADMISSION", "gap_status": "UNCLASSIFIED",
                    "admission_record": {"path": str(admission_path), "bytes": 2016, "sha256": scheduler._DECLARED_M5_ADMISSION_SHA},
                    "lineage": admission["lineage"], "gap_pattern_observations": admission["gap_pattern_observations"],
                    "control_identification_sha256": "d" * 64}, "end_grid_root": str(self.root / "end-grid"),
                "retirement": {"receipt_path": str(self.root / "retirement.json"), "max_paths": 100000,
                    "max_nodes": 200000, "max_bytes": 8 * 1024**3},
                "source_fence_path": str(self.root / "source-fence.json")}}
        return {"schema_version": scheduler.DECLARED_M5_DEVELOPMENT_SCHEDULER_SCHEMA,
            "native_universe_sha256": "1" * 64, "config_sha256": "2" * 64,
            "profile_sha256": scheduler._DECLARED_M5_PROFILE, "active_config_path": str(self.root / "config.json"),
            "cells": [row], "spool_limits": dict(scheduler._DECLARED_M5_LIMITS),
            "view_budgets": {name: 1024 for name in scheduler._DECLARED_M5_BUDGET_KEYS},
            "cost_edition_route": {"schema_version": "fxpm.recorded-derived-cost-route.v1",
                "metadata_kind": "ROUTING_ONLY_NOT_AUTHORITY", "root": str(self.root / "recorded-edition"),
                "receipt": receipt, "cost_edition_identity": identity}, "scope": dict(scheduler._DECLARED_M5_SCOPE)}

    def reject(self, declaration, code):
        with self.assertRaises(scheduler.HistoricalSchedulerError) as caught:
            scheduler._declared_m5_contract(declaration)
        self.assertEqual(caught.exception.code, code)

    def test_recorded_cost_status_uses_original_publication_enum(self):
        self.assertEqual(publication.COST_READY, "DERIVED_DATA_COST_READY_NOT_FINANCIAL_AUTHORITY")
        value = self.declaration()
        original = deepcopy(value["cells"][0]["m5_development"]["cost_group_metadata"])
        owned, _ = scheduler._declared_m5_contract(value)
        group = owned["cells"][0]["m5_development"]["cost_group_metadata"]
        self.assertEqual(group, original)
        self.assertIs(group["financial_consumer_ready"], False)
        self.assertIs(group["full_native_context_eligibility"], False)
        value["cells"][0]["m5_development"]["cost_group_metadata"]["cost_status"] = "COST_READY"
        self.reject(value, "DECLARED_M5_RECORDED_CELL_METADATA_INVALID")

    def test_declaration_is_detached_and_does_not_create_context(self):
        value = self.declaration()
        original = deepcopy(value)
        owned, groups = scheduler._declared_m5_contract(value)
        self.assertEqual(value, original)
        self.assertEqual(list(groups), ["XAGUSD"])
        owned["cells"][0]["m5_development"]["slot_view_ids"].clear()
        self.assertTrue(value["cells"][0]["m5_development"]["slot_view_ids"])
        self.assertFalse((self.root / "spool").exists())

    def test_limits_require_all_eight_exact_values_and_integer_types(self):
        for key in scheduler._DECLARED_M5_LIMITS:
            with self.subTest(key=key):
                value = self.declaration()
                del value["spool_limits"][key]
                self.reject(value, "DECLARED_M5_LIMITS_OR_SCOPE_INVALID")
        value = self.declaration()
        value["spool_limits"]["min_free_bytes"] = True
        self.reject(value, "DECLARED_M5_LIMITS_OR_SCOPE_INVALID")

    def test_view_budgets_reject_zero_and_bool(self):
        for bad in (0, True):
            value = self.declaration()
            value["view_budgets"]["max_source_rows"] = bad
            self.reject(value, "DECLARED_M5_LIMITS_OR_SCOPE_INVALID")

    def test_m5_window_is_fixed48h_without_backdated_lead(self):
        for policy in ({"lead_seconds": 1, "validity_seconds": 172800},
                {"lead_seconds": 0, "validity_seconds": 172801}):
            value = self.declaration()
            value["cells"][0]["source_access_window_policy"] = policy
            self.reject(value, "DECLARED_M5_WINDOW_POLICY_INVALID")

    def test_authority_false_flags_are_strict_bools(self):
        for bad in (True, 0):
            value = self.declaration()
            value["scope"]["order_authority"] = bad
            self.reject(value, "DECLARED_M5_LIMITS_OR_SCOPE_INVALID")

    def test_native_profile_cannot_substitute_for_recorded_edition(self):
        value = self.declaration()
        value["profile_sha256"] = "b" * 64
        self.reject(value, "DECLARED_M5_LIMITS_OR_SCOPE_INVALID")

    def test_selected_prefix_is_bounded_and_has_exact_view_map(self):
        value = self.declaration()
        value["cells"][0]["m5_development"]["development_slot_ids"] *= 4
        self.reject(value, "DECLARED_M5_DEVELOPMENT_PREFIX_INVALID")
        value = self.declaration()
        value["cells"][0]["m5_development"]["slot_view_ids"]["EXTRA"] = "extra"
        self.reject(value, "DECLARED_M5_DEVELOPMENT_PREFIX_INVALID")

    def test_result_roster_cannot_invent_calibration_or_future_owner(self):
        value = self.declaration()
        value["cells"][0]["m5_development"]["required_result_keys"].insert(1, "invented-calibration")
        self.reject(value, "DECLARED_M5_VIEW_ROSTER_INVALID")

    def test_axis_and_source_pin_hashes_must_join(self):
        value = self.declaration()
        value["cells"][0]["m5_development"]["m5_source_pins"]["view_plan_sha256"] = "0" * 64
        self.reject(value, "DECLARED_M5_SOURCE_PINS_MISMATCH")

    def test_clock_and_wrong_constructor_are_not_fixed_h1(self):
        value = self.declaration()
        value["cells"][0]["m5_development"]["view_axes"][0]["clock_axes_sha256"] = "e" * 64
        self.reject(value, "DECLARED_M5_VIEW_AXES_INVALID")
        value = self.declaration()
        dev = value["cells"][0]["m5_development"]
        dev["view_axes"][0]["constructor_implementation_sha256"] = "e" * 64
        dev["m5_source_pins"]["view_plan_sha256"] = views.declared_view_plan_sha256(tuple(dev["view_axes"]))
        self.reject(value, "DECLARED_M5_SOURCE_PINS_MISMATCH")

    def test_missing_research_admission_stops_before_context_or_payload_access(self):
        owned, _ = scheduler._declared_m5_contract(self.declaration())
        owned["cells"][0]["m5_development"]["gap_provenance"]["status"] = "TARGET_GAPS_UNEXAMINED"
        row = scheduler._declared_m5_run_one_cell(owned, owned["cells"][0])
        self.assertEqual((row["status"], row["reason_code"]), ("FAILED", "DECLARED_M5_GAP_PROVENANCE_INVALID"))
        self.assertFalse((self.root / "spool").exists())
        self.assertIsNone(row["precommit_sha256"])
        self.assertIsNone(row["run_receipt"])

    def test_control_comparison_is_not_a_target_gap_pass(self):
        value = self.declaration()
        value["cells"][0]["m5_development"]["gap_provenance"]["status"] = "PASS"
        self.reject(value, "DECLARED_M5_GAP_PROVENANCE_INVALID")

    def test_external_final_paths_cannot_alias_or_enter_spool(self):
        value = self.declaration()
        dev = value["cells"][0]["m5_development"]
        dev["end_grid_root"] = str(self.root / "spool" / "grid.json")
        self.reject(value, "DECLARED_M5_FINAL_PATH_INVALID")
        value = self.declaration()
        dev = value["cells"][0]["m5_development"]
        dev["source_fence_path"] = dev["end_grid_root"]
        self.reject(value, "DECLARED_M5_FINAL_PATH_INVALID")

    def test_cell_contract_digest_binds_late_account_and_terminal_plan(self):
        value, _ = scheduler._declared_m5_contract(self.declaration())
        before = scheduler._declared_m5_cell_contract_sha256(value, value["cells"][0])
        row = deepcopy(value["cells"][0])
        row["m5_development"]["account_view_id"] = "different-account"
        self.assertNotEqual(before, scheduler._declared_m5_cell_contract_sha256(value, row))

    def test_archived_projection_drops_working_receipts_and_denies_cold_reopen(self):
        value, _ = scheduler._declared_m5_contract(self.declaration())
        expected = value["cells"][0]
        row = scheduler._declared_cell_row(expected, "COMPLETED", pid=17)
        row.update(source_kind="m5_derived", run_receipt={"fixture_provenance": PROVENANCE},
            account_descriptor={"fixture_provenance": PROVENANCE})
        recording = {"generation_context_sha256": "f" * 64,
            "retirement_receipt_path": expected["m5_development"]["retirement"]["receipt_path"],
            "retirement_receipt_sha256": "e" * 64, "precommit_sha256": "b" * 64,
            "source_fence": {"path": str(self.root / "source-fence.json"), "sha256": "c" * 64},
            "end_grid_rows": {"fixture_provenance": PROVENANCE}}
        actual = scheduler._declared_m5_archived_row(row, recording)
        self.assertEqual(actual["status"], "ARCHIVED")
        self.assertIsNone(actual["run_receipt"])
        self.assertIsNone(actual["account_descriptor"])
        self.assertIs(actual["genuine_cold_reopen_available"], False)
        self.assertEqual(actual["descriptor_assurance"], "IDENTITY_ONLY_AFTER_WORKSPACE_RELEASE")
        worker_result = {"schema_version": scheduler.DECLARED_SCHEDULER_SCHEMA,
            "symbol": "XAGUSD", "cells": [actual], "closes_admission": False}
        scheduler._declared_validate_result(worker_result, [expected], "XAGUSD", worker_pid=17)
        actual["account_descriptor"] = {"fixture_provenance": PROVENANCE}
        with self.assertRaisesRegex(scheduler.HistoricalSchedulerError, "DECLARED_M5_ARCHIVED_RESULT_INVALID"):
            scheduler._declared_validate_result(worker_result, [expected], "XAGUSD", worker_pid=17)

    def test_external_record_is_exclusive_exact_and_retained(self):
        path = self.root / "software-result.json"
        body = {"fixture_provenance": PROVENANCE, "result": "NO_NATIVE_VERIFICATION"}
        first = scheduler._declared_m5_external_record(path, body, maximum_bytes=4096)
        second = scheduler._declared_m5_external_record(path, body, maximum_bytes=4096)
        self.assertIs(first["written"], True)
        self.assertIs(second["written"], False)
        self.assertEqual(first["sha256"], second["readback_sha256"])
        with self.assertRaises(scheduler.HistoricalSchedulerError) as caught:
            scheduler._declared_m5_external_record(path, {**body, "result": "changed"}, maximum_bytes=4096)
        self.assertEqual(caught.exception.code, "DECLARED_M5_FINAL_RECORD_CHANGED")
        self.assertEqual(json.loads(path.read_bytes()), body)

    def test_external_record_refuses_bound_before_creation(self):
        path = self.root / "too-large.json"
        with self.assertRaises(scheduler.HistoricalSchedulerError) as caught:
            scheduler._declared_m5_external_record(path, {"fixture_provenance": PROVENANCE}, maximum_bytes=1)
        self.assertEqual(caught.exception.code, "DECLARED_M5_FINAL_RECORD_BOUND")
        self.assertFalse(path.exists())

    def test_real_grid_writer_boundary_normalizes_without_rewriting_source_clocks(self):
        import pm_end_grid as grid
        recorded = {"start": "2024-01-02T00:00:00Z", "end": "2024-01-03T00:00:00Z",
            "cutoff": "2024-01-01T23:00:00Z", "produced": "2024-01-03T01:00:00Z"}
        original = deepcopy(recorded)
        row = grid.build_end_grid_row(symbol="XAGUSD", timeframe="H1", regime="TREND",
            cycle_start_utc=scheduler._declared_m5_grid_utc(recorded["start"]),
            cycle_end_utc=scheduler._declared_m5_grid_utc(recorded["end"]),
            information_cutoff_utc=scheduler._declared_m5_grid_utc(recorded["cutoff"]),
            produced_at_utc=scheduler._declared_m5_grid_utc(recorded["produced"]),
            outcome="STOP", reason_code="INVENTED_SOFTWARE_CLOCK_BOUNDARY", source_kind="m5_derived",
            source_contract_sha256=None, view_key=None, source_identity={}, cost_tier="NONE",
            process_edition_sha256="a" * 64, code_sha256={"software-fixture": "b" * 64}, metrics={})
        self.assertEqual(grid.row_from_dict(row.to_dict()).to_dict(), row.to_dict())
        self.assertEqual(row.cycle_start_utc, "2024-01-02T00:00:00+00:00")
        self.assertEqual(row.cycle_end_utc, "2024-01-03T00:00:00+00:00")
        self.assertEqual(row.information_cutoff_utc, "2024-01-01T23:00:00+00:00")
        self.assertEqual(row.produced_at_utc, "2024-01-03T01:00:00+00:00")
        self.assertEqual(recorded, original)
        self.assertEqual(scheduler._declared_m5_grid_utc(row.cycle_start_utc), row.cycle_start_utc)
        for bad in ("2024-01-01T00:00:00", "2024-01-01T00:00:00+01:00",
                "2024-01-01T00:00:00.1Z", "2024-01-01T00:00:00.000Z",
                "2024-13-01T00:00:00Z", "2024-01-01T00:00:00Z "):
            with self.subTest(bad=bad), self.assertRaisesRegex(scheduler.HistoricalSchedulerError,
                    "DECLARED_M5_GRID_TIMESTAMP_INVALID"):
                scheduler._declared_m5_grid_utc(bad)

    def test_no_plan_shape_is_narrow_and_does_not_grant_verification(self):
        # Ordinary invented routing metadata, not a public-verifier result.
        no_plan = {"status": "NO_TRADE", "reason_code": "REGIME_UNOBSERVED",
            "plan_transport": None, "horizon_plan": None, "owner": None}
        self.assertTrue(scheduler._declared_m5_no_plan_shape(no_plan))
        for changes in ({"reason_code": "NO_PROCESS_WINNER"}, {"plan_transport": {}},
                {"horizon_plan": {}}, {"owner": {}}, {"status": "STOP"}):
            self.assertFalse(scheduler._declared_m5_no_plan_shape({**no_plan, **changes}))
        self.assertFalse(scheduler._declared_m5_no_plan_shape({"status": "NO_TRADE",
            "reason_code": "REGIME_UNOBSERVED"}))
        self.assertIsNone(no_plan["plan_transport"])
        self.assertIsNone(no_plan["horizon_plan"])

    def test_utc_comparison_preserves_instants_and_refuses_non_utc(self):
        self.assertEqual(scheduler._declared_m5_utc("2024-01-01T00:00:00Z"),
            scheduler._declared_m5_utc("2024-01-01T00:00:00+00:00"))
        for bad in ("2024-01-01T00:00:00", "2024-01-01T00:00:00+01:00", "2024-01-01T00:00:00.1Z"):
            with self.subTest(bad=bad), self.assertRaises(scheduler.HistoricalSchedulerError):
                scheduler._declared_m5_utc(bad)


if __name__ == "__main__":
    unittest.main()
