"""Development-account metadata joins: SYNTHETIC_NO_NATIVE_VERIFICATION.

These invented values exercise actual pure helper bodies and original roster
math. They prove no issuer, saved OWNER, native prefix, account replay or public
verifier acceptance. No product module is replaced by a successful stub.
"""
import ast
from copy import deepcopy
import hashlib
import json
from pathlib import Path
import re
import sys
from types import SimpleNamespace
from typing import Any, Final, Sequence
import unittest

import pandas as pd


_CANDIDATE = Path(__file__).resolve().parents[1]
_REPO = _CANDIDATE
for _path in (_REPO,):
    sys.path.insert(0, str(_path))

import pm_historical_anchor_orchestration as historical
import pm_m5_job_view as job
import pm_production_anchor_plan as anchor

if Path(historical.__file__).resolve() != _CANDIDATE / "pm_historical_anchor_orchestration.py":
    raise RuntimeError("requires the candidate orchestration source")
if Path(job.__file__).resolve() != _CANDIDATE / "pm_m5_job_view.py":
    raise RuntimeError("requires the candidate job-view source")
if Path(anchor.__file__).resolve() != _REPO / "pm_production_anchor_plan.py":
    raise RuntimeError("requires the original anchor mathematics")

PROVENANCE = "SYNTHETIC_NO_NATIVE_VERIFICATION_NOT_AN_ISSUED_OR_SAVED_ACCOUNT"


def _digest(value):
    encoded = json.dumps(value, ensure_ascii=True, allow_nan=False,
        sort_keys=True, separators=(",", ":")).encode("ascii")
    return hashlib.sha256(encoded).hexdigest()


def _pin(label):
    return hashlib.sha256((PROVENANCE + ":" + label).encode("ascii")).hexdigest()


def _forward_helpers():
    """Load only real pure definitions; their collaborators remain real modules."""
    path = _CANDIDATE / "pm_forward_oos_replay_producer.py"
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    names = {"ForwardOOSReplayProductionError", "_fail", "_canonical_bytes", "_sha", "_hex",
        "_timestamp", "_declared_cell_measurement", "_declared_development_cell_measurement",
        "_declared_m5_account_consumers"}
    nodes = [node for node in tree.body if isinstance(node, (ast.ClassDef, ast.FunctionDef))
        and node.name in names]
    if {node.name for node in nodes} != names:
        raise RuntimeError("actual pure account helper definitions are missing")
    constants = [node for node in tree.body if isinstance(node, ast.AnnAssign)
        and isinstance(node.target, ast.Name) and node.target.id in {"_HEX64", "_MAX_JSON_BYTES"}]
    if {node.target.id for node in constants} != {"_HEX64", "_MAX_JSON_BYTES"}:
        raise RuntimeError("actual strict-hex/canonical JSON constants are missing")
    future = ast.ImportFrom(module="__future__", names=[ast.alias(name="annotations")], level=0)
    selected = ast.fix_missing_locations(ast.Module(body=[future, *constants, *nodes], type_ignores=[]))
    scope = {"__name__": "synthetic_development_account_helpers", "historical": historical,
        "pd": pd, "hashlib": hashlib, "json": json, "re": re,
        "Any": Any, "Final": Final, "Sequence": Sequence}
    exec(compile(selected, str(path), "exec"), scope)
    return SimpleNamespace(**scope)


def _parent(k):
    launch = "2023-06-24T00:00:00Z"
    roster = anchor.derive_declared_owner_slot_roster_v2(
        launch_activation_utc=launch,
        frozen_refit_clock={"anchor_activation_utc": "2026-03-29T00:01:00Z", "interval_calendar_days": 28},
        config_identity_refs={"config_sha256": _pin("config"), "effective_config_sha256": _pin("effective")},
        extend_frozen_clock_backward=True, protected_end_exclusive_utc="2024-07-14T08:05:00Z")
    scope = historical._declared_development_scope_body(roster,
        [row["slot_id"] for row in roster["slots"][:k]])
    return {"schema_version": historical.DECLARED_M5_DEVELOPMENT_SLOT_PRECOMMIT_SCHEMA,
        "source_kind": "m5_derived", "launch_activation_utc": launch,
        "slot_roster": roster, "slot_roster_sha256": _digest(roster),
        "development_scope": scope, "development_scope_sha256": _digest(scope),
        "cells": [{"cell_id": _pin("cell"), "symbol": "EURUSD", "timeframe": "H1", "regime": "TREND"}],
        "fixture_provenance": PROVENANCE}


def _inputs(statuses):
    parent = _parent(len(statuses))
    schedule, records = [], []
    for slot, status in zip(historical.declared_slot_selected_slots(parent), statuses, strict=True):
        owner_id = _pin("owner-id:" + slot["slot_id"]) if status == "OWNER" else None
        schedule.append({"slot_id": slot["slot_id"], "ordinal": slot["ordinal"],
            "start_utc": slot["activation_utc"], "end_exclusive_utc": slot["end_exclusive_utc"],
            "status": status, "owner_id": owner_id})
        if status == "OWNER":
            records.append({"owner": {"schema_version": "fxpm.declared-m5-slot-owner.v1",
                "cell_id": parent["cells"][0]["cell_id"], "slot_id": slot["slot_id"],
                "owner_id": owner_id, "source_contract_sha256": _pin("trade:" + slot["slot_id"]),
                "fixture_provenance": PROVENANCE}, "source_count_receipt": None})
    run = {"schema_version": "fxpm.declared-development-cell-owner-run.v1",
        "precommit_sha256": _pin("precommit"), "cell_id": parent["cells"][0]["cell_id"],
        "declared_slots": len(statuses), "completed_slots": len(statuses), "terminal_stop": False,
        "slot_commits": [{"slot_id": row["slot_id"], "status": row["status"]} for row in schedule],
        "fixture_provenance": PROVENANCE}
    return {"parent": parent, "progress_status": "DEVELOPMENT_COMPLETE",
        "progress": SimpleNamespace(run_receipt=run), "slot_schedule": schedule,
        "owner_records": records, "commits": [], "fixture_provenance": PROVENANCE}


class DeclaredDevelopmentAccountTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.actual = _forward_helpers()

    def assert_refusal(self, code, inputs):
        with self.assertRaises(self.actual.ForwardOOSReplayProductionError) as caught:
            self.actual._declared_m5_account_consumers(inputs, precommit_sha256=_pin("precommit"))
        self.assertEqual(caught.exception.code, code)

    def test_development_measurement_uses_selected_original_geometry_and_fixed_policy(self):
        for k in (1, 3):
            with self.subTest(k=k):
                parent = _parent(k)
                selected = historical.declared_slot_selected_slots(parent)
                result = self.actual._declared_development_cell_measurement(parent)
                self.assertEqual(len(parent["slot_roster"]["slots"]), 13)
                self.assertEqual(result, {"result_kind": "DEVELOPMENT_PARTIAL", "calendar_days": 28*k,
                    "refit_cycles": k, "cycle_days": 28, "risk_per_trade_pct": 0.25,
                    "terminal_policy": "MARK_OPEN", "start_utc": selected[0]["activation_utc"],
                    "end_exclusive_utc": selected[-1]["end_exclusive_utc"],
                    "development_scope_sha256": parent["development_scope_sha256"],
                    "design_contaminated": True, "pristine_validation": False})

    def test_native_measurement_remains_full13_and_rejects_shortened_roster(self):
        parent = _parent(1)
        native = self.actual._declared_cell_measurement(parent)
        self.assertEqual((native["calendar_days"], native["refit_cycles"]), (364, 13))
        partial = deepcopy(parent)
        partial["slot_roster"]["slots"] = partial["slot_roster"]["slots"][:1]
        with self.assertRaises(self.actual.ForwardOOSReplayProductionError) as caught:
            self.actual._declared_cell_measurement(partial)
        self.assertEqual(caught.exception.code, "DECLARED_CELL_H364_GEOMETRY_INVALID")

    def test_development_measurement_keeps_actual_prefix_and_full_roster_validation(self):
        for change in ("nonprefix", "altered_geometry", "bad_scope_hash"):
            with self.subTest(change=change):
                parent = _parent(2)
                if change == "nonprefix":
                    parent["development_scope"]["selected_slot_ids"] = [
                        row["slot_id"] for row in parent["slot_roster"]["slots"][1:3]]
                    parent["development_scope_sha256"] = _digest(parent["development_scope"])
                elif change == "altered_geometry":
                    parent["slot_roster"]["slots"][0]["end_exclusive_utc"] = "2023-07-21T00:01:00Z"
                    parent["slot_roster_sha256"] = _digest(parent["slot_roster"])
                else:
                    parent["development_scope_sha256"] = _pin("different-scope")
                with self.assertRaises(historical.HistoricalAnchorOrchestrationError):
                    self.actual._declared_development_cell_measurement(parent)

    def test_mixed_prefix_uses_stored_owner_body_hash_and_real_trade_contract_field(self):
        inputs = _inputs(("NO_TRADE", "OWNER", "NO_TRADE"))
        result = self.actual._declared_m5_account_consumers(inputs, precommit_sha256=_pin("precommit"))
        owner = inputs["owner_records"][0]["owner"]
        self.assertEqual(len(result), 1)
        self.assertIs(type(result[0]), job.AccountViewBinding)
        self.assertEqual(result[0].saved_owner_sha256, _digest(owner))
        self.assertNotEqual(result[0].saved_owner_sha256, owner["owner_id"])
        self.assertEqual(result[0].source_contract_sha256, owner["source_contract_sha256"])
        self.assertEqual(result[0].measurement_end_utc, inputs["slot_schedule"][-1]["end_exclusive_utc"])
        self.assertEqual(result[0].measurement_purpose, "DEVELOPMENT_PARTIAL_ACCOUNT")

    def test_each_owner_in_prefix_gets_its_own_binding_in_slot_order(self):
        inputs = _inputs(("OWNER", "OWNER"))
        inputs["owner_records"].reverse()
        result = self.actual._declared_m5_account_consumers(inputs, precommit_sha256=_pin("precommit"))
        by_slot = {row["owner"]["slot_id"]: row["owner"] for row in inputs["owner_records"]}
        self.assertEqual(tuple(value.saved_owner_sha256 for value in result),
            tuple(_digest(by_slot[row["slot_id"]]) for row in inputs["slot_schedule"]))

    def test_all_no_trade_binds_actual_run_and_precommit_without_owner_aliases(self):
        inputs = _inputs(("NO_TRADE", "NO_TRADE", "NO_TRADE"))
        result = self.actual._declared_m5_account_consumers(inputs, precommit_sha256=_pin("precommit"))
        self.assertEqual(len(result), 1)
        self.assertIs(type(result[0]), job.NoTradeAccountViewBinding)
        self.assertEqual(result[0].no_trade_run_receipt_sha256, _digest(inputs["progress"].run_receipt))
        self.assertEqual(result[0].precommit_sha256, _pin("precommit"))
        self.assertEqual(result[0].measurement_end_utc, inputs["slot_schedule"][-1]["end_exclusive_utc"])
        self.assertFalse(hasattr(result[0], "saved_owner_sha256"))
        self.assertFalse(hasattr(result[0], "source_contract_sha256"))

    def test_incomplete_stop_missing_or_wrong_selected_schedule_cannot_be_flat_account(self):
        for change in ("progress", "short", "STOP", "MISSING", "slot_id", "ordinal"):
            with self.subTest(change=change):
                inputs = _inputs(("NO_TRADE", "NO_TRADE"))
                if change == "progress":
                    inputs["progress_status"] = "RUNNING"
                elif change == "short":
                    inputs["slot_schedule"].pop()
                elif change in ("STOP", "MISSING"):
                    inputs["slot_schedule"][0]["status"] = change
                elif change == "slot_id":
                    inputs["slot_schedule"][0]["slot_id"] = "not-the-selected-slot"
                else:
                    inputs["slot_schedule"][0]["ordinal"] = 2
                self.assert_refusal("DECLARED_DEVELOPMENT_ACCOUNT_INPUTS_INCOMPLETE", inputs)

    def test_missing_or_wrong_run_cannot_support_account_consumer_binding(self):
        inputs = _inputs(("NO_TRADE",))
        inputs["progress"].run_receipt = None
        self.assert_refusal("DECLARED_DEVELOPMENT_RUN_RECEIPT_REQUIRED", inputs)
        changes = ({"schema_version": "fxpm.declared-cell-owner-run.v1"},
            {"precommit_sha256": _pin("other-precommit")}, {"cell_id": _pin("other-cell")},
            {"declared_slots": True}, {"completed_slots": 0}, {"terminal_stop": True},
            {"slot_commits": []})
        for fields in changes:
            with self.subTest(fields=fields):
                inputs = _inputs(("NO_TRADE",))
                inputs["progress"].run_receipt.update(fields)
                self.assert_refusal("DECLARED_DEVELOPMENT_RUN_BINDING_MISMATCH", inputs)
        inputs = _inputs(("NO_TRADE",))
        inputs["progress"].run_receipt["precommit_sha256"] = "not-a-hash"
        with self.assertRaises(self.actual.ForwardOOSReplayProductionError) as caught:
            self.actual._declared_m5_account_consumers(inputs, precommit_sha256="not-a-hash")
        self.assertEqual(caught.exception.code, "DECLARED_DEVELOPMENT_RUN_BINDING_MISMATCH")

    def test_legacy_or_mismatched_owner_and_missing_trade_hash_are_refused(self):
        for change in ("legacy", "cell", "slot", "owner", "missing_contract", "bad_contract", "duplicate", "absent"):
            with self.subTest(change=change):
                inputs = _inputs(("OWNER",))
                owner = inputs["owner_records"][0]["owner"]
                if change == "legacy":
                    owner["schema_version"] = "fxpm.declared-slot-owner.v1"
                elif change == "cell":
                    owner["cell_id"] = _pin("other-cell")
                elif change == "slot":
                    owner["slot_id"] = "wrong-slot"
                elif change == "owner":
                    owner["owner_id"] = _pin("wrong-owner")
                elif change == "missing_contract":
                    owner.pop("source_contract_sha256")
                elif change == "bad_contract":
                    owner["source_contract_sha256"] = True
                elif change == "duplicate":
                    inputs["owner_records"].append(deepcopy(inputs["owner_records"][0]))
                else:
                    inputs["owner_records"].clear()
                self.assert_refusal("DECLARED_M5_ACCOUNT_OWNER_BINDING_INVALID", inputs)

    def test_all_no_trade_rejects_any_owner_record_instead_of_fabricating_empty_owner(self):
        inputs = _inputs(("NO_TRADE",))
        inputs["owner_records"] = _inputs(("OWNER",))["owner_records"]
        self.assert_refusal("DECLARED_M5_ACCOUNT_OWNER_BINDING_INVALID", inputs)


if __name__ == "__main__":
    unittest.main()
