"""Declared M5 pin/schema unit cases; synthetic pins confer no source authority."""
from copy import deepcopy
import hashlib
import json
from pathlib import Path
import sys
import unittest
from unittest.mock import patch

_CANDIDATE = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_CANDIDATE))
import pm_historical_anchor_orchestration as o
if Path(o.__file__).resolve() != _CANDIDATE / "pm_historical_anchor_orchestration.py":
    raise RuntimeError("M5_SCHEMA_TEST_REQUIRES_CANDIDATE")


def _pins():
    return {
        "schema_version": o.DECLARED_M5_SOURCE_PINS_SCHEMA,
        "source_kind": "m5_derived", "base_timeframe": "M5", "view_timeframe": "H1",
        "recipe_sha256": "85e321bc794c59cf26e725bbb7772b2590a7b0256e2f6e6086ee3bf52189c1f4",
        "constructor_identity": {name: "1" * 64 for name in o._DECLARED_M5_CONSTRUCTORS},
        "view_plan_sha256": "2" * 64, "gap_findings_sha256": "3" * 64,
        "cost_edition_identity": {"receipt_sha256": "4" * 64,
            "generation_id": "SYNTHETIC_SCHEMA_ONLY_NO_NATIVE_VERIFICATION",
            "native_universe_sha256": "5" * 64, "plan_sha256": "6" * 64,
            "implementation_sha256": "7" * 64, "profile_sha256": "8" * 64},
    }


class DeclaredM5SourceSchemaTests(unittest.TestCase):
    def validate(self, pins):
        with patch.object(o, "_declared_m5_constructor_identity",
                          return_value={name: "1" * 64 for name in o._DECLARED_M5_CONSTRUCTORS}):
            return o._validate_declared_m5_source_pins(pins, {"timeframe": "H1"})

    def test_old_schemas_are_implicitly_native_and_reject_m5_additions(self):
        for schema in (o.DECLARED_SLOT_PRECOMMIT_SCHEMA, o.DECLARED_SLOT_REQUEST_SCHEMA,
                       o.DECLARED_SLOT_PRECOMMIT_SCHEMA_V2, o.DECLARED_SLOT_REQUEST_SCHEMA_V2):
            with self.subTest(schema=schema):
                self.assertEqual(o.declared_slot_source_kind({"schema_version": schema}), "broker_native")
                for key, value in (("source_kind", "m5_derived"), ("m5_source_pins", {}),
                                   ("m5_view_contract_sha256", "1" * 64)):
                    with self.assertRaisesRegex(o.HistoricalAnchorOrchestrationError,
                                                "DECLARED_SLOT_SOURCE_KIND_INVALID"):
                        o.declared_slot_source_kind({"schema_version": schema, key: value})

    def test_new_schemas_are_explicit_m5_v16_only(self):
        for schema in (o.DECLARED_M5_SLOT_PRECOMMIT_SCHEMA, o.DECLARED_M5_SLOT_REQUEST_SCHEMA):
            body = {"schema_version": schema, "source_kind": "m5_derived",
                    "winner_transport_edition": o.DECLARED_WINNER_TRANSPORT_V16}
            self.assertEqual(o.declared_slot_source_kind(body), "m5_derived")
            self.assertEqual(o.declared_slot_transport_edition(body), o.DECLARED_WINNER_TRANSPORT_V16)
            body["winner_transport_edition"] = o.DECLARED_WINNER_TRANSPORT_V15
            with self.assertRaisesRegex(o.HistoricalAnchorOrchestrationError,
                                        "DECLARED_SLOT_TRANSPORT_EDITION_INVALID"):
                o.declared_slot_transport_edition(body)

    def test_new_schema_rejects_native_and_control_clock_stays_typed_stop(self):
        for kind, code in (("broker_native", "DECLARED_SLOT_SOURCE_KIND_INVALID"),
                           ("m5_derived_control_clock.v1", "DECLARED_SLOT_M5_CONTROL_CLOCK_UNAVAILABLE")):
            with self.assertRaisesRegex(o.HistoricalAnchorOrchestrationError, code):
                o.declared_slot_source_kind({"schema_version": o.DECLARED_M5_SLOT_REQUEST_SCHEMA,
                                             "source_kind": kind})

    def test_declared_digest_binds_exact_pins_without_mutation(self):
        pins = _pins(); before = deepcopy(pins)
        digest = self.validate(pins)
        self.assertEqual(digest, hashlib.sha256(o._declared_json(pins)).hexdigest())
        self.assertEqual(pins, before)
        self.assertNotIn("source_contract_sha256", pins)

    def test_observed_facts_cannot_be_inserted_into_pre_access_pins(self):
        for key in ("source_accessed_at_utc", "native_prefix_index_sha256", "observed_gap_intervals",
                    "view_payload_sha256", "cost_status", "source_contract_sha256"):
            pins = _pins(); pins[key] = "OBSERVED_NOT_DECLARED"
            with self.subTest(key=key), self.assertRaisesRegex(
                    o.HistoricalAnchorOrchestrationError, "DECLARED_SLOT_M5_PINS_INVALID"):
                self.validate(pins)

    def test_constructor_code_change_is_a_binding_refusal(self):
        pins = _pins(); pins["constructor_identity"]["pm_m5_job_view.py"] = "9" * 64
        with self.assertRaisesRegex(o.HistoricalAnchorOrchestrationError,
                                    "DECLARED_SLOT_M5_CONSTRUCTOR_BINDING_MISMATCH"):
            self.validate(pins)

    def test_timeframe_recipe_or_bad_digest_cannot_reuse_pins(self):
        for key, value in (("view_timeframe", "M15"), ("base_timeframe", "H1"),
                           ("recipe_sha256", "0" * 64), ("gap_findings_sha256", "bad")):
            pins = _pins(); pins[key] = value
            with self.subTest(key=key), self.assertRaises(o.HistoricalAnchorOrchestrationError):
                self.validate(pins)

    def test_cost_identity_is_recorded_exactly_and_schema_helpers_preserve_native(self):
        pins = _pins(); pins["cost_edition_identity"]["current_tree_sha256"] = "9" * 64
        with self.assertRaisesRegex(o.HistoricalAnchorOrchestrationError,
                                    "DECLARED_SLOT_M5_COST_EDITION_INVALID"):
            self.validate(pins)
        self.assertEqual(o._declared_precommit_schema(o.DECLARED_WINNER_TRANSPORT_V15, "broker_native"),
                         o.DECLARED_SLOT_PRECOMMIT_SCHEMA)
        self.assertEqual(o._declared_request_schema(o.DECLARED_WINNER_TRANSPORT_V16, "broker_native"),
                         o.DECLARED_SLOT_REQUEST_SCHEMA_V2)
        self.assertEqual(o._declared_precommit_schema(o.DECLARED_WINNER_TRANSPORT_V16, "m5_derived"),
                         o.DECLARED_M5_SLOT_PRECOMMIT_SCHEMA)
        with self.assertRaises(o.HistoricalAnchorOrchestrationError):
            o._declared_request_schema(o.DECLARED_WINNER_TRANSPORT_V15, "m5_derived")


    def test_m5_profile_identity_is_the_recorded_derived_cost_profile(self):
        pins = _pins()
        expected = {"profile_sha256": pins["cost_edition_identity"]["profile_sha256"]}
        self.assertIsNone(o._validate_declared_m5_profile_binding(pins, expected))
        for value in ({"profile_sha256": "9" * 64},
                      {**expected, "source_accessed_at_utc": "NOT_DECLARED_PROFILE_IDENTITY"}, None):
            with self.subTest(value=value), self.assertRaisesRegex(
                    o.HistoricalAnchorOrchestrationError, "DECLARED_SLOT_M5_PROFILE_BINDING_MISMATCH"):
                o._validate_declared_m5_profile_binding(pins, value)


def _development_parent(count=1):
    import pm_production_anchor_plan as anchors
    launch = "2023-06-24T00:00:00Z"
    roster = anchors.derive_declared_owner_slot_roster_v2(
        launch_activation_utc=launch,
        frozen_refit_clock={"anchor_activation_utc": "2026-03-29T00:01:00Z", "interval_calendar_days": 28},
        config_identity_refs={"config_sha256": "1" * 64, "effective_config_sha256": "2" * 64},
        extend_frozen_clock_backward=True, protected_end_exclusive_utc="2024-07-14T08:05:00Z")
    scope = o._declared_development_scope_body(roster, [slot["slot_id"] for slot in roster["slots"][:count]])
    return {"schema_version": o.DECLARED_M5_DEVELOPMENT_SLOT_PRECOMMIT_SCHEMA,
        "source_kind": "m5_derived", "winner_transport_edition": o.DECLARED_WINNER_TRANSPORT_V16,
        "slot_roster": roster, "slot_roster_sha256": hashlib.sha256(o._declared_json(roster)).hexdigest(),
        "launch_activation_utc": launch, "cells": [{"symbol": "XAGUSD"}],
        "development_scope": scope, "development_scope_sha256": hashlib.sha256(o._declared_json(scope)).hexdigest()}


def _rehash_development(parent):
    parent["development_scope_sha256"] = hashlib.sha256(o._declared_json(parent["development_scope"])).hexdigest()


class DeclaredDevelopmentScopeTests(unittest.TestCase):
    """Invented structural metadata only; no request issuance or native verification."""
    def test_prefix_one_to_three_preserves_the_accepted_full_roster(self):
        for count in (1, 2, 3):
            parent = _development_parent(count); before = deepcopy(parent)
            slots = o.declared_slot_selected_slots(parent)
            self.assertEqual(slots, tuple(parent["slot_roster"]["slots"][:count]))
            self.assertEqual(len(parent["slot_roster"]["slots"]), 13)
            self.assertEqual(parent, before)
            scope = o.declared_slot_development_scope(parent)
            self.assertIs(scope["design_contaminated"], True)
            self.assertIs(scope["pristine_validation"], False)
            for key in ("qualification_authority", "capital_authority", "live_authority", "order_authority"):
                self.assertIs(scope[key], False)

    def test_foreign_duplicate_too_many_and_nonprefix_slots_refuse(self):
        parent = _development_parent(); slots = parent["slot_roster"]["slots"]
        cases = ([], ["foreign"], [slots[0]["slot_id"]] * 2,
                 [slot["slot_id"] for slot in slots[:4]], [slots[1]["slot_id"]],
                 [slots[0]["slot_id"], slots[2]["slot_id"]], "not-a-list", [1])
        for selected in cases:
            with self.subTest(selected=selected), self.assertRaisesRegex(
                    o.HistoricalAnchorOrchestrationError, "DECLARED_DEVELOPMENT_PREFIX_INVALID"):
                o._declared_development_scope_body(parent["slot_roster"], selected)

    def test_nonadopted_symbol_and_changed_adopted_set_refuse(self):
        parent = _development_parent(); parent["cells"][0]["symbol"] = "AUDUSD"
        with self.assertRaisesRegex(o.HistoricalAnchorOrchestrationError, "DECLARED_DEVELOPMENT_SYMBOL_NOT_ADOPTED"):
            o.declared_slot_selected_slots(parent)
        parent = _development_parent(); parent["development_scope"]["adopted_symbols"][0] = "AUDUSD"
        _rehash_development(parent)
        with self.assertRaisesRegex(o.HistoricalAnchorOrchestrationError, "DECLARED_DEVELOPMENT_SCOPE_INVALID"):
            o.declared_slot_development_scope(parent)

    def test_scope_hash_semantics_and_false_flags_are_exact(self):
        parent = _development_parent(); parent["development_scope_sha256"] = "f" * 64
        with self.assertRaisesRegex(o.HistoricalAnchorOrchestrationError, "DECLARED_DEVELOPMENT_SCOPE_INVALID"):
            o.declared_slot_development_scope(parent)
        for key, value in (("design_contaminated", 1), ("pristine_validation", True),
                           ("selection_policy", "ARBITRARY_SUBSET"), ("qualification_authority", True),
                           ("capital_authority", 0), ("live_authority", True), ("order_authority", True)):
            parent = _development_parent(); parent["development_scope"][key] = value; _rehash_development(parent)
            with self.subTest(key=key), self.assertRaisesRegex(o.HistoricalAnchorOrchestrationError,
                                                            "DECLARED_DEVELOPMENT_SCOPE_INVALID"):
                o.declared_slot_development_scope(parent)

    def test_legacy_schemas_cannot_smuggle_development_scope(self):
        for schema in (o.DECLARED_SLOT_PRECOMMIT_SCHEMA, o.DECLARED_SLOT_REQUEST_SCHEMA,
                       o.DECLARED_SLOT_PRECOMMIT_SCHEMA_V2, o.DECLARED_SLOT_REQUEST_SCHEMA_V2,
                       o.DECLARED_M5_SLOT_PRECOMMIT_SCHEMA, o.DECLARED_M5_SLOT_REQUEST_SCHEMA):
            parent = _development_parent(); parent["schema_version"] = schema
            with self.subTest(schema=schema), self.assertRaisesRegex(o.HistoricalAnchorOrchestrationError,
                                                                   "DECLARED_DEVELOPMENT_SCOPE_INVALID"):
                o.declared_slot_development_scope(parent)

    def test_shortening_legacy_full13_roster_still_refuses(self):
        parent = _development_parent()
        for count in (1, 3, 12):
            roster = deepcopy(parent["slot_roster"]); roster["slots"] = roster["slots"][:count]
            roster["slot_count"] = count
            with self.subTest(count=count), self.assertRaisesRegex(o.HistoricalAnchorOrchestrationError,
                                                                 "DECLARED_SLOT_ROSTER_INVALID"):
                o._declared_roster(roster, launch_activation_utc=parent["launch_activation_utc"],
                    protected_end_exclusive_utc="2024-07-14T08:05:00Z")

    def test_development_schema_remains_explicit_m5_and_v16(self):
        parent = _development_parent()
        for schema in (o.DECLARED_M5_DEVELOPMENT_SLOT_PRECOMMIT_SCHEMA, o.DECLARED_M5_DEVELOPMENT_SLOT_REQUEST_SCHEMA):
            body = deepcopy(parent); body["schema_version"] = schema
            self.assertEqual(o.declared_slot_source_kind(body), "m5_derived")
            self.assertEqual(o.declared_slot_transport_edition(body), o.DECLARED_WINNER_TRANSPORT_V16)
        self.assertEqual(o._declared_precommit_schema(o.DECLARED_WINNER_TRANSPORT_V16, "m5_derived", development=True),
                         o.DECLARED_M5_DEVELOPMENT_SLOT_PRECOMMIT_SCHEMA)
        self.assertEqual(o._declared_request_schema(o.DECLARED_WINNER_TRANSPORT_V16, "m5_derived", development=True),
                         o.DECLARED_M5_DEVELOPMENT_SLOT_REQUEST_SCHEMA)
        with self.assertRaisesRegex(o.HistoricalAnchorOrchestrationError, "DECLARED_DEVELOPMENT_SCOPE_INVALID"):
            o._declared_request_schema(o.DECLARED_WINNER_TRANSPORT_V16, "broker_native", development=True)

    def test_decoded_scope_is_detached_and_parent_prefix_is_rechecked(self):
        parent = _development_parent(); scope = o.declared_slot_development_scope(parent)
        scope["selected_slot_ids"].append("foreign")
        self.assertEqual(len(o.declared_slot_selected_slots(parent)), 1)
        parent["development_scope"]["selected_slot_ids"] = [parent["slot_roster"]["slots"][1]["slot_id"]]
        _rehash_development(parent)
        with self.assertRaisesRegex(o.HistoricalAnchorOrchestrationError, "DECLARED_DEVELOPMENT_PREFIX_INVALID"):
            o.declared_slot_selected_slots(parent)


if __name__ == "__main__":
    unittest.main()
