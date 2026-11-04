"""Invented documentary scopes; no request, precommit or completion is issued."""
import copy
import hashlib
import unittest

import pm_historical_anchor_orchestration as orchestration
import pm_historical_production_adapter as adapter


def development_body():
    scope = {
        "schema_version": orchestration.DECLARED_DEVELOPMENT_SCOPE_SCHEMA,
        "selection_policy": "FIRST_K_OF_ACCEPTED_13", "selected_slot_ids": ["invented-slot-01"],
        "adopted_symbols": list(orchestration.DECLARED_DEVELOPMENT_SYMBOLS),
        "design_contaminated": True, "pristine_validation": False,
        "qualification_authority": False, "capital_authority": False,
        "live_authority": False, "order_authority": False,
    }
    return {"schema_version": orchestration.DECLARED_M5_DEVELOPMENT_SLOT_REQUEST_SCHEMA,
            "development_scope": scope,
            "development_scope_sha256": hashlib.sha256(orchestration._declared_json(scope)).hexdigest()}


class DevelopmentCompletionMetadataTests(unittest.TestCase):
    def test_completion_kind_tracks_explicit_scope_without_authenticating_it(self):
        native = {"schema_version": orchestration.DECLARED_SLOT_REQUEST_SCHEMA_V2}
        self.assertEqual(adapter.declared_slot_completion_schema(native), adapter.DECLARED_SLOT_COMPLETION_SCHEMA)
        body = development_body()
        self.assertEqual(adapter.declared_slot_completion_schema(body), adapter.DECLARED_DEVELOPMENT_SLOT_COMPLETION_SCHEMA)
        body["schema_version"] = orchestration.DECLARED_SLOT_REQUEST_SCHEMA_V2
        with self.assertRaises(orchestration.HistoricalAnchorOrchestrationError):
            adapter.declared_slot_completion_schema(body)

    def test_development_completion_joins_parent_hash_and_contamination_flags(self):
        body = development_body()
        parent = copy.deepcopy(body)
        parent["schema_version"] = orchestration.DECLARED_M5_DEVELOPMENT_SLOT_PRECOMMIT_SCHEMA
        completion = {"development_scope_sha256": body["development_scope_sha256"],
                      "design_contaminated": True, "pristine_validation": False}
        adapter._declared_development_completion_binding(completion, body, parent)
        for key, value in (("development_scope_sha256", "f" * 64),
                           ("design_contaminated", False), ("pristine_validation", True)):
            changed = dict(completion); changed[key] = value
            with self.subTest(field=key), self.assertRaises(adapter.HistoricalProductionAdapterError):
                adapter._declared_development_completion_binding(changed, body, parent)
        changed = copy.deepcopy(parent)
        changed["development_scope"]["selected_slot_ids"] = ["invented-slot-02"]
        changed["development_scope_sha256"] = hashlib.sha256(orchestration._declared_json(changed["development_scope"])).hexdigest()
        with self.assertRaises(adapter.HistoricalProductionAdapterError):
            adapter._declared_development_completion_binding(completion, body, changed)

    def test_legacy_completion_refuses_development_claims(self):
        body = {"schema_version": orchestration.DECLARED_SLOT_REQUEST_SCHEMA_V2}
        adapter._declared_development_completion_binding({}, body)
        for key, value in (("development_scope_sha256", "f" * 64),
                           ("design_contaminated", True), ("pristine_validation", False)):
            with self.subTest(field=key), self.assertRaises(adapter.HistoricalProductionAdapterError):
                adapter._declared_development_completion_binding({key: value}, body)
