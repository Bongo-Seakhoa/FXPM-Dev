"""Pre-source DATA dispatch only: SYNTHETIC_NO_NATIVE_VERIFICATION.

The actual private validator receives ordinary failure metadata. No source-session
capability, request issuer, product, header, fit or successful verifier is mocked.
"""
import ast
from copy import deepcopy
from datetime import datetime, timedelta
import hashlib
import json
from pathlib import Path
import sys
from types import SimpleNamespace
import unittest


_REPAIR = Path(__file__).resolve().parents[1]
_CANDIDATE = _REPAIR
sys.path.insert(0, str(_CANDIDATE))
import pm_historical_anchor_orchestration as orchestration

if Path(orchestration.__file__).resolve() != _CANDIDATE / "pm_historical_anchor_orchestration.py":
    raise RuntimeError("requires the actual candidate development-scope decoder")

PROVENANCE = "SYNTHETIC_PRE_SOURCE_FAILURE_DATA_NOT_AUTHENTICATED_SOURCE_OR_FIT"


def _digest(value):
    return hashlib.sha256(json.dumps(value, ensure_ascii=True, allow_nan=False,
        sort_keys=True, separators=(",", ":")).encode("ascii")).hexdigest()


def _actual_helpers():
    path = _REPAIR / "pm_historical_production_adapter.py"
    tree = ast.parse(path.read_bytes(), filename=str(path))
    names = {"HistoricalProductionAdapterError", "_fail", "_canonical_bytes", "_sha256", "_hex64", "_reason",
        "_declared_slot_is_m5_source", "_declared_m5_profile_identity", "_declared_m5_observation",
        "_validate_declared_m5_source_session"}
    selected = [node for node in tree.body if isinstance(node, (ast.FunctionDef, ast.ClassDef))
        and node.name in names]
    if {node.name for node in selected} != names:
        raise RuntimeError("actual repair helper structure changed")
    constants = [node for node in tree.body if isinstance(node, (ast.Assign, ast.AnnAssign))
        and any(isinstance(target, ast.Name) and target.id in {
            "DECLARED_M5_SLOT_SOURCE_SESSION_SCHEMA", "DECLARED_M5_SLOT_SOURCE_SESSION_KEYS"}
            for target in (node.targets if isinstance(node, ast.Assign) else [node.target]))]
    future = ast.ImportFrom(module="__future__", names=[ast.alias(name="annotations")], level=0)
    namespace = {"__name__": "synthetic_pre_source_metadata_helpers", "orchestration": orchestration,
        "hashlib": hashlib, "json": json, "datetime": datetime, "timedelta": timedelta}
    exec(compile(ast.fix_missing_locations(ast.Module(body=[future, *constants, *selected],
        type_ignores=[])), str(path), "exec"), namespace)
    return SimpleNamespace(**namespace)


def _fixture(helpers, development):
    pins = {"view_plan_sha256": "1"*64, "cost_edition_identity": {
        "receipt_sha256": "2"*64, "generation_id": "UNIT_COST_EDITION_NOT_ISSUED",
        "native_universe_sha256": "3"*64, "plan_sha256": "4"*64,
        "implementation_sha256": "5"*64, "profile_sha256": "6"*64}}
    body = {"schema_version": "fxpm.declared-m5-slot-producer-request.v1", "source_kind": "m5_derived",
        "effective_config_sha256": "7"*64, "anchor": {"selection_cutoff_utc": "2024-01-02T00:00:00Z"},
        "spread_profile_identity": {"profile_sha256": "6"*64}, "m5_source_pins": pins,
        "m5_view_contract_sha256": _digest(pins)}
    if development:
        scope = {"schema_version": orchestration.DECLARED_DEVELOPMENT_SCOPE_SCHEMA,
            "selection_policy": "FIRST_K_OF_ACCEPTED_13", "selected_slot_ids": ["UNIT_DOCUMENTARY_SELECTED_SLOT"],
            "adopted_symbols": list(orchestration.DECLARED_DEVELOPMENT_SYMBOLS),
            "design_contaminated": True, "pristine_validation": False,
            "qualification_authority": False, "capital_authority": False,
            "live_authority": False, "order_authority": False}
        body.update(schema_version="fxpm.declared-development-m5-slot-producer-request.v1",
            development_scope=scope, development_scope_sha256=_digest(scope))
    context = SimpleNamespace(context_sha256="8"*64, config_sha256="9"*64)
    parent = {"effective_config_sha256": body["effective_config_sha256"],
        "precommitted_at_utc": "2026-11-03T00:00:00Z"}
    value = dict.fromkeys(helpers.DECLARED_M5_SLOT_SOURCE_SESSION_KEYS)
    value.update(schema_version=helpers.DECLARED_M5_SLOT_SOURCE_SESSION_SCHEMA,
        source_kind="m5_derived", request_sha256="a"*64,
        generation_context_sha256=context.context_sha256, config_sha256=context.config_sha256,
        effective_config_sha256=body["effective_config_sha256"],
        spread_profile_identity=deepcopy(body["spread_profile_identity"]), m5_source_pins=deepcopy(pins),
        m5_view_contract_sha256=body["m5_view_contract_sha256"], view_plan_sha256=pins["view_plan_sha256"],
        session_opened_at_utc="2026-11-03T00:00:01Z", selection_cutoff_utc=body["anchor"]["selection_cutoff_utc"],
        source_state="FAILED", cost_status="COST_UNAVAILABLE",
        failure={"exception_type": "SyntheticPreSourceRefusal", "failure_code": "UNIT_PRE_SOURCE_REFUSAL",
            "failure_observed_at_utc": "2026-11-03T00:00:02Z", "phase": "PRE_SOURCE"},
        unverified_caller_source_or_fit_accepted=False, qualification_authority=False,
        capital_authority=False, live_authority=False, order_authority=False)
    return context, body, value, parent


class M5DevelopmentSourceSessionDispatchTests(unittest.TestCase):
    def validate(self, helpers, fixture):
        context, body, value, parent = fixture
        return helpers._validate_declared_m5_source_session(context, body=body, source_session=value,
            request_sha256="a"*64, parent=parent, completed_at_utc="2026-11-03T00:00:03Z")

    def test_full_and_development_pre_source_failure_metadata_use_same_strict_validator(self):
        helpers = _actual_helpers()
        for development in (False, True):
            with self.subTest(development=development):
                fixture = _fixture(helpers, development)
                result = self.validate(helpers, fixture)
                self.assertEqual(result, fixture[2])
                self.assertIs(type(result), dict)
                self.assertIsNone(result["source_accessed_at_utc"])
                self.assertIsNone(result["native_source_route"])
                self.assertEqual(result["failure"]["phase"], "PRE_SOURCE")
                for field in ("qualification_authority", "capital_authority", "live_authority", "order_authority"):
                    self.assertIs(result[field], False)
                result["failure"]["phase"] = "CHANGED_RETURNED_DATA"
                self.assertEqual(fixture[2]["failure"]["phase"], "PRE_SOURCE")

    def test_wrong_source_and_invalid_development_scope_keep_original_refusals(self):
        helpers = _actual_helpers()
        fixture = _fixture(helpers, False)
        fixture[1]["source_kind"] = "broker_native"
        with self.assertRaisesRegex(orchestration.HistoricalAnchorOrchestrationError,
                "DECLARED_SLOT_SOURCE_KIND_INVALID"):
            self.validate(helpers, fixture)
        for change in ("authority", "pristine", "scope_hash"):
            fixture = _fixture(helpers, True)
            scope = fixture[1]["development_scope"]
            if change == "authority":
                scope["qualification_authority"] = True
                fixture[1]["development_scope_sha256"] = _digest(scope)
            elif change == "pristine":
                scope["pristine_validation"] = True
                fixture[1]["development_scope_sha256"] = _digest(scope)
            else:
                fixture[1]["development_scope_sha256"] = "b"*64
            with self.subTest(change=change), self.assertRaisesRegex(
                    orchestration.HistoricalAnchorOrchestrationError, "DECLARED_DEVELOPMENT_SCOPE_INVALID"):
                self.validate(helpers, fixture)

    def test_request_binding_source_kind_and_exact_false_authorities_still_refuse(self):
        helpers = _actual_helpers()
        for change in ("request", "view_plan", "source_kind", "qualification_zero", "caller_accepted"):
            fixture = _fixture(helpers, True)
            value = fixture[2]
            if change == "request":
                value["request_sha256"] = "c"*64
            elif change == "view_plan":
                value["view_plan_sha256"] = "c"*64
            elif change == "source_kind":
                value["source_kind"] = "broker_native"
            elif change == "qualification_zero":
                value["qualification_authority"] = 0
            else:
                value["unverified_caller_source_or_fit_accepted"] = True
            with self.subTest(change=change), self.assertRaises(helpers.HistoricalProductionAdapterError) as caught:
                self.validate(helpers, fixture)
            expected = "DECLARED_M5_SOURCE_SESSION_INVALID" if change == "source_kind" else "DECLARED_M5_SOURCE_BINDING_MISMATCH"
            self.assertEqual(str(caught.exception), expected)


if __name__ == "__main__":
    unittest.main()
