"""Owned clock-build software fixtures: NO_NATIVE_VERIFICATION_PERFORMED.

The positive composition uses an ordinary reader, real publication/spool and
cold reopening. It proves no original native scope, operator adoption, DATA
allocation or successful public authenticated build entry.
"""

from dataclasses import asdict
import hashlib
import json
import os
from pathlib import Path
import stat
import unittest
from uuid import uuid4

import pm_explicit_clock_build as clock_build
from pm_explicit_derived_history import canonical_json_bytes
from test_explicit_clock_native_io import build_unverified_clock_inputs


SYNTHETIC_CLASSIFICATION = "NO_NATIVE_VERIFICATION_PERFORMED"
IMPLEMENTATION_PATHS = (
    "pm_explicit_clock_build.py", "pm_explicit_derived_native.py",
    "pm_explicit_clock_adapter.py", "pm_explicit_clock_publication.py",
    "pm_explicit_observed_clock.py", "pm_explicit_derived_history.py",
    "pm_authority_spool.py", "tools/build_spread_profile.py",
)
FALSE_FLAGS = {name: False for name in (
    "qualification_authority", "capital_authority", "live_authority", "order_authority",
    "full_native_context_eligibility", "financial_consumer_ready")}
DATA_LABEL = "AVAILABLE_DATA_ONLY_NOT_EXECUTION_READY"
SOFTWARE_FIXTURE_LIMITS = {
    "max_artifact_bytes": 2 * 1024**2, "max_symbol_bytes": 4 * 1024**2,
    "max_generation_bytes": 4 * 1024**2, "max_artifacts_per_symbol": 1000,
    "max_manifest_bytes": 1024**2, "max_json_bytes": 1024**2,
    "stream_chunk_bytes": 64 * 1024, "min_free_bytes": 1,
}


class ExplicitClockBuildTests(unittest.TestCase):
    def setUp(self):
        configured = os.environ.get("FXPM_DERIVED_QA_ROOT")
        if configured is None:
            raise unittest.SkipTest("FXPM_DERIVED_QA_ROOT_NOT_CONFIGURED")
        if not configured:
            raise RuntimeError("FXPM_DERIVED_QA_ROOT_REQUIRED_NO_TEMP_FALLBACK")
        parent = Path(configured)
        if not parent.is_absolute() or not parent.is_dir():
            raise RuntimeError("FXPM_DERIVED_QA_ROOT_MUST_BE_AN_EXISTING_OWNED_ABSOLUTE_DIRECTORY")
        for directory in (parent, *parent.parents):
            identity = directory.lstat()
            if stat.S_ISLNK(identity.st_mode) or getattr(identity, "st_file_attributes", 0) & 0x400:
                raise RuntimeError("FXPM_DERIVED_QA_ROOT_REPARSE_PATH_REFUSED")
        self.root = parent / ("clock-build-" + uuid4().hex)
        self.root.mkdir(exist_ok=False)
        with (self.root / "NO_NATIVE_VERIFICATION_PERFORMED.txt").open("x", encoding="utf-8") as handle:
            handle.write("Invented owned build metadata/software composition only; no native verification or DATA grant.\n")

    def write_json(self, name, value):
        path = self.root / name
        raw = json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode("ascii")
        with path.open("xb") as handle:
            handle.write(raw)
        return {"path": str(path), "bytes": len(raw), "sha256": hashlib.sha256(raw).hexdigest()}

    def fixture(self, *, scope_changes=None, approval_changes=None, missing_role=None):
        """Toy metadata repo only; none of these files are executed authority."""
        repo = self.root / ("toy-repository-" + uuid4().hex)
        repo.mkdir(exist_ok=False)
        source_map = {}
        for relative in IMPLEMENTATION_PATHS:
            path = repo / relative
            path.parent.mkdir(parents=True, exist_ok=True)
            with path.open("xb") as handle:
                handle.write(("# SYNTHETIC_SOURCE_MAP_NOT_EXECUTED_AUTHORITY " + relative + "\n").encode("ascii"))
            source_map[relative] = path
        implementation = clock_build._implementation_from_source_map(source_map)
        held = []
        for role, relative in (
                ("native_activation", "data/native/fixture-activation.json"),
                ("active_pointer", "data/native/ACTIVE.json"),
                ("held_profile", "data/native/terms/spread_profile.json")):
            path = repo / relative
            path.parent.mkdir(parents=True, exist_ok=True)
            raw = canonical_json_bytes({"classification": SYNTHETIC_CLASSIFICATION, "role": role})
            with path.open("xb") as handle:
                handle.write(raw)
            held.append({"role": role, "path": relative, "sha256": hashlib.sha256(raw).hexdigest()})
        if missing_role is not None:
            held = [{**pin, "path": "data/native/missing-owned-input.json"}
                    if pin["role"] == missing_role else pin for pin in held]
        (repo / clock_build.RELATIVE_QA).mkdir(parents=True, exist_ok=True)
        scope = {"schema_version": "fxpm.clock-build-adopted-scope.v1", "status": "ADOPTED",
            "source_kind": "m5_derived_control_clock.v1", "cutoff_utc": "2024-07-14T08:05:00+00:00",
            "profile_disposition": "PRICE_ONLY_NO_PROFILE_ADOPTED",
            "targets": [{"symbol": "GBPNZD", "timeframe": "H4"}],
            "data_relative_path": "data",
            "output_relative_path": (clock_build.RELATIVE_QA / "fixture-clock-output").as_posix(),
            "generation_id": "synthetic-clock-build-001",
            "activation_sha256": next(p["sha256"] for p in held if p["role"] == "native_activation"),
            "active_pointer_sha256": next(p["sha256"] for p in held if p["role"] == "active_pointer"),
            "held_inputs": held, "limits": dict(clock_build.LIMITS),
            "data_scope": DATA_LABEL, "readiness_label": DATA_LABEL,
            "classification": "SYNTHETIC_SOFTWARE_FIXTURE_NOT_OPERATOR_GRANT", **FALSE_FLAGS}
        scope.update(scope_changes or {})
        scope_pin = self.write_json(repo.name + "/fixture-adoption.json", scope)
        approval = {"schema_version": "fxpm.clock-build-data-approval.v1", "status": "APPROVED",
            "adoption_sha256": scope_pin["sha256"],
            "implementation_sha256": implementation["implementation_sha256"],
            "target_list_sha256": hashlib.sha256(canonical_json_bytes(scope["targets"])).hexdigest(),
            "output_relative_path": scope["output_relative_path"], "generation_id": scope["generation_id"],
            "limits": scope["limits"], "attempts": 1, "retry": False, "renewal": False,
            "authority_ref": "SYNTHETIC_SOFTWARE_FIXTURE_NOT_AN_OPERATOR_OR_DATA_GRANT",
            "classification": "SYNTHETIC_SOFTWARE_FIXTURE_NOT_OPERATOR_GRANT", **FALSE_FLAGS}
        approval.update(approval_changes or {})
        approval_pin = self.write_json(repo.name + "/fixture-data-approval.json", approval)
        return {"repo": repo, "scope": scope, "approval": approval,
            "implementation": implementation, "source_map": source_map,
            "kwargs": {"expected_implementation_sha256": implementation["implementation_sha256"],
                "adoption_path": "fixture-adoption.json", "adoption_sha256": scope_pin["sha256"],
                "data_approval_path": "fixture-data-approval.json", "data_approval_sha256": approval_pin["sha256"],
                "implementation": implementation, "free_bytes": clock_build.LIMITS["min_free_bytes"]}}

    def preflight(self, fixture, **overrides):
        return clock_build._preflight(fixture["repo"], **{**fixture["kwargs"], **overrides})

    def assert_prewrite_refusal(self, fixture, code, **overrides):
        output = fixture["repo"] / fixture["scope"]["output_relative_path"]
        existed = output.exists()
        with self.assertRaises(clock_build.ClockBuildError) as caught:
            self.preflight(fixture, **overrides)
        self.assertEqual(caught.exception.code, code)
        self.assertEqual(output.exists(), existed)

    def test_public_missing_external_disposition_refuses_without_creating_a_root(self):
        repo = self.root / "empty-public-negative-repository"
        repo.mkdir(exist_ok=False)
        with self.assertRaises(clock_build.ClockBuildError) as caught:
            clock_build.build_approved_clock_scope(repo, expected_implementation_sha256="0" * 64)
        self.assertEqual(caught.exception.code, "CLOCK_EXTERNAL_DISPOSITION_REQUIRED")
        self.assertEqual(list(repo.iterdir()), [])

    def test_each_missing_external_pin_refuses_before_implementation_or_writes(self):
        for key in ("adoption_path", "adoption_sha256", "data_approval_path", "data_approval_sha256"):
            with self.subTest(key=key):
                fixture = self.fixture()
                self.assert_prewrite_refusal(fixture, "CLOCK_EXTERNAL_DISPOSITION_REQUIRED", **{key: None})

    def test_read_only_preflight_uses_only_the_pinned_declared_target_set(self):
        targets = [{"symbol": "GBPNZD", "timeframe": "H4"}, {"symbol": "AUDCAD", "timeframe": "D1"}]
        fixture = self.fixture(scope_changes={"targets": targets})
        result = self.preflight(fixture)
        self.assertEqual(set(result["targets"]), {("GBPNZD", "H4"), ("AUDCAD", "D1")})
        self.assertEqual(len(result["targets"]), 2)
        self.assertFalse(result["output_root"].exists())
        self.assertEqual(result["scope"], fixture["scope"])

    def test_implementation_binding_has_all_eight_sources_and_explicit_copy_provenance(self):
        fixture = self.fixture()
        binding = fixture["implementation"]
        self.assertEqual([row["path"] for row in binding["sources"]], sorted(IMPLEMENTATION_PATHS))
        self.assertEqual(binding["binding_provenance"], "SYNTHETIC_SOURCE_MAP_NOT_EXECUTED_AUTHORITY")
        self.assertEqual(binding["implementation_sha256"],
                         hashlib.sha256(canonical_json_bytes(binding["sources"])).hexdigest())
        for row in binding["sources"]:
            raw = fixture["source_map"][row["path"]].read_bytes()
            self.assertEqual(row["bytes"], len(raw))
            self.assertEqual(row["sha256"], hashlib.sha256(raw).hexdigest())

    def test_actual_implementation_binding_names_every_executed_product_source(self):
        candidate_root = Path(clock_build.__file__).parent
        binding = clock_build.implementation_binding(candidate_root)
        self.assertEqual([row["path"] for row in binding["sources"]], sorted(IMPLEMENTATION_PATHS))
        self.assertEqual(binding["binding_provenance"], "ACTUAL_EXECUTED_MODULE_ORIGINS")
        self.assertEqual({row["path"] for row in binding["origins"]}, set(IMPLEMENTATION_PATHS))
        self.assertTrue(all(Path(row["origin"]).is_file() for row in binding["origins"]))

    def test_actual_implementation_binding_refuses_an_unrelated_owned_root(self):
        other = self.root / "not-the-executed-product-root"
        other.mkdir(exist_ok=False)
        with self.assertRaises(clock_build.ClockBuildError) as caught:
            clock_build.implementation_binding(other)
        self.assertEqual(caught.exception.code, "CLOCK_EXECUTED_ROOT_MISMATCH")
        self.assertEqual(list(other.iterdir()), [])

    def test_implementation_mismatch_refuses_before_output(self):
        fixture = self.fixture()
        self.assert_prewrite_refusal(fixture, "CLOCK_IMPLEMENTATION_CHANGED",
                                    expected_implementation_sha256="0" * 64)

    def test_changed_owned_source_map_invalidates_the_expected_implementation(self):
        fixture = self.fixture()
        with fixture["source_map"]["pm_explicit_clock_build.py"].open("ab") as handle:
            handle.write(b"# OWNED_SYNTHETIC_CHANGED_SOURCE\n")
        changed = clock_build._implementation_from_source_map(fixture["source_map"])
        self.assertNotEqual(changed["implementation_sha256"], fixture["implementation"]["implementation_sha256"])
        self.assert_prewrite_refusal(fixture, "CLOCK_IMPLEMENTATION_CHANGED", implementation=changed)

    def test_missing_or_unsafe_external_paths_refuse_without_fallback(self):
        for path, code in (("missing-adoption.json", "CLOCK_PIN_UNAVAILABLE"),
                           ("../outside-adoption.json", "CLOCK_DISPOSITION_PATH_INVALID")):
            with self.subTest(path=path):
                fixture = self.fixture()
                self.assert_prewrite_refusal(fixture, code, adoption_path=path)

    def test_external_scope_or_approval_hash_mismatch_refuses(self):
        for key in ("adoption_sha256", "data_approval_sha256"):
            with self.subTest(key=key):
                fixture = self.fixture()
                self.assert_prewrite_refusal(fixture, "CLOCK_HELD_INPUT_CHANGED", **{key: "0" * 64})

    def test_unadopted_or_wrong_source_scope_refuses_before_output(self):
        for changes in ({"status": "PROPOSED"}, {"source_kind": "broker_native"},
                        {"profile_disposition": "NUMERICAL_PROFILE_ADOPTED"},
                        {"cutoff_utc": "2024-07-14T08:10:00+00:00"},
                        {"data_relative_path": "other-data"},
                        {"live_authority": True}, {"live_authority": 0}):
            with self.subTest(changes=changes):
                fixture = self.fixture(scope_changes=changes)
                self.assert_prewrite_refusal(fixture, "CLOCK_ADOPTION_INVALID")

    def test_targets_must_be_a_nonempty_distinct_h4_d1_roster(self):
        cases = ([], "GBPNZD", [{"symbol": "", "timeframe": "H4"}],
                 [{"symbol": "GBPNZD", "timeframe": "H1"}],
                 [{"symbol": "GBPNZD", "timeframe": "H4"}] * 2)
        for targets in cases:
            with self.subTest(targets=targets):
                fixture = self.fixture(scope_changes={"targets": targets})
                self.assert_prewrite_refusal(fixture, "CLOCK_TARGET_ROSTER_INVALID")

    def test_target_cap_is_checked_before_any_attempt_root(self):
        from pm_native_request_contract import PRODUCTION_SYMBOL_ORDER

        fixture = self.fixture(scope_changes={"targets": [
            {"symbol": symbol, "timeframe": "H4"} for symbol in PRODUCTION_SYMBOL_ORDER[:26]]})
        self.assert_prewrite_refusal(fixture, "CLOCK_TARGET_CAP_EXCEEDED")

    def test_approval_must_bind_scope_implementation_targets_and_single_attempt(self):
        cases = ({"status": "PROPOSED"}, {"adoption_sha256": "0" * 64},
                 {"implementation_sha256": "0" * 64}, {"target_list_sha256": "0" * 64},
                 {"generation_id": "other-generation"}, {"output_relative_path": "other-output"},
                 {"attempts": 2}, {"attempts": True}, {"retry": True}, {"retry": 0},
                 {"renewal": True}, {"authority_ref": ""}, {"capital_authority": 0},
                 {"limits": {**dict(clock_build.LIMITS), "min_free_bytes": 1}})
        for changes in cases:
            with self.subTest(changes=changes):
                fixture = self.fixture(approval_changes=changes)
                self.assert_prewrite_refusal(fixture, "CLOCK_DATA_APPROVAL_INVALID")

    def test_existing_attempt_root_is_preserved_and_never_reused(self):
        fixture = self.fixture()
        output = fixture["repo"] / fixture["scope"]["output_relative_path"]
        output.mkdir(exist_ok=False)
        marker = output / "owned-partial-must-stay.txt"
        with marker.open("xb") as handle:
            handle.write(b"RETAINED_OWNED_PARTIAL")
        self.assert_prewrite_refusal(fixture, "CLOCK_ATTEMPT_ROOT_ALREADY_EXISTS_NO_RETRY")
        self.assertEqual(marker.read_bytes(), b"RETAINED_OWNED_PARTIAL")

    def test_unsafe_output_path_or_generation_identity_refuses_before_write(self):
        for changes in ({"output_relative_path": "../outside-owned-repository"},
                        {"generation_id": ""}):
            with self.subTest(changes=changes):
                fixture = self.fixture(scope_changes=changes)
                self.assert_prewrite_refusal(fixture, "CLOCK_ADOPTION_INVALID")

    def test_disk_floor_precedes_held_input_reads_and_drift_still_refuses_after_floor(self):
        fixture = self.fixture()
        held = fixture["repo"] / "data/native/terms/spread_profile.json"
        with held.open("ab") as handle:
            handle.write(b"\n")
        self.assert_prewrite_refusal(fixture, "CLOCK_MIN_FREE_DISK_GUARD", free_bytes=0)
        self.assert_prewrite_refusal(fixture, "CLOCK_HELD_INPUT_CHANGED")

    def test_missing_held_input_refuses_without_an_attempt_root(self):
        fixture = self.fixture(missing_role="native_activation")
        self.assert_prewrite_refusal(fixture, "CLOCK_PIN_UNAVAILABLE")

    def test_public_limits_are_the_exact_data_limits_and_low_free_space_refuses(self):
        expected = {"max_artifact_bytes": 1024**3, "max_symbol_bytes": 8 * 1024**3,
            "max_generation_bytes": 8 * 1024**3, "max_artifacts_per_symbol": 100000,
            "max_manifest_bytes": 128 * 1024**2, "max_json_bytes": 64 * 1024**2,
            "stream_chunk_bytes": 1024**2, "min_free_bytes": 16 * 1024**3}
        self.assertEqual(dict(clock_build.LIMITS), expected)
        fixture = self.fixture()
        self.assert_prewrite_refusal(fixture, "CLOCK_MIN_FREE_DISK_GUARD", free_bytes=expected["min_free_bytes"] - 1)

    def test_adoption_cannot_lower_or_replace_the_public_data_limits(self):
        lowered = {**dict(clock_build.LIMITS), "min_free_bytes": 1}
        fixture = self.fixture(scope_changes={"limits": lowered})
        self.assert_prewrite_refusal(fixture, "CLOCK_ADOPTION_INVALID")

    def test_public_composition_refuses_ordinary_reader_and_fixture_callbacks_before_prepare(self):
        output = self.root / "refused-public-composition"

        def forbidden(*args, **kwargs):
            raise AssertionError("ordinary fixture callbacks must not enter the public composition")

        with self.assertRaises(clock_build.ClockBuildError) as caught:
            clock_build._compose_clock_edition(output, object(), (("GBPNZD", "H4"),),
                generation_id="SYNTHETIC_PUBLIC_NEGATIVE", native_universe_sha256="1" * 64,
                implementation={"implementation_sha256": "2" * 64},
                limits=dict(SOFTWARE_FIXTURE_LIMITS), prepare_plan=forbidden,
                populate_plan=forbidden, before_write=forbidden, before_finish=forbidden,
                build_context={"classification": SYNTHETIC_CLASSIFICATION}, software_fixture=False)
        self.assertEqual(caught.exception.code, "CLOCK_PUBLIC_COMPOSITION_REQUIRED")
        self.assertFalse(output.exists())

    def test_one_owned_private_composition_seals_reopens_and_records_price_only_outcome(self):
        from pm_explicit_clock_adapter import (
            PROTECTED_CUTOFF, _prepare_protocol, _populate_protocol, _checked_eof_roles,
            prepare_frozen_clock_plan, populate_frozen_clock_plan, ClockAdapterError,
        )
        from pm_explicit_clock_publication import SOURCE_CONTEXT_SCHEMA, AUTHENTICATION
        from pm_explicit_derived_native import NativeInputs

        reader = build_unverified_clock_inputs(self.root / "composition-owned-inputs")
        self.assertNotIsInstance(reader, NativeInputs)
        self.assertIsNone(reader._scope)
        with self.assertRaises(ClockAdapterError):
            prepare_frozen_clock_plan(reader, (reader.target_pair,))
        with self.assertRaises(ClockAdapterError):
            populate_frozen_clock_plan(object(), object(), reader)
        output = self.root / "composition-owned-output"
        events = []

        def prepare(inputs, targets, *, cutoff_utc=PROTECTED_CUTOFF):
            events.append("prepare_metadata")
            return _prepare_protocol(inputs, targets, cutoff_utc=cutoff_utc)

        def populate(publisher, plan, inputs):
            events.append("populate_owned_files")
            roles = _populate_protocol(publisher, plan, inputs)
            pins = tuple(inputs.revalidate_owned_files_only())
            _checked_eof_roles(roles, pins)
            payload = plan.to_dict()
            scope = {name: payload[name] for name in (
                "data_scope", "readiness_label", "full_native_context_eligibility",
                "financial_consumer_ready", "qualification_authority", "capital_authority",
                "live_authority", "order_authority")}
            publisher.bind_source_context({"schema_version": SOURCE_CONTEXT_SCHEMA,
                "authentication_semantics": AUTHENTICATION,
                "authority_bindings": asdict(inputs.authority_bindings),
                "metadata_read_pins": [asdict(pin) for pin in pins], "consumed_roles": list(roles),
                "fixture_provenance": SYNTHETIC_CLASSIFICATION,
                "original_scope_entered": False, "original_receipt_backed_revalidation_performed": False,
                "fixture_owned_file_revalidation_only": True, **scope})

        def before_write():
            self.assertFalse(output.exists())
            events.append("before_write")

        def before_finish():
            self.assertTrue(output.is_dir())
            events.append("before_finish")

        implementation = clock_build.implementation_binding(Path(clock_build.__file__).parent)
        outcome = clock_build._compose_clock_edition(output, reader, (reader.target_pair,),
            generation_id="SYNTHETIC_CLOCK_BUILD_NO_NATIVE_VERIFICATION",
            native_universe_sha256=reader.authority_bindings.activation_sha256,
            implementation=implementation, limits=dict(SOFTWARE_FIXTURE_LIMITS),
            prepare_plan=prepare, populate_plan=populate, before_write=before_write,
            before_finish=before_finish,
            build_context={"classification": SYNTHETIC_CLASSIFICATION,
                           "original_scope_entered": False, "operator_or_DATA_grant": False},
            software_fixture=True)
        self.assertEqual(events, ["prepare_metadata", "before_write", "populate_owned_files", "before_finish"])
        self.assertEqual(outcome["status"], "CLOCK_PRICE_ONLY_BUILD_COMPLETED_WITH_EXPLICIT_STOPS")
        self.assertEqual(outcome["price_cells"], 1)
        self.assertEqual(outcome["stop_cells"], 0)
        self.assertEqual([(row["symbol"], row["timeframe"]) for row in outcome["results"]], [reader.target_pair])
        self.assertEqual(outcome["results"][0]["status"], "PRICE_DATA_PUBLISHED")
        self.assertEqual(outcome["results"][0]["cost_status"], "PRICE_ONLY")
        self.assertTrue(outcome["software_fixture"])
        self.assertIsNone(outcome["profile"])
        self.assertIsNone(outcome["statistics"])
        self.assertTrue(outcome["receipt"])
        self.assertEqual(outcome["implementation"]["implementation_sha256"], implementation["implementation_sha256"])
        for name in FALSE_FLAGS:
            self.assertIs(outcome[name], False)
        self.assertEqual(clock_build.LIMITS["min_free_bytes"], 16 * 1024**3)
        self.assertIsNone(reader._scope)
