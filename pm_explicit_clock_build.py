"""Library-only clock edition build; external adoption/allocation remain required.

No artifact or target defaults grant a run. Public composition uses the original
native owner and unchanged DATA limits; private fixture seams confer no authority.
"""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import shutil

import pm_authority_spool as _spool
import pm_explicit_derived_native as _native
import pm_explicit_clock_adapter as _adapter
import pm_explicit_clock_publication as _publication
import pm_explicit_observed_clock as _clock
import pm_explicit_derived_history as _history
import tools.build_spread_profile as _profile_tools


RELATIVE_QA = Path("audit_reports/fxpm_workshop_20261007/declared_task_efficiency_continuation")
PROTECTED_CUTOFF = _history.PROTECTED_CUTOFF
LIMITS = dict(max_artifact_bytes=1073741824, max_symbol_bytes=8589934592,
    max_generation_bytes=8589934592, max_artifacts_per_symbol=100000,
    max_manifest_bytes=134217728, max_json_bytes=67108864,
    stream_chunk_bytes=1048576, min_free_bytes=17179869184)
IMPLEMENTATION_FILES = (
    "pm_explicit_clock_build.py", "pm_explicit_derived_native.py",
    "pm_explicit_clock_adapter.py", "pm_explicit_clock_publication.py",
    "pm_explicit_observed_clock.py", "pm_explicit_derived_history.py",
    "pm_authority_spool.py", "tools/build_spread_profile.py",
)
_FALSE_KEYS = ("qualification_authority", "capital_authority", "live_authority",
    "order_authority", "full_native_context_eligibility", "financial_consumer_ready")
_RESULT_FIELDS = ("symbol", "timeframe", "status", "cost_status", "stratum_id",
    "requested_interval_semantics", "extent_resolution", "metadata_available_interval",
    "required_interval", "m5_acquisition_interval", "m5_observed_interval",
    "available_interval", "omitted_intervals", "target_spec", "control_extent")


class ClockBuildError(ValueError):
    def __init__(self, code: str):
        self.code = code
        super().__init__(code)


def _require(condition, code):
    if not condition:
        raise ClockBuildError(code)


def _digest(value):
    return hashlib.sha256(_history.canonical_json_bytes(value)).hexdigest()


def _same(left, right):
    return _history.canonical_json_bytes(left) == _history.canonical_json_bytes(right)


def _hex(value):
    return type(value) is str and len(value) == 64 and all(c in "0123456789abcdef" for c in value)


def _relative(value):
    raw = value.as_posix() if isinstance(value, Path) else value
    try:
        _spool._relative_parts(raw)
    except (TypeError, ValueError, _spool.AuthoritySpoolError):
        raise ClockBuildError("CLOCK_DISPOSITION_PATH_INVALID")
    return raw


def _external_args(adoption_path, adoption_sha256, data_approval_path, data_approval_sha256):
    _require(adoption_path is not None and data_approval_path is not None
        and _hex(adoption_sha256) and _hex(data_approval_sha256),
        "CLOCK_EXTERNAL_DISPOSITION_REQUIRED")
    return (_relative(adoption_path), adoption_sha256,
            _relative(data_approval_path), data_approval_sha256)


def _pin(repo_root, relative, expected, *, capture=False):
    _require(_hex(expected), "CLOCK_HELD_INPUT_CHANGED")
    try:
        path = _spool._safe_regular_file(Path(repo_root), _relative(relative))
        before = path.stat()
        digest, body = hashlib.sha256(), bytearray()
        with path.open("rb") as handle:
            for chunk in iter(lambda: handle.read(LIMITS["stream_chunk_bytes"]), b""):
                digest.update(chunk)
                if capture:
                    body.extend(chunk)
                    _require(len(body) <= LIMITS["max_json_bytes"], "CLOCK_DISPOSITION_INVALID")
        after = path.stat()
    except (OSError, _spool.AuthoritySpoolError) as error:
        raise ClockBuildError("CLOCK_PIN_UNAVAILABLE") from error
    identity = lambda value: (value.st_dev, value.st_ino, value.st_size, value.st_mtime_ns)
    _require(identity(before) == identity(after) and digest.hexdigest() == expected,
             "CLOCK_HELD_INPUT_CHANGED")
    record = {"path": _relative(relative), "sha256": expected,
              "bytes": after.st_size, "mtime_ns": after.st_mtime_ns}
    return (record, bytes(body)) if capture else record


def _implementation_from_source_map(source_paths):
    """Hash an explicit owned source map; this alone is not executed-code proof."""
    _require(set(source_paths) == set(IMPLEMENTATION_FILES), "CLOCK_IMPLEMENTATION_SET_INVALID")
    sources, origins = [], []
    for name, supplied in sorted(source_paths.items()):
        path = Path(supplied).absolute()
        try:
            safe = _spool._safe_regular_file(path.parent, path.name)
            before = safe.stat()
            body = safe.read_bytes()
            after = safe.stat()
        except (OSError, _spool.AuthoritySpoolError) as error:
            raise ClockBuildError("CLOCK_IMPLEMENTATION_UNAVAILABLE") from error
        _require((before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns) ==
                 (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns),
                 "CLOCK_IMPLEMENTATION_CHANGED")
        sources.append({"path": name, "bytes": len(body), "sha256": hashlib.sha256(body).hexdigest()})
        origins.append({"path": name, "origin": str(safe)})
    return {"sources": sources, "implementation_sha256": _digest(sources), "origins": origins,
            "binding_provenance": "SYNTHETIC_SOURCE_MAP_NOT_EXECUTED_AUTHORITY"}


def implementation_binding(repo_root: Path) -> dict:
    """Bind the eight actual module/code origins, never a caller-selected copy."""
    root = Path(repo_root).absolute()
    own = Path(__file__).absolute()
    _require(root == own.parent, "CLOCK_EXECUTED_ROOT_MISMATCH")
    modules = {
        "pm_explicit_clock_build.py": (own, build_approved_clock_scope),
        "pm_explicit_derived_native.py": (Path(_native.__file__), _native.NativeInputs.__init__),
        "pm_explicit_clock_adapter.py": (Path(_adapter.__file__), _adapter.prepare_frozen_clock_plan),
        "pm_explicit_clock_publication.py": (Path(_publication.__file__), _publication.ClockEditionPublisher.__init__),
        "pm_explicit_observed_clock.py": (Path(_clock.__file__), _clock.freeze_observed_clock),
        "pm_explicit_derived_history.py": (Path(_history.__file__), _history.derive_bars),
        "pm_authority_spool.py": (Path(_spool.__file__), _spool.freeze_generation_context),
        "tools/build_spread_profile.py": (Path(_profile_tools.__file__), _profile_tools._source_metadata),
    }
    paths = {}
    for name, (origin, function) in modules.items():
        _require(origin.absolute() == root / name
            and Path(function.__code__.co_filename).absolute() == origin.absolute(),
            "CLOCK_IMPLEMENTATION_ORIGIN_MISMATCH")
        paths[name] = origin
    result = _implementation_from_source_map(paths)
    return {**result, "binding_provenance": "ACTUAL_EXECUTED_MODULE_ORIGINS"}


def _false_flags(value, code):
    _require(all(type(value.get(name)) is bool and value[name] is False for name in _FALSE_KEYS), code)
    for name in ("data_scope", "readiness_label"):
        _require(name not in value or value[name] == _history.DATA_SCOPE, code)


def _json_pin(repo_root, path, expected, code):
    record, body = _pin(repo_root, path, expected, capture=True)
    try:
        value = _spool._strict_json_bytes(body, LIMITS["max_json_bytes"])
    except (ValueError, _spool.AuthoritySpoolError) as error:
        raise ClockBuildError(code) from error
    _require(type(value) is dict, code)
    return record, value


def _preflight(repo_root, *, expected_implementation_sha256, adoption_path, adoption_sha256,
               data_approval_path, data_approval_sha256, implementation, free_bytes):
    """Read-only artifact/pin checks; an injected fixture binding grants no run."""
    root = Path(repo_root).absolute()
    ap, ah, dp, dh = _external_args(adoption_path, adoption_sha256, data_approval_path, data_approval_sha256)
    _require(_hex(expected_implementation_sha256)
        and implementation["implementation_sha256"] == expected_implementation_sha256,
        "CLOCK_IMPLEMENTATION_CHANGED")
    adoption_pin, scope = _json_pin(root, ap, ah, "CLOCK_ADOPTION_INVALID")
    approval_pin, approval = _json_pin(root, dp, dh, "CLOCK_DATA_APPROVAL_INVALID")
    _false_flags(scope, "CLOCK_ADOPTION_INVALID")
    _require(scope.get("schema_version") == "fxpm.clock-build-adopted-scope.v1"
        and scope.get("status") == "ADOPTED" and scope.get("source_kind") == "m5_derived_control_clock.v1"
        and scope.get("profile_disposition") == _publication.PROFILE_DISPOSITION
        and scope.get("cutoff_utc") == PROTECTED_CUTOFF and scope.get("data_relative_path") == "data"
        and _hex(scope.get("activation_sha256")) and _hex(scope.get("active_pointer_sha256")),
        "CLOCK_ADOPTION_INVALID")
    rows = scope.get("targets")
    _require(type(rows) is list, "CLOCK_TARGET_ROSTER_INVALID")
    _require(len(rows) <= _adapter.MAX_CLOCK_TARGETS, "CLOCK_TARGET_CAP_EXCEEDED")
    _require(bool(rows) and all(type(row) is dict and set(row) == {"symbol", "timeframe"}
        and type(row["symbol"]) is str and row["symbol"] == row["symbol"].strip() and bool(row["symbol"])
        and row["timeframe"] in ("H4", "D1") for row in rows), "CLOCK_TARGET_ROSTER_INVALID")
    targets = tuple((row["symbol"], row["timeframe"]) for row in rows)
    _require(len(set(targets)) == len(targets), "CLOCK_TARGET_ROSTER_INVALID")
    try:
        relative = _relative(scope.get("output_relative_path"))
        generation_id = _spool._identity(scope.get("generation_id"), "CLOCK_GENERATION_ID")
    except (ClockBuildError, _spool.AuthoritySpoolError) as error:
        raise ClockBuildError("CLOCK_ADOPTION_INVALID") from error
    _require(Path(relative).parent == RELATIVE_QA and _same(scope.get("limits"), LIMITS)
        and generation_id == scope["generation_id"],
        "CLOCK_ADOPTION_INVALID")
    output_root = root / relative
    _false_flags(approval, "CLOCK_DATA_APPROVAL_INVALID")
    _require(approval.get("schema_version") == "fxpm.clock-build-data-approval.v1"
        and approval.get("status") == "APPROVED" and approval.get("adoption_sha256") == ah
        and approval.get("implementation_sha256") == expected_implementation_sha256
        and approval.get("target_list_sha256") == _digest(rows)
        and approval.get("output_relative_path") == relative
        and approval.get("generation_id") == scope["generation_id"]
        and _same(approval.get("limits"), LIMITS)
        and type(approval.get("attempts")) is int and approval["attempts"] == 1
        and approval.get("retry") is False and approval.get("renewal") is False
        and type(approval.get("authority_ref")) is str and bool(approval["authority_ref"].strip()),
        "CLOCK_DATA_APPROVAL_INVALID")
    _require(not os.path.lexists(output_root), "CLOCK_ATTEMPT_ROOT_ALREADY_EXISTS_NO_RETRY")
    _require(type(free_bytes) is int and free_bytes >= LIMITS["min_free_bytes"], "CLOCK_MIN_FREE_DISK_GUARD")
    held = scope.get("held_inputs")
    _require(type(held) is list and all(type(row) is dict and set(row) == {"role", "path", "sha256"}
        and type(row["role"]) is str and bool(row["role"]) and _hex(row["sha256"]) for row in held),
        "CLOCK_ADOPTION_INVALID")
    roles = {row["role"]: row for row in held}
    _require(len(roles) == len(held) and {"native_activation", "active_pointer", "held_profile"} <= set(roles)
        and len({_relative(row["path"]) for row in held}) == len(held), "CLOCK_ADOPTION_INVALID")
    _require(roles["native_activation"]["sha256"] == scope["activation_sha256"]
        and roles["active_pointer"]["sha256"] == scope["active_pointer_sha256"]
        and roles["held_profile"]["path"] == "data/native/terms/spread_profile.json", "CLOCK_ADOPTION_INVALID")
    held_pins = [_pin(root, row["path"], row["sha256"]) for row in held]
    _spool._reject_reparse_chain(output_root.parent)
    return {"scope": scope, "approval": approval, "targets": targets, "output_root": output_root,
        "generation_id": scope["generation_id"], "activation_sha256": scope["activation_sha256"],
        "pointer_sha256": scope["active_pointer_sha256"], "held_pins": held_pins,
        "external_pins": [adoption_pin, approval_pin], "implementation": implementation}


def _recheck(repo_root, snapshot, *, require_absent_output):
    root = Path(repo_root).absolute()
    if require_absent_output:
        _require(not os.path.lexists(snapshot["output_root"]), "CLOCK_ATTEMPT_ROOT_ALREADY_EXISTS_NO_RETRY")
    _require(shutil.disk_usage(root).free >= LIMITS["min_free_bytes"], "CLOCK_MIN_FREE_DISK_GUARD")
    external = [_pin(root, row["path"], row["sha256"]) for row in snapshot["external_pins"]]
    held = [_pin(root, row["path"], row["sha256"]) for row in snapshot["held_pins"]]
    implementation = implementation_binding(root)
    _require(_same(external, snapshot["external_pins"]) and _same(held, snapshot["held_pins"])
        and _same(implementation, snapshot["implementation"]), "CLOCK_HELD_INPUT_OR_IMPLEMENTATION_CHANGED")
    return {"external_pins": external, "held_pins": held, "implementation": implementation}


def _compose_clock_edition(output_root, inputs, targets, *, generation_id, native_universe_sha256,
        implementation, limits, prepare_plan, populate_plan, before_write, before_finish,
        build_context, software_fixture=False):
    """Internal composition; fixture callbacks/limits are software-only inputs."""
    _require(type(software_fixture) is bool, "CLOCK_SOFTWARE_FIXTURE_LABEL_REQUIRED")
    if not software_fixture:
        _require(type(inputs) is _native.NativeInputs
            and prepare_plan is _adapter.prepare_frozen_clock_plan
            and populate_plan is _adapter.populate_frozen_clock_plan and _same(limits, LIMITS),
            "CLOCK_PUBLIC_COMPOSITION_REQUIRED")
    output_root = Path(output_root).absolute()
    plan = prepare_plan(inputs, targets, cutoff_utc=PROTECTED_CUTOFF)
    payload = plan.to_dict()
    requested = [{"symbol": symbol, "timeframe": timeframe} for symbol, timeframe in sorted(targets)]
    _require(_same(payload["requested_targets"], requested) and plan.sha256 == _digest(payload),
             "CLOCK_PREPARED_SCOPE_MISMATCH")
    before_write()
    _require(not os.path.lexists(output_root), "CLOCK_ATTEMPT_ROOT_ALREADY_EXISTS_NO_RETRY")
    _spool._reject_reparse_chain(output_root.parent)
    os.mkdir(output_root)
    _spool._validate_root(output_root, create=False)
    context = _spool.freeze_generation_context(output_root / "spool", generation_id=generation_id,
        native_universe_sha256=native_universe_sha256, config_sha256=plan.sha256,
        limits=_spool.SpoolLimits(**limits))
    publisher = _publication.ClockEditionPublisher(context, plan=payload,
        implementation_sha256=implementation["implementation_sha256"])
    provenance = "SYNTHETIC_SOFTWARE_NO_NATIVE_VERIFICATION" if software_fixture else "PUBLIC_ORIGINAL_NATIVE_OWNER_PATH"
    publisher.diagnostic("clock-build-input-bindings", {"build_context": build_context,
        "implementation": implementation, "limits": limits, "execution_provenance": provenance,
        "software_fixture": software_fixture, **_publication.SCOPE})
    populate_plan(publisher, plan, inputs)
    rechecked = before_finish()
    receipt = publisher.finish()
    reopened = _publication.reopen_clock_edition(context.root, receipt,
        expected_generation_id=context.generation_id,
        expected_native_universe_sha256=native_universe_sha256,
        expected_implementation_sha256=implementation["implementation_sha256"], expected_plan_sha256=plan.sha256)
    results = []
    for row in reopened["entries"]:
        summary = {name: row[name] for name in _RESULT_FIELDS if name in row}
        summary.update({name: row[name] for name in _publication.SCOPE})
        if "reason_code" in row:
            summary["reason_code"] = row["reason_code"]
        results.append(summary)
    return {"schema_version": "fxpm.clock-build-outcome.v1",
        "status": "CLOCK_PRICE_ONLY_BUILD_COMPLETED_WITH_EXPLICIT_STOPS", "receipt": receipt,
        "plan_sha256": plan.sha256, "implementation": implementation, "results": results,
        "price_cells": sum(row["status"] == "PRICE_DATA_PUBLISHED" for row in results),
        "stop_cells": sum(row["status"] == "STOP" for row in results),
        "profile_disposition": _publication.PROFILE_DISPOSITION, "statistics": None, "profile": None,
        "build_context": build_context, "rechecked_inputs": rechecked, "software_fixture": software_fixture,
        "execution_provenance": provenance, "total_temp_rss_io_or_time_bound": False, **_publication.SCOPE}


def build_approved_clock_scope(repo_root: Path, *, expected_implementation_sha256: str,
        adoption_path=None, adoption_sha256=None, data_approval_path=None, data_approval_sha256=None) -> dict:
    """Use only explicitly pinned adopted targets and a separate one-attempt grant."""
    _external_args(adoption_path, adoption_sha256, data_approval_path, data_approval_sha256)
    root = Path(repo_root).absolute()
    implementation = implementation_binding(root)
    snapshot = _preflight(root, expected_implementation_sha256=expected_implementation_sha256,
        adoption_path=adoption_path, adoption_sha256=adoption_sha256,
        data_approval_path=data_approval_path, data_approval_sha256=data_approval_sha256,
        implementation=implementation, free_bytes=shutil.disk_usage(root).free)
    build_context = {"external_pins": snapshot["external_pins"], "held_pins": snapshot["held_pins"],
        "authority_ref": snapshot["approval"]["authority_ref"], "adopted_targets": snapshot["scope"]["targets"]}
    with _native.NativeInputs(root / "data", snapshot["activation_sha256"], snapshot["pointer_sha256"]) as inputs:
        return _compose_clock_edition(snapshot["output_root"], inputs, snapshot["targets"],
            generation_id=snapshot["generation_id"], native_universe_sha256=snapshot["activation_sha256"],
            implementation=implementation, limits=dict(LIMITS),
            prepare_plan=_adapter.prepare_frozen_clock_plan, populate_plan=_adapter.populate_frozen_clock_plan,
            before_write=lambda: _recheck(root, snapshot, require_absent_output=True),
            before_finish=lambda: _recheck(root, snapshot, require_absent_output=False),
            build_context=build_context, software_fixture=False)
