"""One approved available-history data build from authenticated native inputs.

This is a library operation, not a launcher, scheduler or financial issuer.
The external phase owner must retain its actual exit/error and returned receipt.
"""
from __future__ import annotations

from contextlib import closing
from dataclasses import asdict
import hashlib
import json
import os
from pathlib import Path
import shutil

from pm_authority_spool import (
    SpoolLimits, freeze_generation_context, _safe_regular_file,
    _reject_reparse_chain, _validate_root,
)
from pm_explicit_derived_history import (
    RECIPE_SHA256, PROTECTED_CUTOFF, DATA_SCOPE, DerivedHistoryError, canonical_json_bytes,
    derive_bars, identify_spread_rule, apply_spread_rule,
)
from pm_explicit_derived_publication import (
    DerivedEditionPublisher, PublicationError, calibrated_statistics,
    reopen_derived_edition, COST_READY, AUTHORITIES,
)


ACTIVATION_SHA256 = "2b27257f0eb191794ab3209a766a04f1be3394a394fe267bce2d0e298fa6ddf5"
ACTIVE_POINTER_SHA256 = "42f7bc9c5830578c333b1a4d34b2285ee1440ae8dc5eb894b3efed8a13149196"
HISTORICAL_CONTEXT_IDENTITY = "6fda536f66059dac893f2b326b2309ae613b62693ad85b66c50ac1034cb1fa95"
HELD_PROFILE_SHA256 = "b6be2a8f9fa2d4e39c5ad5168060e35e478f7dfbe48c18f75d4a136ade105622"
APPROVAL_SHA256 = "dd16918b558ebfc902e532651711b4e716f8c34740b0289135386935d9b06f3d"
SCOPE_ADOPTION_SHA256 = "fe4097091c10673cfa77d3cd823c7638f4cc8647e446d98cf1c15d0018ad1ac1"
RESULT_CONTEXT_FIELDS = (
    "readiness_label", "data_scope", "full_native_context_eligibility",
    "financial_consumer_ready", "required_interval", "m5_acquisition_interval",
    "m5_observed_interval", "available_interval", "omitted_intervals",
    "target_spec", "control_extent",
)
RELATIVE_QA = Path("audit_reports/fxpm_workshop_20261007/declared_task_efficiency_continuation")
LIMITS = dict(max_artifact_bytes=1073741824, max_symbol_bytes=8589934592,
              max_generation_bytes=8589934592, max_artifacts_per_symbol=100000,
              max_manifest_bytes=134217728, max_json_bytes=67108864,
              stream_chunk_bytes=1048576, min_free_bytes=17179869184)


def _digest(value) -> str:
    return hashlib.sha256(canonical_json_bytes(value)).hexdigest()


def _no_buildable_targets_error(native_plan) -> PublicationError:
    counts = {}
    for stopped in native_plan.stops:
        counts[stopped.reason_code] = counts.get(stopped.reason_code, 0) + 1
    error = PublicationError("NO_BUILDABLE_TARGETS_UNDER_APPROVED_SCOPE")
    error.stop_code_counts = dict(sorted(counts.items()))
    error.stop_count = len(native_plan.stops)
    return error


def implementation_binding(repo_root: Path) -> dict:
    """Bind exact new source and the unchanged calculation/publication helpers."""
    own = Path(__file__).absolute().parent
    paths = {name: own / name for name in (
        "pm_explicit_derived_history.py", "pm_explicit_derived_native.py",
        "pm_explicit_derived_publication.py", "pm_explicit_derived_build.py")}
    paths.update({"pm_authority_spool.py": repo_root / "pm_authority_spool.py",
                  "tools/build_spread_profile.py": repo_root / "tools/build_spread_profile.py"})
    values = []
    for name, path in sorted(paths.items()):
        root = own if path.parent == own else repo_root
        relative = path.relative_to(root).as_posix()
        safe = _safe_regular_file(root, relative)
        before = safe.stat()
        raw = safe.read_bytes()
        after = safe.stat()
        if (before.st_size, before.st_mtime_ns, before.st_ino) != (
                after.st_size, after.st_mtime_ns, after.st_ino):
            raise PublicationError("IMPLEMENTATION_CHANGED")
        values.append({"path": name, "bytes": len(raw),
                       "sha256": hashlib.sha256(raw).hexdigest()})
    return {"sources": values, "implementation_sha256": _digest(values)}


def _pin(repo_root: Path, relative: Path, expected: str, *, capture: bytearray | None = None) -> dict:
    path = _safe_regular_file(repo_root, relative.as_posix())
    before = path.stat()
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1048576), b""):
            digest.update(chunk)
            if capture is not None:
                capture.extend(chunk)
    after = path.stat()
    if (digest.hexdigest() != expected or (before.st_size, before.st_mtime_ns, before.st_ino)
            != (after.st_size, after.st_mtime_ns, after.st_ino)):
        raise PublicationError("HELD_INPUT_CHANGED")
    return {"path": relative.as_posix(), "sha256": expected,
            "bytes": after.st_size, "mtime_ns": after.st_mtime_ns}


def populate_frozen_plan(publisher: DerivedEditionPublisher, native_plan, reader) -> None:
    """Process caller-authenticated sources; this helper creates no native trust.

    The real entry supplies the original native input owner. Synthetic tests
    supply ordinary owned data readers, never a replacement native verifier.
    """
    from pm_explicit_derived_native import NativeInputsError, exception_details

    def record_error(identity: str, error: BaseException, *, status: str) -> None:
        if getattr(error, "abort_attempt", False):
            raise error
        publisher.diagnostic(identity, {"status": status,
            "reason_code": getattr(error, "code", type(error).__name__),
            **exception_details(error), **AUTHORITIES})

    for stopped in native_plan.stops:
        publisher.diagnostic("metadata-stop:" + _digest([stopped.symbol, stopped.timeframe]),
                             {"status": "METADATA_STOP", **asdict(stopped), **AUTHORITIES})
        publisher.stop(stopped.symbol, stopped.timeframe, stopped.reason_code)
    identifications, control_stops = {}, {}
    for control in native_plan.controls:
        identity = _digest(asdict(control.stratum))
        try:
            with closing(reader.iter_bars(control.m5, control.interval)) as source:
                derived = derive_bars(source, stratum=control.stratum,
                    source=control.m5.source_identity, interval=control.interval,
                    cutoff_utc=PROTECTED_CUTOFF)
            with closing(reader.iter_bars(control.native, control.interval)) as source:
                identification = identify_spread_rule(
                    derived, source, native_source=control.native.source_identity)
            identifications[control.stratum] = identification
            publisher.diagnostic(identity, {"status": "CONTROL_COMPARED",
                "identification": asdict(identification), **AUTHORITIES})
        except (DerivedHistoryError, NativeInputsError) as error:
            record_error(identity, error, status="CONTROL_STOP")
            control_stops[control.stratum] = error.code
    for target in native_plan.targets:
        identity = "target:" + _digest([target.symbol, target.timeframe])
        if target.stratum in control_stops:
            publisher.stop(target.symbol, target.timeframe, control_stops[target.stratum])
            continue
        identification = identifications.get(target.stratum)
        if identification is None:
            publisher.stop(target.symbol, target.timeframe, "FROZEN_CONTROL_UNAVAILABLE")
            continue
        try:
            with closing(reader.iter_bars(target.m5, target.interval)) as source:
                series = derive_bars(source, stratum=target.stratum,
                    source=target.m5.source_identity, interval=target.interval,
                    cutoff_utc=PROTECTED_CUTOFF)
            if not series.bars:
                publisher.stop(target.symbol, target.timeframe, "TARGET_SOURCE_PREFIX_EMPTY")
                continue
            series = apply_spread_rule(series, identification)
            try:
                fine = reader.calibration_source(target.m5, PROTECTED_CUTOFF)
                statistics = calibrated_statistics(series, fine)
            except (ValueError, KeyError, TypeError) as error:
                if isinstance(error, NativeInputsError):
                    raise
                statistics = {"status": "CALIBRATION_UNAVAILABLE",
                              "reason_code": type(error).__name__, **exception_details(error)}
            publisher.add_series(series, identification, statistics=statistics)
        except (DerivedHistoryError, NativeInputsError) as error:
            record_error(identity, error, status="TARGET_STOP")
            publisher.stop(target.symbol, target.timeframe, error.code)


def build_approved_scope(repo_root: Path, *, expected_implementation_sha256: str) -> dict:
    """Attempt the approved scope once; refuse existing roots and retain partials."""
    from pm_explicit_derived_native import NativeInputs, NativeInputsError
    repo_root = Path(repo_root).absolute()
    output_root = repo_root / RELATIVE_QA / "explicit_m5_derived_001"
    if os.path.lexists(output_root):
        raise PublicationError("DERIVED_ATTEMPT_ROOT_ALREADY_EXISTS_NO_RETRY")
    implementation = implementation_binding(repo_root)
    if implementation["implementation_sha256"] != expected_implementation_sha256:
        raise PublicationError("IMPLEMENTATION_CHANGED")
    recipe_path = RELATIVE_QA / "explicit_m5_derived_recipe_005.adopted.json"
    approval_path = RELATIVE_QA / "explicit_derived_operator_approval_001.json"
    adoption_path = RELATIVE_QA / "explicit_derived_root_scope_adoption_005.json"
    recipe_bytes = bytearray()
    _pin(repo_root, recipe_path, RECIPE_SHA256, capture=recipe_bytes)
    _pin(repo_root, approval_path, APPROVAL_SHA256)
    _pin(repo_root, adoption_path, SCOPE_ADOPTION_SHA256)
    recipe = json.loads(recipe_bytes)
    held_path = Path("data/native/terms/spread_profile.json")
    held_before = _pin(repo_root, held_path, HELD_PROFILE_SHA256)
    if shutil.disk_usage(repo_root).free < LIMITS["min_free_bytes"]:
        raise PublicationError("MIN_FREE_DISK_GUARD")
    targets = tuple((row["symbol"], row["timeframe"])
                    for row in recipe["selection"]["target_list"])
    with NativeInputs(repo_root / "data", ACTIVATION_SHA256, ACTIVE_POINTER_SHA256) as inputs:
        native_plan = inputs.plan(targets, PROTECTED_CUTOFF)
        if not native_plan.targets:
            raise _no_buildable_targets_error(native_plan)
        plan = {"schema_version": "fxpm.explicit-derived-plan.v2",
                "recipe_sha256": RECIPE_SHA256, "approval_sha256": APPROVAL_SHA256,
                "scope_adoption_sha256": SCOPE_ADOPTION_SHA256,
                "data_scope": DATA_SCOPE,
                "readiness_label": DATA_SCOPE,
                "full_native_context_eligibility": False,
                "financial_consumer_ready": False,
                "targets": recipe["selection"]["target_list"],
                "native_plan": native_plan.to_dict(), "implementation": implementation,
                "held_profile": held_before, "limits": LIMITS,
                "accepted_historical_context_identity": HISTORICAL_CONTEXT_IDENTITY,
                "context_native_identity_semantics": "RAW_VERIFIED_ACTIVATION_MANIFEST_SHA256_NOT_HISTORICAL_CONTEXT_IDENTITY",
                "authority_scope": "DATA_ONLY_NO_FINANCIAL_ISSUANCE", **AUTHORITIES}
        # Exclusive scope root; normal spool creation only creates its final leaf.
        _reject_reparse_chain(output_root.parent)
        os.mkdir(output_root)
        _validate_root(output_root, create=False)
        # Fresh data context; no process/precommit/source-window API is involved.
        context = freeze_generation_context(
            output_root / "spool", generation_id="explicit-m5-derived-001",
            native_universe_sha256=ACTIVATION_SHA256,
            config_sha256=_digest(plan), limits=SpoolLimits(**LIMITS))
        publisher = DerivedEditionPublisher(
            context, plan=plan, implementation_sha256=expected_implementation_sha256)
        populate_frozen_plan(publisher, native_plan, inputs)
        inputs.revalidate()
        held_after = _pin(repo_root, held_path, HELD_PROFILE_SHA256)
        if held_after != held_before or implementation_binding(repo_root) != implementation:
            raise PublicationError("HELD_INPUT_OR_IMPLEMENTATION_CHANGED")
        receipt = publisher.finish()
    reopened = reopen_derived_edition(
        context.root, receipt, expected_generation_id=context.generation_id,
        expected_native_universe_sha256=ACTIVATION_SHA256,
        expected_implementation_sha256=expected_implementation_sha256,
        expected_plan_sha256=_digest(plan))
    return {"schema_version": "fxpm.explicit-derived-data-outcome.v1",
            "status": "AVAILABLE_DATA_ONLY_BUILD_COMPLETED_WITH_EXPLICIT_UNAVAILABLE",
            "data_scope": DATA_SCOPE,
            "readiness_label": DATA_SCOPE,
            "full_native_context_eligibility": False,
            "financial_consumer_ready": False,
            "receipt": receipt, "plan_sha256": _digest(plan),
            "implementation": implementation,
            "price_cells": sum(row["status"] == "PRICE_DATA_PUBLISHED" for row in reopened["entries"]),
            "cost_ready_data_cells": sum(row["cost_status"] == COST_READY for row in reopened["entries"]),
            "results": [{k: row[k] for k in (
                            "symbol", "timeframe", "status", "cost_status",
                            *RESULT_CONTEXT_FIELDS)}
                        | ({"reason_code": row["reason_code"]} if "reason_code" in row else {})
                        for row in reopened["entries"]],
            "held_profile_before": held_before, "held_profile_after": held_after,
            "financial_consumer_integration": "NOT_GRANTED_OR_IMPLEMENTED_BY_THIS_DATA_READER",
            "total_temp_rss_io_or_time_bound": False, **AUTHORITIES}
