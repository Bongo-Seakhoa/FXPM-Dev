"""Explicit derived data publication and cold data inspection, never native trust.

The low-level publisher accepts caller-authenticated inputs. The data-only
workflow authenticates real inputs through NativeInputs before using it. This
schema is intentionally separate from native products and frozen native tiers.
"""
from __future__ import annotations

from dataclasses import asdict
from datetime import datetime, timezone
import csv
import hashlib
import io
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np

from pm_authority_spool import (
    GenerationSpoolContext, open_symbol_sink, verify_symbol_receipt,
    GENERAL_MANIFEST_DOMAIN, _stream_file, _strict_json_bytes,
)
from tools.build_spread_profile import aligned_statistics
from pm_explicit_derived_history import (
    Bar, DerivedBar, DerivedSeries, Interval, SourceIdentity, Stratum,
    SpreadIdentification, RECIPE_SHA256, PROTECTED_CUTOFF,
    canonical_json_bytes, apply_spread_rule, require_spread_ready,
    build_lineage,
)


DOMAIN = GENERAL_MANIFEST_DOMAIN
DATA_SCOPE = "AVAILABLE_DATA_ONLY_NOT_EXECUTION_READY"
SCOPE_ADOPTION_SHA256 = "fe4097091c10673cfa77d3cd823c7638f4cc8647e446d98cf1c15d0018ad1ac1"
_EXTENT_FIELDS = ("required_interval", "m5_acquisition_interval", "m5_observed_interval",
                  "available_interval", "omitted_intervals")
_SPEC_FIELDS = frozenset(("symbol", "timeframe", "native_status", "native_reason_codes",
                         "native_entry_sha256", "run_manifest_sha256", "run_request_sha256",
                         "target_source_request_interval", "target_calendar_boundaries",
                         "point", "instrument_session_class", "source_identity_from"))
EDITION_SYMBOL = "EXPLICIT_DERIVED_EDITION"
AUTHORITIES = {"qualification_authority": False, "capital_authority": False,
               "live_authority": False, "order_authority": False}
COST_READY = "DERIVED_DATA_COST_READY_NOT_FINANCIAL_AUTHORITY"
CSV_FIELDS = ("time", "_BAR_CLOSE_TS", "Open", "High", "Low", "Close",
              "Volume", "RealVolume", "Spread")


class PublicationError(ValueError):
    def __init__(self, code: str) -> None:
        self.code = code
        super().__init__(code)


def _fail(code: str = "EDITION_BINDING_MISMATCH") -> None:
    raise PublicationError(code)


def _sha(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _digest(value: Any) -> str:
    return _sha(canonical_json_bytes(value))


def _checked_digest(value: Any) -> str:
    if (type(value) is not str or len(value) != 64
            or any(c not in "0123456789abcdef" for c in value)):
        _fail()
    return value


def _detached(value: Any) -> Any:
    return json.loads(canonical_json_bytes(value))


def _ns(value: str) -> int:
    stamp = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if stamp.tzinfo is None or stamp.utcoffset().total_seconds() != 0:
        _fail()
    delta = stamp - datetime(1970, 1, 1, tzinfo=timezone.utc)
    return (delta.days * 86400 + delta.seconds) * 1_000_000_000 + delta.microseconds * 1000


def calibrated_statistics(series: DerivedSeries, fine: dict) -> dict:
    """Reuse the original exact-prior-close calculation without changing it."""
    target = {
        "opens_ns": np.asarray([_ns(row.value.open_utc) for row in series.bars], dtype=np.int64),
        "closes_ns": np.asarray([_ns(row.value.close_utc) for row in series.bars], dtype=np.int64),
    }
    return aligned_statistics(target, fine, series.source.point)


def _cost_ready(series: DerivedSeries, statistics: dict) -> bool:
    try:
        require_spread_ready(series)
    except ValueError:
        return False
    return (
        statistics.get("status") == "CALCULATED"
        and statistics.get("quantile_method") == "higher_order_statistic_ceil((n-1)*p)"
        and statistics.get("point_price") == series.source.point
        and statistics.get("target_prefix_rows") == len(series.bars)
        and type(statistics.get("invalid_rows")) is int
        and statistics["invalid_rows"] == 0
        and type(statistics.get("valid_rows")) is int
        and statistics["valid_rows"] > 0
        and statistics.get("tiers", {}).get("P75", {}).get("usability") == "POSITIVE_QUANTILE"
    )


def _csv_chunks(series: DerivedSeries):
    buffer = io.StringIO(newline="")
    writer = csv.writer(buffer, lineterminator="\n")
    writer.writerow(CSV_FIELDS)
    yield buffer.getvalue().encode("ascii")
    for row in series.bars:
        value = row.value
        buffer.seek(0)
        buffer.truncate(0)
        writer.writerow((value.open_utc, value.close_utc, value.open, value.high,
                         value.low, value.close, value.volume,
                         "" if value.real_volume is None else value.real_volume,
                         "" if value.spread is None else value.spread))
        yield buffer.getvalue().encode("ascii")


def _members_chunks(series: DerivedSeries):
    for row in series.bars:
        yield canonical_json_bytes(asdict(row)) + b"\n"


def _series_metadata(series: DerivedSeries) -> dict:
    return {name: _detached(value) for name, value in asdict(series).items() if name != "bars"}


def _coverage_disclosure(statistics: dict) -> dict:
    """Disclose available exact-prior coverage without changing calibration."""
    return {"rule": "AVAILABLE_EXACT_PRIOR_CLOSE_OBSERVATIONS_ONLY_NO_NEAREST_FILL",
            "target_prefix_rows": statistics.get("target_prefix_rows"),
            "matched_rows": statistics.get("matched_rows"),
            "missing_exact_prior_close_rows": statistics.get("missing_exact_prior_close_rows"),
            "invalid_rows": statistics.get("invalid_rows"),
            "financial_consumer_ready": False}


def _interval_bounds(value: dict) -> tuple[datetime, datetime]:
    if type(value) is not dict or set(value) != {"start_utc", "end_exclusive_utc"}:
        _fail()
    try:
        start, end = (datetime.fromisoformat(value[key]) for key in ("start_utc", "end_exclusive_utc"))
        if (start.utcoffset() != timezone.utc.utcoffset(start)
                or end.utcoffset() != timezone.utc.utcoffset(end) or start >= end):
            _fail()
        return start, end
    except (TypeError, ValueError) as error:
        if isinstance(error, PublicationError):
            raise
        _fail()


def _checked_extent(value: dict) -> dict:
    if type(value) is not dict or set(value) != set(_EXTENT_FIELDS):
        _fail()
    extent = _detached(value)
    required, acquired, observed, available = (
        _interval_bounds(extent[name]) for name in _EXTENT_FIELDS[:4])
    if (available[0] < max(required[0], acquired[0], observed[0])
            or available[1] > min(required[1], acquired[1], observed[1])):
        _fail()
    expected = []
    if required[0] < available[0]:
        expected.append({"start_utc": extent["required_interval"]["start_utc"],
                         "end_exclusive_utc": extent["available_interval"]["start_utc"]})
    if available[1] < required[1]:
        expected.append({"start_utc": extent["available_interval"]["end_exclusive_utc"],
                         "end_exclusive_utc": extent["required_interval"]["end_exclusive_utc"]})
    if (type(extent["omitted_intervals"]) is not list
            or canonical_json_bytes(extent["omitted_intervals"]) != canonical_json_bytes(expected)):
        _fail()
    return extent


def _context_fields(target_spec, extent, control_extent) -> dict:
    return {"data_scope": DATA_SCOPE, "readiness_label": DATA_SCOPE,
            "full_native_context_eligibility": False, "financial_consumer_ready": False,
            "target_spec": _detached(target_spec),
            **({name: _detached(extent[name]) for name in _EXTENT_FIELDS}
               if extent is not None else {name: None for name in _EXTENT_FIELDS}),
            "control_extent": _detached(control_extent), **AUTHORITIES}


def _check_context_fields(actual: dict, expected: dict) -> None:
    if any(name not in actual or canonical_json_bytes(actual[name]) != canonical_json_bytes(value)
           for name, value in expected.items()):
        _fail()


def _checked_plan_scope(plan: dict) -> dict:
    native = plan["native_plan"]
    if (plan.get("schema_version") != "fxpm.explicit-derived-plan.v2"
            or plan.get("data_scope") != DATA_SCOPE
            or plan.get("readiness_label") != DATA_SCOPE
            or plan.get("scope_adoption_sha256") != SCOPE_ADOPTION_SHA256
            or plan.get("full_native_context_eligibility") is not False
            or plan.get("financial_consumer_ready") is not False
            or native.get("schema_version") != "fxpm.explicit-derived-plan.v2"
            or native.get("scope") != "ONE_AVAILABLE_HISTORY_M5_DERIVED_DATA_ONLY_ATTEMPT"
            or native.get("cutoff_utc") != PROTECTED_CUTOFF):
        _fail()
    return native


def _row_scope(row: dict) -> None:
    if (row.get("readiness_label") != DATA_SCOPE
            or row.get("full_native_context_eligibility") is not False
            or row.get("financial_consumer_ready") is not False):
        _fail()


def _check_plan_join(plan: dict, series: DerivedSeries,
                     identification: SpreadIdentification) -> dict:
    """Bind available data, technical target specification and all frozen extents."""
    try:
        def same(left, right):
            return canonical_json_bytes(left) == canonical_json_bytes(right)
        native_plan = _checked_plan_scope(plan)
        targets = [row for row in native_plan["targets"] if (row["symbol"], row["timeframe"]) ==
                   (series.source.symbol, series.stratum.timeframe)]
        if len(targets) != 1:
            _fail()
        target = targets[0]
        stratum = _detached(asdict(series.stratum))
        controls = [row for row in native_plan["controls"] if same(row["stratum"], stratum)]
        if len(controls) != 1:
            _fail()
        control = controls[0]
        _row_scope(target)
        _row_scope(control)
        extent, control_extent = _checked_extent(target["extent"]), _checked_extent(control["extent"])
        spec = target["target_spec"]
        if (type(spec) is not dict or set(spec) != _SPEC_FIELDS
                or spec["symbol"] != series.source.symbol or spec["timeframe"] != series.stratum.timeframe
                or spec["native_status"] != "technical_error"
                or spec["source_identity_from"] != "ACCEPTED_SAME_SYMBOL_M5"
                or type(spec["native_reason_codes"]) is not list
                or any(type(code) is not str for code in spec["native_reason_codes"])
                or type(spec["target_calendar_boundaries"]) is not dict
                or not same(spec["point"], series.source.point)
                or spec["instrument_session_class"] != series.stratum.instrument_session_class):
            _fail()
        for name in ("native_entry_sha256", "run_manifest_sha256", "run_request_sha256"):
            _checked_digest(spec[name])
        _interval_bounds(spec["target_source_request_interval"])
        if (not same(target["stratum"], stratum)
                or not same(target["interval"], _detached(asdict(series.interval)))
                or not same(target["interval"], extent["available_interval"])
                or not same(target["m5"]["interval"], extent["available_interval"])
                or not same(target["m5"]["source_identity"], _detached(asdict(series.source)))
                or series.cutoff_utc != native_plan["cutoff_utc"]
                or series.cutoff_utc != target["m5"]["cutoff_utc"]
                or not same(_detached(asdict(identification.stratum)), stratum)
                or not same(control["m5"]["source_identity"], _detached(asdict(identification.control_source)))
                or not same(control["native"]["source_identity"], _detached(asdict(identification.native_source)))
                or not same(control["interval"], _detached(asdict(identification.interval)))
                or not same(control["interval"], control_extent["available_interval"])
                or not same(control["m5"]["interval"], control_extent["available_interval"])
                or not same(control["native"]["interval"], control_extent["available_interval"])):
            _fail()
        return _context_fields(spec, extent, control_extent)
    except (KeyError, TypeError, AttributeError, ValueError) as error:
        if isinstance(error, PublicationError):
            raise
        _fail()


def _stop_context(plan: dict, symbol: str, timeframe: str) -> dict:
    try:
        native = _checked_plan_scope(plan)
        targets = [row for row in native["targets"] if (row["symbol"], row["timeframe"]) == (symbol, timeframe)]
        stopped = [row for row in native["stops"] if (row["symbol"], row["timeframe"]) == (symbol, timeframe)]
        rows = targets if targets else stopped
        if len(rows) > 1:
            _fail()
        row = rows[0] if rows else {}
        if row:
            _row_scope(row)
        extent = _checked_extent(row["extent"]) if row.get("extent") is not None else None
        controls = [item for item in native["controls"] if item["stratum"] == row.get("stratum")]
        if len(controls) > 1:
            _fail()
        control_extent = _checked_extent(controls[0]["extent"]) if controls else None
        return _context_fields(row.get("target_spec"), extent, control_extent)
    except (KeyError, TypeError, AttributeError, ValueError) as error:
        if isinstance(error, PublicationError):
            raise
        _fail()


def _scoped_lineage(series: DerivedSeries, context_fields: dict, *, output_sha256: str,
                    implementation_sha256: str, derived_profile_sha256: str) -> dict:
    body = json.loads(build_lineage(series, output_sha256=output_sha256,
                                  implementation_sha256=implementation_sha256,
                                  derived_profile_sha256=derived_profile_sha256))
    body.pop("lineage_sha256")
    body["series_interval"] = _detached(asdict(series.interval))
    body.update(_detached(context_fields))
    return {**body, "lineage_sha256": _digest(body)}


def _check_publication_rosters(entries: list, profile: dict) -> None:
    """Require one profile disposition for each activation entry."""
    try:
        def keyed(rows):
            result = {(row["symbol"], row["timeframe"]): row for row in rows}
            if len(result) != len(rows):
                _fail()
            return result
        actual = keyed(entries)
        groups = keyed(profile["groups"])
        unavailable = keyed(profile["unavailable"])
        series_keys = {key for key, row in actual.items() if row["status"] == "PRICE_DATA_PUBLISHED"}
        stop_keys = {key for key, row in actual.items() if row["status"] == "STOP"}
        if (series_keys | stop_keys != set(actual)
                or set(groups) != series_keys or set(unavailable) != stop_keys
                or any(canonical_json_bytes(unavailable[key]) != canonical_json_bytes(actual[key])
                       for key in stop_keys)):
            _fail()
    except (KeyError, TypeError, AttributeError, ValueError) as error:
        if isinstance(error, PublicationError):
            raise
        _fail()


class DerivedEditionPublisher:
    """One bounded immutable edition; authentication belongs to its caller."""

    def __init__(self, context: GenerationSpoolContext, *, plan: dict,
                 implementation_sha256: str) -> None:
        self.context = context
        self.implementation_sha256 = _checked_digest(implementation_sha256)
        self._plan_json = canonical_json_bytes(_detached(plan))
        self.plan = json.loads(self._plan_json)
        self.plan_sha256 = _sha(self._plan_json)
        if (self.plan.get("schema_version") != "fxpm.explicit-derived-plan.v2"
                or self.plan.get("recipe_sha256") != RECIPE_SHA256
                or context.config_sha256 != self.plan_sha256):
            _fail()
        _checked_plan_scope(self.plan)
        targets = self.plan.get("targets")
        if type(targets) is not list or not 0 < len(targets) <= 35:
            _fail("TARGET_PLAN_INVALID")
        try:
            keys = [(row["symbol"], row["timeframe"]) for row in targets]
        except (KeyError, TypeError):
            _fail("TARGET_PLAN_INVALID")
        if (len(set(keys)) != len(keys) or any(
                type(s) is not str or not s or tf not in ("M15", "M30", "H1", "H4", "D1")
                for s, tf in keys)):
            _fail("TARGET_PLAN_INVALID")
        self.targets = frozenset(keys)
        self.entries: dict[tuple[str, str], dict] = {}
        self.groups: dict[tuple[str, str], dict] = {}
        self.finished = False
        self.sink = open_symbol_sink(context, EDITION_SYMBOL, manifest_domain=DOMAIN)
        # Written before any fidelity result; the workflow freezes all controls here.
        self.plan_artifact = self.sink.put_json("plan", "explicit_derived_plan", self.plan)

    def _new_key(self, symbol: str, timeframe: str) -> tuple[str, str]:
        if self.finished:
            _fail("EDITION_ALREADY_FINISHED")
        key = (symbol, timeframe)
        if key not in self.targets:
            _fail("TARGET_NOT_PLANNED")
        if key in self.entries:
            _fail("TARGET_ALREADY_RECORDED")
        return key

    def stop(self, symbol: str, timeframe: str, reason_code: str) -> None:
        key = self._new_key(symbol, timeframe)
        if type(reason_code) is not str or not reason_code:
            _fail()
        self.entries[key] = {"symbol": symbol, "timeframe": timeframe,
                             "status": "STOP", "reason_code": reason_code,
                             "cost_status": "COST_UNAVAILABLE",
                             **_stop_context(json.loads(self._plan_json), symbol, timeframe)}

    def diagnostic(self, identity: str, value: dict) -> None:
        if self.finished:
            _fail("EDITION_ALREADY_FINISHED")
        self.sink.put_json("diagnostic:" + identity, "explicit_derived_diagnostic", _detached(value))

    def add_series(self, series: DerivedSeries, identification: SpreadIdentification,
                   *, statistics: dict, target_spec: dict | None = None,
                   extent: dict | None = None, control_extent: dict | None = None) -> None:
        key = self._new_key(series.source.symbol, series.stratum.timeframe)
        context_fields = _check_plan_join(json.loads(self._plan_json), series, identification)
        assertions = {"target_spec": target_spec, "control_extent": control_extent}
        if extent is not None:
            assertions.update(_checked_extent(extent))
        for name, value in assertions.items():
            if value is not None and canonical_json_bytes(value) != canonical_json_bytes(context_fields[name]):
                _fail()
        # This checks membership and the identification's canonical body as well.
        expected = apply_spread_rule(series, identification)
        if expected != series or not series.bars:
            _fail("SERIES_OR_IDENTIFICATION_MISMATCH")
        statistics = _detached(statistics)
        tag = _digest(list(key))[:24]
        bars = self.sink.put_opaque(tag + ":bars", "explicit_derived_bars",
                                    _csv_chunks(series), format="csv")
        members = self.sink.put_opaque(tag + ":membership", "explicit_derived_membership",
                                       _members_chunks(series), format="jsonl")
        evidence = self.sink.put_json(tag + ":identification", "explicit_derived_control",
                                      _detached(asdict(identification)))
        cost_status = COST_READY if _cost_ready(series, statistics) else "COST_UNAVAILABLE"
        group = {"symbol": key[0], "timeframe": key[1], "source_kind": "m5_derived",
                 "cost_status": cost_status, "statistics": statistics,
                 "calibration_coverage": _coverage_disclosure(statistics),
                 "financial_consumer_ready": False,
                 "target_bars_sha256": bars["descriptor"]["sha256"],
                 "membership_sha256": series.membership_sha256,
                 "fine_source": asdict(series.source),
                 "spread_identification_sha256": identification.evidence_sha256,
                 **context_fields, **AUTHORITIES}
        self.groups[key] = group
        self.entries[key] = {"symbol": key[0], "timeframe": key[1],
                             "status": "PRICE_DATA_PUBLISHED", "cost_status": cost_status,
                             "bars": bars, "membership": members,
                             "identification": evidence,
                             "series": _series_metadata(series), **context_fields}

    def finish(self) -> dict:
        if self.finished:
            _fail("EDITION_ALREADY_FINISHED")
        if set(self.entries) != self.targets:
            _fail("TARGET_RESULTS_INCOMPLETE")
        profile = {"schema_version": "fxpm.explicit-derived-spread-profile.v1",
                   "source_kind": "m5_derived", "recipe_sha256": RECIPE_SHA256,
                   "implementation_sha256": self.implementation_sha256,
                   "plan_sha256": self.plan_sha256,
                   "protected_cutoff_utc": PROTECTED_CUTOFF,
                   "calculation_policy": "ONE_TIME_OFFLINE_NO_RUNTIME_RECALCULATION",
                   "source_convention": "FINEST_ACTIVATED_CANDIDATE_FIXED_PER_SYMBOL_EXACT_PRIOR_CLOSE_AT_TARGET_OPEN",
                   "calibration_scope": "RETROSPECTIVE_FIXED_SPREAD_SCENARIO_NOT_HISTORICALLY_KNOWN_THRESHOLDS",
                   "spread_semantics": "CONTROL_MATCHED_DERIVED_STORED_POINTS_NOT_EXECUTION_QUOTES",
                   "financial_consumer_ready": False,
                   "data_scope": DATA_SCOPE, "readiness_label": DATA_SCOPE,
                   "full_native_context_eligibility": False,
                   "groups": [self.groups[key] for key in sorted(self.groups)],
                   "unavailable": [self.entries[key] for key in sorted(self.entries)
                                   if key not in self.groups], **AUTHORITIES}
        profile_artifact = self.sink.put_json("profile", "explicit_derived_profile", profile)
        entries = []
        for key in sorted(self.entries):
            entry = self.entries[key]
            if key in self.groups:
                series = _restore_series(entry["series"], list(_members_chunks_from_entry(self, entry)))
                context_fields = {key: entry[key] for key in _context_fields(None, None, None)}
                lineage = _scoped_lineage(
                    series, context_fields, output_sha256=entry["bars"]["descriptor"]["sha256"],
                    implementation_sha256=self.implementation_sha256,
                    derived_profile_sha256=profile_artifact["descriptor"]["sha256"])
                entry["lineage"] = self.sink.put_json(
                    _digest(list(key))[:24] + ":lineage", "explicit_derived_lineage", lineage)
            entries.append(entry)
        _check_publication_rosters(entries, profile)
        activation = {"schema_version": "fxpm.explicit-derived-activation.v1",
                      "source_kind": "m5_derived", "recipe_sha256": RECIPE_SHA256,
                      "implementation_sha256": self.implementation_sha256,
                      "plan_sha256": self.plan_sha256, "plan": self.plan_artifact,
                      "profile": profile_artifact, "entries": entries,
                      "financial_consumer_integration": "NOT_GRANTED_OR_IMPLEMENTED_BY_THIS_DATA_READER",
                      "financial_consumer_ready": False,
                      "data_scope": DATA_SCOPE, "readiness_label": DATA_SCOPE,
                      "full_native_context_eligibility": False, **AUTHORITIES}
        self.sink.put_json("activation", "explicit_derived_activation", activation)
        receipt = self.sink.seal()
        self.finished = True
        return receipt


def _read(context: GenerationSpoolContext, descriptor: dict, *, json_value: bool = False):
    digest, size, raw = _stream_file(
        context.root, descriptor["relative_path"],
        maximum_bytes=(context.limits.max_json_bytes if json_value else context.limits.max_artifact_bytes),
        expected_size=descriptor["size_bytes"], collect=True,
        chunk_bytes=context.limits.stream_chunk_bytes)
    if digest != descriptor["sha256"] or size != descriptor["size_bytes"]:
        _fail()
    return _strict_json_bytes(raw, context.limits.max_json_bytes) if json_value else raw


def _members_chunks_from_entry(publisher: DerivedEditionPublisher, entry: dict):
    raw = _read(publisher.context, entry["membership"]["descriptor"])
    for line in raw.splitlines():
        yield _strict_json_bytes(line, publisher.context.limits.max_json_bytes)


def _restore_series(meta: dict, members: list[dict]) -> DerivedSeries:
    values = dict(meta)
    values["source"] = SourceIdentity(**values["source"])
    values["stratum"] = Stratum(**values["stratum"])
    values["interval"] = Interval(**values["interval"])
    values["bars"] = tuple(DerivedBar(
        Bar(**row["value"]), row["member_count"], row["membership_sha256"],
        tuple(row["spread_candidates"])) for row in members)
    return DerivedSeries(**values)


def _restore_identification(value: dict) -> SpreadIdentification:
    values = dict(value)
    values["stratum"] = Stratum(**values["stratum"])
    values["control_source"] = SourceIdentity(**values["control_source"])
    values["native_source"] = SourceIdentity(**values["native_source"])
    values["interval"] = Interval(**values["interval"])
    values["mismatch_counts"] = tuple(tuple(row) for row in values["mismatch_counts"])
    values["matching_candidates"] = tuple(values["matching_candidates"])
    return SpreadIdentification(**values)


def reopen_derived_edition(root: Path, receipt: dict, *, expected_generation_id: str,
                           expected_native_universe_sha256: str,
                           expected_implementation_sha256: str,
                           expected_plan_sha256: str) -> dict:
    """Verify a separate derived edition and read data; grant no model capability."""
    from pm_authority_spool import reopen_generation_context
    manifest = verify_symbol_receipt(
        root, receipt, expected_generation_id=expected_generation_id,
        expected_symbol=EDITION_SYMBOL,
        expected_native_universe_sha256=expected_native_universe_sha256,
        expected_config_sha256=expected_plan_sha256, expected_manifest_domain=DOMAIN)
    context = reopen_generation_context(
        root, expected_generation_id=expected_generation_id,
        expected_native_universe_sha256=expected_native_universe_sha256,
        expected_config_sha256=expected_plan_sha256)
    roster = {row["artifact_id"]: row for row in manifest["artifacts"]}

    def registered(value: dict, *, json_value: bool = False):
        if roster.get(value.get("artifact_id")) != value:
            _fail()
        return _read(context, value["descriptor"], json_value=json_value)

    try:
        activation = registered(roster["activation"], json_value=True)
        profile = registered(activation["profile"], json_value=True)
        plan = registered(activation["plan"], json_value=True)
        bindings = {"source_kind": "m5_derived", "recipe_sha256": RECIPE_SHA256,
                    "implementation_sha256": expected_implementation_sha256,
                    "plan_sha256": expected_plan_sha256,
                    "financial_consumer_ready": False,
                    "data_scope": DATA_SCOPE, "readiness_label": DATA_SCOPE,
                    "full_native_context_eligibility": False, **AUTHORITIES}
        if (activation.get("schema_version") != "fxpm.explicit-derived-activation.v1"
                or profile.get("schema_version") != "fxpm.explicit-derived-spread-profile.v1"
                or any(obj.get(key) != expected for obj in (activation, profile)
                       for key, expected in bindings.items())
                or _digest(plan) != expected_plan_sha256
                or plan.get("recipe_sha256") != RECIPE_SHA256):
            _fail()
        _checked_plan_scope(plan)
        expected_keys = {(row["symbol"], row["timeframe"]) for row in plan["targets"]}
        groups = {(row["symbol"], row["timeframe"]): row for row in profile["groups"]}
        _check_publication_rosters(activation["entries"], profile)
        results = []
        for entry in activation["entries"]:
            result = {"symbol": entry["symbol"], "timeframe": entry["timeframe"],
                      "cost_status": entry["cost_status"], "status": entry["status"],
                      "financial_consumer_ready": False,
                    "data_scope": DATA_SCOPE, "readiness_label": DATA_SCOPE,
                    "full_native_context_eligibility": False, **AUTHORITIES}
            key = (entry["symbol"], entry["timeframe"])
            if entry["status"] == "STOP":
                if key in groups or entry["cost_status"] != "COST_UNAVAILABLE":
                    _fail()
                context_fields = _stop_context(plan, *key)
                _check_context_fields(entry, context_fields)
                result.update(context_fields)
                result["reason_code"] = entry["reason_code"]
                results.append(result)
                continue
            members = registered(entry["membership"])
            series = _restore_series(entry["series"], [
                _strict_json_bytes(line, context.limits.max_json_bytes) for line in members.splitlines()])
            identification = _restore_identification(registered(entry["identification"], json_value=True))
            context_fields = _check_plan_join(plan, series, identification)
            _check_context_fields(entry, context_fields)
            if (apply_spread_rule(series, identification) != series
                    or registered(entry["bars"]) != b"".join(_csv_chunks(series))
                    or key != (series.source.symbol, series.stratum.timeframe)):
                _fail()
            group = groups[key]
            _check_context_fields(group, context_fields)
            if (group["target_bars_sha256"] != entry["bars"]["descriptor"]["sha256"]
                    or group["fine_source"] != asdict(series.source)
                    or group["membership_sha256"] != series.membership_sha256
                    or group["spread_identification_sha256"] != identification.evidence_sha256
                    or any(group.get(k) is not False for k in AUTHORITIES)
                    or group["cost_status"] != entry["cost_status"]
                    or group.get("calibration_coverage") != _coverage_disclosure(group["statistics"])
                    or group.get("financial_consumer_ready") is not False
                    or (group["cost_status"] == COST_READY) != _cost_ready(series, group["statistics"])):
                _fail()
            lineage = registered(entry["lineage"], json_value=True)
            if canonical_json_bytes(lineage) != canonical_json_bytes(_scoped_lineage(
                    series, context_fields, output_sha256=entry["bars"]["descriptor"]["sha256"],
                    implementation_sha256=expected_implementation_sha256,
                    derived_profile_sha256=activation["profile"]["descriptor"]["sha256"])):
                _fail()
            result.update(context_fields, bars=tuple(row.value for row in series.bars), profile=group)
            results.append(result)
        keys = [(row["symbol"], row["timeframe"]) for row in results]
        if len(keys) != len(set(keys)) or set(keys) != expected_keys:
            _fail()
        return {"source_kind": "m5_derived", "entries": results, "profile": profile,
                "plan_sha256": expected_plan_sha256,
                "financial_consumer_integration": activation["financial_consumer_integration"],
                "financial_consumer_ready": False,
                "data_scope": DATA_SCOPE, "readiness_label": DATA_SCOPE,
                "full_native_context_eligibility": False, **AUTHORITIES}
    except (KeyError, TypeError, AttributeError):
        _fail()


def require_cost_ready(edition: dict, symbol: str, timeframe: str) -> dict:
    """Select cold-read data with costs; this is not a financial capability."""
    if (edition.get("source_kind") != "m5_derived"
            or edition.get("financial_consumer_ready") is not False
            or edition.get("data_scope") != DATA_SCOPE
            or edition.get("readiness_label") != DATA_SCOPE
            or edition.get("full_native_context_eligibility") is not False
            or any(edition.get(key) is not False for key in AUTHORITIES)):
        _fail("DERIVED_COST_UNAVAILABLE")
    matches = [row for row in edition.get("entries", ())
               if (row.get("symbol"), row.get("timeframe")) == (symbol, timeframe)]
    if (len(matches) != 1 or matches[0].get("cost_status") != COST_READY
            or not matches[0].get("bars")
            or any(row.spread is None for row in matches[0]["bars"])):
        _fail("DERIVED_COST_UNAVAILABLE")
    return {**matches[0], "profile": _detached(matches[0]["profile"])}
