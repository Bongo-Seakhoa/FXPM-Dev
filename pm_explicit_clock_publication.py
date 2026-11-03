"""Price-only observed-clock editions over the original general spool.

Input authentication belongs to the original NativeInputs owner. The persisted
source-context report records that caller's EOF/revalidation assertion; it is
not another native authority or a financial capability.
"""
from __future__ import annotations

from dataclasses import asdict, fields
from datetime import datetime
import csv
import hashlib
import io
import json
import math
from pathlib import Path

from pm_authority_spool import (
    GenerationSpoolContext, GENERAL_MANIFEST_DOMAIN, CANONICAL_JSON_FORMAT, open_symbol_sink,
    reopen_generation_context, verify_symbol_receipt, _stream_file,
    _strict_json_bytes,
)
from pm_explicit_derived_history import Bar, Interval, SourceIdentity, canonical_json_bytes
from pm_explicit_observed_clock import (
    ClockStratum, ClockExtent, ObservedClockTemplate, ClockDerivedBar,
    ClockDerivedSeries, ClockSpreadIdentification, PROTECTED_CUTOFF, DATA_SCOPE,
    SOURCE_KIND, CLOCK_POLICY, freeze_observed_clock, identify_clock_spread_rule,
    apply_clock_spread_rule, clock_lineage,
)


EDITION_SYMBOL = "EXPLICIT_OBSERVED_CLOCK_EDITION"
PLAN_SCHEMA = "fxpm.clock-derived-plan.v1"
RUNTIME_SCHEMA = "fxpm.clock-runtime-context.v1"
ACTIVATION_SCHEMA = "fxpm.observed-clock-activation.v1"
SOURCE_CONTEXT_SCHEMA = "fxpm.clock-native-source-context.v1"
CONTROL_SCHEMA = "fxpm.clock-control-comparison.v1"
PROFILE_DISPOSITION = "PRICE_ONLY_NO_PROFILE_ADOPTED"
REQUEST_SEMANTICS = "RAW_NATIVE_REQUEST_METADATA_NOT_RECONSTRUCTED_TARGET_GRID"
R3_RESTRICTION = "COMMON_POSITIVE_POINT_CLASS_COARSE_BROKER_POLICY_V1"
AUTHENTICATION = "ORIGINAL_NATIVEINPUTS_EOF_AND_REVALIDATE_CALLER_REPORT_NOT_NEW_AUTHORITY"
AUTHORITIES = {name: False for name in (
    "qualification_authority", "capital_authority", "live_authority", "order_authority")}
SCOPE = {"source_kind": SOURCE_KIND, "data_scope": DATA_SCOPE,
         "readiness_label": DATA_SCOPE, "full_native_context_eligibility": False,
         "financial_consumer_ready": False, **AUTHORITIES}
EXTENTS = ("required_interval", "m5_acquisition_interval", "m5_observed_interval",
           "available_interval", "omitted_intervals")
CSV_FIELDS = ("time", "_BAR_CLOSE_TS", "Open", "High", "Low", "Close",
              "Volume", "RealVolume", "Spread")


class ClockPublicationError(ValueError):
    def __init__(self, code: str = "CLOCK_EDITION_BINDING_MISMATCH") -> None:
        self.code = code
        super().__init__(code)


def _require(condition, code="CLOCK_EDITION_BINDING_MISMATCH"):
    if not condition:
        raise ClockPublicationError(code)


def _sha(raw):
    return hashlib.sha256(raw).hexdigest()


def _digest(value):
    return _sha(canonical_json_bytes(value))


def _copy(value):
    return json.loads(canonical_json_bytes(value))


def _same(left, right):
    return canonical_json_bytes(left) == canonical_json_bytes(right)


def _hash(value):
    _require(type(value) is str and len(value) == 64
             and all(c in "0123456789abcdef" for c in value))
    return value


def _scope(value):
    _require(type(value) is dict)
    for name, expected in SCOPE.items():
        _require(name in value and type(value[name]) is type(expected)
                 and value[name] == expected)


def _plan_child_scope(value):
    _require(type(value) is dict)
    for name, expected in SCOPE.items():
        if name != "source_kind":
            _require(name in value and type(value[name]) is type(expected)
                     and value[name] == expected)


def _restore(cls, wire_value, **converted):
    _require(type(wire_value) is dict and set(wire_value) == {f.name for f in fields(cls)})
    return cls(**{**wire_value, **converted})


def _interval(value):
    return _restore(Interval, value)


def _bounds(value):
    interval = _interval(value)
    try:
        a, b = (datetime.fromisoformat(v.replace("Z", "+00:00"))
                for v in (interval.start_utc, interval.end_exclusive_utc))
    except (TypeError, ValueError, AttributeError):
        raise ClockPublicationError() from None
    _require(a.tzinfo is not None and b.tzinfo is not None
             and a.utcoffset().total_seconds() == b.utcoffset().total_seconds() == 0 and a < b)
    return a, b


def _source(value):
    source = _restore(SourceIdentity, value)
    _require(type(source.symbol) is str and source.symbol
             and source.timeframe in ("M5", "H4", "D1")
             and type(source.point) in (int, float) and math.isfinite(source.point)
             and source.point > 0 and source.spread_unit == "points"
             and type(source.activation_id) is str and source.activation_id)
    for name in ("activation_sha256", "bars_sha256", "sidecar_sha256",
                 "provenance_sha256", "request_sha256"):
        _hash(getattr(source, name))
    return source


def _extent(value):
    _require(type(value) is dict and set(value) == set(EXTENTS)
             and type(value["omitted_intervals"]) is list)
    return _restore(ClockExtent, value, **{
        name: tuple(_interval(x) for x in value[name]) if name == "omitted_intervals"
        else _interval(value[name]) for name in EXTENTS})


def _resolved_extent(base, available):
    _extent(base)
    lo, hi = _bounds(available)
    for name in EXTENTS[:3]:
        a, b = _bounds(base[name])
        _require(a <= lo < hi <= b)
    required = base["required_interval"]
    omitted = []
    if _bounds(required)[0] < lo:
        omitted.append({"start_utc": required["start_utc"],
                        "end_exclusive_utc": available["start_utc"]})
    if hi < _bounds(required)[1]:
        omitted.append({"start_utc": available["end_exclusive_utc"],
                        "end_exclusive_utc": required["end_exclusive_utc"]})
    return {**{name: base[name] for name in EXTENTS[:3]},
            "available_interval": available, "omitted_intervals": omitted}


def _template(value):
    template = _restore(ObservedClockTemplate, value,
        stratum=_restore(ClockStratum, value["stratum"]),
        clock_source=_source(value["clock_source"]),
        corroboration_source=_source(value["corroboration_source"]),
        interval=_interval(value["interval"]), clock=tuple(_interval(x) for x in value["clock"]))
    expected = freeze_observed_clock(template.clock, stratum=template.stratum,
        clock_source=template.clock_source, corroboration_source=template.corroboration_source,
        interval=template.interval, cutoff_utc=template.cutoff_utc)
    _require(expected == template)
    return template


def _series(value):
    series = _restore(ClockDerivedSeries, value,
        template=_template(value["template"]), source=_source(value["source"]),
        extent=_extent(value["extent"]),
        bars=tuple(_restore(ClockDerivedBar, row,
            value=_restore(Bar, row["value"]), spread_candidates=tuple(row["spread_candidates"]))
            for row in value["bars"]), empty_clock_ordinals=tuple(value["empty_clock_ordinals"]))
    # The original pure validator checks every axis/member/count and self hash.
    if series.spread_identification_sha256 is None:
        clock_lineage(series)
    return series


def _identification(value):
    return _restore(ClockSpreadIdentification, value,
        control_m5_source=_source(value["control_m5_source"]),
        native_control_source=_source(value["native_control_source"]),
        mismatch_counts=tuple(tuple(x) for x in value["mismatch_counts"]),
        matching_candidates=tuple(value["matching_candidates"]))


def _cell(cell):
    _require(type(cell) is dict)
    source = _source(cell["source_identity"])
    _require(cell["symbol"] == source.symbol and cell["timeframe"] == source.timeframe
             and cell["native_status"] == "evidence_candidate"
             and cell["cutoff_utc"] == PROTECTED_CUTOFF)
    _hash(cell["metadata_digest"])
    if source.timeframe != "M5":
        for name in ("broker_source_sha256", "broker_policy_sha256"):
            _hash(cell[name])
    _require(type(_class(cell)) is str and _class(cell))
    return source


def _class(cell):
    return (cell["stratum"]["instrument_session_class"] if cell["source_identity"]["timeframe"] == "M5"
            else cell["instrument_session_class"])


def _acquisition(cell):
    return (cell["geometry"]["acquisition_interval"] if cell["source_identity"]["timeframe"] == "M5"
            else cell["acquisition_interval"])


def _cell_metadata(cell):
    return {name: value for name, value in cell.items()
            if name not in ("bars_consumed", "consumption_planned")}


def _group_id(group):
    return _digest({"stratum": group["stratum"], "common_point": group["common_point"],
                    "coarse_broker_policy": group["coarse_broker_policy"],
                    "r3_restriction": R3_RESTRICTION})


def _checked_plan(plan):
    _scope(plan)
    _require(plan.get("schema_version") == PLAN_SCHEMA
             and plan.get("cutoff_utc") == PROTECTED_CUTOFF
             and plan.get("profile_disposition") == PROFILE_DISPOSITION
             and plan.get("requested_interval_semantics") == REQUEST_SEMANTICS)
    for name in ("strata", "targets", "stops", "requested_targets", "metadata_read_pins"):
        _require(type(plan.get(name)) is list)
    _require(all(type(pin) is dict and pin.get("kind") != "csv"
                 for pin in plan["metadata_read_pins"]), "CLOCK_PLAN_CONTAINS_CSV_PIN")
    for name in ("strata", "targets", "stops", "unavailable_strata"):
        rows = plan.get(name, [])
        _require(type(rows) is list)
        for row in rows:
            _plan_child_scope(row)
    authority = plan.get("authority_bindings")
    _require(type(authority) is dict and type(authority.get("activation_id")) is str)
    for name in ("activation_sha256", "pointer_sha256", "run_manifest_sha256",
                 "semantic_receipt_sha256", "semantic_verifier_build_digest"):
        _hash(authority[name])
    groups = {}
    for group in plan["strata"]:
        sid = group["stratum_id"]
        _require(sid == _group_id(group) and sid not in groups)
        _require(group.get("template") is None
            and group.get("resolution_status") == "METADATA_ONLY_C_AXES_NOT_READ")
        stratum = _restore(ClockStratum, group["stratum"])
        _require(stratum.timeframe in ("H4", "D1") and stratum.policy == CLOCK_POLICY)
        point = group["common_point"]
        _require(type(point) in (int, float) and math.isfinite(point) and point > 0)
        _hash(group["coarse_broker_policy"])
        _bounds(group["metadata_common_interval"])
        _extent(group["control_extent"])
        _require(_same(group["control_extent"], _resolved_extent(
            group["control_extent"], group["metadata_common_interval"]))
            and _same(group["control_extent"]["required_interval"], group["metadata_common_interval"]))
        cells = {role: _cell(group[role]) for role in ("c_native", "c_m5", "d_native", "d_m5")}
        for role, source in cells.items():
            cell = group[role]
            _require(cell.get("bars_consumed") is False
                and cell.get("consumption_planned") is (role != "c_m5"))
            _require(_same(source.point, point) and source.activation_id == authority["activation_id"]
                and source.activation_sha256 == authority["activation_sha256"]
                and _class(cell) == stratum.catalog_class)
            if source.timeframe != "M5":
                _require(cell["broker_source_sha256"] == stratum.broker_source_sha256
                    and cell["broker_policy_sha256"] == group["coarse_broker_policy"]
                    and cell["alignment_mode"] == group["coarse_alignment_mode"])
            _require(source.timeframe == ("M5" if role.endswith("m5") else stratum.timeframe))
        _require(cells["c_native"].symbol == cells["c_m5"].symbol
            and cells["d_native"].symbol == cells["d_m5"].symbol
            and cells["c_native"].symbol < cells["d_native"].symbol)
        _require(_same(group["control_extent"]["m5_acquisition_interval"], _acquisition(group["d_m5"]))
            and _same(group["control_extent"]["m5_observed_interval"], group["d_m5"]["observed_interval"]))
        roster = group["eligible_roster"]
        _require(type(roster) is list and len(roster) >= 2)
        symbols = [row["native"]["symbol"] for row in roster]
        _require(symbols == sorted(set(symbols)))
        excluded = group["target_symbols_excluded"]
        _require(excluded == sorted(set(excluded)) and not set(excluded).intersection(symbols))
        for row in roster:
            native, m5 = _cell(row["native"]), _cell(row["m5"])
            _require(native.symbol == m5.symbol and native.timeframe == stratum.timeframe
                and m5.timeframe == "M5" and _same(native.point, point) and _same(m5.point, point))
            for cell in (row["native"], row["m5"]):
                _require(_class(cell) == stratum.catalog_class
                    and cell["source_identity"]["activation_id"] == authority["activation_id"]
                    and cell["source_identity"]["activation_sha256"] == authority["activation_sha256"])
            _require(row["native"]["broker_source_sha256"] == stratum.broker_source_sha256
                and row["native"]["broker_policy_sha256"] == group["coarse_broker_policy"]
                and row["native"]["alignment_mode"] == group["coarse_alignment_mode"])
            summary = {"symbol": native.symbol, "native_source": asdict(native), "m5_source": asdict(m5),
                "catalog_class": stratum.catalog_class, "point": point,
                "broker_source_sha256": stratum.broker_source_sha256,
                "coarse_broker_policy": group["coarse_broker_policy"],
                "native_status": row["native"]["native_status"], "m5_status": row["m5"]["native_status"],
                "native_metadata_sha256": row["native"]["metadata_digest"],
                "m5_metadata_sha256": row["m5"]["metadata_digest"]}
            _require(all(name in row and _same(row[name], value) for name, value in summary.items()))
        _require(all(_same(_cell_metadata(roster[index][roster_role]),
                          _cell_metadata(group[group_role]))
            for index, roster_role, group_role in (
                (0, "native", "c_native"), (0, "m5", "c_m5"),
                (1, "native", "d_native"), (1, "m5", "d_m5"))))
        groups[sid] = group
    targets = {}
    for row in plan["targets"]:
        key = (row["symbol"], row["timeframe"])
        _require(key not in targets and row["stratum_id"] in groups)
        group = groups[row["stratum_id"]]
        source = _cell(row["m5"])
        _extent(row["extent"])
        _require(_same(row["extent"], _resolved_extent(row["extent"], group["metadata_common_interval"]))
            and _same(row["extent"]["required_interval"], row["target_spec"]["target_source_request_interval"])
            and _same(row["extent"]["m5_acquisition_interval"], _acquisition(row["m5"]))
            and _same(row["extent"]["m5_observed_interval"], row["m5"]["observed_interval"]))
        _require(source.symbol == key[0] and key[1] == group["stratum"]["timeframe"]
            and _same(row["stratum"], group["stratum"])
            and source.timeframe == "M5" and key[0] in group["target_symbols_excluded"]
            and _same(source.point, group["common_point"])
            and source.activation_id == authority["activation_id"]
            and source.activation_sha256 == authority["activation_sha256"]
            and row["m5"].get("bars_consumed") is False
            and row["m5"].get("consumption_planned") is True
            and _same(row["common_point"], group["common_point"])
            and row["coarse_broker_policy"] == group["coarse_broker_policy"]
            and _class(row["m5"]) == group["stratum"]["catalog_class"]
            and row["broker_source_sha256"] == group["stratum"]["broker_source_sha256"]
            and row["requested_interval_semantics"] == REQUEST_SEMANTICS
            and row["target_spec"]["symbol"] == key[0]
            and row["target_spec"]["timeframe"] == key[1]
            and row["target_spec"]["native_status"] == "technical_error"
            and _same(row["target_spec"]["point"], group["common_point"])
            and row["target_spec"]["instrument_session_class"] == group["stratum"]["catalog_class"]
            and row["target_spec"]["source_identity_from"] == "ACCEPTED_SAME_SYMBOL_M5")
        targets[key] = row
    for sid, group in groups.items():
        intervals = []
        for role in ("c_native", "d_native"):
            cell = group[role]
            intervals.extend((cell["prefix_interval"], cell["acquisition_interval"]))
            if cell["observed_interval"] is not None:
                intervals.append(cell["observed_interval"])
        fine = [group["c_m5"], group["d_m5"]]
        for row in targets.values():
            if row["stratum_id"] == sid:
                intervals.append(row["target_spec"]["target_source_request_interval"])
                fine.append(row["m5"])
        for cell in fine:
            intervals.extend((cell["interval"], _acquisition(cell), cell["observed_interval"]))
        bounds = [_bounds(value) for value in intervals]
        lo = max(value[0] for value in bounds)
        hi = min([value[1] for value in bounds] + [datetime.fromisoformat(PROTECTED_CUTOFF)])
        _require(lo < hi and _same(group["metadata_common_interval"], {
            "start_utc": lo.isoformat(timespec="seconds"),
            "end_exclusive_utc": hi.isoformat(timespec="seconds")}),
            "CLOCK_METADATA_COMMON_INTERVAL_MISMATCH")
    stops = {(row["symbol"], row["timeframe"]): row for row in plan["stops"]}
    requested = [(row["symbol"], row["timeframe"]) for row in plan["requested_targets"]]
    # The adopted source-preparation scope covers at most 25 remaining H4/D1 targets.
    _require(len(stops) == len(plan["stops"]) and not set(stops).intersection(targets)
        and 0 < len(requested) <= 25 and len(set(requested)) == len(requested)
        and all(type(symbol) is str and symbol and tf in ("H4", "D1") for symbol, tf in requested)
        and set(requested) == set(targets) | set(stops))
    return groups, targets, stops


def _runtime(plan, groups, targets, sid, template, context):
    _require(type(template) is ObservedClockTemplate and sid in groups)
    _template(_copy(asdict(template)))
    group = groups[sid]
    _require(template.stratum == _restore(ClockStratum, group["stratum"])
        and _same(asdict(template.clock_source), group["c_native"]["source_identity"])
        and _same(asdict(template.corroboration_source), group["d_native"]["source_identity"])
        and template.cutoff_utc == plan["cutoff_utc"])
    a, b = _bounds(group["metadata_common_interval"])
    lo, hi = _bounds(asdict(template.interval))
    _require(a <= lo < hi <= b)
    expected = {"schema_version": RUNTIME_SCHEMA, "stratum_id": sid,
        "template_sha256": template.template_sha256,
        "available_interval": asdict(template.interval),
        "control_extent": _resolved_extent(group["control_extent"], asdict(template.interval)),
        "target_extents": [{"symbol": key[0], "timeframe": key[1],
            "extent": _resolved_extent(row["extent"], asdict(template.interval))}
            for key, row in sorted(targets.items()) if row["stratum_id"] == sid]}
    _require(_same(context, expected), "CLOCK_RUNTIME_CONTEXT_MISMATCH")
    return _copy(expected)


def _context_fields(row, runtime):
    extent = None if runtime is None else next((item["extent"] for item in runtime["target_extents"]
        if (item["symbol"], item["timeframe"]) == (row["symbol"], row["timeframe"])), None)
    metadata_extent = row.get("extent")
    return {**SCOPE, "target_spec": row.get("target_spec"),
        "stratum_id": row.get("stratum_id"),
        "requested_interval_semantics": REQUEST_SEMANTICS,
        "control_extent": runtime["control_extent"] if runtime else None,
        "metadata_available_interval": metadata_extent["available_interval"] if metadata_extent else None,
        "extent_resolution": "C_TEMPLATE_BOUND" if extent else "METADATA_ONLY_NO_C_TEMPLATE",
        **{name: extent[name] if extent else
            metadata_extent[name] if metadata_extent and name in EXTENTS[:3] else None
            for name in EXTENTS}}


def _control(group, template, runtime, d_series, d_native_bars, identification):
    _require(type(d_series) is ClockDerivedSeries and type(identification) is ClockSpreadIdentification
             and type(d_native_bars) is tuple and all(type(x) is Bar for x in d_native_bars))
    _require(d_series.template == template
        and _same(asdict(d_series.source), group["d_m5"]["source_identity"])
        and _same(asdict(d_series.extent), runtime["control_extent"]))
    actual = identify_clock_spread_rule(d_series, d_native_bars,
        native_source=_source(group["d_native"]["source_identity"]))
    _require(actual == identification, "CLOCK_CONTROL_RECOMPUTATION_MISMATCH")
    return {"series": asdict(d_series), "native_bars": [asdict(x) for x in d_native_bars],
            "identification": asdict(identification)}


def _control_record(sid, template, evidence, plan_sha, implementation_sha):
    return {"schema_version": CONTROL_SCHEMA, **SCOPE, "stratum_id": sid,
        "template_sha256": template.template_sha256, "plan_sha256": plan_sha,
        "implementation_sha256": implementation_sha, **evidence}


def _target(row, template, runtime, series, identification, control):
    _require(type(series) is ClockDerivedSeries and type(identification) is ClockSpreadIdentification)
    extent = next(x["extent"] for x in runtime["target_extents"]
                  if (x["symbol"], x["timeframe"]) == (row["symbol"], row["timeframe"]))
    _require(series.template == template and series.source.symbol == row["symbol"]
        and _same(asdict(series.source), row["m5"]["source_identity"])
        and _same(asdict(series.extent), extent)
        and _same(asdict(identification), control["identification"])
        and series.source.symbol not in (template.clock_source.symbol, template.corroboration_source.symbol)
        and series.bars and apply_clock_spread_rule(series, identification) == series,
        "CLOCK_TARGET_JOIN_MISMATCH")
    clock_lineage(series, identification=identification)


def _csv_bytes(series):
    stream = io.StringIO(newline="")
    writer = csv.writer(stream, lineterminator="\n")
    writer.writerow(CSV_FIELDS)
    for item in series.bars:
        bar = item.value
        writer.writerow((bar.open_utc, bar.close_utc, bar.open, bar.high, bar.low,
                         bar.close, bar.volume, "" if bar.real_volume is None else bar.real_volume,
                         "" if bar.spread is None else bar.spread))
    return stream.getvalue().encode("utf-8")


def _members_bytes(series):
    return b"".join(canonical_json_bytes(asdict(x)) + b"\n" for x in series.bars)


def _lineage(series, identification, context_fields, *, plan_sha, implementation_sha,
             output_sha, source_context_sha):
    body = clock_lineage(series, identification=identification)
    body.pop("lineage_sha256")
    body.update(context_fields)
    body.update(plan_sha256=plan_sha, implementation_sha256=implementation_sha,
        output_sha256=output_sha, source_context_sha256=source_context_sha,
        profile_disposition=PROFILE_DISPOSITION, statistics=None,
        publication_cost_status="PRICE_ONLY", target_native_status=context_fields["target_spec"].get("native_status"),
        target_metadata_binding="FROZEN_TECHNICAL_METADATA_NOT_NATIVE_BAR_ELIGIBILITY")
    return {**body, "lineage_sha256": _digest(body)}


def _source_context(plan, groups, targets, runtimes, controls, entries, report):
    _require(type(report) is dict and report.get("schema_version") == SOURCE_CONTEXT_SCHEMA
        and report.get("authentication_semantics") == AUTHENTICATION
        and _same(report.get("authority_bindings"), plan["authority_bindings"]))
    _plan_child_scope(report)
    pins, roles = report.get("metadata_read_pins"), report.get("consumed_roles")
    _require(type(pins) is list and type(roles) is list)
    planned, required = {}, set()
    for sid, group in groups.items():
        for role in ("c_native", "c_m5", "d_native", "d_m5"):
            planned[(sid, role, group[role]["symbol"], group[role]["timeframe"])] = group[role]
        if sid in runtimes:
            required.add((sid, "c_native", group["c_native"]["symbol"], group["c_native"]["timeframe"]))
        if sid in controls:
            for role in ("d_native", "d_m5"):
                required.add((sid, role, group[role]["symbol"], group[role]["timeframe"]))
    for key, row in targets.items():
        identity = (row["stratum_id"], "target_m5", row["symbol"], "M5")
        planned[identity] = row["m5"]
        if entries.get(key, {}).get("status") == "PRICE_DATA_PUBLISHED":
            required.add(identity)
    seen = set()
    for role in roles:
        identity = (role["stratum_id"], role["role"], role["symbol"], role["timeframe"])
        _require(identity in planned and identity not in seen)
        seen.add(identity)
        cell = planned[identity]
        _require(cell.get("consumption_planned") is True and role.get("bars_consumed") is True,
                 "CLOCK_CONSUMPTION_NOT_PLANNED")
        _require(_same(role["source_identity"], cell["source_identity"])
            and role["bars_path"] == cell["bars_path"]
            and role["expected_rows"] == cell["expected_rows"]
            and role["expected_bytes"] == cell["expected_bytes"])
        _require(any(pin.get("kind") == "csv" and pin.get("path") == cell["bars_path"]
            and pin.get("sha256") == cell["source_identity"]["bars_sha256"]
            and pin.get("bytes") == cell["expected_bytes"]
            and pin.get("rows") == cell["expected_rows"] for pin in pins),
            "CLOCK_CONSUMED_SOURCE_PIN_MISSING")
    _require(required <= seen, "CLOCK_CONSUMED_SOURCE_ROLE_MISSING")
    for sid, role, _, _ in seen:
        keys = [key for key, row in targets.items() if row["stratum_id"] == sid]
        all_stopped = bool(keys) and all(entries.get(key, {}).get("status") == "STOP" for key in keys)
        if role == "c_native" and sid not in runtimes:
            _require(all_stopped, "CLOCK_CONSUMED_ROLE_STATE_MISMATCH")
        elif role in ("d_native", "d_m5"):
            _require(sid in runtimes and (sid in controls or all_stopped),
                     "CLOCK_CONSUMED_ROLE_STATE_MISMATCH")
            if role == "d_native":
                fine = groups[sid]["d_m5"]
                _require((sid, "d_m5", fine["symbol"], fine["timeframe"]) in seen,
                         "CLOCK_CONSUMED_ROLE_STATE_MISMATCH")
        elif role == "target_m5":
            _require(sid in controls, "CLOCK_CONSUMED_ROLE_STATE_MISMATCH")
    # Prefrozen metadata pins must remain present; physical CSVs may serve several roles.
    for pin in plan["metadata_read_pins"]:
        _require(any(_same(pin, actual) for actual in pins), "CLOCK_METADATA_PIN_MISSING")
    return _copy(report)


class ClockEditionPublisher:
    def __init__(self, context: GenerationSpoolContext, *, plan: dict,
                 implementation_sha256: str):
        self.context = context
        self.implementation_sha256 = _hash(implementation_sha256)
        self._plan = _copy(plan)
        self.plan_sha256 = _digest(self._plan)
        self._groups, self._targets, self._stops = _checked_plan(self._plan)
        _require(context.config_sha256 == self.plan_sha256)
        self.sink = open_symbol_sink(context, EDITION_SYMBOL, manifest_domain=GENERAL_MANIFEST_DOMAIN)
        self.plan_artifact = self.sink.put_json("plan", "clock_frozen_metadata_plan", self._plan)
        self._runtimes, self._templates, self._controls, self._entries = {}, {}, {}, {}
        self._source_report = None
        self.finished = False

    def _open(self):
        _require(not self.finished, "CLOCK_EDITION_ALREADY_FINISHED")
        _require(self._source_report is None, "CLOCK_SOURCE_CONTEXT_ALREADY_BOUND")

    def freeze_template(self, group_id: str, template: ObservedClockTemplate, runtime_context: dict):
        self._open()
        _require(group_id not in self._templates, "CLOCK_TEMPLATE_ALREADY_RECORDED")
        runtime = _runtime(self._plan, self._groups, self._targets, group_id, template, runtime_context)
        payload = {"schema_version": "fxpm.clock-template-context.v1", **SCOPE,
            "plan_sha256": self.plan_sha256, "implementation_sha256": self.implementation_sha256,
            "template": asdict(template), "runtime_context": runtime}
        artifact = self.sink.put_json("template:" + group_id, "clock_template_context", _copy(payload))
        self._templates[group_id] = template
        self._runtimes[group_id] = runtime
        return _copy(artifact)

    def control_compared(self, *, stratum_id: str, template: ObservedClockTemplate,
                         d_series: ClockDerivedSeries, d_native_bars: tuple[Bar, ...],
                         identification: ClockSpreadIdentification):
        self._open()
        group_id = stratum_id
        _require(group_id in self._templates and group_id not in self._controls
                 and template == self._templates[group_id])
        evidence = _control(self._groups[group_id], template, self._runtimes[group_id],
                            d_series, d_native_bars, identification)
        self.sink.put_json("control:" + group_id, "clock_actual_d_comparison", _copy(
            _control_record(group_id, template, evidence, self.plan_sha256, self.implementation_sha256)))
        self._controls[group_id] = _copy(evidence)

    def diagnostic(self, identity: str, value: dict):
        self._open()
        _require(type(identity) is str and identity)
        self.sink.put_json("diagnostic:" + identity, "clock_diagnostic", _copy(value))

    def add_series(self, series: ClockDerivedSeries, identification: ClockSpreadIdentification,
                   *, statistics=None):
        self._open()
        _require(statistics is None, "CLOCK_PROFILE_NOT_ADOPTED")
        _require(type(series) is ClockDerivedSeries, "CLOCK_DERIVED_SERIES_REQUIRED")
        key = (series.source.symbol, series.template.stratum.timeframe)
        _require(key in self._targets and key not in self._entries, "CLOCK_TARGET_NOT_AVAILABLE")
        row = self._targets[key]
        sid = row["stratum_id"]
        _require(sid in self._controls, "CLOCK_ACTUAL_D_COMPARISON_REQUIRED")
        _target(row, self._templates[sid], self._runtimes[sid], series, identification, self._controls[sid])
        tag = _digest(list(key))[:24]
        bars = self.sink.put_opaque(tag + ":bars", "clock_price_bars", (_csv_bytes(series),), format="csv")
        members = self.sink.put_opaque(tag + ":members", "clock_membership", (_members_bytes(series),), format="jsonl")
        state = self.sink.put_json(tag + ":series", "clock_derived_series", _copy(asdict(series)))
        self._entries[key] = {"symbol": key[0], "timeframe": key[1],
            "status": "PRICE_DATA_PUBLISHED", "cost_status": "PRICE_ONLY", "statistics": None,
            "profile_disposition": PROFILE_DISPOSITION, "bars": bars, "membership": members,
            "series": state, **_context_fields(row, self._runtimes[sid])}

    def stop(self, symbol: str, timeframe: str, reason_code: str, details=None):
        self._open()
        key = (symbol, timeframe)
        _require(key in set(self._targets) | set(self._stops) and key not in self._entries)
        _require(type(reason_code) is str and reason_code)
        row = self._targets.get(key, self._stops.get(key))
        if key in self._stops:
            _require(reason_code == row["reason_code"])
        self._entries[key] = {"symbol": symbol, "timeframe": timeframe, "status": "STOP",
            "reason_code": reason_code, "details": _copy(details), "cost_status": "COST_UNAVAILABLE",
            "statistics": None, "profile_disposition": PROFILE_DISPOSITION,
            **_context_fields(row, self._runtimes.get(row.get("stratum_id")))}

    def bind_source_context(self, report: dict):
        self._open()
        _require(set(self._entries) == set(self._targets) | set(self._stops), "CLOCK_TARGET_RESULTS_INCOMPLETE")
        self._source_report = _source_context(self._plan, self._groups, self._targets, self._runtimes,
                                             self._controls, self._entries, _copy(report))

    def finish(self):
        _require(not self.finished, "CLOCK_EDITION_ALREADY_FINISHED")
        _require(self._source_report is not None, "CLOCK_SOURCE_CONTEXT_REQUIRED")
        source = self.sink.put_json("source-context", "clock_original_owner_source_context", {
            "schema_version": "fxpm.clock-source-context-binding.v1", **SCOPE,
            "plan_sha256": self.plan_sha256, "implementation_sha256": self.implementation_sha256,
            "report": self._source_report})
        entries = []
        for key, stored in sorted(self._entries.items()):
            entry = _copy(stored)
            if entry["status"] == "PRICE_DATA_PUBLISHED":
                sid = entry["stratum_id"]
                series = _series(_read(self.context, entry["series"]["descriptor"], json_value=True))
                identification = _identification(self._controls[sid]["identification"])
                context_fields = _context_fields(self._targets[key], self._runtimes[sid])
                lineage = _lineage(series, identification, context_fields, plan_sha=self.plan_sha256,
                    implementation_sha=self.implementation_sha256,
                    output_sha=entry["bars"]["descriptor"]["sha256"],
                    source_context_sha=source["descriptor"]["sha256"])
                entry["lineage"] = self.sink.put_json(_digest(list(key))[:24] + ":lineage", "clock_data_lineage", _copy(lineage))
            entries.append(entry)
        activation = {"schema_version": ACTIVATION_SCHEMA, **SCOPE,
            "implementation_sha256": self.implementation_sha256, "plan_sha256": self.plan_sha256,
            "plan": self.plan_artifact, "source_context": source, "entries": entries,
            "profile_disposition": PROFILE_DISPOSITION, "statistics": None,
            "template_ids": sorted(self._templates), "control_ids": sorted(self._controls),
            "authentication": AUTHENTICATION}
        self.sink.put_json("activation", "observed_clock_activation", _copy(activation))
        receipt = self.sink.seal()
        self.finished = True
        return receipt


def _read(context, descriptor, *, json_value=False):
    maximum = context.limits.max_json_bytes if json_value else context.limits.max_artifact_bytes
    sha, size, raw = _stream_file(context.root, descriptor["relative_path"], maximum_bytes=maximum,
        expected_size=descriptor["size_bytes"], collect=True, chunk_bytes=context.limits.stream_chunk_bytes)
    _require(sha == descriptor["sha256"] and size == descriptor["size_bytes"])
    return _strict_json_bytes(raw, maximum) if json_value else raw


def reopen_clock_edition(root: Path, receipt: dict, *, expected_generation_id: str,
                         expected_native_universe_sha256: str,
                         expected_implementation_sha256: str, expected_plan_sha256: str):
    """Verify original custody, then recompute strict clock and plan joins."""
    manifest = verify_symbol_receipt(root, receipt, expected_generation_id=expected_generation_id,
        expected_symbol=EDITION_SYMBOL, expected_native_universe_sha256=expected_native_universe_sha256,
        expected_config_sha256=expected_plan_sha256, expected_manifest_domain=GENERAL_MANIFEST_DOMAIN)
    context = reopen_generation_context(root, expected_generation_id=expected_generation_id,
        expected_native_universe_sha256=expected_native_universe_sha256,
        expected_config_sha256=expected_plan_sha256)
    roster = {row["artifact_id"]: row for row in manifest["artifacts"]}
    used = set()

    def read_id(identity, reference=None, *, json_value=True):
        _require(identity in roster)
        row = roster[identity]
        _require(reference is None or _same(reference, row))
        types = {"activation": "observed_clock_activation", "plan": "clock_frozen_metadata_plan",
                 "source-context": "clock_original_owner_source_context"}
        expected_type = types.get(identity)
        if expected_type is None:
            if identity.startswith("template:"):
                expected_type = "clock_template_context"
            elif identity.startswith("control:"):
                expected_type = "clock_actual_d_comparison"
            else:
                expected_type = {"bars": "clock_price_bars", "members": "clock_membership",
                    "series": "clock_derived_series", "lineage": "clock_data_lineage"}.get(identity.rsplit(":", 1)[-1])
        _require(expected_type is not None and row["artifact_type"] == expected_type)
        _require(row["descriptor"]["format"] == (CANONICAL_JSON_FORMAT if json_value
                 else "csv" if identity.endswith(":bars") else "jsonl"))
        used.add(identity)
        return _read(context, row["descriptor"], json_value=json_value)

    activation = read_id("activation")
    _scope(activation)
    _require(activation.get("schema_version") == ACTIVATION_SCHEMA
        and activation.get("implementation_sha256") == _hash(expected_implementation_sha256)
        and activation.get("plan_sha256") == expected_plan_sha256
        and activation.get("profile_disposition") == PROFILE_DISPOSITION
        and activation.get("statistics") is None and activation.get("authentication") == AUTHENTICATION)
    plan = read_id("plan", activation["plan"])
    _require(_digest(plan) == expected_plan_sha256)
    groups, targets, stops = _checked_plan(plan)
    runtimes, templates, controls = {}, {}, {}
    template_ids, control_ids = activation["template_ids"], activation["control_ids"]
    _require(template_ids == sorted(set(template_ids)) and control_ids == sorted(set(control_ids))
             and set(control_ids) <= set(template_ids) <= set(groups))
    for sid in template_ids:
        value = read_id("template:" + sid)
        _scope(value)
        _require(value["schema_version"] == "fxpm.clock-template-context.v1"
            and value["plan_sha256"] == expected_plan_sha256
            and value["implementation_sha256"] == expected_implementation_sha256)
        template = _template(value["template"])
        runtimes[sid] = _runtime(plan, groups, targets, sid, template, value["runtime_context"])
        templates[sid] = template
    for sid in control_ids:
        value = read_id("control:" + sid)
        _scope(value)
        controls[sid] = _copy(_control(groups[sid], templates[sid], runtimes[sid],
            _series(value["series"]), tuple(_restore(Bar, x) for x in value["native_bars"]),
            _identification(value["identification"])))
        _require(_same(_control_record(sid, templates[sid], controls[sid],
            expected_plan_sha256, expected_implementation_sha256), value))
    entries = {}
    results = []
    for entry in activation["entries"]:
        _scope(entry)
        key = (entry["symbol"], entry["timeframe"])
        _require(key not in entries and key in set(targets) | set(stops)
            and entry["cost_status"] == ("COST_UNAVAILABLE" if entry["status"] == "STOP" else "PRICE_ONLY")
            and entry["statistics"] is None
            and entry["profile_disposition"] == PROFILE_DISPOSITION)
        row = targets.get(key, stops.get(key))
        sid = row.get("stratum_id")
        # A STOP may precede a later C template for other work in the stratum.
        unresolved_stop = (entry["status"] == "STOP"
            and entry.get("extent_resolution") == "METADATA_ONLY_NO_C_TEMPLATE")
        context_fields = _context_fields(row, None if unresolved_stop else runtimes.get(sid))
        own_fields = {"symbol", "timeframe", "status", "cost_status", "statistics", "profile_disposition"}
        own_fields |= {"reason_code", "details"} if entry["status"] == "STOP" else {"bars", "membership", "series", "lineage"}
        _require(set(entry) == set(context_fields) | own_fields, "CLOCK_ENTRY_FIELDS_MISMATCH")
        for name, value in context_fields.items():
            _require(name in entry and _same(entry[name], value))
        if entry["status"] == "STOP":
            _require(type(entry.get("reason_code")) is str and entry["reason_code"]
                and not any(name in entry for name in ("bars", "membership", "series", "lineage")))
            if key in stops:
                _require(entry["reason_code"] == stops[key]["reason_code"])
            result = _copy(entry)
        else:
            _require(entry["status"] == "PRICE_DATA_PUBLISHED" and key in targets and sid in controls)
            tag = _digest(list(key))[:24]
            series = _series(read_id(tag + ":series", entry["series"]))
            identification = _identification(controls[sid]["identification"])
            _target(row, templates[sid], runtimes[sid], series, identification, controls[sid])
            _require(read_id(tag + ":bars", entry["bars"], json_value=False) == _csv_bytes(series)
                and read_id(tag + ":members", entry["membership"], json_value=False) == _members_bytes(series))
            lineage = read_id(tag + ":lineage", entry["lineage"])
            expected_lineage = _lineage(series, identification, context_fields,
                plan_sha=expected_plan_sha256, implementation_sha=expected_implementation_sha256,
                output_sha=entry["bars"]["descriptor"]["sha256"],
                source_context_sha=activation["source_context"]["descriptor"]["sha256"])
            _require(_same(lineage, expected_lineage))
            result = {**_copy(entry), "bars": [_copy(asdict(x.value)) for x in series.bars]}
        entries[key] = entry
        results.append(result)
    _require(list(entries) == sorted(entries) and set(entries) == set(targets) | set(stops))
    source = read_id("source-context", activation["source_context"])
    _scope(source)
    _require(source["schema_version"] == "fxpm.clock-source-context-binding.v1"
        and source["plan_sha256"] == expected_plan_sha256
        and source["implementation_sha256"] == expected_implementation_sha256)
    _source_context(plan, groups, targets, runtimes, controls, entries, source["report"])
    _require(all(identity in used or (identity.startswith("diagnostic:")
        and roster[identity]["artifact_type"] == "clock_diagnostic"
        and roster[identity]["descriptor"]["format"] == CANONICAL_JSON_FORMAT) for identity in roster),
             "CLOCK_ARTIFACT_ROSTER_MISMATCH")
    return _copy({"schema_version": "fxpm.observed-clock-edition-read.v1", **SCOPE,
        "entries": results, "profile_disposition": PROFILE_DISPOSITION, "statistics": None,
        "plan_sha256": expected_plan_sha256, "implementation_sha256": expected_implementation_sha256,
        "source_context": source["report"], "authentication": AUTHENTICATION})
