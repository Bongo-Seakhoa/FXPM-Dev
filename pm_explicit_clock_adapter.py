"""Authenticated-owner orchestration for the unadopted finite clock data path.

Public entry points require the same live original NativeInputs owner. Private
protocol helpers support labelled ordinary unit fixtures, not native proofs.
No profile, source window, financial capability or new native seal is created.
"""
from __future__ import annotations

from contextlib import closing
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta
import hashlib
import json
import math
from typing import Any, Iterable

from pm_explicit_derived_history import Interval, SourceIdentity, canonical_json_bytes
from pm_explicit_derived_native import NativeInputs, NativeInputsError
from pm_explicit_observed_clock import (
    ClockStratum, ClockExtent, ObservedClockError, ObservedClockTemplate,
    freeze_observed_clock, derive_on_observed_clock,
    identify_clock_spread_rule, apply_clock_spread_rule,
)


PROTECTED_CUTOFF = "2024-07-14T08:05:00+00:00"
MAX_CLOCK_TARGETS = 25
DATA_SCOPE = "AVAILABLE_DATA_ONLY_NOT_EXECUTION_READY"
R3_RESTRICTION = "COMMON_POSITIVE_POINT_CLASS_COARSE_BROKER_POLICY_V1"
REQUEST_SEMANTICS = "RAW_NATIVE_REQUEST_METADATA_NOT_RECONSTRUCTED_TARGET_GRID"
_FLAGS = {"data_scope": DATA_SCOPE, "readiness_label": DATA_SCOPE,
    "full_native_context_eligibility": False, "financial_consumer_ready": False,
    "qualification_authority": False, "capital_authority": False,
    "live_authority": False, "order_authority": False}


class ClockAdapterError(ValueError):
    def __init__(self, code: str):
        self.code = code
        super().__init__(code)


@dataclass(frozen=True)
class ClockTargetPlan:
    row: dict
    m5: Any = field(repr=False, compare=False)


@dataclass(frozen=True)
class ClockStratumPlan:
    row: dict
    c_native: Any = field(repr=False, compare=False)
    d_native: Any = field(repr=False, compare=False)
    d_m5: Any = field(repr=False, compare=False)
    targets: tuple[ClockTargetPlan, ...] = field(repr=False)


@dataclass(frozen=True)
class FrozenClockPlan:
    payload_json: bytes
    strata: tuple[ClockStratumPlan, ...] = field(repr=False)
    owner: Any = field(repr=False, compare=False)

    def to_dict(self) -> dict:
        return json.loads(self.payload_json)

    @property
    def sha256(self) -> str:
        return hashlib.sha256(self.payload_json).hexdigest()


def _fail(code):
    raise ClockAdapterError(code)


def _detach(value):
    return json.loads(canonical_json_bytes(value))


def _same(left, right):
    return canonical_json_bytes(left) == canonical_json_bytes(right)


def _wire(value):
    return _detach(asdict(value))


def _metadata_pins_only(pins):
    if any(pin.kind == "csv" for pin in pins):
        _fail("CLOCK_METADATA_PLAN_HAS_PRIOR_CSV")


def _utc(value):
    if type(value) is not str or value != value.strip():
        _fail("CLOCK_METADATA_TIMESTAMP_INVALID")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        _fail("CLOCK_METADATA_TIMESTAMP_INVALID")
    if parsed.tzinfo is None or parsed.utcoffset() != timedelta(0) or parsed.microsecond:
        _fail("CLOCK_METADATA_TIMESTAMP_INVALID")
    return parsed


def _bounds(value):
    if type(value) is not dict or set(value) != {"start_utc", "end_exclusive_utc"}:
        _fail("CLOCK_METADATA_INTERVAL_INVALID")
    start, end = _utc(value["start_utc"]), _utc(value["end_exclusive_utc"])
    if start >= end:
        _fail("CLOCK_METADATA_INTERVAL_INVALID")
    return start, end


def _positive(value):
    try:
        return type(value) in (int, float) and math.isfinite(value) and value > 0
    except OverflowError:
        return False


def _digest(value):
    return type(value) is str and len(value) == 64 and all(c in "0123456789abcdef" for c in value)


def _fine_bounds(binding):
    values = (binding["interval"], binding["geometry"]["acquisition_interval"],
              binding["observed_interval"])
    for value in values:
        _bounds(value)
    return values


def _native_bounds(binding):
    values = (binding["prefix_interval"], binding["acquisition_interval"])
    if binding["observed_interval"] is not None:
        values += (binding["observed_interval"],)
    for value in values:
        _bounds(value)
    return values


def _key(timeframe, broker, catalog_class, point, policy, mode):
    if (timeframe not in ("H4", "D1") or not _digest(broker) or not _digest(policy)
            or type(catalog_class) is not str or not catalog_class.strip()
            or not _positive(point) or type(mode) is not str or not mode):
        _fail("CLOCK_R3_METADATA_INVALID")
    return timeframe, broker, catalog_class, canonical_json_bytes(point), policy, mode


def _target_key(envelope, fine):
    identity = fine["source_identity"]
    spec = envelope["target_spec"]
    if (spec["native_status"] != "technical_error" or fine["native_status"] != "evidence_candidate"
            or spec["symbol"] != identity["symbol"] or identity["timeframe"] != "M5"):
        _fail("CLOCK_TECHNICAL_TARGET_ACCEPTED_M5_REQUIRED")
    _bounds(spec["target_source_request_interval"])
    _fine_bounds(fine)
    return _key(spec["timeframe"], envelope["broker_source_sha256"],
        fine["stratum"]["instrument_session_class"], identity["point"],
        envelope["broker_policy_sha256"], envelope["alignment_mode"])


def _control_key(native, fine):
    n, m = native["source_identity"], fine["source_identity"]
    if (native["native_status"] != "evidence_candidate" or fine["native_status"] != "evidence_candidate"
            or n["symbol"] != m["symbol"] or m["timeframe"] != "M5"
            or native["symbol"] != n["symbol"] or fine["symbol"] != m["symbol"]
            or (n["activation_id"], n["activation_sha256"]) != (m["activation_id"], m["activation_sha256"])
            or not _same(n["point"], m["point"])
            or native["instrument_session_class"] != fine["stratum"]["instrument_session_class"]):
        _fail("CLOCK_CONTROL_NATIVE_M5_METADATA_MISMATCH")
    _native_bounds(native)
    _fine_bounds(fine)
    return _key(n["timeframe"], native["broker_source_sha256"],
        native["instrument_session_class"], n["point"], native["broker_policy_sha256"], native["alignment_mode"])


def _intersection(intervals):
    values = [_bounds(value) for value in intervals]
    start, end = max(row[0] for row in values), min(row[1] for row in values)
    end = min(end, _utc(PROTECTED_CUTOFF))
    if start >= end:
        _fail("CLOCK_COMMON_METADATA_INTERVAL_EMPTY")
    return Interval(start.isoformat(timespec="seconds"), end.isoformat(timespec="seconds"))


def _extent(required, fine, available):
    start, end = _bounds(required)
    left, right = _bounds(asdict(available))
    acquired, observed = fine["geometry"]["acquisition_interval"], fine["observed_interval"]
    if left < max(start, _bounds(acquired)[0], _bounds(observed)[0]) or right > min(
            end, _bounds(acquired)[1], _bounds(observed)[1]):
        _fail("CLOCK_EXTENT_OUTSIDE_METADATA")
    omitted = []
    if start < left:
        omitted.append(Interval(required["start_utc"], available.start_utc))
    if right < end:
        omitted.append(Interval(available.end_exclusive_utc, required["end_exclusive_utc"]))
    return ClockExtent(Interval(**required), Interval(**acquired), Interval(**observed), available, tuple(omitted))


def _planned(binding, consumed):
    return {**binding, "bars_consumed": False, "consumption_planned": consumed}


def _stop(symbol, timeframe, error, **known):
    if getattr(error, "abort_attempt", False):
        raise error
    return {"symbol": symbol, "timeframe": timeframe,
        "reason_code": getattr(error, "code", "CLOCK_METADATA_INVALID"),
        "details": {"error_type": type(error).__name__, "error_message": str(error)[:512]},
        **known, **_FLAGS}


def _roster_row(native, fine):
    return {"symbol": native["symbol"], "native": native, "m5": fine,
        "native_source": native["source_identity"],
        "m5_source": fine["source_identity"], "catalog_class": native["instrument_session_class"],
        "point": native["source_identity"]["point"], "broker_source_sha256": native["broker_source_sha256"],
        "coarse_broker_policy": native["broker_policy_sha256"],
        "native_status": native["native_status"], "m5_status": fine["native_status"],
        "native_metadata_sha256": native["metadata_digest"], "m5_metadata_sha256": fine["metadata_digest"]}


def _prepare_protocol(inputs, targets, *, cutoff_utc):
    """Metadata protocol seam; ordinary fixtures here establish no native proof."""
    if _utc(cutoff_utc) != _utc(PROTECTED_CUTOFF):
        _fail("CLOCK_PROTECTED_CUTOFF_MISMATCH")
    cutoff_utc = PROTECTED_CUTOFF
    roster = tuple(targets)
    if len(roster) > MAX_CLOCK_TARGETS:
        _fail("CLOCK_TARGET_CAP_EXCEEDED")
    if (not roster or any(type(row) is not tuple or len(row) != 2
            or any(type(value) is not str or not value.strip() for value in row)
            or row[1] not in ("H4", "D1") for row in roster) or len(set(roster)) != len(roster)):
        _fail("CLOCK_TARGET_ROSTER_INVALID")
    _metadata_pins_only(inputs.pins)
    excluded = sorted({row[0] for row in roster})
    grouped, stops, diagnostics, unavailable, runtime = {}, [], [], [], []
    catalogs = {}
    for timeframe in sorted({row[1] for row in roster}):
        catalog = inputs.clock_catalog(timeframe)
        catalogs[timeframe] = catalog
        diagnostics.append({"timeframe": timeframe, "status_roster": [_wire(row) for row in catalog.status_roster],
            "metadata_refusals": [_wire(row) for row in catalog.metadata_refusals]})
    for symbol, timeframe in sorted(roster):
        envelope = None
        try:
            envelope = _detach(inputs.clock_target_metadata(symbol, timeframe))
            cell = inputs.m5_source(symbol)
            fine = _wire(cell)
            key = _target_key(envelope, fine)
            if envelope["target_spec"]["symbol"] != symbol or key[0] != timeframe:
                _fail("CLOCK_TARGET_METADATA_IDENTITY_MISMATCH")
            grouped.setdefault(key, []).append((envelope, fine, cell))
        except (NativeInputsError, ClockAdapterError, KeyError, TypeError, ValueError) as error:
            stops.append(_stop(symbol, timeframe, error,
                target_spec=None if envelope is None else envelope.get("target_spec")))
    for key, declared in sorted(grouped.items(), key=lambda pair: pair[0]):
        timeframe, broker, catalog_class, point_json, policy, mode = key
        point = json.loads(point_json)
        stratum = ClockStratum(timeframe, broker, catalog_class)
        group_id = hashlib.sha256(canonical_json_bytes({"stratum": asdict(stratum),
            "common_point": point, "coarse_broker_policy": policy, "r3_restriction": R3_RESTRICTION})).hexdigest()
        candidates = []
        for candidate in catalogs[timeframe].candidates:
            native, fine = _wire(candidate.native), _wire(candidate.m5)
            try:
                candidate_key = _control_key(native, fine)
                if candidate_key == key and native["symbol"] not in excluded:
                    candidates.append((native, fine, candidate))
            except (ClockAdapterError, KeyError, TypeError, ValueError) as error:
                diagnostics.append({"timeframe": timeframe, "symbol": native.get("symbol"),
                    "reason_code": getattr(error, "code", "CLOCK_CONTROL_METADATA_INVALID")})
        candidates.sort(key=lambda row: row[0]["symbol"])
        symbols = [row[0]["symbol"] for row in candidates]
        if len(set(symbols)) != len(symbols):
            _fail("CLOCK_CATALOG_DUPLICATE_SYMBOL")
        eligible = [_roster_row(native, fine) for native, fine, _ in candidates]
        try:
            if len(candidates) < 2:
                _fail("CLOCK_DISTINCT_CONTROLS_UNAVAILABLE")
            c, d = candidates[:2]
            activation = c[0]["source_identity"]["activation_id"], c[0]["source_identity"]["activation_sha256"]
            if any((binding["source_identity"]["activation_id"], binding["source_identity"]["activation_sha256"]) != activation
                   for binding in (c[1], d[0], d[1], *(fine for _, fine, _ in declared))):
                _fail("CLOCK_ROLE_ACTIVATION_MISMATCH")
            intervals = []
            for native, fine, _ in (c, d):
                intervals.extend((*_native_bounds(native), *_fine_bounds(fine)))
            for envelope, fine, _ in declared:
                intervals.extend((envelope["target_spec"]["target_source_request_interval"], *_fine_bounds(fine)))
            common = _intersection(intervals)
            control_extent = _extent(asdict(common), d[1], common)
            group_row = {"stratum_id": group_id, "stratum": asdict(stratum), "common_point": point,
                "coarse_broker_policy": policy, "coarse_alignment_mode": mode,
                "metadata_common_interval": asdict(common), "control_extent": asdict(control_extent),
                "eligible_roster": eligible, "target_symbols_excluded": excluded,
                "c_native": _planned(c[0], True), "c_m5": _planned(c[1], False),
                "d_native": _planned(d[0], True), "d_m5": _planned(d[1], True),
                "template": None, "resolution_status": "METADATA_ONLY_C_AXES_NOT_READ", **_FLAGS}
            target_rows = []
            for envelope, fine, cell in declared:
                spec = {**envelope["target_spec"], "point": point,
                    "instrument_session_class": catalog_class, "source_identity_from": "ACCEPTED_SAME_SYMBOL_M5"}
                target_rows.append(ClockTargetPlan({"symbol": spec["symbol"], "timeframe": timeframe,
                    "stratum_id": group_id, "stratum": asdict(stratum), "common_point": point,
                    "coarse_broker_policy": policy, "broker_source_sha256": broker,
                    "m5": _planned(fine, True), "target_spec": spec,
                    "extent": asdict(_extent(spec["target_source_request_interval"], fine, common)),
                    "requested_interval_semantics": REQUEST_SEMANTICS, **_FLAGS}, cell))
            runtime.append(ClockStratumPlan(group_row, c[2].native, d[2].native, d[2].m5, tuple(target_rows)))
        except (ClockAdapterError, KeyError, TypeError, ValueError) as error:
            unavailable.append({"stratum_id": group_id, "stratum": asdict(stratum), "common_point": point,
                "coarse_broker_policy": policy, "eligible_roster": eligible,
                "target_symbols_excluded": excluded, "fixed_c": symbols[0] if symbols else None,
                "fixed_d": symbols[1] if len(symbols) > 1 else None,
                "reason_code": getattr(error, "code", "CLOCK_GROUP_METADATA_INVALID"), **_FLAGS})
            stops.extend(_stop(envelope["target_spec"]["symbol"], timeframe, error,
                target_spec=envelope["target_spec"], stratum_id=group_id) for envelope, _, _ in declared)
    metadata_pins = tuple(inputs.pins)
    _metadata_pins_only(metadata_pins)
    payload = {"schema_version": "fxpm.clock-derived-plan.v1", "source_kind": "m5_derived_control_clock.v1",
        "profile_disposition": "PRICE_ONLY_NO_PROFILE_ADOPTED", "cutoff_utc": cutoff_utc,
        "r3_restriction": R3_RESTRICTION, "requested_interval_semantics": REQUEST_SEMANTICS,
        "requested_targets": [{"symbol": s, "timeframe": tf} for s, tf in sorted(roster)],
        "authority_bindings": _wire(inputs.authority_bindings),
        "metadata_read_pins": [_wire(pin) for pin in metadata_pins],
        "strata": [row.row for row in runtime], "targets": [target.row for row in runtime for target in row.targets],
        "stops": stops, "catalog_diagnostics": diagnostics, "unavailable_strata": unavailable, **_FLAGS}
    return FrozenClockPlan(canonical_json_bytes(payload), tuple(runtime), inputs)


def prepare_frozen_clock_plan(inputs: NativeInputs, targets: Iterable[tuple[str, str]], *,
                               cutoff_utc: str = PROTECTED_CUTOFF) -> FrozenClockPlan:
    """Freeze at most 25 H4/D1 targets before C values; reject any prior CSV pins."""
    if type(inputs) is not NativeInputs:
        _fail("CLOCK_LIVE_NATIVE_INPUTS_REQUIRED")
    return _prepare_protocol(inputs, targets, cutoff_utc=cutoff_utc)


def _check_runtime_plan(plan, inputs):
    if type(plan) is not FrozenClockPlan or plan.owner is not inputs:
        _fail("CLOCK_PLAN_OWNER_MISMATCH")
    payload = plan.to_dict()
    if (not _same(payload["strata"], [row.row for row in plan.strata])
            or not _same(payload["targets"], [target.row for row in plan.strata for target in row.targets])):
        _fail("CLOCK_METADATA_PLAN_MUTATED")
    for group in plan.strata:
        for name, cell in (("c_native", group.c_native), ("d_native", group.d_native), ("d_m5", group.d_m5)):
            frozen = {k: v for k, v in group.row[name].items() if k not in ("bars_consumed", "consumption_planned")}
            if not _same(frozen, _wire(cell)):
                _fail("CLOCK_RUNTIME_SOURCE_BINDING_MISMATCH")
        for target in group.targets:
            frozen = {k: v for k, v in target.row["m5"].items() if k not in ("bars_consumed", "consumption_planned")}
            if not _same(frozen, _wire(target.m5)):
                _fail("CLOCK_RUNTIME_SOURCE_BINDING_MISMATCH")
    return payload


def _whole_clock(rows, interval):
    start, end = _bounds(asdict(interval))
    output = []
    for value in rows:
        opened, closed = _utc(value.open_utc), _utc(value.close_utc)
        if start <= opened and closed <= end:
            output.append(Interval(value.open_utc, value.close_utc))
    if not output:
        _fail("CLOCK_COMMON_WHOLE_BUCKETS_EMPTY")
    return tuple(output)


def _consumed(role, group, cell):
    binding = _wire(cell)
    return {"role": role, "stratum_id": group.row["stratum_id"], "symbol": binding["symbol"],
        "timeframe": binding["timeframe"], "source_identity": binding["source_identity"],
        "bars_path": binding["bars_path"], "expected_rows": binding["expected_rows"],
        "expected_bytes": binding["expected_bytes"], "bars_consumed": True}


def _populate_protocol(publisher, plan, inputs):
    """Ordinary protocol seam; only the public wrapper adds original revalidation."""
    payload = _check_runtime_plan(plan, inputs)
    if publisher.plan_sha256 != plan.sha256:
        _fail("CLOCK_PUBLISHER_PLAN_BINDING_MISMATCH")
    consumed, resolved = [], []
    for stopped in payload["stops"]:
        publisher.stop(stopped["symbol"], stopped["timeframe"], stopped["reason_code"], details=stopped)
    # All C templates are resolved and persisted before any D value stream opens.
    for group in plan.strata:
        try:
            _check_runtime_plan(plan, inputs)
            metadata_interval = Interval(**group.row["metadata_common_interval"])
            consumed.append(_consumed("c_native", group, group.c_native))
            with closing(inputs.iter_clock_bars(group.c_native, group.c_native.prefix_interval)) as rows:
                clock = _whole_clock(rows, metadata_interval)
            available = Interval(clock[0].start_utc, clock[-1].end_exclusive_utc)
            template = freeze_observed_clock(clock, stratum=ClockStratum(**group.row["stratum"]),
                clock_source=group.c_native.source_identity,
                corroboration_source=group.d_native.source_identity, interval=available,
                cutoff_utc=payload["cutoff_utc"])
            control_extent = _extent(group.row["metadata_common_interval"], _wire(group.d_m5), available)
            target_extents = []
            for target in group.targets:
                extent = _extent(target.row["target_spec"]["target_source_request_interval"], _wire(target.m5), available)
                target_extents.append((target, extent))
            context = {"schema_version": "fxpm.clock-runtime-context.v1", "stratum_id": group.row["stratum_id"],
                "template_sha256": template.template_sha256, "available_interval": asdict(available),
                "control_extent": asdict(control_extent), "target_extents": [
                    {"symbol": target.row["symbol"], "timeframe": target.row["timeframe"], "extent": asdict(extent)}
                    for target, extent in target_extents]}
            publisher.freeze_template(group.row["stratum_id"], template, context)
            d_fine = inputs.project_m5_source(group.d_m5, available)
            targets = tuple((target, inputs.project_m5_source(target.m5, available), extent)
                            for target, extent in target_extents)
            resolved.append((group, template, d_fine, control_extent, tuple(targets)))
        except (NativeInputsError, ClockAdapterError, ObservedClockError) as error:
            for target in group.targets:
                row = _stop(target.row["symbol"], target.row["timeframe"], error)
                publisher.stop(row["symbol"], row["timeframe"], row["reason_code"], details=row)
    for group, template, d_fine, control_extent, targets in resolved:
        try:
            _check_runtime_plan(plan, inputs)
            consumed.append(_consumed("d_m5", group, d_fine))
            with closing(inputs.iter_bars(d_fine, template.interval)) as rows:
                derived_d = derive_on_observed_clock(rows, template=template,
                    source=d_fine.source_identity, extent=control_extent)
            consumed.append(_consumed("d_native", group, group.d_native))
            with closing(inputs.iter_clock_bars(group.d_native, template.interval)) as rows:
                native_d = tuple(rows)
            identification = identify_clock_spread_rule(derived_d, native_d,
                native_source=group.d_native.source_identity)
            publisher.control_compared(stratum_id=group.row["stratum_id"], template=template, d_series=derived_d,
                d_native_bars=native_d, identification=identification)
        except (NativeInputsError, ClockAdapterError, ObservedClockError) as error:
            for target, _, _ in targets:
                row = _stop(target.row["symbol"], target.row["timeframe"], error)
                publisher.stop(row["symbol"], row["timeframe"], row["reason_code"], details=row)
            continue
        for target, projected, extent in targets:
            try:
                _check_runtime_plan(plan, inputs)
                consumed.append(_consumed("target_m5", group, projected))
                with closing(inputs.iter_bars(projected, template.interval)) as rows:
                    series = derive_on_observed_clock(rows, template=template,
                        source=projected.source_identity, extent=extent)
                if not series.bars:
                    row = _stop(target.row["symbol"], target.row["timeframe"],
                        ClockAdapterError("CLOCK_TARGET_OBSERVED_PREFIX_EMPTY"))
                    publisher.stop(row["symbol"], row["timeframe"], row["reason_code"], details=row)
                    continue
                series = apply_clock_spread_rule(series, identification)
                publisher.add_series(series, identification, statistics=None)
            except (NativeInputsError, ClockAdapterError, ObservedClockError) as error:
                row = _stop(target.row["symbol"], target.row["timeframe"], error)
                publisher.stop(row["symbol"], row["timeframe"], row["reason_code"], details=row)
    _check_runtime_plan(plan, inputs)
    return tuple(consumed)


def _checked_eof_roles(consumed, pins):
    """Join plain records; only the public caller supplies original revalidation."""
    csv = {pin.path: pin for pin in pins if pin.kind == "csv"}
    for role in consumed:
        pin = csv.get(role["bars_path"])
        if (pin is None or pin.sha256 != role["source_identity"]["bars_sha256"]
                or pin.bytes != role["expected_bytes"] or pin.rows != role["expected_rows"]):
            _fail("CLOCK_CONSUMED_ROLE_EOF_PIN_MISSING")


def populate_frozen_clock_plan(publisher, plan: FrozenClockPlan, inputs: NativeInputs) -> None:
    """Consume registered sources, then bind original EOF/revalidation evidence."""
    if type(inputs) is not NativeInputs:
        _fail("CLOCK_LIVE_NATIVE_INPUTS_REQUIRED")
    from pm_explicit_clock_publication import ClockEditionPublisher
    if type(publisher) is not ClockEditionPublisher:
        _fail("CLOCK_ORIGINAL_EDITION_PUBLISHER_REQUIRED")
    consumed = _populate_protocol(publisher, plan, inputs)
    pins = tuple(inputs.revalidate())
    _checked_eof_roles(consumed, pins)
    publisher.bind_source_context({"schema_version": "fxpm.clock-native-source-context.v1",
        "authority_bindings": _wire(inputs.authority_bindings), "metadata_read_pins": [_wire(pin) for pin in pins],
        "consumed_roles": _detach(consumed),
        "authentication_semantics": "ORIGINAL_NATIVEINPUTS_EOF_AND_REVALIDATE_CALLER_REPORT_NOT_NEW_AUTHORITY",
        **_FLAGS})
