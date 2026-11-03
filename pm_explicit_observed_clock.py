"""Unadopted finite C-clock/D-corroboration data constructor; no I/O or trust.

Callers must supply authenticated native C/D clocks and accepted M5 values.
This module validates their finite transfer, not native receipts or calendars.
Its plain types cannot confer native, financial, profile or trading authority.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass, replace
from datetime import datetime, timedelta
import hashlib
import json
import math
from typing import Iterable

from pm_explicit_derived_history import Bar, Interval, SourceIdentity


PROTECTED_CUTOFF = "2024-07-14T08:05:00+00:00"
DATA_SCOPE = "AVAILABLE_DATA_ONLY_NOT_EXECUTION_READY"
SOURCE_KIND = "m5_derived_control_clock.v1"
CLOCK_POLICY = "FINITE_OBSERVED_C_DISTINCT_D_V1_UNADOPTED"
SPREAD_CANDIDATES = ("first", "last", "min", "max")
_PRICE_ONLY = "CLOCK_PRICE_DATA_ONLY_PROFILE_UNVERIFIED"
_SPREAD_READY = "CLOCK_SPREAD_AVAILABLE_PROFILE_UNVERIFIED"
_AUTHORITIES = ("qualification_authority", "capital_authority", "live_authority", "order_authority")


class ObservedClockError(ValueError):
    def __init__(self, code: str):
        self.code = code
        super().__init__(code)


@dataclass(frozen=True, slots=True)
class ClockStratum:
    timeframe: str
    broker_source_sha256: str
    catalog_class: str
    policy: str = CLOCK_POLICY


@dataclass(frozen=True, slots=True)
class ClockExtent:
    required_interval: Interval
    m5_acquisition_interval: Interval
    m5_observed_interval: Interval
    available_interval: Interval
    omitted_intervals: tuple[Interval, ...]


@dataclass(frozen=True, slots=True)
class ObservedClockTemplate:
    stratum: ClockStratum
    clock_source: SourceIdentity
    corroboration_source: SourceIdentity
    interval: Interval
    clock: tuple[Interval, ...]
    cutoff_utc: str
    axes_sha256: str
    template_sha256: str
    source_kind: str = SOURCE_KIND
    data_scope: str = DATA_SCOPE
    readiness_label: str = DATA_SCOPE
    full_native_context_eligibility: bool = False
    financial_consumer_ready: bool = False
    qualification_authority: bool = False
    capital_authority: bool = False
    live_authority: bool = False
    order_authority: bool = False


@dataclass(frozen=True, slots=True)
class ClockDerivedBar:
    value: Bar
    clock_ordinal: int
    member_count: int
    membership_sha256: str
    spread_candidates: tuple[int | None, int | None, int | None, int | None]


@dataclass(frozen=True, slots=True)
class ClockDerivedSeries:
    template: ObservedClockTemplate
    source: SourceIdentity
    extent: ClockExtent
    bars: tuple[ClockDerivedBar, ...]
    empty_clock_ordinals: tuple[int, ...]
    source_member_count: int
    observed_gap_count: int
    membership_sha256: str
    series_sha256: str
    cost_status: str = _PRICE_ONLY
    selected_spread_rule: str | None = None
    spread_identification_sha256: str | None = None
    source_kind: str = SOURCE_KIND
    data_scope: str = DATA_SCOPE
    readiness_label: str = DATA_SCOPE
    full_native_context_eligibility: bool = False
    financial_consumer_ready: bool = False
    qualification_authority: bool = False
    capital_authority: bool = False
    live_authority: bool = False
    order_authority: bool = False


@dataclass(frozen=True, slots=True)
class ClockSpreadIdentification:
    template_sha256: str
    control_m5_source: SourceIdentity
    native_control_source: SourceIdentity
    derived_control_sha256: str
    native_control_sha256: str
    compared_bar_count: int
    mismatch_counts: tuple[tuple[str, int], ...]
    matching_candidates: tuple[str, ...]
    selected_spread_rule: str | None
    cost_status: str
    reason_code: str
    evidence_sha256: str
    source_kind: str = SOURCE_KIND
    data_scope: str = DATA_SCOPE
    readiness_label: str = DATA_SCOPE
    full_native_context_eligibility: bool = False
    financial_consumer_ready: bool = False
    qualification_authority: bool = False
    capital_authority: bool = False
    live_authority: bool = False
    order_authority: bool = False


def _fail(code):
    raise ObservedClockError(code)


def _json(value):
    return json.dumps(value, allow_nan=False, ensure_ascii=True, sort_keys=True,
                      separators=(",", ":")).encode("ascii")


def _sha(value):
    return hashlib.sha256(_json(value)).hexdigest()


def _body(value, excluded):
    return {name: item for name, item in asdict(value).items() if name != excluded}


def _digest(value):
    return type(value) is str and len(value) == 64 and all(c in "0123456789abcdef" for c in value)


def _utc(value):
    if type(value) is not str or value != value.strip():
        _fail("UTC_TIMESTAMP_INVALID")
    try:
        result = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        _fail("UTC_TIMESTAMP_INVALID")
    if result.tzinfo is None or result.utcoffset() != timedelta(0) or result.microsecond:
        _fail("UTC_TIMESTAMP_INVALID")
    return result


def _interval(value):
    if type(value) is not Interval:
        _fail("CLOCK_INTERVAL_INVALID")
    start, end = _utc(value.start_utc), _utc(value.end_exclusive_utc)
    if start >= end:
        _fail("CLOCK_INTERVAL_INVALID")
    return start, end


def _finite(value):
    try:
        return type(value) in (int, float) and math.isfinite(value)
    except OverflowError:
        return False


def _integral(value):
    return _finite(value) and value >= 0 and int(value) == value


def _source(value):
    if (type(value) is not SourceIdentity or type(value.symbol) is not str or not value.symbol.strip()
            or type(value.activation_id) is not str or not value.activation_id.strip()
            or value.spread_unit != "points" or not _finite(value.point) or value.point <= 0
            or any(not _digest(getattr(value, field)) for field in
                   ("activation_sha256", "bars_sha256", "sidecar_sha256", "provenance_sha256", "request_sha256"))):
        _fail("SOURCE_IDENTITY_INVALID")


def _scope(value):
    if (value.source_kind != SOURCE_KIND or value.data_scope != DATA_SCOPE
            or value.readiness_label != DATA_SCOPE
            or value.full_native_context_eligibility is not False
            or value.financial_consumer_ready is not False
            or any(getattr(value, name) is not False for name in _AUTHORITIES)):
        _fail("DATA_ONLY_SCOPE_INVALID")


def _bar(value, *, fine=False):
    if type(value) is not Bar:
        _fail("OBSERVED_BAR_INVALID")
    opened, closed = _utc(value.open_utc), _utc(value.close_utc)
    if closed <= opened or (fine and closed - opened != timedelta(minutes=5)):
        _fail("OBSERVED_BAR_INTERVAL_INVALID")
    prices = (value.open, value.high, value.low, value.close)
    if (any(not _finite(price) or price <= 0 for price in prices)
            or value.high < max(prices) or value.low > min(prices)
            or not _integral(value.volume)
            or (value.real_volume is not None and not _integral(value.real_volume))):
        _fail("OBSERVED_BAR_INVALID")
    if value.spread is not None and not _integral(value.spread):
        _fail("OBSERVED_SPREAD_INVALID")
    return replace(value, volume=int(value.volume),
        real_volume=None if value.real_volume is None else int(value.real_volume),
        spread=None if value.spread is None else int(value.spread)), opened, closed


def _extent(value, template):
    if type(value) is not ClockExtent or type(value.omitted_intervals) is not tuple:
        _fail("CLOCK_EXTENT_INVALID")
    required, acquired, observed, available = map(_interval,
        (value.required_interval, value.m5_acquisition_interval,
         value.m5_observed_interval, value.available_interval))
    if (available != _interval(template.interval)
            or available[0] < max(required[0], acquired[0], observed[0])
            or available[1] > min(required[1], acquired[1], observed[1])):
        _fail("CLOCK_EXTENT_INVALID")
    expected = []
    if required[0] < available[0]:
        expected.append((required[0], available[0]))
    if available[1] < required[1]:
        expected.append((available[1], required[1]))
    if [_interval(row) for row in value.omitted_intervals] != expected:
        _fail("CLOCK_EXTENT_INVALID")


def _clock_fields(value):
    _scope(value)
    s, c, d = value.stratum, value.clock_source, value.corroboration_source
    if (type(s) is not ClockStratum or s.timeframe not in ("H4", "D1")
            or s.policy != CLOCK_POLICY or not _digest(s.broker_source_sha256)
            or type(s.catalog_class) is not str or not s.catalog_class.strip()):
        _fail("CLOCK_STRATUM_INVALID")
    _source(c)
    _source(d)
    if (c.timeframe != s.timeframe or d.timeframe != s.timeframe or c.symbol >= d.symbol
            or (c.activation_id, c.activation_sha256) != (d.activation_id, d.activation_sha256)):
        _fail("DISTINCT_PREFROZEN_C_D_REQUIRED")
    start, end = _interval(value.interval)
    if (_utc(value.cutoff_utc) != _utc(PROTECTED_CUTOFF) or end > _utc(value.cutoff_utc)
            or type(value.clock) is not tuple or not value.clock):
        _fail("CLOCK_PREFIX_INVALID")
    previous = None
    for interval in value.clock:
        opened, closed = _interval(interval)
        if opened < start or closed > end or (previous is not None and opened < previous):
            _fail("CLOCK_INTERVAL_ORDER_OR_COVERAGE_INVALID")
        previous = closed
    if _interval(value.clock[0])[0] != start or _interval(value.clock[-1])[1] != end:
        _fail("CLOCK_INTERVAL_ORDER_OR_COVERAGE_INVALID")


def _checked_template(value):
    if type(value) is not ObservedClockTemplate:
        _fail("OBSERVED_CLOCK_TEMPLATE_REQUIRED")
    _clock_fields(value)
    if (value.axes_sha256 != _sha([asdict(row) for row in value.clock])
            or value.template_sha256 != _sha(_body(value, "template_sha256"))):
        _fail("CLOCK_TEMPLATE_BINDING_MISMATCH")


def freeze_observed_clock(clock: tuple[Interval, ...], *, stratum: ClockStratum,
                          clock_source: SourceIdentity, corroboration_source: SourceIdentity,
                          interval: Interval, cutoff_utc: str = PROTECTED_CUTOFF) -> ObservedClockTemplate:
    """Freeze caller-declared C/D and actual C axes before any price comparisons."""
    value = ObservedClockTemplate(stratum, clock_source, corroboration_source,
        interval, clock, cutoff_utc, "", "")
    _clock_fields(value)
    value = replace(value, axes_sha256=_sha([asdict(row) for row in clock]))
    return replace(value, template_sha256=_sha(_body(value, "template_sha256")))


def _membership(source, template, extent, bars, empty):
    return _sha({"source": asdict(source), "template_sha256": template.template_sha256,
        "extent": asdict(extent), "empty_clock_ordinals": empty,
        "buckets": [{"clock_ordinal": row.clock_ordinal, "member_count": row.member_count,
                     "membership_sha256": row.membership_sha256} for row in bars]})


def derive_on_observed_clock(rows: Iterable[Bar], *, template: ObservedClockTemplate,
                             source: SourceIdentity, extent: ClockExtent) -> ClockDerivedSeries:
    """Aggregate whole ordered M5 members into supplied C buckets; never fill gaps."""
    _checked_template(template)
    _source(source)
    _extent(extent, template)
    if (source.timeframe != "M5" or (source.activation_id, source.activation_sha256) !=
            (template.clock_source.activation_id, template.clock_source.activation_sha256)):
        _fail("M5_SOURCE_BINDING_INVALID")
    bounds = tuple(_interval(row) for row in template.clock)
    start, end = _interval(template.interval)
    groups = [[] for _ in bounds]
    previous_open = previous_close = None
    real_present = None
    count = gaps = index = 0
    for ordinal, raw in enumerate(rows):
        value, opened, closed = _bar(raw, fine=True)
        if previous_open is not None and opened <= previous_open:
            _fail("M5_DUPLICATE_OR_ORDER_INVALID")
        if previous_close is not None and opened < previous_close:
            _fail("M5_OVERLAP_INVALID")
        if opened < start or closed > end or closed > _utc(template.cutoff_utc):
            _fail("M5_OUTSIDE_FROZEN_INTERVAL")
        if previous_close is not None and opened > previous_close:
            gaps += 1
        present = value.real_volume is not None
        if real_present is None:
            real_present = present
        elif real_present != present:
            _fail("M5_REAL_VOLUME_SCHEMA_MIXED")
        while index < len(bounds) and bounds[index][1] <= opened:
            index += 1
        if index == len(bounds) or opened < bounds[index][0]:
            _fail("M5_UNASSIGNED_INTERVAL")
        if closed > bounds[index][1]:
            _fail("M5_STRADDLING_CLOCK_BOUNDARY")
        groups[index].append((ordinal, value))
        previous_open, previous_close = opened, closed
        count += 1
    output = []
    empty = []
    for ordinal, members in enumerate(groups):
        if not members:
            empty.append(ordinal)
            continue
        values = [row for _, row in members]
        spreads = [row.spread for row in values]
        candidates = ((spreads[0], spreads[-1], min(spreads), max(spreads))
                      if all(row is not None for row in spreads) else (None, None, None, None))
        bucket = template.clock[ordinal]
        value = Bar(bucket.start_utc, bucket.end_exclusive_utc, values[0].open,
            max(row.high for row in values), min(row.low for row in values), values[-1].close,
            sum(row.volume for row in values),
            sum(row.real_volume for row in values) if real_present else None, None)
        _bar(value)
        digest = _sha({"source_bars_sha256": source.bars_sha256,
            "members": [{"ordinal": position, "value": asdict(row)} for position, row in members]})
        output.append(ClockDerivedBar(value, ordinal, len(members), digest, candidates))
    bars, empty = tuple(output), tuple(empty)
    value = ClockDerivedSeries(template, source, extent, bars, empty, count, gaps,
        _membership(source, template, extent, bars, empty), "")
    return replace(value, series_sha256=_sha(_body(value, "series_sha256")))


def _checked_series(value):
    if type(value) is not ClockDerivedSeries:
        _fail("CLOCK_DERIVED_SERIES_REQUIRED")
    _scope(value)
    _checked_template(value.template)
    _source(value.source)
    _extent(value.extent, value.template)
    if (value.source.timeframe != "M5" or type(value.bars) is not tuple
            or (value.source.activation_id, value.source.activation_sha256) !=
                (value.template.clock_source.activation_id, value.template.clock_source.activation_sha256)
            or value.selected_spread_rule not in (*SPREAD_CANDIDATES, None)
            or type(value.empty_clock_ordinals) is not tuple
            or type(value.source_member_count) is not int or value.source_member_count < 0
            or type(value.observed_gap_count) is not int or value.observed_gap_count < 0):
        _fail("CLOCK_SERIES_INVALID")
    previous, total, occupied = -1, 0, []
    for row in value.bars:
        if (type(row) is not ClockDerivedBar or type(row.clock_ordinal) is not int
                or not previous < row.clock_ordinal < len(value.template.clock)
                or type(row.member_count) is not int or row.member_count <= 0
                or not _digest(row.membership_sha256)
                or type(row.spread_candidates) is not tuple or len(row.spread_candidates) != 4
                or not (all(v is None for v in row.spread_candidates)
                        or all(_integral(v) for v in row.spread_candidates))):
            _fail("CLOCK_SERIES_INVALID")
        _, opened, closed = _bar(row.value)
        if (opened, closed) != _interval(value.template.clock[row.clock_ordinal]):
            _fail("CLOCK_SERIES_AXIS_MISMATCH")
        expected = None if value.selected_spread_rule is None else row.spread_candidates[
            SPREAD_CANDIDATES.index(value.selected_spread_rule)]
        if row.value.spread != expected:
            _fail("CLOCK_SERIES_SPREAD_MISMATCH")
        previous = row.clock_ordinal
        occupied.append(previous)
        total += row.member_count
    if (value.selected_spread_rule not in (*SPREAD_CANDIDATES, None)
            or value.empty_clock_ordinals != tuple(i for i in range(len(value.template.clock)) if i not in occupied)
            or total != value.source_member_count
            or value.membership_sha256 != _membership(value.source, value.template, value.extent,
                                                       value.bars, value.empty_clock_ordinals)
            or value.series_sha256 != _sha(_body(value, "series_sha256"))):
        _fail("CLOCK_SERIES_BINDING_MISMATCH")
    expected_cost = (_PRICE_ONLY if value.spread_identification_sha256 is None else
        _SPREAD_READY if value.selected_spread_rule is not None and value.bars
            and all(row.value.spread is not None for row in value.bars) else "COST_UNAVAILABLE")
    if (value.cost_status != expected_cost
            or (value.spread_identification_sha256 is not None
                and not _digest(value.spread_identification_sha256))
            or (value.selected_spread_rule is not None and value.spread_identification_sha256 is None)):
        _fail("CLOCK_SERIES_COST_BINDING_MISMATCH")


def _checked_identification(value):
    if type(value) is not ClockSpreadIdentification:
        _fail("CLOCK_SPREAD_IDENTIFICATION_REQUIRED")
    _scope(value)
    _source(value.control_m5_source)
    _source(value.native_control_source)
    if (type(value.mismatch_counts) is not tuple or len(value.mismatch_counts) != 4
            or any(type(pair) is not tuple or len(pair) != 2 for pair in value.mismatch_counts)):
        _fail("CLOCK_SPREAD_BINDING_MISMATCH")
    matches = tuple(name for name, count in value.mismatch_counts if count == 0)
    selected = matches[0] if len(matches) == 1 else None
    if (type(value.compared_bar_count) is not int or value.compared_bar_count <= 0
            or tuple(name for name, _ in value.mismatch_counts) != SPREAD_CANDIDATES
            or any(type(count) is not int or not 0 <= count <= value.compared_bar_count
                   for _, count in value.mismatch_counts)
            or value.matching_candidates != matches or value.selected_spread_rule != selected
            or value.control_m5_source.timeframe != "M5"
            or value.native_control_source.timeframe not in ("H4", "D1")
            or value.control_m5_source.symbol != value.native_control_source.symbol
            or value.control_m5_source.point != value.native_control_source.point
            or value.cost_status != ("SOURCE_RULE_MATCHED" if selected else "COST_UNAVAILABLE")
            or value.reason_code not in (("UNIQUE_CONTROL_MATCH",) if selected else
                ("AMBIGUOUS_CONTROL_MATCH",) if len(matches) > 1 else
                ("MISSING_CONTROL_SPREAD", "NO_CONTROL_MATCH"))
            or any(not _digest(getattr(value, name)) for name in
                   ("template_sha256", "derived_control_sha256", "native_control_sha256"))
            or value.evidence_sha256 != _sha(_body(value, "evidence_sha256"))):
        _fail("CLOCK_SPREAD_BINDING_MISMATCH")


def identify_clock_spread_rule(derived_d: ClockDerivedSeries, native_d: Iterable[Bar],
                              *, native_source: SourceIdentity) -> ClockSpreadIdentification:
    """Require distinct D clock/price/volume transfer before four spread selectors."""
    _checked_series(derived_d)
    template = derived_d.template
    _source(native_source)
    if (native_source != template.corroboration_source
            or derived_d.source.symbol != native_source.symbol
            or derived_d.source.point != native_source.point
            or derived_d.source.symbol == template.clock_source.symbol):
        _fail("DISTINCT_D_CONTROL_BINDING_INVALID")
    native = tuple(_bar(row)[0] for row in native_d)
    if tuple((_utc(row.open_utc), _utc(row.close_utc)) for row in native) != tuple(
            _interval(row) for row in template.clock):
        _fail("CONTROL_CLOCK_FIDELITY_MISMATCH")
    if len(native) != len(derived_d.bars) or derived_d.empty_clock_ordinals:
        _fail("CONTROL_COVERAGE_MISMATCH")
    for row, expected in zip(derived_d.bars, native, strict=True):
        if _json({field: getattr(row.value, field) for field in
                  ("open", "high", "low", "close", "volume", "real_volume")}) != _json({
                  field: getattr(expected, field) for field in
                  ("open", "high", "low", "close", "volume", "real_volume")}):
            _fail("CONTROL_PRICE_FIDELITY_MISMATCH")
    mismatches = [0, 0, 0, 0]
    missing = False
    for row, expected in zip(derived_d.bars, native, strict=True):
        for index, observed in enumerate(row.spread_candidates):
            if observed is None or expected.spread is None:
                missing = True
                mismatches[index] += 1
            elif observed != expected.spread:
                mismatches[index] += 1
    matches = tuple(name for name, count in zip(SPREAD_CANDIDATES, mismatches, strict=True) if count == 0)
    if missing:
        matches = ()
    selected = matches[0] if len(matches) == 1 else None
    reason = ("MISSING_CONTROL_SPREAD" if missing else "UNIQUE_CONTROL_MATCH" if selected
              else "AMBIGUOUS_CONTROL_MATCH" if len(matches) > 1 else "NO_CONTROL_MATCH")
    value = ClockSpreadIdentification(template.template_sha256, derived_d.source, native_source,
        derived_d.series_sha256, _sha([asdict(row) for row in native]), len(native),
        tuple(zip(SPREAD_CANDIDATES, mismatches, strict=True)), matches, selected,
        "SOURCE_RULE_MATCHED" if selected else "COST_UNAVAILABLE", reason, "")
    return replace(value, evidence_sha256=_sha(_body(value, "evidence_sha256")))


def apply_clock_spread_rule(target: ClockDerivedSeries,
                            identification: ClockSpreadIdentification) -> ClockDerivedSeries:
    _checked_series(target)
    _checked_identification(identification)
    if (target.template.template_sha256 != identification.template_sha256
            or target.template.corroboration_source != identification.native_control_source):
        _fail("CLOCK_CONTROL_TRANSFER_BINDING_MISMATCH")
    rule = identification.selected_spread_rule
    index = SPREAD_CANDIDATES.index(rule) if rule is not None else None
    bars = tuple(replace(row, value=replace(row.value,
        spread=None if index is None else row.spread_candidates[index])) for row in target.bars)
    ready = bool(bars) and rule is not None and all(row.value.spread is not None for row in bars)
    value = replace(target, bars=bars, selected_spread_rule=rule,
        spread_identification_sha256=identification.evidence_sha256,
        cost_status=_SPREAD_READY if ready else "COST_UNAVAILABLE", series_sha256="")
    return replace(value, series_sha256=_sha(_body(value, "series_sha256")))


def clock_lineage(series: ClockDerivedSeries, *,
                  identification: ClockSpreadIdentification | None = None) -> dict:
    """Plain source contract and membership metadata; no seal or audit receipt."""
    _checked_series(series)
    if series.spread_identification_sha256 is not None:
        _checked_identification(identification)
        if (series.spread_identification_sha256 != identification.evidence_sha256
                or series.template.template_sha256 != identification.template_sha256
                or series.template.corroboration_source != identification.native_control_source):
            _fail("CLOCK_LINEAGE_CONTROL_BINDING_MISMATCH")
    elif identification is not None:
        _fail("CLOCK_LINEAGE_CONTROL_BINDING_MISMATCH")
    body = {"schema_version": "fxpm.observed-clock-data-lineage.v1",
        "source_kind": SOURCE_KIND, "data_scope": DATA_SCOPE, "readiness_label": DATA_SCOPE,
        "full_native_context_eligibility": False, "financial_consumer_ready": False,
        **{name: False for name in _AUTHORITIES},
        "authentication": "CALLER_AUTHENTICATED_INPUTS_REQUIRED_NOT_VERIFIED_BY_THIS_ENGINE",
        "calendar_interpretation": "FINITE_RETROSPECTIVE_OBSERVED_C_TO_DISTINCT_D_TRANSFER_ONLY",
        "target_native_status": None,
        "target_metadata_binding": "REQUIRES_FUTURE_AUTHENTICATED_TARGET_METADATA_ADAPTER",
        "template": asdict(series.template), "m5_source": asdict(series.source),
        **asdict(series.extent), "series_sha256": series.series_sha256,
        "membership_sha256": series.membership_sha256,
        "source_member_count": series.source_member_count,
        "observed_gap_count": series.observed_gap_count,
        "empty_clock_ordinals": series.empty_clock_ordinals,
        "buckets": [{"clock_ordinal": row.clock_ordinal,
                     "interval": asdict(series.template.clock[row.clock_ordinal]),
                     "member_count": row.member_count, "membership_sha256": row.membership_sha256}
                    for row in series.bars],
        "cost_status": series.cost_status, "selected_spread_rule": series.selected_spread_rule,
        "spread_identification_sha256": series.spread_identification_sha256,
        "control_identification": asdict(identification) if identification is not None else None,
        "limitations": ["NO_GLOBAL_BROKER_CALENDAR_OR_TARGET_SESSION_EQUIVALENCE_PROOF",
                        "NO_HISTORICAL_CAUSAL_CALENDAR_AVAILABILITY_PROOF",
                        "NO_NATIVE_PROFILE_FINANCIAL_OR_EXECUTION_AUTHORITY"]}
    return {**body, "lineage_sha256": _sha(body)}
