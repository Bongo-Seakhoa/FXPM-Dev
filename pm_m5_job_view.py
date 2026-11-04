"""Cell-owned M5 views from original same-generation native prefix products.

No NativeInputs authority route, result writer or financial eligibility grant is
created here. Pure helpers authenticate nothing. Caller result bindings describe
validation by the existing consumer; strings cannot prove a durable write/fsync.
"""
from __future__ import annotations

from contextlib import closing
from dataclasses import asdict, dataclass, fields, is_dataclass
from datetime import datetime, timedelta
import hashlib
import json
import math
import os
from pathlib import Path
from collections.abc import Mapping
from typing import Callable

import pm_authority_spool as spool
import pm_native_prefix_production as native_prefix

from pm_explicit_derived_history import (
    Bar, DerivedSeries, Interval, PROTECTED_CUTOFF, RECIPE_SHA256, SourceIdentity,
    SpreadIdentification, Stratum, apply_spread_rule, derive_bars,
)
from pm_explicit_derived_native import (
    _CsvReadContract, _CsvReadState, _no_reparse, _read_csv_bars,
    _safe_path, _stat_identity, aligned_prefix_geometry, instrument_session_class,
)
from pm_explicit_observed_clock import (
    ClockDerivedSeries, ClockExtent, ClockSpreadIdentification,
    ObservedClockTemplate,
)


SOURCE_KIND = "m5_derived"
DATA_SCOPE = "DATA_ONLY_NOT_EXECUTION_READY"
_TIMEFRAMES = {"M15", "M30", "H1", "H4", "D1"}


class M5JobViewError(ValueError):
    def __init__(self, code: str, detail: str = "") -> None:
        self.code = code
        self.detail = detail[:512]
        super().__init__(code, self.detail)


def _fail(code: str, detail: str = "") -> None:
    raise M5JobViewError(code, detail)


def _digest(value) -> str:
    return hashlib.sha256(json.dumps(value, allow_nan=False, sort_keys=True,
        ensure_ascii=True, separators=(",", ":")).encode("ascii")).hexdigest()


def _sha(value) -> bool:
    return type(value) is str and len(value) == 64 and all(c in "0123456789abcdef" for c in value)


def _text(value) -> bool:
    return type(value) is str and bool(value) and value == value.strip()


def _positive_finite(value) -> bool:
    try:
        return type(value) in (int, float) and math.isfinite(value) and value > 0
    except OverflowError:
        return False


def _utc(value: str) -> datetime:
    if not _text(value):
        _fail("M5_VIEW_UTC_INVALID")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as error:
        raise M5JobViewError("M5_VIEW_UTC_INVALID") from error
    if parsed.tzinfo is None or parsed.utcoffset() != timedelta(0) or parsed.microsecond:
        _fail("M5_VIEW_UTC_INVALID")
    return parsed


def _bounds(interval: Interval) -> tuple[datetime, datetime]:
    if type(interval) is not Interval:
        _fail("M5_VIEW_INTERVAL_INVALID")
    start, end = _utc(interval.start_utc), _utc(interval.end_exclusive_utc)
    if start >= end:
        _fail("M5_VIEW_INTERVAL_INVALID")
    return start, end


def _interval(start: datetime, end: datetime) -> Interval:
    return Interval(start.isoformat(timespec="seconds"), end.isoformat(timespec="seconds"))


def _immutable(value) -> bool:
    if is_dataclass(value) and not isinstance(value, type):
        return all(_immutable(getattr(value, f.name)) for f in fields(value))
    if type(value) is tuple:
        return all(_immutable(item) for item in value)
    return value is None or type(value) in (str, int, float, bool)


def _same_source(left: SourceIdentity, right: SourceIdentity) -> bool:
    return (type(left) is SourceIdentity and type(right) is SourceIdentity
            and _digest(asdict(left)) == _digest(asdict(right)))


@dataclass(frozen=True, slots=True)
class DerivedJobBinding:
    m5_view_contract_sha256: str
    generation_context_sha256: str
    config_sha256: str
    effective_config_sha256: str
    search_process_receipt_sha256: str
    precommit_sha256: str
    run_id: str
    cell_id: str
    view_budgets_sha256: str
    view_plan_sha256: str
    required_result_keys: tuple[str, ...]
    terminal_result_key: str


@dataclass(frozen=True, slots=True)
class ViewBudgets:
    max_source_rows: int
    max_source_bytes: int
    max_view_rows: int
    max_view_bytes: int
    max_cache_rows: int
    max_cache_bytes: int


@dataclass(frozen=True, slots=True)
class SlotViewBinding:
    request_sha256: str
    slot_id: str


@dataclass(frozen=True, slots=True)
class AccountViewBinding:
    measurement_purpose: str
    measurement_end_utc: str
    saved_owner_sha256: str
    source_contract_sha256: str


@dataclass(frozen=True, slots=True)
class NoTradeAccountViewBinding:
    """Caller-verified all-selected-NO_TRADE run metadata, not an issuer."""
    measurement_purpose: str
    measurement_end_utc: str
    no_trade_run_receipt_sha256: str
    precommit_sha256: str


@dataclass(frozen=True, slots=True)
class CalibrationViewBinding:
    profile_sha256: str


@dataclass(frozen=True, slots=True)
class M5ArchiveSource:
    symbol: str
    source_identity: SourceIdentity
    source_identity_sha256: str
    request_interval: Interval
    prefix_interval: Interval
    instrument_session_class: str
    expected_rows: int
    expected_bytes: int
    provenance_bytes: int
    metadata_digest: str
    bars_path: str
    provenance_path: str


@dataclass(frozen=True, slots=True)
class MemberSpan:
    first_open_utc: str
    last_open_utc: str
    last_close_utc: str


@dataclass(frozen=True, slots=True)
class M5ReadObservation:
    """One canonical parse; not total source-verifier or filesystem I/O."""
    source_identity_sha256: str
    bars_sha256: str
    bytes: int
    rows: int
    provenance_sha256: str
    request_sha256: str


@dataclass(frozen=True, slots=True)
class ViewRequest:
    view_id: str
    source: SourceIdentity
    timeframe: str
    interval: Interval
    cutoff_utc: str
    constructor_implementation_sha256: str
    gap_findings_sha256: str
    result_key: str
    recipe_sha256: str
    consumer: SlotViewBinding | AccountViewBinding | NoTradeAccountViewBinding | CalibrationViewBinding | None = None
    stratum: Stratum | None = None
    clock_template: ObservedClockTemplate | None = None
    clock_extent: ClockExtent | None = None
    spread_identification: SpreadIdentification | ClockSpreadIdentification | None = None
    execution_required: bool = False
    source_kind: str = SOURCE_KIND
    purpose: str = "FINANCIAL_INPUT"


@dataclass(frozen=True, slots=True)
class ViewCoverage:
    required_interval: Interval
    examined_intervals: tuple[Interval, ...]
    retained_intervals: tuple[Interval, ...]
    unexamined_intervals: tuple[Interval, ...]
    unretained_intervals: tuple[Interval, ...]
    available_interval: Interval | None
    omitted_intervals: tuple[Interval, ...]
    observed_gaps: tuple[Interval, ...] | None
    empty_clock_ordinals: tuple[int, ...]


@dataclass(frozen=True, slots=True)
class DerivedPrefixView:
    view_key: str
    source: SourceIdentity
    source_metadata_digest: str
    constructor_implementation_sha256: str
    gap_findings_sha256: str
    timeframe: str
    interval: Interval
    cutoff_utc: str
    rows: tuple[Bar, ...]
    member_spans: tuple[MemberSpan, ...]
    series: DerivedSeries | ClockDerivedSeries
    coverage: ViewCoverage
    payload_sha256: str
    membership_sha256: str
    cost_status: str
    constructor_evidence_status: str
    source_kind: str = SOURCE_KIND
    data_scope: str = DATA_SCOPE
    profile_status: str = "PROFILE_UNVERIFIED"
    full_native_context_eligibility: bool = False
    financial_consumer_ready: bool = False
    qualification_authority: bool = False
    capital_authority: bool = False
    live_authority: bool = False
    order_authority: bool = False


@dataclass(frozen=True, slots=True)
class CallerValidatedResultBinding:
    result_sha256: str
    job_binding_sha256: str
    consumer_binding_sha256: tuple[str, ...]
    result_descriptor_sha256: str | None = None
    existing_producer_receipt_sha256: str | None = None


@dataclass(frozen=True, slots=True)
class UnusedResultBinding:
    reason: str


@dataclass(frozen=True, slots=True)
class EndGridResultBinding:
    path: str
    row_sha256: str
    appended: bool
    readback_row_sha256: str


@dataclass(frozen=True, slots=True)
class ViewSourceSummary:
    view_id: str
    status: str
    payload_sha256: str | None
    membership_sha256: str | None
    source_sha256: str
    constructor_implementation_sha256: str
    gap_findings_sha256: str
    row_count: int | None
    source_member_count: int | None
    coverage: ViewCoverage | None
    cost_status: str | None
    stop_reason: str | None


@dataclass(frozen=True, slots=True)
class JobSourceSummary:
    binding: DerivedJobBinding
    budgets: ViewBudgets
    results: tuple[tuple[str, CallerValidatedResultBinding | UnusedResultBinding], ...]
    end_grid_rows: tuple[tuple[str, EndGridResultBinding], ...]
    views: tuple[ViewSourceSummary, ...]
    source_pins: tuple[M5ReadObservation, ...]
    lease_observations: tuple[LeaseObservation, ...]
    source_kind: str = SOURCE_KIND
    result_assurance: str = "CALLER_VALIDATED_EXISTING_RESULT_NOT_DURABILITY_PROOF_BY_JOB_VIEW"
    owned_values_released: bool = True
    release_eligible: bool = True
    full_native_context_eligibility: bool = False
    financial_consumer_ready: bool = False


def _binding(binding: DerivedJobBinding) -> str:
    if type(binding) is not DerivedJobBinding:
        _fail("M5_JOB_BINDING_INVALID")
    if (any(not _sha(getattr(binding, name)) for name in (
            "m5_view_contract_sha256", "generation_context_sha256", "config_sha256",
            "effective_config_sha256", "search_process_receipt_sha256", "precommit_sha256",
            "view_budgets_sha256", "view_plan_sha256"))
            or not _text(binding.run_id) or not _text(binding.cell_id)
            or type(binding.required_result_keys) is not tuple or not binding.required_result_keys
            or any(not _text(key) for key in binding.required_result_keys)
            or len(set(binding.required_result_keys)) != len(binding.required_result_keys)
            or binding.terminal_result_key not in binding.required_result_keys):
        _fail("M5_JOB_BINDING_INVALID")
    return _digest(asdict(binding))


def _budgets(binding: DerivedJobBinding, budgets: ViewBudgets) -> None:
    if type(budgets) is not ViewBudgets or any(
            type(getattr(budgets, f.name)) is not int or getattr(budgets, f.name) <= 0
            for f in fields(budgets)):
        _fail("M5_VIEW_BUDGETS_INVALID")
    if _digest(asdict(budgets)) != binding.view_budgets_sha256:
        _fail("M5_VIEW_BUDGET_BINDING_MISMATCH")


def _encoded_bytes(value) -> int:
    try:
        return sum(len(part.encode("ascii")) for part in json.JSONEncoder(
            allow_nan=False, sort_keys=True, ensure_ascii=True,
            separators=(",", ":")).iterencode(value))
    except (TypeError, ValueError, OverflowError) as error:
        raise M5JobViewError("M5_VIEW_BUDGET_INPUT_INVALID") from error


def _cache_limit(budgets: ViewBudgets, rows: int, size: int) -> None:
    if rows > budgets.max_cache_rows:
        _fail("M5_VIEW_CACHE_ROW_BUDGET")
    if size > budgets.max_cache_bytes:
        _fail("M5_VIEW_CACHE_BYTE_BUDGET")


def _view_limit(budgets: ViewBudgets, rows: int, size: int,
                retained_rows: int, retained_bytes: int) -> None:
    if rows > budgets.max_view_rows:
        _fail("M5_VIEW_ROW_BUDGET")
    if size > budgets.max_view_bytes:
        _fail("M5_VIEW_BYTE_BUDGET")
    _cache_limit(budgets, retained_rows + rows, retained_bytes + size)


def _counter(value) -> int:
    try:
        if type(value) not in (int, float) or not math.isfinite(value) or value < 0 or int(value) != value:
            _fail("M5_VIEW_BUDGET_INPUT_INVALID")
        return int(value)
    except (OverflowError, ValueError) as error:
        if isinstance(error, M5JobViewError):
            raise
        raise M5JobViewError("M5_VIEW_BUDGET_INPUT_INVALID") from error


def _view_reservation(request: ViewRequest, rows: tuple[Bar, ...]) -> tuple[int, int]:
    """Conservative logical-row bound BEFORE the unchanged constructor allocates.

    OHLC values are selected members; nonnegative volume sums cannot exceed the
    whole input sum. This counts encoded DerivedBar values, not Python/RSS size.
    Existing consumers must still enforce their own encoded-frame ceilings.
    """
    if not rows:
        return 0, 0
    if request.clock_template is None:
        seconds = {"M15": 900, "M30": 1800, "H1": 3600}[request.timeframe]
        start, end = _bounds(request.interval)
        count = min(len(rows), int((end - start).total_seconds()) // seconds)
        axes = (request.interval,)
    else:
        count = min(len(rows), len(request.clock_template.clock))
        axes = request.clock_template.clock
    axis_width = max(_encoded_bytes(value) for axis in axes
        for value in (axis.start_utc, axis.end_exclusive_utc,
                      _utc(axis.start_utc).isoformat(timespec="seconds"),
                      _utc(axis.end_exclusive_utc).isoformat(timespec="seconds")))
    probe = asdict(Bar("", "", 0, 0, 0, 0, 0, None, None))
    widths = {name: max(_encoded_bytes(getattr(row, name)) for row in rows)
              for name in ("open", "high", "low", "close")}
    widths.update(open_utc=axis_width, close_utc=axis_width,
        volume=_encoded_bytes(sum(_counter(row.volume) for row in rows)),
        real_volume=max(4, _encoded_bytes(sum(_counter(row.real_volume)
            for row in rows if row.real_volume is not None))),
        spread=max(4, max((_encoded_bytes(_counter(row.spread))
                          for row in rows if row.spread is not None), default=4)))
    bar_bound = _encoded_bytes(probe) + sum(widths[name] - _encoded_bytes(value)
                                          for name, value in probe.items())
    wrapper = {"value": {}, "member_count": len(rows), "membership_sha256": "0" * 64,
               "spread_candidates": (None, None, None, None)}
    if request.clock_template is not None:
        wrapper["clock_ordinal"] = len(request.clock_template.clock)
    per_row = bar_bound + _encoded_bytes(wrapper) - 2 + 4 * max(0, widths["spread"] - 4)
    member_width = max(_encoded_bytes(value) for row in rows for value in
                       (row.open_utc, row.close_utc))
    member_probe = {"first_open_utc": "", "last_open_utc": "", "last_close_utc": ""}
    per_row += _encoded_bytes(member_probe) - 6 + 3 * member_width
    per_row += _encoded_bytes({"derived": {}, "members": {}}) - 4
    return count, count * per_row


def _consumer(binding: DerivedJobBinding, consumer) -> str:
    if type(consumer) is SlotViewBinding:
        if not _sha(consumer.request_sha256) or not _text(consumer.slot_id):
            _fail("M5_VIEW_CONSUMER_BINDING_INVALID")
    elif type(consumer) is AccountViewBinding:
        if (not _text(consumer.measurement_purpose) or not _sha(consumer.saved_owner_sha256)
                or not _sha(consumer.source_contract_sha256)):
            _fail("M5_VIEW_CONSUMER_BINDING_INVALID")
        _utc(consumer.measurement_end_utc)
    elif type(consumer) is NoTradeAccountViewBinding:
        if (not _text(consumer.measurement_purpose) or not _sha(consumer.no_trade_run_receipt_sha256)
                or not _sha(consumer.precommit_sha256)
                or consumer.precommit_sha256 != binding.precommit_sha256):
            _fail("M5_VIEW_CONSUMER_BINDING_INVALID")
        _utc(consumer.measurement_end_utc)
    elif type(consumer) is CalibrationViewBinding:
        if not _sha(consumer.profile_sha256):
            _fail("M5_VIEW_CONSUMER_BINDING_INVALID")
    else:
        _fail("M5_VIEW_CONSUMER_BINDING_INVALID")
    return _digest({"kind": type(consumer).__name__, "binding": asdict(consumer)})


def _request(binding: DerivedJobBinding, request: ViewRequest) -> str:
    _binding(binding)
    if (type(request) is not ViewRequest or not _immutable(request)
            or not _text(request.view_id) or request.timeframe not in _TIMEFRAMES
            or type(request.source) is not SourceIdentity or request.source.timeframe != "M5"
            or not _sha(request.constructor_implementation_sha256) or not _sha(request.gap_findings_sha256)
            or request.result_key not in binding.required_result_keys
            or request.recipe_sha256 != RECIPE_SHA256
            or request.source_kind not in (SOURCE_KIND, "m5_derived_control_clock.v1")
            or request.purpose not in ("FINANCIAL_INPUT", "COST_EDITION_REPRODUCTION_ONLY")
            or type(request.execution_required) is not bool):
        _fail("M5_VIEW_REQUEST_INVALID")
    if request.consumer is not None:
        _consumer(binding, request.consumer)
    if (not _text(request.source.symbol) or not _text(request.source.activation_id)
            or any(not _sha(getattr(request.source, name)) for name in (
                "activation_sha256", "bars_sha256", "sidecar_sha256", "provenance_sha256", "request_sha256"))
            or not _positive_finite(request.source.point)
            or request.source.spread_unit != "points"):
        _fail("M5_VIEW_SOURCE_BINDING_INVALID")
    start, end = _bounds(request.interval)
    cutoff = _utc(request.cutoff_utc)
    if end > cutoff or cutoff > _utc(PROTECTED_CUTOFF):
        _fail("M5_VIEW_CUTOFF_VIOLATION")
    if type(request.consumer) in (AccountViewBinding, NoTradeAccountViewBinding) and end > _utc(request.consumer.measurement_end_utc):
        _fail("M5_VIEW_MEASUREMENT_END_MISMATCH")
    if request.timeframe in {"H4", "D1"}:
        if (type(request.clock_template) is not ObservedClockTemplate
                or type(request.clock_extent) is not ClockExtent
                or request.stratum is not None
                or request.clock_template.stratum.timeframe != request.timeframe
                or type(request.clock_template.clock) is not tuple or not request.clock_template.clock
                or any(type(axis) is not Interval for axis in request.clock_template.clock)
                or request.clock_template.interval != request.interval
                or _utc(request.clock_template.cutoff_utc) != cutoff
                or request.clock_extent.available_interval != request.interval):
            _fail("M5_VIEW_CLOCK_BINDING_INVALID")
        if request.spread_identification is not None and type(request.spread_identification) is not ClockSpreadIdentification:
            _fail("M5_VIEW_SPREAD_BINDING_INVALID")
    elif (type(request.stratum) is not Stratum or request.stratum.timeframe != request.timeframe
            or request.clock_template is not None or request.clock_extent is not None):
        _fail("M5_VIEW_FIXED_BINDING_INVALID")
    elif request.spread_identification is not None and type(request.spread_identification) is not SpreadIdentification:
        _fail("M5_VIEW_SPREAD_BINDING_INVALID")
    return _digest(_value_request_body(request))


def _value_request_body(request: ViewRequest) -> dict:
    body = asdict(request)
    # Consumer/use bindings guard leases, not immutable bar-value identity.
    for name in ("view_id", "consumer", "execution_required", "result_key", "purpose"):
        del body[name]
    return body


_AXIS_PLAN_KEYS = frozenset(("view_id", "symbol", "timeframe", "interval", "cutoff_utc",
    "recipe_sha256", "constructor_implementation_sha256", "gap_findings_sha256", "purpose",
    "source_kind", "stratum", "clock_template_sha256", "clock_axes_sha256",
    "spread_identification_sha256"))


def declared_view_plan_sha256(plans: tuple[dict, ...]) -> str:
    """Hash exact pre-access axes/pins; no observed source or future owner fields."""
    if type(plans) is not tuple or not plans:
        _fail("M5_VIEW_PLAN_INVALID")
    ids = set()
    for row in plans:
        if (type(row) is not dict or set(row) != _AXIS_PLAN_KEYS
                or not _text(row["view_id"]) or not _text(row["symbol"])
                or row["view_id"] in ids or row["timeframe"] not in _TIMEFRAMES
                or row["source_kind"] not in (SOURCE_KIND, "m5_derived_control_clock.v1")
                or row["purpose"] not in ("FINANCIAL_INPUT", "COST_EDITION_REPRODUCTION_ONLY")
                or row["recipe_sha256"] != RECIPE_SHA256
                or any(not _sha(row[name]) for name in (
                    "constructor_implementation_sha256", "gap_findings_sha256"))
                or any(row[name] is not None and not _sha(row[name]) for name in (
                    "clock_template_sha256", "clock_axes_sha256", "spread_identification_sha256"))):
            _fail("M5_VIEW_PLAN_INVALID")
        interval = row["interval"]
        if type(interval) is not dict or set(interval) != {"start_utc", "end_exclusive_utc"}:
            _fail("M5_VIEW_PLAN_INVALID")
        _, end = _bounds(Interval(**interval))
        cutoff = _utc(row["cutoff_utc"])
        if end > cutoff or cutoff > _utc(PROTECTED_CUTOFF):
            _fail("M5_VIEW_PLAN_INVALID")
        stratum = row["stratum"]
        if row["timeframe"] in {"M15", "M30", "H1"}:
            if (type(stratum) is not dict
                    or set(stratum) != {"timeframe", "instrument_session_class", "alignment_identity"}
                    or stratum["timeframe"] != row["timeframe"]
                    or any(not _text(value) for value in stratum.values())):
                _fail("M5_VIEW_PLAN_INVALID")
        elif stratum is not None:
            _fail("M5_VIEW_PLAN_INVALID")
        ids.add(row["view_id"])
    return _digest(sorted(plans, key=lambda row: row["view_id"]))


def value_plan_sha256(requests: tuple[ViewRequest, ...]) -> str:
    if type(requests) is not tuple or any(type(row) is not ViewRequest or not _immutable(row) for row in requests):
        _fail("M5_VIEW_PLAN_INVALID")
    plans = tuple({"view_id": row.view_id, "symbol": row.source.symbol,
        "timeframe": row.timeframe, "interval": asdict(row.interval), "cutoff_utc": row.cutoff_utc,
        "recipe_sha256": row.recipe_sha256,
        "constructor_implementation_sha256": row.constructor_implementation_sha256,
        "gap_findings_sha256": row.gap_findings_sha256, "purpose": row.purpose,
        "source_kind": row.source_kind, "stratum": asdict(row.stratum) if row.stratum else None,
        "clock_template_sha256": row.clock_template.template_sha256 if row.clock_template else None,
        "clock_axes_sha256": row.clock_template.axes_sha256 if row.clock_template else None,
        "spread_identification_sha256": row.spread_identification.evidence_sha256 if row.spread_identification else None}
        for row in requests)
    return declared_view_plan_sha256(plans)


def _execution_use(request: ViewRequest) -> None:
    if request.source_kind == "m5_derived_control_clock.v1" or request.timeframe in {"H4", "D1"}:
        _fail("M5_VIEW_CONTROL_CLOCK_SOURCE_HELD")
    if request.execution_required:
        _fail("M5_VIEW_CLOCK_CAUSAL_ELIGIBILITY_UNPROVED" if request.clock_template
              else "M5_VIEW_COST_PROFILE_UNVERIFIED")


def _merge(intervals: tuple[Interval, ...]) -> tuple[Interval, ...]:
    pairs = sorted(_bounds(row) for row in intervals)
    merged: list[tuple[datetime, datetime]] = []
    for start, end in pairs:
        if merged and start <= merged[-1][1]:
            merged[-1] = (merged[-1][0], max(end, merged[-1][1]))
        else:
            merged.append((start, end))
    return tuple(_interval(start, end) for start, end in merged)


def _missing(interval: Interval, examined: tuple[Interval, ...]) -> tuple[Interval, ...]:
    start, end = _bounds(interval)
    cursor = start
    missing = []
    for lower, upper in map(_bounds, _merge(examined)):
        if upper <= cursor or lower >= end:
            continue
        if lower > cursor:
            missing.append(_interval(cursor, min(lower, end)))
        cursor = max(cursor, min(upper, end))
    if cursor < end:
        missing.append(_interval(cursor, end))
    return tuple(missing)


def _observed_gaps(interval: Interval, rows: tuple[Bar, ...]) -> tuple[Interval, ...]:
    start, end = _bounds(interval)
    cursor, gaps = start, []
    for row in rows:
        opened, closed = _utc(row.open_utc), _utc(row.close_utc)
        if opened < start or closed > end or closed <= opened or opened < cursor:
            _fail("M5_VIEW_OBSERVED_INTERVAL_INVALID")
        if opened > cursor:
            gaps.append(_interval(cursor, opened))
        cursor = closed
    if cursor < end:
        gaps.append(_interval(cursor, end))
    return tuple(gaps)


def _view_row_bytes(row, members: MemberSpan) -> int:
    return _encoded_bytes({"derived": asdict(row), "members": asdict(members)})


def _member_spans(series: DerivedSeries, rows: tuple[Bar, ...]) -> tuple[MemberSpan, ...]:
    spans, cursor = [], 0
    for bucket in series.bars:
        start, end = _utc(bucket.value.open_utc), _utc(bucket.value.close_utc)
        first = cursor
        while cursor < len(rows) and _utc(rows[cursor].open_utc) < end:
            if _utc(rows[cursor].open_utc) < start or _utc(rows[cursor].close_utc) > end:
                _fail("M5_VIEW_MEMBER_SPAN_MISMATCH")
            cursor += 1
        if cursor == first or cursor - first != bucket.member_count:
            _fail("M5_VIEW_MEMBER_SPAN_MISMATCH")
        spans.append(MemberSpan(rows[first].open_utc, rows[cursor - 1].open_utc,
                                rows[cursor - 1].close_utc))
    if cursor != len(rows):
        _fail("M5_VIEW_MEMBER_SPAN_MISMATCH")
    return tuple(spans)


def _build_view(binding: DerivedJobBinding, request: ViewRequest, rows: tuple[Bar, ...], *,
                source: M5ArchiveSource, examined_interval: Interval, budgets: ViewBudgets,
                retained_cache_rows: int = 0, retained_cache_bytes: int = 0) -> DerivedPrefixView:
    """Construct from caller-supplied data; this pure helper proves no native trust."""
    _request(binding, request)
    _budgets(binding, budgets)
    _execution_use(request)
    if (type(source) is not M5ArchiveSource or source.symbol != request.source.symbol
            or not _same_source(source.source_identity, request.source)
            or not _sha(source.metadata_digest) or not _sha(source.source_identity_sha256)
            or type(rows) is not tuple or any(type(row) is not Bar for row in rows)
            or type(source.expected_rows) is not int or source.expected_rows <= 0
            or type(source.expected_bytes) is not int or source.expected_bytes <= 0
            or len(rows) > source.expected_rows):
        _fail("M5_VIEW_SOURCE_BINDING_INVALID")
    if len(rows) > budgets.max_source_rows:
        _fail("M5_VIEW_SOURCE_ROW_BUDGET")
    if sum(_encoded_bytes(asdict(row)) for row in rows) > budgets.max_source_bytes:
        _fail("M5_VIEW_SOURCE_BYTE_BUDGET")
    start, end = _bounds(request.interval)
    examined_start, examined_end = _bounds(examined_interval)
    source_start, source_end = _bounds(source.prefix_interval)
    if (examined_start > start or examined_end < end or examined_start < source_start
            or examined_end > source_end):
        _fail("M5_VIEW_EXAMINED_INTERVAL_MISMATCH")
    for row in rows:
        opened, closed = _utc(row.open_utc), _utc(row.close_utc)
        if opened < start or closed > end or closed > _utc(request.cutoff_utc):
            _fail("M5_VIEW_CUTOFF_OR_INTERVAL_VIOLATION")
    # Reserve logical output before either original constructor retains a row.
    reserved_rows, reserved_bytes = _view_reservation(request, rows)
    _view_limit(budgets, reserved_rows, reserved_bytes, retained_cache_rows, retained_cache_bytes)
    available = Interval(rows[0].open_utc, rows[-1].close_utc) if rows else None
    required = request.interval
    omitted = _missing(required, (available,) if available else ())
    if request.stratum.instrument_session_class != source.instrument_session_class:
        _fail("M5_VIEW_SOURCE_CLASS_MISMATCH")
    series = derive_bars(rows, stratum=request.stratum, source=request.source,
        interval=request.interval, cutoff_utc=request.cutoff_utc)
    if request.spread_identification is not None:
        series = apply_spread_rule(series, request.spread_identification)
    member_spans = _member_spans(series, rows)
    values, encoded_size = [], 0
    for row, members in zip(series.bars, member_spans, strict=True):
        next_size = encoded_size + _view_row_bytes(row, members)
        _view_limit(budgets, len(values) + 1, next_size, retained_cache_rows, retained_cache_bytes)
        values.append(row.value)
        encoded_size = next_size
    values = tuple(values)
    coverage = ViewCoverage(required, (examined_interval,), (request.interval,), (), (),
        available, omitted, _observed_gaps(request.interval, rows), ())
    return DerivedPrefixView(_request(binding, request),
        request.source, source.metadata_digest, request.constructor_implementation_sha256,
        request.gap_findings_sha256, request.timeframe, request.interval,
        request.cutoff_utc, values, member_spans, series, coverage, _digest([asdict(row) for row in values]),
        series.membership_sha256, series.cost_status,
        "FIXED_CONSTRUCTOR_ONLY")


class DerivedViewLease:
    __slots__ = ("_owner", "_view_id", "_view", "_consumer", "_result_key", "_released")

    def __init__(self, owner: _JobState, view_id: str, view: DerivedPrefixView,
                 consumer, result_key: str) -> None:
        self._owner, self._view_id, self._view = owner, view_id, view
        self._consumer, self._result_key, self._released = consumer, result_key, False

    @property
    def view(self) -> DerivedPrefixView:
        return self._owner.view(self)

    @property
    def consumer_binding_sha256(self) -> str:
        self.view
        return _consumer(self._owner.binding, self._consumer)

    @property
    def job_binding_sha256(self) -> str:
        self.view
        return self._owner.binding_sha256

    @property
    def m5_view_contract_sha256(self) -> str:
        self.view
        return self._owner.binding.m5_view_contract_sha256

    @property
    def result_key(self) -> str:
        self.view
        return self._result_key

    def release(self) -> None:
        if self._released:
            _fail("M5_VIEW_LEASE_RELEASED")
        self._owner._release(self)
        self._released, self._view = True, None

    def __enter__(self) -> DerivedPrefixView:
        return self.view

    def __exit__(self, exc_type, exc, traceback) -> None:
        if not self._released:
            self.release()


@dataclass(frozen=True, slots=True)
class LeaseObservation:
    view_id: str
    view_key: str
    result_key: str
    consumer_binding_sha256: str
    consumer: SlotViewBinding | AccountViewBinding | NoTradeAccountViewBinding | CalibrationViewBinding


class _JobState:
    """Cell value/lease bookkeeping. Test callbacks convey no native authority."""
    def __init__(self, binding: DerivedJobBinding, requests: tuple[ViewRequest, ...], *,
                 budgets: ViewBudgets) -> None:
        self.binding, self.binding_sha256 = binding, _binding(binding)
        _budgets(binding, budgets)
        self.plan_sha256 = value_plan_sha256(requests)
        if self.plan_sha256 != binding.view_plan_sha256:
            _fail("M5_VIEW_PLAN_BINDING_MISMATCH")
        self.budgets = budgets
        self.requests, self.keys = {}, {}
        for request in requests:
            self.keys[request.view_id] = _request(binding, request)
            self.requests[request.view_id] = request
        self.status = {key: "PENDING" for key in self.requests}
        self.cache, self.source_usage, self.cache_usage = {}, {}, {}
        self.active, self.records, self.uses = {}, {}, {}
        self.seen = {key: set() for key in binding.required_result_keys}
        self.closed, self.live_check, self.spool_root = False, None, None

    @property
    def consumer_sha256(self) -> tuple[str, ...]:
        return tuple(sorted(set().union(*self.seen.values())))

    def consumer_bindings_for(self, result_key: str) -> tuple[str, ...]:
        if result_key not in self.seen:
            _fail("M5_JOB_RESULT_KEY_NOT_DECLARED")
        return tuple(sorted(self.seen[result_key]))

    def _cache_totals(self) -> tuple[int, int]:
        values = (*self.source_usage.values(), *self.cache_usage.values())
        return sum(row[0] for row in values), sum(row[1] for row in values)

    def _retain_source_row(self, key: str, row: Bar) -> None:
        size = _encoded_bytes(asdict(row))
        rows = sum(value[0] for value in self.source_usage.values()) + 1
        total = sum(value[1] for value in self.source_usage.values()) + size
        if rows > self.budgets.max_source_rows:
            _fail("M5_VIEW_SOURCE_ROW_BUDGET")
        if total > self.budgets.max_source_bytes:
            _fail("M5_VIEW_SOURCE_BYTE_BUDGET")
        cached_rows, cached_bytes = self._cache_totals()
        _cache_limit(self.budgets, cached_rows + 1, cached_bytes + size)
        previous = self.source_usage.get(key, (0, 0))
        self.source_usage[key] = previous[0] + 1, previous[1] + size

    def _discard_source(self, key: str) -> None:
        self.source_usage.pop(key, None)

    def _live(self) -> None:
        if self.closed:
            _fail("M5_JOB_CLOSED")
        if self.live_check is not None:
            self.live_check()

    def view(self, lease: DerivedViewLease) -> DerivedPrefixView:
        if type(lease) is not DerivedViewLease or lease._owner is not self:
            _fail("M5_VIEW_LEASE_NOT_OWNED")
        if lease._released or self.closed:
            _fail("M5_VIEW_LEASE_RELEASED")
        if self.active.get(id(lease)) is not lease:
            _fail("M5_VIEW_LEASE_NOT_OWNED")
        self._live()
        return lease._view

    def consumer_binding(self, lease: DerivedViewLease):
        self.view(lease)
        return lease._consumer

    def financial_view(self, lease: DerivedViewLease) -> DerivedPrefixView:
        view = self.view(lease)
        if (type(self.consumer_binding(lease)) is CalibrationViewBinding
                or self.requests[lease._view_id].purpose != "FINANCIAL_INPUT"):
            _fail("M5_VIEW_CALIBRATION_NOT_FINANCIAL")
        return view

    def acquire(self, view_id: str, load_view: Callable[[ViewRequest], DerivedPrefixView], *,
                consumer=None, result_key: str | None = None) -> DerivedViewLease:
        self._live()
        if type(view_id) is not str or view_id not in self.requests:
            _fail("M5_VIEW_NOT_DECLARED")
        if self.status[view_id] in {"UNUSED", "STOPPED"}:
            _fail("M5_VIEW_NOT_AVAILABLE")
        request = self.requests[view_id]
        actual_consumer = request.consumer if consumer is None else consumer
        actual_key = request.result_key if result_key is None else result_key
        if actual_consumer is None:
            _fail("M5_VIEW_CONSUMER_REQUIRED")
        consumer_sha = _consumer(self.binding, actual_consumer)
        if not _text(actual_key) or actual_key not in self.seen:
            _fail("M5_JOB_RESULT_KEY_NOT_DECLARED")
        if ((request.purpose == "COST_EDITION_REPRODUCTION_ONLY")
                != (type(actual_consumer) is CalibrationViewBinding)):
            _fail("M5_VIEW_PURPOSE_MISMATCH")
        if type(actual_consumer) in (AccountViewBinding, NoTradeAccountViewBinding) and _utc(request.interval.end_exclusive_utc) > _utc(actual_consumer.measurement_end_utc):
            _fail("M5_VIEW_MEASUREMENT_END_MISMATCH")
        key = self.keys[view_id]
        try:
            _execution_use(request)
            if key not in self.cache:
                view = load_view(request)
                if (type(view) is not DerivedPrefixView or view.view_key != key
                        or not _same_source(view.source, request.source)
                        or view.timeframe != request.timeframe or view.interval != request.interval
                        or view.cutoff_utc != request.cutoff_utc
                        or view.constructor_implementation_sha256 != request.constructor_implementation_sha256
                        or view.gap_findings_sha256 != request.gap_findings_sha256
                        or view.source_kind != SOURCE_KIND or view.data_scope != DATA_SCOPE
                        or view.profile_status != "PROFILE_UNVERIFIED"
                        or any(getattr(view, name) is not False for name in (
                            "full_native_context_eligibility", "financial_consumer_ready",
                            "qualification_authority", "capital_authority", "live_authority", "order_authority"))):
                    _fail("M5_VIEW_OWNER_BINDING_MISMATCH")
                size = sum(_view_row_bytes(row, span) for row, span in
                           zip(view.series.bars, view.member_spans, strict=True))
                retained_rows, retained_bytes = self._cache_totals()
                _view_limit(self.budgets, len(view.rows), size, retained_rows, retained_bytes)
                self.cache[key], self.cache_usage[key] = view, (len(view.rows), size)
            view = self.cache[key]
        except BaseException as error:
            self.status[view_id] = "STOPPED"
            self.records[view_id] = ViewSourceSummary(view_id, "STOPPED", None, None,
                _digest(asdict(request.source)), request.constructor_implementation_sha256,
                request.gap_findings_sha256, None, None, None, None,
                str(getattr(error, "code", type(error).__name__))[:512])
            raise
        lease = DerivedViewLease(self, view_id, view, actual_consumer, actual_key)
        self.active[id(lease)], self.status[view_id] = lease, "USED"
        self.seen[actual_key].add(consumer_sha)
        observation = LeaseObservation(view_id, key, actual_key, consumer_sha, actual_consumer)
        self.uses[(view_id, actual_key, consumer_sha)] = observation
        self.records[view_id] = ViewSourceSummary(view_id, "USED", view.payload_sha256,
            view.membership_sha256, _digest(asdict(view.source)),
            view.constructor_implementation_sha256, view.gap_findings_sha256, len(view.rows),
            view.series.source_member_count, view.coverage, view.cost_status, None)
        return lease

    def _release(self, lease: DerivedViewLease) -> None:
        self.view(lease)
        del self.active[id(lease)]
        # A released lease does NOT release cell values. Saved-slot/account
        # readers may acquire them again until the terminal/end-grid fence.

    def release_unused(self, view_id: str, reason: str) -> None:
        self._live()
        if type(view_id) is not str or self.status.get(view_id) != "PENDING":
            _fail("M5_VIEW_NOT_PENDING")
        if not _text(reason) or len(reason) > 512:
            _fail("M5_VIEW_UNUSED_REASON_INVALID")
        request = self.requests[view_id]
        self.status[view_id] = "UNUSED"
        self.records[view_id] = ViewSourceSummary(view_id, "UNUSED", None, None,
            _digest(asdict(request.source)), request.constructor_implementation_sha256,
            request.gap_findings_sha256, None, None, None, None, reason)

    def finish(self, results: Mapping[str, CallerValidatedResultBinding | UnusedResultBinding], *,
               end_grid_rows: Mapping[str, EndGridResultBinding],
               revalidate: Callable[[], tuple[M5ReadObservation, ...]]) -> JobSourceSummary:
        self._live()
        if self.active or "PENDING" in self.status.values():
            _fail("M5_JOB_CONSUMERS_INCOMPLETE")
        required = set(self.binding.required_result_keys)
        terminal = self.binding.terminal_result_key
        if not isinstance(results, Mapping) or set(results) != required:
            _fail("M5_JOB_RESULT_COVERAGE_INVALID")
        for key, result in results.items():
            if type(result) is UnusedResultBinding:
                planned = [row for row in self.requests.values() if row.result_key == key]
                if (key == terminal or self.seen[key] or not planned
                        or any(self.status[row.view_id] != "UNUSED" for row in planned)
                        or not _text(result.reason) or len(result.reason) > 512):
                    _fail("M5_JOB_UNUSED_RESULT_INVALID")
                continue
            if (type(result) is not CallerValidatedResultBinding or not _sha(result.result_sha256)
                    or result.job_binding_sha256 != self.binding_sha256
                    or result.consumer_binding_sha256 != self.consumer_bindings_for(key)
                    or any(value is not None and not _sha(value) for value in (
                        result.result_descriptor_sha256, result.existing_producer_receipt_sha256))):
                _fail("M5_JOB_RESULT_BINDING_INVALID")
        if (not isinstance(end_grid_rows, Mapping) or terminal not in end_grid_rows
                or not set(end_grid_rows).issubset(required)):
            _fail("M5_JOB_END_GRID_COVERAGE_INVALID")
        described = {key for key, result in results.items()
                     if type(result) is CallerValidatedResultBinding
                     and result.result_descriptor_sha256 is not None}
        if described != set(end_grid_rows):
            _fail("M5_JOB_END_GRID_COVERAGE_INVALID")
        for key, row in end_grid_rows.items():
            result = results[key]
            if (type(row) is not EndGridResultBinding or not _text(row.path)
                    or not Path(row.path).is_absolute() or ".." in Path(row.path).parts
                    or type(row.appended) is not bool or not _sha(row.row_sha256)
                    or row.readback_row_sha256 != row.row_sha256
                    or type(result) is not CallerValidatedResultBinding
                    or result.result_descriptor_sha256 != row.row_sha256):
                _fail("M5_JOB_END_GRID_BINDING_INVALID")
            if self.spool_root is not None and Path(row.path).is_relative_to(self.spool_root):
                _fail("M5_JOB_END_GRID_INSIDE_SPOOL")
        try:
            observations = revalidate()
            if type(observations) is not tuple or any(type(row) is not M5ReadObservation for row in observations):
                _fail("M5_JOB_SOURCE_PIN_RESULT_INVALID")
            summary = JobSourceSummary(self.binding, self.budgets,
                tuple((key, results[key]) for key in sorted(results)),
                tuple((key, end_grid_rows[key]) for key in sorted(end_grid_rows)),
                tuple(self.records[key] for key in sorted(self.records)), observations,
                tuple(self.uses[key] for key in sorted(self.uses)))
        except BaseException:
            self.close()
            raise
        self.close()
        return summary

    def close(self) -> None:
        self.cache.clear()
        self.source_usage.clear()
        self.cache_usage.clear()
        for lease in self.active.values():
            lease._released, lease._view = True, None
        self.active.clear()
        self.requests.clear()
        self.keys.clear()
        self.records.clear()
        self.uses.clear()
        self.live_check = None
        self.closed = True


def _reverified_products(context, products: tuple, canonical_data_dir: Path):
    if (type(context) is not spool.GenerationSpoolContext or type(products) is not tuple
            or not products or any(type(row) is not native_prefix.NativePrefixProduct for row in products)):
        _fail("M5_JOB_ORIGINAL_PREFIX_PRODUCTS_REQUIRED")
    data = Path(canonical_data_dir)
    try:
        canonical = data.resolve(strict=True)
    except OSError as error:
        raise M5JobViewError("M5_JOB_CANONICAL_DATA_DIR_INVALID") from error
    if not data.is_absolute() or data != canonical:
        _fail("M5_JOB_CANONICAL_DATA_DIR_INVALID")
    _no_reparse(data)
    reopened = spool.reopen_generation_context(context.root,
        expected_generation_id=context.generation_id,
        expected_native_universe_sha256=context.native_universe_sha256,
        expected_config_sha256=context.config_sha256)
    if reopened.context_sha256 != context.context_sha256:
        _fail("M5_JOB_GENERATION_CONTEXT_CHANGED")
    checked = []
    for product in products:
        if product._data_dir != data:
            _fail("M5_JOB_CANONICAL_DATA_DIR_MISMATCH")
        verified = native_prefix.reverify_activated_native_prefix_product(
            product, reopened, require_current_activation=True)
        if verified.source_state != native_prefix.AVAILABLE or verified.index["timeframe"] != "M5":
            _fail("M5_JOB_AVAILABLE_M5_PRODUCT_REQUIRED")
        checked.append(verified)
    return reopened, tuple(checked), data


def _checked_provenance(path: Path, expected_sha256: str, expected_bytes: int) -> dict:
    if type(expected_bytes) is not int or not 0 < expected_bytes <= native_prefix.MAX_PROVENANCE_BYTES:
        _fail("M5_JOB_PROVENANCE_SIZE_INVALID")
    before = _no_reparse(path)
    if before.st_size != expected_bytes:
        _fail("M5_JOB_PROVENANCE_CHANGED")
    with path.open("rb") as handle:
        first = os.fstat(handle.fileno())
        if (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns) != (
                first.st_dev, first.st_ino, first.st_size, first.st_mtime_ns):
            _fail("M5_JOB_PROVENANCE_CHANGED")
        raw = handle.read(expected_bytes + 1)
        last = os.fstat(handle.fileno())
    after = _no_reparse(path)
    if (_stat_identity(before) != _stat_identity(after)
            or _stat_identity(first) != _stat_identity(last)
            or len(raw) != expected_bytes or hashlib.sha256(raw).hexdigest() != expected_sha256):
        _fail("M5_JOB_PROVENANCE_CHANGED")
    def pairs(items):
        value = {}
        for key, item in items:
            if key in value:
                _fail("M5_JOB_PROVENANCE_JSON_INVALID")
            value[key] = item
        return value
    try:
        value = json.loads(raw, object_pairs_hook=pairs,
            parse_constant=lambda value: _fail("M5_JOB_PROVENANCE_JSON_INVALID"))
    except (ValueError, UnicodeError) as error:
        raise M5JobViewError("M5_JOB_PROVENANCE_JSON_INVALID") from error
    if type(value) is not dict or type(value.get("request")) is not dict:
        _fail("M5_JOB_PROVENANCE_JSON_INVALID")
    return value


def _source_from_verified_product(product, data: Path) -> M5ArchiveSource:
    source, activation = product.index["source"], product.index["activation"]
    bars_path = _safe_path(data / "native", source["bars_relative_path"])
    provenance_path = _safe_path(data / "native", source["provenance_relative_path"])
    provenance = _checked_provenance(provenance_path, source["provenance_sha256"], source["provenance_bytes"])
    instrument = product.instrument_snapshot
    if not isinstance(instrument, Mapping) or not _positive_finite(instrument.get("point")):
        _fail("M5_JOB_INSTRUMENT_SNAPSHOT_INVALID")
    symbol = product.index["symbol"]
    identity = SourceIdentity(symbol, "M5", activation["activation_id"], activation["manifest_sha256"],
        source["bars_sha256"], source["audit_sidecar_sha256"], source["provenance_sha256"],
        _digest(provenance["request"]), instrument["point"])
    acquired = Interval(source["request_start_utc"], source["request_end_exclusive_utc"])
    geometry = aligned_prefix_geometry("M5", acquired, product.index["cutoff_utc"])
    metadata = {"source": source, "activation": activation, "identity": asdict(identity)}
    return M5ArchiveSource(symbol, identity, source["source_identity_sha256"], acquired,
        geometry.interval, instrument_session_class(symbol, instrument), source["bars_rows"],
        source["bars_bytes"], source["provenance_bytes"], _digest(metadata), str(bars_path), str(provenance_path))


def source_identity_from_m5_product(product: native_prefix.NativePrefixProduct,
                                    context: spool.GenerationSpoolContext, *,
                                    canonical_data_dir: Path) -> SourceIdentity:
    """Original same-generation reverify plus exact authenticated provenance map.

    A live cell owner already exposes this identity; reuse that fact in its cost
    resolver instead of invoking this standalone boundary again for each tier.
    """
    _, products, data = _reverified_products(context, (product,), canonical_data_dir)
    return _source_from_verified_product(products[0], data).source_identity


def _route(product) -> dict:
    return {"cutoff_utc": product.index["cutoff_utc"],
        "source_accessed_at_utc": product.index["source_accessed_at_utc"],
        "index_sha256": product.index_sha256,
        "source_identity_sha256": product.index["source"]["source_identity_sha256"],
        "activation_id": product.index["activation"]["activation_id"],
        "activation_manifest_sha256": product.index["activation"]["manifest_sha256"],
        "activation_entry_sha256": product.index["activation"]["entry_sha256"]}


class M5JobViews:
    """One frozen process's declared CELL unit, including saved-slot/account reads.

    Finite value/cutoff plans are frozen before canonical M5 values are read.
    Actual consumers bind leases when they exist, never as future placeholder
    owners. Native prefix/fitted capabilities are never synthesized or shared.
    """
    def __init__(self, context: spool.GenerationSpoolContext, *, canonical_data_dir: Path,
                 binding: DerivedJobBinding, fine_products: tuple[native_prefix.NativePrefixProduct, ...],
                 requests: tuple[ViewRequest, ...], budgets: ViewBudgets) -> None:
        self._state = _JobState(binding, requests, budgets=budgets)
        reopened, products, data = _reverified_products(context, fine_products, canonical_data_dir)
        if (reopened.context_sha256 != binding.generation_context_sha256
                or reopened.config_sha256 != binding.config_sha256):
            _fail("M5_JOB_GENERATION_BINDING_MISMATCH")
        self._context, self._data_dir = reopened, data
        self._products, self._routes = {}, {}
        for product in products:
            key = _utc(product.index["cutoff_utc"]).isoformat(timespec="seconds")
            if key in self._products and product.index_sha256 != self._products[key].index_sha256:
                _fail("M5_JOB_FINE_PRODUCT_CONFLICT")
            self._products[key], self._routes[key] = product, _route(product)
        maximum = max(self._products, key=_utc)
        self._source = _source_from_verified_product(self._products[maximum], data)
        first = self._products[maximum]
        for product in self._products.values():
            if (_digest(product.index["source"]) != _digest(first.index["source"])
                    or _digest(product.index["activation"]) != _digest(first.index["activation"])):
                _fail("M5_JOB_BASE_SOURCE_MISMATCH")
        spans = []
        for request in requests:
            self.product_at(request.cutoff_utc)
            if not _same_source(request.source, self._source.source_identity):
                _fail("M5_VIEW_SOURCE_BINDING_INVALID")
            if type(request.consumer) in (AccountViewBinding, NoTradeAccountViewBinding):
                self.product_at(request.consumer.measurement_end_utc)
            spans.append(request.interval)
        self._spans = _merge(tuple(spans))
        self._hull = Interval(self._spans[0].start_utc, self._spans[-1].end_exclusive_utc)
        lower, upper = _bounds(self._hull)
        source_lower, source_upper = _bounds(self._source.prefix_interval)
        if lower < source_lower or upper > source_upper:
            _fail("M5_JOB_VALUE_RANGE_OUTSIDE_PRODUCTS")
        self._base_rows, self._examined, self._observation = None, (), None
        self._state.spool_root = reopened.root

    @property
    def binding(self) -> DerivedJobBinding:
        return self._state.binding

    @property
    def job_binding_sha256(self) -> str:
        return self._state.binding_sha256

    @property
    def view_plan_sha256(self) -> str:
        return self._state.plan_sha256

    @property
    def consumer_binding_sha256(self) -> tuple[str, ...]:
        return self._state.consumer_sha256

    def consumer_bindings_for(self, result_key: str) -> tuple[str, ...]:
        return self._state.consumer_bindings_for(result_key)

    @property
    def budgets(self) -> ViewBudgets:
        return self._state.budgets

    @property
    def source_identity(self) -> SourceIdentity:
        self._state._live()
        return self._source.source_identity

    @property
    def pending_view_ids(self) -> tuple[str, ...]:
        return tuple(key for key, status in self._state.status.items() if status == "PENDING")

    def product_at(self, cutoff_utc: str) -> native_prefix.NativePrefixProduct:
        self._state._live()
        key = _utc(cutoff_utc).isoformat(timespec="seconds")
        if key not in self._products:
            _fail("M5_JOB_CUTOFF_NOT_DECLARED")
        return self._products[key]

    def route_at(self, cutoff_utc: str) -> dict:
        self.product_at(cutoff_utc)
        return dict(self._routes[_utc(cutoff_utc).isoformat(timespec="seconds")])

    def _read_source(self) -> None:
        if self._base_rows is not None:
            return
        source = self._source
        before = _no_reparse(Path(source.bars_path))
        maximum = max(self._products, key=_utc)
        contract = _CsvReadContract(Path(source.bars_path), source.symbol, "M5",
            source.source_identity.bars_sha256, source.expected_bytes, source.expected_rows, maximum)
        state, retained = _CsvReadState(), []
        try:
            with closing(_read_csv_bars(contract, self._hull, before, state)) as stream:
                for row in stream:
                    if any(_bounds(span)[0] <= _utc(row.open_utc) < _bounds(span)[1] for span in self._spans):
                        self._state._retain_source_row("canonical_m5", row)
                        retained.append(row)
            if not state.authenticated:
                _fail("M5_JOB_CANONICAL_EOF_INCOMPLETE")
        except BaseException:
            self._state._discard_source("canonical_m5")
            raise
        self._base_rows, self._examined = tuple(retained), (self._hull,)
        self._observation = M5ReadObservation(source.source_identity_sha256, state.sha256,
            state.bytes_read, state.rows, source.source_identity.provenance_sha256,
            source.source_identity.request_sha256)

    def _load(self, request: ViewRequest) -> DerivedPrefixView:
        self._read_source()
        start, end = _bounds(request.interval)
        cutoff = _utc(request.cutoff_utc)
        rows = tuple(row for row in self._base_rows
            if start <= _utc(row.open_utc) < end and _utc(row.close_utc) <= cutoff)
        cached_rows, cached_bytes = self._state._cache_totals()
        return _build_view(self.binding, request, rows, source=self._source,
            examined_interval=self._hull, budgets=self.budgets,
            retained_cache_rows=cached_rows, retained_cache_bytes=cached_bytes)

    def acquire(self, view_id: str, *, consumer=None, result_key: str | None = None) -> DerivedViewLease:
        if type(consumer) in (AccountViewBinding, NoTradeAccountViewBinding):
            self.product_at(consumer.measurement_end_utc)
        return self._state.acquire(view_id, self._load, consumer=consumer, result_key=result_key)

    def view(self, lease: DerivedViewLease) -> DerivedPrefixView:
        return self._state.view(lease)

    def consumer_binding(self, lease: DerivedViewLease):
        return self._state.consumer_binding(lease)

    def frame(self, lease: DerivedViewLease):
        view = self._state.financial_view(lease)
        if not view.rows:
            _fail("M5_VIEW_EMPTY_FRAME")
        if len(view.rows) > native_prefix.MAX_OPEN_FRAME_ROWS:
            _fail("M5_VIEW_FRAME_ROW_LIMIT")
        import pandas as pd
        data = {"Open": [row.open for row in view.rows], "High": [row.high for row in view.rows],
            "Low": [row.low for row in view.rows], "Close": [row.close for row in view.rows],
            "Volume": [row.volume for row in view.rows], "RealVolume": [row.real_volume for row in view.rows],
            "Spread": [row.spread for row in view.rows],
            "_SOURCE_FIRST_TS": pd.to_datetime([span.first_open_utc for span in view.member_spans], utc=True),
            "_SOURCE_LAST_TS": pd.to_datetime([span.last_open_utc for span in view.member_spans], utc=True),
            "_BAR_CLOSE_TS": pd.to_datetime([row.close_utc for row in view.rows], utc=True)}
        frame = pd.DataFrame(data, index=pd.to_datetime([row.open_utc for row in view.rows], utc=True))
        frame.index.name = "time"
        if int(frame.memory_usage(index=True, deep=True).sum()) > native_prefix.MAX_OPEN_FRAME_BYTES:
            _fail("M5_VIEW_FRAME_BYTE_LIMIT")
        identity = self.source_identity
        frame.attrs[native_prefix.NATIVE_SOURCE_BINDING_ATTR] = {
            "symbol": identity.symbol, "timeframe": "M5", "activation_id": identity.activation_id,
            "activation_manifest_sha256": identity.activation_sha256, "bars_sha256": identity.bars_sha256,
            "sidecar_sha256": identity.sidecar_sha256, "source_kind": SOURCE_KIND,
            "view_timeframe": view.timeframe, "m5_view_contract_sha256": self.binding.m5_view_contract_sha256}
        return frame

    def coverage(self, source: SourceIdentity, interval: Interval) -> ViewCoverage:
        self._state._live()
        if not _same_source(source, self.source_identity):
            _fail("M5_VIEW_SOURCE_BINDING_INVALID")
        retained = self._spans if self._base_rows is not None else ()
        gaps, available = None, None
        if not _missing(interval, retained) and not _missing(interval, self._examined):
            start, end = _bounds(interval)
            rows = tuple(row for row in self._base_rows if start <= _utc(row.open_utc) < end)
            gaps = _observed_gaps(interval, rows)
            available = Interval(rows[0].open_utc, rows[-1].close_utc) if rows else None
        return ViewCoverage(interval, self._examined, retained, _missing(interval, self._examined),
            _missing(interval, retained), available, _missing(interval, (available,) if available else ()), gaps, ())

    def release_unused(self, view_id: str, reason: str) -> None:
        self._state.release_unused(view_id, reason)

    def _final_revalidate(self) -> tuple[M5ReadObservation, ...]:
        reopened, products, _ = _reverified_products(self._context, tuple(self._products.values()), self._data_dir)
        if reopened.context_sha256 != self.binding.generation_context_sha256:
            _fail("M5_JOB_GENERATION_CONTEXT_CHANGED")
        for product in products:
            key = _utc(product.index["cutoff_utc"]).isoformat(timespec="seconds")
            if _route(product) != self._routes[key]:
                _fail("M5_JOB_SOURCE_CHANGED")
        return (self._observation,) if self._observation is not None else ()

    def abort_before_consumption(self, reason: str) -> dict:
        """Close unused source work; caller owns dispatch/phase and STOP fencing."""
        if type(self) is not M5JobViews or type(getattr(self, "_state", None)) is not _JobState:
            _fail("M5_JOB_ABORT_ORIGINAL_OWNER_REQUIRED")
        state = self._state
        state._live()
        if not _text(reason) or len(reason) > 512:
            _fail("M5_JOB_ABORT_REASON_INVALID")
        if (not state.requests or set(state.status) != set(state.requests)
                or any(value != "PENDING" for value in state.status.values())
                or set(state.seen) != set(state.binding.required_result_keys)
                or any(state.seen.values()) or state.uses or state.records or state.active
                or state.cache or state.cache_usage or state.source_usage
                or self._base_rows is not None or self._observation is not None or self._examined):
            _fail("M5_JOB_ABORT_UNUSED_STATE_REQUIRED")
        try:
            observations = M5JobViews._final_revalidate(self)
            if type(observations) is not tuple or observations:
                _fail("M5_JOB_ABORT_SOURCE_OBSERVATIONS_INVALID")
            report = _bounded_reopen_plain({
                "schema_version": "fxpm.m5-abort-before-consumption.v1",
                "status": "ABORTED_SOURCE_BEFORE_CONSUMPTION", "reason": reason,
                "binding": asdict(state.binding), "budgets": asdict(state.budgets),
                "job_binding_sha256": state.binding_sha256, "view_plan_sha256": state.plan_sha256,
                "source_identity": asdict(self._source.source_identity),
                "product_routes": [dict(self._routes[key]) for key in sorted(self._routes, key=_utc)],
                "declared_view_ids": sorted(state.requests), "requests_all_pending": True,
                "active_leases_before_close": 0, "observed_consumer_bindings": [],
                "value_cache_empty_before_close": True, "prefix_routes_revalidated": True,
                "job_view_read_observations": [], "owner_closed": False,
                "source_complete": False, "job_complete": False, "retirement_authority": False,
                "caller_obligations_not_authenticated": [
                    "ORIGINAL_DISPATCH_LEASE_HELD", "FRESH_PRE_CONTROLLER_PHASE",
                    "ACTUAL_STOP_GRID_READBACK_AND_DISTINCT_RETIREMENT_FENCE"],
                "qualification_authority": False, "capital_authority": False,
                "live_authority": False, "order_authority": False,
                "full_native_context_eligibility": False, "financial_consumer_ready": False}, _encoding=True)
        finally:
            M5JobViews.close(self)
        if (not state.closed or state.active or state.cache or state.source_usage or state.cache_usage
                or state.requests or state.records or state.uses or self._products or self._routes
                or self._source is not None or self._context is not None
                or self._base_rows is not None or self._observation is not None or self._examined):
            _fail("M5_JOB_ABORT_CLOSE_INCOMPLETE")
        report["owner_closed"] = True
        return _bounded_reopen_plain(report)

    def finish(self, results, *, end_grid_rows) -> JobSourceSummary:
        try:
            return self._state.finish(results, end_grid_rows=end_grid_rows, revalidate=self._final_revalidate)
        finally:
            if self._state.closed:
                self.close()

    def reopen_metadata(self) -> dict:
        self._state._live()
        return _encode_m5_reopen_metadata(binding=self.binding, budgets=self.budgets,
            requests=tuple(self._state.requests.values()), product_cutoffs=tuple(self._products),
            source_identity=self.source_identity)

    def close(self) -> None:
        self._state.close()
        self._base_rows, self._observation = None, None
        self._products.clear()
        self._routes.clear()
        self._source, self._context = None, None
        self._spans, self._examined = (), ()

    def __enter__(self) -> M5JobViews:
        return self

    def __exit__(self, exc_type, exc, traceback) -> None:
        self.close()


M5_REOPEN_METADATA_SCHEMA = "fxpm.m5-job-view-reopen-metadata.v1"
MAX_REOPEN_METADATA_BYTES = 1024 * 1024
_MAX_REOPEN_ITEMS = 1024


def _bounded_reopen_plain(value, *, _encoding=False):
    stack, nodes = [(value, 0)], 0
    while stack:
        item, depth = stack.pop()
        nodes += 1
        if nodes > 100000 or depth > 16:
            _fail("M5_REOPEN_METADATA_LIMIT")
        if type(item) is dict:
            if any(type(key) is not str for key in item):
                _fail("M5_REOPEN_METADATA_INVALID")
            if (nodes + len(stack) + len(item) > 100000
                    or any(len(key) > MAX_REOPEN_METADATA_BYTES for key in item)):
                _fail("M5_REOPEN_METADATA_LIMIT")
            stack.extend((child, depth + 1) for child in item.values())
        elif type(item) is list or (_encoding and type(item) is tuple):
            if nodes + len(stack) + len(item) > 100000:
                _fail("M5_REOPEN_METADATA_LIMIT")
            stack.extend((child, depth + 1) for child in item)
        elif type(item) is str:
            if len(item) > MAX_REOPEN_METADATA_BYTES:
                _fail("M5_REOPEN_METADATA_LIMIT")
        elif item is None or type(item) in (int, bool):
            pass
        elif type(item) is float and math.isfinite(item):
            pass
        else:
            _fail("M5_REOPEN_METADATA_INVALID")
    try:
        chunks, size = [], 0
        encoder = json.JSONEncoder(sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False)
        for chunk in encoder.iterencode(value):
            raw = chunk.encode("ascii")
            size += len(raw)
            if size > MAX_REOPEN_METADATA_BYTES:
                _fail("M5_REOPEN_METADATA_LIMIT")
            chunks.append(raw)
        encoded = b"".join(chunks)
    except M5JobViewError:
        raise
    except (TypeError, ValueError, OverflowError, RecursionError) as error:
        raise M5JobViewError("M5_REOPEN_METADATA_INVALID") from error
    if len(encoded) > MAX_REOPEN_METADATA_BYTES:
        _fail("M5_REOPEN_METADATA_LIMIT")
    return json.loads(encoded)


def _reopen_fields(value, cls):
    if type(value) is not dict or set(value) != {field.name for field in fields(cls)}:
        _fail("M5_REOPEN_METADATA_FIELDS_INVALID")
    return dict(value)


def _encode_reopen_consumer(value):
    kinds = {SlotViewBinding: "slot", AccountViewBinding: "account", NoTradeAccountViewBinding: "no_trade_account", CalibrationViewBinding: "calibration"}
    if value is None:
        return None
    if type(value) not in kinds:
        _fail("M5_REOPEN_CONSUMER_INVALID")
    return {"kind": kinds[type(value)], "binding": asdict(value)}


def _decode_reopen_consumer(value):
    if value is None:
        return None
    kinds = {"slot": SlotViewBinding, "account": AccountViewBinding, "no_trade_account": NoTradeAccountViewBinding, "calibration": CalibrationViewBinding}
    if type(value) is not dict or set(value) != {"kind", "binding"} or type(value["kind"]) is not str:
        _fail("M5_REOPEN_CONSUMER_INVALID")
    cls = kinds.get(value["kind"])
    if cls is None:
        _fail("M5_REOPEN_CONSUMER_INVALID")
    return cls(**_reopen_fields(value["binding"], cls))


def _decode_reopen_request(value):
    data = _reopen_fields(value, ViewRequest)
    if (data["timeframe"] not in ("M15", "M30", "H1") or data["source_kind"] != SOURCE_KIND
            or data["clock_template"] is not None or data["clock_extent"] is not None):
        _fail("M5_REOPEN_CLOCK_NOT_ENABLED")
    data["source"] = SourceIdentity(**_reopen_fields(data["source"], SourceIdentity))
    data["interval"] = Interval(**_reopen_fields(data["interval"], Interval))
    data["stratum"] = Stratum(**_reopen_fields(data["stratum"], Stratum))
    data["consumer"] = _decode_reopen_consumer(data["consumer"])
    identification = data["spread_identification"]
    if identification is not None:
        identification = _reopen_fields(identification, SpreadIdentification)
        for key in ("control_source", "native_source"):
            identification[key] = SourceIdentity(**_reopen_fields(identification[key], SourceIdentity))
        identification["stratum"] = Stratum(**_reopen_fields(identification["stratum"], Stratum))
        identification["interval"] = Interval(**_reopen_fields(identification["interval"], Interval))
        mismatches, matches = identification["mismatch_counts"], identification["matching_candidates"]
        if (type(mismatches) is not list or any(type(row) is not list or len(row) != 2
                or type(row[0]) is not str or type(row[1]) is not int or row[1] < 0 for row in mismatches)
                or type(matches) is not list or any(type(row) is not str for row in matches)
                or type(identification["compared_bar_count"]) is not int):
            _fail("M5_REOPEN_SPREAD_IDENTIFICATION_INVALID")
        identification["mismatch_counts"] = tuple(tuple(row) for row in mismatches)
        identification["matching_candidates"] = tuple(matches)
        data["spread_identification"] = SpreadIdentification(**identification)
        from pm_explicit_derived_history import _checked_identification
        try:
            _checked_identification(data["spread_identification"])
        except Exception as error:
            raise M5JobViewError("M5_REOPEN_SPREAD_IDENTIFICATION_INVALID") from error
    return ViewRequest(**data)


def _encode_m5_reopen_metadata(*, binding, budgets, requests, product_cutoffs, source_identity):
    _binding(binding)
    _budgets(binding, budgets)
    if (type(requests) is not tuple or not 1 <= len(requests) <= _MAX_REOPEN_ITEMS
            or type(product_cutoffs) is not tuple or not 1 <= len(product_cutoffs) <= _MAX_REOPEN_ITEMS
            or type(source_identity) is not SourceIdentity):
        _fail("M5_REOPEN_METADATA_INVALID")
    encoded_requests = []
    for request in requests:
        if (type(request) is not ViewRequest or request.timeframe not in ("M15", "M30", "H1")
                or request.source_kind != SOURCE_KIND or request.clock_template is not None
                or request.clock_extent is not None):
            _fail("M5_REOPEN_CLOCK_NOT_ENABLED")
        _request(binding, request)
        if not _same_source(request.source, source_identity):
            _fail("M5_REOPEN_SOURCE_IDENTITY_MISMATCH")
        data = asdict(request)
        data["consumer"] = _encode_reopen_consumer(request.consumer)
        encoded_requests.append(data)
    if value_plan_sha256(requests) != binding.view_plan_sha256:
        _fail("M5_VIEW_PLAN_BINDING_MISMATCH")
    cutoffs = tuple(sorted({_utc(value).isoformat(timespec="seconds") for value in product_cutoffs}, key=_utc))
    if len(cutoffs) != len(product_cutoffs) or any(
            _utc(request.cutoff_utc).isoformat(timespec="seconds") not in cutoffs for request in requests):
        _fail("M5_REOPEN_PRODUCT_CUTOFFS_INVALID")
    return _bounded_reopen_plain({"schema_version": M5_REOPEN_METADATA_SCHEMA,
        "source_kind": SOURCE_KIND, "lifecycle_state": "UNSEEDED_DECLARED_PLAN_ONLY",
        "binding": asdict(binding), "budgets": asdict(budgets), "requests": encoded_requests,
        "product_cutoffs": list(cutoffs), "source_identity": asdict(source_identity),
        "job_binding_sha256": _binding(binding), "view_plan_sha256": value_plan_sha256(requests)}, _encoding=True)


def _decode_m5_reopen_metadata(metadata):
    value = _bounded_reopen_plain(metadata)
    expected = {"schema_version", "source_kind", "lifecycle_state", "binding", "budgets", "requests",
                "product_cutoffs", "source_identity", "job_binding_sha256", "view_plan_sha256"}
    if (type(value) is not dict or set(value) != expected or value["schema_version"] != M5_REOPEN_METADATA_SCHEMA
            or value["source_kind"] != SOURCE_KIND or value["lifecycle_state"] != "UNSEEDED_DECLARED_PLAN_ONLY"
            or type(value["requests"]) is not list or type(value["product_cutoffs"]) is not list):
        _fail("M5_REOPEN_METADATA_INVALID")
    data = _reopen_fields(value["binding"], DerivedJobBinding)
    if type(data["required_result_keys"]) is not list:
        _fail("M5_REOPEN_METADATA_FIELDS_INVALID")
    data["required_result_keys"] = tuple(data["required_result_keys"])
    binding = DerivedJobBinding(**data)
    budgets = ViewBudgets(**_reopen_fields(value["budgets"], ViewBudgets))
    requests = tuple(_decode_reopen_request(row) for row in value["requests"])
    source = SourceIdentity(**_reopen_fields(value["source_identity"], SourceIdentity))
    cutoffs = tuple(value["product_cutoffs"])
    encoded = _encode_m5_reopen_metadata(binding=binding, budgets=budgets, requests=requests,
        product_cutoffs=cutoffs, source_identity=source)
    if _digest(encoded) != _digest(value):
        _fail("M5_REOPEN_METADATA_BINDING_MISMATCH")
    return binding, budgets, requests, cutoffs, source


def reopen_m5_job_views(context, *, canonical_data_dir, fine_products, metadata):
    """Plain metadata supplies arguments; only original product verifiers authenticate inputs."""
    metadata = _bounded_reopen_plain(metadata)
    binding, budgets, requests, cutoffs, source = _decode_m5_reopen_metadata(metadata)
    if (type(context) is not spool.GenerationSpoolContext or type(fine_products) is not tuple
            or not fine_products or any(type(row) is not native_prefix.NativePrefixProduct for row in fine_products)):
        _fail("M5_JOB_ORIGINAL_PREFIX_PRODUCTS_REQUIRED")
    actual = tuple(sorted({_utc(row.index["cutoff_utc"]).isoformat(timespec="seconds") for row in fine_products}, key=_utc))
    if actual != cutoffs or len(fine_products) != len(cutoffs):
        _fail("M5_REOPEN_PRODUCT_CUTOFFS_INVALID")
    owner = M5JobViews(context, canonical_data_dir=canonical_data_dir, binding=binding,
        fine_products=fine_products, requests=requests, budgets=budgets)
    try:
        if not _same_source(owner.source_identity, source) or _digest(owner.reopen_metadata()) != _digest(metadata):
            _fail("M5_REOPEN_METADATA_BINDING_MISMATCH")
        return owner
    except Exception:
        owner.close()
        raise
