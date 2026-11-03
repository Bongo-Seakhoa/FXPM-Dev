"""Pure recipe005 aggregation of caller-authenticated M5 values.

This module performs no I/O and authenticates no native receipt. Its supplied
identities must come from the publisher's original authenticated preflight.
Derived metadata is never a native seal, profile approval or trading grant.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, replace
from datetime import datetime, timedelta, timezone
import hashlib
import json
import math
from typing import Any, Iterable


PROTECTED_CUTOFF = "2024-07-14T08:05:00+00:00"
RECIPE_SHA256 = "85e321bc794c59cf26e725bbb7772b2590a7b0256e2f6e6086ee3bf52189c1f4"
IMPLEMENTATION_VERSION = "fxpm.explicit-m5-derived-history.v2-available-data-only"
DATA_SCOPE = "AVAILABLE_DATA_ONLY_NOT_EXECUTION_READY"
SPREAD_CANDIDATES = ("first", "last", "min", "max")
_FIXED_SECONDS = {"M15": 900, "M30": 1800, "H1": 3600}
_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)
_PRICE_ONLY = "PRICE_SOURCE_ONLY_NOT_EXECUTION_READY"
_SPREAD_AVAILABLE = "DERIVED_SPREAD_AVAILABLE_PROFILE_UNVERIFIED"


@dataclass(frozen=True, slots=True)
class Interval:
    start_utc: str
    end_exclusive_utc: str


@dataclass(frozen=True, slots=True)
class Stratum:
    timeframe: str
    instrument_session_class: str
    alignment_identity: str


@dataclass(frozen=True, slots=True)
class SourceIdentity:
    symbol: str
    timeframe: str
    activation_id: str
    activation_sha256: str
    bars_sha256: str
    sidecar_sha256: str
    provenance_sha256: str
    request_sha256: str
    point: int | float
    spread_unit: str = "points"


@dataclass(frozen=True, slots=True)
class Bar:
    open_utc: str
    close_utc: str
    open: int | float
    high: int | float
    low: int | float
    close: int | float
    volume: int | float
    real_volume: int | float | None = None
    spread: int | float | None = None


@dataclass(frozen=True, slots=True)
class DerivedBar:
    value: Bar
    member_count: int
    membership_sha256: str
    spread_candidates: tuple[int | None, int | None, int | None, int | None]


@dataclass(frozen=True, slots=True)
class DerivedSeries:
    stratum: Stratum
    source: SourceIdentity
    interval: Interval
    cutoff_utc: str
    bars: tuple[DerivedBar, ...]
    source_member_count: int
    gap_count: int
    membership_sha256: str
    cost_status: str = _PRICE_ONLY
    selected_spread_rule: str | None = None
    spread_identification_sha256: str | None = None


@dataclass(frozen=True, slots=True)
class SpreadIdentification:
    stratum: Stratum
    control_source: SourceIdentity
    native_source: SourceIdentity
    interval: Interval
    compared_bar_count: int
    mismatch_counts: tuple[tuple[str, int], ...]
    matching_candidates: tuple[str, ...]
    selected_spread_rule: str | None
    cost_status: str
    reason_code: str
    derived_control_sha256: str
    native_control_sha256: str
    evidence_sha256: str


class DerivedHistoryError(ValueError):
    def __init__(self, code: str, *, stratum: Stratum | None = None,
                 details: tuple[Any, ...] = ()) -> None:
        self.code = code
        self.stratum = stratum
        self.details = details
        super().__init__(code, *details)


def _fail(code: str, stratum: Stratum | None, *details: Any) -> None:
    raise DerivedHistoryError(code, stratum=stratum, details=tuple(details))


def canonical_json_bytes(value: Any) -> bytes:
    return json.dumps(value, allow_nan=False, ensure_ascii=True,
                      sort_keys=True, separators=(",", ":")).encode("ascii")


def _digest(value: Any) -> str:
    return hashlib.sha256(canonical_json_bytes(value)).hexdigest()


def _is_digest(value: Any) -> bool:
    return (type(value) is str and len(value) == 64
            and all(character in "0123456789abcdef" for character in value))


def _text(value: Any) -> bool:
    return type(value) is str and bool(value.strip())


def _utc(value: Any, code: str, stratum: Stratum | None) -> datetime:
    if type(value) is not str or not value or value != value.strip():
        _fail(code, stratum)
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        _fail(code, stratum)
    if (parsed.tzinfo is None or parsed.utcoffset() != timedelta(0)
            or parsed.microsecond != 0):
        _fail(code, stratum)
    return parsed


def _seconds(value: datetime) -> int:
    delta = value - _EPOCH
    return delta.days * 86400 + delta.seconds


def _iso(seconds: int) -> str:
    return (_EPOCH + timedelta(seconds=seconds)).isoformat(timespec="seconds")


def _finite(value: Any) -> bool:
    if type(value) not in (int, float):
        return False
    try:
        return math.isfinite(value)
    except OverflowError:
        return False


def _integral(value: Any) -> bool:
    return (_finite(value) and value >= 0
            and (type(value) is int or value.is_integer()))


def _stratum(value: Stratum) -> None:
    if (type(value) is not Stratum or type(value.timeframe) is not str
            or value.timeframe not in
            {"M15", "M30", "H1", "H4", "D1"}
            or not _text(value.instrument_session_class)
            or not _text(value.alignment_identity)):
        _fail("STRATUM_INVALID", value if type(value) is Stratum else None)


def _source(value: SourceIdentity, stratum: Stratum) -> None:
    if (type(value) is not SourceIdentity or not _text(value.symbol)
            or not _text(value.timeframe) or not _text(value.activation_id)
            or not _text(value.spread_unit)
            or not _finite(value.point) or value.point <= 0):
        _fail("SOURCE_IDENTITY_INVALID", stratum)
    for field in ("activation_sha256", "bars_sha256", "sidecar_sha256",
                  "provenance_sha256", "request_sha256"):
        if not _is_digest(getattr(value, field)):
            _fail("SOURCE_IDENTITY_INVALID", stratum, field)


def _interval(value: Interval, stratum: Stratum) -> tuple[datetime, datetime]:
    if type(value) is not Interval:
        _fail("INTERVAL_INVALID", stratum)
    start = _utc(value.start_utc, "INTERVAL_INVALID", stratum)
    end = _utc(value.end_exclusive_utc, "INTERVAL_INVALID", stratum)
    if start >= end:
        _fail("INTERVAL_INVALID", stratum)
    return start, end


def _bar(value: Bar, stratum: Stratum, *, control: bool = False,
         duration: int | None = None) -> tuple[Bar, datetime, datetime]:
    code = "CONTROL_BAR_INVALID" if control else "SOURCE_BAR_INVALID"
    spread_code = "CONTROL_SPREAD_INVALID" if control else "SOURCE_SPREAD_INVALID"
    if type(value) is not Bar:
        _fail(code, stratum)
    opened = _utc(value.open_utc, code, stratum)
    closed = _utc(value.close_utc, code, stratum)
    if opened >= closed or (duration is not None
                            and closed - opened != timedelta(seconds=duration)):
        _fail(code, stratum)
    if not all(_finite(item) for item in
               (value.open, value.high, value.low, value.close)):
        _fail(code, stratum)
    if not (value.low <= min(value.open, value.close)
            <= max(value.open, value.close) <= value.high):
        _fail(code, stratum)
    if (not _integral(value.volume)
            or (value.real_volume is not None and not _integral(value.real_volume))):
        _fail(code, stratum)
    if value.spread is not None and not _integral(value.spread):
        _fail(spread_code, stratum)
    return replace(value, volume=int(value.volume),
                   real_volume=(int(value.real_volume)
                                if value.real_volume is not None else None),
                   spread=int(value.spread) if value.spread is not None else None), opened, closed


def _membership_root(source: SourceIdentity, interval: Interval,
                     bars: tuple[DerivedBar, ...]) -> str:
    return _digest({"source": asdict(source), "interval": asdict(interval),
                    "buckets": [{"open_utc": row.value.open_utc,
                                 "close_utc": row.value.close_utc,
                                 "member_count": row.member_count,
                                 "membership_sha256": row.membership_sha256}
                                for row in bars]})


def derive_bars(rows: Iterable[Bar], *, stratum: Stratum, source: SourceIdentity,
                interval: Interval, cutoff_utc: str = PROTECTED_CUTOFF) -> DerivedSeries:
    """Aggregate ordered observed members; never sort, fill or trim dependencies."""
    _stratum(stratum)
    _source(source, stratum)
    if source.timeframe != "M5":
        _fail("SOURCE_TIMEFRAME_INVALID", stratum)
    if stratum.timeframe not in _FIXED_SECONDS:
        _fail("UNSUPPORTED_ALIGNMENT_CONSTRUCTOR", stratum)
    period = _FIXED_SECONDS[stratum.timeframe]
    start, end = _interval(interval, stratum)
    cutoff = _utc(cutoff_utc, "PROTECTED_CUTOFF_MISMATCH", stratum)
    if cutoff > _utc(PROTECTED_CUTOFF, "PROTECTED_CUTOFF_MISMATCH", stratum):
        _fail("PROTECTED_CUTOFF_MISMATCH", stratum)
    if end > cutoff:
        _fail("SOURCE_CUTOFF_VIOLATION", stratum)
    if _seconds(start) % period or _seconds(end) % period:
        _fail("UNSUPPORTED_INTERVAL_COVERAGE", stratum)

    output: list[DerivedBar] = []
    members: list[tuple[int, Bar]] = []
    bucket: int | None = None
    previous_open: datetime | None = None
    previous_close: datetime | None = None
    source_count = gap_count = 0
    real_volume_present: bool | None = None

    def emit() -> None:
        if not members or bucket is None:
            return
        values = [row for _, row in members]
        spreads = [row.spread for row in values]
        candidates = ((spreads[0], spreads[-1], min(spreads), max(spreads))
                      if all(value is not None for value in spreads)
                      else (None, None, None, None))
        value = Bar(_iso(bucket), _iso(bucket + period), values[0].open,
                    max(row.high for row in values), min(row.low for row in values),
                    values[-1].close, sum(row.volume for row in values),
                    (sum(row.real_volume for row in values)
                     if real_volume_present else None), None)
        digest = _digest({"source_bars_sha256": source.bars_sha256,
                          "members": [{"ordinal": ordinal, "value": asdict(row)}
                                      for ordinal, row in members]})
        output.append(DerivedBar(value, len(members), digest, candidates))

    for ordinal, item in enumerate(rows):
        value, opened, closed = _bar(item, stratum, duration=300)
        if _seconds(opened) % 300:
            _fail("SOURCE_BAR_INVALID", stratum, ordinal)
        if previous_open is not None:
            if opened == previous_open:
                _fail("SOURCE_DUPLICATE_TIMESTAMP", stratum, ordinal)
            if opened < previous_open:
                _fail("SOURCE_ORDER_INVALID", stratum, ordinal)
        if opened < start or opened >= end or closed > end:
            _fail("SOURCE_ROW_OUTSIDE_INTERVAL", stratum, ordinal)
        if closed > cutoff:
            _fail("SOURCE_CUTOFF_VIOLATION", stratum, ordinal)
        present = value.real_volume is not None
        if real_volume_present is None:
            real_volume_present = present
        elif present != real_volume_present:
            _fail("SOURCE_REAL_VOLUME_SCHEMA_MIXED", stratum, ordinal)
        if previous_close is not None and opened > previous_close:
            gap_count += _seconds(opened) // 300 - _seconds(previous_close) // 300
        next_bucket = (_seconds(opened) // period) * period
        if closed > _EPOCH + timedelta(seconds=next_bucket + period):
            _fail("UNSUPPORTED_INTERVAL_COVERAGE", stratum, ordinal)
        if bucket != next_bucket:
            emit()
            members = []
            bucket = next_bucket
        members.append((ordinal, value))
        previous_open, previous_close = opened, closed
        source_count += 1
    emit()
    bars = tuple(output)
    return DerivedSeries(stratum, source, interval, cutoff_utc, bars, source_count,
                         gap_count, _membership_root(source, interval, bars))


def _checked_series(series: DerivedSeries) -> None:
    if type(series) is not DerivedSeries:
        _fail("DERIVED_SERIES_INVALID", None)
    _stratum(series.stratum)
    _source(series.source, series.stratum)
    start, end = _interval(series.interval, series.stratum)
    if series.stratum.timeframe not in _FIXED_SECONDS:
        _fail("UNSUPPORTED_ALIGNMENT_CONSTRUCTOR", series.stratum)
    period = _FIXED_SECONDS[series.stratum.timeframe]
    cutoff = _utc(series.cutoff_utc, "DERIVED_SERIES_INVALID", series.stratum)
    if (end > cutoff or cutoff > _utc(PROTECTED_CUTOFF, "DERIVED_SERIES_INVALID", series.stratum)
            or _seconds(start) % period or _seconds(end) % period):
        _fail("DERIVED_SERIES_INVALID", series.stratum)
    if (series.source.timeframe != "M5" or type(series.bars) is not tuple
            or type(series.source_member_count) is not int
            or type(series.gap_count) is not int or series.gap_count < 0):
        _fail("DERIVED_SERIES_INVALID", series.stratum)
    total = 0
    previous: datetime | None = None
    for row in series.bars:
        if (type(row) is not DerivedBar or type(row.value) is not Bar
                or type(row.member_count) is not int
                or not 0 < row.member_count <= period // 300
                or not _is_digest(row.membership_sha256)
                or type(row.spread_candidates) is not tuple
                or len(row.spread_candidates) != 4):
            _fail("DERIVED_SERIES_INVALID", series.stratum)
        try:
            _bar(replace(row.value, spread=None), series.stratum, duration=period)
        except DerivedHistoryError:
            _fail("DERIVED_SERIES_INVALID", series.stratum)
        if (type(row.value.volume) is not int
                or (row.value.real_volume is not None
                    and type(row.value.real_volume) is not int)
                or not (all(value is None for value in row.spread_candidates)
                        or all(_integral(value) for value in row.spread_candidates))):
            _fail("DERIVED_SERIES_INVALID", series.stratum)
        opened = _utc(row.value.open_utc, "DERIVED_SERIES_INVALID", series.stratum)
        closed = _utc(row.value.close_utc, "DERIVED_SERIES_INVALID", series.stratum)
        if (opened < start or closed > end or _seconds(opened) % period
                or (previous is not None and opened < previous)):
            _fail("DERIVED_SERIES_INVALID", series.stratum)
        previous = closed
        total += row.member_count
    if (total != series.source_member_count
            or series.membership_sha256 != _membership_root(
                series.source, series.interval, series.bars)):
        _fail("DERIVED_SERIES_INVALID", series.stratum)


def _price_record(value: Bar, stratum: Stratum) -> dict[str, Any]:
    return {"open_utc": _iso(_seconds(_utc(value.open_utc, "CONTROL_BAR_INVALID", stratum))),
            "close_utc": _iso(_seconds(_utc(value.close_utc, "CONTROL_BAR_INVALID", stratum))),
            "open": value.open, "high": value.high, "low": value.low,
            "close": value.close, "volume": value.volume,
            "real_volume": value.real_volume}


def _identification_body(value: SpreadIdentification) -> dict[str, Any]:
    body = asdict(value)
    del body["evidence_sha256"]
    return body


def _checked_identification(value: SpreadIdentification) -> None:
    if type(value) is not SpreadIdentification:
        _fail("SPREAD_IDENTIFICATION_INVALID", None)
    stratum = value.stratum
    _stratum(stratum)
    _source(value.control_source, stratum)
    _source(value.native_source, stratum)
    _interval(value.interval, stratum)
    if (value.control_source.timeframe != "M5"
            or value.native_source.timeframe != stratum.timeframe
            or value.control_source.symbol != value.native_source.symbol
            or value.control_source.point != value.native_source.point
            or value.control_source.spread_unit != "points"
            or value.native_source.spread_unit != "points"
            or type(value.compared_bar_count) is not int or value.compared_bar_count <= 0
            or type(value.mismatch_counts) is not tuple or len(value.mismatch_counts) != 4
            or type(value.matching_candidates) is not tuple
            or not _is_digest(value.derived_control_sha256)
            or not _is_digest(value.native_control_sha256)):
        _fail("SPREAD_IDENTIFICATION_INVALID", stratum)
    for name, pair in zip(SPREAD_CANDIDATES, value.mismatch_counts, strict=True):
        if (type(pair) is not tuple or len(pair) != 2 or pair[0] != name
                or type(pair[1]) is not int or not 0 <= pair[1] <= value.compared_bar_count):
            _fail("SPREAD_IDENTIFICATION_INVALID", stratum)
    matches = tuple(name for name, count in value.mismatch_counts if count == 0)
    if value.matching_candidates != matches:
        _fail("SPREAD_IDENTIFICATION_INVALID", stratum)
    selected = matches[0] if len(matches) == 1 else None
    if (value.selected_spread_rule != selected
            or value.cost_status != ("SOURCE_RULE_MATCHED" if selected else "COST_UNAVAILABLE")
            or (selected is not None and value.reason_code != "UNIQUE_CONTROL_MATCH")
            or not _is_digest(value.evidence_sha256)
            or value.evidence_sha256 != _digest(_identification_body(value))):
        _fail("SPREAD_IDENTIFICATION_INVALID", stratum)


def identify_spread_rule(derived_control: DerivedSeries,
                         native_control_bars: Iterable[Bar], *,
                         native_source: SourceIdentity) -> SpreadIdentification:
    """Require whole-interval price fidelity, then exactly one fixed selector."""
    _checked_series(derived_control)
    stratum = derived_control.stratum
    _source(native_source, stratum)
    if (native_source.symbol != derived_control.source.symbol
            or native_source.timeframe != stratum.timeframe):
        _fail("CONTROL_IDENTITY_MISMATCH", stratum)
    if (native_source.point != derived_control.source.point
            or native_source.spread_unit != "points"
            or derived_control.source.spread_unit != "points"):
        _fail("CONTROL_UNITS_MISMATCH", stratum)
    native = tuple(_bar(row, stratum, control=True,
                        duration=_FIXED_SECONDS.get(stratum.timeframe))[0]
                   for row in native_control_bars)
    if not native or len(native) != len(derived_control.bars):
        _fail("CONTROL_PRICE_FIDELITY_MISMATCH", stratum)
    for row, expected in zip(derived_control.bars, native, strict=True):
        if canonical_json_bytes(_price_record(row.value, stratum)) != canonical_json_bytes(
                _price_record(expected, stratum)):
            _fail("CONTROL_PRICE_FIDELITY_MISMATCH", stratum)

    mismatches = [0, 0, 0, 0]
    missing = False
    for row, expected in zip(derived_control.bars, native, strict=True):
        for index, observed in enumerate(row.spread_candidates):
            if observed is not None and not _integral(observed):
                _fail("SOURCE_SPREAD_INVALID", stratum)
            if observed is None or expected.spread is None:
                missing = True
                mismatches[index] += 1
            elif observed != expected.spread:
                mismatches[index] += 1
    matches = (() if missing else tuple(name for name, count in
                                       zip(SPREAD_CANDIDATES, mismatches, strict=True)
                                       if count == 0))
    selected = matches[0] if len(matches) == 1 else None
    reason = ("MISSING_CONTROL_SPREAD" if missing else "UNIQUE_CONTROL_MATCH"
              if selected is not None else "AMBIGUOUS_CONTROL_MATCH"
              if len(matches) > 1 else "NO_CONTROL_MATCH")
    result = SpreadIdentification(
        stratum, derived_control.source, native_source, derived_control.interval,
        len(native), tuple(zip(SPREAD_CANDIDATES, mismatches, strict=True)), matches,
        selected, "SOURCE_RULE_MATCHED" if selected else "COST_UNAVAILABLE", reason,
        _digest(asdict(derived_control)), _digest([asdict(row) for row in native]), "")
    return replace(result, evidence_sha256=_digest(_identification_body(result)))


def apply_spread_rule(target: DerivedSeries,
                      identification: SpreadIdentification) -> DerivedSeries:
    """Apply only the control-matched selector in its exact declared stratum."""
    _checked_series(target)
    if (type(identification) is not SpreadIdentification
            or identification.stratum != target.stratum):
        _fail("STRATUM_MISMATCH", target.stratum)
    _checked_identification(identification)
    if identification.selected_spread_rule is None:
        cleared = tuple(replace(row, value=replace(row.value, spread=None))
                        for row in target.bars)
        return replace(target, bars=cleared, cost_status="COST_UNAVAILABLE",
                       selected_spread_rule=None,
                       spread_identification_sha256=identification.evidence_sha256)
    rule = identification.selected_spread_rule
    if (identification.cost_status != "SOURCE_RULE_MATCHED"
            or rule not in SPREAD_CANDIDATES
            or identification.matching_candidates != (rule,)
            or target.source.spread_unit != "points"):
        _fail("SPREAD_IDENTIFICATION_INVALID", target.stratum)
    index = SPREAD_CANDIDATES.index(rule)
    values = []
    complete = bool(target.bars)
    for row in target.bars:
        spread = row.spread_candidates[index]
        if spread is not None and not _integral(spread):
            _fail("SOURCE_SPREAD_INVALID", target.stratum)
        complete = complete and spread is not None
        values.append(replace(row, value=replace(row.value, spread=spread)))
    return replace(target, bars=tuple(values), cost_status=(
        _SPREAD_AVAILABLE if complete else "COST_UNAVAILABLE"),
        selected_spread_rule=rule,
        spread_identification_sha256=identification.evidence_sha256)


def require_spread_ready(series: DerivedSeries) -> None:
    """Check every derived Spread; profile/consumer approval remains separate."""
    _checked_series(series)
    if (series.cost_status != _SPREAD_AVAILABLE
            or series.selected_spread_rule not in SPREAD_CANDIDATES
            or not _is_digest(series.spread_identification_sha256)
            or series.source.spread_unit != "points" or not series.bars):
        _fail("DERIVED_SPREAD_NOT_READY", series.stratum)
    index = SPREAD_CANDIDATES.index(series.selected_spread_rule)
    for row in series.bars:
        if (not _integral(row.value.spread)
                or row.value.spread != row.spread_candidates[index]):
            _fail("DERIVED_SPREAD_NOT_READY", series.stratum)


def build_lineage(series: DerivedSeries, *, output_sha256: str,
                  implementation_sha256: str,
                  derived_profile_sha256: str | None = None) -> bytes:
    """Return canonical immutable derived metadata, never a native attestation."""
    _checked_series(series)
    if series.cost_status == _SPREAD_AVAILABLE:
        require_spread_ready(series)
    if (not _is_digest(output_sha256) or not _is_digest(implementation_sha256)
            or (derived_profile_sha256 is not None
                and not _is_digest(derived_profile_sha256))):
        _fail("LINEAGE_IDENTITY_INVALID", series.stratum)
    body = {"schema_version": "fxpm.explicit-m5-derived-lineage.v1",
            "source_kind": "m5_derived", "recipe_sha256": RECIPE_SHA256,
            "implementation_version": IMPLEMENTATION_VERSION,
            "implementation_sha256": implementation_sha256,
            "source": asdict(series.source), "stratum": asdict(series.stratum),
            "request_digest_semantics": "CANONICAL_REQUEST_MAPPING_DIGEST_NOT_A_NATIVE_CAPABILITY",
            "available_interval": asdict(series.interval),
            "data_scope": DATA_SCOPE, "readiness_label": DATA_SCOPE,
            "full_native_context_eligibility": False,
            "financial_consumer_ready": False,
            "cutoff_utc": series.cutoff_utc,
            "source_member_count": series.source_member_count,
            "observed_internal_gap_count": series.gap_count,
            "completeness": "INHERITS_AUTHENTICATED_M5_NO_CLOSURE_OR_NO_TICK_INFERENCE",
            "membership_sha256": series.membership_sha256,
            "output_sha256": output_sha256,
            "fine_spread_carrier_sha256": series.source.bars_sha256,
            "derived_profile_sha256": derived_profile_sha256,
            "cost_status": series.cost_status,
            "selected_spread_rule": series.selected_spread_rule,
            "spread_identification_sha256": series.spread_identification_sha256,
            "source_authentication_performed_by_this_engine": False,
            "qualification_authority": False, "capital_authority": False,
            "live_authority": False, "order_authority": False}
    return canonical_json_bytes({**body, "lineage_sha256": _digest(body)})


def stratum_stop_record(error: DerivedHistoryError) -> bytes:
    if type(error) is not DerivedHistoryError:
        raise TypeError("DerivedHistoryError required")
    return canonical_json_bytes({"schema_version": "fxpm.explicit-derived-stratum-stop.v1",
                                 "status": "STOP", "reason_code": error.code,
                                 "stratum": asdict(error.stratum) if error.stratum else None,
                                 "data_scope": DATA_SCOPE, "readiness_label": DATA_SCOPE,
                                 "full_native_context_eligibility": False,
                                 "financial_consumer_ready": False,
                                 "qualification_authority": False,
                                 "capital_authority": False,
                                 "live_authority": False, "order_authority": False})
