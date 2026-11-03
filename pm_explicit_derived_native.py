"""Read-only native inputs for one explicitly frozen derived-source plan.

Native authenticity remains owned by the original receipt-backed verifier.
This adapter neither creates native capabilities nor audits the native grid.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
import csv
import hashlib
import io
import json
import math
import os
from pathlib import Path
import stat
from typing import Any, Iterable, Mapping

from pm_explicit_derived_history import (
    Bar, Interval, PROTECTED_CUTOFF, SourceIdentity, Stratum, canonical_json_bytes,
)


_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)
_PERIODS = {"M5": 300, "M15": 900, "M30": 1800, "H1": 3600}
_MAX_METADATA_BYTES = 64 * 1024 * 1024
_REPARSE_POINT = 0x400
_DATA_ONLY = "AVAILABLE_DATA_ONLY_NOT_EXECUTION_READY"
_INTEGRITY_CODES = frozenset({
    "NATIVE_PATH_UNSAFE", "NATIVE_PATH_REPARSE_POINT", "NATIVE_INPUT_CHANGED",
    "NATIVE_METADATA_CHANGED_DURING_READ", "NATIVE_METADATA_IDENTITY_MISMATCH",
    "NATIVE_RECEIPT_AUTHORITY_IDENTITY_MISMATCH", "SOURCE_METADATA_PATH_MISMATCH",
    "SOURCE_SIZE_MISMATCH", "SOURCE_CHANGED_BEFORE_SCAN", "SOURCE_CHANGED_DURING_SCAN",
    "SOURCE_DIGEST_ROWS_OR_BYTES_MISMATCH", "SOURCE_STREAM_AUTHENTICATION_INCOMPLETE",
    "SOURCE_CHANGED_DURING_CALIBRATION_READ", "CALIBRATION_SOURCE_AUTHENTICATION_FAILED",
    "SOURCE_CELL_NOT_OWNED", "SOURCE_AUTHENTICATION_FAILED",
    "SOURCE_METADATA_AUTHENTICATION_FAILED", "SOURCE_CSV_PARSING_UNAUTHENTICATED",
    "PLANNING_IO_ABORT",
})


def _bounded_message(value: Any, maximum: int = 512) -> str:
    return str(value).encode("utf-8", errors="replace")[:maximum].decode("utf-8", errors="ignore")


def exception_details(error: BaseException) -> dict[str, str]:
    result = {"error_type": _bounded_message(type(error).__name__, 128),
              "error_message": _bounded_message(error)}
    if error.__cause__ is not None:
        result.update(cause_type=_bounded_message(type(error.__cause__).__name__, 128),
                      cause_message=_bounded_message(error.__cause__))
    return result


class NativeInputsError(ValueError):
    def __init__(self, code: str, *, symbol: str | None = None,
                 timeframe: str | None = None, detail: str | None = None,
                 abort_attempt: bool | None = None) -> None:
        self.code, self.symbol, self.timeframe = code, symbol, timeframe
        self.detail = _bounded_message(detail) if detail is not None else None
        self.abort_attempt = code in _INTEGRITY_CODES if abort_attempt is None else bool(abort_attempt)
        super().__init__(code, symbol, timeframe, self.detail)


def _fail(code: str, symbol: str | None = None, timeframe: str | None = None,
          detail: str | None = None, *, abort_attempt: bool | None = None) -> None:
    raise NativeInputsError(code, symbol=symbol, timeframe=timeframe, detail=detail,
                            abort_attempt=abort_attempt)


def _sha(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _digest(value: Any) -> str:
    return _sha(canonical_json_bytes(value))


def _is_digest(value: Any) -> bool:
    return (type(value) is str and len(value) == 64
            and all(character in "0123456789abcdef" for character in value))


def _utc(value: Any) -> datetime:
    if type(value) is not str or not value or value != value.strip():
        _fail("UTC_AS_RECORDED_REQUIRED")
    try:
        result = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        _fail("UTC_AS_RECORDED_REQUIRED")
    if (result.tzinfo is None or result.utcoffset() != timedelta(0)
            or result.microsecond):
        _fail("UTC_AS_RECORDED_REQUIRED")
    return result


def _seconds(value: datetime) -> int:
    delta = value - _EPOCH
    return delta.days * 86400 + delta.seconds


def _iso(value: int) -> str:
    return (_EPOCH + timedelta(seconds=value)).isoformat(timespec="seconds")


def _bounds(value: Interval) -> tuple[datetime, datetime]:
    if type(value) is not Interval:
        _fail("REQUEST_INTERVAL_INVALID")
    start, end = _utc(value.start_utc), _utc(value.end_exclusive_utc)
    if start >= end:
        _fail("REQUEST_INTERVAL_INVALID")
    return start, end


@dataclass(frozen=True, slots=True)
class ExcludedEdges:
    leading: Interval | None
    trailing: Interval | None


@dataclass(frozen=True, slots=True)
class PrefixGeometry:
    acquisition_interval: Interval
    source_prefix_interval: Interval
    interval: Interval
    excluded_partial_edges: ExcludedEdges

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def aligned_prefix_geometry(timeframe: str, acquisition_interval: Interval,
                            protected_cutoff_utc: str) -> PrefixGeometry:
    """Keep every eligible whole fixed-grid target bar before the cutoff."""
    if type(timeframe) is not str or timeframe not in _PERIODS:
        _fail("UNSUPPORTED_ALIGNMENT_CONSTRUCTOR", timeframe=timeframe)
    start, end = _bounds(acquisition_interval)
    cutoff = _utc(protected_cutoff_utc)
    if cutoff > _utc(PROTECTED_CUTOFF):
        _fail("PROTECTED_CUTOFF_MISMATCH", timeframe=timeframe)
    if end < cutoff or start >= cutoff:
        _fail("SOURCE_PREFIX_COVERAGE_UNSUPPORTED", timeframe=timeframe)
    period = _PERIODS[timeframe]
    lower = -(-_seconds(start) // period) * period
    upper = (_seconds(cutoff) // period) * period
    if lower >= upper:
        _fail("EMPTY_ELIGIBLE_PREFIX", timeframe=timeframe)
    aligned = Interval(_iso(lower), _iso(upper))
    prefix = Interval(acquisition_interval.start_utc, protected_cutoff_utc)
    edges = ExcludedEdges(
        Interval(prefix.start_utc, aligned.start_utc) if start < _EPOCH + timedelta(seconds=lower) else None,
        Interval(aligned.end_exclusive_utc, prefix.end_exclusive_utc)
        if _EPOCH + timedelta(seconds=upper) < cutoff else None)
    return PrefixGeometry(acquisition_interval, prefix, aligned, edges)


def require_m5_coverage(m5_interval: Interval, target_interval: Interval) -> None:
    first, last = _bounds(m5_interval)
    start, end = _bounds(target_interval)
    if first > start or last < end:
        _fail("M5_REQUIRED_INTERVAL_COVERAGE_UNSUPPORTED")


def available_history_extent(timeframe: str, required_interval: Interval,
                             m5_acquisition_interval: Interval,
                             m5_observed_interval: Interval, *,
                             other_observed_interval: Interval | None = None) -> dict[str, Any]:
    """Freeze whole-bar available data and every omitted required prefix/suffix."""
    if type(timeframe) is not str or timeframe not in {"M15", "M30", "H1"}:
        _fail("UNSUPPORTED_ALIGNMENT_CONSTRUCTOR", timeframe=timeframe)
    required_start, required_end = _bounds(required_interval)
    acquisition_start, acquisition_end = _bounds(m5_acquisition_interval)
    observed_start, observed_end = _bounds(m5_observed_interval)
    period = _PERIODS[timeframe]
    if _seconds(required_start) % period or _seconds(required_end) % period:
        _fail("REQUIRED_WHOLE_BAR_INTERVAL_INVALID", timeframe=timeframe)
    first = max(required_start, acquisition_start, observed_start)
    last = min(required_end, acquisition_end, observed_end)
    if other_observed_interval is not None:
        other_start, other_end = _bounds(other_observed_interval)
        first, last = max(first, other_start), min(last, other_end)
    lower = -(-_seconds(first) // period) * period
    upper = (_seconds(last) // period) * period
    if lower >= upper:
        _fail("NO_AVAILABLE_WHOLE_BARS", timeframe=timeframe)
    available = Interval(_iso(lower), _iso(upper))
    omitted = []
    if required_start < _EPOCH + timedelta(seconds=lower):
        omitted.append(asdict(Interval(required_interval.start_utc, available.start_utc)))
    if _EPOCH + timedelta(seconds=upper) < required_end:
        omitted.append(asdict(Interval(available.end_exclusive_utc, required_interval.end_exclusive_utc)))
    return {"required_interval": asdict(required_interval),
            "m5_acquisition_interval": asdict(m5_acquisition_interval),
            "m5_observed_interval": asdict(m5_observed_interval),
            "available_interval": asdict(available), "omitted_intervals": omitted}


def _observed_interval(entry: Mapping[str, Any], timeframe: str,
                       expected_rows: int) -> Interval:
    """Bind only authenticated observation stamps, without inferring gap causes."""
    try:
        evidence = entry["evidence_metadata"]
        observed = evidence["observed_interval"]
        first = _utc(observed["start_open_utc"])
        last = _utc(observed["end_open_utc"])
        if (type(observed["rows"]) is not int or observed["rows"] != expected_rows
                or first > last or timeframe not in _PERIODS):
            _fail("AUTHENTICATED_OBSERVED_BOUNDS_UNAVAILABLE")
        closed = last + timedelta(seconds=_PERIODS[timeframe])
        gaps = evidence.get("page_gap_diagnostics", {})
        for field, expected in (("actual_first_open_utc", first),
                                ("actual_last_open_utc", last),
                                ("actual_last_close_utc", closed)):
            if field in gaps and _utc(gaps[field]) != expected:
                _fail("AUTHENTICATED_OBSERVED_BOUNDS_UNAVAILABLE")
        return Interval(observed["start_open_utc"], _iso(_seconds(closed)))
    except (KeyError, TypeError) as error:
        raise NativeInputsError("AUTHENTICATED_OBSERVED_BOUNDS_UNAVAILABLE",
                                detail=_bounded_message(error)) from error


def instrument_session_class(symbol: str, instrument: Mapping[str, Any]) -> str:
    """Use authenticated classifications, never infer a calendar from a name."""
    if type(symbol) is not str or not symbol or not isinstance(instrument, Mapping):
        _fail("INSTRUMENT_SESSION_CLASS_UNAVAILABLE", symbol=symbol)
    asset = instrument.get("asset_class")
    asset = asset.strip() if type(asset) is str else ""
    catalog = instrument.get("path")
    parts = ([part for part in catalog.replace("\\", "/").split("/") if part]
             if type(catalog) is str else [])
    if parts and parts[-1] == symbol:
        parts.pop()
    parent = "/".join(parts)
    if not parent and (not asset or asset.lower() in {"unknown", "unavailable", "none"}):
        _fail("INSTRUMENT_SESSION_CLASS_UNAVAILABLE", symbol=symbol)
    return canonical_json_bytes({"asset_class": asset or None,
                                 "catalog_parent_path": parent or None,
                                 "calendar_proof": False}).decode("ascii")


@dataclass(frozen=True, slots=True)
class SourceCell:
    symbol: str
    timeframe: str
    native_status: str
    bars_path: str
    sidecar_path: str
    provenance_path: str
    source_identity: SourceIdentity
    interval: Interval
    stratum: Stratum
    geometry: PrefixGeometry
    expected_rows: int
    expected_bytes: int
    cutoff_utc: str
    metadata_digest: str
    observed_interval: Interval | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class TargetCell:
    symbol: str
    timeframe: str
    stratum: Stratum
    interval: Interval
    m5: SourceCell
    target_spec: dict[str, Any]
    extent: dict[str, Any]
    readiness_label: str = _DATA_ONLY
    data_scope: str = _DATA_ONLY
    full_native_context_eligibility: bool = False
    financial_consumer_ready: bool = False


@dataclass(frozen=True, slots=True)
class ControlPair:
    stratum: Stratum
    interval: Interval
    native: SourceCell
    m5: SourceCell
    extent: dict[str, Any]
    readiness_label: str = _DATA_ONLY
    data_scope: str = _DATA_ONLY
    full_native_context_eligibility: bool = False
    financial_consumer_ready: bool = False


@dataclass(frozen=True, slots=True)
class StopCell:
    symbol: str
    timeframe: str
    reason_code: str
    stratum: Stratum | None = None
    error_type: str | None = None
    error_message: str | None = None
    cause_type: str | None = None
    cause_message: str | None = None
    target_spec: dict[str, Any] | None = None
    extent: dict[str, Any] | None = None
    readiness_label: str = _DATA_ONLY
    data_scope: str = _DATA_ONLY
    full_native_context_eligibility: bool = False
    financial_consumer_ready: bool = False


@dataclass(frozen=True, slots=True)
class AuthorityBindings:
    activation_id: str
    activation_sha256: str
    pointer_sha256: str
    run_manifest_sha256: str
    semantic_receipt_sha256: str
    semantic_verifier_build_digest: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class InputPin:
    path: str
    sha256: str
    bytes: int
    stat_identity: tuple[int, ...]
    kind: str
    rows: int | None = None


@dataclass(frozen=True, slots=True)
class NativePlan:
    scope: str
    cutoff_utc: str
    targets: tuple[TargetCell, ...]
    controls: tuple[ControlPair, ...]
    stops: tuple[StopCell, ...]
    control_catalog_diagnostics: tuple[StopCell, ...]
    authority_bindings: AuthorityBindings
    metadata_read_pins: tuple[InputPin, ...]
    qualification_authority: bool = False
    capital_authority: bool = False
    live_authority: bool = False
    order_authority: bool = False
    schema_version: str = "fxpm.explicit-derived-plan.v2"
    readiness_label: str = _DATA_ONLY
    data_scope: str = _DATA_ONLY
    full_native_context_eligibility: bool = False
    financial_consumer_ready: bool = False

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def _stat_identity(value: os.stat_result) -> tuple[int, ...]:
    return (value.st_dev, value.st_ino, value.st_size, value.st_mtime_ns,
            value.st_ctime_ns)


def _common_identity(value: os.stat_result) -> tuple[int, ...]:
    return (value.st_dev, value.st_ino, value.st_size, value.st_mtime_ns)


def _no_reparse(path: Path) -> os.stat_result:
    value = path.lstat()
    if stat.S_ISLNK(value.st_mode) or getattr(value, "st_file_attributes", 0) & _REPARSE_POINT:
        _fail("NATIVE_PATH_REPARSE_POINT", detail=str(path))
    return value


def _safe_root(path: Path) -> Path:
    if not path.is_absolute() or ".." in path.parts:
        _fail("NATIVE_PATH_UNSAFE")
    for item in reversed((path, *path.parents)):
        _no_reparse(item)
    resolved = path.resolve(strict=True)
    if not resolved.is_dir():
        _fail("NATIVE_PATH_UNSAFE")
    return resolved


def _safe_path(root: Path, relative: str) -> Path:
    if type(relative) is not str or not relative:
        _fail("NATIVE_PATH_UNSAFE")
    part = Path(relative.replace("\\", "/"))
    if part.is_absolute() or part.drive or any(item in {".", ".."} for item in part.parts):
        _fail("NATIVE_PATH_UNSAFE")
    candidate = root / part
    current = root
    _no_reparse(current)
    for name in part.parts:
        current = current / name
        _no_reparse(current)
    resolved = candidate.resolve(strict=True)
    if root not in resolved.parents or not resolved.is_file():
        _fail("NATIVE_PATH_UNSAFE")
    return resolved


def _pairs(items: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in items:
        if key in result:
            _fail("NATIVE_METADATA_DUPLICATE_KEY")
        result[key] = value
    return result


def _bad_constant(value: str) -> None:
    _fail("NATIVE_METADATA_NONFINITE", detail=value)


class _DigestReader(io.RawIOBase):
    def __init__(self, raw: Any) -> None:
        self.raw = raw
        self.digest = hashlib.sha256()
        self.bytes_read = 0

    def readable(self) -> bool:
        return True

    def readinto(self, buffer: Any) -> int:
        count = self.raw.readinto(buffer)
        if count:
            self.digest.update(memoryview(buffer)[:count])
            self.bytes_read += count
        return count or 0


class NativeInputs:
    def __init__(self, data_dir: Path, expected_activation_sha256: str,
                 expected_pointer_sha256: str) -> None:
        if not _is_digest(expected_activation_sha256) or not _is_digest(expected_pointer_sha256):
            _fail("EXPECTED_NATIVE_IDENTITIES_INVALID")
        self.data_dir = Path(data_dir)
        self.expected_activation_sha256 = expected_activation_sha256
        self.expected_pointer_sha256 = expected_pointer_sha256
        self._scope = None
        self._authority = None
        self._closed = False
        self._revalidated = False
        self._planned = False
        self._pins: dict[str, InputPin] = {}
        self._json_cache: dict[str, dict[str, Any]] = {}
        self._cell_cache: dict[tuple[str, str], SourceCell] = {}
        self._registered: dict[int, SourceCell] = {}
        self._metadata: dict[int, dict[str, Any]] = {}
        self._inflight: set[str] = set()
        self._failed_streams: set[str] = set()
        self._calibration_key = None
        self._calibration_data = None

    @property
    def pins(self) -> tuple[InputPin, ...]:
        return tuple(self._pins[key] for key in sorted(self._pins))

    def _live(self) -> None:
        if self._scope is None or self._closed:
            _fail("NATIVE_INPUTS_NOT_ACTIVE")

    def _record(self, path: Path, digest: str, size: int,
                identity: os.stat_result, kind: str, rows: int | None = None) -> None:
        pin = InputPin(str(path), digest, size, _stat_identity(identity), kind, rows)
        previous = self._pins.get(str(path))
        if previous is not None and previous != pin:
            _fail("NATIVE_INPUT_CHANGED", detail=str(path))
        self._pins[str(path)] = pin
        self._revalidated = False

    def _read_json(self, relative: str, expected: str | None = None) -> tuple[dict[str, Any], Path]:
        path = _safe_path(self.native_root, relative)
        if str(path) in self._json_cache:
            if expected is not None and self._pins[str(path)].sha256 != expected:
                _fail("NATIVE_METADATA_IDENTITY_MISMATCH")
            return self._json_cache[str(path)], path
        before = _no_reparse(path)
        if not 0 < before.st_size <= _MAX_METADATA_BYTES:
            _fail("NATIVE_METADATA_SIZE_INVALID", detail=str(path))
        with path.open("rb") as handle:
            before_handle = os.fstat(handle.fileno())
            raw = handle.read(_MAX_METADATA_BYTES + 1)
            after_handle = os.fstat(handle.fileno())
        after = _no_reparse(path)
        if (_stat_identity(before) != _stat_identity(after)
                or _stat_identity(before_handle) != _stat_identity(after_handle)
                or _common_identity(before) != _common_identity(before_handle)
                or len(raw) != before.st_size):
            _fail("NATIVE_METADATA_CHANGED_DURING_READ")
        digest = _sha(raw)
        if expected is not None and digest != expected:
            _fail("NATIVE_METADATA_IDENTITY_MISMATCH", detail=str(path))
        try:
            value = json.loads(raw.decode("utf-8-sig"), object_pairs_hook=_pairs,
                               parse_constant=_bad_constant)
        except (UnicodeError, ValueError) as exc:
            if isinstance(exc, NativeInputsError):
                raise
            _fail("NATIVE_METADATA_INVALID_JSON", detail=str(path))
        if type(value) is not dict:
            _fail("NATIVE_METADATA_NOT_OBJECT")
        self._record(path, digest, len(raw), after, "metadata")
        self._json_cache[str(path)] = value
        return value, path

    def __enter__(self) -> NativeInputs:
        if self._scope is not None or self._closed:
            _fail("NATIVE_INPUTS_NOT_REUSABLE")
        from pm_native_activation import ProductionNativeAuthorityVerificationScope

        self.data_dir = _safe_root(self.data_dir)
        self.native_root = _safe_root(self.data_dir / "native")
        pointer, _ = self._read_json("ACTIVE.json", self.expected_pointer_sha256)
        manifest, manifest_path = self._read_json(
            pointer["manifest_relative_path"], self.expected_activation_sha256)
        self._read_json(manifest["semantic_receipt_relative_path"], manifest["semantic_receipt_sha256"])
        self._read_json(manifest["run_manifest_relative_path"], manifest["run_manifest_sha256"])
        scope = ProductionNativeAuthorityVerificationScope()
        scope.__enter__()
        try:
            authority = scope.verify_receipt_backed_authority(self.data_dir)
            if (authority.active_pointer_sha256 != self.expected_pointer_sha256
                    or authority.activation_manifest_sha256 != self.expected_activation_sha256
                    or authority.activation_manifest_path != manifest_path):
                _fail("NATIVE_RECEIPT_AUTHORITY_IDENTITY_MISMATCH")
            for path, raw in ((self.native_root / "ACTIVE.json", authority.active_pointer_bytes),
                              (authority.activation_manifest_path, authority.activation_manifest_bytes),
                              (authority.run_manifest_path, authority.run_manifest_bytes),
                              (authority.semantic_receipt_path, authority.semantic_receipt_bytes)):
                if self._pins[str(path)].sha256 != _sha(raw):
                    _fail("NATIVE_RECEIPT_AUTHORITY_IDENTITY_MISMATCH")
        except BaseException as exc:
            scope.__exit__(type(exc), exc, exc.__traceback__)
            raise
        self._scope, self._authority = scope, authority
        self.authority_bindings = AuthorityBindings(
            authority.activation_id, authority.activation_manifest_sha256,
            authority.active_pointer_sha256, authority.run_manifest_sha256,
            authority.semantic_receipt_sha256, authority.semantic_verifier_build_digest)
        return self

    def __exit__(self, exc_type: Any, exc: Any, traceback: Any) -> bool:
        try:
            if exc_type is None and not self._revalidated:
                self.revalidate()
        except BaseException as failure:
            self._scope.__exit__(type(failure), failure, failure.__traceback__)
            self._closed = True
            raise
        try:
            return self._scope.__exit__(exc_type, exc, traceback)
        finally:
            self._closed = True
            self._calibration_data = None

    def _entry(self, symbol: str, timeframe: str) -> Mapping[str, Any]:
        try:
            entry = self._authority.entries[symbol][timeframe]
        except (KeyError, TypeError):
            _fail("NATIVE_CELL_NOT_PRESENT", symbol, timeframe)
        if not isinstance(entry, Mapping) or entry.get("symbol") != symbol or entry.get("timeframe") != timeframe:
            _fail("NATIVE_CELL_METADATA_INVALID", symbol, timeframe)
        return entry

    def _cell(self, symbol: str, timeframe: str) -> SourceCell:
        entry = self._entry(symbol, timeframe)
        if entry.get("status") != "evidence_candidate":
            _fail("SOURCE_CELL_NOT_EVIDENCE_CANDIDATE", symbol, timeframe)
        cached = self._cell_cache.get((symbol, timeframe))
        if cached is not None:
            return cached
        from pm_native_request_contract import FIXED_ALIGNMENT_MODE
        from tools.build_spread_profile import _source_metadata

        bars_path = _safe_path(self.native_root, entry["bars_relative_path"])
        sidecar, sidecar_path = self._read_json(entry["sidecar_relative_path"], entry["sidecar_sha256"])
        provenance, provenance_path = self._read_json(
            sidecar["source_provenance_relative_path"], sidecar["source_provenance_sha256"])
        try:
            meta = _source_metadata(self.native_root, dict(entry), self._authority.activation_id,
                                    dict(self._authority.run_manifest))
        except ValueError as error:
            if str(error) in {"METADATA_DIGEST_MISMATCH", "ARTIFACT_SIZE_MISMATCH"}:
                raise NativeInputsError("SOURCE_METADATA_AUTHENTICATION_FAILED", symbol=symbol,
                    timeframe=timeframe, detail=_bounded_message(error), abort_attempt=True) from error
            raise
        if str(meta["path"]) != str(bars_path):
            _fail("SOURCE_METADATA_PATH_MISMATCH", symbol, timeframe)
        request = provenance.get("request")
        if type(request) is not dict or request.get("alignment_mode") != FIXED_ALIGNMENT_MODE:
            _fail("UNSUPPORTED_ALIGNMENT_CONSTRUCTOR", symbol, timeframe)
        raw_interval = request.get("source_request_interval")
        if type(raw_interval) is not dict or set(raw_interval) != {"start_utc", "end_exclusive_utc"}:
            _fail("REQUEST_INTERVAL_INVALID", symbol, timeframe)
        acquisition = Interval(raw_interval["start_utc"], raw_interval["end_exclusive_utc"])
        protected = meta["protected_cutoff"].isoformat()
        if _utc(protected) != _utc(PROTECTED_CUTOFF):
            _fail("PROTECTED_CUTOFF_MISMATCH", symbol, timeframe)
        geometry = aligned_prefix_geometry(timeframe, acquisition, protected)
        classification = instrument_session_class(symbol, provenance.get("instrument"))
        alignment = _digest({"contract": "V5", "mode": FIXED_ALIGNMENT_MODE, "timeframe": timeframe})
        stratum = Stratum(timeframe, classification, alignment)
        identity = SourceIdentity(symbol, timeframe, self._authority.activation_id,
            self._authority.activation_manifest_sha256, meta["sha256"], meta["sidecar_sha256"],
            meta["provenance_sha256"], _digest(request), meta["point"], "points")
        if (type(meta["expected_rows"]) is not int or meta["expected_rows"] <= 0
                or type(meta["expected_bytes"]) is not int or meta["expected_bytes"] <= 0):
            _fail("SOURCE_ARTIFACT_METADATA_INVALID", symbol, timeframe)
        observed = _observed_interval(entry, timeframe, meta["expected_rows"])
        cell = SourceCell(symbol, timeframe, entry["status"], str(bars_path), str(sidecar_path),
            str(provenance_path), identity, geometry.interval, stratum, geometry,
            meta["expected_rows"], meta["expected_bytes"], protected, "", observed)
        cell = replace(cell, metadata_digest=_digest(asdict(cell)))
        self._cell_cache[(symbol, timeframe)] = cell
        self._registered[id(cell)] = cell
        self._metadata[id(cell)] = meta
        return cell

    def _project_cell(self, cell: SourceCell, interval: Interval, stratum: Stratum) -> SourceCell:
        start, end = _bounds(interval)
        source_start, source_end = _bounds(cell.interval)
        if start < source_start or end > source_end:
            _fail("PROJECTED_SOURCE_INTERVAL_UNSUPPORTED", cell.symbol, cell.timeframe)
        projected = replace(cell, interval=interval, stratum=stratum, metadata_digest="")
        projected = replace(projected, metadata_digest=_digest(asdict(projected)))
        self._registered[id(projected)] = projected
        self._metadata[id(projected)] = self._metadata[id(cell)]
        return projected

    def _target_spec(self, symbol: str, timeframe: str) -> dict[str, Any]:
        """Read only activated target facts and authenticated RUN request geometry."""
        def ordinary_json(value: Any) -> Any:
            if isinstance(value, Mapping):
                return {key: ordinary_json(item) for key, item in value.items()}
            if isinstance(value, (list, tuple)):
                return [ordinary_json(item) for item in value]
            return value

        entry = ordinary_json(self._entry(symbol, timeframe))
        request = ordinary_json(self._authority.run_manifest["request"])
        acquisition = request["timeframe_source_request_intervals"][timeframe]
        calendar = request["timeframe_calendar_boundaries"][timeframe]
        reasons = entry.get("reason_codes")
        if (type(reasons) is not list or any(type(value) is not str for value in reasons)
                or type(acquisition) is not dict
                or set(acquisition) != {"start_utc", "end_exclusive_utc"}
                or type(calendar) is not dict
                or not _is_digest(self._authority.run_manifest_sha256)):
            _fail("TARGET_RUN_SPECIFICATION_UNSUPPORTED", symbol, timeframe)
        _bounds(Interval(**acquisition))
        if _utc(calendar["final_lockbox"]["start_ts"]) != _utc(PROTECTED_CUTOFF):
            _fail("PROTECTED_CUTOFF_MISMATCH", symbol, timeframe)
        return {"symbol": symbol, "timeframe": timeframe,
                "native_status": entry.get("status"), "native_reason_codes": list(reasons),
                "native_entry_sha256": _digest(dict(entry)),
                "run_manifest_sha256": self._authority.run_manifest_sha256,
                "run_request_sha256": _digest(dict(request)),
                "target_source_request_interval": json.loads(canonical_json_bytes(acquisition)),
                "target_calendar_boundaries": json.loads(canonical_json_bytes(calendar)),
                "point": None, "instrument_session_class": None,
                "source_identity_from": "UNRESOLVED_ACCEPTED_M5_METADATA"}

    def plan(self, targets: Iterable[tuple[str, str]], cutoff: str) -> NativePlan:
        self._live()
        if self._planned:
            _fail("NATIVE_PLAN_ALREADY_FROZEN")
        self._planned = True
        if _utc(cutoff) != _utc(PROTECTED_CUTOFF):
            _fail("PROTECTED_CUTOFF_MISMATCH")
        roster = tuple(targets)
        if (len(roster) != 35
                or any(type(pair) is not tuple or len(pair) != 2
                       or any(type(part) is not str or not part for part in pair) for pair in roster)
                or len(set(roster)) != 35):
            _fail("EXACT_35_TARGET_ROSTER_REQUIRED")
        from pm_native_request_contract import FIXED_ALIGNMENT_MODE

        selected: list[TargetCell] = []
        stops: list[StopCell] = []
        catalog_errors: list[StopCell] = []
        for symbol, timeframe in sorted(roster):
            spec = extent = stratum = None
            try:
                spec = self._target_spec(symbol, timeframe)
                if (spec["native_status"] != "technical_error"
                        or self._entry(symbol, "M5").get("status") != "evidence_candidate"):
                    _fail("TARGET_NATIVE_TECHNICAL_M5_CANDIDATE_REQUIRED", symbol, timeframe)
                if timeframe not in {"M15", "M30", "H1"}:
                    spec["source_identity_from"] = "UNRESOLVED_UNSUPPORTED_ALIGNMENT"
                    _fail("UNSUPPORTED_ALIGNMENT_CONSTRUCTOR", symbol, timeframe)
                m5 = self._cell(symbol, "M5")
                if m5.observed_interval is None:
                    _fail("AUTHENTICATED_OBSERVED_BOUNDS_UNAVAILABLE", symbol, "M5")
                spec.update(point=m5.source_identity.point,
                    instrument_session_class=m5.stratum.instrument_session_class,
                    source_identity_from="ACCEPTED_SAME_SYMBOL_M5")
                required = aligned_prefix_geometry(timeframe,
                    Interval(**spec["target_source_request_interval"]), cutoff).interval
                alignment = _digest({"contract": "V5", "mode": FIXED_ALIGNMENT_MODE,
                                     "timeframe": timeframe})
                stratum = Stratum(timeframe, m5.stratum.instrument_session_class, alignment)
                extent = available_history_extent(timeframe, required,
                    m5.geometry.acquisition_interval, m5.observed_interval)
                available = Interval(**extent["available_interval"])
                projected = self._project_cell(m5, available, stratum)
                selected.append(TargetCell(symbol, timeframe, stratum, available, projected, spec, extent))
            except OSError as error:
                raise NativeInputsError("PLANNING_IO_ABORT", symbol=symbol, timeframe=timeframe,
                    detail=_bounded_message(error), abort_attempt=True) from error
            except (NativeInputsError, ValueError, KeyError, TypeError) as error:
                if getattr(error, "abort_attempt", False):
                    raise
                stops.append(StopCell(symbol, timeframe,
                    getattr(error, "code", "TARGET_METADATA_UNSUPPORTED"), stratum=stratum,
                    target_spec=spec, extent=extent, **exception_details(error)))
        controls: list[ControlPair] = []
        strata = sorted({row.stratum for row in selected}, key=lambda row: (row.timeframe,
                        row.instrument_session_class, row.alignment_identity))
        for stratum in strata:
            control = None
            for symbol in sorted(self._authority.entries):
                try:
                    if (self._entry(symbol, stratum.timeframe).get("status") != "evidence_candidate"
                            or self._entry(symbol, "M5").get("status") != "evidence_candidate"):
                        continue
                    native, m5 = self._cell(symbol, stratum.timeframe), self._cell(symbol, "M5")
                    if native.stratum != stratum:
                        continue
                    if native.observed_interval is None or m5.observed_interval is None:
                        _fail("AUTHENTICATED_OBSERVED_BOUNDS_UNAVAILABLE", symbol, stratum.timeframe)
                    if (native.source_identity.point != m5.source_identity.point
                            or native.stratum.instrument_session_class != m5.stratum.instrument_session_class):
                        _fail("TARGET_M5_INSTRUMENT_CLASS_MISMATCH", symbol, stratum.timeframe)
                    extent = available_history_extent(stratum.timeframe, native.geometry.interval,
                        m5.geometry.acquisition_interval, m5.observed_interval,
                        other_observed_interval=native.observed_interval)
                    available = Interval(**extent["available_interval"])
                    control = ControlPair(stratum, available,
                        self._project_cell(native, available, stratum),
                        self._project_cell(m5, available, stratum), extent)
                    break
                except OSError as error:
                    raise NativeInputsError("PLANNING_IO_ABORT", symbol=symbol,
                        timeframe=stratum.timeframe, detail=_bounded_message(error),
                        abort_attempt=True) from error
                except (NativeInputsError, ValueError, KeyError, TypeError) as error:
                    if getattr(error, "abort_attempt", False):
                        raise
                    catalog_errors.append(StopCell(symbol, stratum.timeframe,
                        getattr(error, "code", "CONTROL_METADATA_UNSUPPORTED"),
                        stratum=stratum, **exception_details(error)))
            if control is not None:
                controls.append(control)
            else:
                affected = [row for row in selected if row.stratum == stratum]
                stops.extend(StopCell(row.symbol, row.timeframe, "CONTROL_STRATUM_UNSUPPORTED", stratum,
                    target_spec=row.target_spec, extent=row.extent) for row in affected)
                selected = [row for row in selected if row.stratum != stratum]
        return NativePlan("ONE_AVAILABLE_HISTORY_M5_DERIVED_DATA_ONLY_ATTEMPT", cutoff,
                          tuple(selected), tuple(controls), tuple(stops), tuple(catalog_errors),
                          self.authority_bindings, self.pins)

    def _known(self, cell: SourceCell) -> dict[str, Any]:
        self._live()
        if self._registered.get(id(cell)) is not cell:
            _fail("SOURCE_CELL_NOT_OWNED")
        return self._metadata[id(cell)]

    @staticmethod
    def _counter(value: str, code: str) -> int:
        try:
            number = Decimal(value)
        except (InvalidOperation, TypeError, ValueError) as error:
            raise NativeInputsError(code, detail=_bounded_message(error), abort_attempt=False) from error
        if not number.is_finite() or number < 0 or number != number.to_integral_value():
            _fail(code)
        return int(number)

    def iter_bars(self, cell: SourceCell, interval: Interval) -> Iterable[Bar]:
        self._known(cell)
        if interval != cell.interval:
            _fail("FROZEN_SOURCE_INTERVAL_MISMATCH", cell.symbol, cell.timeframe)
        lower, upper = _bounds(interval)
        cutoff = _utc(cell.cutoff_utc)
        try:
            path = _safe_path(self.native_root, str(Path(cell.bars_path).relative_to(self.native_root)))
            before = _no_reparse(path)
        except ValueError as error:
            if isinstance(error, NativeInputsError):
                raise
            raise NativeInputsError("SOURCE_AUTHENTICATION_FAILED", symbol=cell.symbol,
                timeframe=cell.timeframe, detail=_bounded_message(error), abort_attempt=True) from error
        except OSError as error:
            raise NativeInputsError("SOURCE_AUTHENTICATION_FAILED", symbol=cell.symbol,
                timeframe=cell.timeframe, detail=_bounded_message(error), abort_attempt=True) from error
        key = str(path)
        if key in self._inflight:
            _fail("SOURCE_STREAM_ALREADY_ACTIVE", cell.symbol, cell.timeframe)
        if before.st_size != cell.expected_bytes:
            _fail("SOURCE_SIZE_MISMATCH", cell.symbol, cell.timeframe)
        self._inflight.add(key)
        self._revalidated = False
        rows = 0
        previous_open = previous_close = None
        exhausted = authenticated = parsing_failed = False
        try:
            with path.open("rb") as raw:
                first_handle = os.fstat(raw.fileno())
                if _common_identity(before) != _common_identity(first_handle):
                    _fail("SOURCE_CHANGED_BEFORE_SCAN")
                reader = _DigestReader(raw)
                with io.BufferedReader(reader) as buffer:
                    with io.TextIOWrapper(buffer, encoding="utf-8-sig", newline="") as text:
                        table = csv.DictReader(text, strict=True)
                        try:
                            fields = table.fieldnames
                            required = {"time", "_BAR_CLOSE_TS", "Open", "High", "Low", "Close", "Volume"}
                            if (fields is None or len(fields) != len(set(fields))
                                    or not required.issubset(fields)):
                                _fail("SOURCE_CSV_SCHEMA_UNSUPPORTED", cell.symbol, cell.timeframe)
                            for row in table:
                                rows += 1
                                if None in row or any(row.get(name) is None for name in fields):
                                    _fail("SOURCE_CSV_ROW_INVALID", cell.symbol, cell.timeframe)
                                opened, closed = _utc(row["time"]), _utc(row["_BAR_CLOSE_TS"])
                                if (closed <= opened or (previous_open is not None
                                        and (opened <= previous_open or opened < previous_close))):
                                    _fail("SOURCE_UTC_AXIS_INVALID", cell.symbol, cell.timeframe)
                                previous_open, previous_close = opened, closed
                                if ((opened < lower < closed)
                                        or (opened < upper < closed and opened >= lower)):
                                    _fail("SOURCE_ROW_STRADDLES_FROZEN_INTERVAL", cell.symbol, cell.timeframe)
                                if opened < lower or opened >= upper or closed > cutoff:
                                    continue
                                try:
                                    prices = [float(row[name]) for name in ("Open", "High", "Low", "Close")]
                                except (ValueError, TypeError, OverflowError) as error:
                                    raise NativeInputsError("SOURCE_PRICE_PARSE_INVALID", symbol=cell.symbol,
                                        timeframe=cell.timeframe, detail=_bounded_message(error),
                                        abort_attempt=False) from error
                                if not all(math.isfinite(value) for value in prices):
                                    _fail("SOURCE_PRICE_INVALID", cell.symbol, cell.timeframe)
                                volume = self._counter(row["Volume"], "SOURCE_VOLUME_INVALID")
                                real = (self._counter(row["RealVolume"], "SOURCE_REAL_VOLUME_INVALID")
                                        if "RealVolume" in fields else None)
                                spread = (self._counter(row["Spread"], "SOURCE_SPREAD_INVALID")
                                          if "Spread" in fields and row["Spread"] != "" else None)
                                yield Bar(row["time"], row["_BAR_CLOSE_TS"], *prices, volume, real, spread)
                            exhausted = True
                        except (csv.Error, UnicodeError) as error:
                            parsing_failed = True
                            raise NativeInputsError("SOURCE_CSV_PARSING_UNAUTHENTICATED", symbol=cell.symbol,
                                timeframe=cell.timeframe, detail=_bounded_message(error),
                                abort_attempt=True) from error
                        finally:
                            # Authenticate EOF even after schema/value refusal or consumer close.
                            try:
                                if not exhausted:
                                    for row in table:
                                        rows += 1
                            except (csv.Error, UnicodeError) as error:
                                raise NativeInputsError("SOURCE_CSV_PARSING_UNAUTHENTICATED", symbol=cell.symbol,
                                    timeframe=cell.timeframe, detail=_bounded_message(error),
                                    abort_attempt=True) from error
                            last_handle = os.fstat(raw.fileno())
                            after = _no_reparse(path)
                            if (_stat_identity(before) != _stat_identity(after)
                                    or _stat_identity(first_handle) != _stat_identity(last_handle)):
                                _fail("SOURCE_CHANGED_DURING_SCAN", cell.symbol, cell.timeframe)
                            if (parsing_failed or reader.digest.hexdigest() != cell.source_identity.bars_sha256
                                    or reader.bytes_read != cell.expected_bytes or rows != cell.expected_rows):
                                _fail("SOURCE_DIGEST_ROWS_OR_BYTES_MISMATCH", cell.symbol, cell.timeframe)
                            self._record(path, reader.digest.hexdigest(), reader.bytes_read, after, "csv", rows)
                            authenticated = True
        except OSError as error:
            self._failed_streams.add(key)
            raise NativeInputsError("SOURCE_AUTHENTICATION_FAILED", symbol=cell.symbol,
                timeframe=cell.timeframe, detail=_bounded_message(error), abort_attempt=True) from error
        except BaseException:
            if not authenticated:
                self._failed_streams.add(key)
            raise
        finally:
            self._inflight.discard(key)

    def _authenticate_csv_read(self, cell: SourceCell,
                               expected_stat: tuple[int, ...]) -> None:
        """Finish identity proof after an original reader's format refusal."""
        try:
            path = _safe_path(self.native_root, str(Path(cell.bars_path).relative_to(self.native_root)))
            before = _no_reparse(path)
            if _stat_identity(before) != expected_stat:
                _fail("SOURCE_CHANGED_DURING_SCAN", cell.symbol, cell.timeframe)
            with path.open("rb") as raw:
                first_handle = os.fstat(raw.fileno())
                if _common_identity(before) != _common_identity(first_handle):
                    _fail("SOURCE_CHANGED_BEFORE_SCAN", cell.symbol, cell.timeframe)
                reader = _DigestReader(raw)
                with io.BufferedReader(reader) as buffer:
                    with io.TextIOWrapper(buffer, encoding="utf-8-sig", newline="") as text:
                        table = csv.reader(text, strict=True)
                        next(table, None)
                        rows = sum(bool(row) for row in table)
                        last_handle = os.fstat(raw.fileno())
            after = _no_reparse(path)
            if (_stat_identity(before) != _stat_identity(after)
                    or _stat_identity(first_handle) != _stat_identity(last_handle)):
                _fail("SOURCE_CHANGED_DURING_SCAN", cell.symbol, cell.timeframe)
            if (reader.digest.hexdigest() != cell.source_identity.bars_sha256
                    or reader.bytes_read != cell.expected_bytes or rows != cell.expected_rows):
                _fail("SOURCE_DIGEST_ROWS_OR_BYTES_MISMATCH", cell.symbol, cell.timeframe)
            self._record(path, reader.digest.hexdigest(), reader.bytes_read, after, "csv", rows)
        except NativeInputsError as error:
            if error.abort_attempt:
                self._failed_streams.add(cell.bars_path)
            raise
        except (OSError, csv.Error, UnicodeError) as error:
            self._failed_streams.add(cell.bars_path)
            raise NativeInputsError("SOURCE_AUTHENTICATION_FAILED", symbol=cell.symbol,
                timeframe=cell.timeframe, detail=_bounded_message(error), abort_attempt=True) from error

    def calibration_source(self, cell: SourceCell, cutoff: str) -> dict[str, Any]:
        meta = self._known(cell)
        if _utc(cutoff) != _utc(cell.cutoff_utc):
            _fail("PROTECTED_CUTOFF_MISMATCH", cell.symbol, cell.timeframe)
        from tools.build_spread_profile import read_bound_prefix
        import pandas as pd

        try:
            path = _safe_path(self.native_root, str(Path(cell.bars_path).relative_to(self.native_root)))
            before = _no_reparse(path)
        except (OSError, ValueError) as error:
            if isinstance(error, NativeInputsError):
                raise
            raise NativeInputsError("CALIBRATION_SOURCE_AUTHENTICATION_FAILED", symbol=cell.symbol,
                timeframe=cell.timeframe, detail=_bounded_message(error), abort_attempt=True) from error
        key = (str(path), cutoff)
        if key == self._calibration_key:
            return dict(self._calibration_data)
        try:
            value = read_bound_prefix(dict(meta), pd.Timestamp(cutoff), with_spreads=True)
            after = _no_reparse(path)
        except OSError as error:
            raise NativeInputsError("CALIBRATION_SOURCE_AUTHENTICATION_FAILED", symbol=cell.symbol,
                timeframe=cell.timeframe, detail=_bounded_message(error), abort_attempt=True) from error
        except (ValueError, KeyError, TypeError, UnicodeError) as error:
            if isinstance(error, NativeInputsError):
                raise
            integrity = str(error) in {"SOURCE_CHANGED_DURING_SCAN", "SOURCE_DIGEST_MISMATCH",
                                      "SOURCE_ROWS_OR_BYTES_MISMATCH"}
            if integrity:
                self._failed_streams.add(str(path))
            else:
                self._authenticate_csv_read(cell, _stat_identity(before))
            raise NativeInputsError("CALIBRATION_SOURCE_AUTHENTICATION_FAILED" if integrity
                else "CALIBRATION_SOURCE_INVALID", symbol=cell.symbol, timeframe=cell.timeframe,
                detail=_bounded_message(error), abort_attempt=integrity) from error
        if _stat_identity(before) != _stat_identity(after):
            _fail("SOURCE_CHANGED_DURING_CALIBRATION_READ", cell.symbol, cell.timeframe)
        self._record(path, cell.source_identity.bars_sha256, cell.expected_bytes,
                     after, "csv", cell.expected_rows)
        for array in value.values():
            if hasattr(array, "setflags"):
                array.setflags(write=False)
        self._calibration_key, self._calibration_data = key, value
        return dict(value)

    def _revalidate_read_pins(self) -> tuple[InputPin, ...]:
        """Check file identities only; this is not native authority verification."""
        self._live()
        if self._inflight or self._failed_streams:
            _fail("SOURCE_STREAM_AUTHENTICATION_INCOMPLETE")
        for pin in self.pins:
            try:
                path = _safe_path(self.native_root, str(Path(pin.path).relative_to(self.native_root)))
                before = _no_reparse(path)
                if _stat_identity(before) != pin.stat_identity:
                    _fail("NATIVE_INPUT_CHANGED", detail=pin.path)
                digest = hashlib.sha256()
                with path.open("rb") as handle:
                    first_handle = os.fstat(handle.fileno())
                    if _common_identity(before) != _common_identity(first_handle):
                        _fail("NATIVE_INPUT_CHANGED", detail=pin.path)
                    for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                        digest.update(chunk)
                    last_handle = os.fstat(handle.fileno())
                after = _no_reparse(path)
            except OSError as error:
                raise NativeInputsError("NATIVE_INPUT_CHANGED", detail=_bounded_message(error),
                                         abort_attempt=True) from error
            if (_stat_identity(before) != _stat_identity(after)
                    or _stat_identity(first_handle) != _stat_identity(last_handle)
                    or digest.hexdigest() != pin.sha256):
                _fail("NATIVE_INPUT_CHANGED", detail=pin.path)
        return self.pins

    def revalidate(self) -> tuple[InputPin, ...]:
        self._live()
        if self._revalidated:
            return self.pins
        if self._inflight or self._failed_streams:
            _fail("SOURCE_STREAM_AUTHENTICATION_INCOMPLETE")
        self._scope.revalidate_receipt_backed_authority()
        pins = self._revalidate_read_pins()
        self._revalidated = True
        return pins
