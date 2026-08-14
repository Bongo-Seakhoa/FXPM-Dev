"""Immutable broker swap-term overlays bound to native price activations.

The MetaTrader5 Python package does not expose the seven per-weekday swap
factors.  Production-authenticated terms therefore enter through a nonce-bound
MQL5 export.  Python-only or legacy specifications are explicitly incomplete
and can never be upgraded by inference.
"""

from __future__ import annotations

import csv
import hashlib
import io
import json
import math
import os
import secrets
import threading
import time
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, Iterator, Mapping, Optional, Sequence, Tuple

import numpy as np
import pandas as pd


TERMS_EXPORT_SCHEMA = "fxpm-mql5-broker-terms-export-v1"
TERMS_REQUEST_SCHEMA = "fxpm-mql5-broker-terms-request-v1"
TERMS_SNAPSHOT_SCHEMA = "fxpm-broker-terms-snapshot-v1"
TERMS_ACTIVATION_SCHEMA = "fxpm-native-terms-activation-v1"
TERMS_ACTIVE_POINTER_SCHEMA = "fxpm-native-terms-active-pointer-v1"
D1_SESSION_SCHEDULE_SCHEMA = "fxpm-d1-swap-session-schedule-v2"
SESSION_SCHEDULE_SCHEMA = "fxpm-swap-session-schedule-v3"
D1_SESSION_DERIVATION_CONTRACT = "d1-interval-axis-v1"
INTRADAY_SESSION_DERIVATION_CONTRACT = "intraday-session-date-axis-v1"
SWAP_PROJECTION_CONTRACT = "fxpm-swap-preopen-close-projection-v1"
STRESS_POLICY_ID = "all_sessions_triple_debit_v1"
PRICE_UNIVERSE_V4_SCHEMA = "fxpm-native-universe-authenticity-v4"
PRICE_CELL_EVIDENCE_CANDIDATE = "evidence_candidate"
PRICE_CELL_TERMINAL_NONCANDIDATES = frozenset({"technical_error", "source_unavailable"})
_SCHEDULE_SOURCE_PREFERENCE = ("D1", "H4", "H1", "M30", "M15", "M5")
SCHEDULE_SOURCE_POLICY = "d1-preferred-coarsest-intraday-fallback-v1"
SCHEDULE_SOURCE_SELECTION_SCHEMA = "fxpm-schedule-source-selection-v1"
TERMS_PRICE_RECEIPT_MIGRATION_PIN_SCHEMA = "fxpm-terms-price-receipt-migration-pin-v1"
_TERMS_PRICE_RECEIPT_MIGRATION_PIN_FIELDS = frozenset(
    {
        "schema_version",
        "prepared_at_utc",
        "source_terms_active_pointer_sha256",
        "source_terms_activation_id",
        "source_terms_manifest_relative_path",
        "source_terms_manifest_sha256",
        "source_terms_snapshot_relative_path",
        "source_terms_snapshot_sha256",
        "predecessor_price_active_pointer_sha256",
        "predecessor_price_activation_id",
        "predecessor_price_manifest_sha256",
    }
)

_DIRECT_SNAPSHOT_KEYS = frozenset({
    "schema_version",
    "provenance_status",
    "extractor_contract",
    "nonce",
    "captured_at_utc",
    "capture_started_at_utc",
    "broker_identity",
    "roster",
    "symbols",
    "source_export_sha256",
    "content_sha256",
})
_DIRECT_RECORD_KEYS = frozenset({
    "canonical_symbol",
    "broker_symbol",
    "swap_mode",
    "swap_long",
    "swap_short",
    "swap_rollover3days",
    "weekday_factors",
    "d1_anchors",
    "provenance_status",
})
_BROKER_IDENTITY_KEYS = frozenset({
    "broker_server",
    "terminal_company",
    "account_currency",
    "terminal_build",
})
_STRESS_POLICY = {
    "policy_id": STRESS_POLICY_ID,
    "explicit_approval_required": True,
    "positive_credits_suppressed": True,
}
_TERMS_POINTER_THREAD_LOCK = threading.RLock()
_NATIVE_POINTER_THREAD_LOCK = threading.RLock()

AUTHENTICATED = "AUTHENTICATED"
STRESS_APPROVED = "STRESS_APPROVED"
INCOMPLETE = "INCOMPLETE"
LEGACY_UNTRUSTED = "LEGACY_UNTRUSTED"
INVALID = "INVALID"

WEEKDAYS: Tuple[str, ...] = (
    "sunday",
    "monday",
    "tuesday",
    "wednesday",
    "thursday",
    "friday",
    "saturday",
)

EXPORT_COLUMNS: Tuple[str, ...] = (
    "schema_version",
    "nonce",
    "captured_at_utc",
    "canonical_symbol",
    "broker_symbol",
    "broker_server",
    "terminal_company",
    "account_currency",
    "terminal_build",
    "swap_mode",
    "swap_long",
    "swap_short",
    "swap_rollover3days",
    *(f"swap_{weekday}" for weekday in WEEKDAYS),
    "d1_anchors",
)

REQUEST_COLUMNS: Tuple[str, ...] = (
    "schema_version",
    "nonce",
    "requested_at_utc",
    "canonical_symbol",
    "broker_symbol",
    "expected_server",
    "expected_company",
    "expected_currency",
)


class BrokerTermsError(ValueError):
    """A broker-terms artifact is incomplete, inconsistent, or unsafe."""


class ScheduleSourceUnsuitableError(BrokerTermsError):
    """One authentic candidate cannot represent the required broker days."""

    def __init__(self, reason_code: str, message: str):
        super().__init__(message)
        self.reason_code = str(reason_code)


class TermsStorageBoundError(BrokerTermsError):
    """Protected evidence alone exceeds the configured storage bound."""


@dataclass(frozen=True)
class SwapProjection:
    pre_open_units: np.ndarray
    at_close_units: np.ndarray
    digest: str
    event_count: int


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _aware_utc(value: Any, field: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(str(value).strip().replace("Z", "+00:00"))
    except (TypeError, ValueError) as exc:
        raise BrokerTermsError(f"{field} is not a valid ISO timestamp") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise BrokerTermsError(f"{field} must be timezone-aware")
    return parsed.astimezone(timezone.utc)


def _iso(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _require_snapshot_fresh(
    capture_started_at_utc: datetime,
    observed_at_utc: datetime,
    max_age_seconds: int,
) -> None:
    age_seconds = (
        observed_at_utc.astimezone(timezone.utc)
        - capture_started_at_utc.astimezone(timezone.utc)
    ).total_seconds()
    if age_seconds < -60 or age_seconds > int(max_age_seconds):
        raise BrokerTermsError("terms snapshot is not fresh at activation time")


def _canonical_bytes(value: Any) -> bytes:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")


def canonical_sha256(value: Any) -> str:
    return hashlib.sha256(_canonical_bytes(value)).hexdigest()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _is_sha256(value: Any) -> bool:
    text = str(value or "").strip().lower()
    return len(text) == 64 and all(character in "0123456789abcdef" for character in text)


def _finite_float(value: Any, field: str) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError) as exc:
        raise BrokerTermsError(f"{field} is not numeric") from exc
    if not math.isfinite(number):
        raise BrokerTermsError(f"{field} is not finite")
    return number


def _strict_int(value: Any, field: str) -> int:
    number = _finite_float(value, field)
    integer = int(number)
    if not math.isclose(number, integer, rel_tol=0.0, abs_tol=1e-12):
        raise BrokerTermsError(f"{field} is not an integer")
    return integer


def _safe_relative(root: Path, relative: Any) -> Path:
    text = str(relative or "").strip()
    if not text or Path(text).is_absolute():
        raise BrokerTermsError("artifact path is empty or absolute")
    root = Path(root).resolve()
    candidate = (root / text).resolve()
    try:
        candidate.relative_to(root)
    except ValueError as exc:
        raise BrokerTermsError("artifact path escapes its root") from exc
    return candidate


def _read_bounded(path: Path, max_bytes: int) -> bytes:
    path = Path(path)
    try:
        if not path.is_file() or path.is_symlink():
            raise BrokerTermsError(f"artifact is missing or unsafe: {path}")
        size = path.stat().st_size
        if size <= 0 or size > int(max_bytes):
            raise BrokerTermsError(f"artifact size {size} is outside the allowed bound")
        with path.open("rb") as handle:
            payload = handle.read(int(max_bytes) + 1)
    except BrokerTermsError:
        raise
    except OSError as exc:
        raise BrokerTermsError(f"artifact cannot be read safely: {path}") from exc
    if len(payload) > int(max_bytes):
        raise BrokerTermsError("artifact exceeded its read bound")
    return payload


def _atomic_create_json(path: Path, value: Mapping[str, Any]) -> None:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        raise FileExistsError(f"immutable artifact already exists: {path}")
    temporary = path.with_name(f".{path.name}.{os.getpid()}.{secrets.token_hex(6)}.tmp")
    encoded = json.dumps(value, indent=2, sort_keys=True, ensure_ascii=False, allow_nan=False)
    try:
        with temporary.open("x", encoding="utf-8", newline="\n") as handle:
            handle.write(encoded)
            handle.flush()
            os.fsync(handle.fileno())
        if path.exists():
            raise FileExistsError(f"immutable artifact appeared concurrently: {path}")
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _atomic_replace_json(
    path: Path,
    value: Mapping[str, Any],
    *,
    before_replace: Optional[Callable[[], None]] = None,
) -> None:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.{secrets.token_hex(6)}.tmp")
    encoded = json.dumps(value, indent=2, sort_keys=True, ensure_ascii=False, allow_nan=False)
    try:
        with temporary.open("x", encoding="utf-8", newline="\n") as handle:
            handle.write(encoded)
            handle.flush()
            os.fsync(handle.fileno())
        if before_replace is not None:
            before_replace()
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


@contextmanager
def _file_pointer_lock(
    lock_path: Path,
    process_lock: threading.RLock,
    *,
    label: str,
    timeout_seconds: float = 30.0,
) -> Iterator[None]:
    """Acquire one persistent one-byte advisory publication lock."""

    lock_path = Path(lock_path)
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    deadline = time.monotonic() + max(0.1, float(timeout_seconds))
    with process_lock:
        with lock_path.open("a+b") as handle:
            handle.seek(0, os.SEEK_END)
            if handle.tell() == 0:
                handle.write(b"0")
                handle.flush()
                os.fsync(handle.fileno())
            handle.seek(0)
            if os.name == "nt":
                import msvcrt

                while True:
                    try:
                        msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
                        break
                    except OSError as exc:
                        if time.monotonic() >= deadline:
                            raise BrokerTermsError(
                                f"timed out waiting for the {label} pointer lock"
                            ) from exc
                        time.sleep(0.025)
                try:
                    yield
                finally:
                    handle.seek(0)
                    try:
                        msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
                    except OSError:
                        # Closing the handle releases the kernel lock.  An
                        # unlock diagnostic must not turn a completed pointer
                        # swap into a false publication failure.
                        pass
            else:
                import fcntl

                while True:
                    try:
                        fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                        break
                    except BlockingIOError as exc:
                        if time.monotonic() >= deadline:
                            raise BrokerTermsError(
                                f"timed out waiting for the {label} pointer lock"
                            ) from exc
                        time.sleep(0.025)
                try:
                    yield
                finally:
                    try:
                        fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
                    except OSError:
                        # Handle close remains the authoritative release.
                        pass


@contextmanager
def _terms_pointer_lock(
    native_root: Path,
    *,
    timeout_seconds: float = 30.0,
) -> Iterator[None]:
    """Serialize terms publication and pruning across processes."""

    with _file_pointer_lock(
        Path(native_root).resolve() / ".TERMS_ACTIVE.json.lock",
        _TERMS_POINTER_THREAD_LOCK,
        label="broker-terms",
        timeout_seconds=timeout_seconds,
    ):
        yield


@contextmanager
def _native_price_pointer_lock(
    native_root: Path,
    *,
    timeout_seconds: float = 30.0,
) -> Iterator[None]:
    """Coordinate the final price snapshot with the canonical native promoter."""

    with _file_pointer_lock(
        Path(native_root).resolve() / ".ACTIVE.json.lock",
        _NATIVE_POINTER_THREAD_LOCK,
        label="native ACTIVE",
        timeout_seconds=timeout_seconds,
    ):
        yield


def _optional_pointer_bytes(path: Path, max_bytes: int = 1024 * 1024) -> Optional[bytes]:
    path = Path(path)
    if not path.exists():
        return None
    return _read_bounded(path, max_bytes)


def classify_terms_provenance(
    source: str,
    *,
    complete_weekday_factors: bool = False,
    stress_approved: bool = False,
    valid: bool = True,
) -> str:
    if not valid:
        return INVALID
    normalized = str(source or "").strip().upper()
    if stress_approved and normalized == "EXPLICIT_STRESS_POLICY":
        return STRESS_APPROVED
    if normalized == "MQL5_SYMBOLINFO_DIRECT" and complete_weekday_factors:
        return AUTHENTICATED
    if normalized in {"PYTHON_MT5", "PYTHON_MT5_PARTIAL"}:
        return INCOMPLETE
    if normalized in {"BROKER_SPECS_JSON", "CONFIG", "LEGACY"}:
        return LEGACY_UNTRUSTED
    return INVALID


def _parse_anchors(raw: Any, symbol: str) -> list[Dict[str, int]]:
    text = str(raw or "").strip()
    if not text:
        raise BrokerTermsError(f"{symbol}: D1 anchors are missing")
    anchors: list[Dict[str, int]] = []
    seen: set[int] = set()
    for token in text.split("|"):
        pieces = token.split(":")
        if len(pieces) != 2:
            raise BrokerTermsError(f"{symbol}: invalid D1 anchor {token!r}")
        epoch = _strict_int(pieces[0], f"{symbol}.anchor_epoch")
        weekday = _strict_int(pieces[1], f"{symbol}.anchor_weekday")
        if epoch <= 0 or not 0 <= weekday <= 6 or epoch in seen:
            raise BrokerTermsError(f"{symbol}: invalid or duplicate D1 anchor")
        seen.add(epoch)
        anchors.append({"open_epoch": epoch, "server_weekday": weekday})
    if not anchors or len(anchors) > 128:
        raise BrokerTermsError(f"{symbol}: D1 anchor count is outside 1..128")
    return anchors


def parse_terms_export(
    export_path: Path,
    *,
    expected_nonce: str,
    expected_roster: Mapping[str, str],
    expected_identity: Optional[Mapping[str, Any]] = None,
    requested_at_utc: Optional[Any] = None,
    now_utc: Optional[datetime] = None,
    max_age_seconds: int = 3600,
    max_bytes: int = 2 * 1024 * 1024,
    max_symbols: int = 1000,
) -> Dict[str, Any]:
    """Parse a complete nonce-bound MQL5 export; never infer missing factors."""
    nonce = str(expected_nonce or "").strip()
    if len(nonce) < 16 or len(nonce) > 128:
        raise BrokerTermsError("expected nonce length is invalid")
    roster = {
        str(canonical).strip().upper(): str(broker).strip()
        for canonical, broker in dict(expected_roster or {}).items()
    }
    if not roster or len(roster) > int(max_symbols) or any(not key or not value for key, value in roster.items()):
        raise BrokerTermsError("expected roster is empty, oversized, or malformed")
    source_payload = _read_bounded(Path(export_path), max_bytes)
    source_export_sha256 = hashlib.sha256(source_payload).hexdigest()
    payload = source_payload.decode("utf-8-sig", errors="strict")
    reader = csv.DictReader(io.StringIO(payload, newline=""))
    if tuple(reader.fieldnames or ()) != EXPORT_COLUMNS:
        raise BrokerTermsError("export header does not exactly match the v1 schema")

    expected_identity = dict(expected_identity or {})
    rows: Dict[str, Dict[str, Any]] = {}
    captures: list[datetime] = []
    observed_identity: Optional[Dict[str, Any]] = None
    for row_number, row in enumerate(reader, start=2):
        if row_number - 1 > int(max_symbols):
            raise BrokerTermsError("export row count exceeds the configured bound")
        if None in row or set(row) != set(EXPORT_COLUMNS):
            raise BrokerTermsError(f"row {row_number} has an invalid column shape")
        if str(row["schema_version"]) != TERMS_EXPORT_SCHEMA:
            raise BrokerTermsError(f"row {row_number} has the wrong schema version")
        if str(row["nonce"]) != nonce:
            raise BrokerTermsError(f"row {row_number} nonce mismatch")
        canonical = str(row["canonical_symbol"] or "").strip().upper()
        broker_symbol = str(row["broker_symbol"] or "").strip()
        if canonical not in roster or roster[canonical] != broker_symbol or canonical in rows:
            raise BrokerTermsError(f"row {row_number} roster mismatch or duplicate")
        captured = _aware_utc(row["captured_at_utc"], f"row {row_number}.captured_at_utc")
        captures.append(captured)
        identity = {
            "broker_server": str(row["broker_server"] or "").strip(),
            "terminal_company": str(row["terminal_company"] or "").strip(),
            "account_currency": str(row["account_currency"] or "").strip().upper(),
            "terminal_build": _strict_int(row["terminal_build"], "terminal_build"),
        }
        if not all(identity.values()) or identity["terminal_build"] <= 0:
            raise BrokerTermsError(f"row {row_number} broker identity is incomplete")
        if observed_identity is None:
            observed_identity = identity
        elif identity != observed_identity:
            raise BrokerTermsError("broker identity changed within one export")
        for key, expected in expected_identity.items():
            if expected in (None, ""):
                continue
            observed = identity.get(str(key))
            if str(observed).casefold() != str(expected).strip().casefold():
                raise BrokerTermsError(f"broker identity mismatch for {key}")

        swap_mode = _strict_int(row["swap_mode"], f"{canonical}.swap_mode")
        rollover_day = _strict_int(row["swap_rollover3days"], f"{canonical}.swap_rollover3days")
        # Several live MT5 brokers use integer 7 as a sentinel for "no triple
        # rollover day".  The seven direct factors remain authoritative.
        if swap_mode < 0 or not 0 <= rollover_day <= 7:
            raise BrokerTermsError(f"{canonical}: invalid swap mode or rollover weekday")
        factors: Dict[str, int] = {}
        for weekday in WEEKDAYS:
            factor_value = _finite_float(row[f"swap_{weekday}"], f"{canonical}.swap_{weekday}")
            factor = int(round(factor_value))
            if not math.isclose(factor_value, factor, rel_tol=0.0, abs_tol=1e-12) or factor not in {0, 1, 3}:
                raise BrokerTermsError(f"{canonical}: unsupported {weekday} swap factor")
            factors[weekday] = factor
        if swap_mode == 0:
            if any(factors.values()):
                raise BrokerTermsError(f"{canonical}: disabled swap mode has nonzero weekday factors")
        else:
            triple_days = [index for index, weekday in enumerate(WEEKDAYS) if factors[weekday] == 3]
            expected_triple_days = [] if rollover_day == 7 else [rollover_day]
            if triple_days != expected_triple_days:
                raise BrokerTermsError(f"{canonical}: weekday factors contradict swap_rollover3days")

        rows[canonical] = {
            "canonical_symbol": canonical,
            "broker_symbol": broker_symbol,
            "swap_mode": swap_mode,
            "swap_long": _finite_float(row["swap_long"], f"{canonical}.swap_long"),
            "swap_short": _finite_float(row["swap_short"], f"{canonical}.swap_short"),
            "swap_rollover3days": rollover_day,
            "weekday_factors": factors,
            "d1_anchors": _parse_anchors(row["d1_anchors"], canonical),
            "provenance_status": AUTHENTICATED,
        }
    if set(rows) != set(roster):
        missing = sorted(set(roster) - set(rows))
        extra = sorted(set(rows) - set(roster))
        raise BrokerTermsError(f"export roster is not exact; missing={missing}, extra={extra}")

    now = (now_utc or _utc_now()).astimezone(timezone.utc)
    newest = max(captures)
    oldest = min(captures)
    if newest > now + timedelta(seconds=60):
        raise BrokerTermsError("export capture time is in the future")
    if (now - oldest).total_seconds() > int(max_age_seconds):
        raise BrokerTermsError("export is stale")
    if (newest - oldest).total_seconds() > 300:
        raise BrokerTermsError("one export spans more than five minutes")
    requested = _aware_utc(requested_at_utc, "requested_at_utc") if requested_at_utc is not None else None
    if requested is not None and oldest < requested - timedelta(seconds=60):
        raise BrokerTermsError("export predates its request")

    snapshot: Dict[str, Any] = {
        "schema_version": TERMS_SNAPSHOT_SCHEMA,
        "provenance_status": AUTHENTICATED,
        "extractor_contract": "mql5-symbolinfo-direct-v1",
        "nonce": nonce,
        "captured_at_utc": _iso(newest),
        "capture_started_at_utc": _iso(oldest),
        "broker_identity": observed_identity or {},
        "roster": sorted(rows),
        "symbols": {key: rows[key] for key in sorted(rows)},
        "source_export_sha256": source_export_sha256,
    }
    snapshot["content_sha256"] = canonical_sha256(snapshot)
    return snapshot


def _casefold_key_map(payload: Mapping[str, Any], field: str) -> Dict[str, str]:
    """Map case-insensitive identities while rejecting ambiguous manifests."""

    if not isinstance(payload, Mapping):
        raise BrokerTermsError(f"{field} is not an object")
    index: Dict[str, str] = {}
    for raw_key in payload:
        if not isinstance(raw_key, str) or not raw_key.strip():
            raise BrokerTermsError(f"{field} contains an empty or non-string key")
        key = raw_key.strip()
        folded = key.casefold()
        previous = index.get(folded)
        if previous is not None and previous != key:
            raise BrokerTermsError(
                f"{field} contains a case-insensitive identity collision: "
                f"{previous!r} versus {key!r}"
            )
        index[folded] = key
    return index


def _snapshot_content_sha256(snapshot: Mapping[str, Any]) -> str:
    declared = str(snapshot.get("content_sha256") or "").strip().lower()
    without_hash = dict(snapshot)
    without_hash.pop("content_sha256", None)
    if not _is_sha256(declared) or declared != canonical_sha256(without_hash):
        raise BrokerTermsError("terms snapshot content hash is invalid")
    return declared


def _validate_direct_terms_snapshot(snapshot: Mapping[str, Any]) -> str:
    """Verify the exact JSON product emitted by ``parse_terms_export``."""

    if not isinstance(snapshot, Mapping):
        raise BrokerTermsError("authenticated terms snapshot is not an object")
    if set(snapshot) != set(_DIRECT_SNAPSHOT_KEYS):
        raise BrokerTermsError("authenticated snapshot shape is not the direct-export shape")
    if str(snapshot.get("schema_version") or "") != TERMS_SNAPSHOT_SCHEMA:
        raise BrokerTermsError("terms snapshot schema is invalid")
    if str(snapshot.get("provenance_status") or "") != AUTHENTICATED:
        raise BrokerTermsError("stress source is not an authenticated direct export")
    if str(snapshot.get("extractor_contract") or "") != "mql5-symbolinfo-direct-v1":
        raise BrokerTermsError("authenticated snapshot lacks direct-export provenance")
    nonce = str(snapshot.get("nonce") or "")
    if not 16 <= len(nonce) <= 128 or not _is_sha256(snapshot.get("source_export_sha256")):
        raise BrokerTermsError("authenticated snapshot lacks direct-export provenance")

    captured = _aware_utc(snapshot.get("captured_at_utc"), "snapshot.captured_at_utc")
    started = _aware_utc(
        snapshot.get("capture_started_at_utc"),
        "snapshot.capture_started_at_utc",
    )
    if started > captured or (captured - started).total_seconds() > 300:
        raise BrokerTermsError("authenticated snapshot capture interval is invalid")

    identity = snapshot.get("broker_identity")
    if not isinstance(identity, Mapping) or set(identity) != set(_BROKER_IDENTITY_KEYS):
        raise BrokerTermsError("authenticated snapshot broker identity is incomplete")
    if (
        not str(identity.get("broker_server") or "").strip()
        or not str(identity.get("terminal_company") or "").strip()
        or not str(identity.get("account_currency") or "").strip()
        or str(identity.get("account_currency") or "")
        != str(identity.get("account_currency") or "").upper()
        or type(identity.get("terminal_build")) is not int
        or int(identity.get("terminal_build")) <= 0
    ):
        raise BrokerTermsError("authenticated snapshot broker identity is incomplete")

    roster = snapshot.get("roster")
    symbols = snapshot.get("symbols")
    if not isinstance(roster, list) or not isinstance(symbols, Mapping):
        raise BrokerTermsError("terms snapshot roster and symbol records are not exact")
    _casefold_key_map(symbols, "terms snapshot symbols")
    if (
        not roster
        or len(roster) > 1000
        or any(not isinstance(value, str) or not value for value in roster)
        or roster != sorted(symbols)
        or len(roster) != len(set(roster))
    ):
        raise BrokerTermsError("terms snapshot roster and symbol records are not exact")

    for symbol in roster:
        record = symbols.get(symbol)
        if not isinstance(record, Mapping) or set(record) != set(_DIRECT_RECORD_KEYS):
            raise BrokerTermsError(f"{symbol}: direct-export record shape is invalid")
        if (
            symbol != symbol.upper()
            or str(record.get("canonical_symbol") or "") != symbol
            or not str(record.get("broker_symbol") or "").strip()
            or str(record.get("provenance_status") or "") != AUTHENTICATED
        ):
            raise BrokerTermsError(f"{symbol}: direct-export record identity is invalid")
        if type(record.get("swap_mode")) is not int:
            raise BrokerTermsError(f"{symbol}.swap_mode is not an integer")
        swap_mode = int(record.get("swap_mode"))
        if swap_mode < 0:
            raise BrokerTermsError(f"{symbol}: invalid swap mode")
        for rate_field in ("swap_long", "swap_short"):
            if type(record.get(rate_field)) is not float:
                raise BrokerTermsError(f"{symbol}.{rate_field} is not a direct-export float")
            _finite_float(record.get(rate_field), f"{symbol}.{rate_field}")
        if type(record.get("swap_rollover3days")) is not int:
            raise BrokerTermsError(f"{symbol}.swap_rollover3days is not an integer")
        rollover_day = int(record.get("swap_rollover3days"))
        if not 0 <= rollover_day <= 7:
            raise BrokerTermsError(f"{symbol}: invalid rollover weekday")
        factors = record.get("weekday_factors")
        if not isinstance(factors, Mapping) or set(factors) != set(WEEKDAYS):
            raise BrokerTermsError(f"{symbol}: seven direct weekday factors are required")
        normalized_factors: Dict[str, int] = {}
        for weekday in WEEKDAYS:
            if type(factors.get(weekday)) is not int:
                raise BrokerTermsError(f"{symbol}.{weekday} factor is not an integer")
            factor = int(factors.get(weekday))
            if factor not in {0, 1, 3}:
                raise BrokerTermsError(f"{symbol}: unsupported {weekday} swap factor")
            normalized_factors[weekday] = factor
        triple_days = [
            index for index, weekday in enumerate(WEEKDAYS)
            if normalized_factors[weekday] == 3
        ]
        expected_triple_days = [] if rollover_day == 7 else [rollover_day]
        if (
            (swap_mode == 0 and any(normalized_factors.values()))
            or (swap_mode != 0 and triple_days != expected_triple_days)
        ):
            raise BrokerTermsError(f"{symbol}: weekday factors contradict swap metadata")

        anchors = record.get("d1_anchors")
        if not isinstance(anchors, list) or not 1 <= len(anchors) <= 128:
            raise BrokerTermsError(f"{symbol}: D1 anchors are missing")
        epochs: set[int] = set()
        for anchor in anchors:
            if (
                not isinstance(anchor, Mapping)
                or set(anchor) != {"open_epoch", "server_weekday"}
                or type(anchor.get("open_epoch")) is not int
                or type(anchor.get("server_weekday")) is not int
            ):
                raise BrokerTermsError(f"{symbol}: D1 anchor shape is invalid")
            epoch = int(anchor.get("open_epoch"))
            weekday = int(anchor.get("server_weekday"))
            if epoch <= 0 or not 0 <= weekday <= 6 or epoch in epochs:
                raise BrokerTermsError(f"{symbol}: D1 anchor is invalid or duplicated")
            epochs.add(epoch)

    return _snapshot_content_sha256(snapshot)


def _stress_transform(snapshot: Mapping[str, Any]) -> Dict[str, Any]:
    source = json.loads(json.dumps(snapshot, allow_nan=False))
    output = json.loads(json.dumps(snapshot, allow_nan=False))
    output["stress_source_snapshot"] = source
    for symbol, record in dict(output.get("symbols") or {}).items():
        long_rate = _finite_float(record.get("swap_long"), f"{symbol}.swap_long")
        short_rate = _finite_float(record.get("swap_short"), f"{symbol}.swap_short")
        adverse = -max(abs(long_rate), abs(short_rate))
        record["swap_long"] = adverse
        record["swap_short"] = adverse
        record["weekday_factors"] = {weekday: 3 for weekday in WEEKDAYS}
        record["provenance_status"] = STRESS_APPROVED
    output["provenance_status"] = STRESS_APPROVED
    output["stress_policy"] = dict(_STRESS_POLICY)
    output.pop("content_sha256", None)
    output["content_sha256"] = canonical_sha256(output)
    return output


def _validate_terms_snapshot(snapshot: Mapping[str, Any]) -> str:
    provenance = str(snapshot.get("provenance_status") or "")
    if provenance == AUTHENTICATED:
        return _validate_direct_terms_snapshot(snapshot)
    if provenance != STRESS_APPROVED:
        raise BrokerTermsError("terms snapshot is not admissible for activation")
    source = snapshot.get("stress_source_snapshot")
    if not isinstance(source, Mapping):
        raise BrokerTermsError("stress snapshot lacks its authenticated direct-export source")
    _validate_direct_terms_snapshot(source)
    expected = _stress_transform(source)
    if _canonical_bytes(snapshot) != _canonical_bytes(expected):
        raise BrokerTermsError("stress snapshot is not the exact approved source transform")
    return _snapshot_content_sha256(snapshot)


def build_explicit_stress_snapshot(snapshot: Mapping[str, Any]) -> Dict[str, Any]:
    """Create the exact approved debit-only transform of one direct export."""

    _validate_direct_terms_snapshot(snapshot)
    return _stress_transform(snapshot)


def _load_json_bounded_hashed(
    path: Path,
    max_bytes: int = 64 * 1024 * 1024,
) -> Tuple[Dict[str, Any], str, bytes]:
    payload = _read_bounded(path, max_bytes)
    try:
        value = json.loads(payload.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise BrokerTermsError(f"invalid JSON artifact: {path}") from exc
    if not isinstance(value, dict):
        raise BrokerTermsError(f"JSON artifact is not an object: {path}")
    return value, hashlib.sha256(payload).hexdigest(), payload


def _load_json_bounded(path: Path, max_bytes: int = 64 * 1024 * 1024) -> Dict[str, Any]:
    value, _digest, _payload = _load_json_bounded_hashed(path, max_bytes)
    return value


def _price_contract_mode(pointer: Mapping[str, Any], manifest: Mapping[str, Any]) -> str:
    universe = manifest.get("universe_eligibility")
    production_markers_present = (
        "production_authoritative" in pointer
        or any(
            field in manifest
            for field in (
                "production_authoritative",
                "native_is_signal_authority",
                "silent_fallback_allowed",
                "universe_eligibility",
            )
        )
    )
    if not production_markers_present:
        return "legacy-active-v1"
    if (
        pointer.get("production_authoritative") is not True
        or manifest.get("production_authoritative") is not True
        or manifest.get("native_is_signal_authority") is not True
        or manifest.get("silent_fallback_allowed") is not False
        or not isinstance(universe, Mapping)
        or str(universe.get("schema_version") or "") != PRICE_UNIVERSE_V4_SCHEMA
        or universe.get("trading_verdicts_emitted") is not False
        or universe.get("performance_inputs_used") is not False
    ):
        raise BrokerTermsError("native production price authority contract is incomplete")
    counts = manifest.get("counts")
    if (
        not isinstance(counts, Mapping)
        or type(counts.get("pending")) is not int
        or type(counts.get("terminal")) is not int
        or type(counts.get("total")) is not int
        or int(counts.get("pending")) != 0
        or int(counts.get("terminal")) <= 0
        or int(counts.get("terminal")) != int(counts.get("total"))
    ):
        raise BrokerTermsError("native production price authority is not fully terminal")
    if list(universe.get("required_timeframes") or []) != [
        "M5",
        "M15",
        "M30",
        "H1",
        "H4",
        "D1",
    ]:
        raise BrokerTermsError("native production price authority timeframe contract is invalid")
    if not isinstance(universe.get("cell_statuses"), Mapping) or not isinstance(
        universe.get("evidence_candidate_timeframes_by_symbol"), Mapping
    ):
        raise BrokerTermsError("native production price authority eligibility map is missing")
    return "promoted-v4"


def _price_cell_is_admissible(
    price: Mapping[str, Any],
    symbol: str,
    timeframe: str,
    cell: Mapping[str, Any],
) -> bool:
    status = str(cell.get("status") or "").strip().lower()
    if price.get("contract_mode") != "promoted-v4":
        return status == "active"

    manifest = price.get("manifest")
    if not isinstance(manifest, Mapping):
        raise BrokerTermsError("native production price manifest is unavailable")
    universe = manifest.get("universe_eligibility")
    if not isinstance(universe, Mapping):
        raise BrokerTermsError("native production price eligibility contract is unavailable")
    declared_status = str(
        ((universe.get("cell_statuses") or {}).get(symbol) or {}).get(timeframe) or ""
    ).strip().lower()
    if declared_status != status:
        raise BrokerTermsError(
            f"{symbol} {timeframe}: price-cell status contradicts universe eligibility"
        )
    if status in PRICE_CELL_TERMINAL_NONCANDIDATES:
        return False
    if status != PRICE_CELL_EVIDENCE_CANDIDATE:
        raise BrokerTermsError(
            f"{symbol} {timeframe}: unsupported production price-cell status {status!r}"
        )
    candidate_timeframes = list(
        ((universe.get("evidence_candidate_timeframes_by_symbol") or {}).get(symbol) or [])
    )
    if (
        timeframe not in candidate_timeframes
        or str(cell.get("universe_eligibility_status") or "").strip().lower()
        != PRICE_CELL_EVIDENCE_CANDIDATE
        or str(cell.get("symbol") or "") != symbol
        or str(cell.get("timeframe") or "").upper() != timeframe
    ):
        raise BrokerTermsError(
            f"{symbol} {timeframe}: evidence-candidate identity is not authenticated"
        )
    return True


def _same_price_identity(left: Mapping[str, Any], right: Mapping[str, Any]) -> bool:
    return (
        left.get("pointer_bytes") == right.get("pointer_bytes")
        and left.get("manifest_bytes") == right.get("manifest_bytes")
        and left.get("pointer_sha256") == right.get("pointer_sha256")
        and left.get("manifest_sha256") == right.get("manifest_sha256")
        and left.get("activation_id") == right.get("activation_id")
        and left.get("contract_mode") == right.get("contract_mode")
    )


def load_current_price_activation(native_root: Path) -> Dict[str, Any]:
    native_root = Path(native_root).resolve()
    pointer_path = native_root / "ACTIVE.json"
    pointer, pointer_sha, pointer_bytes = _load_json_bounded_hashed(
        pointer_path,
        1024 * 1024,
    )
    if str(pointer.get("schema_version") or "") != "fxpm-native-active-pointer-v1":
        raise BrokerTermsError("native price pointer schema is unsupported")
    manifest_path = _safe_relative(native_root, pointer.get("manifest_relative_path"))
    manifest, manifest_sha, manifest_bytes = _load_json_bounded_hashed(manifest_path)
    if manifest_sha != str(pointer.get("manifest_sha256") or "").lower():
        raise BrokerTermsError("native price manifest hash mismatch")
    if str(manifest.get("schema_version") or "") != "fxpm-native-activation-v1":
        raise BrokerTermsError("native price activation schema is unsupported")
    if str(manifest.get("status") or "").lower() != "active":
        raise BrokerTermsError("native price activation is not active")
    if str(manifest.get("activation_id") or "") != str(pointer.get("activation_id") or ""):
        raise BrokerTermsError("native price activation identity mismatch")
    contract_mode = _price_contract_mode(pointer, manifest)
    if _read_bounded(pointer_path, 1024 * 1024) != pointer_bytes:
        raise BrokerTermsError("native price pointer changed during activation read")
    return {
        "pointer": pointer,
        "pointer_path": pointer_path,
        "pointer_sha256": pointer_sha,
        "pointer_bytes": pointer_bytes,
        "manifest": manifest,
        "manifest_path": manifest_path,
        "manifest_sha256": manifest_sha,
        "manifest_bytes": manifest_bytes,
        "activation_id": str(manifest.get("activation_id") or ""),
        "contract_mode": contract_mode,
    }


def _receipt_backed_price_view(
    native_root: Path,
    authority: Any,
) -> Dict[str, Any]:
    """Adapt the already-verified native receipt scope result."""

    native_root = Path(native_root).resolve()
    pointer_bytes = bytes(authority.active_pointer_bytes)
    manifest_bytes = bytes(authority.activation_manifest_bytes)
    pointer_sha = str(authority.active_pointer_sha256 or "").lower()
    manifest_sha = str(authority.activation_manifest_sha256 or "").lower()
    receipt_sha = str(authority.semantic_receipt_sha256 or "").lower()
    try:
        pointer = json.loads(pointer_bytes.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise BrokerTermsError("receipt-backed native pointer JSON is invalid") from exc
    manifest = authority.manifest
    entries = authority.entries
    receipt = authority.semantic_receipt
    if (
        not isinstance(pointer, Mapping)
        or not isinstance(manifest, Mapping)
        or not isinstance(entries, Mapping)
        or not isinstance(receipt, Mapping)
        or manifest.get("entries") != entries
    ):
        raise BrokerTermsError("receipt-backed native authority payload is invalid")
    activation_id = str(authority.activation_id or "")
    manifest_path = Path(authority.activation_manifest_path).resolve()
    receipt_path = Path(authority.semantic_receipt_path).resolve()
    expected_manifest_path = _safe_relative(
        native_root,
        pointer.get("manifest_relative_path"),
    )
    expected_receipt_path = _safe_relative(
        native_root,
        manifest.get("semantic_receipt_relative_path"),
    )
    if (
        Path(authority.data_dir).resolve() != native_root.parent
        or str(pointer.get("activation_id") or "") != activation_id
        or str(pointer.get("manifest_sha256") or "").lower() != manifest_sha
        or expected_manifest_path != manifest_path
        or str(manifest.get("activation_id") or "") != activation_id
        or str(manifest.get("semantic_receipt_sha256") or "").lower() != receipt_sha
        or expected_receipt_path != receipt_path
        or str(receipt.get("activation_id") or "") != activation_id
    ):
        raise BrokerTermsError("receipt-backed native authority identity mismatch")
    contract_mode = _price_contract_mode(pointer, manifest)
    return {
        "pointer": dict(pointer),
        "pointer_path": native_root / "ACTIVE.json",
        "pointer_sha256": pointer_sha,
        "pointer_bytes": pointer_bytes,
        "manifest": manifest,
        "manifest_path": manifest_path,
        "manifest_sha256": manifest_sha,
        "manifest_bytes": manifest_bytes,
        "activation_id": activation_id,
        "contract_mode": contract_mode,
        "semantic_receipt": receipt,
        "semantic_receipt_path": receipt_path,
        "semantic_receipt_sha256": receipt_sha,
    }


@contextmanager
def _receipt_backed_price_authority_scope(
    native_root: Path,
) -> Iterator[Tuple[Dict[str, Any], Callable[[], None]]]:
    """Open the sole receipt-backed price authority scope for one operation."""

    from pm_native_activation import (
        production_native_authority_verification_scope,
    )

    native_root = Path(native_root).resolve()
    try:
        with production_native_authority_verification_scope() as scope:
            authority = scope.verify_receipt_backed_authority(native_root.parent)
            price = _receipt_backed_price_view(native_root, authority)

            def revalidate() -> None:
                try:
                    scope.revalidate_receipt_backed_authority()
                except RuntimeError as exc:
                    raise BrokerTermsError(
                        "receipt-backed native price authority changed"
                    ) from exc

            yield price, revalidate
    except BrokerTermsError:
        raise
    except RuntimeError as exc:
        raise BrokerTermsError(
            "receipt-backed native price authority is invalid"
        ) from exc


def _read_interval_axis_hashed(
    csv_path: Path,
    *,
    label: str,
    max_rows: int,
    max_bytes: int,
    expected_sha256: Optional[str],
    axis_cache: Optional[
        Dict[Path, Tuple[pd.DatetimeIndex, pd.DatetimeIndex, str]]
    ] = None,
) -> Tuple[pd.DatetimeIndex, pd.DatetimeIndex, str]:
    """Hash and validate one exact native interval axis from the same bytes."""

    path = Path(csv_path).resolve()
    cached = axis_cache.get(path) if axis_cache is not None else None
    if cached is not None:
        opens, closes, observed_sha256 = cached
        if (
            expected_sha256 is not None
            and observed_sha256 != str(expected_sha256).lower()
        ):
            raise BrokerTermsError(
                f"{label} bars hash mismatch before schedule derivation"
            )
        if len(opens) <= 0 or len(opens) > int(max_rows):
            raise BrokerTermsError(f"{label} row count is outside the permitted bound")
        return opens, closes, observed_sha256
    payload = _read_bounded(path, int(max_bytes))
    observed_sha256 = hashlib.sha256(payload).hexdigest()
    if expected_sha256 is not None and observed_sha256 != str(expected_sha256).lower():
        raise BrokerTermsError(f"{label} bars hash mismatch before schedule derivation")
    try:
        frame = pd.read_csv(
            io.BytesIO(payload),
            usecols=["time", "_BAR_CLOSE_TS"],
        )
    except Exception as exc:
        raise BrokerTermsError(
            f"{label} artifact lacks the required interval columns"
        ) from exc
    if len(frame) <= 0 or len(frame) > int(max_rows):
        raise BrokerTermsError(f"{label} row count is outside the permitted bound")
    opens = pd.DatetimeIndex(pd.to_datetime(frame["time"], utc=True, errors="coerce"))
    closes = pd.DatetimeIndex(
        pd.to_datetime(frame["_BAR_CLOSE_TS"], utc=True, errors="coerce")
    )
    if (
        opens.isna().any()
        or closes.isna().any()
        or opens.has_duplicates
        or closes.has_duplicates
        or not opens.is_monotonic_increasing
        or not closes.is_monotonic_increasing
        or not bool((closes > opens).all())
    ):
        raise BrokerTermsError(f"{label} interval axis is invalid")
    if axis_cache is not None:
        axis_cache[path] = (opens, closes, observed_sha256)
    return opens, closes, observed_sha256


def _bind_session_anchors(
    opens: pd.DatetimeIndex,
    *,
    symbol_terms: Mapping[str, Any],
    require_single_phase: bool,
    allow_represented_session: bool = False,
    closes: Optional[pd.DatetimeIndex] = None,
) -> Tuple[int, int, Optional[int]]:
    """Bind direct-export broker weekdays to an authenticated price axis.

    D1 bars bind by exact open.  Intraday sources may legitimately omit the
    first bar of a broker day, so they bind when the authenticated anchor's
    broker-day bucket is represented by at least one source bar.  The session
    phase and weekday offset still come only from the direct-export anchors.
    """

    open_epochs = {int(value.timestamp()) for value in opens}
    offsets: set[int] = set()
    phases: set[int] = set()
    matched = 0
    anchors: list[Tuple[int, int]] = []
    for anchor in list(symbol_terms.get("d1_anchors") or []):
        epoch = _strict_int(anchor.get("open_epoch"), "anchor.open_epoch")
        weekday = _strict_int(anchor.get("server_weekday"), "anchor.server_weekday")
        anchors.append((epoch, weekday))
        if allow_represented_session:
            offsets.add(
                (
                    weekday
                    - ((pd.Timestamp(epoch, unit="s", tz="UTC").weekday() + 1) % 7)
                )
                % 7
            )
            phases.add(epoch % 86_400)
        elif epoch not in open_epochs:
            continue
        else:
            opened = pd.Timestamp(epoch, unit="s", tz="UTC")
            utc_weekday_mql5 = (opened.weekday() + 1) % 7
            offsets.add((weekday - utc_weekday_mql5) % 7)
            phases.add(epoch % 86_400)
            matched += 1

    if allow_represented_session:
        if len(phases) != 1 or len(offsets) != 1:
            raise BrokerTermsError(
                "broker-day anchors do not bind one intraday session phase contract"
            )
        if closes is None or len(closes) != len(opens):
            raise BrokerTermsError(
                "intraday anchor binding requires the exact close axis"
            )
        phase = next(iter(phases))
        represented_sessions = set(
            _completed_or_represented_session_epochs(
                opens,
                phase_seconds=phase,
            )
        )
        first_open_epoch = int(opens.asi8[0] // 1_000_000_000)
        last_close_epoch = int(closes.asi8[-1] // 1_000_000_000)
        overlapping_anchors = [
            epoch
            for epoch, _weekday in anchors
            if epoch < last_close_epoch and epoch + 86_400 > first_open_epoch
        ]
        if not overlapping_anchors:
            raise ScheduleSourceUnsuitableError(
                "ANCHOR_INTERVAL_NOT_OVERLAPPED",
                "broker-day anchors do not overlap the source interval"
            )
        missing_anchors = sorted(
            set(overlapping_anchors) - represented_sessions
        )
        if missing_anchors:
            raise ScheduleSourceUnsuitableError(
                "ANCHOR_SESSION_UNREPRESENTED",
                "source does not represent every overlapping broker-day anchor"
            )
        matched = len(overlapping_anchors)
    else:
        phase = next(iter(phases)) if len(phases) == 1 else None

    if matched <= 0 or len(offsets) != 1:
        raise BrokerTermsError(
            "broker-weekday anchors do not bind uniquely to the active artifact"
        )
    if require_single_phase and len(phases) != 1:
        raise BrokerTermsError(
            "broker-day anchors do not bind one intraday session phase"
        )
    return next(iter(offsets)), matched, phase


def _completed_or_represented_session_epochs(
    opens: pd.DatetimeIndex,
    *,
    phase_seconds: int,
    closes: Optional[pd.DatetimeIndex] = None,
) -> np.ndarray:
    """Return represented broker days, optionally restricted to completed days."""

    phase = int(phase_seconds)
    if not 0 <= phase < 86_400:
        raise BrokerTermsError("broker-day phase is outside one UTC day")
    open_epochs = opens.asi8 // 1_000_000_000
    sessions = np.unique(
        ((open_epochs - phase) // 86_400) * 86_400 + phase
    )
    if closes is None:
        return sessions
    if len(closes) != len(opens) or len(closes) <= 0:
        raise BrokerTermsError("session source close axis is invalid")
    last_verified_close_epoch = int(closes.asi8[-1] // 1_000_000_000)
    return sessions[sessions + 86_400 <= last_verified_close_epoch]


def _derive_d1_session_events_hashed(
    d1_csv_path: Path,
    *,
    symbol_terms: Mapping[str, Any],
    max_rows: int = 100_000,
    max_bytes: int = 256 * 1024 * 1024,
    expected_sha256: Optional[str] = None,
    axis_cache: Optional[
        Dict[Path, Tuple[pd.DatetimeIndex, pd.DatetimeIndex, str]]
    ] = None,
) -> Tuple[list[Dict[str, Any]], str]:
    """Hash and parse one bounded D1 payload from the same exact bytes."""

    opens, closes, observed_sha256 = _read_interval_axis_hashed(
        d1_csv_path,
        label="D1",
        max_rows=max_rows,
        max_bytes=max_bytes,
        expected_sha256=expected_sha256,
        axis_cache=axis_cache,
    )
    weekday_offset, _matched, _phase = _bind_session_anchors(
        opens,
        symbol_terms=symbol_terms,
        require_single_phase=False,
    )
    factors = dict(symbol_terms.get("weekday_factors") or {})
    if set(factors) != set(WEEKDAYS):
        raise BrokerTermsError("seven weekday factors are required")
    events: list[Dict[str, Any]] = []
    for index, (opened, closed) in enumerate(zip(opens, closes)):
        utc_mql5_weekday = (opened.weekday() + 1) % 7
        from_weekday = (utc_mql5_weekday + weekday_offset) % 7
        weekday_name = WEEKDAYS[from_weekday]
        factor = _strict_int(factors[weekday_name], f"weekday_factors.{weekday_name}")
        if factor not in {0, 1, 3}:
            raise BrokerTermsError("session factor is outside the official factor set")
        events.append({
            "d1_bar_index": index,
            "d1_open_utc": opened.isoformat(),
            "event_utc": closed.isoformat(),
            "from_weekday": from_weekday,
            "from_weekday_name": weekday_name,
            "factor": factor,
        })
    return events, observed_sha256


def _derive_intraday_session_events_hashed(
    source_csv_path: Path,
    *,
    source_timeframe: str,
    symbol_terms: Mapping[str, Any],
    max_rows: int = 2_000_000,
    max_bytes: int = 256 * 1024 * 1024,
    expected_sha256: Optional[str] = None,
    axis_cache: Optional[
        Dict[Path, Tuple[pd.DatetimeIndex, pd.DatetimeIndex, str]]
    ] = None,
) -> Tuple[list[Dict[str, Any]], str, Dict[str, Any]]:
    """Derive completed broker-day boundaries from one exact intraday axis.

    A broker-day event is tied to the authenticated D1 anchor phase, not to
    whichever intraday bar happens to be last before a holiday close.  A
    session is emitted only when its expected boundary is within the verified
    source axis, so a trailing partial day cannot manufacture a future event.
    """

    timeframe = str(source_timeframe or "").strip().upper()
    if timeframe not in _SCHEDULE_SOURCE_PREFERENCE[1:]:
        raise BrokerTermsError(
            f"{timeframe}: unsupported intraday schedule-source timeframe"
        )
    opens, closes, observed_sha256 = _read_interval_axis_hashed(
        source_csv_path,
        label=timeframe,
        max_rows=max_rows,
        max_bytes=max_bytes,
        expected_sha256=expected_sha256,
        axis_cache=axis_cache,
    )
    weekday_offset, matched, phase_seconds = _bind_session_anchors(
        opens,
        symbol_terms=symbol_terms,
        require_single_phase=True,
        allow_represented_session=True,
        closes=closes,
    )
    if phase_seconds is None:
        raise BrokerTermsError(
            f"{timeframe} schedule source has no authenticated session phase"
        )

    factors = dict(symbol_terms.get("weekday_factors") or {})
    if set(factors) != set(WEEKDAYS):
        raise BrokerTermsError("seven weekday factors are required")
    completed_sessions = _completed_or_represented_session_epochs(
        opens,
        closes=closes,
        phase_seconds=int(phase_seconds),
    )
    if len(completed_sessions) <= 0:
        raise ScheduleSourceUnsuitableError(
            "NO_COMPLETED_SOURCE_SESSION",
            f"{timeframe} schedule source has no completed broker day"
        )

    events: list[Dict[str, Any]] = []
    for index, session_epoch in enumerate(completed_sessions.tolist()):
        opened = pd.Timestamp(int(session_epoch), unit="s", tz="UTC")
        event = pd.Timestamp(int(session_epoch) + 86_400, unit="s", tz="UTC")
        utc_mql5_weekday = (opened.weekday() + 1) % 7
        from_weekday = (utc_mql5_weekday + weekday_offset) % 7
        weekday_name = WEEKDAYS[from_weekday]
        factor = _strict_int(factors[weekday_name], f"weekday_factors.{weekday_name}")
        if factor not in {0, 1, 3}:
            raise BrokerTermsError("session factor is outside the official factor set")
        events.append({
            "session_index": index,
            "session_open_utc": opened.isoformat(),
            "event_utc": event.isoformat(),
            "from_weekday": from_weekday,
            "from_weekday_name": weekday_name,
            "factor": factor,
        })
    return events, observed_sha256, {
        "matched_anchor_count": int(matched),
        "phase_seconds": int(phase_seconds),
        "source_first_open_utc": opens[0].isoformat(),
        "source_last_close_utc": closes[-1].isoformat(),
        "source_row_count": int(len(opens)),
        "weekday_offset": int(weekday_offset),
    }


def derive_d1_session_events(
    d1_csv_path: Path,
    *,
    symbol_terms: Mapping[str, Any],
    max_rows: int = 100_000,
    max_bytes: int = 256 * 1024 * 1024,
) -> list[Dict[str, Any]]:
    """Derive broker-day rollover events exclusively from immutable D1 intervals."""

    events, _digest = _derive_d1_session_events_hashed(
        d1_csv_path,
        symbol_terms=symbol_terms,
        max_rows=max_rows,
        max_bytes=max_bytes,
    )
    return events


def project_session_events(
    events: Sequence[Any],
    target_frame: pd.DataFrame,
) -> SwapProjection:
    """Lazily map D1 events to pre-open and at-close signed-unit arrays."""
    if not isinstance(target_frame, pd.DataFrame) or "_BAR_CLOSE_TS" not in target_frame.columns:
        raise BrokerTermsError("target frame requires an explicit _BAR_CLOSE_TS column")
    opens = pd.DatetimeIndex(pd.to_datetime(target_frame.index, utc=True, errors="coerce"))
    closes = pd.DatetimeIndex(pd.to_datetime(target_frame["_BAR_CLOSE_TS"], utc=True, errors="coerce"))
    count = len(target_frame)
    if (
        len(closes) != count
        or opens.isna().any()
        or closes.isna().any()
        or opens.has_duplicates
        or closes.has_duplicates
        or not opens.is_monotonic_increasing
        or not closes.is_monotonic_increasing
        or not bool((closes > opens).all())
    ):
        raise BrokerTermsError("target interval axis is invalid")
    pre = np.zeros(count, dtype=np.int16)
    close = np.zeros(count, dtype=np.int16)
    assignments: list[Tuple[str, int, int]] = []
    for record in events:
        if isinstance(record, Mapping):
            raw_event = record.get("event_utc")
            raw_factor = record.get("factor")
        elif isinstance(record, (list, tuple)) and len(record) == 2:
            raw_event, raw_factor = record
        else:
            raise BrokerTermsError("rollover event record is malformed")
        factor = _strict_int(raw_factor, "event.factor")
        if factor == 0 or count == 0:
            continue
        event = pd.Timestamp(raw_event)
        event = event.tz_localize("UTC") if event.tzinfo is None else event.tz_convert("UTC")
        # A sliced train/validation frame starts with no inherited position.
        # Historical events at or before its first open must not accumulate on
        # that first bar; only events inside the observable target axis (or a
        # gap between two observed bars) can charge a carried position.
        if event <= opens[0] or event > closes[-1]:
            continue
        located = int(closes.searchsorted(event, side="left"))
        if located >= count:
            continue
        if opens[located] < event <= closes[located]:
            close[located] += factor
            assignments.append(("close", located, factor))
        elif event <= opens[located]:
            pre[located] += factor
            assignments.append(("pre_open", located, factor))
        else:
            raise BrokerTermsError("rollover event cannot be placed on the target axis")
    if (pre > np.iinfo(np.int8).max).any() or (close > np.iinfo(np.int8).max).any():
        raise BrokerTermsError("projected rollover units overflow int8")
    pre8 = pre.astype(np.int8)
    close8 = close.astype(np.int8)
    digest = hashlib.sha256()
    digest.update(SWAP_PROJECTION_CONTRACT.encode("ascii"))
    digest.update(pre8.tobytes())
    digest.update(close8.tobytes())
    digest.update(_canonical_bytes(assignments))
    return SwapProjection(pre8, close8, digest.hexdigest(), len(assignments))


def _write_or_verify_immutable_json(path: Path, value: Mapping[str, Any]) -> str:
    if path.exists():
        observed = _load_json_bounded(path)
        if observed != dict(value):
            raise BrokerTermsError(f"immutable artifact collision: {path}")
    else:
        _atomic_create_json(path, value)
    return sha256_file(path)


def _price_cell_binding_from_verified_manifest(
    native_root: Path,
    cell: Mapping[str, Any],
    *,
    label: str,
) -> Dict[str, str]:
    """Copy one receipt-verified cell binding without reopening its leaves."""

    binding: Dict[str, str] = {}
    for artifact in ("bars", "sidecar"):
        relative_field = f"{artifact}_relative_path"
        sha_field = f"{artifact}_sha256"
        path = _safe_relative(native_root, cell.get(relative_field))
        expected = str(cell.get(sha_field) or "").strip().lower()
        if not _is_sha256(expected):
            raise BrokerTermsError(f"{label}: {artifact} pin is invalid")
        normalized = path.relative_to(Path(native_root).resolve()).as_posix()
        if str(cell.get(relative_field) or "").replace("\\", "/") != normalized:
            raise BrokerTermsError(f"{label}: {artifact} path is not canonical")
        binding[relative_field] = normalized
        binding[sha_field] = expected
    return binding


def _verified_price_cell_axis(
    native_root: Path,
    cell: Mapping[str, Any],
    *,
    label: str,
    phase_seconds: int,
    axis_cache: Optional[
        Dict[Path, Tuple[pd.DatetimeIndex, pd.DatetimeIndex, str]]
    ] = None,
) -> Dict[str, Any]:
    """Reopen one exact target axis for completed-session coverage proof."""

    path = _safe_relative(native_root, cell.get("bars_relative_path"))
    expected = str(cell.get("bars_sha256") or "").strip().lower()
    if not _is_sha256(expected):
        raise BrokerTermsError(f"{label}: bars pin is invalid")
    opens, closes, observed = _read_interval_axis_hashed(
        path,
        label=label,
        max_rows=2_000_000,
        max_bytes=256 * 1024 * 1024,
        expected_sha256=expected,
        axis_cache=axis_cache,
    )
    completed_sessions = _completed_or_represented_session_epochs(
        opens,
        closes=closes,
        phase_seconds=phase_seconds,
    )
    return {
        "first_open_utc": opens[0].isoformat(),
        "last_close_utc": closes[-1].isoformat(),
        "row_count": int(len(opens)),
        "completed_session_epochs": tuple(
            int(value) for value in completed_sessions.tolist()
        ),
    }


def _verify_exact_artifact(path: Path, expected_sha256: str, label: str) -> None:
    try:
        observed = sha256_file(path)
    except OSError as exc:
        raise BrokerTermsError(f"{label} is missing or unreadable") from exc
    if observed != str(expected_sha256 or "").lower():
        raise BrokerTermsError(f"{label} hash mismatch")


def _validated_schedule_axis_bindings(
    native_root: Path,
    raw_bindings: Any,
) -> list[Tuple[Path, str]]:
    if not isinstance(raw_bindings, list) or not raw_bindings:
        raise BrokerTermsError("terms schedule-axis binding set is invalid")
    normalized: list[Tuple[Path, str]] = []
    rows: list[Dict[str, str]] = []
    for raw in raw_bindings:
        if not isinstance(raw, Mapping) or set(raw) != {
            "bars_relative_path",
            "bars_sha256",
        }:
            raise BrokerTermsError("terms schedule-axis binding is invalid")
        path = _safe_relative(native_root, raw.get("bars_relative_path"))
        relative = path.relative_to(Path(native_root).resolve()).as_posix()
        expected = str(raw.get("bars_sha256") or "").strip().lower()
        row = {
            "bars_relative_path": relative,
            "bars_sha256": expected,
        }
        if str(raw.get("bars_relative_path") or "").replace(
            "\\", "/"
        ) != relative or not _is_sha256(expected):
            raise BrokerTermsError("terms schedule-axis binding is invalid")
        rows.append(row)
        normalized.append((path, expected))
    if rows != sorted(
        rows,
        key=lambda value: (value["bars_relative_path"], value["bars_sha256"]),
    ) or len({path for path, _sha in normalized}) != len(normalized):
        raise BrokerTermsError("terms schedule-axis binding set is not canonical")
    return normalized


def _rehash_schedule_axis_bindings(
    native_root: Path,
    raw_bindings: Any,
) -> None:
    for path, expected in _validated_schedule_axis_bindings(
        native_root,
        raw_bindings,
    ):
        _verify_exact_artifact(path, expected, "staged price schedule axis")


def _load_active_terms_source(native_root: Path) -> Dict[str, Any]:
    """Load one exact pointer/manifest/snapshot TERMS authority."""

    native_root = Path(native_root).resolve()
    pointer_path = native_root / "terms" / "ACTIVE.json"
    pointer, pointer_sha, pointer_bytes = _load_json_bounded_hashed(
        pointer_path,
        1024 * 1024,
    )
    if str(pointer.get("schema_version") or "") != TERMS_ACTIVE_POINTER_SCHEMA:
        raise BrokerTermsError("active terms pointer schema is invalid")
    manifest_path = _safe_relative(native_root, pointer.get("manifest_relative_path"))
    manifest, manifest_sha, manifest_bytes = _load_json_bounded_hashed(manifest_path)
    if (
        manifest_sha != str(pointer.get("manifest_sha256") or "").lower()
        or str(manifest.get("schema_version") or "") != TERMS_ACTIVATION_SCHEMA
        or str(manifest.get("status") or "").lower() != "active"
        or str(manifest.get("activation_id") or "")
        != str(pointer.get("activation_id") or "")
    ):
        raise BrokerTermsError("active terms activation is invalid")
    snapshot_path = _safe_relative(
        native_root,
        manifest.get("terms_snapshot_relative_path"),
    )
    snapshot, snapshot_sha, snapshot_bytes = _load_json_bounded_hashed(snapshot_path)
    if snapshot_sha != str(manifest.get("terms_snapshot_sha256") or "").lower():
        raise BrokerTermsError("active terms snapshot binding is invalid")
    if (
        _read_bounded(pointer_path, 1024 * 1024) != pointer_bytes
        or _read_bounded(manifest_path, 64 * 1024 * 1024) != manifest_bytes
        or _read_bounded(snapshot_path, 64 * 1024 * 1024) != snapshot_bytes
    ):
        raise BrokerTermsError("active terms authority changed during validation")
    return {
        "pointer": pointer,
        "pointer_path": pointer_path,
        "pointer_sha256": pointer_sha,
        "pointer_bytes": pointer_bytes,
        "manifest": manifest,
        "manifest_path": manifest_path,
        "manifest_sha256": manifest_sha,
        "manifest_bytes": manifest_bytes,
        "snapshot": snapshot,
        "snapshot_path": snapshot_path,
        "snapshot_sha256": snapshot_sha,
        "snapshot_bytes": snapshot_bytes,
    }


def _validated_active_terms_repair_source(
    native_root: Path,
    *,
    expected_pointer_sha256: str,
    snapshot: Mapping[str, Any],
    price: Mapping[str, Any],
) -> Dict[str, Any]:
    """Authenticate the exact active overlay whose stale snapshot is repaired."""

    expected = str(expected_pointer_sha256 or "").strip().lower()
    if not _is_sha256(expected):
        raise BrokerTermsError("repair source terms pointer pin is invalid")
    source = _load_active_terms_source(native_root)
    if source["pointer_sha256"] != expected:
        raise BrokerTermsError("repair source terms pointer pin mismatch")
    pointer = source["pointer"]
    manifest = source["manifest"]
    if (
        str(manifest.get("price_activation_manifest_sha256") or "")
        != price.get("manifest_sha256")
        or str(pointer.get("price_activation_manifest_sha256") or "")
        != price.get("manifest_sha256")
        or str(manifest.get("price_activation_id") or "") != price.get("activation_id")
        or str(manifest.get("price_active_pointer_sha256_at_creation") or "").lower()
        != price.get("pointer_sha256")
    ):
        raise BrokerTermsError("repair source is bound to a different price activation")
    persisted_snapshot = source["snapshot"]
    if (
        _canonical_bytes(persisted_snapshot) != _canonical_bytes(snapshot)
        or persisted_snapshot.get("provenance_status")
        != manifest.get("provenance_status")
    ):
        raise BrokerTermsError("repair source terms snapshot binding is invalid")
    return {
        "activation_id": str(manifest.get("activation_id") or ""),
        "manifest_sha256": source["manifest_sha256"],
        "pointer_sha256": source["pointer_sha256"],
        "pointer_bytes": source["pointer_bytes"],
    }


def build_price_receipt_migration_pin(
    native_root: Path,
    *,
    prepared_at_utc: Optional[datetime] = None,
) -> Dict[str, Any]:
    """Pin the exact active legacy TERMS overlay before the price migration."""

    native_root = Path(native_root).resolve()
    with _terms_pointer_lock(native_root):
        with _native_price_pointer_lock(native_root):
            price = load_current_price_activation(native_root)
            if (
                price["manifest"].get("semantic_receipt_relative_path") is not None
                or price["manifest"].get("semantic_receipt_sha256") is not None
            ):
                raise BrokerTermsError(
                    "price receipt migration predecessor is already receipt-backed"
                )
            source = _load_active_terms_source(native_root)
            pointer = source["pointer"]
            manifest = source["manifest"]
            snapshot = source["snapshot"]
            _validate_terms_snapshot(snapshot)
            if (
                str(manifest.get("price_activation_manifest_sha256") or "").lower()
                != price["manifest_sha256"]
                or str(pointer.get("price_activation_manifest_sha256") or "").lower()
                != price["manifest_sha256"]
                or str(manifest.get("price_activation_id") or "")
                != price["activation_id"]
                or str(
                    manifest.get("price_active_pointer_sha256_at_creation") or ""
                ).lower()
                != price["pointer_sha256"]
            ):
                raise BrokerTermsError(
                    "price receipt migration source binding is invalid"
                )
            final_price = load_current_price_activation(native_root)
            if not _same_price_identity(price, final_price):
                raise BrokerTermsError(
                    "native price authority changed while preparing migration pin"
                )
            if (
                _read_bounded(source["pointer_path"], 1024 * 1024)
                != source["pointer_bytes"]
            ):
                raise BrokerTermsError(
                    "broker terms authority changed while preparing migration pin"
                )
            prepared = (prepared_at_utc or _utc_now()).astimezone(timezone.utc)
            return {
                "schema_version": TERMS_PRICE_RECEIPT_MIGRATION_PIN_SCHEMA,
                "prepared_at_utc": _iso(prepared),
                "source_terms_active_pointer_sha256": source["pointer_sha256"],
                "source_terms_activation_id": str(
                    manifest.get("activation_id") or ""
                ),
                "source_terms_manifest_relative_path": source[
                    "manifest_path"
                ].relative_to(native_root).as_posix(),
                "source_terms_manifest_sha256": source["manifest_sha256"],
                "source_terms_snapshot_relative_path": source[
                    "snapshot_path"
                ].relative_to(native_root).as_posix(),
                "source_terms_snapshot_sha256": source["snapshot_sha256"],
                "predecessor_price_active_pointer_sha256": price["pointer_sha256"],
                "predecessor_price_activation_id": price["activation_id"],
                "predecessor_price_manifest_sha256": price["manifest_sha256"],
            }


def _validated_price_receipt_migration_source(
    native_root: Path,
    *,
    pin: Mapping[str, Any],
    snapshot: Mapping[str, Any],
    price: Mapping[str, Any],
) -> Dict[str, Any]:
    """Authenticate one exact prepared TERMS source after price ACTIVE moved."""

    if (
        not isinstance(pin, Mapping)
        or set(pin) != _TERMS_PRICE_RECEIPT_MIGRATION_PIN_FIELDS
        or pin.get("schema_version") != TERMS_PRICE_RECEIPT_MIGRATION_PIN_SCHEMA
    ):
        raise BrokerTermsError("price receipt migration pin is invalid")
    _aware_utc(pin.get("prepared_at_utc"), "migration_pin.prepared_at_utc")
    for field in (
        "source_terms_active_pointer_sha256",
        "source_terms_manifest_sha256",
        "source_terms_snapshot_sha256",
        "predecessor_price_active_pointer_sha256",
        "predecessor_price_manifest_sha256",
    ):
        if not _is_sha256(pin.get(field)):
            raise BrokerTermsError("price receipt migration pin digest is invalid")
    if (
        not _is_sha256(price.get("semantic_receipt_sha256"))
        or price.get("pointer_sha256")
        == pin.get("predecessor_price_active_pointer_sha256")
        or price.get("manifest_sha256") == pin.get("predecessor_price_manifest_sha256")
        or not isinstance(price.get("manifest"), Mapping)
        or str(
            price["manifest"].get("previous_active_pointer_sha256") or ""
        ).lower()
        != pin.get("predecessor_price_active_pointer_sha256")
    ):
        raise BrokerTermsError(
            "price receipt migration requires the direct receipt-backed price successor"
        )
    native_root = Path(native_root).resolve()
    source = _load_active_terms_source(native_root)
    pointer = source["pointer"]
    manifest = source["manifest"]
    persisted_snapshot = source["snapshot"]
    if (
        source["pointer_sha256"] != pin.get("source_terms_active_pointer_sha256")
        or str(pointer.get("activation_id") or "")
        != pin.get("source_terms_activation_id")
        or source["manifest_path"].relative_to(native_root).as_posix()
        != pin.get("source_terms_manifest_relative_path")
        or source["manifest_sha256"] != pin.get("source_terms_manifest_sha256")
        or source["snapshot_path"].relative_to(native_root).as_posix()
        != pin.get("source_terms_snapshot_relative_path")
        or source["snapshot_sha256"] != pin.get("source_terms_snapshot_sha256")
        or _canonical_bytes(persisted_snapshot) != _canonical_bytes(snapshot)
        or str(manifest.get("price_activation_id") or "")
        != pin.get("predecessor_price_activation_id")
        or str(manifest.get("price_activation_manifest_sha256") or "").lower()
        != pin.get("predecessor_price_manifest_sha256")
        or str(pointer.get("price_activation_manifest_sha256") or "").lower()
        != pin.get("predecessor_price_manifest_sha256")
        or str(manifest.get("price_active_pointer_sha256_at_creation") or "").lower()
        != pin.get("predecessor_price_active_pointer_sha256")
    ):
        raise BrokerTermsError("price receipt migration source pin mismatch")
    _validate_terms_snapshot(persisted_snapshot)
    return {
        "activation_id": str(manifest.get("activation_id") or ""),
        "manifest_sha256": source["manifest_sha256"],
        "pointer_sha256": source["pointer_sha256"],
        "pointer_bytes": source["pointer_bytes"],
        "predecessor_price_activation_id": pin["predecessor_price_activation_id"],
        "predecessor_price_manifest_sha256": pin["predecessor_price_manifest_sha256"],
        "predecessor_price_pointer_sha256": pin[
            "predecessor_price_active_pointer_sha256"
        ],
    }


def _activate_broker_terms_with_price_authority(
    native_root: Path,
    snapshot: Mapping[str, Any],
    *,
    price: Mapping[str, Any],
    revalidate_price_authority: Callable[[], None],
    activation_id: Optional[str] = None,
    now_utc: Optional[datetime] = None,
    update_pointer: bool = True,
    max_snapshot_age_seconds: int = 3600,
    repair_source_terms_pointer_sha256: Optional[str] = None,
    price_receipt_migration_pin: Optional[Mapping[str, Any]] = None,
    staged_manifest_created_at_utc: Optional[datetime] = None,
) -> Dict[str, Any]:
    """Bind terms schedules to the current immutable price activation."""
    activation_started_monotonic = time.monotonic()
    native_root = Path(native_root).resolve()
    provenance = str(snapshot.get("provenance_status") or "")
    declared_content_sha = _validate_terms_snapshot(snapshot)
    now = (now_utc or _utc_now()).astimezone(timezone.utc)
    manifest_created_at = (
        _aware_utc(
            staged_manifest_created_at_utc,
            "staged_manifest_created_at_utc",
        )
        if staged_manifest_created_at_utc is not None
        else now
    )
    capture_started = _aware_utc(
        snapshot.get("capture_started_at_utc"),
        "snapshot.capture_started_at_utc",
    )
    roster = list(snapshot.get("roster") or [])
    symbols_payload = dict(snapshot.get("symbols") or {})
    if (
        repair_source_terms_pointer_sha256 is not None
        and price_receipt_migration_pin is not None
    ):
        raise BrokerTermsError(
            "ordinary repair and price receipt migration are mutually exclusive"
        )
    if (
        price_receipt_migration_pin is not None
        and staged_manifest_created_at_utc is not None
    ):
        raise BrokerTermsError(
            "price receipt migration cannot promote a pre-staged terms activation"
        )
    repair_source: Optional[Dict[str, Any]] = None
    migration_source: Optional[Dict[str, Any]] = None
    if price_receipt_migration_pin is not None:
        migration_source = _validated_price_receipt_migration_source(
            native_root,
            pin=price_receipt_migration_pin,
            snapshot=snapshot,
            price=price,
        )
    elif repair_source_terms_pointer_sha256 is None:
        _require_snapshot_fresh(capture_started, now, max_snapshot_age_seconds)
    else:
        repair_source = _validated_active_terms_repair_source(
            native_root,
            expected_pointer_sha256=repair_source_terms_pointer_sha256,
            snapshot=snapshot,
            price=price,
        )
    activation_id = str(activation_id or f"{now.strftime('%Y%m%dT%H%M%SZ')}-{secrets.token_hex(4)}")
    if not activation_id or any(character not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_" for character in activation_id):
        raise BrokerTermsError("terms activation id is unsafe")
    terms_root = native_root / "terms"
    terms_pointer_path = terms_root / "ACTIVE.json"
    expected_terms_pointer_bytes: Optional[bytes] = None
    if update_pointer:
        with _terms_pointer_lock(native_root):
            expected_terms_pointer_bytes = _optional_pointer_bytes(terms_pointer_path)
            if (
                repair_source is not None
                and expected_terms_pointer_bytes != repair_source["pointer_bytes"]
            ):
                raise BrokerTermsError(
                    "repair source terms pointer changed before staging"
                )
            if (
                migration_source is not None
                and expected_terms_pointer_bytes != migration_source["pointer_bytes"]
            ):
                raise BrokerTermsError(
                    "price receipt migration source changed before staging"
                )
    snapshot_id = declared_content_sha[:24]
    snapshot_path = terms_root / "snapshots" / f"{snapshot_id}.json"
    snapshot_sha = _write_or_verify_immutable_json(snapshot_path, dict(snapshot))

    raw_entries = price["manifest"].get("entries")
    if not isinstance(raw_entries, Mapping):
        raise BrokerTermsError("native price manifest has no symbol entries")
    entries = dict(raw_entries)
    price_symbol_index = _casefold_key_map(entries, "native price symbols")
    snapshot_symbol_index = _casefold_key_map(symbols_payload, "terms snapshot symbols")
    if set(price_symbol_index) != set(snapshot_symbol_index):
        raise BrokerTermsError("terms snapshot roster does not exactly match price authority")

    symbol_results: Dict[str, Any] = {}
    unscheduled_candidate_cells: list[Dict[str, str]] = []
    active_target_binding_count = 0
    staged_schedule_pins: list[Tuple[Path, str]] = []
    schedule_contracts_used: set[str] = set()
    schedule_axis_cache: Dict[Path, Tuple[pd.DatetimeIndex, pd.DatetimeIndex, str]] = {}
    for symbol in roster:
        terms = dict(symbols_payload.get(symbol) or {})
        price_symbol = price_symbol_index[symbol.casefold()]
        raw_price_cells = entries.get(price_symbol)
        if not isinstance(raw_price_cells, Mapping):
            raise BrokerTermsError(f"{price_symbol}: native price cells are invalid")
        price_cells = dict(raw_price_cells)
        timeframe_index = _casefold_key_map(
            price_cells,
            f"{price_symbol} native price timeframes",
        )
        if "d1" not in timeframe_index:
            raise BrokerTermsError(f"{price_symbol}: native D1 price cell is missing")
        admissible_cells: Dict[str, Dict[str, Any]] = {}
        for raw_timeframe, raw_entry in sorted(price_cells.items()):
            timeframe = str(raw_timeframe).upper()
            cell = dict(raw_entry or {})
            if _price_cell_is_admissible(
                price,
                price_symbol,
                timeframe,
                cell,
            ):
                admissible_cells[timeframe] = cell

        source_preference = (
            ("D1",)
            if price.get("contract_mode") != "promoted-v4"
            else _SCHEDULE_SOURCE_PREFERENCE
        )
        source_candidates = [
            timeframe
            for timeframe in source_preference
            if timeframe in admissible_cells
        ]
        if not source_candidates:
            unscheduled_timeframes = sorted(admissible_cells)
            for timeframe in unscheduled_timeframes:
                unscheduled_candidate_cells.append({
                    "symbol": price_symbol,
                    "timeframe": timeframe,
                    "reason": "NO_ADMISSIBLE_SESSION_SCHEDULE_SOURCE",
                })
            symbol_results[symbol] = {
                "status": INVALID,
                "reason": "NO_ADMISSIBLE_SESSION_SCHEDULE_SOURCE",
                "price_symbol": price_symbol,
                "unscheduled_evidence_candidate_timeframes": unscheduled_timeframes,
            }
            continue

        target_axis_cache: Dict[Tuple[int, str], Dict[str, Any]] = {}
        source_rejections: list[Dict[str, Any]] = []
        selected_source_completed_sessions: set[int] = set()
        selected_target_session_union: set[int] = set()
        selected_source: Optional[Tuple[
            str,
            list[Dict[str, Any]],
            Dict[str, Any],
            Dict[str, str],
            str,
        ]] = None
        for candidate_timeframe in source_candidates:
            candidate_entry = admissible_cells[candidate_timeframe]
            candidate_path = _safe_relative(
                native_root,
                candidate_entry.get("bars_relative_path"),
            )
            candidate_expected_sha = str(
                candidate_entry.get("bars_sha256") or ""
            ).strip().lower()
            if not _is_sha256(candidate_expected_sha):
                raise BrokerTermsError(
                    f"{price_symbol} {candidate_timeframe}: bars pin is invalid"
                )
            if candidate_timeframe == "D1":
                candidate_events, candidate_sha = _derive_d1_session_events_hashed(
                    candidate_path,
                    symbol_terms=terms,
                    expected_sha256=candidate_expected_sha,
                    axis_cache=schedule_axis_cache,
                )
                candidate_metadata: Dict[str, Any] = {}
                candidate_contract = D1_SESSION_DERIVATION_CONTRACT
            else:
                try:
                    (
                        candidate_events,
                        candidate_sha,
                        candidate_metadata,
                    ) = _derive_intraday_session_events_hashed(
                        candidate_path,
                        source_timeframe=candidate_timeframe,
                        symbol_terms=terms,
                        expected_sha256=candidate_expected_sha,
                        axis_cache=schedule_axis_cache,
                    )
                except ScheduleSourceUnsuitableError as exc:
                    # This exception is raised only after the exact bars
                    # payload has passed its hash and interval-axis checks.
                    rejected_binding = _price_cell_binding_from_verified_manifest(
                        native_root,
                        candidate_entry,
                        label=(
                            f"{price_symbol} {candidate_timeframe} "
                            "rejected schedule source"
                        ),
                    )
                    source_rejections.append({
                        "timeframe": candidate_timeframe,
                        "binding": rejected_binding,
                        "reason": exc.reason_code,
                        "missing_target_session_count": 0,
                        "missing_target_sessions_sha256": canonical_sha256([]),
                    })
                    continue
                candidate_contract = INTRADAY_SESSION_DERIVATION_CONTRACT

            candidate_binding = _price_cell_binding_from_verified_manifest(
                native_root,
                candidate_entry,
                label=(f"{price_symbol} {candidate_timeframe} schedule source"),
            )
            if candidate_timeframe != "D1":
                phase_seconds = int(candidate_metadata["phase_seconds"])
                source_completed_sessions = {
                    int(pd.Timestamp(event["session_open_utc"]).timestamp())
                    for event in candidate_events
                }
                target_session_union: set[int] = set()
                missing_by_target: Dict[str, Tuple[int, ...]] = {}
                for target_timeframe, target_cell in sorted(admissible_cells.items()):
                    if target_timeframe == candidate_timeframe:
                        target_completed_sessions = source_completed_sessions
                    else:
                        cache_key = (phase_seconds, target_timeframe)
                        target_axis = target_axis_cache.get(cache_key)
                        if target_axis is None:
                            target_axis = _verified_price_cell_axis(
                                native_root,
                                target_cell,
                                label=(
                                    f"{price_symbol} {target_timeframe} target axis"
                                ),
                                phase_seconds=phase_seconds,
                                axis_cache=schedule_axis_cache,
                            )
                            target_axis_cache[cache_key] = target_axis
                        target_completed_sessions = set(
                            target_axis["completed_session_epochs"]
                        )
                    target_session_union.update(target_completed_sessions)
                    missing_sessions = tuple(
                        sorted(
                            target_completed_sessions
                            - source_completed_sessions
                        )
                    )
                    if missing_sessions:
                        missing_by_target[target_timeframe] = missing_sessions
                if missing_by_target:
                    missing_rows = [
                        [target, int(session)]
                        for target, missing in sorted(
                            missing_by_target.items()
                        )
                        for session in missing
                    ]
                    source_rejections.append({
                        "timeframe": candidate_timeframe,
                        "binding": candidate_binding,
                        "reason": "INCOMPLETE_TARGET_SESSION_COVERAGE",
                        "missing_target_session_count": len(missing_rows),
                        "missing_target_sessions_sha256": canonical_sha256(
                            missing_rows
                        ),
                    })
                    continue
                selected_source_completed_sessions = (
                    source_completed_sessions
                )
                selected_target_session_union = target_session_union
            selected_source = (
                candidate_timeframe,
                candidate_events,
                candidate_metadata,
                candidate_binding,
                candidate_contract,
            )
            break

        if selected_source is None:
            detail = "; ".join(
                f"{rejection['timeframe']}:{rejection['reason']}"
                for rejection in source_rejections
            )[:2048]
            raise BrokerTermsError(
                f"{price_symbol}: no intraday schedule source covers all "
                f"completed target sessions ({detail})"
            )
        (
            schedule_source_timeframe,
            events,
            schedule_derivation_metadata,
            schedule_source_binding,
            schedule_derivation_contract,
        ) = selected_source
        schedule_source_selection_receipt: Optional[Dict[str, Any]] = None
        if source_rejections:
            schedule_source_selection_receipt = {
                "schema_version": SCHEDULE_SOURCE_SELECTION_SCHEMA,
                "policy": SCHEDULE_SOURCE_POLICY,
                "rejected_candidates": source_rejections,
                "selected_source_timeframe": schedule_source_timeframe,
                "selected_source_completed_session_count": len(
                    selected_source_completed_sessions
                ),
                "selected_source_completed_sessions_sha256": canonical_sha256(
                    sorted(selected_source_completed_sessions)
                ),
                "target_completed_session_union_count": len(
                    selected_target_session_union
                ),
                "target_completed_session_union_sha256": canonical_sha256(
                    sorted(selected_target_session_union)
                ),
            }
        if schedule_source_timeframe == "D1":
            # Keep the established D1-v2 artifact and record shape exact.
            schedule: Dict[str, Any] = {
                "schema_version": D1_SESSION_SCHEDULE_SCHEMA,
                "symbol": symbol,
                "price_symbol": price_symbol,
                "price_activation_id": price["activation_id"],
                "price_activation_manifest_sha256": price["manifest_sha256"],
                "d1_bars_relative_path": schedule_source_binding[
                    "bars_relative_path"
                ],
                "d1_bars_sha256": schedule_source_binding["bars_sha256"],
                "d1_sidecar_relative_path": schedule_source_binding[
                    "sidecar_relative_path"
                ],
                "d1_sidecar_sha256": schedule_source_binding["sidecar_sha256"],
                "terms_snapshot_sha256": snapshot_sha,
                "terms_record_sha256": canonical_sha256(terms),
                "event_axis_contract": "event_utc_factor_pairs_v1",
                "events": [
                    [str(event["event_utc"]), int(event["factor"])]
                    for event in events
                ],
            }
            schedule_schema = D1_SESSION_SCHEDULE_SCHEMA
        else:
            schedule = {
                "schema_version": SESSION_SCHEDULE_SCHEMA,
                "symbol": symbol,
                "price_symbol": price_symbol,
                "price_activation_id": price["activation_id"],
                "price_activation_manifest_sha256": price["manifest_sha256"],
                "schedule_source_timeframe": schedule_source_timeframe,
                "schedule_source_binding": dict(schedule_source_binding),
                "schedule_derivation_contract": schedule_derivation_contract,
                "schedule_derivation_metadata": dict(
                    schedule_derivation_metadata
                ),
                "terms_snapshot_sha256": snapshot_sha,
                "terms_record_sha256": canonical_sha256(terms),
                "event_axis_contract": "event_utc_factor_pairs_v1",
                "events": [
                    [str(event["event_utc"]), int(event["factor"])]
                    for event in events
                ],
            }
            if schedule_source_selection_receipt is not None:
                schedule["schedule_source_selection_receipt"] = dict(
                    schedule_source_selection_receipt
                )
            schedule_schema = SESSION_SCHEDULE_SCHEMA
        schedule_contracts_used.add(schedule_schema)
        schedule_path = (
            terms_root
            / "schedules"
            / schedule_schema
            / price["manifest_sha256"]
            / snapshot_sha
            / f"{symbol}.json"
        )
        schedule_sha = _write_or_verify_immutable_json(schedule_path, schedule)
        staged_schedule_pins.append((schedule_path, schedule_sha))
        target_bindings: Dict[str, Any] = {}
        for normalized_timeframe, cell in sorted(admissible_cells.items()):
            target_bindings[normalized_timeframe] = (
                _price_cell_binding_from_verified_manifest(
                    native_root,
                    cell,
                    label=f"{price_symbol} {normalized_timeframe}",
                )
            )
        active_target_binding_count += len(target_bindings)
        if schedule_source_timeframe == "D1":
            symbol_results[symbol] = {
                "status": provenance,
                "price_symbol": price_symbol,
                "broker_symbol": terms.get("broker_symbol"),
                "terms_record_sha256": canonical_sha256(terms),
                "d1_binding": dict(schedule_source_binding),
                "d1_bars_sha256": schedule_source_binding["bars_sha256"],
                "d1_sidecar_sha256": schedule_source_binding["sidecar_sha256"],
                "schedule_relative_path": schedule_path.relative_to(
                    native_root
                ).as_posix(),
                "schedule_sha256": schedule_sha,
                "event_count": len(events),
                "target_bindings": target_bindings,
            }
        else:
            symbol_results[symbol] = {
                "status": provenance,
                "price_symbol": price_symbol,
                "broker_symbol": terms.get("broker_symbol"),
                "terms_record_sha256": canonical_sha256(terms),
                "schedule_source_timeframe": schedule_source_timeframe,
                "schedule_source_binding": dict(schedule_source_binding),
                "schedule_derivation_contract": schedule_derivation_contract,
                "schedule_derivation_metadata": dict(
                    schedule_derivation_metadata
                ),
                "schedule_relative_path": schedule_path.relative_to(
                    native_root
                ).as_posix(),
                "schedule_sha256": schedule_sha,
                "event_count": len(events),
                "target_bindings": target_bindings,
            }
            if schedule_source_selection_receipt is not None:
                symbol_results[symbol][
                    "schedule_source_selection_receipt"
                ] = dict(schedule_source_selection_receipt)

    schedule_axis_bindings = sorted(
        (
            {
                "bars_relative_path": path.relative_to(native_root).as_posix(),
                "bars_sha256": digest,
            }
            for path, (_opens, _closes, digest) in schedule_axis_cache.items()
        ),
        key=lambda value: (value["bars_relative_path"], value["bars_sha256"]),
    )
    active_symbols = sorted(
        symbol for symbol, value in symbol_results.items()
        if value.get("status") in {AUTHENTICATED, STRESS_APPROVED}
    )
    if not active_symbols:
        raise BrokerTermsError("no symbol has an active source-bound terms schedule")
    _validated_schedule_axis_bindings(native_root, schedule_axis_bindings)
    primary_schedule_contract = (
        D1_SESSION_SCHEDULE_SCHEMA
        if D1_SESSION_SCHEDULE_SCHEMA in schedule_contracts_used
        else SESSION_SCHEDULE_SCHEMA
    )
    manifest: Dict[str, Any] = {
        "schema_version": TERMS_ACTIVATION_SCHEMA,
        "activation_id": activation_id,
        "status": "active",
        "created_at_utc": _iso(manifest_created_at),
        "provenance_status": provenance,
        "price_activation_id": price["activation_id"],
        "price_activation_manifest_sha256": price["manifest_sha256"],
        "price_active_pointer_sha256_at_creation": price["pointer_sha256"],
        "terms_snapshot_relative_path": snapshot_path.relative_to(
            native_root
        ).as_posix(),
        "terms_snapshot_sha256": snapshot_sha,
        "schedule_contract": primary_schedule_contract,
        "projection_contract": SWAP_PROJECTION_CONTRACT,
        "symbols": symbol_results,
        "active_symbols": active_symbols,
        "active_target_binding_count": active_target_binding_count,
        "schedule_axis_bindings": schedule_axis_bindings,
        "unscheduled_evidence_candidate_cells": sorted(
            unscheduled_candidate_cells,
            key=lambda value: (value["symbol"], value["timeframe"]),
        ),
    }
    if len(schedule_contracts_used) > 1:
        manifest["schedule_contracts"] = sorted(schedule_contracts_used)
        manifest["schedule_source_policy"] = SCHEDULE_SOURCE_POLICY
    if repair_source is not None:
        manifest["repair_source_terms_activation_id"] = repair_source[
            "activation_id"
        ]
        manifest["repair_source_terms_manifest_sha256"] = repair_source[
            "manifest_sha256"
        ]
        manifest["repair_source_terms_pointer_sha256"] = repair_source[
            "pointer_sha256"
        ]
    if migration_source is not None:
        manifest["price_receipt_migration"] = {
            "source_terms_activation_id": migration_source["activation_id"],
            "source_terms_manifest_sha256": migration_source["manifest_sha256"],
            "source_terms_pointer_sha256": migration_source["pointer_sha256"],
            "predecessor_price_activation_id": migration_source[
                "predecessor_price_activation_id"
            ],
            "predecessor_price_manifest_sha256": migration_source[
                "predecessor_price_manifest_sha256"
            ],
            "predecessor_price_pointer_sha256": migration_source[
                "predecessor_price_pointer_sha256"
            ],
        }
    manifest_path = terms_root / "activations" / f"{activation_id}.json"
    if staged_manifest_created_at_utc is not None and not manifest_path.is_file():
        raise BrokerTermsError(
            "staged manifest creation time is valid only for an existing activation"
        )
    staged_manifest_sha = _write_or_verify_immutable_json(manifest_path, manifest)
    persisted_manifest, manifest_sha, _manifest_bytes = _load_json_bounded_hashed(
        manifest_path
    )
    if (
        manifest_sha != staged_manifest_sha
        or _canonical_bytes(persisted_manifest) != _canonical_bytes(manifest)
    ):
        raise BrokerTermsError("persisted terms activation manifest changed during creation")
    pointer_updated = False
    if update_pointer:
        with _terms_pointer_lock(native_root):
            if _optional_pointer_bytes(terms_pointer_path) != expected_terms_pointer_bytes:
                raise BrokerTermsError("broker-terms pointer changed during activation")
            with _native_price_pointer_lock(native_root):
                commit_now = now + timedelta(
                    seconds=max(0.0, time.monotonic() - activation_started_monotonic)
                )
                pointer = {
                    "schema_version": TERMS_ACTIVE_POINTER_SCHEMA,
                    "activation_id": activation_id,
                    "manifest_relative_path": manifest_path.relative_to(native_root).as_posix(),
                    "manifest_sha256": manifest_sha,
                    "price_activation_manifest_sha256": price["manifest_sha256"],
                    "activated_at_utc": _iso(commit_now),
                }

                def _validate_immediately_before_replace() -> None:
                    revalidate_price_authority()
                    _verify_exact_artifact(
                        snapshot_path,
                        snapshot_sha,
                        "staged terms snapshot",
                    )
                    for (
                        staged_schedule_path,
                        staged_schedule_sha,
                    ) in staged_schedule_pins:
                        _verify_exact_artifact(
                            staged_schedule_path,
                            staged_schedule_sha,
                            "staged D1 session schedule",
                        )
                    _verify_exact_artifact(
                        manifest_path,
                        manifest_sha,
                        "staged terms activation manifest",
                    )
                    _rehash_schedule_axis_bindings(
                        native_root,
                        schedule_axis_bindings,
                    )
                    if repair_source is None and migration_source is None:
                        replace_now = now + timedelta(
                            seconds=max(
                                0.0,
                                time.monotonic() - activation_started_monotonic,
                            )
                        )
                        _require_snapshot_fresh(
                            capture_started,
                            replace_now,
                            max_snapshot_age_seconds,
                        )

                _atomic_replace_json(
                    terms_pointer_path,
                    pointer,
                    before_replace=_validate_immediately_before_replace,
                )
                pointer_updated = True
    else:
        with _terms_pointer_lock(native_root):
            with _native_price_pointer_lock(native_root):
                revalidate_price_authority()
                _verify_exact_artifact(
                    snapshot_path,
                    snapshot_sha,
                    "staged terms snapshot",
                )
                for staged_schedule_path, staged_schedule_sha in staged_schedule_pins:
                    _verify_exact_artifact(
                        staged_schedule_path,
                        staged_schedule_sha,
                        "staged D1 session schedule",
                    )
                _verify_exact_artifact(
                    manifest_path,
                    manifest_sha,
                    "staged terms activation manifest",
                )
                _rehash_schedule_axis_bindings(
                    native_root,
                    schedule_axis_bindings,
                )
                staged_now = now + timedelta(
                    seconds=max(0.0, time.monotonic() - activation_started_monotonic)
                )
                if repair_source is None and migration_source is None:
                    _require_snapshot_fresh(
                        capture_started,
                        staged_now,
                        max_snapshot_age_seconds,
                    )
    return {
        **manifest,
        "manifest_path": str(manifest_path),
        "manifest_sha256": manifest_sha,
        "pointer_updated": pointer_updated,
    }


def activate_broker_terms(
    native_root: Path,
    snapshot: Mapping[str, Any],
    *,
    activation_id: Optional[str] = None,
    now_utc: Optional[datetime] = None,
    update_pointer: bool = True,
    max_snapshot_age_seconds: int = 3600,
    repair_source_terms_pointer_sha256: Optional[str] = None,
    price_receipt_migration_pin: Optional[Mapping[str, Any]] = None,
    staged_manifest_created_at_utc: Optional[datetime] = None,
) -> Dict[str, Any]:
    """Bind terms through exactly one receipt-backed price authority scope."""

    native_root = Path(native_root).resolve()
    with _receipt_backed_price_authority_scope(native_root) as (
        price,
        revalidate_price_authority,
    ):
        return _activate_broker_terms_with_price_authority(
            native_root,
            snapshot,
            price=price,
            revalidate_price_authority=revalidate_price_authority,
            activation_id=activation_id,
            now_utc=now_utc,
            update_pointer=update_pointer,
            max_snapshot_age_seconds=max_snapshot_age_seconds,
            repair_source_terms_pointer_sha256=(repair_source_terms_pointer_sha256),
            price_receipt_migration_pin=price_receipt_migration_pin,
            staged_manifest_created_at_utc=staged_manifest_created_at_utc,
        )


def _resolve_active_swap_contract_with_price_authority(
    native_root: Path,
    symbol: str,
    timeframe: str,
    *,
    price: Mapping[str, Any],
    revalidate_price_authority: Callable[[], None],
) -> Dict[str, Any]:
    native_root = Path(native_root).resolve()
    terms_root = native_root / "terms"
    pointer, pointer_sha, pointer_bytes = _load_json_bounded_hashed(
        terms_root / "ACTIVE.json",
        1024 * 1024,
    )
    if str(pointer.get("schema_version") or "") != TERMS_ACTIVE_POINTER_SCHEMA:
        raise BrokerTermsError("terms pointer schema is invalid")
    manifest_path = _safe_relative(native_root, pointer.get("manifest_relative_path"))
    manifest, manifest_sha, manifest_bytes = _load_json_bounded_hashed(manifest_path)
    if manifest_sha != str(pointer.get("manifest_sha256") or ""):
        raise BrokerTermsError("terms activation manifest hash mismatch")
    if (
        str(manifest.get("schema_version") or "") != TERMS_ACTIVATION_SCHEMA
        or str(manifest.get("status") or "").lower() != "active"
        or str(manifest.get("activation_id") or "") != str(pointer.get("activation_id") or "")
    ):
        raise BrokerTermsError("terms activation manifest identity/schema is invalid")
    if str(manifest.get("price_activation_manifest_sha256") or "") != price["manifest_sha256"]:
        raise BrokerTermsError("terms overlay is bound to a different price activation")
    if str(pointer.get("price_activation_manifest_sha256") or "") != price["manifest_sha256"]:
        raise BrokerTermsError("terms pointer price binding mismatch")
    if (
        str(manifest.get("price_active_pointer_sha256_at_creation") or "").lower()
        != price["pointer_sha256"]
    ):
        raise BrokerTermsError("terms price ACTIVE pointer binding mismatch")
    _validated_schedule_axis_bindings(
        native_root,
        manifest.get("schedule_axis_bindings"),
    )
    raw_schedule_contracts = manifest.get("schedule_contracts")
    if raw_schedule_contracts is None:
        declared_schedule_contracts = [str(manifest.get("schedule_contract") or "")]
    else:
        declared_schedule_contracts = list(raw_schedule_contracts or [])
        if (
            declared_schedule_contracts != sorted(set(declared_schedule_contracts))
            or str(manifest.get("schedule_contract") or "")
            not in declared_schedule_contracts
            or str(manifest.get("schedule_source_policy") or "")
            != SCHEDULE_SOURCE_POLICY
        ):
            raise BrokerTermsError("terms schedule-contract declaration is invalid")
    if (
        not declared_schedule_contracts
        or not set(declared_schedule_contracts).issubset(
            {D1_SESSION_SCHEDULE_SCHEMA, SESSION_SCHEDULE_SCHEMA}
        )
    ):
        raise BrokerTermsError("terms schedule contract is unsupported")

    raw_terms_symbols = manifest.get("symbols")
    if not isinstance(raw_terms_symbols, Mapping):
        raise BrokerTermsError("terms activation symbol map is invalid")
    terms_symbol_index = _casefold_key_map(raw_terms_symbols, "terms activation symbols")
    requested = str(symbol or "").strip()
    terms_symbol = terms_symbol_index.get(requested.casefold())
    if terms_symbol is None:
        raise BrokerTermsError(f"{requested or symbol}: no active terms record")
    tf = str(timeframe or "").strip().upper()
    record = dict(raw_terms_symbols.get(terms_symbol) or {})
    if record.get("status") not in {AUTHENTICATED, STRESS_APPROVED}:
        raise BrokerTermsError(f"{terms_symbol}: no admissible active terms schedule")

    raw_price_entries = price["manifest"].get("entries")
    if not isinstance(raw_price_entries, Mapping):
        raise BrokerTermsError("native price manifest has no symbol entries")
    price_symbol_index = _casefold_key_map(raw_price_entries, "native price symbols")
    price_symbol = price_symbol_index.get(terms_symbol.casefold())
    if price_symbol is None or str(record.get("price_symbol") or "") != price_symbol:
        raise BrokerTermsError(f"{terms_symbol}: price-symbol identity mismatch")
    raw_price_cells = raw_price_entries.get(price_symbol)
    if not isinstance(raw_price_cells, Mapping):
        raise BrokerTermsError(f"{price_symbol}: native price cells are invalid")
    price_cells = dict(raw_price_cells)
    timeframe_index = _casefold_key_map(
        price_cells,
        f"{price_symbol} native price timeframes",
    )
    target_key = timeframe_index.get(tf.casefold())
    if target_key is None:
        raise BrokerTermsError(f"{price_symbol} {tf}: target price cell is missing")
    binding = dict((record.get("target_bindings") or {}).get(tf) or {})
    price_cell = dict(price_cells.get(target_key) or {})
    if not _price_cell_is_admissible(price, price_symbol, tf, price_cell):
        raise BrokerTermsError(f"{price_symbol} {tf}: target price cell is not admissible")

    expected_target_binding = _price_cell_binding_from_verified_manifest(
        native_root,
        price_cell,
        label=f"{price_symbol} {tf} target artifact",
    )
    if _canonical_bytes(binding) != _canonical_bytes(expected_target_binding):
        raise BrokerTermsError(f"{price_symbol} {tf}: target price binding mismatch")

    admissible_source_cells: Dict[str, Dict[str, Any]] = {}
    source_preference = (
        ("D1",)
        if price.get("contract_mode") != "promoted-v4"
        else _SCHEDULE_SOURCE_PREFERENCE
    )
    for source_timeframe in source_preference:
        source_key = timeframe_index.get(source_timeframe.casefold())
        if source_key is None:
            if source_timeframe == "D1":
                raise BrokerTermsError(
                    f"{price_symbol}: native D1 price cell is missing"
                )
            continue
        source_cell = dict(price_cells.get(source_key) or {})
        if _price_cell_is_admissible(
            price,
            price_symbol,
            source_timeframe,
            source_cell,
        ):
            admissible_source_cells[source_timeframe] = source_cell
    preferred_source_timeframe = next(
        (
            source_timeframe
            for source_timeframe in source_preference
            if source_timeframe in admissible_source_cells
        ),
        None,
    )
    if preferred_source_timeframe is None:
        raise BrokerTermsError(
            f"{price_symbol}: no admissible session schedule source"
        )

    selection_receipt = record.get(
        "schedule_source_selection_receipt"
    )
    generic_source_fields = {
        "schedule_source_timeframe",
        "schedule_source_binding",
        "schedule_derivation_contract",
        "schedule_derivation_metadata",
    }
    present_generic_fields = generic_source_fields.intersection(record)
    if present_generic_fields and present_generic_fields != generic_source_fields:
        raise BrokerTermsError("terms record has an incomplete schedule-source binding")
    is_generic_schedule = bool(present_generic_fields)
    if is_generic_schedule:
        if preferred_source_timeframe == "D1":
            raise BrokerTermsError(
                f"{price_symbol}: generic schedule cannot override native D1"
            )
        schedule_source_timeframe = str(
            record.get("schedule_source_timeframe") or ""
        ).strip().upper()
        if schedule_source_timeframe not in _SCHEDULE_SOURCE_PREFERENCE[1:]:
            raise BrokerTermsError(
                f"{price_symbol}: schedule-source preference mismatch"
            )
        if selection_receipt is None:
            if schedule_source_timeframe != preferred_source_timeframe:
                raise BrokerTermsError(
                    f"{price_symbol}: schedule-source preference mismatch"
                )
        else:
            if (
                not isinstance(selection_receipt, Mapping)
                or set(selection_receipt)
                != {
                    "schema_version",
                    "policy",
                    "rejected_candidates",
                    "selected_source_timeframe",
                    "selected_source_completed_session_count",
                    "selected_source_completed_sessions_sha256",
                    "target_completed_session_union_count",
                    "target_completed_session_union_sha256",
                }
                or selection_receipt.get("schema_version")
                != SCHEDULE_SOURCE_SELECTION_SCHEMA
                or selection_receipt.get("policy")
                != SCHEDULE_SOURCE_POLICY
                or selection_receipt.get("selected_source_timeframe")
                != schedule_source_timeframe
            ):
                raise BrokerTermsError(
                    "terms record schedule-source selection receipt is invalid"
                )
            rejected_candidates = selection_receipt.get(
                "rejected_candidates"
            )
            selected_index = _SCHEDULE_SOURCE_PREFERENCE.index(
                schedule_source_timeframe
            )
            expected_rejected_timeframes = [
                source_timeframe
                for source_timeframe in _SCHEDULE_SOURCE_PREFERENCE[
                    1:selected_index
                ]
                if source_timeframe in admissible_source_cells
            ]
            if (
                schedule_source_timeframe == preferred_source_timeframe
                or not isinstance(rejected_candidates, list)
                or [
                    str(rejection.get("timeframe") or "")
                    if isinstance(rejection, Mapping)
                    else ""
                    for rejection in rejected_candidates
                ]
                != expected_rejected_timeframes
            ):
                raise BrokerTermsError(
                    "terms record schedule-source rejection chain is invalid"
                )
            allowed_rejection_reasons = {
                "ANCHOR_INTERVAL_NOT_OVERLAPPED",
                "ANCHOR_SESSION_UNREPRESENTED",
                "NO_COMPLETED_SOURCE_SESSION",
                "INCOMPLETE_TARGET_SESSION_COVERAGE",
            }
            for rejection in rejected_candidates:
                if (
                    not isinstance(rejection, Mapping)
                    or set(rejection)
                    != {
                        "timeframe",
                        "binding",
                        "reason",
                        "missing_target_session_count",
                        "missing_target_sessions_sha256",
                    }
                    or rejection.get("reason")
                    not in allowed_rejection_reasons
                    or type(rejection.get("missing_target_session_count"))
                    is not int
                    or int(rejection["missing_target_session_count"]) < 0
                    or not _is_sha256(
                        rejection.get("missing_target_sessions_sha256")
                    )
                    or not isinstance(rejection.get("binding"), Mapping)
                ):
                    raise BrokerTermsError(
                        "terms record schedule-source rejection is invalid"
                    )
                missing_count = int(
                    rejection["missing_target_session_count"]
                )
                if (
                    rejection["reason"]
                    == "INCOMPLETE_TARGET_SESSION_COVERAGE"
                ) != (missing_count > 0):
                    raise BrokerTermsError(
                        "terms record schedule-source rejection count is invalid"
                    )
                if (
                    missing_count == 0
                    and rejection["missing_target_sessions_sha256"]
                    != canonical_sha256([])
                ):
                    raise BrokerTermsError(
                        "terms record schedule-source rejection digest is invalid"
                    )
                rejected_timeframe = str(rejection["timeframe"])
                expected_rejected_binding = _price_cell_binding_from_verified_manifest(
                    native_root,
                    admissible_source_cells[rejected_timeframe],
                    label=(
                        f"{price_symbol} {rejected_timeframe} rejected schedule source"
                    ),
                )
                if _canonical_bytes(
                    rejection["binding"]
                ) != _canonical_bytes(expected_rejected_binding):
                    raise BrokerTermsError(
                        "terms record rejected schedule-source binding mismatch"
                    )
            selected_session_count = selection_receipt.get(
                "selected_source_completed_session_count"
            )
            target_session_count = selection_receipt.get(
                "target_completed_session_union_count"
            )
            if (
                type(selected_session_count) is not int
                or type(target_session_count) is not int
                or int(selected_session_count) <= 0
                or int(target_session_count) <= 0
                or int(selected_session_count) < int(target_session_count)
                or not _is_sha256(
                    selection_receipt.get(
                        "selected_source_completed_sessions_sha256"
                    )
                )
                or not _is_sha256(
                    selection_receipt.get(
                        "target_completed_session_union_sha256"
                    )
                )
            ):
                raise BrokerTermsError(
                    "terms record schedule-source selection summary is invalid"
                )
        schedule_source_binding = record.get("schedule_source_binding")
        schedule_derivation_contract = str(
            record.get("schedule_derivation_contract") or ""
        )
        schedule_derivation_metadata = record.get(
            "schedule_derivation_metadata"
        )
        if (
            not isinstance(schedule_source_binding, Mapping)
            or schedule_derivation_contract
            != INTRADAY_SESSION_DERIVATION_CONTRACT
            or not isinstance(schedule_derivation_metadata, Mapping)
            or set(schedule_derivation_metadata)
            != {
                "matched_anchor_count",
                "phase_seconds",
                "source_first_open_utc",
                "source_last_close_utc",
                "source_row_count",
                "weekday_offset",
            }
        ):
            raise BrokerTermsError("terms record schedule-source contract is invalid")
        if (
            type(schedule_derivation_metadata.get("matched_anchor_count")) is not int
            or int(schedule_derivation_metadata["matched_anchor_count"]) <= 0
            or type(schedule_derivation_metadata.get("phase_seconds")) is not int
            or not 0 <= int(schedule_derivation_metadata["phase_seconds"]) < 86_400
            or type(schedule_derivation_metadata.get("source_row_count")) is not int
            or int(schedule_derivation_metadata["source_row_count"]) <= 0
            or type(schedule_derivation_metadata.get("weekday_offset")) is not int
            or not 0 <= int(schedule_derivation_metadata["weekday_offset"]) <= 6
        ):
            raise BrokerTermsError("terms record schedule-source metadata is invalid")
        try:
            source_first_open = pd.Timestamp(
                schedule_derivation_metadata["source_first_open_utc"]
            )
            source_last_close = pd.Timestamp(
                schedule_derivation_metadata["source_last_close_utc"]
            )
        except (TypeError, ValueError) as exc:
            raise BrokerTermsError(
                "terms record schedule-source interval is invalid"
            ) from exc
        if (
            source_first_open.tzinfo is None
            or source_last_close.tzinfo is None
            or source_last_close <= source_first_open
        ):
            raise BrokerTermsError("terms record schedule-source interval is invalid")
    else:
        schedule_source_timeframe = "D1"
        if selection_receipt is not None:
            raise BrokerTermsError(
                "legacy D1 record has an unexpected source-selection receipt"
            )
        if preferred_source_timeframe != "D1":
            raise BrokerTermsError(
                f"{price_symbol}: legacy D1 schedule source is not admissible"
            )
        schedule_source_binding = record.get("d1_binding")
        schedule_derivation_contract = D1_SESSION_DERIVATION_CONTRACT
        schedule_derivation_metadata = {}
        if not isinstance(schedule_source_binding, Mapping):
            raise BrokerTermsError("terms record lacks its D1 artifact binding")

    schedule_source_cell = admissible_source_cells[schedule_source_timeframe]
    expected_schedule_source_binding = _price_cell_binding_from_verified_manifest(
        native_root,
        schedule_source_cell,
        label=f"{price_symbol} {schedule_source_timeframe} schedule source",
    )
    if _canonical_bytes(schedule_source_binding) != _canonical_bytes(
        expected_schedule_source_binding
    ):
        raise BrokerTermsError(f"{price_symbol}: schedule-source binding mismatch")

    schedule_path = _safe_relative(native_root, record.get("schedule_relative_path"))
    schedule, schedule_sha, schedule_bytes = _load_json_bounded_hashed(schedule_path)
    if schedule_sha != str(record.get("schedule_sha256") or ""):
        raise BrokerTermsError("session schedule hash mismatch")
    schedule_schema = str(schedule.get("schema_version") or "")
    common_schedule_invalid = (
        schedule.get("symbol") != terms_symbol
        or schedule.get("price_symbol") != price_symbol
        or schedule.get("price_activation_id") != price["activation_id"]
        or schedule.get("price_activation_manifest_sha256") != price["manifest_sha256"]
        or schedule.get("terms_snapshot_sha256") != manifest.get("terms_snapshot_sha256")
        or schedule.get("terms_record_sha256") != record.get("terms_record_sha256")
        or schedule.get("event_axis_contract") != "event_utc_factor_pairs_v1"
        or not isinstance(schedule.get("events"), list)
    )
    if common_schedule_invalid:
        raise BrokerTermsError("session schedule binding/schema mismatch")
    if is_generic_schedule:
        if (
            schedule_schema != SESSION_SCHEDULE_SCHEMA
            or SESSION_SCHEDULE_SCHEMA not in declared_schedule_contracts
            or schedule.get("schedule_source_timeframe")
            != schedule_source_timeframe
            or _canonical_bytes(schedule.get("schedule_source_binding"))
            != _canonical_bytes(expected_schedule_source_binding)
            or schedule.get("schedule_derivation_contract")
            != schedule_derivation_contract
            or _canonical_bytes(schedule.get("schedule_derivation_metadata"))
            != _canonical_bytes(schedule_derivation_metadata)
            or _canonical_bytes(
                schedule.get("schedule_source_selection_receipt")
            )
            != _canonical_bytes(selection_receipt)
        ):
            raise BrokerTermsError("session schedule source binding/schema mismatch")
    elif (
        schedule_schema != D1_SESSION_SCHEDULE_SCHEMA
        or D1_SESSION_SCHEDULE_SCHEMA not in declared_schedule_contracts
        or schedule.get("schedule_source_selection_receipt") is not None
        or schedule.get("d1_bars_relative_path")
        != expected_schedule_source_binding["bars_relative_path"]
        or schedule.get("d1_bars_sha256")
        != expected_schedule_source_binding["bars_sha256"]
        or schedule.get("d1_sidecar_relative_path")
        != expected_schedule_source_binding["sidecar_relative_path"]
        or schedule.get("d1_sidecar_sha256")
        != expected_schedule_source_binding["sidecar_sha256"]
    ):
        raise BrokerTermsError("D1 session schedule binding/schema mismatch")
    snapshot_path = _safe_relative(native_root, manifest.get("terms_snapshot_relative_path"))
    snapshot, snapshot_sha, snapshot_bytes = _load_json_bounded_hashed(snapshot_path)
    if snapshot_sha != str(manifest.get("terms_snapshot_sha256") or ""):
        raise BrokerTermsError("terms snapshot hash mismatch")
    _validate_terms_snapshot(snapshot)
    if (
        snapshot.get("provenance_status") != manifest.get("provenance_status")
        or record.get("status") != manifest.get("provenance_status")
    ):
        raise BrokerTermsError("terms snapshot provenance does not match its activation")
    snapshot_symbols = snapshot.get("symbols")
    if not isinstance(snapshot_symbols, Mapping):
        raise BrokerTermsError("active terms snapshot symbol map is invalid")
    snapshot_symbol_index = _casefold_key_map(snapshot_symbols, "terms snapshot symbols")
    snapshot_symbol = snapshot_symbol_index.get(terms_symbol.casefold())
    if snapshot_symbol != terms_symbol:
        raise BrokerTermsError("symbol terms snapshot identity mismatch")
    terms = dict(snapshot_symbols.get(terms_symbol) or {})
    if canonical_sha256(terms) != record.get("terms_record_sha256"):
        raise BrokerTermsError("symbol terms record hash mismatch")
    stress_policy = dict(snapshot.get("stress_policy") or {})
    stress_approved = record["status"] == STRESS_APPROVED
    with _terms_pointer_lock(native_root):
        with _native_price_pointer_lock(native_root):
            if (
                _read_bounded(terms_root / "ACTIVE.json", 1024 * 1024) != pointer_bytes
                or _read_bounded(manifest_path, 64 * 1024 * 1024) != manifest_bytes
                or _read_bounded(snapshot_path, 64 * 1024 * 1024) != snapshot_bytes
                or _read_bounded(schedule_path, 64 * 1024 * 1024) != schedule_bytes
            ):
                raise BrokerTermsError(
                    "broker terms activation changed during resolution"
                )
            revalidate_price_authority()
    return {
        "status": "AVAILABLE",
        "schedule_provenance_status": record["status"],
        "source_provenance_authenticated": record["status"] == AUTHENTICATED,
        "stress_approved": stress_approved,
        "stress_policy_immutable": stress_approved and stress_policy == _STRESS_POLICY,
        "stress_policy_sha256": (
            canonical_sha256(stress_policy)
            if stress_approved and stress_policy
            else ""
        ),
        "price_activation_id": price["activation_id"],
        "price_activation_manifest_sha256": price["manifest_sha256"],
        "terms_activation_id": manifest.get("activation_id"),
        "terms_activation_manifest_sha256": manifest_sha,
        "terms_active_pointer_sha256": pointer_sha,
        "terms_snapshot_sha256": manifest.get("terms_snapshot_sha256"),
        "schedule_sha256": record.get("schedule_sha256"),
        "target_bars_sha256": binding.get("bars_sha256"),
        "target_sidecar_sha256": binding.get("sidecar_sha256"),
        "schedule_source_timeframe": schedule_source_timeframe,
        "schedule_derivation_contract": schedule_derivation_contract,
        "schedule_source_bars_sha256": expected_schedule_source_binding.get(
            "bars_sha256"
        ),
        "schedule_source_sidecar_sha256": expected_schedule_source_binding.get(
            "sidecar_sha256"
        ),
        "d1_bars_sha256": (
            expected_schedule_source_binding.get("bars_sha256")
            if schedule_source_timeframe == "D1"
            else None
        ),
        "d1_sidecar_sha256": (
            expected_schedule_source_binding.get("sidecar_sha256")
            if schedule_source_timeframe == "D1"
            else None
        ),
        "projection_contract": SWAP_PROJECTION_CONTRACT,
        "terms": terms,
        "events": list(schedule.get("events") or []),
    }


def resolve_active_swap_contract(
    native_root: Path,
    symbol: str,
    timeframe: str,
) -> Dict[str, Any]:
    """Resolve one terms contract without reopening any raw price leaf."""

    native_root = Path(native_root).resolve()
    with _receipt_backed_price_authority_scope(native_root) as (
        price,
        revalidate_price_authority,
    ):
        return _resolve_active_swap_contract_with_price_authority(
            native_root,
            symbol,
            timeframe,
            price=price,
            revalidate_price_authority=revalidate_price_authority,
        )


def write_capture_request(
    path: Path,
    roster: Mapping[str, str],
    *,
    nonce: Optional[str] = None,
    requested_at_utc: Optional[datetime] = None,
    expected_identity: Optional[Mapping[str, Any]] = None,
) -> Dict[str, Any]:
    """Atomically publish the bounded CSV request consumed by the MQL5 service."""
    nonce = str(nonce or secrets.token_hex(24))
    requested = (requested_at_utc or _utc_now()).astimezone(timezone.utc)
    identity = dict(expected_identity or {})
    rows = []
    for canonical, broker in sorted(dict(roster).items()):
        rows.append({
            "schema_version": TERMS_REQUEST_SCHEMA,
            "nonce": nonce,
            "requested_at_utc": _iso(requested),
            "canonical_symbol": str(canonical).upper(),
            "broker_symbol": str(broker),
            "expected_server": str(identity.get("broker_server") or ""),
            "expected_company": str(identity.get("terminal_company") or ""),
            "expected_currency": str(identity.get("account_currency") or ""),
        })
    if not rows:
        raise BrokerTermsError("capture request roster is empty")
    output = io.StringIO(newline="")
    writer = csv.DictWriter(output, fieldnames=REQUEST_COLUMNS, lineterminator="\n")
    writer.writeheader()
    writer.writerows(rows)
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.{secrets.token_hex(4)}.tmp")
    try:
        with temporary.open("x", encoding="utf-8", newline="") as handle:
            handle.write(output.getvalue())
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)
    return {"nonce": nonce, "requested_at_utc": _iso(requested), "rows": len(rows), "path": str(path)}


def _prune_terms_store_locked(
    native_root: Path,
    *,
    max_snapshots: int = 16,
    max_total_bytes: int = 64 * 1024 * 1024,
    referenced_sha256: Iterable[str] = (),
) -> Dict[str, Any]:
    """Prune with the caller holding the shared terms-pointer lock."""
    native_root = Path(native_root).resolve()
    terms_root = native_root / "terms"
    protected_hashes = {str(value).lower() for value in referenced_sha256 if _is_sha256(value)}
    protected_paths: set[Path] = set()
    pointer_path = terms_root / "ACTIVE.json"
    if pointer_path.exists():
        protected_paths.add(pointer_path.resolve())
        pointer = _load_json_bounded(pointer_path, 1024 * 1024)
        manifest_path = _safe_relative(native_root, pointer.get("manifest_relative_path"))
        protected_paths.add(manifest_path.resolve())
        manifest = _load_json_bounded(manifest_path)
        for field in ("terms_snapshot_relative_path",):
            protected_paths.add(_safe_relative(native_root, manifest.get(field)).resolve())
        for record in (manifest.get("symbols") or {}).values():
            if isinstance(record, Mapping) and record.get("schedule_relative_path"):
                protected_paths.add(_safe_relative(native_root, record.get("schedule_relative_path")).resolve())
    references_path = terms_root / "REFERENCES.json"
    if references_path.exists():
        protected_paths.add(references_path.resolve())
        references = _load_json_bounded(references_path, 4 * 1024 * 1024)
        protected_hashes.update(
            str(value).lower() for value in references.get("protected_sha256", [])
            if _is_sha256(value)
        )
    files = [path for path in terms_root.rglob("*") if path.is_file() and not path.is_symlink()]
    hashes = {path.resolve(): sha256_file(path) for path in files}
    protected_paths.update(path for path, digest in hashes.items() if digest in protected_hashes)
    protected_bytes = sum(path.stat().st_size for path in protected_paths if path.exists())
    if protected_bytes > int(max_total_bytes):
        raise TermsStorageBoundError("protected broker-terms evidence exceeds the hard byte cap")
    snapshots = sorted(
        (path.resolve() for path in (terms_root / "snapshots").glob("*.json")),
        key=lambda path: path.stat().st_mtime_ns,
        reverse=True,
    ) if (terms_root / "snapshots").exists() else []
    protected_snapshots = {path for path in snapshots if path in protected_paths}
    available_snapshot_slots = max(
        0,
        int(max_snapshots) - len(protected_snapshots),
    )
    unprotected_snapshots = [path for path in snapshots if path not in protected_paths]
    keep_snapshots = protected_snapshots | set(
        unprotected_snapshots[:available_snapshot_slots]
    )
    candidates = sorted(
        (path.resolve() for path in files if path.resolve() not in protected_paths),
        key=lambda path: path.stat().st_mtime_ns,
    )
    deleted: list[str] = []
    for path in candidates:
        if path in snapshots and path in keep_snapshots:
            continue
        path.unlink(missing_ok=True)
        deleted.append(str(path))
    remaining = [path for path in terms_root.rglob("*") if path.is_file()]
    total = sum(path.stat().st_size for path in remaining)
    if total > int(max_total_bytes):
        removable = sorted(
            (path for path in remaining if path.resolve() not in protected_paths),
            key=lambda path: path.stat().st_mtime_ns,
        )
        for path in removable:
            if total <= int(max_total_bytes):
                break
            size = path.stat().st_size
            path.unlink(missing_ok=True)
            deleted.append(str(path))
            total -= size
    if total > int(max_total_bytes):
        raise TermsStorageBoundError("broker-terms store cannot satisfy its hard byte cap")
    return {"deleted": deleted, "remaining_bytes": total, "protected_bytes": protected_bytes}


def prune_terms_store(
    native_root: Path,
    *,
    max_snapshots: int = 16,
    max_total_bytes: int = 64 * 1024 * 1024,
    referenced_sha256: Iterable[str] = (),
) -> Dict[str, Any]:
    """Bound unreferenced terms artifacts without racing pointer publication."""

    native_root = Path(native_root).resolve()
    with _terms_pointer_lock(native_root):
        return _prune_terms_store_locked(
            native_root,
            max_snapshots=max_snapshots,
            max_total_bytes=max_total_bytes,
            referenced_sha256=referenced_sha256,
        )
