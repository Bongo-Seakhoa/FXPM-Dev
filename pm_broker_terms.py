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
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, Mapping, Optional, Sequence, Tuple

import numpy as np
import pandas as pd


TERMS_EXPORT_SCHEMA = "fxpm-mql5-broker-terms-export-v1"
TERMS_REQUEST_SCHEMA = "fxpm-mql5-broker-terms-request-v1"
TERMS_SNAPSHOT_SCHEMA = "fxpm-broker-terms-snapshot-v1"
TERMS_ACTIVATION_SCHEMA = "fxpm-native-terms-activation-v1"
TERMS_ACTIVE_POINTER_SCHEMA = "fxpm-native-terms-active-pointer-v1"
D1_SESSION_SCHEDULE_SCHEMA = "fxpm-d1-swap-session-schedule-v2"
SWAP_PROJECTION_CONTRACT = "fxpm-swap-preopen-close-projection-v1"
STRESS_POLICY_ID = "all_sessions_triple_debit_v1"

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
    if not path.is_file() or path.is_symlink():
        raise BrokerTermsError(f"artifact is missing or unsafe: {path}")
    size = path.stat().st_size
    if size <= 0 or size > int(max_bytes):
        raise BrokerTermsError(f"artifact size {size} is outside the allowed bound")
    with path.open("rb") as handle:
        payload = handle.read(int(max_bytes) + 1)
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


def _atomic_replace_json(path: Path, value: Mapping[str, Any]) -> None:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.{secrets.token_hex(6)}.tmp")
    encoded = json.dumps(value, indent=2, sort_keys=True, ensure_ascii=False, allow_nan=False)
    try:
        with temporary.open("x", encoding="utf-8", newline="\n") as handle:
            handle.write(encoded)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


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
    payload = _read_bounded(Path(export_path), max_bytes).decode("utf-8-sig", errors="strict")
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
        "source_export_sha256": sha256_file(Path(export_path)),
    }
    snapshot["content_sha256"] = canonical_sha256(snapshot)
    return snapshot


def build_explicit_stress_snapshot(snapshot: Mapping[str, Any]) -> Dict[str, Any]:
    """Create an explicit debit-only stress snapshot; never called implicitly."""
    if str(snapshot.get("schema_version") or "") != TERMS_SNAPSHOT_SCHEMA:
        raise BrokerTermsError("stress source snapshot schema is invalid")
    output = json.loads(json.dumps(snapshot))
    for symbol, record in dict(output.get("symbols") or {}).items():
        long_rate = _finite_float(record.get("swap_long"), f"{symbol}.swap_long")
        short_rate = _finite_float(record.get("swap_short"), f"{symbol}.swap_short")
        adverse = -max(abs(long_rate), abs(short_rate))
        record["swap_long"] = adverse
        record["swap_short"] = adverse
        record["weekday_factors"] = {weekday: 3 for weekday in WEEKDAYS}
        record["provenance_status"] = STRESS_APPROVED
    output["provenance_status"] = STRESS_APPROVED
    output["stress_policy"] = {
        "policy_id": STRESS_POLICY_ID,
        "explicit_approval_required": True,
        "positive_credits_suppressed": True,
    }
    output.pop("content_sha256", None)
    output["content_sha256"] = canonical_sha256(output)
    return output


def _load_json_bounded(path: Path, max_bytes: int = 64 * 1024 * 1024) -> Dict[str, Any]:
    try:
        value = json.loads(_read_bounded(path, max_bytes).decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise BrokerTermsError(f"invalid JSON artifact: {path}") from exc
    if not isinstance(value, dict):
        raise BrokerTermsError(f"JSON artifact is not an object: {path}")
    return value


def load_current_price_activation(native_root: Path) -> Dict[str, Any]:
    native_root = Path(native_root).resolve()
    pointer_path = native_root / "ACTIVE.json"
    pointer = _load_json_bounded(pointer_path, 1024 * 1024)
    if str(pointer.get("schema_version") or "") != "fxpm-native-active-pointer-v1":
        raise BrokerTermsError("native price pointer schema is unsupported")
    manifest_path = _safe_relative(native_root, pointer.get("manifest_relative_path"))
    manifest_sha = sha256_file(manifest_path)
    if manifest_sha != str(pointer.get("manifest_sha256") or "").lower():
        raise BrokerTermsError("native price manifest hash mismatch")
    manifest = _load_json_bounded(manifest_path)
    if str(manifest.get("schema_version") or "") != "fxpm-native-activation-v1":
        raise BrokerTermsError("native price activation schema is unsupported")
    if str(manifest.get("status") or "").lower() != "active":
        raise BrokerTermsError("native price activation is not active")
    if str(manifest.get("activation_id") or "") != str(pointer.get("activation_id") or ""):
        raise BrokerTermsError("native price activation identity mismatch")
    return {
        "pointer": pointer,
        "pointer_path": pointer_path,
        "pointer_sha256": sha256_file(pointer_path),
        "manifest": manifest,
        "manifest_path": manifest_path,
        "manifest_sha256": manifest_sha,
        "activation_id": str(manifest.get("activation_id") or ""),
    }


def derive_d1_session_events(
    d1_csv_path: Path,
    *,
    symbol_terms: Mapping[str, Any],
    max_rows: int = 100_000,
    max_bytes: int = 256 * 1024 * 1024,
) -> list[Dict[str, Any]]:
    """Derive broker-day rollover events exclusively from immutable D1 intervals."""
    path = Path(d1_csv_path)
    if path.stat().st_size > int(max_bytes):
        raise BrokerTermsError("D1 artifact exceeds the schedule read bound")
    try:
        frame = pd.read_csv(path, usecols=["time", "_BAR_CLOSE_TS"])
    except Exception as exc:
        raise BrokerTermsError("D1 artifact lacks the required interval columns") from exc
    if len(frame) <= 0 or len(frame) > int(max_rows):
        raise BrokerTermsError("D1 row count is outside the permitted bound")
    opens = pd.DatetimeIndex(pd.to_datetime(frame["time"], utc=True, errors="coerce"))
    closes = pd.DatetimeIndex(pd.to_datetime(frame["_BAR_CLOSE_TS"], utc=True, errors="coerce"))
    if (
        opens.isna().any()
        or closes.isna().any()
        or opens.has_duplicates
        or closes.has_duplicates
        or not opens.is_monotonic_increasing
        or not closes.is_monotonic_increasing
        or not bool((closes > opens).all())
    ):
        raise BrokerTermsError("D1 interval axis is invalid")
    open_epochs = {int(value.timestamp()): index for index, value in enumerate(opens)}
    offsets: set[int] = set()
    matched = 0
    for anchor in list(symbol_terms.get("d1_anchors") or []):
        epoch = _strict_int(anchor.get("open_epoch"), "anchor.open_epoch")
        weekday = _strict_int(anchor.get("server_weekday"), "anchor.server_weekday")
        if epoch not in open_epochs:
            continue
        utc_weekday_mql5 = (opens[open_epochs[epoch]].weekday() + 1) % 7
        offsets.add((weekday - utc_weekday_mql5) % 7)
        matched += 1
    if matched <= 0 or len(offsets) != 1:
        raise BrokerTermsError("D1 broker-weekday anchors do not bind to the active artifact")
    weekday_offset = next(iter(offsets))
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


def activate_broker_terms(
    native_root: Path,
    snapshot: Mapping[str, Any],
    *,
    activation_id: Optional[str] = None,
    now_utc: Optional[datetime] = None,
    update_pointer: bool = True,
    max_snapshot_age_seconds: int = 3600,
) -> Dict[str, Any]:
    """Bind terms and D1 schedules to the current immutable price activation."""
    native_root = Path(native_root).resolve()
    if str(snapshot.get("schema_version") or "") != TERMS_SNAPSHOT_SCHEMA:
        raise BrokerTermsError("terms snapshot schema is invalid")
    provenance = str(snapshot.get("provenance_status") or "")
    if provenance not in {AUTHENTICATED, STRESS_APPROVED}:
        raise BrokerTermsError("terms snapshot is not admissible for activation")
    declared_content_sha = str(snapshot.get("content_sha256") or "")
    without_hash = dict(snapshot)
    without_hash.pop("content_sha256", None)
    if declared_content_sha != canonical_sha256(without_hash):
        raise BrokerTermsError("terms snapshot content hash is invalid")
    now = (now_utc or _utc_now()).astimezone(timezone.utc)
    captured = _aware_utc(snapshot.get("captured_at_utc"), "snapshot.captured_at_utc")
    age_seconds = (now - captured).total_seconds()
    if age_seconds < -60 or age_seconds > int(max_snapshot_age_seconds):
        raise BrokerTermsError("terms snapshot is not fresh at activation time")
    roster = [str(value or "").strip().upper() for value in (snapshot.get("roster") or [])]
    symbols_payload = dict(snapshot.get("symbols") or {})
    if not roster or len(roster) != len(set(roster)) or set(roster) != set(symbols_payload):
        raise BrokerTermsError("terms snapshot roster and symbol records are not exact")
    if provenance == AUTHENTICATED and (
        str(snapshot.get("extractor_contract") or "") != "mql5-symbolinfo-direct-v1"
        or not _is_sha256(snapshot.get("source_export_sha256"))
        or len(str(snapshot.get("nonce") or "")) < 16
    ):
        raise BrokerTermsError("authenticated snapshot lacks direct-export provenance")
    if provenance == STRESS_APPROVED and (
        str((snapshot.get("stress_policy") or {}).get("policy_id") or "")
        != STRESS_POLICY_ID
    ):
        raise BrokerTermsError("stress snapshot lacks the approved immutable policy")
    price = load_current_price_activation(native_root)
    pointer_sha_at_start = str(price["pointer_sha256"])
    activation_id = str(activation_id or f"{now.strftime('%Y%m%dT%H%M%SZ')}-{secrets.token_hex(4)}")
    if not activation_id or any(character not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_" for character in activation_id):
        raise BrokerTermsError("terms activation id is unsafe")
    terms_root = native_root / "terms"
    snapshot_id = declared_content_sha[:24]
    snapshot_path = terms_root / "snapshots" / f"{snapshot_id}.json"
    snapshot_sha = _write_or_verify_immutable_json(snapshot_path, dict(snapshot))

    entries = dict(price["manifest"].get("entries") or {})
    symbol_results: Dict[str, Any] = {}
    for symbol in list(snapshot.get("roster") or []):
        terms = dict((snapshot.get("symbols") or {}).get(symbol) or {})
        price_cells = dict(entries.get(symbol) or {})
        d1_entry = dict(price_cells.get("D1") or {})
        if str(d1_entry.get("status") or "").lower() != "active":
            symbol_results[symbol] = {"status": INVALID, "reason": "D1_NOT_ACTIVE"}
            continue
        d1_path = _safe_relative(native_root, d1_entry.get("bars_relative_path"))
        d1_sha = sha256_file(d1_path)
        if d1_sha != str(d1_entry.get("bars_sha256") or "").lower():
            raise BrokerTermsError(f"{symbol}: D1 bars hash mismatch")
        events = derive_d1_session_events(d1_path, symbol_terms=terms)
        schedule: Dict[str, Any] = {
            "schema_version": D1_SESSION_SCHEDULE_SCHEMA,
            "symbol": symbol,
            "price_activation_id": price["activation_id"],
            "price_activation_manifest_sha256": price["manifest_sha256"],
            "d1_bars_sha256": d1_sha,
            "terms_snapshot_sha256": snapshot_sha,
            "terms_record_sha256": canonical_sha256(terms),
            # Persist only the two values required to replay the event.  The
            # D1 bars hash and derivation contract authenticate bar index,
            # open, close and weekday semantics without repeating those
            # verbose fields thousands of times per symbol.
            "event_axis_contract": "event_utc_factor_pairs_v1",
            "events": [
                [str(event["event_utc"]), int(event["factor"])]
                for event in events
            ],
        }
        schedule_path = (
            terms_root
            / "schedules"
            / D1_SESSION_SCHEDULE_SCHEMA
            / price["manifest_sha256"]
            / snapshot_sha
            / f"{symbol}.json"
        )
        schedule_sha = _write_or_verify_immutable_json(schedule_path, schedule)
        target_bindings: Dict[str, Any] = {}
        for timeframe, raw_entry in sorted(price_cells.items()):
            cell = dict(raw_entry or {})
            if str(cell.get("status") or "").lower() != "active":
                continue
            bars_path = _safe_relative(native_root, cell.get("bars_relative_path"))
            sidecar_path = _safe_relative(native_root, cell.get("sidecar_relative_path"))
            bars_sha = sha256_file(bars_path)
            sidecar_sha = sha256_file(sidecar_path)
            if (
                bars_sha != str(cell.get("bars_sha256") or "").lower()
                or sidecar_sha != str(cell.get("sidecar_sha256") or "").lower()
            ):
                raise BrokerTermsError(f"{symbol} {timeframe}: active price artifact hash mismatch")
            target_bindings[str(timeframe).upper()] = {
                "bars_relative_path": bars_path.relative_to(native_root).as_posix(),
                "bars_sha256": bars_sha,
                "sidecar_relative_path": sidecar_path.relative_to(native_root).as_posix(),
                "sidecar_sha256": sidecar_sha,
            }
        symbol_results[symbol] = {
            "status": provenance,
            "broker_symbol": terms.get("broker_symbol"),
            "terms_record_sha256": canonical_sha256(terms),
            "d1_bars_sha256": d1_sha,
            "schedule_relative_path": schedule_path.relative_to(native_root).as_posix(),
            "schedule_sha256": schedule_sha,
            "event_count": len(events),
            "target_bindings": target_bindings,
        }

    active_symbols = sorted(
        symbol for symbol, value in symbol_results.items()
        if value.get("status") in {AUTHENTICATED, STRESS_APPROVED}
    )
    if not active_symbols:
        raise BrokerTermsError("no symbol has an active D1-bound terms schedule")
    manifest: Dict[str, Any] = {
        "schema_version": TERMS_ACTIVATION_SCHEMA,
        "activation_id": activation_id,
        "status": "active",
        "created_at_utc": _iso(now),
        "provenance_status": provenance,
        "price_activation_id": price["activation_id"],
        "price_activation_manifest_sha256": price["manifest_sha256"],
        "price_active_pointer_sha256_at_creation": pointer_sha_at_start,
        "terms_snapshot_relative_path": snapshot_path.relative_to(native_root).as_posix(),
        "terms_snapshot_sha256": snapshot_sha,
        "schedule_contract": D1_SESSION_SCHEDULE_SCHEMA,
        "projection_contract": SWAP_PROJECTION_CONTRACT,
        "symbols": symbol_results,
        "active_symbols": active_symbols,
    }
    manifest_path = terms_root / "activations" / f"{activation_id}.json"
    _atomic_create_json(manifest_path, manifest)
    manifest_sha = sha256_file(manifest_path)
    if sha256_file(native_root / "ACTIVE.json") != pointer_sha_at_start:
        raise BrokerTermsError("native price pointer changed during terms activation")
    if update_pointer:
        pointer = {
            "schema_version": TERMS_ACTIVE_POINTER_SCHEMA,
            "activation_id": activation_id,
            "manifest_relative_path": manifest_path.relative_to(native_root).as_posix(),
            "manifest_sha256": manifest_sha,
            "price_activation_manifest_sha256": price["manifest_sha256"],
            "activated_at_utc": _iso(now),
        }
        _atomic_replace_json(terms_root / "ACTIVE.json", pointer)
    return {
        **manifest,
        "manifest_path": str(manifest_path),
        "manifest_sha256": manifest_sha,
        "pointer_updated": bool(update_pointer),
    }


def resolve_active_swap_contract(native_root: Path, symbol: str, timeframe: str) -> Dict[str, Any]:
    native_root = Path(native_root).resolve()
    terms_root = native_root / "terms"
    pointer = _load_json_bounded(terms_root / "ACTIVE.json", 1024 * 1024)
    if str(pointer.get("schema_version") or "") != TERMS_ACTIVE_POINTER_SCHEMA:
        raise BrokerTermsError("terms pointer schema is invalid")
    manifest_path = _safe_relative(native_root, pointer.get("manifest_relative_path"))
    manifest_sha = sha256_file(manifest_path)
    if manifest_sha != str(pointer.get("manifest_sha256") or ""):
        raise BrokerTermsError("terms activation manifest hash mismatch")
    manifest = _load_json_bounded(manifest_path)
    if (
        str(manifest.get("schema_version") or "") != TERMS_ACTIVATION_SCHEMA
        or str(manifest.get("status") or "").lower() != "active"
        or str(manifest.get("activation_id") or "") != str(pointer.get("activation_id") or "")
    ):
        raise BrokerTermsError("terms activation manifest identity/schema is invalid")
    price = load_current_price_activation(native_root)
    if str(manifest.get("price_activation_manifest_sha256") or "") != price["manifest_sha256"]:
        raise BrokerTermsError("terms overlay is bound to a different price activation")
    if str(pointer.get("price_activation_manifest_sha256") or "") != price["manifest_sha256"]:
        raise BrokerTermsError("terms pointer price binding mismatch")
    canonical = str(symbol or "").strip().upper()
    tf = str(timeframe or "").strip().upper()
    record = dict((manifest.get("symbols") or {}).get(canonical) or {})
    if record.get("status") not in {AUTHENTICATED, STRESS_APPROVED}:
        raise BrokerTermsError(f"{canonical}: no admissible active terms schedule")
    binding = dict((record.get("target_bindings") or {}).get(tf) or {})
    price_cell = dict(((price["manifest"].get("entries") or {}).get(canonical) or {}).get(tf) or {})
    if (
        str(price_cell.get("status") or "").lower() != "active"
        or binding.get("bars_sha256") != price_cell.get("bars_sha256")
        or binding.get("sidecar_sha256") != price_cell.get("sidecar_sha256")
    ):
        raise BrokerTermsError(f"{canonical} {tf}: target price binding mismatch")
    target_bars_path = _safe_relative(native_root, binding.get("bars_relative_path"))
    target_sidecar_path = _safe_relative(native_root, binding.get("sidecar_relative_path"))
    if (
        sha256_file(target_bars_path) != binding.get("bars_sha256")
        or sha256_file(target_sidecar_path) != binding.get("sidecar_sha256")
    ):
        raise BrokerTermsError(f"{canonical} {tf}: target artifact hash mismatch")
    schedule_path = _safe_relative(native_root, record.get("schedule_relative_path"))
    if sha256_file(schedule_path) != str(record.get("schedule_sha256") or ""):
        raise BrokerTermsError("session schedule hash mismatch")
    schedule = _load_json_bounded(schedule_path)
    if (
        str(schedule.get("schema_version") or "") != D1_SESSION_SCHEDULE_SCHEMA
        or schedule.get("price_activation_manifest_sha256") != price["manifest_sha256"]
        or schedule.get("terms_snapshot_sha256") != manifest.get("terms_snapshot_sha256")
    ):
        raise BrokerTermsError("session schedule binding/schema mismatch")
    snapshot_path = _safe_relative(native_root, manifest.get("terms_snapshot_relative_path"))
    if sha256_file(snapshot_path) != str(manifest.get("terms_snapshot_sha256") or ""):
        raise BrokerTermsError("terms snapshot hash mismatch")
    snapshot = _load_json_bounded(snapshot_path)
    if str(snapshot.get("schema_version") or "") != TERMS_SNAPSHOT_SCHEMA:
        raise BrokerTermsError("active terms snapshot schema mismatch")
    terms = dict((snapshot.get("symbols") or {}).get(canonical) or {})
    if canonical_sha256(terms) != record.get("terms_record_sha256"):
        raise BrokerTermsError("symbol terms record hash mismatch")
    stress_policy = dict(snapshot.get("stress_policy") or {})
    stress_approved = record["status"] == STRESS_APPROVED
    return {
        "status": "AVAILABLE",
        "schedule_provenance_status": record["status"],
        "source_provenance_authenticated": record["status"] == AUTHENTICATED,
        "stress_approved": stress_approved,
        "stress_policy_immutable": stress_approved and bool(stress_policy),
        "stress_policy_sha256": (
            canonical_sha256(stress_policy)
            if stress_approved and stress_policy
            else ""
        ),
        "price_activation_id": price["activation_id"],
        "price_activation_manifest_sha256": price["manifest_sha256"],
        "terms_activation_id": manifest.get("activation_id"),
        "terms_activation_manifest_sha256": manifest_sha,
        "terms_snapshot_sha256": manifest.get("terms_snapshot_sha256"),
        "schedule_sha256": record.get("schedule_sha256"),
        "target_bars_sha256": binding.get("bars_sha256"),
        "target_sidecar_sha256": binding.get("sidecar_sha256"),
        "projection_contract": SWAP_PROJECTION_CONTRACT,
        "terms": terms,
        "events": list(schedule.get("events") or []),
    }


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


def prune_terms_store(
    native_root: Path,
    *,
    max_snapshots: int = 16,
    max_total_bytes: int = 64 * 1024 * 1024,
    referenced_sha256: Iterable[str] = (),
) -> Dict[str, Any]:
    """Bound unreferenced terms artifacts while preserving active evidence."""
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
