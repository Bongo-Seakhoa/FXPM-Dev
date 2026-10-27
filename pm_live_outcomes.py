"""Durable live-entry attribution and position-level outcome accounting.

The module has no native order or promotion API. Pre-send reservations and unresolved
fills live in the fsynced state file. Closed outcomes move to the append-only
ledger before state cleanup. Any ambiguous or corrupt transition fails closed.
Local PAPER transitions use that same state and require a bound PAPER namespace.
"""

from __future__ import annotations

import json
import hashlib
import math
import os
import re
import threading
import uuid
from copy import deepcopy
from dataclasses import dataclass, field
from datetime import date, datetime, timezone, timedelta
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Mapping, Optional, Set, Tuple, Union

from pm_position import TradeTagEncoder


LEGACY_STATE_SCHEMA_VERSION = "2026-07-10-live-outcome-state-v1"
STATE_SCHEMA_VERSION = "2026-07-14-live-outcome-state-v2"
RUNTIME_STATE_SCHEMA_VERSION = "fxpm.runtime-account-outcome-state.v3"
LEDGER_SCHEMA_VERSION = "2026-07-10-matched-position-close-v1"
RUNTIME_LEDGER_SCHEMA_VERSION = "fxpm.runtime-account-matched-position-close.v2"
RUNTIME_IDENTITY_SCHEMA = "fxpm.runtime-account-identity.v1"
RESERVATION_SCHEMA_VERSION = "fxpm.live-entry-reservation.v1"

# Evidence is never silently pruned. Capacity exhaustion blocks new entries.
MAX_STATE_BYTES = 32 * 1024 * 1024
MAX_LEDGER_BYTES = 512 * 1024 * 1024
MAX_LEDGER_RECORD_BYTES = 256 * 1024
MAX_LEDGER_RECORDS = 250_000
MAX_LEDGER_INDEX_RECORDS = 250_000
MAX_PENDING_POSITIONS = 10_000
MAX_PENDING_EXIT_DEALS = 50_000
MAX_ENTRY_INTENT_ALIASES = 30_000
MAX_ENTRY_RESERVATIONS = 5_000
MAX_STATE_ID_VALUES = 250_000
MAX_TEXT_VALUE = 4_096
MAX_PAPER_LEVEL_HISTORY = 1024
PAPER_FILL_POLICY_ID = "PAPER_NATIVE_FIRST_TOUCH_SL_QUOTE_TP_LIMIT_V1"
PAPER_QUOTE_AUDIT_SCHEMA = "fxpm.paper-quote-audit.v1"

_RESERVATION_ID_RE = re.compile(r"^[0-9a-f]{32}$")
_INTENT_ALIAS_RE = re.compile(r"^(?:paper-)?(position|deal|order):[1-9][0-9]*$")
_HEX64_RE = re.compile(r"^[0-9a-f]{64}$")
_DEMO_BOOK_ROLE = "PROCESS_VALIDATED_CURRENT_REFIT"
_TIMEFRAMES = frozenset({"M5", "M15", "M30", "H1", "H4", "D1"})
_REGIMES = frozenset({"TREND", "RANGE", "BREAKOUT", "CHOP"})
_BOOK_ROLES = frozenset(
    {
        "LOCKBOX_VALIDATED_CHAMPION",
        "FROZEN_PRELOCKBOX_CHAMPION",
        "LATEST_DATA_EDGE_CHALLENGER",
        _DEMO_BOOK_ROLE,
    }
)
_RUNTIME_WRITERS_LOCK = threading.RLock()
_RUNTIME_WRITERS: Dict[str, Any] = {}


class LiveOutcomeError(RuntimeError):
    """Raised when durable outcome evidence cannot be trusted."""


def _reject_json_constant(value: str) -> None:
    raise ValueError(f"non-finite JSON constant {value}")


def _reject_duplicate_keys(pairs: List[Tuple[str, Any]]) -> Dict[str, Any]:
    result: Dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"duplicate JSON key {key}")
        result[key] = value
    return result


def _strict_json_loads(raw: Union[str, bytes]) -> Any:
    return json.loads(
        raw,
        object_pairs_hook=_reject_duplicate_keys,
        parse_constant=_reject_json_constant,
    )


def _canonical_json_bytes(payload: Any) -> bytes:
    return json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("utf-8")


@dataclass(frozen=True, slots=True)
class RuntimeAccountIdentity:
    login: int
    server: str
    currency: str
    destination: str

    def __post_init__(self) -> None:
        if type(self.login) is not int or self.login <= 0:
            raise LiveOutcomeError("runtime account login is unavailable")
        if (type(self.server) is not str or not self.server or self.server != self.server.strip()
                or len(self.server) > 256 or any(ord(c) < 32 for c in self.server)):
            raise LiveOutcomeError("runtime account server is unavailable")
        if type(self.currency) is not str or re.fullmatch(r"[A-Z0-9]{3,12}", self.currency) is None:
            raise LiveOutcomeError("runtime account currency is unavailable")
        if type(self.destination) is not str or self.destination not in {"PAPER", "MT5_DEMO", "FUNDED"}:
            raise LiveOutcomeError("runtime execution destination is invalid")

    def _body(self) -> Dict[str, Any]:
        return {"schema_version": RUNTIME_IDENTITY_SCHEMA, "login": self.login,
                "server": self.server, "currency": self.currency, "destination": self.destination}

    @property
    def namespace(self) -> str:
        return self.destination.lower() + "-" + hashlib.sha256(_canonical_json_bytes(self._body())).hexdigest()

    def to_dict(self) -> Dict[str, Any]:
        return {**self._body(), "namespace": self.namespace}


def runtime_account_identity(actual_account_info: Any, destination: str) -> RuntimeAccountIdentity:
    """Bind an actual account sample without changing its broker trade mode."""
    def observed(name: str) -> Any:
        if isinstance(actual_account_info, Mapping):
            return actual_account_info.get(name)
        return getattr(actual_account_info, name, None)

    identity = RuntimeAccountIdentity(observed("login"), observed("server"), observed("currency"), destination)
    mode = observed("trade_mode")
    if type(mode) is not int or mode not in {0, 1, 2}:
        raise LiveOutcomeError("actual account trade mode is unavailable")
    if (destination == "MT5_DEMO" and mode != 0) or (destination == "FUNDED" and mode != 2):
        raise LiveOutcomeError("execution destination does not match actual account mode")
    return identity


def _runtime_identity_from_mapping(raw: Any) -> RuntimeAccountIdentity:
    if type(raw) is not dict or set(raw) != {
        "schema_version", "login", "server", "currency", "destination", "namespace"
    }:
        raise ValueError("invalid runtime account identity schema")
    identity = RuntimeAccountIdentity(raw["login"], raw["server"], raw["currency"], raw["destination"])
    if identity.to_dict() != raw:
        raise ValueError("runtime account identity mismatch")
    return identity


class _RuntimeWriterLease:
    """One account/destination writer, owned by its constructing Windows thread."""

    def __init__(self, identity: RuntimeAccountIdentity) -> None:
        if os.name != "nt":
            raise LiveOutcomeError("scoped outcome writer requires a Windows named mutex")
        import ctypes

        self.key = identity.namespace
        self.owner_thread = threading.current_thread()
        self.closed = False
        self.blocked = False
        self.abandoned = False
        self.kernel = ctypes.WinDLL("kernel32", use_last_error=True)
        self.kernel.CreateMutexW.argtypes = [ctypes.c_void_p, ctypes.c_int, ctypes.c_wchar_p]
        self.kernel.CreateMutexW.restype = ctypes.c_void_p
        self.kernel.WaitForSingleObject.argtypes = [ctypes.c_void_p, ctypes.c_uint32]
        self.kernel.WaitForSingleObject.restype = ctypes.c_uint32
        self.kernel.ReleaseMutex.argtypes = [ctypes.c_void_p]
        self.kernel.ReleaseMutex.restype = ctypes.c_int
        self.kernel.CloseHandle.argtypes = [ctypes.c_void_p]
        self.kernel.CloseHandle.restype = ctypes.c_int
        with _RUNTIME_WRITERS_LOCK:
            if self.key in _RUNTIME_WRITERS:
                raise LiveOutcomeError("runtime outcome writer already exists in this process")
            handle = self.kernel.CreateMutexW(None, False, "Local\\FXPM.OutcomeWriter." + self.key)
            if not handle:
                raise LiveOutcomeError("cannot create runtime outcome writer mutex")
            status = self.kernel.WaitForSingleObject(handle, 0)
            if status not in {0, 0x80}:
                self.kernel.CloseHandle(handle)
                raise LiveOutcomeError("runtime outcome writer is busy or unavailable")
            self.handle = handle
            self.abandoned = status == 0x80
            _RUNTIME_WRITERS[self.key] = self

    def verify(self) -> None:
        with _RUNTIME_WRITERS_LOCK:
            if (self.closed or self.blocked or not self.owner_thread.is_alive()
                    or _RUNTIME_WRITERS.get(self.key) is not self):
                raise LiveOutcomeError("runtime outcome writer lease is not usable")

    def close(self) -> None:
        with _RUNTIME_WRITERS_LOCK:
            if self.closed:
                return
            if threading.current_thread() is not self.owner_thread:
                self.blocked = True
                raise LiveOutcomeError("runtime outcome writer must close on its acquiring thread")
            if not self.kernel.ReleaseMutex(self.handle):
                self.blocked = True
                raise LiveOutcomeError("runtime outcome writer mutex release failed")
            self.closed = True
            _RUNTIME_WRITERS.pop(self.key, None)
            if not self.kernel.CloseHandle(self.handle):
                raise LiveOutcomeError("runtime outcome writer handle close failed")


def _bounded_management_json(raw: Any) -> Any:
    encoded = _canonical_json_bytes(raw)
    if len(encoded) > 64 * 1024:
        raise ValueError("held management data exceeds its byte bound")

    def check(value: Any, depth: int = 0) -> None:
        if depth > 8:
            raise ValueError("held management data is too deeply nested")
        if value is None or type(value) is bool:
            return
        if type(value) in {int, float}:
            if not math.isfinite(value):
                raise ValueError("held management data has a non-finite number")
            return
        if type(value) is str:
            if len(value) > MAX_TEXT_VALUE or any(ord(character) < 32 for character in value):
                raise ValueError("held management text is invalid")
            return
        if type(value) is list and len(value) <= 256:
            for item in value:
                check(item, depth + 1)
            return
        if type(value) is dict and len(value) <= 256:
            for key, item in value.items():
                if (type(key) is not str or not key or len(key) > 128
                        or any(word in key.lower() for word in
                               ("config", "context", "password", "secret", "token", "credential", "api_key"))):
                    raise ValueError("held management data contains a forbidden field")
                if key.lower() in {
                    "new_entry_authority", "increase_authority", "stop_widening_authority",
                    "allow_new_entries", "allow_increase", "allow_risk_increase", "allow_stop_widening",
                    "can_open", "can_increase", "can_widen_stop", "live_authority", "order_authority",
                    "capital_authority", "qualification_authority", "funded_live_execution",
                } and item is not False:
                    raise ValueError("held management data cannot authorize new risk")
                check(item, depth + 1)
            return
        raise ValueError("held management data contains a non-JSON value")

    check(raw)
    return _strict_json_loads(encoded)


def _owner_snapshot(raw: Any) -> Optional[Dict[str, Any]]:
    if raw is None:
        return None
    fields = {"schema_version", "classification", "symbol", "timeframe", "regime", "strategy_name",
              "strategy_module", "strategy_class", "parameters", "governance", "book_role", "candidate_id",
              "source_refs", "new_entry_authority", "increase_authority", "stop_widening_authority"}
    if (type(raw) is not dict or not fields.issubset(raw) or not set(raw).issubset(fields | {"detector"})
            or raw["schema_version"] != "fxpm.held-owner-data.v1"
            or raw["classification"] != "MANAGEMENT_DATA_ONLY_NOT_ENTRY_AUTHORITY"
            or any(raw[name] is not False for name in
                   ("new_entry_authority", "increase_authority", "stop_widening_authority"))):
        raise ValueError("invalid data-only held-owner snapshot")
    if (type(raw["timeframe"]) is not str or raw["timeframe"] not in _TIMEFRAMES
            or type(raw["regime"]) is not str or raw["regime"] not in _REGIMES
            or type(raw["book_role"]) is not str or raw["book_role"] not in _BOOK_ROLES
            or type(raw["candidate_id"]) is not str
            or any(type(raw[name]) is not str or not raw[name] for name in
                   ("symbol", "strategy_name", "strategy_module", "strategy_class"))
            or type(raw["parameters"]) is not dict or type(raw["governance"]) is not dict
            or type(raw["source_refs"]) is not dict
            or any(type(key) is not str or type(value) is not str for key, value in raw["source_refs"].items())):
        raise ValueError("invalid held-owner strategy data")
    if "detector" in raw:
        detector = raw["detector"]
        if (type(detector) is not dict or set(detector) != {"parameters", "parameters_sha256", "detector_code_sha256"}
                or type(detector["parameters"]) is not dict
                or any(type(detector[name]) is not str or not _HEX64_RE.fullmatch(detector[name])
                       for name in ("parameters_sha256", "detector_code_sha256"))):
            raise ValueError("invalid held-owner detector data")
    return _bounded_management_json(raw)


def _funding_terms(raw: Any) -> Optional[Dict[str, Any]]:
    if raw is None:
        return None
    allowed = {"schema_version", "symbol", "broker_symbol", "swap_mode", "swap_long", "swap_short",
               "swap_rollover3days", "point", "tick_size", "tick_value", "contract_size", "pip_size",
               "currency", "profit_currency", "base_currency", "margin_currency", "rollover_hour_utc",
               "rollover_timezone", "rollover_schedule", "source_ref", "observed_at", "provenance_sha256",
               "coverage_start_utc", "coverage_end_utc"}
    if (type(raw) is not dict or not set(raw).issubset(allowed)
            or raw.get("schema_version") != "fxpm.paper-funding-terms.v1"):
        raise ValueError("invalid original paper funding terms")
    if "rollover_timezone" in raw and raw["rollover_timezone"] != "UTC":
        raise ValueError("paper funding rollover timezone must be explicitly UTC")
    coverage = []
    for name in ("coverage_start_utc", "coverage_end_utc"):
        if name in raw:
            value = raw[name]
            try:
                if type(value) is not str or not value.endswith("Z") or _iso_utc(value) != value:
                    raise ValueError("noncanonical UTC coverage")
                coverage.append(datetime.fromisoformat(value[:-1] + "+00:00"))
            except (TypeError, ValueError) as exc:
                raise ValueError("paper funding coverage must use canonical UTC timestamps") from exc
    if coverage and (len(coverage) != 2 or coverage[0] >= coverage[1]):
        raise ValueError("paper funding coverage is incomplete or reversed")
    return _bounded_management_json(raw)


def _paper_cursor(raw: Any) -> Dict[str, Any]:
    if (type(raw) is not dict or set(raw) != {"time_msc", "same_msc_ordinal", "prefix_sha256"}
            or type(raw["time_msc"]) is not int or not 100_000_000_000 <= raw["time_msc"] < 2**63
            or not _iso_utc(raw["time_msc"])
            or type(raw["same_msc_ordinal"]) is not int or not 0 <= raw["same_msc_ordinal"] < 2**31
            or type(raw["prefix_sha256"]) is not str or not _HEX64_RE.fullmatch(raw["prefix_sha256"])):
        raise ValueError("invalid paper quote cursor")
    return dict(raw)


def _paper_cursor_order(previous: Mapping[str, Any], current: Mapping[str, Any]) -> None:
    before = previous["time_msc"], previous["same_msc_ordinal"]
    after = current["time_msc"], current["same_msc_ordinal"]
    if after < before or (after == before and current["prefix_sha256"] != previous["prefix_sha256"]):
        raise LiveOutcomeError("paper quote cursor moved backward or changed its recorded prefix")


def _paper_execution_evidence(raw: Any) -> Dict[str, Any]:
    recovery = type(raw) is dict and raw.get("event") == "MANUAL_RECOVERY_CLOSE"
    fields = {"model_id", "bid", "ask", "ask_source", "event"}
    if recovery:
        fields |= {"terminal_audit_status", "terminal_audit_code"}
    if (type(raw) is not dict or set(raw) != fields
            or any(type(raw[name]) is not str for name in ("model_id", "ask_source", "event"))
            or raw["model_id"] != PAPER_FILL_POLICY_ID or raw["ask_source"] not in {"NATIVE", "MODELLED"}
            or raw["event"] not in {"ENTRY", "SL", "TP", "MANUAL_CLOSE", "PARTIAL_CLOSE",
                                    "LEVEL_CHANGE", "ADVANCE", "MANUAL_RECOVERY_CLOSE"}):
        raise ValueError("invalid paper execution evidence")
    if recovery and (raw["ask_source"] != "NATIVE"
            or type(raw["terminal_audit_status"]) is not str
            or raw["terminal_audit_status"] not in {"DIVERGENT", "UNAVAILABLE"}
            or type(raw["terminal_audit_code"]) is not str
            or re.fullmatch(r"[A-Z][A-Z0-9_]{0,127}", raw["terminal_audit_code"]) is None):
        raise ValueError("invalid paper recovery terminal evidence")
    values = {}
    for name in ("bid", "ask"):
        value = raw[name]
        if type(value) not in {int, float} or not math.isfinite(value) or value <= 0:
            raise ValueError("invalid paper execution quote")
        values[name] = float(value)
    if values["ask"] < values["bid"]:
        raise ValueError("crossed paper execution quote")
    return {**raw, **values}


def _paper_same_quote(previous: Mapping[str, Any], current: Mapping[str, Any]) -> bool:
    return all(previous[name] == current[name] for name in ("model_id", "bid", "ask", "ask_source"))


def _paper_quote_audit(value: Any, entry_msc: int) -> Optional[Dict[str, Any]]:
    if value is None:
        return None
    fields = {
        "schema_version", "pending_minute_msc", "processed_count", "prefix_sha256",
        "corroborated_minute_count", "last_corroborated_minute_msc",
        "first_failure_minute_msc", "first_failure_code", "first_failure_status",
        "terminal_failure_minute_msc", "terminal_failure_code", "status", "overdue",
    }
    if type(value) is not dict or set(value) != fields:
        raise ValueError("invalid paper quote audit schema")
    audit = deepcopy(value)
    if audit["schema_version"] != PAPER_QUOTE_AUDIT_SCHEMA:
        raise ValueError("unsupported paper quote audit schema")
    if type(audit["status"]) is not str or audit["status"] not in {"PENDING", "CLEAN", "DIVERGENT", "UNAVAILABLE"}:
        raise ValueError("invalid paper quote audit status")
    if type(audit["overdue"]) is not bool:
        raise ValueError("invalid paper quote audit overdue marker")
    for name in ("processed_count", "corroborated_minute_count"):
        if type(audit[name]) is not int or not 0 <= audit[name] < 2**63:
            raise ValueError("invalid paper quote audit count")
    first_minute = entry_msc // 60000 * 60000

    def minute(value):
        return (type(value) is int and value >= first_minute and value % 60000 == 0
                and bool(_iso_utc(value)))

    count, last = audit["corroborated_minute_count"], audit["last_corroborated_minute_msc"]
    if (count == 0 and last is not None) or (count > 0 and (
            not minute(last) or last != first_minute + (count - 1) * 60000)):
        raise ValueError("paper quote audit corroboration is not contiguous")
    pending = audit["pending_minute_msc"]
    if pending is None:
        if audit["processed_count"] != 0 or audit["prefix_sha256"] is not None:
            raise ValueError("paper quote audit has a prefix without a pending minute")
    elif (not minute(pending) or pending != first_minute + count * 60000
            or type(audit["prefix_sha256"]) is not str
            or not _HEX64_RE.fullmatch(audit["prefix_sha256"])):
        raise ValueError("invalid paper quote audit pending prefix")
    failure = tuple(audit[name] for name in (
        "first_failure_minute_msc", "first_failure_code", "first_failure_status"))
    if any(item is not None for item in failure):
        if (not minute(failure[0]) or type(failure[1]) is not str
                or re.fullmatch(r"[A-Z][A-Z0-9_]{0,127}", failure[1]) is None
                or type(failure[2]) is not str
                or failure[2] not in {"RETRYABLE", "DIVERGENT", "UNAVAILABLE"}):
            raise ValueError("invalid paper quote audit first failure")
    terminal = (audit["terminal_failure_minute_msc"], audit["terminal_failure_code"])
    terminal_status = audit["status"] in {"DIVERGENT", "UNAVAILABLE"}
    if terminal_status:
        if (not minute(terminal[0]) or type(terminal[1]) is not str
                or re.fullmatch(r"[A-Z][A-Z0-9_]{0,127}", terminal[1]) is None
                or failure[0] is None):
            raise ValueError("terminal paper quote audit lacks its retained cause")
        if failure[2] in {"DIVERGENT", "UNAVAILABLE"} and (
                audit["status"] != failure[2] or terminal != failure[:2]):
            raise ValueError("paper quote audit changed its first terminal failure")
    elif any(item is not None for item in terminal) or failure[2] in {"DIVERGENT", "UNAVAILABLE"}:
        raise ValueError("paper quote audit hides a terminal failure")
    if audit["status"] == "CLEAN" and (pending is not None or count == 0 or audit["overdue"]):
        raise ValueError("clean paper quote audit is incomplete")
    return audit


def _paper_incident_time(value=None) -> str:
    if value is None:
        value = datetime.now(timezone.utc)
    if type(value) is str:
        if not value.endswith("Z"):
            raise ValueError("paper binding incident time must be canonical UTC")
        try:
            parsed = datetime.fromisoformat(value[:-1] + "+00:00")
        except ValueError as exc:
            raise ValueError("invalid paper binding incident time") from exc
        if parsed.isoformat().replace("+00:00", "Z") != value:
            raise ValueError("paper binding incident time must be canonical UTC")
        return value
    if (not isinstance(value, datetime) or value.tzinfo is None
            or value.utcoffset() != timedelta(0)):
        raise ValueError("paper binding incident time must be UTC")
    return value.isoformat().replace("+00:00", "Z")


def _paper_incident_identity(value, *, bound=False):
    if value is None and not bound:
        return None
    fields = {"login", "server", "currency"} | ({"destination"} if bound else set())
    if type(value) is not dict or set(value) != fields:
        raise ValueError("invalid paper binding incident identity fields")
    if type(value["login"]) is not int or not 0 < value["login"] < 2**63:
        raise ValueError("invalid paper binding incident login")
    for name, limit in (("server", 256), ("currency", 16)):
        text = value[name]
        if (type(text) is not str or not 0 < len(text) <= limit or text != text.strip()
                or any(not char.isprintable() for char in text)):
            raise ValueError("invalid paper binding incident identity text")
    if bound and value["destination"] != "PAPER":
        raise ValueError("paper binding incident has another destination")
    return deepcopy(value)


def _paper_binding_incident(value, identity):
    if value is None:
        return None
    fields = {"first_seen_utc", "last_seen_utc", "count", "observed_identity", "bound_identity", "reason"}
    if type(value) is not dict or set(value) != fields:
        raise ValueError("invalid paper binding incident schema")
    if any(type(value[name]) is not str for name in ("first_seen_utc", "last_seen_utc")):
        raise ValueError("paper binding incident timestamps must be strings")
    first, last = (_paper_incident_time(value[name]) for name in ("first_seen_utc", "last_seen_utc"))
    if datetime.fromisoformat(first[:-1] + "+00:00") > datetime.fromisoformat(last[:-1] + "+00:00"):
        raise ValueError("paper binding incident times moved backwards")
    if type(value["count"]) is not int or not 1 <= value["count"] < 2**63:
        raise ValueError("invalid paper binding incident count")
    bound = _paper_incident_identity(value["bound_identity"], bound=True)
    expected = {name: getattr(identity, name) for name in ("login", "server", "currency", "destination")}
    if bound != expected or identity.destination != "PAPER":
        raise ValueError("paper binding incident belongs to another namespace")
    _paper_incident_identity(value["observed_identity"])
    reason = value["reason"]
    if (type(reason) is not str or not 0 < len(reason) <= 256 or reason != reason.strip()
            or any(not char.isprintable() for char in reason)):
        raise ValueError("invalid paper binding incident reason")
    return deepcopy(value)


def _paper_recovery_quote(position, evidence, price, when):
    evidence = _paper_execution_evidence(evidence)
    audit = position["quote_audit"]
    held = position["quote_cursor"]
    if (evidence["event"] != "MANUAL_RECOVERY_CLOSE" or held is None or audit is None
            or audit["status"] not in {"DIVERGENT", "UNAVAILABLE"}
            or (position["last_execution_evidence"] or {}).get("event") in {"SL", "TP"}
            or evidence["terminal_audit_status"] != audit["status"]
            or evidence["terminal_audit_code"] != audit["terminal_failure_code"]):
        raise LiveOutcomeError("paper recovery close requires its terminal audit and no saved trigger")
    if when < held["time_msc"] or price != evidence["bid" if position["type"] == 0 else "ask"]:
        raise LiveOutcomeError("paper recovery close must use its fresh native side quote and time")
    return evidence


def _paper_require_ordinary_mutation(position, *, saved_trigger_settlement=False):
    audit = position.get("quote_audit")
    if audit is not None and audit["status"] in {"DIVERGENT", "UNAVAILABLE"}:
        saved = (position.get("last_execution_evidence") or {}).get("event") in {"SL", "TP"}
        if not (saved_trigger_settlement and saved):
            raise LiveOutcomeError("terminal paper quote audit requires explicit manual recovery")


@dataclass
class OutcomeSyncResult:
    emitted_records: List[Dict[str, Any]] = field(default_factory=list)
    ingested_deal_count: int = 0
    duplicate_deal_count: int = 0
    pending_position_count: int = 0
    delivered_drift_count: int = 0
    fail_closed_reasons: List[str] = field(default_factory=list)
    state_recovered: bool = False
    paper_statistics: Dict[str, Any] = field(default_factory=dict)


def _safe_int(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError, OverflowError):
        return 0


def _safe_float(value: Any) -> float:
    try:
        result = float(value or 0.0)
    except (TypeError, ValueError, OverflowError):
        return 0.0
    return result if math.isfinite(result) else 0.0


def _iso_utc(value: Any) -> str:
    if value is None or value == "":
        return ""
    if isinstance(value, (int, float)):
        try:
            numeric = float(value)
            # MT5 exposes both seconds (`time`) and milliseconds
            # (`time_msc`). Normalize either without trusting the caller to
            # label which unit it supplied.
            if abs(numeric) >= 100_000_000_000:
                numeric /= 1000.0
            value = datetime.fromtimestamp(numeric, tz=timezone.utc)
        except (OSError, OverflowError, ValueError):
            return ""
    if isinstance(value, date) and not isinstance(value, datetime):
        value = datetime.combine(value, datetime.min.time(), tzinfo=timezone.utc)
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        else:
            value = value.astimezone(timezone.utc)
        return value.isoformat().replace("+00:00", "Z")
    raw = str(value).strip()
    if not raw:
        return ""
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return raw
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _entry_kind(value: Any) -> str:
    if isinstance(value, str):
        normalized = value.strip().upper()
        aliases = {
            "DEAL_ENTRY_IN": "IN",
            "ENTRY_IN": "IN",
            "DEAL_ENTRY_OUT": "OUT",
            "ENTRY_OUT": "OUT",
            "DEAL_ENTRY_INOUT": "INOUT",
            "ENTRY_INOUT": "INOUT",
            "DEAL_ENTRY_OUT_BY": "OUT_BY",
            "ENTRY_OUT_BY": "OUT_BY",
        }
        if normalized in aliases:
            return aliases[normalized]
        if normalized in {"IN", "OUT", "INOUT", "OUT_BY"}:
            return normalized
    numeric = _safe_int(value)
    return {0: "IN", 1: "OUT", 2: "INOUT", 3: "OUT_BY"}.get(numeric, "UNKNOWN")


class LiveOutcomeLedger:
    """Single-writer, durable MT5 position-close ledger and drift outbox."""

    def __init__(
        self,
        config: Any,
        *,
        ledger_path: Optional[Union[str, Path]] = None,
        state_path: Optional[Union[str, Path]] = None,
        runtime_identity: Optional[RuntimeAccountIdentity] = None,
    ) -> None:
        if runtime_identity is not None and type(runtime_identity) is not RuntimeAccountIdentity:
            raise LiveOutcomeError("runtime account identity must be verified and typed")
        self.runtime_identity = runtime_identity
        self.config = config
        base_dir = Path(getattr(config, "output_dir", "pm_outputs")) / "production_live"
        if runtime_identity is not None and runtime_identity.namespace not in base_dir.parts:
            base_dir /= runtime_identity.namespace
        configured_ledger = str(getattr(config, "live_outcome_ledger_path", "") or "").strip()
        configured_state = str(getattr(config, "live_outcome_state_path", "") or "").strip()
        self.ledger_path = Path(ledger_path or configured_ledger or (base_dir / "closed_positions.jsonl"))
        self.state_path = Path(state_path or configured_state or (base_dir / "closed_positions.state.json"))
        if runtime_identity is not None and any(
            runtime_identity.namespace not in path.parts for path in (self.ledger_path, self.state_path)
        ):
            raise LiveOutcomeError("runtime ledger paths must be inside their account namespace")
        self.enabled = bool(getattr(config, "live_outcome_feedback_enabled", True))
        self.require_history = bool(getattr(config, "live_outcome_require_position_history", True))
        self.feed_cycle_drift = bool(getattr(config, "live_outcome_feed_cycle_drift", True))
        self.volume_tolerance = max(
            1e-12,
            _safe_float(getattr(config, "live_outcome_volume_tolerance", 1e-8)) or 1e-8,
        )
        # Auto-promotion is a prohibited capability, not a runtime option.
        self.auto_promotion_enabled = False
        self._lock = threading.RLock()
        self._closed = False
        self._writer_lease = _RuntimeWriterLease(runtime_identity) if runtime_identity is not None else None
        self._state_recovered = False
        self._state_disk_sha256: Optional[str] = None
        self._ledger_disk_size = 0
        self._ledger_record_count = 0
        self._ledger_records: Dict[str, Dict[str, Any]] = {}
        self._state: Dict[str, Any] = self._new_state()
        try:
            if runtime_identity is not None and not self.state_path.exists() and self.ledger_path.exists():
                raise LiveOutcomeError("scoped outcome ledger lacks its matching durable state")
            self._load_state()
            self._load_ledger_index()
            self._merge_ledger_index_into_state()
            if self._state_recovered:
                self._save_state()
        except BaseException as original:
            try:
                self.close()
            except Exception as cleanup_error:
                original.add_note(f"runtime writer cleanup failed: {type(cleanup_error).__name__}: {cleanup_error}")
            raise

    @staticmethod
    def _empty_state() -> Dict[str, Any]:
        return {
            "schema_version": STATE_SCHEMA_VERSION,
            "seen_deal_tickets": [],
            "emitted_position_ids": [],
            "drift_delivered_position_ids": [],
            "pending_positions": {},
            "entry_intents": {},
            "pre_send_reservations": {},
            "updated_at": "",
        }

    def _new_state(self) -> Dict[str, Any]:
        state = self._empty_state()
        if self.runtime_identity is not None:
            state.update(schema_version=RUNTIME_STATE_SCHEMA_VERSION,
                         runtime_identity=self.runtime_identity.to_dict(),
                         paper_positions={}, paper_deals={}, paper_operations={}, next_paper_ticket=1)
        if self.runtime_identity is not None and self.runtime_identity.destination == "PAPER":
            state["paper_binding_incident"] = None
        return state

    def _assert_runtime_state(self, state: Mapping[str, Any]) -> None:
        if self._closed:
            raise LiveOutcomeError("outcome ledger is closed")
        if self._writer_lease is not None:
            self._writer_lease.verify()
        observed = state.get("runtime_identity")
        expected = self.runtime_identity.to_dict() if self.runtime_identity is not None else None
        if observed != expected or (expected is not None and state.get("schema_version") != RUNTIME_STATE_SCHEMA_VERSION):
            raise LiveOutcomeError("runtime outcome namespace mismatch; unscoped histories are read-only")

    @property
    def runtime_writer_abandoned(self) -> bool:
        return bool(self._writer_lease is not None and self._writer_lease.abandoned)

    def close(self) -> None:
        with self._lock:
            if self._writer_lease is not None:
                self._writer_lease.close()
            self._closed = True

    def _intent_alias(self, kind: str, ticket: Any) -> str:
        prefix = "paper-" if self.runtime_identity is not None and self.runtime_identity.destination == "PAPER" else ""
        return f"{prefix}{kind}:{ticket}"

    def _assert_runtime_record(self, record: Mapping[str, Any]) -> None:
        expected = self.runtime_identity.to_dict() if self.runtime_identity is not None else None
        if record.get("runtime_identity") != expected:
            raise LiveOutcomeError("close ledger belongs to another runtime namespace")

    @classmethod
    def _validate_paper_state(cls, state: Dict[str, Any]) -> None:
        identity = _runtime_identity_from_mapping(state["runtime_identity"])
        positions, deals, operations = (state[name] for name in
                                         ("paper_positions", "paper_deals", "paper_operations"))
        if any(type(value) is not dict for value in (positions, deals, operations)):
            raise ValueError("paper state collections must be objects")
        if (len(positions) > MAX_PENDING_POSITIONS or len(deals) > MAX_PENDING_EXIT_DEALS
                or len(operations) > MAX_PENDING_EXIT_DEALS):
            raise ValueError("paper evidence capacity exhausted")
        counter = state["next_paper_ticket"]
        if type(counter) is not int or not 1 <= counter <= 2**63 - 1:
            raise ValueError("invalid paper ticket counter")
        if identity.destination != "PAPER":
            if positions or deals or operations or counter != 1:
                raise ValueError("broker namespace cannot contain local paper transitions")
            return
        _paper_binding_incident(state.get("paper_binding_incident"), identity)
        position_fields = {
            "ticket", "identifier", "position_id", "paper_position_id", "destination", "runtime_identity",
            "reservation_id", "fill_sha256", "symbol", "original_symbol", "type", "magic", "comment",
            "volume_initial", "volume", "price_open", "sl", "tp", "initial_stop", "initial_take_profit", "risk_cash",
            "time", "time_msc", "opening_deal_ticket", "opening_order_ticket", "status", "owner_snapshot", "funding_terms",
            "entry_quote_cursor", "quote_cursor", "fill_policy_id", "entry_execution_evidence",
            "last_execution_evidence", "quote_transition_sha256", "level_history",
            "quote_audit", "statistics_delivered",
        }
        deal_fields = {
            "ticket", "order_ticket", "position_id", "paper_position_id", "destination", "runtime_identity",
            "symbol", "entry_kind", "type", "volume", "price", "profit", "commission", "swap", "fee",
            "magic", "comment", "reason", "time_msc",
            "quote_cursor", "execution_evidence", "fill_policy_id",
        }
        used_ids: Set[int] = set()
        reservations: Set[str] = set()
        for key, row in positions.items():
            if type(row) is not dict or set(row) != position_fields or cls._position_key(key) != key:
                raise ValueError("invalid local paper position schema")
            ticket = row["ticket"]
            if (type(ticket) is not int or str(ticket) != key or ticket <= 0
                    or type(row["identifier"]) is not int or row["identifier"] != ticket
                    or type(row["position_id"]) is not int or row["position_id"] != ticket
                    or row["paper_position_id"] != f"paper:{identity.namespace}:{ticket}"
                    or row["destination"] != "PAPER" or row["runtime_identity"] != identity.to_dict()):
                raise ValueError("local paper position identity mismatch")
            if row["reservation_id"] in reservations or not _RESERVATION_ID_RE.fullmatch(str(row["reservation_id"])):
                raise ValueError("duplicate or invalid paper reservation")
            reservations.add(row["reservation_id"])
            if not _HEX64_RE.fullmatch(str(row["fill_sha256"])):
                raise ValueError("invalid paper fill identity")
            for name in ("symbol", "original_symbol", "comment"):
                cls._bounded_text(row[name], name, required=name != "comment")
            if type(row["type"]) is not int or row["type"] not in {0, 1} or type(row["magic"]) is not int or row["magic"] <= 0:
                raise ValueError("invalid paper position direction or magic")
            for name in ("volume_initial", "price_open", "initial_stop"):
                cls._finite_number(row[name], name, positive=True)
            for name in ("volume", "sl", "tp", "initial_take_profit"):
                if cls._finite_number(row[name], name) < 0:
                    raise ValueError("negative local paper volume or stop")
            if row["volume"] > row["volume_initial"]:
                raise ValueError("paper position exceeds its original fill")
            if row["risk_cash"] is not None:
                cls._finite_number(row["risk_cash"], "risk_cash", positive=True)
            snapshot = _owner_snapshot(row["owner_snapshot"])
            if snapshot is not None and snapshot["symbol"] != row["original_symbol"]:
                raise ValueError("paper owner data changed its original symbol")
            _funding_terms(row["funding_terms"])
            if (type(row["time_msc"]) is not int or row["time_msc"] < 100_000_000_000
                    or row["time"] != _iso_utc(row["time_msc"])):
                raise ValueError("invalid paper opening time")
            _paper_quote_audit(row["quote_audit"], row["time_msc"])
            if type(row["statistics_delivered"]) is not bool:
                raise ValueError("invalid paper statistics delivery marker")
            if row["statistics_delivered"] and (row["status"] != "CLOSED" or key not in state["emitted_position_ids"]):
                raise ValueError("paper statistics delivery lacks a retained financial close")
            entry, current = row["entry_quote_cursor"], row["quote_cursor"]
            if entry is None:
                if any(row[name] is not None for name in ("quote_cursor", "fill_policy_id", "entry_execution_evidence",
                                                          "last_execution_evidence", "quote_transition_sha256")):
                    raise ValueError("partial tracking on an untracked paper position")
            else:
                entry, current = _paper_cursor(entry), _paper_cursor(current)
                _paper_cursor_order(entry, current)
                initial = _paper_execution_evidence(row["entry_execution_evidence"])
                last = _paper_execution_evidence(row["last_execution_evidence"])
                if (entry["time_msc"] != row["time_msc"] or row["fill_policy_id"] != PAPER_FILL_POLICY_ID
                        or initial["event"] != "ENTRY" or not _HEX64_RE.fullmatch(str(row["quote_transition_sha256"]))
                        or (entry == current and not _paper_same_quote(initial, last))):
                    raise ValueError("paper entry quote or model binding mismatch")
            levels = row["level_history"]
            if type(levels) is not list or not 1 <= len(levels) <= MAX_PAPER_LEVEL_HISTORY:
                raise ValueError("paper level history is missing or exceeds its bound")
            previous_cursor = entry
            for level in levels:
                if type(level) is not dict or set(level) != {"sl", "tp", "effective_cursor"}:
                    raise ValueError("invalid paper level history schema")
                for name in ("sl", "tp"):
                    if cls._finite_number(level[name], name) < 0:
                        raise ValueError("negative paper level history")
                effective = level["effective_cursor"]
                if entry is None:
                    if effective is not None:
                        raise ValueError("untracked paper history cannot invent an effective cursor")
                else:
                    effective = _paper_cursor(effective)
                    _paper_cursor_order(previous_cursor, effective)
                    _paper_cursor_order(effective, current)
                    previous_cursor = effective
            if (levels[0] != {"sl": row["initial_stop"], "tp": row["initial_take_profit"], "effective_cursor": entry}
                    or levels[-1]["sl"] != row["sl"] or levels[-1]["tp"] != row["tp"]):
                raise ValueError("paper levels differ from their retained history")
            if row["status"] != ("OPEN" if row["volume"] > 0 else "CLOSED"):
                raise ValueError("paper position status disagrees with its volume")
            for name in ("opening_deal_ticket", "opening_order_ticket"):
                if type(row[name]) is not int or row[name] <= 0:
                    raise ValueError("invalid paper fill ticket")
            if ticket in used_ids:
                raise ValueError("paper ticket collision")
            used_ids.add(ticket)
        histories: Dict[int, List[Dict[str, Any]]] = {}
        for key, row in deals.items():
            if (type(row) is not dict or set(row) != deal_fields or cls._position_key(key) != key
                    or type(row["ticket"]) is not int or str(row["ticket"]) != key
                    or type(row["position_id"]) is not int or str(row["position_id"]) not in positions
                    or type(row["order_ticket"]) is not int or row["order_ticket"] <= 0):
                raise ValueError("invalid local paper deal schema")
            position = positions[str(row["position_id"])]
            if (row["destination"] != "PAPER" or row["runtime_identity"] != identity.to_dict()
                    or row["paper_position_id"] != position["paper_position_id"]
                    or row["symbol"] != position["symbol"] or type(row["magic"]) is not int
                    or row["magic"] != position["magic"]
                    or row["comment"] != position["comment"] or row["entry_kind"] not in {"IN", "OUT"}
                    or type(row["type"]) is not int or row["type"] != (
                        position["type"] if row["entry_kind"] == "IN" else 1 - position["type"])):
                raise ValueError("paper deal does not match its original position")
            cls._finite_number(row["volume"], "volume", positive=True)
            cls._finite_number(row["price"], "price", positive=True)
            for name in ("profit", "commission", "swap", "fee"):
                cls._finite_number(row[name], name)
            if (type(row["time_msc"]) is not int or row["time_msc"] < position["time_msc"]
                    or not _iso_utc(row["time_msc"])):
                raise ValueError("paper deal predates its position")
            if row["entry_kind"] == "IN" and row["profit"] != 0:
                raise ValueError("paper opening fill cannot invent realized profit")
            recovery = (row["execution_evidence"] or {}).get("event") == "MANUAL_RECOVERY_CLOSE"
            if recovery:
                if (row["entry_kind"] != "OUT" or row["quote_cursor"] is not None
                        or row["fill_policy_id"] != position["fill_policy_id"]):
                    raise ValueError("invalid paper recovery deal carrier")
                _paper_recovery_quote(position, row["execution_evidence"], row["price"], row["time_msc"])
            elif position["quote_cursor"] is None:
                if any(row[name] is not None for name in ("quote_cursor", "execution_evidence", "fill_policy_id")):
                    raise ValueError("untracked paper deal cannot invent trigger evidence")
            else:
                cursor = _paper_cursor(row["quote_cursor"])
                evidence = _paper_execution_evidence(row["execution_evidence"])
                _paper_cursor_order(position["entry_quote_cursor"], cursor)
                _paper_cursor_order(cursor, position["quote_cursor"])
                if (cursor["time_msc"] != row["time_msc"] or row["fill_policy_id"] != position["fill_policy_id"]
                        or evidence["event"] not in ({"ENTRY"} if row["entry_kind"] == "IN" else
                            {"SL", "TP", "MANUAL_CLOSE", "PARTIAL_CLOSE"})):
                    raise ValueError("paper deal trigger cursor, time or model mismatch")
                if row["entry_kind"] == "IN" and (cursor != position["entry_quote_cursor"]
                        or evidence != position["entry_execution_evidence"]):
                    raise ValueError("paper deal changed its original entry quote")
            histories.setdefault(row["position_id"], []).append(row)
            cls._bounded_text(row["reason"], "reason", required=True)
            for ticket in (row["ticket"], row["order_ticket"]):
                if ticket in used_ids:
                    raise ValueError("paper deal/order ticket collision")
                used_ids.add(ticket)
        for key, position in positions.items():
            history = histories.get(int(key), [])
            entries = [row for row in history if row["entry_kind"] == "IN"]
            if (len(entries) != 1 or entries[0]["ticket"] != position["opening_deal_ticket"]
                    or entries[0]["order_ticket"] != position["opening_order_ticket"]
                    or entries[0]["price"] != position["price_open"]
                    or entries[0]["volume"] != position["volume_initial"]):
                raise ValueError("paper position lacks its exact positive opening fill")
            closed_volume = sum(row["volume"] for row in history if row["entry_kind"] == "OUT")
            expected_volume = position["volume_initial"] - closed_volume
            if abs(expected_volume - position["volume"]) > 1e-10 * max(1.0, position["volume_initial"]):
                raise ValueError("paper position volume does not reconcile with its deals")
            recovery_closes = [row for row in history
                               if (row["execution_evidence"] or {}).get("event") == "MANUAL_RECOVERY_CLOSE"]
            if recovery_closes:
                exits = sorted((row for row in history if row["entry_kind"] == "OUT"),
                               key=lambda row: (row["time_msc"], row["ticket"]))
                if (len(recovery_closes) != 1 or position["status"] != "CLOSED"
                        or exits[-1]["ticket"] != recovery_closes[0]["ticket"]
                        or abs(recovery_closes[0]["volume"] - (position["volume_initial"]
                               - sum(row["volume"] for row in exits[:-1]))) > 1e-10 * max(1.0, position["volume_initial"])):
                    raise ValueError("paper recovery did not close its full remaining volume")
            if position["quote_cursor"] is not None:
                retained = [(position["entry_quote_cursor"], position["entry_execution_evidence"]),
                            (position["quote_cursor"], position["last_execution_evidence"])]
                retained += [(level["effective_cursor"], None) for level in position["level_history"]]
                retained += [(row["quote_cursor"], row["execution_evidence"]) for row in history if row["quote_cursor"] is not None]
                seen_quotes = {}
                for point, quote in retained:
                    coordinates = point["time_msc"], point["same_msc_ordinal"]
                    prefix, prior_quote = seen_quotes.get(coordinates, (point["prefix_sha256"], None))
                    if prefix != point["prefix_sha256"] or (quote is not None and prior_quote is not None
                            and not _paper_same_quote(prior_quote, quote)):
                        raise ValueError("retained paper quote cursor or evidence conflicts")
                    seen_quotes[coordinates] = prefix, quote if quote is not None else prior_quote
                if position["status"] == "CLOSED":
                    last_close = max((row for row in history if row["entry_kind"] == "OUT"), key=lambda row: row["ticket"])
                    if last_close["execution_evidence"]["event"] != "MANUAL_RECOVERY_CLOSE":
                        if (last_close["quote_cursor"] != position["quote_cursor"]
                                or last_close["execution_evidence"] != position["last_execution_evidence"]):
                            raise ValueError("closed paper cursor does not bind its last trigger")
        for key, row in operations.items():
            if (type(key) is not str or re.fullmatch(r"[A-Za-z0-9_.:-]{1,128}", key) is None
                    or type(row) is not dict or set(row) != {"kind", "request_sha256", "result_ticket"}
                    or row["kind"] not in {"CLOSE", "MODIFY"}
                    or not _HEX64_RE.fullmatch(str(row["request_sha256"]))
                    or type(row["result_ticket"]) is not int
                    or str(row["result_ticket"]) not in (deals if row["kind"] == "CLOSE" else positions)):
                raise ValueError("invalid paper operation receipt")
        if used_ids and counter <= max(used_ids):
            raise ValueError("paper ticket counter would recycle an identifier")
        totals = [sum(float(row[name]) for row in deals.values()) for name in ("profit", "commission", "swap", "fee")]
        if not all(math.isfinite(value) for value in (*totals, sum(totals))):
            raise ValueError("paper realized cash overflow")
        for alias, intent in state["entry_intents"].items():
            position = positions.get(str(intent["position_id"]))
            if position is None or intent.get("reservation_id") != position["reservation_id"]:
                raise ValueError("paper intent has no original local fill")
            expected = {
                f"paper-position:{position['ticket']}",
                f"paper-deal:{position['opening_deal_ticket']}",
                f"paper-order:{position['opening_order_ticket']}",
            }
            if alias not in expected:
                raise ValueError("paper fill cannot be a broker intent alias")
            if (intent["opening_deal_ticket"] != position["opening_deal_ticket"]
                    or intent["opening_order_ticket"] != position["opening_order_ticket"]
                    or intent["entry_price"] != position["price_open"] or intent["volume"] != position["volume_initial"]
                    or intent["symbol"] != position["original_symbol"] or intent["magic"] != position["magic"]
                    or intent.get("owner_snapshot") != position["owner_snapshot"]
                    or intent["risk_cash"] != position["risk_cash"]):
                raise ValueError("paper intent changed original fill or owner data")
        for key, pending in state["pending_positions"].items():
            if key not in positions:
                raise ValueError("paper pending close references a broker position")
            for deal in pending["exit_deals"].values():
                original = deals.get(str(deal.get("ticket")))
                if original is None or original["entry_kind"] != "OUT" or cls._normalize_deal(original) != deal:
                    raise ValueError("paper pending close lacks its positive local deal")
        if any(str(ticket) not in deals for ticket in state["seen_deal_tickets"]):
            raise ValueError("paper state contains foreign deal identifiers")
        if any(key not in positions for name in ("emitted_position_ids", "drift_delivered_position_ids") for key in state[name]):
            raise ValueError("paper state contains foreign position identifiers")

    @property
    def state_recovered(self) -> bool:
        return self._state_recovered

    def _require_paper(self, *, write: bool = False) -> RuntimeAccountIdentity:
        identity = self.runtime_identity
        if identity is None or identity.destination != "PAPER":
            raise LiveOutcomeError("local paper transitions require a PAPER runtime namespace")
        self._assert_runtime_state(self._state)
        if write and not self.enabled:
            raise LiveOutcomeError("durable paper outcome storage is disabled")
        return identity

    def _paper_save(self, previous: Dict[str, Any]) -> None:
        try:
            self._save_state()
        except Exception:
            self._state = previous
            raise

    def _paper_tickets(self, count: int) -> List[int]:
        first = self._state["next_paper_ticket"]
        if first + count > 2**63 - 1:
            raise LiveOutcomeError("paper ticket capacity exhausted")
        self._state["next_paper_ticket"] = first + count
        return list(range(first, first + count))

    @staticmethod
    def _paper_time(value: Any = None) -> int:
        result = int(datetime.now(timezone.utc).timestamp() * 1000) if value is None else value
        if type(result) is not int or result < 100_000_000_000 or not _iso_utc(result):
            raise ValueError("paper time_msc must be a contemporary UTC millisecond timestamp")
        return result

    @staticmethod
    def _paper_transition(row, *, expected_cursor, quote_cursor, execution_evidence, events, when=None):
        if row["quote_cursor"] is None:
            if any(value is not None for value in (expected_cursor, quote_cursor, execution_evidence)):
                raise LiveOutcomeError("untracked paper positions cannot reconstruct a quote cursor")
            return None
        previous, current = _paper_cursor(expected_cursor), _paper_cursor(quote_cursor)
        evidence = _paper_execution_evidence(execution_evidence)
        if evidence["event"] not in events or (when is not None and current["time_msc"] != when):
            raise ValueError("paper execution cursor, time or event mismatch")
        _paper_cursor_order(previous, current)
        return {"expected_cursor": previous, "quote_cursor": current, "execution_evidence": evidence}

    @staticmethod
    def _check_paper_transition(row, transition):
        if transition is None:
            return
        if row["quote_cursor"] != transition["expected_cursor"]:
            raise LiveOutcomeError("paper quote cursor changed before the operation")
        current = transition["quote_cursor"]
        _paper_cursor_order(row["quote_cursor"], current)
        if current == row["quote_cursor"] and not _paper_same_quote(
                row["last_execution_evidence"], transition["execution_evidence"]):
            raise LiveOutcomeError("paper quote evidence changed at the same cursor")

    @staticmethod
    def _apply_paper_transition(row, transition):
        if transition is not None:
            row["quote_cursor"] = transition["quote_cursor"]
            row["last_execution_evidence"] = transition["execution_evidence"]
            row["quote_transition_sha256"] = hashlib.sha256(_canonical_json_bytes(transition)).hexdigest()

    def paper_advance_quote_cursor(self, position_id: int, *, expected_cursor,
                                   quote_cursor, execution_evidence) -> Dict[str, Any]:
        """Latch a supplied record, including an unpriced first touch; never pass a pending trigger."""
        if type(position_id) is not int or position_id <= 0:
            raise ValueError("paper cursor advance requires a positive local position id")
        with self._lock:
            self._require_paper(write=True)
            row = self._state["paper_positions"].get(str(position_id))
            if row is None or row["quote_cursor"] is None:
                raise LiveOutcomeError("paper cursor advance requires a tracked local position")
            if row["status"] != "OPEN":
                raise LiveOutcomeError("closed paper positions cannot advance their quote cursor")
            transition = self._paper_transition(row, expected_cursor=expected_cursor,
                quote_cursor=quote_cursor, execution_evidence=execution_evidence, events={"ADVANCE", "SL", "TP"})
            digest = hashlib.sha256(_canonical_json_bytes(transition)).hexdigest()
            if row["quote_cursor"] == transition["quote_cursor"] and row["quote_transition_sha256"] == digest:
                return deepcopy(row)
            pending_trigger = row["last_execution_evidence"]["event"] in {"SL", "TP"}
            if pending_trigger and (transition["quote_cursor"] != row["quote_cursor"]
                    or transition["execution_evidence"] != row["last_execution_evidence"]):
                raise LiveOutcomeError("paper first-touch trigger awaits valuation; cursor cannot pass it")
            self._check_paper_transition(row, transition)
            if row["quote_cursor"] == transition["quote_cursor"] and (
                    transition["execution_evidence"]["event"] == "ADVANCE"
                    or transition["execution_evidence"] == row["last_execution_evidence"]):
                return deepcopy(row)
            _paper_require_ordinary_mutation(row)
            previous = deepcopy(self._state)
            self._apply_paper_transition(row, transition)
            self._paper_save(previous)
            return deepcopy(row)


    def paper_update_quote_audit(self, position_id: int, *, expected_audit, quote_audit) -> Dict[str, Any]:
        """CAS one bounded audit carrier; native observations remain the caller's responsibility."""
        if type(position_id) is not int or position_id <= 0 or quote_audit is None:
            raise ValueError("paper quote audit update requires a position and metadata")
        with self._lock:
            self._require_paper(write=True)
            row = self._state["paper_positions"].get(str(position_id))
            if row is None:
                raise LiveOutcomeError("unknown local paper audit position")
            current = row["quote_audit"]
            proposed = _paper_quote_audit(quote_audit, row["time_msc"])
            expected = _paper_quote_audit(expected_audit, row["time_msc"])
            if current == proposed:
                return deepcopy(row)
            if current != expected:
                raise LiveOutcomeError("paper quote audit changed before the operation")
            if current is not None and current["status"] in {"DIVERGENT", "UNAVAILABLE"}:
                raise LiveOutcomeError("terminal paper quote audit is immutable")
            if current is None:
                if proposed["corroborated_minute_count"] != 0:
                    raise LiveOutcomeError("paper quote audit cannot invent prior corroboration")
            else:
                first_fields = ("first_failure_minute_msc", "first_failure_code", "first_failure_status")
                if current["first_failure_minute_msc"] is not None and any(
                        current[name] != proposed[name] for name in first_fields):
                    raise LiveOutcomeError("paper quote audit first failure is immutable")
                delta = proposed["corroborated_minute_count"] - current["corroborated_minute_count"]
                if delta < 0:
                    raise LiveOutcomeError("paper quote audit corroboration moved backwards")
                if delta == 0:
                    if current["pending_minute_msc"] is not None:
                        if proposed["pending_minute_msc"] != current["pending_minute_msc"]:
                            raise LiveOutcomeError("paper quote audit cannot discard its pending minute")
                        if proposed["processed_count"] < current["processed_count"]:
                            raise LiveOutcomeError("paper quote audit processed count moved backwards")
                        if (proposed["processed_count"] == current["processed_count"]
                                and proposed["prefix_sha256"] != current["prefix_sha256"]):
                            raise LiveOutcomeError("paper quote audit recorded prefix changed")
            previous = deepcopy(self._state)
            row["quote_audit"] = proposed
            self._paper_save(previous)
            return deepcopy(row)

    def _paper_audit_span_complete(self, row, audit) -> bool:
        if (row["status"] != "CLOSED" or row["volume"] != 0 or row["entry_quote_cursor"] is None
                or audit is None or audit["status"] != "CLEAN" or audit["pending_minute_msc"] is not None
                or audit["overdue"] or audit["terminal_failure_minute_msc"] is not None):
            return False
        exits = [deal for deal in self._state["paper_deals"].values()
                 if deal["position_id"] == row["position_id"] and deal["entry_kind"] == "OUT"]
        if not exits:
            return False
        final_minute = max(deal["time_msc"] for deal in exits) // 60000 * 60000
        required = (final_minute - row["time_msc"] // 60000 * 60000) // 60000 + 1
        return (audit["corroborated_minute_count"] >= required
                and audit["last_corroborated_minute_msc"] >= final_minute)

    def paper_quote_audit_positions(self) -> List[Dict[str, Any]]:
        """Include closed final-minute work; terminal failures remain retained in history."""
        with self._lock:
            self._require_paper()
            return [deepcopy(row) for _, row in sorted(self._state["paper_positions"].items(), key=lambda item: int(item[0]))
                    if (row["quote_audit"] is None or row["quote_audit"]["status"] not in {"DIVERGENT", "UNAVAILABLE"})
                    and not self._paper_audit_span_complete(row, row["quote_audit"])]

    def paper_outcome_admission(self, record: Mapping[str, Any]) -> str:
        """Monitoring data quality only; this decision cannot qualify a cell or grant authority."""
        with self._lock:
            if self.runtime_identity is None or self.runtime_identity.destination != "PAPER":
                return "CLEAN"
            self._require_paper()
            identity = self.runtime_identity.to_dict()
            position_id = record.get("position_id")
            if (type(position_id) is not int or position_id <= 0 or record.get("runtime_identity") != identity
                    or record.get("event_id") != f"paper-position-close:{self.runtime_identity.namespace}:{position_id}"):
                return "UNAVAILABLE"
            row = self._state["paper_positions"].get(str(position_id))
            if row is None or row["runtime_identity"] != identity:
                return "UNAVAILABLE"
            audit = row["quote_audit"]
            if audit is None:
                return "MISSING"
            if audit["status"] in {"DIVERGENT", "UNAVAILABLE"}:
                return audit["status"]
            if (record.get("entry_reservation_id") != row["reservation_id"]
                    or row["opening_deal_ticket"] not in record.get("opening_deal_tickets", [])
                    or row["opening_order_ticket"] not in record.get("opening_order_tickets", [])):
                return "UNAVAILABLE"
            if not self._paper_audit_span_complete(row, audit):
                return "PENDING"
            local_exits = {deal["ticket"] for deal in self._state["paper_deals"].values()
                           if deal["position_id"] == position_id and deal["entry_kind"] == "OUT"}
            if set(record.get("closing_deal_tickets", [])) != local_exits:
                return "UNAVAILABLE"
            return "CLEAN"

    def _paper_statistics_counts(self, records) -> Dict[str, Any]:
        counts = {f"{status.lower()}_closed_positions": 0
                  for status in ("RAW", "CLEAN", "PENDING", "DIVERGENT", "UNAVAILABLE", "MISSING")}
        excluded = {status: 0.0 for status in ("PENDING", "DIVERGENT", "UNAVAILABLE", "MISSING")}
        for record in records:
            status = self.paper_outcome_admission(record)
            counts["raw_closed_positions"] += 1
            counts[f"{status.lower()}_closed_positions"] += 1
            if status != "CLEAN":
                excluded[status] += float(record["realized_net_pnl"])
        return {**counts, "excluded_provisional_net_cash": sum(excluded.values()),
                "excluded_net_cash_by_status": excluded}

    def paper_statistics_summary(self) -> Dict[str, Any]:
        with self._lock:
            self._require_paper()
            return self._paper_statistics_counts(self._ledger_records.values())


    def record_paper_binding_incident(self, *, observed_identity=None, bound_identity,
                                      reason: str, observed_at_utc=None) -> Dict[str, Any]:
        """Overwrite one bounded incident; persistence failure remains visible to the adapter."""
        with self._lock:
            identity = self._require_paper(write=True)
            observed = _paper_incident_identity(observed_identity)
            bound = _paper_incident_identity(bound_identity, bound=True)
            timestamp = _paper_incident_time(observed_at_utc)
            previous_incident = self._state.get("paper_binding_incident")
            incident = {"first_seen_utc": (previous_incident["first_seen_utc"] if previous_incident else timestamp),
                        "last_seen_utc": timestamp,
                        "count": (previous_incident["count"] + 1 if previous_incident else 1),
                        "observed_identity": observed, "bound_identity": bound, "reason": reason}
            incident = _paper_binding_incident(incident, identity)
            if (previous_incident is not None and datetime.fromisoformat(timestamp[:-1] + "+00:00")
                    < datetime.fromisoformat(previous_incident["last_seen_utc"][:-1] + "+00:00")):
                raise ValueError("paper binding incident observation moved backwards")
            previous = deepcopy(self._state)
            self._state["paper_binding_incident"] = incident
            self._paper_save(previous)
            return deepcopy(incident)

    def paper_binding_incident(self) -> Optional[Dict[str, Any]]:
        with self._lock:
            self._require_paper()
            return deepcopy(self._state.get("paper_binding_incident"))

    def paper_positions(self) -> List[Dict[str, Any]]:
        with self._lock:
            self._require_paper()
            return [deepcopy(row) for _, row in sorted(self._state["paper_positions"].items(), key=lambda item: int(item[0]))
                    if row["status"] == "OPEN"]

    def paper_fill_for_reservation(self, reservation_id: str) -> Optional[Dict[str, Any]]:
        """Return positive original fill evidence; absence cannot establish no-fill."""
        if type(reservation_id) is not str or not _RESERVATION_ID_RE.fullmatch(reservation_id):
            raise ValueError("invalid paper reservation id")
        with self._lock:
            self._require_paper()
            matches = [row for row in self._state["paper_positions"].values() if row["reservation_id"] == reservation_id]
            if len(matches) > 1:
                raise LiveOutcomeError("conflicting local fills for one reservation")
            return deepcopy(matches[0]) if matches else None

    def paper_deals(self) -> List[Dict[str, Any]]:
        with self._lock:
            self._require_paper()
            return deepcopy(sorted(self._state["paper_deals"].values(), key=lambda row: (row["time_msc"], row["ticket"])))

    def paper_history(self, position_id: int) -> Optional[Dict[str, Any]]:
        """Retained PAPER audit carrier; normalized financial rows do not embed this history."""
        if type(position_id) is not int or position_id <= 0:
            raise ValueError("paper position id must be a positive local integer")
        with self._lock:
            identity = self._require_paper()
            row = self._state["paper_positions"].get(str(position_id))
            if row is None:
                return None
            deals = [deal for deal in self.paper_deals() if deal["position_id"] == position_id]
            orders = [{"ticket": deal["order_ticket"], "position_id": position_id,
                       "time_setup_msc": deal["time_msc"], "price_open": deal["price"],
                       "volume_initial": deal["volume"], "sl": row["initial_stop"] if deal["entry_kind"] == "IN" else 0.0,
                       "tp": row["initial_take_profit"] if deal["entry_kind"] == "IN" else 0.0,
                       "magic": row["magic"], "comment": row["comment"], "type": deal["type"],
                       "destination": "PAPER", "runtime_identity": identity.to_dict()}
                      for deal in deals]
            return {"history_complete": True, "position_id": position_id,
                    "paper_position_id": row["paper_position_id"], "destination": "PAPER",
                    "runtime_identity": identity.to_dict(), "deals": deals, "orders": orders,
                    "initial_stop": row["initial_stop"], "risk_cash": row["risk_cash"],
                    "owner_snapshot": deepcopy(row["owner_snapshot"]), "funding_terms": deepcopy(row["funding_terms"]),
                    **{name: deepcopy(row[name]) for name in ("entry_quote_cursor", "quote_cursor", "fill_policy_id",
                        "entry_execution_evidence", "last_execution_evidence", "level_history", "quote_audit")}}

    def paper_cash_snapshot(self) -> Dict[str, Any]:
        """Return supplied realized components, never a fabricated broker balance or mark."""
        with self._lock:
            identity = self._require_paper()
            totals = {name: sum(float(row[name]) for row in self._state["paper_deals"].values())
                      for name in ("profit", "commission", "swap", "fee")}
            adjustment = sum(totals.values())
            if not all(math.isfinite(value) for value in (*totals.values(), adjustment)):
                raise LiveOutcomeError("paper realized cash overflow")
            return {"destination": "PAPER", "runtime_identity": identity.to_dict(),
                    "realized_profit_cash": totals["profit"], "commission_cash": totals["commission"],
                    "swap_cash": totals["swap"], "fee_cash": totals["fee"], "cash_adjustment": adjustment}

    def paper_close_operation(self, position_id: int, operation_id: str) -> Optional[Dict[str, Any]]:
        """Read positive close evidence before retrying a percentage on remaining volume."""
        if type(position_id) is not int or position_id <= 0:
            raise ValueError("paper close lookup requires a positive local position id")
        if type(operation_id) is not str or re.fullmatch(r"[A-Za-z0-9_.:-]{1,128}", operation_id) is None:
            raise ValueError("invalid paper close operation id")
        with self._lock:
            self._require_paper()
            row = self._state["paper_positions"].get(str(position_id))
            if row is None:
                raise LiveOutcomeError("unknown local paper position")
            operation = self._state["paper_operations"].get(operation_id)
            if operation is None:
                return None
            if operation.get("kind") != "CLOSE":
                raise LiveOutcomeError("paper operation is not a close")
            deal = self._state["paper_deals"].get(str(operation.get("result_ticket")))
            if deal is None or deal.get("position_id") != position_id:
                raise LiveOutcomeError("paper close operation belongs to another or missing position")
            return {**deepcopy(deal), "remaining_volume": row["volume"]}

    def paper_cash_adjustment(self) -> float:
        return float(self.paper_cash_snapshot()["cash_adjustment"])

    def _paper_deal(self, row: Mapping[str, Any], *, ticket: int, order: int,
                    entry: str, volume: float, price: float, profit: float,
                    commission: float, swap: float, fee: float, time_msc: int,
                    quote_cursor=None, execution_evidence=None) -> Dict[str, Any]:
        return {"ticket": ticket, "order_ticket": order, "position_id": row["ticket"],
                "paper_position_id": row["paper_position_id"], "destination": "PAPER",
                "runtime_identity": self.runtime_identity.to_dict(), "symbol": row["symbol"],
                "entry_kind": entry, "type": row["type"] if entry == "IN" else 1 - row["type"],
                "volume": volume, "price": price, "profit": profit, "commission": commission,
                "swap": swap, "fee": fee, "magic": row["magic"], "comment": row["comment"],
                "reason": "PAPER_ENTRY" if entry == "IN" else "PAPER_CLOSE", "time_msc": time_msc,
                "quote_cursor": deepcopy(quote_cursor), "execution_evidence": deepcopy(execution_evidence),
                "fill_policy_id": row["fill_policy_id"]}

    def paper_open_position(self, reservation_id: str, fill_mapping: Mapping[str, Any]) -> Dict[str, Any]:
        """Persist a positive local fill while retaining its reservation until main binds it."""
        required = {"price", "volume", "type", "time_msc"}
        optional = {"symbol", "broker_symbol", "magic", "sl", "tp", "comment", "commission", "fee", "swap", "risk_cash", "funding_terms",
                    "quote_cursor", "fill_policy_id", "execution_evidence", "quote_audit"}
        if (not isinstance(fill_mapping, Mapping) or not required.issubset(fill_mapping)
                or not set(fill_mapping).issubset(required | optional)
                or type(reservation_id) is not str or not _RESERVATION_ID_RE.fullmatch(reservation_id)):
            raise ValueError("invalid paper fill mapping or reservation id")
        with self._lock:
            identity = self._require_paper(write=True)
            positions, deals = self._state["paper_positions"], self._state["paper_deals"]
            existing = next((row for row in positions.values() if row["reservation_id"] == reservation_id), None)
            reservation = self._state["pre_send_reservations"].get(reservation_id)
            reference = reservation or ({"symbol": existing["original_symbol"], "magic": existing["magic"],
                                         "initial_stop": existing["initial_stop"], "volume": existing["volume_initial"]}
                                        if existing is not None else None)
            if reference is None:
                raise LiveOutcomeError("paper fill has no durable pre-send reservation")
            symbol = self._bounded_text(fill_mapping.get("symbol", reference["symbol"]), "symbol", required=True)
            magic = fill_mapping.get("magic", reference["magic"])
            if symbol != reference["symbol"] or type(magic) is not int or magic != reference["magic"]:
                raise LiveOutcomeError("paper fill does not match its reserved original owner")
            side = fill_mapping["type"]
            if type(side) is not int or side not in {0, 1}:
                raise ValueError("paper fill type must be BUY 0 or SELL 1")
            fill = {"symbol": symbol, "broker_symbol": self._bounded_text(
                        fill_mapping.get("broker_symbol", symbol), "broker_symbol", required=True),
                    "magic": magic, "type": side,
                    "price": self._finite_number(fill_mapping["price"], "price", positive=True),
                    "volume": self._finite_number(fill_mapping["volume"], "volume", positive=True),
                    "sl": self._finite_number(fill_mapping.get("sl", reference["initial_stop"]), "sl", positive=True),
                    "tp": self._finite_number(fill_mapping.get("tp", 0.0), "tp"),
                    "comment": self._bounded_text(fill_mapping.get("comment", ""), "comment"),
                    "time_msc": self._paper_time(fill_mapping["time_msc"]),
                    **{name: self._finite_number(fill_mapping.get(name, 0.0), name) for name in ("commission", "fee", "swap")},
                    "risk_cash": None if fill_mapping.get("risk_cash") is None else self._finite_number(
                        fill_mapping["risk_cash"], "risk_cash", positive=True),
                    "funding_terms": _funding_terms(fill_mapping.get("funding_terms"))}
            cursor, evidence, policy = (fill_mapping.get(name) for name in
                                        ("quote_cursor", "execution_evidence", "fill_policy_id"))
            if any(value is not None for value in (cursor, evidence, policy)):
                cursor, evidence = _paper_cursor(cursor), _paper_execution_evidence(evidence)
                if (policy != PAPER_FILL_POLICY_ID or evidence["event"] != "ENTRY"
                        or cursor["time_msc"] != fill["time_msc"]):
                    raise ValueError("paper entry tracking, time or policy mismatch")
                fill.update(quote_cursor=cursor, execution_evidence=evidence, fill_policy_id=policy)
            initial_audit = _paper_quote_audit(fill_mapping.get("quote_audit"), fill["time_msc"])
            if initial_audit is not None:
                if (cursor is None or initial_audit["status"] != "PENDING"
                        or initial_audit["corroborated_minute_count"] != 0
                        or initial_audit["pending_minute_msc"] != fill["time_msc"] // 60000 * 60000):
                    raise ValueError("paper entry audit must start at its tracked pending minute")
                fill["quote_audit"] = initial_audit
            if fill["tp"] < 0 or fill["sl"] != reference["initial_stop"] or fill["volume"] > reference["volume"]:
                raise LiveOutcomeError("paper fill exceeds its reserved volume or changes its original stop")
            digest = hashlib.sha256(_canonical_json_bytes({"reservation_id": reservation_id, "fill": fill})).hexdigest()
            if existing is not None:
                if existing["fill_sha256"] != digest:
                    raise LiveOutcomeError("conflicting repeated paper fill")
                return deepcopy(existing)
            if len(positions) >= MAX_PENDING_POSITIONS or len(deals) >= MAX_PENDING_EXIT_DEALS:
                raise LiveOutcomeError("paper fill evidence capacity exhausted; reservation retained")
            previous = deepcopy(self._state)
            ticket, deal_ticket, order_ticket = self._paper_tickets(3)
            row = {"ticket": ticket, "identifier": ticket, "position_id": ticket,
                   "paper_position_id": f"paper:{identity.namespace}:{ticket}", "destination": "PAPER",
                   "runtime_identity": identity.to_dict(), "reservation_id": reservation_id, "fill_sha256": digest,
                   "symbol": fill["broker_symbol"], "original_symbol": symbol, "type": side,
                   "magic": magic, "comment": fill["comment"], "volume_initial": fill["volume"],
                   "volume": fill["volume"], "price_open": fill["price"], "sl": fill["sl"], "tp": fill["tp"],
                   "initial_stop": fill["sl"], "initial_take_profit": fill["tp"], "risk_cash": fill["risk_cash"],
                   "owner_snapshot": _owner_snapshot(reservation.get("owner_snapshot")), "funding_terms": fill["funding_terms"],
                   "time": _iso_utc(fill["time_msc"]), "time_msc": fill["time_msc"],
                   "opening_deal_ticket": deal_ticket, "opening_order_ticket": order_ticket, "status": "OPEN",
                   "entry_quote_cursor": deepcopy(cursor), "quote_cursor": deepcopy(cursor), "fill_policy_id": policy,
                   "entry_execution_evidence": deepcopy(evidence), "last_execution_evidence": deepcopy(evidence),
                   "quote_audit": deepcopy(initial_audit), "statistics_delivered": False,
                   "quote_transition_sha256": None if cursor is None else hashlib.sha256(_canonical_json_bytes({
                       "entry_quote_cursor": cursor, "execution_evidence": evidence})).hexdigest(),
                   "level_history": [{"sl": fill["sl"], "tp": fill["tp"], "effective_cursor": deepcopy(cursor)}]}
            positions[str(ticket)] = row
            deals[str(deal_ticket)] = self._paper_deal(
                row, ticket=deal_ticket, order=order_ticket, entry="IN", volume=fill["volume"], price=fill["price"],
                profit=0.0, commission=fill["commission"], swap=fill["swap"], fee=fill["fee"], time_msc=fill["time_msc"],
                quote_cursor=cursor, execution_evidence=evidence)
            self._paper_save(previous)
            return deepcopy(row)

    def paper_modify_position(self, position_id: int, sl: Optional[float] = None,
                              tp: Optional[float] = None, *, operation_id: str = "",
                              expected_cursor=None, quote_cursor=None, execution_evidence=None) -> Dict[str, Any]:
        if type(position_id) is not int or position_id <= 0 or (sl is None and tp is None):
            raise ValueError("paper modification requires a local position and stop update")
        values = {name: None if value is None else self._finite_number(value, name)
                  for name, value in (("sl", sl), ("tp", tp))}
        if any(value is not None and value < 0 for value in values.values()):
            raise ValueError("paper stops cannot be negative")
        explicit_operation_id = bool(operation_id)
        with self._lock:
            self._require_paper(write=True)
            row = self._state["paper_positions"].get(str(position_id))
            if row is None:
                raise LiveOutcomeError("unknown local paper position")
            if row["quote_cursor"] is not None and row["status"] == "OPEN" and row["last_execution_evidence"]["event"] in {"SL", "TP"}:
                raise LiveOutcomeError("paper first-touch trigger awaits valuation; levels cannot change")
            transition = self._paper_transition(row, expected_cursor=expected_cursor,
                quote_cursor=quote_cursor, execution_evidence=execution_evidence, events={"LEVEL_CHANGE"})
            command = {"position_id": position_id, **values}
            if transition is not None:
                command["tracking"] = transition
            digest = hashlib.sha256(_canonical_json_bytes(command)).hexdigest()
            if not operation_id and transition is not None:
                operation_id = "modify-" + digest
            operations = self._state["paper_operations"]
            if operation_id:
                if type(operation_id) is not str or re.fullmatch(r"[A-Za-z0-9_.:-]{1,128}", operation_id) is None:
                    raise ValueError("invalid paper operation id")
                previous_operation = operations.get(operation_id)
                if previous_operation is not None:
                    if previous_operation != {"kind": "MODIFY", "request_sha256": digest, "result_ticket": position_id}:
                        raise LiveOutcomeError("conflicting paper modification retry")
                    return deepcopy(row)
                if len(operations) >= MAX_PENDING_EXIT_DEALS:
                    raise LiveOutcomeError("paper operation capacity exhausted")
            _paper_require_ordinary_mutation(row)
            if row["status"] != "OPEN":
                raise LiveOutcomeError("closed paper positions cannot be modified")
            self._check_paper_transition(row, transition)
            if (transition is not None and transition["quote_cursor"] == row["quote_cursor"]
                    and all(value is None or value == row[name] for name, value in values.items())):
                if explicit_operation_id:
                    previous = deepcopy(self._state)
                    operations[operation_id] = {"kind": "MODIFY", "request_sha256": digest, "result_ticket": position_id}
                    self._paper_save(previous)
                return deepcopy(row)
            if len(row["level_history"]) >= MAX_PAPER_LEVEL_HISTORY:
                raise LiveOutcomeError("paper level history capacity exhausted")
            previous = deepcopy(self._state)
            self._apply_paper_transition(row, transition)
            for name, value in values.items():
                if value is not None:
                    row[name] = value
            row["level_history"].append({"sl": row["sl"], "tp": row["tp"],
                                         "effective_cursor": deepcopy(row["quote_cursor"])})
            if operation_id:
                operations[operation_id] = {"kind": "MODIFY", "request_sha256": digest, "result_ticket": position_id}
            self._paper_save(previous)
            return deepcopy(row)

    def paper_close_position(self, position_id: int, volume: float, price: float, profit_cash: float,
                             swap_cash: float = 0.0, time_msc: Optional[int] = None, *,
                             commission_cash: float = 0.0, fee_cash: float = 0.0,
                             operation_id: str = "", expected_cursor=None,
                             quote_cursor=None, execution_evidence=None) -> Dict[str, Any]:
        """Record supplied cash; recovery records a fresh quote without claiming historical coverage."""
        if type(position_id) is not int or position_id <= 0:
            raise ValueError("paper close requires a positive local position id")
        command = {"position_id": position_id, "volume": self._finite_number(volume, "volume", positive=True),
                   "price": self._finite_number(price, "price", positive=True),
                   **{name: self._finite_number(value, name) for name, value in (
                       ("profit", profit_cash), ("swap", swap_cash), ("commission", commission_cash), ("fee", fee_cash))}}
        when = self._paper_time(time_msc)
        recovery = type(execution_evidence) is dict and execution_evidence.get("event") == "MANUAL_RECOVERY_CLOSE"
        with self._lock:
            self._require_paper(write=True)
            positions, deals, operations = (self._state[name] for name in
                                             ("paper_positions", "paper_deals", "paper_operations"))
            row = positions.get(str(position_id))
            if row is None:
                raise LiveOutcomeError("unknown local paper position")
            if row["quote_cursor"] is not None and time_msc is None:
                raise ValueError("tracked paper close requires its observed trigger time")
            if recovery:
                if (type(operation_id) is not str
                        or re.fullmatch(r"[A-Za-z0-9_.:-]{1,128}", operation_id) is None):
                    raise ValueError("paper recovery requires an explicit stable operation id")
                if quote_cursor is not None or _paper_cursor(expected_cursor) != row["quote_cursor"]:
                    raise LiveOutcomeError("paper recovery must retain its expected verified cursor")
                recovered_evidence = _paper_recovery_quote(row, execution_evidence, command["price"], when)
                command.update(recovery={"expected_cursor": deepcopy(row["quote_cursor"]),
                                         "execution_evidence": recovered_evidence}, time_msc=when)
                transition = None
            else:
                transition = self._paper_transition(row, expected_cursor=expected_cursor,
                    quote_cursor=quote_cursor, execution_evidence=execution_evidence,
                    events={"SL", "TP", "MANUAL_CLOSE", "PARTIAL_CLOSE"}, when=when)
                if (row["quote_cursor"] is not None and row["status"] == "OPEN"
                        and row["last_execution_evidence"]["event"] in {"SL", "TP"}
                        and (transition["quote_cursor"] != row["quote_cursor"]
                             or transition["execution_evidence"] != row["last_execution_evidence"])):
                    raise LiveOutcomeError("paper close must preserve its retained first-touch trigger")
                if transition is not None:
                    command.update(tracking=transition, time_msc=when)
            digest = hashlib.sha256(_canonical_json_bytes(command)).hexdigest()
            key = operation_id or "close-" + digest
            if type(key) is not str or re.fullmatch(r"[A-Za-z0-9_.:-]{1,128}", key) is None:
                raise ValueError("invalid paper close operation id")
            previous_operation = operations.get(key)
            if previous_operation is not None:
                if previous_operation["kind"] != "CLOSE" or previous_operation["request_sha256"] != digest:
                    raise LiveOutcomeError("conflicting paper close retry")
                return {**deepcopy(deals[str(previous_operation["result_ticket"])]), "remaining_volume": row["volume"]}
            if recovery:
                if command["volume"] != row["volume"]:
                    raise LiveOutcomeError("paper recovery must close the full remaining volume")
            else:
                _paper_require_ordinary_mutation(row, saved_trigger_settlement=True)
            latest_time = max(deal["time_msc"] for deal in deals.values() if deal["position_id"] == position_id)
            if (row["status"] != "OPEN" or command["volume"] > row["volume"] + self.volume_tolerance
                    or when < latest_time):
                raise LiveOutcomeError("paper close has invalid volume, state or observation time")
            if len(deals) >= MAX_PENDING_EXIT_DEALS or len(operations) >= MAX_PENDING_EXIT_DEALS:
                raise LiveOutcomeError("paper close evidence capacity exhausted")
            if not recovery:
                self._check_paper_transition(row, transition)
            previous = deepcopy(self._state)
            actual_volume = min(command["volume"], row["volume"])
            if row["volume"] - actual_volume <= self.volume_tolerance:
                actual_volume = row["volume"]
            ticket, order = self._paper_tickets(2)
            if not recovery:
                self._apply_paper_transition(row, transition)
            deal = self._paper_deal(row, ticket=ticket, order=order, entry="OUT", volume=actual_volume,
                                    price=command["price"], profit=command["profit"], commission=command["commission"],
                                    swap=command["swap"], fee=command["fee"], time_msc=when,
                                    quote_cursor=None if transition is None else transition["quote_cursor"],
                                    execution_evidence=(recovered_evidence if recovery else
                                                        None if transition is None else transition["execution_evidence"]))
            row["volume"] -= actual_volume
            row["status"] = "OPEN" if row["volume"] > 0 else "CLOSED"
            deals[str(ticket)] = deal
            operations[key] = {"kind": "CLOSE", "request_sha256": digest, "result_ticket": ticket}
            self._paper_save(previous)
            return {**deepcopy(deal), "remaining_volume": row["volume"]}

    def exact_cell_summary(
        self,
        symbol: str,
        timeframe: str,
        regime: str,
        strategy_name: str,
    ) -> Dict[str, Any]:
        """Return durable forward evidence for one exactly attributed cell."""
        target = (
            str(symbol or "").upper(),
            str(timeframe or "").upper(),
            str(regime or "").upper(),
            str(strategy_name or ""),
        )
        exact_count = 0
        r_count = 0
        r_sum = 0.0
        gains = 0.0
        losses = 0.0
        selected_records = []
        with self._lock:
            for record in self._ledger_records.values():
                if str(record.get("cell_match_status", "") or "").upper() != "EXACT":
                    continue
                if (
                    str(record.get("symbol", "") or "").upper(),
                    str(record.get("timeframe", "") or "").upper(),
                    str(record.get("regime", "") or "").upper(),
                    str(record.get("strategy_name", "") or ""),
                ) != target:
                    continue
                selected_records.append(record)
                if self.paper_outcome_admission(record) != "CLEAN":
                    continue
                exact_count += 1
                value = record.get("realized_r")
                if value is not None and math.isfinite(float(value)):
                    numeric = float(value)
                    r_count += 1
                    r_sum += numeric
                    if numeric > 0.0:
                        gains += numeric
                    elif numeric < 0.0:
                        losses -= numeric
            counts = (self._paper_statistics_counts(selected_records) if self.runtime_identity is not None
                      and self.runtime_identity.destination == "PAPER" else {})
        profit_factor = gains / losses if losses > 1e-12 else (float("inf") if gains > 0.0 else 0.0)
        return {
            **counts,
            "exact_closed_positions": exact_count,
            "r_observations": r_count,
            "mean_r": (r_sum / r_count) if r_count else None,
            "profit_factor_r": profit_factor,
        }

    @staticmethod
    def _position_key(position_id: Any) -> str:
        numeric = _safe_int(position_id)
        return str(numeric) if numeric > 0 else ""

    @classmethod
    def _validate_ledger_record(cls, raw: Any) -> Dict[str, Any]:
        required = {
            "schema_version",
            "event_id",
            "position_id",
            "symbol",
            "broker_symbol",
            "timeframe",
            "regime",
            "strategy_name",
            "book_role",
            "candidate_id",
            "cell_match_status",
            "cell_match_reason",
            "opening_deal_tickets",
            "opening_order_tickets",
            "closing_deal_tickets",
            "closing_order_tickets",
            "opened_at",
            "closed_at",
            "entry_volume",
            "exit_volume",
            "volume_weighted_entry",
            "volume_weighted_exit",
            "gross_profit",
            "commission",
            "swap",
            "fee",
            "realized_net_pnl",
            "entry_comment",
            "entry_magic",
            "entry_metadata",
            "initial_stop",
            "initial_risk_cash",
            "initial_risk_source",
            "realized_r",
            "realized_r_unavailable_reason",
            "eligible_for_auto_promotion",
            "recorded_at",
        }
        optional = {
            "entry_reservation_id",
            "management_capsule_id",
            "management_generation_id",
            "reserved_at",
            "reserved_entry_price",
            "reserved_volume",
            "reserved_risk_cash",
            "runtime_identity",
            "owner_snapshot",
        }
        if not isinstance(raw, dict) or not required.issubset(raw) or not set(raw).issubset(required | optional):
            raise ValueError("ledger record has an invalid schema")
        record = dict(raw)
        schema = record["schema_version"]
        if schema not in {LEDGER_SCHEMA_VERSION, RUNTIME_LEDGER_SCHEMA_VERSION}:
            raise ValueError("ledger record has an unsupported schema")
        position_id = record["position_id"]
        if not isinstance(position_id, int) or isinstance(position_id, bool) or position_id <= 0:
            raise ValueError("ledger record has an invalid position_id")
        if schema == RUNTIME_LEDGER_SCHEMA_VERSION:
            identity = _runtime_identity_from_mapping(record.get("runtime_identity"))
            prefix = "paper-position-close" if identity.destination == "PAPER" else "mt5-position-close"
            event_id = f"{prefix}:{identity.namespace}:{position_id}"
        else:
            if "runtime_identity" in record:
                raise ValueError("legacy close record cannot carry a scoped identity")
            event_id = f"mt5-position-close:{position_id}"
        if record["event_id"] != event_id:
            raise ValueError("ledger record event_id does not match position_id")
        for name in (
            "symbol",
            "broker_symbol",
            "timeframe",
            "regime",
            "strategy_name",
            "book_role",
            "candidate_id",
            "cell_match_status",
            "cell_match_reason",
            "entry_comment",
            "initial_risk_source",
            "realized_r_unavailable_reason",
            "entry_reservation_id",
            "management_capsule_id",
            "management_generation_id",
            "reserved_at",
        ):
            value = record.get(name)
            if value is not None:
                cls._bounded_text(value, name)
        for name in (
            "opening_deal_tickets",
            "opening_order_tickets",
            "closing_deal_tickets",
            "closing_order_tickets",
        ):
            values = record[name]
            if (
                not isinstance(values, list)
                or len(values) > MAX_PENDING_EXIT_DEALS
                or any(
                    not isinstance(value, int) or isinstance(value, bool) or value <= 0
                    for value in values
                )
                or len(set(values)) != len(values)
            ):
                raise ValueError(f"ledger record has invalid {name}")
        if not record["opening_deal_tickets"] or not record["closing_deal_tickets"]:
            raise ValueError("ledger record lacks opening or closing deals")
        for name in ("opened_at", "closed_at", "recorded_at"):
            value = cls._bounded_text(record[name], name, required=True)
            if _iso_utc(value) != value:
                raise ValueError(f"ledger record has invalid {name}")
        for name in (
            "entry_volume",
            "exit_volume",
            "volume_weighted_entry",
            "volume_weighted_exit",
        ):
            cls._finite_number(record[name], name, positive=True)
        for name in ("gross_profit", "commission", "swap", "fee", "realized_net_pnl"):
            cls._finite_number(record[name], name)
        for name in (
            "initial_stop",
            "initial_risk_cash",
            "realized_r",
            "reserved_entry_price",
            "reserved_volume",
            "reserved_risk_cash",
        ):
            value = record.get(name)
            if value is not None:
                cls._finite_number(
                    value,
                    name,
                    positive=name != "realized_r",
                )
        if not isinstance(record["entry_magic"], int) or isinstance(record["entry_magic"], bool):
            raise ValueError("ledger record has an invalid entry_magic")
        if not isinstance(record["entry_metadata"], dict):
            raise ValueError("ledger record entry_metadata is not an object")
        if record["eligible_for_auto_promotion"] is not False:
            raise ValueError("ledger record cannot authorize promotion")
        expected_net = sum(float(record[name]) for name in ("gross_profit", "commission", "swap", "fee"))
        tolerance = 1e-10 * max(1.0, abs(expected_net), abs(float(record["realized_net_pnl"])))
        if abs(expected_net - float(record["realized_net_pnl"])) > tolerance:
            raise ValueError("ledger record net PnL is inconsistent")
        risk = record.get("initial_risk_cash")
        realized_r = record.get("realized_r")
        if risk is not None:
            expected_r = float(record["realized_net_pnl"]) / float(risk)
            r_tolerance = 1e-10 * max(1.0, abs(expected_r))
            if realized_r is None or abs(float(realized_r) - expected_r) > r_tolerance:
                raise ValueError("ledger record realized R is inconsistent")
        elif realized_r is not None:
            raise ValueError("ledger record has realized R without durable risk")
        reservation_id = str(record.get("entry_reservation_id", "") or "")
        if reservation_id and not _RESERVATION_ID_RE.fullmatch(reservation_id):
            raise ValueError("ledger record has an invalid reservation id")
        capsule_id = str(record.get("management_capsule_id", "") or "")
        generation_id = str(record.get("management_generation_id", "") or "")
        if reservation_id and record["book_role"] == _DEMO_BOOK_ROLE:
            if not _HEX64_RE.fullmatch(capsule_id) or not generation_id:
                raise ValueError("ledger record lacks demo management authority")
        elif bool(capsule_id) != bool(generation_id):
            raise ValueError("ledger record has an incomplete management authority pair")
        if "owner_snapshot" in record:
            _owner_snapshot(record["owner_snapshot"])
        return record

    @staticmethod
    def _finite_number(value: Any, field_name: str, *, positive: bool = False) -> float:
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise ValueError(f"{field_name} is not numeric")
        result = float(value)
        if not math.isfinite(result) or (positive and result <= 0.0):
            raise ValueError(f"{field_name} is invalid")
        return result

    @staticmethod
    def _bounded_text(value: Any, field_name: str, *, required: bool = False) -> str:
        if not isinstance(value, str):
            raise ValueError(f"{field_name} is not text")
        result = value.strip()
        if required and not result:
            raise ValueError(f"{field_name} is empty")
        if len(result) > MAX_TEXT_VALUE:
            raise ValueError(f"{field_name} is too long")
        if any(ord(character) < 32 for character in result):
            raise ValueError(f"{field_name} contains control characters")
        return result

    @classmethod
    def _validate_reservation(cls, reservation_id: str, raw: Any) -> Dict[str, Any]:
        if not _RESERVATION_ID_RE.fullmatch(reservation_id):
            raise ValueError("invalid reservation id")
        if not isinstance(raw, dict):
            raise ValueError(f"reservation {reservation_id} is not an object")
        fields = {
            "schema_version",
            "reservation_id",
            "status",
            "symbol",
            "timeframe",
            "regime",
            "strategy_name",
            "magic",
            "entry_price",
            "volume",
            "initial_stop",
            "risk_cash",
            "book_role",
            "candidate_id",
            "management_capsule_id",
            "management_generation_id",
            "reserved_at",
            "ambiguity_reason",
            "observed_at",
            "broker_order_ticket",
            "reservation_sha256",
        }
        if not fields.issubset(raw) or not set(raw).issubset(fields | {"owner_snapshot"}):
            raise ValueError(f"reservation {reservation_id} has an invalid schema")
        payload = dict(raw)
        if payload["schema_version"] != RESERVATION_SCHEMA_VERSION:
            raise ValueError(f"reservation {reservation_id} has an unsupported schema")
        if payload["reservation_id"] != reservation_id:
            raise ValueError(f"reservation {reservation_id} has an identity mismatch")
        if payload["status"] not in {"RESERVED", "AMBIGUOUS"}:
            raise ValueError(f"reservation {reservation_id} has an invalid status")
        for name in ("symbol", "timeframe", "regime"):
            value = cls._bounded_text(payload[name], name, required=True)
            if value != value.upper() or payload[name] != value:
                raise ValueError(f"reservation {reservation_id} has non-canonical {name}")
        strategy = cls._bounded_text(payload["strategy_name"], "strategy_name", required=True)
        if payload["strategy_name"] != strategy:
            raise ValueError(f"reservation {reservation_id} has non-canonical strategy_name")
        role = cls._bounded_text(
            payload["book_role"],
            "book_role",
            required=bool(payload.get("reservation_id")),
        )
        if payload["book_role"] != role.upper():
            raise ValueError(f"reservation {reservation_id} has non-canonical book_role")
        if payload["timeframe"] not in _TIMEFRAMES or payload["regime"] not in _REGIMES:
            raise ValueError(f"reservation {reservation_id} has an unsupported cell")
        if payload["book_role"] not in _BOOK_ROLES:
            raise ValueError(f"reservation {reservation_id} has an unsupported book_role")
        for name in ("candidate_id", "reserved_at", "ambiguity_reason", "observed_at"):
            if payload[name] != cls._bounded_text(payload[name], name):
                raise ValueError(f"reservation {reservation_id} has non-canonical {name}")
        if not payload["reserved_at"] or _iso_utc(payload["reserved_at"]) != payload["reserved_at"]:
            raise ValueError(f"reservation {reservation_id} has an invalid reserved_at")
        if payload["status"] == "RESERVED" and any(
            (payload["ambiguity_reason"], payload["observed_at"], payload["broker_order_ticket"])
        ):
            raise ValueError(f"reservation {reservation_id} has inconsistent reserved state")
        if payload["status"] == "AMBIGUOUS" and (
            not payload["ambiguity_reason"]
            or not payload["observed_at"]
            or _iso_utc(payload["observed_at"]) != payload["observed_at"]
        ):
            raise ValueError(f"reservation {reservation_id} has incomplete ambiguity evidence")
        capsule_id = cls._bounded_text(payload["management_capsule_id"], "management_capsule_id")
        generation_id = cls._bounded_text(
            payload["management_generation_id"], "management_generation_id"
        )
        if payload["management_capsule_id"] != capsule_id or payload["management_generation_id"] != generation_id:
            raise ValueError(f"reservation {reservation_id} has non-canonical management authority")
        if len(generation_id.encode("utf-8")) > 256:
            raise ValueError(f"reservation {reservation_id} generation id is too long")
        if payload["book_role"] == _DEMO_BOOK_ROLE:
            if not _HEX64_RE.fullmatch(capsule_id) or not generation_id:
                raise ValueError(f"reservation {reservation_id} lacks demo management authority")
        elif bool(capsule_id) != bool(generation_id) or (capsule_id and not _HEX64_RE.fullmatch(capsule_id)):
            raise ValueError(f"reservation {reservation_id} has an invalid management authority pair")
        if not isinstance(payload["magic"], int) or isinstance(payload["magic"], bool) or payload["magic"] <= 0:
            raise ValueError(f"reservation {reservation_id} has an invalid magic")
        for name in ("entry_price", "volume", "initial_stop", "risk_cash"):
            cls._finite_number(payload[name], name, positive=True)
        order_ticket = payload["broker_order_ticket"]
        if not isinstance(order_ticket, int) or isinstance(order_ticket, bool) or order_ticket < 0:
            raise ValueError(f"reservation {reservation_id} has an invalid order ticket")
        digest = payload.pop("reservation_sha256")
        if not isinstance(digest, str) or not re.fullmatch(r"[0-9a-f]{64}", digest):
            raise ValueError(f"reservation {reservation_id} has an invalid digest")
        if hashlib.sha256(_canonical_json_bytes(payload)).hexdigest() != digest:
            raise ValueError(f"reservation {reservation_id} digest mismatch")
        payload["reservation_sha256"] = digest
        if "owner_snapshot" in payload:
            snapshot = _owner_snapshot(payload["owner_snapshot"])
            if snapshot is None or any(snapshot[name] != payload[name] for name in
                                       ("symbol", "timeframe", "regime", "strategy_name", "book_role", "candidate_id")):
                raise ValueError("reservation owner data changed its original cell")
        return payload

    @classmethod
    def _validate_entry_intent(cls, alias: str, raw: Any) -> Dict[str, Any]:
        if not _INTENT_ALIAS_RE.fullmatch(alias) or not isinstance(raw, dict):
            raise ValueError(f"invalid entry intent alias {alias}")
        required = {
            "position_id",
            "opening_deal_ticket",
            "opening_order_ticket",
            "symbol",
            "timeframe",
            "regime",
            "strategy_name",
            "magic",
            "entry_price",
            "volume",
            "initial_stop",
            "risk_cash",
            "book_role",
            "candidate_id",
            "recorded_at",
        }
        optional = {
            "reservation_id",
            "reserved_at",
            "reserved_entry_price",
            "reserved_volume",
            "reserved_risk_cash",
            "management_capsule_id",
            "management_generation_id",
            "owner_snapshot",
        }
        if not required.issubset(raw) or not set(raw).issubset(required | optional):
            raise ValueError(f"entry intent {alias} has an invalid schema")
        payload = dict(raw)
        for name in ("symbol", "timeframe", "regime"):
            value = cls._bounded_text(payload[name], name, required=True)
            if payload[name] != value.upper():
                raise ValueError(f"entry intent {alias} has non-canonical {name}")
        strategy = cls._bounded_text(payload["strategy_name"], "strategy_name", required=True)
        if payload["strategy_name"] != strategy:
            raise ValueError(f"entry intent {alias} has non-canonical strategy_name")
        role = cls._bounded_text(
            payload["book_role"],
            "book_role",
            required=bool(payload.get("reservation_id")),
        )
        if payload["book_role"] != role.upper():
            raise ValueError(f"entry intent {alias} has non-canonical book_role")
        if payload["timeframe"] not in _TIMEFRAMES or payload["regime"] not in _REGIMES:
            raise ValueError(f"entry intent {alias} has an unsupported cell")
        if payload["book_role"] and payload["book_role"] not in _BOOK_ROLES:
            raise ValueError(f"entry intent {alias} has an unsupported book_role")
        for name in ("candidate_id", "recorded_at", "reserved_at"):
            value = payload.get(name, "")
            if value != cls._bounded_text(value, name):
                raise ValueError(f"entry intent {alias} has non-canonical {name}")
        for name in ("position_id", "opening_deal_ticket", "opening_order_ticket"):
            value = payload[name]
            if not isinstance(value, int) or isinstance(value, bool) or value < 0:
                raise ValueError(f"entry intent {alias} has an invalid {name}")
        if not isinstance(payload["magic"], int) or isinstance(payload["magic"], bool) or payload["magic"] <= 0:
            raise ValueError(f"entry intent {alias} has an invalid magic")
        for name in ("entry_price", "volume", "initial_stop"):
            cls._finite_number(payload[name], name, positive=True)
        for name in ("risk_cash", "reserved_entry_price", "reserved_volume", "reserved_risk_cash"):
            value = payload.get(name)
            if value is not None:
                cls._finite_number(value, name, positive=True)
        reservation_id = payload.get("reservation_id", "")
        if reservation_id and not _RESERVATION_ID_RE.fullmatch(str(reservation_id)):
            raise ValueError(f"entry intent {alias} has an invalid reservation id")
        capsule_id = cls._bounded_text(
            payload.get("management_capsule_id", ""), "management_capsule_id"
        )
        generation_id = cls._bounded_text(
            payload.get("management_generation_id", ""), "management_generation_id"
        )
        if payload.get("management_capsule_id", "") != capsule_id or payload.get(
            "management_generation_id", ""
        ) != generation_id:
            raise ValueError(f"entry intent {alias} has non-canonical management authority")
        if len(generation_id.encode("utf-8")) > 256:
            raise ValueError(f"entry intent {alias} generation id is too long")
        if payload["book_role"] == _DEMO_BOOK_ROLE:
            if (
                not reservation_id
                or not _HEX64_RE.fullmatch(capsule_id)
                or not generation_id
            ):
                raise ValueError(f"entry intent {alias} lacks demo management authority")
        elif bool(capsule_id) != bool(generation_id) or (capsule_id and not _HEX64_RE.fullmatch(capsule_id)):
            raise ValueError(f"entry intent {alias} has an invalid management authority pair")
        if "owner_snapshot" in payload:
            snapshot = _owner_snapshot(payload["owner_snapshot"])
            if snapshot is None or any(snapshot[name] != payload[name] for name in
                                       ("symbol", "timeframe", "regime", "strategy_name", "book_role", "candidate_id")):
                raise ValueError("bound owner data changed its original cell")
        return payload

    @classmethod
    def _validate_state_payload(cls, raw: Any) -> Dict[str, Any]:
        if not isinstance(raw, dict):
            raise ValueError("state is not an object")
        schema = raw.get("schema_version")
        scoped = schema == RUNTIME_STATE_SCHEMA_VERSION
        runtime_fields = {"runtime_identity", "paper_positions", "paper_deals", "paper_operations", "next_paper_ticket"}
        paper_scope = scoped and _runtime_identity_from_mapping(raw.get("runtime_identity")).destination == "PAPER"
        allowed = set(cls._empty_state()) | (runtime_fields if scoped else set())
        if paper_scope:
            allowed.add("paper_binding_incident")
        if not set(raw).issubset(allowed):
            raise ValueError("state has unknown fields")
        if schema not in {STATE_SCHEMA_VERSION, LEGACY_STATE_SCHEMA_VERSION, RUNTIME_STATE_SCHEMA_VERSION}:
            raise ValueError("unsupported or missing state schema")
        if scoped and set(raw) - {"paper_binding_incident"} != allowed - {"paper_binding_incident"}:
            raise ValueError("scoped state has missing fields")
        state = cls._empty_state()
        state.update(raw)
        state["schema_version"] = RUNTIME_STATE_SCHEMA_VERSION if scoped else STATE_SCHEMA_VERSION
        for name in ("seen_deal_tickets", "emitted_position_ids", "drift_delivered_position_ids"):
            values = state[name]
            if not isinstance(values, list) or len(values) > MAX_STATE_ID_VALUES:
                raise ValueError(f"state field {name} is invalid or exceeds its bound")
            if name == "seen_deal_tickets":
                valid = all(
                    isinstance(value, int) and not isinstance(value, bool) and value > 0
                    for value in values
                )
            else:
                valid = all(
                    isinstance(value, str)
                    and value.isdigit()
                    and not value.startswith("0")
                    for value in values
                )
            if not valid:
                raise ValueError(f"state field {name} has invalid identifiers")
            if len({str(value) for value in values}) != len(values):
                raise ValueError(f"state field {name} contains duplicates")
        for name in ("pending_positions", "entry_intents", "pre_send_reservations"):
            if not isinstance(state[name], dict):
                raise ValueError(f"state field {name} is not an object")
        if len(state["pending_positions"]) > MAX_PENDING_POSITIONS:
            raise ValueError("pending position bound exceeded")
        exit_deal_count = 0
        for position_key, pending in state["pending_positions"].items():
            if not cls._position_key(position_key) or not isinstance(pending, dict):
                raise ValueError("invalid pending position")
            if set(pending) != {"exit_deals", "last_reason"}:
                raise ValueError(f"pending position {position_key} has an invalid schema")
            if not isinstance(pending["exit_deals"], dict):
                raise ValueError(f"pending position {position_key} deals are invalid")
            cls._bounded_text(pending["last_reason"], "last_reason")
            exit_deal_count += len(pending["exit_deals"])
        if exit_deal_count > MAX_PENDING_EXIT_DEALS:
            raise ValueError("pending exit deal bound exceeded")
        if len(state["entry_intents"]) > MAX_ENTRY_INTENT_ALIASES:
            raise ValueError("entry intent alias bound exceeded")
        state["entry_intents"] = {
            alias: cls._validate_entry_intent(alias, payload)
            for alias, payload in state["entry_intents"].items()
        }
        paper_scope = scoped and _runtime_identity_from_mapping(state["runtime_identity"]).destination == "PAPER"
        if any(alias.startswith("paper-") != paper_scope for alias in state["entry_intents"]):
            raise ValueError("entry intent aliases do not match execution destination")
        if len(state["pre_send_reservations"]) > MAX_ENTRY_RESERVATIONS:
            raise ValueError("pre-send reservation bound exceeded")
        state["pre_send_reservations"] = {
            reservation_id: cls._validate_reservation(reservation_id, payload)
            for reservation_id, payload in state["pre_send_reservations"].items()
        }
        pending_keys = [
            (payload["symbol"], payload["magic"])
            for payload in state["pre_send_reservations"].values()
        ]
        if len(set(pending_keys)) != len(pending_keys):
            raise ValueError("duplicate pending symbol/magic reservation")
        bound_by_reservation: Dict[str, bytes] = {}
        for payload in state["entry_intents"].values():
            reservation_id = str(payload.get("reservation_id", "") or "")
            if not reservation_id:
                continue
            encoded = _canonical_json_bytes(payload)
            previous = bound_by_reservation.setdefault(reservation_id, encoded)
            if previous != encoded:
                raise ValueError(f"conflicting bound aliases for reservation {reservation_id}")
        cls._bounded_text(state.get("updated_at", ""), "updated_at")
        if scoped:
            cls._validate_paper_state(state)
        return state

    def _load_state(self) -> None:
        if not self.state_path.exists():
            return
        try:
            initial_stat = self.state_path.stat()
            if initial_stat.st_size > MAX_STATE_BYTES:
                raise ValueError("state file exceeds its hard byte bound")
            encoded = self.state_path.read_bytes()
            if len(encoded) > MAX_STATE_BYTES:
                raise ValueError("state file exceeds its hard byte bound")
            final_stat = self.state_path.stat()
            if (
                final_stat.st_size != initial_stat.st_size
                or final_stat.st_mtime_ns != initial_stat.st_mtime_ns
                or final_stat.st_ino != initial_stat.st_ino
            ):
                raise ValueError("state changed while it was being loaded")
            raw = _strict_json_loads(encoded)
            legacy = isinstance(raw, dict) and raw.get("schema_version") == LEGACY_STATE_SCHEMA_VERSION
            validated = self._validate_state_payload(raw)
            self._assert_runtime_state(validated)
            self._state = validated
            self._state_disk_sha256 = hashlib.sha256(encoded).hexdigest()
            self._state_recovered = legacy
        except Exception as exc:
            raise LiveOutcomeError(
                f"live outcome state is corrupt; reservations are fail-closed: {self.state_path}: {exc}"
            ) from exc

    def _load_ledger_index(self) -> None:
        if not self.ledger_path.exists():
            return
        try:
            initial_stat = self.ledger_path.stat()
            size = initial_stat.st_size
            if size > MAX_LEDGER_BYTES:
                raise ValueError("ledger exceeds its hard byte bound")
            with self.ledger_path.open("rb") as handle:
                line_number = 0
                while True:
                    raw_line = handle.readline(MAX_LEDGER_RECORD_BYTES + 2)
                    if not raw_line:
                        break
                    line_number += 1
                    if len(raw_line) > MAX_LEDGER_RECORD_BYTES + 1 or not raw_line.endswith(b"\n"):
                        raise ValueError(f"line {line_number} exceeds its bound or is truncated")
                    line = raw_line.strip()
                    if not line:
                        continue
                    record = _strict_json_loads(line)
                    record = self._validate_ledger_record(record)
                    self._assert_runtime_record(record)
                    position_key = self._position_key(record.get("position_id"))
                    if not position_key:
                        raise ValueError(f"line {line_number} has no position_id")
                    if position_key in self._ledger_records:
                        raise ValueError(f"duplicate position {position_key} in append-only ledger")
                    if len(self._ledger_records) >= min(MAX_LEDGER_RECORDS, MAX_LEDGER_INDEX_RECORDS):
                        raise ValueError("ledger record/index bound exceeded")
                    self._ledger_records[position_key] = record
            final_stat = self.ledger_path.stat()
            if (
                final_stat.st_size != initial_stat.st_size
                or final_stat.st_mtime_ns != initial_stat.st_mtime_ns
                or final_stat.st_ino != initial_stat.st_ino
            ):
                raise ValueError("ledger changed while it was being loaded")
            self._ledger_disk_size = size
            self._ledger_record_count = len(self._ledger_records)
        except Exception as exc:
            raise LiveOutcomeError(
                f"live outcome ledger is corrupt; feedback is fail-closed: {self.ledger_path}: {exc}"
            ) from exc

    def _merge_ledger_index_into_state(self) -> None:
        emitted = {str(value) for value in self._state.get("emitted_position_ids", [])}
        for position_key, record in self._ledger_records.items():
            emitted.add(position_key)
        self._state["emitted_position_ids"] = sorted(emitted, key=int)
        pending = self._state.get("pending_positions", {})
        for position_key in emitted:
            pending.pop(position_key, None)
        self._remove_closed_intent_aliases()

    def _remove_closed_intent_aliases(self) -> None:
        intents = self._state.get("entry_intents", {})
        for position_key, record in self._ledger_records.items():
            aliases = {self._intent_alias("position", position_key)}
            for field_name, prefix in (
                ("opening_deal_tickets", "deal"),
                ("opening_order_tickets", "order"),
            ):
                aliases.update(
                    self._intent_alias(prefix, ticket)
                    for ticket in record.get(field_name, []) or []
                    if _safe_int(ticket) > 0
                )
            for alias in aliases:
                intents.pop(alias, None)

    def _save_state(self) -> None:
        self._assert_runtime_state(self._state)
        self.state_path.parent.mkdir(parents=True, exist_ok=True)
        payload = dict(self._state)
        payload["schema_version"] = RUNTIME_STATE_SCHEMA_VERSION if self.runtime_identity is not None else STATE_SCHEMA_VERSION
        payload["updated_at"] = _iso_utc(datetime.now(timezone.utc))
        temp_path = self.state_path.with_name(
            f".{self.state_path.name}.tmp.{os.getpid()}.{threading.get_ident()}"
        )
        validated = self._validate_state_payload(payload)
        encoded = _canonical_json_bytes(validated)
        if len(encoded) > MAX_STATE_BYTES:
            raise LiveOutcomeError("live outcome state byte bound exhausted")
        current: Optional[bytes] = None
        if self.state_path.exists():
            if self.state_path.stat().st_size > MAX_STATE_BYTES:
                raise LiveOutcomeError("live outcome state changed or exceeds its bound")
            current = self.state_path.read_bytes()
        current_sha = hashlib.sha256(current).hexdigest() if current is not None else None
        if current_sha != self._state_disk_sha256:
            raise LiveOutcomeError("live outcome state changed concurrently; write blocked")
        try:
            with temp_path.open("wb") as handle:
                handle.write(encoded)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temp_path, self.state_path)
            persisted = self.state_path.read_bytes()
            if persisted != encoded:
                raise LiveOutcomeError("live outcome state post-write verification failed")
            self._state = validated
            self._state_disk_sha256 = hashlib.sha256(persisted).hexdigest()
        finally:
            try:
                temp_path.unlink(missing_ok=True)
            except OSError:
                pass

    def _append_record(self, record: Dict[str, Any]) -> None:
        self._assert_runtime_state(self._state)
        self.ledger_path.parent.mkdir(parents=True, exist_ok=True)
        record = self._validate_ledger_record(record)
        self._assert_runtime_record(record)
        encoded = _canonical_json_bytes(record) + b"\n"
        if self._validate_ledger_record(_strict_json_loads(encoded)) != record:
            raise LiveOutcomeError("live outcome ledger record reparse mismatch")
        if len(encoded) > MAX_LEDGER_RECORD_BYTES:
            raise LiveOutcomeError("live outcome ledger record byte bound exhausted")
        if self._ledger_record_count >= min(MAX_LEDGER_RECORDS, MAX_LEDGER_INDEX_RECORDS):
            raise LiveOutcomeError("live outcome ledger record/index bound exhausted")
        disk_size = self.ledger_path.stat().st_size if self.ledger_path.exists() else 0
        if disk_size != self._ledger_disk_size:
            raise LiveOutcomeError("live outcome ledger changed concurrently; append blocked")
        if disk_size + len(encoded) > MAX_LEDGER_BYTES:
            raise LiveOutcomeError("live outcome ledger byte bound exhausted")
        flags = os.O_APPEND | os.O_CREAT | os.O_WRONLY
        if hasattr(os, "O_BINARY"):
            flags |= os.O_BINARY
        descriptor = os.open(str(self.ledger_path), flags, 0o600)
        try:
            if os.fstat(descriptor).st_size != self._ledger_disk_size:
                raise LiveOutcomeError("live outcome ledger changed during append")
            view = memoryview(encoded)
            while view:
                written = os.write(descriptor, view)
                if written <= 0:
                    raise OSError("zero-byte append to live outcome ledger")
                view = view[written:]
            os.fsync(descriptor)
            expected_size = self._ledger_disk_size + len(encoded)
            if os.fstat(descriptor).st_size != expected_size:
                raise LiveOutcomeError("live outcome ledger append verification failed")
            self._ledger_disk_size = expected_size
            self._ledger_record_count += 1
        finally:
            os.close(descriptor)

    @staticmethod
    def _normalize_deal(deal: Mapping[str, Any], *, default_exit: bool = False) -> Dict[str, Any]:
        kind = _entry_kind(deal.get("entry_kind", deal.get("entry")))
        if default_exit and kind == "UNKNOWN":
            kind = "OUT"
        return {
            "ticket": _safe_int(deal.get("ticket")),
            "order_ticket": _safe_int(deal.get("order_ticket", deal.get("order"))),
            "position_id": _safe_int(deal.get("position_id", deal.get("position"))),
            "symbol": str(deal.get("symbol", "") or ""),
            "entry_kind": kind,
            "type": deal.get("type"),
            "volume": _safe_float(deal.get("volume")),
            "price": _safe_float(deal.get("price")),
            "profit": _safe_float(deal.get("profit")),
            "commission": _safe_float(deal.get("commission")),
            "swap": _safe_float(deal.get("swap")),
            "fee": _safe_float(deal.get("fee")),
            "magic": _safe_int(deal.get("magic")),
            "comment": str(deal.get("comment", "") or ""),
            "reason": deal.get("reason"),
            "time": _iso_utc(deal.get("time_msc") or deal.get("time")),
        }

    @staticmethod
    def _normalize_order(order: Mapping[str, Any]) -> Dict[str, Any]:
        return {
            "ticket": _safe_int(order.get("ticket", order.get("order"))),
            "position_id": _safe_int(order.get("position_id", order.get("position"))),
            "time": _iso_utc(order.get("time_setup_msc") or order.get("time_setup") or order.get("time")),
            "price_open": _safe_float(order.get("price_open", order.get("price"))),
            "volume_initial": _safe_float(order.get("volume_initial", order.get("volume"))),
            "initial_stop": _safe_float(order.get("initial_stop", order.get("sl"))),
            "initial_take_profit": _safe_float(order.get("initial_take_profit", order.get("tp"))),
            "magic": _safe_int(order.get("magic")),
            "comment": str(order.get("comment", "") or ""),
            "type": order.get("type"),
        }

    @staticmethod
    def _reservation_digest(payload: Mapping[str, Any]) -> str:
        unsigned = {key: value for key, value in payload.items() if key != "reservation_sha256"}
        return hashlib.sha256(_canonical_json_bytes(unsigned)).hexdigest()

    def new_entry_reservation_block_reason(self) -> str:
        """Return why new entries are blocked without affecting management."""
        with self._lock:
            reservations = self._state.get("pre_send_reservations", {})
            intents = self._state.get("entry_intents", {})
            if len(reservations) >= MAX_ENTRY_RESERVATIONS:
                return "pre-send reservation bound exhausted"
            if len(intents) > MAX_ENTRY_INTENT_ALIASES - 3:
                return "entry intent alias bound cannot accept a filled reservation"
            try:
                size = len(_canonical_json_bytes(self._state))
            except (TypeError, ValueError):
                return "live outcome state is not serializable"
            if size >= MAX_STATE_BYTES:
                return "live outcome state byte bound exhausted"
            return ""

    def unresolved_pre_send_intents(self) -> List[Dict[str, Any]]:
        """Return isolated copies of reservations requiring reconciliation."""
        with self._lock:
            reservations = self._state.get("pre_send_reservations", {})
            return [deepcopy(reservations[key]) for key in sorted(reservations)]

    def ambiguous_pre_send_intent(self, symbol: str, magic: int) -> Optional[Dict[str, Any]]:
        """Resolve only an explicitly ambiguous pending send by symbol and magic."""
        symbol_value = self._bounded_text(symbol, "symbol", required=True).upper()
        if isinstance(magic, bool) or not isinstance(magic, int) or magic <= 0:
            raise ValueError("magic must be a positive integer")
        with self._lock:
            matches = [
                payload
                for payload in self._state.get("pre_send_reservations", {}).values()
                if payload.get("status") == "AMBIGUOUS"
                and payload.get("symbol") == symbol_value
                and payload.get("magic") == magic
            ]
            if len(matches) > 1:
                raise LiveOutcomeError(
                    f"multiple ambiguous reservations for {symbol_value} magic {magic}"
                )
            return deepcopy(matches[0]) if matches else None

    def lookup_bound_entry_intent(
        self,
        *,
        position_id: int = 0,
        opening_deal_ticket: int = 0,
        opening_order_ticket: int = 0,
    ) -> Optional[Dict[str, Any]]:
        """Lookup one exact durable binding by broker identifiers."""
        values = (
            ("position", position_id),
            ("deal", opening_deal_ticket),
            ("order", opening_order_ticket),
        )
        for name, value in values:
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise ValueError(f"{name} identifier must be a non-negative integer")
        keys = [self._intent_alias(name, value) for name, value in values if value > 0]
        if not keys:
            raise ValueError("lookup requires a position, deal, or order ticket")
        with self._lock:
            intents = self._state.get("entry_intents", {})
            matches = [intents[key] for key in keys if key in intents]
            if not matches:
                return None
            canonical = matches[0]
            if any(match != canonical for match in matches[1:]):
                raise LiveOutcomeError("broker identifiers map to conflicting entry intents")
            return deepcopy(canonical)

    def reserve_pre_send_intent(
        self,
        *,
        symbol: str,
        timeframe: str,
        regime: str,
        strategy_name: str,
        magic: int,
        entry_price: float,
        volume: float,
        initial_stop: float,
        risk_cash: float,
        book_role: str,
        candidate_id: str = "",
        management_capsule_id: str = "",
        management_generation_id: str = "",
        reservation_id: str = "",
        reserved_at: Optional[datetime] = None,
        owner_snapshot: Optional[Mapping[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Fsync exact order attribution before any broker send."""
        if not self.enabled:
            raise LiveOutcomeError("live outcome feedback is disabled")
        identifier = reservation_id.strip() if isinstance(reservation_id, str) else ""
        if not identifier:
            identifier = uuid.uuid4().hex
        if not _RESERVATION_ID_RE.fullmatch(identifier):
            raise ValueError("reservation_id must be 32 lowercase hexadecimal characters")
        symbol_value = self._bounded_text(symbol, "symbol", required=True).upper()
        timeframe_value = self._bounded_text(timeframe, "timeframe", required=True).upper()
        regime_value = self._bounded_text(regime, "regime", required=True).upper()
        strategy_value = self._bounded_text(strategy_name, "strategy_name", required=True)
        role_value = self._bounded_text(book_role, "book_role", required=True).upper()
        candidate_value = self._bounded_text(candidate_id, "candidate_id")
        capsule_value = self._bounded_text(management_capsule_id, "management_capsule_id")
        generation_value = self._bounded_text(
            management_generation_id, "management_generation_id"
        )
        if isinstance(magic, bool) or not isinstance(magic, int) or magic <= 0:
            raise ValueError("magic must be a positive integer")
        payload: Dict[str, Any] = {
            "schema_version": RESERVATION_SCHEMA_VERSION,
            "reservation_id": identifier,
            "status": "RESERVED",
            "symbol": symbol_value,
            "timeframe": timeframe_value,
            "regime": regime_value,
            "strategy_name": strategy_value,
            "magic": magic,
            "entry_price": self._finite_number(entry_price, "entry_price", positive=True),
            "volume": self._finite_number(volume, "volume", positive=True),
            "initial_stop": self._finite_number(initial_stop, "initial_stop", positive=True),
            "risk_cash": self._finite_number(risk_cash, "risk_cash", positive=True),
            "book_role": role_value,
            "candidate_id": candidate_value,
            "management_capsule_id": capsule_value,
            "management_generation_id": generation_value,
            "reserved_at": _iso_utc(reserved_at or datetime.now(timezone.utc)),
            "ambiguity_reason": "",
            "observed_at": "",
            "broker_order_ticket": 0,
        }
        if owner_snapshot is not None:
            snapshot = _owner_snapshot(owner_snapshot)
            if any(snapshot[name] != payload[name] for name in
                   ("symbol", "timeframe", "regime", "strategy_name", "book_role", "candidate_id")):
                raise ValueError("held owner data does not match its reserved entry")
            payload["owner_snapshot"] = snapshot
        payload["reservation_sha256"] = self._reservation_digest(payload)
        payload = self._validate_reservation(identifier, payload)
        with self._lock:
            reservations = self._state.setdefault("pre_send_reservations", {})
            existing = reservations.get(identifier)
            if existing is not None:
                if existing != payload:
                    raise LiveOutcomeError(f"conflicting pre-send reservation {identifier}")
                return deepcopy(existing)
            reason = self.new_entry_reservation_block_reason()
            if reason:
                raise LiveOutcomeError(reason)
            for other in reservations.values():
                if other.get("symbol") == symbol_value and other.get("magic") == magic:
                    raise LiveOutcomeError(
                        f"unresolved reservation already exists for {symbol_value} magic {magic}"
                    )
            previous = deepcopy(self._state)
            reservations[identifier] = payload
            try:
                self._save_state()
            except Exception:
                self._state = previous
                raise
            return deepcopy(payload)

    def retain_ambiguous_pre_send_intent(
        self,
        reservation_id: str,
        *,
        reason: str,
        broker_order_ticket: int = 0,
        observed_at: Optional[datetime] = None,
    ) -> Dict[str, Any]:
        """Retain an uncertain send for restart reconciliation."""
        reason_value = self._bounded_text(reason, "reason", required=True)
        if (
            isinstance(broker_order_ticket, bool)
            or not isinstance(broker_order_ticket, int)
            or broker_order_ticket < 0
        ):
            raise ValueError("broker_order_ticket must be a non-negative integer")
        with self._lock:
            reservations = self._state.setdefault("pre_send_reservations", {})
            if reservation_id not in reservations:
                raise LiveOutcomeError(f"unknown pre-send reservation {reservation_id}")
            if self.runtime_identity is not None and self.runtime_identity.destination == "PAPER":
                if broker_order_ticket and not any(
                    row["reservation_id"] == reservation_id and row["opening_order_ticket"] == broker_order_ticket
                    for row in self._state["paper_positions"].values()
                ):
                    raise LiveOutcomeError("paper ambiguity cannot reference a broker order")
                broker_order_ticket = 0
            previous = deepcopy(self._state)
            payload = dict(reservations[reservation_id])
            payload.update(
                {
                    "status": "AMBIGUOUS",
                    "ambiguity_reason": reason_value,
                    "observed_at": _iso_utc(observed_at or datetime.now(timezone.utc)),
                    "broker_order_ticket": broker_order_ticket,
                }
            )
            payload["reservation_sha256"] = self._reservation_digest(payload)
            reservations[reservation_id] = self._validate_reservation(reservation_id, payload)
            try:
                self._save_state()
            except Exception:
                self._state = previous
                raise
            return deepcopy(reservations[reservation_id])

    def cancel_pre_send_intent(
        self,
        reservation_id: str,
        *,
        definitive_no_fill: bool,
        reason: str,
    ) -> Dict[str, Any]:
        """Cancel only after the broker proves no fill occurred."""
        if definitive_no_fill is not True:
            raise LiveOutcomeError("non-definitive outcome must retain its reservation")
        self._bounded_text(reason, "reason", required=True)
        with self._lock:
            reservations = self._state.setdefault("pre_send_reservations", {})
            if reservation_id not in reservations:
                raise LiveOutcomeError(f"unknown pre-send reservation {reservation_id}")
            if self.runtime_identity is not None and self.runtime_identity.destination == "PAPER" and any(
                row["reservation_id"] == reservation_id for row in self._state["paper_positions"].values()
            ):
                raise LiveOutcomeError("positive local fill cannot be cancelled as no-fill")
            previous = deepcopy(self._state)
            removed = deepcopy(reservations.pop(reservation_id))
            try:
                self._save_state()
            except Exception:
                self._state = previous
                raise
            return removed

    def bind_pre_send_intent(
        self,
        reservation_id: str,
        *,
        position_id: int = 0,
        opening_deal_ticket: int = 0,
        opening_order_ticket: int = 0,
        fill_price: Optional[float] = None,
        fill_volume: Optional[float] = None,
        risk_cash: Optional[float] = None,
        recorded_at: Optional[datetime] = None,
    ) -> Dict[str, Any]:
        """Atomically bind a durable reservation to broker fill identifiers."""
        identifiers = {
            "position_id": position_id,
            "opening_deal_ticket": opening_deal_ticket,
            "opening_order_ticket": opening_order_ticket,
        }
        for name, value in identifiers.items():
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise ValueError(f"{name} must be a non-negative integer")
        keys = [
            self._intent_alias(prefix, value)
            for prefix, value in (
                ("position", position_id),
                ("deal", opening_deal_ticket),
                ("order", opening_order_ticket),
            )
            if value > 0
        ]
        if not keys:
            raise ValueError("binding requires a position, deal, or order ticket")
        with self._lock:
            paper_row = None
            if self.runtime_identity is not None and self.runtime_identity.destination == "PAPER":
                matches = [row for row in self._state["paper_positions"].values() if row["reservation_id"] == reservation_id]
                if len(matches) != 1:
                    raise LiveOutcomeError("paper binding requires one durable positive local fill")
                paper_row = matches[0]
                actual = {"position_id": paper_row["ticket"], "opening_deal_ticket": paper_row["opening_deal_ticket"],
                          "opening_order_ticket": paper_row["opening_order_ticket"]}
                if any(value > 0 and value != actual[name] for name, value in identifiers.items()):
                    raise LiveOutcomeError("paper binding cannot reference broker identifiers")
                identifiers = actual
                keys = [self._intent_alias("position", actual["position_id"]),
                        self._intent_alias("deal", actual["opening_deal_ticket"]),
                        self._intent_alias("order", actual["opening_order_ticket"])]
                if fill_price is not None and fill_price != paper_row["price_open"]:
                    raise LiveOutcomeError("paper binding changed its observed fill price")
                if fill_volume is not None and fill_volume != paper_row["volume_initial"]:
                    raise LiveOutcomeError("paper binding changed its observed fill volume")
                fill_price, fill_volume = paper_row["price_open"], paper_row["volume_initial"]
                if risk_cash is None:
                    risk_cash = paper_row["risk_cash"]
                if risk_cash is None:
                    raise LiveOutcomeError("paper binding needs observed filled stop risk")
            reservations = self._state.setdefault("pre_send_reservations", {})
            reservation = reservations.get(reservation_id)
            intents = self._state.setdefault("entry_intents", {})
            if reservation is None:
                bound_aliases = {
                    alias: payload
                    for alias, payload in intents.items()
                    if payload.get("reservation_id") == reservation_id
                }
                canonical = {
                    _canonical_json_bytes(payload): payload for payload in bound_aliases.values()
                }
                if len(canonical) != 1:
                    raise LiveOutcomeError(f"unknown or conflicting reservation {reservation_id}")
                existing = next(iter(canonical.values()))
                if any(
                    value > 0 and int(existing.get(name, 0)) not in {0, value}
                    for name, value in identifiers.items()
                ):
                    raise LiveOutcomeError(f"conflicting broker binding for {reservation_id}")
                for supplied, name in (
                    (fill_price, "entry_price"),
                    (fill_volume, "volume"),
                    (risk_cash, "risk_cash"),
                ):
                    if supplied is not None and self._finite_number(
                        supplied, name, positive=True
                    ) != float(existing[name]):
                        raise LiveOutcomeError(f"conflicting filled {name} for {reservation_id}")
                merged = dict(existing)
                for name, value in identifiers.items():
                    if value > 0:
                        merged[name] = value
                all_keys = set(bound_aliases) | set(keys)
                for key in all_keys:
                    self._validate_entry_intent(key, merged)
                    other = intents.get(key)
                    if other is not None and other != existing:
                        raise LiveOutcomeError(f"conflicting durable entry intent for {key}")
                if len(intents) + sum(key not in intents for key in all_keys) > MAX_ENTRY_INTENT_ALIASES:
                    raise LiveOutcomeError("entry intent alias bound exhausted")
                previous = deepcopy(self._state)
                if paper_row is not None:
                    paper_row["risk_cash"] = merged["risk_cash"]
                for key in all_keys:
                    intents[key] = merged
                try:
                    self._save_state()
                except Exception:
                    self._state = previous
                    raise
                return deepcopy(merged)
            planned_price = float(reservation["entry_price"])
            planned_volume = float(reservation["volume"])
            planned_risk = float(reservation["risk_cash"])
            payload = {
                **identifiers,
                "symbol": reservation["symbol"],
                "timeframe": reservation["timeframe"],
                "regime": reservation["regime"],
                "strategy_name": reservation["strategy_name"],
                "magic": reservation["magic"],
                "entry_price": self._finite_number(
                    planned_price if fill_price is None else fill_price,
                    "fill_price",
                    positive=True,
                ),
                "volume": self._finite_number(
                    planned_volume if fill_volume is None else fill_volume,
                    "fill_volume",
                    positive=True,
                ),
                "initial_stop": reservation["initial_stop"],
                "risk_cash": self._finite_number(
                    planned_risk if risk_cash is None else risk_cash,
                    "risk_cash",
                    positive=True,
                ),
                "book_role": reservation["book_role"],
                "candidate_id": reservation["candidate_id"],
                "management_capsule_id": reservation["management_capsule_id"],
                "management_generation_id": reservation["management_generation_id"],
                "recorded_at": _iso_utc(recorded_at or datetime.now(timezone.utc)),
                "reservation_id": reservation_id,
                "reserved_at": reservation["reserved_at"],
                "reserved_entry_price": planned_price,
                "reserved_volume": planned_volume,
                "reserved_risk_cash": planned_risk,
            }
            if "owner_snapshot" in reservation:
                payload["owner_snapshot"] = deepcopy(reservation["owner_snapshot"])
            for key in keys:
                self._validate_entry_intent(key, payload)
                existing = intents.get(key)
                if existing is not None and existing != payload:
                    raise LiveOutcomeError(f"conflicting durable entry intent for {key}")
            new_alias_count = sum(1 for key in keys if key not in intents)
            if len(intents) + new_alias_count > MAX_ENTRY_INTENT_ALIASES:
                raise LiveOutcomeError("entry intent alias bound exhausted; reservation retained")
            previous = deepcopy(self._state)
            if paper_row is not None:
                paper_row["risk_cash"] = payload["risk_cash"]
            for key in keys:
                intents[key] = payload
            reservations.pop(reservation_id)
            try:
                self._save_state()
            except Exception:
                self._state = previous
                raise
            return deepcopy(payload)

    def record_entry_intent(
        self,
        *,
        position_id: int = 0,
        opening_deal_ticket: int = 0,
        opening_order_ticket: int = 0,
        symbol: str,
        timeframe: str,
        regime: str,
        strategy_name: str,
        magic: int,
        entry_price: float,
        volume: float,
        initial_stop: float,
        risk_cash: Optional[float],
        book_role: str = "",
        candidate_id: str = "",
        recorded_at: Optional[datetime] = None,
    ) -> None:
        """Persist entry risk metadata after an existing order has filled.

        This method only records evidence; it cannot place, modify, or promote a
        trade.  At least one broker identifier is required for later recovery.
        """
        if not self.enabled:
            return
        if self.runtime_identity is not None and self.runtime_identity.destination == "PAPER":
            raise LiveOutcomeError("paper owner binding requires its original pre-send reservation")
        keys: List[str] = []
        if _safe_int(position_id) > 0:
            keys.append(f"position:{_safe_int(position_id)}")
        if _safe_int(opening_deal_ticket) > 0:
            keys.append(f"deal:{_safe_int(opening_deal_ticket)}")
        if _safe_int(opening_order_ticket) > 0:
            keys.append(f"order:{_safe_int(opening_order_ticket)}")
        if not keys:
            raise ValueError("entry intent requires a position, deal, or order ticket")
        risk_value = _safe_float(risk_cash)
        payload = {
            "position_id": _safe_int(position_id),
            "opening_deal_ticket": _safe_int(opening_deal_ticket),
            "opening_order_ticket": _safe_int(opening_order_ticket),
            "symbol": str(symbol or "").upper(),
            "timeframe": str(timeframe or "").upper(),
            "regime": str(regime or "").upper(),
            "strategy_name": str(strategy_name or ""),
            "magic": _safe_int(magic),
            "entry_price": _safe_float(entry_price),
            "volume": _safe_float(volume),
            "initial_stop": _safe_float(initial_stop),
            "risk_cash": risk_value if risk_value > 0.0 else None,
            "book_role": str(book_role or "").strip().upper(),
            "candidate_id": str(candidate_id or "").strip(),
            "recorded_at": _iso_utc(recorded_at or datetime.now(timezone.utc)),
        }
        with self._lock:
            intents = self._state.setdefault("entry_intents", {})
            for key in keys:
                self._validate_entry_intent(key, payload)
                existing = intents.get(key)
                if existing is not None and existing != payload:
                    raise LiveOutcomeError(f"conflicting durable entry intent for {key}")
            new_alias_count = sum(1 for key in keys if key not in intents)
            if len(intents) + new_alias_count > MAX_ENTRY_INTENT_ALIASES:
                raise LiveOutcomeError("entry intent alias bound exhausted")
            previous = deepcopy(self._state)
            for key in keys:
                intents[key] = payload
            try:
                self._save_state()
            except Exception:
                self._state = previous
                raise

    def _find_entry_intent(
        self,
        position_key: str,
        entry_deals: List[Dict[str, Any]],
    ) -> Optional[Dict[str, Any]]:
        intents = self._state.get("entry_intents", {})
        keys = [self._intent_alias("position", position_key)]
        for deal in entry_deals:
            if deal["ticket"] > 0:
                keys.append(self._intent_alias("deal", deal['ticket']))
            if deal["order_ticket"] > 0:
                keys.append(self._intent_alias("order", deal['order_ticket']))
        matches = [intents[key] for key in keys if isinstance(intents.get(key), dict)]
        if not matches:
            return None
        canonical = matches[0]
        if any(match != canonical for match in matches[1:]):
            raise LiveOutcomeError(f"conflicting entry intent aliases for position {position_key}")
        return dict(canonical)

    def _build_record(
        self,
        *,
        position_key: str,
        pending: Dict[str, Any],
        history: Dict[str, Any],
        resolve_cell: Optional[Callable[[Dict[str, Any]], Optional[Dict[str, Any]]]],
    ) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
        deal_by_ticket: Dict[int, Dict[str, Any]] = {}
        for raw in history.get("deals", []) or []:
            if not isinstance(raw, Mapping):
                continue
            deal = self._normalize_deal(raw)
            if deal["ticket"] > 0 and self._position_key(deal["position_id"]) == position_key:
                deal_by_ticket[deal["ticket"]] = deal
        for raw in (pending.get("exit_deals", {}) or {}).values():
            if not isinstance(raw, Mapping):
                continue
            deal = self._normalize_deal(raw, default_exit=True)
            if deal["ticket"] > 0:
                deal_by_ticket.setdefault(deal["ticket"], deal)
        deals = sorted(deal_by_ticket.values(), key=lambda item: (item["time"], item["ticket"]))
        if any(deal["entry_kind"] == "INOUT" for deal in deals):
            return None, "unsupported INOUT/reversal deal in position history"
        entries = [deal for deal in deals if deal["entry_kind"] == "IN"]
        exits = [deal for deal in deals if deal["entry_kind"] in {"OUT", "OUT_BY"}]
        if not entries:
            return None, "position history has no opening deal"
        if not exits:
            return None, "position history has no closing deal"
        entry_volume = sum(deal["volume"] for deal in entries)
        exit_volume = sum(deal["volume"] for deal in exits)
        tolerance = self.volume_tolerance * max(1.0, entry_volume, exit_volume)
        if entry_volume <= 0.0 or exit_volume <= 0.0:
            return None, "position history has non-positive entry/exit volume"
        if abs(entry_volume - exit_volume) > tolerance:
            return None, f"entry/exit volume mismatch {entry_volume:.12g}!={exit_volume:.12g}"

        orders = [
            self._normalize_order(raw)
            for raw in history.get("orders", []) or []
            if isinstance(raw, Mapping)
        ]
        entry_order_tickets = sorted({deal["order_ticket"] for deal in entries if deal["order_ticket"] > 0})
        exit_order_tickets = sorted({deal["order_ticket"] for deal in exits if deal["order_ticket"] > 0})
        opening_orders = [order for order in orders if order["ticket"] in entry_order_tickets]
        primary_entry = entries[0]
        entry_comment = primary_entry["comment"]
        entry_magic = primary_entry["magic"]
        if opening_orders:
            opening_order = sorted(opening_orders, key=lambda item: (item["time"], item["ticket"]))[0]
            entry_comment = entry_comment or opening_order["comment"]
            entry_magic = entry_magic or opening_order["magic"]
        decoded = TradeTagEncoder.decode_comment(entry_comment) if entry_comment else None
        decoded = dict(decoded or {})

        intent = self._find_entry_intent(position_key, entries)
        initial_stop = 0.0
        risk_cash = 0.0
        risk_source = ""
        if intent:
            initial_stop = _safe_float(intent.get("initial_stop"))
            risk_cash = _safe_float(intent.get("risk_cash"))
            if risk_cash > 0.0:
                risk_source = "durable_entry_intent"
        if initial_stop <= 0.0:
            initial_stop = _safe_float(history.get("initial_stop"))
        if initial_stop <= 0.0:
            for order in opening_orders:
                if order["initial_stop"] > 0.0:
                    initial_stop = order["initial_stop"]
                    break
        if risk_cash <= 0.0:
            risk_cash = _safe_float(history.get("risk_cash"))
            if risk_cash > 0.0:
                risk_source = "trusted_position_history"

        gross_profit = sum(deal["profit"] for deal in deals)
        commission = sum(deal["commission"] for deal in deals)
        swap = sum(deal["swap"] for deal in deals)
        fee = sum(deal["fee"] for deal in deals)
        net_pnl = gross_profit + commission + swap + fee
        weighted_entry = sum(deal["price"] * deal["volume"] for deal in entries) / entry_volume
        weighted_exit = sum(deal["price"] * deal["volume"] for deal in exits) / exit_volume

        broker_symbol = primary_entry["symbol"] or exits[-1]["symbol"]
        resolution_context = {
            "position_id": _safe_int(position_key),
            "broker_symbol": broker_symbol,
            "entry_comment": entry_comment,
            "entry_magic": entry_magic,
            "decoded_comment": decoded,
            "entry_intent": dict(intent or {}),
            "opening_deal_tickets": [deal["ticket"] for deal in entries],
            "opening_order_tickets": entry_order_tickets,
        }
        resolved: Dict[str, Any] = {}
        resolution_error = "no exact cell resolver"
        if resolve_cell is not None:
            try:
                candidate = resolve_cell(resolution_context)
                if isinstance(candidate, dict):
                    resolved = dict(candidate)
                    resolution_error = str(resolved.get("cell_match_reason", "") or "")
            except Exception as exc:
                resolution_error = f"exact cell resolver error: {type(exc).__name__}: {exc}"
        required_cell_fields = ("symbol", "timeframe", "regime", "strategy_name")
        exact_cell = str(resolved.get("cell_match_status", "") or "").upper() == "EXACT"
        exact_cell = exact_cell and all(str(resolved.get(name, "") or "").strip() for name in required_cell_fields)
        if not exact_cell:
            resolved = {
                "symbol": str(decoded.get("symbol", "") or broker_symbol),
                "timeframe": str(decoded.get("timeframe", "") or ""),
                "regime": "",
                "strategy_name": str(decoded.get("strategy_name", "") or ""),
                "cell_match_status": "UNAVAILABLE",
                "cell_match_reason": resolution_error or "resolver did not prove one exact cell",
            }

        r_multiple: Optional[float] = None
        r_unavailable_reason: Optional[str] = None
        if risk_cash > 0.0 and math.isfinite(risk_cash):
            r_multiple = net_pnl / risk_cash
        else:
            risk_cash = 0.0
            r_unavailable_reason = "risk_cash_unavailable:no_durable_entry_intent_or_trusted_broker_value"

        opened_at = min((deal["time"] for deal in entries if deal["time"]), default="")
        closed_at = max((deal["time"] for deal in exits if deal["time"]), default="")
        record = {
            "schema_version": LEDGER_SCHEMA_VERSION,
            "event_id": f"mt5-position-close:{position_key}",
            "position_id": _safe_int(position_key),
            "symbol": str(resolved.get("symbol", "") or "").upper(),
            "broker_symbol": broker_symbol,
            "timeframe": str(resolved.get("timeframe", "") or "").upper(),
            "regime": str(resolved.get("regime", "") or "").upper(),
            "strategy_name": str(resolved.get("strategy_name", "") or ""),
            "book_role": str(
                resolved.get("book_role")
                or (intent or {}).get("book_role")
                or decoded.get("book_role")
                or "LOCKBOX_VALIDATED_CHAMPION"
            ).upper(),
            "candidate_id": str(
                resolved.get("candidate_id")
                or (intent or {}).get("candidate_id")
                or ""
            ),
            "cell_match_status": str(resolved.get("cell_match_status", "UNAVAILABLE") or "UNAVAILABLE").upper(),
            "cell_match_reason": str(resolved.get("cell_match_reason", "") or ""),
            "opening_deal_tickets": [deal["ticket"] for deal in entries],
            "opening_order_tickets": entry_order_tickets,
            "closing_deal_tickets": [deal["ticket"] for deal in exits],
            "closing_order_tickets": exit_order_tickets,
            "opened_at": opened_at,
            "closed_at": closed_at,
            "entry_volume": entry_volume,
            "exit_volume": exit_volume,
            "volume_weighted_entry": weighted_entry,
            "volume_weighted_exit": weighted_exit,
            "gross_profit": gross_profit,
            "commission": commission,
            "swap": swap,
            "fee": fee,
            "realized_net_pnl": net_pnl,
            "entry_comment": entry_comment,
            "entry_magic": entry_magic,
            "entry_metadata": decoded,
            "entry_reservation_id": str((intent or {}).get("reservation_id", "") or ""),
            "management_capsule_id": str(
                (intent or {}).get("management_capsule_id", "") or ""
            ),
            "management_generation_id": str(
                (intent or {}).get("management_generation_id", "") or ""
            ),
            "reserved_at": str((intent or {}).get("reserved_at", "") or ""),
            "reserved_entry_price": (intent or {}).get("reserved_entry_price"),
            "reserved_volume": (intent or {}).get("reserved_volume"),
            "reserved_risk_cash": (intent or {}).get("reserved_risk_cash"),
            "initial_stop": initial_stop if initial_stop > 0.0 else None,
            "initial_risk_cash": risk_cash if risk_cash > 0.0 else None,
            "initial_risk_source": risk_source or None,
            "realized_r": r_multiple,
            "realized_r_unavailable_reason": r_unavailable_reason,
            "eligible_for_auto_promotion": False,
            "recorded_at": _iso_utc(datetime.now(timezone.utc)),
        }
        if self.runtime_identity is not None:
            prefix = "paper-position-close" if self.runtime_identity.destination == "PAPER" else "mt5-position-close"
            record.update(schema_version=RUNTIME_LEDGER_SCHEMA_VERSION,
                          runtime_identity=self.runtime_identity.to_dict(),
                          event_id=f"{prefix}:{self.runtime_identity.namespace}:{position_key}")
        if intent is not None and "owner_snapshot" in intent:
            record["owner_snapshot"] = deepcopy(intent["owner_snapshot"])
        return record, None

    def _deliver_pending_drift(self, drift_monitor: Optional[Any]) -> int:
        if not self.feed_cycle_drift or drift_monitor is None:
            return 0
        delivered = {str(value) for value in self._state.get("drift_delivered_position_ids", [])}
        delivered_count = 0
        for position_key, record in sorted(self._ledger_records.items(), key=lambda item: int(item[0])):
            if position_key in delivered:
                continue
            if self.paper_outcome_admission(record) != "CLEAN":
                continue
            if record.get("cell_match_status") != "EXACT":
                delivered.add(position_key)
                continue
            try:
                drift_monitor.record_trade(
                    symbol=record["symbol"],
                    timeframe=record["timeframe"],
                    regime=record["regime"],
                    strategy_name=record["strategy_name"],
                    pnl_dollars=record["realized_net_pnl"],
                    event_id=record["event_id"],
                )
            except TypeError as exc:
                # Never fall back to the old non-idempotent API: duplicate live
                # losses would be worse than delayed drift evidence.
                raise LiveOutcomeError(
                    "CycleDriftMonitor lacks idempotent event_id support; feedback is fail-closed"
                ) from exc
            delivered.add(position_key)
            delivered_count += 1
            self._state["drift_delivered_position_ids"] = sorted(delivered, key=int)
            self._save_state()
        self._state["drift_delivered_position_ids"] = sorted(delivered, key=int)
        return delivered_count

    def sync(
        self,
        *,
        closing_deals: Iterable[Mapping[str, Any]],
        open_position_ids: Optional[Set[int]],
        position_history_provider: Optional[Callable[[int], Optional[Dict[str, Any]]]],
        resolve_cell: Optional[Callable[[Dict[str, Any]], Optional[Dict[str, Any]]]],
        drift_monitor: Optional[Any] = None,
    ) -> OutcomeSyncResult:
        """Ingest exits and emit only fully closed, broker-reconciled positions.

        ``open_position_ids=None`` means the MT5 position snapshot was not
        trustworthy.  In that case exit deals are durably staged, but no close
        is emitted and no drift state is changed.
        """
        result = OutcomeSyncResult(state_recovered=self._state_recovered)
        if not self.enabled:
            return result
        with self._lock:
            if self.runtime_identity is not None and self.runtime_identity.destination == "PAPER":
                self._require_paper()
                known = self._state["paper_deals"]
                for supplied in closing_deals or []:
                    if not isinstance(supplied, Mapping) or dict(supplied) != known.get(str(supplied.get("ticket"))):
                        raise LiveOutcomeError("PAPER sync cannot ingest broker or foreign deal identifiers")
                closing_deals = [row for row in self.paper_deals() if row["entry_kind"] == "OUT"]
                open_position_ids = {row["ticket"] for row in self.paper_positions()}
                position_history_provider = self.paper_history
            seen = {_safe_int(value) for value in self._state.get("seen_deal_tickets", []) if _safe_int(value) > 0}
            emitted = {str(value) for value in self._state.get("emitted_position_ids", [])}
            pending_positions = self._state.setdefault("pending_positions", {})
            pending_exit_count = sum(
                len((pending.get("exit_deals", {}) or {}))
                for pending in pending_positions.values()
                if isinstance(pending, dict)
            )
            for raw in closing_deals or []:
                if not isinstance(raw, Mapping):
                    continue
                deal = self._normalize_deal(raw, default_exit=True)
                ticket = deal["ticket"]
                position_key = self._position_key(deal["position_id"])
                if ticket <= 0 or not position_key or deal["entry_kind"] not in {"OUT", "OUT_BY"}:
                    result.fail_closed_reasons.append("ignored malformed/non-exit closing deal")
                    continue
                if position_key in emitted:
                    result.duplicate_deal_count += 1
                    continue
                if ticket in seen:
                    result.duplicate_deal_count += 1
                    continue
                if position_key not in pending_positions and len(pending_positions) >= MAX_PENDING_POSITIONS:
                    result.fail_closed_reasons.append(
                        f"position {position_key}: pending position bound exhausted"
                    )
                    continue
                if pending_exit_count >= MAX_PENDING_EXIT_DEALS or len(seen) >= MAX_PENDING_EXIT_DEALS:
                    result.fail_closed_reasons.append(
                        f"position {position_key}: pending exit evidence bound exhausted"
                    )
                    continue
                seen.add(ticket)
                result.ingested_deal_count += 1
                pending = pending_positions.setdefault(position_key, {"exit_deals": {}, "last_reason": ""})
                exit_deals = pending.setdefault("exit_deals", {})
                if str(ticket) not in exit_deals:
                    pending_exit_count += 1
                exit_deals[str(ticket)] = deal
            self._state["seen_deal_tickets"] = sorted(seen)
            self._save_state()

            if open_position_ids is None:
                result.fail_closed_reasons.append("open position snapshot unavailable")
            else:
                open_keys = {self._position_key(value) for value in open_position_ids if self._position_key(value)}
                for position_key in sorted(list(pending_positions.keys()), key=int):
                    if position_key in emitted or position_key in open_keys:
                        continue
                    pending = pending_positions[position_key]
                    if self.runtime_identity is not None and self.runtime_identity.destination == "PAPER":
                        local = self._state["paper_positions"][position_key]
                        if local["reservation_id"] in self._state["pre_send_reservations"]:
                            pending["last_reason"] = "positive paper fill awaits durable original-owner binding"
                            result.fail_closed_reasons.append(f"position {position_key}: {pending['last_reason']}")
                            continue
                    if self.require_history and position_history_provider is None:
                        pending["last_reason"] = "position history provider unavailable"
                        result.fail_closed_reasons.append(f"position {position_key}: {pending['last_reason']}")
                        continue
                    history: Dict[str, Any]
                    if position_history_provider is None:
                        history = {
                            "history_complete": False,
                            "deals": list((pending.get("exit_deals", {}) or {}).values()),
                            "orders": [],
                        }
                    else:
                        try:
                            raw_history = position_history_provider(int(position_key))
                        except Exception as exc:
                            raw_history = None
                            pending["last_reason"] = f"position history error: {type(exc).__name__}: {exc}"
                        history = dict(raw_history or {})
                    if self.require_history and not bool(history.get("history_complete", False)):
                        reason = str(pending.get("last_reason", "") or "position history incomplete")
                        pending["last_reason"] = reason
                        result.fail_closed_reasons.append(f"position {position_key}: {reason}")
                        continue
                    record, reason = self._build_record(
                        position_key=position_key,
                        pending=pending,
                        history=history,
                        resolve_cell=resolve_cell,
                    )
                    if record is None:
                        pending["last_reason"] = reason or "position reconciliation failed"
                        result.fail_closed_reasons.append(f"position {position_key}: {pending['last_reason']}")
                        continue
                    self._append_record(record)
                    self._ledger_records[position_key] = record
                    emitted.add(position_key)
                    pending_positions.pop(position_key, None)
                    closed_tickets = {
                        _safe_int(ticket)
                        for field_name in ("opening_deal_tickets", "closing_deal_tickets")
                        for ticket in record.get(field_name, []) or []
                        if _safe_int(ticket) > 0
                    }
                    seen.difference_update(closed_tickets)
                    self._state["seen_deal_tickets"] = sorted(seen)
                    if self.runtime_identity is None or self.runtime_identity.destination != "PAPER":
                        result.emitted_records.append(record)
                    self._state["emitted_position_ids"] = sorted(emitted, key=int)
                    self._remove_closed_intent_aliases()
                    self._save_state()

            if self.runtime_identity is not None and self.runtime_identity.destination == "PAPER":
                for position_key, record in sorted(self._ledger_records.items(), key=lambda item: int(item[0])):
                    local = self._state["paper_positions"].get(position_key)
                    if (local is None or local["statistics_delivered"]
                            or record.get("cell_match_status") != "EXACT"
                            or self.paper_outcome_admission(record) != "CLEAN"):
                        continue
                    previous = deepcopy(self._state)
                    local["statistics_delivered"] = True
                    self._paper_save(previous)
                    result.emitted_records.append(deepcopy(record))
                result.paper_statistics = self.paper_statistics_summary()
            try:
                result.delivered_drift_count = self._deliver_pending_drift(drift_monitor)
            except LiveOutcomeError as exc:
                result.fail_closed_reasons.append(str(exc))
            unmatched = [
                position_key
                for position_key, record in self._ledger_records.items()
                if str(record.get("cell_match_status", "") or "").upper() != "EXACT"
            ]
            if unmatched:
                preview = ",".join(sorted(unmatched, key=int)[:8])
                result.fail_closed_reasons.append(
                    "closed positions lack exact cell attribution: " + preview
                )
            result.pending_position_count = len(pending_positions)
            self._save_state()
        return result
