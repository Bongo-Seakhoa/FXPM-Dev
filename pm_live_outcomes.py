"""Durable live-entry attribution and position-level outcome accounting.

The module has no order or promotion API. Pre-send reservations and unresolved
fills live in the fsynced state file. Closed outcomes move to the append-only
ledger before state cleanup. Any ambiguous or corrupt transition fails closed.
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
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Mapping, Optional, Set, Tuple, Union

from pm_position import TradeTagEncoder


LEGACY_STATE_SCHEMA_VERSION = "2026-07-10-live-outcome-state-v1"
STATE_SCHEMA_VERSION = "2026-07-14-live-outcome-state-v2"
LEDGER_SCHEMA_VERSION = "2026-07-10-matched-position-close-v1"
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

_RESERVATION_ID_RE = re.compile(r"^[0-9a-f]{32}$")
_INTENT_ALIAS_RE = re.compile(r"^(position|deal|order):[1-9][0-9]*$")
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


@dataclass
class OutcomeSyncResult:
    emitted_records: List[Dict[str, Any]] = field(default_factory=list)
    ingested_deal_count: int = 0
    duplicate_deal_count: int = 0
    pending_position_count: int = 0
    delivered_drift_count: int = 0
    fail_closed_reasons: List[str] = field(default_factory=list)
    state_recovered: bool = False


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
    ) -> None:
        self.config = config
        base_dir = Path(getattr(config, "output_dir", "pm_outputs")) / "production_live"
        configured_ledger = str(getattr(config, "live_outcome_ledger_path", "") or "").strip()
        configured_state = str(getattr(config, "live_outcome_state_path", "") or "").strip()
        self.ledger_path = Path(ledger_path or configured_ledger or (base_dir / "closed_positions.jsonl"))
        self.state_path = Path(state_path or configured_state or (base_dir / "closed_positions.state.json"))
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
        self._state_recovered = False
        self._state_disk_sha256: Optional[str] = None
        self._ledger_disk_size = 0
        self._ledger_record_count = 0
        self._ledger_records: Dict[str, Dict[str, Any]] = {}
        self._state: Dict[str, Any] = self._empty_state()
        self._load_state()
        self._load_ledger_index()
        self._merge_ledger_index_into_state()
        if self._state_recovered:
            self._save_state()

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

    @property
    def state_recovered(self) -> bool:
        return self._state_recovered

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
        profit_factor = gains / losses if losses > 1e-12 else (float("inf") if gains > 0.0 else 0.0)
        return {
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
        }
        if not isinstance(raw, dict) or not required.issubset(raw) or not set(raw).issubset(required | optional):
            raise ValueError("ledger record has an invalid schema")
        record = dict(raw)
        if record["schema_version"] != LEDGER_SCHEMA_VERSION:
            raise ValueError("ledger record has an unsupported schema")
        position_id = record["position_id"]
        if not isinstance(position_id, int) or isinstance(position_id, bool) or position_id <= 0:
            raise ValueError("ledger record has an invalid position_id")
        if record["event_id"] != f"mt5-position-close:{position_id}":
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
        if set(raw) != fields:
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
        return payload

    @classmethod
    def _validate_state_payload(cls, raw: Any) -> Dict[str, Any]:
        if not isinstance(raw, dict):
            raise ValueError("state is not an object")
        allowed = set(cls._empty_state())
        if not set(raw).issubset(allowed):
            raise ValueError("state has unknown fields")
        schema = raw.get("schema_version")
        if schema not in {STATE_SCHEMA_VERSION, LEGACY_STATE_SCHEMA_VERSION}:
            raise ValueError("unsupported or missing state schema")
        state = cls._empty_state()
        state.update(raw)
        state["schema_version"] = STATE_SCHEMA_VERSION
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
            self._state = self._validate_state_payload(raw)
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
            aliases = {f"position:{position_key}"}
            for field_name, prefix in (
                ("opening_deal_tickets", "deal"),
                ("opening_order_tickets", "order"),
            ):
                aliases.update(
                    f"{prefix}:{ticket}"
                    for ticket in record.get(field_name, []) or []
                    if _safe_int(ticket) > 0
                )
            for alias in aliases:
                intents.pop(alias, None)

    def _save_state(self) -> None:
        self.state_path.parent.mkdir(parents=True, exist_ok=True)
        payload = dict(self._state)
        payload["schema_version"] = STATE_SCHEMA_VERSION
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
        self.ledger_path.parent.mkdir(parents=True, exist_ok=True)
        record = self._validate_ledger_record(record)
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
        keys = [f"{name}:{value}" for name, value in values if value > 0]
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
            f"{prefix}:{value}"
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
            for key in keys:
                self._validate_entry_intent(key, payload)
                existing = intents.get(key)
                if existing is not None and existing != payload:
                    raise LiveOutcomeError(f"conflicting durable entry intent for {key}")
            new_alias_count = sum(1 for key in keys if key not in intents)
            if len(intents) + new_alias_count > MAX_ENTRY_INTENT_ALIASES:
                raise LiveOutcomeError("entry intent alias bound exhausted; reservation retained")
            previous = deepcopy(self._state)
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
        keys = [f"position:{position_key}"]
        for deal in entry_deals:
            if deal["ticket"] > 0:
                keys.append(f"deal:{deal['ticket']}")
            if deal["order_ticket"] > 0:
                keys.append(f"order:{deal['order_ticket']}")
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
        return record, None

    def _deliver_pending_drift(self, drift_monitor: Optional[Any]) -> int:
        if not self.feed_cycle_drift or drift_monitor is None:
            return 0
        delivered = {str(value) for value in self._state.get("drift_delivered_position_ids", [])}
        delivered_count = 0
        for position_key, record in sorted(self._ledger_records.items(), key=lambda item: int(item[0])):
            if position_key in delivered:
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
                    result.emitted_records.append(record)
                    self._state["emitted_position_ids"] = sorted(emitted, key=int)
                    self._remove_closed_intent_aliases()
                    self._save_state()

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
