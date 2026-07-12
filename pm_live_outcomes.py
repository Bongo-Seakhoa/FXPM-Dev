"""Durable, position-level live outcome accounting for MetaTrader 5.

This module is deliberately observation-only.  It has no order API and no
promotion API.  Its one downstream side effect is an idempotent delivery of a
fully closed, exactly attributed position to :class:`CycleDriftMonitor`.

The JSONL ledger is the append-only source of truth.  The JSON state file is a
restart-safe index/outbox used for partial exits, entry-risk metadata, and
deduplication.  A ledger record is appended before its state index is updated;
therefore a crash can at worst leave stale state, which is reconstructed from
the ledger on the next start.
"""

from __future__ import annotations

import json
import math
import os
import threading
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Mapping, Optional, Set, Tuple, Union

from pm_position import TradeTagEncoder


STATE_SCHEMA_VERSION = "2026-07-10-live-outcome-state-v1"
LEDGER_SCHEMA_VERSION = "2026-07-10-matched-position-close-v1"


class LiveOutcomeError(RuntimeError):
    """Raised when durable outcome evidence cannot be trusted."""


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
        with self._lock:
            records = [
                dict(record)
                for record in self._ledger_records.values()
                if str(record.get("cell_match_status", "") or "").upper() == "EXACT"
                and (
                    str(record.get("symbol", "") or "").upper(),
                    str(record.get("timeframe", "") or "").upper(),
                    str(record.get("regime", "") or "").upper(),
                    str(record.get("strategy_name", "") or ""),
                ) == target
            ]
        realized_r = [
            float(record["realized_r"])
            for record in records
            if record.get("realized_r") is not None
            and math.isfinite(float(record.get("realized_r")))
        ]
        gains = sum(value for value in realized_r if value > 0.0)
        losses = -sum(value for value in realized_r if value < 0.0)
        profit_factor = gains / losses if losses > 1e-12 else (float("inf") if gains > 0.0 else 0.0)
        return {
            "exact_closed_positions": len(records),
            "r_observations": len(realized_r),
            "mean_r": (sum(realized_r) / len(realized_r)) if realized_r else None,
            "profit_factor_r": profit_factor,
        }

    @staticmethod
    def _position_key(position_id: Any) -> str:
        numeric = _safe_int(position_id)
        return str(numeric) if numeric > 0 else ""

    def _load_state(self) -> None:
        if not self.state_path.exists():
            return
        try:
            payload = json.loads(self.state_path.read_text(encoding="utf-8"))
            if not isinstance(payload, dict) or payload.get("schema_version") != STATE_SCHEMA_VERSION:
                raise ValueError("unsupported or missing state schema")
            for key in (
                "seen_deal_tickets",
                "emitted_position_ids",
                "drift_delivered_position_ids",
            ):
                if not isinstance(payload.get(key, []), list):
                    raise ValueError(f"state field {key} is not a list")
            for key in ("pending_positions", "entry_intents"):
                if not isinstance(payload.get(key, {}), dict):
                    raise ValueError(f"state field {key} is not an object")
            self._state = self._empty_state()
            self._state.update(payload)
        except Exception:
            self._state_recovered = True
            stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
            backup = self.state_path.with_name(f"{self.state_path.name}.corrupt.{stamp}")
            try:
                self.state_path.replace(backup)
            except OSError:
                # Recovery still proceeds from the append-only ledger.  Failure
                # to quarantine the broken cache must not destroy that evidence.
                pass
            self._state = self._empty_state()

    def _load_ledger_index(self) -> None:
        if not self.ledger_path.exists():
            return
        try:
            with self.ledger_path.open("r", encoding="utf-8") as handle:
                for line_number, raw_line in enumerate(handle, start=1):
                    line = raw_line.strip()
                    if not line:
                        continue
                    record = json.loads(line)
                    if not isinstance(record, dict):
                        raise ValueError(f"line {line_number} is not an object")
                    if record.get("schema_version") != LEDGER_SCHEMA_VERSION:
                        raise ValueError(f"line {line_number} has an unsupported schema")
                    position_key = self._position_key(record.get("position_id"))
                    if not position_key:
                        raise ValueError(f"line {line_number} has no position_id")
                    if position_key in self._ledger_records:
                        raise ValueError(f"duplicate position {position_key} in append-only ledger")
                    self._ledger_records[position_key] = record
        except Exception as exc:
            raise LiveOutcomeError(
                f"live outcome ledger is corrupt; feedback is fail-closed: {self.ledger_path}: {exc}"
            ) from exc

    def _merge_ledger_index_into_state(self) -> None:
        emitted = {str(value) for value in self._state.get("emitted_position_ids", [])}
        seen = {_safe_int(value) for value in self._state.get("seen_deal_tickets", []) if _safe_int(value) > 0}
        for position_key, record in self._ledger_records.items():
            emitted.add(position_key)
            for field_name in ("opening_deal_tickets", "closing_deal_tickets"):
                for ticket in record.get(field_name, []) or []:
                    numeric = _safe_int(ticket)
                    if numeric > 0:
                        seen.add(numeric)
        self._state["emitted_position_ids"] = sorted(emitted, key=int)
        self._state["seen_deal_tickets"] = sorted(seen)
        pending = self._state.get("pending_positions", {})
        for position_key in emitted:
            pending.pop(position_key, None)

    def _save_state(self) -> None:
        self.state_path.parent.mkdir(parents=True, exist_ok=True)
        payload = dict(self._state)
        payload["schema_version"] = STATE_SCHEMA_VERSION
        payload["updated_at"] = _iso_utc(datetime.now(timezone.utc))
        temp_path = self.state_path.with_name(
            f".{self.state_path.name}.tmp.{os.getpid()}.{threading.get_ident()}"
        )
        encoded = json.dumps(payload, indent=2, sort_keys=True, ensure_ascii=False).encode("utf-8")
        try:
            with temp_path.open("wb") as handle:
                handle.write(encoded)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temp_path, self.state_path)
        finally:
            try:
                temp_path.unlink(missing_ok=True)
            except OSError:
                pass

    def _append_record(self, record: Dict[str, Any]) -> None:
        self.ledger_path.parent.mkdir(parents=True, exist_ok=True)
        encoded = (json.dumps(record, sort_keys=True, separators=(",", ":"), ensure_ascii=False) + "\n").encode("utf-8")
        flags = os.O_APPEND | os.O_CREAT | os.O_WRONLY
        if hasattr(os, "O_BINARY"):
            flags |= os.O_BINARY
        descriptor = os.open(str(self.ledger_path), flags, 0o600)
        try:
            view = memoryview(encoded)
            while view:
                written = os.write(descriptor, view)
                if written <= 0:
                    raise OSError("zero-byte append to live outcome ledger")
                view = view[written:]
            os.fsync(descriptor)
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
                existing = intents.get(key)
                if existing is not None and existing != payload:
                    raise LiveOutcomeError(f"conflicting durable entry intent for {key}")
                intents[key] = payload
            self._save_state()

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
            for raw in closing_deals or []:
                if not isinstance(raw, Mapping):
                    continue
                deal = self._normalize_deal(raw, default_exit=True)
                ticket = deal["ticket"]
                position_key = self._position_key(deal["position_id"])
                if ticket <= 0 or not position_key or deal["entry_kind"] not in {"OUT", "OUT_BY"}:
                    result.fail_closed_reasons.append("ignored malformed/non-exit closing deal")
                    continue
                if ticket in seen:
                    result.duplicate_deal_count += 1
                    continue
                seen.add(ticket)
                result.ingested_deal_count += 1
                if position_key in emitted:
                    continue
                pending = pending_positions.setdefault(position_key, {"exit_deals": {}, "last_reason": ""})
                pending.setdefault("exit_deals", {})[str(ticket)] = deal
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
                    result.emitted_records.append(record)
                    self._state["emitted_position_ids"] = sorted(emitted, key=int)
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
