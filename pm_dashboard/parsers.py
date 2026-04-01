from __future__ import annotations

import csv
import copy
import json
import os
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional
import re

from .models import SignalEntry
from .utils import (
    build_entry_id,
    coerce_float,
    derive_price_from_pips,
    direction_from_value,
    extract_field,
    format_timestamp,
    is_recent,
    normalize_regime,
    normalize_symbol,
    normalize_timeframe,
    parse_timestamp,
    pick_action_value,
    pip_size_from_spec,
)


def parse_entries_from_file(
    path: str,
    text: str,
    config: Dict[str, Any],
    instrument_specs: Dict[str, Dict[str, Any]],
    file_mtime: Optional[float] = None,
) -> List[SignalEntry]:
    ext = os.path.splitext(path)[1].lower()
    if ext in (".json",):
        return parse_entries_from_json(path, text, config, instrument_specs, file_mtime)
    if ext in (".csv",):
        return parse_entries_from_csv(path, text, config, instrument_specs, file_mtime)
    if ext in (".log", ".txt"):
        return parse_entries_from_log(path, text, config, instrument_specs, file_mtime)
    return []


def parse_entries_from_json(
    path: str,
    text: str,
    config: Dict[str, Any],
    instrument_specs: Dict[str, Dict[str, Any]],
    file_mtime: Optional[float],
) -> List[SignalEntry]:
    try:
        payload = json.loads(text)
    except json.JSONDecodeError:
        return []

    entries: List[SignalEntry] = []
    if isinstance(payload, list):
        for item in payload:
            entry = normalize_record(item, path, config, instrument_specs, file_mtime)
            if entry:
                entries.append(entry)
        return entries

    if isinstance(payload, dict):
        for key in ("entries", "signals", "recommendations", "data"):
            if isinstance(payload.get(key), list):
                for item in payload[key]:
                    entry = normalize_record(item, path, config, instrument_specs, file_mtime)
                    if entry:
                        entries.append(entry)
                return entries

        for key, value in payload.items():
            if isinstance(value, dict):
                if "symbol" not in value:
                    value = dict(value)
                    value["symbol"] = key
                entry = normalize_record(value, path, config, instrument_specs, file_mtime)
                if entry:
                    entries.append(entry)
        if entries:
            return entries

        entry = normalize_record(payload, path, config, instrument_specs, file_mtime)
        if entry:
            entries.append(entry)
    return entries


def parse_entries_from_csv(
    path: str,
    text: str,
    config: Dict[str, Any],
    instrument_specs: Dict[str, Dict[str, Any]],
    file_mtime: Optional[float],
) -> List[SignalEntry]:
    entries: List[SignalEntry] = []
    reader = csv.DictReader(text.splitlines())
    for row in reader:
        entry = normalize_record(row, path, config, instrument_specs, file_mtime)
        if entry:
            entries.append(entry)
    return entries


def parse_entries_from_log(
    path: str,
    text: str,
    config: Dict[str, Any],
    instrument_specs: Dict[str, Dict[str, Any]],
    file_mtime: Optional[float],
) -> List[SignalEntry]:
    entries: List[SignalEntry] = []
    for line in text.splitlines():
        line = line.strip()
        if "{" not in line or "}" not in line:
            continue
        start = line.find("{")
        end = line.rfind("}")
        if start == -1 or end == -1 or end <= start:
            continue
        snippet = line[start : end + 1]
        try:
            payload = json.loads(snippet)
        except json.JSONDecodeError:
            continue
        entry = normalize_record(payload, path, config, instrument_specs, file_mtime)
        if entry:
            entries.append(entry)

    entries.extend(parse_pm_execution_log(text, path, config, instrument_specs))
    return entries


_RE_TS = re.compile(r"^(?P<ts>\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2})")
_SYMBOL_TOKEN = r"[A-Z0-9_.#-]+"
_NUM_TOKEN = r"[+-]?(?:\d+(?:\.\d+)?|\.\d+)"
_RE_SELECTED = re.compile(
    rf"\[(?P<symbol>{_SYMBOL_TOKEN})\]\s+(?:\[(?P<secondary>SECONDARY)\]\s+)?Selected:\s+"
    r"(?P<strategy>[^@]+)\s+@\s+(?P<tf>[^/]+)/(?P<regime>[A-Z0-9_]+)"
)
_RE_ORDER = re.compile(
    rf"\[(?P<symbol>{_SYMBOL_TOKEN})\]\s+(?P<side>BUY|SELL)\s+\|.*?entry=(?P<entry>{_NUM_TOKEN})\s+\|\s+sl=(?P<sl>{_NUM_TOKEN})\s+\|\s+tp=(?P<tp>{_NUM_TOKEN})",
    re.IGNORECASE,
)
_RE_EXECUTED = re.compile(
    rf"\[OK\]\s+\[(?P<symbol>{_SYMBOL_TOKEN})\]\s+(?P<side>LONG|SHORT)\s+executed.*?@\s+(?P<price>{_NUM_TOKEN})",
    re.IGNORECASE,
)


def parse_pm_execution_log(
    text: str,
    source: str,
    config: Dict[str, Any],
    instrument_specs: Dict[str, Dict[str, Any]],
) -> List[SignalEntry]:
    entries: List[SignalEntry] = []
    context: Dict[str, Dict[str, Any]] = {}

    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue

        ts_match = _RE_TS.search(line)
        ts_val = ts_match.group("ts") if ts_match else ""

        sel = _RE_SELECTED.search(line)
        if sel:
            symbol = normalize_symbol(sel.group("symbol"))
            ctx = context.setdefault(symbol, {})
            ctx["strategy_name"] = sel.group("strategy").strip()
            ctx["timeframe"] = sel.group("tf").strip().upper()
            ctx["regime"] = sel.group("regime").strip().upper()
            ctx["timestamp"] = ts_val
            if sel.group("secondary"):
                ctx["secondary_trade"] = True
                ctx["secondary_reason"] = "log_tag"
            continue

        order = _RE_ORDER.search(line)
        if order:
            symbol = normalize_symbol(order.group("symbol"))
            ctx = context.setdefault(symbol, {})
            ctx["entry_price"] = coerce_float(order.group("entry"))
            ctx["stop_loss_price"] = coerce_float(order.group("sl"))
            ctx["take_profit_price"] = coerce_float(order.group("tp"))
            ctx["signal_direction"] = direction_from_value(order.group("side"))
            ctx["timestamp"] = ts_val
            continue

        executed = _RE_EXECUTED.search(line)
        if executed:
            symbol = normalize_symbol(executed.group("symbol"))
            ctx = context.setdefault(symbol, {})
            side = executed.group("side")
            direction = direction_from_value(side)
            price = coerce_float(executed.group("price"))

            entry = SignalEntry(
                symbol=symbol,
                timeframe=ctx.get("timeframe", ""),
                regime=ctx.get("regime", ""),
                strategy_name=ctx.get("strategy_name", ""),
                signal_direction=ctx.get("signal_direction") or direction,
                entry_price=ctx.get("entry_price") or price,
                stop_loss_price=ctx.get("stop_loss_price"),
                take_profit_price=ctx.get("take_profit_price"),
                signal_strength=None,
                timestamp=format_timestamp(parse_timestamp(ts_val)) if ts_val else None,
                valid_now=True,
                reason="EXECUTED",
                secondary_trade=ctx.get("secondary_trade"),
                secondary_reason=ctx.get("secondary_reason", ""),
                position_context=ctx.get("position_context", {}),
                source=source,
                raw={
                    "symbol": symbol,
                    "action": "EXECUTED",
                    "direction": direction or side,
                    "entry_price": ctx.get("entry_price") or price,
                    "sl": ctx.get("stop_loss_price"),
                    "tp": ctx.get("take_profit_price"),
                    "timeframe": ctx.get("timeframe"),
                    "regime": ctx.get("regime"),
                    "strategy_name": ctx.get("strategy_name"),
                    "secondary_trade": ctx.get("secondary_trade"),
                    "secondary_reason": ctx.get("secondary_reason", ""),
                },
            )
            entry.entry_id = build_entry_id(
                (
                    entry.symbol,
                    entry.timeframe,
                    entry.regime,
                    entry.strategy_name,
                    entry.signal_direction,
                    entry.entry_price,
                    entry.stop_loss_price,
                    entry.take_profit_price,
                    entry.timestamp,
                )
            )
            entries.append(entry)

    return entries


def normalize_record(
    record: Any,
    source: str,
    config: Dict[str, Any],
    instrument_specs: Dict[str, Dict[str, Any]],
    file_mtime: Optional[float],
) -> Optional[SignalEntry]:
    if not isinstance(record, dict):
        return None

    aliases = config.get("field_aliases", {})
    symbol = normalize_symbol(extract_field(record, aliases.get("symbol", [])))
    if not symbol:
        return None
    timeframe = normalize_timeframe(extract_field(record, aliases.get("timeframe", [])))
    regime = normalize_regime(extract_field(record, aliases.get("regime", [])))
    strategy_name = extract_field(record, aliases.get("strategy_name", [])) or ""
    direction_raw = extract_field(record, aliases.get("signal_direction", []))
    direction = direction_from_value(direction_raw)

    entry_price = coerce_float(extract_field(record, aliases.get("entry_price", [])))
    stop_loss = coerce_float(extract_field(record, aliases.get("stop_loss_price", [])))
    take_profit = coerce_float(extract_field(record, aliases.get("take_profit_price", [])))
    strength = coerce_float(extract_field(record, aliases.get("signal_strength", [])))

    timestamp_raw = extract_field(record, aliases.get("timestamp", []))
    timestamp = parse_timestamp(timestamp_raw)
    if timestamp is None and file_mtime:
        timestamp = datetime.fromtimestamp(file_mtime)

    notes = extract_field(record, aliases.get("notes", [])) or ""
    action_value = pick_action_value(record)
    if action_value and not notes:
        notes = action_value

    secondary_trade_raw = extract_field(record, aliases.get("secondary_trade", []))
    secondary_trade: Optional[bool] = None
    if secondary_trade_raw is not None:
        if isinstance(secondary_trade_raw, bool):
            secondary_trade = secondary_trade_raw
        elif isinstance(secondary_trade_raw, (int, float)):
            secondary_trade = bool(secondary_trade_raw)
        elif isinstance(secondary_trade_raw, str):
            lowered = secondary_trade_raw.strip().lower()
            if lowered in ("true", "1", "yes", "y"):
                secondary_trade = True
            elif lowered in ("false", "0", "no", "n"):
                secondary_trade = False

    secondary_reason = extract_field(record, aliases.get("secondary_reason", [])) or ""
    position_context = record.get("position_context")
    if not isinstance(position_context, dict):
        position_context = {}
    else:
        position_context = copy.deepcopy(position_context)

    if entry_price is None:
        entry_price = coerce_float(record.get("entry"))

    spec = instrument_specs.get(symbol)
    pip_size = pip_size_from_spec(spec)

    if stop_loss is None:
        sl_pips = coerce_float(record.get("sl_pips") or record.get("stop_pips") or record.get("stop_loss_pips"))
        if sl_pips is not None and entry_price is not None and pip_size is not None:
            stop_loss = derive_price_from_pips(entry_price, sl_pips, direction, pip_size, "sl")

    if take_profit is None:
        tp_pips = coerce_float(record.get("tp_pips") or record.get("take_profit_pips"))
        if tp_pips is not None and entry_price is not None and pip_size is not None:
            take_profit = derive_price_from_pips(entry_price, tp_pips, direction, pip_size, "tp")

    valid_now = determine_validity(record, direction, strength, action_value, timestamp, config)

    entry = SignalEntry(
        symbol=symbol,
        timeframe=timeframe,
        regime=regime,
        strategy_name=str(strategy_name).strip(),
        signal_direction=direction,
        entry_price=entry_price,
        stop_loss_price=stop_loss,
        take_profit_price=take_profit,
        signal_strength=strength,
        timestamp=format_timestamp(timestamp),
        valid_now=valid_now,
        reason=str(notes).strip(),
        secondary_trade=secondary_trade,
        secondary_reason=str(secondary_reason).strip(),
        position_context=position_context,
        source=source,
        raw=copy.deepcopy(record),
    )
    entry.entry_id = build_entry_id(
        (
            entry.symbol,
            entry.timeframe,
            entry.regime,
            entry.strategy_name,
            entry.signal_direction,
            entry.entry_price,
            entry.stop_loss_price,
            entry.take_profit_price,
            entry.timestamp,
        )
    )
    return entry


def determine_validity(
    record: Dict[str, Any],
    direction: str,
    strength: Optional[float],
    action_value: str,
    timestamp: Optional[datetime],
    config: Dict[str, Any],
) -> bool:
    explicit_valid = extract_field(record, config.get("field_aliases", {}).get("valid_now", []))
    if explicit_valid is not None:
        if isinstance(explicit_valid, str):
            return explicit_valid.strip().lower() in ("true", "1", "yes", "y")
        return bool(explicit_valid)

    exclude_actions = {str(item).upper() for item in config.get("exclude_actions", [])}
    valid_actions = {str(item).upper() for item in config.get("valid_actions", [])}
    valid_prefixes = [str(item).upper() for item in config.get("valid_action_prefixes", [])]

    if action_value:
        if action_value in exclude_actions:
            return False
        if action_value in valid_actions:
            return apply_strength_and_age(strength, timestamp, config)
        if any(action_value.startswith(prefix) for prefix in valid_prefixes if prefix):
            return apply_strength_and_age(strength, timestamp, config)
        return False

    if direction in ("buy", "sell"):
        return apply_strength_and_age(strength, timestamp, config)
    return False


def apply_strength_and_age(
    strength: Optional[float], timestamp: Optional[datetime], config: Dict[str, Any]
) -> bool:
    min_strength = config.get("min_strength")
    if strength is not None and min_strength is not None:
        if strength < float(min_strength):
            return False
    max_age = config.get("max_signal_age_minutes")
    if max_age is not None and not is_recent(timestamp, float(max_age)):
        return False
    return True
