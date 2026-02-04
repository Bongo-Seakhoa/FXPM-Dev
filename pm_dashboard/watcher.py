from __future__ import annotations

import glob
import json
import os
import threading
import time
from datetime import datetime
from typing import Any, Dict, List, Optional

from .models import SignalEntry
from .parsers import parse_entries_from_file
from .utils import (
    build_entry_id,
    direction_from_value,
    iter_candidate_files,
    is_recent,
    load_instrument_specs,
    load_pm_configs,
    normalize_symbol,
    parse_timestamp,
    pick_action_value,
    safe_read_text,
)


class DashboardState:
    def __init__(self, instrument_specs: Optional[Dict[str, Dict[str, Any]]] = None) -> None:
        self._lock = threading.Lock()
        self.entries: List[SignalEntry] = []
        self.valid_entries: List[SignalEntry] = []
        self.last_updated: Optional[str] = None
        self.source_files: List[str] = []
        self.instrument_specs: Dict[str, Dict[str, Any]] = instrument_specs or {}
        self.pm_configs: Dict[str, Any] = {}
        self.last_error: str = ""

    def update(
        self,
        entries: List[SignalEntry],
        valid_entries: List[SignalEntry],
        source_files: List[str],
        last_updated: Optional[str],
        instrument_specs: Optional[Dict[str, Dict[str, Any]]] = None,
        pm_configs: Optional[Dict[str, Any]] = None,
        last_error: str = "",
    ) -> None:
        with self._lock:
            self.entries = entries
            self.valid_entries = valid_entries
            self.source_files = source_files
            self.last_updated = last_updated
            if instrument_specs is not None:
                self.instrument_specs = instrument_specs
            if pm_configs is not None:
                self.pm_configs = pm_configs
            self.last_error = last_error

    def snapshot(self) -> Dict[str, Any]:
        with self._lock:
            entries = [entry_to_dict(item) for item in self.entries]
            valid_entries = [entry_to_dict(item) for item in self.valid_entries]
            return {
                "entries": entries,
                "valid_entries": valid_entries,
                "last_updated": self.last_updated,
                "source_files": self.source_files,
                "instrument_specs": self.instrument_specs,
                "stats": {
                    "total": len(entries),
                    "valid": len(valid_entries),
                },
                "last_error": self.last_error,
            }

    def get_pm_configs(self) -> Dict[str, Any]:
        with self._lock:
            return self.pm_configs


def entry_to_dict(entry: SignalEntry) -> Dict[str, Any]:
    return {
        "entry_id": entry.entry_id,
        "symbol": entry.symbol,
        "timeframe": entry.timeframe,
        "regime": entry.regime,
        "strategy_name": entry.strategy_name,
        "signal_direction": entry.signal_direction,
        "entry_price": entry.entry_price,
        "stop_loss_price": entry.stop_loss_price,
        "take_profit_price": entry.take_profit_price,
        "signal_strength": entry.signal_strength,
        "timestamp": entry.timestamp,
        "valid_now": entry.valid_now,
        "reason": entry.reason,
        "source": entry.source,
    }


class DashboardWatcher(threading.Thread):
    def __init__(self, pm_root: str, config: Dict[str, Any], state: DashboardState) -> None:
        super().__init__(daemon=True)
        self.pm_root = pm_root
        self.config = config
        self.state = state
        self._stop_event = threading.Event()
        self._last_alert_set: str = ""
        self._last_alert_strongest: str = ""
        self._pm_configs_mtime: Optional[float] = None
        self._pm_configs_cache: Dict[str, Any] = {}

    def run(self) -> None:
        while not self._stop_event.is_set():
            self.poll_once()
            interval = self.config.get("refresh_interval_sec", 5)
            try:
                interval = max(1.0, float(interval))
            except (TypeError, ValueError):
                interval = 5.0
            self._stop_event.wait(interval)

    def stop(self) -> None:
        self._stop_event.set()

    def update_config(self, config: Dict[str, Any]) -> None:
        self.config = config
        if self.pm_root != config.get("pm_root"):
            self.pm_root = config.get("pm_root") or self.pm_root
            self.state.update(
                self.state.entries,
                self.state.valid_entries,
                self.state.source_files,
                self.state.last_updated,
                instrument_specs=load_instrument_specs(self.pm_root),
                pm_configs=self._load_pm_configs(),
            )

    def poll_once(self) -> None:
        if not self.pm_root:
            return

        pm_configs = self._load_pm_configs()
        patterns = self.config.get("file_patterns", [])
        explicit_files = self.config.get("explicit_files", [])
        exclude_patterns = self.config.get("exclude_patterns", [])
        source_files = iter_candidate_files(self.pm_root, patterns, explicit_files, exclude_patterns)

        entries: List[SignalEntry] = []
        last_error = ""
        try:
            entries = self._load_primary_entries(source_files, pm_configs)
        except Exception as exc:
            last_error = str(exc)

        deduped = dedupe_entries(entries)
        display_entries = [entry for entry in deduped if should_display_entry(entry, self.config)]
        valid_entries = [entry for entry in display_entries if entry.valid_now]
        valid_entries = sorted(valid_entries, key=entry_sort_key, reverse=True)
        last_updated = datetime.now().isoformat()
        self.state.update(
            display_entries,
            valid_entries,
            source_files,
            last_updated,
            instrument_specs=self.state.instrument_specs,
            pm_configs=pm_configs,
            last_error=last_error,
        )
        self.maybe_alert(valid_entries)

    def _load_primary_entries(self, source_files: List[str], pm_configs: Dict[str, Any]) -> List[SignalEntry]:
        primary_patterns = self.config.get("primary_sources") or ["last_trade_log.json"]
        primary_file = find_primary_file(self.pm_root, primary_patterns)
        if primary_file:
            text = safe_read_text(primary_file)
            if text:
                mtime = os.path.getmtime(primary_file) if os.path.exists(primary_file) else None
                entries = parse_entries_from_file(
                    primary_file, text, self.config, self.state.instrument_specs, mtime
                )
                entries = enrich_entries(entries, pm_configs, self._load_trade_map())
                entries.extend(self._load_log_entries())
                entries = normalize_action_flags(entries, self.config)
                return entries

        entries: List[SignalEntry] = []
        for path in source_files:
            try:
                text = safe_read_text(path)
                if text is None:
                    continue
                mtime = None
                try:
                    mtime = os.path.getmtime(path)
                except OSError:
                    pass
                entries.extend(parse_entries_from_file(path, text, self.config, self.state.instrument_specs, mtime))
            except Exception:
                continue
        entries = enrich_entries(entries, pm_configs, self._load_trade_map())
        entries.extend(self._load_log_entries())
        entries = normalize_action_flags(entries, self.config)
        return entries

    def _load_pm_configs(self) -> Dict[str, Any]:
        path = self.config.get("pm_configs_path", "pm_configs.json")
        if not self.pm_root:
            return {}
        if not os.path.isabs(path):
            path = os.path.join(self.pm_root, path)
        if not os.path.isfile(path):
            self._pm_configs_cache = {}
            self._pm_configs_mtime = None
            return {}
        try:
            mtime = os.path.getmtime(path)
        except OSError:
            return self._pm_configs_cache
        if self._pm_configs_mtime != mtime:
            self._pm_configs_cache = load_pm_configs(self.pm_root, path)
            self._pm_configs_mtime = mtime
        return self._pm_configs_cache

    def _load_trade_map(self) -> Dict[str, Dict[str, Any]]:
        pattern = self.config.get("trade_files_pattern", "**/trades_*.json")
        trade_files = []
        if self.pm_root:
            trade_files = [path for path in glob_paths(self.pm_root, pattern)]
        trade_map: Dict[str, Dict[str, Any]] = {}
        for path in trade_files:
            text = safe_read_text(path)
            if not text:
                continue
            try:
                data = json.loads(text)
            except Exception:
                continue
            if not isinstance(data, list):
                continue
            for record in data:
                if not isinstance(record, dict):
                    continue
                symbol = normalize_symbol(record.get("symbol"))
                if not symbol:
                    continue
                timestamp = parse_timestamp(record.get("timestamp"))
                ts_value = timestamp.timestamp() if timestamp else 0.0
                existing = trade_map.get(symbol)
                if existing and existing.get("_ts", 0.0) > ts_value:
                    continue
                trade_map[symbol] = {
                    "_ts": ts_value,
                    "price": record.get("price") or record.get("entry") or record.get("entry_price"),
                    "sl": record.get("sl") or record.get("stop_loss") or record.get("stop_loss_price"),
                    "tp": record.get("tp") or record.get("take_profit") or record.get("take_profit_price"),
                    "direction": record.get("direction"),
                    "timestamp": record.get("timestamp"),
                    "status": record.get("status"),
                }
        return trade_map

    def _load_log_entries(self) -> List[SignalEntry]:
        log_patterns = self.config.get("log_sources") or ["logs/*.log", "**/pm_*.log"]
        max_files = int(self.config.get("log_max_files", 2) or 2)
        files: List[str] = []
        for pattern in log_patterns:
            files.extend(glob_paths(self.pm_root, pattern))
        if not files:
            return []
        files = sorted(set(files), key=lambda p: os.path.getmtime(p) if os.path.exists(p) else 0.0)
        files = files[-max_files:]
        entries: List[SignalEntry] = []
        for path in files:
            text = safe_read_text(path)
            if not text:
                continue
            mtime = os.path.getmtime(path) if os.path.exists(path) else None
            entries.extend(parse_entries_from_file(path, text, self.config, self.state.instrument_specs, mtime))
        return entries

    def maybe_alert(self, valid_entries: List[SignalEntry]) -> None:
        alert_cfg = self.config.get("alert", {})
        if not alert_cfg.get("enabled", True):
            return
        min_strength = alert_cfg.get("min_strength", 0.0)
        filtered = [
            entry for entry in valid_entries if entry.signal_strength is None or entry.signal_strength >= min_strength
        ]
        if not filtered:
            self._last_alert_set = ""
            self._last_alert_strongest = ""
            return

        current_set = "|".join(sorted(entry_alert_key(entry) for entry in filtered))
        strongest = max(filtered, key=entry_sort_key)
        strongest_id = entry_alert_key(strongest)

        if current_set != self._last_alert_set:
            title = "PM Dashboard: Valid entries changed"
            message = f"{len(filtered)} entries available"
            send_desktop_alert(title, message, alert_cfg.get("sound", True))
            self._last_alert_set = current_set

        if strongest_id != self._last_alert_strongest:
            title = "PM Dashboard: Strongest signal"
            direction = strongest.signal_direction.upper() if strongest.signal_direction else "N/A"
            message = f"{strongest.symbol} {direction} ({strongest.timeframe or 'TF?'})"
            send_desktop_alert(title, message, alert_cfg.get("sound", True))
            self._last_alert_strongest = strongest_id


def dedupe_entries(entries: List[SignalEntry]) -> List[SignalEntry]:
    deduped: Dict[str, SignalEntry] = {}
    for entry in entries:
        entry_id = entry.entry_id or build_entry_id(
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
        entry.entry_id = entry_id
        if entry_id not in deduped:
            deduped[entry_id] = entry
            continue
        incumbent = deduped[entry_id]
        if entry_sort_key(entry) > entry_sort_key(incumbent):
            deduped[entry_id] = entry
    return list(deduped.values())


def entry_sort_key(entry: SignalEntry) -> tuple:
    strength = entry.signal_strength or 0.0
    timestamp = entry.timestamp or ""
    return (strength, timestamp)


def entry_alert_key(entry: SignalEntry) -> str:
    return build_entry_id(
        (
            entry.symbol,
            entry.timeframe,
            entry.regime,
            entry.strategy_name,
            entry.signal_direction,
            entry.entry_price,
            entry.stop_loss_price,
            entry.take_profit_price,
            entry.reason,
        )
    )


def should_display_entry(entry: SignalEntry, config: Dict[str, Any]) -> bool:
    action_value = pick_action_value(entry.raw).upper() if entry.raw else ""
    if not action_value and entry.reason:
        action_value = str(entry.reason).upper()

    display_actions = {str(item).upper() for item in config.get("display_actions", [])}
    if display_actions and action_value not in display_actions:
        return False

    required_fields = config.get("display_require_fields") or [
        "signal_direction",
        "entry_price",
        "stop_loss_price",
        "take_profit_price",
    ]
    missing = False
    for field in required_fields:
        value = getattr(entry, field, None)
        if value is None:
            missing = True
            break
        if isinstance(value, str) and value.strip() == "":
            missing = True
            break
    if not missing:
        return True

    allow_actions = {str(item).upper() for item in config.get("display_allow_if_actions", [])}
    return action_value in allow_actions


def send_desktop_alert(title: str, message: str, play_sound: bool) -> None:
    try:
        from plyer import notification

        notification.notify(title=title, message=message, app_name="PM Dashboard", timeout=5)
    except Exception:
        print(f"[PM Dashboard] {title}: {message}")

    if play_sound:
        play_alert_sound()


def play_alert_sound() -> None:
    try:
        import winsound

        winsound.MessageBeep(winsound.MB_ICONASTERISK)
        return
    except Exception:
        pass
    try:
        print("\a", end="", flush=True)
    except Exception:
        pass


def glob_paths(pm_root: str, pattern: str) -> List[str]:
    if not pm_root:
        return []
    return glob.glob(os.path.join(pm_root, pattern), recursive=True)


def find_primary_file(pm_root: str, patterns: List[str]) -> Optional[str]:
    if not pm_root:
        return None
    for pattern in patterns:
        if not pattern:
            continue
        if os.path.isabs(pattern):
            if os.path.isfile(pattern):
                return pattern
            continue
        candidate = os.path.join(pm_root, pattern)
        if os.path.isfile(candidate):
            return candidate
        matches = glob.glob(os.path.join(pm_root, pattern), recursive=True)
        if matches:
            matches.sort()
            return matches[-1]
    return None


def enrich_entries(
    entries: List[SignalEntry],
    pm_configs: Dict[str, Any],
    trade_map: Dict[str, Dict[str, Any]],
) -> List[SignalEntry]:
    for entry in entries:
        symbol = normalize_symbol(entry.symbol)
        if not symbol:
            continue
        entry.symbol = symbol

        action_value = pick_action_value(entry.raw).upper() if entry.raw else ""
        if not action_value and entry.reason:
            action_value = str(entry.reason).upper()

        trade = trade_map.get(symbol)
        if action_value == "EXECUTED" and trade:
            if entry.entry_price is None:
                entry.entry_price = trade.get("price")
            if entry.stop_loss_price is None:
                entry.stop_loss_price = trade.get("sl")
            if entry.take_profit_price is None:
                entry.take_profit_price = trade.get("tp")
            if not entry.signal_direction:
                entry.signal_direction = direction_from_value(trade.get("direction"))
            if not entry.timestamp and trade.get("timestamp"):
                entry.timestamp = str(trade.get("timestamp"))

        if not entry.signal_direction and entry.raw and "direction" in entry.raw:
            entry.signal_direction = direction_from_value(entry.raw.get("direction"))

        entry = enrich_from_pm_configs(entry, pm_configs.get(symbol))
    return entries


def enrich_from_pm_configs(entry: SignalEntry, pm_config: Optional[Dict[str, Any]]) -> SignalEntry:
    if not pm_config:
        return entry
    if not entry.timeframe:
        entry.timeframe = pm_config.get("timeframe", "") or entry.timeframe
    if not entry.strategy_name:
        strategy = ""
        if entry.timeframe and entry.regime:
            strategy = (
                pm_config.get("regime_configs", {})
                .get(entry.timeframe, {})
                .get(entry.regime, {})
                .get("strategy_name", "")
            )
        if not strategy:
            strategy = pm_config.get("strategy_name", "") or pm_config.get("default_config", {}).get("strategy_name", "")
        entry.strategy_name = strategy or entry.strategy_name
    return entry


def normalize_action_flags(entries: List[SignalEntry], config: Dict[str, Any]) -> List[SignalEntry]:
    valid_actions = {str(item).upper() for item in config.get("valid_actions", [])}
    max_age = config.get("max_signal_age_minutes")
    for entry in entries:
        action_value = pick_action_value(entry.raw).upper() if entry.raw else ""
        if not action_value and entry.reason:
            action_value = str(entry.reason).upper()
        is_valid_action = action_value in valid_actions
        if entry.timestamp:
            ts = parse_timestamp(entry.timestamp)
        else:
            ts = None
        entry.valid_now = is_valid_action and is_recent(ts, float(max_age)) if max_age is not None else is_valid_action
        if action_value:
            entry.reason = action_value
    return entries
