from __future__ import annotations

import fnmatch
import glob
import json
import os
from copy import deepcopy
from datetime import datetime, timedelta
from typing import Any, Dict, Iterable, List, Optional, Tuple


DEFAULT_CONFIG: Dict[str, Any] = {
    "pm_root": "",
    "refresh_interval_sec": 5,
    "file_patterns": [
        "**/*recommendation*.json",
        "**/*recommendations*.json",
        "**/*signal*.json",
        "**/*signals*.json",
        "**/*signal*.csv",
        "**/*signals*.csv",
        "**/*trades*.json",
        "**/*trade*log*.json",
        "**/*last_trade_log*.json",
        "**/*portfolio*.json",
        "**/*.log",
    ],
    "primary_sources": [
        "last_trade_log.json",
    ],
    "log_sources": [
        "logs/*.log",
        "**/pm_*.log",
    ],
    "log_max_files": 2,
    "trade_files_pattern": "**/trades_*.json",
    "pm_configs_path": "pm_configs.json",
    "explicit_files": [],
    "exclude_patterns": [
        "**/pm_dashboard/**",
        "**/__pycache__/**",
        "**/*.pyc",
    ],
    "min_strength": 0.0,
    "max_signal_age_minutes": 1440,
    "valid_actions": [
        "EXECUTED",
    ],
    "valid_action_prefixes": [
        "EXECUTED",
    ],
    "display_actions": [
        "EXECUTED",
        "SKIPPED_RISK_CAP",
    ],
    "exclude_actions": [
        "NO_ACTIONABLE_SIGNAL",
        "NO_ACTIONABLE_SIGNAL_WITHIN_MARGIN",
        "SKIPPED_NO_SIGNAL",
        "FAILED",
    ],
    "display_require_fields": [
        "signal_direction",
        "entry_price",
        "stop_loss_price",
        "take_profit_price",
    ],
    "display_allow_if_actions": [
        "SKIPPED_RISK_CAP",
    ],
    "alert": {
        "enabled": True,
        "sound": True,
        "min_strength": 0.0,
    },
    "field_aliases": {
        "symbol": ["symbol", "pair", "instrument", "ticker"],
        "timeframe": ["timeframe", "tf", "interval", "frame"],
        "regime": ["regime", "market_regime"],
        "strategy_name": ["strategy", "strategy_name", "signal_source", "model", "algo"],
        "signal_direction": ["direction", "side", "signal", "action", "trade_direction"],
        "entry_price": ["entry", "entry_price", "price", "entryPrice", "open_price", "signal_price"],
        "stop_loss_price": ["sl", "stop", "stop_loss", "stop_loss_price"],
        "take_profit_price": ["tp", "take_profit", "take_profit_price"],
        "signal_strength": ["strength", "signal_strength", "score", "confidence", "probability"],
        "timestamp": [
            "timestamp",
            "time",
            "datetime",
            "date",
            "ts",
            "action_time",
            "bar_time",
            "bar_timestamp",
        ],
        "valid_now": ["valid", "is_valid", "actionable", "allow_trade", "allow"],
        "notes": ["reason", "notes", "comment", "message"],
    },
}


def deep_merge(base: Dict[str, Any], updates: Dict[str, Any]) -> Dict[str, Any]:
    result = deepcopy(base)
    for key, value in updates.items():
        if isinstance(value, dict) and isinstance(result.get(key), dict):
            result[key] = deep_merge(result[key], value)
        else:
            result[key] = value
    return result


def load_dashboard_config(path: Optional[str]) -> Dict[str, Any]:
    config = deepcopy(DEFAULT_CONFIG)
    if path and os.path.isfile(path):
        try:
            with open(path, "r", encoding="utf-8") as handle:
                loaded = json.load(handle)
            if isinstance(loaded, dict):
                config = deep_merge(config, loaded)
        except (OSError, json.JSONDecodeError):
            pass
    return config


def save_dashboard_config(path: str, config: Dict[str, Any]) -> None:
    payload = deepcopy(config)
    payload.pop("_runtime", None)
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, sort_keys=True)


def safe_read_text(path: str) -> Optional[str]:
    try:
        stat_before = os.stat(path)
        with open(path, "r", encoding="utf-8", errors="ignore") as handle:
            data = handle.read()
        stat_after = os.stat(path)
        if (stat_before.st_mtime != stat_after.st_mtime) or (stat_before.st_size != stat_after.st_size):
            return None
        return data
    except OSError:
        return None


def iter_candidate_files(
    pm_root: str,
    patterns: Iterable[str],
    explicit_files: Iterable[str],
    exclude_patterns: Iterable[str],
) -> List[str]:
    candidates: List[str] = []
    seen = set()

    def _add(path: str) -> None:
        norm = os.path.normpath(path)
        if norm in seen:
            return
        if not os.path.isfile(norm):
            return
        if is_excluded(norm, pm_root, exclude_patterns):
            return
        seen.add(norm)
        candidates.append(norm)

    if pm_root:
        for pattern in patterns:
            for path in glob.glob(os.path.join(pm_root, pattern), recursive=True):
                _add(path)

    for item in explicit_files:
        if not item:
            continue
        path = item
        if not os.path.isabs(path) and pm_root:
            path = os.path.join(pm_root, path)
        _add(path)

    return candidates


def is_excluded(path: str, pm_root: str, exclude_patterns: Iterable[str]) -> bool:
    if not exclude_patterns:
        return False
    rel = os.path.relpath(path, pm_root) if pm_root else path
    rel = rel.replace("\\", "/")
    for pattern in exclude_patterns:
        if fnmatch.fnmatch(rel, pattern):
            return True
    return False


def extract_field(record: Dict[str, Any], aliases: Iterable[str]) -> Any:
    if not isinstance(record, dict):
        return None
    lower_map = {str(key).lower(): key for key in record.keys()}
    for alias in aliases:
        if alias in record:
            return record[alias]
        alias_lower = str(alias).lower()
        if alias_lower in lower_map:
            return record[lower_map[alias_lower]]
    return None


def normalize_symbol(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip().upper()


def normalize_timeframe(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip().upper()


def normalize_regime(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip().upper()


def coerce_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        cleaned = value.strip()
        if cleaned == "":
            return None
        cleaned = cleaned.replace(",", "")
        if cleaned.endswith("%"):
            cleaned = cleaned[:-1]
        try:
            return float(cleaned)
        except ValueError:
            return None
    return None


def direction_from_value(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        lowered = value.strip().lower()
        if lowered in ("buy", "long", "l", "bull", "up", "1"):
            return "buy"
        if lowered in ("sell", "short", "s", "bear", "down", "-1"):
            return "sell"
        if lowered in ("flat", "none", "neutral", "0", "no", "n/a"):
            return ""
    if isinstance(value, (int, float)):
        if value > 0:
            return "buy"
        if value < 0:
            return "sell"
        return ""
    return ""


def parse_timestamp(value: Any) -> Optional[datetime]:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    if isinstance(value, (int, float)):
        epoch = float(value)
        if epoch > 1e12:
            epoch = epoch / 1000.0
        try:
            return datetime.fromtimestamp(epoch)
        except OSError:
            return None
    if isinstance(value, str):
        raw = value.strip()
        if not raw:
            return None
        raw = raw.replace("Z", "+00:00")
        try:
            return datetime.fromisoformat(raw)
        except ValueError:
            pass
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y/%m/%d %H:%M:%S", "%Y-%m-%d", "%Y/%m/%d"):
            try:
                return datetime.strptime(raw, fmt)
            except ValueError:
                continue
    return None


def format_timestamp(value: Optional[datetime]) -> Optional[str]:
    if value is None:
        return None
    return value.isoformat()


def build_entry_id(parts: Tuple[Any, ...]) -> str:
    safe = ["" if item is None else str(item) for item in parts]
    return "|".join(safe)


def is_recent(timestamp: Optional[datetime], max_age_minutes: Optional[float]) -> bool:
    if timestamp is None or max_age_minutes is None:
        return True
    try:
        age = datetime.now() - timestamp
    except Exception:
        return True
    return age <= timedelta(minutes=max_age_minutes)


def load_instrument_specs(pm_root: str) -> Dict[str, Dict[str, Any]]:
    if not pm_root:
        return {}
    config_path = os.path.join(pm_root, "config.json")
    if not os.path.isfile(config_path):
        return {}
    try:
        with open(config_path, "r", encoding="utf-8") as handle:
            data = json.load(handle)
    except (OSError, json.JSONDecodeError):
        return {}
    specs = data.get("instrument_specs", {})
    defaults = data.get("instrument_spec_defaults", {})
    merged: Dict[str, Dict[str, Any]] = {}
    if isinstance(specs, dict):
        for symbol, spec in specs.items():
            if not isinstance(spec, dict):
                continue
            merged_spec = dict(defaults) if isinstance(defaults, dict) else {}
            merged_spec.update(spec)
            merged[normalize_symbol(symbol)] = merged_spec
    return merged


def load_pm_configs(pm_root: str, path_override: Optional[str] = None) -> Dict[str, Any]:
    if not pm_root:
        return {}
    path = path_override or "pm_configs.json"
    if not os.path.isabs(path):
        path = os.path.join(pm_root, path)
    if not os.path.isfile(path):
        return {}
    try:
        with open(path, "r", encoding="utf-8") as handle:
            return json.load(handle)
    except (OSError, json.JSONDecodeError):
        return {}


def pip_size_from_spec(spec: Optional[Dict[str, Any]]) -> Optional[float]:
    if not spec:
        return None
    pip_pos = spec.get("pip_position")
    if pip_pos is None:
        return None
    try:
        pip_pos_int = int(pip_pos)
        return 10 ** (-pip_pos_int)
    except (TypeError, ValueError):
        return None


def derive_price_from_pips(
    entry_price: float, pips: float, direction: str, pip_size: float, kind: str
) -> Optional[float]:
    if entry_price is None or pips is None or pip_size is None or direction not in ("buy", "sell"):
        return None
    distance = pips * pip_size
    if kind == "sl":
        return entry_price - distance if direction == "buy" else entry_price + distance
    if kind == "tp":
        return entry_price + distance if direction == "buy" else entry_price - distance
    return None


def pick_action_value(record: Dict[str, Any]) -> str:
    action = record.get("action")
    if action is None:
        action = record.get("status")
    if action is None:
        action = record.get("result")
    if action is None:
        return ""
    return str(action).strip().upper()
