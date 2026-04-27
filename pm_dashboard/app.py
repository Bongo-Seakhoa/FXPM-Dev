from __future__ import annotations

import argparse
import copy
import hashlib
import hmac
import ipaddress
import json
import logging
import os
from datetime import datetime
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse

from flask import Flask, jsonify, render_template, request
from werkzeug.exceptions import HTTPException

from .utils import (
    DEFAULT_CONFIG,
    coerce_float,
    deep_merge,
    load_dashboard_config,
    resolve_pm_configs_path,
    save_dashboard_config,
)
from .watcher import DashboardState, DashboardWatcher
from .utils import parse_timestamp
from .utils import load_instrument_specs
from .analytics import (
    build_analytics_payload,
    load_trade_history,
    reconstruct_trade_outcomes,
    compute_equity_curve,
    compute_drawdown_curve,
    compute_performance_metrics
)

logger = logging.getLogger(__name__)
BASE_DIR = os.path.dirname(__file__)

# Try to import data jobs (optional dependency)
try:
    from .jobs import initialize_data_jobs
    JOBS_AVAILABLE = True
except ImportError:
    JOBS_AVAILABLE = False
    logger.warning("Data jobs module not available - simulation features disabled")

# Try to import MT5 connector
try:
    from pm_mt5 import MT5Connector, MT5_AVAILABLE
except ImportError:
    MT5_AVAILABLE = False
    MT5Connector = None


def resolve_pm_root(pm_root_value: Optional[str], base_dir: str) -> str:
    """
    Resolve dashboard pm_root to a valid directory.

    Fallback order:
    1) Valid configured/updated path (absolute or project-relative)
    2) Project root (parent of pm_dashboard package)
    """
    project_root = os.path.abspath(os.path.join(base_dir, os.pardir))
    raw = str(pm_root_value or "").strip()

    if raw:
        candidate = raw
        if not os.path.isabs(candidate):
            candidate = os.path.join(project_root, candidate)
        candidate = os.path.abspath(os.path.expanduser(candidate))
        if os.path.isdir(candidate):
            return candidate
        logger.warning("Invalid dashboard pm_root '%s'; falling back to '%s'", raw, project_root)
    return project_root


def _stop_scheduler_if_running(app: Flask) -> None:
    scheduler = app.config.get("data_scheduler")
    if scheduler and hasattr(scheduler, "stop"):
        try:
            scheduler.stop()
        except Exception as exc:
            logger.warning("Failed to stop dashboard data scheduler: %s", exc)


def _setup_runtime_services(
    app: Flask,
    config: Dict[str, Any],
    base_dir: str,
    start_background_workers: bool,
) -> None:
    _stop_scheduler_if_running(app)

    mt5_connector = None
    data_downloader = None
    data_scheduler = None

    if JOBS_AVAILABLE and start_background_workers:
        scheduler_enabled = bool(config.get("enable_data_maintenance_scheduler", True))
        scheduler_time = str(config.get("data_maintenance_time", "00:00") or "00:00")
        pm_root = config.get("pm_root") or resolve_pm_root(config.get("pm_root"), base_dir)

        if MT5_AVAILABLE:
            try:
                mt5_connector = MT5Connector()
                if mt5_connector.connect():
                    logger.info("MT5 connector initialized for root data maintenance")
                else:
                    logger.warning("MT5 initial connect failed; local-data simulation remains available")
            except Exception as exc:
                logger.error("Failed to initialize MT5 connector: %s", exc)
                mt5_connector = None

        try:
            data_downloader, data_scheduler = initialize_data_jobs(
                pm_root,
                mt5_connector=mt5_connector,
                enable_scheduler=scheduler_enabled and start_background_workers,
                run_time=scheduler_time,
            )
            logger.info(
                "Dashboard data jobs initialized (scheduler=%s @ %s)",
                "enabled" if scheduler_enabled and start_background_workers else "disabled",
                scheduler_time,
            )
        except Exception as exc:
            logger.error("Failed to initialize data jobs: %s", exc)

    app.config["mt5_connector"] = mt5_connector
    app.config["data_downloader"] = data_downloader
    app.config["data_scheduler"] = data_scheduler


def _build_asset_version(static_root: str) -> str:
    digest = hashlib.sha1()
    if static_root and os.path.isdir(static_root):
        for name in sorted(os.listdir(static_root)):
            path = os.path.join(static_root, name)
            if not os.path.isfile(path):
                continue
            try:
                stat = os.stat(path)
                digest.update(f"{name}:{int(stat.st_mtime_ns)}:{int(stat.st_size)}".encode("utf-8"))
            except OSError:
                continue
    return digest.hexdigest()[:12] or "0"


def _json_error(message: str, status_code: int = 400) -> Any:
    return jsonify({"success": False, "error": message}), status_code


def _configured_write_token(config: Dict[str, Any]) -> str:
    env_name = str(config.get("write_api_token_env") or "PM_DASHBOARD_WRITE_TOKEN").strip()
    if not env_name:
        return ""
    return os.environ.get(env_name, "")


def _request_write_token() -> str:
    token = str(request.headers.get("X-PM-Dashboard-Token") or "").strip()
    if token:
        return token
    auth = str(request.headers.get("Authorization") or "").strip()
    prefix = "bearer "
    if auth.lower().startswith(prefix):
        return auth[len(prefix):].strip()
    return ""


def _is_loopback_request() -> bool:
    remote_addr = request.remote_addr or ""
    try:
        return ipaddress.ip_address(remote_addr).is_loopback
    except ValueError:
        return remote_addr.lower() in {"localhost", "127.0.0.1", "::1"}


def _is_same_origin_request() -> bool:
    origin = str(request.headers.get("Origin") or "").strip()
    referer = str(request.headers.get("Referer") or "").strip()
    source = origin or referer
    if not source:
        return True
    try:
        source_url = urlparse(source)
        host_url = urlparse(request.host_url)
    except Exception:
        return False
    return (
        source_url.scheme == host_url.scheme
        and source_url.netloc.lower() == host_url.netloc.lower()
    )


def _authorize_write_api(config: Dict[str, Any]) -> Optional[Any]:
    expected_token = _configured_write_token(config)
    if expected_token:
        supplied_token = _request_write_token()
        if supplied_token and hmac.compare_digest(supplied_token, expected_token):
            return None
        return _json_error("Dashboard write token required", 401)

    if _is_loopback_request() and _is_same_origin_request():
        return None
    if _is_loopback_request():
        return _json_error("Cross-origin dashboard writes are disabled", 403)

    return _json_error(
        "Remote dashboard writes are disabled. Set PM_DASHBOARD_WRITE_TOKEN and send it as "
        "X-PM-Dashboard-Token or Authorization: Bearer <token>.",
        403,
    )


def _coerce_bool(value: Any, field_name: str) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        lowered = value.strip().lower()
        if lowered in {"1", "true", "yes", "y", "on"}:
            return True
        if lowered in {"0", "false", "no", "n", "off"}:
            return False
    raise ValueError(f"{field_name} must be a boolean")


def _coerce_float(value: Any, field_name: str, minimum: Optional[float] = None) -> float:
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        raise ValueError(f"{field_name} must be numeric")
    if minimum is not None and numeric < minimum:
        raise ValueError(f"{field_name} must be >= {minimum}")
    return numeric


def _coerce_int(value: Any, field_name: str, minimum: Optional[int] = None) -> int:
    try:
        numeric = int(value)
    except (TypeError, ValueError):
        raise ValueError(f"{field_name} must be an integer")
    if minimum is not None and numeric < minimum:
        raise ValueError(f"{field_name} must be >= {minimum}")
    return numeric


def _coerce_string_list(value: Any, field_name: str) -> list[str]:
    if not isinstance(value, list):
        raise ValueError(f"{field_name} must be a list")
    normalized = []
    for item in value:
        text = str(item or "").strip()
        if text:
            normalized.append(text)
    return normalized


def _load_root_config(pm_root: str) -> Dict[str, Any]:
    if not pm_root:
        return {}
    config_path = os.path.join(pm_root, "config.json")
    if not os.path.isfile(config_path):
        return {}
    try:
        with open(config_path, "r", encoding="utf-8") as handle:
            payload = json.load(handle)
        return payload if isinstance(payload, dict) else {}
    except (OSError, json.JSONDecodeError):
        return {}


def _configured_symbols(root_config: Dict[str, Any], pm_configs: Dict[str, Any]) -> List[str]:
    raw_symbols = root_config.get("symbols")
    if isinstance(raw_symbols, list) and raw_symbols:
        return sorted({str(symbol).strip().upper() for symbol in raw_symbols if str(symbol).strip()})

    specs = root_config.get("instrument_specs")
    if isinstance(specs, dict) and specs:
        return sorted({str(symbol).strip().upper() for symbol in specs.keys() if str(symbol).strip()})

    return sorted({str(symbol).strip().upper() for symbol in pm_configs.keys() if str(symbol).strip()})


def _parse_dt(value: Any) -> Optional[datetime]:
    return parse_timestamp(value)


def _max_timestamp(values: List[Any]) -> Optional[str]:
    parsed = [_parse_dt(value) for value in values if value]
    parsed = [value for value in parsed if value is not None]
    if not parsed:
        return None

    def _rank(value: datetime) -> float:
        try:
            return value.timestamp()
        except Exception:
            return 0.0

    return max(parsed, key=_rank).isoformat()


def _entry_action(entry: Dict[str, Any]) -> str:
    return str(entry.get("action") or entry.get("status") or entry.get("reason") or "").strip().upper()


def _entry_timestamp(entry: Dict[str, Any]) -> Optional[datetime]:
    return _parse_dt(entry.get("timestamp"))


def _risk_action_group(action: str) -> str:
    action = str(action or "").upper()
    if action.startswith("SKIPPED_MARGIN_"):
        return "margin"
    if "RISK_CAP" in action or action.startswith("BLOCKED_MIN_LOT"):
        return "risk"
    if action.startswith("BLOCKED_SPREAD"):
        return "spread"
    if action.startswith("BLOCKED_POSITION"):
        return "position"
    if action.startswith("FAILED"):
        return "failed"
    if action == "EXECUTED":
        return "executed"
    return "other"


def _ledger_status_from_pm_configs(
    root_config: Dict[str, Any],
    pm_configs: Dict[str, Any],
) -> Dict[str, Any]:
    configured = _configured_symbols(root_config, pm_configs)
    configured_set = set(configured)
    optimized = sorted({str(symbol).strip().upper() for symbol in pm_configs.keys() if str(symbol).strip()})
    optimized_set = set(optimized)

    now = datetime.now()
    symbols_validated = 0
    symbols_expired = 0
    symbols_invalid = 0
    regime_slots = 0
    tradeable_winners = 0
    no_trade_slots = 0
    validation_status_counts: Dict[str, int] = {}
    artifact_contract_counts: Dict[str, int] = {}
    optimized_at_values: List[Any] = []
    valid_until_values: List[Any] = []

    for cfg in pm_configs.values():
        if not isinstance(cfg, dict):
            continue
        if cfg.get("is_validated", True):
            symbols_validated += 1
        else:
            symbols_invalid += 1

        valid_until = cfg.get("valid_until")
        if valid_until:
            valid_until_values.append(valid_until)
            valid_until_dt = _parse_dt(valid_until)
            if valid_until_dt is not None:
                probe_now = datetime.now(valid_until_dt.tzinfo) if valid_until_dt.tzinfo else now
                if valid_until_dt < probe_now:
                    symbols_expired += 1

        if cfg.get("optimized_at"):
            optimized_at_values.append(cfg.get("optimized_at"))

        artifact = cfg.get("artifact_meta", {}) if isinstance(cfg.get("artifact_meta"), dict) else {}
        contract_key = "|".join([
            str(artifact.get("artifact_version", "missing")),
            str(artifact.get("split_contract_version", "missing")),
        ])
        artifact_contract_counts[contract_key] = artifact_contract_counts.get(contract_key, 0) + 1

        regime_configs = cfg.get("regime_configs", {})
        if isinstance(regime_configs, dict) and regime_configs:
            for regimes in regime_configs.values():
                if not isinstance(regimes, dict):
                    continue
                for reg_cfg in regimes.values():
                    if not isinstance(reg_cfg, dict):
                        continue
                    regime_slots += 1
                    status = str(reg_cfg.get("validation_status") or "validated").strip().lower()
                    validation_status_counts[status] = validation_status_counts.get(status, 0) + 1
                    strategy_name = str(reg_cfg.get("strategy_name") or "").strip().upper()
                    if strategy_name == "NO_TRADE" or bool((reg_cfg.get("artifact_meta") or {}).get("no_trade")):
                        no_trade_slots += 1
                    elif status != "invalid":
                        tradeable_winners += 1
        else:
            default_cfg = cfg.get("default_config", {}) if isinstance(cfg.get("default_config"), dict) else {}
            if default_cfg:
                regime_slots += 1
                strategy_name = str(default_cfg.get("strategy_name") or cfg.get("strategy_name") or "").strip().upper()
                if strategy_name == "NO_TRADE":
                    no_trade_slots += 1
                else:
                    tradeable_winners += 1

    missing = sorted(configured_set - optimized_set)
    extra = sorted(optimized_set - configured_set) if configured_set else []
    configured_count = len(configured)
    optimized_count = len(optimized)
    coverage_pct = (optimized_count / configured_count * 100.0) if configured_count else 0.0

    return {
        "configured_symbol_count": configured_count,
        "optimized_symbol_count": optimized_count,
        "missing_symbol_count": len(missing),
        "missing_symbols": missing[:20],
        "extra_symbols": extra[:20],
        "complete_universe": bool(configured_set) and configured_set.issubset(optimized_set),
        "coverage_pct": round(coverage_pct, 2),
        "symbols_validated": symbols_validated,
        "symbols_invalid": symbols_invalid,
        "symbols_expired": symbols_expired,
        "regime_slots": regime_slots,
        "tradeable_winners": tradeable_winners,
        "no_trade_slots": no_trade_slots,
        "validation_status_counts": validation_status_counts,
        "artifact_contract_counts": artifact_contract_counts,
        "latest_optimized_at": _max_timestamp(optimized_at_values),
        "latest_valid_until": _max_timestamp(valid_until_values),
    }


def _signal_status_from_snapshot(snapshot: Dict[str, Any]) -> Dict[str, Any]:
    entries = snapshot.get("entries") or []
    valid_entries = snapshot.get("valid_entries") or []
    action_counts: Dict[str, int] = {}
    action_groups: Dict[str, int] = {}
    direction_counts = {"buy": 0, "sell": 0, "other": 0}
    newest_ts: Optional[datetime] = None

    for entry in entries:
        if not isinstance(entry, dict):
            continue
        action = _entry_action(entry) or "UNKNOWN"
        action_counts[action] = action_counts.get(action, 0) + 1
        group = _risk_action_group(action)
        action_groups[group] = action_groups.get(group, 0) + 1
        ts = _entry_timestamp(entry)
        if ts is not None and (newest_ts is None or ts > newest_ts):
            newest_ts = ts

    for entry in valid_entries:
        direction = str(entry.get("signal_direction") or "").lower()
        if direction in direction_counts:
            direction_counts[direction] += 1
        else:
            direction_counts["other"] += 1

    age_minutes = None
    if newest_ts is not None:
        try:
            probe = datetime.now(newest_ts.tzinfo) if newest_ts.tzinfo else datetime.now()
            age_minutes = max((probe - newest_ts).total_seconds() / 60.0, 0.0)
        except Exception:
            age_minutes = None

    top_actions = sorted(action_counts.items(), key=lambda item: (-item[1], item[0]))[:8]
    return {
        "total_entries": len(entries),
        "valid_entries": len(valid_entries),
        "source_file_count": len(snapshot.get("source_files") or []),
        "last_updated": snapshot.get("last_updated"),
        "newest_signal_at": newest_ts.isoformat() if newest_ts else None,
        "newest_signal_age_minutes": None if age_minutes is None else round(age_minutes, 2),
        "direction_counts": direction_counts,
        "action_counts": dict(top_actions),
        "action_groups": action_groups,
        "last_error": snapshot.get("last_error") or "",
    }


def _trade_status(pm_root: str, pm_configs_path: str, initial_capital: float) -> Dict[str, Any]:
    trades = load_trade_history(pm_root, max_files=100, pm_configs_path=pm_configs_path)
    realized = [trade for trade in trades if bool(trade.get("realized"))]
    open_events = [trade for trade in trades if not bool(trade.get("realized"))]
    realized_pnl = 0.0
    latest_event_at = None
    latest_realized_at = None
    for trade in trades:
        ts = trade.get("_sort_timestamp") or trade.get("_parsed_timestamp") or _parse_dt(trade.get("timestamp"))
        if ts is not None and (latest_event_at is None or ts > latest_event_at):
            latest_event_at = ts
    for trade in realized:
        try:
            realized_pnl += float(trade.get("pnl", 0.0) or 0.0)
        except (TypeError, ValueError):
            continue
        ts = trade.get("_sort_timestamp") or trade.get("_parsed_timestamp") or _parse_dt(trade.get("timestamp"))
        if ts is not None and (latest_realized_at is None or ts > latest_realized_at):
            latest_realized_at = ts
    return {
        "total_events": len(trades),
        "realized_events": len(realized),
        "open_or_entry_events": len(open_events),
        "realized_pnl": round(realized_pnl, 2),
        "equity_estimate": round(initial_capital + realized_pnl, 2),
        "latest_event_at": latest_event_at.isoformat() if latest_event_at else None,
        "latest_realized_at": latest_realized_at.isoformat() if latest_realized_at else None,
        "analytics_backed_by_realized_trades": len(realized) > 0,
    }


def build_live_command_payload(
    pm_root: str,
    dashboard_config: Dict[str, Any],
    snapshot: Dict[str, Any],
    pm_configs: Dict[str, Any],
) -> Dict[str, Any]:
    root_config = _load_root_config(pm_root)
    pipeline = root_config.get("pipeline", {}) if isinstance(root_config.get("pipeline"), dict) else {}
    pm_configs_path = resolve_pm_configs_path(pm_root, dashboard_config)
    initial_capital = 10000.0
    try:
        initial_capital = float(pipeline.get("initial_capital", initial_capital))
    except (TypeError, ValueError):
        initial_capital = 10000.0

    ledger = _ledger_status_from_pm_configs(root_config, pm_configs)
    signals = _signal_status_from_snapshot(snapshot)
    trades = _trade_status(pm_root, pm_configs_path, initial_capital)

    blockers: List[str] = []
    warnings: List[str] = []
    score = 100
    if ledger["configured_symbol_count"] and not ledger["complete_universe"]:
        score -= 22
        warnings.append(f"{ledger['missing_symbol_count']} configured symbols are not yet in the active ledger")
    if ledger["tradeable_winners"] <= 0:
        score -= 35
        blockers.append("No tradeable winners found in the active ledger")
    if ledger["symbols_expired"] > 0:
        score -= 20
        blockers.append(f"{ledger['symbols_expired']} symbol configs are expired")
    if signals["last_error"]:
        score -= 15
        warnings.append("Watcher reported a parsing/loading warning")
    if signals["total_entries"] <= 0:
        score -= 10
        warnings.append("No signal/action entries are currently visible")
    if not trades["analytics_backed_by_realized_trades"] and trades["total_events"] > 0:
        warnings.append("Trade analytics are currently entry-event based; no realized close outcomes found")

    score = max(0, min(100, score))
    if blockers:
        score = min(score, 55)
        tone = "danger"
        label = "Blocked"
    elif score < 75:
        tone = "warning"
        label = "Needs Review"
    else:
        tone = "good"
        label = "Operational"

    return {
        "generated_at": datetime.now().isoformat(),
        "pm_root": pm_root,
        "active_pm_configs_path": pm_configs_path,
        "readiness": {
            "score": score,
            "tone": tone,
            "label": label,
            "blockers": blockers,
            "warnings": warnings,
        },
        "ledger": ledger,
        "signals": signals,
        "trades": trades,
        "telegram": {
            "enabled": bool((dashboard_config.get("telegram") or {}).get("enabled", False)),
            "chat_id_configured": bool(str((dashboard_config.get("telegram") or {}).get("chat_id") or "").strip()),
            "token_env": str((dashboard_config.get("telegram") or {}).get("bot_token_env") or "PM_DASHBOARD_TELEGRAM_BOT_TOKEN"),
            "token_configured": bool(os.environ.get(str((dashboard_config.get("telegram") or {}).get("bot_token_env") or "PM_DASHBOARD_TELEGRAM_BOT_TOKEN"))),
        },
    }


def create_app(
    config_path: str,
    pm_root_override: Optional[str] = None,
    start_background_workers: bool = True,
) -> Flask:
    base_dir = os.path.dirname(__file__)
    config = load_dashboard_config(config_path)
    if pm_root_override:
        config["pm_root"] = pm_root_override

    pm_root = resolve_pm_root(config.get("pm_root"), base_dir)
    config["pm_root"] = pm_root
    instrument_specs = load_instrument_specs(pm_root)
    state = DashboardState(instrument_specs)
    watcher = DashboardWatcher(pm_root, config, state)
    try:
        # Prime snapshot synchronously so the dashboard is populated at first load.
        watcher.poll_once()
    except Exception as exc:
        logger.warning("Initial dashboard poll failed: %s", exc)
    if start_background_workers:
        watcher.start()

    app = Flask(__name__, static_folder="static", template_folder="templates")
    app.config["asset_version"] = _build_asset_version(app.static_folder or "")
    app.config["dashboard_state"] = state
    app.config["dashboard_watcher"] = watcher
    app.config["dashboard_config"] = config
    app.config["dashboard_config_path"] = config_path
    app.config["start_background_workers"] = start_background_workers

    if JOBS_AVAILABLE and start_background_workers:
        _setup_runtime_services(app, config, base_dir, start_background_workers)
    else:
        app.config["mt5_connector"] = None
        app.config["data_downloader"] = None
        app.config["data_scheduler"] = None

    if not JOBS_AVAILABLE:
        logger.info("Dashboard jobs module unavailable - simulation features will be limited")

    @app.context_processor
    def inject_asset_version() -> Dict[str, Any]:
        return {"asset_version": app.config.get("asset_version", "0")}

    @app.errorhandler(HTTPException)
    def handle_http_exception(exc: HTTPException) -> Any:
        if request.path.startswith("/api/"):
            return _json_error(exc.description or exc.name, exc.code or 500)
        return exc

    @app.errorhandler(Exception)
    def handle_unexpected_exception(exc: Exception) -> Any:
        logger.exception("Unhandled dashboard error")
        if request.path.startswith("/api/"):
            return _json_error("Internal server error", 500)
        return "Internal server error", 500

    if not JOBS_AVAILABLE:
        logger.info("Dashboard jobs module unavailable - simulation features will be limited")

    @app.route("/")
    def index() -> str:
        return render_template("index.html")

    @app.route("/strategies")
    def strategies() -> str:
        return render_template("strategies.html")

    @app.route("/analytics")
    def analytics() -> str:
        return render_template("analytics.html")

    @app.route("/trades")
    def trades() -> str:
        return render_template("trades.html")

    @app.route("/api/entries", methods=["GET"])
    def api_entries() -> Any:
        current_config = app.config["dashboard_config"]
        snapshot = state.snapshot()
        snapshot["config"] = {
            "pm_root": current_config.get("pm_root"),
            "refresh_interval_sec": current_config.get("refresh_interval_sec"),
        }
        return jsonify(snapshot)

    @app.route("/api/live-command", methods=["GET"])
    def api_live_command() -> Any:
        current_config = app.config["dashboard_config"]
        snapshot = state.snapshot()
        pm_configs = state.get_pm_configs()
        pm_root = current_config.get("pm_root") or ""
        payload = build_live_command_payload(pm_root, current_config, snapshot, pm_configs)
        return jsonify(payload)

    @app.route("/api/config", methods=["GET", "POST"])
    def api_config() -> Any:
        current_config = app.config["dashboard_config"]
        if request.method == "GET":
            return jsonify(serialize_config(current_config))

        auth_error = _authorize_write_api(current_config)
        if auth_error is not None:
            return auth_error

        payload = request.get_json(silent=True)
        if payload is None and request.data:
            return _json_error("Invalid JSON payload", 400)
        payload = payload or {}
        if not isinstance(payload, dict):
            return _json_error("Config payload must be a JSON object", 400)
        original_config = copy.deepcopy(current_config)
        try:
            updated = apply_config_updates(original_config, payload)
        except ValueError as exc:
            return _json_error(str(exc), 400)
        updated["pm_root"] = resolve_pm_root(updated.get("pm_root"), base_dir)
        runtime_keys = ("pm_root", "enable_data_maintenance_scheduler", "data_maintenance_time")
        runtime_changed = any(updated.get(key) != original_config.get(key) for key in runtime_keys)
        try:
            save_dashboard_config(config_path, updated)
        except Exception as exc:
            logger.error("Failed to persist dashboard config: %s", exc)
            return _json_error("Failed to persist dashboard config", 500)

        app.config["dashboard_config"] = copy.deepcopy(updated)
        if runtime_changed:
            new_pm_root = resolve_pm_root(updated.get("pm_root"), base_dir)
            updated["pm_root"] = new_pm_root
            state.set_instrument_specs(load_instrument_specs(new_pm_root))
            if JOBS_AVAILABLE and app.config.get("start_background_workers", True):
                _setup_runtime_services(app, updated, base_dir, True)
        app.config["dashboard_watcher"].update_config(updated)
        return jsonify(serialize_config(updated))

    @app.route("/api/strategies", methods=["GET"])
    def api_strategies() -> Any:
        include_invalid = request.args.get("include_invalid", "false").lower() in ("1", "true", "yes", "y")
        pm_configs = state.get_pm_configs()
        payload = build_strategy_payload(pm_configs, include_invalid)
        return jsonify(payload)

    @app.route("/api/analytics", methods=["GET"])
    def api_analytics() -> Any:
        current_config = app.config["dashboard_config"]
        pm_root = current_config.get("pm_root") or ""
        pm_configs_path = resolve_pm_configs_path(pm_root, current_config)
        try:
            initial_capital = float(request.args.get("initial_capital", 10000.0))
        except (ValueError, TypeError):
            initial_capital = 10000.0
        payload = build_analytics_payload(pm_root, initial_capital=initial_capital, pm_configs_path=pm_configs_path)
        return jsonify(payload)

    @app.route("/api/trades", methods=["GET"])
    def api_trades() -> Any:
        current_config = app.config["dashboard_config"]
        pm_root = current_config.get("pm_root") or ""
        pm_configs_path = resolve_pm_configs_path(pm_root, current_config)
        try:
            limit = int(request.args.get("limit", 200))
        except (ValueError, TypeError):
            limit = 200
        trades = load_trade_history(pm_root, max_files=100, pm_configs_path=pm_configs_path)

        filtered_trades = []
        for trade in trades[:limit]:
            sort_ts = trade.get("_sort_timestamp")
            if hasattr(sort_ts, "isoformat"):
                trade_timestamp = sort_ts.isoformat()
            else:
                trade_timestamp = trade.get("timestamp")
            filtered_trades.append({
                "timestamp": trade_timestamp,
                "symbol": trade.get("symbol"),
                "direction": trade.get("direction"),
                "volume": trade.get("volume"),
                "price": trade.get("price"),
                "sl": trade.get("sl"),
                "tp": trade.get("tp"),
                "pnl": trade.get("pnl", 0.0) or trade.get("profit", 0.0) or 0.0,
                "status": trade.get("status"),
                "timeframe": trade.get("timeframe"),
                "regime": trade.get("regime"),
                "strategy": trade.get("strategy") or trade.get("strategy_name"),
                "magic": trade.get("magic"),
                "realized": bool(trade.get("realized")),
            })

        return jsonify({"trades": filtered_trades, "total": len(trades)})

    @app.route("/api/simulate", methods=["POST"])
    def api_simulate() -> Any:
        """
        Simulate trade outcomes with historical data reconstruction.

        Request body:
            - initial_capital: Starting capital (default: 10000)
            - start_date: Start date for simulation (ISO format)
            - end_date: End date for simulation (ISO format)
            - return_basis: "dollar", "pip", or "trade" (default: "dollar")
            - max_trades: Max trades to simulate (default: 1000)
        """
        current_config = app.config["dashboard_config"]
        pm_root = current_config.get("pm_root") or ""
        pm_configs_path = resolve_pm_configs_path(pm_root, current_config)
        data_downloader = app.config.get("data_downloader")

        payload = request.get_json(silent=True)
        if payload is None and request.data:
            return jsonify({
                "success": False,
                "error": "Invalid JSON payload",
                "trades": [],
                "metrics": {},
                "equity_curve": [],
                "drawdown_curve": [],
            }), 400
        payload = payload or {}

        try:
            initial_capital = float(payload.get("initial_capital", 10000.0))
        except (TypeError, ValueError):
            initial_capital = 10000.0
        start_date_str = payload.get("start_date")
        end_date_str = payload.get("end_date")
        return_basis = str(payload.get("return_basis", "dollar") or "dollar").strip().lower()
        try:
            max_trades = max(1, int(payload.get("max_trades", 1000)))
        except (TypeError, ValueError):
            max_trades = 1000

        # Parse dates
        start_date = parse_timestamp(start_date_str) if start_date_str else None
        end_date = parse_timestamp(end_date_str) if end_date_str else None
        if start_date and end_date and start_date > end_date:
            return jsonify({
                "success": False,
                "error": "start_date must be earlier than end_date",
                "trades": [],
                "metrics": {},
                "equity_curve": [],
                "drawdown_curve": [],
            }), 400

        # Load trades
        all_trades = load_trade_history(pm_root, max_files=100, pm_configs_path=pm_configs_path)

        if not all_trades:
            return jsonify({
                "success": False,
                "error": "No trade data available",
                "trades": [],
                "metrics": {},
                "equity_curve": [],
                "drawdown_curve": []
            })

        # Filter by date range
        filtered_trades = []
        for trade in all_trades:
            ts = trade.get("_parsed_timestamp")
            if not ts:
                continue
            if start_date and ts < start_date:
                continue
            if end_date and ts > end_date:
                continue
            filtered_trades.append(trade)

        # Check if historical-data loader is available
        if not data_downloader:
            logger.warning("Local historical-data loader unavailable - returning existing trade data")
            # Use existing PnL data if available
            metrics = compute_performance_metrics(filtered_trades[:max_trades], initial_capital)
            equity_curve = compute_equity_curve(filtered_trades[:max_trades], initial_capital)
            drawdown_curve = compute_drawdown_curve(equity_curve)

            return jsonify({
                "success": True,
                "simulated": False,
                "message": "Using existing trade data (historical-data loader unavailable)",
                "trades": filtered_trades[:50],  # Return first 50 for display
                "metrics": metrics,
                "equity_curve": equity_curve,
                "drawdown_curve": drawdown_curve,
                "total_trades": len(filtered_trades)
            })

        # Define data loader function for reconstruction
        def load_historical_data(symbol, timeframe, start, end):
            return data_downloader.load_historical_data(symbol, timeframe, start, end)

        instrument_specs = load_instrument_specs(pm_root)

        # Reconstruct trade outcomes
        logger.info("Reconstructing %d trade outcomes...", len(filtered_trades))
        reconstructed_trades = reconstruct_trade_outcomes(
            filtered_trades,
            load_historical_data,
            max_trades=max_trades,
            instrument_specs=instrument_specs,
        )

        if not reconstructed_trades:
            return jsonify({
                "success": False,
                "error": "Failed to reconstruct any trades (missing historical data)",
                "trades": [],
                "metrics": {},
                "equity_curve": [],
                "drawdown_curve": []
            })

        # Calculate metrics based on return_basis
        if return_basis == "pip":
            # Convert PnL to pips
            for trade in reconstructed_trades:
                trade["pnl"] = trade.get("pnl_pips", 0)
        elif return_basis == "trade":
            # Binary: +1 for win, -1 for loss
            for trade in reconstructed_trades:
                pnl = trade.get("pnl", 0)
                trade["pnl"] = 1 if pnl > 0 else (-1 if pnl < 0 else 0)

        # Compute metrics
        metrics = compute_performance_metrics(reconstructed_trades, initial_capital)
        equity_curve = compute_equity_curve(reconstructed_trades, initial_capital)
        drawdown_curve = compute_drawdown_curve(equity_curve)

        return jsonify({
            "success": True,
            "simulated": True,
            "message": f"Reconstructed {len(reconstructed_trades)} trades",
            "trades": reconstructed_trades[:50],  # Return first 50 for display
            "metrics": metrics,
            "equity_curve": equity_curve,
            "drawdown_curve": drawdown_curve,
            "total_trades": len(reconstructed_trades),
            "return_basis": return_basis
        })

    @app.route("/api/download_historical_data", methods=["POST"])
    def api_download_historical_data() -> Any:
        """Trigger manual root-data M5 refresh."""
        current_config = app.config["dashboard_config"]
        auth_error = _authorize_write_api(current_config)
        if auth_error is not None:
            return auth_error

        data_downloader = app.config.get("data_downloader")
        data_scheduler = app.config.get("data_scheduler")

        if not data_downloader:
            return jsonify({
                "success": False,
                "error": "Data maintenance service not available"
            })
        if not data_downloader.can_refresh_from_mt5():
            return jsonify({
                "success": False,
                "error": "MT5 is not connected; cannot refresh root M5 data"
            })

        try:
            # Run maintenance in background
            import threading
            run_target = data_scheduler.run_now if data_scheduler else data_downloader.refresh_all_m5_data
            thread = threading.Thread(target=run_target, daemon=True)
            thread.start()

            return jsonify({
                "success": True,
                "message": "Root M5 data maintenance started"
            })
        except Exception as e:
            logger.error("Failed to start data maintenance: %s", e)
            return jsonify({
                "success": False,
                "error": str(e)
            })

    return app


def serialize_config(config: Dict[str, Any]) -> Dict[str, Any]:
    merged = deep_merge(DEFAULT_CONFIG, config if isinstance(config, dict) else {})
    pm_root = str(merged.get("pm_root") or "")
    telegram_cfg = copy.deepcopy(merged.get("telegram", {}) or {})
    token_env = str(telegram_cfg.get("bot_token_env") or "PM_DASHBOARD_TELEGRAM_BOT_TOKEN")
    telegram_cfg["bot_token_configured"] = bool(os.environ.get(token_env))
    return {
        "pm_root": pm_root,
        "refresh_interval_sec": merged.get("refresh_interval_sec"),
        "file_patterns": list(merged.get("file_patterns", [])),
        "explicit_files": list(merged.get("explicit_files", [])),
        "pm_configs_path": merged.get("pm_configs_path", "auto"),
        "active_pm_configs_path": resolve_pm_configs_path(pm_root, merged),
        "min_strength": merged.get("min_strength"),
        "max_signal_age_minutes": merged.get("max_signal_age_minutes"),
        "alert": copy.deepcopy(merged.get("alert", {})),
        "telegram": telegram_cfg,
    }


def apply_config_updates(config: Dict[str, Any], payload: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(payload, dict):
        raise ValueError("Config payload must be a JSON object")

    updated = deep_merge(DEFAULT_CONFIG, copy.deepcopy(config if isinstance(config, dict) else {}))
    if "pm_root" in payload:
        updated["pm_root"] = str(payload.get("pm_root") or "").strip()
    if "refresh_interval_sec" in payload:
        updated["refresh_interval_sec"] = _coerce_int(payload.get("refresh_interval_sec"), "refresh_interval_sec", 1)
    if "min_strength" in payload:
        updated["min_strength"] = _coerce_float(payload.get("min_strength"), "min_strength", 0.0)
    if "max_signal_age_minutes" in payload:
        updated["max_signal_age_minutes"] = _coerce_int(
            payload.get("max_signal_age_minutes"),
            "max_signal_age_minutes",
            1,
        )
    if "file_patterns" in payload:
        updated["file_patterns"] = _coerce_string_list(payload.get("file_patterns"), "file_patterns")
    if "explicit_files" in payload:
        updated["explicit_files"] = _coerce_string_list(payload.get("explicit_files"), "explicit_files")
    if "pm_configs_path" in payload:
        updated["pm_configs_path"] = str(payload.get("pm_configs_path") or "auto").strip() or "auto"

    if "alert" in payload:
        alert_payload = payload.get("alert")
        if not isinstance(alert_payload, dict):
            raise ValueError("alert must be an object")
        alert_cfg = copy.deepcopy(updated.get("alert", {}))
        if "enabled" in alert_payload:
            alert_cfg["enabled"] = _coerce_bool(alert_payload.get("enabled"), "alert.enabled")
        if "sound" in alert_payload:
            alert_cfg["sound"] = _coerce_bool(alert_payload.get("sound"), "alert.sound")
        if "min_strength" in alert_payload:
            alert_cfg["min_strength"] = _coerce_float(alert_payload.get("min_strength"), "alert.min_strength", 0.0)
        updated["alert"] = alert_cfg
    if "telegram" in payload:
        telegram_payload = payload.get("telegram")
        if not isinstance(telegram_payload, dict):
            raise ValueError("telegram must be an object")
        telegram_cfg = copy.deepcopy(updated.get("telegram", {}))
        if "enabled" in telegram_payload:
            telegram_cfg["enabled"] = _coerce_bool(telegram_payload.get("enabled"), "telegram.enabled")
        if "chat_id" in telegram_payload:
            telegram_cfg["chat_id"] = str(telegram_payload.get("chat_id") or "").strip()
        if "bot_token_env" in telegram_payload:
            telegram_cfg["bot_token_env"] = str(
                telegram_payload.get("bot_token_env") or "PM_DASHBOARD_TELEGRAM_BOT_TOKEN"
            ).strip() or "PM_DASHBOARD_TELEGRAM_BOT_TOKEN"
        if "min_strength" in telegram_payload:
            telegram_cfg["min_strength"] = _coerce_float(
                telegram_payload.get("min_strength"),
                "telegram.min_strength",
                0.0,
            )
        if "max_signal_age_minutes" in telegram_payload:
            telegram_cfg["max_signal_age_minutes"] = _coerce_int(
                telegram_payload.get("max_signal_age_minutes"),
                "telegram.max_signal_age_minutes",
                1,
            )
        if "include_strategy" in telegram_payload:
            telegram_cfg["include_strategy"] = _coerce_bool(
                telegram_payload.get("include_strategy"),
                "telegram.include_strategy",
            )
        if "include_regime" in telegram_payload:
            telegram_cfg["include_regime"] = _coerce_bool(
                telegram_payload.get("include_regime"),
                "telegram.include_regime",
            )
        if "send_on_startup" in telegram_payload:
            telegram_cfg["send_on_startup"] = _coerce_bool(
                telegram_payload.get("send_on_startup"),
                "telegram.send_on_startup",
            )
        if "actions" in telegram_payload:
            telegram_cfg["actions"] = _coerce_string_list(telegram_payload.get("actions"), "telegram.actions")
        if "action_prefixes" in telegram_payload:
            telegram_cfg["action_prefixes"] = _coerce_string_list(
                telegram_payload.get("action_prefixes"),
                "telegram.action_prefixes",
            )
        updated["telegram"] = telegram_cfg
    return updated


def build_strategy_payload(pm_configs: Dict[str, Any], include_invalid: bool) -> Dict[str, Any]:
    rows = []
    summary = {"total": 0, "validated": 0, "invalid": 0, "expired": 0}
    now = None

    for symbol, cfg in pm_configs.items():
        if not isinstance(cfg, dict):
            continue
        is_validated = cfg.get("is_validated")
        if is_validated is None:
            is_validated = True
        validation_reason = cfg.get("validation_reason", "")
        optimized_at = cfg.get("optimized_at")
        valid_until = cfg.get("valid_until")
        symbol_timeframe = cfg.get("timeframe", "")

        regime_configs = cfg.get("regime_configs", {}) or {}
        has_regimes = False
        for timeframe, regimes in regime_configs.items():
            if not isinstance(regimes, dict):
                continue
            for regime, reg_cfg in regimes.items():
                if not isinstance(reg_cfg, dict):
                    continue
                has_regimes = True
                row = strategy_row_from_config(
                    symbol,
                    timeframe,
                    regime,
                    reg_cfg,
                    is_validated,
                    validation_reason,
                    optimized_at,
                    valid_until,
                )
                status = row.get("validation_status", "validated")
                if not include_invalid and status == "invalid":
                    continue
                summary["total"] += 1
                summary["validated"] += 1 if status == "validated" else 0
                summary["invalid"] += 1 if status == "invalid" else 0
                summary["expired"] += 1 if status == "expired" else 0
                rows.append(row)

        if not has_regimes:
            default_cfg = cfg.get("default_config", {}) if isinstance(cfg.get("default_config"), dict) else {}
            if default_cfg:
                row = strategy_row_from_config(
                    symbol,
                    symbol_timeframe or "DEFAULT",
                    "DEFAULT",
                    default_cfg,
                    is_validated,
                    validation_reason,
                    optimized_at,
                    valid_until,
                )
                status = row.get("validation_status", "validated")
                if include_invalid or status != "invalid":
                    summary["total"] += 1
                    summary["validated"] += 1 if status == "validated" else 0
                    summary["invalid"] += 1 if status == "invalid" else 0
                    summary["expired"] += 1 if status == "expired" else 0
                    rows.append(row)

    rows.sort(key=lambda item: (item.get("symbol", ""), item.get("timeframe", ""), item.get("regime", "")))
    return {"rows": rows, "summary": summary}


def _optional_count(value: Any) -> Optional[int]:
    numeric = coerce_float(value)
    if numeric is None:
        return None
    return int(numeric)


def strategy_row_from_config(
    symbol: str,
    timeframe: str,
    regime: str,
    reg_cfg: Dict[str, Any],
    is_validated: bool,
    validation_reason: str,
    optimized_at: Optional[str],
    symbol_valid_until: Optional[str],
) -> Dict[str, Any]:
    strategy_name = reg_cfg.get("strategy_name") or ""
    quality_score = coerce_float(reg_cfg.get("quality_score"))
    train_metrics = reg_cfg.get("train_metrics", {}) or {}
    val_metrics = reg_cfg.get("val_metrics", {}) or {}
    trained_at = reg_cfg.get("trained_at")
    valid_until = reg_cfg.get("valid_until") or symbol_valid_until

    status = "validated" if is_validated else "invalid"
    if valid_until:
        ts = parse_timestamp(valid_until)
        if ts is not None:
            if ts < datetime.now():
                status = "expired"

    row_id = "|".join([str(symbol), str(timeframe), str(regime), str(strategy_name)])
    return {
        "id": row_id,
        "symbol": symbol,
        "timeframe": timeframe,
        "regime": regime,
        "strategy_name": strategy_name,
        "quality_score": quality_score,
        "regime_train_trades": _optional_count(reg_cfg.get("regime_train_trades")),
        "regime_val_trades": _optional_count(reg_cfg.get("regime_val_trades")),
        "validation_status": status,
        "validation_reason": validation_reason,
        "optimized_at": optimized_at,
        "trained_at": trained_at,
        "valid_until": valid_until,
        "parameters": reg_cfg.get("parameters", {}) or {},
        "train_metrics": train_metrics,
        "val_metrics": val_metrics,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="PM Dashboard (read-mostly)")
    parser.add_argument("--pm-root", dest="pm_root", default=None, help="Path to the PM project directory")
    parser.add_argument("--config", dest="config_path", default=None, help="Path to dashboard_config.json")
    parser.add_argument("--host", dest="host", default="127.0.0.1", help="Bind host (default: 127.0.0.1)")
    parser.add_argument("--port", dest="port", type=int, default=8000, help="Bind port (default: 8000)")
    args = parser.parse_args()

    config_path = args.config_path or os.path.join(BASE_DIR, "dashboard_config.json")

    app = create_app(config_path, pm_root_override=args.pm_root)
    app.run(host=args.host, port=args.port, debug=False)


if __name__ == "__main__":
    main()
