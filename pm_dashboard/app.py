from __future__ import annotations

import argparse
import os
from datetime import datetime
from typing import Any, Dict, Optional

from flask import Flask, jsonify, render_template, request

from .utils import load_dashboard_config, save_dashboard_config
from .watcher import DashboardState, DashboardWatcher
from .utils import parse_timestamp
from .utils import load_instrument_specs
from .analytics import build_analytics_payload, load_trade_history


def create_app(config_path: str, pm_root_override: Optional[str] = None) -> Flask:
    config = load_dashboard_config(config_path)
    if pm_root_override:
        config["pm_root"] = pm_root_override

    pm_root = config.get("pm_root") or ""
    instrument_specs = load_instrument_specs(pm_root)
    state = DashboardState(instrument_specs)
    watcher = DashboardWatcher(pm_root, config, state)
    watcher.start()

    app = Flask(__name__, static_folder="static", template_folder="templates")
    app.config["dashboard_state"] = state
    app.config["dashboard_watcher"] = watcher
    app.config["dashboard_config"] = config
    app.config["dashboard_config_path"] = config_path

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

    @app.route("/api/config", methods=["GET", "POST"])
    def api_config() -> Any:
        current_config = app.config["dashboard_config"]
        if request.method == "GET":
            return jsonify(serialize_config(current_config))

        payload = request.get_json(silent=True) or {}
        updated = apply_config_updates(current_config, payload)
        app.config["dashboard_config"] = updated
        app.config["dashboard_watcher"].update_config(updated)
        save_dashboard_config(config_path, updated)
        return jsonify(serialize_config(updated))

    @app.route("/api/strategies", methods=["GET"])
    def api_strategies() -> Any:
        include_invalid = request.args.get("include_invalid", "false").lower() in ("1", "true", "yes", "y")
        pm_configs = state.get_pm_configs()
        payload = build_strategy_payload(pm_configs, include_invalid)
        return jsonify(payload)

    @app.route("/api/analytics", methods=["GET"])
    def api_analytics() -> Any:
        pm_root = config.get("pm_root") or ""
        initial_capital = float(request.args.get("initial_capital", 10000.0))
        payload = build_analytics_payload(pm_root, initial_capital=initial_capital)
        return jsonify(payload)

    @app.route("/api/trades", methods=["GET"])
    def api_trades() -> Any:
        pm_root = config.get("pm_root") or ""
        limit = int(request.args.get("limit", 200))
        trades = load_trade_history(pm_root, max_files=100)

        filtered_trades = []
        for trade in trades[:limit]:
            filtered_trades.append({
                "timestamp": trade.get("timestamp"),
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
                "strategy": trade.get("strategy"),
                "magic": trade.get("magic")
            })

        return jsonify({"trades": filtered_trades, "total": len(trades)})

    return app


def serialize_config(config: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "pm_root": config.get("pm_root", ""),
        "refresh_interval_sec": config.get("refresh_interval_sec", 5),
        "file_patterns": config.get("file_patterns", []),
        "explicit_files": config.get("explicit_files", []),
        "min_strength": config.get("min_strength", 0.0),
        "max_signal_age_minutes": config.get("max_signal_age_minutes", 1440),
        "alert": config.get("alert", {}),
    }


def apply_config_updates(config: Dict[str, Any], payload: Dict[str, Any]) -> Dict[str, Any]:
    updated = dict(config)
    for key in ("pm_root", "refresh_interval_sec", "min_strength", "max_signal_age_minutes"):
        if key in payload:
            updated[key] = payload[key]

    if "file_patterns" in payload and isinstance(payload["file_patterns"], list):
        updated["file_patterns"] = payload["file_patterns"]
    if "explicit_files" in payload and isinstance(payload["explicit_files"], list):
        updated["explicit_files"] = payload["explicit_files"]

    if "alert" in payload and isinstance(payload["alert"], dict):
        alert_cfg = dict(updated.get("alert", {}))
        for key in ("enabled", "sound", "min_strength"):
            if key in payload["alert"]:
                alert_cfg[key] = payload["alert"][key]
        updated["alert"] = alert_cfg
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
    quality_score = reg_cfg.get("quality_score")
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
        "regime_train_trades": reg_cfg.get("regime_train_trades"),
        "regime_val_trades": reg_cfg.get("regime_val_trades"),
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
    parser = argparse.ArgumentParser(description="PM Dashboard (read-only)")
    parser.add_argument("--pm-root", dest="pm_root", default=None, help="Path to the PM project directory")
    parser.add_argument("--config", dest="config_path", default=None, help="Path to dashboard_config.json")
    parser.add_argument("--host", dest="host", default="127.0.0.1", help="Bind host (default: 127.0.0.1)")
    parser.add_argument("--port", dest="port", type=int, default=8000, help="Bind port (default: 8000)")
    args = parser.parse_args()

    base_dir = os.path.dirname(__file__)
    config_path = args.config_path or os.path.join(base_dir, "dashboard_config.json")

    app = create_app(config_path, pm_root_override=args.pm_root)
    app.run(host=args.host, port=args.port, debug=False)


if __name__ == "__main__":
    main()
