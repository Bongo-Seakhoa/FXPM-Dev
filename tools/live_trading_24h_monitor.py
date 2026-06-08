"""24-hour live-trading observer for FXPM.

This process is intentionally read-only. It does not start, stop, or influence
trades; it records whether the live trader is healthy and whether the trading
side is behaving as expected while optimization can continue in parallel.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
import time
from collections import Counter
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _now() -> datetime:
    return datetime.now()


def _read_json(path: Path, default: Any) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return default


def _write_json_atomic(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f"{path.name}.tmp.{os.getpid()}")
    tmp.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
    os.replace(tmp, path)


def _append_jsonl(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, separators=(",", ":"), default=str) + "\n")


def _tail_text(path: Path, max_bytes: int = 2_000_000) -> str:
    try:
        size = path.stat().st_size
        with path.open("rb") as handle:
            if size > max_bytes:
                handle.seek(size - max_bytes)
            return handle.read().decode("utf-8", errors="replace")
    except Exception:
        return ""


def _latest_log(log_dir: Path, pattern: str) -> Optional[Path]:
    files = sorted(log_dir.glob(pattern), key=lambda p: p.stat().st_mtime if p.exists() else 0, reverse=True)
    return files[0] if files else None


def _powershell_processes() -> List[Dict[str, Any]]:
    command = (
        "Get-CimInstance Win32_Process | "
        "Where-Object { $_.CommandLine -match 'pm_main.py' } | "
        "Select-Object ProcessId,ParentProcessId,Name,CommandLine | ConvertTo-Json -Compress"
    )
    try:
        result = subprocess.run(
            ["powershell", "-NoProfile", "-Command", command],
            cwd=str(ROOT),
            capture_output=True,
            text=True,
            timeout=15,
        )
        if result.returncode != 0 or not result.stdout.strip():
            return []
        payload = json.loads(result.stdout)
        if isinstance(payload, dict):
            return [payload]
        if isinstance(payload, list):
            return [item for item in payload if isinstance(item, dict)]
    except Exception:
        return []
    return []


def process_snapshot() -> Dict[str, Any]:
    processes = _powershell_processes()
    live = [p for p in processes if "--trade" in str(p.get("CommandLine", ""))]
    optimize = [p for p in processes if "--optimize" in str(p.get("CommandLine", ""))]
    children_by_parent = Counter(int(p.get("ParentProcessId") or 0) for p in processes)
    return {
        "pm_main_process_count": len(processes),
        "live_pids": [int(p.get("ProcessId") or 0) for p in live],
        "optimizer_pids": [int(p.get("ProcessId") or 0) for p in optimize],
        "optimizer_child_counts": {
            str(int(p.get("ProcessId") or 0)): int(children_by_parent.get(int(p.get("ProcessId") or 0), 0))
            for p in optimize
        },
        "processes": processes,
    }


def optimizer_progress(log_dir: Path) -> Dict[str, Any]:
    log_path = _latest_log(log_dir, "optimize_*.err.log")
    text = _tail_text(log_path) if log_path else ""
    progress = list(re.finditer(r"Optimizing:\s+(\d+)%\|.*?\|\s+(\d+)/(\d+)\s+\[([^\]]+)\]", text))
    saves = re.findall(r"(\d\d:\d\d:\d\d) \[INFO\] SAVED\s+([^\s]+).*?pm_configs\.json", text)
    complete = "OPTIMIZATION COMPLETE" in text or "Optimization complete" in text
    payload: Dict[str, Any] = {
        "log_path": str(log_path) if log_path else None,
        "complete_marker_seen": bool(complete),
        "saved_count_in_tail": len(saves),
        "last_saves": [{"time": t, "symbol": s} for t, s in saves[-12:]],
    }
    if progress:
        match = progress[-1]
        payload.update(
            {
                "progress_pct": int(match.group(1)),
                "progress_count": f"{match.group(2)}/{match.group(3)}",
                "progress_timing": match.group(4),
            }
        )
    return payload


def _is_tradeable_cell(cell: Any) -> bool:
    if not isinstance(cell, dict):
        return False
    name = str(cell.get("strategy_name", "") or "").upper()
    status = str(cell.get("promotion_status", "PROMOTED") or "PROMOTED").upper()
    meta = cell.get("artifact_meta") if isinstance(cell.get("artifact_meta"), dict) else {}
    return bool(name and name != "NO_TRADE" and not meta.get("no_trade") and status not in {"NO_TRADE", "BLOCKED", "PAPER_ONLY"})


def ledger_snapshot(config_path: Path, ledger_path: Path, regime_path: Path) -> Dict[str, Any]:
    cfg = _read_json(config_path, {})
    ledger = _read_json(ledger_path, {})
    regime_params = _read_json(regime_path, {})
    symbols = [str(s) for s in cfg.get("symbols", [])] if isinstance(cfg, dict) else []
    ledger_by_upper = {
        str(k).upper(): (k, v)
        for k, v in (ledger.items() if isinstance(ledger, dict) else [])
        if isinstance(v, dict)
    }
    configured: List[str] = []
    missing: List[str] = []
    valid: List[str] = []
    live: List[str] = []
    no_trade: List[str] = []
    missing_regime_params: List[str] = []
    tradeable_cell_ids: List[str] = []
    tradeable_cells = 0

    for symbol in symbols:
        hit = ledger_by_upper.get(symbol.upper())
        if not hit:
            missing.append(symbol)
            continue
        configured.append(symbol)
        canonical, payload = hit
        tradeable_symbol = False
        for timeframe, regimes in (payload.get("regime_configs") or {}).items():
            if not isinstance(regimes, dict):
                continue
            for regime, cell in regimes.items():
                if not _is_tradeable_cell(cell):
                    continue
                tradeable_symbol = True
                tradeable_cells += 1
                tradeable_cell_ids.append(f"{canonical}|{timeframe}|{regime}")
                if canonical not in regime_params or timeframe not in (regime_params.get(canonical) or {}):
                    missing_regime_params.append(f"{canonical}|{timeframe}|{regime}")
        if payload.get("is_validated") or tradeable_symbol:
            valid.append(symbol)
        else:
            no_trade.append(symbol)
        if tradeable_symbol:
            live.append(symbol)

    return {
        "symbols_total": len(symbols),
        "configured": len(configured),
        "missing": missing,
        "valid_cell_aware": len(valid),
        "invalid_or_no_trade": len(no_trade),
        "tradeable_live_symbols": live,
        "tradeable_live_symbol_count": len(live),
        "tradeable_cells": tradeable_cells,
        "tradeable_live_cells": sorted(tradeable_cell_ids),
        "missing_promoted_regime_params": missing_regime_params,
        "missing_promoted_regime_param_count": len(missing_regime_params),
    }


def signal_ledger_snapshot(output_dir: Path) -> Dict[str, Any]:
    path = _latest_log(output_dir, "signal_ledger_*.jsonl")
    actions: Counter[str] = Counter()
    symbols: Counter[str] = Counter()
    last_records: List[Dict[str, Any]] = []
    if path:
        for line in _tail_text(path).splitlines():
            try:
                item = json.loads(line)
            except Exception:
                continue
            action = str(item.get("action", "") or "UNKNOWN")
            symbol = str(item.get("symbol", "") or "UNKNOWN")
            actions[action] += 1
            symbols[symbol] += 1
            last_records.append(
                {
                    "symbol": symbol,
                    "timeframe": item.get("timeframe"),
                    "regime": item.get("regime"),
                    "strategy_name": item.get("strategy_name"),
                    "direction": item.get("direction"),
                    "action": action,
                    "target_risk_pct": item.get("target_risk_pct"),
                    "actual_risk_pct": item.get("actual_risk_pct"),
                    "action_time": item.get("action_time"),
                }
            )
    return {
        "path": str(path) if path else None,
        "actions": dict(actions),
        "symbol_counts": dict(symbols.most_common(20)),
        "last_records": last_records[-10:],
    }


def trade_files_snapshot(output_dir: Path) -> Dict[str, Any]:
    files = sorted(output_dir.glob("trades_*.json"), key=lambda p: p.stat().st_mtime, reverse=True)
    latest = files[0] if files else None
    payload = _read_json(latest, []) if latest else []
    statuses = Counter()
    for item in payload if isinstance(payload, list) else []:
        statuses[str(item.get("status", "") or "UNKNOWN")] += 1
    return {
        "latest_path": str(latest) if latest else None,
        "latest_trade_count": len(payload) if isinstance(payload, list) else None,
        "latest_statuses": dict(statuses),
        "latest_records": payload[-10:] if isinstance(payload, list) else [],
        "file_count": len(files),
    }


def live_log_snapshot(log_dir: Path) -> Dict[str, Any]:
    path = _latest_log(log_dir, "pm_*.log")
    text = _tail_text(path) if path else ""
    lines = text.splitlines()
    counters = {
        "live_sweeps": sum("Live sweep complete" in line for line in lines),
        "selected": sum(" Selected:" in line for line in lines),
        "no_actionable": sum("NO_ACTIONABLE_WINNER_SIGNAL" in line for line in lines),
        "order_failed": sum("Order failed" in line for line in lines),
        "trade_skipped": sum("Skipping trade" in line or "Trade skipped" in line for line in lines),
        "storage_advisory": sum("Storage pressure advisory active" in line for line in lines),
        "storage_old_skip": sum("storage pressure gate active" in line for line in lines),
        "readiness_errors": sum("Live readiness ERROR" in line for line in lines),
        "errors": sum(" [ERROR] " in line for line in lines),
        "warnings": sum(" [WARNING] " in line for line in lines),
    }
    interesting = [
        line
        for line in lines[-400:]
        if any(
            marker in line
            for marker in (
                "STARTING LIVE TRADING",
                "Live readiness",
                "Live sweep complete",
                "Selected:",
                "Order failed",
                "[OK]",
                "Skipping trade",
                "Trade skipped",
                "Storage pressure",
                "ERROR",
                "WARNING",
            )
        )
    ]
    return {
        "path": str(path) if path else None,
        "counters_in_tail": counters,
        "last_interesting_lines": interesting[-40:],
    }


def current_live_process_log_snapshot(log_dir: Path) -> Dict[str, Any]:
    """Inspect the current per-process live-trader log without old same-day noise."""
    path = _latest_log(log_dir, "live_trade_*.err.log")
    text = _tail_text(path) if path else ""
    lines = text.splitlines()
    counters = {
        "live_sweeps": sum("Live sweep complete" in line for line in lines),
        "selected": sum(" Selected:" in line for line in lines),
        "actual_risk": sum("Actual risk" in line for line in lines),
        "risk_blocks": sum("Skipping trade; actual risk" in line for line in lines),
        "order_send": sum("order_send" in line for line in lines),
        "order_failed": sum("Order failed" in line for line in lines),
        "tickets": sum("ticket" in line.lower() for line in lines),
        "errors": sum(" [ERROR] " in line for line in lines),
        "criticals": sum(" [CRITICAL] " in line for line in lines),
        "tracebacks": sum("Traceback" in line for line in lines),
    }
    interesting = [
        line
        for line in lines[-400:]
        if any(
            marker in line
            for marker in (
                "position: risk",
                "Live sweep complete",
                "Selected:",
                "Actual risk",
                "Skipping trade",
                "Order failed",
                "order_send",
                "ticket",
                "[ERROR]",
                "[CRITICAL]",
                "Traceback",
            )
        )
    ]
    return {
        "path": str(path) if path else None,
        "counters_in_tail": counters,
        "last_interesting_lines": interesting[-40:],
    }


def mt5_snapshot(config_path: Path) -> Dict[str, Any]:
    try:
        from pm_main import _filter_dataclass_kwargs, load_config_json
        from pm_mt5 import MT5Config, MT5Connector

        config_data = load_config_json(str(config_path))
        mt5_section = dict(config_data.get("mt5") or {})
        if "terminal_path" in mt5_section and "path" not in mt5_section:
            mt5_section["path"] = mt5_section["terminal_path"]
        mt5_config = MT5Config(**_filter_dataclass_kwargs(MT5Config, mt5_section))
        connector = MT5Connector(mt5_config)
        try:
            if not connector.connect():
                return {"connected": False, "error": "connect_failed"}
            account = connector.get_account_info()
            positions = connector.get_positions()
            account_payload = None
            if account is not None:
                account_payload = {
                    "login": getattr(account, "login", None),
                    "server": getattr(account, "server", None),
                    "currency": getattr(account, "currency", None),
                    "balance": getattr(account, "balance", None),
                    "equity": getattr(account, "equity", None),
                    "profit": getattr(account, "profit", None),
                    "margin": getattr(account, "margin", None),
                    "margin_free": getattr(account, "margin_free", None),
                    "margin_level": getattr(account, "margin_level", None),
                    "trade_allowed": getattr(account, "trade_allowed", None),
                    "trade_expert": getattr(account, "trade_expert", None),
                }
            position_payload = []
            if positions is not None:
                for pos in positions:
                    position_payload.append(
                        {
                            "ticket": getattr(pos, "ticket", None),
                            "symbol": getattr(pos, "symbol", None),
                            "type": getattr(pos, "type", None),
                            "volume": getattr(pos, "volume", None),
                            "price_open": getattr(pos, "price_open", None),
                            "price_current": getattr(pos, "price_current", None),
                            "sl": getattr(pos, "sl", None),
                            "tp": getattr(pos, "tp", None),
                            "profit": getattr(pos, "profit", None),
                            "swap": getattr(pos, "swap", None),
                            "magic": getattr(pos, "magic", None),
                            "comment": getattr(pos, "comment", None),
                            "time": getattr(pos, "time", None),
                            "time_update": getattr(pos, "time_update", None),
                        }
                    )
            return {
                "connected": True,
                "account": account_payload,
                "open_position_count": len(position_payload) if positions is not None else None,
                "open_positions": position_payload,
            }
        finally:
            try:
                connector.disconnect()
            except Exception:
                pass
    except Exception as exc:
        return {"connected": False, "error": repr(exc)}


def build_sample(args: argparse.Namespace, sample_index: int, started_at: datetime) -> Dict[str, Any]:
    config_path = ROOT / args.config
    output_dir = ROOT / args.output_dir
    log_dir = ROOT / args.log_dir
    ledger_path = ROOT / args.ledger
    regime_path = ROOT / args.regime_params
    now = _now()
    return {
        "sample_index": sample_index,
        "sampled_at": now.isoformat(),
        "started_at": started_at.isoformat(),
        "elapsed_minutes": round((now - started_at).total_seconds() / 60.0, 3),
        "process": process_snapshot(),
        "mt5": mt5_snapshot(config_path),
        "ledger": ledger_snapshot(config_path, ledger_path, regime_path),
        "signal_ledger": signal_ledger_snapshot(output_dir),
        "trade_files": trade_files_snapshot(output_dir),
        "live_log": live_log_snapshot(log_dir),
        "current_live_process_log": current_live_process_log_snapshot(log_dir),
        "optimizer": optimizer_progress(log_dir),
    }


def classify_sample(sample: Dict[str, Any]) -> Tuple[str, List[str]]:
    findings: List[str] = []
    live_pids = sample.get("process", {}).get("live_pids") or []
    if not live_pids:
        findings.append("live_trader_process_not_running")
    mt5 = sample.get("mt5", {})
    account = mt5.get("account") or {}
    if not mt5.get("connected"):
        findings.append("mt5_monitor_connection_failed")
    if account and not account.get("trade_allowed"):
        findings.append("mt5_trade_allowed_false")
    if account and not account.get("trade_expert"):
        findings.append("mt5_trade_expert_false")
    ledger = sample.get("ledger", {})
    if ledger.get("missing"):
        findings.append("symbols_missing_from_ledger")
    if ledger.get("missing_promoted_regime_param_count"):
        findings.append("promoted_regime_params_missing")
    if int(ledger.get("tradeable_live_symbol_count") or 0) <= 0:
        findings.append("no_tradeable_live_symbols")
    current_log = sample.get("current_live_process_log", {})
    current_counters = current_log.get("counters_in_tail") or {}
    if int(current_counters.get("errors") or 0) > 0:
        findings.append("current_live_log_errors")
    if int(current_counters.get("criticals") or 0) > 0:
        findings.append("current_live_log_criticals")
    if int(current_counters.get("tracebacks") or 0) > 0:
        findings.append("current_live_log_tracebacks")
    if int(current_counters.get("order_failed") or 0) > 0:
        findings.append("current_live_order_failures")
    if findings:
        hard = {
            "live_trader_process_not_running",
            "mt5_monitor_connection_failed",
            "mt5_trade_allowed_false",
            "mt5_trade_expert_false",
            "promoted_regime_params_missing",
            "no_tradeable_live_symbols",
            "current_live_log_criticals",
            "current_live_log_tracebacks",
        }
        return ("ERROR" if any(item in hard for item in findings) else "WARNING", findings)
    return "OK", []


def run_monitor(args: argparse.Namespace) -> int:
    started_at = _now()
    run_id = started_at.strftime("%Y%m%d_%H%M%S")
    out_dir = ROOT / args.monitor_dir
    events_path = out_dir / f"live_monitor_{run_id}.jsonl"
    latest_path = out_dir / "live_monitor_latest.json"
    summary_path = out_dir / f"live_monitor_{run_id}_summary.json"
    end_at = started_at + timedelta(hours=float(args.duration_hours))
    interval_seconds = max(60.0, float(args.interval_minutes) * 60.0)
    sample_index = 0
    statuses: Counter[str] = Counter()
    baseline_tradeable_symbols: Optional[set[str]] = None
    baseline_tradeable_cells: Optional[set[str]] = None

    while True:
        sample = build_sample(args, sample_index, started_at)
        ledger = sample.get("ledger", {})
        current_symbols = set(str(s) for s in ledger.get("tradeable_live_symbols") or [])
        current_cells = set(str(c) for c in ledger.get("tradeable_live_cells") or [])
        if baseline_tradeable_symbols is None:
            baseline_tradeable_symbols = set(current_symbols)
            baseline_tradeable_cells = set(current_cells)
        sample["promotion_delta"] = {
            "baseline_tradeable_symbol_count": len(baseline_tradeable_symbols),
            "current_tradeable_symbol_count": len(current_symbols),
            "new_tradeable_symbols": sorted(current_symbols - baseline_tradeable_symbols),
            "removed_tradeable_symbols": sorted(baseline_tradeable_symbols - current_symbols),
            "baseline_tradeable_cell_count": len(baseline_tradeable_cells or set()),
            "current_tradeable_cell_count": len(current_cells),
            "new_tradeable_cells": sorted(current_cells - (baseline_tradeable_cells or set())),
            "removed_tradeable_cells": sorted((baseline_tradeable_cells or set()) - current_cells),
        }
        status, findings = classify_sample(sample)
        sample["monitor_status"] = status
        sample["monitor_findings"] = findings
        sample["planned_end_at"] = end_at.isoformat()
        statuses[status] += 1
        _append_jsonl(events_path, sample)
        _write_json_atomic(latest_path, sample)
        print(
            f"{sample['sampled_at']} sample={sample_index} status={status} "
            f"live_pids={sample['process']['live_pids']} "
            f"equity={(sample.get('mt5', {}).get('account') or {}).get('equity')} "
            f"positions={sample.get('mt5', {}).get('open_position_count')} "
            f"findings={','.join(findings) if findings else 'none'}",
            flush=True,
        )

        sample_index += 1
        if _now() >= end_at:
            break
        sleep_for = min(interval_seconds, max(0.0, (end_at - _now()).total_seconds()))
        time.sleep(sleep_for)

    summary = {
        "started_at": started_at.isoformat(),
        "ended_at": _now().isoformat(),
        "duration_hours": args.duration_hours,
        "interval_minutes": args.interval_minutes,
        "sample_count": sample_index,
        "status_counts": dict(statuses),
        "events_path": str(events_path),
        "latest_path": str(latest_path),
    }
    _write_json_atomic(summary_path, summary)
    return 0 if not statuses.get("ERROR") else 2


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", default="config.json")
    parser.add_argument("--ledger", default="pm_configs.json")
    parser.add_argument("--regime-params", default="regime_params.json")
    parser.add_argument("--output-dir", default="pm_outputs/production_live")
    parser.add_argument("--log-dir", default="logs/production_live")
    parser.add_argument("--monitor-dir", default="logs/production_live/monitoring")
    parser.add_argument("--duration-hours", type=float, default=24.0)
    parser.add_argument("--interval-minutes", type=float, default=30.0)
    return run_monitor(parser.parse_args())


if __name__ == "__main__":
    raise SystemExit(main())
