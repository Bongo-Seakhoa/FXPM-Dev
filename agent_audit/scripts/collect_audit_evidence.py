"""Read-only evidence collector for the FxPM multi-agent audit.

The script intentionally avoids importing project modules so it cannot
initialize MT5, start trading, or run optimization as an import side effect.
It reads existing artifacts and writes normalized evidence only under
agent_audit/data/.
"""

from __future__ import annotations

import csv
import json
import math
import os
import re
import subprocess
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple


ROOT = Path(__file__).resolve().parents[2]
OUT = ROOT / "agent_audit" / "data"


METRIC_KEYS = {
    "total_trades",
    "win_rate",
    "profit_factor",
    "max_drawdown_pct",
    "total_return_pct",
    "total_pnl",
    "expectancy_pips",
    "sharpe_ratio",
    "sharpe_approx",
    "sortino_ratio",
    "sortino_approx",
    "calmar_ratio",
    "return_dd_ratio",
    "mean_r",
    "median_r",
    "worst_5pct_r",
    "recovery_factor",
    "max_consec_losses",
}

NON_LIVE_STATUSES = {"NO_TRADE", "BLOCKED", "PAPER_ONLY"}


def rel(path: Path) -> str:
    try:
        return str(path.relative_to(ROOT)).replace("\\", "/")
    except ValueError:
        return str(path).replace("\\", "/")


def read_json(path: Path) -> Any:
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, sort_keys=True, allow_nan=True)
        handle.write("\n")


def write_csv(path: Path, rows: List[Dict[str, Any]], fieldnames: List[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow({key: csv_value(row.get(key)) for key in fieldnames})


def csv_value(value: Any) -> Any:
    if isinstance(value, float):
        if math.isinf(value):
            return "Infinity" if value > 0 else "-Infinity"
        if math.isnan(value):
            return "NaN"
    if isinstance(value, (dict, list, tuple)):
        return json.dumps(value, sort_keys=True, allow_nan=True)
    return value


def finite_float(value: Any) -> Optional[float]:
    if value is None or value == "":
        return None
    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    if not math.isfinite(number):
        return None
    return number


def metric_value(payload: Dict[str, Any], key: str) -> Optional[float]:
    if not isinstance(payload, dict):
        return None
    if key in payload:
        return finite_float(payload.get(key))
    return None


def summarize_numbers(values: Iterable[Any]) -> Dict[str, Any]:
    nums = sorted(v for v in (finite_float(value) for value in values) if v is not None)
    if not nums:
        return {"count": 0}
    n = len(nums)
    mid = n // 2
    median = nums[mid] if n % 2 else (nums[mid - 1] + nums[mid]) / 2.0
    return {
        "count": n,
        "min": round(nums[0], 6),
        "p25": round(nums[int((n - 1) * 0.25)], 6),
        "median": round(median, 6),
        "p75": round(nums[int((n - 1) * 0.75)], 6),
        "max": round(nums[-1], 6),
        "mean": round(sum(nums) / n, 6),
    }


def run_git(args: List[str]) -> str:
    try:
        completed = subprocess.run(
            ["git", *args],
            cwd=ROOT,
            check=False,
            capture_output=True,
            text=True,
            timeout=20,
        )
    except Exception as exc:
        return f"ERROR: {type(exc).__name__}: {exc}"
    text = (completed.stdout or "") + (completed.stderr or "")
    return text.strip()


def is_tradeable(payload: Dict[str, Any]) -> bool:
    if not isinstance(payload, dict):
        return False
    name = str(payload.get("strategy_name", "") or "").strip().upper()
    meta = payload.get("artifact_meta") if isinstance(payload.get("artifact_meta"), dict) else {}
    status = str(payload.get("promotion_status", "BLOCKED") or "BLOCKED").strip().upper()
    return bool(name and name != "NO_TRADE" and not meta.get("no_trade") and status not in NON_LIVE_STATUSES)


def iter_tradeable_cells(symbol_key: str, symbol_payload: Dict[str, Any]) -> Iterable[Dict[str, Any]]:
    symbol = str(symbol_payload.get("symbol") or symbol_key or "").strip().upper()
    default_cfg = symbol_payload.get("default_config")
    if isinstance(default_cfg, dict) and is_tradeable(default_cfg):
        yield make_cell_row(symbol, "DEFAULT", "LEGACY", default_cfg, symbol_payload)

    regimes = symbol_payload.get("regime_configs")
    if isinstance(regimes, dict):
        for timeframe, regime_payload in regimes.items():
            if not isinstance(regime_payload, dict):
                continue
            for regime, cell_payload in regime_payload.items():
                if isinstance(cell_payload, dict) and is_tradeable(cell_payload):
                    yield make_cell_row(
                        symbol,
                        str(timeframe or "").upper(),
                        str(regime or "").upper(),
                        cell_payload,
                        symbol_payload,
                    )
        return

    if not isinstance(default_cfg, dict) and is_tradeable(symbol_payload):
        yield make_cell_row(symbol, str(symbol_payload.get("timeframe") or "LEGACY").upper(), "LEGACY", symbol_payload, symbol_payload)


def make_cell_row(symbol: str, timeframe: str, regime: str, payload: Dict[str, Any], symbol_payload: Dict[str, Any]) -> Dict[str, Any]:
    train = payload.get("train_metrics") if isinstance(payload.get("train_metrics"), dict) else {}
    val = payload.get("val_metrics") if isinstance(payload.get("val_metrics"), dict) else {}
    evidence = payload.get("validation_evidence") if isinstance(payload.get("validation_evidence"), dict) else {}
    fdc = payload.get("false_discovery") if isinstance(payload.get("false_discovery"), dict) else {}
    if not fdc and isinstance(evidence.get("false_discovery"), dict):
        fdc = evidence.get("false_discovery") or {}
    live_obs = payload.get("live_observability") if isinstance(payload.get("live_observability"), dict) else {}
    drift = live_obs.get("last_seen_drift") if isinstance(live_obs.get("last_seen_drift"), dict) else {}
    artifact = payload.get("artifact_meta") if isinstance(payload.get("artifact_meta"), dict) else {}
    return {
        "symbol": symbol,
        "timeframe": timeframe,
        "regime": regime,
        "strategy_name": payload.get("strategy_name", ""),
        "promotion_status": str(payload.get("promotion_status", "BLOCKED") or "BLOCKED"),
        "valid_until": payload.get("valid_until") or symbol_payload.get("valid_until"),
        "quality_score": payload.get("quality_score", ""),
        "selection_score": payload.get("selection_score", evidence.get("selection_score_raw", "")),
        "train_trades": metric_value(train, "total_trades"),
        "train_return_pct": metric_value(train, "total_return_pct"),
        "train_profit_factor": metric_value(train, "profit_factor"),
        "train_win_rate": metric_value(train, "win_rate"),
        "train_drawdown_pct": metric_value(train, "max_drawdown_pct"),
        "train_sharpe": metric_value(train, "sharpe_ratio"),
        "train_sortino": metric_value(train, "sortino_ratio"),
        "train_calmar": metric_value(train, "calmar_ratio"),
        "train_expectancy_pips": metric_value(train, "expectancy_pips"),
        "val_trades": metric_value(val, "total_trades"),
        "val_return_pct": metric_value(val, "total_return_pct"),
        "val_profit_factor": metric_value(val, "profit_factor"),
        "val_win_rate": metric_value(val, "win_rate"),
        "val_drawdown_pct": metric_value(val, "max_drawdown_pct"),
        "val_sharpe": metric_value(val, "sharpe_ratio") or metric_value(val, "sharpe_approx"),
        "val_sortino": metric_value(val, "sortino_ratio") or metric_value(val, "sortino_approx"),
        "val_calmar": metric_value(val, "calmar_ratio"),
        "val_expectancy_pips": metric_value(val, "expectancy_pips"),
        "validation_return_dd_ratio": evidence.get("val_return_dd_ratio", ""),
        "deflated_sharpe": fdc.get("deflated_sharpe", ""),
        "pbo_status": fdc.get("pbo_status", ""),
        "artifact_version": artifact.get("artifact_version", ""),
        "split_contract_version": artifact.get("split_contract_version", ""),
        "live_trade_count": drift.get("trade_count", ""),
        "live_win_rate": drift.get("live_win_rate", ""),
        "recent_consecutive_losses": live_obs.get("recent_consecutive_losses", ""),
        "operator_state": live_obs.get("operator_state", ""),
    }


def ledger_summary() -> Tuple[Dict[str, Any], List[Dict[str, Any]], List[Dict[str, Any]]]:
    path = ROOT / "pm_configs.json"
    payload = read_json(path)
    if not isinstance(payload, dict):
        return {"error": "pm_configs.json is not a dict"}, [], []

    cell_rows: List[Dict[str, Any]] = []
    symbol_rows: List[Dict[str, Any]] = []
    rejection_reasons = Counter()
    status_counts = Counter()
    artifact_counts = Counter()

    for symbol_key, symbol_payload in payload.items():
        if not isinstance(symbol_payload, dict):
            continue
        symbol = str(symbol_payload.get("symbol") or symbol_key or "").strip().upper()
        artifact = symbol_payload.get("artifact_meta") if isinstance(symbol_payload.get("artifact_meta"), dict) else {}
        contract = "|".join([
            str(artifact.get("artifact_version", "missing")),
            str(artifact.get("split_contract_version", "missing")),
        ])
        artifact_counts[contract] += 1
        cells = list(iter_tradeable_cells(symbol_key, symbol_payload))
        cell_rows.extend(cells)
        regimes = symbol_payload.get("regime_configs") if isinstance(symbol_payload.get("regime_configs"), dict) else {}
        status_counter = Counter()
        candidate_count = 0
        if regimes:
            for tf_payload in regimes.values():
                if not isinstance(tf_payload, dict):
                    continue
                for cell_payload in tf_payload.values():
                    if isinstance(cell_payload, dict):
                        candidate_count += 1
                        status = str(cell_payload.get("promotion_status", "BLOCKED") or "BLOCKED").strip().upper()
                        status_counter[status] += 1
                        status_counts[status] += 1
        else:
            status = str(symbol_payload.get("promotion_status", "BLOCKED") or "BLOCKED").strip().upper()
            status_counter[status] += 1
            status_counts[status] += 1
            candidate_count = 1
        reason = str(symbol_payload.get("validation_reason") or symbol_payload.get("reason") or "").strip()
        if reason and not cells:
            rejection_reasons[reason[:180]] += 1
        symbol_rows.append({
            "symbol": symbol,
            "is_validated": symbol_payload.get("is_validated", ""),
            "valid_until": symbol_payload.get("valid_until", ""),
            "tradeable_cell_count": len(cells),
            "candidate_cell_count": candidate_count,
            "status_counts": dict(status_counter),
            "artifact_contract": contract,
            "validation_reason": reason[:500],
        })

    summary = {
        "source": rel(path),
        "symbol_count": len(payload),
        "tradeable_symbol_count": len({row["symbol"] for row in cell_rows}),
        "tradeable_cell_count": len(cell_rows),
        "status_counts": dict(status_counts),
        "artifact_contract_counts": dict(artifact_counts),
        "top_non_tradeable_reasons": rejection_reasons.most_common(20),
        "metric_distributions": {
            "val_trades": summarize_numbers(row.get("val_trades") for row in cell_rows),
            "val_return_pct": summarize_numbers(row.get("val_return_pct") for row in cell_rows),
            "val_profit_factor": summarize_numbers(row.get("val_profit_factor") for row in cell_rows),
            "val_win_rate": summarize_numbers(row.get("val_win_rate") for row in cell_rows),
            "val_drawdown_pct": summarize_numbers(row.get("val_drawdown_pct") for row in cell_rows),
            "val_sharpe": summarize_numbers(row.get("val_sharpe") for row in cell_rows),
            "val_calmar": summarize_numbers(row.get("val_calmar") for row in cell_rows),
            "deflated_sharpe": summarize_numbers(row.get("deflated_sharpe") for row in cell_rows),
            "live_trade_count": summarize_numbers(row.get("live_trade_count") for row in cell_rows),
            "live_win_rate": summarize_numbers(row.get("live_win_rate") for row in cell_rows),
        },
        "risk_flags": {
            "low_validation_trade_cells_lt_20": sum(1 for row in cell_rows if (finite_float(row.get("val_trades")) or 0.0) < 20.0),
            "extreme_val_sharpe_cells_gt_10": sum(1 for row in cell_rows if (finite_float(row.get("val_sharpe")) or 0.0) > 10.0),
            "missing_pbo_status_cells": sum(1 for row in cell_rows if not str(row.get("pbo_status") or "").strip()),
            "cells_with_live_drift_observations": sum(1 for row in cell_rows if finite_float(row.get("live_trade_count")) is not None),
        },
    }
    return summary, cell_rows, symbol_rows


def optimization_summary() -> Dict[str, Any]:
    path = ROOT / "pm_outputs" / "production_live" / "optimization_summary.csv"
    if not path.exists():
        return {"available": False, "source": rel(path)}
    rows: List[Dict[str, Any]] = []
    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            rows.append(row)
    return {
        "available": True,
        "source": rel(path),
        "row_count": len(rows),
        "validated_count": sum(1 for row in rows if str(row.get("validated", "")).lower() == "true"),
        "val_trades": summarize_numbers(row.get("val_trades") for row in rows),
        "val_win_rate": summarize_numbers(row.get("val_win_rate") for row in rows),
        "score": summarize_numbers(row.get("score") for row in rows),
        "top_scores": sorted(
            (
                {
                    "symbol": row.get("symbol"),
                    "strategy": row.get("strategy"),
                    "timeframe": row.get("timeframe"),
                    "score": finite_float(row.get("score")),
                    "val_trades": finite_float(row.get("val_trades")),
                    "val_win_rate": finite_float(row.get("val_win_rate")),
                }
                for row in rows
            ),
            key=lambda item: item.get("score") if item.get("score") is not None else -9999,
            reverse=True,
        )[:20],
    }


def summarize_signal_ledger() -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
    path = ROOT / "pm_outputs" / "production_live" / "signal_ledger_202606.jsonl"
    if not path.exists():
        return {"available": False, "source": rel(path)}, []
    action_counts = Counter()
    symbol_counts = Counter()
    strategy_counts = Counter()
    timeframe_counts = Counter()
    risk_events: List[Dict[str, Any]] = []
    total = 0
    latest_ts = ""
    earliest_ts = ""
    with path.open("r", encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, 1):
            line = line.strip()
            if not line:
                continue
            try:
                row = json.loads(line)
            except Exception:
                action_counts["__parse_error__"] += 1
                continue
            total += 1
            action = str(row.get("action") or "UNKNOWN").strip().upper()
            symbol = str(row.get("symbol") or "").strip().upper()
            strategy = str(row.get("strategy_name") or "").strip()
            timeframe = str(row.get("timeframe") or "").strip().upper()
            ts = str(row.get("action_time") or row.get("timestamp") or "")
            earliest_ts = ts if ts and not earliest_ts else earliest_ts
            latest_ts = ts or latest_ts
            action_counts[action] += 1
            if symbol:
                symbol_counts[symbol] += 1
            if strategy:
                strategy_counts[strategy] += 1
            if timeframe:
                timeframe_counts[timeframe] += 1
            if action in {"EXECUTED", "SKIPPED_RISK_CAP", "BLOCKED_MIN_LOT_EXCEEDS_CAP", "SKIPPED_MARGIN", "SKIPPED_SPREAD", "ORDER_FAILED"} or "RISK" in action or "MARGIN" in action:
                risk_events.append({
                    "line": line_number,
                    "action_time": ts,
                    "symbol": symbol,
                    "timeframe": timeframe,
                    "regime": row.get("regime", ""),
                    "strategy_name": strategy,
                    "action": action,
                    "direction": row.get("direction", ""),
                    "target_risk_pct": row.get("target_risk_pct", ""),
                    "actual_risk_pct": row.get("actual_risk_pct", ""),
                    "entry_price": row.get("entry_price", ""),
                    "stop_loss_price": row.get("stop_loss_price", ""),
                    "take_profit_price": row.get("take_profit_price", ""),
                    "volume": row.get("volume", ""),
                    "context": row.get("context", {}),
                })
    summary = {
        "available": True,
        "source": rel(path),
        "row_count": total,
        "earliest_action_time": earliest_ts,
        "latest_action_time": latest_ts,
        "action_counts": dict(action_counts.most_common()),
        "top_symbols": symbol_counts.most_common(20),
        "top_strategies": strategy_counts.most_common(20),
        "timeframe_counts": dict(timeframe_counts.most_common()),
        "risk_event_count": len(risk_events),
    }
    return summary, risk_events


def _prefixed_stats(prefix: str, values: Iterable[Any]) -> Dict[str, Any]:
    stats = summarize_numbers(values)
    return {f"{prefix}_{key}": value for key, value in stats.items()}


def summarize_risk_events(risk_events: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]]]:
    """Build derived risk evidence from signal-ledger rows."""
    by_action: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    by_symbol_action: Dict[Tuple[str, str], List[Dict[str, Any]]] = defaultdict(list)
    for row in risk_events:
        action = str(row.get("action") or "UNKNOWN").upper()
        symbol = str(row.get("symbol") or "").upper()
        by_action[action].append(row)
        by_symbol_action[(symbol, action)].append(row)

    action_rows: List[Dict[str, Any]] = []
    for action, rows in sorted(by_action.items()):
        actual_values = [finite_float(row.get("actual_risk_pct")) for row in rows]
        target_values = [finite_float(row.get("target_risk_pct")) for row in rows]
        ratios = []
        for actual, target in zip(actual_values, target_values):
            if actual is not None and target is not None and target > 0:
                ratios.append(actual / target)
        row = {
            "action": action,
            "rows": len(rows),
            "actual_gt_5_pct": sum(1 for value in actual_values if value is not None and value > 5.0),
            "actual_gt_10_pct": sum(1 for value in actual_values if value is not None and value > 10.0),
            "actual_gt_20_pct": sum(1 for value in actual_values if value is not None and value > 20.0),
            "actual_gt_25_pct": sum(1 for value in actual_values if value is not None and value > 25.0),
            "actual_gt_2x_target": sum(1 for value in ratios if value > 2.0),
            "actual_gt_5x_target": sum(1 for value in ratios if value > 5.0),
            "actual_gt_10x_target": sum(1 for value in ratios if value > 10.0),
            "actual_gt_20x_target": sum(1 for value in ratios if value > 20.0),
        }
        row.update(_prefixed_stats("actual_risk_pct", actual_values))
        row.update(_prefixed_stats("target_risk_pct", target_values))
        row.update(_prefixed_stats("actual_to_target_ratio", ratios))
        action_rows.append(row)

    executed_actual = [
        finite_float(row.get("actual_risk_pct"))
        for row in risk_events
        if str(row.get("action") or "").upper() == "EXECUTED"
    ]
    executed_actual = [value for value in executed_actual if value is not None]
    cap_rows: List[Dict[str, Any]] = []
    for cap in [3, 5, 8, 10, 15, 20, 25]:
        blocked = sum(1 for value in executed_actual if value > cap + 1e-9)
        cap_rows.append({
            "cap_pct": cap,
            "executed_with_actual_risk": len(executed_actual),
            "would_block_count": blocked,
            "would_block_pct": round((blocked / len(executed_actual)) * 100.0, 3) if executed_actual else "",
            "would_allow_count": len(executed_actual) - blocked,
        })

    symbol_rows: List[Dict[str, Any]] = []
    for (symbol, action), rows in sorted(by_symbol_action.items()):
        actual_values = [finite_float(row.get("actual_risk_pct")) for row in rows]
        target_values = [finite_float(row.get("target_risk_pct")) for row in rows]
        row = {
            "symbol": symbol,
            "action": action,
            "rows": len(rows),
        }
        row.update(_prefixed_stats("actual_risk_pct", actual_values))
        row.update(_prefixed_stats("target_risk_pct", target_values))
        symbol_rows.append(row)

    return action_rows, cap_rows, symbol_rows


def summarize_json_action_file(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {"available": False, "source": rel(path)}
    payload = read_json(path)
    actions = Counter()
    symbols = Counter()

    def visit(value: Any) -> None:
        if isinstance(value, dict):
            action = value.get("action")
            symbol = value.get("symbol")
            if action:
                actions[str(action).upper()] += 1
            if symbol:
                symbols[str(symbol).upper()] += 1
            for child in value.values():
                visit(child)
        elif isinstance(value, list):
            for child in value:
                visit(child)

    visit(payload)
    return {
        "available": True,
        "source": rel(path),
        "top_level_type": type(payload).__name__,
        "top_level_count": len(payload) if isinstance(payload, (dict, list)) else 1,
        "action_counts": dict(actions.most_common()),
        "top_symbols": symbols.most_common(20),
    }


def summarize_logs() -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
    root = ROOT / "logs" / "production_live"
    patterns = {
        "live_block": re.compile(r"LIVE BLOCK ([^:]+): (.*)"),
        "warning": re.compile(r"\[WARNING\]"),
        "error": re.compile(r"\[ERROR\]|Traceback|Exception"),
        "connected_mt5": re.compile(r"Connected to MT5"),
        "trading_enabled": re.compile(r"Trading enabled: True"),
        "executed": re.compile(r"\bEXECUTED\b|Order executed|Market order"),
        "risk_cap": re.compile(r"SKIPPED_RISK_CAP|BLOCKED_MIN_LOT_EXCEEDS_CAP|risk cap", re.IGNORECASE),
        "margin": re.compile(r"margin", re.IGNORECASE),
    }
    totals = Counter()
    live_block_reasons = Counter()
    files: List[Dict[str, Any]] = []
    notable: List[Dict[str, Any]] = []
    if not root.exists():
        return {"available": False, "source": rel(root)}, []
    for path in sorted(root.glob("*.log")) + sorted(root.glob("*.err.log")):
        counts = Counter()
        try:
            with path.open("r", encoding="utf-8", errors="replace") as handle:
                for line_number, line in enumerate(handle, 1):
                    for key, pattern in patterns.items():
                        match = pattern.search(line)
                        if match:
                            counts[key] += 1
                            totals[key] += 1
                            if key == "live_block":
                                live_block_reasons[match.group(2).strip()[:180]] += 1
                            if key in {"error", "risk_cap", "trading_enabled", "connected_mt5"} and len(notable) < 250:
                                notable.append({
                                    "file": rel(path),
                                    "line": line_number,
                                    "kind": key,
                                    "text": line.strip()[:500],
                                })
        except Exception as exc:
            files.append({"file": rel(path), "error": f"{type(exc).__name__}: {exc}"})
            continue
        if counts:
            files.append({"file": rel(path), **dict(counts)})
    return {
        "available": True,
        "source": rel(root),
        "file_count": len(list(root.glob("*.log"))) + len(list(root.glob("*.err.log"))),
        "event_totals": dict(totals),
        "top_live_block_reasons": live_block_reasons.most_common(20),
        "files": files,
    }, notable


def summarize_oos_manifests(max_files: int = 120) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
    root = ROOT / "pm_outputs" / "production_live" / "oos_evidence"
    if not root.exists():
        return {"available": False, "source": rel(root)}, []
    rows: List[Dict[str, Any]] = []
    metric_accumulator: Dict[str, List[float]] = defaultdict(list)
    files = sorted(root.glob("*_oos_manifest.json"))[:max_files]
    parse_errors = []
    for path in files:
        symbol = path.name.split("_oos_manifest.json")[0]
        row = {
            "symbol": symbol,
            "file": rel(path),
            "size_bytes": path.stat().st_size,
            "modified": datetime.fromtimestamp(path.stat().st_mtime).isoformat(),
        }
        try:
            payload = read_json(path)
        except Exception as exc:
            row["parse_error"] = f"{type(exc).__name__}: {exc}"
            parse_errors.append(row)
            rows.append(row)
            continue
        found = Counter()
        collect_metric_keys(payload, found, metric_accumulator, max_nodes=50000)
        row["top_metric_key_counts"] = dict(found.most_common(20))
        row["top_level_keys"] = list(payload.keys())[:30] if isinstance(payload, dict) else []
        rows.append(row)
    summary = {
        "available": True,
        "source": rel(root),
        "manifest_count_scanned": len(rows),
        "parse_error_count": len(parse_errors),
        "metric_distributions": {
            key: summarize_numbers(values)
            for key, values in sorted(metric_accumulator.items())
            if values
        },
    }
    return summary, rows


def collect_metric_keys(value: Any, counts: Counter, metrics: Dict[str, List[float]], *, max_nodes: int) -> None:
    visited = 0
    stack = [value]
    while stack and visited < max_nodes:
        visited += 1
        item = stack.pop()
        if isinstance(item, dict):
            for key, child in item.items():
                if key in METRIC_KEYS:
                    counts[key] += 1
                    number = finite_float(child)
                    if number is not None:
                        metrics[key].append(number)
                if isinstance(child, (dict, list)):
                    stack.append(child)
        elif isinstance(item, list):
            stack.extend(child for child in item if isinstance(child, (dict, list)))


def summarize_state_file(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {"available": False, "source": rel(path)}
    try:
        payload = read_json(path)
    except Exception as exc:
        return {"available": False, "source": rel(path), "error": f"{type(exc).__name__}: {exc}"}
    result = {
        "available": True,
        "source": rel(path),
        "top_level_type": type(payload).__name__,
        "top_level_keys": list(payload.keys())[:50] if isinstance(payload, dict) else [],
    }
    if isinstance(payload, dict):
        for key in [
            "latched",
            "block_reasons",
            "pressure_level",
            "state_updated_at",
            "last_sweep_at",
            "last_live_sweep_at",
            "last_optimization_at",
        ]:
            if key in payload:
                result[key] = payload[key]
        result["nested_counts"] = {
            key: len(value)
            for key, value in payload.items()
            if isinstance(value, (dict, list))
        }
    return result


def search_toronto() -> Dict[str, Any]:
    matches = []
    excluded_dirs = {".git", "agent_audit", "data", "feature_cache", "__pycache__", ".pytest_cache"}
    excluded_files = {"pm_configs.json", "storage_manifest.jsonl"}
    for dirpath, dirnames, filenames in os.walk(ROOT):
        parts = set(Path(dirpath).relative_to(ROOT).parts) if Path(dirpath) != ROOT else set()
        if parts & excluded_dirs:
            dirnames[:] = []
            continue
        dirnames[:] = [d for d in dirnames if d not in excluded_dirs]
        for filename in filenames:
            if filename in excluded_files:
                continue
            path = Path(dirpath) / filename
            if path.suffix.lower() in {".png", ".jpg", ".jpeg", ".gif", ".pdf", ".pack", ".idx", ".npz", ".pyc"}:
                continue
            try:
                text = path.read_text(encoding="utf-8", errors="ignore")
            except Exception:
                continue
            if "toronto" in text.lower():
                for line_number, line in enumerate(text.splitlines(), 1):
                    if "toronto" in line.lower():
                        matches.append({"file": rel(path), "line": line_number, "text": line.strip()[:300]})
    return {
        "match_count": len(matches),
        "matches": matches[:50],
        "conclusion": "No explicit Toronto metric found" if not matches else "Toronto references found; inspect matches",
    }


def summarize_config() -> Dict[str, Any]:
    path = ROOT / "config.json"
    payload = read_json(path)
    pipeline = payload.get("pipeline") if isinstance(payload.get("pipeline"), dict) else {}
    position = payload.get("position") if isinstance(payload.get("position"), dict) else {}
    mt5 = payload.get("mt5") if isinstance(payload.get("mt5"), dict) else {}
    return {
        "source": rel(path),
        "symbol_count": len(payload.get("symbols") or []),
        "pipeline": {
            key: pipeline.get(key)
            for key in [
                "production_split_mode",
                "walk_forward_validation_bars",
                "active_universe_bars",
                "active_stage2_pct",
                "clean_holdout_pct",
                "risk_per_trade_pct",
                "fx_val_max_drawdown",
                "regime_min_val_profit_factor",
                "regime_min_val_return_pct",
                "regime_min_val_return_dd_ratio",
                "train_min_profit_factor",
                "train_max_drawdown",
                "exceptional_val_profit_factor",
                "max_total_open_risk_pct",
                "max_new_entries_when_total_open_risk_above_pct",
                "daily_loss_hard_block_pct",
                "weekly_loss_hard_block_pct",
                "open_drawdown_hard_block_pct",
                "margin_entry_block_level",
                "secondary_trade_max_risk_pct",
                "live_min_account_equity",
                "live_artifact_drift_policy",
                "production_promotion_gate_mode",
                "production_require_full_symbol_ledger",
                "live_allow_partial_symbol_ledger",
                "live_risk_scalars_mode",
                "local_governance_live_mode",
            ]
        },
        "position": {
            key: position.get(key)
            for key in [
                "risk_per_trade_pct",
                "risk_basis",
                "max_risk_pct",
                "risk_tolerance_pct",
                "min_position_size",
                "max_position_size",
                "allow_min_lot_risk_clamp",
                "auto_widen_sl",
            ]
        },
        "mt5": {
            "login_is_blank": mt5.get("login") in (0, "", None),
            "password_is_blank": mt5.get("password") in ("", None),
            "server_is_blank": mt5.get("server") in ("", None),
            "expected_login": mt5.get("expected_login"),
            "expected_server": mt5.get("expected_server"),
            "expected_currency": mt5.get("expected_currency"),
        },
    }


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    ledger, cell_rows, symbol_rows = ledger_summary()
    signal_summary, risk_events = summarize_signal_ledger()
    risk_action_rows, risk_cap_rows, risk_symbol_rows = summarize_risk_events(risk_events)
    log_summary, notable_logs = summarize_logs()
    oos_summary, oos_rows = summarize_oos_manifests()

    summary = {
        "generated_at": datetime.now().isoformat(),
        "repo": str(ROOT),
        "git": {
            "status_short": run_git(["status", "--short"]),
            "diff_stat": run_git(["diff", "--stat"]),
        },
        "config": summarize_config(),
        "ledger": ledger,
        "optimization_summary": optimization_summary(),
        "signal_ledger": signal_summary,
        "risk_event_action_summary": risk_action_rows,
        "risk_cap_sensitivity": risk_cap_rows,
        "last_actionable_log": summarize_json_action_file(ROOT / "last_actionable_log.json"),
        "last_trade_log": summarize_json_action_file(ROOT / "last_trade_log.json"),
        "logs": log_summary,
        "oos_manifests": oos_summary,
        "cycle_drift_state": summarize_state_file(ROOT / "logs" / "production_live" / "cycle_drift_state.json"),
        "supervisor_state": summarize_state_file(ROOT / "logs" / "institutional_supervisor_state.json"),
        "storage_state": summarize_state_file(ROOT / "pm_outputs" / "production_live" / "storage_state.json"),
        "toronto_metric_search": search_toronto(),
    }

    write_json(OUT / "audit_evidence_summary.json", summary)
    write_csv(
        OUT / "ledger_tradeable_cells.csv",
        cell_rows,
        [
            "symbol",
            "timeframe",
            "regime",
            "strategy_name",
            "promotion_status",
            "valid_until",
            "quality_score",
            "selection_score",
            "train_trades",
            "train_return_pct",
            "train_profit_factor",
            "train_win_rate",
            "train_drawdown_pct",
            "train_sharpe",
            "train_sortino",
            "train_calmar",
            "train_expectancy_pips",
            "val_trades",
            "val_return_pct",
            "val_profit_factor",
            "val_win_rate",
            "val_drawdown_pct",
            "val_sharpe",
            "val_sortino",
            "val_calmar",
            "val_expectancy_pips",
            "validation_return_dd_ratio",
            "deflated_sharpe",
            "pbo_status",
            "artifact_version",
            "split_contract_version",
            "live_trade_count",
            "live_win_rate",
            "recent_consecutive_losses",
            "operator_state",
        ],
    )
    write_csv(
        OUT / "ledger_symbol_summary.csv",
        symbol_rows,
        [
            "symbol",
            "is_validated",
            "valid_until",
            "tradeable_cell_count",
            "candidate_cell_count",
            "status_counts",
            "artifact_contract",
            "validation_reason",
        ],
    )
    write_csv(
        OUT / "signal_risk_events.csv",
        risk_events,
        [
            "line",
            "action_time",
            "symbol",
            "timeframe",
            "regime",
            "strategy_name",
            "action",
            "direction",
            "target_risk_pct",
            "actual_risk_pct",
            "entry_price",
            "stop_loss_price",
            "take_profit_price",
            "volume",
            "context",
        ],
    )
    write_csv(
        OUT / "signal_risk_action_summary.csv",
        risk_action_rows,
        [
            "action",
            "rows",
            "actual_risk_pct_count",
            "actual_risk_pct_min",
            "actual_risk_pct_p25",
            "actual_risk_pct_median",
            "actual_risk_pct_p75",
            "actual_risk_pct_max",
            "actual_risk_pct_mean",
            "target_risk_pct_count",
            "target_risk_pct_min",
            "target_risk_pct_p25",
            "target_risk_pct_median",
            "target_risk_pct_p75",
            "target_risk_pct_max",
            "target_risk_pct_mean",
            "actual_to_target_ratio_count",
            "actual_to_target_ratio_min",
            "actual_to_target_ratio_p25",
            "actual_to_target_ratio_median",
            "actual_to_target_ratio_p75",
            "actual_to_target_ratio_max",
            "actual_to_target_ratio_mean",
            "actual_gt_5_pct",
            "actual_gt_10_pct",
            "actual_gt_20_pct",
            "actual_gt_25_pct",
            "actual_gt_2x_target",
            "actual_gt_5x_target",
            "actual_gt_10x_target",
            "actual_gt_20x_target",
        ],
    )
    write_csv(
        OUT / "risk_cap_sensitivity.csv",
        risk_cap_rows,
        ["cap_pct", "executed_with_actual_risk", "would_block_count", "would_block_pct", "would_allow_count"],
    )
    write_csv(
        OUT / "signal_risk_symbol_action_summary.csv",
        risk_symbol_rows,
        [
            "symbol",
            "action",
            "rows",
            "actual_risk_pct_count",
            "actual_risk_pct_min",
            "actual_risk_pct_p25",
            "actual_risk_pct_median",
            "actual_risk_pct_p75",
            "actual_risk_pct_max",
            "actual_risk_pct_mean",
            "target_risk_pct_count",
            "target_risk_pct_min",
            "target_risk_pct_p25",
            "target_risk_pct_median",
            "target_risk_pct_p75",
            "target_risk_pct_max",
            "target_risk_pct_mean",
        ],
    )
    write_csv(
        OUT / "log_notable_events.csv",
        notable_logs,
        ["file", "line", "kind", "text"],
    )
    write_csv(
        OUT / "oos_manifest_summary.csv",
        oos_rows,
        ["symbol", "file", "size_bytes", "modified", "parse_error", "top_level_keys", "top_metric_key_counts"],
    )
    print(f"Wrote audit evidence to {rel(OUT)}")
    print(f"Tradeable cells: {ledger.get('tradeable_cell_count')} across {ledger.get('tradeable_symbol_count')} symbols")
    print(f"Signal ledger rows: {signal_summary.get('row_count')}")
    print(f"OOS manifests scanned: {oos_summary.get('manifest_count_scanned')}")


if __name__ == "__main__":
    main()
