"""Read-only second-pass audit evidence builder.

This script parses existing repository artifacts and writes derived evidence
only under agent_audit/data/. It does not import pm_main, connect to MT5,
change production configs, or run optimization.
"""

from __future__ import annotations

import csv
import json
import math
from collections import Counter, defaultdict
from pathlib import Path
from statistics import median


REPO = Path(__file__).resolve().parents[2]
OUT = REPO / "agent_audit" / "data"


def _load_json(path: Path):
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def _write_json(path: Path, payload) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, sort_keys=True)
        handle.write("\n")


def _write_csv(path: Path, rows, fieldnames) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def _split_geometry(config: dict, n: int) -> list[dict]:
    pipe = config.get("pipeline", {})
    max_bars = max(1, int(pipe.get("max_bars", 300000)))
    active_bars = max(1, int(pipe.get("active_universe_bars", 250000)))
    audit_bars = max(0, int(pipe.get("historical_stress_audit_bars", 50000)))
    stage2_pct = max(1.0, min(100.0, float(pipe.get("active_stage2_pct", 50.0))))
    embargo = max(0, int(pipe.get("oos_embargo_bars", 0) or 0))
    overlap_pct = float(pipe.get("overlap_pct", 0.0) or 0.0)
    workflow_size = min(n, max_bars, max(1, active_bars + audit_bars))
    workflow_start = n - workflow_size
    workflow_end = n
    active_size = min(active_bars, workflow_size)
    active_start = workflow_end - active_size
    active_end = workflow_end
    audit_start = workflow_start
    audit_end = active_start
    stage2_size = max(1, int(math.ceil(active_size * stage2_pct / 100.0)))
    stage2_start = active_end - min(active_size, stage2_size)
    stage2_end = active_end
    warmup_bars = int(active_size * overlap_pct / 100.0)
    stage2_warmup_start = max(active_start, stage2_start - max(0, warmup_bars))
    wfv_bars = max(0, int(pipe.get("walk_forward_validation_bars", 50000) or 0))
    wfv_end = max(active_start, stage2_start - embargo)
    wfv_start = max(active_start, wfv_end - wfv_bars)
    windows = {
        "workflow_window": (workflow_start, workflow_end),
        "historical_stress_audit": (audit_start, audit_end),
        "active_universe": (active_start, active_end),
        "base_selection": (active_start, active_end),
        "walk_forward_validation": (wfv_start, wfv_end),
        "optimization_train": (stage2_start, stage2_end),
        "optimization_train_with_warmup": (stage2_warmup_start, stage2_end),
        "stage2_selection": (stage2_start, stage2_end),
        "clean_holdout": (stage2_end, stage2_end),
    }
    rows = []
    for name, (start, end) in windows.items():
        rows.append(
            {
                "input_bars": n,
                "window": name,
                "start": start,
                "end": end,
                "rows": max(0, end - start),
            }
        )
    rows.append(
        {
            "input_bars": n,
            "window": "wfv_gap_to_optimization_train",
            "start": wfv_end,
            "end": stage2_start,
            "rows": max(0, stage2_start - wfv_end),
        }
    )
    return rows


def build_split_geometry(config: dict) -> list[dict]:
    rows = []
    for n in (200000, 300000, 400000):
        rows.extend(_split_geometry(config, n))
    return rows


def summarize_tradeable_cells() -> dict:
    path = OUT / "ledger_tradeable_cells.csv"
    if not path.exists():
        return {"status": "missing", "path": str(path)}
    rows = []
    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            rows.append(row)

    def f(row, key):
        try:
            return float(row.get(key, "") or 0.0)
        except Exception:
            return 0.0

    val_trades = [int(float(r.get("val_trades", 0) or 0)) for r in rows]
    val_sharpe = [f(r, "val_sharpe") for r in rows]
    pbo = Counter(r.get("pbo_status", "") or "missing" for r in rows)
    by_symbol = Counter(r.get("symbol", "") for r in rows)
    low_trade = [r for r in rows if int(float(r.get("val_trades", 0) or 0)) < 20]
    high_sharpe = [r for r in rows if f(r, "val_sharpe") > 10.0]
    return {
        "status": "ok",
        "rows": len(rows),
        "symbols": len(by_symbol),
        "low_validation_trade_cells_lt_20": len(low_trade),
        "high_validation_sharpe_cells_gt_10": len(high_sharpe),
        "pbo_status_counts": dict(pbo),
        "val_trades_min": min(val_trades) if val_trades else 0,
        "val_trades_median": median(val_trades) if val_trades else 0,
        "val_trades_max": max(val_trades) if val_trades else 0,
        "val_sharpe_median": median(val_sharpe) if val_sharpe else 0.0,
        "val_sharpe_max": max(val_sharpe) if val_sharpe else 0.0,
        "top_symbols_by_cells": dict(by_symbol.most_common(10)),
    }


def summarize_signal_risk() -> dict:
    path = OUT / "signal_risk_events.csv"
    if not path.exists():
        return {"status": "missing", "path": str(path)}
    rows = []
    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        rows.extend(reader)

    actions = Counter(r.get("action", "") or "missing" for r in rows)
    executed_risks = []
    blocked_risks = []
    by_symbol_action = defaultdict(Counter)
    for row in rows:
        action = row.get("action", "") or "missing"
        symbol = row.get("symbol", "") or "unknown"
        by_symbol_action[symbol][action] += 1
        try:
            risk = float(row.get("actual_risk_pct", "") or "nan")
        except Exception:
            risk = float("nan")
        if math.isfinite(risk):
            if action == "EXECUTED":
                executed_risks.append(risk)
            elif action.startswith("BLOCKED") or action.startswith("SKIPPED"):
                blocked_risks.append(risk)

    def stats(values):
        values = sorted(values)
        if not values:
            return {}
        idx95 = min(len(values) - 1, int(math.floor(0.95 * (len(values) - 1))))
        return {
            "count": len(values),
            "min": values[0],
            "median": median(values),
            "p95": values[idx95],
            "max": values[-1],
        }

    symbol_rows = []
    for symbol, counter in by_symbol_action.items():
        symbol_rows.append(
            {
                "symbol": symbol,
                "executed": counter.get("EXECUTED", 0),
                "blocked_min_lot": counter.get("BLOCKED_MIN_LOT_EXCEEDS_CAP", 0),
                "blocked_supervisor": counter.get("BLOCKED_INSTITUTIONAL_SUPERVISOR", 0),
                "blocked_cycle_drift": counter.get("BLOCKED_CYCLE_DRIFT", 0),
                "total": sum(counter.values()),
            }
        )
    symbol_rows.sort(key=lambda r: (r["blocked_min_lot"], r["total"]), reverse=True)
    _write_csv(
        OUT / "deep_risk_symbol_actions.csv",
        symbol_rows,
        ["symbol", "executed", "blocked_min_lot", "blocked_supervisor", "blocked_cycle_drift", "total"],
    )
    return {
        "status": "ok",
        "rows": len(rows),
        "action_counts": dict(actions),
        "executed_actual_risk_pct": stats(executed_risks),
        "blocked_actual_risk_pct": stats(blocked_risks),
        "top_symbols_by_min_lot_blocks": symbol_rows[:10],
    }


def summarize_oos_manifests() -> dict:
    path = OUT / "oos_manifest_summary.csv"
    if not path.exists():
        return {"status": "missing", "path": str(path)}
    rows = []
    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        rows.extend(reader)
    size_bytes = []
    metric_key_counts = Counter()
    for row in rows:
        try:
            size_bytes.append(int(float(row.get("size_bytes", 0) or 0)))
        except Exception:
            pass
        try:
            keys = json.loads(row.get("top_metric_key_counts", "{}") or "{}")
        except Exception:
            keys = {}
        for key in keys:
            metric_key_counts[key] += 1
    return {
        "status": "ok",
        "manifest_count": len(rows),
        "size_bytes_total": sum(size_bytes),
        "size_bytes_median": median(size_bytes) if size_bytes else 0,
        "metric_key_manifest_presence": dict(metric_key_counts.most_common()),
    }


def build_metric_definition_rows() -> list[dict]:
    return [
        {
            "metric": "profit_factor_no_loss",
            "engine_definition": "inf if gross_loss==0 and gross_profit>0",
            "dashboard_definition": "0.0 if gross_loss==0",
            "impact": "zero-loss systems look best in engine and worst in dashboard",
        },
        {
            "metric": "expectancy",
            "engine_definition": "expected pips per trade",
            "dashboard_definition": "average realized dollars per trade",
            "impact": "same label, different units",
        },
        {
            "metric": "sharpe",
            "engine_definition": "per-bar equity returns annualized by timeframe bars/year",
            "dashboard_definition": "per-trade pnl annualized by sqrt(252)",
            "impact": "same label, different sampling basis",
        },
        {
            "metric": "bucket_drawdown",
            "engine_definition": "full backtest floating equity at bar close",
            "dashboard_definition": "realized trade equity curve only",
            "impact": "sub-bucket and dashboard reports can miss open-trade pain",
        },
    ]


def main() -> int:
    config = _load_json(REPO / "config.json")
    split_rows = build_split_geometry(config)
    _write_csv(
        OUT / "deep_split_geometry.csv",
        split_rows,
        ["input_bars", "window", "start", "end", "rows"],
    )
    metric_rows = build_metric_definition_rows()
    _write_csv(
        OUT / "deep_metric_definition_mismatches.csv",
        metric_rows,
        ["metric", "engine_definition", "dashboard_definition", "impact"],
    )
    summary = {
        "split_geometry": {
            "rows_written": len(split_rows),
            "config": {
                "production_split_mode": config.get("pipeline", {}).get("production_split_mode"),
                "max_bars": config.get("pipeline", {}).get("max_bars"),
                "active_universe_bars": config.get("pipeline", {}).get("active_universe_bars"),
                "historical_stress_audit_bars": config.get("pipeline", {}).get("historical_stress_audit_bars"),
                "active_stage2_pct": config.get("pipeline", {}).get("active_stage2_pct"),
                "walk_forward_validation_bars": config.get("pipeline", {}).get("walk_forward_validation_bars"),
                "oos_embargo_bars": config.get("pipeline", {}).get("oos_embargo_bars"),
                "clean_holdout_pct": config.get("pipeline", {}).get("clean_holdout_pct"),
                "threadspark_selection_surface": config.get("pipeline", {}).get("threadspark_selection_surface"),
            },
        },
        "tradeable_cells": summarize_tradeable_cells(),
        "signal_risk": summarize_signal_risk(),
        "oos_manifests": summarize_oos_manifests(),
        "metric_definition_mismatch_rows": len(metric_rows),
    }
    _write_json(OUT / "deep_consistency_summary.json", summary)
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
