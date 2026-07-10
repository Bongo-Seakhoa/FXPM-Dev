"""Inspect and compare winners at the specialist-cell level.

The decision unit is one `symbol x timeframe x regime` specialist. This tool
does not replay symbol-level winners and does not compute performance means or
medians across cells. Counts are navigation metadata only.
"""

from __future__ import annotations

import argparse
import json
import math
import subprocess
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple


REPO = Path(__file__).resolve().parent.parent
TIMEFRAMES = ("M5", "M15", "M30", "H1", "H4", "D1")
REGIMES = ("TREND", "RANGE", "BREAKOUT", "CHOP")
PF_CAP = 10.0


def _git_head() -> str:
    try:
        return subprocess.check_output(
            ["git", "rev-parse", "--short", "HEAD"],
            cwd=str(REPO),
            text=True,
        ).strip()
    except Exception:
        return "unknown"


def load_json(path: Path) -> Dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8-sig"))


def write_json(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, allow_nan=False, default=str), encoding="utf-8")


def finite(value: Any, default: float = 0.0) -> float:
    try:
        out = float(value)
    except (TypeError, ValueError):
        return float(default)
    return out if math.isfinite(out) else float(default)


def sanitize_pf(value: Any) -> float:
    try:
        pf = float(value)
    except (TypeError, ValueError):
        return 0.0
    if math.isnan(pf):
        return 0.0
    if math.isinf(pf):
        return PF_CAP
    return max(0.0, min(PF_CAP, pf))


def cell_key(symbol: str, timeframe: str, regime: str) -> str:
    return f"{symbol}|{timeframe}|{regime}"


def is_no_trade(cell: Any) -> bool:
    if not isinstance(cell, dict):
        return True
    if (cell.get("strategy_name") or cell.get("strategy")) == "NO_TRADE":
        return True
    evidence = cell.get("validation_evidence")
    return isinstance(evidence, dict) and bool(evidence.get("no_trade"))


def iter_cells(
    ledger: Dict[str, Any],
    symbols: Iterable[str] | None = None,
) -> Iterable[Tuple[str, str, str, Dict[str, Any]]]:
    selected = list(symbols) if symbols else sorted(ledger.keys())
    for symbol in selected:
        record = ledger.get(symbol) or {}
        regime_configs = record.get("regime_configs") or {}
        for timeframe in TIMEFRAMES:
            tf_record = regime_configs.get(timeframe) or {}
            for regime in REGIMES:
                cell = tf_record.get(regime)
                if not isinstance(cell, dict):
                    cell = {
                        "strategy_name": "NO_TRADE",
                        "validation_evidence": {
                            "no_trade": True,
                            "validation_reason": "missing cell",
                        },
                    }
                yield symbol, timeframe, regime, cell


def nested_metrics(cell: Dict[str, Any], key: str) -> Dict[str, Any]:
    value = cell.get(key)
    return value if isinstance(value, dict) else {}


def historical_metrics(cell: Dict[str, Any]) -> Dict[str, Any]:
    audit = nested_metrics(cell, "historical_audit_metrics")
    metrics = audit.get("metrics")
    return metrics if isinstance(metrics, dict) else {}


def walk_forward_summary(cell: Dict[str, Any]) -> Dict[str, Any]:
    audit = nested_metrics(cell, "walk_forward_audit")
    summary = audit.get("summary")
    return summary if isinstance(summary, dict) else {}


def evidence_snapshot(cell: Dict[str, Any]) -> Dict[str, Any]:
    val = nested_metrics(cell, "val_metrics")
    train = nested_metrics(cell, "train_metrics")
    holdout = nested_metrics(cell, "holdout_metrics")
    hist = historical_metrics(cell)
    wf = walk_forward_summary(cell)
    audit = nested_metrics(cell, "historical_audit_metrics")
    validation = nested_metrics(cell, "validation_evidence")
    return {
        "train_trades": int(finite(train.get("total_trades"))),
        "train_return_pct": finite(train.get("total_return_pct")),
        "train_profit_factor": sanitize_pf(train.get("profit_factor")),
        "val_trades": int(finite(val.get("total_trades"))),
        "val_return_pct": finite(val.get("total_return_pct")),
        "val_profit_factor": sanitize_pf(val.get("profit_factor")),
        "val_max_dd_pct": finite(val.get("max_drawdown_pct")),
        "holdout_trades": int(finite(holdout.get("total_trades"))),
        "holdout_return_pct": finite(holdout.get("total_return_pct")),
        "holdout_profit_factor": sanitize_pf(holdout.get("profit_factor")),
        "holdout_max_dd_pct": finite(holdout.get("max_drawdown_pct")),
        "historical_trades": int(finite(hist.get("total_trades"))),
        "historical_return_pct": finite(hist.get("total_return_pct")),
        "historical_profit_factor": sanitize_pf(hist.get("profit_factor")),
        "historical_max_dd_pct": finite(hist.get("max_drawdown_pct")),
        "historical_severe_failure": bool(audit.get("severe_failure")),
        "walk_forward_windows": int(finite(nested_metrics(cell, "walk_forward_audit").get("window_count"))),
        "walk_forward_positive_windows": int(finite(wf.get("positive_windows"))),
        "walk_forward_avg_return_pct": finite(wf.get("avg_total_return_pct")),
        "walk_forward_avg_profit_factor": sanitize_pf(wf.get("avg_profit_factor")),
        "promotion_status": str(cell.get("promotion_status") or ""),
        "promotion_reasons": list(cell.get("promotion_reasons") or []),
        "selection_score_raw": finite(validation.get("selection_score_raw")),
        "validation_reason": str(validation.get("validation_reason") or ""),
    }


def quality_status(snapshot: Dict[str, Any]) -> Tuple[str, List[str]]:
    reasons: List[str] = []
    promotion_status = str(snapshot.get("promotion_status") or "").upper()

    val_trades = int(snapshot["val_trades"])
    holdout_trades = int(snapshot["holdout_trades"])
    hist_trades = int(snapshot["historical_trades"])
    wf_windows = int(snapshot["walk_forward_windows"])

    if promotion_status.startswith("BLOCK"):
        reasons.append("promotion_status=BLOCKED")
    if bool(snapshot["historical_severe_failure"]):
        reasons.append("historical_audit.severe_failure")
    if hist_trades >= 5 and snapshot["historical_profit_factor"] < 1.0:
        reasons.append("historical PF < 1.0")
    if hist_trades >= 5 and snapshot["historical_return_pct"] < 0.0:
        reasons.append("historical return < 0")
    if snapshot["historical_max_dd_pct"] > 40.0:
        reasons.append("historical DD > 40%")
    if holdout_trades >= 5 and snapshot["holdout_profit_factor"] < 1.0:
        reasons.append("holdout PF < 1.0")
    if holdout_trades >= 5 and snapshot["holdout_return_pct"] < 0.0:
        reasons.append("holdout return < 0")
    if val_trades >= 5 and snapshot["val_profit_factor"] < 1.0:
        reasons.append("validation PF < 1.0")
    if val_trades >= 5 and snapshot["val_return_pct"] < 0.0:
        reasons.append("validation return < 0")

    if reasons:
        return "FAIL", reasons

    if hist_trades >= 5:
        hist_pass = (
            snapshot["historical_profit_factor"] >= 1.15
            and snapshot["historical_return_pct"] > 0.0
            and snapshot["historical_max_dd_pct"] <= 35.0
        )
        holdout_pass = (
            holdout_trades >= 5
            and snapshot["holdout_profit_factor"] >= 1.0
            and snapshot["holdout_return_pct"] >= 0.0
        )
        wf_pass = wf_windows == 0 or (
            snapshot["walk_forward_positive_windows"] >= max(1, math.ceil(wf_windows * 0.5))
            and snapshot["walk_forward_avg_profit_factor"] >= 1.0
        )
        if hist_pass and holdout_pass and wf_pass:
            return "PASS_STRICT", ["per-cell historical/holdout/WF evidence passed"]
        if snapshot["historical_profit_factor"] >= 1.0 and snapshot["historical_return_pct"] >= 0.0:
            return "PASS_THIN", ["positive historical evidence but thin/partial confirmation"]

    if val_trades >= 5 and snapshot["val_profit_factor"] >= 1.0 and snapshot["val_return_pct"] >= 0.0:
        return "INSUFFICIENT_EVIDENCE", ["selection surface positive, independent cell evidence missing/thin"]
    return "INSUFFICIENT_EVIDENCE", ["not enough per-cell evidence to classify"]


def candidate_signature(row: Dict[str, Any]) -> Tuple[Any, ...]:
    params = row.get("parameters") or {}
    try:
        params_blob = json.dumps(params, sort_keys=True, separators=(",", ":"))
    except TypeError:
        params_blob = str(params)
    return (
        row.get("strategy"),
        row.get("timeframe"),
        row.get("regime"),
        params_blob,
    )


def classify_ledger_cell(symbol: str, timeframe: str, regime: str, cell: Dict[str, Any]) -> Dict[str, Any]:
    key = cell_key(symbol, timeframe, regime)
    if is_no_trade(cell):
        evidence = nested_metrics(cell, "validation_evidence")
        return {
            "cell_key": key,
            "symbol": symbol,
            "timeframe": timeframe,
            "regime": regime,
            "status": "NO_TRADE_ABSTAINED",
            "strategy": "NO_TRADE",
            "reason": evidence.get("validation_reason", "no trade"),
        }

    snapshot = evidence_snapshot(cell)
    status, reasons = quality_status(snapshot)
    return {
        "cell_key": key,
        "symbol": symbol,
        "timeframe": timeframe,
        "regime": regime,
        "status": status,
        "strategy": cell.get("strategy_name") or cell.get("strategy"),
        "parameters": cell.get("parameters") or {},
        **snapshot,
        "reasons": reasons,
    }


def inspect_ledger(ledger_path: Path, symbols: List[str] | None = None, label: str = "") -> Dict[str, Any]:
    ledger = load_json(ledger_path)
    rows = [
        classify_ledger_cell(symbol, timeframe, regime, cell)
        for symbol, timeframe, regime, cell in iter_cells(ledger, symbols)
    ]
    statuses = sorted({str(row.get("status")) for row in rows})
    return {
        "label": label or ledger_path.stem,
        "ledger": str(ledger_path),
        "engine_commit": _git_head(),
        "decision_scope": "symbol_timeframe_regime_specialist_only",
        "aggregate_decision_allowed": False,
        "symbols": list(symbols) if symbols else sorted(ledger.keys()),
        "rows": rows,
        "navigation_counts": {
            "total_cells": len(rows),
            **{status: sum(1 for row in rows if row.get("status") == status) for status in statuses},
        },
    }


def compare_cell(base: Dict[str, Any], cand: Dict[str, Any]) -> Dict[str, Any]:
    key = cand.get("cell_key") or base.get("cell_key")
    b_status = str(base.get("status", "MISSING"))
    c_status = str(cand.get("status", "MISSING"))
    out: Dict[str, Any] = {
        "cell_key": key,
        "symbol": cand.get("symbol") or base.get("symbol"),
        "timeframe": cand.get("timeframe") or base.get("timeframe"),
        "regime": cand.get("regime") or base.get("regime"),
        "baseline_status": b_status,
        "candidate_status": c_status,
    }

    good_statuses = {"PASS_STRICT", "PASS_THIN"}
    bad_statuses = {"FAIL"}
    insufficient = {"INSUFFICIENT_EVIDENCE", "MISSING"}

    if b_status == "NO_TRADE_ABSTAINED" and c_status == "NO_TRADE_ABSTAINED":
        out["verdict"] = "NO_TRADE_BOTH"
    elif b_status in good_statuses and c_status in good_statuses:
        out["verdict"] = (
            "RETAINED_SAME_GOOD_CARRIER"
            if candidate_signature(base) == candidate_signature(cand)
            else "REPLACED_WITH_GOOD_CARRIER"
        )
    elif b_status in good_statuses and c_status not in good_statuses:
        out["verdict"] = "LOST_GOOD_CARRIER"
    elif b_status not in good_statuses and c_status in good_statuses:
        out["verdict"] = "GAINED_GOOD_CARRIER"
    elif b_status in bad_statuses and c_status == "NO_TRADE_ABSTAINED":
        out["verdict"] = "DROPPED_BAD_CARRIER"
    elif c_status in bad_statuses and b_status not in bad_statuses:
        out["verdict"] = "NEW_BAD_CARRIER"
    elif c_status in bad_statuses and b_status in bad_statuses:
        out["verdict"] = "RETAINED_BAD_CARRIER"
    elif c_status in insufficient or b_status in insufficient:
        out["verdict"] = "INSUFFICIENT_EVIDENCE"
    else:
        out["verdict"] = "CELL_REVIEW"

    for prefix, row in (("baseline", base), ("candidate", cand)):
        out[f"{prefix}_strategy"] = row.get("strategy")
        out[f"{prefix}_historical_return_pct"] = row.get("historical_return_pct")
        out[f"{prefix}_historical_profit_factor"] = row.get("historical_profit_factor")
        out[f"{prefix}_historical_max_dd_pct"] = row.get("historical_max_dd_pct")
        out[f"{prefix}_historical_trades"] = row.get("historical_trades")
        out[f"{prefix}_holdout_trades"] = row.get("holdout_trades")
        out[f"{prefix}_holdout_profit_factor"] = row.get("holdout_profit_factor")
        out[f"{prefix}_reasons"] = row.get("reasons") or row.get("reason")
    return out


def compare_records(base: Dict[str, Any], cand: Dict[str, Any]) -> Dict[str, Any]:
    errors: List[str] = []
    if base.get("decision_scope") != cand.get("decision_scope"):
        errors.append(
            f"decision_scope mismatch: {base.get('decision_scope')} vs {cand.get('decision_scope')}"
        )
    bmap = {row["cell_key"]: row for row in base.get("rows", []) if isinstance(row, dict) and row.get("cell_key")}
    cmap = {row["cell_key"]: row for row in cand.get("rows", []) if isinstance(row, dict) and row.get("cell_key")}
    if set(bmap) != set(cmap):
        errors.append(
            "cell set mismatch: "
            f"baseline_only={sorted(set(bmap) - set(cmap))[:10]} "
            f"candidate_only={sorted(set(cmap) - set(bmap))[:10]}"
        )
    comparisons = [
        compare_cell(
            bmap.get(key, {"cell_key": key, "status": "MISSING"}),
            cmap.get(key, {"cell_key": key, "status": "MISSING"}),
        )
        for key in sorted(set(bmap) | set(cmap))
    ]
    verdicts = sorted({str(row.get("verdict")) for row in comparisons})
    blockers = [
        row for row in comparisons
        if row.get("verdict") in {
            "LOST_GOOD_CARRIER",
            "NEW_BAD_CARRIER",
            "RETAINED_BAD_CARRIER",
            "INSUFFICIENT_EVIDENCE",
        }
    ]
    return {
        "decision_scope": "symbol_timeframe_regime_specialist_only",
        "aggregate_decision_allowed": False,
        "baseline_label": base.get("label"),
        "candidate_label": cand.get("label"),
        "errors": errors,
        "cell_comparisons": comparisons,
        "blockers": blockers,
        "navigation_counts": {
            "total_cells": len(comparisons),
            **{verdict: sum(1 for row in comparisons if row.get("verdict") == verdict) for verdict in verdicts},
            "blockers": len(blockers),
        },
    }


def _promotion_cell_key(row: Dict[str, Any]) -> str:
    return cell_key(str(row.get("symbol") or ""), str(row.get("timeframe") or ""), str(row.get("regime") or ""))


def _promotion_verdict(row: Dict[str, Any]) -> str:
    return str(row.get("final_verdict") or row.get("verdict") or "").upper()


def promotion_safety_summary(cell_record: Dict[str, Any], promotion_record: Dict[str, Any]) -> Dict[str, Any]:
    """Cross-check promotion-readiness verdicts against specialist-cell evidence.

    Counts are navigation metadata only. The important invariant is per-cell:
    a final PROMOTE must not point at a specialist classified as FAIL,
    INSUFFICIENT_EVIDENCE, or missing from the cell audit.
    """
    cells = {
        str(row.get("cell_key")): row
        for row in cell_record.get("rows", [])
        if isinstance(row, dict) and row.get("cell_key")
    }
    unsafe_promotes: List[Dict[str, Any]] = []
    fail_not_reject: List[Dict[str, Any]] = []
    insufficient_not_reject: List[Dict[str, Any]] = []
    final_promotes: List[Dict[str, Any]] = []

    for row in promotion_record.get("rows", []):
        if not isinstance(row, dict):
            continue
        key = _promotion_cell_key(row)
        verdict = _promotion_verdict(row)
        cell = cells.get(key)
        status = str(cell.get("status") if isinstance(cell, dict) else "MISSING")
        entry = {
            "cell_key": key,
            "final_verdict": verdict,
            "cell_status": status,
            "strategy": row.get("strategy_name") or (cell or {}).get("strategy"),
            "promotion_status": row.get("promotion_status") or (cell or {}).get("promotion_status"),
            "why": row.get("why"),
            "gate_failure_codes": row.get("gate_failure_codes") or [],
            "cell_reasons": (cell or {}).get("reasons") or (cell or {}).get("reason"),
        }
        if verdict == "PROMOTE":
            final_promotes.append(entry)
            if status in {"FAIL", "INSUFFICIENT_EVIDENCE", "MISSING"}:
                unsafe_promotes.append(entry)
        if verdict != "REJECT" and status == "FAIL":
            fail_not_reject.append(entry)
        if verdict != "REJECT" and status == "INSUFFICIENT_EVIDENCE":
            insufficient_not_reject.append(entry)

    return {
        "decision_scope": "symbol_timeframe_regime_specialist_only",
        "aggregate_decision_allowed": False,
        "promotion_label": promotion_record.get("label") or promotion_record.get("stress_mode") or "",
        "cell_label": cell_record.get("label") or "",
        "promotion_counts": promotion_record.get("counts") or {},
        "navigation_counts": {
            "final_promotes": len(final_promotes),
            "unsafe_promotes": len(unsafe_promotes),
            "fail_not_reject": len(fail_not_reject),
            "insufficient_not_reject": len(insufficient_not_reject),
        },
        "final_promotes": final_promotes,
        "unsafe_promotes": unsafe_promotes,
        "fail_not_reject": fail_not_reject,
        "insufficient_not_reject": insufficient_not_reject,
    }


def print_record(record: Dict[str, Any]) -> None:
    print(f"\n=== SPECIALIST-CELL LEDGER INSPECTION [{record.get('label')}] ===")
    print("Decision scope: symbol_timeframe_regime_specialist_only. Counts are navigation only.")
    cols = [
        "cell_key", "status", "strategy", "historical_trades", "historical_return_pct",
        "historical_profit_factor", "historical_max_dd_pct", "holdout_trades",
        "holdout_profit_factor", "promotion_status", "reasons",
    ]
    print("\t".join(cols))
    for row in record.get("rows", []):
        print("\t".join(str(row.get(col, "")) for col in cols))
    print("\n-- navigation counts, not an adoption verdict --")
    for key, value in (record.get("navigation_counts") or {}).items():
        print(f"  {key}: {value}")


def print_compare(compare: Dict[str, Any]) -> None:
    if compare.get("errors"):
        print("\n=== INVALID SPECIALIST-CELL COMPARISON ===")
        for err in compare["errors"]:
            print(f"ERROR: {err}")
        return
    print(
        f"\n=== SPECIALIST-CELL COMPARE: {compare.get('baseline_label')} -> "
        f"{compare.get('candidate_label')} ==="
    )
    print("Decision scope: symbol_timeframe_regime_specialist_only. Do not accept/reject from counts.")
    cols = [
        "cell_key", "verdict", "baseline_status", "candidate_status",
        "baseline_strategy", "candidate_strategy", "baseline_historical_return_pct",
        "candidate_historical_return_pct", "baseline_historical_profit_factor",
        "candidate_historical_profit_factor", "baseline_historical_max_dd_pct",
        "candidate_historical_max_dd_pct", "baseline_reasons", "candidate_reasons",
    ]
    print("\t".join(cols))
    for row in compare.get("cell_comparisons", []):
        print("\t".join(str(row.get(col, "")) for col in cols))
    print("\n-- blockers: inspect each one; counts are not a verdict --")
    for row in compare.get("blockers", []):
        print(f"  {row.get('cell_key')}: {row.get('verdict')}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Inspect/compare specialists per symbol/timeframe/regime cell from ledger evidence."
    )
    parser.add_argument("--ledger", help="pm_configs ledger to inspect.")
    parser.add_argument("--label", default="", help="Label for output.")
    parser.add_argument("--symbols", nargs="*", default=None)
    parser.add_argument("--out", help="Write inspection JSON here.")
    parser.add_argument("--compare", nargs=2, metavar=("BASELINE_CELL_JSON", "CANDIDATE_CELL_JSON"))
    parser.add_argument("--compare-out", help="Write comparison JSON here.")
    parser.add_argument("--promotion-review", help="Promotion-review JSON to cross-check against cell evidence.")
    parser.add_argument("--promotion-safety-out", help="Write promotion/cell safety summary JSON here.")
    args = parser.parse_args()

    if args.compare:
        base = load_json(Path(args.compare[0]))
        cand = load_json(Path(args.compare[1]))
        comparison = compare_records(base, cand)
        print_compare(comparison)
        if args.compare_out:
            write_json(Path(args.compare_out), comparison)
            print(f"\nwrote {args.compare_out}")
        if comparison.get("errors"):
            raise SystemExit(2)
        return

    if not args.ledger:
        raise SystemExit("--ledger is required unless --compare is used")
    record = inspect_ledger(Path(args.ledger), symbols=args.symbols, label=args.label)
    print_record(record)
    if args.out:
        write_json(Path(args.out), record)
        print(f"\nwrote {args.out}")
    if args.promotion_review:
        promotion = load_json(Path(args.promotion_review))
        safety = promotion_safety_summary(record, promotion)
        print("\n-- promotion/cell safety cross-check; counts are navigation only --")
        for key, value in safety["navigation_counts"].items():
            print(f"  {key}: {value}")
        if safety["unsafe_promotes"]:
            print("  unsafe PROMOTE cells:")
            for row in safety["unsafe_promotes"]:
                print(f"    {row['cell_key']}: {row.get('cell_status')} {row.get('cell_reasons')}")
        if args.promotion_safety_out:
            write_json(Path(args.promotion_safety_out), safety)
            print(f"\nwrote {args.promotion_safety_out}")


if __name__ == "__main__":
    main()
