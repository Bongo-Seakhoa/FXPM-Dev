"""Validate leak-clean meta-label predictors on regime-gated specialist labels.

This is a diagnostic/evaluation tool, not a scorer. It intentionally filters to
`status == "ok"` so skipped non-specialists cannot inflate a classifier result.
Outputs are per-cell top-pick decisions plus research summaries; no aggregate
metric is allowed to become an adoption verdict.
"""

from __future__ import annotations

import argparse
import json
import math
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any, Callable


DEFAULT_FEATURES = [
    "all_oos_sharpe",
    "all_oos_mean_return",
    "all_oos_sortino",
    "all_oos_neg_cvar5_loss",
    "val_reg_sharpe_approx",
    "val_reg_mean_r",
    "val_reg_calmar_ratio",
    "val_reg_profit_factor",
    "val_reg_return_pct",
    "val_reg_neg_dd",
]


def safe_float(value: object, default: float = 0.0) -> float:
    try:
        out = float(value)
    except (TypeError, ValueError):
        return float(default)
    return out if math.isfinite(out) else float(default)


def rank_auc(labels: list[bool], scores: list[float]) -> float | None:
    pairs = [
        (float(score), 1 if label else 0)
        for label, score in zip(labels, scores)
        if math.isfinite(float(score))
    ]
    pos = sum(label for _score, label in pairs)
    neg = len(pairs) - pos
    if pos == 0 or neg == 0:
        return None

    pairs.sort(key=lambda item: item[0])
    rank_sum = 0.0
    rank = 1
    idx = 0
    while idx < len(pairs):
        end = idx + 1
        while end < len(pairs) and pairs[end][0] == pairs[idx][0]:
            end += 1
        avg_rank = (rank + rank + (end - idx) - 1) / 2.0
        tied_pos = sum(pairs[pos_idx][1] for pos_idx in range(idx, end))
        rank_sum += tied_pos * avg_rank
        rank += end - idx
        idx = end
    return (rank_sum - pos * (pos + 1) / 2.0) / (pos * neg)


def load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8-sig"))


def load_manifest_records(evidence_dir: Path) -> dict[str, dict[str, Any]]:
    records: dict[str, dict[str, Any]] = {}
    for manifest_path in sorted(evidence_dir.glob("*_oos_manifest.json")):
        manifest = load_json(manifest_path)
        for record in manifest.get("records", []):
            candidate_id = str(record.get("candidate_id") or "")
            if candidate_id:
                records[candidate_id] = record
    return records


def get_regime_metric(row: dict[str, Any], record: dict[str, Any], split: str, key: str) -> float:
    regime = str(row.get("label_regime") or row.get("tuned_for_regime") or "").upper()
    metrics = (record.get(f"{split}_regime_metrics") or {}).get(regime) or {}
    if key == "sharpe_approx":
        return safe_float(metrics.get("sharpe_approx", metrics.get("sharpe_ratio", 0.0)))
    return safe_float(metrics.get(key, 0.0))


def feature_functions() -> dict[str, Callable[[dict[str, Any], dict[str, Any]], float]]:
    return {
        "all_oos_sharpe": lambda row, _record: safe_float(row.get("oos_sharpe")),
        "all_oos_mean_return": lambda row, _record: safe_float(row.get("oos_mean_return")),
        "all_oos_sortino": lambda row, _record: safe_float(row.get("oos_sortino")),
        "all_oos_neg_cvar5_loss": lambda row, _record: -safe_float(row.get("oos_cvar5_loss")),
        "val_reg_sharpe_approx": lambda row, record: get_regime_metric(row, record, "val", "sharpe_approx"),
        "val_reg_mean_r": lambda row, record: get_regime_metric(row, record, "val", "mean_r"),
        "val_reg_calmar_ratio": lambda row, record: get_regime_metric(row, record, "val", "calmar_ratio"),
        "val_reg_profit_factor": lambda row, record: get_regime_metric(row, record, "val", "profit_factor"),
        "val_reg_return_pct": lambda row, record: get_regime_metric(row, record, "val", "total_return_pct"),
        "val_reg_neg_dd": lambda row, record: -get_regime_metric(row, record, "val", "max_drawdown_pct"),
        "train_reg_mean_r": lambda row, record: get_regime_metric(row, record, "train", "mean_r"),
        "train_reg_calmar_ratio": lambda row, record: get_regime_metric(row, record, "train", "calmar_ratio"),
    }


def cell_key(row: dict[str, Any]) -> str:
    return "|".join([
        str(row.get("symbol") or ""),
        str(row.get("timeframe") or ""),
        str(row.get("label_regime") or row.get("tuned_for_regime") or ""),
    ])


def compact_candidate(row: dict[str, Any], score: float) -> dict[str, Any]:
    return {
        "candidate_id": row.get("candidate_id"),
        "symbol": row.get("symbol"),
        "timeframe": row.get("timeframe"),
        "regime": row.get("label_regime") or row.get("tuned_for_regime"),
        "strategy_name": row.get("strategy_name"),
        "score": round(float(score), 10),
        "carrier_label": bool(row.get("carrier_label")),
        "strict_carrier_label": bool(row.get("strict_carrier_label")),
        "full_history_return_pct": safe_float(row.get("full_history_return_pct")),
        "profit_factor": safe_float(row.get("profit_factor")),
        "max_dd_pct": safe_float(row.get("max_dd_pct")),
        "trades": int(row.get("trades") or 0),
    }


def evaluate(
    labels_path: Path,
    evidence_dir: Path,
    label_field: str,
    feature_names: list[str],
) -> dict[str, Any]:
    payload = load_json(labels_path)
    all_rows = list(payload.get("rows", []))
    rows = [row for row in all_rows if row.get("status") == "ok"]
    skipped = [row for row in all_rows if row.get("status") == "skipped"]
    records = load_manifest_records(evidence_dir)
    fns = feature_functions()

    missing = [name for name in feature_names if name not in fns]
    if missing:
        raise SystemExit(f"Unknown feature(s): {', '.join(missing)}")

    y = [bool(row.get(label_field)) for row in rows]
    aucs: dict[str, Any] = {}
    for feature in feature_names:
        scores = [fns[feature](row, records.get(str(row.get("candidate_id")), {})) for row in rows]
        per_symbol = {}
        for symbol in sorted({str(row.get("symbol")) for row in rows}):
            sym_rows = [row for row in rows if str(row.get("symbol")) == symbol]
            per_symbol[symbol] = rank_auc(
                [bool(row.get(label_field)) for row in sym_rows],
                [fns[feature](row, records.get(str(row.get("candidate_id")), {})) for row in sym_rows],
            )
        aucs[feature] = {
            "pooled_auc": rank_auc(y, scores),
            "per_symbol_auc": per_symbol,
        }

    rows_by_cell: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        rows_by_cell[cell_key(row)].append(row)

    feature_cell_summaries: dict[str, dict[str, int]] = {}
    cell_decisions: list[dict[str, Any]] = []
    for feature in feature_names:
        summary = {
            "cells": 0,
            "oracle_carrier_cells": 0,
            "picked_carrier": 0,
            "missed_carrier": 0,
            "no_carrier_in_pool": 0,
            "picked_strict_carrier": 0,
        }
        for key, cell_rows in sorted(rows_by_cell.items()):
            scored = [
                (fns[feature](row, records.get(str(row.get("candidate_id")), {})), row)
                for row in cell_rows
            ]
            score, picked = max(scored, key=lambda item: item[0])
            oracle_has_carrier = any(bool(row.get(label_field)) for row in cell_rows)
            picked_is_carrier = bool(picked.get(label_field))
            if not oracle_has_carrier:
                verdict = "NO_CARRIER_IN_POOL"
                summary["no_carrier_in_pool"] += 1
            elif picked_is_carrier:
                verdict = "PICKED_CARRIER"
                summary["picked_carrier"] += 1
            else:
                verdict = "MISSED_CARRIER"
                summary["missed_carrier"] += 1
            summary["cells"] += 1
            if oracle_has_carrier:
                summary["oracle_carrier_cells"] += 1
            if bool(picked.get("strict_carrier_label")):
                summary["picked_strict_carrier"] += 1
            cell_decisions.append({
                "feature": feature,
                "cell": key,
                "verdict": verdict,
                "oracle_has_carrier": bool(oracle_has_carrier),
                "picked": compact_candidate(picked, score),
            })
        feature_cell_summaries[feature] = summary

    return {
        "created_at": datetime.now().isoformat(),
        "guardrail": "Rows are filtered to status=='ok'; skipped non-specialists are excluded. Summaries are diagnostics only; scorer verdicts must remain per-cell.",
        "labels_path": str(labels_path),
        "evidence_dir": str(evidence_dir),
        "label_field": label_field,
        "row_counts": {
            "all_rows": len(all_rows),
            "ok_specialists": len(rows),
            "skipped_non_specialists": len(skipped),
            "positive_labels": sum(bool(row.get(label_field)) for row in rows),
        },
        "feature_auc": aucs,
        "feature_cell_summaries": feature_cell_summaries,
        "cell_decisions": cell_decisions,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Validate meta-label predictors on regime-gated specialist labels.")
    parser.add_argument("--labels", required=True, type=Path)
    parser.add_argument("--evidence-dir", required=True, type=Path)
    parser.add_argument("--label-field", default="carrier_label")
    parser.add_argument("--features", nargs="*", default=DEFAULT_FEATURES)
    parser.add_argument("--out", type=Path)
    args = parser.parse_args()

    result = evaluate(args.labels, args.evidence_dir, args.label_field, list(args.features))
    text = json.dumps(result, indent=2, sort_keys=True, default=str)
    if args.out:
        args.out.parent.mkdir(parents=True, exist_ok=True)
        tmp = args.out.with_suffix(args.out.suffix + ".tmp")
        tmp.write_text(text, encoding="utf-8")
        tmp.replace(args.out)
        print(args.out)
    else:
        print(text)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
