"""Read-only clean-holdout sufficiency diagnostics.

This tool does not promote or reject anything. It explains whether the clean
holdout gate is failing because candidates are bad, because evidence is too
thin, or because the ledger/review status disagree.
"""

import argparse
import json
import math
import sys
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional


REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))
sys.path.insert(0, str(REPO / "tools"))

from benchmark import _git_head  # noqa: E402


DEFAULT_PROMOTION = REPO / "pm_outputs" / "experiments" / "production_live_final_verdict_promotion.json"
DEFAULT_LEDGER = REPO / "pm_configs.json"


def _load_json(path: Path) -> Dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _as_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return default


def _as_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except Exception:
        return default


def _has_float(row: Dict[str, Any], key: str) -> bool:
    try:
        float(row.get(key))
    except (TypeError, ValueError):
        return False
    return row.get(key) is not None


def _counts(values: Iterable[Any]) -> Dict[str, int]:
    out: Dict[str, int] = {}
    for value in values:
        key = str(value)
        out[key] = out.get(key, 0) + 1
    return dict(sorted(out.items(), key=lambda item: item[0]))


def _window_rows(ledger: Dict[str, Any], symbol: str, window: str) -> int:
    entry = ledger.get(symbol)
    if not isinstance(entry, dict):
        return 0
    meta = entry.get("data_window_meta") if isinstance(entry.get("data_window_meta"), dict) else {}
    windows = meta.get("windows") if isinstance(meta.get("windows"), dict) else {}
    win = windows.get(window) if isinstance(windows.get(window), dict) else {}
    return _as_int(win.get("rows"), 0)


def _is_pbo_fail(row: Dict[str, Any]) -> bool:
    pbo = row.get("pbo_timeframe")
    return pbo is not None and _as_float(pbo) > 0.5


def _passes_holdout_at(row: Dict[str, Any], min_trades: int) -> bool:
    trades = _as_int(row.get("holdout_trades"), 0)
    ret = _as_float(row.get("holdout_return_pct"), 0.0)
    pf = _as_float(row.get("holdout_pf"), 0.0)
    return trades >= min_trades and ret > 0.0 and pf >= 1.0


def _fails_holdout_at(row: Dict[str, Any], min_trades: int) -> bool:
    trades = _as_int(row.get("holdout_trades"), 0)
    if trades < min_trades:
        return False
    return not _passes_holdout_at(row, min_trades)


def _gate_clean_except_holdout(row: Dict[str, Any]) -> bool:
    gate_codes = {
        str(code)
        for code in (row.get("gate_failure_codes") or [])
        if str(code)
    }
    non_holdout_blockers = {
        code for code in gate_codes
        if not code.startswith("holdout_")
    }
    if non_holdout_blockers:
        return False
    promotion_status = str(row.get("promotion_status") or "").upper()
    if promotion_status.startswith("BLOCK"):
        return False
    val_trades = _as_int(row.get("val_trades"), 0)
    if val_trades >= 5 and (
        (_has_float(row, "val_pf") and _as_float(row.get("val_pf"), 0.0) < 1.0)
        or (_has_float(row, "val_return_pct") and _as_float(row.get("val_return_pct"), 0.0) < 0.0)
    ):
        return False
    historical_trades = _as_int(row.get("historical_trades"), 0)
    if historical_trades >= 5 and (
        (_has_float(row, "historical_pf") and _as_float(row.get("historical_pf"), 0.0) < 1.0)
        or (
            _has_float(row, "historical_return_pct")
            and _as_float(row.get("historical_return_pct"), 0.0) < 0.0
        )
        or (
            _has_float(row, "historical_max_dd_pct")
            and _as_float(row.get("historical_max_dd_pct"), 0.0) > 40.0
        )
    ):
        return False
    return (
        row.get("stress") == "SURVIVE"
        and row.get("historical_status") != "FAIL"
        and not _is_pbo_fail(row)
        and row.get("pbo_timeframe") is not None
        and row.get("provenance") in ("fx_historical_spread", "broker_spread", "")
    )


def _group_holdout(rows: List[Dict[str, Any]], key: str) -> Dict[str, Dict[str, int]]:
    grouped: Dict[str, Dict[str, int]] = {}
    for row in rows:
        group = str(row.get(key, "") or "")
        status = str(row.get("holdout_status", "") or "")
        grouped.setdefault(group, {})
        grouped[group][status] = grouped[group].get(status, 0) + 1
    return dict(sorted(grouped.items(), key=lambda item: item[0]))


def _trade_rate_rows(rows: List[Dict[str, Any]], ledger: Dict[str, Any]) -> List[Dict[str, Any]]:
    out = []
    for row in rows:
        symbol = str(row.get("symbol", "") or "")
        val_rows = _window_rows(ledger, symbol, "stage2_training_selection")
        holdout_rows = _window_rows(ledger, symbol, "clean_holdout")
        val_trades = _as_int(row.get("val_trades"), 0)
        holdout_trades = _as_int(row.get("holdout_trades"), 0)
        out.append({
            "symbol": symbol,
            "timeframe": row.get("timeframe"),
            "regime": row.get("regime"),
            "strategy_name": row.get("strategy_name"),
            "val_trades": val_trades,
            "holdout_trades": holdout_trades,
            "stage2_training_rows_m5": val_rows,
            "clean_holdout_rows_m5": holdout_rows,
            "val_trades_per_10k_m5": round((val_trades / val_rows) * 10000, 4) if val_rows else None,
            "holdout_trades_per_10k_m5": round((holdout_trades / holdout_rows) * 10000, 4) if holdout_rows else None,
            "holdout_status": row.get("holdout_status"),
            "final_verdict": row.get("final_verdict"),
        })
    return out


def _median(values: Iterable[float]) -> Optional[float]:
    clean = sorted(float(value) for value in values if value is not None and math.isfinite(float(value)))
    if not clean:
        return None
    mid = len(clean) // 2
    if len(clean) % 2:
        return round(clean[mid], 6)
    return round((clean[mid - 1] + clean[mid]) / 2.0, 6)


def _rate_context(rows: List[Dict[str, Any]], ledger: Dict[str, Any]) -> Dict[str, Any]:
    holdout_to_training = []
    holdout_to_stage2 = []
    trade_rate_ratios = []
    for row in rows:
        symbol = str(row.get("symbol", "") or "")
        val_rows = _window_rows(ledger, symbol, "stage2_training_selection")
        holdout_rows = _window_rows(ledger, symbol, "clean_holdout")
        total_rows = val_rows + holdout_rows
        val_trades = _as_int(row.get("val_trades"), 0)
        holdout_trades = _as_int(row.get("holdout_trades"), 0)
        if val_rows > 0 and holdout_rows > 0:
            holdout_to_training.append(holdout_rows / val_rows)
        if total_rows > 0 and holdout_rows > 0:
            holdout_to_stage2.append(holdout_rows / total_rows)
        if val_rows > 0 and holdout_rows > 0 and val_trades > 0 and holdout_trades > 0:
            val_rate = val_trades / val_rows
            holdout_rate = holdout_trades / holdout_rows
            if val_rate > 0:
                trade_rate_ratios.append(holdout_rate / val_rate)
    return {
        "median_clean_holdout_to_training_bar_ratio": _median(holdout_to_training),
        "median_clean_holdout_to_stage2_bar_ratio": _median(holdout_to_stage2),
        "median_holdout_to_validation_trade_rate_ratio": _median(trade_rate_ratios),
        "trade_rate_ratio_sample": len(trade_rate_ratios),
    }


def _is_positive_holdout(row: Dict[str, Any]) -> bool:
    return (
        _as_float(row.get("holdout_return_pct"), 0.0) > 0.0
        and _as_float(row.get("holdout_pf"), 0.0) >= 1.0
    )


def _trade_count_anchor_projection(rows: List[Dict[str, Any]],
                                   ledger: Dict[str, Any],
                                   min_trades: int) -> Dict[str, Any]:
    projected = []
    for row in rows:
        symbol = str(row.get("symbol", "") or "")
        val_rows = _window_rows(ledger, symbol, "stage2_training_selection")
        holdout_rows = _window_rows(ledger, symbol, "clean_holdout")
        total_rows = val_rows + holdout_rows
        val_trades = _as_int(row.get("val_trades"), 0)
        holdout_trades = _as_int(row.get("holdout_trades"), 0)

        rate_source = "current_holdout" if holdout_trades > 0 and holdout_rows > 0 else "validation"
        if holdout_trades > 0 and holdout_rows > 0:
            trade_rate = holdout_trades / holdout_rows
        elif val_trades > 0 and val_rows > 0:
            trade_rate = val_trades / val_rows
        else:
            trade_rate = 0.0
            rate_source = "unavailable"

        required_rows = math.ceil(min_trades / trade_rate) if trade_rate > 0 else None
        extra_rows_if_flat = (
            max(0, required_rows - holdout_rows)
            if required_rows is not None else None
        )
        projected.append({
            "symbol": symbol,
            "timeframe": row.get("timeframe"),
            "regime": row.get("regime"),
            "strategy_name": row.get("strategy_name"),
            "holdout_status": row.get("holdout_status"),
            "holdout_trades": holdout_trades,
            "val_trades": val_trades,
            "gate_clean_except_holdout": _gate_clean_except_holdout(row),
            "current_holdout_positive": _is_positive_holdout(row),
            "rate_source": rate_source,
            "required_holdout_rows_m5": required_rows,
            "current_holdout_rows_m5": holdout_rows,
            "stage2_training_rows_m5": val_rows,
            "stage2_total_rows_m5": total_rows,
            "feasible_within_stage2": bool(required_rows is not None and required_rows <= total_rows),
            "extra_rows_if_flat_tail_expansion": extra_rows_if_flat,
            "extra_training_loss_pct_if_flat": (
                round((extra_rows_if_flat / val_rows) * 100.0, 4)
                if extra_rows_if_flat is not None and val_rows > 0 else None
            ),
        })

    insufficient = [row for row in projected if row["holdout_trades"] < min_trades]
    feasible = [row for row in insufficient if row["feasible_within_stage2"]]
    clean_feasible = [row for row in feasible if row["gate_clean_except_holdout"]]
    clean_positive_feasible = [
        row for row in clean_feasible if row["current_holdout_positive"]
    ]
    extra_loss = [
        row["extra_training_loss_pct_if_flat"]
        for row in clean_feasible
        if row["extra_training_loss_pct_if_flat"] is not None
    ]
    required_ratios = [
        (row["required_holdout_rows_m5"] / row["stage2_training_rows_m5"]) * 100.0
        for row in clean_feasible
        if row["required_holdout_rows_m5"] is not None and row["stage2_training_rows_m5"] > 0
    ]
    return {
        "method": "trade_count_anchored_holdout_projection",
        "min_trades": min_trades,
        "meaning": (
            "Projects whether each specialist could reach the holdout trade-count floor "
            "by reserving enough candidate-specific tail evidence. This is sufficiency-only; "
            "it does not assert the additional trades would be profitable."
        ),
        "insufficient_rows": len(insufficient),
        "insufficient_feasible_within_stage2": len(feasible),
        "gate_clean_except_holdout_feasible": len(clean_feasible),
        "gate_clean_positive_thin_feasible": len(clean_positive_feasible),
        "median_required_holdout_to_training_pct_for_clean_feasible": _median(required_ratios),
        "median_extra_training_loss_pct_if_done_as_flat_tail": _median(extra_loss),
        "rows": sorted(
            clean_feasible,
            key=lambda row: (
                row["extra_training_loss_pct_if_flat"] is None,
                row["extra_training_loss_pct_if_flat"] or 0.0,
                row["symbol"],
                row["timeframe"],
                row["regime"],
            ),
        )[:100],
    }


def _purged_kfold_projection(rows: List[Dict[str, Any]], min_trades: int, folds: int) -> Dict[str, Any]:
    projected = []
    folds = max(2, int(folds))
    for row in rows:
        val_trades = _as_int(row.get("val_trades"), 0)
        holdout_trades = _as_int(row.get("holdout_trades"), 0)
        total_stage2_trades = val_trades + holdout_trades
        projected.append({
            "symbol": row.get("symbol"),
            "timeframe": row.get("timeframe"),
            "regime": row.get("regime"),
            "strategy_name": row.get("strategy_name"),
            "holdout_status": row.get("holdout_status"),
            "holdout_trades": holdout_trades,
            "val_trades": val_trades,
            "stage2_trade_evidence": total_stage2_trades,
            "mean_trades_per_fold": round(total_stage2_trades / folds, 4),
            "gate_clean_except_holdout": _gate_clean_except_holdout(row),
            "current_holdout_positive": _is_positive_holdout(row),
            "projected_sufficient_aggregate_oos": total_stage2_trades >= min_trades,
        })

    insufficient = [
        row for row in projected if _as_int(row.get("holdout_trades"), 0) < min_trades
    ]
    sufficient = [row for row in insufficient if row["projected_sufficient_aggregate_oos"]]
    clean_sufficient = [row for row in sufficient if row["gate_clean_except_holdout"]]
    clean_positive_sufficient = [
        row for row in clean_sufficient if row["current_holdout_positive"]
    ]
    return {
        "method": "purged_embargoed_kfold_oos_projection",
        "min_trades": min_trades,
        "folds": folds,
        "meaning": (
            "Projects whether aggregated purged-k-fold OOS evidence could meet the trade-count "
            "floor without a permanently reserved freshest tail. This is sufficiency-only; "
            "real implementation must recompute OOS metrics fold-by-fold with purging/embargo."
        ),
        "permanent_train_data_loss_pct": 0.0,
        "approx_runtime_multiplier": folds,
        "insufficient_rows": len(insufficient),
        "insufficient_with_stage2_trade_evidence": len(sufficient),
        "gate_clean_except_holdout_sufficient": len(clean_sufficient),
        "gate_clean_positive_thin_sufficient": len(clean_positive_sufficient),
        "rows": sorted(
            clean_sufficient,
            key=lambda row: (
                -row["stage2_trade_evidence"],
                row["symbol"],
                row["timeframe"],
                row["regime"],
            ),
        )[:100],
    }


def _construction_projection(rows: List[Dict[str, Any]],
                             ledger: Dict[str, Any],
                             min_trades: int,
                             folds: int) -> Dict[str, Any]:
    current_clean = len([
        row for row in rows
        if _gate_clean_except_holdout(row) and _passes_holdout_at(row, min_trades)
    ])
    anchor = _trade_count_anchor_projection(rows, ledger, min_trades)
    kfold = _purged_kfold_projection(rows, min_trades, folds)
    return {
        "projection_scope": "sufficiency_only_not_profitability",
        "rate_context": _rate_context(rows, ledger),
        "current_clean_holdout": {
            "min_trades": min_trades,
            "gate_clean_except_holdout_promotable": current_clean,
        },
        "headroom_estimate_if_positive_thin_rows_hold_up": {
            "current_promotable": current_clean,
            "trade_count_anchor_ceiling": current_clean + int(anchor["gate_clean_positive_thin_feasible"]),
            "purged_kfold_ceiling": current_clean + int(kfold["gate_clean_positive_thin_sufficient"]),
            "note": "Ceiling assumes currently positive-but-thin rows remain positive after added/purged OOS evidence.",
        },
        "trade_count_anchor": anchor,
        "purged_kfold": kfold,
    }


def build_diagnostics(promotion: Dict[str, Any],
                      ledger: Dict[str, Any],
                      max_threshold: int = 10,
                      projection_min_trades: int = 5,
                      projection_folds: int = 5) -> Dict[str, Any]:
    rows = [row for row in promotion.get("rows", []) if isinstance(row, dict)]
    tradeable = [row for row in rows if row.get("strategy_name") not in ("", "NO_TRADE")]
    clean_except_holdout = [row for row in tradeable if _gate_clean_except_holdout(row)]

    threshold_sensitivity = []
    for threshold in range(1, max(1, max_threshold) + 1):
        insufficient = [row for row in tradeable if _as_int(row.get("holdout_trades"), 0) < threshold]
        holdout_pass = [row for row in tradeable if _passes_holdout_at(row, threshold)]
        holdout_fail = [row for row in tradeable if _fails_holdout_at(row, threshold)]
        clean_promotable = [
            row for row in clean_except_holdout
            if _passes_holdout_at(row, threshold)
        ]
        threshold_sensitivity.append({
            "min_trades": threshold,
            "holdout_pass": len(holdout_pass),
            "holdout_fail": len(holdout_fail),
            "holdout_insufficient": len(insufficient),
            "gate_clean_except_holdout_promotable": len(clean_promotable),
        })

    mismatches = [
        {
            "symbol": row.get("symbol"),
            "timeframe": row.get("timeframe"),
            "regime": row.get("regime"),
            "strategy_name": row.get("strategy_name"),
            "promotion_status": row.get("promotion_status"),
            "holdout_status": row.get("holdout_status"),
            "holdout_trades": row.get("holdout_trades"),
            "final_verdict": row.get("final_verdict"),
            "why": row.get("why"),
        }
        for row in tradeable
        if row.get("promotion_status") == "PROMOTED" and row.get("holdout_status") == "INSUFFICIENT"
    ]

    positive_but_thin = [
        row for row in tradeable
        if row.get("holdout_status") == "INSUFFICIENT"
        and _as_float(row.get("holdout_return_pct"), 0.0) > 0.0
        and _as_float(row.get("holdout_pf"), 0.0) >= 1.0
    ]

    return {
        "engine_commit": _git_head(),
        "source_engine_commit": promotion.get("engine_commit"),
        "rows": len(tradeable),
        "final_verdict_counts": _counts(row.get("final_verdict") for row in tradeable),
        "holdout_status_counts": _counts(row.get("holdout_status") for row in tradeable),
        "holdout_trades_histogram": {
            str(k): v for k, v in sorted(
                _counts(_as_int(row.get("holdout_trades"), 0) for row in tradeable).items(),
                key=lambda item: int(item[0]),
            )
        },
        "holdout_by_timeframe": _group_holdout(tradeable, "timeframe"),
        "holdout_by_regime": _group_holdout(tradeable, "regime"),
        "threshold_sensitivity": threshold_sensitivity,
        "promotion_status_mismatches": {
            "promoted_but_holdout_insufficient": len(mismatches),
            "rows": mismatches[:100],
        },
        "positive_but_insufficient_holdout": {
            "count": len(positive_but_thin),
            "rows": [
                {
                    "symbol": row.get("symbol"),
                    "timeframe": row.get("timeframe"),
                    "regime": row.get("regime"),
                    "strategy_name": row.get("strategy_name"),
                    "holdout_trades": row.get("holdout_trades"),
                    "holdout_return_pct": row.get("holdout_return_pct"),
                    "holdout_pf": row.get("holdout_pf"),
                    "final_verdict": row.get("final_verdict"),
                    "why": row.get("why"),
                }
                for row in positive_but_thin[:100]
            ],
        },
        "trade_rate_rows": _trade_rate_rows(tradeable, ledger),
        "construction_projection": _construction_projection(
            tradeable,
            ledger,
            min_trades=projection_min_trades,
            folds=projection_folds,
        ),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Diagnose clean-holdout sufficiency from promotion artifacts.")
    parser.add_argument("--promotion-review", default=str(DEFAULT_PROMOTION))
    parser.add_argument("--ledger", default=str(DEFAULT_LEDGER))
    parser.add_argument("--label", default="holdout_diagnostics")
    parser.add_argument("--max-threshold", type=int, default=10)
    parser.add_argument("--projection-min-trades", type=int, default=5)
    parser.add_argument("--projection-folds", type=int, default=5)
    args = parser.parse_args()

    promotion_path = Path(args.promotion_review)
    ledger_path = Path(args.ledger)
    if not promotion_path.exists():
        raise SystemExit(f"promotion review not found: {promotion_path}")
    if not ledger_path.exists():
        raise SystemExit(f"ledger not found: {ledger_path}")

    diagnostics = build_diagnostics(
        _load_json(promotion_path),
        _load_json(ledger_path),
        max_threshold=args.max_threshold,
        projection_min_trades=args.projection_min_trades,
        projection_folds=args.projection_folds,
    )
    out = REPO / "pm_outputs" / "experiments" / f"{args.label}.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(diagnostics, indent=2, default=str), encoding="utf-8")

    print(f"\n=== HOLDOUT DIAGNOSTICS [{args.label}] engine={diagnostics['engine_commit']} ===")
    print(f"rows: {diagnostics['rows']}")
    print(f"holdout_status_counts: {diagnostics['holdout_status_counts']}")
    print(
        "promoted_but_holdout_insufficient: "
        f"{diagnostics['promotion_status_mismatches']['promoted_but_holdout_insufficient']}"
    )
    print("threshold_sensitivity:")
    for row in diagnostics["threshold_sensitivity"]:
        print(
            f"  min={row['min_trades']:2d} pass={row['holdout_pass']:3d} "
            f"fail={row['holdout_fail']:3d} insufficient={row['holdout_insufficient']:3d} "
            f"clean_except_holdout={row['gate_clean_except_holdout_promotable']:3d}"
        )
    projection = diagnostics["construction_projection"]
    rate_context = projection["rate_context"]
    print("construction_projection:")
    print(
        "  rate_context: "
        f"holdout/training={rate_context['median_clean_holdout_to_training_bar_ratio']} "
        f"holdout/stage2={rate_context['median_clean_holdout_to_stage2_bar_ratio']} "
        f"holdout_vs_val_trade_rate={rate_context['median_holdout_to_validation_trade_rate_ratio']}"
    )
    anchor = projection["trade_count_anchor"]
    print(
        "  trade_count_anchor: "
        f"clean_feasible={anchor['gate_clean_except_holdout_feasible']} "
        f"positive_thin_feasible={anchor['gate_clean_positive_thin_feasible']} "
        f"median_flat_loss_pct={anchor['median_extra_training_loss_pct_if_done_as_flat_tail']}"
    )
    kfold = projection["purged_kfold"]
    print(
        "  purged_kfold: "
        f"clean_sufficient={kfold['gate_clean_except_holdout_sufficient']} "
        f"positive_thin_sufficient={kfold['gate_clean_positive_thin_sufficient']} "
        f"runtime_x~{kfold['approx_runtime_multiplier']}"
    )
    print(f"wrote {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
