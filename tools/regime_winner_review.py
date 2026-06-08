"""Regime-level discovery winner review.

Expands the discovery ledger from one legacy/default row per symbol into the
actual production surface: symbol x timeframe x regime. This is intentionally
read-only and light enough to run while the optimizer is still active.
"""
import argparse
import json
import sys
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))
sys.path.insert(0, str(REPO / "tools"))

from benchmark import _git_head
from pbo import load_oos_manifest_matrix, pbo_by_aligned_group


TIMEFRAME_ORDER = ["M5", "M15", "M30", "H1", "H4", "D1"]
REGIME_ORDER = ["TREND", "RANGE", "BREAKOUT", "CHOP"]
HOLDOUT_MIN_TRADES = 5
PBO_REVIEW = 0.5


def _is_no_trade(cfg: Dict[str, Any]) -> bool:
    if not isinstance(cfg, dict):
        return True
    strategy_name = str(cfg.get("strategy_name", "") or "")
    val = cfg.get("val_metrics") if isinstance(cfg.get("val_metrics"), dict) else {}
    return (
        strategy_name in ("", "NO_TRADE")
        or bool((cfg.get("artifact_meta") or {}).get("no_trade"))
        or str(val.get("validation_status", "")).lower() == "no_trade"
    )


def _metric(metrics: Dict[str, Any], *names: str, default: float = 0.0) -> float:
    for name in names:
        if name in metrics:
            try:
                return float(metrics.get(name) or 0.0)
            except Exception:
                return default
    return default


def _trades(metrics: Dict[str, Any]) -> int:
    try:
        return int(metrics.get("total_trades", metrics.get("trades", 0)) or 0)
    except Exception:
        return 0


def _holdout_gate(metrics: Dict[str, Any], min_trades: int = HOLDOUT_MIN_TRADES) -> str:
    if not metrics:
        return "NONE"
    trades = _trades(metrics)
    ret = _metric(metrics, "total_return_pct", "return_pct")
    pf = _metric(metrics, "profit_factor")
    if trades < max(0, int(min_trades)):
        return "INSUFFICIENT"
    return "PASS" if ret > 0.0 and pf >= 1.0 else "FAIL"


def _historical_gate(metrics: Dict[str, Any]) -> str:
    if not metrics:
        return "NONE"
    if bool(metrics.get("severe_failure")):
        return "FAIL"
    if metrics.get("enabled") is False:
        return "DISABLED"
    return "PASS"


def _manifest_pbo_by_timeframe(evidence_dir: Path, symbol: str) -> Dict[str, Any]:
    manifest_path = evidence_dir / f"{symbol}_oos_manifest.json"
    if not manifest_path.exists():
        return {"_error": "missing_manifest", "_groups": {}}
    try:
        matrix, manifest, candidate_ids = load_oos_manifest_matrix(manifest_path)
        result = pbo_by_aligned_group(matrix, manifest, n_blocks=16, candidate_ids=candidate_ids)
    except Exception as exc:
        return {"_error": f"{type(exc).__name__}: {exc}", "_groups": {}}
    groups = {}
    for group in result.get("groups", []) or []:
        tf = str(group.get("timeframe", "") or "")
        if not tf:
            continue
        groups[tf] = {
            "pbo": group.get("pbo"),
            "n_candidates": group.get("n_candidates"),
            "reason": group.get("reason"),
        }
    return {
        "_aggregate_pbo": result.get("aggregate_pbo"),
        "_worst_group_pbo": result.get("worst_group_pbo"),
        "_alignment": result.get("alignment"),
        "_groups": groups,
    }


def _row_verdict(holdout_status: str, historical_status: str, pbo_value: Optional[float]) -> str:
    if holdout_status == "FAIL" or historical_status == "FAIL":
        return "REJECT"
    if isinstance(pbo_value, (int, float)) and float(pbo_value) > PBO_REVIEW:
        return "REJECT"
    if holdout_status in ("NONE", "INSUFFICIENT"):
        return "REVIEW"
    if pbo_value is None:
        return "REVIEW"
    return "LIGHT_PASS"


def _sort_key(row: Dict[str, Any]):
    tf = row.get("timeframe", "")
    regime = row.get("regime", "")
    return (
        row.get("symbol", ""),
        TIMEFRAME_ORDER.index(tf) if tf in TIMEFRAME_ORDER else 99,
        REGIME_ORDER.index(regime) if regime in REGIME_ORDER else 99,
    )


def iter_rows(ledger: Dict[str, Any],
              evidence_dir: Path,
              symbols: Optional[Iterable[str]] = None,
              include_no_trade: bool = False,
              holdout_min_trades: int = HOLDOUT_MIN_TRADES) -> List[Dict[str, Any]]:
    selected = list(symbols) if symbols else sorted(
        key for key, value in ledger.items()
        if isinstance(value, dict) and not str(key).startswith("_")
    )
    pbo_cache: Dict[str, Dict[str, Any]] = {}
    rows: List[Dict[str, Any]] = []
    for symbol in selected:
        entry = ledger.get(symbol, {})
        if not isinstance(entry, dict):
            continue
        pbo_cache.setdefault(symbol, _manifest_pbo_by_timeframe(evidence_dir, symbol))
        pbo_groups = pbo_cache[symbol].get("_groups", {})
        regime_configs = entry.get("regime_configs") or {}
        for timeframe, regimes in regime_configs.items():
            if not isinstance(regimes, dict):
                continue
            tf_pbo = pbo_groups.get(str(timeframe), {})
            for regime, cfg in regimes.items():
                if not isinstance(cfg, dict):
                    continue
                no_trade = _is_no_trade(cfg)
                if no_trade and not include_no_trade:
                    continue
                val = cfg.get("val_metrics") if isinstance(cfg.get("val_metrics"), dict) else {}
                train = cfg.get("train_metrics") if isinstance(cfg.get("train_metrics"), dict) else {}
                holdout = cfg.get("holdout_metrics") if isinstance(cfg.get("holdout_metrics"), dict) else {}
                historical = cfg.get("historical_audit_metrics") if isinstance(cfg.get("historical_audit_metrics"), dict) else {}
                holdout_status = "NO_TRADE" if no_trade else _holdout_gate(holdout, holdout_min_trades)
                historical_status = "NO_TRADE" if no_trade else _historical_gate(historical)
                pbo_value = tf_pbo.get("pbo")
                verdict = "NO_TRADE" if no_trade else _row_verdict(holdout_status, historical_status, pbo_value)
                rows.append({
                    "symbol": symbol,
                    "timeframe": timeframe,
                    "regime": regime,
                    "strategy_name": cfg.get("strategy_name", ""),
                    "parameters": cfg.get("parameters", {}) or {},
                    "quality_score": round(_metric(cfg, "quality_score"), 5),
                    "train_trades": _trades(train),
                    "train_return_pct": round(_metric(train, "total_return_pct", "return_pct"), 2),
                    "train_pf": round(_metric(train, "profit_factor"), 3),
                    "val_trades": _trades(val),
                    "val_return_pct": round(_metric(val, "total_return_pct", "return_pct"), 2),
                    "val_pf": round(_metric(val, "profit_factor"), 3),
                    "val_max_dd_pct": round(_metric(val, "max_drawdown_pct"), 2),
                    "holdout_status": holdout_status,
                    "holdout_trades": _trades(holdout),
                    "holdout_return_pct": round(_metric(holdout, "total_return_pct", "return_pct"), 2),
                    "holdout_pf": round(_metric(holdout, "profit_factor"), 3),
                    "holdout_max_dd_pct": round(_metric(holdout, "max_drawdown_pct"), 2),
                    "historical_status": historical_status,
                    "historical_trades": _trades(historical.get("metrics", historical) if isinstance(historical, dict) else {}),
                    "historical_return_pct": round(_metric(historical.get("metrics", historical) if isinstance(historical, dict) else {}, "total_return_pct", "return_pct"), 2),
                    "historical_pf": round(_metric(historical.get("metrics", historical) if isinstance(historical, dict) else {}, "profit_factor"), 3),
                    "historical_max_dd_pct": round(_metric(historical.get("metrics", historical) if isinstance(historical, dict) else {}, "max_drawdown_pct"), 2),
                    "pbo_timeframe": pbo_value,
                    "pbo_candidates": tf_pbo.get("n_candidates"),
                    "pbo_reason": tf_pbo.get("reason"),
                    "promotion_status": cfg.get("promotion_status", ""),
                    "promotion_reasons": cfg.get("promotion_reasons", []),
                    "verdict": verdict,
                })
    return sorted(rows, key=_sort_key)


def _print_table(rows: List[Dict[str, Any]], include_no_trade: bool) -> None:
    cols = [
        "symbol", "timeframe", "regime", "strategy_name",
        "val_trades", "val_return_pct", "val_pf",
        "holdout_status", "holdout_trades", "holdout_return_pct", "holdout_pf",
        "historical_status", "pbo_timeframe", "verdict",
    ]
    print("\t".join(cols))
    for row in rows:
        if row["verdict"] == "NO_TRADE" and not include_no_trade:
            continue
        print("\t".join(str(row.get(col, "")) for col in cols))


def main() -> int:
    parser = argparse.ArgumentParser(description="Review discovery winners per symbol/timeframe/regime.")
    parser.add_argument("--ledger", default=str(REPO / "pm_configs.json"))
    parser.add_argument("--evidence-dir", default=str(REPO / "pm_outputs" / "production_live" / "oos_evidence"))
    parser.add_argument("--symbols", nargs="*")
    parser.add_argument("--include-no-trade", action="store_true")
    parser.add_argument("--holdout-min-trades", type=int, default=HOLDOUT_MIN_TRADES)
    parser.add_argument("--label", default="regime_winner_review")
    args = parser.parse_args()

    ledger_path = Path(args.ledger)
    evidence_dir = Path(args.evidence_dir)
    if not ledger_path.exists():
        raise SystemExit(f"ledger not found: {ledger_path}")
    ledger = json.loads(ledger_path.read_text(encoding="utf-8"))
    rows = iter_rows(
        ledger,
        evidence_dir,
        symbols=args.symbols,
        include_no_trade=args.include_no_trade,
        holdout_min_trades=args.holdout_min_trades,
    )
    counts: Dict[str, int] = {}
    for row in rows:
        counts[row["verdict"]] = counts.get(row["verdict"], 0) + 1

    print(f"\n=== REGIME WINNER REVIEW [{args.label}] engine={_git_head()} ===")
    print(f"rows={len(rows)} counts={counts}")
    _print_table(rows, args.include_no_trade)

    out = REPO / "pm_outputs" / "experiments" / f"{args.label}.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps({
        "engine_commit": _git_head(),
        "ledger": str(ledger_path),
        "evidence_dir": str(evidence_dir),
        "holdout_min_trades": int(args.holdout_min_trades),
        "rows": rows,
        "counts": counts,
    }, indent=2, default=str), encoding="utf-8")
    print(f"\nwrote {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
