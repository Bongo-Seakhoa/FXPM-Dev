"""Replay-based promotion audit gate.

Why this exists (evidence: agent_audit/performance/CLAUDE_DECISIVE_REPLAY_FINDING.md):
the optimizer promotes winners on 15-16-trade validation slices; full-sample $10k replay
of the 2026-06-19 book (117 promoted cells) showed 38 net losers (down to PF 0.31 /
DD 99.5%) and a median PF of 1.10. This tool closes that gap: it replays every logical
promoted cell over the full active universe at the canonical $10,000 baseline and blocks
cells whose FULL-SAMPLE evidence fails hard profitability gates.

It reuses the exact enumerator/replayer from agent_audit/performance/scripts so the audit
verdicts are guaranteed consistent with the recorded replay evidence.

Modes:
  --report-only (default): replay + report; ledger untouched.
  --apply: back up pm_configs.json, then set promotion_status=BLOCKED (+ evidence) on
           failing cells at BOTH the symbol level and the matching regime leaf.

Gates (CLI-overridable):
  full-sample profit factor >= 1.05
  full-sample max drawdown  <= 30%
  full-sample trade count   >= 30

Unverified cells (replay errors, e.g. missing local data/spec) are reported, and only
blocked when --block-unverified is passed.
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import shutil
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Tuple

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))
sys.path.insert(0, str(REPO / "agent_audit" / "performance" / "scripts"))

AUDIT_SCHEMA_VERSION = "2026-06-20-replay-audit-v1"
TRADEABLE = {"PROMOTED", "PROMOTE_SMALL"}


def _finite(value: Any, default: float = 0.0) -> float:
    try:
        out = float(value)
    except Exception:
        return default
    return out if math.isfinite(out) else default


def evaluate_gates(row: Dict[str, Any],
                   *,
                   min_pf: float,
                   max_dd_pct: float,
                   min_trades: int) -> Tuple[str, List[str]]:
    """Pure gate decision for one replay row.

    Returns (verdict, reasons) where verdict is PASS / FAIL / UNVERIFIED.
    PF gate treats inf (no-loss profitable) as passing.
    """
    if str(row.get("error", "") or ""):
        return "UNVERIFIED", [f"replay_error:{row['error']}"]
    reasons: List[str] = []
    pf_raw = row.get("profit_factor")
    pf = float("inf") if str(pf_raw) == "inf" else _finite(pf_raw)
    trades = int(_finite(row.get("total_trades"), 0))
    dd = _finite(row.get("max_drawdown_pct"), 100.0)
    if not (pf == float("inf") or pf >= min_pf):
        reasons.append(f"full_sample_pf {pf:.2f} < {min_pf:.2f}")
    if dd > max_dd_pct:
        reasons.append(f"full_sample_max_dd {dd:.1f}% > {max_dd_pct:.1f}%")
    if trades < min_trades:
        reasons.append(f"full_sample_trades {trades} < {min_trades}")
    return ("PASS" if not reasons else "FAIL"), reasons


def _audit_evidence(row: Dict[str, Any], verdict: str, reasons: List[str],
                    thresholds: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "schema_version": AUDIT_SCHEMA_VERSION,
        "audited_at": datetime.now().isoformat(),
        "verdict": verdict,
        "reasons": list(reasons),
        "thresholds": dict(thresholds),
        "starting_balance": _finite(row.get("starting_balance"), 10000.0),
        "net_profit": _finite(row.get("net_profit")),
        "return_pct": _finite(row.get("return_pct")),
        "profit_factor": str(row.get("profit_factor")),
        "max_drawdown_pct": _finite(row.get("max_drawdown_pct")),
        "total_trades": int(_finite(row.get("total_trades"), 0)),
        "expectancy_pips": _finite(row.get("expectancy_pips")),
        "avg_mae_r": _finite(row.get("avg_mae_r")),
    }


def _matches(payload: Dict[str, Any], cell: Dict[str, Any]) -> bool:
    return (
        str(payload.get("strategy_name", "") or "").strip()
        == str(cell.get("strategy_name", "") or "").strip()
        and str(payload.get("promotion_status", "") or "").upper() in TRADEABLE
    )


def apply_audit(ledger: Dict[str, Any],
                results: List[Dict[str, Any]],
                *,
                block_unverified: bool = False) -> Dict[str, int]:
    """Mutate ledger in place from audit results. Returns counters."""
    counters = {"blocked_symbol": 0, "blocked_leaf": 0, "pass_annotated": 0, "unverified": 0}
    # Ledger keys are not guaranteed upper-case (e.g. key 'Palladium' with symbol field
    # 'Palladium' enumerated as 'PALLADIUM'), so resolve keys case-insensitively via both
    # the dict key and the payload's own symbol field.
    key_by_upper: Dict[str, str] = {}
    for k, v in ledger.items():
        key_by_upper.setdefault(str(k).upper(), k)
        if isinstance(v, dict) and v.get("symbol"):
            key_by_upper.setdefault(str(v["symbol"]).upper(), k)
    for res in results:
        cell, verdict, reasons, evidence = (
            res["cell"], res["verdict"], res["reasons"], res["evidence"],
        )
        should_block = verdict == "FAIL" or (verdict == "UNVERIFIED" and block_unverified)
        if verdict == "UNVERIFIED":
            counters["unverified"] += 1
        symbol = str(cell["symbol"]).upper()
        payload = ledger.get(key_by_upper.get(symbol, symbol))
        if not isinstance(payload, dict):
            continue
        targets: List[Dict[str, Any]] = []
        if (
            str(payload.get("timeframe", "") or "").upper() == str(cell["timeframe"]).upper()
            and _matches(payload, cell)
        ):
            targets.append(payload)
        regimes = payload.get("regime_configs")
        if isinstance(regimes, dict):
            leaf_tf = regimes.get(str(cell["timeframe"]).upper())
            if isinstance(leaf_tf, dict):
                leaf = leaf_tf.get(str(cell.get("regime", "")).upper())
                if isinstance(leaf, dict) and _matches(leaf, cell):
                    targets.append(leaf)
                elif isinstance(leaf_tf, dict):
                    # regime label may be SYMBOL_LEVEL etc.; scan for matching promoted leaf
                    for leaf2 in leaf_tf.values():
                        if isinstance(leaf2, dict) and _matches(leaf2, cell):
                            targets.append(leaf2)
        for idx, target in enumerate(targets):
            target["replay_audit"] = evidence
            if should_block:
                target["promotion_status"] = "BLOCKED"
                prior = target.get("promotion_reasons")
                reason_list = list(prior) if isinstance(prior, list) else []
                reason_list.append(
                    "replay_audit: " + ("; ".join(reasons) if reasons else "failed full-sample gate")
                )
                target["promotion_reasons"] = reason_list
                counters["blocked_symbol" if target is payload else "blocked_leaf"] += 1
            else:
                counters["pass_annotated"] += 1
    return counters


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", default=str(REPO / "config.json"))
    parser.add_argument("--ledger", default=str(REPO / "pm_configs.json"))
    parser.add_argument("--starting-balance", type=float, default=10000.0)
    parser.add_argument("--min-pf", type=float, default=1.05)
    parser.add_argument("--max-dd-pct", type=float, default=30.0)
    parser.add_argument("--min-trades", type=int, default=30)
    parser.add_argument("--apply", action="store_true", help="write audited ledger (backup first)")
    parser.add_argument("--block-unverified", action="store_true")
    parser.add_argument("--reuse-replay-csv", default="",
                        help="optional: reuse an existing replay CSV instead of re-replaying")
    parser.add_argument("--out-dir", default=str(REPO / "agent_audit" / "performance" / "data"))
    args = parser.parse_args()

    from run_promoted_cell_replay import _pipeline_config, iter_promoted_cells, replay_cell
    from pm_core import DataLoader

    ledger_path = Path(args.ledger)
    ledger = json.loads(ledger_path.read_text(encoding="utf-8-sig"))
    cells = list(iter_promoted_cells(ledger))
    print(f"logical promoted cells: {len(cells)}")

    rows_by_key: Dict[Tuple[str, str, str, str], Dict[str, Any]] = {}
    if args.reuse_replay_csv:
        with open(args.reuse_replay_csv, encoding="utf-8") as fh:
            for row in csv.DictReader(fh):
                key = (str(row.get("symbol", "")).upper(), str(row.get("timeframe", "")).upper(),
                       str(row.get("regime", "")).upper(), str(row.get("strategy_name", "")))
                rows_by_key[key] = row

    cfg = _pipeline_config(Path(args.config), args.starting_balance)
    loader = DataLoader(cfg.data_dir) if not rows_by_key else None

    thresholds = {"min_pf": args.min_pf, "max_dd_pct": args.max_dd_pct,
                  "min_trades": args.min_trades, "starting_balance": args.starting_balance}
    results: List[Dict[str, Any]] = []
    for i, cell in enumerate(cells, 1):
        key = (str(cell["symbol"]).upper(), str(cell["timeframe"]).upper(),
               str(cell.get("regime", "")).upper(), str(cell.get("strategy_name", "")))
        row = rows_by_key.get(key)
        if row is None:
            if rows_by_key:
                row = {**cell, "error": "not_in_reused_csv"}
            else:
                row = replay_cell(cfg, loader, cell)
        verdict, reasons = evaluate_gates(
            row, min_pf=args.min_pf, max_dd_pct=args.max_dd_pct, min_trades=args.min_trades)
        results.append({
            "cell": cell, "row": row, "verdict": verdict, "reasons": reasons,
            "evidence": _audit_evidence(row, verdict, reasons, thresholds),
        })
        print(f"[{i}/{len(cells)}] {cell['symbol']:8} {cell['timeframe']:4} "
              f"{str(cell['strategy_name'])[:26]:26} -> {verdict}"
              f"{(' (' + '; '.join(reasons) + ')') if reasons else ''}")

    n_pass = sum(1 for r in results if r["verdict"] == "PASS")
    n_fail = sum(1 for r in results if r["verdict"] == "FAIL")
    n_unv = sum(1 for r in results if r["verdict"] == "UNVERIFIED")
    print(f"\nSUMMARY: PASS={n_pass}  FAIL={n_fail}  UNVERIFIED={n_unv}  of {len(results)}")

    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    report_path = out_dir / f"promotion_replay_audit_{stamp}.csv"
    with report_path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)
        writer.writerow(["symbol", "timeframe", "regime", "strategy", "verdict", "reasons",
                         "net_profit", "return_pct", "profit_factor", "max_drawdown_pct",
                         "total_trades", "expectancy_pips"])
        for r in results:
            c, row = r["cell"], r["row"]
            writer.writerow([c["symbol"], c["timeframe"], c.get("regime", ""),
                             c.get("strategy_name", ""), r["verdict"], "; ".join(r["reasons"]),
                             row.get("net_profit"), row.get("return_pct"), row.get("profit_factor"),
                             row.get("max_drawdown_pct"), row.get("total_trades"),
                             row.get("expectancy_pips")])
    print(f"report -> {report_path}")

    if not args.apply:
        print("report-only mode; ledger untouched (use --apply to write).")
        return 0

    # Keep backups OUT of the repo root: tests/test_config_json_integrity.py requires the
    # root pm_configs*.json glob to contain only the canonical ledger.
    backup = out_dir / f"pm_configs.backup_replay_audit_{stamp}.json"
    shutil.copy2(ledger_path, backup)
    print(f"backup -> {backup}")
    counters = apply_audit(ledger, results, block_unverified=args.block_unverified)
    tmp = ledger_path.with_suffix(".tmp_audit")
    tmp.write_text(json.dumps(ledger, indent=2, default=str), encoding="utf-8")
    tmp.replace(ledger_path)
    print(f"ledger updated -> {ledger_path}  counters={counters}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
