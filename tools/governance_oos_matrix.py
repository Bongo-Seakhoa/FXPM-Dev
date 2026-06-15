"""OOS walk-forward governance matrix.

Re-runs each ledger tournament cell's 3-window walk-forward-validation backtests
for every governance policy plus a `none` (Numba) leg. The `none` leg must
reproduce the ledger's stored control-only walk_forward_audit (parity gate);
`control_fixed` vs `none` doubles as a Numba<->Python loop check. Read-only
against the engine; writes JSONL under pm_outputs/governance_oos_eval/.
"""
import argparse
import gc
import json
import subprocess
import time
from collections import defaultdict
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd

from governance_eval_common import (
    DEFAULT_OUT_DIR, REPO, cost_parity_mismatches, build_config, finite,
    iter_tournament_cells, load_ledger, sanitize_pf,
)

REGIMES = ("TREND", "RANGE", "BREAKOUT", "CHOP")
TOL_RET = 0.005
TOL_SCORE = 1e-3
TOL_PF = 0.01


def git_head():
    try:
        return subprocess.check_output(["git", "rev-parse", "--short", "HEAD"],
                                       cwd=str(REPO), text=True).strip()
    except Exception:
        return "unknown"


def slim_trade(t):
    return {
        "sb": int(t.get("signal_bar", t.get("entry_bar", 0)) or 0),
        "eb": int(t.get("entry_bar", 0) or 0),
        "xb": int(t.get("exit_bar", 0) or 0),
        "d": str(t.get("direction", "")),
        "pnl": round(finite(t.get("pnl_dollars")), 4),
        "r": round(finite(t.get("r_multiple")), 4),
        "x": str(t.get("exit_reason", "")),
    }


def partition_by_regime(trades, features):
    buckets = {rg: [] for rg in REGIMES}
    has_col = "REGIME_LIVE" in features.columns
    for trade in trades:
        try:
            sb = int(trade.get("signal_bar", trade.get("entry_bar", 0)))
        except (TypeError, ValueError):
            continue
        if has_col and 0 <= sb < len(features):
            regime = features["REGIME_LIVE"].iloc[sb]
            if pd.notna(regime) and regime in buckets:
                buckets[regime].append(trade)
    return buckets


def window_metrics(ro, bucket, features, tf):
    if not bucket:
        return {"total_trades": 0, "profit_factor": 0.0, "total_return_pct": 0.0,
                "max_drawdown_pct": 0.0, "win_rate": 0.0}, 0.0
    metrics = ro._compute_bucket_metrics(bucket, features=features, timeframe=tf)
    score = float(ro.scorer.score(metrics, purpose="selection")) if metrics.get("total_trades", 0) else 0.0
    return metrics, score


def parity_status(stored_windows, none_rows):
    """Compare the `none` (Numba) leg against the ledger's stored walk_forward_audit.

    PASS/WARN = reproduced within tolerance. DRIFT = window geometry exact but trade
    set differs — stale stored audit (regime_params.json evolved after the cell was
    optimized; live uses current params, so DRIFT cells stay analysis-eligible).
    FAIL = reconstruction error (bars/windows wrong, or same trades but float drift).
    """
    if not stored_windows:
        return {"status": "NO_REFERENCE", "windows": []}
    by_idx = {int(r["window"]): r for r in none_rows}
    diffs, bars_bad, trades_bad, floats_bad, floats_warn_any = [], False, False, False, False
    for sw in stored_windows:
        idx = int(sw.get("window_index", -1))
        row = by_idx.get(idx)
        if row is None:
            diffs.append({"window": idx, "issue": "missing_reproduced_window"})
            bars_bad = True
            continue
        d = {
            "window": idx,
            "bars": [int(sw.get("scored_start_bar", -1)), int(sw.get("scored_end_bar", -1)),
                     row["scored_start_bar"], row["scored_end_bar"]],
            "trades": [int(sw.get("total_trades", 0) or 0), row["metrics"]["total_trades"]],
            "d_ret": round(abs(finite(sw.get("total_return_pct")) - row["metrics"]["total_return_pct"]), 6),
            "d_dd": round(abs(finite(sw.get("max_drawdown_pct")) - row["metrics"]["max_drawdown_pct"]), 6),
            "d_pf": round(abs(sanitize_pf(sw.get("profit_factor")) - sanitize_pf(row["metrics"]["profit_factor"])), 6),
            "d_score": round(abs(finite(sw.get("score")) - row["metrics"]["score"]), 6),
        }
        diffs.append(d)
        if not (d["bars"][0] == d["bars"][2] and d["bars"][1] == d["bars"][3]):
            bars_bad = True
        if d["trades"][0] != d["trades"][1]:
            trades_bad = True
        ok = d["d_ret"] <= TOL_RET and d["d_dd"] <= TOL_RET and d["d_pf"] <= TOL_PF and d["d_score"] <= TOL_SCORE
        warn = d["d_ret"] <= 10 * TOL_RET and d["d_dd"] <= 10 * TOL_RET and d["d_pf"] <= 10 * TOL_PF and d["d_score"] <= 10 * TOL_SCORE
        if not warn:
            floats_bad = True
        elif not ok:
            floats_warn_any = True
    if bars_bad:
        status = "FAIL"
    elif trades_bad:
        status = "DRIFT"
    elif floats_bad:
        status = "FAIL"
    elif floats_warn_any:
        status = "WARN"
    else:
        status = "PASS"
    return {"status": status, "windows": diffs}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--ledger", default=str(REPO / "pm_configs.json"))
    ap.add_argument("--out", default=str(DEFAULT_OUT_DIR))
    ap.add_argument("--symbols", nargs="*", default=None)
    ap.add_argument("--policies", default=None, help="comma list; default = all 11")
    ap.add_argument("--resume", action="store_true")
    ap.add_argument("--limit", type=int, default=0, help="max combos this invocation")
    ap.add_argument("--no-disk-cache", action="store_true")
    ap.add_argument("--max-fail-rate", type=float, default=0.20)
    args = ap.parse_args()

    cfg = build_config(no_disk_cache=args.no_disk_cache)
    from pm_core import DataLoader, FeatureComputer
    from pm_pipeline import RegimeOptimizer
    from pm_order_governance import candidate_policy_names, make_policy
    from pm_strategies import StrategyRegistry

    ro = RegimeOptimizer(cfg)
    loader = DataLoader(cfg.data_dir)
    policies = candidate_policy_names(args.policies.split(",") if args.policies else None)
    variants = ["none"] + policies

    cells = list(iter_tournament_cells(load_ledger(args.ledger)))
    if args.symbols:
        wanted = {s.upper() for s in args.symbols}
        cells = [c for c in cells if c.symbol.upper() in wanted]

    grouped = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))
    for c in cells:
        grouped[c.symbol][c.tf][c.combo_key].append(c)

    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)
    done_path = out_dir / "done_combos.jsonl"
    done = set()
    if args.resume and done_path.exists():
        done = {line.strip() for line in done_path.read_text(encoding="utf-8").splitlines() if line.strip()}

    results_f = open(out_dir / "oos_results.jsonl", "a", encoding="utf-8")
    parity_f = open(out_dir / "parity.jsonl", "a", encoding="utf-8")
    done_f = open(done_path, "a", encoding="utf-8")

    meta = {
        "started": datetime.now().isoformat(), "git_head": git_head(),
        "engine_mtimes": {f: datetime.fromtimestamp((REPO / f).stat().st_mtime).isoformat()
                          for f in ("pm_core.py", "pm_pipeline.py", "pm_order_governance.py", "regime_params.json")},
        "args": vars(args), "policies": policies,
        "cost_parity": cost_parity_mismatches(cfg, cells[0].artifact_meta) if cells else [],
        "skips": [], "n_cells": len(cells),
    }
    if meta["cost_parity"]:
        print(f"WARNING cost parity mismatches: {meta['cost_parity']}")

    parity_counts = {"PASS": 0, "WARN": 0, "DRIFT": 0, "FAIL": 0, "NO_REFERENCE": 0}
    combos_run = 0
    t0 = time.time()

    def write_meta():
        meta["finished"] = datetime.now().isoformat()
        meta["elapsed_s"] = round(time.time() - t0, 1)
        meta["parity_counts"] = parity_counts
        meta["combos_run"] = combos_run
        (out_dir / "run_meta.json").write_text(json.dumps(meta, indent=1, default=str), encoding="utf-8")

    for symbol in sorted(grouped):
        sym_cells = [c for tfm in grouped[symbol].values() for cl in tfm.values() for c in cl]
        wmeta = (sym_cells[0].data_window_meta.get("windows") or {})
        wf_meta, wk_meta = wmeta.get("walk_forward_validation") or {}, wmeta.get("workflow_window") or {}
        if not wf_meta.get("start_ts") or not wk_meta.get("start_ts"):
            meta["skips"].append({"symbol": symbol, "reason": "missing_window_meta"})
            continue
        ws, we = pd.Timestamp(wk_meta["start_ts"]), pd.Timestamp(wk_meta["end_ts"])
        wf_s, wf_e = pd.Timestamp(wf_meta["start_ts"]), pd.Timestamp(wf_meta["end_ts"])
        base = loader.load_symbol(symbol)
        if base is None or len(base) < 200:
            meta["skips"].append({"symbol": symbol, "reason": "no_data"})
            continue
        base = base.sort_index()
        if wf_s < base.index[0]:
            meta["skips"].append({"symbol": symbol, "reason": "wfv_window_rolled_out"})
            continue
        if ws < base.index[0]:
            meta["skips"].append({"symbol": symbol, "reason": "workflow_leading_bars_missing",
                                  "missing_until": str(base.index[0])})
        base_wf = base.loc[(base.index >= ws) & (base.index <= we)]

        for tf in sorted(grouped[symbol]):
            if args.limit and combos_run >= args.limit:
                break
            try:
                data = base_wf.copy() if tf == "M5" else loader.resample(base_wf, tf)
                full = FeatureComputer.compute_all(data, symbol=symbol, timeframe=tf,
                                                   regime_params_file=cfg.regime_params_file)
            except Exception as exc:
                meta["skips"].append({"symbol": symbol, "tf": tf, "reason": f"features_failed:{exc}"})
                continue
            stage2 = full.loc[(full.index >= wf_s) & (full.index < wf_e)].copy()
            del full, data
            FeatureComputer.clear_cache()
            gc.collect()
            if len(stage2) <= 1:
                meta["skips"].append({"symbol": symbol, "tf": tf, "reason": "empty_wfv_slice"})
                continue
            cuts = np.linspace(0, len(stage2), num=4, dtype=int)

            for combo_key, combo_cells in grouped[symbol][tf].items():
                if args.limit and combos_run >= args.limit:
                    break
                if combo_key in done:
                    continue
                c0 = combo_cells[0]
                try:
                    strategy = StrategyRegistry.get(c0.strategy_name, **c0.parameters)
                except Exception as exc:
                    for cell in combo_cells:
                        parity_f.write(json.dumps({"cell": cell.cell_id, "combo": combo_key,
                                                   "status": "SKIP", "reason": f"strategy_load_failed:{exc}"}) + "\n")
                    done_f.write(combo_key + "\n")
                    done_f.flush()
                    continue

                rows_by_cell = defaultdict(list)
                for w, (a, b) in enumerate(zip(cuts[:-1], cuts[1:])):
                    if b - a <= 1:
                        continue
                    win = stage2.iloc[int(a):int(b)].copy()
                    try:
                        signals = strategy.generate_signals(win, symbol)
                    except Exception as exc:
                        meta["skips"].append({"combo": combo_key, "window": w, "reason": f"signals_failed:{exc}"})
                        continue
                    for variant in variants:
                        gp = None if variant == "none" else make_policy(variant)
                        try:
                            res = ro.backtester.run(win, signals, symbol, strategy,
                                                    timeframe=tf, warmup_bars=0, governance_policy=gp)
                        except Exception as exc:
                            meta["skips"].append({"combo": combo_key, "window": w, "variant": variant,
                                                  "reason": f"backtest_failed:{exc}"})
                            continue
                        buckets = partition_by_regime(res.get("trades", []), win)
                        for cell in combo_cells:
                            bucket = buckets.get(cell.regime, [])
                            metrics, score = window_metrics(ro, bucket, win, tf)
                            rows_by_cell[cell.cell_id].append({
                                "cell": cell.cell_id, "combo": combo_key, "symbol": symbol,
                                "tf": tf, "regime": cell.regime, "strategy": c0.strategy_name,
                                "policy": variant, "window": w,
                                "scored_start_bar": int(a), "scored_end_bar": int(b),
                                "metrics": {
                                    "total_trades": int(metrics.get("total_trades", 0) or 0),
                                    "profit_factor": sanitize_pf(metrics.get("profit_factor")),
                                    "total_return_pct": round(finite(metrics.get("total_return_pct")), 6),
                                    "max_drawdown_pct": round(finite(metrics.get("max_drawdown_pct")), 6),
                                    "win_rate": round(finite(metrics.get("win_rate")), 4),
                                    "score": round(finite(score), 6),
                                },
                                "trades": [slim_trade(t) for t in bucket],
                            })
                    del win
                for cell in combo_cells:
                    rows = rows_by_cell.get(cell.cell_id, [])
                    for row in rows:
                        results_f.write(json.dumps(row, default=str) + "\n")
                    none_rows = [r for r in rows if r["policy"] == "none"]
                    ctrl_rows = [r for r in rows if r["policy"] == "control_fixed"]
                    par = parity_status(cell.stored_wf_windows, none_rows)
                    par.update({
                        "cell": cell.cell_id, "combo": combo_key,
                        "promotion_status": cell.promotion_status,
                        "selected_policy": cell.selected_policy,
                        "optimized_at": cell.optimized_at,
                        "numba_vs_python": [
                            {"window": n["window"],
                             "d_trades": ctrl["metrics"]["total_trades"] - n["metrics"]["total_trades"],
                             "d_ret": round(ctrl["metrics"]["total_return_pct"] - n["metrics"]["total_return_pct"], 6)}
                            for n in none_rows for ctrl in ctrl_rows if ctrl["window"] == n["window"]
                        ],
                    })
                    parity_counts[par["status"]] = parity_counts.get(par["status"], 0) + 1
                    parity_f.write(json.dumps(par, default=str) + "\n")
                results_f.flush()
                parity_f.flush()
                done_f.write(combo_key + "\n")
                done_f.flush()
                combos_run += 1

                judged = parity_counts["PASS"] + parity_counts["WARN"] + parity_counts["DRIFT"] + parity_counts["FAIL"]
                if judged >= 30 and parity_counts["FAIL"] / judged > args.max_fail_rate:
                    print(f"ABORT: parity fail rate {parity_counts['FAIL']}/{judged} exceeds {args.max_fail_rate}")
                    write_meta()
                    raise SystemExit(2)
            del stage2
            gc.collect()
        loader.clear_cache()
        print(f"[{symbol}] done combos_run={combos_run} parity={parity_counts} "
              f"elapsed={time.time() - t0:.0f}s", flush=True)

    write_meta()
    print(f"FINISHED combos={combos_run} parity={parity_counts} elapsed={time.time() - t0:.0f}s")


if __name__ == "__main__":
    main()
