"""Mine the in-sample governance tournament evidence already stored in the ledger.

Zero new computation: parses candidate_results per regime cell and quantifies how
each policy compared to control_fixed on the bleeding-edge optimization surface,
including the max-of-11 selection pressure that the OOS study must discount.
"""
import argparse
import json
from collections import Counter, defaultdict
from statistics import mean, median

from governance_eval_common import (
    DEFAULT_OUT_DIR, REPO, finite, iter_tournament_cells, load_ledger, sanitize_pf,
)

POLICIES = (
    "breakeven_1r", "stepped_breakeven", "atr_trail_capped", "chandelier_atr",
    "structure_trail", "partial_tp_runner", "time_decay_exit",
    "volatility_contraction_exit", "regime_flip_reduce", "pure_atr",
)


def deltas_vs_control(cr):
    ctrl = cr.get("control_fixed") or {}
    base = {
        "score": finite(ctrl.get("score")), "pf": sanitize_pf(ctrl.get("profit_factor")),
        "ret": finite(ctrl.get("total_return_pct")), "dd": finite(ctrl.get("max_drawdown_pct")),
        "trades": int(ctrl.get("total_trades", 0) or 0),
    }
    out = {}
    for name, row in cr.items():
        if name == "control_fixed" or not isinstance(row, dict):
            continue
        out[name] = {
            "d_score": finite(row.get("score")) - base["score"],
            "d_pf": sanitize_pf(row.get("profit_factor")) - base["pf"],
            "d_ret": finite(row.get("total_return_pct")) - base["ret"],
            "d_dd": finite(row.get("max_drawdown_pct")) - base["dd"],
            "trades": int(row.get("total_trades", 0) or 0),
        }
    return base, out


def policy_table(cells):
    rows = {}
    for policy in POLICIES:
        ds = [c["deltas"][policy] for c in cells if policy in c["deltas"]]
        if not ds:
            continue
        rows[policy] = {
            "n": len(ds),
            "median_d_score": round(median(d["d_score"] for d in ds), 4),
            "median_d_pf": round(median(d["d_pf"] for d in ds), 4),
            "median_d_ret_pp": round(median(d["d_ret"] for d in ds), 4),
            "median_d_dd_pp": round(median(d["d_dd"] for d in ds), 4),
            "mean_d_ret_pp": round(mean(d["d_ret"] for d in ds), 4),
            "win_share_score": round(sum(1 for d in ds if d["d_score"] > 0) / len(ds), 4),
            "win_share_ret": round(sum(1 for d in ds if d["d_ret"] > 0) / len(ds), 4),
            "argmax_count": sum(1 for c in cells if c["argmax"] == policy),
            "selected_count": sum(1 for c in cells if c["selected"] == policy),
        }
    return rows


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--ledger", default=str(REPO / "pm_configs.json"))
    ap.add_argument("--out", default=str(DEFAULT_OUT_DIR))
    args = ap.parse_args()

    cells = []
    for cell in iter_tournament_cells(load_ledger(args.ledger)):
        base, deltas = deltas_vs_control(cell.candidate_results)
        if not deltas:
            continue
        argmax = max(deltas, key=lambda p: deltas[p]["d_score"])
        cells.append({
            "cell": cell.cell_id, "tf": cell.tf, "regime": cell.regime,
            "promotion_status": cell.promotion_status, "is_validated": cell.is_validated,
            "selected": cell.selected_policy, "basis": cell.selection_basis,
            "control": base, "deltas": deltas,
            "argmax": argmax if deltas[argmax]["d_score"] > 0 else "control_fixed",
            "max_d_score": round(max(d["d_score"] for d in deltas.values()), 4),
            "median_d_score": round(median(d["d_score"] for d in deltas.values()), 4),
            "n_policies_beating_control": sum(1 for d in deltas.values() if d["d_score"] > 0),
        })

    active = [c for c in cells if c["selected"] != "control_fixed"]
    live = [c for c in cells if c["promotion_status"] in ("PROMOTED", "PROMOTE_SMALL")]
    live_active = [c for c in live if c["selected"] != "control_fixed"]

    sel_d_ret = [c["deltas"][c["selected"]]["d_ret"] for c in active]
    sel_d_score = [c["deltas"][c["selected"]]["d_score"] for c in active]

    report = {
        "n_cells": len(cells),
        "n_active_selected": len(active),
        "n_live_relevant": len(live),
        "n_live_relevant_active": len(live_active),
        "selection_basis_counts": dict(Counter(c["basis"] for c in cells)),
        "selected_policy_counts": dict(Counter(c["selected"] for c in cells)),
        "selected_policy_insample_improvement": {
            "median_d_ret_pp": round(median(sel_d_ret), 4) if sel_d_ret else None,
            "mean_d_ret_pp": round(mean(sel_d_ret), 4) if sel_d_ret else None,
            "median_d_score": round(median(sel_d_score), 4) if sel_d_score else None,
            "mean_d_score": round(mean(sel_d_score), 4) if sel_d_score else None,
        },
        "selection_pressure": {
            "median_max_d_score": round(median(c["max_d_score"] for c in cells), 4),
            "median_median_d_score": round(median(c["median_d_score"] for c in cells), 4),
            "share_cells_with_2plus_beating_control": round(
                sum(1 for c in cells if c["n_policies_beating_control"] >= 2) / len(cells), 4),
            "mean_n_policies_beating_control": round(
                mean(c["n_policies_beating_control"] for c in cells), 3),
        },
        "policy_table_all": policy_table(cells),
        "policy_table_live_relevant": policy_table(live),
        "policy_table_by_tf": {tf: policy_table([c for c in cells if c["tf"] == tf])
                               for tf in sorted({c["tf"] for c in cells})},
        "policy_table_by_regime": {rg: policy_table([c for c in cells if c["regime"] == rg])
                                   for rg in sorted({c["regime"] for c in cells})},
        "cells": cells,
    }

    out_dir = REPO / args.out if not str(args.out).startswith(str(REPO)) else __import__("pathlib").Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "insample_evidence.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=1, default=str)
    print(f"cells={report['n_cells']} active={report['n_active_selected']} "
          f"live={report['n_live_relevant']} live_active={report['n_live_relevant_active']}")
    print(f"basis: {report['selection_basis_counts']}")
    print(f"selected IS improvement: {report['selected_policy_insample_improvement']}")
    print(f"selection pressure: {report['selection_pressure']}")
    print("\npolicy (all cells): median_d_score / median_d_ret_pp / win_share_score / selected")
    for p, r in sorted(report["policy_table_all"].items(), key=lambda kv: -kv[1]["median_d_score"]):
        print(f"  {p:28} {r['median_d_score']:+8.3f}  {r['median_d_ret_pp']:+7.3f}  "
              f"{r['win_share_score']:.2f}  {r['selected_count']}")
    print(f"\nwrote {out_path}")


if __name__ == "__main__":
    main()
