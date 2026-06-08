"""Spread stress kill-test: re-evaluate ledger winners under conservative_max vs stress_max.

A promotion-readiness gate component. conservative_max is the honest discovery baseline;
stress_max additionally charges SpreadMax at entry. A winner that collapses (PF<1, negative
return, or loses most of its edge) under stress_max is fragile to spread spikes and should not
be promoted to live without review. Read-only against the engine; reuses the benchmark harness.
"""
import json, sys, argparse
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))
sys.path.insert(0, str(REPO / "tools"))

from pm_core import DataLoader
from benchmark import build_config, evaluate_winner, _git_head, is_fx


def killtest(ledger, symbols, collapse_frac=0.5):
    cfg_c = build_config("conservative_max")
    cfg_s = build_config("stress_max")
    loader = DataLoader(cfg_c.data_dir)
    rows = []
    for sym in symbols:
        if sym not in ledger:
            rows.append({"symbol": sym, "verdict": "MISSING"}); continue
        w = ledger[sym]
        try:
            rc = evaluate_winner(cfg_c, loader, w)
            rs = evaluate_winner(cfg_s, loader, w)
        except Exception as e:
            rows.append({"symbol": sym, "verdict": "ERROR", "error": f"{type(e).__name__}: {e}"}); continue
        if "error" in rc or "error" in rs:
            rows.append({"symbol": sym, "verdict": "NO_DATA"}); continue
        ret_c, ret_s = rc["return_pct"], rs["return_pct"]
        pf_s = rs["profit_factor"]
        lost = ((ret_c - ret_s) / abs(ret_c)) if ret_c != 0 else 0.0
        kill = (pf_s < 1.0) or (ret_s <= 0) or (ret_c > 0 and lost > collapse_frac)
        rows.append({
            "symbol": sym, "strategy": rc.get("strategy"), "tf": rc.get("tf"),
            "asset_class": rc.get("asset_class"), "provenance": rc.get("spread_provenance_status"),
            "ret_conservative": round(ret_c, 2), "ret_stress": round(ret_s, 2),
            "pf_conservative": round(rc["profit_factor"], 3), "pf_stress": round(pf_s, 3),
            "dd_conservative": round(rc["max_dd_pct"], 2), "dd_stress": round(rs["max_dd_pct"], 2),
            "frac_return_lost": round(lost, 3),
            "verdict": "KILL" if kill else "SURVIVE",
        })
    return rows


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--ledger", default=str(REPO / "pm_configs.json"))
    ap.add_argument("--symbols", nargs="*")
    ap.add_argument("--all", action="store_true")
    ap.add_argument("--fx-only", action="store_true")
    ap.add_argument("--collapse-frac", type=float, default=0.5)
    ap.add_argument("--label", default="stress_killtest")
    args = ap.parse_args()

    ledger = json.load(open(args.ledger))
    symbols = sorted(ledger.keys()) if (args.all or not args.symbols) else args.symbols
    if args.fx_only:
        symbols = [s for s in symbols if is_fx(s)]

    rows = killtest(ledger, symbols, args.collapse_frac)
    ok = [r for r in rows if r["verdict"] in ("KILL", "SURVIVE")]
    killed = [r for r in ok if r["verdict"] == "KILL"]
    print(f"\n=== SPREAD STRESS KILL-TEST [{args.label}] engine={_git_head()} ===")
    hdr = ["symbol", "tf", "asset_class", "ret_conservative", "ret_stress", "pf_stress",
           "frac_return_lost", "verdict"]
    print("\t".join(hdr))
    for r in rows:
        if r["verdict"] in ("MISSING", "ERROR", "NO_DATA"):
            print(f"{r['symbol']}\t{r['verdict']}"); continue
        print("\t".join(str(r.get(h, "")) for h in hdr))
    if ok:
        print(f"\n-- {len(killed)}/{len(ok)} winners KILLED by stress_max (fragile to spread spikes) --")

    out = REPO / "pm_outputs" / "experiments" / f"{args.label}.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    json.dump({"engine_commit": _git_head(), "collapse_frac": args.collapse_frac, "rows": rows},
              open(out, "w"), indent=2, default=str)
    print(f"\nwrote {out}")


if __name__ == "__main__":
    main()
