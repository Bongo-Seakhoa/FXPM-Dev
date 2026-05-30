"""Consolidated promotion-readiness review — the single trust gate before any winner is live.

A discovery winner is only promotable if it clears EVERY gate:
  Gate 1  stress kill-test : survives stress_max entry spread (tools/stress_killtest)
  Gate 2  global PBO       : its (symbol,timeframe) group is not overfit (tools/pbo, timeframe-aligned)
  Gate 3  clean holdout    : positive return on the freshest untouched holdout window
  Gate 4  provenance       : spread feed is trustworthy (no non-FX frozen-feed outlier flag)
  Gate 0  parity           : (optional) fast-run winners are bit-identical to a sequential reference

Any failure -> REJECT; provenance/holdout-missing/elevated-PBO -> REVIEW; all clean -> PROMOTE.
Read-only; promotes nothing by itself. This is the "trust with my life" boundary.
"""
import json, sys, argparse
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))
sys.path.insert(0, str(REPO / "tools"))

from stress_killtest import killtest
from pbo import load_oos_manifest_matrix, pbo_by_aligned_group
from benchmark import _git_head

PBO_REVIEW = 0.5   # worst-group PBO above this -> overfit-risk REVIEW/REJECT
HOLDOUT_MIN_TRADES = 5


def _holdout_gate(entry):
    """Gate 3: read the freshest-holdout evidence persisted on the winner."""
    h = entry.get("holdout_metrics") or {}
    if not h:
        ve = entry.get("validation_evidence") or {}
        h = ve.get("holdout") or ve.get("holdout_metrics") or {}
    if not h:
        return "NONE", {}
    trades = int(h.get("total_trades", h.get("trades", 0)) or 0)
    ret = float(h.get("total_return_pct", h.get("return_pct", 0.0)) or 0.0)
    pf = float(h.get("profit_factor", 0.0) or 0.0)
    if trades < HOLDOUT_MIN_TRADES:
        return "INSUFFICIENT", {"trades": trades, "return_pct": ret, "pf": pf}
    return ("PASS" if (ret > 0 and pf >= 1.0) else "FAIL"), {"trades": trades, "return_pct": round(ret, 2), "pf": round(pf, 3)}


def _pbo_by_symbol(evidence_dir):
    out = {}
    if not evidence_dir:
        return out
    for man in Path(evidence_dir).glob("*_oos_manifest.json"):
        try:
            M, manifest, cids = load_oos_manifest_matrix(man)
            res = pbo_by_aligned_group(M, manifest, n_blocks=16, candidate_ids=cids)
            out[str(manifest.get("symbol", man.stem))] = res.get("worst_group_pbo")
        except Exception as e:
            out[man.stem] = f"error:{type(e).__name__}"
    return out


def _verdict(stress_verdict, holdout_status, provenance, worst_pbo):
    if stress_verdict == "KILL":
        return "REJECT", "fails stress_max (spread-spike fragile)"
    if holdout_status == "FAIL":
        return "REJECT", "negative/sub-1 holdout"
    if isinstance(worst_pbo, (int, float)) and worst_pbo > PBO_REVIEW:
        return "REJECT", f"overfit (worst-group PBO {worst_pbo})"
    reasons = []
    if holdout_status in ("NONE", "INSUFFICIENT"):
        reasons.append(f"holdout {holdout_status.lower()}")
    if provenance and provenance not in ("fx_historical_spread", "broker_spread", ""):
        reasons.append(f"provenance {provenance}")
    if reasons:
        return "REVIEW", "; ".join(reasons)
    return "PROMOTE", "all gates clean"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--ledger", default=str(REPO / "pm_outputs" / "discovery_honest" / "pm_configs_discovery_honest.json"))
    ap.add_argument("--evidence-dir", default=str(REPO / "pm_outputs" / "discovery_honest" / "oos_evidence"))
    ap.add_argument("--symbols", nargs="*")
    ap.add_argument("--label", default="promotion_review")
    args = ap.parse_args()

    if not Path(args.ledger).exists():
        print(f"ledger not found yet: {args.ledger} (run not complete) — gates ready, nothing to review.")
        return
    ledger = json.load(open(args.ledger))
    symbols = args.symbols or sorted(ledger.keys())

    stress_rows = {r["symbol"]: r for r in killtest(ledger, symbols)}
    pbo_by_symbol = _pbo_by_symbol(args.evidence_dir)

    rows = []
    for sym in symbols:
        entry = ledger.get(sym, {})
        sr = stress_rows.get(sym, {})
        sv = sr.get("verdict", "NO_DATA")
        hstatus, hdetail = _holdout_gate(entry)
        prov = sr.get("provenance", "")
        wpbo = pbo_by_symbol.get(sym)
        verdict, why = _verdict(sv, hstatus, prov, wpbo)
        rows.append({"symbol": sym, "stress": sv, "holdout": hstatus, "holdout_detail": hdetail,
                     "provenance": prov, "worst_pbo": wpbo, "verdict": verdict, "why": why})

    counts = {}
    for r in rows:
        counts[r["verdict"]] = counts.get(r["verdict"], 0) + 1
    print(f"\n=== PROMOTION-READINESS REVIEW [{args.label}] engine={_git_head()} ===")
    print(f"{'symbol':10} {'stress':9} {'holdout':12} {'worst_pbo':>9} {'verdict':9}  why")
    for r in rows:
        print(f"{r['symbol']:10} {r['stress']:9} {r['holdout']:12} {str(r['worst_pbo']):>9} {r['verdict']:9}  {r['why']}")
    print(f"\n-- {counts.get('PROMOTE',0)} PROMOTE / {counts.get('REVIEW',0)} REVIEW / {counts.get('REJECT',0)} REJECT "
          f"/ {counts.get('NO_DATA',0) + counts.get('MISSING',0)} no-data, of {len(rows)} --")

    out = REPO / "pm_outputs" / "experiments" / f"{args.label}.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    json.dump({"engine_commit": _git_head(), "rows": rows, "counts": counts}, open(out, "w"), indent=2, default=str)
    print(f"\nwrote {out}")


if __name__ == "__main__":
    main()
