"""Accuracy parity gate for efficiency changes.

Efficiency work (parallelism, disk feature cache, worker count) must NOT change WHAT is selected
— only how fast. This compares two discovery ledgers winner-by-winner and FAILS if any winner's
strategy, timeframe, parameters, or key metrics differ beyond a float epsilon. The intended use:
produce a ground-truth ledger (sequential, cache off) and a fast ledger (parallel, disk-cache on)
on the same verification slice; require PARITY_OK before trusting the full fast run's results.

Trust standard: a speedup is only acceptable if it is provably accuracy-neutral here.
"""
import json, sys, argparse
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent

# Fields that define the SELECTED winner (must match exactly) and metrics (match within eps).
IDENTITY_FIELDS = ["strategy_name", "timeframe"]
PARAM_FIELD = "parameters"
METRIC_FIELDS = ["val_return", "val_sharpe", "train_return", "train_sharpe",
                 "composite_score", "robustness_ratio"]


def _winner_view(entry):
    if not isinstance(entry, dict):
        return None
    return {
        "identity": {k: entry.get(k) for k in IDENTITY_FIELDS},
        "params": entry.get(PARAM_FIELD, {}) or {},
        "metrics": {k: entry.get(k) for k in METRIC_FIELDS if entry.get(k) is not None},
    }


def _params_equal(a, b):
    if set(a.keys()) != set(b.keys()):
        return False, f"param keys differ: {sorted(set(a) ^ set(b))}"
    for k in a:
        av, bv = a[k], b[k]
        if isinstance(av, float) or isinstance(bv, float):
            if abs(float(av) - float(bv)) > 1e-9:
                return False, f"param {k}: {av} != {bv}"
        elif av != bv:
            return False, f"param {k}: {av} != {bv}"
    return True, ""


def compare_ledgers(ref, test, eps=1e-6):
    diffs = []
    syms = sorted(set(ref) | set(test))
    matched = 0
    for sym in syms:
        rv, tv = _winner_view(ref.get(sym)), _winner_view(test.get(sym))
        if rv is None or tv is None:
            diffs.append({"symbol": sym, "issue": "present in only one ledger"}); continue
        if rv["identity"] != tv["identity"]:
            diffs.append({"symbol": sym, "issue": "winner identity differs",
                          "ref": rv["identity"], "test": tv["identity"]}); continue
        ok, why = _params_equal(rv["params"], tv["params"])
        if not ok:
            diffs.append({"symbol": sym, "issue": f"winner params differ: {why}"}); continue
        mdiff = {k: (rv["metrics"].get(k), tv["metrics"].get(k)) for k in set(rv["metrics"]) | set(tv["metrics"])
                 if abs(float(rv["metrics"].get(k, 0.0)) - float(tv["metrics"].get(k, 0.0))) > eps}
        if mdiff:
            diffs.append({"symbol": sym, "issue": "winner metrics differ", "deltas": mdiff}); continue
        matched += 1
    verdict = "PARITY_OK" if not diffs else "PARITY_BROKEN"
    return {"verdict": verdict, "symbols": len(syms), "matched": matched, "diffs": diffs}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("ref_ledger", help="ground-truth ledger (sequential, cache off)")
    ap.add_argument("test_ledger", help="fast ledger (parallel, disk-cache on)")
    ap.add_argument("--eps", type=float, default=1e-6)
    args = ap.parse_args()
    ref = json.load(open(args.ref_ledger))
    test = json.load(open(args.test_ledger))
    res = compare_ledgers(ref, test, args.eps)
    print(f"\n=== ACCURACY PARITY GATE ===")
    print(f"  ref:  {args.ref_ledger}")
    print(f"  test: {args.test_ledger}")
    print(f"  symbols={res['symbols']} matched={res['matched']} -> {res['verdict']}")
    for d in res["diffs"][:30]:
        print(f"  DIFF {d['symbol']}: {d['issue']}" + (f" {d.get('deltas') or d.get('ref','')}" if d.get('deltas') or d.get('ref') else ""))
    if res["verdict"] != "PARITY_OK":
        print("\n  ACCURACY-NEUTRALITY NOT PROVEN — the fast config changed winners. Do NOT trust the fast run.")
        sys.exit(1)
    print("\n  Efficiency change is accuracy-neutral on this slice. Safe to trust the fast run.")


if __name__ == "__main__":
    main()
