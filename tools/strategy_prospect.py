"""Strategy prospect screener.

Tenet-1 evidence gate for candidate strategies: before a strategy is taken seriously it must
show tradeful, profitable cases under honest (conservative_max FBS) cost. Samples a strategy's
param grid across a representative pool, backtests via the production engine, and reports how
many configs are tradeful and profitable. Read-only; does not promote anything.
"""
import json, sys, argparse, random, itertools
from pathlib import Path
import numpy as np

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))
sys.path.insert(0, str(REPO / "tools"))

from pm_core import DataLoader, FeatureComputer, Backtester, get_instrument_spec
from pm_strategies import StrategyRegistry
from benchmark import build_config, _git_head, is_fx

# Representative FX-led pool + one of each non-FX class for coverage.
DEFAULT_POOL = ["EURUSD", "GBPUSD", "USDJPY", "EURJPY", "AUDNZD", "USDCAD",
                "XAUUSD", "US500", "BTCUSD"]
MIN_TRADES = 20


def _sample_grid(grid, n, seed):
    keys = list(grid.keys())
    combos = list(itertools.product(*[grid[k] for k in keys]))
    rng = random.Random(seed)
    if len(combos) > n:
        combos = rng.sample(combos, n)
    return [dict(zip(keys, c)) for c in combos]


def _run(cfg, loader, name, params, symbol):
    base = loader.load_symbol(symbol)
    if base is None or len(base) == 0:
        return None
    feats_by_tf = {}
    best = None
    for tf in ("M15", "H1", "H4", "D1"):
        df = loader.resample(base, tf)
        if df is None or len(df) < 300:
            continue
        strat = StrategyRegistry.get(name, **params)
        feats = FeatureComputer.compute_for_strategy(df, strat, symbol=symbol, timeframe=tf,
                                                     regime_params_file=cfg.regime_params_file)
        sig = strat.generate_signals(feats, symbol)
        if not hasattr(sig, "abs") or sig.abs().sum() == 0:
            continue
        r = Backtester(cfg).run(feats, sig, symbol, strat, spec=get_instrument_spec(symbol), timeframe=tf)
        tr = int(r.get("total_trades", 0))
        if tr < MIN_TRADES:
            continue
        cand = {"symbol": symbol, "tf": tf, "trades": tr,
                "return_pct": float(r.get("total_return_pct", 0.0)),
                "profit_factor": float(r.get("profit_factor", 0.0)),
                "max_dd_pct": float(r.get("max_drawdown_pct", 0.0))}
        if best is None or cand["return_pct"] > best["return_pct"]:
            best = cand
    return best


def screen(name, cfg, loader, pool, samples, seed):
    grid = StrategyRegistry.get(name).get_param_grid()
    param_sets = _sample_grid(grid, samples, seed)
    runs, tradeful, profitable, best = 0, 0, 0, None
    for params in param_sets:
        for symbol in pool:
            res = _run(cfg, loader, name, params, symbol)
            if res is None:
                continue
            runs += 1
            tradeful += 1
            if res["profit_factor"] >= 1.0 and res["return_pct"] > 0:
                profitable += 1
                if best is None or res["return_pct"] > best["return_pct"]:
                    best = {**res, "params": params}
    verdict = "VIABLE" if profitable >= max(3, runs * 0.05) else "WEAK"
    return {"strategy": name, "tradeful_runs": tradeful, "profitable_runs": profitable,
            "verdict": verdict, "best": best}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--strategies", nargs="*", help="Strategy names; default = all registered.")
    ap.add_argument("--pool", nargs="*", default=DEFAULT_POOL)
    ap.add_argument("--fx-only", action="store_true")
    ap.add_argument("--samples", type=int, default=24)
    ap.add_argument("--seed", type=int, default=7)
    ap.add_argument("--spread-mode", default="conservative_max")
    ap.add_argument("--label", default="prospect_screen")
    ap.add_argument("--out", default=str(REPO / "pm_outputs" / "experiments"))
    args = ap.parse_args()

    cfg = build_config(args.spread_mode)
    loader = DataLoader(cfg.data_dir)
    pool = [s for s in args.pool if (is_fx(s) if args.fx_only else True)]
    names = args.strategies or sorted(StrategyRegistry._strategies.keys())

    rows = []
    for name in names:
        try:
            rows.append(screen(name, cfg, loader, pool, args.samples, args.seed))
        except Exception as e:
            rows.append({"strategy": name, "error": f"{type(e).__name__}: {e}"})

    print(f"\n=== STRATEGY PROSPECT SCREEN [{args.label}] {args.spread_mode} engine={_git_head()} ===")
    print(f"{'strategy':40} {'tradeful':>9} {'profit>=1':>10} {'verdict':>8}  best")
    for r in rows:
        if "error" in r:
            print(f"{r['strategy']:40} ERROR {r['error']}"); continue
        b = r["best"]
        bs = f"{b['symbol']} {b['tf']} ret={b['return_pct']:.1f} pf={b['profit_factor']:.2f}" if b else "-"
        print(f"{r['strategy']:40} {r['tradeful_runs']:>9} {r['profitable_runs']:>10} {r['verdict']:>8}  {bs}")

    out_dir = Path(args.out); out_dir.mkdir(parents=True, exist_ok=True)
    rec = {"label": args.label, "engine_commit": _git_head(), "spread_mode": args.spread_mode,
           "pool": pool, "samples": args.samples, "rows": rows}
    out_path = out_dir / f"{args.label}.json"
    json.dump(rec, open(out_path, "w"), indent=2, default=str)
    print(f"\nwrote {out_path}")


if __name__ == "__main__":
    main()
