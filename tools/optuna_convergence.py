"""Quantify the Optuna trial-budget cut: how much search quality do we lose at a given cap?

The discovery contract was cut from (coverage 0.15, max 5000) to (coverage 0.05, max 600). The cut
only bites LARGE search spaces (small spaces are exhausted either way). This probe runs the REAL
optimizer objective (same ParameterSpace, scoring_fn, TPE sampler, early-rejection) on the largest-
search-space strategies, captures the genuine Optuna study, and reports best-score-so-far vs trial
number -- i.e. what fraction of the eventually-discoverable edge a 200/400/600/1000 budget captures.

If best@600 is ~99%+ of best@deep, the cut is cheap. If it is materially below, the cut sacrifices
quality and the budget should be restored (now affordable again with the Numba JIT reclaimed).

Faithful, not a proxy: it calls optimizer.optimize() unchanged and only monkeypatches _compile_stats
to stash the study object so the trial history can be read back. No engine files are modified.

Run:  python tools/optuna_convergence.py [--symbol EURUSD] [--tf H1] [--deep 2000] [--top 2]
"""
import argparse
import sys
from pathlib import Path

import numpy as np

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))
sys.path.insert(0, str(REPO / "tools"))

import pm_optuna
from pm_optuna import create_optimizer, ParameterSpace
from pm_core import StrategyScorer, FeatureComputer, Backtester
from pm_strategies import StrategyRegistry
from benchmark import build_config, DataLoader


def make_scoring_fn(scorer, cfg):
    """Exact replica of pm_pipeline._optimize_with_optuna.scoring_fn (FX stage-2 objective)."""
    def scoring_fn(train_metrics, val_metrics):
        if val_metrics.get("total_trades", 0) >= cfg.fx_val_min_trades:
            val_dd = float(val_metrics.get("max_drawdown_pct", 100.0))
            if val_dd >= cfg.fx_val_max_drawdown:
                return -500.0
            final_score, _, _, _ = scorer.fx_generalization_score(
                train_metrics, val_metrics, purpose="opt")
            return final_score
        return scorer.score(train_metrics, purpose="opt") * 0.7
    return scoring_fn


def _largest_spaces(top):
    sizes = []
    for name in getattr(StrategyRegistry, "_strategies", {}):
        try:
            strat = StrategyRegistry.get(name)
            grid = strat.get_param_grid()
            size = ParameterSpace(grid, strat.get_params()).get_search_space_size()
            sizes.append((int(size), name))
        except Exception:
            continue
    sizes.sort(reverse=True)
    return sizes[:top]


def _curve_report(study, deep, search_space):
    vals = [t.value for t in study.trials if t.value is not None]
    if not vals:
        print("    no completed trials"); return
    best = np.maximum.accumulate(np.asarray(vals, dtype=np.float64))
    final = float(best[-1])
    n = len(best)
    print(f"    completed trials: {n}  | search_space={search_space}  | final best={final:.4f}")
    marks = [50, 100, 200, 400, 600, 1000, 1500, n]
    seen = set()
    print(f"    {'@trials':>8} {'best':>10} {'%of_final':>10}")
    for k in marks:
        if k > n or k in seen:
            continue
        seen.add(k)
        b = float(best[k - 1])
        pct = (b / final * 100.0) if final > 0 else (100.0 if b == final else float("nan"))
        print(f"    {k:>8} {b:>10.4f} {pct:>9.1f}%")
    # first trial reaching X% of final (only meaningful for positive final)
    if final > 0:
        for frac in (0.95, 0.99, 0.999):
            hit = next((i + 1 for i, b in enumerate(best) if b >= frac * final), None)
            print(f"    reaches {frac*100:>5.1f}% of final at trial: {hit}")
    return {"final": final, "n": n,
            "best_at_600": float(best[min(600, n) - 1]),
            "pct_at_600": (float(best[min(600, n) - 1]) / final * 100.0) if final > 0 else None}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--symbol", default="EURUSD")
    ap.add_argument("--tf", default="H1")
    ap.add_argument("--deep", type=int, default=2000, help="max trials to run (the 'deep' reference)")
    ap.add_argument("--top", type=int, default=2, help="probe the N largest-search-space strategies")
    ap.add_argument("--train-frac", type=float, default=0.70)
    args = ap.parse_args()

    if not pm_core_numba_ok():
        print("WARNING: Numba not active; probe will run on the slow Python loop.", file=sys.stderr)

    # capture the real study without touching engine files
    captured = {}
    orig = pm_optuna.OptunaTPEOptimizer._compile_stats
    def patched(self, study, elapsed):
        captured["study"] = study
        return orig(self, study, elapsed)
    pm_optuna.OptunaTPEOptimizer._compile_stats = patched

    cfg = build_config("conservative_max")
    # deep, faithful budget: family-aware, coverage 1.0 so it runs min(deep, search_space) trials
    cfg.optuna_family_size_aware_budget = True
    cfg.optuna_min_trials_per_strategy = 80
    cfg.optuna_max_trials_per_strategy = args.deep
    cfg.optuna_target_coverage_pct = 1.0
    cfg.max_param_combos = args.deep

    scorer = StrategyScorer(cfg)
    backtester = Backtester(cfg)
    optimizer = create_optimizer(cfg, backtester, scorer, StrategyRegistry)
    scoring_fn = make_scoring_fn(scorer, cfg)

    loader = DataLoader(cfg.data_dir)
    base = loader.load_symbol(args.symbol)
    df = loader.resample(base, args.tf)
    cut = int(len(df) * args.train_frac)

    targets = _largest_spaces(args.top)
    print(f"\n=== OPTUNA CONVERGENCE PROBE  {args.symbol} {args.tf}  deep={args.deep} ===")
    print(f"largest search spaces probed: {targets}")
    summary = []
    for size, name in targets:
        strat = StrategyRegistry.get(name)
        feats = FeatureComputer.compute_for_strategy(
            df, strat, symbol=args.symbol, timeframe=args.tf,
            regime_params_file=cfg.regime_params_file)
        train, val = feats.iloc[:cut], feats.iloc[cut:]
        captured.clear()
        print(f"\n[{name}] search_space={size}")
        optimizer.optimize(
            symbol=args.symbol, strategy_name=name, param_grid=strat.get_param_grid(),
            train_features=train, val_features=val, scoring_fn=scoring_fn,
            min_trades=cfg.fx_opt_min_trades)
        study = captured.get("study")
        if study is None:
            print("    (no study captured)"); continue
        res = _curve_report(study, args.deep, size)
        if res:
            summary.append((name, res))

    print("\n=== VERDICT ===")
    for name, res in summary:
        p = res.get("pct_at_600")
        if p is None:
            print(f"  {name}: final best<=0 ({res['final']:.3f}); no tradable edge at this cell")
        else:
            tag = "cut is cheap" if p >= 99.0 else ("cut costs some edge" if p >= 95.0 else "cut SACRIFICES edge")
            print(f"  {name}: best@600 = {p:.1f}% of best@{res['n']}  -> {tag}")
    return 0


def pm_core_numba_ok():
    import pm_core
    return bool(getattr(pm_core, "NUMBA_AVAILABLE", False))


if __name__ == "__main__":
    raise SystemExit(main())
