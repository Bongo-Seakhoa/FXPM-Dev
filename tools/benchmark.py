"""FxPM benchmark harness.

Reconstructs winners from a ledger, re-runs the production Backtester, and reports the
Tenet-2 performance hallmarks including path-dependent drawdown. Produces a reproducible
baseline JSON used as the Tenet-5 improvement gate. Read-only against the engine.
"""
import json, sys, math, argparse, subprocess
from pathlib import Path
from dataclasses import fields
import numpy as np


def _git_head():
    try:
        return subprocess.check_output(["git", "rev-parse", "--short", "HEAD"],
                                       cwd=str(Path(__file__).resolve().parent.parent),
                                       text=True).strip()
    except Exception:
        return "unknown"

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

from pm_core import PipelineConfig, DataLoader, FeatureComputer, Backtester, get_instrument_spec
from pm_strategies import StrategyRegistry
from pm_symbol import is_fiat_fx_symbol

DEFAULT_POOL = ["EURUSD", "GBPUSD", "USDJPY", "XAUUSD", "BTCUSD"]
SPREAD_MODES = ("fixed_spec", "historical_max", "conservative_max")


def is_fx(symbol):
    """Pure FX = two fiat currency legs. Excludes crypto (BTCETH), metals (XAU), indices."""
    return is_fiat_fx_symbol(symbol)


def build_config(spread_mode="fixed_spec"):
    cfg_json = json.load(open(REPO / "config.json"))
    pipe = cfg_json.get("pipeline", {})
    allowed = {f.name for f in fields(PipelineConfig)}
    kw = {k: v for k, v in pipe.items() if k in allowed}
    kw["data_dir"] = str(REPO / "data")
    rp = kw.get("regime_params_file", "regime_params.json")
    kw["regime_params_file"] = str(REPO / rp)
    cfg = PipelineConfig(**kw)
    if spread_mode not in SPREAD_MODES:
        raise NotImplementedError(f"spread_mode={spread_mode} not supported; have {SPREAD_MODES}")
    setattr(cfg, "backtest_spread_mode", spread_mode)  # engine reads via getattr
    setattr(cfg, "use_spread", True)
    return cfg


def path_dependent_dd(equity):
    eq = np.asarray(equity, dtype=np.float64)
    if eq.size < 2:
        return {"ulcer_index": 0.0, "time_underwater_pct": 0.0,
                "max_dd_duration": 0, "avg_dd_duration": 0.0, "recovery_factor": 0.0}
    peak = np.maximum.accumulate(eq)
    dd = np.where(peak > 0, (peak - eq) / peak, 0.0)
    underwater = dd > 1e-9
    ulcer = float(np.sqrt(np.mean((dd * 100.0) ** 2)))
    tuw = float(underwater.mean() * 100.0)
    durations, run = [], 0
    for u in underwater:
        if u:
            run += 1
        elif run:
            durations.append(run); run = 0
    if run:
        durations.append(run)
    max_dd = float(dd.max() * 100.0)
    total_ret = (eq[-1] - eq[0]) / eq[0] * 100.0 if eq[0] else 0.0
    return {
        "ulcer_index": round(ulcer, 3),
        "time_underwater_pct": round(tuw, 2),
        "max_dd_duration": int(max(durations) if durations else 0),
        "avg_dd_duration": round(float(np.mean(durations)) if durations else 0.0, 1),
        "recovery_factor": round(total_ret / max_dd, 3) if max_dd > 0 else 0.0,
    }


def evaluate_winner(cfg, loader, winner):
    symbol, tf = winner["symbol"], winner["timeframe"]
    name, params = winner["strategy_name"], (winner.get("parameters", {}) or {})
    base = loader.load_symbol(symbol)
    if base is None or len(base) == 0:
        return {"symbol": symbol, "error": "no_data"}
    df = loader.resample(base, tf)
    if df is None or len(df) < 200:
        return {"symbol": symbol, "error": "insufficient_bars"}
    strategy = StrategyRegistry.get(name, **params)
    feats = FeatureComputer.compute_for_strategy(df, strategy, symbol=symbol, timeframe=tf,
                                                 regime_params_file=cfg.regime_params_file)
    signals = strategy.generate_signals(feats, symbol)
    if hasattr(signals, "abs") and signals.abs().sum() == 0:
        return {"symbol": symbol, "error": "no_signals"}
    r = Backtester(cfg).run(feats, signals, symbol, strategy, spec=get_instrument_spec(symbol), timeframe=tf)
    pdd = path_dependent_dd(r.get("equity_curve", []))
    return {
        "symbol": symbol, "strategy": name, "tf": tf,
        "trades": int(r.get("total_trades", 0)),
        "return_pct": float(r.get("total_return_pct", 0.0)),
        "profit_factor": float(r.get("profit_factor", 0.0)),
        "win_rate": float(r.get("win_rate", 0.0)),
        "max_dd_pct": float(r.get("max_drawdown_pct", 0.0)),
        "sharpe": float(r.get("sharpe_ratio", 0.0)),
        "sortino": float(r.get("sortino_ratio", 0.0)),
        "std_weekly_return": float(r.get("std_weekly_return", 0.0)),
        "pct_positive_r": float(r.get("pct_positive_r", 0.0)),
        "max_consec_losses": int(r.get("max_consecutive_losses", 0)),
        "expectancy_pips": float(r.get("expectancy_pips", 0.0)),
        **pdd,
    }


def pooled_summary(rows):
    ok = [r for r in rows if "error" not in r]
    if not ok:
        return {}
    agg = lambda k: float(np.mean([r[k] for r in ok]))
    med = lambda k: float(np.median([r[k] for r in ok]))
    return {
        "n": len(ok),
        "mean_return_pct": round(agg("return_pct"), 2),
        "median_return_pct": round(med("return_pct"), 2),
        "mean_profit_factor": round(agg("profit_factor"), 3),
        "median_profit_factor": round(med("profit_factor"), 3),
        "mean_max_dd_pct": round(agg("max_dd_pct"), 2),
        "mean_ulcer_index": round(agg("ulcer_index"), 3),
        "mean_time_underwater_pct": round(agg("time_underwater_pct"), 2),
        "total_trades": int(sum(r["trades"] for r in ok)),
        "mean_win_rate": round(agg("win_rate"), 2),
    }


def _pct_delta(new, old):
    if old == 0:
        return float("inf") if new > 0 else (float("-inf") if new < 0 else 0.0)
    return (new - old) / abs(old) * 100.0


def is_improvement(baseline, candidate, material=5.0):
    """Tenet-5 gate. Net profit and profit factor are primary; a frequency regression is
    acceptable if profit rises materially and drawdown falls. Ambiguous cases return
    NEEDS_REVIEW (consult) rather than auto-accepting."""
    b, c = baseline["summary"], candidate["summary"]
    d = {
        "return": _pct_delta(c["mean_return_pct"], b["mean_return_pct"]),
        "pf": _pct_delta(c["mean_profit_factor"], b["mean_profit_factor"]),
        "max_dd": _pct_delta(c["mean_max_dd_pct"], b["mean_max_dd_pct"]),       # lower better
        "ulcer": _pct_delta(c["mean_ulcer_index"], b["mean_ulcer_index"]),     # lower better
        "time_uw": _pct_delta(c["mean_time_underwater_pct"], b["mean_time_underwater_pct"]),  # lower better
        "trades": _pct_delta(c["total_trades"], b["total_trades"]),
        "win_rate": _pct_delta(c["mean_win_rate"], b["mean_win_rate"]),
    }
    bmap = {r["symbol"]: r for r in baseline.get("rows", []) if "error" not in r}
    cmap = {r["symbol"]: r for r in candidate.get("rows", []) if "error" not in r}
    per_symbol = []
    for sym in sorted(set(bmap) & set(cmap)):
        rd = _pct_delta(cmap[sym]["return_pct"], bmap[sym]["return_pct"])
        pfd = _pct_delta(cmap[sym]["profit_factor"], bmap[sym]["profit_factor"])
        if rd < -material or pfd < -material:
            per_symbol.append({"symbol": sym, "return_delta_pct": round(rd, 2),
                               "pf_delta_pct": round(pfd, 2)})

    def result(verdict):
        if per_symbol and verdict in ("IMPROVEMENT", "NO_CHANGE", "TRADE_OFF_ACCEPT"):
            verdict = "NEEDS_REVIEW"
        return {"verdict": verdict, "deltas_pct": {k: round(v, 2) for k, v in d.items()},
                "per_symbol_regressions": per_symbol}

    if all(abs(v) < material for v in (d["return"], d["pf"], d["max_dd"], d["ulcer"])):
        return result("NO_CHANGE")

    profit_down = d["return"] < -material
    pf_down = d["pf"] < -material
    profit_up = d["return"] > material
    pf_up = d["pf"] > material
    risk_better = d["max_dd"] <= 0 and d["ulcer"] <= 0
    risk_worse = d["max_dd"] > material or d["ulcer"] > material

    if profit_down or pf_down:
        if (profit_up or pf_up) and risk_better:
            verdict = "TRADE_OFF_ACCEPT"  # one primary up strongly, risk improved
        else:
            verdict = "REGRESSION"
    elif (profit_up or pf_up) and not risk_worse:
        verdict = "IMPROVEMENT"
    elif risk_better and d["return"] >= -material and d["pf"] >= -material:
        verdict = "IMPROVEMENT"   # risk reduced, profit/PF held
    else:
        verdict = "NEEDS_REVIEW"
    return result(verdict)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--compare", nargs=2, metavar=("BASELINE_JSON", "CANDIDATE_JSON"),
                    help="Compare two benchmark records and emit a Tenet-5 verdict, then exit.")
    ap.add_argument("--symbols", nargs="*", default=DEFAULT_POOL)
    ap.add_argument("--all", action="store_true", help="Benchmark every symbol in the ledger.")
    ap.add_argument("--fx-only", action="store_true", help="Restrict to pure FX (fiat/fiat) symbols.")
    ap.add_argument("--ledger", default=str(REPO / "pm_configs_high_risk.json"))
    ap.add_argument("--spread-mode", default="fixed_spec")
    ap.add_argument("--label", default="baseline_v0")
    ap.add_argument("--out", default=str(REPO / "pm_outputs" / "benchmarks"))
    args = ap.parse_args()

    if args.compare:
        base = json.load(open(args.compare[0]))
        cand = json.load(open(args.compare[1]))
        v = is_improvement(base, cand)
        bc, cc = base.get("engine_commit", "?"), cand.get("engine_commit", "?")
        if bc != cc:
            print(f"WARNING: cross-engine comparison ({bc} vs {cc}) — verdict may be invalid; "
                  f"regenerate both on the same commit.")
        print(f"\n=== TENET-5 GATE: {base.get('label')} -> {cand.get('label')} ===")
        print(f"VERDICT: {v['verdict']}")
        for k, dv in v["deltas_pct"].items():
            print(f"  {k:10} {dv:+.2f}%")
        if v.get("per_symbol_regressions"):
            print("  per-symbol regressions (hidden by pooled mean):")
            for ps in v["per_symbol_regressions"]:
                print(f"    {ps['symbol']:8} return {ps['return_delta_pct']:+.1f}%  pf {ps['pf_delta_pct']:+.1f}%")
        return

    cfg = build_config(args.spread_mode)
    loader = DataLoader(cfg.data_dir)
    ledger = json.load(open(args.ledger))
    symbols = sorted(ledger.keys()) if args.all else args.symbols
    if args.fx_only:
        symbols = [s for s in symbols if is_fx(s)]

    rows = []
    for sym in symbols:
        if sym not in ledger:
            rows.append({"symbol": sym, "error": "not_in_ledger"}); continue
        try:
            rows.append(evaluate_winner(cfg, loader, ledger[sym]))
        except Exception as e:
            rows.append({"symbol": sym, "error": f"{type(e).__name__}: {e}"})

    summary = pooled_summary(rows)
    cols = ["symbol", "strategy", "tf", "trades", "return_pct", "profit_factor", "win_rate",
            "max_dd_pct", "ulcer_index", "time_underwater_pct", "max_dd_duration",
            "recovery_factor", "max_consec_losses"]
    print(f"\n=== BENCHMARK [{args.label}] spread_mode={args.spread_mode} ===")
    print("\t".join(cols))
    for r in rows:
        if "error" in r:
            print(f"{r['symbol']}\tERROR\t{r['error']}")
        else:
            print("\t".join(str(r.get(c, "")) for c in cols))
    print("\n-- pooled summary --")
    for k, v in summary.items():
        print(f"  {k}: {v}")

    out_dir = Path(args.out); out_dir.mkdir(parents=True, exist_ok=True)
    record = {"label": args.label, "spread_mode": args.spread_mode,
              "engine_commit": _git_head(), "symbols": symbols, "ledger": args.ledger,
              "summary": summary, "rows": rows}
    out_path = out_dir / f"{args.label}.json"
    json.dump(record, open(out_path, "w"), indent=2, default=str)
    print(f"\nwrote {out_path}")


if __name__ == "__main__":
    main()
