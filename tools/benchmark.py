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
PF_CAP = 10.0
SPREAD_MODES = (
    "fixed_spec",
    "historical_max",
    "conservative_max",
    "stress_p90",
    "stress_p95",
    "stress_max",
)


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
    spread_model = dict(r.get("spread_model", {}) or {})
    return {
        "symbol": symbol, "strategy": name, "tf": tf,
        "asset_class": spread_model.get("asset_class", ""),
        "spread_provenance_status": spread_model.get("spread_provenance_status", ""),
        "spread_entry_column": spread_model.get("entry_column", ""),
        "spread_entry_model": spread_model.get("entry_model", ""),
        "spread_bar_column": spread_model.get("bar_column", ""),
        "mean_to_broker_spread_ratio": _finite_float(spread_model.get("mean_to_broker_spread_ratio", 0.0)),
        "max_to_broker_spread_ratio": _finite_float(spread_model.get("max_to_broker_spread_ratio", 0.0)),
        "trades": int(r.get("total_trades", 0)),
        "return_pct": _finite_float(r.get("total_return_pct", 0.0)),
        "profit_factor": _sanitize_profit_factor(r.get("profit_factor", 0.0)),
        "win_rate": _finite_float(r.get("win_rate", 0.0)),
        "max_dd_pct": _finite_float(r.get("max_drawdown_pct", 0.0)),
        "sharpe": _finite_float(r.get("sharpe_ratio", 0.0)),
        "sortino": _finite_float(r.get("sortino_ratio", 0.0)),
        "std_weekly_return": _finite_float(r.get("std_weekly_return", 0.0)),
        "pct_positive_r": _finite_float(r.get("pct_positive_r", 0.0)),
        "max_consec_losses": int(r.get("max_consecutive_losses", 0)),
        "expectancy_pips": _finite_float(r.get("expectancy_pips", 0.0)),
        **pdd,
    }


def pooled_summary(rows):
    ok = [r for r in rows if "error" not in r]
    if not ok:
        return {}
    def values(k):
        if k == "profit_factor":
            return [_sanitize_profit_factor(r.get(k, 0.0)) for r in ok]
        return [_finite_float(r.get(k, 0.0)) for r in ok]

    agg = lambda k: float(np.mean(values(k)))
    med = lambda k: float(np.median(values(k)))
    return {
        "n": len(ok),
        "mean_return_pct": round(agg("return_pct"), 2),
        "median_return_pct": round(med("return_pct"), 2),
        "mean_profit_factor": round(agg("profit_factor"), 3),
        "median_profit_factor": round(med("profit_factor"), 3),
        "mean_max_dd_pct": round(agg("max_dd_pct"), 2),
        "median_max_dd_pct": round(med("max_dd_pct"), 2),
        "mean_ulcer_index": round(agg("ulcer_index"), 3),
        "median_ulcer_index": round(med("ulcer_index"), 3),
        "mean_time_underwater_pct": round(agg("time_underwater_pct"), 2),
        "median_time_underwater_pct": round(med("time_underwater_pct"), 2),
        "total_trades": int(sum(r["trades"] for r in ok)),
        "mean_win_rate": round(agg("win_rate"), 2),
        "median_win_rate": round(med("win_rate"), 2),
    }


def pooled_summary_by_asset_class(rows):
    groups = {}
    for row in rows:
        if "error" in row:
            continue
        asset_class = str(row.get("asset_class") or "unknown")
        groups.setdefault(asset_class, []).append(row)
    return {asset_class: pooled_summary(group_rows) for asset_class, group_rows in sorted(groups.items())}


def _pct_delta(new, old, denom_floor=1e-9):
    denom = max(abs(float(old)), float(denom_floor))
    return (float(new) - float(old)) / denom * 100.0


def _finite_float(value, default=0.0):
    try:
        out = float(value)
    except Exception:
        return float(default)
    return out if math.isfinite(out) else float(default)


def _sanitize_profit_factor(value):
    try:
        pf = float(value)
    except Exception:
        return 0.0
    if math.isnan(pf):
        return 0.0
    if math.isinf(pf):
        return PF_CAP
    return max(0.0, min(PF_CAP, pf))


def _summary_value(summary, key, fallback_key=None):
    if key in summary:
        return _finite_float(summary.get(key))
    if fallback_key and fallback_key in summary:
        return _finite_float(summary.get(fallback_key))
    return 0.0


def is_improvement(baseline, candidate, material=5.0):
    """Tenet-5 gate. Net profit and profit factor are primary; a frequency regression is
    acceptable if profit rises materially and drawdown falls. Ambiguous cases return
    NEEDS_REVIEW (consult) rather than auto-accepting."""
    b, c = baseline["summary"], candidate["summary"]
    d = {
        "return": _pct_delta(_summary_value(c, "mean_return_pct"), _summary_value(b, "mean_return_pct")),
        "median_return": _pct_delta(_summary_value(c, "median_return_pct", "mean_return_pct"),
                                    _summary_value(b, "median_return_pct", "mean_return_pct")),
        # PF sits on an absolute breakeven hinge around 1.0; relative deltas near zero
        # are numerically dramatic but not decision-grade evidence.
        "pf": _pct_delta(_summary_value(c, "mean_profit_factor"), _summary_value(b, "mean_profit_factor"), denom_floor=1.0),
        "median_pf": _pct_delta(_summary_value(c, "median_profit_factor", "mean_profit_factor"),
                                _summary_value(b, "median_profit_factor", "mean_profit_factor"), denom_floor=1.0),
        "max_dd": _pct_delta(_summary_value(c, "mean_max_dd_pct"), _summary_value(b, "mean_max_dd_pct")),       # lower better
        "median_max_dd": _pct_delta(_summary_value(c, "median_max_dd_pct", "mean_max_dd_pct"),
                                    _summary_value(b, "median_max_dd_pct", "mean_max_dd_pct")),
        "ulcer": _pct_delta(_summary_value(c, "mean_ulcer_index"), _summary_value(b, "mean_ulcer_index")),     # lower better
        "median_ulcer": _pct_delta(_summary_value(c, "median_ulcer_index", "mean_ulcer_index"),
                                   _summary_value(b, "median_ulcer_index", "mean_ulcer_index")),
        "time_uw": _pct_delta(_summary_value(c, "mean_time_underwater_pct"), _summary_value(b, "mean_time_underwater_pct")),  # lower better
        "median_time_uw": _pct_delta(_summary_value(c, "median_time_underwater_pct", "mean_time_underwater_pct"),
                                     _summary_value(b, "median_time_underwater_pct", "mean_time_underwater_pct")),
        "trades": _pct_delta(_summary_value(c, "total_trades"), _summary_value(b, "total_trades")),
        "win_rate": _pct_delta(_summary_value(c, "mean_win_rate"), _summary_value(b, "mean_win_rate")),
        "median_win_rate": _pct_delta(_summary_value(c, "median_win_rate", "mean_win_rate"),
                                      _summary_value(b, "median_win_rate", "mean_win_rate")),
    }
    abs_d = {
        "return_pp": float(_summary_value(c, "mean_return_pct") - _summary_value(b, "mean_return_pct")),
        "median_return_pp": float(_summary_value(c, "median_return_pct", "mean_return_pct") -
                                  _summary_value(b, "median_return_pct", "mean_return_pct")),
        "pf": float(_summary_value(c, "mean_profit_factor") - _summary_value(b, "mean_profit_factor")),
        "median_pf": float(_summary_value(c, "median_profit_factor", "mean_profit_factor") -
                           _summary_value(b, "median_profit_factor", "mean_profit_factor")),
        "max_dd_pp": float(_summary_value(c, "mean_max_dd_pct") - _summary_value(b, "mean_max_dd_pct")),
        "median_max_dd_pp": float(_summary_value(c, "median_max_dd_pct", "mean_max_dd_pct") -
                                  _summary_value(b, "median_max_dd_pct", "mean_max_dd_pct")),
        "ulcer": float(_summary_value(c, "mean_ulcer_index") - _summary_value(b, "mean_ulcer_index")),
        "median_ulcer": float(_summary_value(c, "median_ulcer_index", "mean_ulcer_index") -
                              _summary_value(b, "median_ulcer_index", "mean_ulcer_index")),
        "time_uw_pp": float(_summary_value(c, "mean_time_underwater_pct") -
                            _summary_value(b, "mean_time_underwater_pct")),
        "median_time_uw_pp": float(_summary_value(c, "median_time_underwater_pct", "mean_time_underwater_pct") -
                                   _summary_value(b, "median_time_underwater_pct", "mean_time_underwater_pct")),
        "win_rate_pp": float(_summary_value(c, "mean_win_rate") - _summary_value(b, "mean_win_rate")),
        "median_win_rate_pp": float(_summary_value(c, "median_win_rate", "mean_win_rate") -
                                    _summary_value(b, "median_win_rate", "mean_win_rate")),
    }
    bmap = {r["symbol"]: r for r in baseline.get("rows", []) if "error" not in r}
    cmap = {r["symbol"]: r for r in candidate.get("rows", []) if "error" not in r}
    # Per-symbol scan covers return, PF, AND path-dependent risk (Tenet 2): a pooled mean
    # cannot hide a single-symbol drawdown/time-underwater blowup.
    per_symbol = []
    for sym in sorted(set(bmap) & set(cmap)):
        rd = _pct_delta(cmap[sym]["return_pct"], bmap[sym]["return_pct"])
        c_pf = _sanitize_profit_factor(cmap[sym].get("profit_factor", 0.0))
        b_pf = _sanitize_profit_factor(bmap[sym].get("profit_factor", 0.0))
        pf_abs = float(c_pf - b_pf)
        pfd = _pct_delta(c_pf, b_pf, denom_floor=1.0)
        ddd = _pct_delta(cmap[sym]["max_dd_pct"], bmap[sym]["max_dd_pct"])
        uld = _pct_delta(cmap[sym]["ulcer_index"], bmap[sym]["ulcer_index"])
        tud = _pct_delta(cmap[sym]["time_underwater_pct"], bmap[sym]["time_underwater_pct"])
        if rd < -material or (pfd < -material and pf_abs < -0.05) or ddd > material or uld > material or tud > material:
            per_symbol.append({"symbol": sym, "return_delta_pct": round(rd, 2),
                               "pf_delta_pct": round(pfd, 2), "pf_delta_abs": round(pf_abs, 4),
                               "max_dd_delta_pct": round(ddd, 2),
                               "ulcer_delta_pct": round(uld, 2), "time_uw_delta_pct": round(tud, 2)})

    def result(verdict):
        if per_symbol and verdict in ("IMPROVEMENT", "NO_CHANGE", "TRADE_OFF_ACCEPT"):
            verdict = "NEEDS_REVIEW"
        return {"verdict": verdict, "deltas_pct": {k: round(v, 2) for k, v in d.items()},
                "absolute_deltas": {k: round(v, 4) for k, v in abs_d.items()},
                "per_symbol_regressions": per_symbol}

    core_deltas = (
        d["return"], d["median_return"], d["pf"], d["median_pf"],
        d["max_dd"], d["median_max_dd"], d["ulcer"], d["median_ulcer"],
        d["time_uw"], d["median_time_uw"],
    )
    if all(abs(v) < material for v in core_deltas):
        return result("NO_CHANGE")

    profit_down = d["return"] < -material
    pf_down = d["pf"] < -material and abs_d["pf"] < -0.05
    profit_up = d["return"] > material
    pf_up = d["pf"] > material and abs_d["pf"] > 0.05
    median_profit_down = d["median_return"] < -material
    median_pf_down = d["median_pf"] < -material and abs_d["median_pf"] < -0.05
    median_profit_up = d["median_return"] > material
    median_pf_up = d["median_pf"] > material and abs_d["median_pf"] > 0.05
    # Path-dependent DD (max_dd, ulcer, time-underwater) is first-class risk per Tenet 2.
    risk_better = (
        d["max_dd"] <= 0 and d["median_max_dd"] <= 0 and
        d["ulcer"] <= 0 and d["median_ulcer"] <= 0 and
        d["time_uw"] <= 0 and d["median_time_uw"] <= 0
    )
    risk_worse = any(
        d[key] > material for key in
        ("max_dd", "median_max_dd", "ulcer", "median_ulcer", "time_uw", "median_time_uw")
    )
    mean_primary_up = profit_up or pf_up
    mean_primary_down = profit_down or pf_down
    median_primary_up = median_profit_up or median_pf_up
    median_primary_down = median_profit_down or median_pf_down

    # Means are allowed to start a verdict, but medians must confirm the sign. If they
    # disagree, the result is outlier-sensitive and must be reviewed rather than accepted.
    if (mean_primary_up and not median_primary_up) or (mean_primary_down and not median_primary_down):
        return result("NEEDS_REVIEW")

    if mean_primary_down:
        if mean_primary_up and median_primary_up and risk_better:
            verdict = "TRADE_OFF_ACCEPT"  # one primary up strongly, risk improved
        else:
            verdict = "REGRESSION"
    elif mean_primary_up and median_primary_up and not risk_worse:
        verdict = "IMPROVEMENT"
    elif risk_better and d["return"] >= -material and d["median_return"] >= -material and d["pf"] >= -material and d["median_pf"] >= -material:
        verdict = "IMPROVEMENT"   # risk reduced, profit/PF held
    else:
        verdict = "NEEDS_REVIEW"
    return result(verdict)


def _ok_symbols(record):
    return {
        str(row.get("symbol"))
        for row in record.get("rows", [])
        if isinstance(row, dict) and row.get("symbol") and "error" not in row
    }


def _requested_symbols(record):
    return {str(sym) for sym in record.get("symbols", [])}


def validate_like_for_like(base, cand):
    errors = []
    if str(base.get("spread_mode", "")) != str(cand.get("spread_mode", "")):
        errors.append(f"spread_mode mismatch: {base.get('spread_mode')} vs {cand.get('spread_mode')}")
    base_requested = _requested_symbols(base)
    cand_requested = _requested_symbols(cand)
    if base_requested != cand_requested:
        errors.append(
            "requested symbol set mismatch: "
            f"baseline_only={sorted(base_requested - cand_requested)} "
            f"candidate_only={sorted(cand_requested - base_requested)}"
        )
    base_ok = _ok_symbols(base)
    cand_ok = _ok_symbols(cand)
    if base_requested and base_ok != base_requested:
        errors.append(
            "baseline benchmark incomplete: "
            f"missing_or_error={sorted(base_requested - base_ok)}"
        )
    if cand_requested and cand_ok != cand_requested:
        errors.append(
            "candidate benchmark incomplete: "
            f"missing_or_error={sorted(cand_requested - cand_ok)}"
        )
    if base_ok != cand_ok:
        errors.append(
            "non-error benchmark symbol set mismatch: "
            f"baseline_only={sorted(base_ok - cand_ok)} "
            f"candidate_only={sorted(cand_ok - base_ok)}"
        )
    return errors


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--compare", nargs=2, metavar=("BASELINE_JSON", "CANDIDATE_JSON"),
                    help="Compare two benchmark records and emit a Tenet-5 verdict, then exit.")
    ap.add_argument("--symbols", nargs="*", default=DEFAULT_POOL)
    ap.add_argument("--all", action="store_true", help="Benchmark every symbol in the ledger.")
    ap.add_argument("--fx-only", action="store_true", help="Restrict to pure FX (fiat/fiat) symbols.")
    ap.add_argument("--ledger", default=str(REPO / "pm_configs.json"))
    ap.add_argument("--spread-mode", default="fixed_spec")
    ap.add_argument("--label", default="baseline_v0")
    ap.add_argument("--out", default=str(REPO / "pm_outputs" / "benchmarks"))
    args = ap.parse_args()

    if args.compare:
        base = json.load(open(args.compare[0]))
        cand = json.load(open(args.compare[1]))
        compare_errors = validate_like_for_like(base, cand)
        if compare_errors:
            print("\n=== TENET-5 GATE: INVALID LIKE-FOR-LIKE COMPARISON ===")
            for err in compare_errors:
                print(f"ERROR: {err}")
            raise SystemExit(2)
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
                print(f"    {ps['symbol']:8} ret {ps['return_delta_pct']:+.1f}%  pf {ps['pf_delta_pct']:+.1f}%  "
                      f"dd {ps['max_dd_delta_pct']:+.1f}%  ulcer {ps['ulcer_delta_pct']:+.1f}%  "
                      f"time_uw {ps['time_uw_delta_pct']:+.1f}%")
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
    cols = ["symbol", "asset_class", "spread_provenance_status", "mean_to_broker_spread_ratio",
            "strategy", "tf", "trades", "return_pct", "profit_factor", "win_rate",
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
    class_summary = pooled_summary_by_asset_class(rows)
    if len(class_summary) > 1:
        print("\n-- by asset class --")
        for asset_class, values in class_summary.items():
            print(
                f"  {asset_class}: n={values.get('n', 0)} "
                f"mean_ret={values.get('mean_return_pct')} "
                f"median_pf={values.get('median_profit_factor')} "
                f"trades={values.get('total_trades')}"
            )

    out_dir = Path(args.out); out_dir.mkdir(parents=True, exist_ok=True)
    record = {"label": args.label, "spread_mode": args.spread_mode,
              "engine_commit": _git_head(), "symbols": symbols, "ledger": args.ledger,
              "summary": summary, "class_summary": class_summary, "rows": rows}
    out_path = out_dir / f"{args.label}.json"
    json.dump(record, open(out_path, "w"), indent=2, default=str, allow_nan=False)
    print(f"\nwrote {out_path}")


if __name__ == "__main__":
    main()
