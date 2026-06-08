"""Accuracy proof: Numba JIT backtest loop == pure-Python loop (governance=None path).

The discovery workload always calls Backtester.run() with governance=None, which dispatches
to the Numba kernel _backtest_loop_numba when NUMBA_AVAILABLE, else _run_python_loop. Re-enabling
Numba (numpy<2.4) is only a legitimate efficiency win if the two loops are bit-for-bit equivalent
on that path. This harness PROVES it instead of trusting the code comment:

  * same df -> same features -> same signals fed to BOTH loops (computed once)
  * loop A: NUMBA_AVAILABLE=True  -> Numba kernel
  * loop B: NUMBA_AVAILABLE=False -> Python loop (monkeypatched module global)
  * discrete trade structure (entry_bar/exit_bar/direction/exit_reason) must match EXACTLY
  * continuous values (prices/pnl/equity/metrics) must match within ATOL

Run:  python tools/parity_numba_python.py [--symbols EURUSD GBPUSD ...] [--timeframes H1 M15]
Exit 0 = parity proven; exit 1 = divergence (a real accuracy issue, do NOT enable JIT blindly).
Also exposed as a pytest (test_numba_python_parity) so CI guards the invariant.
"""
import argparse
import sys
from pathlib import Path

import numpy as np

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))
sys.path.insert(0, str(REPO / "tools"))

import pm_core
from pm_core import Backtester, FeatureComputer, get_instrument_spec
from pm_strategies import StrategyRegistry
from benchmark import build_config, DataLoader

ATOL = 1e-6          # absolute tolerance for continuous values
DISCRETE_KEYS = ("entry_bar", "exit_bar", "signal_bar", "direction", "exit_reason")
CONT_KEYS = ("entry_price", "exit_price", "stop_loss", "take_profit",
             "position_size", "pnl_dollars", "pnl_pips")
METRIC_KEYS = ("total_trades", "total_return_pct", "profit_factor",
               "max_drawdown_pct", "sharpe_ratio", "final_equity")


def _run_once(cfg, feats, signals, symbol, strategy, tf, use_numba):
    saved = pm_core.NUMBA_AVAILABLE
    pm_core.NUMBA_AVAILABLE = bool(use_numba)
    try:
        return Backtester(cfg).run(feats, signals, symbol, strategy,
                                   spec=get_instrument_spec(symbol), timeframe=tf)
    finally:
        pm_core.NUMBA_AVAILABLE = saved


def _compare_cell(rn, rp):
    """Return list of (field, detail) divergences between numba result rn and python result rp."""
    diffs = []
    tn, tp = rn.get("trades", []), rp.get("trades", [])
    if len(tn) != len(tp):
        diffs.append(("trade_count", f"numba={len(tn)} python={len(tp)}"))
        return diffs, 0.0
    worst = 0.0
    for j, (a, b) in enumerate(zip(tn, tp)):
        for k in DISCRETE_KEYS:
            if a.get(k) != b.get(k):
                diffs.append((f"trade[{j}].{k}", f"numba={a.get(k)} python={b.get(k)}"))
        for k in CONT_KEYS:
            va, vb = float(a.get(k, 0.0) or 0.0), float(b.get(k, 0.0) or 0.0)
            d = abs(va - vb)
            worst = max(worst, d)
            if d > ATOL:
                diffs.append((f"trade[{j}].{k}", f"|{va}-{vb}|={d:.3e}"))
    en = np.asarray(rn.get("equity_curve", []), dtype=np.float64)
    ep = np.asarray(rp.get("equity_curve", []), dtype=np.float64)
    if en.shape != ep.shape:
        diffs.append(("equity_len", f"numba={en.shape} python={ep.shape}"))
    elif en.size:
        d = float(np.max(np.abs(en - ep)))
        worst = max(worst, d)
        if d > ATOL:
            diffs.append(("equity_curve", f"max|d|={d:.3e}"))
    for k in METRIC_KEYS:
        va, vb = rn.get(k), rp.get(k)
        try:
            d = abs(float(va) - float(vb))
        except (TypeError, ValueError):
            if va != vb:
                diffs.append((k, f"numba={va} python={vb}"))
            continue
        worst = max(worst, d)
        if d > ATOL:
            diffs.append((k, f"|{va}-{vb}|={d:.3e}"))
    return diffs, worst


def run_parity(symbols, timeframes, max_strategies, verbose=True):
    if not pm_core.NUMBA_AVAILABLE:
        print("FATAL: numba is not importable in this env; cannot prove parity. "
              "Install numpy<2.4 first.", file=sys.stderr)
        return 2, {}
    cfg = build_config("conservative_max")
    loader = DataLoader(cfg.data_dir)
    names = list(getattr(StrategyRegistry, "_strategies", {}).keys())[:max_strategies]

    cells = 0          # cells actually compared (had signals + trades)
    skipped = 0
    global_worst = 0.0
    all_diffs = []
    for symbol in symbols:
        base = loader.load_symbol(symbol)
        if base is None or len(base) == 0:
            continue
        for tf in timeframes:
            df = loader.resample(base, tf)
            if df is None or len(df) < 500:
                continue
            for name in names:
                try:
                    strategy = StrategyRegistry.get(name)
                    feats = FeatureComputer.compute_for_strategy(
                        df, strategy, symbol=symbol, timeframe=tf,
                        regime_params_file=cfg.regime_params_file)
                    signals = strategy.generate_signals(feats, symbol)
                except Exception:
                    skipped += 1
                    continue
                if hasattr(signals, "abs") and signals.abs().sum() == 0:
                    skipped += 1
                    continue
                rn = _run_once(cfg, feats, signals, symbol, strategy, tf, use_numba=True)
                rp = _run_once(cfg, feats, signals, symbol, strategy, tf, use_numba=False)
                if int(rn.get("total_trades", 0)) == 0:
                    skipped += 1
                    continue
                diffs, worst = _compare_cell(rn, rp)
                global_worst = max(global_worst, worst)
                cells += 1
                tag = "OK  " if not diffs else "DIFF"
                if verbose:
                    print(f"  [{tag}] {symbol:8} {tf:4} {name:34} "
                          f"trades={int(rn.get('total_trades',0)):4} worst|d|={worst:.2e}")
                if diffs:
                    all_diffs.append((symbol, tf, name, diffs))

    summary = {"cells": cells, "skipped": skipped, "worst_abs_diff": global_worst,
               "diverging_cells": len(all_diffs)}
    print(f"\n=== NUMBA vs PYTHON PARITY (governance=None) ===")
    print(f"engine numba={pm_core.numba.__version__ if hasattr(pm_core,'numba') else '?'} "
          f"numpy={np.__version__}")
    print(f"cells compared : {cells}")
    print(f"cells skipped  : {skipped} (no signals / no trades / unsupported)")
    print(f"worst |diff|   : {global_worst:.3e}  (tol {ATOL:.0e})")
    if all_diffs:
        print(f"\nDIVERGENCES in {len(all_diffs)} cell(s):")
        for symbol, tf, name, diffs in all_diffs[:20]:
            print(f"  {symbol} {tf} {name}:")
            for field, detail in diffs[:8]:
                print(f"      {field}: {detail}")
        return 1, summary
    if cells == 0:
        print("\nNO CELLS COMPARED (no tradable strategy/symbol found) -- inconclusive.")
        return 2, summary
    print(f"\nPARITY PROVEN: {cells} cells, all discrete trade decisions identical, "
          f"all continuous values within {ATOL:.0e}. JIT is accuracy-neutral on the discovery path.")
    return 0, summary


# --- pytest entrypoint (small, fast subset) ---
def test_numba_python_parity():
    code, summary = run_parity(["EURUSD", "GBPUSD"], ["H1", "M15"], max_strategies=40, verbose=False)
    assert code == 0, f"Numba/Python parity FAILED: {summary}"
    assert summary["cells"] >= 5, f"too few comparable cells ({summary['cells']}) to trust the proof"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--symbols", nargs="*", default=["EURUSD", "GBPUSD", "XAUUSD", "BTCUSD"])
    ap.add_argument("--timeframes", nargs="*", default=["H1", "M15", "M5"])
    ap.add_argument("--max-strategies", type=int, default=100)
    args = ap.parse_args()
    code, _ = run_parity(args.symbols, args.timeframes, args.max_strategies)
    return code


if __name__ == "__main__":
    raise SystemExit(main())
