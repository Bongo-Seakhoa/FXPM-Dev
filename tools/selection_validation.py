"""Validate candidate-selection rules on forward OOS evidence.

Method (self-contained -- uses only the existing OOS returns matrix, no re-backtest, no
engine touch):
  * Per cell (symbol x timeframe x regime), split the OOS periods into a SELECTION half
    (earlier) and a FORWARD half (later) -- a genuine forward test of the selection rule.
    The unit is deliberately a specialist cell, not the portfolio. Portfolio balance
    diagnostics must not suppress a specialist at this layer unless a future experiment
    proves that improves live outcomes without reducing edge.
  * Each competing rule picks ONE candidate using ONLY the selection half:
       - volbal_geomean : percentile-normalized quality volume x balance
       - path_shape : equity-path topology only, useful as a dent detector
       - trace_gate_* : evidence-geometry tensor as a hard survivor gate, then a profit objective
       - evidence_geometry / geom_plus_trace: full tensor features, not a final-face-only score
       - pareto_topsis : Pareto -> percentile-norm -> TOPSIS
       - max_profit    : chase highest selection-half return (the overfit-prone baseline)
       - max_pf        : chase highest selection-half profit factor
       - max_ret_per_dd: highest selection-half return / Ulcer (single-ratio baseline)
  * Score each pick by its REALIZED forward-half return, raw forward drawdown, and
    rule runtime. In the geometry, that risk axis is interpreted as capital
    preservation (reverse drawdown, higher=better); the table keeps raw DD visible
    as an audit number.
  * Let the metrics disagree honestly: profitability, shape/risk, and runtime are reported
    separately, with a Pareto summary over reliability, capital preservation, and runtime.

Caveat: this validates the SELECTION RULE's generalization on the OOS surface; the ultimate
live proof remains the clean holdout (promotion_review). Returns are face-value matrix units,
identical scale across rules, so the comparison is valid.

Run:  python tools/selection_validation.py [--symbols EURUSD GBPUSD] [--min-survivors 4]
"""
import argparse
import sys
from pathlib import Path

import numpy as np

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO / "tools"))

from pareto_select import (load_symbol, rank_pool, pareto_mask, percentile_normalize,
                           AXIS_NAMES, axes_from_blocks,
                           path_features, PATH_FEATURE_NAMES,
                           TRACE_FEATURE_NAMES,
                           trace_geometry_features_from_normalized)  # reuse the core


def _slice_axes(r: np.ndarray):
    """Full 8-axis vector (higher=better) from a slice of per-period returns, identical to
    the production selector (all block-derived; face-value scale consistent across
    candidates within a cell)."""
    ax = axes_from_blocks(r)
    if ax is None:
        return None
    return np.array([ax[k] for k in AXIS_NAMES], dtype=np.float64)


def _trace_axis_matrix(r: np.ndarray, n_stages: int = 4):
    """Raw evidence_step x metric trace for one candidate.

    The important part happens later: traces are normalized across the candidate pool
    stage-by-stage, not inside a single candidate's own span. That is the cross-comparison
    topology Bongo described: a dimension stays flat when its relative quality stays flat,
    and a dent is a real loss of relative standing at that selection stage.

    Current evidence_step source: cumulative OOS return-block progress. This is already
    a valid evidence axis, but it is not yet a true per-trade-index tensor. Once the engine
    exports per-trade paths, this function is the insertion point for trade-count buckets.
    """
    r = np.asarray(r, dtype=np.float64)
    r = r[np.isfinite(r)]
    active = r[np.abs(r) > 1e-12]
    if active.size < 2:
        return None
    stages = [stage for stage in np.array_split(r, n_stages) if stage.size]
    rows = []
    for idx in range(1, len(stages) + 1):
        prefix = np.concatenate(stages[:idx])
        ax = axes_from_blocks(prefix)
        if ax is None:
            # A prefix can be too early/thin to form ratios yet. Keep the candidate in
            # the topology instead of deleting it: this creates a flat weak starting
            # face, allowing later expansion/timing quality to be compared.
            rows.append([0.0 for _name in AXIS_NAMES])
        else:
            rows.append([float(ax[name]) for name in AXIS_NAMES])
    if len(rows) < 2:
        return None
    return np.asarray(rows, dtype=np.float64)


def _trace_features_from_normalized(trace: np.ndarray) -> np.ndarray:
    """Features from one candidate's pool-normalized evidence geometry.

    Input values are already [0,1] rank/percentile quality at each stage and axis. Higher
    is better. This makes the "3D" object comparable without letting raw axis units or tiny
    within-candidate movements dominate the result.
    """
    return trace_geometry_features_from_normalized(trace)


def _percentile_normalize_with_nan(M: np.ndarray) -> np.ndarray:
    """Percentile-normalize each axis while preserving missing train axes as NaN."""
    M = np.asarray(M, dtype=np.float64)
    out = np.full_like(M, np.nan, dtype=np.float64)
    for c in range(M.shape[1]):
        mask = np.isfinite(M[:, c])
        if not mask.any():
            continue
        vals = M[mask, c:c+1]
        out[mask, c] = percentile_normalize(vals)[:, 0]
    return out


def _train_axes_from_record(rec: dict) -> np.ndarray:
    """Best-effort train face aligned to AXIS_NAMES.

    Missing axes stay NaN and are ignored in train-to-OOS alignment. This keeps the
    feature honest: we use train information where the manifest provides comparable
    evidence, without inventing fake dimensions.
    """
    tm = rec.get("train_metrics") or {}
    out = np.full(len(AXIS_NAMES), np.nan, dtype=np.float64)
    if not isinstance(tm, dict) or not tm:
        return out

    def f(name, default=np.nan):
        try:
            value = tm.get(name, default)
            return float(value) if value is not None else default
        except Exception:
            return default

    idx = {name: i for i, name in enumerate(AXIS_NAMES)}
    out[idx["net_return"]] = f("total_return_pct")
    out[idx["profit_factor"]] = f("profit_factor")
    out[idx["sortino"]] = f("sortino_ratio", f("sharpe_ratio"))
    dd = f("max_drawdown_pct")
    ret = out[idx["net_return"]]
    if np.isfinite(ret) and np.isfinite(dd):
        out[idx["recovery"]] = ret / max(abs(dd), 1e-9)
        out[idx["ulcer_control"]] = -dd
    out[idx["tail_cvar"]] = f("worst_5pct_r")
    out[idx["temporal_consistency"]] = f("pct_positive_r", f("win_rate"))
    # No true train equity-R2 is emitted yet. Leave it missing rather than guessing.
    return out


def _pool_trace_features(raw_traces, train_axes=None) -> np.ndarray:
    """Pool-normalize stage x axis traces and return topology features per candidate."""
    tensor = np.asarray(raw_traces, dtype=np.float64)
    if tensor.ndim != 3:
        raise ValueError("raw_traces must have shape [candidates, stages, axes]")
    n, stages, _axes = tensor.shape
    norm = np.zeros_like(tensor, dtype=np.float64)
    for stage_idx in range(stages):
        norm[:, stage_idx, :] = percentile_normalize(tensor[:, stage_idx, :])
    train_norm = None
    if train_axes is not None:
        train_arr = np.asarray(train_axes, dtype=np.float64)
        if train_arr.shape == (n, tensor.shape[2]):
            train_norm = _percentile_normalize_with_nan(train_arr)
    return np.array([
        trace_geometry_features_from_normalized(
            norm[i],
            None if train_norm is None else train_norm[i],
        )
        for i in range(n)
    ], dtype=np.float64)


def _forward_return(r: np.ndarray) -> float:
    r = np.asarray(r, dtype=np.float64)
    if not np.any(np.abs(r) > 1e-12):
        return 0.0
    return float((np.cumprod(1.0 + r)[-1] - 1.0) * 100.0)


def _forward_dd(r: np.ndarray) -> float:
    """Forward-half max drawdown (%) of the pick.

    Printed as raw lower-is-better DD for auditability; selection geometry treats
    the corresponding axis as capital preservation (reverse DD, higher=better).
    """
    r = np.asarray(r, dtype=np.float64)
    if not np.any(np.abs(r) > 1e-12):
        return 0.0
    eq = np.cumprod(1.0 + r)
    peak = np.maximum.accumulate(eq)
    dd = np.where(peak > 0, (peak - eq) / peak, 0.0) * 100.0
    return float(dd.max())


def _train_trades(rec: dict) -> int:
    train = rec.get("train_metrics") or {}
    if not isinstance(train, dict):
        return 0
    try:
        return int(train.get("total_trades", 0) or 0)
    except Exception:
        return 0


def _selection_dd(r: np.ndarray) -> float:
    return _forward_dd(r)


def _candidate_is_gate_survivor(rec: dict, selection_returns: np.ndarray, min_trades: int, dd_ceiling: float) -> bool:
    """Gate using only pre-forward evidence.

    Do not read rec["metrics"] here: those metrics cover the full OOS matrix and
    leak the forward half into this selector-validation experiment.
    """
    return _train_trades(rec) >= min_trades and _selection_dd(selection_returns) <= dd_ceiling


RULES = ("volbal_geomean", "path_shape", "geom_plus_path",
         "shape_gate_pf", "shape_gate_geomean",
         "trace_gate_pf", "trace_gate_geomean",
         "evidence_geometry", "geom_plus_trace",
         "pareto_topsis", "max_profit", "max_pf", "max_ret_per_dd")


def _shape_survivors(sel_path: np.ndarray) -> np.ndarray:
    """Threshold-free path-shape gate: keep the path-topology Pareto frontier.

    This uses the candidate pool itself as the scale. It rejects candidates whose path
    shape is strictly dominated by another candidate on every topology axis, while
    preserving trade-offs for the profit objective to decide.
    """
    if sel_path.shape[0] <= 1:
        return np.arange(sel_path.shape[0])
    mask = pareto_mask(sel_path)
    idx = np.flatnonzero(mask)
    return idx if idx.size else np.arange(sel_path.shape[0])


def _trace_survivors(sel_trace: np.ndarray) -> np.ndarray:
    """Keep candidates not dominated on axis-by-time topology features."""
    if sel_trace.shape[0] <= 1:
        return np.arange(sel_trace.shape[0])
    mask = pareto_mask(sel_trace)
    idx = np.flatnonzero(mask)
    return idx if idx.size else np.arange(sel_trace.shape[0])


def _pick(rule, sel_axes, sel_path, sel_trace):
    """sel_axes: (n, 8) window axes; path/trace features are higher=better.
    Return index of the chosen candidate."""
    if rule == "volbal_geomean":                 # flattened window-axis geometry (volume x balance)
        order, _, _, _, _ = rank_pool(sel_axes, method="geomean")
        return int(order[0])
    if rule == "path_shape":                     # Bongo's path topology ONLY (dent-sensitive)
        order, _, _, _, _ = rank_pool(sel_path, method="geomean")
        return int(order[0])
    if rule == "geom_plus_path":                 # window quality + path topology (full object)
        order, _, _, _, _ = rank_pool(np.hstack([sel_axes, sel_path]), method="geomean")
        return int(order[0])
    if rule == "shape_gate_pf":                  # shape is a gate; PF chooses among healthy paths
        idx = _shape_survivors(sel_path)
        return int(idx[int(np.argmax(sel_axes[idx, 1]))])
    if rule == "shape_gate_geomean":             # shape is a gate; volume-balance chooses survivor
        idx = _shape_survivors(sel_path)
        order, _, _, _, _ = rank_pool(sel_axes[idx], method="geomean")
        return int(idx[int(order[0])])
    if rule == "trace_gate_pf":                  # axis-by-time topology gate; PF chooses survivor
        idx = _trace_survivors(sel_trace)
        return int(idx[int(np.argmax(sel_axes[idx, 1]))])
    if rule == "trace_gate_geomean":             # axis-by-time topology gate; geomean chooses survivor
        idx = _trace_survivors(sel_trace)
        order, _, _, _, _ = rank_pool(sel_axes[idx], method="geomean")
        return int(idx[int(order[0])])
    if rule == "evidence_geometry":              # full evidence object, no final-face-only objective
        order, _, _, _, _ = rank_pool(sel_trace, method="geomean")
        return int(order[0])
    if rule == "geom_plus_trace":                # final face + full evidence-geometry object
        order, _, _, _, _ = rank_pool(np.hstack([sel_axes, sel_trace]), method="geomean")
        return int(order[0])
    if rule == "pareto_topsis":
        order, _, _, _, _ = rank_pool(sel_axes, method="topsis")
        return int(order[0])
    if rule == "max_profit":
        return int(np.argmax(sel_axes[:, 0]))
    if rule == "max_pf":
        return int(np.argmax(sel_axes[:, 1]))
    if rule == "max_ret_per_dd":
        return int(np.argmax(sel_axes[:, 3]))
    raise ValueError(rule)


def run(symbols, evidence_dir, min_trades, dd_ceiling, min_survivors):
    import time
    from collections import defaultdict
    fwd = {k: [] for k in RULES}        # forward returns of each rule's picks
    fwd_dd = {k: [] for k in RULES}     # raw forward max-DD; cap-pres is its reverse
    rt = {k: 0.0 for k in RULES}        # cumulative selection runtime per rule (seconds)
    cells_used = 0
    for sym in symbols:
        m, mat = load_symbol(sym, evidence_dir)
        if m is None or mat is None:
            continue
        returns = mat["returns"]
        P = returns.shape[0]
        half = P // 2
        cells = defaultdict(list)
        records_by_col = {}
        for rec in m["records"]:
            col = rec.get("matrix_column")
            if col is None or col >= returns.shape[1]:
                continue
            records_by_col[int(col)] = rec
            selection_returns = np.asarray(returns[:half, int(col)], dtype=np.float64)
            if not _candidate_is_gate_survivor(rec, selection_returns, min_trades, dd_ceiling):
                continue
            regime = rec.get("tuned_for_regime") or "BASE"
            cells[(rec["timeframe"], regime)].append(col)
        for key, cols in cells.items():
            if len(cols) < min_survivors:
                continue
            sel, selp, selt, train_axes, fwdret, fwddd = [], [], [], [], [], []
            for col in cols:
                rec = records_by_col.get(int(col), {})
                s = _slice_axes(returns[:half, col])
                p = path_features(returns[:half, col])
                t = _trace_axis_matrix(returns[:half, col])
                if s is None or p is None or t is None:
                    sel.append(None); selp.append(None); selt.append(None); train_axes.append(None)
                    fwdret.append(None); fwddd.append(None); continue
                sel.append(s); selp.append(np.array([p[k] for k in PATH_FEATURE_NAMES], dtype=np.float64))
                selt.append(t)
                train_axes.append(_train_axes_from_record(rec))
                fwdret.append(_forward_return(returns[half:, col]))
                fwddd.append(_forward_dd(returns[half:, col]))
            valid = [i for i, s in enumerate(sel) if s is not None and fwdret[i] is not None]
            if len(valid) < min_survivors:
                continue
            sel_axes = np.array([sel[i] for i in valid], dtype=np.float64)
            sel_path = np.array([selp[i] for i in valid], dtype=np.float64)
            sel_train = np.array([train_axes[i] for i in valid], dtype=np.float64)
            sel_trace = _pool_trace_features([selt[i] for i in valid], sel_train)
            fwd_vals = np.array([fwdret[i] for i in valid], dtype=np.float64)
            dd_vals = np.array([fwddd[i] for i in valid], dtype=np.float64)
            cells_used += 1
            for rule in RULES:
                t0 = time.perf_counter()
                pick = _pick(rule, sel_axes, sel_path, sel_trace)
                rt[rule] += time.perf_counter() - t0
                fwd[rule].append(float(fwd_vals[pick]))
                fwd_dd[rule].append(float(dd_vals[pick]))
    return fwd, fwd_dd, rt, cells_used


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--symbols", nargs="*")
    ap.add_argument("--evidence-dir", default=str(REPO / "pm_outputs" / "production_live" / "oos_evidence"))
    ap.add_argument("--min-trades", type=int, default=20)
    ap.add_argument("--dd-ceiling", type=float, default=35.0)
    ap.add_argument("--min-survivors", type=int, default=4)
    args = ap.parse_args()
    ev = Path(args.evidence_dir)
    symbols = args.symbols or sorted({p.name.split("_oos_manifest")[0] for p in ev.glob("*_oos_manifest.json")})
    fwd, fwd_dd, rt, cells = run(symbols, ev, args.min_trades, args.dd_ceiling, args.min_survivors)
    print(f"=== SELECTION VALIDATION (forward-half realized performance of each rule's pick) ===")
    print(f"symbols={symbols} cells_evaluated={cells} (min_survivors={args.min_survivors})")
    if cells == 0:
        print("no cells with enough gate-surviving candidates yet."); return 0
    # Reliability-adjusted score = median forward return x %profitable. Mean alone rewards
    # lottery picks (a few big hits); the goal is RELIABLE forward money, so we rank by a
    # measure that punishes coin-flip reliability. fwd_dd% is raw mean forward max-DD
    # for auditability; the corresponding geometry axis is capital preservation
    # (reverse DD, higher=better). A fair yardstick credits low forward DD, but does
    # not pretend low-DD/low-return is better than profitable reliability.
    def reliab(v):
        return float(np.median(v) * (v > 0).mean())
    print(f"\n{'rule':18} {'mean%':>8} {'median%':>8} {'%prof':>7} {'fwd_dd%':>8} {'us/cell':>8} {'reliab':>8}  (reliab=median x %prof; fwd_dd lower better = higher capital preservation; us/cell=selection runtime)")
    scored = {}
    table = {}
    for rule in RULES:
        v = np.array(fwd[rule])
        scored[rule] = reliab(v)
        us_per_cell = (rt[rule] / cells * 1e6) if cells else 0.0
        mean_dd = float(np.mean(fwd_dd[rule]))
        table[rule] = {
            "reliab": scored[rule],
            "median_return": float(np.median(v)),
            "profitable_pct": float((v > 0).mean() * 100.0),
            "forward_dd": mean_dd,
            "us_per_cell": us_per_cell,
        }
        print(f"{rule:18} {v.mean():>8.2f} {np.median(v):>8.2f} {float((v>0).mean()*100):>6.0f}% {mean_dd:>8.2f} {us_per_cell:>8.0f} {scored[rule]:>8.3f}")

    winner_return = max(scored, key=scored.get)
    winner_dd = min(table, key=lambda rule: table[rule]["forward_dd"])
    winner_runtime = min(table, key=lambda rule: table[rule]["us_per_cell"])
    metric_matrix = np.array([
        [table[rule]["reliab"], -table[rule]["forward_dd"], -table[rule]["us_per_cell"]]
        for rule in RULES
    ], dtype=np.float64)
    efficient = [rule for rule, keep in zip(RULES, pareto_mask(metric_matrix)) if keep]

    print(f"\nVERDICT BY DIMENSION:")
    print(f"- best forward profitability/reliability: {winner_return} ({scored[winner_return]:.3f})")
    print(f"- best forward capital preservation / lowest raw drawdown: {winner_dd} ({table[winner_dd]['forward_dd']:.2f}% raw DD)")
    print(f"- fastest selector runtime: {winner_runtime} ({table[winner_runtime]['us_per_cell']:.0f} us/cell)")
    print(f"- non-dominated over reliability + capital preservation + runtime: {', '.join(efficient)}")
    if "shape_gate_pf" in efficient or "shape_gate_geomean" in efficient:
        print("-> shape-as-gate is competitive on at least one quality/runtime frontier; re-test as more symbols land.")
    else:
        print("-> shape-as-gate is not on the current frontier; keep it experimental until more evidence supports it.")
    print(f"NOTE: only {cells} cells / {len(symbols)} symbols so far -- treat as directional, not final.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
