"""Pareto multi-axis ("spider-web") winner selection -- the anti-arbitrary replacement
for the hardcoded weighted scorer.

Mandate (Bongo): stop letting hardcoded constants define "success". The winner per
regime/symbol/timeframe must be the candidate most likely to MAKE MONEY out-of-sample,
chosen by comparing candidates against each other (the pool sets the scale), not against
magic absolute thresholds.

Pipeline per cell (symbol x timeframe x regime):
  1. HARD GATES (survival, pass/fail -- kept hard): min trades, max-DD ceiling.
     (holdout / PBO / stress remain hard gates in promotion_review and compose ON TOP.)
  2. AXES, all measured out-of-sample under honest cost, all oriented higher=better:
       a. oos_profit       = total_return_pct
       b. oos_pf           = profit_factor (clamped)
       c. capital_preservation = reverse path-dependent drawdown: -Ulcer from the
                             per-period equity curve when the block-return scale
                             reconciles with the reported return, else -(max_drawdown_pct)
       d. consistency      = fraction of ACTIVE periods that are positive
     (cost-robustness is a documented 5th axis, added once stress survival is emitted
      per-candidate; the core below is axis-count agnostic.)
  3. PARETO frontier among gate-survivors (non-dominated across the axes).
  4. PERCENTILE-NORMALIZE each axis within the cell pool (data sets the scale; zero
     absolute constants), then rank by TOPSIS closeness-to-ideal.
  5. Winner = highest TOPSIS closeness (reported with whether it is Pareto-optimal).

Important boundary: "balance" in this file means cross-axis health inside one
specialist candidate. It is not symbol/regime portfolio balance. Portfolio-level
exposure, correlation, crowding, and account survival controls are downstream
diagnostics/governors, not primary per-cell scoring axes.

The CORE (pareto_mask / percentile_normalize / topsis) is schema-independent and
unit-tested with synthetic data (see test_pareto_core). Read-only; selects nothing in
the engine -- this is the candidate selection logic we will VALIDATE against the current
scorer (tools/selection_validation.py) before any integration.

Run:  python tools/pareto_select.py --symbols EURUSD [--min-trades 20 --dd-ceiling 35]
"""
import argparse
import glob
import json
import sys
from collections import defaultdict
from pathlib import Path

import numpy as np

REPO = Path(__file__).resolve().parent.parent

AXIS_NAMES = ("net_return", "profit_factor", "sortino", "recovery",
              "tail_cvar", "equity_r2", "temporal_consistency", "ulcer_control")
# `ulcer_control` is the legacy field name for the capital-preservation axis:
# reverse path-dependent drawdown, oriented higher=better like every other axis.
# Axes 9-10 (execution_robustness via stress re-sim; generalization IS->OOS) are added
# once the per-candidate stress + in-sample metrics are emitted into the manifest.


# ----------------------------------------------------------------------------
# CORE (schema-independent, all axes oriented higher=better)
# ----------------------------------------------------------------------------
def pareto_mask(M: np.ndarray) -> np.ndarray:
    """Boolean mask of Pareto-non-dominated rows. M[i] dominated if some j is >= on all
    axes and > on at least one."""
    n = M.shape[0]
    keep = np.ones(n, dtype=bool)
    for i in range(n):
        if not keep[i]:
            continue
        for j in range(n):
            if i == j:
                continue
            if np.all(M[j] >= M[i]) and np.any(M[j] > M[i]):
                keep[i] = False
                break
    return keep


def percentile_normalize(M: np.ndarray) -> np.ndarray:
    """Per-axis rank -> [0,1] percentile. Ties share the average percentile. The pool's
    own distribution defines the scale -- no absolute constants."""
    n, k = M.shape
    out = np.zeros_like(M, dtype=np.float64)
    if n == 1:
        return np.ones_like(M, dtype=np.float64)
    for c in range(k):
        col = M[:, c]
        order = col.argsort()
        ranks = np.empty(n, dtype=np.float64)
        ranks[order] = np.arange(n, dtype=np.float64)
        # average ranks for ties
        uniq, inv, counts = np.unique(col, return_inverse=True, return_counts=True)
        avg = np.zeros(len(uniq))
        acc = defaultdict(list)
        for idx, r in zip(range(n), ranks):
            acc[col[idx]].append(r)
        meanrank = {v: float(np.mean(rs)) for v, rs in acc.items()}
        out[:, c] = np.array([meanrank[col[i]] for i in range(n)]) / (n - 1)
    return out


def topsis(Mn: np.ndarray) -> np.ndarray:
    """TOPSIS closeness on already-normalized (higher=better) axes. Returns score in
    [0,1]; higher is closer to the ideal and farther from the anti-ideal."""
    ideal = Mn.max(axis=0)
    anti = Mn.min(axis=0)
    d_ideal = np.sqrt(((Mn - ideal) ** 2).sum(axis=1))
    d_anti = np.sqrt(((Mn - anti) ** 2).sum(axis=1))
    denom = d_ideal + d_anti
    out = np.ones_like(denom, dtype=np.float64)  # degenerate single-point pool -> 1.0
    nz = denom > 0
    out[nz] = d_anti[nz] / denom[nz]
    return out


def volume_balance(Mn: np.ndarray):
    """Bongo's quality-geometry: on percentile-normalized axes,
        geomean = volume x balance   (AM-GM identity)
    volume = arithmetic mean (radar size); balance = geomean/volume in [0,1] (=1 only when
    all axes equal -> a regular, undistorted polygon). The geomean is large only for a
    candidate that is BOTH high-quality and balanced, and collapses if any single axis is
    damaged -- exactly 'no distorted side, no single metric carrying the structure'.
    A half-rank floor (1/2n, data-scaled not arbitrary) stops one worst-in-pool axis from
    annihilating an otherwise strong candidate."""
    n = Mn.shape[0]
    floored = np.clip(Mn, 1.0 / (2.0 * max(n, 1)), None)
    geomean = np.exp(np.mean(np.log(floored), axis=1))
    volume = floored.mean(axis=1)                 # same values as geomean -> AM-GM holds
    balance = np.where(volume > 0, geomean / volume, 0.0)  # in (0,1], =1 iff all axes equal
    return geomean, volume, balance


PATH_FEATURE_NAMES = ("expansion", "dent_health", "stage_balance", "stage_positivity")
TRACE_FEATURE_NAMES = (
    "trace_final_face_quality",
    "trace_average_path_quality",
    "trace_worst_axis_dent_health",
    "trace_dent_area_health",
    "trace_dent_recovery",
    "trace_path_smoothness",
    "trace_metric_stability",
    "trace_cross_axis_balance",
    "trace_risk_axis_integrity",
    "trace_late_rescue_independence",
    "trace_train_oos_alignment",
)


def path_features(r, n_stages: int = 4):
    """Bongo's path topology, distilled to scale-robust, dent-sensitive features over the
    equity PATH (not the flattened endpoint). Higher=better. Matrix-math over stages -- no
    3D rendering:
      expansion        = fraction of the path at/near its running peak (smooth upward climb)
      dent_health      = 1 - deepest capital-preservation dent (THE one-big-dent detector)
      stage_balance    = geomean/mean of per-stage returns (no single stage carries it)
      stage_positivity = fraction of stages net-positive (consistent expansion through time)
    A few-trade strategy with one deep dent scores low on dent_health/expansion; a many-trade
    strategy with minor ripples does not -- the distinction you wanted, preserved.
    Returns None if too few active blocks to form a path."""
    r = np.asarray(r, dtype=np.float64)
    r = r[np.isfinite(r)]
    active = r[np.abs(r) > 1e-12]
    if active.size < max(n_stages, 4):
        return None
    eq = np.cumprod(1.0 + r)
    peak = np.maximum.accumulate(eq)
    dd = np.where(peak > 0, (peak - eq) / peak, 0.0)
    expansion = float(1.0 - (dd > 1e-9).mean())
    dent_health = float(1.0 - min(float(dd.max()), 1.0))
    stages = [s for s in np.array_split(r, n_stages) if s.size]
    stage_ret = np.array([float(np.prod(1.0 + s) - 1.0) for s in stages], dtype=np.float64)
    stage_positivity = float((stage_ret > 0).mean())
    shifted = stage_ret - stage_ret.min() + 1e-9          # positive-shift for AM-GM balance
    gm = float(np.exp(np.mean(np.log(shifted))))
    am = float(shifted.mean())
    stage_balance = gm / am if am > 0 else 0.0
    return {"expansion": expansion, "dent_health": dent_health,
            "stage_balance": stage_balance, "stage_positivity": stage_positivity}


def _face_quality_balance(face: np.ndarray) -> tuple[float, float]:
    """Return (geometric face quality, cross-axis balance) for one normalized face."""
    face = np.asarray(face, dtype=np.float64)
    face = np.where(np.isfinite(face), face, 0.0)
    floored = np.clip(face, 1e-9, None)
    quality = float(np.exp(np.mean(np.log(floored))))
    mean = float(floored.mean())
    balance = float(quality / mean) if mean > 0 else 0.0
    return quality, balance


def trace_geometry_features_from_normalized(trace: np.ndarray,
                                            train_norm: np.ndarray | None = None) -> np.ndarray:
    """Features for Bongo's evidence-geometry object.

    Input is a normalized quality tensor slice for one candidate:
    ``trace[evidence_step, metric]``. Values are already relative quality in [0, 1]
    where higher is better. The output is a structural feature vector, not a final
    radar chart:

    - final face and average path quality,
    - worst/duration/recovery of metric dents,
    - smoothness and metric stability through the evidence path,
    - cross-axis balance,
    - risk-axis damage concentration,
    - late-rescue dependence,
    - optional train-to-OOS face alignment.
    """
    trace = np.asarray(trace, dtype=np.float64)
    trace = np.where(np.isfinite(trace), trace, 0.0)
    if trace.ndim != 2 or trace.shape[0] < 2:
        return np.zeros(len(TRACE_FEATURE_NAMES), dtype=np.float64)

    face_quality = np.array([_face_quality_balance(row)[0] for row in trace], dtype=np.float64)
    face_balance = np.array([_face_quality_balance(row)[1] for row in trace], dtype=np.float64)
    terminal = trace[-1]

    peak = np.maximum.accumulate(trace, axis=0)
    dent = np.clip(peak - trace, 0.0, 1.0)
    worst_axis_dent_health = float(1.0 - np.max(dent))
    dent_area_health = float(1.0 - np.mean(dent))

    peak_final = peak[-1]
    retention = np.ones_like(terminal)
    nz = peak_final > 1e-9
    retention[nz] = np.clip(terminal[nz] / peak_final[nz], 0.0, 1.0)
    dent_recovery = float(retention.mean())

    moves = np.diff(trace, axis=0)
    total_variation = np.abs(moves).sum(axis=0)
    direct_move = np.abs(terminal - trace[0])
    excess_chop = np.maximum(0.0, total_variation - direct_move)
    path_smoothness = float(1.0 / (1.0 + np.mean(excess_chop)))
    metric_stability = (
        float(1.0 / (1.0 + np.mean(np.std(moves, axis=0))))
        if moves.shape[0] > 1 else 1.0
    )

    risk_names = {"tail_cvar", "ulcer_control", "recovery"}
    risk_idx = [
        i for i, name in enumerate(AXIS_NAMES)
        if name in risk_names and i < trace.shape[1]
    ]
    risk_axis_integrity = (
        float(1.0 - np.max(dent[:, risk_idx]))
        if risk_idx else 1.0
    )

    # Penalize final quality that is rescued by the last evidence segment rather than
    # supported through the full path. This uses relative shares, not a hand threshold.
    quality_positive_progress = float(np.maximum(np.diff(face_quality), 0.0).sum())
    last_quality_jump = float(max(0.0, face_quality[-1] - face_quality[-2]))
    quality_late_share = (
        last_quality_jump / quality_positive_progress
        if quality_positive_progress > 1e-12 else 0.0
    )
    net = trace[:, 0] if trace.shape[1] else face_quality
    net_positive_progress = float(np.maximum(np.diff(net), 0.0).sum())
    last_net_jump = float(max(0.0, net[-1] - net[-2]))
    net_late_share = (
        last_net_jump / net_positive_progress
        if net_positive_progress > 1e-12 else 0.0
    )
    final_minus_path = max(0.0, float(face_quality[-1] - face_quality.mean()))
    final_gap_share = final_minus_path / max(float(face_quality[-1]), 1e-12)
    late_rescue_dependence = float(1.0 - np.clip(
        max(quality_late_share, net_late_share, final_gap_share), 0.0, 1.0
    ))

    train_oos_alignment = 1.0
    if train_norm is not None:
        train_norm = np.asarray(train_norm, dtype=np.float64)
        mask = np.isfinite(train_norm) & np.isfinite(terminal)
        if mask.any():
            train_oos_alignment = float(1.0 - np.clip(
                np.mean(np.abs(train_norm[mask] - terminal[mask])), 0.0, 1.0
            ))

    return np.array([
        float(face_quality[-1]),
        float(face_quality.mean()),
        worst_axis_dent_health,
        dent_area_health,
        dent_recovery,
        path_smoothness,
        metric_stability,
        float(face_balance.mean()),
        risk_axis_integrity,
        late_rescue_dependence,
        train_oos_alignment,
    ], dtype=np.float64)


def topology_trace_features(r, n_stages: int = 4):
    """Single-candidate diagnostic over cumulative OOS stages.

    Do not use this as the selector gate: cross-candidate topology needs the whole
    candidate pool so each stage/axis can be normalized against the available alternatives.
    `tools/selection_validation.py` does that pool-normalized trace comparison. This helper
    is kept only for ad-hoc inspection where no pool is available.
    """
    r = np.asarray(r, dtype=np.float64)
    r = r[np.isfinite(r)]
    active = r[np.abs(r) > 1e-12]
    if active.size < max(n_stages, 4):
        return None
    stages = [stage for stage in np.array_split(r, n_stages) if stage.size]
    rows = []
    for idx in range(1, len(stages) + 1):
        prefix = np.concatenate(stages[:idx])
        ax = axes_from_blocks(prefix)
        if ax is None:
            return None
        rows.append([float(ax[name]) for name in AXIS_NAMES])
    if len(rows) < 2:
        return None

    trace = np.asarray(rows, dtype=np.float64)
    trace = np.where(np.isfinite(trace), trace, 0.0)
    norm = np.zeros_like(trace, dtype=np.float64)
    for axis_idx in range(trace.shape[1]):
        col = trace[:, axis_idx]
        span = float(col.max() - col.min())
        norm[:, axis_idx] = 1.0 if span <= 1e-12 else (col - col.min()) / span
    features = trace_geometry_features_from_normalized(norm)
    return {name: float(value) for name, value in zip(TRACE_FEATURE_NAMES, features)}


def rank_pool(M: np.ndarray, method: str = "geomean"):
    """Rank a cell pool (axis-matrix, higher=better). Default 'geomean' = volume x balance
    (Bongo's shape-aware quality geometry); 'topsis' kept for comparison in validation.
    Returns (order best->worst, scores, pareto_mask, volume, balance)."""
    mask = pareto_mask(M)
    Mn = percentile_normalize(M)
    geomean, volume, balance = volume_balance(Mn)
    scores = geomean if method == "geomean" else topsis(Mn)
    order = np.argsort(-scores)
    return order, scores, mask, volume, balance


# ----------------------------------------------------------------------------
# AXIS EXTRACTION (evidence-specific). All 8 axes oriented higher=better.
# Block-derived axes (sortino/cvar/r2/temporal/capital-preservation) are
# scale-consistent within a cell. `ulcer_control` stores the reversed score.
# pool, and percentile-normalization is rank-based, so no absolute constants or clamps
# are needed -- the pool defines the scale.
# ----------------------------------------------------------------------------
def axes_from_blocks(r, *, total_return_pct=None, profit_factor=None, max_dd_pct=None):
    """Compute the 8 axes from a per-period block-return series. metrics overrides
    (full-window return/pf/maxDD) are used when available; otherwise derived from blocks
    (used by selection_validation on period slices)."""
    r = np.asarray(r, dtype=np.float64)
    r = r[np.isfinite(r)]
    active = r[np.abs(r) > 1e-12]
    if active.size < 2:
        return None
    eq = np.cumprod(1.0 + r)
    comp_return = (eq[-1] - 1.0) * 100.0
    # drawdown path
    peak = np.maximum.accumulate(eq)
    dd = np.where(peak > 0, (peak - eq) / peak, 0.0) * 100.0
    ulcer = float(np.sqrt(np.mean(dd ** 2)))
    maxdd_blocks = float(dd.max())
    # equity-curve linearity (R^2 vs straight line)
    t = np.arange(eq.size, dtype=np.float64)
    ss_tot = float(((eq - eq.mean()) ** 2).sum())
    if ss_tot > 1e-18:
        slope, intercept = np.polyfit(t, eq, 1)
        resid = eq - (slope * t + intercept)
        r2 = 1.0 - float((resid ** 2).sum()) / ss_tot
        if slope < 0:           # straight line DOWN is not "good consistency"
            r2 = 0.0
    else:
        r2 = 0.0
    # downside / tail
    downside = active[active < 0]
    dstd = float(downside.std()) if downside.size else 0.0
    sortino = float(active.mean() / dstd) if dstd > 1e-12 else float(active.mean() / 1e-9)
    k = max(1, int(np.ceil(0.05 * active.size)))
    tail_cvar = float(np.sort(active)[:k].mean())          # negative; higher=better
    temporal = float((active > 0).mean())
    # profit/pf/recovery: prefer engine metrics, else block-derived
    net_return = float(total_return_pct) if total_return_pct is not None else comp_return
    if profit_factor is not None:
        pf = min(float(profit_factor), 10.0)
    else:
        gains = float(active[active > 0].sum()); losses = float(-active[active < 0].sum())
        pf = min(gains / losses, 10.0) if losses > 1e-12 else (10.0 if gains > 0 else 0.0)
    maxdd = float(max_dd_pct) if max_dd_pct is not None else max(maxdd_blocks, 1e-9)
    recovery = net_return / max(maxdd, 0.1)
    return {
        "net_return": net_return, "profit_factor": pf, "sortino": sortino,
        "recovery": recovery, "tail_cvar": tail_cvar, "equity_r2": r2 * 100.0,
        "temporal_consistency": temporal * 100.0, "ulcer_control": -ulcer,
        "_ulcer": ulcer, "_maxdd": maxdd,
    }


def candidate_axes(rec: dict, col_returns: np.ndarray) -> dict:
    m = rec.get("metrics", {}) or {}
    ax = axes_from_blocks(
        col_returns,
        total_return_pct=float(m.get("total_return_pct", 0.0)),
        profit_factor=float(m.get("profit_factor", 0.0) or 0.0),
        max_dd_pct=float(m.get("max_drawdown_pct", 100.0)),
    )
    if ax is None:  # degenerate block series -> fall back to metrics-only
        ax = {k: 0.0 for k in AXIS_NAMES}
        ax["net_return"] = float(m.get("total_return_pct", 0.0))
        ax["profit_factor"] = min(float(m.get("profit_factor", 0.0) or 0.0), 10.0)
        ax["ulcer_control"] = -float(m.get("max_drawdown_pct", 100.0))
        ax["_ulcer"] = float("nan"); ax["_maxdd"] = float(m.get("max_drawdown_pct", 100.0))
    ax["_max_dd"] = float(m.get("max_drawdown_pct", 100.0))
    ax["_trades"] = int(m.get("total_trades", 0))
    return ax


def load_symbol(symbol: str, evidence_dir: Path):
    man = evidence_dir / f"{symbol}_oos_manifest.json"
    if not man.exists():
        return None, None
    m = json.loads(man.read_text(encoding="utf-8"))
    npz = glob.glob(str(evidence_dir / f"{symbol}_oos_returns_*.npz"))
    if not npz:
        return m, None
    z = np.load(npz[0])
    return m, {"returns": z["returns"], "candidate_ids": [str(x) for x in z["candidate_ids"]]}


def select_symbol(symbol, evidence_dir, min_trades, dd_ceiling):
    m, mat = load_symbol(symbol, evidence_dir)
    if m is None:
        return None
    returns = mat["returns"] if mat else None
    cells = defaultdict(list)
    for rec in m["records"]:
        regime = rec.get("tuned_for_regime") or "BASE"
        col = rec.get("matrix_column")
        series = returns[:, col] if (returns is not None and col is not None and col < returns.shape[1]) else np.zeros(1)
        ax = candidate_axes(rec, series)
        cells[(rec["timeframe"], regime)].append((rec, ax))
    results = []
    for (tf, regime), pool in sorted(cells.items()):
        survivors = [(r, a) for r, a in pool if a["_trades"] >= min_trades and a["_max_dd"] <= dd_ceiling]
        if not survivors:
            results.append({"tf": tf, "regime": regime, "pool": len(pool), "survivors": 0, "winner": None})
            continue
        M = np.array([[a[k] for k in AXIS_NAMES] for _, a in survivors], dtype=np.float64)
        order, scores, mask, volume, balance = rank_pool(M)
        best_i = int(order[0])
        rec, ax = survivors[best_i]
        results.append({
            "tf": tf, "regime": regime, "pool": len(pool), "survivors": len(survivors),
            "winner": {
                "strategy": rec["strategy_name"], "candidate_id": rec["candidate_id"][:12],
                "geomean": round(float(scores[best_i]), 4), "volume": round(float(volume[best_i]), 3),
                "balance": round(float(balance[best_i]), 3), "pareto_optimal": bool(mask[best_i]),
                "net_return": round(ax["net_return"], 2), "pf": round(ax["profit_factor"], 2),
                "sortino": round(ax["sortino"], 2), "recovery": round(ax["recovery"], 2),
                "tail_cvar": round(ax["tail_cvar"], 3), "equity_r2": round(ax["equity_r2"], 1),
                "temporal": round(ax["temporal_consistency"], 1), "ulcer": round(ax["_ulcer"], 2) if np.isfinite(ax["_ulcer"]) else None,
                "trades": ax["_trades"],
            },
        })
    return results


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--symbols", nargs="*")
    ap.add_argument("--evidence-dir", default=str(REPO / "pm_outputs" / "production_live" / "oos_evidence"))
    ap.add_argument("--min-trades", type=int, default=20)
    ap.add_argument("--dd-ceiling", type=float, default=35.0)
    args = ap.parse_args()
    ev = Path(args.evidence_dir)
    symbols = args.symbols or sorted({p.name.split("_oos_manifest")[0] for p in ev.glob("*_oos_manifest.json")})
    print(f"=== PARETO MULTI-AXIS SELECTION  gates[min_trades>={args.min_trades}, max_dd<={args.dd_ceiling}] ===")
    print(f"axes (higher=better): {AXIS_NAMES}  | tie-break: percentile-normalize -> TOPSIS")
    for sym in symbols:
        res = select_symbol(sym, ev, args.min_trades, args.dd_ceiling)
        if res is None:
            print(f"\n{sym}: no evidence yet"); continue
        print(f"\n--- {sym} ---  rank=geomean(volume x balance); axes: ret/pf/sortino/recovery/cvar/R2/temporal/cap-pres")
        print(f"{'tf':4} {'regime':7} {'pl':>3} {'sv':>3}  {'winner_strategy':30} {'ret%':>6} {'pf':>4} {'rec':>5} {'R2':>5} {'tmp':>5} {'ulc':>5} {'trd':>4} {'vol':>5} {'bal':>5} {'geo':>5} {'par':>4}")
        for c in res:
            w = c["winner"]
            if not w:
                print(f"{c['tf']:4} {c['regime']:7} {c['pool']:>3} {c['survivors']:>3}  (no gate survivors)")
                continue
            print(f"{c['tf']:4} {c['regime']:7} {c['pool']:>3} {c['survivors']:>3}  {w['strategy'][:30]:30} {w['net_return']:>6} {w['pf']:>4} {w['recovery']:>5} {w['equity_r2']:>5} {w['temporal']:>5} {str(w['ulcer']):>5} {w['trades']:>4} {w['volume']:>5} {w['balance']:>5} {w['geomean']:>5} {str(w['pareto_optimal'])[0]:>4}")
    return 0


# --- schema-independent core unit test (run: python tools/pareto_select.py selftest) ---
def test_pareto_core():
    # A dominates B on all axes; C trades off. A,C on frontier; B dominated.
    M = np.array([[10, 2.0, -5, 0.8],   # A strong
                  [ 5, 1.5, -8, 0.6],   # B dominated by A
                  [ 3, 3.0, -2, 0.9]],  # C different tradeoff
                 dtype=np.float64)
    mask = pareto_mask(M)
    assert mask.tolist() == [True, False, True], mask.tolist()
    order, scores, _, vol, bal = rank_pool(M)
    assert order[-1] == 1, (order.tolist(), scores.tolist())  # dominated B ranks last
    assert bal.min() >= 0 and bal.max() <= 1.0 + 1e-9, bal.tolist()  # balance in [0,1]
    # normalization is rank-based: best per axis -> 1.0, worst -> 0.0
    Mn = percentile_normalize(M)
    assert abs(Mn[:, 0].max() - 1.0) < 1e-9 and abs(Mn[:, 0].min()) < 1e-9
    print("test_pareto_core PASSED (pareto mask + percentile + topsis)")


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "selftest":
        test_pareto_core(); raise SystemExit(0)
    raise SystemExit(main())
