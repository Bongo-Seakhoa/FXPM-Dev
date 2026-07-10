"""Specialist-survival spokes -- ONE clean, oriented metric per dimension (Bongo's locked design).

Per the locked decision: 8 TRANSPARENT single-metric spokes (no opaque clusters), each a clear
higher-is-better curve over the evidence axis, and the representative metric for each dimension is
chosen by FORWARD-PREDICTIVE POWER, not asserted. This module defines the spokes as single metrics
and forward-validates them on completed OOS evidence (which single metric actually predicts the
unseen second half).

Claude builds this reference library (validation/measurement lane). Codex integrates the validated
spokes into the canonical selector and owns the engine-gated spokes. Read-only on engine/Codex files.

Spokes (all higher=better, computed from a candidate's per-block OOS return series unless noted):
  edge_survival           cost-adjusted net OOS return (the blocks are honest-cost) -- the money
  profit_structure_quality profit factor (gross win / gross loss), clamped
  capital_preservation    -Ulcer (path-dependent drawdown control; reverse of drawdown)
  tail_risk_control       CVaR = mean of worst-5% blocks (negative; less-negative = better)
  temporal_consistency    fraction of active blocks that are positive
  generalization_survival OOS/train retention, FIXED: train<=0 -> 0 (no reward for train-losers),
                          OOS<=0 -> 0 (collapse), else min(1, OOS/train) in [0,1]   [needs train metric]
  cost_execution_survival STUB -- needs realistic p90 stress re-sim (engine, Codex)
  parameter_stability     STUB -- needs Optuna neighborhood/plateau evidence (engine, Codex)

Run:  python tools/specialist_spokes.py validate     # forward-validate spokes on completed W4 evidence
      python tools/specialist_spokes.py selftest      # orientation/logic unit test
"""
import sys
from pathlib import Path
import numpy as np

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO / "tools"))
from pbo import load_oos_manifest_matrix

_EPS = 1e-12

# --- block-derivable single-metric spokes (operate on any block slice -> forward-validatable) ---
def edge_survival(b):
    b = np.asarray(b, float)
    if not np.any(np.abs(b) > _EPS): return 0.0
    return float((np.cumprod(1.0 + b)[-1] - 1.0) * 100.0)

def profit_structure_quality(b):
    a = np.asarray(b, float); a = a[np.abs(a) > _EPS]
    if a.size == 0: return 0.0
    g = float(a[a > 0].sum()); l = float(-a[a < 0].sum())
    return float(min(g / l, 10.0)) if l > _EPS else (10.0 if g > 0 else 0.0)

def capital_preservation(b):
    b = np.asarray(b, float)
    if not np.any(np.abs(b) > _EPS): return 0.0
    eq = np.cumprod(1.0 + b); peak = np.maximum.accumulate(eq)
    dd = np.where(peak > 0, (peak - eq) / peak, 0.0) * 100.0
    return float(-np.sqrt(np.mean(dd ** 2)))                 # -Ulcer; higher (closer to 0) = better

def tail_risk_control(b):
    a = np.asarray(b, float); a = a[np.abs(a) > _EPS]
    if a.size == 0: return 0.0
    k = max(1, int(np.ceil(0.05 * a.size)))
    return float(np.sort(a)[:k].mean())                      # CVaR (negative); less-negative = better

def temporal_consistency(b):
    a = np.asarray(b, float); a = a[np.abs(a) > _EPS]
    return float((a > 0).mean()) if a.size else 0.0

def generalization_survival(oos_return_pct, train_return_pct):
    """FIXED vs my AUDIT-2 flaw: a train-loser is NOT rewarded, OOS collapse -> 0, bounded [0,1]."""
    tr = float(train_return_pct); oos = float(oos_return_pct)
    if tr <= 0.0:   return 0.0          # don't reward survival when train itself didn't make money
    if oos <= 0.0:  return 0.0          # OOS lost -> survival collapses
    return float(min(1.0, oos / tr))    # retention, bounded (no unbounded-negative)

# block-derivable spokes usable for forward-validation (operate on a block slice)
BLOCK_SPOKES = {
    "edge_survival": edge_survival,
    "profit_structure_quality": profit_structure_quality,
    "capital_preservation": capital_preservation,
    "tail_risk_control": tail_risk_control,
    "temporal_consistency": temporal_consistency,
}
# engine-gated spokes (Codex owns the data); stubbed so the contract is explicit
ENGINE_GATED_SPOKES = {
    "cost_execution_survival": "needs realistic p90 stress re-sim (engine spread mode)",
    "parameter_stability": "needs Optuna neighborhood/plateau evidence",
}


def _forward_return(b):
    b = np.asarray(b, float)
    return 0.0 if not np.any(np.abs(b) > _EPS) else float((np.cumprod(1.0 + b)[-1] - 1.0) * 100.0)


def validate(evidence_dir, min_active=12):
    """For each block-derivable spoke: correlation between the spoke (computed on the SELECTION half)
    and realized FORWARD-half return, across all completed-run candidates. Higher |corr| with the
    right sign = the spoke carries genuine forward signal -> it earns its place as the dimension's
    single metric. This is the anti-arbitrary, data-chosen selection of spokes."""
    ev = Path(evidence_dir)
    sel = {k: [] for k in BLOCK_SPOKES}
    fwd = []
    n_cand = 0
    for man in ev.glob("*_oos_manifest.json"):
        try:
            R, _manifest, _candidate_ids = load_oos_manifest_matrix(man)
        except Exception as exc:
            print(f"skip {man.name}: {type(exc).__name__}: {exc}")
            continue
        half = R.shape[0] // 2
        for c in range(R.shape[1]):
            col = R[:, c]
            if int((np.abs(col) > _EPS).sum()) < min_active:   # evidence-sufficiency floor
                continue
            s = col[:half]
            if int((np.abs(s) > _EPS).sum()) < 3:              # need activity in the selection half
                continue
            for k, fn in BLOCK_SPOKES.items():
                sel[k].append(fn(s))
            fwd.append(_forward_return(col[half:]))
            n_cand += 1
    fwd = np.asarray(fwd, float)
    print(f"=== SPECIALIST-SPOKE FORWARD VALIDATION (completed evidence) ===")
    print(f"candidates: {n_cand}  | dir: {ev}")
    if n_cand < 20:
        print("too few candidates for a trustworthy read."); return
    print(f"\n{'spoke':26} {'corr(sel,fwd)':>14}  (higher +corr = genuine forward signal)")
    scored = {}
    for k in BLOCK_SPOKES:
        v = np.asarray(sel[k], float)
        c = float(np.corrcoef(v, fwd)[0, 1]) if v.std() > 0 else 0.0
        scored[k] = c
        print(f"{k:26} {c:>+14.3f}")
    best = max(scored, key=scored.get)
    print(f"\nstrongest single forward-predictive spoke: {best} ({scored[best]:+.3f})")
    print("engine-gated spokes (Codex): " + ", ".join(f"{k} [{why}]" for k, why in ENGINE_GATED_SPOKES.items()))
    print("NOTE: directional -- correlations are noisy; this ranks which single metrics earn their place,")
    print("      and must be confirmed against the live promotion outcome, not adopted blindly.")


def test_specialist_spokes():
    n = 64
    up = np.abs(np.random.RandomState(1).randn(n)) * 0.003 + 0.003     # steady gains
    dn = -np.abs(np.random.RandomState(2).randn(n)) * 0.003 - 0.003    # steady losses
    assert edge_survival(up) > 0 > edge_survival(dn)
    assert profit_structure_quality(up) > profit_structure_quality(dn)
    assert capital_preservation(up) > capital_preservation(dn)          # less drawdown = higher
    assert temporal_consistency(up) > temporal_consistency(dn)
    # generalization_survival FIXED behaviour
    assert generalization_survival(oos_return_pct=10, train_return_pct=12) > 0.0   # held up
    assert generalization_survival(oos_return_pct=10, train_return_pct=-5) == 0.0  # train-loser NOT rewarded
    assert generalization_survival(oos_return_pct=-3, train_return_pct=12) == 0.0  # OOS collapse -> 0
    assert 0.0 <= generalization_survival(oos_return_pct=6, train_return_pct=12) <= 1.0  # bounded
    print("test_specialist_spokes PASSED (orientation + fixed generalization_survival)")


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "selftest":
        test_specialist_spokes(); raise SystemExit(0)
    if len(sys.argv) > 1 and sys.argv[1] == "validate":
        validate(REPO / "pm_outputs" / "production_live" / "oos_evidence"); raise SystemExit(0)
    print("usage: python tools/specialist_spokes.py [selftest|validate]")
