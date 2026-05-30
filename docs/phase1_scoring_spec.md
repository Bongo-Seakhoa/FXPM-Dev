# Phase 1 — Scoring Paradigm Rework (spec)

Status: DRAFT, pre-implementation. Gated by `tools/benchmark.py` against **baseline_v1**
(point/digits-correct conservative spread). No engine edits until Codex releases pm_core.

## Verified current state (pm_core / pm_pipeline)
- Selection score `calculate_fx_selection_score` (pm_core.py:5121). Composite of risk-adj,
  PF, win-rate, return/DD, expectancy, then multiplicative penalties (continuous DD, tail,
  consecutive-loss) and a **raw trade-frequency BONUS** (pm_core.py:5189-5195): up to +8%
  for count>30. Default ON; flag absent from config.
- Optuna objective `calculate_fx_opt_score` (pm_core.py:5199): clean of trade-count terms.
- Regime bucket score `_compute_regime_score` (pm_pipeline.py:2747-2780): multiplies the
  generalization score by a raw-count **stability factor clamped to [0.7,1.0]** (0.3 train/
  0.7 val). ~30% swing on count — the heaviest raw-count lever.
- Candidate descent tiebreak on train trade count (pm_pipeline.py:2443-2449).
- Hard floors (KEEP as floors): min_trades=25, fx_*_min_trades=15, regime 25/15.

## Objective (Tenets 1,2,5)
Replace **raw-count reward** with **statistical-confidence** so the optimiser prefers
return + reliability/consistency within a healthy count — not busy/churning strategies.
Keep count floors. Net profit & PF stay primary; path-dependent DD (ulcer, % time
underwater) is a first-class quality axis.

## Change sequence (smallest, each independently benchmark-gated)
Each step: implement behind a flag → re-OPTIMIZE small pool (Codex, >20min) → benchmark the
NEW winners → `--compare baseline_v1 candidate` → accept only IMPROVEMENT/justified
TRADE_OFF_ACCEPT with no per-symbol blowup; else revert.

1. **Regime stability band [0.7,1.0] → [0.9,1.0]** (pm_pipeline.py:2765). Smallest, highest-
   impact realignment. Hypothesis: winners that cleared the trade floor stop losing ~30% of
   score for merely-floor counts; selection shifts toward higher return/consistency.
2. **Replace freq bonus with a confidence term** (pm_core.py:5189-5195). Instead of
   `0.03*log(tc/30)`, reward low standard error of mean-R:
   `confidence = 1 - clamp(SE(R)/|mean_R|, 0, 1)` style, bounded to a small multiplier band.
   Rewards count AND consistency jointly; a high-count but noisy strategy gets no edge.
3. **Drop/!replace the raw-count tiebreak** (pm_pipeline.py:2443-2449) with a
   consistency tiebreak (pct_positive_r or inverse ulcer).
4. **Add path-dependent DD into selection** as a penalty axis (ulcer / % time underwater),
   since baseline shows current winners sit underwater ~79% of the time and scoring ignores
   it. Calibrate so it never overrides net profit/PF (Tenet-2 priority order).

## Later (separate sub-phase, bigger): plateau robustness + utility
- Plateau: score the neighbourhood of top Optuna params (local perturbation + walk-forward
  survival), prefer broad profitable regions over isolated spikes. Likely the largest
  accuracy lever; needs the re-opt harness.
- Utility master objective: sample-aware fractional-Kelly/CRRA with tail+DD penalty and
  shrinkage `k` fixed a priori (never grid-searched). Radar/geometric axes = winsorised
  diagnostic + quality floor, not the selector.

## Validation method (critical)
Scoring changes alter WINNER SELECTION, not just execution. So they cannot be validated by
re-backtesting fixed winners. Validation = **re-optimise the small pool under the new
scoring**, benchmark the resulting winners, gate vs baseline_v1. Re-opt is the long run →
Codex executes/monitors (small pool only, per the discovery-pool rule).

## Risks
- Confidence term needs SE(R); ensure backtest exposes per-trade R series (it returns
  trades; mean_r/worst_5pct_r already computed — extend with SE).
- Re-opt nondeterminism: fix seeds; compare like-for-like.
- Over-rotating to consistency could suppress profitable-but-lumpy winners — the gate's
  net-profit/PF priority guards against this.
