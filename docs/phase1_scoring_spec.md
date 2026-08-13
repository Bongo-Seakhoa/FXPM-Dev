# Scoring Design Spec â€” ThreadSpark Evidence-Tensor Selector

Status: HISTORICAL research design. The ThreadSpark/spoke variants failed their
like-for-like gates and were removed from the executable system on 2026-08-13.
This file preserves the research reasoning; current production uses one fixed
`legacy` scoring contract recorded in artifact provenance.

Production split note: current production uses `production_split_mode="bleeding_edge"` with
adjacent WFV and no clean holdout. Clean-holdout ThreadSpark surfaces in this document are
research-only unless an explicit research profile is configured. The canonical production
geometry is `docs/PRODUCTION_SPLIT_AND_PROCESS.md`.

---

## 1. Objective (what we select for)

Per `symbol Ã— timeframe Ã— regime`, select the strategy that will be **profitable on live
markets** â€” operationalized as: the candidate whose **8-spoke shape is broadest and most
dent-free across evidence steps, measured on surfaces the optimizer did not fit and under
full-history honest cost.**

We do **not** select on in-sample net profit, profit-factor Ã— trades, or trade volume. Breadth
(high on all spokes) + persistence (holds across evidence steps) is the live-survival predictor.
"Short and broad" (few trades, robust shape) beats "long and narrow" (many trades, big in-window
net profit that dies on honest cost).

The selection object is a 3-D tensor: **`candidate Ã— evidence_step Ã— spoke`** (ThreadSpark).
Evidence steps are surfaces in increasing honesty: `train â†’ validation â†’ clean_holdout â†’
historical_cost`. Each step is an 8-spoke face; the score is geomean breadth per face plus
dent topology across steps.

---

## 2. Why (diagnosis that motivated this design)

The prior spoke models (`specialist_spokes`, `_v2`, `_asym`) regressed because **every spoke and
every guard was measured on an in-sample surface the optimizer had already fit**, and no axis
priced full-history honest cost:

- `val` (stage2) was the newest half of `train` (active) â€” fully nested, so a high-frequency
  churner that overfit the recent window scored "broad" on val. Proven by slice provenance.
- The selection spokes read `val_metrics` â€” the exact surface TPE maximized.
- The catastrophe only appeared on full-history `conservative_max`, which no axis evaluated.
  Forensics: GBPUSD val +53.98% / PF 1.78 â†’ honest replay âˆ’61.65% (527 trades);
  EURUSD val PF âˆž / 100% win â†’ honest replay âˆ’11.94%.

`v2`'s veto/fallback machinery was a band-aid for the dishonest surface and a multiplicative
`1 âˆ’ kÂ·penalty` cannot fix a Goodhart objective (the penalty self-cancels for any survivor).
Fix: make the measurement honest; let the geomean's weakest-link property do the vetoing.

Like-for-like (honest cost, 4 symbols) that drove the rework:

| model | mean% | median% | profitable | worst% |
|---|---|---|---|---|
| legacy | +13.01 | +0.04 | 2/4 | âˆ’12.73 |
| old `specialist_spokes` | +5.06 | +2.07 | 2/4 | âˆ’7.40 |
| `specialist_spokes_asym` (rejected) | âˆ’23.34 | âˆ’17.41 | 0/4 | âˆ’56.55 |
| `specialist_spokes_v2` (rejected) | âˆ’23.38 | âˆ’12.54 | 0/4 | âˆ’61.65 |

---

## 3. Leakage boundary (invariant)

- Inner Optuna/TPE objective uses **train + validation + candidate evidence only**.
  Holdout / historical / forward are **downstream-only**, never in the trial.
- TPE-stationary: no pool rank, no cross-trial ranking, no pool statistics inside the trial.
  Pool-level stats (deflated-Sharpe, etc.) appear only at final selection.
- ThreadSpark post-search faces are attached to candidate dicts **after** the Optuna pool is
  fixed; they never mutate train/val metrics, so the inner objective remains stationary.
- Numba contract unchanged (one-bar lag, SL-before-TP); honest-surface measurement reuses the
  same `backtester.run` semantics, no parallel measurement loop.

---

## 4. Architecture (three layers + the tensor)

New scoring model: **`specialist_spokes_threadspark`** (`pm_core.py:610`). Production default
remains `legacy`; the model is opt-in per config.

### L1 - production adjacent walk-forward evidence surface (`pm_core.py`)
- Production now uses `production_split_mode="bleeding_edge"`: full active-universe eligibility, newest active half for optimization through the market edge, and the immediately prior window for adjacent walk-forward/resilience validation.
- The clean-holdout surface below is retained only for explicit research profiles, not as the production split contract.
- `DataSplitter` emits `walk_forward_validation` as the fixed adjacent validation slice before `optimization_train`; `oos_embargo_bars` creates the leakage buffer between the two windows.
- When fixed clean holdout is enabled, `stage2_training_selection` also ends
  `oos_embargo_bars` before `clean_holdout` begins. The pipeline consumes this emitted
  `stage2_training_selection` end directly, so the split metadata and actual feature slicing
  agree at both evidence boundaries.
- When the flag is false the legacy nested split is preserved exactly.

### L2 â€” post-search selection surface (`pm_pipeline.py:2540-2647`)
- `_attach_threadspark_selection_evidence_to_candidates` runs on the **candidate pool after
  Optuna collection and before `_select_best_for_regime`** (not `_attach_holdout_metrics`,
  which runs too late on already-selected winners). It backtests each candidate on the
  clean-holdout surface, buckets by regime, and writes evidence onto candidate dicts only.
- Missing/thin clean-holdout is **flagged** (`threadspark_selection_surface="clean_holdout_missing"`,
  `threadspark_selection_missing=True`) and **penalized**, never silently relabeled as val.

### L3 â€” full-history honest-cost evidence (`pm_pipeline.py:2628-2647`)
- A candidate-pool historical-cost face is added by replaying each candidate over
  `historical_features` (full history, `conservative_max`) â€” post-search selector evidence,
  not an inner-loop target. This is where a churner's cost/tail spokes collapse.

### The tensor selector (`pm_pipeline.py:4748-4877`)
- `_compute_threadspark_spoke_evidence` builds the face stack
  `[train, validation, (clean_holdout | zero-face-if-missing), (historical_cost)]`, preserving
  the 8-spoke roster.
- Per face: `face_quality = geomean(8 spokes)` (`_geo_mean`, clip `[1e-9,1]`; one weak spoke
  collapses the face â€” the intrinsic veto).
- Across faces: `dent_health = min(dent(face[i-1], face[i]))` â€” the worst step-to-step
  collapse. A candidate must hold up at **every** evidence step, not just one.
- `final_quality = geomean(final_face)` (the clean-holdout face when present), `path_quality =
  mean(face_qualities)`, plus `balance` and confidence weighting for thin faces.
- A missing clean-holdout face is a **zero face** â†’ `final_qualityâ‰ˆ0`, `dent_healthâ‰ˆ0`: the
  candidate is de-selected, not rewarded.

### L1-shortlist guard (`pm_pipeline.py:3791-3842`)
- `_threadspark_l1_base_score` (the spoke score on the train/validation evidence surface) selects the
  **top-K shortlist** (`regime_validation_top_k`); the full tensor reranks **only within** that
  shortlist. Holdout/historical evidence therefore cannot resurrect a rank-K+1 churner that
  train/val already rejected â€” it can only reorder genuine L1 survivors.

### Inner stationarity (`pm_pipeline.py:4789-4792`)
- Inside an Optuna trial the post-search faces do not exist yet, so the tensor degenerates to
  the two-face (`train â†’ validation`) spoke approximation. The inner objective stays stationary.

---

## 5. The 8 spokes

**2026-06-05 correction:** `specialist_spokes` keeps the original spoke projection for
reproducible comparison. `specialist_spokes_threadspark` now uses the ThreadSpark set:
`edge_survival, profit_structure_quality, capital_preservation, tail_risk_control,
path_persistence, generalization_survival, execution_cost_survival, parameter_stability`.
`path_persistence` replaces the volume-gameable `temporal_consistency` only for ThreadSpark;
the old-spoke comparator is intentionally unchanged.

`SPECIALIST_SPOKES`: `edge_survival, profit_structure_quality, capital_preservation,
tail_risk_control, temporal_consistency, generalization_survival, execution_cost_survival,
parameter_stability`. Eight stays eight; capital/tail stay primary. Geomean aggregation is the
breadth measure and is retained â€” it was never the problem.

Tail axis now reads `r_block_downside_deviation` / `r_block_cvar` before falling back to
`worst_5pct_r`.

**Known spoke weaknesses (refine after the surface fix proves out, not before):**
- `temporal_consistency = 1/(1+max_consec_losses/âˆštrades)` is **gameable by trade volume** â€” a
  churner scores ~0.62 even on a âˆ’61% live surface. Replace with a volume-neutral
  `path_persistence` (positive-block reappearance + expectancy reappearance + variance drag).
- `execution_cost_survival` and `tail_risk_control` saturate in `(0,1]` and currently cannot
  reach the geomean floor; measuring them on the historical-cost face (L3) is what lets the
  weakest-link veto bite. Remove any "absent â†’ 1.0 neutral" free pass.
- `parameter_stability` is near-constant 1.0 in practice (no Optuna neighbourhood evidence
  attached) and dilutes the geomean â€” wire real evidence or demote it to a gate.

---

## 6. Config flags

| Flag | Default (universal `config.json`) | Historical honest research profile |
|---|---|---|
| scoring contract | fixed `legacy` artifact provenance | retired experimental arms remain historical only |
| `production_split_mode` | `bleeding_edge` | `research_clean_holdout` |
| `walk_forward_validation_bars` | 50000 | 50000 |
| `oos_embargo_bars` | 300 | 300 |
| `threadspark_selection_surface` | `validation` | `clean_holdout` |
| `threadspark_use_historical_cost_evidence` | `true` | `true` |
| `threadspark_historical_cost_scope` | `workflow` | `workflow` |
| `threadspark_historical_chunk_count` | `5` | `5` |
| `clean_holdout_pct` | 0.0 | 15.0 |

Note: with `production_split_mode="bleeding_edge"` and `clean_holdout_pct=0` there is no production clean-holdout face. ThreadSpark clean-holdout conclusions apply to research profiles only.

---

## 7. Validation method & acceptance bar

Scoring changes alter **winner selection**, so they cannot be validated by re-backtesting fixed
winners. Validation = **re-optimize under the new scoring**, benchmark the resulting winners on
the honest surface (`tools/benchmark.py`, `conservative_max`), gate against a **robust** bar.

Like-for-like arms (same roster, differ only by `regime_scoring_model`): `legacy` vs old
`specialist_spokes` vs `specialist_spokes_threadspark`. Subset chosen to cover the prior
failures: GBPUSD, EURAUD, EURUSD, USDJPY (v2 blowups), AUDJPY (old-spoke outlier), XAUUSD
(churner).

**2026-06-05 acceptance framing correction:** this is not a per-cell tuning exercise.
Granular drilldown is diagnostic only. The general scorer should avoid promoting high-frequency
churn when short-window evidence looks broad; if no robust candidate exists, low confidence /
no-trade is preferable to forcing a replacement.

**Acceptance bar (robust, no raw mean):**
- median + worst-case must hold (the benchmark verdict requires medians to confirm the mean's
  sign or returns `NEEDS_REVIEW`);
- **no ~5Ã— churn** vs the reference pick;
- **no âˆ’40% per-symbol blowup**;
- **regression fixture:** ThreadSpark must NOT select the GBPUSD 527-trade churner
  (`PiercingDarkCloud M30`); its holdout/historical faces should collapse â†’ `dent_healthâ‰ˆ0`.

If ThreadSpark does not beat `legacy`/old-spoke on the honest surface against this bar, that is a
legitimate verdict â€” `legacy` stays.

---

## 8. Open items / residuals

- **ThreadSpark v1 run (2026-06-05):** failed robust benchmark readiness despite large upside
  cases. Root diagnosis: evidence lifespan was too short and the L1 surface could still favor
  high-frequency variants. Active bounded iteration adds workflow-scope historical chunks,
  volume-neutral `path_persistence`, and removes ThreadSpark trade-count tie-break preference.
- **Second-boundary embargo:** fixed before the clean ThreadSpark rerun. `stage2_training_selection`
  now ends `oos_embargo_bars` before fixed clean holdout begins, and `run_for_symbol()` consumes
  that emitted end rather than recomputing it from `clean_holdout_start`.
- **Embargo sufficiency:** current registry/grid audit found the largest numeric rolling
  feature/strategy lookback at 200 bars; `oos_embargo_bars=300` is above that known rolling-window
  horizon. Calendar prior-period features use completed previous periods rather than a same-period
  rolling window.
- **Spoke refinements** (Section 5) are a separate, later sub-phase â€” only after the honest
  surface proves the geomean-breadth selector beats legacy.

---

## 9. Implementation status

- L1 / L2 / L3 + tensor + L1-shortlist guard: **implemented** (`specialist_spokes_threadspark`),
  audited leak-clean, suite green (1110 passed). Production default unchanged.
- Bounded ThreadSpark iteration after the 2026-06-05 v1 verdict: **implemented locally pending
  rerun**. Changes: ThreadSpark primary spoke `path_persistence`, chunked workflow historical
  evidence faces per candidate/regime cell, and no high-trade-count tie-break in ThreadSpark
  L1/L2 sorting.
- Like-for-like: first partial run was stopped after a correctness audit found Optuna objective
  backtests were passing an empty timeframe into `Backtester.run()`. `pm_optuna.py` now passes
  the real timeframe on every objective backtest path and a regression test covers it. Clean
  like-for-like rerun starts from scratch after discarding the invalid partial outputs.
