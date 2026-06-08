# FxPM Data-Driven Discovery Execution Roadmap

Status: active working roadmap, created 2026-06-02.

Lifecycle: this is not evergreen documentation. It is the shared planning basis for Codex,
Claude, and any audit agents during the current discovery/rebuild phase. When a section is
implemented, rejected, or superseded, move durable conclusions into code/tests/config or
`CHANGELOG.md`, then retire this file or replace it with the next active roadmap.

## 1. North Star

Build the most production-ready version of FxPM by improving the evidence path that selects
specialist strategies. No scoring, validation, execution, or architecture change should become a
default because it is elegant in theory. It becomes a default only after an honest-cost,
same-roster, data-driven comparison proves that it improves the outcome or provides a justified
trade-off.

The primary optimization unit is:

```text
symbol x timeframe x regime
```

Each winner is a specialist inside one market cell. The portfolio layer is not the primary
selector. Portfolio logic should remain a survival governor and diagnostic layer unless a future
experiment proves that portfolio-level intervention improves live profitability without suppressing
specialist edge.

## 2. Current Evidence Baseline

### W4 honest discovery run

The current trustworthy completed benchmark is W4:

- Universe: 62 configured symbols, 100-strategy roster, all OOS manifests complete.
- Granularity: 379 tradeable `symbol/timeframe/regime` specialist rows.
- Promotion result after stress, holdout, PBO, historical, and provenance gates:
  - `2 PROMOTE`
  - `75 REVIEW`
  - `302 REJECT`
- Solid promoted specialist:
  - `EURAUD H4 CHOP / TimeOfDayMomentumStrategy`
- Marginal promoted specialist:
  - `USDJPY H4 RANGE / EMAPullbackContinuationStrategy`

This is a baseline, not a target. Any new selector or validation path must beat this baseline on
the actual promotion-readiness suite, not only on in-sample score.

### Stress replay conclusion

Stress replay under `stress_max`, `stress_p90`, and `stress_p95` showed that relaxing stress is not
the current promotion lever.

- `stress_max`: `2 PROMOTE / 75 REVIEW / 302 REJECT`
- `stress_p90`: `2 PROMOTE / 77 REVIEW / 300 REJECT`
- `stress_p95`: `2 PROMOTE / 77 REVIEW / 300 REJECT`

The important nuance: `stress_p90`/`stress_p95` only alter the entry-bar fill percentile while
intrabar/exit protection remains `SpreadMax`. This is intentional defensive stress behavior, not a
fully realistic p90-cost replay.

Decision: keep `stress_max` as the promotion torture gate. Stress instrumentation remains useful,
but the next improvement effort should not focus on relaxing stress defaults.

### Holdout bottleneck conclusion

W4 holdout diagnostics:

- `60 PASS`
- `73 FAIL`
- `246 INSUFFICIENT`
- `61` rows had ledger status `PROMOTED` while final promotion review held them at `REVIEW` due to
  insufficient holdout evidence. The status-alignment fix now prevents this in future runs.

Threshold sensitivity at the current 5-trade holdout floor:

- `min_trades=5`: only `3` gate-clean rows are holdout-promotable.
- `min_trades=1`: at most `13` gate-clean rows become holdout-promotable.

Decision: do not lower the 5-trade floor. A PF/return read on 1-4 trades is not reliable enough
for live-capital selection. The correct direction is to improve holdout construction so specialists
get enough evidence, not to lower the standard.

### Holdout construction projection

W4 projection results:

- median clean-holdout/training bar ratio: `0.176472`
- median clean-holdout/stage2 bar ratio: `0.150001`
- median holdout-vs-validation trade-rate ratio: `0.985499`

Interpretation: the holdout tail is not unusually quiet. The thinness comes from bar-count times
genuinely low-frequency specialists after splitting by symbol/timeframe/regime.

Two construction candidates were projected:

1. Trade-count anchored holdout
   - `23` gate-clean-except-holdout rows are feasible to make sufficient within stage2.
   - `10` are currently positive-but-thin.
   - Quality-cautious ceiling: `3 + 10 = 13`.
   - If approximated by a flat tail enlargement, median extra training-data loss is `26.4712%`.

2. Purged/embargoed k-fold OOS
   - Same `23` gate-clean-except-holdout sufficient rows.
   - Same `10` currently positive-but-thin rows.
   - Quality-cautious ceiling: `13`.
   - Permanent training-data loss: `0%`.
   - Approximate discovery runtime multiplier: `5x`.

Decision: prototype purged/embargoed k-fold OOS before changing defaults. It recovers the same
projected evidence sufficiency without permanently depleting the freshest/training data.

### PBO / false-discovery conclusion

PBO is a real wall, but not the first immediate fix.

- Total numeric high-PBO rows in W4: `99`.
- Primary PBO rejects: `13`.
- Many PBO-high rows are already blocked by stress, holdout, or historical failure.

The current timeframe-aligned PBO implementation fixed the earlier cross-timeframe alignment
defect. Missing PBO usually means insufficient candidate count, not a numeric overfit signal.

Decision: do not relax PBO thresholds. After holdout construction is made fairer, re-evaluate the
PBO wall with cleaner OOS evidence. If the wall remains, investigate whether the selection surface
itself is too overfit or whether the PBO artifact still measures a related but imperfect surface.

## 3. Current Architecture Facts

### Strategy roster

The strategy pool is broad enough for the next discovery phase:

- Roster: 100 strategies.
- Coverage includes trend, mean reversion, breakout, volatility compression, candlestick/price
  action, calendar/time/session, carry/swap, range, volume-like, and level-based families.
- ML/meta-model strategies remain intentionally out of scope due to maintenance burden and fit
  concerns.

Decision: freeze roster for controlled scoring/validation experiments unless a clear coverage gap
is proven. Adding more strategies now would confound selection-scoring experiments.

### Cost model

Current honest discovery cost model:

- Baseline: `conservative_max`
- Stress gate: `stress_max`
- Broker/instrument spec priority is in place.
- Spread provenance is surfaced, and non-FX/crypto/index outliers are flagged rather than blindly
  trusted.

Decision: preserve this fidelity. Accuracy corrections that lower historical performance are not
regressions; they are truth-telling.

### Efficiency baseline

Known efficiency fixes already banked:

- Numba JIT restored by pinning compatible NumPy.
- Numba/Python parity proved bit-identical across representative assets/timeframes.
- Optuna budget calibrated to cap around 1500 trials.
- Feature disk cache validated as accuracy-neutral and useful, but RAM remains the production
  constraint on the current 8 GB machine.

Decision: future parallelism work must be memory-aware. Disk cache can accelerate retrieval, but
RAM-heavy optimization still needs bounded per-worker memory or per-symbol/per-timeframe lifecycle
control.

## 4. Scoring Concept: Correct Target

The correct scoring architecture is not:

```text
portfolio-balanced selector first
```

It is:

```text
specialist-first evidence selector
+ minimal account survival controls
```

The scoring question is:

```text
Inside this symbol/timeframe/regime cell, which candidate has the strongest evidence of live
survival and profitability?
```

The scoring question is not:

```text
Does this candidate make the entire portfolio look balanced?
```

Portfolio controls should protect against ruin, margin collapse, broker constraints, and excessive
aggregate risk. They should not suppress a specialist merely because it is specialized.

## 5. 3D Evidence Geometry Design

The intended model is a path-dependent evidence geometry:

```text
quality_tensor[candidate, evidence_step, metric]
```

The evidence axis may be:

- trade index buckets,
- chronological time buckets,
- purged folds,
- OOS slices,
- normalized evidence progress.

Each metric is a curve over evidence, not just a final scalar.

The objective is to answer:

```text
Did this candidate build a healthy, stable quality object as evidence accumulated?
```

The geometry should reject or penalize candidates whose final score looks good but whose evidence
path is structurally weak:

- PF improves only late.
- Final return is rescued by a small lucky cluster.
- capital preservation is poor for most of the path.
- OOS quality collapses against train quality.
- trade quality deteriorates as trade count increases.
- drawdown or tail risk is concentrated in one region.
- execution/cost stress kills the edge.
- the parameter solution is a narrow needle rather than a plateau.

### Primary eight spokes

Use eight specialist-survival dimensions:

1. `edge_survival`
2. `profit_structure_quality`
3. `capital_preservation`
4. `tail_risk_control`
5. `temporal_consistency`
6. `generalization_survival`
7. `execution_cost_survival`
8. `parameter_stability`

These are not portfolio-balance spokes. They describe whether a specialist is structurally
trustworthy inside its own cell.

### Derived shape features

Do not turn every pathology into a raw spoke. Compute these as path diagnostics/gates:

- worst dent depth,
- dent duration,
- dent recovery,
- late-rescue dependence,
- evidence-slice concentration,
- trade-cluster concentration,
- train/OOS path distance,
- path smoothness,
- cross-axis balance,
- evidence confidence.

### Trade count treatment

Trade count is evidence resolution, not the prize.

Allowed:

- hard minimum trade floors,
- saturating confidence term,
- evidence sufficiency checks.

Not allowed:

- raw trade-count reward that makes busy strategies win over better-quality strategies.

## 6. Known Scoring Implementation Gap

The locked 8-spoke specialist geometry is not yet the live optimizer scorer.

Current implemented state includes:

- post-run geometry/selection validation tools,
- visual concept artifacts (`preview.html`, `scoring_model_proposed_vs_real.html`),
- legacy/raw metric scoring still active in the optimizer path,
- holdout/PBO/stress/provenance promotion gates outside the scorer.

The new scoring model must not be bolted in blindly. It needs a staged proof path:

1. Build the evidence tensor artifact.
2. Compute spokes and diagnostics from the tensor.
3. Validate as post-run selector first.
4. Run same-roster control experiments.
5. Only then consider replacing optimizer selection defaults.

## 7. Next Workstreams

### Workstream A: Holdout construction prototype

Goal: convert the current fixed-tail holdout into a fairer discovery-stage OOS evidence process
without lowering evidence standards.

Recommended first implementation:

- Opt-in purged/embargoed k-fold OOS prototype.
- Keep current fixed clean holdout as the default until replay proves improvement.
- Emit fold-level metrics per candidate/cell.
- Aggregate OOS evidence only after purging/embargoing neighboring bars to avoid leakage.
- Preserve the freshest-data promotion refit principle: discovery validates process; promotion can
  later refit selected config on full data before live/test deployment.

Required diagnostics:

- fold count,
- embargo size,
- per-fold trades,
- aggregate OOS trades,
- fold pass rate,
- aggregate PF/return,
- worst fold,
- OOS time-underwater/capital preservation,
- train-vs-OOS path divergence,
- runtime overhead,
- memory overhead.

Acceptance criteria:

- Same W4-style roster and cost model.
- No leakage found by audit.
- At least equal or better promotion-readiness result versus W4 fixed-holdout baseline.
- New promotes must clear stress, PBO, historical, provenance, and aggregate OOS gates.
- If promotes rise from `2` toward projected `13`, inspect every additional promote manually before
  treating it as candidate-live quality.

Rejection criteria:

- leakage risk,
- materially worse PBO or holdout false positives,
- excessive runtime/memory without quality gain,
- promotion increase driven only by thin/noisy folds.

### Workstream B: PBO wall analysis

Goal: determine whether high PBO is a true overfit signal or a measurement/selection-surface mismatch.

Questions:

- Does PBO remain high after purged-kfold OOS gives more aligned evidence?
- Are high-PBO rows concentrated by timeframe, strategy family, asset class, or candidate count?
- Does PBO use the same evidence surface the specialist selector optimized, or only a related full-
  window validation surface?
- Are no-trade/near-zero columns affecting ranks in a conservative but distorting way?
- Should per-regime PBO become available instead of per-timeframe candidate-family PBO?

Do not relax the `0.5` threshold until this analysis is done.

### Workstream C: 8-spoke evidence tensor prototype

Goal: implement the path-dependent specialist evidence geometry as an artifact and post-run selector,
not yet as the optimizer default.

Minimal tensor implementation:

```text
CandidateQualityTensor:
  cell: symbol/timeframe/regime
  candidate_id
  evidence_axis: trade_bucket | time_bucket | fold | oos_slice
  metrics:
    edge_survival
    profit_structure_quality
    capital_preservation
    tail_risk_control
    temporal_consistency
    generalization_survival
    execution_cost_survival
    parameter_stability
  values: evidence_step x metric
  derived_shape_features
```

Prototype stages:

1. Build tensor from existing W4 OOS candidate manifests where possible.
2. Identify missing sensors needed for full spokes.
3. Add missing evidence exports in the engine only after confirming they are required.
4. Score post-run selector rules against W4 baseline.
5. Compare against legacy selector and max-PF/PF-efficiency controls.

Initial selector roles:

- hard pathology gates,
- ranking diagnostics,
- tie-break among candidates that pass hard gates,
- not yet the sole objective.

Acceptance criteria:

- A geometry selector must beat W4 baseline on forward/OOS gate quality, not just look better on
  shape.
- It must not increase false-promote rate.
- It must produce explainable cell-level evidence.
- It must preserve specialist freedom and avoid portfolio-balance penalties.

### Workstream D: Parameter stability / plateau robustness

Goal: prevent needle-peak parameter winners.

Questions:

- Can Optuna expose local neighborhood candidates around the winning config without a full rerun?
- How often do current W4 winners sit on a broad plateau versus a single spike?
- Does plateau support predict holdout/PBO survival?

Potential metrics:

- neighbor pass rate,
- local score variance,
- rank stability,
- parameter perturbation survival,
- cluster support around winner.

This is likely one of the most important 8-spoke additions, but it needs actual candidate-neighbor
evidence before implementation.

### Workstream E: Data and resource architecture

Goal: make large discovery runs reliable on low RAM without degrading accuracy.

Questions:

- What exact memory is retained per worker during Optuna/backtesting/OOS export?
- Can one worker process one `symbol/timeframe` end-to-end and then release memory deterministically?
- Can disk cache remain content-addressed and disposable without stale-read risk?
- Can future production hardware run 4 workers safely with bounded memory per worker?

Acceptance criteria:

- parallel/sequential parity,
- no stale cache,
- bounded RAM profile,
- resumable per-symbol/per-timeframe outputs,
- no global precompute dependency that overwhelms storage or memory lifecycle.

### Workstream F: Tenet-3 resilience before live/test activation

Goal: harden live/test environment behavior after the discovery selector improves.

Current live activation is premature because only one strong specialist exists. Still, resilience
work remains mandatory before test-live activation:

- stale feed watchdog,
- broker reconnect behavior,
- order-result idempotency,
- partial fill and rejection handling,
- margin close-out prevention,
- spread spike no-new-trade controls,
- realized/floating loss guards,
- clean dashboard/status telemetry.

Acceptance criteria:

- failures protect capital without permanently blocking recovery,
- logs expose actionable state,
- tests cover common MT5 failure modes,
- no discovery artifact is treated as live-valid without current broker specs and freshness checks.

## 8. Experiment Sequence

### Phase 0: Freeze current truth

Already done:

- W4 complete.
- Stress replay complete.
- Holdout diagnostic complete.
- Holdout construction projection complete.

Need:

- Claude audit of the holdout construction projection.
- Confirm no unresolved High/Critical findings in active audit notes.

### Phase 1: Purged-kfold OOS prototype

Implement opt-in only.

Suggested config flags:

```json
{
  "oos_validation_mode": "fixed_holdout|purged_kfold",
  "oos_kfold_splits": 5,
  "oos_embargo_bars": "<derived or explicit>",
  "oos_min_trades": 5,
  "oos_aggregate_policy": "all_folds|aggregate_with_worst_fold_floor"
}
```

Run a bounded replay:

- start with a small representative subset,
- include promoted, review, and rejected candidates,
- include H4/D1 sparse specialists and lower-timeframe higher-frequency cells,
- compare fixed holdout vs purged-kfold.

Decision gate:

- If purged-kfold gives sufficient OOS evidence without false-positive inflation, graduate to a
  larger run.
- If it only adds noisy positives or creates leakage risk, reject or revise.

### Phase 2: PBO reassessment

After purged-kfold evidence exists:

- recompute PBO with the new evidence surface,
- compare high-PBO rows against old W4,
- identify whether PBO is blocking true survivors or correctly rejecting overfit.

Decision gate:

- Keep threshold if it remains predictive.
- Modify artifact construction if PBO is measuring the wrong surface.
- Do not lower threshold purely to get more promotes.

### Phase 3: 8-spoke post-run selector

Build tensor and run post-run selection comparison.

Controls:

- legacy score,
- max-PF,
- volume/geomean,
- path-geometry gate + PF,
- 8-spoke specialist geometry.

Metrics:

- promotions after gates,
- OOS PF/return,
- stress survival,
- PBO,
- holdout/kfold survival,
- capital preservation,
- late-rescue dependence,
- runtime cost,
- explainability.

Decision gate:

- Only promote to optimizer-default consideration if the 8-spoke selector beats controls on the
  actual promotion-readiness suite.

### Phase 4: Optimizer scorer integration

Only after post-run proof:

- add flags,
- run same-roster small re-opt,
- compare against W4-like control,
- audit code for complexity/spaghetti risk,
- then decide default.

### Phase 5: Live/test activation readiness

Only after enough high-confidence specialists exist:

- refit selected validated configs on full recent data,
- run final stress/PBO/OOS/provenance review,
- ensure Tenet-3 live safeguards green,
- deploy to test environment with clear kill-switch and monitoring.

## 9. Data Artifacts to Keep for Now

Keep these until their decisions are closed:

- W4 completion report and results.
- W4 regime surface.
- W4 stress max/p90/p95 replays and stress realism summary.
- W4 holdout diagnostics with construction projection.
- OOS evidence manifests/matrices for W4.
- `preview.html` and `scoring_model_proposed_vs_real.html` while the scoring concept is still being
  reconciled.
- Active audit notes that still contain unresolved High/Critical findings.

Delete or retire when done:

- smoke artifacts,
- interim JSONs that have been replaced by a named decision artifact,
- stale audit docs whose findings are closed,
- generated caches and `__pycache__`,
- old roadmap versions after durable conclusions are moved to `CHANGELOG.md`.

## 10. Current Recommendation

Do not change scoring defaults yet.

Do not relax holdout min-trades.

Do not relax PBO.

Do not focus on stress relaxation.

Do not add more strategies until the next same-roster experiment is complete.

The next implementation should be:

1. opt-in purged/embargoed k-fold OOS prototype,
2. small replay proving whether the projected `+10` holdout headroom survives actual OOS PF/return,
3. PBO reassessment on the improved evidence surface,
4. 8-spoke path-dependent specialist tensor as a post-run selector,
5. only then optimizer scoring integration.

This sequence keeps the work aligned with the core rule: proof dictates progress.
