# Screening Runs, Seen Data, and the Bleeding Edge

Author: Claude (research + audit lane) — 2026-07-12
Operator questions addressed: (1) does the first full screening run count as
"seen data"? (2) where should validation data come from, and how do we avoid
spending our best (most live-relevant) data on validation instead of trading?
Companion docs: EVIDENCE_MINIMUMS_AND_BOUNDARIES_20260712.md.

---

## Part 1 — The screening run and "seen data"

### 1.1 The operator's claim, restated precisely

A first pass that runs every baseline strategy across every cell over long
history exists to eliminate irredeemable strategies (no feasible live path)
and save compute for the expensive optimization stages. Claim: this pass
should not count as "seen data" for the final model, because nothing was
optimized on it — data only becomes "seen" once optimization touches it.

### 1.2 Verdict: right purpose, wrong justification — and cheaply fixable

The funnel purpose is correct and standard practice (multi-stage selection /
successive halving; Optuna's pruners are the same idea inside one stage).
The "doesn't count" claim is **formally wrong as stated**: information leaks
through *any* selection decision, not only through parameter fitting. If a
screening pass looked at the final lockbox window and discarded losers, the
survivors are enriched for strategies that happened to do well in the
lockbox period — the lockbox verdict on survivors is biased upward even
though no parameter was ever fitted there. This is selection bias / data
snooping (White's Reality Check; Bailey & López de Prado's Deflated Sharpe
Ratio exists precisely to correct for *all* trials, including discarded
ones).

However, the *magnitude* intuition behind the operator's claim is sound. A
coarse screen that removes only catastrophic, unrecoverable strategies
(e.g., cost-inclusive PF < 0.8 over years, structural DD > 60%, too few
trades to ever validate) makes a weak selection — it leaks on the order of
`log2(1/survival_rate)` bits per decision, versus thousands of effective
trials in a full Optuna search. Weak selection ≠ no selection, but it is
cheap to account for honestly.

> **SUPERSEDED (coordination seq819, 2026-07-14): the default-parameter
> catastrophic screen above must NOT be wired as a production invariant.**
> The two amendments below make the screen *statistically* priced, but
> statistically-free ≠ economically safe: default-parameter performance
> does not bound the optimized family (parameters change entry frequency,
> holding, stops, cost exposure), so the screen is a wrong-proxy filter
> that can remove the true cell winner before the full search —
> violating quality-before-efficiency. Production integration is
> full-family/full-attempt. Compute pruning is admissible only for
> structural invalidity or a precommitted, mathematically safe
> futility/upper-bound rule, replay-validated against representative
> complete searches with ZERO missed Winner-admissible candidates under
> the exact live objective — and every pruned attempt stays in
> multiplicity.

### 1.3 The two amendments that make the funnel statistically free

1. **The screen must never see the final lockbox.** Run it on
   optimizer-train only (train + tuning at most). Then all information it
   consumes comes from partitions already designated as search surface, and
   the lockbox stays pristine. This gives the full compute saving at zero
   inferential cost.
2. **Screened trials still enter the multiplicity ledger.** Every strategy
   that touched the data — including ones discarded at the screen — counts
   in the DSR/PBO trial counts for that cell. Discarding a strategy does not
   un-spend the look.

With both amendments, the operator's position becomes correct in practice:
the screening run costs the lockbox nothing, and its cost to train/tuning
inference is explicitly priced through the trial ledger rather than ignored.

### 1.4 What the code actually does today (verified 2026-07-12)

- There is **no separate full-data screening pass** in calendar-lockbox v2.
  The "first run" IS Phase A: all registered strategies receive
  power-allocated Optuna trials on **optimizer-train**, candidates are
  ranked by train score with a validation-aware tiebreak on **tuning**
  (pm_pipeline ~2003), and the final lockbox is untouched until the sealed
  one-shot Phase B joint replay (pm_two_phase_lockbox: Phase A uses
  train+tuning only; Phase B may not re-rank after seeing the lockbox).
- Therefore the operator's feared defect — a screening pass contaminating
  the lockbox — **does not exist in the current design**. The design already
  implements the correct version of the operator's intuition.
- What is missing is the compute saving: today every strategy pays full
  Optuna search in every cell. A cheap default-parameter screen on
  optimizer-train (one backtest per strategy per cell; catastrophic-only
  cuts; survivors → Optuna; screened trials ledgered into DSR multiplicity)
  is a legitimate optimization. Proposed to Codex as a lane
  (screening-stage spec below); it must be categorically labelled a compute
  funnel, never an evidence source.

### 1.5 Screening-stage spec (proposed)

- Input: optimizer-train partition only, default parameters, full cost
  model (conservative_max spreads — a strategy that cannot survive honest
  costs at default params on 5y of train has no redemption path).
- Cuts (catastrophic-only, pre-registered, no tuning): cost-inclusive
  PF < 0.8; max DD > 60%; trades below the power floor for the cell's
  claimed edge class; strategy raises/degenerates.
- Output: per-cell survivor list + a screen ledger
  (strategy, cell, metrics, verdict) merged into the DSR trial count.
- Explicit non-goals: no ranking influence beyond pass/fail, no
  tuning-partition access, no lockbox access, no parameter search.

## Part 2 — The bleeding edge: where validation lives and who trades "now"

### 2.1 The tension, stated honestly

Under non-stationarity the most recent data is the most predictive of the
near future — and honest out-of-sample evaluation demands that exactly this
data stay untouched. You cannot both train on the freshest data and use it
as pristine validation. Every serious methodology is a negotiated settlement
of this conflict; anyone who claims it away is selling something.

### 2.2 What the current design does (verified)

Calendar v2 partitions counted back from now: 5y train → purge → 2y tuning →
purge → 2y final lockbox. Parameters are selected using information through
the **tuning cutoff ≈ 2 years ago**; the lockbox pass/fail is the only bit
of information taken from the last 2 years. The deployed champion is
therefore **~2 years stale on the day it goes live** — the operator's fear
is not hypothetical; it is the design's explicit price for a clean one-shot
lockbox.

The codebase already contains the counterweight: a two-book temporal
lineage (pm_model_lifecycle): `LOCKBOX_VALIDATED_CHAMPION` (stale but
honestly validated) vs `LATEST_DATA_EDGE_CHALLENGER` (re-optimized on the
freshest data; explicitly does **not** inherit the champion's validation;
runs demo/forward until it earns promotion). This is correct and
conservative — but as it stands, the freshest model waits on forward
evidence with no defined budget, which under-serves the bleeding-edge
requirement.

### 2.3 The established answer: validate the process, then refit to now

The resolution used by practitioners for decades (Pardo's walk-forward
analysis is the canonical treatment; López de Prado's CPCV/embargo work is
the modern statistical framing):

1. **The lockbox validates the *process*, not the parameter vector.** The
   frozen object under test is the entire deterministic pipeline: screen →
   search → rank → gate. A single lockbox pass is one observation of that
   process's out-of-sample behavior.
2. **Walk-forward turns one observation into a distribution.** Run the
   identical frozen process at multiple historical as-of dates (e.g.,
   quarterly anchors over the deepest history available); each anchor
   yields an honest OOS segment. Aggregate: OOS PF/DD distribution,
   consistency across anchors, DSR on the aggregate. This is far stronger
   process evidence than one 2-year lockbox — and it is the evidence that
   *licenses* trading a fresh refit.
3. **After the process passes, refit through the present and deploy the
   refit** — because the deploy decision was made before the refit ran, the
   process evidence remains honest. The refit parameters themselves carry
   process-level validation, not parameter-level validation; that residual
   risk is priced by the next two controls, not by more backtesting.
4. **Probation sizing + forward evidence ramp** (already implemented
   2026-07-07: probation tier at 0.5× risk for 14 days, stacking with
   PROMOTE_SMALL) — the fresh refit trades *immediately* but small, and
   earns full size through live/demo evidence. This is the honest form of
   "no intermediary delay": deployment is immediate, trust is earned.
5. **Edge-decay measurement closes the loop.** The walk-forward anchors
   directly measure OOS performance as a function of time since the
   training cutoff — the empirical decay curve per timeframe. That curve
   (a) quantifies exactly what the current 2-year champion staleness costs,
   (b) sets the retrain cadence per timeframe from data instead of the
   folklore 28 days, and (c) answers the operator's "what sizes in terms of
   depreciating that live edge" with a measured half-life instead of a
   guess.

### 2.4 Why not just validate on older data and train on the newest?

The legacy bleeding-edge mode did this (validate on the 50k bars *before*
the training window). Reverse-holdout is not worthless — an effect that
holds both backward and forward is more likely structural — but validating
only on the past cannot detect that an edge has *recently died*, which is
the single most common failure mode of deployed strategies. The
walk-forward + refit + probation stack achieves the same freshness with
forward-facing evidence at every step. (Project history is consistent:
the governance memo records that the old tournament-on-recent-data
selected noise.)

### 2.5 Recommended decisions (for Codex cross-review)

- D1: Add the train-only screening stage per §1.5 (compute funnel; screen
  ledger feeds DSR multiplicity). Priority: after current blocking lanes.
- D2: Add walk-forward process validation as the evidence backbone around
  the existing Phase A/B machinery (multiple historical as-of anchors of
  the same frozen process; aggregate OOS stats + decay curves per TF).
- D3: After a joint Phase B PASS of the process, the production book is a
  **refit-to-present** run of the identical frozen process, deployed under
  probation sizing with the existing challenger lineage labels — the
  champion/challenger machinery stays, but the refit becomes the default
  production path rather than an optional challenger, with promotion
  budgets defined in advance (probation days + evidence thresholds), so
  "bleeding edge" has a bounded, pre-committed delay instead of an
  open-ended one.
- D4: Edge-decay study on walk-forward outputs sets per-TF retrain cadence
  (replacing uniform 28d where the data disagrees) and quantifies champion
  staleness cost.
- Non-negotiables preserved: one-shot lockbox semantics (a failed Phase B
  burns the lockbox; the next attempt needs new data or explicit
  multiplicity accounting); demo shadow before funded; funded enablement
  requires operator approval per protocol.

## Part 4 — Operator follow-up (2026-07-12 late): mixture-of-experts framing and the middle holdout

### 4.1 What the operator clarified

FXPM is a consolidation of experts (strategy × regime × timeframe cells,
potentially thousands), each activated when conditions match its expertise.
Three claims followed:

1. Per-timeframe data minima necessarily elongate the calendar window as the
   timeframe rises (50k M5 bars ≈ 1 year; 50k H1 bars ≈ 8 years), giving each
   expert a progressively longer "mental perspective" matched to its trading
   horizon — and this is not a problem.
2. The real risk sits in **governance**: deciding which experts are still
   the best to execute live.
3. OOS validation should be **bitten out of the middle** of the data (train
   on both sides), because reserving the trailing window for validation
   keeps the train-end→live gap open, and edge demonstrably decays with
   that gap ("bleeding edge") — live suffers while validation looks fine.

### 4.2 Assessment

**Claim 1 — agreed, with one governance caveat.** Horizon-matched lookback
depth is a feature: a D1 expert *should* average over more regime epochs
than an M5 expert, exactly as its holding period spans more regimes. Two
disciplines keep it safe: (a) evidence comparability across experts is
normalized in effective trades and power (pm_trade_power /
pm_sample_dependence), never in bars or years, so a 9-year H4 window and a
1-year M5 window can be compared honestly; (b) elongation must stop where
history stops helping — the edge-decay curves (D4) tell each timeframe
where old epochs stop being informative rather than assuming all history
is good history.

**Claim 2 — agreed, and this project already has the receipt.** The
2026-07-07 selection-bias study on this codebase's own book measured
Spearman ρ ≈ 0.26 between stored validation-slice metrics and true
full-sample outcomes, and ~0.30 as the ceiling for any formula over stored
slices. Backtest-slice cleverness cannot govern a live expert book. Live
governance must therefore be **forward-evidence-weighted**: probation
sizing for fresh cells, live/demo outcome feedback, cycle-drift derating
and pausing — with backtest evidence setting who *enters* the book, and
forward evidence deciding who *stays* and at what size. Recent-window
tournaments were already tried and selected noise (governance memo).

**Claim 3 — the phenomenon is real; the proposed placement fixes the right
problem at the wrong layer.** The middle holdout is legitimate — with both
boundaries purged/embargoed, it is exactly a purged interior fold (CPCV,
López de Prado), and it lets training touch the present. It is adopted
here for the **tuning/selection surface**. But it cannot serve as the
**final gate**, for one reason that no procedure can wash out: an interior
window is bracketed by training data on both sides, so passing it proves
*interpolation*; live trading is *extrapolation*. Interior estimates are
systematically optimistic under drift, and — decisive for live money — an
interior holdout is structurally blind to an edge that died recently,
which is the single most common way deployed strategies fail. A gate that
cannot see recent death will wave dead experts into the live book, and the
error will surface only in the account balance.

### 4.3 Reconciliation — the operator's topology, achieved honestly

The refit-to-present pattern (D3) *is* the middle-holdout topology, with
the decision order flipped so the gate stays honest: relative to the
**deployed** (refit-through-now) model, the historical walk-forward OOS
segments are interior — training data exists on both sides of them. The
difference from a naive middle bite is only procedural: the deploy/no-deploy
decision is taken on trailing-OOS evidence *before* the refit runs, so the
gate retained its power to detect recent edge death, and the deployed book
still carries zero structural staleness. The operator's requirement is met
in full; the honesty cost is zero.

### 4.4 Amended decisions (supersede D2/D3 wording; for Codex cross-review)

- **D2′**: tuning/selection may use purged interior folds (CPCV-style,
  embargo both sides) so selection sees near-present data; the **gate**
  evidence remains trailing OOS from walk-forward anchors of the frozen
  process.
- **D3′**: trailing gate windows are sized **per timeframe by power
  analysis** (enough expected trades to detect the claimed edge class —
  likely months for intraday, longer for H4/D1), not a uniform 2 years;
  multiple shorter trailing segments beat one fat lockbox. After a PASS,
  production book = refit-to-present under probation sizing with
  pre-committed promotion budgets (unchanged).
- **D6 (new)**: live governance contract — entry to the book by backtest
  evidence (power-floored, DSR/PBO-corrected); continued presence and size
  by forward evidence (probation ramp, live-outcome feedback, cycle-drift
  derate/pause). No recent-window re-tournaments.

## Part 3 — References

- R. Pardo, *The Evaluation and Optimization of Trading Strategies* —
  walk-forward analysis; trading the most recent reoptimization after WFA
  passes.
- M. López de Prado, *Advances in Financial Machine Learning* — purged
  K-fold/CPCV, embargo, backtest overfitting, PBO.
- Bailey & López de Prado — Deflated Sharpe Ratio (multiplicity-corrected
  performance inference; count all trials including discarded).
- H. White, *A Reality Check for Data Snooping* (Econometrica 2000);
  Romano & Wolf stepdown — selection across many strategies.
