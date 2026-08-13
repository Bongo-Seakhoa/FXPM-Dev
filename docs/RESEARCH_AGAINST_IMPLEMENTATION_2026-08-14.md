# Research against the implementation — 2026-08-14

Literature checked against what FxPM actually does, not against what a generic
trading system does. Every claim below is anchored to a file and line, and where
the code is already correct that is stated so nobody "improves" it later.

Written for the operator's end goal: **profitable and reliable in live trading.**

---

## What is already correct, and must not be touched

**The Deflated Sharpe Ratio is a faithful Bailey–López de Prado implementation.**
`pm_statistics.py:249` computes

```
E[max SR] = σ_SR · [ (1-γ)·Φ⁻¹(1 - 1/N) + γ·Φ⁻¹(1 - 1/(N·e)) ]
```

That is the **two-term** expression from the paper. Most public summaries — and
the one I fetched — quote only the first term. Whoever wrote this used the
source, not a blog post.

**The PSR standard error carries the higher moments.** `pm_statistics.py:190`:

```
variance_term = 1 - skew·SR + ((kurtosis - 1)/4)·SR²
```

That is the Mertens expression Bailey & López de Prado use. Non-normality is
handled correctly.

**Selection-consumption is enforced.** The literature's sharpest warning about
walk-forward is that re-running it after adjusting the strategy destroys the
out-of-sample property. FxPM already blocks this: *"final lockbox already
consumed for this lineage/cell"*. Many published frameworks do not.

**Fold aggregation is robustness-oriented, not naive.**
`pm_core.py:1526` sets `oos_aggregate_policy = "aggregate_with_worst_fold_floor"`,
with the worst fold applied as a floor (`pm_pipeline.py:8767`). This directly
implements the literature's *"choose a stable strategy across folds, never the
best strategy per fold."*

---

## Gap 1 — Minimum Track Record Length is absent, and it is the cheapest win

**Status:** `mintrl` / `minimum_track_record` appears in **zero** engine files.

DSR answers *"is this Sharpe significant given N trials?"* — a **post-hoc**
question. MinTRL answers *"how many observations would this Sharpe need before it
**could** be significant?"* — a **pre-registration** question, computable before
a single backtest runs.

```
MinTRL = 1 + [1 - skew·SR + ((kurt-1)/4)·SR²] · ( z_α / (SR - SR*) )²
```

Every term is **already computed** in `pm_statistics.py`: `sharpe`, `skew`,
`kurtosis`, `variance_term`, and `expected_max` as the benchmark `SR*`. This is
a handful of lines over values already in scope.

**Why it matters here specifically, in three places:**

1. **D1 was declared infeasible geometry *after* burning a canary.** MinTRL
   would have said so in advance, from bar counts alone, for zero compute.
2. **`false_discovery_min_daily_observations = 252` is a hardcoded constant.**
   MinTRL makes the required observation count a *function* of the observed
   Sharpe and its moments. A cell with SR 0.4 and a cell with SR 2.0 do not need
   the same track record, and today they are held to the same one.
3. **Test B's 365-day horizon is asserted, not derived.** MinTRL derives the
   required length from the target Sharpe. If the honest answer is longer than
   365 days, the operator needs to know that *before* committing a year.

**Classification:** Class B (changes what gets rejected), justified on accuracy.
It is **additive** — it adds a dimension to the decision rather than narrowing
the search, so it runs with the never-reductionist direction rather than against
it.

**Recommended as a pre-flight report first, gating second.** Compute and record
MinTRL per cell without enforcing it, compare it against what the existing gates
actually rejected, and only then decide whether it becomes a gate. That gives
evidence for a Class B change instead of an argument.

---

## Gap 2 — the recency question is a trade-off against an existing policy

The operator raised recency-weighted folds. The literature supports the
motivation: FX is strongly non-stationary, rolling windows "naturally adapt by
forgetting stale data," and data from three years ago "may actively mislead."

**But FxPM does not equally weight folds.** It applies a **worst-fold floor**.
So recency weighting would not fix an oversight — it would **replace one
deliberate robustness policy with a different adaptation policy**, and the two
genuinely conflict:

| | Rewards | Fails to catch |
|---|---|---|
| **worst-fold floor** (current) | survives every regime in the sample | a strategy whose edge has genuinely decayed, if old folds still carry it |
| **recency weighting** | adapts to the current regime | a strategy that works now and collapses when the regime turns |

The second column is the honest cost of each. A strategy that thrived recently
but failed an old fold **passes recency weighting and fails the current policy**.
Which is right is an empirical question about this universe, not a principle.

**Recommendation: do not swap. Measure both.** The existing machinery already
scores every fold; computing a recency-weighted aggregate *alongside* the
worst-fold floor costs one extra reduction over numbers already in memory. Record
both, promote on the current policy, and compare the two rankings against the
forward window when it scores. That is a real experiment with a pre-registered
comparison, and it costs almost nothing to run.

**Do not decide this from the backtest.** Recency weighting will *always* look
better in-sample on non-stationary data — that is what it is designed to do. Only
the forward window can separate adaptation from overfitting-to-recency.

---

## Gap 3 — one window geometry where the literature recommends two

FxPM runs `oos_validation_mode = "fixed_holdout"` with
`production_split_mode = "calendar_lockbox_v2"`. The walk-forward guidance is
explicit that for FX one should **run both anchored and rolling**: a strategy
passing under both is robust, and one passing under only one has told you
something specific about what it actually exploits.

**This is a coverage increase, not a reduction**, so it sits on the right side of
the operator's direction. The cost is a second pass over the same folds, and the
information gained is a discriminator we do not currently have: *is this edge
structural or is it regime-local?*

**Sequencing:** this is worth doing **after** the fixed-cost baseline exists.
Doubling the validation geometry before we know what one pass costs would repeat
the mistake the withdrawn cost model already made.

---

## Gap 4 — PBO's verdict rests on a candidate count worth re-examining

Gate 1 returned `pbo = 0.0` with `S = 8/70`. CSCV's discriminating power depends
on having enough candidates to rank; `false_discovery_pbo_min_candidates = 5` is
low for a method whose output is a *rank distribution*.

A PBO of exactly 0.0 is the strongest possible result, and a strongest-possible
result from a thin candidate set deserves scrutiny rather than celebration —
that is the same reasoning that condemned the June book. **This is a question,
not a finding:** I have not established that the PBO verdict is weak, only that
its candidate count is at the bottom of the configured range and nobody has
checked the sensitivity.

**Cheap test:** recompute PBO for the Gate-1 cell at several candidate counts and
see whether 0.0 is stable or an artifact of thinness.

---

## What I did not find worth changing

- **No haircut-Sharpe / SPA / reality-check machinery.** These are alternatives
  to DSR, not complements. DSR is correctly implemented and adding a second
  multiple-testing framework would be ceremony, not protection.
- **No regime-detection upgrade proposed.** HMM-based regime detection is
  well-published, but the current thresholds are not the demonstrated weakness —
  the demonstrated weaknesses were thin samples and inoperative defences.
  Changing regime detection now would move a variable nobody has shown is wrong.

---

## Priority, on the operator's own criterion

Ordered by *does this change the probability the deployed book makes money, or
keeps making money?*

1. **MinTRL as a pre-flight report** — cheapest, and it prevents burning another
   canary on infeasible geometry. Also the only item that could shorten or
   correct the Test B schedule.
2. **Recency-vs-worst-fold measured in parallel** — near-zero cost, and it turns
   an open question into a pre-registered forward comparison.
3. **PBO candidate-count sensitivity** — cheap, and it either confirms Gate 1 or
   reveals that our one clean statistical result is thinner than it looks.
4. **Dual window geometry** — real information, real cost; after the baseline.

Items 1–3 are all cheap enough to run before the next full attempt. None of them
narrows the search.
