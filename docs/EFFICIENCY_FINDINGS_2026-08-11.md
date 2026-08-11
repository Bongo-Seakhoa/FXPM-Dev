# Efficiency findings — 2026-08-11

**For Codex-PM to review on return.** Written while Codex was offline, so
nothing here has been through PM debate. Every number has a reproduction path.
Where I corrected myself, the wrong figure is left visible with the correction,
because Codex may already be reasoning from the wrong one.

Two changes are committed (`811669c`, `0c2e848`). Both are bit-identical
transformations pinned by tests that passed against unchanged code *first*.
Everything else here is a finding awaiting a ruling.

---

## 1. The headline

The operator's challenge was that the runtime estimates were "absolutely and
sufficiently nonsense" because a comparable academic framework does 50 symbols
× 50 strategies in **6 hours**. That challenge was correct, and the measurement
proves it.

| | |
|---|---|
| The jitted backtest loop, 106,697 H1 bars, warm | **6.7 ms** |
| `Backtester.run` wrapping it, unprofiled, pre-fix | **548 ms** (82×) |
| Production, inferred from the Gate-2 canary | **~1.78 s** |

The economic work is 6.7 ms. Everything else is machinery around it.

**At the loop floor, the full 576-million-evaluation universe is 44.7 CPU-days
— about 7.4 days of wall clock across 6 workers.** The universe is not too big.
The per-evaluation cost is too high. That reframes the whole problem from
architecture to arithmetic:

> **Every 100 ms removed from a backtest is roughly 8 months off the wall clock
> of a full run.**

That is the exchange rate all efficiency work should be judged against.

---

## 2. What the run actually costs

`config.json: regime_hyperparam_max_combos = 200` feeds straight into
`optuna_optimizer.config.n_trials` at `pm_pipeline.py:4480`. So one *attempt*
is one strategy-configuration carrying ~200 Optuna trials, each scored across
4 folds — about **800 `Backtester.run` calls per attempt**.

- Per cell: 300 attempts × 200 trials × 4 folds = **240,000 backtests**
- Full universe: 2,400 cells × 240,000 = **576 million backtests**

**Cross-check that this model is right:** at 2.27 s (the then-current profiled
figure) it predicted ~3,632 s for two attempts. Codex's canary reached attempt 2
in **2,851 s**. Prediction and observation agree within the slack expected from
fold subsets and pruning.

### The gap decomposes into two independent factors

| factor | size | source |
|---|---|---|
| evaluation count | ~1,152× | 6 TF × 4 regimes × 3 variants × 4 folds |
| cost per evaluation | ~53× | vs the academic baseline's ~43 ms |

Product ≈ 61,000×, which is what turns 6 hours into years. **Neither factor
alone explains the gap.** Fixing only overhead leaves 1,152×; scoping only the
universe leaves every survivor 53× too expensive.

**The 6 timeframes and 4 regimes are the product thesis, not overhead.** The
reducible parts are the 3 variants and the 200-trial budget. Open question, not
yet measured: whether 60,000 parameter evaluations per cell buys anything over
a few hundred, given Optuna typically converges in low hundreds.

**My own contribution, stated plainly:** I raised the planner from 128 to 300
attempts (`BASELINE_ATTEMPTS=100`, `VARIANTS_PER_STRATEGY=2`) so every strategy
is tuned three times instead of 28-of-100 tuned once. That is a deliberate
**2.34× cost increase I introduced** for search coverage, back when coverage
looked free. I am not withdrawing it — thin coverage produced the June noise
book — but it should be re-decided against this cost picture, not inherited.

---

## 3. Committed

### `811669c` — partition-axis digest, 1.96× off every backtest

`build_backtest_partition_axis` (`pm_core.py:634`) sealed the scored bar axis
with a Python loop over every bar, boxing two Timestamps and calling
`hashlib.update()` twice per bar. On 106,697 bars: 213k `to_bytes` calls, 213k
digest updates — **essentially all the hashing in the entire backtest** — to
seal an axis fully determined by `(start, end, count, timeframe)`.

This is the operator's "don't hash one pipeline seven ways" directive with a
price tag attached.

| | |
|---|---|
| digest, isolated | 287.9 ms → **19.2 ms** (15×) |
| `Backtester.run`, unprofiled | 548.4 ms → **279.7 ms** (1.96×) |

### `0c2e848` — per-bar metrics, 94.5 ms off every backtest

In-process interleaved A/B, best of 7:

| site | old | new | |
|---|---|---|---|
| `pm_core._max_drawdown_from_equity_curve` | 68.56 ms | 6.45 ms | 10.6× |
| `pm_metrics.safe_float_array` (sharpe path) | 37.15 ms | 4.75 ms | 7.8× |
| `pm_metrics.max_drawdown_from_equity` | 127.89 ms | 6.41 ms | 20.0× |

**Only the first two are claimed** — 94.5 ms, ~105 days off a full run.
`max_drawdown_from_equity` has **no production callers** (grep finds only
tests), so its 121 ms is not a production saving.

**Deliberately not vectorized:** `expectancy_per_trade` and `gross_profit_loss`
use Python `sum()`/`+=`. Pairwise summation reorders the adds and moves results
in the last bits. These feed **gate thresholds**, so a last-bit change is real
risk for no gain — and both are per-trade, not per-bar.

### How equivalence was established, both times

1. Keep the original implementation **verbatim** in the test as an oracle.
2. Capture golden digests from the old code **before any edit**.
3. Run the test against **unchanged** code and require it to pass.
4. Only then change the implementation.

Step 3 is the one that matters. On L-0016 it caught a bug in *my own oracle*
rather than in production. Without it, a test only proves the new code agrees
with itself.

Coverage: 66 subtests (axis) and 402 subtests (metrics), including `nan`, `inf`,
`None`, junk strings, bools, empty, single-element, `1e308`, `1e-320`, and
monotonic curves both directions.

---

## 4. Findings awaiting a ruling

### 4.1 Gate 2 root cause — the cost-stress invariant is right, the stress is wrong

**This is the blocker that killed the H1 canary, and I believe it blocks every
run.**

`pm_core.py:8411` widens the entry spread, moving the entry price adversely —
correct. But `pm_core.py:8465` then rebuilds the swap arrays from the
**stressed** entry prices, and per-lot swap scales with entry price through the
notional conversion (`pm_core.py:7477`). For a **positive-carry** direction, a
worse entry price yields a **larger positive credit**. Trade 23 was held across
enough rollover units that accumulated carry outran the one-time spread cost,
so the invariant at `pm_core.py:8502` fired.

The invariant is doing its job. The stress construction is adverse in price and
unconstrained in swap, so it is not monotonically adverse overall.
**Do not widen the tolerance** — that hides a real non-monotonicity.

**Blast radius:** the check entered at `e6e8be4` (2026-07-17), after the June
book completed, so no finished run has ever exercised it. The canary died
**0.67% into the search**. Any full run launched today dies within minutes of
the same cause.

**Smallest defensible repair, proposed not implemented:**
`stress_swap = min(stress_swap, base_swap)`. One line, monotone by construction,
preserves the genuine case where stress makes swap more costly. Supporting
argument: the swap contract self-reports `historical_rates_authenticated: false`,
so re-deriving swap from a stressed price is precision on unauthenticated data.

### 4.2 Seven anti-forgery tests are absent, not failing

`tests/test_causal_evidence_producer.py` — 7 setup errors, all
`FileNotFoundError` on a fixture `provenance.json`. **Proven pre-existing** by
stashing my change and re-running against HEAD: identical result.

Read what they cover: a caller cannot inject its own return vector; a
coordinator request cannot relabel the historical book; future-refit leakage is
rejected; a cost tamper fails before returns. That is the anti-forgery and
anti-leakage surface, and **none of it executes**. The suite reports "18 passed"
while silently skipping its seven most important assertions.

A suite that does this is worse than one that fails — it buys confidence it has
not earned.

### 4.3 An O(n²) landmine behind a comment

`pm_strategies.py:772` takes an O(1) `.iat` path when `ATR_14` is present, and
otherwise slices `features.iloc[:bar_index+1]`, builds three Series, concats and
takes a full `DataFrame.max` — **per trade**. I hit this accidentally by naming
a synthetic column `ATR` instead of `ATR_14`: it turned a 548 ms backtest into
49 s, a ~20× silent slowdown.

Production emits `ATR_14`, so this does not currently fire. It is guarded by
nothing but a code comment saying it "should be rare". That deserves a hard
guard.

### 4.4 Duplicate drawdown implementations with divergent semantics

`pm_core._max_drawdown_from_equity_curve` **skips** non-finite values so they do
not move the running peak. `pm_metrics.max_drawdown_from_equity` maps them to
**0.0** via `safe_float`, which does move it. Same concept, different answers on
dirty input, and the pm_metrics one is dead in production.

---

## 5. Corrections to my own earlier numbers

Left visible deliberately.

| claim | correction |
|---|---|
| "48,000× overhead" | cProfile-inflated. Unprofiled: **82×** |
| "`Backtester.run` = 2.27 s / 339×" | Profiled figures. Unprofiled: **548 ms / 82×** |
| "4 regimes partition the bars" | **False.** `pm_pipeline.py:13079` zeroes signals outside the regime and still backtests the full frame. A cell is symbol\|timeframe\|regime — 24 per symbol, not 6. Cost ×4 |
| "0.81 of 6 cores = wasted 6×" | **False.** `optimization_max_workers: 0` resolves to all logical CPUs; the pool fans out **per symbol** (`pm_pipeline.py:19761`). The canary was one symbol, so one worker is correct |
| "28 years" / "41 years" | CPU-years quoted without dividing by workers. Wall clock across 6 workers: **5.4 years** before the fixes |

The pattern in these errors is worth noting: I twice reported a number that made
the situation look worse than it was, and once one that made it look better. The
fix in each case was measuring instead of scaling.

---

## 6. Where the remaining cost is — and it is not in my layer

Production is ~1.78 s per backtest against my synthetic's 548 ms on identical
bar counts. **Even driving `Backtester.run` to the 6.7 ms floor leaves
production paying ~1.2 s per evaluation somewhere above it.**

That is the largest single remaining term in the program. It sits in the
search-execution and attestation layer. Unmeasured candidates:

- per-attempt feature recomputation across ~100 indicator columns
- the frozen-path replay — a second full backtest plus swap rebuilds plus an
  attestation rebuild-and-compare
- PBO/CSCV
- evidence writing

**Ask for Codex:** profile one attempt end to end and attribute it. If it would
rather I did it, a read-only instrumentation lane is enough.

---

## 7. Reproduction

Scripts are in the session scratchpad, not the repo:

| script | what it establishes |
|---|---|
| `bench_floor.py` | the 6.7 ms loop floor |
| `profile_run.py` | cProfile attribution of `Backtester.run` |
| `bench_axis.py` | old vs vectorized digest, with a digest-equality assert |
| `bench_run.py` | unprofiled `Backtester.run` wall time |
| `ab_metrics.py` | interleaved in-process A/B of the metrics sites |
| `verify_safe_float.py` | numpy fast path vs `safe_float` across 14 adversarial inputs |

In-repo, permanent: `tests/test_backtest_partition_axis_digest.py` and
`tests/test_metrics_vectorization.py`.
