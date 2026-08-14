# FxPM Reconciliation Register

**Purpose.** The 2026 pathfinding attempt asked AI models to do the research *and*
decide what to integrate. The operator's verdict (2026-08-08): that experiment
failed. It nonetheless produced usable elements. This register inventories every
iteration and every candidate element so the **operator** decides keep-or-discard.

**Method — non-negotiable.** This file records findings and costs. It does **not**
record my verdicts as decisions. Every element carries `DECISION: <pending>` until
the operator rules. Reproducing the failed pattern — the model deciding its own
integration — is the one outcome this register exists to prevent.

Status: **first pass, 2026-08-08.** Sections marked `NEEDS OPERATOR CONTEXT` are
gaps I cannot close by reading code.

**Governance update (2026-08-11).** The operator-only decision rule above is
superseded by `agent_comms/PM_CHARTER.md`: the equal PMs may resolve elements
inside that charter, and only a three-round deadlock escalates to the operator.

---

## 1. Iteration map

| # | Iteration | Where it lives | Era | Distinctive |
|---|---|---|---|---|
| 1 | `FxPM` | GitHub, private, archived | Jan 2026 | Original live signal PM |
| 2 | `FxPM-backtester` | GitHub, private, archived | Jan 2026 | Backtester split out |
| 3 | Old engine snapshot | `fxpm-evidence-desk` @ `deploy/fxpm-labs-pages` | ~Jan–Feb 2026 | `pm_core.py` 86 KB; `max_bars` 500k; no Optuna |
| 4 | "FxPM 1.4 - Obtimized" | orphan data dir only (removed) | ~Mar 2026 | Name survives; code not located — **NEEDS OPERATOR CONTEXT** |
| 5 | FXPM-Dev pre-rewrite | tags `backup-pre-cleanup-{acs,main}-20260616` | Feb–Jun 2026 | Numba + Optuna era; `max_param_combos` 200/strategy, Optuna 1500–5000 trials |
| 6 | **Academic runtime** | `fxpm_academic_research` @ `artifact/fxpm_runtime` | ~Jul 2026 | **Different architecture**: `pm_data_pipeline.py` (no `pm_pipeline.py`); `GATE_CATALOG.md`; `RISK_STATE_MACHINE.md`; `pm_core.py` 148 KB |
| 7 | **Gate-ablation package** | `fxpm-gate-ablation-reproducibility` | Apr 2026 | Standalone Monte Carlo + paired statistical study of **validation-gate design**, with expected results and verifier scripts |
| 8 | Codex integration patches | `Documents\Codex\2026-07-10\...\work\` | Jul 2026 | 7 named patches with base/modified pairs (below) |
| 9 | FXPM-Dev institutional recovery | current `HEAD` | Jul–Aug 2026 | 86 modules, 218k lines; 46% authority machinery |

### Iteration 8 — the discrete integration decisions

`calendar_lockbox_v2_patch` · `live_feedback_patch` · `native_timeframe_evidence_patch`
· `native_history_patch` · `portfolio_replay_patch` · `regime_provenance_patch` ·
`risk_patch`, plus reference copies `fxpm_baseline_audit`, `fxpm_current_focus`,
`fxpm_dev_pre_audit_20260710`, `fxpm_academic_reference`.

Each carries `base/` and `modified/` — i.e. a reviewable diff of exactly what a
model chose to integrate. This is the clearest surviving record of the failed
experiment's decision-making and is worth reading before discarding.

---

## 2. Element register

### E1 — Gate-ablation study `DECISION: pending`
`fxpm-gate-ablation-reproducibility/src/gate_ablation_study.py` (34.7 KB) plus
expected results and verifier scripts. Backs the paper *"Simulation and Historical
Evaluation of Validation-Gate Design in a Regime-Adaptive Decision System."*

**Why it matters.** The operator states they never obtained evidence they could
point to about gate design. This package is a calibrated, reproducible, peer-review-
grade study of exactly that question. It may already answer choices the production
engine currently guesses at.
**Cost to keep:** none — it is standalone and already frozen.
**Open:** does its gate taxonomy match today's gates? Not yet checked.

### E2 — Search breadth `DECISION: pending`
Current engine: **128 attempts per cell**, spent as 100 strategy defaults (one shot
each) + **28 non-default grid points total** — `pm_production_search_planner.py:7-14`.
Prior engine: 200 combos **per strategy** plus Optuna at 1500–5000 trials **per
strategy**.

The docstring states the ceiling is why only one calendar depth is permitted, since
more "would make it impossible to cover all 100 strategies."
**Consequence:** strategies are effectively untuned. The DSR/PBO/multiplicity gates
then adjudicate an unsearched space.
**Note:** the ≥300-attempt roster was designed, approved, and parked (seq2294).

**FIXED 2026-08-08.** The ceiling is lifted and the budget reallocated.

| | Was | Now |
|---|---:|---:|
| Registry defaults per cell | 100 | 100 |
| Tuned variants per cell | **28 (shared across the pool)** | **200** |
| Variants **per strategy** | 0.28 | **2** |
| Strategies ever tuned | **28 of 100** | **100 of 100** |
| `ATTEMPT_COUNT` / `MAX_ATTEMPTS` | 128 | **300** |

`VARIANTS_PER_STRATEGY` now drives the allocation, and `_nondefault_parameters`
takes a `variant_ordinal` folded into the same outcome-blind cell hash, so a
strategy can contribute several distinct points without any of them depending on
a result. Collisions advance a deterministic salt within a bounded budget and
fail closed rather than emitting a duplicate or collapsing to the default.

Grounds for the sizing, measured: the smallest strategy grid admits **165**
distinct combinations, the median **4,455**, the largest **1,267,200**, and every
strategy can supply at least 8 distinct non-default points. Sampling 0.28 tuned
points against a median space of 4,455 was not a search.

Payload headroom, measured: a full 300-attempt plan canonicalizes to **67.7 KB**
against `MAX_REQUEST_BYTES` of 16 MB — **248×** headroom, and 300 sits well under
`MAX_CONTAINER_ITEMS` of 4,096. The 128 bound was nowhere near any real limit.

Statistically safe because deflation scales *with* trials rather than being
bounded by them: `pm_statistics` deflates by
`effective_independent_trials = min(n, 1 + (n-1)(1 - avg_correlation))`, so more
attempts automatically raise the bar a winner must clear. Correlated variants of
one strategy cost less deflation than unrelated strategies, which is the right
incentive — tune deeply rather than broadly.

Verified: 200 variant slots, all distinct; all 100 strategies tuned; 200 distinct
parameter points; zero collapses and zero duplicates. Tests updated to assert the
contract (every strategy tuned `VARIANTS_PER_STRATEGY` times, no strategy
repeating a point) rather than a magic total, and the mirroring test now asserts
`planner.ATTEMPT_COUNT == winner.MAX_ATTEMPTS` instead of pinning a literal —
pinning it is how a serialization bound came to read as a design constant.

**CORRECTION (2026-08-08).** An earlier entry here claimed commit `169f7e6`
"silently deleted" `max_bars`, `max_param_combos` and related keys from
`config.json` as collateral damage. **That was wrong.** Those keys are
`research_only_key`s: their removal from `config.json` is *deliberate and
test-enforced* by `test_config_json_integrity`, which asserts they are absent so
that production values are owned by `PipelineConfig` defaults rather than config.
The ownership model is sound.

The genuine defect was narrower: when `max_param_combos` moved to code ownership,
the code default (150) disagreed with the value `config.json` had carried (200),
so effective breadth fell by 25% — and **no test pinned the resolved value**, so
nothing caught it. Fixed 2026-08-08: default restored to 200 and pinned by
`test_research_only_defaults_pin_production_breadth`. The 128-attempt ceiling
above is a separate and far larger constraint, and remains open.

### E3 — Authority / evidence machinery `DECISION: pending`
101,050 lines, 46% of the engine. Static reachability: **87% of all modules are
reachable from `pm_main`** — this is entangled, not detachable. Fan-in ranks
`pm_authority_spool` (16) and `pm_book_multiplicity` (15) above `pm_strategies` (8).
**Cost to keep:** it is the dominant maintenance and runtime surface.
**Cost to discard:** unknown; requires per-subsystem reachability proof.

### E4 — Orphaned modules `DECISION: pending`
8 modules, 26,234 lines, reachable only from tests: `pm_native_lifecycle` (7,799),
`pm_state_runtime` (6,679), `pm_native_storage` (3,331), `pm_native_composite`
(3,092, zero non-test references), `pm_raw_winner_canary_conformance`,
`pm_native_continuity`, `pm_native_source_chain`, `pm_history_dependencies`.
Each has a test file, so the suite proves the correctness of code that never runs.
The ninth audited island, `pm_native_run_reseal` (2,139 lines plus its 133-line
CLI and 1,778-line/45-case test; 4,050 lines total), was retired under
XPM-064/CPM-059 after its two unique acquisition checks were migrated; see BI-12.

### E5 — Storage retention graph `DECISION: pending`
`pm_native_storage` holds `protect_tree` and the collectability graph. **Zero
references in shipping code.** Nothing prunes in production — the mechanical cause
of the 25 GiB `data/native` and the 15.5 GiB unpruned spool.
Either wire it in or delete it; leaving it is the worst of both.

### E14 — Look-ahead leak: searched, not found `DECISION: pending`
The operator reports the traded baseline was "a bit leaky and the backtester/WFV
had issues". Static comparison of both engines, 2026-08-08:

- `_backtest_loop_numba` — academic 372 lines, current 534. Both take
  `signal = sig_arr[i - 1]` and enter at `entry_prices[i]`. Causal boundary identical.
- Stop construction — both compute stops at `signal_bar_index = i - 1`
  (`atr_arr[signal_idx]` in current; `bar_index=signal_bar_index` in academic).
- `BaseStrategy.calculate_stops` is the **single** implementation all 100 strategies
  inherit. It reads `.iat[bar_index]` when indexed; the `.iloc[-1]` branch runs only
  when `bar_index is None`, i.e. the live path where the last bar *is* the current bar.
  The ATR fallback slices `features.iloc[:bar_index + 1]` first. Clean.
- No negative `.shift(-n)` anywhere in `pm_core`, `pm_strategies`, `pm_regime` — in
  **either** version. No bare full-series `.mean()/.std()/.quantile()/.rank()`; the
  only bare `.min()/.max()` sit inside `.rolling(...)` chains.

**Conclusion: no naive look-ahead found in either engine.** The reproduced failure
mechanism is *selection bias*, not leakage — FORD promoted on 16 validation trades at
PF 1.77, inverting to 0.66 over 166. That is an overfitting problem in the gates, not
a causality problem in the engine, and it is addressed by E6.

**WFV split — audited 2026-08-11, CLEAN.** This was the remaining place a leak
could hide, and the operator named it explicitly.

`build_calendar_boundaries` (`pm_evidence.py:299`) constructs windows strictly
backwards from the information cutoff with a purge at *both* seams:

```
lockbox_end   = as_of
lockbox_start = lockbox_end   - years(final_lockbox)
tuning_end    = lockbox_start - purge
tuning_start  = tuning_end    - years(tuning_validation)
train_end     = tuning_start  - purge
train_start   = train_end     - years(optimizer_train)
```

Negative embargo clamps to zero. Training is entirely *older* than tuning, so
training cannot see tuning data by construction.

The purge is `max(7 days, 5min × split_embargo_bars)` = **7 days, timeframe-
agnostic** (`derive_purge_duration`, `pm_evidence.py:187`). That looked like the
defect: the label horizon is timeframe-*dependent*, and 39 of 132 promoted regime
cells are H4 with 16 on D1, where a position could plausibly stay open longer than
seven days and inherit prices from the next window.

**It cannot.** `pm_core.py:1227` — after the bar loop, `if in_position:` force-
closes at `close_arr[n_bars - 1]` with spread applied and records the trade as
`EXIT_EOD`. **No trade spans a window seam.** Every label is bounded by its own
window, so a training outcome can never depend on validation-period prices, and
the purge does not need to cover holding periods at all.

Features at the start of the tuning window do look back through the purge into
the training window — that is not leakage, it is how live trading works: the
history exists at decision time.

Residual, minor, not leakage: force-closing at each window end exits the final
open position at an arbitrary price rather than its stop or target, a small
per-window distortion.

**Conclusion: no leak found in the split.** Together with the loop, stop
construction and feature scan above, the search for the operator's reported
leakiness is now exhausted in the current engine. The reproduced failure
mechanism remains selection bias on thin samples, not look-ahead.

### E6 — Promotion gate floors `DECISION: pending`
`historical_audit_min_profit_factor` defaults to **0.60** (`pm_core.py:1667`), unset
in config, read with a `0.60` fallback (`pm_pipeline.py:13261`). PF < 1.0 loses money.
Named in the June audit as the reason a net loser was promoted. Hardened gates *were*
added elsewhere (`probation_min_profit_factor` 1.0,
`production_portfolio_replay_min_profit_factor` 1.05, `fx_val_max_drawdown` 12.0).

**FIXED 2026-08-08** — the one element on this register acted on rather than logged,
because a gate pointed the wrong way is a live defect, not a design question:

| Setting | Was | Now |
|---|---:|---:|
| `historical_audit_min_profit_factor` | 0.60 | **1.10** |
| `historical_audit_min_trades` | 5 | **20** |
| `historical_audit_max_drawdown` | 35.0 | **25.0** |
| `max_param_combos` (see E2) | 150 | **200** |

Applied at `pm_core.py` (the `PipelineConfig` defaults, which *are* the production
values under the research_only ownership model) and at the `getattr` fallbacks in
`pm_pipeline._attach_historical_stress_audit`, where a hardcoded `0.60` would have
reinstated the same trap. Three new tests in `test_config_json_integrity` pin the
gates above breakeven, pin the sample/drawdown floors, and pin search breadth.

**What this does not do:** it blocks bad promotions; it does not find good ones. On
the June book these thresholds would have emptied the ledger — which the audit itself
called the correct outcome. Trading resumes only once the search is wide enough to
find something that clears them, i.e. after E2.

### E7 — Optuna config keys `DECISION: pending, PARTIALLY RE-FRAMED`

> **Re-framed 2026-08-13.** The provenance argument below is **unaffected and still
> decisive**: the seven keys are read by `build_artifact_meta` and compared by
> live-readiness, so removing them changes recorded provenance and breaks the
> contract against existing books. What is withdrawn is any *runtime* framing of
> these keys: BI-4 established that the Optuna path they describe is the legacy
> one, not the canonical executor, so they cannot be argued about on cost grounds.
> Decide this on provenance alone.

Seven keys are "functionally dead but contractually LIVE" — read by
`build_artifact_meta` and compared by live-readiness for artifact drift
(`docs/PROJECT_MILESTONES.md` M5). Removal was correctly dropped on 2026-07-21 when
a reachability proof failed. Removing them changes recorded provenance and breaks
the live-readiness contract against existing books.

### E8 — Academic runtime architecture `DECISION: pending`
Iteration 6 uses `pm_data_pipeline.py` rather than `pm_pipeline.py`, and ships
`GATE_CATALOG.md` and `RISK_STATE_MACHINE.md` — two design documents with no
equivalent in FXPM-Dev. **NEEDS OPERATOR CONTEXT:** was this a deliberate simpler
fork, or a frozen snapshot for publication?

### E9 — Milestone plan `DECISION: pending`
`docs/PROJECT_MILESTONES.md` M0–M10. M0–M4 (all provenance) complete; **M5 config
freeze never started**; M6 launched anyway and died 2026-07-31. No milestone before
M6 touches profitability. The plan itself put five infrastructure gates ahead of the
first money-making step.

---

### E10 — The traded baseline `DECISION: pending`
Operator (2026-08-08): the version that inspired the paper **traded a funded broker
challenge and came very close to completing it**, then failed; severe defects in it
began the refactor. It is the **only** FxPM iteration with real-money evidence.

The 2026-03-08 academic audit describes that system: a **seven-gate validation
pipeline**, **five-layer risk control with margin-state escalation**, regime-adaptive
architecture, Optuna Bayesian optimisation, 18 test modules. Its verdict: *"not a toy
prototype; a working decision engine with real operational history."*

**No successor has been shown to beat it.** Any candidate design should be measured
against this baseline, not against the current engine.
**NEEDS OPERATOR CONTEXT:** what were the severe defects, and do the broker
statements / trade records from that challenge still exist? Real fills, spreads and
slippage from a funded account outrank every backtest in this repository.

---

## 2a. Root cause — why the mass went where it went

The 2026-03-08 audit's Section 14 roadmap has four phases: **Research Packaging →
Data & Replication Control → Experiment Design → Governance Formalisation.** Its
Section 15 instructs, verbatim, that proceeding "must mean focusing on
**research-readiness, not further feature growth**", and that "additional strategies,
new indicators, or dashboard enhancements should be **deferred** until the research
infrastructure is in place."

Map the roadmap onto what was built:

| Audit deliverable | What it became |
|---|---|
| Phase 2 — "frozen benchmark dataset bundles with checksums and provenance metadata" | `data/native`, run manifests, `provenance.json`, activation manifests (M1) |
| Phase 3 — "experiment manifest schema capturing all parameters of every study run" | `generation-context.json`, canonical-json receipts, the authority spool |
| Phase 4 — "governance formalisation" | `pm_authority_*`, `pm_promotion_authorization`, the attestation layer |
| Phase 1 — "artifact release rules and version tagging" | `artifact_meta` and artifact-contract drift checks |

**The 101,050 lines of authority machinery are this roadmap, implemented.** The models
did not go rogue; they executed a plan faithfully. The plan was written to satisfy a
doctoral defence committee, and it was applied to the system whose job is to make money.
It also told them to defer feature growth — which is why search breadth collapsed to
128 attempts/cell while provenance grew.

**The distinction that was lost** is one the audit itself made, in Section 13:

> "Separation between **operational heuristics (fixed for production)** and
> **research variables (manipulated for study)**"

That separation was never built. Instead the research instrumentation was built *into*
the production engine. The cure is the audit's own recommendation applied properly:
the research artifact carries the provenance machinery for the dissertation; the
production engine sheds it and gets its search back.

---

### E11 — Known defects of the traded baseline `DECISION: pending`
Operator (2026-08-08): **lot sizing, recovery, order scaling** — "amongst others" —
but *"the real issue is that it was a bit leaky and the backtester/WFV had issues."*

Two very different classes:
- **Sizing/recovery/scaling** are localised to execution (`pm_position`, `pm_main`) —
  ~14% of the engine. Bounded and fixable.
- **Leakage in the backtester/WFV** invalidates evidence rather than execution. It is
  the class that makes a strong backtest fail live, which matches a system that came
  close to a funded challenge and then failed.

**Highest-priority open code question:** current `pm_core.py` is 473 KB against the
academic artifact's 148 KB, but the backtest loop may share lineage. **Was the leak
fixed, or inherited?** Nothing found so far answers this.

**Operator's caveat, to be honoured:** *"we made exceptions but I'd argue that the
exceptions made it better than what they detracted from."* The current engine is the
over-correction toward scientific purity. Some of the discarded pragmatic exceptions
may be exactly what made the traded version work. Do not treat purity as the goal.

### E12 — Forward-simulation window `DECISION: pending`
No broker records survive. Operator's proposal: forward-simulate from the academic
results' end date, since the academic artifact was substantially the traded system.

Measured, 2026-08-08:
- Academic analysis generated **2026-03-31** (`historical_results/analysis/export_audit.json`).
- Authoritative native data `production_native_v5r2_20260714` spans **2016-05-12 → 2026-07-14**.
- Out-of-sample window **2026-04-01 → 2026-07-14** = **21,334 M5 bars ≈ 74 trading days**.

This is the only honest forward test available anywhere in the project. Its second
output matters more than its first: the gap between the academic system's *reported*
backtest performance and its *forward* performance is a direct measurement of the leak.

### E18 — The forward window is not out-of-sample `DECISION: pending`
Verified 2026-08-11, and it invalidates the forward test as previously specified.

With `evidence_as_of_utc` empty, `as_of` derives from the data end (2026-07-14)
and `build_calendar_boundaries` yields:

| Partition | Window |
|---|---|
| `optimizer_train` | 2017-06-30 → 2022-06-30 |
| `tuning_validation` | 2022-07-07 → 2024-07-07 |
| `final_lockbox` | **2024-07-14 → 2026-07-14** |

The proposed forward window, **2026-04-01 → 2026-07-14**, sits *entirely inside*
`final_lockbox` — 104 days of overlap.

That matters because the lockbox is selection data, not merely unfitted data:
`pm_pipeline.py:12135` defines `"clean_holdout": (lockbox_start, lockbox_end)`,
and `classify_promotion_status` consumes those holdout metrics at `:2512-2520`
against `false_discovery_holdout_min_profit_factor` / `_min_return_pct` /
`_max_drawdown`, returning BLOCKED or PROMOTE_SMALL. **A promoted cell's status
is a function of its performance inside the window we would be "forward" testing
on.** The cells reaching the test are exactly those that did well in it.

**There is currently no untouched forward window.** Native data ends 2026-07-14
and every day of it lies in a fitting or selection partition. Two remedies, both
open: pin `evidence_as_of_utc` to 2026-04-01 so all partitions close before the
window (free, costs fit recency, moves the `evidence_as_of_utc == ""` assertion
in `test_config_json_integrity`), or acquire post-2026-07-14 broker data (costs
an acquisition run, keeps the fit recent).

### E13 — Data depth `DECISION: pending`
Native EURUSD M5 holds **758,012 bars**. The engine's window is **300,000** (~2.85
years) — **40% of what is on disk**. The prior engine used 500,000 (~4.75 years, 66%).
A third coverage dimension alongside attempts/cell and per-strategy tuning.

---

### E15 — Runtime: the binding constraint is RAM, not CPU `DECISION: pending, EVIDENCE SUPERSEDED`

> **Re-framed 2026-08-13. The heading's claim is wrong and must not be decided as
> written.** Codex's canonical bounded probe measured **1.07 GiB peak RSS** with
> **99.763% of wall time inside the native semantic resolver** — Backtester.run,
> stress replay and attempt builder were all zero. The binding constraint is
> neither RAM nor CPU: it is the **authority preflight**. The `optimization_max_workers`
> finding below still stands; the scale comparison against the 6-hour academic run
> inherits the withdrawn run structure of BI-4 and must not be reused.
> The live question is E26/E27, not this one.

Measured 2026-08-08 on the operator's machine.

**Hardware: 7.4 GB total RAM, 0.8 GB free, 6 logical cores.**

| Measurement | Value |
|---|---|
| 250k-bar M5 feature frame | **168 MB**, 70 columns |
| Framework baseline per worker (interpreter + pandas + `pm_core`) | ~118 MB |
| A symbol's full 6-timeframe feature set | ~270 MB |
| 6 workers × (baseline + one M5 frame + the `.copy()` on cache read) | **~2.7 GB minimum** |

Scale comparison against the version that ran in 6 hours:

| | Academic (6 h) | Current (3+ d, unfinished) |
|---|---|---|
| Symbols | 80 | 100 |
| `max_bars` | 500,000 | 300,000 |
| Per-cell search | 200 combos/strategy + Optuna | 128 attempts total |
| Workers | 4 | **1** during the sprint |

The current engine does a **narrower** search on **less** data and takes **>12×**
longer. Scope does not explain it.

**What is already recovered.** `optimization_max_workers` was 1 throughout the
sprint (committed 2026-07-12, still 1 at 2026-07-17). The working tree carried an
uncommitted `0`, which `resolve_worker_count` maps to all 6 cores; that landed in
`f891307`. The memory governor is purely worker-count bound — it does not throttle
on RAM — and `tqdm` progress reporting is present at `pm_pipeline.py:19802`. So
parallelization and the status bar both exist again.

**What is NOT safe to change blindly.** `feature_cache_max_entries` is `0` in
config, which `pm_core.py:1425` documents as *disabling in-memory caching* — every
feature read is a forced miss that falls through to disk. It looks like an obvious
win to raise it. It is not: at 168 MB per frame, even 2 entries × 6 workers is
~2 GB on a machine with 0.8 GB free. **Setting it to `0` was most likely a correct
response to memory pressure, and the 4→1 worker cut may have been too.**

**Consequences worth noting rather than acting on unilaterally:**
- `_cache_get` returns `.copy()` (`pm_core.py:5634`) — every hit duplicates a
  168 MB frame. Both a memory spike and a CPU cost.
- `compute_all` computes all 70 columns. It is called once per (symbol, timeframe),
  not per attempt — correct design — but the frame is wide.
- `compute_required` exists and is documented as the narrow path; the optimize path
  does not use it. Whether that is waste depends on how many of the 70 columns the
  100-strategy pool collectively needs.

**The honest conclusion: this laptop is the constraint.** 7.4 GB cannot hold 6
workers × 168 MB frames plus the evidence machinery's own footprint. The academic
version fit in that budget; the current one, with 70-column frames and the
attestation layer, does not. Any plan that assumes a 2-3 day completed run must
either shrink per-worker memory or accept fewer workers — and choosing between
those is an operator call about hardware, not a code change.

---

### E16 — Quantified efficiency defects `DECISION: pending, FIGURES STALE`

> **Re-framed 2026-08-13.** The feature-cache copy is recorded below at 51 ms;
> **re-measured on current HEAD it is 174 ms** (250,000 rows x 68 cols = 126.6 MB,
> post-categorical, `copy_on_write` off) — the register understates it 3.4x, and
> `_cache_get` copies on every hit while `_cache_put` copies again, so a
> miss-then-hit pays twice. **The derived "4.4 hours across 100 symbols" is
> withdrawn**: it multiplies by the run structure retired as BI-4. The defensible
> statement is *174 ms per cache hit, hit count per run unknown*.
> Recommended disposition (CPM-043): fold into E26 rather than open a lane, since
> array ownership after a cache read is the same question E26 must answer anyway.

All measured on the operator's machine, 2026-08-08. None of these trade accuracy.

**FIXED — regime labels as object columns.** `REGIME_RAW`/`REGIME`/`REGIME_LIVE`
held 250k interned Python strings per column. 44.0 MB → 6.4 MB; the 250k-bar M5
frame drops **168.2 MB → 130.4 MB (22%)**. Verified elementwise-identical against
HEAD across 250,000 bars; only difference is `REGIME_LIVE[0]`, the `shift(1)`
sentinel, `None` → `NaN`, which nothing reads. Landed `e999e36`.

**OPEN — full-frame copy on every cache hit** (`pm_core.py:5634`).
`_cache_get` returns `.copy()`. Measured: **51 ms per 129 MB frame**. At 128
attempts × 24 cells that is **157 s per symbol of pure copying**, ≈**4.4 hours
across 100 symbols**. pandas here is 2.2.3, which supports copy-on-write, making
a protective copy near-free. Requires either enabling CoW globally (a real
semantics change worth testing) or auditing consumers for mutation. Not changed
without the profile.

**OPEN — the run's hot path is not where it looks.** Timing three suites:

| Suite | Tests | Wall clock |
|---|---:|---:|
| `test_config_source_of_truth` | 41 | **13.5 s** |
| `test_backtester` | 14 | **8.4 s** |
| `test_profitability_readiness_blueprint` | 64 | **~3.5 hours** |

119 tests took 3:29:33 and essentially all of it is one suite of live-readiness
and supervisor tests. Ruled out by measurement: no subprocesses, and the 22.4 MB
winner ledger parses in 0.23 s. This suite is a *runnable* proxy for the
production hot path — unlike a 3-day production run — and profiling it is the
cheapest route to the real multiple. Profile in flight.

---

### E17 — The resolver re-verifies 12 GB on every call `DECISION: pending, LARGELY OVERTAKEN`

> **Re-framed 2026-08-13.** Measured 2026-08-08, before the resolver memo landed
> and before E26/E27 existed. The mechanism it describes is real and is now the
> subject of an agreed programme: E26 (extent cache with explicit detachment,
> agreed CPM-040) and E27 (move full semantic verification to activation/promotion,
> deferred until a code-version-bound receipt and forced selected-leaf verification
> exist). **Do not decide this element in isolation** — deciding it would either
> duplicate E26/E27 or contradict them.

**This is the largest efficiency defect found, and it violates a principle the
operator already recorded.**

Profiled 2026-08-08. `test_profitability_readiness_blueprint`: **six of 64 tests
consumed 12,236 of 12,241 seconds — 99.96% of a 3h24m suite.** The other 58 finish
in under a second combined. Five of the six land within 30s of each other at
~1,525s, which is the signature of a fixed cost, not computation.

Isolated outside pytest: `validate_live_readiness(cfg, symbols=[])` — an **empty**
symbol list — runs for **over 8 minutes** before being killed. Stack sampling:

```
validate_live_readiness                       pm_live_readiness.py:3601
  resolve_active_native_universe              pm_native_activation.py:6007
    _resolve_active_native_universe_uncached  pm_native_activation.py:5059
      _verify_activation_cell_evidence        pm_native_activation.py:3090
        _verify_v5_activation_cell_evidence   pm_native_activation.py:2627
          _v5_read_hashed_frame -> normalize_ohlc_frame
          audit_native_cross_resolution -> _coarse_interval_ends_ns
```

**Every readiness call re-reads, re-hashes and cross-resolution-audits the entire
active native universe: 12 GB across 567 `bars.csv` files.**

`resolve_active_native_universe` reuses a prior result *only* when the
`_PRODUCTION_NATIVE_AUTHORITY_SCOPE` ContextVar is set. Unscoped — as in every
test, and any caller that does not enter the scope — it falls straight through to
the uncached path and re-verifies all 12 GB.

This is precisely the boundary the operator recorded in memory as
`activation-vs-resolver-boundary`: *activation is a one-time semantic evaluation
over hashed axes; the resolver does cheap hash checks only, never per-call
semantic replay.* The activation manifest already carries a sha256 for every leaf
(5.4 MB, 1,202 references). The resolver should verify the manifest and trust
those recorded hashes, re-reading leaf bytes only when a leaf is actually
consumed. It currently replays the whole semantic verification instead.

**Open production question:** `validate_live_readiness` is called from
`pm_main.py:12254` and `:12415`. Scope logging appears at `:12357`, between them,
so the second call may be scoped and the first may not. If any readiness check on
the live path runs unscoped, live trading stalls ~25 minutes on it. Not yet traced
to a verdict.

**Two hypotheses this killed, recorded so they are not re-chased.** The uniform
~1,525s looked like an MT5 connect-retry loop — `mt5.initialize()` genuinely does
block 102.5s with no terminal — but the slow tests never touch MT5. It then looked
like unformatted datetime parsing, since stack samples kept landing in
`_array_strptime_with_fallback` — but parsing 60k `_BAR_CLOSE_TS` values takes
0.07s and `format="ISO8601"` buys 1.1×. The samples landed there because it is
called constantly, not because it is slow.

**Landed regardless:** `tests/conftest.py` now fast-fails `mt5.initialize` so no
test can ever wait 102s on an absent terminal. Semantically identical (a real
absent terminal also returns `False`), opt out with `@pytest.mark.real_mt5`.
62 connector, live-loop and regime tests pass with it.

**FIXED 2026-08-08 — `5e76111`.** `_resolve_unscoped_memoized` pins the result to
the authority identity (ACTIVE.json pointer bytes + manifest sha256), re-checked
on every hit.

| | Cold | Warm |
|---|---:|---:|
| `validate_live_readiness(symbols=[])` | 1711.6 s | **0.451 s** — 3798× |

Verdicts and error codes identical across cold and warm calls. Suite effect:
**3:29:33 → 0:53:42.** `test_live_readiness_rejects_each_partial_mt5_identity_pin`
went 4581.94 s → **1.44 s**.

Two cold verifications remain, and both are correct: the first readiness test
alphabetically pays one for `symbols=[]`, and `test_shipped_config_passes_funded_
breaker_contract` resolves the full 100-symbol universe, a genuinely different
key. Down from ~9. Seven tests fix the safety contract — changed authority forces
re-verification, invalid / sha-mismatched / mid-change universes are never pinned,
callers cannot mutate what is stored.

**Remaining, and now the floor:** the cold verification itself is ~1600 s for
12 GB — roughly 7.5 MB/s, so it is dominated by CSV parsing and per-leaf audit
work, not disk I/O. Making *that* cheaper is the architectural fix: verify the
manifest and trust its 1,202 recorded leaf hashes, reading leaf bytes only when a
leaf is consumed. That is the remainder of the activation-vs-resolver boundary and
it is what a production run pays on every cold start.

---

### E19 — Gate 1 answered; D1 is infeasible geometry `DECISION: closed`
Measured 2026-08-11 on the bounded EURUSD|D1 non-authorizing canary.

**PBO works.** `pbo=0.0`, N=224 evaluation-required, T=253 complete calendar
days, S=8/70 combinations, PASS in both base and cost-stress lanes. The
multiple-testing machinery produces real verdicts. The earlier claim that PBO
had never returned one was true only of the June book, which predates this
contract. **Closed — do not reopen.**

**No candidate authenticated, and the cause is geometry, not strictness.** Of
896 final 63-day lanes: 782 had zero trades, 114 had 1–2, pooled max 3 for the
best candidate — against Winner's 5-per-fold and 40-overall. Decisively,
recomputing at `effective_trials=1` (multiplicity removed entirely) still
demands ESS 42–481 against an observed 4.50–11: **all 17 surviving decisions
fail anyway.** Shrinking the roster or weakening Sidak cannot rescue D1.

Corroborated independently by arithmetic. Scaling D1's observed 2 trades/lane by
EURUSD source rows:

| TF | rows | ×D1 | pred trades/lane | clears 5/fold | clears 40 overall |
|---|---:|---:|---:|---|---|
| D1 | 4,524 | 1.0 | 2.0 | no | no |
| H4 | 26,790 | 5.9 | 11.8 | yes | yes (1.18× margin) |
| H1 | 106,697 | 23.6 | 47.2 | yes | yes (4.7× margin) |
| M30 | 151,399 | 33.5 | 66.9 | yes | yes |

D1's failure is predicted by bar count alone. **No gate may be moved toward the
observed result.** Excluding D1 from a full run would save only 0.34% of
row-work — it is the cheapest timeframe — so it is not an efficiency lever;
those cells simply return honest `NO_TRADE`.

### E20 — Execution defects, all three verified `DECISION: closed`
The operator named lot sizing, recovery and order scaling as defects in the
version that nearly passed a funded challenge. All three measured in the current
engine, 2026-08-11:

- **Lot sizing** — realised risk-at-stop matches the 1.00% target exactly across
  EURUSD, USDJPY, XAUUSD, BTCUSD, DE30, US100 at $1,000 and $10,000.
  `_round_volume_numba` floors to step ("risk-safe — never round up"); zero is
  caught as no-trade at five sites. Already pinned by
  `test_position_sizing_edge_cases`, including
  `test_min_lot_clamp_rejects_historical_live_overshoots`. No test added — that
  would be duplicate validation.
- **Recovery** — all 2,400 magic numbers generated for the real universe:
  **zero collisions** (birthday bound put the risk near 0.13%, so it needed
  measuring). `pm_main.py:4467` fails closed on any future collision.
- **Order scaling** — exact-cell uniqueness via
  `BLOCKED_SPECIALIST_CELL_ALREADY_OPEN`, and fail-closed
  `BLOCKED_POSITION_SNAPSHOT_UNAVAILABLE` rather than assuming no positions.

Side result for the $50 question: at $50 with 1% risk and a 20-pip stop, EURUSD
and USDJPY are correctly **rejected at zero lots**; only XAUUSD, BTCUSD, DE30 and
US100 size. A $50 account would trade only the volatile non-FX subset — a
concentration effect, not a sizing bug.

## 3. Questions only the operator can answer

1. **Iteration 4** — what was "FxPM 1.4 - Obtimized"? Only its orphaned data
   directory survived. Is its code worth locating?
2. **Iteration 6** — deliberate simpler fork, or publication snapshot?
3. **The 128-attempt ceiling** — was Winner V11's bound ever a deliberate accuracy
   trade, or did it emerge from the Horizon v1 depth × trial product?
4. **Which prior iteration traded best on live or demo?** No live-outcome evidence
   has been located in any repo. If it exists outside git, it is the single most
   valuable artifact for this reconciliation.
5. Which of the 7 Codex patches (iteration 8) were *your* calls versus model calls?

---

## 4. Update protocol

An element moves off `pending` only on an explicit operator decision, recorded here
with the date and the reason. No element is discarded on a model's judgement alone.
