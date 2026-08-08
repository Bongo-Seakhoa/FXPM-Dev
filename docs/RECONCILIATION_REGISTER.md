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

### E3 — Authority / evidence machinery `DECISION: pending`
101,050 lines, 46% of the engine. Static reachability: **87% of all modules are
reachable from `pm_main`** — this is entangled, not detachable. Fan-in ranks
`pm_authority_spool` (16) and `pm_book_multiplicity` (15) above `pm_strategies` (8).
**Cost to keep:** it is the dominant maintenance and runtime surface.
**Cost to discard:** unknown; requires per-subsystem reachability proof.

### E4 — Orphaned modules `DECISION: pending`
9 modules, 28,373 lines, reachable only from tests: `pm_native_lifecycle` (7,799),
`pm_state_runtime` (6,679), `pm_native_storage` (3,331), `pm_native_composite`
(3,092, zero non-test references), `pm_native_run_reseal`, `pm_raw_winner_canary_conformance`,
`pm_native_continuity`, `pm_native_source_chain`, `pm_history_dependencies`.
Each has a test file, so the suite proves the correctness of code that never runs.

### E5 — Storage retention graph `DECISION: pending`
`pm_native_storage` holds `protect_tree` and the collectability graph. **Zero
references in shipping code.** Nothing prunes in production — the mechanical cause
of the 25 GiB `data/native` and the 15.5 GiB unpruned spool.
Either wire it in or delete it; leaving it is the worst of both.

### E6 — Promotion gate floors `DECISION: pending`
`historical_audit_min_profit_factor` defaults to **0.60** (`pm_core.py:1667`), unset
in config, read with a `0.60` fallback (`pm_pipeline.py:13261`). PF < 1.0 loses money.
Named in the June audit as the reason a net loser was promoted; still present.
Hardened gates *were* added elsewhere (`probation_min_profit_factor` 1.0,
`production_portfolio_replay_min_profit_factor` 1.05, `fx_val_max_drawdown` 12.0).

### E7 — Optuna config keys `DECISION: pending`
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

### E13 — Data depth `DECISION: pending`
Native EURUSD M5 holds **758,012 bars**. The engine's window is **300,000** (~2.85
years) — **40% of what is on disk**. The prior engine used 500,000 (~4.75 years, 66%).
A third coverage dimension alongside attempts/cell and per-strategy tuning.

---

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
