# FXPM Project Milestones — Scope, Definition of Done, Progress

**Owner:** Bongo · **Executors:** Claude (review/verify/<20 min jobs) + Codex (engine/runs/>20 min jobs)
**Branch:** `fxpm-institutional-recovery-20260712` · **Freeze base:** `e6e8be4` (Checkpoint institutional recovery)
**Created:** 2026-07-19 (Claude, as PM, on operator instruction) · **Living document — update on every ticked milestone.**

> **Purpose.** This is the one dependable file that says *what the finished product is*, *which
> milestones remain*, and *how far along we are*. It exists to keep the program in scope and to
> stop the work drifting into review churn. Every milestone has explicit **exit criteria** — it is
> not ticked until those criteria are met **and** independently reviewed (Claude ⇄ Codex, evidence
> on the coordination ledger). Numbers here are anchored to commits, ledger sequences, and on-disk
> artifacts, never to optimism.

**Status legend:** `[x]` done + reviewed · `[~]` in progress · `[ ]` not started · `[-]` dropped/parked (reason given)

**Doc map (no duplication):**
- **This file** = program milestones, definition of done, scope guardrails, progress.
- `agent_comms/STATE.md` = one-page shared operational truth (updated frequently).
- `tools/agent_coordination.py` ledger = authoritative hash-chained event history (the evidence).
- `docs/PROGRAM_PLAN.md` = **historical** scoring-discovery workstream (CLOSED 2026-06-06; `legacy` scorer chosen). Kept for no-regression; not the current plan.

---

## 0. North star — what "done" means

**The finished product:** a production-ready FXPM engine that trades a ~100-symbol universe on
live markets, **consistently profitable** at the operator's calibrated medium-to-high risk
appetite (risk expressed as a calibrated ruin tolerance, never as weakened evidence), where every
traded specialist earns its place on its own honest per-cell evidence.

**Hard gates on "done" (non-negotiable):**
1. **Demo validation precedes any funded exposure.** No real capital until a demo shadow has
   confirmed the live path end-to-end.
2. **Funded trading stays disabled until evidence gates pass AND the operator explicitly approves.**
3. **No arbitrary thresholds** — every bar is pre-registered, evidence-based, or calibrated.
4. **Per-specialist verdicts, never aggregated** — no good cell offsets a bad one (see PROGRAM_PLAN §1).
5. **All memory/storage bounded, at zero accuracy cost.**
6. `fxpm_academic_research` repo is untouchable.

The program is **done** when M0–M10 are all `[x]` and the operator has signed off on funded exposure
under M10's evidence gate.

---

## 1. Milestone map (the whole journey)

| # | Milestone | Status | On critical path to the run? |
|---|-----------|--------|------------------------------|
| M0 | Institutional recovery + engine hardening | `[x]` | prerequisite (done) |
| M1 | Native history authority (V5) built + 600-cell audit | `[x]` | prerequisite (done) |
| M2 | Producer-wiring + terminal-cell promotion correctness (successor freezes) | `[x]` | done — all 6 artifacts accepted |
| M3 | Atomic promotion of the e3ce manifest to ACTIVE | `[x]` | **done + independently verified (2026-07-22)** |
| M4 | Raw-winner canary (bounded, non-authorizing) at 128 attempts | `[x]` | **gate closed 2026-07-26 (offline fix-verification, operator no-rerun)** |
| M5 | Pre-optimize hygiene config freeze | `[ ]` | YES |
| M6 | Full production optimization run (parallel, streamed) | `[~]` | **infrastructure BUILT + fully reviewed; ready for operator launch** |
| M7 | Synchronized replay + capital-envelope calibration | `[ ]` | YES |
| M8 | Evidence pack + operator review | `[ ]` | YES |
| M9 | Demo shadow validation | `[ ]` | gate before funded |
| M10 | Funded exposure — operator-approved, evidence-gated | `[ ]` | the end |

**WHERE WE ARE NOW (2026-07-28):** **inside M6.** Seven operator optimize runs have been attempted.
Every one failed inside the **historical-preplan** phase; **no run has yet reached the optimization
phase** (where winners are produced). Each failure was root-caused precisely and its *whole check class*
swept statically, so the same class is never rediscovered by another multi-hour run — that method has
twice caught blockers *before* a run hit them.

**Gate classes cleared and released this stretch (each ACCEPT-gated on the ledger):**
1. whole-run deadline removed (the 180-min ceiling) + memory-governor admission **livelock**;
2. spool-quota `os.replace` transient-lock (no retry → whole generation aborted);
3. **AU200 boundary** — short-history symbols wrongly demanded terminal-grade proof (30 cells admitted,
   107 terminal-prefix shapes still rejected);
4. spool **reservation count-cap** (stale max-5 vs 6 OS-managed workers);
5. **HK50** zero `trade_tick_value` → authenticated broker_specs resolution, tamper-bound;
6. **Platinum/Palladium** case-asymmetric identity check (caught by static sweep, pre-run);
7. **native-structural-authority** (JP225 digits / USDTHB path / TONUSD contract) — the *native*
   snapshot is authoritative; stale `broker_specs.json` (2026-06-07) yielded to it;
8. **broker-terms schedule repair** — 4 symbols had EMPTY terms records (`D1_NOT_EVIDENCE_CANDIDATE`);
   real hash-bound schedules were derived from the coarsest admissible intraday source and **promoted**
   (terms ACTIVE `66713019`), independently verified by a **418/418 live resolver sweep**;
9. **D1 max-context veto** — one extreme strategy's 3 882-bar context was applied cell-globally,
   vetoing all 128 attempts on **96 D1 cells**; it produced *zero winners that read as honest no-winners*
   (the most dangerous class hit so far, since it fails silently rather than loudly);
10. **planner/Horizon geometry parity** (`left_purge_bars`=31 bound to genuine Horizon geometry);
11. **shared fit-feasibility predicate** — planner and executor each had their *own* fold-adequacy
    formula and disagreed; `minimum_fold_fit_rows` is now **defined in the executor and imported by the
    planner**, so they cannot drift again.

**D1 coverage, honestly measured (post-fix):** selected context **545** bars; **104/128** attempts
admitted; of the 100 registry strategies **86 can search D1** (80 fully, 6 partially) and **14 are
genuinely infeasible** at D1's data depth and are typed as such. This is *representative* coverage, not
a short-context-biased subset — so the D1 book will be interpretable rather than silently skewed.

**Current blocker (open, needs an operator decision):** the reviewed canary ran ~48 min, cleared the
geometry and fit-window gates, then failed at final evidence with
`FINAL_BASE_STRESS_TRADE_COUNT_MISMATCH`. The evidence contract requires base and stress runs to have
**equal trade counts** (a *paired* cost-stress: same trades, worse fills) — but the live spread filter
(min-edge 1.5×, spike 2.0×) means a genuinely higher stress spread *should* sometimes suppress a
marginal entry, making the counts differ by real economics. Paired vs unpaired changes what
"stress-approved" means, so it is an operator decision, not an agent one (ledger seq2903).

**The canary saga (M4), all defect classes cleared + reviewed:** (1) three early execution blockers
(trade-power owner, spool phase-domain, regime-cutoff Z-vs-+00:00 format); (2) the **weekend-gap** class
— an equality that required the observed last bar to land exactly on a requested cutoff, false across
weekends; found and swept in **both** the executor (seq2503) and **all 5 planner predicates** (seq2512),
pre-empting an M6 current-refit failure keyed to the Sunday anchor `2026-06-07`; (3) the **feature-contract
completeness** class — an independent grid×feature sweep found the complete set of 8 missing feature
contracts (EMA 15/25/26/35/40/150, RSI 3/4) in one pass (seq2523), instead of one 43-min canary failure
per missing period; (4) the **boundary-terminal** current-window fix (seq2534); (5) the **realized-start
conflation** — a latent (base-e6e8) bug conflating historical discovery-start with cutoff-refit-start,
fixed across executor+planner (seq2512-era) and the adapter/Winner/Book successor (seq2589); (6) the
**cost-stress `NO_NUMERICAL_SHOCK`** correctness fix (seq2611) — a tight-spread cell (real spread below
the broker floor) genuinely produces no cost shock, now a typed honest abstention rather than a hard
failure, admitted only on hash-identical base/stress vectors with improvement/drift still fail-closed.
The gate was closed on offline preserved-root replay of the fix per the operator's no-second-long-canary
instruction.

**Parallel M6 build (11 lanes, all ACCEPTED + RELEASED 2026-07-26):** the whole optimize now runs both
the historical-anchor and current-refit halves **in parallel** (≤5 workers, 1 GiB reserve) under one
**hard 180-min absolute deadline** (created once after argparse, never reset), inside a **Windows Job
Object** supervisor (kill-on-close descendant containment; promotion reconciled only after the process
tree is proven empty); crash-safe (cross-process spool-quota with inode-dedup recovery; backward-compatible
bounded locks); and **provably deterministic** — both halves carry executed tests proving 5-worker parallel
output is byte-identical to serial, and the multi-cutoff single-scan efficiency preserves exact evidence
bytes. Config activated `optimization_max_workers 1→5`. Freeze-binding discipline during review caught a
stale pin hiding a real admission-race bug (a worker could launch just after the deadline) and surfaced
`manual_new_entries_enabled=true` (a demo-scope entry enable; funded/live fail-closed) for operator awareness.

**Next:** operator launches M6 `--optimize` (one command) → M7 replay/capital → M8 evidence → M9 demo →
M10 funded. Open operator items: confirm/keep `manual_new_entries_enabled=true`; once M6 yields winners,
Claude does the performance doc (§6/§7/§8) and the deferred evidence-led analyses (blanket-vs-per-design
feasibility, KAMA 3882-bar context coupling, the tight-spread no-shock characteristic, per-cell expiry decay).

**Session note (2026-07-26):** Codex completed and RELEASED every lane, then went idle (clean stop on a
RELEASE, not a silent death). If the two-agent cross-check stops producing ledger events for an extended
period mid-task, assume the process died and verify liveness rather than waiting.

---

## 2. Milestones in detail (exit criteria = what "ticked & reviewed" requires)

### M0 — Institutional recovery + engine hardening `[x]`
**Goal:** a coordinated, hardened engine base from which a production run can be launched safely.
**Ticked because:**
- Hash-chained coordination ledger (`tools/agent_coordination.py`) + agent protocol v1 in force.
- Authority machinery, canary prep, and hardened tests checkpointed at `e6e8be4`.
- Live-capital safety controls in place (kill-switch, ambiguous-send reconciliation, MT5 identity
  pinning, silent watchdog) — see PROGRAM_PLAN §5.2.
**Evidence:** commit `e6e8be4`; PROGRAM_PLAN §5; ledger to seq2316.
**Standing exit gate for the run (re-verified at M5→M6):** full test suite green (last recorded
`1639 passed / 5 skipped`, 2026-07-12; 190 test files present — **a fresh full-suite green is
required immediately before M6**).

### M1 — Native history authority (V5) built + 600-cell audit `[x]`
**Goal:** an authenticated per-cell native-history universe usable as the signal authority.
**Ticked because:**
- 600/600 terminal cells audited: `418 evidence_candidate` (346 consensus-PASS + 72 authenticated
  consensus-PENDING), `182 technical_error`, `0 pending`, `100/100` symbols carry ≥1 candidate.
- Manifest `data/native/activations/20260719T101758Z-e050a26f.json`, sha256 `e3ce0310…`,
  `status=failed` (awaiting promotion), `production_authoritative=true`, `native_is_signal_authority=true`,
  `silent_fallback_allowed=false`. Verified on disk 2026-07-19.
**Evidence:** manifest above; ledger seq2303–2304 (audit completion), seq2311/2318 (byte-identity held).

### M2 — Producer-wiring + terminal-cell promotion correctness `[~]`  ← **CURRENT**
**Goal:** the producer must treat an authenticated terminal cell correctly — a consensus-PENDING
cell is a valid candidate **iff** it passes `_has_only_authenticated_terminal_pending`; anything
else (unknown/unfinished/unauthenticated pending, source/provenance/integrity error) blocks globally.
**Exit criteria (ticked & reviewed when):**
1. Candidate derivation routes consensus-PENDING through the classifier — gate-true → candidate,
   gate-false/unknown → `NATIVE_PENDING` → global block. *(Working tree:
   [audit_activate_native_history.py:3274-3282](../tools/audit_activate_native_history.py#L3274-L3282).)*
2. Promotion precheck applies the **same** classifier over **rehashed** candidate-PENDING parity
   bytes, so "all 72 pass" is mechanically enforced, not trusted. *(Working tree:
   [audit_activate_native_history.py:5086-5091](../tools/audit_activate_native_history.py#L5086-L5091)
   and the technical-PENDING path at
   [:5277](../tools/audit_activate_native_history.py#L5277).)*
3. Two new canon tests: gate-true consensus-PENDING → candidate published; gate-false/unknown → global
   block + pointer unchanged. The existing fail+pending canon test stays **UNMODIFIED**.
4. Platinum/Palladium case-normalization verifier bug (ledger seq2318) resolved: exact sidecar
   identity preserved, canonical uppercase comparison applied to parity, mixed-case regression added.
5. Claude review at the **full-canon bar** — full `tests/test_native_consensus_activation.py`
   (not focused), tracing every exit to the candidate sink (trace-to-return discipline).
6. **Receipt-version-compatibility** (lane `native-receipt-version-compatibility-v1`): a frozen
   pre-change recomputation profile that verifies the e3ce legacy receipts **only** when they
   reproduce the stored bytes byte-exact **and** the legacy edge is no more permissive than current
   semantics (fail-closed; conservative subset). Pointer-only promoter replaces the manifest-overwrite.
   Claude policy conditions bound at seq2322 (mechanical enforcement, no receipt mutation, 47 M5 stay
   technical, full downstream validators pass, own hostile regression suite).
7. **Durable crash-seam recovery record** (bound by Claude at seq2329, six conditions): the in-process
   rollback loses the predecessor pointer BYTES if the process dies between the atomic swap and the
   postcheck (the receipt keeps only a SHA). Requires an fsync'd recovery record written BEFORE the
   swap, byte-exact restore proof, mandatory fail-closed re-entry (predecessor→resume swap;
   candidate→resume postcheck then success-or-exact-restore; any third pointer→fail without clobber),
   and the record cleared only after the full postcheck passes.
**Owner:** Codex implements; Claude reviews. **Blocking dependency for M3.**
**PROGRESS — 3 of 4 artifacts reviewed and ACCEPTED (2026-07-21):**
**Historical record (retired 2026-08-14):** the pointer-only terminal-promotion
publisher, recovery path, and promoted-view consumer described below are no longer
runnable authority. Their artifacts remain preserved only as provenance and as the
one migration predecessor for the receipt-backed canonical activation.
- `[x]` **Receipt-version-compatibility** — Claude ACCEPT seq2332 (pins byte-exact; 18 tests green;
  current-first + byte-exact + real per-window no-more-permissive ordering; frozen profile reverts
  BOTH receipt fields and deferral behaviour). COMPLETE seq2334, RELEASED seq2335.
- `[x]` **Promoted-view / anchor-plan consumer** — Claude ACCEPT seq2333 (pins byte-exact; exact
  two-element path whitelist; promoted path ADDS `validate_terminal_promotion_view` rather than
  relaxing anything; canonical path unchanged). COMPLETE seq2336, RELEASED seq2337.
- `[x]` **Pointer-only promoter + durable recovery record** — Claude ACCEPT seq2346 (pins byte-exact;
  30 tests green; independent precheck 0 errors on the real e3ce; no recovery marker; ACTIVE unchanged).
  All six seq2329 conditions verified in code: fsync'd record created inside the swap lock *before* the
  swap; byte-exact restore proof (bytes AND hash re-read); record can never act as authority; fail-closed
  three-way re-entry; record cleared only after the full postcheck, with an explicit
  `recovery_record_cleared` flag. Exceeded the bar with post-postcheck linearization and correct
  commit-point reasoning (a missing record + candidate pointer is post-commit → refuses a guessed rollback).
- `[~]` **Reference-resolution parity successor** (`native-reference-resolution-parity-successor-v1`,
  seq2340/2342) — **REMAINING BLOCKER.** Some cells legitimately store no `pair_report_sha256` because
  they terminalized before pair reports were produced; the runtime recomputes and compares against
  `None`, so the resolver/canary fails on valid cells (193 mismatching edges / 117 cells). Claude's
  seq2341 P0 correction: this is NOT technical-cell-only — **51 of the 418 searchable candidates** also
  carry absent digests, so a status- or shape-based exemption would either leave M4 blocked or silently
  exempt 51 tradeable cells. Rule is **reason-linked**, and per Codex's seq2343 strengthening (adopted
  at seq2344) the runtime must **independently re-derive** the reference resolution FAIL from the frozen
  source/sidecar chain — stored reason text is not authority. Present digests always verified; unexplained
  absence fails closed for candidates and technicals alike.
- Regression check: `tests/test_native_parity_activation.py` 57/57 green after the released compat
  change (same canon accepted at seq2307).
**M3 will not execute until the parity successor lands AND a FRESH FULL
`tests/test_native_consensus_activation.py` is green against the complete final state.** No partial or
superseded canon run will be cited as evidence.
**Evidence:** ledger seq2303–2337 (P0 caught seq2312/2313, rule reconciled seq2314–2317, verifier bug
seq2318, receipt-compat seq2320–2322/2328/2332/2335, crash seam seq2326/2329, anchor consumer
seq2323/2330/2333/2337).

### M3 — Atomic promotion of the e3ce manifest to ACTIVE `[ ]`
**Goal:** make the audited universe the live signal authority without reacquisition or replay.
**Exit criteria:**
- Receipt-bound atomic promotion: source manifest pinned by sha256 `e3ce0310…`, current last-good
  pointer (`20260711T145205Z-092ca527`, sha `abdd7f6c…`) retained on **any** verification failure.
- Promotion runs the full precheck (counts 418/182/0, 100/100 candidate-bearing symbols, classifier
  over rehashed parity, integrity-reason rejection) and only flips control status `failed→active`,
  bytes otherwise unchanged; atomic pointer swap; structure reopened.
- **No reacquisition / no reactivation.**
- Downstream consumers still fail-closed against the promoted universe (anchor-plan projection gate,
  portfolio-contract parity). Consumer-side hardening for the immutable promoted view is lane
  `native-promoted-view-consumer-v1` (seq2323): the anchor plan must accept the receipt-validated
  promoted-view geometry, retaining canonical-path acceptance + all pointer/sha/authority checks.
**Command (determined):** `python tools/audit_activate_native_history.py --promote-completed-activation
20260719T101758Z-e050a26f --run-id production_native_v5r2_20260714
--expected-activation-manifest-sha256 e3ce0310be580933f883d888099128aaa4d305edde2a400afd78c1b846d6870c`
**Ticked when:** ACTIVE.json points at the e3ce manifest and Claude confirms the transition receipt +
retained-on-failure behavior.
**DONE (2026-07-22), independently verified by Claude (seq2399):** ACTIVE.json (`7e4aedf8`) →
`activations/promoted/20260719T101758Z-e050a26f.json`; the promoted view differs from the immutable
source (`e3ce0310`, still `status=failed`) in **exactly one key** (`status` failed→active); counts
418 candidate / 182 technical / 0 pending / 100 symbols-with-candidates / 600 terminal; transition
receipt present (`data_reacquired=false`, `cell_evidence_replayed=false`, predecessor `1853b278`
retained); recovery record cleared. A first attempt (seq2387) **failed closed** on 13 H4 crypto cells
and rolled back cleanly to last-good — a writer/verifier status-parity defect (the runtime verifier
omitted the producer's unopposed-semantic-failure rule); Codex fixed it (`_v5_has_unopposed_semantic_failure`,
a conservative candidate→technical strengthening), full canon 60/60, and the retry committed.
**Process note:** the enabling fix's ledger acceptance (seq2395) was a one-word stub Claude did not
author (Claude was in an operator dialog); Claude closed the verification gap post-hoc at seq2399 —
the fix is sound, but the two-agent check must not be stubbed on safety-critical steps again.

### M4 — Raw-winner canary (bounded, non-authorizing) `[ ]`
**Goal:** prove the end-to-end authority chain on one cell and measure peak spool before the full run.
**Exit criteria:**
- One EURUSD|D1 cell through the released authority builders (resolver → anchor plan → generation
  context → frozen search → causal precommit → sealed request → executor → raw Winner → verify),
  at the pinned `BOUNDED_RAW_WINNER_CANARY_ATTEMPT_COUNT = 128`.
- Non-authorizing by contract: writes only to a diagnostic spool, no Book/causal/current/capital/
  pointer/order state created.
- Mechanical success (`RAW_WINNER_VERIFIED` / `…_ALL_PROCESS_REJECTED`, verifier accepted).
- **Measured** peak authority-spool footprint recorded (bounds the full run; `pm_storage` does not
  prune production authority roots).
- **BLOCKER (seq2347, lane `raw-winner-canary-resource-bound-v1`):** the canary performs **six identical
  full 100×6 raw authority verifications** before searching one cell (resolver guard → anchor plan →
  generation context → search → precommit → executor each re-verify independently). At 64–94 min/pass
  that is **6.4–9.4 hours to canary a single cell**, for zero added assurance — passes 2–6 verify
  byte-identical inputs and cannot reach a different verdict. Fix = verify once, then cheap exact
  identity revalidation, bound by Claude at seq2348 with five conditions: first full pass never skipped;
  process/run-local only (no cross-run cache); revalidation compares exact `ACTIVE.json` bytes + receipt-backed
  canonical manifest sha256; fail closed and loudly, never quiet; and it may never skip a consumer-specific gate
  (anchor projection, portfolio parity, resolver `production_contract_required`) that asks a different
  question. Claude corrected Codex's cost model at seq2348 (the frozen recompute is **lazy** and
  frame-reusing — ~1,107 not 1,828 pair computations, and CPU not I/O), which Codex accepted at seq2349.
- **OPEN QUESTION on the critical path:** does the same redundant full-universe re-verification tax the
  **M6 `--optimize`** path? One cell × six passes is annoying; 418 candidate cells × repeated full-grid
  verification would be run-wrecking. Codex is auditing the `--optimize` call graph (seq2349); if
  concrete, the same reuse is in scope for M6 rather than deferred.
**Command:** `python tools/run_raw_winner_canary.py --config config.json --symbol EURUSD --timeframe D1`
**Ticked when:** canary receipt is a mechanical success and the peak-spool number is captured.

### M5 — Pre-optimize hygiene config freeze `[ ]`
**Goal:** one reviewed config freeze that removes the legacy/live ambiguity before the run.
**Exit criteria:**
- `storage_observe_only = false` (routine lifecycle on; per ledger seq2298).
- ~~Remove the 7 dead Optuna/regime-tuner config keys~~ — **DROPPED 2026-07-21, reachability proof
  FAILED. Do not remove these keys before the run.** The 7 candidates
  (`regime_enable_hyperparam_tuning`, `regime_hyperparam_top_k`, `regime_hyperparam_max_combos`,
  `optuna_min_trials_per_strategy`, `optuna_max_trials_per_strategy`, `optuna_target_coverage_pct`,
  `optuna_family_size_aware_budget`) are **functionally dead but contractually LIVE**: they are read
  by `build_artifact_meta` (pm_pipeline.py:1995, the block at :2021-2038), which is called from the
  optimizer (pm_pipeline.py:14061) **and from live readiness** (pm_live_readiness.py:2107/2193/2503/3537)
  where `artifact_contract_matches` compares the current meta against each winner's **stored**
  `artifact_meta` for per-symbol drift. Because the config.json values differ from the code defaults
  (max_combos 200≠150, top_k 5≠3, min_trials 120≠50, max_trials 1500≠5000, family_size_aware true≠False),
  deleting the keys would make `getattr` fall back to different values, changing recorded run provenance
  and producing **artifact-contract drift** between the book this run creates and the live-readiness gate
  (an M9/M10 hazard). This is the operator's "reachability proof, not name" guard cutting the other way.
  Their removal moves to the **post-run** freeze, where config + `build_artifact_meta` + `pm_optuna`
  are removed coherently (with book regeneration or contract migration).
- **2026-08-13 supersession:** reachability review proved every alternative scoring branch dead and
  removed the mutable `regime_scoring_model` plus its nine variant-only controls. The surviving
  scorer is still the same `legacy` behavior; artifact metadata records a fixed literal `legacy`
  so old nonlegacy books fail closed and old legacy books remain auditable.
- This cleanup intentionally changes the config authority hash. Existing in-flight generation,
  spool, and activation receipts must be reissued; there is no compatibility fallback.
**Ticked when:** canonical scorer tests, artifact compatibility tests, and all active config
boundaries reject the retired keys.

### M6 — Full production optimization run (streamed) `[ ]`  ← operator's headline deliverable
**Goal:** the actual production optimization across the receipt-backed canonical universe.
**Exit criteria:**
- `--optimize` launched with the receipt-backed canonical V5 authority; streamed per-cell so peak
  memory stays bounded (loader-level streaming already satisfied — `_BoundedNativeSource`).
- Search budget = the reviewed **128** attempts/cell (the ≥300 successor is parked — see §4).
- Full evidence machinery runs and is honoured: DSR, PBO, bootstrap, multiplicity, two-phase lockbox,
  causal-evidence gate, portfolio-replay evidence gate.
- Run completes with a per-cell Winner book (honest NO_TRADE where no specialist carries).
**Ticked when:** the run finishes cleanly and the Winner book + evidence artifacts exist and validate.

### M7 — Synchronized replay + capital-envelope calibration `[ ]`
**Goal:** convert the Winner book into a risk-calibrated deployable book.
**Exit criteria:**
- Synchronized portfolio replay over the selected winners (per-specialist, no cross-cell aggregation
  for verdicts).
- Capital envelope calibrated to the operator's ruin tolerance (medium-to-high appetite, evidence
  never weakened to hit it).
- Promotion-authorization gate passes for each tradeable cell; unsafe cells abstain.
**Ticked when:** a risk-calibrated production book exists and its promotion gate is reviewed.

### M8 — Evidence pack + operator review `[ ]`
**Goal:** give the operator a tangible, readable evidence pack of the run and book.
**Exit criteria:**
- HTML evidence pack: per-cell outcomes, evidence-gate results (DSR/PBO/bootstrap/multiplicity),
  capital envelope, honest NO_TRADE accounting, run provenance.
- Operator reviews and decides go/no-go to demo.
**Ticked when:** the operator has the pack and records a demo decision.

### M9 — Demo shadow validation `[ ]`
**Goal:** prove the live path on a demo account before any real capital.
**Exit criteria:**
- Demo readiness gate (`pm_demo_readiness`) passes; demo shadow runs the live loop against the
  pinned demo identity.
- Live readiness (`pm_live_readiness`) confirms MT5 identity pinning, watchdog freshness, kill-switch.
- Demo results consistent with the run's pre-registered expectations (no live/backtest divergence
  beyond honest spread/cost bounds).
**Ticked when:** demo shadow is green and reviewed against pre-registered bars.

### M10 — Funded exposure (operator-approved, evidence-gated) `[ ]`  ← the end
**Goal:** consistently-profitable live funded trading at calibrated risk.
**Exit criteria:**
- All prior milestones `[x]`.
- Evidence gates satisfied on the demo-validated book; operator explicitly enables funded trading.
- Realized-loss kill-switches and the silent watchdog armed and verified live.
**Ticked when:** funded trading is live under operator approval with all safety controls verified.
**This is the definition of done for the program.**

---

## 3. Critical path & immediate next actions

```
M2 ✔  →  M3 ✔ (e3ce promoted)  →  [efficiency lane + e3ce terms overlay]  →  M4 canary@128
  →  ★ WINNER-PERFORMANCE DOC §5 (operator step 4)  →  M5 (hygiene freeze)  →  M6 (--optimize)
  →  DOC §6/§7 (backtest results + optimization analysis)  →  M7 (replay+capital)
  →  M8 (evidence pack)  →  M9 (demo)  →  M10 (funded)
   [we are here: INSIDE M6 — 7 runs attempted, all failed in preplan; 11 gate classes cleared;
    optimization phase not yet reached; blocked on the paired-vs-unpaired cost-stress decision]
```

**Operator re-sequencing (2026-07-22):** immediately after the canary (step 3), Claude fills §6 of
[`docs/WINNER_PERFORMANCE_AND_EXPECTATIONS.html`](WINNER_PERFORMANCE_AND_EXPECTATIONS.html) — the
full data-rich HTML chain documentation (per operator: not a high-level summary). §1–§5 are complete
now from live artifacts: the 11-stage chain, the full 600-cell promoted universe (100×6 status grid,
per-TF backtest data volumes, exclusion-reason taxonomy), split/data floors, every statistical gate
with live thresholds, and the prior book's **actual backtest results** (100 champions:
22 PROMOTED / 3 PROMOTE_SMALL / 48 BLOCKED / 27 NO_TRADE, with train+validation returns, PF, DD,
trades, win rate, mean-R, Sharpe≈, Calmar, edge-t per row — expired, evidence-only). §6 fills from
the canary receipt **before** M5/M6 proceed (operator's step 4); §7–§8 fill from M6 and feed M8.

**Next concrete actions (2026-07-28):** (1) **operator decision** — paired vs unpaired cost-stress
(seq2903); the equality check must not simply be relaxed, since that would compare *different trade
sets* under a single stress verdict; (2) regress the canary on whichever semantics is chosen and reach
`RAW_WINNER_VERIFIED`; (3) operator relaunches the canonical multi-day `--optimize` — it should now
clear the entire preplan and reach the **optimization phase for the first time**; (4) verify the sealed
receipt + winner book, then fill perf-doc §5.3/§6/§7/§8 with real per-cell winners using the
whole-window-aggregate + 3-fold walk-forward structure documented in §5.1.1; (5) DEMO forward test.

**Open non-blocking debts:** (a) **11 adapter tests are dead at setup** (fixture builds EURUSD while the
code resolves AUDUSD = `PINNED_SYMBOL_ORDER[0]`) — the dead set includes the **splice/tamper and
128-attempt integrity regressions**, so there is currently no *executing* proof that splice attacks fail
closed in the adapter (ledger seq2880; verified pre-existing, not patch-caused); (b) the sealed canary
receipt is **not persisted anywhere I can read**, so the 104/24 and 86/80/6/14 figures remain
Codex-computed and independently *reconciled* but not independently *recomputed* (seq2903); (c) the
supervised run's stdout is detached (`[Errno 9] Bad file descriptor`), so operators see hours of silence
— cosmetic but costly during multi-hour runs; (d) final production runs are to capture price history and
broker specs **fresh from MT5 in one atomic session** (operator direction seq2827), which structurally
eliminates the native-vs-broker divergence class that caused items 5 and 7 above.

---

## 4. Scope policy — what's in, what's parked

> **⭑ Standing charter — RUN-FIRST-THEN-PRUNE (operator, 2026-07-22).** The end-to-end model is built
> as production-ready as possible, but the operator explicitly accepts that **additional hardenings and
> selection parameters remain, to be discovered cooperatively and solidified into the *subsequent*
> leaner chain — not now.** The immediate priority is reaching **analytical results to analyze,
> quantify, and decide the pathway from.** After the first full M6 run shows how results take shape,
> run a **post-verification rationalization pass** that: (1) eliminates hardenings/selection gates the
> run proves non-beneficial; (2) revisits the per-cell **sidecar / metadata / volume of hashing** —
> scientifically justified *during* methodology+verification, a leanness target *after* it; (3) converts
> **every-run-searched parameters that need not be re-searched into find-once-fixed policies** (named
> example: **max capital allocation to open positions relative to active balance** — calibrate once,
> pin; there are "a myriad" of such fixed policies to set from the first run's performance).
> **Accuracy and quality remain the #1 constraint; leanness is second, pursued only post-verification.**
> Do NOT trim sidecars / cut hashing / remove search params now — that risks the in-flight run. Pairs
> with the parked ≥300 roster, dead-code deletion, and the efficiency findings (F1/F2 + bars/sidecar
> read audit). Ledger footnote seq2464; memory `post-verification-leanness-directive`.

**Lane admission filter (operator, 2026-07-19):** new/unrelated lanes are **NOT forbidden**. A lane
is in scope when it is **medium-to-high impact AND aligned with the core goal** — a highly profitable,
reliable live-trading codebase. Only **cosmetic / low-impact** work is deferred. Impact-and-alignment
is the test, not novelty. (This corrects the earlier "no new lanes except run-path blockers" framing.)

**In scope right now (correctness/reliability that protects the run):**
- Reproduced run-path blockers (e.g. the M2 producer P0, the Platinum/Palladium verifier bug seq2318,
  and the receipt-version-compatibility lane `native-receipt-version-compatibility-v1` seq2321).
- Any medium-to-high-impact correctness, evidence-integrity, live-capital-safety, or profitability
  issue that is reproduced and goal-aligned — open a lane, don't defer it.

**Parked by explicit operator SEQUENCING decision (not because low-impact):**
- `[-]` **≥300-attempt search roster** — designed and approved, **parked to the first post-run
  upgrade** (seq2294). Run now at the reviewed 128; this is a sequencing call, the design is sound.
- `[-]` **Dead legacy CODE deletion** (pm_optuna, stranded RegimeOptimizer branches, superseded tests) —
  **post-run** freeze (seq2299/2296); only the config *keys* ride M5. Pairs with the ≥300 lane.
- `[-]` **Re-audit / reacquire the 600-cell activation** — forbidden for this run; the e3ce manifest is
  byte-pinned and promoted as-is (seq2311/2320). See the M2/M3 materiality note (47 M5 cells).

**Deprioritized by separate directive:**
- `[-]` **Security/adversarial hardening** (forgery/splice/TOCTOU/DoS/crypto) — **not a priority**
  (single-laptop, no attacker). Statistical/correctness rigor is retained; security theater is dropped.

**Materiality decision — RESOLVED (operator, 2026-07-19): RUN INSTEAD OF RECOVERY.**
- Promoting e3ce as-is keeps ~47 M5 trailing-partial cells as `technical_error` even though the
  accepted boundary fix (seq2307) would make them candidates on a fresh audit — a bounded, conservative
  UNDER-inclusion (fewer searched cells, never a wrong inclusion). **Operator decision:** promote at
  418 candidates and run now; the 47-cell recovery is a **post-run** item (pairs with the re-audit /
  ≥300 work). Reaffirms no-reacquisition for this run. Recorded on the ledger.

---

## 5. Standing risk register (recurring failure modes)

| Risk | Guard |
|---|---|
| Legacy-looking knob steers a dead path (the seq2285 placebo) | M5 removes proven-dead keys; reachability proof, not name |
| Unresolved/unknown state promoted to ACTIVE (seq2207 class) | M2 classifier gate + rehashed-parity precheck; global block on gate-false |
| False global veto of valid sibling cells | Cell-local terminal semantics for authenticated conclusions (M2) |
| Verify-the-gate-not-the-return miss | Trace every exit to the concrete return/side-effect (recurring lesson) |
| Goalpost-moving | Pre-register acceptance bars before each run |
| Storage exhaustion mid-run | Bounded streaming + measured canary peak (M4) + staged cleanup |
| Test-weakening to pass a freeze | No assertion edits without sign-off; canon tests stay unmodified |
| Engine edits during a live run | Freeze the engine before M6/M9; no edits mid-run |

---

## 6. Update protocol (keeps this file dependable)

1. A milestone flips to `[x]` **only** after its exit criteria are met **and** independently reviewed
   (Claude ⇄ Codex, verdict on the ledger). Cite the commit / ledger seq / artifact as evidence.
2. When status changes, update the M-map table (§1), the "WHERE WE ARE NOW" line, and the milestone's
   detail block in the same edit.
3. New scope arrives as a §4 parked item first; it only becomes a milestone by explicit operator decision.
4. This file is the scope authority. If work isn't advancing a milestone or clearing a reproduced
   run-path blocker, it is out of scope until M6 launches.
