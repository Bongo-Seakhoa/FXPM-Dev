# FxPM prototype readiness audit, pass 1

**Date:** 2026-11-03
**Author:** claude-pm
**Authority used:** checkout at `529c04f0`, native activation `20261028T221800Z-workshop-native-reverify-v3`, bus events through E4462.
**Status:** assessment and recommendation. It confers no authority. Plans and proposals are labelled as such; nothing here is a claim that code runs unless the evidence column says it ran.

## 1. What the operator asked, and what the bus shows

- **Goal.** A fully fleshed-out, working prototype. The 13 x 28-day replay is a later one-time validation that can confirm or discredit the prototype; it is not the current milestone.
- **Rule.** OPEN, USE, PROCESS, RESULT, DELETE. Permanent elements are code, canonical M5 history and the end grids. Everything else serves reaching the 2,400-case grid and is released after its last use (E4452). Free-disk floor is 5 GiB (E4451).
- **Did MAIN acknowledge the correction?** Yes. E4454 adopted E4451 and E4452 and retired the older holds. E4459 opened a source-only batch for the M5 on-demand consumer. E4460 asked claude-pm for a scope disposition on that batch's capsule; the disposition was posted today with four minimal corrections (`claude_scope_disposition_001.json`). The side-chat sequence of three steps (verify one path, process the roster, review and land) matches the bus direction and this audit's sequence.

## 2. The single processing path, stage by stage

Figures are from the active activation manifest counts and the run tree, tallied today.

| # | Stage | What exists, with evidence | Status | Gap |
|---|---|---|---|---|
| 1 | Canonical M5 source | `data/native/runs/production_native_v5r2_20260714/<SYMBOL>/<TF>/`, 13 GB. Active activation counts 600 cells: 465 evidence candidates, 135 technical errors. M5 is an evidence candidate for 78 symbols and a technical error for 22. | Exists, verified | 22 symbols have no usable M5. Their 132 cells hold 100 technical errors and 32 good native higher-timeframe cells. M5-only retention would discard those 32. Operator choice: keep native bars for those symbols, or declare them unavailable. |
| 2 | Requested timeframe view, derived on demand | Library published: `pm_explicit_derived_*`, `pm_explicit_observed_clock.py`, `pm_explicit_clock_build.py` (05973896, 3a711a1d, 4805af3e, 529c04f0). One data attempt published 8 cost-ready cells out of 35 derivable targets; 25 H4/D1 targets stopped on the clock rule, 2 exotic stops. | Library exists; consumer missing | The job-view consumer `pm_m5_job_view.py` is not created. Batch E4459 is open with Root's capsule; claude-pm scope disposition posted. Derivable gap: 35 cells (H4 24, H1 7, M30 2, D1 1, M15 1). |
| 3 | Features and regime labels | `FeatureComputer`, `RegimeStream` numeric kernel in `pm_regime.py`, train-only regime params per symbol and timeframe with provenance tests. | Exists, tested | None blocking. A derived view must run the same detector under the same provenance rule. |
| 4 | Cost tiers P75 and P95 | `data/native/terms/spread_profile.json`, frozen policy of 2026-10-13, per cell. For derived views the Spread=min method was verified on six controls and 8 cells are cost-ready under profile 9fe54. | Exists for native cells; partial for derived | Using the Spread=min method beyond the 8 verified cells is a cost policy ruling only the operator can make. Without it every other derived cell stops as COST_UNAVAILABLE. |
| 5 | Search, current process | Frozen 300-attempt roster per cell (`pm_production_search_planner.py`), authoritative executor. The full13 run reached NO_TRADE on 13 of 13 cells at the search stage, then failed at the account reader with exit 1. The fix landed (bd78c0d5, QA 20 of 20) but was never exercised on real data; the account-only re-entry was deferred and the full13 spool is now absent (E4468). The spool-copy consumer run passed; perf candidates landed (65729d04). | Search ran on real data; the account stage did not complete | Per-cell phase costs are unmeasured. The only full cell measured took 54,581 s including setup, a STOP and a cold reopen. |
| 6 | Search, Route A (accepted today, k = 5) | Cash-free labeller `pm_signal_opportunities.py` and the executor facade `describe_signal_opportunity_design` exist, descriptive only. Chain statistic, shortlist, per-regime studies sharing one cache: proposed (E4461). | Parts exist; edition missing | Route A is a new frozen process edition. Chain builder plus byte-equality test, then rank agreement on existing full-cell output, then the edition. |
| 7 | Account path and qualification | Kernel in `pm_core.py` (one position per cell, equity-path sizing, SL/TP/EOD exits), gates, winner selection, book cell family. Continuous account assessment landed. | Exists, tested; not yet completed on real cells | The account stage has not been exercised on real cells since the reader fix. It must run inside the one complete path. |
| 8 | Saved grid result | Results were written to the full13 working spool through the authority sink, and that spool is now absent; the book cell family defines the 2,400 cells. | Exists in spool form only, and the one real spool is gone | No compact end-grid row independent of spool paths. E4452 and capsule correction C4 require one: cell id, winner or NO_TRADE or STOP, configuration identity, source_contract_sha256, code sha, outcome metrics. |
| 9 | Delete, owned-transient release | Not implemented. Cleanup attempts were auto-review rejected; compression recovered little (E4456). | Missing | Release belongs inside the consumer lifecycle (capsule result-and-release rules). |
| 10 | Resource floor 5 GiB | Six prepared copies change only the constants (two builders 16 GiB to 5 GiB, scheduler reserve 1.5 GiB to 5 GiB) and add boundary tests at 5 GiB minus one, equal, plus one. | Prepared, not landed | Source review, then land. |
| 11 | Phase cost measurement | Not done (E4448 items A2 and A3). Budget arithmetic: 2,400 cells per 28 days allows about 17 minutes per cell serially. | Missing | Instrument one native cell and one derived cell: bar prep, features, regime, signals, kernel, spool I/O. |
| 12 | 13 x 28 replay | Anchor geometry computed (forward windows from 2021-03-29, 2022-05-04, 2023-06-09, 2024-07-14). | Not a current goal | Run once after the prototype works. |

## 3. Findings, ranked

1. **The native path has not been demonstrated end to end.** Corrected after E4469. The full13 run reached NO_TRADE at the search stage on thirteen real cells and then failed at the account stage; the fix landed but was never run on real data, and the spool is now absent, so nothing can be re-entered. The components exist and are tested, which is why the remaining work is narrow: the derived-view consumer, a compact end-grid row, the DELETE step, the Route A edition, cost measurement, and one fresh complete run that proves the chain.
2. **The binding decision is cost readiness for derived cells.** The derived library can build views for 35 cells, but only 8 can be costed. Extending the accepted Spread=min method to the rest is an operator ruling, requested today. **Ruled the same evening: yes, extend it**, by the same method as an edition extension verified per symbol control, typed STOP where the control fails; owner Root (W2.1).
3. **The M5-only premise holds for 78 of 100 symbols.** For the other 22, M5 itself is a technical error. Keeping their 32 good native cells or declaring them unavailable is an operator choice. Either answer is honest; silently dropping them is not.
4. **Throughput is the real constraint and it is unmeasured.** No reordering of the search should be sized before one native cell and one derived cell are timed by stage.
5. **Ceremony is slowing the research path.** The funded-path machinery (precommits, seals, cross-provider QA, 16 GiB floor) has been applied to research batches. Under the prototype-first ruling, MAIN should run research batches at T1 or T2 assurance: source review plus one QA run per batch, per-file sha pins only, and keep T3 ceremony for live promotion. This follows the proportional verification directive (E3987).
6. **Standing documents conflict with the ruling.** Section 1 of the 2026-07-12 timeframe design says not to infer higher timeframes from M5; E4452 supersedes it and the document should say so. `docs/ACTIVE_DISCOVERY_STATE.md` is already marked historical.

## 4. Recommendations and sequence

Owners: MAIN (codex-pm as Root) implements; claude-pm reviews; the operator rules at T3.

0. **Operator, now.** Rule on derived cost readiness (Spread=min beyond the 8 verified cells) and on the 22 symbols without usable M5.
1. **Land the 5 GiB copies** after source review. Small, T1.
2. **Build the consumer** (`pm_m5_job_view.py` plus dispatch into the five consumer paths named in the capsule) with corrections C1 to C4. Validation: one symbol and timeframe through coverage, acquire, consume, finish, close on a fixture; then one real derivable cell (an H4 target) through the existing slot, result saved, view released, free space never below 5 GiB.
3. **Measure phase costs** on that derived cell and one native cell. This decides whether Route A's 10 percent coverage target is affordable and where the one-traversal sharing pays.
4. **Define the end-grid row and the DELETE step**, then rerun full13 through the new path to prove RESULT then DELETE on real cells.
5. **Route A edition.** Chain builder with the byte-equality test against the kernel trade list; rank agreement of the chain statistic with the full13 account outcomes; then the process edition with the shortlist of 5 and per-regime studies sharing one cache; QA.
6. **Process the 2,400-case roster incrementally.** Available cells first; NO_TRADE, STOP and unavailable recorded honestly; intermediates released as each cell's row is saved.
7. **Later, once.** The 13 x 28 replay as the one-time validation.

## 5. The audit loop

This file is the living audit. Each pass appends a dated section with: stage status changes with evidence paths, defects found, recommendations, owner, and the next executable validation. Trigger: every landed batch and every operator ruling. A pass that changes nothing records that in one line.

## 6. Evidence paths

- `data/native/ACTIVE.json`, `data/native/activations/20261028T221800Z-workshop-native-reverify-v3.json` (counts, entries)
- `data/native/runs/production_native_v5r2_20260714/` (bars per symbol and timeframe)
- `data/native/terms/spread_profile.json`
- `pm_signal_opportunities.py`; `pm_authoritative_search_execution.py` lines 4519 to 4630 (facade)
- `pm_core.py` lines 208 to 211, 1846 to 1866
- `audit_reports/fxpm_workshop_20261007/declared_task_efficiency_continuation/m5_consumer_candidate_001/` (Root capsule, six copies, claude-pm disposition)
- `audit_reports/fxpm_workshop_20261007/declared_task_efficiency_continuation/operator_m5_on_demand_lifecycle_side_handoff_001.json` and `operator_route_a_shortlist_side_handoff_001.json`
- `docs/FXPM_PROTOTYPE_FLOW_ROUTE_A_20261103.html`
- Bus: E4448, E4451, E4452, E4454, E4455, E4456, E4457, E4459, E4460, E4461, E4462

## 7. Pass 1a addendum, 2026-11-03 afternoon

**Operator ruling (relayed to the bus).** Keep the native bars for the 22 symbols without usable M5. A one-time MetaTrader backfill of missing M5 is allowed where the broker can supply it; otherwise the missing period is treated like a weekend or holiday. Standard: the best we can have, not perfection. The cost ruling on Spread=min beyond the 8 verified derived cells is still open.

**Why the 135 technical-error cells failed.** Tallied from the activation reason codes, so the backfill option can be aimed.

| Class | Cells | Of which M5 | What it means | Recoverable by a same-server re-pull? |
|---|---:|---:|---|---|
| No bars or history gap | 33 | 7 | The terminal returned no usable bars for the request | Plausibly yes. Set max bars unlimited, restart, pull once, publish as a new source edition. M5 symbols: CHFJPY, Platinum, USDMXN, USDNOK, USDTHB, XAUAUD, XNGUSD. |
| Daily-like early years | 88 | 14 | The broker's history for 2014 to 2019 is one record per day at intraday requests; later years are real | No. Needs resolver segment exclusion: mark the daily-like years as an unavailable segment with the broker-depth explanation, keep the real years, apply the window minimums to what remains. Mostly indices and energy. |
| H4 cross-resolution consensus pending | 13 | 0 | Consensus between H4 and its reference timeframes was never completed | Not a download problem; finish or waive the consensus step for these cells. |
| Cross-resolution consensus failed | 1 | 1 | EURZAR M5 disagrees with M15/M30 | One bounded investigation. |

Within the 22 no-M5 symbols: 73 daily-like, 26 no-bars, 1 consensus-failed. Within the 35 derivable cells: 15 daily-like, 13 consensus-pending, 7 no-bars.

**Conditions on the one-time backfill, proposed for MAIN scope.** Same server as the existing run first; a different server only with its own provenance and the identity audit, never mixed into one series without a versioned change point. Output is a new activation edition with new source identity, not an edit of existing bars. It happens once, before the grid run. The cells it fails to recover are recorded as unavailable under the gap rule and the grid continues.

**Sequence change.** Step 0 gains a third item: the backfill pull for the 33 no-bars cells and the resolver segment-exclusion rule for the 88 daily-like cells, both owned by MAIN, both before the roster run.

**Correction after E4469 and E4468.** Root corrected finding 1: the full13 run was not an end-to-end account success, and its spool is now absent, so no re-entry is possible. The stage table and finding 1 above are corrected accordingly. Root also reports a first `pm_m5_job_view.py` draft (27,368 bytes, unrun, AST and compile only) with counterreview findings and a C1 to C5 delta in progress, and treats the claude-pm scope disposition E4466 as advisory until the Root-driven Claude session posts its own verdict on E4460.
