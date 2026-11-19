# FxPM prototype readiness audit

**Date:** 2026-11-19, pass 2. It replaces pass 1 (2026-11-03) and its addenda through 2026-11-09.
**Author:** claude-pm, third PM seat.
**Authority used:** HEAD `c85da1c5` on `fxpm-institutional-recovery-20260712` (pushed); bus events through E6698; run metadata only (file counts and times) for the active real-data run.
**Status:** assessment. It confers no authority. Nothing here claims a run result unless the evidence column says the run happened.

## 1. Standing operator directions

| Direction | Date | What it means for the work |
|---|---|---|
| The prototype must land and make money | standing | Every task is judged by whether it raises the chance the deployed book makes money. |
| One period is the unit | 2026-11-17 | Tests use one period, two only where the hand-over between periods is under test. Live always runs one period. The 13-period study runs once, at the end. |
| No time limits on runs | 2026-11-17 | Write down the outputs a run should produce before it starts. Stop it only when an expected output does not appear and nothing else is being written. Otherwise it runs to completion. |
| Remove redundancy everywhere | 2026-11-17 | Verify a thing once, where it enters, then hand the verified value on. Now rule 23 and ruling 30 of the shared OmniMind contract (E6698). |
| Parallel work and visibility | 2026-11-19 | Third PM relay E6700: a search taking a week is acceptable. Prioritise useful parallel independent streams and visible current work. Cloud workspaces and Colab are options; paid execution still needs a concrete shard, hours and cost plan before spending authority. |
| Prepare once, then slice | 2026-11-18 | Suggestion: transform a symbol and timeframe's data once, let later steps subdivide it, free it when the timeframe or symbol is done. Bar-by-bar loading belongs to forward and live simulation only. |
| Stale information is removed | 2026-11-16 | Superseded text is deleted; a change-log line stays. This pass applies it to itself. |

## 2. The processing path at HEAD

| Stage | State | Evidence |
|---|---|---|
| Native source | Activation v3 unchanged. Its semantic receipt pins seven verifier files, so `pm_native_activation.py` was kept at `e09a4935`; the speed-ups that first edited it were moved into the callers. | E6398, E6399, E6406 |
| Search, Route A full-panel edition (V17), 100 strategies, 300 attempts | Landed in `bc5c023e`. Ran on real data for the first time on 2026-11-19: 300 attempt records for EURUSD H1 TREND, one period. | E6668; unit records 08:17 to 10:23 UTC |
| Result records and winner decision | Repaired in `bc5c023e`: V17 policy version, one evaluation instead of three, V17 restore. Proven end to end on invented data. Real-data proof is in progress. | E6501, E6502, E6573 |
| Cold re-check and negative-origin checks | Passed in the landed one-period acceptance test. | E6573 |
| Account, end-grid row, fence, retirement | Exercised once on the derived route (case 007, 2026-11-05, earlier edition). Not yet on the native route of this edition; the active run is the first. | E4848, E4853 |
| One-period launch | `pm_main.py declared-rnd --contract … --fresh-contract`. Default is the first period; two periods is an explicit hand-over; 13 needs `final_study_13: true`. One- and two-period runs are limited to the ten development symbols; every other symbol is reached only by the final study. | `5e4f4fee`, `e1c44ffe`, `c85da1c5`; 92 and 55 targeted cases |
| STOP control and its 15 dependent checks | Passed, with the STOP record retained. | E6476, E6480 |
| Strategy 101, noise-band breakout | Source edition landed in `3c64600d`. Not production-qualified: it needs a US100 M5 cost tier and a recorded-clock and DST proof. Its defaults are stop 2 and target 2 ATR; the study found its edge at targets of 3 ATR and above. | E6592; `noise_band_breakout_study/INTEGRATION_PACKET_101.md` |

## 3. Measured throughput

One period of one cell. Invented zero-signal models unless the row says otherwise.

| Run | Image | Result | Time |
|---|---|---|---|
| Before the redundancy removals | lean004 | stopped by the operator, never finished | about 15 hours (E6292, E6297) |
| q293owner | cur293 | all 300 staged; failed at exit on a transport guard | 300 staged at 146.9 min |
| q301owner | cur301 | failed in result records on an aliased list | 300 staged by 56.4 min; 77.7 min to exit |
| q303owner | cur303 | failed winner verification (V17 policy version) | 55.6 min to exit |
| Landed acceptance test | `bc5c023e` | **PASS** | 7,021 s of pytest for the whole chain (E6573) |
| Native315, real data and real models | `c85da1c5` | search complete; result stage running | search about 126 min (08:17 to 10:23 UTC) |

The landed test's 7,021 s includes the result and cold-check stages the earlier runs never reached, and the machine was shared with other runs; it is not comparable to the search-only figures above it.

Where the finished q293 run spent its time (39,167 samples; the rows overlap): time-axis reconversion on every slice 29.6%, partly cut in lane 300; roster rebinding 29.0%, cut in lanes 297 and 298; the three per-request working-copy hashes 22.9%, still open; result records 13.3%, cut from three evaluations to one; the strategy runs themselves 6.9%; whole-state rechecks 0.1%, removed.

Historical serial arithmetic: 2,400 cells in 28 days would allow about 17 minutes per cell on one worker; Native315's search took about 126 minutes. This is neither an admission gate nor a completion threshold. The newer operator direction relayed in E6700 accepts a week-long search and prioritises useful parallel independent streams.

## 4. Findings, ranked

1. **The machinery is proven on invented data, not yet on real data.** Native315 is the first real cell on this edition. Until it exits, nothing after the search is proven with real models.
2. **Every full-run failure this week was the same class: code that handled V16 but not V17.** The transport guard, the restore map, the decision verifier's policy version and the M5 cross-binding were all fixed. One known member is still open off the current path: `_build_current_cross_binding_bytes` in `pm_authoritative_search_execution.py` (4938 and 4955 at HEAD) has V16-only schema branches. A V17 current-refit is rejected by the historical-schema expectation before the output is emitted; the current-schema selection is also wrong. Both need the existing generation-aware mapping (E6499). This remains open for the live one-period refit path.
3. **Parallel independent streams remain to be deployed.** Native315 uses one worker. The next batch should use the existing scheduler's independent work units, with observed worker memory informing concurrency; a portable shard-bundle design is source-only work under E6717. The largest identified redundant-work cut remains the per-request deep copy and triple hash of unchanged prepared data (22.9%). The proposed cut uses read-only prepared arrays with a shallow copy per request to retain refusal of actual input mutation; correctness and the timing benefit remain unmeasured (E6447, parked until Native315 exits). It is also the operator's prepare-once suggestion.
4. **Method risk is unresolved.** On case002, choosing strategies by their history did not predict their later results (methodology review, 2026-11-16). Native315 is the first new real evidence. A pilot design waits for a retained 300 by 24 panel table that the current edition does not keep.
5. **Compatibility is broken by design for old records.** Processes and bound fits saved before lane 288 refuse under the landed executor. M5 source pins issued against earlier `pm_forward_oos_replay_producer.py` or `pm_authority_spool.py` bytes need their original code or a new authorised issuance before a real M5-derived run. Both are stated in the `bc5c023e` message.
6. **Not re-checked in this pass:** the one-time MetaTrader backfill, daily-like segment exclusion and the Spread=min cost extension (landing plan W1.3, W1.4, W2.1). No landing for them was found in the log since 2026-11-03.

## 5. Next, in order

1. Native315 runs to its end, judged against its declared outputs, with no timer. Its source window closes 2026-11-21 08:15 UTC. Root reads the outcome at exit.
2. Land the reviewed 316 and 317 batch (fresh ordinary-family V17 routing, dead quota accounting removed) after Native315 exits.
3. After Native315 passes and the next execution scope is recorded, use useful parallel independent streams for the one-period ten-symbol development set. On failure, repair the named defect and obtain the next scoped execution disposition. E6700 is not an automatic retry, source-window renewal or paid-run grant.
4. Close finding 2 before any live one-period refit.
5. Strategy 101: US100 M5 cost tier and clock proof, then qualification.
6. Cut the per-request copy and hashes (finding 3) the next time the search code is touched.
7. Once, at the end: the 13-period walk-forward study.

## 6. Change log

- 2026-11-19, pass 2: rewritten to the state at `c85da1c5`. Pass 1 of 2026-11-03 and its addenda through 2026-11-09 are superseded and removed; the previous text is `git show e52fa02e:docs/FXPM_PROTOTYPE_READINESS_AUDIT_20261103.md`.
