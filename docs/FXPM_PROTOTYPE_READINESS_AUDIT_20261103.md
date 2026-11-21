# FxPM prototype readiness audit

**Date:** 2026-11-21, pass 4. It replaces pass 3 of 2026-11-19.
**Author:** claude-pm, third PM seat.
**Baseline:** Product and test code at `4544e4f2a8` on `fxpm-institutional-recovery-20260712`. BUS through E7375 and the Source367 timing profile.
**Status:** assessment. It confers no authority. Nothing here claims a run result unless the evidence column says the run happened.

## 1. Standing directions and latest relay

| Direction | Date | What it means for the work |
|---|---|---|
| Production-ready FxPM | Standing | Close the engineering and evidence needed to put an independently reviewed Book into service. |
| Net profit is the research measure | 2026-11-21 | Report study outcomes at P75 and P95 under their declared cost model. Report modelling limits. |
| P75 is the minimum candidate criterion | E7368 relay | The candidate either wins the cell or it does not. The production scoring method and adoption evidence are open. |
| The trained grid is the live input | E7367 relay | A winning cell may trade and a cell without a winner stays flat. Live admission and the operator's mode choice still apply. |
| Options, not exclusions | 2026-11-21 | An idea, order handling included, is an option added to the pool with its own settings. The system chooses. |
| One period is the unit | 2026-11-17 | Tests use one period. The 13-period study runs once, at the end. |
| Run controls | 2026-11-17 | Predeclare expected outputs. Preserve the actual source window and fail-closed boundaries. Diagnose an actual failure before any permitted recovery. |
| Remove redundancy | 2026-11-17 | Reuse verified values within the operation that owns them and retain independent cold checks. |
| One-pass research direction | 2026-11-19 | The gated unit route is off. Its two-regime shared-ledger proof and the consumer decision remain open. |
| Independent signal evaluation | 2026-11-19 | V19 evaluates eligible signals independently. Account position rules are a separate path. |
| Hygiene while finishing | 2026-11-21 | Unneeded code and storage go as the work lands. |
| Local parallelism | E7367 relay | Four workers or occupied cores is the default. Sequence heavy native and research workloads. |

## 2. The path at HEAD

| Stage | State | Evidence |
|---|---|---|
| Evaluation edition V19 | Landed: independent per-signal R, one entry per bar, regime label at the closed signal bar. | `7dc40929`, `182bd5ffec` |
| Search to commit, one cell, invented data | **Ran end to end once.** EURUSD D1 TREND, one period: 300 attempt units, Raw completion, Winner decision NO_PROCESS_WINNER, Book marginal, cold-verified commit, run receipt. | Lane 366, E7312, E7318 |
| Defects found in the earlier proofs | Fixed and landed: admission counters, cold provenance, generation-specific Winner fields, witness ownership and shared-array graph uniqueness. | `18d40993`, `847328c3`, `62c6ad43`, `12c3e0c2` |
| Search to commit, real data | V19 terminal proof remains open. Native315 completed on an older edition. Source330 ended before V19 process/precommit publication. E7367 relays fresh engineering direction. Exact binding and peer scope are pending. | E6880, E6922, E7367, E7373 |
| One-pass unit route | Landed switched off: one ledger per symbol-timeframe unit, cohorts, a gated consumer. The timeframe roster is a test-only recipe. | `d2c0a5b0`, lane 341 |
| Grid across cells | The 2,400-cell Book family and V19 native-account fence remain open. Current native development requires Route A. | E7318; `pm_historical_anchor_orchestration.py:3887` |
| Live promotion | **Refuses.** The route ends at `_fail("RETIRED_COST_POLICY_INTERVAL_QUALIFICATION_REPLACEMENT_REQUIRED")`. | `pm_forward_oos_replay_producer.py` 5174, 5293 |
| Live entries | **Refused** on demo and funded with `interval_cell_qualification_policy_unadopted`. | `pm_live_readiness.py` 83, `pm_demo_readiness.py` 1263 |
| Strategy 101 | Source landed, not production-qualified. | E6592 |

## 3. Measured throughput

| Run | Image | Result | Time |
|---|---|---|---|
| Native315, real data, older edition | `c85da1c5` | exit 0, NO_TRADE | search about 126 min |
| One V19 cell, invented D1 data | `12c3e0c2` | complete, NO_TRADE | 1,034.5 s: attempts 162.0 s, Raw to commit start 472.0 s, cold commit 330.8 s |

The two post-attempt phases total 802.836 s for the synthetic case. Generation-flag reuse landed at `9078dd45` and has yet to be timed during a complete cell. There is no real V19 full-cell timing.

## 4. Findings, ranked

1. **The first native V19 case needs exact scope.** E7367 supplies the new direction. Bind the actual retained input, cutoff, cost/profile and source window and get Original Claude scope before issuing the case.
2. **V19 terminal custody is proven for one invented cell.** Native data, the full Book family and the V19 account fence need their own evidence.
3. **Post-attempt work dominated the measured cell.** Measure each remaining repeat in a bounded scope before changing or extrapolating it.
4. **Winner and live promotion are open.** The study does not establish adopted scoring or profitability. The promotion path still ends at the retired interval gate. Shared order governance exists, while V19 handling evidence and MT5 account/budget isolation need design and proof.
5. **Hygiene is part of delivery.** Portable test fixes landed. Check exact disposable files against live consumers and evidence holds before removal.

## 5. What needs the operator

1. Confirm the exact first native case's operator provenance through the named relay. E7367 already states the real one-period case and following ten-symbol development set. PMs bind cells and Codex runs locally.
2. Choose paper, demo or funded use when the architecture and admitted evidence are ready. Admission and account isolation must be implemented and reviewed first.
3. Adopt any change to production scoring, handling, hourly cost or early stopping on exact evidence. E7368 and E7370 give design direction. Approximate drawdown guidance does not define a runtime threshold.

## 6. Next, in order

The landing plan, section 3.

## 7. Change log

- 2026-11-21, pass 4: reconciled synthetic evidence, live source checks, scope relay and hygiene to `4544e4f2a8` and E7367–E7375. E6899 retains the earlier pass.
- 2026-11-19, pass 3: Native315 exited and the 25-file landing `fc3ef42a` closed pass 2's findings 2 and 3; rewritten to that state.
- 2026-11-19, pass 2: rewritten to `c85da1c5`; pass 1 text is `git show e52fa02e:docs/FXPM_PROTOTYPE_READINESS_AUDIT_20261103.md`.
