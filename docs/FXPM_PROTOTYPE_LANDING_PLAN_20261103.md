# FxPM prototype landing plan

**Date:** 2026-11-19, re-baselined. It replaces the work breakdown of 2026-11-03.
**Author:** claude-pm, third PM seat.
**Authority used:** HEAD `c85da1c5`; bus events through E6698; operator directions listed in the readiness audit, section 1.
**Status:** plan. `landed` means committed and pushed; `in flight` means running or under review now; `open` means not started. Nothing here claims a result the evidence does not show.

## 1. Goal and the test

The goal is justified confidence that the deployed book makes money in live markets. Every task answers one test: does it raise the probability that the book we deploy makes money, or keeps making money? If not, it is out of scope.

The deliverable is a working prototype: the complete path proven on one period of real cells, then the development set and the grid through it. Live operation is always one period. The 13-period walk-forward study runs once, at the end, to confirm or discredit the finished prototype.

## 2. Status by workstream

| Workstream | Status | Evidence |
|---|---|---|
| W1 Data base | M5 job-view consumer landed (`474e6539`). Backfill (W1.3), daily-like segment exclusion (W1.4) and EURZAR (W1.5) open; no landing found since 2026-11-03. | E4710 |
| W2 Cost readiness | Cost diagnostics landed (`1f5dfd3a`). Spread=min extension (W2.1) open. Next cost edition (W2.3) after the demo shadow. | E4541 |
| W3 Result and release | End-grid row and release landed and exercised once on the derived route (case 007). | E4848, E4853 |
| W4 One complete path | Derived route: case 007, one slot, NO_TRADE, on the earlier edition. Native route on this edition: Native315 in flight, EURUSD H1 TREND, one period, real models. One-period acceptance test on invented data PASSED. | E6573, E6668 |
| W5 Phase costs | The stage monitor landed (`3501a48a`), but its table was never produced. Whole-run profiles of the acceptance test replaced it and drove the redundancy removals. Budget: see the readiness audit, section 3. | E6382, E6435 |
| W6 Route A | Chain library (`1f5dfd3a`) and rank agreement (`425a30a9`, `ca697497`) landed. Full-panel edition V17 landed in `bc5c023e`. | E6517 |
| W7 Development set and walk-forward | The one-period launch route landed (`5e4f4fee`, `e1c44ffe`, `c85da1c5`). Development-set run open, after Native315. Walk-forward during development is one or two periods. | E6527, E6576 |
| W8 Full grid and landing | Open. The 13-period study (W8.4) runs once, after landing. | |
| W9 Scoring and qualification | Scoring review done (2026-11-03). Methodology review (2026-11-16): on case002, selection by history did not predict later results. Pilot design waits on a retained panel table. | `docs/FXPM_SCORING_AND_QUALIFICATION_REVIEW_20261103.md` |
| Strategy 101 | Source edition landed (`3c64600d`), not production-qualified: US100 M5 cost tier and recorded-clock proof needed first. | E6592, E6601 |

## 3. Order from here

1. **Native315 to its end** (W4). No timer; judged against its declared outputs. Source window closes 2026-11-21 08:15 UTC.
2. **Land the reviewed 316 and 317 batch**, after Native315 exits.
3. **Development set, one period, ten symbols** (W7.1), if Native315 passes; otherwise repair the named defect and rerun that one period.
4. **Close the open V17 cross-binding gap** on the current-refit route before any live one-period refit (readiness audit, finding 2).
5. **Throughput**: remove the per-request copy and hashes of unchanged prepared data (readiness audit, finding 3); then measure one period per cell against the grid budget.
6. **Strategy 101 qualification**: cost tier and clock proof.
7. **Data and cost leaves W1.3, W1.4, W2.1** for the symbols the development set touches.
8. **Grid and landing** (W8), then **the 13-period study once**, then the evidence pack, demo shadow and funded stages.

## 4. Working rules for this plan

- One period is the unit of every run, test and estimate. Two only for a hand-over check.
- No run is stopped by a clock. Expected outputs are written down before launch; a run is stopped only when they stop appearing.
- Verify once where data enters, then hand the verified value on. No repeated self-checks.
- Research batches: lane claim, lead and peer scope, build, one QA run, independent review, then landing. Per-file sha pins bind a freeze; no new receipt machinery. Landings and anything live need the operator's tier.
- Route each review question to one Claude seat, not two.

## 5. Risks

- **Real-data defects past the search.** Every full run this week found one defect after the search. Native315 is the first real run of that tail on this edition.
- **Throughput.** One cell's search on real models took about 126 minutes against a serial grid budget of about 17.
- **Method.** If selection by history does not predict later results, a working prototype still loses money; the development set must test this directly.
- **Old records.** Saved processes, bound fits and M5 source pins from before the landings need their original code or new issuance.
- **Machine.** One 7.36 GiB laptop; a restart killed a run on 2026-11-17. Check boot time before blaming a run that disappears.

## 6. Change log

- 2026-11-19: re-baselined to `c85da1c5`. The 2026-11-03 work breakdown, its leaf tables and its status notes through 2026-11-09 are superseded and removed; the previous text is `git show e52fa02e:docs/FXPM_PROTOTYPE_LANDING_PLAN_20261103.md`.
