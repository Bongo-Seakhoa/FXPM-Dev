# FxPM prototype landing plan, re-baselined WBS

**Date:** 2026-11-03
**Author:** claude-pm, third PM seat (operator ruling E4472)
**Authority used:** operator rulings E4451, E4452, E4462, E4465, E4470, E4472; checkout `529c04f0`; activation `20261028T221800Z-workshop-native-reverify-v3`.
**Status:** plan. Labels: `in code` exists today; `directed` ruled or accepted, not built; `proposed` awaiting council disposition. Nothing here claims a run or a result.

## 1. Goal and the test

The goal is justified confidence that the deployed book makes money in live markets. Confidence comes from a process that can tell an edge from a lucky backtest and refuses to promote when it cannot. The PM charter's single test still decides every task: does this change the probability that the book we deploy makes money, or keeps making money? If the honest answer is no, it is out of scope.

The immediate deliverable is a **working prototype**: one complete processing path, proven on real cells, then the 2,400-case grid through it. The 13-period study runs once, after landing, to confirm or discredit the prototype. It is not a milestone now.

## 2. Where the July milestone map stands

| July milestone | Status now | Disposition |
|---|---|---|
| M0 to M3 recovery, native authority, producer wiring, promotion | done | stand |
| M4 raw-winner canary | semantic gate never closed; historical root unrecoverable | replaced by W4 below |
| M5 hygiene config freeze | not started | folded into W8 landing |
| M6 full production optimization run | infrastructure built; seven launches failed in preplan; full13 reached search NO_TRADE then failed at the account stage, spool now absent | replaced by W4, W7, W8 |
| M7 synchronized replay and capital envelope | not started | becomes the 1 to 3 period walk-forward in W7, then the one-time 13-period study after landing |
| M8 evidence pack, M9 demo shadow, M10 funded | not started | unchanged, after landing |

Standing rules that changed since July: M5 on-demand lifecycle OPEN, USE, PROCESS, RESULT, DELETE (E4452); permanent elements are code, canonical M5 and end grids; free-disk floor 5 GiB (E4451); 28-day cell cycle (E2628); Route A search with a shortlist of 5 (E4462, E4464); native bars kept for the 22 no-M5 symbols with a one-time MetaTrader backfill option (E4470).

## 3. Development scope

**Symbol set, maximum 10, pre-decided** (proposed, from activation coverage):

| Symbol | Class | Coverage today | Why it is in the set |
|---|---|---|---|
| EURUSD | FX major | all six native | clean baseline |
| GBPAUD | FX cross | all six native | second baseline; prior H1 canary evidence |
| USDJPY | FX major | H4 derivable from M5 | one derived cell on a liquid pair |
| USDZAR | FX exotic | H1, H4 derivable | high-spread cost-tier stress |
| USDNOK | FX exotic | M5 no bars, higher timeframes native | the one-time backfill path |
| XAUUSD | metal | H4 derivable | metal session calendar |
| XAGUSD | metal | M15, M30, H1, H4 derivable | strongest derived-view exercise |
| BTCUSD | crypto | H4 derivable | 24x7 calendar |
| US500 | index | D1 native only; intraday daily-like 2017 to 2019 | keep-native and segment-exclusion path |
| APPLE | US equity | all six native | regular-session calendar |

Every index and energy symbol has only D1 native with daily-like early intraday years; US500 represents that class and XTIUSD is the energy alternate. The set gives about 60 symbol-timeframe cells, 240 regime cells, one tenth of the grid.

**Rules.** Pre-cutoff data only, protected cutoff 2024-07-14T08:05Z. Walk-forward during development is 1 to 3 periods at 28-day boundaries from the existing anchor plan, used to prove the mechanism, never as selection evidence. Because design decisions will be made from these symbols, their results are design-contaminated for the later study and are weighed separately from the 90 untouched symbols.

## 4. Work breakdown

Each leaf has one owner, an acceptance, and an evidence path. Root is codex-pm's implementer; claude is claude-pm's implementer. T2 leaves get a peer PM scope ruling and a cross-provider review; T1 leaves get the lead PM only.

### W1 Data base, owner Root

| Leaf | Scope | Acceptance | Tier |
|---|---|---|---|
| W1.1 5 GiB floor | land the six prepared copies | tests pass; constants and boundary tests only | T1 |
| W1.2 M5 job-view consumer | `pm_m5_job_view.py` plus dispatch into the five consumer paths, corrections C1 to C4 (E4466) | fixture path test: coverage, acquire, consume, finish, close, release asserted | T2 |
| W1.3 One-time MT5 backfill | pull missing M5 for the 33 no-bars cells, new activation edition with provenance, same server first | cells move to evidence candidate or are recorded unavailable; no in-place edit | T2 |
| W1.4 Resolver segment exclusion | daily-like early years become an unavailable segment with the broker-depth explanation; later real years kept; window minimums applied to what remains | the 88 cells re-resolve with an explicit segment record; no invented bars | T2 |
| W1.5 EURZAR consensus | one bounded investigation | parked unless a dev-set symbol needs it | T1 |

### W2 Cost readiness, owner Root, operator ruling pending

| Leaf | Scope | Acceptance | Tier |
|---|---|---|---|
| W2.1 Spread=min edition extension (operator ruling 2026-11-03, "Yes, extend it") | extend profile 9fe54 by the same method to every derivable cell, verified per symbol control; no second method, no floor-only costing; owner Root with the M5 consumer | each derived cell either cost-ready with its control match recorded, or a typed COST_UNAVAILABLE stop; the 8 existing identities unchanged | T2 |
| W2.2 Cost lane presentation and diagnostics (operator ruling 2026-11-03, "A now") | keep percentile tiers and the frozen profile; present lanes as BASE (P75) and STRESS (P95) in human-facing surfaces, identifiers unchanged; `tools/spread_tier_diagnostics.py` computes per cell, pre-cutoff, mean and SD of bar spread, tier over mean, SD distance of each tier; the end-grid producer records the same two figures as metrics | diagnostics artifact for the development set; tests on a fixture; no re-key | T1 |
| W2.3 Next cost edition (operator ruling 2026-11-03, "B next") | BASE = per-trade observed entry and exit spread through the kernel `entry_spread_observations` hook, with an uplift calibrated in the M9 demo shadow against real fills; STRESS = P95 tier; both lanes required; never a P99 or 2SD required lane | calibration record from demo fills; byte-equal lanes on a fixture; cross-provider review; new process edition | T2, after W5 |

### W3 Result and release, owner claude

| Leaf | Scope | Acceptance | Tier |
|---|---|---|---|
| W3.1 End-grid row | one compact row per cell per cycle: cell identity, winner or NO_TRADE or STOP with reason, configuration identity, source kind and `source_contract_sha256`, code sha, outcome metrics, view key; written independently of working spools; built on the existing book cell family, not a parallel record | round-trip test; a row rebuilds its provenance from code and M5 identity; consumed by the consumer's finish step | T2 |
| W3.2 Release | owned-transient release after the row is durable, inside W1.2's lifecycle | spool and view absent after finish; row present | with W1.2 |

Status 2026-11-05: both leaves were exercised by case 007 on the derived route (Root E4848 and E4852; the Original seat's acceptance E4853): one v2 NO_TRADE row whose retained view contract digest matches it, then a recording-only retirement that archived 24,597 payload files; no payload or cold reopen remains. A bounded engineering result, one of thirteen slots, not a financial acceptance. Named gap for W3.1: a NO_TRADE row requires a real cost tier, which early native NO_TRADE slots cannot supply; a rule for that case is a separate decision. The writer gained an atomic same-symbol batch append for native finalization, reviewed in this lane (E4869) and landed with the W4.2 software packet as 39bc78c1 (E4884). Root's E4902 opened that decision as read-only feasibility with no tier NONE policy, so the row rule stands; E4903 found no historical tier recovery and proposed a prospective typed artifact for fresh REGIME_UNOBSERVED completions, under the Original seat's scope review (E4904).

### W4 One complete path, joint

| Leaf | Scope | Acceptance | Tier |
|---|---|---|---|
| W4.1 Fixture path | the whole chain on a fixture cell | all stages reached; row written; transients released | T1 |
| W4.2 One native cell | EURUSD H1 through search, account, row, release | real row, honest outcome, floor held, spool released | T2 |
| W4.3 One derived cell | XAGUSD H4 or USDJPY H4, same | as W4.2 with source kind DERIVED_FROM_M5 | T2 |

Status 2026-11-05: case 007 ran XAGUSD H1 TREND K1 (not the H4 cell named above) through source, fit, search, account, row and retirement in one run, exit 0, grid outcome NO_TRADE, spool released (Root E4848). The Original seat accepted it as a bounded W4.3 engineering result and Root adopted it (E4853, E4855); W4.2 on a native cell is the named next leaf, with its source-only finalization design and four semantics accepted (E4859, E4863) and no code yet. The W4.2 software packet (scheduler, spool, grid batch append and three tests) landed as 39bc78c1 after a 60-case QA accepted by the Original seat (E4879, E4881, E4884); the landing did not close W4.2, since positive native finalization on genuine producer records is unexercised. Root's feasibility read (E4886) found no eligible native reuse and a capacity block: the native opt-in binds 512 GiB spool caps with a 5 GiB floor against the last authorized 8 GiB caps and 16 GiB floor, and free disk is below that floor; a separate minimal limit-binding leaf is proposed and under the Original seat's scope review (E4887). The native limit binding landed as f49aca9f after a 48-case QA accepted by the Original seat (E4898, E4901): opt-in native now binds the authorized 8 GiB caps and 16 GiB floor, synthetic proof only; the capacity block and the genuine run scope stay open, with the early NO_TRADE tier follow-up in scope review (E4902 to E4904). The REGIME_UNOBSERVED witness landed as d4592c15 after a 41-case QA, two nodes re-run after a test-only fix (E4931, E4934); the Original seat adopted a fresh native FULL13 case on EURUSD H1 TREND and held launch on three fit gates, bytes, retirable file count and one window (E4938, E4939). Free disk rose above the 16 GiB floor after the operator's approved cleanup (E4930).

### W5 Phase costs, owner claude

| Leaf | Scope | Acceptance | Tier |
|---|---|---|---|
| W5.1 Stage timers | bar prep, features, regime, signals, kernel, spool I/O on the W4 cells | a measured table per stage, written to the living audit | T1 |
| W5.2 Budget decision | whether Route A's 10 percent coverage is affordable and where one-traversal sharing pays | a recorded decision with the numbers | council |

Input: docs/FXPM_EFFICIENCY_AUDIT_20261103.md (E4572, E4573). W5.1 timers instrument the sites in its sections 4 and 11 so that the ranked findings get real-data shares; its gate-once list (fresh fit at open, process identity at open, final-source check at close) is proposed as W6.3's hashing discipline, for the W6.3 design to adopt or counter; nothing in it is adopted by this pointer.

On 2026-11-09, W5.1 software landed as `3501a48a` (design C stage monitor at the governor child entry, opt-in by `FXPM_STAGE_TIMERS_DIR`, 98 top-level QA nodes plus 52 subtests; E4978, E4982, E4985). The measured table requires a separately authorized instrumented run. The native FULL13 path remains held on F1 to F3. Use exclusive seconds and same-stage edges to interpret the table. W5.2 waits on that table.

### W6 Route A, library by claude, edition by Root

| Leaf | Scope | Acceptance | Tier |
|---|---|---|---|
| W6.1 Chain builder | `pm_signal_chain.py`: per regime, non-overlapping chain from the opportunity tape; chain statistics; lower-bound total R | chain trade list byte-equal to the kernel trade list on a fixture | T2 |
| W6.2 Rank agreement | chain statistic versus account outcomes on the W4 and W7 real cells | agreement measured and recorded before any selector authority | T1 |
| W6.3 Process edition | per-regime studies sharing one evaluation cache keyed by source, config, cutoff, tier and params; shortlist 5 with family quota; account path on the 5; economic floor gate on mean net R per trade on the BASE lane at the operator's declared level (ruling 2026-11-03, candidate 0.25R); stated tie-break rule | new frozen process edition; planner and precommit re-keyed; QA | T2 |

### W7 Development set and walk-forward, joint

| Leaf | Scope | Acceptance | Tier |
|---|---|---|---|
| W7.1 Dev-set run | the 10 symbols through the complete path, available cells only | rows for every cell: winner, NO_TRADE, STOP or unavailable; intermediates released | T2 |
| W7.2 Walk-forward | 1 to 3 periods at 28-day boundaries | selections and account carry replayed honestly; mechanism proven | T2 |
| W7.3 Audit pass | append to `FXPM_PROTOTYPE_READINESS_AUDIT_20261103.md` after each landed batch | defects ruled blocker or deferred with owner | council |

### W8 Full grid and landing

| Leaf | Scope | Acceptance | Tier |
|---|---|---|---|
| W8.1 Roster | the 2,400-case grid incrementally, honest outcomes, floor held | end grid complete for available cells | T2 |
| W8.2 Review | coverage, calculation correctness, causal boundaries, cleanup | demonstrated defects fixed; one qualified full suite per frozen batch | T2 |
| W8.3 Land | commit and push the completed implementation | guarded push; publication receipt | T3 |
| W8.4 13-period study | once, after landing | confirms or discredits; then M8 evidence pack, M9 demo, M10 funded | T3 |

### W9 Scoring and qualification review, owner claude-pm

Added 2026-11-03 on the operator's side note: with one process run and no prefiltering, the per-cell score and qualification rule carry the whole decision, so they get a review of their own before the Route A edition is frozen.

| Leaf | Scope | Acceptance | Tier |
|---|---|---|---|
| W9.1 Scoring map | the authoritative path only: train ranking objective, tuning gates and their threshold sources, multiple-testing control, final lockbox rule, cross-family selector, NO_TRADE conditions, with file and line | a cited map, no speculation | T0 |
| W9.2 Operative controls | confirm the multiple-testing and precision controls that were inoperative in the June 2026 book are reached from the authoritative path today | each control named as operative, diagnostic-only or absent | T0 |
| W9.3 Route A consistency | the chain lower-bound statistic (selection) against the account gates (qualification): same cost tier, same trade definition, same window; what changes and what must not | a written delta with the exact rules | council |
| W9.4 Scoring assessment | the written verdict: sound, or the specific corrections required, with owners | delivered before W6.3 freezes; gates W6.3 | council |

Validation intermediaries that remain between a cell score and live money: one frozen tuning evaluation, the final lockbox verdict for the frozen winner, the demo shadow (M9) and the one-time 13-period study after landing. None of them substitutes for a sound score; a bad score reaches them with a good-looking record.

## 5. Order and dependencies

- W1.1 then W1.2; W3.1 runs in parallel with W1.2 against the agreed finish interface; W4.1 needs both.
- W4.2 needs W1.2 and W3.1; W4.3 also needs W2.1 or an accepted STOP.
- W5 runs on the W4 cells; W6.3 waits for W5.2.
- W6.1 is a pure library and starts now; W6.2 waits for real cells from W4 and W7.
- W6.3 also waits for W9.4: the scoring assessment gates freezing the edition.
- W1.3 and W1.4 run beside W4; they gate only the symbols they touch (USDNOK, US500 in the dev set).
- XAGUSD gaps, adopted state (E4594, E4600): the 283,827 missing M5 slots over the development interval remain UNCLASSIFIED in the worker's record, with the observed pattern classes from record xagusd_h1_gap_classification_001 (fixed weekly positions, full-day absences, 60 to 65 minute weekday runs, 587 single slots) carried beside that status as observations only. No calendar, holiday, no-tick, eligibility or no-source-pull rule is adopted, and the record's scripts are archived heuristics with no execution grant (see its qualification sidecar). The one-time broker history verification of the 587 single slots and 237 longer runs is a W1.3 item for when an authorized non-interactive route exists. The development period runs available-data-only under the admission record f2a6ffdf.
- W7 needs W4 and W3; W8 needs W7.

## 6. Ceremony profile for this plan

Research batches run at T1 or T2: lane claim, lead PM scope, peer PM scope at T2, build, one QA run per batch, cross-provider review at T2. Per-file sha pins bind a freeze; no new receipt or hash machinery. T3 is for landing, authority and anything live. The debate budget is three rounds, then one framed decision to the operator. When providers disagree on fact, run the thing.

Council requests: route each scope question to one claude seat, not two; record degraded assurance when a peer is unavailable rather than waiting; ask the operator only with one framed decision.

## 7. Immediate next actions

1. Operator: rule on W2.1 (Spread=min beyond the 8 cells).
2. Root: land W1.1; continue W1.2 with C1 to C4; name the finish interface so W3.1 can target it.
3. claude: claim W3.1 and W6.1; post the lane scopes for codex-pm's peer ruling.
4. Root: scope W1.3 and W1.4 as T2 leaves with the dev-set symbols as their first targets.
5. Council: adopt the development symbol set or amend it, in one decision.
6. claude-pm: W9 scoring map and assessment, started 2026-11-03.

## 8. Risks

- Cost ruling delayed: W4.3 and most derived cells stop. Mitigation: proceed on native cells.
- MetaTrader availability for W1.3: needs the terminal and account that produced the native run.
- Throughput: unmeasured; the one measured cell took over fifteen hours with setup. W5 decides the search budget.
- Two Claude channels: Root drives a managed Claude session by pointers while this seat works in the side chat. Duplicate reviews waste effort; council should route each question once.
- Disk. E4944 reported 85.73 GB free after the operator-approved cleanup. Use the 5 GiB floor where that scope specifies it. The prospective native FULL13 run keeps its separate 16 GiB floor (E4939).
