# FXPM Program State

Updated: 2026-08-14 (Codex/Claude PM agreement). One page. Rewritten in place, never appended.

## The only objective

**A live-trading system the operator can be as certain as possible is profitable
and reliable with real money.** Every task, verification, model and artifact is
justified by that sentence or it does not get built.

Test for any proposed work: *does this change the probability that the book we
deploy makes money, or that it keeps making money?* If the honest answer is no,
it is not in scope — however rigorous, provable or elegant it is.

## Operator directives now in force (2026-08-11)

1. **The ballooning is a defect, not a side effect.** 218,389 engine lines,
   136,703 test lines, 4,234 collected tests. Clean up after yourself; delete
   fluff. Not a hard rule — analysis needed to reach the goal is legitimate —
   but the default is subtract, not add.
2. **Stop hashing one pipeline seven ways.** If a single datum flows through a
   single pipeline with no reasonable prospect of corruption, carry it to the
   end. Validate hard truths; drop ceremony. The RUN-FIRST-THEN-PRUNE charter of
   2026-07-22 is superseded: prune as we go.
3. **PM-governed integration.** Both PMs may approve, reject, rescope, replace
   or discard work inside `PM_CHARTER.md`. They are equals; explicit peer
   evidence is required. Only a three-round deadlock goes to the operator.

## Where the system actually is

- **Nothing is tradeable.** Book optimised 2026-06-09/11, `valid_until`
  2026-07-05, expired on 2026-07-05. No run has completed since.
- **The last book was noise.** All 157 promoted rows: `pbo_status` =
  `not_available`, median `val_sharpe` 38.23 (max 208.97), deflated Sharpe
  saturated at 1.000 for 144/157, median 21 validation trades with 75 rows under
  20, PF 3.08 at 1.08% drawdown. Both multiple-testing defences were inoperative.
- **Three gates shipped switched off** and are now on: profit-factor floor
  (0.60 → 1.10), implausible-Sharpe-without-PBO (0.0 → 4.0), sample and
  drawdown floors. Against the checked-in book these reject **all 157 rows** —
  the correct outcome.
- **The search can now find something.** 128 → 300 attempts; every strategy
  tuned twice instead of 28 of 100 tuned once.
- **Runtime is now measured at the actual bottleneck.** The exact 100×6 native
  authority sweep took 2,217.712s before any strategy, attempt or backtest and
  remains the dominant pre-search cost. E21/E22 reduced one exact authoritative
  fold package from 2.618s to 1.006s; E23 reduced full canary fits from 17 to 2;
  E24 reduced an exact duplicate reopen from 81.884s to 5.717s; E26 reduced one
  warm native-cell check by 22.01%. These are bounded wins, not a universe ETA.
  Complete-generation and full-run duration remain unmeasured.
- **The leak search is exhausted.** Backtest loop, stop construction, feature
  computation and the calendar_lockbox_v2 split are all clean. `pm_core.py:1227`
  force-closes at each window boundary, so no trade spans a seam and no training
  label can inherit validation prices. The reproduced failure mechanism is
  selection bias on thin samples, not look-ahead.

## Critical path to the objective

**Gate 1 is ANSWERED (2026-08-11).** PBO produced a real verdict on the D1
canary: `pbo=0.0`, N=224 evaluation-required, T=253 complete calendar days,
S=8/70, PASS in base and stress. The multiple-testing machinery works. The
earlier claim that PBO had never returned a verdict was true only of the June
book, which predates this contract. **Do not reopen this question.**

**D1 is infeasible geometry, not a strict threshold.** No candidate
authenticated: 782 of 896 final lanes had zero trades, 114 had 1-2, pooled max
3, against Winner's 5-per-fold and 40-overall. Decisively, recomputing at
`effective_trials=1` - multiplicity removed entirely - still demands ESS 42-481
against an observed 4.50-11, so all 17 surviving decisions fail anyway.
**No gate may be moved toward the observed result.**

**Gate 2 remains UNANSWERED after a repaired technical defect.** The sole
EURUSD|H1 canary was consumed at attempt 2, fold 0 after exposing
`FROZEN_PATH_COST_STRESS_FAILED`. E25 corrected the heterogeneous swap-cent
settlement defect and the exact preserved fold now passes with unchanged
economics (`c5c42cc`). No Gate-2 authentication rerun has been authorised or
attempted. The terminal receipt remains preserved at
`audit_reports/fxpm_system_audit_2026-08-11/GATE2_H1_TERMINAL_RECEIPT.json`.

**E27b is COMPLETE and peer-closed (2026-08-20).** The native authority
preflight — 99.763% of a bounded probe's wall time — is removed. Cold exact-600
universe is **1.0991 s** against a historical **2,218 s (36m 58s)**: a ~2,018x
reduction, inside the 2 s failure threshold with 45% margin. Migration moved 47
M5 cells from `technical_error` to `evidence_candidate`, all 47 resolution
checks PASS; the derived broker surface is 105 paths, accepted in CPM-070 as
legitimate final-code output rather than a moved criterion. Economic controls
byte-identical, TERMS snapshot unchanged.

The defence that survives is point-of-use raw recomputation, and it is proven
rather than asserted: `test_v5_receipt_rechain_cannot_defeat_selected_leaf_raw_recomputation`
poisons a leaf, coherently rechains report, consensus, parity, receipt, manifest
and the ACTIVE pointer so every hash validates, then asserts the structural layer
ACCEPTS it (`universe.valid`) while the artifact is REJECTED
(`RAW_PAIR_RECOMPUTATION_MISMATCH`). **If that test ever weakens, the
justification for E27b weakens with it.**

**Full suite: terminal PASS at `c0710aa`** — 4,245 passed, 11 skipped, 0
failures, JUnit-confirmed across 5,506 outcomes, 9,347 s. Pinned to that commit,
not to current HEAD.

| # | Gate | Owner | State |
|---|---|---|---|
| 1 | PBO produces a per-cell verdict | Codex | **DONE** — pbo=0.0, N=224, T=253 |
| 2 | A timeframe with enough activity to authenticate a candidate | Codex | **UNANSWERED** — consumed H1 canary exposed a real defect; E25 retained at `c5c42cc`; candidate authentication has not been rerun |
| 3 | A full run that completes | open | |
| 4 | Replay the book, corrected enumerator | open | |
| 5 | Forward Test A — reject-only falsification | pre-registered | `docs/FORWARD_TEST_A_PREREGISTRATION.md` v2 |
| 6 | Execution defects | Claude | **DONE** — lot sizing, recovery and order scaling all verified in this engine |
| 7 | Demo shadow | open | |
| 8 | Test B — the only certification | blocked on data | first valid day 2026-08-18; 365-day horizon ends 2027-08-17 |

Gate 6 closed 2026-08-11: realised risk matches target to 1.00% across six
instruments; 2,400 magic numbers generated with zero collisions plus a runtime
collision guard; exact-cell uniqueness with fail-closed on an unavailable
position snapshot. The three defects the operator named as killing the funded
attempt are fixed in this engine.

## Lane protocol

`agent_comms/PROTOCOL.md` v1 stands. Use `python tools/agent_bus.py`
(post / claim / release / tail / lanes). Claim before editing shared core files.

**Retired 2026-08-11:** `tools/agent_coordination.py` (1,543 lines) and its test
(437 lines) — an OS-locked, hash-chained event ledger built so two agents could
prove neither had tampered with the other's notes, on a single laptop with no
attacker, against the operator's own security-not-a-priority directive. Nothing
in the engine imported it. The 326-line bus does the same job. The existing
`coordination/` ledger is preserved read-only as decision history, because
PROJECT_MILESTONES.md cites its seq numbers as provenance.

That retirement is the template: **capability preserved, ceremony removed.**
