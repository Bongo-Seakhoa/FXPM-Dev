# FXPM Program State

Updated: 2026-08-11 (Claude). One page. Rewritten in place, never appended.

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
3. **No unilateral integration.** The failed experiment was AI deciding its own
   integration. Reconcile-or-discard decisions go to the operator.

## Where the system actually is

- **Nothing is tradeable.** Book optimised 2026-06-09/11, `valid_until`
  2026-07-05, expired 37 days. No run has completed since.
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
- **Runtime: one real win, one overstated.** Regime labels categorical is real —
  22% off every feature frame, in every worker, on the constraint that actually
  binds (7.4 GB RAM). The resolver memo's 3798× is real but **does not speed the
  production run**: `pm_main.py:12341` already wraps `run_optimization` in the
  authority scope, so the optimize path was cached before the memo existed. It
  buys test-suite time (3:29:33 → 0:53:42) and one-shot `--preflight`/`--trade`
  startup. Corrected by CPM-001 after this file claimed otherwise.
- **The leak search is exhausted.** Backtest loop, stop construction, feature
  computation and the calendar_lockbox_v2 split are all clean. `pm_core.py:1227`
  force-closes at each window boundary, so no trade spans a seam and no training
  label can inherit validation prices. The reproduced failure mechanism is
  selection bias on thin samples, not look-ahead.

## Critical path to the objective

| # | Gate | Owner | State |
|---|---|---|---|
| 1 | PBO actually produces a verdict per cell | open | **blocking** — it has never once returned one |
| 2 | Cold native verification cheap (manifest-trust) | Claude | designed, safe, not built |
| 3 | A run that completes | open | never achieved |
| 4 | Replay the resulting book, corrected enumerator | open | |
| 5 | Forward-simulate 2026-04-01 → 07-14 (74 trading days OOS) | open | the only honest forward test available |
| 6 | Execution defects: lot sizing, recovery, order scaling | open | named by operator as what killed the funded attempt |
| 7 | Demo shadow, then funded at a size where min-lot geometry is not fighting us | open | |

Gate 1 is the one that decides whether any of this is real. A book that cannot
produce a PBO verdict cannot be trusted at any size.

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
