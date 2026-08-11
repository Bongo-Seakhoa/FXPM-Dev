# Forward Test A — pre-registration

**Written before the test runs. Any change after seeing a result permanently
consumes the window.** Agreed CPM-010 / XPM-012, 2026-08-11.

## What A is, and what it is not

A pins `evidence_as_of_utc = 2026-04-01`, so every fitting and selection
partition closes on or before that date, and replays the resulting book over
**2026-04-01 → 2026-07-14** (~74 trading days).

**A is a falsification gate. It can reject a book. It can never certify one.**
Pinning the cutoff blinds the *fitting pipeline* to Apr–Jul 2026. It cannot blind
the *research process*: this period has already been read, measured, and used
while tuning gate thresholds. Out-of-sample for the algorithm is not
out-of-sample for the people who built the algorithm.

Certification is Test B — a frozen July-14 package replayed on post-2026-07-14
data nobody has inspected. A is what we run first because it is free.

## Hypothesis

> A book selected without contact with Apr–Jul 2026 retains a materially similar
> edge in that period.

The interesting outcome is the **decay ratio**: realised PF ÷ the book's own
backtest PF. Absolute forward PF alone is not the test — a book can post PF 1.1
forward while its backtest claimed 3.0, and that gap is the leak the operator
described.

## Rejection criteria — any one rejects the book

| # | Condition | Why |
|---|---|---|
| R1 | Aggregate realised PF **< 1.00** | The book loses money forward. Nothing else matters. |
| R2 | Decay ratio **< 0.60** (realised PF ÷ backtest PF) | The engine is measuring something that does not persist. This is the leak test. |
| R3 | Realised max drawdown **> 2×** the book's backtest max DD | The risk profile does not hold out of period. |
| R4 | Any single cell loses **> 25%** of its allocated capital | One cell can ruin the account; the June book had cells at −94.9%. |
| R5 | Fewer than **150** closed trades across the book | Insufficient evidence. **This rejects — it does not trigger a retry.** |

## What A cannot conclude

Passing all five does **not** mean the book is profitable. At PF 1.10 the
per-trade mean R is ≈0.048 with unit SD; detecting mean R > 0 at 95% one-sided
with 80% power needs ≈2,700 trades. A 74-day window will not supply that. **A
clean A result means "not yet falsified", and that is all it means.**

## Stopping condition

One run. No parameter, threshold, universe or gate change after results are
seen. If the book is rejected, the *next* book is a new experiment requiring a
fresh pre-registration — the Apr–Jul window is spent for the rejected lineage
and cannot be reused to evaluate a tuned successor.

## Failure-to-run conditions

These abort A without consuming the window, because no result was observed:
run does not complete; spool or memory bound trips; fewer than 20 cells reach a
promotion verdict; PBO returns `INSUFFICIENT_EVIDENCE` for the majority of cells.
An aborted A may be re-run after the cause is fixed.

## Recorded before the run

- Config change: `evidence_as_of_utc: "" → "2026-04-01T00:00:00Z"`. This moves
  the `evidence_as_of_utc == ""` assertion in `test_config_json_integrity`.
- Everything else frozen at the commit that produces the book.
- The book, its backtest metrics, and the replay output are all captured before
  any human or agent reads the forward numbers.
