# Post-Optimization Status

Date: 2026-06-19

The operator reported that optimization is complete. Codex refreshed the current-book inventory and ran the corrected all-promoted replay.

## Current Book

| Item | Value |
|---|---:|
| Logical promoted cells | 117 |
| Replay rows completed | 117 |
| Positive replay rows | 79 |
| Negative replay rows | 38 |
| Rows with PF below 1.0 | 38 |
| Net replay sum, all rows | 4,529,727.86 |
| Net replay sum, excluding PALLADIUM default-spec row | 2,443,636.25 |
| Median PF | 1.10 |
| Max DD, all rows | 1580.65% |
| Max DD, excluding PALLADIUM | 99.55% |

## Important Caveats

- `PALLADIUM H4 StochasticReversalStrategy` used a default instrument spec because no `PALLADIUM` spec was found. Its `2,086,091.61` net result must not be treated as reliable until broker specs are supplied and replayed.
- Profit is highly concentrated in a few rows: `PALLADIUM`, `XAUUSD`, `BTCUSD`, and `US100` dominate the aggregate.
- 38 of 117 replayed cells are net losers and have PF below 1.0.
- Shadow-selection audit still flags all 117 rows for extreme validation Sharpe without PBO evidence.

## Worst Replay Rows

| Symbol | TF | Regime | Strategy | Net | PF | Return % | Max DD % |
|---|---|---|---|---:|---:|---:|---:|
| ETHUSD | M5 | TREND | DayOfWeekBiasStrategy | -9487.02 | 0.31 | -94.86 | 99.55 |
| EURJPY | M30 | RANGE | ZScoreVWAPReversionStrategy | -9158.13 | 0.72 | -91.58 | 91.68 |
| USDJPY | M30 | TREND | KeltnerFadeStrategy | -6784.14 | 0.74 | -67.84 | 70.35 |
| ETHUSD | H4 | RANGE | TimeOfDayMomentumStrategy | -5155.57 | 0.88 | -51.56 | 57.72 |
| US30 | M15 | TREND | SessionVWAPReversionStrategy | -3983.63 | 0.82 | -39.84 | 43.52 |

## Best Replay Rows

| Symbol | TF | Regime | Strategy | Net | PF | Return % | Max DD % |
|---|---|---|---|---:|---:|---:|---:|
| PALLADIUM | H4 | TREND | StochasticReversalStrategy | 2086091.61 | 1.17 | 20860.92 | 1580.65 |
| XAUUSD | M15 | CHOP | EMAPullbackContinuationStrategy | 1494060.89 | 1.62 | 14940.62 | 17.26 |
| BTCUSD | M5 | RANGE | DayOfWeekBiasStrategy | 301286.37 | 1.32 | 3012.86 | 36.45 |
| US100 | M5 | RANGE | DayOfWeekBiasStrategy | 185082.00 | 1.39 | 1850.82 | 32.58 |
| BTCUSD | M5 | TREND | DayOfWeekBiasStrategy | 107521.64 | 1.33 | 1075.22 | 15.89 |

## Deployment Verdict

The completed book is materially better than yesterday's one-winner interim ledger, but it is not clean enough to treat as automatically live-ready.

Required before live use:

- Add or verify the `PALLADIUM` broker/instrument spec and replay it.
- Decide whether `PROMOTE_SMALL` cells are intended to be live-eligible or paper/small-size only.
- Filter or quarantine replay-negative cells with PF below 1.0 and severe drawdown.
- Review concentration risk in `XAUUSD`, `BTCUSD`, `US100`, and `PALLADIUM`.
- Re-run live readiness/preflight against the completed book.
