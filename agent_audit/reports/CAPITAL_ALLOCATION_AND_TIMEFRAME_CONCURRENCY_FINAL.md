# FxPM Capital Allocation and Multi-Timeframe Concurrency — Final Audit

Date: 2026-07-11  
Scope: `FxPM-Dev` only. The academic repository was not opened for writing.  
Decision standard: maximise durable geometric growth subject to survival, executable broker constraints, and evidence from the synchronized final-lockbox replay. No calculation below is a profitability guarantee.

## Executive decision

The “trading house of specialist cells” architecture is coherent. Strategy discovery and promotion should remain local to each `(symbol, timeframe, regime)` expert. Capital, margin and common-factor risk cannot be local, because every expert draws from the same account. A central allocator is therefore not a competing portfolio strategy; it is the institution’s balance-sheet controller.

The following interpretations are accepted:

- `0.25%` target initial-stop risk per admitted trade is appropriately small for a large expert pool.
- One D1 expert and one lower-timeframe expert may coexist on a symbol. When the lower trade closes, another lower-timeframe opportunity may subsequently be admitted while the D1 trade remains open.
- This permits many sequential M5 trades during one D1 holding period. It does **not** permit ten simultaneous M5 positions or arbitrary pyramiding on one symbol.
- `50%` is accepted only as the hard projected **margin-utilization** ceiling, corresponding to a `200%` margin level. The separate `300%` reopen threshold creates recovery hysteresis: after a margin block, entries do not resume until utilization falls to about `33.33%`.
- `33.33–50%` aggregate initial-stop risk is rejected as an unevidenced default. It remains in the calibration grid, but may be frozen only if tuning replay actually exercises it and every performance, drawdown, stress, bootstrap, margin and coverage constraint passes at every required balance.

## Do not conflate the three capital quantities

1. **Initial-stop risk** is the estimated account-currency loss if the position exits at its initial stop, including commission where measurable. This is the quantity summed by `max_total_open_risk_pct`.
2. **Margin utilization** is `used_margin / equity`. Its funded ceiling is 50%; it is never added to or compared with loss-at-stop risk.
3. **Gross/notional exposure** is the market value controlled. It can be many times equity and cannot be compared numerically with stop risk.

Margin level and margin utilization are reciprocals on the same snapshot:

`margin_level_pct = 10,000 / margin_utilization_pct`

| Margin utilization | Margin level | Current policy implication |
| ---: | ---: | --- |
| 33.33% | 300.03% | Reopen threshold after a block |
| 50.00% | 200.00% | Hard projected entry ceiling |
| 66.67% | 150.00% | Recovery/forced-reduction zone |
| 83.33% | 120.00% | Panic zone |

## The 35% win-rate, 2:1 payoff example

Let a win return `+2R`, a loss return `-1R`, and `p=0.35`.

- Gross expectancy: `0.35 × 2 - 0.65 × 1 = +0.05R` per trade.
- Break-even win rate: `1 / (2 + 1) = 33.333%`.
- Any all-in execution cost above `0.05R` removes the stated edge.
- At `0.25%` equity risk, gross arithmetic expectancy is `0.05 × 0.25% = 0.0125%` of equity per trade.
- Binary Kelly fraction under known, stationary and independent odds is `(2×0.35 - 0.65)/2 = 2.5%`. A `0.25%` trade is one-tenth of that single-bet Kelly estimate. The assumptions are not satisfied merely because a backtest reports a 35% win rate, and Kelly fractions cannot be added across dependent trades.
- Expected log growth at `0.25%` risk is approximately `0.01186%` per trade before costs.

For ten independent sequential trades with unchanged odds and zero costs:

| Quantity | Exact result |
| --- | ---: |
| Expected aggregate outcome | `+0.50R` = `+0.125%` equity |
| Probability aggregate outcome is positive | `48.6173%` |
| Probability aggregate outcome is negative | `51.3827%` |
| Probability all ten lose | `1.3463%` |
| Compounded drawdown if all ten lose at 0.25% | `2.4721%` |

The user’s throughput intuition is correct: ten genuine positive-expectancy opportunities have ten times the expected gross contribution of one otherwise identical opportunity. But most batches of ten in this thin-edge example still lose, and costs of only `0.05R` per trade erase the gross edge. Frequency is valuable only after edge, independence and execution survival are demonstrated.

## Why 33.33–50% cannot be initial-stop risk

At `0.25%` target risk, the position counts needed to fill each stop-risk cap are:

| Aggregate initial-stop cap | Full-size positions |
| ---: | ---: |
| 3% | 12 |
| 8% | 32 |
| 12% | 48 |
| 33.33% | about 133 |
| 50% | 200 |

If every stop is filled exactly, a 33.33% stop-risk book can lose one third of equity and needs a 50% return merely to recover. A 50% loss needs a 100% recovery. Stops are not loss guarantees. Under a common gap/slippage multiplier:

| Nominal stop-risk book | 1.5× stress | 2× stress | 3× stress |
| ---: | ---: | ---: | ---: |
| 3% | 4.5% | 6% | 9% |
| 12% | 18% | 24% | 36% |
| 33.33% | 50.0% | 66.7% | ~100% |
| 50% | 75% | 100% | 150% |

Therefore 33.33–50% stop risk is not “letting winners express themselves”; it makes survival depend on stops filling perfectly during the very event in which dependence and liquidity usually deteriorate.

## Dependence defeats position-count diversification

For `N` equal-risk exposures with common pairwise correlation `ρ`, the variance multiplier relative to one exposure is `N[1+(N-1)ρ]`, and a useful effective-independent-count diagnostic is:

`N_eff = N / [1 + (N-1)ρ]`

| Positions | ρ=0 | ρ=0.05 | ρ=0.10 | ρ=0.20 | ρ=0.50 |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 12 | 12.00 | 7.74 | 5.71 | 3.75 | 1.85 |
| 48 | 48.00 | 14.33 | 8.42 | 4.62 | 1.96 |
| 133 | 133.00 | 17.50 | 9.37 | 4.85 | 1.99 |
| 200 | 200.00 | 18.26 | 9.57 | 4.90 | 1.99 |

One hundred and thirty-three weakly correlated positions are not 133 independent bets. At only 5% average dependence they behave, by this variance diagnostic, like roughly 17.5 independent bets.

An illustrative seeded Gaussian common-factor simulation was also run for the user’s `p=35%`, `+2R/-1R`, `0.25%` setup. It is a sensitivity analysis, not a calibrated forecast. With 133 simultaneously resolved trades:

| Latent common-factor correlation | Probability batch loss | Probability loss ≥10% | 1st-percentile batch P&L |
| ---: | ---: | ---: | ---: |
| 0.00 | 35.82% | 0.24% | -7.75% |
| 0.05 | 45.24% | 10.84% | -17.50% |
| 0.20 | 50.37% | 29.00% | -28.75% |
| 0.50 | 54.77% | 43.38% | -33.25% |

The expected arithmetic P&L stays positive in the simulation, but the path and tail become unacceptable as common dependence rises. This is exactly why factor-group, signed-correlation and stress limits must sit above the specialist layer.

## Local evidence from the previous live implementation

The audited signal ledger is direct evidence that permissive caps can amplify a mechanical fault:

- 67 executed records had target risk median `0.50%`, but actual post-normalization risk median `5.35%`, mean `7.58%`, 95th percentile `21.10%`, and maximum `24.67%`.
- The median actual/target multiple was about `10.17×`; the maximum was `98.67×`.
- 83 minimum-lot blocks had actual risk median `37.67%` and maximum `362.06%`.
- The historical supervisor state records `total_open_risk 75.72% > 75.00%`.

Those observations came from the obsolete minimum-lot-up-clamp era. The current implementation correctly refuses unsafe minimum lots and has a much higher minimum funded equity. They remain relevant evidence against treating a large aggregate stop-risk number as harmless diversification.

## Required hierarchy for the trading-house model

The allocator should preserve every local edge unless a shared-account constraint is breached:

1. **Cell gate:** only the exact promoted `(symbol, timeframe, regime, strategy)` may propose an order. Risk starts at the fixed 0.25% target and may only be reduced by probation or drift controls.
2. **Same-symbol controller:** at most one D1 and one lower-timeframe position; never two lower-timeframe positions and never arbitrary pyramiding. Combined actual stop risk is capped.
3. **Factor groups:** currency legs, equity beta, crypto beta, metals and energy are capped by summed stop risk, not position count.
4. **Signed correlation:** positively aligned exposures consume one correlated-cluster budget. Opposite directions are not automatically treated as compounding risk, but they still consume gross stop and margin budgets.
5. **Institutional stop-risk caps:** summed commission-inclusive current initial-stop loss is bounded by the tuning-selected sealed cap; every non-netted factor group is independently bounded at 2%. The checked-in 8% value is provisional and is not funded-launch evidence.
6. **Stressed-loss cap:** gap/slippage and common-factor stress must be reported and used to reject a policy in synchronized replay. The live gross cap must remain survivable even when historical correlation estimates fail.
7. **Margin layer:** projected post-entry utilization is capped at 50% (`200%` margin level). A blocked account must recover to `300%` margin level before reopening; recovery and panic thresholds remain `150%` and `120%`.
8. **Drawdown layer:** daily/session/weekly and mark-to-market breakers stop new risk; they do not select experts or displace profitable cells.

The eventual aggregate stop-risk limit should be chosen on the tuning surface, frozen, and evaluated once in the synchronized final lockbox. It should not be selected by trying several limits on the final lockbox. Until that evidence exists, no assertion that 3%, 8% or 12% is return-optimal is statistically supportable.

## Tuning-only cap calibration and final-lockbox firewall

The selector evaluates every 0.25%-aligned cap from 3.00% through 90.00% on the common `tuning_validation` calendar window. It freezes the highest cap that passes **all** required starting-balance scenarios under the exact shared-capital simulator. A cap fails if any scenario has:

- fewer than 100 accepted positions or fewer than 730 calendar days;
- profit factor below 1.05 or a negative net return;
- mark-to-market or initial-stop-stress drawdown above 20%;
- incomplete mark-to-market coverage;
- observed ruin, a bootstrap ruin 95% upper bound above 5%, or bootstrap P95 maximum drawdown above 20%; or
- peak observed stop-risk use below 80% of that candidate cap.

The last rule prevents a non-binding high cap from "winning" merely because another limit constrained the book first. The full 349-point sweep, scenario summaries, policies, constraints, candidate hash, source lineage and selected maximum are stored in a sealed content-addressed artifact. Readiness requires its internal seal and the configured whole-file SHA-256.

The synchronized final replay must validate the frozen cap artifact **before reading final-lockbox bars**. Its evidence binds the cap artifact hash and selected value. The final lockbox may accept or reject; it has no path that selects, raises or substitutes the cap.

### P0 survivor-conditioning finding

The original one-pass workflow cannot honestly produce this evidence. `classify_promotion_status` consumes each cell's final-lockbox metrics, while the old replay enumerated only final `PROMOTED` survivors. Replaying those survivors on tuning data and then on the joint final period conditions the portfolio on outcomes already observed in that final period. This is survivor-selection leakage.

The corrected contract requires a separate sealed **pre-lockbox frozen roster** produced after tuning selection and before any final outcome is read. Cap tuning and final synchronized replay must consume the exact embedded frozen ledger, including candidates that later might fail an individual verdict; neither may filter or reprioritize using final outcomes. Evidence-only candidates use `FROZEN_PRELOCKBOX`. The live selector still requires `PROMOTED` plus promotion authorization, so the evidence status cannot authorize funded execution.

Until a genuine two-phase roster is generated, the calibration path and hash intentionally remain blank, 8% remains provisional, and readiness remains NO-GO. No synthetic artifact was created to manufacture a sign-off.

### Methodological limitations

- Winner and cap selection share the tuning surface, so the cap remains conditional on a selected candidate set.
- The feasible set need not be monotone because chronological admission changes which trades enter.
- Offline margin and timeframe-close marks can understate broker-specific intrabar, gap, liquidity and conversion losses.
- Bootstrap results are conditional sequence stresses, not forecasts; regime change can invalidate them.
- A final rejection fails the frozen institution as a whole. It does not authorize pruning final losers and replaying the remainder on the same observations.
- Passing these gates reduces known error modes but cannot guarantee live profitability.

## Code reconciliation completed in this audit

`pm_portfolio_replay.py` previously rejected every same-symbol overlap, while live had an optional D1/lower rule. It also contained a hidden six-position ceiling that could silently prevent institutional aggregation even when all risk and margin caps had room.

The synchronized replay now:

- permits exactly one D1 plus exactly one lower-timeframe position only when the funded policy enables it;
- rejects D1+D1, lower+lower, a third same-symbol position, and arbitrary pyramiding;
- enforces a separate same-symbol summed stop-risk cap;
- applies the same non-netted tuning-selected total cap and 2% gross factor-group stop-risk contract in replay and at the final live pre-send snapshot (8% remains only the checked-in provisional value);
- sizes and measures the final live candidate and every reconstructed open stop against the replay-equivalent conservative denominator `min(fresh equity, fresh balance - reconstructed open stop loss)`; missing balance, equity, stop geometry or positive risk capital fails closed;
- keeps signed-correlation stress and separately calculated projected margin checks authoritative;
- derives its structural position ceiling from the configured/required symbol roster, doubled only for D1/lower mode, instead of imposing an unexplained six-position allocation decision;
- bumps replay and sealed evidence schemas to `2026-07-11 ... v3`, invalidating evidence produced under incompatible concurrency or allocator semantics.

Verification:

- `tests/test_synchronized_portfolio_replay.py`: 21 tests passed after the change.
- Shared risk, synchronized replay, evidence seal, pipeline integration and performance-tool regression: **50 tests passed plus 6 subtests** before the broader regression pass.

## Primary references

- J. L. Kelly Jr., “A New Interpretation of Information Rate” (1956): https://onlinelibrary.wiley.com/doi/abs/10.1002/j.1538-7305.1956.tb03809.x
- Busseti, Ryu and Boyd, “Risk-Constrained Kelly Gambling”: https://web.stanford.edu/~boyd/papers/kelly.html
- Basel Committee, stress and concentration-risk principles: https://www.bis.org/bcbs/publ/d595.pdf
- CFTC, stop-loss execution study/risk context: https://www.cftc.gov/sites/default/files/Stoploss_final_ada.pdf
