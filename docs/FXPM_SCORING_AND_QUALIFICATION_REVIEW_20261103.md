# FxPM scoring and qualification review, W9 pass 1

**Date:** 2026-11-03
**Author:** claude-pm
**Scope:** the authoritative research path only: `pm_authoritative_search_execution.py`, `pm_winner_raw_evidence.py`, `pm_winner_selection.py`, `pm_trade_power.py`. The legacy scorer in `pm_pipeline.py` is not reached from this path and is not reviewed.
**Method:** a read-only map by a search agent, with every consequential claim re-read in source by claude-pm before it appears below. Line numbers are from checkout `529c04f0`.
**Status:** W9.1 and W9.2 complete; W9.3 drafted; W9.4 is a preliminary verdict with three decisions for the operator or council. Nothing here changes code or policy.

## 1. How a cell is scored today

1. **No train-window ranking exists.** The 300 attempts per cell are fixed before any result is seen (`selection_scope: PRECOMMITTED_ATTEMPT_NOT_POSTHOC_WINNER`, executor 2584; `post_selection_search: False`, 2595). Every attempt is backtested on three partitions of each inner fold at both cost tiers: the fold's fit window split 75/25 into `optimizer_train` and `tuning_validation`, and the fold's validation window, which the code names `final_lockbox` (executor 2001 to 2041, 2682 to 2710).
2. **Train is used only to size the evidence requirement.** `calibrate_train_trade_power` turns the train trades into a frozen required trade count under a Šidák-adjusted alpha over the exact attempt count (`pm_trade_power.py` 342 to 372). It produces no score.
3. **Tuning has one gate.** Effective trades on `tuning_validation` must reach the frozen requirement, otherwise `PARTITION_EFFECTIVE_TRADES_UNDERPOWERED` (`pm_trade_power.py` about 2039). The same check repeats on the fold validation window (`pm_winner_raw_evidence.py` 2506 to 2527).
4. **The selection surface is the fold validation window of every attempt.** The selector consumes the daily R surface of `final_lockbox_p75` and `final_lockbox_p95` (`pm_winner_raw_evidence.py` 4924 to 4931). The receipts label it `PRECOMMITTED_HISTORICAL_INNER_FOLD_ONLY` and `confers_final_oos_or_trading_authority: False` (executor 2530 to 2533). So in this path "final" means the inner fold's held-out window, scored for all attempts; it is not a window consumed once for one frozen winner.
5. **Qualification gates** (`pm_winner_selection.py` 231 to 262, applied in `_candidate_statistics` 4222 to 4302) are constants in the policy dict: at least 252 days, HAC effective days at least 126, at least 4 dependence blocks, at least 40 completed trades and 5 per fold, HAC effective trades at least 20 and at least the trade-power requirement, stochastic support, mean, worst-fold and newest-fold log growth above zero on the base lane and each stressed lane, a moving-block bootstrap lower bound that must pass, and a within-cell deflated-Sharpe p of at most 0.05.
6. **Ranking key.** The minimum across the tier's lanes of the dependence-robust lower confidence bound of mean daily log growth at 0.25 percent cell risk (`primary_candidate_score`, 255 to 262). Dominance needs a better key and a Holm one-sided p of at most 0.05. If several survive and any Holm-adjusted pairwise difference is at most 0.05, the cell returns `UNRESOLVED_NONDOMINATED_SET` and no winner. Otherwise ties fall to `_simplicity_key` (4436 to 4441), which is alphabetical order of family, strategy id and candidate id.
7. **Multiple-testing controls that are operative:** attempt-aware deflated Sharpe over the exact attempt count (`EXACT_ATTEMPT_COUNT_DEFLATED_SHARPE`, 326; `_one_sided_dsr_p` 3152 to 3188), per-cell CSCV probability of backtest overfitting capped at 0.50 (3644, 4705 to 4715), Holm on pairwise comparisons (4512 to 4527), Šidák inside trade power. Benjamini-Yekutieli exists only at book level in `pm_book_multiplicity.py` and is diagnostic by its own docstring.
8. **Diagnostic only, never gates:** per-fold HAC lower bounds, tail CVaR, maximum path drawdown, on every tier (283 to 300). The comment states the reason: a universal cell-level risk preference would be an unauthenticated portfolio-risk oracle; sizing belongs to the capital authority.
9. **Cost tiers.** P75 is required; P95 is preferred. Outside the archived generation, a P95 tier with no qualified winner or a PBO failure falls back to the P75 winner (`NO_QUALIFIED_P95_WINNER_P75_FALLBACK`, 4883 to 4888 and 4980 to 4985). Only the archived generation fails closed. `docs/IDEAL_END_STATE.md` row 6 says a failing P95 tier fails closed; the code and Root's E4464 say fallback. The document and the code disagree.

## 2. What was inoperative in June 2026 and what the code says now

The June 2026 book promoted 157 rows with both multiple-testing defences inoperative and PBO returning not-available (`docs/IDEAL_END_STATE.md` 22 to 24, 91). In the current authoritative selector the deflated Sharpe and the PBO limit are both fail-closed gates with typed rejection codes. The legacy weighted scalar score with 200 Optuna trials lives only in `pm_pipeline.py` and is not imported by the authoritative modules. On this point the code is sound.

## 3. Assessment

**Sound.** Precommitment of the attempt roster, exact-count deflation, PBO, Holm dominance, the unresolved-set refusal, the trade-power requirement derived from a stated economic edge of 0.25R, and the fail-closed typed outcomes. This is a selector built to refuse, which is what the operator asked for.

**Weak points, in order of consequence.**

- **W1 Selection and qualification share one surface.** Every attempt is scored on the same inner-fold validation windows that then qualify the winner. Precommitment and deflation are the only defences against selecting the luckiest of 300 on that surface. They are real defences, but the cleanest structure is to select on one surface and qualify on another. Route A does exactly that: selection on the train-window opportunity chain, qualification on the held-out account path. This is the main reason to prefer Route A beyond cost.
- **W2 The P95 fallback.** A cell whose P95 lane cannot qualify still trades on its P75 winner. Under the operator's goal, confidence in live money, the harsher reading is the safer one: a nonempty P95 pool that fails should fail the cell. This is a policy choice and the operator should make it explicitly, because the standing document and the code disagree.
- **W3 Alphabetical tie-break.** When two candidates are statistically indistinguishable and no unresolved-set guard fires, the winner is the alphabetically earlier family. It is deterministic and rare, but it is not a reason. A stated rule such as fewer parameters, then the smaller stop distance, then the identifier, would be a reason. Low consequence, cheap to fix.
- **W4 No economic floor on the score.** Log growth above zero on every lane is the only return condition. A cell with a tiny but statistically supported edge qualifies. The 0.25R edge sits in the power calculation, not in the verdict. Whether a minimum edge or a minimum lower bound should gate is a risk-appetite choice for the operator; today the portfolio layer is expected to size such cells down.
- **W5 Drawdown is not a gate by design.** The comment's reasoning is coherent: the cell selector owns no sizing. But the 28-day account handoff hands each cell a real account. If a per-cell drawdown stop is wanted, it belongs in the lifecycle rule for live cells, not in the historical selector.
- **W6 The 28-day cycle is not in the selector.** The selector runs once over a fixed fold geometry. The operator's objective is a reselection at every 28-day boundary from the latest eligible past. The Route A edition has to define the per-cycle windows and the per-cycle deflation count; today that definition does not exist in code.

## 4. What Route A changes, W9.3

| Element | Today | Route A as proposed (E4461, E4464) |
|---|---|---|
| Candidates per cell | 300 precommitted attempts | adaptive per-strategy studies, budget by coverage of the domain, every evaluated tuple counted |
| Selection surface | inner-fold validation daily R, account path | train-window opportunity chain per regime, cash-free |
| Selection key | min dependence-robust LCB of daily log growth across lanes | lower bound of total R over the non-overlapping chain at P95 (recommendation, not adopted) |
| Qualification | the same surface, all gates above | the account path on the shortlist of 5 only: one frozen tuning evaluation per tier, then the same gates |
| Deflation count | exact attempt count, 300 | all tuples evaluated on the selection surface, before dedupe (Root E4464) |
| Tie-break | alphabetical | to be stated |
| Family quota in the shortlist | none, one pool | at most 2 of 5 per family (recommendation) |

Two conditions make Route A at least as honest as today: the chain must reproduce the kernel's trade list exactly, which `pm_signal_chain.py` and its byte-equality test establish on a fixture; and the deflation count must be the number of tuples the selection surface saw, not the five that reached the account path.

## 5. Decisions requested

1. **P95 rule.** Fail closed when a nonempty P95 pool cannot qualify, or keep the P75 fallback. Recommendation: fail closed, matching the written policy. **Operator ruling 2026-11-03: keep the P75 fallback.** The code stands as it is; `docs/IDEAL_END_STATE.md` row 6 is now the stale text and should be corrected by its owner. The end-grid row and every report must state the tier the winner was selected under, so a fallback is always visible.
2. **Economic floor.** Add a minimum lower bound on mean daily log growth, or a minimum chain edge in R, as a gate, or leave sizing to the portfolio layer. Recommendation: no new gate in the selector; record the lower bound in the end-grid row so the capital layer can size on it. **Operator ruling 2026-11-03: add a floor.** Specification proposed for council: mean net R per completed trade on the BASE lane at or above a declared floor, every existing gate unchanged; the level is the operator's risk-appetite choice, with the pre-registered trade-power economic edge of 0.25R as the candidate so that the evidence requirement and the verdict share one number. Placement: the Route A process edition (W6.3), never a patch to the frozen process. Expected effect: more NO_TRADE cells, by design.
3. **Tie-break.** Replace alphabetical order with a stated simplicity rule. Recommendation: yes, in the Route A edition.

## 6. Cost tier definition, measured 2026-11-03

The operator proposed replacing P75 and P95 with mean plus one and mean plus two standard deviations, read as 68.3 percent and 95 percent. Under a normal distribution those figures are the central bands; the one-sided coverage of mean plus one SD is the 84.1st percentile and of mean plus two SD the 97.7th. Spreads are not normal, so neither reading applies. Percentile tiers target an adopted quantile; with ties and finite samples the achieved share can differ slightly from 75 or 95 percent, which the frozen profile records as `share_at_or_below`. Measured on pre-cutoff native bar spreads (MT5 points, the protected cutoff applied first, bar-minimum spread as the profile defines it):

| Symbol | TF | Rows | Zero share | Skew | P75 | P95 | mean+1SD lands at | mean+2SD lands at | 2SD over P95 |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|
| EURUSD | H1 | 94,288 | 40% | 1.4 | 9 | 20 | 81.4th pct | 99.2nd pct | 1.12 |
| EURUSD | M5 | 609,623 | 21% | 6.3 | 9 | 12 | 95.5th pct | 97.8th pct | 1.83 |
| GBPAUD | H1 | 94,573 | 0% | 2.2 | 52 | 100 | 80.6th pct | 97.0th pct | 1.23 |
| USDZAR | M5 | 599,479 | 2% | 4.9 | 140 | 338 | 92.0th pct | 96.3rd pct | 1.07 |
| XAUUSD | H1 | 56,748 | 16% | 3.8 | 35 | 50 | 93.7th pct | 98.1st pct | 1.39 |
| BTCUSD | H1 | 84,599 | 26% | 1.9 | 4,000 | 11,643 | 81.5th pct | 94.4th pct | 0.94 |
| APPLE | H1 | 47,446 | 88% | 41.0 | 0 | 5 | 93.0th pct | 95.8th pct | 1.02 |
| USDJPY | H1 | 59,451 | 0% | 2.2 | 22 | 50 | 84.0th pct | 98.6th pct | 1.06 |
| XAGUSD | M5 | 575,855 | 2% | 0.2 | 50 | 63 | 84.1st pct | 99.8th pct | 1.14 |

Reading: recorded bar spreads are right-skewed with spikes and many zero-reported bars, so a standard deviation is dominated by the tail. "One SD" would sit anywhere from the 81st to the 96th percentile of the recorded bar spreads depending on the symbol, and "two SD" from the 94th to the 99.8th. The same label would mean a different test per symbol, harsher than today for EURUSD M5 and lighter for BTCUSD. A percentile tier targets an adopted quantile of the recorded series; with ties and finite samples the achieved share differs slightly, and the frozen profile records it per cell as `share_at_or_below`. None of this is coverage of execution prices: the recorded Spread field is the bar minimum, not a quote.

Recommendation: keep the quantile definition and the 2026-10-13 policy. Rename the lanes by role, BASE for P75 and STRESS for P95, state the coverage in plain words wherever a tier is shown, and report each tier's distance from the mean in standard deviations per cell as a diagnostic. That gives the PMs the SD language without changing a number. If a harsher stress is wanted, raise the quantile deliberately, P97.5 or P99, per asset class. Changing the levels now would re-key the frozen profile, the derived profile 9fe54 and every precommit, and invalidate the eight cost-ready derived cells.

### 6.1 What the two lanes mean in money, and the council options

The operator's intent for P75 and P95 was to account consistently for 75 to 95 percent of cost; FBS charges spread and swap only. Coverage is a frequency statement about recorded bars, not about execution prices. Against the mean recorded bar spread, P75 sits at 1.1 to 1.4 times and P95 at 1.7 to 3.6 times on the development symbols; because the recorded value is the bar minimum rather than a quote, this measures the height of each lane against the recorded series and does not prove that the lanes over-cover real cost (EURUSD H1 1.28 and 2.85, GBPAUD H1 1.18 and 2.27, USDZAR M5 1.17 and 2.82, XAUUSD H1 1.32 and 1.88, BTCUSD H1 1.25 and 3.63, USDJPY H1 1.11 and 2.52, XAGUSD M5 1.38 and 1.74). The frozen profile uses the exact prior M5 close spread per target bar, so its tiers differ from these bar-level figures, but the ordering holds. Two limits of any tier: 37 profile cells are `UNUSABLE_ZERO_QUANTILE` because the broker reports zero spread on most bars, and the bar Spread field is the bar minimum, not an executable quote, so even an observed per-bar spread is optimistic without an uplift calibrated against real fills.

The real weakness is not the level but the constant: one number per symbol and timeframe ignores when a strategy enters, and strategies that enter at news or session opens face systematically wider spreads than the tier. The kernel already accepts per-entry observed spreads (`entry_spread_observations` in `Backtester.run`), and the labeller takes per-bar half-spread arrays.

Options put to council:

- **A, now.** Keep percentile tiers, rename BASE and STRESS, add per-cell diagnostics to the end-grid row: tier over mean spread, and SD distance. No re-key.
- **B, next cost edition.** BASE becomes the per-trade observed entry and exit spread with an uplift calibrated in the M9 demo shadow against real fills; STRESS stays the P95 tier; both lanes required. This is the truthful model the operator is reaching for, without the normality assumption.
- **C, rejected.** SD tiers: a different coverage per symbol under one label, and a full re-key.

Recommendation: A now, B scheduled after the W5 cost measurements, C rejected. A P99 or two-SD lane may be reported, never required. In the live edition, tie the live entry refusal to the STRESS tier explicitly so research and live mean the same thing by "covered".

**Operator ruling, 2026-11-03:** "Yes, A now, B next." Relayed to the bus the same day. A is W2.2 in the landing plan, B is W2.3.

## 7. Open items for W9 pass 2

- Trace how a failed trade-power check marks an attempt rejected before the selector runs (`pm_winner_raw_evidence.py` 4954 against `pm_winner_selection.py` 3891).
- Confirm which generation the next run uses, archived or current, since that decides the P95 behaviour today.
- Read the book-level multiplicity diagnostic in `pm_book_multiplicity.py` and decide whether it should gate under a 2,400-cell grid.
- Define the per-cycle window and deflation count for the 28-day reselection.
