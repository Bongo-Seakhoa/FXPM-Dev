# Recommendation Triage

Existing audit recommendations were reclassified under the performance-first mandate in `prompt.md`. The decision labels are: `KEEP NOW`, `KEEP BUT REFRAME`, `DEFER`, `REJECT`, `ALREADY IMPLEMENTED`, and `NEEDS EVIDENCE`.

| Recommendation | Existing Source | Direct Profit Mechanism | Compute Burden | Optuna Compatible? | Numba Compatible? | Decision | Reason |
|---|---|---|---|---|---|---|---|
| Add per-trade MAE/MFE capture | `PRIORITIZED_RECOMMENDATIONS.md` P1; F107 | Separates paid drawdown from bad risk, enabling exit/filter changes without killing profitable pain | Low for instrumentation; medium for replay | Yes; no search-space change | Yes; implemented in compiled loop | ALREADY IMPLEMENTED | PERF-FIX-001 adds fields and parity tests |
| Add expected-vs-fill slippage attribution | F005; strategic/technical reports | Detects live execution leakage that can erase edge | Low | Yes; log-only | Not applicable | ALREADY IMPLEMENTED | Entry attribution implemented in PERF-FIX-002; exit attribution remains queued |
| Fix dashboard no-loss profit factor | F112 | Prevents profitable no-loss samples being interpreted as PF=0 | Low | Yes | Not applicable | ALREADY IMPLEMENTED | PERF-FIX-003 |
| Add schema version for new MAE/MFE/slippage fields | Claude review of performance pass; prompt section 17 | Prevents old missing fields being read as zero excursion/slippage | Low | Yes | Yes; Python metadata only | ALREADY IMPLEMENTED / PARTNER RE-REVIEW | PERF-FIX-004 implemented after Claude challenge |
| Canonical metric contract across engine/dashboard/pipeline/tools | F105/F106/F112; prompt section 9 | Prevents objective/reporting from selecting or displaying wrong economic winner | Medium | Yes, required before objective change | Partly; metric code mostly Python | ALREADY IMPLEMENTED / PARTNER REVIEW | `pm_metrics.py` plus engine/dashboard/pipeline/benchmark wiring added; objective metric mutation still evidence-gated |
| Bounded USD 10,000 promoted-cell replay with new MAE/MFE fields | Prompt sections 5, 8, 14 | Produces first paid/unpaid drawdown table and candidate A/B evidence | Medium | Yes | Yes, uses existing backtester | ALREADY IMPLEMENTED / ACCEPTED WITH CORRECTION | Bounded replay script added; Claude found symbol-level enumeration bug; Codex fixed it and current corrected replay shows the one logical promoted winner loses |
| EMAPullbackContinuation TP/trailing A/B | Claude partner review CC-AB-1 | May retain more MFE and increase net profit for trend continuation winners | Medium | Yes if parameters propagate end-to-end | Requires Numba/Python exit parity if behavior changes | NEEDS EVIDENCE | Do only after replay proves capped MFE/giveback |
| DayOfWeekBias post-selection replay | Claude partner review CC-AB-2 | Detects validation-surface noise in historical large strategy family | Medium | Yes | Yes | TOOLING IMPLEMENTED / NEEDS CLEAN BOOK | Shadow audit script flags DayOfWeekBias cells when present; current live book has no DayOfWeekBias cell |
| Objective shadow-ranking without stability multiplier | Claude partner review CC-AB-3 | Tests whether rewarded aggression is being demoted by variance penalty | Low-Medium | Yes as shadow metric first | Not applicable | NEEDS EVIDENCE | Do not alter objective until ranking aligns with net dollars |
| Canonical Sharpe/expectancy objective inputs | Claude partner review CC-AB-4; F106/F111 | Prevents inflated/sparse metrics from selecting wrong winners | Medium | Yes | Not directly | TOOLING IMPLEMENTED / HIGH PRIORITY | Current FORD winner proves sparse validation optimism can invert: 16 validation trades PF 1.77 to 166 replay trades PF 0.66 |
| Startup/preflight account and tradeability report | `PRIORITIZED_RECOMMENDATIONS.md` P3 | Avoids operating on wrong account/symbol constraints | Low | Yes | Not applicable | ALREADY IMPLEMENTED / PARTNER REVIEW | `python pm_main.py --preflight` plus min-lot tradeability readiness checks added |
| Account-size regimes | Prior reports/F113 | Smooths scaling risk but does not by itself prove higher net profit | Medium | Yes | Not direct | DEFER | Prompt explicitly deprioritizes account-regime work in this phase |
| Balance-aware live universe | Prior reports/F103 | Avoids untradeable/min-lot distorted trades | Medium | Yes | Not direct | PARTIAL / PREFLIGHT IMPLEMENTED | Readiness now warns/errors on min-lot tradeability; automatic universe suppression remains evidence-gated |
| Absolute-dollar breakers | Prior reports/F102 | Can prevent unrecoverable live loss | Low-Medium | Yes | Not direct | DEFER | Prompt says do not center conservative controls without profit evidence |
| Test `max_risk_pct` at 8-10% | `PRIORITIZED_RECOMMENDATIONS.md` P1; F003 | Could remove tail-risk rows | Low | Yes | Not direct | DEFER | Risk lowering is superseded unless it improves net value under USD 10,000 |
| Lower secondary/same-symbol risk caps | `PRIORITIZED_RECOMMENDATIONS.md` P1; F104 | Reduces exposure stacking | Low | Yes | Not direct | DEFER | Needs portfolio replay with profit impact |
| Run bounded USD 22 replay | `PRIORITIZED_RECOMMENDATIONS.md` P1; F108 | Would show current micro-account distortion | Medium | Not relevant to canonical baseline | Not relevant | REJECT | Prompt forbids balance/account-size focus for this phase |
| Add pristine-OOS decay audit | `PRIORITIZED_RECOMMENDATIONS.md` P2; F109 | Could reduce false winners | Medium-High | Yes | Not direct | KEEP BUT REFRAME | Do only if it changes winner ranking or objective confidence |
| Enable max-validation-Sharpe severe flag when PBO unavailable | `PRIORITIZED_RECOMMENDATIONS.md` P2; F002 | Prevents sparse/extreme validation winners | Low | Yes | Not direct | NEEDS EVIDENCE | Shadow-rank first to avoid rejecting profitable aggression |
| Raise validation trade-count requirements | `PRIORITIZED_RECOMMENDATIONS.md` P2; F002 | Reduces false winners from tiny samples | Low-Medium | Yes | Not direct | NEEDS EVIDENCE | Must be strategy/timeframe-specific and tested against net value |
| Relabel/annualize Calmar | `PRIORITIZED_RECOMMENDATIONS.md` P2; F105 | Prevents metric misinterpretation | Low | Yes | Not direct | KEEP NOW | Reporting correctness; no strategy behavior change |
| Weekly drift report | `PRIORITIZED_RECOMMENDATIONS.md` P3 | Detects strategy decay | Medium | Yes | Not direct | KEEP BUT REFRAME | Useful after profit-extraction replay establishes measured edge |
| Define Toronto metric | `PRIORITIZED_RECOMMENDATIONS.md` P3; F004 | None unless operator defines economic use | Low | Yes | Not direct | DEFER | No Toronto metric exists and no production decision depends on it |
| Full optimization rerun | Prompt forbidden outcome | Could produce new winners but violates budget/constraint | High | Yes | Yes | REJECT | Full optimizer explicitly forbidden |
| Balance sweep | Prompt forbidden outcome | Mostly explains account-size/min-lot distortion | High | Not needed | Not needed | REJECT | Canonical baseline is USD 10,000 only |
| Live trading / production credential edit | Prompt guardrail | Not an audit/test mechanism | High risk | Not applicable | Not applicable | REJECT | No live trades or credential/config changes in this phase |

## Current Highest-Value Path

1. Resolve whether the current one-winner `pm_configs.json` is final or mid-reoptimization. Do not run live on it in the current state.
2. After a clean completed winner book exists, rerun corrected current-ledger inventory, all-promoted-winner replay, and shadow-selection audit.
3. Implement or shadow-test validation trade-count floors, candidate-count deflation, and regime/strategy compatibility checks before any live promotion.
4. Use MAE/MFE fields to identify paid versus unpaid drawdown on the clean book.
5. Only then implement strategy behavior changes such as trailing, TP expansion, or MAE/MFE filters.
