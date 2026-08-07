# Prioritized Recommendations

## P0 - Decide and Protect

| Recommendation | Why | Evidence | Effort | Risk |
| --- | --- | --- | --- | --- |
| Decide $22 mode: preservation, lottery, or fund-up | The correct caps differ by intent | F101-F104, F108 | Low | Low |
| Add a balance-aware live universe | Removes symbols that are already untradeable at min-lot | F103, signal risk summaries | Small-Medium | Low |
| Add absolute-dollar breaker overlays | Percent breakers are too wide for $22 | F102 | Small-Medium | Medium, because trading may halt more |
| Confirm terminal account before live mode | Config pins identity but does not log in | F001 | Small | Low |

## P1 - Reduce Unrewarded Risk

| Recommendation | Why | Evidence | Effort | Risk |
| --- | --- | --- | --- | --- |
| Test `max_risk_pct` at 8-10% | 10% cap keeps 48/67 executed rows while blocking the 19 riskiest | F003 | Small | Medium |
| Lower secondary/same-symbol risk caps | Current 50% secondary cap is too high for a micro account | F104 | Small | Medium |
| Run bounded $22 replay/backtest on executable symbols | Demo/backtest notional does not match live constraints | F108 | Medium | Low |
| Add MAE/MFE capture | Needed to classify high-drawdown trades | F107 | Medium | Low |

## P2 - Improve Robustness and Reporting

| Recommendation | Why | Evidence | Effort | Risk |
| --- | --- | --- | --- | --- |
| Enable max-validation-Sharpe severe flag when PBO unavailable | Extreme validation Sharpe is widespread | F002 | Small | Medium |
| Raise validation trade-count requirements for Sharpe override | Low sample sizes create fragile winners | F002 | Small-Medium | Medium |
| Add pristine-OOS decay audit | Bleeding-edge split favors freshness over clean holdout | F109 | Medium | Low |
| Relabel or annualize Calmar | Current value is cumulative return/maxDD | F105 | Small | Low-Medium |

## P3 - Monitoring and Productization

| Recommendation | Why | Evidence | Effort | Risk |
| --- | --- | --- | --- | --- |
| Startup tradeability report | Operator should see current equity, account pin, tradeable symbols, and breaker headroom | F001-F103 | Small | Low |
| Weekly drift report | Live-vs-validation drift should drive retraining and deactivation | F002, F109 | Medium | Low |
| Define Toronto metric if needed | No implementation exists | F004 | Small | Low |

## Suggested First Patch Set

1. Read-only startup/preflight account and tradeability summary.
2. Config-only micro-account risk profile with 8-10% cap and lower secondary cap.
3. Additive MAE/MFE instrumentation.
4. Reporting-only Calmar relabel and validation-Sharpe/PBO warning.

