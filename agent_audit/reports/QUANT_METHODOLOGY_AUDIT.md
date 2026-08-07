# Quant Methodology Audit

## Evidence Base

- `agent_audit/data/audit_evidence_summary.json`
- `agent_audit/data/ledger_tradeable_cells.csv`
- `agent_audit/data/oos_manifest_summary.csv`
- `pm_core.py`, `pm_pipeline.py`, `pm_dashboard/analytics.py`, `tools/benchmark.py`

## Metric Formula Review

| Metric | Implementation | Audit verdict |
| --- | --- | --- |
| Profit factor | `pm_core.py:5924`, gross wins divided by gross losses | Correct, with `inf` if wins and no losses |
| Win rate | wins divided by total closed trades | Correct; breakevens remain in denominator |
| Expectancy | average win/loss in pips | Correct per symbol, but not directly dollar-comparable across symbols |
| Sharpe | per-bar equity returns annualized by timeframe | Useful, but vulnerable to sparse/flat-bar inflation |
| Sortino | downside version of per-bar annualized return | Same caveat plus degenerate `inf` cases |
| Calmar | cumulative return divided by max drawdown | Mislabeled if interpreted as textbook annualized Calmar |
| Drawdown | floating-aware equity at bar close | Good for account DD, but no per-trade MAE/MFE |
| Toronto | no explicit metric found | Not implemented |

## Winner Ledger Observations

- 157 tradeable cells across 45 symbols.
- 75 tradeable cells have fewer than 20 validation trades.
- 147 tradeable cells have validation Sharpe above 10.
- All 157 tradeable cells have `pbo_status=not_available`.
- Deflated Sharpe is present but mostly saturated near 1.0, so it does not by itself resolve small-sample/model-selection risk.

## OOS and Search-Surface Observations

Across scanned OOS manifest metrics, the broad candidate surface is weak:

- Median profit factor: 0.86.
- Median total return: -0.17%.
- Median Sharpe ratio: -0.05.
- Median total trades: 7.

This does not invalidate the selected winners, but it means selection discipline matters. The profitable edge appears concentrated in selected cells, not broad across the search space.

## Overfitting and Robustness Risks

F002 is the central methodology finding. The current promotion system can allow very high validation Sharpe with low trade counts while PBO is unavailable. The production split is intentionally bleeding-edge, which improves freshness but raises regime-decay risk.

Recommended controls:

1. Require stronger minimum validation trade counts before Sharpe can override robustness.
2. Set `specialist_spokes_v2_max_validation_sharpe` above zero and treat excessive Sharpe with unavailable PBO as severe.
3. Use OOS/PBO manifests as a veto or penalty, not only as documentation.
4. Add a measurement-only pristine-OOS decay report.
5. Recompute representative Sharpe/Sortino on per-trade or in-market returns before trusting extreme annualized values.

## Interpretation of Aggressiveness

The strategy is intentionally aggressive and should not be rejected merely for high growth or high activity. The audit question is whether the aggression is rewarded, controlled, repeatable, and recoverable. Current evidence says:

- Rewarded on demo: plausible.
- Controlled at $22: weak, because min-lot risk dominates.
- Repeatable: uncertain, because validation/PBO evidence is fragile.
- Recoverable: weak without lower caps, absolute-dollar breakers, and MAE/MFE evidence.

