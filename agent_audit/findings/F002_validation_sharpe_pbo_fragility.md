# F002: Tradeable winners rely on tiny validation samples, extreme Sharpe, and unavailable PBO status

## Category
Quant methodology / Overfitting resistance

## Severity
High

## Confidence
High

## Evidence
- `agent_audit/data/ledger_tradeable_cells.csv` contains 157 tradeable cells across 45 symbols.
- `agent_audit/data/audit_evidence_summary.json` flags 75 of 157 tradeable cells with fewer than 20 validation trades, and 147 of 157 with validation Sharpe above 10.
- All 157 tradeable cells have `pbo_status=not_available` in the normalized ledger evidence. Deflated Sharpe exists but is saturated: count 157, average about 0.994, max 1.0.
- `pm_core.py:5975-5991` annualizes per-bar equity returns by `sqrt(bars_per_year)`, which can generate very large Sharpe/Sortino values on quiet equity curves.
- `pm_pipeline.py:4254` documents that validation Sharpe can override robustness. `pm_core.py:597` sets `fx_val_sharpe_override=0.3`.
- `pm_core.py:621` defaults `specialist_spokes_v2_max_validation_sharpe=0.0`, and `pm_pipeline.py:6360-6372` only flags excessive validation Sharpe when that threshold is set above zero.
- `agent_audit/data/oos_manifest_summary.csv` and the JSON summary show the broad OOS candidate surface is weak: OOS median profit factor 0.86, median total return -0.17%, median Sharpe -0.05, and median total trades 7.

## Explanation
The current winners may still be useful, but the promotion surface is fragile. A cell with 15-20 validation trades, annualized per-bar Sharpe above 40, and no available PBO status is not equivalent to a repeatable edge. Deflated Sharpe helps, but if the input Sharpe is already inflated by small samples or sparse equity curves, DSR saturation does not remove the model-selection risk.

## Trading Impact
- The system may over-select cells whose recent validation slice was unusually clean rather than robust.
- This can make the aggressive profile look more rewarded than it really is, especially when moved from demo to a min-lot-constrained live account.
- Weak OOS base-rate evidence means the positive cells need stricter stability filters, not looser live risk.

## Suggested Fix or Experiment
1. Require a higher validation trade count before validation Sharpe can override robustness, such as 30-50 trades depending on timeframe.
2. Enable `specialist_spokes_v2_max_validation_sharpe` as a severe flag when `pbo_status` is unavailable, for example at Sharpe > 10 or > 20.
3. Use OOS/PBO evidence as a veto or penalty for validation-only winners.
4. Recompute one or two representative winners using per-trade or in-market-bar Sharpe to quantify the difference from all-bar annualized Sharpe.

## Validation Plan
- Re-run the collector after any gate changes and compare number of tradeable cells, trade counts, validation PF, and live blocked/executed mix.
- Use bounded per-symbol tests only; do not run the full optimizer.

## Partner Review
Pending.

