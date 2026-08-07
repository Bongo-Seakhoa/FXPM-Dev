# F105: `calmar_ratio` is cumulative-return ÷ max-DD, not an annualized Calmar (mislabeled/overstated)

## Category
Accounting / Reporting

## Severity
Medium

## Confidence
High

## Evidence
- `pm_core.py:6059-6061`:
  `total_return_pct = (final_equity - initial_capital)/initial_capital*100`;
  `calmar = total_return_pct / max_dd`.
- `total_return_pct` is the **cumulative** return over the whole backtest window, not annualized.

## Explanation
A textbook Calmar ratio is **annualized return ÷ max drawdown**. Here the numerator is the full-sample
cumulative return, so over a multi-year window the reported "Calmar" is inflated by roughly the number of
years (e.g. a 3-year 60% cumulative / 20% DD reports 3.0 where true annualized ≈ 0.85). It is effectively a
**return-to-DD / recovery-factor** measure, not Calmar.

## Trading Impact
- Cross-strategy ranking within a single fixed window is preserved (all use the same window length), so
  **selection is not corrupted**.
- But any **absolute** interpretation of "Calmar" (investor reporting, dashboards, cross-window comparison)
  overstates risk-adjusted quality.

## Suggested Fix or Experiment
- Either rename the field to `return_dd_ratio`/`recovery_factor`, or annualize the numerator using the
  same `bars_per_year` table already present at `pm_core.py:5873-5877`.

## Estimated Effort
Small

## Risk of Change
Low (relabel) / Medium (changing the number affects any downstream threshold that reads `calmar_ratio`)

## Validation Plan
Codex: grep selection/promotion gates for `calmar_ratio` usage before changing the value; relabel if it
feeds any gate.

## Partner Review
Pending (Codex).
