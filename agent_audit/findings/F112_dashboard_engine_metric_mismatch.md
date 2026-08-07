# F112: Dashboard and selection-engine compute the same metrics with different formulas, units, and edge cases

## Category
Accounting / Reporting / Internal consistency

## Severity
Medium

## Confidence
High (both implementations read directly)

## Evidence
| Metric | Engine (`pm_core.py`) | Dashboard (`pm_dashboard/analytics.py`) | Mismatch |
|---|---|---|---|
| Sharpe | per-**bar** equity returns × √`bars_per_year` (`:5975-5981`) | per-**trade** PnL `mean/std × √252` (`:644`) | Different basis & annualization |
| Sortino | per-bar downside × √bars/yr (`:5983-5999`) | per-trade downside × √252 (`:653`) | Different basis |
| Profit factor | `inf` when no losses (`:5924`) | `0.0` when no losses (`:626`) | Opposite sentinel — a flawless cell reads 0.0 in the dashboard |
| Expectancy | **pips** (`:5937`) | **dollars** = `avg_trade_pnl` (`:665`) | Different unit and definition |

## Explanation
The dashboard re-derives performance metrics independently of the engine that actually selects winners, and
the two disagree on basis (per-trade vs per-bar), annualization (√252 vs √bars_per_year), units (pips vs $),
and the no-loss edge case (0.0 vs inf). An operator comparing dashboard numbers to ledger/selection metrics is
comparing different quantities. The PF sentinel is the most dangerous: a zero-loss cell shows `profit_factor =
0.0` in the dashboard, which looks like the *worst* possible value rather than the best.

## Trading Impact
- Operator-facing reporting can contradict the engine's own selection metrics, undermining trust and
  monitoring (e.g. weekly drift checks would compare incompatible Sharpe definitions).
- Risk of mis-deciding to deactivate a good cell because the dashboard PF reads 0.0.

## Suggested Fix or Experiment
1. Make the dashboard import/reuse the engine's metric functions, or document explicitly that dashboard
   metrics are a separate per-trade view with different definitions.
2. Fix the PF no-loss sentinel to match the engine (`inf`/`None`), not `0.0`.
3. Align Sharpe/Sortino basis or label them distinctly (`sharpe_per_trade` vs `sharpe_equity`).

## Estimated Effort
Small–Medium

## Risk of Change
Low (reporting only; confirm no gate reads the dashboard values)

## Validation Plan
Codex: pick one winner cell, compute Sharpe/PF/expectancy both ways, and confirm the magnitude of divergence.

## Partner Review
Pending (Codex).
