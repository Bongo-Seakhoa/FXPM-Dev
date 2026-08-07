# F106: Sharpe/Sortino computed over per-bar equity including flat (no-position) bars

## Category
Quant methodology / Accounting

## Severity
Medium

## Confidence
Medium

## Evidence
- `pm_core.py:5950-5999` — Sharpe/Sortino from `bar_returns = diff(equity_curve)/equity[:-1]` over the full
  curve, annualized by `sqrt(bars_per_year)` (`_TF_BARS_PER_YEAR`, `pm_core.py:5873-5877`).
- `pm_core.py:5803-5816` — equity is appended every bar, including bars with no open position (return 0).
- `pm_core.py:5997-5999` — Sortino returns `float('inf')` when there are zero negative bar-returns and mean>0;
  single-negative case uses one observation as the downside deviation proxy.

## Explanation
Including many flat bars (typical for selective strategies) makes per-bar mean and std both small but leaves
the annualization factor `sqrt(bars_per_year)` large, because it assumes **every bar is an independent bet**.
For infrequent traders this tends to **overstate** the annualized Sharpe/Sortino and makes the ratio
sensitive to how often the strategy is in-market rather than to trade quality. The degenerate `inf`/single-
observation Sortino cases can further flatter low-trade-count candidates.

## Trading Impact
- Risk-adjusted ranking can favor strategies that are in-market less (more zeros → smaller std) rather than
  strategies with genuinely better trades.
- Cross-timeframe comparison is affected because `bars_per_year` differs by an order of magnitude (M5 72,576
  vs D1 252), so the same dollar edge annualizes very differently.

## Suggested Fix or Experiment
- Prefer **per-trade** Sharpe/Sortino (or returns sampled only on in-market bars) for selection, or document
  that the equity-curve Sharpe is in-market-frequency-dependent. Trade-count gates (`regime_min_val_trades`,
  deflated Sharpe in `pm_pipeline.py:3812`) partially mitigate but do not remove the bias.

## Estimated Effort
Medium

## Risk of Change
Medium (affects selection scores; must be re-validated, not hot-swapped)

## Validation Plan
Codex (bounded): for one existing winner artifact, recompute Sharpe on (a) all bars vs (b) in-market bars vs
(c) per-trade returns and report the spread. No full optimization.

## Partner Review
Pending (Codex).
