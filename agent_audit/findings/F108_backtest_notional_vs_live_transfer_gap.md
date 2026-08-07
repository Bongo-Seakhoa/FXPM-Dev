# F108: Discovery metrics are computed on a $10,000 notional and do not transfer to the $22 live account

## Category
Quant methodology / Risk management (the linchpin strategic finding)

## Severity
High

## Confidence
High

## Evidence
- `config.json:20` — `pipeline.initial_capital: 10000.0`; `pm_core.py:6059-6060` — all `total_return_pct`
  and the equity curve (hence `max_drawdown_pct`, Sharpe, Calmar) are computed on that $10k notional.
- Backtest sizing (`pm_position.py:398-498`) uses risk% of equity with min-lot clamp but a $10k equity, so
  the min-lot floor (F101) almost never binds in discovery — every winner sizes cleanly at ~1%.
- Live reality (F101/F103): on $22 the same winners size at the min-lot floor → 3–6% (FX/crypto) or are
  blocked (indices). The demo ran ~US$170; live is ~US$22.

## Explanation
Every headline number the operator is "happy with" (the demo's ~60% week-on-week growth, the backtest
returns and drawdowns) was generated where the broker lot floor is irrelevant and risk is a clean fraction
of a large balance. At $22 the lot floor dominates and the **per-trade risk profile is a different system**:
fewer tradeable symbols (F103), 6–12× the intended per-trade risk (F101), and near-inert breakers (F102).
The encouraging demo evidence therefore **does not establish** that the $22 live deployment will behave the
same way — its risk and return distribution are governed by mechanics that were dormant in the demo.

## Trading Impact
- Demo/backtest Sharpe, drawdown, and growth are **optimistic proxies** for $22 live behavior, not estimates
  of it. Expected $22 outcome: chunky 3–6%+ swings per trade, concentrated in a few pairs, with drawdown
  governance that only engages near account destruction.
- This reframes "I'm happy with the demo results, only change the account" — the account change itself
  materially changes the system's behavior.

## Suggested Fix or Experiment
1. Run (or have Codex run) a bounded backtest with `initial_capital = 22` on the live tradeable subset to see
   the *real* return/DD distribution at this balance (no full optimization; one or two symbols).
2. Either fund the account to where min-lot ≈ target risk (so demo behavior transfers), or explicitly accept
   and document the micro-account regime (high per-trade variance, few symbols, weak breakers).

## Estimated Effort
Small (bounded backtest) / operator decision (funding)

## Risk of Change
Low (analysis only)

## Validation Plan
Codex: bounded `initial_capital=22` backtest on 1–2 executable FX/crypto symbols; compare return/DD/Sharpe to
the $10k run for the same winners.

## Partner Review
Pending (Codex).
