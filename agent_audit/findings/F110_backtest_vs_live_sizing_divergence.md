# F110: The backtest that selects winners does not model the live sizing regime (cap + min-lot block)

## Category
Internal consistency / Quant methodology / Risk

## Severity
High

## Confidence
High (both code paths read directly)

## Evidence
- **Backtest sizing (Numba):** `pm_core.py:338` `risk_amount_at_entry = equity * (position_size_pct/100)` with
  `position_size_pct = discovery_risk_per_trade_pct = 1.0` (`config.json:22`) and `equity = initial_capital
  = 10000` (`config.json:20`). `pm_core.py:361-362` `raw_size = risk_amount/loss_per_lot`,
  `position_size = _round_volume_numba(...)`.
- **`_round_volume_numba` (`pm_core.py:81-102`)**: rounds to step and `if rounded < min_lot: rounded = min_lot`.
  There is **no `max_risk_pct` cap and no "skip trade" path** — every signal is taken, clamped up to min-lot.
- **Live sizing (`pm_main.py:4899-4987`)**: `risk_per_trade_pct = 0.5` on live equity (~$22), min-lot clamp,
  then a hard `max_risk_pct = 25%` cap that **blocks** (`BLOCKED_MIN_LOT_EXCEEDS_CAP` / `SKIPPED_RISK_CAP`).
- Consequence in real logs: `agent_audit/data/signal_risk_events.csv` — 83 live blocks; index/metal signals
  (DE30 362%, PLATINUM 98%) are blocked live but in backtest on $10k the same 0.01-lot trade is ~6% risk and
  is **silently taken**.

## Explanation
Discovery/selection runs at **1.0% of $10,000** with a min-lot clamp that never blocks; live runs at **0.5%
of ~$22** where the min-lot floor dominates and a 25% cap blocks a large fraction of signals. These are
different code paths with different risk %, different balance, and — critically — different cap/block
semantics. So the equity curve, drawdown, profit factor, Sharpe, and trade frequency that *select the
winners* are produced under sizing conditions that do not occur live. The backtester never experiences a
blocked trade, so its trade sequence and compounding differ structurally from live, especially on
high-notional symbols.

This is the internal-consistency root cause behind F103 (universe mismatch) and F108 (transfer gap), and it
interacts with the scaling intent: as the account grows toward $10k the live regime converges *back* toward
the backtest regime — so the backtest is most representative exactly at the target balance, and least
representative now.

## Trading Impact
- Selected winners are optim/validated in a regime that does not match current live execution.
- Backtest drawdown/PF understate live path dependency at small balances (no blocks, clean fractional sizing).
- High-notional winners (indices/metals) are essentially fictional for the live $22 account.

## Suggested Fix or Experiment
1. Add an optional **min-lot-aware backtest mode** that takes the live `equity`, `risk_per_trade_pct`,
   `max_risk_pct`, and min-lot/step, and **records blocked trades** — so discovery can be replayed at the
   live balance (bounded; F108 validation).
2. At minimum, compute and store a per-winner "live-tradeable-at-$X" flag during selection so the ledger
   reflects which winners are actionable at the current balance.
3. Keep discovery at $10k for scaling, but report the divergence explicitly rather than implicitly.

## Estimated Effort
Medium

## Risk of Change
Low (additive analysis mode; does not change selection unless adopted as a gate)

## Validation Plan
Codex: run one bounded EURUSD + one index backtest at `initial_capital=22, risk=0.5, max_risk_pct=25` and
compare trade count, blocked count, DD, PF vs the $10k/1% run.

## Partner Review
Pending (Codex). Reinforces Codex F003 (risk-cap sensitivity) and Claude F103/F108.
