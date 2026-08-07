# F107: No per-trade MAE/MFE captured — the operator's high-drawdown classification cannot be done directly

## Category
Data quality / Reporting

## Severity
Medium (High relative to the operator's explicitly stated goal of reducing high-drawdown orders)

## Confidence
High

## Evidence
- `pm_core.py:5780-5800` — the backtest trade record stores `entry/exit price`, `pnl_pips/dollars`,
  `r_multiple`, `exit_reason`, `governance_policy` — but **no maximum adverse/favorable excursion**.
- `pm_position.py:995-1018` — `to_trades_list()` (live/manager path) likewise has no MAE/MFE fields.
- `pm_core.py:5803-5816` — equity DD is tracked at **bar close** (floating-aware, good) but the worst
  intrabar excursion per trade is not retained, and it is account-level not per-trade.
- Grep for `mae|mfe|excursion` in `pm_core.py` → no matches.

## Explanation
The operator specifically wants to find and reduce "high-drawdown orders." Doing that properly requires
per-trade **Maximum Adverse Excursion** (how far underwater a trade went before resolving) so trades can be
bucketed into *profitable pain* vs *avoidable bad risk*. That field is not recorded anywhere, so the
analysis the operator wants cannot currently be produced from the artifacts — only realized PnL and R are
available, which hide how much heat a winning trade took.

## Trading Impact
- Cannot distinguish "won but sat at −2R for hours" from "clean win," so MAE-based filters, time-stops, or
  stop redesigns cannot be evidence-driven.
- The good news (positive sub-finding): account-level reported max drawdown **is** floating-aware at bar
  resolution (`current_equity = equity + unrealized`), so headline DD is not understated by closed-trade-only
  marking — confirm Numba parity (Codex).

## Suggested Fix or Experiment
1. Add `mae_pips`, `mfe_pips`, `mae_r`, `mfe_r` to the backtest trade record (track running min/max of the
   open position's excursion inside the existing per-bar loop).
2. Backfill an MAE/MFE report for existing winners and build the bucket taxonomy (profitable pain / bad risk
   / oversized / poor exit / regime mismatch / execution-driven) the operator asked for.

## Estimated Effort
Medium

## Risk of Change
Low (additive instrumentation; does not change trade logic)

## Validation Plan
Codex: implement MAE/MFE capture in the Python backtest loop, run one bounded EURUSD backtest, and confirm
MAE ≥ realized loss for losers and MAE ≤ 0 ≤ MFE bounds hold.

## Partner Review
Pending (Codex). This is a strong candidate for a Codex patch (`patches/`).
