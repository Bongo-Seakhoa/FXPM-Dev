# F003: Lower live risk caps would reduce tail exposure while preserving part of the executed flow

## Category
Risk management / Recovery

## Severity
High

## Confidence
High

## Evidence
- `config.json:285-288` sets `position.max_risk_pct=25.0`, `risk_tolerance_pct=25.0`, and `allow_min_lot_risk_clamp=true`.
- `pm_main.py:4933-4982` recomputes actual post-normalization risk and blocks only when the hard cap is exceeded.
- `agent_audit/data/signal_risk_action_summary.csv` shows 67 executed rows with actual risk median 5.35%, mean 7.58%, p75 10.49%, and max 24.67%. The target risk median was 0.5%.
- The same file shows 57 of 67 executed trades were above 2x target, 47 above 5x target, 34 above 10x target, and 19 above 20x target.
- `agent_audit/data/risk_cap_sensitivity.csv` shows a 10% cap would have allowed 48 of 67 executed trades and blocked 19; an 8% cap would have allowed 41; a 5% cap would have allowed 32.
- 83 `BLOCKED_MIN_LOT_EXCEEDS_CAP` rows had actual risk median 37.67%, mean 54.93%, and max 362.06%.

## Explanation
The current 25% hard cap is the only thing standing between min-lot drift and execution. It blocks the most extreme index/metal cases, but it also permits trades risking close to a quarter of the account. The cap sensitivity table shows a less extreme cap does not necessarily shut the system off: a 10% cap would preserve most executed trades while cutting the riskiest tail.

## Trading Impact
- Current risk settings maximize activity but make recovery fragile after one or two adverse trades.
- A lower cap would reduce the probability of account-level damage while still allowing a meaningful subset of the live flow.
- This is a better first adjustment than changing strategy logic because it targets unrewarded mechanical risk from min-lot sizing.

## Suggested Fix or Experiment
1. Test a live-profile cap of 8-10% for the micro account; reserve 25% only for explicit lottery-ticket mode.
2. Pair the cap change with a balance-aware universe filter so blocked signals are removed from the live workload.
3. Recompute `risk_cap_sensitivity.csv` after the next week of demo/live-forward data.

## Validation Plan
- Replay the signal ledger with candidate caps of 5%, 8%, 10%, 15%, and 20%.
- Compare blocked-trade opportunity cost against drawdown reduction using realized trade outcomes once available.

## Partner Review
Pending.

