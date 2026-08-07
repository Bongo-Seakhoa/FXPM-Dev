# F102: Circuit-breaker thresholds widened to 40–75% leave almost no real protection on a $22 account

## Category
Risk management / Architecture

## Severity
Critical

## Confidence
High (config + supervisor code; independently verified from `config.json`)

## Evidence
- `config.json:149-166` (verified directly, not just via Codex summary):
  `max_total_open_risk_pct 75`, `max_new_entries_when_total_open_risk_above_pct 50`,
  `daily_loss_hard_block_pct 50`, `session_loss_hard_block_pct 40`, `weekly_loss_hard_block_pct 60`,
  `open_drawdown_hard_block_pct 50`, `margin_entry_block_level 60`, `live_min_account_equity 20`.
- `pm_institutional_supervisor.py:105-203` — the supervisor blocks/latches only when a context value
  `>= threshold`. With thresholds at 40–75%, it does not fire until the account is already devastated.
- `pm_main.py:2824-2884` — `_estimate_open_drawdown_pct` = `(balance-equity)/balance×100`;
  `_estimate_realized_loss_pct` = realized losses ÷ balance over the period. Both are account-relative.
- Note: README.md documents these as `2/1/4/3%` — that is a **stale example**; the live config is 40–75%.

## Explanation
On a ~$22 balance the intended retail-scale breakers (a few %) would trip on the first min-lot trade's
floating loss (F101), so they appear to have been widened to 40–75% to keep the account trading. The
side effect is that the institutional supervisor — the system's primary hard stop — now permits a
**~50% daily loss, ~60% weekly loss, and ~50% open drawdown** before doing anything. With
`live_min_account_equity 20`, a $22 account has roughly **$2 of usable headroom** before the readiness
floor, yet a single tolerated trade (F101/F104) can lose far more than $2 inside policy.

## Trading Impact
- The circuit breaker provides effectively **no meaningful capital protection** at this balance.
- The account can be halved in a day, or driven to the $20 floor, without any hard block engaging.
- This is the mirror image of F101: the two known ways to run this system on $22 are "halt after one
  trade" (tight breakers) or "no real protection" (current 40–75% breakers). Neither is a healthy state.

## Suggested Fix or Experiment
1. Decide the **operating intent** for $22 (see QUESTIONS.md): capital-preservation vs. lottery-ticket.
2. If preservation: set breakers to absolute-dollar stops appropriate to $22 (e.g. halt at −$3 to −$5
   cumulative) rather than percentages calibrated for a $10k book, AND restrict the universe (F103) so
   per-trade risk fits — otherwise tight breakers just halt immediately.
3. If lottery-ticket: keep as-is but document explicitly that no drawdown governance is active.

## Estimated Effort
Small (threshold change) / Medium (dollar-based breaker mode)

## Risk of Change
Low–Medium (tighter breakers may halt trading frequently at this balance — that is the real trade-off)

## Validation Plan
Codex: from broker deal history, reconstruct the realized daily/weekly loss series the supervisor would
have seen and confirm whether any block would have fired under both old (2/1/4/3) and current (40–75) configs.

## Partner Review
Pending (Codex).
