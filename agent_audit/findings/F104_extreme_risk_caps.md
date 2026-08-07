# F104: Per-trade (25%) and secondary (50%) risk caps permit extreme single-position risk

## Category
Risk management

## Severity
High

## Confidence
High

## Evidence
- `config.json:285` — `position.max_risk_pct: 25.0`; `config.json:286` — `risk_tolerance_pct: 25.0`.
- `config.json:161` — `secondary_trade_max_risk_pct: 50.0`; `config.json:149` — `max_total_open_risk_pct: 75.0`.
- `pm_main.py:4934-4987` — a trade is allowed whenever `actual_risk_pct <= max_risk_pct`; only `>25%` blocks.
- `pm_main.py:4639-4707` — secondary same-symbol trades are capped at `secondary_trade_max_risk_pct` then
  by `max_combined_risk_pct`/available; combined same-symbol can reach the 50–75% band.

## Explanation
The hard cap that backstops the min-lot clamp (F101) is set to **25% per trade**. On $22 that authorizes a
single trade to risk **$5.50**, and the secondary/combined caps authorize same-symbol stacks up to
**$11–$16.50**. `risk_tolerance_pct: 25` also widens the "within tolerance" band so large min-lot drift is
not even warned. These values make sense only as deliberate small-account permissiveness, but they remove
the per-trade guardrail that would otherwise catch oversized min-lot trades.

## Trading Impact
- One adverse trade can remove a quarter of the account inside policy; two correlated ones, half.
- Interacts with F102: neither the per-trade cap (25%) nor the daily breaker (50%) stops ordinary ruin.

## Suggested Fix or Experiment
- Set `max_risk_pct` to the largest single-trade loss the operator will accept on $22 in absolute terms
  (e.g. 5–8%), accepting that this will block more min-lot trades (consistent with F103 universe pruning).
- Reduce `secondary_trade_max_risk_pct` well below `max_risk_pct`; a 50% secondary cap on a micro account
  is not defensible.

## Estimated Effort
Small (config)

## Risk of Change
Low–Medium (more trades blocked at this balance — expected and desirable)

## Validation Plan
Codex: tabulate how many historical EXECUTED trades in `signal_risk_events.csv` would have been blocked at
`max_risk_pct ∈ {5,8,10}%` to quantify the activity/safety trade-off.

## Partner Review
Pending (Codex).
