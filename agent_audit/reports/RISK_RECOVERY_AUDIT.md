# Risk and Recovery Audit

## Evidence Base

- `agent_audit/data/signal_risk_events.csv`
- `agent_audit/data/signal_risk_action_summary.csv`
- `agent_audit/data/risk_cap_sensitivity.csv`
- `agent_audit/data/signal_risk_symbol_action_summary.csv`
- `config.json`, `pm_position.py`, `pm_main.py`, `pm_institutional_supervisor.py`

## Live Risk Reality

The configured target risk is often 0.5%, but the live signal ledger shows actual post-normalization risk is much higher:

| Action | Rows | Actual risk median | Actual risk mean | Actual risk max |
| --- | ---: | ---: | ---: | ---: |
| EXECUTED | 67 | 5.35% | 7.58% | 24.67% |
| BLOCKED_MIN_LOT_EXCEEDS_CAP | 83 | 37.67% | 54.93% | 362.06% |

Of the 67 executed rows, 57 exceeded 2x target, 47 exceeded 5x target, 34 exceeded 10x target, and 19 exceeded 20x target. This is not just "aggressive strategy"; it is broker min-lot mechanics overwhelming the target risk.

## Risk Cap Sensitivity

The current 25% cap allows all 67 executed rows in the audited ledger. Candidate alternatives:

| Cap | Would allow | Would block |
| ---: | ---: | ---: |
| 5% | 32 | 35 |
| 8% | 41 | 26 |
| 10% | 48 | 19 |
| 15% | 55 | 12 |
| 20% | 61 | 6 |
| 25% | 67 | 0 |

A 10% cap is a useful first experiment: it keeps most flow while removing the riskiest tail.

## Symbol and Universe Risk

Min-lot blocks concentrate in symbols that are structurally too large for the account:

- XAUGBP: 35 blocked min-lot rows, median actual risk 39.15%.
- US100: 23 blocked rows, median 33.11%.
- DE30: 12 blocked rows, median 56.47%, max 362.06%.
- US30, XAUEUR, PLATINUM, XAUUSD, TESLA also show severe min-lot risk.

At $22, the effective universe is much smaller than the configured 100 symbols. This reduces diversification and creates log/signal noise.

## Supervisor and Recovery

The institutional supervisor and cycle drift controls are active:

- 65 `BLOCKED_INSTITUTIONAL_SUPERVISOR` rows.
- 33 `BLOCKED_CYCLE_DRIFT` rows.

These are useful protection layers, but current hard thresholds are very wide for a $22 account. A block that triggers after 40-75% risk/drawdown does not provide much recovery headroom.

## Recovery Recommendations

1. Add a balance-aware universe filter before live eligibility.
2. Lower `position.max_risk_pct` to 8-10% for preservation mode; leave 25% only for explicit lottery mode.
3. Convert micro-account breaker limits to absolute dollars or add absolute-dollar overlays.
4. Add a startup summary: current equity, expected account, tradeable symbols at min-lot, and breaker headroom.
5. Add MAE/MFE capture so high-drawdown winners are not accidentally removed before they are understood.

