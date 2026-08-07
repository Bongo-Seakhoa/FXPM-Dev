# Executive Summary

Date: 2026-06-17  
Agents: Codex and Claude Code  
Scope: bounded read-only audit. No live trades, no intentional broker connection, no full optimization, no credential edits.

## Bottom Line

The account switch to FBS Real is implemented as a safe identity pin: `config.json` now expects account `28829971` on `FBS-Real`, and readiness checks should block if the active MT5 terminal is still on demo. It does not log the terminal into the real account because login/password/server remain blank. The operator must ensure the MT5 terminal is already on FBS Real before any live trade run.

The bigger audit result is that the promising demo/backtest behavior does not transfer cleanly to the approximately $22 live account. At this balance, broker minimum lot size dominates position sizing. The configured target risk is usually 0.5%, but executed live ledger rows show actual risk median 5.35%, mean 7.58%, and max 24.67%. The system is aggressive by design; the problem is that some of the aggression is mechanical min-lot risk rather than rewarded strategy edge.

## Highest Priority Findings

1. Min-lot risk dominates live sizing: 67 executed rows had median actual risk 5.35% vs 0.5% target; 83 signals were blocked because min-lot risk exceeded caps. See F101 and F003.
2. Current hard stops are too wide for a $22 account: daily/session/weekly/open-drawdown and open-risk thresholds are in the 40-75% range. See F102 and F104.
3. The 100-symbol universe is not realistically tradeable at $22: indices/metals/stocks are mostly blocked at min-lot. See F103.
4. Winner validation is fragile: 75 of 157 tradeable cells have fewer than 20 validation trades; 147 have validation Sharpe above 10; all have `pbo_status=not_available`. See F002.
5. High-drawdown order analysis is blocked by missing MAE/MFE fields. See F107.

## Account Switch Verdict

Pass with operational caveat.

- Correct: expected account pin changed from demo to `FBS-Real / 28829971`.
- Correct: credentials remain blank, so secrets are not stored.
- Correct: readiness code compares actual account identity to expected identity.
- Caveat: the config does not perform the login. The terminal session controls which account is active.
- Caveat: `pm_configs.json` also changed ledger/live-observability state, which is not necessary for the account swap.

## Best Next Actions

P0:
- Choose operating mode for the $22 account: capital preservation, lottery-ticket, or fund-up.
- Add/use a balance-aware tradeable universe so untradeable symbols do not generate live signals.
- Replace or supplement percentage breakers with absolute-dollar stops for a micro account.

P1:
- Test a lower `max_risk_pct` such as 8-10%. A 10% cap would have allowed 48 of 67 executed rows and blocked the 19 riskiest.
- Run a bounded $22-notional replay/backtest for one or two executable symbols.
- Add MAE/MFE capture for high-drawdown trade classification.

P2:
- Enable a severe flag for extreme validation Sharpe when PBO is unavailable.
- Relabel or annualize `calmar_ratio`.
- Add a measurement-only pristine-OOS decay audit beside the bleeding-edge production split.

## Verification

- JSON validation passed for `config.json`, `pm_configs.json`, and `pm_outputs/production_live/signal_ledger_202606.jsonl`.
- Focused pytest run passed: 245 tests and 11 subtests.
- Evidence generated under `agent_audit/data/`.

