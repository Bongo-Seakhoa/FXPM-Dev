# Experiment Queue

All experiments must use USD 10,000 as starting balance and must not run the full optimizer unless separately approved.

Status update 2026-06-18: PEXP-001 tooling exists and the replay enumerator has been corrected after Claude review. The current `pm_configs.json` contains one logical promoted winner, `FORD H4 CHOP DoubleEMATrendStrategy`, and its USD 10,000 replay loses `-23.18%` with PF `0.66`. PEXP-004 core metric contract is implemented; PEXP-006 shadow-audit tooling exists but objective mutation remains open.

| ID | Experiment | Profit Mechanism | Data Needed | Command Scope | Accept Criteria |
|---|---|---|---|---|---|
| PEXP-000 | Current-ledger completion check | Prevents deploying an interim or sparse loser book. | Current `pm_configs.json`, optimizer/export logs, current-ledger inventory. | Read-only file/log review. | Establish whether the one-winner ledger is final or mid-reoptimization; no live use until resolved. |
| PEXP-001 | Promoted-cell replay with MAE/MFE capture | Identify high-MAE losers and low-MAE winners by strategy/symbol/regime. | Existing promoted ledger + historical bars. | Bounded per-symbol or top-N family replay. | Produces per-trade `mae_r`, `mfe_r`, underwater bars, unchanged trade count semantics. |
| PEXP-002 | MAE/MFE filter simulation | Remove unrewarded drawdown while preserving winners. | PEXP-001 trade records. | Offline CSV simulation only. | Improves PF/Calmar/recovery without cutting net profit materially. |
| PEXP-003 | Entry slippage stress by family/session | Detect strategies whose edge is fragile to entry cost. | Live actionable fills after new fields accumulate; historical stress proxy before then. | No broker calls; parse logs. | Identifies symbol/session/family groups where adverse slippage consumes expected R. |
| PEXP-004 | Dashboard/core metric parity audit | Prevent selection/reporting divergence. | Existing metric tests and code scan. | Unit tests and static diff only. | Engine, dashboard, benchmark, and pipeline agree on no-loss PF and naming. |
| PEXP-005 | DayOfWeekBias and EMA family concentration review | The ledger is concentrated in two strategy families; confirm this is rewarded concentration. | `strategy_family_summary.csv` + bounded replay. | Top two families only. | Concentration improves or maintains risk-adjusted return versus diversified control. |
| PEXP-006 | Validation-selection bias penalty study | Keep bleeding-edge philosophy but penalize unstable validation-driven winners. | OOS manifests + replay results. | Offline ranking experiment, no optimizer. | Better OOS/live proxy stability at similar return. |
| PEXP-007 | Strategy/regime compatibility shadow score | Avoid promoting structurally mismatched cells such as trend logic in `CHOP`. | Current and historical promoted cells with strategy family/regime labels. | Offline score/report only. | Flags mismatches that replay poorly without suppressing profitable exceptions. |
