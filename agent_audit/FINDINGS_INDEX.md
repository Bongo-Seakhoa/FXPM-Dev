# Findings Index

Findings are created as `agent_audit/findings/F###_<short_name>.md`.

Codex uses F001+; Claude Code uses F101+.

| ID | Severity | Category | Title | Confidence | Partner Review |
| --- | --- | --- | --- | --- | --- |
| F001 | Medium | MT5/Account | Live account switch updates the MT5 identity pin, not the terminal login | High | Agreed by Claude Code |
| F002 | High | Quant methodology | Tradeable winners rely on tiny validation samples, extreme Sharpe, and unavailable PBO status | High | Agreed by Claude Code; nuance: some Sharpe inflation is the F106 annualization artifact |
| F003 | High | Risk/Recovery | Live risk-cap sensitivity: lower caps preserve part of executed flow while cutting tail exposure | High | Agreed by Claude Code |
| F004 | Informational | Metrics | No explicit Toronto metric exists in the codebase | High | Agreed by Claude Code |
| F005 | Medium | Execution accounting | Backtest/live execution-cost model misses entry and TP slippage attribution | High | **Agreed by Claude Code** — execution-cost sibling of F110/F107; strengthens the backtest-realism theme. Recommend measurement-first (log expected-vs-fill), gate later |
| F101 | Critical | Risk/Execution | Min-lot floor decouples per-trade risk from target on small account | High | Agreed by Codex, with scale-aware framing |
| F102 | Critical | Risk/Architecture | Circuit breakers widened to 40-75% create weak protection at larger balances | High | Agreed by Codex; not the central defect at $22 |
| F103 | High | Risk/Portfolio | 100-symbol universe is mismatched to a $22 account | High | Agreed by Codex; strongest as a tradeability/filtering issue |
| F104 | High | Risk | Per-trade 25% / secondary 50% risk caps permit extreme single-position risk | High | Agreed by Codex; quantify as intentional micro-account aggression plus transition risk |
| F105 | Medium | Accounting/Reporting | `calmar_ratio` is cumulative return divided by max drawdown, not annualized Calmar | High | Agreed by Codex |
| F106 | Medium | Quant methodology | Sharpe/Sortino over per-bar equity including flat bars can bias ratios | Medium | Agreed by Codex |
| F107 | Medium | Data quality | No per-trade MAE/MFE captured, so high-drawdown bucketing is not directly possible | High | Agreed by Codex |
| F108 | High | Quant methodology | Discovery metrics on $10k notional do not transfer directly to $22 live | High | Agreed by Codex |
| F109 | Medium | Optimization | Bleeding-edge split with no pristine clean holdout increases regime-decay risk | Medium | Agreed by Codex |
| F110 | High | Internal consistency | Backtest sizing does not model live sizing cap/block semantics | High | Agreed by Codex; independently verified in code and generated evidence |
| F111 | High | Quant methodology | 50k validation geometry is correct, but validation is a model-selection surface | High | Agreed by Codex; explains F002 |
| F112 | Medium | Accounting/Reporting | Dashboard and selection engine compute same-named metrics with different definitions | High | Agreed by Codex; extended to bucket drawdown differences |
| F113 | Medium | Architecture/Scaling | No explicit account-size risk regimes; $22-to-$10k transition is manual/unowned | High | Agreed by Codex with scale-aware nuance |

## Cross-Review Summary

- Claude Code reviewed Codex F001-F004 and agreed.
- Codex reviewed Claude Code F101-F113 and agreed, with scale-aware reframing for F101-F104/F113.
- Codex added F005 during the corrective second pass; **Claude Code has now reviewed and agreed** (execution-cost sibling of F110/F107; measurement-first, gate later).
- Major converged conclusions: split geometry is implemented correctly, validation is selection-biased, backtest/live sizing semantics diverge at the current account size, dashboard/engine metrics are inconsistent, MAE/MFE is missing, and explicit account-size regimes are needed for scaling.
