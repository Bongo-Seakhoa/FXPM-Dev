# Agent: Codex

## Identity
- Agent name: Codex
- Partner agent: PARTNER_AGENT
- Start time: 2026-06-17T13:14:30+02:00

## Current Hypothesis
The codebase has substantial risk and methodology infrastructure already. The highest-value audit work is likely to come from measuring whether the current aggressive profile is rewarded by live/demo evidence, identifying avoidable risk-cap/min-lot/spread/spec drag, and verifying that selection and accounting metrics are aligned with real trade outcomes.

## Planned Inspection Areas
- Current config and account/risk posture.
- Winner ledger and live eligibility/rejection surface.
- Live/actionable/trade logs and signal ledger.
- OOS manifests and benchmark artifacts.
- Backtester/accounting formulas.
- Position sizing, margin protection, min-lot clamp, and risk caps.
- MT5 connector order safety and account identity gates.
- Optimization selection gates, split geometry, OOS/PBO/false-discovery methodology.
- Dashboard analytics and reporting formulas.

## Focus
Code, quant methodology, risk/recovery, execution, accounting, and reporting.

## Safety Notes
- No live trading commands.
- No full optimization.
- No production credential edits.

## Evidence Generated
- Added read-only collector: `agent_audit/scripts/collect_audit_evidence.py`.
- Generated normalized evidence:
  - `agent_audit/data/audit_evidence_summary.json`
  - `agent_audit/data/ledger_tradeable_cells.csv`
  - `agent_audit/data/ledger_symbol_summary.csv`
  - `agent_audit/data/signal_risk_events.csv`
  - `agent_audit/data/signal_risk_action_summary.csv`
  - `agent_audit/data/risk_cap_sensitivity.csv`
  - `agent_audit/data/signal_risk_symbol_action_summary.csv`
  - `agent_audit/data/log_notable_events.csv`
  - `agent_audit/data/oos_manifest_summary.csv`

## Key Verified Facts
- Account switch: `config.json` now pins `expected_login=28829971`, `expected_server=FBS-Real`, `expected_currency=USD`. MT5 login credentials remain blank, so routing depends on the already-open MT5 terminal session and readiness identity checks.
- Live signal ledger: 297 rows from 2026-06-08 through 2026-06-17, including 67 `EXECUTED`, 83 `BLOCKED_MIN_LOT_EXCEEDS_CAP`, 65 `BLOCKED_INSTITUTIONAL_SUPERVISOR`, and 33 `BLOCKED_CYCLE_DRIFT`.
- Executed risk distribution: actual risk median 5.35%, mean 7.58%, max 24.67%; target risk median 0.5%. Of 67 executed rows, 19 exceeded 10% actual risk and 6 exceeded 20%.
- Cap sensitivity: a 10% cap would have allowed 48 of 67 executed rows and blocked 19; a 5% cap would have allowed 32 and blocked 35.
- Tradeable ledger: 157 tradeable cells across 45 symbols; 75 cells have fewer than 20 validation trades; 147 cells have validation Sharpe above 10; all tradeable cells have `pbo_status=not_available`.
- OOS manifests: broad candidate surface median profit factor 0.86, median total return -0.17%, median Sharpe -0.05, median total trades 7.
- Toronto metric: no source matches outside `agent_audit/`.

## Partner Cross-Review
- F101: Agreed. Codex evidence strengthens it: executed median actual risk is 5.35% and max is 24.67%, not just the smaller examples in the first draft.
- F102: Agreed. Config thresholds are 40-75% scale and weak for a $22 balance. Current supervisor latching is active in the signal ledger, but mainly after risk is already elevated.
- F103: Agreed. Min-lot blocks concentrate in XAUGBP, US100, DE30, US30, metals, and some high-stop FX contexts.
- F104: Agreed. F003 adds the activity/safety trade-off at candidate caps.
- F105: Agreed. `calmar_ratio` is cumulative return divided by max drawdown, not annualized return divided by max drawdown.
- F106: Agreed with medium confidence. Static code confirms all-bar annualized equity returns; a per-winner recomputation would quantify the exact bias.
- F107: Agreed. MAE/MFE fields are absent from trade records, so high-drawdown-order classification is not directly possible from current artifacts.
- F108: Agreed. Backtest/discovery notional is $10k while live is about $22; min-lot behavior changes the system.
- F109: Agreed. Bleeding-edge split is intentional, but current PBO status is unavailable for live tradeable cells and OOS base rates are weak.

## Validation Completed
- JSON validation passed for `config.json`, `pm_configs.json`, and `pm_outputs/production_live/signal_ledger_202606.jsonl`.
- Focused pytest command passed: 245 tests and 11 subtests across metric, pipeline, margin, sizing, live loop, MT5 connector, dashboard, config source-of-truth, and readiness modules.

## Not Validated
- No live broker connection was intentionally opened.
- No live orders were placed, modified, or closed.
- No full optimization or unbounded backtest was run.
- Broker-session-only behavior such as real requotes, fills, or partial fills was not empirically tested.

## Corrective Second Pass - 2026-06-17

### Reframed Risk Position
- The USD 22 account is treated as intentional micro-account growth mode.
- High percentage risk is not the main defect by itself.
- The main audit question is whether the code measures, selects, executes, and scales that aggression consistently.

### Added Evidence
- Added read-only script: `agent_audit/scripts/deep_consistency_audit.py`.
- Added HTML renderer: `agent_audit/scripts/render_audit_html.py`.
- Generated:
  - `agent_audit/data/deep_consistency_summary.json`
  - `agent_audit/data/deep_split_geometry.csv`
  - `agent_audit/data/deep_metric_definition_mismatches.csv`
  - `agent_audit/data/deep_risk_symbol_actions.csv`

### Second-Pass Verified Facts
- Bleeding-edge split geometry is correct. For 400k input bars, WFV is `224700-274700`, embargo is `274700-275000`, optimization train is `275000-400000`, and clean holdout is empty.
- The 50k validation window is used as a validation/selection surface. It is not pristine post-selection OOS.
- Current `signal_risk_events.csv` subset has 67 executed rows and 84 blocked/skipped risk-cap rows; blocked-risk median is 37.86%, max 362.06%.
- Tradeable-cell evidence still shows 157 rows, 75 with <20 validation trades, 147 with validation Sharpe >10, all `pbo_status=not_available`.
- OOS manifest inventory covers 93 manifests with metric keys including PF, win rate, Sharpe, Sortino, Calmar, return, DD, expectancy, and R metrics.
- Dashboard and engine metric definitions diverge for PF, expectancy, Sharpe, Sortino, drawdown basis, and recovery.
- Backtester slippage is applied only to stop exits; live logs actual fill price but do not persist expected-vs-fill slippage fields.

### Partner Cross-Review Update
- F110: Agreed. Backtest sizing does not model live USD 22 cap/block semantics.
- F111: Agreed. Split math is correct; validation metrics are selection-biased.
- F112: Agreed. Same-named dashboard and engine metrics are not equivalent.
- F113: Agreed with scale-aware nuance. USD 22 aggression is intentional; missing automated transition regimes are the problem.
- F005: Added by Codex and agreed by Claude Code. Partner framed it as the execution-cost sibling of F110/F107 and recommended measurement-first, gate later.

### Second-Pass Validation
- `python -m pytest tests\test_data_splitter.py tests\test_position_sizing_edge_cases.py tests\test_dashboard_trade_enrichment.py -q` -> 35 passed.
- `python -m pytest tests\test_optimizer_hardening.py -q` -> 75 passed, 2 Optuna experimental warnings.
- Required report sections verified with `rg`.

### Reports Updated
- `agent_audit/reports/TECHNICAL_AUDIT.md`
- `agent_audit/reports/TECHNICAL_AUDIT.html`
- `agent_audit/reports/STRATEGIC_AUDIT.md`
- `agent_audit/reports/STRATEGIC_AUDIT.html`
