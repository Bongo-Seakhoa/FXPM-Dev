# FxPM Audit — Repository Map (Phase 1 Cartography)

Author: Claude Code (Agent A). Companion to Codex's evidence pack in `agent_audit/data/`.

FxPM ("FX Portfolio Manager v3.1") is a regime-aware, 100-strategy MT5 trading system with a
Numba backtester, stateful Optuna optimization, a winner ledger, and a live execution loop.

## Core modules (root)
| File | Role | Audit relevance |
|---|---|---|
| `pm_main.py` (~6.7k lines) | CLI entrypoint + **live trading loop** | Live sizing, supervisor wiring, spread overlay, secondary-trade caps |
| `pm_core.py` (~6k lines) | Config, features, **backtester**, **metric calc** | Sharpe/Sortino/Calmar/expectancy (`_calculate_metrics` ~5879), equity/DD (~5803) |
| `pm_position.py` | Position sizing + trade-tag encode/decode | `calculate_position_size` (~398) min-lot clamp logic |
| `pm_mt5.py` (~1.1k) | MT5 connect, broker metadata, order send | Order safety, `normalize_volume`, account identity |
| `pm_pipeline.py` | Optimization pipeline, ledger, portfolio orchestration | Split geometry, deflated Sharpe, fold PF |
| `pm_strategies.py` | 100 strategy classes + indicators | Strategy roster (33 trend / 39 mean-rev / 28 breakout) |
| `pm_regime.py` | Score-based 4-regime detector (TREND/RANGE/BREAKOUT/CHOP) | Regime labeling, hysteresis |
| `pm_optuna.py` | Optuna TPE integration | Hyperparameter search |
| `pm_institutional_supervisor.py` | Hard-breakpoint circuit breaker | **F102** — block thresholds |
| `pm_cycle_drift.py` | Lightweight per-cell drift sentinel | Degraded-context pausing |
| `pm_order_governance.py` | Post-selection order-management policy | Governance policy selection |
| `pm_live_readiness.py` | Fail-closed funded-live startup gate | MT5 identity pinning (`expected_login`/`server`) |
| `pm_regime_tuner.py`, `pm_regime_audit.py` | Regime param tuning / evidence | — |
| `pm_event_context.py`, `pm_enhancement_seams.py` | Event calendar, risk-scalar/spread seams | Shadow-mode risk scalars, spread overlay |
| `pm_storage.py`, `pm_symbol.py`, `pm_fairness_audit.py` | Storage governance, symbol specs, fairness | — |

## Config & state
| File | Role |
|---|---|
| `config.json` | **Primary config** (pipeline/position/mt5/instrument specs). MT5 pinned to `FBS-Real`/`28829971`. |
| `broker_specs.json` | Per-symbol broker metadata (min/max/step lot, tick value, contract size) |
| `pm_configs.json` | **Winner ledger** (validated winners per symbol×TF×regime, promotion status, evidence) |
| `regime_params.json` | Tuned regime thresholds |
| `last_actionable_log.json`, `last_trade_log.json` | Latest per-symbol decision feed + throttle state |
| `logs/production_live/` | Live runtime logs, supervisor/drift state |
| `pm_outputs/production_live/` | OOS evidence, feature cache, retrain artifacts |

## Data flow
1. **Optimize:** `pm_main.py --optimize` → `pm_pipeline.py` loads newest 300k M5 bars (oldest 50k = stress audit, newest 250k = active universe), Stage 1 eligibility → Stage 2 Top-K/Optuna on newest half (bleeding edge) → adjacent 50k = WFV → winners + promotion status written atomically to `pm_configs.json`.
2. **Trade:** `pm_main.py --trade` → live-readiness gate → bar-triggered loop → features+regime → exact `(timeframe, regime)` winner lookup → sizing (risk%, min-lot clamp, broker constraints) → spread overlay → supervisor + cycle-drift → margin protection → `pm_mt5` order send.

## Key entry points for the audit
- Live sizing & caps: `pm_main.py:4466-5015`
- Metric computation: `pm_core.py:5879-6260`
- Backtest equity/DD: `pm_core.py:5803-5816`
- Min-lot clamp: `pm_position.py:436-525`
- Circuit breaker: `pm_institutional_supervisor.py:95-203`; thresholds `config.json:149-166`

See `FINDINGS_INDEX.md` for findings, `reports/` for the technical & strategic audits.
