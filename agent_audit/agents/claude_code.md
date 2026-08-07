# Agent: Claude Code

- **Identity:** Claude Code (Agent A). Partner agent: Codex (Agent B).
- **Primary bias:** architecture mapping, quant/risk methodology critique, optimization
  methodology, high-drawdown classification framework, strategic synthesis, red-team.
- **Start:** 2026-06-17
- **Mode:** read-only inspection. No live trades, no full optimization, no destructive edits.

## Operating context discovered this session
The operator switched the live target from demo (`FBS-Demo`, login 106165989) to a **real
FBS account (`FBS-Real`, login 28829971) funded with ~US$22** (prior demo balance was ~US$170).
The audit must judge the system *as it will actually behave on a ~$22 live balance*, not on the
$10,000 backtest notional in `config.json`.

## Current hypothesis (confirmed by evidence — see findings)
1. On a ~$22 balance the broker **minimum lot (0.01)** floor dominates position sizing, so
   effective per-trade risk is **9–25%**, not the configured 0.5%. (F001)
2. The institutional supervisor's 1–4% loss breakpoints are incoherent with a 9–25%/trade
   reality: the first floating loss trips `open_drawdown_hard_block_pct` (3%) and latches the
   circuit breaker, so the system effectively halts after ~1 trade. (F002)
3. Therefore the encouraging demo growth (~$170) **does not transfer** to $22 — the risk profile
   is balance-dependent through the lot floor.

## Files inspected
config.json, broker_specs.json, pm_position.py, pm_main.py (sizing + supervisor wiring +
loss/dd estimators), pm_core.py (metrics + backtest equity/DD), pm_institutional_supervisor.py,
pm_live_readiness.py, README.md, SETUP_AND_RUN.md, last_actionable_log.json.

## Handoff to Codex (see TASKS.md)
Independent verification requested for: Numba-vs-Python equity-curve/DD parity, MT5 order-send
retcode handling, `_estimate_stop_loss_amount` / `normalize_volume` correctness, and the
Sharpe/flat-bar bias. Code-level patches are Codex's lane.
