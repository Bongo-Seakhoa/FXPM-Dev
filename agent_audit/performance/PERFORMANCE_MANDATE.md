# Performance-First Mandate

Source: `prompt.md`  
Canonical starting balance: USD 10,000  
Forbidden in this phase: live trading, full optimization, balance sweeps, risk lowering without direct performance evidence.

## Decision Hierarchy

1. Correctness and fidelity.
2. Trading and economic quality.
3. Code and optimization quality.
4. Efficiency.

## Current Disposition

Proceed with bounded P0 measurement/correctness fixes that directly protect profit attribution and winner selection:

- Canonical metric contract scaffolding and known formula fixes.
- Per-trade MAE/MFE instrumentation in Python and Numba backtest paths.
- Expected-versus-filled live entry slippage attribution.
- Targeted parity and metric tests.

Broader strategy optimization remains bounded and cannot run full sweeps.
