# FxPM 1.4 Technical Upgrade - Archived Progress Summary

> Archive note (2026-03-30): this file is retained as a historical implementation summary.
> Final completion status lives in `technical_upgrade_checklist.md` in the same archive folder.

## Final Status

- Single integrated upgrade completed across research kernel, validation/search, strategy layer, live runtime, dashboard, optimization seams, and documentation.
- Final audited codebase includes the 5 added strategies, bringing the live registry to **47** strategies.
- Final verification pass: `python -m unittest discover -s tests -p "test*.py"` -> `Ran 98 tests ... OK`

## Core Implemented Outcomes

- `A2`: Python and Numba backtest loops now share the same same-bar gap-through-stop and fill semantics.
- `B8`: Regime tuning now uses a warmup-aware train/holdout contract and blended train/holdout quality scoring instead of holdout-only scoring on short slices.
- `B10`: Deflated Sharpe Ratio is implemented in `pm_pipeline.py` and now uses effective search breadth with Sharpe-only confidence deflation.
- `B11`: Regime-local rescue remains in place, but now requires acceptable regime profit factor, return, drawdown, and minimum regime trades.
- `D1`: Dead `LiveTrader.start()` surface removed.
- `D4`: Live tradability checks now rely on real MT5 `visible` / `trade_mode` metadata carried through the connector.
- `D6`: R-multiple preservation on SL widening remains implemented.
- `D11`: Runtime version strings are aligned to `v3.1`.
- `E7`: Drawdown Duration, Recovery Time, and Ulcer Index are implemented in analytics, exposed in the frontend, and preserved in the no-data schema.
- `F1`: Legacy production instantiation of selector/optimizer/validator surfaces remains retired.
- `G1-G5`: Enhancement seams are real, with live wiring for the risk scalar stack and spread-aware execution overlay.
- `G7`: `VortexTrendStrategy`, `TRIXSignalStrategy`, `RelativeVigorIndexStrategy`, `VIDYABandTrendStrategy`, and `ChoppinessCompressionBreakoutStrategy` are implemented and registered.
- `X1`: ATR percentile self-reference bias fix remains implemented.
- `X2`: Unstable `hash()`-based and hardcoded seeding has been replaced by deterministic context-derived stable seeds.

## Post-Audit Hardening Added After The Main Upgrade Pass

- Live risk overlays now use a real tracked equity peak instead of a balance proxy.
- Volatility targeting now uses ATR as a fraction of price rather than ATR divided by account equity.
- Spread-aware execution now uses recent spread medians from live feature history where available, not only static instrument averages.
- MT5 tradability metadata is tested directly in `tests/test_mt5_connector.py`.
- Optimizer hardening is covered by targeted tests in `tests/test_optimizer_hardening.py`.
- Regime warmup exclusion is covered in `tests/test_regime_warmup_exclusion.py`.
- Analytics no-data schema stability is covered in `tests/test_dashboard_trade_enrichment.py`.
- Strategy integration coverage now explicitly includes the 5 added strategies in `tests/test_pipeline_integration.py`.

## Documentation State

- `suggestions.md` / `suggestions.html` remain the finalized audit baseline.
- `technical_upgrade.md` / `technical_upgrade.html` remain the finalized implementation specification.
- `technical_upgrade_checklist.md` is the final completion ledger.
