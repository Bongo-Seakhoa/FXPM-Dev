# Technical Upgrade Implementation Checklist

Primary source of truth: `technical_upgrade.md`
Supporting context: `suggestions.md`

This file is the live progress tracker for the upgrade implementation. Items are checked only when the corresponding code and test work have been completed to a reviewable standard.

## Change Block A: Research Kernel and Data Contract

- [x] Enforce exact source-file selection in `DataLoader` and remove wildcard-first base-file fallback
- [x] Quarantine or drop invalid OHLC bars instead of only logging them
- [x] Strengthen feature-cache identity and semantic invalidation
- [x] Keep regime feature generation on one canonical path with proper invalidation
- [x] Make broken signal/stop contracts fail explicitly in backtesting
- [x] Unify Python/Numba fill semantics including same-bar exit handling
- [x] Unify Python/Numba gap-stop, slippage, and unsafe-sizing behavior
- [x] Correct metric math to use corrected equity and net-dollar truth
- [x] Align regime-bucket metrics with corrected kernel metric definitions
- [x] Add or extend tests for kernel/data-contract changes

## Change Block B: Validation, Search, and Artifact Integrity

- [x] Redesign split contract to expose train/warmup/validation/holdout regions
- [x] Remove scored overlap while preserving warmup-only overlap
- [x] Remove fallback-to-train approval behavior for stored winners
- [x] Fix Optuna validation leakage and regime-max objective bias
- [x] Add semantic config/artifact fingerprinting and invalidation
- [x] Remove production retrain-window search in favor of fixed biweekly cadence
- [x] Make regime parameter tuning invalidation-safe and research-only
- [x] Centralize expiry semantics on the fixed production retrain model
- [x] Retire or quarantine legacy selector/validator surfaces from the production path
- [x] Add or extend tests for split, validation, fingerprinting, and expiry behavior

## Change Block C: Strategy Layer Remediation

- [x] Fix shared helper/precompute surface issues for ADX/DI, Bollinger, Keltner, and swing-point helpers
- [x] Fix `SupertrendStrategy`
- [x] Fix `ADXTrendStrategy`
- [x] Fix `ADXDIStrengthStrategy`
- [x] Fix `EMARibbonADXStrategy`
- [x] Fix `AroonTrendStrategy`
- [x] Fix `InsideBarBreakoutStrategy`
- [x] Fix `PinBarReversalStrategy`
- [x] Fix `EngulfingPatternStrategy`
- [x] Fix `FisherTransformMRStrategy`
- [x] Fix `SqueezeBreakoutStrategy`
- [x] Fix `KeltnerPullbackStrategy`
- [x] Fix `ParabolicSARTrendStrategy`
- [x] Fix `TurtleSoupReversalStrategy`
- [x] Remove conditional dead dimensions from `ZScoreVWAPReversionStrategy`
- [x] Remove conditional dead dimensions from `MACDHistogramMomentumStrategy`
- [x] Remove parameter-specific cache risk from `StochRSITrendGateStrategy`
- [x] Update registry/schema/feature-contract integrity where needed
- [x] Add or extend targeted strategy regression and param-surface tests

## Change Block D: Live Runtime and Order Path

- [x] Separate canonical symbol identity from broker symbol identity in live decision flow
- [x] Collapse to one authoritative live runtime loop owner
- [x] Make MT5 position/account/symbol state fail closed in entry gating
- [x] Reuse one positions snapshot per sweep where appropriate
- [x] Fix live stop-placement parity against closed signal bars
- [x] Wire or remove dormant live exit surfaces
- [x] Invalidate runtime candidate/spec caches on reconnect/retrain/state change
- [x] Make fill policy explicit and broker-tested
- [x] Integrate drift monitoring into the live path
- [x] Add or extend live/runtime tests

## Change Block E: Dashboard and Analytics Truthfulness

- [x] Separate realized trade analytics from actionable/signal feeds
- [x] Fix trade enrichment identity to avoid symbol-only mismatches
- [x] Fix analytics metric consistency and initial-capital propagation
- [x] Fix `/api/simulate` input handling and date-window semantics
- [x] Make dashboard config persistence atomic and transparent on failure
- [x] Make watcher lifecycle/freshness behavior deterministic
- [x] Add or extend dashboard tests

## Change Block F: Performance and Output-Preserving Optimization

- [x] Retire dead or duplicate production-path surfaces
- [x] Add dashboard analytics ingress caching by file identity
- [x] Add watcher incremental parse caching
- [x] Wire parameter-aware lazy feature requests where parity is preserved
- [x] Batch trade reconstruction by grouped data windows
- [x] Optimize verified hotspots only under parity controls
- [x] Remove dead work and stale helper branches that do not affect outputs
- [x] Add or extend parity/benchmark tests

## Change Block G: Quant Enhancement Insertion Points

- [x] Add risk scalar stack insertion seam
- [x] Add market-driven exit-pack insertion seam
- [x] Add portfolio construction / exposure redistribution seam
- [x] Add regime-model upgrade insertion seam
- [x] Add execution-quality overlay insertion seam
- [x] Add options-model adaptation insertion seam
- [x] Add new strategy insertion seam and roster integration path

## Verification and Closeout

- [x] Run targeted tests for all touched workstreams
- [x] Run broader integration/backtest verification
- [x] Update fingerprints / invalidation/version constants as required
- [x] Reconcile checklist against implemented code and remaining risks

## Post-Implementation Hardening Sweep

- [x] Remove the legacy `--auto-retrain` live loop path and CLI surface
- [x] Replace rolling config expiry with the fixed biweekly Sunday `00:01` production schedule
- [x] Make `config.json` own `data_dir` / `output_dir` unless explicitly overridden
- [x] Align operator docs with the fixed production retrain schedule
- [x] Harden MT5 broker-symbol resolution to avoid loose prefix matches
- [x] Carry MT5 filling/execution/freeze metadata through the connector
- [x] Add MT5 `order_check()` preflight before `order_send()` requests where supported
- [x] Treat MT5 partial fills (`10010`) as partial success rather than hard failure
- [x] Extend tests for schedule anchoring, app path ownership, and MT5 order construction

## Final Audit Amendments (2026-03-30)

- [x] Replace unstable optimizer seeding (`hash()` / hardcoded defaults) with deterministic context-derived stable seeds
- [x] Make Optuna sampler seeding context-aware and config-driven
- [x] Carry effective search-trial breadth into DSR computation for finalist comparison
- [x] Apply DSR as a Sharpe-only confidence adjustment instead of shrinking the entire composite score
- [x] Strengthen regime-local rescue to require acceptable regime return and drawdown, not just regime PF
- [x] Make regime tuner warmup-aware and score blended train/holdout quality instead of holdout-only scoring with short slices
- [x] Carry MT5 `visible` / `trade_mode` metadata through the connector and enforce live tradability from real broker metadata
- [x] Track live equity peak and dynamic recent spread medians in execution overlays instead of static approximations
- [x] Expose Drawdown Duration / Recovery Time / Ulcer Index in the analytics frontend and keep the no-data payload schema stable
- [x] Extend regression coverage for tradability metadata, overlay semantics, stable seeding, DSR trial breadth, warmup exclusion, analytics schema, and the five added strategies
