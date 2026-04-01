# FxPM 1.4 Changelog

All notable repository-level changes are documented here.

Notes:

- The application/runtime banner remains `v3.1`
- The repository changelog uses the `1.4.x` track

---

## [1.4.5] - 2026-04-01 (Current)

### Core PM upgrade completion

- completed the major PM implementation pass driven by the archived audit and upgrade specs
- kept the active live regime engine as the score-based 4-regime detector
- expanded the live strategy registry from `42` to `47`
- hardened backtest/live parity around same-bar gap-through-stop handling
- strengthened validation, regime selection, and optimizer reproducibility

### Strategy roster

Added:

- `VortexTrendStrategy`
- `TRIXSignalStrategy`
- `RelativeVigorIndexStrategy`
- `VIDYABandTrendStrategy`
- `ChoppinessCompressionBreakoutStrategy`

### Live trading and MT5 hardening

- removed the legacy `--auto-retrain` path
- moved production retraining to the calendar schedule controlled by:
  - `production_retrain_mode`
  - `production_retrain_interval_weeks`
  - `production_retrain_weekday`
  - `production_retrain_time`
  - `production_retrain_anchor_date`
- hardened MT5 symbol resolution, tradability checks, order preflight, and partial-fill handling
- wired the spread-quality overlay into config with:
  - `execution_spread_filter_enabled`
  - `execution_spread_min_edge_mult`
  - `execution_spread_spike_mult`
  - `execution_spread_penalty_start_mult`

### Dashboard alignment

- fixed analytics expectancy and pip-value handling
- hardened watcher, jobs, parsers, and utils behavior
- aligned templates/static assets with current dashboard behavior
- clarified the dashboard as read-mostly rather than fully read-only

### Documentation sync

- rewrote `README.md` to match the current PM architecture and runtime behavior
- rewrote `SETUP_AND_RUN.md` for the fixed retrain schedule, root `logs/`, and current live flow
- rewrote `SETTINGS_REFERENCE.md` against the current config and code semantics
- rewrote `pm_dashboard/README.md` for current dashboard capabilities and write behavior
- refreshed this changelog so the repo state and docs align

---

## [1.4.4] - 2026-02-07

### Winners-only cleanup

- removed deprecated fallback/tier risk artifacts from the main live path
- simplified `_execute_entry` around a winners-only risk model
- updated trade comment formats while preserving backward decoding compatibility

### Warning and config cleanup

- fixed a broad batch of pandas warning sources
- aligned `fx_min_robustness_ratio` propagation across code paths
- added optimization progress visibility

---

## [1.4.3] - 2026-02-07

### Config and indicator cleanup

- expanded config documentation coverage
- introduced shared indicator helper caching
- added live-loop integration tests

---

## [1.4.2] - 2026-02-07

### Strategy expansion

- expanded the roster from `27` to `42` strategies

### Safety and dashboard upgrades

- improved warmup protection, numeric guards, and MT5 parity
- added a major dashboard capability upgrade

---

## [1.4.1] - 2026-02-01

### Optimization and efficiency

- integrated Optuna TPE
- introduced major speed improvements around backtesting and regime work
- added stateful optimization persistence

---

## [1.4.0] - 2026-01-15

### Initial release

- introduced the core regime-aware PM architecture
- shipped the initial 27-strategy version
- added MT5 live trading and the original dashboard
