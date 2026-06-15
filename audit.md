# FxPM Data Loading And Split Audit

Date: 2026-06-11

## Scope

Reviewed the current testing/evaluation state after recent changes to:

- canonical M5 data loading and MT5 historical backfill
- local/live data cache interaction
- production split geometry and walk-forward validation
- artifact/ledger validity around the new split contract
- tests covering loader, splitter, historical backfill, MT5 connector, artifacts, and live cache behavior

## Functional Summary

The current production design is coherent and explicit:

- `production_split_mode="bleeding_edge"` is the production default.
- The latest `300000` M5 bars are loaded.
- The oldest `50000` bars inside that loaded window are retained as `historical_stress_audit`.
- The newest `250000` bars form the active universe.
- The newest active half, `optimization_train`, is `175000..300000` for a full 300k M5 sample.
- The adjacent WFV window is `124700..174700`, separated from optimization by `oos_embargo_bars=300`.
- Production clean holdout is intentionally empty.
- Live uses a bounded `.live` cache, seeded from canonical M5 plus fresh MT5 deltas, and can fall back to MT5 direct bars.

The loader changes are directionally sound:

- symbol/timeframe CSV resolution avoids broad wildcard fallback
- OHLC validation drops invalid rows
- spread statistics survive resampling
- MT5 historical fetching now has retry/sync and paged date-walk behavior for symbols with shallow local terminal history

## Findings

### 1. Promoted Ledger Cells Missing Tuned Regime Params

Severity: high

`config.json` has `live_require_tuned_regime_params=true`, and live entry rejects a timeframe when `has_tuned_regime_params(symbol, timeframe)` is false.

Current mismatch:

- `Platinum / H4 / CHOP` is `PROMOTED` in `pm_configs.json`, but `regime_params.json` has no `Platinum/H4` row.
- `Palladium / H4 / TREND` is `PROMOTED` in `pm_configs.json`, but `regime_params.json` has no `Palladium/H4` row.

Impact: the ledger advertises live-promoted cells that live trading will later treat as no-trade because tuned regime detector params are absent.

Evidence:

- `pm_main.py:3619` checks for tuned params before live candidate evaluation.
- `pm_regime.py:456` defines `has_tuned_regime_params`.
- `tests/test_config_json_integrity.py:151` asserts promoted cells must have regime params coverage.
- Full pytest fails on this exact mismatch.

Recommended fix:

- Materialize tuned/default-equivalent regime params rows for promoted `Platinum/H4` and `Palladium/H4`, or demote/block those cells until regime params are present.
- Add a pre-live readiness check that reports promoted cells missing `(symbol, timeframe)` regime params before trading starts.

### 2. Split Metadata Drops The Final Row When Re-Sliced By Timestamp

Severity: medium-high

`_build_m5_workflow_meta()` stores `end_ts` as the timestamp of the last included row, but `_slice_time_window()` treats the end timestamp as exclusive.

Impact: every closed window loses its final bar when metadata is converted back into timestamp slices. I verified this with a synthetic 1100-row M5 window:

- `base_selection`: metadata says 1000 rows, timestamp slice returns 999
- `walk_forward_validation`: metadata says 100 rows, timestamp slice returns 99
- `optimization_train`: metadata says 500 rows, timestamp slice returns 499

Evidence:

- `pm_pipeline.py:5578` records `end_ts` as `frame.index[-1]`.
- `pm_pipeline.py:5556` documents and implements half-open slicing `[start_ts, end_ts)`.
- `pm_pipeline.py:6559` and nearby lines consume these timestamps for eligibility/train/validation slices.

Recommended fix:

- Store an exclusive `end_ts_exclusive` using the timestamp at integer boundary `end`, or keep integer indices authoritative and translate to timestamps using the next row boundary.
- Add a regression test that `rows` in workflow metadata matches the actual row count returned by `_slice_time_window()`.

### 3. Higher-Timeframe Windows Can Straddle M5 Split Boundaries

Severity: medium

The pipeline resamples the full M5 window into H1/H4/D1, computes features, then applies M5-derived timestamp windows. Higher-timeframe bars are labeled by their open time, so a bar whose open is before the split end can still include M5 data after the split boundary.

Impact: validation or holdout slices can contain a higher-timeframe bar that partially includes data beyond the intended M5 window. The current production `300` M5-bar embargo likely masks this for H1/H4/D1 in the configured profile, but smaller research embargos or clean-holdout mode can be exposed.

Evidence:

- `pm_pipeline.py:6559` slices full features by timestamp after feature computation/resampling.
- `DataSplitter` defines the split in base-M5 integer coordinates in `pm_core.py:4648`.

Recommended fix:

- For strict evidence windows, slice base M5 first and then resample per window.
- Alternatively, filter higher-timeframe rows by their right edge, not just open timestamp.
- Document the required minimum embargo by timeframe if keeping current behavior.

### 4. Ledger Temp Files Leak Into Git Status

Severity: low

`ConfigLedger` now writes unique temp files like `.pm_configs.json.<pid>.<ns>.tmp`, but `.gitignore` only ignores `*.tmp.*`, which does not catch that suffix form.

Current untracked file:

- `.pm_configs.json.11256.1781013876192720100.tmp`

Evidence:

- `pm_pipeline.py:705` creates the temp path.
- `.gitignore` has `*.tmp.*` but not `.pm_configs.json.*.tmp`.

Recommended fix:

- Add `.pm_configs.json.*.tmp` to `.gitignore`.
- Optionally add startup cleanup for stale ledger temp files older than a short grace period.

## Verification

Focused suite:

```text
pytest tests/test_data_splitter.py tests/test_data_loader_contract.py tests/test_historical_data_backfill.py tests/test_mt5_connector.py tests/test_pipeline_artifacts.py tests/test_pipeline_config_fields.py tests/test_storage_live_data.py -q
105 passed
```

Full suite:

```text
pytest -q
1 failed, 1226 passed, 5 skipped
```

The single full-suite failure is `ConfigJsonIntegrityTests.test_universal_pm_configs_candidates_are_governed`, caused by the promoted `Platinum/H4` and `Palladium/H4` cells missing corresponding regime params.

`git diff --check` on the touched code paths reported no whitespace errors, only LF/CRLF warnings on modified test files.

## Overall Assessment

The new data loading and split workflow is much stronger than the old loose setup. The biggest conceptual decision, using bleeding-edge optimization with adjacent WFV instead of clean holdout, is implemented consistently at the index-contract level and documented clearly.

The two issues to fix before trusting live promotion are the ledger/regime-params mismatch and the metadata timestamp off-by-one. The higher-timeframe boundary behavior is less urgent in the current production profile, but it should be made explicit before relying on research clean-holdout comparisons.
