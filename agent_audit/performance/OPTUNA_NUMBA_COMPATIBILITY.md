# Optuna And Numba Compatibility

## Summary

No optimizable strategy parameter was added, removed, or retuned in this pass. No Optuna objective, sampler, pruner, storage, study name, trial budget, fallback random-search path, or winner-loading path was changed.

The implemented changes are additive measurement/correctness changes:

- per-trade MAE/MFE fields;
- live entry slippage attribution fields;
- dashboard PF sentinel correction;
- schema-version metadata;
- canonical metric/basis helpers;
- live-readiness min-lot tradeability checks;
- bounded replay and shadow-selection tooling.

## Modified Parameters

None.

## Optuna Integration Path

| Item | Status |
|---|---|
| Search space | Unchanged |
| Fallback random search | Unchanged |
| Objective scoring | Unchanged |
| Trial metadata | No current Optuna metadata change; future replay exports can consume trade schema fields |
| Winner serialization | Existing winners remain loadable; new trade records have additive metadata |
| Live reload | Unchanged |
| Study resume | Compatible; no storage migration |
| Search-space version | Not bumped because no search parameter changed |

## Numba Kernel Changes

| Area | Compatibility Decision |
|---|---|
| Kernel | `pm_core._backtest_loop_numba` remains nopython-compatible |
| Return tuple | Legacy indices 0 through 15 preserved |
| New return fields | MAE/MFE arrays appended at indices 16 through 24 |
| Types | `float64` for pips/R/distances; `int64` for timing counters |
| Schema version | Added in Python trade-dict reconstruction, not inside compiled kernel |
| Python fallback | `Backtester._run_python_loop` emits same measurement field names |
| Parity test | `tests/test_backtester.py::test_trade_excursion_fields_match_python_and_numba` |

## Python/Numba Parity Results

Focused test result from redo pass:

```text
python -m pytest tests/test_backtester.py tests/test_config_source_of_truth.py -q
37 passed
```

The deterministic fixture verifies:

- MAE pips;
- MFE pips;
- MAE R;
- MFE R;
- time to MAE;
- time to MFE;
- underwater bars;
- entry-to-stop pips;
- entry-to-target pips.

## Compilation Results

```text
python -m py_compile pm_core.py pm_main.py tests/test_backtester.py tests/test_config_source_of_truth.py
passed
```

## Runtime Results

No runtime benchmark was claimed. This pass used unit/focused integration tests only. The Numba change adds fixed-size per-trade arrays and simple per-open-position arithmetic, not full path storage.

## Memory Results

No peak-memory benchmark was run. Expected memory impact is one fixed preallocated array per new per-trade field in the Numba kernel. This preserves the prompt preference for per-trade aggregates rather than full intrabar path storage.

## Backward Compatibility

| Artifact/Consumer | Compatibility |
|---|---|
| Existing Optuna studies | Compatible |
| Existing winner files | Compatible |
| Existing trade records | Still readable, but lack `trade_record_schema_version`; parsers must treat missing schema as pre-MAE/MFE |
| Existing actionable logs | Still readable, but lack `actionable_schema_version`; parsers must treat missing schema as pre-entry-slippage |
| Existing metric payloads | Still readable; new payloads add `metric_contract_version`, `return_drawdown_ratio`, and Calmar basis fields |
| Dashboard JSON | Claude verified non-finite PF is JSON-safe through dashboard safe-value conversion |

## Migration Requirements

No migration is required to run the bot or tests. Any future analysis that uses MAE/MFE or slippage must filter for:

- `trade_record_schema_version == "2026-06-18-mae-mfe-v1"`
- `actionable_schema_version == "2026-06-18-entry-slippage-v1"`

Missing schema means “not instrumented,” not zero drawdown/slippage.

Metric consumers should treat:

- `calmar_ratio` as the backward-compatible FXPM legacy alias for cumulative return divided by max drawdown;
- `return_drawdown_ratio` as the explicit name for that same legacy ratio;
- `calmar_ratio_annualized` as the annualized companion for future shadow ranking only.

## Unresolved Incompatibilities / Open Quality Items

| Item | Status | Why It Matters |
|---|---|---|
| Canonical metric module | Implemented / partner review | PF sentinel, Calmar basis, return/DD, annualized-Calmar companion, and benchmark recovery factor now share helpers or explicit aliases |
| Objective de-biasing | Open | Validation is a selection surface; objective ranking may still prefer fragile winners |
| Exit/TP slippage attribution | Open | Entry slippage is measured; exit fill quality still needs instrumentation |
| Strategy behavior changes | Not attempted | Any TP/trailing/filter change must propagate through Optuna and Numba with parity tests |
| Current-ledger negative replay | Accepted evidence after enumerator fix | `FORD H4 CHOP DoubleEMATrendStrategy` is the sole logical promoted winner and replayed negative under USD 10,000; this is evidence that selection validation can fail, not yet a code-level objective mutation |

## Current Ledger Selection Implication

Claude and Codex agree the current `pm_configs.json` contains one logical promoted winner and that the corrected $10k replay loses. No Optuna search-space/objective change was made in this implementation pass, but the next methodology fix should be tested against this failure mode:

- validation trade-count floors that are strategy/timeframe aware;
- candidate-count or multiple-testing deflation for sparse validation winners;
- explicit regime/strategy compatibility scoring;
- post-selection replay over all promoted winners before live eligibility.
