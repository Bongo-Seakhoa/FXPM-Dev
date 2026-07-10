# Runtime History Cleanup - 2026-06-09

This cleanup removed stale generated runtime files after the bleeding-edge production split was
made canonical.

Removed generated/cache categories:

- `pm_outputs/production_live/feature_cache/`
- `pm_outputs/production_live/oos_evidence/`
- `pm_outputs/production_live/reports/`
- stale `pm_outputs/*_active_run.json` run markers
- old root `pm_outputs/trades_*.json` snapshots
- old root `pm_outputs/signal_ledger_202604.jsonl` and `signal_ledger_202605.jsonl`
- old root `pm_outputs/storage_state.json`, `storage_manifest.jsonl`, and `optimization_summary.csv`
- old benchmark output folder `pm_outputs/benchmarks/`
- Python/test/tool caches
- stale `last_trade_log.json.tmp.*`

Moved to `documentation_archive/legacy_root_reports_20260609/`:

- old root recommendation HTML reports
- old workflow operating model reports
- old preview/scoring/value-ranking reports
- old data-workflow restructure report

Preserved operational state:

- `pm_configs.json`
- `regime_params.json`
- `last_trade_log.json`
- `last_actionable_log.json`
- `runtime/manual_new_entries_enabled.flag`
- `data/` market data and `data/.live/` live caches
- current live process and live monitor
