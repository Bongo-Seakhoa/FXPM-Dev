# Baseline 10000

## Canonical Baseline Contract

The only accepted account baseline for direct comparisons in this phase is USD 10,000.

This redo pass did not run a balance sweep, did not optimize starting balance, did not run the full optimizer, and did not place live trades. The USD 10,000 baseline is therefore a fingerprinted comparison contract plus measurement implementation, not a new full profit curve.

## Machine-Readable Evidence

- Baseline JSON: `agent_audit/performance/baseline_10000_summary.json`
- Evidence manifest: `agent_audit/performance/evidence_manifest.json`
- Comparison CSV: `agent_audit/performance/PERFORMANCE_COMPARISONS.csv`
- Strategy anatomy summary: `agent_audit/performance/strategy_anatomy/strategy_family_summary.csv`
- Current ledger inventory: `agent_audit/performance/data/current_ledger_inventory.json`
- Current promoted cells: `agent_audit/performance/data/current_promoted_cells.csv`

## Recorded Baseline Fields

| Field | Location |
|---|---|
| Git commit / working tree fingerprint | `baseline_10000_summary.json.git_head`, `working_tree_status` |
| Configuration hash | `config_sha256` |
| Winner ledger hash | `pm_configs_sha256` |
| Regime parameter hash | `regime_params_sha256` |
| Current production ledger fingerprint | `pm_configs_sha256` |
| Historical anatomy data fingerprint | `historical_ledger_tradeable_cells_sha256` |
| Current ledger inventory fingerprint | `current_ledger_inventory_sha256` |
| Starting balance | `canonical_starting_balance_usd = 10000.0` |
| Cost assumptions | `cost_settings` |
| Risk settings | `risk_settings` |
| MT5 identity fields | `mt5_identity` |
| Optuna settings | `optuna_settings` |
| Numba status | `numba_status` |

## Current Baseline Snapshot

| Item | Value |
|---|---:|
| Canonical starting balance | 10000.0 |
| Balance sweep performed | false |
| Full optimizer run performed | false |
| Live trade performed | false |
| `pm_configs.json` top-level entries | 100 |
| Historical tradeable ledger rows used for anatomy | 157 |
| Current logical promoted live-book cells | 1 |
| Strategy families generated | 42 |

## Interpretation

The implemented changes are measurement/correctness fixes:

- MAE/MFE and underwater timing make high-drawdown classification possible in future replays.
- Entry slippage fields make live fill leakage measurable.
- PF sentinel correction prevents false dashboard interpretation.
- Schema-version fields prevent historical missing data from being treated as zero excursion/slippage.

No row in this phase should be read as proof that a strategy now makes more money. The corrected current-ledger replay shows the opposite: the only logical promoted winner in the rewritten `pm_configs.json` loses at the USD 10,000 baseline. The next proof step is a clean completed winner book, followed by corrected all-promoted replay and shadow-ranking using the new fields.
