# Production Split And Process

Status: canonical production reference as of 2026-06-09. The visual companion is
`docs/audit/production_split_process_20260609.html`.

## Production Geometry

FxPM production uses `production_split_mode="bleeding_edge"`.

Indexes are half-open (`start..end`):

| Window | Index Range | Bars | Meaning |
|---|---:|---:|---|
| `historical_stress_audit` | `0..50000` | 50,000 | Older-window fragility audit |
| `base_selection` | `50000..300000` | 250,000 | Full active universe for broad/base eligibility |
| `walk_forward_validation` | `124700..174700` | 50,000 | Adjacent WFV/resilience validation |
| `oos_embargo` | `174700..175000` | 300 | Leakage buffer before optimization |
| `optimization_train` | `175000..300000` | 125,000 | Newest active half through the market edge |
| `clean_holdout` | `300000..300000` | 0 | Disabled in production |

The WFV window is adjacent validation evidence, not pristine forward OOS, because Stage 1
eligibility may inspect the full active universe. This is intentional: production optimizes on
the freshest market-adjacent data instead of withholding the bleeding edge.

## Process

1. Load the latest `300000` M5 bars.
2. Reserve the oldest `50000` bars for historical stress audit.
3. Use the newest `250000` bars as the active universe for broad/base strategy eligibility.
4. Use the newest active half, `175000..300000`, for optimization and final selection.
5. Use `124700..174700` as adjacent walk-forward validation, separated from optimization by
   `oos_embargo_bars=300`.
6. Promote on WFV evidence instead of clean-holdout evidence. Clean WFV promotes, weak or
   thin WFV derates to `PROMOTE_SMALL`, and severe WFV blocks promotion.
7. Keep production clean holdout empty; clean-holdout modes are research-only.
8. Live readiness rejects stale split contracts before new live entries are allowed, then live
   performance, cycle-drift monitoring, and governance derate or pause contexts as trade
   evidence accumulates.

## Required Production Settings

| Setting | Value |
|---|---|
| `production_split_mode` | `"bleeding_edge"` |
| `walk_forward_validation_bars` | `50000` |
| `active_stage2_pct` | `50.0` |
| `clean_holdout_pct` | `0.0` |
| `walk_forward_promotion_policy` | `"promotion_gate"` |
| `walk_forward_live_blocking_enabled` | `true` |
| `false_discovery_holdout_policy` | `"off"` |
| `threadspark_selection_surface` | `"validation"` |
| `oos_embargo_bars` | `300` |
| `optimization_max_workers` | `4` |

## Non-Production Path

The older clean-holdout/disjoint layout is retained only as explicit research mode. It should
not be treated as the production default because it withholds the newest market-adjacent bars
from optimization and can make deployed strategies stale before live use.
