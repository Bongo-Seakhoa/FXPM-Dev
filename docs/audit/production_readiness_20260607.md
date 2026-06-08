# Production Readiness Pass - 2026-06-07

## Scope

User asked Codex to continue without Claude, preserve the per-specialist rule, keep the process
data-driven, and move the code toward production readiness for live capital. Codex used three
independent stand-in audits for live safety, config/artifact integrity, and monitoring.

Follow-up scope: reassess walk-forward validation from first principles, especially whether it
belongs between final validation and live execution. The production answer is no: walk-forward
remains useful research telemetry, but it should not spend or delay a freshly approved specialist's
live opportunity after final selection gates have passed.

## Production Book

Generated from the corrected W5 promotion gatefix artifacts:

- `config.json`
- `pm_configs.json`

The tracked default `config.json` now points to the universal root ledger `pm_configs.json`. MT5
credentials remain blank; non-secret identity pins are set
to the currently connected `FBS-Demo` account (`expected_login=106165989`,
`expected_server=FBS-Demo`, `expected_currency=USD`) so any different MT5 session fails closed.

Tradeable cells:

| Cell | Strategy | Cell verdict | Historical PF | Holdout PF |
|---|---|---:|---:|---:|
| `AUDUSD|H4|CHOP` | `LiquiditySweepReversalStrategy` | `PASS_STRICT` | 4.85 | 1.29 |
| `EURUSD|H4|TREND` | `LiquiditySweepReversalStrategy` | `PASS_THIN` | 1.03 | 1.25 |
| `USDCAD|H4|TREND` | `VIDYABandTrendStrategy` | `PASS_STRICT` | 1.39 | 2.58 |
| `USDJPY|H4|TREND` | `DoubleEMATrendStrategy` | `PASS_THIN` | 1.10 | inf |

Per-cell audit artifact:

- `pm_outputs/production_live/production_live_w5_specialist_cells.json`

Audit result: 96 specialist cells inspected individually; 92 `NO_TRADE_ABSTAINED`, 2
`PASS_STRICT`, 2 `PASS_THIN`. Unsafe promotes: 0.

## Safety Changes

- `pm_live_readiness.py` now enforces production promotion-gate parity, promotion/cell safety,
  completion-marker safety, manual kill switch state, and MT5 account identity pinning.
- `pm_live_readiness.py` now blocks tradeable ledgers with missing/expired `valid_until` metadata
  and requires production completion markers to declare `review_ready`.
- `pm_main.py` fails closed when broker deal history is unavailable for realized-loss hard blocks.
- `pm_main.py` blocks entries when the manual new-entry kill switch is active.
- `pm_mt5.py` marks market-entry `order_send(None)` as ambiguous.
- `tools/live_silent_watchdog.py` reports stale live telemetry as warning/critical and returns
  nonzero exit codes for non-OK states.

## Walk-forward and Cadence Decision

- Production profiles now use `walk_forward_promotion_policy = "observe_only"`.
- Research/discovery profiles can still use `block_severe_only` when the experiment intentionally
  wants severe walk-forward fragility to block candidates before any live-candidate handoff.
- Default production retraining cadence is now four weeks, not the former 14-day cycle. The current promoted book
  is H4-only and too sparse for a 14-day forced reproduction cycle to produce a meaningful live
  read. The schedule remains a freshness/reproduction cadence, not a live-adjacent walk-forward
  validation layer.
- `config.json` now uses four-week `notify` mode so operators can
  review/rebuild on schedule without automatically replacing specialists before they have had a
  reasonable chance to express edge.
- Production profiles now include non-zero hard circuit breakers and a 10 GB storage floor for
  pausing new entries.

## Remediation Completed

- Rebuilt `pm_configs.json` through the hardened
  production-book builder.
- Created an explicit review-ready gatefix marker:
  `logs/discovery_watch/production_book_legacy_freqoff_20260606_resume_w5_review_ready_gatefix.json`.
- Stamped deployment validity from the review-ready marker date. The current production book is
  valid until `2026-07-05T00:01:00`, giving a full four-week expression window from the June 7
  review-ready handoff.
- Preserved the original regime-parameter contract. A generated H4-only `regime_params.json` was
  rejected because it changed the artifact fingerprint without a matching re-optimization. The live
  ledger was selected with `regime_params.json` absent, so production readiness now accepts that
  explicit default-regime contract and still blocks if a stored artifact expected a concrete regime
  parameter file.
- Cleaned storage safely: interrupted Git GC temporary pack, PM feature cache, data cache, pytest
  cache, ruff cache, and Python bytecode cache were removed. Free disk rose from about `4.78GB`
  during the interrupted GC state to `43.97GB`, above the 10 GB production entry floor.
- `pm_live_readiness.py` now enforces the storage floor directly before live startup.
- `pm_regime_tuner.py` now uses explicit resampler methods, fixing the pandas aggregation failure
  hit during bounded H4 tuning.

## Current Readiness

Current production status on 2026-06-07 is **live-ready for the pinned FBS-Demo account**:

- `python pm_main.py --status`
- valid configs: `4`
- live eligible configs: `4`
- expired configs: `0`
- invalid configs: `0`
- need optimization: `0`
- all four production cells expire at `2026-07-05 00:01`
- live artifact policy: `block`

Live readiness check with the current MT5 account returned `ok=True` with `0` findings.

The watchdog one-shot against current stale live artifacts returned `CRITICAL`, as expected because
no active live loop is producing fresh telemetry.

## Verification

- Targeted production/live-safety tests: 76 passed.
- Config/readiness subset after making `config.json` production-gated: 66 passed.
- Current targeted remediation suite: 170 passed, 2 warnings.
- Current full suite after the latest readiness/build/tuner remediations: 1182 passed, 5 skipped,
  2 warnings, 575 subtests.
- Compile check passed for touched runtime/tool modules.

## Remaining Operational Blockers

1. For a funded account, replace the current FBS-Demo expected MT5 identity pins with the intended
   live account identity and rerun readiness while connected to that account.
2. Start a paper/live dry-run loop and confirm `tools/live_silent_watchdog.py` reports fresh `OK`
   telemetry rather than stale `CRITICAL`.
3. Do not introduce a new `regime_params.json` for this production book unless the book is
   re-optimized under that exact regime-parameter artifact.
