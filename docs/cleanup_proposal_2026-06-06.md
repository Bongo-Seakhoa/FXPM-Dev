# Cleanup Proposal - 2026-06-06

Status: scorer cleanup completed 2026-08-13; historical helpers archived and the retired selector test removed.

Purpose: reduce workspace confusion after the scoring discovery phase while preserving every durable safety, evaluation, and audit asset. This proposal intentionally avoids removing old scorer engine paths in the same pass as live-safety work, because tests and historical artifacts still reference those names.

## Keep

These are durable assets and should remain available:

- `legacy` scorer and production config pins:
  - `config.json`
  - `pm_core.py`
  - `pm_pipeline.py`
- Honest evidence and evaluation infrastructure:
  - disjoint validation / embargo code
  - warmup-trimmed metrics
  - fixed `selection_validation.py` no-forward-leak guard
  - `tools/selection_validation.py`
  - `tools/pareto_select.py`
  - `tools/benchmark.py`
  - `tools/benchmark_specialist_cells.py`
  - `tools/label_candidate_pool.py`
  - `tools/meta_label_validation.py`
- Safety and live-readiness surfaces:
  - ambiguous-send reconciliation in `pm_main.py`
  - realized-loss hard guard
  - config credential-integrity tests
- Research record:
  - `docs/PROGRAM_PLAN.md`
  - `docs/ACTIVE_DISCOVERY_STATE.md`
  - `docs/archive/AGENT_COMMS_2026-06-04.md`
  - current `AGENT_COMMS.md`
  - candidate-pool label artifacts needed to reproduce L13/L14 conclusions
- Tests that guard real bugs:
  - `tests/test_benchmark_specialist_cells.py`
  - `tests/test_label_candidate_pool.py`
  - `tests/test_meta_label_validation.py`
  - `tests/test_config_json_integrity.py`
  - live safety / risk / readiness tests

## Safe First-Pass Archive Candidates

Executed archive location: `tools/archive/scoring_discovery_20260606/`. These are one-off report/debug helpers from rejected scorer experiments, not production workflow tools.

- `tools/archive/scoring_discovery_20260606/build_metric_trajectory_report.py`
- `tools/archive/scoring_discovery_20260606/build_strategy_cell_atlas_report.py`
- `tools/archive/scoring_discovery_20260606/build_threadspark_debug_report.py`
- `tools/archive/scoring_discovery_20260606/outlier_spoke_shape_visual.py`
- `tools/archive/scoring_discovery_20260606/scoring_like_for_like_postrun.py`

Reason: these are tied to specific old ThreadSpark/spokes runs and static debug reports. They are useful historical artifacts, but keeping them in the active `tools/` root makes future sessions think scoring discovery is still active.

## Archived After Dedicated Review

The dedicated scorer-collapse pass proved these helpers had no production caller. They are retained only as historical evidence under `tools/archive/scoring_discovery_20260606/`; their retired-selector test is no longer part of the active suite.

- `tools/archive/scoring_discovery_20260606/spoke_selector_validation.py`
- `tools/archive/scoring_discovery_20260606/specialist_spokes.py`
- `tests/test_spoke_selector_validation.py` (deleted; tested only retired machinery)
- `tests/test_selection_geometry.py`
- `scoring_model_proposed_vs_real.html`
- `preview.html`

Reason: these are part of the rejected scoring record, but they also contain leak-guarded validation logic and historical explanation. Removing them casually risks losing why the scoring lines were rejected.

## Keep Unless A Future Run Needs Space

These remain operationally useful for diagnostics, performance checks, or future non-scoring maintenance:

- `tools/discovery_status.py`
- `tools/discovery_completion_monitor.py`
- `tools/discovery_silent_watchdog.py`
- `tools/watch_discovery_run.py`
- `tools/holdout_diagnostics.py`
- `tools/purged_kfold_replay.py`
- `tools/optuna_convergence.py`
- `tools/parity_numba_python.py`
- `tools/precompute_feature_cache.py`
- `tools/regime_winner_review.py`
- `tools/promotion_review.py`

Reason: some names still say "discovery", but they solve generic run monitoring, replay, parity, and diagnostics problems.

## Do Not Remove In This Pass

Do not remove the old scorer engine branches yet:

- `specialist_spokes`
- `specialist_spokes_asym`
- `specialist_spokes_v2`
- `specialist_spokes_threadspark`
- related `PipelineConfig` fields
- related tests in `tests/test_optimizer_hardening.py` and `tests/test_pipeline_artifacts.py`

Reason: although these scorer lines are rejected for production, removing them is a larger refactor. Many tests and historical artifacts reference them. The safer plan is:

1. Finish live-capital safety and regression guards.
2. Freeze the rejection record in `PROGRAM_PLAN.md`.
3. In a dedicated cleanup branch/pass, remove old scorer branches with a full reference map and full test run.

## Proposed Next Action

After Claude and Bongo signed off, step 1 was executed:

1. `[x]` Move the first-pass archive candidates into `tools/archive/scoring_discovery_20260606/`.
2. `[x]` Update live references in `AGENT_COMMS.md` / `ACTIVE_DISCOVERY_STATE.md` to say archived, not active.
3. `[ ]` Run:

```powershell
python -m pytest tests/test_config_json_integrity.py tests/test_benchmark_specialist_cells.py tests/test_label_candidate_pool.py tests/test_meta_label_validation.py tests/test_config_source_of_truth.py::LiveRiskIntentTests -q
```

No active production code should be changed during this first archive pass.
