# F111: The 50k validation slice is the model-selection surface (and feeds Stage-1 eligibility), so stored validation metrics are selection-biased

## Category
Quant methodology / Internal consistency / Overfitting resistance

## Severity
High

## Confidence
High (windowing code + tests + config)

## Evidence
- **Windowing is correct** (`pm_core.py:4648-4762`, verified by `tests/test_data_splitter.py`): at 400k bars
  WFV = `(224700, 274700)` sits strictly before optimization_train `(275000, 400000)` with a **300-bar embargo**
  (`oos_embargo_bars=300`, `config.json:103`); `clean_holdout = (400000, 400000)` (empty). Half-open,
  non-overlapping. The latest-half (`active_stage2_pct=50`) and the 50k WFV (`walk_forward_validation_bars`)
  are placed exactly per the intended theory. **No off-by-one or forward leakage into the TPE objective.**
- **But `base_selection == active_universe` (`pm_core.py:4712`)** = the full 250k window, which *contains* the
  WFV slice. Stage-1 eligibility therefore sees the WFV bars.
- **And `threadspark_selection_surface = "validation"` (`config.json:111`)**: per
  `pm_pipeline.py:2690-2789`, after the TPE candidate pool is fixed (leak-clean), the ThreadSpark selector
  ranks/chooses the winner using *validation-surface* evidence. So the WFV slice is the **model-selection
  surface**, not an independent post-selection check.
- **Consequence (Codex F002, corroborated):** 147/157 tradeable cells have validation Sharpe > 10 and 75/157
  have < 20 validation trades, all with `pbo_status=not_available`. Choosing the candidate that scores best on
  the validation slice is a max-over-candidates operation → the stored validation Sharpe/PF is upward-biased.

## Explanation
The intended theory ("50k bars before the cutoff as a reliability check") is implemented with correct
geometry, and the inner Optuna objective is genuinely leak-clean (it does not see validation). The subtle flaw
is selection bias: because the *final winner* is chosen by validation-surface score, the validation metric is
no longer an unbiased estimate of out-of-sample quality for the selected cell — it is the maximum of a noisy
statistic over many candidates. With small validation samples (<20 trades) and an inflation-prone annualized
Sharpe (F106), this produces the systemic "Sharpe > 10" pattern Codex found. The 300-bar embargo controls
immediate autocorrelation but not regime similarity between the adjacent WFV and the optimization window.

## Trading Impact
- The reliability check is weaker than it appears: validation evidence overstates the edge of selected cells.
- False confidence in promotion; live decay risk (the broad OOS surface is weak: median PF 0.86, median 7
  trades — Codex F002 / `oos_manifest_summary.csv`).

## Suggested Fix or Experiment
1. Add a **true post-selection holdout** (small, after the selected winner is fixed) used only to *measure*
   decay, never to select — even in bleeding-edge mode (currently `clean_holdout` is empty).
2. Penalize/deflate validation Sharpe by candidate count (selection-bias correction) or require a minimum
   validation trade count (Codex F002 rec 1) before validation can drive selection.
3. Increase `oos_embargo_bars` experimentally and measure sensitivity of selected winners.

## Estimated Effort
Medium

## Risk of Change
Medium (changes which winners are promoted; must be re-validated, not hot-swapped)

## Validation Plan
Codex: for a few cells, recompute validation Sharpe/PF with a selection-bias penalty (e.g., Bonferroni/DSR on
candidate count) and report how many cells survive a >20-trade + deflated threshold.

## Partner Review
Pending (Codex). **Directly explains Codex F002**; jointly owned methodology finding.
