# Tenet 4 — Resource / Data-Pipeline Audit

Read-only audit of memory/compute in the optimization pipeline. Tenet 4 requires cuts that
DISCOVERY proves are safe — not naive "fewer bars/params/symbols." Verdict: the data pipeline
is already reasonably efficient; the dominant cost is inherent search-breadth compute, and the
one genuinely accuracy-neutral lever is **prospect-driven prioritisation of the re-opt**.

## Verified memory/compute structure (not assumed)
- Per-symbol load (`_load_symbol_timeframe_data`, pm_pipeline.py:2952): base M5 `tail(max_bars=300k)`,
  resampled per TF; resample is cached in DataLoader (memory+disk). `run_for_symbol` (3403) is
  per-symbol, so working-set memory is bounded to one symbol × `optimization_max_workers` (4),
  not the whole 62-symbol universe at once.
- Features computed **once per (symbol, TF, stage)** via `compute_all` (3491) and **reused**
  across all 76 strategies and all Optuna trials. This is the EFFICIENT choice — switching to
  per-strategy lazy `compute_for_strategy` would RECOMPUTE features per strategy and be slower.
  No change recommended here.
- Footprint is dominated by the M5 feature frame (~300k rows × ~50 indicator cols). Reasonable
  at ~100-150 MB/worker; higher TFs are far smaller.

## Honest conclusion
There is no large, free memory win in the mechanics. The real cost is COMPUTE: Optuna trials
(200-5000) × 76 strategies × regimes × ~62 symbols × TFs = the "days" re-opt. The user has
already (correctly) ruled out blindly cutting trials/params/symbols/bars because those degrade
accuracy. So the lever must be data-driven, not a blunt cut.

## The accuracy-neutral lever: prospect-driven re-opt PRIORITISATION
Use the cheap prospect screener (`tools/strategy_prospect.py`, seconds-per-strategy) to inform
the expensive re-opt — WITHOUT excluding anything that could win:
- **Prioritise, don't hard-prune.** Order the re-opt so strategy×asset-class combos with
  demonstrated prospect (VIABLE: frac_profitable>=0.30, median PF>=1) run first; combos that are
  DECISIVELY hopeless (zero profitable samples across a wide honest-cost screen) run last or are
  made opt-in. This front-loads compute onto promising combos and lets a bounded run still cover
  the high-value surface, while preserving the option to explore the rest.
- **Asset-class scoping.** A strategy WEAK across all FX in screening is a low-priority FX re-opt
  target; one strong only on indices/crypto should wait on the broker-spec provenance fix (H5)
  before its re-opt is trusted.
- **Post-first-re-opt pruning (data-driven).** After the first honest full re-opt, drop from the
  routine retrain only those strategy×symbol combos that produced NO promotable winner across the
  cycle — a cut justified by discovery evidence, not theory. Keep them available for periodic
  re-exploration so a regime shift can resurface them.

Net: this attacks the actual bottleneck (re-opt compute) in an accuracy-neutral way and reuses
an instrument that already exists. It should be folded into Codex's re-opt protocol, not run as
a separate pass.

## Minor mechanical notes (low priority, flag only)
- Several `.copy()` on resampled/sliced frames (pm_pipeline.py:2984-2988, 1474-1475, 3148) are
  defensive; some may be safe as views, but the saving is small vs the compute cost. Not worth
  the mutation risk now.
- Features are built on the full `max_bars` window though only the active/scored window is scored
  (indicators need lead-in, so most of this is legitimate warmup). Negligible.

## Recommendation
Do NOT touch the data-pipeline mechanics for efficiency now — it is not where the cost is, and
changes there risk accuracy/correctness for little gain. Instead, fold **prospect-driven
prioritisation** into the re-opt protocol so the resource-heavy run is spent where discovery
shows return is plausible. Re-audit memory only if the 100-symbol equities expansion changes the
per-worker footprint materially.
