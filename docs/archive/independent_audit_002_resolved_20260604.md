# Independent Accuracy Audit 002

**Auditor:** Independent skeptical accuracy auditor (verified against code, not agent prose)
**Date:** 2026-05-31
**Branch:** ai-commercialisation-sprint
**Scope (ONLY these three):**
1. Screening ATR-anchor expansion
2. C3 global false-discovery DSR gate
3. OOS candidate evidence export for PBO

All findings carry `file:line`. Other elements (disk feature cache, benchmark gate, PBO tool internals,
OOM fix) were explicitly out of scope and not re-audited, except where the OOS export feeds the PBO tool.

Live config audited: `config.discovery_honest.json`
(`screening_atr_anchor_enabled=true`, multipliers `[1.0,2.0,3.0]`;
`false_discovery_control_enabled=true`, `false_discovery_min_dsr=0.0`, `confidence_floor=0.35`,
`holdout_policy="block"`; `oos_evidence_export_enabled=true`, periods=64, min_periods=8).

---

## VERDICTS (summary)

| Area | Verdict |
|------|---------|
| 1. Screening ATR-anchor expansion | **trustworthy** (one MEDIUM, two LOW) |
| 2. C3 false-discovery DSR gate | **trustworthy** (evidence-only as configured; one MEDIUM doc/logging, one LOW) |
| 3. OOS candidate evidence export for PBO | **not-yet-trustworthy** (one HIGH cross-timeframe alignment defect) |

---

## CRITICAL
None.

---

## HIGH

### H1 — OOS PBO matrix mixes candidates across DIFFERENT timeframes onto a single "relative block" row axis, which can corrupt the CSCV PBO
**Where:** `pm_pipeline.py:4133-4139` (records consumed once per *symbol*, spanning all timeframes) → `pm_pipeline.py:3940-3943` (`np.column_stack` of per-candidate `period_returns[-min_periods:]`) → consumed by `tools/pbo.py:47-74` (`pbo_cscv` splits the row axis `T` into contiguous time blocks).

**Why it is a problem:**
- `oos_candidate_evidence_records` accumulates across the per-timeframe loop inside `optimize_symbol` (records are appended in `_attach_oos_evidence_to_candidates`, `pm_pipeline.py:2113`, which is called once per timeframe at `:2324/:2351/:2392`). `consume_oos_candidate_evidence` (`:1948-1952`) returns the whole symbol-level pool, and `_write_oos_candidate_evidence` (`:3915`) builds ONE matrix for the symbol with no timeframe partitioning.
- Each column's `period_returns` is `relative_equal_blocks`: the per-bar validation equity curve (`pm_core.py:225`, equity is per-bar length = bar count) is split into `block_count = min(max(2, periods), returns.size)` equal blocks (`pm_pipeline.py:1984`). With `periods=64` and validation windows of hundreds/thousands of bars, every candidate yields exactly 64 blocks — so column *lengths* match and `np.column_stack` does not raise.
- BUT row *i* for an H1 candidate and row *i* for an M5/D1 candidate are completely different, differently-sized calendar slices. `pbo_cscv` (`tools/pbo.py:56-61`) forms IS/OOS partitions over contiguous row blocks assuming rows share a comparable time axis across columns. They do not when timeframes are mixed. The IS-vs-OOS "best config" comparison then compares performance over non-coincident periods, which biases the PBO estimate in an uncontrolled direction (it is neither a clean over- nor under-estimate). The matrix contract string even admits this: `"alignment": "per-candidate relative OOS periods truncated to a common T"` (`:3979`).
- Net effect: the headline PBO number the human will use to trust/distrust the discovery pool is computed on a misaligned matrix. This is a silent accuracy degradation of the C3 measurement itself.

**Secondary leak/misalignment within the same matrix:** even within one timeframe, `_attach_oos_evidence_to_candidates` runs a *full-validation-window* backtest per candidate (`:2087-2095`) — not regime-segmented — so a candidate that was only ever a *trend-regime* winner contributes its full-window equity, while its selection score was regime-bucketed. Columns are therefore comparable to each other within a timeframe (good for CSCV), but they are NOT the series the selector actually optimized, so PBO measures a *related* surface, not the selection surface. This is acceptable as a documented approximation but should be stated; combined with H1 it compounds.

**No actual lookahead/leakage into the values:** verified the returns come only from `val_result` over `val_features` with `warmup_bars=val_warmup_bars` (`:2088-2094`); `_period_returns_from_equity` uses only the equity array (`:1968-1996`). No future data enters the matrix. The defect is *alignment*, not leakage.

**Fix recommendation (pick one):**
- Preferred: partition the matrix per `(symbol, timeframe)` — group `ordered_records` by `record["timeframe"]` in `_write_oos_candidate_evidence` and emit one manifest+npz per timeframe (or one matrix with a `timeframe` block-mask so `pbo_cscv` only forms partitions within a timeframe). Then rows are a comparable axis.
- Minimum: refuse to build a single mixed-timeframe matrix — if `len({r["timeframe"] for r in ordered_records}) > 1`, either split or emit per-timeframe and log loudly. Do not silently column_stack across timeframes.
- Also recommend carrying a real OOS timestamp index (or at least per-block bar ranges) instead of pure `relative_equal_blocks`, so downstream tools can verify row comparability.

---

## MEDIUM

### M1 — ATR-anchor "keep best of 3" is max-over-trials selection; multiple-testing is *counted* but, with `min_dsr=0.0`, *not rejected* — only softly down-weighted
**Where:** expansion `pm_pipeline.py:2214-2236`; per-anchor candidates appended `:2289-2299`; top-K dedup keeps MAX train score per strategy name `:2424-2426`; winner selection takes max over all anchor variants `:2837-2917`. Trial accounting: `screening_trials_evaluated` increments once per anchor `:2259`; folded into `local_search_trials_total` at `:2320` and `:2388`; used by `_effective_search_trials` `:2743-2754` and the DSR `effective_trials` `:2771-2783`.

**Why:** Trying 3 anchors and keeping the best is exactly the multiple-testing the DSR is meant to deflate, and to the auditor's relief the anchor count *is* propagated into `effective_trials` (so the DSR/confidence math sees the wider search). That is correct and is the right breadth (not understated). HOWEVER, with the live `false_discovery_min_dsr=0.0`, there is no hard rejection (`:2895-2905` requires `min_dsr > 0.0`); the only defense is the `confidence_multiplier`, which is floored at `confidence_floor=0.35` (`:2797-2801`) and only applies when `val_sharpe>0`. So a strategy that only "passes" because anchor=3.0 happened to fit the validation window can still win a regime with at most a 0.65× score haircut and zero rejection. This is *evidence-only by design* (consistent with C3 intent) but the operator must understand that the ATR-anchor multiple-testing is currently mitigated only softly.

**Fix recommendation:** No code bug. For the live capital run, consider setting `false_discovery_min_dsr` to a small positive value (e.g. 0.05-0.10) so the anchor max-selection has a real floor, OR document explicitly that anchor expansion relies on the downstream holdout `block` gate (M-area A2 below) rather than the DSR for overfit protection. At minimum, log per-regime when the winner's `screening_atr_anchor_mult` differs from the 2.0 default, so anchor-driven winners are auditable.

### M2 — `min_dsr=0.0` "evidence-only" mode is correct but under-logged; nothing surfaces the DSR value when it is NOT used as a gate
**Where:** selection gate `pm_pipeline.py:2893-2905` (skipped entirely when `min_dsr==0`); promotion gate `pm_pipeline.py:319-328` (also requires `min_dsr>0`); evidence is attached to `validation_evidence["false_discovery"]` (`:2988`, `:3221-3222`) but the only INFO/log line for DSR is the *rejection* path (`:2901-2904`), which never fires at `min_dsr=0`.

**Why:** The intent (evidence-only, no hard rejection) is correctly implemented and the DSR is persisted in evidence — so it is auditable post-hoc. But during a live run there is no log signal that DSR was computed/considered, which makes it easy to mistakenly believe C3 is "off". The confidence multiplier IS applied to score (`:2911-2912`) even at `min_dsr=0`, so the gate is not a no-op — it silently reshapes scores. That combination (silent score reshaping + no rejection log) is a trust hazard.

**Fix recommendation:** Emit a one-line INFO per regime winner with `deflated_sharpe`, `effective_trials`, and `confidence_multiplier` whenever `false_discovery_control_enabled` is true (not only on rejection). State in config docs that `min_dsr=0.0` = evidence + soft score haircut, NOT inert.

---

## LOW

### L1 — Strategies that do not consume `sl_atr_mult`/`tp_atr_mult` produce identical anchor variants, wasting compute and (harmlessly) inflating `effective_trials`
**Where:** `pm_pipeline.py:2226-2231` blindly sets `params['sl_atr_mult']`/`['tp_atr_mult']` for every base strategy. Params are real for the common exit base (`pm_strategies.py:647-648`), but a strategy whose `get_params()` ignores these keys yields 3 functionally identical variants. They get distinct `candidate_id`s (params differ in the dict) and each increments `screening_trials_evaluated`.

**Why:** Mild — adds redundant screening backtests and slightly raises the DSR trial count (conservative direction, not dangerous). Could also produce 3 near-duplicate columns in the OOS matrix (only if they trade) which mildly dilutes PBO ranks.

**Fix recommendation:** Skip anchor expansion when neither `sl_atr_mult` nor `tp_atr_mult` is present in `strategy.get_params()`, or when the produced params equal the base params.

### L2 — Anchor variants set `sl_atr_mult == tp_atr_mult` (forced 1:1 R:R), which is a narrower probe than "expand the exit range"
**Where:** `pm_pipeline.py:2228-2229`.

**Why:** Not a bug, but worth flagging vs. prior behavior: the docstring (`:2217-2219`) says the goal is to avoid rejecting a strategy "solely because its default 2xATR exit was the wrong starting range." Setting SL=TP at 1.0/2.0/3.0 only probes symmetric exits; a strategy needing an asymmetric range (e.g. tight SL / wide TP) is not rescued. Apparent-score inflation risk is therefore low (it is not searching a rich 2-D exit grid at screening), which is good for selection bias but means the feature does less than the prose implies.

**Fix recommendation:** None required; align docstring with the symmetric-anchor reality, or note that asymmetric exits are left to Phase-2 tuning (which is correct — tuning uses the full `get_param_grid`, `:2371`).

### L3 — OOS manifest includes non-deterministic `created_at`, slightly reducing manifest-level reproducibility
**Where:** `pm_pipeline.py:3972`.

**Why:** The npz matrix is content-addressed (`matrix_digest_sha1`, `:3949`) and verified by `tools/pbo.py:122-128`, so the numerical artifact IS reproducible and tamper-evident. Only the manifest JSON has a wall-clock field, so two identical runs produce byte-different manifests. Minor.

**Fix recommendation:** Either drop `created_at` from the hashed/compared surface (it already is not hashed) or accept as-is; document that reproducibility is asserted on `matrix_digest_sha1`, not the manifest bytes.

---

## Things explicitly VERIFIED as correct (so they are not re-flagged)

- **DSR math** (`pm_pipeline.py:2711-2740`): standard Bailey/López de Prado deflated Sharpe; `se_sr` uses the correct skew/kurtosis SR-variance formula; `E[max SR]` uses the iid-normal expected-maximum approximation with the Euler–Mascheroni correction; returns a CDF in [0,1]. `n_trades<5 or n_trials<1 → 0.0` is a safe floor. Trial count is the *wider* search breadth (anchors + tuning + scope multiplier), so it is not understated and not double-applied (the candidate's own `search_trials` is folded in via `max`, `:2782-2783`, not added twice).
- **Effective trials breadth** (`:2743-2783`): prefers the declared `local_search_trials_total` (= screening incl. anchors + tuned trials, `:2388`), then scales by `scope_multiplier ** scope_power` (symbol×tf×regime, `:2756-2764`). Correct breadth; conservative.
- **Legacy behavior preserved when disabled**: `enabled=False` ⇒ `effective_trials = local_trials` (`:2780-2781`), `confidence_multiplier = 1.0` (`:2797-2801`), selection gate skipped (`:2895-2896`), promotion gate skipped (`:319`). No degradation when off.
- **No selection-time val leakage in screening top-K**: top-K scoring uses `_compute_regime_score(train_metrics, {})` (train only, `:2422`); val only enters at the final `_select_best_for_regime` scoring (`:2887-2910`), which is the legitimate validation surface, not the holdout.
- **Anchor params carry downstream correctly**: winner `RegimeConfig.parameters = cand['params']` (`:2974`, `:3016`) = the chosen anchor variant's params (`screening_strategy.get_params()`, `:2292`).
- **OOS no-trade-as-zero claim is TRUE and correct**: `_build_oos_evidence_record` (`:2012-2019`) emits a zero-return column ONLY when the candidate produced 0 validation trades but already passed screening (no-trade *screening* candidates are dropped at `:2268` and never reach export). Zero columns have std=0 ⇒ `_block_perf` returns 0 (`tools/pbo.py:30`) ⇒ never IS-best, so they cannot win selection but still count toward N, which prevents PBO from being inflated by silently dropping losers. Correct.
- **candidate_id stable & unique**: `_stable_hash` over `{symbol, timeframe, strategy_name, parameters}` (`:2024-2030`, sorted-keys JSON sha1, `:98-101`). Anchor variants differ in `parameters` ⇒ distinct ids; cross-timeframe same-params collisions impossible (timeframe in key); dedup by id in `_attach` (`:2106`) and `_write` (`:3928`). Tested at `tests/test_optimizer_hardening.py:283-332`.
- **Manifest/matrix column ordering aligned**: both derive from the same `ordered_records` sorted by candidate_id (`:3929`); `matrix_column` assigned in that order (`:3965`); `pbo.py` re-checks counts/digest (`:119-128`). No misordering between ids and columns.
- All 17 tests in `tests/test_optimizer_hardening.py` pass under the current tree.

---

## ACCURACY VERDICT (per area)

1. **Screening ATR-anchor expansion — trustworthy.** Expansion, dedup-by-best-anchor, downstream param carry, and trial accounting are correct with no lookahead. The max-over-3-anchors selection bias IS counted into the DSR breadth; residual risk (M1) is that with `min_dsr=0.0` it is only softly penalized, plus minor waste (L1) and a doc/prose mismatch (L2).

2. **C3 false-discovery DSR gate — trustworthy.** DSR math is correct, trial breadth is right (not understated, not double-applied), `min_dsr=0.0` is genuinely evidence-only (no hard rejection) and legacy behavior is preserved when disabled — all intended. Caveat (M2): the confidence multiplier still reshapes scores at `min_dsr=0` and that is under-logged, so it can be mistaken for inert. No path found that wrongly rejects a good candidate or hard-passes an overfit one at the current settings (rejection requires `min_dsr>0`).

3. **OOS candidate evidence export for PBO — not-yet-trustworthy.** Values are leak-free and the no-trade-zero-column design is correct, but the matrix is assembled across MULTIPLE timeframes for a symbol onto a single `relative_equal_blocks` row axis (H1), so the CSCV PBO is computed on a misaligned matrix. Fix the per-timeframe partitioning (or block-mask) before relying on the PBO number for live-capital trust decisions.
