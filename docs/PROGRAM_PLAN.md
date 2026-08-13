# Program Plan & Investigation Roadmap — Scoring Discovery + Codebase Quality

**Owner:** Bongo · **Executors:** Claude (analysis/verify/<20min) + Codex (engine/runs/>20min)
**Status date:** 2026-06-07 · **Living document** — update on every completed tangible.

> 2026-06-09 production split note: the current production contract is
> `production_split_mode="bleeding_edge"` with adjacent WFV and no production clean holdout.
> Use `docs/PRODUCTION_SPLIT_AND_PROCESS.md` and
> `docs/audit/production_split_process_20260609.html` as the canonical split/process references.
> Older clean-holdout/disjoint language in this roadmap is historical or research-only.

> Purpose: a single, tickable product-management record so (a) settled findings are never
> re-litigated (no regression), (b) the active work is unambiguous, and (c) the forward
> investigation tree and its branch outcomes are pre-registered. Scoring is closed;
> the codebase-quality workstream (§5) is now the primary thread so we end with a
> materially safer live engine.

**Status legend:** `[ ]` todo · `[~]` in progress · `[x]` done/verified · `[-]` rejected/dropped (reason given)

**Doc map (no duplication):** `phase1_scoring_spec.md` = scoring *design* spec · `ACTIVE_DISCOVERY_STATE.md` = operational *run state* · **this file** = investigation roadmap + tangibles tracker. ACTIVE_DISCOVERY_STATE should link here.

---

## 1. North star & invariants (what everything is measured against)

**Goal.** A trustworthy, scientifically rigorous **scoring methodology** that selects, per
`symbol × timeframe × regime`, the already-optimized strategy most likely to **carry to live** —
best *net-profit impact* **and** reliable, stable, consistent — generalizing to a ~100-symbol
universe. Strategies and their parameter chains are **fixed**; we change *only* the scoring.

**Invariants / guardrails (do not violate):**
- **Leakage boundary:** inner Optuna/TPE objective uses train + validation + candidate evidence only; holdout/historical/forward are downstream-only; TPE-stationary (no pool rank inside the trial).
- **General, not tuned:** no per-cell or per-symbol thresholds; no manufacturing candidates; no manipulating results to look good.
- **PER-SPECIALIST EVALUATION (HARD RULE — non-negotiable):** the unit of selection *and* evaluation is the **(symbol × timeframe × regime) specialist** — each is its own expert for *that* niche, by design. Every specialist is judged on its OWN merits, on its OWN regime-appropriate honest history — profitable, edge-reliable, consistent, low-drawdown. **NEVER aggregate across cells:** do not combine different regime/timeframe winners (or symbols) into a median/mean/portfolio and expect a "sweet" net outcome; no good specialist may offset a bad one. A scorer passes a cell only if THAT specialist is a genuine live-carrier for its niche, and must **NO_TRADE** the cell when none exists.
  - *Learn-vs-evaluate distinction:* finding **patterns across many cells** to train/generalize the scorer is allowed and encouraged (that's how the model learns). **Aggregating performance across cells to *judge* an outcome is forbidden.** Patterns inform the model; the verdict is always per-specialist.
  - *Implication:* the honest label/replay for each specialist must be **regime-appropriate** (a TREND-on-H1 specialist evaluated on its TREND-on-H1 occurrences), not a single all-regime blend of the symbol champion.
- **Honest NO_TRADE** beats promoting a churner when nothing carries.
- **Default stays `legacy`** until a candidate scorer improves the per-specialist carrier decision:
  better winners inside the same `symbol x timeframe x regime` cells, fewer bad carriers, and
  better honest NO_TRADE decisions where no specialist carries.
- **Pre-register acceptance bars** before each run (no goalpost-moving).
- **Adequate n** for any statistical claim (the n=6 lesson).
- **Independent verification** of every claim; **don't edit engine files during a live run.**

**Pre-registered acceptance - PER-WINNER, NO AGGREGATION (supersedes the old "robust bar").**
Each selected winner is judged individually on its honest regime-appropriate life: profitable,
edge-reliable, consistent, low-DD, and not churn-dependent. A scorer is judged by the cell-level
decisions it makes: `GAINED_GOOD_CARRIER`, `RETAINED_GOOD_CARRIER`, `LOST_GOOD_CARRIER`,
`NEW_BAD_CARRIER`, `RETAINED_BAD_CARRIER`, `NO_TRADE_BOTH`, and `INSUFFICIENT_EVIDENCE`.
`DROPPED_BAD_CARRIER` is a valid improvement when a scorer abstains from a bad baseline cell.
Averaging or taking medians across winners is forbidden. One great winner never offsets one bad
specialist. Under this rule even `legacy` remains a baseline to beat, not a sacred answer: any
individual bad carrier is still a fail for that cell.

---

## 2. Investigation LOG — settled findings (NO REGRESSION)

Do not re-open these without new evidence.

| # | Investigation | Outcome | Evidence |
|---|---|---|---|
| L1 | Legacy scoring frequency bias | Legacy rewards trade count (freq bonus + `-train_total_trades` tie-break) | pm_core freq bonus; pm_pipeline tie-break |
| L2 | `specialist_spokes_asym` | **[-] REJECTED** — trimmed-mean core destroys the geomean weakest-link veto | honest LFL: mean −23.3% / median −17.4% / 0-of-4 |
| L3 | `specialist_spokes_v2` | **[-] REJECTED** — multiplicative `1−k·penalty` self-cancels for survivors (factor 0.94) | honest LFL: −23.4% / −12.5% / 0-of-4; GBPUSD −61.65% |
| L4 | Root-cause assessment (5 agents) | `val` (stage2) was nested in `train` (in-sample); `temporal_consistency` volume-gameable; evidence windows too short; **geomean aggregator is sound** | 5-agent reports; line-level provenance |
| L5 | Metric-integrity bugs | **[x] FIXED+verified** — equity-curve phantom-zero/desync on invalid sizing; warmup dilution of Sharpe/Sortino | pm_core invalid-entry paths carry equity fwd; warmup-trim L5766 |
| L6 | Disjoint evidence surface + embargo | **[x] RESEARCH-ONLY** — production now uses bleeding-edge optimization with adjacent walk-forward validation; `train→[300]→val→[300]→clean_holdout` remains an explicit research profile only | pm_core DataSplitter; pm_pipeline slicing |
| L7 | `timeframe=""` in Optuna objective | **[x] FIXED+verified** — all objective `.run()` pass real timeframe; dormant single-strategy path also fixed | pm_optuna 10 call sites |
| L8 | `selection_validation.py` forward-leak | **[x] FIXED** — gate uses pre-forward evidence only, explicit guard comment | tools/selection_validation.py:208 |
| L9 | ThreadSpark v1 (disjoint + holdout selection + historical face + tensor) | **[-] FAILED bar** — picked high-freq churners; GBPUSD −49.2% | LFL 20260604: median −1.59%, 3/6, 6087 tr |
| L10 | ThreadSpark v1.1 (path_persistence + full-history chunk faces + no tie-break) | **[-] FAILED bar — worse churn** (9927 tr) | LFL v11: median −1.59→ mean +116%/median +32.6% mirage, GBPUSD −50.6% |
| L11 | Granular cell analysis | Upside (+171%) and churn (−50%) are the **same high-frequency mechanism**; the 3-D shape geometry structurally favors high-frequency | per-cell `regime_configs` drill |
| L12 | Blind independent agent (n=6) | Best lead = **edge-TREND / OOS-return slope** (strengthens=hold, decays=fall-off); directionally perfect 3v3 but **unprovable at n=6** (p≥0.19, window-fragile, multiple-testing) | agent report; npz slope analysis |
| L13 | Candidate-pool edge-persistence test (n=4,194) | **[-] edge-TREND REFUTED for the all-regime label set** (AUC 0.571 pooled / 0.433 min / window-fragile). `oos_mean_return` was the strongest legal all-regime predictor, but the labels were not regime-gated, so this is research evidence only and not the final per-specialist verdict. `profit_factor`/`sharpe` AUC~1.0 are CIRCULAR (full-history outcomes), not predictors | §3.4 symbol-held-out AUC; label-source caveat |
| L14 | Meta-labeling / data-driven selector on regime-gated per-specialist labels (n=722, 290 carriers) | **[-] REJECTED on the honest per-cell bar.** Signal real but MODERATE (best `all_oos_sharpe` AUC 0.767; regime-matched `calmar` 0.746, `mean_r` 0.715; matched-scope did NOT beat all-regime). SYMBOL-INCONSISTENT per-cell: top-1 carrier-hit works on XAUUSD 0.94 (cost-suspect, 17/17 cells carry)/USDJPY 0.75/EURUSD 0.71 but BREAKS on GBPUSD 0.29 (gate 0.00)/AUDJPY 0.08; ex-XAUUSD only 46%. | §3.4 matched-scope AUC + per-cell gate test |

| L15 | W5 production-book promotion-readiness gate drift | **[x] FOUND+FIXED in post-run readiness.** Original generated completion report/promotion JSON counted `6` final `PROMOTE`; per-cell join found `2/6` unsafe (`CHFJPY|H4|CHOP`, `EURJPY|H4|TREND`) because blocked/negative historical evidence was not a final blocker. Corrected W5 gatefix is `PROMOTE=4`, `REVIEW=10`, `REJECT=293`; unsafe `PROMOTE` cells `0`. | `*_promotion_gatefix.json`; per-cell triage gatefix MD/JSON; `AGENT_COMMS.md` 2026-06-07 |
| L16 | Promotion/completion trust-boundary hardening | **[x] IMPLEMENTED+FULL-SUITE VERIFIED.** Promotion review now requires explicit `review_ready` markers in automated paths, validates dead PID/no memory-pressure stop/ledger path+count/OOS coverage, and completion reports ignore stale safety JSON unless the current cell audit succeeds. Legacy benchmark compare now rejects matching partial records. | `tools/promotion_review.py`; `tools/discovery_completion_monitor.py`; `tools/watch_discovery_run.py`; `tools/benchmark.py`; focused tests; full suite `1156 passed, 5 skipped` on 2026-06-07 |
| L17 | Production live book and readiness gate | **[x] BUILT+VERIFIED+CONSOLIDATED.** Generated the corrected production live book from W5 gatefix artifacts and consolidated the active runtime path to `config.json` plus `pm_configs.json`. The default CLI config no longer points at stale high-risk artifacts. Only four specialist cells are tradeable; all other cells abstain. Per-cell audit: 96 cells, 92 `NO_TRADE_ABSTAINED`, 2 `PASS_STRICT`, 2 `PASS_THIN`, unsafe promotes `0`. Readiness is live-ready for the pinned FBS-Demo identity; watchdog correctly reports stale telemetry as `CRITICAL` when no live loop is running. | `tools/build_production_live_book.py`; `tools/live_silent_watchdog.py`; `production_live_w5_specialist_cells.json`; full suite `1182 passed, 5 skipped` on 2026-06-07 |

**SCORING SEARCH CLOSED (2026-06-06, Bongo's call).** Every evidence-motivated scoring approach — `specialist_spokes`, `_asym`, `_v2`, `specialist_spokes_threadspark` (v1+v1.1), the edge-trend lead, and the data-driven meta-labeling selector — was tested and **REJECTED on the honest per-specialist/per-cell bar**. `legacy` (with the trade-frequency bonus **OFF** — the configuration actually validated in every like-for-like) is the production scorer. **No regression: do not re-open any of the above without genuinely new evidence.** The active workstream now pivots to §5 (codebase quality + live-capital safety), which improves the system regardless.

**Net (post-L13 + hard-rule correction):** L13 refutes the all-regime edge-trend hypothesis, but it
does not close the per-specialist scoring problem because the labels blend regimes. The next valid
training/evaluation surface must be per `(symbol, timeframe, regime)` and regime-gated. `legacy`
remains the conservative default baseline until a per-cell scorer proves better on individual carrier
decisions.

---

## 3. CLOSED investigation — Candidate-Pool Edge-Persistence Test

**Pre-registered hypothesis:** a candidate's *edge-trend* (OOS-return slope / `late_gain_share`),
measurable **at selection time**, predicts whether it **holds** its edge over full history —
testable at real n (~4,000 candidates) where n=6 cannot separate signal from luck.

| | Tangible | Owner | Status |
|---|---|---|---|
| 3.1 | Confirm feasible N + ETA for candidate-pool labeling | Codex | `[x]` — 4,194 candidates; actual runtime ~452.8s on 4 workers |
| 3.2 | Full-history `conservative_max` replay → label every candidate (holder/fall-off) + emit return/PF/trades/DD keyed by `candidate_id`/`matrix_column` | Codex | `[x]` — `candidate_pool_labels_combined.json`; 4,177 ok / 17 no-signal |
| 3.3 | Pre-compute predictors | Codex+Claude | `[x]` — predictors emitted in labels file |
| 3.4 | Symbol-held-out AUC test | Claude | `[x]` — see result below |
| 3.5 | **Decision gate** (pre-registered) | Bongo | `[x]` — scoring search closed by L14; keep legacy freq-off as production scorer |

| 3.6 | Regime-gated per-specialist labels for meta-labeling | Codex | `[x]` - `candidate_pool_regime_labels_codex/candidate_pool_labels_combined.json`; 722 ok regime specialists / 3,472 skipped non-specialists / 0 errors; all ok rows use `conservative_max` label cost and `regime_gated` scope |

**3.4 RESULT (n=4,194, all 6 symbols):**
- **Edge-trend REFUTED.** `oos_slope` AUC = 0.571 pooled, **0.433 min** (below chance on GBPUSD/EURAUD); `oos_first32_slope` 0.496 vs `oos_last32_slope` 0.589 = **window-fragile**. The n=6 split was luck. `[-]` edge-persistence hypothesis dropped.
- **All-regime signal = OOS-return LEVEL** (`oos_mean_return` AUC **0.839**, min 0.763, generalizes all 6) — useful research evidence, but not a final scorer verdict because the labels blend regimes.
- **TRAP avoided:** `profit_factor`/`sharpe` AUC ~1.0 are **circular** (full-history outcomes = the label), NOT predictors.

**Decision gate 3.5:** all-regime edge-trend has no robust OOS power. Do not use this as a final
per-specialist verdict. The next valid dataset must label each `(symbol, timeframe, regime)` specialist
on regime-gated honest history.

**3.6 RESULT (regime-gated truth set):**
- Built with `tools/label_candidate_pool.py --label-scope regime_gated`.
- Output: `pm_outputs/experiments/threadspark_like_for_like_v11_20260605/candidate_pool_regime_labels_codex/candidate_pool_labels_combined.json`.
- Counts: 4,194 rows total; 722 `ok` tuned specialists; 3,472 skipped non-specialists; 0 errors.
- Labels: 295 holders, 290 carriers, 184 strict carriers.
- Scope/cost invariant: all ok rows have `label_scope=regime_gated`, `label_cost_basis=conservative_max`,
  `predictor_cost_basis=conservative_max`, and `predictor_regime_scope=all_regimes_validation_equity`.
- This is the admissible training/evaluation label surface for meta-labeling under the hard rule.

**3.7 RESULT (filtered meta-label predictor check):**
- Tool: `tools/meta_label_validation.py`.
- Output: `pm_outputs/experiments/threadspark_like_for_like_v11_20260605/candidate_pool_regime_labels_codex/meta_label_validation_codex.json`.
- Guardrail: filters to `status=="ok"` before any AUC or per-cell selector diagnostic; skipped non-specialists are excluded.
- On 722 specialists / 290 carriers, the signal is moderate, not transformative:
  `all_oos_sharpe` AUC 0.767, `all_oos_sortino` 0.761, `all_oos_mean_return` 0.746,
  `val_reg_calmar_ratio` 0.746, `val_reg_mean_r` 0.715, `val_reg_sharpe_approx` 0.631.
- Per-cell within the ThreadSpark candidate pool: 88 cells, 65 with at least one carrier.
  Best single-feature selector (`val_reg_return_pct`) picked carriers in 50/65 oracle-carrier cells;
  `all_oos_sharpe` picked 49/65. Useful signal, but not a scorer verdict.
- Scope caveat: this experiment has candidate OOS manifests only for ThreadSpark, so this is a
  within-pool research diagnostic. A true legacy-vs-learned verdict requires integrating the selector
  into the selection path or creating a shared candidate/evidence surface.

---

## 4. Forward investigation TREE (branch by §3 outcome)

### 4.A — IF edge-persistence is CONFIRMED
- `[ ]` Design edge-trend as a **general** spoke or selection-time gate (leak-clean, TPE-stationary, threadspark-only so baselines stay frozen).
- `[ ]` Codex implements; Claude audits scoping + leak-cleanliness.
- `[ ]` Re-run like-for-like (single arm, frozen baselines) against the **per-cell carrier gate**; require it to keep/gain individual good carriers, avoid bad carriers, and abstain where no carrier exists.
- `[ ]` Generalization: confirm on a wider symbol set; window-stability; thin-candidate confidence weighting.
- `[ ]` Further angle: multi-objective "best = net-profit-impact **and** reliable" — combine edge-trend with the surviving quality spokes; verify it doesn't reintroduce churn.

### 4.B — IF edge-persistence FAILS on regime-gated per-cell labels
- `[ ]` Keep `legacy` as the production scorer (documented as the conservative baseline).
- `[ ]` Archive the ThreadSpark line as a **tested dead-end** (keep the full record in §2).
- `[ ]` Redirect to §4.C scoring angles and/or the codebase workstream (§5).

### 4.C — Parked scoring angles (not yet investigated; pull from here as needed)
- `[ ]` **Plateau robustness** — score the neighbourhood of top Optuna params (perturbation + walk-forward survival), prefer broad profitable regions over spikes.
- `[ ]` **Utility master objective** — sample-aware fractional-Kelly/CRRA with tail+DD penalty; shrinkage `k` fixed a priori, never grid-searched.
- `[ ]` **Legacy confidence / no raw frequency bonus** — L13 suggests trade-volume is noisy in the all-regime label set; test only as a bounded, per-cell run. A candidate variant must improve individual carrier decisions, not median/mean performance.
- `[ ]` **9th spoke / conditional upside capture** — only as a tie-breaker among already-broad survivors, never a primary return-chaser.

---

## 5. Codebase-quality workstream (parallel — "far better codebase")

### 5.1 Correctness / leakage — mostly done
- `[x]` Equity-curve phantom-zero/desync (L5) · `[x]` warmup dilution (L5) · `[x]` disjoint surface + dual embargo (L6) · `[x]` `timeframe=""` objective (L7) · `[x]` selection_validation forward-leak (L8).

### 5.2 Live-capital safety — PRIMARY
- `[x]` Ambiguous-send double-fill reconciliation (pm_main / pm_mt5): ambiguous timeout/unknown send results now reconcile by live symbol+magic position before logging failure; unreconciled ambiguous sends fail closed with a cooldown block. Test-locked in `test_config_source_of_truth`.
- `[x]` Hard realized-loss kill-switch (independent of discovery): LiveTrader hard-blocks configured daily/session/weekly realized-loss thresholds even when the optional institutional supervisor is disabled. Test-locked in `test_portfolio_risk_cap` + realized-loss date-window tests.
- `[x]` `config.json` git-tracked + MT5 creds hazard: tracked config has blank MT5 login/password/server, `.gitignore` protects local/private configs, config integrity tests enforce blank tracked credentials, and a non-disclosing git-history scan found 0 nonblank credential matches.
- `[x]` Production live book generated from corrected W5 cells only: `config.json` points to the
  universal root ledger `pm_configs.json`, with no broad default fallback.
- `[x]` Realized-loss hard block now fails closed when broker deal history is unavailable; manual new-entry
  kill switch added; primary `order_send(None)` path is marked ambiguous.
- `[x]` Live readiness now requires non-secret MT5 account identity pinning (`expected_login`,
  `expected_server`, `expected_currency`) before `--trade`; current production config is intentionally
  blocked until the operator fills these values in a private/local config and verifies the connected account.
- `[x]` Independent live silent watchdog added: `tools/live_silent_watchdog.py`; stale live sweep/action/trade
  telemetry is a non-OK status with warning/critical exit codes.

### 5.3 Clean-coding hygiene
- `[x]` Bare `except:` / silent passes in touched surfaces; artifacts record the fixed canonical scoring contract as `legacy`.
- `[ ]` Remaining 2 benign `except Exception: pass` (pm_core:5219, pm_pipeline:2086) — low priority.
- `[ ]` Extract `_coerce_int/_coerce_float` helpers for the ~40 config-coercion blocks (maintainability; narrow excepts to `(TypeError, ValueError)`).

### 5.4 Spoke-definition quality (only if scoring continues, §4.A)
- `[~]` `temporal_consistency` → `path_persistence` (done in THREADSPARK roster only).
- `[ ]` `execution_cost_survival` "absent → 1.0 neutral" free pass — remove.
- `[ ]` `parameter_stability` inert (defaults 1.0) — wire real Optuna-neighbourhood evidence or demote to a gate.
- `[ ]` `tail_risk_control` saturating (cannot reach geomean floor) — strengthen so a tail blow-up can veto.

### 5.5 Tests & regression guards
- `[x]` Named regression fixture principle: a mixed fixture with one great specialist (+460%) and one bad churner (-50%, 1132 trades) cannot pass by aggregation; the bad cell remains a blocker.
- `[x]` Encode the pre-registered per-cell carrier gate as an automated comparison check: taxonomy now covers `GAINED_GOOD_CARRIER`, `RETAINED_SAME_GOOD_CARRIER`, `REPLACED_WITH_GOOD_CARRIER`, `NEW_BAD_CARRIER`, `RETAINED_BAD_CARRIER`, `DROPPED_BAD_CARRIER`, `NO_TRADE_BOTH`, and blockers.
- `[x]` Circular-feature guard: full-history outcome fields (`full_history_return_pct`, `profit_factor`, `sharpe`, `max_dd_pct`, `trades`) are forbidden as meta-label predictors; requesting them must fail.
- `[x]` Production scorer is a single test-locked `legacy` path; `scoring_use_trade_frequency_bonus=false` remains fixed.
- `[x]` Reporting guardrail follow-up: completion/post-run monitors now cross-check promotion `PROMOTE` rows against the specialist-cell audit before emitting a promotable headline. The W5 `*_completion_report.md` "6 cleared" line remains a stale pre-gatefix artifact and is now marked as such; use the gatefix artifacts for W5 readiness.
- `[x]` Keep the focused suite green; **no test-weakening without explicit sign-off** (watch for assertion edits alongside code).
- `[x]` Production-readiness guardrail tests added for MT5 identity pinning, production promotion mismatch,
  production live config pins, corrected promoted cells only, `order_send(None)` ambiguity, realized-loss
  unknown fail-closed, manual kill switch, and live watchdog stale/fresh behavior. Full suite after this
  pass: `1169 passed, 5 skipped, 2 warnings, 575 subtests`.
### 5.6 Docs & alignment
- `[x]` `phase1_scoring_spec.md` = canonical ThreadSpark design.
- `[x]` This `PROGRAM_PLAN.md` created.
- `[x]` `ACTIVE_DISCOVERY_STATE.md` links phase1 spec + this plan; retire/merge any stale roadmap duplicates.

---

## 6. Roles, cadence & method

- **Claude:** analysis, independent verification, leak-clean/stationarity audits, statistical/separator tests, robust-bar verdicts. Jobs < 20 min.
- **Codex:** engine changes, the big runs (discovery, labeling, full-history replays). Jobs > 20 min.
- **Independent agents:** blind cross-checks at decision gates (bias guard — see L12).
- **Comms:** `AGENT_COMMS.md`, high-frequency; every code change audited before any number is trusted; frozen baselines reused across iterations (no re-running unchanged arms → no Optuna drift).

---

## 7. Risk register / guardrails (recurring failure modes to watch)

| Risk | Guard |
|---|---|
| Goalpost-moving | Pre-register bars in this doc before the run |
| Pattern-forcing on small n | Adequate n + held-out validation + independent blind agent |
| Per-cell/per-symbol tuning | General principles only; reject cell-specific rules |
| In-sample glamour | Honest disjoint surface + full-history replay as the verdict |
| Optuna baseline drift | Reuse frozen baselines; single-arm reruns |
| Silent metric corruption | Audit every engine change; keep suite green |
| Test-weakening to pass | No assertion edits without sign-off (5.5) |
| Leak via edits mid-run | No engine edits during a live run |

---

## 8. Definition of done — "far better codebase than we started"

1. **Scoring:** either a *proven-better* scorer (improves per-specialist carrier decisions, generalization-validated) **or** a rigorously-validated decision that **legacy remains the best baseline**, with dead ends documented in §2. Both are acceptable, honest outcomes.
2. **Engine:** leakage-clean, metric-integrity-correct, honest-surface, live-capital-safe (§5.2), clean-coded (§5.3), regression-tested (§5.5), and documented (§5.6) — improved **regardless** of the scoring verdict.
3. **Process:** every claim independently verified; no un-recorded regressions; this document current.
