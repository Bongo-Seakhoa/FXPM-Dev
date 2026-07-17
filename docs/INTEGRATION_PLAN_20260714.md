# Integration Plan — Wiring the Standalone Authorities (v2, JOINT ORDER)

Status: v2 incorporating Codex seq772 corrections in full (no disputes).
Supersedes v1. Claude answers on the bus and claims `pm_pipeline.py` only
after this v2 is recorded. Author: Claude, 2026-07-14.

Goal (operator core objective): first WORKING end-to-end process —
historical anchors → causal PASS → current book → readiness — with a
mechanical smoke canary first. All authorities currently have zero
production callers.

---

## 1. Authority status (post 2026-07-14 review wave)

| Authority | Status |
|---|---|
| `pm_winner_selection` v7 | ACCEPTED + RELEASED (seq735) |
| `pm_mt5` V3 | ACCEPTED + COMPLETED + RELEASED (seq767/770/771) |
| `pm_book_multiplicity` V6 | frozen seq763; independent ACCEPT (Claude seq773); pinnable by consumers only after Codex RELEASE |
| `pm_causal_process` | superseded twice (alias-domain audit; then seq829 executable defects: arithmetic-estimand passes compounding loser, per-anchor conjunction power collapse). Schema moves to **decision-v4**: one precommitted confirmatory stressed-LOG-growth LCB with variance = max(primary HAC, sensitivity HAC, between-anchor cluster), newest-anchor direction only, top-block diagnostics non-veto. Consumers pin decision-v4 when frozen/accepted |
| `pm_model_lifecycle` / `pm_specialist_arbitration` | implemented (seq728); specialist-owned; seam review pending repaired causal |
| `pm_training_horizon` | accepted |
| `pm_cap_calibration` → `pm_live_readiness` | wired |
| Live runtime arbiter | JOINT spec v2 (`LIVE_RUNTIME_ARBITER_SPEC_20260714.md`); build in integration, code claim separate |

## 2. Corrected process topology (Codex seq772 §2 — normative)

**Historical phase (repeated at EACH as-of anchor):**

```
activated/continuous native snapshot (as-of anchor)
  → pm_training_horizon (windows, embargo, interior folds)
  → FULL strategy/parameter/depth search           [optimizer, §4 evidence contract]
  → Winner V10 decisions for the EXACT 100×6×4 roster (pin fxpm.winner-selection.v10, released seq838)
  → Book V6 (complete-family multiplicity ledger)
  → anchor-local fitting
  → frozen arbitration/risk/cost policy
  → synchronized next-period OOS portfolio MTM/event/fill/cost/exposure
    ledgers + reconciliation
```

**Causal comes AFTER:** `pm_causal_process` evaluates the REPEATED frozen
process across all anchors (whole-process validation). It is never a
pre-Winner gate and never a per-run filter.

**Current phase (only after causal PASS):** rerun the IDENTICAL ENTIRE
process through the latest closed cutoff:
Horizon → full search → Winner → exact Book → fit newly selected current
book → lifecycle binding → arbitration → synchronized replay/cap
calibration → readiness.

## 3. Deletion map (Q1 resolved — code_hygiene_v1)

DELETE with their authority semantics (same change as the replacing seam):
- `build_prelockbox_champion_lineage`, `build_lockbox_champion_lineage`
  imports and attach sites (`pm_pipeline.py:87`, `:12718`).
- Legacy champion-refit eligibility path (`~:10857`) and
  lockbox-era temporal-role authority checks that gate refit/promotion.

RETAIN only neutral cutoff/window provenance, renamed to explicitly
non-authoritative records (naming to make non-authority obvious, e.g.
`window_provenance` — final names in the code claim). A decision-v4
receipt validates the whole process; nothing in the pipeline treats it as
a pre-Winner replacement gate.

## 4. Optimizer evidence contract (Q-new — v1's "mechanics unchanged" was wrong)

The current 64-OOS-period export CANNOT satisfy Winner V10. Integration
must make the optimizer generate and persist bounded, replayable evidence
for EVERY attempted and rejected candidate:
- exact search-plan / candidate / data identities;
- at least four non-overlapping ≥63-day folds;
- ≥252 complete daily base activity ledgers plus stress activity ledgers,
  with costs;
- explicit rejection status for every non-surviving candidate.

Memory bound: stream/spill finalized evidence to disk as it is produced;
never retain all DataFrames in RAM; never trade accuracy for the bound
(operator bounded-memory directive).

## 5. Ownership (Q4 resolved)

- **Claude**: ALL `pm_pipeline.py` integration + the diagnostic canary
  driver, in ONE claim/release (no splitting one large file across
  models). Hostile-reviewed by Codex.
- **Codex**: Book/adapters + hostile review of the pipeline seam.
- **Specialist lane**: `pm_model_lifecycle` / `pm_specialist_arbitration`
  modules and seam review.

## 6. Mandatory-before-trust ladder (Q5 resolved; corrected per seq784)

- Before any real optimizer evidence is trusted: a COMPLETE V5 acquisition
  manifest + per-cell provenance + independent continuity/activation
  audit. `pm_native_run_reseal` is NOT on the V5 critical path — it is a
  detached verifier used only when deliberately adopting a legacy V1
  source (which `production_native_v5_20260713` is not); blanket-requiring
  it would add an unnecessary signed legacy-adoption authority and another
  non-constructible key path (the seq778 disease).
- `pm_state_runtime` + exact regime/signal parity: MANDATORY before
  readiness/live.
- These may be omitted ONLY from a clearly labeled mechanical diagnostic
  smoke canary, which must never be cited as production fitness.

## 7. Canary contract (corrected per seq772 §7)

- **EURUSD smoke canary = diagnostics only**: interface correctness,
  resource bounds, failure-ledger behavior. It CANNOT emit an
  authoritative Book/Causal/cap/readiness verdict — with 2,376 cells
  TECHNICAL/UNSEARCHED the exact Book is INCOMPLETE and downstream
  authority is zero by construction (Book V6 fail-closed).
- Authority requires all 100×6×4 cells represented. Economic NO_WINNER is
  valid only after a complete search. TECHNICAL/UNSEARCHED must never be
  relabeled NO_TRADE.
- Canary artifacts are labeled `MECHANICAL_SMOKE_DIAGNOSTIC` end to end.

## 8. Sequencing

1. This v2 recorded on the bus → Claude claims `pm_pipeline.py` (major).
2. Seam order inside the claim: horizon windows → optimizer evidence
   contract (§4) → Winner V10 consumer → Book V7 consumer (after Codex
   RELEASE) → anchor-loop driver → OOS ledger production → causal
   consumer (after fresh causal freeze/ACCEPT) → current-phase rerun path
   → lifecycle/arbitration seam (specialist-owned modules, Claude wires
   the calls) → replay/cap → readiness.
3. Each seam lands with its regression + the §3 deletions for the path it
   replaces; no parallel old/new code persists.
4. Smoke canary after seams 1–5; full-roster historical run is the first
   authoritative run.

## 9. Non-goals

Unchanged from v1: no security hardening beyond fail-closed correctness
(security_scope_v1); no live-order changes before readiness end-to-end;
Book internals stay Codex-owned.

## 10. Seam 1 implementation contract (horizon-request builder — verified against source 2026-07-14)

Authority API (`pm_training_horizon.__all__`): `build_historical_horizon_plan`,
`evaluate_historical_horizon_plan`, `verify_historical_horizon_evidence`.
Request schema `fxpm.historical-horizon-request.v1`; executable fixture at
`tests/test_training_horizon.py::_request` (line ~96).

Pipeline-side builder must produce, per cell per anchor:
- `run_id`, `anchor{anchor_id, ordinal, selection_cutoff_utc,
  forward_start_utc, forward_end_exclusive_utc, evidence_accessed_at_utc,
  process_precommit_receipt_sha256}`, `cell{symbol,timeframe,regime}`.
- `native_prefix`: activation identity hashes (activation_id/manifest/entry/
  artifact/provenance/request_map/source_identity/continuity_policy/
  continuity_receipt sha256s) — sourced from the accepted activation
  manifest; `source_state ∈ {AVAILABLE, SOURCE_UNAVAILABLE,
  TECHNICAL_FAILURE}` (unavailable/technical cells still enter the roster —
  never dropped).
- `dependency policy payload` (schema `...dependency-policy-payload.v1`):
  feature_lookback_bars, label_lookahead_bars, max_holding_bars,
  execution_delay_bars, regime_warmup_bars, cost_lookback_bars,
  max_holding_seconds — derived from the deployment contract per TF.
- Depth `candidates` (the operator's empirical-depth attempt family):
  `lookback_kind ∈ {NATIVE_OBSERVATIONS, ACTIVE_SESSIONS,
  CALENDAR_INTERVAL}` + value + realized_start + folds; every candidate is
  precommitted and pays multiplicity (seq762).
- `folds`: roles `CAUSAL_ROLLING_ORIGIN_INNER` (no right-fit) and
  `INTERPOLATION_ONLY` (right-fit mandatory, right-purged, never final
  OOS). Constraints enforced by `_normalize_fold` (`pm_training_horizon.py:727`):
  left purge ≥ max(label_lookahead·tf, delay·tf + max(holding·tf,
  max_holding_seconds)); warmup window ≥ regime_warmup·tf before
  validation_start; validation_end ≤ anchor cutoff AND ≤ prefix_end;
  fit/validation rows ≤ interval capacity in native bars;
  `final_oos_authority` must be False.
- Digest discipline: every `*_definition_sha256` = SHA-256 of canonical
  JSON (sort_keys, separators (",",":"), ensure_ascii=False, UTF-8) of the
  object WITHOUT its own digest key. UTC format `%Y-%m-%dT%H:%M:%SZ`.

Builder placement: new bounded section in `pm_pipeline.py` (functions
`_horizon_dependency_payload`, `_horizon_depth_candidates`,
`_build_cell_horizon_request`, `_cell_horizon_plan`), plus seam test
`tests/test_pipeline_horizon_seam.py` that round-trips a synthetic-data
request through `build_historical_horizon_plan` (authority is the oracle;
it fails closed with precise codes).

## 11. Bounded-loader contract (seq786 — part of the pm_pipeline claim)

Measured problem: strict-native `_load_symbol_timeframe_data` retains all
six full TF DataFrames in `preloaded_native`/`data_by_tf` before
`run_for_symbol` starts popping (EURUSD V5: 758,013 M5 / 277,980 M15 /
151,372 M30 / 106,684 H1 / 20,545 H4 / 4,523 D1 rows held concurrently).
DataLoader/cache/copy amplification is avoidable live memory.

Required design (operator bounded-memory directive, zero accuracy cost):
1. Resolve activation/calendar metadata FIRST (no frame loads).
2. Lazily load exactly ONE native timeframe at a time.
3. Trim/compute all four regimes + finalized attempt/fold evidence for
   that TF, SPILL finalized evidence to disk, clear DataLoader and
   FeatureComputer caches, then open the next TF.
4. No row/sample reduction; no precision change.
5. Ship a measured-peak diagnostic proving memory does not scale with the
   sum of six frames, and that M5 results are byte/evidence identical to
   the eager control.

## 12. Anchor-local regime fitting (seq788/789 — part of the pm_pipeline claim)

Kills the manual pre-run prerequisite behind the original 0/35 failure:
`pm_regime_tuner.py` + stale `regime_params.json` sidecars are NOT a
production input. Instead, at every historical anchor, BEFORE cell
strategy search:
1. Fit the symbol×timeframe regime detector ONLY on that anchor's
   Horizon-authorized train prefix.
2. Freeze a deterministic **RegimeFit receipt** binding symbol, TF,
   cutoff, source/activation, horizon, feature contract, detector
   algorithm/parameters/code, and the canonical four-regime mapping.
3. Label inner folds / search / OOS with that frozen fit only.
4. Detector alternatives/thresholds are part of the precommitted attempt
   family and pay multiplicity — never a global hidden default.
4b. **One controller per group — two-level contract (seq791 superseded by
   seq813/815).** Each symbol×TF has exactly ONE RegimeFitDecision per
   anchor/current run, shared identically by its four regime experts.
   Level 1 (fit hygiene): each precommitted detector candidate is fit
   causally from price/features only on the authorized anchor prefix — no
   strategy outcomes inside latent-state estimation. Level 2 (family
   selection): detector family/features/threshold policy is selected
   JOINTLY for the 4-regime group via nested inner-validation post-cost
   group log-growth (precommitted economic utility), with all
   detector×strategy×parameter attempts in one multiplicity family.
   Guards (final form, seq816 correction adopted): (a) controller
   validity rejects ONLY statistically non-identifiable/collapsed
   four-state fits, via precommitted detector-estimation requirements —
   dependence-adjusted effective state observations, transitions,
   posterior/state separation, canonical-label uniqueness. NO fixed
   occupancy percentage and NO per-regime holding-horizon requirement:
   that would recreate the sparse-sibling group veto (one genuinely rare
   regime invalidating three sound specialists) and contradict the
   operator's empty-cells allowance. Per-regime trade/horizon power
   belongs to that cell's Winner and may yield NO_LOCAL_WINNER without
   touching the group controller. (b) Group utility is computed on the
   COMPLETE synchronized day axis with inactive regimes/days included, so
   a collapsed labeling cannot manufacture utility. (c) Unsupervised
   separation/persistence baseline is a MANDATORY candidate and the
   conservative default — if the economically selected detector is
   indistinguishable from the simpler baseline after multiplicity, the
   baseline wins. Book enforces one identical controller receipt across
   the 4-cell group (objective-agnostic, seq814). Outer causal validates
   the entire gate-plus-experts process. Live arbiter parity binds to the
   winning controller receipt unchanged.
5. After causal PASS, rerun the same detector fitting through the latest
   closed cutoff before the full current search.
6. REPLACE the `run_for_symbol` static `validate_regime_params_provenance`
   gate and its pre-existing-sidecar requirement (a manual current file
   cannot represent historical anchor-local fits: it either blocks again
   or leaks future information).
7. Technical fit failures are typed cell/source process failures — never
   overwrite the winner book.
8. Regressions: (a) end-to-end run with `regime_params.json` and sidecar
   ABSENT → in-process anchor fit/search proceeds; (b) future-data
   mutation → old anchor labels/receipts byte-unchanged.

Live-arbiter tie-in: the runtime parity-lock (LIVE_RUNTIME_ARBITER_SPEC
§3) binds to the current RegimeFit receipt — one shared function, one
frozen fit, fail closed on drift.

## 13a. Evidence-builder v2 spec (seq976 REJECT of f2ee9ab2 — fix AFTER telemetry artifact clears, per seq977 order)

Defects to kill, each with named regression:
1. **Multiplicity laundering (decisive):** plan must REQUIRE frozen pins —
   `planned_attempt_count`, `process/horizon/trial_registry_sha256`,
   `causal_fold_plan_sha256` — and the builder VERIFIES supplied attempts/
   fold-plan reproduce them (typed `*_PIN_MISMATCH`); never derive truth
   from the post-search subset. Omission attack (drop attempt, keep pins)
   must fail typed.
2. **Exact V10 status vocabulary:** `AUTHENTICATED` / `PROCESS_REJECTED`
   (reason exactly `PREREGISTERED_PROCESS_SCREEN_REJECTED`) /
   `TECHNICAL_FAILURE` (reason `EVALUATOR_TECHNICAL_FAILURE` |
   `SOURCE_TECHNICAL_FAILURE`). Mixed and all-rejected payloads must
   EVALUATE + VERIFY + produce Book V7 marginal receipts in tests (not
   just create non-None).
3. **True pre-materialization bounds:** V10 caps (`MAX_ATTEMPTS`=128,
   `MAX_FOLDS`=12, `MAX_DAYS_PER_FOLD`=1024, `MAX_TRADES_PER_FOLD`=20000,
   `MAX_ABS_DAILY_R` elementwise, stress ≤ base + 1e-12 per day) enforced
   before assembly; FINAL full-request canonical byte bound (request
   to_dict shape incl. overhead) not a running under-estimate; exact fold
   key set (extra keys rejected); trade-axis lengths equal; missing
   attempt fields → typed error, never raw KeyError. 129-attempt payload
   must fail typed BEFORE building.
Scope stays assembly-only. PIN TO WINNER V11 when released: full producer
contract in docs/WINNER_V11_PRODUCER_ADAPTER.md (evidence-ledger v3,
position-events-v2 trade records, per-fold trade_power_binding, failure
mapping technical-vs-process-ineligible, never economic).

## 13. Dead-authority disposition (seq797 — supersedes §1's implicit wire-everything)

`pm_state_runtime` (6,679 ln), `pm_native_lifecycle` (7,799),
`pm_native_composite` (3,092), `pm_native_continuity` (1,513) have ZERO
non-test callers; `pm_native_run_reseal` (1,432) only its detached V1
materializer. 20,515 lines of passing tests are not production
safeguards. Policy:
- Do NOT wire these security-heavy dead authorities into the new critical
  path. V5 source boundary = acquisition manifest/provenance + activation
  audit (consistent with §6/seq784).
- Before readiness, build only the operational capabilities actually
  required, lean: (a) bounded crash-recovery / order-state / checkpoint
  store with broker reconciliation and deterministic restart; (b) an
  explicit activated-source continuity resolver.
- Delete or archive the unreachable lifecycle/composite/security
  machinery AFTER golden behavior tests + caller proof (code_hygiene_v1:
  certain-better-then-delete).
- Out of scope per seq666/672: signatures/HMAC/key config. Mandatory
  regardless: correctness, CAS/idempotency, bounded storage, atomicity,
  reconciliation.


## 14. Core-unblock v8 spec (seq1028 REJECT of bbf37531 — verbatim requirements)

Retained from prior freezes: stale-roster equality, observed-leaf equality,
fatal telemetry, typed evidence failure, NON_SEARCH TF skip.
1. Observed-regime authority BEFORE every candidate/probe/search path:
   `_collect_candidates` + `_collect_family_probe_candidates` take the
   allowed/observed regime set; unobserved cells must consume zero
   probes/attempts (not just lack final leaves).
2. `_validate_active_regime_provenance` preflight: accept
   fitted + terminal_non_search == resolved (FIT+NON_SEARCH sidecars valid
   before workers); any TECHNICAL_EXCLUSION keeps generation incomplete.
3. All-NON_SEARCH must flow as a TYPED RECEIPT (worker writes a data-state
   receipt with per-TF decision sha/kind/terminal/retryable; manager
   consumes the receipt object) — never exception-text parsing.
4. Support recompute compares counts + vector sha + eligible_rows +
   first_ts + last_ts; bindings source_sha256/train_window_sha256/
   detector_code_sha256 must EQUAL the current record/source/window/code
   values (not hex-shape only).
5. NON_SEARCH validator enforces kind-bound reason vocabulary (seq1012:
   INSUFFICIENT_TRAIN_HISTORY=>terminal etc.), generation_terminal/
   retryable fields, fit scope/window-contract/source/train bindings.
6. E2E mixed FIT+NON_SEARCH+sparse-observed and all-statistical-vs-
   technical regressions (Codex generation harness, lockstep).


## 13b. Builder v3 spec (seq1175 REJECT of c4d90f9f — verbatim requirements)

1. REAL trade-power provenance: builder INVOKES the released pm_trade_power
   sequence (build_trade_power_multiplicity_receipt from the frozen attempt
   ledger -> chained build_trade_power_partition_contract train/tuning/final
   -> calibrate_train_trade_power -> evaluate_trade_power_partition ->
   trade_power_requirement_sha256/trade_power_decision_sha256). R/position
   evidence and statuses DERIVED from raw position-events-v2 records
   (pnl_dollars, positive risk_amount) + authenticated technical-vs-process
   outcome receipts — never caller-asserted hex fields.
2. IMMUTABLE PRE-SEARCH TRUTH: precommitment is a separately frozen
   artifact/receipt created BEFORE search; builder consumes and verifies its
   bytes/receipt — never co-creates precommitment+attempts+evidence in one
   call (forged-pin subset counterexample must die).
3. Final byte bound over the EXACT V11 nested request shape
   (payload + payload_sha256 nesting), not flat *_payload keys.
4. Mirror released V11 bounds/order EXACTLY: daily |R| bound, balanced
   per-trade cost-stress rule, canonical trade order
   (exit_time, exit_order, entry_time, entry_order, position_id),
   timestamp parsing, counter-reset validity (open_order>close_order legal
   across different timestamps), cross-fold position domains, receipt
   domains, abs/nonfinite, ID length, event-order upper bounds.
5. Total typed error containment (no raw KeyError/TypeError) and V11
   canonicalization ensure_ascii=True (non-ASCII identity parity).
Oracle-parity hostile tests required (builder-pass must imply V11-pass and
vice versa on the covered surface). Pipeline/completion stay vetoed until
hostile re-review passes (seq1176).
