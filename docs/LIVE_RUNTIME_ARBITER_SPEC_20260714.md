# Live Runtime Arbiter — Specification (v2, JOINT)

Status: JOINT Claude+Codex position — operator-ratified hierarchy 2026-07-14
(seq744) + Codex statistical corrections adopted without dispute (seq761;
consensus recorded on the bus). Supersedes DRAFT v1 in full. Becomes
normative code only through the integration lane with its own frozen
artifact + hostile review. Companion to
`CAUSAL_SPECIALIST_PROCESS_POLICY_20260713.md`; covers the live layer that
Specialist Arbitration V4 deliberately excludes ("no live authority").

---

## 1. Scope and slots (unchanged from v1)

Per symbol: **D1 arena** (regime winners only → ≤1 D1 position) and
**lower-TF arena** (all M5..H4 regime×TF winners pooled → ≤1 lower-TF
position); both may hold concurrently under `max_combined_risk_pct` /
`secondary_trade_max_risk_pct`; envelope admission (`pm_risk_admission`)
always final. Lower-TF concurrency >1 deferred until envelope calibration
measures decorrelated-concurrency drawdown.

## 2. Selection hierarchy (operator order, calibrated quantities only)

Layer order is fixed (operator-ratified); the *quantities* inside each
layer are Codex-corrected to be calibrated and cross-TF comparable:

1. **Regime validity/survival — first eligibility layer.**
   - Identical pure regime function on **closed native bars only** (a
     partial current bar is never admissible), per TF.
   - Per-TF **out-of-sample calibrated regime probability** (empirical
     percentile where probability calibration lacks power). Raw
     classification margins are NOT comparable across TF-specific
     classifiers and are not used directly.
   - **Conditional survival**: P(regime remains valid through the
     candidate expert's measured holding horizon | current elapsed dwell),
     from the regime/TF-specific dwell distribution. This replaces raw
     bars-held ratios and correctly handles regime *exhaustion* (long
     elapsed dwell can raise exit hazard, not lower it).
   - Dwell distributions, calibration maps, and holding-horizon mappings
     are **fit inside each historical anchor** and rerun in the full
     current process — never fit on the future, never global constants.
2. **Signal presence — second layer.** Only cells with an active validated
   entry signal proceed. Evidence strength never substitutes for a signal.
3. **Arbitration among eligible signals — calibrated conditional net-R
   utility**, in one shared risk unit, after current spread/slippage,
   exposure/correlation, and order-state opportunity cost.
4. **Deterministic final tie-break — evidence strength** (effective
   trades, then DSR). Tie-break only; never a selection substitute.

**No hardcoded combination.** Any combination function, threshold,
quantile, min-bars floor, or EV weight is a **search-plan attempt**: it
must earn admission through Winner/Book selection plus whole-process
causal validation, paying multiplicity like every other searched object.
The spec pins the hierarchy and the admissible quantity types, not magic
numbers.

## 3. Parity-lock (Codex-hardened, fail-closed)

- **One shared function/import** for regime computation used by both
  training and live — a second "equivalent" live formula is prohibited by
  construction, not by review.
- Exact feature-contract, regime-parameter, source/provenance,
  timezone/session, closed-bar-cutoff, and code hashes bound into the
  deployment; **fail closed on any contract/hash/cutoff drift** (no new
  entries from the affected arena until parity restored).
- Floating-point: the shared implementation emits **one deterministic
  canonical representation**; parity compares that canonical receipt,
  never two platform-dependent raw float pipelines.
- Canonical input/output-state receipts persisted per decision for audit.

## 4. Data-modality constraint (from the joint rollup policy, seq745/762)

Rolled-up (lower→higher) bars are a **distinct provenance-bound
modality**, not a transparent substitute:

- A winner validated on native TF data may execute on rolled data only if
  a same-broker overlapping native-vs-derived parity study passes for
  OHLC, volume, spread/cost fields, bar timestamps, and signal outputs.
- Otherwise the expert must be trained, selected, process-validated, and
  run live **entirely on the derived modality**. Never splice native and
  derived segments silently; never count both representations as
  independent evidence.
- Bucket completeness judged against the broker/session calendar and the
  exact expected constituent timestamps (documented closed/no-tick
  intervals are not corruption; missing open-session bars invalidate the
  bucket).
- **D1 stays native-only** until a broker-specific daily close/session/DST
  contract and overlap parity test exist; generic intraday→D1 rollup is
  rejected. Native remains preferred whenever valid.
- Per-TF training-depth search is empirical but **precommitted** inside
  the Horizon attempt family: every candidate span pays multiplicity,
  preserves shared tuning/lockbox cutoffs, meets TF-specific
  trade/effective-sample floors, and reruns inside every historical
  anchor.

## 5. No new authority (unchanged)

Selection authority only. No sizing, no risk admission, no evidence
mutation, no resurrecting expired winners, no trading unpromoted cells.
All evidence inputs read from sealed artifacts by hash.

## 6. Verification obligations (before code drops DRAFT status)

- Parity probe fails closed under deliberately drifted classifier code,
  feature contract, and cutoff (regression each).
- Flicker regression: synthetic fast-relabeling M5 stream must not
  capture the slot from a stable H1 regime under the survival layer.
- Exhaustion regression: an old, near-exhaustion regime (high elapsed
  dwell, low conditional survival for the expert's horizon) must lose to
  a younger regime with higher survival, all else equal.
- Cost gate: positive-eligibility candidate with non-positive conditional
  net-R utility is never deployed.
- Calibration/dwell/survival policies persisted as sealed artifacts with
  anchor-scoped provenance; live loads them by hash.
- Modality receipts: every live decision records native-vs-derived
  modality identity of its inputs.
