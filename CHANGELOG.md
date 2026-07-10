# FXPM-Dev Changelog

All notable repository-level changes are documented here.

Notes:

- The application/runtime banner remains `v3.1`
- The repository changelog uses the `1.4.x` track
- The repository/folder name is now `FXPM-Dev`

---

## [1.4.10] - Unreleased

### Discovery context hygiene

- Added `docs/ACTIVE_DISCOVERY_STATE.md` as the compact current source of truth for active
  discovery runs, scorer alignment, leakage guardrails, and next actions.
- Reset `AGENT_COMMS.md` to a short live coordination channel and archived the long prior history
  under `docs/archive/AGENT_COMMS_2026-06-04.md` to reduce context drift.
- Moved resolved audit material out of `docs/audit/` after durable conclusions were captured,
  keeping active audit docs focused on unresolved findings.
- Applied clean-coding audit follow-ups to the recently changed engine/tooling surface: removed
  silent cleanup catches, added debug traces for best-effort fallbacks, tightened numeric config
  coercion catches, and made `regime_scoring_model: legacy` explicit in both active configs.

### Runtime hot-path cleanup

- Replaced `CCIReversalStrategy`'s pandas `rolling.apply` mean-absolute-deviation calculation
  with a Numba-backed helper plus pandas-compatible fallback. The helper is parity-tested against
  the legacy lambda formula and removes a BTCUSD M5 Optuna hotspot discovered during the
  `legacy` vs `specialist_spokes` like-for-like run.
- Replaced the row-wise price/indicator divergence loop used by RSI/MACD/OBV divergence
  strategies with a Numba-backed helper. The helper is parity-tested against the legacy loop and
  removes another large-candidate-surface runtime hotspot found while unblocking the like-for-like
  run.
- Added a guarded vectorized fast path for inherited default ATR stop precompute in
  `Backtester.run`. It applies only to default stop/intent behavior with `use_structural_sl=False`
  and falls back to the existing per-signal trade-intent path for structural/custom behavior.
  Focused parity tests confirm identical trades, equity curves, final equity, and scalar summaries;
  an 80k-bar timing probe showed a `1.68x` end-to-end speedup on dense-signal default-stop trials.
- Fixed backtest equity-curve bookkeeping for invalid-entry sizing paths. The Numba loop no
  longer leaves phantom zero equity points before `continue`, and the Python fallback no longer
  skips appending a bar's equity point when an entry is not sizeable. Warmup-prefixed equity is
  now trimmed for Sharpe/Sortino/std-weekly and OOS period/equity-path evidence, so risk and
  path metrics are computed on the scored window rather than the prepended warmup context.

### Discovery W4 completion coverage

- Completed the one missing configured symbol (`BTCETH`) after the W4 discovery run. The
  symbol had data and was evaluated across M5/M15/M30/H1/H4/D1, but every timeframe had zero
  candidates survive training eligibility; no winner was added to the ledger.
- Added explicit empty OOS evidence manifests for all-no-candidate symbols so completed
  symbols are not confused with missing symbols. `BTCETH_oos_manifest.json` now records
  `completion_status: NO_CANDIDATE_RECORDS`, `record_count: 0`, and a verified `0x0` sidecar
  matrix.
- Updated discovery status/reporting to reflect `62/62` OOS manifests and to prefer the main
  active-run log over a newer targeted single-symbol add-on log.
- Removed the disposable feature disk cache after completion, restoring local disk headroom
  from ~4.1 GB to ~17.9 GB while preserving ledger, OOS evidence, reports, logs, and benchmark
  artifacts.

### Stress-realism replay and safety gate cleanup

- Added opt-in `stress_p90` and `stress_p95` backtest spread modes. These charge per-bar
  `SpreadP90`/`SpreadP95` at entry while keeping `SpreadMax` for intrabar/exit protection,
  leaving `stress_max` as the existing max-spread torture diagnostic.
- Hardened percentile-stress fallback so a `stress_p90`/`stress_p95` frame missing its requested
  percentile column falls forward to `SpreadMax` instead of silently de-escalating to
  `SpreadOpen`.
- Added `SpreadP95` to resampled data, feature-cache fingerprints, and resample-cache schema
  invalidation so old cached frames without the new spread statistic are not reused silently.
- Extended `tools/promotion_review.py` with `--stress-mode` so completed discovery ledgers can
  be replayed under `stress_p90`, `stress_p95`, or `stress_max` without changing promotion
  defaults.
- Replayed W4 under current `stress_max`, `stress_p90`, and `stress_p95`. Result:
  `stress_max` remains `2 PROMOTE / 75 REVIEW / 302 REJECT`; both realistic stress modes produce
  `2 PROMOTE / 77 REVIEW / 300 REJECT`. Only two rows moved from stress rejection to holdout
  review, so the evidence does not justify changing the default promotion gate yet.
- Fixed weekly realized-loss protection to use a rolling 7-day boundary instead of calendar-week
  start. This restores the broker refetch-beyond-cache safety behavior for losses that occurred
  earlier in the rolling week.
- Hardened realized-loss timestamp handling for timezone-aware broker deal times by converting
  them to local time before the existing local-naive comparison.

### Specialist spoke tooling audit

- Completed the matched 11-symbol `legacy` vs `specialist_spokes` like-for-like scorer run
  excluding BTCUSD after the user-directed BTC cap. The run resolved eight config-producing
  symbols (`EURUSD`, `GBPUSD`, `USDJPY`, `EURAUD`, `AUDJPY`, `USDCAD`, `XAUUSD`, `XAGUSD`)
  plus three explicit no-config symbols (`US30`, `US500`, `DOGUSD`) in both arms. Benchmark
  artifacts live under
  `pm_outputs/experiments/scoring_like_for_like_20260602/benchmark_w4_completion/`.
- The pre-registered Tenet-5 comparison rejected the current `specialist_spokes` scorer as a
  default candidate: verdict `REGRESSION`, mean return `6.21% -> -0.76%`, PF `1.194 -> 1.294`,
  ulcer `7.098 -> 7.944`, trades `1513 -> 1322`. The spoke arm improved median return, PF, win
  rate, and metals, but introduced unacceptable per-symbol FX blowups in `AUDJPY`, `EURUSD`,
  and `USDJPY`.
- Interpreted the result narrowly: this rejects the current spoke scorer calibration/integration
  as unsafe to promote, not the broader path-dependent spoke/evidence-geometry concept.
- Audited `tools/specialist_spokes.py` and changed its OOS evidence loading to use the
  digest-verified `load_oos_manifest_matrix` manifest path instead of globbing the first matching
  `.npz` sidecar. This removes a stale-sidecar risk while preserving the same forward-validation
  result (`15,008` eligible candidates).
- Added opt-in v3 OOS candidate evidence for the specialist 8-spoke discovery lane without changing
  optimizer, scorer, or promotion defaults. Candidate records now preserve compact
  `trade_path_evidence`, `equity_path_evidence` with higher-is-better
  `capital_preservation_blocks`, explicit `execution_cost_survival` status/evidence, and
  `parameter_stability_evidence` when available.
- Added discovery-only config controls for the new evidence export:
  `oos_evidence_trade_path_max_points`,
  `oos_evidence_include_execution_cost_survival`,
  `oos_evidence_execution_stress_mode`, and
  `oos_evidence_execution_cost_max_candidates_per_tf`. Stress-cost replay remains disabled by
  default and records explicit `not_requested`, `capped`, or `error` statuses instead of filling
  missing evidence as neutral.
- Attached compact Optuna parameter-neighborhood evidence to tuned candidates using only completed
  same-regime trial scores. The evidence reports `available` only when enough true local
  non-exact neighbors exist; sparse neighborhoods remain `insufficient_neighbors` so the 8th spoke
  cannot pretend a one-point needle is a plateau.
- Extended `tools/spoke_selector_validation.py` to represent all 8 specialist spokes while
  remaining backward-compatible with old v2/W3 manifests. Missing engine-gated spokes are dropped
  from ranking and reported dynamically via `spoke_availability` instead of being treated as
  passed. A read-only check on EURAUD/USDJPY old evidence confirmed the new cost/stability spokes
  remain `0%` available until a v3 run emits real evidence.
- Added an opt-in in-framework optimizer scorer via `regime_scoring_model: specialist_spokes`.
  The scorer uses exactly the established 8 specialist spokes, exposes the spoke evidence object
  and derived shape features for audit, keeps trade count as saturating evidence confidence rather
  than a raw reward, ignores holdout/promotion/future fields, and lets candidate-level
  `parameter_stability_evidence` affect selection when available. Default remains `legacy` until a
  like-for-like discovery run proves the new scorer improves out-of-sample results.
- Added leakage/orientation and selection-pass-through tests for the opt-in spoke scorer, including
  a final regime-selection case where two otherwise identical candidates are separated only by
  parameter-stability evidence.
- Verification for the evidence-export tranche: focused config/artifact/OOS/validator tests
  `84 passed`; latest full suite `1066 passed, 5 skipped, 571 subtests passed`.

### Specialist spokes asym like-for-like rejection

- Added `regime_scoring_model: specialist_spokes_asym` as an opt-in scorer variant for discovery.
  The variant replaced the old geomean veto with a vote-like asymmetric aggregate, kept the
  validated specialist-spoke evidence surfaces behind a flag, and rebuilt the tail axis around
  R-normalized downside/block-CVaR fields (`r_downside_deviation`, `r_block_downside_deviation`,
  `r_block_cvar`) rather than the previously dead scalar tail proxy.
- Completed the 11-symbol ex-BTC `specialist_spokes_asym` like-for-like run with 8 symbol-level
  winners and explicit no-winner outcomes for `US30`, `US500`, and `DOGUSD`. The run completed
  without the earlier 4-worker OOM failure and emitted 11 OOS manifests/matrices.
- Reran legacy, old-spoke, and asym benchmarks under the same current engine (`c257c7a`) and
  conservative honest-cost mode. The asym scorer was decisively rejected as a default candidate:
  legacy mean/median return `+6.29% / +5.03%`, old-spoke `-0.69% / +8.74%`, asym
  `-35.24% / -35.99%`; asym median PF `0.835`, mean drawdown `44.02%`, mean ulcer `29.853`,
  time-underwater `98.39%`, and all 8 configured symbol-level winners negative.
- Recorded the diagnosis as a scorer-objective failure rather than a run-health failure:
  `specialist_spokes_asym` selected high-churn H1/M30 strategies that looked strong on short
  train/validation surfaces but bled under full honest-cost benchmarking. The two promoted
  regime cells in the promotion review do not make the asym symbol-level default ledger
  deployable.
- Default remains `legacy`. Do not rerun or promote `specialist_spokes_asym` v1 unchanged; any
  asym v2 must first address cost/frequency-aware objective behavior and then beat legacy,
  old-spoke, and a simple high-signal baseline under the robust like-for-like gate.

### Holdout sufficiency diagnostics and status alignment

- Added `tools/holdout_diagnostics.py`, a read-only W4 holdout diagnostic that reports holdout
  trade-count histograms by timeframe/regime, min-trade threshold sensitivity, validation-vs-
  holdout trade rates, and ledger/final-review status mismatches.
- W4 diagnostic result: at the current 5-trade clean-holdout floor, the promotion baseline remains
  `2 PROMOTE / 75 REVIEW / 302 REJECT`; holdout statuses are `60 PASS`, `73 FAIL`, and
  `246 INSUFFICIENT`. The artifact also surfaces `61` rows where the ledger said `PROMOTED` even
  though final review correctly held them at `REVIEW` for insufficient holdout evidence.
- Aligned `classify_promotion_status()` with the final promotion gate: when false-discovery
  control is enabled and `false_discovery_holdout_policy` is `block`, winners with fewer than
  `false_discovery_holdout_min_trades` now become `BLOCKED` instead of being labelled promoted.
  Under `promote_small`, insufficient holdout remains derated rather than clean-promoted.
- Extended promotion/regime review artifacts to preserve all simultaneous gate failures per row
  (`gate_failures`, `gate_failure_codes`, and non-exclusive gate counts), so ordered final reasons
  no longer hide secondary blockers such as holdout/PBO behind an earlier stress rejection.
- Parameterized the holdout min-trade threshold in the review tools and persisted it in review
  output, reducing future config/tool drift during cross-run comparisons.
- Extended the holdout diagnostic with construction projections before any default change:
  trade-count anchored holdout and purged/embargoed k-fold OOS. W4 projection shows the clean
  holdout tail has near-identical trade rate to validation (`0.985499` median ratio), so thinness
  is bar-count/frequency, not a quiet tail. Both projections recover the same `10` currently
  positive-but-thin candidates (`3 -> 13` quality-cautious ceiling), but flat tail enlargement
  would imply a median `26.4712%` extra training-data loss for clean-feasible rows; purged-k-fold
  gets the same sufficiency projection with `0%` permanent training-data loss at about `5x`
  discovery runtime. Defaults remain unchanged pending an opt-in implementation/replay.
- Added `docs/discovery_execution_roadmap.md` as the active PM/data-science roadmap for the next
  discovery phase. It consolidates W4 evidence, the holdout/PBO sequence, the specialist-first
  8-spoke evidence-geometry target, resource/resilience workstreams, and the rule that the roadmap
  is disposable once durable conclusions are moved into code/tests/config or this changelog.
- Added opt-in `oos_validation_mode: purged_kfold` support for selected specialist validation.
  The default remains `fixed_holdout`, and `config.discovery_honest.json` now explicitly locks that
  current baseline. The new prototype emits purged/embargoed fold boundaries, train-exclusion
  metadata, fold trades/PF/return/drawdown/capital-preservation diagnostics, aggregate OOS metrics,
  worst-fold details, fold pass rates, and conservative aggregate-policy status without changing
  optimizer selection defaults.
- In `purged_kfold` mode, the splitter now suppresses the fixed clean-holdout tail
  (`effective_clean_holdout_pct: 0.0`) so the prototype actually avoids permanent training-data
  loss. Fixed-holdout mode keeps the existing clean-holdout slicing.
- Added artifact-fingerprint fields and tests for the new OOS validation contract so fixed-holdout
  and purged-kfold ledgers cannot be silently reused across modes. Verification after the prototype:
  focused OOS/config/artifact tests `79 passed`; full suite `1052 passed, 5 skipped, 571 subtests
  passed`.
- Added `tools/purged_kfold_replay.py` for bounded post-run replay of completed specialist rows.
  Initial W4 representative replay artifact:
  `pm_outputs/experiments/discovery_honest_w4_purged_kfold_bounded_replay.json`. On 8 sampled rows,
  the prototype evaluated 8/8 without errors, recovered the 5-trade evidence floor on all 5
  fixed-holdout-insufficient rows, produced 3 purged-kfold policy passes, and surfaced one manual
  candidate after existing non-OOS gates (`AUDJPY H1 RANGE / EMAPullbackContinuationStrategy`).

### Independent audit #001 — resolved (working doc retired)

- C1: honest discovery cost contract realized via dedicated `config.discovery_honest.json`
  (conservative_max spread + 15% clean holdout); main `config.json` left untouched.
- C2: Tenet-5 benchmark gate now treats path-dependent drawdown (time-underwater) as
  first-class risk, pooled and per-symbol (`tools/benchmark.py`).
- C3: global/effective-trial deflated-Sharpe wired into regime winner selection, plus the
  PBO (CSCV) measurement side (`tools/pbo.py`) consuming engine-emitted
  `oos_candidate_matrix_v1` manifests; end-to-end proven (EURUSD H1 PBO wiring run).
- H1: strategy prospect screen replaced its lax best-of-N bar with a robustness bar
  (fraction-profitable + median PF).
- H4: added `stress_max` entry-spread stress mode alongside `conservative_max`.
- H5: benchmarks now surface `asset_class` + `spread_provenance_status` + frozen/broker
  spread ratio; non-FX outliers (e.g. DOGUSD ~15x) flagged for review rather than trusted.

### Numba JIT restoration + trial-budget calibration (efficiency, accuracy-neutral)

- Root-caused the discovery slowdown: the installed `numpy 2.4.4` exceeded Numba's `<=2.3`
  ceiling, so `NUMBA_AVAILABLE` was `False` and every backtest silently fell back to the pure
  Python loop (`_run_python_loop`) instead of the JIT kernel `_backtest_loop_numba` — a 3–10x
  penalty on the hottest path. Pinned `numpy>=2.2,<2.4` in `requirements.txt` and downgraded the
  environment to `numpy 2.3.5`; `NUMBA_AVAILABLE` is `True` again.
- Proved the JIT path is accuracy-neutral rather than assuming it: new
  `tools/parity_numba_python.py` runs identical features/signals through both loops
  (governance=None dispatch) and asserts discrete trade decisions match exactly and continuous
  values within 1e-6. Result: 128 strategy×symbol cells across EURUSD/GBPUSD (H1), USDJPY
  (M5/M15/H4), and XAUUSD/BTCUSD (H4) — worst absolute difference `0.000e+00`. Exposed as a
  pytest (`test_numba_python_parity`) so the invariant is CI-guarded.
- Calibrated the Optuna trial budget with evidence instead of a guess: new
  `tools/optuna_convergence.py` captures the genuine Optuna study (no engine edit; monkeypatches
  `_compile_stats` only) and reports best-score-so-far vs trial count for the largest search
  spaces. Both giants converge by ~300 trials; `best@600` is 95.5–100% of `best@1500`, with the
  residual arriving only at trial ~1345. Because the live failure mode is overfitting (holdout
  collapse), in-sample score is not trusted on its own — the holdout/PBO gates are the overfit
  backstop. Budget calibrated to span the measured convergence (the residual edge lands at
  ~trial 1345) rather than the old 5000: `optuna_min_trials_per_strategy` 80→120,
  `optuna_target_coverage_pct` 0.05→0.10, `optuna_max_trials_per_strategy` set to 1500.

### Path-dependent selection geometry (post-run evidence, experimental)

- added shape-aware post-run selection validation for Bongo's path-dependent quality geometry:
  `tools/pareto_select.py` now exposes volume-balance/geomean ranking plus dent-sensitive path
  topology features, while `tools/selection_validation.py` compares selectors by forward OOS
  profitability, forward drawdown, and selector runtime
- added `shape_gate_pf` and `shape_gate_geomean` validation rules: path topology acts as a
  threshold-free Pareto survivor gate, then PF or volume-balance chooses among shape-healthy
  candidates
- added `trace_gate_pf` and `trace_gate_geomean` as the closer axis-by-time version of the
  topology idea: each candidate is represented as a stage-by-quality-axis trace, but the trace is
  normalized across the competing candidate pool at each stage/axis so a small single-axis wobble
  is not amplified by self-scaling
- added `tests/test_selection_geometry.py` to lock the intended behavior: one large path dent is
  penalized, a path-dominated high-PF candidate is rejected by the shape gate, and a one-axis trace
  dent is penalized without collapsing unrelated flat dimensions
- fixed `tools/discovery_status.py` to read UTF-8 BOM active-run markers and auto-detect the
  matching optimizer PID, so active W3 monitoring reports liveness instead of `run pid: n/a`
- current status is deliberately experimental: on the 3-symbol partial evidence, max-PF still has
  the strongest forward profitability/reliability, while path/geometry rules reduce forward
  drawdown; defaults should only change after the full honest OOS evidence proves the rule wins on
  profitability, drawdown/shape, and runtime/complexity
- added `tools/spoke_selector_validation.py`, a digest-verified post-run validator for the locked
  specialist-first spoke model. It tests selectors per `symbol/timeframe/regime` cell using only
  selection-half OOS evidence to choose and forward-half OOS evidence to score, with training
  metadata used for eligibility so full-OOS metrics cannot leak into the selection step.
- added `tests/test_spoke_selector_validation.py` to lock no-forward-leakage behavior, digest-
  verified synthetic manifest loading, specialist-cell grouping, and explicit reporting of missing
  engine-gated spokes.
- W4 available-spoke result across `62` manifests and `553` evaluated cells:
  `spoke_face_geomean` achieved median forward return `0.830%`, `58.8%` profitable picks,
  mean forward drawdown `4.509%`, reliability `0.488`, and drawdown-aware reliability `0.089`;
  the `baseline_max_pf` control achieved median forward return `0.413%`, `54.1%` profitable picks,
  mean forward drawdown `5.328%`, reliability `0.224`, and drawdown-aware reliability `0.035`.
  Claude's paired audit reframed this correctly: `350/553` cells had identical picks, and among
  the `203` differing picks `spoke_face_geomean` was `104` better / `99` worse with sign-test
  `p=0.778988`. The advantage is therefore distributional and capital-preservation-oriented,
  not systematic per-cell dominance, and it is not yet a default scorer change.
- Added experimental ablation checks. Removing the tail-risk spoke slightly increased reliability
  (`0.491` vs `0.488`) but worsened forward drawdown (`4.911%` vs `4.509%`), and tail-risk control
  strongly predicted forward capital preservation (`+0.674` correlation), so the evidence supports
  keeping tail risk as a risk spoke rather than dropping it for marginal return reliability.
- Current spoke caveat: only six of eight locked spokes are available in the completed OOS manifests.
  `execution_cost_survival` still needs per-candidate stress/cost replay evidence, and
  `parameter_stability` still needs Optuna neighborhood/plateau evidence before the full 8-spoke
  scorer can be honestly integrated into optimization defaults.

### Package K strategy audit fixes

- corrected the new carry-aware strategies so they only trade when the broker swap snapshot shows
  a truly positive carry side; both-negative symbols now stay flat instead of choosing the less
  negative bleed side
- fixed `IntradayRangeFadeStrategy` to shift developing range highs/lows within each session,
  preventing previous-session range leakage into the first bar of the next session
- limited `OvernightDriftStrategy` to one entry decision per configured entry hour on sub-hour
  timeframes and measured the drift from the session's first `Open` when available
- hardened Package K time/calendar helpers so non-datetime input fails flat instead of treating
  every bar as hour/day zero
- removed structural-stop opt-in from the two Package K mean-reversion strategies to preserve the
  existing ATR-only mean-reversion stop contract
- updated the honest discovery metadata and strategy exports to the 84-strategy roster

### Package L strategy expansion

- added Package L, expanding the deterministic strategy roster from 84 to 92 with:
  - confirmed-fractal channel breakout
  - swing-failure reversal
  - session VWAP reclaim and pullback trend variants
  - volume-climax reversal
  - efficiency-ratio trend and mean-reversion regimes
  - opening-range retest continuation
- reconciled independent Package L audit findings by guarding session-shift comparisons, making
  retest/efficiency/VWAP entries event-like rather than every-bar state signals, and adding direct
  behavioral regression tests for the new strategy semantics
- updated discovery metadata and registry tests to the 92-strategy roster

### Package M strategy expansion

- added Package M, expanding the deterministic strategy roster from 92 to 100 with:
  - Darvas-box breakout
  - ADR expansion breakout
  - prior-day range reclaim
  - weekly/monthly level rejection
  - previous-week range breakout
  - head-and-shoulders neckline break
  - variance-ratio trend
  - autocorrelation reversal
- reconciled independent Package M audit findings by using broker-aligned week periods, debouncing
  same-level rejection signals per day/period, removing the dead `adr_period` axis from prior-day
  reclaim, optimizing the head-and-shoulders detector to scan confirmed pivot events instead of
  every bar, and adding timezone/week-boundary regression tests
- updated discovery metadata and registry tests to the 100-strategy roster
- removed the stale strategy-surface audit document now that Packages J-M have superseded it; the
  durable record lives in this changelog and the implemented tests/code

### Global false-discovery gate

- added opt-in global false-discovery controls to `PipelineConfig` so broad roster re-optimization
  can account for strategy x parameter x symbol x timeframe search breadth instead of only local
  candidate counts
- wired global DSR evidence into regime winner selection, including score confidence penalties,
  optional minimum-DSR rejection, persisted validation evidence, and explicit `pbo_status:
  not_available` until a real candidate-by-fold PBO implementation exists
- enabled the discovery-honest contract to use global false-discovery scoring and to block winners
  that fail the freshest holdout once the holdout has enough trades
- added tests for artifact invalidation, config normalization, evidence persistence, promotion
  blocking, and descent behavior that skips false-discovery-failed candidates before validation

### OOS candidate evidence for PBO

- added opt-in OOS candidate evidence export for discovery runs so every final candidate can emit
  compact validation-period returns for CSCV/PBO measurement without bloating the winner ledger
- discovery-honest config now writes a sidecar `oos_candidate_matrix_v1` manifest plus compressed
  NPZ return matrix under the output directory; default production behavior remains off
- kept no-trade validation candidates in the evidence matrix as zero-return columns so PBO does not
  overstate robustness by dropping weak candidates
- added cache invalidation for missing, corrupt, or artifact-mismatched OOS sidecars when evidence
  export is enabled, plus content-addressed matrix files to avoid stale manifest/matrix pairings
- added `tools/pbo.py --manifest` support to load and verify engine-emitted sidecars directly
  before running CSCV/PBO
- extended `tools/pbo.py` with candidate-level CSCV evidence, including how often each candidate is
  selected in-sample and how often that selected candidate lands below the OOS median
- documented the current PBO scope honestly as all-regimes validation equity, not regime-filtered
  folds; this is the bridge artifact for Claude's `tools/pbo.py` measurement layer

### Spread stress validation

- added opt-in `backtest_spread_mode="stress_max"` for stricter spread validation; unlike the
  discovery baseline `conservative_max`, it charges `SpreadMax` at entry as well as intrabar/exit
  checks, while still floor-protecting against the broker fixed spread
- documented the existing `conservative_max` behavior in metadata as bar-open/mean entry plus
  max-spread intrabar, so the stricter stress surface is explicit rather than implied
- added spread provenance flags to backtest metadata and benchmark output so non-FX frozen-feed
  spread outliers are marked for review instead of being treated as clean promotion evidence

### Benchmark gate hardening

- hardened the Tenet-5 benchmark gate so profit-factor deltas near zero use a stable denominator and
  absolute PF movement, preventing tiny 0.00-to-0.01 changes from being treated as infinite evidence
- benchmark records now include per-asset-class summaries so mixed FX/non-FX runs expose class-level
  collapses instead of relying only on one pooled mean

### Discovery run resource hardening

- added a configurable `feature_cache_max_entries` setting to disable in-memory feature-frame
  caching during large discovery re-optimizations; the cache had no useful hit path in the full
  per-timeframe discovery flow and doubled memory pressure by copying full feature frames
- added a bounded disk-backed full-feature cache for discovery workloads; cache keys include the
  feature-code version, regime-parameter fingerprint, full source-data fingerprint, symbol,
  timeframe, and request tag so retrieval cannot silently reuse stale or mismatched features
- changed `config.discovery_honest.json` to run the gated 100-strategy discovery contract with
  two workers, no in-memory feature-frame cache, a bounded disk feature-cache budget, and a
  precompute/retrieval phase before strategy optimization; this uses storage for repeat feature
  frames while keeping RAM pressure low on the 8 GB machine
- added a hard disk-cache free-space reserve and pre-write pruning/skip logic so feature-cache
  writes cannot fill the drive or silently rely on a cache cap larger than current storage headroom
- added `tools/precompute_feature_cache.py` so discovery feature frames can be warmed through the
  same file-backed multiprocessing path used by Windows production launches
- hardened the disk feature cache with content-hashed regime params, feature-code fingerprints,
  parquet+manifest atomic writes, manifest shape validation, grouped LRU pruning, temp-file
  protection, and worker-level precompute telemetry for hits/writes/skips/failures
- added `tools/discovery_status.py`, a read-only status helper for long discovery runs that reports
  process liveness, ledger progress, OOS sidecar counts, disk headroom, and recent log lines

### ATR exit-surface discovery anchors

- added an opt-in initial screening sweep for paired `1x/2x/3xATR` SL/TP anchors so base
  strategies are not discarded before tuning solely because their default `2xATR` exit range was
  the wrong first-pass assumption
- enabled that sweep in `config.discovery_honest.json` for the gated discovery run and fingerprinted
  the anchor contract in pipeline artifact metadata
- expanded the global ATR stop-loss grid to include the missing `1.0xATR` anchor while retaining
  the existing `1.5/2.0/2.5/3.0xATR` coverage; the take-profit grid already included
  `1.0/2.0/3.0xATR`
- added regression coverage that the first-pass screening layer evaluates the configured ATR
  anchors and that duplicate anchor variants cannot crowd out strategy diversity in the tuning
  shortlist

### Audit artifact lifecycle

- clarified that audit outputs, agent communication files, experiment notes, and review documents
  are working artifacts rather than evergreen documentation
- durable conclusions should move into code, tests, config, or this changelog; stale audit/meta
  files should be deleted once their findings are fixed, waived, or superseded
- clarified that no repository file is inherently read-only; role-based read-only auditing is only
  a coordination boundary, and any file may be changed when evidence supports it

### Profitability-readiness integration hardening

- added regression coverage for the latest audit fixes:
  - `swing_trail` now normalizes to `structure_trail`
  - `promote_small_risk_multiplier` is a clamped `PipelineConfig` field instead of a hardcoded live multiplier
  - funded-live readiness now refuses startup while a prior institutional-supervisor hard block remains latched
  - funded-live readiness now mirrors `manual_or_next_cycle` reopen semantics so previous-cycle latches warn instead of creating a permanent startup dead end
  - `CycleDriftMonitor.record_trade()` can persist execution-slippage and spread-degradation observations when the live caller supplies them
- refreshed operator documentation for the current 50-strategy registry, 11-policy governance pool, live readiness gate, promotion-status behavior, supervisor/cycle-drift state files, swap modeling, regime audit fields, and event-calendar settings
- removed the implemented v2 profitability-readiness blueprint from the active root documentation surface after signoff; historical audit/planning material remains in `documentation_archive/`
- added `best_config_reccomendations_gpt.html` as a fresh settings review document for the current `config.json` profile
- aligned active documentation with the `FXPM-Dev` repository/folder rename while preserving internal `v3.1` runtime versioning and the `1.4.x` changelog track
- verification: `python -m pytest -q`: `593 passed, 1 skipped, 356 subtests passed`

---

## [1.4.9] - 2026-04-29 (Current)

### Configuration cleanup — single workflow, single scoring path

The pipeline already used the active-recent M5 workflow and the FX backtester scoring path in production. The legacy alternates were dormant code and dormant config keys. This release deletes them so the codebase has one well-defined path for each axis.

Removed config keys (no behavior change for the active path):

- `data_workflow_mode` and the entire `legacy_percentage` workflow path (legacy splitter, `train_pct`, `val_pct`, `holdout_pct`).
- `scoring_mode` and the entire `pm_weighted` scoring path (`min_robustness`, `min_win_rate`, `min_profit_factor`, `min_sharpe`, `max_drawdown`, `score_weights`).
- `regime_freshness_decay`, `regime_chop_no_trade` — declared on `PipelineConfig` but never read anywhere in production code.
- `position_size_pct` — was a `__post_init__`-synced alias for `risk_per_trade_pct`; the dataclass now exposes only `risk_per_trade_pct`.

Removed code:

- `DataSplitter.get_split_indices`, `DataSplitter.split`, `DataSplitter._holdout_bars`, `DataSplitter.workflow_enabled` (all served the legacy splitter).
- `StrategyScorer.calculate_composite_score`, `StrategyScorer.passes_minimum_criteria` (pm_weighted-only).
- The `if scoring_mode == "fx_backtester"` and `if active_mode` branches in `pm_pipeline.py` collapse to the single FX/active path.
- `Validator.validate` keeps only the FX-aligned validation rules (early-return branch is now the only branch).
- `RegimeOptimizer.use_fx_scoring` and the legacy fallback math in `_compute_regime_score` / `_validate_regime_winner`.

Repo hygiene:

- `__pycache__/` and `data/.live/` are no longer tracked in git. Working files remain on disk; `.gitignore` already excluded `__pycache__/`, and now also excludes `data/.live/`.
- Saved artifact metadata no longer carries `data_workflow_mode`, `train_pct`, `val_pct`, `holdout_pct`, or `scoring_mode`. The artifact contract view ignores keys absent from the current contract, so existing winner ledgers do not invalidate.

Tests: `tests/test_data_splitter.py` was rewritten to cover only the active workflow (the legacy splitter cases were removed). `568 → 566` total tests, 1 new empty-window test. All pass.

---

## [1.4.8] - 2026-04-25

### Neutral discovery exit surface

- reset Stage 1 strategy defaults to a neutral `2.0` ATR stop / `2.0` ATR target across the full registered strategy roster, while leaving the Stage 2 SL/TP search grid broad (`tp_atr_mult` still searches above and below `2.0`)
- changed the fallback stop/target floor to a symmetric `5.0` pips so tiny-ATR Stage 1 candidates do not inherit a hidden TP-side advantage
- removed live-only regime TP multipliers from backtest/discovery scoring by having `Backtester.run()` call `build_trade_intent(..., apply_regime_tp_multiplier=False)`
- changed the default `regime_tp_multipliers` table to neutral `1.0` values; non-neutral values remain available as an explicit live-only operator overlay, not part of winner discovery
- updated artifact metadata to `2026-04-29-neutral-discovery-exit-surface` / `neutral_discovery_sl_tp_v1` so optimized ledgers can distinguish this fair-tournament exit contract from the previous regime-multiplied surface

### Live eligibility and optimize-readiness hardening

- added a strict live eligibility surface separate from raw validated-ledger reads:
  - live trading now blocks expired configs, no-expiry configs, no-winner configs, and artifact-drifted configs when `live_artifact_drift_policy = "block"`
  - `python pm_main.py --status` now reports `Live eligible` and marks live-blocked configs with `BL`
  - `live_artifact_drift_policy` supports `block`, `warn`, and `ignore`; the active profile uses `block`
  - `live_config_expiry_grace_minutes` defaults to `0` for strict expiry
- fixed optimize-linked artifact invalidation so volatile saved metadata such as `ledger_status` no longer makes a freshly optimized config appear due on the next status/retrain check
- hardened live margin gates against non-finite broker/account values:
  - `NaN`/`inf` margin level is treated as unavailable
  - non-finite required margin or free margin blocks with `SKIPPED_MARGIN_UNAVAILABLE`
- propagated the fresh final broker-side symbol position read into the same-symbol combined-risk cap, closing the remaining stale-snapshot race for different-magic same-symbol positions
- surfaced low-balance and margin feasibility events in the dashboard defaults, including `SKIPPED_MARGIN_*` and `BLOCKED_MIN_LOT_EXCEEDS_CAP`
- added storage-state freshness labels (`state_updated_at` and `freshness`) so stale live-sweep fields are distinguishable from current housekeeping state
- changed the default live-loop trigger to MT5 bar timestamps (`live_loop_trigger_mode = "bar"`), so signal checks run from broker bar availability rather than a wall-clock bar scheduler
- kept the legacy due-time scheduler available as `live_loop_trigger_mode = "scheduled"` for intentional fallback/testing use
- kept signal discovery bar-gated while preserving runtime-only management cycles between changed signal bars, so margin protection and open-order governance are not starved while waiting for the next strategy candle
- refreshed executable quote, SL/TP geometry, actual risk, same-symbol risk, and margin immediately before order submit, while keeping strategy signal generation on the closed bar surface

### Audit implementation sweep

- aligned live winner selection with the optimized decision surface by preferring `REGIME_LIVE` / `REGIME_STRENGTH_LIVE`, with legacy fallback to `REGIME` only when shifted live-decision columns are unavailable
- implemented stateful margin reopen hysteresis:
  - stressed margin states and forced margin closes set a reopen-required latch
  - new entries resume only after `margin_reopen_level` and the configured cooldown are satisfied
  - missing or unparseable `margin_level` fails closed only when margin exposure exists
- hardened the final live duplicate-position guard so `_execute_entry()` keeps the sweep snapshot as a cheap first pass but refreshes exact/symbol broker positions immediately before order send
- improved artifact truthfulness:
  - `NO_TRADE` marker reasons survive metadata propagation
  - regime and symbol artifacts persist compact `validation_evidence`
  - top-level selected robustness is populated from validation evidence when available
  - workflow metadata carries data-window fingerprints
  - saved symbol artifacts include active-ledger completion status, missing symbols, and artifact-contract counts
- added optimizer/storage observability:
  - pre-tuning eligibility gates record compact rejection/rescue counters
  - resample-cache telemetry now tracks memory hits, disk hits, misses, invalidations, bytes, and read/write seconds
  - active cache sizing remains benefit-driven; the high-risk profile keeps its 4 GB quota rather than shrinking cache capacity for its own sake
- updated `README.md`, `SETTINGS_REFERENCE.md`, `PATCH_NOTES.md`, `audit.md`, `audit.html`, and `trading_implementation.html` to reflect the implemented behavior
- focused verification:
  - `python -m pytest tests/test_resample_cache.py tests/test_margin_protection.py tests/test_secondary_position_inference.py tests/test_live_loop_integration.py tests/test_pipeline_artifacts.py -q`: `78 passed, 11 subtests passed`
  - `python -m pytest tests/test_config_source_of_truth.py tests/test_position_sizing_edge_cases.py tests/test_portfolio_risk_cap.py tests/test_storage_manager.py tests/test_storage_live_data.py tests/test_feature_cache.py -q`: `66 passed`
  - `python -m py_compile pm_core.py pm_main.py pm_pipeline.py`
- full-suite verification after the latest implementation sweep: `python -m pytest -q`: `559 passed, 1 skipped, 350 subtests passed`

### Active recent workflow and risk-management alignment

- implemented and documented the active recent M5 workflow as the production baseline:
  - latest `300000` M5 bars per symbol
  - oldest `50000` M5 bars as `historical_stress_audit`
  - newest `250000` M5 bars as the active strategy-selection universe
  - Stage 1 baseline eligibility over the full active universe
  - newest `50%` of the active universe as the fresh Stage 2 optimization/risk-management surface with warmup context
- clarified that `historical_stress_audit` is an older-window fragility check, not a forward-looking holdout selector
- hardened the workflow splitter so raising `max_bars` cannot silently expand the configured 50k M5 historical audit window
- passed timeframe context into regime-bucket metrics so Sharpe/Sortino annualization and stability penalties use the intended timeframe instead of an H1 fallback
- aligned risk-management/governance policy selection with selected winners through `risk_management_optimization_enabled` and `risk_management_selection_stage`
- clarified Stage 1 terminology: Stage 1 is a full-active-universe eligibility gate and any "Top-K-like" wording refers to the survivor pool, not the Stage 2 Top-K optimizer
- kept the checked-in Stage 1/Stage 2 presets unchanged as the current recommended baseline:
  - `max_param_combos = 200`
  - `optimization_max_workers = 4`
  - `regime_hyperparam_top_k = 5`
  - `regime_hyperparam_max_combos = 200`
  - family-size-aware Optuna budgeting enabled
- synchronized operator documentation so `README.md`, `SETUP_AND_RUN.md`, and `SETTINGS_REFERENCE.md` reflect the active high-risk low-balance profile and current winner ledger path
- recorded the active profile's governance/risk runtime posture: high-risk live sizing under `position`, `live_risk_scalars_mode = "shadow"`, and `local_governance_live_mode = "shadow"`

### Documentation cleanup

- archived completed audit, analysis, and implementation-planning artifacts under `documentation_archive/2026-04-25-active-workflow-audit/`
- kept active operator guidance at the root while moving historical audit/planning documents out of the main doc surface
- updated the root README and `documentation_archive/README.md` so current docs and archive docs are clearly separated

### Verification

- current implementation verification: `python -m pytest -q`: `559 passed, 1 skipped, 350 subtests passed`
- documentation sync verified with targeted stale-reference checks and `git diff --check`

---

## [1.4.7] - 2026-04-22 (Previous)

Driven by the findings / technical-direction rollout now archived under `documentation_archive/2026-04-25-active-workflow-audit/`. All changes ship with paired tests and zero suite regressions.

### Dashboard hardening audit - 2026-04-24

- dashboard write APIs now allow default local loopback use but block remote writes unless `PM_DASHBOARD_WRITE_TOKEN` is configured and supplied via `X-PM-Dashboard-Token` or `Authorization: Bearer`
- dashboard strategy, trade, and analytics reads now follow `pipeline.winner_ledger_path` from root `config.json` when `pm_configs_path` is left at `auto`, so the high-risk profile reads `pm_configs_high_risk.json`
- dashboard root-data refresh now merges overlapping MT5 bars under an in-process lock and publishes `data/<SYMBOL>_M5.csv` through atomic replace
- README, setup, dashboard, implementation-tracking, and trading-implementation docs now describe the remote-bind security posture and active-ledger behavior

### Phase A — Validation gate retightening

- `regime_min_val_return_pct`: `0.0` -> `5.0`
- `exceptional_val_profit_factor`: `1.15` -> `1.50`
- `exceptional_val_return_pct`: `5.0` -> `10.0`
- `fx_min_robustness_ratio`: `0.75` -> `0.80`
- `actionable_score_margin`: `0.90` -> `0.92`
- `execution_spread_spike_mult`: `3.0` -> `2.0`
- `min_trade_risk_pct`: `0.05` -> `0.10`

Note: the later checked-in small-account high-risk profile intentionally loosens `execution_spread_spike_mult` back to `3.0` and `min_trade_risk_pct` back to `0.05`; `config.json` is the active profile source of truth.

### Phase B — Live-loop hardening

- secondary-trade risk inference now emits a reason-coded `WARN` when both broker geometry and comment decoding fail to recover risk
- spread-spike penalty rewritten with a 100-bar window, anchored to `max(rolling_median, spec.spread_avg)`, with a non-saturating floor (`max(0.25, 1 - 0.4·(ratio - start))`) so spread quality is throttled rather than nuked
- `margin_reopen_cooldown_minutes` (default `15`) added to `PipelineConfig` + `config.json`; engaged after every successful margin-cycle close; entry gate refuses with `SKIPPED_MARGIN_COOLDOWN`
- `DecisionThrottle._load()` now purges entries older than `max_age_hours` (default `24`, `0` disables); rewrites the on-disk file when anything was dropped; one INFO log per load
- `_execute_entry` now `WARN`s when post-normalization actual risk drift exceeds `position.risk_tolerance_pct` (still in-cap), keeps INFO for in-tolerance drift, and blocks unchanged for over-cap

### Phase C — Selection quality

- DD penalty slope tightened from `exp(-0.03·dd)` to `exp(-0.05·dd)` in both `calculate_fx_selection_score` and `calculate_fx_opt_score` so the TPE sampler matches the live selector
- weak-train rescue now also requires `val_sharpe >= 0.5`, `val_win_rate > 50%`, and `val_trades >= max(2·min_val_trades, 50)`; rescue reason names which gate(s) failed
- `fx_val_max_drawdown` tightened `15.0` -> `12.0` (config + dataclass default)
- `fx_stability_penalty_k` (default `2.0`) added to `PipelineConfig` + `config.json`; backtester now emits `std_weekly_return`; `StrategyScorer.fx_generalization_score` applies multiplicative `exp(-k·std_weekly_return)` (disabled when `k=0`)
- `load_regime_params()` records each fallback once per `(symbol, timeframe)` and emits an INFO log; `LiveTrader.run_trading` startup banner surfaces the snapshot
- `OptimizationPipeline._log_regime_coverage` emits a one-line regime distribution per `(symbol, tf)` after the per-TF winner summary (silent when `REGIME` column absent)
- `_process_symbol` refuses to open a trade when `best_overall_score <= 0`, emitting `NO_ACTIONABLE_BEST_SCORE_NONPOSITIVE` through the throttle so the actionable-margin threshold cannot collapse/invert with non-positive leader scores
- removed dead Optuna objective-blend knobs from `PipelineConfig`, `OptunaConfig`, the trial manifest, the startup banner, and `config.json` / `Normal config (Full Equity).json`:
  - `optuna_use_val_in_objective`
  - `optuna_objective_blend_enabled`
  - `optuna_objective_train_weight`
  - `optuna_objective_val_weight`
  These fields populated dataclasses but were never read anywhere (no `.use_validation_in_objective` / `.objective_blend_enabled` / `.objective_train_weight` / `.objective_val_weight` consumer survived in code). The actual train/val lever is `fx_gap_penalty_lambda`. Guard tests in `test_scoring_audit.py` prevent silent reintroduction.
- helper short-circuits in `pm_strategies.py` (`_get_keltner`, `_get_bb`, `_get_adx_di`, `_get_stochastic`, `_get_macd`) now gate on a per-DataFrame precomputed-params tag (`features.attrs['_fxpm_precomputed_params']`) in addition to the exact-default-params check, not on column presence alone. `FeatureComputer` stamps the tag at both precompute paths after writing default-params indicators

### Phase E — Optional uplift (in progress)

- **E4**: `DEFAULT_PARAMS_BY_TIMEFRAME` rows now override `bb_squeeze_lookback` per timeframe — M5=`50`, M15=`80`, H1=`50`, H4=`50`, D1=`60` — so squeeze detection is no longer dense on intraday and stale on daily. M30 intentionally keeps the dataclass default (`200`) because the archived findings did not specify a value for it. The dataclass default stays at `200` so any unknown TF still gets the conservative baseline. Locked by `BBSqueezeLookbackTimeframeTests` in `test_regime_fallback_log.py`.
- **E5**: `DEFAULT_PARAMS_BY_TIMEFRAME['D1'].k_hold` lifted `2` → `3` (`pm_regime.py:332`) so an overnight-holding D1 winner cannot flip its held regime on a single-bar spike before the next evaluation. All other timeframes and the D1 `k_confirm` / `gap_min` rows are untouched. Locked by `DefaultParamsByTimeframeTests` in `test_regime_fallback_log.py`.
- **E1**: tri-state `live_risk_scalars_mode: "off" | "shadow" | "on"` added to `PipelineConfig` + `config.json`. `"shadow"` installs the full overlay stack but `RiskScalarStack.apply()` returns the input `risk_pct` unchanged; the live entry path then calls `RiskScalarStack.compute()` and emits an `INFO` line `Risk scalar SHADOW: would size X% -> Y% (delta ±Δ%); live target unchanged` so operators can measure the would-be sizing delta on real trades before flipping to authoritative `"on"`. The legacy `live_risk_scalars_enabled` boolean still works (`true` falls back to `"on"` when `live_risk_scalars_mode` is left at default `"off"`); invalid mode strings WARN once and degrade to `"off"`. Startup banner now surfaces the active mode (`risk_scalars=off|shadow|on`) instead of a bare boolean.
- **E2**: `BaseStrategy.build_trade_intent()` keeps an optional live-only regime TP multiplier hook, but the default table is neutral (`1.0` for all regimes) and `Backtester.run()` disables the hook during discovery scoring. Operators can still opt into non-neutral live multipliers through `PositionConfig.regime_tp_multipliers`; Stage 1 and Stage 2 now trade the same discovered SL/TP surface. The minimum exit floor is symmetric at `5.0` pips. Locked by `tests/test_regime_tp_multipliers.py`.
- **E3**: `RegimeParams.adx_trend_threshold` (new field, default `25.0`) + `DEFAULT_ADX_TREND_THRESHOLD_BY_ASSET_CLASS` re-anchor the regime-detector's ADX normalization to the instrument's natural trend floor. `fx`/`index` keep the classic Wilder `25`; metals use `22` (slower buildup); crypto uses `30` (higher natural floor). `_infer_asset_class()` classifies by symbol prefix (XAU/XAG/XPT/XPD + GOLD/SILVER → metal; BTC/ETH/LTC/XRP/SOL/ADA/DOT/DOGE/BCH → crypto; SP500/NAS100/US30/DAX/FTSE/... → index; all else → fx). `load_regime_params()` applies the asset-class default during fallback **and** to JSON-cached entries unless the JSON explicitly pinned `adx_trend_threshold`. `_normalize_adx(_vectorized)` and `_normalize_adx_mid(_vectorized)` now take a `threshold` parameter; knees are expressed as multiples of the threshold (`0.8·T` / `1.6·T` / `3.2·T` for TREND, `0.6·T` / `0.9·T` / `1.2·T` / `1.6·T` for CHOP) so the curves collapse to byte-identical legacy behavior at `T=25`. `compute_regime_scores` forwards `p.adx_trend_threshold` into both curves and uses it as the NaN seed when the ADX column has gaps. Locked by `tests/test_adx_trend_threshold.py` (22 tests).

### Section 13 - Final implementation sweep

- added `pm_order_governance.py` as the shared post-selection order-governance layer with canonical policy normalization and causal policy evaluation for `control_fixed`, `breakeven_1r`, `atr_trail_capped`, and `pure_atr`
- extended `Backtester.run()` and the Python execution loop so a winner can be replayed under a selected governance policy without changing entry logic, costs, or the fixed-SL/TP control path
- added `TradeIntent` to `pm_strategies.py` and wired the live executor to consume the typed strategy-to-execution contract while preserving backward compatibility for legacy callers
- added the Section 13 weekly-review surfaces:
  - `live_eligibility_report(symbol, timeframe, regime)` for exact-context artifact age / cycle reporting
  - per-winner `live_observability` persistence and live-loop updates
  - `PortfolioObservatory` as an INFO/report-only exposure surface
  - daily/session loss advisories that log at INFO without blocking entries
- added walk-forward audit attachment inside the retraining pipeline; it records artifact telemetry but does not insert a promotion delay between validation and live deployment
- added local governance tournament plumbing in the pipeline and live runtime:
  - offline per-context policy selection and artifact persistence
  - stateful live governance dispatch with `local_governance_live_mode = off|shadow|on`
  - order-governance state persistence in `pm_storage.py`
- synced `config.json`, `Normal config (Full Equity).json`, `SETTINGS_REFERENCE.md`, and the then-active implementation-tracking journal to the shipped keys and behavior
- cleaned remaining patch-history style comments in the touched runtime modules so comments explain function, not implementation history
- full verification now passes with `python -m unittest discover -s tests -p "test*.py"` (`429` tests, `OK`)

### Small-account proof-mode implementation

- `config.json` now ships a dedicated small-account high-risk live profile:
  - `position.risk_per_trade_pct = 2.0`
  - `position.max_risk_pct = 3.0`
  - `position.risk_basis = "balance"`
  - looser spread, same-symbol, and margin thresholds suited to the temporary proof phase
  - full symbol universe remains active
- `pipeline.risk_per_trade_pct` now stays at `1.0` so the tournament-facing baseline remains standard while the temporary live aggression sits downstream in `position`
- added `PipelineConfig.winner_ledger_path` plus `--winner-ledger`; the shipped high-risk profile isolates winners in `pm_configs_high_risk.json`
- live risk estimation now uses a shared cost-aware stop-loss helper so commission is included in:
  - pre-entry sizing and hard-cap checks
  - open-position risk estimation
  - post-fill executable-risk telemetry
- `MT5Connector.calc_margin_required()` added and wired into `_execute_entry()` so the PM blocks locally when required margin exceeds free margin before `send_market_order()`
- added a reason-coded min-lot hard-cap outcome (`BLOCKED_MIN_LOT_EXCEEDS_CAP`) for clearer tiny-account observability
- `PositionConfig.allow_min_lot_risk_clamp` added so the standalone sizing helper can opt into the live clamp-and-cap philosophy only when the resulting min-lot risk still fits `max_risk_pct`
- high-value auxiliary settings are now enabled in the shipped profile without contaminating discovery:
  - `local_governance_tournament_enabled = true`
  - `local_governance_live_mode = "shadow"` in the current active profile
  - `portfolio_observatory_enabled = true`
  - `live_risk_scalars_mode = "shadow"`
  - `daily_loss_advisory_pct = 8.0`
  - `session_loss_advisory_pct = 15.0`
  - `storage_observe_only = true`
  - `storage_metaquotes_cleanup_enabled = false`
- added focused regressions for the new behavior in:
  - `tests/test_config_source_of_truth.py`
  - `tests/test_position_sizing_edge_cases.py`

Note: the controlled live A/B promotion harness for the local governance tournament was tracked as in-progress in the archived implementation journal; the shipped code covers offline selection, artifact persistence, and live shadow/on dispatch.

### Documentation sync

- `SETTINGS_REFERENCE.md`: updated for the new Section 13 config surfaces (`portfolio_observatory_enabled`, walk-forward audit, local governance tournament, advisory loss thresholds) and the checked-in `regime_tp_multipliers` defaults
- the then-active implementation-tracking journal was updated with the final Section 13 sweep and accurate completion states, and has since been archived with the completed audit package
- `config.json` and `Normal config (Full Equity).json`: synced to the newly shipped pipeline and position keys with safe defaults
- expanded regression coverage now includes `tests/test_pipeline_config_fields.py`, `tests/test_enhancement_seams.py`, `tests/test_portfolio_risk_cap.py`, and `tests/test_section13_surfaces.py` in addition to the earlier Phase A-E tests
- repo-wide verification: `python -m unittest discover -s tests -p "test*.py"` passes (`429` tests, `OK`)

---

## [1.4.6] - 2026-04-02

### Restart recovery hardening

- fixed the live-loop `NameError` triggered when an existing position had an unknown timeframe after an ungraceful shutdown
- consolidated open-position timeframe inference into one helper with this priority order:
  - manual `position_timeframe_overrides`
  - live comment decoding
  - legacy `PM_<tag>` strategy-tag matching
  - deterministic magic lookup
  - MT5 opening order/deal metadata recovery
- added support for truncated `PM2`/`PM3` comments so broker-shortened comments can still recover symbol/timeframe
- cached MT5 history-based timeframe recovery per position identifier for the session lifetime
- throttled unknown-timeframe warnings to one warning per open position per session instead of repeated log spam

### Regression cleanup

- restored the code default for `pipeline.actionable_score_margin` to `0.92`
- restored the missing `PipelineConfig` scoring and validation fields used by the current regime-selection baseline
- aligned weak-train rejection messaging and candidate-descent handling with the active scoring-audit contract
- hardened enhancement-seam config coercion so partial or mocked pipeline configs fall back to safe numeric defaults
- fixed dashboard execution-log parsing for `[SECONDARY] Selected:` lines
- fixed dashboard temp-data startup and refresh-path behavior so route tests no longer depend on a pre-created `data/` directory
- completed the live margin-protection state-classification and deleveraging helper path used by the current tests
- refreshed stale test imports and the strategy-expansion baseline to the current `47`-strategy roster
- aligned live risk enforcement with the intended model:
  - `position.max_risk_pct` remains the per-trade hard cap
  - post-scalar `target_risk_pct` remains a sizing target, not the maximum allowed risk
  - same-symbol `max_combined_risk_pct` remains symbol-scoped, not portfolio-wide
- normalized risk fallback values to the active dataclass defaults so config stays authoritative and code backups stay consistent when fields are absent or mocked
- improved live observability:
  - startup config summary now reports the active same-symbol and per-trade risk controls
  - min-lot / volume-normalization drift above target risk is logged explicitly
  - market-order success logs no longer show `0.00000` when MT5 omits the returned fill price
  - throttled `NO_ACTIONABLE_*` live decisions are now surfaced at `INFO` so quiet sessions still show that the PM is evaluating bars
  - each completed live sweep now emits a heartbeat with symbol count, open-position count, equity, and sweep duration
- added regression coverage for config precedence and the live hard-cap-vs-target-risk path
- repo-wide verification now passes with `python -m unittest discover -s tests -p "test*.py"` (`237` tests, `OK`)

### Documentation sync

- updated `README.md`, `SETUP_AND_RUN.md`, and `SETTINGS_REFERENCE.md` to document restart recovery, restored scoring defaults, `position_timeframe_overrides`, and the clarified live risk model
- refreshed patch notes and version references for the `1.4.6` repository state

### Storage governance foundation

- added config-backed storage controls under `pipeline.*` for PM-owned retention, disk-pressure thresholds, write-protection windows, MetaQuotes review cadence, and signal-ledger behavior
- added a PM-native storage manager that:
  - records storage state and cleanup manifests in `pm_outputs`
  - runs on sweep/shutdown/optimization hooks without adding a sleep-based janitor loop
  - keeps cleanup comparator-driven via due-time checks
  - supports observe-only rollout by default
- added a monthly JSONL actionable signal ledger under `pm_outputs/signal_ledger_YYYYMM.jsonl`
- wired actionable live outcomes to both `last_actionable_log.json` and the new signal ledger
- hardened `save_trade_log()` to use atomic replace writes
- moved runtime logging setup onto config-driven `pipeline.log_dir`
- upgraded historical M5 refresh to merge, deduplicate, and cap in place instead of blindly overwriting full files
- corrected the local-data-first live path so `live_bars_count` is respected at the I/O layer:
  - live sweeps no longer full-read `*_M5.csv` for every symbol before trimming
  - the trader now maintains bounded timeframe-specific `.live/<symbol>_<TF>.csv` source caches
  - live refresh now delta-merges from the last stored timestamp with a config-backed overlap window via `storage_live_sync_overlap_bars`
- added guarded MetaQuotes discovery for demo roots and stale tester bases, including the post-optimization tester-review path
- cleaned duplicate `pipeline` keys in `config.json` so storage and risk values remain unambiguous
- added regression coverage for storage defaults, PM-owned retention, write-protection windows, signal-ledger writes, and MetaQuotes candidate discovery
- repo-wide verification now passes with `python -m unittest discover -s tests -p "test*.py"` (`237` tests, `OK`)

### Final hardening pass

- corrected the local-data-first live path for mixed timeframes:
  - timeframe-specific `.live/<symbol>_<TF>.csv` caches now remain authoritative for live reads
  - undersized local caches fall back cleanly to MT5 for the missing depth instead of silently degrading the requested bar target
  - bounded live-cache writes remain atomic
- completed a second live hardening pass focused on quality-first runtime behavior:
  - bounded live caches now stop delta-stitching across long local gaps and re-seed from MT5 instead of carrying discontinuous series forward
  - `storage_live_sync_bars` now participates in real M5 reseed behavior rather than existing only as a logged config knob
  - the outer live scheduler now derives the next wake-up from per-symbol/per-timeframe due state first, using the stale-retry cadence only as a recovery fallback
  - same-symbol exposure checks now estimate open-position risk from live entry/SL/volume geometry first and use comment metadata only as a fallback
  - secondary-trade snapshot failures no longer touch unassigned runtime values in actionable logging
  - live winner scoring now preserves legitimate `0.0` quality scores and uses an explicit deterministic tie-break instead of implicit config-order bias
- restored strict candle-close live evaluation semantics for the regime-aware path:
  - stale symbol/timeframe branches are skipped before bar loading once that timeframe is not due
  - freshness decay is no longer used to keep stale higher-timeframe candidates alive between bars
  - the main winners-only path now matches the intended no-new-bar, no-re-evaluation behavior
- added `storage_live_cache_max_age_days` so PM-owned `.live` caches are governed separately from `data/.cache`
- normalized nested root-level `storage` config sections into `PipelineConfig` keys so natural config shapes no longer get silently dropped
- made `config.json` more trustworthy as the operator source of truth:
  - added explicit `live_risk_scalars_enabled` and `target_annual_vol` entries
  - removed duplicate instrument-spec keys that were previously being shadowed by later JSON entries
  - added regression coverage to fail if duplicate JSON object keys reappear
- removed dead internal wrappers from the PM/dashboard runtime to reduce maintenance surface without changing behavior
- replaced the dashboard data-maintenance scheduler's fixed sleep loop with the same due-time comparator pattern used by the PM runtime and removed per-symbol sleep pacing from the root-data refresh path
- updated `README.md`, `SETUP_AND_RUN.md`, `SETTINGS_REFERENCE.md`, and dashboard docs so the documented live flow now matches the real runtime:
  - `position.risk_per_trade_pct` is the authoritative live target risk
  - risk scalars are opt-in
  - local-data-first uses timeframe-specific bounded live caches rather than full canonical reloads
- repo-wide verification now passes with `python -m unittest discover -s tests -p "test*.py"` (`253` tests, `OK`)

---

## [1.4.5] - 2026-04-01

### Core PM upgrade completion

- completed the major PM implementation pass driven by the archived audit and upgrade specs
- kept the active live regime engine as the score-based 4-regime detector
- expanded the live strategy registry from `42` to `47`
- hardened backtest/live parity around same-bar gap-through-stop handling
- strengthened validation, regime selection, and optimizer reproducibility

### Strategy roster

Added:

- `VortexTrendStrategy`
- `TRIXSignalStrategy`
- `RelativeVigorIndexStrategy`
- `VIDYABandTrendStrategy`
- `ChoppinessCompressionBreakoutStrategy`

### Live trading and MT5 hardening

- removed the legacy `--auto-retrain` path
- moved production retraining to the calendar schedule controlled by:
  - `production_retrain_mode`
  - `production_retrain_interval_weeks`
  - `production_retrain_weekday`
  - `production_retrain_time`
  - `production_retrain_anchor_date`
- hardened MT5 symbol resolution, tradability checks, order preflight, and partial-fill handling
- wired the spread-quality overlay into config with:
  - `execution_spread_filter_enabled`
  - `execution_spread_min_edge_mult`
  - `execution_spread_spike_mult`
  - `execution_spread_penalty_start_mult`

### Dashboard alignment

- fixed analytics expectancy and pip-value handling
- hardened watcher, jobs, parsers, and utils behavior
- aligned templates/static assets with current dashboard behavior
- clarified the dashboard as read-mostly rather than fully read-only

### Documentation sync

- rewrote `README.md` to match the current PM architecture and runtime behavior
- rewrote `SETUP_AND_RUN.md` for the fixed retrain schedule, root `logs/`, and current live flow
- rewrote `SETTINGS_REFERENCE.md` against the current config and code semantics
- rewrote `pm_dashboard/README.md` for current dashboard capabilities and write behavior
- refreshed this changelog so the repo state and docs align

---

## [1.4.4] - 2026-02-07

### Winners-only cleanup

- removed deprecated fallback/tier risk artifacts from the main live path
- simplified `_execute_entry` around a winners-only risk model
- updated trade comment formats while preserving backward decoding compatibility

### Warning and config cleanup

- fixed a broad batch of pandas warning sources
- aligned `fx_min_robustness_ratio` propagation across code paths
- added optimization progress visibility

---

## [1.4.3] - 2026-02-07

### Config and indicator cleanup

- expanded config documentation coverage
- introduced shared indicator helper caching
- added live-loop integration tests

---

## [1.4.2] - 2026-02-07

### Strategy expansion

- expanded the roster from `27` to `42` strategies

### Safety and dashboard upgrades

- improved warmup protection, numeric guards, and MT5 parity
- added a major dashboard capability upgrade

---

## [1.4.1] - 2026-02-01

### Optimization and efficiency

- integrated Optuna TPE
- introduced major speed improvements around backtesting and regime work
- added stateful optimization persistence

---

## [1.4.0] - 2026-01-15

### Initial release

- introduced the core regime-aware PM architecture
- shipped the initial 27-strategy version
- added MT5 live trading and the original dashboard
