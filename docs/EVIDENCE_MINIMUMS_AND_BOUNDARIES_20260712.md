# Evidence Minimums, Timeframe Sufficiency, and System Boundaries

Author: Claude (audit + research lane) — 2026-07-12
Status: reasoning record for the 2026-07-12 corrections; companion to the
per-cell eligibility contract Codex is implementing. Nothing here is a
profit forecast.

---

## 1. The question being answered

The operator asked: 365 D1 bars represent one year, while the same year gives
~75,000 M5 bars. How does that translate into signals, strategy longevity,
and the minimum data needed to call a D1 strategy consistent? And which parts
of the system must be independent versus deliberately shared?

## 2. Bars are the wrong unit. The chain is bars → signals → trades → effective trades → power

A strategy is validated on **trades**, not bars. The correct sufficiency
chain:

1. **Bars/year by timeframe** (FX 24/5 reference; sessioned CFDs lower):

   | TF | Bars/year | Bars in 5y train | Bars in 2y tuning |
   |----|----------:|-----------------:|------------------:|
   | M5 | ~75,000 | ~375,000 | ~150,000 |
   | M15 | ~25,000 | ~125,000 | ~50,000 |
   | M30 | ~12,500 | ~62,500 | ~25,000 |
   | H1 | ~6,260 | ~31,300 | ~12,500 |
   | H4 | ~1,570 | ~7,800 | ~3,100 |
   | D1 | ~261 | ~1,300 | ~520 |

2. **Trades/year** is bars × signal rate and is strategy-specific. Realistic
   ranges per specialist cell: intraday 50–400, H4 20–100, D1 10–60.

3. **Effective trades < raw trades.** Serial dependence and overlapping
   holds shrink the information content. This is now computed, not assumed:
   `pm_sample_dependence.py` (integrated autocorrelation time, Geyer initial
   monotone estimator) converts raw observations into effective observations.

4. **Power determines the minimum.** For a per-trade edge of `e` (in units of
   per-trade return dispersion σ), one-sided α=0.05, 80% power:

   `N ≈ (z₀.₉₅ + z₀.₈)² / e² ≈ 6.18 / e²`

   | Per-trade edge (σ units) | Trades needed |
   |---:|---:|
   | 0.05 | ~2,470 |
   | 0.10 | ~620 |
   | 0.15 | ~275 |
   | 0.20 | ~155 |
   | 0.30 | ~69 |

   Multiplicity (searching many strategies/parameters per cell) raises these
   further; `pm_trade_power.py` applies a Šidák effective-trials adjustment.

**The honest D1 conclusion:** a D1 cell trading ~1×/week accrues ~104 trades
in the 2-year tuning partition. At that budget the minimum *detectable* edge
is ≈ `sqrt(6.18/104)` ≈ **0.24σ per trade**. More years help linearly, but D1
physically caps trades/year — so the system must (a) demand larger unit
economics from low-frequency cells rather than pretend years substitute for
power, (b) use the full broker D1 depth where it exists (84 symbols passed
strict D1 criteria in the 2026-07-12 recount), and (c) never apply one
folklore trade floor (e.g. "15 trades") across timeframes. This is exactly
what power-derived per-cell floors implement.

**Folklore floors are retired.** The earlier timeframe floors (D1 15 /
H1–H4 25 / intraday 30) were direction-correct but unprincipled. The
replacement contract: a cell is promotable only when its **effective** trade
count meets the power requirement for the **economic edge it claims**, at the
declared α/power, after multiplicity. Small-N cells with big true edges can
pass; small-N cells with small claimed edges honestly cannot — they park as
`NO_TRADE(insufficient evidence)`, not as fake winners.

## 3. Strategy longevity vs timeframe

- Higher-TF strategies ride slower structures: fewer regime transitions per
  holding period, less microstructure/spread sensitivity → **slower decay,
  longer usable validation windows, longer retrain cycles.**
- Intraday strategies decay faster (spread regime changes, session
  microstructure) → shorter half-life; the 28-day retrain cadence is sized
  for these, and is conservative for D1/H4.
- Implication adopted: a common 5y/2y/2y calendar is kept for portfolio
  synchronization, but **sufficiency is judged per cell in effective trades**
  (§2), and D1/H4 windows may extend deeper where broker history exists.
  Retrain cadence per timeframe (longer for D1/H4) is a sensible later
  refinement — flagged, not yet implemented.

## 4. Independent vs shared — the boundary map

**Independent by design (must never couple):**

| Element | Status |
|---|---|
| Per-TF native acquisition (each TF fetched directly from MT5; no M5 resampling in calendar-v2) | verified in `pm_pipeline._load_symbol_timeframe_data` (strict_native branch) |
| Per-cell (SYMBOL×TF) eligibility — one bad TF cannot veto siblings | landed 2026-07-12 (Codex); the H1/D1 spine no longer requires M5 (`pm_pipeline.py` picks the first active TF) |
| Per-cell regime parameters + provenance | preflight now verifies every active cell before workers spawn |
| Per-cell strategy search and evidence gates | per-cell NO_TRADE markers carry activation bindings |
| Infrastructure failure state vs trading verdict | landed 2026-07-12: technical failures leave the ledger unchanged ("technical failures are not trading verdicts") |

**Deliberately shared (dependencies with a reason):**

| Element | Why it must stay shared |
|---|---|
| Common UTC calendar partitions per symbol | synchronized portfolio replay and capital-envelope calibration require aligned windows |
| D1 comparator in cadence-authenticity QA | data QA only — detects daily bars masquerading as intraday (AU200 2017 case); it is not signal coupling |
| One MT5 terminal session | physical constraint: one broker clock/sync state; bulk acquisition and live trading must not run concurrently; live delta-sync stays retry-free (hot path) while acquisition owns hydration/retries |
| Symbol-level combined-risk caps (D1 + one lower-TF position; `max_combined_risk_pct`, `secondary_trade_max_risk_pct`) | prevents self-collision on one symbol without freezing capital for the whole D1 hold |
| Institution-level risk envelope | survival rules must bind all specialists identically |

**The "M5 competes with H4 for allocation" question.** Correct resolution is
admission control, not signal-strength arbitration: each specialist earns its
slot through validated evidence; at runtime `pm_risk_admission.py` admits
entries against the gross-risk envelope, and the envelope ceiling itself must
come from `pm_portfolio_replay.py` + `pm_cap_calibration.py` (synchronized
replay of the actual winner book measuring drawdown/margin distributions) —
not from a feel number in either direction (neither 3% nor 90%).

**Risk appetite translation (medium-high, own capital, loss-tolerable):**
appetite enters as the **ruin-tolerance parameter** given to envelope
calibration (e.g., accept a materially higher probability of a deep drawdown
than an institution would), *never* as looser evidence standards, wider
spreads assumptions, or skipped gates. Bad evidence at high risk is just a
faster way to lose; the same evidence with a higher calibrated envelope is a
risk preference, honestly priced.

## 5. Bounded memory and storage (use-and-dump contract)

Requirement (operator, 2026-07-12): all memory and storage must be bounded by
the processes that use them — no unbounded growth — with zero accuracy cost.

Partially landed today (the remaining gaps are explicit; this is not a full
boundedness sign-off):

- **Memory** (Codex lane): `ensure_utc_index(copy=False)` borrow path for
  read-only hot paths. The strict-native loader still preloads all active
  timeframe frames for a symbol, so full per-TF use-and-release is **not yet
  implemented**. Remaining loader/pipeline copy amplification and a
  memory-aware worker governor are open; the 8 GB OOM guardrail in
  AGENT_COMMS still stands until peak usage is re-measured.
- **Storage** (Claude implementation, independently corrected by Codex):
  - `storage_manifest.jsonl` (was 69 MB, append-forever) now rotates at
    `storage_manifest_max_mb` (16 MB default) keeping
    `storage_manifest_keep_rotations` (2) generations. Rotate plus append is
    one OS-locked transaction across threads and processes; a rotation failure
    cannot suppress the terminal event being recorded. No reader depends on
    this pure housekeeping journal.
  - Observe-only journal spam deduplicated: identical
    trigger/action/path/reason/result/size records journal once per
    `storage_manifest_dedupe_hours` (24h); terminal results
    (deleted/failed) always journal. The dedupe index itself is bounded
    (512 paths, oldest-evicted).
  - Monthly `signal_ledger_YYYYMM.jsonl` files are canonical inputs to
    `pm_dashboard.analytics.load_trade_history`; deleting older months changes
    lifetime equity, drawdown, and performance results. The attempted
    age-based deletion was therefore removed. Appends are now OS-locked, but
    exact accuracy-preserving bounded compaction/archive remains open.

Accuracy guarantee: the implemented destructive bounds act only on the pure
housekeeping manifest and disposable caches. Canonical signal/trade history,
decision inputs, evidence artifacts, and the winner ledger are not silently
deleted. Consequently, canonical-ledger storage is honestly **not yet
bounded** rather than falsely claiming boundedness by degrading analytics.

## 6. Open items (next in line)

1. Exact bounded canonical-ledger design: either an externally archived,
   checksummed event stream with transparent readers, or a formally proven
   exact summary contract plus a separately retained audit source. A rolling
   delete that silently turns lifetime metrics into 12-month metrics is not
   acceptable.
2. Atomic generation promotion (contract pt 9): per-symbol incremental saves
   remain; a complete-generation gate before replacing the book is still open.
3. Memory governor, per-timeframe release, and remaining copy elimination;
   then re-measure peak commit
   on the 7.36 GB host before any multi-worker run.
4. Per-TF retrain cadence (longer for D1/H4) once the new book exists.

## 7. Verification trail for this document

- Recount figures (287 cells / 84 symbols / 77 suppressed) cross-checked
  against `data/native/activations/20260711T145205Z-092ca527.json` (210
  active cells, 48/162 regime cells present/missing verified by script).
- Per-cell contract points 1, 2, 5, 6, 7 verified landed in code on
  2026-07-12 (preflight at `pm_pipeline._preflight_active_regime_provenance`;
  ledger-preserving failure branch; active-TF-only loading; M5-spine
  removal; per-cell audit statuses in `tools/audit_activate_native_history.py`).
- Storage regressions include thread/process contention, terminal-event
  preservation, rotation bounds, semantic deduplication, and canonical-ledger
  non-deletion; see `tests/test_storage_bounds.py` and the existing storage
  suites.
