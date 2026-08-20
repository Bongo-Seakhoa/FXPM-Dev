# FxPM — the ideal end state

**What this document is for.** The dashboard shows programme *mechanics* — gates,
lanes, bus traffic. It does not say what we are building, what it runs on, or what
we already tried and rejected. This says all three, with numbers rather than
adjectives.

Written 2026-08-14. Every figure here is measured from the repository, not
recalled.

---

## 1. What FxPM is, in one paragraph

A regime-aware portfolio manager that searches **100 instruments × 6 timeframes ×
4 regimes = 2,400 cells**, promotes only the cells whose out-of-sample evidence
survives multiple-testing correction, and trades the survivors on MT5 with
per-cell attribution and hard risk caps. The product is not a strategy. **The
product is the selection process** — a machine that can tell the difference
between an edge and a lucky backtest, and that refuses to promote when it cannot.

The June 2026 book is the counter-example that defines the target: 157 promoted
rows, median validation Sharpe 38.23, and **both multiple-testing defences
inoperative**. It was noise wearing a promotion. Everything below exists to make
that outcome impossible rather than unlikely.

---

## 2. The methodology, end to end

Each numbered step is a real stage in the current code, not an aspiration.

> **Corrected 2026-08-20.** Stages 4 and 6 originally described the *legacy*
> route — `RegimeOptimizer._call_regime_score → _compute_regime_score`, a
> weighted scalar with 200 Optuna trials — as current production. That was
> wrong. Codex-PM's source audit (XPM-074, XPM-086 Round 0) established, and I
> verified, that `_compute_regime_score` and `_call_regime_score` appear **only
> in `pm_pipeline.py`** and are never reached from
> `pm_authoritative_search_execution.py`, `pm_winner_selection.py`,
> `pm_historical_production_adapter.py` or `pm_winner_raw_evidence.py`.
> Production selects through **Winner v13**. The error is left visible rather
> than silently rewritten, because it matters *why* it happened: I read which
> settings exist instead of which code runs — the third time (see CPM-051,
> CPM-058).
>
> **Consequence worth stating plainly:** the concern that the scoring
> parameters were educated guesses applies to the legacy scalar, which **does
> not decide production winners**.

| # | Stage | What it does | Where it lives |
|---|---|---|---|
| 1 | **Native data authority** | Per symbol/timeframe leaves under a run manifest, each sha256-verified. Nothing enters the engine unattested. | `data/native/runs/…`, `pm_native_activation.py` |
| 2 | **Feature computation** | ~70 columns per symbol/timeframe frame; ATR, MACD, regime inputs. | `pm_core.FeatureComputer` |
| 3 | **Regime labelling** | Every bar assigned TREND / RANGE / BREAKOUT / CHOP. Cells are `symbol\|timeframe\|regime`. | `pm_regime.py` |
| 4 | **Search** | Per cell the planner freezes **exactly 300 attempts** — 100 registry defaults plus two deterministic non-defaults per strategy. Not an Optuna trial budget. | `pm_production_search_planner.py:113-133` |
| 5 | **Validation geometry** | Purged and embargoed walk-forward; **four non-overlapping 63-day rolling-origin folds**; a final lockbox consumed once per lineage. | `calendar_lockbox_v2` |
| 6 | **Selection — Winner v13** | Statistical admissibility and paired dominance on a **dependence-robust lower confidence bound of daily log growth**. Worst-fold *and* newest-fold log growth must be positive, and again under cost stress. No weighted scalar, no tuned constants. | `pm_winner_selection.py:209-258` |
| 7 | **Multiple-testing defence** | PBO via CSCV, and Deflated Sharpe using the two-term Bailey–López de Prado expected-max with effective independent trials. | `pm_statistics.py` |
| 8 | **Promotion gates** | Profit factor ≥ 1.10, ≥ 20 trades, ≤ 25% drawdown, DSR ≥ 0.95, PBO ≤ 0.5, ≥ 252 daily observations. | `config.json` pipeline block |
| 9 | **Evidence receipt** | Each promoted cell carries a receipt binding data identity, frozen process, and outcome. Replayable; tamper fails closed. | `pm_winner_raw_evidence.py` |
| 10 | **Live execution** | MT5, magic-number cell attribution (2,400 unique, collision-guarded), lot geometry floored to volume step, per-cell risk caps. | `pm_main.py` |

**The one-line summary:** *search wide, validate hard, promote rarely, attribute
everything.*

---

## 3. The data it runs on — measured

| | |
|---|---|
| Instruments | **100** — FX majors and crosses, indices (AU200), metals, crypto (BTCUSD, LTCUSD, BCHUSD), single-name CFDs (APPLE, AMAZON, ALCOA) |
| Timeframes | **M5, M15, M30, H1, H4, D1** |
| Total data on disk | **16 GB** |
| Native authoritative store | **14 GB**, 736 verified leaves |
| Deepest single leaf | `BTCUSD/M5` — **1,064,913 bars**, 160 MB |
| Typical M5 depth | ~300,000 bars ≈ 4 years |
| Layout | `data/native/runs/production_native_v5r2_20260714/<SYMBOL>/<TF>/bars.csv` |

**Why this matters to the design:** the deepest cells carry a million bars and the
shallowest carry a few thousand. That spread is why D1 failed on trade counts
rather than on strategy quality, and why a single global minimum-observation
threshold is a blunt instrument.

---

## 4. The eight gates, and what each actually proves

| # | Gate | Proves | State |
|---|---|---|---|
| 1 | PBO returns a per-cell verdict | The multiple-testing machinery *runs*, rather than reporting `not_available` as it did for the June book | **DONE** — pbo=0.0, N=224, T=253 |
| 2 | A timeframe with enough activity to authenticate | The geometry can produce a candidate at all | **DONE** — real technical result, E25 resolved the blocker |
| 3 | A full run that completes | The pipeline survives its own scale | open |
| 4 | Replay the book, corrected enumerator | Results reproduce from receipts | open |
| 5 | Forward Test A — reject-only | The frozen lineage does not collapse out of sample | pre-registered |
| 6 | Execution defects closed | Lot sizing, recovery, order scaling — the three defects that killed the funded attempt | **DONE** |
| 7 | Demo shadow | Live plumbing matches backtest assumptions | open |
| 8 | **Test B — the only certification** | Profitability on 365 days of data nobody has seen | blocked on data — first valid day 2026-08-18, credible verdict **2027-08-17** |

**Gate 8 is the honest schedule and nothing shortens it.** No code change, no
efficiency win, no additional gate substitutes for a year of unseen data.

---

## 5. What was tried and rejected

The full record is `docs/BAD_IDEA_REGISTER.md` (11 entries, each with a revival
trigger) and `docs/RECONCILIATION_REGISTER.md` (20 elements). The ones that
shaped the current design:

| Rejected | Why | Can it come back? |
|---|---|---|
| Five alternate scoring models — `specialist_spokes`, `_v2`, `_asym`, `threadspark`, `legacy_confidence` | Unreachable under production config; 2,652 lines deleted | No — one canonical scorer is the directive |
| `min()` clamp on frozen-path stress swap | Forced monotonicity by discarding the disagreement instead of fixing its cause | No — superseded by E25's settlement-relative delta |
| Feature-cache identity as `id(frame)+len` | Returned a **fifty-times-wrong ATR** on same-length mutation | No — length is not identity for mutable content |
| Caching a Series in `DataFrame.attrs` | Breaks every later `pd.concat` on that frame | No — pandas comparison semantics |
| The 576M-backtest "years" cost model | Mixed the legacy path into the canonical executor | No — forecasts must come from one measured cell |
| Config-only `evidence_as_of_utc` cutoff for Test A | Activation authority overrides config; the run would not be blinded | No |
| Cutting the hyperparameter budget to buy runtime | Class B change wearing Class A clothes | **No — interred by operator directive** |
| Fixed `--basetemp` in pytest | Concurrent agents wipe each other's temp files | Yes — if only one agent runs tests |
| `specialist_spokes_v2_max_validation_sharpe` guard | Unreachable under `regime_scoring_model=legacy` | Yes — if the scoring model changes |
| `tools/agent_coordination.py` (1,543 lines) | Anti-tamper ledger for two agents on one laptop with no attacker | Yes — if a real multi-party trust boundary appears |
| Removing the eager native verification sweep | A stale receipt or direct bars consumer would bypass runtime | Yes — E27b's preconditions are exactly this trigger |

---

## 6. What "done" means

FxPM is finished when **all eight gates are closed and Test B has run its full
365 days**, producing a book whose out-of-sample performance is consistent with
its backtest under unchanged costs and controls.

Concretely, the acceptance statement is:

> A promoted book exists. Every cell in it carries a replayable receipt. Its
> promotion survived PBO and DSR at the configured thresholds. It was scored on
> data that did not exist when the strategy was frozen. It traded a demo shadow
> without plumbing divergence. And 365 days of unseen data did not falsify it.

Anything less is a candidate, not a product. **Nothing before Gate 8 authorises
real money**, however good the numbers look.

---

## 7. Where the effort actually goes

Measured, current:

| | |
|---|---|
| Full verification suite | **4,245 tests, 2h 36m** |
| Native authority preflight | **36m 58s** per run (E27b targets this) |
| One authoritative fold package | 1.01 s |
| E27a1 selected-artifact check | 3.2885 s |
| Backtest loop floor | **6.7 ms** on 106,697 H1 bars |

The gap between the 6.7 ms loop and everything wrapped around it is the whole
efficiency programme. It is Class A work only — pure overhead removal, no
coverage change, no output change — because **coverage is never traded for
runtime**.
