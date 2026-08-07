# Depth-of-Market Last-Mile Filter — Discovery & Design

**Owner:** Claude (operator-mandated quarterback, 2026-07-23). Codex remains contained on the
canary/optimize chain.
**Status:** Phase 0 (discovery) COMPLETE — empirical broker probe done. Phase 1 (design) drafted
below, pending operator discussion.
**Operator directive:** DOM is the institutional last-mile filter — "the final cheat code" for
good position ordering. It is an **overlay / final filter on the order path, NOT a strategy.**
It must be professionally built, requested, applied as the last filter, and then proven to have
a **net positive effect** before it gates real orders.

---

## 1. Empirical findings (probe 2026-07-23, FBS-Real 28829971, MT5 build 5833)

Read-only probe of the full 100-symbol pinned universe (`scratchpad/dom_probe.py`):

| Question | Result |
|---|---|
| Symbols declaring DOM (`ticks_bookdepth > 0`) | **1 of 100** (CADCHF=32 — config artifact; book subscribes but never populates: `entries=0`) |
| `market_book_add` on FX / indices / stocks / metals / crypto | **Refused for all sampled symbols** (returns False + error "Success" = no book channel) |
| Real traded volume (`real_volume`, bars and ticks) | **0 everywhere — including APPLE, NVIDIA, TESLA, US500** |
| Tick volume (price-update activity) | **Rich and universal** (24×H1 sums: NVIDIA 149k, APPLE 136k, XAUUSD 310k, EURUSD 60k) |
| Per-bar spread state | **Fully populated, session-sensitive** (DE30 spread halved 520→260 pts as the European session opened; US30 390; EURUSD 8) |
| Historical DOM (any MT5 broker) | **Does not exist** — the MT5 API has no historical book; DOM is live-subscription only |

**Ground truth: FBS provides no order book for any instrument class — including the indices
and stocks.** They are CFDs without depth. Additionally, even if DOM existed, MT5 offers no
historical book, so a DOM filter can never be validated in backtest — only by live shadow
capture.

## 2. What this means for the directive

The institutional *purpose* of DOM at the last mile is: **"will my order, at my size, right
now, execute well?"** — i.e., predicted execution quality. FBS denies us the book, but it does
give us every observable that *prices* that same question, plus one instrument institutions
value even above DOM:

1. **Live spread state** — current spread vs its session-conditional baseline. Spread widening
   is the single most direct broker-side signal of thin liquidity (and spread realism is
   already this codebase's #1 modeled cost).
2. **Tick-rate / activity state** — price-update frequency vs session baseline (quote staleness
   = danger; the rich `tick_volume` the operator has seen on indices/stocks is exactly this).
3. **Own-fill TCA (transaction cost analysis)** — `order_send` returns the actual fill price;
   requested-vs-filled slippage per symbol per session-state is *measured*, not predicted,
   execution quality. DOM is the ex-ante proxy; realized slippage is the ex-post truth.

So the deliverable becomes an **Execution-Quality Gate (EQG)**: the same last-mile filter DOM
would drive, built on the observables this broker actually serves, with real DOM from an
external venue as an optional later upgrade (§6).

## 3. Where it slots

The four `order_send` seams in `pm_mt5.py` are the entire last mile:
`send_market_order` (:1665), `close_position` (:1853), `close_position_partial` (:1932),
`modify_position` (:2045).

The EQG wraps **entry orders only** (`send_market_order`) in v1 — never exits. Blocking an
exit on liquidity grounds holds risk open; institutionally you pay the spread to get flat.
Exits get TCA logging only.

Placement is in the **live/demo execution layer** (the validation/authentication side, exactly
as the operator anticipated), *after* the winner/signal decision and *before* the MT5 request
is built. It never touches: search, optimization, statistical gates, winner selection, the
canary chain, or any Codex lane. Zero coupling to M4-M6.

## 4. Design — Execution-Quality Gate v1

**Inputs at decision time** (all available from FBS today):
- `symbol_info_tick`: live bid/ask → current spread
- Session-conditional spread baseline (rolling percentile per symbol × hour-of-week, built
  from the shadow log; cold-start from the bars' spread field)
- Tick-rate over the last N seconds vs baseline (staleness / activity)
- Intended size vs symbol max/step (sanity)
- Own-fill history: realized slippage distribution per symbol × session-state (accumulates)

**Verdicts** (deterministic, typed, logged):
- `ALLOW` — spread and activity within baseline bands → send unchanged
- `DEFER(reason, ttl)` — spread > p95 baseline or quotes stale → hold up to ttl seconds,
  re-evaluate; on expiry send anyway (v1 never silently drops a winner's order) and tag the
  fill for TCA
- Everything is logged with the full observable vector + the counterfactual (what would have
  happened had the gate acted the other way)

**Fail-open by construction in v1:** any EQG error, missing baseline, or timeout → order goes
out exactly as today, tagged `EQG_BYPASS`. The book's behavior is unchanged until evidence
says otherwise. Fail-open (not fail-closed) is deliberate here: the gate is an *optimization*
overlay, not a safety invariant — a missing overlay must not stop a proven winner from trading.

## 5. Evidence protocol (operator requirement: prove net effect)

Three stages, each gated on the previous:

1. **SHADOW (weeks 1-2+):** EQG computes verdicts and logs everything but **acts on nothing**.
   Deliverable: per-symbol spread/activity baselines + verdict distribution + the TCA ledger of
   actual fills.
2. **COUNTERFACTUAL ANALYSIS:** compare realized slippage on fills sent during `ALLOW` states
   vs `DEFER` states (the shadow log contains both, since nothing was blocked). Statistical
   question: does the DEFER state predict materially worse fills, and would the ttl-deferred
   re-entry price + delay cost have netted better? Report with the same statistical honesty as
   the rest of the chain (no aggregation tricks; per-symbol verdicts).
3. **GATED ACTIVATION:** only symbols/states where stage-2 shows a net-positive effect get the
   active gate; everything else stays shadow. Re-evaluated on the same cadence as the retrain
   schedule.

This mirrors the operator's run-first-then-prune charter: build → measure → keep only what
provably helps.

## 6. Real DOM (post-M6 option, if wanted)

Real depth for the index/stock subset exists at their underlying venues (CME ES/NQ/YM for
US500/US100/US30, Eurex for DE30, NYSE/NASDAQ L2 for the stocks). Getting it means an external
feed (second broker with true DOM, or a market-data subscription) used **data-only** to gate
FBS CFD orders. Evaluate only after EQG shadow evidence shows liquidity-state gating carries
value; then the sharper instrument may justify its cost and new dependency.

## 7. Non-goals / constraints

- NOT a strategy, no signal generation, no coupling into the search space or optimizer.
- No backtest contamination: the backtest chain keeps its existing conservative_max/stress_p95
  spread model; EQG is a live-side overlay with its own accumulating evidence.
- No new hashing/sidecar ceremony (leanness directive): the shadow log is a single append-only
  JSONL per symbol-set with place-overwrite rotation.
- Respects agent protocol: Claude-owned lane; no shared-core edits until the design is agreed
  and the canary chain (M4-M6) is not blocked by anything EQG touches.

## 8. Current status & next actions

- [x] Codebase + MT5 integration survey (order path mapped; DOM API unused today)
- [x] Empirical DOM/volume probe on FBS-Real (findings §1)
- [ ] Operator discussion: confirm EQG direction + shadow-first rollout (this doc)
- [ ] Phase 2: implement shadow EQG (recorder + baselines; zero behavior change)
- [ ] Phase 3: counterfactual evaluation report
- [ ] Phase 4: gated activation for net-positive symbols/states only
