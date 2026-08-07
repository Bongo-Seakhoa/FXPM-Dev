# Claude Code — Decisive Replay Finding (2026-06-18)

## Headline
**The current production ledger (`pm_configs.json`) contains exactly ONE live-eligible promoted
winner, and it is a backtested net loser.** Do not treat the live book as profitable in its current
state.

## Verified evidence (read-only)
Direct tally of `pm_configs.json` (100 symbols × 6 TF × 4 regime = 2400 regime leaves + 100 symbol-level):
- Symbol-level `promotion_status`: **96 NO_TRADE, 3 BLOCKED, 1 PROMOTED**.
- Regime leaves: **2387 NO_TRADE, 12 BLOCKED, 1 PROMOTED**.
- Sole PROMOTED, live-eligible winner: **FORD H4 CHOP `DoubleEMATrendStrategy`**
  (`is_validated=True`, `valid_until=2026-07-05`).

Canonical $10,000 replay of that winner (`promoted_winner_replay_all_10000.csv`):
- **net −$2,317.84 (−23.18%), PF 0.66, 166 trades, max DD 24.88%, avg MAE 0.74R, ~0.80R MFE given back.**

**Selection-bias smoking gun (concrete F111/F002):** the winner was promoted on **16 validation trades
showing PF 1.77**, but the **166-trade replay shows PF 0.66**. The promotion rests on a tiny, optimistic
validation sample (just above `fx_val_min_trades=15`) that inverts on the full sample. It is also a
**trend strategy assigned to the CHOP regime** — a structural mismatch.

## "157 tradeable cells" is STALE
`agent_audit/data/ledger_tradeable_cells.csv` (mtime 2026-06-17 13:30) predates a **massive rewrite of
`pm_configs.json`** (mtime 2026-06-18 11:05; `git diff --stat`: **−434,482 / +60,598 lines**). Every
Codex artifact/report citing "157 tradeable cells across 45 symbols" describes a **prior ledger state**,
not the current one. The current state is 1 promoted winner.

## Replay-tool enumeration bug (review of Codex PERF-FIX-005)
`run_promoted_cell_replay.iter_promoted_cells` only descends into `regime_configs[timeframe][regime]`
leaves (which are ~all NO_TRADE markers) and **never evaluates the symbol-level promoted winner**, so it
enumerated **1 of the intended book** — FORD was caught by accident. The −23% datapoint is real but was
not a representative sample. Corrected enumerator: `claude_replay_all_promoted.py` (symbol-level), which
confirms the current book = 1 promoted winner.

## Implications
1. **Do not go live on the current ledger.** It would trade ~one strategy, which backtests as a net loser.
2. If `pm_configs.json` is **mid-re-optimization** (the −434k-line rewrite suggests a reset/in-progress
   run), let it complete and re-check; the interim state is not tradeable.
3. The sole surviving winner is a **selection-bias artifact** (16-trade PF 1.77 → 166-trade PF 0.66) — this
   is the live, concrete proof behind F111 and Codex F002. The objective/validation-floor fixes
   (CC-AB-4 / Q4 timeframe floor) are now demonstrably high-value, not theoretical.

## Verdicts on Codex's implemented fixes (review lane)
- **PERF-FIX-001/002/003: ACCEPT** (verified earlier — measurement-only, parity-tested, JSON-safe).
- **PERF-FIX-004 (schema versions): ACCEPT** (emitted in both backtest paths + actionable log; additive;
  follow-up: add a consumer-side guard that branches on version).
- **PERF-FIX-005 (replay tool): ACCEPT WITH FIX REQUIRED** — per-cell `replay_cell` is correct, but
  `iter_promoted_cells` under-enumerates (1 vs the real book). Use symbol-level enumeration.
- **`pm_metrics.py` canonical contract: ACCEPT** — `calmar_ratio` preserved as alias (no gate break),
  explicit `return_drawdown_ratio`/`calmar_ratio_annualized`/`basis`/`metric_contract_version`, canonical
  PF inf sentinel, NaN/inf-safe. Definitive winner-selection-preservation check = run `shadow_selection_audit`.

## UPDATE 2026-06-20 — full re-optimized book replayed: 117 promoted winners at $10k
117 logical promoted winners (21 PROMOTED + 3 PROMOTE_SMALL), replayed clean at $10k, 0 errors.
- **79/117 (68%) profitable, 38/117 losing. Median PF 1.10. Median net +$1,751.**
- **The aggregate "+$4.53M" is a COMPOUNDING MIRAGE, not real.** It is driven by a handful of M5/M15
  high-trade-count cells with physically implausible returns: XAUUSD M15 EMAPullback **+14,940%** (6,422
  trades), BTCUSD M5 DayOfWeekBias **+3,013%**, US100 M5 DayOfWeekBias **+1,851%**. These are the F110
  artifact — the backtest compounds 1%-risk sizing with no live cap / min-lot / liquidity / slippage limit,
  so equity explodes over thousands of trades. They will NOT reproduce live. Honest center = median PF 1.10.
- **Catastrophic losers in the promoted book** (near-wipeouts that should never be promoted): ETHUSD M5
  DayOfWeekBias −94.9% (DD **99.5%**), EURJPY M30 ZScoreVWAP −91.6% (DD 91.7%), USDJPY M30 KeltnerFade
  −67.8% (DD 70%), ETHUSD H4 TimeOfDay −51.6% (DD 57.7%).
- **DayOfWeekBias (CC-AB-2) confirmed overfit noise** — it appears in BOTH the explosive-gain list and the
  −95% wipeout list, i.e. the optimizer fits whatever weekday/direction worked in-sample.
- **Genuine-edge subset (~8 cells, plausible):** H4/M30 with PF≥1.3, DD<18%, few-hundred trades — e.g.
  XAUUSD H4 EMAPullback (PF 1.74, DD 4.9%), TESLA H4 Darvas (PF 1.9, DD 8.4%), AIG H4 DoubleTopBottom
  (PF 1.63), ALIBABA H4 Choppiness (PF 1.64), JP225 H4 EMAPullback (PF 1.44).

**Verdict:** more winners, not better quality. The book mixes (a) compounding mirages, (b) ~60 marginal
PF≈1.0–1.3 cells, and (c) 38 losers incl. near-wipeouts. Gate fixes are now clearly essential:
PF≥1.0 floor (cuts 38 losers), trade-count/timeframe floor, a **max-DD cap** (cut the 40–99% DD cells), and
a **realistic-cost/compounding sanity check** on high-frequency M5/M15 cells before believing their returns.
Trade, at most, only the validated low-DD H4 subset — not the 117-cell book.

## UPDATE 2026-06-18 ~21:35 — ledger re-optimized at $10k; 7 promoted winners replayed
Config now canonical (`initial_capital=10000`, `discovery_risk_per_trade_pct=1.0`). The fixed
`iter_promoted_cells` now finds **7** logical promoted winners. $10k replay
(`promoted_winner_replay_all_10000.csv`):

| Symbol/TF/Strategy | Net$ | PF | Trades | MaxDD% |
|---|---:|---:|---:|---:|
| AUDUSD H4 BollingerBounce | +349 | 1.04 | 174 | 10.4 |
| EURUSD D1 VWAPTrendContinuation | +867 | 1.07 | 202 | 11.5 |
| EURUSD M30 VolumeSpikeMomentum | +517 | 1.03 | 313 | 18.2 |
| FORD H4 DoubleEMATrend | -2318 | 0.66 | 166 | 24.9 |
| USDJPY H1 VolumeClimaxReversal | +396 | 1.04 | 215 | 14.0 |
| USDJPY H4 ThreeSoldiersCrows | +3213 | 1.53 | 162 | 4.8 |
| USDJPY M30 KeltnerFade | -6784 | 0.74 | 748 | 70.4 |

- **Aggregate (equal $10k each): −$3,761 — the book loses.** Sunk by FORD (−23%) and the catastrophic
  USDJPY M30 KeltnerFade (−68%, 70% DD).
- **AUDUSD confirmed a thin-sample artifact:** 16-trade val PF 2.69 → 1.04 over 174 trades (same pattern as FORD 1.77→0.66).
- **Only 1 of 7 is clearly tradeable:** USDJPY H4 ThreeSoldiersCrows (PF 1.53, +32%, 4.8% DD). 4 are marginal (PF 1.03–1.07); 2 are losers.
- FORD still carries `artifact disc_risk=4.0` ≠ config 1.0 → it will still trigger `winner_ledger_artifact_drift` and block `--trade`.
- The PF≥1.0 + trade-count-floor gate fixes remain UNAPPLIED — which is why a −68% strategy was promoted.

## ROOT-CAUSE UPDATE — the promotion gate admits net losers (NEW, concrete, fixable)
The 2026-06-18 run is **COMPLETE** (all 100 symbols `optimized_at=2026-06-18`, all 2400 leaves evaluated,
`optimization_summary.csv` written) — not mid-reoptimization. It validated **4** candidates; the promotion
gate then ran:
- `BTCETH D1 EMAPullback` → **BLOCKED** ("CHOP profitability failed PF=0.63, ret −10.95%")
- `JP225 M30 RSITrendFilteredMR` → **BLOCKED** ("profit_factor 0.58 < 0.60")
- `ATT H4 VarianceRatioTrend` → **BLOCKED** ("profit_factor 0.39 < 0.60")
- `FORD H4 DoubleEMATrend` → **PROMOTED** (reasons=[])

**Two concrete promotion-logic flaws let a loser through:**
1. **Profitability floor is below breakeven.** The historical-audit block uses
   `historical_audit_min_profit_factor = 0.60` — but PF<1.0 *loses money*. So strategies with
   `0.60 ≤ PF < 1.0` are net losers yet are NOT blocked. FORD (PF 0.66) passed for exactly this reason.
2. **Promotion on a 16-trade validation sample.** `regime_min_val_profit_factor = 1.05` with
   `fx_val_min_trades = 15` let FORD promote on 16 trades at PF 1.77, which inverts to PF 0.66 over 166 trades.

**Two high-value, evidence-backed fixes (would have left the book empty this cycle — the correct outcome):**
- Raise the profitability floor to **PF ≥ 1.0 + margin** (e.g. ≥1.1) so net-losing strategies are blocked. FORD → blocked.
- Raise the **validation trade-count floor** (timeframe-aware, per Q4) so thin-sample optimism cannot drive promotion.

## Next decisive action
Re-run a clean optimization **at the canonical $10k** (`initial_capital=10000`, not the $20 small-balance
config, which the F108/F110 audit shows distorts sizing via the min-lot floor), with the PF≥1.0 and
trade-count-floor gate fixes, then replay ALL promoted winners (corrected enumerator) + run
`shadow_selection_audit` before any live use. The current single winner (FORD) must not be traded.
