# Strategy Surface Coverage Audit

Date: 2026-05-30

This audit tracks which strategy families are represented in FxPM after the
Package H and Package I expansions, and which families still need candidate
implementations or pipeline support.

## Current Coverage

Registered strategies: 66

- Trend following: 22
- Mean reversion: 26
- Breakout/momentum: 18

Covered families:

- Moving-average trend and pullback: EMA crossover, EMA ribbon, DEMA, KAMA,
  VIDYA, Hull MA, TRIX, Parabolic SAR, EMA pullback.
- Directional-strength trend: ADX, DI spread, Vortex, Aroon, RVI.
- Volatility/channel continuation: Keltner breakout, Bollinger band walk,
  Supertrend, Donchian, ATR percentile breakout.
- Compression/breakout: squeeze, choppiness compression, narrow range,
  inside bar, opening range, doji breakout.
- Session/value behavior: session VWAP reversion, VWAP trend continuation,
  gap continuation.
- Oscillator mean reversion: RSI, Stochastic, StochRSI, CCI, Williams %R,
  Fisher transform, ROC exhaustion.
- Pattern/candlestick reversal: pin bar, engulfing, morning/evening star,
  piercing/dark cloud, harami, tweezer, three-bar reversal, turtle soup.
- Pattern/candlestick momentum: marubozu momentum, three soldiers/crows.
- Volume-aware behavior: OBV divergence, volume spike momentum, VWAP families.
- Regime-aware behavior: regime transition breakout and regime-bucket optimizer.

## Still Underrepresented

These are good candidates for future expansion, assuming each is implemented as
a full strategy and must earn promotion through the same honest-cost pipeline.

### Chart/Market-Structure Patterns

Candidate package:

- Double top / double bottom reclaim or breakdown.
- Head-and-shoulders / inverse head-and-shoulders neckline break.
- Flag / pennant continuation after impulse and low-volatility pullback.
- Triangle/wedge compression breakout.
- Cup-and-handle style continuation for indices/equities.

Why it matters:

- These patterns are common discretionary/traditional setups and overlap well
  with equities/indices.
- They should be implemented conservatively with swing-point confirmation and
  no future-bar pattern completion.

### Gap Fade And Session Reversal

Candidate package:

- Gap fade after failed continuation.
- Opening-drive failure reversal.
- Previous-day high/low rejection.
- Prior session close/VWAP magnet reversion.

Why it matters:

- Especially relevant for equities and index CFDs.
- Needs careful session handling because FX/crypto have different session
  structure from shares/indices.

### Multi-Timeframe Confirmation

Candidate package:

- Lower-timeframe trigger aligned to higher-timeframe trend or volatility state.
- Higher-timeframe breakout with lower-timeframe pullback entry.

Pipeline note:

- The current strategy interface receives one feature frame at a time. True
  multi-timeframe strategies need either pre-merged higher-timeframe features
  or a strategy context object. Single-frame approximations can be added now,
  but proper MTF should be a pipeline feature, not a hack inside strategies.

### Relative/Cross-Sectional Strategies

Candidate package:

- Relative strength rotation for equities/indices.
- Pair spread mean reversion.
- Sector/index relative momentum.

Pipeline note:

- These require multi-symbol data access during signal generation and portfolio
  selection. They should not be forced into the current single-symbol strategy
  API without a deliberate architecture change.

### Volume/Profile/Flow Strategies

Candidate package:

- Chaikin Money Flow / Money Flow Index confirmation.
- Anchored VWAP from swing/session/event anchors.
- Volume profile value-area break/fade.
- Accumulation/distribution divergence.

Pipeline note:

- FX tick volume is only a proxy; equities/shares may provide more meaningful
  volume. Asset-class-aware feature handling will matter here.

### Carry/Swap And Longer-Horizon Holding

Candidate package:

- Swap-positive trend carry.
- Low-volatility carry continuation.
- Carry-filtered breakout/pullback.

Pipeline note:

- Broker swap is now captured in specs and modeled. This family is plausible,
  but it should be evaluated mostly on H4/D1 and path-dependent drawdown, not
  intraday win rate.

### Explicitly Excluded: ML/Meta-Model Selection

ML/meta-model selection is not a target for this codebase right now.

Reason:

- The optimizer, scorer, validation path, and winner ledger already behave like
  a pseudo-ML selection system over a large strategy/configuration surface.
- Adding a learned model on top would increase maintenance burden and leakage
  risk before the simpler deterministic candidate families have been fully
  exhausted.
- The current priority is broader deterministic strategy coverage plus strict
  evidence gating, not another modeling layer.

## Expansion Rule

More strategies are acceptable when all of the following are true:

- Full implementation: registered class, defaults, parameter grid, signal
  contract, stop compatibility, no lookahead.
- Tests: signal validity, truncation/no-lookahead, grid/default consistency,
  registry/category coverage.
- Evidence: at least a small conservative-cost prospect screen showing the
  family can produce tradeful profitable cases before it is kept as a serious
  candidate.
- Promotion: only full re-optimization under the current broker-cost artifact
  contract can move a strategy into the winner ledger.

## Next Best Additions

Recommended next packages:

1. Package J: chart/market-structure, session-fade, and money-flow strategies.
2. Package K: carry/swap-aware deterministic strategies.
3. Package L: MTF pipeline support before true multi-timeframe strategies.
4. Package M: cross-sectional/pairs architecture for equities expansion.
