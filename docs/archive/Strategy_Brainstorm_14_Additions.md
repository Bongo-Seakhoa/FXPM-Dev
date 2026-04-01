# PM Strategy Expansion: 28 → 42 — Brainstorm Analysis

## Current Inventory Snapshot

**Trend Following (10):** EMACrossover, Supertrend, MACDTrend, ADXTrend, Ichimoku, HullMATrend, EMARibbonADX, AroonTrend, ADXDIStrength, KeltnerPullback

**Mean Reversion (11):** RSIExtremes, BollingerBounce, ZScoreMR, StochasticReversal, CCIReversal, WilliamsR, RSITrendFilteredMR, StochRSITrendGate, VWAPDeviationReversion, FisherTransformMR, ZScoreVWAPReversion

**Breakout/Momentum (7):** DonchianBreakout, VolatilityBreakout, MomentumBurst, SqueezeBreakout, KeltnerBreakout, PivotBreakout, MACDHistogramMomentum

---

## Part 1: Critique of the Wayne Free AI Suggestions

I've reviewed all 14 suggestions. Here's my honest assessment — what's genuinely good, what has problems, and what is operationally a duplicate dressed up in different language.

### Strong Picks (Would Keep)

**#1 Turtle Soup Reversal** — Legitimate. This is a genuine anti-breakout that directly exploits failure of your existing Donchian edge. Different alpha source, high win-rate stabilizer, fills the "trap/failure" gap that's completely absent from your pool. **Agree.**

**#4 Inside Bar Breakout** — Legitimate. Pure OHLC candle structure, no indicator dependency. Compression → expansion via single bar geometry is genuinely different from your squeeze logic (which uses BB inside KC). Clean invalidation, good trade frequency. **Agree.**

**#5 NR7/NRx Expansion Breakout** — Legitimate. Bar-range-based compression is structurally different from band-based squeeze. NRx fires in different spots. Good complement to SqueezeBreakout without duplicating the trigger mechanism. **Agree.**

### Problematic Picks

**#3 Failed ORB Fade, #7 Asia Range Breakout, #8 ORB Breakout, #9 Session Mean Reversion** — All four of these are session-time-dependent strategies. They require parsing timestamps from your OHLCV bars to define "session boundaries" (London open, Asia box, NY open). This is absolutely doable in your architecture since timestamps ARE available, but the other model didn't acknowledge this as an implementation consideration at all. More importantly, **four of fourteen** being session-dependent is excessive concentration in a single structural dependency. If your MT5 timestamps are in server time and you misconfigure the session boundary by even 1 bar, all four break simultaneously. I'd take one session-based strategy, not four.

**#10 Trend Pullback + Momentum Reset Continuation** — This has meaningful overlap with your existing **KeltnerPullbackStrategy**, which already does trend-direction pullback → continuation with EMA slope + Keltner band pullback + confirmation close. The Wayne model says it "explicitly requires reset + continuation trigger, which changes trade timing" — but that's exactly what Keltner Pullback does with its slope filter + band touch + bounce confirmation. The timing IS different, but the operational concept is the same: "trend exists, wait for dip, enter on bounce." **Near-duplicate.**

**#12 Swing Break + Retest Continuation** — This is essentially your existing **PivotBreakoutStrategy** repackaged. Your PivotBreakout already does: rolling swing high/low breakout → wait for retest within confirm_window → confirm price closes back in trend direction. The Wayne model says "swing/fractal structure behaves differently and adapts across markets" — but your pivot logic already uses rolling highs/lows, which ARE swing structure. **Operational duplicate.**

**#14 Bollinger Bandwidth Expansion** — Too close to **SqueezeBreakoutStrategy**. Your Squeeze already measures BB inside KC (which is a bandwidth compression measurement) and trades the expansion. Bandwidth as a standalone trigger is just the same edge with one fewer filter. The Wayne model admits "Even if you have squeeze, bandwidth-based triggers can fire differently" — but "can fire differently" is not "adds a different alpha source." **Near-duplicate.**

**#13 ATR Contraction → Expansion** — Conceptually valid as a standalone volatility regime detector, but has overlap with Squeeze and VolatilityBreakout. I'd keep this one but with a different implementation angle (ATR percentile rank rather than simple contraction).

### Weak Picks

**#2 Liquidity Sweep + Reclaim** — In pure OHLCV, a "liquidity sweep" is just a wick that exceeds a prior level then closes back inside. The term "liquidity sweep" borrows from order flow / tape reading, but without order book data, you're just detecting wick rejections at levels. It's implementable and the concept is valid, but **it's better described as what it actually is: a wick rejection pattern at key levels** rather than dressing it in smart money terminology that implies data you don't have.

**#6 2-Bar Reversal at Level** — Valid but thin. A two-bar reversal (down bar followed by up bar, or vice versa) at a significant level is just a simplified engulfing pattern. I'd rather go with a proper engulfing/pin bar detection that has more structural robustness.

### Summary Score

| # | Strategy | Verdict | Reason |
|---|----------|---------|--------|
| 1 | Turtle Soup | ✅ Keep | Genuine anti-breakout, fills gap |
| 2 | Liquidity Sweep | ⚠️ Rename | Valid concept, but it's a wick rejection, not "liquidity sweep" |
| 3 | Failed ORB Fade | ⚠️ Session risk | Needs timestamp parsing, 4/14 session-dependent is too many |
| 4 | Inside Bar | ✅ Keep | Clean OHLC structure, genuinely different |
| 5 | NR7/NRx | ✅ Keep | Different compression source than Squeeze |
| 6 | 2-Bar Reversal | ⚠️ Thin | Simplified engulfing — better to do proper candle patterns |
| 7 | Asia Range | ⚠️ Session risk | Same session-dependency concern as #3 |
| 8 | ORB Breakout | ⚠️ Session risk | Same session-dependency concern as #3 |
| 9 | Session MR | ⚠️ Session risk | Same session-dependency concern as #3 |
| 10 | Trend Pullback Reset | ❌ Near-dup | KeltnerPullback already does this |
| 11 | First Pullback After TC | ⚠️ Complex | Requires regime flip detection, coupling |
| 12 | Swing Break + Retest | ❌ Duplicate | PivotBreakout already does this |
| 13 | ATR Contraction→Expansion | ⚠️ Overlap | Overlap with Squeeze, but salvageable with different framing |
| 14 | BB Bandwidth Expansion | ❌ Near-dup | SqueezeBreakout already captures this |

**Result: 3 clean keeps, 3 duplicates/near-duplicates, 8 conditional/problematic.**

---

## Part 2: Identified Gaps in the Current Pool

Before proposing strategies, here's what's ACTUALLY missing — grounded in the code I reviewed:

### Gap 1: Zero Divergence Logic
You have 11 mean reversion strategies and not a single one uses divergence. RSI divergence, MACD divergence, and OBV divergence are among the most empirically reliable reversal signals in FX. Every one of your MR strategies triggers on "indicator crosses threshold" — none triggers on "price action disagrees with indicator." This is a major structural gap.

### Gap 2: Zero Price Action / Candle Pattern Logic
All 28 strategies are indicator-math-based. Not one uses candle body/wick geometry (engulfing bars, pin bars, inside bars). These patterns are OHLC-native, require zero additional computation, and have different autocorrelation properties than indicator signals.

### Gap 3: Volume Is Nearly Unused
You have a Volume column. Only VWAP-based strategies touch it. OBV (On-Balance Volume), volume spike detection, and volume-price divergence are completely absent. This is an entire data dimension that's essentially wasted.

### Gap 4: No Anti-Breakout / Trap Logic
All breakout strategies are continuation-based (break level → ride direction). Zero strategies exploit *failed* breakouts. In FX, false breakouts are arguably more common than real ones, especially on lower timeframes. This is the gap the Wayne model correctly identified.

### Gap 5: No Momentum Reversal (only Momentum Continuation)
MomentumBurst and VolatilityBreakout both trade momentum continuation. The opposite — momentum exhaustion reversal — is absent. ROC at extreme percentiles fading back to mean is a different edge.

### Gap 6: Breakout/Momentum Category Is Underweight
You have 10 trend + 11 MR + 7 breakout. The breakout category has the fewest strategies but covers both BREAKOUT and transitional regimes. Adding even 3–4 breakout strategies would meaningfully improve coverage in the regimes where they're the only viable family.

### Gap 7: No Adaptive / Multi-Period Logic
Every strategy uses fixed lookback periods. An adaptive period strategy (e.g., using Kaufman's efficiency ratio to dynamically adjust) would activate in conditions where fixed-period strategies fail.

---

## Part 3: My Proposed 14 Strategies

Organized by the role each one plays in the portfolio. Each strategy is described with: what it does, why it wins, which regimes it covers, why it's not a duplicate, and what portfolio quality metric it improves.

---

### CATEGORY A: Divergence (Filling the Biggest Gap)

#### 1. RSIDivergenceStrategy
**Category:** Mean Reversion

**What it does:** Detects bullish divergence (price makes lower low, RSI makes higher low) and bearish divergence (price makes higher high, RSI makes lower high). Uses swing point detection on both price and RSI over a configurable lookback. Triggers on the confirmation candle after the divergence completes.

**Why it wins:** Divergence is one of the most well-documented reversal signals across all markets. It captures momentum exhaustion that oscillator *threshold* strategies (your entire MR pool) fundamentally cannot detect. RSI at 35 with bullish divergence is a completely different setup than RSI at 35 without it — your existing RSIExtremes can't distinguish between the two.

**Regime coverage:** RANGE (classic mean reversion), late TREND (exhaustion), CHOP (picking turning points).

**Not a duplicate because:** None of your 11 MR strategies use divergence logic. They all use "indicator crosses level" triggers. This uses "indicator direction disagrees with price direction."

**Quality impact:** High win-rate stabilizer. Reduces drawdown during extended trends where threshold-based MR strategies get stopped out repeatedly. Empirically, divergence entries have higher PF than raw oscillator threshold entries because they're conditioned on momentum deceleration.

---

#### 2. MACDDivergenceStrategy
**Category:** Mean Reversion

**What it does:** Detects divergence between price swing points and MACD histogram swing points. Bullish: price makes lower low but MACD histogram makes higher low. Bearish: inverse. Triggers on the histogram bar that confirms the higher low / lower high.

**Why it wins:** MACD histogram divergence is a separate signal from MACD crossover (your MACDTrendStrategy) and MACD zero-line cross (your MACDHistogramMomentumStrategy). Same indicator family, completely different alpha source — you're reading the *shape* of momentum, not the *level* or *cross*.

**Regime coverage:** Late TREND exhaustion, RANGE boundaries.

**Not a duplicate because:** MACDTrend = crossover signal. MACDHistogramMomentum = zero-line cross. This = histogram shape divergence from price. Three distinct signal types from one indicator family.

**Quality impact:** Catches trend exhaustion that crossover signals miss. Improves regime-transition capture.

---

### CATEGORY B: Price Action / Candle Structure (New Signal Source Entirely)

#### 3. PinBarReversalStrategy
**Category:** Mean Reversion

**What it does:** Detects pin bars (hammer/shooting star patterns) by measuring wick-to-body ratio. Long: lower wick ≥ 2× body size, small upper wick, near a support level (lower BB or rolling low). Short: upper wick ≥ 2× body, near resistance. Configurable wick ratio threshold and level proximity filter (using ATR-scaled distance to BB band or Donchian extreme).

**Why it wins:** Pin bars are the single most researched candlestick pattern in quantitative FX studies. They indicate rejection at a level — the market tried to go there, was rejected, and closed away from it. This is pure OHLC geometry with zero indicator lag.

**Regime coverage:** RANGE (at boundaries), CHOP (picking direction after rejection), late TREND (exhaustion wicks).

**Not a duplicate because:** No existing strategy examines candle body/wick geometry. All use computed indicators.

**Quality impact:** High win-rate stabilizer with tight stops (stop behind the wick). Improves Sharpe by adding a signal source that has near-zero correlation with your oscillator-based entries.

---

#### 4. EngulfingPatternStrategy
**Category:** Breakout / Mean Reversion (depends on location)

**What it does:** Detects bullish engulfing (current bar's body fully engulfs prior bar's body, with current close > prior open and current open ≤ prior close) and bearish engulfing (inverse). Filters by location: must be near a BB band, Donchian extreme, or significant swing level (rolling N-bar high/low).

**Why it wins:** Engulfing bars represent a decisive shift in control. They're the candle-structure equivalent of a one-bar momentum reversal. The location filter ensures you're not trading engulfing patterns in the middle of nowhere (which is where they fail).

**Regime coverage:** RANGE (at edges), BREAKOUT (momentum ignition), CHOP (direction establishment).

**Not a duplicate because:** Same reasoning as pin bar — no candle geometry strategies exist.

**Quality impact:** Good trade frequency (engulfing patterns are common), moderate win rate with strong expectancy when location-filtered. Diversifies the signal generation layer.

---

#### 5. InsideBarBreakoutStrategy
**Category:** Breakout

**What it does:** Detects inside bars (current bar's high < prior bar's high AND current bar's low > prior bar's low). Entry: breakout of the mother bar's range (close above mother high = long, close below mother low = short) on a subsequent bar. Optional: require 2+ inside bars for tighter compression.

**Why it wins:** Inside bars are the purest OHLC compression signal. No indicators, no lookback periods, no parameters to overfit. Compression produces expansion — this is one of the most robust patterns in all of price action trading.

**Regime coverage:** BREAKOUT (primary), TREND continuation (inside bar in trend direction).

**Not a duplicate because:** No candle structure strategies exist. Different from SqueezeBreakout (which uses BB/KC channels) and NarrowRangeBreakout (which uses multi-bar range comparison).

**Quality impact:** Clean invalidation (stop behind mother bar), good R:R, robust across timeframes. Low overfit risk due to minimal parameters.

---

### CATEGORY C: Volume-Based (Exploiting Unused Data)

#### 6. OBVDivergenceStrategy
**Category:** Trend Following / Mean Reversion (context-dependent)

**What it does:** Computes On-Balance Volume (cumulative volume added on up-closes, subtracted on down-closes). Detects divergence between OBV trend and price trend. Bullish: price makes lower low but OBV makes higher low (accumulation). Bearish: price makes higher high but OBV makes lower high (distribution). Can also be used as a trend confirmation filter (OBV slope aligns with price slope).

**Why it wins:** Volume is the only dimension of market data you have that ISN'T price. OBV divergence tells you whether the volume participants (the "money") agree with the price move. When they disagree, the price move is likely to reverse. This is a fundamentally different information source than any price-based indicator.

**Regime coverage:** Late TREND (detecting distribution/accumulation), RANGE (confirming which side has volume support), BREAKOUT (confirming volume participation in the breakout).

**Not a duplicate because:** Only VWAP strategies use volume, and they use it as a weighted-average anchor, not as a trend/divergence signal. OBV is a different analytical framework.

**Quality impact:** Adds a genuinely orthogonal signal source. When all your price-based indicators are in conflict, volume divergence can be the tiebreaker.

---

#### 7. VolumeSpikeMomentumStrategy
**Category:** Breakout / Momentum

**What it does:** Detects bars where volume exceeds N× the rolling average volume (e.g., 2× the 20-bar rolling mean). Then applies a directional filter: if the volume spike bar closes in the upper 30% of its range = long, lower 30% = short. Optional: require the spike bar to also exceed an ATR threshold for price movement.

**Why it wins:** Volume spikes indicate institutional participation or news-driven flow. A large-volume bar with strong directional close is one of the cleanest "smart money just entered" signals available from OHLCV data. Most of your breakout strategies use price structure only — this one uses volume structure.

**Regime coverage:** BREAKOUT (primary — volume confirms the break), TREND (continuation spikes).

**Not a duplicate because:** No existing strategy triggers on volume magnitude. All triggers are price-based.

**Quality impact:** Strong compounder on genuine breakout days. Filters out "low-conviction" breakouts where your price-only breakout strategies get stopped out.

---

### CATEGORY D: Anti-Breakout / Failure Exploitation

#### 8. TurtleSoupReversalStrategy
**Category:** Mean Reversion

**What it does:** Monitors for failed Donchian breakouts. When price breaks above the N-period high (Donchian upper) but then closes back below it within M bars, that's a bull trap — enter short. Inverse for bear traps. The "reclaim" requirement (close back inside the prior range) is the key filter.

**Why it wins:** Your DonchianBreakout profits when breakouts follow through. This strategy profits when they don't. In FX, false breakouts are extremely common — particularly on the 20-period channel that Donchian typically uses. Turtle Soup is one of the most well-known counter-trend setups, named by Linda Raschke.

**Regime coverage:** RANGE (primary — range boundaries produce the traps), late TREND (exhaustion probes), CHOP (both sides get trapped).

**Not a duplicate because:** It's the literal opposite trade of DonchianBreakout. When Donchian succeeds, this would have been stopped out. When Donchian fails, this profits.

**Quality impact:** Directly compensates for DonchianBreakout losses. Anti-correlated returns reduce portfolio drawdown during range-bound markets where breakout strategies churn.

---

#### 9. KeltnerFadeStrategy
**Category:** Mean Reversion

**What it does:** Fades touches of the outer Keltner Channel band back toward the midline. When price touches/exceeds the upper KC band and then the next bar closes back inside, enter short targeting the KC midline (EMA). Inverse for lower band. Optionally gated by ADX < threshold (range filter).

**Why it wins:** You have KeltnerBreakout (continuation THROUGH the band) and KeltnerPullback (pullback TO the mid for continuation). This is the THIRD Keltner trade: fade FROM the outer band BACK to the mid. Same channel, opposite trade to KeltnerBreakout. In range regimes, band fades are high-probability because the channel acts as containment.

**Regime coverage:** RANGE (primary), CHOP (fading whipsaws).

**Not a duplicate because:** KeltnerBreakout = continuation through band. KeltnerPullback = pullback to mid for trend continuation. This = fade from band back to mid (mean reversion). Three distinct edges from one channel.

**Quality impact:** High win-rate in range regimes. Directly offsets KeltnerBreakout losses in non-trending conditions.

---

### CATEGORY E: Compression / Expansion (Structural)

#### 10. NarrowRangeBreakoutStrategy
**Category:** Breakout

**What it does:** Identifies NR4 or NR7 bars (current bar has the narrowest High-Low range of the last 4 or 7 bars). On the next bar, takes a breakout in the direction of the close relative to the NR bar's midpoint (or waits for a break of the NR bar's high/low). Configurable NR lookback (4, 7, 10).

**Why it wins:** Narrow range bars indicate market indecision and compression. The statistical probability of a large move following an NR7 bar is well-documented (Toby Crabel's work on volatility patterns). Different from Squeeze because NR is purely bar-range-based — no bands, no channels, just the literal range of recent bars.

**Regime coverage:** BREAKOUT (primary), regime transitions (compression before expansion).

**Not a duplicate because:** SqueezeBreakout uses BB inside KC. This uses "smallest bar range in N bars." Different measurement, different trigger timing, different conditions.

**Quality impact:** Tight stops (behind the NR bar), good R:R on expansion moves. Fires in the early stage of breakouts, often before channel-based signals trigger.

---

### CATEGORY F: Momentum Reversal (Missing the Other Side of Momentum)

#### 11. ROCExhaustionReversalStrategy
**Category:** Mean Reversion

**What it does:** Computes Rate of Change (percentage price change over N periods). When ROC reaches an extreme percentile (top/bottom 5-10% of its own rolling distribution), and then crosses back inside, enter the reversal. Long: ROC was in the extreme negative zone and crosses above -threshold. Short: inverse.

**Why it wins:** Your MomentumBurstStrategy trades momentum continuation (when ROC exceeds a threshold). This trades momentum EXHAUSTION reversal (when ROC reaches an extreme and then fades). Same underlying metric, opposite trade logic. Momentum exhaustion at extreme ROC levels is one of the most reliable mean-reversion setups because it captures "extended too fast, must snap back."

**Regime coverage:** RANGE (snapback from overextension), late TREND (exhaustion fade), CHOP (fading short-term spikes).

**Not a duplicate because:** MomentumBurst = continuation when ROC > threshold. This = reversal when ROC reaches extreme and fades. Opposite trade.

**Quality impact:** High PF stabilizer. Catches snapbacks that all your trend and breakout strategies miss because they're designed to ride the move, not fade it.

---

### CATEGORY G: Trend Entry Refinement

#### 12. EMAPullbackContinuationStrategy
**Category:** Trend Following

**What it does:** Two-stage entry. Stage 1: Detect trend via EMA crossover (fast > slow = uptrend). Stage 2: Instead of entering on the cross itself (like EMACrossoverStrategy), wait for a pullback to the fast EMA. Enter long when price touches/dips below the fast EMA and then closes back above it, while slow EMA still confirms uptrend. Inverse for shorts.

**Why it wins:** EMA crossovers are your most basic trend signal, but entering ON the cross often means entering late into the initial thrust and then experiencing a pullback drawdown. This strategy accepts the same trend signal but waits for the first pullback to get a better entry. Better entries → tighter stops → higher PF → lower drawdown.

**Regime coverage:** TREND (primary — this only fires when a trend is established and pulls back).

**Not a duplicate because:** EMACrossover enters on the cross. KeltnerPullback uses Keltner bands for pullback level and has its own trend definition. This uses the fast EMA itself as the pullback level, and specifically conditions on a prior crossover. Different entry timing, different level logic.

**Quality impact:** Lower average entry drawdown than EMACrossover. Higher Sharpe because better entries mean fewer trades that immediately go against you.

---

#### 13. ParabolicSARTrendStrategy
**Category:** Trend Following

**What it does:** Computes the Parabolic SAR (acceleration factor system). Generates signals on SAR flip: when SAR moves from above price to below = long, below to above = short. Optional: filter by ADX > threshold to only trade strong trends.

**Why it wins:** Parabolic SAR is a completely different mathematical framework from everything in your trend pool. Your trend strategies use: crossovers (EMA, MACD, Ichimoku), direction changes (Hull MA, Supertrend), and strength measurements (ADX, Aroon). PSAR uses an acceleration factor that tightens with time — it's essentially a trailing stop system turned into a signal generator. The mathematical basis has zero overlap with MA/oscillator math.

**Regime coverage:** TREND (primary), BREAKOUT (catches early trends).

**Not a duplicate because:** None of your 10 trend strategies use acceleration factor / parabolic math. Closest analog is Supertrend, but Supertrend uses ATR bands with direction logic while PSAR uses a fundamentally different recursive acceleration formula.

**Quality impact:** Adds a mathematically independent trend signal. When your EMA/MACD/ADX cluster disagrees with PSAR, the system has more information about whether the trend is genuine.

---

### CATEGORY H: Volatility Regime Detection

#### 14. ATRPercentileBreakoutStrategy
**Category:** Breakout

**What it does:** Computes ATR's percentile rank within its own rolling history (e.g., where current ATR sits in the last 100 bars' ATR distribution). When ATR drops below the Nth percentile (compression), enter a pending signal. When ATR then rises above the Mth percentile (expansion), take the direction of the expansion bar's close.

**Why it wins:** Your SqueezeBreakout uses BB inside KC to detect compression — a binary state (squeeze on/off). This uses ATR's percentile rank, which is a continuous measure. It fires differently because: (1) it can detect compression even when BB is NOT inside KC, (2) it uses the rate of change of volatility, not a channel relationship. Think of it as measuring volatility's position within its own cycle, not its relationship to two different channels.

**Regime coverage:** BREAKOUT (primary), regime transitions.

**Not a duplicate because:** SqueezeBreakout = BB inside KC → first expansion bar. VolatilityBreakout = single bar exceeds ATR threshold. This = ATR drops to historically low level → expansion detected → directional entry. Different trigger mechanism, fires in different market spots.

**Quality impact:** Catches breakouts that Squeeze misses (when channels don't overlap but volatility is still historically low). Better at avoiding false signals in medium-volatility conditions where Squeeze is ambiguous.

---

## Part 4: Portfolio Impact Analysis

### New Category Distribution (post-addition)

| Category | Before | After | Added |
|----------|--------|-------|-------|
| Trend Following | 10 | 12 | +2 (EMAPullback, ParabolicSAR) |
| Mean Reversion | 11 | 17 | +6 (RSIDivergence, MACDDivergence, PinBar, KeltnerFade, TurtleSoup, ROCExhaustion) |
| Breakout/Momentum | 7 | 13 | +6 (Engulfing, InsideBar, NarrowRange, OBVDivergence, VolumeSpike, ATRPercentile) |

**Why the distribution skews MR and Breakout:**
- Trend has 10 strategies and the gap analysis shows it's the most well-covered category. Adding 2 refined entries (better timing) is sufficient.
- Mean Reversion has 11 strategies BUT they all use the same signal type (oscillator threshold crosses). Adding divergence, candle patterns, and fade strategies introduces genuinely new MR alpha sources, not more of the same.
- Breakout had only 7 strategies covering two regimes (BREAKOUT and partial TREND). This was the most underweight category. 6 additions bring it to near-parity with the others.

### New Alpha Sources Introduced

| Alpha Source | Strategies Using It | Currently in Pool? |
|--------------|--------------------|--------------------|
| Price-indicator divergence | RSIDivergence, MACDDivergence, OBVDivergence | ❌ No |
| Candle body/wick geometry | PinBar, Engulfing, InsideBar | ❌ No |
| Volume magnitude | VolumeSpike | ❌ No |
| Volume trend (OBV) | OBVDivergence | ❌ No |
| Failed breakout exploitation | TurtleSoup | ❌ No |
| Band fade (vs band break) | KeltnerFade | ❌ No |
| Bar range compression | NarrowRange | ❌ No |
| Momentum exhaustion reversal | ROCExhaustion | ❌ No |
| Acceleration factor (PSAR) | ParabolicSAR | ❌ No |
| Volatility percentile rank | ATRPercentile | ❌ No |

**All 14 strategies introduce signal types that do not currently exist in the pool.**

### Regime Coverage Improvement

| Regime | Current Coverage | Additions That Fire Here |
|--------|-----------------|-------------------------|
| TREND | Strong (10 strategies) | EMAPullback, ParabolicSAR, OBVDivergence (confirmation) |
| RANGE | Good MR but same-type | KeltnerFade, TurtleSoup, PinBar, ROCExhaustion, RSIDivergence, MACDDivergence |
| BREAKOUT | Weakest (7 strategies) | InsideBar, NarrowRange, VolumeSpike, ATRPercentile, Engulfing |
| CHOP | Worst (few strategies trade here) | PinBar, TurtleSoup, KeltnerFade, ROCExhaustion |

The biggest coverage improvement is in **CHOP**, where currently almost nothing trades. Anti-breakout fades (TurtleSoup), wick rejection (PinBar), and exhaustion reversals (ROCExhaustion) are the strategy types that CAN trade CHOP profitably because they explicitly exploit the failure patterns that chop creates.

### Expected Portfolio Quality Improvements

| Metric | How It Improves | Key Strategies Responsible |
|--------|----------------|--------------------------|
| **Lower Max DD** | Anti-correlated returns from fade strategies offset breakout losses | TurtleSoup, KeltnerFade |
| **Higher Profit Factor** | Better entries (EMAPullback) + new high-PF MR sources (divergence) | EMAPullback, RSIDivergence, MACDDivergence |
| **Better Recovery** | CHOP coverage means trades happen when previously idle | PinBar, TurtleSoup, ROCExhaustion |
| **Higher Sharpe** | Orthogonal signal sources reduce return correlation | All volume and candle strategies |
| **More Consistent Trade Volume** | Breakout strategies +6 fills gaps in trade opportunity | InsideBar, NarrowRange, VolumeSpike, ATRPercentile |
| **Lower Overfit Risk** | Candle patterns have minimal parameters | PinBar, Engulfing, InsideBar |

---

## Part 5: Comparison — My 14 vs Wayne Free AI 14

| Slot | Wayne Free AI | My Pick | Why I Differ (or Agree) |
|------|---------------|---------|------------------------|
| 1 | Turtle Soup Reversal | TurtleSoup Reversal | **Same.** Both agree this fills a critical gap. |
| 2 | Liquidity Sweep + Reclaim | PinBar Reversal | Same concept (wick rejection at level) but properly named and structured. Pin bar is more rigorous than "liquidity sweep" from OHLCV. |
| 3 | Failed ORB Fade | RSI Divergence | I take divergence over session-dependent logic. Divergence fills a bigger gap (zero divergence in pool vs already having some MR tools). |
| 4 | Inside Bar Breakout | Inside Bar Breakout | **Same.** Both agree. |
| 5 | NR7/NRx Expansion | NR7/NRx Expansion | **Same concept**, I implement it similarly. |
| 6 | 2-Bar Reversal at Level | Engulfing Pattern | 2BR is a simplified engulfing. I go full engulfing + location filter for more robustness. |
| 7 | Asia Range Breakout | MACD Divergence | I take a second divergence type over a second session-dependent strategy. Bigger alpha source gap. |
| 8 | ORB Breakout | OBV Divergence | Volume divergence exploits unused data. ORB adds a third session strategy. |
| 9 | Session Mean Reversion | Volume Spike Momentum | Volume spike uses unused data dimension. Session MR is a fourth session strategy. |
| 10 | Trend Pullback + Reset | EMA Pullback Continuation | Similar *intent* (better trend entry) but mine explicitly conditions on prior EMA cross and uses the fast EMA as pullback level, avoiding duplication with KeltnerPullback. |
| 11 | First Pullback After TC | Parabolic SAR Trend | PSAR adds a mathematically independent trend signal. FTP requires regime-flip coupling that adds fragility. |
| 12 | Swing Break + Retest | Keltner Fade | Wayne's #12 duplicates PivotBreakout. Keltner Fade is genuinely new (opposite trade from KeltnerBreakout). |
| 13 | ATR Contraction→Expansion | ATR Percentile Breakout | Same family, but my version uses percentile rank (continuous measure) vs simple contraction (binary). More nuanced, fires differently. |
| 14 | BB Bandwidth Expansion | ROC Exhaustion Reversal | Wayne's #14 duplicates Squeeze. ROC Exhaustion fills the "momentum reversal" gap — a genuinely missing edge. |

### Key Philosophical Differences

**Wayne's list** is heavy on structural/session-based breakout strategies (4 of 14 are session-dependent) and light on genuinely new alpha sources. It adds more strategies from existing families (more breakouts, more structure patterns) rather than introducing new signal types.

**My list** prioritizes introducing signal types that have ZERO representation in the current pool: divergence (3 strategies), candle geometry (3), volume-based (2), fade/anti-breakout (2), and new math frameworks (PSAR, ATR percentile). This maximizes the information content of the pool because each strategy reads the market through a different lens.

The practical difference: Wayne's additions would make the PM better at what it already does. My additions make it capable of things it currently cannot do at all.

---

## Part 6: Implementation Priority Order

If you want to implement in phases rather than all at once, here's the order I'd recommend based on expected impact per unit of effort:

**Phase 1 — Highest Impact, Easiest Implementation (do first):**
1. InsideBarBreakout — Minimal parameters, pure OHLC, fast to implement
2. PinBarReversal — Pure OHLC, straightforward geometry
3. TurtleSoupReversal — Simple logic: Donchian break + fail + reclaim
4. ParabolicSARTrend — Well-defined algorithm, many reference implementations

**Phase 2 — High Impact, Moderate Complexity:**
5. RSIDivergence — Requires swing point detection helper function
6. NarrowRangeBreakout — Simple bar comparison, easy to implement
7. VolumeSpikeMomentum — Simple volume ratio comparison
8. KeltnerFade — Reuse Keltner channel code from existing strategies

**Phase 3 — High Impact, More Complexity:**
9. EngulfingPattern — Needs body/wick decomposition + level filter
10. ROCExhaustionReversal — Needs percentile rank computation
11. ATRPercentileBreakout — Needs rolling percentile rank of ATR
12. MACDDivergence — Requires swing point detection on histogram
13. OBVDivergence — Requires OBV computation + swing point detection
14. EMAPullbackContinuation — Stateful (needs to track prior crossover)

---

*This document represents a brainstorm starting point. All strategies are designed to slot directly into the existing BaseStrategy → StrategyRegistry architecture with OHLCV + timestamp input and {-1, 0, 1} signal output, using the global ATR SL/TP grid.*
