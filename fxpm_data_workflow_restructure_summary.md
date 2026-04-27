# FXPM Data Workflow Restructure Summary

## Purpose of this document

This document summarises the reasoning, design shift, and implementation direction that came out of the FXPM data workflow discussion. It is intended to give a developer, technical reviewer, or project manager enough context to understand why the current workflow needs to change, what was wrong with the previous framing, and what the new intended FXPM structure should achieve.

The central goal is not to create an academically neat train, validation, and holdout split. The central goal is to make FXPM more profitable, more actionable, more accurate, more adaptive to current market behaviour, and better aligned with the live deployment window.

Efficiency and runtime optimisation matter, but they are secondary. They should support the core objective rather than govern it. The main objective is to improve the quality of the PM itself.

---

## Starting concern

The discussion began with a concern that the current data split may not be serving FXPM well. The initial idea was to use a middle holdout cut, where the earlier section of data and a later section of data would be used for training or strategy selection, while a middle section would be reserved for holdout evaluation. The purpose of that idea was to create a more balanced assessment process that included older historical context, recent relevance, and a clean block that was not directly involved in strategy selection.

A key caveat was identified immediately: if the training data is split into two disconnected time blocks, those blocks cannot simply be stitched together as if they were one continuous sequence. Doing that would allow trades, indicators, exits, trailing stops, and equity states to run across artificial seams, which would distort results. Any split-block evaluation would need to handle each section independently.

However, as the discussion evolved, the more important issue became clear: the problem was not simply about where to place a holdout. The deeper issue was that the current FXPM workflow may be using stale or less relevant data to decide which strategies are allowed to reach later optimisation.

---

## Key conceptual correction

A major correction in the discussion was moving away from a strict machine-learning framing.

FXPM is not primarily training a supervised model. It is performing strategy selection, candidate filtering, arithmetic optimisation, parameter search, and risk-management optimisation. Some concepts from machine learning still matter, especially overfitting and selection bias, but the workflow should not be forced into a classical train, validation, and test structure if that structure harms live-market relevance.

The better framing is:

> FXPM should select and optimise strategies from the data window that is most relevant to live deployment, while using older data as a stress or audit layer rather than allowing older data to dominate strategy selection.

This distinction is important because a blind validation process can appear clean but still be practically weak. If the first-stage candidate pool is created from older data, then the later validation or tuning stage can only work with candidates that already survived the older-data filter. A strategy that is highly relevant to the current market regime may never reach the optimisation stage if it was excluded too early.

This is the core structural issue that has likely weighed on FXPM for a long time.

---

## Current workflow problem

The previous or current workflow can be understood roughly as follows:

1. Download approximately 500,000 candles.
2. Use a large earlier portion of the dataset, roughly 80 percent, to identify strategy candidates.
3. Use the latest portion, roughly 20 percent to 30 percent, as the validation, tuning, or relevance check.
4. Assume that strategies selected from the earlier data remain the correct candidates for the latest live-relevant window.

The weakness is that the most recent market information does not control the strategy-selection pool strongly enough. The latest window can only tune or validate strategies that already passed the earlier filter. It cannot recover strategies that were excluded despite being more relevant to the current market structure.

This creates what can be called beam exclusion risk.

Beam exclusion risk means that the workflow may discard the best live-relevant strategy before the expensive optimisation step ever begins. Once that happens, later optimisation cannot fix the issue because the correct candidate is no longer available.

This is different from a normal overfitting problem. It is a live-relevance problem.

---

## Revised view of relevance versus validation

The important production question is not:

> Which strategy performed best across the largest historical dataset?

The more relevant FXPM question is:

> Which strategy is most likely to be profitable, actionable, accurate, and executable in the market window closest to live deployment?

This means old data should not be ignored, but it should also not be allowed to dominate the active strategy selection process. In FX, older data can be useful for stress testing, but it can also introduce legacy bias. More data is not automatically better if that data reflects market structures that are no longer dominant.

The new workflow should therefore prioritise recent market relevance while retaining older data as a secondary audit mechanism.

---

## Final proposed data structure

The new proposed structure is:

1. Download or retain the latest 300,000 base M5 bars.
2. Reserve the oldest 50,000 M5 bars as `historical_stress_audit` / `historical_audit_window`.
3. Use the newest 250,000 M5 bars as the active FXPM processing universe.

All 300,000 / 50,000 / 250,000 quantities in this workflow refer to base M5 bars. Higher timeframes should be sliced by timestamp boundaries derived from the base M5 data, not by independently taking 50,000 or 250,000 H1/H4/D1 candles.

The oldest 50,000 M5 bars are the oldest portion of the downloaded dataset. They should not govern current strategy selection. They should be used to detect serious collapse, fragility, unacceptable drawdown, or major structural weakness.

The newest 250,000 M5 bars are the live-relevant data universe. This is the data window that should drive candidate eligibility, strategy optimisation, risk-management optimisation, and final production selection.

The new data layout is therefore:

```text
Downloaded base data: 300,000 M5 bars

Oldest 50,000 M5 bars:
historical_stress_audit / historical_audit_window

Newest 250,000 M5 bars:
Active strategy selection and optimisation universe
```

---

## Stage 1 clarification

A very important correction is that Top-K does not primarily belong to Stage 1.

Stage 1 has sometimes been discussed using Top-K language, but that is not technically correct for the intended FXPM architecture. Stage 1 has a Top-K-like effect only in the loose sense that some strategies survive and others do not. However, Stage 1 itself should be understood as a criteria-based eligibility and filtering stage.

The Stage 1 criteria are already defined in the configuration document. These config-based criteria should determine whether a baseline strategy is allowed to pass into the later optimisation process.

Stage 1 should therefore be described as:

> A baseline strategy eligibility and viability stage governed by config-defined pass criteria.

It should not be described as:

> A Top-K optimisation layer.

This distinction matters because Stage 1 strategies are baseline strategies. They do not yet have hyperparameter tuning or refined risk-management optimisation. They are not final strategy forms. They are coarse candidates being assessed for whether they deserve expensive downstream optimisation.

Stage 1 should be permissive enough to preserve promising strategies. It should not overgovern the strategy space before optimisation has had a chance to work.

---

## What Stage 1 should do

Stage 1 should run baseline strategy configurations across the active 250,000 M5-bar universe and apply the predefined config criteria.

Its purpose is to answer:

> Which baseline strategies are viable enough to proceed into deeper optimisation?

Stage 1 should apply only the criteria that are already intended and defined in config, such as minimum viability, unacceptable drawdown, insufficient trade count, invalid outputs, execution incompatibility, or other established thresholds.

Stage 1 should not try to be the final judge of strategy quality. It should not attempt to prove full robustness. It should not impose additional chronological segmentation unless that is explicitly required by config and aligned with the architecture.

The goal of Stage 1 is candidate preservation, not final selection.

---

## Why Stage 1 should not be over-segmented

A segmented A/B/C internal test was considered, but it is not necessary for Stage 1 and may be counterproductive.

The reason is that Stage 1 deals with untuned baseline strategies. If the active 250,000 M5-bar universe is split into smaller chronological segments, then each baseline strategy is judged on smaller windows before it has been optimised. That can prematurely reject strategies that could perform very well after Stage 2 parameter optimisation.

Segmentation also reintroduces the same relevance problem in a different form. Segment A is older than Segment B, and Segment B is older than Segment C. Once these internal segments begin governing the candidate pool, older data again starts influencing which strategies survive. That works against the purpose of shifting FXPM toward a more recent and adaptive data universe.

For Stage 1, a simpler full-window aggregation over the active 250,000 M5 bars is more appropriate.

Stage 1 should therefore not use A/B/C segmented scoring as a governing selection mechanism unless there is a very specific implementation reason for doing so.

---

## Stage 2 clarification

Top-K primarily belongs to Stage 2.

Stage 2 is the high-compute optimisation layer. This is where the system should perform deeper algorithmic optimisation, parameter search, risk-management selection, and final ranking among viable candidates.

Stage 2 receives strategies that have passed Stage 1 config-based eligibility. It then applies the expensive optimisation process to determine which variants are genuinely strongest.

This is the stage where Top-K is conceptually appropriate because the optimisation process may generate many possible parameter combinations, potentially tens of thousands or hundreds of thousands of variants depending on the strategy and risk-management space.

Stage 2 should therefore be described as:

> The Top-K and algorithmic optimisation layer that searches across parameter, strategy, and risk-management combinations for the best production-ready candidates.

---

## What Stage 2 should do

Stage 2 should optimise the strategies that passed Stage 1 across the active 250,000 M5-bar universe, or across the intended Stage 2 optimisation slice within that active universe if the implementation has a specific split.

The key responsibilities of Stage 2 are:

1. Run the expensive parameter search.
2. Optimise strategy settings.
3. Optimise or select risk-management profiles.
4. Rank the resulting candidate variants.
5. Select the best production-ready strategy per symbol, timeframe, routine, and market regime.
6. Ensure that the final selected strategy is aligned with live execution rules.

Stage 2 is where the real competitive selection happens.

This is also where it makes sense to speak about Top-K candidates, winning iterations, final rankings, and optimised strategy variants.

---

## Risk-management integration

A new important element is the inclusion of risk-management strategy selection.

The revised FXPM should not only identify the best trading strategy per symbol, timeframe, and market regime. It should also identify the best risk-management approach for strategies that pass through the Stage 1 criteria and enter Stage 2 optimisation.

The intended selection target therefore becomes:

> Best strategy plus best risk-management profile per symbol, timeframe, routine, and market regime.

This means Stage 2 should not treat risk management as a fixed afterthought. Risk management should be part of the optimisation surface where appropriate.

The PM should not merely ask, “Which entry strategy is best?” It should ask, “Which complete trade-management package is best for this symbol, timeframe, routine, and regime?”

---

## Historical stress audit role in the new workflow

The oldest 50,000 M5 bars should be treated as `historical_stress_audit` / `historical_audit_window`.

This audit window should not become the primary selector. Its job is to detect severe fragility, not to force the final PM to prefer historically broad but currently weaker strategies.

The historical stress audit should answer questions such as:

1. Does the selected strategy completely collapse on older data?
2. Does it produce unacceptable drawdown?
3. Does it fail basic execution or trade-count viability?
4. Does it depend on a very narrow recent market condition?
5. Does the risk-management profile become structurally unsafe outside the recent window?

A strategy should not automatically lose simply because it performs less impressively on the older 50,000 M5 bars. That older block is not the live target. However, if the strategy fails catastrophically on the historical stress audit, that should trigger review, rejection, or risk adjustment.

The historical stress audit is therefore an audit layer, not the main optimisation target.

---

## Revised workflow summary

The intended FXPM workflow should be:

```text
1. Download or retain the latest 300,000 base M5 bars.

2. Split the data into:
   - Oldest 50,000 M5 bars: `historical_stress_audit` / `historical_audit_window`.
   - Newest 250,000 M5 bars: active recent processing universe.

3. Stage 1:
   - Run baseline strategies on the active 250,000 M5 bars.
   - Apply config-defined eligibility criteria.
   - Do not treat Stage 1 as the primary Top-K layer.
   - Do not over-segment the active window unless explicitly required.
   - Preserve viable strategies for Stage 2.

4. Stage 2:
   - Take Stage 1 survivors.
   - Run high-compute algorithmic optimisation.
   - Apply Top-K logic within the optimisation process.
   - Optimise hyperparameters, strategy variants, and risk-management profiles.
   - Select the best complete candidate per symbol, timeframe, routine, and market regime.

5. Historical stress audit:
   - Test selected candidates against the oldest 50,000 M5-bar historical audit window.
   - Use this to detect catastrophic fragility, unacceptable drawdown, or structural failure.
   - Do not allow the historical audit to dominate recent relevance unless the failure is severe.

6. Live-alignment check:
   - Ensure the backtest, optimiser, reports, and live executor use the same TP, SL, exit, and risk-management assumptions.
```

---

## Exit-surface and live-behaviour alignment

Another important issue identified earlier was potential misalignment between optimisation behaviour and live GPT or PM behaviour.

If the system optimises on one exit surface but trades on another, then the optimisation result is not fully valid. This includes differences in TP behaviour, SL handling, exit assumptions, regime multipliers, GP multiples, or any other mechanism where the optimiser and live executor do not match.

Some practical shortcuts may be acceptable if they preserve the actual decision logic and do not materially affect final results. However, if the difference changes which strategies win, how trades exit, how risk is applied, or how profitability is measured, then it is a core issue and should be corrected.

The revised FXPM workflow should include a live-alignment check to ensure that:

1. The optimiser uses the same TP and SL logic as live trading.
2. The report reflects the same exit assumptions as execution.
3. Regime multipliers and GP multiples are consistently applied.
4. Risk-management assumptions match between optimisation and live deployment.
5. No strategy is selected based on behaviour that cannot actually occur in live trading.

This is essential because the goal is not simply to produce good backtest reports. The goal is to produce live-actionable strategy selections.

---

## Why the new workflow is preferable

The new workflow is preferable because it directly addresses the live-relevance problem.

The old workflow risks selecting strategies from a broad or older historical universe and then merely hoping they remain relevant in the latest market window. The latest data becomes a passive check rather than the primary driver of strategy selection.

The new workflow uses the most recent 250,000 M5 bars as the active decision universe. This makes the selected strategies more likely to reflect current market structure, current volatility conditions, current symbol behaviour, and current execution relevance.

The older data is still retained, but it is moved into the correct role: stress testing and auditing.

This produces a better balance:

1. Recent data drives selection and optimisation.
2. Older data checks for catastrophic fragility.
3. Stage 1 remains broad and config-governed.
4. Stage 2 performs the expensive Top-K optimisation.
5. Risk management becomes part of the final selection process.
6. Live execution alignment becomes a required quality check.

---

## Implementation implications

The developer should review and update the FXPM stack in the following areas.

### 1. Data download configuration

Change the loaded base M5 bar count from 500,000 to 300,000 for the active workflow.

The new structure should reserve the oldest 50,000 M5 bars as `historical_stress_audit` and use the most recent 250,000 M5 bars as the active processing universe.

### 2. Data slicing logic

Implement explicit slicing logic:

```text
historical_stress_audit = oldest 50,000 M5 bars
active_universe = newest 250,000 M5 bars
```

The direction of time must be handled carefully. The "oldest" 50,000 means the oldest 50,000 M5 bars if the dataset is chronologically ordered from oldest to newest. If the dataset order is reversed, this must be corrected before slicing.

### 3. Stage 1 processing

Update Stage 1 terminology and logic so that it is not treated as the primary Top-K layer.

Stage 1 should:

1. Use the active 250,000 M5-bar window.
2. Run baseline strategies.
3. Apply config-defined pass or fail criteria.
4. Output eligible strategy candidates for Stage 2.
5. Avoid unnecessary chronological segmentation.
6. Avoid prematurely rejecting strategies based on overly strict untuned performance assumptions.

### 4. Stage 2 optimisation

Ensure Stage 2 is the main Top-K and optimisation layer.

Stage 2 should:

1. Receive Stage 1 survivors.
2. Perform algorithmic optimisation.
3. Search across parameter combinations.
4. Include risk-management strategy selection where applicable.
5. Output final optimised candidates by symbol, timeframe, routine, and market regime.

### 5. Historical stress audit

Add or revise the historical audit evaluation so that the oldest 50,000 M5 bars are used after candidate optimisation.

The historical stress audit should be used to flag serious risk, not to dominate the recent-window selection process.

### 6. Reporting changes

Reports should clearly label each section according to its real purpose.

Recommended terminology:

```text
Stage 1: Config-based baseline eligibility
Stage 2: Top-K optimisation and risk-management selection
Historical stress audit: Oldest 50,000 M5-bar severe-fragility check
Active universe: Most recent 250,000 M5-bar optimisation universe
```

Avoid calling the Stage 2 optimisation slice “validation” if it is used to tune or select strategies. That wording can create confusion. A more accurate term is optimisation, calibration, final ranking, or production selection.

### 7. Live execution alignment

Audit the optimiser, backtest engine, reports, and live executor for consistency.

Any mismatch in TP, SL, exit surface, risk profile, regime multiplier, or GP multiple should be corrected if it can affect final selection or live behaviour.

---

## Recommended developer instruction

Revise the FXPM workflow so that it operates on the latest 300,000 base M5 bars instead of up to 500,000 bars. The oldest 50,000 M5 bars should be reserved as `historical_stress_audit` / `historical_audit_window`. The newest 250,000 M5 bars should become the active processing universe for strategy eligibility and optimisation.

Stage 1 should not be treated as the primary Top-K layer. Stage 1 should run baseline strategies across the active 250,000 M5-bar universe and apply the config-defined eligibility criteria. Its job is to determine which strategies are viable enough to proceed, not to perform final strategy optimisation. Avoid adding unnecessary A/B/C chronological segmentation to Stage 1 because the strategies are still untuned baselines and segmentation may prematurely reject candidates that would perform well after optimisation.

Stage 2 should be treated as the main Top-K and algorithmic optimisation layer. It should take Stage 1 survivors and perform the expensive parameter search, strategy refinement, and risk-management selection. Stage 2 should identify the best complete strategy package per symbol, timeframe, routine, and market regime.

The 50,000 M5-bar historical stress audit should be used after optimisation as a stress and audit layer. It should detect catastrophic failure, unacceptable drawdown, execution fragility, or structural risk. It should not dominate selection unless the failure is severe, because the main FXPM objective is live relevance based on the most recent 250,000 M5 bars.

Finally, ensure the optimiser, backtest engine, reports, and live executor are aligned. Any difference between optimised behaviour and live behaviour, especially around TP, SL, exits, risk-management assumptions, regime multipliers, or GP multiples, should be corrected so that FXPM selects strategies based on behaviour that can actually occur in live trading.

---

## Final target state

The target state is an FXPM workflow that is more adaptive, less biased by stale historical data, and more directly aligned with live-market profitability.

The final system should:

1. Use the most relevant recent data for active strategy selection and optimisation.
2. Preserve older data as a stress and audit mechanism.
3. Keep Stage 1 broad, simple, and config-governed.
4. Keep Top-K primarily inside Stage 2.
5. Optimise both strategy parameters and risk-management profiles.
6. Prevent live/backtest/optimiser mismatch.
7. Improve profitability, signal quality, actionability, and codebase clarity.

This change should leave the PM codebase in a better position than it was found: more accurate, more actionable, more profitable, cleaner in structure, and more honest about what each data subset and processing stage is actually doing.
