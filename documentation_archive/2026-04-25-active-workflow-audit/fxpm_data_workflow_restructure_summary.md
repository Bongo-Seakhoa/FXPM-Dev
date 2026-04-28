# FXPM Data Workflow Restructure Summary

## Purpose Of This Document

This document summarizes the reasoning, design shift, and implementation direction behind the proposed FXPM data workflow restructure.

The central goal is not to create an academically neat train, validation, and holdout split. The central goal is to make FXPM more profitable, more actionable, more accurate, more adaptive to current market behavior, and better aligned with the live deployment window.

Efficiency and runtime optimization matter, but they are secondary. They should support the core objective rather than govern it. The main objective is to improve the quality of the PM itself.

## Core Principle

FXPM is not primarily training a supervised model. It is performing strategy selection, candidate filtering, arithmetic optimization, parameter search, and risk-management selection.

Some machine-learning concepts still matter, especially overfitting and selection bias, but FXPM should not be forced into a classical train/validation/test structure if that structure reduces live-market relevance.

The better framing is:

> FXPM should select and optimize strategies from the data window most relevant to live deployment, while using older data as a stress or audit layer rather than allowing older data to dominate strategy selection.

## Starting Concern

The current workflow may be using stale or less relevant data to decide which strategies are allowed to reach later optimization.

The current or previous flow can be understood roughly as:

1. Load up to about 500,000 M5 bars.
2. Use a large earlier portion of the dataset, roughly 80%, to identify strategy candidates.
3. Use the latest portion as validation, tuning, or relevance check.
4. Assume strategies selected from the earlier data remain the right candidates for the latest live-relevant window.

The weakness is that the most recent market information does not control the strategy-selection pool strongly enough. The latest window can only tune, validate, or reject strategies that already passed the earlier filter. It cannot recover a strategy that was excluded before the expensive optimization stage, even if that strategy is more relevant to the current market structure.

This creates beam exclusion risk.

Beam exclusion risk means FXPM may discard the best live-relevant strategy before the expensive optimization step ever begins. Once that happens, later optimization cannot fix the issue because the correct candidate is no longer available.

This is different from a normal overfitting problem. It is a live-relevance problem.

## Terminology Note

In the current codebase, `holdout` refers to the freshest scored out-of-sample tail produced by `DataSplitter`. That existing meaning is different from the oldest-data audit concept in this proposal.

To avoid confusion, the oldest 50,000 M5 bars in this proposal should not be called a holdout. They should be called:

- `historical_stress_audit`
- `historical_audit_window`

This historical audit window is out of active selection, but it is older than the active universe. It is not a forward-looking test. Its job is to detect severe fragility, catastrophic drawdown, execution weakness, or structural risk on older market data.

## Revised View Of Relevance Versus Validation

The important production question is not:

> Which strategy performed best across the largest historical dataset?

The more relevant FXPM question is:

> Which strategy is most likely to be profitable, actionable, accurate, and executable in the market window closest to live deployment?

Older data should not be ignored, but it should not dominate active strategy selection. In FX, older data can be useful for stress testing, but it can also introduce legacy bias. More data is not automatically better if that data reflects market structures that are no longer dominant.

The new workflow should therefore prioritize recent market relevance while retaining older data as a secondary audit mechanism.

## Final Proposed Data Structure

All quantities in this proposal are base M5 bars. Higher timeframes should be sliced by timestamp boundaries derived from the base M5 data, not by independent 50,000/250,000 candle counts.

The proposed structure is:

1. Download or retain the latest 300,000 M5 bars.
2. Reserve the oldest 50,000 M5 bars as `historical_stress_audit` / `historical_audit_window`.
3. Use the newest 250,000 M5 bars as the active FXPM processing universe.

The layout is:

```text
Downloaded base data: 300,000 M5 bars

Oldest 50,000 M5 bars:
historical_stress_audit / historical_audit_window

Newest 250,000 M5 bars:
active processing universe for strategy eligibility, parameter optimization, ranking, and production selection
```

The historical audit window should not govern current strategy selection. It should be used to detect serious collapse, fragility, unacceptable drawdown, execution incompatibility, or major structural weakness.

The active 250,000 M5 bars are the live-relevant data universe. This is the data window that should drive candidate eligibility, strategy optimization, risk-management selection, and final production selection.

## Current Behavior Versus Proposed Behavior

This proposal is not the current split contract.

Current config loads up to 500,000 bars, and `DataSplitter` currently creates train, validation, and freshest-tail holdout windows by percentage. With the current `train_pct = 80`, `val_pct = 10`, `overlap_pct = 10`, and no explicit `holdout_pct`, the effective shape is roughly:

```text
oldest 80%: train
next 10%: scored validation
newest 10%: current code holdout / freshest-tail diagnostic
```

The proposed workflow instead creates an explicit active universe before Stage 1 and Stage 2:

```text
oldest 50,000 M5 bars of the 300,000-bar window:
historical_stress_audit

newest 250,000 M5 bars:
active universe for strategy eligibility and optimization
```

Implementing this requires explicit slicing before or alongside `DataSplitter` so the historical audit window is not confused with the existing freshest-tail holdout.

## Stage 1 Clarification

Top-K does not primarily belong to Stage 1.

Stage 1 has a Top-K-like effect only in the loose sense that some strategies survive and others do not. Technically, Stage 1 should be a criteria-based eligibility and filtering stage.

Stage 1 should be described as:

> A baseline strategy eligibility and viability stage governed by config-defined pass criteria.

It should not be described as:

> A Top-K optimization layer.

This distinction matters because Stage 1 strategies are baseline strategies. They do not yet have hyperparameter tuning or refined risk-management optimization. They are not final strategy forms. They are coarse candidates being assessed for whether they deserve expensive downstream optimization.

Stage 1 should be permissive enough to preserve promising strategies. It should not overgovern the strategy space before optimization has had a chance to work.

## What Stage 1 Should Do

Stage 1 should run baseline strategy configurations across the active 250,000 M5 bar universe and apply predefined config criteria.

Its purpose is to answer:

> Which baseline strategies are viable enough to proceed into deeper optimization?

Stage 1 should apply only the criteria that are already intended and defined in config, such as minimum viability, unacceptable drawdown, insufficient trade count, invalid outputs, execution incompatibility, or other established thresholds.

Stage 1 should not try to be the final judge of strategy quality. It should not attempt to prove full robustness. It should not impose extra chronological segmentation unless that is explicitly required by config and aligned with the architecture.

The goal of Stage 1 is candidate preservation, not final selection.

## Why Stage 1 Should Not Be Over-Segmented

A segmented A/B/C internal test was considered, but it is not necessary for Stage 1 and may be counterproductive.

Stage 1 deals with untuned baseline strategies. If the active 250,000 M5 bar universe is split into smaller chronological segments, then each baseline strategy is judged on smaller windows before it has been optimized. That can prematurely reject strategies that could perform well after Stage 2 parameter optimization.

Segmentation also reintroduces the relevance problem in a different form. Segment A is older than Segment B, and Segment B is older than Segment C. Once these internal segments govern the candidate pool, older data again influences which strategies survive. That works against the purpose of shifting FXPM toward a more recent and adaptive data universe.

For Stage 1, full-window aggregation over the active 250,000 M5 bars is more appropriate.

Stage 1 should therefore not use A/B/C segmented scoring as a governing selection mechanism unless there is a very specific implementation reason.

## Stage 2 Clarification

Top-K primarily belongs to Stage 2.

Stage 2 is the high-compute optimization layer. This is where the system should perform deeper algorithmic optimization, parameter search, final ranking, and complete package selection among viable candidates.

Stage 2 receives strategies that passed Stage 1 config-based eligibility. It then applies the expensive optimization process to determine which variants are strongest.

This is where it makes sense to speak about Top-K candidates, winning iterations, final rankings, and optimized strategy variants.

Stage 2 should be described as:

> The Top-K and algorithmic optimization layer that searches across parameter, strategy, and, where appropriate, risk-management combinations for the best production-ready candidates.

## What Stage 2 Should Do

Stage 2 should optimize the strategies that passed Stage 1 across the active 250,000 M5 bar universe, or across the intended Stage 2 optimization slice within that active universe if the implementation has a specific split.

The key responsibilities of Stage 2 are:

1. Run the expensive parameter search.
2. Optimize strategy settings.
3. Rank the resulting candidate variants.
4. Select the best production-ready strategy per symbol, timeframe, and market regime.
5. Ensure that the selected strategy is aligned with live execution rules.
6. Optionally include controlled risk-management policy selection if that approach is chosen.

Stage 2 is where the real competitive selection happens.

## Risk-Management Integration

The revised FXPM should eventually identify not only the best entry strategy per symbol, timeframe, and regime, but the best complete trade-management package.

The intended selection target is:

> Best strategy plus best risk-management profile per symbol, timeframe, and market regime.

There are two acceptable implementation paths:

### Path A: Risk Management Inside Stage 2

Risk-management policy selection can be included inside Stage 2 as part of the optimization surface. This makes the final Stage 2 winner a complete strategy/risk-management package.

This is conceptually clean, but it expands the optimization surface. It should use a curated policy set, strict baseline-improvement rules, enough-trade safeguards, and artifact metadata so the added surface improves profitability without creating a new overfit channel.

### Path B: Optional Future Pseudo Stage 3

An alternative is to keep Stage 2 focused on choosing the best strategy, timeframe, regime, and parameters, then run a separate pseudo Stage 3 for risk-management/governance selection.

Under this approach, Stage 3 would run after the Stage 2 best regime/timeframe/symbol candidate has been selected. It would evaluate risk-management or governance policies using the Stage 2 data slice or another explicitly defined active-universe slice. It would then attach the selected risk-management policy to the Stage 2 winner.

If the implementation keeps a current-flow style internal split inside the 250,000 M5 bar active universe, this pseudo Stage 3 may run on the same Stage 2 data slice. For example, if Stage 2 is defined as half of the 250,000 M5 bar active universe, pseudo Stage 3 can reuse that same Stage 2 slice for risk-management selection rather than creating a new walk-forward gate.

This should be treated as optional future architecture, not as a hidden current production stage. Current code already has local governance tournament plumbing, but it currently selects policies on the codebase's freshest-tail holdout surface and runs in shadow mode by config.

The key rule for either path is the same: risk management must not become a fixed afterthought if it materially changes live trade results. If risk-management behavior can change which candidates are profitable or actionable, it belongs in the audited optimization/selection contract.

## Historical Stress Audit Role

The oldest 50,000 M5 bars should be treated as `historical_stress_audit` / `historical_audit_window`.

This block is not the same as the current codebase's freshest holdout. It is older than the active universe and should not become the primary selector.

The historical audit should answer questions such as:

1. Does the selected strategy completely collapse on older data?
2. Does it produce unacceptable drawdown?
3. Does it fail basic execution or trade-count viability?
4. Does it depend on a very narrow recent market condition?
5. Does the risk-management profile become structurally unsafe outside the recent window?
6. Does it reveal warmup or indicator-state requirements that should be made explicit?

A strategy should not automatically lose simply because it performs less impressively on the historical audit window. That older block is not the live target. However, if the strategy fails catastrophically, that should trigger review, rejection, risk adjustment, or reduced confidence.

The historical audit is therefore a severe-fragility screen and reporting layer, not the main optimization target.

## Non-Gating Walk-Forward Reporting

Walk-forward evaluation should not be inserted as an intermediary gate between Stage 2, optional pseudo Stage 3, and live implementation.

The reason is the same principle discussed throughout the workflow redesign: adding a heavy walk-forward gate can degrade live relevance by forcing fresh selections through stale or overly segmented filters.

However, a separate non-gating walk-forward report can be useful if it is implemented correctly.

Recommended future report:

1. At each two-week retrain, take the winners from the previous production cycle.
2. Replay those stale winners over the prior two-week live-adjacent period.
3. Compare expected behavior against realized behavior.
4. Measure drift, regime mismatch, edge decay, trade-count deviation, drawdown deviation, and execution feasibility.
5. Produce an independent report for operator insight.
6. Do not use this report as an automatic gate for the newly selected winners.

This stale-winner walk-forward report is observational. It should help diagnose whether the prior winners decayed as expected, but it should not sit between the new winners and live implementation unless a future operator policy explicitly promotes it to a gating rule.

## Revised Workflow Summary

The intended FXPM workflow should be:

```text
1. Download or retain the latest 300,000 M5 bars.

2. Split the base M5 data into:
   - Oldest 50,000 M5 bars: historical_stress_audit / historical_audit_window.
   - Newest 250,000 M5 bars: active recent processing universe.

3. Derive timestamp boundaries from the M5 split:
   - Apply those boundaries consistently to M15, M30, H1, H4, D1, and any future timeframe.
   - Do not independently take 50,000 higher-timeframe candles.

4. Stage 1:
   - Run baseline strategies on the active 250,000 M5 bar universe.
   - Apply config-defined eligibility criteria.
   - Do not treat Stage 1 as the primary Top-K layer.
   - Do not over-segment the active window unless explicitly required.
   - Preserve viable strategies for Stage 2.

5. Stage 2:
   - Take Stage 1 survivors.
   - Run high-compute algorithmic optimization.
   - Apply Top-K logic within the optimization process.
   - Optimize hyperparameters and strategy variants.
   - Optionally include controlled risk-management selection if Path A is chosen.
   - Select the best production-ready candidate per symbol, timeframe, and market regime.

6. Optional pseudo Stage 3:
   - If Path B is chosen, run risk-management/governance policy selection after the Stage 2 winner is selected.
   - Use the same Stage 2 slice where appropriate, including a half-of-active-universe slice if that is the chosen Stage 2 contract.
   - Require enough trades and baseline improvement.
   - Store the selected policy and evidence in artifact metadata.

7. Historical stress audit:
   - Test selected candidates against the oldest 50,000 M5 bar historical audit window.
   - Use this to detect catastrophic fragility, unacceptable drawdown, execution weakness, or structural failure.
   - Do not allow the historical audit to dominate recent relevance unless the failure is severe.

8. Live-alignment check:
   - Ensure the backtest, optimizer, reports, and live executor use the same TP, SL, exit, and risk-management assumptions.
```

## Exit-Surface And Live-Behavior Alignment

If the system optimizes on one exit surface but trades on another, then the optimization result is not fully valid.

This includes differences in:

- TP behavior
- SL handling
- exit assumptions
- regime multipliers
- GP multiples
- governance or risk-management policy
- position sizing assumptions
- execution feasibility rules

Some practical shortcuts may be acceptable if they preserve the actual decision logic and do not materially affect final results. However, if a difference changes which strategies win, how trades exit, how risk is applied, or how profitability is measured, then it is a core issue and should be corrected.

The revised FXPM workflow should include a live-alignment check to ensure:

1. The optimizer uses the same TP and SL logic as live trading.
2. Reports reflect the same exit assumptions as execution.
3. Regime multipliers and GP multiples are consistently applied.
4. Risk-management assumptions match between optimization and live deployment.
5. No strategy is selected based on behavior that cannot occur in live trading.

This is essential because the goal is not simply to produce good backtest reports. The goal is to produce live-actionable strategy selections.

## Why The New Workflow Is Preferable

The new workflow directly addresses the live-relevance problem.

The old workflow risks selecting strategies from a broad or older historical universe and then merely hoping they remain relevant in the latest market window. The latest data becomes a passive check rather than the primary driver of strategy selection.

The new workflow uses the most recent 250,000 M5 bars as the active decision universe. This makes selected strategies more likely to reflect current market structure, current volatility conditions, current symbol behavior, and current execution relevance.

The older data is still retained, but it is moved into the correct role: stress testing and auditing.

This produces a better balance:

1. Recent data drives selection and optimization.
2. Older data checks for catastrophic fragility.
3. Stage 1 remains broad, simple, and config-governed.
4. Stage 2 performs the expensive Top-K optimization.
5. Risk management is handled through a controlled optimization or pseudo Stage 3 path.
6. Walk-forward reporting remains non-gating and observational.
7. Live execution alignment becomes a required quality check.

## Implementation Implications

The developer should review and update the FXPM stack in the following areas.

### 1. Data Download Configuration

Change the loaded base M5 bar count from 500,000 to 300,000 for the proposed workflow.

The new structure should reserve the oldest 50,000 M5 bars as `historical_stress_audit` and use the newest 250,000 M5 bars as the active processing universe.

### 2. Data Slicing Logic

Implement explicit base-M5 slicing:

```text
historical_stress_audit = oldest 50,000 M5 bars
active_universe = newest 250,000 M5 bars
```

The direction of time must be handled carefully. "Oldest" means earliest in chronological order. If a dataset is reversed, it must be sorted before slicing.

Higher-timeframe data should inherit timestamp boundaries from the M5 split.

### 3. Stage 1 Processing

Update Stage 1 terminology and logic so it is not treated as the primary Top-K layer.

Stage 1 should:

1. Use the active 250,000 M5 bar universe.
2. Run baseline strategies.
3. Apply config-defined pass/fail criteria.
4. Output eligible strategy candidates for Stage 2.
5. Avoid unnecessary chronological segmentation.
6. Avoid prematurely rejecting strategies based on overly strict untuned performance assumptions.

### 4. Stage 2 Optimization

Ensure Stage 2 is the main Top-K and algorithmic optimization layer.

Stage 2 should:

1. Receive Stage 1 survivors.
2. Perform algorithmic optimization.
3. Search across parameter combinations.
4. Include controlled risk-management selection if Path A is chosen.
5. Output final optimized candidates by symbol, timeframe, and market regime.

### 5. Optional Pseudo Stage 3

If risk management is not included inside Stage 2, add an optional pseudo Stage 3 after Stage 2.

Pseudo Stage 3 should:

1. Receive the selected Stage 2 winner.
2. Evaluate curated risk-management/governance policies.
3. Use the same Stage 2 slice where appropriate, including a half-of-active-universe slice if that is the chosen Stage 2 contract.
4. Require enough trades and baseline improvement.
5. Attach selected policy evidence to the artifact.
6. Remain separate from walk-forward gating.

### 6. Historical Stress Audit

Add or revise historical audit evaluation so selected candidates are tested against the oldest 50,000 M5 bars.

The historical audit should be used to flag serious risk, not to dominate recent-window selection.

### 7. Non-Gating Walk-Forward Report

Add a future independent walk-forward report for stale winners over the prior two-week retrain period.

This report should measure drift and expectation deviation. It must not be added as an intermediary live gate.

### 8. Reporting Changes

Reports should clearly label each section according to its real purpose.

Recommended terminology:

```text
Stage 1: Config-based baseline eligibility
Stage 2: Top-K optimization and production selection
Optional pseudo Stage 3: Risk-management/governance policy selection
Historical stress audit: Oldest 50,000 M5 bar severe-fragility check
Active universe: Newest 250,000 M5 bar optimization universe
Freshest-tail holdout: Existing codebase term for newest out-of-sample diagnostic, if retained
Stale-winner walk-forward report: Non-gating prior-cycle drift report
```

Avoid calling any slice "validation" if it is used to tune or select strategies. More accurate terms are optimization, calibration, final ranking, production selection, historical audit, or freshness diagnostic.

### 9. Live Execution Alignment

Audit the optimizer, backtest engine, reports, and live executor for consistency.

Any mismatch in TP, SL, exit surface, risk profile, regime multiplier, GP multiple, governance policy, or sizing assumption should be corrected if it can affect final selection or live behavior.

## Recommended Developer Instruction

Revise the FXPM workflow so that it operates on the latest 300,000 M5 bars instead of up to 500,000 M5 bars. The oldest 50,000 M5 bars should be reserved as `historical_stress_audit` / `historical_audit_window`. The newest 250,000 M5 bars should become the active processing universe for strategy eligibility, optimization, and production selection.

Stage 1 should not be treated as the primary Top-K layer. Stage 1 should run baseline strategies across the active 250,000 M5 bar universe and apply config-defined eligibility criteria. Its job is to determine which strategies are viable enough to proceed, not to perform final strategy optimization. Avoid unnecessary A/B/C chronological segmentation in Stage 1 because the strategies are still untuned baselines and segmentation may prematurely reject candidates that would perform well after optimization.

Stage 2 should be treated as the main Top-K and algorithmic optimization layer. It should take Stage 1 survivors and perform expensive parameter search, strategy refinement, and final ranking. Risk-management selection can either be integrated into Stage 2 or deferred into an optional pseudo Stage 3 after the Stage 2 best symbol/timeframe/regime strategy is chosen.

The historical 50,000 M5 bar audit should be used after optimization as a stress and audit layer. It should detect catastrophic failure, unacceptable drawdown, execution fragility, or structural risk. It should not dominate selection unless the failure is severe, because the main FXPM objective is live relevance based on the newest 250,000 M5 bars.

Walk-forward evaluation should not be inserted as a gate between Stage 2 or optional pseudo Stage 3 and live implementation. A separate non-gating stale-winner walk-forward report can be added later for the prior two-week retrain period, but it should be observational and diagnostic.

Finally, ensure the optimizer, backtest engine, reports, and live executor are aligned. Any difference between optimized behavior and live behavior, especially around TP, SL, exits, risk-management assumptions, regime multipliers, GP multiples, or sizing assumptions, should be corrected so FXPM selects strategies based on behavior that can actually occur in live trading.

## Final Target State

The target state is an FXPM workflow that is more adaptive, less biased by stale historical data, and more directly aligned with live-market profitability.

The final system should:

1. Use the most relevant recent data for active strategy selection and optimization.
2. Preserve older data as a stress and audit mechanism.
3. Keep Stage 1 broad, simple, and config-governed.
4. Keep Top-K primarily inside Stage 2.
5. Add risk-management selection through a controlled Stage 2 or optional pseudo Stage 3 path.
6. Keep walk-forward reporting independent and non-gating.
7. Prevent live/backtest/optimizer mismatch.
8. Improve profitability, signal quality, actionability, and codebase clarity.

This change should leave the PM codebase in a better position than it was found: more accurate, more actionable, more profitable, cleaner in structure, and more honest about what each data subset and processing stage is actually doing.
