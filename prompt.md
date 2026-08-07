# PERFORMANCE-FIRST IMPLEMENTATION MANDATE
## Optuna- and Numba-Compatible Trading System Improvement Phase

This is a corrective follow-up to the previous audit prompts and reports.

This phase is not another broad academic audit, account-size study, governance exercise, or documentation-only review.

The objective is to analyze, implement, test, and prove code-level changes that improve the trading system’s practical ability to:

1. Trade successfully.
2. Make more net money after realistic costs.
3. Extract more value from the existing strategy architecture.
4. Reduce avoidable drawdown without destroying profitable behavior.
5. Recover more effectively.
6. Maintain or improve profitable trade frequency.
7. Execute more accurately.
8. Optimize more accurately and with higher-quality candidate selection.
9. Remain fully compatible with Optuna and Numba.
10. Become more computationally efficient only after correctness and quality are established.

The two agents are Claude Code and Codex. Both agents operate on the same repository and must actively coordinate through the existing shared `agent_audit/` workspace.

The agents are authorized to inspect, modify, test, benchmark, and implement code within the constraints below.

---

# 1. Superseding Operator Direction

This instruction supersedes any conflicting recommendation in previous Markdown or HTML reports.

In particular, do not make the following the focus of this phase:

- Simulating many different starting balances.
- Replaying USD 22, USD 200, USD 1,000, USD 5,000, USD 10,000, and USD 100,000.
- Building account-size regime frameworks.
- Building balance-tiered symbol universes.
- Designing micro-account preservation modes.
- Lowering risk simply because conventional risk guidance considers it high.
- Running broad academic robustness exercises that do not change a production decision.
- Creating additional reporting infrastructure without a direct profitability use.
- Spending significant compute on simulations whose main result is that account size affects minimum-lot behavior.

For every comparative simulation in this phase, use:

```text
Canonical starting balance: USD 10,000
````

Do not optimize starting balance.

Do not include starting balance as an Optuna parameter.

Do not run a balance sweep.

Do not substitute USD 100,000 unless explicitly instructed by the operator in a future prompt.

The USD 10,000 baseline is intended to:

* Compare strategies on a common capital basis.
* Reduce extreme minimum-lot distortions.
* Reflect the account level toward which the system is intended to scale.
* Allow the best strategies and implementations to win based on trading quality.
* Avoid consuming compute on balance variations that do not improve strategy selection.

The current aggressive posture remains intentional.

Do not reduce risk, restrict trade flow, remove symbols, or add conservative controls merely because they appear aggressive. A risk-policy change may be proposed only when a controlled experiment shows that it:

* Increases net profit after costs;
* Removes losses that are not compensated by future profit;
* Improves recovery materially without destroying profitable trade flow;
* Corrects a genuine sizing, margin, accounting, or execution defect; or
* Prevents a false profitability conclusion.

---

# 2. Non-Negotiable Decision Hierarchy

All work must follow this hierarchy.

## Gate 1: Correctness and Fidelity

A change must first be:

* Mathematically correct.
* Internally consistent.
* Broker-spec aware.
* Cost aware.
* Reproducible.
* Compatible with the intended strategy.
* Compatible with the optimization pipeline.
* Compatible with the Numba execution path where applicable.
* Consistent between Python reference behavior and compiled behavior.
* Free of silent fallbacks or hidden semantic changes.

A faster implementation that changes trading decisions or produces less accurate calculations fails this gate.

## Gate 2: Trading and Economic Quality

Among correct implementations, prefer the implementation that produces the strongest practical trading outcome.

Primary economic evidence includes:

* Net profit after spread, commission, swap, and slippage.
* Total return.
* Expectancy after costs.
* Average net value per trade.
* Recovery factor.
* Recovery duration.
* Maximum equity drawdown.
* Profit factor.
* High-drawdown trade contribution.
* Profitable trade frequency.
* Stability of realized trading behavior.
* Accuracy of execution and position accounting.

No academic metric may automatically override materially better monetary performance unless it proves that the apparent profit is invalid or non-executable.

## Gate 3: Code and Optimization Quality

The implementation must be:

* Maintainable.
* Explicitly parameterized.
* Testable.
* Deterministic where intended.
* Compatible with existing configuration and result artifacts.
* Traceable from Optuna trial to selected parameters to live execution.
* Free of duplicated metric definitions.
* Resistant to silent parameter drift.

## Gate 4: Efficiency

Efficiency is a secondary qualifying criterion.

Only after two implementations are shown to be correct and economically equivalent should the faster, lower-memory, simpler, or more scalable implementation win.

Do not trade away:

* Calculation precision;
* Trading accuracy;
* Cost realism;
* Trade-path fidelity;
* Parameter-search quality;
* Logging needed for profit attribution; or
* Reproducibility

merely to reduce runtime.

The required principle is:

```text
Higher accuracy and higher quality first.
Greater efficiency second.
```

---

# 3. Definition of “Better”

For this phase, “better” does not mean more academically sophisticated.

A change is better when it demonstrably does at least one of the following:

* Increases net profit after realistic costs.
* Increases expectancy per trade.
* Improves average trade value.
* Removes recurrent loss patterns while preserving profitable trades.
* Converts poor entries into better-timed entries.
* Retains more MFE through better exits.
* Reduces unnecessary MAE.
* Improves recovery from drawdown.
* Reduces execution leakage.
* Corrects a calculation that was selecting the wrong strategy.
* Improves the quality of Optuna’s winner selection.
* Allows the optimizer to search a better parameterization.
* Reduces optimization runtime without changing results or search quality.
* Improves trade frequency where additional trades have positive marginal expectancy.
* Removes low-value trading activity that consumes risk and costs without contributing profit.

A change is not automatically better because it:

* Produces a higher Sharpe ratio.
* Produces a lower drawdown solely through lower position size.
* Produces a more conservative equity curve.
* Adds additional holdout reports.
* Adds additional balance simulations.
* Adds statistical terminology.
* Reduces runtime by simplifying the trading model.
* Makes the reports appear more institutional.
* Makes the implementation academically cleaner but less profitable.

---

# 4. Performance Relevance Gate

Before beginning any new task, experiment, suggestion, refactor, or implementation, create a short entry with the following information:

```markdown
## Performance Relevance

### Direct profitability mechanism
What exact mechanism could cause this work to increase or protect net profit?

### Evidence
Which code, trade records, logs, historical results, metrics, or observed behavior indicate that this mechanism exists?

### Code-level location
Which exact files, classes, functions, and configuration fields are involved?

### Production decision affected
What trading, optimization, execution, or strategy-selection decision will change?

### Minimum useful experiment
What is the smallest bounded test capable of confirming or rejecting the hypothesis?

### Optuna impact
How does this affect the search space, objective, trial lifecycle, result serialization, or winner selection?

### Numba impact
Does this touch a compiled hot path? If so, how will nopython compatibility and Python/Numba parity be maintained?

### Compute cost
Low / Medium / High

### Disposition
Proceed / Defer / Reject
```

Do not proceed with work that lacks a direct performance mechanism.

Academic work may proceed only when it prevents a false profit conclusion or changes an actual production decision.

---

# 5. Canonical Baseline Contract

Create:

```text
agent_audit/performance/BASELINE_10000.md
```

The canonical baseline must use:

* USD 10,000 initial capital.
* The current intended aggressive strategy posture.
* The existing broker specifications.
* The same symbol contract specifications used by production.
* The same lot-size, volume-step, margin, tick-size, tick-value, and currency rules.
* Realistic spread.
* Realistic commission.
* Realistic swap where applicable.
* Realistic slippage assumptions.
* The existing bleeding-edge data geometry unless a confirmed bug is found.
* The same data and random seed for every direct A/B comparison.
* The same Optuna trial budget for baseline and candidate comparisons.
* The same symbols and timeframes for each A/B comparison.
* The same strategy eligibility rules unless the tested change specifically concerns eligibility.

Record:

* Git commit or working-tree fingerprint.
* Configuration hash.
* Data fingerprint.
* Starting balance.
* Cost assumptions.
* Risk settings.
* Symbol specifications.
* Strategy version.
* Optuna study name.
* Sampler.
* Pruner.
* Seed.
* Trial budget.
* Numba enabled or disabled.
* Python/Numba implementation version.
* Compilation status.
* Exact command used.

Do not compare two candidates that were run under different balances, costs, data, seeds, or trial budgets and present the difference as strategy improvement.

---

# 6. Existing Audit Triage

Read all existing Markdown and HTML audit reports before making changes.

At minimum, inspect:

```text
QUANT_METHODOLOGY_AUDIT.md
RISK_RECOVERY_AUDIT.md
STRATEGIC_AUDIT.md
TECHNICAL_AUDIT.md
EXECUTIVE_SUMMARY.md
MT5_EXECUTION_ACCOUNTING_AUDIT.md
PRIORITIZED_RECOMMENDATIONS.md
```

Also inspect corresponding HTML reports in the repository.

Create:

```text
agent_audit/performance/RECOMMENDATION_TRIAGE.md
```

Classify every existing recommendation as:

* `KEEP NOW`
* `KEEP BUT REFRAME`
* `DEFER`
* `REJECT`
* `ALREADY IMPLEMENTED`
* `NEEDS EVIDENCE`

For every recommendation, include:

```markdown
| Recommendation | Existing Source | Direct Profit Mechanism | Compute Burden | Optuna Compatible? | Numba Compatible? | Decision | Reason |
```

The following prior directions are presumptively superseded for this phase:

* Multiple-balance replay.
* Account-size regimes.
* Balance-tiered symbol suppression.
* USD 22-specific optimization.
* Broad capital-preservation infrastructure.
* Lowering aggressive risk without direct evidence.
* Academic-only pristine-OOS expansion.
* Broad PBO research with no production decision attached.

The following prior findings remain high-value starting points:

* Metric definitions differ across engine, dashboard, pipeline, and tools.
* Dashboard profit factor can mishandle profitable no-loss samples.
* Sharpe and Sortino use incompatible bases across modules.
* Expectancy is reported in inconsistent units.
* Recovery factor is not canonical.
* Per-trade MAE/MFE is missing.
* Expected-versus-filled slippage attribution is incomplete.
* Entry and exit cost modeling may not be symmetrical.
* Backtest and live sizing semantics do not fully match.
* Bleeding-edge split geometry appears correct.
* The 50k window is a selection surface and must be labeled accurately.
* Current optimization evidence can contain extremely high metrics on small trade counts.

Do not re-audit confirmed split geometry from scratch unless new evidence contradicts the existing tests.

---

# 7. Mandatory Strategy Profit Anatomy

The previous audit concentrated too heavily on risk and methodology and not enough on how each strategy actually makes or loses money.

For every material strategy or strategy family, create a structured profit anatomy.

Use:

```text
agent_audit/performance/strategy_anatomy/
```

Each strategy file must include:

```markdown
# Strategy Profit Anatomy: <strategy name>

## Intended edge
Why should this strategy make money?

## Exact implementation
- Files
- Classes
- Functions
- Parameters
- Indicators
- Entry rules
- Exit rules
- Stop logic
- Take-profit logic
- Trailing logic
- Break-even logic
- Session logic
- Regime logic

## Profit attribution
Break down profitability by:
- Symbol
- Timeframe
- Long/short direction
- Session
- Market regime
- Holding duration
- Exit reason
- Volatility level
- Spread level
- Parameter region
- Strategy combination

## Loss attribution
Identify:
- Immediate adverse entries
- Repeated stop-out structures
- Poor payoff structures
- High-cost low-edge trades
- Entries during unsuitable regimes
- Entries with late confirmation
- MFE given back
- Excessive holding time
- Repeated correlated exposure
- Unprofitable parameter regions

## Entry analysis
- Which entry conditions distinguish winners from losers?
- Which conditions are redundant?
- Which conditions delay entry?
- Which conditions allow low-quality trades?
- Is entry logic calculated identically in Python and Numba?
- Is current-bar information used incorrectly?
- Are all parameters exposed correctly to Optuna?

## Exit analysis
- How much MFE is retained?
- How much MFE is given back?
- Which stop-outs subsequently recover?
- Which winners are exited too early?
- Does trailing improve or reduce net profit?
- Does break-even improve or reduce net profit?
- Are exit prices and slippage modeled accurately?
- Is exit logic calculated identically in Python and Numba?

## Strategy interaction analysis
- Risk and position sizing
- Regime filters
- Portfolio filters
- Execution handling
- Cost model
- Symbol specifications
- Parameter-selection process

## Exact fix points
Specific code changes that could improve profitability.

## Optuna integration
How each proposed change enters or affects optimization.

## Numba integration
How the change will remain compiled-path compatible.

## Bounded validation
The exact A/B experiment needed.
```

Do not finish this phase with only infrastructure recommendations. The actual strategy entry and exit implementations must be examined.

---

# 8. High-Drawdown Trade Analysis

High drawdown must be treated as a profit attribution question, not merely a risk warning.

The main question is:

```text
Is the drawdown being paid for?
```

Implement sufficient instrumentation to distinguish:

1. Profitable pain worth retaining.
2. Bad risk that produces poor final returns.
3. Valid signals with poor entry timing.
4. Valid entries with poor exit management.
5. Trades harmed by spread or slippage.
6. Trades taken in the wrong regime.
7. Oversized exposure caused by implementation.
8. Correlated trade stacking.
9. Reporting artifacts.
10. Trades with insufficient evidence.

At minimum, add or validate:

* `mae_pips`
* `mfe_pips`
* `mae_r`
* `mfe_r`
* Time to MAE
* Time to MFE
* Time underwater
* Entry-to-stop distance
* Entry-to-target distance
* Final realized R
* Exit reason
* Spread at entry
* Spread at exit
* Expected entry price
* Filled entry price
* Expected exit price
* Filled exit price
* Entry slippage
* Exit slippage
* Commission
* Swap
* Strategy identifier
* Parameter-set identifier
* Optuna trial or winner identifier where applicable

For backtesting, MAE/MFE must be captured in the actual trade-path implementation. Do not derive it from insufficient summary data after the fact.

For Numba paths, MAE/MFE must be implemented in the compiled loop or through a proven equivalent mechanism.

Avoid storing every intrabar value if per-trade aggregate excursion values provide sufficient evidence. Preserve accuracy while avoiding unnecessary memory growth.

---

# 9. Canonical Metric Contract

Create one canonical metric implementation or a clearly defined shared contract.

Inspect at minimum:

* `pm_core.py`
* `pm_pipeline.py`
* `pm_optuna.py`
* `pm_dashboard/analytics.py`
* `tools/benchmark.py`

The same metric name must not refer to different formulas without an explicit suffix.

Use distinct names where necessary:

* `expectancy_pips`
* `expectancy_dollars`
* `expectancy_r`
* `sharpe_equity_bar`
* `sharpe_trade`
* `sortino_equity_bar`
* `sortino_trade`
* `max_equity_drawdown_pct`
* `max_balance_drawdown_pct`
* `open_drawdown_pct`
* `recovery_factor_currency`
* `return_to_drawdown_ratio`
* `recovery_duration_bars`
* `recovery_duration_time`

Requirements:

* Profit factor must handle no-loss profitable samples correctly.
* Infinite or undefined metrics must not silently become zero.
* NaN, infinity, and insufficient-sample cases must be explicit.
* Optuna objectives must not receive misleading sentinel values.
* Reporting must use the same definitions as winner selection or clearly identify a different basis.
* Metric units must be included in field names or metadata.
* Cost assumptions must be attached to results.
* Sparse-trade metrics must carry trade-count context.
* Trade-count floors must not be imposed academically; they must prevent false winner selection.

Before changing the optimizer objective, document the current objective mathematically and show how each term contributes to representative trial scores.

Do not introduce an arbitrary weighted composite without proving that its rankings align better with actual net trading performance.

---

# 10. Optuna Compatibility Contract

Any new strategy feature, filter, exit mechanism, risk control, cost model, or optimization suggestion must be compatible with the existing Optuna workflow.

Inspect at minimum:

* `pm_optuna.HyperparameterOptimizer.optimize`
* `pm_optuna.HyperparameterOptimizer._fallback_random_search`
* `pm_pipeline.RegimeOptimizer.optimize_symbol`
* Current objective/scoring functions
* Trial serialization
* Winner export
* Selected parameter loading
* Historical study resume behavior

## 10.1 End-to-End Parameter Propagation

Every new optimizable parameter must be added consistently to:

1. Configuration schema.
2. Default configuration.
3. Parameter validation.
4. Optuna search space.
5. Conditional search logic.
6. Fallback random search where applicable.
7. Strategy constructor or runtime input.
8. Python reference implementation.
9. Numba implementation.
10. Trial metadata.
11. Result export.
12. Winner ledger.
13. Parameter reload.
14. Live/demo execution.
15. Tests.
16. Documentation.
17. Search-space version or fingerprint.

A parameter is not considered implemented if it exists only in the Optuna suggestion function but is ignored by the backtester or live strategy.

## 10.2 Objective Requirements

The Optuna objective must:

* Use canonical metrics.
* Use net results after realistic costs.
* Reject invalid or non-finite trials explicitly.
* Record the reason for rejected trials.
* Not allow no-trade or extremely sparse trials to appear optimal.
* Not silently translate errors into attractive scores.
* Not reward inflated annualized ratios without sufficient trading evidence.
* Not optimize starting balance.
* Not reward a change solely because it takes less risk.
* Remain aligned with actual winner selection.
* Remain aligned with the metrics shown in reports.
* Preserve the aggressive trading mandate.

Prefer constrained or Pareto-aware selection over an opaque composite when practical.

A valid performance hierarchy is:

```text
Validity and correctness gate
    ↓
Positive net economic result after costs
    ↓
Trading quality and drawdown/recovery characteristics
    ↓
Trade-count and execution viability
    ↓
Efficiency tie-break
```

## 10.3 Search Quality

Do not claim improvement merely because a candidate wins a small noisy Optuna run.

For baseline versus candidate:

* Use the same trial budget.
* Use the same sampler.
* Use the same seed.
* Use the same pruner.
* Use the same data.
* Use the same starting balance.
* Use the same cost model.
* Use the same symbols and timeframes.
* Use the same search-space treatment unless the change itself is the search-space change.

When the search space changes, report:

* Old dimensions.
* New dimensions.
* Conditional dimensions.
* Bounds.
* Step or log scales.
* Invalid-region controls.
* Expected impact on search difficulty.
* Whether historical studies remain loadable.
* Whether old and new trial scores remain comparable.

## 10.4 Pruning

Pruning is an efficiency mechanism, not a quality mechanism.

Do not introduce aggressive pruning unless a bounded control proves that it does not systematically remove:

* Late-recovering strategies;
* Low-frequency strategies;
* Strategies whose profitability appears later in the sample; or
* Strategies with initially poor but ultimately strong equity paths.

Compare a bounded pruned study against an unpruned control before accepting a new pruning policy.

## 10.5 Reproducibility

Record:

* Sampler type.
* Sampler seed.
* Pruner type.
* Study name.
* Storage.
* Search-space version.
* Objective version.
* Metric-contract version.
* Data fingerprint.
* Config hash.
* Numba kernel version.
* Trial number.
* Trial parameters.
* Trial state.
* Rejection reason.
* Final selected candidate.

## 10.6 Bounded Optimization Only

Do not run the full optimization sweep.

Allowed:

* Small representative studies.
* One or a few selected symbols.
* One or a few selected timeframes.
* Fixed trial budgets.
* Fixed time budgets.
* Resumable studies.
* Targeted parameter sensitivity tests.
* Controlled objective comparisons.

Before starting, record the budget in:

```text
agent_audit/performance/EXPERIMENT_QUEUE.md
```

A bounded result may support or reject a hypothesis. It must not be represented as proof that the global optimum has been found.

---

# 11. Numba Compatibility Contract

Any logic used inside optimization or backtest hot paths must be compatible with Numba or precomputed safely outside the compiled loop.

Inspect at minimum:

* `pm_core.Backtester`
* `pm_core.Backtester._run_python_loop`
* Existing Numba backtest path
* `_round_volume_numba`
* Trade result conversion
* Indicator and feature arrays
* Strategy decision functions
* Risk and cost calculations used during trials

## 11.1 Reference First, Compiled Second

For every material trading logic change:

1. Implement or identify a clear Python reference implementation.
2. Verify correctness with deterministic fixtures.
3. Implement the Numba path.
4. Compare Python and Numba results.
5. Benchmark only after parity is established.

Do not begin with a speed-optimized implementation whose correctness cannot be independently checked.

## 11.2 Nopython Requirement

Hot-path implementations must compile in Numba nopython mode.

Do not silently fall back to object mode.

Avoid inside compiled loops:

* Pandas objects.
* Python dictionaries.
* Dynamic class instances.
* Arbitrary Python callbacks.
* Unsupported exception patterns.
* Variable-type returns.
* Object arrays.
* Dynamic string operations.
* Per-trial function generation that triggers unnecessary recompilation.

Convert data to stable primitive arrays before entering the compiled path.

## 11.3 Stable Trial Types

Optuna changes parameter values frequently.

Do not allow parameter values to change Numba types between trials.

Use stable scalar and array types so that:

* New compilation is not triggered for every trial.
* Parameter values are passed as data rather than captured into dynamically generated functions.
* Compiled kernels can be reused across trials.
* Study runtime is not dominated by repeated JIT compilation.

Record compilation time separately from steady-state execution time.

## 11.4 Numeric Accuracy

Use sufficient precision for:

* Prices.
* Tick calculations.
* Position sizing.
* PnL.
* Drawdown.
* Risk.
* Commission.
* Swap.
* Slippage.
* Currency conversion.
* Optimization scores.

Do not change `float64` calculations to lower precision solely for speed unless exact trade-decision and financial parity are proven.

Do not enable `fastmath` merely to improve benchmarks. It may be used only if tests prove that it does not change:

* Entry decisions.
* Exit decisions.
* Stop/target ordering.
* Position sizes.
* Trade sequence.
* PnL within required monetary precision.
* Winner selection.

## 11.5 Python/Numba Parity Tests

For deterministic fixtures, require:

* Identical number of trades.
* Identical entry timestamps.
* Identical exit timestamps.
* Identical direction.
* Identical exit reason.
* Identical parameter interpretation.
* Identical blocked/accepted decisions.
* Identical stop and target behavior.
* Price differences no larger than justified broker tick precision.
* PnL differences no larger than justified account-currency rounding.
* Equivalent metric outputs.
* Equivalent MAE/MFE outputs.
* Equivalent cost deductions.

Test edge cases:

* No trades.
* One trade.
* All winning trades.
* All losing trades.
* No-loss profitable sample.
* Breakeven trades.
* Stop and target touched in the same bar.
* Very small stop distance.
* Minimum lot.
* Maximum lot.
* Commission.
* Spread.
* Slippage.
* Swap.
* Missing or invalid bars.
* Extreme but valid parameters.
* Conditional parameters disabled.
* Sparse-trade strategies.

Any material parity failure blocks acceptance.

## 11.6 Memory and Performance

After parity is proven, improve efficiency through safe mechanisms such as:

* Preallocation.
* Reusing arrays.
* Caching immutable features.
* Avoiding repeated indicator calculation across trials.
* Separating data preparation from trial evaluation.
* Reusing compiled kernels.
* Avoiding Python-level loops inside trial evaluation.
* Passing stable arrays and scalar parameters.
* Avoiding unnecessary result materialization.
* Storing per-trade aggregates instead of full paths where appropriate.
* Batching compatible calculations.
* Profiling before optimizing.

Do not reduce data fidelity or skip costs to obtain faster benchmark results.

---

# 12. Change Proposal Contract

Every proposed or implemented change must have a register entry in:

```text
agent_audit/performance/FIX_REGISTER.md
```

Use:

```markdown
# FIX-###: <title>

## Status
Proposed / Implementing / Partner Review / Accepted / Rejected / Deferred

## Category
Entry / Exit / Risk / Cost / Execution / Metric / Optimization / Numba / Reporting / Data / Portfolio

## Direct profitability mechanism
Explain exactly how this may increase or protect net profit.

## Evidence
Code, logs, trades, historical results, or calculation inconsistency.

## Exact code locations
Files, classes, functions, and relevant configuration.

## Proposed implementation
Specific code-level change.

## Optuna compatibility
- Search-space impact
- Objective impact
- Trial metadata impact
- Serialization impact
- Resume compatibility
- Winner-loading impact

## Numba compatibility
- Hot-path impact
- Types
- Python reference path
- Compiled path
- Parity test
- Compile/runtime implications

## Expected trading impact
- Net profit
- Drawdown
- Recovery
- Trade count
- Expectancy
- Costs
- Execution

## Risk of unintended consequences
What profitable behavior might this change damage?

## Minimum bounded experiment
Exact baseline/candidate comparison.

## Compute budget
Trials, symbols, timeframes, and expected scope.

## Results
Populate after testing.

## Verdict
Accept / Reject / Needs More Evidence / Measurement Only

## Partner review
Reviewer, agreement/disagreement, and comments.
```

Vague recommendations are not acceptable.

---

# 13. Implementation Order

Use the following default implementation order unless repository evidence justifies a better sequence.

## P0: Measurement and Correctness Foundations

1. Canonicalize metrics used by the optimizer, engine, tools, and reporting.
2. Fix confirmed formula inconsistencies.
3. Add Python/Numba parity coverage.
4. Add MAE/MFE instrumentation.
5. Add expected-versus-fill slippage attribution.
6. Verify commission, spread, swap, tick, pip, and currency calculations.
7. Confirm that Optuna-selected parameters reach the actual strategy implementation.
8. Confirm that the compiled path and Python path interpret every parameter identically.

These are not academic exercises. They are required to know whether the system is truly making money and why.

## P1: Strategy Profit Extraction

1. Analyze entry quality.
2. Analyze exit quality.
3. Analyze MFE giveback.
4. Analyze high-MAE outcomes.
5. Analyze session and regime behavior.
6. Analyze direction-specific performance.
7. Analyze symbol/timeframe contribution.
8. Analyze cost-sensitive strategies.
9. Identify exact fix points.
10. Implement small, reversible candidate changes.

## P2: Optuna Winner-Selection Quality

1. Trace the existing objective mathematically.
2. Identify where the objective selects the wrong economic winner.
3. Correct metric inputs.
4. Add necessary validity constraints.
5. Improve parameter spaces where current bounds or dependencies are poor.
6. Compare existing and candidate ranking on a bounded study.
7. Ensure changes remain compatible with fallback search and artifact loading.

## P3: Efficiency

Only after P0-P2:

1. Profile optimization runtime.
2. Profile feature generation.
3. Profile Python overhead.
4. Profile Numba compilation.
5. Identify repeated work across trials.
6. Cache and precompute safely.
7. Reduce memory allocation.
8. Improve pruning only after proving search-quality parity.
9. Choose the faster implementation only when trading quality is equivalent.

---

# 14. Controlled Experiment Standard

Every candidate change must be compared against the canonical baseline.

Use a scoreboard:

```markdown
| Metric | Baseline | Candidate | Delta | Better/Worse | Interpretation |
|---|---:|---:|---:|---|---|
| Net profit after costs | | | | | |
| Total return | | | | | |
| Maximum equity drawdown | | | | | |
| Recovery factor | | | | | |
| Recovery duration | | | | | |
| Profit factor | | | | | |
| Expectancy dollars | | | | | |
| Expectancy R | | | | | |
| Average net trade | | | | | |
| Trade count | | | | | |
| Trades per time unit | | | | | |
| Win rate | | | | | |
| Average win | | | | | |
| Average loss | | | | | |
| MFE retained | | | | | |
| MAE | | | | | |
| Total spread cost | | | | | |
| Total commission | | | | | |
| Total swap | | | | | |
| Total slippage | | | | | |
| Runtime per trial | | | | | |
| Steady-state backtest runtime | | | | | |
| JIT compilation time | | | | | |
| Peak memory | | | | | |
| Python/Numba parity | | | | | |
```

Use one of these verdicts:

* `ACCEPT`
* `REJECT`
* `NEEDS MORE EVIDENCE`
* `MEASUREMENT-ONLY CHANGE`
* `EFFICIENCY-ONLY ACCEPTANCE`

An efficiency-only change may be accepted only if economic and trading outputs are equivalent within a justified tolerance.

Do not accept a candidate merely because one metric improves.

Do not accept a candidate that improves Sharpe while materially reducing net profit, expectancy, or profitable trade flow without a clearly justified economic trade-off.

Do not reject a candidate merely because its drawdown remains aggressive if the drawdown is demonstrably compensated by substantially greater net value.

---

# 15. Optuna Experiment Selection

Use representative bounded studies.

The subset should include, where available:

* A liquid major FX symbol such as EURUSD.
* A strategy or symbol with strong existing performance.
* A strategy or symbol associated with high drawdown.
* A different timeframe or strategy family to detect over-specialized changes.
* A cost-sensitive or execution-sensitive example.

Do not automatically run every symbol.

Do not run the full optimization.

For each study:

* Fix the trial budget in advance.
* Use resumable storage where available.
* Log the exact command.
* Warm Numba kernels before steady-state timing.
* Keep baseline and candidate budgets equal.
* Record failed and pruned trials.
* Report whether the result is representative or narrow.
* Stop when the predeclared budget is reached.

---

# 16. Efficiency Acceptance Rules

Efficiency is valuable, but secondary.

A faster change is acceptable when:

* Trading decisions remain identical; or
* Financial outputs remain economically equivalent under a justified tolerance; and
* Winner rankings remain materially equivalent; and
* No cost or path information is discarded; and
* No Numba object-mode fallback occurs; and
* Code quality remains acceptable.

Efficiency improvements should target:

* Repeated feature calculation.
* Repeated data conversion.
* Repeated JIT specialization.
* Python overhead per trial.
* Unnecessary allocations.
* Redundant metric calculation.
* Repeated loading of immutable data.
* Avoidable serialization overhead.
* Safe parallelism.
* Safe caching.
* Better Optuna study resumption.
* Carefully validated pruning.

Reject efficiency changes that:

* Use lower precision without proof.
* Change bar data.
* Drop commission, spread, swap, or slippage.
* Skip MAE/MFE.
* Reduce the data period merely to look faster.
* Alter strategy decisions.
* Cause non-deterministic winner selection without justification.
* Hide invalid trials.
* Change the objective silently.
* Trigger a new Numba compile for every trial unnecessarily.

---

# 17. Backward Compatibility and Migration

Strategy-changing implementations should be reversible and versioned.

Requirements:

* Preserve the current baseline mode.
* Put behavioral changes behind explicit configuration flags where practical.
* Add configuration schema versions where needed.
* Include parameter defaults.
* Avoid silently changing old winner behavior.
* Record whether existing Optuna studies remain resumable.
* Record whether existing winner files remain loadable.
* Provide an explicit migration path if artifacts change.
* Do not overwrite historical results.
* Do not reinterpret old metrics without identifying the old and new definitions.
* Include a result-schema version for new MAE/MFE or slippage fields.

A historical result generated with an older formula must not be silently presented as though it used the new formula.

---

# 18. Agent Collaboration Protocol

Claude Code and Codex must collaborate, not merely produce separate reports.

Use the existing:

```text
agent_audit/
```

Create:

```text
agent_audit/performance/
  PERFORMANCE_MANDATE.md
  RECOMMENDATION_TRIAGE.md
  BASELINE_10000.md
  FIX_REGISTER.md
  EXPERIMENT_QUEUE.md
  OPTUNA_NUMBA_COMPATIBILITY.md
  IMPLEMENTATION_LOG.md
  PERFORMANCE_COMPARISONS.csv
  ACCEPTED_FIXES.md
  REJECTED_FIXES.md
  OPEN_QUESTIONS.md
  PARTNER_REVIEW.md
  FINAL_PERFORMANCE_REPORT.md
  FINAL_PERFORMANCE_REPORT.html
```

## Suggested responsibilities

### Codex

Primary bias:

* Exact code tracing.
* Implementation.
* Unit tests.
* Python/Numba parity tests.
* Optuna integration.
* Bounded study execution.
* Benchmarking.
* Diff generation.
* Result tables.

### Claude Code

Primary bias:

* Strategy-theory analysis.
* Entry/exit critique.
* Profit and loss attribution.
* Objective-function critique.
* Counter-hypotheses.
* Detection of unintended consequences.
* Review of Codex patches.
* Review of whether measured improvements are artifacts.

These are primary biases, not exclusive responsibilities.

## Mandatory review cycle

For every material change:

1. One agent creates the fix entry.
2. The other agent challenges the stated profitability mechanism.
3. The proposing agent implements a bounded version.
4. The partner reviews the diff.
5. Both inspect test and performance results.
6. Both inspect Optuna and Numba compatibility.
7. The finding is marked accepted, rejected, or unresolved.
8. Only accepted changes enter the final implementation plan.

Use `LOCKS.md` before editing shared source files or shared reports.

Do not concurrently edit the same source file.

Do not accept a change without partner review unless it is explicitly marked unreviewed.

---

# 19. Required Source Inspection

At minimum, inspect the relevant portions of:

```text
pm_core.py
pm_pipeline.py
pm_optuna.py
pm_main.py
pm_position.py
pm_mt5.py
pm_live_readiness.py
pm_dashboard/analytics.py
tools/benchmark.py
config.json
pm_configs.json
```

Pay particular attention to:

```text
pm_core.Backtester.run
pm_core.Backtester._run_python_loop
the Numba backtest implementation
pm_core.Backtester._calculate_metrics
pm_core._round_volume_numba
pm_core.InstrumentSpec
pm_pipeline.RegimeOptimizer.optimize_symbol
pm_pipeline.RegimeOptimizer scoring and promotion logic
pm_optuna.HyperparameterOptimizer.optimize
pm_optuna.HyperparameterOptimizer._fallback_random_search
pm_main.ProductionLiveTrader._execute_entry
pm_position.PositionCalculator.calculate_position_size
pm_dashboard.analytics.compute_performance_metrics
tools.benchmark.evaluate_winner
```

Also inspect:

* Existing strategy classes.
* Indicator generation.
* Feature caching.
* Parameter schemas.
* Optimization result export.
* Winner reload.
* Trade record structures.
* Tests covering Python and Numba behavior.
* Existing optimization summaries.
* Existing demo/evaluation trade records.
* Historical logs.
* Existing profiler or benchmark scripts.

---

# 20. Explicitly Forbidden Outcomes

The phase is not complete if the primary output is:

* A recommendation to test more account sizes.
* A recommendation to lower risk without evidence.
* A recommendation to remove instruments because of USD 22 constraints.
* A recommendation to build account regimes.
* A recommendation to produce more academic validation reports.
* A recommendation to run the full optimization.
* A list of vague ideas without exact code locations.
* A list of code changes without before/after results.
* An Optuna parameter that is ignored in the Numba path.
* A Numba implementation with no Python parity test.
* A faster implementation that changes strategy behavior.
* A new objective that uses inconsistent metrics.
* A report that claims improvement based only on Sharpe or win rate.
* A report that fails to analyze actual entry and exit logic.
* Two independent agent reports with no cross-review.

---

# 21. Required Final Deliverables

## A. Final Performance Report

Produce:

```text
agent_audit/performance/FINAL_PERFORMANCE_REPORT.md
agent_audit/performance/FINAL_PERFORMANCE_REPORT.html
```

Required sections:

```markdown
# Final Performance Implementation Report

## 1. Executive Outcome
- What was implemented
- What improved
- What did not improve
- Confirmed profit leaks
- Confirmed calculation bugs
- Confirmed strategy fix points
- Remaining uncertainties

## 2. Canonical Baseline
- USD 10,000 baseline
- Data
- Costs
- Risk
- Symbols/timeframes
- Optuna settings
- Numba settings

## 3. Existing Recommendation Triage
- Keep
- Reframe
- Defer
- Reject
- Superseded

## 4. Strategy Profit Anatomy
- Entry findings
- Exit findings
- Regime findings
- Symbol/timeframe findings
- Cost findings

## 5. High-Drawdown Trade Findings
- Profitable pain
- Bad risk
- Entry problems
- Exit problems
- Execution problems
- Evidence limitations

## 6. Calculation and Metric Corrections
- Exact old formula
- Exact new formula
- Trading/selection impact

## 7. Implemented Changes
| Fix ID | Files | Mechanism | Optuna Impact | Numba Impact | Result | Verdict |

## 8. Performance Comparisons
Full before/after tables.

## 9. Optimization Quality
- Current objective
- Changes
- Trial behavior
- Winner-ranking changes
- Search-space changes

## 10. Numba Quality and Performance
- Python/Numba parity
- Compilation mode
- Runtime
- Memory
- Recompilation behavior
- Remaining bottlenecks

## 11. Accepted Fixes

## 12. Rejected Fixes
Explain why plausible ideas failed.

## 13. Next Highest-Value Experiments
Only direct, bounded, performance-relevant work.

## 14. Partner Review
Agreement, disagreement, and unresolved findings.

## 15. Exact Commands, Files, Configs, and Artifacts
```

## B. Optimization Compatibility Report

Produce:

```text
agent_audit/performance/OPTUNA_NUMBA_COMPATIBILITY.md
```

Include:

* Every modified parameter.
* Optuna integration path.
* Fallback-search integration.
* Search-space version.
* Trial metadata changes.
* Winner serialization changes.
* Live reload changes.
* Numba kernel changes.
* Python/Numba parity results.
* Compilation results.
* Runtime results.
* Memory results.
* Backward compatibility.
* Migration requirements.
* Unresolved incompatibilities.

## C. Performance Comparison Data

Produce:

```text
agent_audit/performance/PERFORMANCE_COMPARISONS.csv
```

Every row must identify:

* Fix ID.
* Baseline or candidate.
* Commit/config fingerprint.
* Symbol.
* Timeframe.
* Strategy.
* Data fingerprint.
* Starting balance.
* Trial budget.
* Seed.
* Numba status.
* Net profit.
* Return.
* Drawdown.
* Recovery.
* PF.
* Expectancy.
* Trade count.
* Costs.
* MAE/MFE summaries.
* Runtime.
* Memory.
* Verdict.

## D. Accepted and Rejected Fixes

Produce:

```text
agent_audit/performance/ACCEPTED_FIXES.md
agent_audit/performance/REJECTED_FIXES.md
```

Rejected experiments are valuable and must be documented so they are not repeated.

---

# 22. Completion Standard

This phase is complete only when:

* The USD 10,000 canonical baseline is documented.
* No balance sweep was run.
* No full optimization was run.
* Existing suggestions were triaged against the new performance mandate.
* Actual strategy entry and exit code was analyzed.
* Exact profit and loss mechanisms were identified.
* Metric inconsistencies were resolved or explicitly isolated.
* MAE/MFE was implemented or a precise blocking reason was documented.
* Slippage attribution was implemented or a precise blocking reason was documented.
* Every strategy-affecting change was integrated with Optuna.
* Every hot-path change was integrated with Numba or safely precomputed outside it.
* Python/Numba parity was tested.
* Bounded baseline/candidate comparisons were performed.
* Efficiency was evaluated only after correctness and quality.
* Both agents reviewed each other’s work.
* Accepted and rejected changes were distinguished.
* Final reports exist in Markdown and HTML.
* Every claim is supported by code, logs, historical results, or a controlled experiment.

The final question is not:

```text
How academically impressive is this system?
```

The final questions are:

```text
Does it trade correctly?

Does it make more money after realistic costs?

Where exactly is it gaining or leaking value?

Which code changes improve the result?

Are those changes compatible with Optuna and Numba?

Do the Python and Numba paths produce the same trading behavior?

Can the same improvement be implemented more efficiently without reducing quality?
```

Proceed now with the performance-first analysis and implementation phase.

```
```
