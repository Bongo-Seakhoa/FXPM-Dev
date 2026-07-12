# FxPM timeframe evidence and specialist competition design

**Status:** implementation specification; no production behavior is changed by this document

**Date:** 2026-07-12

**Scope:** independently sourced M5, M15, M30, H1, H4 and D1 evidence; specialist selection; hierarchical capital allocation

**Decision boundary:** this design can make the evidence chain more honest and the allocation problem better posed. It cannot guarantee that any cell, symbol or portfolio will be profitable.

## 1. Executive decision

FxPM should retain the symbol x timeframe x regime specialist hypothesis, but it should stop treating six very different sampling horizons as though the same number of calendar years, raw bars and trades carries the same information.

The production design should have four distinct layers:

1. **Independent timeframe sources.** Request and persist each of M5, M15, M30, H1, H4 and D1 separately from the terminal or an approved vendor. Do not infer M15-D1 from one M5 tail for production research.
2. **Timeframe-specific cell evidence.** Each timeframe receives its own minimum calendar span. Raw bars are checked against the instrument's actual session calendar, while statistical sufficiency is determined from effective independent observations and power, not raw bar count alone.
3. **Common-clock competition.** Frozen specialists are compared on synchronized daily mark-to-market net returns and capital occupancy. Raw return, raw trade count and native timeframe do not receive an automatic advantage.
4. **Hierarchical allocation.** Specialists propose trades; symbol controllers coordinate them; the institution allocates the shared balance sheet under empirically calibrated stress, drawdown, margin and concentration constraints.

The current global `5y/2y/2y` contract is a reasonable starting point for M5/M15. Its five-year **training** component is too short for D1 and often H4 after four-way regime partitioning. The recent tuning and final windows should remain concurrent across all specialists so they are compared on the same market history. The current global trade floors are also too weak to establish comparable precision across cells.

## 2. Requirements versus calibration

This distinction is mandatory throughout implementation.

### 2.1 Evidence-backed structural requirements

These are hard production requirements:

- Every production timeframe has its own requested source artifact, provenance, source hash, UTC interval semantics and acquisition audit.
- All research and ranking inputs are closed-bar, point-in-time and earlier than the decision they influence.
- Train, tuning and final windows are chronological, purged and immutable. Tuning is a selection surface; final is not.
- A whole symbol is not rejected because one timeframe is unusable. Eligibility and `NO_TRADE` are cell-level decisions.
- Session completeness is evaluated against versioned broker/exchange session segments, including documented session-policy changes; it is not inferred from one stationary full-history rate.
- Raw bars are a data-integrity measure, not a count of independent experiments.
- Autocorrelation, overlapping holdings and clustered opportunities reduce effective sample size.
- Search breadth and correlated trials are recorded and used in multiple-testing controls.
- Costs, holding time, risk, correlation and capital occupancy are included before specialists compete for shared capital.
- A capital cap is selected on tuning evidence and frozen before final evaluation. Final evidence may reject the cap but may not choose a replacement.
- Missing or underpowered evidence means abstention. A default parameter set is not a substitute for evidence.

### 2.2 Empirical calibration choices

The following values cannot be justified universally from theory and must be estimated or declared as risk appetite:

- exact calendar years above the minimum design priors below;
- the smallest economically meaningful net edge, in R or basis points;
- desired confidence-interval width and test power;
- autocorrelation truncation/block length;
- strategy decay half-life and retraining cadence;
- posterior shrinkage strength;
- spread, commission, slippage, market-impact and swap stresses;
- total, symbol and factor risk caps;
- drawdown, gap-loss, margin and ruin tolerances;
- the utility weights applied to return, tail risk and capital occupancy.

These values must be written to immutable calibration artifacts with inputs, estimation window, code hash and uncertainty. They must not be hidden as unexplained constants in `config.json`.

## 3. Independent timeframe history

### 3.1 Source contract

For each `symbol x timeframe`, acquisition must issue a distinct range request using the requested MT5 timeframe identifier and the same information cutoff. MetaTrader's Python `copy_rates_range` accepts a timeframe and UTC date range, returns the bar fields for that request, and notes that the terminal only exposes history available on charts and subject to the terminal's bar limit. The terminal setting is therefore a ceiling, not proof that deep intraday history exists. MT5 officially defines M5, M15, M30, H1, H4 and D1 as separate timeframe identifiers.

Production terminology should be precise: these are **independently requested terminal timeframes**, not necessarily exchange-originated bars. The audit must detect when the broker/terminal returns one daily-like record for an intraday request or identical records across timeframe files.

Each artifact must include:

```text
canonical_symbol
broker_symbol
timeframe
source_provider
source_account/server
requested_start_utc
requested_end_exclusive_utc
actual_first_open_utc
actual_last_close_utc
bar_interval_semantics
terminal_build/API_version
terminal_max_bars
rows
sha256
session_calendar_id + version
source_schedule_segments[]
pagination pages/retries/empty/truncated diagnostics
resolution diagnostics
quality diagnostics
```

### 3.2 Resolution and identity audit

An intraday cell fails source integrity when any material historical segment has one or more of these properties without an explicit market-calendar explanation:

- median bars per active session is compatible with D1 rather than the requested timeframe;
- M5, M15, M30, H1 or H4 timestamps and OHLCV are substantially identical to D1;
- nominal timeframe spacing is absent across a sustained open-session segment;
- a large unexplained internal gap intersects train, tuning or final;
- the provider changes session or symbol definition without a versioned change point.

The audit result is per cell. A valid D1 cell may proceed while the same symbol's historical M5 cell remains `SOURCE_RESOLUTION_UNUSABLE`.

### 3.3 Session-aware completeness

Replace `train_only_robust_weekly_session_rate_v1` as the production authority with a segmented session contract:

1. Start from broker symbol-session metadata where available and an approved exchange calendar for exchange-traded instruments.
2. Detect candidate schedule change points using only source timestamps and metadata, never returns or strategy performance.
3. Review/freeze each segment as `calendar_id`, `effective_start`, `effective_end`, weekdays, session intervals, holidays and expected bar alignment.
4. Measure missing bars within expected open intervals for each segment.
5. Report point coverage, a lower confidence bound, maximum unexplained contiguous gap and which partition is affected.

No single 95% number should be hard-pinned across FX, US equities, indices, metals, energy and 24/7 crypto. The pass rule should be a versioned policy artifact per asset/session family. A threshold change requires a new activation and may never be selected from strategy performance.

## 4. Timeframe-specific evidence horizon

### 4.1 Minimum calendar-span priors

The table below is the **initial engineering prior** for planning acquisition. It is not evidence that these exact spans are optimal, nor does reaching them promote a cell. The power and effective-sample gates in Sections 5-6 remain binding.

| Timeframe | Train minimum | Tuning minimum | Final cell lockbox minimum | Total excluding purge/warmup | Rationale |
|---|---:|---:|---:|---:|---|
| M5 | 5 years | 2 years | 2 years | 9 years | Many events are available, but microstructure/cost regimes change quickly. More old data is not automatically more relevant. |
| M15 | 5 years | 2 years | 2 years | 9 years | Similar market-cycle breadth to M5 with fewer overlapping opportunities. |
| M30 | 6 years | 2 years | 2 years | 10 years | Fewer decisions per regime justify a longer train span. |
| H1 | 7 years | 2 years | 2 years | 11 years | Requires broader training-cycle coverage while retaining the common recent comparison clock. |
| H4 | 8 years | 2 years | 2 years | 12 years | Sparse regime-event counts become binding before raw bars do. |
| D1 | 12 years | 2 years | 2 years | 16 years | Daily bars and regime-specific trades accumulate slowly; a ten-year source may be inadequate after four-way regime partitioning. |

All windows end at one shared UTC information cutoff. **Only the training start differs by timeframe.** Tuning and final boundaries are identical for all six timeframes, preserving one concurrent recent comparison. Purge length is `max(label horizon, maximum modeled holding horizon, feature dependency horizon)` rather than an unrelated fixed number of bars.

The common two-year frozen final interval is used for both cell verdicts and synchronized all-cell portfolio replay. Every candidate is frozen before any final observation is evaluated. Older timeframe-specific history belongs to training and outer walk-forward diagnostics; it cannot give H1/H4/D1 a different final market period or create a survivors-only portfolio before the common verdict.

### 4.2 Illustrative expected bars, not hard gates

The values below show why a universal bar floor is inappropriate. They are arithmetic expectations under two simplified schedules: 24 hours x five days/week x 52 weeks, and 252 US equity regular sessions/year with 6.5 hours/session. Actual gates must use the versioned session calendar.

| Timeframe | Train years | Expected train bars, 24x5 | Expected train bars, US regular session | Tuning/final years | Expected bars per tuning/final, 24x5 | Expected bars per tuning/final, US regular session |
|---|---:|---:|---:|---:|---:|---:|
| M5 | 5 | 374,400 | 98,280 | 2 | 149,760 | 39,312 |
| M15 | 5 | 124,800 | 32,760 | 2 | 49,920 | 13,104 |
| M30 | 6 | 74,880 | 19,656 | 2 | 24,960 | 6,552 |
| H1 | 7 | 43,680 | 12,348 | 2 | 12,480 | 3,528 |
| H4 | 8 | 12,480 | 4,032 | 2 | 3,120 | 1,008 |
| D1 | 12 | 3,120 | 3,024 | 2 | 520 | 504 |

For a cell, record both:

```text
raw_bars_observed
session_bars_expected
session_coverage_point
session_coverage_lower_confidence_bound
largest_unexplained_open_session_gap
calendar_years_observed
```

The raw-bar requirement is the session-calendar expectation after approved closures and schedule segments. An optional small absolute sanity floor may catch malformed calendars, but it cannot authorize a cell by itself.

### 4.3 D1 longevity

A D1 file containing 2,500 bars is roughly ten years for a weekday market, not 2,500 independent strategy trials. After train/tuning/final, four regimes, persistent signals and multi-day holdings, a D1 specialist may have only a few dozen independent trades in one regime.

Therefore:

- D1 should preferentially use 15-25 years of trustworthy daily history when provider and instrument longevity allow it.
- Twelve/two/two years is the acquisition planning minimum, not an automatic pass; D1 statistical power may still require an earlier training start or abstention.
- If instrument inception makes that impossible, compute power from the available span. Do not backfill with synthetic/resampled observations and do not lower the confidence requirement merely to retain the symbol.
- Cross-symbol hierarchical priors may stabilize estimates, but borrowed strength must not be counted as independent D1 validation trades.
- The D1 cell abstains when its own final evidence remains underpowered.

## 5. Effective independent observations

### 5.1 Why raw bars and raw trades overstate information

Adjacent bars share state, regime labels persist, positions overlap and strategies often wait for similar conditions. Lo's Sharpe-ratio analysis shows that serial correlation changes uncertainty and can materially change rankings. Newey-West provides heteroskedasticity-and-autocorrelation-consistent covariance estimation. Geyer's work formalizes integrated autocorrelation time/effective sample size, and the stationary bootstrap preserves weak dependence by resampling blocks rather than isolated observations.

For an ordered series `x[1:n]`, compute an autocorrelation-time estimate:

```text
tau_int = max(1, 1 + 2 * sum_k rho_k)
n_eff_acf = n / tau_int
```

The summation must use a deterministic, finite rule such as Geyer's initial-positive/initial-monotone sequence. Do not sum noisy high-lag autocorrelations indefinitely.

Also compute a HAC-based effective count for the sample mean:

```text
omega_hac = NeweyWestLongRunVariance(x)
n_eff_hac = n * sample_variance(x) / omega_hac
n_eff = min(n, n_eff_acf, n_eff_hac, n_eff_overlap)
```

`n_eff_overlap` accounts for simultaneous/overlapping holding intervals. One implementation is a graph where trades are connected when their risk intervals overlap or share the same causal signal event; cluster-robust mean variance supplies the corresponding effective count.

Calculate ESS separately for:

- bar/feature observations used by the detector;
- daily net mark-to-market returns used for portfolio inference;
- per-trade net R outcomes used for cell inference;
- regime episodes, where one long persistent regime is not hundreds of independent regime observations.

### 5.2 Required artifact fields

Every partition/regime record must include:

```text
raw_observations
raw_trades
unique_regime_episodes
acf_truncation_method
acf_lags_used
integrated_autocorrelation_time
hac_kernel
hac_bandwidth_method
overlap_cluster_count
effective_observations
effective_trades
effective_regime_episodes
```

An effective count may never exceed its raw count. Failure to estimate dependence is `EFFECTIVE_SAMPLE_UNAVAILABLE`, not an IID fallback.

## 6. Minimum trades per regime: power-derived, not timeframe folklore

### 6.1 Binding rule

The production configuration should not contain unexplained values such as “15 validation trades is enough.” For each `symbol x timeframe x regime`, determine the required effective trades from the smallest net edge worth deploying.

For mean net R, a planning approximation is:

```text
n_eff_power = ceil(((z_(1-alpha_adj) + z_power) * sigma_R / delta_R)^2)
n_eff_precision = ceil((z_(1-alpha/2) * sigma_R / ci_half_width_R)^2)
```

where:

- `delta_R` is the smallest economically meaningful mean R after all costs;
- `sigma_R` is a robust pilot dispersion estimated without final data;
- `alpha_adj` reflects the effective number of strategy/parameter trials;
- `power` is the declared probability of detecting `delta_R`;
- `ci_half_width_R` is the maximum acceptable uncertainty.

Then:

| Partition | Minimum per regime |
|---|---|
| Train | `effective_trades >= n_eff_power(alpha_adj, power, delta_R, sigma_R)` and enough regime episodes to support the detector/search |
| Tuning | `effective_trades >= n_eff_precision(alpha_tuning, ci_half_width_R, sigma_R)` plus dependence-aware lower net-edge bound above zero |
| Final | The frozen tuning requirement recomputed without parameter changes; final lower net-edge bound above zero and block-bootstrap survival |

Raw-trade requirements are derived, not separately guessed:

```text
raw_trades_required = ceil(effective_trades_required * tau_trade)
```

with an additional requirement for enough independent overlap clusters and bootstrap blocks.

### 6.2 Numerical illustration

This example is explanatory, not a production default. If `sigma_R=1.2`, `delta_R=0.25`, one-sided `alpha=0.05` and power `0.80`, the planning requirement is approximately 143 **effective** train trades. A 95% confidence half-width of 0.25R requires approximately 89 effective tuning/final trades. If `tau_trade=2`, the corresponding raw counts are approximately 286 and 178.

The same precision requirement applies to D1 and M5. M5 merely reaches it faster. If D1 cannot reach it, the honest outcome is `NO_TRADE_UNDERPOWERED`, not a smaller evidence standard.

### 6.3 Multiple testing

The local trial ledger must count strategy families, parameter configurations, regime-detector alternatives, exits, cost treatments and selection retries. Correlated trials should produce an estimated effective trial count rather than pretending every trial is independent or counting only the final candidates.

Retain DSR and PBO, but bind them to the full local search ledger. White's Reality Check or Hansen's Superior Predictive Ability test is an appropriate additional benchmark-comparison control. The exact choice is empirical, but ordinary unadjusted `t > 2`, raw Sharpe or the best tuning return is not sufficient after a large search.

## 7. Walk-forward, half-life and retraining

### 7.1 Outer walk-forward design

Before burning the official final lockbox, run a nested chronological outer walk-forward on train+tuning only:

1. Fit detector/features/strategy parameters on the expanding or rolling train window.
2. Freeze them.
3. Score the next forward slice, including costs and daily MTM.
4. Advance by one candidate cadence.
5. Record every forecast, trade, error and retraining decision.

Candidate cadence grids should be timeframe-aware planning ranges, not fixed production schedules:

| Timeframe | Cadence candidates evaluated on train+tuning only |
|---|---|
| M5 | weekly, two-weekly, monthly, six-weekly |
| M15 | two-weekly, monthly, six-weekly, quarterly |
| M30 | monthly, six-weekly, quarterly |
| H1 | monthly, quarterly, semiannual |
| H4 | quarterly, semiannual, annual |
| D1 | semiannual, annual, two-year |

The selected cadence minimizes forward predictive loss or maximizes net certainty-equivalent return subject to turnover and stability. It is frozen before final evaluation.

### 7.2 Strategy half-life

Estimate decay from outer-fold forward outcomes, never in-sample fit:

```text
edge(age) = edge_0 * exp(-ln(2) * age / half_life)
```

Use dependence-aware regression or block bootstrap and retain a confidence interval. If decay is non-monotonic, report the model as misspecified and use change-point/conditional predictive-ability monitoring instead of forcing an exponential half-life.

McLean and Pontiff document material out-of-sample and post-publication decay across published return predictors. That evidence supports monitoring decay; it does not supply a universal FX strategy half-life. Page's sequential inspection/CUSUM framework and Giacomini-White conditional predictive-ability tests support event-driven monitoring of changed performance.

### 7.3 Live retraining rule

Replace one global 14-day/4-week authority with:

```text
retrain_due = earliest(
    frozen_max_staleness,
    selected_walk_forward_cadence,
    conservative_half_life_fraction,
    confirmed_edge_drift,
    confirmed_cost_or_execution_drift,
    source/session contract change
)
```

Drift must be tested on net R residuals, daily MTM residuals, spread/slippage/swap residuals and regime occupancy. Consecutive losses alone are not a drift test. One alarm pauses or derates the affected cell; it does not silently retune or rewrite the champion.

## 8. Fair competition across timeframes

### 8.1 Common evaluation clock

Every specialist emits a daily net mark-to-market return series on the same UTC calendar, including:

- realized and unrealized P&L;
- spread, commission, slippage and swap;
- position size and initial stop risk;
- gross and net exposure;
- margin used;
- capital/risk occupancy time;
- stale/missing marks as hard errors.

Annualization is applied to this common daily series with autocorrelation-aware uncertainty. Do not multiply a bar-level Sharpe by `sqrt(bars_per_year)` and compare it with D1. Lo's result is directly relevant: serial correlation invalidates naive scaling and can reverse rankings.

### 8.2 Horizon-normalized cell statistics

For each cell, estimate on tuning only and freeze:

```text
posterior_mean_net_R
posterior_standard_error
lower_net_R_bound
expected_holding_days
expected_stop_risk_days = E[initial_stop_risk_pct * holding_days]
net_edge_rate = posterior_mean_net_R / expected_holding_days
lower_edge_rate = lower_net_R_bound / expected_holding_days
net_profit_per_risk_day = E[net_profit] / E[initial_stop_risk * holding_days]
daily_tail_loss / expected_shortfall
turnover and cost share of gross edge
```

This prevents a slow D1 trade from winning solely on absolute return and prevents a rapid M5 strategy from winning solely on trade count. Ten profitable M5 opportunities during one D1 holding period are allowed to compete through their net daily contribution, dependence and cumulative occupancy.

### 8.3 Uncertainty shrinkage

Sparse cell means should be shrunk toward a parent prior estimated without final data:

```text
mu_cell_posterior = w * mu_cell + (1-w) * mu_parent
w = between_cell_variance / (between_cell_variance + cell_standard_error^2)
```

Parents may be `symbol x timeframe`, `asset-class x timeframe x regime` and institution-wide strategy family. The hierarchy and shrinkage estimator are frozen on train+tuning. The raw cell estimate and posterior must both remain visible. Shrinkage can size a cell more cautiously; it cannot manufacture missing final trades.

### 8.4 Marginal portfolio utility

At a decision time, compare proposals by their incremental effect on the current book:

```text
marginal_utility_i =
    expected_net_profit_i
  - risk_aversion * marginal_portfolio_variance_i
  - tail_aversion * marginal_stress_loss_i
  - occupancy_shadow_price * expected_stop_risk_days_i
  - liquidity_shadow_price * expected_execution_cost_i
```

Inputs use posterior means, conservative lower bounds and point-in-time covariance. Covariance should be estimated on synchronized daily cell returns using a shrinkage estimator; sample covariance is unstable when cells are numerous relative to observations. Ledoit-Wolf supplies a primary basis for this regularization.

No-trade is an explicit competitor with utility zero. A proposal whose conservative marginal utility is not positive waits.

## 9. Hierarchical allocation for a medium-high risk appetite

### 9.1 The hierarchy

**Specialist layer**

- One specialist owns one `symbol x timeframe x regime` cell.
- It produces direction, entry, stop, target/exit contract, expected net R distribution, holding distribution, cost distribution and evidence identity.
- It does not choose institution-level size.

**Symbol-controller layer**

- Routes only currently active regime specialists.
- Compares proposals using marginal utility and common-clock evidence.
- Permits D1 plus one lower-timeframe position when both remain beneficial after same-symbol dependence and stop-risk aggregation.
- Blocks duplicate/pyramided expression of the same causal event.
- Applies symbol liquidity, margin and topology constraints.

**Institution layer**

- Reconstructs all open stop risk from current equity and broker geometry.
- Uses gross, non-netted loss-at-stop for survival controls.
- Uses signed exposures/covariance separately for economic diversification.
- Constrains factor clusters, symbol concentration, margin, stressed gap loss, drawdown and ruin probability.
- Allocates to maximize frozen tuning-selected certainty equivalent or expected log growth.

### 9.2 Risk appetite is stated in losses, not utilization slogans

“Medium-high risk” should be encoded as owner-approved tolerances for:

```text
maximum acceptable stress loss
maximum acceptable drawdown distribution
maximum upper confidence bound on ruin probability
minimum margin level under stress
maximum symbol and factor loss contribution
minimum effective number of independent bets
```

Gross open stop risk is then an output of feasible allocation. It should not be set to 3%, 33%, 50% or 90% merely because one number feels conservative or aggressive.

The tuning cap sweep should derive its range:

```text
cap_upper = min(
    stress_loss_budget / calibrated_gap_multiple,
    margin_feasible_stop_risk,
    concentration_feasible_stop_risk
)
cap_lower = maximum feasible single-trade risk
step = min(one_quarter_single_trade_risk, numerical_resolution_limit)
```

Select the highest-utility feasible cap, not automatically the highest cap. The candidate cap, symbol/factor subcaps and fractional-Kelly multiplier are selected jointly on synchronized tuning replay. Kelly's result provides a log-growth objective under known probabilities; because trading probabilities are estimated, FxPM must use shrinkage, fractional sizing and hard survival constraints rather than full plug-in Kelly.

### 9.3 Correlation and effective bets

Use two complementary views:

- deterministic economic factor exposures for hard concentration limits;
- shrinkage covariance of synchronized cell returns for allocation and marginal risk.

Report eigenvalue/risk concentration and an effective-number-of-bets measure. Correlation cannot be treated as static, and unknown correlation is uncertainty to penalize, not assumed diversification.

## 10. Implementation-ready schema

Replace the current global evidence fields with a versioned contract referenced by hash. The following schema is intentionally explicit; `null` calibration values block production rather than invoking hidden defaults.

```json
{
  "pipeline": {
    "evidence_contract": {
      "schema_version": "fxpm-timeframe-evidence-v3",
      "source_mode": "independent_requested_timeframe",
      "shared_information_cutoff_utc": "",
      "common_portfolio_lockbox_years": 2,
      "timeframes": {
        "M5":  {"train_years": 5,  "tuning_years": 2, "final_years": 2},
        "M15": {"train_years": 5,  "tuning_years": 2, "final_years": 2},
        "M30": {"train_years": 6,  "tuning_years": 2, "final_years": 2},
        "H1":  {"train_years": 7,  "tuning_years": 2, "final_years": 2},
        "H4":  {"train_years": 8,  "tuning_years": 2, "final_years": 2},
        "D1":  {"train_years": 12, "tuning_years": 2, "final_years": 2}
      },
      "purge_mode": "max_label_feature_holding_horizon",
      "warmup_mode": "feature_graph_exact",
      "session_policy_artifact_path": "",
      "session_policy_artifact_sha256": "",
      "require_resolution_audit": true,
      "require_cross_timeframe_identity_audit": true,
      "eligibility_scope": "symbol_timeframe_cell"
    },
    "sample_dependence": {
      "schema_version": "fxpm-effective-sample-v1",
      "acf_method": "geyer_initial_monotone",
      "hac_kernel": "bartlett",
      "hac_bandwidth_mode": "data_driven",
      "overlap_adjustment": "holding_interval_clusters",
      "fail_if_unavailable": true
    },
    "trade_power": {
      "schema_version": "fxpm-cell-power-v1",
      "alpha_train": null,
      "alpha_tuning": null,
      "alpha_final": null,
      "target_power": null,
      "minimum_economic_edge_R": null,
      "maximum_ci_half_width_R": null,
      "multiple_testing_adjustment": "effective_trials",
      "pilot_dispersion": "median_absolute_deviation_R",
      "requirements_artifact_path": "",
      "requirements_artifact_sha256": ""
    },
    "walk_forward": {
      "schema_version": "fxpm-walk-forward-v2",
      "nested": true,
      "candidate_cadence_artifact_path": "",
      "candidate_cadence_artifact_sha256": "",
      "half_life_model": "outer_fold_decay_with_block_bootstrap",
      "drift_action": "pause_cell",
      "automatic_self_promotion": false
    },
    "competition": {
      "schema_version": "fxpm-common-clock-competition-v1",
      "return_clock": "daily_utc_mark_to_market",
      "return_basis": "net_after_all_costs",
      "mean_model": "hierarchical_shrinkage",
      "covariance_model": "ledoit_wolf_shrinkage",
      "holding_normalization": "stop_risk_days",
      "no_trade_is_candidate": true,
      "policy_artifact_path": "",
      "policy_artifact_sha256": ""
    },
    "capital_policy": {
      "schema_version": "fxpm-hierarchical-allocation-v1",
      "risk_appetite_label": "medium_high",
      "risk_appetite_artifact_path": "",
      "risk_appetite_artifact_sha256": "",
      "required_owner_constraints": {
        "maximum_p95_drawdown_pct": null,
        "maximum_p99_stress_loss_pct": null,
        "maximum_ruin_probability_upper_bound": null,
        "minimum_stressed_margin_level_pct": null,
        "maximum_symbol_loss_contribution_pct": null,
        "maximum_factor_loss_contribution_pct": null,
        "minimum_effective_independent_bets": null
      },
      "cap_selection_partition": "tuning_validation",
      "cap_selection_objective": "certainty_equivalent_net_daily_return",
      "derive_cap_search_bounds": true,
      "final_may_reselect": false,
      "layers": ["specialist", "symbol_controller", "institution"]
    }
  }
}
```

### 10.1 Required calibration artifact

`requirements_artifact` must materialize the dynamic results for every cell:

```json
{
  "cell_id": "EURUSD|H4|TREND",
  "pilot_window": {},
  "sigma_R_robust": 0.0,
  "minimum_economic_edge_R": 0.0,
  "alpha_adjusted": 0.0,
  "power": 0.0,
  "effective_trials": 0.0,
  "required_effective_train_trades": 0,
  "required_effective_tuning_trades": 0,
  "required_effective_final_trades": 0,
  "tau_trade": 0.0,
  "required_raw_train_trades": 0,
  "required_raw_tuning_trades": 0,
  "required_raw_final_trades": 0,
  "calibration_code_sha256": ""
}
```

Zero values in this example are placeholders. Production validation rejects nonpositive or missing calibrated values.

### 10.2 Migration from current keys

| Current key | Disposition |
|---|---|
| `evidence_window_years` | Replace with `evidence_contract.timeframes.<TF>` |
| `evidence_min_native_bars` | Replace as authority with session-policy-derived expectations; retain only as audited diagnostic/sanity floor |
| `evidence_min_expected_session_coverage` | Replace universal value with hash-bound session-family policy |
| `evidence_warmup_days` | Replace with exact maximum dependency from the frozen feature graph |
| `regime_min_train_trades` | Replace with cell power artifact |
| `regime_min_val_trades` and `regime_timeframe_val_trade_floors` | Replace with tuning/final effective-trade requirements |
| `walk_forward_audit_windows` | Replace with nested timeframe-specific cadence artifact |
| `fixed_retrain_days` / one global retrain interval | Demote to emergency maximum staleness; cadence and drift become per cell |
| `max_total_open_risk_pct` | Populate only from the frozen tuning capital-policy artifact |
| `max_factor_group_open_risk_pct` | Jointly calibrate with total/symbol caps and stress constraints |

## 11. Implementation sequence and acceptance tests

1. **Source v3:** independently acquire all timeframe files; add session-segment and cross-timeframe identity audits.
2. **Cell eligibility:** change activation from whole-symbol to cell-level, preserving explicit reasons.
3. **Evidence v3:** add timeframe windows, feature-graph warmup and dynamic session expectations.
4. **Dependence:** implement ACF/HAC/overlap ESS with property tests and independent numerical fixtures.
5. **Power:** generate immutable per-cell trade requirements before strategy search.
6. **Walk-forward:** generate the cadence/half-life artifact entirely from train+tuning.
7. **Competition:** construct common daily MTM cell returns, shrinkage means/covariance and occupancy metrics.
8. **Allocation:** calibrate hierarchical policy on synchronized tuning replay; freeze it.
9. **Final:** burn cell and common portfolio lockboxes once; all-or-none portfolio verdict.
10. **Demo:** reconcile fills, costs, swap, margin, topology, restarts and cell attribution before funded authorization.

Minimum acceptance tests:

- requesting D1 cannot read or resample the M5 artifact;
- a daily-like M5 history fails only M5, not D1 or the whole symbol;
- a documented session change does not appear as missing history;
- ESS falls under positive serial correlation and overlapping holdings;
- final data cannot alter required trade counts, cadence, shrinkage or cap selection;
- M5 and D1 with identical daily net P&L receive identical return statistics before occupancy/risk differences;
- adding a highly correlated specialist cannot create free diversification;
- a slow position pays an explicit capital-occupancy cost;
- no-trade wins whenever conservative marginal utility is nonpositive;
- infrastructure failure cannot overwrite a prior winner with a semantic `NO_TRADE` verdict.

## 12. Primary references

### MetaTrader 5 official documentation

- MetaQuotes, [`copy_rates_range`](https://www.mql5.com/en/docs/python_metatrader5/mt5copyratesrange_py): requested timeframe/date range, UTC semantics, returned fields and chart-history limitation.
- MetaQuotes, [Chart Timeframes / `ENUM_TIMEFRAMES`](https://www.mql5.com/en/docs/constants/chartconstants/enum_timeframes): official identifiers for M5, M15, M30, H1, H4 and D1.
- MetaQuotes, [Timeseries and Indicators Access](https://www.mql5.com/en/docs/series): official terminal timeseries access model.

### Statistical inference and selection

- Geyer (1992), [Practical Markov Chain Monte Carlo](https://projecteuclid.org/journals/statistical-science/volume-7/issue-4/Practical-Markov-Chain-Monte-Carlo/10.1214/ss/1177011137.full), *Statistical Science* 7(4), 473-483.
- Newey and West (1987), [A Simple, Positive Semi-Definite, Heteroskedasticity and Autocorrelation Consistent Covariance Matrix](https://www.jstor.org/stable/1913610), *Econometrica* 55(3), 703-708.
- Lo (2002), [The Statistics of Sharpe Ratios](https://doi.org/10.2469/faj.v58.n4.2453), *Financial Analysts Journal* 58(4), 36-52.
- Politis and Romano (1994), [The Stationary Bootstrap](https://doi.org/10.1080/01621459.1994.10476870), *Journal of the American Statistical Association* 89(428), 1303-1313.
- White (2000), [A Reality Check for Data Snooping](https://doi.org/10.1111/1468-0262.00152), *Econometrica* 68(5), 1097-1126.
- Hansen (2005), [A Test for Superior Predictive Ability](https://www.jstor.org/stable/27638834), *Journal of Business & Economic Statistics* 23(4), 365-380.
- Bailey and Lopez de Prado (2014), [The Deflated Sharpe Ratio](https://papers.ssrn.com/sol3/papers.cfm?abstract_id=2460551), *Journal of Portfolio Management* 40(5), 94-107.
- Bailey, Borwein, Lopez de Prado and Zhu (2015), [The Probability of Backtest Overfitting](https://papers.ssrn.com/sol3/papers.cfm?abstract_id=2326253), *Journal of Computational Finance*.
- Harvey, Liu and Zhu (2016), [...and the Cross-Section of Expected Returns](https://academic.oup.com/rfs/article/29/1/5/1843824), *Review of Financial Studies* 29(1), 5-68.

### Drift, costs and allocation

- Page (1954), [Continuous Inspection Schemes](https://doi.org/10.1093/biomet/41.1-2.100), *Biometrika* 41(1-2), 100-115.
- Giacomini and White (2006), [Tests of Conditional Predictive Ability](https://doi.org/10.1111/j.1468-0262.2006.00718.x), *Econometrica* 74(6), 1545-1578.
- McLean and Pontiff (2016), [Does Academic Research Destroy Stock Return Predictability?](https://doi.org/10.1111/jofi.12365), *Journal of Finance* 71(1), 5-32.
- Novy-Marx and Velikov (2016), [A Taxonomy of Anomalies and Their Trading Costs](https://www.nber.org/papers/w20721), *Review of Financial Studies* 29(1), 104-147.
- Markowitz (1952), [Portfolio Selection](https://doi.org/10.1111/j.1540-6261.1952.tb01525.x), *Journal of Finance* 7(1), 77-91.
- Kelly (1956), [A New Interpretation of Information Rate](https://doi.org/10.1002/j.1538-7305.1956.tb03809.x), *Bell System Technical Journal* 35(4), 917-926.
- Ledoit and Wolf (2004), [A Well-Conditioned Estimator for Large-Dimensional Covariance Matrices](https://doi.org/10.1016/S0047-259X(03)00096-4), *Journal of Multivariate Analysis* 88(2), 365-411.
- Engle and Colacito (2006), [Testing and Valuing Dynamic Correlations for Asset Allocation](https://papers.ssrn.com/sol3/papers.cfm?abstract_id=1267010), *Journal of Business & Economic Statistics* 24(2), 238-253.
- Meucci (2009), [Managing Diversification](https://papers.ssrn.com/sol3/papers.cfm?abstract_id=1358533), *Risk* 22(5), 74-79.

## 13. Final recommendation

Implement this as a new evidence-contract version rather than weakening the current contract in place. First make all six independently requested timeframe sources auditable and cell-eligible. Then generate power and dependence artifacts before any new full optimization. Only after those prerequisites pass should FxPM spend two days searching for specialists.

The expected result is not that every timeframe or symbol produces a winner. The expected result is that each survivor has comparable evidence, each abstention has a precise reason, and the institution allocates to genuinely complementary net edges rather than to whichever timeframe generated the largest raw backtest number.
