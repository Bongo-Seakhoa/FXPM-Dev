# FxPM Causal Specialist Process Policy

Status: ratified design record, not a live-trading authorization  
Date: 2026-07-13  
Scope: the production research, selection, refit, arbitration, and governance
process for the 100-symbol x 6-timeframe x 4-regime specialist book.

## Decision

FxPM is a conditional specialist institution. Each symbol, native timeframe,
and regime cell may admit a strategy expert when the evidence supports one;
an empty cell is legitimate. The institution must request and account for all
2,400 cells, but it must never manufacture a winner merely to fill the grid.

The object validated for deployment is the frozen selection process, not one
historical parameter vector. After that process passes causal rolling-origin
validation, the identical process is refit independently through the latest
fully closed native bar. The fresh refit may enter probation immediately, but
it does not inherit parameter-specific validation from historical winners.

This resolves the bleeding-edge conflict without pretending it does not
exist: historical forward tests decide whether the process is admissible;
only after that decision is fixed may the process consume the latest data to
produce the parameter vector intended for the current market.

## 1. Data clocks and specialist independence

1. M5, M15, M30, H1, H4, and D1 are acquired directly from the broker's
   native bars. Higher timeframes are not inferred from an M5 tail for
   production evidence.
2. Every cell shares one authenticated information cutoff, expressed as the
   latest fully closed native bar available at or before the common as-of
   time. Open or partially formed bars are excluded.
3. Acquisition requests cover the exact 100 x 6 roster. A short broker
   history, pre-listing boundary, or sparse sibling timeframe is a typed data
   fact for that cell. It may reduce the evidence available to the cell; it
   may not erase the symbol, veto another timeframe, or write `NO_TRADE` into
   the winner ledger.
4. Authenticity failures, internal gaps that contradict the declared source,
   corrupt provenance, and fetch failures are technical states and fail
   closed. Statistical insufficiency is assessed later and separately.

## 2. Training horizon is selected, not assumed

Equal bar counts and equal calendar spans are both invalid universal rules.
Fifty thousand M5 bars and fifty thousand D1 bars do not describe comparable
market histories, while five calendar years of each do not provide comparable
numbers of decisions or completed trades.

For each timeframe, the frozen process pre-registers a bounded set of
candidate horizons. Admissibility uses all of the following:

- strategy and feature warm-up dependencies;
- regime and session-state dependencies;
- native cadence and observed availability;
- dependence-adjusted effective sample size;
- completed-trade power for the pre-registered minimally relevant effect;
- the number of strategies, parameter trials, horizons, folds, and cells
  searched; and
- evidence of non-stationarity and edge decay.

The horizon is a searched hyperparameter and therefore consumes multiplicity.
It is selected independently inside every historical outer anchor. The
longest history is not privileged: if statistically indistinguishable
horizons remain, worst-forward-fold and newest-fold robustness are considered
before a deterministic simplicity rule. Old history that dilutes a current
edge is not automatically beneficial.

## 3. Inner selection and the middle holdout

A purged, embargoed interior holdout is allowed inside the historical prefix
used for screening, tuning, horizon selection, and model selection. Training
may exist on both sides of such an inner fold, provided all observations are
strictly earlier than the outer forward-test segment.

An interior holdout is not the final out-of-sample authority. It estimates
interpolation performance and cannot, by itself, detect that an edge died at
the end of the available history. Once observations after an interior window
have influenced selection, that window cannot honestly represent the unseen
future.

Every screened strategy, failed trial, pruned trial, horizon, and discarded
candidate remains in the immutable search ledger. Filtering does not unspend
a look at the data.

## 4. Outer process validation

The frozen process is rerun at multiple historical rolling origins. At every
anchor it must:

1. expose only information available by that anchor's cutoff;
2. rerun screening, horizon selection, strategy selection, and parameter
   search locally for that anchor;
3. refit the anchor-local winner only on the permitted historical prefix;
4. evaluate it on the strictly later, purged forward segment; and
5. emit the exact post-cost daily net-mark-to-market return vector and its
   source, execution-cost, selection, model, and parameter identities.

A scalar utility is not sufficient evidence. The outer receipt must bind the
vector of anchor-local selection decisions and refits. Replaying today's
survivor over every historical window is survivorship laundering and is
forbidden.

Outer evidence is assessed as a distribution across anchors, not as one large
two-year observation. Dependence-aware confidence bounds, completed-trade
power, downside/tail constraints, worst-anchor behavior, the newest anchor,
and full-book multiplicity all apply. Inactive cells remain in the 2,400-cell
family with null evidence rather than disappearing from correction.

## 5. Current refit and evidence inheritance

Only after the deployment decision for the frozen process is fixed may the
same process refit through the current closed-bar cutoff. The current refit
must bind its source frame, selected horizon, full trial ledger, strategy,
parameters, costs, and latest-bar identity.

Historical process evidence transfers to the deterministic process. It does
not prove that the newly selected parameter vector is profitable. That
residual uncertainty is managed with evidence-calibrated probation,
pre-committed promotion and demotion budgets, and forward demo/live evidence.
The current refit is not allowed to alter the already-recorded historical
process verdict.

Walk-forward anchors also produce an edge-decay curve by timeframe. That
curve, plus forward drift evidence, sets retraining and probation cadence.
The existing uniform 28-day retrain and 14-day probation values are
placeholders until this measurement exists.

## 6. Winner selection and cross-timeframe arbitration

The legacy weighted score is a compute-screening aid only. It is not final
winner authority because it double-counts correlated metrics and mixes units,
horizons, and risk scales.

Final cell admission uses exact post-cost daily net-MTM returns and requires:

- a dependence-aware lower confidence utility above the pre-registered null;
- acceptable worst and newest forward-fold behavior;
- completed-trade and effective-sample power;
- the complete attempt ledger and book-level multiplicity;
- explicit drawdown, expected-shortfall, loss-cluster, and cost-stress
  constraints; and
- non-dominance across these dimensions, with the simpler candidate winning
  only when the evidence cannot distinguish candidates economically.

Simultaneous timeframe proposals are compared on one complete UTC
calendar-day axis, including zero-return non-session days and open-position
mark-to-market effects. Raw bar counts, raw trade counts, raw returns, and
unadjusted Sharpe values are not comparable voting weights. D1 has a
structurally separate capacity slot; at most one lower-timeframe specialist
occupies the second symbol slot unless later synchronized evidence justifies a
different policy.

## 7. Capital semantics

`Summed initial-stop loss / conservative equity`, gross notional, net
notional, margin utilization, factor/gap loss, mark-to-market drawdown, and
ruin probability are different quantities and must be reported separately.
A bank's 33-90% balance-sheet or margin deployment is not evidence that
33-90% of account equity may safely be lost at initial stops.

No fixed 3%, 8%, 33%, 50%, 75%, or 90% aggregate limit is authorized by this
policy. The production frontier is selected only after a fresh authenticated
book has synchronized portfolio replays and factor, gap, liquidity, margin,
and ruin stresses. A medium-to-high appetite may choose an aggressive point
on that evidenced frontier; it may not redefine the risk measure.

## 8. Production gates

The design is not complete until all of these are executable and integrated:

- authenticated 600-cell native acquisition and activation;
- causal horizon and rolling-origin receipts with no local signing oracle;
- immutable all-attempt and 2,400-cell multiplicity ledgers;
- current-refit receipts and bounded state continuity from backtest to live;
- a runtime indicator-state parity probe that, at demo/live decision points,
  recomputes features from authenticated authoritative history and requires
  equality within pre-registered numeric tolerances before a signal may act;
- synchronized daily evidence and deterministic specialist arbitration;
- failure-safe winner-book publication that preserves the prior book on
  infrastructure or data failure;
- bounded memory, storage, logs, and replay work without lossy shortcuts;
- one-symbol and multi-symbol canaries, then demo forward evidence; and
- an evidence-derived capital frontier and explicit operator authorization.

Until these gates pass on fresh evidence, engineering progress is not proof
of profitability and funded live trading remains disabled.

## Primary references

- Robert Pardo, walk-forward analysis: <https://onlinelibrary.wiley.com/doi/abs/10.1002/9781119196969.ch11>
- Bailey et al., probability of backtest overfitting: <https://escholarship.org/uc/item/4w1110bb>
- Bailey and Lopez de Prado, deflated Sharpe ratio: <https://papers.ssrn.com/sol3/papers.cfm?abstract_id=2460551>
- Bailey et al., backtest overfitting: <https://papers.ssrn.com/sol3/papers.cfm?abstract_id=2308659>
- Hyndman, rolling-origin time-series cross-validation: <https://robjhyndman.com/hyndsight/tscv/>
