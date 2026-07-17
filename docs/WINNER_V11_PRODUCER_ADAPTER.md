# Winner V13 producer adapter

Contract: `fxpm.winner-selection.v13` / `fxpm.winner-attempt-ledger.v2` /
`fxpm.winner-evidence-ledger.v4`.

V13 is fail-closed. A stale or incomplete Winner fold is technical evidence
failure, never `NO_TRADE`. V13 also closes the candidate-selection loophole in
the earlier PBO contract: an attempt cannot disappear from the PBO population
merely because its observed OOS result failed an eligibility gate.

Before search, each attempt receives one immutable evidence role:

- `EVALUATION_REQUIRED`: the attempt must produce a complete PBO fold column
  for every frozen fold, even if its outcome later fails TradePower or another
  process gate;
- `PRESEARCH_PROCESS_REJECTED`: the attempt was independently ruled out before
  any search/OOS outcome was observed and must produce neither Winner nor PBO
  folds.

The frozen horizon contracts independently imply the same role, and the raw
producer rejects any mismatch. This prevents post-OOS relabeling from changing
the population against which selection overfit is measured.

## Trade-power sequence

1. Freeze the complete Winner attempt ledger, including rejected attempts.
2. Build multiplicity with `build_trade_power_multiplicity_receipt(attempt_ledger)`.
3. Build non-overlapping half-open train, tuning, and final contracts with `build_trade_power_partition_contract(...)`. Chain each contract to its predecessor.
4. Call `calibrate_train_trade_power(...)` with the train contract and receipt.
5. Call `evaluate_trade_power_partition(...)` with the current and prior contracts. Use distinct authenticated event axes and partition projections.
6. Hash the exact artifacts with `trade_power_requirement_sha256(...)` and `trade_power_decision_sha256(...)`.

Trade records must use schema `2026-07-14-position-events-v2` and include:

- unique `position_id` after flat-to-flat aggregation;
- `entry_time`, `exit_time`;
- nonnegative `entry_event_order`, `exit_event_order`;
- canonical order `(exit_time, exit_event_order, entry_time, entry_event_order, position_id)`;
- post-cost `pnl_dollars` and positive entry `risk_amount`.

Same-timestamp exits are valid only when the event order is strictly causal. Partial legs must be aggregated to one position record.

## Winner fold additions

Each authenticated fold adds these lists, aligned one-for-one with completed-trade R:

- `completed_trade_position_ids`
- `completed_trade_open_event_order`
- `completed_trade_close_event_order`

It also adds `trade_power_binding`:

```json
{
  "schema_version": "fxpm.winner-trade-power-binding.v1",
  "attempt_ledger_payload_sha256": "<exact Winner attempt-ledger payload hash>",
  "attempted_trials_exact": 1,
  "train_requirement_sha256": "<hash>",
  "train_calibration_sample_sha256": "<hash>",
  "train_partition_contract_sha256": "<hash>",
  "validation_decision_sha256": "<hash>",
  "validation_partition_contract_sha256": "<hash>",
  "required_effective_trades": 2,
  "binding_sha256": "<SHA-256 of the preceding fields>"
}
```

`required_effective_trades` is the exact requirement used by the PASS validation decision. V13 independently recomputes base/stress position-aware ESS and an OOS dispersion floor, then uses the maximum bound.

The fold `trade_vector_sha256` now covers the position IDs, event orders, power binding, base/stress trade R, timestamps, units, activity ledger, and risk contract.

## Required failure mapping

- Missing/stale schema, ledger mismatch, replay, event ambiguity, or lineage overlap: technical retryable failure.
- Valid but underpowered evidence: process ineligible.
- Do not persist either state as an economic symbol/regime rejection.

## V13 overfit and resampling gates

V13 applies two additional, precommitted gates to the genuine fold ledgers:

- Cell CSCV PBO v2 uses every `EVALUATION_REQUIRED` attempt in the exact frozen
  roster and evaluates all 70 symmetric splits of eight contiguous blocks.
  Outcome-derived rejection does not remove a column. Both ordinary-cost and
  cost-stress matrices use fixed-0.25%-risk daily log growth and must have PBO
  at or below 0.50. Fewer than two evaluation-required columns is typed
  insufficient evidence, not a pass.
- Candidate moving-block bootstrap pools every within-fold moving-block start
  without ever constructing a block across a causal-fold boundary, then
  resamples fixed-risk daily log growth at primary and sensitivity dependence
  scales. The seed derivation, SplitMix64 generator, 1,024 repetitions, 5%
  lower-tail rank, block rule, and operation bound are frozen in statistical
  policy v10. Base and stress lower confidence bounds must both be positive.

These gates are conjunctive with, and do not replace or weaken, TradePower,
HAC/DSR, positive-fold, cost-stress, multiplicity, or paired-comparison gates.

## Search-time integration boundary

`pm_winner_raw_evidence.freeze_winner_search_plan_v3(...)` runs before the
first attempt. It consumes full process/horizon/trial/refit bodies, the exact
fold plan, source/cost bodies, the risk-unit body, and the independently
precommitted evidence role for every attempt. It derives every hash, freezes
the complete V13 attempt ledger, derives multiplicity, and writes the immutable
artifacts through `pm_authority_spool`.

Each evaluator then passes the un-compacted `Backtester` results to
`produce_winner_v11_raw_evidence(...)` as train-base, tuning-base,
final-base, and final-stress lanes. The producer runs the released TradePower
sequence and validates the full genuine event evidence for every frozen fold.
The callable retains its historical V11 name for API compatibility, but emits
and verifies the released V13 contracts.

For every `EVALUATION_REQUIRED` attempt/fold, the producer first writes a
complete PBO fold. A fold that passes the existing power and eligibility gates
also projects to a full Winner fold; the verifier requires that projection to
match the PBO fold exactly. A valid but underpowered or otherwise
outcome-rejected attempt retains its PBO column but receives no eligibility
fold. Outcome-only evidence is never accepted for an evaluation-required
attempt.

Two source identities are deliberately distinct:

- `attempt_oos_source_receipt_sha256` preserves the candidate-specific raw
  evidence and attempt provenance;
- `shared_oos_partition` is a self-hashed, candidate-neutral identity for the
  common source prefix, fold interval, costs, stress, regime fit, execution
  specification, and risk facts.

CSCV pairing uses the shared partition identity, while the verifier still
requires candidate-specific source, replay, activity, and trade identities.
This permits genuine candidates on the same unseen interval to form a matrix
without erasing their individual provenance.

The canonical engine payload is
`Backtester.run(...)["winner_daily_r_surface"]`. Its schema is
`fxpm.backtester-winner-daily-r.v1`. It marks every open flat-to-flat position
at each UTC day boundary after spread, slippage, commission, and causal carry,
then normalizes the position by its entry risk. Partial legs reconcile to the
same position-events-v2 identity. The surface uses a bounded streaming
projection and exact 12-decimal half-even R accounting; it never retains a
day-by-position matrix. Missing observable close timestamps, noncausal carry,
bound exhaustion, or reconciliation drift fail technically. Account-equity
returns are not accepted as this payload.
