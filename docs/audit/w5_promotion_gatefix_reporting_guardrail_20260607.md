# W5 Promotion Gatefix Reporting Guardrail

Date: 2026-06-07

Scope: W5 promotion-reporting guardrail. No scorer or optimizer behavior changed.

## Finding

The generated W5 completion report still says:

- `VERDICT: CANDIDATES_PROMOTABLE_AFTER_GATES`
- `WHY: 6 regime winner(s) cleared the configured promotion gates.`

That report reflects the original promotion review, not the post-fix readiness result. The corrected
W5 readiness source is:

- `pm_outputs/experiments/production_book_legacy_freqoff_20260606_resume_w5_promotion_gatefix.json`
- `pm_outputs/experiments/production_book_legacy_freqoff_20260606/benchmarks/production_book_legacy_freqoff_20260606_per_cell_triage_gatefix.md`
- `pm_outputs/experiments/production_book_legacy_freqoff_20260606/benchmarks/production_book_legacy_freqoff_20260606_per_cell_triage_gatefix.json`

Corrected result: `PROMOTE=4`, `REVIEW=10`, `REJECT=293`; unsafe `PROMOTE` cells: `0`.

The two unsafe rows in the original `6` were:

- `CHFJPY|H4|CHOP`
- `EURJPY|H4|TREND`

## Monitor Inspection

`tools/discovery_completion_monitor.py` runs `promotion_review.py` and builds the headline from
`pm_outputs/experiments/{label}_promotion.json`. It does not run or consume
`benchmark_specialist_cells.py`, and it does not join promotion `PROMOTE` rows to per-cell audit
statuses. Therefore it can emit `CANDIDATES_PROMOTABLE_AFTER_GATES` from promotion counts alone.

`tools/postrun_cell_audit_monitor.py` runs `benchmark_specialist_cells.py` after optimizer exit and
appends navigation counts to `AGENT_COMMS.md`. It does not run `promotion_review.py`, consume the
promotion JSON, or identify "promotion says PROMOTE but cell audit says FAIL/INSUFFICIENT" conflicts.

## Recommendation

Implemented after this review: an automatic unsafe-promote cross-check now exists in
`tools/benchmark_specialist_cells.py`, and the completion/post-run monitors consume it. The check is
a read-only join keyed by `symbol|timeframe|regime` after both promotion review and specialist-cell
audit exist.

The completion headline is downgraded or flagged if any final `PROMOTE` row has a cell-audit
failure, insufficient evidence, or is missing from the cell audit. The stale W5 generated completion
report should still be treated as historical residue; use the W5 gatefix artifacts above for that
specific run's readiness counts.
