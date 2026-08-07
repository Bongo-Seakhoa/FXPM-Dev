# F004: No explicit Toronto metric exists in the codebase

## Category
Metrics / Documentation

## Severity
Informational

## Confidence
High

## Evidence
- `rg -n "Toronto|toronto" -g "!*agent_audit*"` returned no matches.
- `agent_audit/data/audit_evidence_summary.json` records `toronto_metric_search.match_count=0`.
- The codebase contains standard and custom metrics such as profit factor, expectancy, Sharpe, Sortino, Calmar/return-DD ratio, recovery factor, and the ledger metric contract `net_dollar_mtm_equity_v1`.

## Explanation
The requested "Toronto" metric is not currently implemented or named in the repository. Any Toronto score would need to be defined before it can be audited.

## Trading Impact
No direct trading impact. This is a reporting and scope clarification.

## Suggested Fix or Experiment
If Toronto is an operator-defined metric, add a short formula contract to the repo and wire it into the collector/reporting layer.

## Validation Plan
Re-run the source search after any metric addition.

## Partner Review
Pending.

