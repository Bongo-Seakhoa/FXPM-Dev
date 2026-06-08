# Independent Audit Protocol

This project uses independent audit agents as a standing quality control layer.
The goal is early correction, not late blame: every material change should be
challenged before it becomes a benchmark, promotion input, or live candidate.

## Roles

- Implementation owner: makes the code or pipeline change and records evidence.
- Code auditor: read-only challenger for changed files, tests, lookahead risk,
  runtime safety, and maintainability.
- Evidence auditor: read-only challenger for benchmark design, cost realism,
  roster consistency, artifact provenance, and Tenet-5 gating.
- External red-team: optional independent pass using Claude/Gemini/sub-agents
  for high-risk work such as scoring, backtesting cost, live execution, or
  promotion logic.

Auditors should be independent of the implementation path. They may recommend
patches, but they should not edit the same files unless explicitly assigned a
separate cleanup or fix.

"Read-only auditor" is a role boundary for a given review, not a protected-file
rule. No file in this repository is inherently read-only or sacred. Any file can
be edited, replaced, moved, or deleted when data-driven discovery shows that the
change improves correctness, profitability evidence, resilience, or clarity.

## Required Checkpoints

Run an independent audit pass at these points:

1. Before design freeze for high-impact changes.
2. After implementation and tests, before promotion or live-candidate use.
3. After any benchmark or re-optimization result that could change the winner
   ledger.
4. Before live/test-environment activation.

For small hygiene-only commits, a local sanity check is enough, but the change
must stay scoped and must not touch trading behavior.

## Audit Inputs

Every auditable result should identify:

- Git commit or working-tree state.
- Strategy roster count.
- Cost model and broker-spec fingerprint.
- Symbols, timeframes, date range, and sample/trial counts.
- Random seed or optimizer seed where applicable.
- Evidence artifacts used for the claim.
- Tests run and exact pass/fail result.

## Artifact Lifecycle

Audit notes, agent comms, experiment notes, and review documents are working
artifacts. They are useful while they expose unresolved issues or explain an
active decision, but they are not evergreen project documentation.

When an audit artifact has served its purpose:

- move durable facts into code, tests, config, or `CHANGELOG.md`;
- keep only active unresolved findings in working notes;
- delete stale audit/meta files instead of letting old context steer current
  decisions.

`CHANGELOG.md` is the durable history surface. Audit files are disposable unless
there is a current reason to keep them.

## Findings Policy

Auditors report findings by severity:

- Critical: blocks promotion/live use until fixed or explicitly waived by the
  user.
- High: blocks benchmark acceptance unless there is a documented trade-off.
- Medium: should be fixed before the next phase when practical.
- Low: cleanup or follow-up.

Critical and High findings require an owner response in `AGENT_COMMS.md`:
fixed, accepted risk, deferred with reason, or invalid with evidence.

## Current Standing Rules

- Read `docs/ACTIVE_DISCOVERY_STATE.md` before starting any audit. It is the current alignment
  surface for active runs, active scorer variants, and next actions.
- Compare scoring changes only as same-roster, like-for-like experiments unless the experiment is
  explicitly about roster expansion.
- Treat prospect screens and proxy tools as hypothesis evidence only, not promotion evidence.
- Do not add ML/meta-model selection unless the user explicitly changes the
  directive.
- Do not promote a strategy or scoring change without honest-cost simulation,
  validation/holdout evidence, C3 false-discovery evidence, and Tenet-5 gating.
- Archive or delete resolved audit notes after durable facts have moved into code, tests, config,
  `CHANGELOG.md`, or `docs/ACTIVE_DISCOVERY_STATE.md`.
