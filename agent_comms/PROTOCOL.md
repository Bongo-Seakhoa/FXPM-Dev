# FXPM Agent Protocol v1 (Claude <-> Codex)

Adopted 2026-07-12 by operator directive: the two agents must collaborate,
hold each other accountable, and cross-audit so neither walks a wrong
implementation path alone. This protocol replaces prose-append coordination
for day-to-day work. `AGENT_COMMS.md` remains the **operator-facing digest**
(milestone summaries only, human-readable).

Core objective every message serves: a production-ready FXPM with a
medium-to-high risk appetite that can be consistently profitable on live
markets. Risk appetite is expressed as a calibrated ruin-tolerance envelope,
never as weakened evidence standards.

## Files

| File | Purpose | Bound |
|---|---|---|
| `BUS.jsonl` | Append-only structured message bus, one JSON object per line | rotate to `BUS.archive-YYYYMMDD.jsonl` when > 4 MB |
| `LANES.json` | Live file-lane ownership registry (atomic rewrite) | released lanes pruned after 14 days, max 100 history entries |
| `STATE.md` | One-page current program truth (phase, gates, next actions) | always rewritten in place, one page |
| `PROTOCOL.md` | This file | changes require a `decision` message + no `veto` |

Use `python tools/agent_bus.py` (post / claim / release / tail / lanes) —
it validates the schema, assigns ids/timestamps, and enforces the bounds.
Direct file edits are allowed when the tool is unavailable, same schema.

## Message schema (BUS.jsonl)

```json
{"id": "CC-001", "ts": "2026-07-12T18:30:00Z", "from": "claude", "to": "codex",
 "type": "status", "paths": [], "refs": [], "body": "..."}
```

- `id`: `CC-` (Claude) / `CX-` (Codex) + zero-padded increment per agent.
- `type`: one of
  `claim` | `release` | `status` | `finding` | `question` | `answer` |
  `decision` | `veto` | `review_request` | `review`.
- `refs`: ids this message responds to (threading).
- `paths`: repo-relative files/dirs the message concerns.

## Rules

1. **Session start**: read `STATE.md`, `LANES.json`, and the unread tail of
   `BUS.jsonl` before touching shared code. **Session end**: post a `status`
   (what changed, test state) and release finished lanes.
2. **Claim before edit**: shared core files (pm_*.py, tools/*, tests of the
   other agent's lanes, config.json, pm_configs.json) require an active lane
   in `LANES.json` before editing. Never edit a path inside another agent's
   active lane — post a `question` instead. New files in your own lane need
   no claim.
3. **Mandatory secondary audit**: a major lane (new module, contract change,
   anything touching evidence/scoring/risk/live execution) is DONE only
   after the other agent posts a `review` answering a `review_request`.
   Reviews verify claims independently (run the tests, read the diff) — a
   rubber stamp is a protocol violation.
4. **Decisions and vetoes**: irreversible or architectural choices are
   posted as `decision` before implementation where practical, otherwise
   immediately after. The other agent may `veto` with evidence; a veto
   freezes the disputed lane until resolved between agents or escalated to
   the operator via AGENT_COMMS.md.
5. **Evidence discipline**: findings cite file:line, test names, or artifact
   paths. No "should work". Failed tests are reported as failed.
6. **Operator visibility**: milestone summaries and unresolved disputes go
   to `AGENT_COMMS.md`; routine traffic stays on the bus.
7. **Sub-agents**: either agent may internally spawn specialist perspectives
   (data science, trading, financial engineering, risk); bus messages always
   represent the parent agent, which owns the quality of delegated work.
8. **Live-market safety override**: anything that could enable funded
   trading requires an explicit `decision` + the other agent's `review` +
   operator approval recorded in AGENT_COMMS.md. No exceptions.
