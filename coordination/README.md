# FXPM Agent Coordination Protocol — RETIRED 2026-08-11

> **`tools/agent_coordination.py` has been removed.** Live coordination is
> `agent_comms/PROTOCOL.md` with `python tools/agent_bus.py`
> (post / claim / release / tail / lanes).
>
> The tool was 1,543 lines plus a 437-line test: an OS-locked, hash-chained,
> append-only event ledger built so two agents could prove neither had tampered
> with the other's notes — on a single laptop, with no attacker, against the
> operator's own directive that security is not a priority here. Nothing in the
> engine imported it. The 326-line bus does the same job.
>
> **This directory is preserved read-only as decision history.**
> `docs/PROJECT_MILESTONES.md` cites its `seq` numbers as provenance, so the
> ledger itself is evidence and stays. Everything below describes the retired
> tool and is kept for reading those records, not for running anything.

---

`tools/agent_coordination.py` was the single machine-readable coordination
authority for Codex, Claude Code, reviewers, and the operator. Human milestone
summaries may still be written to `AGENT_COMMS.md`, but claims, vetoes, review
gates, completion, and release were decided only by this transactional ledger.

Do not hand-edit the ledger, manifest, or segment files. Do not maintain a
second live lane registry. Every reader and writer uses the same OS lock.

## Start and finish a major lane

```powershell
python tools/agent_coordination.py claim `
  --actor CODEX --task regime-preflight `
  --message "Implement fail-closed regime preflight" `
  --path pm_regime_tuner.py --path tests/test_regime_tuner.py
```

The returned `event_id` is the claim id. A major lane is completed against an
immutable artifact, either a full Git commit:

```text
git:<40-hex-commit>
```

or an exact uncommitted diff bound to its base commit:

```text
diff:<64-hex-diff-sha256>@base:<40-hex-commit>
```

The owner requests review:

```powershell
python tools/agent_coordination.py review-request `
  --actor CODEX --task regime-preflight --claim-id CLAIM_UUID `
  --commit FULL_40_HEX_COMMIT --message "Please rerun focused tests and inspect diff"
```

Another actor independently reviews it:

```powershell
python tools/agent_coordination.py review `
  --actor CLAUDE --task regime-preflight-audit --request-id REQUEST_UUID `
  --verdict ACCEPT --message "Diff inspected and tests independently green"
```

`COMPLETE` checks the exact artifact, latest verdict, actor independence, and
active veto state inside the same lock used to append the event. Completion
does not release the lane, preventing a race between the gate and relinquishing
ownership:

```powershell
python tools/agent_coordination.py complete `
  --actor CODEX --task regime-preflight --claim-id CLAIM_UUID `
  --commit FULL_40_HEX_COMMIT --message "Reviewed artifact complete"

python tools/agent_coordination.py release `
  --actor CODEX --task regime-preflight --claim-id CLAIM_UUID `
  --message "Lane released after gated completion"
```

Use `ABANDON` to relinquish unfinished work without claiming success. A major
lane cannot bypass review by calling `RELEASE`; release requires a prior
`COMPLETE`. Minor, non-material lanes may be claimed with `--minor`.

## Claims and conflicts

Claim paths must be literal repo-relative paths. Absolute paths, `..`, globs,
Windows device aliases, and trailing-dot/space aliases are rejected. Conflict
keys are always Unicode-casefolded, even on Linux, because FXPM runs on Windows.
File/directory prefix overlap is symmetric: a claim on `tools` conflicts with
`TOOLS/helper.py`.

Claim conflict detection and event append occur in one OS-locked transaction.
Concurrent contenders therefore produce exactly one winner; there is no
read-check-write gap or separately rewritten lane registry.

## Reviews, decisions, and vetoes

Supported public event types are:

- `CLAIM`, `UPDATE`, `STATUS`, `FINDING`, `QUESTION`, and `ANSWER`
- `REVIEW_REQUEST` and `REVIEW`
- `DECISION` and `VETO`
- `COMPLETE`, `RELEASE`, and `ABANDON`

The latest verdict for the latest request matching the exact claim and artifact
controls the gate. Commit prefixes are invalid. Claim owners and requesters
cannot review their own work.

A `VETO` freezes both completion and release for its task. Only the veto author
or `OPERATOR` may resolve it through a `DECISION --resolve-veto VETO_UUID`.
Architectural decisions use stable keys; a later decision with the same key
supersedes the prior current decision without growing checkpoint state.

## Bounded retention and integrity

The default local budget is four 4 MiB raw segments plus the active segment.
Individual events and checkpoints also have hard size caps. When another
rotation would exceed the segment count, the tool:

1. replays and verifies the complete retained hash chain;
2. snapshots all active claims, current reviews, current decisions, unresolved
   vetoes, and bounded completion summaries;
3. creates a `CHECKPOINT` whose predecessor is the last compacted event;
4. advances a cumulative chain-anchor commitment;
5. atomically switches the hashed manifest to the checkpoint generation; and
6. removes superseded, now-unreferenced raw segments.

This bounds local storage and replay memory without losing any state needed to
coordinate current work. Old status chatter is intentionally compacted. The
manifest anchor records the compacted terminal event hash, sequence, and prior
anchor commitment; the first retained checkpoint must match it exactly.

Only a non-newline-terminated fragment at the end of the active file is
recoverable. Recovery truncates that fragment and appends a hash-chained
`RECOVERY` event. Invalid JSON with a newline, corruption in a retained segment,
sequence gaps, predecessor mismatches, manifest changes, and event hash changes
all fail closed and are never silently skipped.

## Session and audit commands

```powershell
python tools/agent_coordination.py status --json
python tools/agent_coordination.py claims --json
python tools/agent_coordination.py tail --count 20 --json
python tools/agent_coordination.py tail --since-sequence 100 --type FINDING
python tools/agent_coordination.py verify --json
python tools/agent_coordination.py gate --claim-id CLAIM_UUID --commit FULL_40_HEX_COMMIT
```

At session start, read `status`, active claims, and the recent tail before
editing. During work, use `update`, `finding`, `question`, `answer`, or
`announce-status`. At session end, post the evidence and request review; release
only after the independent gate accepts the immutable artifact.

The runtime files are created beside this README on first use:

- `events.jsonl.manifest.json`: atomic, hashed generation manifest
- one active `.jsonl` file and at most the configured number of segments
- `events.jsonl.lock`: OS lock byte

Tests exercise 28 simultaneous writers with concurrent rotation, a
same-file/case-alias claim race, exact review gates, vetoes, finite compaction,
chain preservation, and fail-closed corruption handling.
