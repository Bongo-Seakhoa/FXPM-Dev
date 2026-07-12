"""Concurrency, integrity, retention, and review gates for agent coordination."""

from __future__ import annotations

import json
import os
import subprocess
import sys
import time
from pathlib import Path

import pytest

from tools.agent_coordination import (
    CoordinationError,
    EventStore,
    TruncatedTailError,
    _artifact_review_gate,
)


ROOT = Path(__file__).resolve().parents[1]
COMMIT_A = "a" * 40
COMMIT_B = "b" * 40


def _store(path: Path, **overrides) -> EventStore:
    settings = {
        "segment_max_bytes": 1024 * 1024,
        "retain_segments": 2,
        "max_event_bytes": 16 * 1024,
        "max_checkpoint_bytes": 64 * 1024,
        "lock_timeout_seconds": 20,
    }
    settings.update(overrides)
    return EventStore(path, **settings)


def _claim(store: EventStore, *, actor: str = "CODEX", task: str = "lane") -> dict:
    return store.append(
        actor=actor,
        event_type="CLAIM",
        task_id=task,
        payload={"paths": ["pm_core.py"], "message": "Own core lane", "major": True},
    )


def test_casefolded_and_directory_claim_conflicts(tmp_path: Path) -> None:
    store = _store(tmp_path / "events.jsonl")
    _claim(store)

    with pytest.raises(CoordinationError, match="conflicts"):
        store.append(
            actor="CLAUDE",
            event_type="CLAIM",
            task_id="case-alias",
            payload={"paths": ["PM_CORE.py"], "message": "Windows alias"},
        )

    store.append(
        actor="CLAUDE",
        event_type="CLAIM",
        task_id="tools-parent",
        payload={"paths": ["tools"], "message": "Own tools directory"},
    )
    with pytest.raises(CoordinationError, match="conflicts"):
        store.append(
            actor="CODEX",
            event_type="CLAIM",
            task_id="tools-child",
            payload={"paths": ["TOOLS/helper.py"], "message": "Nested collision"},
        )

    for unsafe in ("../outside.py", "C:/absolute.py", "NUL.txt", "bad*.py", "dir./x.py"):
        with pytest.raises(CoordinationError):
            store.append(
                actor="CODEX",
                event_type="CLAIM",
                task_id=f"unsafe-{unsafe}",
                payload={"paths": [unsafe], "message": "must reject"},
            )


def test_major_complete_release_veto_and_exact_review_gate(tmp_path: Path) -> None:
    store = _store(tmp_path / "events.jsonl")
    claim = _claim(store)
    claim_id = claim["event_id"]

    with pytest.raises(CoordinationError, match="prior gated COMPLETE"):
        store.append(
            actor="CODEX",
            event_type="RELEASE",
            task_id="lane",
            payload={"claim_event_id": claim_id, "message": "too early"},
        )
    with pytest.raises(CoordinationError, match="review gate failed"):
        store.append(
            actor="CODEX",
            event_type="COMPLETE",
            task_id="lane",
            payload={"claim_event_id": claim_id, "artifact": COMMIT_A, "message": "done"},
        )
    with pytest.raises(CoordinationError, match="artifact must be"):
        store.append(
            actor="CODEX",
            event_type="REVIEW_REQUEST",
            task_id="lane",
            payload={"claim_event_id": claim_id, "artifact": "aaaaaaa", "message": "prefix"},
        )

    request = store.append(
        actor="CODEX",
        event_type="REVIEW_REQUEST",
        task_id="lane",
        payload={"claim_event_id": claim_id, "artifact": COMMIT_A, "message": "Audit commit"},
    )
    with pytest.raises(CoordinationError, match="cannot independently review"):
        store.append(
            actor="CODEX",
            event_type="REVIEW",
            task_id="lane-audit",
            payload={
                "request_event_id": request["event_id"],
                "verdict": "ACCEPT",
                "message": "self review",
            },
        )
    store.append(
        actor="CLAUDE",
        event_type="REVIEW",
        task_id="lane-audit",
        payload={
            "request_event_id": request["event_id"],
            "verdict": "CHANGES_REQUIRED",
            "message": "Fix the boundary",
        },
    )
    with pytest.raises(CoordinationError, match="CHANGES_REQUIRED"):
        store.append(
            actor="CODEX",
            event_type="COMPLETE",
            task_id="lane",
            payload={"claim_event_id": claim_id, "artifact": COMMIT_A, "message": "not yet"},
        )
    store.append(
        actor="CLAUDE",
        event_type="REVIEW",
        task_id="lane-audit",
        payload={
            "request_event_id": request["event_id"],
            "verdict": "ACCEPT",
            "message": "Independent rerun is green",
        },
    )

    veto = store.append(
        actor="CLAUDE",
        event_type="VETO",
        task_id="lane",
        payload={"message": "Hold completion", "evidence": "artifact mismatch under audit"},
    )
    with pytest.raises(CoordinationError, match="frozen"):
        store.append(
            actor="CODEX",
            event_type="COMPLETE",
            task_id="lane",
            payload={"claim_event_id": claim_id, "artifact": COMMIT_A, "message": "blocked"},
        )
    with pytest.raises(CoordinationError, match="veto author or OPERATOR"):
        store.append(
            actor="CODEX",
            event_type="DECISION",
            task_id="lane",
            payload={
                "key": "veto-resolution",
                "resolves_veto": veto["event_id"],
                "message": "cannot overrule reviewer",
            },
        )
    store.append(
        actor="CLAUDE",
        event_type="DECISION",
        task_id="lane",
        payload={
            "key": "veto-resolution",
            "resolves_veto": veto["event_id"],
            "message": "Evidence reconciled; veto withdrawn",
        },
    )
    store.append(
        actor="CODEX",
        event_type="COMPLETE",
        task_id="lane",
        payload={"claim_event_id": claim_id, "artifact": COMMIT_A, "message": "gated complete"},
    )
    store.append(
        actor="CODEX",
        event_type="RELEASE",
        task_id="lane",
        payload={"claim_event_id": claim_id, "message": "release after completion"},
    )
    status = store.status()
    assert status["active_claims"] == []
    assert status["recent_completions"][-1]["outcome"] == "RELEASED"
    assert status["recent_completions"][-1]["artifact"] == f"git:{COMMIT_A}"


def test_checkpoint_rotation_is_finite_and_preserves_live_state(tmp_path: Path) -> None:
    store = _store(
        tmp_path / "events.jsonl",
        segment_max_bytes=900,
        retain_segments=2,
    )
    claim = _claim(store)
    request = store.append(
        actor="CODEX",
        event_type="REVIEW_REQUEST",
        task_id="lane",
        payload={
            "claim_event_id": claim["event_id"],
            "artifact": COMMIT_B,
            "message": "Review bounded-ledger work",
        },
    )
    store.append(
        actor="CLAUDE",
        event_type="REVIEW",
        task_id="ledger-audit",
        payload={
            "request_event_id": request["event_id"],
            "verdict": "ACCEPT",
            "message": "Independent checks pass",
        },
    )
    store.append(
        actor="CODEX",
        event_type="DECISION",
        task_id="lane",
        payload={"key": "canonical-protocol", "message": "Use one transactional ledger"},
    )
    for index in range(70):
        store.append(
            actor="CODEX" if index % 2 else "CLAUDE",
            event_type="STATUS",
            task_id=f"chatter-{index}",
            payload={"message": f"bounded status {index} " + "x" * 180},
        )

    status = store.status()
    assert status["valid"] is True
    assert status["compacted_through_sequence"] > 0
    assert status["storage"]["segment_count"] <= 2
    assert len(list(tmp_path.glob("*.jsonl"))) <= 3
    assert status["active_claims"][0]["claim_event_id"] == claim["event_id"]
    assert status["decisions"][0]["key"] == "canonical-protocol"
    _, _, state = store.read_bundle()
    passed, reason = _artifact_review_gate(claim["event_id"], f"git:{COMMIT_B}", state)
    assert passed, reason
    retained = store.read_events()
    assert retained[0]["event_type"] == "CHECKPOINT"
    # Each retained segment can exceed the trigger only by one bounded event;
    # the checkpoint has its own explicit cap.
    hard_bound = (
        2 * (900 + 16 * 1024)
        + max(900, 64 * 1024)
        + 16 * 1024
    )
    assert status["storage"]["referenced_bytes"] <= hard_bound


def test_only_incomplete_final_fragment_is_recoverable(tmp_path: Path) -> None:
    event_file = tmp_path / "events.jsonl"
    store = _store(event_file)
    store.append(
        actor="CODEX",
        event_type="STATUS",
        task_id="tail",
        payload={"message": "valid first record"},
    )
    current = Path(store.status()["storage"]["current"])
    with current.open("ab") as handle:
        handle.write(b'{"incomplete":')
        handle.flush()
        os.fsync(handle.fileno())

    with pytest.raises(TruncatedTailError):
        store.read_events()
    store.append(
        actor="CLAUDE",
        event_type="STATUS",
        task_id="tail",
        payload={"message": "append performs audited final-tail recovery"},
    )
    events = store.read_events()
    assert [event["event_type"] for event in events] == ["STATUS", "RECOVERY", "STATUS"]
    assert events[1]["payload"]["bytes_removed"] == len(b'{"incomplete":')

    corrupt_file = tmp_path / "corrupt.jsonl"
    corrupt_store = _store(corrupt_file)
    corrupt_store.append(
        actor="CODEX",
        event_type="STATUS",
        task_id="corrupt",
        payload={"message": "one"},
    )
    corrupt_store.append(
        actor="CLAUDE",
        event_type="STATUS",
        task_id="corrupt",
        payload={"message": "two"},
    )
    corrupt_current = Path(corrupt_store.status()["storage"]["current"])
    lines = corrupt_current.read_bytes().splitlines(keepends=True)
    lines[0] = lines[0].replace(b'"actor":"CODEX"', b'"actor":"XXXXX"')
    corrupt_current.write_bytes(b"".join(lines))
    with pytest.raises(CoordinationError, match="event hash mismatch"):
        corrupt_store.read_events()
    # A newline-terminated middle/hash corruption must not be auto-repaired.
    with pytest.raises(CoordinationError, match="event hash mismatch"):
        corrupt_store.append(
            actor="CODEX",
            event_type="STATUS",
            task_id="corrupt",
            payload={"message": "must fail closed"},
        )


_PROCESS_SCRIPT = r"""
import json
import os
import time
from tools.agent_coordination import main

while time.time() < float(os.environ["FXPM_COORD_TARGET"]):
    time.sleep(0.001)
raise SystemExit(main(json.loads(os.environ["FXPM_COORD_ARGV"])))
"""


def _run_synchronized(argument_vectors: list[list[str]], timeout: int = 45):
    target = time.time() + 3.0
    processes = []
    for arguments in argument_vectors:
        environment = os.environ.copy()
        environment.update({
            "FXPM_COORD_TARGET": str(target),
            "FXPM_COORD_ARGV": json.dumps(arguments),
            "PYTHONDONTWRITEBYTECODE": "1",
            "PYTHONPATH": str(ROOT),
        })
        processes.append(
            subprocess.Popen(
                [sys.executable, "-X", "utf8", "-c", _PROCESS_SCRIPT],
                cwd=ROOT,
                env=environment,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
        )
    results = []
    for process in processes:
        stdout, stderr = process.communicate(timeout=timeout)
        results.append((process.returncode, stdout, stderr))
    return results


def test_28_process_append_stress_with_concurrent_rotation(tmp_path: Path) -> None:
    event_file = tmp_path / "events.jsonl"
    base = [
        "--event-file", str(event_file),
        "--segment-max-bytes", "900",
        "--retain-segments", "2",
        "--max-event-bytes", str(16 * 1024),
        "--max-checkpoint-bytes", str(64 * 1024),
        "--lock-timeout", "30",
    ]
    commands = []
    for index in range(28):
        commands.append(base + [
            "post",
            "--actor", "CODEX" if index % 2 else "CLAUDE",
            "--task", f"parallel-{index}",
            "--type", "STATUS",
            "--message", f"parallel record {index} " + "z" * 180,
        ])
    results = _run_synchronized(commands)
    assert [code for code, _, _ in results] == [0] * 28, [stderr for _, _, stderr in results]
    emitted = [json.loads(stdout) for _, stdout, _ in results]
    assert len({event["event_id"] for event in emitted}) == 28
    assert len({event["sequence"] for event in emitted}) == 28

    store = EventStore(event_file)
    status = store.status()
    assert status["valid"] is True
    assert status["high_water_sequence"] >= 28
    assert status["compacted_through_sequence"] > 0
    assert status["storage"]["segment_count"] <= 2
    assert len(list(tmp_path.glob("*.jsonl"))) <= 3


def test_multiprocess_conflicting_claim_has_exactly_one_winner(tmp_path: Path) -> None:
    event_file = tmp_path / "claims.jsonl"
    base = ["--event-file", str(event_file), "--lock-timeout", "30"]
    results = _run_synchronized([
        base + [
            "claim", "--actor", "CODEX", "--task", "codex-claim",
            "--message", "claim lower case", "--path", "pm_core.py",
        ],
        base + [
            "claim", "--actor", "CLAUDE", "--task", "claude-claim",
            "--message", "claim Windows alias", "--path", "PM_CORE.py",
        ],
    ])
    assert sorted(code for code, _, _ in results) == [0, 2]
    status = EventStore(event_file).status()
    assert len(status["active_claims"]) == 1
    assert status["active_claims"][0]["path_keys"] == ["pm_core.py"]


def test_status_tail_and_claim_views_are_verified(tmp_path: Path, capsys) -> None:
    from tools.agent_coordination import main

    event_file = tmp_path / "ux.jsonl"
    assert main([
        "--event-file", str(event_file),
        "claim", "--actor", "CODEX", "--task", "ux",
        "--message", "claim UX test", "--path", "tools/new.py", "--minor",
    ]) == 0
    capsys.readouterr()
    assert main(["--event-file", str(event_file), "status", "--json"]) == 0
    status = json.loads(capsys.readouterr().out)
    assert status["valid"] is True
    assert len(status["active_claims"]) == 1
    assert main(["--event-file", str(event_file), "tail", "--count", "1", "--json"]) == 0
    tail = json.loads(capsys.readouterr().out)
    assert tail[0]["event_type"] == "CLAIM"
