"""Transactional coordination ledger for Codex, Claude Code, and reviewers.

The ledger is intentionally small and local.  Every read and write takes the
same OS lock.  Events are hash chained, file claims are checked in the locked
transaction, and major work cannot be completed without an exact immutable
artifact plus an ACCEPT from another actor.

Raw traffic is kept in finite segments.  When the segment budget is exhausted
the live state is written to a hash-anchored CHECKPOINT and superseded raw
segments are removed.  The checkpoint preserves every active claim, current
decision, unresolved veto, and review gate needed by ongoing work.
"""

from __future__ import annotations

import argparse
import copy
import hashlib
import json
import os
import re
import sys
import time
import uuid
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Iterator, Mapping, Optional, Sequence


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_EVENT_FILE = PROJECT_ROOT / "coordination" / "events.jsonl"
SCHEMA_VERSION = "fxpm.agent_coordination.v2"
MANIFEST_SCHEMA_VERSION = "fxpm.agent_coordination.manifest.v1"

PUBLIC_EVENT_TYPES = {
    "CLAIM",
    "UPDATE",
    "STATUS",
    "FINDING",
    "QUESTION",
    "ANSWER",
    "DECISION",
    "VETO",
    "REVIEW_REQUEST",
    "REVIEW",
    "COMPLETE",
    "RELEASE",
    "ABANDON",
}
INTERNAL_EVENT_TYPES = {"CHECKPOINT", "RECOVERY"}
EVENT_TYPES = PUBLIC_EVENT_TYPES | INTERNAL_EVENT_TYPES
MESSAGE_EVENT_TYPES = {"UPDATE", "STATUS", "FINDING", "QUESTION", "ANSWER"}
REVIEW_VERDICTS = {"ACCEPT", "REJECT", "CHANGES_REQUIRED"}

DEFAULT_SEGMENT_MAX_BYTES = 4 * 1024 * 1024
DEFAULT_RETAIN_SEGMENTS = 4
DEFAULT_MAX_EVENT_BYTES = 256 * 1024
DEFAULT_MAX_CHECKPOINT_BYTES = 1024 * 1024
DEFAULT_LOCK_TIMEOUT_SECONDS = 30.0
MAX_MESSAGE_CHARS = 64 * 1024
MAX_PATHS_PER_CLAIM = 128
MAX_ACTIVE_CLAIMS = 256
MAX_CURRENT_REVIEWS = 512
MAX_CURRENT_DECISIONS = 256
MAX_ACTIVE_VETOES = 256
MAX_COMPLETION_SUMMARIES = 128

_ACTOR_RE = re.compile(r"^[A-Z0-9_-]+$")
_FULL_GIT_RE = re.compile(r"^[0-9a-f]{40}$")
_GIT_ARTIFACT_RE = re.compile(r"^git:([0-9a-f]{40})$")
_DIFF_ARTIFACT_RE = re.compile(r"^diff:([0-9a-f]{64})@base:([0-9a-f]{40})$")
_WINDOWS_RESERVED = {
    "CON", "PRN", "AUX", "NUL",
    *(f"COM{i}" for i in range(1, 10)),
    *(f"LPT{i}" for i in range(1, 10)),
}
_GENESIS_COMMITMENT = hashlib.sha256(b"FXPM_AGENT_COORDINATION_GENESIS_V2").hexdigest()


class CoordinationError(RuntimeError):
    """The ledger or requested state transition is invalid."""


class LockTimeoutError(CoordinationError):
    """Another process held the coordination lock beyond the allowed wait."""


class TruncatedTailError(CoordinationError):
    """The active file ends in an incomplete, non-newline-terminated record."""


def _canonical(payload: Mapping[str, Any]) -> bytes:
    return json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    ).encode("utf-8")


def _sha256(payload: Mapping[str, Any]) -> str:
    return hashlib.sha256(_canonical(payload)).hexdigest()


def _event_hash(event_without_hash: Mapping[str, Any]) -> str:
    return _sha256(event_without_hash)


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _normal_actor(value: Any) -> str:
    actor = str(value or "").strip().upper()
    if not actor or not _ACTOR_RE.fullmatch(actor):
        raise CoordinationError("actor must contain only letters, digits, underscore, or hyphen")
    return actor


def _normal_task(value: Any) -> str:
    task = str(value or "").strip()
    if not task or len(task) > 256:
        raise CoordinationError("task id is required and must be at most 256 characters")
    return task


def _normal_message(value: Any, *, required: bool = False) -> str:
    message = str(value or "").strip()
    if required and not message:
        raise CoordinationError("a non-empty message is required")
    if len(message) > MAX_MESSAGE_CHARS:
        raise CoordinationError(f"message exceeds {MAX_MESSAGE_CHARS} characters")
    return message


def _normal_refs(values: Iterable[Any]) -> list[str]:
    output: list[str] = []
    for value in values or []:
        ref = str(value or "").strip()
        if not ref or len(ref) > 128 or any(character.isspace() for character in ref):
            raise CoordinationError(f"invalid event reference: {value!r}")
        if ref not in output:
            output.append(ref)
    return output


def _normal_path(value: Any) -> tuple[str, str]:
    raw = str(value or "").strip().replace("\\", "/")
    if (
        not raw
        or raw.startswith("/")
        or ":" in raw
        or "\x00" in raw
        or any(character in raw for character in "*?[]")
    ):
        raise CoordinationError(f"claim path must be a safe relative literal path: {value!r}")
    pieces = raw.split("/")
    if any(piece in {"", ".", ".."} for piece in pieces):
        raise CoordinationError(f"claim path is unsafe: {value!r}")
    for piece in pieces:
        if piece.endswith((" ", ".")):
            raise CoordinationError(f"Windows-ambiguous claim path component: {piece!r}")
        stem = piece.split(".", 1)[0].upper()
        if stem in _WINDOWS_RESERVED:
            raise CoordinationError(f"Windows-reserved claim path component: {piece!r}")
    display = "/".join(pieces)
    # FXPM is deployed on Windows.  Always casefold claim keys so a ledger
    # written on Linux cannot later permit an alias collision on Windows.
    return display, display.casefold()


def _normal_paths(values: Iterable[Any]) -> tuple[list[str], list[str]]:
    display: list[str] = []
    keys: list[str] = []
    for value in values or []:
        item, key = _normal_path(value)
        if key not in keys:
            display.append(item)
            keys.append(key)
    if not display:
        raise CoordinationError("at least one claim path is required")
    if len(display) > MAX_PATHS_PER_CLAIM:
        raise CoordinationError(f"a claim may contain at most {MAX_PATHS_PER_CLAIM} paths")
    ordered = sorted(zip(keys, display))
    return [item for _, item in ordered], [key for key, _ in ordered]


def _paths_overlap(left: str, right: str) -> bool:
    a = str(left).rstrip("/").casefold()
    b = str(right).rstrip("/").casefold()
    return a == b or a.startswith(b + "/") or b.startswith(a + "/")


def _normal_artifact(value: Any) -> str:
    raw = str(value or "").strip().lower()
    if _FULL_GIT_RE.fullmatch(raw):
        return f"git:{raw}"
    match = _GIT_ARTIFACT_RE.fullmatch(raw)
    if match:
        return f"git:{match.group(1)}"
    match = _DIFF_ARTIFACT_RE.fullmatch(raw)
    if match:
        return f"diff:{match.group(1)}@base:{match.group(2)}"
    raise CoordinationError(
        "artifact must be an exact 40-hex Git commit or "
        "diff:<64-hex-sha256>@base:<40-hex-commit>"
    )


@contextmanager
def _locked_file(lock_path: Path, timeout_seconds: float) -> Iterator[None]:
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    deadline = time.monotonic() + max(0.1, float(timeout_seconds))
    with lock_path.open("a+b") as handle:
        handle.seek(0, os.SEEK_END)
        if handle.tell() == 0:
            handle.write(b"0")
            handle.flush()
            os.fsync(handle.fileno())
        handle.seek(0)
        if os.name == "nt":
            import msvcrt

            while True:
                try:
                    msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
                    break
                except OSError as exc:
                    if time.monotonic() >= deadline:
                        raise LockTimeoutError(f"timed out waiting for {lock_path}") from exc
                    time.sleep(0.025)
            try:
                yield
            finally:
                handle.seek(0)
                msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
        else:
            import fcntl

            while True:
                try:
                    fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                    break
                except BlockingIOError as exc:
                    if time.monotonic() >= deadline:
                        raise LockTimeoutError(f"timed out waiting for {lock_path}") from exc
                    time.sleep(0.025)
            try:
                yield
            finally:
                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


@dataclass
class ProgramState:
    active_claims: dict[str, dict[str, Any]] = field(default_factory=dict)
    reviews: dict[str, dict[str, Any]] = field(default_factory=dict)
    decisions: dict[str, dict[str, Any]] = field(default_factory=dict)
    active_vetoes: dict[str, dict[str, Any]] = field(default_factory=dict)
    completions: list[dict[str, Any]] = field(default_factory=list)

    def snapshot(self) -> dict[str, Any]:
        return {
            "active_claims": copy.deepcopy(self.active_claims),
            "reviews": copy.deepcopy(self.reviews),
            "decisions": copy.deepcopy(self.decisions),
            "active_vetoes": copy.deepcopy(self.active_vetoes),
            "completions": copy.deepcopy(self.completions[-MAX_COMPLETION_SUMMARIES:]),
        }

    @classmethod
    def from_snapshot(cls, value: Any) -> "ProgramState":
        if not isinstance(value, dict):
            raise CoordinationError("checkpoint state must be an object")
        mappings: dict[str, dict[str, Any]] = {}
        for key in ("active_claims", "reviews", "decisions", "active_vetoes"):
            item = value.get(key, {})
            if not isinstance(item, dict):
                raise CoordinationError(f"checkpoint {key} must be an object")
            mappings[key] = copy.deepcopy(item)
        completions = value.get("completions", [])
        if not isinstance(completions, list):
            raise CoordinationError("checkpoint completions must be a list")
        return cls(completions=copy.deepcopy(completions), **mappings)

    def apply(self, event: Mapping[str, Any]) -> None:
        kind = str(event.get("event_type") or "")
        payload = event.get("payload") or {}
        event_id = str(event.get("event_id") or "")
        if kind == "CHECKPOINT":
            restored = ProgramState.from_snapshot(payload.get("state"))
            self.active_claims = restored.active_claims
            self.reviews = restored.reviews
            self.decisions = restored.decisions
            self.active_vetoes = restored.active_vetoes
            self.completions = restored.completions
        elif kind == "CLAIM":
            self.active_claims[event_id] = {
                "claim_event_id": event_id,
                "actor": event.get("actor"),
                "task_id": event.get("task_id"),
                "paths": list(payload.get("paths") or []),
                "path_keys": list(payload.get("path_keys") or []),
                "summary": payload.get("message", ""),
                "major": bool(payload.get("major", True)),
                "claimed_sequence": event.get("sequence"),
                "completed": False,
                "artifact": None,
                "complete_event_id": None,
            }
        elif kind == "REVIEW_REQUEST":
            self.reviews[event_id] = {
                "request_event_id": event_id,
                "actor": event.get("actor"),
                "task_id": event.get("task_id"),
                "claim_event_id": payload.get("claim_event_id"),
                "artifact": payload.get("artifact"),
                "paths": list(payload.get("paths") or []),
                "request_sequence": event.get("sequence"),
                "message": payload.get("message", ""),
                "latest_review": None,
            }
        elif kind == "REVIEW":
            request_id = str(payload.get("request_event_id") or "")
            if request_id in self.reviews:
                self.reviews[request_id]["latest_review"] = {
                    "review_event_id": event_id,
                    "actor": event.get("actor"),
                    "verdict": payload.get("verdict"),
                    "message": payload.get("message", ""),
                    "review_sequence": event.get("sequence"),
                }
        elif kind == "DECISION":
            self.decisions[str(payload.get("key") or "")] = {
                "decision_event_id": event_id,
                "actor": event.get("actor"),
                "task_id": event.get("task_id"),
                "key": payload.get("key"),
                "message": payload.get("message", ""),
                "sequence": event.get("sequence"),
            }
            resolved = str(payload.get("resolves_veto") or "")
            if resolved:
                self.active_vetoes.pop(resolved, None)
        elif kind == "VETO":
            self.active_vetoes[event_id] = {
                "veto_event_id": event_id,
                "actor": event.get("actor"),
                "task_id": event.get("task_id"),
                "message": payload.get("message", ""),
                "evidence": payload.get("evidence", ""),
                "sequence": event.get("sequence"),
            }
        elif kind == "COMPLETE":
            claim_id = str(payload.get("claim_event_id") or "")
            claim = self.active_claims.get(claim_id)
            if claim is not None:
                claim["completed"] = True
                claim["artifact"] = payload.get("artifact")
                claim["complete_event_id"] = event_id
        elif kind in {"RELEASE", "ABANDON"}:
            claim_id = str(payload.get("claim_event_id") or "")
            claim = self.active_claims.pop(claim_id, None)
            if claim is not None:
                self.completions.append({
                    "claim_event_id": claim_id,
                    "actor": claim.get("actor"),
                    "task_id": claim.get("task_id"),
                    "outcome": "RELEASED" if kind == "RELEASE" else "ABANDONED",
                    "artifact": claim.get("artifact"),
                    "event_id": event_id,
                    "sequence": event.get("sequence"),
                    "message": payload.get("message", ""),
                })
                self.completions = self.completions[-MAX_COMPLETION_SUMMARIES:]
                for request_id in [
                    key for key, review in self.reviews.items()
                    if review.get("claim_event_id") == claim_id
                ]:
                    self.reviews.pop(request_id, None)


def _replay_events(events: Sequence[Mapping[str, Any]]) -> ProgramState:
    state = ProgramState()
    for event in events:
        state.apply(event)
    return state


def _genesis_anchor() -> dict[str, Any]:
    return {
        "sequence": 0,
        "event_hash": None,
        "previous_commitment": None,
        "commitment": _GENESIS_COMMITMENT,
    }


def _next_anchor(previous: Mapping[str, Any], terminal_event: Mapping[str, Any]) -> dict[str, Any]:
    material = {
        "previous_commitment": previous.get("commitment"),
        "sequence": int(terminal_event.get("sequence") or 0),
        "event_hash": terminal_event.get("event_hash"),
    }
    return {**material, "commitment": _sha256(material)}


def _validate_anchor(value: Any) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise CoordinationError("manifest chain_anchor must be an object")
    try:
        sequence = int(value.get("sequence"))
    except (TypeError, ValueError) as exc:
        raise CoordinationError("chain anchor sequence is invalid") from exc
    event_hash = value.get("event_hash")
    previous = value.get("previous_commitment")
    commitment = str(value.get("commitment") or "")
    if sequence == 0:
        if event_hash is not None or previous is not None or commitment != _GENESIS_COMMITMENT:
            raise CoordinationError("genesis chain anchor is invalid")
    else:
        if not isinstance(event_hash, str) or len(event_hash) != 64:
            raise CoordinationError("chain anchor event hash is invalid")
        material = {
            "previous_commitment": previous,
            "sequence": sequence,
            "event_hash": event_hash,
        }
        if commitment != _sha256(material):
            raise CoordinationError("chain anchor commitment mismatch")
    return {
        "sequence": sequence,
        "event_hash": event_hash,
        "previous_commitment": previous,
        "commitment": commitment,
    }


def _artifact_review_gate(
    claim_id: str,
    artifact: str,
    state: ProgramState,
) -> tuple[bool, str]:
    claim = state.active_claims.get(claim_id)
    if claim is None:
        return False, "claim is not active"
    matching = [
        review
        for review in state.reviews.values()
        if review.get("claim_event_id") == claim_id and review.get("artifact") == artifact
    ]
    if not matching:
        return False, "no review request exists for the exact artifact"
    latest = max(matching, key=lambda item: int(item.get("request_sequence") or 0))
    review = latest.get("latest_review")
    if not isinstance(review, dict):
        return False, f"review request {latest.get('request_event_id')} has no verdict"
    if review.get("actor") == claim.get("actor"):
        return False, "review is not independent of the claim owner"
    if review.get("verdict") != "ACCEPT":
        return False, f"latest verdict is {review.get('verdict')}"
    return True, f"accepted by {review.get('actor')} via {review.get('review_event_id')}"


def _task_vetoes(task_id: str, state: ProgramState) -> list[dict[str, Any]]:
    return [
        veto for veto in state.active_vetoes.values()
        if veto.get("task_id") == task_id
    ]


def _atomic_write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    unsigned = dict(payload)
    unsigned.pop("manifest_hash", None)
    encoded_payload = {**unsigned, "manifest_hash": _sha256(unsigned)}
    encoded = json.dumps(encoded_payload, indent=2, sort_keys=True).encode("utf-8") + b"\n"
    temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
    with temporary.open("xb") as handle:
        handle.write(encoded)
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary, path)
    _fsync_directory(path.parent)


def _create_file(path: Path, content: bytes = b"") -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("xb") as handle:
        if content:
            handle.write(content)
        handle.flush()
        os.fsync(handle.fileno())
    _fsync_directory(path.parent)


def _fsync_directory(path: Path) -> None:
    """Persist directory entries on POSIX; Windows flushes via file handles."""
    if os.name == "nt":
        return
    descriptor: Optional[int] = None
    try:
        descriptor = os.open(path, os.O_RDONLY)
        os.fsync(descriptor)
    except OSError:
        # Some network/virtual filesystems do not permit directory fsync. The
        # event and manifest files themselves have already been fsynced.
        pass
    finally:
        if descriptor is not None:
            os.close(descriptor)


class EventStore:
    """A finite, process-safe event store rooted at one logical event path."""

    def __init__(
        self,
        event_file: Path = DEFAULT_EVENT_FILE,
        *,
        segment_max_bytes: int = DEFAULT_SEGMENT_MAX_BYTES,
        retain_segments: int = DEFAULT_RETAIN_SEGMENTS,
        max_event_bytes: int = DEFAULT_MAX_EVENT_BYTES,
        max_checkpoint_bytes: int = DEFAULT_MAX_CHECKPOINT_BYTES,
        lock_timeout_seconds: float = DEFAULT_LOCK_TIMEOUT_SECONDS,
    ) -> None:
        self.event_file = Path(event_file).resolve()
        self.base_dir = self.event_file.parent
        self.manifest_path = self.event_file.with_suffix(self.event_file.suffix + ".manifest.json")
        self.lock_path = self.event_file.with_suffix(self.event_file.suffix + ".lock")
        self.segment_max_bytes = max(512, int(segment_max_bytes))
        self.retain_segments = max(1, min(32, int(retain_segments)))
        self.max_event_bytes = max(512, int(max_event_bytes))
        self.max_checkpoint_bytes = max(4096, int(max_checkpoint_bytes))
        self.lock_timeout_seconds = max(0.1, float(lock_timeout_seconds))

    def _path_from_name(self, name: Any) -> Path:
        value = str(name or "")
        if not value or Path(value).name != value:
            raise CoordinationError(f"unsafe ledger filename in manifest: {value!r}")
        return self.base_dir / value

    def _new_current_name(self, generation: int) -> str:
        return f"{self.event_file.stem}.g{generation:06d}.{uuid.uuid4().hex}.jsonl"

    def _initial_manifest_locked(self) -> dict[str, Any]:
        self.base_dir.mkdir(parents=True, exist_ok=True)
        if self.event_file.exists() and self.event_file.stat().st_size:
            raise CoordinationError(
                f"{self.event_file} predates the transactional manifest; migrate it explicitly"
            )
        if not self.event_file.exists():
            _create_file(self.event_file)
        manifest = {
            "schema_version": MANIFEST_SCHEMA_VERSION,
            "generation": 0,
            "chain_anchor": _genesis_anchor(),
            "segments": [],
            "current": self.event_file.name,
            "settings": {
                "segment_max_bytes": self.segment_max_bytes,
                "retain_segments": self.retain_segments,
                "max_event_bytes": self.max_event_bytes,
                "max_checkpoint_bytes": self.max_checkpoint_bytes,
            },
        }
        _atomic_write_json(self.manifest_path, manifest)
        return self._read_manifest_locked()

    def _read_manifest_locked(self) -> dict[str, Any]:
        if not self.manifest_path.exists():
            return self._initial_manifest_locked()
        try:
            payload = json.loads(self.manifest_path.read_text(encoding="utf-8"))
        except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise CoordinationError(f"invalid coordination manifest: {exc}") from exc
        if not isinstance(payload, dict):
            raise CoordinationError("coordination manifest must be an object")
        observed = str(payload.get("manifest_hash") or "")
        unsigned = dict(payload)
        unsigned.pop("manifest_hash", None)
        if observed != _sha256(unsigned):
            raise CoordinationError("coordination manifest hash mismatch")
        if payload.get("schema_version") != MANIFEST_SCHEMA_VERSION:
            raise CoordinationError("coordination manifest schema mismatch")
        _validate_anchor(payload.get("chain_anchor"))
        segments = payload.get("segments")
        if not isinstance(segments, list):
            raise CoordinationError("manifest segments must be a list")
        if len(segments) > 32:
            raise CoordinationError("manifest segment count exceeds the hard safety bound")
        for segment in segments:
            if not isinstance(segment, dict):
                raise CoordinationError("manifest segment entry must be an object")
            self._path_from_name(segment.get("file"))
        self._path_from_name(payload.get("current"))
        settings = payload.get("settings")
        if not isinstance(settings, dict):
            raise CoordinationError("manifest settings must be an object")
        self.segment_max_bytes = max(512, int(settings.get("segment_max_bytes")))
        self.retain_segments = max(1, min(32, int(settings.get("retain_segments"))))
        self.max_event_bytes = max(512, int(settings.get("max_event_bytes")))
        self.max_checkpoint_bytes = max(4096, int(settings.get("max_checkpoint_bytes")))
        return payload

    def _read_jsonl_file_locked(
        self,
        path: Path,
        *,
        recover_final_tail: bool,
    ) -> tuple[list[dict[str, Any]], int]:
        if not path.exists():
            raise CoordinationError(f"referenced ledger file is missing: {path}")
        try:
            data = path.read_bytes()
        except OSError as exc:
            raise CoordinationError(f"cannot read ledger file {path}: {exc}") from exc
        removed = 0
        if data and not data.endswith(b"\n"):
            if not recover_final_tail:
                raise TruncatedTailError(
                    f"{path} has a truncated final line; run repair-tail or append through the tool"
                )
            boundary = data.rfind(b"\n") + 1
            removed = len(data) - boundary
            with path.open("r+b") as handle:
                handle.truncate(boundary)
                handle.flush()
                os.fsync(handle.fileno())
            data = data[:boundary]
        records: list[dict[str, Any]] = []
        for line_number, raw_line in enumerate(data.splitlines(), start=1):
            if not raw_line.strip():
                raise CoordinationError(f"blank record in {path} at line {line_number}")
            try:
                decoded = raw_line.decode("utf-8")
                record = json.loads(decoded)
            except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                # Newline-terminated or non-final corruption is never repaired.
                raise CoordinationError(
                    f"invalid JSONL in {path} at line {line_number}: {exc}"
                ) from exc
            if not isinstance(record, dict):
                raise CoordinationError(f"non-object event in {path} at line {line_number}")
            records.append(record)
        return records, removed

    def _load_locked(
        self,
        *,
        recover_final_tail: bool = False,
    ) -> tuple[dict[str, Any], list[dict[str, Any]], ProgramState, int]:
        manifest = self._read_manifest_locked()
        anchor = _validate_anchor(manifest.get("chain_anchor"))
        file_names = [segment.get("file") for segment in manifest.get("segments", [])]
        file_names.append(manifest.get("current"))
        events: list[dict[str, Any]] = []
        recovered_bytes = 0
        for index, name in enumerate(file_names):
            records, removed = self._read_jsonl_file_locked(
                self._path_from_name(name),
                recover_final_tail=recover_final_tail and index == len(file_names) - 1,
            )
            if removed and index != len(file_names) - 1:
                raise CoordinationError("only the active file tail may be recovered")
            recovered_bytes += removed
            events.extend(records)

        expected_previous = anchor.get("event_hash")
        expected_sequence = int(anchor.get("sequence") or 0) + 1
        for index, event in enumerate(events):
            if event.get("schema_version") != SCHEMA_VERSION:
                raise CoordinationError(f"event schema mismatch at retained index {index}")
            if event.get("event_type") not in EVENT_TYPES:
                raise CoordinationError(f"unknown event type at retained index {index}")
            try:
                sequence = int(event.get("sequence"))
            except (TypeError, ValueError) as exc:
                raise CoordinationError(f"invalid sequence at retained index {index}") from exc
            if sequence != expected_sequence:
                raise CoordinationError(
                    f"event sequence mismatch at retained index {index}: "
                    f"expected {expected_sequence}, observed {sequence}"
                )
            if event.get("previous_hash") != expected_previous:
                raise CoordinationError(f"hash-chain predecessor mismatch at retained index {index}")
            observed_hash = str(event.get("event_hash") or "")
            unsigned = dict(event)
            unsigned.pop("event_hash", None)
            if observed_hash != _event_hash(unsigned):
                raise CoordinationError(f"event hash mismatch at retained index {index}")
            if event.get("event_type") == "CHECKPOINT" and index != 0:
                raise CoordinationError("CHECKPOINT may only be the first retained event")
            expected_previous = observed_hash
            expected_sequence += 1

        if int(anchor.get("sequence") or 0) > 0:
            if not events or events[0].get("event_type") != "CHECKPOINT":
                raise CoordinationError("a compacted ledger must begin with CHECKPOINT")
            if (events[0].get("payload") or {}).get("chain_anchor") != anchor:
                raise CoordinationError("CHECKPOINT chain anchor does not match manifest")
        state = _replay_events(events)
        self._validate_state_bounds(state)
        return manifest, events, state, recovered_bytes

    def _validate_state_bounds(self, state: ProgramState) -> None:
        if len(state.active_claims) > MAX_ACTIVE_CLAIMS:
            raise CoordinationError("active claim bound exceeded")
        if len(state.reviews) > MAX_CURRENT_REVIEWS:
            raise CoordinationError("current review bound exceeded")
        if len(state.decisions) > MAX_CURRENT_DECISIONS:
            raise CoordinationError("current decision bound exceeded")
        if len(state.active_vetoes) > MAX_ACTIVE_VETOES:
            raise CoordinationError("active veto bound exceeded")
        snapshot_bytes = len(_canonical(state.snapshot()))
        # Reserve deterministic room for the CHECKPOINT event envelope,
        # anchor, UUID, timestamp, hashes, and retention metadata. This check
        # happens before an accepted event is written, avoiding a later
        # rotation failure after persistence.
        snapshot_budget = max(1024, self.max_checkpoint_bytes - 4096)
        if snapshot_bytes > snapshot_budget:
            raise CoordinationError(
                f"live coordination state needs {snapshot_bytes} bytes, above checkpoint state "
                f"budget {snapshot_budget}; close or supersede stale operational state"
            )

    def _claim_for_owner(
        self,
        claim_id: Any,
        actor: str,
        task_id: str,
        state: ProgramState,
    ) -> dict[str, Any]:
        claim = state.active_claims.get(str(claim_id or ""))
        if claim is None:
            raise CoordinationError("claim_event_id does not identify an active claim")
        if claim.get("actor") != actor:
            raise CoordinationError("only the claim owner may change its lifecycle")
        if claim.get("task_id") != task_id:
            raise CoordinationError("event task does not match the claimed task")
        return claim

    def _normalize_transition(
        self,
        *,
        actor: str,
        event_type: str,
        task_id: str,
        payload: Mapping[str, Any],
        state: ProgramState,
    ) -> dict[str, Any]:
        if event_type not in PUBLIC_EVENT_TYPES:
            raise CoordinationError(f"unsupported public event type: {event_type}")
        normalized = dict(payload)
        if "message" in normalized:
            normalized["message"] = _normal_message(normalized.get("message"))

        if event_type == "CLAIM":
            paths, keys = _normal_paths(normalized.get("paths") or [])
            for claim in state.active_claims.values():
                for requested in keys:
                    for held in claim.get("path_keys") or []:
                        if _paths_overlap(requested, held):
                            raise CoordinationError(
                                f"claim conflicts with {claim.get('actor')} task "
                                f"{claim.get('task_id')} ({claim.get('claim_event_id')}): "
                                f"{requested} overlaps {held}"
                            )
            if len(state.active_claims) >= MAX_ACTIVE_CLAIMS:
                raise CoordinationError("active claim bound reached")
            normalized.update({
                "paths": paths,
                "path_keys": keys,
                "major": bool(normalized.get("major", True)),
                "message": _normal_message(normalized.get("message"), required=True),
            })

        elif event_type == "REVIEW_REQUEST":
            claim = self._claim_for_owner(
                normalized.get("claim_event_id"), actor, task_id, state
            )
            if claim.get("completed"):
                raise CoordinationError("a completed claim cannot request another review")
            artifact = _normal_artifact(normalized.get("artifact"))
            requested_values = normalized.get("paths") or claim.get("paths") or []
            paths, keys = _normal_paths(requested_values)
            claim_keys = list(claim.get("path_keys") or [])
            for key in keys:
                if not any(key == held or key.startswith(str(held).rstrip("/") + "/") for held in claim_keys):
                    raise CoordinationError(f"review path {key!r} falls outside the claimed lane")
            if len(state.reviews) >= MAX_CURRENT_REVIEWS:
                raise CoordinationError("current review bound reached")
            normalized.update({
                "claim_event_id": claim.get("claim_event_id"),
                "artifact": artifact,
                "paths": paths,
                "path_keys": keys,
                "message": _normal_message(normalized.get("message"), required=True),
            })

        elif event_type == "REVIEW":
            request_id = str(normalized.get("request_event_id") or "")
            request = state.reviews.get(request_id)
            if request is None:
                raise CoordinationError("request_event_id does not identify a current review request")
            claim = state.active_claims.get(str(request.get("claim_event_id") or ""))
            if claim is None:
                raise CoordinationError("review request belongs to a closed claim")
            if actor in {request.get("actor"), claim.get("actor")}:
                raise CoordinationError("a claim owner/requester cannot independently review its own work")
            verdict = str(normalized.get("verdict") or "").strip().upper()
            if verdict not in REVIEW_VERDICTS:
                raise CoordinationError(f"review verdict must be one of {sorted(REVIEW_VERDICTS)}")
            normalized.update({
                "request_event_id": request_id,
                "verdict": verdict,
                "message": _normal_message(normalized.get("message"), required=True),
            })

        elif event_type == "VETO":
            if len(state.active_vetoes) >= MAX_ACTIVE_VETOES:
                raise CoordinationError("active veto bound reached")
            normalized.update({
                "message": _normal_message(normalized.get("message"), required=True),
                "evidence": _normal_message(normalized.get("evidence"), required=True),
            })

        elif event_type == "DECISION":
            key = str(normalized.get("key") or "").strip()
            if not key or len(key) > 256:
                raise CoordinationError("decision key is required and must be at most 256 characters")
            if key not in state.decisions and len(state.decisions) >= MAX_CURRENT_DECISIONS:
                raise CoordinationError("current decision bound reached; supersede an existing key")
            resolves = str(normalized.get("resolves_veto") or "").strip()
            if resolves:
                veto = state.active_vetoes.get(resolves)
                if veto is None:
                    raise CoordinationError("resolves_veto does not identify an active veto")
                if actor not in {veto.get("actor"), "OPERATOR"}:
                    raise CoordinationError("only the veto author or OPERATOR may resolve a veto")
                if task_id != veto.get("task_id"):
                    raise CoordinationError("veto resolution must use the vetoed task id")
            normalized.update({
                "key": key,
                "message": _normal_message(normalized.get("message"), required=True),
                "resolves_veto": resolves or None,
            })

        elif event_type == "COMPLETE":
            claim = self._claim_for_owner(
                normalized.get("claim_event_id"), actor, task_id, state
            )
            if claim.get("completed"):
                raise CoordinationError("claim is already complete; release it explicitly")
            vetoes = _task_vetoes(task_id, state)
            if vetoes:
                raise CoordinationError(
                    f"task is frozen by active veto(es): {[item.get('veto_event_id') for item in vetoes]}"
                )
            artifact: Optional[str] = None
            if claim.get("major"):
                artifact = _normal_artifact(normalized.get("artifact"))
                passed, reason = _artifact_review_gate(
                    str(claim.get("claim_event_id")), artifact, state
                )
                if not passed:
                    raise CoordinationError(f"independent review gate failed: {reason}")
            elif normalized.get("artifact"):
                artifact = _normal_artifact(normalized.get("artifact"))
            normalized.update({
                "claim_event_id": claim.get("claim_event_id"),
                "artifact": artifact,
                "message": _normal_message(normalized.get("message"), required=True),
            })

        elif event_type == "RELEASE":
            claim = self._claim_for_owner(
                normalized.get("claim_event_id"), actor, task_id, state
            )
            if not claim.get("completed"):
                raise CoordinationError("RELEASE requires a prior gated COMPLETE; use ABANDON otherwise")
            vetoes = _task_vetoes(task_id, state)
            if vetoes:
                raise CoordinationError("an active veto freezes release")
            normalized.update({
                "claim_event_id": claim.get("claim_event_id"),
                "artifact": claim.get("artifact"),
                "message": _normal_message(normalized.get("message"), required=True),
            })

        elif event_type == "ABANDON":
            claim = self._claim_for_owner(
                normalized.get("claim_event_id"), actor, task_id, state
            )
            normalized.update({
                "claim_event_id": claim.get("claim_event_id"),
                "message": _normal_message(normalized.get("message"), required=True),
            })

        elif event_type in MESSAGE_EVENT_TYPES:
            normalized["message"] = _normal_message(normalized.get("message"), required=True)
            if normalized.get("paths"):
                paths, keys = _normal_paths(normalized.get("paths") or [])
                normalized.update({"paths": paths, "path_keys": keys})

        return normalized

    @staticmethod
    def _make_event(
        *,
        actor: str,
        event_type: str,
        task_id: str,
        payload: Mapping[str, Any],
        to: str,
        refs: Sequence[str],
        sequence: int,
        previous_hash: Optional[str],
    ) -> dict[str, Any]:
        unsigned: dict[str, Any] = {
            "schema_version": SCHEMA_VERSION,
            "event_id": str(uuid.uuid4()),
            "sequence": int(sequence),
            "timestamp_utc": _utc_now(),
            "actor": actor,
            "to": to,
            "event_type": event_type,
            "task_id": task_id,
            "refs": list(refs),
            "payload": dict(payload),
            "previous_hash": previous_hash,
        }
        return {**unsigned, "event_hash": _event_hash(unsigned)}

    def _append_raw_locked(self, manifest: Mapping[str, Any], event: Mapping[str, Any]) -> None:
        encoded = _canonical(event) + b"\n"
        if event.get("event_type") != "CHECKPOINT" and len(encoded) > self.max_event_bytes:
            raise CoordinationError(
                f"event needs {len(encoded)} bytes, above event bound {self.max_event_bytes}"
            )
        if event.get("event_type") == "CHECKPOINT" and len(encoded) > self.max_checkpoint_bytes:
            raise CoordinationError(
                f"checkpoint needs {len(encoded)} bytes, above bound {self.max_checkpoint_bytes}"
            )
        current_path = self._path_from_name(manifest.get("current"))
        with current_path.open("ab") as handle:
            handle.write(encoded)
            handle.flush()
            os.fsync(handle.fileno())

    def _segment_metadata_locked(self, path: Path) -> dict[str, Any]:
        records, removed = self._read_jsonl_file_locked(path, recover_final_tail=False)
        if removed or not records:
            raise CoordinationError("cannot rotate an empty or truncated active segment")
        return {
            "file": path.name,
            "first_sequence": int(records[0].get("sequence")),
            "last_sequence": int(records[-1].get("sequence")),
            "first_previous_hash": records[0].get("previous_hash"),
            "last_hash": records[-1].get("event_hash"),
            "bytes": path.stat().st_size,
        }

    def _rotate_locked(
        self,
        manifest: dict[str, Any],
        events: list[dict[str, Any]],
        state: ProgramState,
    ) -> dict[str, Any]:
        current_path = self._path_from_name(manifest.get("current"))
        if current_path.stat().st_size < self.segment_max_bytes:
            return manifest
        generation = int(manifest.get("generation") or 0) + 1
        segment = self._segment_metadata_locked(current_path)
        prospective_segments = list(manifest.get("segments") or []) + [segment]
        new_name = self._new_current_name(generation)
        new_path = self._path_from_name(new_name)

        if len(prospective_segments) <= self.retain_segments:
            _create_file(new_path)
            updated = {
                **manifest,
                "generation": generation,
                "segments": prospective_segments,
                "current": new_name,
            }
            _atomic_write_json(self.manifest_path, updated)
            return self._read_manifest_locked()

        if not events:
            raise CoordinationError("cannot compact an empty ledger")
        terminal = events[-1]
        new_anchor = _next_anchor(_validate_anchor(manifest.get("chain_anchor")), terminal)
        checkpoint = self._make_event(
            actor="SYSTEM",
            event_type="CHECKPOINT",
            task_id="coordination-retention",
            payload={
                "message": "Finite-retention checkpoint; superseded raw segments compacted.",
                "chain_anchor": new_anchor,
                "state": state.snapshot(),
                "compacted_event_count": int(terminal.get("sequence") or 0),
            },
            to="ALL",
            refs=[],
            sequence=int(terminal.get("sequence") or 0) + 1,
            previous_hash=str(terminal.get("event_hash") or ""),
        )
        encoded_checkpoint = _canonical(checkpoint) + b"\n"
        if len(encoded_checkpoint) > self.max_checkpoint_bytes:
            raise CoordinationError("bounded checkpoint cannot encode the current operational state")
        _create_file(new_path, encoded_checkpoint)
        old_names = [segment_item.get("file") for segment_item in manifest.get("segments", [])]
        old_names.append(manifest.get("current"))
        updated = {
            **manifest,
            "generation": generation,
            "chain_anchor": new_anchor,
            "segments": [],
            "current": new_name,
        }
        _atomic_write_json(self.manifest_path, updated)
        for name in old_names:
            old_path = self._path_from_name(name)
            if old_path != new_path:
                try:
                    old_path.unlink(missing_ok=True)
                except OSError:
                    # It is unreferenced after the atomic manifest switch and
                    # will be retried by the next cleanup/status transaction.
                    pass
        return self._read_manifest_locked()

    def _cleanup_unreferenced_locked(self, manifest: Mapping[str, Any]) -> None:
        referenced = {str(manifest.get("current") or "")}
        referenced.update(str(item.get("file") or "") for item in manifest.get("segments", []))
        candidates = {self.event_file}
        candidates.update(self.base_dir.glob(f"{self.event_file.stem}.g*.jsonl"))
        for candidate in candidates:
            if candidate.name not in referenced:
                try:
                    candidate.unlink(missing_ok=True)
                except OSError:
                    pass

    def append(
        self,
        *,
        actor: str,
        event_type: str,
        task_id: str,
        payload: Mapping[str, Any],
        to: str = "ALL",
        refs: Sequence[str] = (),
    ) -> dict[str, Any]:
        actor = _normal_actor(actor)
        task_id = _normal_task(task_id)
        event_type = str(event_type or "").strip().upper()
        to = _normal_actor(to)
        refs = _normal_refs(refs)
        if not isinstance(payload, Mapping):
            raise CoordinationError("event payload must be an object")

        with _locked_file(self.lock_path, self.lock_timeout_seconds):
            manifest, events, state, recovered_bytes = self._load_locked(
                recover_final_tail=True
            )
            normalized = self._normalize_transition(
                actor=actor,
                event_type=event_type,
                task_id=task_id,
                payload=payload,
                state=state,
            )
            previous_hash = (
                str(events[-1].get("event_hash"))
                if events
                else _validate_anchor(manifest.get("chain_anchor")).get("event_hash")
            )
            next_sequence = (
                int(events[-1].get("sequence")) + 1
                if events
                else int(_validate_anchor(manifest.get("chain_anchor")).get("sequence")) + 1
            )

            recovery_event: Optional[dict[str, Any]] = None
            if recovered_bytes:
                recovery_event = self._make_event(
                    actor="SYSTEM",
                    event_type="RECOVERY",
                    task_id="coordination-recovery",
                    payload={
                        "message": "Removed one incomplete final JSONL fragment.",
                        "bytes_removed": recovered_bytes,
                        "file": manifest.get("current"),
                    },
                    to="ALL",
                    refs=[],
                    sequence=next_sequence,
                    previous_hash=previous_hash,
                )
                previous_hash = str(recovery_event.get("event_hash"))
                next_sequence += 1

            event = self._make_event(
                actor=actor,
                event_type=event_type,
                task_id=task_id,
                payload=normalized,
                to=to,
                refs=refs,
                sequence=next_sequence,
                previous_hash=previous_hash,
            )
            preview = copy.deepcopy(state)
            preview.apply(event)
            self._validate_state_bounds(preview)
            if len(_canonical(event)) + 1 > self.max_event_bytes:
                raise CoordinationError("event exceeds the configured finite event-size bound")

            if recovery_event is not None:
                self._append_raw_locked(manifest, recovery_event)
                events.append(recovery_event)
            self._append_raw_locked(manifest, event)
            events.append(event)
            state = preview
            manifest = self._rotate_locked(manifest, events, state)
            self._cleanup_unreferenced_locked(manifest)
            return event

    def read_bundle(self) -> tuple[dict[str, Any], list[dict[str, Any]], ProgramState]:
        """Return a verified snapshot while holding the same lock writers use."""
        with _locked_file(self.lock_path, self.lock_timeout_seconds):
            manifest, events, state, _ = self._load_locked(recover_final_tail=False)
            return copy.deepcopy(manifest), copy.deepcopy(events), copy.deepcopy(state)

    def read_events(self) -> list[dict[str, Any]]:
        return self.read_bundle()[1]

    def repair_final_tail(self) -> Optional[dict[str, Any]]:
        """Repair only a non-newline-terminated final fragment and audit it."""
        with _locked_file(self.lock_path, self.lock_timeout_seconds):
            manifest, events, state, recovered_bytes = self._load_locked(
                recover_final_tail=True
            )
            if not recovered_bytes:
                return None
            anchor = _validate_anchor(manifest.get("chain_anchor"))
            previous_hash = events[-1].get("event_hash") if events else anchor.get("event_hash")
            sequence = int(events[-1].get("sequence")) + 1 if events else int(anchor.get("sequence")) + 1
            event = self._make_event(
                actor="SYSTEM",
                event_type="RECOVERY",
                task_id="coordination-recovery",
                payload={
                    "message": "Removed one incomplete final JSONL fragment.",
                    "bytes_removed": recovered_bytes,
                    "file": manifest.get("current"),
                },
                to="ALL",
                refs=[],
                sequence=sequence,
                previous_hash=previous_hash,
            )
            self._append_raw_locked(manifest, event)
            events.append(event)
            manifest = self._rotate_locked(manifest, events, state)
            self._cleanup_unreferenced_locked(manifest)
            return event

    def status(self) -> dict[str, Any]:
        # File sizes belong to the same snapshot as the manifest, so retain
        # the writer lock until every referenced path has been inspected.
        with _locked_file(self.lock_path, self.lock_timeout_seconds):
            manifest, events, state, _ = self._load_locked(recover_final_tail=False)
            anchor = _validate_anchor(manifest.get("chain_anchor"))
            referenced_names = [item.get("file") for item in manifest.get("segments", [])]
            referenced_names.append(manifest.get("current"))
            referenced_bytes = sum(
                self._path_from_name(name).stat().st_size for name in referenced_names
            )
            high_water = int(events[-1].get("sequence")) if events else int(anchor.get("sequence"))
            return {
                "schema_version": SCHEMA_VERSION,
                "valid": True,
                "high_water_sequence": high_water,
                "retained_event_count": len(events),
                "compacted_through_sequence": int(anchor.get("sequence")),
                "chain_anchor_commitment": anchor.get("commitment"),
                "active_claims": list(state.active_claims.values()),
                "open_reviews": [
                    review for review in state.reviews.values()
                    if not isinstance(review.get("latest_review"), dict)
                    or review.get("latest_review", {}).get("verdict") != "ACCEPT"
                ],
                "accepted_reviews": [
                    review for review in state.reviews.values()
                    if isinstance(review.get("latest_review"), dict)
                    and review.get("latest_review", {}).get("verdict") == "ACCEPT"
                ],
                "active_vetoes": list(state.active_vetoes.values()),
                "decisions": list(state.decisions.values()),
                "recent_completions": state.completions,
                "storage": {
                    "referenced_bytes": referenced_bytes,
                    "segment_count": len(manifest.get("segments", [])),
                    "segment_limit": self.retain_segments,
                    "segment_max_bytes": self.segment_max_bytes,
                    "max_event_bytes": self.max_event_bytes,
                    "max_checkpoint_bytes": self.max_checkpoint_bytes,
                    "manifest": str(self.manifest_path),
                    "current": str(self._path_from_name(manifest.get("current"))),
                },
            }

    def tail(
        self,
        *,
        count: int = 20,
        since_sequence: int = 0,
        actor: str = "",
        event_type: str = "",
    ) -> list[dict[str, Any]]:
        events = self.read_events()
        actor_filter = _normal_actor(actor) if actor else ""
        type_filter = str(event_type or "").strip().upper()
        if type_filter and type_filter not in EVENT_TYPES:
            raise CoordinationError(f"unknown event type filter: {type_filter}")
        selected = [
            event for event in events
            if int(event.get("sequence") or 0) > max(0, int(since_sequence))
            and (not actor_filter or event.get("actor") == actor_filter)
            and (not type_filter or event.get("event_type") == type_filter)
        ]
        return selected[-max(1, min(1000, int(count))):]


def append_event(
    *,
    actor: str,
    event_type: str,
    task_id: str,
    payload: Mapping[str, Any],
    event_file: Path = DEFAULT_EVENT_FILE,
    to: str = "ALL",
    refs: Sequence[str] = (),
    segment_max_bytes: int = DEFAULT_SEGMENT_MAX_BYTES,
    retain_segments: int = DEFAULT_RETAIN_SEGMENTS,
    max_event_bytes: int = DEFAULT_MAX_EVENT_BYTES,
    max_checkpoint_bytes: int = DEFAULT_MAX_CHECKPOINT_BYTES,
    lock_timeout_seconds: float = DEFAULT_LOCK_TIMEOUT_SECONDS,
) -> dict[str, Any]:
    store = EventStore(
        event_file,
        segment_max_bytes=segment_max_bytes,
        retain_segments=retain_segments,
        max_event_bytes=max_event_bytes,
        max_checkpoint_bytes=max_checkpoint_bytes,
        lock_timeout_seconds=lock_timeout_seconds,
    )
    return store.append(
        actor=actor,
        event_type=event_type,
        task_id=task_id,
        payload=payload,
        to=to,
        refs=refs,
    )


def load_events(event_file: Path = DEFAULT_EVENT_FILE, *, verify: bool = True) -> list[dict[str, Any]]:
    # ``verify`` remains for v1 API compatibility; verification is now
    # mandatory and cannot be disabled by a caller.
    del verify
    return EventStore(event_file).read_events()


def active_claims(events: Sequence[Mapping[str, Any]]) -> dict[str, dict[str, Any]]:
    return _replay_events(events).active_claims


def review_state(events: Sequence[Mapping[str, Any]]) -> dict[str, dict[str, Any]]:
    return _replay_events(events).reviews


def commit_review_gate(
    artifact: str,
    events: Sequence[Mapping[str, Any]],
) -> tuple[bool, str]:
    """Compatibility gate that rejects prefixes and requires one exact match."""
    normalized = _normal_artifact(artifact)
    state = _replay_events(events)
    claims = {
        str(review.get("claim_event_id"))
        for review in state.reviews.values()
        if review.get("artifact") == normalized
    }
    if len(claims) != 1:
        return False, "artifact does not identify exactly one current claim review"
    return _artifact_review_gate(next(iter(claims)), normalized, state)


def coordination_status(events: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    state = _replay_events(events)
    return {
        "schema_version": SCHEMA_VERSION,
        "retained_event_count": len(events),
        "high_water_sequence": int(events[-1].get("sequence")) if events else 0,
        "active_claims": list(state.active_claims.values()),
        "reviews": list(state.reviews.values()),
        "active_vetoes": list(state.active_vetoes.values()),
        "decisions": list(state.decisions.values()),
        "recent_completions": state.completions,
    }


def _add_event_identity(parser: argparse.ArgumentParser, *, message_required: bool = True) -> None:
    parser.add_argument("--actor", required=True)
    parser.add_argument("--task", required=True)
    parser.add_argument("--message", required=message_required, default="")
    parser.add_argument("--to", default="ALL")
    parser.add_argument("--ref", action="append", default=[])


def _add_artifact_arguments(parser: argparse.ArgumentParser, *, required: bool) -> None:
    group = parser.add_mutually_exclusive_group(required=required)
    group.add_argument("--artifact", default="")
    group.add_argument("--commit", default="", help="alias for an exact 40-hex Git artifact")


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--event-file", type=Path, default=DEFAULT_EVENT_FILE)
    parser.add_argument("--segment-max-bytes", type=int, default=DEFAULT_SEGMENT_MAX_BYTES)
    parser.add_argument("--retain-segments", type=int, default=DEFAULT_RETAIN_SEGMENTS)
    parser.add_argument("--max-event-bytes", type=int, default=DEFAULT_MAX_EVENT_BYTES)
    parser.add_argument("--max-checkpoint-bytes", type=int, default=DEFAULT_MAX_CHECKPOINT_BYTES)
    parser.add_argument("--lock-timeout", type=float, default=DEFAULT_LOCK_TIMEOUT_SECONDS)
    sub = parser.add_subparsers(dest="command", required=True)

    verify = sub.add_parser("verify", help="verify manifest, segments, sequence, and hash chain")
    verify.add_argument("--json", action="store_true")
    status = sub.add_parser("status", help="show derived claims/reviews/vetoes/storage state")
    status.add_argument("--json", action="store_true")
    claims = sub.add_parser("claims", help="show active file claims")
    claims.add_argument("--json", action="store_true")
    tail = sub.add_parser("tail", help="show the verified retained event tail")
    tail.add_argument("--count", type=int, default=20)
    tail.add_argument("--since-sequence", type=int, default=0)
    tail.add_argument("--actor", default="")
    tail.add_argument("--type", default="")
    tail.add_argument("--json", action="store_true")
    sub.add_parser("repair-tail", help="repair only an incomplete active-file final fragment")

    gate = sub.add_parser("gate", help="check the exact-artifact independent review gate")
    gate.add_argument("--claim-id", required=True)
    _add_artifact_arguments(gate, required=True)

    claim = sub.add_parser("claim", help="claim safe repo-relative paths before editing")
    _add_event_identity(claim)
    claim.add_argument("--path", action="append", required=True)
    claim.add_argument("--minor", action="store_true", help="minor lane does not require review")

    post = sub.add_parser("post", help="post a structured status/finding/question/answer/update")
    _add_event_identity(post)
    post.add_argument("--type", required=True, type=str.upper, choices=sorted(MESSAGE_EVENT_TYPES))
    post.add_argument("--path", action="append", default=[])

    for name, event_type in (
        ("update", "UPDATE"),
        ("finding", "FINDING"),
        ("question", "QUESTION"),
        ("answer", "ANSWER"),
        ("announce-status", "STATUS"),
    ):
        command = sub.add_parser(name)
        _add_event_identity(command)
        command.add_argument("--path", action="append", default=[])
        command.set_defaults(event_type=event_type)

    request = sub.add_parser("review-request", help="request review of one immutable artifact")
    _add_event_identity(request)
    request.add_argument("--claim-id", required=True)
    request.add_argument("--path", action="append", default=[])
    _add_artifact_arguments(request, required=True)

    review = sub.add_parser("review", help="record an independent evidence-backed verdict")
    _add_event_identity(review)
    review.add_argument("--request-id", required=True)
    review.add_argument("--verdict", required=True, type=str.upper, choices=sorted(REVIEW_VERDICTS))

    veto = sub.add_parser("veto", help="freeze completion/release for a task")
    _add_event_identity(veto)
    veto.add_argument("--evidence", required=True)

    decision = sub.add_parser("decision", help="record/supersede a decision or resolve a veto")
    _add_event_identity(decision)
    decision.add_argument("--key", required=True)
    decision.add_argument("--resolve-veto", default="")

    complete = sub.add_parser("complete", help="mark a lane complete after its review gate")
    _add_event_identity(complete)
    complete.add_argument("--claim-id", required=True)
    _add_artifact_arguments(complete, required=False)

    release = sub.add_parser("release", help="release a lane only after COMPLETE")
    _add_event_identity(release)
    release.add_argument("--claim-id", required=True)

    abandon = sub.add_parser("abandon", help="close an unfinished lane without claiming success")
    _add_event_identity(abandon)
    abandon.add_argument("--claim-id", required=True)
    return parser


def _store_from_args(args: argparse.Namespace) -> EventStore:
    return EventStore(
        args.event_file,
        segment_max_bytes=args.segment_max_bytes,
        retain_segments=args.retain_segments,
        max_event_bytes=args.max_event_bytes,
        max_checkpoint_bytes=args.max_checkpoint_bytes,
        lock_timeout_seconds=args.lock_timeout,
    )


def _print_json(value: Any) -> None:
    print(json.dumps(value, indent=2, sort_keys=True, ensure_ascii=False))


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = _build_parser().parse_args(argv)
    try:
        store = _store_from_args(args)
        if args.command == "verify":
            status = store.status()
            output = {
                "valid": True,
                "high_water_sequence": status["high_water_sequence"],
                "retained_event_count": status["retained_event_count"],
                "compacted_through_sequence": status["compacted_through_sequence"],
                "chain_anchor_commitment": status["chain_anchor_commitment"],
            }
            _print_json(output) if args.json else print(
                f"valid: sequence={output['high_water_sequence']} "
                f"retained={output['retained_event_count']} "
                f"compacted_through={output['compacted_through_sequence']}"
            )
            return 0
        if args.command == "status":
            output = store.status()
            _print_json(output)
            return 0
        if args.command == "claims":
            output = store.status()["active_claims"]
            _print_json(output)
            return 0
        if args.command == "tail":
            output = store.tail(
                count=args.count,
                since_sequence=args.since_sequence,
                actor=args.actor,
                event_type=args.type,
            )
            if args.json:
                _print_json(output)
            else:
                for event in output:
                    print(json.dumps(event, ensure_ascii=False, sort_keys=True))
            return 0
        if args.command == "repair-tail":
            event = store.repair_final_tail()
            _print_json({"repaired": event is not None, "recovery_event": event})
            return 0
        if args.command == "gate":
            _, _, state = store.read_bundle()
            artifact = _normal_artifact(args.artifact or args.commit)
            passed, reason = _artifact_review_gate(args.claim_id, artifact, state)
            _print_json({"passed": passed, "reason": reason, "artifact": artifact})
            return 0 if passed else 2

        event_type = str(getattr(args, "event_type", "") or "").upper()
        payload: dict[str, Any] = {"message": getattr(args, "message", "")}
        if args.command == "claim":
            event_type = "CLAIM"
            payload.update({"paths": args.path, "major": not args.minor})
        elif args.command == "post":
            event_type = args.type
            if args.path:
                payload["paths"] = args.path
        elif args.command in {"update", "finding", "question", "answer", "announce-status"}:
            if args.path:
                payload["paths"] = args.path
        elif args.command == "review-request":
            event_type = "REVIEW_REQUEST"
            payload.update({
                "claim_event_id": args.claim_id,
                "artifact": args.artifact or args.commit,
                "paths": args.path,
            })
        elif args.command == "review":
            event_type = "REVIEW"
            payload.update({"request_event_id": args.request_id, "verdict": args.verdict})
        elif args.command == "veto":
            event_type = "VETO"
            payload["evidence"] = args.evidence
        elif args.command == "decision":
            event_type = "DECISION"
            payload.update({"key": args.key, "resolves_veto": args.resolve_veto})
        elif args.command == "complete":
            event_type = "COMPLETE"
            payload.update({
                "claim_event_id": args.claim_id,
                "artifact": args.artifact or args.commit,
            })
        elif args.command == "release":
            event_type = "RELEASE"
            payload["claim_event_id"] = args.claim_id
        elif args.command == "abandon":
            event_type = "ABANDON"
            payload["claim_event_id"] = args.claim_id
        else:
            raise CoordinationError(f"unhandled command: {args.command}")

        event = store.append(
            actor=args.actor,
            event_type=event_type,
            task_id=args.task,
            payload=payload,
            to=args.to,
            refs=args.ref,
        )
        _print_json(event)
        return 0
    except CoordinationError as exc:
        print(f"coordination error: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
