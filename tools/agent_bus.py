"""CLI for the FXPM agent protocol bus (agent_comms/PROTOCOL.md).

Validates messages, assigns ids/timestamps, enforces file bounds, and keeps
the lane registry consistent so Claude and Codex never edit the same core
file concurrently.

Usage:
  python tools/agent_bus.py post --from claude --type status --body "..." \
      [--to codex] [--paths a.py b.py] [--refs CX-003]
  python tools/agent_bus.py claim --from codex --paths pm_pipeline.py --purpose "..."
  python tools/agent_bus.py release --from codex --lane L-0003 [--note "..."]
  python tools/agent_bus.py tail [--count 20] [--unread-for claude]
  python tools/agent_bus.py lanes
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

ROOT = Path(__file__).resolve().parents[1]
COMMS_DIR = ROOT / "agent_comms"
BUS_PATH = COMMS_DIR / "BUS.jsonl"
LANES_PATH = COMMS_DIR / "LANES.json"

AGENTS = ("claude", "codex")
AGENT_PREFIX = {"claude": "CC", "codex": "CX"}
MESSAGE_TYPES = (
    "claim", "release", "status", "finding", "question", "answer",
    "decision", "veto", "review_request", "review",
)
BUS_MAX_BYTES = 4 * 1024 * 1024
LANE_HISTORY_MAX = 100
LANE_HISTORY_KEEP_DAYS = 14


def _now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _read_bus_lines() -> List[Dict[str, Any]]:
    if not BUS_PATH.exists():
        return []
    records = []
    for line in BUS_PATH.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            payload = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(payload, dict):
            records.append(payload)
    return records


def _next_id(agent: str, records: List[Dict[str, Any]]) -> str:
    prefix = AGENT_PREFIX[agent]
    highest = 0
    for record in records:
        rid = str(record.get("id") or "")
        if rid.startswith(prefix + "-"):
            try:
                highest = max(highest, int(rid.split("-", 1)[1]))
            except ValueError:
                continue
    # Include archived ids so rotation never resets numbering.
    for archive in sorted(COMMS_DIR.glob("BUS.archive-*.jsonl")):
        for line in archive.read_text(encoding="utf-8").splitlines():
            if f'"{prefix}-' in line:
                try:
                    rid = json.loads(line).get("id", "")
                    if str(rid).startswith(prefix + "-"):
                        highest = max(highest, int(str(rid).split("-", 1)[1]))
                except (json.JSONDecodeError, ValueError):
                    continue
    return f"{prefix}-{highest + 1:03d}"


def _rotate_bus_if_needed() -> None:
    try:
        if BUS_PATH.exists() and BUS_PATH.stat().st_size > BUS_MAX_BYTES:
            stamp = datetime.now(timezone.utc).strftime("%Y%m%d")
            target = COMMS_DIR / f"BUS.archive-{stamp}.jsonl"
            suffix = 1
            while target.exists():
                suffix += 1
                target = COMMS_DIR / f"BUS.archive-{stamp}-{suffix}.jsonl"
            os.replace(BUS_PATH, target)
    except OSError:
        pass


def _append_bus(record: Dict[str, Any]) -> None:
    COMMS_DIR.mkdir(parents=True, exist_ok=True)
    _rotate_bus_if_needed()
    with open(BUS_PATH, "a", encoding="utf-8") as handle:
        handle.write(json.dumps(record, ensure_ascii=True))
        handle.write("\n")


def _load_lanes() -> Dict[str, Any]:
    if not LANES_PATH.exists():
        return {"next_lane": 1, "lanes": []}
    try:
        payload = json.loads(LANES_PATH.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {"next_lane": 1, "lanes": []}
    if not isinstance(payload, dict) or not isinstance(payload.get("lanes"), list):
        return {"next_lane": 1, "lanes": []}
    payload.setdefault("next_lane", 1)
    return payload


def _prune_lanes(payload: Dict[str, Any]) -> None:
    cutoff = datetime.now(timezone.utc) - timedelta(days=LANE_HISTORY_KEEP_DAYS)
    kept: List[Dict[str, Any]] = []
    released: List[Dict[str, Any]] = []
    for lane in payload["lanes"]:
        if lane.get("status") == "active":
            kept.append(lane)
            continue
        try:
            released_at = datetime.fromisoformat(
                str(lane.get("released_at", "")).replace("Z", "+00:00")
            )
        except ValueError:
            released.append(lane)
            continue
        if released_at >= cutoff:
            released.append(lane)
    released = released[-LANE_HISTORY_MAX:]
    payload["lanes"] = kept + released


def _save_lanes(payload: Dict[str, Any]) -> None:
    COMMS_DIR.mkdir(parents=True, exist_ok=True)
    _prune_lanes(payload)
    tmp = LANES_PATH.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    os.replace(tmp, LANES_PATH)


def _normalize_paths(paths: List[str]) -> List[str]:
    normalized = []
    for item in paths or []:
        value = str(item).replace("\\", "/").strip()
        if value:
            normalized.append(value)
    return normalized


def _conflicting_lane(paths: List[str], agent: str, payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    wanted = set(_normalize_paths(paths))
    for lane in payload["lanes"]:
        if lane.get("status") != "active" or lane.get("owner") == agent:
            continue
        held = set(_normalize_paths(lane.get("paths", [])))
        for want in wanted:
            for have in held:
                if want == have or want.startswith(have.rstrip("/") + "/") or have.startswith(want.rstrip("/") + "/"):
                    return lane
    return None


def cmd_post(args: argparse.Namespace) -> int:
    records = _read_bus_lines()
    record = {
        "id": _next_id(args.agent, records),
        "ts": _now_iso(),
        "from": args.agent,
        "to": args.to,
        "type": args.type,
        "paths": _normalize_paths(args.paths or []),
        "refs": list(args.refs or []),
        "body": args.body,
    }
    _append_bus(record)
    print(record["id"])
    return 0


def cmd_claim(args: argparse.Namespace) -> int:
    payload = _load_lanes()
    paths = _normalize_paths(args.paths)
    if not paths:
        print("ERROR: claim requires --paths", file=sys.stderr)
        return 2
    conflict = _conflicting_lane(paths, args.agent, payload)
    if conflict:
        print(
            f"CONFLICT: {conflict.get('id')} owned by {conflict.get('owner')} "
            f"covers {conflict.get('paths')} — post a question instead of editing",
            file=sys.stderr,
        )
        return 3
    lane_id = f"L-{int(payload.get('next_lane', 1)):04d}"
    payload["next_lane"] = int(payload.get("next_lane", 1)) + 1
    lane = {
        "id": lane_id,
        "owner": args.agent,
        "paths": paths,
        "purpose": args.purpose,
        "status": "active",
        "claimed_at": _now_iso(),
    }
    payload["lanes"].append(lane)
    _save_lanes(payload)
    records = _read_bus_lines()
    _append_bus({
        "id": _next_id(args.agent, records),
        "ts": _now_iso(),
        "from": args.agent,
        "to": "all",
        "type": "claim",
        "paths": paths,
        "refs": [],
        "body": f"{lane_id}: {args.purpose}",
    })
    print(lane_id)
    return 0


def cmd_release(args: argparse.Namespace) -> int:
    payload = _load_lanes()
    for lane in payload["lanes"]:
        if lane.get("id") == args.lane and lane.get("status") == "active":
            if lane.get("owner") != args.agent:
                print(f"ERROR: {args.lane} is owned by {lane.get('owner')}", file=sys.stderr)
                return 2
            lane["status"] = "released"
            lane["released_at"] = _now_iso()
            if args.note:
                lane["release_note"] = args.note
            _save_lanes(payload)
            records = _read_bus_lines()
            _append_bus({
                "id": _next_id(args.agent, records),
                "ts": _now_iso(),
                "from": args.agent,
                "to": "all",
                "type": "release",
                "paths": _normalize_paths(lane.get("paths", [])),
                "refs": [],
                "body": f"{args.lane} released" + (f": {args.note}" if args.note else ""),
            })
            print("released")
            return 0
    print(f"ERROR: no active lane {args.lane}", file=sys.stderr)
    return 2


def cmd_tail(args: argparse.Namespace) -> int:
    records = _read_bus_lines()
    if args.unread_for:
        marker = None
        for record in reversed(records):
            if record.get("from") == args.unread_for:
                marker = record.get("id")
                break
        if marker is not None:
            index = next(
                (i for i, record in enumerate(records) if record.get("id") == marker),
                -1,
            )
            records = records[index + 1:]
    for record in records[-max(1, args.count):]:
        print(json.dumps(record, ensure_ascii=True))
    return 0


def cmd_lanes(_args: argparse.Namespace) -> int:
    payload = _load_lanes()
    active = [lane for lane in payload["lanes"] if lane.get("status") == "active"]
    print(json.dumps(active, indent=2, sort_keys=True))
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="command", required=True)

    post = sub.add_parser("post", help="post a validated bus message")
    post.add_argument("--from", dest="agent", required=True, choices=AGENTS)
    post.add_argument("--to", default="all", choices=AGENTS + ("all",))
    post.add_argument("--type", required=True, choices=MESSAGE_TYPES)
    post.add_argument("--body", required=True)
    post.add_argument("--paths", nargs="*", default=[])
    post.add_argument("--refs", nargs="*", default=[])
    post.set_defaults(func=cmd_post)

    claim = sub.add_parser("claim", help="claim a file lane before editing shared code")
    claim.add_argument("--from", dest="agent", required=True, choices=AGENTS)
    claim.add_argument("--paths", nargs="+", required=True)
    claim.add_argument("--purpose", required=True)
    claim.set_defaults(func=cmd_claim)

    release = sub.add_parser("release", help="release a finished lane")
    release.add_argument("--from", dest="agent", required=True, choices=AGENTS)
    release.add_argument("--lane", required=True)
    release.add_argument("--note", default="")
    release.set_defaults(func=cmd_release)

    tail = sub.add_parser("tail", help="show recent bus messages")
    tail.add_argument("--count", type=int, default=20)
    tail.add_argument("--unread-for", default="", choices=("",) + AGENTS)
    tail.set_defaults(func=cmd_tail)

    lanes = sub.add_parser("lanes", help="show active lanes")
    lanes.set_defaults(func=cmd_lanes)
    return parser


def main(argv: Optional[List[str]] = None) -> int:
    args = build_parser().parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
