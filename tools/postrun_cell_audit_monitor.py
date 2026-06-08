"""Run the specialist-cell audit after a long optimizer run exits.

This is intentionally non-invasive: it does not inspect or alter the optimizer
process. It waits for the configured PID to finish, then runs
benchmark_specialist_cells.py against the configured ledger if the ledger exists.
"""

from __future__ import annotations

import argparse
import csv
import json
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any


REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO / "tools"))

from benchmark_specialist_cells import promotion_safety_summary


def _load_json(path: Path) -> dict[str, Any]:
    try:
        return json.loads(path.read_text(encoding="utf-8-sig"))
    except FileNotFoundError:
        return {}


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
    tmp.replace(path)


def _pid_alive(pid: int) -> bool:
    if pid <= 0:
        return False
    try:
        out = subprocess.check_output(
            ["tasklist", "/FI", f"PID eq {pid}", "/FO", "CSV", "/NH"],
            text=True,
            stderr=subprocess.DEVNULL,
        ).strip()
    except Exception:
        return False
    if not out or "No tasks are running" in out:
        return False
    try:
        row = next(csv.reader([out]))
        return row[0].lower() == "python.exe"
    except Exception:
        return True


def _paths_from_config(config_path: Path) -> tuple[Path, Path]:
    cfg = _load_json(config_path)
    pipe = cfg.get("pipeline") if isinstance(cfg.get("pipeline"), dict) else {}
    output_dir = (REPO / str(pipe.get("output_dir", "pm_outputs/production_live"))).resolve()
    ledger = (REPO / str(pipe.get("winner_ledger_path", output_dir / "pm_configs.json"))).resolve()
    return output_dir, ledger


def _count_statuses(cell_json: Path) -> dict[str, int]:
    payload = _load_json(cell_json)
    rows = payload.get("rows") if isinstance(payload.get("rows"), list) else []
    counts: dict[str, int] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        status = str(row.get("status") or "UNKNOWN")
        counts[status] = counts.get(status, 0) + 1
    return counts


def _promotion_safety(cell_json: Path, label: str) -> dict[str, Any]:
    promotion_json = REPO / "pm_outputs" / "experiments" / f"{label}_promotion.json"
    if not cell_json.exists() or not promotion_json.exists():
        return {
            "available": False,
            "promotion_json": str(promotion_json),
            "reason": "cell audit or promotion review missing",
        }
    try:
        return {
            "available": True,
            "promotion_json": str(promotion_json),
            **promotion_safety_summary(_load_json(cell_json), _load_json(promotion_json)),
        }
    except Exception as exc:
        return {
            "available": False,
            "promotion_json": str(promotion_json),
            "reason": f"{type(exc).__name__}: {exc}",
        }


def _append_comms(
    comms_path: Path,
    label: str,
    cell_json: Path,
    done_path: Path,
    rc: int,
    counts: dict[str, int],
    safety: dict[str, Any],
) -> None:
    counts_text = ", ".join(f"{key}={value}" for key, value in sorted(counts.items())) or "unavailable"
    safety_counts = safety.get("navigation_counts") if isinstance(safety.get("navigation_counts"), dict) else {}
    if safety.get("available"):
        safety_text = (
            f"final_promotes={safety_counts.get('final_promotes', 0)}, "
            f"unsafe_promotes={safety_counts.get('unsafe_promotes', 0)}, "
            f"fail_not_reject={safety_counts.get('fail_not_reject', 0)}, "
            f"insufficient_not_reject={safety_counts.get('insufficient_not_reject', 0)}"
        )
    else:
        safety_text = f"unavailable ({safety.get('reason', 'unknown')})"
    message = (
        f"\n## [Codex -> Claude] {datetime.now().date()} — post-run specialist-cell audit completed\n"
        f"- Label: `{label}`\n"
        f"- Audit return code: `{rc}`\n"
        f"- Cell audit JSON: `{cell_json.relative_to(REPO) if cell_json.is_relative_to(REPO) else cell_json}`\n"
        f"- Done marker: `{done_path.relative_to(REPO) if done_path.is_relative_to(REPO) else done_path}`\n"
        f"- Navigation counts only, not an aggregate verdict: {counts_text}\n"
        f"- Promotion/cell safety cross-check: {safety_text}\n"
        f"- Next step: inspect each deployed FAIL / INSUFFICIENT_EVIDENCE cell on its own merits; no aggregation.\n"
        "- Codex\n"
    )
    with comms_path.open("a", encoding="utf-8") as handle:
        handle.write(message)


def main() -> int:
    parser = argparse.ArgumentParser(description="Run specialist-cell audit after optimizer completion.")
    parser.add_argument("--config", required=True)
    parser.add_argument("--pid", type=int, required=True)
    parser.add_argument("--label", required=True)
    parser.add_argument("--poll-seconds", type=int, default=120)
    parser.add_argument("--status-path", required=True)
    parser.add_argument("--done-path", required=True)
    parser.add_argument("--comms-path", default="AGENT_COMMS.md")
    args = parser.parse_args()

    config_path = (REPO / args.config).resolve()
    output_dir, ledger = _paths_from_config(config_path)
    cell_json = output_dir / "benchmarks" / f"{args.label}_specialist_cells.json"
    log_path = output_dir / "benchmarks" / f"{args.label}_specialist_cells.log"
    status_path = (REPO / args.status_path).resolve()
    done_path = (REPO / args.done_path).resolve()
    comms_path = (REPO / args.comms_path).resolve()

    while _pid_alive(args.pid):
        _write_json(
            status_path,
            {
                "label": args.label,
                "pid": args.pid,
                "pid_alive": True,
                "ledger_exists": ledger.exists(),
                "updated_at": datetime.now().isoformat(),
                "cell_json": str(cell_json),
            },
        )
        time.sleep(max(30, int(args.poll_seconds)))

    rc = None
    output = ""
    if ledger.exists():
        cell_json.parent.mkdir(parents=True, exist_ok=True)
        cmd = [
            sys.executable,
            str(REPO / "tools" / "benchmark_specialist_cells.py"),
            "--ledger",
            str(ledger),
            "--label",
            args.label,
            "--out",
            str(cell_json),
        ]
        proc = subprocess.run(
            cmd,
            cwd=str(REPO),
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
        )
        rc = int(proc.returncode)
        output = proc.stdout
        log_path.write_text(output, encoding="utf-8")
    else:
        rc = 2
        output = f"Skipped specialist-cell audit: ledger missing at {ledger}\n"
        log_path.parent.mkdir(parents=True, exist_ok=True)
        log_path.write_text(output, encoding="utf-8")

    counts = _count_statuses(cell_json) if cell_json.exists() else {}
    safety = _promotion_safety(cell_json, args.label)
    done = {
        "label": args.label,
        "pid": args.pid,
        "pid_alive": _pid_alive(args.pid),
        "completed_at": datetime.now().isoformat(),
        "ledger": str(ledger),
        "ledger_exists": ledger.exists(),
        "cell_json": str(cell_json),
        "cell_json_exists": cell_json.exists(),
        "log_path": str(log_path),
        "returncode": rc,
        "navigation_counts": counts,
        "promotion_cell_safety": safety,
    }
    _write_json(status_path, done)
    _write_json(done_path, done)
    if comms_path.exists():
        _append_comms(comms_path, args.label, cell_json, done_path, int(rc), counts, safety)
    return int(rc)


if __name__ == "__main__":
    raise SystemExit(main())
