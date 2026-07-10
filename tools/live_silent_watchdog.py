"""Live trading silent-failure watchdog.

Checks process liveness and freshness of live sweep/action/trade artifacts.
It is intentionally independent from the trader process, so a stuck or silent
trader cannot mark itself healthy.
"""

from __future__ import annotations

import argparse
import json
import math
import os
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional


def _utc_naive_now() -> datetime:
    return datetime.now()


def _parse_dt(value: Any) -> Optional[datetime]:
    if value in ("", None):
        return None
    try:
        text = str(value).strip()
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        dt = datetime.fromisoformat(text)
        if dt.tzinfo is not None:
            return dt.astimezone().replace(tzinfo=None)
        return dt
    except Exception:
        return None


def _age_seconds(value: Any, *, now: Optional[datetime] = None) -> Optional[float]:
    dt = _parse_dt(value)
    if dt is None:
        return None
    return max(0.0, ((now or _utc_naive_now()) - dt).total_seconds())


def _load_json(path: Path) -> Optional[Any]:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def _newest_timestamp(payload: Any, keys: Iterable[str]) -> Optional[datetime]:
    newest: Optional[datetime] = None
    wanted = set(keys)

    def visit(value: Any) -> None:
        nonlocal newest
        if isinstance(value, dict):
            for key, item in value.items():
                if key in wanted:
                    dt = _parse_dt(item)
                    if dt is not None and (newest is None or dt > newest):
                        newest = dt
                visit(item)
        elif isinstance(value, list):
            for item in value:
                visit(item)

    visit(payload)
    return newest


def _pid_alive(pid: int) -> bool:
    if pid <= 0:
        return False
    try:
        # Windows and POSIX both support signal 0 as a liveness probe in Python.
        os.kill(pid, 0)
        return True
    except PermissionError:
        return True
    except OSError:
        return False


def _finding(status: str, code: str, message: str, **context: Any) -> Dict[str, Any]:
    payload = {"status": status, "code": code, "message": message}
    if context:
        payload["context"] = context
    return payload


def check_live_health(
    *,
    storage_state: Path,
    actionable_log: Path,
    trade_log: Path,
    pid: Optional[int] = None,
    max_sweep_age_seconds: int = 180,
    max_action_age_seconds: int = 900,
    max_trade_log_age_seconds: int = 900,
    now: Optional[datetime] = None,
) -> Dict[str, Any]:
    probe_now = now or _utc_naive_now()
    findings: List[Dict[str, Any]] = []

    if pid is not None and not _pid_alive(int(pid)):
        findings.append(_finding("CRITICAL", "pid_not_alive", f"live trader PID {pid} is not running"))

    storage_payload = _load_json(storage_state)
    if not isinstance(storage_payload, dict):
        findings.append(_finding("CRITICAL", "storage_state_unreadable", f"cannot read {storage_state}"))
    else:
        last_sweep = ((storage_payload.get("last_sweep") or {}).get("at"))
        age = _age_seconds(last_sweep, now=probe_now)
        if age is None:
            findings.append(_finding("CRITICAL", "last_sweep_missing", "storage state has no last_sweep.at"))
        elif age > max_sweep_age_seconds:
            findings.append(
                _finding(
                    "CRITICAL",
                    "last_sweep_stale",
                    f"last live sweep is stale ({age:.0f}s > {max_sweep_age_seconds}s)",
                    last_sweep_at=last_sweep,
                    age_seconds=round(age, 3),
                )
            )

    for label, path, max_age, keys in (
        ("actionable", actionable_log, max_action_age_seconds, ("action_time", "updated_at")),
        ("trade", trade_log, max_trade_log_age_seconds, ("action_time", "entry_time", "exit_time", "updated_at")),
    ):
        payload = _load_json(path)
        if payload is None:
            findings.append(_finding("WARNING", f"{label}_log_unreadable", f"cannot read {path}"))
            continue
        newest = _newest_timestamp(payload, keys)
        if newest is None:
            findings.append(_finding("WARNING", f"{label}_log_no_timestamp", f"{path} has no recognized timestamp"))
            continue
        age = max(0.0, (probe_now - newest).total_seconds())
        if age > max_age:
            findings.append(
                _finding(
                    "WARNING",
                    f"{label}_log_stale",
                    f"newest {label} log timestamp is stale ({age:.0f}s > {max_age}s)",
                    newest_timestamp=newest.isoformat(),
                    age_seconds=round(age, 3),
                )
            )

    has_critical = any(item["status"] == "CRITICAL" for item in findings)
    has_warning = any(item["status"] == "WARNING" for item in findings)
    status = "CRITICAL" if has_critical else "WARNING" if has_warning else "OK"
    return {
        "status": status,
        "checked_at": probe_now.isoformat(),
        "findings": findings,
    }


def _write_outputs(report: Dict[str, Any], status_path: Optional[Path], events_path: Optional[Path]) -> None:
    if status_path is not None:
        status_path.parent.mkdir(parents=True, exist_ok=True)
        status_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
    if events_path is not None:
        events_path.parent.mkdir(parents=True, exist_ok=True)
        with events_path.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(report, separators=(",", ":")) + "\n")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--storage-state", default="pm_outputs/storage_state.json")
    parser.add_argument("--actionable-log", default="last_actionable_log.json")
    parser.add_argument("--trade-log", default="last_trade_log.json")
    parser.add_argument("--pid", type=int, default=None)
    parser.add_argument("--max-sweep-age-seconds", type=int, default=180)
    parser.add_argument("--max-action-age-seconds", type=int, default=900)
    parser.add_argument("--max-trade-log-age-seconds", type=int, default=900)
    parser.add_argument("--status-out", default="logs/live_silent_watchdog_status.json")
    parser.add_argument("--events-out", default="logs/live_silent_watchdog_events.jsonl")
    parser.add_argument("--loop", action="store_true")
    parser.add_argument("--interval-seconds", type=float, default=30.0)
    args = parser.parse_args()

    while True:
        report = check_live_health(
            storage_state=Path(args.storage_state),
            actionable_log=Path(args.actionable_log),
            trade_log=Path(args.trade_log),
            pid=args.pid,
            max_sweep_age_seconds=args.max_sweep_age_seconds,
            max_action_age_seconds=args.max_action_age_seconds,
            max_trade_log_age_seconds=args.max_trade_log_age_seconds,
        )
        _write_outputs(report, Path(args.status_out) if args.status_out else None, Path(args.events_out) if args.events_out else None)
        print(json.dumps(report, indent=2))
        if not args.loop:
            return 2 if report["status"] == "CRITICAL" else 1 if report["status"] == "WARNING" else 0
        time.sleep(max(1.0, float(args.interval_seconds) if math.isfinite(float(args.interval_seconds)) else 30.0))


if __name__ == "__main__":
    raise SystemExit(main())
