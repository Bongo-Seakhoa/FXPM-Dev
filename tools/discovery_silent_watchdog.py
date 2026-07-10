"""Non-invasive watchdog for long discovery/optimization runs.

The completion monitor waits for process exit and then runs the review stack. This
watchdog is narrower: while a run is active it checks for silent-failure signs such as
lost workers, missing CPU movement, stale logs/artifacts, or a dead monitor process.
It never kills, restarts, or changes the optimizer.
"""

import argparse
import json
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional


REPO = Path(__file__).resolve().parent.parent


def _now_iso() -> str:
    return datetime.now().isoformat()


def _read_json(path: Path) -> Optional[Dict[str, Any]]:
    try:
        return json.loads(path.read_text(encoding="utf-8-sig"))
    except Exception:
        return None


def _write_json(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
    tmp.replace(path)


def _ps_json(command: str) -> Any:
    script = f"{command} | ConvertTo-Json -Compress -Depth 4"
    try:
        out = subprocess.check_output(
            ["powershell", "-NoProfile", "-Command", script],
            cwd=str(REPO),
            text=True,
            stderr=subprocess.DEVNULL,
        ).strip()
    except Exception:
        return None
    if not out:
        return None
    try:
        return json.loads(out)
    except json.JSONDecodeError:
        return None


def _process(pid: int) -> Optional[Dict[str, Any]]:
    data = _ps_json(
        f"Get-Process -Id {int(pid)} -ErrorAction SilentlyContinue "
        "| Select-Object Id,ProcessName,CPU,WorkingSet64,StartTime"
    )
    if isinstance(data, dict):
        return data
    return None


def _children(pid: int) -> List[Dict[str, Any]]:
    data = _ps_json(
        f"Get-CimInstance Win32_Process -Filter \"ParentProcessId={int(pid)}\" "
        "| Select-Object ProcessId,Name,WorkingSetSize"
    )
    if isinstance(data, list):
        return [item for item in data if isinstance(item, dict)]
    if isinstance(data, dict):
        return [data]
    return []


def _mtime(path: Path) -> Optional[float]:
    try:
        return path.stat().st_mtime
    except FileNotFoundError:
        return None


def _age_seconds(mtime: Optional[float]) -> Optional[float]:
    if mtime is None:
        return None
    return max(0.0, time.time() - float(mtime))


def _pipeline_paths(config_path: Path) -> Dict[str, Path]:
    cfg = _read_json(config_path)
    if not isinstance(cfg, dict):
        raise SystemExit(f"Unable to read config: {config_path}")
    pipe = cfg.get("pipeline") or {}
    output_dir = (REPO / str(pipe.get("output_dir", "pm_outputs/production_live"))).resolve()
    ledger = (REPO / str(pipe.get("winner_ledger_path", output_dir / "pm_configs.json"))).resolve()
    evidence = output_dir / str(pipe.get("oos_evidence_dirname", "oos_evidence") or "oos_evidence")
    log_dir = (REPO / str(pipe.get("log_dir", "logs"))).resolve()
    return {"output_dir": output_dir, "ledger": ledger, "evidence_dir": evidence, "log_dir": log_dir}


def _artifact_counts(evidence_dir: Path) -> Dict[str, int]:
    if not evidence_dir.exists():
        return {"oos_manifests": 0, "oos_matrices": 0}
    return {
        "oos_manifests": len(list(evidence_dir.glob("*_oos_manifest.json"))),
        "oos_matrices": len(list(evidence_dir.glob("*_oos_returns_*.npz"))),
    }


def _ledger_count(path: Path) -> int:
    data = _read_json(path)
    if not isinstance(data, dict):
        return 0
    return sum(1 for key, value in data.items() if not str(key).startswith("_") and isinstance(value, dict))


def _latest_mtime(paths: List[Path]) -> Optional[float]:
    values = [_mtime(path) for path in paths]
    values = [value for value in values if value is not None]
    return max(values) if values else None


def _append_event(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(payload, default=str) + "\n")


def _status(args, paths, previous: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    proc = _process(args.pid)
    children = _children(args.pid) if proc else []
    python_children = [child for child in children if str(child.get("Name", "")).lower().startswith("python")]
    python_child_details = []
    for child in python_children:
        try:
            child_pid = int(child.get("ProcessId"))
        except Exception:
            continue
        detail = _process(child_pid) or {}
        detail["ProcessId"] = child_pid
        detail["WorkingSetSize"] = child.get("WorkingSetSize", 0)
        python_child_details.append(detail)
    monitor = _process(args.monitor_pid) if args.monitor_pid else None

    app_log = Path(args.app_log).resolve() if args.app_log else None
    ledger = paths["ledger"]
    evidence_dir = paths["evidence_dir"]
    artifact_counts = _artifact_counts(evidence_dir)
    artifact_paths = [ledger]
    if evidence_dir.exists():
        artifact_paths.extend(evidence_dir.glob("*_oos_manifest.json"))
        artifact_paths.extend(evidence_dir.glob("*_oos_returns_*.npz"))

    cpu = float(proc.get("CPU", 0.0) or 0.0) if proc else None
    worker_cpu = sum(float(child.get("CPU", 0.0) or 0.0) for child in python_child_details)
    total_cpu = (cpu or 0.0) + worker_cpu if proc else None
    prev_main_cpu = previous.get("main_cpu_seconds") if previous else None
    prev_total_cpu = previous.get("total_cpu_seconds") if previous else None
    main_cpu_delta = None
    total_cpu_delta = None
    if cpu is not None and isinstance(prev_main_cpu, (int, float)):
        main_cpu_delta = max(0.0, cpu - float(prev_main_cpu))
    if total_cpu is not None and isinstance(prev_total_cpu, (int, float)):
        total_cpu_delta = max(0.0, total_cpu - float(prev_total_cpu))

    latest_artifact = _latest_mtime(artifact_paths)
    latest_progress = max(
        [value for value in [_mtime(app_log) if app_log else None, latest_artifact] if value is not None],
        default=None,
    )
    log_age = _age_seconds(_mtime(app_log)) if app_log else None
    progress_age = _age_seconds(latest_progress)
    stale_limit = max(60, int(args.stall_minutes) * 60)

    alerts = []
    if not proc:
        alerts.append("optimizer_pid_dead")
    if args.monitor_pid and not monitor:
        alerts.append("completion_monitor_dead")
    if proc and len(python_children) < max(1, int(args.min_workers)):
        alerts.append(f"worker_count_low:{len(python_children)}<{args.min_workers}")
    if (
        proc
        and progress_age is not None
        and progress_age > stale_limit
        and total_cpu_delta is not None
        and total_cpu_delta <= 0.01
    ):
        alerts.append(f"progress_stale_and_cpu_idle:{int(progress_age)}s>{stale_limit}s")
    if proc and total_cpu_delta is not None and total_cpu_delta <= 0.01 and progress_age is not None and progress_age > stale_limit:
        alerts.append("cpu_and_progress_stalled")

    return {
        "label": args.label,
        "updated_at": _now_iso(),
        "optimizer_pid": int(args.pid),
        "optimizer_alive": bool(proc),
        "monitor_pid": int(args.monitor_pid) if args.monitor_pid else None,
        "monitor_alive": bool(monitor) if args.monitor_pid else None,
        "main_cpu_seconds": cpu,
        "worker_cpu_seconds": worker_cpu if proc else None,
        "total_cpu_seconds": total_cpu,
        "main_cpu_delta_since_last_poll": main_cpu_delta,
        "total_cpu_delta_since_last_poll": total_cpu_delta,
        "main_working_set_mb": round(float(proc.get("WorkingSet64", 0) or 0) / 1048576.0, 2) if proc else None,
        "child_process_count": len(children),
        "python_worker_count": len(python_children),
        "python_worker_working_set_mb": [
            round(float(child.get("WorkingSetSize", 0) or 0) / 1048576.0, 2)
            for child in python_children
        ],
        "python_worker_cpu_seconds": [
            round(float(child.get("CPU", 0.0) or 0.0), 3)
            for child in python_child_details
        ],
        "ledger_exists": ledger.exists(),
        "ledger_count": _ledger_count(ledger),
        **artifact_counts,
        "app_log": str(app_log) if app_log else None,
        "app_log_age_seconds": round(log_age, 1) if log_age is not None else None,
        "latest_progress_age_seconds": round(progress_age, 1) if progress_age is not None else None,
        "alerts": alerts,
        "state": "ALERT" if alerts else "OK",
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Watch for silent discovery-run failures.")
    parser.add_argument("--config", required=True)
    parser.add_argument("--pid", type=int, required=True)
    parser.add_argument("--monitor-pid", type=int, default=0)
    parser.add_argument("--label", required=True)
    parser.add_argument("--app-log", default=None)
    parser.add_argument("--poll-seconds", type=int, default=60)
    parser.add_argument("--stall-minutes", type=int, default=45)
    parser.add_argument("--min-workers", type=int, default=1)
    parser.add_argument("--status-path", required=True)
    parser.add_argument("--events-path", required=True)
    args = parser.parse_args()

    paths = _pipeline_paths((REPO / args.config).resolve())
    status_path = (REPO / args.status_path).resolve()
    events_path = (REPO / args.events_path).resolve()
    previous = None

    while True:
        status = _status(args, paths, previous)
        _write_json(status_path, status)
        if status["state"] == "ALERT" or previous is None:
            _append_event(events_path, status)
        previous = status
        if not status["optimizer_alive"]:
            break
        time.sleep(max(15, int(args.poll_seconds)))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
