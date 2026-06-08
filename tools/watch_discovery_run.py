"""Watch a long FxPM discovery run and run post-run gates.

This helper is intentionally lightweight. It polls process liveness and artifact
counts, writes a small status marker, and only runs the heavier promotion review
after the optimizer process exits.
"""

import argparse
import csv
import ctypes
import json
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path


REPO = Path(__file__).resolve().parent.parent


def _load_json(path: Path):
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return None
    except Exception as exc:
        return {"_error": str(exc)}


def _pid_alive(pid: int) -> bool:
    try:
        out = subprocess.check_output(
            ["tasklist", "/FI", f"PID eq {int(pid)}", "/FO", "CSV", "/NH"],
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


def _ledger_count(path: Path) -> int:
    data = _load_json(path)
    if not isinstance(data, dict):
        return 0
    return sum(1 for key, value in data.items() if not str(key).startswith("_") and isinstance(value, dict))


def _artifact_counts(evidence_dir: Path):
    if not evidence_dir.exists():
        return 0, 0
    return (
        len(list(evidence_dir.glob("*_oos_manifest.json"))),
        len(list(evidence_dir.glob("*_oos_returns_*.npz"))),
    )


def _memory_status_mb():
    """Return free/total physical and pagefile-backed virtual memory on Windows."""
    class MEMORYSTATUSEX(ctypes.Structure):
        _fields_ = [
            ("dwLength", ctypes.c_ulong),
            ("dwMemoryLoad", ctypes.c_ulong),
            ("ullTotalPhys", ctypes.c_ulonglong),
            ("ullAvailPhys", ctypes.c_ulonglong),
            ("ullTotalPageFile", ctypes.c_ulonglong),
            ("ullAvailPageFile", ctypes.c_ulonglong),
            ("ullTotalVirtual", ctypes.c_ulonglong),
            ("ullAvailVirtual", ctypes.c_ulonglong),
            ("ullAvailExtendedVirtual", ctypes.c_ulonglong),
        ]

    status = MEMORYSTATUSEX()
    status.dwLength = ctypes.sizeof(MEMORYSTATUSEX)
    if not ctypes.windll.kernel32.GlobalMemoryStatusEx(ctypes.byref(status)):
        return {}
    scale = 1024.0 * 1024.0
    return {
        "memory_load_pct": int(status.dwMemoryLoad),
        "free_physical_mb": round(status.ullAvailPhys / scale, 1),
        "total_physical_mb": round(status.ullTotalPhys / scale, 1),
        "free_pagefile_mb": round(status.ullAvailPageFile / scale, 1),
        "total_pagefile_mb": round(status.ullTotalPageFile / scale, 1),
        "free_virtual_mb": round(status.ullAvailVirtual / scale, 1),
    }


def _memory_pressure(memory_status, min_free_physical_mb, min_free_virtual_mb):
    reasons = []
    if min_free_physical_mb and memory_status:
        free_phys = float(memory_status.get("free_physical_mb", 0.0) or 0.0)
        if free_phys < float(min_free_physical_mb):
            reasons.append(f"free_physical_mb {free_phys:.1f} < {float(min_free_physical_mb):.1f}")
    if min_free_virtual_mb and memory_status:
        free_virtual = float(memory_status.get("free_pagefile_mb", 0.0) or 0.0)
        if free_virtual < float(min_free_virtual_mb):
            reasons.append(f"free_pagefile_mb {free_virtual:.1f} < {float(min_free_virtual_mb):.1f}")
    return reasons


def _stop_process_tree(pid: int) -> None:
    subprocess.run(
        ["taskkill", "/PID", str(int(pid)), "/T", "/F"],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        text=True,
    )


def _write_json(path: Path, payload) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
    tmp.replace(path)


def _run_promotion_review(ledger: Path, evidence_dir: Path, label: str, out_path: Path, marker_path: Path) -> int:
    cmd = [
        sys.executable,
        str(REPO / "tools" / "promotion_review.py"),
        "--ledger",
        str(ledger),
        "--evidence-dir",
        str(evidence_dir),
        "--label",
        label,
        "--completion-marker",
        str(marker_path),
        "--require-completion-marker",
    ]
    with out_path.open("w", encoding="utf-8", errors="replace") as out:
        proc = subprocess.run(
            cmd,
            cwd=str(REPO),
            stdout=out,
            stderr=subprocess.STDOUT,
            text=True,
        )
    return int(proc.returncode)


def main() -> int:
    parser = argparse.ArgumentParser(description="Watch FxPM discovery optimizer and post-run gates.")
    parser.add_argument("--pid", type=int, required=True)
    parser.add_argument("--config", default="config.json")
    parser.add_argument("--label", required=True)
    parser.add_argument("--poll-seconds", type=int, default=120)
    parser.add_argument("--status-path", default=None)
    parser.add_argument("--marker-path", default=None)
    parser.add_argument("--min-free-physical-mb", type=float, default=0.0)
    parser.add_argument("--min-free-virtual-mb", type=float, default=0.0)
    parser.add_argument("--stop-on-memory-pressure", action="store_true")
    args = parser.parse_args()

    config_path = (REPO / args.config).resolve()
    config = _load_json(config_path)
    if not isinstance(config, dict) or "_error" in config:
        raise SystemExit(f"Unable to load config: {config_path}")

    pipeline = dict(config.get("pipeline") or {})
    output_dir = (REPO / pipeline.get("output_dir", "pm_outputs/production_live")).resolve()
    ledger = (REPO / pipeline.get("winner_ledger_path", "pm_configs.json")).resolve()
    evidence_dir = output_dir / str(pipeline.get("oos_evidence_dirname", "oos_evidence") or "oos_evidence")
    watch_dir = REPO / "logs" / "discovery_watch"
    status_path = Path(args.status_path).resolve() if args.status_path else watch_dir / f"{args.label}_status.json"
    marker_path = Path(args.marker_path).resolve() if args.marker_path else watch_dir / f"{args.label}_done.json"
    review_ready_path = watch_dir / f"{args.label}_review_ready.json"
    review_out = output_dir / f"promotion_review_{args.label}.txt"
    pressure_reasons = []
    stopped_for_pressure = False

    while _pid_alive(args.pid):
        manifests, matrices = _artifact_counts(evidence_dir)
        memory_status = _memory_status_mb()
        pressure_reasons = _memory_pressure(
            memory_status,
            args.min_free_physical_mb,
            args.min_free_virtual_mb,
        )
        payload = {
            "label": args.label,
            "pid": args.pid,
            "pid_alive": True,
            "ledger_exists": ledger.exists(),
            "ledger_count": _ledger_count(ledger),
            "oos_manifests": manifests,
            "oos_matrices": matrices,
            "memory": memory_status,
            "memory_pressure": pressure_reasons,
            "updated_at": datetime.now().isoformat(),
        }
        _write_json(status_path, payload)
        if pressure_reasons and args.stop_on_memory_pressure:
            stopped_for_pressure = True
            _stop_process_tree(args.pid)
            break
        time.sleep(max(10, int(args.poll_seconds)))

    manifests, matrices = _artifact_counts(evidence_dir)
    ledger_exists = ledger.exists()
    ledger_count = _ledger_count(ledger)
    review_rc = None
    if ledger_exists and ledger_count > 0:
        _write_json(
            review_ready_path,
            {
                "label": args.label,
                "review_ready": True,
                "pid": args.pid,
                "pid_alive": False,
                "stopped_for_memory_pressure": stopped_for_pressure,
                "ledger": str(ledger),
                "ledger_count": ledger_count,
                "oos_manifests": manifests,
                "oos_matrices": matrices,
                "evidence_dir": str(evidence_dir),
                "created_at": datetime.now().isoformat(),
            },
        )
        review_rc = _run_promotion_review(ledger, evidence_dir, args.label, review_out, review_ready_path)

    marker = {
        "label": args.label,
        "pid": args.pid,
        "pid_alive": False,
        "ledger_exists": ledger_exists,
        "ledger_count": ledger_count,
        "oos_manifests": manifests,
        "oos_matrices": matrices,
        "memory": _memory_status_mb(),
        "stopped_for_memory_pressure": stopped_for_pressure,
        "memory_pressure": pressure_reasons,
        "promotion_review_path": str(review_out) if review_rc is not None else None,
        "promotion_review_returncode": review_rc,
        "review_ready_path": str(review_ready_path) if review_rc is not None else None,
        "completed_at": datetime.now().isoformat(),
    }
    _write_json(status_path, marker)
    _write_json(marker_path, marker)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
