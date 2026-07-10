"""End-to-end completion monitor for honest discovery runs.

This observer is intentionally non-invasive: it does not stop or tune the optimizer.
It waits for the configured optimizer PID to exit, then runs the read-only review stack
needed for a trustable verdict:

  - discovery_status.py
  - regime_winner_review.py at symbol/timeframe/regime granularity
  - promotion_review.py with stress/PBO/holdout/provenance gates
  - selection_validation.py over the emitted OOS candidate evidence

It writes a compact done marker plus a human-readable verdict report. Email is handled by
the assistant/Gmail connector after the marker appears; this script only creates the body.
"""

import argparse
import csv
import json
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path


REPO = Path(__file__).resolve().parent.parent


def _load_json(path: Path):
    try:
        return json.loads(path.read_text(encoding="utf-8-sig"))
    except FileNotFoundError:
        return None
    except Exception as exc:
        return {"_error": str(exc)}


def _write_json(path: Path, payload) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
    tmp.replace(path)


def _pid_alive(pid: int) -> bool:
    if not pid:
        return False
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


def _run(cmd, *, timeout=None):
    started = datetime.now().isoformat()
    try:
        proc = subprocess.run(
            cmd,
            cwd=str(REPO),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            timeout=timeout,
        )
        return {
            "command": cmd,
            "returncode": int(proc.returncode),
            "started_at": started,
            "completed_at": datetime.now().isoformat(),
            "output": proc.stdout,
        }
    except subprocess.TimeoutExpired as exc:
        return {
            "command": cmd,
            "returncode": None,
            "started_at": started,
            "completed_at": datetime.now().isoformat(),
            "output": (exc.stdout or "") + f"\n<TIMEOUT after {timeout}s>",
            "timeout": timeout,
        }
    except Exception as exc:
        return {
            "command": cmd,
            "returncode": -1,
            "started_at": started,
            "completed_at": datetime.now().isoformat(),
            "output": f"{type(exc).__name__}: {exc}",
        }


def _pipeline_paths(config_path: Path):
    cfg = _load_json(config_path)
    if not isinstance(cfg, dict) or "_error" in cfg:
        raise SystemExit(f"Unable to load config {config_path}: {cfg}")
    pipe = cfg.get("pipeline") or {}
    output_dir = (REPO / pipe.get("output_dir", "pm_outputs/production_live")).resolve()
    ledger = (REPO / pipe.get("winner_ledger_path", "pm_configs.json")).resolve()
    evidence_dir = output_dir / str(pipe.get("oos_evidence_dirname", "oos_evidence") or "oos_evidence")
    return cfg, output_dir, ledger, evidence_dir


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


def _active_run_pid(active_run_path: Path):
    data = _load_json(active_run_path)
    if not isinstance(data, dict):
        return None, None
    return data.get("optimizer_pid"), data.get("label")


def _summarize_promotion(path: Path):
    data = _load_json(path)
    if not isinstance(data, dict):
        return {"available": False}
    counts = data.get("counts") if isinstance(data.get("counts"), dict) else {}
    rows = data.get("rows") if isinstance(data.get("rows"), list) else []
    return {
        "available": True,
        "counts": counts,
        "rows": len(rows),
        "promote": int(counts.get("PROMOTE", 0) or 0),
        "review": int(counts.get("REVIEW", 0) or 0),
        "reject": int(counts.get("REJECT", 0) or 0),
    }


def _summarize_cell_safety(path: Path):
    data = _load_json(path)
    if not isinstance(data, dict):
        return {"available": False}
    counts = data.get("navigation_counts") if isinstance(data.get("navigation_counts"), dict) else {}
    return {
        "available": True,
        "navigation_counts": counts,
        "unsafe_promotes": int(counts.get("unsafe_promotes", 0) or 0),
        "fail_not_reject": int(counts.get("fail_not_reject", 0) or 0),
        "insufficient_not_reject": int(counts.get("insufficient_not_reject", 0) or 0),
        "final_promotes": int(counts.get("final_promotes", 0) or 0),
    }


def _build_report(label, config_path, output_dir, ledger, evidence_dir, results, completion):
    promotion_json = REPO / "pm_outputs" / "experiments" / f"{label}_promotion.json"
    promotion_result = results.get("promotion_review") if isinstance(results.get("promotion_review"), dict) else {}
    promotion_current = promotion_result.get("returncode") == 0
    if promotion_current:
        promotion_summary = _summarize_promotion(promotion_json)
    else:
        promotion_summary = {
            "available": False,
            "error": "promotion_review did not complete successfully in this monitor run",
        }
    safety_json = output_dir / "benchmarks" / f"{label}_promotion_cell_safety.json"
    cell_audit_result = results.get("specialist_cell_audit") if isinstance(results.get("specialist_cell_audit"), dict) else {}
    if cell_audit_result.get("returncode") == 0:
        cell_safety_summary = _summarize_cell_safety(safety_json)
    else:
        cell_safety_summary = {
            "available": False,
            "error": "specialist_cell_audit did not complete successfully in this monitor run",
        }
    ledger_count = _ledger_count(ledger)
    manifests, matrices = _artifact_counts(evidence_dir)

    if ledger_count == 0:
        verdict = "NO_VERDICT_RUN_FAILED_OR_EMPTY"
        reason = "The optimizer exited without a discovery ledger. Treat this as an operational failure, not a trading result."
    elif cell_safety_summary.get("available") and cell_safety_summary.get("unsafe_promotes", 0) > 0:
        verdict = "PROMOTION_CELL_CONTRADICTION_REVIEW_REQUIRED"
        reason = (
            f"{cell_safety_summary['unsafe_promotes']} final PROMOTE cell(s) failed the "
            "specialist-cell audit; inspect each cell before any deployment."
        )
    elif promotion_summary.get("promote", 0) > 0 and not cell_safety_summary.get("available"):
        verdict = "REVIEW_INCOMPLETE"
        reason = (
            f"{promotion_summary['promote']} regime winner(s) cleared promotion gates, "
            "but the specialist-cell safety cross-check is unavailable."
        )
    elif promotion_summary.get("promote", 0) > 0:
        verdict = "CANDIDATES_PROMOTABLE_AFTER_GATES"
        reason = f"{promotion_summary['promote']} regime winner(s) cleared the configured promotion gates."
    elif promotion_summary.get("available"):
        verdict = "NO_PROMOTION_READY_WINNERS"
        reason = "The run produced winners, but none cleared the full promotion gate stack."
    else:
        verdict = "REVIEW_INCOMPLETE"
        reason = "The run produced artifacts, but the promotion review did not complete cleanly."

    lines = [
        f"# FxPM Discovery Completion Report - {label}",
        "",
        f"completed_at: {completion['completed_at']}",
        f"config: {config_path}",
        f"output_dir: {output_dir}",
        f"ledger: {ledger}",
        f"evidence_dir: {evidence_dir}",
        "",
        f"VERDICT: {verdict}",
        f"WHY: {reason}",
        "",
        f"ledger_symbols: {ledger_count}",
        f"oos_manifests: {manifests}",
        f"oos_matrices: {matrices}",
        f"promotion_summary: {promotion_summary}",
        f"promotion_cell_safety: {cell_safety_summary}",
        "",
        "## Review Outputs",
    ]
    for name, result in results.items():
        lines.append("")
        lines.append(f"### {name}")
        lines.append(f"returncode: {result.get('returncode')}")
        output = str(result.get("output", "") or "").strip()
        if len(output) > 12000:
            output = output[:12000] + "\n<output truncated in report; see JSON result for full captured output>"
        lines.append("```")
        lines.append(output)
        lines.append("```")
    return verdict, "\n".join(lines) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser(description="Monitor a discovery run to completion and run final reviews.")
    parser.add_argument("--config", default="config.json")
    parser.add_argument("--pid", type=int)
    parser.add_argument("--active-run", default=None)
    parser.add_argument("--label", required=True)
    parser.add_argument("--poll-seconds", type=int, default=300)
    parser.add_argument("--status-path", default=None)
    parser.add_argument("--done-path", default=None)
    parser.add_argument("--run-now-if-dead", action="store_true")
    args = parser.parse_args()

    config_path = (REPO / args.config).resolve()
    _cfg, output_dir, ledger, evidence_dir = _pipeline_paths(config_path)
    active_path = Path(args.active_run).resolve() if args.active_run else None
    pid = args.pid
    run_label = None
    if not pid and active_path:
        pid, run_label = _active_run_pid(active_path)

    watch_dir = REPO / "logs" / "discovery_watch"
    status_path = Path(args.status_path).resolve() if args.status_path else watch_dir / f"{args.label}_completion_status.json"
    done_path = Path(args.done_path).resolve() if args.done_path else watch_dir / f"{args.label}_completion_done.json"
    review_ready_path = watch_dir / f"{args.label}_review_ready.json"
    report_path = output_dir / f"{args.label}_completion_report.md"
    result_json_path = output_dir / f"{args.label}_completion_results.json"

    while pid and _pid_alive(int(pid)):
        manifests, matrices = _artifact_counts(evidence_dir)
        payload = {
            "label": args.label,
            "run_label": run_label,
            "pid": int(pid),
            "pid_alive": True,
            "ledger_exists": ledger.exists(),
            "ledger_count": _ledger_count(ledger),
            "oos_manifests": manifests,
            "oos_matrices": matrices,
            "updated_at": datetime.now().isoformat(),
            "report_path": str(report_path),
        }
        _write_json(status_path, payload)
        time.sleep(max(30, int(args.poll_seconds)))

    if pid and not args.run_now_if_dead:
        # Normal path: process has exited after monitoring.
        pass

    results = {
        "discovery_status": _run([
            sys.executable, str(REPO / "tools" / "discovery_status.py"),
            "--config", str(config_path.relative_to(REPO)), "--tail", "160",
        ], timeout=180),
    }
    if ledger.exists() and _ledger_count(ledger) > 0:
        _write_json(
            review_ready_path,
            {
                "label": args.label,
                "run_label": run_label,
                "review_ready": True,
                "pid": pid,
                "pid_alive": bool(pid and _pid_alive(int(pid))),
                "ledger": str(ledger),
                "ledger_count": _ledger_count(ledger),
                "oos_manifests": _artifact_counts(evidence_dir)[0],
                "oos_matrices": _artifact_counts(evidence_dir)[1],
                "evidence_dir": str(evidence_dir),
                "created_at": datetime.now().isoformat(),
            },
        )
        results["regime_winner_review"] = _run([
            sys.executable, str(REPO / "tools" / "regime_winner_review.py"),
            "--ledger", str(ledger),
            "--evidence-dir", str(evidence_dir),
            "--include-no-trade",
            "--label", f"{args.label}_regime_surface",
        ], timeout=900)
        results["promotion_review"] = _run([
            sys.executable, str(REPO / "tools" / "promotion_review.py"),
            "--ledger", str(ledger),
            "--evidence-dir", str(evidence_dir),
            "--label", f"{args.label}_promotion",
            "--completion-marker", str(review_ready_path),
            "--require-completion-marker",
        ], timeout=7200)
        cell_json = output_dir / "benchmarks" / f"{args.label}_specialist_cells.json"
        safety_json = output_dir / "benchmarks" / f"{args.label}_promotion_cell_safety.json"
        promotion_json = REPO / "pm_outputs" / "experiments" / f"{args.label}_promotion.json"
        cell_cmd = [
            sys.executable, str(REPO / "tools" / "benchmark_specialist_cells.py"),
            "--ledger", str(ledger),
            "--label", args.label,
            "--out", str(cell_json),
        ]
        if results["promotion_review"].get("returncode") == 0 and promotion_json.exists():
            cell_cmd.extend([
                "--promotion-review", str(promotion_json),
                "--promotion-safety-out", str(safety_json),
            ])
        results["specialist_cell_audit"] = _run(cell_cmd, timeout=900)
    else:
        results["regime_winner_review"] = {
            "returncode": None,
            "output": "Skipped: ledger missing or empty.",
        }
        results["promotion_review"] = {
            "returncode": None,
            "output": "Skipped: ledger missing or empty.",
        }
        results["specialist_cell_audit"] = {
            "returncode": None,
            "output": "Skipped: ledger missing or empty.",
        }

    manifests, _matrices = _artifact_counts(evidence_dir)
    if manifests > 0:
        results["selection_validation"] = _run([
            sys.executable, str(REPO / "tools" / "selection_validation.py"),
            "--evidence-dir", str(evidence_dir),
            "--min-survivors", "4",
        ], timeout=1800)
    else:
        results["selection_validation"] = {
            "returncode": None,
            "output": "Skipped: no OOS manifests emitted.",
        }

    completion = {
        "label": args.label,
        "run_label": run_label,
        "pid": pid,
        "pid_alive": bool(pid and _pid_alive(int(pid))),
        "completed_at": datetime.now().isoformat(),
    }
    verdict, report = _build_report(args.label, config_path, output_dir, ledger, evidence_dir, results, completion)
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(report, encoding="utf-8")
    result_json_path.write_text(json.dumps({
        "completion": completion,
        "verdict": verdict,
        "report_path": str(report_path),
        "results": results,
    }, indent=2, default=str), encoding="utf-8")

    done = {
        **completion,
        "verdict": verdict,
        "report_path": str(report_path),
        "result_json_path": str(result_json_path),
        "review_ready_path": str(review_ready_path),
        "ledger_count": _ledger_count(ledger),
        "oos_manifests": _artifact_counts(evidence_dir)[0],
        "oos_matrices": _artifact_counts(evidence_dir)[1],
        "email_to": "bongokosa@gmail.com",
        "email_pending": True,
    }
    _write_json(status_path, done)
    _write_json(done_path, done)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
