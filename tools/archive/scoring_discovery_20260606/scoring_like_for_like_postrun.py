"""Post-run scorer comparison watcher.

Waits for a discovery completion marker, then runs the like-for-like analysis
needed for the scorer decision and posts a concise handoff into AGENT_COMMS.

It is intentionally read-only against optimizer outputs. It does not kill,
restart, tune, or mutate the active optimization run.
"""

import argparse
import json
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List


REPO = Path(__file__).resolve().parent.parent
DEFAULT_SYMBOLS = [
    "EURUSD",
    "GBPUSD",
    "USDJPY",
    "EURAUD",
    "AUDJPY",
    "USDCAD",
    "XAUUSD",
    "XAGUSD",
    "US30",
    "US500",
    "DOGUSD",
]


def _now() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _read_json(path: Path) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8-sig"))
    except Exception:
        return None


def _write_json(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
    tmp.replace(path)


def _run(name: str, cmd: List[str], *, timeout: int) -> Dict[str, Any]:
    started = _now()
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
            "name": name,
            "command": cmd,
            "returncode": int(proc.returncode),
            "started_at": started,
            "completed_at": _now(),
            "output": proc.stdout,
        }
    except subprocess.TimeoutExpired as exc:
        return {
            "name": name,
            "command": cmd,
            "returncode": None,
            "started_at": started,
            "completed_at": _now(),
            "timeout": timeout,
            "output": (exc.stdout or "") + f"\n<TIMEOUT after {timeout}s>",
        }
    except Exception as exc:
        return {
            "name": name,
            "command": cmd,
            "returncode": -1,
            "started_at": started,
            "completed_at": _now(),
            "output": f"{type(exc).__name__}: {exc}",
        }


def _ledger_count(path: Path) -> int:
    data = _read_json(path)
    if not isinstance(data, dict):
        return 0
    return sum(1 for key, value in data.items() if not str(key).startswith("_") and isinstance(value, dict))


def _summary(path: Path) -> Dict[str, Any]:
    data = _read_json(path)
    if not isinstance(data, dict):
        return {"available": False}
    return {
        "available": True,
        "label": data.get("label"),
        "engine_commit": data.get("engine_commit"),
        "spread_mode": data.get("spread_mode"),
        "summary": data.get("summary", {}),
        "class_summary": data.get("class_summary", {}),
    }


def _extract_verdict(compare_output: str) -> str:
    lines = []
    for line in str(compare_output or "").splitlines():
        if "TENET-5 GATE" in line or line.strip().startswith("VERDICT:") or "per-symbol regressions" in line:
            lines.append(line.rstrip())
        elif lines and line.startswith("    "):
            lines.append(line.rstrip())
    return "\n".join(lines).strip()


def _write_report(path: Path,
                  results: Dict[str, Dict[str, Any]],
                  summaries: Dict[str, Dict[str, Any]],
                  candidate_label: str) -> None:
    lines = [
        f"# {candidate_label} Like-For-Like Post-Run Analysis",
        "",
        f"generated_at: {_now()}",
        "",
        "## Benchmark Summaries",
    ]
    for key, summary in summaries.items():
        s = summary.get("summary", {}) if summary.get("available") else {}
        lines.append("")
        lines.append(f"### {key}")
        if not summary.get("available"):
            lines.append("not available")
            continue
        lines.append(f"engine_commit: {summary.get('engine_commit')}")
        lines.append(f"spread_mode: {summary.get('spread_mode')}")
        for metric in (
            "n",
            "mean_return_pct",
            "median_return_pct",
            "mean_profit_factor",
            "median_profit_factor",
            "mean_max_dd_pct",
            "mean_ulcer_index",
            "mean_time_underwater_pct",
            "total_trades",
            "mean_win_rate",
        ):
            lines.append(f"- {metric}: {s.get(metric)}")

    lines.append("")
    lines.append("## Tenet-5 Comparisons")
    for name in ("compare_legacy_vs_candidate", "compare_old_spokes_vs_candidate", "compare_legacy_vs_old_spokes"):
        lines.append("")
        lines.append(f"### {name}")
        result = results.get(name, {})
        lines.append(f"returncode: {result.get('returncode')}")
        excerpt = _extract_verdict(result.get("output", ""))
        lines.append("```")
        lines.append(excerpt or str(result.get("output", "") or "").strip()[:4000])
        lines.append("```")

    lines.append("")
    lines.append("## Command Results")
    for name, result in results.items():
        lines.append("")
        lines.append(f"### {name}")
        lines.append(f"returncode: {result.get('returncode')}")
        output = str(result.get("output", "") or "").strip()
        if len(output) > 6000:
            output = output[:6000] + "\n<output truncated; see JSON artifact for full output>"
        lines.append("```")
        lines.append(output)
        lines.append("```")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _append_comms(report_path: Path,
                  result_json: Path,
                  summaries: Dict[str, Dict[str, Any]],
                  results: Dict[str, Dict[str, Any]],
                  candidate_label: str) -> None:
    candidate = summaries.get("candidate_current", {}).get("summary", {})
    legacy = summaries.get("legacy_current", {}).get("summary", {})
    old = summaries.get("old_spokes_current", {}).get("summary", {})
    compare_legacy = _extract_verdict(results.get("compare_legacy_vs_candidate", {}).get("output", ""))
    compare_old = _extract_verdict(results.get("compare_old_spokes_vs_candidate", {}).get("output", ""))
    entry = [
        "",
        f"## {_now()} - Codex Post-Run Analysis Ready",
        "",
        f"The {candidate_label} run has completed and Codex ran the current-engine like-for-like analysis.",
        "",
        f"- Report: `{report_path.relative_to(REPO)}`",
        f"- JSON: `{result_json.relative_to(REPO)}`",
        f"- Candidate summary: n={candidate.get('n')} median_ret={candidate.get('median_return_pct')} "
        f"mean_ret={candidate.get('mean_return_pct')} median_pf={candidate.get('median_profit_factor')} "
        f"ulcer={candidate.get('mean_ulcer_index')} time_uw={candidate.get('mean_time_underwater_pct')} trades={candidate.get('total_trades')}",
        f"- Legacy current summary: n={legacy.get('n')} median_ret={legacy.get('median_return_pct')} "
        f"mean_ret={legacy.get('mean_return_pct')} median_pf={legacy.get('median_profit_factor')}",
        f"- Old-spoke current summary: n={old.get('n')} median_ret={old.get('median_return_pct')} "
        f"mean_ret={old.get('mean_return_pct')} median_pf={old.get('median_profit_factor')}",
        "",
        "Legacy -> candidate Tenet-5 excerpt:",
        "```",
        compare_legacy or "(not available)",
        "```",
        "",
        "Old-spoke -> candidate Tenet-5 excerpt:",
        "```",
        compare_old or "(not available)",
        "```",
        "",
        "Claude ask: independently audit the report, especially robust median/trimmed interpretation, "
        "per-symbol blowups, no-trade symbols, and whether any apparent candidate improvement is thin-evidence or outlier-driven.",
    ]
    comms = REPO / "AGENT_COMMS.md"
    with comms.open("a", encoding="utf-8") as fh:
        fh.write("\n".join(entry) + "\n")


def main() -> int:
    parser = argparse.ArgumentParser(description="Wait for a candidate run completion and run like-for-like analysis.")
    parser.add_argument("--done-path", required=True)
    parser.add_argument("--candidate-ledger", default=None)
    parser.add_argument("--asym-ledger", default=None, help="Backward-compatible alias for --candidate-ledger")
    parser.add_argument("--legacy-ledger", required=True)
    parser.add_argument("--old-spokes-ledger", required=True)
    parser.add_argument("--out-dir", required=True)
    parser.add_argument("--label", default="specialist_spokes_asym_11")
    parser.add_argument("--poll-seconds", type=int, default=120)
    parser.add_argument("--force", action="store_true")
    args = parser.parse_args()

    done_path = (REPO / args.done_path).resolve()
    candidate_ledger_arg = args.candidate_ledger or args.asym_ledger
    if not candidate_ledger_arg:
        raise SystemExit("--candidate-ledger is required")
    candidate_ledger = (REPO / candidate_ledger_arg).resolve()
    legacy_ledger = (REPO / args.legacy_ledger).resolve()
    old_ledger = (REPO / args.old_spokes_ledger).resolve()
    out_dir = (REPO / args.out_dir).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)
    status_path = out_dir / "postrun_status.json"
    result_json = out_dir / "postrun_results.json"
    report_path = out_dir / "POSTRUN_VERDICT.md"

    if report_path.exists() and result_json.exists() and not args.force:
        return 0

    while not done_path.exists():
        _write_json(status_path, {
            "state": "WAITING_FOR_COMPLETION",
            "updated_at": _now(),
            "done_path": str(done_path),
            "candidate_ledger_exists": candidate_ledger.exists(),
            "candidate_ledger_count": _ledger_count(candidate_ledger),
        })
        time.sleep(max(30, int(args.poll_seconds)))

    while not candidate_ledger.exists() or _ledger_count(candidate_ledger) <= 0:
        _write_json(status_path, {
            "state": "WAITING_FOR_LEDGER",
            "updated_at": _now(),
            "done_path": str(done_path),
            "candidate_ledger_exists": candidate_ledger.exists(),
            "candidate_ledger_count": _ledger_count(candidate_ledger),
        })
        time.sleep(max(30, int(args.poll_seconds)))

    _write_json(status_path, {"state": "RUNNING_ANALYSIS", "updated_at": _now()})

    symbols = DEFAULT_SYMBOLS
    bench = REPO / "tools" / "benchmark.py"
    spoke_val = REPO / "tools" / "spoke_selector_validation.py"
    candidate_evidence = candidate_ledger.parent / "oos_evidence"

    results = {
        "benchmark_legacy_current": _run(
            "benchmark_legacy_current",
            [sys.executable, str(bench), "--ledger", str(legacy_ledger), "--spread-mode", "conservative_max",
             "--label", "legacy_11_current", "--out", str(out_dir), "--symbols", *symbols],
            timeout=7200,
        ),
        "benchmark_old_spokes_current": _run(
            "benchmark_old_spokes_current",
            [sys.executable, str(bench), "--ledger", str(old_ledger), "--spread-mode", "conservative_max",
             "--label", "specialist_spokes_11_current", "--out", str(out_dir), "--symbols", *symbols],
            timeout=7200,
        ),
        "benchmark_candidate_current": _run(
            "benchmark_candidate_current",
            [sys.executable, str(bench), "--ledger", str(candidate_ledger), "--spread-mode", "conservative_max",
             "--label", f"{args.label}_current", "--out", str(out_dir), "--symbols", *symbols],
            timeout=7200,
        ),
    }

    legacy_json = out_dir / "legacy_11_current.json"
    old_json = out_dir / "specialist_spokes_11_current.json"
    candidate_json = out_dir / f"{args.label}_current.json"
    results["compare_legacy_vs_candidate"] = _run(
        "compare_legacy_vs_candidate",
        [sys.executable, str(bench), "--compare", str(legacy_json), str(candidate_json)],
        timeout=900,
    )
    results["compare_old_spokes_vs_candidate"] = _run(
        "compare_old_spokes_vs_candidate",
        [sys.executable, str(bench), "--compare", str(old_json), str(candidate_json)],
        timeout=900,
    )
    results["compare_legacy_vs_old_spokes"] = _run(
        "compare_legacy_vs_old_spokes",
        [sys.executable, str(bench), "--compare", str(legacy_json), str(old_json)],
        timeout=900,
    )
    if candidate_evidence.exists():
        results["spoke_selector_validation_candidate"] = _run(
            "spoke_selector_validation_candidate",
            [sys.executable, str(spoke_val), "--evidence-dir", str(candidate_evidence),
             "--output", str(out_dir / "spoke_selector_validation_candidate.json")],
            timeout=1800,
        )

    summaries = {
        "legacy_current": _summary(legacy_json),
        "old_spokes_current": _summary(old_json),
        "candidate_current": _summary(candidate_json),
    }
    _write_report(report_path, results, summaries, args.label)
    _write_json(result_json, {
        "state": "DONE",
        "completed_at": _now(),
        "done_marker": str(done_path),
        "report_path": str(report_path),
        "summaries": summaries,
        "results": results,
    })
    _write_json(status_path, {
        "state": "DONE",
        "updated_at": _now(),
        "report_path": str(report_path),
        "result_json": str(result_json),
    })
    _append_comms(report_path, result_json, summaries, results, args.label)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
