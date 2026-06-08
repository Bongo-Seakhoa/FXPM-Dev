"""Summarize an FxPM discovery optimization run.

Read-only helper for long honest re-optimization runs. It reports ledger progress,
OOS evidence sidecars, disk headroom, process liveness, and the latest log lines.
"""
import argparse
import csv
import json
import re
import shutil
import subprocess
from pathlib import Path


REPO = Path(__file__).resolve().parent.parent


def _load_json(path):
    try:
        return json.loads(path.read_text(encoding="utf-8-sig"))
    except FileNotFoundError:
        return None
    except Exception as exc:
        return {"_error": str(exc)}


def _pipeline(config):
    return config.get("pipeline", {}) if isinstance(config, dict) else {}


def _ledger_symbols(ledger):
    if not isinstance(ledger, dict):
        return []
    return [key for key, value in ledger.items() if isinstance(value, dict) and not key.startswith("_")]


def _is_validated(entry):
    if not isinstance(entry, dict):
        return False
    if "is_validated" in entry:
        return bool(entry.get("is_validated"))
    # Legacy ledgers use validated winner entries without an explicit flag.
    return bool(entry.get("strategy_name") or entry.get("regime_configs"))


def _is_no_trade_regime(cfg):
    if not isinstance(cfg, dict):
        return True
    val = cfg.get("val_metrics") if isinstance(cfg.get("val_metrics"), dict) else {}
    return (
        str(cfg.get("strategy_name", "") or "") in ("", "NO_TRADE")
        or bool((cfg.get("artifact_meta") or {}).get("no_trade"))
        or str(val.get("validation_status", "")).lower() == "no_trade"
    )


def _regime_surface_counts(ledger):
    if not isinstance(ledger, dict):
        return {"slots": 0, "tradeable": 0, "no_trade": 0}
    slots = tradeable = no_trade = 0
    for symbol, entry in ledger.items():
        if str(symbol).startswith("_") or not isinstance(entry, dict):
            continue
        for regimes in (entry.get("regime_configs") or {}).values():
            if not isinstance(regimes, dict):
                continue
            for cfg in regimes.values():
                if not isinstance(cfg, dict):
                    continue
                slots += 1
                if _is_no_trade_regime(cfg):
                    no_trade += 1
                else:
                    tradeable += 1
    return {"slots": slots, "tradeable": tradeable, "no_trade": no_trade}


def _pid_alive(pid):
    if not pid:
        return None
    try:
        out = subprocess.check_output(
            ["tasklist", "/FI", f"PID eq {int(pid)}", "/FO", "CSV", "/NH"],
            text=True,
            stderr=subprocess.DEVNULL,
        ).strip()
    except Exception:
        return None
    if not out or "No tasks are running" in out:
        return False
    try:
        row = next(csv.reader([out]))
        return row[0].lower().endswith("python.exe") or row[0].lower() == "python.exe"
    except Exception:
        return True


def _python_process_snapshot(target_pid=None):
    """Return a lightweight tasklist snapshot for active Python processes."""
    try:
        out = subprocess.check_output(
            ["tasklist", "/FI", "IMAGENAME eq python.exe", "/FO", "CSV", "/NH"],
            text=True,
            stderr=subprocess.DEVNULL,
        ).strip()
    except Exception:
        return []
    if not out or "No tasks are running" in out:
        return []
    rows = []
    for raw in out.splitlines():
        try:
            row = next(csv.reader([raw]))
        except Exception:
            continue
        if len(row) < 5:
            continue
        try:
            pid = int(row[1])
        except Exception:
            continue
        mem_kb_text = re.sub(r"\D+", "", row[4])
        try:
            mem_mb = int(mem_kb_text) / 1024.0
        except Exception:
            mem_mb = 0.0
        rows.append({
            "pid": pid,
            "session": row[2],
            "mem_mb": mem_mb,
            "target": bool(target_pid and pid == int(target_pid)),
        })
    rows.sort(key=lambda item: item["mem_mb"], reverse=True)
    return rows


def _latest_log(log_dir):
    if not log_dir.exists():
        return None
    logs = sorted(log_dir.glob("*.log"), key=lambda p: p.stat().st_mtime, reverse=True)
    return logs[0] if logs else None


def _active_run_for(config_path: Path, output_dir: Path):
    """Find the active-run marker that matches this config/output directory."""
    candidates = sorted((REPO / "pm_outputs").glob("*_active_run.json"), key=lambda p: p.stat().st_mtime, reverse=True)
    for path in candidates:
        data = _load_json(path)
        if not isinstance(data, dict):
            continue
        raw_config = str(data.get("config", "") or "")
        raw_output = str(data.get("output_dir", "") or "")
        try:
            marker_config = (REPO / raw_config).resolve()
            marker_output = (REPO / raw_output).resolve()
        except Exception:
            continue
        if marker_config == config_path and marker_output == output_dir:
            data["_marker_path"] = str(path)
            return data
    return None


def _tail(path, lines):
    if not path or not path.exists() or lines <= 0:
        return []
    try:
        return path.read_text(encoding="utf-8", errors="replace").splitlines()[-lines:]
    except Exception as exc:
        return [f"<failed to read log: {exc}>"]


def _dir_size(path, pattern="*"):
    if not path.exists():
        return 0, 0
    count = 0
    total = 0
    for item in path.glob(pattern):
        if not item.is_file():
            continue
        try:
            total += int(item.stat().st_size)
            count += 1
        except OSError:
            continue
    return count, total


def main():
    parser = argparse.ArgumentParser(description="Summarize FxPM discovery optimization status.")
    parser.add_argument("--config", default="config.json")
    parser.add_argument("--pid", type=int, default=None)
    parser.add_argument("--tail", type=int, default=20)
    args = parser.parse_args()

    config_path = (REPO / args.config).resolve()
    config = _load_json(config_path)
    if not isinstance(config, dict) or "_error" in config:
        raise SystemExit(f"Unable to load config {config_path}: {config}")

    pipe = _pipeline(config)
    symbols = list(config.get("symbols", []) or [])
    output_dir = (REPO / pipe.get("output_dir", "pm_outputs")).resolve()
    ledger_path = (REPO / pipe.get("winner_ledger_path", "pm_configs.json")).resolve()
    log_dir = (REPO / pipe.get("log_dir", "logs")).resolve()
    evidence_dir = output_dir / str(pipe.get("oos_evidence_dirname", "oos_evidence") or "oos_evidence")
    feature_cache_dir = (REPO / pipe.get("feature_disk_cache_dir", "")).resolve() if pipe.get("feature_disk_cache_dir") else None

    ledger = _load_json(ledger_path)
    ledger_symbols = _ledger_symbols(ledger)
    regime_counts = _regime_surface_counts(ledger)
    validated = [
        symbol for symbol in ledger_symbols
        if isinstance(ledger, dict) and _is_validated(ledger.get(symbol))
    ]

    manifests = list(evidence_dir.glob("*_oos_manifest.json")) if evidence_dir.exists() else []
    matrices = list(evidence_dir.glob("*_oos_returns_*.npz")) if evidence_dir.exists() else []
    feature_cache_count, feature_cache_bytes = _dir_size(feature_cache_dir, "feature_*") if feature_cache_dir else (0, 0)
    disk = shutil.disk_usage(REPO.anchor or REPO)
    active_run = _active_run_for(config_path, output_dir)
    latest_log = None
    active_stdout = (active_run or {}).get("stdout")
    if active_stdout:
        candidate = Path(str(active_stdout))
        latest_log = candidate if candidate.exists() else None
    latest_log = latest_log or _latest_log(log_dir)
    run_pid = args.pid or (active_run or {}).get("optimizer_pid")
    run_label = (active_run or {}).get("label")

    print("FxPM discovery status")
    print("=" * 72)
    print(f"config:        {config_path.relative_to(REPO)}")
    print(f"run pid:       {run_pid or 'n/a'} alive={_pid_alive(run_pid)}")
    if run_label:
        print(f"run label:     {run_label}")
    print(f"symbols:       {len(symbols)} configured")
    print(f"ledger:        {ledger_path.relative_to(REPO)} exists={ledger_path.exists()}")
    print(f"ledger count:  {len(ledger_symbols)} entries, {len(validated)} validated-like")
    print(
        "regime rows:   "
        f"{regime_counts['tradeable']} tradeable / "
        f"{regime_counts['no_trade']} no-trade / "
        f"{regime_counts['slots']} total slots"
    )
    print(f"oos evidence:  {len(manifests)} manifests, {len(matrices)} matrices")
    print(f"output dir:    {output_dir.relative_to(REPO)}")
    print(f"log:           {latest_log.relative_to(REPO) if latest_log else 'n/a'}")
    print(f"disk free:     {disk.free / (1024 ** 3):.2f} GB")
    print(f"workers:       optimize={pipe.get('optimization_max_workers')} precompute={pipe.get('feature_disk_cache_precompute_workers')}")
    print(
        "feature cache: "
        f"enabled={pipe.get('feature_disk_cache_enabled')} "
        f"files={feature_cache_count} size={feature_cache_bytes / (1024 ** 3):.2f} GB "
        f"cap={pipe.get('feature_disk_cache_max_gb')} GB "
        f"min_free={pipe.get('feature_disk_cache_min_free_gb')} GB"
    )
    print(f"spread mode:   {pipe.get('backtest_spread_mode')}")
    print(f"holdout pct:   {pipe.get('clean_holdout_pct')}")
    print(f"oos export:    {pipe.get('oos_evidence_export_enabled')}")
    py_procs = _python_process_snapshot(run_pid)
    if py_procs:
        print("python procs:  " + "; ".join(
            f"{'*' if proc['target'] else ''}{proc['pid']}={proc['mem_mb']:.0f}MB"
            for proc in py_procs[:8]
        ))
    print()
    print(f"Latest {args.tail} log lines")
    print("-" * 72)
    for line in _tail(latest_log, args.tail):
        print(line)


if __name__ == "__main__":
    main()
