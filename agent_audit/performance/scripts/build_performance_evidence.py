"""Build read-only performance audit evidence artifacts.

The script imports no trading-bot modules and never calls MT5. It fingerprints
existing artifacts, parses source text for strategy implementation anchors, and
writes derived evidence only under agent_audit/performance.
"""

from __future__ import annotations

import ast
import csv
import hashlib
import json
import math
import re
import subprocess
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[3]
OUT = ROOT / "agent_audit" / "performance"
ANATOMY = OUT / "strategy_anatomy"


CRITICAL_FILES = [
    "prompt.md",
    "config.json",
    "pm_configs.json",
    "regime_params.json",
    "pm_core.py",
    "pm_metrics.py",
    "pm_pipeline.py",
    "pm_optuna.py",
    "pm_main.py",
    "pm_position.py",
    "pm_mt5.py",
    "pm_live_readiness.py",
    "pm_dashboard/analytics.py",
    "tools/benchmark.py",
    "agent_audit/performance/scripts/run_promoted_cell_replay.py",
    "agent_audit/performance/scripts/shadow_selection_audit.py",
    "agent_audit/performance/scripts/claude_replay_all_promoted.py",
    "pm_strategies.py",
    "tests/test_metric_contract.py",
    "tests/test_backtester.py",
    "tests/test_dashboard_trade_enrichment.py",
    "tests/test_config_source_of_truth.py",
    "tests/test_profitability_readiness_blueprint.py",
    "agent_audit/reports/TECHNICAL_AUDIT.md",
    "agent_audit/reports/STRATEGIC_AUDIT.md",
    "agent_audit/data/ledger_tradeable_cells.csv",
    "agent_audit/data/ledger_symbol_summary.csv",
    "agent_audit/data/oos_manifest_summary.csv",
    "agent_audit/data/deep_split_geometry.csv",
    "agent_audit/data/deep_metric_definition_mismatches.csv",
    "agent_audit/performance/FINAL_PERFORMANCE_REPORT.md",
    "agent_audit/performance/BASELINE_10000.md",
    "agent_audit/performance/RECOMMENDATION_TRIAGE.md",
    "agent_audit/performance/OPTUNA_NUMBA_COMPATIBILITY.md",
    "agent_audit/performance/FIX_REGISTER.md",
    "agent_audit/performance/PERFORMANCE_COMPARISONS.csv",
    "agent_audit/performance/COMMUNICATIONS_CHECK.md",
    "agent_audit/performance/PARTNER_REVIEW.md",
    "agent_audit/performance/ACCEPTED_FIXES.md",
    "agent_audit/performance/OPEN_QUESTIONS.md",
    "agent_audit/performance/CLAUDE_DECISIVE_REPLAY_FINDING.md",
    "agent_audit/performance/data/current_ledger_inventory.json",
    "agent_audit/performance/data/current_promoted_cells.csv",
    "agent_audit/performance/data/promoted_cell_replay_10000.csv",
    "agent_audit/performance/data/promoted_winner_replay_all_10000.csv",
    "agent_audit/performance/data/shadow_selection_audit.csv",
    "agent_audit/performance/data/shadow_selection_audit.summary.json",
]


PERFORMANCE_COMPARISON_COLUMNS = [
    "fix_id",
    "baseline_or_candidate",
    "commit_fingerprint",
    "config_fingerprint",
    "symbol",
    "timeframe",
    "strategy",
    "data_fingerprint",
    "starting_balance",
    "trial_budget",
    "seed",
    "numba_status",
    "net_profit",
    "return_pct",
    "drawdown_pct",
    "recovery_factor",
    "profit_factor",
    "expectancy",
    "trade_count",
    "costs",
    "mae_mfe_summaries",
    "runtime",
    "memory",
    "verdict",
    "notes",
]


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def load_json(path: Path) -> Any:
    with path.open("r", encoding="utf-8") as fh:
        return json.load(fh)


def read_csv(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8-sig", newline="") as fh:
        return list(csv.DictReader(fh))


def as_float(value: Any) -> float | None:
    try:
        parsed = float(value)
    except Exception:
        return None
    if not math.isfinite(parsed):
        return None
    return parsed


def avg(values: list[float]) -> float | None:
    values = [v for v in values if math.isfinite(v)]
    return (sum(values) / len(values)) if values else None


def sanitize_filename(name: str) -> str:
    clean = re.sub(r"[^A-Za-z0-9_.-]+", "_", name.strip())
    return clean[:120] or "UNKNOWN"


def write_csv(path: Path, rows: list[dict[str, Any]], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({key: row.get(key, "") for key in fieldnames})


def git_value(*args: str) -> str | None:
    try:
        result = subprocess.run(
            ["git", *args],
            cwd=ROOT,
            check=True,
            capture_output=True,
            text=True,
            timeout=20,
        )
    except Exception:
        return None
    return result.stdout.strip()


def rel(path: Path) -> str:
    return str(path.relative_to(ROOT)).replace("\\", "/")


def build_manifest() -> dict[str, Any]:
    files: list[dict[str, Any]] = []
    for rel_path in CRITICAL_FILES:
        path = ROOT / rel_path
        record: dict[str, Any] = {"path": rel_path, "exists": path.exists()}
        if path.exists() and path.is_file():
            stat = path.stat()
            record.update({
                "size_bytes": stat.st_size,
                "modified": datetime.fromtimestamp(stat.st_mtime).isoformat(),
                "sha256": sha256_file(path),
            })
        files.append(record)
    return {
        "created_at": datetime.now().isoformat(),
        "git_head": git_value("rev-parse", "HEAD"),
        "git_status_short": git_value("status", "--short"),
        "files": files,
    }


def _config_section(cfg: dict[str, Any], name: str) -> dict[str, Any]:
    section = cfg.get(name)
    return section if isinstance(section, dict) else {}


def build_baseline() -> dict[str, Any]:
    cfg_path = ROOT / "config.json"
    pm_cfg_path = ROOT / "pm_configs.json"
    regime_path = ROOT / "regime_params.json"
    ledger_path = ROOT / "agent_audit" / "data" / "ledger_tradeable_cells.csv"
    current_inventory_path = ROOT / "agent_audit" / "performance" / "data" / "current_ledger_inventory.json"
    current_inventory = load_json(current_inventory_path) if current_inventory_path.exists() else {}
    cfg = load_json(cfg_path) if cfg_path.exists() else {}
    pm_cfg = load_json(pm_cfg_path) if pm_cfg_path.exists() else {}
    pipeline = _config_section(cfg, "pipeline")
    position = _config_section(cfg, "position")
    mt5 = _config_section(cfg, "mt5")
    return {
        "created_at": datetime.now().isoformat(),
        "canonical_starting_balance_usd": 10000.0,
        "balance_sweep_performed": False,
        "full_optimizer_run_performed": False,
        "live_trade_performed": False,
        "git_head": git_value("rev-parse", "HEAD"),
        "working_tree_status": git_value("status", "--short"),
        "config_sha256": sha256_file(cfg_path) if cfg_path.exists() else None,
        "pm_configs_sha256": sha256_file(pm_cfg_path) if pm_cfg_path.exists() else None,
        "regime_params_sha256": sha256_file(regime_path) if regime_path.exists() else None,
        "historical_ledger_tradeable_cells_sha256": sha256_file(ledger_path) if ledger_path.exists() else None,
        "current_ledger_inventory_sha256": sha256_file(current_inventory_path) if current_inventory_path.exists() else None,
        "current_logical_promoted_cell_count": current_inventory.get("logical_promoted_cell_count"),
        "config_initial_capital": cfg.get("initial_capital") or pipeline.get("initial_capital"),
        "config_starting_balance": cfg.get("starting_balance"),
        "risk_settings": {
            "position_risk_per_trade_pct": position.get("risk_per_trade_pct"),
            "position_max_risk_pct": position.get("max_risk_pct"),
            "pipeline_discovery_risk_per_trade_pct": pipeline.get("discovery_risk_per_trade_pct"),
            "pipeline_max_combined_risk_pct": pipeline.get("max_combined_risk_pct"),
        },
        "cost_settings": {
            "use_spread": pipeline.get("use_spread"),
            "use_slippage": pipeline.get("use_slippage"),
            "use_commission": pipeline.get("use_commission"),
            "backtest_spread_mode": pipeline.get("backtest_spread_mode"),
        },
        "mt5_identity": {
            "account_id": mt5.get("account_id"),
            "server": mt5.get("server"),
        },
        "pm_configs_type": type(pm_cfg).__name__,
        "pm_configs_top_level_count": len(pm_cfg) if isinstance(pm_cfg, dict) else None,
        "optuna_settings": {
            "study_name": pipeline.get("optuna_study_name"),
            "sampler": pipeline.get("optuna_sampler"),
            "pruner": pipeline.get("optuna_pruner"),
            "seed": pipeline.get("random_seed"),
            "trial_budget": pipeline.get("optuna_trials"),
        },
        "numba_status": "available in tests when pm_core.NUMBA_AVAILABLE is true; parity test executed conditionally",
        "source_note": (
            "USD 10,000 is the canonical comparison baseline. This script fingerprints "
            "current artifacts and does not execute strategy optimization. Historical "
            "ledger_tradeable_cells evidence is not the current live-book count after "
            "the 2026-06-18 pm_configs.json rewrite."
        ),
    }


def build_strategy_code_index() -> dict[str, dict[str, Any]]:
    path = ROOT / "pm_strategies.py"
    text = path.read_text(encoding="utf-8")
    lines = text.splitlines()
    tree = ast.parse(text)
    index: dict[str, dict[str, Any]] = {}
    for node in tree.body:
        if not isinstance(node, ast.ClassDef):
            continue
        if not node.name.endswith("Strategy"):
            continue
        methods: dict[str, dict[str, Any]] = {}
        default_params: dict[str, Any] | str = "not statically literal"
        for child in node.body:
            if isinstance(child, ast.FunctionDef):
                method_lines = lines[child.lineno - 1: child.end_lineno]
                methods[child.name] = {
                    "line": child.lineno,
                    "end_line": child.end_lineno,
                    "snippet": "\n".join(method_lines[:45]),
                }
                if child.name == "get_default_params":
                    for stmt in ast.walk(child):
                        if isinstance(stmt, ast.Return):
                            try:
                                default_params = ast.literal_eval(stmt.value)
                            except Exception:
                                default_params = "not statically literal"
                            break
        entry_lines: list[str] = []
        gen = methods.get("generate_signals", {}).get("snippet", "")
        for raw in gen.splitlines():
            stripped = raw.strip()
            if (
                "signals" in stripped
                or "long" in stripped.lower()
                or "short" in stripped.lower()
                or "condition" in stripped.lower()
            ):
                entry_lines.append(stripped)
        index[node.name] = {
            "file": "pm_strategies.py",
            "line": node.lineno,
            "end_line": node.end_lineno,
            "methods": methods,
            "default_params": default_params,
            "entry_rule_lines": entry_lines[:18],
        }
    return index


def intended_edge(strategy: str) -> str:
    name = strategy.lower()
    if "dayofweek" in name:
        return "Calendar/weekday tendency: exploit recurring day-of-week directional bias after regime and symbol selection."
    if "timeofday" in name or "session" in name or "opening" in name:
        return "Session timing edge: exploit recurring intraday liquidity, momentum, or reversion windows."
    if "ema" in name or "trend" in name or "continuation" in name or "pullback" in name:
        return "Trend-continuation edge: enter pullbacks or confirmations in an established directional move."
    if "reversion" in name or "fade" in name or "bounce" in name or "extremes" in name:
        return "Mean-reversion edge: buy stretched weakness or sell stretched strength when reversal conditions are present."
    if "breakout" in name or "burst" in name or "expansion" in name:
        return "Breakout/momentum edge: enter when compression or level breaks signal directional expansion."
    if "darvas" in name or "range" in name or "channel" in name:
        return "Structure edge: trade break/rejection of recent boxes, ranges, or channels."
    return "Strategy-specific technical edge selected by the promoted ledger; exact signal logic is in `pm_strategies.py`."


def fix_points_for_strategy(strategy: str) -> list[str]:
    points = [
        "Run a USD 10,000 promoted-cell replay and bucket by `mae_r`, `mfe_r`, final R, and exit reason.",
        "Stress entry slippage by symbol/timeframe/session before changing trade eligibility.",
        "Do not add a gate until the new measurement fields prove it removes unpaid drawdown rather than profitable pain.",
    ]
    name = strategy.lower()
    if "emapullbackcontinuation" in name:
        points.insert(0, "Test wider `tp_atr_mult` and optional trailing only after measuring MFE retention; Claude flagged 1:1 behavior as a likely capped-winner issue.")
    if "dayofweekbias" in name:
        points.insert(0, "Run post-selection replay for weekday/mode choices; Claude flagged validation-surface noise as the main risk for this family.")
    if "timeofday" in name or "session" in name or "opening" in name:
        points.insert(0, "Break out results by broker session and spread regime; this family is especially cost-window sensitive.")
    if "zscore" in name or "vwap" in name or "reversion" in name or "fade" in name:
        points.insert(0, "Measure immediate MAE after entry; late mean-reversion entries can look profitable while consuming avoidable adverse excursion.")
    return points


def top_table(sorted_items: list[dict[str, str]], limit: int = 12) -> list[str]:
    md = [
        "| Symbol | TF | Regime | Status | Selection | Quality | Val PF | Val Return % | Val DD % | Val Trades |",
        "|---|---|---|---|---:|---:|---:|---:|---:|---:|",
    ]
    for row in sorted_items[:limit]:
        md.append(
            "| {symbol} | {timeframe} | {regime} | {promotion_status} | {selection_score} | "
            "{quality_score} | {val_profit_factor} | {val_return_pct} | {val_drawdown_pct} | {val_trades} |".format(
                **{k: row.get(k, "") for k in [
                    "symbol", "timeframe", "regime", "promotion_status", "selection_score",
                    "quality_score", "val_profit_factor", "val_return_pct",
                    "val_drawdown_pct", "val_trades",
                ]}
            )
        )
    return md


def build_strategy_anatomy() -> dict[str, Any]:
    rows = read_csv(ROOT / "agent_audit" / "data" / "ledger_tradeable_cells.csv")
    code_index = build_strategy_code_index()
    grouped: dict[str, list[dict[str, str]]] = defaultdict(list)
    for row in rows:
        grouped[row.get("strategy_name") or "UNKNOWN"].append(row)

    summary_rows: list[dict[str, Any]] = []
    for strategy, items in sorted(grouped.items(), key=lambda kv: (-len(kv[1]), kv[0])):
        symbols = sorted({r.get("symbol", "") for r in items if r.get("symbol")})
        timeframes = sorted({r.get("timeframe", "") for r in items if r.get("timeframe")})
        regimes = sorted({r.get("regime", "") for r in items if r.get("regime")})
        statuses = Counter(r.get("promotion_status", "") for r in items)
        val_pf = [v for v in (as_float(r.get("val_profit_factor")) for r in items) if v is not None]
        val_ret = [v for v in (as_float(r.get("val_return_pct")) for r in items) if v is not None]
        val_dd = [v for v in (as_float(r.get("val_drawdown_pct")) for r in items) if v is not None]
        selection = [v for v in (as_float(r.get("selection_score")) for r in items) if v is not None]
        quality = [v for v in (as_float(r.get("quality_score")) for r in items) if v is not None]
        sorted_items = sorted(
            items,
            key=lambda r: (
                as_float(r.get("selection_score")) or float("-inf"),
                as_float(r.get("val_profit_factor")) or float("-inf"),
            ),
            reverse=True,
        )
        info = code_index.get(strategy, {})
        methods = info.get("methods", {})
        method_summary = ", ".join(
            f"`{name}`:{data.get('line')}" for name, data in sorted(methods.items())
            if name in {"get_default_params", "generate_signals", "calculate_stops", "build_trade_intent"}
        ) or "class not found in `pm_strategies.py`"
        params = info.get("default_params", "not available")
        params_text = json.dumps(params, sort_keys=True) if isinstance(params, dict) else str(params)
        entry_lines = info.get("entry_rule_lines") or ["No static entry assignment lines extracted; inspect `generate_signals` manually."]
        top_symbols = ";".join(symbols)
        top_timeframes = ";".join(timeframes)
        top_regimes = ";".join(regimes)

        summary_rows.append({
            "strategy_name": strategy,
            "cell_count": len(items),
            "symbols": top_symbols,
            "timeframes": top_timeframes,
            "regimes": top_regimes,
            "status_counts": json.dumps(dict(statuses), sort_keys=True),
            "avg_val_profit_factor": avg(val_pf),
            "avg_val_return_pct": avg(val_ret),
            "avg_val_drawdown_pct": avg(val_dd),
            "avg_selection_score": avg(selection),
            "avg_quality_score": avg(quality),
            "source_line": info.get("line", ""),
        })

        md = [
            f"# Strategy Profit Anatomy: {strategy}",
            "",
            "## Intended edge",
            intended_edge(strategy),
            "",
            "## Exact implementation",
            f"- Files: `pm_strategies.py` (class line {info.get('line', 'unknown')}), `pm_core.py` backtest engine, `pm_pipeline.py` selection/export, `pm_main.py` live execution.",
            f"- Classes: `{strategy}` plus `BaseStrategy` for shared stop/intent behavior.",
            f"- Functions: {method_summary}.",
            f"- Parameters: `{params_text}`.",
            f"- Indicators: inferred from `generate_signals` source and required feature columns; exact source anchor is `pm_strategies.py:{methods.get('generate_signals', {}).get('line', 'unknown')}`.",
            "- Entry rules:",
            *[f"  - `{line}`" for line in entry_lines[:12]],
            "- Exit rules: signal creates direction only; stop/target exits are executed by `pm_core._backtest_loop_numba` and `Backtester._run_python_loop` using SL-before-TP ordering.",
            "- Stop logic: strategy stop/target pips come from `calculate_stops` or `BaseStrategy.build_trade_intent`; default ATR multipliers are propagated to precomputed SL/TP arrays before the Numba loop.",
            "- Take-profit logic: TP price is precomputed from strategy TP pips and then checked in the backtest/live execution path.",
            "- Trailing logic: no strategy-specific trailing was confirmed from this static anatomy; order governance overlays are separate and must be tested independently.",
            "- Break-even logic: no strategy-specific break-even path was confirmed from this static anatomy; governance overlays may adjust stops outside signal generation.",
            "- Session logic: present only if strategy source uses time/session columns; ledger-level session attribution is not present in existing `ledger_tradeable_cells.csv`.",
            "- Regime logic: winner selection is regime-specific in `pm_configs.json`; the strategy class itself generally emits raw signals independent of the final promoted regime cell.",
            "",
            "## Profit attribution",
            f"- Source: `agent_audit/data/ledger_tradeable_cells.csv`; cells reviewed: {len(items)}.",
            f"- Symbols: {', '.join(symbols) if symbols else 'not recorded'}.",
            f"- Timeframes: {', '.join(timeframes) if timeframes else 'not recorded'}.",
            f"- Regimes: {', '.join(regimes) if regimes else 'not recorded'}.",
            f"- Promotion statuses: `{json.dumps(dict(statuses), sort_keys=True)}`.",
            "",
            *top_table(sorted_items),
            "",
            "| Attribution dimension | Evidence status | Required next data |",
            "|---|---|---|",
            "| Long/short direction | Not present in ledger summary | Replay/trade records with direction and final R |",
            "| Session | Not present in ledger summary | Add/session-enrich replay output |",
            "| Holding duration | Not present in ledger summary | Trade records after replay |",
            "| Exit reason | Not present in ledger summary | New MAE/MFE trade records include exit reason |",
            "| Volatility level | Not present in ledger summary | Join trades to ATR/regime features |",
            "| Spread level | Not present in ledger summary | Historical spread columns or live actionable logs |",
            "| Parameter region | Partially present in winner artifacts, not in this CSV | Join to `pm_configs.json` winner params |",
            "| Strategy combination | Portfolio-level interaction not present | Replay concurrent promoted book |",
            "",
            "## Loss attribution",
            "- Immediate adverse entries: now measurable through `mae_pips`, `mae_r`, and `time_to_mae_bars`; existing historical summaries cannot classify it.",
            "- Repeated stop-out structures: requires replay with `exit_reason`, `mae_r`, and symbol/regime grouping.",
            "- Poor payoff structures: compare `mfe_r` versus final realized R after replay.",
            "- High-cost low-edge trades: requires spread/commission/swap/slippage fields joined to trades.",
            "- Entries during unsuitable regimes: compare MAE/final R by promoted regime versus live/computed regime.",
            "- Entries with late confirmation: use time-to-MAE/time-to-MFE and first-bar MAE from the new records.",
            "- MFE given back: requires replay; the new fields make this testable but do not backfill old summaries.",
            "- Excessive holding time: not available in current ledger summary; add holding bars/time in replay export.",
            "- Repeated correlated exposure: not strategy-local; portfolio replay required.",
            "- Unprofitable parameter regions: join winner parameter metadata and replay performance.",
            "",
            "## Entry analysis",
            "- Static code anchor: `pm_strategies.py:{}`.".format(methods.get("generate_signals", {}).get("line", "unknown")),
            "- Backtester entry timing uses previous completed signal bar (`sig_arr[i - 1]`) and enters at the next bar open, which prevents same-bar signal/entry lookahead in the engine.",
            "- Parameter exposure is controlled by `get_default_params`/search-space generation; any new entry parameter must propagate through Optuna, fallback random search, strategy construction, winner export, reload, and live execution.",
            "- Redundant/delaying/low-quality entry conditions cannot be proven from the current summary CSV; use the bounded replay to compare first-bar MAE and final R by condition bucket.",
            "",
            "## Exit analysis",
            "- SL/TP exits are modeled in the trade-path implementation; Numba and Python paths now emit equivalent MAE/MFE fields.",
            "- MFE retained/given back is now measurable after replay but not derivable from old ledger summaries.",
            "- Stop-outs that later recover require post-exit feature/path analysis; current records do not include post-exit recovery.",
            "- Slippage is currently strongest for entry attribution; exit/TP fill slippage remains a queued measurement extension.",
            "",
            "## Strategy interaction analysis",
            "- Risk and position sizing: handled by `pm_core.Backtester` in research and `pm_position.PositionCalculator`/`pm_main.LiveTrader` in live.",
            "- Regime filters: regime-specific winners come from `pm_configs.json`; validation is a selection surface, not pristine post-selection OOS.",
            "- Portfolio filters: live position/risk gates can block otherwise promoted cells; replay needs portfolio context to measure missed positive expectancy.",
            "- Execution handling: live actionable logs now include entry slippage schema/version fields.",
            "- Cost model: backtest spread/commission/swap/slippage assumptions must be matched in USD 10,000 A/B replay before claiming profit improvement.",
            "- Symbol specifications: broker tick/pip/volume specs are part of `InstrumentSpec`/MT5 spec sync and must stay unchanged across baseline/candidate.",
            "- Parameter-selection process: current winner metrics can be sparse; use trade count and post-selection decay when comparing candidates.",
            "",
            "## Exact fix points",
            *[f"- {point}" for point in fix_points_for_strategy(strategy)],
            "",
            "## Optuna integration",
            "- No new optimizable parameter was added by the current measurement fixes.",
            "- If an entry/exit change is tested, add it to config schema, default params, Optuna suggestion, fallback random search, strategy construction, result export, winner reload, live execution, and search-space version.",
            "",
            "## Numba integration",
            "- Strategy signal generation remains Python/pandas; the trade path is Numba/Python dual-path.",
            "- Any exit-path change must be implemented in `_backtest_loop_numba` and `_run_python_loop` with parity tests.",
            "- Current MAE/MFE fields preserve legacy Numba return indices 0-15 and append new arrays at 16-24.",
            "",
            "## Bounded validation",
            "- Canonical baseline: USD 10,000, same data, same costs, same seed, same trial/replay budget.",
            f"- Minimum test: replay top promoted `{strategy}` cells, then compare net profit, PF, expectancy, drawdown, `mae_r`, `mfe_r`, time underwater, and trade count.",
            "- Verdict standard: accept only if the candidate increases/protects net value or removes unpaid drawdown without destroying profitable flow.",
            "",
        ]
        (ANATOMY / f"{sanitize_filename(strategy)}.md").write_text("\n".join(md), encoding="utf-8")

    write_csv(
        ANATOMY / "strategy_family_summary.csv",
        summary_rows,
        [
            "strategy_name",
            "cell_count",
            "symbols",
            "timeframes",
            "regimes",
            "status_counts",
            "avg_val_profit_factor",
            "avg_val_return_pct",
            "avg_val_drawdown_pct",
            "avg_selection_score",
            "avg_quality_score",
            "source_line",
        ],
    )
    return {
        "source_rows": len(rows),
        "strategy_count": len(grouped),
        "summary_csv": rel(ANATOMY / "strategy_family_summary.csv"),
        "strategy_files": [
            rel(ANATOMY / f"{sanitize_filename(name)}.md")
            for name in sorted(grouped)
        ],
    }


def _promotion_status(payload: dict[str, Any]) -> str:
    return str(payload.get("promotion_status", "BLOCKED") or "BLOCKED").upper()


def _is_live_tradeable(payload: dict[str, Any]) -> bool:
    name = str(payload.get("strategy_name", "") or "").upper()
    meta = payload.get("artifact_meta") if isinstance(payload.get("artifact_meta"), dict) else {}
    status = _promotion_status(payload)
    return bool(
        name
        and name != "NO_TRADE"
        and not meta.get("no_trade")
        and status not in {"NO_TRADE", "BLOCKED", "PAPER_ONLY"}
    )


def _current_promoted_cells(ledger: dict[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for symbol, symbol_payload in sorted(ledger.items()):
        if not isinstance(symbol_payload, dict):
            continue
        symbol_is_promoted = _is_live_tradeable(symbol_payload)
        leaves: list[dict[str, Any]] = []
        regimes = symbol_payload.get("regime_configs")
        if isinstance(regimes, dict):
            for timeframe, regime_payload in sorted(regimes.items()):
                if not isinstance(regime_payload, dict):
                    continue
                for regime, cell in sorted(regime_payload.items()):
                    if isinstance(cell, dict) and _is_live_tradeable(cell):
                        metrics = cell.get("val_metrics") if isinstance(cell.get("val_metrics"), dict) else {}
                        leaves.append({
                            "symbol": str(symbol_payload.get("symbol") or symbol).upper(),
                            "timeframe": str(cell.get("timeframe") or timeframe).upper(),
                            "regime": str(cell.get("regime") or regime).upper(),
                            "strategy_name": cell.get("strategy_name", ""),
                            "promotion_status": _promotion_status(cell),
                            "promotion_source": "symbol_and_regime" if symbol_is_promoted else "regime_leaf",
                            "val_trades": metrics.get("total_trades", cell.get("regime_val_trades", "")),
                            "val_profit_factor": metrics.get("profit_factor", ""),
                            "valid_until": cell.get("valid_until", symbol_payload.get("valid_until", "")),
                        })
        if leaves:
            seen = set()
            for row in leaves:
                key = (row["symbol"], row["timeframe"], row["regime"], row["strategy_name"])
                if key in seen:
                    continue
                seen.add(key)
                rows.append(row)
        elif symbol_is_promoted:
            metrics = symbol_payload.get("val_metrics") if isinstance(symbol_payload.get("val_metrics"), dict) else {}
            rows.append({
                "symbol": str(symbol_payload.get("symbol") or symbol).upper(),
                "timeframe": str(symbol_payload.get("timeframe") or "H1").upper(),
                "regime": str(symbol_payload.get("regime") or "SYMBOL_LEVEL").upper(),
                "strategy_name": symbol_payload.get("strategy_name", ""),
                "promotion_status": _promotion_status(symbol_payload),
                "promotion_source": "symbol_level",
                "val_trades": metrics.get("total_trades", symbol_payload.get("regime_val_trades", "")),
                "val_profit_factor": metrics.get("profit_factor", ""),
                "valid_until": symbol_payload.get("valid_until", ""),
            })
    return rows


def write_current_ledger_inventory() -> dict[str, Any]:
    ledger_path = ROOT / "pm_configs.json"
    out_dir = OUT / "data"
    out_dir.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "symbol",
        "timeframe",
        "regime",
        "strategy_name",
        "promotion_status",
        "promotion_source",
        "val_trades",
        "val_profit_factor",
        "valid_until",
    ]
    if not ledger_path.exists():
        inventory = {
            "created_at": datetime.now().isoformat(),
            "ledger_path": "pm_configs.json",
            "exists": False,
        }
        (out_dir / "current_ledger_inventory.json").write_text(
            json.dumps(inventory, indent=2, sort_keys=True),
            encoding="utf-8",
        )
        write_csv(out_dir / "current_promoted_cells.csv", [], fieldnames)
        return inventory

    ledger = load_json(ledger_path)
    symbol_status = Counter()
    leaf_status = Counter()
    top_level_count = 0
    regime_leaf_count = 0
    if isinstance(ledger, dict):
        for _symbol, symbol_payload in ledger.items():
            if not isinstance(symbol_payload, dict):
                continue
            top_level_count += 1
            symbol_status[_promotion_status(symbol_payload)] += 1
            regimes = symbol_payload.get("regime_configs")
            if isinstance(regimes, dict):
                for regime_payload in regimes.values():
                    if not isinstance(regime_payload, dict):
                        continue
                    for cell in regime_payload.values():
                        if isinstance(cell, dict):
                            regime_leaf_count += 1
                            leaf_status[_promotion_status(cell)] += 1
    promoted = _current_promoted_cells(ledger if isinstance(ledger, dict) else {})
    write_csv(out_dir / "current_promoted_cells.csv", promoted, fieldnames)
    stat = ledger_path.stat()
    inventory = {
        "created_at": datetime.now().isoformat(),
        "ledger_path": "pm_configs.json",
        "exists": True,
        "mtime": datetime.fromtimestamp(stat.st_mtime).isoformat(),
        "sha256": sha256_file(ledger_path),
        "top_level_symbol_count": top_level_count,
        "regime_leaf_count": regime_leaf_count,
        "symbol_status_counts": dict(sorted(symbol_status.items())),
        "regime_leaf_status_counts": dict(sorted(leaf_status.items())),
        "logical_promoted_cell_count": len(promoted),
        "logical_promoted_cells": promoted,
        "stale_prior_tradeable_cell_file": {
            "path": "agent_audit/data/ledger_tradeable_cells.csv",
            "exists": (ROOT / "agent_audit" / "data" / "ledger_tradeable_cells.csv").exists(),
            "note": "Historical evidence from the prior ledger state; do not read it as the current live book.",
        },
    }
    (out_dir / "current_ledger_inventory.json").write_text(
        json.dumps(inventory, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    return inventory


def write_performance_comparisons(manifest: dict[str, Any]) -> None:
    git_head = manifest.get("git_head") or ""
    files = {item["path"]: item for item in manifest.get("files", [])}
    config_hash = files.get("config.json", {}).get("sha256", "")
    data_hash = files.get("pm_configs.json", {}).get("sha256", "")
    common = {
        "commit_fingerprint": git_head,
        "config_fingerprint": config_hash,
        "symbol": "unit_fixture_or_all",
        "timeframe": "unit_fixture_or_all",
        "strategy": "not_strategy_specific_measurement_fix",
        "data_fingerprint": data_hash,
        "starting_balance": "10000",
        "trial_budget": "0_full_optimizer_not_run",
        "seed": "not_applicable",
        "numba_status": "compiled path covered where applicable",
        "net_profit": "not_changed_measurement_only",
        "return_pct": "not_changed_measurement_only",
        "drawdown_pct": "not_changed_measurement_only",
        "recovery_factor": "not_changed_measurement_only",
        "trade_count": "not_changed_measurement_only",
        "costs": "not_changed_measurement_only",
        "runtime": "unit_tests_only",
        "memory": "not_measured",
    }
    rows = [
        {
            **common,
            "fix_id": "PERF-FIX-001",
            "baseline_or_candidate": "candidate",
            "profit_factor": "not_changed_measurement_only",
            "expectancy": "not_changed_measurement_only",
            "mae_mfe_summaries": "9 new backtest fields plus schema version emitted; Python/Numba parity tested",
            "verdict": "MEASUREMENT-ONLY CHANGE",
            "notes": "Adds MAE/MFE pips/R, time-to-MAE/MFE, underwater bars, stop/target distance, and trade schema version.",
        },
        {
            **common,
            "fix_id": "PERF-FIX-002",
            "baseline_or_candidate": "candidate",
            "profit_factor": "not_changed_measurement_only",
            "expectancy": "not_changed_measurement_only",
            "mae_mfe_summaries": "not_applicable_live_entry_slippage",
            "verdict": "MEASUREMENT-ONLY CHANGE",
            "notes": "Adds expected/filled entry, adverse slippage pips/R, slippage direction, and actionable schema version.",
        },
        {
            **common,
            "fix_id": "PERF-FIX-003",
            "baseline_or_candidate": "candidate",
            "profit_factor": "inf for profitable no-loss sample instead of 0.0",
            "expectancy": "not_changed",
            "mae_mfe_summaries": "not_applicable_metric_sentinel",
            "verdict": "ACCEPT",
            "notes": "Dashboard no-loss PF now matches core engine; JSON-safe conversion verified by Claude Code review.",
        },
        {
            **common,
            "fix_id": "PERF-FIX-004",
            "baseline_or_candidate": "candidate",
            "profit_factor": "not_changed_measurement_only",
            "expectancy": "not_changed_measurement_only",
            "mae_mfe_summaries": "schema version prevents old missing fields being read as zero excursion",
            "verdict": "MEASUREMENT-ONLY CHANGE",
            "notes": "Adds `trade_record_schema_version` and `actionable_schema_version`.",
        },
        {
            **common,
            "fix_id": "PERF-FIX-005",
            "baseline_or_candidate": "candidate",
            "profit_factor": "shared sentinel helper across engine/dashboard",
            "expectancy": "not_changed",
            "mae_mfe_summaries": "not_applicable_metric_contract",
            "verdict": "REPORTING/CALCULATION FIX",
            "notes": "Adds shared metric helpers, explicit return_drawdown_ratio/calmar basis fields, annualized Calmar companion, and benchmark recovery-factor correction.",
        },
        {
            **common,
            "fix_id": "PERF-FIX-006",
            "baseline_or_candidate": "candidate",
            "profit_factor": "not_changed",
            "expectancy": "not_changed",
            "mae_mfe_summaries": "not_applicable_live_readiness",
            "verdict": "READINESS FIX",
            "notes": "Adds read-only preflight mode and broker min-lot tradeability warnings/errors before live startup.",
        },
    ]
    replay_rows = read_csv(OUT / "data" / "promoted_cell_replay_10000.csv")
    if replay_rows:
        replay = replay_rows[0]
        rows.append({
            **common,
            "fix_id": "PERF-FIX-007",
            "baseline_or_candidate": "bounded_replay_evidence",
            "symbol": replay.get("symbol", ""),
            "timeframe": replay.get("timeframe", ""),
            "strategy": replay.get("strategy_name", ""),
            "net_profit": replay.get("net_profit", ""),
            "return_pct": replay.get("return_pct", ""),
            "drawdown_pct": replay.get("max_drawdown_pct", ""),
            "recovery_factor": replay.get("return_drawdown_ratio", ""),
            "profit_factor": replay.get("profit_factor", ""),
            "expectancy": replay.get("expectancy_pips", ""),
            "trade_count": replay.get("total_trades", ""),
            "mae_mfe_summaries": (
                f"avg_mae_r={replay.get('avg_mae_r', '')}; "
                f"avg_mfe_r={replay.get('avg_mfe_r', '')}; "
                f"schema={replay.get('schema_version', '')}"
            ),
            "runtime": "bounded_one_cell_replay",
            "verdict": "CURRENT CELL REPLAYED NEGATIVE" if str(replay.get("error", "")) == "" else "REPLAY ERROR",
            "notes": "Read-only replay evidence for the current logical promoted live-book cell; no strategy or optimizer behavior changed.",
        })
    write_csv(OUT / "PERFORMANCE_COMPARISONS.csv", rows, PERFORMANCE_COMPARISON_COLUMNS)


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    ANATOMY.mkdir(parents=True, exist_ok=True)

    current_inventory = write_current_ledger_inventory()
    manifest = build_manifest()
    baseline = build_baseline()
    anatomy = build_strategy_anatomy()
    write_performance_comparisons(manifest)
    manifest = build_manifest()

    (OUT / "evidence_manifest.json").write_text(
        json.dumps(manifest, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    (OUT / "baseline_10000_summary.json").write_text(
        json.dumps(baseline, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    (OUT / "strategy_anatomy_summary.json").write_text(
        json.dumps(anatomy, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    print(json.dumps({
        "evidence_manifest": "agent_audit/performance/evidence_manifest.json",
        "baseline": "agent_audit/performance/baseline_10000_summary.json",
        "current_ledger_inventory": {
            "json": "agent_audit/performance/data/current_ledger_inventory.json",
            "promoted_cells_csv": "agent_audit/performance/data/current_promoted_cells.csv",
            "logical_promoted_cell_count": current_inventory.get("logical_promoted_cell_count"),
        },
        "strategy_anatomy": anatomy,
        "comparison_columns": PERFORMANCE_COMPARISON_COLUMNS,
    }, indent=2))


if __name__ == "__main__":
    main()
