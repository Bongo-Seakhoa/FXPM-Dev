"""Build unshaped metric-over-trade-time trajectories for benchmark winners.

This intentionally avoids the ThreadSpark geometry/geomean collapse. It replays the
already-selected symbol champions from benchmark ledgers, slices each full-history
trade list into equal trade-count windows, and reports raw metric trajectories.
"""

from __future__ import annotations

import argparse
import html
import json
import math
import subprocess
import sys
from dataclasses import fields
from datetime import datetime
from pathlib import Path
from typing import Any

import numpy as np

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

from pm_core import Backtester, DataLoader, FeatureComputer, PipelineConfig, get_instrument_spec
from pm_strategies import StrategyRegistry


MODELS = ("legacy", "specialist_spokes", "specialist_spokes_threadspark")
MODEL_LABEL = {
    "legacy": "Legacy",
    "specialist_spokes": "Old Spokes",
    "specialist_spokes_threadspark": "ThreadSpark v1.1",
}
MODEL_SHORT = {
    "legacy": "Legacy",
    "specialist_spokes": "Old",
    "specialist_spokes_threadspark": "TS v1.1",
}
MODEL_COLOR = {
    "legacy": "#2f6f9f",
    "specialist_spokes": "#b87916",
    "specialist_spokes_threadspark": "#b4443c",
}


def esc(value: object) -> str:
    return html.escape(str(value))


def load_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8-sig"))


def finite(value: object, default: float = 0.0) -> float:
    try:
        out = float(value)
    except (TypeError, ValueError):
        return default
    return out if math.isfinite(out) else default


def fmt_num(value: object, digits: int = 2) -> str:
    if value is None:
        return "n/a"
    try:
        v = float(value)
    except (TypeError, ValueError):
        return esc(value)
    if math.isnan(v):
        return "n/a"
    if math.isinf(v):
        return "∞" if v > 0 else "-∞"
    return f"{v:,.{digits}f}"


def fmt_pct(value: object, digits: int = 2, sign: bool = False) -> str:
    if value is None:
        return "n/a"
    try:
        v = float(value)
    except (TypeError, ValueError):
        return esc(value)
    if math.isnan(v) or math.isinf(v):
        return "n/a"
    prefix = "+" if sign and v > 0 else ""
    return f"{prefix}{v:,.{digits}f}%"


def fmt_int(value: object) -> str:
    try:
        return f"{int(round(float(value))):,}"
    except (TypeError, ValueError):
        return "n/a"


def git_head() -> str:
    try:
        return subprocess.check_output(["git", "rev-parse", "--short", "HEAD"], cwd=REPO, text=True).strip()
    except Exception:
        return "unknown"


def build_config(spread_mode: str) -> PipelineConfig:
    cfg_json = load_json(REPO / "config.json")
    pipe = cfg_json.get("pipeline", {})
    allowed = {field.name for field in fields(PipelineConfig)}
    kwargs = {key: value for key, value in pipe.items() if key in allowed}
    kwargs["data_dir"] = str(REPO / "data")
    regime_params_file = kwargs.get("regime_params_file", "regime_params.json")
    kwargs["regime_params_file"] = str(REPO / regime_params_file)
    cfg = PipelineConfig(**kwargs)
    setattr(cfg, "backtest_spread_mode", spread_mode)
    setattr(cfg, "use_spread", True)
    return cfg


def profit_factor(trades: list[dict[str, Any]]) -> float:
    gross_profit = sum(float(t.get("pnl_dollars", 0.0)) for t in trades if float(t.get("pnl_dollars", 0.0)) > 0)
    gross_loss = abs(sum(float(t.get("pnl_dollars", 0.0)) for t in trades if float(t.get("pnl_dollars", 0.0)) < 0))
    if gross_loss > 0:
        return gross_profit / gross_loss
    return math.inf if gross_profit > 0 else 0.0


def max_drawdown_pct(equity: list[float]) -> float:
    if len(equity) < 2:
        return 0.0
    peak = equity[0]
    max_dd = 0.0
    for raw in equity:
        value = float(raw)
        peak = max(peak, value)
        if peak > 0:
            max_dd = max(max_dd, (peak - value) / peak * 100.0)
    return max_dd


def ulcer_and_time_underwater(equity: list[float]) -> tuple[float, float]:
    if len(equity) < 2:
        return 0.0, 0.0
    arr = np.asarray(equity, dtype=np.float64)
    peak = np.maximum.accumulate(arr)
    dd = np.where(peak > 0, (peak - arr) / peak, 0.0)
    ulcer = float(np.sqrt(np.mean((dd * 100.0) ** 2)))
    tuw = float(np.mean(dd > 1e-9) * 100.0)
    return ulcer, tuw


def max_consecutive_losses(trades: list[dict[str, Any]]) -> int:
    best = run = 0
    for trade in trades:
        if float(trade.get("pnl_dollars", 0.0)) < 0:
            run += 1
            best = max(best, run)
        else:
            run = 0
    return best


def chunk_metrics(
    trades: list[dict[str, Any]],
    start_equity: float,
    initial_capital: float,
    cumulative_pnl_before: float,
) -> dict[str, Any]:
    pnl_values = [float(t.get("pnl_dollars", 0.0)) for t in trades]
    r_values = [float(t.get("r_multiple", 0.0)) for t in trades]
    gross_profit = sum(v for v in pnl_values if v > 0)
    gross_loss = abs(sum(v for v in pnl_values if v < 0))
    total_pnl = sum(pnl_values)
    equity = [float(start_equity)]
    current = float(start_equity)
    for pnl in pnl_values:
        current += pnl
        equity.append(current)
    dd = max_drawdown_pct(equity)
    ulcer, tuw = ulcer_and_time_underwater(equity)
    wins = sum(1 for v in pnl_values if v > 0)
    losses = sum(1 for v in pnl_values if v < 0)
    r_arr = np.asarray(r_values, dtype=np.float64) if r_values else np.asarray([], dtype=np.float64)
    if len(r_arr):
        tail_n = max(1, int(math.ceil(len(r_arr) * 0.20)))
        tail_cvar = abs(float(np.mean(np.sort(r_arr)[:tail_n])))
        worst_5 = float(np.percentile(r_arr, 5)) if len(r_arr) >= 20 else float(np.min(r_arr))
        mean_r = float(np.mean(r_arr))
        median_r = float(np.median(r_arr))
        pct_pos_r = float(np.mean(r_arr > 0) * 100.0)
    else:
        tail_cvar = worst_5 = mean_r = median_r = pct_pos_r = 0.0
    cumulative_pnl = cumulative_pnl_before + total_pnl
    return {
        "trades": len(trades),
        "wins": wins,
        "losses": losses,
        "return_pct": total_pnl / initial_capital * 100.0,
        "cumulative_return_pct": cumulative_pnl / initial_capital * 100.0,
        "profit_factor": profit_factor(trades),
        "win_rate": wins / len(trades) * 100.0 if trades else 0.0,
        "mean_r": mean_r,
        "median_r": median_r,
        "pct_positive_r": pct_pos_r,
        "tail_cvar_r": tail_cvar,
        "worst_5pct_r": worst_5,
        "max_dd_pct": dd,
        "capital_preservation": max(0.0, 100.0 - dd),
        "ulcer_index": ulcer,
        "time_underwater_pct": tuw,
        "max_consecutive_losses": max_consecutive_losses(trades),
        "net_to_gross": total_pnl / gross_profit if gross_profit > 0 else 0.0,
        "gross_profit": gross_profit,
        "gross_loss": gross_loss,
        "total_pnl": total_pnl,
    }


def replay_winner(cfg: PipelineConfig, loader: DataLoader, winner: dict[str, Any]) -> dict[str, Any]:
    symbol = winner["symbol"]
    timeframe = winner["timeframe"]
    strategy_name = winner["strategy_name"]
    params = winner.get("parameters", {}) or {}
    base = loader.load_symbol(symbol)
    if base is None or len(base) == 0:
        raise RuntimeError(f"{symbol}: no data")
    df = loader.resample(base, timeframe)
    if df is None or len(df) < 200:
        raise RuntimeError(f"{symbol} {timeframe}: insufficient bars")
    strategy = StrategyRegistry.get(strategy_name, **params)
    features = FeatureComputer.compute_for_strategy(
        df,
        strategy,
        symbol=symbol,
        timeframe=timeframe,
        regime_params_file=cfg.regime_params_file,
    )
    signals = strategy.generate_signals(features, symbol)
    result = Backtester(cfg).run(
        features,
        signals,
        symbol,
        strategy,
        spec=get_instrument_spec(symbol),
        timeframe=timeframe,
    )
    return result


def trajectory_for_result(result: dict[str, Any], initial_capital: float, windows: int) -> list[dict[str, Any]]:
    trades = list(result.get("trades") or [])
    if not trades:
        return []
    indices = np.array_split(np.arange(len(trades)), min(windows, len(trades)))
    out = []
    start_equity = float(initial_capital)
    cumulative_pnl = 0.0
    for idx, chunk_idx in enumerate(indices, start=1):
        chunk = [trades[int(i)] for i in chunk_idx]
        metrics = chunk_metrics(chunk, start_equity, initial_capital, cumulative_pnl)
        cumulative_pnl += float(metrics["total_pnl"])
        start_equity += float(metrics["total_pnl"])
        metrics.update({
            "window": idx,
            "start_trade": int(chunk_idx[0]) + 1,
            "end_trade": int(chunk_idx[-1]) + 1,
        })
        out.append(metrics)
    return out


def sanitize_json(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(k): sanitize_json(v) for k, v in value.items()}
    if isinstance(value, list):
        return [sanitize_json(v) for v in value]
    if isinstance(value, float):
        if math.isnan(value):
            return None
        if math.isinf(value):
            return "Infinity" if value > 0 else "-Infinity"
        return round(value, 8)
    return value


def build_trajectory_data(experiment: Path, windows: int, spread_mode: str) -> dict[str, Any]:
    cfg = build_config(spread_mode)
    loader = DataLoader(cfg.data_dir)
    bench_dir = experiment / "benchmarks"
    records = {model: load_json(bench_dir / f"{model}.json") for model in MODELS}
    ledgers = {model: load_json(Path(records[model]["ledger"])) for model in MODELS}
    symbols = list(records["specialist_spokes_threadspark"]["symbols"])
    output: dict[str, Any] = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "engine_commit": git_head(),
        "spread_mode": spread_mode,
        "windows": windows,
        "symbols": symbols,
        "models": {},
    }
    for model in MODELS:
        output["models"][model] = {}
        for symbol in symbols:
            winner = ledgers[model][symbol]
            result = replay_winner(cfg, loader, winner)
            output["models"][model][symbol] = {
                "symbol": symbol,
                "model": model,
                "strategy": winner.get("strategy_name"),
                "timeframe": winner.get("timeframe"),
                "summary": {
                    "trades": int(result.get("total_trades", 0)),
                    "return_pct": finite(result.get("total_return_pct")),
                    "profit_factor": result.get("profit_factor"),
                    "win_rate": finite(result.get("win_rate")),
                    "max_dd_pct": finite(result.get("max_drawdown_pct")),
                    "ulcer_index": finite(result.get("ulcer_index")),
                    "time_underwater_pct": finite(result.get("time_underwater_pct")),
                    "mean_r": finite(result.get("mean_r")),
                    "tail_cvar_r": finite(result.get("r_block_cvar")),
                },
                "windows": trajectory_for_result(result, cfg.initial_capital, windows),
            }
    return sanitize_json(output)


def js_data(data: dict[str, Any]) -> str:
    return json.dumps(data, ensure_ascii=False, allow_nan=False)


class HtmlReport:
    def __init__(self, data: dict[str, Any]) -> None:
        self.data = data
        self.symbols = data["symbols"]

    def value(self, model: str, symbol: str, key: str) -> float:
        return finite(self.data["models"][model][symbol]["summary"].get(key))

    def metric_extent(self, metric: str, cap_pf: bool = False) -> tuple[float, float]:
        vals = []
        for model in MODELS:
            for symbol in self.symbols:
                for row in self.data["models"][model][symbol]["windows"]:
                    value = row.get(metric)
                    if value in ("Infinity", "-Infinity"):
                        value = 10.0 if value == "Infinity" else -10.0
                    value = finite(value)
                    if cap_pf and metric == "profit_factor":
                        value = min(value, 10.0)
                    vals.append(value)
        if not vals:
            return 0.0, 1.0
        mn, mx = min(vals), max(vals)
        if metric in {"profit_factor", "capital_preservation", "win_rate", "pct_positive_r"}:
            mn = min(mn, 0.0)
        if metric in {"return_pct", "cumulative_return_pct", "mean_r", "net_to_gross"}:
            mn = min(mn, 0.0)
            mx = max(mx, 0.0)
        if abs(mx - mn) < 1e-9:
            mx = mn + 1.0
        pad = (mx - mn) * 0.08
        return mn - pad, mx + pad

    def line_chart(self, symbol: str, metric: str, title: str, subtitle: str, percent: bool = False, cap_pf: bool = False) -> str:
        width, height = 720, 310
        left, right, top, bottom = 54, 22, 52, 46
        plot_w, plot_h = width - left - right, height - top - bottom
        mn, mx = self.metric_extent(metric, cap_pf=cap_pf)

        def y_coord(value: float) -> float:
            return top + (mx - value) / (mx - mn) * plot_h

        def x_coord(window: int) -> float:
            max_w = max(1, int(self.data["windows"]))
            return left + (window - 1) / max(1, max_w - 1) * plot_w

        parts = [
            f'<svg class="chart small" viewBox="0 0 {width} {height}" role="img" aria-label="{esc(title)}">',
            f'<text class="svg-title" x="{left}" y="23">{esc(title)}</text>',
            f'<text class="svg-subtitle" x="{left}" y="42">{esc(subtitle)}</text>',
            f'<line x1="{left}" y1="{height-bottom}" x2="{width-right}" y2="{height-bottom}" stroke="#5b625f"/>',
            f'<line x1="{left}" y1="{top}" x2="{left}" y2="{height-bottom}" stroke="#5b625f"/>',
        ]
        if mn <= 0 <= mx:
            y0 = y_coord(0)
            parts.append(f'<line x1="{left}" y1="{y0:.1f}" x2="{width-right}" y2="{y0:.1f}" stroke="#8c928e" stroke-dasharray="3 5"/>')
        if metric == "profit_factor" and mn <= 1 <= mx:
            y1 = y_coord(1)
            parts.append(f'<line x1="{left}" y1="{y1:.1f}" x2="{width-right}" y2="{y1:.1f}" stroke="#7b6a2d" stroke-dasharray="4 4"/>')
            parts.append(f'<text class="tick" x="{width-right-4}" y="{y1-4:.1f}" text-anchor="end">PF 1.0</text>')
        for tick in (mn, (mn + mx) / 2, mx):
            y = y_coord(tick)
            label = fmt_pct(tick, 0) if percent else fmt_num(tick, 2)
            parts.append(f'<text class="tick" x="{left-8}" y="{y+4:.1f}" text-anchor="end">{esc(label)}</text>')
        for model in MODELS:
            points = []
            for row in self.data["models"][model][symbol]["windows"]:
                raw = row.get(metric)
                if raw == "Infinity":
                    value = 10.0
                elif raw == "-Infinity":
                    value = -10.0
                else:
                    value = finite(raw)
                if cap_pf and metric == "profit_factor":
                    value = min(value, 10.0)
                points.append((x_coord(int(row["window"])), y_coord(value), value))
            if not points:
                continue
            path = " ".join(("M" if idx == 0 else "L") + f"{x:.1f},{y:.1f}" for idx, (x, y, _v) in enumerate(points))
            parts.append(f'<path d="{path}" fill="none" stroke="{MODEL_COLOR[model]}" stroke-width="2.4"/>')
            for x, y, _value in points:
                parts.append(f'<circle cx="{x:.1f}" cy="{y:.1f}" r="3.2" fill="{MODEL_COLOR[model]}" stroke="#fff" stroke-width="1"/>')
        for i in range(1, int(self.data["windows"]) + 1):
            x = x_coord(i)
            parts.append(f'<text class="tick" x="{x:.1f}" y="{height-18}" text-anchor="middle">{i}</text>')
        lx, ly = left, height - 3
        for model in MODELS:
            parts.append(f'<rect x="{lx}" y="{ly-10}" width="11" height="7" rx="2" fill="{MODEL_COLOR[model]}"/><text class="legend" x="{lx+16}" y="{ly-3}">{MODEL_SHORT[model]}</text>')
            lx += 110
        parts.append("</svg>")
        return "".join(parts)

    def trajectory_table(self, symbol: str) -> str:
        rows = ""
        for model in MODELS:
            rec = self.data["models"][model][symbol]
            rows += f'<tr class="section-row"><th colspan="12"><span class="swatch" style="background:{MODEL_COLOR[model]}"></span>{MODEL_LABEL[model]} - {esc(rec["strategy"])} / {esc(rec["timeframe"])}</th></tr>'
            for row in rec["windows"]:
                pf = row.get("profit_factor")
                pf_text = "&infin;" if pf == "Infinity" else fmt_num(pf, 2)
                rows += (
                    f'<tr><td>{row["window"]}</td><td>{row["start_trade"]}-{row["end_trade"]}</td>'
                    f'<td class="num">{fmt_int(row["trades"])}</td><td class="num">{fmt_pct(row["return_pct"],2,True)}</td>'
                    f'<td class="num">{fmt_pct(row["cumulative_return_pct"],2,True)}</td><td class="num">{pf_text}</td>'
                    f'<td class="num">{fmt_pct(row["win_rate"],1)}</td><td class="num">{fmt_num(row["mean_r"],3)}</td>'
                    f'<td class="num">{fmt_num(row["tail_cvar_r"],3)}</td><td class="num">{fmt_pct(row["max_dd_pct"],2)}</td>'
                    f'<td class="num">{fmt_num(row["net_to_gross"],3)}</td><td class="num">{row["max_consecutive_losses"]}</td></tr>'
                )
        return rows

    def symbol_card(self, symbol: str) -> str:
        summaries = []
        for model in MODELS:
            rec = self.data["models"][model][symbol]
            summary = rec["summary"]
            pf = summary.get("profit_factor")
            pf_text = "&infin;" if pf == "Infinity" else fmt_num(pf, 2)
            summaries.append(
                f'<div class="mini-summary" style="border-top-color:{MODEL_COLOR[model]}"><b>{MODEL_SHORT[model]}</b>'
                f'<span>{esc(rec["strategy"])} / {esc(rec["timeframe"])}</span>'
                f'<dl><dt>Return</dt><dd>{fmt_pct(summary["return_pct"],2,True)}</dd>'
                f'<dt>Trades</dt><dd>{fmt_int(summary["trades"])}</dd>'
                f'<dt>PF</dt><dd>{pf_text}</dd>'
                f'<dt>Max-DD</dt><dd>{fmt_pct(summary["max_dd_pct"],2)}</dd></dl></div>'
            )
        return f"""<section class="panel symbol-panel">
<h2>{esc(symbol)} metric trajectories</h2>
<div class="mini-grid">{''.join(summaries)}</div>
<div class="chart-grid two">
{self.line_chart(symbol, 'profit_factor', 'Profit factor by trade-time window', 'Raw PF capped visually at 10; PF=1 line shown.', cap_pf=True)}
{self.line_chart(symbol, 'trades', 'Trades per window', 'Equal trade-count windows expose dense-vs-sparse edge formation.')}
{self.line_chart(symbol, 'cumulative_return_pct', 'Cumulative return path', 'Full-history replay, conservative_max.', percent=True)}
{self.line_chart(symbol, 'capital_preservation', 'Capital preservation', '100 - window max drawdown; higher is better.', percent=True)}
{self.line_chart(symbol, 'mean_r', 'Mean R per window', 'Risk-normalized expectancy.')}
{self.line_chart(symbol, 'tail_cvar_r', 'Tail CVaR of R', 'Lower is better; shown raw to avoid hiding tail damage.')}
</div>
<details><summary>Window metric table for {esc(symbol)}</summary><div class="table-wrap"><table><thead><tr><th>Win</th><th>Trades</th><th>N</th><th>Win Ret</th><th>Cum Ret</th><th>PF</th><th>Win Rate</th><th>Mean R</th><th>Tail CVaR</th><th>Max-DD</th><th>Net/Gross</th><th>Max Loss Streak</th></tr></thead><tbody>{self.trajectory_table(symbol)}</tbody></table></div></details>
</section>"""

    def separator_table(self) -> str:
        rows = ""
        for symbol in self.symbols:
            legacy = self.data["models"]["legacy"][symbol]
            ts = self.data["models"]["specialist_spokes_threadspark"][symbol]
            leg_pf = np.median([10.0 if r["profit_factor"] == "Infinity" else finite(r["profit_factor"]) for r in legacy["windows"]])
            ts_pf = np.median([10.0 if r["profit_factor"] == "Infinity" else finite(r["profit_factor"]) for r in ts["windows"]])
            leg_trades = np.median([finite(r["trades"]) for r in legacy["windows"]])
            ts_trades = np.median([finite(r["trades"]) for r in ts["windows"]])
            leg_mean_r = np.median([finite(r["mean_r"]) for r in legacy["windows"]])
            ts_mean_r = np.median([finite(r["mean_r"]) for r in ts["windows"]])
            leg_ret = legacy["summary"]["return_pct"]
            ts_ret = ts["summary"]["return_pct"]
            rows += (
                f"<tr><td>{symbol}</td><td>{esc(legacy['strategy'])} / {legacy['timeframe']}</td>"
                f"<td>{esc(ts['strategy'])} / {ts['timeframe']}</td><td class=\"num\">{fmt_num(leg_pf,2)}</td>"
                f"<td class=\"num\">{fmt_num(ts_pf,2)}</td><td class=\"num\">{fmt_num(leg_trades,1)}</td>"
                f"<td class=\"num\">{fmt_num(ts_trades,1)}</td><td class=\"num\">{fmt_num(leg_mean_r,3)}</td>"
                f"<td class=\"num\">{fmt_num(ts_mean_r,3)}</td><td class=\"num\">{fmt_pct(leg_ret,1,True)}</td>"
                f"<td class=\"num\">{fmt_pct(ts_ret,1,True)}</td></tr>"
            )
        return rows

    def threadspark_holder_table(self) -> str:
        rows = ""
        for symbol in self.symbols:
            rec = self.data["models"]["specialist_spokes_threadspark"][symbol]
            summary = rec["summary"]
            windows = rec["windows"]
            held = finite(summary["return_pct"]) > 0
            status = "HELD" if held else "FELL"
            status_class = "status-good" if held else "status-bad"
            pfs = [
                10.0 if row["profit_factor"] == "Infinity" else finite(row["profit_factor"])
                for row in windows
            ]
            pos_windows = sum(1 for row in windows if finite(row["return_pct"]) > 0)
            rows += (
                f'<tr><td>{esc(symbol)}</td><td><span class="{status_class}">{status}</span></td>'
                f'<td>{esc(rec["strategy"])} / {esc(rec["timeframe"])}</td>'
                f'<td class="num">{fmt_pct(summary["return_pct"],1,True)}</td>'
                f'<td class="num">{fmt_pct(summary["max_dd_pct"],1)}</td>'
                f'<td class="num">{fmt_int(summary["trades"])}</td>'
                f'<td class="num">{fmt_num(np.median(pfs),2)}</td>'
                f'<td class="num">{fmt_num(min(pfs),2)}</td>'
                f'<td class="num">{pos_windows}/{len(windows)}</td>'
                f'<td class="num">{fmt_num(np.median([finite(row["mean_r"]) for row in windows]),3)}</td>'
                f'<td class="num">{fmt_num(np.median([finite(row["net_to_gross"]) for row in windows]),3)}</td>'
                f'<td class="num">{fmt_pct(np.median([finite(row["capital_preservation"]) for row in windows]),1)}</td></tr>'
            )
        return rows

    def threadspark_holder_summary(self) -> str:
        groups: dict[str, list[dict[str, Any]]] = {"HELD": [], "FELL": []}
        for symbol in self.symbols:
            rec = self.data["models"]["specialist_spokes_threadspark"][symbol]
            groups["HELD" if finite(rec["summary"]["return_pct"]) > 0 else "FELL"].append(rec)

        def group_stats(records: list[dict[str, Any]]) -> dict[str, float]:
            windows = [row for rec in records for row in rec["windows"]]
            pfs = [
                10.0 if row["profit_factor"] == "Infinity" else finite(row["profit_factor"])
                for row in windows
            ]
            return {
                "return": float(np.median([finite(rec["summary"]["return_pct"]) for rec in records])),
                "pf": float(np.median(pfs)),
                "pos_windows": float(sum(1 for row in windows if finite(row["return_pct"]) > 0)),
                "total_windows": float(len(windows)),
                "mean_r": float(np.median([finite(row["mean_r"]) for row in windows])),
                "net_to_gross": float(np.median([finite(row["net_to_gross"]) for row in windows])),
                "cap": float(np.median([finite(row["capital_preservation"]) for row in windows])),
            }

        held = group_stats(groups["HELD"])
        fell = group_stats(groups["FELL"])
        return f"""<div class="metric-strip">
<div><b>Held median return</b><span>{fmt_pct(held['return'],1,True)}</span></div>
<div><b>Fell median return</b><span>{fmt_pct(fell['return'],1,True)}</span></div>
<div><b>Held med PF</b><span>{fmt_num(held['pf'],2)}</span></div>
<div><b>Fell med PF</b><span>{fmt_num(fell['pf'],2)}</span></div>
<div><b>Held positive windows</b><span>{fmt_int(held['pos_windows'])}/{fmt_int(held['total_windows'])}</span></div>
<div><b>Fell positive windows</b><span>{fmt_int(fell['pos_windows'])}/{fmt_int(fell['total_windows'])}</span></div>
<div><b>Held med net/gross</b><span>{fmt_num(held['net_to_gross'],3)}</span></div>
<div><b>Fell med net/gross</b><span>{fmt_num(fell['net_to_gross'],3)}</span></div>
</div>"""

    def build(self) -> str:
        generated = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        return f"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Metric Trajectory Report</title>
{self.styles()}
</head>
<body>
<header><div class="report"><h1>Unshaped Metric Trajectories</h1>
<p class="subhead">Full-history conservative_max replay of the already-selected winners, sliced into equal trade-count windows. This deliberately drops the 3-D shape/geomean collapse and looks at each metric as a propensity over trade time.</p>
<div class="meta"><span>Generated: {esc(generated)}</span><span>Windows: {self.data['windows']}</span><span>Models: 3</span><span>Symbols: {len(self.symbols)}</span><span>Engine: {esc(self.data.get('engine_commit','unknown'))}</span></div></div></header>
<main><div class="report">
<section class="panel callout"><div><h2>Why this exists</h2><p class="lead">The previous reports were valid for benchmark verdicts, but they still hid the question Bongo is asking now: do the individual metrics themselves separate sparse fat-edge strategies from dense thin-edge churners when we stop forcing them into an idealized shape?</p></div><div class="warning"><b>Evidence boundary</b><p>This is not a new optimization run. It replays selected winners from existing ledgers. It can diagnose selected-winner behavior; it cannot prove how every rejected candidate would rank unless a future run exports top-N candidate pools.</p></div></section>
<section class="panel"><h2>Separator Snapshot</h2><p class="lead">Median values across trade-time windows. This is the fastest read of the hypothesis: sparse high-PF/fat-edge versus dense low-PF/thin-edge.</p><div class="table-wrap"><table><thead><tr><th>Symbol</th><th>Legacy pick</th><th>ThreadSpark pick</th><th>Legacy med PF</th><th>TS med PF</th><th>Legacy med trades/window</th><th>TS med trades/window</th><th>Legacy med R</th><th>TS med R</th><th>Legacy return</th><th>TS return</th></tr></thead><tbody>{self.separator_table()}</tbody></table></div></section>
<section class="panel"><h2>ThreadSpark Held vs Fell</h2><p class="lead">This is the direct answer to Claude's latest question using the true full-history replay. It does not prove a production selector yet, but it shows whether the selected high-payout ThreadSpark winners have a different metric path than the ThreadSpark fall-offs.</p>{self.threadspark_holder_summary()}<div class="table-wrap"><table><thead><tr><th>Symbol</th><th>Outcome</th><th>Pick</th><th>Return</th><th>Max-DD</th><th>Trades</th><th>Med PF</th><th>Min PF</th><th>Positive windows</th><th>Med R</th><th>Med net/gross</th><th>Med cap preservation</th></tr></thead><tbody>{self.threadspark_holder_table()}</tbody></table></div></section>
{''.join(self.symbol_card(symbol) for symbol in self.symbols)}
<section class="panel"><h2>Reading Notes</h2><div class="notes"><div><b>PF and trade count together</b><p>A high PF on a small number of trades is not automatically deployable, but it is a different evidence signature from low PF across thousands of trades. This report keeps those two dimensions separate.</p></div><div><b>Capital preservation</b><p>Drawdown is shown as capital preservation where useful: 100 minus window max drawdown. Higher is better, matching the shape-preserving direction you asked for.</p></div><div><b>No final scorer here</b><p>This report does not propose another aggregate score. It is a diagnostic layer for deciding whether the metrics have life after removing the shape collapse.</p></div></div></section>
</div></main>
<footer><div class="report">Source: benchmark ledger winners replayed through the production Backtester. JSON sidecar is generated beside this HTML.</div></footer>
</body></html>"""

    @staticmethod
    def styles() -> str:
        return """<style>
:root{--ink:#202725;--muted:#66706d;--paper:#fbfaf7;--panel:#fff;--line:#ded8cf;--bad:#b4443c;--good:#2a8656;--warn:#b87916}
*{box-sizing:border-box} body{margin:0;background:var(--paper);color:var(--ink);font:14px/1.45 Inter,ui-sans-serif,system-ui,-apple-system,Segoe UI,Arial,sans-serif}
header{padding:34px 28px 20px;border-bottom:1px solid var(--line);background:#fffdf8}.report{max-width:1360px;margin:0 auto}
h1{margin:0 0 10px;font-size:clamp(30px,4vw,52px);line-height:1;letter-spacing:0}.subhead{max-width:1040px;color:#43504d;font-size:16px;margin:0}
.meta{display:flex;flex-wrap:wrap;gap:10px;margin-top:18px}.meta span{border:1px solid var(--line);background:#fff;padding:7px 10px;border-radius:6px;color:#47504e}
main{padding:24px 28px 60px}.panel{background:var(--panel);border:1px solid var(--line);border-radius:8px;padding:18px;margin:18px 0;box-shadow:0 1px 0 rgba(0,0,0,.02)}
.panel h2{font-size:22px;margin:0 0 8px}.lead{color:#44504d;max-width:1040px;margin:0 0 14px}.callout{display:grid;grid-template-columns:1.2fr .8fr;gap:14px}.warning{background:#fff8e8;border:1px solid #edd39e;border-radius:8px;padding:14px}
.mini-grid{display:grid;grid-template-columns:repeat(3,1fr);gap:10px;margin:12px 0}.mini-summary{border:1px solid var(--line);border-top:4px solid;border-radius:8px;padding:10px;background:#fff}.mini-summary b{display:block;font-size:15px}.mini-summary span{display:block;color:var(--muted);font-size:12px;margin:2px 0 8px}dl{display:grid;grid-template-columns:1fr auto;gap:4px;margin:0}dt{color:var(--muted)}dd{margin:0;font-weight:700}
.chart-grid{display:grid;gap:14px}.chart-grid.two{grid-template-columns:repeat(2,minmax(0,1fr))}.chart{width:100%;height:auto;background:#fff;border:1px solid var(--line);border-radius:8px}.svg-title{font-size:17px;font-weight:700;fill:#222}.svg-subtitle{font-size:11px;fill:#68716e}.tick,.legend{font-size:10px;fill:#39423f}
.table-wrap{overflow:auto;border:1px solid var(--line);border-radius:8px}table{width:100%;min-width:980px;border-collapse:collapse;background:#fff}th,td{padding:8px 9px;border-bottom:1px solid #ece6dd;text-align:left;vertical-align:top}th{background:#f6f0e8;font-size:12px;text-transform:uppercase;letter-spacing:.04em;color:#4d5653}td.num{text-align:right;font-variant-numeric:tabular-nums}.section-row th{background:#ebe2d7;text-transform:none;font-size:14px}.swatch{display:inline-block;width:10px;height:10px;border-radius:2px;margin-right:7px;vertical-align:-1px}
.metric-strip{display:grid;grid-template-columns:repeat(4,1fr);gap:10px;margin:10px 0 14px}.metric-strip div{border:1px solid var(--line);border-radius:8px;background:#fff;padding:10px}.metric-strip b{display:block;color:var(--muted);font-size:12px}.metric-strip span{display:block;font-weight:800;font-size:18px;margin-top:2px}.status-good,.status-bad{display:inline-block;border-radius:999px;padding:3px 8px;font-weight:800;font-size:11px}.status-good{background:#e7f3ea;color:#1f6b43}.status-bad{background:#fae9e7;color:#9b342d}
details{margin-top:12px}summary{cursor:pointer;font-weight:700;color:#3a4441}.notes{display:grid;grid-template-columns:repeat(3,1fr);gap:12px}.notes div{background:#fff;border:1px solid var(--line);border-radius:8px;padding:12px}.notes b{display:block;margin-bottom:6px}
footer{padding:18px 28px 40px;color:var(--muted);border-top:1px solid var(--line)}
@media(max-width:900px){header,main,footer{padding-left:16px;padding-right:16px}.callout,.mini-grid,.chart-grid.two,.notes,.metric-strip{grid-template-columns:1fr}}
</style>"""


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--experiment", required=True, type=Path)
    parser.add_argument("--out", type=Path)
    parser.add_argument("--json-out", type=Path)
    parser.add_argument("--windows", type=int, default=10)
    parser.add_argument("--spread-mode", default="conservative_max")
    args = parser.parse_args()

    data = build_trajectory_data(args.experiment, max(2, int(args.windows)), args.spread_mode)
    json_out = args.json_out or args.experiment / "metric_trajectories.json"
    json_out.write_text(json.dumps(data, indent=2, ensure_ascii=False, allow_nan=False), encoding="utf-8")
    out = args.out or args.experiment / "metric_trajectory_report.html"
    out.write_text(HtmlReport(data).build(), encoding="utf-8")
    print(out.resolve())
    print(json_out.resolve())


if __name__ == "__main__":
    main()
