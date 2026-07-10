"""Build a granular symbol/timeframe/regime atlas from optimization ledgers."""

from __future__ import annotations

import argparse
import html
import json
import math
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path


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
TIMEFRAMES = ("M5", "M15", "M30", "H1", "H4", "D1")
REGIMES = ("TREND", "RANGE", "BREAKOUT", "CHOP")


def esc(value: object) -> str:
    return html.escape(str(value))


def load_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8-sig"))


def fnum(value: object, digits: int = 2) -> str:
    if value is None:
        return "n/a"
    try:
        v = float(value)
    except (TypeError, ValueError):
        return esc(value)
    if math.isnan(v) or math.isinf(v):
        return "n/a"
    return f"{v:,.{digits}f}"


def fpct(value: object, digits: int = 2, sign: bool = False) -> str:
    if value is None:
        return "n/a"
    try:
        v = float(value)
    except (TypeError, ValueError):
        return esc(value)
    prefix = "+" if sign and v > 0 else ""
    return f"{prefix}{v:,.{digits}f}%"


def fint(value: object) -> str:
    try:
        return f"{int(round(float(value))):,}"
    except (TypeError, ValueError):
        return "n/a"


def metric(metrics: dict | None, key: str) -> object:
    if not isinstance(metrics, dict):
        return None
    return metrics.get(key)


def hist_metrics(cell: dict) -> dict:
    hist = cell.get("historical_audit_metrics")
    if not isinstance(hist, dict):
        return {}
    metrics = hist.get("metrics")
    return metrics if isinstance(metrics, dict) else {}


def is_no_trade(cell: dict) -> bool:
    if not isinstance(cell, dict):
        return True
    if (cell.get("strategy_name") or cell.get("strategy")) == "NO_TRADE":
        return True
    ve = cell.get("validation_evidence")
    return isinstance(ve, dict) and bool(ve.get("no_trade"))


def safe_float(value: object, default: float = 0.0) -> float:
    try:
        v = float(value)
    except (TypeError, ValueError):
        return default
    if math.isnan(v) or math.isinf(v):
        return default
    return v


def color_for_return(value: object, no_trade: bool = False) -> str:
    if no_trade:
        return "#ece7df"
    v = max(-30.0, min(30.0, safe_float(value)))
    if v < 0:
        t = abs(v) / 30.0
        r, g, b = 246, int(229 - 80 * t), int(224 - 90 * t)
    else:
        t = v / 30.0
        r, g, b = int(232 - 100 * t), int(243 - 50 * t), int(236 - 110 * t)
    return f"rgb({r},{g},{b})"


def color_for_density(active: int, total: int = 4) -> str:
    t = max(0.0, min(1.0, active / max(1, total)))
    r = int(239 - 130 * t)
    g = int(233 - 80 * t)
    b = int(223 - 90 * t)
    return f"rgb({r},{g},{b})"


class CellAtlas:
    def __init__(self, experiment: Path) -> None:
        self.experiment = experiment
        self.bench_dir = experiment / "benchmarks"
        self.benchmarks = {
            model: load_json(self.bench_dir / f"{model}.json")
            for model in MODELS
        }
        self.ledgers = {
            model: load_json(Path(self.benchmarks[model]["ledger"]))
            for model in MODELS
        }
        self.symbols = list(self.benchmarks["specialist_spokes_threadspark"]["symbols"])
        self.cells = self.extract_cells()

    def extract_cells(self) -> list[dict]:
        cells = []
        for model, ledger in self.ledgers.items():
            for symbol in self.symbols:
                symbol_record = ledger.get(symbol, {})
                regime_configs = symbol_record.get("regime_configs") or {}
                for timeframe in TIMEFRAMES:
                    tf_record = regime_configs.get(timeframe) or {}
                    for regime in REGIMES:
                        cell = tf_record.get(regime)
                        if not isinstance(cell, dict):
                            cell = {
                                "strategy_name": "NO_TRADE",
                                "validation_evidence": {"no_trade": True, "validation_reason": "missing cell"},
                            }
                        no_trade = is_no_trade(cell)
                        train = cell.get("train_metrics") if isinstance(cell.get("train_metrics"), dict) else {}
                        val = cell.get("val_metrics") if isinstance(cell.get("val_metrics"), dict) else {}
                        holdout = cell.get("holdout_metrics") if isinstance(cell.get("holdout_metrics"), dict) else {}
                        hist = hist_metrics(cell)
                        validation = cell.get("validation_evidence") if isinstance(cell.get("validation_evidence"), dict) else {}
                        selector = validation.get("threadspark_selector") if isinstance(validation.get("threadspark_selector"), dict) else {}
                        derived = selector.get("derived_shape_features") if isinstance(selector.get("derived_shape_features"), dict) else {}
                        cells.append(
                            {
                                "model": model,
                                "symbol": symbol,
                                "timeframe": timeframe,
                                "regime": regime,
                                "strategy": cell.get("strategy_name") or cell.get("strategy") or "NO_TRADE",
                                "no_trade": no_trade,
                                "quality_score": cell.get("quality_score"),
                                "promotion_status": cell.get("promotion_status"),
                                "promotion_reasons": cell.get("promotion_reasons") or [],
                                "train_trades": metric(train, "total_trades") or cell.get("regime_train_trades"),
                                "val_trades": metric(val, "total_trades") or cell.get("regime_val_trades"),
                                "holdout_trades": metric(holdout, "total_trades"),
                                "hist_trades": metric(hist, "total_trades"),
                                "train_return": metric(train, "total_return_pct"),
                                "val_return": metric(val, "total_return_pct") or validation.get("val_return_pct"),
                                "holdout_return": metric(holdout, "total_return_pct"),
                                "hist_return": metric(hist, "total_return_pct"),
                                "train_pf": metric(train, "profit_factor"),
                                "val_pf": metric(val, "profit_factor") or validation.get("val_profit_factor"),
                                "holdout_pf": metric(holdout, "profit_factor"),
                                "hist_pf": metric(hist, "profit_factor"),
                                "hist_dd": metric(hist, "max_drawdown_pct"),
                                "hist_severe": bool((cell.get("historical_audit_metrics") or {}).get("severe_failure")),
                                "regime_severe": bool(((cell.get("historical_audit_metrics") or {}).get("regime_profitability_audit") or {}).get("severe_failure")),
                                "selection_score": validation.get("selection_score_raw"),
                                "threadspark_score": selector.get("score"),
                                "final_face_quality": derived.get("final_face_quality"),
                                "path_average_quality": derived.get("path_average_quality"),
                                "dent_health": derived.get("dent_health"),
                                "evidence_confidence": derived.get("evidence_confidence"),
                                "validation_reason": validation.get("validation_reason", ""),
                            }
                        )
        return cells

    def active_cells(self, model: str | None = None) -> list[dict]:
        return [
            cell for cell in self.cells
            if not cell["no_trade"] and (model is None or cell["model"] == model)
        ]

    def model_summary_cards(self) -> str:
        parts = []
        for model in MODELS:
            cells = [cell for cell in self.cells if cell["model"] == model]
            active = [cell for cell in cells if not cell["no_trade"]]
            severe = sum(1 for cell in active if cell["hist_severe"] or cell["regime_severe"])
            promoted = sum(1 for cell in active if str(cell["promotion_status"]).upper().startswith("PROMOTE"))
            holdout_pos = sum(1 for cell in active if safe_float(cell["holdout_return"], -999) > 0)
            hist_pos = sum(1 for cell in active if safe_float(cell["hist_return"], -999) > 0)
            parts.append(
                f'<div class="summary-card" style="border-top-color:{MODEL_COLOR[model]}">'
                f'<h3>{MODEL_LABEL[model]}</h3>'
                f'<div class="big">{len(active)} / {len(cells)}</div><p>active regime cells</p>'
                f'<dl><dt>Promote-like</dt><dd>{promoted}</dd>'
                f'<dt>Holdout positive</dt><dd>{holdout_pos}</dd>'
                f'<dt>Historical positive</dt><dd>{hist_pos}</dd>'
                f'<dt>Severe audit flags</dt><dd>{severe}</dd></dl></div>'
            )
        return "".join(parts)

    def coverage_svg(self) -> str:
        tile_w, tile_h = 54, 28
        left, top = 86, 54
        model_gap = 26
        width = left + len(TIMEFRAMES) * tile_w + 30
        panel_h = top + len(self.symbols) * tile_h + 34
        height = len(MODELS) * panel_h + model_gap * (len(MODELS) - 1)
        parts = [
            f'<svg class="chart" viewBox="0 0 {width} {height}" role="img" aria-label="Coverage matrix">',
            '<text class="svg-title" x="18" y="24">Active regime-cell coverage</text>',
            '<text class="svg-subtitle" x="18" y="42">Each tile is a symbol/timeframe; label is active regimes out of 4.</text>',
        ]
        y_offset = 0
        for model in MODELS:
            parts.append(f'<text class="model-title" x="18" y="{y_offset+top-16}" fill="{MODEL_COLOR[model]}">{MODEL_LABEL[model]}</text>')
            for col, timeframe in enumerate(TIMEFRAMES):
                x = left + col * tile_w
                parts.append(f'<text class="tick" x="{x+tile_w/2}" y="{y_offset+top-5}" text-anchor="middle">{timeframe}</text>')
            for row, symbol in enumerate(self.symbols):
                y = y_offset + top + row * tile_h
                parts.append(f'<text class="axis-label" x="18" y="{y+18}">{symbol}</text>')
                for col, timeframe in enumerate(TIMEFRAMES):
                    x = left + col * tile_w
                    active = 0
                    strategies = []
                    for cell in self.cells:
                        if cell["model"] == model and cell["symbol"] == symbol and cell["timeframe"] == timeframe and not cell["no_trade"]:
                            active += 1
                            strategies.append(cell["strategy"].replace("Strategy", ""))
                    parts.append(
                        f'<rect x="{x}" y="{y}" width="{tile_w-4}" height="{tile_h-4}" rx="4" fill="{color_for_density(active)}" stroke="#d4cdc3"/>'
                        f'<text class="tile-label" x="{x+(tile_w-4)/2}" y="{y+16}" text-anchor="middle">{active}/4</text>'
                    )
                    if strategies:
                        parts.append(f'<title>{model} {symbol} {timeframe}: {", ".join(strategies)}</title>')
            y_offset += panel_h + model_gap
        parts.append("</svg>")
        return "".join(parts)

    def strategy_frequency_svg(self) -> str:
        counters = {model: Counter(cell["strategy"] for cell in self.active_cells(model)) for model in MODELS}
        top = []
        seen = set()
        for model in MODELS:
            for strat, _count in counters[model].most_common(8):
                if strat not in seen:
                    seen.add(strat)
                    top.append(strat)
        top = top[:14]
        max_count = max([counters[model][s] for model in MODELS for s in top] + [1])
        width = 1120
        left, top_y, row_h = 270, 54, 31
        height = top_y + len(top) * row_h + 44
        plot_w = width - left - 40
        parts = [
            f'<svg class="chart" viewBox="0 0 {width} {height}" role="img" aria-label="Strategy frequency">',
            f'<text class="svg-title" x="{left}" y="24">Strategy concentration across active cells</text>',
            f'<text class="svg-subtitle" x="{left}" y="42">Counts are selected winners per symbol/timeframe/regime cell, not benchmark default symbols.</text>',
        ]
        for i, strat in enumerate(top):
            y = top_y + i * row_h
            parts.append(f'<text class="axis-label" x="12" y="{y+17}">{esc(strat.replace("Strategy",""))[:34]}</text>')
            for j, model in enumerate(MODELS):
                count = counters[model][strat]
                bar_w = count / max_count * plot_w
                yy = y + j * 8
                parts.append(f'<rect x="{left}" y="{yy}" width="{bar_w:.1f}" height="6" rx="2" fill="{MODEL_COLOR[model]}"/>')
                if count:
                    parts.append(f'<text class="bar-label" x="{left+bar_w+5:.1f}" y="{yy+6}">{count}</text>')
        lx, ly = left, height - 16
        for model in MODELS:
            parts.append(f'<rect x="{lx}" y="{ly-9}" width="12" height="8" rx="2" fill="{MODEL_COLOR[model]}"/><text class="legend" x="{lx+18}" y="{ly}">{MODEL_SHORT[model]}</text>')
            lx += 120
        parts.append("</svg>")
        return "".join(parts)

    def cell_metric_heatmap(self, model: str, metric_name: str, title: str) -> str:
        active = self.active_cells(model)
        values = [safe_float(cell[metric_name]) for cell in active if cell[metric_name] is not None]
        width = 1120
        tile_w, tile_h = 42, 24
        left, top = 86, 58
        group_w = len(TIMEFRAMES) * tile_w + 22
        height = top + len(self.symbols) * tile_h + 40
        parts = [
            f'<svg class="chart" viewBox="0 0 {width} {height}" role="img" aria-label="{esc(title)}">',
            f'<text class="svg-title" x="18" y="24">{esc(title)}</text>',
            f'<text class="svg-subtitle" x="18" y="42">Cells are timeframe/regime winners; gray means NO_TRADE.</text>',
        ]
        x_base = left
        for regime in REGIMES:
            parts.append(f'<text class="model-title" x="{x_base}" y="{top-18}">{regime}</text>')
            for col, timeframe in enumerate(TIMEFRAMES):
                x = x_base + col * tile_w
                parts.append(f'<text class="tick" x="{x+tile_w/2-2}" y="{top-5}" text-anchor="middle">{timeframe}</text>')
            x_base += group_w
        for row, symbol in enumerate(self.symbols):
            y = top + row * tile_h
            parts.append(f'<text class="axis-label" x="18" y="{y+16}">{symbol}</text>')
            x_base = left
            for regime in REGIMES:
                for col, timeframe in enumerate(TIMEFRAMES):
                    x = x_base + col * tile_w
                    cell = next(
                        c for c in self.cells
                        if c["model"] == model and c["symbol"] == symbol and c["timeframe"] == timeframe and c["regime"] == regime
                    )
                    val = cell.get(metric_name)
                    fill = color_for_return(val, cell["no_trade"])
                    label = "NT" if cell["no_trade"] else f"{safe_float(val):.0f}"
                    parts.append(f'<rect x="{x}" y="{y}" width="{tile_w-4}" height="{tile_h-4}" rx="3" fill="{fill}" stroke="#d2cbc2"/>')
                    parts.append(f'<text class="tile-label" x="{x+(tile_w-4)/2}" y="{y+14}" text-anchor="middle">{esc(label)}</text>')
                    parts.append(
                        f'<title>{model} {symbol} {timeframe} {regime}: {cell["strategy"]}; {metric_name}={val}</title>'
                    )
                x_base += group_w
        parts.append("</svg>")
        return "".join(parts)

    def status_table(self) -> str:
        rows = ""
        for model in MODELS:
            active = self.active_cells(model)
            statuses = Counter(str(cell["promotion_status"] or "n/a") for cell in active)
            severe = sum(1 for cell in active if cell["hist_severe"] or cell["regime_severe"])
            no_trade = sum(1 for cell in self.cells if cell["model"] == model and cell["no_trade"])
            rows += (
                f'<tr><td><span class="swatch" style="background:{MODEL_COLOR[model]}"></span>{MODEL_LABEL[model]}</td>'
                f"<td>{len(active)}</td><td>{no_trade}</td><td>{severe}</td>"
                f"<td>{esc(', '.join(f'{k}: {v}' for k, v in statuses.most_common()))}</td></tr>"
            )
        return rows

    def full_cell_rows(self) -> str:
        rows = []
        for cell in self.cells:
            no_trade_class = " no-trade" if cell["no_trade"] else ""
            ret = cell["hist_return"] if cell["hist_return"] is not None else cell["holdout_return"]
            ret_class = "pos" if safe_float(ret, -1) >= 0 and not cell["no_trade"] else "neg"
            severe = "YES" if cell["hist_severe"] or cell["regime_severe"] else ""
            reasons = "; ".join(map(str, cell["promotion_reasons"]))[:220]
            rows.append(
                f'<tr class="{no_trade_class}"><td><span class="swatch" style="background:{MODEL_COLOR[cell["model"]]}"></span>{MODEL_SHORT[cell["model"]]}</td>'
                f"<td>{cell['symbol']}</td><td>{cell['timeframe']}</td><td>{cell['regime']}</td>"
                f"<td>{esc(cell['strategy'])}</td><td>{esc(cell['promotion_status'])}</td><td>{esc(severe)}</td>"
                f'<td class="num">{fint(cell["train_trades"])}</td><td class="num">{fint(cell["val_trades"])}</td><td class="num">{fint(cell["holdout_trades"])}</td><td class="num">{fint(cell["hist_trades"])}</td>'
                f'<td class="num">{fpct(cell["val_return"],1,True)}</td><td class="num">{fpct(cell["holdout_return"],1,True)}</td><td class="num {ret_class}">{fpct(cell["hist_return"],1,True)}</td>'
                f'<td class="num">{fnum(cell["val_pf"],2)}</td><td class="num">{fnum(cell["holdout_pf"],2)}</td><td class="num">{fnum(cell["hist_pf"],2)}</td>'
                f'<td class="num">{fnum(cell["selection_score"],3)}</td><td>{esc(reasons)}</td></tr>'
            )
        return "".join(rows)

    def top_threadspark_table(self) -> str:
        cells = sorted(self.active_cells("specialist_spokes_threadspark"), key=lambda c: safe_float(c["threadspark_score"]), reverse=True)
        rows = ""
        for cell in cells[:30]:
            rows += (
                f"<tr><td>{cell['symbol']}</td><td>{cell['timeframe']}</td><td>{cell['regime']}</td>"
                f"<td>{esc(cell['strategy'])}</td><td class=\"num\">{fnum(cell['threadspark_score'],3)}</td>"
                f"<td class=\"num\">{fnum(cell['final_face_quality'],3)}</td><td class=\"num\">{fnum(cell['path_average_quality'],3)}</td>"
                f"<td class=\"num\">{fnum(cell['dent_health'],3)}</td><td class=\"num\">{fnum(cell['evidence_confidence'],3)}</td>"
                f"<td class=\"num\">{fpct(cell['holdout_return'],1,True)}</td><td class=\"num\">{fpct(cell['hist_return'],1,True)}</td>"
                f"<td class=\"num\">{fint(cell['hist_trades'])}</td><td>{'YES' if cell['hist_severe'] or cell['regime_severe'] else ''}</td></tr>"
            )
        return rows

    def build(self) -> str:
        generated = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        return f"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Strategy Cell Atlas</title>
{self.styles()}
</head>
<body>
<header><div class="report">
<h1>Strategy Cell Atlas</h1>
<p class="subhead">Granular view of the stored symbol × timeframe × regime selection surface for Legacy, Old Spokes, and ThreadSpark v1.1. This is the layer hidden by a symbol-level benchmark report.</p>
<div class="meta"><span>Generated: {esc(generated)}</span><span>Cells: {len(self.cells)}</span><span>Symbols: {len(self.symbols)}</span><span>Timeframes: {len(TIMEFRAMES)}</span><span>Regimes: {len(REGIMES)}</span></div>
</div></header>
<main><div class="report">
<section class="panel callout"><div><h2>What this report shows</h2><p class="lead">This report exposes every stored regime-cell winner or NO_TRADE decision in the ledgers: 144 cells per arm, 432 cells total. It does not claim to show every rejected Optuna trial, because those full candidate pools are not preserved in the benchmark ledgers.</p></div><div class="warning"><b>Important</b><p>The benchmark default winner is only the final deployable symbol choice. The strategy-cell surface below is where the scoring model forms its preferences across timeframes and regimes.</p></div></section>
<section class="summary-grid">{self.model_summary_cards()}</section>
<section class="panel"><h2>Cell Status Summary</h2><div class="table-wrap"><table><thead><tr><th>Model</th><th>Active Cells</th><th>NO_TRADE Cells</th><th>Severe Audit Flags</th><th>Promotion Status Counts</th></tr></thead><tbody>{self.status_table()}</tbody></table></div></section>
<section class="panel chart-grid">{self.coverage_svg()}{self.strategy_frequency_svg()}</section>
<section class="panel chart-grid"><h2>Per-Cell Historical Return Heatmaps</h2><p class="lead">Each tile is one symbol/timeframe/regime cell. Green means positive historical replay return, red means negative, gray means NO_TRADE. This is a broad surface view, not a symbol-default aggregate.</p>{''.join(self.cell_metric_heatmap(model, 'hist_return', MODEL_LABEL[model] + ' historical return by cell') for model in MODELS)}</section>
<section class="panel"><h2>Top ThreadSpark Cells by Tensor Score</h2><p class="lead">These are the highest-scoring ThreadSpark regime cells, not just the 6 symbol defaults. This table is meant to reveal whether the tensor score is concentrating in high-frequency or fragile pockets.</p><div class="table-wrap"><table><thead><tr><th>Symbol</th><th>TF</th><th>Regime</th><th>Strategy</th><th>TS Score</th><th>Final</th><th>Path Avg</th><th>Dent</th><th>Confidence</th><th>Holdout Ret</th><th>Hist Ret</th><th>Hist Trades</th><th>Severe</th></tr></thead><tbody>{self.top_threadspark_table()}</tbody></table></div></section>
<section class="panel"><h2>All Stored Cells</h2><p class="lead">This table includes active winners and NO_TRADE cells across all three arms. It is intentionally large because the purpose is to stop hiding the cell layer.</p><div class="table-wrap tall"><table><thead><tr><th>Model</th><th>Symbol</th><th>TF</th><th>Regime</th><th>Strategy</th><th>Status</th><th>Severe</th><th>Train Tr</th><th>Val Tr</th><th>Holdout Tr</th><th>Hist Tr</th><th>Val Ret</th><th>Holdout Ret</th><th>Hist Ret</th><th>Val PF</th><th>Holdout PF</th><th>Hist PF</th><th>Score</th><th>Reasons</th></tr></thead><tbody>{self.full_cell_rows()}</tbody></table></div></section>
<section class="panel"><h2>What is still not shown</h2><div class="notes"><div><b>Rejected trial candidates</b><p>The ledgers do not preserve every Optuna candidate rejected inside a cell. To audit the entire optimizer candidate pool, the next run must export top-N candidate snapshots per cell.</p></div><div><b>Candidate rank path</b><p>This atlas shows the selected cell winners and their audits. It does not yet show the rank ordering among runner-up strategies inside each cell.</p></div><div><b>Why this matters</b><p>If the next question is whether ThreadSpark's idea is flawed or only the current implementation is flawed, the missing object is the per-cell top-N candidate pool with tensor components and benchmark replay.</p></div></div></section>
</div></main>
<footer><div class="report">Source ledgers are read from benchmark JSON ledger paths in <code>{esc(self.bench_dir)}</code>.</div></footer>
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
.summary-grid{display:grid;grid-template-columns:repeat(3,1fr);gap:12px;margin:18px 0}.summary-card{background:#fff;border:1px solid var(--line);border-top:5px solid;border-radius:8px;padding:14px}.summary-card h3{margin:0}.summary-card .big{font-size:30px;font-weight:800;margin:8px 0 0}.summary-card p{margin:0 0 10px;color:var(--muted)}dl{display:grid;grid-template-columns:1fr auto;gap:5px;margin:0}dt{color:var(--muted)}dd{margin:0;font-weight:700}
.table-wrap{overflow:auto;border:1px solid var(--line);border-radius:8px}.table-wrap.tall{max-height:760px}table{width:100%;min-width:1000px;border-collapse:collapse;background:#fff}th,td{padding:8px 9px;border-bottom:1px solid #ece6dd;text-align:left;vertical-align:top}th{position:sticky;top:0;background:#f6f0e8;font-size:12px;text-transform:uppercase;letter-spacing:.04em;color:#4d5653;z-index:1}td.num{text-align:right;font-variant-numeric:tabular-nums}.no-trade{color:#8a8781;background:#faf8f4}.pos{color:#217348;font-weight:700}.neg{color:#ae332d;font-weight:700}.swatch{display:inline-block;width:10px;height:10px;border-radius:2px;margin-right:7px;vertical-align:-1px}
.chart-grid{display:grid;grid-template-columns:1fr;gap:16px}.chart{width:100%;height:auto;background:#fff;border:1px solid var(--line);border-radius:8px}.svg-title{font-size:18px;font-weight:700;fill:#222}.svg-subtitle{font-size:12px;fill:#68716e}.axis-label{font-size:12px;font-weight:700;fill:#333}.model-title{font-size:14px;font-weight:800;fill:#333}.tick,.tile-label,.bar-label,.legend{font-size:10px;fill:#38413e}.tile-label{font-weight:700}.notes{display:grid;grid-template-columns:repeat(3,1fr);gap:12px}.notes div{background:#fff;border:1px solid var(--line);border-radius:8px;padding:12px}.notes b{display:block;margin-bottom:6px}
footer{padding:18px 28px 40px;color:var(--muted);border-top:1px solid var(--line)}
@media(max-width:900px){header,main,footer{padding-left:16px;padding-right:16px}.callout,.summary-grid,.notes{grid-template-columns:1fr}}
</style>"""


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--experiment", required=True, type=Path)
    parser.add_argument("--out", type=Path)
    args = parser.parse_args()
    atlas = CellAtlas(args.experiment)
    out = args.out or args.experiment / "strategy_cell_atlas_report.html"
    out.write_text(atlas.build(), encoding="utf-8")
    print(out.resolve())
    print(f"bytes={out.stat().st_size}")


if __name__ == "__main__":
    main()
