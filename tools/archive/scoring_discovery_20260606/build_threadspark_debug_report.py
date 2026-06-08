"""Build a self-contained ThreadSpark debug HTML report from benchmark artifacts."""

from __future__ import annotations

import argparse
import html
import json
import math
from datetime import datetime
from pathlib import Path


MODEL_LABEL = {
    "legacy": "Legacy",
    "old_spokes": "Old Spokes",
    "threadspark_v11": "ThreadSpark v1.1",
}

MODEL_COLOR = {
    "legacy": "#2f6f9f",
    "old_spokes": "#b87916",
    "threadspark_v11": "#b4443c",
}

SPOKE_LABELS = {
    "edge_survival": "edge",
    "profit_structure_quality": "profit structure",
    "capital_preservation": "capital preservation",
    "tail_risk_control": "tail risk",
    "path_persistence": "path persistence",
    "generalization_survival": "generalization",
    "execution_cost_survival": "cost survival",
    "parameter_stability": "param stability",
}


def load_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8-sig"))


def esc(value: object) -> str:
    return html.escape(str(value))


def num(value: object, ndigits: int = 2) -> str:
    if value is None:
        return "n/a"
    try:
        v = float(value)
    except (TypeError, ValueError):
        return esc(value)
    if math.isnan(v) or math.isinf(v):
        return "n/a"
    return f"{v:,.{ndigits}f}"


def pct(value: object, ndigits: int = 2, sign: bool = False) -> str:
    if value is None:
        return "n/a"
    try:
        v = float(value)
    except (TypeError, ValueError):
        return esc(value)
    s = "+" if sign and v > 0 else ""
    return f"{s}{v:,.{ndigits}f}%"


def whole(value: object) -> str:
    try:
        return f"{int(round(float(value))):,}"
    except (TypeError, ValueError):
        return "n/a"


def cell_color(value: object) -> str:
    try:
        v = max(0.0, min(1.0, float(value)))
    except (TypeError, ValueError):
        v = 0.0
    if v < 0.5:
        t = v / 0.5
        r = 190
        g = int(68 + (144 - 68) * t)
        b = int(61 + (49 - 61) * t)
    else:
        t = (v - 0.5) / 0.5
        r = int(190 + (42 - 190) * t)
        g = int(144 + (134 - 144) * t)
        b = int(49 + (86 - 49) * t)
    return f"rgb({r},{g},{b})"


def badge(ok: bool) -> str:
    cls = "pass" if ok else "fail"
    text = "PASS" if ok else "FAIL"
    return f'<span class="badge {cls}">{text}</span>'


class ReportBuilder:
    def __init__(self, experiment: Path, previous_experiment: Path | None) -> None:
        self.exp = experiment
        self.prev = previous_experiment
        self.bench = experiment / "benchmarks"
        self.ledger_path = experiment / "specialist_spokes_threadspark" / "pm_configs.json"
        self.data = {
            "legacy": load_json(self.bench / "legacy.json"),
            "old_spokes": load_json(self.bench / "specialist_spokes.json"),
            "threadspark_v11": load_json(self.bench / "specialist_spokes_threadspark.json"),
        }
        self.ledger = load_json(self.ledger_path)
        self.symbols = list(self.data["threadspark_v11"]["symbols"])
        self.rows = {
            model: {row["symbol"]: row for row in payload["rows"]}
            for model, payload in self.data.items()
        }
        self.summary = {model: payload["summary"] for model, payload in self.data.items()}
        self.prev_ts = None
        if previous_experiment:
            prev_file = previous_experiment / "benchmarks" / "specialist_spokes_threadspark.json"
            if prev_file.exists():
                self.prev_ts = load_json(prev_file)

    def grouped_bars(self, metric: str, title: str, subtitle: str, fmt: str = "pct") -> str:
        values = [
            float(self.rows[model][symbol][metric])
            for symbol in self.symbols
            for model in ("legacy", "old_spokes", "threadspark_v11")
        ]
        mn = min(values + [0.0])
        mx = max(values + [0.0])
        pad = (mx - mn) * 0.08 or 1.0
        mn -= pad
        mx += pad
        width = 1120
        left = 140
        right = 30
        top = 52
        row_h = 30
        group_gap = 14
        model_h = 9
        height = top + len(self.symbols) * (row_h * 3 + group_gap) + 40
        plot_w = width - left - right

        def x_coord(value: float) -> float:
            return left + (value - mn) / (mx - mn) * plot_w

        zero_x = x_coord(0.0)
        parts = [
            f'<svg class="chart" viewBox="0 0 {width} {height}" role="img" aria-label="{esc(title)}">',
            f'<text class="svg-title" x="{left}" y="24">{esc(title)}</text>',
            f'<text class="svg-subtitle" x="{left}" y="42">{esc(subtitle)}</text>',
            f'<line x1="{zero_x:.1f}" y1="48" x2="{zero_x:.1f}" y2="{height-26}" '
            'stroke="#535353" stroke-width="1" stroke-dasharray="3 5"/>',
        ]
        y = top
        for symbol in self.symbols:
            parts.append(f'<text class="axis-label" x="16" y="{y+27}">{esc(symbol)}</text>')
            for idx, model in enumerate(("legacy", "old_spokes", "threadspark_v11")):
                value = float(self.rows[model][symbol][metric])
                yy = y + idx * row_h
                x0 = min(x_coord(0.0), x_coord(value))
                width_bar = abs(x_coord(value) - x_coord(0.0))
                label = pct(value, 1, True) if fmt == "pct" else (whole(value) if fmt == "int" else num(value, 2))
                lx = x_coord(value) + (6 if value >= 0 else -6)
                anchor = "start" if value >= 0 else "end"
                parts.append(
                    f'<rect x="{x0:.1f}" y="{yy+4}" width="{max(width_bar,1):.1f}" '
                    f'height="{model_h}" rx="2" fill="{MODEL_COLOR[model]}"/>'
                )
                parts.append(
                    f'<text class="bar-label" x="{lx:.1f}" y="{yy+12}" '
                    f'text-anchor="{anchor}">{esc(label)}</text>'
                )
            y += row_h * 3 + group_gap
        lx = left
        ly = height - 16
        for model in ("legacy", "old_spokes", "threadspark_v11"):
            parts.append(
                f'<rect x="{lx}" y="{ly-9}" width="12" height="8" rx="2" fill="{MODEL_COLOR[model]}"/>'
                f'<text class="legend" x="{lx+18}" y="{ly}">{MODEL_LABEL[model]}</text>'
            )
            lx += 160
        parts.append("</svg>")
        return "".join(parts)

    def scatter(self) -> str:
        all_rows = []
        for model in ("legacy", "old_spokes", "threadspark_v11"):
            for symbol, row in self.rows[model].items():
                all_rows.append((model, symbol, row))
        xs = [math.log10(max(1, row["trades"])) for _, _, row in all_rows]
        ys = [row["return_pct"] for _, _, row in all_rows]
        minx, maxx = min(xs), max(xs)
        miny, maxy = min(min(ys), -60), max(max(ys), 500)
        width, height = 900, 520
        left, right, top, bottom = 70, 30, 52, 64
        plot_w = width - left - right
        plot_h = height - top - bottom

        def x_coord(trades: float) -> float:
            return left + (math.log10(max(1, trades)) - minx) / (maxx - minx) * plot_w

        def y_coord(value: float) -> float:
            return top + (maxy - value) / (maxy - miny) * plot_h

        parts = [
            f'<svg class="chart" viewBox="0 0 {width} {height}" role="img" aria-label="Return versus trade count scatter">',
            f'<text class="svg-title" x="{left}" y="24">Return vs trade load</text>',
            f'<text class="svg-subtitle" x="{left}" y="42">X-axis is log-scaled trades; circle size reflects max drawdown.</text>',
            f'<line x1="{left}" y1="{y_coord(0):.1f}" x2="{width-right}" y2="{y_coord(0):.1f}" '
            'stroke="#6b6b6b" stroke-dasharray="3 5"/>',
            f'<line x1="{left}" y1="{top}" x2="{left}" y2="{height-bottom}" stroke="#555"/>',
            f'<line x1="{left}" y1="{height-bottom}" x2="{width-right}" y2="{height-bottom}" stroke="#555"/>',
        ]
        for trades in (100, 250, 500, 1000, 2500):
            if minx <= math.log10(trades) <= maxx:
                x = x_coord(trades)
                parts.append(
                    f'<line x1="{x:.1f}" y1="{height-bottom}" x2="{x:.1f}" y2="{height-bottom+5}" stroke="#555"/>'
                    f'<text class="tick" x="{x:.1f}" y="{height-bottom+22}" text-anchor="middle">{trades}</text>'
                )
        for value in (-50, 0, 100, 250, 500):
            if miny <= value <= maxy:
                y = y_coord(value)
                parts.append(
                    f'<line x1="{left-5}" y1="{y:.1f}" x2="{left}" y2="{y:.1f}" stroke="#555"/>'
                    f'<text class="tick" x="{left-10}" y="{y+4:.1f}" text-anchor="end">{value}%</text>'
                )
        for model, symbol, row in all_rows:
            x = x_coord(row["trades"])
            y = y_coord(row["return_pct"])
            radius = 4 + min(15, max(0.0, row["max_dd_pct"]) / 5)
            parts.append(
                f'<circle cx="{x:.1f}" cy="{y:.1f}" r="{radius:.1f}" fill="{MODEL_COLOR[model]}" '
                'fill-opacity="0.72" stroke="#111" stroke-opacity="0.25"/>'
            )
            if model == "threadspark_v11":
                parts.append(f'<text class="point-label" x="{x+radius+3:.1f}" y="{y+3:.1f}">{esc(symbol)}</text>')
        parts.append(f'<text class="axis-title" x="{left+plot_w/2:.1f}" y="{height-16}" text-anchor="middle">Trades (log scale)</text>')
        parts.append(f'<text class="axis-title" transform="translate(18 {top+plot_h/2:.1f}) rotate(-90)" text-anchor="middle">Return %</text>')
        lx, ly = left, height - 38
        for model in ("legacy", "old_spokes", "threadspark_v11"):
            parts.append(f'<circle cx="{lx}" cy="{ly}" r="5" fill="{MODEL_COLOR[model]}"/>')
            parts.append(f'<text class="legend" x="{lx+12}" y="{ly+4}">{MODEL_LABEL[model]}</text>')
            lx += 150
        parts.append("</svg>")
        return "".join(parts)

    def tensor_card(self, symbol: str) -> str:
        selector = self.ledger.get(symbol, {}).get("validation_evidence", {}).get("threadspark_selector", {})
        steps = selector.get("evidence_steps", []) if isinstance(selector, dict) else []
        features = selector.get("derived_shape_features", {}) if isinstance(selector, dict) else {}
        spokes = list(SPOKE_LABELS.keys())
        cell = 18
        row_h = 24
        left = 132
        top = 36
        width = left + len(spokes) * cell + 20
        height = top + len(steps) * row_h + 28
        ledger_row = self.ledger.get(symbol, {})
        parts = [
            '<div class="tensor-card">',
            '<div class="tensor-head">',
            f'<b>{esc(symbol)}</b>',
            f'<span>{esc(ledger_row.get("strategy_name", ""))} / {esc(ledger_row.get("timeframe", ""))}</span>',
            '</div><div class="feature-strip">',
        ]
        for key, label in (
            ("final_face_quality", "final"),
            ("path_average_quality", "path avg"),
            ("cross_axis_balance", "balance"),
            ("dent_health", "dent"),
            ("evidence_confidence", "confidence"),
        ):
            parts.append(f'<span><b>{esc(label)}</b>{num(features.get(key), 3)}</span>')
        parts.append("</div>")
        parts.append(f'<svg class="tensor" viewBox="0 0 {width} {height}" role="img" aria-label="ThreadSpark tensor for {symbol}">')
        for col, _spoke in enumerate(spokes):
            x = left + col * cell + cell / 2
            parts.append(f'<text class="tensor-col" x="{x:.1f}" y="18" text-anchor="middle">{col+1}</text>')
        y = top
        for step in steps:
            surface = step.get("surface", "?")
            quality = step.get("quality", 0.0)
            label = (
                surface.replace("historical_cost_chunk_", "chunk ")
                .replace("historical_cost", "full history")
                .replace("clean_holdout", "holdout")
            )
            parts.append(f'<text class="tensor-row" x="4" y="{y+14}">{esc(label[:22])}</text>')
            qw = max(1.0, min(90.0, float(quality or 0.0) * 90.0))
            parts.append(f'<rect x="80" y="{y+5}" width="90" height="7" rx="2" fill="#e7e0d6"/>')
            parts.append(f'<rect x="80" y="{y+5}" width="{qw:.1f}" height="7" rx="2" fill="{cell_color(quality)}"/>')
            spoke_values = step.get("spokes", {})
            for col, spoke in enumerate(spokes):
                value = spoke_values.get(spoke, 0.0)
                x = left + col * cell
                parts.append(
                    f'<rect x="{x}" y="{y}" width="{cell-2}" height="{cell-2}" rx="2" '
                    f'fill="{cell_color(value)}"><title>{esc(SPOKE_LABELS[spoke])}: {num(value,3)}</title></rect>'
                )
            y += row_h
        parts.append("</svg>")
        parts.append('<div class="spoke-key">')
        for idx, label in enumerate(SPOKE_LABELS.values()):
            parts.append(f'<span>{idx+1} {esc(label)}</span>')
        parts.append("</div></div>")
        return "".join(parts)

    def metric_cards(self) -> str:
        metrics = (
            ("Mean return", "mean_return_pct", "pct", "Higher is better; distorted by XAUUSD."),
            ("Median return", "median_return_pct", "pct", "Robust central outcome."),
            ("Median PF", "median_profit_factor", "num", "Profit structure floor was 1.15."),
            ("Median max-DD", "median_max_dd_pct", "pct", "Lower is better."),
            ("Median ulcer", "median_ulcer_index", "num", "Lower is better; captures persistent pain."),
            ("Total trades", "total_trades", "int", "Churn proxy."),
        )
        cards = []
        for title, key, kind, desc in metrics:
            cards.append('<div class="metric-card">')
            cards.append(f"<h4>{esc(title)}</h4><div class=\"metric-grid\">")
            for model in ("legacy", "old_spokes", "threadspark_v11"):
                value = self.summary[model][key]
                text = pct(value, 2, True) if kind == "pct" else (whole(value) if kind == "int" else num(value, 2))
                cards.append(
                    f'<div><span style="background:{MODEL_COLOR[model]}"></span>'
                    f'<b>{esc(text)}</b><em>{MODEL_LABEL[model]}</em></div>'
                )
            cards.append(f"</div><p>{esc(desc)}</p></div>")
        return "".join(cards)

    def hard_gates(self) -> list[tuple[str, bool, str, str]]:
        ts_rows = self.data["threadspark_v11"]["rows"]
        ts_sum = self.summary["threadspark_v11"]
        legacy_sum = self.summary["legacy"]
        worst = min(row["return_pct"] for row in ts_rows)
        legacy_worst = min(row["return_pct"] for row in self.data["legacy"]["rows"])
        profitable = sum(1 for row in ts_rows if row["return_pct"] > 0)
        ratios = {
            symbol: self.rows["threadspark_v11"][symbol]["trades"] / max(1, self.rows["legacy"][symbol]["trades"])
            for symbol in self.symbols
        }
        ratio_text = ", ".join(f"{s} {v:.1f}x" for s, v in ratios.items() if v > 5) or "none"
        gbp_trades = self.rows["threadspark_v11"].get("GBPUSD", {}).get("trades", 0)
        return [
            ("No blowup below -40%", worst >= -40, pct(worst, 2, True), "Worst symbol must stay above -40%."),
            ("Worst >= legacy worst", worst >= legacy_worst, f"{pct(worst,2,True)} vs legacy {pct(legacy_worst,2,True)}", "Do not take a worse left-tail than the current default."),
            ("Median return >= +10%", ts_sum["median_return_pct"] >= 10, pct(ts_sum["median_return_pct"], 2, True), "Headline return must survive the median, not only the mean."),
            ("At least 5/6 profitable", profitable >= 5, f"{profitable}/6", "Breadth must carry across symbols."),
            ("Median PF >= 1.15", ts_sum["median_profit_factor"] >= 1.15, num(ts_sum["median_profit_factor"], 2), "Profit structure must clear the robust floor."),
            ("Trades <= 1.5x legacy", ts_sum["total_trades"] <= legacy_sum["total_trades"] * 1.5, f"{whole(ts_sum['total_trades'])} vs cap {whole(legacy_sum['total_trades']*1.5)}", "Churn must stay comparable to baseline."),
            ("No symbol >5x legacy trades", all(v <= 5 for v in ratios.values()), ratio_text, "Per-symbol churn cannot hide inside the aggregate."),
            ("GBPUSD not >500 trades", gbp_trades <= 500, whole(gbp_trades), "The known GBPUSD churn failure must be gone."),
        ]

    def build(self) -> str:
        summary_rows = ""
        for model in ("legacy", "old_spokes", "threadspark_v11"):
            s = self.summary[model]
            summary_rows += (
                f'<tr><td><span class="swatch" style="background:{MODEL_COLOR[model]}"></span>{MODEL_LABEL[model]}</td>'
                f"<td>{pct(s['mean_return_pct'],2,True)}</td><td>{pct(s['median_return_pct'],2,True)}</td>"
                f"<td>{num(s['median_profit_factor'],2)}</td><td>{pct(s['median_max_dd_pct'],2)}</td>"
                f"<td>{num(s['median_ulcer_index'],2)}</td><td>{pct(s['median_time_underwater_pct'],2)}</td>"
                f"<td>{whole(s['total_trades'])}</td></tr>"
            )

        gate_rows = ""
        for name, ok, value, note in self.hard_gates():
            gate_rows += f"<tr><td>{badge(ok)}</td><td>{esc(name)}</td><td>{esc(value)}</td><td>{esc(note)}</td></tr>"

        symbol_rows = ""
        for symbol in self.symbols:
            symbol_rows += f'<tr class="section-row"><th colspan="12">{symbol}</th></tr>'
            for model in ("legacy", "old_spokes", "threadspark_v11"):
                row = self.rows[model][symbol]
                ret_class = "pos" if row["return_pct"] >= 0 else "neg"
                pf_class = "pos" if row["profit_factor"] >= 1 else "neg"
                churn = (
                    row["trades"] / max(1, self.rows["legacy"][symbol]["trades"])
                    if model == "threadspark_v11"
                    else 1.0
                )
                symbol_rows += (
                    f'<tr><td><span class="swatch" style="background:{MODEL_COLOR[model]}"></span>{MODEL_LABEL[model]}</td>'
                    f"<td>{esc(row['strategy'])}</td><td>{esc(row['tf'])}</td>"
                    f'<td class="num {ret_class}">{pct(row["return_pct"],2,True)}</td>'
                    f'<td class="num {pf_class}">{num(row["profit_factor"],2)}</td>'
                    f'<td class="num">{whole(row["trades"])}</td><td class="num">{pct(row["max_dd_pct"],2)}</td>'
                    f'<td class="num">{num(row["ulcer_index"],2)}</td><td class="num">{pct(row["time_underwater_pct"],2)}</td>'
                    f'<td class="num">{pct(row["win_rate"],2)}</td><td class="num">{num(row["recovery_factor"],2)}</td>'
                    f'<td class="num">{num(churn,1)}x</td></tr>'
                )

        mismatch_rows = ""
        for symbol in self.symbols:
            led = self.ledger.get(symbol, {})
            bench = self.rows["threadspark_v11"][symbol]
            val_return = led.get("val_return")
            full_return = bench.get("return_pct")
            gap = (full_return or 0) - (val_return or 0)
            mismatch_rows += (
                f"<tr><td>{symbol}</td><td>{esc(led.get('strategy_name', bench.get('strategy')))}</td>"
                f"<td>{esc(led.get('timeframe', bench.get('tf')))}</td><td class=\"num\">{pct(val_return,2,True)}</td>"
                f'<td class="num {"pos" if full_return >= 0 else "neg"}">{pct(full_return,2,True)}</td>'
                f'<td class="num {"pos" if gap >= 0 else "neg"}">{pct(gap,2,True)}</td>'
                f"<td class=\"num\">{whole(bench['trades'])}</td><td class=\"num\">{num(bench['profit_factor'],2)}</td>"
                f"<td class=\"num\">{pct(bench['max_dd_pct'],2)}</td></tr>"
            )

        v1_block = self.iteration_delta()
        tensor_cards = "".join(self.tensor_card(symbol) for symbol in self.symbols)
        generated = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        return f"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>ThreadSpark v1.1 Debug Report</title>
{self.styles()}
</head>
<body>
<header><div class="report">
  <h1>ThreadSpark v1.1 Debug Report</h1>
  <p class="subhead">Artifact-based diagnostic report for the single-arm ThreadSpark v1.1 rerun against frozen legacy and old-spoke baselines. The report focuses on why the new tensor iteration failed the hard-stop gate: not because it lacks upside, but because the upside still arrives with extreme churn, fragile breadth, and poor left-tail reliability.</p>
  <div class="meta"><span>Generated: {esc(generated)}</span><span>Experiment: {esc(self.exp.name)}</span><span>Spread mode: conservative_max</span><span>Symbols: {len(self.symbols)}</span><span>Baselines: frozen from 20260604</span></div>
</div></header>
<main><div class="report">
<section class="callout">
  <div class="verdict"><h2>Verdict: hard-stop bar failed</h2><p><strong>Do not promote ThreadSpark v1.1.</strong> The scorer found very large winners, but it did not solve the core failure mode. It still selects high-frequency EMAPullback variants in 5 of 6 symbols, produces only 3 profitable symbols, and carries a GBPUSD blowup worse than the legacy left-tail.</p><p>The mean return is spectacular because XAUUSD and USDJPY dominate the aggregate. The debug read must therefore use the median, per-symbol worst case, trade load, PF, and time-underwater.</p></div>
  <div class="panel" style="margin:0;"><h2>One-line diagnosis</h2><p class="lead">v1.1 added historical faces and volume-neutral path persistence, but the selection surface still let a churn-heavy EMAPullback cluster score as broad enough. The full-history replay exposes that three FX symbols do not carry live under honest cost.</p></div>
</section>
<section class="panel"><h2>Pre-Registered Hard-Stop Gate</h2><div class="table-wrap"><table><thead><tr><th>Status</th><th>Check</th><th>Observed</th><th>Why it matters</th></tr></thead><tbody>{gate_rows}</tbody></table></div></section>
<section class="panel"><h2>Run Summary Across Models</h2><p class="lead">The mean is the least trustworthy headline here. ThreadSpark wins mean and median return, but loses the reliability gates: PF, drawdown, time-underwater, breadth, and churn.</p><div class="cards">{self.metric_cards()}</div><div class="table-wrap" style="margin-top:14px;"><table><thead><tr><th>Model</th><th>Mean Return</th><th>Median Return</th><th>Median PF</th><th>Median Max-DD</th><th>Median Ulcer</th><th>Median Time UW</th><th>Total Trades</th></tr></thead><tbody>{summary_rows}</tbody></table></div></section>
<section class="panel chart-grid">{self.grouped_bars('return_pct','Per-symbol return by model','ThreadSpark v1.1 has huge right-tail winners but three negative symbols.',fmt='pct')}{self.grouped_bars('trades','Per-symbol trade load by model','The central unresolved issue is churn: ThreadSpark picks far more trades than either baseline.',fmt='int')}{self.grouped_bars('max_dd_pct','Per-symbol max drawdown by model','Lower is better. ThreadSpark creates larger drawdown on most FX symbols.',fmt='pct')}{self.scatter()}</section>
<section class="panel"><h2>Selection-Time vs Full-History Benchmark</h2><p class="lead">Every ThreadSpark winner looked positive on the selection surface. The benchmark replay is where the failures appear. That is the most useful debug gap: the selection tensor still does not make full-life damage decisive enough.</p><div class="table-wrap"><table><thead><tr><th>Symbol</th><th>Selected Strategy</th><th>TF</th><th>Selection Val Return</th><th>Full-History Return</th><th>Carry Gap</th><th>Trades</th><th>PF</th><th>Max-DD</th></tr></thead><tbody>{mismatch_rows}</tbody></table></div></section>
<section class="panel"><h2>Full Per-Symbol Debug Table</h2><p class="lead">Use this table to drill from aggregate to symbol-level behavior. The last column is ThreadSpark trade load relative to legacy for that symbol.</p><div class="table-wrap"><table><thead><tr><th>Model</th><th>Strategy</th><th>TF</th><th>Return</th><th>PF</th><th>Trades</th><th>Max-DD</th><th>Ulcer</th><th>Time UW</th><th>Win Rate</th><th>Recovery</th><th>TS/Legacy Trades</th></tr></thead><tbody>{symbol_rows}</tbody></table></div></section>
{v1_block}
<section class="panel"><h2>ThreadSpark Tensor Evidence Heatmaps</h2><p class="lead">Each card shows the selected candidate's evidence path. Red cells are inward dents, green cells are stronger spokes. Rows include train, validation, clean holdout, five historical cost chunks, and full historical cost. The quality bar beside each row is the row's aggregate face quality.</p><div class="tensor-grid">{tensor_cards}</div></section>
<section class="panel"><h2>Debug Findings</h2><div class="debug-grid">
  <div class="finding bad"><h3>1. EMAPullback monoculture</h3><p>Five of six ThreadSpark v1.1 winners are EMAPullbackContinuation. That is not automatically wrong, but in this run it coincides with high trade load, weak median PF, and three negative FX symbols under full-history cost.</p></div>
  <div class="finding bad"><h3>2. The churn signal is still not decisive</h3><p>Total trades are 9,927 versus legacy's 1,646. Per-symbol ratios exceed 5x for GBPUSD, EURUSD, USDJPY, and XAUUSD. The report therefore treats churn as a structural scorer failure, not a cosmetic metric.</p></div>
  <div class="finding bad"><h3>3. Breadth is not solved</h3><p>Only 3 of 6 symbols are profitable. The mean return is lifted by two very large winners, while GBPUSD and AUDJPY still violate the left-tail bar and EURAUD is also negative.</p></div>
  <div class="finding bad"><h3>4. Time-underwater is extreme</h3><p>ThreadSpark median time-underwater is 99.31%. Even profitable symbols spend nearly the entire path underwater. This is incompatible with the capital-preservation intent of the geometry.</p></div>
  <div class="finding good"><h3>5. There is real upside information</h3><p>USDJPY and XAUUSD show that the scorer is finding strong positive pockets. The problem is not absence of alpha signal; it is inability to separate robust positive extension from churn-shaped fragility.</p></div>
  <div class="finding"><h3>6. Next debug focus</h3><p>Do not debug from the aggregate. Inspect candidate pools per symbol/timeframe/regime: compare why the tensor ranks churny EMAPullback above lower-frequency alternatives, then test any fix as a general scorer change.</p></div>
</div></section>
<section class="panel"><h2>Recommended Debug Questions</h2><div class="note-list">
  <div class="note"><b>Candidate-pool ranking</b>For each failed symbol, list the top 10 ThreadSpark candidates with tensor score, trades, full-history return, PF, DD, and historical chunk quality. The bug is likely in relative ranking, not post-hoc benchmark math.</div>
  <div class="note"><b>Historical face weight</b>Historical chunk damage exists, but it did not stop the high-churn selections. Inspect whether final-face and historical-cost rows are overwhelmed by validation/parameter stability or by weak evidence-confidence penalties.</div>
  <div class="note"><b>Churn as shape, not veto</b>If a future branch continues, churn should be represented as evidence-resolution confidence and lifespan integrity inside the tensor, not as a v2-style external veto.</div>
  <div class="note"><b>Regime granularity</b>Use aggregate data for verdicts only. For fixes, drill down to symbol x timeframe x regime and candidate rank. That is where the scorer's wrong preference is formed.</div>
  <div class="note"><b>Cost survival</b>Execution-cost survival is still not making cost-heavy churn sufficiently unattractive. Compare net-to-gross and cost degradation across top candidates, not only the selected winner.</div>
  <div class="note"><b>Hard stop discipline</b>The pre-registered rule failed. Any further ThreadSpark work should be a new research branch, not a continuation that silently moves the goalposts.</div>
</div></section>
</div></main>
<footer><div class="report">Source artifacts: <code>{esc(self.bench)}</code> and <code>{esc(self.ledger_path)}</code>. This report is static and self-contained.</div></footer>
</body></html>"""

    def iteration_delta(self) -> str:
        if not self.prev_ts:
            return ""
        prev_summary = self.prev_ts["summary"]
        ts_sum = self.summary["threadspark_v11"]
        old_profitable = sum(1 for row in self.prev_ts["rows"] if row["return_pct"] > 0)
        new_profitable = sum(1 for row in self.data["threadspark_v11"]["rows"] if row["return_pct"] > 0)
        old_worst = min(row["return_pct"] for row in self.prev_ts["rows"])
        new_worst = min(row["return_pct"] for row in self.data["threadspark_v11"]["rows"])
        items = (
            ("Mean return", pct(prev_summary["mean_return_pct"], 2, True), pct(ts_sum["mean_return_pct"], 2, True)),
            ("Median return", pct(prev_summary["median_return_pct"], 2, True), pct(ts_sum["median_return_pct"], 2, True)),
            ("Profitable symbols", f"{old_profitable}/6", f"{new_profitable}/6"),
            ("Worst symbol", pct(old_worst, 2, True), pct(new_worst, 2, True)),
            ("Median PF", num(prev_summary["median_profit_factor"], 2), num(ts_sum["median_profit_factor"], 2)),
            ("Total trades", whole(prev_summary["total_trades"]), whole(ts_sum["total_trades"])),
        )
        cards = []
        for label, old, new in items:
            cards.append(
                f'<div class="delta-card"><h4>{esc(label)}</h4>'
                f'<div><span>v1</span><b>{esc(old)}</b></div>'
                f'<div><span>v1.1</span><b>{esc(new)}</b></div></div>'
            )
        return (
            '<section class="panel"><h2>Iteration Delta: v1 -> v1.1</h2>'
            '<p class="lead">The bounded iteration improved the headline median, but it did not cure the underlying trade-load and left-tail problem. It also intensified EMAPullback concentration.</p>'
            f'<div class="delta-grid">{"".join(cards)}</div></section>'
        )

    @staticmethod
    def styles() -> str:
        return """<style>
:root { --ink:#1f2428; --muted:#65706f; --paper:#fbfaf7; --panel:#ffffff; --line:#ded8cf; --good:#2a8656; --bad:#be443d; --warn:#b87916; }
* { box-sizing:border-box; }
body { margin:0; background:var(--paper); color:var(--ink); font:14px/1.45 Inter, ui-sans-serif, system-ui, -apple-system, Segoe UI, Arial, sans-serif; }
header { padding:34px 28px 20px; border-bottom:1px solid var(--line); background:#fffdf8; }
.report { max-width:1320px; margin:0 auto; }
h1 { margin:0 0 10px; font-size:clamp(28px,4vw,50px); line-height:1.0; letter-spacing:0; }
.subhead { max-width:1000px; color:#45504e; font-size:16px; margin:0; }
.meta { display:flex; flex-wrap:wrap; gap:10px; margin-top:18px; }
.meta span { border:1px solid var(--line); background:#fff; padding:7px 10px; border-radius:6px; color:#47504e; }
main { padding:24px 28px 60px; }
.panel { background:var(--panel); border:1px solid var(--line); border-radius:8px; padding:18px; margin:18px 0; box-shadow:0 1px 0 rgba(0,0,0,.02); }
.panel h2 { font-size:22px; margin:0 0 8px; }
.panel h3 { font-size:17px; margin:18px 0 8px; }
.lead { color:#44504d; max-width:1000px; margin:0 0 14px; }
.callout { display:grid; grid-template-columns:1.1fr .9fr; gap:16px; align-items:stretch; }
.verdict { background:#fff5f3; border:1px solid #efc5bf; border-radius:8px; padding:18px; }
.verdict h2 { color:#9d302b; font-size:26px; margin:0 0 8px; }
.verdict strong { color:#7f221e; }
.cards { display:grid; grid-template-columns:repeat(3,1fr); gap:12px; }
.metric-card { border:1px solid var(--line); border-radius:8px; padding:12px; background:#fff; }
.metric-card h4 { margin:0 0 10px; font-size:14px; color:#27302f; }
.metric-card p { margin:10px 0 0; color:var(--muted); font-size:12px; }
.metric-grid { display:grid; gap:7px; }
.metric-grid div { display:grid; grid-template-columns:12px 1fr auto; align-items:center; gap:8px; }
.metric-grid span { width:12px; height:12px; border-radius:3px; }
.metric-grid b { font-size:17px; }
.metric-grid em { color:var(--muted); font-style:normal; font-size:12px; }
.badge { display:inline-block; min-width:48px; text-align:center; padding:3px 7px; border-radius:5px; font-weight:700; font-size:12px; }
.badge.pass { background:#e2f2e8; color:#21663f; border:1px solid #b8dec6; }
.badge.fail { background:#fae2df; color:#9b2e29; border:1px solid #edb8b1; }
.table-wrap { overflow:auto; border:1px solid var(--line); border-radius:8px; }
table { width:100%; border-collapse:collapse; min-width:760px; background:#fff; }
th,td { padding:9px 10px; border-bottom:1px solid #ece6dd; text-align:left; vertical-align:top; }
th { background:#f6f0e8; font-size:12px; text-transform:uppercase; letter-spacing:.04em; color:#4d5653; }
td.num { text-align:right; font-variant-numeric:tabular-nums; }
tr.section-row th { background:#ebe2d7; color:#222; text-transform:none; font-size:14px; letter-spacing:0; }
tr:last-child td { border-bottom:0; }
.pos { color:#217348; font-weight:700; } .neg { color:#ae332d; font-weight:700; }
.swatch { display:inline-block; width:10px; height:10px; border-radius:2px; margin-right:7px; vertical-align:-1px; }
.chart-grid { display:grid; grid-template-columns:1fr; gap:16px; }
.chart { width:100%; height:auto; background:#fff; border:1px solid var(--line); border-radius:8px; }
.svg-title { font-size:18px; font-weight:700; fill:#222; }
.svg-subtitle { font-size:12px; fill:#68716e; }
.axis-label { font-size:13px; font-weight:700; fill:#333; }
.bar-label,.tick,.legend,.point-label,.axis-title { font-size:11px; fill:#38413e; }
.point-label { font-weight:700; }
.debug-grid { display:grid; grid-template-columns:repeat(2, minmax(0,1fr)); gap:14px; }
.finding { border-left:4px solid var(--warn); background:#fffaf0; padding:12px 14px; border-radius:6px; }
.finding.bad { border-left-color:var(--bad); background:#fff5f3; }
.finding.good { border-left-color:var(--good); background:#f3fbf6; }
.finding h3 { margin:0 0 6px; }
.finding p { margin:0; color:#46514f; }
.tensor-grid { display:grid; grid-template-columns:repeat(2, minmax(0,1fr)); gap:14px; }
.tensor-card { border:1px solid var(--line); border-radius:8px; background:#fff; padding:12px; overflow:hidden; }
.tensor-head { display:flex; flex-wrap:wrap; justify-content:space-between; gap:8px; margin-bottom:8px; }
.tensor-head span { color:var(--muted); font-size:12px; }
.feature-strip { display:grid; grid-template-columns:repeat(5,1fr); gap:6px; margin-bottom:8px; }
.feature-strip span { background:#f7f2eb; border:1px solid #e8ded2; border-radius:6px; padding:5px; font-size:11px; color:#56605d; }
.feature-strip b { display:block; color:#27302f; }
.tensor { width:100%; height:auto; }
.tensor-row { font-size:10px; fill:#4f5755; } .tensor-col { font-size:10px; font-weight:700; fill:#353c3a; }
.spoke-key { display:flex; flex-wrap:wrap; gap:5px 8px; color:#5f6966; font-size:10px; margin-top:5px; }
.delta-grid { display:grid; grid-template-columns:repeat(3,1fr); gap:10px; }
.delta-card { border:1px solid var(--line); border-radius:8px; padding:10px; background:#fff; }
.delta-card h4 { margin:0 0 8px; }
.delta-card div { display:flex; justify-content:space-between; gap:8px; border-top:1px solid #eee7df; padding-top:5px; margin-top:5px; }
.delta-card span { color:var(--muted); }
.note-list { display:grid; grid-template-columns:repeat(3,1fr); gap:12px; }
.note { border:1px solid var(--line); border-radius:8px; padding:12px; background:#fff; }
.note b { display:block; margin-bottom:6px; }
footer { padding:18px 28px 40px; color:var(--muted); border-top:1px solid var(--line); }
@media (max-width: 900px) {
  .callout,.cards,.debug-grid,.tensor-grid,.delta-grid,.note-list { grid-template-columns:1fr; }
  header, main, footer { padding-left:16px; padding-right:16px; }
  .feature-strip { grid-template-columns:repeat(2,1fr); }
}
</style>"""


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--experiment", required=True, type=Path)
    parser.add_argument("--previous-experiment", type=Path)
    parser.add_argument("--out", type=Path)
    args = parser.parse_args()

    builder = ReportBuilder(args.experiment, args.previous_experiment)
    out = args.out or args.experiment / "threadspark_v11_debug_report.html"
    out.write_text(builder.build(), encoding="utf-8")
    print(out.resolve())
    print(f"bytes={out.stat().st_size}")


if __name__ == "__main__":
    main()
