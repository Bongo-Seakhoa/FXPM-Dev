"""Living FxPM program dashboard — a local service, not a static file.

Every request regenerates the page from repo state: the agent bus, the lane
ledger, STATE.md, the reconciliation register and git history. Nothing is
hand-maintained, so if a figure looks wrong the underlying artifact is wrong.

    python tools/pm_dashboard.py serve            # http://localhost:8787
    python tools/pm_dashboard.py serve --port 9000 --open
    python tools/pm_dashboard.py build            # one-off static snapshot

Charts are hand-built inline SVG. No CDN, no chart library, no network - it
works offline and on a laptop with no spare memory.
"""

from __future__ import annotations

import argparse
import html
import json
import re
import subprocess
import sys
import threading
import webbrowser
from datetime import date, datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
SNAPSHOT = ROOT / "pm_dashboard.html"

DECISION_TYPES = {"question", "decision", "veto", "review_request"}

# Program timeline. Dates come from STATE.md, the Test A pre-registration and
# the book's own valid_until; edit here when a milestone genuinely moves.
TIMELINE = [
    ("June book optimised",        "2026-06-09", "2026-06-11", "done"),
    ("Book valid (expired)",       "2026-06-11", "2026-07-05", "expired"),
    ("Agent protocol v1",          "2026-07-12", "2026-07-14", "done"),
    ("Repo consolidation",         "2026-08-07", "2026-08-09", "done"),
    ("Test A scoring window",      "2026-04-01", "2026-07-14", "ready"),
    ("Gate 1 — PBO verdict",       "2026-08-10", "2026-08-11", "done"),
    ("Gate 2 — H1 canary",         "2026-08-11", "2026-08-11", "blocked"),
    ("Efficiency program",         "2026-08-11", "2026-08-13", "active"),
    ("Gate 3 — a full run",        "2026-08-14", "2026-09-15", "open"),
    ("Gate 7 — demo shadow",       "2026-09-15", "2026-10-15", "open"),
    ("Test B acquisition",         "2026-08-18", "2026-10-19", "pending"),
    ("Test B 63-day checkpoint",   "2026-10-19", "2026-10-26", "pending"),
    ("Test B 365-day horizon",     "2026-10-26", "2027-08-17", "pending"),
]

# Measured, unprofiled, 106,697-bar H1 frame. See docs/EFFICIENCY_FINDINGS.
COST_STAGES = [
    ("Production (inferred)", 1780.0, "Implied by the Gate-2 canary: 2 attempts in 2,851 s"),
    ("Synthetic, before",      548.4, "Backtester.run before any fix"),
    ("After axis digest",      279.7, "811669c — per-bar hashing vectorised"),
    ("After metrics",          201.1, "0c2e848 — drawdown + safe_float vectorised"),
    ("Loop floor",               6.7, "The jitted loop alone. The actual economic work."),
]

TOTAL_BACKTESTS = 576_000_000
WORKERS = 6

RISKS = [
    ("Cost-stress defect blocks every run", "critical",
     "The frozen-path invariant fires on positive-carry trades 0.67% into a search. "
     "Landed 2026-07-17, after the last completed book, so no finished run has ever "
     "exercised it. One-line repair proposed (CPM-022), awaiting Codex-PM."),
    ("A full run is not yet feasible", "critical",
     "576M backtests. At today's cost that is years of wall clock; at the 6.7 ms loop "
     "floor it is 7.4 days. Every 100 ms removed is ~8 months off the run."),
    ("Test suite can go green while blind", "high",
     "7 anti-forgery tests errored at setup, so they did not run while the file "
     "reported 18 passed. 4 of 5 contract drifts fixed; the 5th is structural."),
    ("Certification cannot be rushed", "high",
     "Test B is the only certification and needs 365 days of unseen data. First valid "
     "day 2026-08-18, credible result 2027-08-17. No code change shortens this."),
    ("Nothing is tradeable today", "critical",
     "The June book expired 2026-07-05 and was statistically indistinguishable from "
     "noise; both multiple-testing defences were inoperative when it was produced."),
]


# --------------------------------------------------------------- data loading

def _read(path: Path) -> str:
    try:
        return path.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return ""


def load_bus() -> list[dict]:
    rows = []
    for line in _read(ROOT / "agent_comms" / "BUS.jsonl").splitlines():
        line = line.strip()
        if line:
            try:
                rows.append(json.loads(line))
            except json.JSONDecodeError:
                pass
    return sorted(rows, key=lambda m: str(m.get("ts") or ""))


def load_lanes() -> list[dict]:
    """Newest first. LANES.json is stored newest-first and BUS.jsonl oldest-first,
    so sort rather than trust either file's order."""
    try:
        lanes = json.loads(_read(ROOT / "agent_comms" / "LANES.json")).get("lanes", [])
    except (json.JSONDecodeError, AttributeError):
        return []
    return sorted(lanes, key=lambda l: str(l.get("claimed_at") or ""), reverse=True)


def git(*args: str) -> str:
    try:
        return subprocess.run(
            ["git", *args], cwd=ROOT, capture_output=True, text=True, timeout=30
        ).stdout.strip()
    except (OSError, subprocess.SubprocessError):
        return ""


def load_commits(n: int = 20) -> list[dict]:
    raw = git("log", f"-{n}", "--format=%h\x1f%an\x1f%ad\x1f%s",
              "--date=format:%Y-%m-%d %H:%M")
    out = []
    for line in raw.splitlines():
        parts = line.split("\x1f")
        if len(parts) == 4:
            out.append(dict(zip(("sha", "author", "date", "subject"), parts)))
    return out


def load_gates() -> list[dict]:
    gates = []
    for line in _read(ROOT / "agent_comms" / "STATE.md").splitlines():
        if not line.startswith("|"):
            continue
        cells = [c.strip() for c in line.strip().strip("|").split("|")]
        if len(cells) >= 4 and cells[0].isdigit():
            gates.append({"n": cells[0], "gate": cells[1], "owner": cells[2],
                          "state": re.sub(r"\*\*|`", "", cells[3])})
    return gates


def load_register() -> list[dict]:
    """E-items are headings, not table rows: `### E1 — Title  \\`DECISION: x\\``."""
    out = []
    for line in _read(ROOT / "docs" / "RECONCILIATION_REGISTER.md").splitlines():
        m = re.match(r"^#{2,4}\s*(E\d+)\s*[—-]\s*(.*)$", line.strip())
        if not m:
            continue
        rest = m.group(2)
        d = re.search(r"DECISION:\s*([^`]+)", rest)
        out.append({
            "id": m.group(1),
            "what": re.sub(r"`DECISION:[^`]*`", "", rest).strip(" —-`"),
            "state": (d.group(1).strip() if d else "unrecorded"),
        })
    return out


def load_test_health() -> dict:
    n = git("ls-files", "tests/")
    return {"files": len([x for x in n.splitlines() if x.endswith(".py")])}


# ------------------------------------------------------------------ derivation

def gate_class(state: str) -> str:
    s = state.upper()
    if "DONE" in s:
        return "ok"
    if "BLOCK" in s or "TECHNICAL" in s:
        return "bad"
    if "PRE-REGISTERED" in s or "DATA" in s:
        return "warn"
    return "idle"


def unanswered(bus: list[dict]) -> list[dict]:
    referenced = {r for m in bus for r in (m.get("refs") or [])}
    return [m for m in bus
            if m.get("type") in DECISION_TYPES and m.get("id") not in referenced]


def parse_ts(ts):
    try:
        return datetime.fromisoformat(str(ts).replace("Z", "+00:00"))
    except (ValueError, AttributeError):
        return None


def age(ts) -> str:
    dt = parse_ts(ts)
    if dt is None:
        return "?"
    secs = int((datetime.now(timezone.utc) - dt).total_seconds())
    if secs < 0:
        return "just now"
    if secs < 3600:
        return f"{secs // 60}m ago"
    if secs < 86400:
        return f"{secs // 3600}h ago"
    return f"{secs // 86400}d ago"


def wall_days(ms_per_backtest: float) -> float:
    return TOTAL_BACKTESTS * (ms_per_backtest / 1000.0) / WORKERS / 86400.0


# --------------------------------------------------------------------- charts

def esc(v) -> str:
    return html.escape(str(v if v is not None else ""))


def truncate(text, n: int = 400) -> str:
    text = " ".join(str(text).split())
    return text if len(text) <= n else text[:n].rstrip() + "…"


def svg_gantt() -> str:
    lo, hi = date(2026, 4, 1), date(2027, 9, 30)
    span = (hi - lo).days
    row_h, pad_l, pad_t = 26, 196, 34
    w, h = 1080, pad_t + row_h * len(TIMELINE) + 30

    def x(d: date) -> float:
        return pad_l + (d - lo).days / span * (w - pad_l - 22)

    p = [f"<svg viewBox='0 0 {w} {h}' class='chart' role='img' "
         f"aria-label='Program timeline Gantt chart'>"]
    # quarter gridlines
    q = date(2026, 4, 1)
    while q <= hi:
        gx = x(q)
        p.append(f"<line x1='{gx:.1f}' y1='{pad_t-14}' x2='{gx:.1f}' y2='{h-24}' "
                 f"class='grid'/>")
        p.append(f"<text x='{gx:.1f}' y='{pad_t-20}' class='axis' "
                 f"text-anchor='middle'>{q:%b %Y}</text>")
        q = date(q.year + (q.month + 2) // 12, (q.month + 2) % 12 + 1, 1)
    # today
    today = datetime.now(timezone.utc).date()
    if lo <= today <= hi:
        tx = x(today)
        p.append(f"<line x1='{tx:.1f}' y1='{pad_t-14}' x2='{tx:.1f}' y2='{h-24}' "
                 f"class='today'/>")
        p.append(f"<text x='{tx:.1f}' y='{h-10}' class='axis today-t' "
                 f"text-anchor='middle'>today</text>")

    for i, (label, s, e, status) in enumerate(TIMELINE):
        ds, de = date.fromisoformat(s), date.fromisoformat(e)
        y = pad_t + i * row_h
        x0, x1 = x(ds), max(x(de), x(ds) + 4)
        p.append(f"<text x='{pad_l-10}' y='{y+13}' class='lbl' "
                 f"text-anchor='end'>{esc(label)}</text>")
        p.append(f"<rect x='{x0:.1f}' y='{y+3}' width='{x1-x0:.1f}' height='15' "
                 f"rx='4' class='bar-{status}'><title>{esc(label)}: {s} to {e} "
                 f"({esc(status)})</title></rect>")
    p.append("</svg>")
    return "".join(p)


def svg_cost_bars() -> str:
    w, h, pad_l, pad_t, row_h = 1080, 40 + 34 * len(COST_STAGES), 196, 26, 34
    top = max(s[1] for s in COST_STAGES)
    p = [f"<svg viewBox='0 0 {w} {h}' class='chart' role='img' "
         f"aria-label='Cost per backtest by stage'>"]
    for i, (label, ms, note) in enumerate(COST_STAGES):
        y = pad_t + i * row_h
        bw = max(2.0, ms / top * (w - pad_l - 190))
        cls = "bar-floor" if ms < 10 else ("bar-now" if abs(ms - 201.1) < .01 else "bar-cost")
        p.append(f"<text x='{pad_l-10}' y='{y+14}' class='lbl' "
                 f"text-anchor='end'>{esc(label)}</text>")
        p.append(f"<rect x='{pad_l}' y='{y+2}' width='{bw:.1f}' height='17' rx='4' "
                 f"class='{cls}'><title>{esc(note)}</title></rect>")
        p.append(f"<text x='{pad_l+bw+9:.1f}' y='{y+15}' class='val'>"
                 f"{ms:,.1f} ms · {wall_days(ms):,.0f}d run</text>")
    p.append("</svg>")
    return "".join(p)


def svg_feasibility() -> str:
    """Full-run wall time as a function of per-backtest cost. Log-log."""
    import math
    w, h, pl, pr, pt, pb = 1080, 300, 62, 24, 20, 42
    xmin, xmax = math.log10(5), math.log10(2000)      # ms
    ymin, ymax = math.log10(3), math.log10(3000)      # days

    def X(ms):
        return pl + (math.log10(ms) - xmin) / (xmax - xmin) * (w - pl - pr)

    def Y(d):
        return h - pb - (math.log10(max(d, 1e-9)) - ymin) / (ymax - ymin) * (h - pt - pb)

    p = [f"<svg viewBox='0 0 {w} {h}' class='chart' role='img' "
         f"aria-label='Full run duration versus per-backtest cost'>"]
    for dv in (7, 30, 90, 365, 1095):
        y = Y(dv)
        p.append(f"<line x1='{pl}' y1='{y:.1f}' x2='{w-pr}' y2='{y:.1f}' class='grid'/>")
        lab = f"{dv}d" if dv < 365 else f"{dv//365}y"
        p.append(f"<text x='{pl-9}' y='{y+4:.1f}' class='axis' text-anchor='end'>{lab}</text>")
    for mv in (6.7, 25, 100, 400, 1780):
        xx = X(mv)
        p.append(f"<line x1='{xx:.1f}' y1='{pt}' x2='{xx:.1f}' y2='{h-pb}' class='grid'/>")
        p.append(f"<text x='{xx:.1f}' y='{h-pb+17:.1f}' class='axis' "
                 f"text-anchor='middle'>{mv:g} ms</text>")
    # tolerance band: anything under 14 days
    p.append(f"<rect x='{pl}' y='{Y(14):.1f}' width='{w-pl-pr}' "
             f"height='{h-pb-Y(14):.1f}' class='band'/>")
    p.append(f"<text x='{w-pr-10}' y='{Y(11):.1f}' class='axis band-t' "
             f"text-anchor='end'>under two weeks — a runnable job</text>")
    pts = []
    ms = 5.0
    while ms <= 2000:
        pts.append(f"{X(ms):.1f},{Y(wall_days(ms)):.1f}")
        ms *= 1.06
    p.append(f"<polyline points='{' '.join(pts)}' class='curve'/>")
    for label, ms, _ in COST_STAGES:
        d = wall_days(ms)
        if not (5 <= ms <= 2000):
            continue
        cx, cy = X(ms), Y(d)
        cls = "dot-floor" if ms < 10 else ("dot-now" if abs(ms - 201.1) < .01 else "dot")
        p.append(f"<circle cx='{cx:.1f}' cy='{cy:.1f}' r='5' class='{cls}'>"
                 f"<title>{esc(label)}: {ms:,.1f} ms → {d:,.0f} days</title></circle>")
        anchor = "start" if ms < 300 else "end"
        dx = 10 if ms < 300 else -10
        p.append(f"<text x='{cx+dx:.1f}' y='{cy-9:.1f}' class='pt' "
                 f"text-anchor='{anchor}'>{esc(label)}</text>")
    p.append("</svg>")
    return "".join(p)


def svg_activity(bus: list[dict]) -> str:
    """Messages per day, stacked by sender."""
    by_day: dict[str, dict[str, int]] = {}
    for m in bus:
        dt = parse_ts(m.get("ts"))
        if dt is None:
            continue
        key = dt.date().isoformat()
        by_day.setdefault(key, {}).setdefault(m.get("from", "?"), 0)
        by_day[key][m.get("from", "?")] += 1
    if not by_day:
        return "<div class='empty'>No bus activity yet.</div>"
    days = sorted(by_day)[-21:]
    senders = sorted({s for d in days for s in by_day[d]})
    top = max(sum(by_day[d].values()) for d in days) or 1
    w, h, pb, pl = 1080, 190, 40, 34
    bw = (w - pl - 16) / max(len(days), 1)
    palette = {s: f"seg{i%4}" for i, s in enumerate(senders)}
    p = [f"<svg viewBox='0 0 {w} {h}' class='chart' role='img' "
         f"aria-label='Bus messages per day by agent'>"]
    for i, d in enumerate(days):
        x0 = pl + i * bw
        acc = 0.0
        for s in senders:
            n = by_day[d].get(s, 0)
            if not n:
                continue
            bh = n / top * (h - pb - 16)
            acc += bh
            p.append(f"<rect x='{x0+2:.1f}' y='{h-pb-acc:.1f}' width='{bw-4:.1f}' "
                     f"height='{bh:.1f}' class='{palette[s]}' rx='2'>"
                     f"<title>{d} · {esc(s)}: {n}</title></rect>")
        if i % 3 == 0 or i == len(days) - 1:
            p.append(f"<text x='{x0+bw/2:.1f}' y='{h-pb+16:.1f}' class='axis' "
                     f"text-anchor='middle'>{d[5:]}</text>")
    for i, s in enumerate(senders):
        p.append(f"<rect x='{pl + i*150}' y='{h-16}' width='10' height='10' rx='2' "
                 f"class='{palette[s]}'/>")
        p.append(f"<text x='{pl + i*150 + 16}' y='{h-7}' class='axis'>{esc(s)}</text>")
    p.append("</svg>")
    return "".join(p)


# ----------------------------------------------------------------------- style

CSS = """
:root{
 --bg:#f6f6f4;--panel:#fff;--ink:#1a1a19;--muted:#6d6d67;--line:#e3e3de;
 --ok:#1f7a4d;--okbg:#e6f4ec;--bad:#b3261e;--badbg:#fbe9e8;
 --warn:#8a6100;--warnbg:#fdf3e0;--idle:#4a5568;--idlebg:#eef0f3;
 --accent:#2f5fd0;--grid:#e8e8e3;--curve:#2f5fd0;--band:#e6f4ec;
 --c0:#2f5fd0;--c1:#1f7a4d;--c2:#8a6100;--c3:#7a4bd0;
 --mono:ui-monospace,"SF Mono",Menlo,Consolas,monospace;}
@media (prefers-color-scheme:dark){:root:not([data-theme="light"]){
 --bg:#121212;--panel:#1b1b1a;--ink:#ecebe5;--muted:#9b988f;--line:#2f2f2c;
 --ok:#6ddba0;--okbg:#12291e;--bad:#ff9d95;--badbg:#2e1614;
 --warn:#e8bf6a;--warnbg:#2b2213;--idle:#a6b0bd;--idlebg:#1f2429;
 --accent:#8fb0ff;--grid:#2a2a27;--curve:#8fb0ff;--band:#15291f;
 --c0:#8fb0ff;--c1:#6ddba0;--c2:#e8bf6a;--c3:#c3a3ff;}}
:root[data-theme="dark"]{
 --bg:#121212;--panel:#1b1b1a;--ink:#ecebe5;--muted:#9b988f;--line:#2f2f2c;
 --ok:#6ddba0;--okbg:#12291e;--bad:#ff9d95;--badbg:#2e1614;
 --warn:#e8bf6a;--warnbg:#2b2213;--idle:#a6b0bd;--idlebg:#1f2429;
 --accent:#8fb0ff;--grid:#2a2a27;--curve:#8fb0ff;--band:#15291f;
 --c0:#8fb0ff;--c1:#6ddba0;--c2:#e8bf6a;--c3:#c3a3ff;}
*{box-sizing:border-box}
body{margin:0;background:var(--bg);color:var(--ink);padding:22px 20px 70px;
 max-width:1180px;margin-inline:auto;
 font:15px/1.55 -apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,sans-serif}
h1{font-size:22px;margin:0 0 2px;letter-spacing:-.015em}
h2{font-size:12px;text-transform:uppercase;letter-spacing:.1em;color:var(--muted);
 margin:32px 0 10px;font-weight:650}
.sub{color:var(--muted);font-size:13px;margin-bottom:18px}
.panel{background:var(--panel);border:1px solid var(--line);border-radius:11px;
 padding:15px 17px;margin-bottom:12px}
.verdict{border-left:4px solid var(--bad);background:var(--badbg)}
.verdict .big{font-size:17px;font-weight:660;margin-bottom:5px}
.grid{display:grid;gap:12px;grid-template-columns:repeat(auto-fit,minmax(212px,1fr))}
.kpi{background:var(--panel);border:1px solid var(--line);border-radius:11px;padding:13px 15px}
.kpi .n{font-size:26px;font-weight:670;letter-spacing:-.025em;font-variant-numeric:tabular-nums}
.kpi .l{color:var(--muted);font-size:12px;margin-top:2px}
.kpi .d{font-size:12px;color:var(--muted);margin-top:7px;line-height:1.45}
table{width:100%;border-collapse:collapse;font-size:14px}
th{text-align:left;font-weight:650;color:var(--muted);font-size:11px;
 text-transform:uppercase;letter-spacing:.06em;padding:7px 9px;border-bottom:1px solid var(--line)}
td{padding:8px 9px;border-bottom:1px solid var(--line);vertical-align:top}
tr:last-child td{border-bottom:none}
.wrap{overflow-x:auto}
.tag{display:inline-block;padding:2px 8px;border-radius:20px;font-size:11px;font-weight:650}
.ok{background:var(--okbg);color:var(--ok)}.bad{background:var(--badbg);color:var(--bad)}
.warn{background:var(--warnbg);color:var(--warn)}.idle{background:var(--idlebg);color:var(--idle)}
code,.mono{font-family:var(--mono);font-size:12.5px}
.msg{border-bottom:1px solid var(--line);padding:10px 0}.msg:last-child{border-bottom:none}
.msg .h{font-size:12px;color:var(--muted);margin-bottom:3px}
.msg .b{font-size:13.5px;line-height:1.5}
.who{font-weight:650}.arrow{color:var(--muted)}
.foot{color:var(--muted);font-size:12px;margin-top:36px;border-top:1px solid var(--line);padding-top:12px}
.chart{width:100%;height:auto;display:block;overflow:visible}
.grid line,line.grid{stroke:var(--grid);stroke-width:1}
.axis{font-size:10.5px;fill:var(--muted)}
.lbl{font-size:12px;fill:var(--ink)}
.val{font-size:11.5px;fill:var(--muted);font-family:var(--mono)}
.pt{font-size:11px;fill:var(--ink);font-weight:600}
.today{stroke:var(--bad);stroke-width:1.5;stroke-dasharray:3 3}
.today-t{fill:var(--bad);font-weight:650}
.bar-done{fill:var(--ok)}.bar-active{fill:var(--accent)}.bar-blocked{fill:var(--bad)}
.bar-open{fill:var(--idle);opacity:.55}.bar-pending{fill:var(--warn);opacity:.75}
.bar-expired{fill:var(--bad);opacity:.35}.bar-ready{fill:var(--ok);opacity:.45}
.bar-cost{fill:var(--idle);opacity:.7}.bar-now{fill:var(--accent)}.bar-floor{fill:var(--ok)}
.curve{fill:none;stroke:var(--curve);stroke-width:2}
.band{fill:var(--band)}.band-t{fill:var(--ok);font-weight:600}
.dot{fill:var(--idle)}.dot-now{fill:var(--accent)}.dot-floor{fill:var(--ok)}
.seg0{fill:var(--c0)}.seg1{fill:var(--c1)}.seg2{fill:var(--c2)}.seg3{fill:var(--c3)}
.empty{color:var(--muted);font-size:13px}
.legend{display:flex;gap:15px;flex-wrap:wrap;margin-top:9px;font-size:11.5px;color:var(--muted)}
.legend i{display:inline-block;width:10px;height:10px;border-radius:2px;margin-right:5px;
 vertical-align:-1px}
.note{font-size:12.5px;color:var(--muted);margin-top:9px;line-height:1.5}
"""


# ---------------------------------------------------------------------- render

def render() -> str:
    bus, lanes, gates = load_bus(), load_lanes(), load_gates()
    commits, register = load_commits(), load_register()
    now = datetime.now(timezone.utc)
    active = [l for l in lanes if l.get("status") == "active"]
    open_q = unanswered(bus)[-10:]
    gates_done = sum(1 for g in gates if "DONE" in g["state"].upper())
    blocked = [g for g in gates if gate_class(g["state"]) == "bad"]

    p: list[str] = []
    A = p.append
    A("<!doctype html><html><head><meta charset='utf-8'>")
    A("<meta name='viewport' content='width=device-width,initial-scale=1'>")
    A("<title>FxPM — program dashboard</title>")
    A("<meta http-equiv='refresh' content='60'>")
    A(f"<style>{CSS}</style></head><body>")

    A("<h1>FxPM — program dashboard</h1>")
    A(f"<div class='sub'>Live · rebuilt from repo state on every request · "
      f"{now:%Y-%m-%d %H:%M} UTC · refreshes every 60s</div>")

    A("<div class='panel verdict'><div class='big'>Not tradeable. Do not fund an account.</div>"
      "<div>No optimisation run has completed since the June book expired on 2026-07-05. "
      "That book was statistically indistinguishable from noise and both multiple-testing "
      "defences were inoperative when it was produced.</div>")
    if blocked:
        A("<div style='margin-top:7px'>Hard blocker: "
          + "; ".join(f"<b>Gate {esc(g['n'])}</b> — {esc(g['gate'])}" for g in blocked)
          + "</div>")
    A("</div>")

    A("<div class='grid'>")
    A(f"<div class='kpi'><div class='n'>{gates_done}/{len(gates) or 8}</div>"
      f"<div class='l'>Gates closed</div><div class='d'>Gate 8 (Test B) is the only "
      f"certification and cannot finish before Aug 2027.</div></div>")
    A(f"<div class='kpi'><div class='n'>201 ms</div><div class='l'>Cost per backtest</div>"
      f"<div class='d'>Was 548 ms. The loop doing the real work is 6.7 ms — 30x of pure "
      f"overhead remains.</div></div>")
    A(f"<div class='kpi'><div class='n'>{wall_days(201.1):,.0f} d</div>"
      f"<div class='l'>Full run at today's cost</div>"
      f"<div class='d'>{wall_days(6.7):,.1f} days at the loop floor. That gap is the "
      f"whole problem.</div></div>")
    pending = [e for e in register if "pending" in e["state"].lower()]
    A(f"<div class='kpi'><div class='n'>{len(pending)}</div>"
      f"<div class='l'>Decisions waiting on you</div>"
      f"<div class='d'>Reconciliation elements nobody else can settle. "
      f"{len(active)} lane(s) active, {len(open_q)} bus item(s) unanswered.</div></div>")
    A("</div>")

    A("<h2>Programme timeline</h2><div class='panel'>")
    A(svg_gantt())
    A("<div class='legend'>"
      "<span><i style='background:var(--ok)'></i>done</span>"
      "<span><i style='background:var(--accent)'></i>active</span>"
      "<span><i style='background:var(--bad)'></i>blocked</span>"
      "<span><i style='background:var(--warn);opacity:.75'></i>pending</span>"
      "<span><i style='background:var(--idle);opacity:.55'></i>open</span>"
      "<span><i style='background:var(--bad);opacity:.35'></i>expired</span></div>")
    A("<div class='note'>Test B is the binding constraint on certification: it needs 365 "
      "days of data nobody has seen, so the earliest credible verdict is 2027-08-17. "
      "No code change shortens it.</div></div>")

    A("<h2>Gates</h2><div class='panel wrap'><table>"
      "<tr><th>#</th><th>Gate</th><th>Owner</th><th>State</th></tr>")
    for g in gates:
        A(f"<tr><td class='mono'>{esc(g['n'])}</td><td>{esc(g['gate'])}</td>"
          f"<td>{esc(g['owner'])}</td><td><span class='tag {gate_class(g['state'])}'>"
          f"{esc(truncate(g['state'], 95))}</span></td></tr>")
    A("</table></div>")

    A("<h2>Cost per backtest — what has been won</h2><div class='panel'>")
    A(svg_cost_bars())
    A("<div class='note'>Measured unprofiled on a 106,697-bar H1 frame. The right-hand "
      "figure is the implied full-run wall clock across 6 workers. Production is dearer "
      "than the synthetic because its frames carry ~100 columns; the fixes so far are "
      "bar-count driven, so they transfer roughly 1:1 rather than scaling.</div></div>")

    A("<h2>Is a full run reachable?</h2><div class='panel'>")
    A(svg_feasibility())
    A(f"<div class='note'>{TOTAL_BACKTESTS:,} backtests — 2,400 cells x 300 attempts x 200 "
      f"Optuna trials x 4 folds — spread across {WORKERS} workers. The curve is the whole "
      f"argument: the universe is not too big, the per-evaluation cost is too high. Every "
      f"100 ms removed is about 8 months off the run.</div></div>")

    A("<h2>Raised and not yet answered</h2>")
    if open_q:
        A("<div class='panel'>")
        for m in open_q[::-1]:
            A(f"<div class='msg'><div class='h'><span class='mono'>{esc(m.get('id'))}</span> · "
              f"<span class='who'>{esc(m.get('from'))}</span> <span class='arrow'>&rarr;</span> "
              f"<span class='who'>{esc(m.get('to'))}</span> · {esc(m.get('type'))} · "
              f"{esc(age(m.get('ts')))}</div>"
              f"<div class='b'>{esc(truncate(m.get('body'), 300))}</div></div>")
        A("</div>")
    else:
        A("<div class='panel empty'>Nothing outstanding.</div>")

    A("<h2>Risk register</h2><div class='panel wrap'><table>"
      "<tr><th>Risk</th><th>Severity</th><th>Detail</th></tr>")
    for name, sev, detail in RISKS:
        cls = "bad" if sev == "critical" else "warn"
        A(f"<tr><td><b>{esc(name)}</b></td>"
          f"<td><span class='tag {cls}'>{esc(sev)}</span></td>"
          f"<td>{esc(detail)}</td></tr>")
    A("</table></div>")

    A("<h2>Agent activity</h2><div class='panel'>")
    A(svg_activity(bus))
    A("</div>")

    A("<h2>Lane ledger</h2><div class='panel wrap'><table>"
      "<tr><th>Lane</th><th>Owner</th><th>Status</th><th>Purpose</th><th>Claimed</th></tr>")
    for l in lanes[:12]:
        cls = "warn" if l.get("status") == "active" else "idle"
        A(f"<tr><td class='mono'>{esc(l.get('id'))}</td><td>{esc(l.get('owner'))}</td>"
          f"<td><span class='tag {cls}'>{esc(l.get('status'))}</span></td>"
          f"<td>{esc(truncate(l.get('purpose'), 175))}</td>"
          f"<td class='mono'>{esc(age(l.get('claimed_at')))}</td></tr>")
    A("</table></div>")

    A("<h2>Agent bus — latest</h2><div class='panel'>")
    for m in bus[-12:][::-1]:
        A(f"<div class='msg'><div class='h'><span class='mono'>{esc(m.get('id'))}</span> · "
          f"<span class='who'>{esc(m.get('from'))}</span> <span class='arrow'>&rarr;</span> "
          f"<span class='who'>{esc(m.get('to'))}</span> · {esc(m.get('type'))} · "
          f"{esc(age(m.get('ts')))}</div>"
          f"<div class='b'>{esc(truncate(m.get('body')))}</div></div>")
    A("</div>")

    if register:
        A("<h2>Reconciliation register — your decisions</h2><div class='panel wrap'><table>"
          "<tr><th>ID</th><th>Element</th><th>Decision</th></tr>")
        ordered = sorted(register, key=lambda e: "pending" not in e["state"].lower())
        for e in ordered:
            cls = "warn" if "pending" in e["state"].lower() else "ok"
            A(f"<tr><td class='mono'>{esc(e['id'])}</td>"
              f"<td>{esc(truncate(e['what'], 130))}</td>"
              f"<td><span class='tag {cls}'>{esc(truncate(e['state'], 60))}</span></td></tr>")
        A(f"</table></div><div class='note'>{len(pending)} of {len(register)} elements are "
          f"still open. These are reconcile-or-discard calls on work from earlier FxPM "
          f"iterations — they need your judgement, not another measurement. Detail in "
          f"docs/RECONCILIATION_REGISTER.md.</div>")

    A("<h2>Recent commits</h2><div class='panel wrap'><table>"
      "<tr><th>SHA</th><th>When</th><th>Author</th><th>Subject</th></tr>")
    for c in commits:
        A(f"<tr><td class='mono'>{esc(c['sha'])}</td><td class='mono'>{esc(c['date'])}</td>"
          f"<td>{esc(c['author'])}</td><td>{esc(c['subject'])}</td></tr>")
    A("</table></div>")

    A("<div class='foot'>Rebuilt on every request from agent_comms/BUS.jsonl, "
      "agent_comms/LANES.json, agent_comms/STATE.md, docs/RECONCILIATION_REGISTER.md and "
      "git log. Nothing is hand-maintained — if a figure looks wrong, the underlying "
      "artifact is wrong. Timeline dates and the measured cost stages are declared at the "
      "top of tools/pm_dashboard.py. Full detail: docs/EFFICIENCY_FINDINGS_2026-08-11.md."
      "</div></body></html>")
    return "\n".join(p)


# ---------------------------------------------------------------------- server

class Handler(BaseHTTPRequestHandler):
    def do_GET(self):  # noqa: N802
        if self.path not in ("/", "/index.html"):
            self.send_error(404)
            return
        try:
            body = render().encode("utf-8")
        except Exception as exc:  # keep the service up; show the failure
            body = (f"<pre>dashboard build failed:\n{html.escape(repr(exc))}</pre>"
                    ).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *_args):
        pass


def serve(port: int, open_browser: bool) -> int:
    srv = ThreadingHTTPServer(("127.0.0.1", port), Handler)
    url = f"http://127.0.0.1:{port}/"
    print(f"FxPM dashboard live at {url}  (ctrl-c to stop)")
    if open_browser:
        threading.Timer(0.6, lambda: webbrowser.open(url)).start()
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        print("\nstopped")
    finally:
        srv.server_close()
    return 0


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("mode", nargs="?", default="serve", choices=("serve", "build"))
    ap.add_argument("--port", type=int, default=8787)
    ap.add_argument("--open", action="store_true")
    args = ap.parse_args()
    if args.mode == "build":
        SNAPSHOT.write_text(render(), encoding="utf-8")
        print(f"wrote {SNAPSHOT}")
        if args.open:
            webbrowser.open(SNAPSHOT.as_uri())
        return 0
    return serve(args.port, args.open)


if __name__ == "__main__":
    sys.exit(main())
