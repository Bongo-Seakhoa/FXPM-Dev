"""Living FxPM program dashboard — a local service, not a static file.

Every request regenerates operational state from the agent bus, lane ledger,
STATE.md, reconciliation register and git history. A small number of dated
benchmark and programme constants are declared below and labelled in the page.

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
from datetime import date, datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlparse

ROOT = Path(__file__).resolve().parent.parent
SNAPSHOT = ROOT / "pm_dashboard.html"

RESPONSE_REQUIRED_TYPES = {"question", "veto", "review_request"}

# Program timeline. Dates come from STATE.md, the Test A pre-registration and
# the book's own valid_until; edit here when a milestone genuinely moves.
TIMELINE = [
    ("June book optimised",        "2026-06-09", "2026-06-11", "done"),
    ("Book valid (expired)",       "2026-06-11", "2026-07-05", "expired"),
    ("Agent protocol v1",          "2026-07-12", "2026-07-14", "done"),
    ("Repo consolidation",         "2026-08-07", "2026-08-09", "done"),
    ("Test A scoring window",      "2026-04-01", "2026-07-14", "ready"),
    ("Gate 1 — PBO verdict",       "2026-08-10", "2026-08-11", "done"),
    ("Gate 2 — H1 technical result", "2026-08-11", "2026-08-11", "done"),
    ("Efficiency baseline work",   "2026-08-11", None, "active"),
    ("Test B purge / no valid days", "2026-07-14", "2026-08-18", "pending"),
    ("Test B preliminary window",  "2026-08-18", "2026-10-19", "pending"),
    ("Test B credible horizon",    "2026-08-18", "2027-08-17", "pending"),
]

# One exact preserved EURUSD/H1 attempt-2/fold-0 path. These stages are
# comparable end-to-end fold packages, not synthetic per-backtest estimates.
FOLD_STAGES = [
    ("Measured baseline", 2.618102, "Preserved H1 fold before E21/E22"),
    ("Reuse one backtester", 1.780566, "1d6dee6 — reuse immutable instance state"),
    ("Reuse verified swap contract", 1.006329, "ad3d9db — retain strict replay"),
]

STARTUP_STAGES = [
    ("Preparation", 245.766, "CX-076: plan receipt to first freeze-object write"),
    ("Artifact registration", 86.154, "CX-076: 1,211 logical freeze artifacts"),
]

E24_REOPEN_BEFORE_SECONDS = 81.884079
E24_REOPEN_AFTER_SECONDS = 5.716661

SINGLE_GRID_MAX_RUNS = 11_520_000

RISKS = [
    ("Cost-stress defect blocks Gate 2", "critical",
     "The sole H1 canary stopped at attempt 2/fold 0 because trade 23 improved under "
     "stress. It is a real technical result; no profitability verdict was reached."),
    ("Full-run duration is not yet credibly measured", "high",
     "The former 576M/years estimate mixed the legacy Optuna path into the canonical "
     "executor and is withdrawn. One-cell end-to-end timing comes before a forecast."),
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
    rows: dict[str, dict] = {}
    bus_root = ROOT / "agent_comms"
    paths = sorted(bus_root.glob("BUS.archive-*.jsonl")) + [bus_root / "BUS.jsonl"]
    for path in paths:
        for line in _read(path).splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                message = json.loads(line)
            except json.JSONDecodeError:
                continue
            if not isinstance(message, dict):
                continue
            message_id = str(message.get("id") or "")
            if message_id:
                rows[message_id] = message
    return sorted(rows.values(), key=lambda message: str(message.get("ts") or ""))


def source_health() -> list[dict]:
    specs = [
        ("agent_comms/STATE.md", "text"),
        ("agent_comms/LANES.json", "json"),
        ("docs/RECONCILIATION_REGISTER.md", "text"),
    ]
    bus_paths = sorted((ROOT / "agent_comms").glob("BUS.archive-*.jsonl")) + [
        ROOT / "agent_comms" / "BUS.jsonl"
    ]
    specs.extend((path.relative_to(ROOT).as_posix(), "jsonl") for path in bus_paths)
    rows = []
    for relative, kind in specs:
        path = ROOT / relative
        state, detail = "ok", "readable"
        if not path.is_file():
            state, detail = "missing", "required file is absent"
        else:
            try:
                raw = path.read_text(encoding="utf-8")
                if kind == "json":
                    payload = json.loads(raw)
                    if not isinstance(payload, dict) or not isinstance(
                        payload.get("lanes"), list
                    ):
                        raise ValueError("LANES_SCHEMA_INVALID")
                elif kind == "jsonl":
                    for line in raw.splitlines():
                        if line.strip():
                            message = json.loads(line)
                            if not isinstance(message, dict) or any(
                                not isinstance(message.get(key), str)
                                or not message.get(key)
                                for key in ("id", "from", "to", "type")
                            ) or not isinstance(message.get("refs", []), list):
                                raise ValueError("BUS_MESSAGE_SCHEMA_INVALID")
                elif not raw.strip():
                    raise ValueError("TEXT_SOURCE_EMPTY")
            except (OSError, UnicodeError, json.JSONDecodeError, ValueError) as exc:
                state, detail = "invalid", type(exc).__name__
        rows.append(
            {
                "name": relative,
                "state": state,
                "detail": detail,
                "updated": datetime.fromtimestamp(path.stat().st_mtime, timezone.utc)
                if path.is_file()
                else None,
            }
        )
    return rows


def load_lanes() -> list[dict]:
    """Newest first. LANES.json is stored newest-first and BUS.jsonl oldest-first,
    so sort rather than trust either file's order."""
    try:
        payload = json.loads(_read(ROOT / "agent_comms" / "LANES.json"))
        if not isinstance(payload, dict) or not isinstance(payload.get("lanes"), list):
            return []
        lanes = [lane for lane in payload["lanes"] if isinstance(lane, dict)]
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


def audit_report_status() -> dict:
    root = ROOT / "audit_reports" / "fxpm_system_audit_2026-08-11"
    receipt_path = root / "DELIVERY_RECEIPTS.json"
    try:
        receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
        reports = receipt.get("reports") or []
    except (OSError, UnicodeError, json.JSONDecodeError, AttributeError):
        reports = []
    verified = [
        report
        for report in reports
        if report.get("ok") is True
        and (root / str(report.get("html") or "")).is_file()
        and report.get("stages", {}).get("verification") == "passed"
    ]
    return {
        "root": root,
        "total": len(reports),
        "verified": len(verified),
        "updated": datetime.fromtimestamp(receipt_path.stat().st_mtime, timezone.utc)
        if receipt_path.is_file()
        else None,
    }


def lineage_status(bus: list[dict]) -> list[dict]:
    by_id = {message.get("id"): message for message in bus}
    g0 = by_id.get("CX-025", {})
    g0_complete = "G0 COMPLETE" in str(g0.get("body") or "")
    return [
        {
            "lineage": "Academic research",
            "state": "restore-tested" if g0_complete else "unverified",
            "evidence": "CX-025" if g0_complete else "CX-011",
        },
        {
            "lineage": "Commercial history",
            "state": "remote tags confirmed" if g0_complete else "unverified",
            "evidence": "CX-025" if g0_complete else "CX-011",
        },
        {
            "lineage": "Canonical production",
            "state": git("branch", "--show-current") or "unknown",
            "evidence": git("rev-parse", "--short", "HEAD") or "unknown",
        },
    ]


# ------------------------------------------------------------------ derivation

def gate_class(state: str, owner: str = "") -> str:
    s = f"{owner} {state}".upper()
    if "DONE" in s:
        return "ok"
    if "BLOCK" in s or "TECHNICAL" in s:
        return "bad"
    if "PRE-REGISTERED" in s or "DATA" in s:
        return "warn"
    return "idle"


def message_resolutions(bus: list[dict]) -> dict[str, dict[str, str]]:
    """Classify only messages that actually require a peer response.

    A decision is a record, not automatically an unanswered question. A
    reciprocal, chronological ref is the machine-checkable closure signal.
    """

    by_id = {str(message.get("id") or ""): message for message in bus}
    replies: dict[str, list[str]] = {}
    for message in bus:
        for reference in message.get("refs") or []:
            original = by_id.get(str(reference))
            if original is None or message.get("from") == original.get("from"):
                continue
            original_ts = parse_ts(original.get("ts"))
            response_ts = parse_ts(message.get("ts"))
            if original_ts and response_ts and response_ts < original_ts:
                continue
            original_target = original.get("to")
            if original_target not in {"all", message.get("from")}:
                continue
            replies.setdefault(str(reference), []).append(str(message.get("id") or ""))
    result: dict[str, dict[str, str]] = {}
    for message_id, message in by_id.items():
        if message.get("type") not in RESPONSE_REQUIRED_TYPES:
            continue
        if replies.get(message_id):
            status = "answered"
            note = "Answered by " + ", ".join(replies[message_id])
        else:
            status = "open"
            note = "Peer response required"
        result[message_id] = {"status": status, "note": note}
    return result


def open_peer_requests(bus: list[dict]) -> list[dict]:
    resolutions = message_resolutions(bus)
    return [
        message
        for message in bus
        if resolutions.get(str(message.get("id") or ""), {}).get("status") == "open"
        and message.get("from") in {"claude-pm", "codex-pm"}
        and message.get("to") in {"claude-pm", "codex-pm"}
    ]


def pm_continuity(bus: list[dict]) -> list[dict]:
    rows = []
    for actor in ("claude-pm", "codex-pm"):
        messages = [message for message in bus if message.get("from") == actor]
        latest = messages[-1] if messages else {}
        rows.append(
            {
                "actor": actor,
                "latest_id": latest.get("id") or "—",
                "latest_ts": latest.get("ts"),
                "latest_type": latest.get("type") or "—",
                "message_count": len(messages),
            }
        )
    return rows


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


def improvement_pct(before: float, after: float) -> float:
    return (before - after) / before * 100.0


def service_snapshot() -> dict:
    bus, lanes, gates = load_bus(), load_lanes(), load_gates()
    active = [lane for lane in lanes if lane.get("status") == "active"]
    sources = source_health()
    service_state = "ok" if all(row["state"] == "ok" for row in sources) else "degraded"
    return {
        "schema_version": "fxpm.project-dashboard-status.v1",
        "status": service_state,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "tradeable": False,
        "git_head": git("rev-parse", "--short", "HEAD"),
        "branch": git("branch", "--show-current"),
        "gates": gates,
        "active_lanes": active,
        "open_peer_requests": open_peer_requests(bus),
        "pm_continuity": pm_continuity(bus),
        "sources": [
            {**row, "updated": row["updated"].isoformat() if row["updated"] else None}
            for row in sources
        ],
    }


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
        ds = date.fromisoformat(s)
        de = date.fromisoformat(e) if e else datetime.now(timezone.utc).date()
        y = pad_t + i * row_h
        x0, x1 = x(ds), max(x(de), x(ds) + 4)
        p.append(f"<text x='{pad_l-10}' y='{y+13}' class='lbl' "
                 f"text-anchor='end'>{esc(label)}</text>")
        p.append(f"<rect x='{x0:.1f}' y='{y+3}' width='{x1-x0:.1f}' height='15' "
                 f"rx='4' class='bar-{status}'><title>{esc(label)}: {s} to {e or 'present'} "
                 f"({esc(status)})</title></rect>")
    p.append("</svg>")
    return "".join(p)


def svg_fold_performance() -> str:
    w, h, pad_l, pad_t, row_h = 1080, 40 + 42 * len(FOLD_STAGES), 210, 26, 42
    top = max(stage[1] for stage in FOLD_STAGES)
    p = [f"<svg viewBox='0 0 {w} {h}' class='chart' role='img' "
         f"aria-label='Measured authoritative fold package time by stage'>"]
    for i, (label, seconds, note) in enumerate(FOLD_STAGES):
        y = pad_t + i * row_h
        bw = max(2.0, seconds / top * (w - pad_l - 210))
        cls = "bar-now" if i == len(FOLD_STAGES) - 1 else "bar-cost"
        p.append(f"<text x='{pad_l-10}' y='{y+14}' class='lbl' "
                 f"text-anchor='end'>{esc(label)}</text>")
        p.append(f"<rect x='{pad_l}' y='{y+2}' width='{bw:.1f}' height='17' rx='4' "
                 f"class='{cls}'><title>{esc(note)}</title></rect>")
        p.append(f"<text x='{pad_l+bw+9:.1f}' y='{y+15}' class='val'>"
                 f"{seconds:.3f} s</text>")
    p.append("</svg>")
    return "".join(p)


def svg_startup_breakdown() -> str:
    total = sum(stage[1] for stage in STARTUP_STAGES)
    w, h, x0, usable = 1080, 135, 36, 1008
    p = [f"<svg viewBox='0 0 {w} {h}' class='chart' role='img' "
         f"aria-label='Measured pre-evaluation startup breakdown'>"]
    cursor = x0
    for index, (label, seconds, note) in enumerate(STARTUP_STAGES):
        width = usable * seconds / total
        p.append(
            f"<rect x='{cursor:.1f}' y='28' width='{width:.1f}' height='34' "
            f"rx='5' class='seg{index}'><title>{esc(note)}: {seconds:.3f}s</title></rect>"
        )
        if width > 150:
            p.append(
                f"<text x='{cursor + width / 2:.1f}' y='49' class='inside' "
                f"text-anchor='middle'>{esc(label)} · {seconds:.1f}s</text>"
            )
        cursor += width
    p.append(
        f"<text x='{x0}' y='91' class='lbl'>Total before evaluation: {total:.1f}s</text>"
    )
    p.append(
        f"<text x='{w-x0}' y='91' class='axis' text-anchor='end'>"
        "Preserved pre-E23 baseline (CX-076); current end-to-end rebaseline pending.</text>"
    )
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
.top{display:flex;justify-content:space-between;gap:18px;align-items:flex-start}
.live{display:inline-flex;gap:7px;align-items:center;color:var(--ok);font-size:12px;font-weight:650}
.live:before{content:"";width:8px;height:8px;border-radius:50%;background:var(--ok);box-shadow:0 0 0 4px var(--okbg)}
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
.split{display:grid;grid-template-columns:minmax(0,1.15fr) minmax(280px,.85fr);gap:12px}
.flow{display:grid;grid-template-columns:repeat(6,minmax(140px,1fr));gap:8px;overflow-x:auto;padding-bottom:2px}
.step{position:relative;min-height:108px;border:1px solid var(--line);border-radius:9px;padding:11px;background:var(--panel)}
.step:not(:last-child):after{content:"→";position:absolute;right:-10px;top:40px;color:var(--muted);z-index:2}
.step .num{font:11px var(--mono);color:var(--muted)}
.step .name{font-weight:650;margin:5px 0 4px}.step .why{font-size:11.5px;color:var(--muted);line-height:1.4}
.step.active{border-color:var(--accent);box-shadow:inset 0 3px 0 var(--accent)}
.step.blocked{border-color:var(--bad);box-shadow:inset 0 3px 0 var(--bad)}
.inside{font-size:12px;fill:white;font-weight:650}
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
@media (max-width:760px){body{padding:16px 12px 50px}.top{display:block}.live{margin:8px 0}.split{grid-template-columns:1fr}.chart{min-width:720px}.panel:has(.chart){overflow-x:auto}.flow{grid-template-columns:repeat(6,170px)}}
"""


# ---------------------------------------------------------------------- render

def render(*, live: bool = True) -> str:
    bus, lanes, gates = load_bus(), load_lanes(), load_gates()
    commits, register = load_commits(), load_register()
    now = datetime.now(timezone.utc)
    active = [l for l in lanes if l.get("status") == "active"]
    open_q = open_peer_requests(bus)
    pm_rows = pm_continuity(bus)
    reports = audit_report_status()
    lineages = lineage_status(bus)
    gates_done = sum(1 for g in gates if "DONE" in g["state"].upper())
    blocked = [g for g in gates if gate_class(g["state"], g["owner"]) == "bad"]
    current_fold = FOLD_STAGES[-1][1]
    fold_gain = improvement_pct(FOLD_STAGES[0][1], current_fold)
    sources = source_health()
    oldest_source = min((row["updated"] for row in sources if row["updated"]), default=None)

    p: list[str] = []
    A = p.append
    A("<!doctype html><html><head><meta charset='utf-8'>")
    A("<meta name='viewport' content='width=device-width,initial-scale=1'>")
    A("<title>FxPM — program control room</title>")
    if live:
        A("<meta http-equiv='refresh' content='60'>")
    A(f"<style>{CSS}</style></head><body>")

    A("<div class='top'><div><h1>FxPM — program control room</h1>")
    A(f"<div class='sub'>{'Rebuilt from repository state on every request' if live else 'Static snapshot'} · "
      f"{now:%Y-%m-%d %H:%M} UTC{' · refreshes every 60s' if live else ''}</div></div>")
    if live:
        A("<div class='live'>local service online</div>")
    A("</div>")

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
    A(f"<div class='kpi'><div class='n'>{current_fold:.2f} s</div>"
      f"<div class='l'>Authoritative fold package</div>"
      f"<div class='d'>Exact preserved H1 path; down {fold_gain:.1f}% from 2.62 s with "
      f"identical decision hashes and failure.</div></div>")
    A("<div class='kpi'><div class='n'>17 → 2</div>"
      "<div class='l'>Native fits per canary</div>"
      "<div class='d'>E23 retained at 79303f8. Fifteen discarded full refits were "
      "replaced by bound held-state checks; final replay remains.</div></div>")
    pending = [e for e in register if "pending" in e["state"].lower()]
    A(f"<div class='kpi'><div class='n'>{len(pending)}</div>"
      f"<div class='l'>Unresolved lineage items</div>"
      f"<div class='d'>PMs may resolve items inside their charter; only a recorded "
      f"deadlock becomes an operator decision. "
      f"{len(active)} lane(s) active, {len(open_q)} peer request(s) unanswered.</div></div>")
    A("</div>")

    A("<h2>Canonical path and next action</h2><div class='panel'><div class='flow'>")
    flow = [
        ("1", "Correct executor", "active", "Make canonical evaluation accurate and fast."),
        ("2", "One-cell baseline", "active", "Measure end to end; no universe extrapolation yet."),
        ("3", "Full run", "", "Complete the canonical 4-anchor/current-refit path."),
        ("4", "Replay + Test A", "", "Reject-only retrospective falsification."),
        ("5", "Demo shadow", "", "Broker-reconciled operational probation."),
        ("6", "Test B", "blocked", "Only prospective evidence can support certification."),
    ]
    for number, name, cls, why in flow:
        A(f"<div class='step {cls}'><div class='num'>{number}</div><div class='name'>{esc(name)}</div>"
          f"<div class='why'>{esc(why)}</div></div>")
    A("</div><div class='note'><b>Current next action (XPM-030):</b> rebaseline the "
      "complete canonical fixed-cost path after E23/E24 before authorising another "
      "optimization. The cost-stress repair remains a separate PM-gated correctness "
      "decision; no Gate-2 rerun is authorised.</div></div>")

    A("<h2>Deliverables and preserved lineages</h2><div class='split'><div class='panel'>")
    A(f"<div class='kpi' style='border:0;padding:0'><div class='n'>{reports['verified']}/{reports['total'] or 4}</div>"
      "<div class='l'>Audit reports verified</div>"
      "<div class='d'>Reconciliation, architecture/capabilities, quant/risk, and "
      "production-readiness HTML reports. These are dated audit snapshots, not live status.</div></div></div>")
    A("<div class='panel wrap'><table><tr><th>Lineage</th><th>State</th><th>Evidence</th></tr>")
    for lineage in lineages:
        A(f"<tr><td><b>{esc(lineage['lineage'])}</b></td><td>{esc(lineage['state'])}</td>"
          f"<td class='mono'>{esc(lineage['evidence'])}</td></tr>")
    A("</table></div></div>")

    A("<h2>Recent experiments and next gate</h2><div class='panel wrap'><table>"
      "<tr><th>Experiment</th><th>Measured outcome</th><th>Decision</th></tr>"
      "<tr><td><b>E23 · native replay</b></td><td>17 → 2 full fits; fifteen held "
      "checks took 0.559s versus about 239s of discarded refits; substitution and "
      "pre-evaluation rejection controls passed.</td><td>Retained · 79303f8</td></tr>"
      f"<tr><td><b>E24 · duplicate publication</b></td><td>Exact 1,211-entry reopen "
      f"{E24_REOPEN_BEFORE_SECONDS:.3f}s → {E24_REOPEN_AFTER_SECONDS:.3f}s "
      f"(-{improvement_pct(E24_REOPEN_BEFORE_SECONDS, E24_REOPEN_AFTER_SECONDS):.1f}%); "
      "identical entries and fail-closed CAS checks.</td><td>Retained · 38e5022</td></tr>"
      "<tr><td><b>Next gate</b></td><td>Measure the complete canonical fixed-cost "
      "path on current HEAD; identify one dominant remaining phase before any new "
      "change.</td><td>No implementation authorised</td></tr>"
      "</table><div class='note'>No experiment changed the roster, folds, gates, search "
      "scope or economic thresholds. Isolated gains are not a universe ETA.</div></div>")

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
          f"<td>{esc(g['owner'])}</td><td><span class='tag {gate_class(g['state'], g['owner'])}'>"
          f"{esc(truncate(g['state'], 95))}</span></td></tr>")
    A("</table></div>")

    A("<h2>Measured efficiency</h2><div class='split'><div class='panel'>")
    A(svg_fold_performance())
    A(f"<div class='note'>One exact preserved EURUSD/H1 attempt-2/fold-0 path. Cumulative "
      f"gain: {fold_gain:.1f}%. No search, fold, gate or roster change.</div></div>")
    A("<div class='panel'>")
    A(svg_startup_breakdown())
    A("<div class='note'>This chart is the preserved pre-E23 baseline, not current "
      "runtime. E23 removed the repeated-refit mechanism; the complete path must now "
      "be measured again before another optimization is selected.</div>")
    A("<div class='note'>Canonical count ceiling: one 2,400-cell grid has at most "
      f"{SINGLE_GRID_MAX_RUNS/1_000_000:.2f}M Backtester.run calls, 2.88M stress repricings "
      "and 720k strategy surfaces. The complete multi-grid generation total and duration "
      "are not yet established. This is a workload bound, not a duration forecast."
      "</div></div></div>")

    A("<h2>Open PM-to-PM requests</h2>")
    if open_q:
        A("<div class='panel'>")
        for m in open_q[-10:][::-1]:
            A(f"<div class='msg'><div class='h'><span class='mono'>{esc(m.get('id'))}</span> · "
              f"<span class='who'>{esc(m.get('from'))}</span> <span class='arrow'>&rarr;</span> "
              f"<span class='who'>{esc(m.get('to'))}</span> · {esc(m.get('type'))} · "
              f"{esc(age(m.get('ts')))}</div>"
              f"<div class='b'>{esc(truncate(m.get('body'), 300))}</div></div>")
        A("</div>")
    else:
        A("<div class='panel empty'>Nothing outstanding.</div>")

    A("<h2>PM continuity</h2><div class='panel wrap'><table>"
      "<tr><th>PM</th><th>Latest</th><th>Type</th><th>Age</th><th>Bus messages</th></tr>")
    for row in pm_rows:
        A(f"<tr><td><b>{esc(row['actor'])}</b></td>"
          f"<td class='mono'>{esc(row['latest_id'])}</td>"
          f"<td>{esc(row['latest_type'])}</td>"
          f"<td>{esc(age(row['latest_ts']))}</td>"
          f"<td>{row['message_count']}</td></tr>")
    A("</table><div class='note'>The PM channel remains active even when operator-facing "
      "updates are sparse. “Open” here means a PM question, veto or review request with "
      "no reciprocal, chronological reference.</div></div>")

    A("<h2>Risk register</h2><div class='panel wrap'><table>"
      "<tr><th>Risk</th><th>Severity</th><th>Detail</th></tr>")
    for name, sev, detail in RISKS:
        cls = "bad" if sev == "critical" else "warn"
        A(f"<tr><td><b>{esc(name)}</b></td>"
          f"<td><span class='tag {cls}'>{esc(sev)}</span></td>"
          f"<td>{esc(detail)}</td></tr>")
    A("</table></div>")

    A("<h2>Lane ledger</h2><div class='panel wrap'><table>"
      "<tr><th>Lane</th><th>Owner</th><th>Status</th><th>Purpose</th><th>Claimed</th></tr>")
    for l in lanes[:12]:
        cls = "warn" if l.get("status") == "active" else "idle"
        A(f"<tr><td class='mono'>{esc(l.get('id'))}</td><td>{esc(l.get('owner'))}</td>"
          f"<td><span class='tag {cls}'>{esc(l.get('status'))}</span></td>"
          f"<td>{esc(truncate(l.get('purpose'), 175))}</td>"
          f"<td class='mono'>{esc(age(l.get('claimed_at')))}</td></tr>")
    A("</table></div>")

    A("<h2>Source health</h2><div class='panel wrap'><table>"
      "<tr><th>Source</th><th>State</th><th>Updated UTC</th><th>Age</th></tr>")
    for source in sources:
        updated = source["updated"]
        A(f"<tr><td class='mono'>{esc(source['name'])}</td>"
          f"<td><span class='tag {'ok' if source['state'] == 'ok' else 'bad'}'>"
          f"{esc(source['state'])}</span></td>"
          f"<td class='mono'>{updated.strftime('%Y-%m-%d %H:%M') if updated else '—'}</td>"
          f"<td>{esc(age(updated.isoformat() if updated else None))}</td></tr>")
    A("</table>")
    if oldest_source:
        A(f"<div class='note'>Oldest controlling source: {esc(age(oldest_source.isoformat()))}. "
          "A fresh page proves the files were reread; it does not prove every source was "
          "recently maintained.</div>")
    A("</div>")

    A("<h2>Agent bus — latest</h2><div class='panel'>")
    for m in bus[-12:][::-1]:
        A(f"<div class='msg'><div class='h'><span class='mono'>{esc(m.get('id'))}</span> · "
          f"<span class='who'>{esc(m.get('from'))}</span> <span class='arrow'>&rarr;</span> "
          f"<span class='who'>{esc(m.get('to'))}</span> · {esc(m.get('type'))} · "
          f"{esc(age(m.get('ts')))}</div>"
          f"<div class='b'>{esc(truncate(m.get('body')))}</div></div>")
    A("</div>")

    if register:
        A("<h2>Reconciliation register — unresolved lineage</h2><div class='panel wrap'><table>"
          "<tr><th>ID</th><th>Element</th><th>Decision</th></tr>")
        ordered = sorted(register, key=lambda e: "pending" not in e["state"].lower())
        for e in ordered:
            cls = "warn" if "pending" in e["state"].lower() else "ok"
            A(f"<tr><td class='mono'>{esc(e['id'])}</td>"
              f"<td>{esc(truncate(e['what'], 130))}</td>"
              f"<td><span class='tag {cls}'>{esc(truncate(e['state'], 60))}</span></td></tr>")
        A(f"</table></div><div class='note'>{len(pending)} of {len(register)} elements are "
          f"still open. These are unresolved lineage calls from earlier FxPM iterations. "
          f"PMs may decide within the charter; only a genuine deadlock is escalated. Detail in "
          f"docs/RECONCILIATION_REGISTER.md.</div>")

    A("<h2>Recent commits</h2><div class='panel wrap'><table>"
      "<tr><th>SHA</th><th>When</th><th>Author</th><th>Subject</th></tr>")
    for c in commits:
        A(f"<tr><td class='mono'>{esc(c['sha'])}</td><td class='mono'>{esc(c['date'])}</td>"
          f"<td>{esc(c['author'])}</td><td>{esc(c['subject'])}</td></tr>")
    A("</table></div>")

    A("<div class='foot'>Rebuilt on every live request from agent_comms/BUS*.jsonl, "
      "agent_comms/LANES.json, agent_comms/STATE.md, docs/RECONCILIATION_REGISTER.md and "
      "git log. Gates, lanes, communication and commits are source-derived; declared "
      "timeline milestones and measured benchmark constants live in tools/pm_dashboard.py "
      "and are labelled as such. Efficiency corrections are recorded in CX-067 and "
      "XPM-019; docs/EFFICIENCY_FINDINGS_2026-08-11.md is a superseded reference report."
      "</div></body></html>")
    return "\n".join(p)


# ---------------------------------------------------------------------- server

class Handler(BaseHTTPRequestHandler):
    def do_GET(self):  # noqa: N802
        path = urlparse(self.path).path
        if path == "/healthz":
            snapshot = service_snapshot()
            body = _canonical_json_bytes(snapshot)
            self.send_response(200 if snapshot["status"] == "ok" else 503)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(body)
            return
        if path == "/api/status":
            body = _canonical_json_bytes(service_snapshot())
            self.send_response(200)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(body)
            return
        if path not in ("/", "/index.html"):
            self.send_error(404)
            return
        status = 200
        try:
            body = render(live=True).encode("utf-8")
        except Exception as exc:  # keep the service up; show the failure
            status = 500
            body = (f"<pre>dashboard build failed:\n{html.escape(repr(exc))}</pre>"
                    ).encode("utf-8")
        self.send_response(status)
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


def _canonical_json_bytes(value: dict) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("mode", nargs="?", default="serve", choices=("serve", "build"))
    ap.add_argument("--port", type=int, default=8787)
    ap.add_argument("--open", action="store_true")
    args = ap.parse_args()
    if args.mode == "build":
        SNAPSHOT.write_text(render(live=False), encoding="utf-8")
        print(f"wrote {SNAPSHOT}")
        if args.open:
            webbrowser.open(SNAPSHOT.as_uri())
        return 0
    return serve(args.port, args.open)


if __name__ == "__main__":
    sys.exit(main())
