"""Render audit Markdown reports to standalone HTML.

Uses the local Python-Markdown package with table support, so generated reports
open as real HTML rather than preformatted Markdown text.
"""

from __future__ import annotations

import html
from pathlib import Path

import markdown


REPO = Path(__file__).resolve().parents[2]
REPORTS = REPO / "agent_audit" / "reports"
PERFORMANCE_REPORT = REPO / "agent_audit" / "performance" / "FINAL_PERFORMANCE_REPORT.md"


STYLE = """
:root {
  color-scheme: light;
  --bg: #f4f6f8;
  --paper: #ffffff;
  --ink: #111827;
  --muted: #4b5563;
  --line: #d9dee8;
  --soft: #f8fafc;
  --accent: #1f6feb;
}
* {
  box-sizing: border-box;
}
body {
  margin: 0;
  background: var(--bg);
  color: var(--ink);
  font-family: "Segoe UI", Inter, Arial, sans-serif;
  line-height: 1.58;
}
main {
  max-width: 1240px;
  margin: 0 auto;
  padding: 32px 24px 64px;
}
article {
  background: var(--paper);
  border: 1px solid var(--line);
  border-radius: 8px;
  padding: 34px;
  box-shadow: 0 10px 28px rgba(15, 23, 42, 0.08);
  overflow-x: auto;
}
h1, h2, h3, h4 {
  line-height: 1.22;
  margin: 1.5em 0 0.55em;
}
h1 {
  margin-top: 0;
  font-size: 2rem;
}
h2 {
  border-top: 1px solid var(--line);
  padding-top: 1.1em;
  font-size: 1.45rem;
}
h3 {
  font-size: 1.13rem;
}
p {
  margin: 0.75em 0;
}
ul, ol {
  padding-left: 1.45rem;
}
li + li {
  margin-top: 0.25rem;
}
table {
  width: 100%;
  border-collapse: collapse;
  margin: 1rem 0 1.35rem;
  font-size: 0.91rem;
}
th, td {
  border: 1px solid var(--line);
  padding: 8px 10px;
  text-align: left;
  vertical-align: top;
}
th {
  background: #eef3f8;
  font-weight: 650;
}
tr:nth-child(even) td {
  background: var(--soft);
}
code {
  background: #edf2f7;
  border: 1px solid #d8e0ea;
  border-radius: 4px;
  padding: 0.08rem 0.28rem;
  font-family: Consolas, "SFMono-Regular", Menlo, monospace;
  font-size: 0.92em;
}
pre {
  overflow-x: auto;
  background: #0f172a;
  color: #e5e7eb;
  padding: 14px 16px;
  border-radius: 6px;
}
pre code {
  background: transparent;
  border: 0;
  color: inherit;
  padding: 0;
}
a {
  color: var(--accent);
}
blockquote {
  border-left: 4px solid var(--line);
  color: var(--muted);
  margin: 1rem 0;
  padding-left: 1rem;
}
"""


def _title_from_markdown(text: str, fallback: str) -> str:
    for line in text.splitlines():
        stripped = line.strip()
        if stripped.startswith("# "):
            return stripped[2:].strip()
    return fallback


def render(markdown_path: Path) -> Path:
    text = markdown_path.read_text(encoding="utf-8")
    title = _title_from_markdown(text, markdown_path.stem.replace("_", " ").title())
    body = markdown.markdown(
        text,
        extensions=["tables", "fenced_code", "sane_lists"],
        output_format="html5",
    )
    html_text = f"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>{html.escape(title)}</title>
<style>{STYLE}</style>
</head>
<body>
<main>
<article>
{body}
</article>
</main>
</body>
</html>
"""
    out = markdown_path.with_suffix(".html")
    out.write_text(html_text, encoding="utf-8")
    return out


def main() -> int:
    for markdown_path in sorted(REPORTS.glob("*.md")):
        out = render(markdown_path)
        print(out)
    if PERFORMANCE_REPORT.exists():
        out = render(PERFORMANCE_REPORT)
        print(out)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
