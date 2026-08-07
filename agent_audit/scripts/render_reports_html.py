#!/usr/bin/env python3
"""Render audit report Markdown into properly-rendered standalone HTML.

The earlier HTML files wrapped raw Markdown in a <pre> block, so they displayed
literal '#', '##', and '|' source text instead of rendered headings and tables.
This converter uses the `markdown` package (tables + fenced_code + sane_lists) to
produce real HTML. It is read-only with respect to the source .md files; it only
(re)writes the matching .html files in agent_audit/reports/.

Run:  python agent_audit/scripts/render_reports_html.py
"""
from pathlib import Path

import markdown

REPORTS = Path(__file__).resolve().parents[1] / "reports"

# (source_md, output_html, page_title)
PAIRS = [
    ("STRATEGIC_AUDIT.md", "STRATEGIC_AUDIT.html", "FxPM Strategic Audit"),
    ("TECHNICAL_AUDIT.md", "TECHNICAL_AUDIT.html", "FxPM Technical Audit"),
]

CSS = """
  :root { color-scheme: light; }
  body { font: 15px/1.55 -apple-system, "Segoe UI", Roboto, Helvetica, Arial, sans-serif;
         color: #1a1f29; background: #f6f7f9; margin: 0; }
  main { max-width: 1100px; margin: 0 auto; padding: 28px 22px 64px; }
  article { background: #fff; border: 1px solid #d9dde5; border-radius: 8px;
            padding: 30px 34px; box-shadow: 0 8px 24px rgba(17,24,39,.07); }
  h1 { font-size: 1.7em; border-bottom: 3px solid #b00020; padding-bottom: 7px; margin-top: 0; }
  h2 { font-size: 1.3em; margin-top: 1.9em; border-bottom: 1px solid #e2e5ea; padding-bottom: 4px; }
  h3 { font-size: 1.08em; margin-top: 1.4em; color: #2b3442; }
  h4 { font-size: 1em; margin-top: 1.1em; color: #2b3442; }
  p, li { overflow-wrap: anywhere; }
  table { border-collapse: collapse; width: 100%; margin: 14px 0; font-size: 12.8px; }
  th, td { border: 1px solid #cfd4dc; padding: 6px 9px; text-align: left; vertical-align: top; }
  th { background: #eef1f5; font-weight: 600; }
  tbody tr:nth-child(even) { background: #fafbfc; }
  code { background: #eef1f5; padding: 1px 5px; border-radius: 4px;
         font: 12.5px/1.4 Consolas, "SF Mono", Menlo, monospace; }
  pre { background: #f4f6f9; border: 1px solid #e2e5ea; border-radius: 6px; padding: 12px 14px;
        overflow-x: auto; }
  pre code { background: none; padding: 0; }
  blockquote { margin: 14px 0; padding: 8px 16px; border-left: 4px solid #b00020;
               background: #fff7f7; color: #444; }
  a { color: #1f6feb; }
  hr { border: 0; border-top: 1px solid #e2e5ea; margin: 24px 0; }
  .render-note { color: #6b7280; font-size: 12px; margin: 0 0 18px; }
"""

HEAD_OPEN = (
    "<!doctype html>\n<html lang=\"en\">\n<head>\n<meta charset=\"utf-8\">\n"
    "<meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">\n"
    "<title>"
)
HEAD_MID = "</title>\n<style>"
HEAD_CLOSE = "</style>\n</head>\n<body>\n<main>\n<article>\n"
FOOT = "\n</article>\n</main>\n</body>\n</html>\n"


def build_page(title: str, body_html: str) -> str:
    note = ('<p class="render-note">Rendered from the canonical Markdown source by '
            'render_reports_html.py.</p>\n')
    return HEAD_OPEN + title + HEAD_MID + CSS + HEAD_CLOSE + note + body_html + FOOT


def main() -> int:
    md = markdown.Markdown(
        extensions=["tables", "fenced_code", "sane_lists", "attr_list", "toc"]
    )
    wrote = 0
    for src, dst, title in PAIRS:
        src_path = REPORTS / src
        if not src_path.exists():
            print(f"skip (missing): {src}")
            continue
        md.reset()
        body = md.convert(src_path.read_text(encoding="utf-8"))
        (REPORTS / dst).write_text(build_page(title, body), encoding="utf-8")
        print(f"wrote {dst}  ({len(body)} chars of rendered HTML)")
        wrote += 1
    print(f"done: {wrote} file(s)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
