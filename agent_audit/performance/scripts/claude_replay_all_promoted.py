"""Corrected promoted-winner replay (Claude review lane).

This driver replays the canonical logical live book enumerated by
run_promoted_cell_replay.iter_promoted_cells. That enumerator now includes
symbol-level promoted winners and de-duplicates matching promoted regime leaves.
Read-only; writes one CSV.
"""
from __future__ import annotations

import csv
import json
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPO / "agent_audit" / "performance" / "scripts"))
sys.path.insert(0, str(REPO))

from run_promoted_cell_replay import _pipeline_config, iter_promoted_cells, replay_cell  # type: ignore
from pm_core import DataLoader  # type: ignore

LEDGER = REPO / "pm_configs.json"
OUT = REPO / "agent_audit" / "performance" / "data" / "promoted_winner_replay_all_10000.csv"
def main() -> int:
    ledger = json.loads(LEDGER.read_text(encoding="utf-8-sig"))
    cells = list(iter_promoted_cells(ledger))
    print(f"logical promoted winners: {len(cells)}")
    cfg = _pipeline_config(REPO / "config.json", 10000.0)
    loader = DataLoader(cfg.data_dir)
    rows = []
    for i, cell in enumerate(cells, 1):
        row = replay_cell(cfg, loader, cell)
        row["promotion_status"] = cell["promotion_status"]
        rows.append(row)
        print(f"[{i}/{len(cells)}] {cell['symbol']:8} {cell['timeframe']:4} "
              f"{str(cell['strategy_name'])[:26]:26} -> net={row.get('net_profit')} "
              f"pf={row.get('profit_factor')} err={row.get('error','')}")
    OUT.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = sorted({k for r in rows for k in r})
    with OUT.open("w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(rows)
    print(f"wrote {len(rows)} rows -> {OUT}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
