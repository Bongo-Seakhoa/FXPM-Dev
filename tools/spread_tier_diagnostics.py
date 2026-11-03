"""Cost lane diagnostics: where the frozen tiers sit against the spread a trade faces.

Operator ruling 2026-11-03 ("A now"): the percentile tiers and the frozen profile
stay as they are; the lanes are presented as BASE (P75) and STRESS (P95); and
every cell gets two plain diagnostics, the tier divided by the mean spread and
the tier's distance from the mean in standard deviations.  This tool computes
them from the native bars, with the protected cutoff applied before any
statistic, and writes one compact JSON.  It edits nothing and decides nothing.

Spread here is the bar's Spread field in MT5 points: the minimum spread inside
the bar, not an executable quote.  The frozen profile is computed from the exact
prior M5 close instead, so its tiers differ from the bar-level P75/P95 below;
both are reported side by side so the difference is visible, never hidden.
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
import math
from pathlib import Path
import sys
from typing import Any, Iterable

import numpy as np
import pandas as pd


SCHEMA_VERSION = "fxpm.spread-tier-diagnostics.v1"
QUANTILE_METHOD = "higher_order_statistic_ceil((n-1)*p)"
LANE_NAMES = {"P75": "BASE", "P95": "STRESS"}


class SpreadDiagnosticsError(ValueError):
    """A missing or malformed input; never a cost verdict."""


def _utc(value: Any) -> pd.Timestamp:
    stamp = pd.Timestamp(value)
    if stamp.tzinfo is None or str(stamp.tz) != "UTC":
        raise SpreadDiagnosticsError("CUTOFF_MUST_BE_UTC")
    return stamp


def quantile_higher(values: np.ndarray, p: float) -> float:
    """The profile's rule: the order statistic at ceil((n-1)*p), zero-based."""
    if values.size == 0:
        raise SpreadDiagnosticsError("EMPTY_SAMPLE")
    ordered = np.sort(values)
    index = int(math.ceil((ordered.size - 1) * p))
    return float(ordered[index])


def spread_statistics(spreads: np.ndarray) -> dict[str, Any]:
    """Descriptive figures for one cell; the sample is already pre-cutoff."""
    s = np.asarray(spreads, dtype=np.float64)
    if s.size < 2 or not np.isfinite(s).all() or (s < 0).any():
        raise SpreadDiagnosticsError("SPREAD_SAMPLE_INVALID")
    mean = float(s.mean())
    sd = float(s.std(ddof=1))
    skew = float(((s - mean) ** 3).mean() / sd ** 3) if sd > 0 else None
    out: dict[str, Any] = {
        "rows": int(s.size), "zero_share": float((s == 0).mean()),
        "mean": mean, "sd": sd, "skew": skew, "quantile_method": QUANTILE_METHOD,
        "tiers": {},
    }
    for tier, p in (("P75", 0.75), ("P95", 0.95)):
        value = quantile_higher(s, p)
        out["tiers"][tier] = {
            "lane": LANE_NAMES[tier], "spread_points": value,
            "share_at_or_below": float((s <= value).mean()),
            "tier_over_mean": (value / mean) if mean > 0 else None,
            "sd_distance": ((value - mean) / sd) if sd > 0 else None,
        }
    return out


def _profile_cells(profile_path: Path | None) -> dict[tuple[str, str], dict[str, Any]]:
    if profile_path is None:
        return {}
    payload = json.loads(profile_path.read_text(encoding="utf-8"))
    cells = {}
    for cell in payload.get("cells", []):
        cells[(cell["symbol"], cell["timeframe"])] = cell
    return cells


def _frozen_tiers(cell: dict[str, Any] | None, mean: float, sd: float) -> dict[str, Any] | None:
    """The frozen profile's own tiers against the same bar-level mean and SD."""
    if not cell or not cell.get("tiers"):
        return None
    out: dict[str, Any] = {"status": cell.get("status"), "source_timeframe": cell.get("source_timeframe")}
    for tier, body in cell["tiers"].items():
        points = body.get("spread_points")
        if type(points) not in (int, float):
            continue
        out[tier] = {
            "lane": LANE_NAMES.get(tier, tier), "spread_points": float(points),
            "usability": body.get("usability"),
            "share_at_or_below_in_profile": body.get("share_at_or_below"),
            "tier_over_bar_mean": (float(points) / mean) if mean > 0 else None,
            "sd_distance_from_bar_mean": ((float(points) - mean) / sd) if sd > 0 else None,
        }
    return out


def diagnose_cell(bars_path: Path, cutoff: pd.Timestamp, *, symbol: str, timeframe: str,
                  profile_cell: dict[str, Any] | None = None) -> dict[str, Any]:
    if not bars_path.is_file():
        raise SpreadDiagnosticsError(f"BARS_MISSING:{symbol}:{timeframe}")
    frame = pd.read_csv(bars_path, usecols=["time", "Spread"])
    stamps = pd.to_datetime(frame["time"], utc=True)
    kept = frame.loc[stamps <= cutoff, "Spread"].to_numpy(dtype=np.float64)
    stats = spread_statistics(kept)
    stats.update({
        "symbol": symbol, "timeframe": timeframe,
        "rows_total": int(len(frame)), "rows_after_cutoff_excluded": int(len(frame) - kept.size),
        "bars_relative_path": str(bars_path.as_posix()),
        "frozen_profile": _frozen_tiers(profile_cell, stats["mean"], stats["sd"]),
    })
    return stats


def diagnose(native_root: Path, run_id: str, cells: Iterable[tuple[str, str]], cutoff_utc: Any,
             *, profile_path: Path | None = None, activation_id: str | None = None) -> dict[str, Any]:
    cutoff = _utc(cutoff_utc)
    profile = _profile_cells(profile_path)
    results, skipped = [], []
    for symbol, timeframe in cells:
        path = native_root / "runs" / run_id / symbol / timeframe / "bars.csv"
        try:
            results.append(diagnose_cell(path, cutoff, symbol=symbol, timeframe=timeframe,
                                         profile_cell=profile.get((symbol, timeframe))))
        except SpreadDiagnosticsError as error:
            skipped.append({"symbol": symbol, "timeframe": timeframe, "reason": str(error)})
    tool_sha = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
    return {
        "schema_version": SCHEMA_VERSION, "classification": "DESCRIPTIVE_DIAGNOSTIC_ONLY",
        "spread_semantics": "bar_minimum_spread_in_MT5_points_not_execution_quote",
        "native_root": str(native_root), "run_id": run_id, "activation_id": activation_id,
        "protected_cutoff_utc": cutoff.isoformat(), "profile_path": None if profile_path is None else str(profile_path),
        "tool_sha256": tool_sha, "computed_at_utc": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        "cells": results, "skipped": skipped,
    }


def _parse_cells(text: str) -> list[tuple[str, str]]:
    cells = []
    for item in text.split(","):
        item = item.strip()
        if not item:
            continue
        symbol, _, timeframe = item.partition("/")
        if not symbol or not timeframe:
            raise SpreadDiagnosticsError(f"CELL_SPEC_INVALID:{item}")
        cells.append((symbol, timeframe))
    return cells


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--native-root", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--cells", required=True, help="comma list of SYMBOL/TF")
    parser.add_argument("--cutoff-utc", required=True)
    parser.add_argument("--profile", default=None)
    parser.add_argument("--activation-id", default=None)
    parser.add_argument("--out", required=True)
    args = parser.parse_args(argv)
    payload = diagnose(Path(args.native_root), args.run_id, _parse_cells(args.cells), args.cutoff_utc,
                       profile_path=None if args.profile is None else Path(args.profile),
                       activation_id=args.activation_id)
    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    raw = json.dumps(payload, sort_keys=True, separators=(",", ":"), allow_nan=False).encode("ascii")
    with out.open("xb") as handle:
        handle.write(raw)
    print(f"{out} {len(raw)} bytes sha256 {hashlib.sha256(raw).hexdigest()}")
    for cell in payload["cells"]:
        base, stress = cell["tiers"]["P75"], cell["tiers"]["P95"]
        print(f"{cell['symbol']:10s} {cell['timeframe']:3s} rows={cell['rows']:7d} zero={cell['zero_share']:.2f} "
              f"mean={cell['mean']:.1f} sd={cell['sd']:.1f} BASE={base['spread_points']:.0f} "
              f"x{(base['tier_over_mean'] or 0):.2f} {(base['sd_distance'] or 0):+.2f}sd "
              f"STRESS={stress['spread_points']:.0f} x{(stress['tier_over_mean'] or 0):.2f} "
              f"{(stress['sd_distance'] or 0):+.2f}sd")
    for item in payload["skipped"]:
        print(f"skipped {item['symbol']}/{item['timeframe']}: {item['reason']}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
