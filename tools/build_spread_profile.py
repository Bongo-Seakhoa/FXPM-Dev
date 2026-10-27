"""Offline empirical spread statistics for the frozen SymbolTF calibration."""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime, timezone
import hashlib
import io
import json
import math
from numbers import Real
import os
from pathlib import Path
import sys
from typing import Iterable

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CUTOFF = "2024-07-14T08:05:00Z"


@dataclass
class SpreadDistribution:
    """Keep discrete source points, including zeros, without retaining bar data."""

    frequencies: Counter[int] = field(default_factory=Counter)
    rows: int = 0
    invalid_nonfinite: int = 0
    invalid_negative: int = 0
    invalid_fractional: int = 0

    def add(self, values: Iterable[float]) -> None:
        values = np.asarray(values, dtype=np.float64)
        if values.ndim != 1:
            raise ValueError("SPREAD_VALUES_MUST_BE_ONE_DIMENSIONAL")
        self.rows += int(values.size)
        finite = np.isfinite(values)
        nonnegative = finite & (values >= 0)
        integral = nonnegative & (values == np.floor(values))
        self.invalid_nonfinite += int((~finite).sum())
        self.invalid_negative += int((finite & (values < 0)).sum())
        self.invalid_fractional += int((nonnegative & ~integral).sum())
        points, counts = np.unique(values[integral], return_counts=True)
        self.frequencies.update({int(p): int(n) for p, n in zip(points, counts)})

    def snapshot(self, point: float) -> dict:
        if isinstance(point, bool) or not isinstance(point, Real):
            raise ValueError("SOURCE_POINT_MUST_BE_POSITIVE_FINITE")
        point = float(point)
        if not math.isfinite(point) or point <= 0:
            raise ValueError("SOURCE_POINT_MUST_BE_POSITIVE_FINITE")
        n = sum(self.frequencies.values())
        invalid = self.rows - n
        result = {
            "quantile_method": "higher_order_statistic_ceil((n-1)*p)",
            "spread_unit": "MT5_points",
            "point_price": point,
            "rows": self.rows,
            "valid_rows": n,
            "zero_rows": self.frequencies[0],
            "invalid_rows": invalid,
            "invalid_nonfinite": self.invalid_nonfinite,
            "invalid_negative": self.invalid_negative,
            "invalid_fractional": self.invalid_fractional,
            "zero_fraction_of_valid": self.frequencies[0] / n if n else None,
            "status": "CALCULATED" if n else "NO_VALID_SPREAD_OBSERVATIONS",
            "tiers": {},
        }
        if not n:
            return result
        ordered = sorted(self.frequencies.items())
        for percentile in (75, 95):
            # Integer ceiling avoids floating rank errors on large samples.
            rank = ((n - 1) * percentile + 99) // 100
            cumulative = 0
            for points, count in ordered:
                cumulative += count
                if cumulative > rank:
                    break
            full_price = points * point
            if not math.isfinite(full_price):
                raise ValueError("SPREAD_PRICE_CONVERSION_NONFINITE")
            result["tiers"][f"P{percentile}"] = {
                "spread_points": points,
                "full_spread_price": full_price,
                "half_spread_price": full_price / 2,
                "share_at_or_below": cumulative / n,
                "usability": "POSITIVE_QUANTILE" if points else "UNUSABLE_ZERO_QUANTILE",
            }
        return result


class _DigestReader(io.RawIOBase):
    def __init__(self, handle):
        self.handle = handle
        self.digest = hashlib.sha256()
        self.bytes_read = 0

    def readable(self):
        return True

    def readinto(self, buffer):
        n = self.handle.readinto(buffer)
        if n:
            self.digest.update(memoryview(buffer)[:n])
            self.bytes_read += n
        return n


def _utc(value) -> pd.Timestamp:
    result = pd.Timestamp(value)
    if pd.isna(result) or result.tzinfo is None or result.utcoffset().total_seconds() != 0:
        raise ValueError("EXPLICIT_UTC_TIMESTAMP_REQUIRED")
    return result


def _bound_json(path: Path, expected_sha: str | None = None) -> tuple[dict, str]:
    raw = path.read_bytes()
    digest = hashlib.sha256(raw).hexdigest()
    if expected_sha is not None and digest != expected_sha:
        raise ValueError("METADATA_DIGEST_MISMATCH")
    result = json.loads(raw)
    if not isinstance(result, dict):
        raise ValueError("METADATA_MAPPING_REQUIRED")
    return result, digest


def _source_metadata(native: Path, entry: dict, activation_id: str, run: dict) -> dict:
    from pm_native_activation import _contained_path
    from pm_native_request_contract import canonical_source_identity, TIMEFRAME_SECONDS

    symbol, tf = entry["symbol"], entry["timeframe"]
    paths = {}
    for name in ("bars", "sidecar"):
        path = _contained_path(native, entry.get(f"{name}_relative_path"))
        if path is None or not path.is_file():
            raise ValueError("SOURCE_PATH_MISSING_OR_UNSAFE")
        paths[name] = path
    sidecar, _ = _bound_json(paths["sidecar"], entry["sidecar_sha256"])
    prov_path = _contained_path(native, sidecar.get("source_provenance_relative_path"))
    if prov_path is None or not prov_path.is_file():
        raise ValueError("PROVENANCE_PATH_MISSING_OR_UNSAFE")
    provenance, _ = _bound_json(prov_path, sidecar["source_provenance_sha256"])
    if any(sidecar.get(k) != v for k, v in {
        "activation_id": activation_id, "symbol": symbol, "timeframe": tf,
        "sha256": entry["bars_sha256"], "bar_close_column": "_BAR_CLOSE_TS",
        "timezone": "UTC", "timestamp_semantics": "bar_open_utc",
    }.items()):
        raise ValueError("SIDECAR_IDENTITY_MISMATCH")
    if any(provenance.get(k) != v for k, v in {
        "canonical_symbol": symbol, "broker_symbol": symbol, "timeframe": tf,
        "source_kind": "broker_native", "timezone": "UTC",
        "timestamp_semantics": "bar_open_utc", "bar_close_column": "_BAR_CLOSE_TS",
        "timeframe_seconds": TIMEFRAME_SECONDS[tf], "sha256": entry["bars_sha256"],
    }.items()):
        raise ValueError("PROVENANCE_IDENTITY_MISMATCH")
    artifact = provenance["artifact"]
    if artifact.get("sha256") != entry["bars_sha256"] or artifact.get("relative_path") != "bars.csv":
        raise ValueError("ARTIFACT_IDENTITY_MISMATCH")
    if artifact.get("bytes") != paths["bars"].stat().st_size:
        raise ValueError("ARTIFACT_SIZE_MISMATCH")
    instrument = provenance["instrument"]
    point = instrument.get("point")
    SpreadDistribution().snapshot(point)
    if instrument.get("status") != "available" or instrument.get("symbol") != symbol:
        raise ValueError("INSTRUMENT_IDENTITY_UNAVAILABLE")
    identity = canonical_source_identity(provenance["source_environment"])
    if identity != canonical_source_identity(run["source_environment"]):
        raise ValueError("RUN_BROKER_IDENTITY_MISMATCH")
    boundaries = [
        sidecar["cell_calendar_boundaries"]["final_lockbox"]["start_ts"],
        provenance["request"]["cell_calendar_boundaries"]["final_lockbox"]["start_ts"],
        run["request"]["timeframe_calendar_boundaries"][tf]["final_lockbox"]["start_ts"],
    ]
    cutoffs = [_utc(v) for v in boundaries]
    if len(set(cutoffs)) != 1:
        raise ValueError("PROTECTED_CUTOFF_BINDING_MISMATCH")
    return {
        "path": paths["bars"], "sha256": entry["bars_sha256"],
        "expected_rows": artifact["rows"], "expected_bytes": artifact["bytes"],
        "point": point, "digits": instrument.get("digits"),
        "source_identity": identity, "protected_cutoff": cutoffs[0],
        "sidecar_relative_path": entry["sidecar_relative_path"],
        "sidecar_sha256": entry["sidecar_sha256"],
        "provenance_relative_path": sidecar["source_provenance_relative_path"],
        "provenance_sha256": sidecar["source_provenance_sha256"],
        "symbol": symbol, "timeframe": tf,
    }


def read_bound_prefix(meta: dict, cutoff: pd.Timestamp, *, with_spreads: bool) -> dict:
    """Scan once, hashing the same bytes, and retain only allowed scalar axes."""
    if cutoff > meta["protected_cutoff"]:
        raise ValueError("REQUESTED_CUTOFF_EXCEEDS_PROTECTED_BOUNDARY")
    path = meta["path"]
    before = path.stat()
    columns = ["time", "_BAR_CLOSE_TS"] + (["Spread"] if with_spreads else [])
    opens, closes, spread_values = [], [], []
    rows = 0
    previous_open = previous_close = None
    with path.open("rb") as handle:
        reader = _DigestReader(handle)
        with io.BufferedReader(reader) as stream:
            for chunk in pd.read_csv(stream, usecols=columns, chunksize=100_000):
                o = pd.DatetimeIndex(pd.to_datetime(chunk["time"], utc=True, format="mixed", errors="raise"))
                c = pd.DatetimeIndex(pd.to_datetime(chunk["_BAR_CLOSE_TS"], utc=True, format="mixed", errors="raise"))
                explicit_utc = all(
                    chunk[column].astype("string").str.endswith(("Z", "+00:00", "+0000")).all()
                    for column in ("time", "_BAR_CLOSE_TS")
                )
                if (
                    not explicit_utc or o.isna().any() or c.isna().any() or o.has_duplicates or c.has_duplicates
                    or not o.is_monotonic_increasing or not c.is_monotonic_increasing
                    or bool((c <= o).any()) or bool((o[1:] < c[:-1]).any())
                    or (previous_open is not None and (o[0] <= previous_open or o[0] < previous_close))
                ):
                    raise ValueError("SOURCE_UTC_AXIS_INVALID")
                previous_open, previous_close = o[-1], c[-1]
                rows += len(chunk)
                allowed = c <= cutoff
                opens.append(o.asi8[allowed])
                closes.append(c.asi8[allowed])
                if with_spreads:
                    spread_values.append(pd.to_numeric(chunk.loc[allowed, "Spread"], errors="coerce").to_numpy(dtype=float))
    after = path.stat()
    if (before.st_size, before.st_mtime_ns, before.st_ino) != (after.st_size, after.st_mtime_ns, after.st_ino):
        raise ValueError("SOURCE_CHANGED_DURING_SCAN")
    if reader.digest.hexdigest() != meta["sha256"]:
        raise ValueError("SOURCE_DIGEST_MISMATCH")
    if rows != meta["expected_rows"] or reader.bytes_read != meta["expected_bytes"]:
        raise ValueError("SOURCE_ROWS_OR_BYTES_MISMATCH")
    result = {
        "opens_ns": np.concatenate(opens) if opens else np.array([], dtype=np.int64),
        "closes_ns": np.concatenate(closes) if closes else np.array([], dtype=np.int64),
        "source_rows": rows,
    }
    if with_spreads:
        result["spreads"] = np.concatenate(spread_values) if spread_values else np.array([], dtype=float)
    return result


def aligned_statistics(target: dict, source: dict, point: float) -> dict:
    """Use only an exact prior close; never fill gaps or use an in-bar quote."""
    opens = target["opens_ns"]
    closes = source["closes_ns"]
    positions = np.searchsorted(closes, opens)
    matches = np.zeros(len(opens), dtype=bool)
    within = positions < len(closes)
    matches[within] = closes[positions[within]] == opens[within]
    distribution = SpreadDistribution()
    distribution.add(source["spreads"][positions[matches]])
    result = distribution.snapshot(point)
    result.update({
        "target_prefix_rows": len(opens), "matched_rows": int(matches.sum()),
        "missing_exact_prior_close_rows": int((~matches).sum()),
        "first_target_open_utc": pd.Timestamp(int(opens[0]), tz="UTC").isoformat() if len(opens) else None,
        "last_target_close_utc": pd.Timestamp(int(target["closes_ns"][-1]), tz="UTC").isoformat() if len(opens) else None,
        "first_matched_open_utc": pd.Timestamp(int(opens[matches][0]), tz="UTC").isoformat() if matches.any() else None,
        "last_matched_open_utc": pd.Timestamp(int(opens[matches][-1]), tz="UTC").isoformat() if matches.any() else None,
    })
    return result


def build_native_profile(data_dir: Path, cutoff: str = DEFAULT_CUTOFF) -> dict:
    from pm_native_activation import _contained_path, native_semantic_leaf_bindings
    from pm_native_request_contract import TIMEFRAME_SECONDS

    native = data_dir.resolve() / "native"
    cutoff = _utc(cutoff)
    pointer, pointer_sha = _bound_json(native / "ACTIVE.json")
    manifest_path = _contained_path(native, pointer["manifest_relative_path"])
    if manifest_path is None:
        raise ValueError("ACTIVATION_PATH_UNSAFE")
    manifest, manifest_sha = _bound_json(manifest_path, pointer["manifest_sha256"])
    if manifest.get("status") != "active" or manifest.get("activation_id") != pointer["activation_id"]:
        raise ValueError("ACTIVATION_IDENTITY_INVALID")
    leaves = native_semantic_leaf_bindings(manifest)
    run_path = _contained_path(native, manifest["run_manifest_relative_path"])
    if run_path is None:
        raise ValueError("RUN_MANIFEST_PATH_UNSAFE")
    run, _ = _bound_json(run_path, manifest["run_manifest_sha256"])
    result = {
        "schema_version": "fxpm.frozen-spread-profile.v1",
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "operator_decisions": ["E-00000685", "E-00000713"],
        "calculation_policy": "ONE_TIME_OFFLINE_NO_RUNTIME_RECALCULATION",
        "source_convention": "FINEST_ACTIVATED_CANDIDATE_FIXED_PER_SYMBOL_EXACT_PRIOR_CLOSE_AT_TARGET_OPEN",
        "calibration_scope": "RETROSPECTIVE_FIXED_SPREAD_SCENARIO_NOT_HISTORICALLY_KNOWN_THRESHOLDS",
        "spread_semantics": "minimum_spread_in_source_bar_points_not_execution_quote_or_slippage_bound",
        "clock_semantics": "captured_acquisition_close_proxy_not_independent_quote_timestamp",
        "protected_cutoff_utc": cutoff.isoformat(),
        "production_authority": False,
        "activation": {"id": pointer["activation_id"], "pointer_sha256": pointer_sha,
                       "manifest_relative_path": pointer["manifest_relative_path"], "manifest_sha256": manifest_sha},
        "cells": [],
    }
    symbols = manifest["universe_eligibility"]["requested_symbols"]
    tfs = manifest["universe_eligibility"]["required_timeframes"]
    for symbol in symbols:
        available = [tf for tf in tfs if manifest["entries"][symbol][tf]["status"] == "evidence_candidate"]
        finest = min(available, key=TIMEFRAME_SECONDS.get) if available else None
        fine_meta = fine_data = None
        fine_error = None
        if finest:
            try:
                fine_meta = _source_metadata(native, manifest["entries"][symbol][finest], pointer["activation_id"], run)
                fine_data = read_bound_prefix(fine_meta, cutoff, with_spreads=True)
            except (OSError, ValueError, KeyError, TypeError) as exc:
                fine_error = str(exc) if isinstance(exc, ValueError) else type(exc).__name__
        for leaf in (x for x in leaves if x["symbol"] == symbol):
            tf = leaf["timeframe"]
            cell = {"symbol": symbol, "timeframe": tf, "activation_status": leaf["status"],
                    "source_timeframe": finest, "production_authority": False}
            if leaf["status"] != "evidence_candidate":
                cell.update(status="UNAVAILABLE_TARGET_SOURCE", reason_codes=manifest["entries"][symbol][tf].get("reason_codes", []))
            elif fine_error or fine_data is None:
                cell.update(status="UNAVAILABLE_FINEST_SOURCE", error=fine_error)
            elif TIMEFRAME_SECONDS[finest] > TIMEFRAME_SECONDS[tf]:
                cell.update(status="UNAVAILABLE_FINER_SOURCE_FOR_TARGET")
            else:
                try:
                    meta = fine_meta if tf == finest else _source_metadata(native, manifest["entries"][symbol][tf], pointer["activation_id"], run)
                    if meta["point"] != fine_meta["point"] or meta["source_identity"] != fine_meta["source_identity"]:
                        raise ValueError("TARGET_SOURCE_POINT_OR_BROKER_MISMATCH")
                    target = fine_data if tf == finest else read_bound_prefix(meta, cutoff, with_spreads=False)
                    cell.update(aligned_statistics(target, fine_data, meta["point"]))
                    cell["source_identity"] = meta["source_identity"]
                    cell["source_binding"] = {
                        "bars_relative_path": fine_meta["path"].relative_to(native).as_posix(),
                        "bars_sha256": fine_meta["sha256"],
                        "sidecar_sha256": fine_meta["sidecar_sha256"],
                        "provenance_sha256": fine_meta["provenance_sha256"],
                        "target_bars_relative_path": meta["path"].relative_to(native).as_posix(),
                        "target_bars_sha256": meta["sha256"],
                    }
                except (OSError, ValueError, KeyError, TypeError) as exc:
                    cell.update(status="UNAVAILABLE_TARGET_INPUT", error=str(exc) if isinstance(exc, ValueError) else type(exc).__name__)
            result["cells"].append(cell)
        print(f"spread profile: {symbol} ({len(result['cells'])}/600 groups)", flush=True)
    if len(result["cells"]) != 600:
        raise ValueError("EXACT_600_CELL_PROFILE_REQUIRED")
    result["status_counts"] = dict(Counter(c["status"] for c in result["cells"]))
    result["status"] = "COMPLETE_CALIBRATION_WITH_EXPLICIT_UNAVAILABLE_GROUPS"
    return result


def main() -> None:
    if str(ROOT) not in sys.path:
        sys.path.insert(0, str(ROOT))
    output = ROOT / "data/native/terms/spread_profile.json"
    if output.exists():
        raise FileExistsError("FROZEN_SPREAD_PROFILE_ALREADY_EXISTS_REUSE_IT")
    temporary = output.with_suffix(".json.tmp")
    handle = temporary.open("xb")
    written_complete = published = False
    try:
        result = build_native_profile(ROOT / "data")
        raw = json.dumps(result, indent=2, sort_keys=True, allow_nan=False).encode("utf-8") + b"\n"
        handle.write(raw)
        handle.flush()
        os.fsync(handle.fileno())
        written_complete = True
        handle.close()
        # A concurrently published profile is never overwritten.
        os.link(temporary, output)
        published = True
    finally:
        handle.close()
        if published or not written_complete:
            temporary.unlink(missing_ok=True)
    print(json.dumps({"output": str(output), "status_counts": result["status_counts"]}), flush=True)


if __name__ == "__main__":
    main()
