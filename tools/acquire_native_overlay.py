"""Acquire a bounded broker-native overlay on top of an immutable origin.

The historical origin is never rewritten and this tool never asks MT5 to
rehydrate its multi-year horizon.  For each symbol/timeframe cell it reads a
small authenticated tail from the origin, requests only that overlap plus the
new interval, verifies the splice, and publishes one create-only leaf.

Storage contract::

    data/native/overlays/runs/<overlay-run-id>/<SYMBOL>/<TIMEFRAME>/bars.csv
    data/native/overlays/runs/<overlay-run-id>/<SYMBOL>/<TIMEFRAME>/provenance.json
    data/native/overlays/runs/<overlay-run-id>/run_manifest.json

All MT5 synchronization belongs to ``MT5Connector.get_bars_range``.  This
module makes exactly one connector call per new cell with ``sync_retry=True``;
it contains no retry loop.  Failure leaves are technical transport evidence,
never broker-availability or economic verdicts.
"""

from __future__ import annotations

import argparse
import gc
import hashlib
import io
import json
import logging
import math
import os
import shutil
import subprocess
import sys
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

import numpy as np
import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from pm_native_intervals import TIMEFRAME_SECONDS  # noqa: E402
from pm_native_request_contract import PRODUCTION_SYMBOL_ORDER  # noqa: E402
from tools.acquire_native_history import (  # noqa: E402
    DEFAULT_EXECUTION_RUNTIME_SECONDS,
    DEFAULT_TIMEFRAMES,
    HARD_TIMEOUT_EXIT_CODE,
    NativeHistoryRunLock,
    NativeHistoryTerminalLock,
    RESOURCE_LIMIT_EXIT_CODE,
    SUPERVISOR_POLL_SECONDS,
    _count_csv_rows,
    _filtered_dataclass_kwargs,
    _iso,
    _json_safe,
    _normalized_native_root,
    _safe_name,
    _sha256,
    _supervisor_terminal_cache_snapshot,
    _terminate_supervised_worker,
    _utc,
    parse_utc_datetime,
    snapshot_read_only_runtime_metadata,
)


LOGGER = logging.getLogger("fxpm.native_overlay")
LEAF_SCHEMA_VERSION = "fxpm.native_overlay_leaf.v1"
MANIFEST_SCHEMA_VERSION = "fxpm.native_overlay_run_manifest.v1"
CONTRACT_VERSION = "fxpm.native_overlay_acquisition.v1"
SOURCE_KIND = "broker_native"
DEFAULT_BASELINE_RUN_ID = "production_native_20260710T2140Z"
DEFAULT_OVERLAP_BARS = 64
DEFAULT_MAX_SPAN_DAYS = 120.0
DEFAULT_MAX_CELL_ROWS = 50_000
DEFAULT_MAX_CELL_BYTES = 32 * 1024 * 1024
DEFAULT_MAX_RUN_BYTES = 8 * 1024 * 1024 * 1024
MAX_CONNECTOR_LEDGER_ENTRIES = 3
MAX_BASELINE_TAIL_SCAN_BYTES = 4 * 1024 * 1024
MAX_JSON_ARTIFACT_BYTES = 8 * 1024 * 1024
# Overlay acquisition is a small current delta, not a multi-day optimizer.
# Six hours permits an explicitly slower 600-cell run while remaining a hard
# upper bound on a stuck worker.  The ordinary execute path uses the native
# acquisition authority's one-hour default.
MAX_EXECUTION_RUNTIME_SECONDS = 6 * 3600.0


class OverlayValidationError(RuntimeError):
    """An immutable origin/overlay cannot satisfy the overlay contract."""


class OverlayBoundExceeded(OverlayValidationError):
    """A configured deterministic resource ceiling was exceeded."""


@dataclass(frozen=True)
class OverlayBounds:
    overlap_bars: int = DEFAULT_OVERLAP_BARS
    max_span_days: float = DEFAULT_MAX_SPAN_DAYS
    max_cell_rows: int = DEFAULT_MAX_CELL_ROWS
    max_cell_bytes: int = DEFAULT_MAX_CELL_BYTES
    max_run_bytes: int = DEFAULT_MAX_RUN_BYTES

    def __post_init__(self) -> None:
        if self.overlap_bars < 2:
            raise ValueError("overlap_bars must be at least two")
        if not math.isfinite(self.max_span_days) or self.max_span_days <= 0.0:
            raise ValueError("max_span_days must be finite and positive")
        for name in ("max_cell_rows", "max_cell_bytes", "max_run_bytes"):
            if int(getattr(self, name)) <= 0:
                raise ValueError(f"{name} must be positive")
        if self.max_cell_bytes > self.max_run_bytes:
            raise ValueError("max_cell_bytes cannot exceed max_run_bytes")


@dataclass(frozen=True)
class OverlayRequest:
    symbols: Tuple[str, ...]
    timeframes: Tuple[str, ...]
    cutoff_utc: datetime
    baseline_run_id: str = DEFAULT_BASELINE_RUN_ID
    bounds: OverlayBounds = OverlayBounds()

    def __post_init__(self) -> None:
        object.__setattr__(self, "cutoff_utc", _utc(self.cutoff_utc))
        symbols = tuple(_safe_name(value, "symbol") for value in self.symbols)
        timeframes = tuple(str(value).strip().upper() for value in self.timeframes)
        if not symbols or len(set(symbols)) != len(symbols):
            raise ValueError("symbols must be non-empty and unique")
        if not timeframes or len(set(timeframes)) != len(timeframes):
            raise ValueError("timeframes must be non-empty and unique")
        unknown = [value for value in timeframes if value not in TIMEFRAME_SECONDS]
        if unknown:
            raise ValueError(f"unsupported timeframe(s): {unknown}")
        if any(value not in PRODUCTION_SYMBOL_ORDER for value in symbols):
            raise ValueError("overlay symbols must be canonical production symbols")
        if any(value not in DEFAULT_TIMEFRAMES for value in timeframes):
            raise ValueError("overlay timeframes must belong to the production six")
        if not 1970 <= self.cutoff_utc.year <= 2200:
            raise ValueError("overlay cutoff year is outside the bounded contract")
        object.__setattr__(self, "symbols", symbols)
        object.__setattr__(self, "timeframes", timeframes)
        object.__setattr__(
            self, "baseline_run_id", _safe_name(self.baseline_run_id, "baseline run id")
        )


@dataclass(frozen=True)
class BaselineCell:
    canonical_symbol: str
    broker_symbol: str
    timeframe: str
    leaf: Path
    bars_path: Path
    provenance_path: Path
    provenance: Mapping[str, Any]
    source_identity: Mapping[str, Any]
    baseline_pin: Mapping[str, Any]
    tail: pd.DataFrame
    request_start_utc: datetime
    actual_last_open_utc: datetime
    actual_last_close_utc: datetime


def _canonical_json_sha256(value: Any) -> str:
    payload = json.dumps(
        _json_safe(value),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _reject_duplicate_json_pairs(pairs: Sequence[Tuple[str, Any]]) -> Dict[str, Any]:
    value: Dict[str, Any] = {}
    for key, item in pairs:
        if key in value:
            raise OverlayValidationError(f"duplicate JSON object key: {key!r}")
        value[key] = item
    return value


def _load_json(path: Path) -> Dict[str, Any]:
    try:
        size = int(path.stat().st_size)
    except OSError as exc:
        raise OverlayValidationError(
            f"cannot stat JSON artifact {path}: {exc}"
        ) from exc
    if size <= 0 or size > MAX_JSON_ARTIFACT_BYTES:
        raise OverlayValidationError(
            f"JSON artifact size {size} is outside (0,{MAX_JSON_ARTIFACT_BYTES}]: {path}"
        )
    try:
        with path.open("r", encoding="utf-8") as handle:
            value = json.load(handle, object_pairs_hook=_reject_duplicate_json_pairs)
    except Exception as exc:
        raise OverlayValidationError(
            f"cannot read JSON artifact {path}: {exc}"
        ) from exc
    if not isinstance(value, dict):
        raise OverlayValidationError(f"JSON artifact is not an object: {path}")
    return value


def _flat_source_identity(source_environment: Any) -> Dict[str, Any]:
    """Return the small stable identity shared by origin, overlay and resolver."""

    environment = source_environment if isinstance(source_environment, Mapping) else {}
    account = environment.get("account")
    account = account if isinstance(account, Mapping) else {}
    return {
        "broker": account.get("company"),
        "server": account.get("server"),
        "account_id": account.get("login"),
        "source_kind": SOURCE_KIND,
    }


def _read_csv_tail(
    path: Path,
    rows: int,
    *,
    max_scan_bytes: int = MAX_BASELINE_TAIL_SCAN_BYTES,
) -> pd.DataFrame:
    """Read at most ``rows`` trailing records with bounded memory and scan size."""

    if rows <= 0 or max_scan_bytes <= 0:
        raise ValueError("tail bounds must be positive")
    size = int(path.stat().st_size)
    if size <= 0:
        raise OverlayValidationError(f"baseline bars are empty: {path}")
    with path.open("rb") as handle:
        header = handle.readline()
        if not header:
            raise OverlayValidationError(f"baseline CSV has no header: {path}")
        header_end = handle.tell()
        available = max(0, size - header_end)
        scan = min(available, int(max_scan_bytes))
        handle.seek(size - scan)
        body = handle.read(scan)
    lines = body.splitlines()
    if scan < available and lines:
        lines = lines[1:]
    nonempty = [line for line in lines if line.strip()]
    selected = nonempty[-int(rows) :]
    if len(selected) < min(int(rows), 2):
        raise OverlayValidationError(
            f"bounded tail scan found only {len(selected)} baseline rows: {path}"
        )
    payload = header.rstrip(b"\r\n") + b"\n" + b"\n".join(selected) + b"\n"
    try:
        frame = pd.read_csv(io.BytesIO(payload), index_col=0)
    except Exception as exc:
        raise OverlayValidationError(
            f"cannot parse baseline tail {path}: {exc}"
        ) from exc
    index = pd.to_datetime(frame.index, utc=True, errors="coerce")
    if index.isna().any():
        raise OverlayValidationError(
            f"baseline tail contains invalid timestamps: {path}"
        )
    frame.index = pd.DatetimeIndex(index)
    if frame.index.has_duplicates or not frame.index.is_monotonic_increasing:
        raise OverlayValidationError(f"baseline tail is not unique and ordered: {path}")
    return frame


class BaselineCatalog:
    """Authenticate one immutable origin and expose bounded per-cell tails."""

    def __init__(
        self,
        native_root: Path,
        run_id: str,
        *,
        require_production_roster: bool = True,
    ) -> None:
        self.native_root = _normalized_native_root(native_root)
        self.run_id = _safe_name(run_id, "baseline run id")
        self.run_root = self.native_root / "runs" / self.run_id
        self.manifest_path = self.run_root / "run_manifest.json"
        if not self.run_root.is_dir() or self.run_root.is_symlink():
            raise OverlayValidationError(
                f"baseline run does not exist: {self.run_root}"
            )
        if self.manifest_path.is_symlink():
            raise OverlayValidationError("baseline manifest cannot be a symlink")
        self.manifest = _load_json(self.manifest_path)
        if (
            self.manifest.get("complete") is not True
            or self.manifest.get("immutable_record") is not True
        ):
            raise OverlayValidationError(
                "baseline manifest is not complete and immutable"
            )
        request = self.manifest.get("request")
        if not isinstance(request, Mapping):
            raise OverlayValidationError("baseline manifest has no request object")
        observed_symbols = tuple(str(value) for value in (request.get("symbols") or ()))
        if require_production_roster:
            # The preserved V1 origin predates the casing authority for the two
            # mixed-case metals.  Position and case-folded identity are exact;
            # overlay artifacts use today's canonical casing while their pins
            # retain the legacy leaf names verbatim.
            permitted_legacy_symbols = tuple(
                {
                    "Platinum": "PLATINUM",
                    "Palladium": "PALLADIUM",
                }.get(value, value)
                for value in PRODUCTION_SYMBOL_ORDER
            )
            if observed_symbols != permitted_legacy_symbols:
                raise OverlayValidationError(
                    "baseline manifest is not the exact ordered production symbol roster"
                )
            if tuple(request.get("timeframes") or ()) != tuple(DEFAULT_TIMEFRAMES):
                raise OverlayValidationError(
                    "baseline manifest is not the exact six-timeframe roster"
                )
            self.canonical_to_origin_symbol = dict(
                zip(PRODUCTION_SYMBOL_ORDER, observed_symbols)
            )
        else:
            self.canonical_to_origin_symbol = {
                value: value for value in observed_symbols
            }
        self.manifest_sha256 = _sha256(self.manifest_path)
        self.baseline = {
            "run_id": self.run_id,
            "manifest_relative_path": f"runs/{self.run_id}/run_manifest.json",
            "manifest_sha256": self.manifest_sha256,
        }
        raw_results = self.manifest.get("results")
        if not isinstance(raw_results, list):
            raise OverlayValidationError("baseline manifest results are missing")
        self.results: Dict[Tuple[str, str], Mapping[str, Any]] = {}
        for item in raw_results:
            if not isinstance(item, Mapping):
                raise OverlayValidationError("baseline result is not an object")
            key = (
                str(item.get("symbol") or ""),
                str(item.get("timeframe") or "").upper(),
            )
            if key in self.results:
                raise OverlayValidationError(f"duplicate baseline result: {key}")
            self.results[key] = item
        if require_production_roster:
            expected_cells = {
                (symbol, timeframe)
                for symbol in observed_symbols
                for timeframe in DEFAULT_TIMEFRAMES
            }
            if set(self.results) != expected_cells:
                raise OverlayValidationError(
                    "baseline manifest does not contain exactly the production 100x6 cells"
                )

    def load_cell(self, symbol: str, timeframe: str, overlap_bars: int) -> BaselineCell:
        origin_symbol = self.canonical_to_origin_symbol.get(symbol, symbol)
        key = (origin_symbol, timeframe)
        summary = self.results.get(key)
        if summary is None:
            raise OverlayValidationError(f"baseline manifest has no cell {key}")
        if summary.get("status") not in {"success", "partial"}:
            raise OverlayValidationError(f"baseline cell is not a data artifact: {key}")
        relative_leaf = str(summary.get("relative_leaf") or "")
        if relative_leaf != f"{origin_symbol}/{timeframe}":
            raise OverlayValidationError(f"baseline leaf path mismatch for {key}")
        leaf = self.run_root / origin_symbol / timeframe
        bars_path = leaf / "bars.csv"
        provenance_path = leaf / "provenance.json"
        if (
            not leaf.is_dir()
            or leaf.is_symlink()
            or bars_path.is_symlink()
            or provenance_path.is_symlink()
        ):
            raise OverlayValidationError(f"baseline leaf is missing or unsafe: {leaf}")
        entries = {item.name for item in leaf.iterdir()}
        if entries != {"bars.csv", "provenance.json"}:
            raise OverlayValidationError(
                f"baseline leaf has unexpected entries: {leaf}"
            )
        provenance = _load_json(provenance_path)
        expected_provenance = {
            "schema_version": "fxpm-native-bars-v1",
            "immutable_record": True,
            "run_id": self.run_id,
            "status": summary.get("status"),
            "source_kind": SOURCE_KIND,
        }
        for name, expected_value in expected_provenance.items():
            if provenance.get(name) != expected_value:
                raise OverlayValidationError(
                    f"baseline provenance {name} mismatch for {leaf}"
                )
        artifact = provenance.get("artifact")
        if (
            not isinstance(artifact, Mapping)
            or artifact.get("relative_path") != "bars.csv"
        ):
            raise OverlayValidationError(f"baseline leaf has no artifact pin: {leaf}")
        bars_sha = _sha256(bars_path)
        rows = _count_csv_rows(bars_path)
        byte_count = int(bars_path.stat().st_size)
        expected = {
            "sha256": bars_sha,
            "rows": rows,
            "bytes": byte_count,
        }
        for name, observed in expected.items():
            if artifact.get(name) != observed:
                raise OverlayValidationError(
                    f"baseline artifact {name} mismatch for {leaf}: {artifact.get(name)!r} != {observed!r}"
                )
        if summary.get("sha256") != bars_sha or int(summary.get("rows", -1)) != rows:
            raise OverlayValidationError(f"baseline manifest pin mismatch for {leaf}")
        if (
            provenance.get("sha256") != bars_sha
            or int(provenance.get("rows", -1)) != rows
        ):
            raise OverlayValidationError(
                f"baseline provenance top-level pin mismatch: {leaf}"
            )
        if (
            provenance.get("canonical_symbol") != origin_symbol
            or str(provenance.get("timeframe") or "").upper() != timeframe
        ):
            raise OverlayValidationError(f"baseline provenance cell mismatch: {leaf}")
        coverage = provenance.get("coverage")
        request = provenance.get("request")
        if not isinstance(coverage, Mapping) or not isinstance(request, Mapping):
            raise OverlayValidationError(f"baseline coverage/request missing: {leaf}")
        actual_last_open = parse_utc_datetime(
            str(
                coverage.get("actual_last_open_utc")
                or request.get("last_fully_closed_bar_open_utc")
            )
        )
        actual_last_close = parse_utc_datetime(
            str(coverage.get("actual_last_close_utc"))
        )
        if actual_last_close <= actual_last_open:
            raise OverlayValidationError(
                f"baseline last close is not after last open: {leaf}"
            )
        tail = _read_csv_tail(bars_path, int(overlap_bars))
        if tail.index[-1].to_pydatetime() != actual_last_open:
            raise OverlayValidationError(
                f"baseline actual-last-open pin mismatch: {leaf}"
            )
        if "_BAR_CLOSE_TS" not in tail.columns:
            raise OverlayValidationError(
                f"baseline tail has no authenticated close column: {leaf}"
            )
        tail_closes = pd.to_datetime(tail["_BAR_CLOSE_TS"], utc=True, errors="coerce")
        if (
            tail_closes.isna().any()
            or tail_closes.iloc[-1].to_pydatetime() != actual_last_close
        ):
            raise OverlayValidationError(
                f"baseline actual-last-close pin mismatch: {leaf}"
            )
        request_start = tail.index[0].to_pydatetime()
        source_identity = _flat_source_identity(provenance.get("source_environment"))
        if any(
            source_identity.get(key) is None
            for key in ("broker", "server", "account_id")
        ):
            raise OverlayValidationError(
                f"baseline source identity is incomplete: {leaf}"
            )
        baseline_pin = {
            **self.baseline,
            "relative_leaf": relative_leaf,
            "provenance_sha256": _sha256(provenance_path),
            "bars_sha256": bars_sha,
            "rows": rows,
            "bytes": byte_count,
            "actual_last_open_utc": _iso(actual_last_open),
            "actual_last_close_utc": _iso(actual_last_close),
        }
        return BaselineCell(
            canonical_symbol=symbol,
            broker_symbol=str(provenance.get("broker_symbol") or symbol),
            timeframe=timeframe,
            leaf=leaf,
            bars_path=bars_path,
            provenance_path=provenance_path,
            provenance=provenance,
            source_identity=source_identity,
            baseline_pin=baseline_pin,
            tail=tail,
            request_start_utc=request_start,
            actual_last_open_utc=actual_last_open,
            actual_last_close_utc=actual_last_close,
        )


def _atomic_json(path: Path, value: Mapping[str, Any]) -> None:
    with path.open("x", encoding="utf-8", newline="\n") as handle:
        json.dump(
            _json_safe(value), handle, indent=2, sort_keys=True, ensure_ascii=False
        )
        handle.write("\n")
        handle.flush()
        os.fsync(handle.fileno())


class OverlayStore:
    """Create-only atomic overlay leaves and a final immutable manifest."""

    def __init__(self, native_root: Path, run_id: str, *, resume: bool = False) -> None:
        self.native_root = _normalized_native_root(native_root)
        self.run_id = _safe_name(run_id, "overlay run id")
        self.overlays_root = self.native_root / "overlays" / "runs"
        self.run_root = self.overlays_root / self.run_id
        self.resume = bool(resume)
        self.overlays_root.mkdir(parents=True, exist_ok=True)
        if self.run_root.exists() and not self.resume:
            raise FileExistsError(f"overlay run already exists: {self.run_root}")
        self.run_root.mkdir(parents=True, exist_ok=self.resume)
        if self.run_root.is_symlink():
            raise OverlayValidationError("overlay run root cannot be a symlink")
        if self.resume:
            for staging in self.run_root.rglob(".*.staging"):
                if staging.is_dir() and not staging.is_symlink():
                    shutil.rmtree(staging)
                else:
                    staging.unlink(missing_ok=True)

    def leaf_path(self, symbol: str, timeframe: str) -> Path:
        return self.run_root / symbol / timeframe

    def discover_final_leaves(self) -> Dict[Tuple[str, str], Path]:
        """Discover only canonical two-level leaves; reject unreferenced state."""

        leaves: Dict[Tuple[str, str], Path] = {}
        for symbol_entry in sorted(self.run_root.iterdir(), key=lambda item: item.name):
            if symbol_entry.name == "run_manifest.json":
                continue
            if not symbol_entry.is_dir() or symbol_entry.is_symlink():
                raise OverlayValidationError(
                    f"unexpected overlay run entry: {symbol_entry}"
                )
            symbol = _safe_name(symbol_entry.name, "symbol")
            if symbol not in PRODUCTION_SYMBOL_ORDER:
                raise OverlayValidationError(
                    f"non-canonical overlay symbol directory: {symbol_entry}"
                )
            for timeframe_entry in sorted(
                symbol_entry.iterdir(), key=lambda item: item.name
            ):
                if not timeframe_entry.is_dir() or timeframe_entry.is_symlink():
                    raise OverlayValidationError(
                        f"unexpected overlay symbol entry: {timeframe_entry}"
                    )
                timeframe = _safe_name(timeframe_entry.name, "timeframe").upper()
                if timeframe not in DEFAULT_TIMEFRAMES:
                    raise OverlayValidationError(
                        f"non-canonical overlay timeframe directory: {timeframe_entry}"
                    )
                key = (symbol, timeframe)
                if key in leaves:
                    raise OverlayValidationError(f"duplicate overlay leaf: {key}")
                leaves[key] = timeframe_entry
        return leaves

    def _commit(
        self,
        symbol: str,
        timeframe: str,
        record: Mapping[str, Any],
        frame: Optional[pd.DataFrame],
        *,
        max_bytes: int,
    ) -> Tuple[Dict[str, Any], Dict[str, Any]]:
        final = self.leaf_path(symbol, timeframe)
        if final.exists():
            raise FileExistsError(f"immutable overlay leaf already exists: {final}")
        parent = final.parent
        parent.mkdir(parents=True, exist_ok=True)
        staging = parent / f".{timeframe}.{uuid.uuid4().hex}.staging"
        staging.mkdir(exist_ok=False)
        try:
            committed = dict(_json_safe(record))
            if frame is not None:
                bars_path = staging / "bars.csv"
                frame.to_csv(
                    bars_path,
                    index=True,
                    index_label="time",
                    lineterminator="\n",
                )
                with bars_path.open("r+b") as handle:
                    handle.flush()
                    os.fsync(handle.fileno())
                byte_count = int(bars_path.stat().st_size)
                if byte_count > int(max_bytes):
                    raise OverlayBoundExceeded(
                        f"serialized cell bytes {byte_count} exceed cap {max_bytes}"
                    )
                rows = _count_csv_rows(bars_path)
                digest = _sha256(bars_path)
                committed["artifact"] = {
                    "relative_path": "bars.csv",
                    "sha256": digest,
                    "bytes": byte_count,
                    "rows": rows,
                }
            else:
                committed["artifact"] = None
            provenance_path = staging / "provenance.json"
            _atomic_json(provenance_path, committed)
            provenance_sha = _sha256(provenance_path)
            os.rename(staging, final)
            artifact = committed.get("artifact")
            result = {
                "canonical_symbol": symbol,
                "broker_symbol": committed.get("broker_symbol"),
                "timeframe": timeframe,
                "status": committed.get("status"),
                "failure_status": committed.get("failure_status"),
                "relative_leaf": f"{symbol}/{timeframe}",
                "provenance_sha256": provenance_sha,
                "bars_sha256": artifact.get("sha256")
                if isinstance(artifact, Mapping)
                else None,
                "rows": int(artifact.get("rows", 0))
                if isinstance(artifact, Mapping)
                else 0,
                "bytes": int(artifact.get("bytes", 0))
                if isinstance(artifact, Mapping)
                else 0,
            }
            return committed, result
        except Exception:
            if staging.exists():
                shutil.rmtree(staging, ignore_errors=True)
            raise

    def commit_success(
        self,
        symbol: str,
        timeframe: str,
        record: Mapping[str, Any],
        frame: pd.DataFrame,
        *,
        max_bytes: int,
    ) -> Tuple[Dict[str, Any], Dict[str, Any]]:
        return self._commit(symbol, timeframe, record, frame, max_bytes=max_bytes)

    def commit_failure(
        self,
        symbol: str,
        timeframe: str,
        record: Mapping[str, Any],
    ) -> Tuple[Dict[str, Any], Dict[str, Any]]:
        return self._commit(symbol, timeframe, record, None, max_bytes=1)

    def commit_manifest(self, manifest: Mapping[str, Any]) -> Path:
        final = self.run_root / "run_manifest.json"
        if final.exists():
            raise FileExistsError(f"immutable overlay manifest already exists: {final}")
        staging = self.run_root / f".run_manifest.{uuid.uuid4().hex}.staging"
        try:
            _atomic_json(staging, manifest)
            # Hard-link publication is create-only and atomic on the same
            # volume: unlike replace/rename it cannot overwrite a manifest that
            # appeared after the preflight check.
            os.link(staging, final)
            staging.unlink()
            return final
        finally:
            staging.unlink(missing_ok=True)


def _connector_diagnostics(connector: Any, frame: Any) -> Dict[str, Any]:
    diagnostics: Any = None
    if frame is not None and isinstance(getattr(frame, "attrs", None), Mapping):
        diagnostics = frame.attrs.get("mt5_history_range_diagnostics")
    if not isinstance(diagnostics, Mapping):
        reader = getattr(connector, "get_last_history_range_diagnostics", None)
        diagnostics = reader() if callable(reader) else None
    return dict(_json_safe(diagnostics)) if isinstance(diagnostics, Mapping) else {}


def _validate_connector_ledger(diagnostics: Mapping[str, Any]) -> None:
    if diagnostics.get("range_contract_version") != "mt5-half-open-range-v4":
        raise OverlayValidationError(
            "connector did not emit the V4 half-open range contract"
        )
    if diagnostics.get("sync_retry") is not True:
        raise OverlayValidationError(
            "connector diagnostics do not bind sync_retry=True"
        )
    ledger = diagnostics.get("call_ledger")
    if (
        not isinstance(ledger, list)
        or not ledger
        or len(ledger) > MAX_CONNECTOR_LEDGER_ENTRIES
    ):
        raise OverlayValidationError(
            "connector call ledger is missing or exceeds its bound"
        )
    call_types = [
        str(item.get("call_type") or "") if isinstance(item, Mapping) else ""
        for item in ledger
    ]
    permitted_sequences = {
        ("copy_rates_range_initial",),
        ("copy_rates_range_initial", "copy_rates_from_warm"),
        (
            "copy_rates_range_initial",
            "copy_rates_from_warm",
            "copy_rates_range_retry",
        ),
    }
    if tuple(call_types) not in permitted_sequences:
        raise OverlayValidationError(
            "connector call ledger has an invalid retry sequence"
        )
    exact_calls = sum(value.startswith("copy_rates_range_") for value in call_types)
    warm_calls = sum(value == "copy_rates_from_warm" for value in call_types)
    if exact_calls > 2 or warm_calls > 1:
        raise OverlayValidationError(
            "connector exceeded its single-owner retry contract"
        )
    if int(diagnostics.get("copy_rates_range_attempts", -1)) != exact_calls:
        raise OverlayValidationError(
            "connector exact-read count contradicts its ledger"
        )
    if int(diagnostics.get("history_download_trigger_attempts", -1)) != warm_calls:
        raise OverlayValidationError("connector warm-read count contradicts its ledger")


def _technical_transport_status(diagnostics: Mapping[str, Any]) -> str:
    if bool(diagnostics.get("hard_failure")):
        return "terminal_hard_failure"
    relation = str(diagnostics.get("raw_relation") or "")
    status = str(diagnostics.get("sync_status") or "")
    if (
        relation == "RIGHT_DISJOINT_PREFIX"
        or status == "terminal_prefix_unsynchronized"
    ):
        return "terminal_prefix_unsynchronized"
    if relation == "LEFT_DISJOINT_SUFFIX" or status == "terminal_suffix_unsynchronized":
        return "terminal_suffix_unsynchronized"
    if relation == "MIXED_INVALID" or status in {
        "mixed_invalid_no_admissible_data",
        "invalid_filtered_response",
    }:
        return "terminal_range_mixed_invalid"
    return "terminal_empty_unresolved"


def _normalize_overlay_frame(
    frame: pd.DataFrame,
    timeframe: str,
    start_utc: datetime,
    cutoff_utc: datetime,
) -> pd.DataFrame:
    if not isinstance(frame, pd.DataFrame):
        raise OverlayValidationError("connector returned a non-DataFrame payload")
    if len(frame) <= 0:
        return frame
    local = frame.copy()
    if not isinstance(local.index, pd.DatetimeIndex):
        if "time" not in local.columns:
            raise OverlayValidationError("connector frame has no timestamp axis")
        local.index = pd.to_datetime(local.pop("time"), utc=True, errors="coerce")
    else:
        local.index = pd.to_datetime(local.index, utc=True, errors="coerce")
    if local.index.isna().any():
        raise OverlayValidationError("connector frame contains invalid timestamps")
    start = pd.Timestamp(start_utc)
    cutoff = pd.Timestamp(cutoff_utc)
    if bool(((local.index < start) | (local.index >= cutoff)).any()):
        raise OverlayValidationError(
            "connector returned a bar outside the exact request"
        )
    local = local.sort_index(kind="stable")
    local = local[~local.index.duplicated(keep="last")]
    required = ("Open", "High", "Low", "Close")
    if any(name not in local.columns for name in required):
        raise OverlayValidationError("connector frame is missing OHLC columns")
    numeric = local.loc[:, required].apply(pd.to_numeric, errors="coerce")
    if not np.isfinite(numeric.to_numpy(dtype=float)).all():
        raise OverlayValidationError("connector frame contains non-finite OHLC values")
    if bool(
        (
            (numeric["High"] < numeric[["Open", "Close", "Low"]].max(axis=1))
            | (numeric["Low"] > numeric[["Open", "Close", "High"]].min(axis=1))
        ).any()
    ):
        raise OverlayValidationError("connector frame violates OHLC invariants")
    seconds = int(TIMEFRAME_SECONDS[timeframe])
    index_ns = local.index.asi8
    nominal_ns = seconds * 1_000_000_000
    if timeframe in {"H4", "D1"}:
        next_ns = np.empty_like(index_ns)
        if len(index_ns) > 1:
            next_ns[:-1] = index_ns[1:]
        next_ns[-1] = index_ns[-1] + nominal_ns
        deltas = next_ns - index_ns
        regular = (deltas >= nominal_ns * 0.5) & (deltas <= nominal_ns * 1.5)
        close_ns = np.where(regular, next_ns, index_ns + nominal_ns)
    else:
        close_ns = index_ns + nominal_ns
    fully_closed = close_ns <= cutoff.value
    local = local.loc[np.asarray(fully_closed)].copy()
    close_ns = close_ns[np.asarray(fully_closed)]
    local.index.name = "time"
    if local.empty:
        return local
    local["_SOURCE_FIRST_TS"] = local.index
    local["_SOURCE_LAST_TS"] = pd.to_datetime(close_ns - 1, utc=True)
    local["_BAR_CLOSE_TS"] = pd.to_datetime(close_ns, utc=True)
    return local


def _verify_overlap(
    baseline: BaselineCell,
    overlay: pd.DataFrame,
) -> Dict[str, Any]:
    common = baseline.tail.index.intersection(overlay.index)
    required = len(baseline.tail)
    if len(common) < required:
        raise OverlayValidationError(
            f"overlay has {len(common)} authenticated overlap rows; {required} required"
        )
    columns = [
        "Open",
        "High",
        "Low",
        "Close",
        "Volume",
        "Spread",
        "RealVolume",
    ]
    for column in columns:
        if column not in baseline.tail.columns or column not in overlay.columns:
            raise OverlayValidationError(f"splice comparison column missing: {column}")
        left = pd.to_numeric(
            baseline.tail.loc[common, column], errors="coerce"
        ).to_numpy(float)
        right = pd.to_numeric(overlay.loc[common, column], errors="coerce").to_numpy(
            float
        )
        if not np.allclose(left, right, rtol=0.0, atol=1e-12, equal_nan=False):
            raise OverlayValidationError(f"baseline/overlay OHLC mismatch in {column}")
    baseline_closes = pd.to_datetime(
        baseline.tail.loc[common, "_BAR_CLOSE_TS"], utc=True, errors="coerce"
    )
    overlay_closes = pd.to_datetime(
        overlay.loc[common, "_BAR_CLOSE_TS"], utc=True, errors="coerce"
    )
    if not baseline_closes.equals(overlay_closes):
        raise OverlayValidationError("baseline/overlay close timestamps mismatch")
    canonical_rows = [
        {
            "open_utc": _iso(timestamp.to_pydatetime()),
            "close_utc": _iso(
                pd.Timestamp(overlay_closes.loc[timestamp]).to_pydatetime()
            ),
            **{column: float(overlay.loc[timestamp, column]) for column in columns},
        }
        for timestamp in common
    ]
    return {
        "status": "verified",
        "required_rows": required,
        "matched_rows": int(len(common)),
        "first_open_utc": _iso(common[0].to_pydatetime()),
        "last_open_utc": _iso(common[-1].to_pydatetime()),
        "canonical_rows_sha256": _canonical_json_sha256(canonical_rows),
    }


class NativeOverlayAcquirer:
    """Acquire at most one bounded in-memory current cell at a time."""

    def __init__(
        self,
        connector: Any,
        store: OverlayStore,
        request: OverlayRequest,
        *,
        runtime_metadata: Mapping[str, Any],
        require_production_baseline: bool = True,
        now_fn: Any = None,
    ) -> None:
        self.connector = connector
        self.store = store
        self.request = request
        self.runtime_metadata = dict(_json_safe(runtime_metadata))
        self.source_identity = _flat_source_identity(self.runtime_metadata)
        if any(
            self.source_identity.get(key) is None
            for key in ("broker", "server", "account_id")
        ):
            raise OverlayValidationError(
                "current terminal source identity is incomplete"
            )
        self.catalog = BaselineCatalog(
            store.native_root,
            request.baseline_run_id,
            require_production_roster=require_production_baseline,
        )
        self.now_fn = now_fn or (lambda: datetime.now(timezone.utc))

    def _base_record(self, baseline: BaselineCell) -> Dict[str, Any]:
        return {
            "schema_version": LEAF_SCHEMA_VERSION,
            "contract_version": CONTRACT_VERSION,
            "run_id": self.store.run_id,
            "immutable_record": True,
            "canonical_symbol": baseline.canonical_symbol,
            "broker_symbol": baseline.broker_symbol,
            "timeframe": baseline.timeframe,
            "source_identity": dict(self.source_identity),
            "baseline": dict(baseline.baseline_pin),
            "request": {
                "start_utc": _iso(baseline.request_start_utc),
                "end_exclusive_utc": _iso(self.request.cutoff_utc),
                "common_current_closed_cutoff_utc": _iso(self.request.cutoff_utc),
                "overlap_policy": "trailing_actual_baseline_bars_v1",
                "overlap_bars_requested": int(self.request.bounds.overlap_bars),
            },
            "bounds": {
                "max_span_days": float(self.request.bounds.max_span_days),
                "max_cell_rows": int(self.request.bounds.max_cell_rows),
                "max_cell_bytes": int(self.request.bounds.max_cell_bytes),
                "max_run_bytes": int(self.request.bounds.max_run_bytes),
            },
            "status": "failed",
            "failure_status": None,
            "transport_status": "not_started",
            "coverage": None,
            "connector": None,
            "overlap": None,
            "artifact": None,
            "acquisition_started_utc": _iso(_utc(self.now_fn())),
            "acquisition_completed_utc": None,
        }

    def _failure_record(
        self,
        base: Mapping[str, Any],
        failure_status: str,
        *,
        diagnostics: Optional[Mapping[str, Any]] = None,
        message: str,
    ) -> Dict[str, Any]:
        record = dict(_json_safe(base))
        record.update(
            {
                "status": "failed",
                "failure_status": failure_status,
                "transport_status": failure_status,
                "connector": dict(_json_safe(diagnostics or {})),
                "error": {
                    "classification": "technical_transport",
                    "type": failure_status,
                    "message": str(message)[:1024],
                    "economic_verdict": False,
                    "broker_absence_inferred": False,
                },
                "acquisition_completed_utc": _iso(_utc(self.now_fn())),
            }
        )
        return record

    def _acquire_cell(
        self,
        symbol: str,
        timeframe: str,
        *,
        run_bytes_remaining: int,
    ) -> Dict[str, Any]:
        baseline = self.catalog.load_cell(
            symbol, timeframe, self.request.bounds.overlap_bars
        )
        base = self._base_record(baseline)
        try:
            if dict(baseline.source_identity) != dict(self.source_identity):
                failure = self._failure_record(
                    base,
                    "source_identity_mismatch",
                    message="current terminal identity differs from the authenticated origin",
                )
                _, result = self.store.commit_failure(symbol, timeframe, failure)
                return result
            if self.request.cutoff_utc <= baseline.actual_last_close_utc:
                failure = self._failure_record(
                    base,
                    "cutoff_not_after_baseline",
                    message="overlay cutoff is not newer than the origin's actual last close",
                )
                _, result = self.store.commit_failure(symbol, timeframe, failure)
                return result
            span_days = (
                self.request.cutoff_utc - baseline.request_start_utc
            ).total_seconds() / 86400.0
            if span_days > self.request.bounds.max_span_days:
                failure = self._failure_record(
                    base,
                    "baseline_too_stale_for_overlay",
                    message=(
                        f"bounded overlay span {span_days:.6f}d exceeds "
                        f"{self.request.bounds.max_span_days:.6f}d"
                    ),
                )
                _, result = self.store.commit_failure(symbol, timeframe, failure)
                return result

            frame: Optional[pd.DataFrame]
            caught: Optional[BaseException] = None
            try:
                frame = self.connector.get_bars_range(
                    symbol,
                    timeframe,
                    baseline.request_start_utc,
                    self.request.cutoff_utc,
                    sync_retry=True,
                )
            except Exception as exc:
                frame = None
                caught = exc
            diagnostics = _connector_diagnostics(self.connector, frame)
            try:
                _validate_connector_ledger(diagnostics)
            except Exception as exc:
                failure = self._failure_record(
                    base,
                    "connector_contract_invalid",
                    diagnostics=diagnostics,
                    message=str(exc),
                )
                _, result = self.store.commit_failure(symbol, timeframe, failure)
                return result
            if caught is not None or frame is None or len(frame) <= 0:
                failure_status = (
                    "terminal_hard_failure"
                    if caught is not None
                    else _technical_transport_status(diagnostics)
                )
                failure = self._failure_record(
                    base,
                    failure_status,
                    diagnostics=diagnostics,
                    message=str(
                        caught or diagnostics.get("sync_status") or "empty MT5 response"
                    ),
                )
                _, result = self.store.commit_failure(symbol, timeframe, failure)
                return result
            try:
                overlay = _normalize_overlay_frame(
                    frame,
                    timeframe,
                    baseline.request_start_utc,
                    self.request.cutoff_utc,
                )
            except Exception as exc:
                failure = self._failure_record(
                    base,
                    "data_quality_failed",
                    diagnostics=diagnostics,
                    message=str(exc),
                )
                _, result = self.store.commit_failure(symbol, timeframe, failure)
                return result
            finally:
                del frame
            if overlay.empty:
                failure = self._failure_record(
                    base,
                    "no_fully_closed_overlay_bars",
                    diagnostics=diagnostics,
                    message="MT5 returned no fully closed bars inside the overlay interval",
                )
                _, result = self.store.commit_failure(symbol, timeframe, failure)
                return result
            if len(overlay) > self.request.bounds.max_cell_rows:
                failure = self._failure_record(
                    base,
                    "cell_row_bound_exceeded",
                    diagnostics=diagnostics,
                    message=(
                        f"overlay rows {len(overlay)} exceed cap "
                        f"{self.request.bounds.max_cell_rows}"
                    ),
                )
                del overlay
                _, result = self.store.commit_failure(symbol, timeframe, failure)
                return result
            try:
                overlap = _verify_overlap(baseline, overlay)
            except Exception as exc:
                status = (
                    "overlap_missing"
                    if "overlap rows" in str(exc)
                    else "overlap_mismatch"
                )
                failure = self._failure_record(
                    base,
                    status,
                    diagnostics=diagnostics,
                    message=str(exc),
                )
                del overlay
                _, result = self.store.commit_failure(symbol, timeframe, failure)
                return result
            actual_first = overlay.index[0].to_pydatetime()
            actual_last = overlay.index[-1].to_pydatetime()
            actual_last_close = pd.Timestamp(
                overlay["_BAR_CLOSE_TS"].iloc[-1]
            ).to_pydatetime()
            record = dict(_json_safe(base))
            record.update(
                {
                    "status": "success",
                    "failure_status": None,
                    "transport_status": "admitted_current_overlay",
                    "connector": diagnostics,
                    "overlap": overlap,
                    "coverage": {
                        "actual_first_open_utc": _iso(actual_first),
                        "actual_last_open_utc": _iso(actual_last),
                        "actual_last_close_utc": _iso(actual_last_close),
                        "requested_start_utc": _iso(baseline.request_start_utc),
                        "requested_end_exclusive_utc": _iso(self.request.cutoff_utc),
                    },
                    "acquisition_completed_utc": _iso(_utc(self.now_fn())),
                }
            )
            byte_cap = min(
                int(self.request.bounds.max_cell_bytes), int(run_bytes_remaining)
            )
            if byte_cap <= 0:
                del overlay
                failure = self._failure_record(
                    base,
                    "run_byte_bound_exceeded",
                    diagnostics=diagnostics,
                    message="no run byte budget remains",
                )
                _, result = self.store.commit_failure(symbol, timeframe, failure)
                return result
            try:
                _, result = self.store.commit_success(
                    symbol,
                    timeframe,
                    record,
                    overlay,
                    max_bytes=byte_cap,
                )
                return result
            except OverlayBoundExceeded as exc:
                failure = self._failure_record(
                    base,
                    "cell_or_run_byte_bound_exceeded",
                    diagnostics=diagnostics,
                    message=str(exc),
                )
                _, result = self.store.commit_failure(symbol, timeframe, failure)
                return result
            except Exception as exc:
                # A failed CSV/provenance write never exposes its staging tree.
                # If the store remains writable, preserve the infrastructure
                # state as one provenance-only leaf for deterministic resume.
                failure = self._failure_record(
                    base,
                    "write_failed",
                    diagnostics=diagnostics,
                    message=f"{type(exc).__name__}: {exc}",
                )
                _, result = self.store.commit_failure(symbol, timeframe, failure)
                return result
            finally:
                del overlay
        finally:
            # Baseline tail and current frame are scoped to one cell and released
            # before the next 1/600 cell is loaded.
            del baseline
            gc.collect()

    def _expected_request(self, baseline: BaselineCell) -> Dict[str, Any]:
        return {
            "start_utc": _iso(baseline.request_start_utc),
            "end_exclusive_utc": _iso(self.request.cutoff_utc),
            "common_current_closed_cutoff_utc": _iso(self.request.cutoff_utc),
            "overlap_policy": "trailing_actual_baseline_bars_v1",
            "overlap_bars_requested": int(self.request.bounds.overlap_bars),
        }

    def _validate_existing(self, symbol: str, timeframe: str) -> Dict[str, Any]:
        baseline = self.catalog.load_cell(
            symbol, timeframe, self.request.bounds.overlap_bars
        )
        leaf = self.store.leaf_path(symbol, timeframe)
        if not leaf.is_dir() or leaf.is_symlink():
            raise OverlayValidationError(f"existing overlay leaf is unsafe: {leaf}")
        provenance_path = leaf / "provenance.json"
        record = _load_json(provenance_path)
        exact = {
            "schema_version": LEAF_SCHEMA_VERSION,
            "contract_version": CONTRACT_VERSION,
            "run_id": self.store.run_id,
            "immutable_record": True,
            "canonical_symbol": symbol,
            "timeframe": timeframe,
            "source_identity": dict(self.source_identity),
            "baseline": dict(baseline.baseline_pin),
            "request": self._expected_request(baseline),
            "bounds": {
                "max_span_days": float(self.request.bounds.max_span_days),
                "max_cell_rows": int(self.request.bounds.max_cell_rows),
                "max_cell_bytes": int(self.request.bounds.max_cell_bytes),
                "max_run_bytes": int(self.request.bounds.max_run_bytes),
            },
        }
        for name, expected in exact.items():
            if record.get(name) != _json_safe(expected):
                raise OverlayValidationError(
                    f"existing overlay {name} mismatch in {provenance_path}"
                )
        status = record.get("status")
        artifact = record.get("artifact")
        entries = {item.name for item in leaf.iterdir()}
        if status == "success":
            if entries != {"bars.csv", "provenance.json"} or not isinstance(
                artifact, Mapping
            ):
                raise OverlayValidationError(
                    f"successful overlay leaf is incomplete: {leaf}"
                )
            bars_path = leaf / "bars.csv"
            observed = {
                "sha256": _sha256(bars_path),
                "rows": _count_csv_rows(bars_path),
                "bytes": int(bars_path.stat().st_size),
            }
            if any(artifact.get(name) != value for name, value in observed.items()):
                raise OverlayValidationError(
                    f"existing overlay artifact hash mismatch: {leaf}"
                )
        elif status == "failed":
            if entries != {"provenance.json"} or artifact is not None:
                raise OverlayValidationError(
                    f"failed overlay leaf is not provenance-only: {leaf}"
                )
            observed = {"sha256": None, "rows": 0, "bytes": 0}
        else:
            raise OverlayValidationError(f"unknown overlay leaf status: {leaf}")
        return {
            "canonical_symbol": symbol,
            "broker_symbol": record.get("broker_symbol"),
            "timeframe": timeframe,
            "status": status,
            "failure_status": record.get("failure_status"),
            "relative_leaf": f"{symbol}/{timeframe}",
            "provenance_sha256": _sha256(provenance_path),
            "bars_sha256": observed["sha256"],
            "rows": int(observed["rows"]),
            "bytes": int(observed["bytes"]),
        }

    def acquire(self, *, max_new_cells: Optional[int] = None) -> Dict[str, Any]:
        if max_new_cells is not None and int(max_new_cells) <= 0:
            raise ValueError("max_new_cells must be positive")
        manifest_path = self.store.run_root / "run_manifest.json"
        if manifest_path.exists():
            raise OverlayValidationError("completed overlay run cannot be resumed")
        started = _utc(self.now_fn())
        results: List[Dict[str, Any]] = []
        new_cells = 0
        run_bytes = 0
        target = [
            (symbol, timeframe)
            for symbol in self.request.symbols
            for timeframe in self.request.timeframes
        ]
        existing = self.store.discover_final_leaves()
        extras = sorted(set(existing) - set(target))
        if extras:
            raise OverlayValidationError(
                f"overlay run contains leaves outside the exact resume target: {extras}"
            )
        for symbol, timeframe in target:
            leaf = self.store.leaf_path(symbol, timeframe)
            if leaf.exists():
                result = self._validate_existing(symbol, timeframe)
            else:
                if max_new_cells is not None and new_cells >= int(max_new_cells):
                    break
                result = self._acquire_cell(
                    symbol,
                    timeframe,
                    run_bytes_remaining=self.request.bounds.max_run_bytes - run_bytes,
                )
                new_cells += 1
            run_bytes += int(result["bytes"])
            if run_bytes > self.request.bounds.max_run_bytes:
                raise OverlayBoundExceeded("validated overlay run exceeds its byte cap")
            results.append(result)
        complete = len(results) == len(target)
        counts = {
            "total": len(results),
            "success": sum(item["status"] == "success" for item in results),
            "failed": sum(item["status"] == "failed" for item in results),
            "required": len(target),
        }
        manifest: Dict[str, Any] = {
            "schema_version": MANIFEST_SCHEMA_VERSION,
            "contract_version": CONTRACT_VERSION,
            "run_id": self.store.run_id,
            "complete": complete,
            "immutable_record": complete,
            "source_identity": dict(self.source_identity),
            "baseline": dict(self.catalog.baseline),
            "request": {
                "symbols": list(self.request.symbols),
                "timeframes": list(self.request.timeframes),
                "common_current_closed_cutoff_utc": _iso(self.request.cutoff_utc),
                "bounds": _json_safe(self.request.bounds.__dict__),
            },
            "counts": counts,
            "run_artifact_bytes": int(run_bytes),
            "results": results,
            "acquisition_started_utc": _iso(started),
            "acquisition_completed_utc": _iso(_utc(self.now_fn())),
            "resume": {
                "reused_cells": len(results) - new_cells,
                "newly_acquired_cells": new_cells,
            },
        }
        if complete:
            self.store.commit_manifest(manifest)
        return manifest


def load_overlay_manifest(native_root: Path, run_id: str) -> Dict[str, Any]:
    """Load a completed manifest; source-chain code still verifies its own pins."""

    root = _normalized_native_root(native_root)
    path = (
        root
        / "overlays"
        / "runs"
        / _safe_name(run_id, "overlay run id")
        / "run_manifest.json"
    )
    manifest = _load_json(path)
    if (
        manifest.get("schema_version") != MANIFEST_SCHEMA_VERSION
        or manifest.get("complete") is not True
        or manifest.get("immutable_record") is not True
    ):
        raise OverlayValidationError(
            f"overlay manifest is not complete/immutable: {path}"
        )
    return manifest


def validate_overlay_leaf(
    native_root: Path, run_id: str, result: Mapping[str, Any]
) -> Dict[str, Any]:
    """Hash-check one manifest-selected overlay leaf without loading its bars."""

    root = _normalized_native_root(native_root)
    relative = str(result.get("relative_leaf") or "")
    parts = Path(relative).parts
    if len(parts) != 2:
        raise OverlayValidationError(
            "overlay result relative_leaf must have two components"
        )
    symbol = _safe_name(parts[0], "symbol")
    timeframe = _safe_name(parts[1], "timeframe").upper()
    if symbol not in PRODUCTION_SYMBOL_ORDER or timeframe not in DEFAULT_TIMEFRAMES:
        raise OverlayValidationError("overlay result names a non-production cell")
    leaf = (
        root
        / "overlays"
        / "runs"
        / _safe_name(run_id, "overlay run id")
        / symbol
        / timeframe
    )
    provenance = leaf / "provenance.json"
    if _sha256(provenance) != result.get("provenance_sha256"):
        raise OverlayValidationError(f"overlay provenance hash mismatch: {leaf}")
    record = _load_json(provenance)
    artifact = record.get("artifact")
    if record.get("status") == "success":
        bars = leaf / "bars.csv"
        if not isinstance(artifact, Mapping) or _sha256(bars) != result.get(
            "bars_sha256"
        ):
            raise OverlayValidationError(f"overlay bars hash mismatch: {leaf}")
    return record


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Acquire a bounded current MT5 overlay on an immutable native origin."
    )
    parser.add_argument("--config", default="config.json")
    parser.add_argument("--output-root", default="data/native")
    parser.add_argument("--baseline-run-id", default=DEFAULT_BASELINE_RUN_ID)
    parser.add_argument("--run-id")
    parser.add_argument(
        "--cutoff", help="Aware ISO UTC cutoff; defaults to current UTC"
    )
    parser.add_argument("--symbols", nargs="+")
    parser.add_argument("--timeframes", nargs="+")
    parser.add_argument("--allow-subset", action="store_true")
    parser.add_argument("--overlap-bars", type=int, default=DEFAULT_OVERLAP_BARS)
    parser.add_argument("--max-span-days", type=float, default=DEFAULT_MAX_SPAN_DAYS)
    parser.add_argument("--max-cell-rows", type=int, default=DEFAULT_MAX_CELL_ROWS)
    parser.add_argument("--max-cell-bytes", type=int, default=DEFAULT_MAX_CELL_BYTES)
    parser.add_argument("--max-run-bytes", type=int, default=DEFAULT_MAX_RUN_BYTES)
    parser.add_argument("--max-new-cells", type=int)
    parser.add_argument(
        "--max-runtime-seconds",
        type=float,
        help=(
            "Hard worker deadline; execute defaults to 3600 seconds and may be "
            f"overridden only up to {MAX_EXECUTION_RUNTIME_SECONDS:.0f} seconds."
        ),
    )
    parser.add_argument("--resume", action="store_true")
    parser.add_argument("--execute", action="store_true")
    parser.add_argument("--log-level", default="INFO")
    parser.add_argument(
        "--_supervised-worker", action="store_true", help=argparse.SUPPRESS
    )
    return parser


def _run_supervised_worker(
    argv: Optional[Sequence[str]], timeout_seconds: float
) -> int:
    """Run a killable child while enforcing the released MT5 cache guard."""

    child_argv = list(sys.argv[1:] if argv is None else argv)
    child_argv.append("--_supervised-worker")
    command = [sys.executable, str(Path(__file__).resolve()), *child_argv]
    initial_snapshot = _supervisor_terminal_cache_snapshot()
    if initial_snapshot.get("violations"):
        print(
            json.dumps(
                {
                    "status": "terminal_cache_resource_limit",
                    "snapshot": initial_snapshot,
                    "published_partial_cell": False,
                    "resume_required": True,
                },
                sort_keys=True,
            ),
            file=sys.stderr,
        )
        return RESOURCE_LIMIT_EXIT_CODE

    process = subprocess.Popen(command)
    started = time.monotonic()
    while True:
        remaining = float(timeout_seconds) - (time.monotonic() - started)
        if remaining <= 0.0:
            _terminate_supervised_worker(process)
            print(
                json.dumps(
                    {
                        "status": "hard_runtime_timeout",
                        "timeout_seconds": float(timeout_seconds),
                        "published_partial_cell": False,
                        "resume_required": True,
                    },
                    sort_keys=True,
                ),
                file=sys.stderr,
            )
            return HARD_TIMEOUT_EXIT_CODE
        try:
            return int(process.wait(timeout=min(SUPERVISOR_POLL_SECONDS, remaining)))
        except subprocess.TimeoutExpired:
            snapshot = _supervisor_terminal_cache_snapshot()
            if not snapshot.get("violations"):
                continue
            _terminate_supervised_worker(process)
            print(
                json.dumps(
                    {
                        "status": "terminal_cache_resource_limit",
                        "snapshot": snapshot,
                        "published_partial_cell": False,
                        "resume_required": True,
                    },
                    sort_keys=True,
                ),
                file=sys.stderr,
            )
            return RESOURCE_LIMIT_EXIT_CODE


def _load_config(path: Path) -> Dict[str, Any]:
    return _load_json(path)


def _main_with_args(args: argparse.Namespace) -> int:
    config_path = Path(args.config).resolve()
    config = _load_config(config_path)
    native_root = Path(args.output_root)
    if not native_root.is_absolute():
        native_root = (config_path.parent / native_root).resolve()
    native_root = _normalized_native_root(native_root)
    symbols = tuple(args.symbols or PRODUCTION_SYMBOL_ORDER)
    timeframes = tuple(
        str(value).upper() for value in (args.timeframes or DEFAULT_TIMEFRAMES)
    )
    if not args.allow_subset and (
        symbols != tuple(PRODUCTION_SYMBOL_ORDER)
        or timeframes != tuple(DEFAULT_TIMEFRAMES)
    ):
        raise ValueError(
            "CLI overlay acquisition requires the exact 100x6 roster unless --allow-subset is explicit"
        )
    cutoff = (
        parse_utc_datetime(args.cutoff) if args.cutoff else datetime.now(timezone.utc)
    )
    run_id = args.run_id or f"native_overlay_{cutoff.strftime('%Y%m%dT%H%M%SZ')}"
    bounds = OverlayBounds(
        overlap_bars=args.overlap_bars,
        max_span_days=args.max_span_days,
        max_cell_rows=args.max_cell_rows,
        max_cell_bytes=args.max_cell_bytes,
        max_run_bytes=args.max_run_bytes,
    )
    request = OverlayRequest(
        symbols=symbols,
        timeframes=timeframes,
        cutoff_utc=cutoff,
        baseline_run_id=args.baseline_run_id,
        bounds=bounds,
    )
    plan = {
        "contract_version": CONTRACT_VERSION,
        "run_id": run_id,
        "baseline_run_id": request.baseline_run_id,
        "symbols": list(request.symbols),
        "timeframes": list(request.timeframes),
        "cells": len(request.symbols) * len(request.timeframes),
        "cutoff_utc": _iso(request.cutoff_utc),
        "bounds": _json_safe(bounds.__dict__),
        "output_root": str(native_root),
        "resume": bool(args.resume),
        "max_runtime_seconds": args.max_runtime_seconds,
    }
    print(json.dumps(plan, indent=2, sort_keys=True))
    if not args.execute:
        return 0
    connector: Any = None
    terminal_lock = NativeHistoryTerminalLock(native_root).acquire()
    run_lock = NativeHistoryRunLock(native_root, f"overlay-{run_id}").acquire()
    try:
        import pm_mt5 as pm_mt5_module
        from pm_mt5 import MT5Config, MT5Connector

        mt5_values = dict(config.get("mt5") or {})
        if "terminal_path" in mt5_values and "path" not in mt5_values:
            mt5_values["path"] = mt5_values["terminal_path"]
        connector = MT5Connector(
            MT5Config(**_filtered_dataclass_kwargs(MT5Config, mt5_values))
        )
        if not connector.connect():
            print(
                f"MT5 connection failed: {connector.get_last_error()}", file=sys.stderr
            )
            return 2
        runtime = snapshot_read_only_runtime_metadata(
            connector, getattr(pm_mt5_module, "mt5", None)
        )
        store = OverlayStore(native_root, run_id, resume=args.resume)
        manifest = NativeOverlayAcquirer(
            connector,
            store,
            request,
            runtime_metadata=runtime,
            require_production_baseline=not args.allow_subset,
        ).acquire(max_new_cells=args.max_new_cells)
    finally:
        if connector is not None:
            connector.disconnect()
        run_lock.release()
        terminal_lock.release()
    print(json.dumps(_json_safe(manifest), indent=2, sort_keys=True))
    if not manifest["complete"]:
        return 4
    return 0 if int(manifest["counts"]["failed"]) == 0 else 3


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = _build_parser().parse_args(argv)
    logging.basicConfig(
        level=getattr(logging, str(args.log_level).upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    if args.execute and args.max_runtime_seconds is None:
        args.max_runtime_seconds = DEFAULT_EXECUTION_RUNTIME_SECONDS
    if args.max_runtime_seconds is not None:
        if (
            not math.isfinite(args.max_runtime_seconds)
            or args.max_runtime_seconds <= 0.0
            or args.max_runtime_seconds > MAX_EXECUTION_RUNTIME_SECONDS
        ):
            raise ValueError(
                "--max-runtime-seconds must be finite, positive, and no greater "
                f"than {MAX_EXECUTION_RUNTIME_SECONDS:.0f}"
            )
        if args.execute and not args._supervised_worker:
            return _run_supervised_worker(argv, args.max_runtime_seconds)
    return _main_with_args(args)


if __name__ == "__main__":
    raise SystemExit(main())
