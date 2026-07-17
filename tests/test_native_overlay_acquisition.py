import gc
import json
import subprocess
import sys
import tempfile
import unittest
import weakref
from contextlib import redirect_stderr, redirect_stdout
from datetime import datetime, timedelta, timezone
from io import StringIO
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

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
    RESOURCE_LIMIT_EXIT_CODE,
)
from tools.acquire_native_overlay import (  # noqa: E402
    CONTRACT_VERSION,
    LEAF_SCHEMA_VERSION,
    MANIFEST_SCHEMA_VERSION,
    MAX_EXECUTION_RUNTIME_SECONDS,
    BaselineCatalog,
    NativeOverlayAcquirer,
    OverlayBounds,
    OverlayRequest,
    OverlayStore,
    OverlayValidationError,
    _build_parser,
    _normalize_overlay_frame,
    _run_supervised_worker,
    load_overlay_manifest,
    main,
    validate_overlay_leaf,
)


UTC = timezone.utc
SOURCE_ENVIRONMENT = {
    "account": {
        "company": "Test Broker",
        "server": "Test-Server",
        "login": 123456,
        "currency": "USD",
    },
    "terminal": {"build": 5833, "max_bars": 100000000},
}
SOURCE_IDENTITY = {
    "broker": "Test Broker",
    "server": "Test-Server",
    "account_id": 123456,
    "source_kind": "broker_native",
}


def _iso(value):
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _sha(path):
    import hashlib

    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _raw_frame(index):
    index = pd.DatetimeIndex(pd.to_datetime(index, utc=True))
    epoch = index.asi8 // 1_000_000_000
    base = 10.0 + (epoch % 100_000) / 100_000.0
    return pd.DataFrame(
        {
            "Open": base,
            "High": base + 0.02,
            "Low": base - 0.02,
            "Close": base + 0.005,
            "Volume": np.full(len(index), 100, dtype=np.int64),
            "Spread": np.full(len(index), 3, dtype=np.int64),
            "RealVolume": np.zeros(len(index), dtype=np.int64),
        },
        index=index,
    )


def _origin_frame(index, timeframe):
    frame = _raw_frame(index)
    seconds = TIMEFRAME_SECONDS[timeframe]
    closes = frame.index + pd.Timedelta(seconds=seconds)
    frame["_SOURCE_FIRST_TS"] = frame.index
    frame["_SOURCE_LAST_TS"] = closes - pd.Timedelta(nanoseconds=1)
    frame["_BAR_CLOSE_TS"] = closes
    return frame


def _write_origin(
    native_root,
    cells,
    *,
    run_id="baseline",
    manifest_symbols=None,
    manifest_timeframes=None,
    include_all_results=False,
):
    run_root = native_root / "runs" / run_id
    run_root.mkdir(parents=True)
    manifest_symbols = tuple(manifest_symbols or dict.fromkeys(s for s, _ in cells))
    manifest_timeframes = tuple(
        manifest_timeframes or dict.fromkeys(t for _, t in cells)
    )
    frames = {}
    results = []
    for symbol, timeframe in cells:
        seconds = TIMEFRAME_SECONDS[timeframe]
        # Different broker-aligned opens are intentional: the overlay seam must
        # come from these bars, never a UTC-floor projection or request end.
        last_open = {
            "M5": datetime(2026, 7, 10, 11, 55, tzinfo=UTC),
            "M15": datetime(2026, 7, 10, 11, 45, tzinfo=UTC),
            "M30": datetime(2026, 7, 10, 11, 30, tzinfo=UTC),
            "H1": datetime(2026, 7, 10, 11, 0, tzinfo=UTC),
            "H4": datetime(2026, 7, 10, 9, 0, tzinfo=UTC),
            "D1": datetime(2026, 7, 9, 22, 0, tzinfo=UTC),
        }[timeframe]
        periods = 80
        index = pd.date_range(
            end=last_open,
            periods=periods,
            freq=pd.Timedelta(seconds=seconds),
            tz="UTC",
        )
        frame = _origin_frame(index, timeframe)
        frames[(symbol, timeframe)] = frame
        leaf = run_root / symbol / timeframe
        leaf.mkdir(parents=True)
        bars = leaf / "bars.csv"
        frame.to_csv(bars, lineterminator="\n")
        artifact = {
            "relative_path": "bars.csv",
            "sha256": _sha(bars),
            "rows": len(frame),
            "bytes": bars.stat().st_size,
        }
        actual_last_close = last_open + timedelta(seconds=seconds)
        provenance = {
            "schema_version": "fxpm-native-bars-v1",
            "acquisition_contract_version": "legacy-test-v1",
            "immutable_record": True,
            "run_id": run_id,
            "status": "success",
            "source_kind": "broker_native",
            "canonical_symbol": symbol,
            "broker_symbol": symbol.upper(),
            "timeframe": timeframe,
            "source_environment": SOURCE_ENVIRONMENT,
            "artifact": artifact,
            "sha256": artifact["sha256"],
            "rows": artifact["rows"],
            "coverage": {
                "actual_first_open_utc": _iso(index[0].to_pydatetime()),
                "actual_last_open_utc": _iso(last_open),
                "actual_last_close_utc": _iso(actual_last_close),
            },
            "request": {
                "last_fully_closed_bar_open_utc": _iso(last_open),
                # Deliberately unrelated to the actual seam.
                "native_fetch_end_exclusive_utc": "2099-01-01T00:00:00Z",
            },
        }
        (leaf / "provenance.json").write_text(
            json.dumps(provenance, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        results.append(
            {
                "symbol": symbol,
                "timeframe": timeframe,
                "status": "success",
                "failure_status": None,
                "relative_leaf": f"{symbol}/{timeframe}",
                "sha256": artifact["sha256"],
                "rows": artifact["rows"],
            }
        )
    if include_all_results:
        existing = {(item["symbol"], item["timeframe"]) for item in results}
        for symbol in manifest_symbols:
            for timeframe in manifest_timeframes:
                if (symbol, timeframe) not in existing:
                    results.append(
                        {
                            "symbol": symbol,
                            "timeframe": timeframe,
                            "status": "success",
                            "failure_status": None,
                            "relative_leaf": f"{symbol}/{timeframe}",
                            "sha256": "0" * 64,
                            "rows": 1,
                        }
                    )
    manifest = {
        "schema_version": "fxpm.native_history_run_manifest.v1",
        "complete": True,
        "immutable_record": True,
        "request": {
            "symbols": list(manifest_symbols),
            "timeframes": list(manifest_timeframes),
        },
        "results": results,
    }
    (run_root / "run_manifest.json").write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    return frames


def _diagnostics(
    *,
    relation="IN_RANGE",
    sync_status="data_current",
    hard=False,
    ledger_entries=1,
):
    ledger = []
    for sequence in range(ledger_entries):
        if sequence == 1:
            call_type = "copy_rates_from_warm"
        elif sequence == 0:
            call_type = "copy_rates_range_initial"
        else:
            call_type = "copy_rates_range_retry"
        ledger.append(
            {
                "sequence": sequence + 1,
                "call_type": call_type,
                "last_error": {"code": 1, "message": "Success"},
                "hard_failure": hard,
                "raw_relation": relation,
            }
        )
    return {
        "range_contract_version": "mt5-half-open-range-v4",
        "sync_retry": True,
        "sync_status": sync_status,
        "raw_relation": relation,
        "hard_failure": hard,
        "copy_rates_range_attempts": sum(
            item["call_type"].startswith("copy_rates_range_") for item in ledger
        ),
        "history_download_trigger_attempts": sum(
            item["call_type"] == "copy_rates_from_warm" for item in ledger
        ),
        "call_ledger": ledger,
    }


class _Connector:
    def __init__(self, modes=None, resident_check=False):
        self.modes = dict(modes or {})
        self.calls = []
        self.last_diagnostics = {}
        self.previous = None
        self.resident_check = resident_check

    def get_bars_range(self, symbol, timeframe, start, end, sync_retry=False):
        if self.resident_check and self.previous is not None:
            gc.collect()
            if self.previous() is not None:
                raise AssertionError("previous connector frame remained resident")
        self.calls.append((symbol, timeframe, start, end, sync_retry))
        mode = self.modes.get((symbol, timeframe), "data")
        if mode == "right":
            self.last_diagnostics = _diagnostics(
                relation="RIGHT_DISJOINT_PREFIX",
                sync_status="terminal_prefix_unsynchronized",
            )
            return None
        if mode == "left":
            self.last_diagnostics = _diagnostics(
                relation="LEFT_DISJOINT_SUFFIX",
                sync_status="terminal_suffix_unsynchronized",
            )
            return None
        if mode == "hard":
            self.last_diagnostics = _diagnostics(
                relation=None, sync_status="hard_failure", hard=True
            )
            return None
        if mode == "raise":
            self.last_diagnostics = _diagnostics(
                relation=None, sync_status="exception", hard=True
            )
            raise TimeoutError("mock IPC timeout")
        if mode == "empty":
            self.last_diagnostics = _diagnostics(relation=None, sync_status="empty")
            return None
        if mode == "invalid-ledger":
            self.last_diagnostics = _diagnostics(ledger_entries=4)
            return _raw_frame(
                pd.date_range(start, end - timedelta(minutes=5), periods=2)
            )
        seconds = TIMEFRAME_SECONDS[timeframe]
        first = start
        if mode == "no-overlap":
            first = start + timedelta(seconds=seconds * 70)
        index = pd.date_range(
            start=first,
            end=end - timedelta(seconds=seconds),
            freq=pd.Timedelta(seconds=seconds),
            tz="UTC",
        )
        frame = _raw_frame(index)
        self.last_diagnostics = _diagnostics()
        frame.attrs["mt5_history_range_diagnostics"] = self.last_diagnostics
        if self.resident_check:
            self.previous = weakref.ref(frame)
        return frame

    def get_last_history_range_diagnostics(self):
        return dict(self.last_diagnostics)


class NativeOverlayAcquisitionTests(unittest.TestCase):
    def _root(self, temp):
        native = Path(temp) / "data" / "native"
        native.mkdir(parents=True)
        return native

    def _request(self, symbols, timeframes, cutoff, **bound_values):
        return OverlayRequest(
            symbols=tuple(symbols),
            timeframes=tuple(timeframes),
            cutoff_utc=cutoff,
            baseline_run_id="baseline",
            bounds=OverlayBounds(**bound_values),
        )

    def _run(
        self,
        native,
        cells,
        connector,
        cutoff,
        *,
        run_id="overlay",
        bounds=None,
        resume=False,
        max_new_cells=None,
    ):
        request = OverlayRequest(
            symbols=tuple(dict.fromkeys(symbol for symbol, _ in cells)),
            timeframes=tuple(dict.fromkeys(timeframe for _, timeframe in cells)),
            cutoff_utc=cutoff,
            baseline_run_id="baseline",
            bounds=bounds or OverlayBounds(),
        )
        return NativeOverlayAcquirer(
            connector,
            OverlayStore(native, run_id, resume=resume),
            request,
            runtime_metadata=SOURCE_ENVIRONMENT,
            require_production_baseline=False,
            now_fn=lambda: datetime(2026, 7, 14, tzinfo=UTC),
        ).acquire(max_new_cells=max_new_cells)

    def test_default_authorities_are_exact_100_by_6_roster(self):
        args = _build_parser().parse_args([])
        self.assertIsNone(args.symbols)
        self.assertIsNone(args.timeframes)
        self.assertEqual(len(PRODUCTION_SYMBOL_ORDER), 100)
        self.assertEqual(
            tuple(DEFAULT_TIMEFRAMES), ("M5", "M15", "M30", "H1", "H4", "D1")
        )
        self.assertEqual(len(PRODUCTION_SYMBOL_ORDER) * len(DEFAULT_TIMEFRAMES), 600)

    def test_preserved_legacy_metal_casing_maps_to_canonical_roster(self):
        with tempfile.TemporaryDirectory() as temp:
            native = self._root(temp)
            observed = list(PRODUCTION_SYMBOL_ORDER)
            observed[observed.index("Platinum")] = "PLATINUM"
            observed[observed.index("Palladium")] = "PALLADIUM"
            _write_origin(
                native,
                [("PLATINUM", "M5")],
                manifest_symbols=observed,
                manifest_timeframes=DEFAULT_TIMEFRAMES,
                include_all_results=True,
            )
            catalog = BaselineCatalog(native, "baseline")
            cell = catalog.load_cell("Platinum", "M5", 64)
            self.assertEqual(cell.canonical_symbol, "Platinum")
            self.assertEqual(cell.baseline_pin["relative_leaf"], "PLATINUM/M5")

    def test_baseline_duplicate_keys_and_false_immutable_pins_are_rejected(self):
        for defect in ("duplicate", "immutable"):
            with self.subTest(defect=defect), tempfile.TemporaryDirectory() as temp:
                native = self._root(temp)
                _write_origin(native, [("EURUSD", "M5")])
                provenance = native / "runs/baseline/EURUSD/M5/provenance.json"
                text = provenance.read_text(encoding="utf-8")
                if defect == "duplicate":
                    text = text.replace(
                        '"status": "success",',
                        '"status": "success",\n  "status": "success",',
                    )
                else:
                    text = text.replace(
                        '"immutable_record": true', '"immutable_record": false'
                    )
                provenance.write_text(text, encoding="utf-8")
                catalog = BaselineCatalog(
                    native, "baseline", require_production_roster=False
                )
                with self.assertRaises(OverlayValidationError):
                    catalog.load_cell("EURUSD", "M5", 64)

    def test_m5_success_uses_actual_tail_seam_and_hash_bound_manifest(self):
        with tempfile.TemporaryDirectory() as temp:
            native = self._root(temp)
            _write_origin(native, [("EURUSD", "M5")])
            cutoff = datetime(2026, 7, 10, 13, 0, tzinfo=UTC)
            connector = _Connector()
            manifest = self._run(native, [("EURUSD", "M5")], connector, cutoff)
            self.assertTrue(manifest["complete"])
            self.assertEqual(
                manifest["counts"],
                {"total": 1, "success": 1, "failed": 0, "required": 1},
            )
            call = connector.calls[0]
            catalog = BaselineCatalog(
                native, "baseline", require_production_roster=False
            )
            baseline = catalog.load_cell("EURUSD", "M5", 64)
            self.assertEqual(call[2], baseline.tail.index[0].to_pydatetime())
            self.assertNotEqual(call[2].year, 2099)
            self.assertEqual(call[3], cutoff)
            self.assertIs(call[4], True)
            persisted = load_overlay_manifest(native, "overlay")
            self.assertEqual(persisted["schema_version"], MANIFEST_SCHEMA_VERSION)
            self.assertEqual(persisted["source_identity"], SOURCE_IDENTITY)
            result = persisted["results"][0]
            record = validate_overlay_leaf(native, "overlay", result)
            self.assertEqual(record["schema_version"], LEAF_SCHEMA_VERSION)
            self.assertEqual(record["contract_version"], CONTRACT_VERSION)
            self.assertEqual(record["overlap"]["status"], "verified")
            self.assertEqual(record["request"]["end_exclusive_utc"], _iso(cutoff))
            header = (
                (native / "overlays/runs/overlay/EURUSD/M5/bars.csv")
                .read_text(encoding="utf-8")
                .splitlines()[0]
            )
            self.assertEqual(
                header,
                "time,Open,High,Low,Close,Volume,Spread,RealVolume,"
                "_SOURCE_FIRST_TS,_SOURCE_LAST_TS,_BAR_CLOSE_TS",
            )

    def test_h4_and_d1_seams_use_actual_broker_aligned_closes(self):
        for timeframe in ("H4", "D1"):
            with (
                self.subTest(timeframe=timeframe),
                tempfile.TemporaryDirectory() as temp,
            ):
                native = self._root(temp)
                _write_origin(native, [("EURUSD", timeframe)])
                cutoff = datetime(2026, 7, 13, 22, 0, tzinfo=UTC)
                connector = _Connector()
                manifest = self._run(native, [("EURUSD", timeframe)], connector, cutoff)
                self.assertEqual(manifest["counts"]["success"], 1)
                baseline = BaselineCatalog(
                    native, "baseline", require_production_roster=False
                ).load_cell("EURUSD", timeframe, 64)
                self.assertEqual(
                    connector.calls[0][2], baseline.tail.index[0].to_pydatetime()
                )
                record = validate_overlay_leaf(
                    native, "overlay", manifest["results"][0]
                )
                self.assertEqual(
                    record["baseline"]["actual_last_close_utc"],
                    _iso(baseline.actual_last_close_utc),
                )
                self.assertLessEqual(
                    pd.Timestamp(record["coverage"]["actual_last_close_utc"]),
                    pd.Timestamp(cutoff),
                )

    def test_disjoint_hard_and_empty_are_typed_technical_failures(self):
        cases = {
            "right": "terminal_prefix_unsynchronized",
            "left": "terminal_suffix_unsynchronized",
            "hard": "terminal_hard_failure",
            "raise": "terminal_hard_failure",
            "empty": "terminal_empty_unresolved",
        }
        for mode, expected in cases.items():
            with self.subTest(mode=mode), tempfile.TemporaryDirectory() as temp:
                native = self._root(temp)
                _write_origin(native, [("EURUSD", "M5")])
                connector = _Connector({("EURUSD", "M5"): mode})
                manifest = self._run(
                    native,
                    [("EURUSD", "M5")],
                    connector,
                    datetime(2026, 7, 10, 13, 0, tzinfo=UTC),
                )
                result = manifest["results"][0]
                self.assertEqual(result["failure_status"], expected)
                record = validate_overlay_leaf(native, "overlay", result)
                self.assertFalse(record["error"]["economic_verdict"])
                self.assertFalse(record["error"]["broker_absence_inferred"])
                self.assertEqual(
                    {
                        item.name
                        for item in (
                            native / "overlays/runs/overlay/EURUSD/M5"
                        ).iterdir()
                    },
                    {"provenance.json"},
                )

    def test_no_overlap_and_mismatched_overlap_cannot_publish_bars(self):
        for mode, expected in (
            ("no-overlap", "overlap_missing"),
            ("mismatch", "overlap_mismatch"),
        ):
            with self.subTest(mode=mode), tempfile.TemporaryDirectory() as temp:
                native = self._root(temp)
                _write_origin(native, [("EURUSD", "M5")])
                connector = _Connector(
                    {("EURUSD", "M5"): "no-overlap" if mode == "no-overlap" else "data"}
                )
                if mode == "mismatch":
                    original = connector.get_bars_range

                    def corrupt(*args, **kwargs):
                        frame = original(*args, **kwargs)
                        frame.iloc[0, frame.columns.get_loc("Close")] += 0.001
                        return frame

                    connector.get_bars_range = corrupt
                manifest = self._run(
                    native,
                    [("EURUSD", "M5")],
                    connector,
                    datetime(2026, 7, 10, 18, 0, tzinfo=UTC),
                )
                self.assertEqual(manifest["results"][0]["failure_status"], expected)
                self.assertFalse(
                    (native / "overlays/runs/overlay/EURUSD/M5/bars.csv").exists()
                )

    def test_atomic_write_failure_exposes_no_leaf_or_staging(self):
        with tempfile.TemporaryDirectory() as temp:
            native = self._root(temp)
            store = OverlayStore(native, "atomic")
            frame = _raw_frame([datetime(2026, 7, 10, tzinfo=UTC)])
            with patch.object(pd.DataFrame, "to_csv", side_effect=OSError("disk full")):
                with self.assertRaises(OSError):
                    store.commit_success(
                        "EURUSD", "M5", {"status": "success"}, frame, max_bytes=1000
                    )
            leaf = store.leaf_path("EURUSD", "M5")
            self.assertFalse(leaf.exists())
            self.assertFalse(list(leaf.parent.glob(".*.staging")))

    def test_cell_row_byte_and_staleness_bounds_fail_closed(self):
        scenarios = [
            (
                OverlayBounds(max_cell_rows=10),
                datetime(2026, 7, 10, 18, 0, tzinfo=UTC),
                "cell_row_bound_exceeded",
            ),
            (
                OverlayBounds(max_cell_bytes=100, max_run_bytes=1000),
                datetime(2026, 7, 10, 13, 0, tzinfo=UTC),
                "cell_or_run_byte_bound_exceeded",
            ),
            (
                OverlayBounds(max_span_days=0.01),
                datetime(2026, 7, 10, 13, 0, tzinfo=UTC),
                "baseline_too_stale_for_overlay",
            ),
        ]
        for bounds, cutoff, expected in scenarios:
            with self.subTest(expected=expected), tempfile.TemporaryDirectory() as temp:
                native = self._root(temp)
                _write_origin(native, [("EURUSD", "M5")])
                manifest = self._run(
                    native,
                    [("EURUSD", "M5")],
                    _Connector(),
                    cutoff,
                    bounds=bounds,
                )
                self.assertEqual(manifest["results"][0]["failure_status"], expected)

    def test_acquisition_is_single_retry_owner_and_rejects_unbounded_ledger(self):
        with tempfile.TemporaryDirectory() as temp:
            native = self._root(temp)
            _write_origin(native, [("EURUSD", "M5")])
            connector = _Connector({("EURUSD", "M5"): "invalid-ledger"})
            manifest = self._run(
                native,
                [("EURUSD", "M5")],
                connector,
                datetime(2026, 7, 10, 13, 0, tzinfo=UTC),
            )
            self.assertEqual(len(connector.calls), 1)
            self.assertIs(connector.calls[0][4], True)
            self.assertEqual(
                manifest["results"][0]["failure_status"], "connector_contract_invalid"
            )

    def test_resume_validates_hash_splice_and_adds_only_missing_leaf(self):
        with tempfile.TemporaryDirectory() as temp:
            native = self._root(temp)
            cells = [("EURUSD", "M5"), ("EURUSD", "H4")]
            _write_origin(native, cells)
            cutoff = datetime(2026, 7, 13, 22, 0, tzinfo=UTC)
            first = _Connector()
            partial = self._run(
                native,
                cells,
                first,
                cutoff,
                run_id="resume",
                max_new_cells=1,
            )
            self.assertFalse(partial["complete"])
            self.assertEqual(len(first.calls), 1)
            second = _Connector()
            complete = self._run(
                native,
                cells,
                second,
                cutoff,
                run_id="resume",
                resume=True,
            )
            self.assertTrue(complete["complete"])
            self.assertEqual(
                complete["resume"], {"reused_cells": 1, "newly_acquired_cells": 1}
            )
            self.assertEqual(len(second.calls), 1)
            provenance = native / "overlays/runs/resume/EURUSD/M5/provenance.json"
            value = json.loads(provenance.read_text(encoding="utf-8"))
            value["baseline"]["bars_sha256"] = "f" * 64
            provenance.write_text(json.dumps(value), encoding="utf-8")
            with self.assertRaises(OverlayValidationError):
                validate_overlay_leaf(native, "resume", complete["results"][0])

    def test_resume_rejects_unreferenced_extra_leaf_before_fetch(self):
        with tempfile.TemporaryDirectory() as temp:
            native = self._root(temp)
            cells = [("EURUSD", "M5"), ("EURUSD", "H4")]
            _write_origin(native, cells)
            cutoff = datetime(2026, 7, 13, 22, 0, tzinfo=UTC)
            self._run(
                native,
                cells,
                _Connector(),
                cutoff,
                run_id="extra",
                max_new_cells=1,
            )
            extra = native / "overlays/runs/extra/AUDUSD/M5"
            extra.mkdir(parents=True)
            (extra / "provenance.json").write_text("{}", encoding="utf-8")
            connector = _Connector()
            with self.assertRaisesRegex(
                OverlayValidationError, "outside the exact resume target"
            ):
                self._run(
                    native,
                    cells,
                    connector,
                    cutoff,
                    run_id="extra",
                    resume=True,
                )
            self.assertEqual(connector.calls, [])

    def test_acquirer_converts_atomic_write_error_to_typed_failure_leaf(self):
        with tempfile.TemporaryDirectory() as temp:
            native = self._root(temp)
            _write_origin(native, [("EURUSD", "M5")])
            store = OverlayStore(native, "write-failure")
            request = self._request(
                ["EURUSD"],
                ["M5"],
                datetime(2026, 7, 10, 13, 0, tzinfo=UTC),
            )
            acquirer = NativeOverlayAcquirer(
                _Connector(),
                store,
                request,
                runtime_metadata=SOURCE_ENVIRONMENT,
                require_production_baseline=False,
            )
            with patch.object(
                store, "commit_success", side_effect=OSError("disk full")
            ):
                manifest = acquirer.acquire()
            self.assertEqual(manifest["results"][0]["failure_status"], "write_failed")
            self.assertEqual(
                {item.name for item in store.leaf_path("EURUSD", "M5").iterdir()},
                {"provenance.json"},
            )

    def test_forming_bar_is_removed_at_common_cutoff(self):
        cutoff = datetime(2026, 7, 10, 12, 2, tzinfo=UTC)
        frame = _raw_frame(
            [
                datetime(2026, 7, 10, 11, 55, tzinfo=UTC),
                datetime(2026, 7, 10, 12, 0, tzinfo=UTC),
            ]
        )
        normalized = _normalize_overlay_frame(
            frame,
            "M5",
            datetime(2026, 7, 10, 11, 55, tzinfo=UTC),
            cutoff,
        )
        self.assertEqual(list(normalized.index), [pd.Timestamp("2026-07-10T11:55:00Z")])

    def test_direct_request_rejects_noncanonical_cells(self):
        with self.assertRaisesRegex(ValueError, "canonical production"):
            OverlayRequest(
                symbols=("eurusd",),
                timeframes=("M5",),
                cutoff_utc=datetime(2026, 7, 14, tzinfo=UTC),
            )

    def test_only_one_cell_payload_remains_resident_between_fetches(self):
        with tempfile.TemporaryDirectory() as temp:
            native = self._root(temp)
            cells = [(symbol, "M5") for symbol in ("EURUSD", "AUDUSD", "USDJPY")]
            _write_origin(native, cells)
            connector = _Connector(resident_check=True)
            manifest = self._run(
                native,
                cells,
                connector,
                datetime(2026, 7, 10, 13, 0, tzinfo=UTC),
            )
            self.assertEqual(manifest["counts"]["success"], 3)
            self.assertEqual(len(connector.calls), 3)

    def test_source_identity_mismatch_is_typed_without_connector_call(self):
        with tempfile.TemporaryDirectory() as temp:
            native = self._root(temp)
            _write_origin(native, [("EURUSD", "M5")])
            connector = _Connector()
            runtime = json.loads(json.dumps(SOURCE_ENVIRONMENT))
            runtime["account"]["login"] = 999
            request = self._request(
                ["EURUSD"],
                ["M5"],
                datetime(2026, 7, 10, 13, 0, tzinfo=UTC),
            )
            manifest = NativeOverlayAcquirer(
                connector,
                OverlayStore(native, "identity"),
                request,
                runtime_metadata=runtime,
                require_production_baseline=False,
            ).acquire()
            self.assertEqual(
                manifest["results"][0]["failure_status"], "source_identity_mismatch"
            )
            self.assertEqual(connector.calls, [])

    def test_execute_without_runtime_flag_always_uses_finite_supervisor_default(self):
        with patch(
            "tools.acquire_native_overlay._run_supervised_worker", return_value=7
        ) as supervised:
            result = main(["--execute"])
        self.assertEqual(result, 7)
        self.assertEqual(
            supervised.call_args.args,
            (["--execute"], DEFAULT_EXECUTION_RUNTIME_SECONDS),
        )

    def test_supervisor_runs_child_and_monitors_at_released_poll_interval(self):
        process = SimpleNamespace(wait=Mock(return_value=7))
        with (
            patch(
                "tools.acquire_native_overlay._supervisor_terminal_cache_snapshot",
                return_value={"status": "within_bounds", "violations": []},
            ),
            patch(
                "tools.acquire_native_overlay.subprocess.Popen", return_value=process
            ) as popen,
        ):
            result = _run_supervised_worker(["--execute"], 12.5)
        self.assertEqual(result, 7)
        command = popen.call_args.args[0]
        self.assertEqual(
            command[:2],
            [
                sys.executable,
                str(PROJECT_ROOT / "tools" / "acquire_native_overlay.py"),
            ],
        )
        self.assertIn("--_supervised-worker", command)
        self.assertEqual(process.wait.call_args.kwargs["timeout"], 0.5)

    def test_supervisor_timeout_is_typed_and_cannot_publish_partial_cell(self):
        process = SimpleNamespace(
            wait=Mock(side_effect=[subprocess.TimeoutExpired("worker", 0.01), 0]),
            poll=Mock(return_value=None),
            terminate=Mock(),
            kill=Mock(),
        )
        stderr = StringIO()
        with (
            patch(
                "tools.acquire_native_overlay._supervisor_terminal_cache_snapshot",
                return_value={"status": "within_bounds", "violations": []},
            ),
            patch(
                "tools.acquire_native_overlay.subprocess.Popen", return_value=process
            ),
            patch(
                "tools.acquire_native_overlay.time.monotonic",
                side_effect=[0.0, 0.0, 0.02],
            ),
            redirect_stderr(stderr),
        ):
            result = _run_supervised_worker(["--execute"], 0.01)
        self.assertEqual(result, HARD_TIMEOUT_EXIT_CODE)
        process.terminate.assert_called_once_with()
        event = json.loads(stderr.getvalue())
        self.assertEqual(event["status"], "hard_runtime_timeout")
        self.assertFalse(event["published_partial_cell"])
        self.assertTrue(event["resume_required"])

    def test_supervisor_stops_blocked_child_on_hcc_or_disk_resource_growth(self):
        process = SimpleNamespace(
            wait=Mock(side_effect=[subprocess.TimeoutExpired("worker", 0.5), 0]),
            poll=Mock(return_value=None),
            terminate=Mock(),
            kill=Mock(),
        )
        snapshots = [
            {"status": "within_bounds", "violations": []},
            {
                "status": "limit_exceeded",
                "violations": ["terminal_hcc_file_bound_breached"],
                "largest_hcc_bytes": 2 * 1024**3 + 1,
            },
        ]
        stderr = StringIO()
        with (
            patch(
                "tools.acquire_native_overlay._supervisor_terminal_cache_snapshot",
                side_effect=snapshots,
            ),
            patch(
                "tools.acquire_native_overlay.subprocess.Popen", return_value=process
            ),
            redirect_stderr(stderr),
        ):
            result = _run_supervised_worker(["--execute"], 30.0)
        self.assertEqual(result, RESOURCE_LIMIT_EXIT_CODE)
        process.terminate.assert_called_once_with()
        event = json.loads(stderr.getvalue())
        self.assertEqual(event["status"], "terminal_cache_resource_limit")
        self.assertFalse(event["published_partial_cell"])
        self.assertEqual(
            event["snapshot"]["violations"],
            ["terminal_hcc_file_bound_breached"],
        )

    def test_supervisor_refuses_preexisting_cache_limit_before_child_start(self):
        stderr = StringIO()
        with (
            patch(
                "tools.acquire_native_overlay._supervisor_terminal_cache_snapshot",
                return_value={
                    "status": "limit_exceeded",
                    "violations": ["terminal_disk_free_reserve_breached"],
                },
            ),
            patch("tools.acquire_native_overlay.subprocess.Popen") as popen,
            redirect_stderr(stderr),
        ):
            result = _run_supervised_worker(["--execute"], 30.0)
        self.assertEqual(result, RESOURCE_LIMIT_EXIT_CODE)
        popen.assert_not_called()
        self.assertFalse(json.loads(stderr.getvalue())["published_partial_cell"])

    def test_runtime_override_must_remain_inside_finite_hard_cap(self):
        for value in ("nan", "0", str(MAX_EXECUTION_RUNTIME_SECONDS + 1.0)):
            with (
                self.subTest(value=value),
                self.assertRaisesRegex(ValueError, "finite, positive"),
            ):
                main(["--execute", "--max-runtime-seconds", value])

    def test_cli_plan_defaults_to_production_even_if_config_has_subset(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            (root / "data/native").mkdir(parents=True)
            config = root / "config.json"
            config.write_text(json.dumps({"symbols": ["EURUSD"]}), encoding="utf-8")
            stdout = StringIO()
            with redirect_stdout(stdout):
                result = main(
                    [
                        "--config",
                        str(config),
                        "--output-root",
                        str(root / "data/native"),
                        "--cutoff",
                        "2026-07-14T00:00:00Z",
                    ]
                )
            self.assertEqual(result, 0)
            plan = json.loads(stdout.getvalue())
            self.assertEqual(plan["cells"], 600)
            self.assertEqual(plan["symbols"], list(PRODUCTION_SYMBOL_ORDER))


if __name__ == "__main__":
    unittest.main()
