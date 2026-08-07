import hashlib
import json
import io
import os
import shutil
import subprocess
import sys
import tempfile
import unittest
from copy import deepcopy
from contextlib import redirect_stdout
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

import numpy as np
import pandas as pd


PATCH_ROOT = Path(__file__).resolve().parents[1]
if str(PATCH_ROOT) not in sys.path:
    sys.path.insert(0, str(PATCH_ROOT))

import tools.acquire_native_history as native_acquisition  # noqa: E402
import pm_native_request_contract as request_contract  # noqa: E402

from pm_native_consensus import CONSENSUS_POLICY, REFERENCE_GRAPH  # noqa: E402
from pm_native_intervals import native_source_interval  # noqa: E402
from pm_native_request_contract import (  # noqa: E402
    PRODUCTION_SYMBOL_ORDER,
    _fixture_mt5_v4_empty_diagnostics,
    _fixture_mt5_v4_page_diagnostics,
    build_valid_v5_contract_fixture,
    canonical_source_identity,
    validate_v5_terminal_technical_cell,
    validate_v5_source_quality_omission,
)
from pm_native_csv_validation import NativeCsvValidationError  # noqa: E402
from tools.acquire_native_history import (  # noqa: E402
    ACQUISITION_CONTRACT_VERSION,
    DEFAULT_TIMEFRAMES,
    FORENSIC_TIMEFRAME_REQUEST_MAP_SCHEMA_VERSION,
    LEGACY_FLAT_WINDOW_CONTRACT_VERSION,
    SEGMENTED_SESSION_COVERAGE_METHOD_V2,
    TIMEFRAME_EVIDENCE_CONTRACT_VERSION,
    TIMEFRAME_REQUEST_MAP_SCHEMA_VERSION,
    TWO_REFERENCE_CONSENSUS_POLICY,
    ConcurrentWriterError,
    HARD_TIMEOUT_EXIT_CODE,
    RESOURCE_LIMIT_EXIT_CODE,
    NativeHistoryAcquirer,
    NativeHistoryRunLock,
    NativeHistoryTerminalLock,
    NativeHistoryRequest,
    OutputStorageResourceLimitError,
    OUTPUT_MIN_FREE_BYTES,
    ResumeValidationError,
    NativeHistoryStore,
    TIMEFRAME_SECONDS,
    TerminalCacheResourceLimitError,
    build_common_partition_windows,
    cross_resolution_reference_map,
    expected_session_coverage_by_partition,
    load_native_history_resume_contract,
    main,
    segmented_session_coverage_by_partition_v2,
    snapshot_read_only_runtime_metadata,
    _canonical_json_sha256,
    _timeframe_request_hash_payload,
)


UTC = timezone.utc


def _iso(value):
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _bar_frame(timestamps):
    index = pd.DatetimeIndex(list(timestamps))
    return pd.DataFrame(
        {
            "Open": [1.0] * len(index),
            "High": [1.2] * len(index),
            "Low": [0.8] * len(index),
            "Close": [1.1] * len(index),
            "Volume": [100] * len(index),
            "Spread": [2] * len(index),
            "RealVolume": [0] * len(index),
        },
        index=index,
    )


def _retried_page_diagnostics(
    *, symbol, timeframe, start, end, first_open, last_open, rows
):
    """Simulate released V4 empty -> warm -> exact data retry."""

    empty = _fixture_mt5_v4_empty_diagnostics(
        symbol=symbol,
        timeframe=timeframe,
        start=start,
        end=end,
    )
    data = _fixture_mt5_v4_page_diagnostics(
        symbol=symbol,
        timeframe=timeframe,
        start=start,
        end=end,
        first_open=first_open,
        last_open=last_open,
        rows=rows,
    )
    retry = dict(data["call_ledger"][0])
    retry.update(sequence=3, call_type="copy_rates_range_retry")
    empty["call_ledger"][2] = retry
    for field in (
        "sync_status",
        "raw_relation",
        "raw_count",
        "raw_extent",
        "admitted_count",
        "admitted_extent",
        "boundary_complete",
        "best_extent",
        "last_error",
    ):
        empty[field] = data[field]
    empty["confirmation_raw_relation"] = data["raw_relation"]
    return empty


def _hard_failure_diagnostics(*, symbol, timeframe, start, end):
    receipt = _fixture_mt5_v4_empty_diagnostics(
        symbol=symbol,
        timeframe=timeframe,
        start=start,
        end=end,
    )
    last_error = {
        "code": -10005,
        "message": "IPC timeout",
        "capture_error": None,
    }
    initial = receipt["call_ledger"][0]
    initial.update(last_error=last_error, hard_failure=True)
    receipt.update(
        sync_status="hard_mt5_failure",
        copy_rates_range_attempts=1,
        history_download_trigger_attempts=0,
        history_download_trigger_max_bars=0,
        call_ledger=[initial],
        hard_failure=True,
        hard_failure_call="copy_rates_range_initial",
        last_error=last_error,
    )
    return receipt


def _authenticated_boundary_diagnostics(
    *, symbol, timeframe, start, end, boundary_open
):
    """Released V4 receipt for a stable one-row wholly-newer boundary sentinel."""

    receipt = _fixture_mt5_v4_empty_diagnostics(
        symbol=symbol,
        timeframe=timeframe,
        start=start,
        end=end,
    )
    epoch = int(boundary_open.timestamp())
    extent = {
        "rows": 1,
        "first_epoch_second": epoch,
        "last_epoch_second": epoch,
    }
    for entry in receipt["call_ledger"]:
        entry.update(
            raw_relation="RIGHT_DISJOINT_PREFIX",
            raw_count=1,
            raw_extent=dict(extent),
            admitted_count=0,
            admitted_extent=None,
        )
    receipt.update(
        sync_status="terminal_prefix_unsynchronized",
        initial_raw_relation="RIGHT_DISJOINT_PREFIX",
        confirmation_raw_relation="RIGHT_DISJOINT_PREFIX",
        raw_relation="RIGHT_DISJOINT_PREFIX",
        raw_count=1,
        raw_extent=dict(extent),
        history_download_trigger_nonempty=1,
        terminal_confirmation_consumed=True,
    )
    return receipt


def _segmented_coverage_windows():
    return {
        "optimizer_train": (
            datetime(2020, 1, 6, tzinfo=UTC),
            datetime(2022, 1, 3, tzinfo=UTC),
        ),
        "tuning_validation": (
            datetime(2022, 1, 3, tzinfo=UTC),
            datetime(2024, 1, 1, tzinfo=UTC),
        ),
        "final_lockbox": (
            datetime(2024, 1, 1, tzinfo=UTC),
            datetime(2026, 1, 5, tzinfo=UTC),
        ),
    }


def _scheduled_hourly_index(kind):
    index = pd.date_range(
        "2020-01-06",
        "2026-01-05",
        freq="h",
        inclusive="left",
        tz="UTC",
    )
    if kind == "fx":
        return index[index.weekday < 5]
    if kind == "equity":
        return index[(index.weekday < 5) & (index.hour >= 14) & (index.hour < 20)]
    if kind == "24x7":
        return index
    if kind == "d1":
        return index[(index.weekday < 5) & (index.hour == 21)]
    raise AssertionError(f"unsupported test schedule {kind!r}")


def _minimal_provenance(run_id, symbol="EURUSD", timeframe="M5"):
    return {
        "schema_version": "fxpm-native-bars-v1",
        "acquisition_contract_version": ACQUISITION_CONTRACT_VERSION,
        "immutable_record": True,
        "run_id": run_id,
        "status": "success",
        "failure_status": None,
        "canonical_symbol": symbol,
        "symbol": symbol,
        "broker_symbol": f"{symbol}.a",
        "timeframe": timeframe,
        "source_kind": "broker_native",
        "timezone": "UTC",
    }


def _latest_attempt_record(run_root, symbol, timeframe):
    attempts = sorted(
        (run_root / "_attempts" / symbol / timeframe).glob("attempt-*.json")
    )
    assert attempts
    envelope = json.loads(attempts[-1].read_text(encoding="utf-8"))
    assert envelope["nonfinal"] is True
    assert envelope["retryable"] is True
    assert envelope["retention_status"] == "full"
    return envelope["record"]


def _terminal_writer_attempt_fixture(run_id, *, attempt_number=1):
    cutoff = datetime(2026, 7, 13, 9, 3, 17, tzinfo=UTC)
    fixture = build_valid_v5_contract_fixture(requested_information_cutoff_utc=cutoff)
    source = fixture["provenance"]
    pagination = deepcopy(source["pagination"])
    internal = deepcopy(
        pagination["page_attempts"][len(pagination["page_attempts"]) // 2]
    )
    internal["copy_rates_range_attempts"] = 2
    internal["connector_diagnostics"] = _fixture_mt5_v4_empty_diagnostics(
        symbol="EURUSD",
        timeframe="M5",
        start=datetime.fromisoformat(internal["start_utc"].replace("Z", "+00:00")),
        end=datetime.fromisoformat(
            internal["end_exclusive_utc"].replace("Z", "+00:00")
        ),
    )
    pagination["empty_pages"] = [internal]
    pagination["pages_with_data"] -= 1
    for page in pagination["page_attempts"]:
        if page["request_sequence"] == internal["request_sequence"]:
            page.update(deepcopy(internal))
            break
    pagination["copy_rates_range_attempts_total"] += 1
    attempt_fields = (
        request_contract._FAILED_PROVENANCE_BASE_FIELDS
        | request_contract._FAILED_PROVENANCE_FETCH_FIELDS
    )
    attempt = {key: deepcopy(source[key]) for key in attempt_fields if key in source}
    manifest_started = datetime.fromisoformat(
        fixture["manifest"]["acquisition_started_utc"].replace("Z", "+00:00")
    )
    attempt_started = manifest_started + timedelta(seconds=2 * (attempt_number - 1))
    attempt_completed = attempt_started + timedelta(seconds=1)
    attempt.update(
        {
            "run_id": run_id,
            "status": "partial",
            "failure_status": "technical_history_gap",
            "eligible_for_optimization": False,
            "eligible_for_downstream_validation": False,
            "statistical_sufficiency_assessed": False,
            "artifact": None,
            "sha256": None,
            "rows": 0,
            "start_ts": None,
            "end_ts": None,
            "complete": False,
            "instrument": {},
            "pagination": pagination,
            "acquisition_started_utc": attempt_started.isoformat().replace(
                "+00:00", "Z"
            ),
            "acquisition_completed_utc": attempt_completed.isoformat().replace(
                "+00:00", "Z"
            ),
            "error": {
                "type": "TechnicalHistoryGap",
                "message": f"independent technical gap attempt {attempt_number}",
            },
        }
    )
    attempt["request"]["last_fully_closed_bar_open_utc"] = None
    attempt["output_storage_resource_snapshot"].update(
        {"estimated_cell_bytes": 0, "sample_rows": 0}
    )
    return fixture, attempt


def _prepare_terminal_writer_receipts(store, *, duplicate=False):
    fixture, first = _terminal_writer_attempt_fixture(store.run_id, attempt_number=1)
    started = datetime.fromisoformat(
        fixture["manifest"]["acquisition_started_utc"].replace("Z", "+00:00")
    )
    store.ensure_resume_contract(
        fixture["common_request"],
        fixture["source_environment"],
        started,
        symbols=("EURUSD",),
        timeframes=("M5",),
        production_authoritative=False,
    )
    store.commit_retryable_attempt("EURUSD", "M5", first)
    _fixture, second = _terminal_writer_attempt_fixture(
        store.run_id,
        attempt_number=(1 if duplicate else 2),
    )
    store.commit_retryable_attempt(
        "EURUSD",
        "M5",
        first if duplicate else second,
    )
    fixture["manifest"]["acquisition_completed_utc"] = (
        (started + timedelta(seconds=4)).isoformat().replace("+00:00", "Z")
    )
    return fixture


class _RangeConnector:
    def __init__(self, *, cutoff=None, failing_timeframes=()):
        self.cutoff = cutoff
        self.failing_timeframes = set(failing_timeframes)
        self.calls = []
        self._last_error = (1, "Success")
        self.diagnostics = {}

    def find_broker_symbol(self, symbol):
        return f"{symbol}.a"

    def get_runtime_metadata(self):
        return {
            "metadata_status": "available",
            "terminal": {
                "name": "MetaTrader 5",
                "company": "MetaQuotes",
                "path": str(Path(tempfile.gettempdir()) / "terminal64.exe"),
                "build": 5432,
                "max_bars": 10000,
                "version": [5, 5432, "10 Jul 2026"],
                "data_path": tempfile.gettempdir(),
                "commondata_path": tempfile.gettempdir(),
            },
            "account": {
                "login": 123456,
                "server": "MockBroker-ReadOnly",
                "company": "Mock Broker",
                "currency": "USD",
            },
            "configured_connection": {
                "server": "MockBroker-ReadOnly",
                "terminal_path": str(Path(tempfile.gettempdir()) / "terminal64.exe"),
                "expected_login": 123456,
                "expected_server": "MockBroker-ReadOnly",
                "expected_currency": "USD",
            },
        }

    def get_last_error(self):
        return self._last_error

    def get_last_history_range_diagnostics(self):
        return dict(self.diagnostics)

    def get_bars_range(self, symbol, timeframe, start_date, end_date, sync_retry=False):
        self.calls.append((symbol, timeframe, start_date, end_date, sync_retry))
        if timeframe in self.failing_timeframes:
            self._last_error = (-10005, "IPC timeout")
            raise TimeoutError(f"mock {timeframe} timeout")
        self._last_error = (1, "Success")
        if self.cutoff is not None and end_date <= self.cutoff:
            self.diagnostics = _fixture_mt5_v4_empty_diagnostics(
                symbol=symbol,
                timeframe=timeframe,
                start=start_date,
                end=end_date,
            )
            return None
        seconds = TIMEFRAME_SECONDS[timeframe]
        first = start_date
        effective_end = end_date
        if timeframe in {"H4", "D1"}:
            # Production fetches one second beyond the common cutoff so an
            # exact-cutoff successor can prove the prior bar closed. Keep this
            # generic fake's returned bars fully inside that sentinel window.
            first = start_date + timedelta(seconds=seconds)
            effective_end = end_date - timedelta(seconds=1)
        if self.cutoff is not None:
            first = max(first, self.cutoff)
        last = effective_end - timedelta(seconds=seconds)
        if last < first:
            self.diagnostics = _fixture_mt5_v4_empty_diagnostics(
                symbol=symbol,
                timeframe=timeframe,
                start=start_date,
                end=end_date,
            )
            return None
        points = [first]
        if last != first:
            points.append(last)
        frame = _bar_frame(points)
        data_args = {
            "symbol": symbol,
            "timeframe": timeframe,
            "start": start_date,
            "end": end_date,
            "first_open": first,
            "last_open": last,
            "rows": len(frame),
        }
        initial = _fixture_mt5_v4_page_diagnostics(**data_args)
        self.diagnostics = (
            initial
            if initial["boundary_complete"]
            else _retried_page_diagnostics(**data_args)
        )
        frame.attrs["mt5_history_range_diagnostics"] = dict(self.diagnostics)
        return frame


class _DriftingRangeConnector(_RangeConnector):
    def __init__(self):
        super().__init__()
        self.identity_drifted = False

    def get_runtime_metadata(self):
        metadata = super().get_runtime_metadata()
        if self.identity_drifted:
            metadata["account"]["login"] = 654321
            metadata["account"]["server"] = "MockBroker-Demo"
            metadata["configured_connection"]["expected_login"] = 654321
            metadata["configured_connection"]["expected_server"] = "MockBroker-Demo"
            metadata["configured_connection"]["server"] = "MockBroker-Demo"
        return metadata

    def get_bars_range(self, *args, **kwargs):
        frame = super().get_bars_range(*args, **kwargs)
        self.identity_drifted = True
        return frame


class NativeHistoryAcquisitionTests(unittest.TestCase):
    @staticmethod
    def _fbs_real_runtime_metadata():
        return {
            "metadata_status": "available",
            "account": {
                "login": 28829971,
                "server": "FBS-Real",
                "company": "FBS Markets Inc.",
                "currency": "USD",
            },
            "configured_connection": {
                "server": "FBS-Real",
                "terminal_path": r"C:\FBS\terminal64.exe",
                "expected_login": 28829971,
                "expected_server": "FBS-Real",
                "expected_currency": "USD",
            },
            "terminal": {
                "name": "MetaTrader 5",
                "company": "MetaQuotes Ltd.",
                "path": r"C:\FBS",
                "data_path": r"C:\FBS\Data",
                "commondata_path": r"C:\ProgramData\MetaQuotes",
                "build": 5833,
                "version": [500, 5833, "25 Apr 2026"],
                "max_bars": 100_000_000,
            },
        }

    def test_fr40_source_quality_policy_omits_only_exact_confirmed_fbs_row(self):
        target = pd.Timestamp("2019-11-25T00:00:00Z")
        frame = _bar_frame(
            [
                target - pd.Timedelta(minutes=5),
                target,
                target + pd.Timedelta(minutes=5),
            ]
        )
        frame.loc[target, ["Open", "High", "Low", "Close"]] = [
            5917.25,
            5935.3,
            0.0,
            5929.2,
        ]
        frame.loc[target, ["Volume", "Spread", "RealVolume"]] = [5778, 0, 0]
        source = self._fbs_real_runtime_metadata()
        acquirer = object.__new__(NativeHistoryAcquirer)
        acquirer.runtime_metadata = source

        for timeframe in DEFAULT_TIMEFRAMES:
            with self.subTest(timeframe=timeframe):
                filtered, receipts = acquirer._apply_source_quality_omission_policy(
                    frame,
                    symbol="FR40",
                    broker_symbol="FR40",
                    timeframe=timeframe,
                )

                self.assertEqual(len(receipts), 1)
                self.assertNotIn(target, filtered.index)
                self.assertEqual(len(filtered), 2)
                validate_v5_source_quality_omission(
                    receipts[0],
                    source_environment=source,
                    symbol="FR40",
                    broker_symbol="FR40",
                    timeframe=timeframe,
                )

        changed = frame.copy()
        changed.loc[target, "Low"] = 1.0
        retained, changed_receipts = acquirer._apply_source_quality_omission_policy(
            changed,
            symbol="FR40",
            broker_symbol="FR40",
            timeframe="M5",
        )
        self.assertIn(target, retained.index)
        self.assertEqual(changed_receipts, [])

        wrong_source = json.loads(json.dumps(source))
        wrong_source["account"].update(login=1, server="Other-Real")
        wrong_source["configured_connection"].update(
            server="Other-Real",
            expected_login=1,
            expected_server="Other-Real",
        )
        acquirer.runtime_metadata = wrong_source
        retained, wrong_source_receipts = (
            acquirer._apply_source_quality_omission_policy(
                frame,
                symbol="FR40",
                broker_symbol="FR40",
                timeframe="M5",
            )
        )
        self.assertIn(target, retained.index)
        self.assertEqual(wrong_source_receipts, [])

    def test_fr40_source_quality_policy_rejects_duplicate_target_rows(self):
        target = pd.Timestamp("2019-11-25T00:00:00Z")
        frame = pd.DataFrame(
            {
                "Open": [5917.25, 5917.25],
                "High": [5935.3, 5935.3],
                "Low": [0.0, 0.0],
                "Close": [5929.2, 5929.2],
                "Volume": [5778, 5778],
                "Spread": [0, 0],
                "RealVolume": [0, 0],
            },
            index=pd.DatetimeIndex([target, target]),
        )
        acquirer = object.__new__(NativeHistoryAcquirer)
        acquirer.runtime_metadata = self._fbs_real_runtime_metadata()
        with self.assertRaisesRegex(
            NativeCsvValidationError,
            "SOURCE_QUALITY_OMISSION_MULTIPLICITY_INVALID",
        ):
            acquirer._apply_source_quality_omission_policy(
                frame,
                symbol="FR40",
                broker_symbol="FR40",
                timeframe="M5",
            )

    def test_timeframe_v5_uses_consensus_module_as_single_authority(self):
        self.assertEqual(TWO_REFERENCE_CONSENSUS_POLICY, CONSENSUS_POLICY)
        self.assertEqual(
            cross_resolution_reference_map(DEFAULT_TIMEFRAMES),
            {
                timeframe: list(REFERENCE_GRAPH[timeframe])
                for timeframe in DEFAULT_TIMEFRAMES
            },
        )

    def test_v5_rejects_connector_trigger_caps_below_receipt_contract(self):
        connector = _RangeConnector()
        connector.config = SimpleNamespace(history_page_size=10_000)
        with tempfile.TemporaryDirectory() as tmp:
            _, native_root = self._paths(tmp)
            store = NativeHistoryStore(native_root, "trigger-cap-mismatch")
            try:
                with self.assertRaisesRegex(ValueError, "50,000-bar"):
                    NativeHistoryAcquirer(
                        connector,
                        store,
                        NativeHistoryRequest(
                            symbols=("EURUSD",),
                            timeframes=("M5",),
                            horizon_years=1,
                            as_of_utc=datetime(2026, 1, 10, tzinfo=UTC),
                            retries=1,
                            enforce_evidence_horizon=False,
                        ),
                    )
                self.assertEqual(connector.calls, [])
            finally:
                store.close()

    def test_v5_writer_and_resume_preserve_case_sensitive_symbol_identity(self):
        as_of = datetime(2026, 1, 5, tzinfo=UTC)
        request = NativeHistoryRequest(
            symbols=("Platinum",),
            timeframes=DEFAULT_TIMEFRAMES,
            as_of_utc=as_of,
            retries=1,
            retry_sleep_seconds=0.0,
            timeframe_window_years={},
            window_contract_version=TIMEFRAME_EVIDENCE_CONTRACT_VERSION,
            warmup_days=0.0,
            safety_days=0.0,
        )
        self.assertEqual(request.symbols, ("Platinum",))

        with tempfile.TemporaryDirectory() as tmp:
            _, native_root = self._paths(tmp)
            store = NativeHistoryStore(native_root, "mixed-case-v5")
            result = NativeHistoryAcquirer(
                _RangeConnector(),
                store,
                request,
                now_fn=lambda: as_of,
                sleep_fn=lambda _: None,
            ).acquire(max_new_cells=1)
            self.assertFalse(result["complete"])
            self.assertTrue(
                (native_root / "runs" / "mixed-case-v5" / "Platinum" / "M5").is_dir()
            )
            self.assertEqual(
                [
                    path.name
                    for path in (native_root / "runs" / "mixed-case-v5").iterdir()
                    if path.is_dir()
                ],
                ["Platinum"],
            )
            provenance_path = (
                native_root
                / "runs"
                / "mixed-case-v5"
                / "Platinum"
                / "M5"
                / "provenance.json"
            )
            provenance = json.loads(provenance_path.read_text(encoding="utf-8"))
            self.assertEqual(provenance["canonical_symbol"], "Platinum")
            recovered = load_native_history_resume_contract(
                native_root,
                "mixed-case-v5",
            )
            self.assertEqual(recovered.existing_cells, (("Platinum", "M5"),))

            provenance["canonical_symbol"] = "PLATINUM"
            provenance_path.write_text(
                json.dumps(provenance, indent=2, sort_keys=True),
                encoding="utf-8",
            )
            with self.assertRaisesRegex(ResumeValidationError, "symbol mismatch"):
                load_native_history_resume_contract(native_root, "mixed-case-v5")

    def test_v5_rejects_casefold_path_alias_but_legacy_remains_uppercase(self):
        with self.assertRaisesRegex(ValueError, "case-insensitive filesystem"):
            NativeHistoryRequest(
                symbols=("Platinum", "PLATINUM"),
                timeframes=DEFAULT_TIMEFRAMES,
                timeframe_window_years={},
                window_contract_version=TIMEFRAME_EVIDENCE_CONTRACT_VERSION,
            )
        legacy = NativeHistoryRequest(
            symbols=("Platinum",),
            timeframes=("M5",),
            window_contract_version=LEGACY_FLAT_WINDOW_CONTRACT_VERSION,
        )
        self.assertEqual(legacy.symbols, ("PLATINUM",))

    def test_config_driven_v5_plan_requires_exact_production_roster(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            config_path = root / "config.json"
            config = {
                "symbols": list(PRODUCTION_SYMBOL_ORDER),
                "pipeline": {
                    "native_history_window_contract": (
                        TIMEFRAME_EVIDENCE_CONTRACT_VERSION
                    )
                },
            }
            config_path.write_text(json.dumps(config), encoding="utf-8")
            output = io.StringIO()
            with redirect_stdout(output):
                code = main(
                    [
                        "--config",
                        str(config_path),
                        "--output-root",
                        str(root / "data" / "native"),
                        "--as-of",
                        "2026-01-05T00:00:00Z",
                    ]
                )
            self.assertEqual(code, 0)
            plan = json.loads(output.getvalue())
            self.assertTrue(plan["production_authoritative"])
            self.assertEqual(plan["symbols"], list(PRODUCTION_SYMBOL_ORDER))

            config["symbols"][PRODUCTION_SYMBOL_ORDER.index("Platinum")] = "PLATINUM"
            config_path.write_text(json.dumps(config), encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "exact ordered 100-symbol"):
                main(
                    [
                        "--config",
                        str(config_path),
                        "--output-root",
                        str(root / "bad" / "data" / "native"),
                        "--as-of",
                        "2026-01-05T00:00:00Z",
                    ]
                )

    def _paths(self, temp_dir):
        root = Path(temp_dir)
        native_root = root / "data" / "native"
        return root, native_root

    @staticmethod
    def _finalize_spill(assembly, timeframe, start, end, pages):
        span = end - start
        one_third = span / 3
        windows = {
            "optimizer_train": (start, start + one_third),
            "tuning_validation": (start + one_third, start + 2 * one_third),
            "final_lockbox": (start + 2 * one_third, end),
        }
        attempts = [
            {
                "start_utc": page_start.isoformat().replace("+00:00", "Z"),
                "end_exclusive_utc": page_end.isoformat().replace("+00:00", "Z"),
                "connector_diagnostics": {"admitted_count": rows},
            }
            for page_start, page_end, rows in pages
        ]
        return assembly.finalize(
            timeframe=timeframe,
            timeframe_seconds=TIMEFRAME_SECONDS[timeframe],
            broker_session_aligned=timeframe in {"H4", "D1"},
            native_start=start,
            fetch_end_exclusive=end,
            as_of_utc=end,
            partition_windows=windows,
            common_request={
                "common_as_of_utc": end.isoformat().replace("+00:00", "Z"),
                "timeframe_source_request_intervals": {
                    timeframe: {
                        "start_utc": start.isoformat().replace("+00:00", "Z"),
                        "end_exclusive_utc": end.isoformat().replace("+00:00", "Z"),
                    }
                },
            },
            pagination={"page_attempts": attempts},
        )

    def _single_page_spill_artifact(self, store, timeframe="M5"):
        seconds = TIMEFRAME_SECONDS[timeframe]
        start = datetime(2026, 1, 1, tzinfo=UTC)
        end = start + 2 * timedelta(seconds=seconds)
        frame = _bar_frame([start, start + timedelta(seconds=seconds)])
        assembly = native_acquisition._NativePageSpillAssembly(
            store.run_root, "EURUSD", timeframe
        )
        assembly.add_page(frame, start, end)
        return self._finalize_spill(
            assembly, timeframe, start, end, [(start, end, len(frame))]
        )

    def test_utc_common_horizon_and_last_fully_closed_bar(self):
        as_of = datetime(2026, 1, 10, 12, 2, 17, tzinfo=UTC)
        connector = _RangeConnector()
        with tempfile.TemporaryDirectory() as tmp:
            _, native_root = self._paths(tmp)
            store = NativeHistoryStore(native_root, "utc-range-test")
            request = NativeHistoryRequest(
                symbols=("EURUSD",),
                timeframes=("M5",),
                horizon_years=1,
                as_of_utc=as_of,
                retries=1,
                retry_sleep_seconds=0.0,
                enforce_evidence_horizon=False,
            )
            manifest = NativeHistoryAcquirer(
                connector,
                store,
                request,
                now_fn=lambda: as_of,
                sleep_fn=lambda _: None,
            ).acquire()

            self.assertEqual(
                manifest["counts"],
                {"success": 1, "partial": 0, "failed": 0, "total": 1},
            )
            self.assertGreater(
                len(connector.calls), 1, "M5 should be calendar-window paginated"
            )
            for _, timeframe, start, end, sync_retry in connector.calls:
                self.assertEqual(timeframe, "M5")
                self.assertIs(start.tzinfo, UTC)
                self.assertIs(end.tzinfo, UTC)
                self.assertLess(start, end)
                self.assertTrue(sync_retry)

            sidecar_path = store.run_root / "EURUSD" / "M5" / "provenance.json"
            sidecar = json.loads(sidecar_path.read_text(encoding="utf-8"))
            self.assertEqual(
                sidecar["request"]["requested_information_cutoff_utc"],
                "2026-01-10T12:02:17Z",
            )
            self.assertEqual(
                sidecar["request"]["common_as_of_utc"], "2026-01-10T12:00:00Z"
            )
            self.assertEqual(
                sidecar["request"]["native_fetch_start_utc"], "2025-01-10T12:00:00Z"
            )
            self.assertEqual(
                sidecar["request"]["native_fetch_end_exclusive_utc"],
                "2026-01-10T12:00:00Z",
            )
            self.assertEqual(
                sidecar["request"]["last_fully_closed_bar_open_utc"],
                "2026-01-10T11:55:00Z",
            )
            self.assertEqual(
                sidecar["source_environment"]["terminal"]["max_bars"], 10000
            )
            self.assertEqual(
                sidecar["source_environment"]["account"]["server"],
                "MockBroker-ReadOnly",
            )
            self.assertEqual(len(sidecar["artifact"]["sha256"]), 64)
            self.assertEqual(sidecar["schema_version"], "fxpm-native-bars-v1")
            self.assertEqual(sidecar["timestamp_semantics"], "bar_open_utc")
            self.assertEqual(
                set(sidecar["expected_session_coverage_by_partition"]),
                {"optimizer_train", "tuning_validation", "final_lockbox"},
            )
            self.assertEqual(
                set(sidecar["observed_session_bars_by_partition"]),
                {"optimizer_train", "tuning_validation", "final_lockbox"},
            )
            self.assertEqual(
                set(sidecar["expected_session_bars_by_partition"]),
                {"optimizer_train", "tuning_validation", "final_lockbox"},
            )
            self.assertFalse(sidecar["eligible_for_optimization"])
            self.assertTrue(sidecar["eligible_for_downstream_validation"])
            self.assertFalse(sidecar["statistical_sufficiency_assessed"])

            bars = pd.read_csv(store.run_root / "EURUSD" / "M5" / "bars.csv")
            latest = pd.to_datetime(bars["time"], utc=True).max()
            self.assertEqual(latest, pd.Timestamp("2026-01-10T11:55:00Z"))

    def test_empty_prefix_is_technical_not_broker_absence(self):
        as_of = datetime(2026, 1, 10, 12, 2, tzinfo=UTC)
        cutoff = datetime(2025, 12, 1, 0, 0, tzinfo=UTC)
        connector = _RangeConnector(cutoff=cutoff)
        with tempfile.TemporaryDirectory() as tmp:
            _, native_root = self._paths(tmp)
            store = NativeHistoryStore(native_root, "partial-history-test")
            request = NativeHistoryRequest(
                symbols=("NEWCFD",),
                timeframes=("M5",),
                horizon_years=2,
                as_of_utc=as_of,
                retries=1,
                retry_sleep_seconds=0.0,
                enforce_evidence_horizon=False,
            )
            manifest = NativeHistoryAcquirer(
                connector,
                store,
                request,
                now_fn=lambda: as_of,
            ).acquire()

            self.assertEqual(manifest["counts"]["partial"], 1)
            self.assertFalse(manifest["complete"])
            self.assertFalse((store.run_root / "NEWCFD" / "M5").exists())
            sidecar = _latest_attempt_record(store.run_root, "NEWCFD", "M5")
            self.assertEqual(sidecar["status"], "partial")
            self.assertEqual(
                sidecar["failure_status"],
                "technical_history_gap",
            )
            self.assertFalse(sidecar["eligible_for_optimization"])
            self.assertEqual(sidecar["error"]["type"], "TechnicalHistoryGap")
            self.assertGreater(len(sidecar["pagination"]["empty_pages"]), 0)
            self.assertIsNone(sidecar["artifact"])

    def test_cell_local_technical_gap_does_not_starve_later_symbols(self):
        as_of = datetime(2026, 1, 10, 12, 2, tzinfo=UTC)
        listing_cutoff = datetime(2025, 12, 1, 0, 0, tzinfo=UTC)

        class OneSparseSymbolConnector(_RangeConnector):
            def get_bars_range(
                self, symbol, timeframe, start_date, end_date, sync_retry=False
            ):
                original_cutoff = self.cutoff
                self.cutoff = listing_cutoff if symbol == "NEWCFD" else None
                try:
                    return super().get_bars_range(
                        symbol,
                        timeframe,
                        start_date,
                        end_date,
                        sync_retry=sync_retry,
                    )
                finally:
                    self.cutoff = original_cutoff

        connector = OneSparseSymbolConnector()
        with tempfile.TemporaryDirectory() as tmp:
            _, native_root = self._paths(tmp)
            store = NativeHistoryStore(native_root, "cell-local-gap-continues")
            request = NativeHistoryRequest(
                symbols=("NEWCFD", "EURUSD"),
                timeframes=("M5",),
                horizon_years=2,
                as_of_utc=as_of,
                retries=1,
                retry_sleep_seconds=0.0,
                enforce_evidence_horizon=False,
            )
            result = NativeHistoryAcquirer(
                connector,
                store,
                request,
                now_fn=lambda: as_of,
            ).acquire()

            self.assertFalse(result["complete"])
            self.assertEqual(
                result["bounded_stop_reason"], "retryable_acquisition_failures"
            )
            self.assertEqual(result["remaining_cells"], 1)
            by_symbol = {item["symbol"]: item for item in result["results"]}
            self.assertEqual(
                by_symbol["NEWCFD"]["failure_status"], "technical_history_gap"
            )
            self.assertEqual(
                by_symbol["NEWCFD"]["resume_disposition"], "retryable_attempt"
            )
            self.assertEqual(by_symbol["EURUSD"]["status"], "success")
            self.assertTrue((store.run_root / "EURUSD" / "M5" / "bars.csv").is_file())
            self.assertFalse((store.run_root / "run_manifest.json").exists())

    def test_all_empty_pages_are_a_typed_source_failure_not_an_economic_verdict(self):
        as_of = datetime(2026, 1, 10, 12, 2, tzinfo=UTC)

        class AlwaysEmptyConnector(_RangeConnector):
            def get_bars_range(
                self, symbol, timeframe, start_date, end_date, sync_retry=False
            ):
                self.calls.append((symbol, timeframe, start_date, end_date, sync_retry))
                self._last_error = (1, "Success")
                self.diagnostics = _fixture_mt5_v4_empty_diagnostics(
                    symbol=symbol,
                    timeframe=timeframe,
                    start=start_date,
                    end=end_date,
                )
                return None

        connector = AlwaysEmptyConnector()
        with tempfile.TemporaryDirectory() as tmp:
            _, native_root = self._paths(tmp)
            store = NativeHistoryStore(native_root, "all-empty-pages")
            request = NativeHistoryRequest(
                symbols=("EURUSD",),
                timeframes=("M5",),
                horizon_years=1,
                as_of_utc=as_of,
                retries=1,
                retry_sleep_seconds=0.0,
                enforce_evidence_horizon=False,
            )
            manifest = NativeHistoryAcquirer(
                connector,
                store,
                request,
                now_fn=lambda: as_of,
            ).acquire()

            self.assertEqual(
                manifest["counts"],
                {
                    "total": 1,
                    "success": 0,
                    "partial": 0,
                    "failed": 1,
                },
            )
            result = manifest["results"][0]
            self.assertEqual(
                result["failure_status"], "terminal_history_empty_unresolved"
            )
            self.assertEqual(result["rows"], 0)
            self.assertIsNone(result["sha256"])
            leaf = store.run_root / "EURUSD" / "M5"
            self.assertFalse(leaf.exists())
            self.assertFalse(manifest["complete"])
            self.assertIsNone(manifest["manifest_path"])
            attempt_paths = list(
                (store.run_root / "_attempts" / "EURUSD" / "M5").glob("attempt-*.json")
            )
            self.assertEqual(len(attempt_paths), 1)
            attempt = json.loads(attempt_paths[0].read_text(encoding="utf-8"))
            self.assertTrue(attempt["nonfinal"])
            self.assertTrue(attempt["retryable"])
            provenance = attempt["record"]
            self.assertEqual(
                provenance["failure_status"], "terminal_history_empty_unresolved"
            )
            self.assertFalse(provenance["eligible_for_downstream_validation"])
            self.assertIsNone(provenance["artifact"])
            self.assertNotIn("columns", provenance)
            self.assertEqual(
                provenance["pagination"]["pages_requested"],
                len(provenance["pagination"]["empty_pages"]),
            )
            self.assertTrue(
                all(
                    page["copy_rates_range_attempts"] == 2
                    for page in provenance["pagination"]["page_attempts"]
                )
            )
            self.assertNotEqual(provenance["failure_status"], "no_history")
            self.assertIn("not proof of broker absence", provenance["error"]["message"])

    def test_connector_is_the_only_history_retry_owner(self):
        start = datetime(2024, 1, 1, tzinfo=UTC)
        end = datetime(2024, 1, 10, tzinfo=UTC)

        class _BoundedConnector:
            def __init__(self):
                self.calls = 0

            def get_bars_range(
                self,
                symbol,
                timeframe,
                start_date,
                end_date,
                sync_retry=False,
            ):
                self.calls += 1
                frame = _bar_frame([end_date - timedelta(minutes=5)])
                frame.attrs["mt5_history_range_diagnostics"] = (
                    _retried_page_diagnostics(
                        symbol=symbol,
                        timeframe=timeframe,
                        start=start_date,
                        end=end_date,
                        first_open=frame.index[0].to_pydatetime(),
                        last_open=frame.index[-1].to_pydatetime(),
                        rows=len(frame),
                    )
                )
                return frame

            @staticmethod
            def find_broker_symbol(symbol):
                return f"{symbol}.a"

            @staticmethod
            def get_last_error():
                # A post-return MT5 last_error can be stale. Released connector
                # diagnostics, captured immediately around the copy call, own
                # the hard-failure decision.
                return (-2, "stale invalid params")

        with tempfile.TemporaryDirectory() as tmp:
            _, native_root = self._paths(tmp)
            store = NativeHistoryStore(native_root, "outer-sync-retry")
            request = NativeHistoryRequest(
                symbols=("EURUSD",),
                timeframes=("M5",),
                horizon_years=1,
                as_of_utc=end,
                retries=1,
                retry_sleep_seconds=0.0,
                enforce_evidence_horizon=False,
            )
            connector = _BoundedConnector()
            acquirer = NativeHistoryAcquirer(
                connector,
                store,
                request,
                now_fn=lambda: end,
                runtime_metadata={},
            )

            try:
                frame, outcome = acquirer._fetch_page(
                    "EURUSD",
                    "M5",
                    start,
                    end,
                )

                self.assertIsNotNone(frame)
                self.assertEqual(len(frame), 1)
                self.assertEqual(connector.calls, 1)
                self.assertEqual(outcome["attempts"], 1)
                self.assertEqual(outcome["copy_rates_range_attempts"], 2)
            finally:
                store.close()

    def test_acquisition_rejects_connector_attempt_count_outside_v4_bound(self):
        class _InvalidLedgerConnector:
            @staticmethod
            def get_bars_range(*_args, **_kwargs):
                frame = _bar_frame([datetime(2024, 1, 9, 23, 55, tzinfo=UTC)])
                frame.attrs["mt5_history_range_diagnostics"] = {
                    "copy_rates_range_attempts": 3,
                    "hard_failure": False,
                    "sync_status": "exact_range_retrieved",
                }
                return frame

        with tempfile.TemporaryDirectory() as tmp:
            _, native_root = self._paths(tmp)
            store = NativeHistoryStore(native_root, "invalid-v4-ledger")
            request = NativeHistoryRequest(
                symbols=("EURUSD",),
                timeframes=("M5",),
                horizon_years=1,
                as_of_utc=datetime(2024, 1, 10, tzinfo=UTC),
                retries=1,
                retry_sleep_seconds=0.0,
                enforce_evidence_horizon=False,
            )
            try:
                _, outcome = NativeHistoryAcquirer(
                    _InvalidLedgerConnector(),
                    store,
                    request,
                    now_fn=lambda: datetime(2024, 1, 10, tzinfo=UTC),
                    runtime_metadata={},
                )._fetch_page(
                    "EURUSD",
                    "M5",
                    datetime(2024, 1, 1, tzinfo=UTC),
                    datetime(2024, 1, 10, tzinfo=UTC),
                )
                self.assertEqual(outcome["outcome"], "contract_error")
                self.assertEqual(outcome["copy_rates_range_attempts"], 3)
            finally:
                store.close()

    def test_connector_diagnostics_fail_bounded_before_unapproved_value_traversal(self):
        class _Poison:
            def __str__(self):
                raise AssertionError("unapproved diagnostic traversed")

        class _PoisonConnector:
            @staticmethod
            def get_bars_range(*_args, **_kwargs):
                frame = _bar_frame([datetime(2024, 1, 9, 23, 55, tzinfo=UTC)])
                frame.attrs["mt5_history_range_diagnostics"] = {
                    "copy_rates_range_attempts": 0,
                    "unapproved": _Poison(),
                }
                return frame

        with tempfile.TemporaryDirectory() as tmp:
            _, native_root = self._paths(tmp)
            store = NativeHistoryStore(native_root, "bounded-poison-diagnostic")
            request = NativeHistoryRequest(
                symbols=("EURUSD",),
                timeframes=("M5",),
                horizon_years=1,
                as_of_utc=datetime(2024, 1, 10, tzinfo=UTC),
                enforce_evidence_horizon=False,
            )
            try:
                frame, outcome = NativeHistoryAcquirer(
                    _PoisonConnector(), store, request, runtime_metadata={}
                )._fetch_page(
                    "EURUSD",
                    "M5",
                    datetime(2024, 1, 1, tzinfo=UTC),
                    datetime(2024, 1, 10, tzinfo=UTC),
                )
                self.assertIsNone(frame)
                self.assertEqual(outcome["outcome"], "contract_error")
                self.assertEqual(outcome["connector_diagnostics"], {})
            finally:
                store.close()

    def test_unapproved_dataframe_attrs_are_cleared_before_page_copy(self):
        class _Poison:
            def __deepcopy__(self, _memo):
                raise AssertionError("unapproved dataframe attr deep-copied")

        start = datetime(2024, 1, 1, tzinfo=UTC)
        end = datetime(2024, 1, 10, tzinfo=UTC)

        class _AttrConnector:
            @staticmethod
            def get_bars_range(symbol, timeframe, start_date, end_date, **_kwargs):
                frame = _bar_frame([end_date - timedelta(minutes=5)])
                frame.attrs["mt5_history_range_diagnostics"] = (
                    _retried_page_diagnostics(
                        symbol=symbol,
                        timeframe=timeframe,
                        start=start_date,
                        end=end_date,
                        first_open=frame.index[0].to_pydatetime(),
                        last_open=frame.index[-1].to_pydatetime(),
                        rows=len(frame),
                    )
                )
                frame.attrs["unapproved"] = _Poison()
                return frame

            @staticmethod
            def find_broker_symbol(symbol):
                return f"{symbol}.a"

        with tempfile.TemporaryDirectory() as tmp:
            _, native_root = self._paths(tmp)
            store = NativeHistoryStore(native_root, "bounded-frame-attrs")
            request = NativeHistoryRequest(
                symbols=("EURUSD",),
                timeframes=("M5",),
                horizon_years=1,
                as_of_utc=end,
                enforce_evidence_horizon=False,
            )
            try:
                frame, outcome = NativeHistoryAcquirer(
                    _AttrConnector(), store, request, runtime_metadata={}
                )._fetch_page("EURUSD", "M5", start, end)
                self.assertEqual(outcome["outcome"], "data")
                self.assertEqual(frame.attrs, {})
                normalized, _rows = NativeHistoryAcquirer._normalize_page(
                    frame, start, end
                )
                self.assertEqual(len(normalized), 1)
            finally:
                store.close()

    def test_connection_unavailable_is_not_rewritten_as_empty_history(self):
        class _DisconnectedConnector:
            def __init__(self):
                self.calls = 0
                self.diagnostics = {}

            def get_bars_range(
                self, symbol, timeframe, start_date, end_date, **_kwargs
            ):
                self.calls += 1
                diagnostics = _fixture_mt5_v4_empty_diagnostics(
                    symbol=symbol,
                    timeframe=timeframe,
                    start=start_date,
                    end=end_date,
                )
                diagnostics.update(
                    {
                        "broker_symbol": None,
                        "sync_status": "connection_unavailable",
                        "copy_rates_range_attempts": 0,
                        "copy_rates_range_max_attempts": 0,
                        "history_download_trigger_attempts": 0,
                        "history_download_trigger_nonempty": 0,
                        "history_download_trigger_max_bars": 0,
                        "history_download_trigger_errors": [],
                        "terminal_confirmation_consumed": False,
                        "call_ledger": [],
                        "hard_failure": False,
                        "hard_failure_call": None,
                        "last_error": None,
                        "initial_raw_relation": None,
                        "confirmation_raw_relation": None,
                        "raw_relation": None,
                        "raw_count": 0,
                        "raw_extent": None,
                        "admitted_count": 0,
                        "admitted_extent": None,
                        "boundary_span_seconds": 0,
                        "boundary_slack_seconds": 0,
                        "boundary_complete": False,
                        "best_extent": None,
                    }
                )
                self.diagnostics = diagnostics
                return None

            def get_last_history_range_diagnostics(self):
                return self.diagnostics

        connector = _DisconnectedConnector()
        with tempfile.TemporaryDirectory() as tmp:
            _, native_root = self._paths(tmp)
            store = NativeHistoryStore(native_root, "connection-unavailable")
            request = NativeHistoryRequest(
                symbols=("EURUSD",),
                timeframes=("M5",),
                horizon_years=1,
                as_of_utc=datetime(2024, 1, 10, tzinfo=UTC),
                retries=1,
                retry_sleep_seconds=0.0,
                enforce_evidence_horizon=False,
            )
            try:
                _, outcome = NativeHistoryAcquirer(
                    connector,
                    store,
                    request,
                    now_fn=lambda: datetime(2024, 1, 10, tzinfo=UTC),
                    runtime_metadata={},
                )._fetch_page(
                    "EURUSD",
                    "M5",
                    datetime(2024, 1, 1, tzinfo=UTC),
                    datetime(2024, 1, 10, tzinfo=UTC),
                )
                self.assertEqual(outcome["outcome"], "connector_preflight_failed")
                self.assertEqual(outcome["copy_rates_range_attempts"], 0)
                self.assertEqual(connector.calls, 1)
                self.assertIn("connection_unavailable", outcome["errors"][0]["message"])
            finally:
                store.close()

    def test_systemic_disconnect_stops_after_one_bounded_attempt(self):
        class _SystemicallyDisconnectedConnector(_RangeConnector):
            def __init__(self):
                super().__init__()
                self.connection_checks = 0
                self.find_calls = 0
                self.range_calls = 0

            def _check_connection(self):
                self.connection_checks += 1
                return False

            def find_broker_symbol(self, symbol):
                self.find_calls += 1
                raise AssertionError("symbol resolution must not run after preflight")

            def get_bars_range(self, *_args, **_kwargs):
                self.range_calls += 1
                raise AssertionError("history must not run after terminal preflight")

        as_of = datetime(2026, 1, 10, 12, 2, tzinfo=UTC)
        request = NativeHistoryRequest(
            symbols=("EURUSD", "GBPUSD"),
            timeframes=("M5", "M15"),
            horizon_years=1,
            as_of_utc=as_of,
            retries=1,
            retry_sleep_seconds=0.0,
            enforce_evidence_horizon=False,
        )
        with tempfile.TemporaryDirectory() as tmp:
            _, native_root = self._paths(tmp)
            connector = _SystemicallyDisconnectedConnector()
            store = NativeHistoryStore(native_root, "systemic-disconnect")
            result = NativeHistoryAcquirer(
                connector,
                store,
                request,
                now_fn=lambda: as_of,
            ).acquire()

            self.assertFalse(result["complete"])
            self.assertEqual(
                result["bounded_stop_reason"], "connector_preflight_failed"
            )
            self.assertEqual(result["resume"]["retryable_attempts"], 1)
            self.assertEqual(result["remaining_cells"], 4)
            self.assertEqual(connector.connection_checks, 1)
            self.assertEqual(connector.find_calls, 0)
            self.assertEqual(connector.range_calls, 0)
            attempts = list(store.run_root.glob("_attempts/*/*/attempt-*.json"))
            self.assertEqual(len(attempts), 1)
            record = _latest_attempt_record(store.run_root, "EURUSD", "M5")
            self.assertEqual(record["failure_status"], "connector_preflight_failed")
            self.assertEqual(
                record["connector_preflight_failure"]["history_range_calls"], 0
            )

    def test_symbol_resolution_failure_is_one_receipt_and_universe_continues(self):
        class _OneMissingSymbolConnector(_RangeConnector):
            def __init__(self):
                super().__init__()
                self.find_calls = []

            def find_broker_symbol(self, symbol):
                self.find_calls.append(symbol)
                return None if symbol == "MISSING" else f"{symbol}.a"

        as_of = datetime(2026, 1, 10, 12, 2, tzinfo=UTC)
        request = NativeHistoryRequest(
            symbols=("MISSING", "EURUSD"),
            timeframes=("M5", "M15"),
            horizon_years=1,
            as_of_utc=as_of,
            retries=1,
            retry_sleep_seconds=0.0,
            enforce_evidence_horizon=False,
        )
        with tempfile.TemporaryDirectory() as tmp:
            _, native_root = self._paths(tmp)
            connector = _OneMissingSymbolConnector()
            store = NativeHistoryStore(native_root, "one-symbol-resolution-receipt")
            result = NativeHistoryAcquirer(
                connector,
                store,
                request,
                now_fn=lambda: as_of,
            ).acquire()

            self.assertFalse(result["complete"])
            self.assertEqual(result["remaining_cells"], 2)
            self.assertEqual(result["resume"]["retryable_attempts"], 1)
            missing = [
                item for item in result["results"] if item["symbol"] == "MISSING"
            ]
            self.assertEqual(len(missing), 1)
            self.assertEqual(missing[0]["failure_status"], "symbol_resolution_failed")
            eurusd = [item for item in result["results"] if item["symbol"] == "EURUSD"]
            self.assertEqual(len(eurusd), 2)
            self.assertTrue(all(item["status"] == "success" for item in eurusd))
            attempts = list(store.run_root.glob("_attempts/*/*/attempt-*.json"))
            self.assertEqual(len(attempts), 1)
            self.assertEqual(connector.find_calls, ["MISSING", "EURUSD"])

    def test_request_rejects_any_outer_retry_budget_or_sleep(self):
        common = {
            "symbols": ("EURUSD",),
            "timeframes": ("M5",),
            "horizon_years": 1,
            "as_of_utc": datetime(2026, 1, 10, tzinfo=UTC),
            "enforce_evidence_horizon": False,
        }
        with self.assertRaisesRegex(ValueError, "connector owns"):
            NativeHistoryRequest(**common, retries=2, retry_sleep_seconds=0.0)
        for forged_retry in (1.0, 1.9, "1", True):
            with self.subTest(retries=forged_retry):
                with self.assertRaisesRegex(ValueError, "connector owns"):
                    NativeHistoryRequest(
                        **common,
                        retries=forged_retry,
                        retry_sleep_seconds=0.0,
                    )
        with self.assertRaisesRegex(ValueError, "no outer retry loop"):
            NativeHistoryRequest(**common, retries=1, retry_sleep_seconds=0.1)
        for forged_horizon in (1.0, "1", True, 0, 51):
            with self.subTest(horizon_years=forged_horizon):
                with self.assertRaisesRegex(ValueError, "horizon_years"):
                    NativeHistoryRequest(
                        **{**common, "horizon_years": forged_horizon},
                        retries=1,
                        retry_sleep_seconds=0.0,
                    )

    def test_prefix_sentinel_inside_strategy_stays_retryable(self):
        as_of = datetime(2026, 1, 10, 12, 0, tzinfo=UTC)

        class _PrefixSentinelConnector:
            def __init__(self):
                self.calls = []
                self.diagnostics = {}

            def find_broker_symbol(self, symbol):
                return f"{symbol}.a"

            def get_last_error(self):
                return (1, "Success")

            @staticmethod
            def get_runtime_metadata():
                return _RangeConnector().get_runtime_metadata()

            def get_last_history_range_diagnostics(self):
                return dict(self.diagnostics)

            def get_bars_range(
                self,
                symbol,
                timeframe,
                start_date,
                end_date,
                sync_retry=False,
            ):
                self.calls.append((start_date, end_date, sync_retry))
                if len(self.calls) < 3:
                    frame = _bar_frame([start_date, end_date - timedelta(minutes=5)])
                    self.diagnostics = _fixture_mt5_v4_page_diagnostics(
                        symbol=symbol,
                        timeframe=timeframe,
                        start=start_date,
                        end=end_date,
                        first_open=start_date,
                        last_open=end_date - timedelta(minutes=5),
                        rows=len(frame),
                    )
                    frame.attrs["mt5_history_range_diagnostics"] = dict(
                        self.diagnostics
                    )
                    return frame
                self.diagnostics = _fixture_mt5_v4_empty_diagnostics(
                    symbol=symbol,
                    timeframe=timeframe,
                    start=start_date,
                    end=end_date,
                )
                sentinel = int(end_date.timestamp()) + 86_400
                raw_extent = {
                    "rows": 1,
                    "first_epoch_second": sentinel,
                    "last_epoch_second": sentinel,
                }
                self.diagnostics.update(
                    {
                        "sync_status": "terminal_prefix_unsynchronized",
                        "confirmation_raw_relation": "RIGHT_DISJOINT_PREFIX",
                        "raw_relation": "RIGHT_DISJOINT_PREFIX",
                        "raw_count": 1,
                        "raw_extent": dict(raw_extent),
                        "history_download_trigger_nonempty": 1,
                    }
                )
                for ledger_entry in self.diagnostics["call_ledger"][1:]:
                    ledger_entry.update(
                        {
                            "raw_relation": "RIGHT_DISJOINT_PREFIX",
                            "raw_count": 1,
                            "raw_extent": dict(raw_extent),
                        }
                    )
                return None

        connector = _PrefixSentinelConnector()
        with tempfile.TemporaryDirectory() as tmp:
            _, native_root = self._paths(tmp)
            store = NativeHistoryStore(native_root, "prefix-sentinel")
            request = NativeHistoryRequest(
                symbols=("ALCOA",),
                timeframes=("M5",),
                horizon_years=1,
                as_of_utc=as_of,
                retries=1,
                retry_sleep_seconds=0.0,
                enforce_evidence_horizon=False,
            )
            manifest = NativeHistoryAcquirer(
                connector,
                store,
                request,
                now_fn=lambda: as_of,
            ).acquire()

            self.assertEqual(len(connector.calls), 3)
            self.assertGreater(connector.calls[0][0], connector.calls[1][0])
            self.assertGreater(connector.calls[1][0], connector.calls[2][0])
            self.assertTrue(all(call[2] for call in connector.calls))
            self.assertEqual(manifest["counts"]["partial"], 1)
            self.assertFalse(manifest["complete"])
            leaf = store.run_root / "ALCOA" / "M5"
            self.assertFalse(leaf.exists())
            sidecar = _latest_attempt_record(store.run_root, "ALCOA", "M5")
            self.assertEqual(sidecar["failure_status"], "technical_history_gap")
            self.assertFalse(sidecar["eligible_for_downstream_validation"])
            self.assertIsNone(sidecar.get("artifact"))
            self.assertFalse(
                any(
                    path.name.endswith(".assembly.staging")
                    for path in store.run_root.rglob("*")
                )
            )

    def test_unconfirmed_terminal_prefix_diagnostic_is_a_contract_failure(self):
        class _UnconfirmedPrefixConnector:
            @staticmethod
            def find_broker_symbol(symbol):
                return f"{symbol}.a"

            @staticmethod
            def get_bars_range(*_args, **_kwargs):
                return None

            @staticmethod
            def get_last_history_range_diagnostics():
                return {
                    "copy_rates_range_attempts": 1,
                    "sync_status": "terminal_prefix_unsynchronized",
                    "raw_relation": "RIGHT_DISJOINT_PREFIX",
                    "admitted_count": 0,
                    "admitted_extent": None,
                    "terminal_confirmation_consumed": True,
                    "hard_failure": False,
                }

        with tempfile.TemporaryDirectory() as tmp:
            _, native_root = self._paths(tmp)
            store = NativeHistoryStore(native_root, "unconfirmed-prefix")
            request = NativeHistoryRequest(
                symbols=("ALCOA",),
                timeframes=("M5",),
                horizon_years=1,
                as_of_utc=datetime(2026, 1, 10, tzinfo=UTC),
                retries=1,
                retry_sleep_seconds=0.0,
                enforce_evidence_horizon=False,
            )
            try:
                _, outcome = NativeHistoryAcquirer(
                    _UnconfirmedPrefixConnector(),
                    store,
                    request,
                    now_fn=lambda: datetime(2026, 1, 10, tzinfo=UTC),
                    runtime_metadata={},
                )._fetch_page(
                    "ALCOA",
                    "M5",
                    datetime(2025, 1, 1, tzinfo=UTC),
                    datetime(2025, 2, 1, tzinfo=UTC),
                )
                self.assertEqual(outcome["outcome"], "contract_error")
                self.assertTrue(
                    any(
                        "confirmation" in item["message"]
                        or "DIAGNOSTICS" in item["message"]
                        for item in outcome["errors"]
                    )
                )
            finally:
                store.close()

    def test_terminal_hcc_bound_stops_run_before_any_history_call(self):
        as_of = datetime(2026, 1, 10, 12, 0, tzinfo=UTC)
        connector = _RangeConnector()
        with tempfile.TemporaryDirectory() as tmp:
            root, native_root = self._paths(tmp)
            terminal_data = root / "terminal-data"
            symbol_cache = (
                terminal_data / "bases" / "MockBroker-ReadOnly" / "history" / "EURUSD.a"
            )
            symbol_cache.mkdir(parents=True)
            (symbol_cache / "2026.hcc").write_bytes(b"x" * 11)
            runtime_metadata = {
                "terminal": {
                    "data_path": str(terminal_data),
                    "max_bars": 10000,
                },
                "account": {"server": "MockBroker-ReadOnly"},
            }
            store = NativeHistoryStore(native_root, "hcc-resource-bound")
            request = NativeHistoryRequest(
                symbols=("EURUSD",),
                timeframes=("M5",),
                horizon_years=1,
                as_of_utc=as_of,
                retries=1,
                enforce_evidence_horizon=False,
            )
            with (
                patch(
                    "tools.acquire_native_history.TERMINAL_HCC_MAX_FILE_BYTES",
                    10,
                ),
                patch(
                    "tools.acquire_native_history.TERMINAL_MIN_FREE_BYTES",
                    0,
                ),
            ):
                result = NativeHistoryAcquirer(
                    connector,
                    store,
                    request,
                    now_fn=lambda: as_of,
                    runtime_metadata=runtime_metadata,
                ).acquire()

            self.assertFalse(result["complete"])
            self.assertEqual(
                result["bounded_stop_reason"], "terminal_cache_resource_limit"
            )
            self.assertEqual(connector.calls, [])
            self.assertFalse((store.run_root / "EURUSD" / "M5").exists())
            provenance = _latest_attempt_record(store.run_root, "EURUSD", "M5")
            self.assertEqual(
                provenance["failure_status"], "terminal_cache_resource_limit"
            )
            snapshot = provenance["terminal_cache_resource_snapshot"]
            self.assertIn("terminal_hcc_file_bound_breached", snapshot["violations"])
            self.assertEqual(snapshot["largest_hcc_bytes"], 11)
            self.assertFalse((store.run_root / "EURUSD" / "M5" / "bars.csv").exists())

    def test_terminal_hcc_scan_reports_zero_length_placeholders_exactly(self):
        with tempfile.TemporaryDirectory() as tmp:
            terminal_data = Path(tmp) / "terminal-data"
            symbol_cache = terminal_data / "history" / "EURUSD.a"
            symbol_cache.mkdir(parents=True)
            (symbol_cache / "2026.hcc").write_bytes(b"")

            snapshot = native_acquisition._bounded_terminal_hcc_scan(
                symbol_cache,
                relative_root=terminal_data,
                maximum_directory_depth=0,
            )

            self.assertEqual(snapshot["hcc_files_inspected"], 1)
            self.assertGreaterEqual(snapshot["entries_inspected"], 1)
            self.assertEqual(snapshot["largest_hcc_bytes"], 0)
            self.assertIsNone(snapshot["largest_hcc_relative_path"])
            self.assertEqual(snapshot["violations"], [])

    def test_terminal_hcc_scan_stops_at_directory_cap_plus_one(self):
        as_of = datetime(2026, 1, 10, 12, 0, tzinfo=UTC)
        connector = _RangeConnector()
        with tempfile.TemporaryDirectory() as tmp:
            root, native_root = self._paths(tmp)
            terminal_data = root / "terminal-data"
            symbol_cache = (
                terminal_data / "bases" / "MockBroker-ReadOnly" / "history" / "EURUSD.a"
            )
            symbol_cache.mkdir(parents=True)
            for year in (2024, 2025, 2026):
                (symbol_cache / f"{year}.hcc").write_bytes(b"x")
            runtime_metadata = {
                "terminal": {"data_path": str(terminal_data), "max_bars": 10000},
                "account": {"server": "MockBroker-ReadOnly"},
            }
            store = NativeHistoryStore(native_root, "hcc-directory-cap")
            request = NativeHistoryRequest(
                symbols=("EURUSD",),
                timeframes=("M5",),
                horizon_years=1,
                as_of_utc=as_of,
                retries=1,
                enforce_evidence_horizon=False,
            )
            with (
                patch.object(
                    native_acquisition,
                    "MAX_TERMINAL_DIRECTORY_ENTRIES",
                    2,
                ),
                patch.object(native_acquisition, "TERMINAL_MIN_FREE_BYTES", 0),
            ):
                result = NativeHistoryAcquirer(
                    connector,
                    store,
                    request,
                    now_fn=lambda: as_of,
                    runtime_metadata=runtime_metadata,
                ).acquire()
            self.assertFalse(result["complete"])
            self.assertEqual(connector.calls, [])
            receipt = _latest_attempt_record(store.run_root, "EURUSD", "M5")
            snapshot = receipt["terminal_cache_resource_snapshot"]
            self.assertIn(
                "terminal_hcc_directory_entry_cap_exceeded",
                snapshot["violations"],
            )
            self.assertEqual(snapshot["hcc_files_inspected"], 2)

    def test_terminal_hcc_scan_ignores_legitimate_symbol_cache_subtree(self):
        as_of = datetime(2026, 1, 10, 12, 0, tzinfo=UTC)
        connector = _RangeConnector()
        with tempfile.TemporaryDirectory() as tmp:
            root, native_root = self._paths(tmp)
            terminal_data = root / "terminal-data"
            symbol_history = (
                terminal_data / "bases" / "MockBroker-ReadOnly" / "history" / "EURUSD.a"
            )
            cache = symbol_history / "cache"
            cache.mkdir(parents=True)
            (symbol_history / "2026.hcc").write_bytes(b"year-history")
            (cache / "terminal-managed.bin").write_bytes(b"cache")
            runtime_metadata = {
                "terminal": {"data_path": str(terminal_data), "max_bars": 10000},
                "account": {"server": "MockBroker-ReadOnly"},
            }
            store = NativeHistoryStore(native_root, "hcc-legitimate-cache")
            request = NativeHistoryRequest(
                symbols=("EURUSD",),
                timeframes=("M5",),
                horizon_years=1,
                as_of_utc=as_of,
                retries=1,
                enforce_evidence_horizon=False,
            )
            try:
                with patch.object(
                    native_acquisition,
                    "TERMINAL_MIN_FREE_BYTES",
                    0,
                ):
                    result = NativeHistoryAcquirer(
                        connector,
                        store,
                        request,
                        now_fn=lambda: as_of,
                        runtime_metadata=runtime_metadata,
                    ).acquire()

                self.assertTrue(result["complete"])
                self.assertGreater(len(connector.calls), 0)
                self.assertNotEqual(
                    result.get("bounded_stop_reason"),
                    "terminal_cache_resource_limit",
                )
            finally:
                store.close()

    def test_page_ledger_cardinality_is_bounded_before_history_calls(self):
        as_of = datetime(2026, 1, 10, tzinfo=UTC)
        connector = _RangeConnector()
        metadata = connector.get_runtime_metadata()
        metadata["terminal"]["max_bars"] = 1
        with tempfile.TemporaryDirectory() as tmp:
            _, native_root = self._paths(tmp)
            store = NativeHistoryStore(native_root, "page-ledger-cap")
            request = NativeHistoryRequest(
                symbols=("EURUSD",),
                timeframes=("M5",),
                horizon_years=50,
                as_of_utc=as_of,
                retries=1,
                retry_sleep_seconds=0.0,
                enforce_evidence_horizon=False,
            )
            acquirer = NativeHistoryAcquirer(
                connector,
                store,
                request,
                now_fn=lambda: as_of,
                runtime_metadata=metadata,
            )
            try:
                with self.assertRaises(TerminalCacheResourceLimitError) as caught:
                    acquirer._fetch_timeframe("EURUSD", "EURUSD.a", "M5", as_of, {})
                self.assertIn(
                    "page_ledger_cardinality_cap_exceeded",
                    caught.exception.snapshot["violations"],
                )
                self.assertEqual(connector.calls, [])
            finally:
                store.close()

    def test_page_spill_matches_legacy_stable_dedup_bytes_and_boundary_filter(self):
        start = datetime(2026, 1, 1, tzinfo=UTC)
        middle = start + timedelta(minutes=10)
        end = start + timedelta(minutes=20)
        older_raw = _bar_frame(
            [
                start + timedelta(minutes=5),
                start,
                start + timedelta(minutes=5),
                middle,  # inclusive MT5 endpoint: excluded from older page
            ]
        )
        older_raw["Close"] = [1.11, 1.12, 1.19, 99.0]
        newer_raw = _bar_frame([middle, middle + timedelta(minutes=5)])
        newer_raw["Close"] = [2.0, 2.1]
        # Exercise the exact global dtype promotion that pd.concat used to do:
        # native pages may expose compatible numpy/nullable numeric dtypes.
        newer_raw["Volume"] = newer_raw["Volume"].astype("float64")
        newer_raw["Spread"] = newer_raw["Spread"].astype("Int64")
        older, _ = NativeHistoryAcquirer._normalize_page(older_raw, start, middle)
        newer, _ = NativeHistoryAcquirer._normalize_page(newer_raw, middle, end)

        # This is the removed implementation's exact ordering/dedup oracle:
        # connector pages arrive newest->oldest, stable sort preserves duplicate
        # input order, and keep-last retains 1.19 for the same-page duplicate.
        expected = pd.concat([newer, older]).sort_index(kind="stable")
        expected = expected[~expected.index.duplicated(keep="last")]
        expected["_SOURCE_FIRST_TS"] = expected.index
        expected["_SOURCE_LAST_TS"] = (
            expected.index + pd.Timedelta(minutes=5) - pd.Timedelta(nanoseconds=1)
        )
        expected["_BAR_CLOSE_TS"] = expected.index + pd.Timedelta(minutes=5)
        expected = expected.loc[:, native_acquisition.PRODUCTION_ARTIFACT_COLUMNS]
        expected_bytes = expected.to_csv(
            index_label="time", lineterminator="\n"
        ).encode("utf-8")

        with tempfile.TemporaryDirectory() as tmp:
            _, native_root = self._paths(tmp)
            store = NativeHistoryStore(native_root, "spill-byte-equivalence")
            assembly = native_acquisition._NativePageSpillAssembly(
                store.run_root, "EURUSD", "M5"
            )
            try:
                assembly.add_page(newer, middle, end)
                assembly.add_page(older, start, middle)
                artifact = self._finalize_spill(
                    assembly,
                    "M5",
                    start,
                    end,
                    [(start, middle, len(older)), (middle, end, len(newer))],
                )
                self.assertEqual(artifact.csv_path.read_bytes(), expected_bytes)
                self.assertEqual(
                    artifact.scan.artifact.sha256,
                    hashlib.sha256(expected_bytes).hexdigest(),
                )
                observed = pd.read_csv(artifact.csv_path, index_col="time")
                self.assertEqual(
                    observed.loc[str(start + timedelta(minutes=5)), "Close"], 1.19
                )
                self.assertEqual(observed.loc[str(middle), "Close"], 2.0)
                self.assertEqual(artifact.peak_in_process_rows, 3)
            finally:
                if "artifact" in locals():
                    artifact.cleanup()
                else:
                    assembly.cleanup()
                store.close()

    def test_page_spill_round_trips_adversarial_float64_values_exactly(self):
        start = datetime(2026, 1, 1, tzinfo=UTC)
        timestamps = [start + timedelta(minutes=5 * index) for index in range(6)]
        values = np.array(
            [
                np.nextafter(1.0, 0.0),
                np.nextafter(1.0, 2.0),
                -0.9891213503478509,
                np.nextafter(0.0, 1.0),
                np.finfo(np.float64).tiny,
                np.finfo(np.float64).max / 2.0,
            ],
            dtype=np.float64,
        )
        frame = _bar_frame(timestamps)
        for offset, column in enumerate(("Open", "High", "Low", "Close")):
            frame[column] = np.roll(values, offset)

        with tempfile.TemporaryDirectory() as tmp:
            _, native_root = self._paths(tmp)
            store = NativeHistoryStore(native_root, "spill-float64-round-trip")
            assembly = native_acquisition._NativePageSpillAssembly(
                store.run_root, "EURUSD", "M5"
            )
            try:
                assembly.add_page(
                    frame,
                    start,
                    start + timedelta(minutes=5 * len(frame)),
                )
                observed = assembly._read_page(assembly.pages[0])
                for column in ("Open", "High", "Low", "Close"):
                    self.assertTrue(
                        np.array_equal(
                            observed[column].to_numpy(dtype=np.float64),
                            frame[column].to_numpy(dtype=np.float64),
                        ),
                        column,
                    )
            finally:
                assembly.cleanup()
                store.close()

    def test_page_spill_preserves_global_mixed_float_dtype_promotion_bytes(self):
        start = datetime(2026, 1, 1, tzinfo=UTC)
        middle = start + timedelta(minutes=10)
        end = start + timedelta(minutes=20)
        older = _bar_frame([start, start + timedelta(minutes=5)])
        newer = _bar_frame([middle, middle + timedelta(minutes=5)])
        older["Close"] = pd.Series(
            np.array(
                [0.1, np.nextafter(np.float32(0.1), np.float32(1.0))], dtype=np.float32
            ),
            index=older.index,
        )
        newer["Close"] = np.array([0.2, np.nextafter(0.2, 1.0)], dtype=np.float64)
        expected = pd.concat([newer, older]).sort_index(kind="stable")
        expected = expected[~expected.index.duplicated(keep="last")]
        expected["_SOURCE_FIRST_TS"] = expected.index
        expected["_SOURCE_LAST_TS"] = (
            expected.index + pd.Timedelta(minutes=5) - pd.Timedelta(nanoseconds=1)
        )
        expected["_BAR_CLOSE_TS"] = expected.index + pd.Timedelta(minutes=5)
        expected = expected.loc[:, native_acquisition.PRODUCTION_ARTIFACT_COLUMNS]
        expected_bytes = expected.to_csv(
            index_label="time", lineterminator="\n"
        ).encode("utf-8")

        with tempfile.TemporaryDirectory() as tmp:
            _, native_root = self._paths(tmp)
            store = NativeHistoryStore(native_root, "spill-mixed-float-dtypes")
            assembly = native_acquisition._NativePageSpillAssembly(
                store.run_root, "EURUSD", "M5"
            )
            try:
                assembly.add_page(newer, middle, end)
                assembly.add_page(older, start, middle)
                artifact = self._finalize_spill(
                    assembly,
                    "M5",
                    start,
                    end,
                    [(start, middle, len(older)), (middle, end, len(newer))],
                )
                self.assertEqual(artifact.csv_path.read_bytes(), expected_bytes)
            finally:
                if "artifact" in locals():
                    artifact.cleanup()
                else:
                    assembly.cleanup()
                store.close()

    def test_page_spill_rejects_overlapping_admitted_chunks(self):
        start = datetime(2026, 1, 1, tzinfo=UTC)
        with tempfile.TemporaryDirectory() as tmp:
            _, native_root = self._paths(tmp)
            store = NativeHistoryStore(native_root, "spill-overlap-reject")
            assembly = native_acquisition._NativePageSpillAssembly(
                store.run_root, "EURUSD", "M5"
            )
            try:
                first = _bar_frame([start + timedelta(minutes=10)])
                second = _bar_frame([start + timedelta(minutes=10)])
                assembly.add_page(first, start, start + timedelta(minutes=15))
                assembly.add_page(
                    second,
                    start + timedelta(minutes=10),
                    start + timedelta(minutes=20),
                )
                with self.assertRaisesRegex(ValueError, "cross-page duplicate"):
                    self._finalize_spill(
                        assembly,
                        "M5",
                        start,
                        start + timedelta(minutes=20),
                        [
                            (start, start + timedelta(minutes=15), 1),
                            (
                                start + timedelta(minutes=10),
                                start + timedelta(minutes=20),
                                1,
                            ),
                        ],
                    )
            finally:
                assembly.cleanup()
                store.close()

    def test_page_spill_is_non_executable_and_digest_pinned(self):
        with tempfile.TemporaryDirectory() as tmp:
            _, native_root = self._paths(tmp)
            store = NativeHistoryStore(native_root, "spill-digest-pin")
            assembly = native_acquisition._NativePageSpillAssembly(
                store.run_root, "EURUSD", "M5"
            )
            start = datetime(2026, 1, 1, tzinfo=UTC)
            end = start + timedelta(minutes=10)
            try:
                assembly.add_page(
                    _bar_frame([start, start + timedelta(minutes=5)]),
                    start,
                    end,
                )
                page = assembly.pages[0]
                self.assertEqual(page.path.suffix, ".csv")
                raw = page.path.read_bytes()
                offset = raw.find(b"1.0")
                self.assertGreaterEqual(offset, 0)
                with page.path.open("r+b", buffering=0) as handle:
                    handle.seek(offset)
                    handle.write(b"2.0")
                    handle.flush()
                    os.fsync(handle.fileno())
                os.utime(
                    page.path,
                    ns=(page.identity.mtime_ns, page.identity.mtime_ns),
                )
                with self.assertRaisesRegex(
                    native_acquisition.NativePageAssemblyResourceLimitError,
                    "assembly_spill_digest_changed",
                ):
                    assembly._read_page(page)
            finally:
                assembly.cleanup()
                store.close()

    def test_page_spill_stops_at_page_count_cap_plus_one_before_write(self):
        with tempfile.TemporaryDirectory() as tmp:
            _, native_root = self._paths(tmp)
            store = NativeHistoryStore(native_root, "spill-page-count")
            assembly = native_acquisition._NativePageSpillAssembly(
                store.run_root, "EURUSD", "M5"
            )
            start = datetime(2026, 1, 1, tzinfo=UTC)
            midpoint = start + timedelta(minutes=5)
            end = midpoint + timedelta(minutes=5)
            try:
                with patch.object(native_acquisition, "MAX_PAGES_PER_CELL", 1):
                    assembly.add_page(_bar_frame([start]), start, midpoint)
                    with self.assertRaisesRegex(
                        native_acquisition.NativePageAssemblyResourceLimitError,
                        "assembly_page_count_cap_exceeded",
                    ):
                        assembly.add_page(_bar_frame([midpoint]), midpoint, end)
                self.assertEqual(len(assembly.pages), 1)
                self.assertEqual(
                    len(list(assembly.workspace.glob("page-*.csv"))),
                    1,
                )
            finally:
                assembly.cleanup()
                store.close()

    def test_page_spill_refuses_output_row_cap_plus_one_before_append(self):
        with tempfile.TemporaryDirectory() as tmp:
            _, native_root = self._paths(tmp)
            store = NativeHistoryStore(native_root, "spill-output-row-cap")
            assembly = native_acquisition._NativePageSpillAssembly(
                store.run_root, "EURUSD", "M5"
            )
            start = datetime(2026, 1, 1, tzinfo=UTC)
            end = start + timedelta(minutes=10)
            workspace = assembly.workspace
            try:
                assembly.add_page(
                    _bar_frame([start, start + timedelta(minutes=5)]),
                    start,
                    end,
                )
                with patch.object(native_acquisition, "MAX_CSV_ROWS", 1):
                    with self.assertRaisesRegex(
                        native_acquisition.NativePageAssemblyResourceLimitError,
                        "assembly_output_row_cap_exceeded",
                    ):
                        self._finalize_spill(
                            assembly,
                            "M5",
                            start,
                            end,
                            [(start, end, 2)],
                        )
                self.assertFalse(workspace.exists())
            finally:
                assembly.cleanup()
                store.close()

    def test_broker_aligned_page_spill_preserves_cross_page_successor_bytes(self):
        for timeframe in ("H4", "D1"):
            with (
                self.subTest(timeframe=timeframe),
                tempfile.TemporaryDirectory() as tmp,
            ):
                seconds = TIMEFRAME_SECONDS[timeframe]
                delta = timedelta(seconds=seconds)
                start = datetime(2026, 1, 1, tzinfo=UTC)
                middle = start + 2 * delta
                end = start + 4 * delta
                older = _bar_frame([start, start + delta])
                newer = _bar_frame([middle, middle + delta])
                combined = pd.concat([newer, older]).sort_index(kind="stable")
                combined = combined[~combined.index.duplicated(keep="last")]
                index_ns = combined.index.asi8
                nominal_ns = seconds * 1_000_000_000
                next_ns = native_acquisition.np.empty_like(index_ns)
                next_ns[:-1] = index_ns[1:]
                next_ns[-1] = index_ns[-1] + nominal_ns
                deltas = next_ns - index_ns
                closes = native_acquisition.np.where(
                    (deltas >= nominal_ns * 0.5) & (deltas <= nominal_ns * 1.5),
                    next_ns,
                    index_ns + nominal_ns,
                )
                combined["_SOURCE_FIRST_TS"] = combined.index
                combined["_SOURCE_LAST_TS"] = pd.to_datetime(closes - 1, utc=True)
                combined["_BAR_CLOSE_TS"] = pd.to_datetime(closes, utc=True)
                combined = combined.loc[
                    :, native_acquisition.PRODUCTION_ARTIFACT_COLUMNS
                ]
                expected_bytes = combined.to_csv(
                    index_label="time", lineterminator="\n"
                ).encode("utf-8")
                _, native_root = self._paths(tmp)
                store = NativeHistoryStore(native_root, f"spill-{timeframe.lower()}")
                assembly = native_acquisition._NativePageSpillAssembly(
                    store.run_root, "EURUSD", timeframe
                )
                try:
                    assembly.add_page(newer, middle, end)
                    assembly.add_page(older, start, middle)
                    artifact = self._finalize_spill(
                        assembly,
                        timeframe,
                        start,
                        end,
                        [(start, middle, 2), (middle, end, 2)],
                    )
                    self.assertEqual(artifact.csv_path.read_bytes(), expected_bytes)
                    observed = pd.read_csv(artifact.csv_path)
                    self.assertEqual(
                        pd.Timestamp(observed.iloc[1]["_BAR_CLOSE_TS"]),
                        pd.Timestamp(middle),
                    )
                finally:
                    if "artifact" in locals():
                        artifact.cleanup()
                        del artifact
                    else:
                        assembly.cleanup()
                    store.close()

    def test_many_page_spill_never_uses_full_cell_concat_and_cleans(self):
        start = datetime(2026, 1, 1, tzinfo=UTC)
        page_span = timedelta(minutes=10)
        page_count = 40
        end = start + page_count * page_span
        with tempfile.TemporaryDirectory() as tmp:
            _, native_root = self._paths(tmp)
            store = NativeHistoryStore(native_root, "spill-many-pages")
            assembly = native_acquisition._NativePageSpillAssembly(
                store.run_root, "EURUSD", "M5"
            )
            pages = []
            for position in reversed(range(page_count)):
                page_start = start + position * page_span
                page_end = page_start + page_span
                frame = _bar_frame([page_start, page_start + timedelta(minutes=5)])
                assembly.add_page(frame, page_start, page_end)
                pages.append((page_start, page_end, 2))
            pages.sort(key=lambda item: item[0])
            workspace = assembly.workspace
            try:
                with patch.object(
                    pd,
                    "concat",
                    side_effect=AssertionError("full-cell concat is forbidden"),
                ):
                    artifact = self._finalize_spill(assembly, "M5", start, end, pages)
                self.assertEqual(artifact.rows, page_count * 2)
                self.assertEqual(artifact.peak_in_process_rows, 2)
                self.assertLessEqual(
                    artifact.peak_in_process_rows,
                    native_acquisition.MAX_NATIVE_PAGE_IN_PROCESS_ROWS,
                )
            finally:
                if "artifact" in locals():
                    artifact.cleanup()
                else:
                    assembly.cleanup()
                self.assertFalse(workspace.exists())
                store.close()

    def test_page_row_budget_is_typed_retryable_and_leaves_no_spill(self):
        as_of = datetime(2026, 1, 10, 12, 2, tzinfo=UTC)
        connector = _RangeConnector()
        with tempfile.TemporaryDirectory() as tmp:
            _, native_root = self._paths(tmp)
            store = NativeHistoryStore(native_root, "spill-row-budget")
            request = NativeHistoryRequest(
                symbols=("EURUSD",),
                timeframes=("M5",),
                horizon_years=1,
                as_of_utc=as_of,
                retries=1,
                enforce_evidence_horizon=False,
            )
            with patch.object(native_acquisition, "MAX_NATIVE_PAGE_IN_PROCESS_ROWS", 1):
                manifest = NativeHistoryAcquirer(
                    connector, store, request, now_fn=lambda: as_of
                ).acquire()
            self.assertEqual(manifest["counts"]["failed"], 1)
            attempt = _latest_attempt_record(store.run_root, "EURUSD", "M5")
            self.assertEqual(
                attempt["failure_status"], "native_page_assembly_resource_limit"
            )
            self.assertIn(
                "assembly_page_in_process_row_cap_exceeded",
                attempt["native_page_assembly_resource_snapshot"]["violations"],
            )
            self.assertFalse((store.run_root / "EURUSD" / "M5").exists())
            self.assertFalse(
                any(
                    path.name.endswith(".assembly.staging")
                    for path in store.run_root.rglob("*")
                )
            )

    def test_csv_write_failure_exposes_no_partial_artifact_or_canonical_change(self):
        as_of = datetime(2026, 1, 10, 12, 2, tzinfo=UTC)
        connector = _RangeConnector()

        def explode_after_partial_write(frame, path):
            path.write_text("partial bytes", encoding="utf-8")
            raise OSError("simulated disk failure")

        with tempfile.TemporaryDirectory() as tmp:
            root, native_root = self._paths(tmp)
            canonical = root / "data" / "EURUSD_M5.csv"
            canonical.parent.mkdir(parents=True, exist_ok=True)
            canonical.write_text("canonical-must-not-change\n", encoding="utf-8")
            store = NativeHistoryStore(
                native_root,
                "atomic-failure-test",
                csv_writer=explode_after_partial_write,
            )
            request = NativeHistoryRequest(
                symbols=("EURUSD",),
                timeframes=("M5",),
                horizon_years=1,
                as_of_utc=as_of,
                retries=1,
                retry_sleep_seconds=0.0,
                enforce_evidence_horizon=False,
            )
            manifest = NativeHistoryAcquirer(
                connector,
                store,
                request,
                now_fn=lambda: as_of,
            ).acquire()

            leaf = store.run_root / "EURUSD" / "M5"
            self.assertEqual(manifest["counts"]["failed"], 1)
            self.assertFalse(leaf.exists())
            sidecar = _latest_attempt_record(store.run_root, "EURUSD", "M5")
            self.assertEqual(sidecar["failure_status"], "write_failed")
            self.assertIsNone(sidecar["artifact"])
            self.assertEqual(
                canonical.read_text(encoding="utf-8"), "canonical-must-not-change\n"
            )
            self.assertFalse(list(leaf.parent.glob(".*.staging")))

    def test_connector_contract_failure_stops_before_poisoning_later_timeframes(self):
        as_of = datetime(2026, 1, 10, 12, 2, tzinfo=UTC)
        connector = _RangeConnector(failing_timeframes={"H4"})
        with tempfile.TemporaryDirectory() as tmp:
            _, native_root = self._paths(tmp)
            store = NativeHistoryStore(native_root, "timeframe-isolation-test")
            request = NativeHistoryRequest(
                symbols=("EURUSD",),
                timeframes=DEFAULT_TIMEFRAMES,
                horizon_years=1,
                as_of_utc=as_of,
                retries=1,
                retry_sleep_seconds=0.0,
                enforce_evidence_horizon=False,
            )
            manifest = NativeHistoryAcquirer(
                connector,
                store,
                request,
                now_fn=lambda: as_of,
                sleep_fn=lambda _: None,
            ).acquire()

            self.assertEqual(
                manifest["counts"],
                {"success": 4, "partial": 0, "failed": 1, "total": 5},
            )
            self.assertFalse(manifest["complete"])
            self.assertEqual(
                manifest["bounded_stop_reason"], "unexpected_acquisition_failure"
            )
            by_timeframe = {item["timeframe"]: item for item in manifest["results"]}
            self.assertEqual(
                by_timeframe["H4"]["failure_status"],
                "unexpected_acquisition_failure",
            )
            self.assertFalse((store.run_root / "EURUSD" / "H4").exists())
            self.assertNotIn("D1", by_timeframe)
            for timeframe in {"M5", "M15", "M30", "H1"}:
                self.assertIn(by_timeframe[timeframe]["status"], {"success", "partial"})
                self.assertTrue(
                    (store.run_root / "EURUSD" / timeframe / "bars.csv").exists()
                )

    def test_hard_mt5_failure_is_retryable_not_empty_and_resume_reacquires(self):
        as_of = datetime(2026, 1, 10, 12, 2, tzinfo=UTC)

        class HardFailureConnector(_RangeConnector):
            def get_bars_range(
                self, symbol, timeframe, start_date, end_date, sync_retry=False
            ):
                self.calls.append((symbol, timeframe, start_date, end_date, sync_retry))
                self.diagnostics = _hard_failure_diagnostics(
                    symbol=symbol,
                    timeframe=timeframe,
                    start=start_date,
                    end=end_date,
                )
                return None

        request = NativeHistoryRequest(
            symbols=("EURUSD",),
            timeframes=("M5",),
            horizon_years=1,
            as_of_utc=as_of,
            retries=1,
            retry_sleep_seconds=0.0,
            enforce_evidence_horizon=False,
        )
        with tempfile.TemporaryDirectory() as tmp:
            _, native_root = self._paths(tmp)
            store = NativeHistoryStore(native_root, "hard-failure-resume")
            failed = NativeHistoryAcquirer(
                HardFailureConnector(), store, request, now_fn=lambda: as_of
            ).acquire()

            self.assertEqual(failed["counts"]["failed"], 1)
            self.assertFalse((store.run_root / "EURUSD" / "M5").exists())
            attempt = _latest_attempt_record(store.run_root, "EURUSD", "M5")
            self.assertEqual(attempt["failure_status"], "fetch_failed")
            pagination = attempt["pagination"]
            self.assertGreater(len(pagination["failed_pages"]), 0)
            self.assertEqual(pagination["empty_pages"], [])
            self.assertTrue(
                all(
                    page["connector_diagnostics"]["sync_status"] == "hard_mt5_failure"
                    for page in pagination["failed_pages"]
                )
            )

            stale = store.run_root / ".EURUSD.M5.dead.assembly.staging"
            stale.mkdir()
            (stale / "page.csv").write_text("crash residue", encoding="utf-8")
            resumed = NativeHistoryStore(
                native_root,
                "hard-failure-resume",
                resume=True,
            )
            self.assertFalse(stale.exists())
            self.assertEqual(resumed.stale_staging_removed, 1)

            healthy = _RangeConnector()
            completed = NativeHistoryAcquirer(
                healthy, resumed, request, now_fn=lambda: as_of
            ).acquire()
            self.assertTrue(completed["complete"])
            self.assertTrue(healthy.calls)
            self.assertTrue((resumed.run_root / "EURUSD" / "M5" / "bars.csv").is_file())
            self.assertFalse((resumed.run_root / "_attempts").exists())
            resumed.close()

    def test_v5_authenticated_late_history_suffix_finalizes_and_resumes(self):
        as_of = datetime(2026, 7, 13, 9, 0, tzinfo=UTC)
        boundary = datetime(2024, 7, 1, 0, 0, tzinfo=UTC)

        class _AvailableSuffixConnector(_RangeConnector):
            def __init__(self, *, sentinel_delta=timedelta(0)):
                super().__init__()
                self.sentinel_delta = sentinel_delta

            def get_bars_range(
                self,
                symbol,
                timeframe,
                start_date,
                end_date,
                sync_retry=False,
            ):
                self.calls.append((symbol, timeframe, start_date, end_date, sync_retry))
                if end_date <= boundary:
                    self.diagnostics = _authenticated_boundary_diagnostics(
                        symbol=symbol,
                        timeframe=timeframe,
                        start=start_date,
                        end=end_date,
                        boundary_open=boundary + self.sentinel_delta,
                    )
                    return None
                seconds = TIMEFRAME_SECONDS[timeframe]
                first = max(start_date, boundary)
                last = end_date - timedelta(seconds=seconds)
                if last < first:
                    raise AssertionError("test page cannot contain a closed bar")
                points = [first] if first == last else [first, last]
                frame = _bar_frame(points)
                data_args = {
                    "symbol": symbol,
                    "timeframe": timeframe,
                    "start": start_date,
                    "end": end_date,
                    "first_open": first,
                    "last_open": last,
                    "rows": len(frame),
                }
                initial = _fixture_mt5_v4_page_diagnostics(**data_args)
                self.diagnostics = (
                    initial
                    if initial["boundary_complete"]
                    else _retried_page_diagnostics(**data_args)
                )
                frame.attrs["mt5_history_range_diagnostics"] = dict(self.diagnostics)
                return frame

        timeframe_years = {
            timeframe: {
                "optimizer_train": 1,
                "tuning_validation": 1,
                "final_lockbox": 1,
            }
            for timeframe in DEFAULT_TIMEFRAMES
        }
        request = NativeHistoryRequest(
            symbols=("BTCETH",),
            timeframes=DEFAULT_TIMEFRAMES,
            horizon_years=5,
            as_of_utc=as_of,
            retries=1,
            retry_sleep_seconds=0.0,
            timeframe_window_years=timeframe_years,
            window_contract_version=TIMEFRAME_EVIDENCE_CONTRACT_VERSION,
            warmup_days=0.0,
            safety_days=0.0,
        )
        with tempfile.TemporaryDirectory() as tmp:
            root, native_root = self._paths(tmp)
            terminal_data = root / "terminal-data"
            (
                terminal_data / "bases" / "MockBroker-ReadOnly" / "history" / "BTCETH.a"
            ).mkdir(parents=True)
            connector = _AvailableSuffixConnector()
            runtime_metadata = connector.get_runtime_metadata()
            runtime_metadata["terminal"]["data_path"] = str(terminal_data)
            connector.get_runtime_metadata = lambda: runtime_metadata
            store = NativeHistoryStore(native_root, "listed-late-boundary")
            result = NativeHistoryAcquirer(
                connector,
                store,
                request,
                now_fn=lambda: as_of,
                sleep_fn=lambda _: None,
                runtime_metadata=runtime_metadata,
            ).acquire(max_new_cells=1)

            self.assertEqual(result["counts"]["partial"], 1)
            leaf = store.run_root / "BTCETH" / "M5"
            self.assertTrue((leaf / "bars.csv").is_file())
            provenance = json.loads(
                (leaf / "provenance.json").read_text(encoding="utf-8")
            )
            self.assertEqual(provenance["status"], "partial")
            self.assertEqual(provenance["failure_status"], "boundary_history_shortfall")
            self.assertFalse(provenance["eligible_for_optimization"])
            self.assertFalse(provenance["eligible_for_downstream_validation"])
            self.assertFalse(provenance["statistical_sufficiency_assessed"])
            self.assertEqual(
                provenance["coverage"]["actual_first_open_utc"], _iso(boundary)
            )
            self.assertEqual(
                provenance["coverage"]["technical_integrity_status"],
                "complete_page_ledger",
            )
            proof = provenance["pagination"]["available_history_boundary"]
            self.assertEqual(proof["boundary_open_utc"], _iso(boundary))
            self.assertFalse(proof["market_listing_date_asserted"])
            self.assertFalse(proof["statistical_sufficiency_assessed"])
            self.assertFalse((store.run_root / "_attempts" / "BTCETH" / "M5").exists())
            store.close()

            resumed_store = NativeHistoryStore(
                native_root,
                "listed-late-boundary",
                resume=True,
            )
            before_resume_calls = len(connector.calls)
            resumed = NativeHistoryAcquirer(
                connector,
                resumed_store,
                request,
                now_fn=lambda: as_of,
                sleep_fn=lambda _: None,
                runtime_metadata=runtime_metadata,
            )
            reusable = resumed._validated_reusable_cells()
            self.assertIn(("BTCETH", "M5"), reusable)
            self.assertEqual(len(connector.calls), before_resume_calls)
            resumed_store.close()

    def test_v5_mismatched_boundary_sentinel_remains_retryable_technical(self):
        as_of = datetime(2026, 7, 13, 9, 0, tzinfo=UTC)
        boundary = datetime(2024, 7, 1, 0, 0, tzinfo=UTC)

        class _MismatchedBoundaryConnector(_RangeConnector):
            def get_bars_range(
                self,
                symbol,
                timeframe,
                start_date,
                end_date,
                sync_retry=False,
            ):
                self.calls.append((symbol, timeframe, start_date, end_date, sync_retry))
                if end_date <= boundary:
                    self.diagnostics = _authenticated_boundary_diagnostics(
                        symbol=symbol,
                        timeframe=timeframe,
                        start=start_date,
                        end=end_date,
                        boundary_open=boundary + timedelta(minutes=5),
                    )
                    return None
                seconds = TIMEFRAME_SECONDS[timeframe]
                first = max(start_date, boundary)
                last = end_date - timedelta(seconds=seconds)
                points = [first] if first == last else [first, last]
                frame = _bar_frame(points)
                data_args = {
                    "symbol": symbol,
                    "timeframe": timeframe,
                    "start": start_date,
                    "end": end_date,
                    "first_open": first,
                    "last_open": last,
                    "rows": len(frame),
                }
                initial = _fixture_mt5_v4_page_diagnostics(**data_args)
                self.diagnostics = (
                    initial
                    if initial["boundary_complete"]
                    else _retried_page_diagnostics(**data_args)
                )
                frame.attrs["mt5_history_range_diagnostics"] = dict(self.diagnostics)
                return frame

        timeframe_years = {
            timeframe: {
                "optimizer_train": 1,
                "tuning_validation": 1,
                "final_lockbox": 1,
            }
            for timeframe in DEFAULT_TIMEFRAMES
        }
        request = NativeHistoryRequest(
            symbols=("BTCETH",),
            timeframes=DEFAULT_TIMEFRAMES,
            horizon_years=5,
            as_of_utc=as_of,
            timeframe_window_years=timeframe_years,
            window_contract_version=TIMEFRAME_EVIDENCE_CONTRACT_VERSION,
            warmup_days=0.0,
            safety_days=0.0,
        )
        with tempfile.TemporaryDirectory() as tmp:
            root, native_root = self._paths(tmp)
            terminal_data = root / "terminal-data"
            (
                terminal_data / "bases" / "MockBroker-ReadOnly" / "history" / "BTCETH.a"
            ).mkdir(parents=True)
            connector = _MismatchedBoundaryConnector()
            runtime_metadata = connector.get_runtime_metadata()
            runtime_metadata["terminal"]["data_path"] = str(terminal_data)
            connector.get_runtime_metadata = lambda: runtime_metadata
            store = NativeHistoryStore(native_root, "mismatched-late-boundary")
            result = NativeHistoryAcquirer(
                connector,
                store,
                request,
                now_fn=lambda: as_of,
                sleep_fn=lambda _: None,
                runtime_metadata=runtime_metadata,
            ).acquire(max_new_cells=1)

            self.assertEqual(result["counts"]["partial"], 1)
            self.assertFalse((store.run_root / "BTCETH" / "M5").exists())
            attempt = _latest_attempt_record(store.run_root, "BTCETH", "M5")
            self.assertEqual(attempt["failure_status"], "technical_history_gap")
            self.assertNotIn(
                "available_history_boundary", attempt.get("pagination", {})
            )
            self.assertFalse(attempt["eligible_for_downstream_validation"])
            store.close()

    def test_v5_h1_prestrategy_available_suffix_matches_au200_geometry(self):
        requested_cutoff = datetime(2026, 7, 14, 8, 9, 56, 737168, tzinfo=UTC)
        boundary = datetime(2013, 6, 11, 0, 0, tzinfo=UTC)

        class AU200BoundaryConnector(_RangeConnector):
            def __init__(self, *, sentinel_delta=timedelta(0)):
                super().__init__()
                self.sentinel_delta = sentinel_delta

            def get_bars_range(
                self,
                symbol,
                timeframe,
                start_date,
                end_date,
                sync_retry=False,
            ):
                self.calls.append((symbol, timeframe, start_date, end_date, sync_retry))
                if end_date <= boundary:
                    self.diagnostics = _authenticated_boundary_diagnostics(
                        symbol=symbol,
                        timeframe=timeframe,
                        start=start_date,
                        end=end_date,
                        boundary_open=boundary + self.sentinel_delta,
                    )
                    return None
                seconds = TIMEFRAME_SECONDS[timeframe]
                first = max(start_date, boundary)
                # AU200's observed H1 session tail in the production receipt is
                # 00:00 UTC inside pages whose fixed boundary is 08:00 UTC.
                last = end_date - timedelta(seconds=8 * seconds)
                if last < first:
                    raise AssertionError("test page cannot contain a closed bar")
                points = [first] if first == last else [first, last]
                frame = _bar_frame(points)
                data_args = {
                    "symbol": symbol,
                    "timeframe": timeframe,
                    "start": start_date,
                    "end": end_date,
                    "first_open": first,
                    "last_open": last,
                    "rows": len(frame),
                }
                initial = _fixture_mt5_v4_page_diagnostics(**data_args)
                self.diagnostics = (
                    initial
                    if initial["boundary_complete"]
                    else _retried_page_diagnostics(**data_args)
                )
                frame.attrs["mt5_history_range_diagnostics"] = dict(self.diagnostics)
                return frame

        for sentinel_delta, expected_failure in (
            (timedelta(0), "boundary_history_shortfall"),
            (timedelta(hours=1), "technical_history_gap"),
        ):
            with (
                self.subTest(sentinel_delta=sentinel_delta),
                tempfile.TemporaryDirectory() as tmp,
            ):
                root, native_root = self._paths(tmp)
                store = NativeHistoryStore(
                    native_root,
                    "au200-prestrategy-boundary"
                    + ("-mismatch" if sentinel_delta else ""),
                )
                artifact = None
                try:
                    connector = AU200BoundaryConnector(sentinel_delta=sentinel_delta)
                    terminal_data = root / "terminal-data"
                    (
                        terminal_data
                        / "bases"
                        / "MockBroker-ReadOnly"
                        / "history"
                        / "AU200.a"
                    ).mkdir(parents=True)
                    runtime_metadata = connector.get_runtime_metadata()
                    runtime_metadata["terminal"]["data_path"] = str(terminal_data)
                    request = NativeHistoryRequest(
                        symbols=("AU200",),
                        timeframes=DEFAULT_TIMEFRAMES,
                        as_of_utc=requested_cutoff,
                        retries=1,
                        retry_sleep_seconds=0.0,
                        boundary_tolerance_days=7.0,
                        timeframe_window_years={},
                        window_contract_version=TIMEFRAME_EVIDENCE_CONTRACT_VERSION,
                    )
                    acquirer = NativeHistoryAcquirer(
                        connector,
                        store,
                        request,
                        now_fn=lambda: requested_cutoff,
                        sleep_fn=lambda _: None,
                        runtime_metadata=runtime_metadata,
                    )
                    artifact, record = acquirer._fetch_timeframe(
                        "AU200",
                        "AU200.a",
                        "H1",
                        requested_cutoff,
                        {},
                    )

                    self.assertEqual(record["status"], "partial")
                    self.assertEqual(record["failure_status"], expected_failure)
                    pagination = record["pagination"]
                    self.assertEqual(pagination["empty_pages"], [])
                    self.assertEqual(pagination["failed_pages"], [])
                    self.assertEqual(
                        len(pagination["terminal_prefix_unresolved_pages"]), 1
                    )
                    terminal_page = pagination["terminal_prefix_unresolved_pages"][0]
                    self.assertEqual(terminal_page["start_utc"], "2011-07-18T08:00:00Z")
                    self.assertEqual(
                        terminal_page["end_exclusive_utc"],
                        "2012-07-17T08:00:00Z",
                    )
                    ledger = terminal_page["connector_diagnostics"]["call_ledger"]
                    self.assertEqual(len(ledger), 3)
                    self.assertTrue(
                        all(
                            entry["raw_relation"] == "RIGHT_DISJOINT_PREFIX"
                            and entry["raw_count"] == 1
                            for entry in ledger
                        )
                    )
                    self.assertEqual(len(pagination["truncated_pages"]), 1)
                    truncated = pagination["truncated_pages"][0]
                    self.assertEqual(truncated["actual_first_open_utc"], _iso(boundary))
                    self.assertEqual(truncated["start_delay_seconds"], 28_396_800.0)
                    self.assertEqual(truncated["end_lag_seconds"], 25_200.0)

                    if sentinel_delta:
                        self.assertIsNone(artifact)
                        self.assertNotIn("available_history_boundary", pagination)
                        self.assertEqual(
                            record["coverage"]["technical_integrity_status"],
                            "internal_gap_detected",
                        )
                    else:
                        self.assertIsNotNone(artifact)
                        proof = pagination["available_history_boundary"]
                        self.assertEqual(proof["boundary_open_utc"], _iso(boundary))
                        self.assertLessEqual(
                            pd.Timestamp(proof["terminal_page_end_exclusive_utc"]),
                            pd.Timestamp(
                                record["request"]["strategy_evidence_interval"][
                                    "start_utc"
                                ]
                            ),
                        )
                        self.assertEqual(
                            record["coverage"]["technical_integrity_status"],
                            "complete_page_ledger",
                        )
                        committed = store.commit_leaf("AU200", "H1", record, artifact)
                        artifact = None
                        result = acquirer._manifest_result(
                            "AU200",
                            "H1",
                            committed,
                            disposition="newly_acquired",
                        )
                        acquirer._validate_v5_publishable_leaf(
                            "AU200",
                            "H1",
                            store.run_root / "AU200" / "H1" / "bars.csv",
                            committed,
                            result,
                        )
                finally:
                    if artifact is not None:
                        artifact.cleanup()
                    store.close()

    def test_v5_h1_old_hard_defects_preserve_strategy_covering_suffix(self):
        as_of = datetime(2026, 7, 13, 9, 0, tzinfo=UTC)

        class HardPrefixConnector(_RangeConnector):
            def __init__(self):
                super().__init__()
                self.fail_call = 0

            def get_bars_range(
                self, symbol, timeframe, start_date, end_date, sync_retry=False
            ):
                self.calls.append((symbol, timeframe, start_date, end_date, sync_retry))
                if len(self.calls) == self.fail_call:
                    self.diagnostics = _hard_failure_diagnostics(
                        symbol=symbol,
                        timeframe=timeframe,
                        start=start_date,
                        end=end_date,
                    )
                    return None
                seconds = TIMEFRAME_SECONDS[timeframe]
                points = [start_date, end_date - timedelta(seconds=seconds)]
                frame = _bar_frame(points)
                self.diagnostics = _fixture_mt5_v4_page_diagnostics(
                    symbol=symbol,
                    timeframe=timeframe,
                    start=start_date,
                    end=end_date,
                    first_open=points[0],
                    last_open=points[-1],
                    rows=len(frame),
                )
                frame.attrs["mt5_history_range_diagnostics"] = dict(self.diagnostics)
                return frame

        for symbol, pages_before_oldest in (("AUDCHF", 0), ("AUDJPY", 2)):
            with self.subTest(symbol=symbol), tempfile.TemporaryDirectory() as tmp:
                root, native_root = self._paths(tmp)
                store = NativeHistoryStore(native_root, f"{symbol.lower()}-h1-tail")
                artifact = None
                try:
                    connector = HardPrefixConnector()
                    terminal_data = root / "terminal-data"
                    (
                        terminal_data
                        / "bases"
                        / "MockBroker-ReadOnly"
                        / "history"
                        / f"{symbol}.a"
                    ).mkdir(parents=True)
                    runtime_metadata = connector.get_runtime_metadata()
                    runtime_metadata["terminal"]["data_path"] = str(terminal_data)
                    request = NativeHistoryRequest(
                        symbols=(symbol,),
                        timeframes=DEFAULT_TIMEFRAMES,
                        as_of_utc=as_of,
                        retries=1,
                        retry_sleep_seconds=0.0,
                        timeframe_window_years={},
                        window_contract_version=TIMEFRAME_EVIDENCE_CONTRACT_VERSION,
                    )
                    acquirer = NativeHistoryAcquirer(
                        connector,
                        store,
                        request,
                        now_fn=lambda: as_of,
                        sleep_fn=lambda _: None,
                        runtime_metadata=runtime_metadata,
                    )
                    source_start, source_end = (
                        acquirer.timeframe_source_request_intervals["H1"]
                    )
                    planned = int(
                        np.ceil((source_end - source_start) / acquirer._page_span("H1"))
                    )
                    connector.fail_call = planned - pages_before_oldest
                    artifact, record = acquirer._fetch_timeframe(
                        symbol,
                        f"{symbol}.a",
                        "H1",
                        as_of,
                        {},
                    )
                    self.assertIsNotNone(artifact)
                    self.assertEqual(record["status"], "partial")
                    self.assertEqual(
                        record["failure_status"], "boundary_history_shortfall"
                    )
                    pagination = record["pagination"]
                    self.assertEqual(
                        pagination["stop_reason"], "technical_prefix_unresolved"
                    )
                    self.assertEqual(len(pagination["failed_pages"]), 1)
                    self.assertEqual(
                        record["coverage"]["technical_integrity_status"],
                        "complete_page_ledger",
                    )
                    strategy_start, _ = acquirer.timeframe_strategy_evidence_intervals[
                        "H1"
                    ]
                    failed_end = pd.Timestamp(
                        pagination["failed_pages"][0]["end_exclusive_utc"]
                    ).to_pydatetime()
                    self.assertLessEqual(failed_end, strategy_start)
                    self.assertLessEqual(
                        pd.Timestamp(record["coverage"]["actual_first_open_utc"]),
                        pd.Timestamp(strategy_start),
                    )
                    self.assertEqual(
                        pagination["unrequested_prefix"] is not None,
                        pages_before_oldest > 0,
                    )

                    committed = store.commit_leaf(symbol, "H1", record, artifact)
                    artifact = None
                    result = acquirer._manifest_result(
                        symbol,
                        "H1",
                        committed,
                        disposition="newly_acquired",
                    )
                    acquirer._validate_v5_publishable_leaf(
                        symbol,
                        "H1",
                        store.run_root / symbol / "H1" / "bars.csv",
                        committed,
                        result,
                    )
                finally:
                    if artifact is not None:
                        artifact.cleanup()
                    store.close()

    def test_silent_range_cap_is_detected_per_page(self):
        as_of = datetime(2026, 1, 10, 12, 2, tzinfo=UTC)

        class SilentlyCappedConnector(_RangeConnector):
            def get_bars_range(
                self, symbol, timeframe, start_date, end_date, sync_retry=False
            ):
                frame = super().get_bars_range(
                    symbol, timeframe, start_date, end_date, sync_retry=sync_retry
                )
                # Leave the outer pages intact so whole-run first/last timestamps
                # look complete, but silently truncate an interior response.
                if len(self.calls) == 2:
                    seconds = TIMEFRAME_SECONDS[timeframe]
                    point = end_date - timedelta(seconds=seconds)
                    frame = _bar_frame([point])
                    self.diagnostics = _retried_page_diagnostics(
                        symbol=symbol,
                        timeframe=timeframe,
                        start=start_date,
                        end=end_date,
                        first_open=point,
                        last_open=point,
                        rows=1,
                    )
                    frame.attrs["mt5_history_range_diagnostics"] = dict(
                        self.diagnostics
                    )
                    return frame
                return frame

        connector = SilentlyCappedConnector()
        with tempfile.TemporaryDirectory() as tmp:
            _, native_root = self._paths(tmp)
            store = NativeHistoryStore(native_root, "silent-cap-test")
            request = NativeHistoryRequest(
                symbols=("EURUSD",),
                timeframes=("M5",),
                horizon_years=1,
                as_of_utc=as_of,
                retries=1,
                retry_sleep_seconds=0.0,
                enforce_evidence_horizon=False,
            )
            manifest = NativeHistoryAcquirer(
                connector,
                store,
                request,
                now_fn=lambda: as_of,
            ).acquire()

            self.assertEqual(manifest["counts"]["partial"], 1)
            self.assertFalse(manifest["complete"])
            self.assertFalse((store.run_root / "EURUSD" / "M5").exists())
            sidecar = _latest_attempt_record(store.run_root, "EURUSD", "M5")
            self.assertEqual(sidecar["failure_status"], "technical_history_gap")
            self.assertEqual(len(sidecar["pagination"]["truncated_pages"]), 1)
            self.assertFalse(sidecar["eligible_for_optimization"])
            self.assertEqual(sidecar["error"]["type"], "TechnicalHistoryGap")

    def test_broker_aligned_d1_dst_keeps_last_closed_and_excludes_forming_bar(self):
        # The information cutoff is exactly the next broker D1 open. That next
        # bar is a sentinel proving the prior 23h/24h DST bar closed, but is
        # itself still forming and must not be published.
        as_of = datetime(2025, 3, 31, 21, 0, tzinfo=UTC)
        opens = pd.to_datetime(
            [
                "2025-03-27T22:00:00Z",
                "2025-03-28T22:00:00Z",
                # Broker changes from UTC+2 to UTC+3 over the DST weekend.
                "2025-03-30T21:00:00Z",
                "2025-03-31T21:00:00Z",  # still forming at as_of
            ],
            utc=True,
        )

        class BrokerAlignedD1(_RangeConnector):
            def get_bars_range(
                self, symbol, timeframe, start_date, end_date, sync_retry=False
            ):
                self.calls.append((symbol, timeframe, start_date, end_date, sync_retry))
                self._last_error = (1, "Success")
                selected = [
                    ts for ts in opens if start_date <= ts.to_pydatetime() < end_date
                ]
                if not selected:
                    self.diagnostics = _fixture_mt5_v4_empty_diagnostics(
                        symbol=symbol,
                        timeframe=timeframe,
                        start=start_date,
                        end=end_date,
                    )
                    return None
                frame = _bar_frame(selected)
                self.diagnostics = _retried_page_diagnostics(
                    symbol=symbol,
                    timeframe=timeframe,
                    start=start_date,
                    end=end_date,
                    first_open=selected[0].to_pydatetime(),
                    last_open=selected[-1].to_pydatetime(),
                    rows=len(frame),
                )
                frame.attrs["mt5_history_range_diagnostics"] = dict(self.diagnostics)
                return frame

        connector = BrokerAlignedD1()
        with tempfile.TemporaryDirectory() as tmp:
            _, native_root = self._paths(tmp)
            store = NativeHistoryStore(native_root, "broker-dst-d1-test")
            request = NativeHistoryRequest(
                symbols=("EURUSD",),
                timeframes=("D1",),
                horizon_years=1,
                as_of_utc=as_of,
                retries=1,
                retry_sleep_seconds=0.0,
                enforce_evidence_horizon=False,
            )
            NativeHistoryAcquirer(
                connector,
                store,
                request,
                now_fn=lambda: as_of,
            ).acquire()

            bars = pd.read_csv(store.run_root / "EURUSD" / "D1" / "bars.csv")
            published = list(pd.to_datetime(bars["time"], utc=True))
            self.assertIn(pd.Timestamp("2025-03-30T21:00:00Z"), published)
            self.assertNotIn(pd.Timestamp("2025-03-31T21:00:00Z"), published)
            sidecar = json.loads(
                (store.run_root / "EURUSD" / "D1" / "provenance.json").read_text(
                    encoding="utf-8"
                )
            )
            self.assertEqual(
                sidecar["request"]["alignment_mode"],
                "broker_observed_successor_or_nominal_close_v1",
            )
            self.assertEqual(
                sidecar["request"]["last_fully_closed_bar_open_utc"],
                "2025-03-30T21:00:00Z",
            )
            self.assertEqual(
                sidecar["coverage"]["actual_last_close_utc"], "2025-03-31T21:00:00Z"
            )

    def test_broker_aligned_h4_true_interior_page_truncation_remains_technical(self):
        as_of = datetime(2026, 1, 5, tzinfo=UTC)

        class InteriorTruncatedH4(_RangeConnector):
            def __init__(self):
                super().__init__()
                self.h4_page = 0

            def get_bars_range(
                self, symbol, timeframe, start_date, end_date, sync_retry=False
            ):
                self.calls.append((symbol, timeframe, start_date, end_date, sync_retry))
                self._last_error = (1, "Success")
                if timeframe != "H4":
                    raise AssertionError("this focused connector only serves H4")
                self.h4_page += 1
                points = pd.date_range(
                    start=start_date,
                    end=end_date,
                    freq="4h",
                    inclusive="left",
                )
                # Delay one genuine interior page by more than the declared
                # one-day tolerance.  The source-union prefix fix must not
                # launder this into a harmless leading boundary shortfall.
                if self.h4_page == 2:
                    points = points[13:]
                if not len(points):
                    self.diagnostics = _fixture_mt5_v4_empty_diagnostics(
                        symbol=symbol,
                        timeframe=timeframe,
                        start=start_date,
                        end=end_date,
                    )
                    return None
                frame = _bar_frame(points)
                data_args = {
                    "symbol": symbol,
                    "timeframe": timeframe,
                    "start": start_date,
                    "end": end_date,
                    "first_open": points[0].to_pydatetime(),
                    "last_open": points[-1].to_pydatetime(),
                    "rows": len(frame),
                }
                initial = _fixture_mt5_v4_page_diagnostics(**data_args)
                self.diagnostics = (
                    initial
                    if initial["boundary_complete"]
                    else _retried_page_diagnostics(**data_args)
                )
                frame.attrs["mt5_history_range_diagnostics"] = dict(self.diagnostics)
                return frame

        request = NativeHistoryRequest(
            symbols=("EURUSD",),
            timeframes=DEFAULT_TIMEFRAMES,
            as_of_utc=as_of,
            retries=1,
            retry_sleep_seconds=0.0,
            timeframe_window_years={},
            window_contract_version=TIMEFRAME_EVIDENCE_CONTRACT_VERSION,
            warmup_days=0.0,
            safety_days=0.0,
            boundary_tolerance_days=1.0,
        )
        with tempfile.TemporaryDirectory() as tmp:
            _, native_root = self._paths(tmp)
            store = NativeHistoryStore(native_root, "broker-h4-interior-truncation")
            try:
                connector = InteriorTruncatedH4()
                acquirer = NativeHistoryAcquirer(
                    connector,
                    store,
                    request,
                    now_fn=lambda: as_of,
                    sleep_fn=lambda _: None,
                )
                frame, record = acquirer._fetch_timeframe(
                    "EURUSD",
                    "EURUSD.a",
                    "H4",
                    as_of,
                    {},
                )
            finally:
                store.close()

        # Technical partials are nonpublishable.  The bounded assembler must
        # delete their spill tree before returning rather than leak a full-cell
        # frame solely as a failure marker.
        self.assertIsNone(frame)
        self.assertEqual(record["failure_status"], "technical_history_gap")
        self.assertGreaterEqual(record["coverage"]["internal_truncated_page_count"], 1)
        self.assertEqual(
            record["coverage"]["technical_integrity_status"],
            "internal_gap_detected",
        )
        self.assertFalse(record["eligible_for_downstream_validation"])

    def test_store_is_shadow_only_and_create_only(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            with self.assertRaises(ValueError):
                NativeHistoryStore(root / "data", "wrong-root")
            native_root = root / "data" / "native"
            NativeHistoryStore(native_root, "immutable-run")
            with self.assertRaises(FileExistsError):
                NativeHistoryStore(native_root, "immutable-run")

    def test_output_storage_guard_stops_before_csv_serialization(self):
        with tempfile.TemporaryDirectory() as tmp:
            _, native_root = self._paths(tmp)
            store = NativeHistoryStore(native_root, "output-resource-bound")
            frame = _bar_frame([datetime(2026, 1, 1, tzinfo=UTC)])
            low_space = SimpleNamespace(
                total=100 * 1024**3,
                free=OUTPUT_MIN_FREE_BYTES,
                used=96 * 1024**3,
            )
            try:
                with patch(
                    "tools.acquire_native_history.shutil.disk_usage",
                    return_value=low_space,
                ):
                    with self.assertRaises(OutputStorageResourceLimitError):
                        store.commit_leaf(
                            "EURUSD",
                            "M5",
                            _minimal_provenance("output-resource-bound"),
                            frame,
                        )
                self.assertFalse(store.leaf_path("EURUSD", "M5").exists())
                self.assertFalse(
                    any(
                        path.name.endswith(".staging")
                        for path in (store.run_root / "EURUSD").iterdir()
                    )
                )
            finally:
                store.close()

    def test_streamed_artifact_cap_plus_one_fails_before_leaf_publish(self):
        with tempfile.TemporaryDirectory() as tmp:
            _, native_root = self._paths(tmp)
            store = NativeHistoryStore(native_root, "stream-cap-plus-one")
            artifact = self._single_page_spill_artifact(store)
            workspace = artifact.workspace
            try:
                with patch.object(
                    native_acquisition,
                    "OUTPUT_MAX_CELL_BYTES",
                    artifact.bytes - 1,
                ):
                    with self.assertRaises(OutputStorageResourceLimitError):
                        store.commit_leaf(
                            "EURUSD",
                            "M5",
                            _minimal_provenance("stream-cap-plus-one"),
                            artifact,
                        )
                self.assertFalse(store.leaf_path("EURUSD", "M5").exists())
                self.assertFalse(workspace.exists())
                self.assertFalse(list(store.run_root.rglob("*.staging")))
            finally:
                artifact.cleanup()
                store.close()

    def test_streamed_artifact_free_space_fall_before_publish_leaves_no_leaf(self):
        with tempfile.TemporaryDirectory() as tmp:
            _, native_root = self._paths(tmp)
            store = NativeHistoryStore(native_root, "stream-free-fall")
            artifact = self._single_page_spill_artifact(store)
            total = 100 * 1024**3
            high = SimpleNamespace(
                total=total,
                free=OUTPUT_MIN_FREE_BYTES + 1024,
                used=total - OUTPUT_MIN_FREE_BYTES - 1024,
            )
            low = SimpleNamespace(
                total=total,
                free=OUTPUT_MIN_FREE_BYTES - 1,
                used=total - OUTPUT_MIN_FREE_BYTES + 1,
            )
            workspace = artifact.workspace
            try:
                with patch(
                    "tools.acquire_native_history.shutil.disk_usage",
                    side_effect=[high, high, low],
                ):
                    with self.assertRaises(OutputStorageResourceLimitError):
                        store.commit_leaf(
                            "EURUSD",
                            "M5",
                            _minimal_provenance("stream-free-fall"),
                            artifact,
                        )
                self.assertFalse(store.leaf_path("EURUSD", "M5").exists())
                self.assertFalse(workspace.exists())
                self.assertFalse(list(store.run_root.rglob("*.staging")))
            finally:
                artifact.cleanup()
                store.close()

    def test_streamed_artifact_mutation_before_publish_is_detected(self):
        with tempfile.TemporaryDirectory() as tmp:
            _, native_root = self._paths(tmp)
            store = NativeHistoryStore(native_root, "stream-mutation-race")
            artifact = self._single_page_spill_artifact(store)
            original_sha256 = native_acquisition._sha256
            calls = 0

            def mutate_after_staged_digest(path):
                nonlocal calls
                calls += 1
                digest = original_sha256(path)
                if calls == 2:
                    with Path(path).open("ab") as handle:
                        handle.write(b"MUTATION")
                        handle.flush()
                        os.fsync(handle.fileno())
                return digest

            try:
                with patch.object(
                    native_acquisition,
                    "_sha256",
                    side_effect=mutate_after_staged_digest,
                ):
                    with self.assertRaises(OutputStorageResourceLimitError):
                        store.commit_leaf(
                            "EURUSD",
                            "M5",
                            _minimal_provenance("stream-mutation-race"),
                            artifact,
                        )
                self.assertFalse(store.leaf_path("EURUSD", "M5").exists())
                self.assertFalse(list(store.run_root.rglob("*.staging")))
            finally:
                artifact.cleanup()
                store.close()

    def test_successful_stream_publish_clears_only_its_attempt_receipts(self):
        with tempfile.TemporaryDirectory() as tmp:
            _, native_root = self._paths(tmp)
            store = NativeHistoryStore(native_root, "stream-attempt-cleanup")
            failed = _minimal_provenance("stream-attempt-cleanup")
            failed.update(
                {
                    "status": "failed",
                    "failure_status": "fetch_failed",
                    "eligible_for_downstream_validation": False,
                    "error": {"type": "PageFetchError", "message": "retry"},
                }
            )
            store.commit_retryable_attempt("EURUSD", "M5", failed)
            other = dict(failed)
            other["timeframe"] = "M15"
            other["timeframe_seconds"] = TIMEFRAME_SECONDS["M15"]
            store.commit_retryable_attempt("EURUSD", "M15", other)
            artifact = self._single_page_spill_artifact(store)
            try:
                record = store.commit_leaf(
                    "EURUSD",
                    "M5",
                    _minimal_provenance("stream-attempt-cleanup"),
                    artifact,
                )
                self.assertEqual(record["status"], "success")
                attempts = store.run_root / "_attempts" / "EURUSD"
                self.assertFalse((attempts / "M5").exists())
                self.assertTrue((attempts / "M15").is_dir())
                self.assertTrue(store.leaf_path("EURUSD", "M5").is_dir())
            finally:
                artifact.cleanup()
                store.close()

    def test_retry_receipt_projects_before_traversal_and_retains_only_two(self):
        class Poison:
            def __str__(self):
                raise AssertionError("unapproved provenance value was traversed")

        with tempfile.TemporaryDirectory() as tmp:
            _, native_root = self._paths(tmp)
            store = NativeHistoryStore(native_root, "bounded-attempt-projection")
            failed = _minimal_provenance("bounded-attempt-projection")
            nested = {"leaf": True}
            for _ in range(40):
                nested = {"nested": nested}
            failed.update(
                {
                    "status": "failed",
                    "failure_status": "fetch_failed",
                    "pagination": nested,
                    "error": {"type": "PageFetchError", "message": "retry"},
                    "unapproved_poison": Poison(),
                }
            )
            bounded = store.commit_retryable_attempt("EURUSD", "M5", failed)
            self.assertEqual(bounded["error"]["type"], "RetryableProvenanceBounded")
            self.assertNotIn("pagination", bounded)

            normal = dict(failed)
            normal.pop("pagination")
            for index in range(3):
                normal["error"] = {
                    "type": "PageFetchError",
                    "message": f"retry-{index}",
                }
                store.commit_retryable_attempt("EURUSD", "M5", normal)
            receipts = list(
                (store.run_root / "_attempts" / "EURUSD" / "M5").glob("attempt-*.json")
            )
            self.assertEqual(len(receipts), 2)
            store.close()

    def test_exact_run_artifact_byte_cap_accepts_boundary_and_rejects_cap_plus_one(
        self,
    ):
        with tempfile.TemporaryDirectory() as tmp:
            _, native_root = self._paths(tmp)
            store = NativeHistoryStore(native_root, "aggregate-byte-bound")
            first = self._single_page_spill_artifact(store, "M5")
            second = self._single_page_spill_artifact(store, "M15")
            exact_limit = int(first.bytes)
            try:
                with patch.object(
                    native_acquisition,
                    "MAX_TOTAL_ARTIFACT_BYTES",
                    exact_limit,
                ):
                    store.commit_leaf(
                        "EURUSD",
                        "M5",
                        _minimal_provenance("aggregate-byte-bound"),
                        first,
                    )
                    with self.assertRaisesRegex(
                        OutputStorageResourceLimitError,
                        "output_run_total_artifact_byte_cap_exceeded",
                    ):
                        store.commit_leaf(
                            "EURUSD",
                            "M15",
                            _minimal_provenance(
                                "aggregate-byte-bound", timeframe="M15"
                            ),
                            second,
                        )
                self.assertTrue(store.leaf_path("EURUSD", "M5").is_dir())
                self.assertFalse(store.leaf_path("EURUSD", "M15").exists())
                self.assertFalse((store.run_root / "run_manifest.json").exists())
            finally:
                first.cleanup()
                second.cleanup()
                store.close()

    def test_manifest_refuses_unresolved_attempts_and_removes_only_empty_root(self):
        with tempfile.TemporaryDirectory() as tmp:
            _, native_root = self._paths(tmp)
            store = NativeHistoryStore(native_root, "manifest-attempt-gate")
            failed = _minimal_provenance("manifest-attempt-gate")
            failed.update(
                {
                    "status": "failed",
                    "failure_status": "fetch_failed",
                    "error": {"type": "PageFetchError", "message": "retry"},
                }
            )
            store.commit_retryable_attempt("EURUSD", "M5", failed)
            with self.assertRaisesRegex(
                ResumeValidationError,
                "retryable attempt evidence remains",
            ):
                store.commit_manifest({"run_id": "manifest-attempt-gate"})
            self.assertFalse((store.run_root / "run_manifest.json").exists())
            store.close()

            empty_store = NativeHistoryStore(native_root, "empty-attempt-root")
            (empty_store.run_root / "_attempts").mkdir()
            manifest_path = empty_store.commit_manifest(
                {"run_id": "empty-attempt-root"}
            )
            self.assertTrue(manifest_path.is_file())
            self.assertFalse((empty_store.run_root / "_attempts").exists())
            empty_store.close()

    def test_manifest_post_link_crash_twin_is_reconciled_on_restart(self):
        class _CrashAfterLink(BaseException):
            pass

        def link_then_crash(source, destination):
            os.link(source, destination)
            raise _CrashAfterLink()

        run_id = "manifest-post-link-crash"
        manifest = {
            "schema_version": native_acquisition.MANIFEST_SCHEMA_VERSION,
            "immutable_record": True,
            "complete": True,
            "run_id": run_id,
        }
        with tempfile.TemporaryDirectory() as tmp:
            _, native_root = self._paths(tmp)
            store = NativeHistoryStore(
                native_root,
                run_id,
                manifest_link_fn=link_then_crash,
                publish_retry_delays=(),
            )
            with self.assertRaises(_CrashAfterLink):
                store.commit_manifest(manifest)
            final_path = store.run_root / "run_manifest.json"
            staging_path = store.runs_root / f".{run_id}.run_manifest.staging"
            self.assertEqual(final_path.stat().st_ino, staging_path.stat().st_ino)
            self.assertEqual(final_path.stat().st_nlink, 2)
            store.close()

            with self.assertRaisesRegex(ResumeValidationError, "already has"):
                NativeHistoryStore(native_root, run_id, resume=True)
            self.assertFalse(staging_path.exists())
            self.assertEqual(final_path.stat().st_nlink, 1)
            self.assertEqual(
                json.loads(final_path.read_text(encoding="utf-8")), manifest
            )

    def test_resume_seed_post_link_crash_twin_is_reconciled_before_load(self):
        run_id = "seed-post-link-crash"
        started = datetime(2026, 1, 10, tzinfo=UTC)
        with tempfile.TemporaryDirectory() as tmp:
            _, native_root = self._paths(tmp)
            store = NativeHistoryStore(native_root, run_id)
            try:
                metadata = _RangeConnector().get_runtime_metadata()
                request = NativeHistoryRequest(
                    symbols=("EURUSD",),
                    timeframes=("M5",),
                    horizon_years=1,
                    as_of_utc=started,
                    enforce_evidence_horizon=False,
                )
                acquirer = NativeHistoryAcquirer(
                    _RangeConnector(),
                    store,
                    request,
                    now_fn=lambda: started,
                    runtime_metadata=metadata,
                )
                store.ensure_resume_contract(
                    acquirer._common_request_contract(),
                    metadata,
                    started,
                    symbols=("EURUSD",),
                    timeframes=("M5",),
                    production_authoritative=False,
                )
                seed_path = store.run_root / native_acquisition.RESUME_SEED_FILENAME
                staging_path = (
                    store.run_root
                    / f".{native_acquisition.RESUME_SEED_FILENAME}.{'a' * 32}.staging"
                )
                # Reconstruct the exact on-disk state left by an OS-level kill,
                # which bypasses Python finally blocks after link and before unlink.
                os.link(seed_path, staging_path)
                self.assertEqual(seed_path.stat().st_ino, staging_path.stat().st_ino)
                self.assertEqual(seed_path.stat().st_nlink, 2)
            finally:
                store.close()

            resumed = NativeHistoryStore(native_root, run_id, resume=True)
            try:
                self.assertEqual(resumed.resume_contract.symbols, ("EURUSD",))
                self.assertEqual(seed_path.stat().st_nlink, 1)
                self.assertFalse(staging_path.exists())
            finally:
                resumed.close()

    def test_sealed_run_refuses_every_post_manifest_mutation(self):
        run_id = "sealed-run-mutation-gate"
        manifest = {
            "schema_version": native_acquisition.MANIFEST_SCHEMA_VERSION,
            "immutable_record": True,
            "complete": True,
            "run_id": run_id,
        }
        with tempfile.TemporaryDirectory() as tmp:
            _, native_root = self._paths(tmp)
            store = NativeHistoryStore(native_root, run_id)
            store.commit_manifest(manifest)
            failed = _minimal_provenance(run_id)
            failed.update(
                {
                    "status": "failed",
                    "failure_status": "fetch_failed",
                    "error": {"type": "PageFetchError", "message": "retry"},
                }
            )
            with self.assertRaisesRegex(ResumeValidationError, "already sealed"):
                store.commit_retryable_attempt("EURUSD", "M5", failed)
            with self.assertRaisesRegex(ResumeValidationError, "already sealed"):
                store.ensure_resume_contract(
                    {},
                    {},
                    datetime(2026, 1, 10, tzinfo=UTC),
                    symbols=("EURUSD",),
                    timeframes=("M5",),
                    production_authoritative=False,
                )
            with self.assertRaisesRegex(ResumeValidationError, "already sealed"):
                store.commit_leaf(
                    "EURUSD",
                    "M5",
                    _minimal_provenance(run_id),
                    _bar_frame([datetime(2026, 1, 10, tzinfo=UTC)]),
                )
            self.assertEqual(
                {path.name for path in store.run_root.iterdir()},
                {"run_manifest.json"},
            )
            store.close()

    def test_manifest_and_leaf_provenance_refuse_reader_cap_plus_one(self):
        with tempfile.TemporaryDirectory() as tmp:
            _, native_root = self._paths(tmp)
            manifest_store = NativeHistoryStore(native_root, "manifest-reader-cap")
            with patch.object(native_acquisition, "MAX_REUSABLE_PROVENANCE_BYTES", 256):
                with self.assertRaises(OutputStorageResourceLimitError):
                    manifest_store.commit_manifest(
                        {
                            "schema_version": native_acquisition.MANIFEST_SCHEMA_VERSION,
                            "immutable_record": True,
                            "complete": True,
                            "run_id": "manifest-reader-cap",
                            "padding": "x" * 512,
                        }
                    )
            self.assertFalse((manifest_store.run_root / "run_manifest.json").exists())
            self.assertFalse(
                (
                    manifest_store.runs_root
                    / ".manifest-reader-cap.run_manifest.staging"
                ).exists()
            )
            manifest_store.close()

            leaf_store = NativeHistoryStore(native_root, "leaf-reader-cap")
            with patch.object(native_acquisition, "MAX_REUSABLE_PROVENANCE_BYTES", 256):
                with self.assertRaises(OutputStorageResourceLimitError):
                    leaf_store.commit_leaf(
                        "EURUSD",
                        "M5",
                        _minimal_provenance("leaf-reader-cap"),
                        _bar_frame([datetime(2026, 1, 10, tzinfo=UTC)]),
                    )
            self.assertFalse(leaf_store.leaf_path("EURUSD", "M5").exists())
            leaf_store.close()

    def test_plan_mode_is_offline_and_does_not_import_mt5_connector(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            config = root / "config.json"
            config.write_text(json.dumps({"symbols": ["EURUSD"]}), encoding="utf-8")
            output = io.StringIO()
            sentinel = sys.modules.get("pm_mt5", "__absent__")
            sys.modules["pm_mt5"] = None
            try:
                with redirect_stdout(output):
                    result = main(
                        [
                            "--config",
                            str(config),
                            "--output-root",
                            str(root / "data" / "native"),
                            "--as-of",
                            "2026-01-10T12:02:00Z",
                        ]
                    )
            finally:
                if sentinel == "__absent__":
                    sys.modules.pop("pm_mt5", None)
                else:
                    sys.modules["pm_mt5"] = sentinel

            self.assertEqual(result, 0)
            self.assertIn('"mode": "plan_only_no_connection"', output.getvalue())
            self.assertFalse((root / "data" / "native").exists())

    def test_read_only_runtime_snapshot_records_terminal_version_and_broker_identity(
        self,
    ):
        class Connector:
            config = SimpleNamespace(
                server="Configured-Server",
                path="C:/MT5/terminal64.exe",
                expected_login=42,
                expected_server="Expected-Server",
                expected_currency="USD",
            )

            def get_account_info(self):
                return SimpleNamespace(
                    login=42,
                    server="Expected-Server",
                    company="Broker Plc",
                    currency="USD",
                )

        class TerminalAPI:
            @staticmethod
            def terminal_info():
                return SimpleNamespace(
                    name="MetaTrader 5",
                    company="MetaQuotes",
                    build=5432,
                    maxbars=250000,
                    connected=True,
                    path="C:/MT5/terminal64.exe",
                    data_path="C:/MT5/data",
                    commondata_path="C:/MT5/common",
                    trade_allowed=True,
                    tradeapi_disabled=False,
                    dlls_allowed=False,
                )

            @staticmethod
            def version():
                return (500, 5432, "10 Jul 2026")

        snapshot = snapshot_read_only_runtime_metadata(Connector(), TerminalAPI())

        self.assertEqual(snapshot["metadata_status"], "available")
        self.assertEqual(snapshot["account"]["login"], 42)
        self.assertEqual(snapshot["account"]["server"], "Expected-Server")
        self.assertEqual(snapshot["terminal"]["max_bars"], 250000)
        self.assertEqual(snapshot["terminal"]["version"], [500, 5432, "10 Jul 2026"])

    def test_runtime_identity_recheck_uses_injected_terminal_api(self):
        class Connector(_RangeConnector):
            get_runtime_metadata = None
            config = SimpleNamespace(
                history_page_size=50_000,
                server="",
                path="",
                expected_login=42,
                expected_server="Expected-Server",
                expected_currency="USD",
            )

            def get_account_info(self):
                return SimpleNamespace(
                    login=42,
                    server="Expected-Server",
                    company="Broker Plc",
                    currency="USD",
                    leverage=500,
                    trade_allowed=True,
                    trade_expert=True,
                )

        class TerminalAPI:
            @staticmethod
            def terminal_info():
                return SimpleNamespace(
                    name="MetaTrader 5",
                    company="MetaQuotes",
                    build=5432,
                    maxbars=250000,
                    connected=True,
                    path="C:/MT5/terminal64.exe",
                    data_path="C:/MT5/data",
                    commondata_path="C:/MT5/common",
                    trade_allowed=True,
                    tradeapi_disabled=False,
                    dlls_allowed=False,
                )

            @staticmethod
            def version():
                return (500, 5432, "10 Jul 2026")

        as_of = datetime(2026, 1, 10, 12, 2, 17, tzinfo=UTC)
        request = NativeHistoryRequest(
            symbols=("EURUSD",),
            timeframes=DEFAULT_TIMEFRAMES,
            as_of_utc=as_of,
            retries=1,
            retry_sleep_seconds=0.0,
            timeframe_window_years={},
            window_contract_version=TIMEFRAME_EVIDENCE_CONTRACT_VERSION,
            warmup_days=0.0,
            safety_days=0.0,
        )
        with tempfile.TemporaryDirectory() as tmp:
            _, native_root = self._paths(tmp)
            connector = Connector()
            terminal_api = TerminalAPI()
            runtime = snapshot_read_only_runtime_metadata(connector, terminal_api)
            store = NativeHistoryStore(native_root, "runtime-recheck-test")
            try:
                acquirer = NativeHistoryAcquirer(
                    connector,
                    store,
                    request,
                    now_fn=lambda: as_of,
                    runtime_metadata=runtime,
                    terminal_api=terminal_api,
                )
                acquirer._assert_runtime_source_identity("test-boundary")
            finally:
                store.close()

    def test_session_coverage_is_partition_keyed_to_common_calendar_boundaries(self):
        as_of = datetime(2026, 1, 1, tzinfo=UTC)
        windows = build_common_partition_windows(
            as_of,
            {
                "optimizer_train": 5,
                "tuning_validation": 2,
                "final_lockbox": 2,
            },
            embargo_days=7,
        )
        frame = _bar_frame(
            pd.date_range("2016-01-01", "2025-12-31", freq="B", tz="UTC")
        )

        coverage, details = expected_session_coverage_by_partition(frame, windows)

        self.assertEqual(
            set(coverage), {"optimizer_train", "tuning_validation", "final_lockbox"}
        )
        self.assertEqual(details["schedule_source_partition"], "optimizer_train")
        self.assertNotIn("whole_file", details)
        for partition, value in coverage.items():
            self.assertIsNotNone(value, partition)
            self.assertGreaterEqual(value, 0.98, partition)
            self.assertEqual(
                details["partitions"][partition]["start_ts"],
                pd.Timestamp(windows[partition][0]).isoformat(),
            )

    def test_segmented_session_coverage_v2_handles_stable_market_schedules(self):
        windows = _segmented_coverage_windows()

        for market_kind in ("fx", "equity", "24x7", "d1"):
            with self.subTest(market_kind=market_kind):
                frame = _bar_frame(_scheduled_hourly_index(market_kind))
                coverage, details = segmented_session_coverage_by_partition_v2(
                    frame,
                    windows,
                )

                self.assertEqual(details["status"], "estimated")
                self.assertTrue(details["timestamp_only"])
                self.assertFalse(details["performance_inputs_used"])
                self.assertEqual(len(details["schedule_segments"]), 1)
                self.assertEqual(details["change_points"], [])
                for partition, value in coverage.items():
                    self.assertIsNotNone(value, partition)
                    self.assertGreaterEqual(value, 0.99, partition)
                    partition_detail = details["partitions"][partition]
                    self.assertEqual(partition_detail["missing_calendar_weeks"], 0)
                    self.assertIsNotNone(
                        partition_detail["conservative_coverage_lower_bound"]
                    )

    def test_streaming_session_accumulator_is_exact_without_full_bar_arrays(self):
        windows = _segmented_coverage_windows()
        for market_kind in ("fx", "equity", "24x7", "d1"):
            with self.subTest(market_kind=market_kind):
                frame = _bar_frame(_scheduled_hourly_index(market_kind))
                frame["_BAR_CLOSE_TS"] = frame.index + pd.Timedelta(hours=1)
                expected = segmented_session_coverage_by_partition_v2(frame, windows)
                accumulator = native_acquisition._SegmentedSessionCoverageAccumulator(
                    windows
                )
                for start in range(0, len(frame), 997):
                    accumulator.feed(frame.iloc[start : start + 997])
                self.assertEqual(accumulator.finish(), expected)
                self.assertLessEqual(
                    len(accumulator.weekly_counts),
                    53 * native_acquisition.MAX_HORIZON_YEARS,
                )

    def test_segmented_session_coverage_v2_accepts_sustained_schedule_change(self):
        windows = _segmented_coverage_windows()
        old_schedule = _scheduled_hourly_index("fx")
        change = pd.Timestamp("2023-01-02", tz="UTC")
        old_schedule = old_schedule[old_schedule < change]
        new_schedule = _scheduled_hourly_index("fx")
        new_schedule = new_schedule[(new_schedule >= change) & (new_schedule.hour < 8)]
        frame = _bar_frame(old_schedule.append(new_schedule))

        legacy_coverage, _ = expected_session_coverage_by_partition(frame, windows)
        coverage, details = segmented_session_coverage_by_partition_v2(frame, windows)

        self.assertLess(legacy_coverage["final_lockbox"], 0.50)
        self.assertGreaterEqual(coverage["final_lockbox"], 0.99)
        self.assertEqual(len(details["schedule_segments"]), 2)
        self.assertEqual(len(details["change_points"]), 1)
        self.assertEqual(
            details["change_points"][0]["effective_week_start_utc"],
            change.isoformat(),
        )
        self.assertEqual(
            details["schedule_segments"][0]["nominal_bars_per_week_q75"],
            120.0,
        )
        self.assertEqual(
            details["schedule_segments"][1]["nominal_bars_per_week_q75"],
            40.0,
        )

    def test_segmented_session_coverage_v2_keeps_missing_weeks_expected(self):
        windows = _segmented_coverage_windows()
        index = _scheduled_hourly_index("fx")
        missing_start = pd.Timestamp("2024-06-03", tz="UTC")
        missing_end = missing_start + pd.Timedelta(weeks=4)
        frame = _bar_frame(index[(index < missing_start) | (index >= missing_end)])

        coverage, details = segmented_session_coverage_by_partition_v2(frame, windows)

        final = details["partitions"]["final_lockbox"]
        self.assertEqual(final["missing_calendar_weeks"], 4)
        self.assertEqual(final["weeks_below_half_expected_schedule"], 4)
        self.assertLess(coverage["final_lockbox"], 0.98)
        self.assertGreater(coverage["final_lockbox"], 0.90)
        self.assertIsNotNone(final["largest_unexplained_gap"])
        self.assertGreater(final["largest_unexplained_gap"]["seconds"], 0.0)
        self.assertEqual(details["change_points"], [])

    def test_segmented_session_coverage_v2_fails_closed_on_short_history(self):
        start = datetime(2025, 1, 6, tzinfo=UTC)
        windows = {
            "optimizer_train": (start, start + timedelta(weeks=4)),
            "tuning_validation": (
                start + timedelta(weeks=4),
                start + timedelta(weeks=8),
            ),
            "final_lockbox": (
                start + timedelta(weeks=8),
                start + timedelta(weeks=12),
            ),
        }
        frame = _bar_frame(
            pd.date_range(
                start,
                start + timedelta(weeks=12),
                freq="h",
                inclusive="left",
                tz="UTC",
            )
        )

        coverage, details = segmented_session_coverage_by_partition_v2(frame, windows)

        self.assertTrue(all(value is None for value in coverage.values()))
        self.assertEqual(
            details["status"],
            "fewer_than_26_complete_calendar_weeks",
        )

    def test_segmented_session_coverage_v2_never_reads_bar_values(self):
        windows = _segmented_coverage_windows()
        frame = _bar_frame(_scheduled_hourly_index("d1"))
        changed = frame.copy()
        changed["Open"] = range(len(changed))
        changed["High"] = -1.0
        changed["Low"] = float("nan")
        changed["Close"] = float("inf")
        changed["Volume"] = 0
        changed["Spread"] = 999999

        first = segmented_session_coverage_by_partition_v2(frame, windows)
        second = segmented_session_coverage_by_partition_v2(changed, windows)

        self.assertEqual(first, second)

    def test_nonaligned_partition_counts_match_pipeline_full_bar_containment(self):
        from pm_pipeline import OptimizationPipeline

        index = pd.date_range(
            "2019-12-31T00:00:00Z",
            "2026-01-02T00:00:00Z",
            freq="h",
            inclusive="left",
        )
        frame = _bar_frame(index)
        frame["_BAR_CLOSE_TS"] = frame.index + pd.Timedelta(hours=1)
        windows = {
            "optimizer_train": (
                datetime(2020, 1, 1, 0, 7, tzinfo=UTC),
                datetime(2022, 1, 1, 0, 7, tzinfo=UTC),
            ),
            "tuning_validation": (
                datetime(2022, 1, 1, 0, 7, tzinfo=UTC),
                datetime(2024, 1, 1, 0, 7, tzinfo=UTC),
            ),
            "final_lockbox": (
                datetime(2024, 1, 1, 0, 7, tzinfo=UTC),
                datetime(2026, 1, 1, 0, 7, tzinfo=UTC),
            ),
        }

        _coverage, details = segmented_session_coverage_by_partition_v2(
            frame,
            windows,
        )

        for partition, (start, end) in windows.items():
            sliced = OptimizationPipeline._slice_time_window(
                frame,
                pd.Timestamp(start),
                pd.Timestamp(end),
            )
            self.assertEqual(
                details["partitions"][partition]["observed_rows"],
                len(sliced),
            )

    def test_effective_acquisition_start_precedes_train_by_warmup_safety_and_close_slack(
        self,
    ):
        as_of = datetime(2026, 1, 1, tzinfo=UTC)
        with tempfile.TemporaryDirectory() as tmp:
            _, native_root = self._paths(tmp)
            store = NativeHistoryStore(native_root, "evidence-lead-test")
            request = NativeHistoryRequest(
                symbols=("EURUSD",),
                timeframes=("M5", "D1"),
                horizon_years=10,
                as_of_utc=as_of,
                window_years={
                    "optimizer_train": 5,
                    "tuning_validation": 2,
                    "final_lockbox": 2,
                },
                embargo_days=7,
                warmup_days=400,
                safety_days=14,
            )
            acquirer = NativeHistoryAcquirer(
                _RangeConnector(),
                store,
                request,
                now_fn=lambda: as_of,
            )

            train_start = acquirer.partition_windows["optimizer_train"][0]
            required_latest_start = train_start - timedelta(days=415)
            self.assertLessEqual(acquirer.common_start_utc, required_latest_start)
            self.assertEqual(
                acquirer.required_evidence_lead_start_utc,
                required_latest_start,
            )
            store.close()

    def test_timeframe_v5_hash_binds_two_references_and_source_unions(self):
        as_of = datetime(2026, 1, 5, tzinfo=UTC)
        request = NativeHistoryRequest(
            symbols=("EURUSD",),
            timeframes=DEFAULT_TIMEFRAMES,
            as_of_utc=as_of,
            retries=1,
            retry_sleep_seconds=0.0,
            timeframe_window_years={},
            window_contract_version=TIMEFRAME_EVIDENCE_CONTRACT_VERSION,
            warmup_days=0.0,
            safety_days=0.0,
        )
        with tempfile.TemporaryDirectory() as tmp:
            _, native_root = self._paths(tmp)
            connector = _RangeConnector()
            store = NativeHistoryStore(native_root, "timeframe-v5-ranges")
            acquirer = NativeHistoryAcquirer(
                connector,
                store,
                request,
                now_fn=lambda: as_of,
                sleep_fn=lambda _: None,
            )

            m5_windows = acquirer.timeframe_partition_windows["M5"]
            d1_windows = acquirer.timeframe_partition_windows["D1"]
            self.assertLess(
                d1_windows["optimizer_train"][0],
                m5_windows["optimizer_train"][0],
            )
            for partition in ("tuning_validation", "final_lockbox"):
                self.assertEqual(d1_windows[partition], m5_windows[partition])
            m5_source_start = acquirer.timeframe_source_request_intervals["M5"][0]
            d1_source_start = acquirer.timeframe_source_request_intervals["D1"][0]
            self.assertLess(d1_source_start, m5_source_start - timedelta(days=6 * 365))
            self.assertGreater(m5_source_start, datetime(2016, 1, 1, tzinfo=UTC))
            self.assertEqual(
                acquirer.timeframe_audit_reference_map,
                {
                    "M5": ["M15", "M30"],
                    "M15": ["M5", "M30"],
                    "M30": ["M15", "H1"],
                    "H1": ["M30", "H4"],
                    "H4": ["H1", "D1"],
                    "D1": ["H4", "H1"],
                },
            )
            self.assertEqual(
                acquirer.timeframe_audit_reference_map,
                cross_resolution_reference_map(DEFAULT_TIMEFRAMES),
            )
            self.assertEqual(
                acquirer.timeframe_audit_consumer_map,
                {
                    "M5": ["M15"],
                    "M15": ["M5", "M30"],
                    "M30": ["M5", "M15", "H1"],
                    "H1": ["M30", "H4", "D1"],
                    "H4": ["H1", "D1"],
                    "D1": ["H4"],
                },
            )
            self.assertEqual(
                acquirer.timeframe_consensus_policy,
                TWO_REFERENCE_CONSENSUS_POLICY,
            )
            for source in DEFAULT_TIMEFRAMES:
                interval_parts = [
                    acquirer.timeframe_strategy_evidence_intervals[source]
                ]
                for target in acquirer.timeframe_audit_consumer_map[source]:
                    interval_parts.extend(
                        acquirer.timeframe_audit_reference_intervals[target][
                            source
                        ].values()
                    )
                self.assertEqual(
                    acquirer.timeframe_source_request_intervals[source],
                    (
                        min(interval[0] for interval in interval_parts),
                        max(interval[1] for interval in interval_parts),
                    ),
                )

            manifest = acquirer.acquire()
            self.assertTrue(manifest["complete"])
            self.assertEqual(len(manifest["results"]), len(DEFAULT_TIMEFRAMES))
            self.assertEqual(
                sorted(
                    path.name
                    for path in (store.run_root / "EURUSD").iterdir()
                    if path.is_dir()
                ),
                sorted(DEFAULT_TIMEFRAMES),
            )
            m5_sidecar = json.loads(
                (store.run_root / "EURUSD" / "M5" / "provenance.json").read_text(
                    encoding="utf-8"
                )
            )
            h4_sidecar = json.loads(
                (store.run_root / "EURUSD" / "H4" / "provenance.json").read_text(
                    encoding="utf-8"
                )
            )
            d1_sidecar = json.loads(
                (store.run_root / "EURUSD" / "D1" / "provenance.json").read_text(
                    encoding="utf-8"
                )
            )
            for timeframe, sidecar in (
                ("M5", m5_sidecar),
                ("H4", h4_sidecar),
                ("D1", d1_sidecar),
            ):
                sidecar_request = sidecar["request"]
                pagination = sidecar["pagination"]
                self.assertGreaterEqual(
                    pagination["copy_rates_range_attempts_total"],
                    pagination["attempts_total"],
                )
                self.assertEqual(
                    len(pagination["page_attempts"]),
                    pagination["pages_requested"],
                )
                self.assertEqual(
                    sum(
                        page["copy_rates_range_attempts"]
                        for page in pagination["page_attempts"]
                    ),
                    pagination["copy_rates_range_attempts_total"],
                )
                self.assertEqual(
                    sidecar_request["window_contract_version"],
                    TIMEFRAME_EVIDENCE_CONTRACT_VERSION,
                )
                self.assertEqual(sidecar_request["cell_timeframe"], timeframe)
                self.assertEqual(
                    sidecar_request["timeframe_request_map_schema_version"],
                    TIMEFRAME_REQUEST_MAP_SCHEMA_VERSION,
                )
                self.assertEqual(
                    sidecar_request["cell_calendar_boundaries"],
                    sidecar_request["timeframe_calendar_boundaries"][timeframe],
                )
                self.assertEqual(
                    sidecar_request["source_request_interval"]["start_utc"],
                    sidecar_request["native_fetch_start_utc"],
                )
                self.assertEqual(
                    sidecar_request["source_request_interval"]["end_exclusive_utc"],
                    sidecar_request["native_fetch_end_exclusive_utc"],
                )
                self.assertEqual(
                    sidecar["expected_session_coverage_details_by_partition"]["method"],
                    SEGMENTED_SESSION_COVERAGE_METHOD_V2,
                )
                self.assertEqual(
                    sidecar_request["strategy_evidence_interval"],
                    sidecar_request["timeframe_strategy_evidence_intervals"][timeframe],
                )
                self.assertEqual(
                    sidecar_request["audit_reference_timeframes"],
                    sidecar_request["timeframe_audit_reference_map"][timeframe],
                )
                self.assertEqual(
                    sidecar_request["audit_consumer_targets"],
                    sidecar_request["timeframe_audit_consumer_map"][timeframe],
                )
                self.assertEqual(
                    sidecar_request["audit_windows"],
                    sidecar_request["timeframe_audit_windows"][timeframe],
                )
                self.assertEqual(
                    sidecar_request["audit_reference_intervals"],
                    sidecar_request["timeframe_audit_reference_intervals"][timeframe],
                )
                self.assertEqual(
                    sidecar_request["audit_only_intervals"],
                    sidecar_request["timeframe_audit_only_intervals"][timeframe],
                )
                self.assertNotIn("audit_reference_timeframe", sidecar_request)
                self.assertNotIn("audit_only_prefix_interval", sidecar_request)
                self.assertEqual(
                    len(sidecar_request["timeframe_calendar_boundaries_sha256"]),
                    64,
                )
                self.assertEqual(
                    len(sidecar_request["timeframe_request_map_sha256"]),
                    64,
                )

            # H4 is an audit source for the longer D1 evidence window.  The
            # source-union request therefore starts before H4's own strategy
            # interval, and the serialized artifact must retain that audit-only
            # prefix instead of silently filtering it away.
            h4_request = h4_sidecar["request"]
            h4_source_start = pd.Timestamp(
                h4_request["source_request_interval"]["start_utc"]
            )
            h4_strategy_start = pd.Timestamp(
                h4_request["strategy_evidence_interval"]["start_utc"]
            )
            h4_bars = pd.read_csv(store.run_root / "EURUSD" / "H4" / "bars.csv")
            h4_first_open = pd.to_datetime(h4_bars["time"], utc=True).min()
            h4_calls = [call for call in connector.calls if call[1] == "H4"]
            self.assertTrue(h4_calls)
            self.assertEqual(
                min(pd.Timestamp(call[2]) for call in h4_calls),
                h4_source_start,
            )
            self.assertLess(h4_source_start, h4_strategy_start)
            self.assertLess(h4_first_open, h4_strategy_start)
            self.assertEqual(
                h4_first_open,
                h4_source_start + pd.Timedelta(seconds=TIMEFRAME_SECONDS["H4"]),
            )
            self.assertEqual(
                pd.Timestamp(h4_sidecar["coverage"]["actual_first_open_utc"]),
                h4_first_open,
            )
            self.assertEqual(
                m5_sidecar["request"]["timeframe_calendar_boundaries"],
                d1_sidecar["request"]["timeframe_calendar_boundaries"],
            )
            self.assertEqual(
                manifest["request"]["timeframe_calendar_boundaries"],
                m5_sidecar["request"]["timeframe_calendar_boundaries"],
            )
            self.assertEqual(
                manifest["request"]["timeframe_consensus_policy"],
                TWO_REFERENCE_CONSENSUS_POLICY,
            )
            store.close()

    def test_timeframe_v5_nonaligned_edges_transform_each_start_once(self):
        as_of = datetime(2026, 7, 12, 17, 33, 41, tzinfo=UTC)
        request = NativeHistoryRequest(
            symbols=("EURUSD",),
            timeframes=DEFAULT_TIMEFRAMES,
            as_of_utc=as_of,
            retries=1,
            retry_sleep_seconds=0.0,
            timeframe_window_years={},
            window_contract_version=TIMEFRAME_EVIDENCE_CONTRACT_VERSION,
        )
        with tempfile.TemporaryDirectory() as tmp:
            _, native_root = self._paths(tmp)
            store = NativeHistoryStore(native_root, "timeframe-v5-nonaligned")
            acquirer = NativeHistoryAcquirer(
                _RangeConnector(),
                store,
                request,
                now_fn=lambda: as_of,
            )

            for target, references in acquirer.timeframe_audit_reference_map.items():
                target_start = acquirer.timeframe_requested_start_utc[target]
                for reference in references:
                    edge_interval = acquirer.timeframe_audit_reference_intervals[
                        target
                    ][reference]["strategy_full_context"]
                    expected_interval = native_source_interval(
                        reference,
                        target_start,
                        acquirer.as_of_utc,
                    )
                    self.assertEqual(edge_interval, expected_interval)
                    edge_start, edge_end = edge_interval
                    self.assertLessEqual(
                        edge_start,
                        target_start,
                        f"{target}->{reference} omits its containing reference bar",
                    )
                    self.assertLessEqual(
                        acquirer.timeframe_source_request_intervals[reference][0],
                        edge_start,
                    )
                    self.assertGreaterEqual(
                        acquirer.timeframe_source_request_intervals[reference][1],
                        edge_end,
                    )

            self.assertEqual(acquirer.as_of_utc.minute, 30)
            self.assertEqual(
                acquirer.timeframe_required_evidence_lead_start_utc["H4"].minute,
                30,
            )
            h4_target_start = acquirer.timeframe_requested_start_utc["H4"]
            h4_h1_edge_start = acquirer.timeframe_audit_reference_intervals["H4"]["H1"][
                "strategy_full_context"
            ][0]
            self.assertEqual(
                h4_h1_edge_start,
                pd.Timestamp(h4_target_start).floor("h").to_pydatetime(),
            )
            self.assertGreater(
                h4_h1_edge_start,
                acquirer.timeframe_strategy_evidence_intervals["H4"][0],
            )
            d1_target_start = acquirer.timeframe_requested_start_utc["D1"]
            d1_h4_edge_start = acquirer.timeframe_audit_reference_intervals["D1"]["H4"][
                "strategy_full_context"
            ][0]
            self.assertEqual(d1_h4_edge_start, d1_target_start - timedelta(hours=4))
            self.assertGreater(
                d1_h4_edge_start,
                acquirer.timeframe_strategy_evidence_intervals["D1"][0],
            )
            resume_contract = acquirer._common_request_contract()
            self.assertEqual(
                resume_contract["timeframe_request_map_schema_version"],
                TIMEFRAME_REQUEST_MAP_SCHEMA_VERSION,
            )
            self.assertIn("timeframe_strategy_evidence_intervals", resume_contract)
            self.assertIn("timeframe_audit_reference_map", resume_contract)
            self.assertIn("timeframe_audit_consumer_map", resume_contract)
            self.assertIn("timeframe_audit_windows", resume_contract)
            self.assertIn("timeframe_audit_reference_intervals", resume_contract)
            self.assertIn("timeframe_audit_only_intervals", resume_contract)
            self.assertIn("timeframe_consensus_policy", resume_contract)
            store.close()

    def test_native_interval_transform_handles_nonaligned_start_and_end(self):
        start = datetime(2026, 7, 12, 21, 7, 13, tzinfo=UTC)
        end = datetime(2026, 7, 12, 21, 47, 19, tzinfo=UTC)

        self.assertEqual(
            native_source_interval("M30", start, end),
            (
                datetime(2026, 7, 12, 21, 0, tzinfo=UTC),
                datetime(2026, 7, 12, 21, 30, tzinfo=UTC),
            ),
        )
        self.assertEqual(
            native_source_interval("H4", start, end),
            (
                datetime(2026, 7, 12, 17, 7, 13, tzinfo=UTC),
                datetime(2026, 7, 12, 21, 47, 20, tzinfo=UTC),
            ),
        )

    def test_timeframe_v5_contract_is_canonical_under_reordered_roster(self):
        as_of = datetime(2026, 7, 12, 17, 33, 41, tzinfo=UTC)
        with tempfile.TemporaryDirectory() as tmp:
            _, native_root = self._paths(tmp)
            stores = (
                NativeHistoryStore(native_root, "v5-canonical-order"),
                NativeHistoryStore(native_root, "v5-reversed-order"),
            )
            try:
                contracts = []
                for store, timeframes in zip(
                    stores,
                    (DEFAULT_TIMEFRAMES, tuple(reversed(DEFAULT_TIMEFRAMES))),
                    strict=True,
                ):
                    acquirer = NativeHistoryAcquirer(
                        _RangeConnector(),
                        store,
                        NativeHistoryRequest(
                            symbols=("EURUSD",),
                            timeframes=timeframes,
                            as_of_utc=as_of,
                            timeframe_window_years={},
                            window_contract_version=(
                                TIMEFRAME_EVIDENCE_CONTRACT_VERSION
                            ),
                        ),
                        now_fn=lambda: as_of,
                    )
                    contracts.append(acquirer._common_request_contract())
                self.assertEqual(contracts[0], contracts[1])
                self.assertEqual(
                    contracts[0]["timeframe_request_map_sha256"],
                    contracts[1]["timeframe_request_map_sha256"],
                )
            finally:
                for store in stores:
                    store.close()

    def test_legacy_flat_window_request_remains_reproducible(self):
        as_of = datetime(2026, 1, 5, tzinfo=UTC)
        request_values = {
            "symbols": ("EURUSD",),
            "timeframes": ("M5", "D1"),
            "horizon_years": 10,
            "as_of_utc": as_of,
            "window_years": {
                "optimizer_train": 5,
                "tuning_validation": 2,
                "final_lockbox": 2,
            },
            "embargo_days": 7,
            "warmup_days": 400,
            "safety_days": 14,
        }
        with tempfile.TemporaryDirectory() as tmp:
            _, native_root = self._paths(tmp)
            implicit_store = NativeHistoryStore(native_root, "legacy-implicit")
            implicit = NativeHistoryAcquirer(
                _RangeConnector(),
                implicit_store,
                NativeHistoryRequest(**request_values),
                now_fn=lambda: as_of,
            )
            explicit_store = NativeHistoryStore(native_root, "legacy-explicit")
            explicit = NativeHistoryAcquirer(
                _RangeConnector(),
                explicit_store,
                NativeHistoryRequest(
                    **request_values,
                    window_contract_version=LEGACY_FLAT_WINDOW_CONTRACT_VERSION,
                ),
                now_fn=lambda: as_of,
            )

            self.assertEqual(
                implicit._common_request_contract(),
                explicit._common_request_contract(),
            )
            self.assertNotIn(
                "window_contract_version",
                implicit._common_request_contract(),
            )
            self.assertEqual(
                implicit.timeframe_partition_windows["M5"],
                implicit.timeframe_partition_windows["D1"],
            )
            self.assertEqual(
                implicit.timeframe_requested_start_utc["M5"],
                implicit.timeframe_requested_start_utc["D1"],
            )
            implicit_store.close()
            explicit_store.close()

    def test_v5_resume_hash_binds_graph_edges_consumers_windows_and_policy(self):
        as_of = datetime(2026, 1, 5, tzinfo=UTC)
        request = NativeHistoryRequest(
            symbols=("EURUSD",),
            timeframes=DEFAULT_TIMEFRAMES,
            as_of_utc=as_of,
            retries=1,
            retry_sleep_seconds=0.0,
            timeframe_window_years={},
            window_contract_version=TIMEFRAME_EVIDENCE_CONTRACT_VERSION,
            warmup_days=0.0,
            safety_days=0.0,
        )
        with tempfile.TemporaryDirectory() as tmp:
            _, native_root = self._paths(tmp)
            store = NativeHistoryStore(native_root, "v5-hash-resume")
            NativeHistoryAcquirer(
                _RangeConnector(),
                store,
                request,
                now_fn=lambda: as_of,
                sleep_fn=lambda _: None,
            ).acquire(max_new_cells=1)
            store.close()

            recovered = load_native_history_resume_contract(
                native_root,
                "v5-hash-resume",
            )
            self.assertEqual(
                recovered.request["window_contract_version"],
                TIMEFRAME_EVIDENCE_CONTRACT_VERSION,
            )
            resumed_store = NativeHistoryStore(
                native_root,
                "v5-hash-resume",
                resume=True,
            )
            try:
                resumed = NativeHistoryAcquirer(
                    _RangeConnector(),
                    resumed_store,
                    request,
                    now_fn=lambda: as_of,
                    sleep_fn=lambda _: None,
                )
                self.assertEqual(
                    resumed._common_request_contract(),
                    dict(recovered.request),
                )
            finally:
                resumed_store.close()

            sidecar_path = store.run_root / "EURUSD" / "M5" / "provenance.json"
            original = json.loads(sidecar_path.read_text(encoding="utf-8"))
            mutations = (
                lambda value: value["request"]["timeframe_audit_reference_map"][
                    "M5"
                ].reverse(),
                lambda value: value["request"]["timeframe_audit_consumer_map"][
                    "M15"
                ].pop(),
                lambda value: value["request"]["timeframe_audit_windows"]["H4"][
                    "optimizer_train"
                ].update({"start_utc": "2010-01-01T00:00:00Z"}),
                lambda value: value["request"]["timeframe_audit_reference_intervals"][
                    "D1"
                ]["H4"]["strategy_full_context"].update(
                    {"start_utc": "2010-01-01T00:00:00Z"}
                ),
                lambda value: value["request"]["timeframe_consensus_policy"].update(
                    {"activation_rule": "both_edges_pass"}
                ),
            )
            for mutate in mutations:
                with self.subTest(mutate=mutate):
                    sidecar = json.loads(json.dumps(original))
                    mutate(sidecar)
                    sidecar_path.write_text(
                        json.dumps(sidecar, indent=2, sort_keys=True),
                        encoding="utf-8",
                    )
                    with self.assertRaisesRegex(
                        ResumeValidationError,
                        "request map hash mismatch",
                    ):
                        load_native_history_resume_contract(
                            native_root,
                            "v5-hash-resume",
                        )
            sidecar_path.write_text(
                json.dumps(original, indent=2, sort_keys=True),
                encoding="utf-8",
            )

    def test_v2_seed_rejects_forensic_request_downgrade(self):
        as_of = datetime(2026, 1, 5, tzinfo=UTC)
        request = NativeHistoryRequest(
            symbols=("EURUSD",),
            timeframes=DEFAULT_TIMEFRAMES,
            as_of_utc=as_of,
            retries=1,
            retry_sleep_seconds=0.0,
            timeframe_window_years={},
            window_contract_version=TIMEFRAME_EVIDENCE_CONTRACT_VERSION,
            warmup_days=0.0,
            safety_days=0.0,
        )
        with tempfile.TemporaryDirectory() as tmp:
            _, native_root = self._paths(tmp)
            store = NativeHistoryStore(native_root, "v4-forensic-resume")
            NativeHistoryAcquirer(
                _RangeConnector(),
                store,
                request,
                now_fn=lambda: as_of,
                sleep_fn=lambda _: None,
            ).acquire(max_new_cells=1)
            sidecar_path = store.run_root / "EURUSD" / "M5" / "provenance.json"
            store.close()

            sidecar = json.loads(sidecar_path.read_text(encoding="utf-8"))
            sidecar_request = sidecar["request"]
            v4_graph = {
                "M5": "M15",
                "M15": "M5",
                "M30": "M15",
                "H1": "M30",
                "H4": "H1",
                "D1": "H4",
            }
            v4_prefixes = {}
            for timeframe in DEFAULT_TIMEFRAMES:
                source_interval = sidecar_request["timeframe_source_request_intervals"][
                    timeframe
                ]
                strategy_interval = sidecar_request[
                    "timeframe_strategy_evidence_intervals"
                ][timeframe]
                v4_prefixes[timeframe] = (
                    {
                        "start_utc": source_interval["start_utc"],
                        "end_exclusive_utc": strategy_interval["start_utc"],
                        "role": "audit_only_reference",
                    }
                    if source_interval["start_utc"] < strategy_interval["start_utc"]
                    else None
                )
            for field in (
                "timeframe_audit_consumer_map",
                "timeframe_audit_windows",
                "timeframe_audit_reference_intervals",
                "timeframe_audit_only_intervals",
                "timeframe_consensus_policy",
                "audit_reference_timeframes",
                "audit_consumer_targets",
                "audit_windows",
                "audit_reference_intervals",
                "audit_only_intervals",
            ):
                sidecar_request.pop(field, None)
            sidecar_request.update(
                {
                    "timeframe_request_map_schema_version": (
                        FORENSIC_TIMEFRAME_REQUEST_MAP_SCHEMA_VERSION
                    ),
                    "timeframe_audit_reference_map": v4_graph,
                    "timeframe_audit_only_prefix_intervals": v4_prefixes,
                    "audit_reference_timeframe": v4_graph["M5"],
                    "audit_only_prefix_interval": v4_prefixes["M5"],
                }
            )
            sidecar_request["timeframe_request_map_sha256"] = _canonical_json_sha256(
                _timeframe_request_hash_payload(sidecar_request)
            )
            sidecar_path.write_text(
                json.dumps(sidecar, indent=2, sort_keys=True),
                encoding="utf-8",
            )

            with self.assertRaisesRegex(
                ResumeValidationError,
                "request contract differs across leaves",
            ):
                load_native_history_resume_contract(
                    native_root,
                    "v4-forensic-resume",
                )

    def test_cli_defaults_to_v5_six_timeframes_and_extracts_nested_config(self):
        with tempfile.TemporaryDirectory() as tmp:
            root, native_root = self._paths(tmp)
            config = root / "config.json"
            config.write_text(
                json.dumps(
                    {
                        "symbols": ["EURUSD"],
                        "pipeline": {
                            "evidence_timeframe_window_years": {
                                "D1": {"optimizer_train": 15}
                            }
                        },
                    }
                ),
                encoding="utf-8",
            )
            stdout = io.StringIO()
            with redirect_stdout(stdout):
                result = main(
                    [
                        "--config",
                        str(config),
                        "--output-root",
                        str(native_root),
                        "--as-of",
                        "2026-01-05T00:00:00Z",
                    ]
                )
            self.assertEqual(result, 0)
            plan = json.loads(stdout.getvalue())
            self.assertEqual(
                plan["window_contract_version"],
                TIMEFRAME_EVIDENCE_CONTRACT_VERSION,
            )
            self.assertEqual(plan["timeframes"], list(DEFAULT_TIMEFRAMES))
            self.assertEqual(
                plan["timeframe_window_years"]["M5"]["optimizer_train"],
                5,
            )
            self.assertEqual(
                plan["timeframe_window_years"]["D1"]["optimizer_train"],
                15,
            )
            self.assertEqual(
                plan["timeframe_window_years"]["M5"]["final_lockbox"],
                plan["timeframe_window_years"]["D1"]["final_lockbox"],
            )

            stdout = io.StringIO()
            with redirect_stdout(stdout):
                result = main(
                    [
                        "--config",
                        str(config),
                        "--output-root",
                        str(native_root),
                        "--timeframes",
                        "M5",
                        "D1",
                        "--as-of",
                        "2026-01-05T00:00:00Z",
                        "--train-years",
                        "5",
                    ]
                )
            self.assertEqual(result, 0)
            legacy_plan = json.loads(stdout.getvalue())
            self.assertEqual(
                legacy_plan["window_contract_version"],
                LEGACY_FLAT_WINDOW_CONTRACT_VERSION,
            )
            self.assertIsNone(legacy_plan["timeframe_window_years"])

    def test_v5_request_refuses_a_partial_reference_roster(self):
        with self.assertRaisesRegex(ValueError, "exact six-timeframe roster"):
            NativeHistoryRequest(
                symbols=("EURUSD",),
                timeframes=("M5", "M15"),
                timeframe_window_years={},
                window_contract_version=TIMEFRAME_EVIDENCE_CONTRACT_VERSION,
            )

    def test_bounded_run_resumes_without_overwriting_valid_leaf(self):
        as_of = datetime(2026, 1, 10, 12, 2, 17, tzinfo=UTC)
        request = NativeHistoryRequest(
            symbols=("EURUSD",),
            timeframes=("M5", "M15"),
            horizon_years=1,
            as_of_utc=as_of,
            retries=1,
            retry_sleep_seconds=0.0,
            enforce_evidence_horizon=False,
        )
        with tempfile.TemporaryDirectory() as tmp:
            _, native_root = self._paths(tmp)
            first_connector = _RangeConnector()
            first_store = NativeHistoryStore(native_root, "bounded-resume-test")
            progress = NativeHistoryAcquirer(
                first_connector,
                first_store,
                request,
                now_fn=lambda: as_of,
            ).acquire(max_new_cells=1)
            self.assertFalse(progress["complete"])
            self.assertEqual(progress["bounded_stop_reason"], "max_new_cells")
            self.assertFalse((first_store.run_root / "run_manifest.json").exists())
            original_csv = first_store.run_root / "EURUSD" / "M5" / "bars.csv"
            original_bytes = original_csv.read_bytes()
            original_mtime = original_csv.stat().st_mtime_ns

            contract = load_native_history_resume_contract(
                native_root, "bounded-resume-test"
            )
            self.assertEqual(
                contract.request["requested_information_cutoff_utc"],
                "2026-01-10T12:02:17Z",
            )
            second_connector = _RangeConnector()
            resumed_store = NativeHistoryStore(
                native_root,
                "bounded-resume-test",
                resume=True,
            )
            manifest = NativeHistoryAcquirer(
                second_connector,
                resumed_store,
                request,
                now_fn=lambda: as_of,
            ).acquire()

            self.assertTrue(manifest["complete"])
            self.assertEqual(manifest["resume"]["reused_cells"], 1)
            self.assertEqual(manifest["resume"]["newly_acquired_cells"], 1)
            self.assertEqual({call[1] for call in second_connector.calls}, {"M15"})
            self.assertEqual(original_csv.read_bytes(), original_bytes)
            self.assertEqual(original_csv.stat().st_mtime_ns, original_mtime)

    def test_v5_resume_rejects_forged_coverage_before_broker_fetch(self):
        as_of = datetime(2026, 1, 10, 12, 2, 17, tzinfo=UTC)
        request = NativeHistoryRequest(
            symbols=("EURUSD",),
            timeframes=DEFAULT_TIMEFRAMES,
            as_of_utc=as_of,
            retries=1,
            retry_sleep_seconds=0.0,
            timeframe_window_years={},
            window_contract_version=TIMEFRAME_EVIDENCE_CONTRACT_VERSION,
            warmup_days=0.0,
            safety_days=0.0,
        )
        with tempfile.TemporaryDirectory() as tmp:
            _, native_root = self._paths(tmp)
            NativeHistoryAcquirer(
                _RangeConnector(),
                NativeHistoryStore(native_root, "forged-coverage-resume"),
                request,
                now_fn=lambda: as_of,
            ).acquire(max_new_cells=1)
            sidecar = (
                native_root
                / "runs"
                / "forged-coverage-resume"
                / "EURUSD"
                / "M5"
                / "provenance.json"
            )
            value = json.loads(sidecar.read_text(encoding="utf-8"))
            value["coverage"]["status"] = "FORGED_ACCEPTED"
            sidecar.write_text(
                json.dumps(value, indent=2, sort_keys=True),
                encoding="utf-8",
            )

            connector = _RangeConnector()
            resumed_store = NativeHistoryStore(
                native_root,
                "forged-coverage-resume",
                resume=True,
            )
            with self.assertRaisesRegex(
                ResumeValidationError,
                "COVERAGE_STATUS_INVALID",
            ):
                NativeHistoryAcquirer(
                    connector,
                    resumed_store,
                    request,
                    now_fn=lambda: as_of,
                ).acquire()
            self.assertEqual(connector.calls, [])

    def test_v5_resume_rejects_hidden_hard_or_contract_page_failure(self):
        as_of = datetime(2026, 1, 10, 12, 2, 17, tzinfo=UTC)
        request = NativeHistoryRequest(
            symbols=("EURUSD",),
            timeframes=DEFAULT_TIMEFRAMES,
            as_of_utc=as_of,
            retries=1,
            retry_sleep_seconds=0.0,
            timeframe_window_years={},
            window_contract_version=TIMEFRAME_EVIDENCE_CONTRACT_VERSION,
            warmup_days=0.0,
            safety_days=0.0,
        )
        with tempfile.TemporaryDirectory() as tmp:
            _, native_root = self._paths(tmp)
            NativeHistoryAcquirer(
                _RangeConnector(),
                NativeHistoryStore(native_root, "forged-page-resume"),
                request,
                now_fn=lambda: as_of,
            ).acquire(max_new_cells=1)
            sidecar = (
                native_root
                / "runs"
                / "forged-page-resume"
                / "EURUSD"
                / "M5"
                / "provenance.json"
            )
            original = json.loads(sidecar.read_text(encoding="utf-8"))

            def hidden_hard_failure(value):
                page = value["pagination"]["page_attempts"][0]
                diagnostics = page["connector_diagnostics"]
                diagnostics["hard_failure"] = True
                diagnostics["hard_failure_call"] = "copy_rates_range_initial"
                diagnostics["sync_status"] = "hard_mt5_failure"
                value["pagination"]["empty_pages"].append(json.loads(json.dumps(page)))
                value["pagination"]["pages_with_data"] -= 1

            def contract_error(value):
                value["pagination"]["page_attempts"][0]["connector_diagnostics"][
                    "sync_status"
                ] = "contract_error"

            for mutate in (hidden_hard_failure, contract_error):
                with self.subTest(mutation=mutate.__name__):
                    forged = json.loads(json.dumps(original))
                    mutate(forged)
                    sidecar.write_text(
                        json.dumps(forged, indent=2, sort_keys=True) + "\n",
                        encoding="utf-8",
                    )
                    connector = _RangeConnector()
                    resumed = NativeHistoryStore(
                        native_root,
                        "forged-page-resume",
                        resume=True,
                    )
                    with self.assertRaisesRegex(
                        ResumeValidationError,
                        "V5_PAGE_MT5_V4_DIAGNOSTICS_INVALID",
                    ):
                        NativeHistoryAcquirer(
                            connector,
                            resumed,
                            request,
                            now_fn=lambda: as_of,
                        ).acquire()
                    self.assertEqual(connector.calls, [])
                    resumed.close()

    def test_resume_rejects_duplicate_provenance_keys_before_fetch(self):
        as_of = datetime(2026, 1, 10, 12, 2, 17, tzinfo=UTC)
        request = NativeHistoryRequest(
            symbols=("EURUSD",),
            timeframes=DEFAULT_TIMEFRAMES,
            as_of_utc=as_of,
            retries=1,
            retry_sleep_seconds=0.0,
            timeframe_window_years={},
            window_contract_version=TIMEFRAME_EVIDENCE_CONTRACT_VERSION,
            warmup_days=0.0,
            safety_days=0.0,
        )
        with tempfile.TemporaryDirectory() as tmp:
            _, native_root = self._paths(tmp)
            NativeHistoryAcquirer(
                _RangeConnector(),
                NativeHistoryStore(native_root, "duplicate-json-resume"),
                request,
                now_fn=lambda: as_of,
            ).acquire(max_new_cells=1)
            sidecar = (
                native_root
                / "runs"
                / "duplicate-json-resume"
                / "EURUSD"
                / "M5"
                / "provenance.json"
            )
            original = sidecar.read_text(encoding="utf-8")
            mutations = (
                original.replace(
                    '"status": "success"',
                    '"status": "success",\n  "status": "success"',
                    1,
                ),
                original.replace(
                    '"coverage": {',
                    '"coverage": {},\n  "coverage": {',
                    1,
                ),
            )
            for forged in mutations:
                with self.subTest(forged=forged[:80]):
                    sidecar.write_text(forged, encoding="utf-8")
                    with self.assertRaisesRegex(
                        ResumeValidationError,
                        "duplicate JSON object key",
                    ):
                        NativeHistoryStore(
                            native_root,
                            "duplicate-json-resume",
                            resume=True,
                        )
            sidecar.write_text(original, encoding="utf-8")

    def test_failed_attempt_is_reacquired_on_resume_and_never_becomes_a_leaf(self):
        as_of = datetime(2026, 1, 10, 12, 2, tzinfo=UTC)
        resume_now = as_of + timedelta(hours=1)

        class AlwaysEmptyConnector(_RangeConnector):
            def get_bars_range(
                self, symbol, timeframe, start_date, end_date, sync_retry=False
            ):
                self.calls.append((symbol, timeframe, start_date, end_date, sync_retry))
                self._last_error = (1, "Success")
                self.diagnostics = _fixture_mt5_v4_empty_diagnostics(
                    symbol=symbol,
                    timeframe=timeframe,
                    start=start_date,
                    end=end_date,
                )
                return None

        request = NativeHistoryRequest(
            symbols=("EURUSD",),
            timeframes=DEFAULT_TIMEFRAMES,
            as_of_utc=as_of,
            retries=1,
            retry_sleep_seconds=0.0,
            timeframe_window_years={},
            window_contract_version=TIMEFRAME_EVIDENCE_CONTRACT_VERSION,
            warmup_days=0.0,
            safety_days=0.0,
        )
        with tempfile.TemporaryDirectory() as tmp:
            _, native_root = self._paths(tmp)
            first_connector = AlwaysEmptyConnector()
            first = NativeHistoryAcquirer(
                first_connector,
                NativeHistoryStore(native_root, "failed-bounded-resume"),
                request,
                now_fn=lambda: as_of,
            ).acquire(max_new_cells=1)
            self.assertFalse(first["complete"])
            self.assertEqual(first["results"][0]["status"], "failed")
            self.assertEqual(
                first["results"][0]["failure_status"],
                "terminal_history_empty_unresolved",
            )
            resume_seed = json.loads(
                (
                    native_root
                    / "runs"
                    / "failed-bounded-resume"
                    / "resume_contract.json"
                ).read_text(encoding="utf-8")
            )
            first_run_started = resume_seed["acquisition_started_utc"]
            self.assertEqual(
                resume_seed["source_identity"],
                canonical_source_identity(first_connector.get_runtime_metadata()),
            )
            self.assertFalse(
                (
                    native_root / "runs" / "failed-bounded-resume" / "EURUSD" / "M5"
                ).exists()
            )

            resumed_connector = _RangeConnector()
            manifest = NativeHistoryAcquirer(
                resumed_connector,
                NativeHistoryStore(
                    native_root,
                    "failed-bounded-resume",
                    resume=True,
                ),
                request,
                now_fn=lambda: resume_now,
            ).acquire()
            self.assertTrue(manifest["complete"])
            self.assertEqual(manifest["resume"]["reused_cells"], 0)
            self.assertEqual(manifest["resume"]["newly_acquired_cells"], 6)
            self.assertEqual(manifest["counts"]["failed"], 0)
            self.assertEqual(
                manifest["acquisition_started_utc"],
                first_run_started,
            )
            self.assertTrue(any(call[1] == "M5" for call in resumed_connector.calls))

    def test_resume_corruption_fails_before_any_fetch_or_new_leaf(self):
        as_of = datetime(2026, 1, 10, 12, 2, 17, tzinfo=UTC)
        request = NativeHistoryRequest(
            symbols=("EURUSD",),
            timeframes=("M5", "M15"),
            horizon_years=1,
            as_of_utc=as_of,
            retries=1,
            retry_sleep_seconds=0.0,
            enforce_evidence_horizon=False,
        )
        with tempfile.TemporaryDirectory() as tmp:
            _, native_root = self._paths(tmp)
            store = NativeHistoryStore(native_root, "corrupt-resume-test")
            NativeHistoryAcquirer(
                _RangeConnector(), store, request, now_fn=lambda: as_of
            ).acquire(max_new_cells=1)
            csv_path = store.run_root / "EURUSD" / "M5" / "bars.csv"
            with csv_path.open("ab") as handle:
                handle.write(b"corruption\n")

            connector = _RangeConnector()
            resumed_store = NativeHistoryStore(
                native_root, "corrupt-resume-test", resume=True
            )
            acquirer = NativeHistoryAcquirer(
                connector, resumed_store, request, now_fn=lambda: as_of
            )
            with self.assertRaises(ResumeValidationError):
                acquirer.acquire()
            self.assertEqual(connector.calls, [])
            self.assertFalse((store.run_root / "EURUSD" / "M15").exists())
            self.assertFalse((store.run_root / "run_manifest.json").exists())

    def test_resume_rejects_cutoff_and_source_identity_mismatch(self):
        as_of = datetime(2026, 1, 10, 12, 2, 17, tzinfo=UTC)
        request = NativeHistoryRequest(
            symbols=("EURUSD",),
            timeframes=("M5", "M15"),
            horizon_years=1,
            as_of_utc=as_of,
            retries=1,
            retry_sleep_seconds=0.0,
            enforce_evidence_horizon=False,
        )
        with tempfile.TemporaryDirectory() as tmp:
            _, native_root = self._paths(tmp)
            store = NativeHistoryStore(native_root, "contract-resume-test")
            NativeHistoryAcquirer(
                _RangeConnector(), store, request, now_fn=lambda: as_of
            ).acquire(max_new_cells=1)

            resumed_store = NativeHistoryStore(
                native_root, "contract-resume-test", resume=True
            )
            changed_cutoff = NativeHistoryRequest(
                symbols=("EURUSD",),
                timeframes=("M5", "M15"),
                horizon_years=1,
                as_of_utc=as_of + timedelta(seconds=1),
                retries=1,
                enforce_evidence_horizon=False,
            )
            with self.assertRaises(ResumeValidationError):
                NativeHistoryAcquirer(
                    _RangeConnector(),
                    resumed_store,
                    changed_cutoff,
                    now_fn=lambda: as_of,
                )

            changed_source = _RangeConnector()
            metadata = changed_source.get_runtime_metadata()
            metadata["account"]["login"] = 999999
            with self.assertRaises(ResumeValidationError):
                NativeHistoryAcquirer(
                    changed_source,
                    resumed_store,
                    request,
                    now_fn=lambda: as_of,
                    runtime_metadata=metadata,
                )
            resumed_store.close()

    def test_source_identity_drift_after_fetch_never_commits_leaf(self):
        as_of = datetime(2026, 1, 10, 12, 2, 17, tzinfo=UTC)
        request = NativeHistoryRequest(
            symbols=("EURUSD",),
            timeframes=DEFAULT_TIMEFRAMES,
            as_of_utc=as_of,
            retries=1,
            retry_sleep_seconds=0.0,
            timeframe_window_years={},
            window_contract_version=TIMEFRAME_EVIDENCE_CONTRACT_VERSION,
            warmup_days=0.0,
            safety_days=0.0,
        )
        with tempfile.TemporaryDirectory() as tmp:
            _, native_root = self._paths(tmp)
            connector = _DriftingRangeConnector()
            store = NativeHistoryStore(native_root, "identity-drift-test")
            with self.assertRaisesRegex(
                ResumeValidationError,
                "source identity changed at after-fetch:EURUSD:M5",
            ):
                NativeHistoryAcquirer(
                    connector,
                    store,
                    request,
                    now_fn=lambda: as_of,
                ).acquire(max_new_cells=1)
            run_root = native_root / "runs" / "identity-drift-test"
            self.assertGreater(len(connector.calls), 0)
            self.assertFalse((run_root / "EURUSD" / "M5").exists())
            self.assertFalse((run_root / "run_manifest.json").exists())
            self.assertFalse(list(run_root.rglob("*.staging")))

    def test_resume_plan_uses_original_cutoff_when_as_of_is_omitted(self):
        as_of = datetime(2026, 1, 10, 12, 2, 17, tzinfo=UTC)
        request = NativeHistoryRequest(
            symbols=("EURUSD",),
            timeframes=("M5", "M15"),
            horizon_years=1,
            as_of_utc=as_of,
            retries=1,
            enforce_evidence_horizon=False,
        )
        with tempfile.TemporaryDirectory() as tmp:
            root, native_root = self._paths(tmp)
            config = root / "config.json"
            config.write_text(json.dumps({"symbols": ["EURUSD"]}), encoding="utf-8")
            store = NativeHistoryStore(native_root, "cli-resume-test")
            NativeHistoryAcquirer(
                _RangeConnector(), store, request, now_fn=lambda: as_of
            ).acquire(max_new_cells=1)
            output = io.StringIO()
            with redirect_stdout(output):
                result = main(
                    [
                        "--config",
                        str(config),
                        "--output-root",
                        str(native_root),
                        "--run-id",
                        "cli-resume-test",
                        "--resume",
                        "--timeframes",
                        "M5",
                        "M15",
                    ]
                )
            self.assertEqual(result, 0)
            self.assertIn('"as_of_utc": "2026-01-10T12:02:17Z"', output.getvalue())

    def test_resume_refuses_completed_manifest_and_cleans_local_crash_staging(self):
        as_of = datetime(2026, 1, 10, 12, 2, 17, tzinfo=UTC)
        request = NativeHistoryRequest(
            symbols=("EURUSD",),
            timeframes=("M5", "M15"),
            horizon_years=1,
            as_of_utc=as_of,
            retries=1,
            enforce_evidence_horizon=False,
        )
        with tempfile.TemporaryDirectory() as tmp:
            _, native_root = self._paths(tmp)
            store = NativeHistoryStore(native_root, "staging-resume-test")
            NativeHistoryAcquirer(
                _RangeConnector(), store, request, now_fn=lambda: as_of
            ).acquire(max_new_cells=1)
            inside = store.run_root / "EURUSD" / ".M15.dead.staging"
            inside.mkdir()
            (inside / "partial.csv").write_text("partial", encoding="utf-8")
            outside = store.run_root.parent / ".outside.staging"
            outside.mkdir()
            resumed = NativeHistoryStore(
                native_root, "staging-resume-test", resume=True
            )
            self.assertEqual(resumed.stale_staging_removed, 1)
            self.assertFalse(inside.exists())
            self.assertTrue(outside.exists())
            resumed.close()

            completed_store = NativeHistoryStore(native_root, "completed-resume-test")
            complete_request = NativeHistoryRequest(
                symbols=("EURUSD",),
                timeframes=("M5",),
                horizon_years=1,
                as_of_utc=as_of,
                retries=1,
                enforce_evidence_horizon=False,
            )
            NativeHistoryAcquirer(
                _RangeConnector(),
                completed_store,
                complete_request,
                now_fn=lambda: as_of,
            ).acquire()
            with self.assertRaises(ResumeValidationError):
                NativeHistoryStore(native_root, "completed-resume-test", resume=True)

    def test_resume_reconciles_leaf_published_before_attempt_cleanup(self):
        as_of = datetime(2026, 1, 10, 12, 2, 17, tzinfo=UTC)
        request = NativeHistoryRequest(
            symbols=("EURUSD",),
            timeframes=("M5", "M15"),
            horizon_years=1,
            as_of_utc=as_of,
            retries=1,
            enforce_evidence_horizon=False,
        )
        with tempfile.TemporaryDirectory() as tmp:
            _, native_root = self._paths(tmp)
            store = NativeHistoryStore(native_root, "leaf-attempt-crash")
            first = NativeHistoryAcquirer(
                _RangeConnector(), store, request, now_fn=lambda: as_of
            ).acquire(max_new_cells=1)
            self.assertFalse(first["complete"])
            crash_store = NativeHistoryStore(
                native_root,
                "leaf-attempt-crash",
                resume=True,
            )
            failed = _minimal_provenance("leaf-attempt-crash")
            failed.update(
                {
                    "status": "failed",
                    "failure_status": "write_failed",
                    "error": {"type": "OSError", "message": "crash residue"},
                }
            )
            crash_store.commit_retryable_attempt("EURUSD", "M5", failed)
            self.assertTrue(
                (crash_store.run_root / "_attempts" / "EURUSD" / "M5").is_dir()
            )
            crash_store.close()

            resumed = NativeHistoryStore(
                native_root,
                "leaf-attempt-crash",
                resume=True,
            )
            result = NativeHistoryAcquirer(
                _RangeConnector(), resumed, request, now_fn=lambda: as_of
            ).acquire()
            self.assertTrue(result["complete"])
            self.assertFalse((resumed.run_root / "_attempts").exists())
            resumed.close()

    def test_resume_seed_is_bounded_and_loaded_before_any_topology_walk(self):
        as_of = datetime(2026, 1, 10, 12, 2, 17, tzinfo=UTC)
        request = NativeHistoryRequest(
            symbols=("EURUSD",),
            timeframes=("M5", "M15"),
            horizon_years=1,
            as_of_utc=as_of,
            retries=1,
            enforce_evidence_horizon=False,
        )
        with tempfile.TemporaryDirectory() as tmp:
            _, native_root = self._paths(tmp)
            store = NativeHistoryStore(native_root, "oversized-seed-first")
            NativeHistoryAcquirer(
                _RangeConnector(), store, request, now_fn=lambda: as_of
            ).acquire(max_new_cells=1)
            seed_path = store.run_root / "resume_contract.json"
            store.close()
            with seed_path.open("ab") as handle:
                handle.write(b" " * (native_acquisition.MAX_RESUME_SEED_BYTES + 1))
            with patch.object(
                native_acquisition,
                "_bounded_directory_entries",
                side_effect=AssertionError("topology walked before bounded seed"),
            ):
                with self.assertRaisesRegex(
                    ResumeValidationError,
                    "unsafe or unbounded",
                ):
                    NativeHistoryStore(
                        native_root,
                        "oversized-seed-first",
                        resume=True,
                    )

            seedless = NativeHistoryStore(native_root, "missing-seed-first")
            (seedless.run_root / "_attempts").mkdir()
            seedless.close()
            with patch.object(
                native_acquisition,
                "_bounded_directory_entries",
                side_effect=AssertionError("topology walked without a seed"),
            ):
                with self.assertRaisesRegex(
                    ResumeValidationError,
                    "resume_contract.json",
                ):
                    NativeHistoryStore(
                        native_root,
                        "missing-seed-first",
                        resume=True,
                    )

    def test_resume_rejects_third_attempt_receipt_at_cap_plus_one(self):
        as_of = datetime(2026, 1, 10, 12, 2, 17, tzinfo=UTC)
        request = NativeHistoryRequest(
            symbols=("EURUSD",),
            timeframes=("M5", "M15"),
            horizon_years=1,
            as_of_utc=as_of,
            retries=1,
            enforce_evidence_horizon=False,
        )
        with tempfile.TemporaryDirectory() as tmp:
            _, native_root = self._paths(tmp)
            store = NativeHistoryStore(native_root, "third-attempt-cap")
            NativeHistoryAcquirer(
                _RangeConnector(), store, request, now_fn=lambda: as_of
            ).acquire(max_new_cells=1)

            writer = NativeHistoryStore(
                native_root,
                "third-attempt-cap",
                resume=True,
            )
            failed = _minimal_provenance("third-attempt-cap", timeframe="M15")
            failed.update(
                {
                    "status": "failed",
                    "failure_status": "fetch_failed",
                    "error": {"type": "PageFetchError", "message": "retry"},
                }
            )
            writer.commit_retryable_attempt("EURUSD", "M15", failed)
            writer.commit_retryable_attempt("EURUSD", "M15", failed)
            attempt_dir = writer.run_root / "_attempts" / "EURUSD" / "M15"
            source = next(attempt_dir.glob("attempt-*.json"))
            shutil.copyfile(source, attempt_dir / f"attempt-{'f' * 32}.json")
            writer.close()

            with self.assertRaisesRegex(
                ResumeValidationError,
                "entry cap exceeded at 3",
            ):
                NativeHistoryStore(
                    native_root,
                    "third-attempt-cap",
                    resume=True,
                )

    def test_os_run_lock_blocks_live_writer_and_recovers_only_dead_pid(self):
        with tempfile.TemporaryDirectory() as tmp:
            _, native_root = self._paths(tmp)
            first = NativeHistoryRunLock(native_root, "lock-test").acquire()
            with self.assertRaises(ConcurrentWriterError):
                NativeHistoryRunLock(native_root, "lock-test").acquire()
            first.release()

            owner_path = first.owner_path
            stale = json.loads(owner_path.read_text(encoding="utf-8"))
            stale.update({"active": True, "pid": 2_147_483_646, "token": "dead"})
            owner_path.write_text(json.dumps(stale), encoding="utf-8")
            recovered = NativeHistoryRunLock(native_root, "lock-test").acquire()
            self.assertEqual(recovered.recovered_stale_owner["pid"], 2_147_483_646)
            recovered.release()

            live = json.loads(owner_path.read_text(encoding="utf-8"))
            live.update({"active": True, "pid": os.getpid(), "token": "live"})
            owner_path.write_text(json.dumps(live), encoding="utf-8")
            with self.assertRaises(ConcurrentWriterError):
                NativeHistoryRunLock(native_root, "lock-test").acquire()

    def test_writer_owner_authority_rejects_oversize_and_hardlinks(self):
        with tempfile.TemporaryDirectory() as tmp:
            _, native_root = self._paths(tmp)
            oversized = NativeHistoryRunLock(native_root, "oversized-owner")
            oversized.runs_root.mkdir(parents=True)
            oversized.owner_path.write_bytes(
                b"x" * (native_acquisition.MAX_OWNER_RECORD_BYTES + 1)
            )
            with self.assertRaisesRegex(
                ConcurrentWriterError,
                "owner record is unreadable",
            ):
                oversized.acquire()

            linked = NativeHistoryRunLock(native_root, "linked-owner")
            source = linked.runs_root / "owner-source.json"
            source.write_text(
                json.dumps(
                    {
                        "schema_version": NativeHistoryRunLock.SCHEMA_VERSION,
                        "active": False,
                    }
                ),
                encoding="utf-8",
            )
            os.link(source, linked.owner_path)
            with self.assertRaisesRegex(
                ConcurrentWriterError,
                "owner record is unreadable",
            ):
                linked.acquire()

    def test_terminal_lock_serializes_distinct_run_ids(self):
        with tempfile.TemporaryDirectory() as tmp:
            _, native_root = self._paths(tmp)
            terminal = NativeHistoryTerminalLock(native_root).acquire()
            run_a = NativeHistoryRunLock(native_root, "run-a").acquire()
            run_b = NativeHistoryRunLock(native_root, "run-b").acquire()
            try:
                with self.assertRaises(ConcurrentWriterError):
                    NativeHistoryTerminalLock(native_root).acquire()
            finally:
                run_b.release()
                run_a.release()
                terminal.release()

    def test_execute_runtime_bound_uses_killable_supervised_worker(self):
        sentinel = sys.modules.get("pm_mt5", "__absent__")
        sys.modules["pm_mt5"] = None
        try:
            with (
                patch(
                    "tools.acquire_native_history._supervisor_terminal_cache_snapshot",
                    return_value={"status": "within_bounds", "violations": []},
                ),
                patch("tools.acquire_native_history.subprocess.Popen") as run_child,
            ):
                run_child.return_value.wait.return_value = 7
                result = main(
                    [
                        "--config",
                        "does-not-need-to-exist.json",
                        "--execute",
                        "--max-runtime-seconds",
                        "12.5",
                    ]
                )
        finally:
            if sentinel == "__absent__":
                sys.modules.pop("pm_mt5", None)
            else:
                sys.modules["pm_mt5"] = sentinel
        self.assertEqual(result, 7)
        command = run_child.call_args.args[0]
        self.assertEqual(
            command[:2],
            [
                sys.executable,
                str(
                    Path(__file__).resolve().parents[1]
                    / "tools"
                    / "acquire_native_history.py"
                ),
            ],
        )
        self.assertIn("--_supervised-worker", command)
        self.assertEqual(run_child.return_value.wait.call_args.kwargs["timeout"], 0.5)

    def test_execute_uses_finite_default_supervisor_when_flag_is_omitted(self):
        with (
            patch(
                "tools.acquire_native_history._supervisor_terminal_cache_snapshot",
                return_value={"status": "within_bounds", "violations": []},
            ),
            patch("tools.acquire_native_history.subprocess.Popen") as run_child,
        ):
            run_child.return_value.wait.return_value = 0
            result = main(["--execute"])

        self.assertEqual(result, 0)
        command = run_child.call_args.args[0]
        self.assertIn("--_supervised-worker", command)
        self.assertNotIn("--max-runtime-seconds", command)
        self.assertEqual(run_child.return_value.wait.call_args.kwargs["timeout"], 0.5)

    def test_execute_runtime_bound_returns_typed_hard_timeout(self):
        process = SimpleNamespace()
        process.wait = Mock(side_effect=[subprocess.TimeoutExpired("worker", 0.01), 0])
        process.poll = Mock(return_value=None)
        process.terminate = Mock()
        process.kill = Mock()
        with (
            patch(
                "tools.acquire_native_history._supervisor_terminal_cache_snapshot",
                return_value={"status": "within_bounds", "violations": []},
            ),
            patch(
                "tools.acquire_native_history.subprocess.Popen",
                return_value=process,
            ),
            patch(
                "tools.acquire_native_history.time.monotonic",
                side_effect=[0.0, 0.02],
            ),
        ):
            result = main(
                [
                    "--execute",
                    "--max-runtime-seconds",
                    "0.01",
                ]
            )
        self.assertEqual(result, HARD_TIMEOUT_EXIT_CODE)

    def test_execute_supervisor_stops_on_terminal_cache_growth(self):
        process = SimpleNamespace()
        process.wait = Mock(side_effect=[subprocess.TimeoutExpired("worker", 0.5), 0])
        process.poll = Mock(return_value=None)
        process.terminate = Mock()
        process.kill = Mock()
        snapshots = [
            {"status": "within_bounds", "violations": []},
            {
                "status": "limit_exceeded",
                "violations": ["terminal_hcc_file_bound_breached"],
            },
        ]
        with (
            patch(
                "tools.acquire_native_history._supervisor_terminal_cache_snapshot",
                side_effect=snapshots,
            ),
            patch(
                "tools.acquire_native_history.subprocess.Popen",
                return_value=process,
            ),
        ):
            result = main(
                [
                    "--execute",
                    "--max-runtime-seconds",
                    "30",
                ]
            )
        self.assertEqual(result, RESOURCE_LIMIT_EXIT_CODE)
        process.terminate.assert_called_once_with()

    def test_publish_race_accepts_valid_concurrent_leaf_without_overwrite(self):
        as_of = datetime(2026, 1, 10, 12, 0, tzinfo=UTC)
        frame = _bar_frame([as_of - timedelta(minutes=5)])
        calls = []

        def concurrent_publish(source, destination):
            calls.append((Path(source), Path(destination)))
            shutil.copytree(source, destination)
            raise PermissionError("simulated OneDrive rename collision")

        with tempfile.TemporaryDirectory() as tmp:
            _, native_root = self._paths(tmp)
            store = NativeHistoryStore(
                native_root,
                "publish-race-test",
                replace_fn=concurrent_publish,
                publish_retry_delays=(),
            )
            record = store.commit_leaf(
                "EURUSD",
                "M5",
                _minimal_provenance("publish-race-test"),
                frame,
            )
            leaf = store.run_root / "EURUSD" / "M5"
            before = (leaf / "bars.csv").read_bytes()
            recognized = store.commit_write_failure(
                "EURUSD",
                "M5",
                _minimal_provenance("publish-race-test"),
                PermissionError("original replace error"),
            )
            self.assertIsNone(recognized["sha256"])
            self.assertEqual(recognized["failure_status"], "write_failed")
            self.assertEqual(
                _latest_attempt_record(store.run_root, "EURUSD", "M5")[
                    "failure_status"
                ],
                "write_failed",
            )
            self.assertEqual((leaf / "bars.csv").read_bytes(), before)
            self.assertEqual(
                record["sha256"],
                json.loads((leaf / "provenance.json").read_text(encoding="utf-8"))[
                    "sha256"
                ],
            )
            self.assertEqual(len(calls), 1)
            self.assertFalse(list(leaf.parent.glob(".*.staging")))
            store.close()

    def test_manifest_publish_race_never_overwrites_concurrent_file(self):
        attacker_bytes = b"ATTACKER-MANIFEST\n"

        def concurrent_create_then_link(source, destination):
            Path(destination).write_bytes(attacker_bytes)
            os.link(source, destination)

        with tempfile.TemporaryDirectory() as tmp:
            _, native_root = self._paths(tmp)
            store = NativeHistoryStore(
                native_root,
                "manifest-race-test",
                manifest_link_fn=concurrent_create_then_link,
                publish_retry_delays=(),
            )
            final_path = store.run_root / "run_manifest.json"
            with self.assertRaisesRegex(
                FileExistsError,
                "appeared concurrently",
            ):
                store.commit_manifest({"run_id": "manifest-race-test"})
            self.assertEqual(final_path.read_bytes(), attacker_bytes)
            self.assertFalse(list(store.run_root.glob(".run_manifest.*.staging")))
            store.close()

    def test_manifest_rolls_back_its_own_link_if_attempts_appear_in_publish(self):
        with tempfile.TemporaryDirectory() as tmp:
            _, native_root = self._paths(tmp)

            def inject_attempts_then_link(source, destination):
                run_root = Path(destination).parent
                (run_root / "_attempts").mkdir()
                os.link(source, destination)

            store = NativeHistoryStore(
                native_root,
                "manifest-attempt-race",
                manifest_link_fn=inject_attempts_then_link,
                publish_retry_delays=(),
            )
            with self.assertRaisesRegex(
                ResumeValidationError,
                "attempt area appeared",
            ):
                store.commit_manifest({"run_id": "manifest-attempt-race"})
            self.assertFalse((store.run_root / "run_manifest.json").exists())
            self.assertTrue((store.run_root / "_attempts").is_dir())
            self.assertFalse(
                (
                    store.runs_root / ".manifest-attempt-race.run_manifest.staging"
                ).exists()
            )
            store.close()

    def test_pinned_native_receipt_detects_directory_entry_mutation(self):
        with tempfile.TemporaryDirectory() as tmp:
            _, native_root = self._paths(tmp)
            store = NativeHistoryStore(native_root, "directory-mutation-pin")
            authority = store.run_root / "authority.json"
            authority.write_text('{"value":1}\n', encoding="utf-8")
            _value, receipt = native_acquisition._load_pinned_native_json_object(
                authority,
                run_root=store.run_root,
            )
            (store.run_root / "_attempts").mkdir()
            with self.assertRaisesRegex(
                ResumeValidationError,
                "directory identity changed",
            ):
                native_acquisition._verify_pinned_native_file_receipt(receipt)
            store.close()

    def test_resume_seed_publish_race_never_overwrites_concurrent_file(self):
        attacker_bytes = b"ATTACKER-SEED\n"

        def concurrent_create_then_link(source, destination):
            Path(destination).write_bytes(attacker_bytes)
            os.link(source, destination)

        with tempfile.TemporaryDirectory() as tmp:
            _, native_root = self._paths(tmp)
            store = NativeHistoryStore(
                native_root,
                "seed-race-test",
                manifest_link_fn=concurrent_create_then_link,
            )
            seed_path = store.run_root / "resume_contract.json"
            with self.assertRaisesRegex(
                FileExistsError,
                "appeared concurrently",
            ):
                store.ensure_resume_contract(
                    {"window_contract_version": (LEGACY_FLAT_WINDOW_CONTRACT_VERSION)},
                    _RangeConnector().get_runtime_metadata(),
                    datetime(2026, 1, 10, tzinfo=UTC),
                    symbols=("EURUSD",),
                    timeframes=("M5",),
                    production_authoritative=False,
                )
            self.assertEqual(seed_path.read_bytes(), attacker_bytes)
            self.assertFalse(list(store.run_root.glob(".resume_contract.*.staging")))
            store.close()

    def test_publish_permission_error_retries_only_while_final_absent(self):
        as_of = datetime(2026, 1, 10, 12, 0, tzinfo=UTC)
        frame = _bar_frame([as_of - timedelta(minutes=5)])
        attempts = []
        sleeps = []

        def flaky_replace(source, destination):
            attempts.append((Path(source), Path(destination)))
            if len(attempts) < 3:
                raise PermissionError("simulated OneDrive sharing violation")
            os.replace(source, destination)

        with tempfile.TemporaryDirectory() as tmp:
            _, native_root = self._paths(tmp)
            store = NativeHistoryStore(
                native_root,
                "publish-retry-test",
                replace_fn=flaky_replace,
                publish_retry_delays=(0.01, 0.02, 0.03),
                sleep_fn=sleeps.append,
            )
            store.commit_leaf(
                "EURUSD",
                "M5",
                _minimal_provenance("publish-retry-test"),
                frame,
            )
            self.assertEqual(len(attempts), 3)
            self.assertEqual(sleeps, [0.01, 0.02])
            self.assertTrue((store.run_root / "EURUSD" / "M5" / "bars.csv").exists())
            store.close()

    def test_execute_resume_lock_fails_before_mt5_import_or_connection(self):
        as_of = datetime(2026, 1, 10, 12, 2, 17, tzinfo=UTC)
        request = NativeHistoryRequest(
            symbols=("EURUSD",),
            timeframes=("M5", "M15"),
            horizon_years=1,
            as_of_utc=as_of,
            retries=1,
            enforce_evidence_horizon=False,
        )
        with tempfile.TemporaryDirectory() as tmp:
            root, native_root = self._paths(tmp)
            config = root / "config.json"
            config.write_text(json.dumps({"symbols": ["EURUSD"]}), encoding="utf-8")
            store = NativeHistoryStore(native_root, "locked-cli-resume")
            NativeHistoryAcquirer(
                _RangeConnector(), store, request, now_fn=lambda: as_of
            ).acquire(max_new_cells=1)
            holder = NativeHistoryRunLock(native_root, "locked-cli-resume").acquire()
            sentinel = sys.modules.get("pm_mt5", "__absent__")
            sys.modules["pm_mt5"] = None
            try:
                with self.assertRaises(ConcurrentWriterError):
                    main(
                        [
                            "--config",
                            str(config),
                            "--output-root",
                            str(native_root),
                            "--run-id",
                            "locked-cli-resume",
                            "--resume",
                            "--timeframes",
                            "M5",
                            "M15",
                            "--execute",
                            "--_supervised-worker",
                        ]
                    )
            finally:
                holder.release()
                if sentinel == "__absent__":
                    sys.modules.pop("pm_mt5", None)
                else:
                    sys.modules["pm_mt5"] = sentinel

    def test_execute_terminal_lock_blocks_other_run_before_mt5_import(self):
        with tempfile.TemporaryDirectory() as tmp:
            root, native_root = self._paths(tmp)
            config = root / "config.json"
            config.write_text(json.dumps({"symbols": ["EURUSD"]}), encoding="utf-8")
            holder = NativeHistoryTerminalLock(native_root).acquire()
            sentinel = sys.modules.get("pm_mt5", "__absent__")
            sys.modules["pm_mt5"] = None
            try:
                with self.assertRaises(ConcurrentWriterError):
                    main(
                        [
                            "--config",
                            str(config),
                            "--output-root",
                            str(native_root),
                            "--run-id",
                            "different-run",
                            "--timeframes",
                            "M5",
                            "--execute",
                            "--_supervised-worker",
                        ]
                    )
            finally:
                holder.release()
                if sentinel == "__absent__":
                    sys.modules.pop("pm_mt5", None)
                else:
                    sys.modules["pm_mt5"] = sentinel

    def test_production_deep_gate_never_subtracts_terminal_cells(self):
        terminal_cell = (PRODUCTION_SYMBOL_ORDER[0], DEFAULT_TIMEFRAMES[0])
        with tempfile.TemporaryDirectory() as tmp:
            run_root = Path(tmp) / "run"
            for symbol in PRODUCTION_SYMBOL_ORDER:
                for timeframe in DEFAULT_TIMEFRAMES:
                    leaf = run_root / symbol / timeframe
                    leaf.mkdir(parents=True)
                    if (symbol, timeframe) == terminal_cell:
                        (leaf / "provenance.json").write_text(
                            '{"status":"failed"}\n', encoding="utf-8"
                        )
            discovered = native_acquisition._discover_native_leaves(
                run_root,
                symbols=PRODUCTION_SYMBOL_ORDER,
                timeframes=DEFAULT_TIMEFRAMES,
            )
            native_acquisition._assert_exact_production_leaf_roster(discovered)

            terminal_leaf = run_root / terminal_cell[0] / terminal_cell[1]
            (terminal_leaf / "provenance.json").unlink()
            terminal_leaf.rmdir()
            discovered = native_acquisition._discover_native_leaves(
                run_root,
                symbols=PRODUCTION_SYMBOL_ORDER,
                timeframes=DEFAULT_TIMEFRAMES,
            )
            with self.assertRaisesRegex(ResumeValidationError, "exact 600-cell"):
                native_acquisition._assert_exact_production_leaf_roster(discovered)

    def test_terminal_technical_leaf_freezes_two_full_receipts_and_validates(self):
        with tempfile.TemporaryDirectory() as tmp:
            native_root = Path(tmp) / "data" / "native"
            store = NativeHistoryStore(native_root, "terminal-two-full")
            try:
                fixture = _prepare_terminal_writer_receipts(store)
                completed = datetime.fromisoformat(
                    fixture["manifest"]["acquisition_completed_utc"].replace(
                        "Z", "+00:00"
                    )
                )
                started = datetime.fromisoformat(
                    fixture["manifest"]["acquisition_started_utc"].replace(
                        "Z", "+00:00"
                    )
                )
                record = store.commit_terminal_technical_leaf(
                    "EURUSD",
                    "M5",
                    expected_broker_symbol="EURUSD.a",
                    common_request=fixture["common_request"],
                    source_environment=fixture["source_environment"],
                    expected_retries=1,
                    manifest_acquisition_started_utc=started,
                    manifest_acquisition_completed_utc=completed,
                )
                self.assertIsNotNone(record)
                leaf = store.leaf_path("EURUSD", "M5")
                self.assertEqual(
                    {path.name for path in leaf.iterdir()}, {"provenance.json"}
                )
                self.assertFalse((store.run_root / "_attempts").exists())
                evidence = record["terminal_technical_evidence"]
                self.assertEqual(len(evidence["receipts"]), 2)
                self.assertTrue(
                    all(
                        receipt["envelope"]["retention_status"] == "full"
                        for receipt in evidence["receipts"]
                    )
                )
                result = store._terminal_manifest_result("EURUSD", "M5", record)
                self.assertEqual(
                    validate_v5_terminal_technical_cell(
                        record,
                        result,
                        fixture["common_request"],
                        symbol="EURUSD",
                        timeframe="M5",
                        run_id=store.run_id,
                        manifest_source_environment=fixture["source_environment"],
                        expected_retries=1,
                        manifest_acquisition_started_utc=started,
                        manifest_acquisition_completed_utc=completed,
                    ),
                    "technical_history_gap",
                )
                discovered = native_acquisition._discover_native_leaves(
                    store.run_root,
                    symbols=("EURUSD",),
                    timeframes=("M5",),
                )
                self.assertEqual(set(discovered), {("EURUSD", "M5")})
            finally:
                store.close()

    def test_terminal_technical_publish_crash_retains_receipts_for_recovery(self):
        with tempfile.TemporaryDirectory() as tmp:
            native_root = Path(tmp) / "data" / "native"
            store = NativeHistoryStore(native_root, "terminal-post-publish-crash")
            fixture = _prepare_terminal_writer_receipts(store)
            started = fixture["manifest"]["acquisition_started_utc"]
            completed = fixture["manifest"]["acquisition_completed_utc"]
            with patch.object(
                store,
                "_clear_attempt_receipts_for_cell",
                side_effect=RuntimeError("simulated crash after publish"),
            ):
                with self.assertRaisesRegex(RuntimeError, "after publish"):
                    store.commit_terminal_technical_leaf(
                        "EURUSD",
                        "M5",
                        expected_broker_symbol="EURUSD.a",
                        common_request=fixture["common_request"],
                        source_environment=fixture["source_environment"],
                        expected_retries=1,
                        manifest_acquisition_started_utc=started,
                        manifest_acquisition_completed_utc=completed,
                    )
            self.assertTrue(store.leaf_path("EURUSD", "M5").is_dir())
            self.assertTrue((store.run_root / "_attempts" / "EURUSD" / "M5").is_dir())
            store.close()
            resumed = NativeHistoryStore(
                native_root,
                "terminal-post-publish-crash",
                resume=True,
            )
            try:
                record = resumed._validate_existing_terminal_technical_leaf(
                    "EURUSD",
                    "M5",
                    common_request=fixture["common_request"],
                    source_environment=fixture["source_environment"],
                    expected_retries=1,
                    manifest_acquisition_started_utc=started,
                    manifest_acquisition_completed_utc=completed,
                )
                self.assertEqual(record["status"], "failed")
                resumed._clear_attempt_receipts_for_cell("EURUSD", "M5")
                self.assertFalse((resumed.run_root / "_attempts").exists())
            finally:
                resumed.close()

    def test_terminal_technical_prepublication_crash_keeps_retry_evidence(self):
        with tempfile.TemporaryDirectory() as tmp:
            native_root = Path(tmp) / "data" / "native"

            def fail_replace(_source, _destination):
                raise OSError("simulated atomic publish failure")

            store = NativeHistoryStore(
                native_root,
                "terminal-prepublish-crash",
                replace_fn=fail_replace,
                publish_retry_delays=(),
            )
            try:
                fixture = _prepare_terminal_writer_receipts(store)
                with self.assertRaisesRegex(OSError, "atomic publish failure"):
                    store.commit_terminal_technical_leaf(
                        "EURUSD",
                        "M5",
                        expected_broker_symbol="EURUSD.a",
                        common_request=fixture["common_request"],
                        source_environment=fixture["source_environment"],
                        expected_retries=1,
                        manifest_acquisition_started_utc=fixture["manifest"][
                            "acquisition_started_utc"
                        ],
                        manifest_acquisition_completed_utc=fixture["manifest"][
                            "acquisition_completed_utc"
                        ],
                    )
                self.assertFalse(store.leaf_path("EURUSD", "M5").exists())
                self.assertEqual(
                    len(
                        list(
                            (store.run_root / "_attempts" / "EURUSD" / "M5").glob(
                                "attempt-*.json"
                            )
                        )
                    ),
                    2,
                )
            finally:
                store.close()

    def test_terminal_technical_rejects_tamper_duplicate_and_summary(self):
        def attempt_terminalization(store, fixture):
            return store.commit_terminal_technical_leaf(
                "EURUSD",
                "M5",
                expected_broker_symbol="EURUSD.a",
                common_request=fixture["common_request"],
                source_environment=fixture["source_environment"],
                expected_retries=1,
                manifest_acquisition_started_utc=fixture["manifest"][
                    "acquisition_started_utc"
                ],
                manifest_acquisition_completed_utc=fixture["manifest"][
                    "acquisition_completed_utc"
                ],
            )

        with tempfile.TemporaryDirectory() as tmp:
            native_root = Path(tmp) / "data" / "native"
            store = NativeHistoryStore(native_root, "terminal-tamper")
            try:
                fixture = _prepare_terminal_writer_receipts(store)
                path = next(
                    (store.run_root / "_attempts" / "EURUSD" / "M5").glob(
                        "attempt-*.json"
                    )
                )
                envelope = json.loads(path.read_text(encoding="utf-8"))
                envelope["record"]["error"]["message"] = "tampered"
                path.write_bytes(
                    json.dumps(
                        envelope,
                        sort_keys=True,
                        separators=(",", ":"),
                        ensure_ascii=False,
                    ).encode("utf-8")
                    + b"\n"
                )
                with self.assertRaises(ResumeValidationError):
                    attempt_terminalization(store, fixture)
                self.assertFalse(store.leaf_path("EURUSD", "M5").exists())
            finally:
                store.close()

        with tempfile.TemporaryDirectory() as tmp:
            native_root = Path(tmp) / "data" / "native"
            store = NativeHistoryStore(native_root, "terminal-duplicate")
            try:
                fixture = _prepare_terminal_writer_receipts(store, duplicate=True)
                with self.assertRaises(ResumeValidationError):
                    attempt_terminalization(store, fixture)
                self.assertFalse(store.leaf_path("EURUSD", "M5").exists())
            finally:
                store.close()

        with tempfile.TemporaryDirectory() as tmp:
            native_root = Path(tmp) / "data" / "native"
            store = NativeHistoryStore(native_root, "terminal-summary")
            try:
                fixture = _prepare_terminal_writer_receipts(store)
                path = next(
                    (store.run_root / "_attempts" / "EURUSD" / "M5").glob(
                        "attempt-*.json"
                    )
                )
                envelope = json.loads(path.read_text(encoding="utf-8"))
                envelope["retention_status"] = "summary_only"
                envelope["record"] = {
                    "status": "partial",
                    "failure_status": "technical_history_gap",
                }
                path.write_bytes(
                    json.dumps(
                        envelope,
                        sort_keys=True,
                        separators=(",", ":"),
                        ensure_ascii=False,
                    ).encode("utf-8")
                    + b"\n"
                )
                with self.assertRaises(ResumeValidationError):
                    attempt_terminalization(store, fixture)
                self.assertFalse(store.leaf_path("EURUSD", "M5").exists())
            finally:
                store.close()

    def test_broker_session_close_helper_uses_regular_successor_only(self):
        nominal = 24 * 60 * 60 * 1_000_000_000
        friday = pd.Timestamp("2026-03-27T21:00:00Z").value
        regular_next = pd.Timestamp("2026-03-28T21:00:00Z").value
        monday = pd.Timestamp("2026-03-30T21:00:00Z").value
        regular = native_acquisition._successor_aware_broker_close_ns(
            np.asarray([friday], dtype="int64"),
            nominal_ns=nominal,
            next_successor_ns=regular_next,
        )
        weekend = native_acquisition._successor_aware_broker_close_ns(
            np.asarray([friday], dtype="int64"),
            nominal_ns=nominal,
            next_successor_ns=monday,
        )
        self.assertEqual(int(regular[0]), regular_next)
        self.assertEqual(int(weekend[0]), friday + nominal)


if __name__ == "__main__":
    unittest.main()
