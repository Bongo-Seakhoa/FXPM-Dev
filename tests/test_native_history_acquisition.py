import json
import io
import os
import shutil
import sys
import tempfile
import unittest
from contextlib import redirect_stdout
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

import pandas as pd


PATCH_ROOT = Path(__file__).resolve().parents[1]
if str(PATCH_ROOT) not in sys.path:
    sys.path.insert(0, str(PATCH_ROOT))

from tools.acquire_native_history import (  # noqa: E402
    ACQUISITION_CONTRACT_VERSION,
    DEFAULT_TIMEFRAMES,
    FORENSIC_TIMEFRAME_REQUEST_MAP_SCHEMA_VERSION,
    LEGACY_FLAT_WINDOW_CONTRACT_VERSION,
    SEGMENTED_SESSION_COVERAGE_METHOD_V2,
    TIMEFRAME_EVIDENCE_CONTRACT_VERSION,
    TIMEFRAME_REQUEST_MAP_SCHEMA_VERSION,
    TIMEFRAME_REQUEST_MAP_SCHEMA_VERSION_V3,
    TWO_REFERENCE_CONSENSUS_POLICY,
    ConcurrentWriterError,
    NativeHistoryAcquirer,
    NativeHistoryRunLock,
    NativeHistoryRequest,
    ResumeValidationError,
    NativeHistoryStore,
    TIMEFRAME_SECONDS,
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


class _RangeConnector:
    def __init__(self, *, cutoff=None, failing_timeframes=()):
        self.cutoff = cutoff
        self.failing_timeframes = set(failing_timeframes)
        self.calls = []
        self._last_error = (1, "Success")

    def find_broker_symbol(self, symbol):
        return f"{symbol}.a"

    def get_runtime_metadata(self):
        return {
            "metadata_status": "available",
            "terminal": {
                "name": "MetaTrader 5",
                "build": 5432,
                "max_bars": 10000,
                "version": [5, 0, 5432, "10 Jul 2026"],
            },
            "account": {
                "login": 123456,
                "server": "MockBroker-ReadOnly",
                "company": "Mock Broker",
                "currency": "USD",
            },
        }

    def get_last_error(self):
        return self._last_error

    def get_bars_range(self, symbol, timeframe, start_date, end_date, sync_retry=False):
        self.calls.append((symbol, timeframe, start_date, end_date, sync_retry))
        if timeframe in self.failing_timeframes:
            self._last_error = (-10005, "IPC timeout")
            raise TimeoutError(f"mock {timeframe} timeout")
        self._last_error = (1, "Success")
        if self.cutoff is not None and end_date <= self.cutoff:
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
            return None
        points = [first]
        if last != first:
            points.append(last)
        return _bar_frame(points)


class NativeHistoryAcquisitionTests(unittest.TestCase):
    def _paths(self, temp_dir):
        root = Path(temp_dir)
        native_root = root / "data" / "native"
        return root, native_root

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

            self.assertEqual(manifest["counts"], {"success": 1, "partial": 0, "failed": 0, "total": 1})
            self.assertGreater(len(connector.calls), 1, "M5 should be calendar-window paginated")
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
            self.assertEqual(sidecar["request"]["common_as_of_utc"], "2026-01-10T12:00:00Z")
            self.assertEqual(sidecar["request"]["native_fetch_start_utc"], "2025-01-10T12:00:00Z")
            self.assertEqual(sidecar["request"]["native_fetch_end_exclusive_utc"], "2026-01-10T12:00:00Z")
            self.assertEqual(sidecar["request"]["last_fully_closed_bar_open_utc"], "2026-01-10T11:55:00Z")
            self.assertEqual(sidecar["source_environment"]["terminal"]["max_bars"], 10000)
            self.assertEqual(sidecar["source_environment"]["account"]["server"], "MockBroker-ReadOnly")
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

    def test_partial_broker_history_is_explicit_and_not_optimization_eligible(self):
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
            sidecar = json.loads(
                (store.run_root / "NEWCFD" / "M5" / "provenance.json").read_text(encoding="utf-8")
            )
            self.assertEqual(sidecar["status"], "partial")
            self.assertEqual(
                sidecar["failure_status"],
                "boundary_history_shortfall",
            )
            self.assertFalse(sidecar["eligible_for_optimization"])
            self.assertEqual(sidecar["coverage"]["status"], "partial_requested_span")
            self.assertGreater(sidecar["coverage"]["start_delay_seconds"], 300 * 86400)
            self.assertEqual(
                sidecar["coverage"]["technical_integrity_status"],
                "complete_page_ledger",
            )
            self.assertEqual(sidecar["coverage"]["internal_empty_page_count"], 0)
            self.assertEqual(sidecar["coverage"]["internal_truncated_page_count"], 0)
            self.assertFalse(sidecar["coverage"]["market_calendar_completeness_assessed"])
            self.assertTrue((store.run_root / "NEWCFD" / "M5" / "bars.csv").exists())

    def test_outer_retry_continues_after_internal_sync_budget_returns_partial_tail(self):
        start = datetime(2024, 1, 1, tzinfo=UTC)
        end = datetime(2024, 1, 10, tzinfo=UTC)

        class _DelayedBoundaryConnector:
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
                if self.calls == 1:
                    frame = _bar_frame([end_date - timedelta(minutes=5)])
                    attempts = 6
                    complete = False
                else:
                    frame = _bar_frame(
                        [start_date, end_date - timedelta(minutes=5)]
                    )
                    attempts = 1
                    complete = True
                frame.attrs["mt5_history_range_diagnostics"] = {
                    "copy_rates_range_attempts": attempts,
                    "boundary_complete": complete,
                    "best_extent": {
                        "rows": len(frame),
                        "first_epoch_second": int(frame.index[0].timestamp()),
                        "last_epoch_second": int(frame.index[-1].timestamp()),
                    },
                }
                return frame

        with tempfile.TemporaryDirectory() as tmp:
            _, native_root = self._paths(tmp)
            store = NativeHistoryStore(native_root, "outer-sync-retry")
            request = NativeHistoryRequest(
                symbols=("EURUSD",),
                timeframes=("M5",),
                horizon_years=1,
                as_of_utc=end,
                retries=3,
                retry_sleep_seconds=0.0,
                enforce_evidence_horizon=False,
            )
            connector = _DelayedBoundaryConnector()
            acquirer = NativeHistoryAcquirer(
                connector,
                store,
                request,
                now_fn=lambda: end,
                runtime_metadata={},
            )

            frame, outcome = acquirer._fetch_page(
                "EURUSD",
                "M5",
                start,
                end,
            )

            self.assertIsNotNone(frame)
            self.assertEqual(len(frame), 2)
            self.assertEqual(connector.calls, 2)
            self.assertEqual(outcome["attempts"], 2)
            self.assertEqual(outcome["copy_rates_range_attempts"], 7)
            store.close()

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
            store = NativeHistoryStore(native_root, "atomic-failure-test", csv_writer=explode_after_partial_write)
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
            self.assertFalse((leaf / "bars.csv").exists())
            sidecar = json.loads((leaf / "provenance.json").read_text(encoding="utf-8"))
            self.assertEqual(sidecar["failure_status"], "write_failed")
            self.assertIsNone(sidecar["artifact"])
            self.assertEqual(canonical.read_text(encoding="utf-8"), "canonical-must-not-change\n")
            self.assertFalse(list(leaf.parent.glob(".*.staging")))

    def test_one_timeframe_failure_does_not_cancel_other_native_timeframes(self):
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
                retries=2,
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

            self.assertEqual(manifest["counts"], {"success": 5, "partial": 0, "failed": 1, "total": 6})
            by_timeframe = {item["timeframe"]: item for item in manifest["results"]}
            self.assertEqual(by_timeframe["H4"]["failure_status"], "fetch_failed")
            self.assertFalse((store.run_root / "EURUSD" / "H4" / "bars.csv").exists())
            for timeframe in set(DEFAULT_TIMEFRAMES) - {"H4"}:
                self.assertIn(by_timeframe[timeframe]["status"], {"success", "partial"})
                self.assertTrue((store.run_root / "EURUSD" / timeframe / "bars.csv").exists())

    def test_silent_range_cap_is_detected_per_page(self):
        as_of = datetime(2026, 1, 10, 12, 2, tzinfo=UTC)

        class SilentlyCappedConnector(_RangeConnector):
            def get_bars_range(self, symbol, timeframe, start_date, end_date, sync_retry=False):
                frame = super().get_bars_range(
                    symbol, timeframe, start_date, end_date, sync_retry=sync_retry
                )
                # Leave the outer pages intact so whole-run first/last timestamps
                # look complete, but silently truncate an interior response.
                if len(self.calls) == 2:
                    seconds = TIMEFRAME_SECONDS[timeframe]
                    return _bar_frame([end_date - timedelta(seconds=seconds)])
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
            sidecar = json.loads(
                (store.run_root / "EURUSD" / "M5" / "provenance.json").read_text(encoding="utf-8")
            )
            self.assertEqual(sidecar["coverage"]["start_delay_seconds"], 0.0)
            self.assertEqual(sidecar["coverage"]["end_lag_seconds"], 0.0)
            self.assertEqual(sidecar["coverage"]["truncated_page_count"], 1)
            self.assertEqual(sidecar["failure_status"], "technical_history_gap")
            self.assertEqual(sidecar["coverage"]["internal_truncated_page_count"], 1)
            self.assertEqual(
                sidecar["coverage"]["technical_integrity_status"],
                "internal_gap_detected",
            )
            self.assertEqual(len(sidecar["pagination"]["truncated_pages"]), 1)
            self.assertFalse(sidecar["eligible_for_optimization"])

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
            def get_bars_range(self, symbol, timeframe, start_date, end_date, sync_retry=False):
                self.calls.append((symbol, timeframe, start_date, end_date, sync_retry))
                self._last_error = (1, "Success")
                selected = [ts for ts in opens if start_date <= ts.to_pydatetime() < end_date]
                return _bar_frame(selected) if selected else None

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
                (store.run_root / "EURUSD" / "D1" / "provenance.json").read_text(encoding="utf-8")
            )
            self.assertEqual(
                sidecar["request"]["alignment_mode"],
                "broker_observed_successor_or_nominal_close_v1",
            )
            self.assertEqual(
                sidecar["request"]["last_fully_closed_bar_open_utc"],
                "2025-03-30T21:00:00Z",
            )
            self.assertEqual(sidecar["coverage"]["actual_last_close_utc"], "2025-03-31T21:00:00Z")

    def test_store_is_shadow_only_and_create_only(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            with self.assertRaises(ValueError):
                NativeHistoryStore(root / "data", "wrong-root")
            native_root = root / "data" / "native"
            NativeHistoryStore(native_root, "immutable-run")
            with self.assertRaises(FileExistsError):
                NativeHistoryStore(native_root, "immutable-run")

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

    def test_read_only_runtime_snapshot_records_terminal_version_and_broker_identity(self):
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
        frame = _bar_frame(pd.date_range("2016-01-01", "2025-12-31", freq="B", tz="UTC"))

        coverage, details = expected_session_coverage_by_partition(frame, windows)

        self.assertEqual(set(coverage), {"optimizer_train", "tuning_validation", "final_lockbox"})
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

    def test_effective_acquisition_start_precedes_train_by_warmup_safety_and_close_slack(self):
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
            d1_sidecar = json.loads(
                (store.run_root / "EURUSD" / "D1" / "provenance.json").read_text(
                    encoding="utf-8"
                )
            )
            for timeframe, sidecar in (("M5", m5_sidecar), ("D1", d1_sidecar)):
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
                    sidecar["expected_session_coverage_details_by_partition"][
                        "method"
                    ],
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
                    edge_start = acquirer.timeframe_audit_reference_intervals[
                        target
                    ][reference]["strategy_full_context"][0]
                    self.assertLessEqual(
                        edge_start,
                        target_start,
                        f"{target}->{reference} omits its containing reference bar",
                    )
                    self.assertLessEqual(
                        acquirer.timeframe_source_request_intervals[reference][0],
                        edge_start,
                    )

            self.assertEqual(acquirer.as_of_utc.minute, 30)
            self.assertEqual(
                acquirer.timeframe_required_evidence_lead_start_utc["H4"].minute,
                30,
            )
            h4_target_start = acquirer.timeframe_requested_start_utc["H4"]
            h4_h1_edge_start = acquirer.timeframe_audit_reference_intervals["H4"][
                "H1"
            ]["strategy_full_context"][0]
            self.assertEqual(
                h4_h1_edge_start,
                pd.Timestamp(h4_target_start).floor("h").to_pydatetime(),
            )
            self.assertGreater(
                h4_h1_edge_start,
                acquirer.timeframe_strategy_evidence_intervals["H4"][0],
            )
            d1_target_start = acquirer.timeframe_requested_start_utc["D1"]
            d1_h4_edge_start = acquirer.timeframe_audit_reference_intervals["D1"][
                "H4"
            ]["strategy_full_context"][0]
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
                lambda value: value["request"][
                    "timeframe_audit_reference_intervals"
                ]["D1"]["H4"]["strategy_full_context"].update(
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

    def test_v4_forensic_contract_loads_but_cannot_resume_as_v5(self):
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
                source_interval = sidecar_request[
                    "timeframe_source_request_intervals"
                ][timeframe]
                strategy_interval = sidecar_request[
                    "timeframe_strategy_evidence_intervals"
                ][timeframe]
                v4_prefixes[timeframe] = (
                    {
                        "start_utc": source_interval["start_utc"],
                        "end_exclusive_utc": strategy_interval["start_utc"],
                        "role": "audit_only_reference",
                    }
                    if source_interval["start_utc"]
                    < strategy_interval["start_utc"]
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
            sidecar_request["timeframe_request_map_sha256"] = (
                _canonical_json_sha256(
                    _timeframe_request_hash_payload(sidecar_request)
                )
            )
            sidecar_path.write_text(
                json.dumps(sidecar, indent=2, sort_keys=True),
                encoding="utf-8",
            )

            recovered = load_native_history_resume_contract(
                native_root,
                "v4-forensic-resume",
            )
            self.assertEqual(
                recovered.request["timeframe_request_map_schema_version"],
                FORENSIC_TIMEFRAME_REQUEST_MAP_SCHEMA_VERSION,
            )

            resumed_store = NativeHistoryStore(
                native_root,
                "v4-forensic-resume",
                resume=True,
            )
            try:
                with self.assertRaisesRegex(
                    ResumeValidationError,
                    "resume request does not exactly match",
                ):
                    NativeHistoryAcquirer(
                        _RangeConnector(),
                        resumed_store,
                        request,
                        now_fn=lambda: as_of,
                        sleep_fn=lambda _: None,
                    )
            finally:
                resumed_store.close()

            for field in (
                "timeframe_strategy_evidence_intervals",
                "timeframe_audit_reference_map",
                "timeframe_audit_only_prefix_intervals",
                "strategy_evidence_interval",
                "audit_reference_timeframe",
                "audit_only_prefix_interval",
            ):
                sidecar_request.pop(field, None)
            sidecar_request["timeframe_request_map_schema_version"] = (
                TIMEFRAME_REQUEST_MAP_SCHEMA_VERSION_V3
            )
            sidecar_request["timeframe_request_map_sha256"] = (
                _canonical_json_sha256(
                    _timeframe_request_hash_payload(sidecar_request)
                )
            )
            sidecar_path.write_text(
                json.dumps(sidecar, indent=2, sort_keys=True),
                encoding="utf-8",
            )
            recovered_v3 = load_native_history_resume_contract(
                native_root,
                "v4-forensic-resume",
            )
            self.assertEqual(
                recovered_v3.request["timeframe_request_map_schema_version"],
                TIMEFRAME_REQUEST_MAP_SCHEMA_VERSION_V3,
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

    def test_resume_refuses_completed_manifest_and_only_cleans_local_staging(self):
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
            old_epoch = datetime.now(tz=UTC).timestamp() - 7200
            os.utime(inside, (old_epoch, old_epoch))
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
                _RangeConnector(), completed_store, complete_request, now_fn=lambda: as_of
            ).acquire()
            with self.assertRaises(ResumeValidationError):
                NativeHistoryStore(
                    native_root, "completed-resume-test", resume=True
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
            self.assertEqual(record["sha256"], recognized["sha256"])
            self.assertEqual((leaf / "bars.csv").read_bytes(), before)
            self.assertEqual(len(calls), 1)
            self.assertFalse(list(leaf.parent.glob(".*.staging")))
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
                        ]
                    )
            finally:
                holder.release()
                if sentinel == "__absent__":
                    sys.modules.pop("pm_mt5", None)
                else:
                    sys.modules["pm_mt5"] = sentinel


if __name__ == "__main__":
    unittest.main()
