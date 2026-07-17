from __future__ import annotations

import base64
import copy
import hashlib
import json
import os
import shutil
import subprocess
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Mapping

import pytest
import pandas as pd

import pm_native_run_reseal as reseal
from pm_native_request_contract import (
    COMMON_REQUEST_KEYS,
    MAX_TOTAL_ARTIFACT_BYTES,
    OUTPUT_MAX_CELL_BYTES,
    OUTPUT_MIN_FREE_BYTES,
    PRODUCTION_ARTIFACT_COLUMNS,
    PRODUCTION_PARTITIONS,
    PRODUCTION_SYMBOL_ORDER,
    PRODUCTION_SYMBOL_ORDER_SHA256,
    PRODUCTION_TIMEFRAME_ORDER,
    PRODUCTION_TIMEFRAME_ORDER_SHA256,
    TIMEFRAME_SECONDS,
    _fixture_mt5_v4_empty_diagnostics,
    _fixture_mt5_v4_page_diagnostics,
    build_trusted_v5_policy,
    build_valid_v5_contract_fixture,
    canonical_json_bytes,
    canonical_source_identity,
)
from tools.acquire_native_history import (
    NativeHistoryAcquirer,
    NativeHistoryRequest,
    NativeHistoryStore,
    TIMEFRAME_EVIDENCE_CONTRACT_VERSION,
    TerminalCacheResourceLimitError,
)
from pm_native_run_reseal import (
    NativeRunResealError,
    RUN_RESEAL_ENVELOPE_SCHEMA_VERSION,
    RUN_RESEAL_SIGNATURE_DOMAIN,
    RUN_RESEAL_TRUST_ROOT_SCHEMA_VERSION,
    VerifiedRunReseal,
    build_run_reseal_expected_authority,
    build_run_reseal_signing_request,
    build_run_reseal_trust_root,
    materialize_run_reseal_body,
    run_reseal_expected_authority_to_mapping,
    run_reseal_expected_authority_from_mapping,
    verify_run_reseal_envelope,
)


RUN_ID = "detached-reseal-fixture-v1"
KEY_ID = "fxpm-native-reseal-test-authority-2026"
ISSUED_AT = "2026-07-13T10:00:00Z"
VALIDATION_TIME = "2026-07-13T10:00:02Z"
ARTIFACT_SYMBOL = PRODUCTION_SYMBOL_ORDER[0]

# Public verification material only.  These constants authenticate the exact
# deterministic fixture written below; no signing capability exists in tests.
PUBLIC_KEY_BASE64 = "Rc4eelQhirDghqrvVXCQbEaJFPUwCcTcNkqwiZXUWfs="
SIGNATURE_BASE64 = "MMjivI4rc0DQBoSktTa8mU5+R34Bx4ODj+t4c667r15zDGzCya/07g32EII5npxSU9UsOetVA1WM9JiokMc6Dg=="
UNDOMAINED_SIGNATURE_BASE64 = "chJkLVMZdkfcW12uLplwP8tjYyakOPhJ0y3vTD2ZKecNOt+0KA1daSGeuHFO+70fP0F5KnD5q7xfsPmCQnetDA=="


def _json_bytes(value: Any) -> bytes:
    return (
        json.dumps(
            value,
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        )
        + "\n"
    ).encode("utf-8")


def _write_json(path: Path, value: Any) -> None:
    path.write_bytes(_json_bytes(value))


def _parse_utc(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def _csv_second(value: datetime) -> str:
    return value.isoformat(sep=" ")


def _source_last(value: datetime) -> str:
    previous = value - timedelta(seconds=1)
    return previous.strftime("%Y-%m-%d %H:%M:%S") + ".999999999+00:00"


def _compact_page_opens(timeframe: str, page: Any) -> list[datetime]:
    first = _parse_utc(page[3])
    last = _parse_utc(page[4])
    values = [first]
    middle = first + timedelta(seconds=TIMEFRAME_SECONDS[timeframe])
    if middle < last:
        values.append(middle)
    if last != first:
        values.append(last)
    return values


def _bars_bytes(timeframe: str, page_observations: Any) -> bytes:
    header = ["time", *PRODUCTION_ARTIFACT_COLUMNS]
    opens: list[datetime] = []
    for page in page_observations:
        opens.extend(_compact_page_opens(timeframe, page))
    rows = [",".join(header)]
    for opened in opens:
        closed = opened + timedelta(seconds=TIMEFRAME_SECONDS[timeframe])
        opened_text = _csv_second(opened)
        row = [
            opened_text,
            "1.00000",
            "1.00020",
            "0.99980",
            "1.00010",
            "1",
            "0",
            "1",
            opened_text,
            _source_last(closed),
            _csv_second(closed),
        ]
        rows.append(",".join(row))
    return ("\n".join(rows) + "\n").encode("utf-8")


def _compact_connector_bindings(provenance: dict[str, Any], observation: Any) -> None:
    attempts = provenance["pagination"]["page_attempts"]
    for attempt, page in zip(attempts, observation.page_observations, strict=True):
        first = _parse_utc(page[3])
        last = _parse_utc(page[4])
        row_count = len(_compact_page_opens(provenance["timeframe"], page))
        extent = {
            "rows": row_count,
            "first_epoch_second": int(first.timestamp()),
            "last_epoch_second": int(last.timestamp()),
        }
        diagnostics = attempt["connector_diagnostics"]
        diagnostics.update(
            {
                "raw_count": row_count,
                "raw_extent": copy.deepcopy(extent),
                "admitted_count": row_count,
                "admitted_extent": copy.deepcopy(extent),
                "best_extent": copy.deepcopy(extent),
            }
        )
        ledger = diagnostics["call_ledger"][0]
        ledger.update(
            {
                "raw_count": row_count,
                "raw_extent": copy.deepcopy(extent),
                "admitted_count": row_count,
                "admitted_extent": copy.deepcopy(extent),
            }
        )


def _compact_quality(provenance: dict[str, Any], observation: Any) -> dict[str, Any]:
    opens = [
        opened
        for page in observation.page_observations
        for opened in _compact_page_opens(provenance["timeframe"], page)
    ]
    seconds = TIMEFRAME_SECONDS[provenance["timeframe"]]
    gaps = [
        (current - previous).total_seconds()
        for previous, current in zip(opens, opens[1:])
    ]
    cadence_gaps = [value for value in gaps if value > seconds * 1.5]
    source = provenance["request"]["source_request_interval"]
    expected_slots = int(
        (
            _parse_utc(source["end_exclusive_utc"]) - _parse_utc(source["start_utc"])
        ).total_seconds()
        // seconds
    )
    return {
        "ohlc_invariants_valid": True,
        "invalid_ohlc_rows": 0,
        "monotonic_increasing": True,
        "unique_timestamps": True,
        "off_native_grid_rows": sum(
            int(opened.timestamp()) % seconds != 0 for opened in opens
        ),
        "expected_continuous_calendar_slots": expected_slots,
        "continuous_calendar_slot_density": float(len(opens) / expected_slots),
        "cadence_break_count": len(cadence_gaps),
        "nominal_missing_continuous_slots": sum(
            max(int(round(value / seconds)) - 1, 0) for value in cadence_gaps
        ),
        "largest_interbar_gap_seconds": max(gaps, default=0.0),
    }


def _writer_source_environment() -> dict[str, Any]:
    return {
        "metadata_status": "available",
        "account": {
            "login": 42,
            "server": "TestBroker-Demo",
            "company": "TestBroker",
            "currency": "USD",
        },
        "configured_connection": {
            "server": "TestBroker-Demo",
            "terminal_path": "C:\\Program Files\\MetaTrader 5\\terminal64.exe",
            "expected_login": 42,
            "expected_server": "TestBroker-Demo",
            "expected_currency": "USD",
        },
        "terminal": {
            "name": "MetaTrader 5",
            "company": "TestBroker",
            "path": "C:\\Program Files\\MetaTrader 5",
            "data_path": "C:\\FxPM\\TerminalData",
            "commondata_path": "C:\\FxPM\\TerminalCommon",
            "build": 5432,
            "version": [500, 5432, "13 Jul 2026"],
            "max_bars": 100_000_000,
        },
    }


def _write_actual_writer_resume_seed(
    native_root: Path,
    *,
    common_request: Mapping[str, Any],
    source_environment: Mapping[str, Any],
    acquisition_started_utc: str,
) -> None:
    """Install the exact V2 seed emitted by the production writer."""

    writer_root = native_root.parent / "writer-seed" / "data" / "native"
    store = NativeHistoryStore(writer_root, RUN_ID)
    try:
        store.ensure_resume_contract(
            common_request,
            source_environment,
            _parse_utc(acquisition_started_utc),
            symbols=PRODUCTION_SYMBOL_ORDER,
            timeframes=PRODUCTION_TIMEFRAME_ORDER,
            production_authoritative=True,
        )
    finally:
        store.close()
    source = writer_root / "runs" / RUN_ID / "resume_contract.json"
    destination = native_root / "runs" / RUN_ID / "resume_contract.json"
    destination.write_bytes(source.read_bytes())


def _writer_bar_frame(opens: list[datetime]) -> pd.DataFrame:
    index = pd.DatetimeIndex(opens)
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


def _writer_retried_page_diagnostics(
    *,
    symbol: str,
    timeframe: str,
    start: datetime,
    end: datetime,
    first_open: datetime,
    last_open: datetime,
    rows: int,
) -> dict[str, Any]:
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
    retry = copy.deepcopy(data["call_ledger"][0])
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
        empty[field] = copy.deepcopy(data[field])
    empty["confirmation_raw_relation"] = data["raw_relation"]
    return empty


class _WriterFailureConnector:
    def __init__(self, mode: str, as_of: datetime) -> None:
        self.mode = mode
        self.as_of = as_of
        self.calls = 0
        self.diagnostics: dict[str, Any] = {}

    @staticmethod
    def find_broker_symbol(symbol: str) -> str:
        return f"{symbol}.a"

    @staticmethod
    def get_last_error() -> tuple[int, str]:
        return (1, "Success")

    def get_last_history_range_diagnostics(self) -> dict[str, Any]:
        return copy.deepcopy(self.diagnostics)

    def get_bars_range(
        self,
        symbol: str,
        timeframe: str,
        start_date: datetime,
        end_date: datetime,
        *,
        sync_retry: bool = False,
    ) -> pd.DataFrame | None:
        assert sync_retry is True
        self.calls += 1
        if self.mode == "fetch_failed":
            raise TimeoutError("deterministic writer fetch failure")
        if self.mode == "output_storage_resource_limit":
            seconds = TIMEFRAME_SECONDS[timeframe]
            opens = [start_date, end_date - timedelta(seconds=seconds)]
            opens = list(dict.fromkeys(opens))
            frame = _writer_bar_frame(opens)
            self.diagnostics = _fixture_mt5_v4_page_diagnostics(
                symbol=symbol,
                timeframe=timeframe,
                start=start_date,
                end=end_date,
                first_open=opens[0],
                last_open=opens[-1],
                rows=len(opens),
            )
            frame.attrs["mt5_history_range_diagnostics"] = copy.deepcopy(
                self.diagnostics
            )
            return frame
        if self.mode == "partial_terminal_prefix" and self.calls < 3:
            seconds = TIMEFRAME_SECONDS[timeframe]
            opens = [start_date, end_date - timedelta(seconds=seconds)]
            frame = _writer_bar_frame(opens)
            self.diagnostics = _fixture_mt5_v4_page_diagnostics(
                symbol=symbol,
                timeframe=timeframe,
                start=start_date,
                end=end_date,
                first_open=opens[0],
                last_open=opens[-1],
                rows=len(opens),
            )
            frame.attrs["mt5_history_range_diagnostics"] = copy.deepcopy(
                self.diagnostics
            )
            return frame
        if self.mode in {"terminal_prefix_unresolved", "partial_terminal_prefix"}:
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
                    "raw_extent": copy.deepcopy(raw_extent),
                    "history_download_trigger_nonempty": 1,
                }
            )
            for ledger_entry in self.diagnostics["call_ledger"][1:]:
                ledger_entry.update(
                    {
                        "raw_relation": "RIGHT_DISJOINT_PREFIX",
                        "raw_count": 1,
                        "raw_extent": copy.deepcopy(raw_extent),
                    }
                )
            return None
        if self.mode == "no_fully_closed_history" and self.calls == 1:
            opened = self.as_of
            frame = _writer_bar_frame([opened])
            self.diagnostics = _writer_retried_page_diagnostics(
                symbol=symbol,
                timeframe=timeframe,
                start=start_date,
                end=end_date,
                first_open=opened,
                last_open=opened,
                rows=1,
            )
            frame.attrs["mt5_history_range_diagnostics"] = copy.deepcopy(
                self.diagnostics
            )
            return frame
        self.diagnostics = _fixture_mt5_v4_empty_diagnostics(
            symbol=symbol,
            timeframe=timeframe,
            start=start_date,
            end=end_date,
        )
        return None


def _write_fixture(native_root: Path) -> None:
    fixture = build_valid_v5_contract_fixture(symbol=ARTIFACT_SYMBOL)
    manifest = copy.deepcopy(fixture["manifest"])
    source_environment = copy.deepcopy(manifest["source_environment"])
    manifest.update(
        {
            "run_id": RUN_ID,
            "acquisition_started_utc": "2026-07-13T09:00:01.123456Z",
            "acquisition_completed_utc": "2026-07-13T09:00:02.654321Z",
        }
    )

    results: list[dict[str, Any]] = []
    for symbol in PRODUCTION_SYMBOL_ORDER:
        for timeframe in PRODUCTION_TIMEFRAME_ORDER:
            leaf = native_root / "runs" / RUN_ID / symbol / timeframe
            leaf.mkdir(parents=True, exist_ok=False)
            provenance = copy.deepcopy(fixture["provenances"][timeframe])
            observation = fixture["artifact_observations"][timeframe]
            broker_symbol = f"{symbol}.a"
            provenance.update(
                {
                    "run_id": RUN_ID,
                    "symbol": symbol,
                    "canonical_symbol": symbol,
                    "broker_symbol": broker_symbol,
                }
            )
            for attempt in provenance["pagination"]["page_attempts"]:
                diagnostics = attempt["connector_diagnostics"]
                diagnostics["symbol"] = symbol
                diagnostics["broker_symbol"] = broker_symbol
            _compact_connector_bindings(provenance, observation)
            bars_raw = _bars_bytes(timeframe, observation.page_observations)
            bars_sha = hashlib.sha256(bars_raw).hexdigest()
            (leaf / "bars.csv").write_bytes(bars_raw)
            rows = len(bars_raw.splitlines()) - 1
            provenance.update(
                {
                    "instrument": {"status": "unavailable"},
                    "acquisition_started_utc": "2026-07-13T09:00:01.2Z",
                    "acquisition_completed_utc": "2026-07-13T09:00:02Z",
                    "expected_session_coverage_by_partition": {
                        partition: None for partition in PRODUCTION_PARTITIONS
                    },
                    "observed_session_bars_by_partition": {
                        partition: None for partition in PRODUCTION_PARTITIONS
                    },
                    "expected_session_bars_by_partition": {
                        partition: None for partition in PRODUCTION_PARTITIONS
                    },
                    "expected_session_coverage_details_by_partition": {},
                    "quality": _compact_quality(provenance, observation),
                    "artifact": {
                        "relative_path": "bars.csv",
                        "sha256": bars_sha,
                        "bytes": len(bars_raw),
                        "rows": rows,
                    },
                    "sha256": bars_sha,
                    "rows": rows,
                    "columns": list(PRODUCTION_ARTIFACT_COLUMNS),
                }
            )
            result = {
                "symbol": symbol,
                "timeframe": timeframe,
                "status": "success",
                "failure_status": None,
                "rows": rows,
                "sha256": bars_sha,
                "eligible_for_optimization": False,
                "eligible_for_downstream_validation": True,
                "relative_leaf": f"{symbol}/{timeframe}",
                "resume_disposition": "newly_acquired",
            }
            _write_json(leaf / "provenance.json", provenance)
            results.append(result)

    manifest["results"] = results
    manifest["counts"] = {
        "success": len(results),
        "partial": 0,
        "failed": 0,
        "total": len(results),
    }
    manifest["resume"] = {
        "enabled": False,
        "newly_acquired_cells": len(results),
        "reused_cells": 0,
        "stale_staging_removed": 0,
    }
    common_request = {
        key: copy.deepcopy(manifest["request"][key]) for key in COMMON_REQUEST_KEYS
    }
    _write_actual_writer_resume_seed(
        native_root,
        common_request=common_request,
        source_environment=source_environment,
        acquisition_started_utc=manifest["acquisition_started_utc"],
    )
    _write_json(
        native_root / "runs" / RUN_ID / "run_manifest.json",
        manifest,
    )


def _actual_writer_record(
    tmp_path: Path, mode: str
) -> tuple[Path, str, dict[str, Any], dict[str, Any] | None]:
    fixture = build_valid_v5_contract_fixture(symbol=ARTIFACT_SYMBOL)
    common = fixture["manifest"]["request"]
    timeframe = "H4" if mode == "no_fully_closed_history" else "M5"
    timeframes = (
        ("H4", "M5", "M15", "M30", "H1", "D1")
        if timeframe == "H4"
        else PRODUCTION_TIMEFRAME_ORDER
    )
    writer_root = tmp_path / f"writer-{mode}" / "data" / "native"
    store = NativeHistoryStore(writer_root, RUN_ID)
    connector = _WriterFailureConnector(
        mode,
        _parse_utc(common["common_as_of_utc"]),
    )
    request = NativeHistoryRequest(
        symbols=(ARTIFACT_SYMBOL,),
        timeframes=tuple(timeframes),
        horizon_years=common["horizon_years"],
        as_of_utc=_parse_utc(common["requested_information_cutoff_utc"]),
        retries=1,
        retry_sleep_seconds=0.0,
        boundary_tolerance_days=common["boundary_tolerance_days"],
        timeframe_window_years=common["timeframe_window_years"],
        window_contract_version=TIMEFRAME_EVIDENCE_CONTRACT_VERSION,
        embargo_days=common["embargo_days"],
        warmup_days=common["warmup_days"],
        safety_days=common["safety_days"],
        enforce_evidence_horizon=True,
    )
    acquirer = NativeHistoryAcquirer(
        connector,
        store,
        request,
        now_fn=lambda: datetime.fromisoformat("2026-07-13T09:00:01.5+00:00"),
        sleep_fn=lambda _: None,
        runtime_metadata=_writer_source_environment(),
    )
    valid_terminal_snapshot = copy.deepcopy(
        fixture["provenances"][timeframe]["pagination"][
            "initial_terminal_cache_resource_snapshot"
        ]
    )
    valid_output_snapshot = copy.deepcopy(
        fixture["provenances"][timeframe]["output_storage_resource_snapshot"]
    )

    if mode == "terminal_cache_resource_limit":
        terminal_limit = {
            "status": "invalid",
            "data_path": "C:\\FxPM\\TerminalData",
            "violations": ["terminal_data_path_invalid"],
        }

        def enforce_terminal_limit(_broker_symbol: str) -> dict[str, Any]:
            raise TerminalCacheResourceLimitError(copy.deepcopy(terminal_limit))

        acquirer._enforce_terminal_cache_resource_bounds = enforce_terminal_limit
    else:
        acquirer._enforce_terminal_cache_resource_bounds = lambda _symbol: (
            copy.deepcopy(valid_terminal_snapshot)
        )

    if mode == "output_storage_resource_limit":
        store._output_storage_snapshot = lambda _frame: {
            "status": "limit_exceeded",
            "output_path": str(writer_root),
            "disk_total_bytes": 100_000_000_000,
            "disk_free_bytes": 0,
            "minimum_free_bytes": OUTPUT_MIN_FREE_BYTES,
            "maximum_cell_bytes": OUTPUT_MAX_CELL_BYTES,
            "estimated_cell_bytes": 0,
            "sample_rows": 0,
            "violations": ["output_disk_free_reserve_breached"],
        }
    else:
        store._output_storage_snapshot = lambda _frame: copy.deepcopy(
            valid_output_snapshot
        )

    try:
        outcome = acquirer.acquire(max_new_cells=1)
    finally:
        store.close()
    expected_failure = {
        "fetch_failed": "unexpected_acquisition_failure",
        "partial_terminal_prefix": "terminal_prefix_unresolved",
    }.get(mode, mode)
    assert outcome["complete"] is False
    assert outcome["manifest_path"] is None
    assert outcome["resume"]["retryable_attempts"] == 1
    assert len(outcome["results"]) == 1
    result = outcome["results"][0]
    assert result["failure_status"] == expected_failure
    assert result["resume_disposition"] == "retryable_attempt"

    run_root = writer_root / "runs" / RUN_ID
    assert not (run_root / "run_manifest.json").exists()
    assert not (run_root / ARTIFACT_SYMBOL / timeframe).exists()
    attempts = sorted(
        (run_root / "_attempts" / ARTIFACT_SYMBOL / timeframe).glob("attempt-*.json")
    )
    if mode == "output_storage_resource_limit":
        assert attempts == []
        receipt = None
    else:
        assert len(attempts) == 1
        envelope = json.loads(attempts[0].read_text(encoding="utf-8"))
        assert envelope["immutable_record"] is True
        assert envelope["nonfinal"] is True
        assert envelope["retryable"] is True
        assert envelope["run_id"] == RUN_ID
        assert envelope["symbol"] == ARTIFACT_SYMBOL
        assert envelope["timeframe"] == timeframe
        assert envelope["retention_status"] in {"full", "summary_only"}
        receipt = envelope
        assert receipt["record"]["failure_status"] == expected_failure
    return writer_root, timeframe, outcome, receipt


@pytest.fixture(scope="module")
def pristine_native_root(tmp_path_factory: pytest.TempPathFactory) -> Path:
    native_root = tmp_path_factory.mktemp("native-reseal-pristine") / "native"
    native_root.mkdir()
    _write_fixture(native_root)
    return native_root


def _copy_run(pristine_native_root: Path, tmp_path: Path) -> Path:
    target = tmp_path / "native"
    shutil.copytree(pristine_native_root, target)
    return target


def _rebind_artifact(native_root: Path, timeframe: str, raw: bytes) -> None:
    leaf = native_root / "runs" / RUN_ID / ARTIFACT_SYMBOL / timeframe
    (leaf / "bars.csv").write_bytes(raw)
    digest = hashlib.sha256(raw).hexdigest()
    rows = len(raw.splitlines()) - 1
    provenance_path = leaf / "provenance.json"
    provenance = json.loads(provenance_path.read_text(encoding="utf-8"))
    provenance["artifact"].update({"sha256": digest, "bytes": len(raw), "rows": rows})
    provenance["sha256"] = digest
    provenance["rows"] = rows
    _write_json(provenance_path, provenance)
    manifest_path = native_root / "runs" / RUN_ID / "run_manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    result = next(
        item
        for item in manifest["results"]
        if item["symbol"] == ARTIFACT_SYMBOL and item["timeframe"] == timeframe
    )
    result["sha256"] = digest
    result["rows"] = rows
    _write_json(manifest_path, manifest)


def _snapshot(root: Path) -> dict[str, tuple[int, int, str]]:
    return {
        path.relative_to(root).as_posix(): (
            path.stat().st_size,
            path.stat().st_mtime_ns,
            hashlib.sha256(path.read_bytes()).hexdigest(),
        )
        for path in root.rglob("*")
        if path.is_file()
    }


def _envelope(body: dict[str, Any]) -> dict[str, Any]:
    return {
        "schema_version": RUN_RESEAL_ENVELOPE_SCHEMA_VERSION,
        "key_id": KEY_ID,
        "body": body,
        "signature_base64": SIGNATURE_BASE64,
    }


def _trust_root():
    return build_run_reseal_trust_root(
        key_id=KEY_ID,
        public_key_base64=PUBLIC_KEY_BASE64,
    )


def _authority(native_root: Path):
    manifest_path = native_root / "runs" / RUN_ID / "run_manifest.json"
    manifest_raw = manifest_path.read_bytes()
    manifest = json.loads(manifest_raw)
    request = manifest["request"]
    policy = build_trusted_v5_policy(
        expected_symbols=PRODUCTION_SYMBOL_ORDER,
        expected_source_environment=manifest["source_environment"],
        requested_information_cutoff_utc=request["requested_information_cutoff_utc"],
        timeframe_order=PRODUCTION_TIMEFRAME_ORDER,
        retries=request["retries"],
        timeframe_window_years=request["timeframe_window_years"],
        horizon_years=request["horizon_years"],
        embargo_days=request["embargo_days"],
        warmup_days=request["warmup_days"],
        safety_days=request["safety_days"],
        boundary_tolerance_days=request["boundary_tolerance_days"],
    )
    source = canonical_source_identity(manifest["source_environment"])
    return build_run_reseal_expected_authority(
        expected_run_id=RUN_ID,
        expected_origin_manifest_sha256=hashlib.sha256(manifest_raw).hexdigest(),
        trusted_policy=policy,
        expected_trusted_policy_sha256=policy.policy_sha256,
        expected_source_environment=source,
        expected_source_identity_sha256=hashlib.sha256(
            canonical_json_bytes(source)
        ).hexdigest(),
        expected_requested_information_cutoff_utc=request[
            "requested_information_cutoff_utc"
        ],
        expected_common_as_of_utc=request["common_as_of_utc"],
        expected_timeframe_request_map_sha256=request["timeframe_request_map_sha256"],
    )


def test_actual_writer_v2_seed_matches_reseal_contract(
    pristine_native_root: Path,
) -> None:
    run_root = pristine_native_root / "runs" / RUN_ID
    seed = json.loads((run_root / "resume_contract.json").read_bytes())
    manifest = json.loads((run_root / "run_manifest.json").read_bytes())
    common_request = {
        key: copy.deepcopy(manifest["request"][key]) for key in COMMON_REQUEST_KEYS
    }
    source_identity = canonical_source_identity(manifest["source_environment"])

    assert seed["source_identity"] == source_identity
    reseal._validate_resume_seed(
        seed,
        run_id=RUN_ID,
        common_request=common_request,
        source_identity=source_identity,
        acquisition_started_utc=manifest["acquisition_started_utc"],
    )


def test_reseal_uses_shared_aggregate_artifact_cap(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    assert reseal.MAX_TOTAL_ARTIFACT_BYTES is MAX_TOTAL_ARTIFACT_BYTES
    assert reseal._run_reseal_runtime_intact() is True

    monkeypatch.setattr(
        reseal,
        "MAX_TOTAL_ARTIFACT_BYTES",
        MAX_TOTAL_ARTIFACT_BYTES + 1,
    )
    assert reseal._run_reseal_runtime_intact() is False


def test_materializes_exact_production_grid_without_mutation(
    pristine_native_root: Path,
) -> None:
    before = _snapshot(pristine_native_root)
    body = materialize_run_reseal_body(
        pristine_native_root,
        expected_authority=_authority(pristine_native_root),
        issued_at_utc=ISSUED_AT,
    )
    after = _snapshot(pristine_native_root)

    assert before == after
    assert body["cell_count"] == 600
    assert body["artifact_cell_count"] == 600
    assert body["symbols"] == list(PRODUCTION_SYMBOL_ORDER)
    assert body["symbol_order_sha256"] == PRODUCTION_SYMBOL_ORDER_SHA256
    assert body["timeframes"] == list(PRODUCTION_TIMEFRAME_ORDER)
    assert body["timeframe_order_sha256"] == PRODUCTION_TIMEFRAME_ORDER_SHA256
    assert body["acquisition_started_utc"].endswith(".123456Z")
    assert body["acquisition_completed_utc"].endswith(".654321Z")
    assert [(cell["symbol"], cell["timeframe"]) for cell in body["cells"]] == [
        (symbol, timeframe)
        for symbol in PRODUCTION_SYMBOL_ORDER
        for timeframe in PRODUCTION_TIMEFRAME_ORDER
    ]
    assert all(cell["artifact_present"] is True for cell in body["cells"])
    assert all(cell["provenance_request_interval_bound"] for cell in body["cells"])


def test_materialization_streams_bars_and_invokes_v5_for_every_publishable_cell(
    pristine_native_root: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original_read = reseal._read_preflighted
    original_validate = reseal.validate_v5_cell_status
    validated: list[tuple[str, str]] = []

    def bounded_read(value: Any, *, code: str) -> bytes:
        assert value.path.name != "bars.csv"
        return original_read(value, code=code)

    def recording_validate(*args: Any, **kwargs: Any) -> str:
        validated.append((kwargs["symbol"], kwargs["timeframe"]))
        return original_validate(*args, **kwargs)

    monkeypatch.setattr(reseal, "_read_preflighted", bounded_read)
    monkeypatch.setattr(reseal, "validate_v5_cell_status", recording_validate)
    materialize_run_reseal_body(
        pristine_native_root,
        expected_authority=_authority(pristine_native_root),
        issued_at_utc=ISSUED_AT,
    )

    assert validated == [
        (symbol, timeframe)
        for symbol in PRODUCTION_SYMBOL_ORDER
        for timeframe in PRODUCTION_TIMEFRAME_ORDER
    ]


@pytest.mark.parametrize(
    ("mode", "expected_failure", "receipt_expected"),
    [
        (
            "terminal_history_empty_unresolved",
            "terminal_history_empty_unresolved",
            True,
        ),
        ("terminal_prefix_unresolved", "terminal_prefix_unresolved", True),
        ("fetch_failed", "unexpected_acquisition_failure", True),
        ("no_fully_closed_history", "no_fully_closed_history", True),
        ("terminal_cache_resource_limit", "terminal_cache_resource_limit", True),
        ("output_storage_resource_limit", "output_storage_resource_limit", False),
    ],
)
def test_actual_writer_failure_remains_nonfinal_and_outside_reseal(
    tmp_path: Path,
    mode: str,
    expected_failure: str,
    receipt_expected: bool,
) -> None:
    writer_root, timeframe, outcome, receipt = _actual_writer_record(tmp_path, mode)

    assert outcome["results"][0]["status"] == "failed"
    assert outcome["results"][0]["failure_status"] == expected_failure
    assert (receipt is not None) is receipt_expected
    assert not (writer_root / "runs" / RUN_ID / ARTIFACT_SYMBOL / timeframe).exists()
    assert not (writer_root / "runs" / RUN_ID / "run_manifest.json").exists()
    if receipt is not None:
        record = receipt["record"]
        assert record["status"] == "failed"
        assert record["failure_status"] == expected_failure
        if receipt["retention_status"] == "full":
            assert record["artifact"] is None
            assert record["rows"] == 0
            assert record["complete"] is False


def test_actual_writer_technical_partial_remains_nonfinal_and_retryable(
    tmp_path: Path,
) -> None:
    writer_root, timeframe, outcome, receipt = _actual_writer_record(
        tmp_path, "partial_terminal_prefix"
    )

    assert outcome["results"][0]["status"] == "partial"
    assert receipt is not None
    record = receipt["record"]
    assert record["status"] == "partial"
    assert record["failure_status"] == "terminal_prefix_unresolved"
    if receipt["retention_status"] == "full":
        assert record["artifact"] is None
        assert record["rows"] == 0
        assert record["complete"] is False
    assert not (writer_root / "runs" / RUN_ID / ARTIFACT_SYMBOL / timeframe).exists()
    assert not (writer_root / "runs" / RUN_ID / "run_manifest.json").exists()


def test_signing_request_exposes_exact_domain_separated_payload(
    pristine_native_root: Path,
) -> None:
    request = build_run_reseal_signing_request(
        pristine_native_root,
        expected_authority=_authority(pristine_native_root),
        authority_key_id=KEY_ID,
        issued_at_utc=ISSUED_AT,
    )
    payload = base64.b64decode(request["signature_payload_base64"], validate=True)
    expected = RUN_RESEAL_SIGNATURE_DOMAIN + canonical_json_bytes(
        {"key_id": KEY_ID, "body": request["body"]}
    )
    assert payload == expected
    assert request["signature_payload_sha256"] == hashlib.sha256(payload).hexdigest()
    assert (
        request["body_sha256"]
        == hashlib.sha256(canonical_json_bytes(request["body"])).hexdigest()
    )
    assert request["grants_trading_authority"] is False
    assert request["grants_statistical_sufficiency_authority"] is False
    assert request["grants_profitability_authority"] is False


def test_expected_authority_mapping_is_exact_and_candidate_independent(
    pristine_native_root: Path,
) -> None:
    authority = _authority(pristine_native_root)
    mapping = run_reseal_expected_authority_to_mapping(authority)
    assert run_reseal_expected_authority_from_mapping(mapping) == authority

    forged_source = copy.deepcopy(mapping)
    forged_source["source_identity_sha256"] = "0" * 64
    with pytest.raises(
        NativeRunResealError,
        match="RUN_RESEAL_EXPECTED_SOURCE_IDENTITY_HASH_MISMATCH",
    ):
        run_reseal_expected_authority_from_mapping(forged_source)

    forged_cutoff = copy.deepcopy(mapping)
    forged_cutoff["requested_information_cutoff_utc"] = "2026-07-13T08:55:00Z"
    with pytest.raises(
        NativeRunResealError,
        match="RUN_RESEAL_EXPECTED_AUTHORITY_COMMON_REQUEST_MISMATCH",
    ):
        run_reseal_expected_authority_from_mapping(forged_cutoff)


@pytest.mark.parametrize(
    ("mutation", "error"),
    [
        (
            lambda seed: seed.update(symbols=list(reversed(seed["symbols"]))),
            "RUN_RESEAL_RESUME_SEED_ROSTER_MISMATCH",
        ),
        (
            lambda seed: seed.update(timeframes=list(reversed(seed["timeframes"]))),
            "RUN_RESEAL_RESUME_SEED_ROSTER_MISMATCH",
        ),
        (
            lambda seed: seed.update(production_authoritative=False),
            "RUN_RESEAL_RESUME_SEED_PRODUCTION_INTENT_INVALID",
        ),
        (
            lambda seed: seed.update(
                schema_version="fxpm.native_history.resume_seed.v1"
            ),
            "RUN_RESEAL_RESUME_SEED_INVALID",
        ),
    ],
)
def test_resume_seed_roster_order_and_production_intent_are_mandatory(
    pristine_native_root: Path,
    tmp_path: Path,
    mutation: Any,
    error: str,
) -> None:
    native_root = _copy_run(pristine_native_root, tmp_path)
    seed_path = native_root / "runs" / RUN_ID / "resume_contract.json"
    seed = json.loads(seed_path.read_text(encoding="utf-8"))
    mutation(seed)
    _write_json(seed_path, seed)

    with pytest.raises(NativeRunResealError, match=error):
        materialize_run_reseal_body(
            native_root,
            expected_authority=_authority(native_root),
            issued_at_utc=ISSUED_AT,
        )


def test_manifest_roster_order_and_case_are_fail_closed(
    pristine_native_root: Path,
    tmp_path: Path,
) -> None:
    native_root = _copy_run(pristine_native_root, tmp_path)
    manifest_path = native_root / "runs" / RUN_ID / "run_manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["results"][0]["symbol"] = manifest["results"][0]["symbol"].lower()
    _write_json(manifest_path, manifest)

    with pytest.raises(
        NativeRunResealError, match="RUN_RESEAL_EXPECTED_MANIFEST_HASH_MISMATCH"
    ):
        materialize_run_reseal_body(
            native_root,
            expected_authority=_authority(pristine_native_root),
            issued_at_utc=ISSUED_AT,
        )


def test_valid_artifact_cannot_be_downgraded_to_writer_failure_under_old_authority(
    pristine_native_root: Path,
    tmp_path: Path,
) -> None:
    expected_authority = _authority(pristine_native_root)
    native_root = _copy_run(pristine_native_root, tmp_path)
    manifest_path = native_root / "runs" / RUN_ID / "run_manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["results"][0].update(
        status="failed",
        failure_status="terminal_history_empty_unresolved",
        rows=0,
        sha256=None,
        eligible_for_downstream_validation=False,
        resume_disposition="retryable_attempt",
    )
    _write_json(manifest_path, manifest)
    with pytest.raises(
        NativeRunResealError,
        match="RUN_RESEAL_EXPECTED_MANIFEST_HASH_MISMATCH",
    ):
        materialize_run_reseal_body(
            native_root,
            expected_authority=expected_authority,
            issued_at_utc=ISSUED_AT,
        )


def test_artifact_tamper_is_rejected_against_provenance_and_result(
    pristine_native_root: Path,
    tmp_path: Path,
) -> None:
    native_root = _copy_run(pristine_native_root, tmp_path)
    bars = native_root / "runs" / RUN_ID / ARTIFACT_SYMBOL / "M5" / "bars.csv"
    raw = bars.read_bytes()
    bars.write_bytes(raw.replace(b"1.00000", b"9.00000", 1))

    with pytest.raises(
        NativeRunResealError,
        match="V5_ARTIFACT_QUALITY_FAILED:invalid_ohlc_count",
    ):
        materialize_run_reseal_body(
            native_root,
            expected_authority=_authority(pristine_native_root),
            issued_at_utc=ISSUED_AT,
        )


@pytest.mark.parametrize(
    ("mutation", "error"),
    [
        ("fractional_volume", "RUN_RESEAL_BARS_NONINTEGRAL_MT5_METRIC"),
        ("oversized_volume", "RUN_RESEAL_BARS_MT5_METRIC_OUT_OF_RANGE"),
        ("off_grid_open", "RUN_RESEAL_BARS_FIXED_CLOCK_OFF_NATIVE_GRID"),
    ],
)
def test_shared_scanner_semantics_are_enforced_by_full_reseal_cell(
    pristine_native_root: Path,
    tmp_path: Path,
    mutation: str,
    error: str,
) -> None:
    native_root = _copy_run(pristine_native_root, tmp_path)
    bars = native_root / "runs" / RUN_ID / ARTIFACT_SYMBOL / "M5" / "bars.csv"
    raw = bars.read_bytes()
    if mutation == "fractional_volume":
        changed = raw.replace(b",1,0,1,", b",0.5,0,1,", 1)
    elif mutation == "oversized_volume":
        changed = raw.replace(b",1,0,1,", b",1e308,0,1,", 1)
    else:
        lines = raw.decode("utf-8").splitlines()
        fields = lines[1].split(",")
        opened = _parse_utc(fields[0]) + timedelta(minutes=1)
        fields[0] = _csv_second(opened)
        lines[1] = ",".join(fields)
        changed = ("\n".join(lines) + "\n").encode("utf-8")
    _rebind_artifact(native_root, "M5", changed)

    with pytest.raises(NativeRunResealError, match=error):
        materialize_run_reseal_body(
            native_root,
            expected_authority=_authority(native_root),
            issued_at_utc=ISSUED_AT,
        )


def test_matched_hash_invalid_ohlc_cannot_be_resealed(
    pristine_native_root: Path,
    tmp_path: Path,
) -> None:
    native_root = _copy_run(pristine_native_root, tmp_path)
    bars = native_root / "runs" / RUN_ID / ARTIFACT_SYMBOL / "M5" / "bars.csv"
    lines = bars.read_text(encoding="utf-8").splitlines()
    fields = lines[2].split(",")
    fields[2] = "0.50000"
    lines[2] = ",".join(fields)
    _rebind_artifact(native_root, "M5", ("\n".join(lines) + "\n").encode())

    with pytest.raises(
        NativeRunResealError,
        match="V5_ARTIFACT_QUALITY_FAILED:invalid_ohlc_count",
    ):
        materialize_run_reseal_body(
            native_root,
            expected_authority=_authority(native_root),
            issued_at_utc=ISSUED_AT,
        )


def test_matched_hash_non_monotonic_rows_cannot_be_resealed(
    pristine_native_root: Path,
    tmp_path: Path,
) -> None:
    native_root = _copy_run(pristine_native_root, tmp_path)
    bars = native_root / "runs" / RUN_ID / ARTIFACT_SYMBOL / "M5" / "bars.csv"
    lines = bars.read_text(encoding="utf-8").splitlines()
    lines[2], lines[3] = lines[3], lines[2]
    _rebind_artifact(native_root, "M5", ("\n".join(lines) + "\n").encode())

    with pytest.raises(
        NativeRunResealError,
        match="V5_ARTIFACT_QUALITY_FAILED:non_monotonic_open_count",
    ):
        materialize_run_reseal_body(
            native_root,
            expected_authority=_authority(native_root),
            issued_at_utc=ISSUED_AT,
        )


def test_matched_hash_invalid_bar_close_cannot_be_resealed(
    pristine_native_root: Path,
    tmp_path: Path,
) -> None:
    native_root = _copy_run(pristine_native_root, tmp_path)
    bars = native_root / "runs" / RUN_ID / ARTIFACT_SYMBOL / "M5" / "bars.csv"
    lines = bars.read_text(encoding="utf-8").splitlines()
    fields = lines[2].split(",")
    invalid_close = _parse_utc(fields[0]) + timedelta(seconds=60)
    fields[9] = _source_last(invalid_close)
    fields[10] = _csv_second(invalid_close)
    lines[2] = ",".join(fields)
    _rebind_artifact(native_root, "M5", ("\n".join(lines) + "\n").encode())

    with pytest.raises(
        NativeRunResealError,
        match="V5_ARTIFACT_QUALITY_FAILED:invalid_bar_close_count",
    ):
        materialize_run_reseal_body(
            native_root,
            expected_authority=_authority(native_root),
            issued_at_utc=ISSUED_AT,
        )


def test_matched_hash_out_of_page_row_cannot_be_resealed(
    pristine_native_root: Path,
    tmp_path: Path,
) -> None:
    native_root = _copy_run(pristine_native_root, tmp_path)
    bars = native_root / "runs" / RUN_ID / ARTIFACT_SYMBOL / "M5" / "bars.csv"
    lines = bars.read_text(encoding="utf-8").splitlines()
    fields = lines[2].split(",")
    outside_open = datetime.fromisoformat("2027-01-01T00:00:00+00:00")
    outside_close = outside_open + timedelta(minutes=5)
    fields[0] = _csv_second(outside_open)
    fields[8] = fields[0]
    fields[9] = _source_last(outside_close)
    fields[10] = _csv_second(outside_close)
    lines[2] = ",".join(fields)
    _rebind_artifact(native_root, "M5", ("\n".join(lines) + "\n").encode())

    with pytest.raises(
        NativeRunResealError,
        match="V5_ARTIFACT_PAGE_ROWS_TOTAL_INVALID",
    ):
        materialize_run_reseal_body(
            native_root,
            expected_authority=_authority(native_root),
            issued_at_utc=ISSUED_AT,
        )


def test_failed_leaf_cannot_smuggle_an_economic_authority_claim(
    pristine_native_root: Path,
    tmp_path: Path,
) -> None:
    native_root = _copy_run(pristine_native_root, tmp_path)
    provenance_path = (
        native_root
        / "runs"
        / RUN_ID
        / PRODUCTION_SYMBOL_ORDER[1]
        / "M5"
        / "provenance.json"
    )
    provenance = json.loads(provenance_path.read_text(encoding="utf-8"))
    provenance["source_unavailable_proof"] = True
    _write_json(provenance_path, provenance)

    with pytest.raises(
        NativeRunResealError,
        match="RUN_RESEAL_PROVENANCE_AUTHORITY_FIELD_PRESENT",
    ):
        materialize_run_reseal_body(
            native_root,
            expected_authority=_authority(pristine_native_root),
            issued_at_utc=ISSUED_AT,
        )


def test_success_leaf_rejects_even_false_valued_authority_field(
    pristine_native_root: Path,
    tmp_path: Path,
) -> None:
    native_root = _copy_run(pristine_native_root, tmp_path)
    provenance_path = (
        native_root / "runs" / RUN_ID / ARTIFACT_SYMBOL / "M5" / "provenance.json"
    )
    provenance = json.loads(provenance_path.read_text(encoding="utf-8"))
    provenance["grants_trading_authority"] = False
    _write_json(provenance_path, provenance)
    with pytest.raises(
        NativeRunResealError,
        match="RUN_RESEAL_PROVENANCE_AUTHORITY_FIELD_PRESENT",
    ):
        materialize_run_reseal_body(
            native_root,
            expected_authority=_authority(pristine_native_root),
            issued_at_utc=ISSUED_AT,
        )


def test_directory_entry_cap_stops_at_first_excess_entry(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    maximum_entries = 3

    class AdversarialScandir:
        def __init__(self) -> None:
            self.pulls = 0
            self.closed = False

        def __enter__(self) -> "AdversarialScandir":
            return self

        def __exit__(self, *_args: Any) -> None:
            self.closed = True

        def __iter__(self) -> "AdversarialScandir":
            return self

        def __next__(self) -> Any:
            self.pulls += 1
            if self.pulls > maximum_entries + 1:
                raise AssertionError("directory tail was consumed past cap+1")
            return type("Entry", (), {"name": f"entry-{self.pulls}"})()

    scan = AdversarialScandir()

    def adversarial_scandir(path: Any) -> AdversarialScandir:
        assert Path(path) == tmp_path
        return scan

    monkeypatch.setattr(reseal.os, "scandir", adversarial_scandir)
    with pytest.raises(
        NativeRunResealError,
        match="RUN_RESEAL_ADVERSARIAL_DIRECTORY_ENTRY_CAP_EXCEEDED",
    ):
        reseal._bounded_directory_entries(
            tmp_path,
            maximum_entries=maximum_entries,
            code="RUN_RESEAL_ADVERSARIAL_DIRECTORY",
        )

    assert scan.pulls == maximum_entries + 1
    assert scan.closed is True


def test_external_json_preparse_depth_and_node_budgets_stop_before_decode(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def decoding_is_forbidden(*_args: Any, **_kwargs: Any) -> Any:
        raise AssertionError("json.loads ran before the structural budget gate")

    monkeypatch.setattr(reseal.json, "loads", decoding_is_forbidden)
    deep = (
        b'{"value":' * (reseal.MAX_EXTERNAL_JSON_DEPTH + 1)
        + b"0"
        + b"}" * (reseal.MAX_EXTERNAL_JSON_DEPTH + 1)
    )
    deep_path = tmp_path / "deep.json"
    deep_path.write_bytes(deep)
    with pytest.raises(
        NativeRunResealError,
        match="RUN_RESEAL_EXTERNAL_JSON_DEPTH_CAP_EXCEEDED",
    ):
        reseal.load_bounded_json_file(deep_path)

    wide = (
        b'{"values":['
        + b",".join(b"0" for _ in range(reseal.MAX_EXTERNAL_JSON_STRUCTURAL_TOKENS + 1))
        + b"]}"
    )
    wide_path = tmp_path / "wide.json"
    wide_path.write_bytes(wide)
    with pytest.raises(
        NativeRunResealError,
        match="RUN_RESEAL_EXTERNAL_JSON_NODE_CAP_EXCEEDED",
    ):
        reseal.load_bounded_json_file(wide_path)


def test_external_json_byte_cap_cannot_be_raised_by_caller(tmp_path: Path) -> None:
    with pytest.raises(
        NativeRunResealError,
        match="RUN_RESEAL_EXTERNAL_JSON_BYTE_CAP_INVALID",
    ):
        reseal.load_bounded_json_file(
            tmp_path / "missing.json",
            maximum_bytes=reseal.MAX_EXTERNAL_JSON_BYTES + 1,
        )


def test_even_empty_attempt_receipt_root_is_not_resealable(
    pristine_native_root: Path,
    tmp_path: Path,
) -> None:
    native_root = _copy_run(pristine_native_root, tmp_path)
    (native_root / "runs" / RUN_ID / "_attempts").mkdir()

    with pytest.raises(
        NativeRunResealError,
        match="RUN_RESEAL_ATTEMPT_RECEIPTS_PRESENT",
    ):
        materialize_run_reseal_body(
            native_root,
            expected_authority=_authority(native_root),
            issued_at_utc=ISSUED_AT,
        )


def test_path_traversal_and_hardlinks_are_rejected(
    pristine_native_root: Path,
    tmp_path: Path,
) -> None:
    native_root = _copy_run(pristine_native_root, tmp_path)
    provenance = (
        native_root
        / "runs"
        / RUN_ID
        / PRODUCTION_SYMBOL_ORDER[1]
        / "M5"
        / "provenance.json"
    )
    alias_source = tmp_path / "alias-source.json"
    shutil.copyfile(provenance, alias_source)
    provenance.unlink()
    os.link(alias_source, provenance)
    with pytest.raises(NativeRunResealError, match="RUN_RESEAL_PROVENANCE_UNSAFE"):
        materialize_run_reseal_body(
            native_root,
            expected_authority=_authority(pristine_native_root),
            issued_at_utc=ISSUED_AT,
        )


def test_symlinked_native_root_is_rejected(
    pristine_native_root: Path,
    tmp_path: Path,
) -> None:
    linked = tmp_path / "linked-native"
    try:
        linked.symlink_to(pristine_native_root, target_is_directory=True)
    except OSError as exc:
        pytest.skip(f"directory symlinks unavailable: {exc}")
    with pytest.raises(NativeRunResealError, match="RUN_RESEAL_NATIVE_ROOT_UNSAFE"):
        materialize_run_reseal_body(
            linked,
            expected_authority=_authority(pristine_native_root),
            issued_at_utc=ISSUED_AT,
        )


def test_preflight_to_open_race_is_rejected(
    pristine_native_root: Path,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    native_root = _copy_run(pristine_native_root, tmp_path)
    original_scan = reseal._scan_bars
    mutated = False

    def racing_scan(value: Any, **kwargs: Any):
        nonlocal mutated
        if not mutated:
            mutated = True
            value.path.write_bytes(value.path.read_bytes() + b"\n")
        return original_scan(value, **kwargs)

    monkeypatch.setattr(reseal, "_scan_bars", racing_scan)
    with pytest.raises(NativeRunResealError, match="RUN_RESEAL_BARS_IDENTITY_CHANGED"):
        materialize_run_reseal_body(
            native_root,
            expected_authority=_authority(pristine_native_root),
            issued_at_utc=ISSUED_AT,
        )


def test_attempt_receipts_inserted_after_final_topology_scan_are_rejected(
    pristine_native_root: Path,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    native_root = _copy_run(pristine_native_root, tmp_path)
    run_root = native_root / "runs" / RUN_ID
    attempts_root = run_root / "_attempts"
    original_scandir = reseal.os.scandir
    root_scans = 0

    class InjectingScandir:
        def __init__(self, inner: Any, *, inject_on_exit: bool) -> None:
            self.inner = inner
            self.inject_on_exit = inject_on_exit

        def __enter__(self) -> Any:
            return self.inner.__enter__()

        def __exit__(self, *args: Any) -> Any:
            result = self.inner.__exit__(*args)
            if self.inject_on_exit:
                attempts_root.mkdir()
            return result

    def racing_scandir(path: Any) -> InjectingScandir:
        nonlocal root_scans
        is_run_root = Path(path) == run_root
        if is_run_root:
            root_scans += 1
        return InjectingScandir(
            original_scandir(path),
            inject_on_exit=is_run_root and root_scans == 2,
        )

    monkeypatch.setattr(reseal.os, "scandir", racing_scandir)
    with pytest.raises(
        NativeRunResealError,
        match="RUN_RESEAL_DIRECTORY_IDENTITY_CHANGED",
    ):
        materialize_run_reseal_body(
            native_root,
            expected_authority=_authority(pristine_native_root),
            issued_at_utc=ISSUED_AT,
        )

    assert root_scans == 2
    assert attempts_root.is_dir()


def test_static_external_envelope_verifies_and_returns_non_authoritative_receipt(
    pristine_native_root: Path,
) -> None:
    body = materialize_run_reseal_body(
        pristine_native_root,
        expected_authority=_authority(pristine_native_root),
        issued_at_utc=ISSUED_AT,
    )
    verified = verify_run_reseal_envelope(
        pristine_native_root,
        _envelope(body),
        trust_root=_trust_root(),
        expected_authority=_authority(pristine_native_root),
        validation_time_utc=VALIDATION_TIME,
    )
    receipt = verified.to_receipt()
    assert receipt["run_id"] == RUN_ID
    assert receipt["cell_count"] == 600
    assert receipt["grants_trading_authority"] is False
    assert receipt["grants_statistical_sufficiency_authority"] is False
    assert receipt["grants_profitability_authority"] is False


def test_wrong_signature_key_and_replay_are_rejected(
    pristine_native_root: Path,
    tmp_path: Path,
) -> None:
    body = materialize_run_reseal_body(
        pristine_native_root,
        expected_authority=_authority(pristine_native_root),
        issued_at_utc=ISSUED_AT,
    )
    tampered_signature = bytearray(base64.b64decode(SIGNATURE_BASE64, validate=True))
    tampered_signature[0] ^= 1
    envelope = _envelope(body)
    envelope["signature_base64"] = base64.b64encode(tampered_signature).decode()
    with pytest.raises(NativeRunResealError, match="RUN_RESEAL_SIGNATURE_INVALID"):
        verify_run_reseal_envelope(
            pristine_native_root,
            envelope,
            trust_root=_trust_root(),
            expected_authority=_authority(pristine_native_root),
            validation_time_utc=VALIDATION_TIME,
        )

    undomained = _envelope(body)
    undomained["signature_base64"] = UNDOMAINED_SIGNATURE_BASE64
    with pytest.raises(NativeRunResealError, match="RUN_RESEAL_SIGNATURE_INVALID"):
        verify_run_reseal_envelope(
            pristine_native_root,
            undomained,
            trust_root=_trust_root(),
            expected_authority=_authority(pristine_native_root),
            validation_time_utc=VALIDATION_TIME,
        )

    tampered_body = copy.deepcopy(body)
    tampered_body["issued_at_utc"] = "2026-07-13T10:00:01Z"
    with pytest.raises(NativeRunResealError, match="RUN_RESEAL_SIGNATURE_INVALID"):
        verify_run_reseal_envelope(
            pristine_native_root,
            _envelope(tampered_body),
            trust_root=_trust_root(),
            expected_authority=_authority(pristine_native_root),
            validation_time_utc=VALIDATION_TIME,
        )

    wrong_key_id = _envelope(body)
    wrong_key_id["key_id"] = "untrusted-key-id"
    with pytest.raises(
        NativeRunResealError,
        match="RUN_RESEAL_ENVELOPE_KEY_ID_MISMATCH",
    ):
        verify_run_reseal_envelope(
            pristine_native_root,
            wrong_key_id,
            trust_root=_trust_root(),
            expected_authority=_authority(pristine_native_root),
            validation_time_utc=VALIDATION_TIME,
        )

    relabelled = _envelope(body)
    relabelled["key_id"] = "relabeled-same-material"
    with pytest.raises(NativeRunResealError, match="RUN_RESEAL_SIGNATURE_INVALID"):
        verify_run_reseal_envelope(
            pristine_native_root,
            relabelled,
            trust_root=build_run_reseal_trust_root(
                key_id="relabeled-same-material",
                public_key_base64=PUBLIC_KEY_BASE64,
            ),
            expected_authority=_authority(pristine_native_root),
            validation_time_utc=VALIDATION_TIME,
        )

    with pytest.raises(NativeRunResealError, match="RUN_RESEAL_FUTURE_ISSUE_TIME"):
        verify_run_reseal_envelope(
            pristine_native_root,
            _envelope(body),
            trust_root=_trust_root(),
            expected_authority=_authority(pristine_native_root),
            validation_time_utc="2026-07-13T09:59:59Z",
        )

    wrong_key = base64.b64encode(bytes(range(32))).decode()
    with pytest.raises(NativeRunResealError, match="RUN_RESEAL_SIGNATURE_INVALID"):
        verify_run_reseal_envelope(
            pristine_native_root,
            _envelope(body),
            trust_root=build_run_reseal_trust_root(
                key_id=KEY_ID,
                public_key_base64=wrong_key,
            ),
            expected_authority=_authority(pristine_native_root),
            validation_time_utc=VALIDATION_TIME,
        )

    native_root = _copy_run(pristine_native_root, tmp_path)
    provenance = (
        native_root
        / "runs"
        / RUN_ID
        / PRODUCTION_SYMBOL_ORDER[1]
        / "M5"
        / "provenance.json"
    )
    value = json.loads(provenance.read_text(encoding="utf-8"))
    value["externally_tampered"] = True
    _write_json(provenance, value)
    with pytest.raises(
        NativeRunResealError,
        match="RUN_RESEAL_PUBLISHABLE_PROVENANCE_SCHEMA_INVALID",
    ):
        verify_run_reseal_envelope(
            native_root,
            _envelope(body),
            trust_root=_trust_root(),
            expected_authority=_authority(pristine_native_root),
            validation_time_utc=VALIDATION_TIME,
        )


def test_cli_emit_and_verify_are_stdout_only(
    pristine_native_root: Path,
    tmp_path: Path,
) -> None:
    project_root = Path(__file__).resolve().parents[1]
    tool = project_root / "tools" / "materialize_native_run_reseal.py"
    before = _snapshot(pristine_native_root)
    expected_authority_path = tmp_path / "expected-authority.json"
    _write_json(
        expected_authority_path,
        run_reseal_expected_authority_to_mapping(_authority(pristine_native_root)),
    )
    emitted = subprocess.run(
        [
            sys.executable,
            str(tool),
            "emit",
            "--native-root",
            str(pristine_native_root),
            "--expected-authority",
            str(expected_authority_path),
            "--authority-key-id",
            KEY_ID,
            "--issued-at-utc",
            ISSUED_AT,
        ],
        cwd=project_root,
        check=True,
        capture_output=True,
        text=True,
    )
    request = json.loads(emitted.stdout)
    envelope_path = tmp_path / "envelope.json"
    trust_root_path = tmp_path / "trust-root.json"
    _write_json(envelope_path, _envelope(request["body"]))
    _write_json(
        trust_root_path,
        {
            "schema_version": RUN_RESEAL_TRUST_ROOT_SCHEMA_VERSION,
            "key_id": KEY_ID,
            "public_key_base64": PUBLIC_KEY_BASE64,
        },
    )
    verified = subprocess.run(
        [
            sys.executable,
            str(tool),
            "verify",
            "--native-root",
            str(pristine_native_root),
            "--expected-authority",
            str(expected_authority_path),
            "--envelope",
            str(envelope_path),
            "--trust-root",
            str(trust_root_path),
            "--validation-time-utc",
            VALIDATION_TIME,
        ],
        cwd=project_root,
        check=True,
        capture_output=True,
        text=True,
    )
    receipt = json.loads(verified.stdout)
    assert receipt["run_id"] == RUN_ID
    assert receipt["cell_count"] == 600
    assert _snapshot(pristine_native_root) == before


def test_runtime_contains_no_signing_primitive() -> None:
    project_root = Path(__file__).resolve().parents[1]
    source = "\n".join(
        path.read_text(encoding="utf-8")
        for path in (
            project_root / "pm_native_run_reseal.py",
            project_root / "tools" / "materialize_native_run_reseal.py",
        )
    )
    assert "Ed25519PrivateKey" not in source
    assert ".sign(" not in source
    assert "load_pem_private_key" not in source


def test_verified_receipt_cannot_be_constructed_or_mutated() -> None:
    with pytest.raises(TypeError, match="verifier-constructed"):
        VerifiedRunReseal(  # type: ignore[call-arg]
            schema_version="forged",
            run_id=RUN_ID,
            key_id=KEY_ID,
            body_sha256="0" * 64,
            envelope_sha256="0" * 64,
            origin_manifest_sha256="0" * 64,
            expected_authority_sha256="0" * 64,
            trusted_policy_sha256="0" * 64,
            cell_count=600,
            artifact_cell_count=600,
            issued_at_utc=ISSUED_AT,
            validated_at_utc=VALIDATION_TIME,
            body={},
        )


@pytest.mark.parametrize(
    "name",
    ["_scan_bars", "_run_reseal_signature_payload", "Ed25519PublicKey"],
)
def test_verifier_rejects_runtime_helper_rebinding(
    pristine_native_root: Path,
    monkeypatch: pytest.MonkeyPatch,
    name: str,
) -> None:
    body = materialize_run_reseal_body(
        pristine_native_root,
        expected_authority=_authority(pristine_native_root),
        issued_at_utc=ISSUED_AT,
    )
    monkeypatch.setattr(reseal, name, object())
    with pytest.raises(
        NativeRunResealError,
        match="RUN_RESEAL_RUNTIME_INTEGRITY_INVALID",
    ):
        verify_run_reseal_envelope(
            pristine_native_root,
            _envelope(body),
            trust_root=_trust_root(),
            expected_authority=_authority(pristine_native_root),
            validation_time_utc=VALIDATION_TIME,
        )
