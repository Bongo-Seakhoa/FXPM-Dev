from __future__ import annotations

from datetime import datetime, timezone

from pm_evidence import TIMEFRAME_EVIDENCE_CONTRACT_VERSION
from tools.acquire_native_history import (
    DEFAULT_TIMEFRAMES,
    NativeHistoryAcquirer,
    NativeHistoryRequest,
    NativeHistoryStore,
)
from tools.audit_activate_native_history import _v5_request_errors
from tests.native_v5_support import SOURCE_ENVIRONMENT


class _MetadataOnlyConnector:
    def get_runtime_metadata(self):
        return SOURCE_ENVIRONMENT


def _generated_v5_request(tmp_path, as_of: datetime):
    store = NativeHistoryStore(tmp_path / "data" / "native", "request-audit-parity")
    request = NativeHistoryRequest(
        symbols=("EURUSD",),
        timeframes=DEFAULT_TIMEFRAMES,
        as_of_utc=as_of,
        retries=1,
        retry_sleep_seconds=0.0,
        timeframe_window_years={},
        window_contract_version=TIMEFRAME_EVIDENCE_CONTRACT_VERSION,
    )
    acquirer = NativeHistoryAcquirer(
        _MetadataOnlyConnector(),
        store,
        request,
        now_fn=lambda: as_of,
        sleep_fn=lambda _: None,
    )
    try:
        return acquirer._common_request_contract()
    finally:
        store.close()


def test_generated_v5_contract_passes_offline_request_validator(tmp_path):
    request = _generated_v5_request(
        tmp_path,
        datetime(2026, 7, 12, 21, 47, 19, tzinfo=timezone.utc),
    )

    assert request["common_as_of_utc"] == "2026-07-12T21:45:00Z"
    assert (
        request["timeframe_strategy_evidence_intervals"]["M30"]["end_exclusive_utc"]
        == "2026-07-12T21:30:00Z"
    )
    assert _v5_request_errors(request, DEFAULT_TIMEFRAMES) == []


def test_validator_rejects_literal_semantic_end_instead_of_native_transform(tmp_path):
    request = _generated_v5_request(
        tmp_path,
        datetime(2026, 7, 12, 21, 47, 19, tzinfo=timezone.utc),
    )
    request["timeframe_audit_reference_intervals"]["M5"]["M30"][
        "strategy_full_context"
    ]["end_exclusive_utc"] = request["common_as_of_utc"]

    assert (
        "RUN_MANIFEST_V5_EDGE_INTERVAL_INVALID:M5:M30:strategy_full_context"
        in _v5_request_errors(request, DEFAULT_TIMEFRAMES)
    )
