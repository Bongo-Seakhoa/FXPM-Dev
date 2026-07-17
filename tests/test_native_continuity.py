from __future__ import annotations

import base64
import gc
import hashlib
import json
import tracemalloc
from copy import deepcopy
from dataclasses import dataclass, replace
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable

import pytest

import pm_native_continuity as continuity
from pm_native_continuity import (
    ARTIFACT_COLUMNS,
    BoundaryStatus,
    DiscontinuityClassification,
    InternalContinuityStatus,
    NativeContinuityError,
    NativeContinuityPolicy,
    audit_native_continuity,
    build_native_continuity_policy,
)


UTC = timezone.utc
REQUEST_MAP_SHA256 = "a" * 64
SOURCE_ENVIRONMENT = {
    "account": {
        "account_identity_sha256": "6a2dfa3b9e1586ec7cb357f1b79056d0b70f6f65b1cd854926cb9570ce877fe5",
        "company": "TestBroker",
        "currency": "USD",
    },
    "configured_connection": {
        "connection_identity_sha256": "d51dd3944c95a17cee5f2ba22a3413306292c54ade14ba5645b141cfa2df1780",
        "server_configured": True,
        "terminal_path_configured": True,
    },
    "identity_sha256": "33507ab67f28100e108712f9036e9559cb39241a2b08146b85fffee95647272d",
    "metadata_status": "available",
    "schema_version": "fxpm.native_history.source_identity.v2",
    "terminal": {
        "build": 5432,
        "company": "TestBroker",
        "installation_identity_sha256": "4b4adba9b3078beb346223ccc0bf68391f0becb3e8f1d683b4a7155a97bb76eb",
        "max_bars": 100000000,
        "name": "MetaTrader 5",
        "version": [500, 5432, "13 Jul 2026"],
    },
}

# These are static signatures produced by the external fixture authority.  No
# private key or signing helper exists in this repository.
WEEKEND_SIGNATURE_BASE64 = (
    "bE9xMwACi8Ybs5tYK4PhlK2fVLSIkgzXIhQH3vGBEnjnHFqpKdeZlbLjgrO7"
    "rylqdt0uubKlxyph6ptK8xyeAQ=="
)
HOLIDAY_SIGNATURE_BASE64 = (
    "ezvz45iRDSirPS3uzTbPrrixmrU95TdzPP36dPzIL+rILvBlBegEA/s9R+6I"
    "Nze7c9ZRQm0XyWbREMw1PAo/Ag=="
)


def _dt(value: str | datetime) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    else:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return parsed.astimezone(UTC).replace(microsecond=0)


def _iso(value: datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _range(start: str, end: str, seconds: int) -> list[datetime]:
    cursor = _dt(start)
    stop = _dt(end)
    values: list[datetime] = []
    while cursor <= stop:
        values.append(cursor)
        cursor += timedelta(seconds=seconds)
    return values


def _bars_bytes(opens: list[datetime], cadence_seconds: int) -> bytes:
    lines = [",".join(("time", *ARTIFACT_COLUMNS))]
    for index, opened in enumerate(opens):
        opened_text = _iso(opened)
        close_text = _iso(opened + timedelta(seconds=cadence_seconds))
        value = f"{1.0 + index / 1_000_000:.6f}"
        lines.append(
            ",".join(
                (
                    opened_text,
                    value,
                    value,
                    value,
                    value,
                    str(100 + index),
                    "1",
                    str(100 + index),
                    opened_text,
                    opened_text,
                    close_text,
                )
            )
        )
    return ("\n".join(lines) + "\n").encode("utf-8")


@dataclass(frozen=True)
class Bundle:
    bars_path: Path
    provenance_path: Path
    policy: NativeContinuityPolicy
    opens: tuple[datetime, ...]
    request_start_utc: str
    request_end_exclusive_utc: str


def _serialize_provenance(value: dict[str, Any]) -> bytes:
    return (
        json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
            allow_nan=False,
        )
        + "\n"
    ).encode("utf-8")


def _bundle(
    root: Path,
    *,
    name: str,
    timeframe: str,
    opens: list[datetime | str],
    request_start_utc: datetime | str | None = None,
    request_end_exclusive_utc: datetime | str | None = None,
    pagination: Any = None,
    source_environment: Any = SOURCE_ENVIRONMENT,
    policy_source_environment: Any = SOURCE_ENVIRONMENT,
) -> Bundle:
    cadence = continuity.TIMEFRAME_SECONDS[timeframe]
    normalized_opens = [_dt(value) for value in opens]
    assert normalized_opens
    start = _dt(request_start_utc or normalized_opens[0])
    end = _dt(
        request_end_exclusive_utc or (normalized_opens[-1] + timedelta(seconds=cadence))
    )
    leaf = root / name
    leaf.mkdir(parents=True)
    bars_path = leaf / "bars.csv"
    provenance_path = leaf / "provenance.json"
    artifact = _bars_bytes(normalized_opens, cadence)
    bars_path.write_bytes(artifact)
    artifact_sha256 = hashlib.sha256(artifact).hexdigest()
    provenance = {
        "schema_version": "fxpm-native-bars-v1",
        "acquisition_contract_version": "2026-07-12-native-calendar-acquisition-v3",
        "immutable_record": True,
        "run_id": "native-continuity-test-run",
        "symbol": "CHFJPY",
        "canonical_symbol": "CHFJPY",
        "timeframe": timeframe,
        "timeframe_seconds": cadence,
        "source_kind": "broker_native",
        "timezone": "UTC",
        "timestamp_semantics": "bar_open_utc",
        "bar_interval_semantics": "half_open_[bar_open,bar_close)",
        "bar_close_column": "_BAR_CLOSE_TS",
        "status": "success",
        "complete": True,
        "artifact": {
            "relative_path": "bars.csv",
            "sha256": artifact_sha256,
            "bytes": len(artifact),
            "rows": len(normalized_opens),
        },
        "sha256": artifact_sha256,
        "rows": len(normalized_opens),
        "start_ts": _iso(normalized_opens[0]),
        "end_ts": _iso(normalized_opens[-1]),
        "columns": list(ARTIFACT_COLUMNS),
        "request": {
            "cell_timeframe": timeframe,
            "timeframe_request_map_sha256": REQUEST_MAP_SHA256,
            "source_request_interval": {
                "start_utc": _iso(start),
                "end_exclusive_utc": _iso(end),
            },
            "native_fetch_start_utc": _iso(start),
            "native_fetch_end_exclusive_utc": _iso(end),
            "last_fully_closed_bar_open_utc": _iso(normalized_opens[-1]),
            "alignment_mode": (
                "broker_observed_successor_or_nominal_close_v1"
                if timeframe in {"H4", "D1"}
                else "fixed_utc_native_boundary_v1"
            ),
        },
        "source_environment": deepcopy(source_environment),
        # Deliberately outside continuity semantics.  Page geometry may differ
        # while the exact sidecar remains independently hash-bound.
        "pagination": pagination
        if pagination is not None
        else {"fixture_page_span": "one-page"},
    }
    provenance_bytes = _serialize_provenance(provenance)
    provenance_path.write_bytes(provenance_bytes)
    policy = build_native_continuity_policy(
        symbol="CHFJPY",
        timeframe=timeframe,
        expected_artifact_sha256=artifact_sha256,
        expected_artifact_bytes=len(artifact),
        expected_artifact_rows=len(normalized_opens),
        expected_provenance_sha256=hashlib.sha256(provenance_bytes).hexdigest(),
        expected_provenance_bytes=len(provenance_bytes),
        expected_source_environment=policy_source_environment,
        expected_request_map_sha256=REQUEST_MAP_SHA256,
        request_start_utc=_iso(start),
        request_end_exclusive_utc=_iso(end),
    )
    return Bundle(
        bars_path=bars_path,
        provenance_path=provenance_path,
        policy=policy,
        opens=tuple(normalized_opens),
        request_start_utc=_iso(start),
        request_end_exclusive_utc=_iso(end),
    )


def _rewrite_provenance(
    bundle: Bundle,
    mutator: Callable[[dict[str, Any]], None],
    *,
    expected_source_environment: Any = SOURCE_ENVIRONMENT,
) -> Bundle:
    value = json.loads(bundle.provenance_path.read_text(encoding="utf-8"))
    mutator(value)
    raw = _serialize_provenance(value)
    bundle.provenance_path.write_bytes(raw)
    policy = build_native_continuity_policy(
        symbol=bundle.policy.symbol,
        timeframe=bundle.policy.timeframe,
        expected_artifact_sha256=bundle.policy.artifact_sha256,
        expected_artifact_bytes=bundle.policy.artifact_bytes,
        expected_artifact_rows=bundle.policy.artifact_rows,
        expected_provenance_sha256=hashlib.sha256(raw).hexdigest(),
        expected_provenance_bytes=len(raw),
        expected_source_environment=expected_source_environment,
        expected_request_map_sha256=bundle.policy.request_map_sha256,
        request_start_utc=bundle.request_start_utc,
        request_end_exclusive_utc=bundle.request_end_exclusive_utc,
    )
    return replace(bundle, policy=policy)


def _audit(bundle: Bundle, receipts: Any = ()):
    return audit_native_continuity(
        bars_path=bundle.bars_path,
        provenance_path=bundle.provenance_path,
        policy=bundle.policy,
        calendar_receipts=receipts,
    )


def _calendar_body(
    bundle: Bundle,
    *,
    explanation_code: str,
    calendar_release_id: str,
) -> dict[str, Any]:
    assert len(bundle.opens) == 2
    left, right = bundle.opens
    delta = int((right - left).total_seconds())
    return {
        "contract_version": continuity.CONTINUITY_CONTRACT_VERSION,
        "symbol": bundle.policy.symbol,
        "timeframe": bundle.policy.timeframe,
        "cadence_seconds": bundle.policy.cadence_seconds,
        "artifact_sha256": bundle.policy.artifact_sha256,
        "provenance_sha256": bundle.policy.provenance_sha256,
        "source_identity_sha256": bundle.policy.source_identity_sha256,
        "request_map_sha256": bundle.policy.request_map_sha256,
        "request_start_utc": bundle.policy.request_start_utc,
        "request_end_exclusive_utc": bundle.policy.request_end_exclusive_utc,
        "left_open_utc": _iso(left),
        "right_open_utc": _iso(right),
        "observed_delta_seconds": delta,
        "discontinuity_kind": (
            "LONGER_THAN_NOMINAL"
            if delta > bundle.policy.cadence_seconds
            else "SHORTER_THAN_NOMINAL"
        ),
        "skipped_nominal_open_count": max(
            0,
            (delta - 1) // bundle.policy.cadence_seconds,
        ),
        "explanation_code": explanation_code,
        "calendar_release_id": calendar_release_id,
        "calendar_release_sha256": hashlib.sha256(
            calendar_release_id.encode("ascii")
        ).hexdigest(),
        "issued_at_utc": "2026-07-13T12:00:00Z",
    }


def _calendar_envelope(body: dict[str, Any], signature_base64: str) -> dict[str, Any]:
    return {
        "schema_version": continuity.CALENDAR_RECEIPT_SCHEMA_VERSION,
        "key_id": continuity.CALENDAR_AUTHORITY_KEY_ID,
        "body": body,
        "signature_base64": signature_base64,
    }


def _weekend_bundle(tmp_path: Path, name: str = "weekend") -> Bundle:
    return _bundle(
        tmp_path,
        name=name,
        timeframe="D1",
        opens=["2026-01-09T00:00:00Z", "2026-01-12T00:00:00Z"],
    )


def _holiday_bundle(tmp_path: Path, name: str = "holiday") -> Bundle:
    return _bundle(
        tmp_path,
        name=name,
        timeframe="D1",
        opens=["2025-12-24T00:00:00Z", "2025-12-26T00:00:00Z"],
    )


def test_contiguous_stream_has_typed_technical_only_outcome(tmp_path: Path) -> None:
    bundle = _bundle(
        tmp_path,
        name="contiguous",
        timeframe="M5",
        opens=_range("2026-01-01T00:00:00Z", "2026-01-01T00:20:00Z", 300),
    )

    first = _audit(bundle)
    second = _audit(bundle)

    assert first == second
    assert first.internal_status is InternalContinuityStatus.CONTIGUOUS
    assert first.boundary_status is BoundaryStatus.COMPLETE_REQUEST_SPAN
    assert first.technical_continuity_passed is True
    assert first.blocks_later_technical_activation is False
    assert first.requested_span_complete is True
    assert len(first.segments) == 1
    assert first.segments[0].rows == 5
    assert first.discontinuities == ()
    assert first.grants_trading_authority is False
    assert first.grants_statistical_sufficiency_authority is False
    assert not hasattr(first, "eligible_for_trading")
    assert not hasattr(first, "statistically_sufficient")


def test_chfjpy_d1_180_and_60_day_gaps_are_never_hidden(tmp_path: Path) -> None:
    bundle = _bundle(
        tmp_path,
        name="chfjpy-d1-regression",
        timeframe="D1",
        opens=[
            "2017-12-28T00:00:00Z",
            "2017-12-29T00:00:00Z",
            "2018-06-27T00:00:00Z",
            "2018-06-28T00:00:00Z",
            "2018-08-27T00:00:00Z",
            "2018-08-28T00:00:00Z",
        ],
    )

    result = _audit(bundle)

    assert (
        result.internal_status
        is InternalContinuityStatus.UNEXPLAINED_INTERNAL_DISCONTINUITIES
    )
    assert result.blocks_later_technical_activation is True
    assert result.technical_continuity_passed is False
    assert len(result.segments) == 3
    assert [gap.observed_delta_seconds // 86_400 for gap in result.discontinuities] == [
        180,
        60,
    ]
    assert result.unexplained_discontinuity_count == 2
    assert all(
        gap.classification is DiscontinuityClassification.UNEXPLAINED
        for gap in result.discontinuities
    )


@pytest.mark.parametrize("timeframe", ["M5", "H4", "D1"])
def test_gap_geometry_is_invariant_to_acquisition_page_geometry(
    tmp_path: Path,
    timeframe: str,
) -> None:
    cadence = continuity.TIMEFRAME_SECONDS[timeframe]
    if timeframe == "D1":
        opens = [
            "2017-12-28T00:00:00Z",
            "2017-12-29T00:00:00Z",
            "2018-06-27T00:00:00Z",
            "2018-06-28T00:00:00Z",
            "2018-08-27T00:00:00Z",
            "2018-08-28T00:00:00Z",
        ]
    else:
        opens = [
            *_range(
                "2017-12-29T19:55:00Z" if timeframe == "M5" else "2017-12-29T16:00:00Z",
                "2017-12-29T20:00:00Z",
                cadence,
            ),
            *_range("2018-06-27T00:00:00Z", "2018-06-28T16:00:00Z", cadence),
            *_range(
                "2018-08-27T00:00:00Z",
                "2018-08-27T00:05:00Z" if timeframe == "M5" else "2018-08-27T04:00:00Z",
                cadence,
            ),
        ]
    fine = _bundle(
        tmp_path,
        name=f"{timeframe}-fine-pages",
        timeframe=timeframe,
        opens=opens,
        pagination={"page_span_seconds": cadence * 2, "pages_requested": 999},
    )
    coarse = _bundle(
        tmp_path,
        name=f"{timeframe}-coarse-pages",
        timeframe=timeframe,
        opens=opens,
        pagination={"page_span_seconds": cadence * 2_000_000, "pages_requested": 1},
    )

    fine_result = _audit(fine)
    coarse_result = _audit(coarse)

    assert len(fine_result.discontinuities) == 2
    assert fine_result.segments == coarse_result.segments
    assert fine_result.discontinuities == coarse_result.discontinuities
    assert fine_result.geometry_sha256 == coarse_result.geometry_sha256
    assert fine_result.audit_sha256 != coarse_result.audit_sha256
    assert fine_result.provenance_sha256 != coarse_result.provenance_sha256


def test_h4_regression_preserves_exact_179_17_and_59_33_day_gaps(
    tmp_path: Path,
) -> None:
    opens = [
        *_range("2017-12-29T16:00:00Z", "2017-12-29T20:00:00Z", 14_400),
        *_range("2018-06-27T00:00:00Z", "2018-06-28T16:00:00Z", 14_400),
        *_range("2018-08-27T00:00:00Z", "2018-08-27T04:00:00Z", 14_400),
    ]
    result = _audit(
        _bundle(
            tmp_path,
            name="chfjpy-h4-regression",
            timeframe="H4",
            opens=opens,
        )
    )

    days = [gap.observed_delta_seconds / 86_400 for gap in result.discontinuities]
    assert days == pytest.approx([179 + 4 / 24, 59 + 8 / 24])
    assert result.unexplained_discontinuity_count == 2


@pytest.mark.parametrize("kind", ["weekend", "holiday"])
def test_weekends_and_holidays_are_unexplained_without_authenticated_receipt(
    tmp_path: Path,
    kind: str,
) -> None:
    bundle = (
        _weekend_bundle(tmp_path) if kind == "weekend" else _holiday_bundle(tmp_path)
    )

    result = _audit(bundle)

    assert (
        result.internal_status
        is InternalContinuityStatus.UNEXPLAINED_INTERNAL_DISCONTINUITIES
    )
    assert result.unexplained_discontinuity_count == 1
    assert result.expected_discontinuity_count == 0
    assert result.blocks_later_technical_activation is True


@pytest.mark.parametrize(
    ("builder", "code", "release_id", "signature"),
    [
        (
            _weekend_bundle,
            "SESSION_CLOSED_WEEKEND",
            "testbroker-fx-session-calendar-2026w02",
            WEEKEND_SIGNATURE_BASE64,
        ),
        (
            _holiday_bundle,
            "SESSION_CLOSED_HOLIDAY",
            "testbroker-fx-session-calendar-2025-christmas",
            HOLIDAY_SIGNATURE_BASE64,
        ),
    ],
)
def test_exact_external_calendar_receipt_is_verify_only_and_expected(
    tmp_path: Path,
    builder: Callable[[Path, str], Bundle],
    code: str,
    release_id: str,
    signature: str,
) -> None:
    bundle = builder(tmp_path, f"signed-{code.lower()}")
    body = _calendar_body(
        bundle,
        explanation_code=code,
        calendar_release_id=release_id,
    )

    result = _audit(bundle, [_calendar_envelope(body, signature)])

    assert (
        result.internal_status
        is InternalContinuityStatus.AUTHENTICATED_EXPECTED_DISCONTINUITIES
    )
    assert result.expected_discontinuity_count == 1
    assert result.unexplained_discontinuity_count == 0
    assert result.technical_continuity_passed is True
    assert result.discontinuities[0].explanation_code == code
    assert result.discontinuities[0].calendar_receipt_sha256 is not None
    assert result.grants_trading_authority is False
    assert result.grants_statistical_sufficiency_authority is False


def test_calendar_receipt_tamper_duplicate_and_cross_artifact_replay_fail_closed(
    tmp_path: Path,
) -> None:
    weekend = _weekend_bundle(tmp_path, "weekend-tamper")
    body = _calendar_body(
        weekend,
        explanation_code="SESSION_CLOSED_WEEKEND",
        calendar_release_id="testbroker-fx-session-calendar-2026w02",
    )
    valid = _calendar_envelope(body, WEEKEND_SIGNATURE_BASE64)

    tampered = deepcopy(valid)
    tampered["body"]["explanation_code"] = "SESSION_CLOSED_HOLIDAY"
    with pytest.raises(NativeContinuityError, match="SIGNATURE_INVALID"):
        _audit(weekend, [tampered])
    with pytest.raises(NativeContinuityError, match="RECEIPT_DUPLICATE"):
        _audit(weekend, [valid, deepcopy(valid)])

    holiday = _holiday_bundle(tmp_path, "holiday-cross-replay")
    with pytest.raises(NativeContinuityError, match="BINDING_MISMATCH"):
        _audit(holiday, [valid])


@pytest.mark.parametrize(
    ("opens", "error"),
    [
        (
            ["2026-01-01T00:00:00Z", "2026-01-01T00:00:00Z"],
            "DUPLICATE_BAR_OPEN",
        ),
        (
            ["2026-01-01T00:05:00Z", "2026-01-01T00:00:00Z"],
            "NON_MONOTONIC_BAR_OPEN",
        ),
    ],
)
def test_duplicate_and_out_of_order_native_opens_fail_closed(
    tmp_path: Path,
    opens: list[str],
    error: str,
) -> None:
    bundle = _bundle(
        tmp_path,
        name=error.lower(),
        timeframe="M5",
        opens=opens,
        request_start_utc="2026-01-01T00:00:00Z",
        request_end_exclusive_utc="2026-01-01T00:10:00Z",
    )

    with pytest.raises(NativeContinuityError, match=error):
        _audit(bundle)


def test_shorter_than_native_cadence_is_preserved_as_unexplained(
    tmp_path: Path,
) -> None:
    result = _audit(
        _bundle(
            tmp_path,
            name="shorter-cadence",
            timeframe="D1",
            opens=["2026-01-01T00:00:00Z", "2026-01-01T12:00:00Z"],
            request_end_exclusive_utc="2026-01-02T12:00:00Z",
        )
    )

    assert result.discontinuities[0].discontinuity_kind == "SHORTER_THAN_NOMINAL"
    assert result.discontinuities[0].skipped_nominal_open_count == 0
    assert result.blocks_later_technical_activation is True


def test_boundary_or_prelisting_shortfall_is_distinct_from_internal_gap(
    tmp_path: Path,
) -> None:
    result = _audit(
        _bundle(
            tmp_path,
            name="prelisting-boundary",
            timeframe="D1",
            opens=["2026-01-03T00:00:00Z", "2026-01-04T00:00:00Z"],
            request_start_utc="2026-01-01T00:00:00Z",
        )
    )

    assert result.internal_status is InternalContinuityStatus.CONTIGUOUS
    assert (
        result.boundary_status
        is BoundaryStatus.LEADING_BOUNDARY_OR_PRELISTING_SHORTFALL
    )
    assert result.unexplained_discontinuity_count == 0
    assert result.technical_continuity_passed is True
    assert result.blocks_later_technical_activation is False
    assert result.requested_span_complete is False
    assert result.boundary_shortfalls[0].side == "LEADING"
    assert "NOT_INTERNAL_GAP" in result.boundary_shortfalls[0].classification


@pytest.mark.parametrize(
    ("start", "end", "expected_status", "expected_sides"),
    [
        (
            "2026-01-02T00:00:00Z",
            "2026-01-06T00:00:00Z",
            BoundaryStatus.TRAILING_BOUNDARY_SHORTFALL,
            {"TRAILING"},
        ),
        (
            "2026-01-01T00:00:00Z",
            "2026-01-06T00:00:00Z",
            BoundaryStatus.BOTH_BOUNDARY_SHORTFALLS,
            {"LEADING", "TRAILING"},
        ),
    ],
)
def test_trailing_and_dual_boundaries_never_become_internal_gaps(
    tmp_path: Path,
    start: str,
    end: str,
    expected_status: BoundaryStatus,
    expected_sides: set[str],
) -> None:
    result = _audit(
        _bundle(
            tmp_path,
            name=expected_status.value.lower(),
            timeframe="D1",
            opens=["2026-01-02T00:00:00Z", "2026-01-03T00:00:00Z"],
            request_start_utc=start,
            request_end_exclusive_utc=end,
        )
    )

    assert result.boundary_status is expected_status
    assert {item.side for item in result.boundary_shortfalls} == expected_sides
    assert result.internal_status is InternalContinuityStatus.CONTIGUOUS
    assert result.technical_continuity_passed is True
    assert result.discontinuities == ()


def test_exact_artifact_and_provenance_tamper_are_rejected(tmp_path: Path) -> None:
    artifact_append = _bundle(
        tmp_path,
        name="artifact-append",
        timeframe="M5",
        opens=["2026-01-01T00:00:00Z", "2026-01-01T00:05:00Z"],
    )
    artifact_append.bars_path.write_bytes(artifact_append.bars_path.read_bytes() + b"x")
    with pytest.raises(NativeContinuityError, match="BYTE_COUNT_MISMATCH"):
        _audit(artifact_append)

    same_size = _bundle(
        tmp_path,
        name="artifact-same-size",
        timeframe="M5",
        opens=["2026-01-01T00:00:00Z", "2026-01-01T00:05:00Z"],
    )
    raw = same_size.bars_path.read_bytes()
    same_size.bars_path.write_bytes(raw.replace(b",1.000000,", b",2.000000,", 1))
    with pytest.raises(NativeContinuityError, match="ARTIFACT_SHA256_MISMATCH"):
        _audit(same_size)

    provenance = _bundle(
        tmp_path,
        name="provenance-append",
        timeframe="M5",
        opens=["2026-01-01T00:00:00Z", "2026-01-01T00:05:00Z"],
    )
    provenance.provenance_path.write_bytes(
        provenance.provenance_path.read_bytes() + b" "
    )
    with pytest.raises(NativeContinuityError, match="PROVENANCE_BYTE_COUNT_MISMATCH"):
        _audit(provenance)


def test_source_timeframe_cadence_and_request_bindings_fail_closed(
    tmp_path: Path,
) -> None:
    other_source = deepcopy(SOURCE_ENVIRONMENT)
    other_source["account"]["company"] = "OtherBroker"
    other_source["identity_sha256"] = "b" * 64
    source = _bundle(
        tmp_path,
        name="source-mismatch",
        timeframe="M5",
        opens=["2026-01-01T00:00:00Z", "2026-01-01T00:05:00Z"],
        source_environment=other_source,
        policy_source_environment=SOURCE_ENVIRONMENT,
    )
    with pytest.raises(NativeContinuityError, match="SOURCE_IDENTITY"):
        _audit(source)

    cadence = _bundle(
        tmp_path,
        name="cadence-mismatch",
        timeframe="D1",
        opens=["2026-01-01T00:00:00Z", "2026-01-02T00:00:00Z"],
    )
    cadence = _rewrite_provenance(
        cadence,
        lambda value: value.__setitem__("timeframe_seconds", 300),
    )
    with pytest.raises(NativeContinuityError, match="timeframe_seconds"):
        _audit(cadence)

    request_map = _bundle(
        tmp_path,
        name="request-map-mismatch",
        timeframe="M5",
        opens=["2026-01-01T00:00:00Z", "2026-01-01T00:05:00Z"],
    )
    request_map = _rewrite_provenance(
        request_map,
        lambda value: value["request"].__setitem__(
            "timeframe_request_map_sha256", "b" * 64
        ),
    )
    with pytest.raises(NativeContinuityError, match="REQUEST_MAP_MISMATCH"):
        _audit(request_map)


def test_no_caller_selected_gap_threshold_exists(tmp_path: Path) -> None:
    bundle = _weekend_bundle(tmp_path, "no-threshold")
    with pytest.raises(TypeError, match="unexpected keyword argument"):
        audit_native_continuity(
            bars_path=bundle.bars_path,
            provenance_path=bundle.provenance_path,
            policy=bundle.policy,
            gap_threshold_seconds=999_999_999,  # type: ignore[call-arg]
        )


def test_cyclic_deep_huge_and_receipt_count_inputs_are_bounded(tmp_path: Path) -> None:
    bundle = _weekend_bundle(tmp_path, "bounded-receipts")

    cyclic: dict[str, Any] = {}
    cyclic["body"] = cyclic
    with pytest.raises(NativeContinuityError, match="CYCLE_DETECTED"):
        _audit(bundle, [cyclic])

    deep: Any = "leaf"
    for _ in range(continuity.MAX_CANONICAL_DEPTH + 2):
        deep = [deep]
    with pytest.raises(NativeContinuityError, match="DEPTH_CAP_EXCEEDED"):
        _audit(bundle, [deep])

    huge = {
        "schema_version": continuity.CALENDAR_RECEIPT_SCHEMA_VERSION,
        "key_id": continuity.CALENDAR_AUTHORITY_KEY_ID,
        "body": {"huge": [0] * (continuity.MAX_CONTAINER_ITEMS + 1)},
        "signature_base64": base64.b64encode(b"x" * 64).decode("ascii"),
    }
    with pytest.raises(NativeContinuityError, match="CONTAINER_CAP_EXCEEDED"):
        _audit(bundle, [huge])

    with pytest.raises(NativeContinuityError, match="RECEIPT_CAP_EXCEEDED"):
        _audit(bundle, [{}] * (continuity.MAX_CALENDAR_RECEIPTS + 1))

    class DictSubclass(dict[str, Any]):
        pass

    class ListSubclass(list[Any]):
        pass

    with pytest.raises(NativeContinuityError, match="CANONICAL_TYPE_INVALID"):
        _audit(bundle, [DictSubclass()])
    with pytest.raises(
        NativeContinuityError,
        match="CALENDAR_RECEIPTS_COLLECTION_INVALID",
    ):
        _audit(bundle, ListSubclass())


def test_discontinuity_ledger_cardinality_is_hard_bounded(tmp_path: Path) -> None:
    start = _dt("2026-01-01T00:00:00Z")
    opens = [
        start + timedelta(minutes=10 * index)
        for index in range(continuity.MAX_DISCONTINUITIES + 2)
    ]
    bundle = _bundle(
        tmp_path,
        name="gap-ledger-cap",
        timeframe="M5",
        opens=opens,
    )

    with pytest.raises(NativeContinuityError, match="DISCONTINUITY_CAP_EXCEEDED"):
        _audit(bundle)


def test_policy_row_byte_and_provenance_caps_fail_before_file_access() -> None:
    common = {
        "symbol": "CHFJPY",
        "timeframe": "M5",
        "expected_artifact_sha256": "a" * 64,
        "expected_artifact_bytes": 1,
        "expected_artifact_rows": 1,
        "expected_provenance_sha256": "b" * 64,
        "expected_provenance_bytes": 1,
        "expected_source_environment": SOURCE_ENVIRONMENT,
        "expected_request_map_sha256": REQUEST_MAP_SHA256,
        "request_start_utc": "2026-01-01T00:00:00Z",
        "request_end_exclusive_utc": "2026-01-01T00:05:00Z",
    }
    with pytest.raises(NativeContinuityError, match="ARTIFACT_ROWS_INVALID"):
        build_native_continuity_policy(
            **{**common, "expected_artifact_rows": continuity.MAX_ARTIFACT_ROWS + 1}
        )
    with pytest.raises(NativeContinuityError, match="ARTIFACT_BYTES_INVALID"):
        build_native_continuity_policy(
            **{**common, "expected_artifact_bytes": continuity.MAX_ARTIFACT_BYTES + 1}
        )
    with pytest.raises(NativeContinuityError, match="PROVENANCE_BYTES_INVALID"):
        build_native_continuity_policy(
            **{
                **common,
                "expected_provenance_bytes": continuity.MAX_PROVENANCE_BYTES + 1,
            }
        )


def test_csv_line_cap_is_enforced_during_streaming(tmp_path: Path) -> None:
    bundle = _bundle(
        tmp_path,
        name="line-cap",
        timeframe="M5",
        opens=["2026-01-01T00:00:00Z"],
    )
    header, row = bundle.bars_path.read_bytes().splitlines(keepends=True)
    oversized = header + row[:-1] + b"9" * continuity.MAX_CSV_LINE_BYTES + b"\n"
    bundle.bars_path.write_bytes(oversized)
    provenance = json.loads(bundle.provenance_path.read_text(encoding="utf-8"))
    digest = hashlib.sha256(oversized).hexdigest()
    provenance["artifact"].update(
        {"sha256": digest, "bytes": len(oversized), "rows": 1}
    )
    provenance["sha256"] = digest
    raw_provenance = _serialize_provenance(provenance)
    bundle.provenance_path.write_bytes(raw_provenance)
    policy = build_native_continuity_policy(
        symbol="CHFJPY",
        timeframe="M5",
        expected_artifact_sha256=digest,
        expected_artifact_bytes=len(oversized),
        expected_artifact_rows=1,
        expected_provenance_sha256=hashlib.sha256(raw_provenance).hexdigest(),
        expected_provenance_bytes=len(raw_provenance),
        expected_source_environment=SOURCE_ENVIRONMENT,
        expected_request_map_sha256=REQUEST_MAP_SHA256,
        request_start_utc=bundle.request_start_utc,
        request_end_exclusive_utc=bundle.request_end_exclusive_utc,
    )

    with pytest.raises(NativeContinuityError, match="CSV_LINE_CAP_EXCEEDED"):
        _audit(replace(bundle, policy=policy))


def test_duplicate_json_keys_in_exact_provenance_are_rejected(tmp_path: Path) -> None:
    bundle = _bundle(
        tmp_path,
        name="duplicate-provenance-key",
        timeframe="M5",
        opens=["2026-01-01T00:00:00Z", "2026-01-01T00:05:00Z"],
    )
    raw = bundle.provenance_path.read_bytes()
    duplicated = raw.replace(
        b'"symbol":"CHFJPY"',
        b'"symbol":"CHFJPY","symbol":"CHFJPY"',
        1,
    )
    assert duplicated != raw
    bundle.provenance_path.write_bytes(duplicated)
    policy = build_native_continuity_policy(
        symbol="CHFJPY",
        timeframe="M5",
        expected_artifact_sha256=bundle.policy.artifact_sha256,
        expected_artifact_bytes=bundle.policy.artifact_bytes,
        expected_artifact_rows=bundle.policy.artifact_rows,
        expected_provenance_sha256=hashlib.sha256(duplicated).hexdigest(),
        expected_provenance_bytes=len(duplicated),
        expected_source_environment=SOURCE_ENVIRONMENT,
        expected_request_map_sha256=REQUEST_MAP_SHA256,
        request_start_utc=bundle.request_start_utc,
        request_end_exclusive_utc=bundle.request_end_exclusive_utc,
    )

    with pytest.raises(NativeContinuityError, match="DUPLICATE_JSON_KEY"):
        _audit(replace(bundle, policy=policy))


@pytest.mark.parametrize(
    ("name", "replacement"),
    [
        ("TIMEFRAME_SECONDS", {"M5": 999}),
        ("ARTIFACT_COLUMNS", ("Open",)),
        ("CALENDAR_AUTHORITY_KEY_ID", "attacker-key"),
        ("CALENDAR_SIGNATURE_DOMAIN", b"attacker-domain"),
        ("MAX_ARTIFACT_ROWS", 10**12),
        ("Ed25519PublicKey", object),
        ("_canonical_source_identity", lambda value: value),
    ],
)
def test_runtime_authority_rebinding_fails_before_artifact_access(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    name: str,
    replacement: Any,
) -> None:
    bundle = _bundle(
        tmp_path,
        name=f"runtime-rebind-{name}",
        timeframe="M5",
        opens=["2026-01-01T00:00:00Z", "2026-01-01T00:05:00Z"],
    )
    monkeypatch.setattr(continuity, name, replacement)

    with pytest.raises(NativeContinuityError, match="RUNTIME_AUTHORITY_REBOUND"):
        _audit(bundle)


def test_streaming_memory_is_bounded_independently_of_bar_count(tmp_path: Path) -> None:
    row_count = 20_000
    start = _dt("2025-01-01T00:00:00Z")
    opens = [start + timedelta(minutes=5 * index) for index in range(row_count)]
    bundle = _bundle(
        tmp_path,
        name="bounded-stream",
        timeframe="M5",
        opens=opens,
    )
    del opens
    gc.collect()

    tracemalloc.start()
    result = _audit(bundle)
    _, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()

    assert result.artifact_rows == row_count
    assert len(result.segments) == 1
    assert result.discontinuities == ()
    assert peak < 8 * 1024 * 1024


def test_production_module_contains_verify_only_public_key_path() -> None:
    source = Path(continuity.__file__).read_text(encoding="utf-8")
    assert "Ed25519PrivateKey" not in source
    assert "private_bytes" not in source
    assert ".sign(" not in source
    assert "Ed25519PublicKey" in source
    assert ".verify(" in source
