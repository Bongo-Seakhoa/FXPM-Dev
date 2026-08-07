from __future__ import annotations

import random
import hashlib
import json
from copy import deepcopy
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Mapping

import pytest
import pandas as pd

import pm_native_request_contract as request_contract
from pm_evidence import DEFAULT_TIMEFRAME_WINDOW_YEARS
from pm_native_intervals import native_source_interval
from pm_native_request_contract import (
    CELL_BOUNDARY_SHORTFALL,
    CELL_SUCCESS,
    MAX_PRODUCTION_CELLS,
    MAX_CANONICAL_DEPTH,
    MAX_CANONICAL_INTEGER_BITS,
    MAX_CANONICAL_STRING_CHARS,
    MAX_CONTAINER_ITEMS,
    PRODUCTION_SYMBOL_ORDER,
    PRODUCTION_SYMBOL_ORDER_SHA256,
    PRODUCTION_TIMEFRAME_ORDER,
    PRODUCTION_TIMEFRAME_ORDER_SHA256,
    NativeRequestContractError,
    TrustedV5Policy,
    V5CellValidationContext,
    build_trusted_v5_policy,
    build_v5_artifact_observation,
    build_v5_cell_validation_context,
    build_v5_leaf_request,
    build_v5_manifest_request,
    build_v5_request_contract,
    build_valid_v5_contract_fixture,
    canonical_json_bytes,
    canonical_sha256,
    canonical_source_identity,
    canonical_symbol,
    canonical_symbols,
    expected_consumer_map,
    expected_reference_map,
    timeframe_request_hash_payload,
    validate_authenticated_available_history_boundary,
    validate_source_identity_match,
    validate_v5_cell_status,
    validate_v5_common_request,
    validate_v5_leaf_request,
    validate_v5_manifest_request,
    validate_v5_production_cell,
    validate_v5_production_manifest,
    validate_v5_terminal_technical_cell,
)


UTC = timezone.utc
CUTOFF = datetime(2026, 7, 13, 9, 3, 17, tzinfo=UTC)


def _dt(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def _iso(value: datetime) -> str:
    return value.isoformat().replace("+00:00", "Z")


def _fbs_source_environment(login: int, server: str) -> dict[str, Any]:
    return {
        "metadata_status": "available",
        "account": {
            "login": login,
            "server": server,
            "company": "FBS Markets Inc.",
            "currency": "USD",
        },
        "configured_connection": {
            "server": server,
            "terminal_path": r"C:\FBS\terminal64.exe",
            "expected_login": login,
            "expected_server": server,
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


def _fr40_source_quality_row() -> dict[str, float | int]:
    return {
        "Open": 5917.25,
        "High": 5935.3,
        "Low": 0.0,
        "Close": 5929.2,
        "Volume": 5778,
        "Spread": 0,
        "RealVolume": 0,
    }


def _rehash(request: dict[str, Any]) -> None:
    request["timeframe_calendar_boundaries_sha256"] = canonical_sha256(
        request["timeframe_calendar_boundaries"]
    )
    request["timeframe_request_map_sha256"] = canonical_sha256(
        timeframe_request_hash_payload(request)
    )


def _assert_error(
    code: str,
    callable_: Callable[..., Any],
    *args: Any,
    **kwargs: Any,
) -> NativeRequestContractError:
    with pytest.raises(NativeRequestContractError) as caught:
        callable_(*args, **kwargs)
    assert code in caught.value.errors
    return caught.value


def _result(fixture: dict[str, Any], timeframe: str = "M5") -> dict[str, Any]:
    return next(
        item
        for item in fixture["manifest"]["results"]
        if item["symbol"] == "EURUSD" and item["timeframe"] == timeframe
    )


def _refresh_manifest_counts(fixture: dict[str, Any]) -> None:
    results = fixture["manifest"]["results"]
    fixture["manifest"]["counts"] = {
        status: sum(item["status"] == status for item in results)
        for status in ("success", "partial", "failed")
    }
    fixture["manifest"]["counts"]["total"] = len(results)


def _cell_kwargs(fixture: dict[str, Any], timeframe: str = "M5") -> dict[str, Any]:
    return {
        "symbol": "EURUSD",
        "timeframe": timeframe,
        "manifest_source_environment": fixture["source_environment"],
        "artifact_observation": fixture["artifact_observations"][timeframe],
        "expected_retries": 1,
    }


def _validate_cell(fixture: dict[str, Any], timeframe: str = "M5") -> str:
    return validate_v5_cell_status(
        fixture["provenances"][timeframe],
        _result(fixture, timeframe),
        fixture["common_request"],
        **_cell_kwargs(fixture, timeframe),
    )


def _cell_outcome(
    fixture: dict[str, Any],
    *,
    timeframe: str = "M5",
    validation_context: V5CellValidationContext | None = None,
    common_request: Any = None,
    source_environment: Any = None,
    expected_retries: int = 1,
) -> tuple[str, Any]:
    kwargs = _cell_kwargs(fixture, timeframe)
    kwargs["manifest_source_environment"] = (
        fixture["source_environment"]
        if source_environment is None
        else source_environment
    )
    kwargs["expected_retries"] = expected_retries
    if validation_context is not None:
        kwargs["validation_context"] = validation_context
    try:
        return (
            "ok",
            validate_v5_cell_status(
                fixture["provenances"][timeframe],
                _result(fixture, timeframe),
                (
                    fixture["common_request"]
                    if common_request is None
                    else common_request
                ),
                **kwargs,
            ),
        )
    except NativeRequestContractError as exc:
        return "error", exc.errors


def _fr40_omission_contract_fixture(*, target_open_count: int) -> dict[str, Any]:
    """Build a coherent D1 fixture with one approved internal row removed."""

    source = _fbs_source_environment(28829971, "FBS-Real")
    fixture = build_valid_v5_contract_fixture(
        symbol="FR40",
        timeframe="D1",
        requested_information_cutoff_utc=CUTOFF,
        source_environment=source,
    )
    provenance = fixture["provenance"]
    provenance["broker_symbol"] = "FR40"
    pages = provenance["pagination"]["page_attempts"]
    for page in pages:
        page["connector_diagnostics"]["broker_symbol"] = "FR40"

    target = request_contract.SOURCE_QUALITY_OMISSION_OPEN_UTC
    target_page_index = next(
        index
        for index, page in enumerate(pages)
        if page["start_utc"] <= target < page["end_exclusive_utc"]
    )
    receipt = request_contract.build_v5_source_quality_omission(
        source_environment=source,
        symbol="FR40",
        broker_symbol="FR40",
        timeframe="D1",
        bar_open_utc=target,
        observed_row=_fr40_source_quality_row(),
    )
    pages[target_page_index]["source_quality_omissions"] = [receipt]

    old = fixture["artifact_observation"]
    page_observations = [
        {
            "start_utc": page[0],
            "end_exclusive_utc": page[1],
            "rows": page[2] - (index == target_page_index),
            "actual_first_open_utc": page[3],
            "actual_last_open_utc": page[4],
        }
        for index, page in enumerate(old.page_observations)
    ]
    observation = build_v5_artifact_observation(
        sha256=canonical_sha256(
            {
                "fixture": "fr40-d1-source-quality-omission",
                "target_open_count": target_open_count,
            }
        ),
        bytes=old.bytes,
        rows=old.rows - 1,
        first_open_utc=old.first_open_utc,
        last_open_utc=old.last_open_utc,
        last_close_utc=old.last_close_utc,
        columns=old.columns,
        page_observations=page_observations,
        source_quality_omission_target_open_count=target_open_count,
    )
    fixture["artifact_observation"] = observation
    fixture["artifact_observations"]["D1"] = observation
    provenance["artifact"].update(
        {
            "sha256": observation.sha256,
            "bytes": observation.bytes,
            "rows": observation.rows,
        }
    )
    provenance["sha256"] = observation.sha256
    provenance["rows"] = observation.rows
    result = next(
        item
        for item in fixture["manifest"]["results"]
        if item["symbol"] == "FR40" and item["timeframe"] == "D1"
    )
    result["sha256"] = observation.sha256
    result["rows"] = observation.rows
    fixture["manifest_result"] = result
    fixture["omission_target_page_index"] = target_page_index
    return fixture


@pytest.mark.parametrize(
    ("login", "server", "account_identity_sha256"),
    (
        (
            28829971,
            "FBS-Real",
            "a615b62e2a01f5ac872eb6396e3584b98f9e0d4c78849bed8531e7ad1c24b732",
        ),
        (
            106165989,
            "FBS-Demo",
            "b26594318e54e02993836852cff39b75b1f88aae47e3116ce20e386257e1a2b3",
        ),
    ),
)
def test_source_quality_omission_accepts_only_the_two_observed_fbs_accounts(
    login: int,
    server: str,
    account_identity_sha256: str,
) -> None:
    source = _fbs_source_environment(login, server)
    canonical_source = canonical_source_identity(source)
    assert canonical_source["account"] == {
        "company": "FBS Markets Inc.",
        "currency": "USD",
        "account_identity_sha256": account_identity_sha256,
    }

    receipt = request_contract.build_v5_source_quality_omission(
        source_environment=source,
        symbol="FR40",
        broker_symbol="FR40",
        timeframe="M5",
        bar_open_utc=request_contract.SOURCE_QUALITY_OMISSION_OPEN_UTC,
        observed_row=_fr40_source_quality_row(),
    )
    assert receipt["source_identity_sha256"] == canonical_sha256(canonical_source)
    assert receipt["replacement_supplied"] is False
    assert "economic_verdict" not in receipt
    assert (
        request_contract.validate_v5_source_quality_omission(
            receipt,
            source_environment=source,
            symbol="FR40",
            broker_symbol="FR40",
            timeframe="M5",
        )
        == receipt
    )


def test_source_quality_omission_rejects_non_fbs_wrong_account_and_relabel() -> None:
    real = _fbs_source_environment(28829971, "FBS-Real")
    demo = _fbs_source_environment(106165989, "FBS-Demo")
    kwargs = {
        "symbol": "FR40",
        "broker_symbol": "FR40",
        "timeframe": "M5",
        "bar_open_utc": request_contract.SOURCE_QUALITY_OMISSION_OPEN_UTC,
        "observed_row": _fr40_source_quality_row(),
    }

    non_fbs = deepcopy(real)
    non_fbs["account"]["company"] = "Other Broker"
    _assert_error(
        "V5_SOURCE_QUALITY_OMISSION_SOURCE_INVALID",
        request_contract.build_v5_source_quality_omission,
        source_environment=non_fbs,
        **kwargs,
    )

    wrong_account = deepcopy(real)
    wrong_account["account"]["login"] = 28829972
    wrong_account["configured_connection"]["expected_login"] = 28829972
    _assert_error(
        "V5_SOURCE_QUALITY_OMISSION_SOURCE_INVALID",
        request_contract.build_v5_source_quality_omission,
        source_environment=wrong_account,
        **kwargs,
    )

    wrong_hash = canonical_source_identity(real)
    wrong_hash["account"]["account_identity_sha256"] = "f" * 64
    _assert_error(
        "V5_SOURCE_QUALITY_OMISSION_SOURCE_INVALID",
        request_contract.build_v5_source_quality_omission,
        source_environment=wrong_hash,
        **kwargs,
    )

    real_receipt = request_contract.build_v5_source_quality_omission(
        source_environment=real,
        **kwargs,
    )
    _assert_error(
        "V5_SOURCE_QUALITY_OMISSION_INVALID",
        request_contract.validate_v5_source_quality_omission,
        real_receipt,
        source_environment=demo,
        symbol="FR40",
        broker_symbol="FR40",
        timeframe="M5",
    )


def test_source_quality_omission_is_exactly_row_time_symbol_and_timeframe_bound() -> (
    None
):
    source = _fbs_source_environment(28829971, "FBS-Real")
    kwargs = {
        "source_environment": source,
        "symbol": "FR40",
        "broker_symbol": "FR40",
        "timeframe": "M5",
        "bar_open_utc": request_contract.SOURCE_QUALITY_OMISSION_OPEN_UTC,
        "observed_row": _fr40_source_quality_row(),
    }

    wrong_row = _fr40_source_quality_row()
    wrong_row["Low"] = 0.01
    _assert_error(
        "V5_SOURCE_QUALITY_OMISSION_ROW_MISMATCH",
        request_contract.build_v5_source_quality_omission,
        **{**kwargs, "observed_row": wrong_row},
    )
    row_with_extra_field = {**_fr40_source_quality_row(), "Synthetic": 1}
    _assert_error(
        "V5_SOURCE_QUALITY_OMISSION_ROW_INVALID",
        request_contract.build_v5_source_quality_omission,
        **{**kwargs, "observed_row": row_with_extra_field},
    )
    _assert_error(
        "V5_SOURCE_QUALITY_OMISSION_TIME_INVALID",
        request_contract.build_v5_source_quality_omission,
        **{**kwargs, "bar_open_utc": "2019-11-25T00:00:01Z"},
    )
    for field, value in (("symbol", "EURUSD"), ("broker_symbol", "FR40.a")):
        _assert_error(
            "V5_SOURCE_QUALITY_OMISSION_SYMBOL_INVALID",
            request_contract.build_v5_source_quality_omission,
            **{**kwargs, field: value},
        )

    receipt = request_contract.build_v5_source_quality_omission(**kwargs)
    timeframe_tamper = deepcopy(receipt)
    timeframe_tamper["timeframe"] = "M15"
    timeframe_tamper["receipt_sha256"] = canonical_sha256(
        {
            key: value
            for key, value in timeframe_tamper.items()
            if key != "receipt_sha256"
        }
    )
    _assert_error(
        "V5_SOURCE_QUALITY_OMISSION_INVALID",
        request_contract.validate_v5_source_quality_omission,
        timeframe_tamper,
        source_environment=source,
        symbol="FR40",
        broker_symbol="FR40",
        timeframe="M5",
    )


def test_v5_cell_accepts_exact_internal_source_quality_omission() -> None:
    fixture = _fr40_omission_contract_fixture(target_open_count=0)
    assert (
        validate_v5_production_cell(
            manifest=fixture["manifest"],
            provenance=fixture["provenance"],
            manifest_result=fixture["manifest_result"],
            artifact_observation=fixture["artifact_observation"],
            trusted_policy=fixture["trusted_policy"],
            symbol="FR40",
            timeframe="D1",
        )
        == CELL_SUCCESS
    )


def test_v5_cell_rejects_wrong_row_removal_when_target_remains_in_artifact() -> None:
    fixture = _fr40_omission_contract_fixture(target_open_count=1)
    _assert_error(
        "V5_SOURCE_QUALITY_OMISSION_TARGET_STILL_PRESENT",
        validate_v5_production_cell,
        manifest=fixture["manifest"],
        provenance=fixture["provenance"],
        manifest_result=fixture["manifest_result"],
        artifact_observation=fixture["artifact_observation"],
        trusted_policy=fixture["trusted_policy"],
        symbol="FR40",
        timeframe="D1",
    )


def test_v5_cell_rejects_endpoint_omission_and_receipt_multiplicity() -> None:
    endpoint = _fr40_omission_contract_fixture(target_open_count=0)
    page = endpoint["provenance"]["pagination"]["page_attempts"][
        endpoint["omission_target_page_index"]
    ]
    page["connector_diagnostics"]["admitted_extent"]["first_epoch_second"] = int(
        _dt(request_contract.SOURCE_QUALITY_OMISSION_OPEN_UTC).timestamp()
    )
    with pytest.raises(NativeRequestContractError) as caught:
        validate_v5_production_cell(
            manifest=endpoint["manifest"],
            provenance=endpoint["provenance"],
            manifest_result=endpoint["manifest_result"],
            artifact_observation=endpoint["artifact_observation"],
            trusted_policy=endpoint["trusted_policy"],
            symbol="FR40",
            timeframe="D1",
        )
    assert any(
        code.startswith("V5_SOURCE_QUALITY_OMISSION_ENDPOINT_INVALID")
        for code in caught.value.errors
    )

    duplicate = _fr40_omission_contract_fixture(target_open_count=0)
    duplicate_page = duplicate["provenance"]["pagination"]["page_attempts"][
        duplicate["omission_target_page_index"]
    ]
    duplicate_page["source_quality_omissions"].append(
        deepcopy(duplicate_page["source_quality_omissions"][0])
    )
    with pytest.raises(NativeRequestContractError) as caught:
        validate_v5_production_cell(
            manifest=duplicate["manifest"],
            provenance=duplicate["provenance"],
            manifest_result=duplicate["manifest_result"],
            artifact_observation=duplicate["artifact_observation"],
            trusted_policy=duplicate["trusted_policy"],
            symbol="FR40",
            timeframe="D1",
        )
    assert any(
        code.startswith("V5_SOURCE_QUALITY_OMISSION_LEDGER_INVALID")
        for code in caught.value.errors
    )


def test_v5_cell_validation_context_matches_public_path_for_all_timeframes() -> None:
    fixture = build_valid_v5_contract_fixture(requested_information_cutoff_utc=CUTOFF)
    context = build_v5_cell_validation_context(
        fixture["common_request"],
        fixture["source_environment"],
        1,
    )

    for timeframe in PRODUCTION_TIMEFRAME_ORDER:
        assert _cell_outcome(fixture, timeframe=timeframe) == _cell_outcome(
            fixture,
            timeframe=timeframe,
            validation_context=context,
        )


@pytest.mark.parametrize(
    "mutation",
    [
        "common_field",
        "unexpected_leaf_field",
        "native_fetch_boundary",
        "last_closed",
        "source_identity",
        "manifest_status",
    ],
)
def test_v5_cell_validation_context_rejection_parity(mutation: str) -> None:
    fixture = build_valid_v5_contract_fixture(requested_information_cutoff_utc=CUTOFF)
    context = build_v5_cell_validation_context(
        fixture["common_request"],
        fixture["source_environment"],
        1,
    )
    provenance = fixture["provenance"]
    result = fixture["manifest_result"]
    if mutation == "common_field":
        provenance["request"]["warmup_days"] += 1.0
    elif mutation == "unexpected_leaf_field":
        provenance["request"]["forged"] = True
    elif mutation == "native_fetch_boundary":
        provenance["request"]["native_fetch_start_utc"] = "2020-01-01T00:00:00Z"
    elif mutation == "last_closed":
        provenance["request"]["last_fully_closed_bar_open_utc"] = fixture[
            "common_request"
        ]["common_as_of_utc"]
    elif mutation == "source_identity":
        provenance["source_environment"]["account"]["company"] = "ForgedBroker"
    elif mutation == "manifest_status":
        result["status"] = "failed"
    else:  # pragma: no cover - parametrization is closed above.
        raise AssertionError(mutation)

    public = _cell_outcome(fixture)
    contextual = _cell_outcome(fixture, validation_context=context)
    assert public[0] == "error"
    assert contextual == public


@pytest.mark.parametrize("forgery", ["common", "source", "retries", "context"])
def test_v5_cell_validation_context_rejects_argument_and_object_substitution(
    forgery: str,
) -> None:
    fixture = build_valid_v5_contract_fixture(requested_information_cutoff_utc=CUTOFF)
    context = build_v5_cell_validation_context(
        fixture["common_request"],
        fixture["source_environment"],
        1,
    )
    common: Any = None
    source: Any = None
    retries = 1
    supplied_context = context
    if forgery == "common":
        common = deepcopy(fixture["common_request"])
    elif forgery == "source":
        source = deepcopy(fixture["source_environment"])
    elif forgery == "retries":
        retries = 2
    elif forgery == "context":
        supplied_context = object.__new__(V5CellValidationContext)

    outcome = _cell_outcome(
        fixture,
        validation_context=supplied_context,
        common_request=common,
        source_environment=source,
        expected_retries=retries,
    )
    assert outcome == ("error", ("V5_CELL_VALIDATION_CONTEXT_INVALID",))


def _set_publishable_state(
    fixture: dict[str, Any],
    *,
    failure_status: str,
    timeframe: str = "M5",
) -> None:
    provenance = fixture["provenances"][timeframe]
    result = _result(fixture, timeframe)
    provenance["status"] = result["status"] = "partial"
    provenance["failure_status"] = result["failure_status"] = failure_status
    provenance["complete"] = False
    provenance["eligible_for_downstream_validation"] = False
    result["eligible_for_downstream_validation"] = False
    _refresh_manifest_counts(fixture)


def _boundary_fixture() -> dict[str, Any]:
    fixture = build_valid_v5_contract_fixture(requested_information_cutoff_utc=CUTOFF)
    provenance = fixture["provenance"]
    _set_publishable_state(fixture, failure_status="boundary_history_shortfall")
    pagination = provenance["pagination"]
    page = deepcopy(pagination["page_attempts"][-1])
    page_end = _dt(page["end_exclusive_utc"])
    actual_first = _dt(page["start_utc"])
    actual_last = page_end - timedelta(days=4, seconds=300)
    last_close = actual_last + timedelta(seconds=300)
    page.update(
        {
            "actual_first_open_utc": _iso(actual_first),
            "actual_last_open_utc": _iso(actual_last),
            "start_delay_seconds": 0.0,
            "end_lag_seconds": float(4 * 86_400),
        }
    )
    pagination["truncated_pages"] = [page]

    old = fixture["artifact_observation"]
    page_observations = [
        {
            "start_utc": value[0],
            "end_exclusive_utc": value[1],
            "rows": value[2],
            "actual_first_open_utc": value[3],
            "actual_last_open_utc": value[4],
        }
        for value in old.page_observations
    ]
    last_page = page_observations[-1]
    last_page["actual_last_open_utc"] = _iso(actual_last)
    last_page["rows"] = (
        int(
            (actual_last - _dt(last_page["actual_first_open_utc"])).total_seconds()
            // 300
        )
        + 1
    )
    bound_page = pagination["page_attempts"][-1]
    connector_diagnostics = request_contract._fixture_mt5_v4_page_diagnostics(
        symbol="EURUSD",
        timeframe="M5",
        start=_dt(bound_page["start_utc"]),
        end=_dt(bound_page["end_exclusive_utc"]),
        first_open=_dt(last_page["actual_first_open_utc"]),
        last_open=actual_last,
        rows=last_page["rows"],
    )
    bound_page["connector_diagnostics"] = connector_diagnostics
    page["connector_diagnostics"] = deepcopy(connector_diagnostics)
    row_count = sum(value["rows"] for value in page_observations)
    observation = build_v5_artifact_observation(
        sha256=old.sha256,
        bytes=old.bytes,
        rows=row_count,
        first_open_utc=old.first_open_utc,
        last_open_utc=actual_last,
        last_close_utc=last_close,
        columns=old.columns,
        page_observations=page_observations,
    )
    fixture["artifact_observation"] = observation
    fixture["artifact_observations"]["M5"] = observation
    provenance["request"]["last_fully_closed_bar_open_utc"] = observation.last_open_utc
    provenance["end_ts"] = observation.last_open_utc
    provenance["rows"] = observation.rows
    provenance["artifact"]["rows"] = observation.rows
    fixture["manifest_result"]["rows"] = observation.rows
    coverage = provenance["coverage"]
    coverage.update(
        {
            "status": "partial_requested_span",
            "actual_last_open_utc": observation.last_open_utc,
            "actual_last_close_utc": observation.last_close_utc,
            "end_lag_seconds": float(4 * 86_400),
            "calendar_span_coverage_ratio": (
                (
                    _dt(observation.last_close_utc) - _dt(observation.first_open_utc)
                ).total_seconds()
                / (
                    _dt(
                        fixture["common_request"]["timeframe_source_request_intervals"][
                            "M5"
                        ]["end_exclusive_utc"]
                    )
                    - _dt(observation.first_open_utc)
                ).total_seconds()
            ),
            "truncated_page_count": 1,
            "boundary_truncated_page_count": 1,
        }
    )
    return fixture


def _terminal_prefix_tail_fixture(
    *,
    authenticated_inside_strategy: bool = False,
    authenticated_before_strategy: bool = False,
) -> dict[str, Any]:
    if authenticated_inside_strategy and authenticated_before_strategy:
        raise ValueError("boundary fixture location must be unique")
    authenticated_boundary = (
        authenticated_inside_strategy or authenticated_before_strategy
    )
    timeframe = "H1"
    fixture = build_valid_v5_contract_fixture(
        requested_information_cutoff_utc=CUTOFF,
        timeframe=timeframe,
    )
    provenance = fixture["provenance"]
    _set_publishable_state(
        fixture,
        failure_status="boundary_history_shortfall",
        timeframe=timeframe,
    )
    pagination = provenance["pagination"]
    strategy_start = _dt(
        fixture["common_request"]["timeframe_strategy_evidence_intervals"][timeframe][
            "start_utc"
        ]
    )
    if authenticated_inside_strategy:
        prefix_index = next(
            index
            for index, page in enumerate(pagination["page_attempts"][:-1])
            if _dt(page["start_utc"]) >= strategy_start
        )
    else:
        prefix_index = max(
            index
            for index, page in enumerate(pagination["page_attempts"])
            if _dt(page["end_exclusive_utc"]) <= strategy_start
        )
    attempts = deepcopy(pagination["page_attempts"][prefix_index:])
    observations = list(
        fixture["artifact_observation"].page_observations[prefix_index:]
    )

    prefix = attempts[0]
    prefix["copy_rates_range_attempts"] = 2
    start = _dt(prefix["start_utc"])
    end = _dt(prefix["end_exclusive_utc"])
    diagnostics = request_contract._fixture_mt5_v4_empty_diagnostics(
        symbol="EURUSD",
        timeframe=timeframe,
        start=start,
        end=end,
    )
    sentinel = (
        int(_dt(observations[1][3]).timestamp())
        if authenticated_boundary
        else int(end.timestamp()) + 86_400
    )
    raw_extent = {
        "rows": 1,
        "first_epoch_second": sentinel,
        "last_epoch_second": sentinel,
    }
    diagnostics.update(
        {
            "sync_status": "terminal_prefix_unsynchronized",
            "confirmation_raw_relation": "RIGHT_DISJOINT_PREFIX",
            "raw_relation": "RIGHT_DISJOINT_PREFIX",
            "raw_count": 1,
            "raw_extent": raw_extent,
            "history_download_trigger_nonempty": 1,
        }
    )
    ledger_entries = (
        diagnostics["call_ledger"]
        if authenticated_boundary
        else diagnostics["call_ledger"][1:]
    )
    for entry in ledger_entries:
        entry.update(
            {
                "raw_relation": "RIGHT_DISJOINT_PREFIX",
                "raw_count": 1,
                "raw_extent": deepcopy(raw_extent),
            }
        )
    if authenticated_boundary:
        diagnostics["initial_raw_relation"] = "RIGHT_DISJOINT_PREFIX"
        diagnostics["terminal_confirmation_consumed"] = True
    prefix["connector_diagnostics"] = diagnostics

    observations[0] = (
        observations[0][0],
        observations[0][1],
        0,
        None,
        None,
    )
    old = fixture["artifact_observation"]
    row_count = sum(item[2] for item in observations)
    observation = build_v5_artifact_observation(
        sha256=old.sha256,
        bytes=old.bytes,
        rows=row_count,
        first_open_utc=observations[1][3],
        last_open_utc=observations[-1][4],
        last_close_utc=old.last_close_utc,
        columns=old.columns,
        page_observations=[
            {
                "start_utc": item[0],
                "end_exclusive_utc": item[1],
                "rows": item[2],
                "actual_first_open_utc": item[3],
                "actual_last_open_utc": item[4],
            }
            for item in observations
        ],
    )
    fixture["artifact_observation"] = observation
    fixture["artifact_observations"][timeframe] = observation
    source_start = _dt(
        fixture["common_request"]["timeframe_source_request_intervals"][timeframe][
            "start_utc"
        ]
    )
    source_end = _dt(
        fixture["common_request"]["timeframe_source_request_intervals"][timeframe][
            "end_exclusive_utc"
        ]
    )
    pagination.update(
        {
            "pages_requested": len(attempts),
            "pages_with_data": len(attempts) - 1,
            "page_attempts": attempts,
            "empty_pages": [],
            "failed_pages": [],
            "truncated_pages": [],
            "terminal_prefix_unresolved_pages": [deepcopy(prefix)],
            "stop_reason": "terminal_prefix_unresolved",
            "unrequested_prefix": {
                "start_utc": _iso(source_start),
                "end_exclusive_utc": prefix["start_utc"],
                "reason": "terminal_prefix_unresolved",
            },
            "attempts_total": len(attempts),
            "copy_rates_range_attempts_total": len(attempts) + 1,
        }
    )
    if authenticated_boundary:
        first_data = attempts[1]
        boundary = _dt(observations[1][3])
        pagination["available_history_boundary"] = {
            "schema_version": (
                request_contract.AVAILABLE_HISTORY_BOUNDARY_SCHEMA_VERSION
            ),
            "source_state": "broker_native_available_suffix",
            "boundary_kind": "first_authenticated_available_bar_open",
            "boundary_open_utc": _iso(boundary),
            "terminal_page_start_utc": prefix["start_utc"],
            "terminal_page_end_exclusive_utc": prefix["end_exclusive_utc"],
            "first_data_page_start_utc": first_data["start_utc"],
            "first_data_page_end_exclusive_utc": first_data["end_exclusive_utc"],
            "confirmation_method": (
                request_contract.AVAILABLE_HISTORY_BOUNDARY_CONFIRMATION_METHOD
            ),
            "market_listing_date_asserted": False,
            "statistical_sufficiency_assessed": False,
        }
    provenance["start_ts"] = observation.first_open_utc
    provenance["rows"] = observation.rows
    provenance["artifact"]["rows"] = observation.rows
    fixture["manifest_result"]["rows"] = observation.rows
    coverage = provenance["coverage"]
    coverage.update(
        {
            "status": "partial_requested_span",
            "actual_first_open_utc": observation.first_open_utc,
            "start_delay_seconds": (
                _dt(observation.first_open_utc) - source_start
            ).total_seconds(),
            "calendar_span_coverage_ratio": (
                (
                    _dt(observation.last_close_utc) - _dt(observation.first_open_utc)
                ).total_seconds()
                / (source_end - source_start).total_seconds()
            ),
            "terminal_prefix_unresolved_page_count": 1,
            "technical_integrity_status": "complete_page_ledger",
        }
    )
    return fixture


def _available_boundary_gate_kwargs(
    fixture: Mapping[str, Any],
    timeframe: str = "H1",
) -> dict[str, Any]:
    """Project the externally bound geometry used by the full V5 gate."""

    provenance = fixture["provenance"]
    common = fixture["common_request"]
    return {
        "symbol": provenance["canonical_symbol"],
        "broker_symbol": provenance["broker_symbol"],
        "timeframe": timeframe,
        "leaf_request": provenance["request"],
        "common_request": common,
        "leaf_source_environment": provenance["source_environment"],
        "manifest_source_environment": fixture["source_environment"],
        "expected_retries": 1,
    }


def _technical_prefix_tail_fixture() -> dict[str, Any]:
    fixture = _terminal_prefix_tail_fixture()
    pagination = fixture["provenance"]["pagination"]
    prefix = pagination["page_attempts"][0]
    start = _dt(prefix["start_utc"])
    end = _dt(prefix["end_exclusive_utc"])
    diagnostics = request_contract._fixture_mt5_v4_empty_diagnostics(
        symbol="EURUSD",
        timeframe="H1",
        start=start,
        end=end,
    )
    last_error = {
        "code": -10005,
        "message": "IPC timeout",
        "capture_error": None,
    }
    initial = diagnostics["call_ledger"][0]
    initial.update(last_error=last_error, hard_failure=True)
    diagnostics.update(
        sync_status="hard_mt5_failure",
        copy_rates_range_attempts=1,
        history_download_trigger_attempts=0,
        history_download_trigger_max_bars=0,
        call_ledger=[initial],
        hard_failure=True,
        hard_failure_call="copy_rates_range_initial",
        last_error=last_error,
    )
    prefix["copy_rates_range_attempts"] = 1
    prefix["connector_diagnostics"] = diagnostics
    pagination.update(
        {
            "failed_pages": [deepcopy(prefix)],
            "terminal_prefix_unresolved_pages": [],
            "stop_reason": "technical_prefix_unresolved",
            "copy_rates_range_attempts_total": len(pagination["page_attempts"]),
        }
    )
    pagination["unrequested_prefix"]["reason"] = "technical_prefix_unresolved"
    coverage = fixture["provenance"]["coverage"]
    coverage["failed_page_count"] = 1
    coverage["terminal_prefix_unresolved_page_count"] = 0
    return fixture


def _failed_empty_fixture() -> dict[str, Any]:
    fixture = build_valid_v5_contract_fixture(requested_information_cutoff_utc=CUTOFF)
    provenance = deepcopy(fixture["provenance"])
    provenance = {
        key: value
        for key, value in provenance.items()
        if key
        in (
            request_contract._FAILED_PROVENANCE_BASE_FIELDS
            | request_contract._FAILED_PROVENANCE_FETCH_FIELDS
        )
    }
    provenance.update(
        {
            "status": "failed",
            "failure_status": "terminal_history_empty_unresolved",
            "eligible_for_optimization": False,
            "eligible_for_downstream_validation": False,
            "artifact": None,
            "sha256": None,
            "rows": 0,
            "start_ts": None,
            "end_ts": None,
            "complete": False,
            "instrument": {},
            "acquisition_started_utc": fixture["manifest"]["acquisition_started_utc"],
            "acquisition_completed_utc": fixture["manifest"][
                "acquisition_completed_utc"
            ],
            "error": {
                "type": "TerminalHistoryEmptyUnresolved",
                "message": "no admitted bars; broker absence was not inferred",
            },
        }
    )
    provenance["request"]["last_fully_closed_bar_open_utc"] = None
    output = provenance["output_storage_resource_snapshot"]
    output["estimated_cell_bytes"] = 0
    output["sample_rows"] = 0
    pagination = provenance["pagination"]
    empty_pages = []
    for page in pagination["page_attempts"]:
        page["copy_rates_range_attempts"] = 2
        page["connector_diagnostics"] = (
            request_contract._fixture_mt5_v4_empty_diagnostics(
                symbol="EURUSD",
                timeframe="M5",
                start=_dt(page["start_utc"]),
                end=_dt(page["end_exclusive_utc"]),
            )
        )
        empty_pages.append(deepcopy(page))
    pagination.update(
        {
            "pages_with_data": 0,
            "empty_pages": empty_pages,
            "failed_pages": [],
            "truncated_pages": [],
            "terminal_prefix_unresolved_pages": [],
            "stop_reason": None,
            "unrequested_prefix": None,
            "attempts_total": len(pagination["page_attempts"]),
            "copy_rates_range_attempts_total": 2 * len(pagination["page_attempts"]),
        }
    )
    result = fixture["manifest_result"]
    result.update(
        {
            "status": "failed",
            "failure_status": "terminal_history_empty_unresolved",
            "rows": 0,
            "sha256": None,
            "eligible_for_downstream_validation": False,
        }
    )
    _refresh_manifest_counts(fixture)
    fixture["provenance"] = provenance
    fixture["provenances"]["M5"] = provenance
    return fixture


def _technical_fixture() -> dict[str, Any]:
    fixture = build_valid_v5_contract_fixture(requested_information_cutoff_utc=CUTOFF)
    _set_publishable_state(fixture, failure_status="technical_history_gap")
    provenance = fixture["provenance"]
    pagination = provenance["pagination"]
    page = deepcopy(pagination["page_attempts"][len(pagination["page_attempts"]) // 2])
    pagination["empty_pages"] = [page]
    pagination["pages_with_data"] -= 1
    old = fixture["artifact_observation"]
    page_observations = [
        {
            "start_utc": value[0],
            "end_exclusive_utc": value[1],
            "rows": value[2],
            "actual_first_open_utc": value[3],
            "actual_last_open_utc": value[4],
        }
        for value in old.page_observations
    ]
    empty_index = len(page_observations) // 2
    page_observations[empty_index].update(
        {
            "rows": 0,
            "actual_first_open_utc": None,
            "actual_last_open_utc": None,
        }
    )
    observation = build_v5_artifact_observation(
        sha256=old.sha256,
        bytes=old.bytes,
        rows=sum(value["rows"] for value in page_observations),
        first_open_utc=old.first_open_utc,
        last_open_utc=old.last_open_utc,
        last_close_utc=old.last_close_utc,
        columns=old.columns,
        page_observations=page_observations,
    )
    fixture["artifact_observation"] = observation
    fixture["artifact_observations"]["M5"] = observation
    provenance["rows"] = observation.rows
    provenance["artifact"]["rows"] = observation.rows
    fixture["manifest_result"]["rows"] = observation.rows
    provenance["coverage"].update(
        {
            "status": "partial_requested_span",
            "internal_empty_page_count": 1,
            "technical_integrity_status": "internal_gap_detected",
        }
    )
    return fixture


def _terminal_technical_fixture() -> dict[str, Any]:
    fixture = _technical_fixture()
    source = fixture["provenance"]
    attempt_fields = (
        request_contract._FAILED_PROVENANCE_BASE_FIELDS
        | request_contract._FAILED_PROVENANCE_FETCH_FIELDS
    )
    base_attempt = {
        key: deepcopy(source[key]) for key in attempt_fields if key in source
    }
    base_attempt.update(
        {
            "status": "partial",
            "failure_status": "technical_history_gap",
            "eligible_for_optimization": False,
            "eligible_for_downstream_validation": False,
            "artifact": None,
            "sha256": None,
            "rows": 0,
            "start_ts": None,
            "end_ts": None,
            "complete": False,
            "instrument": {},
            "acquisition_started_utc": fixture["manifest"]["acquisition_started_utc"],
            "acquisition_completed_utc": fixture["manifest"][
                "acquisition_completed_utc"
            ],
            "error": {
                "type": "TechnicalHistoryGap",
                "message": "source artifact is nonfinal: technical_history_gap",
            },
        }
    )
    base_attempt["request"]["last_fully_closed_bar_open_utc"] = None
    empty_page = base_attempt["pagination"]["empty_pages"][0]
    empty_page["copy_rates_range_attempts"] = 2
    empty_page["connector_diagnostics"] = (
        request_contract._fixture_mt5_v4_empty_diagnostics(
            symbol="EURUSD",
            timeframe="M5",
            start=_dt(empty_page["start_utc"]),
            end=_dt(empty_page["end_exclusive_utc"]),
        )
    )
    for page in base_attempt["pagination"]["page_attempts"]:
        if page["request_sequence"] == empty_page["request_sequence"]:
            page.update(deepcopy(empty_page))
            break
    base_attempt["pagination"]["copy_rates_range_attempts_total"] += 1
    base_attempt["output_storage_resource_snapshot"].update(
        {"estimated_cell_bytes": 0, "sample_rows": 0}
    )
    descriptors = []
    for index, token in enumerate(("1" * 32, "2" * 32), start=1):
        record = deepcopy(base_attempt)
        manifest_started = _dt(fixture["manifest"]["acquisition_started_utc"])
        record["acquisition_started_utc"] = _iso(
            manifest_started + timedelta(milliseconds=200 * (index - 1))
        )
        record["acquisition_completed_utc"] = _iso(
            manifest_started + timedelta(milliseconds=200 * index - 100)
        )
        record["error"]["message"] += f":attempt-{index}"
        full_bytes = json.dumps(
            record,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
        ).encode("utf-8")
        envelope = {
            "schema_version": (
                request_contract.TERMINAL_TECHNICAL_ATTEMPT_RECEIPT_SCHEMA_VERSION
            ),
            "immutable_record": True,
            "nonfinal": True,
            "retryable": True,
            "attempt_id": token,
            "run_id": fixture["manifest"]["run_id"],
            "symbol": "EURUSD",
            "timeframe": "M5",
            "retention_status": "full",
            "full_record_sha256": hashlib.sha256(full_bytes).hexdigest(),
            "full_record_bytes": len(full_bytes),
            "record": record,
        }
        receipt_bytes = (
            json.dumps(
                envelope,
                sort_keys=True,
                separators=(",", ":"),
                ensure_ascii=False,
            ).encode("utf-8")
            + b"\n"
        )
        descriptors.append(
            {
                "relative_path": f"_attempts/EURUSD/M5/attempt-{token}.json",
                "sha256": hashlib.sha256(receipt_bytes).hexdigest(),
                "bytes": len(receipt_bytes),
                "envelope": envelope,
            }
        )
    descriptors.sort(key=lambda value: (value["sha256"], value["relative_path"]))
    evidence = {
        "schema_version": request_contract.TERMINAL_TECHNICAL_EVIDENCE_SCHEMA_VERSION,
        "attempt_cap": request_contract.TERMINAL_TECHNICAL_ATTEMPT_CAP,
        "accepted_failure_status": "technical_history_gap",
        "receipts": descriptors,
    }
    latest = max(
        descriptors,
        key=lambda value: (
            _dt(value["envelope"]["record"]["acquisition_completed_utc"]),
            value["sha256"],
        ),
    )["envelope"]["record"]
    provenance = {
        **deepcopy(latest),
        "status": "failed",
        "terminal_technical_evidence": evidence,
    }
    evidence_bytes = canonical_json_bytes(evidence)
    result = fixture["manifest_result"]
    result.update(
        {
            "status": "failed",
            "failure_status": "technical_history_gap",
            "rows": 0,
            "sha256": None,
            "eligible_for_downstream_validation": False,
            "resume_disposition": "terminal_technical",
            "terminal_evidence_sha256": hashlib.sha256(evidence_bytes).hexdigest(),
            "terminal_evidence_bytes": len(evidence_bytes),
        }
    )
    fixture["provenance"] = provenance
    fixture["provenances"]["M5"] = provenance
    fixture["manifest"]["resume"].update(
        {
            "newly_acquired_cells": 599,
            "terminalized_technical_cells": 1,
        }
    )
    _refresh_manifest_counts(fixture)
    return fixture


def _reseal_terminal_technical_fixture(fixture: dict[str, Any]) -> None:
    evidence = fixture["provenance"]["terminal_technical_evidence"]
    for descriptor in evidence["receipts"]:
        envelope = descriptor["envelope"]
        full_bytes = json.dumps(
            envelope["record"],
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
        ).encode("utf-8")
        envelope["full_record_sha256"] = hashlib.sha256(full_bytes).hexdigest()
        envelope["full_record_bytes"] = len(full_bytes)
        receipt_bytes = (
            json.dumps(
                envelope,
                sort_keys=True,
                separators=(",", ":"),
                ensure_ascii=False,
            ).encode("utf-8")
            + b"\n"
        )
        descriptor["sha256"] = hashlib.sha256(receipt_bytes).hexdigest()
        descriptor["bytes"] = len(receipt_bytes)
    evidence["receipts"].sort(
        key=lambda value: (value["sha256"], value["relative_path"])
    )
    latest = max(
        evidence["receipts"],
        key=lambda value: (
            _dt(value["envelope"]["record"]["acquisition_completed_utc"]),
            value["sha256"],
        ),
    )["envelope"]["record"]
    provenance = {
        **deepcopy(latest),
        "status": "failed",
        "terminal_technical_evidence": evidence,
    }
    fixture["provenance"] = provenance
    fixture["provenances"]["M5"] = provenance
    evidence_bytes = canonical_json_bytes(evidence)
    fixture["manifest_result"].update(
        {
            "terminal_evidence_sha256": hashlib.sha256(evidence_bytes).hexdigest(),
            "terminal_evidence_bytes": len(evidence_bytes),
        }
    )


def test_builder_matches_existing_production_common_request_builder(tmp_path) -> None:
    from tools.acquire_native_history import (
        DEFAULT_TIMEFRAMES,
        NativeHistoryAcquirer,
        NativeHistoryRequest,
        NativeHistoryStore,
    )

    native_root = tmp_path / "data" / "native"
    native_root.mkdir(parents=True)
    fixture = build_valid_v5_contract_fixture(requested_information_cutoff_utc=CUTOFF)
    request = NativeHistoryRequest(
        symbols=("EURUSD",),
        timeframes=DEFAULT_TIMEFRAMES,
        as_of_utc=CUTOFF,
        timeframe_window_years=DEFAULT_TIMEFRAME_WINDOW_YEARS,
    )
    with NativeHistoryStore(native_root, "request-contract-parity") as store:
        acquisition = NativeHistoryAcquirer(
            None,
            store,
            request,
            runtime_metadata=fixture["source_environment"],
        )
        production = acquisition._common_request_contract()
    assert production == build_v5_request_contract(
        requested_information_cutoff_utc=CUTOFF
    )
    assert (
        NativeHistoryAcquirer._manifest_result(
            "EURUSD",
            "M5",
            fixture["provenance"],
        )
        == fixture["manifest_result"]
    )


def test_complete_fixture_validates_all_six_production_cells() -> None:
    fixture = build_valid_v5_contract_fixture(requested_information_cutoff_utc=CUTOFF)
    bound = validate_v5_production_manifest(
        fixture["manifest"], fixture["trusted_policy"]
    )
    assert len(bound["results_by_cell"]) == MAX_PRODUCTION_CELLS == 600
    for timeframe in PRODUCTION_TIMEFRAME_ORDER:
        assert (
            validate_v5_production_cell(
                manifest=fixture["manifest"],
                provenance=fixture["provenances"][timeframe],
                manifest_result=_result(fixture, timeframe),
                artifact_observation=fixture["artifact_observations"][timeframe],
                trusted_policy=fixture["trusted_policy"],
                symbol="EURUSD",
                timeframe=timeframe,
            )
            == CELL_SUCCESS
        )


def test_exact_production_roster_and_order_hashes_are_pinned() -> None:
    assert len(PRODUCTION_SYMBOL_ORDER) == len(set(PRODUCTION_SYMBOL_ORDER)) == 100
    assert PRODUCTION_SYMBOL_ORDER[0:4] == (
        "AUDUSD",
        "EURUSD",
        "USDCAD",
        "USDJPY",
    )
    assert PRODUCTION_SYMBOL_ORDER[62:66] == (
        "Platinum",
        "USDTHB",
        "Palladium",
        "XAGEUR",
    )
    assert PRODUCTION_SYMBOL_ORDER[-4:] == ("PEPSICO", "PFIZER", "TESLA", "VISA")
    assert (
        PRODUCTION_SYMBOL_ORDER_SHA256
        == canonical_sha256(list(PRODUCTION_SYMBOL_ORDER))
        == "a86839e77bd3301cd072624e4fcc682091a7760638948f8cc5b09227e0245900"
    )
    assert PRODUCTION_TIMEFRAME_ORDER == ("M5", "M15", "M30", "H1", "H4", "D1")
    assert (
        PRODUCTION_TIMEFRAME_ORDER_SHA256
        == canonical_sha256(list(PRODUCTION_TIMEFRAME_ORDER))
        == "b35242465c401951118886e5fa8ceafa31bd5543cd0f4162eae0607825b60a54"
    )


def test_exact_graph_partitions_and_native_transforms_are_recomputed() -> None:
    request = build_v5_request_contract(requested_information_cutoff_utc=CUTOFF)
    assert request["timeframe_audit_reference_map"] == expected_reference_map()
    assert request["timeframe_audit_consumer_map"] == expected_consumer_map()
    embargo = timedelta(days=request["embargo_days"])
    for timeframe in PRODUCTION_TIMEFRAME_ORDER:
        cell = request["timeframe_calendar_boundaries"][timeframe]
        assert _dt(cell["optimizer_train"]["end_ts"]) + embargo == _dt(
            cell["tuning_validation"]["start_ts"]
        )
        assert _dt(cell["tuning_validation"]["end_ts"]) + embargo == _dt(
            cell["final_lockbox"]["start_ts"]
        )
        assert _dt(cell["final_lockbox"]["end_ts"]) == _dt(request["common_as_of_utc"])
        own = request["timeframe_audit_windows"][timeframe]["strategy_full_context"]
        expected = native_source_interval(
            timeframe,
            _dt(own["start_utc"]),
            _dt(own["end_exclusive_utc"]),
        )
        assert request["timeframe_strategy_evidence_intervals"][timeframe] == {
            "start_utc": _iso(expected[0]),
            "end_exclusive_utc": _iso(expected[1]),
        }


def test_rehashed_overlapping_partitions_are_invalid() -> None:
    request = build_v5_request_contract(requested_information_cutoff_utc=CUTOFF)
    for timeframe in PRODUCTION_TIMEFRAME_ORDER:
        interval = deepcopy(
            request["timeframe_calendar_boundaries"][timeframe]["final_lockbox"]
        )
        request["timeframe_calendar_boundaries"][timeframe] = {
            name: deepcopy(interval)
            for name in ("optimizer_train", "tuning_validation", "final_lockbox")
        }
    _rehash(request)
    _assert_error(
        "V5_PARTITION_CHAIN_INVALID:M5",
        validate_v5_common_request,
        request,
    )


def test_rehashed_subset_and_forged_reference_maps_are_invalid() -> None:
    request = build_v5_request_contract(requested_information_cutoff_utc=CUTOFF)
    request["timeframe_calendar_boundaries"].pop("D1")
    _rehash(request)
    _assert_error(
        "V5_MAP_ROSTER_INVALID:timeframe_calendar_boundaries",
        validate_v5_common_request,
        request,
    )

    request = build_v5_request_contract(requested_information_cutoff_utc=CUTOFF)
    request["timeframe_audit_reference_map"]["M5"] = ["H4", "D1"]
    _rehash(request)
    _assert_error(
        "V5_COMMON_FIELD_MISMATCH:timeframe_audit_reference_map",
        validate_v5_common_request,
        request,
    )


def test_manifest_request_is_bound_to_external_policy_not_its_own_hashes() -> None:
    fixture = build_valid_v5_contract_fixture(requested_information_cutoff_utc=CUTOFF)
    changed = build_v5_request_contract(
        requested_information_cutoff_utc=CUTOFF,
        warmup_days=401.0,
    )
    self_consistent = build_v5_manifest_request(
        changed, symbols=PRODUCTION_SYMBOL_ORDER, retries=1
    )
    _assert_error(
        "V5_TRUSTED_COMMON_REQUEST_MISMATCH",
        validate_v5_manifest_request,
        self_consistent,
        fixture["trusted_policy"],
    )


@pytest.mark.parametrize(
    "field",
    ("symbols", "timeframes", "retries", "window_years", "common_calendar_boundaries"),
)
def test_manifest_request_requires_roster_retry_and_compatibility_fields(
    field: str,
) -> None:
    fixture = build_valid_v5_contract_fixture(requested_information_cutoff_utc=CUTOFF)
    fixture["manifest_request"].pop(field)
    _assert_error(
        f"V5_MANIFEST_FIELD_MISSING:{field}",
        validate_v5_manifest_request,
        fixture["manifest_request"],
        fixture["trusted_policy"],
    )


def test_manifest_and_leaf_requests_reject_unversioned_extra_authority_fields() -> None:
    common = build_v5_request_contract(requested_information_cutoff_utc=CUTOFF)
    common["tradeable"] = True
    _assert_error(
        "V5_COMMON_FIELD_UNEXPECTED:tradeable",
        validate_v5_common_request,
        common,
    )
    fixture = build_valid_v5_contract_fixture(requested_information_cutoff_utc=CUTOFF)
    fixture["manifest_request"]["tradeable"] = True
    _assert_error(
        "V5_MANIFEST_FIELD_UNEXPECTED:tradeable",
        validate_v5_manifest_request,
        fixture["manifest_request"],
        fixture["trusted_policy"],
    )
    fixture = build_valid_v5_contract_fixture(requested_information_cutoff_utc=CUTOFF)
    fixture["leaf_request"]["statistical_authority"] = True
    _assert_error(
        "V5_LEAF_FIELD_UNEXPECTED:statistical_authority",
        validate_v5_leaf_request,
        fixture["leaf_request"],
        fixture["common_request"],
        "M5",
    )


def test_subset_symbol_policy_cannot_be_constructed() -> None:
    fixture = build_valid_v5_contract_fixture(requested_information_cutoff_utc=CUTOFF)
    _assert_error(
        "V5_PRODUCTION_SYMBOL_ROSTER_MISMATCH",
        build_trusted_v5_policy,
        expected_symbols=["EURUSD", "GBPUSD"],
        expected_source_environment=fixture["source_environment"],
        requested_information_cutoff_utc=CUTOFF,
    )


@pytest.mark.parametrize("forged_retries", (0, 2, 1.0, "1", True))
def test_manifest_and_trusted_policy_require_exact_integer_retry_owner(
    forged_retries: Any,
) -> None:
    fixture = build_valid_v5_contract_fixture(requested_information_cutoff_utc=CUTOFF)
    _assert_error(
        "V5_ACQUISITION_RETRY_OWNER_INVALID",
        build_v5_manifest_request,
        fixture["common_request"],
        symbols=PRODUCTION_SYMBOL_ORDER,
        retries=forged_retries,
    )
    _assert_error(
        "V5_ACQUISITION_RETRY_OWNER_INVALID",
        build_trusted_v5_policy,
        expected_symbols=PRODUCTION_SYMBOL_ORDER,
        expected_source_environment=fixture["source_environment"],
        requested_information_cutoff_utc=CUTOFF,
        retries=forged_retries,
    )


def test_reversed_exact_six_order_is_never_a_canonical_production_order() -> None:
    fixture = build_valid_v5_contract_fixture(requested_information_cutoff_utc=CUTOFF)
    reversed_order = tuple(reversed(PRODUCTION_TIMEFRAME_ORDER))
    _assert_error(
        "V5_MANIFEST_TIMEFRAME_ROSTER_INVALID",
        build_trusted_v5_policy,
        expected_symbols=PRODUCTION_SYMBOL_ORDER,
        expected_source_environment=fixture["source_environment"],
        requested_information_cutoff_utc=CUTOFF,
        timeframe_order=reversed_order,
    )
    _assert_error(
        "V5_MANIFEST_TIMEFRAME_ROSTER_INVALID",
        build_v5_manifest_request,
        fixture["common_request"],
        symbols=PRODUCTION_SYMBOL_ORDER,
        timeframes=reversed_order,
    )


def test_result_order_is_exactly_bound_to_trusted_writer_order() -> None:
    fixture = build_valid_v5_contract_fixture(requested_information_cutoff_utc=CUTOFF)
    fixture["manifest"]["results"].reverse()
    _assert_error(
        "V5_MANIFEST_RESULT_ORDER_MISMATCH",
        validate_v5_production_manifest,
        fixture["manifest"],
        fixture["trusted_policy"],
    )


def test_source_identity_is_mandatory_and_exact_on_stable_fields() -> None:
    fixture = build_valid_v5_contract_fixture(requested_information_cutoff_utc=CUTOFF)
    observed = deepcopy(fixture["source_environment"])
    observed["terminal"]["connected"] = False
    assert validate_source_identity_match(
        observed, fixture["source_environment"]
    ) == canonical_source_identity(fixture["source_environment"])
    observed["metadata_status"] = "partial"
    _assert_error(
        "V5_SOURCE_METADATA_UNAVAILABLE",
        validate_source_identity_match,
        observed,
        fixture["source_environment"],
    )
    observed = deepcopy(fixture["source_environment"])
    observed["account"]["server"] = "OtherBroker-Live"
    observed["configured_connection"]["server"] = "OtherBroker-Live"
    observed["configured_connection"]["expected_server"] = "OtherBroker-Live"
    _assert_error(
        "V5_SOURCE_IDENTITY_MISMATCH",
        validate_source_identity_match,
        observed,
        fixture["source_environment"],
    )
    fixture["manifest"].pop("source_environment")
    _assert_error(
        "V5_SOURCE_ENVIRONMENT_INVALID",
        validate_v5_production_manifest,
        fixture["manifest"],
        fixture["trusted_policy"],
    )
    incomplete = deepcopy(fixture["source_environment"])
    incomplete.pop("configured_connection")
    _assert_error(
        "V5_SOURCE_IDENTITY_SECTION_INVALID:configured_connection",
        canonical_source_identity,
        incomplete,
    )


def test_source_identity_can_be_persisted_and_verified_without_plaintext_account_data() -> (
    None
):
    fixture = build_valid_v5_contract_fixture(requested_information_cutoff_utc=CUTOFF)
    raw = deepcopy(fixture["source_environment"])
    raw["password"] = "never-persist-this-password"
    raw["api_token"] = "never-persist-this-token"
    raw["account"]["password"] = "nested-account-password"
    raw["configured_connection"]["api_token"] = "nested-connection-token"
    redacted = canonical_source_identity(raw)
    assert canonical_source_identity(redacted) == redacted
    assert canonical_json_bytes(canonical_source_identity(redacted)) == (
        canonical_json_bytes(canonical_source_identity(raw))
    )
    serialized = canonical_json_bytes(redacted).decode("utf-8")
    assert '"login"' not in serialized
    assert '"server":' not in serialized
    for secret in (
        "TestBroker-Demo",
        "C:\\Program Files\\MetaTrader 5",
        "C:\\FxPM\\TerminalData",
        "never-persist-this-password",
        "never-persist-this-token",
        "nested-account-password",
        "nested-connection-token",
    ):
        assert secret not in serialized
    assert fixture["manifest"]["source_environment"] == redacted
    assert fixture["provenance"]["source_environment"] == redacted
    assert (
        validate_v5_production_cell(
            manifest=fixture["manifest"],
            provenance=fixture["provenance"],
            manifest_result=fixture["manifest_result"],
            artifact_observation=fixture["artifact_observation"],
            trusted_policy=fixture["trusted_policy"],
            symbol="EURUSD",
            timeframe="M5",
        )
        == CELL_SUCCESS
    )
    fixture["manifest"]["source_environment"]["identity_sha256"] = "f" * 64
    _assert_error(
        "V5_TRUSTED_SOURCE_IDENTITY_MISMATCH",
        validate_v5_production_manifest,
        fixture["manifest"],
        fixture["trusted_policy"],
    )

    typed_identity = deepcopy(raw)
    typed_identity["account"]["login"] = "42"
    typed_identity["configured_connection"]["expected_login"] = "42"
    assert canonical_source_identity(typed_identity) != redacted

    secret_mismatch = deepcopy(raw)
    secret_mismatch["configured_connection"]["expected_server"] = (
        "TOP-SECRET-WRONG-SERVER"
    )
    with pytest.raises(NativeRequestContractError) as caught:
        canonical_source_identity(secret_mismatch)
    assert "TOP-SECRET-WRONG-SERVER" not in str(caught.value)
    assert "never-persist-this-password" not in str(caught.value)
    incomplete = deepcopy(fixture["source_environment"])
    incomplete["terminal"].pop("version")
    _assert_error(
        "V5_SOURCE_IDENTITY_FIELD_MISSING:terminal:version",
        canonical_source_identity,
        incomplete,
    )


def test_directly_constructed_policy_requires_bounded_canonical_payloads() -> None:
    fixture = build_valid_v5_contract_fixture(requested_information_cutoff_utc=CUTOFF)
    policy = fixture["trusted_policy"]
    noncanonical = replace(
        policy,
        common_request_json=b" " + policy.common_request_json,
    )
    payload = {
        "schema_version": "fxpm.native_history.trusted_policy.v1",
        "common_request_sha256": __import__("hashlib")
        .sha256(noncanonical.common_request_json)
        .hexdigest(),
        "symbols": list(noncanonical.symbols),
        "timeframes": list(noncanonical.timeframes),
        "retries": noncanonical.retries,
        "source_identity_sha256": __import__("hashlib")
        .sha256(noncanonical.source_identity_json)
        .hexdigest(),
    }
    noncanonical = replace(noncanonical, policy_sha256=canonical_sha256(payload))
    _assert_error(
        "V5_TRUSTED_POLICY_PAYLOAD_NONCANONICAL",
        validate_v5_manifest_request,
        fixture["manifest_request"],
        noncanonical,
    )
    oversized = TrustedV5Policy(
        common_request_json=b"x" * 4_000_001,
        symbols=("EURUSD",),
        timeframes=PRODUCTION_TIMEFRAME_ORDER,
        retries=1,
        source_identity_json=b"{}",
        policy_sha256="0" * 64,
    )
    _assert_error(
        "V5_TRUSTED_POLICY_PAYLOAD_INVALID",
        validate_v5_manifest_request,
        fixture["manifest_request"],
        oversized,
    )

    forged_subset = replace(policy, symbols=("EURUSD",))
    forged_payload = {
        "schema_version": "fxpm.native_history.trusted_policy.v1",
        "common_request_sha256": __import__("hashlib")
        .sha256(forged_subset.common_request_json)
        .hexdigest(),
        "symbols": ["EURUSD"],
        "timeframes": list(forged_subset.timeframes),
        "retries": forged_subset.retries,
        "source_identity_sha256": __import__("hashlib")
        .sha256(forged_subset.source_identity_json)
        .hexdigest(),
    }
    forged_subset = replace(
        forged_subset,
        policy_sha256=canonical_sha256(forged_payload),
    )
    _assert_error(
        "V5_PRODUCTION_SYMBOL_ROSTER_MISMATCH",
        validate_v5_manifest_request,
        fixture["manifest_request"],
        forged_subset,
    )


def test_contract_contains_no_local_signing_mac_or_seal_oracle() -> None:
    callable_names = {
        name.lower()
        for name, value in vars(request_contract).items()
        if callable(value)
        and getattr(value, "__module__", None) == request_contract.__name__
    }
    assert not {
        name
        for name in callable_names
        if any(token in name for token in ("sign", "_mac", "seal", "private_key"))
    }


@pytest.mark.parametrize(
    ("field", "value", "code"),
    (
        ("window_years", {}, "V5_LEAF_PROJECTION_MISMATCH:window_years"),
        (
            "common_calendar_boundaries",
            {},
            "V5_LEAF_PROJECTION_MISMATCH:common_calendar_boundaries",
        ),
        (
            "native_fetch_start_utc",
            "2026-01-01T00:00:00Z",
            "V5_NATIVE_FETCH_START_MISMATCH",
        ),
        ("alignment_mode", "forged", "V5_NATIVE_ALIGNMENT_MODE_MISMATCH"),
    ),
)
def test_leaf_compatibility_and_native_geometry_are_exact(
    field: str, value: Any, code: str
) -> None:
    fixture = build_valid_v5_contract_fixture(requested_information_cutoff_utc=CUTOFF)
    fixture["leaf_request"][field] = value
    _assert_error(
        code,
        validate_v5_leaf_request,
        fixture["leaf_request"],
        fixture["common_request"],
        "M5",
    )


def test_last_closed_bar_is_required_bounded_and_bound_to_artifact() -> None:
    common = build_v5_request_contract(requested_information_cutoff_utc=CUTOFF)
    _assert_error(
        "V5_LAST_CLOSED_BAR_REQUIRED",
        build_v5_leaf_request,
        common,
        "M5",
    )
    _assert_error(
        "V5_LAST_CLOSED_BAR_OUTSIDE_REQUEST",
        build_v5_leaf_request,
        common,
        "M5",
        last_fully_closed_bar_open_utc=common["common_as_of_utc"],
    )
    fixture = build_valid_v5_contract_fixture(requested_information_cutoff_utc=CUTOFF)
    fixture["provenance"]["request"]["last_fully_closed_bar_open_utc"] = _iso(
        _dt(fixture["artifact_observation"].last_open_utc) - timedelta(minutes=5)
    )
    _assert_error(
        "V5_LAST_CLOSED_BAR_BINDING_MISMATCH",
        _validate_cell,
        fixture,
    )


@pytest.mark.parametrize(("timeframe", "short_session_hours"), (("H4", 3), ("D1", 23)))
def test_broker_session_last_open_does_not_assume_nominal_utc_close(
    timeframe: str, short_session_hours: int
) -> None:
    common = build_v5_request_contract(requested_information_cutoff_utc=CUTOFF)
    as_of = _dt(common["common_as_of_utc"])
    last_open = as_of - timedelta(hours=short_session_hours)
    assert (
        last_open + timedelta(seconds=request_contract.TIMEFRAME_SECONDS[timeframe])
        > as_of
    )
    leaf = build_v5_leaf_request(
        common,
        timeframe,
        last_fully_closed_bar_open_utc=last_open,
    )
    validate_v5_leaf_request(leaf, common, timeframe)
    assert leaf["last_fully_closed_bar_open_utc"] == _iso(last_open)


def test_boundary_shortfall_validates_but_empty_page_artifact_does_not() -> None:
    assert _validate_cell(_boundary_fixture()) == CELL_BOUNDARY_SHORTFALL
    _assert_error(
        "V5_TECHNICAL_ARTIFACT_NOT_PUBLISHABLE",
        _validate_cell,
        _technical_fixture(),
    )


def test_terminal_prefix_nonproof_preserves_authenticated_tail() -> None:
    fixture = _terminal_prefix_tail_fixture()
    assert _validate_cell(fixture, "H1") == CELL_BOUNDARY_SHORTFALL

    forged = _terminal_prefix_tail_fixture()
    forged["provenance"]["pagination"]["terminal_prefix_unresolved_pages"] = []
    _assert_error(
        "V5_TERMINAL_PREFIX_EVIDENCE_INVALID",
        _validate_cell,
        forged,
        "H1",
    )


def test_authenticated_available_boundary_inside_strategy_is_derived_not_asserted() -> (
    None
):
    fixture = _terminal_prefix_tail_fixture(authenticated_inside_strategy=True)
    provenance = fixture["provenance"]
    proof = provenance["pagination"]["available_history_boundary"]

    assert _validate_cell(fixture, "H1") == CELL_BOUNDARY_SHORTFALL
    assert proof["boundary_open_utc"] == fixture["artifact_observation"].first_open_utc
    assert proof["market_listing_date_asserted"] is False
    assert proof["statistical_sufficiency_assessed"] is False
    assert provenance["eligible_for_optimization"] is False
    assert provenance["eligible_for_downstream_validation"] is False
    derived = validate_authenticated_available_history_boundary(
        provenance["pagination"],
        fixture["artifact_observation"],
        **_available_boundary_gate_kwargs(fixture),
    )
    assert derived == proof

    missing = _terminal_prefix_tail_fixture(authenticated_inside_strategy=True)
    missing["provenance"]["pagination"].pop("available_history_boundary")
    _assert_error(
        "V5_TERMINAL_PREFIX_INTERSECTS_STRATEGY_INTERVAL",
        _validate_cell,
        missing,
        "H1",
    )

    for field, value in (
        ("boundary_open_utc", "2020-01-01T00:00:00Z"),
        ("market_listing_date_asserted", True),
        ("statistical_sufficiency_assessed", True),
    ):
        forged = _terminal_prefix_tail_fixture(authenticated_inside_strategy=True)
        forged["provenance"]["pagination"]["available_history_boundary"][field] = value
        _assert_error(
            "V5_AVAILABLE_HISTORY_BOUNDARY_INVALID",
            _validate_cell,
            forged,
            "H1",
        )

    forged_artifact = _terminal_prefix_tail_fixture(authenticated_inside_strategy=True)
    observation = forged_artifact["artifact_observation"]
    wrong_first = replace(
        observation,
        first_open_utc=_iso(_dt(observation.first_open_utc) + timedelta(hours=1)),
    )
    _assert_error(
        "V5_ARTIFACT_PAGE_FIRST_OPEN_MISMATCH",
        validate_authenticated_available_history_boundary,
        forged_artifact["provenance"]["pagination"],
        wrong_first,
        **_available_boundary_gate_kwargs(forged_artifact),
    )


def test_in_strategy_terminal_prefix_is_publishable_error_but_valid_failed_evidence() -> (
    None
):
    fixture = _terminal_prefix_tail_fixture(authenticated_inside_strategy=True)
    pagination = fixture["provenance"]["pagination"]
    pagination.pop("available_history_boundary")
    common = fixture["common_request"]
    kwargs = {
        "expected_symbol": "EURUSD",
        "expected_broker_symbol": "EURUSD",
        "expected_source_environment": fixture["source_environment"],
        "expected_retries": 1,
        "expected_page_span_seconds": pagination["page_span_seconds"],
        "boundary_tolerance_seconds": float(common["boundary_tolerance_days"] * 86_400),
    }

    publishable = request_contract._pagination_evidence(
        pagination,
        common["timeframe_source_request_intervals"]["H1"],
        common["timeframe_strategy_evidence_intervals"]["H1"],
        "H1",
        evidence_mode="publishable",
        **kwargs,
    )
    failed = request_contract._pagination_evidence(
        pagination,
        common["timeframe_source_request_intervals"]["H1"],
        common["timeframe_strategy_evidence_intervals"]["H1"],
        "H1",
        evidence_mode="failed",
        **kwargs,
    )

    assert "V5_TERMINAL_PREFIX_INTERSECTS_STRATEGY_INTERVAL" in publishable["errors"]
    assert "V5_TERMINAL_PREFIX_INTERSECTS_STRATEGY_INTERVAL" not in failed["errors"]
    assert failed["terminal_prefix_unresolved"] is True
    assert failed["authenticated_available_history_boundary_utc"] is None


def test_authenticated_available_boundary_before_strategy_is_source_fact() -> None:
    fixture = _terminal_prefix_tail_fixture(authenticated_before_strategy=True)
    provenance = fixture["provenance"]
    pagination = provenance["pagination"]
    proof = pagination["available_history_boundary"]
    strategy_start = _dt(
        fixture["common_request"]["timeframe_strategy_evidence_intervals"]["H1"][
            "start_utc"
        ]
    )

    assert _dt(proof["terminal_page_end_exclusive_utc"]) <= strategy_start
    assert _validate_cell(fixture, "H1") == CELL_BOUNDARY_SHORTFALL
    assert provenance["eligible_for_optimization"] is False
    assert provenance["eligible_for_downstream_validation"] is False
    assert validate_authenticated_available_history_boundary(
        pagination,
        fixture["artifact_observation"],
        **_available_boundary_gate_kwargs(fixture),
    ) == dict(proof)

    mismatched_call = _terminal_prefix_tail_fixture(authenticated_before_strategy=True)
    for terminal_page in (
        mismatched_call["provenance"]["pagination"]["page_attempts"][0],
        mismatched_call["provenance"]["pagination"]["terminal_prefix_unresolved_pages"][
            0
        ],
    ):
        terminal_page["connector_diagnostics"]["call_ledger"][1]["raw_extent"][
            "first_epoch_second"
        ] += 3_600
    _assert_error(
        "V5_AVAILABLE_HISTORY_BOUNDARY_INVALID",
        _validate_cell,
        mismatched_call,
        "H1",
    )

    mismatched_adjacent_page = _terminal_prefix_tail_fixture(
        authenticated_before_strategy=True
    )
    mismatched_adjacent_page["provenance"]["pagination"]["page_attempts"][1][
        "connector_diagnostics"
    ]["admitted_extent"]["first_epoch_second"] += 3_600
    _assert_error(
        "V5_AVAILABLE_HISTORY_BOUNDARY_INVALID",
        _validate_cell,
        mismatched_adjacent_page,
        "H1",
    )


def test_public_available_boundary_gate_reuses_full_pagination_and_artifact_ledgers() -> (
    None
):
    baseline = _terminal_prefix_tail_fixture(authenticated_inside_strategy=True)

    forged_sequence = deepcopy(baseline)
    sequence_pagination = forged_sequence["provenance"]["pagination"]
    sequence_pagination["page_attempts"][0]["request_sequence"] = 99
    sequence_pagination["terminal_prefix_unresolved_pages"][0]["request_sequence"] = 99
    _assert_error(
        "V5_PAGE_REQUEST_SEQUENCE_INVALID:0",
        validate_authenticated_available_history_boundary,
        sequence_pagination,
        forged_sequence["artifact_observation"],
        **_available_boundary_gate_kwargs(forged_sequence),
    )

    forged_geometry = deepcopy(baseline)
    geometry_pagination = forged_geometry["provenance"]["pagination"]
    geometry_pagination["pages_planned"] += 1
    _assert_error(
        "V5_PAGE_LEDGER_PLANNED_COUNT_MISMATCH",
        validate_authenticated_available_history_boundary,
        geometry_pagination,
        forged_geometry["artifact_observation"],
        **_available_boundary_gate_kwargs(forged_geometry),
    )

    forged_category = deepcopy(baseline)
    category_pagination = forged_category["provenance"]["pagination"]
    category_attempt = deepcopy(category_pagination["page_attempts"][2])
    category_observation = forged_category["artifact_observation"].page_observations[2]
    page_start = _dt(category_observation[0])
    page_end = _dt(category_observation[1])
    actual_first = _dt(category_observation[3])
    actual_last = _dt(category_observation[4])
    category_attempt.update(
        {
            "actual_first_open_utc": category_observation[3],
            "actual_last_open_utc": category_observation[4],
            "start_delay_seconds": max(
                0.0, (actual_first - page_start).total_seconds()
            ),
            "end_lag_seconds": max(
                0.0,
                (page_end - timedelta(hours=1) - actual_last).total_seconds(),
            ),
        }
    )
    category_pagination["truncated_pages"] = [category_attempt]
    _assert_error(
        "V5_TRUNCATED_PAGE_BELOW_TOLERANCE",
        validate_authenticated_available_history_boundary,
        category_pagination,
        forged_category["artifact_observation"],
        **_available_boundary_gate_kwargs(forged_category),
    )

    forged_duplicate = deepcopy(baseline)
    duplicate_pagination = forged_duplicate["provenance"]["pagination"]
    duplicate_pagination["page_attempts"][-1] = deepcopy(
        duplicate_pagination["page_attempts"][-2]
    )
    _assert_error(
        "V5_PAGE_LEDGER_DUPLICATE",
        validate_authenticated_available_history_boundary,
        duplicate_pagination,
        forged_duplicate["artifact_observation"],
        **_available_boundary_gate_kwargs(forged_duplicate),
    )


def test_public_available_boundary_gate_binds_leaf_and_manifest_source_identity() -> (
    None
):
    fixture = _terminal_prefix_tail_fixture(authenticated_inside_strategy=True)
    alternate_manifest_source = deepcopy(fixture["source_environment"])
    alternate_manifest_source["account"]["login"] = 43
    alternate_manifest_source["configured_connection"]["expected_login"] = 43
    assert (
        alternate_manifest_source["terminal"]["max_bars"]
        == fixture["provenance"]["source_environment"]["terminal"]["max_bars"]
    )
    kwargs = _available_boundary_gate_kwargs(fixture)
    kwargs["manifest_source_environment"] = alternate_manifest_source
    _assert_error(
        "V5_SOURCE_IDENTITY_MISMATCH",
        validate_authenticated_available_history_boundary,
        fixture["provenance"]["pagination"],
        fixture["artifact_observation"],
        **kwargs,
    )


def test_public_available_boundary_gate_uses_one_owned_pagination_snapshot() -> None:
    fixture = _terminal_prefix_tail_fixture(authenticated_inside_strategy=True)
    valid = fixture["provenance"]["pagination"]
    serialized = deepcopy(valid)
    serialized["page_attempts"][0]["request_sequence"] = 99
    serialized["terminal_prefix_unresolved_pages"][0]["request_sequence"] = 99

    class TwoViewPagination(Mapping[str, Any]):
        def __getitem__(self, key: str) -> Any:
            return valid[key]

        def __iter__(self):
            return iter(valid)

        def __len__(self) -> int:
            return len(serialized)

        def items(self):
            return serialized.items()

    two_view = TwoViewPagination()
    assert b'"request_sequence":99' in canonical_json_bytes(two_view)
    _assert_error(
        "V5_PAGE_REQUEST_SEQUENCE_INVALID:0",
        validate_authenticated_available_history_boundary,
        two_view,
        fixture["artifact_observation"],
        **_available_boundary_gate_kwargs(fixture),
    )


def test_available_boundary_cannot_launder_a_technical_or_detached_page() -> None:
    detached = build_valid_v5_contract_fixture(
        requested_information_cutoff_utc=CUTOFF,
        timeframe="H1",
    )
    detached["provenance"]["pagination"]["available_history_boundary"] = (
        _terminal_prefix_tail_fixture(authenticated_inside_strategy=True)["provenance"][
            "pagination"
        ]["available_history_boundary"]
    )
    _assert_error(
        "V5_AVAILABLE_HISTORY_BOUNDARY_WITHOUT_TERMINAL_PREFIX",
        _validate_cell,
        detached,
        "H1",
    )

    technical = _terminal_prefix_tail_fixture(authenticated_inside_strategy=True)
    pagination = technical["provenance"]["pagination"]
    first_data = pagination["page_attempts"][1]
    pagination["empty_pages"] = [deepcopy(first_data)]
    pagination["pages_with_data"] -= 1
    _assert_error(
        "V5_ARTIFACT_NONEMPTY_PAGE_MISCLASSIFIED:1",
        _validate_cell,
        technical,
        "H1",
    )


def test_activation_pagination_accepts_only_the_exact_prefix_stopped_suffix() -> None:
    from tools.audit_activate_native_history import _pagination_contract_errors

    fixture = _terminal_prefix_tail_fixture()
    kwargs = {
        "timeframe": "H1",
        "manifest_request": fixture["manifest"]["request"],
        "manifest_source_environment": fixture["source_environment"],
        "availability_class": "success",
        "failure_status": "boundary_history_shortfall",
    }
    assert _pagination_contract_errors(fixture["provenance"], **kwargs) == []

    forged = _terminal_prefix_tail_fixture()
    forged["provenance"]["pagination"]["page_attempts"][0]["request_sequence"] = 99
    assert "SOURCE_PAGINATION_REQUEST_SEQUENCE_INVALID" in _pagination_contract_errors(
        forged["provenance"], **kwargs
    )


def test_hard_failed_audit_prefix_preserves_only_the_h1_strategy_suffix() -> None:
    from tools.audit_activate_native_history import (
        _boundary_shortfall_integrity_errors,
        _pagination_contract_errors,
    )

    fixture = _technical_prefix_tail_fixture()
    assert _validate_cell(fixture, "H1") == CELL_BOUNDARY_SHORTFALL
    kwargs = {
        "timeframe": "H1",
        "manifest_request": fixture["manifest"]["request"],
        "manifest_source_environment": fixture["source_environment"],
        "availability_class": "success",
        "failure_status": "boundary_history_shortfall",
    }
    assert _pagination_contract_errors(fixture["provenance"], **kwargs) == []
    observation = fixture["artifact_observation"]
    frame = pd.DataFrame(
        {"Open": [1.0, 1.0]},
        index=pd.DatetimeIndex([observation.first_open_utc, observation.last_open_utc]),
    )
    assert _boundary_shortfall_integrity_errors(fixture["provenance"], frame) == []


def test_actual_boundary_lag_cannot_be_hidden_by_omitting_page_diagnostic() -> None:
    fixture = _boundary_fixture()
    provenance = fixture["provenance"]
    result = fixture["manifest_result"]
    provenance["pagination"]["truncated_pages"] = []
    provenance["coverage"]["truncated_page_count"] = 0
    provenance["coverage"]["boundary_truncated_page_count"] = 0
    provenance["status"] = result["status"] = "success"
    provenance["failure_status"] = result["failure_status"] = None
    provenance["complete"] = True
    provenance["eligible_for_downstream_validation"] = True
    result["eligible_for_downstream_validation"] = True
    provenance["coverage"]["status"] = "full_requested_span"
    _assert_error("V5_SUCCESS_PARTIAL_DIAGNOSTICS_PRESENT", _validate_cell, fixture)


def test_truncated_page_must_exceed_writer_boundary_tolerance() -> None:
    fixture = build_valid_v5_contract_fixture(requested_information_cutoff_utc=CUTOFF)
    page = deepcopy(fixture["provenance"]["pagination"]["page_attempts"][0])
    page.update(
        {
            "actual_first_open_utc": _iso(
                _dt(page["start_utc"]) + timedelta(minutes=5)
            ),
            "actual_last_open_utc": _iso(
                _dt(page["end_exclusive_utc"]) - timedelta(minutes=5)
            ),
            "start_delay_seconds": 300.0,
            "end_lag_seconds": 0.0,
        }
    )
    fixture["provenance"]["pagination"]["truncated_pages"] = [page]
    _assert_error("V5_TRUNCATED_PAGE_BELOW_TOLERANCE", _validate_cell, fixture)


def test_failed_page_can_never_be_published_as_partial_artifact() -> None:
    fixture = _technical_fixture()
    pagination = fixture["provenance"]["pagination"]
    pagination["failed_pages"] = pagination.pop("empty_pages")
    fixture["provenance"]["coverage"]["failed_page_count"] = 1
    fixture["provenance"]["coverage"]["internal_empty_page_count"] = 0
    _assert_error(
        "V5_PUBLISHABLE_FAILED_PAGE_PRESENT",
        _validate_cell,
        fixture,
    )


def test_artifact_empty_page_cannot_be_hidden_from_page_ledger() -> None:
    fixture = _technical_fixture()
    pagination = fixture["provenance"]["pagination"]
    pagination["empty_pages"] = []
    pagination["pages_with_data"] += 1
    fixture["provenance"]["coverage"]["internal_empty_page_count"] = 0
    _assert_error(
        "V5_ARTIFACT_EMPTY_PAGE_UNDECLARED:62",
        _validate_cell,
        fixture,
    )


def test_internal_truncated_page_can_never_validate_as_success() -> None:
    fixture = build_valid_v5_contract_fixture(requested_information_cutoff_utc=CUTOFF)
    provenance = fixture["provenance"]
    pagination = provenance["pagination"]
    old = fixture["artifact_observation"]
    page_observations = [
        {
            "start_utc": value[0],
            "end_exclusive_utc": value[1],
            "rows": value[2],
            "actual_first_open_utc": value[3],
            "actual_last_open_utc": value[4],
        }
        for value in old.page_observations
    ]
    index = len(page_observations) // 2
    observed = page_observations[index]
    page_start = _dt(observed["start_utc"])
    page_end = _dt(observed["end_exclusive_utc"])
    actual_last = page_end - timedelta(days=4, minutes=5)
    observed["actual_last_open_utc"] = _iso(actual_last)
    observed["rows"] = int((actual_last - page_start).total_seconds() // 300) + 1
    observation = build_v5_artifact_observation(
        sha256=old.sha256,
        bytes=old.bytes,
        rows=sum(value["rows"] for value in page_observations),
        first_open_utc=old.first_open_utc,
        last_open_utc=old.last_open_utc,
        last_close_utc=old.last_close_utc,
        columns=old.columns,
        page_observations=page_observations,
    )
    fixture["artifact_observation"] = observation
    fixture["artifact_observations"]["M5"] = observation
    provenance["rows"] = observation.rows
    provenance["artifact"]["rows"] = observation.rows
    fixture["manifest_result"]["rows"] = observation.rows
    declared = deepcopy(pagination["page_attempts"][index])
    declared.update(
        {
            "actual_first_open_utc": observed["actual_first_open_utc"],
            "actual_last_open_utc": observed["actual_last_open_utc"],
            "start_delay_seconds": 0.0,
            "end_lag_seconds": float(4 * 86_400),
        }
    )
    pagination["truncated_pages"] = [declared]
    provenance["coverage"].update(
        {
            "truncated_page_count": 1,
            "internal_truncated_page_count": 1,
            "technical_integrity_status": "internal_gap_detected",
        }
    )
    _assert_error(
        "V5_SUCCESS_PARTIAL_DIAGNOSTICS_PRESENT",
        _validate_cell,
        fixture,
    )


@pytest.mark.parametrize("diagnostic", ("empty_pages", "truncated_pages"))
def test_unbound_diagnostics_raise_contract_error_not_keyerror(
    diagnostic: str,
) -> None:
    fixture = build_valid_v5_contract_fixture(requested_information_cutoff_utc=CUTOFF)
    fake = {
        **deepcopy(fixture["provenance"]["pagination"]["page_attempts"][0]),
        "start_utc": "2000-01-01T00:00:00Z",
        "end_exclusive_utc": "2000-01-02T00:00:00Z",
    }
    if diagnostic == "truncated_pages":
        fake.update(
            {
                "actual_first_open_utc": "2000-01-01T00:00:00Z",
                "actual_last_open_utc": "2000-01-01T23:55:00Z",
                "start_delay_seconds": 0.0,
                "end_lag_seconds": 0.0,
            }
        )
    fixture["provenance"]["pagination"][diagnostic] = [fake]
    _assert_error(
        f"V5_PAGINATION_DIAGNOSTIC_UNBOUND:{diagnostic}:0",
        _validate_cell,
        fixture,
    )


@pytest.mark.parametrize(
    ("field", "value", "code"),
    (
        ("pages_requested", -1, "V5_PAGINATION_PAGE_COUNT_MISMATCH"),
        ("attempts_total", -1, "V5_PAGINATION_ATTEMPTS_TOTAL_MISMATCH"),
        (
            "copy_rates_range_attempts_total",
            10**30,
            "V5_PAGINATION_COPY_ATTEMPTS_TOTAL_MISMATCH",
        ),
    ),
)
def test_page_summary_counters_are_recomputed(
    field: str, value: Any, code: str
) -> None:
    fixture = build_valid_v5_contract_fixture(requested_information_cutoff_utc=CUTOFF)
    fixture["provenance"]["pagination"][field] = value
    _assert_error(code, _validate_cell, fixture)


def test_page_attempt_types_geometry_and_retry_budget_are_strict() -> None:
    fixture = build_valid_v5_contract_fixture(requested_information_cutoff_utc=CUTOFF)
    page = fixture["provenance"]["pagination"]["page_attempts"][0]
    page["attempts"] = -1
    _assert_error("V5_PAGE_ATTEMPTS_INVALID:0", _validate_cell, fixture)

    fixture = build_valid_v5_contract_fixture(requested_information_cutoff_utc=CUTOFF)
    fixture["provenance"]["pagination"]["page_attempts"][0]["end_exclusive_utc"] = (
        "2025-01-01T00:00:00Z"
    )
    _assert_error("V5_PAGE_LEDGER_GEOMETRY_MISMATCH:0", _validate_cell, fixture)

    fixture = build_valid_v5_contract_fixture(requested_information_cutoff_utc=CUTOFF)
    fixture["provenance"]["pagination"]["retries_per_page"] = 4
    _assert_error("V5_PAGINATION_RETRY_BUDGET_MISMATCH", _validate_cell, fixture)

    fixture = build_valid_v5_contract_fixture(requested_information_cutoff_utc=CUTOFF)
    fixture["provenance"]["pagination"]["page_span_seconds"] += 300
    _assert_error("V5_PAGINATION_PAGE_SPAN_MISMATCH", _validate_cell, fixture)

    fixture = build_valid_v5_contract_fixture(requested_information_cutoff_utc=CUTOFF)
    fixture["provenance"]["pagination"]["page_attempts"][0]["attempts"] = 2
    _assert_error("V5_PAGE_ATTEMPTS_INVALID:0", _validate_cell, fixture)

    fixture = build_valid_v5_contract_fixture(requested_information_cutoff_utc=CUTOFF)
    fixture["provenance"]["pagination"]["page_attempts"][0][
        "copy_rates_range_attempts"
    ] = 3
    _assert_error("V5_PAGE_COPY_ATTEMPTS_INVALID:0", _validate_cell, fixture)


def test_newest_first_request_sequence_and_planned_geometry_are_recomputed() -> None:
    fixture = build_valid_v5_contract_fixture(requested_information_cutoff_utc=CUTOFF)
    fixture["provenance"]["pagination"]["request_order"] = "oldest_to_newest"
    _assert_error("V5_PAGINATION_REQUEST_ORDER_INVALID", _validate_cell, fixture)

    fixture = build_valid_v5_contract_fixture(requested_information_cutoff_utc=CUTOFF)
    fixture["provenance"]["pagination"]["pages_planned"] -= 1
    _assert_error("V5_PAGE_LEDGER_PLANNED_COUNT_MISMATCH", _validate_cell, fixture)

    fixture = build_valid_v5_contract_fixture(requested_information_cutoff_utc=CUTOFF)
    fixture["provenance"]["pagination"]["page_attempts"][0]["request_sequence"] -= 1
    _assert_error("V5_PAGE_REQUEST_SEQUENCE_INVALID:0", _validate_cell, fixture)

    fixture = build_valid_v5_contract_fixture(requested_information_cutoff_utc=CUTOFF)
    fixture["provenance"]["pagination"]["page_attempts"].reverse()
    _assert_error("V5_PAGE_LEDGER_GEOMETRY_MISMATCH:0", _validate_cell, fixture)


def test_terminal_prefix_stop_cannot_be_forged_or_detached() -> None:
    fixture = build_valid_v5_contract_fixture(requested_information_cutoff_utc=CUTOFF)
    pagination = fixture["provenance"]["pagination"]
    pagination["stop_reason"] = "terminal_prefix_unresolved"
    _assert_error("V5_TERMINAL_PREFIX_EVIDENCE_INVALID", _validate_cell, fixture)

    fixture = build_valid_v5_contract_fixture(requested_information_cutoff_utc=CUTOFF)
    pagination = fixture["provenance"]["pagination"]
    pagination["unrequested_prefix"] = {
        "start_utc": fixture["provenance"]["request"]["native_fetch_start_utc"],
        "end_exclusive_utc": pagination["page_attempts"][0]["end_exclusive_utc"],
        "reason": "terminal_prefix_unresolved",
    }
    _assert_error("V5_UNREQUESTED_PREFIX_WITHOUT_STOP", _validate_cell, fixture)

    fixture = build_valid_v5_contract_fixture(requested_information_cutoff_utc=CUTOFF)
    pagination = fixture["provenance"]["pagination"]
    bound_page = pagination["page_attempts"][0]
    bound_page["copy_rates_range_attempts"] = 2
    bound_page["connector_diagnostics"] = {
        "sync_status": "terminal_prefix_unsynchronized",
        "raw_relation": "LEFT_DISJOINT_SUFFIX",
        "admitted_count": 0,
        "admitted_extent": None,
        "terminal_confirmation_consumed": True,
        "hard_failure": False,
    }
    page = deepcopy(bound_page)
    pagination["terminal_prefix_unresolved_pages"] = [page]
    pagination["stop_reason"] = "terminal_prefix_unresolved"
    pagination["pages_with_data"] -= 1
    pagination["copy_rates_range_attempts_total"] += 1
    _assert_error("V5_TERMINAL_PREFIX_DIAGNOSTICS_INVALID", _validate_cell, fixture)


def test_normal_page_connector_receipt_is_mandatory_and_recomputed() -> None:
    fixture = build_valid_v5_contract_fixture(requested_information_cutoff_utc=CUTOFF)
    page = fixture["provenance"]["pagination"]["page_attempts"][0]
    valid_diagnostics = deepcopy(page["connector_diagnostics"])
    common = {
        "start_utc": page["start_utc"],
        "end_exclusive_utc": page["end_exclusive_utc"],
        "symbol": "EURUSD",
        "broker_symbol": "EURUSD.a",
        "timeframe": "M5",
        "copy_attempts": page["copy_rates_range_attempts"],
        "category": "data",
    }
    request_contract.validate_mt5_v4_page_diagnostics(
        page["connector_diagnostics"], **common
    )

    fixture["provenance"]["pagination"]["page_attempts"][0].pop("connector_diagnostics")
    _assert_error("V5_PAGE_MT5_V4_DIAGNOSTICS_INVALID", _validate_cell, fixture)

    for label, mutate in (
        (
            "broker",
            lambda value: value.__setitem__("broker_symbol", "FORGED.BROKER"),
        ),
        (
            "relation",
            lambda value: (
                value.__setitem__("raw_relation", "RIGHT_DISJOINT_PREFIX"),
                value.__setitem__("initial_raw_relation", "RIGHT_DISJOINT_PREFIX"),
                value["call_ledger"][0].__setitem__(
                    "raw_relation", "RIGHT_DISJOINT_PREFIX"
                ),
            ),
        ),
        (
            "boundary",
            lambda value: (
                value["raw_extent"].update(
                    {
                        "first_epoch_second": (
                            value["request_start_epoch_second"]
                            + value["boundary_span_seconds"] // 2
                        ),
                        "last_epoch_second": (
                            value["request_start_epoch_second"]
                            + value["boundary_span_seconds"] // 2
                        ),
                    }
                ),
                value["admitted_extent"].update(value["raw_extent"]),
                value["best_extent"].update(value["raw_extent"]),
                value["call_ledger"][0]["raw_extent"].update(value["raw_extent"]),
                value["call_ledger"][0]["admitted_extent"].update(value["raw_extent"]),
            ),
        ),
        (
            "trigger_errors",
            lambda value: value.__setitem__(
                "history_download_trigger_errors", ["forged-error"]
            ),
        ),
    ):
        with pytest.raises(NativeRequestContractError) as caught:
            forged = deepcopy(valid_diagnostics)
            mutate(forged)
            request_contract.validate_mt5_v4_page_diagnostics(forged, **common)
        assert "V5_PAGE_MT5_V4_DIAGNOSTICS_INVALID" in caught.value.errors, label


def test_released_mt5_capture_error_mapping_is_an_exact_valid_last_error() -> None:
    captured = {
        "code": None,
        "message": "last_error unavailable",
        "capture_error": {"type": "RuntimeError", "message": "terminal stopped"},
    }
    assert request_contract._mt5_last_error_valid(captured)
    captured["capture_error"]["forged"] = True
    assert not request_contract._mt5_last_error_valid(captured)


@pytest.mark.parametrize(
    "postprocess_call", ("dataframe_conversion", "dataframe_half_open_guard")
)
def test_released_mt5_postprocess_failures_are_valid_exact_receipts(
    postprocess_call: str,
) -> None:
    fixture = build_valid_v5_contract_fixture(requested_information_cutoff_utc=CUTOFF)
    page = fixture["provenance"]["pagination"]["page_attempts"][0]
    receipt = deepcopy(page["connector_diagnostics"])
    receipt.update(
        {
            "hard_failure": True,
            "hard_failure_call": postprocess_call,
            "sync_status": "invalid_filtered_response",
        }
    )
    if postprocess_call == "dataframe_half_open_guard":
        receipt.update(
            {
                "admitted_count": 0,
                "admitted_extent": None,
                "best_extent": None,
                "boundary_complete": False,
            }
        )
    request_contract.validate_mt5_v4_page_diagnostics(
        receipt,
        start_utc=page["start_utc"],
        end_exclusive_utc=page["end_exclusive_utc"],
        symbol="EURUSD",
        broker_symbol="EURUSD.a",
        timeframe="M5",
        copy_attempts=page["copy_rates_range_attempts"],
        category="failed",
    )


def test_resource_limit_violations_are_never_accepted_as_clean_evidence() -> None:
    fixture = build_valid_v5_contract_fixture(requested_information_cutoff_utc=CUTOFF)
    fixture["provenance"]["pagination"]["initial_terminal_cache_resource_snapshot"] = {
        "status": "limit_exceeded",
        "violations": ["terminal_hcc_file_bound_breached"],
    }
    _assert_error("V5_INITIAL_RESOURCE_BOUND_INVALID", _validate_cell, fixture)

    fixture = build_valid_v5_contract_fixture(requested_information_cutoff_utc=CUTOFF)
    fixture["provenance"]["pagination"]["page_attempts"][0][
        "terminal_cache_resource_snapshot"
    ] = {
        "status": "limit_exceeded",
        "violations": ["terminal_disk_free_reserve_breached"],
    }
    _assert_error("V5_PAGE_RESOURCE_BOUND_INVALID:0", _validate_cell, fixture)

    mutations = (
        lambda snapshot: snapshot.__setitem__("status", "unavailable"),
        lambda snapshot: snapshot.__setitem__("status", "limit_exceeded"),
        lambda snapshot: snapshot.__setitem__("disk_free_bytes", -1),
        lambda snapshot: snapshot.__setitem__("largest_hcc_bytes", 3 * 1024**3),
        lambda snapshot: snapshot.__setitem__("hcc_entries_inspected", -1),
        lambda snapshot: snapshot.__setitem__("hcc_entries_inspected", 0),
        lambda snapshot: snapshot.__setitem__("hcc_entries_inspected", 1_000_001),
        lambda snapshot: snapshot["violations"].append("forged"),
        lambda snapshot: snapshot.__setitem__("scan_error", "forged"),
    )
    for index, mutate in enumerate(mutations):
        fixture = build_valid_v5_contract_fixture(
            requested_information_cutoff_utc=CUTOFF
        )
        snapshot = fixture["provenance"]["pagination"][
            "initial_terminal_cache_resource_snapshot"
        ]
        mutate(snapshot)
        _assert_error("V5_INITIAL_RESOURCE_BOUND_INVALID", _validate_cell, fixture)

    fixture = build_valid_v5_contract_fixture(requested_information_cutoff_utc=CUTOFF)
    fixture["provenance"]["pagination"].pop("initial_terminal_cache_resource_snapshot")
    _assert_error("V5_INITIAL_RESOURCE_BOUND_INVALID", _validate_cell, fixture)


def test_first_symbol_bounded_server_scope_round_trips_writer_snapshot() -> None:
    fixture = build_valid_v5_contract_fixture(requested_information_cutoff_utc=CUTOFF)
    pagination = fixture["provenance"]["pagination"]
    snapshots = [
        pagination["initial_terminal_cache_resource_snapshot"],
        *[
            page["terminal_cache_resource_snapshot"]
            for page in pagination["page_attempts"]
        ],
    ]
    for snapshot in snapshots:
        snapshot["scan_scope"] = "server"

    assert _validate_cell(fixture) == CELL_SUCCESS

    snapshots[0]["scan_scope"] = "unbounded"
    _assert_error("V5_INITIAL_RESOURCE_BOUND_INVALID", _validate_cell, fixture)


def test_all_zero_hcc_placeholders_round_trip_as_bounded_resource_evidence() -> None:
    fixture = build_valid_v5_contract_fixture(requested_information_cutoff_utc=CUTOFF)
    snapshot = fixture["provenance"]["pagination"]["page_attempts"][0][
        "terminal_cache_resource_snapshot"
    ]
    snapshot.update(
        {
            "hcc_files_inspected": 1,
            "hcc_entries_inspected": 2,
            "largest_hcc_bytes": 0,
            "largest_hcc_relative_path": None,
        }
    )

    assert _validate_cell(fixture) == CELL_SUCCESS

    snapshot["largest_hcc_bytes"] = 1
    _assert_error("V5_PAGE_RESOURCE_BOUND_INVALID:0", _validate_cell, fixture)


def test_diagnostic_duplicate_cannot_change_base_page_evidence() -> None:
    fixture = _boundary_fixture()
    pagination = fixture["provenance"]["pagination"]
    diagnostic = pagination["truncated_pages"][0]
    diagnostic["request_sequence"] = 999
    _assert_error(
        "V5_PAGINATION_DIAGNOSTIC_BASE_MISMATCH:truncated_pages:0",
        _validate_cell,
        fixture,
    )

    fixture = _boundary_fixture()
    pagination = fixture["provenance"]["pagination"]
    diagnostic = pagination["truncated_pages"][0]
    diagnostic["terminal_cache_resource_snapshot"]["disk_free_bytes"] -= 1
    _assert_error(
        "V5_PAGINATION_DIAGNOSTIC_BASE_MISMATCH:truncated_pages:0",
        _validate_cell,
        fixture,
    )

    fixture = _boundary_fixture()
    pagination = fixture["provenance"]["pagination"]
    diagnostic = pagination["truncated_pages"][0]
    diagnostic["connector_diagnostics"]["raw_count"] += 1
    _assert_error(
        "V5_PAGINATION_DIAGNOSTIC_BASE_MISMATCH:truncated_pages:0",
        _validate_cell,
        fixture,
    )


def test_output_resource_snapshot_is_mandatory_and_arithmetically_bound() -> None:
    mutations = (
        lambda value: value.__setitem__("status", "unavailable"),
        lambda value: value.__setitem__("estimated_cell_bytes", 65 * 1024**3),
        lambda value: value.__setitem__("disk_free_bytes", 4 * 1024**3),
        lambda value: value["violations"].append("forged"),
    )
    for mutate in mutations:
        fixture = build_valid_v5_contract_fixture(
            requested_information_cutoff_utc=CUTOFF
        )
        mutate(fixture["provenance"]["output_storage_resource_snapshot"])
        _assert_error("V5_OUTPUT_RESOURCE_BOUND_INVALID", _validate_cell, fixture)

    fixture = build_valid_v5_contract_fixture(requested_information_cutoff_utc=CUTOFF)
    fixture["provenance"].pop("output_storage_resource_snapshot")
    _assert_error("V5_OUTPUT_RESOURCE_BOUND_INVALID", _validate_cell, fixture)


def test_recomputed_page_rows_cannot_exceed_native_interval_capacity() -> None:
    fixture = build_valid_v5_contract_fixture(requested_information_cutoff_utc=CUTOFF)
    old = fixture["artifact_observation"]
    pages = [
        {
            "start_utc": value[0],
            "end_exclusive_utc": value[1],
            "rows": value[2],
            "actual_first_open_utc": value[3],
            "actual_last_open_utc": value[4],
        }
        for value in old.page_observations
    ]
    pages[0]["rows"] += 1
    pages[1]["rows"] -= 1
    observation = build_v5_artifact_observation(
        sha256=old.sha256,
        bytes=old.bytes,
        rows=old.rows,
        first_open_utc=old.first_open_utc,
        last_open_utc=old.last_open_utc,
        last_close_utc=old.last_close_utc,
        columns=old.columns,
        page_observations=pages,
    )
    kwargs = _cell_kwargs(fixture)
    kwargs["artifact_observation"] = observation
    _assert_error(
        "V5_ARTIFACT_PAGE_DENSITY_INVALID:0",
        validate_v5_cell_status,
        fixture["provenance"],
        fixture["manifest_result"],
        fixture["common_request"],
        **kwargs,
    )


def test_actual_artifact_hash_rows_bytes_columns_and_close_are_bound() -> None:
    fixture = build_valid_v5_contract_fixture(requested_information_cutoff_utc=CUTOFF)
    wrong_hash = replace(
        fixture["artifact_observation"],
        sha256="f" * 64,
    )
    kwargs = _cell_kwargs(fixture)
    kwargs["artifact_observation"] = wrong_hash
    _assert_error(
        "V5_ARTIFACT_BINDING_MISMATCH",
        validate_v5_cell_status,
        fixture["provenance"],
        fixture["manifest_result"],
        fixture["common_request"],
        **kwargs,
    )

    unexpected_columns = replace(
        fixture["artifact_observation"],
        columns=(*fixture["artifact_observation"].columns, "Unexpected"),
    )
    kwargs["artifact_observation"] = unexpected_columns
    _assert_error(
        "V5_ARTIFACT_COLUMNS_INVALID",
        validate_v5_cell_status,
        fixture["provenance"],
        fixture["manifest_result"],
        fixture["common_request"],
        **kwargs,
    )

    fixture = build_valid_v5_contract_fixture(requested_information_cutoff_utc=CUTOFF)
    fixture["provenance"]["coverage"]["actual_last_close_utc"] = "2026-07-13T08:00:00Z"
    _assert_error(
        "V5_COVERAGE_ARTIFACT_BINDING_MISMATCH:actual_last_close_utc",
        _validate_cell,
        fixture,
    )

    _assert_error(
        "V5_ARTIFACT_ROWS_INVALID",
        build_v5_artifact_observation,
        sha256="a" * 64,
        bytes=1,
        rows=0,
        first_open_utc="2026-01-01T00:00:00Z",
        last_open_utc="2026-01-01T00:00:00Z",
        last_close_utc="2026-01-01T00:05:00Z",
        columns=["Open", "High", "Low", "Close", "_BAR_CLOSE_TS"],
    )


@pytest.mark.parametrize(
    "field",
    (
        "duplicate_open_count",
        "non_monotonic_open_count",
        "invalid_bar_close_count",
        "nonfinite_ohlc_count",
        "invalid_ohlc_count",
        "missing_required_value_count",
        "out_of_request_count",
        "page_assignment_error_count",
        "nonpositive_price_count",
        "negative_volume_count",
        "negative_spread_count",
    ),
)
def test_recomputed_artifact_quality_failures_are_never_publishable(
    field: str,
) -> None:
    fixture = build_valid_v5_contract_fixture(requested_information_cutoff_utc=CUTOFF)
    bad_observation = replace(fixture["artifact_observation"], **{field: 1})
    kwargs = _cell_kwargs(fixture)
    kwargs["artifact_observation"] = bad_observation
    _assert_error(
        f"V5_ARTIFACT_QUALITY_FAILED:{field}",
        validate_v5_cell_status,
        fixture["provenance"],
        fixture["manifest_result"],
        fixture["common_request"],
        **kwargs,
    )


def test_future_close_and_forged_coverage_metrics_cannot_return_success() -> None:
    fixture = build_valid_v5_contract_fixture(requested_information_cutoff_utc=CUTOFF)
    old = fixture["artifact_observation"]
    future = build_v5_artifact_observation(
        sha256=old.sha256,
        bytes=old.bytes,
        rows=old.rows,
        first_open_utc=old.first_open_utc,
        last_open_utc="2026-07-13T09:00:00Z",
        last_close_utc="2026-07-13T09:05:00Z",
        columns=old.columns,
        page_observations=[
            {
                "start_utc": value[0],
                "end_exclusive_utc": value[1],
                "rows": value[2],
                "actual_first_open_utc": value[3],
                "actual_last_open_utc": value[4],
            }
            for value in old.page_observations
        ],
    )
    kwargs = _cell_kwargs(fixture)
    kwargs["artifact_observation"] = future
    _assert_error(
        "V5_ARTIFACT_CLOSURE_OUTSIDE_REQUEST",
        validate_v5_cell_status,
        fixture["provenance"],
        fixture["manifest_result"],
        fixture["common_request"],
        **kwargs,
    )

    fixture = build_valid_v5_contract_fixture(requested_information_cutoff_utc=CUTOFF)
    fixture["provenance"]["coverage"]["calendar_span_coverage_ratio"] = 0.5
    _assert_error(
        "V5_COVERAGE_METRIC_MISMATCH:calendar_span_coverage_ratio",
        _validate_cell,
        fixture,
    )


@pytest.mark.parametrize(
    ("record", "field", "value", "code"),
    (
        (
            "provenance",
            "tradeable",
            True,
            "V5_PROVENANCE_AUTHORITY_CLAIM_PRESENT:tradeable",
        ),
        (
            "manifest_result",
            "production_eligible",
            True,
            "V5_MANIFEST_AUTHORITY_CLAIM_PRESENT:production_eligible",
        ),
        (
            "provenance",
            "statistical_sufficiency_assessed",
            True,
            "V5_PROVENANCE_FIELD_INVALID:statistical_sufficiency_assessed",
        ),
    ),
)
def test_acquisition_cannot_self_authorize_trading_or_statistics(
    record: str, field: str, value: Any, code: str
) -> None:
    fixture = build_valid_v5_contract_fixture(requested_information_cutoff_utc=CUTOFF)
    fixture[record][field] = value
    _assert_error(code, _validate_cell, fixture)


def test_status_failure_and_boolean_fields_are_exact_not_truthy() -> None:
    fixture = build_valid_v5_contract_fixture(requested_information_cutoff_utc=CUTOFF)
    fixture["provenance"]["status"] = fixture["manifest_result"]["status"] = "SUCCESS"
    _assert_error("V5_PUBLISHABLE_STATUS_INVALID", _validate_cell, fixture)

    fixture = _boundary_fixture()
    fixture["provenance"]["failure_status"] = fixture["manifest_result"][
        "failure_status"
    ] = {"forged": True}
    _assert_error("V5_FAILURE_STATUS_TYPE_INVALID", _validate_cell, fixture)

    fixture = build_valid_v5_contract_fixture(requested_information_cutoff_utc=CUTOFF)
    fixture["provenance"]["immutable_record"] = 1
    _assert_error(
        "V5_PROVENANCE_FIELD_INVALID:immutable_record",
        _validate_cell,
        fixture,
    )


def test_manifest_full_roster_counts_and_result_summaries_are_exact() -> None:
    fixture = build_valid_v5_contract_fixture(requested_information_cutoff_utc=CUTOFF)
    fixture["manifest"]["results"].pop()
    _assert_error(
        "V5_MANIFEST_RESULT_ROSTER_SIZE_INVALID",
        validate_v5_production_manifest,
        fixture["manifest"],
        fixture["trusted_policy"],
    )

    fixture = build_valid_v5_contract_fixture(requested_information_cutoff_utc=CUTOFF)
    fixture["manifest"]["counts"]["success"] = 5
    _assert_error(
        "V5_MANIFEST_COUNT_MISMATCH:success",
        validate_v5_production_manifest,
        fixture["manifest"],
        fixture["trusted_policy"],
    )

    fixture = build_valid_v5_contract_fixture(requested_information_cutoff_utc=CUTOFF)
    fixture["manifest_result"]["rows"] = 0
    _assert_error(
        "V5_MANIFEST_RESULT_ROWS_INVALID:EURUSD:M5",
        validate_v5_production_manifest,
        fixture["manifest"],
        fixture["trusted_policy"],
    )

    fixture = build_valid_v5_contract_fixture(requested_information_cutoff_utc=CUTOFF)
    fixture["manifest_result"]["relative_leaf"] = "../../outside"
    _assert_error(
        "V5_MANIFEST_RESULT_PATH_INVALID:EURUSD:M5",
        validate_v5_production_manifest,
        fixture["manifest"],
        fixture["trusted_policy"],
    )


def test_manifest_result_resume_and_timestamp_schemas_are_exact() -> None:
    fixture = build_valid_v5_contract_fixture(requested_information_cutoff_utc=CUTOFF)
    fixture["manifest"]["tradeable"] = True
    _assert_error(
        "V5_MANIFEST_SCHEMA_FIELDS_INVALID",
        validate_v5_production_manifest,
        fixture["manifest"],
        fixture["trusted_policy"],
    )

    fixture = build_valid_v5_contract_fixture(requested_information_cutoff_utc=CUTOFF)
    fixture["manifest_result"]["unversioned_quality"] = {"pass": True}
    _assert_error(
        "V5_MANIFEST_RESULT_SCHEMA_INVALID:EURUSD:M5",
        validate_v5_production_manifest,
        fixture["manifest"],
        fixture["trusted_policy"],
    )

    fixture = build_valid_v5_contract_fixture(requested_information_cutoff_utc=CUTOFF)
    fixture["manifest"]["resume"]["newly_acquired_cells"] -= 1
    _assert_error(
        "V5_MANIFEST_RESUME_COUNT_MISMATCH:newly_acquired_cells",
        validate_v5_production_manifest,
        fixture["manifest"],
        fixture["trusted_policy"],
    )

    fixture = build_valid_v5_contract_fixture(requested_information_cutoff_utc=CUTOFF)
    fixture["manifest"]["acquisition_completed_utc"] = "2026-07-13T08:00:00Z"
    _assert_error(
        "V5_MANIFEST_ACQUISITION_ORDER_INVALID",
        validate_v5_production_manifest,
        fixture["manifest"],
        fixture["trusted_policy"],
    )


def test_complete_production_manifest_rejects_every_failed_cell() -> None:
    fixture = build_valid_v5_contract_fixture(requested_information_cutoff_utc=CUTOFF)
    result = fixture["manifest_result"]
    result.update(
        {
            "status": "failed",
            "failure_status": "terminal_history_empty_unresolved",
            "rows": 0,
            "sha256": None,
            "eligible_for_downstream_validation": False,
        }
    )
    _refresh_manifest_counts(fixture)
    _assert_error(
        "V5_MANIFEST_RESULT_STATUS_INVALID:EURUSD:M5",
        validate_v5_production_manifest,
        fixture["manifest"],
        fixture["trusted_policy"],
    )


def test_failed_empty_envelope_is_bound_and_cannot_be_downgraded_or_forged() -> None:
    def validate(fixture: dict[str, Any]) -> str:
        return request_contract.validate_v5_failed_cell_envelope(
            fixture["provenance"],
            fixture["manifest_result"],
            fixture["common_request"],
            symbol="EURUSD",
            timeframe="M5",
            run_id=fixture["manifest"]["run_id"],
            manifest_source_environment=fixture["manifest"]["source_environment"],
            expected_retries=1,
            manifest_acquisition_started_utc=fixture["manifest"][
                "acquisition_started_utc"
            ],
            manifest_acquisition_completed_utc=fixture["manifest"][
                "acquisition_completed_utc"
            ],
        )

    fixture = _failed_empty_fixture()
    assert validate(fixture) == "terminal_history_empty_unresolved"

    fixture = _failed_empty_fixture()
    fixture["provenance"]["economic_verdict"] = "guaranteed_profitable"
    _assert_error("V5_FAILED_PROVENANCE_SCHEMA_INVALID", validate, fixture)

    fixture = _failed_empty_fixture()
    fixture["provenance"]["request"].pop("audit_windows")
    _assert_error(
        "V5_LEAF_PROJECTION_MISMATCH:audit_windows",
        validate,
        fixture,
    )

    fixture = _failed_empty_fixture()
    fixture["provenance"]["pagination"]["empty_pages"].pop()
    _assert_error("V5_TERMINAL_EMPTY_EVIDENCE_INVALID", validate, fixture)

    fixture = _failed_empty_fixture()
    fixture["provenance"]["acquisition_started_utc"] = "2020-01-01T00:00:00Z"
    _assert_error(
        "V5_FAILED_ACQUISITION_INTERVAL_OUTSIDE_MANIFEST",
        validate,
        fixture,
    )

    fixture = _failed_empty_fixture()
    fixture["provenance"]["source_environment"]["account"]["server"] = "Forged"
    _assert_error("V5_REDACTED_SOURCE_ACCOUNT_INVALID", validate, fixture)


def test_terminal_technical_cell_replays_exact_two_receipts() -> None:
    fixture = _terminal_technical_fixture()

    assert (
        validate_v5_terminal_technical_cell(
            fixture["provenance"],
            fixture["manifest_result"],
            fixture["common_request"],
            symbol="EURUSD",
            timeframe="M5",
            run_id=fixture["manifest"]["run_id"],
            manifest_source_environment=fixture["source_environment"],
            expected_retries=1,
            manifest_acquisition_started_utc=fixture["manifest"][
                "acquisition_started_utc"
            ],
            manifest_acquisition_completed_utc=fixture["manifest"][
                "acquisition_completed_utc"
            ],
        )
        == "technical_history_gap"
    )
    validate_v5_production_manifest(fixture["manifest"], fixture["trusted_policy"])
    assert (
        validate_v5_production_cell(
            manifest=fixture["manifest"],
            provenance=fixture["provenance"],
            manifest_result=fixture["manifest_result"],
            artifact_observation=None,
            trusted_policy=fixture["trusted_policy"],
            symbol="EURUSD",
            timeframe="M5",
        )
        == "technical_history_gap"
    )


@pytest.mark.parametrize(
    ("mutation", "code"),
    [
        (
            lambda fixture: fixture["provenance"]["terminal_technical_evidence"][
                "receipts"
            ].pop(),
            "V5_TERMINAL_TECHNICAL_RECEIPT_COUNT_INVALID",
        ),
        (
            lambda fixture: fixture["provenance"]["terminal_technical_evidence"][
                "receipts"
            ][1]["envelope"].update({"retention_status": "summary_only"}),
            "V5_TERMINAL_ATTEMPT_ENVELOPE_BINDING_INVALID:1",
        ),
        (
            lambda fixture: fixture["provenance"]["terminal_technical_evidence"][
                "receipts"
            ][1].update(
                deepcopy(
                    fixture["provenance"]["terminal_technical_evidence"]["receipts"][0]
                )
            ),
            "V5_TERMINAL_ATTEMPT_DUPLICATE_EVIDENCE",
        ),
        (
            lambda fixture: fixture["provenance"]["terminal_technical_evidence"][
                "receipts"
            ][1].update(
                {
                    "relative_path": fixture["provenance"][
                        "terminal_technical_evidence"
                    ]["receipts"][0]["relative_path"]
                }
            ),
            "V5_TERMINAL_ATTEMPT_DUPLICATE_PATH",
        ),
        (
            lambda fixture: fixture["provenance"]["terminal_technical_evidence"][
                "receipts"
            ][1]["envelope"].update({"attempt_id": "f" * 32}),
            "V5_TERMINAL_ATTEMPT_PATH_ID_MISMATCH:1",
        ),
        (
            lambda fixture: fixture["provenance"]["terminal_technical_evidence"][
                "receipts"
            ][1]["envelope"]["record"].update({"broker_symbol": "FORGED.a"}),
            "V5_TERMINAL_ATTEMPT_BROKER_SYMBOL_MISMATCH",
        ),
        (
            lambda fixture: fixture["provenance"]["terminal_technical_evidence"][
                "receipts"
            ][1]["envelope"]["record"].update({"instrument": {"forged": True}}),
            "V5_TERMINAL_ATTEMPT_INSTRUMENT_MISMATCH",
        ),
        (
            lambda fixture: fixture["manifest_result"].update(
                {"terminal_evidence_sha256": "f" * 64}
            ),
            "V5_TERMINAL_TECHNICAL_MANIFEST_EVIDENCE_MISMATCH",
        ),
    ],
)
def test_terminal_technical_receipt_forgery_fails_closed(
    mutation: Callable[[dict[str, Any]], Any], code: str
) -> None:
    fixture = _terminal_technical_fixture()
    mutation(fixture)
    _assert_error(
        code,
        validate_v5_terminal_technical_cell,
        fixture["provenance"],
        fixture["manifest_result"],
        fixture["common_request"],
        symbol="EURUSD",
        timeframe="M5",
        run_id=fixture["manifest"]["run_id"],
        manifest_source_environment=fixture["source_environment"],
        expected_retries=1,
        manifest_acquisition_started_utc=fixture["manifest"]["acquisition_started_utc"],
        manifest_acquisition_completed_utc=fixture["manifest"][
            "acquisition_completed_utc"
        ],
    )


def test_terminal_technical_attempts_must_be_sequential_retries() -> None:
    fixture = _terminal_technical_fixture()
    receipts = fixture["provenance"]["terminal_technical_evidence"]["receipts"]
    first = receipts[0]["envelope"]["record"]
    second = receipts[1]["envelope"]["record"]
    second["acquisition_started_utc"] = first["acquisition_started_utc"]
    second["acquisition_completed_utc"] = first["acquisition_completed_utc"]
    _reseal_terminal_technical_fixture(fixture)

    _assert_error(
        "V5_TERMINAL_ATTEMPT_SEQUENCE_INVALID",
        validate_v5_terminal_technical_cell,
        fixture["provenance"],
        fixture["manifest_result"],
        fixture["common_request"],
        symbol="EURUSD",
        timeframe="M5",
        run_id=fixture["manifest"]["run_id"],
        manifest_source_environment=fixture["source_environment"],
        expected_retries=1,
        manifest_acquisition_started_utc=fixture["manifest"]["acquisition_started_utc"],
        manifest_acquisition_completed_utc=fixture["manifest"][
            "acquisition_completed_utc"
        ],
    )


def test_failed_nonterminal_cell_remains_forbidden_in_complete_manifest() -> None:
    fixture = _terminal_technical_fixture()
    fixture["manifest_result"].pop("terminal_evidence_sha256")
    fixture["manifest_result"].pop("terminal_evidence_bytes")
    fixture["manifest_result"]["resume_disposition"] = "newly_acquired"
    fixture["manifest"]["resume"].pop("terminalized_technical_cells")
    fixture["manifest"]["resume"]["newly_acquired_cells"] = 600

    _assert_error(
        "V5_MANIFEST_RESULT_STATUS_INVALID:EURUSD:M5",
        validate_v5_production_manifest,
        fixture["manifest"],
        fixture["trusted_policy"],
    )


def test_production_cell_binds_exact_manifest_result_and_run_id() -> None:
    fixture = build_valid_v5_contract_fixture(requested_information_cutoff_utc=CUTOFF)
    detached = deepcopy(fixture["manifest_result"])
    detached["sha256"] = "f" * 64
    _assert_error(
        "V5_PRODUCTION_RESULT_BINDING_MISMATCH",
        validate_v5_production_cell,
        manifest=fixture["manifest"],
        provenance=fixture["provenance"],
        manifest_result=detached,
        artifact_observation=fixture["artifact_observation"],
        trusted_policy=fixture["trusted_policy"],
        symbol="EURUSD",
        timeframe="M5",
    )
    fixture["provenance"]["run_id"] = "different-run"
    _assert_error(
        "V5_PRODUCTION_RUN_BINDING_MISMATCH",
        validate_v5_production_cell,
        manifest=fixture["manifest"],
        provenance=fixture["provenance"],
        manifest_result=fixture["manifest_result"],
        artifact_observation=fixture["artifact_observation"],
        trusted_policy=fixture["trusted_policy"],
        symbol="EURUSD",
        timeframe="M5",
    )


def test_resource_caps_reject_deep_wide_long_and_huge_numeric_json() -> None:
    nested: Any = None
    for _ in range(MAX_CANONICAL_DEPTH + 2):
        nested = [nested]
    _assert_error("CANONICAL_JSON_DEPTH_EXCEEDED", canonical_json_bytes, nested)
    _assert_error(
        "CANONICAL_JSON_LIST_CAP_EXCEEDED",
        canonical_json_bytes,
        [None] * (MAX_CONTAINER_ITEMS + 1),
    )
    _assert_error(
        "CANONICAL_JSON_STRING_CAP_EXCEEDED",
        canonical_json_bytes,
        "x" * (MAX_CANONICAL_STRING_CHARS + 1),
    )
    _assert_error(
        "CANONICAL_JSON_INTEGER_CAP_EXCEEDED",
        canonical_json_bytes,
        1 << (MAX_CANONICAL_INTEGER_BITS + 1),
    )
    shared_long = "x" * MAX_CANONICAL_STRING_CHARS
    _assert_error(
        "CANONICAL_JSON_CHAR_CAP_EXCEEDED",
        canonical_json_bytes,
        [shared_long] * 129,
    )


def test_cyclic_json_and_oversized_page_inputs_fail_with_typed_verdicts() -> None:
    cyclic_list: list[Any] = []
    cyclic_list.append(cyclic_list)
    _assert_error(
        "CANONICAL_JSON_DEPTH_EXCEEDED",
        canonical_json_bytes,
        cyclic_list,
    )
    cyclic_map: dict[str, Any] = {}
    cyclic_map["self"] = cyclic_map
    _assert_error(
        "CANONICAL_JSON_DEPTH_EXCEEDED",
        validate_v5_common_request,
        cyclic_map,
    )

    fixture = build_valid_v5_contract_fixture(requested_information_cutoff_utc=CUTOFF)
    page = fixture["provenance"]["pagination"]["page_attempts"][0]
    fixture["provenance"]["pagination"]["page_attempts"] = [page] * 10_001
    _assert_error(
        "CANONICAL_JSON_LIST_CAP_EXCEEDED",
        _validate_cell,
        fixture,
    )

    observation = fixture["artifact_observation"]
    page_observation = {
        "start_utc": observation.page_observations[0][0],
        "end_exclusive_utc": observation.page_observations[0][1],
        "rows": 1,
        "actual_first_open_utc": observation.page_observations[0][3],
        "actual_last_open_utc": observation.page_observations[0][3],
    }
    _assert_error(
        "V5_ARTIFACT_PAGE_OBSERVATIONS_INVALID",
        build_v5_artifact_observation,
        sha256="a" * 64,
        bytes=1,
        rows=10_001,
        first_open_utc=observation.first_open_utc,
        last_open_utc=observation.last_open_utc,
        last_close_utc=observation.last_close_utc,
        columns=observation.columns,
        page_observations=[page_observation] * 10_001,
    )


@pytest.mark.parametrize(
    ("kwargs", "code"),
    (
        (
            {"requested_information_cutoff_utc": 10**100_000},
            "REQUESTED_INFORMATION_CUTOFF_INVALID",
        ),
        (
            {"requested_information_cutoff_utc": "9999-01-01T00:00:00Z"},
            "REQUESTED_INFORMATION_CUTOFF_INVALID",
        ),
        (
            {"requested_information_cutoff_utc": "0001-01-01T00:00:00Z"},
            "REQUESTED_INFORMATION_CUTOFF_INVALID",
        ),
        (
            {"requested_information_cutoff_utc": CUTOFF, "horizon_years": 10**100},
            "V5_HORIZON_YEARS_INVALID",
        ),
        (
            {"requested_information_cutoff_utc": CUTOFF, "warmup_days": float("inf")},
            "V5_WARMUP_DAYS_INVALID",
        ),
    ),
)
def test_extreme_date_and_numeric_request_inputs_are_typed_contract_errors(
    kwargs: dict[str, Any], code: str
) -> None:
    _assert_error(code, build_v5_request_contract, **kwargs)


def test_extreme_per_timeframe_year_is_rejected_before_date_arithmetic() -> None:
    years = deepcopy(DEFAULT_TIMEFRAME_WINDOW_YEARS)
    years["M5"]["optimizer_train"] = 10**100
    _assert_error(
        "V5_TIMEFRAME_YEAR_INVALID:M5:optimizer_train",
        build_v5_request_contract,
        requested_information_cutoff_utc=CUTOFF,
        timeframe_window_years=years,
    )


@pytest.mark.parametrize(
    ("name", "replacement"),
    (
        ("PRODUCTION_SYMBOL_ORDER", ("EURUSD",)),
        ("_AUTHORITY_TIMEFRAME_ORDER", tuple(reversed(PRODUCTION_TIMEFRAME_ORDER))),
        ("COMMON_REQUEST_KEYS", ()),
        ("_AUTHORITY_CAPS", (1,)),
        ("MAX_TOTAL_ARTIFACT_BYTES", 1),
        ("TERMINAL_HCC_MAX_FILE_BYTES", 1),
        ("TERMINAL_MIN_FREE_BYTES", 1),
        ("MAX_TERMINAL_HCC_SCAN_ENTRIES", 1),
        ("OUTPUT_MIN_FREE_BYTES", 1),
        ("OUTPUT_MAX_CELL_BYTES", 1),
        ("canonical_source_identity", lambda value: {"forged": value}),
        ("_mt5_v4_diagnostics_errors", lambda *args, **kwargs: []),
        ("_terminal_attempt_receipt_payload", lambda envelope: b"forged\n"),
        ("_same", lambda left, right: True),
    ),
)
def test_public_private_and_function_global_rebinding_fails_closed(
    monkeypatch: pytest.MonkeyPatch,
    name: str,
    replacement: Any,
) -> None:
    monkeypatch.setattr(request_contract, name, replacement)
    with pytest.raises(NativeRequestContractError) as caught:
        build_v5_request_contract(requested_information_cutoff_utc=CUTOFF)
    assert any(code.startswith("V5_RUNTIME_") for code in caught.value.errors)


def test_mutating_imported_timeframe_policy_fails_closed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setitem(request_contract.TIMEFRAME_SECONDS, "M5", 60)
    _assert_error(
        "V5_RUNTIME_AUTHORITY_REBOUND",
        build_v5_request_contract,
        requested_information_cutoff_utc=CUTOFF,
    )


def _random_json(rng: random.Random, depth: int = 0) -> Any:
    atoms: list[Any] = [
        None,
        True,
        False,
        -1,
        0,
        1,
        10**100,
        0.0,
        1.5,
        float("nan"),
        float("inf"),
        "",
        "M5",
        "2026-07-13T09:00:00Z",
        "x" * 200,
    ]
    if depth >= 4 or rng.random() < 0.55:
        return deepcopy(rng.choice(atoms))
    if rng.random() < 0.5:
        return [_random_json(rng, depth + 1) for _ in range(rng.randrange(5))]
    return {
        f"k{index}": _random_json(rng, depth + 1) for index in range(rng.randrange(5))
    }


def test_public_validators_are_total_over_generative_arbitrary_json() -> None:
    rng = random.Random(0xF5C0DE)
    validators: tuple[Callable[[Any], Any], ...] = (
        canonical_json_bytes,
        canonical_sha256,
        canonical_symbol,
        canonical_symbols,
        canonical_source_identity,
        timeframe_request_hash_payload,
        lambda value: build_v5_request_contract(
            requested_information_cutoff_utc=value,
            timeframe_window_years=value,
            horizon_years=value,
            embargo_days=value,
            warmup_days=value,
            safety_days=value,
            boundary_tolerance_days=value,
        ),
        lambda value: build_v5_leaf_request(
            value,
            value,
            last_fully_closed_bar_open_utc=value,
        ),
        lambda value: build_v5_manifest_request(
            value,
            symbols=value,
            retries=value,
            timeframes=value,
        ),
        lambda value: build_trusted_v5_policy(
            expected_symbols=value,
            expected_source_environment=value,
            requested_information_cutoff_utc=value,
            timeframe_order=value,
            retries=value,
            timeframe_window_years=value,
            horizon_years=value,
            embargo_days=value,
            warmup_days=value,
            safety_days=value,
            boundary_tolerance_days=value,
        ),
        lambda value: build_v5_artifact_observation(
            sha256=value,
            bytes=value,
            rows=value,
            first_open_utc=value,
            last_open_utc=value,
            last_close_utc=value,
            columns=value,
            page_observations=value,
            duplicate_open_count=value,
        ),
        lambda value: build_valid_v5_contract_fixture(
            symbol=value,
            timeframe=value,
            requested_information_cutoff_utc=value,
            source_environment=value,
        ),
        validate_v5_common_request,
        lambda value: validate_v5_leaf_request(value, value, value),
        lambda value: validate_source_identity_match(value, value),
        lambda value: validate_v5_manifest_request(value, value),
        lambda value: validate_v5_production_manifest(value, value),
        lambda value: validate_v5_cell_status(
            value,
            value,
            value,
            symbol=value,
            timeframe=value,
            manifest_source_environment=value,
            artifact_observation=value,
            expected_retries=value,
        ),
        lambda value: validate_v5_production_cell(
            manifest=value,
            provenance=value,
            manifest_result=value,
            artifact_observation=value,
            trusted_policy=value,
            symbol=value,
            timeframe=value,
        ),
    )
    for _ in range(250):
        value = _random_json(rng)
        for validator in validators:
            try:
                validator(deepcopy(value))
            except Exception as exc:  # noqa: BLE001 - this is the property.
                assert isinstance(exc, NativeRequestContractError), (
                    validator,
                    value,
                    type(exc),
                )
    cyclic_list: list[Any] = []
    cyclic_list.append(cyclic_list)
    cyclic_map: dict[str, Any] = {}
    cyclic_map["self"] = cyclic_map
    for value in (cyclic_list, cyclic_map):
        for validator in validators:
            try:
                validator(value)
            except Exception as exc:  # noqa: BLE001 - this is the property.
                assert isinstance(exc, NativeRequestContractError), (
                    validator,
                    type(exc),
                )
