from __future__ import annotations

import copy
import csv
import hashlib
import json
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from functools import lru_cache
from pathlib import Path
from types import SimpleNamespace

import pytest

import pm_authority_spool as spool
import pm_authoritative_search_execution as execution
import pm_core
import pm_native_prefix_production as prefix
import pm_native_request_contract as request_contract
from pm_native_activation import ActiveNativeResolution
from pm_native_request_contract import (
    ACQUISITION_CONTRACT_VERSION,
    PRODUCTION_ARTIFACT_COLUMNS,
    PRODUCTION_TIMEFRAME_ORDER,
    TIMEFRAME_SECONDS,
    build_valid_v5_contract_fixture,
)
from pm_training_horizon import _normalize_native_prefix


UTC = timezone.utc
SYMBOL = "EURUSD"
ACTIVATION_ID = "native-activation-prefix-test"
SOURCE_ACCESSED = "2026-01-10T00:00:00Z"


def _sha(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def _canonical(value: object) -> bytes:
    return json.dumps(
        value,
        allow_nan=False,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("ascii")


def _source_environment(*, login: int = 42) -> dict[str, object]:
    return {
        "metadata_status": "available",
        "account": {
            "login": login,
            "server": "TestBroker-Real",
            "company": "TestBroker",
            "currency": "USD",
        },
        "configured_connection": {
            "server": "TestBroker-Real",
            "terminal_path": r"C:\TestBroker\terminal64.exe",
            "expected_login": login,
            "expected_server": "TestBroker-Real",
            "expected_currency": "USD",
        },
        "terminal": {
            "name": "Test Terminal",
            "company": "TestBroker",
            "path": r"C:\TestBroker",
            "data_path": r"C:\TestBroker\Data",
            "commondata_path": r"C:\ProgramData\TestBroker",
            "build": 5432,
            "version": [5, 5432, "13 Jul 2026"],
            "max_bars": 100_000,
        },
    }


def _iso(value: datetime) -> str:
    return value.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def _write_json(path: Path, value: object) -> str:
    path.parent.mkdir(parents=True, exist_ok=True)
    raw = (json.dumps(value, indent=2, sort_keys=True) + "\n").encode("utf-8")
    path.write_bytes(raw)
    return _sha(raw)


def _write_activation(
    data_dir: Path,
    timeframe: str,
    *,
    provenance_login: int = 42,
    run_login: int = 42,
) -> dict[str, str]:
    native = data_dir / "native"
    leaf = native / "runs" / "run-prefix" / SYMBOL / timeframe
    leaf.mkdir(parents=True)
    cadence = TIMEFRAME_SECONDS[timeframe]
    start = datetime(2026, 1, 1, tzinfo=UTC)
    rows: list[list[object]] = []
    for ordinal in range(5):
        opened = start + timedelta(seconds=ordinal * cadence)
        closed = opened + timedelta(seconds=cadence)
        price = 1.1 + ordinal * 0.001
        rows.append(
            [
                _iso(opened),
                f"{price:.6f}",
                f"{price + 0.002:.6f}",
                f"{price - 0.002:.6f}",
                f"{price + 0.001:.6f}",
                "100",
                "12",
                "80",
                _iso(opened),
                _iso(opened),
                _iso(closed),
            ]
        )
    bars = leaf / "bars.csv"
    with bars.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle, lineterminator="\n")
        writer.writerow(("time", *PRODUCTION_ARTIFACT_COLUMNS))
        writer.writerows(rows)
    bars_raw = bars.read_bytes()
    bars_sha = _sha(bars_raw)
    request_map_sha = _sha(f"request-map-{timeframe}".encode("ascii"))
    request_start = _iso(start)
    request_end = _iso(start + timedelta(seconds=5 * cadence))
    provenance = {
        "schema_version": "fxpm-native-bars-v1",
        "acquisition_contract_version": ACQUISITION_CONTRACT_VERSION,
        "immutable_record": True,
        "run_id": "run-prefix",
        "symbol": SYMBOL,
        "canonical_symbol": SYMBOL,
        "timeframe": timeframe,
        "timeframe_seconds": cadence,
        "source_kind": "broker_native",
        "timezone": "UTC",
        "timestamp_semantics": "bar_open_utc",
        "bar_interval_semantics": "half_open_[bar_open,bar_close)",
        "bar_close_column": "_BAR_CLOSE_TS",
        "status": "success",
        "artifact": {
            "relative_path": "bars.csv",
            "sha256": bars_sha,
            "bytes": len(bars_raw),
            "rows": len(rows),
        },
        "sha256": bars_sha,
        "rows": len(rows),
        "start_ts": rows[0][0],
        "end_ts": rows[-1][0],
        "columns": list(PRODUCTION_ARTIFACT_COLUMNS),
        "instrument": {
            "asset_class": "fx",
            "digits": 5,
            "parity_tick_tolerance": 0.00001,
            "path": f"Forex\\Main\\{SYMBOL}",
            "point": 0.00001,
            "status": "available",
            "symbol": SYMBOL,
            "trade_contract_size": 100_000.0,
            "trade_tick_size": 0.00001,
            "trade_tick_value": 1.0,
        },
        "source_environment": _source_environment(login=provenance_login),
        "request": {
            "cell_timeframe": timeframe,
            "timeframe_request_map_sha256": request_map_sha,
            "source_request_interval": {
                "start_utc": request_start,
                "end_exclusive_utc": request_end,
            },
        },
    }
    provenance_path = leaf / "provenance.json"
    provenance_sha = _write_json(provenance_path, provenance)
    sidecar = {
        "schema_version": "fxpm-native-bars-v1",
        "activation_id": ACTIVATION_ID,
        "source_provenance_relative_path": (
            f"runs/run-prefix/{SYMBOL}/{timeframe}/provenance.json"
        ),
        "source_provenance_sha256": provenance_sha,
    }
    sidecar_path = (
        native / "audits" / ACTIVATION_ID / SYMBOL / timeframe / "sidecar.json"
    )
    sidecar_sha = _write_json(sidecar_path, sidecar)
    run_manifest = {
        "schema_version": "fxpm.native_history_run_manifest.v1",
        "immutable_record": True,
        "complete": True,
        "run_id": "run-prefix",
        "request": {"timeframe_request_map_sha256": request_map_sha},
        "source_environment": _source_environment(login=run_login),
        "results": [
            {
                "symbol": SYMBOL,
                "timeframe": timeframe,
                "status": "success",
                "failure_status": None,
                "rows": len(rows),
                "sha256": bars_sha,
            }
        ],
    }
    run_manifest_path = native / "runs" / "run-prefix" / "run_manifest.json"
    run_manifest_sha = _write_json(run_manifest_path, run_manifest)
    entry = {
        "status": "active",
        "symbol": SYMBOL,
        "timeframe": timeframe,
        "bars_relative_path": f"runs/run-prefix/{SYMBOL}/{timeframe}/bars.csv",
        "bars_sha256": bars_sha,
        "sidecar_relative_path": (
            f"audits/{ACTIVATION_ID}/{SYMBOL}/{timeframe}/sidecar.json"
        ),
        "sidecar_sha256": sidecar_sha,
    }
    activation = {
        "schema_version": "fxpm-native-activation-v1",
        "status": "active",
        "activation_id": ACTIVATION_ID,
        "production_authoritative": False,
        "run_manifest_relative_path": "runs/run-prefix/run_manifest.json",
        "run_manifest_sha256": run_manifest_sha,
        "entries": {SYMBOL: {timeframe: entry}},
    }
    manifest_relative = f"activations/{ACTIVATION_ID}.json"
    manifest_path = native / manifest_relative
    activation_sha = _write_json(manifest_path, activation)
    pointer = {
        "schema_version": "fxpm-native-active-pointer-v1",
        "activation_id": ACTIVATION_ID,
        "manifest_relative_path": manifest_relative,
        "manifest_sha256": activation_sha,
        "production_authoritative": False,
    }
    _write_json(native / "ACTIVE.json", pointer)
    return {
        "cutoff": _iso(start + timedelta(seconds=3 * cadence)),
        "later_cutoff": _iso(start + timedelta(seconds=4 * cadence)),
        "activation_sha": activation_sha,
        "bars_path": str(bars),
    }


@lru_cache(maxsize=1)
def _authenticated_suffix_base(timeframe: str) -> dict[str, object]:
    return build_valid_v5_contract_fixture(
        timeframe=timeframe,
        requested_information_cutoff_utc=datetime(2026, 7, 13, 9, 3, 17, tzinfo=UTC),
    )


def _authenticated_suffix_fixture(timeframe: str = "D1") -> dict[str, object]:
    """Build genuine terminal-page + immediately available-page V5 evidence."""

    fixture = copy.deepcopy(_authenticated_suffix_base(timeframe))
    provenance = fixture["provenance"]
    pagination = provenance["pagination"]
    strategy_start = datetime.fromisoformat(
        fixture["common_request"]["timeframe_strategy_evidence_intervals"][timeframe][
            "start_utc"
        ].replace("Z", "+00:00")
    )
    prefix_index = next(
        index
        for index, page in enumerate(pagination["page_attempts"][:-1])
        if datetime.fromisoformat(page["start_utc"].replace("Z", "+00:00"))
        >= strategy_start
    )
    attempts = copy.deepcopy(pagination["page_attempts"][prefix_index:])
    observations = list(
        fixture["artifact_observation"].page_observations[prefix_index:]
    )
    terminal_page = attempts[0]
    first_data_page = attempts[1]
    terminal_start = datetime.fromisoformat(
        terminal_page["start_utc"].replace("Z", "+00:00")
    )
    terminal_end = datetime.fromisoformat(
        terminal_page["end_exclusive_utc"].replace("Z", "+00:00")
    )
    boundary_open = datetime.fromisoformat(observations[1][3].replace("Z", "+00:00"))
    boundary_epoch = int(boundary_open.timestamp())
    raw_extent = {
        "rows": 1,
        "first_epoch_second": boundary_epoch,
        "last_epoch_second": boundary_epoch,
    }
    diagnostics = request_contract._fixture_mt5_v4_empty_diagnostics(
        symbol=SYMBOL,
        timeframe=timeframe,
        start=terminal_start,
        end=terminal_end,
    )
    diagnostics.update(
        {
            "sync_status": "terminal_prefix_unsynchronized",
            "initial_raw_relation": "RIGHT_DISJOINT_PREFIX",
            "confirmation_raw_relation": "RIGHT_DISJOINT_PREFIX",
            "raw_relation": "RIGHT_DISJOINT_PREFIX",
            "raw_count": 1,
            "raw_extent": copy.deepcopy(raw_extent),
            "history_download_trigger_nonempty": 1,
            "terminal_confirmation_consumed": True,
        }
    )
    for entry in diagnostics["call_ledger"]:
        entry.update(
            {
                "raw_relation": "RIGHT_DISJOINT_PREFIX",
                "raw_count": 1,
                "raw_extent": copy.deepcopy(raw_extent),
            }
        )
    terminal_page["copy_rates_range_attempts"] = 2
    terminal_page["connector_diagnostics"] = diagnostics
    observations[0] = (
        observations[0][0],
        observations[0][1],
        0,
        None,
        None,
    )
    source_start = fixture["common_request"]["timeframe_source_request_intervals"][
        timeframe
    ]["start_utc"]
    pagination.update(
        {
            "pages_requested": len(attempts),
            "pages_with_data": len(attempts) - 1,
            "page_attempts": attempts,
            "empty_pages": [],
            "failed_pages": [],
            "truncated_pages": [],
            "terminal_prefix_unresolved_pages": [copy.deepcopy(terminal_page)],
            "stop_reason": "terminal_prefix_unresolved",
            "unrequested_prefix": (
                None
                if terminal_page["start_utc"] == source_start
                else {
                    "start_utc": source_start,
                    "end_exclusive_utc": terminal_page["start_utc"],
                    "reason": "terminal_prefix_unresolved",
                }
            ),
            "attempts_total": len(attempts),
            "copy_rates_range_attempts_total": len(attempts) + 1,
            "available_history_boundary": {
                "schema_version": (
                    request_contract.AVAILABLE_HISTORY_BOUNDARY_SCHEMA_VERSION
                ),
                "source_state": "broker_native_available_suffix",
                "boundary_kind": "first_authenticated_available_bar_open",
                "boundary_open_utc": _iso(boundary_open),
                "terminal_page_start_utc": terminal_page["start_utc"],
                "terminal_page_end_exclusive_utc": terminal_page["end_exclusive_utc"],
                "first_data_page_start_utc": first_data_page["start_utc"],
                "first_data_page_end_exclusive_utc": first_data_page[
                    "end_exclusive_utc"
                ],
                "confirmation_method": (
                    request_contract.AVAILABLE_HISTORY_BOUNDARY_CONFIRMATION_METHOD
                ),
                "market_listing_date_asserted": False,
                "statistical_sufficiency_assessed": False,
            },
        }
    )
    return {
        "fixture": fixture,
        "observations": observations,
        "boundary_open": boundary_open,
    }


def _write_authenticated_suffix_activation(
    data_dir: Path,
    timeframe: str = "D1",
) -> dict[str, object]:
    evidence = _authenticated_suffix_fixture(timeframe)
    fixture = evidence["fixture"]
    observations = evidence["observations"]
    boundary_open = evidence["boundary_open"]
    cadence = TIMEFRAME_SECONDS[timeframe]
    native = data_dir / "native"
    leaf = native / "runs" / "run-listed-late" / SYMBOL / timeframe
    leaf.mkdir(parents=True)
    bars = leaf / "bars.csv"
    opens: list[datetime] = []
    for observation in observations[1:]:
        first = datetime.fromisoformat(observation[3].replace("Z", "+00:00"))
        last = datetime.fromisoformat(observation[4].replace("Z", "+00:00"))
        page_rows = int(observation[2])
        page_opens = [
            first + timedelta(seconds=ordinal * cadence) for ordinal in range(page_rows)
        ]
        page_opens[-1] = last
        opens.extend(page_opens)
    final_successor = opens[-1] + timedelta(seconds=cadence)
    with bars.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle, lineterminator="\n")
        writer.writerow(("time", *PRODUCTION_ARTIFACT_COLUMNS))
        for index, opened in enumerate(opens):
            successor = opens[index + 1] if index + 1 < len(opens) else final_successor
            delta_seconds = (successor - opened).total_seconds()
            closed = (
                successor
                if cadence / 2 <= delta_seconds <= cadence * 3 / 2
                else opened + timedelta(seconds=cadence)
            )
            writer.writerow(
                (
                    _iso(opened),
                    "1.100000",
                    "1.102000",
                    "1.098000",
                    "1.101000",
                    "100",
                    "12",
                    "80",
                    _iso(opened),
                    (
                        (closed - timedelta(seconds=1)).strftime("%Y-%m-%dT%H:%M:%S")
                        + ".999999999Z"
                    ),
                    _iso(closed),
                )
            )
    rows = len(opens)
    bars_raw = bars.read_bytes()
    bars_sha = _sha(bars_raw)

    provenance = copy.deepcopy(fixture["provenance"])
    provenance.update(
        {
            "run_id": "run-listed-late",
            "status": "partial",
            "failure_status": "boundary_history_shortfall",
            "eligible_for_optimization": False,
            "eligible_for_downstream_validation": False,
            "complete": False,
            "artifact": {
                "relative_path": "bars.csv",
                "sha256": bars_sha,
                "bytes": len(bars_raw),
                "rows": rows,
            },
            "sha256": bars_sha,
            "rows": rows,
            "start_ts": _iso(boundary_open),
            "end_ts": observations[-1][4],
            "instrument": {
                "asset_class": "fx",
                "digits": 5,
                "parity_tick_tolerance": 0.00001,
                "path": f"Forex\\Main\\{SYMBOL}",
                "point": 0.00001,
                "status": "available",
                "symbol": SYMBOL,
                "trade_contract_size": 100_000.0,
                "trade_tick_size": 0.00001,
                "trade_tick_value": 1.0,
            },
        }
    )
    provenance_path = leaf / "provenance.json"
    provenance_sha = _write_json(provenance_path, provenance)
    sidecar = {
        "schema_version": "fxpm-native-bars-v1",
        "activation_id": ACTIVATION_ID,
        "source_provenance_relative_path": (
            f"runs/run-listed-late/{SYMBOL}/{timeframe}/provenance.json"
        ),
        "source_provenance_sha256": provenance_sha,
    }
    sidecar_path = (
        native / "audits" / ACTIVATION_ID / SYMBOL / timeframe / "sidecar.json"
    )
    sidecar_sha = _write_json(sidecar_path, sidecar)

    run_manifest = copy.deepcopy(fixture["manifest"])
    run_manifest["run_id"] = "run-listed-late"
    run_cell = next(
        item
        for item in run_manifest["results"]
        if item["symbol"] == SYMBOL and item["timeframe"] == timeframe
    )
    run_cell.update(
        {
            "status": "partial",
            "failure_status": "boundary_history_shortfall",
            "rows": rows,
            "sha256": bars_sha,
            "eligible_for_optimization": False,
            "eligible_for_downstream_validation": False,
        }
    )
    run_manifest_path = native / "runs" / "run-listed-late" / "run_manifest.json"
    run_manifest_sha = _write_json(run_manifest_path, run_manifest)
    entry = {
        "status": "active",
        "symbol": SYMBOL,
        "timeframe": timeframe,
        "bars_relative_path": f"runs/run-listed-late/{SYMBOL}/{timeframe}/bars.csv",
        "bars_sha256": bars_sha,
        "sidecar_relative_path": (
            f"audits/{ACTIVATION_ID}/{SYMBOL}/{timeframe}/sidecar.json"
        ),
        "sidecar_sha256": sidecar_sha,
    }
    activation = {
        "schema_version": "fxpm-native-activation-v1",
        "status": "active",
        "activation_id": ACTIVATION_ID,
        "production_authoritative": False,
        "run_manifest_relative_path": "runs/run-listed-late/run_manifest.json",
        "run_manifest_sha256": run_manifest_sha,
        "entries": {SYMBOL: {timeframe: entry}},
    }
    manifest_relative = f"activations/{ACTIVATION_ID}.json"
    activation_path = native / manifest_relative
    activation_sha = _write_json(activation_path, activation)
    pointer = {
        "schema_version": "fxpm-native-active-pointer-v1",
        "activation_id": ACTIVATION_ID,
        "manifest_relative_path": manifest_relative,
        "manifest_sha256": activation_sha,
        "production_authoritative": False,
    }
    pointer_path = native / "ACTIVE.json"
    _write_json(pointer_path, pointer)
    return {
        "activation_path": activation_path,
        "activation_sha": activation_sha,
        "bars_path": bars,
        "boundary_open": boundary_open,
        "first_close": boundary_open + timedelta(seconds=cadence),
        "pointer_path": pointer_path,
        "provenance_path": provenance_path,
        "sidecar_path": sidecar_path,
        "timeframe": timeframe,
    }


def _reseal_listed_late_provenance(
    facts: dict[str, object],
    mutate,
) -> str:
    provenance_path = facts["provenance_path"]
    provenance = json.loads(provenance_path.read_text(encoding="utf-8"))
    mutate(provenance)
    provenance_sha = _write_json(provenance_path, provenance)
    sidecar_path = facts["sidecar_path"]
    sidecar = json.loads(sidecar_path.read_text(encoding="utf-8"))
    sidecar["source_provenance_sha256"] = provenance_sha
    sidecar_sha = _write_json(sidecar_path, sidecar)
    activation_path = facts["activation_path"]
    activation = json.loads(activation_path.read_text(encoding="utf-8"))
    activation["entries"][SYMBOL][facts["timeframe"]]["sidecar_sha256"] = sidecar_sha
    activation_sha = _write_json(activation_path, activation)
    pointer_path = facts["pointer_path"]
    pointer = json.loads(pointer_path.read_text(encoding="utf-8"))
    pointer["manifest_sha256"] = activation_sha
    _write_json(pointer_path, pointer)
    facts["activation_sha"] = activation_sha
    return activation_sha


def _context(tmp_path: Path) -> spool.GenerationSpoolContext:
    return spool.freeze_generation_context(
        (tmp_path / "spool").resolve(),
        generation_id="native-prefix-generation",
        native_universe_sha256="a" * 64,
        config_sha256="b" * 64,
    )


def _object_path(context: spool.GenerationSpoolContext, descriptor: dict) -> Path:
    return Path(context.root).joinpath(*Path(descriptor["relative_path"]).parts)


def _read_index(
    context: spool.GenerationSpoolContext, product: prefix.NativePrefixProduct
):
    return json.loads(_object_path(context, dict(product.index_descriptor)).read_text())


def _produce(tmp_path: Path, timeframe: str = "M5"):
    data_dir = tmp_path / "data"
    facts = _write_activation(data_dir, timeframe)
    context = _context(tmp_path)
    product = prefix.produce_activated_native_prefix(
        data_dir=data_dir,
        context=context,
        symbol=SYMBOL,
        timeframe=timeframe,
        cutoff_utc=facts["cutoff"],
        source_accessed_at_utc=SOURCE_ACCESSED,
        production_contract_required=False,
    )
    return data_dir, facts, context, product


@pytest.mark.parametrize("timeframe", PRODUCTION_TIMEFRAME_ORDER)
def test_all_six_native_timeframes_produce_exact_horizon_prefix(tmp_path, timeframe):
    data_dir, facts, context, product = _produce(tmp_path, timeframe)

    assert product.source_state == prefix.AVAILABLE
    assert product.prefix_path is not None and product.prefix_path.is_file()
    assert product.native_prefix["prefix_rows"] == 3
    assert product.instrument_snapshot == {
        "asset_class": "fx",
        "digits": 5,
        "parity_tick_tolerance": 0.00001,
        "path": f"Forex\\Main\\{SYMBOL}",
        "point": 0.00001,
        "status": "available",
        "symbol": SYMBOL,
        "trade_contract_size": 100_000.0,
        "trade_tick_size": 0.00001,
        "trade_tick_value": 1.0,
    }
    assert product.native_prefix["prefix_last_close_utc"] == facts["cutoff"]
    assert (
        product.native_prefix["prefix_content_sha256"]
        == product.prefix_descriptor["sha256"]
    )
    assert product.native_prefix["prefix_index_sha256"] == product.index_sha256
    assert len(product.prefix_path.read_text(encoding="utf-8").splitlines()) == 4
    frame = prefix.open_verified_prefix_frame(product)
    assert len(frame) == 3
    assert frame.index.tz is not None
    assert float(frame.iloc[-1]["Close"]) == pytest.approx(1.103)
    assert frame.attrs[pm_core.NATIVE_SOURCE_BINDING_ATTR] == {
        "symbol": SYMBOL,
        "timeframe": timeframe,
        "activation_id": ACTIVATION_ID,
        "activation_manifest_sha256": facts["activation_sha"],
        "bars_sha256": product.index["source"]["bars_sha256"],
        "sidecar_sha256": product.index["source"]["audit_sidecar_sha256"],
    }
    normalized = _normalize_native_prefix(
        dict(product.native_prefix),
        {"symbol": SYMBOL, "timeframe": timeframe},
        {
            "selection_cutoff_utc": facts["cutoff"],
            "evidence_accessed_at_utc": SOURCE_ACCESSED,
        },
    )
    assert normalized == product.native_prefix
    reopened = prefix.verify_activated_native_prefix(
        data_dir=data_dir,
        context=context,
        index_descriptor=product.index_descriptor,
        expected_index_sha256=product.index_sha256,
        expected_symbol=SYMBOL,
        expected_timeframe=timeframe,
        expected_cutoff_utc=facts["cutoff"],
        expected_activation_manifest_sha256=facts["activation_sha"],
        production_contract_required=False,
    )
    assert reopened.native_prefix == product.native_prefix


def test_open_frame_requires_sealed_product_and_rederives_source_binding(tmp_path):
    _data_dir, _facts, _context_value, product = _produce(tmp_path)
    frame = prefix.open_verified_prefix_frame(product)
    frame.attrs[pm_core.NATIVE_SOURCE_BINDING_ATTR]["bars_sha256"] = "0" * 64

    reopened = prefix.open_verified_prefix_frame(product)
    assert (
        reopened.attrs[pm_core.NATIVE_SOURCE_BINDING_ATTR]["bars_sha256"]
        == (product.index["source"]["bars_sha256"])
    )
    forged = replace(product, _seal=object())
    with pytest.raises(prefix.NativePrefixError, match="PRODUCT_NOT_VERIFIER_SEALED"):
        prefix.open_verified_prefix_frame(forged)


def test_open_frame_rejects_content_mutation_after_product_verification(tmp_path):
    _data_dir, _facts, _context_value, product = _produce(tmp_path)
    assert product.prefix_path is not None
    product.prefix_path.write_bytes(product.prefix_path.read_bytes() + b"\n")

    with pytest.raises(prefix.NativePrefixError, match="CONTENT_MISMATCH"):
        prefix.open_verified_prefix_frame(product)


def test_open_frame_binding_drives_backtester_terms_overlay(tmp_path, monkeypatch):
    data_dir, _facts, _context_value, product = _produce(tmp_path)
    frame = prefix.open_verified_prefix_frame(product)
    binding = frame.attrs[pm_core.NATIVE_SOURCE_BINDING_ATTR]
    pointer = data_dir / "native" / "terms" / "ACTIVE.json"
    pointer.parent.mkdir(parents=True, exist_ok=True)
    pointer.write_text("{}", encoding="ascii")
    resolved = {
        "price_activation_id": binding["activation_id"],
        "price_activation_manifest_sha256": binding["activation_manifest_sha256"],
        "target_bars_sha256": binding["bars_sha256"],
        "target_sidecar_sha256": binding["sidecar_sha256"],
        "events": [],
    }
    projection = object()
    monkeypatch.setattr(pm_core, "broker_terms_sha256_file", lambda _path: "9" * 64)
    monkeypatch.setattr(
        pm_core, "resolve_active_swap_contract", lambda *_args: resolved
    )
    monkeypatch.setattr(
        pm_core, "project_session_events", lambda _events, _frame: projection
    )
    backtester = pm_core.Backtester(
        pm_core.PipelineConfig(data_dir=data_dir, research_mode=True)
    )
    spec = pm_core.InstrumentSpec(symbol=SYMBOL, pip_position=4)

    assert backtester._resolve_broker_swap_projection(frame, spec, "M5") == (
        projection,
        resolved,
    )
    hostile = frame.copy(deep=True)
    hostile.attrs[pm_core.NATIVE_SOURCE_BINDING_ATTR] = {
        **binding,
        "bars_sha256": "0" * 64,
    }
    with pytest.raises(pm_core.BrokerTermsError, match="does not match"):
        backtester._resolve_broker_swap_projection(hostile, spec, "M5")


def _execution_config(tmp_path: Path, *, tick_size: float = 0.00001) -> Path:
    broker = {
        SYMBOL: {
            "symbol": SYMBOL,
            "broker_symbol": SYMBOL,
            "asset_class": "fx",
            "path": f"Forex\\Main\\{SYMBOL}",
            "digits": 5,
            "point": 0.00001,
            "tick_size": tick_size,
            "tick_value": 1.25,
            "contract_size": 100_000.0,
            "volume_min": 0.01,
            "volume_max": 500.0,
            "volume_step": 0.01,
            "spread": 25,
            "swap_long": -3.0,
            "swap_short": 1.0,
            "swap_mode": 1,
            "swap_rollover3days": 3,
            "stops_level": 0,
            "pip_size": 0.0001,
        }
    }
    _write_json(tmp_path / "broker_specs.json", broker)
    config = {
        "_meta": {"scope": "funded_live_v1"},
        "broker_specs_path": "broker_specs.json",
        "instrument_spec_defaults": {"commission_per_lot": 7.0},
        "instrument_specs": {SYMBOL: {"commission_per_lot": 7.0}},
    }
    path = tmp_path / "config.json"
    _write_json(path, config)
    return path


def test_execution_spec_is_derived_from_frozen_native_config_and_broker_bytes(
    tmp_path,
):
    _data_dir, _facts, _context_value, product = _produce(tmp_path / "native")
    config_path = _execution_config(tmp_path)
    config = pm_core.PipelineConfig(research_mode=True)
    config._deployment_config_path = str(config_path)

    spec, receipt = execution._freeze_bound_execution_spec(
        product=product,
        config=config,
    )

    assert spec.symbol == SYMBOL
    assert spec.tick_value == pytest.approx(1.0)
    assert spec.commission_per_lot == pytest.approx(7.0)
    assert spec.max_lot == pytest.approx(500.0)
    assert receipt["native_prefix_index_sha256"] == product.index_sha256
    assert receipt[
        "instrument_spec_sha256"
    ] == pm_core.Backtester._cost_attestation_json_sha256(
        {
            "schema_version": "fxpm-backtester-instrument-spec-v1",
            "instrument_spec": receipt["instrument_spec"],
        }
    )
    assert receipt["receipt_sha256"] == execution._sha256(
        {key: value for key, value in receipt.items() if key != "receipt_sha256"}
    )


def test_execution_spec_rejects_caller_override_and_structural_broker_splice(
    tmp_path,
):
    _data_dir, _facts, _context_value, product = _produce(tmp_path / "native")
    config = pm_core.PipelineConfig(research_mode=True)
    with pytest.raises(
        execution.AuthoritativeSearchExecutionError,
        match="CALLER_INSTRUMENT_SPEC_FORBIDDEN",
    ):
        execution.fit_native_timeframe_for_search(
            product=product,
            config=config,
            instrument_spec=pm_core.InstrumentSpec(
                symbol=SYMBOL,
                pip_position=4,
                tick_value=99.0,
                commission_per_lot=0.0,
            ),
        )

    config._deployment_config_path = str(_execution_config(tmp_path, tick_size=0.00002))
    with pytest.raises(
        execution.AuthoritativeSearchExecutionError,
        match="NATIVE_BROKER_STRUCTURAL_MISMATCH",
    ):
        execution._freeze_bound_execution_spec(product=product, config=config)


def _replace_content(
    context: spool.GenerationSpoolContext,
    product: prefix.NativePrefixProduct,
    raw: bytes,
):
    descriptor = spool.write_opaque_artifact(context, (raw,), format="csv")
    index = _read_index(context, product)
    index["prefix"]["content"] = descriptor
    index_descriptor = spool.write_canonical_json_artifact(context, index)
    return index_descriptor


def test_cutoff_leak_is_rejected_even_when_rehashed_and_content_addressed(tmp_path):
    data_dir, facts, context, product = _produce(tmp_path)
    source_lines = Path(facts["bars_path"]).read_bytes().splitlines(keepends=True)
    leaked = b"".join(source_lines[:5])  # header plus four rows; cutoff admits three
    hostile_index = _replace_content(context, product, leaked)

    with pytest.raises(
        prefix.NativePrefixError, match="EXACT_SOURCE_PREFIX|EXTRA_BYTES"
    ):
        prefix.verify_activated_native_prefix(
            data_dir=data_dir,
            context=context,
            index_descriptor=hostile_index,
            expected_index_sha256=hostile_index["sha256"],
            expected_symbol=SYMBOL,
            expected_timeframe="M5",
            expected_cutoff_utc=facts["cutoff"],
            expected_activation_manifest_sha256=facts["activation_sha"],
            production_contract_required=False,
        )


def test_row_reorder_is_rejected_even_when_rehashed(tmp_path):
    data_dir, facts, context, product = _produce(tmp_path)
    lines = product.prefix_path.read_bytes().splitlines(keepends=True)
    reordered = b"".join((lines[0], lines[2], lines[1], lines[3]))
    hostile_index = _replace_content(context, product, reordered)

    with pytest.raises(prefix.NativePrefixError, match="EXACT_SOURCE_PREFIX"):
        prefix.verify_activated_native_prefix(
            data_dir=data_dir,
            context=context,
            index_descriptor=hostile_index,
            expected_index_sha256=hostile_index["sha256"],
            expected_symbol=SYMBOL,
            expected_timeframe="M5",
            expected_cutoff_utc=facts["cutoff"],
            expected_activation_manifest_sha256=facts["activation_sha"],
            production_contract_required=False,
        )


def test_descriptor_splice_from_later_cutoff_is_rejected(tmp_path):
    data_dir, facts, context, product = _produce(tmp_path)
    later = prefix.produce_activated_native_prefix(
        data_dir=data_dir,
        context=context,
        symbol=SYMBOL,
        timeframe="M5",
        cutoff_utc=facts["later_cutoff"],
        source_accessed_at_utc=SOURCE_ACCESSED,
        production_contract_required=False,
    )
    index = _read_index(context, product)
    index["prefix"]["content"] = dict(later.prefix_descriptor)
    hostile_index = spool.write_canonical_json_artifact(context, index)

    with pytest.raises(prefix.NativePrefixError, match="EXTRA_BYTES"):
        prefix.verify_activated_native_prefix(
            data_dir=data_dir,
            context=context,
            index_descriptor=hostile_index,
            expected_index_sha256=hostile_index["sha256"],
            expected_symbol=SYMBOL,
            expected_timeframe="M5",
            expected_cutoff_utc=facts["cutoff"],
            expected_activation_manifest_sha256=facts["activation_sha"],
            production_contract_required=False,
        )


def test_mixed_run_and_leaf_account_provenance_fails_closed(tmp_path):
    data_dir = tmp_path / "data"
    facts = _write_activation(data_dir, "M5", provenance_login=42, run_login=84)

    with pytest.raises(prefix.NativePrefixError, match="MIXED_ACCOUNT_PROVENANCE"):
        prefix.produce_activated_native_prefix(
            data_dir=data_dir,
            context=_context(tmp_path),
            symbol=SYMBOL,
            timeframe="M5",
            cutoff_utc=facts["cutoff"],
            source_accessed_at_utc=SOURCE_ACCESSED,
            production_contract_required=False,
        )


def test_missing_activation_is_typed_technical_not_no_trade(tmp_path):
    product = prefix.produce_activated_native_prefix(
        data_dir=tmp_path / "missing-data",
        context=_context(tmp_path),
        symbol=SYMBOL,
        timeframe="M5",
        cutoff_utc="2026-01-01T00:05:00Z",
        source_accessed_at_utc=SOURCE_ACCESSED,
        production_contract_required=False,
    )

    assert product.source_state == prefix.TECHNICAL_FAILURE
    assert product.native_prefix["source_state"] == "TECHNICAL_FAILURE"
    assert product.native_prefix["artifact_sha256"] is None
    assert product.native_prefix["source_reason"].startswith("TECHNICAL_FAILURE:")
    assert "NO_TRADE" not in product.native_prefix["source_reason"]


def test_authenticated_source_unavailable_is_distinct_and_sparse_sibling_survives(
    tmp_path, monkeypatch
):
    data_dir, facts, context, available = _produce(tmp_path, "M5")
    original = prefix.resolve_active_native_artifact

    def sparse_resolution(data_dir_arg, symbol, timeframe, **kwargs):
        if timeframe == "H1":
            return ActiveNativeResolution(
                symbol=symbol,
                timeframe=timeframe,
                errors=["NATIVE_ACTIVE_CELL_SOURCE_UNAVAILABLE"],
            )
        return original(data_dir_arg, symbol, timeframe, **kwargs)

    monkeypatch.setattr(prefix, "resolve_active_native_artifact", sparse_resolution)
    unavailable = prefix.produce_activated_native_prefix(
        data_dir=data_dir,
        context=context,
        symbol=SYMBOL,
        timeframe="H1",
        cutoff_utc="2026-01-01T01:00:00Z",
        source_accessed_at_utc=SOURCE_ACCESSED,
        production_contract_required=False,
    )

    assert unavailable.source_state == prefix.SOURCE_UNAVAILABLE
    assert unavailable.native_prefix["source_state"] == "SOURCE_UNAVAILABLE"
    assert unavailable.native_prefix["artifact_sha256"] is None
    assert available.source_state == prefix.AVAILABLE
    assert available.native_prefix["prefix_rows"] == 3


def test_index_descriptor_splice_without_expected_hash_is_impossible(tmp_path):
    data_dir, facts, context, product = _produce(tmp_path)
    forged = copy.deepcopy(dict(product.index_descriptor))
    forged["sha256"] = "0" * 64

    with pytest.raises(prefix.NativePrefixError, match="CONTENT_ADDRESS|INTEGRITY"):
        prefix.verify_activated_native_prefix(
            data_dir=data_dir,
            context=context,
            index_descriptor=forged,
            expected_index_sha256=product.index_sha256,
            expected_symbol=SYMBOL,
            expected_timeframe="M5",
            expected_cutoff_utc=facts["cutoff"],
            expected_activation_manifest_sha256=facts["activation_sha"],
            production_contract_required=False,
        )


def test_authenticated_available_suffix_is_unavailable_only_before_first_close(
    tmp_path,
    monkeypatch,
):
    data_dir = tmp_path / "data"
    facts = _write_authenticated_suffix_activation(data_dir)
    context = _context(tmp_path)
    boundary_open = facts["boundary_open"]
    first_close = facts["first_close"]

    terminal = prefix.produce_activated_native_prefix(
        data_dir=data_dir,
        context=context,
        symbol=SYMBOL,
        timeframe="D1",
        cutoff_utc=_iso(boundary_open),
        source_accessed_at_utc="2026-07-14T00:00:00Z",
        production_contract_required=False,
    )

    assert terminal.source_state == prefix.SOURCE_UNAVAILABLE
    assert terminal.index["source_reason"] == prefix.SOURCE_UNAVAILABLE_AT_CUTOFF
    assert (
        terminal.native_prefix["source_reason"] == prefix.SOURCE_UNAVAILABLE_AT_CUTOFF
    )
    assert terminal.native_prefix["artifact_sha256"] is None
    assert terminal.native_prefix["prefix_content_sha256"] is None
    assert terminal.native_prefix["prefix_rows"] is None
    assert terminal.index["source"]["bars_sha256"] == _sha(
        facts["bars_path"].read_bytes()
    )
    terminal_receipt = json.loads(
        _object_path(context, terminal.index["terminal_receipt"]).read_text(
            encoding="ascii"
        )
    )
    assert terminal_receipt["schema_version"] == (
        prefix.PREFIX_CUTOFF_TERMINAL_RECEIPT_SCHEMA
    )
    assert terminal_receipt["cutoff_relation"] == (
        "STRICTLY_BEFORE_FIRST_AUTHENTICATED_BAR_CLOSE"
    )
    assert terminal_receipt["market_listing_date_asserted"] is False
    assert terminal_receipt["statistical_sufficiency_assessed"] is False
    assert terminal_receipt["grants_no_trade_authority"] is False
    assert "NO_TRADE" not in json.dumps(terminal.index, sort_keys=True)
    normalized = _normalize_native_prefix(
        dict(terminal.native_prefix),
        {"symbol": SYMBOL, "timeframe": "D1"},
        {
            "selection_cutoff_utc": _iso(boundary_open),
            "evidence_accessed_at_utc": "2026-07-14T00:00:00Z",
        },
    )
    assert normalized == terminal.native_prefix

    elsewhere = tmp_path / "not-the-native-root"
    elsewhere.mkdir()
    monkeypatch.chdir(elsewhere)
    reopened = prefix.reverify_activated_native_prefix_product(terminal, context)
    assert reopened.index_sha256 == terminal.index_sha256
    assert reopened.source_state == prefix.SOURCE_UNAVAILABLE
    with pytest.raises(
        prefix.NativePrefixError,
        match="PREFIX_PRODUCT_NATIVE_ROOT_UNBOUND",
    ):
        prefix.reverify_activated_native_prefix_product(
            replace(terminal, _data_dir=None),
            context,
        )
    with pytest.raises(
        prefix.NativePrefixError,
        match="PREFIX_PRODUCT_NATIVE_ROOT_INVALID",
    ):
        prefix.reverify_activated_native_prefix_product(
            replace(terminal, _data_dir=Path("data")),
            context,
        )

    available = prefix.produce_activated_native_prefix(
        data_dir=data_dir,
        context=context,
        symbol=SYMBOL,
        timeframe="D1",
        cutoff_utc=_iso(first_close),
        source_accessed_at_utc="2026-07-14T00:00:00Z",
        production_contract_required=False,
    )
    assert available.source_state == prefix.AVAILABLE
    assert available.native_prefix["prefix_rows"] == 1
    assert available.native_prefix["prefix_first_open_utc"] == _iso(boundary_open)
    assert available.native_prefix["prefix_last_close_utc"] == _iso(first_close)
    assert available.native_prefix["boundary_status"] == "LEFT_TRUNCATED"


def test_missing_pagination_shortfall_only_requires_proof_inside_strategy_interval():
    strategy_start = datetime(2020, 1, 1, tzinfo=UTC)
    source = SimpleNamespace(
        provenance={
            "failure_status": "boundary_history_shortfall",
            "request": {
                "strategy_evidence_interval": {
                    "start_utc": _iso(strategy_start),
                }
            },
        },
        run_cell={"failure_status": "boundary_history_shortfall"},
    )
    audit_only_first = prefix._FirstSourceBar(
        opened=strategy_start - timedelta(days=1),
        closed=strategy_start,
    )
    strategy_gap_first = prefix._FirstSourceBar(
        opened=strategy_start + timedelta(days=1),
        closed=strategy_start + timedelta(days=2),
    )

    assert not prefix._available_history_boundary_required(
        source,
        expected_first=audit_only_first,
    )
    assert prefix._available_history_boundary_required(
        source,
        expected_first=strategy_gap_first,
    )


@pytest.mark.parametrize(
    "attack",
    ["missing_pagination", "missing", "forged", "forged_request_sequence"],
)
@pytest.mark.parametrize("cutoff_relation", ["before_close", "at_close", "later"])
def test_listed_late_prefix_rejects_missing_or_forged_boundary_proof(
    tmp_path,
    attack,
    cutoff_relation,
):
    data_dir = tmp_path / "data"
    facts = _write_authenticated_suffix_activation(data_dir)

    def mutate(provenance):
        if attack == "missing_pagination":
            provenance.pop("pagination")
            return
        proof = provenance["pagination"]["available_history_boundary"]
        if attack == "missing":
            provenance["pagination"].pop("available_history_boundary")
        elif attack == "forged":
            proof["boundary_open_utc"] = _iso(
                facts["boundary_open"] + timedelta(days=1)
            )
        else:
            pagination = provenance["pagination"]
            pagination["page_attempts"][0]["request_sequence"] = 99
            pagination["terminal_prefix_unresolved_pages"][0]["request_sequence"] = 99

    _reseal_listed_late_provenance(facts, mutate)
    cutoff = {
        "before_close": facts["boundary_open"],
        "at_close": facts["first_close"],
        "later": facts["first_close"] + timedelta(days=1),
    }[cutoff_relation]
    with pytest.raises(
        prefix.NativePrefixError,
        match="PREFIX_AVAILABLE_HISTORY_BOUNDARY",
    ):
        prefix.produce_activated_native_prefix(
            data_dir=data_dir,
            context=_context(tmp_path),
            symbol=SYMBOL,
            timeframe="D1",
            cutoff_utc=_iso(cutoff),
            source_accessed_at_utc="2026-07-14T00:00:00Z",
            production_contract_required=False,
        )


def test_cutoff_terminal_cannot_launder_corrupt_activated_source(tmp_path):
    data_dir = tmp_path / "data"
    facts = _write_authenticated_suffix_activation(data_dir)
    bars_path = facts["bars_path"]
    raw = bars_path.read_bytes()
    bars_path.write_bytes(raw.replace(b"1.100000", b"0.000000", 1))

    product = prefix.produce_activated_native_prefix(
        data_dir=data_dir,
        context=_context(tmp_path),
        symbol=SYMBOL,
        timeframe="D1",
        cutoff_utc=_iso(facts["boundary_open"]),
        source_accessed_at_utc="2026-07-14T00:00:00Z",
        production_contract_required=False,
    )
    assert product.source_state == prefix.TECHNICAL_FAILURE
    assert product.native_prefix["source_state"] == prefix.TECHNICAL_FAILURE
    assert product.native_prefix["artifact_sha256"] is None
    assert product.native_prefix["source_reason"].startswith("TECHNICAL_FAILURE:")
    assert "NO_TRADE" not in json.dumps(product.index, sort_keys=True)
