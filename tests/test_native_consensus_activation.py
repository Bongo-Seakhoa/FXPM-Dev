import hashlib
import json
import os
from copy import deepcopy
from dataclasses import replace
from pathlib import Path

import pandas as pd
import pytest

from pm_native_activation import (
    ACTIVE_POINTER_SCHEMA,
    NATIVE_EVIDENCE_CANDIDATE,
    NATIVE_SOURCE_UNAVAILABLE,
    NATIVE_TECHNICAL_ERROR,
    _v5_derive_recomputed_authenticity_state,
    _v5_has_unopposed_semantic_failure,
    classify_native_availability_cell,
    clear_unscoped_native_universe_memo,
    resolve_active_native_artifact,
    resolve_active_native_universe,
    sha256_file,
)
from pm_native_consensus import canonical_sha256
from pm_native_request_contract import PRODUCTION_SYMBOL_ORDER
import pm_native_request_contract as request_contract
from tools.acquire_native_history import (
    DEFAULT_TIMEFRAMES,
    cross_resolution_reference_map,
)
import tools.audit_activate_native_history as activation_tool
from tests.native_v5_support import (
    SYMBOL,
    activate_v5_run as _activate_v5_fixture,
    rewrite_no_history as _rewrite_no_history,
    write_v5_run as _write_v5_run,
)


@pytest.mark.parametrize(
    "reason",
    [
        "TARGET_AUDIT_PREFIX_PENDING:optimizer_train",
        "REFERENCE_AUDIT_PREFIX_PENDING:optimizer_train",
        "AUDIT_REFERENCE_TERMINAL_TECHNICAL:H1",
        "AUDIT_REFERENCE_RESOLUTION_EXCLUDED:M30",
        "PARTITION_OVERLAP_TOO_SMALL:optimizer_train:0<20",
        "FINE_REFERENCE_EMPTY:optimizer_train",
    ],
)
def test_terminal_pending_classifier_accepts_only_completed_authenticated_facts(
    reason,
):
    windows = {
        window: {
            "edge_states": {
                "H1": {
                    "integrity_state": "PASS",
                    "evidence_state": "PENDING",
                }
            }
        }
        for window in activation_tool.REQUIRED_AUDIT_WINDOWS
    }
    edges = {
        "H1": {
            "report": {
                "windows": {
                    window: {"reason_codes": [reason]}
                    for window in activation_tool.REQUIRED_AUDIT_WINDOWS
                }
            }
        }
    }
    envelope = {
        "diagnostics": {
            "all_references_evaluated": True,
            "raw_pair_reports_recomputed_from_pinned_bars": True,
        },
        "windows": windows,
        "edges": edges,
    }
    assert activation_tool._has_only_authenticated_terminal_pending(envelope)
    for window in activation_tool.REQUIRED_AUDIT_WINDOWS:
        edges["H1"]["report"]["windows"][window]["reason_codes"] = ["SYNTHETIC_PENDING"]
    assert not activation_tool._has_only_authenticated_terminal_pending(envelope)


def _rewrite_terminal_technical(
    native_root: Path,
    run_id: str,
    timeframe: str,
) -> None:
    """Replace one valid fixture leaf with exact two-attempt terminal evidence."""

    manifest_path = native_root / "runs" / run_id / "run_manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    leaf = native_root / "runs" / run_id / SYMBOL / timeframe
    provenance_path = leaf / "provenance.json"
    source = json.loads(provenance_path.read_text(encoding="utf-8"))
    started = "2025-04-12T00:00:00Z"
    completed = "2025-04-12T00:00:03Z"
    manifest["acquisition_started_utc"] = started
    manifest["acquisition_completed_utc"] = completed
    attempt_fields = (
        request_contract._FAILED_PROVENANCE_BASE_FIELDS
        | request_contract._FAILED_PROVENANCE_FETCH_FIELDS
    )
    attempt = {key: deepcopy(source[key]) for key in attempt_fields if key in source}
    attempt.update(
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
            "acquisition_started_utc": started,
            "acquisition_completed_utc": "2025-04-12T00:00:01Z",
            "error": {
                "type": "TechnicalHistoryGap",
                "message": "authenticated internal page gap",
            },
        }
    )
    attempt["request"]["last_fully_closed_bar_open_utc"] = None
    pages = attempt["pagination"]["page_attempts"]
    empty_page = deepcopy(pages[len(pages) // 2])
    empty_page["copy_rates_range_attempts"] = 2
    empty_page["connector_diagnostics"] = (
        request_contract._fixture_mt5_v4_empty_diagnostics(
            symbol=SYMBOL,
            timeframe=timeframe,
            start=pd.Timestamp(empty_page["start_utc"]).to_pydatetime(),
            end=pd.Timestamp(empty_page["end_exclusive_utc"]).to_pydatetime(),
        )
    )
    for index, page in enumerate(pages):
        if page["request_sequence"] == empty_page["request_sequence"]:
            pages[index] = deepcopy(empty_page)
            break
    attempt["pagination"]["empty_pages"] = [deepcopy(empty_page)]
    attempt["pagination"]["pages_with_data"] -= 1
    attempt["pagination"]["copy_rates_range_attempts_total"] += 1
    attempt["output_storage_resource_snapshot"].update(
        {"estimated_cell_bytes": 0, "sample_rows": 0}
    )

    descriptors = []
    for index, token in enumerate(("1" * 32, "2" * 32), start=1):
        record = deepcopy(attempt)
        record["acquisition_started_utc"] = f"2025-04-12T00:00:0{(index - 1) * 2}Z"
        record["acquisition_completed_utc"] = (
            f"2025-04-12T00:00:0{(index - 1) * 2 + 1}Z"
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
            "run_id": run_id,
            "symbol": SYMBOL,
            "timeframe": timeframe,
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
                "relative_path": (
                    f"_attempts/{SYMBOL}/{timeframe}/attempt-{token}.json"
                ),
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
            pd.Timestamp(value["envelope"]["record"]["acquisition_completed_utc"]),
            value["sha256"],
        ),
    )["envelope"]["record"]
    terminal_provenance = {
        **deepcopy(latest),
        "status": "failed",
        "terminal_technical_evidence": evidence,
    }
    provenance_path.write_text(
        json.dumps(terminal_provenance, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    (leaf / "bars.csv").unlink()
    evidence_bytes = request_contract.canonical_json_bytes(evidence)
    result = next(
        item
        for item in manifest["results"]
        if item["symbol"] == SYMBOL and item["timeframe"] == timeframe
    )
    result.update(
        {
            "resume_disposition": "terminal_technical",
            "status": "failed",
            "failure_status": "technical_history_gap",
            "rows": 0,
            "sha256": None,
            "eligible_for_optimization": False,
            "eligible_for_downstream_validation": False,
            "terminal_evidence_sha256": hashlib.sha256(evidence_bytes).hexdigest(),
            "terminal_evidence_bytes": len(evidence_bytes),
        }
    )
    manifest["counts"] = {
        "success": sum(item["status"] == "success" for item in manifest["results"]),
        "partial": sum(item["status"] == "partial" for item in manifest["results"]),
        "failed": sum(item["status"] == "failed" for item in manifest["results"]),
        "total": len(manifest["results"]),
    }
    manifest_path.write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def _activate(native_root: Path, run_id: str, activation_id: str):
    """Exercise the production-like exact-roster pointer boundary."""

    return _activate_v5_fixture(
        native_root,
        run_id,
        activation_id,
        production_only_pointer=True,
    )


def test_atomic_byte_restore_retries_transient_windows_share_lock(
    tmp_path,
    monkeypatch,
):
    path = tmp_path / "ACTIVE.json"
    path.write_bytes(b"old")
    original_replace = activation_tool._atomic_replace_bytes
    attempts = 0

    def _transient_replace(target, payload):
        nonlocal attempts
        attempts += 1
        if attempts < 3:
            raise PermissionError(13, "simulated Windows sharing violation")
        return original_replace(target, payload)

    monkeypatch.setattr(activation_tool, "_atomic_replace_bytes", _transient_replace)
    activation_tool._atomic_replace_bytes_with_retry(
        path,
        b"last-good",
        initial_delay_seconds=0.0,
    )

    assert attempts == 3
    assert path.read_bytes() == b"last-good"


def test_atomic_audit_leaf_publish_retries_transient_windows_share_lock(tmp_path):
    staging = tmp_path / ".H4.transient.staging"
    final_dir = tmp_path / "H4"
    staging.mkdir()
    (staging / "parity.json").write_text("{}\n", encoding="utf-8")
    attempts = []
    delays = []

    def transient_replace(source, target):
        attempts.append((Path(source), Path(target)))
        if len(attempts) < 3:
            raise PermissionError(13, "simulated Windows sharing violation")
        os.replace(source, target)

    activation_tool._replace_directory_with_retry(
        staging,
        final_dir,
        replace_fn=transient_replace,
        sleep_fn=delays.append,
    )

    assert len(attempts) == 3
    assert delays == [0.025, 0.05]
    assert not staging.exists()
    assert (final_dir / "parity.json").read_text(encoding="utf-8") == "{}\n"


def test_atomic_audit_leaf_publish_never_retries_or_replaces_collision(tmp_path):
    staging = tmp_path / ".H4.collision.staging"
    final_dir = tmp_path / "H4"
    staging.mkdir()
    attempts = []

    def colliding_replace(source, target):
        attempts.append((Path(source), Path(target)))
        final_dir.mkdir()
        (final_dir / "owner.txt").write_text("other writer", encoding="utf-8")
        raise PermissionError(13, "simulated destination collision")

    with pytest.raises(FileExistsError, match="immutable audit leaf"):
        activation_tool._replace_directory_with_retry(
            staging,
            final_dir,
            replace_fn=colliding_replace,
            sleep_fn=lambda _delay: pytest.fail("collision must not be retried"),
        )

    assert len(attempts) == 1
    assert staging.is_dir()
    assert (final_dir / "owner.txt").read_text(encoding="utf-8") == "other writer"


def test_atomic_audit_leaf_retry_exhaustion_cleans_private_staging(
    tmp_path, monkeypatch
):
    audit_root = tmp_path / "audits" / "activation"
    audit_root.mkdir(parents=True)
    monkeypatch.setattr(
        activation_tool.os,
        "replace",
        lambda _source, _target: (_ for _ in ()).throw(
            PermissionError(13, "persistent simulated sharing violation")
        ),
    )
    monkeypatch.setattr(activation_tool.time, "sleep", lambda _delay: None)

    with pytest.raises(PermissionError, match="persistent simulated"):
        activation_tool._write_audit_leaf(
            audit_root,
            "USDCAD",
            "H4",
            {"status": "PENDING"},
            {"complete": False},
        )

    symbol_dir = audit_root / "USDCAD"
    assert not (symbol_dir / "H4").exists()
    assert list(symbol_dir.glob(".H4.*.staging")) == []


def test_v5_resolution_uses_scored_partitions_without_weakening_them():
    strategy_start = pd.Timestamp("2015-01-01T00:00:00Z")
    strategy_end = pd.Timestamp("2015-03-01T00:00:00Z")
    prefix_index = pd.date_range(
        "2014-01-01T00:00:00Z",
        periods=60,
        freq="D",
    )
    evidence_index = pd.date_range(
        strategy_start,
        strategy_end,
        freq="h",
        inclusive="left",
    )

    def _frame(index):
        values = pd.Series(range(len(index)), index=index, dtype=float)
        return pd.DataFrame(
            {
                "Open": values,
                "High": values + 0.2,
                "Low": values - 0.2,
                "Close": values + 0.1,
            },
            index=index,
        )

    source = {
        "provenance": {
            "request": {
                "cell_calendar_boundaries": {
                    "optimizer_train": {
                        "start_ts": strategy_start.isoformat(),
                        "end_ts": "2015-01-20T00:00:00+00:00",
                    },
                    "tuning_validation": {
                        "start_ts": "2015-01-20T00:00:00+00:00",
                        "end_ts": "2015-02-10T00:00:00+00:00",
                    },
                    "final_lockbox": {
                        "start_ts": "2015-02-10T00:00:00+00:00",
                        "end_ts": strategy_end.isoformat(),
                    },
                }
            }
        }
    }
    combined = _frame(prefix_index.append(evidence_index))
    full_span = activation_tool.audit_native_resolution(
        combined,
        symbol=SYMBOL,
        timeframe="H1",
    )
    assert full_span.status == "FAIL"
    assert any("DAILY_LIKE_YEARS:2014" in code for code in full_span.reason_codes)

    evidence, daily, error = activation_tool._v5_scored_resolution_frames(
        source,
        combined,
        None,
    )
    assert error is None
    assert daily is None
    assert evidence is not None
    assert evidence.index.min() == strategy_start
    assert evidence.index.max() < strategy_end
    scoped = activation_tool.audit_native_resolution(
        evidence,
        symbol=SYMBOL,
        timeframe="H1",
    )
    assert scoped.status == "PASS"

    daily_like_evidence = _frame(pd.date_range(strategy_start, periods=40, freq="D"))
    evidence, _, error = activation_tool._v5_scored_resolution_frames(
        source,
        daily_like_evidence,
        None,
    )
    assert error is None
    still_blocked = activation_tool.audit_native_resolution(
        evidence,
        symbol=SYMBOL,
        timeframe="H1",
    )
    assert still_blocked.status == "FAIL"
    assert any("DAILY_LIKE_YEARS:2015" in code for code in still_blocked.reason_codes)


def test_v5_authenticated_resolution_failure_is_terminal_not_global_veto(
    tmp_path,
    monkeypatch,
):
    data_dir = tmp_path / "data"
    native_root = data_dir / "native"
    native_root.mkdir(parents=True)
    _write_v5_run(native_root, "run-resolution-exclusion")
    original = activation_tool.audit_native_resolution
    original_pair = activation_tool.audit_native_cross_resolution

    def _one_scored_resolution_failure(*args, **kwargs):
        result = original(*args, **kwargs)
        if kwargs.get("timeframe") != "H4":
            return result
        return replace(
            result,
            status="FAIL",
            reason_codes=("INTRADAY_RESOLUTION_DAILY_LIKE_YEARS:2018",),
        )

    monkeypatch.setattr(
        activation_tool,
        "audit_native_resolution",
        _one_scored_resolution_failure,
    )

    def _excluded_reference_must_not_be_consumed(*args, **kwargs):
        target = kwargs.get("activation_target_timeframe")
        pair = {kwargs.get("fine_timeframe"), kwargs.get("coarse_timeframe")}
        if target != "H4" and "H4" in pair:
            raise AssertionError("excluded H4 was reused as consensus evidence")
        return original_pair(*args, **kwargs)

    monkeypatch.setattr(
        activation_tool,
        "audit_native_cross_resolution",
        _excluded_reference_must_not_be_consumed,
    )
    activated = _activate(
        native_root,
        "run-resolution-exclusion",
        "activation-resolution-exclusion",
    )

    assert activated["status"] == "active", activated
    assert activated["counts"][NATIVE_EVIDENCE_CANDIDATE] == 5
    assert activated["counts"][NATIVE_TECHNICAL_ERROR] == 1
    assert activated["counts"]["terminal"] == 6
    excluded = activated["entries"][SYMBOL]["H4"]
    assert excluded["status"] == NATIVE_TECHNICAL_ERROR
    assert excluded["audit_status"] == "FAIL"
    assert excluded["reason_codes"] == [
        "RESOLUTION:INTRADAY_RESOLUTION_DAILY_LIKE_YEARS:2018"
    ]
    assert not activated["active_pointer_updated"]


def test_v5_authenticated_terminal_prefix_shortfall_matches_released_contract():
    from tests.test_native_request_contract import _terminal_prefix_tail_fixture

    fixture = _terminal_prefix_tail_fixture(authenticated_inside_strategy=True)
    provenance = fixture["provenance"]
    # The frozen production run predates the optional derived boundary field,
    # but its failed/boundary-shortfall evidence remains fully validated by
    # the V5 page/artifact/source contract.
    provenance["pagination"].pop("available_history_boundary")

    boundary_errors = activation_tool._pagination_contract_errors(
        provenance,
        timeframe="H1",
        manifest_request=fixture["common_request"],
        manifest_source_environment=fixture["source_environment"],
        availability_class="success",
        failure_status="boundary_history_shortfall",
    )
    assert "SOURCE_TERMINAL_PREFIX_INTERSECTS_STRATEGY" not in boundary_errors

    nonboundary_errors = activation_tool._pagination_contract_errors(
        provenance,
        timeframe="H1",
        manifest_request=fixture["common_request"],
        manifest_source_environment=fixture["source_environment"],
        availability_class="success",
        failure_status="",
    )
    assert "SOURCE_TERMINAL_PREFIX_INTERSECTS_STRATEGY" in nonboundary_errors


def test_v5_all_twelve_edges_survive_sorted_json_and_production_recomputation(tmp_path):
    data_dir = tmp_path / "data"
    native_root = data_dir / "native"
    native_root.mkdir(parents=True)
    _write_v5_run(native_root, "run-clean")

    # The compact fixture is sparse but carries authenticated rows at every
    # requested page boundary. Acquisition may admit it only to downstream
    # validation; horizon/ESS/trade-power still decide statistical sufficiency.
    run_manifest = json.loads(
        (native_root / "runs" / "run-clean" / "run_manifest.json").read_text(
            encoding="utf-8"
        )
    )
    for result in run_manifest["results"]:
        assert result["status"] == "success"
        assert result["failure_status"] is None
        assert result["rows"] > 0
        assert result["eligible_for_optimization"] is False
        assert result["eligible_for_downstream_validation"] is True

    activated = _activate(native_root, "run-clean", "activation-clean")

    assert activated["status"] == "active", activated
    assert activated["counts"][NATIVE_EVIDENCE_CANDIDATE] == 6
    assert activated["counts"][NATIVE_SOURCE_UNAVAILABLE] == 0
    for timeframe in DEFAULT_TIMEFRAMES:
        entry = activated["entries"][SYMBOL][timeframe]
        assert entry["status"] == NATIVE_EVIDENCE_CANDIDATE
        assert entry["tradeable"] is False
        assert entry["statistical_sufficiency_receipt_required"] is True
        report = json.loads(
            (native_root / entry["parity_relative_path"]).read_text(encoding="utf-8")
        )
        assert (
            report["reference_timeframes"]
            == cross_resolution_reference_map(DEFAULT_TIMEFRAMES)[timeframe]
        )
        assert set(report["edges"]) == set(report["reference_timeframes"])
        assert report["windows"]["optimizer_train"]["status"] == "PASS"
        assert report["windows"]["strategy_full_context"]["status"] == "PASS"
        assert report["windows"]["final_lockbox"]["status"] == "PASS"
        assert report["windows"]["tuning_validation"]["status"] in {
            "PASS",
            "PENDING",
        }
    assert activated["audit_only"] is True
    assert activated["production_authoritative"] is False
    assert not activated["active_pointer_updated"]
    assert not (native_root / "ACTIVE.json").exists()


def test_production_publisher_requires_force_raw_before_creating_audit(tmp_path):
    native_root = tmp_path / "data" / "native"
    native_root.mkdir(parents=True)

    with pytest.raises(
        ValueError,
        match="production publication requires semantic_verification_mode=FORCE_RAW",
    ):
        activation_tool.audit_and_activate(
            native_root,
            run_id="run-force-raw-preflight",
            symbols=PRODUCTION_SYMBOL_ORDER,
            timeframes=DEFAULT_TIMEFRAMES,
            activation_id="activation-force-raw-preflight",
            update_active_pointer=True,
        )

    assert not (native_root / "audits" / "activation-force-raw-preflight").exists()


def test_v5_production_publisher_audits_exact_600_once_and_reuses_stage(
    tmp_path,
    monkeypatch,
):
    data_dir = tmp_path / "data"
    native_root = data_dir / "native"
    native_root.mkdir(parents=True)
    _write_v5_run(native_root, "run-production-grid", production_grid=True)

    # Migration starts from the currently deployed receiptless promoted-view
    # pointer. FORCE_RAW may CAS from its exact bytes without treating that
    # legacy pointer as valid new production authority.
    pointer_path = native_root / "ACTIVE.json"
    predecessor = {
        "schema_version": ACTIVE_POINTER_SCHEMA,
        "activation_id": "legacy-promoted",
        "manifest_relative_path": "activations/promoted/legacy-promoted.json",
        "manifest_sha256": "9" * 64,
        "activated_at_utc": "2026-07-19T10:17:58Z",
        "production_authoritative": True,
    }
    pointer_path.write_text(
        json.dumps(predecessor, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    predecessor_sha = sha256_file(pointer_path)

    real_audit_symbol = activation_tool._v5_audit_symbol
    audited_cells = 0

    def _bounded_audit_symbol(*args, **kwargs):
        nonlocal audited_cells
        symbol = kwargs["symbol"]
        timeframes = tuple(kwargs["timeframes"])
        audited_cells += len(timeframes)
        if symbol == SYMBOL:
            # One real surface retains end-to-end raw audit behavior; the
            # other 594 cells are bounded structural fixtures. The focused
            # corruption test below remains the real negative control.
            return real_audit_symbol(*args, **kwargs)
        run_cells = kwargs["run_manifest_cells"]
        run_id = kwargs["run_id"]
        activation_id = kwargs["activation_id"]
        coverage = {
            partition: 1.0 for partition in activation_tool.NATIVE_EVIDENCE_PARTITIONS
        }
        entries = {}
        for timeframe in timeframes:
            run_cell = run_cells[(symbol, timeframe)]
            entries[timeframe] = {
                "status": NATIVE_EVIDENCE_CANDIDATE,
                "audit_status": "PASS",
                "universe_eligibility_status": NATIVE_EVIDENCE_CANDIDATE,
                "tradeable": False,
                "statistical_sufficiency_receipt_required": True,
                "symbol": symbol,
                "timeframe": timeframe,
                "run_id": run_id,
                "bars_relative_path": f"runs/{run_id}/{symbol}/{timeframe}/bars.csv",
                "bars_sha256": run_cell["sha256"],
                "sidecar_relative_path": (
                    f"audits/{activation_id}/{symbol}/{timeframe}/bars.meta.json"
                ),
                "sidecar_sha256": "7" * 64,
                "parity_relative_path": (
                    f"audits/{activation_id}/{symbol}/{timeframe}/parity.json"
                ),
                "parity_sha256": "8" * 64,
                "coverage_method": activation_tool.NATIVE_SESSION_COVERAGE_METHOD,
                "expected_session_coverage_by_partition": coverage,
            }
        return entries, {}, []

    monkeypatch.setattr(
        activation_tool,
        "_v5_audit_symbol",
        _bounded_audit_symbol,
    )

    activated = _activate(
        native_root,
        "run-production-grid",
        "activation-production-grid",
    )

    assert audited_cells == 600
    assert activated["status"] == "active", activated
    assert activated["previous_active_pointer_sha256"] == predecessor_sha
    assert (
        tuple(activated["universe_eligibility"]["requested_symbols"])
        == PRODUCTION_SYMBOL_ORDER
    )
    assert len(activated["entries"]) == 100
    assert all(len(grid) == 6 for grid in activated["entries"].values())
    assert activated["counts"][NATIVE_EVIDENCE_CANDIDATE] == 600
    assert activated["counts"][NATIVE_SOURCE_UNAVAILABLE] == 0
    assert activated["counts"]["total"] == 600
    assert activated["counts"]["terminal"] == 600
    assert activated["production_authoritative"] is True
    assert activated["minimum_session_coverage"] is None
    assert activated["coverage_activation_threshold_removed"] is True
    authenticity = activated["universe_eligibility"]
    assert authenticity["coverage_is_diagnostic_only"] is True
    assert authenticity["fixed_coverage_activation_threshold"] is None
    assert "minimum_session_coverage" not in authenticity
    assert len(authenticity["symbols_with_evidence_candidates"]) == 100
    assert all(
        entry["status"] == NATIVE_EVIDENCE_CANDIDATE
        and entry["bars_relative_path"]
        and entry["bars_sha256"]
        and entry["sidecar_relative_path"]
        and entry["sidecar_sha256"]
        and entry["parity_relative_path"]
        and entry["parity_sha256"]
        for grid in activated["entries"].values()
        for entry in grid.values()
    )
    for entry in activated["entries"][SYMBOL].values():
        assert entry["evidence_metadata"]["coverage_is_diagnostic_only"] is True
        report = json.loads(
            (native_root / entry["parity_relative_path"]).read_text(encoding="utf-8")
        )
        assert "minimum_session_coverage" not in report

    original_pointer = pointer_path.read_bytes()
    assert json.loads(original_pointer)["production_authoritative"] is True
    receipt = json.loads(Path(activated["semantic_receipt_path"]).read_text("utf-8"))
    assert len(receipt["native_leaf_bindings"]) == 600
    assert receipt["verification_mode"] == (
        activation_tool.NATIVE_SEMANTIC_VERIFICATION_FORCE_RAW
    )

    def _raw_fallback_forbidden(*args, **kwargs):
        raise AssertionError("receipt-backed production resolution raw-fell back")

    monkeypatch.setattr(
        "pm_native_activation._verify_activation_cell_evidence",
        _raw_fallback_forbidden,
    )
    clear_unscoped_native_universe_memo()
    universe = resolve_active_native_universe(
        data_dir,
        PRODUCTION_SYMBOL_ORDER,
        verify_exclusion_evidence=True,
        production_contract_required=True,
    )
    assert universe.valid, universe.errors
    assert universe.requested_symbols == PRODUCTION_SYMBOL_ORDER
    assert tuple(activated["entries"]) == PRODUCTION_SYMBOL_ORDER
    for broker_symbol in ("Platinum", "Palladium"):
        assert broker_symbol in activated["entries"]
        assert broker_symbol.upper() not in activated["entries"]
        for timeframe in DEFAULT_TIMEFRAMES:
            entry = activated["entries"][broker_symbol][timeframe]
            assert f"/{broker_symbol}/{timeframe}/" in (
                "/" + entry["bars_relative_path"]
            )

    wrong_case_config = tuple(
        "PLATINUM" if symbol == "Platinum" else symbol
        for symbol in PRODUCTION_SYMBOL_ORDER
    )
    wrong_config = resolve_active_native_universe(
        data_dir,
        wrong_case_config,
        verify_exclusion_evidence=True,
        production_contract_required=True,
    )
    assert not wrong_config.valid
    assert "NATIVE_UNIVERSE_CONFIG_SYMBOL_ORDER_IDENTITY_MISMATCH" in (
        wrong_config.errors
    )

    reused = _activate(
        native_root,
        "run-production-grid",
        "activation-production-grid",
    )
    assert reused["reused_existing_force_raw_audit"] is True
    assert reused["active_pointer_updated"] is False
    assert audited_cells == 600


def test_terminal_technical_classification_requires_exact_frozen_tuple():
    assert classify_native_availability_cell(
        "failed",
        "technical_history_gap",
        0,
        "terminal_technical",
    ) == ("terminal_technical", None)
    classification, reason = classify_native_availability_cell(
        "failed",
        "technical_history_gap",
        0,
    )
    assert classification == "invalid"
    assert reason == "NON_AVAILABILITY_FAILURE:technical_history_gap"
    assert classify_native_availability_cell(
        "partial",
        "technical_history_gap",
        0,
        "terminal_technical",
    ) == ("invalid", "TERMINAL_TECHNICAL_STATUS_MISMATCH")


def test_terminal_technical_only_symbol_remains_technical_not_no_trade():
    cells = {
        (SYMBOL, timeframe): {
            "status": "failed",
            "failure_status": "technical_history_gap",
            "rows": 0,
            "resume_disposition": "terminal_technical",
        }
        for timeframe in DEFAULT_TIMEFRAMES
    }
    decision = activation_tool._symbol_availability_decision(
        SYMBOL,
        DEFAULT_TIMEFRAMES,
        cells,
        (),
    )
    assert decision["status"] == NATIVE_TECHNICAL_ERROR
    assert all("TERMINAL_TECHNICAL" in reason for reason in decision["reason_codes"])


def test_terminal_technical_only_symbol_artifact_error_remains_cell_typed(tmp_path):
    data_dir = tmp_path / "data"
    native_root = data_dir / "native"
    native_root.mkdir(parents=True)
    run_id = "run-terminal-technical-only"
    _write_v5_run(native_root, run_id)
    for timeframe in DEFAULT_TIMEFRAMES:
        _rewrite_terminal_technical(native_root, run_id, timeframe)

    activated = _activate_v5_fixture(
        native_root,
        run_id,
        "activation-terminal-technical-only",
        production_only_pointer=True,
    )
    assert activated["status"] == "active", activated
    (native_root / "ACTIVE.json").write_text(
        json.dumps(
            {
                "schema_version": ACTIVE_POINTER_SCHEMA,
                "activation_id": activated["activation_id"],
                "manifest_relative_path": (
                    f"activations/{activated['activation_id']}.json"
                ),
                "manifest_sha256": activated["manifest_sha256"],
                "activated_at_utc": activated["created_at_utc"],
                "production_authoritative": False,
            },
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )

    terminal = resolve_active_native_artifact(data_dir, SYMBOL, "M15")
    assert not terminal.valid
    assert terminal.errors == ["NATIVE_ACTIVE_CELL_TERMINAL_TECHNICAL"]


def test_authenticated_terminal_technical_cell_preserves_valid_siblings(
    tmp_path,
):
    data_dir = tmp_path / "data"
    native_root = data_dir / "native"
    native_root.mkdir(parents=True)
    run_id = "run-terminal-technical"
    _write_v5_run(native_root, run_id)
    _rewrite_terminal_technical(native_root, run_id, "M15")

    activated = _activate_v5_fixture(
        native_root,
        run_id,
        "activation-terminal-technical",
        production_only_pointer=True,
    )

    assert activated["status"] == "active", activated
    assert activated["entries"][SYMBOL]["M15"]["status"] == NATIVE_TECHNICAL_ERROR
    assert activated["entries"][SYMBOL]["M15"]["audit_status"] == "PASS"
    assert activated["entries"][SYMBOL]["M15"]["tradeable"] is False
    assert (
        activated["entries"][SYMBOL]["M15"]["statistical_sufficiency_receipt_required"]
        is True
    )
    assert all(
        activated["entries"][SYMBOL][timeframe]["status"] == NATIVE_EVIDENCE_CANDIDATE
        for timeframe in DEFAULT_TIMEFRAMES
        if timeframe != "M15"
    )
    authenticity = activated["universe_eligibility"]
    assert authenticity["terminal_states"] == [
        NATIVE_EVIDENCE_CANDIDATE,
        NATIVE_SOURCE_UNAVAILABLE,
        NATIVE_TECHNICAL_ERROR,
    ]
    assert authenticity["nonterminal_states"] == ["pending"]
    assert authenticity["eligible_symbols"] == [SYMBOL]
    assert "M15" not in authenticity["evidence_candidate_timeframes_by_symbol"][SYMBOL]
    assert "M15" not in authenticity["source_unavailable_timeframes_by_symbol"][SYMBOL]

    (native_root / "ACTIVE.json").write_text(
        json.dumps(
            {
                "schema_version": ACTIVE_POINTER_SCHEMA,
                "activation_id": activated["activation_id"],
                "manifest_relative_path": (
                    f"activations/{activated['activation_id']}.json"
                ),
                "manifest_sha256": activated["manifest_sha256"],
                "activated_at_utc": activated["created_at_utc"],
                "production_authoritative": False,
            },
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )

    universe = resolve_active_native_universe(
        data_dir,
        [SYMBOL],
        verify_exclusion_evidence=True,
    )
    assert universe.valid, universe.errors
    terminal = resolve_active_native_artifact(data_dir, SYMBOL, "M15")
    assert not terminal.valid
    assert terminal.errors == ["NATIVE_ACTIVE_CELL_TERMINAL_TECHNICAL"]
    sibling = resolve_active_native_artifact(data_dir, SYMBOL, "H1")
    assert sibling.valid, sibling.errors

    provenance_path = native_root / "runs" / run_id / SYMBOL / "M15" / "provenance.json"
    tampered = json.loads(provenance_path.read_text(encoding="utf-8"))
    tampered["terminal_technical_evidence"]["attempt_cap"] = 3
    provenance_path.write_text(
        json.dumps(tampered, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    clear_unscoped_native_universe_memo()
    rejected = resolve_active_native_universe(
        data_dir,
        [SYMBOL],
        verify_exclusion_evidence=True,
    )
    assert not rejected.valid
    assert any(
        "PROVENANCE" in error or "TERMINAL_TECHNICAL" in error
        for error in rejected.errors
    )


def test_v5_wrong_case_request_result_provenance_and_leaf_path_fail_closed(tmp_path):
    native_root = tmp_path / "data" / "native"
    native_root.mkdir(parents=True)
    run_id = "run-wrong-case"
    _write_v5_run(native_root, run_id)
    manifest_path = native_root / "runs" / run_id / "run_manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["request"]["symbols"] = ["eurusd"]
    m5_result = next(item for item in manifest["results"] if item["timeframe"] == "M5")
    m5_result["symbol"] = "eurusd"
    m5_result["relative_leaf"] = "eurusd/m5"
    manifest_path.write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    provenance_path = native_root / "runs" / run_id / SYMBOL / "M5" / "provenance.json"
    provenance = json.loads(provenance_path.read_text(encoding="utf-8"))
    provenance["symbol"] = "eurusd"
    provenance["canonical_symbol"] = "eurusd"
    provenance_path.write_text(
        json.dumps(provenance, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )

    result = activation_tool.audit_and_activate(
        native_root,
        run_id=run_id,
        symbols=[SYMBOL],
        timeframes=DEFAULT_TIMEFRAMES,
        activation_id="activation-wrong-case",
        thresholds=activation_tool.ParityThresholds(min_compared_native_bars=20),
        update_active_pointer=False,
    )

    assert result["status"] == "failed"
    assert not result["active_pointer_updated"]
    reasons = [
        str(reason)
        for entry in result["entries"][SYMBOL].values()
        for reason in entry["reason_codes"]
    ]
    assert any("SYMBOL_ORDER_IDENTITY_MISMATCH" in reason for reason in reasons)
    assert any("CELL_PATH_IDENTITY_MISMATCH" in reason for reason in reasons)
    assert any("SOURCE_SYMBOL_MISMATCH" in reason for reason in reasons)
    assert any("SOURCE_CANONICAL_SYMBOL_MISMATCH" in reason for reason in reasons)


@pytest.mark.parametrize("target", DEFAULT_TIMEFRAMES)
def test_v5_single_target_corruption_isolates_cell_without_global_veto(
    tmp_path, target
):
    native_root = tmp_path / "data" / "native"
    native_root.mkdir(parents=True)
    _write_v5_run(native_root, f"run-corrupt-{target}", corrupt_timeframe=target)

    activated = _activate(
        native_root,
        f"run-corrupt-{target}",
        f"activation-corrupt-{target}",
    )

    assert activated["status"] == "active", activated
    assert not activated["active_pointer_updated"]
    statuses = {
        timeframe: entry["status"]
        for timeframe, entry in activated["entries"][SYMBOL].items()
    }
    assert statuses[target] == NATIVE_TECHNICAL_ERROR
    affected = {
        key for key, value in statuses.items() if value != NATIVE_EVIDENCE_CANDIDATE
    }
    graph = cross_resolution_reference_map(DEFAULT_TIMEFRAMES)
    possible_dependants = {
        timeframe for timeframe, references in graph.items() if target in references
    }
    assert target in affected
    assert affected <= ({target} | possible_dependants)
    if possible_dependants:
        assert any(
            activated["entries"][SYMBOL][timeframe]["consensus_disagreement_count"] > 0
            for timeframe in possible_dependants
        )
    else:
        # D1 is a target-only leaf in the released reference graph. Its own
        # pair checks fail, but no sibling can legitimately cite it.
        assert affected == {target}
    assert not (native_root / "ACTIVE.json").exists()


def test_v5_strategy_context_corruption_is_cell_local_and_cannot_hide(
    tmp_path,
):
    native_root = tmp_path / "data" / "native"
    native_root.mkdir(parents=True)
    _write_v5_run(
        native_root,
        "run-context",
        corrupt_timeframe="H1",
        context_only=True,
    )

    activated = _activate(native_root, "run-context", "activation-context")

    h1 = activated["entries"][SYMBOL]["H1"]
    report = json.loads(
        (native_root / h1["parity_relative_path"]).read_text(encoding="utf-8")
    )
    assert activated["status"] == "active"
    assert not activated["active_pointer_updated"]
    assert h1["status"] == NATIVE_TECHNICAL_ERROR
    assert report["windows"]["strategy_full_context"]["status"] == "FAIL"
    assert report["windows"]["optimizer_train"]["status"] == "PASS"
    assert all(
        report["windows"][partition]["status"] == "PASS"
        for partition in ("tuning_validation", "final_lockbox")
    )


def test_v5_one_unproven_empty_reference_is_technical_and_blocks_publish(tmp_path):
    data_dir = tmp_path / "data"
    native_root = data_dir / "native"
    native_root.mkdir(parents=True)
    _write_v5_run(native_root, "run-one-unavailable")
    _rewrite_no_history(native_root, "run-one-unavailable", "M15")

    activated = _activate(
        native_root,
        "run-one-unavailable",
        "activation-one-unavailable",
    )

    assert activated["status"] == "failed", activated
    assert activated["entries"][SYMBOL]["M15"]["status"] == NATIVE_TECHNICAL_ERROR
    assert activated["counts"][NATIVE_SOURCE_UNAVAILABLE] == 0
    assert activated["audit_only"] is True
    assert not (native_root / "ACTIVE.json").exists()


def test_v5_both_unproven_empty_references_are_integrity_error_and_block_publish(
    tmp_path,
):
    native_root = tmp_path / "data" / "native"
    native_root.mkdir(parents=True)
    _write_v5_run(native_root, "run-both-unavailable")
    _rewrite_no_history(native_root, "run-both-unavailable", "M15")
    _rewrite_no_history(native_root, "run-both-unavailable", "M30")

    activated = _activate(
        native_root,
        "run-both-unavailable",
        "activation-both-unavailable",
    )

    assert activated["status"] == "failed", activated
    assert not activated["active_pointer_updated"]
    assert activated["entries"][SYMBOL]["M5"]["status"] == NATIVE_TECHNICAL_ERROR
    report_path = (
        native_root / activated["entries"][SYMBOL]["M5"]["parity_relative_path"]
    )
    report = json.loads(report_path.read_text(encoding="utf-8"))
    assert report["status"] == "ERROR"
    assert all(detail["status"] == "ERROR" for detail in report["windows"].values())


def test_v5_unproven_all_empty_symbol_is_technical_not_no_trade(
    tmp_path,
):
    data_dir = tmp_path / "data"
    native_root = data_dir / "native"
    native_root.mkdir(parents=True)
    _write_v5_run(native_root, "run-all-unavailable")
    for timeframe in DEFAULT_TIMEFRAMES:
        _rewrite_no_history(
            native_root,
            "run-all-unavailable",
            timeframe,
        )

    activated = _activate(
        native_root,
        "run-all-unavailable",
        "activation-all-unavailable",
    )

    assert activated["status"] == "failed", activated
    assert activated["counts"][NATIVE_EVIDENCE_CANDIDATE] == 0
    assert activated["counts"][NATIVE_SOURCE_UNAVAILABLE] == 0
    assert activated["counts"][NATIVE_TECHNICAL_ERROR] == 6
    contract = activated["universe_eligibility"]
    assert contract["cell_statuses"][SYMBOL] == {
        timeframe: NATIVE_TECHNICAL_ERROR for timeframe in DEFAULT_TIMEFRAMES
    }
    assert contract["symbols_with_evidence_candidates"] == []
    assert not (native_root / "ACTIVE.json").exists()


@pytest.mark.parametrize("attack", ["valid_hex_hashes", "future_first_date"])
def test_v5_caller_asserted_absence_receipt_cannot_promote_terminal_empty(
    tmp_path,
    attack,
):
    native_root = tmp_path / "data" / "native"
    native_root.mkdir(parents=True)
    run_id = f"run-forged-absence-{attack}"
    _write_v5_run(native_root, run_id)
    _rewrite_no_history(native_root, run_id, "M15")
    provenance_path = native_root / "runs" / run_id / SYMBOL / "M15" / "provenance.json"
    provenance = json.loads(provenance_path.read_text(encoding="utf-8"))
    request = provenance["request"]
    requested_end = request["native_fetch_end_exclusive_utc"]
    proof = {
        "schema_version": "fxpm-native-source-unavailable-proof-v1",
        "symbol": SYMBOL,
        "timeframe": "M15",
        "attestation_kind": "broker_server_first_date_and_exhaustive_page_probe",
        "conclusion": "NO_SERVER_HISTORY_IN_REQUESTED_INTERVAL",
        "source_identity_sha256": "a" * 64,
        "probe_ledger_sha256": "b" * 64,
        "requested_start_utc": request["native_fetch_start_utc"],
        "requested_end_exclusive_utc": requested_end,
        "queried_at_utc": requested_end,
        "broker_server_first_open_utc": requested_end,
        "all_requested_pages_terminal_empty": True,
        "tradeable": False,
    }
    if attack == "valid_hex_hashes":
        proof["source_identity_sha256"] = "0" * 64
        proof["probe_ledger_sha256"] = "f" * 64
    else:
        proof["broker_server_first_open_utc"] = "2099-01-01T00:00:00Z"
    provenance["source_unavailable_proof"] = proof
    provenance_path.write_text(
        json.dumps(provenance, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )

    activated = _activate(
        native_root,
        run_id,
        f"activation-forged-absence-{attack}",
    )

    entry = activated["entries"][SYMBOL]["M15"]
    assert activated["status"] == "failed"
    assert not activated["active_pointer_updated"]
    assert entry["status"] == NATIVE_TECHNICAL_ERROR
    assert activated["counts"][NATIVE_SOURCE_UNAVAILABLE] == 0
    assert any(
        "NON_AVAILABILITY_FAILURE:terminal_history_empty_unresolved" in reason
        for reason in entry["reason_codes"]
    )


def test_v5_receipt_rechain_cannot_defeat_selected_leaf_raw_recomputation(tmp_path):
    data_dir = tmp_path / "data"
    native_root = data_dir / "native"
    native_root.mkdir(parents=True)
    _write_v5_run(native_root, "run-repin", production_grid=True)
    activated = _activate(native_root, "run-repin", "activation-repin")
    manifest_path = Path(activated["manifest_path"])
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    entry = manifest["entries"][SYMBOL]["D1"]
    consensus_path = native_root / entry["parity_relative_path"]
    consensus = json.loads(consensus_path.read_text(encoding="utf-8"))
    reference = consensus["reference_timeframes"][0]
    edge = consensus["edges"][reference]
    edge["report"]["windows"]["optimizer_train"]["pair_report_sha256"] = "0" * 64
    edge["report_sha256"] = canonical_sha256(edge["report"])
    consensus["consensus_sha256"] = canonical_sha256(
        {key: value for key, value in consensus.items() if key != "consensus_sha256"}
    )
    consensus_path.write_text(
        json.dumps(consensus, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    entry["parity_sha256"] = sha256_file(consensus_path)
    receipt = activation_tool.build_native_semantic_receipt(manifest)
    receipt_path = native_root / manifest["semantic_receipt_relative_path"]
    receipt_bytes = activation_tool._canonical_json_bytes(receipt)
    receipt_path.write_bytes(receipt_bytes)
    manifest["semantic_receipt_sha256"] = hashlib.sha256(receipt_bytes).hexdigest()
    manifest_bytes = activation_tool._canonical_json_bytes(manifest)
    manifest_path.write_bytes(manifest_bytes)
    pointer_path = native_root / "ACTIVE.json"
    pointer = json.loads(pointer_path.read_text(encoding="utf-8"))
    pointer["manifest_sha256"] = hashlib.sha256(manifest_bytes).hexdigest()
    pointer_path.write_bytes(activation_tool._canonical_json_bytes(pointer))

    universe = resolve_active_native_universe(
        data_dir,
        PRODUCTION_SYMBOL_ORDER,
        verify_exclusion_evidence=False,
        production_contract_required=True,
    )

    assert universe.valid, universe.errors
    artifact = resolve_active_native_artifact(
        data_dir,
        SYMBOL,
        "D1",
        production_contract_required=True,
    )
    assert not artifact.valid
    assert any("RAW_PAIR_RECOMPUTATION_MISMATCH" in error for error in artifact.errors)


def test_v5_failed_audit_never_advances_existing_active_pointer(tmp_path):
    native_root = tmp_path / "data" / "native"
    native_root.mkdir(parents=True)
    _write_v5_run(native_root, "run-first", production_grid=True)
    first = _activate(native_root, "run-first", "activation-first")
    pointer_path = native_root / "ACTIVE.json"
    first_pointer = pointer_path.read_bytes()

    _write_v5_run(native_root, "run-bad-contract")
    run_path = native_root / "runs" / "run-bad-contract" / "run_manifest.json"
    run = json.loads(run_path.read_text(encoding="utf-8"))
    run["request"]["timeframe_consensus_policy"]["evaluate_all_references"] = False
    run_path.write_text(
        json.dumps(run, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    failed = _activate(native_root, "run-bad-contract", "activation-bad-contract")

    assert first["status"] == "active"
    assert failed["status"] == "failed"
    assert not failed["active_pointer_updated"]
    assert pointer_path.read_bytes() == first_pointer


def test_v5_missing_pinned_source_is_integrity_error_not_semantic_exclusion(tmp_path):
    native_root = tmp_path / "data" / "native"
    native_root.mkdir(parents=True)
    _write_v5_run(native_root, "run-missing-bars")
    (native_root / "runs" / "run-missing-bars" / SYMBOL / "H1" / "bars.csv").unlink()

    result = _activate(
        native_root,
        "run-missing-bars",
        "activation-missing-bars",
    )

    assert result["status"] == "failed"
    assert not result["active_pointer_updated"]
    assert result["entries"][SYMBOL]["H1"]["status"] == NATIVE_TECHNICAL_ERROR
    assert any(
        "SOURCE_BARS_MISSING" in reason
        for reason in result["entries"][SYMBOL]["H1"]["reason_codes"]
    )


def test_v5_fail_plus_pending_is_unresolved_technical_and_pointer_is_unchanged(
    tmp_path,
    monkeypatch,
):
    native_root = tmp_path / "data" / "native"
    native_root.mkdir(parents=True)
    _write_v5_run(native_root, "run-pending-base", production_grid=True)
    base = _activate(
        native_root,
        "run-pending-base",
        "activation-pending-base",
    )
    pointer_path = native_root / "ACTIVE.json"
    original_pointer = pointer_path.read_bytes()
    _write_v5_run(native_root, "run-pending-candidate")
    original_audit = activation_tool.audit_native_cross_resolution

    def _mixed_edge_states(*args, **kwargs):
        report = original_audit(*args, **kwargs)
        if kwargs.get("activation_target_timeframe") != "H1":
            return report
        report = json.loads(json.dumps(report))
        window = report["partitions"]["optimizer_train"]
        if kwargs.get("fine_timeframe") == "M30":
            window["status"] = "FAIL"
            window["reason_codes"] = ["SYNTHETIC_SEMANTIC_FAIL"]
            report["status"] = "FAIL"
        elif kwargs.get("coarse_timeframe") == "H4":
            window["status"] = "PENDING"
            window["reason_codes"] = ["SYNTHETIC_PENDING"]
            report["status"] = "PENDING"
        return report

    monkeypatch.setattr(
        activation_tool,
        "audit_native_cross_resolution",
        _mixed_edge_states,
    )
    candidate = _activate(
        native_root,
        "run-pending-candidate",
        "activation-pending-candidate",
    )

    assert base["status"] == "active"
    assert candidate["status"] == "failed"
    assert candidate["entries"][SYMBOL]["H1"]["status"] == NATIVE_TECHNICAL_ERROR
    parity = json.loads(
        (
            native_root / candidate["entries"][SYMBOL]["H1"]["parity_relative_path"]
        ).read_text(encoding="utf-8")
    )
    assert _v5_has_unopposed_semantic_failure(parity)
    assert (
        _v5_derive_recomputed_authenticity_state(
            parity,
            resolution_status=candidate["entries"][SYMBOL]["H1"]["resolution_status"],
        )
        == NATIVE_TECHNICAL_ERROR
    )
    assert not candidate["active_pointer_updated"]
    assert pointer_path.read_bytes() == original_pointer


def test_v5_unknown_pure_pending_cannot_become_evidence_candidate(
    tmp_path,
    monkeypatch,
):
    native_root = tmp_path / "data" / "native"
    native_root.mkdir(parents=True)
    _write_v5_run(native_root, "run-unknown-pending")
    original_audit = activation_tool.audit_native_cross_resolution

    def _unknown_pending(*args, **kwargs):
        report = original_audit(*args, **kwargs)
        if kwargs.get("activation_target_timeframe") != "H1":
            return report
        report = json.loads(json.dumps(report))
        window = report["partitions"]["optimizer_train"]
        window["status"] = "PENDING"
        window["reason_codes"] = ["SYNTHETIC_PENDING"]
        report["status"] = "PENDING"
        return report

    monkeypatch.setattr(
        activation_tool,
        "audit_native_cross_resolution",
        _unknown_pending,
    )
    result = _activate(
        native_root,
        "run-unknown-pending",
        "activation-unknown-pending",
    )

    assert result["status"] == "pending"
    assert result["entries"][SYMBOL]["H1"]["status"] == "pending"
    assert result["entries"][SYMBOL]["H1"]["audit_status"] == "PENDING"
    assert not result["active_pointer_updated"]


def test_v5_authenticated_terminal_pending_remains_evidence_candidate(
    tmp_path,
    monkeypatch,
):
    native_root = tmp_path / "data" / "native"
    native_root.mkdir(parents=True)
    _write_v5_run(native_root, "run-authenticated-pending")
    original_audit = activation_tool.audit_native_cross_resolution

    def _authenticated_pending(*args, **kwargs):
        report = original_audit(*args, **kwargs)
        if kwargs.get("activation_target_timeframe") != "H1":
            return report
        report = json.loads(json.dumps(report))
        window = report["partitions"]["optimizer_train"]
        window["status"] = "PENDING"
        window["reason_codes"] = ["PARTITION_OVERLAP_TOO_SMALL:optimizer_train:19<20"]
        report["status"] = "PENDING"
        return report

    monkeypatch.setattr(
        activation_tool,
        "audit_native_cross_resolution",
        _authenticated_pending,
    )
    result = _activate(
        native_root,
        "run-authenticated-pending",
        "activation-authenticated-pending",
    )

    assert result["status"] == "active", result
    assert result["entries"][SYMBOL]["H1"]["status"] == NATIVE_EVIDENCE_CANDIDATE
    assert result["entries"][SYMBOL]["H1"]["audit_status"] == "PASS"
    assert result["entries"][SYMBOL]["H1"]["consensus_status"] == "PENDING"
    parity = json.loads(
        (
            native_root / result["entries"][SYMBOL]["H1"]["parity_relative_path"]
        ).read_text(encoding="utf-8")
    )
    assert not _v5_has_unopposed_semantic_failure(parity)
    assert (
        _v5_derive_recomputed_authenticity_state(
            parity,
            resolution_status=result["entries"][SYMBOL]["H1"]["resolution_status"],
        )
        == NATIVE_EVIDENCE_CANDIDATE
    )


def test_v5_runtime_derivation_keeps_semantic_failure_with_pass_witness_candidate():
    envelope = {
        "status": "PENDING",
        "windows": {
            window: {
                "edge_states": {
                    "H1": {
                        "integrity_state": "PASS",
                        "evidence_state": "SEMANTIC_FAIL",
                    },
                    "D1": {
                        "integrity_state": "PASS",
                        "evidence_state": "PASS",
                    },
                }
            }
            for window in activation_tool.REQUIRED_AUDIT_WINDOWS
        },
    }

    assert not _v5_has_unopposed_semantic_failure(envelope)
    assert (
        _v5_derive_recomputed_authenticity_state(
            envelope,
            resolution_status="PASS",
        )
        == NATIVE_EVIDENCE_CANDIDATE
    )
