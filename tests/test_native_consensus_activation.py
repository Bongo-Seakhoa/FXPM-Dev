import json
import os
from pathlib import Path

import pytest

from pm_native_activation import (
    NATIVE_EVIDENCE_CANDIDATE,
    NATIVE_SOURCE_UNAVAILABLE,
    NATIVE_TECHNICAL_ERROR,
    resolve_active_native_artifact,
    resolve_active_native_universe,
    sha256_file,
)
from pm_native_consensus import canonical_sha256
from pm_native_request_contract import PRODUCTION_SYMBOL_ORDER
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


def _activate(native_root: Path, run_id: str, activation_id: str):
    """Exercise the production-like exact-roster pointer boundary."""

    return _activate_v5_fixture(
        native_root,
        run_id,
        activation_id,
        production_only_pointer=True,
    )


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


def test_v5_production_grid_accounts_for_all_600_cells(tmp_path):
    data_dir = tmp_path / "data"
    native_root = data_dir / "native"
    native_root.mkdir(parents=True)
    _write_v5_run(native_root, "run-production-grid", production_grid=True)

    activated = _activate(
        native_root,
        "run-production-grid",
        "activation-production-grid",
    )

    assert activated["status"] == "active", activated
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
    pointer_path = native_root / "ACTIVE.json"
    original_pointer = pointer_path.read_bytes()
    assert json.loads(original_pointer)["production_authoritative"] is True
    universe = resolve_active_native_universe(
        data_dir,
        PRODUCTION_SYMBOL_ORDER,
        verify_exclusion_evidence=False,
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
        verify_exclusion_evidence=False,
        production_contract_required=True,
    )
    assert not wrong_config.valid
    assert "NATIVE_UNIVERSE_CONFIG_SYMBOL_ORDER_IDENTITY_MISMATCH" in (
        wrong_config.errors
    )

    pointer = json.loads(original_pointer)
    pointer["manifest_relative_path"] = pointer["manifest_relative_path"].replace(
        "activations/", "Activations/", 1
    )
    pointer_path.write_text(
        json.dumps(pointer, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    wrong_pointer = resolve_active_native_universe(
        data_dir,
        PRODUCTION_SYMBOL_ORDER,
        verify_exclusion_evidence=False,
        production_contract_required=True,
    )
    assert not wrong_pointer.valid
    assert "NATIVE_ACTIVE_MANIFEST_PATH_IDENTITY_MISMATCH" in wrong_pointer.errors
    pointer_path.write_bytes(original_pointer)

    pointer = json.loads(original_pointer)
    pointer["production_authoritative"] = False
    pointer_path.write_text(
        json.dumps(pointer, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    forged_authority = resolve_active_native_universe(
        data_dir,
        PRODUCTION_SYMBOL_ORDER,
        verify_exclusion_evidence=False,
        production_contract_required=True,
    )
    assert not forged_authority.valid
    assert "NATIVE_ACTIVE_PRODUCTION_AUTHORITY_MARKER_MISMATCH" in (
        forged_authority.errors
    )
    pointer_path.write_bytes(original_pointer)

    manifest_path = Path(activated["manifest_path"])
    original_manifest = manifest_path.read_bytes()
    manifest = json.loads(original_manifest)
    eurusd_m5 = manifest["entries"][SYMBOL]["M5"]
    eurusd_m5["symbol"] = "eurusd"
    eurusd_m5["bars_relative_path"] = eurusd_m5["bars_relative_path"].replace(
        "/EURUSD/", "/eurusd/"
    )
    manifest_path.write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    pointer = json.loads(original_pointer)
    pointer["manifest_sha256"] = sha256_file(manifest_path)
    pointer_path.write_text(
        json.dumps(pointer, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    wrong_path = resolve_active_native_artifact(
        data_dir,
        SYMBOL,
        "M5",
        production_contract_required=True,
    )
    assert not wrong_path.valid
    assert "NATIVE_ACTIVE_CELL_SYMBOL_MISMATCH" in wrong_path.errors
    assert any("BARS_RELATIVE_PATH_IDENTITY_MISMATCH" in e for e in wrong_path.errors)
    manifest_path.write_bytes(original_manifest)
    pointer_path.write_bytes(original_pointer)

    run_manifest_path = (
        native_root / "runs" / "run-production-grid" / "run_manifest.json"
    )
    original_run_manifest = run_manifest_path.read_bytes()
    run_manifest = json.loads(original_run_manifest)
    run_manifest["request"]["timeframe_request_map_schema_version"] = (
        "fxpm.native_history.timeframe_request_map.v4"
    )
    run_manifest_path.write_text(
        json.dumps(run_manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    manifest = json.loads(original_manifest)
    manifest["run_manifest_sha256"] = sha256_file(run_manifest_path)
    manifest_path.write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    pointer = json.loads(original_pointer)
    pointer["manifest_sha256"] = sha256_file(manifest_path)
    pointer_path.write_text(
        json.dumps(pointer, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    downgraded = resolve_active_native_universe(
        data_dir,
        wrong_case_config,
        verify_exclusion_evidence=False,
        production_contract_required=False,
    )
    assert not downgraded.valid
    assert "NATIVE_AUTHENTICITY_V5_REQUEST_IDENTITY_REQUIRED" in downgraded.errors
    run_manifest_path.write_bytes(original_run_manifest)
    manifest_path.write_bytes(original_manifest)
    pointer_path.write_bytes(original_pointer)


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
def test_v5_single_target_corruption_isolates_cell_and_blocks_publication(
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

    assert activated["status"] == "failed", activated
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


def test_v5_strategy_context_corruption_cannot_hide_outside_optimizer_partitions(
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
    assert activated["status"] == "failed"
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


def test_v5_repinning_fabricated_consensus_does_not_defeat_raw_recomputation(tmp_path):
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
    manifest_path.write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    pointer_path = native_root / "ACTIVE.json"
    pointer = json.loads(pointer_path.read_text(encoding="utf-8"))
    pointer["manifest_sha256"] = sha256_file(manifest_path)
    pointer_path.write_text(
        json.dumps(pointer, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )

    universe = resolve_active_native_universe(
        data_dir,
        PRODUCTION_SYMBOL_ORDER,
        verify_exclusion_evidence=False,
        production_contract_required=True,
    )

    assert not universe.valid
    assert any(
        "RAW_PAIR_RECOMPUTATION_MISMATCH" in error or "EDGE_PAIR_HASH_INVALID" in error
        for error in universe.errors
    )
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
    assert not candidate["active_pointer_updated"]
    assert pointer_path.read_bytes() == original_pointer
