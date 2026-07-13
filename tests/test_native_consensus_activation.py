import json
import os
from pathlib import Path

import pytest

from pm_native_activation import (
    resolve_active_native_artifact,
    resolve_active_native_universe,
    sha256_file,
)
from pm_native_consensus import canonical_sha256
from tools.acquire_native_history import (
    DEFAULT_TIMEFRAMES,
    cross_resolution_reference_map,
)
import tools.audit_activate_native_history as activation_tool
from tests.native_v5_support import (
    SYMBOL,
    activate_v5_run as _activate,
    rewrite_no_history as _rewrite_no_history,
    write_v5_run as _write_v5_run,
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

    activated = _activate(native_root, "run-clean", "activation-clean")

    assert activated["status"] == "active", activated
    assert activated["counts"]["active"] == 6
    for timeframe in DEFAULT_TIMEFRAMES:
        entry = activated["entries"][SYMBOL][timeframe]
        report = json.loads(
            (native_root / entry["parity_relative_path"]).read_text(encoding="utf-8")
        )
        assert report["reference_timeframes"] == cross_resolution_reference_map(
            DEFAULT_TIMEFRAMES
        )[timeframe]
        assert set(report["edges"]) == set(report["reference_timeframes"])
        assert all(
            detail["status"] == "PASS" for detail in report["windows"].values()
        )
    universe = resolve_active_native_universe(
        data_dir,
        [SYMBOL],
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
    assert artifact.valid, artifact.errors


@pytest.mark.parametrize("target", DEFAULT_TIMEFRAMES)
def test_v5_single_target_corruption_isolates_only_that_specialist(tmp_path, target):
    native_root = tmp_path / "data" / "native"
    native_root.mkdir(parents=True)
    _write_v5_run(native_root, f"run-corrupt-{target}", corrupt_timeframe=target)

    activated = _activate(
        native_root,
        f"run-corrupt-{target}",
        f"activation-corrupt-{target}",
    )

    assert activated["status"] == "active", activated
    statuses = {
        timeframe: entry["status"]
        for timeframe, entry in activated["entries"][SYMBOL].items()
    }
    assert statuses[target] == "excluded_no_trade"
    assert {key for key, value in statuses.items() if value != "active"} == {target}
    assert any(
        entry["consensus_disagreement_count"] > 0
        for timeframe, entry in activated["entries"][SYMBOL].items()
        if timeframe != target
    )
    universe = resolve_active_native_universe(
        tmp_path / "data",
        [SYMBOL],
        production_contract_required=True,
    )
    assert universe.valid, universe.errors


def test_v5_strategy_context_corruption_cannot_hide_outside_optimizer_partitions(tmp_path):
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
    assert h1["status"] == "excluded_no_trade"
    assert report["windows"]["strategy_full_context"]["status"] == "FAIL"
    assert all(
        report["windows"][partition]["status"] == "PASS"
        for partition in ("optimizer_train", "tuning_validation", "final_lockbox")
    )


def test_v5_one_terminal_reference_plus_one_exact_pass_remains_active(tmp_path):
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

    assert activated["status"] == "active", activated
    assert activated["entries"][SYMBOL]["M15"]["status"] == "excluded_no_trade"
    assert activated["entries"][SYMBOL]["M5"]["status"] == "active"
    assert activated["entries"][SYMBOL]["M5"]["consensus_disagreement_count"] == 4
    universe = resolve_active_native_universe(
        data_dir,
        [SYMBOL],
        production_contract_required=True,
    )
    assert universe.valid, universe.errors


def test_v5_both_terminal_references_form_semantic_cell_exclusion(tmp_path):
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

    assert activated["status"] == "active", activated
    assert activated["entries"][SYMBOL]["M5"]["status"] == "excluded_no_trade"
    report_path = (
        native_root
        / activated["entries"][SYMBOL]["M5"]["parity_relative_path"]
    )
    report = json.loads(report_path.read_text(encoding="utf-8"))
    assert report["status"] == "FAIL"
    assert all(detail["status"] == "FAIL" for detail in report["windows"].values())


def test_v5_authenticated_all_unavailable_symbol_resolves_as_no_trade(tmp_path):
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

    assert activated["status"] == "active", activated
    assert activated["counts"]["active"] == 0
    assert activated["counts"]["excluded_no_trade"] == 6
    universe = resolve_active_native_universe(
        data_dir,
        [SYMBOL],
        production_contract_required=True,
    )
    assert universe.valid, universe.errors
    assert universe.symbol_status(SYMBOL) == "excluded_no_trade"
    assert universe.active_timeframes(SYMBOL) == ()
    assert universe.excluded_timeframes(SYMBOL) == DEFAULT_TIMEFRAMES


def test_v5_repinning_fabricated_consensus_does_not_defeat_raw_recomputation(tmp_path):
    data_dir = tmp_path / "data"
    native_root = data_dir / "native"
    native_root.mkdir(parents=True)
    _write_v5_run(native_root, "run-repin")
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
        [SYMBOL],
        verify_exclusion_evidence=False,
        production_contract_required=True,
    )

    assert not universe.valid
    assert any(
        "RAW_PAIR_RECOMPUTATION_MISMATCH" in error
        or "EDGE_PAIR_HASH_INVALID" in error
        for error in universe.errors
    )
    artifact = resolve_active_native_artifact(
        data_dir,
        SYMBOL,
        "D1",
        production_contract_required=True,
    )
    assert not artifact.valid
    assert any(
        "RAW_PAIR_RECOMPUTATION_MISMATCH" in error
        for error in artifact.errors
    )


def test_v5_failed_audit_never_advances_existing_active_pointer(tmp_path):
    native_root = tmp_path / "data" / "native"
    native_root.mkdir(parents=True)
    _write_v5_run(native_root, "run-first")
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
    (
        native_root
        / "runs"
        / "run-missing-bars"
        / SYMBOL
        / "H1"
        / "bars.csv"
    ).unlink()

    result = _activate(
        native_root,
        "run-missing-bars",
        "activation-missing-bars",
    )

    assert result["status"] == "failed"
    assert not result["active_pointer_updated"]
    assert result["entries"][SYMBOL]["H1"]["status"] == "failed"
    assert any(
        "SOURCE_BARS_MISSING" in reason
        for reason in result["entries"][SYMBOL]["H1"]["reason_codes"]
    )


def test_v5_fail_plus_pending_consensus_stays_pending_and_pointer_is_unchanged(
    tmp_path,
    monkeypatch,
):
    native_root = tmp_path / "data" / "native"
    native_root.mkdir(parents=True)
    _write_v5_run(native_root, "run-pending-base")
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
    assert candidate["status"] == "pending"
    assert candidate["entries"][SYMBOL]["H1"]["status"] == "pending"
    assert not candidate["active_pointer_updated"]
    assert pointer_path.read_bytes() == original_pointer
