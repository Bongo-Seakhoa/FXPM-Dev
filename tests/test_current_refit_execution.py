from __future__ import annotations

import hashlib
import inspect
import json
import sys
import types
from pathlib import Path

import pytest

from pm_current_refit_execution import (
    CURRENT_REGIME_PARAMS_BUNDLE_VERSION,
    REGIME_FIT_RECEIPT_VERSION,
    RAW_CURRENT_REFIT_EXECUTOR_FUNCTION,
    RAW_CURRENT_REFIT_EXECUTOR_VERSION,
    build_raw_artifact_descriptor,
    build_raw_current_refit_manifest,
    load_bounded_raw_current_refit_manifest,
    validate_raw_current_refit_manifest,
    verify_raw_current_refit_execution,
)
from pm_model_lifecycle import (
    CURRENT_REFIT_EXECUTION_RECEIPT_TYPE,
    canonical_receipt_sha256,
    open_typed_receipt,
)
from tests.test_model_lifecycle import _process_refit_receipts, _regime_params_bundle


def _raw_fixture(tmp_path: Path):
    _process, book, receipt = _process_refit_receipts()
    payload, _receipt_sha = open_typed_receipt(
        receipt,
        expected_receipt_type=CURRENT_REFIT_EXECUTION_RECEIPT_TYPE,
    )
    authorization = payload["authorization"]
    cells = payload["execution"]["cell_executions"]
    descriptors = []

    def add(
        name: str,
        semantic_sha256: str,
        *,
        content: bytes | None = None,
        format_version: str = "fxpm.test-raw-input.v1",
    ) -> str:
        relative = f"inputs/{name}.json"
        path = tmp_path / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        content = content or (f'{{"artifact":"{name}"}}\n').encode()
        path.write_bytes(content)
        descriptor = build_raw_artifact_descriptor(
            relative_path=relative,
            size_bytes=len(content),
            content_sha256=hashlib.sha256(content).hexdigest(),
            semantic_sha256=semantic_sha256,
            format_version=format_version,
        )
        descriptors.append(descriptor)
        return descriptor["artifact_id"]

    bundle_ids = {
        "accepted_book_generation": add(
            "accepted-book",
            authorization["accepted_book_generation_receipt_sha256"],
            content=json.dumps(
                book,
                allow_nan=False,
                ensure_ascii=False,
                separators=(",", ":"),
                sort_keys=True,
            ).encode("utf-8"),
        ),
        "source_snapshot_bundle": add(
            "source-bundle", authorization["source_snapshot_bundle_sha256"]
        ),
        "regime_params_bundle": add(
            "regime-bundle",
            authorization["regime_params_sha256"],
            content=json.dumps(
                _regime_params_bundle(authorization["current_cutoff_utc"]),
                allow_nan=False,
                ensure_ascii=False,
                separators=(",", ":"),
                sort_keys=True,
            ).encode("utf-8"),
            format_version=CURRENT_REGIME_PARAMS_BUNDLE_VERSION,
        ),
        "horizon_decisions_bundle": add(
            "horizon-bundle", authorization["horizon_decisions_sha256"]
        ),
    }
    requests = []
    for index, cell in enumerate(cells):
        artifact_ids = {
            "source_snapshot": add(f"source-{index}", cell["source_snapshot_sha256"]),
            "winner_process": add(f"process-{index}", cell["winner_process_sha256"]),
            "horizon_process": add(
                f"horizon-{index}", cell["winner_horizon_process_sha256"]
            ),
            "trial_definition": add(
                f"trial-{index}", cell["winner_trial_definition_sha256"]
            ),
            "refit_recipe": add(f"recipe-{index}", cell["winner_refit_recipe_sha256"]),
            "winner_book_entry": add(
                f"book-entry-{index}", cell["winner_book_entry_sha256"]
            ),
        }
        requests.append(
            {
                name: cell[name]
                for name in (
                    "current_refit_cell_id",
                    "symbol",
                    "timeframe",
                    "regime",
                    "strategy_id",
                    "fit_start_utc",
                    "fit_end_utc",
                    "latest_closed_bar_utc",
                    "source_snapshot_sha256",
                    "winner_process_sha256",
                    "winner_horizon_process_sha256",
                    "winner_trial_definition_sha256",
                    "winner_refit_recipe_sha256",
                    "winner_book_entry_sha256",
                    "regime_fit_receipt_sha256",
                    "regime_detector_record_sha256",
                    "regime_detector_parameters_sha256",
                    "regime_detector_code_sha256",
                )
            }
        )
        requests[-1]["artifact_ids"] = artifact_ids
    manifest = build_raw_current_refit_manifest(
        current_refit_receipt=receipt,
        bundle_artifact_ids=bundle_ids,
        artifact_descriptors=descriptors,
        cell_requests=requests,
    )
    return receipt, manifest


def _refresh_manifest(manifest: dict) -> None:
    body = dict(manifest)
    body.pop("manifest_sha256", None)
    manifest["manifest_sha256"] = canonical_receipt_sha256(body)


def _canonical_ascii(value) -> bytes:
    return json.dumps(
        value,
        allow_nan=False,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("ascii")


def _rewrite_regime_bundle(tmp_path: Path, manifest: dict, bundle: dict) -> None:
    old_id = manifest["bundle_artifact_ids"]["regime_params_bundle"]
    index = next(
        index
        for index, item in enumerate(manifest["artifact_descriptors"])
        if item["artifact_id"] == old_id
    )
    old = manifest["artifact_descriptors"][index]
    content = json.dumps(
        bundle,
        allow_nan=False,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    (tmp_path / old["relative_path"]).write_bytes(content)
    replacement = build_raw_artifact_descriptor(
        relative_path=old["relative_path"],
        size_bytes=len(content),
        content_sha256=hashlib.sha256(content).hexdigest(),
        semantic_sha256=old["semantic_sha256"],
        format_version=old["format_version"],
    )
    manifest["artifact_descriptors"][index] = replacement
    manifest["bundle_artifact_ids"]["regime_params_bundle"] = replacement["artifact_id"]
    _refresh_manifest(manifest)


def test_bounded_raw_manifest_round_trips_all_input_bindings(tmp_path: Path) -> None:
    receipt, manifest = _raw_fixture(tmp_path)

    plan, errors = validate_raw_current_refit_manifest(
        receipt, manifest, artifact_root=tmp_path
    )

    assert errors == []
    assert plan is not None
    assert len(plan["cell_requests"]) == 2
    assert all(
        "parameter_sha256" not in request
        and "output_artifact_sha256" not in request
        and "execution_trace_sha256" not in request
        and "source_snapshot_sha256" not in request
        and "winner_refit_recipe_sha256" not in request
        and "regime_fit_receipt_sha256" not in request
        and "regime_detector_record_sha256" not in request
        and "regime_detector_parameters_sha256" not in request
        and "regime_detector_code_sha256" not in request
        for request in plan["cell_requests"]
    )
    assert all(
        "semantic_sha256" not in descriptor
        for descriptor in plan["artifact_descriptors"]
    )


def test_production_verifier_has_no_callback_and_blocks_missing_executor(
    monkeypatch,
    tmp_path: Path,
) -> None:
    receipt, manifest = _raw_fixture(tmp_path)
    module = types.ModuleType("pm_pipeline")
    module.CURRENT_REFIT_RAW_EXECUTOR_VERSION = "unavailable"
    monkeypatch.setitem(sys.modules, "pm_pipeline", module)

    assert (
        "executor"
        not in inspect.signature(verify_raw_current_refit_execution).parameters
    )
    result = verify_raw_current_refit_execution(
        receipt, manifest, artifact_root=tmp_path
    )

    assert result["verified"] is False
    assert "CURRENT_REFIT_RAW_EXECUTOR_UNAVAILABLE" in result["errors"]
    assert result["errors"][-1] == "CURRENT_REFIT_EXECUTION_REVERIFIER_REQUIRED"


def test_expected_outputs_cannot_be_smuggled_into_replay_request(
    tmp_path: Path,
) -> None:
    receipt, manifest = _raw_fixture(tmp_path)
    manifest["cell_requests"][0]["parameter_sha256"] = "a" * 64
    _refresh_manifest(manifest)

    _plan, errors = validate_raw_current_refit_manifest(
        receipt, manifest, artifact_root=tmp_path
    )

    assert "RAW_CURRENT_REFIT_CELL_REQUEST_FIELDS_INVALID:0" in errors


def test_executor_cannot_launder_declared_semantics_or_receipt_outputs(
    monkeypatch, tmp_path: Path
) -> None:
    receipt, manifest = _raw_fixture(tmp_path)

    def laundering_executor(plan, *, artifact_root):
        assert artifact_root == tmp_path.resolve()
        descriptor = plan["artifact_descriptors"][0]
        request = plan["cell_requests"][0]
        assert "semantic_sha256" not in descriptor
        assert "parameter_sha256" not in request
        assert "output_artifact_sha256" not in request
        raise RuntimeError("semantic and output expectations unavailable")

    laundering_executor.__module__ = "pm_pipeline"
    laundering_executor.__name__ = RAW_CURRENT_REFIT_EXECUTOR_FUNCTION
    module = types.ModuleType("pm_pipeline")
    module.CURRENT_REFIT_RAW_EXECUTOR_VERSION = RAW_CURRENT_REFIT_EXECUTOR_VERSION
    setattr(module, RAW_CURRENT_REFIT_EXECUTOR_FUNCTION, laundering_executor)
    monkeypatch.setitem(sys.modules, "pm_pipeline", module)

    result = verify_raw_current_refit_execution(
        receipt, manifest, artifact_root=tmp_path
    )

    assert "CURRENT_REFIT_RAW_REPLAY_FAILED:RuntimeError" in result["errors"]
    assert result["errors"][-1] == "CURRENT_REFIT_EXECUTION_REVERIFIER_REQUIRED"


def test_raw_artifact_bytes_are_rechecked_not_checksum_trusted(tmp_path: Path) -> None:
    receipt, manifest = _raw_fixture(tmp_path)
    relative = manifest["artifact_descriptors"][0]["relative_path"]
    (tmp_path / relative).write_bytes(b"forged\n")

    _plan, errors = validate_raw_current_refit_manifest(
        receipt, manifest, artifact_root=tmp_path
    )

    artifact_id = manifest["artifact_descriptors"][0]["artifact_id"]
    assert f"RAW_CURRENT_REFIT_ARTIFACT_FILE_INVALID:{artifact_id}" in errors


def test_receipt_only_refit_stays_blocked() -> None:
    _process, _book, receipt = _process_refit_receipts()

    result = verify_raw_current_refit_execution(receipt, None, artifact_root=None)

    assert result["verified"] is False
    assert result["errors"] == ["CURRENT_REFIT_EXECUTION_REVERIFIER_REQUIRED"]


def test_manifest_file_loader_rejects_empty_input(tmp_path: Path) -> None:
    path = tmp_path / "empty.json"
    path.write_bytes(b"")

    with pytest.raises(ValueError, match="MANIFEST_FILE_SIZE_INVALID"):
        load_bounded_raw_current_refit_manifest(path)


def test_regime_bundle_rederives_fit_record_source_and_selected_cell_links(
    tmp_path: Path,
) -> None:
    receipt, manifest = _raw_fixture(tmp_path)
    bundle_id = manifest["bundle_artifact_ids"]["regime_params_bundle"]
    descriptor = next(
        item
        for item in manifest["artifact_descriptors"]
        if item["artifact_id"] == bundle_id
    )
    bundle = json.loads((tmp_path / descriptor["relative_path"]).read_text())
    first = bundle["regime_fits"][0]
    record = first["payload"]["regime_detector_record"]

    plan, errors = validate_raw_current_refit_manifest(
        receipt, manifest, artifact_root=tmp_path
    )

    assert errors == []
    assert plan is not None
    assert (
        first["receipt_sha256"]
        == hashlib.sha256(_canonical_ascii(first["payload"])).hexdigest()
    )
    record_body = dict(record)
    record_sha = record_body.pop("record_sha256")
    assert record_sha == hashlib.sha256(_canonical_ascii(record_body)).hexdigest()
    assert record["activation_id"]
    assert record["activation_manifest_sha256"]
    assert record["native_source_sha256"]


def test_rehashed_detector_source_splice_cannot_launder_through_self_hashes(
    tmp_path: Path,
) -> None:
    receipt, manifest = _raw_fixture(tmp_path)
    bundle_id = manifest["bundle_artifact_ids"]["regime_params_bundle"]
    descriptor = next(
        item
        for item in manifest["artifact_descriptors"]
        if item["artifact_id"] == bundle_id
    )
    bundle = json.loads((tmp_path / descriptor["relative_path"]).read_text())
    fit = bundle["regime_fits"][0]
    record = fit["payload"]["regime_detector_record"]
    record["native_source_sha256"] = "e" * 64
    support = fit["payload"]["train_regime_support"]
    support["bindings"]["source_sha256"] = "e" * 64
    support_body = dict(support)
    support_body.pop("receipt_sha256")
    support["receipt_sha256"] = hashlib.sha256(
        _canonical_ascii(support_body)
    ).hexdigest()
    record_body = dict(record)
    record_body.pop("record_sha256")
    record["record_sha256"] = hashlib.sha256(_canonical_ascii(record_body)).hexdigest()
    fit["payload"]["regime_detector_record_sha256"] = record["record_sha256"]
    fit["receipt_sha256"] = hashlib.sha256(_canonical_ascii(fit["payload"])).hexdigest()
    _rewrite_regime_bundle(tmp_path, manifest, bundle)

    _plan, errors = validate_raw_current_refit_manifest(
        receipt, manifest, artifact_root=tmp_path
    )

    assert any("REGIME_PARAMS_BUNDLE_SELECTED_CELL_SPLICE" in error for error in errors)


def test_regime_bundle_fit_receipt_and_canonical_parameter_bytes_are_rederived(
    tmp_path: Path,
) -> None:
    receipt, manifest = _raw_fixture(tmp_path)
    bundle_id = manifest["bundle_artifact_ids"]["regime_params_bundle"]
    descriptor = next(
        item
        for item in manifest["artifact_descriptors"]
        if item["artifact_id"] == bundle_id
    )
    bundle = json.loads((tmp_path / descriptor["relative_path"]).read_text())
    fit = bundle["regime_fits"][0]
    fit["receipt_sha256"] = "f" * 64
    _rewrite_regime_bundle(tmp_path, manifest, bundle)

    _plan, errors = validate_raw_current_refit_manifest(
        receipt, manifest, artifact_root=tmp_path
    )

    assert any("REGIME_FIT_RECEIPT_SHA256_INVALID" in error for error in errors)


def test_regime_bundle_requires_current_v4_fit_contract(tmp_path: Path) -> None:
    assert REGIME_FIT_RECEIPT_VERSION == "fxpm.historical-regime-fit.v4"
    receipt, manifest = _raw_fixture(tmp_path)
    bundle_id = manifest["bundle_artifact_ids"]["regime_params_bundle"]
    descriptor = next(
        item
        for item in manifest["artifact_descriptors"]
        if item["artifact_id"] == bundle_id
    )
    bundle = json.loads((tmp_path / descriptor["relative_path"]).read_text())
    fit = bundle["regime_fits"][0]
    fit["payload"]["contract_version"] = "fxpm.historical-regime-fit.v3"
    fit["receipt_sha256"] = hashlib.sha256(_canonical_ascii(fit["payload"])).hexdigest()
    _rewrite_regime_bundle(tmp_path, manifest, bundle)

    _plan, errors = validate_raw_current_refit_manifest(
        receipt, manifest, artifact_root=tmp_path
    )

    assert any("REGIME_FIT_SEMANTICS_INVALID" in error for error in errors)
