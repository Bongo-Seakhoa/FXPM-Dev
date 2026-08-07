from __future__ import annotations

import json
import shutil
from pathlib import Path
from typing import Any

import pytest

import pm_raw_winner_canary_conformance as conformance


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SOURCE_ROOT = (
    PROJECT_ROOT / ".pytest-tmp" / "raw-winner-canary" / conformance.PRESERVED_ROOT_ID
)
CONFIG_PATH = PROJECT_ROOT / "config.json"


def _copy_preserved_root(parent: Path) -> Path:
    if not SOURCE_ROOT.is_dir():
        pytest.skip("exact preserved raw-Winner canary root is not available")
    target = parent / conformance.PRESERVED_ROOT_ID
    shutil.copytree(SOURCE_ROOT, target)
    return target


@pytest.fixture(scope="module")
def conformed_receipt(
    tmp_path_factory: pytest.TempPathFactory,
) -> tuple[Path, Path, dict[str, Any]]:
    parent = tmp_path_factory.mktemp("preserved-canary-conformance")
    failed_root = _copy_preserved_root(parent)
    output_root = parent / f"{conformance.PRESERVED_ROOT_ID}-conformance"
    receipt = conformance.conform_preserved_raw_winner_canary(
        failed_root=failed_root,
        config_path=CONFIG_PATH,
        output_root=output_root,
    )
    return failed_root, output_root, receipt


def test_exact_preserved_root_reproduces_old_plan_and_verifies_128_bundles(
    conformed_receipt: tuple[Path, Path, dict[str, Any]],
) -> None:
    failed_root, output_root, receipt = conformed_receipt

    assert sorted(path.name for path in output_root.iterdir()) == [
        conformance.CONFORMANCE_RECEIPT_NAME
    ]
    assert (
        receipt["original_inventory"]["before_sha256"]
        == conformance.EXPECTED_INVENTORY_SHA256
    )
    assert receipt["original_inventory"]["before_after_identical"] is True
    assert (
        receipt["old_release_reconstruction"]["horizon_plan_receipt_sha256"]
        == conformance.OLD_HORIZON_PLAN_SHA256
    )
    assert (
        receipt["old_release_reconstruction"]["exact_frozen_receipt_reproduced"] is True
    )
    assert receipt["bundle_conformance"]["bundle_count_exact"] == 128
    assert (
        receipt["bundle_conformance"]["independent_v2_verification_count_exact"] == 128
    )
    assert len(set(receipt["bundle_conformance"]["bundle_sha256"])) == 128
    assert all(
        item["independent_v2_verified"] is True
        and item["reviewed_mapping_verified"] is True
        and item["old_plan_status"] == "PROCESS_REJECTED"
        and item["old_plan_reason_code"] == "SOURCE_BOUNDARY_SHORTFALL"
        for item in receipt["bundle_conformance"]["mapping"]
    )
    assert (
        receipt["reviewed_mapping_replay"]["reviewed_horizon_plan_receipt_sha256"]
        != conformance.OLD_HORIZON_PLAN_SHA256
    )
    assert receipt["safety"]["strategy_evaluator_calls_exact"] == 0
    assert receipt["safety"]["raw_winner_invoked"] is False
    assert receipt["safety"]["closes_named_raw_winner_gate"] is False

    reopened = conformance.verify_preserved_raw_winner_canary_conformance_receipt(
        receipt_path=output_root / conformance.CONFORMANCE_RECEIPT_NAME,
        failed_root=failed_root,
        config_path=CONFIG_PATH,
    )
    assert reopened == receipt


def test_coherently_resealed_safety_tamper_is_rejected(
    conformed_receipt: tuple[Path, Path, dict[str, Any]],
) -> None:
    failed_root, _, receipt = conformed_receipt
    tampered_root = failed_root.parent / "coherently-tampered-conformance"
    tampered_root.mkdir()
    tampered = json.loads(json.dumps(receipt))
    tampered["old_release_reconstruction"]["planner_release_sha256"] = (
        conformance.REVIEWED_PLANNER_RELEASE_SHA256
    )
    tampered["old_release_reconstruction"]["executor_release_sha256"] = (
        conformance.REVIEWED_EXECUTOR_RELEASE_SHA256
    )
    tampered["old_release_reconstruction"]["historical_adapter_release_sha256"] = (
        "1" * 64
    )
    tampered["reviewed_mapping_replay"]["reviewed_planner_release_sha256"] = (
        conformance.OLD_PLANNER_RELEASE_SHA256
    )
    tampered["reviewed_mapping_replay"]["reviewed_executor_release_sha256"] = (
        conformance.OLD_EXECUTOR_RELEASE_SHA256
    )
    tampered["reviewed_mapping_replay"]["reviewed_source_file_sha256"] = {
        "pm_authoritative_search_execution.py": "2" * 64,
        "pm_production_search_planner.py": "3" * 64,
    }
    tampered["reviewed_mapping_replay"]["reviewed_horizon_plan_receipt_sha256"] = (
        "4" * 64
    )
    mapping = tampered["bundle_conformance"]["mapping"]
    mapping[0]["bundle_sha256"] = "5" * 64
    mapping[0]["winner_process_sha256"] = "6" * 64
    tampered["bundle_conformance"]["bundle_sha256"][0] = "5" * 64
    tampered["bundle_conformance"]["bundle_roster_sha256"] = conformance._sha(
        tampered["bundle_conformance"]["bundle_sha256"]
    )
    tampered["frozen_provenance"]["original_failure_sha256"] = "7" * 64
    tampered["source_reopen"]["activation_id"] = "fabricated-activation"
    tampered["safety"]["full_bounded_canary_verifier_replaced"] = True
    tampered["safety"]["raw_winner_verified_claimed"] = True
    tampered["safety"]["economic_no_trade_or_no_winner_claimed"] = True
    body = {key: value for key, value in tampered.items() if key != "receipt_sha256"}
    tampered["receipt_sha256"] = conformance._sha(body)
    receipt_path = tampered_root / conformance.CONFORMANCE_RECEIPT_NAME
    receipt_path.write_bytes(conformance._canonical_bytes(tampered))

    with pytest.raises(
        conformance.RawWinnerCanaryConformanceError,
        match="CONFORMANCE_RECEIPT_BINDING_INVALID",
    ):
        conformance.verify_preserved_raw_winner_canary_conformance_receipt(
            receipt_path=receipt_path,
            failed_root=failed_root,
            config_path=CONFIG_PATH,
        )


def test_prefix_verifier_temp_files_are_outside_preserved_root(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    failed_root = _copy_preserved_root(tmp_path).resolve(strict=True)
    output_root = tmp_path / f"{conformance.PRESERVED_ROOT_ID}-scratch-conformance"
    original_temp_path = conformance.native_prefix._temp_path
    observed: list[Path] = []

    def traced_temp_path(parent: Path) -> Path:
        path = original_temp_path(parent)
        observed.append(path.resolve(strict=False))
        return path

    monkeypatch.setattr(conformance.native_prefix, "_temp_path", traced_temp_path)
    before = conformance.inventory_preserved_root(failed_root)
    conformance.conform_preserved_raw_winner_canary(
        failed_root=failed_root,
        config_path=CONFIG_PATH,
        output_root=output_root,
    )
    after = conformance.inventory_preserved_root(failed_root)

    assert observed
    assert all(
        path != failed_root and failed_root not in path.parents for path in observed
    )
    assert before == after


def test_interruption_after_receipt_replace_removes_partial_output(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    output_root = tmp_path / "interrupted-conformance"
    receipt_path = output_root / conformance.CONFORMANCE_RECEIPT_NAME
    original_read = conformance._read_regular

    def interrupt_final_reopen(path: Path, **kwargs: Any) -> bytes:
        if Path(path) == receipt_path and receipt_path.exists():
            raise KeyboardInterrupt
        return original_read(path, **kwargs)

    monkeypatch.setattr(conformance, "_read_regular", interrupt_final_reopen)
    with pytest.raises(KeyboardInterrupt):
        conformance._write_receipt(output_root, {"schema_version": "test"})

    assert not output_root.exists()


def test_preserved_root_byte_tamper_fails_before_output(tmp_path: Path) -> None:
    failed_root = _copy_preserved_root(tmp_path)
    output_root = tmp_path / f"{conformance.PRESERVED_ROOT_ID}-conformance"
    context_path = failed_root / "spool" / "generation-context.json"
    context_path.write_bytes(context_path.read_bytes() + b" ")

    with pytest.raises(
        conformance.RawWinnerCanaryConformanceError,
        match="PRESERVED_ROOT_INVENTORY_MISMATCH",
    ):
        conformance.conform_preserved_raw_winner_canary(
            failed_root=failed_root,
            config_path=CONFIG_PATH,
            output_root=output_root,
        )
    assert not output_root.exists()


def test_existing_output_collision_is_rejected_without_overwrite(
    tmp_path: Path,
) -> None:
    failed_root = _copy_preserved_root(tmp_path)
    output_root = tmp_path / f"{conformance.PRESERVED_ROOT_ID}-conformance"
    output_root.mkdir()
    sentinel = output_root / "sentinel.bin"
    sentinel.write_bytes(b"do-not-overwrite")

    with pytest.raises(
        conformance.RawWinnerCanaryConformanceError,
        match="OUTPUT_ROOT_NOT_FRESH_SIBLING",
    ):
        conformance.conform_preserved_raw_winner_canary(
            failed_root=failed_root,
            config_path=CONFIG_PATH,
            output_root=output_root,
        )
    assert sentinel.read_bytes() == b"do-not-overwrite"
    assert sorted(path.name for path in output_root.iterdir()) == ["sentinel.bin"]
