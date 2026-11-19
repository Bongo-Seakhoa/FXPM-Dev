"""Fresh-process verification of the current full-panel post-freeze STOP capsule.  Software only.

Final location: tests/fxpm_current_stop_cold_reopen.py (lane 271 apply map).

The saved process is reopened from its own receipt and verified to be the full-panel edition.  Nothing is frozen,
issued or reissued here: no process, precommit, request or capture.
"""

import argparse
from datetime import datetime, timezone
import hashlib
import importlib.machinery
import json
import os
from pathlib import Path
import sys
import types


ROOT = Path(os.environ["FXPM_QA_SOURCE_IMAGE_ROOT"]).resolve(strict=True)
AUTHORITATIVE_CHECKOUT = Path(os.environ["FXPM_QA_AUTHORITATIVE_CHECKOUT"]).resolve(strict=True)
CLASSIFICATION = "INVENTED_CURRENT_FULL_PANEL_POSTFREEZE_STOP_SOFTWARE_ONLY"


def identity(path):
    raw = Path(path).read_bytes()
    return {"sha256": hashlib.sha256(raw).hexdigest(), "bytes": len(raw)}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--capsule", type=Path, required=True)
    parser.add_argument("--capsule-sha256", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    assert sys.flags.dont_write_bytecode and not sys.flags.optimize
    assert not any(name.startswith("pm_") for name in sys.modules), "FRESH_PROCESS_REQUIRED"
    capsule_path = args.capsule.resolve(strict=True)
    assert identity(capsule_path)["sha256"] == args.capsule_sha256, "COLD_CAPSULE_CHANGED"
    capsule = json.loads(capsule_path.read_text(encoding="utf-8"))
    assert capsule["classification"] == CLASSIFICATION
    assert Path(capsule["source_image_root"]).resolve(strict=True) == ROOT, "COLD_SOURCE_IMAGE_CHANGED"
    assert Path(capsule["authoritative_checkout"]).resolve(strict=True) == AUTHORITATIVE_CHECKOUT, "COLD_AUTHORITATIVE_CHECKOUT_CHANGED"
    for name, expected in capsule["source_identity"].items():
        assert identity(ROOT / name)["sha256"] == expected, "COLD_SOURCE_CHANGED"
    root = Path(capsule["generation_root"]).resolve(strict=True)
    assert root.is_relative_to(capsule_path.parent), "OWNED_INVENTED_SPOOL_REQUIRED"
    config_path = Path(capsule["config_path"]).resolve(strict=True)
    assert config_path.is_relative_to(capsule_path.parent), "OWNED_INVENTED_CONFIG_REQUIRED"
    assert identity(config_path) == capsule["config_file_identity"], "COLD_CONFIG_CHANGED"
    issued = {relative: identity(root / relative) for relative in capsule["issued_files"]}
    assert issued == capsule["issued_files"], "COLD_ISSUED_BYTES_CHANGED"
    os.environ["FXPM_QA_SOURCE_IMAGE_ROOT"] = str(ROOT)
    os.environ["FXPM_QA_AUTHORITATIVE_CHECKOUT"] = str(AUTHORITATIVE_CHECKOUT)
    first_paths = [str(ROOT), str(ROOT / "tests"),
                   str(AUTHORITATIVE_CHECKOUT), str(AUTHORITATIVE_CHECKOUT / "tests")]
    sys.path[:] = first_paths + [entry for entry in sys.path if entry not in first_paths]
    namespace = types.ModuleType("tests")
    namespace.__path__ = [str(ROOT / "tests"), str(AUTHORITATIVE_CHECKOUT / "tests")]
    namespace.__spec__ = importlib.machinery.ModuleSpec("tests", loader=None, is_package=True)
    namespace.__spec__.submodule_search_locations = namespace.__path__
    sys.modules["tests"] = namespace
    import pm_authority_spool as spool
    import pm_historical_anchor_orchestration as orchestration
    import pm_historical_search_producer as producer
    import pm_production_config_binding as binding
    import pm_production_search_planner as planner

    context = spool.reopen_generation_context(root,
        expected_generation_id=capsule["generation_id"],
        expected_native_universe_sha256=capsule["native_universe_sha256"],
        expected_config_sha256=capsule["config_sha256"])
    saved_process = spool.verify_symbol_receipt(root, capsule["process_spool_receipt"],
        expected_generation_id=context.generation_id,
        expected_symbol=planner.PROCESS_SPOOL_SYMBOL,
        expected_native_universe_sha256=context.native_universe_sha256,
        expected_config_sha256=context.config_sha256)
    assert len(saved_process["artifacts"]) == 1
    assert saved_process["artifacts"][0]["descriptor"]["sha256"] == capsule["process_receipt_sha256"]
    assert saved_process["artifacts"][0]["artifact_id"] == planner.PROCESS_ARTIFACT_ID
    process_paths = {
        saved_process["artifacts"][0]["descriptor"]["relative_path"],
        capsule["process_spool_receipt"]["symbol_manifest"]["relative_path"],
    }
    assert process_paths.issubset(issued), "SAVED_PROCESS_FILES_MUST_BE_BOUND"
    declared_before = {path.relative_to(root).as_posix(): identity(path)
        for path in (root / "declared-slots").rglob("*") if path.is_file()}
    payload, effective = binding.load_effective_pipeline_config(config_path)
    frozen = binding.freeze_production_pipeline_config(context, active_config_path=config_path,
        config_payload=payload, supplied_config=effective)
    assert frozen.effective_config_sha256 == capsule["effective_config_sha256"]
    assert hashlib.sha256(frozen._snapshot_json).hexdigest() == capsule["effective_config_sha256"]
    # Reopen the saved process from its own receipt; never freeze or reissue one.
    process = planner.reopen_production_search_process(context,
        spool_receipt=capsule["process_spool_receipt"],
        expected_receipt_sha256=capsule["process_receipt_sha256"])
    assert process.receipt_sha256 == capsule["process_receipt_sha256"]
    assert process.to_dict()["schema_version"] == capsule["process_schema_version"]
    assert process.to_dict()["schema_version"] == planner.ROUTE_A_FULL_PANEL_PROCESS_SCHEMA
    assert process.to_dict()["run_id"] == capsule["run_id"]
    assert (planner.frozen_search_execution_edition(context, process)
        == planner.ROUTE_A_FULL_PANEL_EXECUTION_EDITION), "COLD_PROCESS_IS_NOT_FULL_PANEL"
    precommit = orchestration.reopen_declared_slot_owner_run_precommit(context,
        receipt_sha256=capsule["precommit_sha256"], process=process, frozen_pipeline_config=frozen)
    body = precommit.to_dict()
    assert body["launch_activation_utc"] == capsule["launch_activation_utc"]
    assert body["protected_end_exclusive_utc"] == capsule["protected_end_exclusive_utc"]
    progress = producer.reopen_declared_cell_owner_progress(context, precommit=precommit,
        process=process, cell_id=capsule["cell_id"], frozen_pipeline_config=frozen)
    observed = [row["status"] for row in progress.slot_summaries]
    assert observed == capsule["expected_statuses"] == ["STOP"]
    commits, compact_roster = [], []
    for row in progress.slot_summaries:
        relative = producer._declared_commit_path(precommit, capsule["cell_id"], row["ordinal"])
        saved = identity(root / relative)
        assert saved == capsule["commit_files"][relative]
        committed, digest, descriptor = producer._read_declared_commit(context, relative)
        assert descriptor == capsule["commit_descriptors"][row["ordinal"] - 1]
        assert digest == row["commit_sha256"] == descriptor["sha256"]
        compact_roster.append({"artifact_id": f"declared-slot-commit/{row['ordinal']:02d}",
            "artifact_type": "declared_slot_commit", "descriptor": descriptor})
        commits.append({"ordinal": row["ordinal"], "slot_id": row["slot_id"],
            "status": row["status"], "reason_code": committed["completion"]["reason_code"],
            "pointer_identity": saved, "commit_descriptor": descriptor})
        del committed
    assert list(progress.slot_commit_artifacts) == compact_roster
    assert progress.terminal_stop is capsule["terminal_stop"]
    assert capsule["finalized"] is False
    assert progress.symbol_receipt is None and progress.run_receipt is None
    assert {relative: identity(root / relative) for relative in issued} == issued
    assert {path.relative_to(root).as_posix(): identity(path)
        for path in (root / "declared-slots").rglob("*") if path.is_file()} == declared_before
    result = {"schema_version": "fxpm.declared-slot-cold-reopen-receipt.v1",
        "classification": capsule["classification"], "pid": os.getpid(),
        "completed_at_utc": datetime.now(timezone.utc).isoformat(),
        "precommit_sha256": precommit.receipt_sha256, "process_receipt_sha256": process.receipt_sha256,
        "process_schema_version": process.to_dict()["schema_version"],
        "saved_process_reopened_not_reissued": True,
        "all_commits_reverified_from_disk": True, "request_restore_mode": "live=False",
        "commits": commits, "terminal_stop": progress.terminal_stop,
        "compact_commit_roster_verified": True, "finalized": False,
        "actual_final_manifest_and_run_receipt_verified": False,
        "original_issued_files_unchanged": True, "qualification_authority": False,
        "no_new_precommit_request_or_capture_issued": True,
        "capital_authority": False, "live_authority": False, "order_authority": False}
    with args.output.open("x", encoding="utf-8") as stream:
        json.dump(result, stream, allow_nan=False, sort_keys=True, indent=2)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
