"""INVENTED one-slot nativeDEV resume; no native, model or financial proof."""
from __future__ import annotations

import copy
import hashlib
import json
from types import SimpleNamespace

import pm_authority_spool as spool
import pm_forward_oos_replay_producer as forward
import pm_historical_anchor_orchestration as orchestration
import pm_historical_scheduler as scheduler
import pm_historical_search_producer as producer
import pm_native_prefix_production as native
import pm_portfolio_replay as portfolio
import pm_spread_profile as spread

from test_development_cell_prefix import (
    prefix_metadata_factory,
    _prefix_native_issue,
    _prefix_native_scheduler_contract,
)


def test_native_development_last_commit_resume_refreshes_account_run_binding(
        tmp_path, monkeypatch, prefix_metadata_factory, record_property):
    """Real metadata issuers, saved commit and controller; market proofs are doubled."""
    record_property("fixture_provenance",
        "INVENTED_NATIVEDEV_LAST_COMMIT_RESUME_SOURCE_AND_ACCOUNT_BOUNDARIES_DOUBLED")
    env = prefix_metadata_factory()
    slot = env.roster["slots"][0]
    precommit = _prefix_native_issue(env, [slot["slot_id"]])
    contract = _prefix_native_scheduler_contract(env, 1)
    row = contract["cells"][0]
    row["root"] = str(env.context.root)
    contract, _ = scheduler._declared_development_contract(contract)
    row = contract["cells"][0]
    # This targets run publication/account preparation, not Grid retirement.
    row.pop("native_finalization")
    completion = {"status": "NO_TRADE", "slot_id": slot["slot_id"],
        "cell_id": env.cell["cell_id"], "request_sha256": "2" * 64,
        "completed_at_utc": slot["end_exclusive_utc"],
        "request_body": {"artifact_namespace": "invented/native-resume/one-slot"},
        "fixture_provenance": "INVENTED_COMPLETION_PROOF_STAND_IN_NOT_A_MODEL_RESULT"}
    commit = {"schema_version": producer.DECLARED_SLOT_COMMIT_SCHEMA,
        "precommit_sha256": precommit.receipt_sha256,
        "generation_context_sha256": env.context.context_sha256,
        "cell_id": env.cell["cell_id"], "slot_id": slot["slot_id"], "ordinal": 1,
        "previous_commit_sha256": None, "completion": completion, "artifact_roster": []}
    descriptor = spool.write_canonical_json_artifact(env.context, commit)
    pointer = producer._declared_commit_path(precommit, env.cell["cell_id"], 1)
    spool._publish_exact_json(env.context, pointer,
        {"schema_version": producer._DECLARED_SLOT_POINTER_SCHEMA,
         "commit_descriptor": descriptor}, env.context.limits.max_json_bytes)

    # Only the saved completion's upstream economic proof is an existing-style double.
    def verify_transport(context, *, completion: dict, expected_edition, process):
        assert context.context_sha256 == env.context.context_sha256
        assert process.receipt_sha256 == env.process.receipt_sha256
        assert completion == commit["completion"]
        assert expected_edition == orchestration.declared_slot_transport_edition(precommit.to_dict())

    def verify_completion(context, *, precommit: object, process, completion: dict, **kwargs):
        assert context.context_sha256 == env.context.context_sha256
        assert precommit.receipt_sha256 == commit["precommit_sha256"]
        assert process.receipt_sha256 == env.process.receipt_sha256
        assert completion == commit["completion"]
        return copy.deepcopy(completion)

    monkeypatch.setattr(producer, "_verify_declared_completion_transport", verify_transport)
    monkeypatch.setattr(producer.historical_adapter, "verify_declared_slot_completion", verify_completion)
    chain = {}
    progress = producer.reopen_declared_cell_owner_progress(env.context, precommit=precommit,
        process=env.process, cell_id=env.cell["cell_id"], frozen_pipeline_config=env.frozen,
        _verified_progress_out=chain)
    run_path = env.context.root / producer._declared_run_path(precommit, env.cell["cell_id"])
    assert progress.first_uncommitted_slot is None and not progress.terminal_stop
    assert progress.run_receipt is None and not run_path.exists()
    assert len(progress.slot_summaries) == 1
    saved = {relative: (env.context.root / relative).read_bytes() for relative in (
        pointer, descriptor["relative_path"], spool.CONTEXT_RELATIVE_PATH,
        "declared-slots/" + precommit.receipt_sha256 + "/precommit.json")}
    original_window = copy.deepcopy(precommit.to_dict()["source_access_window_utc"])
    prepared_run_hashes, produced = [], []
    original_prepare = forward._prepare_declared_native_development_cell_account_artifact

    def prepare(*args, **kwargs):
        prepared = original_prepare(*args, **kwargs)
        prepared_run_hashes.append(prepared[2]["saved_cell_run_receipt_sha256"])
        return prepared

    monkeypatch.setattr(forward, "_prepare_declared_native_development_cell_account_artifact", prepare)
    # Native precommit reopen consumes the fixture's documentary profile pin here.
    def profile(path):
        assert path == env.root / "data" / "native" / "terms" / "spread_profile.json"
        assert precommit.to_dict()["spread_profile_identity"]["profile_sha256"] == env.profile_sha
        return SimpleNamespace(sha256=env.profile_sha)

    monkeypatch.setattr(spread, "load_spread_profile", profile)
    monkeypatch.setattr(portfolio, "active_native_portfolio_contract", lambda *a, **k: ({}, []))
    monkeypatch.setattr(orchestration, "_declared_slot_native_snapshot_status", lambda *a: "MATCH")
    monkeypatch.setattr(producer, "_declared_slot_attempt",
        lambda *a, **k: (_ for _ in ()).throw(AssertionError("committed slot must not execute again")))

    def native_request(context, *, precommit, process, frozen_pipeline_config, inputs, execution_product=None):
        assert inputs["slot_schedule"][0]["status"] == "NO_TRADE"
        return {"symbol": env.cell["symbol"], "execution_timeframe": env.cell["timeframe"],
            "spread_source_timeframe": "M5", "cutoff_utc": slot["end_exclusive_utc"],
            "production_contract_required": True,
            "frozen_profile_path": str(env.root / "INVENTED_NO_PROFILE_READ.json")}

    # Explicit source/account boundaries: no bars, source product or engine is opened.
    monkeypatch.setattr(forward, "_declared_native_development_cell_account_request_from_inputs", native_request)
    monkeypatch.setattr(native, "produce_activated_native_prefix",
        lambda **kwargs: SimpleNamespace(fixture_provenance="INVENTED_NATIVE_PRODUCT_STAND_IN"))
    monkeypatch.setattr(native, "reverify_activated_native_prefix_product", lambda value, *a, **k: value)

    def produce(context, *, precommit, process, frozen_pipeline_config, inputs, body, **kwargs):
        run_body = json.loads(run_path.read_bytes())
        run_sha = scheduler._declared_run_descriptor(precommit, row, run_body)["sha256"]
        assert inputs["progress"].run_receipt == run_body
        assert body["saved_cell_run_receipt_sha256"] == run_sha
        assert body["progress_status"] == "DEVELOPMENT_COMPLETE"
        assert len(body["slot_summaries"]) == 1
        assert body["slot_summaries"][0]["commit_sha256"] == descriptor["sha256"]
        body["account"] = {"fixture_provenance": "INVENTED_ACCOUNT_NO_ENGINE_OR_NUMERICAL_EVIDENCE"}
        actual = spool.write_canonical_json_artifact(context, body)
        produced.append((copy.deepcopy(body), actual))
        return SimpleNamespace(descriptor=actual)

    monkeypatch.setattr(forward, "_produce_declared_native_development_cell_account_from_inputs", produce)
    result = scheduler._declared_run_one_cell(contract, row)
    record_property("actual_scheduler_result", json.dumps(result, sort_keys=True))
    assert result["status"] == "COMPLETED", json.dumps(result, sort_keys=True)
    assert result["recorded_before_dispatch"] is True
    assert len(prepared_run_hashes) == 2 and prepared_run_hashes[0] is None
    assert prepared_run_hashes[1] == result["run_receipt"]["sha256"]
    body, actual = produced[0]
    encoded = (env.context.root / actual["relative_path"]).read_bytes()
    assert json.loads(encoded) == body
    assert hashlib.sha256(encoded).hexdigest() == result["account_descriptor"]["sha256"]
    assert body["saved_cell_run_receipt_sha256"] == result["run_receipt"]["sha256"]
    assert len(result["period_facts"]) == 1
    assert result["period_facts"][0]["completion_status"] == "NO_TRADE"
    assert result["period_facts"][0]["commit_sha256"] == descriptor["sha256"]
    assert "period_fact_error" not in result
    assert precommit.to_dict()["source_access_window_utc"] == original_window
    assert {relative: (env.context.root / relative).read_bytes() for relative in saved} == saved
