"""One unseamed declared OWNER commit SOFTWARE witness, proposed and unrun.

SYNTHETIC_SOFTWARE_WITNESS_NOT_QUALIFICATION. The companion builder declares
one invented market, the canonical 300-attempt process, all 100 x 6 synthetic
series, actual owned native auditing and the public immutable stress-terms
input recipe. No observed financial inputs or retained genuine case are used.
An unchanged selector abstention fails this witness; it never triggers a new
market, seed, threshold or attempt. This source grants no execution authority.

The reopen below is an actual disk/context/process/precommit reopen in this
same test process; it is not a new-interpreter or genuine cold-process proof.
Only the first of the full declared slot roster is executed and committed.
"""
from __future__ import annotations

import copy
import inspect
import json
import os
from pathlib import Path

import pytest

import pm_authority_spool as spool
import pm_forward_oos_replay_producer as forward
import pm_historical_anchor_orchestration as orchestration
import pm_historical_production_adapter as adapter
import pm_historical_search_producer as producer
import pm_production_search_planner as planner
import pm_winner_raw_evidence as raw
import pm_winner_selection as winner

from declared_bound_owner_market import (
    PREDECLARATION,
    assert_planned_geometry,
    assert_source_count_geometry,
    build_owned_market,
)


LABEL = "SYNTHETIC_SOFTWARE_WITNESS_NOT_QUALIFICATION"
RUN_ENV = "FXPM_RUN_DECLARED_BOUND_OWNER_WITNESS"
ATTEMPT_ID = "attempt-11111111111111111111111111111111"
COMMIT_MAX_BYTES = 64 * 1024 * 1024
COMMIT_MAX_NODES = 2_000_000
AUTHORITY_FIELDS = (
    "qualification_authority", "capital_authority", "live_authority", "order_authority",
)


def _assert_module_originals(originals):
    # Captured module functions must retain their original code/global origins
    # and identities at every tested production boundary. No validator seam is
    # replaced. The one legacy-reader negative guard below is separate.
    for module, name, original in originals:
        assert getattr(module, name) is original, name
        assert original.__globals__ is vars(module), name
        assert original.__module__ == module.__name__, name
        assert Path(inspect.getsourcefile(original)).resolve() == Path(module.__file__).resolve(), name


def _assert_no_authority(completion):
    for field in AUTHORITY_FIELDS:
        assert completion[field] is False, field


def _assert_commit_bounds(body, descriptor):
    # These are bounds on the committed JSON only. They make no claim about
    # aggregate fixture/temp/storage, RSS, process time or native audit I/O.
    encoded = json.dumps(body, allow_nan=False, ensure_ascii=False,
                         sort_keys=True, separators=(",", ":")).encode("utf-8")
    assert len(encoded) == descriptor["size_bytes"]
    assert 0 < len(encoded) <= COMMIT_MAX_BYTES
    pending = [body]
    nodes = 0
    while pending:
        value = pending.pop()
        nodes += 1
        assert nodes <= COMMIT_MAX_NODES
        if type(value) is dict:
            pending.extend(value.keys())
            pending.extend(value.values())
        elif type(value) is list:
            pending.extend(value)
    return nodes


def _restore_actual_bound_plan(context, completion, artifact_roster):
    # Reuse the actual published frozen entries and original typed restorers;
    # no manufactured/sealed fitted object or alternate Winner is installed.
    body = completion["request_body"]
    prefix = (body["artifact_namespace"] + "/winner-search/" +
              body["timeframe"] + "/" + body["regime"] + "/frozen/")
    entries = sorted((entry for entry in artifact_roster
                      if entry["artifact_id"].startswith(prefix)),
                     key=lambda entry: entry["artifact_id"])
    assert entries
    receipt = next(entry for entry in entries if entry["artifact_id"] == prefix + "receipt")
    assert receipt["descriptor"] == completion["frozen_plan_receipt"]
    bodies = [adapter._reopen_worker_descriptor_bytes(
        context, entry["descriptor"], maximum_bytes=winner.MAX_REQUEST_BYTES,
        expected_format=raw.ASCII_JSON_FORMAT, code="SOFTWARE_WITNESS_FROZEN_REOPEN_FAILED")
        for entry in entries]
    receipt_body = json.loads(bodies[entries.index(receipt)])
    restorers = {
        raw.BOUND_FIT_V15_FROZEN_PLAN_SCHEMA: raw.restore_frozen_winner_search_plan_v6,
        raw.BOUND_FIT_V16_FROZEN_PLAN_SCHEMA: raw.restore_frozen_winner_search_plan_v7,
    }
    assert receipt_body["schema_version"] in restorers
    frozen = restorers[receipt_body["schema_version"]](
        context_identity={"generation_id": context.generation_id,
                          "generation_context_sha256": context.context_sha256,
                          "native_universe_sha256": context.native_universe_sha256,
                          "config_sha256": context.config_sha256},
        symbol=body["symbol"], timeframe=body["timeframe"], regime=body["regime"],
        artifact_namespace=body["artifact_namespace"], manifest_entries=entries,
        body_bytes=bodies)
    assert frozen.bound_fitted_state_entry is not None
    assert len(frozen.attempt_ledger["attempts"]) == 300
    assert_planned_geometry(frozen)
    return frozen


@pytest.mark.skipif(
    os.environ.get(RUN_ENV) != "1",
    reason="Heavy owned-synthetic 300-attempt witness; set " + RUN_ENV + "=1 to opt in",
)
def test_unseamed_bound_declared_owner_commit_and_disk_reopen(tmp_path, monkeypatch):
    """One predeclared actual chain; abstention is unexercised OWNER evidence."""
    assert PREDECLARATION["label"] == LABEL
    originals = tuple((module, name, getattr(module, name)) for module, name in (
        (adapter, "verify_declared_slot_completion"),
        (adapter, "_declared_owner_from_verified_winner"),
        (producer, "_commit_declared_slot_completion"),
        (producer, "reopen_declared_cell_owner_progress"),
    ))
    _assert_module_originals(originals)

    def forbidden_legacy_selected_artifacts(*args, **kwargs):
        pytest.fail("Legacy forward _selected_artifacts was called by bound OWNER witness")

    # A fail-only tripwire is the sole product monkeypatch. It supplies neither
    # artifacts nor verification success and does not add Book dispatch.
    monkeypatch.setattr(forward, "_selected_artifacts", forbidden_legacy_selected_artifacts)
    env = build_owned_market(tmp_path)
    context, process = env.context, env.process
    precommit, frozen_config = env.precommit, env.frozen_pipeline_config
    parent = orchestration.verify_declared_slot_owner_run_precommit(
        context, precommit, process=process)
    assert len(parent["cells"]) == 1
    assert parent["cells"][0] == env.cell
    slots = parent["slot_roster"]["slots"]
    assert len(slots) == 13
    slot = slots[0]
    assert slot == env.slot and slot["ordinal"] == 1
    cell = env.cell
    assert (cell["symbol"], cell["timeframe"], cell["regime"]) == ("EURUSD", "H4", "TREND")
    assert frozen_config.materialize(context).discovery_risk_per_trade_pct == 1.0
    sink = spool.open_symbol_sink(context, cell["symbol"])
    request = orchestration.declared_slot_producer_request(
        context, precommit=precommit, process=process, cell_id=cell["cell_id"],
        slot_id=slot["slot_id"], attempt_id=ATTEMPT_ID, sink=sink)
    try:
        _assert_module_originals(originals)
        completion = producer.execute_declared_slot_owner_search(
            context, request=request, process=process,
            frozen_pipeline_config=frozen_config)
        # STOP/NO_TRADE is preserved as the actual unchanged product outcome.
        # It fails this positive witness without any tuning, retry or commit.
        assert completion["status"] == "OWNER", (
            "PREDECLARED_SOFTWARE_WITNESS_ABSTAINED_OR_FAILED", completion["status"],
            completion.get("reason_code"))
        _assert_no_authority(completion)
        namespace = request.to_dict()["artifact_namespace"] + "/"
        roster = [dict(sink._artifacts[key]) for key in sorted(sink._artifacts)
                  if key.startswith(namespace)]
        _assert_module_originals(originals)
        verified = adapter.verify_declared_slot_completion(
            context, precommit=precommit, process=process, completion=completion,
            artifact_roster=roster, frozen_pipeline_config=frozen_config)
        assert verified == completion
        _assert_no_authority(verified)
        body = completion["request_body"]
        wr, wd = adapter._rebuild_winner_pair(context, {
            "request": completion["winner_request_entry"]["descriptor"],
            "request_sha256": completion["winner_request_sha256"],
            "decision": completion["winner_decision_entry"]["descriptor"],
            "decision_sha256": completion["winner_decision_sha256"],
        }, symbol=body["symbol"], timeframe=body["timeframe"], regime=body["regime"])
        assert winner.verify_historical_cell_winner_decision(wd, wr) is True
        assert wd.to_dict()["winner"] is not None
        frozen = _restore_actual_bound_plan(context, completion, roster)
        cross_bytes = adapter._reopen_worker_descriptor_bytes(
            context, completion["horizon_winner_cross_binding"],
            maximum_bytes=winner.MAX_REQUEST_BYTES,
            expected_format=raw.ASCII_JSON_FORMAT,
            code="SOFTWARE_WITNESS_CROSS_REOPEN_FAILED")
        cross, _, _ = raw.verify_historical_frozen_winner_cross_binding(
            cross_bytes, frozen=frozen)
        assert_source_count_geometry(cross)
        source = adapter._rebuild_source_decision(completion["source_group"])
        fit = adapter._rebuild_regime_fit_decision(completion["regime_fit"], source)
        _assert_module_originals(originals)
        expected_owner = adapter._declared_owner_from_verified_winner(
            body, wr, wd, frozen, fit.to_dict())
        assert type(expected_owner) is dict
        assert completion["owner"] == expected_owner

        # Mutate one owner field only; all authentic artifact and request bytes
        # remain untouched. Demand the original binding refusal, not any error.
        changed = copy.deepcopy(completion)
        owner_id = changed["owner"]["owner_id"]
        changed["owner"]["owner_id"] = owner_id[:-1] + ("1" if owner_id[-1] != "1" else "0")
        assert changed["owner"] != expected_owner
        _assert_module_originals(originals)
        with pytest.raises(adapter.HistoricalProductionAdapterError) as rejected:
            adapter.verify_declared_slot_completion(
                context, precommit=precommit, process=process, completion=changed,
                artifact_roster=roster, frozen_pipeline_config=frozen_config)
        assert str(rejected.value) == "DECLARED_SLOT_OWNER_BINDING_MISMATCH"

        _assert_module_originals(originals)
        summary, descriptor = producer._commit_declared_slot_completion(
            context, precommit=precommit, process=process, cell_id=cell["cell_id"],
            slot=slot, completion=completion, artifact_roster=roster,
            previous_commit_sha256=None, frozen_pipeline_config=frozen_config)
        committed = producer._read_declared_commit(context,
            producer._declared_commit_path(precommit, cell["cell_id"], 1))
        assert committed is not None
        commit_body, commit_sha, actual_descriptor = committed
        assert descriptor == actual_descriptor and descriptor["sha256"] == commit_sha
        assert commit_body["completion"]["status"] == "OWNER"
        assert commit_body["completion"]["owner"] == expected_owner
        assert summary == producer._declared_slot_summary(commit_body, commit_sha)
        _assert_no_authority(commit_body["completion"])
        _assert_commit_bounds(commit_body, actual_descriptor)
    finally:
        orchestration._release_declared_slot_producer_request(request, context)

    # Drop the live request capability and reopen the immutable disk products
    # through original APIs; the fitted/Winner runtime object is not supplied.
    cold_context = spool.reopen_generation_context(
        context.root, expected_generation_id=context.generation_id,
        expected_native_universe_sha256=context.native_universe_sha256,
        expected_config_sha256=context.config_sha256)
    assert cold_context is not context and cold_context.context_sha256 == context.context_sha256
    cold_process = planner.reopen_production_search_process(
        cold_context, spool_receipt=process.spool_receipt,
        expected_receipt_sha256=process.receipt_sha256)
    cold_precommit = orchestration.reopen_declared_slot_owner_run_precommit(
        cold_context, receipt_sha256=precommit.receipt_sha256, process=cold_process,
        frozen_pipeline_config=frozen_config)
    _assert_module_originals(originals)
    progress = producer.reopen_declared_cell_owner_progress(
        cold_context, precommit=cold_precommit, process=cold_process,
        cell_id=cell["cell_id"], frozen_pipeline_config=frozen_config)
    assert list(progress.slot_summaries) == [summary]
    assert len(progress.slot_commit_artifacts) == 1
    assert progress.last_commit_sha256 == commit_sha
    assert progress.first_uncommitted_slot == slots[1]
    assert progress.terminal_stop is False and progress.terminal_failure is None
    assert progress.symbol_receipt is None and progress.run_receipt is None
    cold_commit = producer._read_declared_commit(cold_context,
        producer._declared_commit_path(cold_precommit, cell["cell_id"], 1))
    assert cold_commit is not None
    assert cold_commit[1:] == committed[1:]
    assert cold_commit[0] == commit_body
    assert cold_commit[0]["completion"]["owner"] == expected_owner
    _assert_no_authority(cold_commit[0]["completion"])
    assert producer._read_declared_commit(cold_context,
        producer._declared_commit_path(cold_precommit, cell["cell_id"], 2)) is None
    _assert_module_originals(originals)
