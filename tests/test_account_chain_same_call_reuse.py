"""INVENTED one-slot transport/IO tests; no native/M5 origin, model or financial proof."""
from __future__ import annotations

import copy
from datetime import datetime, timedelta, timezone
import hashlib
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

import pm_authority_spool as spool
import pm_forward_oos_replay_producer as forward
import pm_historical_anchor_orchestration as historical
import pm_historical_scheduler as scheduler
import pm_historical_search_producer as producer
import pm_production_config_binding as binding


def _chain_fixture(tmp_path, monkeypatch, route):
    """Real saved commit and account writer; upstream admission is explicitly doubled.

    One STOP is saved, never executed. The ordinary native roster remains13
    metadata rows; development selects its first slot. No source is acquired.
    """
    context = spool.freeze_generation_context(tmp_path / "invented-chain",
        generation_id="INVENTED-same-call-chain", native_universe_sha256="a" * 64, config_sha256="b" * 64)
    development, source_kind = route != "native", "m5_derived" if route == "m5dev" else "broker_native"
    start = datetime(2020, 1, 1, tzinfo=timezone.utc)
    slots = [{"slot_id": "INVENTED-slot-" + str(ordinal), "ordinal": ordinal,
        "activation_utc": (start + timedelta(days=28 * (ordinal - 1))).isoformat(),
        "end_exclusive_utc": (start + timedelta(days=28 * ordinal)).isoformat()}
        for ordinal in range(1, 14)]
    cell = {"cell_id": "f" * 64, "symbol": "EURUSD", "timeframe": "H1", "regime": "TREND"}
    parent = {"cells": [cell], "slot_roster": {"slots": slots}, "source_kind": source_kind,
        "effective_config_sha256": "e" * 64, "development_scope_sha256": "d" * 64,
        "development_scope": {"selected_slot_ids": [slots[0]["slot_id"]]} if development else None}
    precommit = SimpleNamespace(receipt_sha256="c" * 64, to_dict=lambda: copy.deepcopy(parent))
    process = SimpleNamespace(receipt_sha256="1" * 64)
    config = forward.PipelineConfig(initial_capital=10000.0, discovery_risk_per_trade_pct=1.0,
                                    backtest_spread_mode="conservative_max", use_spread=True)
    counts = {"admission": 0, "config": 0, "completion": 0, "commit_payload": 0}

    class InventedFrozenConfig:
        active_config_path = tmp_path / "invented-config.json"
        config_sha256 = context.config_sha256
        effective_config_sha256 = parent["effective_config_sha256"]

        def verify_current(self, observed):
            assert observed is context

        def materialize(self, observed):
            assert observed is context
            counts["config"] += 1
            return config

    frozen = InventedFrozenConfig()
    frozen.active_config_path.write_bytes(b"{}")

    def admitted(observed, actual_precommit, *, process):
        assert observed is context and actual_precommit is precommit and process is env.process
        counts["admission"] += 1
        return copy.deepcopy(parent)

    def verified_completion(observed, *, completion, **kwargs):
        assert observed is context and completion == commit["completion"]
        counts["completion"] += 1
        return copy.deepcopy(completion)

    monkeypatch.setattr(binding, "FrozenProductionPipelineConfig", InventedFrozenConfig)
    monkeypatch.setattr(historical, "verify_declared_slot_owner_run_precommit", admitted)
    monkeypatch.setattr(historical, "declared_slot_source_kind", lambda value: value["source_kind"])
    monkeypatch.setattr(historical, "declared_slot_development_scope", lambda value: value["development_scope"])
    monkeypatch.setattr(historical, "declared_slot_selected_slots", lambda value:
        tuple(value["slot_roster"]["slots"][:1] if value["development_scope"] is not None
              else value["slot_roster"]["slots"]))
    monkeypatch.setattr(historical, "declared_slot_transport_edition", lambda value: historical.DECLARED_WINNER_TRANSPORT_V16)
    monkeypatch.setattr(producer, "_verify_declared_completion_transport", lambda *args, **kwargs: None)
    monkeypatch.setattr(producer.historical_adapter, "verify_declared_slot_completion", verified_completion)
    monkeypatch.setattr(producer, "_read_declared_m5_cell_source_context", lambda *args, **kwargs:
        ({"fixture_provenance": "INVENTED_SOURCE_ADMISSION_STAND_IN"}, None))
    completion = {"status": "STOP", "slot_id": slots[0]["slot_id"], "cell_id": cell["cell_id"],
        "request_sha256": "2" * 64, "completed_at_utc": "2020-01-01T00:00:00Z",
        "request_body": {"artifact_namespace": "invented/one-slot"},
        "fixture_provenance": "INVENTED_SAVED_STOP_NOT_AN_EXECUTED_PERIOD"}
    commit = {"schema_version": producer.DECLARED_SLOT_COMMIT_SCHEMA,
        "precommit_sha256": precommit.receipt_sha256, "generation_context_sha256": context.context_sha256,
        "cell_id": cell["cell_id"], "slot_id": slots[0]["slot_id"], "ordinal": 1,
        "previous_commit_sha256": None, "completion": completion, "artifact_roster": []}
    descriptor = spool.write_canonical_json_artifact(context, commit)
    pointer = producer._declared_commit_path(precommit, cell["cell_id"], 1)
    spool._publish_exact_json(context, pointer,
        {"schema_version": producer._DECLARED_SLOT_POINTER_SCHEMA, "commit_descriptor": descriptor},
        context.limits.max_json_bytes)
    account_io = {"relative": None, "reads": 0, "manifest": None, "manifest_reads": 0,
                  "enabled": False, "stream_depth": 0}
    original_stream, original_read_bytes = spool._stream_file, Path.read_bytes

    def stream(root, relative, **kwargs):
        if relative == descriptor["relative_path"]:
            counts["commit_payload"] += 1
        account = account_io["enabled"] and relative == account_io["relative"]
        if account:
            account_io["reads"] += 1
            account_io["stream_depth"] += 1
        if account_io["enabled"] and relative == account_io["manifest"]:
            account_io["manifest_reads"] += 1
        try:
            return original_stream(root, relative, **kwargs)
        finally:
            if account:
                account_io["stream_depth"] -= 1

    def read_bytes(path):
        if (account_io["enabled"] and account_io["stream_depth"] == 0
                and path.resolve() == (context.root / account_io["relative"]).resolve()):
            account_io["reads"] += 1
        return original_read_bytes(path)

    monkeypatch.setattr(spool, "_stream_file", stream)
    monkeypatch.setattr(Path, "read_bytes", read_bytes)
    functions = {
        "native": (forward._prepare_declared_cell_account_artifact,
            forward._produce_declared_cell_account_from_inputs, forward.discover_declared_cell_account_artifact),
        "nativedev": (forward._prepare_declared_native_development_cell_account_artifact,
            forward._produce_declared_native_development_cell_account_from_inputs,
            forward.discover_declared_native_development_cell_account_artifact),
        "m5dev": (forward._prepare_declared_m5_development_cell_account_artifact,
            forward._produce_declared_m5_development_cell_account_from_inputs,
            forward.discover_declared_m5_development_cell_account_artifact),
    }
    prepare, produce, discover = functions[route]
    env = SimpleNamespace(context=context, precommit=precommit, process=process, frozen=frozen,
        config=config, parent=parent, cell=cell, counts=counts, commit=commit, descriptor=descriptor,
        pointer=pointer, prepare=prepare, produce=produce, discover=discover, route=route, account_io=account_io)
    return env


def _args(env):
    return dict(precommit=env.precommit, process=env.process, frozen_pipeline_config=env.frozen)


def _progress(env):
    chain = {"stale_fixture_key": "MUST_NOT_SURVIVE_A_NEW_VERIFICATION"}
    progress, busy = scheduler._declared_progress_or_busy(env.context, env.precommit,
        env.process, env.frozen, env.cell["cell_id"], _verified_progress_out=chain)
    assert busy is False and chain["progress"] is progress and "stale_fixture_key" not in chain
    return chain


@pytest.mark.parametrize("route", ("native", "nativedev", "m5dev"))
def test_account_chain_reuses_verified_commit_and_preserves_saved_output(tmp_path, monkeypatch, route):
    env = _chain_fixture(tmp_path, monkeypatch, route)
    saved = {relative: (env.context.root / relative).read_bytes()
             for relative in (env.pointer, env.descriptor["relative_path"])}
    chain = _progress(env)
    assert env.counts == {"admission": 1, "config": 1, "completion": 1, "commit_payload": 1}
    existing, inputs, body = env.prepare(env.context, **_args(env), _verified_chain=chain)
    assert existing is None and inputs["progress"] is chain["progress"]
    assert inputs["commits"] == [env.commit] and body["account"] is None and body["progress_status"] == "STOP"
    assert env.counts == {"admission": 1, "config": 1, "completion": 1, "commit_payload": 1}
    for defect in ("process", "source-kind", "commit-binding"):
        broken = dict(chain)
        if defect == "process":
            broken["process"] = object()
        elif defect == "source-kind":
            broken["parent"] = {**chain["parent"], "source_kind": "INVENTED_WRONG_SOURCE_KIND"}
        else:
            broken["commits"] = copy.deepcopy(chain["commits"])
            broken["commits"][env.commit["slot_id"]][0]["cell_id"] = "0" * 64
        with pytest.raises(forward.ForwardOOSReplayProductionError):
            env.prepare(env.context, **_args(env), _verified_chain=broken)
    expected = copy.deepcopy(body)
    artifact = env.produce(env.context, **_args(env), inputs=inputs, body=body)
    encoded = (env.context.root / artifact.descriptor["relative_path"]).read_bytes()
    assert json.loads(encoded) == expected and artifact.descriptor["sha256"] == hashlib.sha256(encoded).hexdigest()
    assert env.counts == {"admission": 1, "config": 1, "completion": 1, "commit_payload": 1}
    env.account_io["relative"] = artifact.descriptor["relative_path"]
    transport_symbol = {"native": forward._declared_cell_transport_symbol,
        "nativedev": forward._declared_native_development_cell_transport_symbol,
        "m5dev": forward._declared_development_cell_transport_symbol}[route]
    env.account_io["manifest"] = spool._manifest_relative_path(
        transport_symbol(expected), spool.GENERAL_MANIFEST_DOMAIN)
    env.account_io["enabled"] = True
    reopened, reopened_inputs, reopened_body = env.prepare(env.context, **_args(env))
    assert reopened.descriptor == artifact.descriptor and reopened_body == expected
    assert env.account_io["reads"] == env.account_io["manifest_reads"] == 1
    assert env.counts == {"admission": 2, "config": 2, "completion": 2, "commit_payload": 2}
    if route != "m5dev":
        result, reused = scheduler._declared_account(env.context, env.precommit, env.process,
            env.frozen, env.config, _prepared_account=(reopened, reopened_inputs, reopened_body))
        assert reused is True and result.descriptor == artifact.descriptor
        assert env.account_io["reads"] == env.account_io["manifest_reads"] == 1
        assert env.counts == {"admission": 2, "config": 2, "completion": 2, "commit_payload": 2}
    env.account_io["enabled"] = False
    assert (env.context.root / artifact.descriptor["relative_path"]).read_bytes() == encoded
    assert {relative: (env.context.root / relative).read_bytes() for relative in saved} == saved


@pytest.mark.parametrize("route", ("native", "nativedev", "m5dev"))
def test_new_cold_account_chain_refuses_changed_commit_bytes(tmp_path, monkeypatch, route):
    env = _chain_fixture(tmp_path, monkeypatch, route)
    existing, inputs, body = env.prepare(env.context, **_args(env))
    assert existing is None
    artifact = env.produce(env.context, **_args(env), inputs=inputs, body=body)
    account_path = env.context.root / artifact.descriptor["relative_path"]
    account_bytes = account_path.read_bytes()
    path = env.context.root / env.descriptor["relative_path"]
    damaged = bytearray(path.read_bytes())
    damaged[-2] ^= 1
    path.write_bytes(damaged)
    with pytest.raises(forward.ForwardOOSReplayProductionError) as caught:
        env.discover(env.context, **_args(env))
    assert caught.value.code == "DECLARED_CELL_OWNER_PROGRESS_INVALID"
    assert env.counts["admission"] == 2 and env.counts["commit_payload"] == 2
    assert account_path.read_bytes() == account_bytes


@pytest.mark.parametrize("m5", (False, True), ids=("native", "m5"))
def test_saved_raw_completion_registers_only_already_verified_metadata(tmp_path, monkeypatch, m5):
    """Real payload/registry; original completion authentication is explicitly doubled."""
    context = spool.freeze_generation_context(tmp_path / "invented-raw-completion",
        generation_id="INVENTED-saved-registration", native_universe_sha256="a" * 64, config_sha256="b" * 64)
    payload = {"fixture_provenance": "INVENTED_SAVED_PAYLOAD_NO_RAW_OR_OWNER_AUTHORITY"}
    descriptor = spool.write_canonical_json_artifact(context, payload)
    entry = {"artifact_id": "invented/one-slot/evidence", "artifact_type": "invented_metadata", "descriptor": descriptor}
    sink = spool.open_symbol_sink(context, "EURUSD")
    body = {"schema_version": (historical.DECLARED_M5_DEVELOPMENT_SLOT_REQUEST_SCHEMA if m5
                               else historical.DECLARED_SLOT_REQUEST_SCHEMA_V2),
        "cell_id": "f" * 64, "slot_id": "INVENTED-slot-1", "attempt_id": "INVENTED-attempt-1",
        "precommit_sha256": "c" * 64, "symbol": "EURUSD", "artifact_namespace": "invented/one-slot"}
    request = SimpleNamespace(receipt_sha256="d" * 64, _precommit=object(),
        to_dict=lambda: copy.deepcopy(body), symbol_sink=lambda symbol: sink)
    completion = {"request_body": body, "status": "STOP", "fixture_provenance": "INVENTED_SEMANTIC_STAND_IN"}
    saved = {"schema_version": "fxpm.declared-slot-raw-completion-unit.v1",
        "request_sha256": request.receipt_sha256, "completion": completion, "artifact_roster": [entry]}
    relative = producer.search_execution._declared_unit_relative(body, "raw-completion.json")
    spool._publish_exact_json(context, relative, saved, context.limits.max_json_bytes)
    calls, original_verify = [], spool._verify_artifact_file

    def verify(observed, actual):
        calls.append(dict(actual))
        return original_verify(observed, actual)

    def verified_completion(observed, **kwargs):
        assert kwargs["completion"] == completion and kwargs["artifact_roster"] == [entry]
        if m5:
            assert kwargs["m5_owner"] is owner and kwargs["m5_cost_edition"] is costs
        spool._verify_artifact_file(observed, descriptor)
        return completion

    owner, costs = object(), object()
    monkeypatch.setattr(spool, "_verify_artifact_file", verify)
    monkeypatch.setattr(producer.historical_adapter, "verify_declared_slot_completion", verified_completion)
    monkeypatch.setattr(producer, "_declared_m5_reuse_arguments", lambda **kwargs: m5)
    kwargs = {"m5_owner": owner, "cost_edition": costs} if m5 else {}
    encoded = (context.root / descriptor["relative_path"]).read_bytes()
    assert producer._declared_saved_raw_completion(context, request=request, process=object(),
        frozen_pipeline_config=object(), **kwargs) == completion
    assert calls == [descriptor] and sink.artifact_roster() == [entry]
    assert (context.root / descriptor["relative_path"]).read_bytes() == encoded
    damaged = bytearray(encoded)
    damaged[-2] ^= 1
    (context.root / descriptor["relative_path"]).write_bytes(damaged)
    with pytest.raises(spool.AuthoritySpoolError) as caught:
        producer._declared_saved_raw_completion(context, request=request, process=object(),
            frozen_pipeline_config=object(), **kwargs)
    assert str(caught.value) == "ARTIFACT_INTEGRITY_MISMATCH"
    assert len(calls) == 2 and sink.artifact_roster() == [entry]



def test_symbol_verification_reuses_only_its_verified_commit_object(tmp_path, monkeypatch):
    """Actual Spool verification/pointer reuse; no blanket artifact-body collection."""
    env = _chain_fixture(tmp_path, monkeypatch, "native")
    sink = spool.open_symbol_sink(env.context, "EURUSD")
    sink.register_artifact("declared-slot-commit/01", "declared_slot_commit", env.descriptor)
    opaque = sink.put_opaque("invented/unrelated", "invented_opaque",
                             [b"INVENTED_UNRELATED_OPAQUE" * 8], format="binary")
    receipt = sink.seal()
    env.counts["commit_payload"] = 0
    stream_calls, original_stream = [], spool._stream_file

    def stream(root, relative, **kwargs):
        stream_calls.append((relative, bool(kwargs.get("collect"))))
        return original_stream(root, relative, **kwargs)

    monkeypatch.setattr(spool, "_stream_file", stream)
    arguments = dict(expected_generation_id=env.context.generation_id, expected_symbol="EURUSD",
        expected_native_universe_sha256=env.context.native_universe_sha256,
        expected_config_sha256=env.context.config_sha256)
    verified = {}
    manifest = spool.verify_symbol_receipt(env.context.root, receipt,
        **arguments, _verified_commit_objects=verified)
    key = tuple(env.descriptor[name] for name in ("relative_path", "sha256", "size_bytes", "format"))
    assert set(verified) == {key}
    assert verified[key] == (env.commit, env.descriptor["sha256"], env.descriptor)
    assert env.counts["commit_payload"] == 1
    assert producer._read_declared_commit(env.context, env.pointer,
        _verified_objects=verified) == verified[key]
    assert env.counts["commit_payload"] == 1
    opaque_reads = [collect for relative, collect in stream_calls
                    if relative == opaque["descriptor"]["relative_path"]]
    assert opaque_reads and not any(opaque_reads)
    assert spool.verify_symbol_receipt(env.context.root, receipt, **arguments) == manifest
    assert env.counts["commit_payload"] == 2
    encoded = bytearray((env.context.root / env.descriptor["relative_path"]).read_bytes())
    encoded[-2] ^= 1
    (env.context.root / env.descriptor["relative_path"]).write_bytes(encoded)
    fresh = {}
    with pytest.raises(spool.AuthoritySpoolError) as caught:
        spool.verify_symbol_receipt(env.context.root, receipt,
            **arguments, _verified_commit_objects=fresh)
    assert str(caught.value) == "ARTIFACT_INTEGRITY_MISMATCH"
    assert fresh == {} and env.counts["commit_payload"] == 3
