"""Synthetic publication/reopen boundaries; no authenticated OWNER or profit claim."""

import json
from pathlib import Path
from types import SimpleNamespace

import pytest

import pm_authority_spool as spool
import pm_causal_evidence_producer as causal
import pm_forward_oos_replay_producer as forward
from tests.test_declared_cell_account_transport import (
    synthetic_declared_transport,
    synthetic_declared_complete_inputs,
)


def _args(env):
    return dict(precommit=env.precommit, process=env.process, frozen_pipeline_config=env.frozen)


def _manifest(env, expected=None):
    body = env.body if expected is None else expected
    return env.context.root / spool._manifest_relative_path(
        forward._declared_cell_transport_symbol(body), spool.GENERAL_MANIFEST_DOMAIN)


def _objects(env):
    return {p.relative_to(env.context.root).as_posix() for p in (env.context.root / "objects").rglob("*") if p.is_file()}


def _forbidden(*args, **kwargs):
    pytest.fail("A committed account must reopen without account simulation or new native acquisition")


def _assert_existing_output_refusal(caught, details, cause=None):
    error = caught.value
    assert error.code == "DECLARED_CELL_EXISTING_OUTPUT_INVALID"
    assert error.details == details
    assert str(error) == "DECLARED_CELL_EXISTING_OUTPUT_INVALID:" + ",".join(details)
    if cause is None:
        assert error.__cause__ is None
    else:
        assert str(error.__cause__) == cause


@pytest.fixture(scope="module")
def complete(tmp_path_factory):
    """One 224-row synthetic account shared by read-only/restore-after-tamper tests."""
    root = tmp_path_factory.mktemp("synthetic-account-reuse")
    with pytest.MonkeyPatch.context() as patch:
        env = synthetic_declared_transport.__wrapped__(root, patch)
        env = synthetic_declared_complete_inputs.__wrapped__(env, root, patch)
        env.artifact = forward.produce_declared_cell_account_artifact(
            env.context, **_args(env), execution_product=env.products["H1"],
            spread_source_product=env.products["M5"], frozen_profile_path=env.profile_path)
        env.body = json.loads((env.context.root / env.artifact.descriptor["relative_path"]).read_bytes())
        yield env


def test_after_seal_reopens_without_simulation_or_replacement(complete, monkeypatch):
    env = complete
    manifest, objects = _manifest(env), _objects(env)
    before = manifest.read_bytes()
    monkeypatch.setattr(forward, "_run_declared_cell_account", _forbidden)
    monkeypatch.setattr(forward.Backtester, "run", _forbidden)
    monkeypatch.setattr(forward.native_prefix, "produce_activated_native_prefix", _forbidden)
    reopened = forward.discover_declared_cell_account_artifact(env.context, **_args(env))
    repeated = forward.produce_declared_cell_account_artifact(env.context, **_args(env))
    assert reopened.descriptor == repeated.descriptor == env.artifact.descriptor
    assert manifest.read_bytes() == before
    assert _objects(env) == objects
    assert causal.read_declared_cell_account_artifact(env.context, reopened,
        process=env.process, frozen_pipeline_config=env.frozen).account == env.body["account"]


def test_body_without_seal_is_absent_then_existing_writer_can_finish(complete, monkeypatch):
    env = complete
    manifest = _manifest(env)
    before, objects = manifest.read_bytes(), _objects(env)
    manifest.unlink()  # Simulate the state after the body put and before atomic seal.
    calls = []
    original = forward._run_declared_cell_account

    def record(**kwargs):
        calls.append(1)
        return original(**kwargs)

    monkeypatch.setattr(forward, "_run_declared_cell_account", record)
    try:
        assert forward.discover_declared_cell_account_artifact(env.context, **_args(env)) is None
        repeated = forward.produce_declared_cell_account_artifact(
            env.context, **_args(env), execution_product=env.products["H1"],
            spread_source_product=env.products["M5"], frozen_profile_path=env.profile_path)
        assert calls == [1]
        assert repeated.descriptor == env.artifact.descriptor
        assert manifest.read_bytes() == before
        assert _objects(env) == objects
    finally:
        manifest.write_bytes(before)


@pytest.mark.parametrize("damage,details,cause", [
    ("noncanonical", ("AuthoritySpoolError", ""), "JSON_NOT_CANONICAL"),
    ("extra_entry", ("MANIFEST_ROSTER",), None),
    ("renamed_entry", ("MANIFEST_ROSTER",), None),
    ("bad_descriptor", ("CausalEvidenceProductionError", "DECLARED_CELL_RAW_ACCOUNT_ARTIFACT_INTEGRITY_MISMATCH"),
     "DECLARED_CELL_RAW_ACCOUNT_ARTIFACT_INTEGRITY_MISMATCH"),
    ("body", ("CausalEvidenceProductionError", "DECLARED_CELL_RAW_ACCOUNT_ARTIFACT_INTEGRITY_MISMATCH"),
     "DECLARED_CELL_RAW_ACCOUNT_ARTIFACT_INTEGRITY_MISMATCH"),
])
def test_present_invalid_output_refuses_without_writes(complete, monkeypatch, damage, details, cause):
    env = complete
    manifest, body_path = _manifest(env), env.context.root / env.artifact.descriptor["relative_path"]
    original_manifest, original_body = manifest.read_bytes(), body_path.read_bytes()
    monkeypatch.setattr(forward, "_run_declared_cell_account", _forbidden)
    try:
        changed = json.loads(original_manifest)
        if damage == "noncanonical":
            manifest.write_bytes(original_manifest + b" ")
        elif damage == "body":
            body_path.write_bytes(original_body + b" ")
        else:
            if damage == "extra_entry":
                changed["artifacts"].append(dict(changed["artifacts"][0]))
            elif damage == "renamed_entry":
                changed["artifacts"][0]["artifact_id"] = "another-account"
            else:
                changed["artifacts"][0]["descriptor"]["sha256"] = "0" * 64
            manifest.write_bytes(spool._canonical_json_bytes(changed, env.context.limits.max_manifest_bytes))
        before = (manifest.read_bytes(), body_path.read_bytes(), _objects(env))
        with pytest.raises(forward.ForwardOOSReplayProductionError) as caught:
            forward.discover_declared_cell_account_artifact(env.context, **_args(env))
        _assert_existing_output_refusal(caught, details, cause)
        assert (manifest.read_bytes(), body_path.read_bytes(), _objects(env)) == before
    finally:
        manifest.write_bytes(original_manifest)
        body_path.write_bytes(original_body)


def test_manifest_directory_and_reparse_are_refusals(complete, monkeypatch):
    env, manifest = complete, _manifest(complete)
    before = manifest.read_bytes()
    manifest.unlink()
    manifest.mkdir()
    try:
        with pytest.raises(forward.ForwardOOSReplayProductionError) as caught:
            forward.discover_declared_cell_account_artifact(env.context, **_args(env))
        _assert_existing_output_refusal(caught, ("MANIFEST_NOT_REGULAR",))
    finally:
        manifest.rmdir()
        manifest.write_bytes(before)
    original = spool._is_reparse
    monkeypatch.setattr(spool, "_is_reparse", lambda path: Path(path) == manifest or original(path))
    with pytest.raises(forward.ForwardOOSReplayProductionError) as caught:
        forward.discover_declared_cell_account_artifact(env.context, **_args(env))
    _assert_existing_output_refusal(caught, ("AuthoritySpoolError", ""), "PATH_REPARSE_POINT_FORBIDDEN")
    assert manifest.read_bytes() == before


def test_manifest_access_error_is_never_absence(complete, monkeypatch):
    manifest = _manifest(complete)
    original = Path.lstat

    def denied(path, *args, **kwargs):
        if path == manifest:
            raise PermissionError("synthetic manifest access failure")
        return original(path, *args, **kwargs)

    monkeypatch.setattr(Path, "lstat", denied)
    with pytest.raises(forward.ForwardOOSReplayProductionError) as caught:
        forward.discover_declared_cell_account_artifact(complete.context, **_args(complete))
    _assert_existing_output_refusal(caught, ("PermissionError", ""), "synthetic manifest access failure")


@pytest.mark.parametrize("changed", ["precommit", "process", "status"])
def test_manifest_cannot_redirect_a_different_caller(complete, monkeypatch, changed):
    env, args = complete, _args(complete)
    inputs = dict(env.inputs)
    if changed == "precommit":
        args["precommit"] = SimpleNamespace(receipt_sha256="3" * 64)
    elif changed == "process":
        args["process"] = SimpleNamespace(receipt_sha256="4" * 64)
    else:
        inputs.update(progress_status="INCOMPLETE", reason_code="SYNTHETIC_MISMATCH")
        monkeypatch.setattr(forward, "_reopen_declared_cell_inputs", lambda *a, **k: inputs)
    expected = forward._declared_cell_body(env.context, inputs=inputs, **args)
    target, original = _manifest(env, expected), _manifest(env)
    created = target != original
    if created:
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(original.read_bytes())
    try:
        before = target.read_bytes()
        with pytest.raises(forward.ForwardOOSReplayProductionError) as caught:
            forward.discover_declared_cell_account_artifact(env.context, **args)
        if changed == "precommit":
            _assert_existing_output_refusal(caught, ("CALLER_BINDING", "declared_precommit_sha256"))
        else:
            _assert_existing_output_refusal(caught,
                ("CausalEvidenceProductionError", "DECLARED_CELL_TRANSPORT_BINDING_MISMATCH"),
                "DECLARED_CELL_TRANSPORT_BINDING_MISMATCH")
        assert target.read_bytes() == before
    finally:
        if created:
            target.unlink()


@pytest.mark.parametrize("status", ["STOP", "INCOMPLETE"])
def test_noncomplete_repeat_is_also_idempotent(synthetic_declared_transport, monkeypatch, status):
    env = synthetic_declared_transport
    if status == "INCOMPLETE":
        env.inputs.update(progress_status=status, reason_code="DECLARED_CELL_OWNER_PREFIX_INCOMPLETE",
            progress=SimpleNamespace(slot_summaries=(), slot_commit_artifacts=()))
        for row in env.inputs["slot_schedule"]:
            row.update(status="MISSING", owner_id=None)
        env.artifact = forward.produce_declared_cell_account_artifact(env.context, **_args(env))
        env.body = json.loads((env.context.root / env.artifact.descriptor["relative_path"]).read_bytes())
    before, objects = _manifest(env).read_bytes(), _objects(env)
    monkeypatch.setattr(forward, "_run_declared_cell_account", _forbidden)
    monkeypatch.setattr(forward.Backtester, "run", _forbidden)
    monkeypatch.setattr(forward.native_prefix, "produce_activated_native_prefix", _forbidden)
    monkeypatch.setattr(spool, "open_symbol_sink", _forbidden)
    reopened = forward.produce_declared_cell_account_artifact(env.context, **_args(env))
    assert reopened.descriptor == env.artifact.descriptor
    assert _manifest(env).read_bytes() == before
    assert _objects(env) == objects


def test_native_request_uses_bound_cutoff_profile_and_execution_terms(complete, tmp_path, monkeypatch):
    env, config = complete, complete.inputs["config"]
    original_data_dir = config.data_dir
    original_profile_sha = env.inputs["parent"]["spread_profile_identity"]["profile_sha256"]
    config.data_dir = tmp_path / "synthetic-request-source"
    target = Path(config.data_dir) / "native" / "terms" / "spread_profile.json"
    target.parent.mkdir(parents=True)
    target.write_bytes(env.profile_path.read_bytes())

    class Frozen:
        def materialize(self, context):
            assert context is env.context
            return config

    monkeypatch.setattr(forward.config_binding, "FrozenProductionPipelineConfig", Frozen)
    monkeypatch.setattr(forward.historical, "verify_declared_slot_owner_run_precommit",
        lambda *a, **k: env.inputs["parent"])
    args = dict(precommit=env.precommit, process=env.process, frozen_pipeline_config=Frozen())
    try:
        first = forward.declared_cell_account_native_request(env.context, **args)
        second = forward.declared_cell_account_native_request(
            env.context, **args, execution_product=env.products["H1"])
        assert first["cutoff_utc"] == env.body["measurement"]["end_exclusive_utc"]
        assert first["execution_timeframe"] == "H1"
        assert first["spread_source_timeframe"] is None
        assert second["spread_source_timeframe"] == "M5"
        assert first["profile_sha256"] == second["profile_sha256"] == env.body["frozen_profile_descriptor"]["sha256"]
        assert first["production_contract_required"] is False
        with pytest.raises(forward.ForwardOOSReplayProductionError) as caught:
            forward.declared_cell_account_native_request(env.context, **args, execution_product=env.products["M5"])
        assert caught.value.code == "FORWARD_NATIVE_PRODUCT_IDENTITY_INVALID"
        assert caught.value.details == ("DECLARED_CELL_EXECUTION",)
        assert str(caught.value) == "FORWARD_NATIVE_PRODUCT_IDENTITY_INVALID:DECLARED_CELL_EXECUTION"
        env.inputs["parent"]["spread_profile_identity"]["profile_sha256"] = "0" * 64
        with pytest.raises(forward.ForwardOOSReplayProductionError) as caught:
            forward.declared_cell_account_native_request(env.context, **args)
        assert caught.value.code == str(caught.value) == "DECLARED_CELL_PROFILE_CHANGED"
        assert caught.value.details == ()
    finally:
        config.data_dir = original_data_dir
        env.inputs["parent"]["spread_profile_identity"]["profile_sha256"] = original_profile_sha
