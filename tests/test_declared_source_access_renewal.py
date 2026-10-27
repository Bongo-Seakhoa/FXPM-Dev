"""Renewal mechanics on NEW labelled synthetic metadata, never capture P1/R.

S0 issuance and changed-source negatives use explicit synthetic identity inputs.
Default S1 uses four repository files plus staged orchestration; applied mode
delegates normal S1 verification to the genuine physical five-sibling locator.
Neither mode makes this a genuine old control. The finite completion-auth
seam below supplies one invented NO_TRADE, not genuine native/OWNER evidence.
Real metadata issuance, request verification, commit writing and cold progress
remain active. Native, search, fit, engine and real MT5 calls are forbidden.
"""

from copy import deepcopy
from dataclasses import replace
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import sys
from types import SimpleNamespace
from unittest.mock import Mock

import pytest


REPO = Path(__file__).resolve().parents[1]
PRODUCT_ROOT = REPO
STAGE = REPO  # Legacy fixture branch names still refer to this sole checkout.
VERIFY_APPLIED_SOURCE = True
sys.path.insert(0, str(REPO))
sys.path.insert(0, str(REPO / "tests"))

# The retained source ID is used only by the explicitly synthetic S0 fixture.
# This compatibility shape is not ownership/native/OWNER authority.
SYNTHETIC_HISTORICAL_S0_ORCHESTRATION_SHA256 = "216c397732cbc448838c8e4b4907e263d657348e3ce7c72b0cbb2b9cd9ecb2d7"
OWNERSHIP = {"files": {"pm_historical_anchor_orchestration.py": {"baseline": {
    "sha256": SYNTHETIC_HISTORICAL_S0_ORCHESTRATION_SHA256}}}}

import pm_authority_spool as spool
import pm_authoritative_search_execution as execution
import pm_historical_anchor_orchestration as orchestration
import pm_historical_production_adapter as adapter
import pm_historical_search_producer as producer
import pm_production_config_binding as binding
import pm_production_search_planner as planner
from prototype_support import _declared_environment
from prototype_support import _synthetic_profile


assert Path(orchestration.__file__).resolve() == PRODUCT_ROOT / "pm_historical_anchor_orchestration.py"
assert producer.historical is orchestration and adapter.orchestration is orchestration

CLASSIFICATION = "LANE188_SYNTHETIC_COMPLETION_AUTH_NOT_NATIVE_OR_OWNER_PROOF"
ISSUED_AT = "2026-10-27T12:00:00Z"
W0 = {"start_utc": "2026-10-27T11:00:00Z", "end_exclusive_utc": "2026-10-28T12:00:00Z"}
W1 = {"start_utc": ISSUED_AT, "end_exclusive_utc": "2026-11-10T12:00:00Z"}


class _Clock(datetime):
    @classmethod
    def now(cls, tz=None):
        assert tz is timezone.utc
        return cls(2026, 10, 27, 12, tzinfo=timezone.utc)


@pytest.fixture(autouse=True)
def no_execution(monkeypatch):
    targets = (
        (producer, "execute_declared_slot_owner_search"),
        (producer, "execute_declared_cell_owner_slots"),
        (execution, "fit_native_timeframe_for_search"),
        (execution.Backtester, "run"),
        (producer.native_prefix, "produce_activated_native_prefix"),
        (producer.native_prefix, "open_verified_prefix_frame"),
    )
    guards = []
    for owner, name in targets:
        guard = Mock(side_effect=AssertionError("Forbidden execution: " + name))
        monkeypatch.setattr(owner, name, guard)
        guards.append(guard)
    try:
        import MetaTrader5 as mt5
    except ImportError:
        mt5 = None
    if mt5 is not None:
        for name in ("initialize", "login", "order_send", "positions_get", "account_info",
                     "copy_rates_from_pos", "copy_rates_range", "history_deals_get"):
            guard = Mock(side_effect=AssertionError("Real MT5 forbidden: " + name))
            monkeypatch.setattr(mt5, name, guard, raising=False)
            guards.append(guard)
    yield
    for guard in guards:
        guard.assert_not_called()


def _sha(data):
    return hashlib.sha256(data).hexdigest()


def _error(expected_type, expected_code, action):
    with pytest.raises(expected_type) as caught:
        action()
    assert getattr(caught.value, "code", str(caught.value)) == expected_code


@pytest.fixture
def prefix(tmp_path, monkeypatch):
    root = tmp_path / "NEW_SYNTHETIC_RENEWAL_PREFIX"
    root.mkdir()
    measured_s1 = {}
    real_source_identity = orchestration._declared_source_identity
    modules = {
        "pm_historical_search_producer.py": producer,
        "pm_historical_production_adapter.py": adapter,
        "pm_authoritative_search_execution.py": execution,
        "pm_production_search_planner.py": planner,
    }
    for name in orchestration._DECLARED_SOURCE_FILES:
        path = PRODUCT_ROOT / name if name == "pm_historical_anchor_orchestration.py" else REPO / name
        if name in modules:
            assert Path(modules[name].__file__).resolve() == path.resolve()
        measured_s1[name] = _sha(path.read_bytes())
    if VERIFY_APPLIED_SOURCE:
        assert real_source_identity() == measured_s1
    recorded_s0 = {**measured_s1, "pm_historical_anchor_orchestration.py":
        OWNERSHIP["files"]["pm_historical_anchor_orchestration.py"]["baseline"]["sha256"]}
    assert [name for name in recorded_s0 if recorded_s0[name] != measured_s1[name]] == [
        "pm_historical_anchor_orchestration.py"]
    state = {"identity": dict(recorded_s0)}

    def source_identity():
        if VERIFY_APPLIED_SOURCE and state["identity"] == measured_s1:
            return real_source_identity()
        # Historical S0 issuance and refusal-only fault inputs remain synthetic.
        return dict(state["identity"])

    monkeypatch.setattr(orchestration, "_declared_source_identity", source_identity)

    env = _declared_environment(root, monkeypatch, issue=False)
    monkeypatch.setattr(orchestration, "datetime", _Clock)
    monkeypatch.setattr(producer, "datetime", _Clock)
    config = env.frozen.materialize(env.context)
    terms = Path(config.data_dir) / "native" / "terms"
    tiers = _synthetic_profile(terms, symbol="AUDUSD")
    (terms / "synthetic_frozen_profile.json").rename(terms / "spread_profile.json")
    env.arguments["spread_profile_identity"] = {"profile_sha256": tiers["P75"].descriptor()["profile_sha256"]}
    env.arguments["source_access_window_utc"] = dict(W0)
    env.precommit = orchestration.precommit_declared_slot_owner_run_v2(env.context, **env.arguments)
    env.state, env.s0, env.s1 = state, recorded_s0, measured_s1
    env.cell_id = env.arguments["cell_roster"][0]["cell_id"]
    env.slot = env.roster["slots"][0]
    env.parent_bytes = env.precommit._body_json
    env.authentication_calls = []

    request = orchestration.declared_slot_producer_request(env.context, precommit=env.precommit,
        process=env.process, cell_id=env.cell_id, slot_id=env.slot["slot_id"],
        attempt_id="labelled-synthetic-prefix", sink=env.sink)
    completion = producer._declared_base_completion(request, status="NO_TRADE",
        reason_code="LABELLED_SYNTHETIC_NO_TRADE", source_entry=None)
    completion["classification"] = CLASSIFICATION
    completion_bytes = orchestration._declared_json(completion)

    def finite_completion_auth(context, *, precommit, process, completion, artifact_roster,
                               frozen_pipeline_config):
        assert context.root == env.context.root and process.receipt_sha256 == env.process.receipt_sha256
        assert frozen_pipeline_config is env.frozen and list(artifact_roster) == []
        assert orchestration._declared_json(completion) == completion_bytes
        assert completion["classification"] == CLASSIFICATION
        sink = spool.open_symbol_sink(context, "AUDUSD")
        reopened = orchestration.DeclaredSlotProducerRequest(
            orchestration._declared_json(completion["request_body"]), precommit, sink,
            orchestration._DECLARED_SLOT_SEAL)
        orchestration._register_declared_request(reopened, context, live=False)
        try:
            verified = orchestration.verify_declared_slot_producer_request(
                context, reopened, precommit=precommit, process=process)
            assert reopened.receipt_sha256 == completion["request_sha256"]
            assert verified["anchor"]["ordinal"] == 1
        finally:
            orchestration._release_declared_slot_producer_request(reopened, context)
        env.authentication_calls.append(completion["request_sha256"])
        return deepcopy(completion)

    monkeypatch.setattr(adapter, "verify_declared_slot_completion", finite_completion_auth)
    _, descriptor = producer._commit_declared_slot_completion(env.context, precommit=env.precommit,
        process=env.process, cell_id=env.cell_id, slot=env.slot, completion=completion,
        artifact_roster=[], previous_commit_sha256=None, frozen_pipeline_config=env.frozen)
    env.first_request, env.commit_descriptor = request, descriptor
    env.commit_pointer = env.context.root / producer._declared_commit_path(env.precommit, env.cell_id, 1)
    env.evidence = {
        env.context.root / f"declared-slots/{env.precommit.receipt_sha256}/precommit.json": env.parent_bytes,
        env.context.root / orchestration._declared_request_path(request.to_dict()): request._body_json,
        env.commit_pointer: env.commit_pointer.read_bytes(),
        env.context.root / descriptor["relative_path"]:
            (env.context.root / descriptor["relative_path"]).read_bytes(),
    }
    env.progress_before = _progress(env)
    assert len(env.progress_before.slot_summaries) == 1
    assert env.progress_before.first_uncommitted_slot["ordinal"] == 2
    assert env.progress_before.last_commit_sha256 == descriptor["sha256"]
    assert env.progress_before.terminal_stop is False
    env.rdir = env.context.root / orchestration._declared_renewal_directory(env.precommit)
    return env


def _progress(env):
    return producer.reopen_declared_cell_owner_progress(env.context, precommit=env.precommit,
        process=env.process, cell_id=env.cell_id, frozen_pipeline_config=env.frozen)


def _issue(env, **kwargs):
    env.state["identity"] = dict(env.s1)
    return orchestration.issue_declared_slot_source_access_renewal(env.context,
        precommit_receipt_sha256=env.precommit.receipt_sha256, process=env.process,
        frozen_pipeline_config=kwargs.pop("frozen_pipeline_config", env.frozen),
        source_access_window_utc=kwargs.pop("source_access_window_utc", dict(W1)), **kwargs)


def _verify(env):
    return orchestration.verify_declared_slot_source_access_renewal(
        env.context, precommit=env.precommit, process=env.process)


def _retained_evidence_unchanged(env):
    for path, before in env.evidence.items():
        assert path.read_bytes() == before


def _replace_renewal(env, body):
    """Owned synthetic corruption only; never an issuer or a genuine record."""
    paths = tuple(env.rdir.iterdir())
    assert len(paths) == 1 and paths[0].parent == env.rdir
    encoded = orchestration._declared_json(body)
    target = env.rdir / (_sha(encoded) + ".json")
    assert target != paths[0] and not target.exists()
    paths[0].write_bytes(encoded)
    paths[0].rename(target)


def test_receipt_bootstrap_preserves_cold_prefix_and_uses_overlapping_suffix_window(prefix):
    env = prefix
    env.state["identity"] = dict(env.s1)
    error = orchestration.HistoricalAnchorOrchestrationError
    _error(error, "DECLARED_SLOT_PRECOMMIT_BINDING_MISMATCH", lambda:
        orchestration.verify_declared_slot_owner_run_precommit(env.context, env.precommit, process=env.process))
    _error(error, "DECLARED_SLOT_PRECOMMIT_BINDING_MISMATCH", lambda:
        orchestration.reopen_declared_slot_owner_run_precommit(env.context,
            receipt_sha256=env.precommit.receipt_sha256, process=env.process,
            frozen_pipeline_config=env.frozen))

    env.context = spool.reopen_generation_context(env.context.root,
        expected_generation_id=env.context.generation_id,
        expected_native_universe_sha256=env.context.native_universe_sha256,
        expected_config_sha256=env.context.config_sha256)
    env.process = planner.reopen_production_search_process(env.context,
        spool_receipt=env.process.spool_receipt, expected_receipt_sha256=env.process.receipt_sha256)
    files_before = {path.relative_to(env.context.root) for path in env.context.root.rglob("*") if path.is_file()}
    renewal = _issue(env)
    env.precommit = orchestration.reopen_declared_slot_owner_run_precommit(env.context,
        receipt_sha256=env.precommit.receipt_sha256, process=env.process, frozen_pipeline_config=env.frozen)
    body = renewal.to_dict()
    assert body["old_source_access_window_utc"] == W0
    assert body["source_access_window_utc"] == W1
    assert body["issued_at_utc"] == ISSUED_AT
    assert body["authority_ref"] == "E-00003232"
    assert all(body[key] is False for key in ("qualification_authority", "capital_authority",
        "live_authority", "order_authority"))
    assert W0["start_utc"] < W1["start_utc"] < W0["end_exclusive_utc"] < W1["end_exclusive_utc"]
    assert body["commit01_body_sha256"] == env.commit_descriptor["sha256"]
    assert body["original_source_identity"] == env.s0 and body["renewed_source_identity"] == env.s1
    assert _verify(env).receipt_sha256 == renewal.receipt_sha256
    paths = tuple(env.rdir.iterdir())
    assert len(paths) == 1 and paths[0].name == renewal.receipt_sha256 + ".json"
    assert paths[0].read_bytes() == orchestration._declared_json(body)
    files_after = {path.relative_to(env.context.root) for path in env.context.root.rglob("*") if path.is_file()}
    assert files_after - files_before == {paths[0].relative_to(env.context.root)}

    authentication_before = len(env.authentication_calls)
    after = _progress(env)
    assert len(env.authentication_calls) == authentication_before + 1
    assert after.slot_summaries == env.progress_before.slot_summaries
    assert after.first_uncommitted_slot == env.progress_before.first_uncommitted_slot
    assert after.last_commit_sha256 == env.progress_before.last_commit_sha256
    assert after.terminal_stop is False
    assert env.precommit._body_json == env.parent_bytes
    _retained_evidence_unchanged(env)

    sink = spool.open_symbol_sink(env.context, "AUDUSD")
    first = orchestration.declared_slot_producer_request(env.context, precommit=env.precommit,
        process=env.process, cell_id=env.cell_id, slot_id=env.roster["slots"][0]["slot_id"],
        attempt_id="slot1-metadata-only", sink=sink)
    second = orchestration.declared_slot_producer_request(env.context, precommit=env.precommit,
        process=env.process, cell_id=env.cell_id, slot_id=env.roster["slots"][1]["slot_id"],
        attempt_id="slot2-metadata-only", sink=sink)
    assert first.to_dict()["source_access_window_utc"] == W0
    assert "source_access_renewal_sha256" not in first.to_dict()
    assert second.to_dict()["source_access_window_utc"] == W1
    assert second.to_dict()["source_access_renewal_sha256"] == renewal.receipt_sha256
    assert orchestration.verify_declared_slot_producer_request(
        env.context, second, precommit=env.precommit, process=env.process) == second.to_dict()
    _retained_evidence_unchanged(env)
    env.state["identity"]["pm_historical_anchor_orchestration.py"] = "0" * 64
    _error(error, "DECLARED_SLOT_RENEWAL_BINDING_MISMATCH", lambda:
        orchestration.verify_declared_slot_owner_run_precommit(env.context, env.precommit, process=env.process))


@pytest.mark.parametrize("name", (
    "pm_historical_search_producer.py", "pm_historical_production_adapter.py",
    "pm_authoritative_search_execution.py", "pm_production_search_planner.py",
))
def test_other_declared_source_changes_are_not_renewable(prefix, name):
    env = prefix
    env.state["identity"] = {**env.s1, name: _sha(("SYNTHETIC_CHANGED:" + name).encode("ascii"))}
    error = orchestration.HistoricalAnchorOrchestrationError
    _error(error, "DECLARED_SLOT_RENEWAL_SOURCE_INVALID", lambda:
        orchestration.issue_declared_slot_source_access_renewal(env.context,
            precommit_receipt_sha256=env.precommit.receipt_sha256, process=env.process,
            frozen_pipeline_config=env.frozen, source_access_window_utc=dict(W1)))
    assert not env.rdir.exists()
    _retained_evidence_unchanged(env)


@pytest.mark.parametrize("damage", ("config", "economics", "process"))
def test_changed_bound_inputs_refuse_before_renewal_publication(prefix, damage):
    env = prefix
    if damage == "process":
        body = env.process.to_dict()
        body["semantics"]["attempt_policy"]["attempt_count_per_observed_cell"] -= 1
        changed = orchestration._declared_json(body)
        assert changed != env.process._body_json
        env.process = replace(env.process, _body_json=changed)
        expected_type, code = planner.ProductionSearchPlannerError, "PROCESS_SEMANTICS_CHANGED"
    else:
        payload = json.loads(env.config_path.read_text(encoding="utf-8"))
        if damage == "economics":
            payload["pipeline"]["discovery_risk_per_trade_pct"] = 0.25
        else:
            payload["storage"]["synthetic_changed_config"] = True
        env.config_path.write_text(json.dumps(payload), encoding="utf-8")
        expected_type, code = binding.ProductionConfigBindingError, "ACTIVE_CONFIG_BYTES_CHANGED"
    _error(expected_type, code, lambda: _issue(env))
    assert not env.rdir.exists()
    _retained_evidence_unchanged(env)


@pytest.mark.parametrize("window", (
    {"start_utc": W0["start_utc"], "end_exclusive_utc": W1["end_exclusive_utc"]},
    {"start_utc": ISSUED_AT, "end_exclusive_utc": W0["end_exclusive_utc"]},
    {"start_utc": "2026-10-27T12:00:00", "end_exclusive_utc": W1["end_exclusive_utc"]},
), ids=("before-issuance", "no-future-extension", "noncanonical-utc"))
def test_invalid_new_windows_refuse_before_publication(prefix, window):
    env = prefix
    _error(orchestration.HistoricalAnchorOrchestrationError, "DECLARED_SLOT_RENEWAL_WINDOW_INVALID",
        lambda: _issue(env, source_access_window_utc=window))
    assert not env.rdir.exists()
    _retained_evidence_unchanged(env)


@pytest.mark.parametrize(("damage", "code"), (
    ("bytes", "DECLARED_SLOT_RENEWAL_INVALID"),
    ("old-window", "DECLARED_SLOT_RENEWAL_BINDING_MISMATCH"),
    ("other-precommit", "DECLARED_SLOT_RENEWAL_BINDING_MISMATCH"),
    ("other-live-source", "DECLARED_SLOT_RENEWAL_BINDING_MISMATCH"),
))
def test_corrupt_or_mixed_renewal_cannot_authorize_old_precommit(prefix, damage, code):
    env = prefix
    renewal = _issue(env)
    if damage == "bytes":
        path = next(env.rdir.iterdir())
        path.write_bytes(path.read_bytes() + b" ")
    else:
        body = renewal.to_dict()
        if damage == "old-window":
            body["old_source_access_window_utc"]["end_exclusive_utc"] = W1["end_exclusive_utc"]
        elif damage == "other-precommit":
            body["precommit_sha256"] = "0" * 64
        else:
            body["renewed_source_identity"]["pm_production_search_planner.py"] = "0" * 64
        _replace_renewal(env, body)
    _error(orchestration.HistoricalAnchorOrchestrationError, code, lambda: _verify(env))
    _error(orchestration.HistoricalAnchorOrchestrationError, code, lambda:
        orchestration.verify_declared_slot_owner_run_precommit(env.context, env.precommit, process=env.process))
    _retained_evidence_unchanged(env)


def test_commit_body_digest_not_pointer_or_filename_is_bound(prefix):
    env = prefix
    renewal = _issue(env)
    item = producer._read_declared_commit(env.context,
        producer._declared_commit_path(env.precommit, env.cell_id, 1))
    body = deepcopy(item[0])
    body["completion"]["reason_code"] = "LABELLED_SYNTHETIC_COMMIT_DAMAGE"
    descriptor = spool.write_canonical_json_artifact(env.context, body)
    assert descriptor["sha256"] != renewal.to_dict()["commit01_body_sha256"]
    pointer = {"schema_version": producer._DECLARED_SLOT_POINTER_SCHEMA, "commit_descriptor": descriptor}
    env.commit_pointer.write_bytes(orchestration._declared_json(pointer))
    _error(orchestration.HistoricalAnchorOrchestrationError, "DECLARED_SLOT_RENEWAL_PREFIX_INVALID",
        lambda: _verify(env))


def test_second_issuance_is_refused_without_replacing_the_first_record(prefix):
    env = prefix
    renewal = _issue(env)
    before = {path: path.read_bytes() for path in env.rdir.iterdir()}
    _error(orchestration.HistoricalAnchorOrchestrationError, "DECLARED_SLOT_RENEWAL_ALREADY_ISSUED",
        lambda: _issue(env))
    assert {path: path.read_bytes() for path in env.rdir.iterdir()} == before
    assert _verify(env).receipt_sha256 == renewal.receipt_sha256
    _retained_evidence_unchanged(env)


@pytest.mark.parametrize("state", ("empty-reservation", "extra-record"))
def test_partial_or_multiple_records_fail_closed(prefix, state):
    env = prefix
    env.state["identity"] = dict(env.s1)
    if state == "empty-reservation":
        env.rdir.mkdir()
    else:
        renewal = _issue(env)
        (env.rdir / ("0" * 64 + ".json")).write_bytes(orchestration._declared_json(renewal.to_dict()))
    _error(orchestration.HistoricalAnchorOrchestrationError, "DECLARED_SLOT_RENEWAL_MIXED_OR_PARTIAL",
        lambda: _verify(env))
    _error(orchestration.HistoricalAnchorOrchestrationError, "DECLARED_SLOT_RENEWAL_MIXED_OR_PARTIAL", lambda:
        orchestration.verify_declared_slot_owner_run_precommit(env.context, env.precommit, process=env.process))
    _retained_evidence_unchanged(env)


@pytest.mark.parametrize("failure_kind", ("quota", "oserror"))
def test_publication_failure_cleans_empty_reservation_and_same_prefix_retries(
    prefix, monkeypatch, failure_kind,
):
    """Injected publisher refusal, not a real disk-full or quota incident."""
    env = prefix
    failure = (spool.AuthoritySpoolError("LABELLED_SYNTHETIC_QUOTA_REFUSAL")
        if failure_kind == "quota" else OSError("LABELLED_SYNTHETIC_PUBLICATION_FAILURE"))
    attempted = []

    def refuse_publication(context, relative_path, value, maximum_bytes):
        assert context is env.context
        target = spool._local_path(context.root, relative_path)
        assert target.parent == env.rdir and env.rdir.is_dir()
        assert tuple(env.rdir.iterdir()) == ()
        attempted.append(target)
        raise failure

    with monkeypatch.context() as patch:
        patch.setattr(spool, "_publish_exact_json", refuse_publication)
        with pytest.raises(orchestration.HistoricalAnchorOrchestrationError) as caught:
            _issue(env)
    assert str(caught.value) == "DECLARED_SLOT_RENEWAL_PUBLICATION_FAILED"
    assert caught.value.__cause__ is failure and len(attempted) == 1
    assert not env.rdir.exists()
    _retained_evidence_unchanged(env)
    _error(orchestration.HistoricalAnchorOrchestrationError,
        "DECLARED_SLOT_PRECOMMIT_BINDING_MISMATCH", lambda:
        orchestration.verify_declared_slot_owner_run_precommit(
            env.context, env.precommit, process=env.process))

    renewal = _issue(env)
    assert attempted[0].name == renewal.receipt_sha256 + ".json"
    assert tuple(env.rdir.iterdir()) == (attempted[0],)
    env.precommit = orchestration.reopen_declared_slot_owner_run_precommit(env.context,
        receipt_sha256=env.precommit.receipt_sha256, process=env.process,
        frozen_pipeline_config=env.frozen)
    after = _progress(env)
    assert after.slot_summaries == env.progress_before.slot_summaries
    assert after.first_uncommitted_slot == env.progress_before.first_uncommitted_slot
    assert after.first_uncommitted_slot["ordinal"] == 2 and after.terminal_stop is False
    assert after.last_commit_sha256 == env.progress_before.last_commit_sha256
    assert env.precommit._body_json == env.parent_bytes
    _retained_evidence_unchanged(env)


@pytest.mark.parametrize("content", ("partial", "published"))
def test_publication_failure_preserves_nonempty_content_and_primary_error(
    prefix, monkeypatch, content,
):
    """A partial or published record is never automatic-cleanup material."""
    env = prefix
    real_publish = spool._publish_exact_json
    failure = OSError("LABELLED_SYNTHETIC_FAILURE_AFTER_CONTENT")
    retained = {}

    def fail_after_content(context, relative_path, value, maximum_bytes):
        target = spool._local_path(context.root, relative_path)
        assert context is env.context and target.parent == env.rdir
        if content == "published":
            real_publish(context, relative_path, value, maximum_bytes)
        else:
            target.write_bytes(b"LABELLED_SYNTHETIC_PARTIAL_RENEWAL")
        retained[target] = target.read_bytes()
        raise failure

    with monkeypatch.context() as patch:
        patch.setattr(spool, "_publish_exact_json", fail_after_content)
        with pytest.raises(orchestration.HistoricalAnchorOrchestrationError) as caught:
            _issue(env)
    assert str(caught.value) == "DECLARED_SLOT_RENEWAL_PUBLICATION_FAILED"
    assert caught.value.__cause__ is failure and len(retained) == 1
    assert env.rdir.is_dir()
    assert {path: path.read_bytes() for path in env.rdir.iterdir()} == retained
    if content == "published":
        renewal = _verify(env)
        assert next(iter(retained)).name == renewal.receipt_sha256 + ".json"
    else:
        _error(orchestration.HistoricalAnchorOrchestrationError,
            "DECLARED_SLOT_RENEWAL_INVALID", lambda: _verify(env))
    _error(orchestration.HistoricalAnchorOrchestrationError,
        "DECLARED_SLOT_RENEWAL_ALREADY_ISSUED", lambda: _issue(env))
    assert {path: path.read_bytes() for path in env.rdir.iterdir()} == retained
    _retained_evidence_unchanged(env)
