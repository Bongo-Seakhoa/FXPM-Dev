"""Synthetic resource routing, not continuation authority or market evidence.

Metadata issuers and producer/driver branches are real. Native/authentication,
fitting and completion boundaries are explicitly synthetic; no financial fit,
backtest, retained QA control, broker or market dataset is requested.
"""
from __future__ import annotations

import errno
from types import SimpleNamespace
from pathlib import Path

import pytest

import pm_authority_spool as spool
import pm_authority_spool_quota as quota
import pm_core
import pm_historical_anchor_orchestration as historical
import pm_historical_production_adapter as adapter
import pm_historical_search_producer as producer
import pm_winner_raw_evidence as raw
from tests.test_declared_slot_owner_surface import (
    CELL_ID, _declared_environment, _execute, _progress, _request,
    _structural_driver_trace, _structural_native_snapshot,
    _structural_snapshot_backend,
)


FREE_SPACE = "SPOOL_QUOTA_FREE_SPACE_RESERVE_BREACHED"
GENERATION_BYTES = "SPOOL_QUOTA_GENERATION_BYTES_EXCEEDED"


def _caused(outer, inner):
    outer.__cause__ = inner
    return outer


def _capacity(wrapped=False):
    error = quota.AuthoritySpoolQuotaError(FREE_SPACE, "synthetic owned spool")
    return _caused(spool.AuthoritySpoolError(str(error)), error) if wrapped else error


@pytest.mark.parametrize("wrapper", ["direct", "spool", "adapter", "raw"])
def test_capacity_classifier_follows_only_actual_explicit_typed_cause(wrapper):
    error = _capacity()
    if wrapper != "direct":
        kind = {"spool": spool.AuthoritySpoolError,
                "adapter": adapter.HistoricalProductionAdapterError,
                "raw": raw.WinnerRawEvidenceError}[wrapper]
        if wrapper == "spool":
            error = _caused(kind(str(error)), error)
        else:
            error = _caused(kind("SYNTHETIC_TRANSPARENT_WRAPPER"), _capacity(True))
    assert spool.is_resource_capacity_error(error) is True


@pytest.mark.parametrize("kind", ["generation", "digest", "limits", "reservation",
                                  "spool_text", "runtime_text", "oserror", "memory", "lookalike"])
def test_capacity_classifier_does_not_exempt_other_failures(kind):
    error = {
        "generation": lambda: quota.AuthoritySpoolQuotaError(GENERATION_BYTES),
        "digest": lambda: quota.AuthoritySpoolQuotaError("SPOOL_QUOTA_STATE_DIGEST_INVALID"),
        "limits": lambda: quota.AuthoritySpoolQuotaError("SPOOL_QUOTA_LIMITS_CHANGED"),
        "reservation": lambda: quota.AuthoritySpoolQuotaError("SPOOL_QUOTA_RESERVATION_NOT_ACTIVE"),
        "spool_text": lambda: spool.AuthoritySpoolError(FREE_SPACE),
        "runtime_text": lambda: RuntimeError(FREE_SPACE),
        "oserror": lambda: OSError(errno.ENOSPC, FREE_SPACE),
        "memory": lambda: MemoryError(FREE_SPACE),
        "lookalike": lambda: SimpleNamespace(code=FREE_SPACE),
    }[kind]()
    assert spool.is_resource_capacity_error(error) is False


def test_first_typed_quota_integrity_error_blocks_deeper_capacity():
    integrity = _caused(quota.AuthoritySpoolQuotaError("SPOOL_QUOTA_STATE_DIGEST_INVALID"), _capacity())
    assert spool.is_resource_capacity_error(_caused(spool.AuthoritySpoolError(str(integrity)), integrity)) is False


def test_native_spool_integrity_code_cannot_launder_capacity_cause():
    integrity = _caused(spool.AuthoritySpoolError("ARTIFACT_DIGEST_MISMATCH"), _capacity())
    outer = _caused(adapter.HistoricalProductionAdapterError("outer"), integrity)
    assert spool.is_resource_capacity_error(outer) is False


def test_spool_message_equality_without_direct_typed_quota_cause_is_not_transparent():
    intermediary = _caused(RuntimeError("intermediary"), _capacity())
    error = _caused(spool.AuthoritySpoolError(str(intermediary)), intermediary)
    assert spool.is_resource_capacity_error(error) is False


def test_unrelated_implicit_context_and_explicit_cause_cycles_do_not_exempt():
    unrelated = RuntimeError("integrity failure after a handled capacity failure")
    unrelated.__context__ = _capacity()
    assert spool.is_resource_capacity_error(unrelated) is False
    first, second = RuntimeError("first"), RuntimeError("second")
    first.__cause__, second.__cause__ = second, first
    assert spool.is_resource_capacity_error(first) is False


def _no_financial_execution(*args, **kwargs):
    pytest.fail("Resource routing assurance must not run a fit/backtest/market experiment")


def _source_path(tmp_path, monkeypatch, *, completed=False):
    snapshot = _structural_native_snapshot()
    env = _declared_environment(tmp_path, monkeypatch, native_snapshot=snapshot)
    request = _request(env)
    _structural_snapshot_backend(env, monkeypatch, observed=snapshot,
        state=producer.native_prefix.AVAILABLE if completed else producer.native_prefix.TECHNICAL_TERMINAL)
    monkeypatch.setattr(pm_core.Backtester, "run", _no_financial_execution)
    capture, fitted = None, None
    if completed:
        source = SimpleNamespace(receipt_sha256="a" * 64,
            to_dict=lambda: {"availability": producer.book.AVAILABLE})
        regime = SimpleNamespace(receipt_sha256="b" * 64,
            to_dict=lambda: {"fit_state": producer.book.REGIME_FITTED})
        fitted = SimpleNamespace(_spread_tiers={"P75": SimpleNamespace(
            descriptor=lambda: {"profile_sha256": "e" * 64})},
            source_group_decision=source, regime_fit_decision=regime)
        monkeypatch.setattr(producer.search_execution, "fit_native_timeframe_for_search", lambda **kwargs: fitted)
        monkeypatch.setattr(producer, "_verified_support_counts", lambda value: {"TREND": 1})
        monkeypatch.setattr(producer.search_planner, "build_declared_slot_observed_cell_plan", lambda *a, **k: object())
        capture = SimpleNamespace(_completion=None, completion=None)
        monkeypatch.setattr(adapter, "open_declared_slot_capture", lambda *a, **k: capture)
    return env, request, capture, fitted


@pytest.mark.parametrize("phase", ["snapshot", "source_session"])
@pytest.mark.parametrize("wrapped", [False, True])
def test_source_capacity_preserves_original_error_without_refusal_or_commit(tmp_path, monkeypatch, phase, wrapped):
    env, request, _, _ = _source_path(tmp_path, monkeypatch)
    original, attempted = env.sink.put_json, []
    error = _capacity(wrapped)

    def put(artifact_id, artifact_type, value):
        attempted.append(artifact_id)
        if artifact_id.endswith("/technical-source-session"):
            pytest.fail("An operational capacity failure attempted a second STOP/refusal publication")
        if ((phase == "snapshot" and artifact_type == "declared_slot_native_snapshot")
                or (phase == "source_session" and artifact_id.endswith("/source-session"))):
            raise error
        return original(artifact_id, artifact_type, value)

    monkeypatch.setattr(env.sink, "put_json", put)
    with pytest.raises(type(error)) as caught:
        producer.execute_declared_slot_owner_search(env.context, request=request,
            process=env.process, frozen_pipeline_config=env.frozen)
    assert caught.value is error
    assert not any(name.endswith("/technical-source-session") for name in attempted)
    assert not (env.context.root / producer._declared_commit_path(env.precommit, CELL_ID, 1)).exists()
    assert not (env.context.root / producer._declared_terminal_path(env.precommit, CELL_ID)).exists()


@pytest.mark.parametrize("code", [GENERATION_BYTES, "SPOOL_QUOTA_STATE_DIGEST_INVALID"])
def test_noncapacity_source_failure_keeps_existing_technical_stop(tmp_path, monkeypatch, code):
    env, request, _, _ = _source_path(tmp_path, monkeypatch)
    original = env.sink.put_json

    def put(artifact_id, artifact_type, value):
        if artifact_id.endswith("/source-session"):
            raise quota.AuthoritySpoolQuotaError(code)
        return original(artifact_id, artifact_type, value)

    monkeypatch.setattr(env.sink, "put_json", put)
    completion = producer.execute_declared_slot_owner_search(env.context, request=request,
        process=env.process, frozen_pipeline_config=env.frozen)
    assert (completion["status"], completion["reason_code"]) == ("STOP", "DECLARED_SLOT_SOURCE_FAILURE")
    source = adapter._reopen_ascii_json_entry(env.context, completion["source_session_entry"],
        expected_type="declared_slot_source_session", code="SYNTHETIC_SOURCE_INVALID")
    assert source["failure_code"] == code


@pytest.mark.parametrize("wrapped", [False, True])
def test_postcompletion_capacity_is_not_terminal_wrapper_and_drops_live_session(tmp_path, monkeypatch, wrapped):
    env, request, capture, fitted = _source_path(tmp_path, monkeypatch, completed=True)
    error, computed = _capacity(wrapped), {"classification": "SYNTHETIC_COMPUTED_BRANCH_NOT_OWNER"}

    def execute(**kwargs):
        assert kwargs["capture"] is capture
        capture._completion = capture.completion = computed
        raise error

    monkeypatch.setattr(producer.search_execution, "execute_historical_observed_cell", execute)
    with pytest.raises(type(error)) as caught:
        producer.execute_declared_slot_owner_search(env.context, request=request,
            process=env.process, frozen_pipeline_config=env.frozen)
    assert caught.value is error and capture._completion is computed
    with pytest.raises(producer.HistoricalSearchProducerError) as cleared:
        producer._verified_declared_slot_fitted(env.context, request=request, process=env.process, fitted=fitted)
    assert cleared.value.code == "DECLARED_SLOT_SOURCE_CAPABILITY_INVALID"
    assert not (env.context.root / producer._declared_commit_path(env.precommit, CELL_ID, 1)).exists()


def test_postcompletion_noncapacity_keeps_original_terminal_failure_contract(tmp_path, monkeypatch):
    env, request, capture, _ = _source_path(tmp_path, monkeypatch, completed=True)
    error = quota.AuthoritySpoolQuotaError("SPOOL_QUOTA_STATE_DIGEST_INVALID")
    computed = {"classification": "SYNTHETIC_COMPUTED_BRANCH_NOT_OWNER"}

    def execute(**kwargs):
        capture._completion = capture.completion = computed
        raise error

    monkeypatch.setattr(producer.search_execution, "execute_historical_observed_cell", execute)
    with pytest.raises(producer._DeclaredSlotPostCompletionFailure) as caught:
        producer.execute_declared_slot_owner_search(env.context, request=request,
            process=env.process, frozen_pipeline_config=env.frozen)
    assert caught.value.__cause__ is error and caught.value.completion == computed
    assert caught.value.failure["failure_code"] == "SPOOL_QUOTA_STATE_DIGEST_INVALID"


@pytest.mark.parametrize("phase,committed", [
    ("attempt_start", 0), ("execute", 0), ("commit_before", 0),
    ("commit_after", 1), ("finalization", 13),
])
def test_driver_capacity_releases_request_without_refusal_commit(tmp_path, monkeypatch, phase, committed):
    env = _declared_environment(tmp_path, monkeypatch)
    state = _structural_driver_trace(env, monkeypatch)
    error, issued = _capacity(True), []
    issue, execute = historical.declared_slot_producer_request, producer.execute_declared_slot_owner_search
    commit, publish = producer._commit_declared_slot_completion, spool._publish_exact_json

    def record(*args, **kwargs):
        request = issue(*args, **kwargs)
        issued.append(request)
        return request

    def run(context, **kwargs):
        if phase == "execute":
            raise error
        return execute(context, **kwargs)

    def put(context, relative_path, *args, **kwargs):
        if phase == "attempt_start" and "/attempts/" in relative_path:
            raise error
        return publish(context, relative_path, *args, **kwargs)

    def commit_once(*args, **kwargs):
        if phase == "commit_before":
            raise error
        result = commit(*args, **kwargs)
        if phase == "commit_after":
            raise error
        return result

    monkeypatch.setattr(historical, "declared_slot_producer_request", record)
    monkeypatch.setattr(producer, "execute_declared_slot_owner_search", run)
    monkeypatch.setattr(spool, "_publish_exact_json", put)
    monkeypatch.setattr(producer, "_commit_declared_slot_completion", commit_once)
    if phase == "finalization":
        monkeypatch.setattr(spool.SymbolAuthoritySink, "seal", lambda sink: (_ for _ in ()).throw(error))
    with pytest.raises(type(error)) as caught:
        _execute(env)
    assert caught.value is error
    progress = _progress(env)
    assert len(progress.slot_summaries) == committed
    assert progress.terminal_stop is False and progress.terminal_failure is None
    assert progress.run_receipt is None
    assert not any(row["status"] == "STOP" for row in progress.slot_summaries)
    for request in issued:
        with pytest.raises(historical.HistoricalAnchorOrchestrationError) as cleared:
            historical._verify_declared_request_identity(request, env.context, require_live=True)
        assert str(cleared.value) == "DECLARED_SLOT_REQUEST_CAPABILITY_INVALID"
    assert len(issued) == (13 if phase == "finalization" else 1)


def test_explicit_later_attempt_reuses_committed_prefix_without_research(tmp_path, monkeypatch):
    """Only existing committed metadata reuse; no partial-fit continuation claim."""
    env = _declared_environment(tmp_path, monkeypatch)
    state = _structural_driver_trace(env, monkeypatch)
    original, error = producer._commit_declared_slot_completion, _capacity()

    def after_link(*args, **kwargs):
        original(*args, **kwargs)
        raise error

    monkeypatch.setattr(producer, "_commit_declared_slot_completion", after_link)
    with pytest.raises(type(error)) as caught:
        _execute(env)
    assert caught.value is error and state.calls == [1]
    first = _progress(env).slot_commit_artifacts[0]
    monkeypatch.setattr(producer, "_commit_declared_slot_completion", original)
    result = _execute(env)  # Explicit test/operator action, never an automatic retry.
    assert result.to_dict()["terminal_stop"] is False
    assert state.calls == list(range(1, 14))
    assert _progress(env).slot_commit_artifacts[0] == first


def _historical_executor_capacity_fixture(tmp_path, monkeypatch, *, persistent_units):
    """Unissued metadata fixture; exercise storage/error routing, never fit authority."""
    from datetime import datetime, timedelta
    import pm_authoritative_search_execution as execution
    from tests import test_declared_slot_owner_surface as declared

    monkeypatch.setattr(declared._TrackedStructuralFitted, '_read_source',
        lambda self: {'symbol': 'AUDUSD', 'timeframe': 'H1'})
    env = _declared_environment(tmp_path, monkeypatch)
    live, fitted, config, plan = declared._structural_live_capture(env, monkeypatch)
    classification = 'SYNTHETIC_RESOURCE_ROUTING_NO_NATIVE_FIT_OR_CONTINUATION_AUTHORITY'
    events = []
    snapshot_marker = {'classification': classification, 'role': 'not-a-valid-bound-reference'}
    def marker_entry(role):
        return live.sink.put_json(live.raw_artifact_namespace + '/resource-fixture-' + role,
            'synthetic_resource_fixture_marker', {'classification': classification, 'role': role})

    # Extend metadata in place; the real declared opener/tag verifier stays in use.
    fitted.source_group_decision.receipt_sha256 = '3' * 64
    fitted.regime_fit_decision.receipt_sha256 = '4' * 64
    fitted.frame_sha256 = '5' * 64
    fitted.execution_spec_receipt = {'classification': classification}
    fitted.product = SimpleNamespace(index={'activation': {'manifest_sha256': '6' * 64}})
    fitted.source_contract_seeds = {tier: {'classification': classification} for tier in ('P75', 'P95')}
    fitted.frozen_tier_pair_contract = {'classification': classification}
    horizon = SimpleNamespace(receipt_sha256='7' * 64,
        to_dict=lambda: {'classification': classification, 'role': 'horizon'})
    plan.horizon_plan = horizon
    live._horizon_plans[(plan.timeframe, plan.regime)] = horizon
    assert plan.receipt_sha256 != horizon.receipt_sha256
    attempt_artifacts = ({'classification': classification, 'ordinal': 0},)
    plan.cell_receipt = {'classification': classification, 'role': 'cell'}
    plan.spool_entry = marker_entry('plan')
    plan.attempt_artifacts = lambda: attempt_artifacts
    plan.state_requirements = lambda: ()
    live.source_session._entry = marker_entry('source-session')
    assert adapter._verify_declared_slot_capture(live) is True

    if persistent_units:
        capture = live
    else:
        # Type-only carrier selects the unchanged nondeclared staging branch.
        capture = object.__new__(adapter.HistoricalSymbolSearchCapture)
        capture.context, capture.request = live.context, live.request
        capture.symbol, capture.sink = live.symbol, live.sink
        capture.raw_artifact_namespace = live.raw_artifact_namespace
        capture._active, capture._finished = None, False
    verified = execution._VerifiedAttempt(ordinal=0, plan_attempt={'status': 'PLANNED'},
        candidate={}, trial={}, artifacts=SimpleNamespace())
    plan_body = {'cell': {'symbol': 'AUDUSD', 'timeframe': 'H1', 'regime': 'TREND'}}
    folds = tuple({'fold_ordinal': ordinal} for ordinal in range(4))
    costs = {'schema_version': 'SYNTHETIC_RESOURCE_COST_MARKER', 'rows': []}
    pointers = {role: marker_entry(role)['descriptor'] for role in ('frozen', 'graph', 'cross')}
    handle = adapter.FrozenHistoricalCell(symbol=capture.symbol, timeframe='H1', regime='TREND',
        frozen=SimpleNamespace(attempt_ledger={'attempt_count': 1},
            precommitment={'latest_evidence_boundary_utc': '2020-01-01T00:00:00Z'},
            partition_cost_precommitments=costs, bound_fitted_state_entry=snapshot_marker),
        frozen_receipt_descriptor=pointers['frozen'], frozen_descriptor_graph=pointers['graph'],
        cross_binding_descriptor=pointers['cross'], artifact_namespace=capture.raw_artifact_namespace,
        _attempt_artifacts=attempt_artifacts, _horizon_execution_contracts=(), _source_count_receipts=(),
        _native_prefix_product=None, _cross_binding_bytes=b'', _capture_nonce='synthetic-resource-routing-only')
    payload = {'attempt_id': 'synthetic-raw-0', 'classification': classification}
    references = {'synthetic-reference-only': {'classification': classification}}

    # Deterministic metadata clock stays inside this synthetic request's own window.
    window = capture.request.to_dict()['source_access_window_utc']
    tick = datetime.fromisoformat(window['start_utc'].replace('Z', '+00:00'))
    def now():
        nonlocal tick
        value = tick.strftime('%Y-%m-%dT%H:%M:%SZ')
        assert window['start_utc'] <= value < window['end_exclusive_utc']
        tick += timedelta(seconds=1)
        return value
    monkeypatch.setattr(execution, '_now_utc_seconds', now)
    def replay(actual_fitted, supplied_config):
        assert actual_fitted is fitted and supplied_config is config
        events.append('reverify')
        return fitted
    monkeypatch.setattr(execution, '_reverify_fitted_timeframe', replay)
    def plan_inputs(actual, **kwargs):
        assert actual is capture and kwargs['search_process'] is env.process
        assert kwargs['cell_plan'] is plan
        return horizon, attempt_artifacts
    monkeypatch.setattr(execution, '_verified_historical_cell_plan_inputs', plan_inputs)
    def horizon_identity(actual, **kwargs):
        assert actual is capture and kwargs['horizon_plan'] is horizon
    monkeypatch.setattr(execution, '_verify_capture_horizon_plan_identity', horizon_identity)
    def envelope(**kwargs):
        assert kwargs == {'cell_plan': plan, 'fitted': fitted}
    monkeypatch.setattr(execution, '_reverify_cell_state_error_envelope', envelope)
    def roster(**kwargs):
        assert kwargs['plan'] is horizon and kwargs['attempt_artifacts'] is attempt_artifacts
        assert kwargs['fitted'] is fitted and kwargs['regime'] == 'TREND'
        return plan_body, (verified,)
    monkeypatch.setattr(execution, '_verify_attempt_roster', roster)
    monkeypatch.setattr(execution, '_winner_fold_plan', lambda attempts: folds)
    def snapshot(actual, actual_fitted, actual_folds, owner):
        assert persistent_units and actual is capture and actual_fitted is fitted and actual_folds is folds
        assert type(owner) is execution._FreshFitOwner and owner.fitted is fitted
        assert owner.original_fitted is fitted and owner.config is config
        assert owner.context_id == id(capture.context) and owner.request_sha256 == capture.request.receipt_sha256
        events.append('snapshot')
        return snapshot_marker
    monkeypatch.setattr(execution, '_publish_declared_bound_fit', snapshot)
    def precommit(**kwargs):
        assert kwargs['fitted'] is fitted and kwargs['attempts'] == (verified,)
        assert kwargs['source_observation_cache'] == {}
        events.append('costs')
        return costs
    monkeypatch.setattr(execution, '_partition_cost_precommitments', precommit)
    def freeze(actual, timeframe, regime, **kwargs):
        assert actual is capture and (timeframe, regime) == ('H1', 'TREND')
        assert kwargs['partition_cost_precommitments'] is costs
        if persistent_units:
            assert kwargs['bound_fitted_state_entry'] is snapshot_marker
        else:
            assert 'bound_fitted_state_entry' not in kwargs
        capture._active = handle
        events.append('freeze')
        return handle
    monkeypatch.setattr(type(capture), 'freeze_observed_cell', freeze)
    def raw_ids(actual_frozen, attempts):
        assert actual_frozen is handle.frozen and attempts == (verified,)
        return ('synthetic-raw-0',)
    monkeypatch.setattr(execution, '_raw_attempt_ids', raw_ids)
    def publish_references(sink, **kwargs):
        assert sink is capture.sink and kwargs['frozen'] is handle.frozen
        assert kwargs['source_observation_cache'] == {}
        events.append('references')
        return references
    monkeypatch.setattr(execution, '_publish_source_observation_references', publish_references)
    def evidence(**kwargs):
        assert kwargs['attempt'] is verified and kwargs['fitted'] is fitted
        assert kwargs['partition_cost_precommitments'] == costs
        assert kwargs['source_observation_references'] is references
        events.append('evidence')
        return dict(payload)
    monkeypatch.setattr(execution, '_build_planned_attempt_evidence', evidence)
    def forbidden_authority(*args, **kwargs):
        pytest.fail('Structural capacity fixture reached genuine fit/cold/native authority')
    monkeypatch.setattr(execution, 'fit_native_timeframe_for_search', forbidden_authority)
    monkeypatch.setattr(execution, 'reopen_bound_fitted_timeframe', forbidden_authority)
    monkeypatch.setattr(execution.current_refit, 'read_bound_fitted_state_reference', forbidden_authority)
    monkeypatch.setattr(raw, 'publish_source_observation_receipt', forbidden_authority)
    monkeypatch.setattr(adapter, 'verify_declared_slot_completion', forbidden_authority)
    return SimpleNamespace(execution=execution, env=env, capture=capture, fitted=fitted,
        config=config, plan=plan, verified=verified, handle=handle, payload=payload,
        snapshot_marker=snapshot_marker, references=references, events=events)


def _raise_executor_capacity(case, monkeypatch, kind):
    execution, capture = case.execution, case.capture
    fault = _capacity(wrapped=True)
    if kind == 'raw':
        fault = _caused(raw.WinnerRawEvidenceIncomplete('SYNTHETIC_RAW_PUBLICATION_FAILURE'), fault)
    def forbidden(*args, **kwargs):
        pytest.fail('Capacity refusal reached terminal failure publication')
    monkeypatch.setattr(execution, '_record_historical_raw_failure', forbidden)
    monkeypatch.setattr(type(capture), 'record_search_infrastructure_failure', forbidden)
    def refuse(actual, actual_handle, **kwargs):
        assert actual is capture and actual_handle is case.handle
        # Read back the actual persistent unit or transient staged payload first.
        assert list(kwargs['attempt_evidence']) == [case.payload]
        case.events.append('raw')
        raise fault
    monkeypatch.setattr(type(capture), 'complete_observed_cell', refuse)
    with pytest.raises(type(fault)) as caught:
        execution.execute_historical_observed_cell(capture=capture, fitted=case.fitted,
            regime='TREND', search_process=case.env.process, cell_plan=case.plan, config=case.config)
    assert caught.value is fault
    assert capture._active is case.handle and capture._finished is False
    return fault


@pytest.mark.parametrize('kind', ['spool', 'raw'])
def test_historical_executor_capacity_escape_precedes_failure_publication(tmp_path, monkeypatch, kind):
    """Declared F units survive typed capacity without a terminal STOP or synthetic authority."""
    case = _historical_executor_capacity_fixture(tmp_path, monkeypatch, persistent_units=True)
    _raise_executor_capacity(case, monkeypatch, kind)
    execution, capture = case.execution, case.capture
    assert case.events == ['reverify', 'snapshot', 'costs', 'freeze', 'references', 'evidence', 'raw']
    body = capture.request.to_dict()
    header = execution._read_declared_unit_header(capture.context, body)
    assert header['fitted_state_entry'] == case.snapshot_marker
    assert header['binding']['cell_plan_receipt_sha256'] == case.plan.receipt_sha256
    assert header['binding']['horizon_plan_receipt_sha256'] == case.plan.horizon_plan.receipt_sha256
    assert header['qualification_authority'] is header['live_authority'] is False
    binding_sha = execution._sha256(header['binding'])
    unit = execution._declared_unit_identity(case.verified, case.payload['attempt_id'])
    records = execution._declared_completed_prefix(capture.context, body, binding_sha, [unit])
    assert len(records) == 1 and records[0]['unit'] == unit
    found = execution._read_declared_completed_unit(capture.context, body, binding_sha, unit)
    assert found == (records[0], case.payload)
    end = execution._read_declared_unit_json(capture.context,
        execution._declared_unit_relative(body, 'evidence-completed.json'))
    assert end['unit_count'] == 1 and end['unit_receipt_sha256'] == [execution._sha256(records[0])]
    assert capture._completion is None
    assert list(tmp_path.rglob('capacity-custody.json')) == []


@pytest.mark.parametrize('kind', ['spool', 'raw'])
def test_historical_transient_executor_capacity_keeps_legacy_custody(tmp_path, monkeypatch, kind):
    """The nondeclared executor retains its existing transient custody contract."""
    import json
    case = _historical_executor_capacity_fixture(tmp_path, monkeypatch, persistent_units=False)
    _raise_executor_capacity(case, monkeypatch, kind)
    assert case.events == ['reverify', 'costs', 'freeze', 'references', 'evidence', 'raw']
    receipts = list(tmp_path.rglob('capacity-custody.json'))
    assert len(receipts) == 1
    body = json.loads(receipts[0].read_text(encoding='ascii'))
    assert body['custody_state'] == 'CAPACITY_RETAINED_NONACTIVATING'
    assert body['completed_row_count'] == 1
    assert body['reuse_authority'] is body['resume_authority'] is False
    assert body['observed_capacity_failure'] == FREE_SPACE
    assert body['completed_row_metadata'][0]['sha256'] == case.execution._sha256(case.payload)
    assert case.execution._read_declared_unit_header(case.capture.context,
        case.capture.request.to_dict()) is None


def _executor_custody_case(tmp_path):
    import pm_authoritative_search_execution as execution
    context = spool.freeze_generation_context(tmp_path / 'spool',
        generation_id='SYNTHETIC-CUSTODY-NO-AUTHORITY',
        native_universe_sha256='1' * 64, config_sha256='2' * 64,
        limits=spool.SpoolLimits(min_free_bytes=0))
    stage = execution._create_stage_directory(context.root, '3' * 64)
    scope = spool.reserve_transient_publication(context)
    reservation = scope.__enter__()
    custody = execution._create_stage_capacity_custody(root=context.root, stage=stage,
        reservation=reservation, binding={
            'producer_request_sha256': '4' * 64, 'precommit_sha256': '5' * 64,
            'attempt_id': 'SYNTHETIC-NOT-ISSUED', 'generation_context_sha256': context.context_sha256,
        })
    row = execution._write_staged_attempt(stage=stage, ordinal=0,
        value={'attempt_id': 'SYNTHETIC-ROW', 'classification': 'SYNTHETIC-NONFINANCIAL-NO-AUTHORITY'},
        remaining_cell_bytes=execution.MAX_CELL_EVIDENCE_BYTES, reservation=reservation)
    return execution, context, stage, scope, reservation, custody, row


def test_capacity_custody_renames_keeps_bytes_and_abandons_accounting_without_cancel(tmp_path):
    import json
    execution, context, stage, scope, reservation, custody, row = _executor_custody_case(tmp_path)
    original = row.path.read_bytes()
    fault = _capacity(wrapped=True)
    execution._close_historical_reserved_stage(stage=stage, rows=[row], reservation=reservation,
        reservation_scope=scope, primary_error=fault, custody=custody,
        evidence_started='2020-01-01T00:00:00Z', evidence_completed='2020-01-01T00:00:01Z')
    assert not stage.exists()
    retained, = context.root.glob('.fxpm-capacity-custody-*')
    assert (retained / row.path.name).read_bytes() == original
    receipt = retained / 'capacity-custody.json'
    assert receipt.stat().st_size == execution.MAX_CAPACITY_CUSTODY_BYTES == 128 * 1024
    body = json.loads(receipt.read_text(encoding='ascii'))
    assert body['producer_request_sha256'] == '4' * 64
    assert body['precommit_sha256'] == '5' * 64
    assert body['completed_row_metadata'] == [{'ordinal': 0, 'kind': 'RAW_FOLDS',
        'sha256': row.sha256, 'size_bytes': row.size_bytes, 'file': row.path.name}]
    assert body['known_retained_bytes'] == len(original) + 128 * 1024
    assert body['reuse_authority'] is body['resume_authority'] is False
    assert all(body[k] is False for k in ('qualification_authority', 'capital_authority', 'live_authority', 'order_authority'))
    state = json.loads((context.root / quota.QUOTA_STATE_NAME).read_text())
    assert len(state['reservations']) == 1  # Original bytes have NOT been cancelled.
    # Fresh path creation is explicit; this does not search or reuse the retained row.
    fresh = execution._create_stage_directory(context.root, '3' * 64)
    assert fresh != retained
    execution._cleanup_stage(fresh, [])
    # The existing public reservation reconciles the abandoned guard from actual files.
    with spool.reserve_transient_publication(context) as next_reservation:
        next_reservation.cancel_after_cleanup()
    state = json.loads((context.root / quota.QUOTA_STATE_NAME).read_text())
    assert state['reservations'] == []
    assert state['committed_bytes'] >= body['known_retained_bytes']
    assert (retained / row.path.name).read_bytes() == original


@pytest.mark.parametrize('failure', ['success', 'integrity', 'generation'])
def test_noncapacity_historical_stage_keeps_original_delete_and_cancel(tmp_path, failure):
    import json
    execution, context, stage, scope, reservation, custody, row = _executor_custody_case(tmp_path)
    fault = None
    if failure != 'success':
        code = GENERATION_BYTES if failure == 'generation' else 'SPOOL_QUOTA_STATE_DIGEST_MISMATCH'
        inner = quota.AuthoritySpoolQuotaError(code)
        fault = _caused(spool.AuthoritySpoolError(str(inner)), inner)
    execution._close_historical_reserved_stage(stage=stage, rows=[row], reservation=reservation,
        reservation_scope=scope, primary_error=fault, custody=custody,
        evidence_started=None, evidence_completed=None)
    assert not stage.exists()
    assert not list(context.root.glob('.fxpm-capacity-custody-*'))
    assert json.loads((context.root / quota.QUOTA_STATE_NAME).read_text())['reservations'] == []


def test_custody_refresh_failure_preserves_rows_and_original_capacity(tmp_path, monkeypatch):
    execution, context, stage, scope, reservation, custody, row = _executor_custody_case(tmp_path)
    original = row.path.read_bytes()
    fault = _capacity(wrapped=True)
    def fail(*args, **kwargs):
        raise OSError('SYNTHETIC_DIAGNOSTIC_WRITE_FAILURE')
    monkeypatch.setattr(execution, '_refresh_capacity_custody', fail)
    execution._close_historical_reserved_stage(stage=stage, rows=[row], reservation=reservation,
        reservation_scope=scope, primary_error=fault, custody=custody,
        evidence_started=None, evidence_completed=None)
    retained, = context.root.glob('.fxpm-capacity-custody-*')
    assert (retained / row.path.name).read_bytes() == original
    assert 'CAPACITY_CUSTODY_REFRESH_FAILED:OSError' in fault.__notes__
    assert fault.__cause__.code == FREE_SPACE


def test_capacity_retirement_refuses_target_outside_original_context(tmp_path):
    execution, context, stage, scope, reservation, custody, row = _executor_custody_case(tmp_path)
    target = tmp_path / 'outside-context'
    with pytest.raises(execution.AuthoritativeSearchExecutionError) as caught:
        execution._retire_capacity_stage_path(stage, target, custody)
    assert caught.value.code == 'CAPACITY_CUSTODY_PATH_INVALID'
    assert stage.is_dir() and not target.exists()
    execution._close_historical_reserved_stage(stage=stage, rows=[row], reservation=reservation,
        reservation_scope=scope, primary_error=None, custody=custody,
        evidence_started=None, evidence_completed=None)


def test_quota_rescan_counts_distinct_actual_files_separately(tmp_path):
    import os
    root = tmp_path / "distinct-owned-files"
    nested = root / "nested"
    nested.mkdir(parents=True)
    first, second = root / "first.bin", nested / "second.bin"
    first.write_bytes(b"A" * 13)
    second.write_bytes(b"B" * 13)
    one, two = (os.stat(path, follow_symlinks=False) for path in (first, second))
    assert (one.st_dev, one.st_ino) != (two.st_dev, two.st_ino)
    assert quota._scan_committed_bytes(root) == 26


def test_quota_rescan_counts_actual_hardlink_alias_only_once(tmp_path):
    import os
    root = tmp_path / "hardlink-owned-files"
    root.mkdir()
    original, second, alias = root / "original.bin", root / "second.bin", root / "alias.bin"
    original.write_bytes(b"A" * 13)
    second.write_bytes(b"B" * 17)
    os.link(original, alias)
    one, two, linked = (os.stat(path, follow_symlinks=False) for path in (original, second, alias))
    assert (one.st_dev, one.st_ino) != (two.st_dev, two.st_ino)
    assert (one.st_dev, one.st_ino) == (linked.st_dev, linked.st_ino)
    assert quota._scan_committed_bytes(root) == 30


@pytest.mark.parametrize("kind", ["reparse", "nonregular"])
def test_quota_rescan_preserves_initial_entry_barriers(tmp_path, monkeypatch, kind):
    import os
    import stat
    root = tmp_path / "initial-entry-guard"
    root.mkdir()
    target = root / "synthetic-entry.bin"
    target.write_bytes(b"owned metadata only")
    actual = os.stat(target, follow_symlinks=False)
    info = SimpleNamespace(st_mode=(stat.S_IFIFO | 0o600) if kind == "nonregular" else actual.st_mode,
        st_dev=actual.st_dev, st_ino=actual.st_ino, st_size=actual.st_size)
    entry = SimpleNamespace(path=str(target), stat=lambda **kwargs: info,
        is_symlink=lambda: kind == "reparse")

    class Entries(list):
        def __enter__(self):
            return self
        def __exit__(self, *args):
            return None

    real_scandir = os.scandir
    monkeypatch.setattr(quota.os, "scandir",
        lambda path: Entries([entry]) if Path(path) == root else real_scandir(path))
    with pytest.raises(quota.AuthoritySpoolQuotaError) as caught:
        quota._scan_committed_bytes(root)
    assert caught.value.code == {"reparse": "SPOOL_QUOTA_REPARSE_ENTRY_FORBIDDEN",
        "nonregular": "SPOOL_QUOTA_NONREGULAR_ENTRY_FORBIDDEN"}[kind]


@pytest.mark.parametrize("fault", ["removed", "nonregular", "reparse", "zero"])
def test_quota_rescan_rejects_fresh_stat_disagreement(tmp_path, monkeypatch, fault):
    import os
    import stat
    root = tmp_path / "fresh-path-stat-guard"
    root.mkdir()
    target = root / "actual-regular-file.bin"
    target.write_bytes(b"actual initial regular entry")
    real_stat, observed = os.stat, []

    def fresh(path, *args, **kwargs):
        if Path(path) != target:
            return real_stat(path, *args, **kwargs)
        assert kwargs.get("follow_symlinks") is False
        observed.append(fault)
        if fault == "removed":
            target.unlink()  # A real disappearance after the cached entry checks.
            return real_stat(path, *args, **kwargs)
        actual = real_stat(path, *args, **kwargs)
        return SimpleNamespace(st_mode=(stat.S_IFDIR | 0o700) if fault == "nonregular" else actual.st_mode,
            st_file_attributes=0x400 if fault == "reparse" else getattr(actual, "st_file_attributes", 0),
            st_dev=0 if fault == "zero" else actual.st_dev,
            st_ino=0 if fault == "zero" else actual.st_ino, st_size=actual.st_size)

    monkeypatch.setattr(quota, "os", SimpleNamespace(stat=fresh, scandir=os.scandir))
    with pytest.raises(quota.AuthoritySpoolQuotaError) as caught:
        quota._scan_committed_bytes(root)
    assert caught.value.code == "SPOOL_QUOTA_RESCAN_FAILED"
    assert observed == [fault]
