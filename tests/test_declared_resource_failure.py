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


@pytest.mark.parametrize('kind', ['spool', 'raw'])
def test_historical_executor_capacity_escape_precedes_failure_publication(tmp_path, monkeypatch, kind):
    import json
    import pm_authoritative_search_execution as execution
    from tests import test_authoritative_search_execution as existing
    fault = _capacity(wrapped=True)
    if kind == 'raw':
        fault = _caused(raw.WinnerRawEvidenceIncomplete('SYNTHETIC_RAW_PUBLICATION_FAILURE'), fault)
    def forbidden(*args, **kwargs):
        pytest.fail('Capacity refusal reached terminal failure publication')
    def after_freeze(_references):
        # The existing setup's source/model/cost pointers are synthetic doubles.
        monkeypatch.setattr(execution, '_build_planned_attempt_evidence', lambda **kwargs: {
            'attempt_id': 'synthetic-raw-0',
            'classification': 'SYNTHETIC_RESOURCE_FAULT_NO_RAW_AUTHORITY',
        })
        def refuse(actual, *args, **kwargs):
            raise fault
        monkeypatch.setattr(execution.historical_adapter.DeclaredSlotSearchCapture,
                            'complete_observed_cell', refuse)
        return {'synthetic-reference-only': '1' * 64}
    monkeypatch.setattr(execution, '_require_source_observation_references', after_freeze)
    monkeypatch.setattr(execution, '_record_historical_raw_failure', forbidden)
    with pytest.raises(type(fault)) as caught:
        existing.test_declared_executor_empty_reference_set_records_stop_with_frozen_pointers(tmp_path, monkeypatch)
    assert caught.value is fault
    receipts = list(tmp_path.rglob('capacity-custody.json'))
    assert len(receipts) == 1
    body = json.loads(receipts[0].read_text(encoding='ascii'))
    assert body['custody_state'] == 'CAPACITY_RETAINED_NONACTIVATING'
    assert body['completed_row_count'] == 1
    assert body['reuse_authority'] is body['resume_authority'] is False
    assert body['observed_capacity_failure'] == FREE_SPACE


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
