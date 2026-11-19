"""Synthetic routing/ownership tests; no source or financial run authority."""
from types import SimpleNamespace
import threading
import json
import hashlib

import pytest

import pm_current_refit_search_producer as producer


def _edition(independent=True):
    return SimpleNamespace(independent_signal_r_enabled=independent,
        edition_id=producer.planner.INDEPENDENT_R_SOURCE_EDITION,
        winner_generation=producer.winner.V19_GENERATION)


def test_explicit_current_facade_materializes_once_and_consumes_before_execution(monkeypatch):
    value = object.__new__(producer.CurrentRefitSearchProducer)
    calls = []
    value._search_process = object()
    value._context = object()
    value._frozen_config = SimpleNamespace(materialize=lambda context: calls.append(context) or 'config')
    value._native_products = {}
    value._current_selection_window = {}
    value._output_root = 'output'
    value._symbol_scheduler = None
    value._lock = threading.Lock()
    value._used = False
    monkeypatch.setattr(producer.planner, 'source_edition_info', lambda _: _edition())
    monkeypatch.setattr(producer, '_request_body', lambda request: request)
    def execute(inputs, request):
        assert value._used is True
        assert inputs.search_process is value._search_process
        assert inputs.pipeline_config == 'config'
        assert request == {'authenticated_request': True}
        return 'v19-result'
    monkeypatch.setattr(producer, '_produce_independent_r', execute)
    assert value({'authenticated_request': True}) == 'v19-result'
    with pytest.raises(producer.CurrentRefitSearchProducerError) as caught:
        value({'authenticated_request': True})
    assert caught.value.code == 'CURRENT_REFIT_PRODUCER_ALREADY_CONSUMED'
    assert calls == [value._context]


def test_legacy_current_entry_remains_retired_before_config_and_source(monkeypatch):
    value = object.__new__(producer.CurrentRefitSearchProducer)
    value._search_process = object()
    value._used = False
    monkeypatch.setattr(producer.planner, 'source_edition_info', lambda _: _edition(False))
    for invoke in (lambda: value(object()), lambda: producer._produce(object(), object())):
        with pytest.raises(producer.CurrentRefitSearchProducerError) as caught:
            invoke()
        assert caught.value.code == 'RETIRED_COST_POLICY_INTERVAL_QUALIFICATION_REPLACEMENT_REQUIRED'
        assert value._used is False


def test_current_timeframe_fits_once_for_four_authorized_regimes_and_expires_owners(monkeypatch):
    """Invented authority/acquisition seams; actual fresh-fit routing and lifetime."""
    import pm_core
    execution = producer.search_execution
    context = object()
    sink = object.__new__(producer.authority_spool.SymbolAuthoritySink)
    sink.context = context
    process = SimpleNamespace(receipt_sha256="1" * 64)
    config = pm_core.PipelineConfig(research_mode=True, swap_model_enabled=False)
    counts = {"fit":0,"authority":0}
    seen = []
    monkeypatch.setattr(producer.planner, "source_edition_info", lambda *a: _edition())
    def fit(**kw):
        counts["fit"] += 1
        assert kw["_numeric_config_sha256"] == execution._independent_r_configuration_sha256(config)
        value = SimpleNamespace(train_regime_support={"counts":{r:1 for r in producer.book.REGIME_ORDER}},
            source_group_decision=object(), regime_fit_decision=object(), _bound_fit_owner=None,
            _numeric_preparation=kw["_numeric_preparation"])
        seen.append(value)
        return value
    monkeypatch.setattr(execution, "fit_native_timeframe_for_search", fit)
    class Plan:
        mode = "CURRENT_REFIT"
        native_prefix_product = fitted_timeframe = None
        horizon_plan = object()
        @property
        def cell_receipt(self):
            raise RuntimeError("INVENTED_STOP_AFTER_REAL_AUTHORITY_AND_FRESH_FIT_ROUTING")
    monkeypatch.setattr(producer.planner, "build_current_observed_cell_plan", lambda *a, **kw: Plan())
    def authority(*a, **kw):
        counts["authority"] += 1
        return True
    monkeypatch.setattr(producer.planner, "verify_production_cell_search_plan", authority)
    def no_refit(*a, **kw):
        raise AssertionError("the already owned timeframe must not reopen/refit per regime")
    monkeypatch.setattr(execution, "_reverify_fitted_timeframe", no_refit)
    issuers = []
    def execute(**kw):
        issuers.append(kw["_fresh_fit_owner"])
        with pytest.raises(execution.AuthoritativeSearchExecutionError) as caught:
            execution._verified_current_observed_inputs(**kw)
        assert caught.value.code == "CURRENT_CELL_PLAN_REOPEN_FAILED"
        return SimpleNamespace(production=None)
    monkeypatch.setattr(execution, "execute_independent_r_current_observed_cell", execute)
    monkeypatch.setattr(producer, "_state_evidence", lambda **kw: kw)
    evidence, selected, artifacts = producer._evaluate_current_timeframe(context=context,
        process=process, authorization=object(), sink=sink, config=config,
        product=SimpleNamespace(source_state=producer.native_prefix.AVAILABLE), symbol="INVENTED", timeframe="H1")
    assert counts == {"fit":1,"authority":4}
    assert len(evidence) == 4 and selected == artifacts == {}
    assert len({id(owner) for owner in issuers}) == 1
    assert seen[0]._numeric_preparation._closed is True
    assert issuers[0].seal is None and issuers[0].fitted is None
    with pytest.raises(execution.AuthoritativeSearchExecutionError) as caught:
        execution._verified_current_observed_inputs(process=process, sink=sink,
            cell_plan=Plan(), fitted=seen[0], config=config, _fresh_fit_owner=issuers[0])
    assert caught.value.code == "CURRENT_FRESH_FIT_OWNER_INVALID"
    assert counts["authority"] == 5
def test_current_v19_reaches_matched_floor_book_after_causal_authorization(monkeypatch):
    context = object.__new__(producer.authority_spool.GenerationSpoolContext)
    process = object.__new__(producer.planner.FrozenProductionSearchProcess)
    config = producer.PipelineConfig()
    inputs = producer._ExecutableCurrentRefitInputs(context=context, search_process=process,
        pipeline_config=config, native_products={}, current_selection_window={}, output_root='output', symbol_scheduler=None)
    decision = {'decision': 'OFFLINE_PROCESS_VALIDATION_PASS'}
    request = {'causal_precommit_payload': {}, 'causal_evidence_payload': {},
        'causal_reconciliation_payload': {}, 'historical_process_decision': decision,
        'historical_process_decision_sha256': '1' * 64}
    order = []
    monkeypatch.setattr(producer.planner, 'verify_frozen_production_search_process', lambda *args: order.append('verify'))
    monkeypatch.setattr(producer.planner, 'source_edition_info', lambda _: _edition())
    monkeypatch.setattr(producer, 'verify_causal_process_pass', lambda **kwargs: {'decision_body': decision, 'decision_sha256': '1' * 64})
    monkeypatch.setattr(producer.causal, 'canonical_sha256', lambda _: '1' * 64)
    monkeypatch.setattr(producer, '_current_cutoff', lambda _: '2026-01-01T00:00:00Z')
    def authorize(*args, **kwargs):
        order.append('authorize')
        return object()
    monkeypatch.setattr(producer.planner, 'freeze_current_search_authorization', authorize)
    monkeypatch.setattr(producer, '_book_evidence_stream', lambda *args, **kwargs: iter(('fresh-current-evidence',)))
    class ReachedV19(Exception):
        pass
    def evaluate(evidence, *, winner_generation):
        assert list(evidence) == ['fresh-current-evidence']
        assert winner_generation.selection_version == producer.winner.V19_GENERATION.selection_version
        assert order == ['verify', 'authorize']
        raise ReachedV19
    monkeypatch.setattr(producer.book, 'evaluate_matched_floor_historical_book_multiplicity', evaluate)
    with pytest.raises(ReachedV19):
        producer._produce_independent_r(inputs, request)


@pytest.mark.parametrize('edition_id', [producer.planner.INDEPENDENT_R_SOURCE_EDITION, 'UNKNOWN'])
def test_worker_reconstructs_explicit_v19_and_refuses_unknown_edition(monkeypatch, edition_id):
    fields = ('spool_root generation_id native_universe_sha256 config_sha256 generation_context_sha256 run_id '
        'search_process_receipt_sha256 authorization_receipt_sha256 active_config_path effective_config_sha256 '
        'data_dir current_cutoff_utc source_accessed_at_utc causal_precommit_payload causal_evidence_payload '
        'causal_reconciliation_payload symbol_roster_sha256 timeframe_roster_sha256').split()
    contract = dict.fromkeys(fields, 'synthetic')
    contract.update(schema_version=producer.INDEPENDENT_R_SYMBOL_SCHEDULER_SCHEMA,
        source_edition_id=edition_id, symbol_roster_sha256=producer._sha(list(producer.book.PINNED_SYMBOL_ORDER)),
        timeframe_roster_sha256=producer._sha(list(producer.book.TIMEFRAME_ORDER)))
    context = SimpleNamespace(context_sha256='synthetic')
    calls = []
    monkeypatch.setattr(producer.authority_spool, 'reopen_generation_context', lambda *args, **kwargs: calls.append('context') or context)
    class ReachedV19(Exception):
        pass
    def freeze(actual_context, *, run_id):
        assert actual_context is context and run_id == 'synthetic'
        raise ReachedV19
    monkeypatch.setattr(producer.planner, 'freeze_independent_signal_r_production_search_process', freeze)
    if edition_id == producer.planner.INDEPENDENT_R_SOURCE_EDITION:
        with pytest.raises(ReachedV19):
            producer._current_refit_symbol_worker(contract, producer.book.PINNED_SYMBOL_ORDER[0])
        assert calls == ['context']
    else:
        with pytest.raises(producer.CurrentRefitSearchProducerError) as caught:
            producer._current_refit_symbol_worker(contract, producer.book.PINNED_SYMBOL_ORDER[0])
        assert caught.value.code == 'CURRENT_WORKER_CONTRACT_ROSTER_INVALID'
        assert calls == []


@pytest.mark.parametrize("tamper", (False, True), ids=("actual-source-pass", "actual-source-wrong-branch"))
def test_current_result_consumer_reaches_real_cold_source_before_book(monkeypatch, tmp_path, tamper):
    """Consumer integration with I/O/restore stubs and actual M1 source math.

    The reused source fixture is explicitly invented and unqualified. This does
    not claim a native/profile issuance or an end-to-end account run.
    """
    from test_independent_signal_r_source_replay import _source_case, _facts, _verify

    case = _source_case(monkeypatch, earlier=True, ambiguous=True)
    facts, geometry = _facts(case)
    if tamper:
        row = facts["batches"]["P75"]["records"][0]
        assert row["exit_reason"] == "SL"
        row.update(exit_reason="TP", exit_fill="TARGET_LEVEL", exit_price=row["take_profit_price"])
    context = SimpleNamespace(root=tmp_path, context_sha256="c" * 64, generation_id="INVENTED_GENERATION",
        native_universe_sha256="n" * 64, config_sha256="d" * 64,
        limits=SimpleNamespace(max_artifact_bytes=1000000))
    generation = producer.winner.V19_GENERATION
    payload = SimpleNamespace(to_dict=lambda: {"contract_version": generation.precommitment_version})
    request = SimpleNamespace(precommitment=payload, request_sha256="e" * 64)
    request.to_dict = lambda: {"contract_version": generation.request_version}
    decision = SimpleNamespace(receipt_sha256="f" * 64, to_dict=lambda: {"INVENTED_DECISION": True})
    process, frozen_config, frozen = object(), object(), object()
    cross = {"schema_version": producer.raw_winner.horizon_winner_cross_binding_schemas_for_generation(generation)[1],
        "mode": producer.raw_winner.CURRENT_CROSS_BINDING_MODE}
    cross_bytes = producer._canonical_bytes(cross)
    entries = {key: {"artifact_id": key, "artifact_type": value, "descriptor": {"sha256": key}}
        for key, value in (("winner_request", "winner_v19_request"), ("winner_decision", "winner_v19_decision"),
            ("production_receipt", "winner_raw_evidence_production_receipt"), ("cross_binding", "current_horizon_winner_cross_binding"))}
    body = {"schema_version": producer.raw_winner.raw_production_schema_for_generation(generation),
        "artifact_namespace": "declared-slot/INVENTED_CURRENT", "generation_context_sha256": context.context_sha256,
        "symbol": case["spec"].symbol, "timeframe": "H1", "regime": "TREND", "request_sha256": request.request_sha256,
        "decision_receipt_sha256": decision.receipt_sha256,
        "provenance_cross_binding_sha256": hashlib.sha256(cross_bytes).hexdigest(),
        "artifact_roster": {"winner-request": entries["winner_request"]["descriptor"],
            "winner-decision": entries["winner_decision"]["descriptor"]}}
    prefix = producer.raw_winner._artifact_prefix(body["artifact_namespace"], "H1", "TREND")
    manifest = {"artifacts": [{"artifact_id": prefix + "/frozen/receipt", "descriptor": {"sha256": "INVENTED"}}]}
    calls = []
    monkeypatch.setattr(producer, "_resolve_raw_artifacts", lambda *args, **kwargs: entries)
    monkeypatch.setattr(producer, "_read_spooled_ascii_object", lambda context, entry, **kwargs:
        (request.to_dict(), b"{}") if entry is entries["winner_request"] else
        (decision.to_dict(), b"{}") if entry is entries["winner_decision"] else (body, b"{}"))
    monkeypatch.setattr(producer, "_read_spooled_bytes", lambda context, entry, **kwargs:
        cross_bytes if entry is entries["cross_binding"] else b"{}")
    monkeypatch.setattr(producer, "_rebuild_winner_request", lambda *args: request)
    monkeypatch.setattr(producer.winner, "evaluate_historical_cell_winner", lambda value: decision)
    monkeypatch.setattr(producer.winner, "verify_historical_cell_winner_decision", lambda *args: True)
    monkeypatch.setattr(producer.planner, "source_edition_info", lambda value:
        SimpleNamespace(winner_generation_id=generation.selection_version))
    def restore(**kwargs):
        assert kwargs["manifest_entries"] == manifest["artifacts"]
        calls.append("restore")
        return frozen
    monkeypatch.setattr(producer.raw_winner, "bound_frozen_plan_restorer_for_generation", lambda value: restore)
    material = (case["frame"], case["spec"], case["tiers"], case["config"], case["observations"])
    def source(context, **kwargs):
        assert kwargs["request"] is None and kwargs["frozen"] is frozen
        assert kwargs["process"] is process and kwargs["frozen_pipeline_config"] is frozen_config
        kwargs["_bound_fit_metadata_out"].update({"provenance": "INVENTED_VERIFIED_BOUNDARY"})
        calls.append("source")
        return material
    monkeypatch.setattr(producer.search_execution, "_reopen_route_a_matched_source_material", source)
    def cold(context, **kwargs):
        assert kwargs["producer_request"] is None and kwargs["source_material"] is material
        assert kwargs["bound_fit_metadata"] == {"provenance": "INVENTED_VERIFIED_BOUNDARY"}
        calls.append("cold")
        return _verify(case, facts, geometry)
    monkeypatch.setattr(producer.raw_winner, "verify_matched_floor_publication_chain", cold)
    args = dict(context=context, manifest=manifest, bindings={}, symbol=case["spec"].symbol,
        timeframe="H1", regime="TREND", expected_decision_sha256=decision.receipt_sha256,
        search_process=process, frozen_pipeline_config=frozen_config)
    if tamper:
        with pytest.raises(producer.CurrentRefitSearchProducerError) as caught:
            producer._rebuild_raw_winner_artifacts(**args)
        assert caught.value.code == "CURRENT_WORKER_INDEPENDENT_COLD_REPLAY_FAILED"
        assert "INDEPENDENT_R_OUTCOME_SOURCE_CHANGED" in caught.value.details
    else:
        result = producer._rebuild_raw_winner_artifacts(**args)
        assert result[:2] == (request, decision)
    assert calls == ["restore", "source", "cold"]
