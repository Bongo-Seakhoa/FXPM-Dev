"""INVENTED software metadata; no Native, model, owner or financial proof.

The process and artifact store use normal metadata issuers. Request/state
authority callbacks are explicit stand-ins. Receipt/member/horizon verification
and stored-file integrity checks run through the real shared reader.
"""
from copy import deepcopy
import json
from types import SimpleNamespace

import pytest

import pm_authority_spool as spool
import pm_historical_anchor_orchestration as historical
import pm_historical_production_adapter as adapter
import pm_production_search_planner as planner
import pm_training_horizon as horizon
from tests.test_production_search_planner import (
    _fake_state_domain_records,
    _state_error_envelope_receipt,
)
from tests.test_training_horizon import _request as invented_horizon_request


@pytest.fixture
def native_saved_plan(tmp_path, monkeypatch):
    label = "INVENTED_NATIVE_COLD_RECEIPT_IO_ONLY"
    digest = lambda role: planner._sha({"fixture": label, "role": role})
    context = spool.freeze_generation_context((tmp_path / "spool").resolve(),
        generation_id="invented-native-cold-plan", native_universe_sha256=digest("native"),
        config_sha256=digest("config"))
    process = planner.freeze_production_search_process(context, run_id="invented-native-cold-plan")
    horizon_request = invented_horizon_request()
    horizon_request["cell"]["symbol"] = "EURUSD"
    horizon_request["native_prefix"]["symbol"] = "EURUSD"
    horizon_plan = horizon.build_historical_horizon_plan(horizon_request)
    native = deepcopy(horizon_plan.to_dict()["native_prefix"])
    symbol, timeframe, regime = "EURUSD", "H1", "TREND"
    namespace = "INVENTED-NATIVE-COLD"
    body = {"schema_version": historical.DECLARED_SLOT_REQUEST_SCHEMA,
        "artifact_namespace": namespace, "precommit_sha256": digest("causal-precommit"),
        "process_precommit_receipt_sha256": digest("causal-precommit"),
        "symbol": symbol, "timeframe": timeframe, "regime": regime, "cell_id": digest("cell"),
        "cell": {"cell_id": digest("cell"), "symbol": symbol, "timeframe": timeframe, "regime": regime}}
    request = SimpleNamespace(receipt_sha256=planner._sha(body), to_dict=lambda: deepcopy(body))
    monkeypatch.setattr(historical, "verify_declared_slot_producer_request", lambda *a, **k: deepcopy(body))
    monkeypatch.setattr(planner, "_verify_state_error_envelope_fitted_binding", lambda *a, **k: None)
    monkeypatch.setattr(planner, "_verify_state_context_plan_binding", lambda **k: None)
    parameters = planner.pm_regime.DEFAULT_PARAMS_BY_TIMEFRAME[timeframe].to_dict()
    parameters_sha = planner.pm_regime.regime_params_sha256(parameters)
    fitted = SimpleNamespace(product=SimpleNamespace(index_sha256=digest("index"), native_prefix={
        **native, "symbol": symbol, "timeframe": timeframe}, instrument_snapshot={"parity_tick_tolerance": 0.01}),
        source_group_decision=SimpleNamespace(receipt_sha256=digest("source")),
        regime_fit_decision=SimpleNamespace(receipt_sha256=digest("fit"),
            to_dict=lambda: {"regime_params_sha256": parameters_sha}),
        train_regime_support={"receipt_sha256": digest("support")}, regime_parameters=parameters,
        instrument_spec=SimpleNamespace(tick_size=0.01))
    attempts = [{"fixture": label, "ordinal": i} for i in range(planner.ATTEMPT_COUNT)]
    states = []
    for i in range(planner.ATTEMPT_COUNT):
        state = {"fixture": label, "ordinal": i}
        states.append({**state, "requirement_sha256": planner._sha(state)})
    sink = spool.open_symbol_sink(context, symbol)
    horizon_entry = sink.put_json(namespace + "/horizon-plan", "declared_slot_horizon_plan", horizon_plan.to_dict())
    publications = []
    original_put = spool.SymbolAuthoritySink.put_json
    def record_publication(self, artifact_id, artifact_type, value):
        if artifact_type == planner.CELL_PLAN_ARTIFACT_TYPE:
            publications.append(artifact_id)
        return original_put(self, artifact_id, artifact_type, value)
    monkeypatch.setattr(spool.SymbolAuthoritySink, "put_json", record_publication)
    receipt_json, entry_json = planner._seal_cell_plan(context, sink=sink, mode="DECLARED_SLOT_UNISSUED",
        namespace=namespace, process=process, causal_precommit_receipt_sha256=body["precommit_sha256"],
        fitted=fitted, regime=regime, horizon_plan=horizon_plan, candidate={"candidate_id": "INVENTED"},
        dependency_payload={"fixture": label}, dependency_geometry={"fixture": label},
        attempt_artifacts=attempts, state_requirements=states,
        state_domain_records=_fake_state_domain_records((20,)), state_context_selection={},
        state_error_envelope=_state_error_envelope_receipt(regime_parameters_sha256=parameters_sha,
            regime_warmup_bars=planner._regime_warmup_contract(fitted)[0]),
        boundary_limited=False, declared_slot_request=request)
    entry = json.loads(entry_json)
    assert publications == [entry["artifact_id"]]
    completion = {"plan_transport": {"cell_receipt": json.loads(receipt_json), "spool_entry": entry,
        "attempt_artifacts": attempts, "state_requirements": states},
        "horizon_plan": {"descriptor": horizon_entry["descriptor"], "receipt_sha256": horizon_plan.receipt_sha256}}
    reads, forbidden_publications = [], []
    original_stream = spool._stream_file
    def stream(root, relative, **kwargs):
        if relative == entry["descriptor"]["relative_path"]:
            reads.append(relative)
        return original_stream(root, relative, **kwargs)
    def no_publication(*args, **kwargs):
        forbidden_publications.append(True)
        raise AssertionError("Cold restore attempted publication")
    monkeypatch.setattr(spool, "_stream_file", stream)
    monkeypatch.setattr(spool.SymbolAuthoritySink, "put_json", no_publication)
    return SimpleNamespace(context=context, process=process, request=request, sink=sink,
        completion=completion, original_entry=entry, original_receipt=json.loads(receipt_json),
        horizon_plan=horizon_plan, reads=reads, forbidden_publications=forbidden_publications,
        fresh_publications=publications, tmp_path=tmp_path)


def _restore(case):
    return adapter._restore_declared_slot_plan(case.context, request=case.request, process=case.process,
        completion=case.completion, sink=case.sink)


def test_native_cold_plan_reuses_original_once_without_publication(native_saved_plan):
    case = native_saved_plan
    restored = _restore(case)
    assert restored.cell_receipt == case.original_receipt
    assert restored.spool_entry == case.original_entry
    assert restored.horizon_plan.to_dict() == case.horizon_plan.to_dict()
    assert len(restored.attempt_artifacts()) == planner.ATTEMPT_COUNT
    assert len(case.reads) == 1
    assert case.forbidden_publications == []
    assert len(case.fresh_publications) == 1


@pytest.mark.parametrize("damage,reason,read_count", (
    ("unregistered", "DECLARED_SLOT_PLAN_ORIGINAL_MISSING", 0),
    ("missing", "M5_SAVED_PLAN_ORIGINAL_MISSING_OR_CHANGED", 1),
    ("tampered", "M5_SAVED_PLAN_ORIGINAL_MISSING_OR_CHANGED", 1),
    ("transport", "M5_SAVED_PLAN_ORIGINAL_CHANGED", 1),
    ("descriptor", "DECLARED_SLOT_PLAN_ORIGINAL_MISSING", 0),
    ("horizon", "WORKER_HORIZON_PLAN_CHANGED", 0),
), ids=("unregistered", "missing", "tampered", "transport", "descriptor", "horizon"))
def test_native_cold_plan_refuses_changed_history_without_recreation(native_saved_plan, damage, reason, read_count):
    case = native_saved_plan
    path = (case.context.root / case.original_entry["descriptor"]["relative_path"]).resolve()
    assert path.is_relative_to(case.tmp_path.resolve())
    if damage == "unregistered":
        case.sink._artifacts.pop(case.original_entry["artifact_id"])
    elif damage == "missing":
        path.unlink()
    elif damage == "tampered":
        path.write_bytes(path.read_bytes() + b" ")
    elif damage == "transport":
        receipt = case.completion["plan_transport"]["cell_receipt"]
        receipt["native_prefix"]["rows"] += 1
        receipt["receipt_sha256"] = planner._sha({k: v for k, v in receipt.items() if k != "receipt_sha256"})
    elif damage == "descriptor":
        changed = deepcopy(case.original_entry)
        changed["descriptor"]["sha256"] = "f" * 64
        case.completion["plan_transport"]["spool_entry"] = changed
    else:
        case.completion["horizon_plan"]["receipt_sha256"] = "f" * 64
    with pytest.raises((adapter.HistoricalProductionAdapterError, planner.ProductionSearchPlannerError), match=reason):
        _restore(case)
    assert len(case.reads) == read_count
    assert case.forbidden_publications == []
    if damage == "missing":
        assert not path.exists()
    if damage == "tampered":
        assert path.read_bytes().endswith(b" ")
