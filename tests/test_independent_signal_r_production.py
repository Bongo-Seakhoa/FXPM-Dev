"""Invented finite software fixtures; no native source issuance or trading results."""
import copy
from dataclasses import replace
from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest

import pm_authoritative_search_execution as execution
import pm_historical_anchor_orchestration as historical
import pm_historical_scheduler as scheduler
import pm_historical_search_producer as producer
import pm_production_search_planner as planner
import pm_winner_selection as winner


def _body(schema=None, edition=None):
    return {"schema_version": schema or planner.INDEPENDENT_R_PROCESS_SCHEMA,
        "execution_edition": edition or planner.INDEPENDENT_R_EXECUTION_EDITION,
        "semantics": {"registry_order": list(planner.state_contract.ACTUAL_STRATEGY_NAMES)},
        "candidate_domain_sha256": "1" * 64, "state_contract_registry_sha256": "2" * 64}


@pytest.mark.parametrize("schema,edition,generation", [
    (planner.ROUTE_A_FULL_PANEL_PROCESS_SCHEMA, planner.ROUTE_A_FULL_PANEL_EXECUTION_EDITION, winner.V17_GENERATION),
    (planner.NOISE101_PROCESS_SCHEMA, planner.NOISE101_EXECUTION_EDITION, winner.V18_GENERATION),
    (planner.INDEPENDENT_R_PROCESS_SCHEMA, planner.INDEPENDENT_R_EXECUTION_EDITION, winner.V19_GENERATION)])
def test_explicit_process_maps_generations_without_changing_legacy(schema, edition, generation):
    assert planner.full_panel_generation(_body(schema, edition)).selection_version == generation.selection_version
    assert winner.CURRENT_GENERATION.selection_version != winner.V19_GENERATION.selection_version


def test_unknown_process_never_falls_back_to_current():
    with pytest.raises(planner.ProductionSearchPlannerError):
        planner.source_edition_info(_body("fxpm.unknown.v999", "unknown"))


def test_v19_current_public_entry_forwards_actual_generation(monkeypatch):
    seen = []
    monkeypatch.setattr(execution, "_execute_current_observed_cell", lambda **kw: seen.append(kw) or "captured")
    assert execution.execute_independent_r_current_observed_cell(process=_body(), sink=object(),
        cell_plan=object(), fitted=object(), config=object()) == "captured"
    assert seen[0]["_generation"].independent_signal_r_enabled
    assert planner.source_edition_info(_body()).winner_generation_id == winner.V19_GENERATION.selection_version


def test_v19_historical_dispatch_authenticates_before_generation_lookup(monkeypatch):
    calls = []
    monkeypatch.setattr(planner, "frozen_search_execution_edition", lambda *a: calls.append("authenticated"))
    def descriptor(*a):
        assert calls == ["authenticated"]
        return SimpleNamespace(independent_signal_r_enabled=True)
    monkeypatch.setattr(planner, "source_edition_info", descriptor)
    found = producer._declared_observed_cell_executor(object(), process=object(),
        transport_edition=historical.DECLARED_WINNER_TRANSPORT_V16)
    assert found is execution.execute_independent_r_historical_observed_cell


def test_v19_scheduler_freezes_explicit_edition_on_fresh_owner(monkeypatch, tmp_path):
    import pm_authority_spool as spool
    monkeypatch.setattr(producer, "_read_declared_record", lambda *a, **kw: None)
    monkeypatch.setattr(spool, "_reject_reparse_chain", lambda *a: None)
    calls = []
    monkeypatch.setattr(planner, "freeze_independent_signal_r_production_search_process",
        lambda context, **kw: calls.append(kw) or "v19")
    context = SimpleNamespace(root=tmp_path, limits=SimpleNamespace(max_manifest_bytes=1024))
    assert scheduler._declared_initial_process(context,
        {"run_id": "invented-software", "execution_edition": planner.INDEPENDENT_R_EXECUTION_EDITION},
        development=False) == "v19"
    assert calls == [{"run_id": "invented-software"}]


class _Strategy:
    def __init__(self):
        self.generated = 0


def _surface_case(monkeypatch):
    frame = pd.DataFrame({"Open": [10.0, 11.0]},
        index=pd.date_range("2024-01-01", periods=2, freq="h", tz="UTC"))
    frame.attrs = {"consumed_tag": "first"}
    owner = producer._UnitNumericPreparation()
    fitted = SimpleNamespace(_frame=frame, frame_sha256="3" * 64,
        _numeric_preparation=owner, _numeric_key=("actual-source", "actual-fit-seed"))
    monkeypatch.setattr(execution, "_route_a_plain_strategy_state", lambda strategy: copy.deepcopy(vars(strategy)))
    calls = []
    def compute(*, frame, strategy, **kw):
        calls.append(kw)
        strategy.generated += 1
        features = frame.copy(deep=True)
        features["owned_feature"] = [4.0, 5.0]
        return SimpleNamespace(features=features,
            raw_signals=pd.Series([1, 0], index=frame.index, dtype=np.int32))
    monkeypatch.setattr(execution, "_build_full_prefix_strategy_surface", compute)
    def run(*, parameters=None, config="4" * 64):
        strategy = _Strategy()
        result = execution._independent_r_strategy_surface(attempt=SimpleNamespace(),
            parameters={"period": 3} if parameters is None else parameters,
            strategy=strategy, fitted=fitted, config_sha=config, symbol="SYNTH", timeframe="H1")
        return strategy, result
    return fitted, owner, calls, run


def test_current_attempt_replay_reuses_math_with_detached_state_and_surfaces(monkeypatch):
    fitted, owner, calls, run = _surface_case(monkeypatch)
    first_strategy, first = run()
    first.features.loc[:, "owned_feature"] = 999.0
    first.raw_signals.iloc[0] = -1
    second_strategy, second = run()
    assert len(calls) == 1
    assert first_strategy is not second_strategy
    assert second_strategy.generated == 1
    assert second.features["owned_feature"].tolist() == [4.0, 5.0]
    assert second.raw_signals.tolist() == [1, 0]
    owner.close()
    run()
    assert len(calls) == 2


def test_every_consumed_input_and_cold_owner_changes_the_numeric_key(monkeypatch):
    fitted, owner, calls, run = _surface_case(monkeypatch)
    run(); run(parameters={"period": 4}); run(config="5" * 64)
    fitted._frame.attrs["consumed_tag"] = "changed"
    run()
    fitted._numeric_key = ("actual-source", "different-consumed-fit-seed")
    run()
    fitted._numeric_preparation = producer._UnitNumericPreparation()
    run()
    assert len(calls) == 6


def test_opaque_strategy_computes_normally_without_a_new_refusal(monkeypatch):
    fitted, owner, calls, run = _surface_case(monkeypatch)
    monkeypatch.setattr(execution, "_route_a_plain_strategy_state", lambda strategy: None)
    run(); run()
    assert len(calls) == 2


def test_strategy_mutating_authenticated_input_is_refused(monkeypatch):
    fitted, owner, calls, run = _surface_case(monkeypatch)
    def mutate(*, frame, **kw):
        frame.iloc[0, 0] = 1000.0
        return SimpleNamespace(features=frame.copy(), raw_signals=pd.Series(0, index=frame.index))
    monkeypatch.setattr(execution, "_build_full_prefix_strategy_surface", mutate)
    with pytest.raises(execution.AuthoritativeSearchExecutionError, match="FULL_PREFIX_STRATEGY_INPUT_MUTATED"):
        run()


def test_temporary_engineering_v2_dispatch_is_retired():
    import inspect
    assert "method_edition" not in inspect.signature(execution.issue_signal_opportunity_design_request).parameters
    assert not hasattr(execution, "INDEPENDENT_R_REQUEST_SCHEMA")


def test_historical_cold_receiver_threads_verified_fit_metadata_into_raw_reader(monkeypatch):
    """Invented authority seam; source replay math is exercised separately."""
    import pm_historical_production_adapter as adapter
    order, received = [], {}
    precommitment = {"selection_complexity_roster": []}
    frozen = SimpleNamespace(selection_generation=winner.V19_GENERATION,
        attempt_artifacts_bundle_bytes=b"[]", precommitment=precommitment, timeframe="H1")
    request = SimpleNamespace(precommitment=SimpleNamespace(to_dict=lambda: precommitment))
    monkeypatch.setattr(planner, "verify_frozen_production_search_process", lambda *a: True)
    monkeypatch.setattr(planner, "source_edition_info", lambda *a: SimpleNamespace(
        winner_generation_id=winner.V19_GENERATION.selection_version))
    monkeypatch.setattr(adapter, "_frozen_selection_complexity_from_process", lambda *a, **kw: [])
    metadata = {"receipts": {"regime_parameters": {"INVENTED": True}}}
    material = tuple(object() for _ in range(5))
    def reopen(*a, **kw):
        order.append("source")
        output = kw["_bound_fit_metadata_out"]
        output.update(metadata)
        received["metadata"] = output
        return material
    def verify(*a, **kw):
        order.append("reader")
        assert kw["bound_fit_metadata"] is received["metadata"]
        assert kw["bound_fit_metadata"] == metadata
        assert kw["source_material"] is material
    monkeypatch.setattr(execution, "_reopen_route_a_matched_source_material", reopen)
    monkeypatch.setattr(adapter.raw_winner, "verify_matched_floor_publication_chain", verify)
    adapter._verify_declared_matched_floor_chain(object(), process=SimpleNamespace(to_dict=lambda: {}),
        producer_request=object(), frozen=frozen, winner_request=request, entries={}, cross={},
        frozen_pipeline_config=object())
    assert order == ["source", "reader"]
