"""Invented source-only activation fixtures, not market or run evidence.

These definitions have not been imported, collected, or executed.
"""
from dataclasses import replace
import pytest
import pm_authority_spool as spool
import pm_production_search_planner as planner
import pm_historical_search_producer as driver
import pm_authoritative_search_execution as engine
import pm_historical_anchor_orchestration as historical
import pm_winner_selection as selection


def _context(tmp_path, name):
    return spool.freeze_generation_context((tmp_path / name).resolve(),
        generation_id=name, native_universe_sha256="a" * 64, config_sha256="b" * 64)


@pytest.mark.parametrize("new_edition", [False, True])
def test_execution_edition_freeze_and_real_spool_reopen(tmp_path, new_edition):
    context = _context(tmp_path, "activation-edition")
    freeze = (planner.freeze_route_a_full_panel_production_search_process
        if new_edition else planner.freeze_production_search_process)
    process = freeze(context, run_id="activation-run")
    body = process.to_dict()
    assert planner.verify_frozen_production_search_process(context, process) is True
    assert body["schema_version"] == (planner.ROUTE_A_FULL_PANEL_PROCESS_SCHEMA
        if new_edition else planner.PROCESS_SCHEMA)
    assert ("execution_edition" in body) is new_edition
    assert planner.frozen_search_execution_edition(context, process) == (
        planner.ROUTE_A_FULL_PANEL_EXECUTION_EDITION if new_edition else None)
    reopened = planner.reopen_production_search_process(context,
        spool_receipt=process.spool_receipt, expected_receipt_sha256=process.receipt_sha256)
    assert reopened._body_json == process._body_json
    expected_semantics = planner._process_semantics(planner._strategy_domains())
    if new_edition:
        expected_semantics["attempt_policy"]["winner_contract_version"] = selection.V17_GENERATION.selection_version
        assert body["winner_selection_binding"] == {
            "selection_version": selection.V17_GENERATION.selection_version,
            "request_version": selection.V17_GENERATION.request_version,
            "selection_policy_sha256": selection.V17_GENERATION.policy_sha256,
            "selection_addendum_contract_sha256": selection.V17_SELECTION_ADDENDUM_CONTRACT_SHA256,
        }
    else:
        assert "winner_selection_binding" not in body
    assert body["semantics"] == expected_semantics
    assert body["semantics"]["attempt_policy"]["attempt_count_per_observed_cell"] == planner.ATTEMPT_COUNT
    assert body["semantics"]["fold_policy"]["fold_count"] == planner.CAUSAL_FOLD_COUNT


@pytest.mark.parametrize("mutation", ["missing", "wrong", "old_schema"])
def test_execution_edition_tampering_refuses_before_dispatch(tmp_path, mutation):
    context = _context(tmp_path, "activation-refusal")
    process = planner.freeze_route_a_full_panel_production_search_process(context, run_id="activation-run")
    body = process.to_dict()
    if mutation == "missing": del body["execution_edition"]
    elif mutation == "wrong": body["execution_edition"] = "unaccepted-edition"
    else: body["schema_version"] = planner.PROCESS_SCHEMA
    altered = replace(process, _body_json=planner._canonical_bytes(body))
    with pytest.raises(planner.ProductionSearchPlannerError):
        driver._declared_observed_cell_executor(context, process=altered,
            transport_edition=historical.DECLARED_WINNER_TRANSPORT_V16)


@pytest.mark.parametrize("new_edition", [False, True])
@pytest.mark.parametrize("transport", [historical.DECLARED_WINNER_TRANSPORT_V15,
                                      historical.DECLARED_WINNER_TRANSPORT_V16])
def test_authenticated_execution_dispatch_is_separate_from_transport(tmp_path, new_edition, transport):
    context = _context(tmp_path, "activation-dispatch")
    freeze = (planner.freeze_route_a_full_panel_production_search_process
        if new_edition else planner.freeze_production_search_process)
    process = freeze(context, run_id="activation-run")
    selected = driver._declared_observed_cell_executor(context, process=process, transport_edition=transport)
    expected = (engine.execute_route_a_full_panel_historical_observed_cell if new_edition
        else engine.execute_transport_historical_observed_cell
        if transport == historical.DECLARED_WINNER_TRANSPORT_V16
        else engine.execute_historical_observed_cell)
    assert selected is expected
