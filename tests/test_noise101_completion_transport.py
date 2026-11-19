"""INVENTED software metadata: dispatch identity and refusal, never303 execution.

Opaque test artifacts carry only a format marker. No Raw-plan, native, model,
source-window or financial qualification follows these tests.
"""
import pytest

import pm_authority_spool as spool
import pm_authoritative_search_execution as execution
import pm_historical_anchor_orchestration as historical
import pm_historical_production_adapter as adapter
import pm_historical_search_producer as producer
import pm_production_search_planner as planner
import pm_winner_raw_evidence as raw
import pm_winner_selection as winner
from test_full_panel_completion_transport import _completion


@pytest.fixture(scope="module")
def noise_description(tmp_path_factory):
    from types import SimpleNamespace
    root = tmp_path_factory.mktemp("INVENTED-noise101-transport").resolve()
    context = spool.freeze_generation_context(root,
        generation_id="INVENTED-noise101-transport",
        native_universe_sha256="a" * 64, config_sha256="b" * 64)
    process = planner.freeze_noise101_software_search_description(
        context, run_id="INVENTED-noise101-transport")
    return SimpleNamespace(root=root, context=context, process=process)


def test_noise101_software_description_reopens_without_production_qualification(noise_description):
    case = noise_description
    assert planner.verify_frozen_production_search_process(case.context, case.process)
    reopened = planner.reopen_production_search_process(case.context,
        spool_receipt=case.process.spool_receipt,
        expected_receipt_sha256=case.process.receipt_sha256)
    assert reopened._body_json == case.process._body_json
    assert reopened.to_dict()["authority_scope"] == "SOFTWARE_SEARCH_DESCRIPTION_ONLY_UNQUALIFIED"
    assert planner.full_panel_generation(reopened) is winner.V18_GENERATION
    assert planner.process_attempt_count(reopened) == 303
    before = {str(p.relative_to(case.root)): p.stat().st_size
        for p in case.root.rglob("*") if p.is_file()}
    with pytest.raises(planner.ProductionSearchPlannerError,
            match="NOISE101_PRODUCTION_QUALIFICATION_NOT_ESTABLISHED"):
        planner.freeze_noise101_full_panel_production_search_process(
            case.context, run_id="INVENTED-unqualified-must-not-publish")
    assert {str(p.relative_to(case.root)): p.stat().st_size
        for p in case.root.rglob("*") if p.is_file()} == before


@pytest.mark.parametrize("edition", [historical.DECLARED_WINNER_TRANSPORT_V15,
    historical.DECLARED_WINNER_TRANSPORT_V16])
def test_noise101_dispatch_metadata_never_admits_unqualified_completion(noise_description, edition):
    case = noise_description
    # Selecting a function reference executes neither a capture issuer nor a model.
    assert producer._declared_observed_cell_executor(case.context,
        process=case.process, transport_edition=edition) is (
            execution.execute_route_a_full_panel_historical_observed_cell)
    assert producer._declared_capture_opener(case.context,
        process=case.process, transport_edition=edition) is (
            adapter.open_matched_floor_declared_slot_capture)
    completion = _completion(case, edition, raw.BOUND_FIT_V18_FROZEN_PLAN_SCHEMA)
    with pytest.raises(planner.ProductionSearchPlannerError,
            match="NOISE101_SOFTWARE_DESCRIPTION_NOT_EXECUTABLE"):
        producer._verify_declared_completion_transport(case.context,
            completion=completion, expected_edition=edition, process=case.process)
    with pytest.raises(producer.HistoricalSearchProducerError,
            match="DECLARED_SLOT_TRANSPORT_EDITION_MISMATCH"):
        producer._verify_declared_completion_transport(case.context,
            completion=completion, expected_edition=edition)
