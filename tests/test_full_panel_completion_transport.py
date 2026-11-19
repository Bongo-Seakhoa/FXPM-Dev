"""INVENTED domain/receipt metadata; real typed process, Spool bytes and transport guard.

These are generation-selection software checks, not complete Raw-plan, native,
model, financial or retained STOP/capsule acceptance.
"""
import json
from types import SimpleNamespace

import pytest

import pm_authority_spool as spool
import pm_historical_anchor_orchestration as orchestration
import pm_historical_search_producer as producer
import pm_production_search_planner as planner
import pm_winner_raw_evidence as raw


_PROVENANCE = "INVENTED_TRANSPORT_METADATA_NO_NATIVE_MODEL_OR_FINANCIAL_AUTHORITY"


@pytest.fixture
def issued_process(tmp_path, monkeypatch):
    # Only catalogue construction is doubled; process issuance/receipt verification is actual.
    monkeypatch.setattr(planner, "_strategy_domains", lambda: [
        {"strategy_id": name, "fixture_provenance": _PROVENANCE}
        for name in planner.state_contract.ACTUAL_STRATEGY_NAMES])
    issued = []

    def create(*, full_panel=True):
        root = (tmp_path / ("typed-process-" + str(len(issued)))).resolve()
        context = spool.freeze_generation_context(root,
            generation_id="INVENTED-transport-" + str(len(issued)),
            native_universe_sha256="a" * 64, config_sha256="b" * 64)
        freeze = (planner.freeze_route_a_full_panel_production_search_process if full_panel
            else planner.freeze_production_search_process)
        process = freeze(context, run_id="INVENTED-transport-" + str(len(issued)))
        result = SimpleNamespace(context=context, process=process)
        issued.append(result)
        return result

    return create


def _completion(case, edition, schema):
    request = {"schema_version": orchestration.DECLARED_SLOT_REQUEST_SCHEMA,
        "search_process_receipt_sha256": case.process.receipt_sha256,
        "fixture_provenance": _PROVENANCE}
    if edition == orchestration.DECLARED_WINNER_TRANSPORT_V16:
        request.update(schema_version=orchestration.DECLARED_SLOT_REQUEST_SCHEMA_V2,
            winner_transport_edition=edition)
    assert orchestration.declared_slot_transport_edition(request) == edition
    encoded = json.dumps({"schema_version": schema, "fixture_provenance": _PROVENANCE},
        sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode("ascii")
    descriptor = spool.write_opaque_artifact(case.context, (encoded,), format=raw.ASCII_JSON_FORMAT)
    return {"status": "STOP", "reason_code": "INVENTED_TRANSPORT_ONLY",
        "request_body": request, "frozen_plan_receipt": descriptor}


_EDITIONS = (orchestration.DECLARED_WINNER_TRANSPORT_V15,
    orchestration.DECLARED_WINNER_TRANSPORT_V16)


@pytest.mark.parametrize("edition", _EDITIONS)
def test_issued_full_panel_accepts_exact_v9_in_either_supported_envelope(issued_process, edition):
    case = issued_process()
    completion = _completion(case, edition, raw.BOUND_FIT_V17_FROZEN_PLAN_SCHEMA)
    producer._verify_declared_completion_transport(case.context, completion=completion,
        expected_edition=edition, process=case.process)


@pytest.mark.parametrize("edition", _EDITIONS)
def test_legacy_process_preserves_exact_legacy_schema_sets_and_refuses_v9(issued_process, edition):
    case = issued_process(full_panel=False)
    schemas = ((raw.CURRENT_FROZEN_PLAN_SCHEMA, raw.BOUND_FIT_V15_FROZEN_PLAN_SCHEMA)
        if edition == orchestration.DECLARED_WINNER_TRANSPORT_V15
        else (raw.V16_FROZEN_PLAN_SCHEMA, raw.BOUND_FIT_V16_FROZEN_PLAN_SCHEMA))
    for schema in schemas:
        producer._verify_declared_completion_transport(case.context,
            completion=_completion(case, edition, schema), expected_edition=edition, process=case.process)
    with pytest.raises(producer.HistoricalSearchProducerError,
            match="DECLARED_SLOT_TRANSPORT_EDITION_MISMATCH"):
        producer._verify_declared_completion_transport(case.context,
            completion=_completion(case, edition, raw.BOUND_FIT_V17_FROZEN_PLAN_SCHEMA),
            expected_edition=edition, process=case.process)


@pytest.mark.parametrize("edition", _EDITIONS)
def test_full_panel_refuses_a_legacy_frozen_generation_downgrade(issued_process, edition):
    case = issued_process()
    schema = (raw.BOUND_FIT_V15_FROZEN_PLAN_SCHEMA
        if edition == orchestration.DECLARED_WINNER_TRANSPORT_V15 else raw.BOUND_FIT_V16_FROZEN_PLAN_SCHEMA)
    with pytest.raises(producer.HistoricalSearchProducerError,
            match="DECLARED_SLOT_TRANSPORT_EDITION_MISMATCH"):
        producer._verify_declared_completion_transport(case.context,
            completion=_completion(case, edition, schema), expected_edition=edition, process=case.process)


def test_full_panel_receipt_requires_the_request_matching_the_actual_issued_process(issued_process):
    case, other = issued_process(), issued_process()
    edition = orchestration.DECLARED_WINNER_TRANSPORT_V15
    completion = _completion(case, edition, raw.BOUND_FIT_V17_FROZEN_PLAN_SCHEMA)
    assert case.process.receipt_sha256 != other.process.receipt_sha256
    with pytest.raises(producer.HistoricalSearchProducerError,
            match="DECLARED_SLOT_TRANSPORT_EDITION_MISMATCH"):
        producer._verify_declared_completion_transport(case.context, completion=completion,
            expected_edition=edition, process=other.process)


def test_v9_is_not_generically_admitted_without_a_full_panel_process(issued_process):
    case = issued_process()
    edition = orchestration.DECLARED_WINNER_TRANSPORT_V16
    completion = _completion(case, edition, raw.BOUND_FIT_V17_FROZEN_PLAN_SCHEMA)
    with pytest.raises(producer.HistoricalSearchProducerError,
            match="DECLARED_SLOT_TRANSPORT_EDITION_MISMATCH"):
        producer._verify_declared_completion_transport(case.context, completion=completion,
            expected_edition=edition)
