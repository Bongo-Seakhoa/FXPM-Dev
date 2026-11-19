"""INVENTED catalogue spies with real Spool process IO; no model or origin acceptance."""
from __future__ import annotations

from dataclasses import replace
import inspect
import json
from types import SimpleNamespace

import pytest

import pm_authority_spool as spool
import pm_production_search_planner as planner


_PROVENANCE = "INVENTED_CATALOGUE_TRACE_ONLY_NOT_REAL_STRATEGY_OR_QUALIFICATION_EVIDENCE"


def _process_case(tmp_path, monkeypatch):
    counts = {"catalogue": 0, "admission": 0, "payload": 0}

    def catalogue():
        counts["catalogue"] += 1
        return [{"strategy_id": "INVENTED-CATALOGUE-" + str(ordinal), "fixture_provenance": _PROVENANCE}
                for ordinal in range(planner.BASELINE_ATTEMPTS)]

    monkeypatch.setattr(planner, "_strategy_domains", catalogue)
    context = spool.freeze_generation_context(tmp_path / "invented-process",
        generation_id="INVENTED-process-memo-software", native_universe_sha256="a" * 64, config_sha256="b" * 64)
    process = planner.freeze_route_a_full_panel_production_search_process(context,
        run_id="INVENTED-process-memo")
    receipt = process.spool_receipt
    manifest = json.loads((context.root / receipt["symbol_manifest"]["relative_path"]).read_bytes())
    descriptor = manifest["artifacts"][0]["descriptor"]
    original_admit, original_stream = spool.verify_symbol_receipt, spool._stream_file

    def admit(root, receipt, **kwargs):
        if kwargs.get("expected_symbol") == planner.PROCESS_SPOOL_SYMBOL:
            counts["admission"] += 1
        return original_admit(root, receipt, **kwargs)

    def stream(root, relative, **kwargs):
        if relative == descriptor["relative_path"]:
            counts["payload"] += 1
        return original_stream(root, relative, **kwargs)

    monkeypatch.setattr(spool, "verify_symbol_receipt", admit)
    monkeypatch.setattr(spool, "_stream_file", stream)
    counts.update(catalogue=0, admission=0, payload=0)
    return SimpleNamespace(context=context, process=process, descriptor=descriptor, counts=counts)


def _expected(case):
    body = case.process.to_dict()
    return planner._expected_process_body(case.context, run_id=body["run_id"], schema_version=body["schema_version"])


def test_whole_call_scope_reuses_one_real_process_admission_across_nested_scopes(tmp_path, monkeypatch):
    case = _process_case(tmp_path, monkeypatch)

    def owner(*, context, process):
        assert planner.verify_frozen_production_search_process(context, process) is True
        with planner._process_verification_scope():
            assert planner.verify_frozen_production_search_process(context, process) is True
            assert _expected(case) == process.to_dict()
        assert planner.verify_frozen_production_search_process(context, process) is True
        reopened = spool.reopen_generation_context(context.root,
            expected_generation_id=context.generation_id,
            expected_native_universe_sha256=context.native_universe_sha256,
            expected_config_sha256=context.config_sha256)
        assert reopened is not context
        assert planner.verify_frozen_production_search_process(reopened, replace(process)) is True
        return process.receipt_sha256

    scoped_owner = planner._owned_process_verification_call(owner)
    assert inspect.signature(scoped_owner) == inspect.signature(owner)
    assert scoped_owner(context=case.context, process=case.process) == case.process.receipt_sha256
    assert case.counts == {"catalogue": 1, "admission": 1, "payload": 1}
    assert planner.verify_frozen_production_search_process(case.context, case.process) is True
    assert case.counts == {"catalogue": 1, "admission": 2, "payload": 2}


def test_expected_process_body_returns_independent_values_and_cold_call_rebuilds(tmp_path, monkeypatch):
    case = _process_case(tmp_path, monkeypatch)
    original = case.process.to_dict()
    with planner._process_verification_scope():
        first = _expected(case)
        first["semantics"]["strategy_domains"][0]["fixture_provenance"] = "INVENTED_POISON"
        first["semantics"]["registry_order"].append("INVENTED_EXTRA")
        assert _expected(case) == original
        assert case.process.to_dict() == original
        assert case.counts == {"catalogue": 1, "admission": 0, "payload": 0}
    assert _expected(case) == original
    assert case.counts == {"catalogue": 2, "admission": 0, "payload": 0}


def test_exception_clears_process_scope_before_an_independent_cold_call(tmp_path, monkeypatch):
    case = _process_case(tmp_path, monkeypatch)
    failure = RuntimeError("INVENTED_OWNER_CALLBACK_FAILURE")
    with pytest.raises(RuntimeError) as caught:
        with planner._process_verification_scope():
            assert planner.verify_frozen_production_search_process(case.context, case.process) is True
            raise failure
    assert caught.value is failure
    assert planner.verify_frozen_production_search_process(case.context, case.process) is True
    assert case.counts == {"catalogue": 0, "admission": 2, "payload": 2}


@pytest.mark.parametrize("change", ("context", "process-identity", "process-body", "receipt-body", "source-generation"))
def test_process_scope_does_not_reuse_changed_bound_inputs(tmp_path, monkeypatch, change):
    case = _process_case(tmp_path, monkeypatch)
    with planner._process_verification_scope():
        assert planner.verify_frozen_production_search_process(case.context, case.process) is True
        if change == "process-identity":
            clone = replace(case.process)
            assert clone is not case.process
            assert planner.verify_frozen_production_search_process(case.context, clone) is True
            assert case.counts == {"catalogue": 0, "admission": 1, "payload": 1}
        elif change == "context":
            other = spool.freeze_generation_context(tmp_path / "another-invented-context",
                generation_id="INVENTED-other-process-context", native_universe_sha256="a" * 64, config_sha256="c" * 64)
            with pytest.raises(planner.ProductionSearchPlannerError, match="PROCESS_SEMANTICS_CHANGED"):
                planner.verify_frozen_production_search_process(other, case.process)
            assert case.counts == {"catalogue": 1, "admission": 1, "payload": 1}
        elif change == "process-body":
            changed = case.process.to_dict()
            changed["semantics"]["fixture_splice"] = "INVENTED_CHANGED_BODY"
            clone = replace(case.process, _body_json=planner._canonical_bytes(changed))
            with pytest.raises(planner.ProductionSearchPlannerError, match="PROCESS_SEMANTICS_CHANGED"):
                planner.verify_frozen_production_search_process(case.context, clone)
            assert case.counts == {"catalogue": 1, "admission": 1, "payload": 1}
        elif change == "receipt-body":
            clone = replace(case.process, _spool_receipt_json=b"{}")
            with pytest.raises(spool.AuthoritySpoolError) as caught:
                planner.verify_frozen_production_search_process(case.context, clone)
            assert str(caught.value) == "COMPACT_RECEIPT_FIELDS_INVALID"
            assert case.counts == {"catalogue": 1, "admission": 2, "payload": 1}
        else:
            with monkeypatch.context() as patch:
                patch.setattr(planner, "PRODUCTION_SEARCH_PLANNER_RELEASE_SHA256", "f" * 64)
                with pytest.raises(planner.ProductionSearchPlannerError, match="PROCESS_SEMANTICS_CHANGED"):
                    planner.verify_frozen_production_search_process(case.context, case.process)
            assert case.counts == {"catalogue": 1, "admission": 1, "payload": 1}
        assert planner.verify_frozen_production_search_process(case.context, case.process) is True


def test_new_cold_process_verification_refuses_actual_changed_spool_bytes(tmp_path, monkeypatch):
    case = _process_case(tmp_path, monkeypatch)
    assert planner.verify_frozen_production_search_process(case.context, case.process) is True
    path = case.context.root / case.descriptor["relative_path"]
    damaged = bytearray(path.read_bytes())
    damaged[-2] ^= 1
    path.write_bytes(damaged)
    with pytest.raises(spool.AuthoritySpoolError) as caught:
        planner.verify_frozen_production_search_process(case.context, case.process)
    assert str(caught.value) == "ARTIFACT_INTEGRITY_MISMATCH"
    assert case.counts == {"catalogue": 0, "admission": 2, "payload": 2}
