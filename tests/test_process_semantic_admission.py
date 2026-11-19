"""INVENTED catalogue trace with actual process Spool IO; no model/source authority.

Only semantic derivation reuse is claimed. Cold receipt and payload verification
remain real, including after a process object has earned semantic admission.
"""
from __future__ import annotations

import copy
from dataclasses import replace
import inspect

import pytest

import pm_authority_spool as spool
import pm_production_search_planner as planner
from tests import test_process_verification_same_call_reuse as fixture_module


def test_frozen_process_semantics_reuse_across_separate_and_nested_calls(tmp_path, monkeypatch):
    derivations = []
    original_derive = planner._derive_expected_process_body

    def derive(*args, **kwargs):
        derivations.append(True)
        return original_derive(*args, **kwargs)

    monkeypatch.setattr(planner, "_derive_expected_process_body", derive)
    case = fixture_module._process_case(tmp_path, monkeypatch)
    assert len(derivations) == 1  # Freeze's one actual derivation earned admission.
    assert case.process._semantic_admission is not None
    original_body = case.process.to_dict()
    assert planner.verify_frozen_production_search_process(case.context, case.process) is True
    assert planner.verify_frozen_production_search_process(case.context, case.process) is True
    assert case.counts == {"catalogue": 0, "admission": 2, "payload": 2}

    def owner(*, context, process):
        assert planner.verify_frozen_production_search_process(context, process) is True
        with planner._process_verification_scope():
            clone = replace(process)
            assert clone._semantic_admission is None
            assert planner.verify_frozen_production_search_process(context, clone) is True
            assert planner.verify_frozen_production_search_process(context, process) is True
        return clone

    clone = planner._owned_process_verification_call(owner)(context=case.context, process=case.process)
    assert case.counts == {"catalogue": 0, "admission": 3, "payload": 3}
    assert clone.to_dict() == original_body
    assert clone._semantic_admission is not None
    assert planner.verify_frozen_production_search_process(case.context, clone) is True
    assert case.counts == {"catalogue": 0, "admission": 4, "payload": 4}
    assert len(derivations) == 1


def test_admitted_process_still_refuses_actual_cold_payload_corruption(tmp_path, monkeypatch):
    case = fixture_module._process_case(tmp_path, monkeypatch)
    assert planner.verify_frozen_production_search_process(case.context, case.process) is True
    path = case.context.root / case.descriptor["relative_path"]
    original = path.read_bytes()
    damaged = bytearray(original)
    damaged[-2] ^= 1
    path.write_bytes(damaged)
    with pytest.raises(spool.AuthoritySpoolError, match="ARTIFACT_INTEGRITY_MISMATCH"):
        planner.verify_frozen_production_search_process(case.context, case.process)
    assert case.counts == {"catalogue": 0, "admission": 2, "payload": 2}
    path.write_bytes(original)
    assert planner.verify_frozen_production_search_process(case.context, case.process) is True
    assert case.counts == {"catalogue": 0, "admission": 3, "payload": 3}


def test_real_reopened_process_derives_once_then_reuses_its_own_admission(tmp_path, monkeypatch):
    case = fixture_module._process_case(tmp_path, monkeypatch)
    assert "_semantic_admission" not in inspect.signature(planner.FrozenProductionSearchProcess).parameters
    assert replace(case.process)._semantic_admission is None
    reopened = planner.reopen_production_search_process(case.context,
        spool_receipt=case.process.spool_receipt, expected_receipt_sha256=case.process.receipt_sha256)
    assert reopened is not case.process
    assert reopened._body_json == case.process._body_json
    assert reopened._semantic_admission is not None
    assert case.counts == {"catalogue": 1, "admission": 2, "payload": 3}
    assert planner.verify_frozen_production_search_process(case.context, reopened) is True
    assert planner.verify_frozen_production_search_process(case.context, reopened) is True
    assert case.counts == {"catalogue": 1, "admission": 4, "payload": 5}


def test_semantic_admission_never_reuses_changed_body_context_or_policy(tmp_path, monkeypatch):
    case = fixture_module._process_case(tmp_path, monkeypatch)
    original_bytes = case.process._body_json
    admission = case.process._semantic_admission
    changed = case.process.to_dict()
    changed["semantics"]["fixture_splice"] = "INVENTED_INVALID_BODY"
    object.__setattr__(case.process, "_body_json", planner._canonical_bytes(changed))
    try:
        with pytest.raises(planner.ProductionSearchPlannerError, match="PROCESS_SEMANTICS_CHANGED"):
            planner.verify_frozen_production_search_process(case.context, case.process)
    finally:
        object.__setattr__(case.process, "_body_json", original_bytes)
    assert case.counts == {"catalogue": 1, "admission": 0, "payload": 0}
    other = spool.freeze_generation_context(tmp_path / "other-process-context",
        generation_id="INVENTED-other-generation", native_universe_sha256="a" * 64, config_sha256="c" * 64)
    with pytest.raises(planner.ProductionSearchPlannerError, match="PROCESS_SEMANTICS_CHANGED"):
        planner.verify_frozen_production_search_process(other, case.process)
    assert case.counts == {"catalogue": 2, "admission": 0, "payload": 0}
    changed_policy = copy.deepcopy(planner.causal.pinned_process_policy())
    changed_policy["fixture_provenance"] = "INVENTED_INVALID_POLICY_NO_NUMERIC_SUBSTITUTE"
    with monkeypatch.context() as patch:
        patch.setattr(planner.causal, "_PINNED_POLICY", changed_policy)
        with pytest.raises(planner.ProductionSearchPlannerError, match="PROCESS_SEMANTICS_CHANGED"):
            planner.verify_frozen_production_search_process(case.context, case.process)
    assert case.counts == {"catalogue": 3, "admission": 0, "payload": 0}
    assert case.process._semantic_admission == admission
    assert planner.verify_frozen_production_search_process(case.context, case.process) is True
    assert case.counts == {"catalogue": 3, "admission": 1, "payload": 1}


def test_admitted_semantics_never_skips_changed_cold_receipt_validation(tmp_path, monkeypatch):
    case = fixture_module._process_case(tmp_path, monkeypatch)
    receipt_bytes = case.process._spool_receipt_json
    object.__setattr__(case.process, "_spool_receipt_json", b"{}")
    try:
        with pytest.raises(spool.AuthoritySpoolError, match="COMPACT_RECEIPT_FIELDS_INVALID"):
            planner.verify_frozen_production_search_process(case.context, case.process)
    finally:
        object.__setattr__(case.process, "_spool_receipt_json", receipt_bytes)
    assert case.counts == {"catalogue": 0, "admission": 1, "payload": 0}
    assert planner.verify_frozen_production_search_process(case.context, case.process) is True
    assert case.counts == {"catalogue": 0, "admission": 2, "payload": 1}
