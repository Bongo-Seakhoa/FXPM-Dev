"""INVENTED metadata callbacks; real public cold-boundary scope lifecycle only.

Upstream admission/resolution stand-ins reuse the existing owned-source fixtures.
Every call stops at the first precommit callback with an intentional exception;
no completion, source CSV, model, account or financial acceptance is claimed.
"""
from types import SimpleNamespace

import pytest

import pm_broker_terms as broker
import pm_historical_production_adapter as adapter
import pm_native_activation as native
import pm_native_prefix_production as prefix
from tests.test_native_cell_resolution_reuse import resolution_case
from tests.test_owned_swap_source_reuse import _case as _swap_case


class _BoundaryProbeComplete(RuntimeError):
    pass


def _case(tmp_path, monkeypatch, resolution_case):
    swap = _swap_case(tmp_path, monkeypatch)
    single_pass = native._resolve_active_native_artifact_single_pass

    def coherent_resolution(*args, **kwargs):
        result = single_pass(*args, **kwargs)
        result.activation_id = swap.authority.activation_id
        result.activation_manifest_path = swap.authority.activation_manifest_path
        result.activation_manifest_sha256 = swap.authority.activation_manifest_sha256
        return result

    monkeypatch.setattr(native, "_resolve_active_native_artifact_single_pass", coherent_resolution)
    return SimpleNamespace(swap=swap, native=resolution_case, observed=[])


def _resolve_twice(case):
    state = prefix._NATIVE_CELL_RESOLUTION_STATE.get()
    contracts = broker._SWAP_CONTRACT_STATE.get()
    scope = native._PRODUCTION_NATIVE_AUTHORITY_SCOPE.get()
    assert state is not None and contracts is not None and state["scope"] is scope
    case.observed.append((state, contracts, scope))
    for _ in range(2):
        assert prefix.resolve_active_native_artifact(case.swap.root.parent,
            "UNITUSD", "H1", production_contract_required=True).valid
        assert broker.resolve_active_swap_contract(case.swap.root, "EURUSD", "H1")["status"] == "AVAILABLE"
    assert len(state["resolutions"]) == len(contracts) == 1


def _call():
    return adapter.verify_declared_slot_completion(None, precommit=None,
        process=None, completion=None, artifact_roster=None, frozen_pipeline_config=None)


def _assert_closed(observed):
    for state, contracts, scope in observed:
        assert state["resolutions"] == {} and contracts == {} and scope.stats()["closed"]
    assert prefix._NATIVE_CELL_RESOLUTION_STATE.get() is None
    assert broker._SWAP_CONTRACT_STATE.get() is None
    assert native._PRODUCTION_NATIVE_AUTHORITY_SCOPE.get() is None


def test_public_cold_operation_reuses_sources_and_next_operation_is_fresh(tmp_path, monkeypatch, resolution_case):
    case = _case(tmp_path, monkeypatch, resolution_case)

    def probe(*args, **kwargs):
        _resolve_twice(case)
        raise _BoundaryProbeComplete("INVENTED_PRECOMMIT_SENTINEL_REFUSAL")

    monkeypatch.setattr(adapter.orchestration, "verify_declared_slot_owner_run_precommit", probe)
    for count in (1, 2):
        with pytest.raises(_BoundaryProbeComplete, match="INVENTED_PRECOMMIT_SENTINEL_REFUSAL"):
            _call()
        assert len(case.swap.loaded) == len(case.swap.resolved) == len(case.native.calls) == count
        _assert_closed(case.observed)
    assert case.observed[0][2] is not case.observed[1][2]


def test_nested_public_cold_operation_reuses_existing_owner_scopes(tmp_path, monkeypatch, resolution_case):
    case = _case(tmp_path, monkeypatch, resolution_case)
    depth = 0

    def probe(*args, **kwargs):
        nonlocal depth
        _resolve_twice(case)
        if depth == 0:
            depth += 1
            with pytest.raises(_BoundaryProbeComplete):
                _call()
        raise _BoundaryProbeComplete("INVENTED_NESTED_SENTINEL_REFUSAL")

    monkeypatch.setattr(adapter.orchestration, "verify_declared_slot_owner_run_precommit", probe)
    with prefix._owned_native_resolution_scope() as state, broker._owned_swap_source_scope() as contracts:
        with pytest.raises(_BoundaryProbeComplete):
            _call()
        assert all(item[0] is state and item[1] is contracts for item in case.observed)
        assert not state["scope"].stats()["closed"]
        assert prefix._NATIVE_CELL_RESOLUTION_STATE.get() is state
        assert broker._SWAP_CONTRACT_STATE.get() is contracts
        assert len(case.swap.loaded) == len(case.swap.resolved) == len(case.native.calls) == 1
    _assert_closed(case.observed)


def test_public_cold_operation_clears_owned_states_after_unexpected_failure(tmp_path, monkeypatch, resolution_case):
    case = _case(tmp_path, monkeypatch, resolution_case)

    def probe(*args, **kwargs):
        _resolve_twice(case)
        raise ValueError("INVENTED_COLD_PRECOMMIT_FAILURE")

    monkeypatch.setattr(adapter.orchestration, "verify_declared_slot_owner_run_precommit", probe)
    with pytest.raises(ValueError, match="INVENTED_COLD_PRECOMMIT_FAILURE"):
        _call()
    assert len(case.swap.loaded) == len(case.swap.resolved) == len(case.native.calls) == 1
    _assert_closed(case.observed)
