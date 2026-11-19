"""INVENTED helper-level fitted-state shapes; no source/issuer admission proof.

Real reuse, reference, frame-mutation and executable guards run on tiny typed
values. Public capture/session admission, native IO and model fits are outside
this fixture and are neither replaced nor claimed as verified here.
"""
from __future__ import annotations

from types import SimpleNamespace

import pandas as pd
import pytest

import pm_authoritative_search_execution as execution


LABEL = "INVENTED_FITTED_REUSE_HELPER_NO_SOURCE_AUTHORITY"


def _invented_case(*, derived=False, bound=False):
    context = object()
    capture = SimpleNamespace(context=context, request=SimpleNamespace(receipt_sha256="a" * 64),
        process=object(), source_session=SimpleNamespace(_frozen_config=object(),
            _entry={"fixture_provenance": LABEL}, to_dict=lambda: {"fixture_provenance": LABEL}))
    config = execution.PipelineConfig()
    frame = pd.DataFrame({"Close": [1.0, 1.25]},
        index=pd.date_range("2026-01-01T00:00:00Z", periods=2, freq="h"))
    derived_binding = (execution._DerivedSearchBinding(context=context, owner=object(),
        request_lease=object(), calibration_lease=object(), cost_edition=object(),
        declared_m5_source_pins={"fixture_provenance": LABEL}, view_evidence={"fixture_provenance": LABEL})
        if derived else None)
    fitted = execution.FittedNativeSearchTimeframe(product=None, source_group_decision=None,
        regime_fit_decision=None, source_contract_seed={"fixture_provenance": LABEL},
        cost_ladder_contract={"fixture_provenance": LABEL}, regime_parameters={},
        train_regime_support={}, instrument_spec=None, execution_spec_receipt={"fixture_provenance": LABEL},
        frame_sha256=execution._frame_sha256(frame), _frame=frame, _derived_binding=derived_binding)
    owner = None
    if bound:
        # An internal owner SHAPE, deliberately not a genuine cold-issued source capability.
        owner = object.__new__(execution._BoundFitOwner)
        owner.seal, owner.fitted = execution._BOUND_FIT_OWNER_SEAL, fitted
        owner.context_id, owner.request_sha256 = id(context), capture.request.receipt_sha256
        owner.header_bytes = b"{}"
        owner.reference_bytes = execution._canonical_bytes({"fixture_provenance": LABEL})
        owner.validation_receipt, owner.verification_only = None, False
        object.__setattr__(fitted, "_bound_fit_owner", owner)
    return capture, fitted, config, owner


def _forbid_repeat_work(monkeypatch):
    def forbidden(*args, **kwargs):
        raise AssertionError("The admitted fitted object must not be refitted or cold-reopened here")

    for name in ("_reverify_fitted_timeframe", "reopen_bound_fitted_timeframe",
                 "fit_native_timeframe_for_search", "fit_m5_derived_timeframe_for_search"):
        monkeypatch.setattr(execution, name, forbidden)


@pytest.mark.parametrize("derived", [False, True], ids=["native", "m5-derived"])
def test_declared_fresh_fit_reuse_keeps_actual_object_and_existing_owner_fields(monkeypatch, derived):
    capture, fitted, config, _ = _invented_case(derived=derived)
    _forbid_repeat_work(monkeypatch)
    actual, owner = execution._declared_fitted_for_execution(capture, fitted, config)
    assert actual is fitted
    assert type(owner) is execution._FreshFitOwner
    assert owner.seal is execution._BOUND_FIT_OWNER_SEAL
    assert owner.fitted is owner.original_fitted is fitted
    assert owner.config is config
    assert owner.context_id == id(capture.context)
    assert owner.request_sha256 == capture.request.receipt_sha256


@pytest.mark.parametrize("derived", [False, True], ids=["native", "m5-derived"])
def test_declared_bound_fit_reuse_runs_real_reference_guard_without_second_reopen(monkeypatch, derived):
    capture, fitted, config, expected_owner = _invented_case(derived=derived, bound=True)
    _forbid_repeat_work(monkeypatch)
    original_reference = execution.bound_fitted_state_reference
    calls = []

    def reference(actual, **kwargs):
        calls.append((actual, kwargs))
        return original_reference(actual, **kwargs)

    monkeypatch.setattr(execution, "bound_fitted_state_reference", reference)
    actual, owner = execution._declared_fitted_for_execution(capture, fitted, config)
    assert actual is fitted and owner is expected_owner
    assert len(calls) == 1 and calls[0][0] is fitted
    assert calls[0][1]["context"] is capture.context
    assert calls[0][1]["request"] is capture.request


def test_declared_fit_reuse_retains_actual_working_frame_mutation_refusal(monkeypatch):
    capture, fitted, config, _ = _invented_case(bound=True)
    _forbid_repeat_work(monkeypatch)
    fitted._frame.iloc[0, 0] += 1.0
    with pytest.raises(execution.AuthoritativeSearchExecutionError) as caught:
        execution._declared_fitted_for_execution(capture, fitted, config)
    assert caught.value.code == "BOUND_FIT_REOPEN_CAPABILITY_INVALID"


def test_declared_fit_reuse_never_executes_a_verification_only_fit(monkeypatch):
    capture, fitted, config, owner = _invented_case(bound=True)
    _forbid_repeat_work(monkeypatch)
    owner.verification_only = True

    def forbidden_reference(*args, **kwargs):
        raise AssertionError("Verification-only refusal must precede the reference consumer")

    monkeypatch.setattr(execution, "bound_fitted_state_reference", forbidden_reference)
    with pytest.raises(execution.AuthoritativeSearchExecutionError) as caught:
        execution._declared_fitted_for_execution(capture, fitted, config)
    assert caught.value.code == "BOUND_FIT_VERIFICATION_ONLY_NOT_EXECUTABLE"
