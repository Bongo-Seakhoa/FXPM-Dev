"""INVENTED source-verification callbacks; actual native scope/resolver lifecycle.

These tests prove software reuse and isolation only. No native activation,
source CSV, model, financial result or Prefix source-pass acceptance is claimed.
"""
from dataclasses import asdict
import inspect
from pathlib import Path
from types import SimpleNamespace

import pytest

import pm_native_activation as native
import pm_native_prefix_production as prefix


_PROVENANCE = "INVENTED_RESOLUTION_METADATA_NOT_NATIVE_OR_FINANCIAL_AUTHORITY"


@pytest.fixture
def resolution_case(tmp_path, monkeypatch):
    calls, authority_reads, identity_reads = [], [], []
    outcome = {"value": "valid"}

    def identity(data_dir):
        return native._ActiveAuthorityIdentity(Path(data_dir).resolve(), b"INVENTED_ACTIVE",
            "a" * 64, "b" * 64, "c" * 64, "d" * 64)

    def read_identity(data_dir):
        identity_reads.append(Path(data_dir).resolve())
        return identity(data_dir)

    def read_authority(data_dir):
        authority_reads.append(Path(data_dir).resolve())
        # Labelled receipt-origin stand-in: only real scope-consumed fields are supplied.
        return SimpleNamespace(**asdict(identity(data_dir)))

    def single_pass(data_dir, symbol, timeframe, *, production_contract_required=False):
        root = Path(data_dir).resolve()
        calls.append((root, symbol, timeframe, production_contract_required))
        if outcome["value"] == "raise":
            raise RuntimeError("INVENTED_SINGLE_PASS_FAILURE")
        result = native.ActiveNativeResolution(symbol=symbol, timeframe=timeframe,
            data_path=root / "INVENTED_NOT_OPENED.csv", sidecar_path=root / "INVENTED_NOT_OPENED.json",
            activation_id="INVENTED_ACTIVATION", activation_manifest_path=root / "INVENTED_MANIFEST.json",
            activation_manifest_sha256="b" * 64,
            entry={"fixture_provenance": _PROVENANCE, "nested": {"value": "original"}})
        if outcome["value"] == "invalid":
            result.errors.append("INVENTED_SOURCE_REFUSAL")
        return result

    monkeypatch.setattr(native, "_read_active_authority_identity", read_identity)
    monkeypatch.setattr(native, "_load_receipt_backed_native_authority", read_authority)
    monkeypatch.setattr(native, "_resolve_active_native_artifact_single_pass", single_pass)
    return SimpleNamespace(root=tmp_path.resolve(), calls=calls, authority_reads=authority_reads,
        identity_reads=identity_reads, outcome=outcome)


def _resolve(case, *, symbol="UNITUSD", timeframe="H1", production=True, root=None):
    return prefix.resolve_active_native_artifact(case.root if root is None else root,
        symbol, timeframe, production_contract_required=production)


def test_valid_cell_is_admitted_once_with_symbol_timeframe_mode_and_root_separation(resolution_case):
    case = resolution_case
    with prefix._owned_native_resolution_scope() as state:
        scope = state["scope"]
        assert _resolve(case).valid
        assert _resolve(case).valid
        assert len(case.calls) == 1
        assert _resolve(case, symbol="OTHERUSD").valid
        assert _resolve(case, timeframe="M5").valid
        assert _resolve(case, production=False).valid
        assert _resolve(case, production=False, root=case.root / "other-root").valid
        assert len(case.calls) == 5
        assert len(state["resolutions"]) == 5
        # The original native scope still admits shared production authority once.
        assert len(case.authority_reads) == 1
    assert state["resolutions"] == {} and scope._closed



def test_returned_resolution_entries_and_errors_do_not_alias_admitted_metadata(resolution_case):
    case = resolution_case
    with prefix._owned_native_resolution_scope():
        first = _resolve(case)
        first.entry["nested"]["value"] = "INVENTED_CALLER_MUTATION"
        first.errors.append("INVENTED_CALLER_ERROR")
        second = _resolve(case)
        assert second is not first and second.valid
        assert second.entry["nested"] == {"value": "original"}
        assert second.errors == []
        second.entry["nested"]["value"] = "INVENTED_SECOND_MUTATION"
        second.errors.append("INVENTED_SECOND_ERROR")
        third = _resolve(case)
        assert third.entry["nested"] == {"value": "original"} and third.errors == []
        assert len(case.calls) == 1



def test_invalid_and_raised_resolutions_are_never_memoized(resolution_case):
    case = resolution_case
    with prefix._owned_native_resolution_scope() as state:
        case.outcome["value"] = "invalid"
        assert not _resolve(case, production=False).valid
        assert not _resolve(case, production=False).valid
        assert len(case.calls) == 2 and state["resolutions"] == {}
        case.outcome["value"] = "raise"
        for _ in range(2):
            with pytest.raises(RuntimeError, match="INVENTED_SINGLE_PASS_FAILURE"):
                _resolve(case, production=False)
        assert len(case.calls) == 4 and state["resolutions"] == {}



def test_owned_scope_joins_closes_on_exception_and_later_cold_calls_are_fresh(resolution_case):
    case = resolution_case
    observed_scopes, observed_states = [], []

    @prefix._owned_native_resolution_call
    def inner(*, root):
        observed_scopes.append(native._PRODUCTION_NATIVE_AUTHORITY_SCOPE.get())
        observed_states.append(prefix._NATIVE_CELL_RESOLUTION_STATE.get())
        return prefix.resolve_active_native_artifact(root, "UNITUSD", "H1", production_contract_required=True)

    def owner(*, root):
        first = inner(root=root)
        second = inner(root=root)
        assert first.valid and second.valid
        assert len(observed_states[-1]["resolutions"]) == 1
        return observed_scopes[-1]

    owned = prefix._owned_native_resolution_call(owner)
    assert inspect.signature(owned) == inspect.signature(owner)
    first_scope = owned(root=case.root)
    assert observed_scopes[-2:] == [first_scope, first_scope]
    assert first_scope._closed and observed_states[-1]["resolutions"] == {}
    assert prefix._NATIVE_CELL_RESOLUTION_STATE.get() is None
    assert native._PRODUCTION_NATIVE_AUTHORITY_SCOPE.get() is None
    assert len(case.calls) == 1
    with native.production_native_authority_verification_scope() as existing:
        assert owned(root=case.root) is existing
        assert not existing._closed and observed_states[-1]["resolutions"] == {}
        assert prefix._NATIVE_CELL_RESOLUTION_STATE.get() is None
    assert existing._closed and observed_states[-1]["resolutions"] == {}
    assert len(case.calls) == 2

    @prefix._owned_native_resolution_call
    def failing():
        inner(root=case.root)
        raise ValueError("INVENTED_OWNER_FAILURE")

    with pytest.raises(ValueError, match="INVENTED_OWNER_FAILURE"):
        failing()
    failed_scope = observed_scopes[-1]
    assert failed_scope._closed and observed_states[-1]["resolutions"] == {}
    assert prefix._NATIVE_CELL_RESOLUTION_STATE.get() is None
    assert native._PRODUCTION_NATIVE_AUTHORITY_SCOPE.get() is None
    assert len(case.calls) == 3
    assert _resolve(case).valid and _resolve(case).valid
    assert len(case.calls) == 5

