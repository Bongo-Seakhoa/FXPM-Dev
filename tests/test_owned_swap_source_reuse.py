"""INVENTED authority/terms stand-ins; real scope lifecycle, no source or trading proof."""
import hashlib
import json
from types import SimpleNamespace

import pytest

import pm_broker_terms as broker
import pm_native_activation as activation


LABEL = "INVENTED_METADATA_ADMISSION_AND_TERMS_RESOLVER_NO_AUTHORITY"


def _case(tmp_path, monkeypatch):
    data_dir = (tmp_path / "INVENTED-data").resolve()
    native_root = data_dir / "native"
    activation_id = "INVENTED_SCOPE_ONLY"
    receipt = {"activation_id": activation_id, "fixture_provenance": LABEL}
    encode = lambda body: json.dumps(body, sort_keys=True, separators=(",", ":")).encode("utf-8")
    receipt_bytes = encode(receipt)
    receipt_sha = hashlib.sha256(receipt_bytes).hexdigest()
    manifest = {"activation_id": activation_id, "entries": {},
        "semantic_receipt_relative_path": "receipts/INVENTED.json",
        "semantic_receipt_sha256": receipt_sha, "fixture_provenance": LABEL,
        "production_authoritative": True, "native_is_signal_authority": True,
        "silent_fallback_allowed": False, "counts": {"pending": 0, "terminal": 600, "total": 600},
        "universe_eligibility": {"schema_version": broker.PRICE_UNIVERSE_V4_SCHEMA,
            "trading_verdicts_emitted": False, "performance_inputs_used": False,
            "required_timeframes": ["M5", "M15", "M30", "H1", "H4", "D1"],
            "cell_statuses": {}, "evidence_candidate_timeframes_by_symbol": {}}}
    manifest_bytes = encode(manifest)
    manifest_sha = hashlib.sha256(manifest_bytes).hexdigest()
    pointer = {"activation_id": activation_id, "manifest_relative_path": "activations/INVENTED.json",
        "manifest_sha256": manifest_sha, "production_authoritative": True, "fixture_provenance": LABEL}
    pointer_bytes = encode(pointer)
    pointer_sha = hashlib.sha256(pointer_bytes).hexdigest()
    authority = activation.ReceiptBackedNativeAuthority(
        data_dir=data_dir, active_pointer_bytes=pointer_bytes, active_pointer_sha256=pointer_sha,
        activation_id=activation_id, activation_manifest_path=native_root / "activations/INVENTED.json",
        activation_manifest_bytes=manifest_bytes, activation_manifest_sha256=manifest_sha,
        manifest=manifest, entries={}, run_manifest_path=native_root / "INVENTED-run.json",
        run_manifest_bytes=b"{}", run_manifest_sha256=hashlib.sha256(b"{}").hexdigest(), run_manifest={},
        semantic_receipt_path=native_root / "receipts/INVENTED.json", semantic_receipt_bytes=receipt_bytes,
        semantic_receipt_sha256=receipt_sha, semantic_receipt=receipt,
        semantic_verifier_build_digest="a" * 64)
    identity = activation._ActiveAuthorityIdentity(data_dir, pointer_bytes, pointer_sha,
        manifest_sha, receipt_sha, "a" * 64)
    loaded, checked, resolved = [], [], []

    def load(observed):
        assert observed == data_dir
        loaded.append(observed)
        return authority

    def recheck(observed):
        assert observed == data_dir
        checked.append(observed)
        return identity

    def resolve(root, symbol, timeframe, *, price, revalidate_price_authority):
        assert root == native_root and price["activation_id"] == activation_id
        assert price["contract_mode"] == "promoted-v4"
        revalidate_price_authority()
        resolved.append((root, symbol.strip().casefold(), timeframe.strip().upper()))
        return {"status": "AVAILABLE", "fixture_provenance": LABEL,
            "identity": {"symbol": symbol.strip().casefold(), "timeframe": timeframe.strip().upper()},
            "detached": {"values": ["INVENTED-original"]}}

    # Only the upstream authority admission and economic terms resolver are doubled.
    monkeypatch.setattr(activation, "_load_receipt_backed_native_authority", load)
    monkeypatch.setattr(activation, "_read_active_authority_identity", recheck)
    monkeypatch.setattr(broker, "_resolve_active_swap_contract_with_price_authority", resolve)
    return SimpleNamespace(root=native_root, loaded=loaded, checked=checked,
        resolved=resolved, resolve=resolve, authority=authority)


def test_owned_swap_hit_is_normalized_and_detached(tmp_path, monkeypatch):
    case = _case(tmp_path, monkeypatch)
    with activation.production_native_authority_verification_scope() as scope, broker._owned_swap_source_scope() as contracts:
        first = broker.resolve_active_swap_contract(case.root, " eurusd ", " h1 ")
        first["detached"]["values"].append("INVENTED-caller-mutation")
        hit = broker.resolve_active_swap_contract(case.root.parent / "native" / ".", "EURUSD", "H1")
        assert hit["detached"]["values"] == ["INVENTED-original"]
        hit["detached"]["values"].clear()
        assert broker.resolve_active_swap_contract(case.root, "eurusd", "h1")["detached"]["values"] == ["INVENTED-original"]
        assert len(case.resolved) == len(case.loaded) == 1
        assert activation._PRODUCTION_NATIVE_AUTHORITY_SCOPE.get() is scope
    assert scope.stats()["closed"] is True and contracts == {}
    assert activation._PRODUCTION_NATIVE_AUTHORITY_SCOPE.get() is None


def test_distinct_swap_keys_and_each_cold_scope_resolve_fresh(tmp_path, monkeypatch):
    case = _case(tmp_path, monkeypatch)
    with activation.production_native_authority_verification_scope(), broker._owned_swap_source_scope():
        for symbol, timeframe in [("EURUSD", "H1"), ("XAUUSD", "H1"), ("EURUSD", "M5")]:
            actual = broker.resolve_active_swap_contract(case.root, symbol, timeframe)
            assert actual["identity"] == {"symbol": symbol.casefold(), "timeframe": timeframe}
        assert len(case.resolved) == 3 and len(case.loaded) == 1
    with activation.production_native_authority_verification_scope(), broker._owned_swap_source_scope():
        broker.resolve_active_swap_contract(case.root, "EURUSD", "H1")
    broker.resolve_active_swap_contract(case.root, "EURUSD", "H1")
    broker.resolve_active_swap_contract(case.root, "EURUSD", "H1")
    assert len(case.resolved) == 6 and len(case.loaded) == 4
    assert activation._PRODUCTION_NATIVE_AUTHORITY_SCOPE.get() is None


def test_unavailable_and_failed_swap_results_are_not_cached(tmp_path, monkeypatch):
    case = _case(tmp_path, monkeypatch)
    calls = []

    def resolve(*args, **kwargs):
        calls.append(None)
        if len(calls) == 1:
            return {"status": "UNAVAILABLE", "fixture_provenance": LABEL}
        if len(calls) == 2:
            raise broker.BrokerTermsError("INVENTED_TERMS_READ_FAILURE")
        return case.resolve(*args, **kwargs)

    monkeypatch.setattr(broker, "_resolve_active_swap_contract_with_price_authority", resolve)
    with activation.production_native_authority_verification_scope(), broker._owned_swap_source_scope():
        assert broker.resolve_active_swap_contract(case.root, "EURUSD", "H1")["status"] == "UNAVAILABLE"
        with pytest.raises(broker.BrokerTermsError, match="INVENTED_TERMS_READ_FAILURE"):
            broker.resolve_active_swap_contract(case.root, "EURUSD", "H1")
        result = broker.resolve_active_swap_contract(case.root, "EURUSD", "H1")
        assert result["status"] == "AVAILABLE"
        assert broker.resolve_active_swap_contract(case.root, "EURUSD", "H1") == result
        assert len(calls) == 3


def test_broker_helper_joins_existing_scope_and_closes_only_owned_scope(tmp_path, monkeypatch):
    case = _case(tmp_path, monkeypatch)
    with activation.production_native_authority_verification_scope() as outer:
        with broker._receipt_backed_price_authority_scope(case.root) as (price, revalidate):
            assert activation._PRODUCTION_NATIVE_AUTHORITY_SCOPE.get() is outer
            assert price["activation_id"] == case.authority.activation_id
            revalidate()
        assert outer.stats()["closed"] is False
        assert activation._PRODUCTION_NATIVE_AUTHORITY_SCOPE.get() is outer
    with broker._receipt_backed_price_authority_scope(case.root) as (_price, revalidate):
        owned = activation._PRODUCTION_NATIVE_AUTHORITY_SCOPE.get()
        assert owned is not None and owned is not outer and owned.stats()["closed"] is False
        revalidate()
    assert owned.stats()["closed"] is True and len(case.loaded) == 2
    assert activation._PRODUCTION_NATIVE_AUTHORITY_SCOPE.get() is None


def test_broker_helper_preserves_actual_authority_root_mismatch_guard(tmp_path, monkeypatch):
    case = _case(tmp_path, monkeypatch)
    with activation.production_native_authority_verification_scope() as outer:
        with broker._receipt_backed_price_authority_scope(case.root):
            pass
        with pytest.raises(broker.BrokerTermsError, match="receipt-backed native authority identity mismatch"):
            with broker._receipt_backed_price_authority_scope(tmp_path / "INVENTED-other" / "native"):
                pytest.fail("mismatched native root must not yield a price view")
        assert activation._PRODUCTION_NATIVE_AUTHORITY_SCOPE.get() is outer
        assert case.resolved == []
