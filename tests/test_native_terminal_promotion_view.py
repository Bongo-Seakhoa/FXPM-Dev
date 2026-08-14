"""Receipt-backed native production authority boundary tests."""

from __future__ import annotations

import hashlib
import json
import shutil
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

import pytest

import pm_native_activation as native
import pm_production_anchor_plan as anchor_plan
from pm_native_request_contract import (
    PRODUCTION_SYMBOL_ORDER,
    PRODUCTION_TIMEFRAME_ORDER,
    build_v5_manifest_request,
    build_v5_request_contract,
)
from tests.native_v5_support import write_receipt_backed_v5_authority


UTC = timezone.utc
CUTOFF = datetime(2026, 7, 13, 9, 3, 17, tzinfo=UTC)


def _canonical(value: dict[str, Any]) -> bytes:
    return (json.dumps(value, indent=2, sort_keys=True) + "\n").encode("utf-8")


def _write_json(path: Path, value: Any) -> str:
    raw = _canonical(value)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(raw)
    return hashlib.sha256(raw).hexdigest()


def _canonical_artifact_fixture(
    tmp_path: Path,
) -> tuple[Path, native.ActiveNativeUniverse, dict[str, Path]]:
    data_dir = tmp_path / "canonical-data"
    native_root = data_dir / "native"
    activation_id = "activation-canonical-view"
    run_id = "run-canonical-view"
    common = build_v5_request_contract(
        requested_information_cutoff_utc=CUTOFF,
        warmup_days=500,
    )
    request = build_v5_manifest_request(
        common,
        symbols=PRODUCTION_SYMBOL_ORDER,
        timeframes=PRODUCTION_TIMEFRAME_ORDER,
    )
    resolver_calendar, resolver_errors = native._validated_calendar_contract(request)
    assert not resolver_errors
    run_manifest = {
        "schema_version": native.NATIVE_RUN_MANIFEST_SCHEMA,
        "run_id": run_id,
        "immutable_record": True,
        "complete": True,
        "request": request,
    }
    run_path = native_root / "runs" / run_id / "run_manifest.json"
    run_sha = _write_json(run_path, run_manifest)
    symbol = PRODUCTION_SYMBOL_ORDER[0]
    timeframe = PRODUCTION_TIMEFRAME_ORDER[0]
    bars_path = native_root / "cells" / symbol / timeframe / "bars.csv"
    sidecar_path = native_root / "cells" / symbol / timeframe / "sidecar.json"
    bars_path.parent.mkdir(parents=True, exist_ok=True)
    bars_path.write_bytes(b"timestamp,open,high,low,close\n")
    sidecar_sha = _write_json(sidecar_path, {"cell": f"{symbol}|{timeframe}"})
    bars_sha = hashlib.sha256(bars_path.read_bytes()).hexdigest()
    entry = {
        "status": native.NATIVE_EVIDENCE_CANDIDATE,
        "symbol": symbol,
        "timeframe": timeframe,
        "bars_relative_path": bars_path.relative_to(native_root).as_posix(),
        "bars_sha256": bars_sha,
        "sidecar_relative_path": sidecar_path.relative_to(native_root).as_posix(),
        "sidecar_sha256": sidecar_sha,
    }
    counts = {
        native.NATIVE_EVIDENCE_CANDIDATE: len(PRODUCTION_SYMBOL_ORDER)
        * len(PRODUCTION_TIMEFRAME_ORDER),
        native.NATIVE_SOURCE_UNAVAILABLE: 0,
        native.NATIVE_TECHNICAL_ERROR: 0,
        native.NATIVE_PENDING: 0,
        "total": len(PRODUCTION_SYMBOL_ORDER) * len(PRODUCTION_TIMEFRAME_ORDER),
        "terminal": len(PRODUCTION_SYMBOL_ORDER) * len(PRODUCTION_TIMEFRAME_ORDER),
        "symbols_with_evidence_candidates": len(PRODUCTION_SYMBOL_ORDER),
    }
    entries = {
        candidate_symbol: {
            candidate_timeframe: {
                **entry,
                "symbol": candidate_symbol,
                "timeframe": candidate_timeframe,
            }
            for candidate_timeframe in PRODUCTION_TIMEFRAME_ORDER
        }
        for candidate_symbol in PRODUCTION_SYMBOL_ORDER
    }
    manifest = {
        "schema_version": native.ACTIVATION_MANIFEST_SCHEMA,
        "activation_id": activation_id,
        "run_id": run_id,
        "status": "active",
        "production_authoritative": True,
        "native_is_signal_authority": True,
        "silent_fallback_allowed": False,
        "run_manifest_relative_path": f"runs/{run_id}/run_manifest.json",
        "run_manifest_sha256": run_sha,
        "previous_active_pointer_sha256": "a" * 64,
        "counts": counts,
        "universe_eligibility": {
            "schema_version": native.UNIVERSE_AUTHENTICITY_SCHEMA,
            "requested_symbols": list(PRODUCTION_SYMBOL_ORDER),
            "required_timeframes": list(PRODUCTION_TIMEFRAME_ORDER),
            "session_coverage_method": native.NATIVE_SESSION_COVERAGE_METHOD,
        },
        "entries": entries,
    }
    semantic_receipt = native.build_native_semantic_receipt(manifest)
    receipt_path = native_root / "semantic_receipts" / f"{activation_id}.json"
    receipt_sha = _write_json(receipt_path, semantic_receipt)
    manifest["semantic_receipt_relative_path"] = receipt_path.relative_to(
        native_root
    ).as_posix()
    manifest["semantic_receipt_sha256"] = receipt_sha
    manifest_path = native_root / "activations" / f"{activation_id}.json"
    manifest_sha = _write_json(manifest_path, manifest)
    pointer_path = native_root / "ACTIVE.json"
    _write_json(
        pointer_path,
        {
            "schema_version": native.ACTIVE_POINTER_SCHEMA,
            "activation_id": activation_id,
            "manifest_relative_path": f"activations/{activation_id}.json",
            "manifest_sha256": manifest_sha,
            "activated_at_utc": "2026-07-19T19:00:00Z",
            "production_authoritative": True,
        },
    )
    cell_statuses = {
        candidate_symbol: {
            candidate_timeframe: native.NATIVE_EVIDENCE_CANDIDATE
            for candidate_timeframe in PRODUCTION_TIMEFRAME_ORDER
        }
        for candidate_symbol in PRODUCTION_SYMBOL_ORDER
    }
    universe = native.ActiveNativeUniverse(
        activation_id=activation_id,
        activation_manifest_path=manifest_path.resolve(),
        activation_manifest_sha256=manifest_sha,
        requested_symbols=tuple(PRODUCTION_SYMBOL_ORDER),
        required_timeframes=tuple(PRODUCTION_TIMEFRAME_ORDER),
        eligible_symbols=tuple(PRODUCTION_SYMBOL_ORDER),
        excluded_symbols={},
        cell_statuses=cell_statuses,
        symbols_with_active_cells=tuple(PRODUCTION_SYMBOL_ORDER),
        fully_excluded_symbols=(),
        calendar_contract=resolver_calendar,
        eligibility_schema=native.UNIVERSE_AUTHENTICITY_SCHEMA,
        decision_basis=native.NATIVE_AUTHENTICITY_DECISION_BASIS,
        explicit_contract=True,
        exact_symbol_identity=True,
        entries={symbol: {timeframe: entry}},
        errors=[],
    )
    return (
        data_dir,
        universe,
        {
            "native_root": native_root,
            "manifest": manifest_path,
            "pointer": pointer_path,
            "bars": bars_path,
            "sidecar": sidecar_path,
            "receipt": receipt_path,
        },
    )


def _stub_runtime_cell_evidence(
    monkeypatch: pytest.MonkeyPatch,
    universe: native.ActiveNativeUniverse,
) -> None:
    monkeypatch.setattr(
        native,
        "resolve_active_native_universe",
        lambda *args, **kwargs: deepcopy(universe),
    )
    monkeypatch.setattr(
        native,
        "_verify_activation_cell_evidence",
        lambda *args, **kwargs: [],
    )


def test_per_cell_resolver_retains_canonical_selected_leaf_verification(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    data_dir, universe, paths = _canonical_artifact_fixture(tmp_path)
    _stub_runtime_cell_evidence(monkeypatch, universe)

    result = native.resolve_active_native_artifact(
        data_dir,
        PRODUCTION_SYMBOL_ORDER[0],
        PRODUCTION_TIMEFRAME_ORDER[0],
        production_contract_required=True,
    )

    assert result.valid, result.errors
    assert result.activation_manifest_path == paths["manifest"].resolve()
    assert result.activation_manifest_sha256 == universe.activation_manifest_sha256
    assert result.data_path == paths["bars"].resolve()
    assert result.sidecar_path == paths["sidecar"].resolve()


def test_per_cell_resolver_reuses_scoped_receipt_authority(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    data_dir, universe, _paths = _canonical_artifact_fixture(tmp_path)
    _stub_runtime_cell_evidence(monkeypatch, universe)
    real_load = native._load_receipt_backed_native_authority
    receipt_loads = 0

    def _load(*args: Any, **kwargs: Any) -> native.ReceiptBackedNativeAuthority:
        nonlocal receipt_loads
        receipt_loads += 1
        return real_load(*args, **kwargs)

    monkeypatch.setattr(native, "_load_receipt_backed_native_authority", _load)
    with native.production_native_authority_verification_scope() as scope:
        first = native.resolve_active_native_artifact(
            data_dir,
            PRODUCTION_SYMBOL_ORDER[0],
            PRODUCTION_TIMEFRAME_ORDER[0],
            production_contract_required=True,
        )
        second = native.resolve_active_native_artifact(
            data_dir,
            PRODUCTION_SYMBOL_ORDER[0],
            PRODUCTION_TIMEFRAME_ORDER[0],
            production_contract_required=True,
        )
        stats = scope.stats()

    assert first.valid, first.errors
    assert second == first
    assert receipt_loads == 1
    assert stats["full_verification_count"] == 1
    assert stats["identity_revalidation_count"] == 1


def test_per_cell_resolver_adds_only_the_outer_identity_bracket(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    data_dir, universe, _paths = _canonical_artifact_fixture(tmp_path)
    real_identity = native._read_active_authority_identity
    identity_reads = 0
    evidence_replays = 0
    universe_resolutions = 0

    def _identity(*args: Any, **kwargs: Any) -> Any:
        nonlocal identity_reads
        identity_reads += 1
        return real_identity(*args, **kwargs)

    def _evidence(*args: Any, **kwargs: Any) -> list[str]:
        nonlocal evidence_replays
        evidence_replays += 1
        return []

    def _universe(*args: Any, **kwargs: Any) -> native.ActiveNativeUniverse:
        nonlocal universe_resolutions
        universe_resolutions += 1
        return deepcopy(universe)

    monkeypatch.setattr(native, "_read_active_authority_identity", _identity)
    monkeypatch.setattr(native, "_verify_activation_cell_evidence", _evidence)
    monkeypatch.setattr(native, "_resolve_active_native_universe_uncached", _universe)

    baseline = native._resolve_active_native_artifact_single_pass(
        data_dir,
        PRODUCTION_SYMBOL_ORDER[0],
        PRODUCTION_TIMEFRAME_ORDER[0],
        production_contract_required=True,
    )
    baseline_identity_reads = identity_reads
    baseline_evidence_replays = evidence_replays
    baseline_universe_resolutions = universe_resolutions
    native.clear_unscoped_native_universe_memo()

    result = native.resolve_active_native_artifact(
        data_dir,
        PRODUCTION_SYMBOL_ORDER[0],
        PRODUCTION_TIMEFRAME_ORDER[0],
        production_contract_required=True,
    )

    assert baseline.valid, baseline.errors
    assert result == baseline
    assert baseline_identity_reads == 2
    assert baseline_evidence_replays == 1
    assert baseline_universe_resolutions == 1
    assert identity_reads - baseline_identity_reads == 4
    assert evidence_replays - baseline_evidence_replays == 1
    assert universe_resolutions - baseline_universe_resolutions == 1


def test_per_cell_resolver_rejects_active_identity_movement(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    data_dir, universe, _paths = _canonical_artifact_fixture(tmp_path)
    _stub_runtime_cell_evidence(monkeypatch, universe)
    identity = native._read_active_authority_identity(data_dir)
    moved = native._ActiveAuthorityIdentity(
        data_dir=identity.data_dir,
        active_pointer_bytes=identity.active_pointer_bytes + b" ",
        active_pointer_sha256="f" * 64,
        activation_manifest_sha256=identity.activation_manifest_sha256,
        semantic_receipt_sha256=identity.semantic_receipt_sha256,
        semantic_verifier_build_digest=identity.semantic_verifier_build_digest,
    )
    identities = iter((identity, moved))
    monkeypatch.setattr(
        native,
        "_read_active_authority_identity",
        lambda _data_dir: next(identities),
    )

    result = native.resolve_active_native_artifact(
        data_dir,
        PRODUCTION_SYMBOL_ORDER[0],
        PRODUCTION_TIMEFRAME_ORDER[0],
        production_contract_required=True,
    )

    assert not result.valid
    assert result.errors == ["NATIVE_ACTIVE_AUTHORITY_CHANGED_DURING_RESOLUTION"]


@pytest.mark.parametrize(
    ("failure_read", "expected_error", "expected_body_calls"),
    [
        (1, "NATIVE_ACTIVE_AUTHORITY_IDENTITY_INITIAL_READ_FAILED", 0),
        (2, "NATIVE_ACTIVE_AUTHORITY_IDENTITY_FINAL_READ_FAILED", 1),
    ],
)
def test_per_cell_resolver_types_active_identity_read_failure(
    monkeypatch: pytest.MonkeyPatch,
    failure_read: int,
    expected_error: str,
    expected_body_calls: int,
) -> None:
    identity = native._ActiveAuthorityIdentity(
        data_dir=Path("data").resolve(),
        active_pointer_bytes=b"{}",
        active_pointer_sha256="a" * 64,
        activation_manifest_sha256="b" * 64,
        semantic_receipt_sha256="c" * 64,
        semantic_verifier_build_digest="d" * 64,
    )
    identity_reads = 0
    body_calls = 0

    def _identity(_data_dir: Path) -> native._ActiveAuthorityIdentity:
        nonlocal identity_reads
        identity_reads += 1
        if identity_reads == failure_read:
            raise RuntimeError("injected authority read failure")
        return identity

    def _single_pass(*args: Any, **kwargs: Any) -> native.ActiveNativeResolution:
        nonlocal body_calls
        body_calls += 1
        return native.ActiveNativeResolution(symbol="EURUSD", timeframe="H1")

    monkeypatch.setattr(native, "_read_active_authority_identity", _identity)
    monkeypatch.setattr(
        native,
        "_resolve_active_native_artifact_single_pass",
        _single_pass,
    )

    result = native.resolve_active_native_artifact(Path("data"), " EURUSD ", "h1")

    assert not result.valid
    assert result.symbol == "EURUSD"
    assert result.timeframe == "H1"
    assert result.errors == [expected_error]
    assert body_calls == expected_body_calls


def test_per_cell_resolver_rejects_result_manifest_identity_mismatch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    identity = native._ActiveAuthorityIdentity(
        data_dir=Path("data").resolve(),
        active_pointer_bytes=b"{}",
        active_pointer_sha256="a" * 64,
        activation_manifest_sha256="b" * 64,
        semantic_receipt_sha256="c" * 64,
        semantic_verifier_build_digest="d" * 64,
    )
    monkeypatch.setattr(
        native,
        "_read_active_authority_identity",
        lambda _data_dir: identity,
    )
    monkeypatch.setattr(
        native,
        "_resolve_active_native_artifact_single_pass",
        lambda *args, **kwargs: native.ActiveNativeResolution(
            symbol="EURUSD",
            timeframe="H1",
            data_path=Path("bars.csv"),
            sidecar_path=Path("sidecar.json"),
            activation_manifest_sha256="e" * 64,
        ),
    )

    result = native.resolve_active_native_artifact(Path("data"), "EURUSD", "H1")

    assert not result.valid
    assert result.errors == ["NATIVE_ACTIVE_RESULT_MANIFEST_IDENTITY_MISMATCH"]


def test_per_cell_resolver_types_malformed_active_pointer_path(
    tmp_path: Path,
) -> None:
    native_root = tmp_path / "data" / "native"
    _write_json(
        native_root / "ACTIVE.json",
        {
            "manifest_relative_path": "malformed\x00path",
            "manifest_sha256": "a" * 64,
        },
    )

    result = native.resolve_active_native_artifact(
        tmp_path / "data",
        "EURUSD",
        "H1",
    )

    assert not result.valid
    assert result.errors == ["NATIVE_ACTIVE_AUTHORITY_IDENTITY_INITIAL_READ_FAILED"]


def test_anchor_loader_retains_canonical_active_manifest_support(
    tmp_path: Path,
) -> None:
    data_dir, universe, _paths = _canonical_artifact_fixture(tmp_path)
    _common, _run_sha, activation_id, activation_sha = (
        anchor_plan._load_verified_v5_common(data_dir, universe)
    )

    assert activation_id == universe.activation_id
    assert activation_sha == universe.activation_manifest_sha256


@pytest.fixture(scope="module")
def pristine_authority(tmp_path_factory: pytest.TempPathFactory) -> Path:
    data_dir = tmp_path_factory.mktemp("receipt-authority")
    write_receipt_backed_v5_authority(data_dir / "native")
    return data_dir


@pytest.fixture
def data_dir(tmp_path: Path, pristine_authority: Path) -> Path:
    copied = tmp_path / "data"
    shutil.copytree(pristine_authority, copied)
    return copied


def _paths(data_dir: Path) -> tuple[Path, Path, Path]:
    native_root = data_dir / "native"
    pointer_path = native_root / "ACTIVE.json"
    pointer = json.loads(pointer_path.read_text(encoding="utf-8"))
    manifest_path = native_root / pointer["manifest_relative_path"]
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    receipt_path = native_root / manifest["semantic_receipt_relative_path"]
    return pointer_path, manifest_path, receipt_path


def _rewrite_receipt_chain(
    data_dir: Path,
    mutate_receipt: Callable[[dict[str, Any]], None],
) -> None:
    pointer_path, manifest_path, receipt_path = _paths(data_dir)
    pointer = json.loads(pointer_path.read_text(encoding="utf-8"))
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
    mutate_receipt(receipt)
    receipt_path.write_bytes(_canonical(receipt))
    manifest["semantic_receipt_sha256"] = hashlib.sha256(
        receipt_path.read_bytes()
    ).hexdigest()
    manifest_path.write_bytes(_canonical(manifest))
    pointer["manifest_sha256"] = hashlib.sha256(manifest_path.read_bytes()).hexdigest()
    pointer_path.write_bytes(_canonical(pointer))


def _rewrite_manifest_and_repin(
    data_dir: Path,
    mutate_manifest: Callable[[dict[str, Any]], None],
) -> None:
    pointer_path, manifest_path, _receipt_path = _paths(data_dir)
    pointer = json.loads(pointer_path.read_text(encoding="utf-8"))
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    mutate_manifest(manifest)
    manifest_path.write_bytes(_canonical(manifest))
    pointer["manifest_sha256"] = hashlib.sha256(manifest_path.read_bytes()).hexdigest()
    pointer_path.write_bytes(_canonical(pointer))


def test_scope_returns_exact_immutable_600_leaf_authority(data_dir: Path) -> None:
    with native.production_native_authority_verification_scope() as scope:
        authority = scope.verify_receipt_backed_authority(data_dir)
        persisted = json.loads(authority.activation_manifest_bytes)
        assert authority.activation_id == "receipt-fixture-activation"
        assert authority.manifest == persisted
        assert authority.entries == persisted["entries"]
        assert len(authority.semantic_receipt["native_leaf_bindings"]) == 600
        assert authority.semantic_receipt["verification_mode"] == "FORCE_RAW"
        assert set(authority.semantic_receipt["runtime_versions"]) == {
            "python",
            "pandas",
            "numpy",
        }
        assert (
            tuple(
                item["relative_path"]
                for item in authority.semantic_receipt["semantic_verifier_sources"]
            )
            == native.NATIVE_SEMANTIC_VERIFIER_SOURCE_ROSTER
        )
        with pytest.raises(TypeError, match="immutable"):
            authority.semantic_receipt["native_leaf_bindings"].append({})
        scope.revalidate_receipt_backed_authority()
        stats = scope.stats()
        assert stats["full_verification_count"] == 1
        assert stats["identity_revalidation_count"] == 1


@pytest.mark.parametrize(
    ("mutation", "code"),
    [
        (
            lambda receipt: receipt.__setitem__(
                "semantic_verifier_build_digest", "0" * 64
            ),
            "NATIVE_SEMANTIC_BUILD_DIGEST_MISMATCH",
        ),
        (
            lambda receipt: receipt.__setitem__(
                "activation_manifest_preimage_sha256", "1" * 64
            ),
            "NATIVE_SEMANTIC_RECEIPT_PREIMAGE_MISMATCH",
        ),
        (
            lambda receipt: receipt["native_leaf_bindings"][0].__setitem__(
                "bars_sha256", "2" * 64
            ),
            "NATIVE_SEMANTIC_RECEIPT_LEAF_BINDINGS_MISMATCH",
        ),
        (
            lambda receipt: receipt["semantic_verifier_sources"].reverse(),
            "NATIVE_SEMANTIC_BUILD_ROSTER_MISMATCH",
        ),
    ],
)
def test_rechained_receipt_tamper_fails_typed(
    data_dir: Path,
    mutation: Callable[[dict[str, Any]], None],
    code: str,
) -> None:
    _rewrite_receipt_chain(data_dir, mutation)
    with native.production_native_authority_verification_scope() as scope:
        with pytest.raises(native.NativeSemanticReceiptError, match=code):
            scope.verify_receipt_backed_authority(data_dir)
        assert scope.stats()["poisoned"] is True


def test_runtime_versions_are_informational(data_dir: Path) -> None:
    _rewrite_receipt_chain(
        data_dir,
        lambda receipt: receipt.__setitem__(
            "runtime_versions",
            {"python": "future", "pandas": "future", "numpy": "future"},
        ),
    )
    with native.production_native_authority_verification_scope() as scope:
        authority = scope.verify_receipt_backed_authority(data_dir)
        assert authority.semantic_receipt["runtime_versions"]["python"] == "future"


def test_runtime_version_schema_requires_all_informational_keys(
    data_dir: Path,
) -> None:
    _rewrite_receipt_chain(
        data_dir,
        lambda receipt: receipt.__setitem__(
            "runtime_versions", {"python": "future", "pandas": "future"}
        ),
    )
    with native.production_native_authority_verification_scope() as scope:
        with pytest.raises(
            native.NativeSemanticReceiptError,
            match="NATIVE_SEMANTIC_RECEIPT_VERSIONS_INVALID",
        ):
            scope.verify_receipt_backed_authority(data_dir)


def test_coherently_rehashed_leaf_poison_cannot_cross_original_receipt(
    data_dir: Path,
) -> None:
    pointer_path, manifest_path, _receipt_path = _paths(data_dir)
    pointer = json.loads(pointer_path.read_text(encoding="utf-8"))
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    entry = manifest["entries"][native.NATIVE_PRODUCTION_SYMBOLS[0]]["M5"]
    bars_path = data_dir / "native" / entry["bars_relative_path"]
    bars_path.write_bytes(bars_path.read_bytes() + b"\n")
    entry["bars_sha256"] = hashlib.sha256(bars_path.read_bytes()).hexdigest()
    manifest_path.write_bytes(_canonical(manifest))
    pointer["manifest_sha256"] = hashlib.sha256(manifest_path.read_bytes()).hexdigest()
    pointer_path.write_bytes(_canonical(pointer))
    with native.production_native_authority_verification_scope() as scope:
        with pytest.raises(
            native.NativeSemanticReceiptError,
            match="NATIVE_SEMANTIC_RECEIPT_PREIMAGE_MISMATCH",
        ):
            scope.verify_receipt_backed_authority(data_dir)


def test_rechained_roster_change_is_not_a_valid_production_receipt(
    data_dir: Path,
) -> None:
    def _drop_symbol(manifest: dict[str, Any]) -> None:
        contract = manifest["universe_eligibility"]
        contract["requested_symbols"] = contract["requested_symbols"][:-1]

    _rewrite_manifest_and_repin(data_dir, _drop_symbol)
    pointer_path, manifest_path, receipt_path = _paths(data_dir)
    pointer = json.loads(pointer_path.read_text(encoding="utf-8"))
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
    receipt["activation_manifest_preimage_sha256"] = (
        native.native_semantic_manifest_preimage_sha256(manifest)
    )
    receipt_path.write_bytes(_canonical(receipt))
    manifest["semantic_receipt_sha256"] = hashlib.sha256(
        receipt_path.read_bytes()
    ).hexdigest()
    manifest_path.write_bytes(_canonical(manifest))
    pointer["manifest_sha256"] = hashlib.sha256(manifest_path.read_bytes()).hexdigest()
    pointer_path.write_bytes(_canonical(pointer))
    with native.production_native_authority_verification_scope() as scope:
        with pytest.raises(
            native.NativeSemanticReceiptError,
            match="NATIVE_SEMANTIC_RECEIPT_SYMBOL_ROSTER_INVALID",
        ):
            scope.verify_receipt_backed_authority(data_dir)


def test_missing_receipt_never_falls_back_to_raw_replay(
    data_dir: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _pointer, _manifest, receipt_path = _paths(data_dir)
    receipt_path.unlink()
    calls = 0

    def _unexpected_raw(*args: Any, **kwargs: Any) -> list[str]:
        nonlocal calls
        calls += 1
        raise AssertionError("raw replay must not be a receipt fallback")

    monkeypatch.setattr(native, "_verify_activation_cell_evidence", _unexpected_raw)
    result = native.resolve_active_native_universe(
        data_dir,
        production_contract_required=True,
    )
    assert "NATIVE_SEMANTIC_RECEIPT_MISSING" in result.errors
    artifact = native.resolve_active_native_artifact(
        data_dir,
        native.NATIVE_PRODUCTION_SYMBOLS[0],
        "M5",
        production_contract_required=True,
    )
    assert "NATIVE_SEMANTIC_RECEIPT_MISSING" in artifact.errors
    assert calls == 0


def test_final_identity_revalidation_detects_active_movement(data_dir: Path) -> None:
    with native.production_native_authority_verification_scope() as scope:
        scope.verify_receipt_backed_authority(data_dir)
        pointer_path, _manifest_path, _receipt_path = _paths(data_dir)
        pointer = json.loads(pointer_path.read_text(encoding="utf-8"))
        pointer["activated_at_utc"] = "2026-01-01T00:00:01Z"
        pointer_path.write_bytes(_canonical(pointer))
        with pytest.raises(
            RuntimeError,
            match="NATIVE_AUTHORITY_IDENTITY_REVALIDATION_FAILED",
        ):
            scope.revalidate_receipt_backed_authority()
        assert scope.stats()["poisoned"] is True


def test_final_identity_revalidation_detects_build_movement(
    data_dir: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    with native.production_native_authority_verification_scope() as scope:
        scope.verify_receipt_backed_authority(data_dir)
        observed = native.native_semantic_verifier_build()
        monkeypatch.setattr(
            native,
            "native_semantic_verifier_build",
            lambda: {**observed, "digest": "f" * 64},
        )
        with pytest.raises(
            RuntimeError,
            match="NATIVE_AUTHORITY_IDENTITY_REVALIDATION_FAILED",
        ):
            scope.revalidate_receipt_backed_authority()


def test_canonical_active_path_rejects_retired_promoted_view(data_dir: Path) -> None:
    pointer_path, _manifest_path, _receipt_path = _paths(data_dir)
    pointer = json.loads(pointer_path.read_text(encoding="utf-8"))
    pointer["manifest_relative_path"] = (
        f"activations/promoted/{pointer['activation_id']}.json"
    )
    pointer_path.write_bytes(_canonical(pointer))
    result = native.resolve_active_native_universe(data_dir)
    assert "NATIVE_ACTIVE_MANIFEST_PATH_IDENTITY_MISMATCH" in result.errors


def test_anchor_loader_rejects_retired_promoted_view(
    tmp_path: Path,
) -> None:
    data_dir, universe, paths = _canonical_artifact_fixture(tmp_path)
    pointer = json.loads(paths["pointer"].read_text(encoding="utf-8"))
    pointer["manifest_relative_path"] = (
        f"activations/promoted/{pointer['activation_id']}.json"
    )
    paths["pointer"].write_bytes(_canonical(pointer))

    with pytest.raises(
        anchor_plan.ProductionAnchorPlanError,
        match="ACTIVE_POINTER_AUTHORITY_MISMATCH",
    ):
        anchor_plan._load_verified_v5_common(data_dir, universe)
