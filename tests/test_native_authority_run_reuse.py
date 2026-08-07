from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from contextvars import copy_context
from copy import deepcopy
import hashlib
import json
import os
from pathlib import Path
import threading
from typing import Any

import pytest

import pm_native_activation as native


def _write_identity(data_dir: Path) -> tuple[Path, Path, str]:
    native_root = data_dir / "native"
    manifest_path = native_root / "activations" / "scope-activation.json"
    manifest_bytes = b'{"activation_id":"scope-activation","generation":1}\n'
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_bytes(manifest_bytes)
    manifest_sha = hashlib.sha256(manifest_bytes).hexdigest()
    pointer_path = native_root / "ACTIVE.json"
    pointer = {
        "schema_version": native.ACTIVE_POINTER_SCHEMA,
        "activation_id": "scope-activation",
        "manifest_relative_path": "activations/scope-activation.json",
        "manifest_sha256": manifest_sha,
        "activated_at_utc": "2026-07-22T00:00:00Z",
        "production_authoritative": True,
    }
    pointer_path.write_text(
        json.dumps(pointer, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return pointer_path, manifest_path, manifest_sha


def _valid_universe(manifest_path: Path, manifest_sha: str) -> native.ActiveNativeUniverse:
    return native.ActiveNativeUniverse(
        activation_id="scope-activation",
        activation_manifest_path=manifest_path.resolve(),
        activation_manifest_sha256=manifest_sha,
        requested_symbols=native.NATIVE_PRODUCTION_SYMBOLS,
        required_timeframes=native.NATIVE_PRODUCTION_TIMEFRAMES,
        eligible_symbols=native.NATIVE_PRODUCTION_SYMBOLS,
        cell_statuses={
            symbol: {
                timeframe: native.NATIVE_EVIDENCE_CANDIDATE
                for timeframe in native.NATIVE_PRODUCTION_TIMEFRAMES
            }
            for symbol in native.NATIVE_PRODUCTION_SYMBOLS
        },
        entries={"sentinel": {"value": 1}},
    )


def _install_full_verifier(
    monkeypatch: pytest.MonkeyPatch,
    universe: native.ActiveNativeUniverse,
) -> list[int]:
    thread_ids: list[int] = []

    def _full(*args: Any, **kwargs: Any) -> native.ActiveNativeUniverse:
        thread_ids.append(threading.get_ident())
        return universe

    monkeypatch.setattr(native, "_resolve_active_native_universe_uncached", _full)
    return thread_ids


def _resolve(data_dir: Path) -> native.ActiveNativeUniverse:
    return native.resolve_active_native_universe(
        data_dir,
        native.NATIVE_PRODUCTION_SYMBOLS,
        required_timeframes=native.NATIVE_PRODUCTION_TIMEFRAMES,
        verify_exclusion_evidence=True,
        production_contract_required=True,
    )


@pytest.mark.parametrize(("total_calls", "reuse_calls"), [(14, 13), (6, 5)])
def test_scope_runs_one_full_verification_then_exact_identity_revalidation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    total_calls: int,
    reuse_calls: int,
) -> None:
    data_dir = tmp_path / "data"
    pointer_path, manifest_path, manifest_sha = _write_identity(data_dir)
    full_result = _valid_universe(manifest_path, manifest_sha)
    full_thread_ids = _install_full_verifier(monkeypatch, full_result)
    identity_reader = native._read_active_authority_identity
    identity_thread_ids: list[int] = []

    def _record_identity(path: Path) -> Any:
        identity_thread_ids.append(threading.get_ident())
        return identity_reader(path)

    monkeypatch.setattr(native, "_read_active_authority_identity", _record_identity)
    scope = native.production_native_authority_verification_scope()

    with scope:
        results = [_resolve(data_dir) for _ in range(total_calls)]
        open_stats = scope.stats()
        assert open_stats == {
            "schema_version": (
                native.PRODUCTION_NATIVE_AUTHORITY_VERIFICATION_SCOPE_SCHEMA
            ),
            "scope": open_stats["scope"],
            "full_verification_count": 1,
            "identity_revalidation_attempt_count": reuse_calls,
            "identity_revalidation_count": reuse_calls,
            "identity_revalidation_failure_count": 0,
            "active_pointer_sha256": hashlib.sha256(
                pointer_path.read_bytes()
            ).hexdigest(),
            "activation_manifest_sha256": manifest_sha,
            "pid": os.getpid(),
            "thread_id": threading.get_ident(),
            "closed": False,
            "poisoned": False,
            "poison_reason": None,
        }
        assert len(open_stats["scope"]) == 64
        assert open_stats["scope"] == open_stats["scope"].lower()
        assert set(open_stats["scope"]) <= set("0123456789abcdef")
        scope.close()
        assert scope.stats()["closed"] is True

    assert full_thread_ids == [threading.get_ident()]
    assert len(identity_thread_ids) == 2 + (2 * reuse_calls)
    assert set(identity_thread_ids) == {threading.get_ident()}
    assert len({id(value) for value in results}) == total_calls
    assert all(value.valid for value in results)
    assert scope.stats()["closed"] is True


def test_scope_returns_isolated_copies_and_bypass_stays_uncached(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    data_dir = tmp_path / "data"
    _pointer_path, manifest_path, manifest_sha = _write_identity(data_dir)
    full_result = _valid_universe(manifest_path, manifest_sha)
    full_calls = _install_full_verifier(monkeypatch, full_result)

    with native.production_native_authority_verification_scope() as scope:
        first = _resolve(data_dir)
        first.errors.append("CALLER_MUTATION")
        first.entries["sentinel"]["value"] = 999
        full_result.entries["sentinel"]["value"] = 888
        second = _resolve(data_dir)

        assert second.errors == []
        assert second.entries["sentinel"]["value"] == 1
        assert first is not second

        bypass = native.resolve_active_native_universe(
            data_dir,
            required_timeframes=native.NATIVE_PRODUCTION_TIMEFRAMES,
            verify_exclusion_evidence=False,
            production_contract_required=False,
        )
        assert bypass is full_result
        assert len(full_calls) == 2
        assert scope.stats()["full_verification_count"] == 1
        assert scope.stats()["identity_revalidation_count"] == 1


def test_scope_rejects_semantically_equal_pointer_reserialization(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    data_dir = tmp_path / "data"
    pointer_path, manifest_path, manifest_sha = _write_identity(data_dir)
    _install_full_verifier(
        monkeypatch,
        _valid_universe(manifest_path, manifest_sha),
    )

    with native.production_native_authority_verification_scope() as scope:
        assert _resolve(data_dir).valid
        pointer = json.loads(pointer_path.read_text(encoding="utf-8"))
        pointer_path.write_text(
            json.dumps(pointer, separators=(",", ":")),
            encoding="utf-8",
        )
        with pytest.raises(
            RuntimeError,
            match="NATIVE_AUTHORITY_IDENTITY_REVALIDATION_FAILED",
        ):
            _resolve(data_dir)
        stats = scope.stats()
        assert stats["identity_revalidation_attempt_count"] == 1
        assert stats["identity_revalidation_count"] == 0
        assert stats["identity_revalidation_failure_count"] == 1
        assert stats["poisoned"] is True
        assert stats["poison_reason"] == "IDENTITY_BEFORE_MISMATCH"


def test_scope_rejects_manifest_mutation_under_unchanged_pointer(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    data_dir = tmp_path / "data"
    _pointer_path, manifest_path, manifest_sha = _write_identity(data_dir)
    _install_full_verifier(
        monkeypatch,
        _valid_universe(manifest_path, manifest_sha),
    )

    with native.production_native_authority_verification_scope() as scope:
        assert _resolve(data_dir).valid
        manifest_path.write_bytes(manifest_path.read_bytes() + b" ")
        with pytest.raises(
            RuntimeError,
            match="NATIVE_AUTHORITY_IDENTITY_REVALIDATION_FAILED",
        ):
            _resolve(data_dir)
        stats = scope.stats()
        assert stats["identity_revalidation_attempt_count"] == 1
        assert stats["identity_revalidation_count"] == 0
        assert stats["identity_revalidation_failure_count"] == 1
        assert stats["poisoned"] is True
        assert stats["poison_reason"] == "IDENTITY_READ_OR_COPY_FAILED"


def test_scope_rejects_pointer_change_during_first_full_verification(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    data_dir = tmp_path / "data"
    pointer_path, manifest_path, manifest_sha = _write_identity(data_dir)
    universe = _valid_universe(manifest_path, manifest_sha)

    def _full(*args: Any, **kwargs: Any) -> native.ActiveNativeUniverse:
        pointer = json.loads(pointer_path.read_text(encoding="utf-8"))
        pointer_path.write_text(
            json.dumps(pointer, separators=(",", ":")),
            encoding="utf-8",
        )
        return universe

    monkeypatch.setattr(native, "_resolve_active_native_universe_uncached", _full)

    with native.production_native_authority_verification_scope() as scope:
        with pytest.raises(
            RuntimeError,
            match="NATIVE_AUTHORITY_IDENTITY_CHANGED_DURING_FULL_VERIFICATION",
        ):
            _resolve(data_dir)
        stats = scope.stats()
        assert stats["full_verification_count"] == 1
        assert stats["identity_revalidation_attempt_count"] == 0
        assert stats["poisoned"] is True
        assert stats["poison_reason"] == (
            "IDENTITY_CHANGED_DURING_FULL_VERIFICATION"
        )


def test_scope_rejects_nested_and_copied_context_thread_drift(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    data_dir = tmp_path / "data"
    _pointer_path, manifest_path, manifest_sha = _write_identity(data_dir)
    _install_full_verifier(
        monkeypatch,
        _valid_universe(manifest_path, manifest_sha),
    )

    with native.production_native_authority_verification_scope() as scope:
        assert _resolve(data_dir).valid
        with pytest.raises(
            RuntimeError,
            match="NATIVE_AUTHORITY_VERIFICATION_SCOPE_NESTED",
        ):
            with native.production_native_authority_verification_scope():
                pass
        context = copy_context()
        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(context.run, _resolve, data_dir)
            with pytest.raises(
                RuntimeError,
                match=(
                    "NATIVE_AUTHORITY_VERIFICATION_SCOPE_EXECUTION_IDENTITY_MISMATCH"
                ),
            ):
                future.result()
        stats = scope.stats()
        assert stats["identity_revalidation_attempt_count"] == 1
        assert stats["identity_revalidation_failure_count"] == 1
        assert stats["poisoned"] is True
        assert stats["poison_reason"] == "SCOPE_EXECUTION_IDENTITY_MISMATCH"


def test_scope_rejects_process_identity_drift(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    data_dir = tmp_path / "data"
    _pointer_path, manifest_path, manifest_sha = _write_identity(data_dir)
    _install_full_verifier(
        monkeypatch,
        _valid_universe(manifest_path, manifest_sha),
    )

    with native.production_native_authority_verification_scope() as scope:
        assert _resolve(data_dir).valid
        original_pid = os.getpid()
        monkeypatch.setattr(native.os, "getpid", lambda: original_pid + 1)
        with pytest.raises(
            RuntimeError,
            match=(
                "NATIVE_AUTHORITY_VERIFICATION_SCOPE_EXECUTION_IDENTITY_MISMATCH"
            ),
        ):
            _resolve(data_dir)
        stats = scope.stats()
        assert stats["pid"] == original_pid
        assert stats["identity_revalidation_attempt_count"] == 1
        assert stats["identity_revalidation_failure_count"] == 1
        assert stats["poisoned"] is True
        assert stats["poison_reason"] == "SCOPE_EXECUTION_IDENTITY_MISMATCH"


def test_scope_close_clears_cache_and_exception_exit_resets_context(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    data_dir = tmp_path / "data"
    _pointer_path, manifest_path, manifest_sha = _write_identity(data_dir)
    full_result = _valid_universe(manifest_path, manifest_sha)
    full_calls = _install_full_verifier(monkeypatch, full_result)
    scope = native.production_native_authority_verification_scope()

    with pytest.raises(SystemExit):
        with scope:
            assert _resolve(data_dir).valid
            raise SystemExit(7)

    stats = scope.stats()
    assert stats["closed"] is True
    assert stats["poisoned"] is True
    assert stats["poison_reason"] == "SCOPE_BODY_EXCEPTION:SystemExit"
    assert scope._active_pointer_bytes is None
    assert scope._universe is None
    assert native._PRODUCTION_NATIVE_AUTHORITY_SCOPE.get() is None

    outside = _resolve(data_dir)
    assert outside is full_result
    assert len(full_calls) == 2
    with pytest.raises(
        RuntimeError,
        match="NATIVE_AUTHORITY_VERIFICATION_SCOPE_NOT_REUSABLE",
    ):
        with scope:
            pass


def test_scope_poisoned_when_full_verifier_raises_and_context_cleans_up(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    data_dir = tmp_path / "data"
    _write_identity(data_dir)

    def _raise(*args: Any, **kwargs: Any) -> native.ActiveNativeUniverse:
        raise ValueError("full verifier failed")

    monkeypatch.setattr(native, "_resolve_active_native_universe_uncached", _raise)
    scope = native.production_native_authority_verification_scope()
    with pytest.raises(ValueError, match="full verifier failed"):
        with scope:
            _resolve(data_dir)

    stats = scope.stats()
    assert stats["full_verification_count"] == 1
    assert stats["closed"] is True
    assert stats["poisoned"] is True
    assert stats["poison_reason"] == "FULL_VERIFICATION_EXCEPTION"
    assert native._PRODUCTION_NATIVE_AUTHORITY_SCOPE.get() is None


def test_stats_returns_fresh_snapshot(tmp_path: Path) -> None:
    del tmp_path
    scope = native.production_native_authority_verification_scope()
    first = scope.stats()
    changed = deepcopy(first)
    changed["full_verification_count"] = 999
    assert scope.stats()["full_verification_count"] == 0
