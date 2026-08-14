import base64
import hashlib
import json
import os
from contextlib import contextmanager
from copy import deepcopy
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest

from pm_native_activation import (
    ACTIVE_POINTER_SCHEMA,
    NATIVE_EVIDENCE_CANDIDATE,
    NATIVE_SOURCE_UNAVAILABLE,
    NATIVE_TECHNICAL_ERROR,
    _v5_derive_recomputed_authenticity_state,
    _v5_has_unopposed_semantic_failure,
    classify_native_availability_cell,
    clear_unscoped_native_universe_memo,
    resolve_active_native_artifact,
    resolve_active_native_universe,
    sha256_file,
)
from pm_native_consensus import canonical_sha256
from pm_native_request_contract import PRODUCTION_SYMBOL_ORDER
import pm_native_request_contract as request_contract
from tools.acquire_native_history import (
    DEFAULT_TIMEFRAMES,
    cross_resolution_reference_map,
)
import tools.audit_activate_native_history as activation_tool
from tests.native_v5_support import (
    SYMBOL,
    activate_v5_run as _activate_v5_fixture,
    rewrite_no_history as _rewrite_no_history,
    write_v5_run as _write_v5_run,
)


def _terminal_promotion_fixture(native_root: Path, *, integrity_error: bool = False):
    activation_id = "activation-terminal-promotion"
    run_id = "run-terminal-promotion"
    run_manifest_path = native_root / "runs" / run_id / "run_manifest.json"
    run_manifest_path.parent.mkdir(parents=True, exist_ok=True)
    run_manifest_path.write_text("{}\n", encoding="utf-8")
    pointer_path = native_root / "ACTIVE.json"
    pointer_path.parent.mkdir(parents=True, exist_ok=True)
    pointer_path.write_bytes(
        activation_tool._canonical_json_bytes(
            {
                "schema_version": ACTIVE_POINTER_SCHEMA,
                "activation_id": "last-good",
                "manifest_relative_path": "activations/last-good.json",
                "manifest_sha256": "1" * 64,
                "activated_at_utc": "2026-07-18T00:00:00Z",
            }
        )
    )
    pointer_sha = sha256_file(pointer_path)
    statuses = {}
    entries = {}
    candidate_projection = {}
    unavailable_projection = {}
    for symbol in PRODUCTION_SYMBOL_ORDER:
        statuses[symbol] = {}
        entries[symbol] = {}
        candidate_projection[symbol] = ["D1"]
        unavailable_projection[symbol] = []
        for timeframe in DEFAULT_TIMEFRAMES:
            candidate = timeframe == "D1"
            state = NATIVE_EVIDENCE_CANDIDATE if candidate else NATIVE_TECHNICAL_ERROR
            statuses[symbol][timeframe] = state
            entries[symbol][timeframe] = {
                "status": state,
                "audit_status": "PASS" if candidate else "FAIL",
                "consensus_status": "PASS" if candidate else "FAIL",
                "reason_codes": (
                    ["SOURCE:SOURCE_BARS_MISSING"]
                    if integrity_error and symbol == PRODUCTION_SYMBOL_ORDER[0] and timeframe == "M5"
                    else ([] if candidate else ["CROSS_RESOLUTION_CONSENSUS_FAILED"])
                ),
            }
    counts = {
        NATIVE_EVIDENCE_CANDIDATE: len(PRODUCTION_SYMBOL_ORDER),
        NATIVE_SOURCE_UNAVAILABLE: 0,
        NATIVE_TECHNICAL_ERROR: len(PRODUCTION_SYMBOL_ORDER)
        * (len(DEFAULT_TIMEFRAMES) - 1),
        "pending": 0,
        "total": len(PRODUCTION_SYMBOL_ORDER) * len(DEFAULT_TIMEFRAMES),
        "terminal": len(PRODUCTION_SYMBOL_ORDER) * len(DEFAULT_TIMEFRAMES),
        "symbols_with_evidence_candidates": len(PRODUCTION_SYMBOL_ORDER),
    }
    manifest = {
        "schema_version": activation_tool.ACTIVATION_MANIFEST_SCHEMA,
        "activation_id": activation_id,
        "run_id": run_id,
        "run_manifest_relative_path": f"runs/{run_id}/run_manifest.json",
        "run_manifest_sha256": sha256_file(run_manifest_path),
        "status": "failed",
        "production_authoritative": True,
        "native_is_signal_authority": True,
        "silent_fallback_allowed": False,
        "previous_active_pointer_sha256": pointer_sha,
        "counts": counts,
        "universe_eligibility": {
            "schema_version": activation_tool.UNIVERSE_AUTHENTICITY_SCHEMA,
            "requested_symbols": list(PRODUCTION_SYMBOL_ORDER),
            "required_timeframes": list(DEFAULT_TIMEFRAMES),
            "cell_statuses": statuses,
            "terminal_states": [
                NATIVE_EVIDENCE_CANDIDATE,
                NATIVE_SOURCE_UNAVAILABLE,
                NATIVE_TECHNICAL_ERROR,
            ],
            "nonterminal_states": ["pending"],
            "evidence_candidate_timeframes_by_symbol": candidate_projection,
            "source_unavailable_timeframes_by_symbol": unavailable_projection,
            "symbols_with_evidence_candidates": list(PRODUCTION_SYMBOL_ORDER),
        },
        "entries": entries,
    }
    manifest_path = native_root / "activations" / f"{activation_id}.json"
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_bytes(activation_tool._canonical_json_bytes(manifest))
    return run_id, activation_id, pointer_path, manifest_path


def _stub_terminal_promotion_dependencies(monkeypatch):
    monkeypatch.setattr(
        activation_tool,
        "resolve_active_native_universe",
        lambda *args, **kwargs: SimpleNamespace(
            errors=[],
            requested_symbols=tuple(PRODUCTION_SYMBOL_ORDER),
            required_timeframes=tuple(DEFAULT_TIMEFRAMES),
        ),
    )
    monkeypatch.setattr(
        activation_tool,
        "_terminal_promotion_evidence_errors",
        lambda *args, **kwargs: [],
    )
    monkeypatch.setattr(
        activation_tool,
        "_terminal_promotion_postcheck",
        lambda *args, **kwargs: {
            "anchor_plan_receipt_sha256": "a" * 64,
            "portfolio_activation_roster_sha256": "b" * 64,
        },
    )


def _leave_candidate_with_recovery(
    native_root: Path,
    *,
    run_id: str,
    activation_id: str,
    manifest_path: Path,
    monkeypatch,
) -> None:
    """Model abrupt death after swap by bypassing only in-process cleanup."""

    with monkeypatch.context() as crash_patch:
        crash_patch.setattr(
            activation_tool,
            "_terminal_promotion_postcheck",
            lambda *args, **kwargs: (_ for _ in ()).throw(SystemExit(91)),
        )
        crash_patch.setattr(
            activation_tool,
            "_terminal_promotion_restore_and_clear_recovery",
            lambda *args, **kwargs: [],
        )
        with pytest.raises(SystemExit):
            activation_tool.promote_completed_terminal_activation(
                native_root,
                run_id=run_id,
                activation_id=activation_id,
                expected_manifest_sha256=sha256_file(manifest_path),
            )


def test_completed_terminal_grid_promotes_without_evidence_replay(tmp_path, monkeypatch):
    native_root = tmp_path / "data" / "native"
    run_id, activation_id, pointer_path, manifest_path = _terminal_promotion_fixture(
        native_root
    )
    original_pointer = pointer_path.read_bytes()
    original_manifest = manifest_path.read_bytes()
    monkeypatch.setattr(
        activation_tool,
        "resolve_active_native_universe",
        lambda *args, **kwargs: SimpleNamespace(
            errors=[],
            requested_symbols=tuple(PRODUCTION_SYMBOL_ORDER),
            required_timeframes=tuple(DEFAULT_TIMEFRAMES),
        ),
    )
    monkeypatch.setattr(
        activation_tool,
        "_terminal_promotion_evidence_errors",
        lambda *args, **kwargs: [],
    )
    monkeypatch.setattr(
        activation_tool,
        "_terminal_promotion_postcheck",
        lambda *args, **kwargs: {
            "anchor_plan_receipt_sha256": "a" * 64,
            "portfolio_activation_roster_sha256": "b" * 64,
        },
    )

    result = activation_tool.promote_completed_terminal_activation(
        native_root,
        run_id=run_id,
        activation_id=activation_id,
        expected_manifest_sha256=sha256_file(manifest_path),
    )

    assert result["status"] == "active"
    assert result["active_pointer_updated"] is True
    assert result["data_reacquired"] is False
    assert result["cell_evidence_replayed"] is False
    assert pointer_path.read_bytes() != original_pointer
    assert manifest_path.read_bytes() == original_manifest
    assert json.loads(manifest_path.read_text(encoding="utf-8"))["status"] == "failed"
    promoted_path = Path(result["manifest_path"])
    promoted = json.loads(promoted_path.read_text(encoding="utf-8"))
    source = json.loads(original_manifest.decode("utf-8"))
    assert promoted == {**source, "status": "active"}
    assert promoted_path.read_bytes() == activation_tool._canonical_json_bytes(promoted)
    assert sha256_file(promoted_path) == result["manifest_sha256"]

    receipt_path = Path(result["transition_receipt_path"])
    receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
    assert set(receipt) == {
        "schema_version",
        "activation_id",
        "run_id",
        "source_manifest_relative_path",
        "source_manifest_sha256",
        "promoted_manifest_relative_path",
        "promoted_manifest_sha256",
        "previous_active_pointer_sha256",
        "counts",
        "transition",
        "data_reacquired",
        "cell_evidence_replayed",
    }
    assert receipt["source_manifest_sha256"] == hashlib.sha256(
        original_manifest
    ).hexdigest()
    assert receipt["promoted_manifest_sha256"] == sha256_file(promoted_path)
    assert receipt["previous_active_pointer_sha256"] == hashlib.sha256(
        original_pointer
    ).hexdigest()
    assert receipt_path.read_bytes() == activation_tool._canonical_json_bytes(receipt)

    pointer = json.loads(pointer_path.read_text(encoding="utf-8"))
    assert set(pointer) == {
        "schema_version",
        "activation_id",
        "manifest_relative_path",
        "manifest_sha256",
        "activated_at_utc",
        "production_authoritative",
    }
    assert pointer["activation_id"] == activation_id
    assert pointer["manifest_relative_path"] == (
        f"activations/promoted/{activation_id}.json"
    )
    assert pointer["manifest_sha256"] == sha256_file(promoted_path)
    assert pointer["production_authoritative"] is True
    recovery_path = native_root / activation_tool._TERMINAL_PROMOTION_RECOVERY_FILENAME
    assert result["recovery_record_cleared"] is True
    assert not recovery_path.exists()

    committed_pointer = pointer_path.read_bytes()
    retry = activation_tool.promote_completed_terminal_activation(
        native_root,
        run_id=run_id,
        activation_id=activation_id,
        expected_manifest_sha256=sha256_file(manifest_path),
    )
    assert retry["idempotent_committed_reentry"] is True
    assert retry["active_pointer_updated"] is False
    assert retry["recovery_record_cleared"] is True
    assert pointer_path.read_bytes() == committed_pointer
    assert not recovery_path.exists()


def test_completed_terminal_promotion_rejects_source_integrity_error(
    tmp_path, monkeypatch
):
    native_root = tmp_path / "data" / "native"
    run_id, activation_id, pointer_path, manifest_path = _terminal_promotion_fixture(
        native_root,
        integrity_error=True,
    )
    original_pointer = pointer_path.read_bytes()
    original_manifest = manifest_path.read_bytes()
    monkeypatch.setattr(
        activation_tool,
        "resolve_active_native_universe",
        lambda *args, **kwargs: SimpleNamespace(
            errors=[],
            requested_symbols=tuple(PRODUCTION_SYMBOL_ORDER),
            required_timeframes=tuple(DEFAULT_TIMEFRAMES),
        ),
    )
    monkeypatch.setattr(
        activation_tool,
        "_terminal_promotion_evidence_errors",
        lambda *args, **kwargs: [],
    )

    with pytest.raises(ValueError, match="TERMINAL_PROMOTION_INTEGRITY_ERROR_PRESENT"):
        activation_tool.promote_completed_terminal_activation(
            native_root,
            run_id=run_id,
            activation_id=activation_id,
            expected_manifest_sha256=sha256_file(manifest_path),
        )

    assert pointer_path.read_bytes() == original_pointer
    assert manifest_path.read_bytes() == original_manifest


def test_completed_terminal_promotion_rolls_back_failed_postcheck(
    tmp_path, monkeypatch
):
    native_root = tmp_path / "data" / "native"
    run_id, activation_id, pointer_path, manifest_path = _terminal_promotion_fixture(
        native_root
    )
    original_pointer = pointer_path.read_bytes()
    original_manifest = manifest_path.read_bytes()
    def _resolve(*args, **kwargs):
        return SimpleNamespace(
            errors=[],
            requested_symbols=tuple(PRODUCTION_SYMBOL_ORDER),
            required_timeframes=tuple(DEFAULT_TIMEFRAMES),
        )

    monkeypatch.setattr(activation_tool, "resolve_active_native_universe", _resolve)
    monkeypatch.setattr(
        activation_tool,
        "_terminal_promotion_evidence_errors",
        lambda *args, **kwargs: [],
    )
    monkeypatch.setattr(
        activation_tool,
        "_terminal_promotion_postcheck",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            RuntimeError("SYNTHETIC_POSTCHECK_FAILURE")
        ),
    )
    with pytest.raises(RuntimeError, match="SYNTHETIC_POSTCHECK_FAILURE"):
        activation_tool.promote_completed_terminal_activation(
            native_root,
            run_id=run_id,
            activation_id=activation_id,
            expected_manifest_sha256=sha256_file(manifest_path),
        )

    assert pointer_path.read_bytes() == original_pointer
    assert manifest_path.read_bytes() == original_manifest


def test_completed_terminal_promotion_pointer_cas_preserves_superseder(
    tmp_path, monkeypatch
):
    native_root = tmp_path / "data" / "native"
    run_id, activation_id, pointer_path, manifest_path = _terminal_promotion_fixture(
        native_root
    )
    original_manifest = manifest_path.read_bytes()
    superseding_pointer = activation_tool._canonical_json_bytes(
        {
            "schema_version": ACTIVE_POINTER_SCHEMA,
            "activation_id": "concurrent-successor",
            "manifest_relative_path": "activations/concurrent-successor.json",
            "manifest_sha256": "c" * 64,
            "activated_at_utc": "2026-07-19T00:00:00Z",
            "production_authoritative": True,
        }
    )
    monkeypatch.setattr(
        activation_tool,
        "resolve_active_native_universe",
        lambda *args, **kwargs: SimpleNamespace(errors=[]),
    )
    monkeypatch.setattr(
        activation_tool,
        "_terminal_promotion_evidence_errors",
        lambda *args, **kwargs: [],
    )
    real_lock = activation_tool._active_pointer_lock
    lock_entries = 0

    @contextmanager
    def _racing_lock(root, **kwargs):
        nonlocal lock_entries
        with real_lock(root, **kwargs):
            lock_entries += 1
            if lock_entries == 1:
                pointer_path.write_bytes(superseding_pointer)
            yield

    monkeypatch.setattr(activation_tool, "_active_pointer_lock", _racing_lock)

    with pytest.raises(RuntimeError, match="TERMINAL_PROMOTION_POINTER_CAS_MISMATCH"):
        activation_tool.promote_completed_terminal_activation(
            native_root,
            run_id=run_id,
            activation_id=activation_id,
            expected_manifest_sha256=sha256_file(manifest_path),
        )

    assert pointer_path.read_bytes() == superseding_pointer
    assert manifest_path.read_bytes() == original_manifest


def test_completed_terminal_promotion_detects_postcheck_superseder_without_clobber(
    tmp_path, monkeypatch
):
    native_root = tmp_path / "data" / "native"
    run_id, activation_id, pointer_path, manifest_path = _terminal_promotion_fixture(
        native_root
    )
    original_manifest = manifest_path.read_bytes()
    superseding_pointer = activation_tool._canonical_json_bytes(
        {
            "schema_version": ACTIVE_POINTER_SCHEMA,
            "activation_id": "postcheck-successor",
            "manifest_relative_path": "activations/postcheck-successor.json",
            "manifest_sha256": "d" * 64,
            "activated_at_utc": "2026-07-19T00:00:01Z",
            "production_authoritative": True,
        }
    )
    monkeypatch.setattr(
        activation_tool,
        "resolve_active_native_universe",
        lambda *args, **kwargs: SimpleNamespace(errors=[]),
    )
    monkeypatch.setattr(
        activation_tool,
        "_terminal_promotion_evidence_errors",
        lambda *args, **kwargs: [],
    )

    def _supersede_during_postcheck(*args, **kwargs):
        pointer_path.write_bytes(superseding_pointer)
        return {
            "anchor_plan_receipt_sha256": "a" * 64,
            "portfolio_activation_roster_sha256": "b" * 64,
        }

    monkeypatch.setattr(
        activation_tool,
        "_terminal_promotion_postcheck",
        _supersede_during_postcheck,
    )

    with pytest.raises(RuntimeError, match="TERMINAL_PROMOTION_ROLLBACK_FAILED") as exc:
        activation_tool.promote_completed_terminal_activation(
            native_root,
            run_id=run_id,
            activation_id=activation_id,
            expected_manifest_sha256=sha256_file(manifest_path),
        )

    assert exc.value.__cause__ is not None
    assert "TERMINAL_PROMOTION_POINTER_CHANGED_DURING_POSTCHECK" in str(
        exc.value.__cause__
    )
    assert pointer_path.read_bytes() == superseding_pointer
    assert manifest_path.read_bytes() == original_manifest


def test_completed_terminal_promotion_rolls_back_interrupt_with_bounded_retry(
    tmp_path, monkeypatch
):
    native_root = tmp_path / "data" / "native"
    run_id, activation_id, pointer_path, manifest_path = _terminal_promotion_fixture(
        native_root
    )
    original_pointer = pointer_path.read_bytes()
    original_manifest = manifest_path.read_bytes()
    monkeypatch.setattr(
        activation_tool,
        "resolve_active_native_universe",
        lambda *args, **kwargs: SimpleNamespace(errors=[]),
    )
    monkeypatch.setattr(
        activation_tool,
        "_terminal_promotion_evidence_errors",
        lambda *args, **kwargs: [],
    )
    monkeypatch.setattr(
        activation_tool,
        "_terminal_promotion_postcheck",
        lambda *args, **kwargs: (_ for _ in ()).throw(KeyboardInterrupt()),
    )
    original_replace = activation_tool._atomic_replace_bytes
    rollback_attempts = 0

    def _transient_rollback_replace(path, payload):
        nonlocal rollback_attempts
        if Path(path) == pointer_path and payload == original_pointer:
            rollback_attempts += 1
            if rollback_attempts < 3:
                raise PermissionError(13, "simulated transient pointer share lock")
        return original_replace(path, payload)

    monkeypatch.setattr(
        activation_tool,
        "_atomic_replace_bytes",
        _transient_rollback_replace,
    )

    with pytest.raises(KeyboardInterrupt):
        activation_tool.promote_completed_terminal_activation(
            native_root,
            run_id=run_id,
            activation_id=activation_id,
            expected_manifest_sha256=sha256_file(manifest_path),
        )

    assert rollback_attempts == 3
    assert pointer_path.read_bytes() == original_pointer
    assert manifest_path.read_bytes() == original_manifest


@pytest.mark.parametrize("collision", ["view", "receipt"])
def test_completed_terminal_promotion_rejects_staged_artifact_collision(
    tmp_path, monkeypatch, collision
):
    native_root = tmp_path / "data" / "native"
    run_id, activation_id, pointer_path, manifest_path = _terminal_promotion_fixture(
        native_root
    )
    original_pointer = pointer_path.read_bytes()
    original_manifest = manifest_path.read_bytes()
    monkeypatch.setattr(
        activation_tool,
        "resolve_active_native_universe",
        lambda *args, **kwargs: SimpleNamespace(errors=[]),
    )
    monkeypatch.setattr(
        activation_tool,
        "_terminal_promotion_evidence_errors",
        lambda *args, **kwargs: [],
    )
    promoted_path = (
        native_root / "activations" / "promoted" / f"{activation_id}.json"
    )
    promoted_path.parent.mkdir(parents=True)
    if collision == "view":
        promoted_path.write_text("{}\n", encoding="utf-8")
        expected_error = "terminal promoted manifest collision"
    else:
        source = json.loads(original_manifest.decode("utf-8"))
        promoted_path.write_bytes(
            activation_tool._canonical_json_bytes({**source, "status": "active"})
        )
        receipt_path = (
            native_root / "transition_receipts" / f"{activation_id}.json"
        )
        receipt_path.parent.mkdir(parents=True)
        receipt_path.write_text("{}\n", encoding="utf-8")
        expected_error = "terminal promotion receipt collision"

    with pytest.raises(FileExistsError, match=expected_error):
        activation_tool.promote_completed_terminal_activation(
            native_root,
            run_id=run_id,
            activation_id=activation_id,
            expected_manifest_sha256=sha256_file(manifest_path),
        )

    assert pointer_path.read_bytes() == original_pointer
    assert manifest_path.read_bytes() == original_manifest


def test_terminal_promotion_reconciles_crash_after_record_before_swap(
    tmp_path, monkeypatch
):
    native_root = tmp_path / "data" / "native"
    run_id, activation_id, pointer_path, manifest_path = _terminal_promotion_fixture(
        native_root
    )
    original_pointer = pointer_path.read_bytes()
    _stub_terminal_promotion_dependencies(monkeypatch)
    recovery_path = native_root / activation_tool._TERMINAL_PROMOTION_RECOVERY_FILENAME

    with monkeypatch.context() as crash_patch:
        crash_patch.setattr(
            activation_tool,
            "_atomic_replace_bytes_with_retry",
            lambda *args, **kwargs: (_ for _ in ()).throw(SystemExit(92)),
        )
        crash_patch.setattr(
            activation_tool,
            "_terminal_promotion_restore_and_clear_recovery",
            lambda *args, **kwargs: [],
        )
        with pytest.raises(SystemExit):
            activation_tool.promote_completed_terminal_activation(
                native_root,
                run_id=run_id,
                activation_id=activation_id,
                expected_manifest_sha256=sha256_file(manifest_path),
            )

    assert pointer_path.read_bytes() == original_pointer
    recovery = json.loads(recovery_path.read_text(encoding="utf-8"))
    persisted_candidate = base64.b64decode(
        recovery["candidate_active_pointer_bytes_base64"], validate=True
    )
    result = activation_tool.promote_completed_terminal_activation(
        native_root,
        run_id=run_id,
        activation_id=activation_id,
        expected_manifest_sha256=sha256_file(manifest_path),
    )

    assert result["reconciled_recovery"] is True
    assert result["active_pointer_updated"] is True
    assert pointer_path.read_bytes() == persisted_candidate
    assert not recovery_path.exists()


def test_terminal_promotion_candidate_on_reentry_finishes_and_clears_record(
    tmp_path, monkeypatch
):
    native_root = tmp_path / "data" / "native"
    run_id, activation_id, pointer_path, manifest_path = _terminal_promotion_fixture(
        native_root
    )
    _stub_terminal_promotion_dependencies(monkeypatch)
    _leave_candidate_with_recovery(
        native_root,
        run_id=run_id,
        activation_id=activation_id,
        manifest_path=manifest_path,
        monkeypatch=monkeypatch,
    )
    candidate_pointer = pointer_path.read_bytes()
    recovery_path = native_root / activation_tool._TERMINAL_PROMOTION_RECOVERY_FILENAME
    assert recovery_path.is_file()

    result = activation_tool.promote_completed_terminal_activation(
        native_root,
        run_id=run_id,
        activation_id=activation_id,
        expected_manifest_sha256=sha256_file(manifest_path),
    )

    assert result["reconciled_recovery"] is True
    assert result["active_pointer_updated"] is False
    assert result["recovery_record_cleared"] is True
    assert pointer_path.read_bytes() == candidate_pointer
    assert not recovery_path.exists()


def test_terminal_promotion_candidate_reentry_failure_restores_exact_predecessor(
    tmp_path, monkeypatch
):
    native_root = tmp_path / "data" / "native"
    run_id, activation_id, pointer_path, manifest_path = _terminal_promotion_fixture(
        native_root
    )
    original_pointer = pointer_path.read_bytes()
    _stub_terminal_promotion_dependencies(monkeypatch)
    _leave_candidate_with_recovery(
        native_root,
        run_id=run_id,
        activation_id=activation_id,
        manifest_path=manifest_path,
        monkeypatch=monkeypatch,
    )
    monkeypatch.setattr(
        activation_tool,
        "_terminal_promotion_postcheck",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            RuntimeError("SYNTHETIC_RECOVERY_POSTCHECK_FAILURE")
        ),
    )

    with pytest.raises(RuntimeError, match="SYNTHETIC_RECOVERY_POSTCHECK_FAILURE"):
        activation_tool.promote_completed_terminal_activation(
            native_root,
            run_id=run_id,
            activation_id=activation_id,
            expected_manifest_sha256=sha256_file(manifest_path),
        )

    recovery_path = native_root / activation_tool._TERMINAL_PROMOTION_RECOVERY_FILENAME
    assert pointer_path.read_bytes() == original_pointer
    assert hashlib.sha256(pointer_path.read_bytes()).hexdigest() == hashlib.sha256(
        original_pointer
    ).hexdigest()
    assert not recovery_path.exists()


def test_terminal_promotion_candidate_fresh_precheck_failure_restores_predecessor(
    tmp_path, monkeypatch
):
    native_root = tmp_path / "data" / "native"
    run_id, activation_id, pointer_path, manifest_path = _terminal_promotion_fixture(
        native_root
    )
    original_pointer = pointer_path.read_bytes()
    _stub_terminal_promotion_dependencies(monkeypatch)
    _leave_candidate_with_recovery(
        native_root,
        run_id=run_id,
        activation_id=activation_id,
        manifest_path=manifest_path,
        monkeypatch=monkeypatch,
    )
    monkeypatch.setattr(
        activation_tool,
        "_terminal_promotion_evidence_errors",
        lambda *args, **kwargs: ["SYNTHETIC_FRESH_PRECHECK_FAILURE"],
    )

    with pytest.raises(ValueError, match="SYNTHETIC_FRESH_PRECHECK_FAILURE"):
        activation_tool.promote_completed_terminal_activation(
            native_root,
            run_id=run_id,
            activation_id=activation_id,
            expected_manifest_sha256=sha256_file(manifest_path),
        )

    recovery_path = native_root / activation_tool._TERMINAL_PROMOTION_RECOVERY_FILENAME
    assert pointer_path.read_bytes() == original_pointer
    assert not recovery_path.exists()


@pytest.mark.parametrize("damage", ["missing_view", "tampered_receipt"])
def test_terminal_promotion_candidate_staged_artifact_failure_restores_predecessor(
    tmp_path, monkeypatch, damage
):
    native_root = tmp_path / "data" / "native"
    run_id, activation_id, pointer_path, manifest_path = _terminal_promotion_fixture(
        native_root
    )
    original_pointer = pointer_path.read_bytes()
    _stub_terminal_promotion_dependencies(monkeypatch)
    _leave_candidate_with_recovery(
        native_root,
        run_id=run_id,
        activation_id=activation_id,
        manifest_path=manifest_path,
        monkeypatch=monkeypatch,
    )
    if damage == "missing_view":
        damaged_path = (
            native_root / "activations" / "promoted" / f"{activation_id}.json"
        )
        damaged_path.unlink()
        expected_error = FileNotFoundError
    else:
        damaged_path = (
            native_root / "transition_receipts" / f"{activation_id}.json"
        )
        damaged_path.write_text("{}\n", encoding="utf-8")
        expected_error = FileExistsError

    with pytest.raises(expected_error):
        activation_tool.promote_completed_terminal_activation(
            native_root,
            run_id=run_id,
            activation_id=activation_id,
            expected_manifest_sha256=sha256_file(manifest_path),
        )

    recovery_path = native_root / activation_tool._TERMINAL_PROMOTION_RECOVERY_FILENAME
    assert pointer_path.read_bytes() == original_pointer
    assert not recovery_path.exists()


def test_terminal_promotion_recovery_third_pointer_is_never_clobbered(
    tmp_path, monkeypatch
):
    native_root = tmp_path / "data" / "native"
    run_id, activation_id, pointer_path, manifest_path = _terminal_promotion_fixture(
        native_root
    )
    _stub_terminal_promotion_dependencies(monkeypatch)
    _leave_candidate_with_recovery(
        native_root,
        run_id=run_id,
        activation_id=activation_id,
        manifest_path=manifest_path,
        monkeypatch=monkeypatch,
    )
    third_pointer = activation_tool._canonical_json_bytes(
        {
            "schema_version": ACTIVE_POINTER_SCHEMA,
            "activation_id": "independent-third-pointer",
            "manifest_relative_path": "activations/independent-third-pointer.json",
            "manifest_sha256": "e" * 64,
            "activated_at_utc": "2026-07-19T00:00:02Z",
            "production_authoritative": True,
        }
    )
    pointer_path.write_bytes(third_pointer)
    recovery_path = native_root / activation_tool._TERMINAL_PROMOTION_RECOVERY_FILENAME
    recovery_bytes = recovery_path.read_bytes()

    with pytest.raises(
        RuntimeError, match="TERMINAL_PROMOTION_RECOVERY_ACTIVE_POINTER_DIVERGED"
    ):
        activation_tool.promote_completed_terminal_activation(
            native_root,
            run_id=run_id,
            activation_id=activation_id,
            expected_manifest_sha256=sha256_file(manifest_path),
        )

    assert pointer_path.read_bytes() == third_pointer
    assert recovery_path.read_bytes() == recovery_bytes


@pytest.mark.parametrize("tamper", ["candidate_hash", "backslash_path"])
def test_terminal_promotion_recovery_record_tamper_fails_closed(
    tmp_path, monkeypatch, tamper
):
    native_root = tmp_path / "data" / "native"
    run_id, activation_id, pointer_path, manifest_path = _terminal_promotion_fixture(
        native_root
    )
    _stub_terminal_promotion_dependencies(monkeypatch)
    _leave_candidate_with_recovery(
        native_root,
        run_id=run_id,
        activation_id=activation_id,
        manifest_path=manifest_path,
        monkeypatch=monkeypatch,
    )
    candidate_pointer = pointer_path.read_bytes()
    recovery_path = native_root / activation_tool._TERMINAL_PROMOTION_RECOVERY_FILENAME
    recovery = json.loads(recovery_path.read_text(encoding="utf-8"))
    if tamper == "candidate_hash":
        recovery["candidate_active_pointer_sha256"] = "0" * 64
        expected_error = "CANDIDATE_POINTER_SHA256_MISMATCH"
    else:
        recovery["promoted_manifest_relative_path"] = (
            f"activations\\promoted\\{activation_id}.json"
        )
        expected_error = "RECOVERY_VIEW_PATH_MISMATCH"
    recovery_path.write_bytes(activation_tool._canonical_json_bytes(recovery))

    with pytest.raises(ValueError, match=expected_error):
        activation_tool.promote_completed_terminal_activation(
            native_root,
            run_id=run_id,
            activation_id=activation_id,
            expected_manifest_sha256=sha256_file(manifest_path),
        )

    assert pointer_path.read_bytes() == candidate_pointer
    assert recovery_path.is_file()


def test_terminal_promotion_recovery_collision_blocks_new_transaction(
    tmp_path, monkeypatch
):
    native_root = tmp_path / "data" / "native"
    run_id, activation_id, pointer_path, manifest_path = _terminal_promotion_fixture(
        native_root
    )
    original_pointer = pointer_path.read_bytes()
    _stub_terminal_promotion_dependencies(monkeypatch)
    recovery_path = native_root / activation_tool._TERMINAL_PROMOTION_RECOVERY_FILENAME
    recovery_path.write_bytes(activation_tool._canonical_json_bytes({"collision": True}))

    with pytest.raises(ValueError, match="TERMINAL_PROMOTION_RECOVERY_FIELDS_INVALID"):
        activation_tool.promote_completed_terminal_activation(
            native_root,
            run_id=run_id,
            activation_id=activation_id,
            expected_manifest_sha256=sha256_file(manifest_path),
        )

    assert pointer_path.read_bytes() == original_pointer
    assert recovery_path.is_file()


def test_terminal_promotion_missing_recovery_during_postcheck_is_catastrophic(
    tmp_path, monkeypatch
):
    native_root = tmp_path / "data" / "native"
    run_id, activation_id, pointer_path, manifest_path = _terminal_promotion_fixture(
        native_root
    )
    _stub_terminal_promotion_dependencies(monkeypatch)
    _leave_candidate_with_recovery(
        native_root,
        run_id=run_id,
        activation_id=activation_id,
        manifest_path=manifest_path,
        monkeypatch=monkeypatch,
    )
    candidate_pointer = pointer_path.read_bytes()
    recovery_path = native_root / activation_tool._TERMINAL_PROMOTION_RECOVERY_FILENAME

    def _remove_recovery_during_postcheck(*args, **kwargs):
        recovery_path.unlink()
        return {
            "anchor_plan_receipt_sha256": "a" * 64,
            "portfolio_activation_roster_sha256": "b" * 64,
        }

    monkeypatch.setattr(
        activation_tool,
        "_terminal_promotion_postcheck",
        _remove_recovery_during_postcheck,
    )
    with pytest.raises(RuntimeError, match="TERMINAL_PROMOTION_ROLLBACK_FAILED"):
        activation_tool.promote_completed_terminal_activation(
            native_root,
            run_id=run_id,
            activation_id=activation_id,
            expected_manifest_sha256=sha256_file(manifest_path),
        )

    assert pointer_path.read_bytes() == candidate_pointer
    assert not recovery_path.exists()


@pytest.mark.parametrize(
    "reason",
    [
        "TARGET_AUDIT_PREFIX_PENDING:optimizer_train",
        "REFERENCE_AUDIT_PREFIX_PENDING:optimizer_train",
        "AUDIT_REFERENCE_TERMINAL_TECHNICAL:H1",
        "AUDIT_REFERENCE_RESOLUTION_EXCLUDED:M30",
        "PARTITION_OVERLAP_TOO_SMALL:optimizer_train:0<20",
        "FINE_REFERENCE_EMPTY:optimizer_train",
    ],
)
def test_terminal_pending_classifier_accepts_only_completed_authenticated_facts(
    reason,
):
    windows = {
        window: {
            "edge_states": {
                "H1": {
                    "integrity_state": "PASS",
                    "evidence_state": "PENDING",
                }
            }
        }
        for window in activation_tool.REQUIRED_AUDIT_WINDOWS
    }
    edges = {
        "H1": {
            "report": {
                "windows": {
                    window: {"reason_codes": [reason]}
                    for window in activation_tool.REQUIRED_AUDIT_WINDOWS
                }
            }
        }
    }
    envelope = {
        "diagnostics": {
            "all_references_evaluated": True,
            "raw_pair_reports_recomputed_from_pinned_bars": True,
        },
        "windows": windows,
        "edges": edges,
    }
    assert activation_tool._has_only_authenticated_terminal_pending(envelope)
    for window in activation_tool.REQUIRED_AUDIT_WINDOWS:
        edges["H1"]["report"]["windows"][window]["reason_codes"] = [
            "SYNTHETIC_PENDING"
        ]
    assert not activation_tool._has_only_authenticated_terminal_pending(envelope)


def _rewrite_terminal_technical(
    native_root: Path,
    run_id: str,
    timeframe: str,
) -> None:
    """Replace one valid fixture leaf with exact two-attempt terminal evidence."""

    manifest_path = native_root / "runs" / run_id / "run_manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    leaf = native_root / "runs" / run_id / SYMBOL / timeframe
    provenance_path = leaf / "provenance.json"
    source = json.loads(provenance_path.read_text(encoding="utf-8"))
    started = "2025-04-12T00:00:00Z"
    completed = "2025-04-12T00:00:03Z"
    manifest["acquisition_started_utc"] = started
    manifest["acquisition_completed_utc"] = completed
    attempt_fields = (
        request_contract._FAILED_PROVENANCE_BASE_FIELDS
        | request_contract._FAILED_PROVENANCE_FETCH_FIELDS
    )
    attempt = {key: deepcopy(source[key]) for key in attempt_fields if key in source}
    attempt.update(
        {
            "status": "partial",
            "failure_status": "technical_history_gap",
            "eligible_for_optimization": False,
            "eligible_for_downstream_validation": False,
            "artifact": None,
            "sha256": None,
            "rows": 0,
            "start_ts": None,
            "end_ts": None,
            "complete": False,
            "acquisition_started_utc": started,
            "acquisition_completed_utc": "2025-04-12T00:00:01Z",
            "error": {
                "type": "TechnicalHistoryGap",
                "message": "authenticated internal page gap",
            },
        }
    )
    attempt["request"]["last_fully_closed_bar_open_utc"] = None
    pages = attempt["pagination"]["page_attempts"]
    empty_page = deepcopy(pages[len(pages) // 2])
    empty_page["copy_rates_range_attempts"] = 2
    empty_page["connector_diagnostics"] = (
        request_contract._fixture_mt5_v4_empty_diagnostics(
            symbol=SYMBOL,
            timeframe=timeframe,
            start=pd.Timestamp(empty_page["start_utc"]).to_pydatetime(),
            end=pd.Timestamp(empty_page["end_exclusive_utc"]).to_pydatetime(),
        )
    )
    for index, page in enumerate(pages):
        if page["request_sequence"] == empty_page["request_sequence"]:
            pages[index] = deepcopy(empty_page)
            break
    attempt["pagination"]["empty_pages"] = [deepcopy(empty_page)]
    attempt["pagination"]["pages_with_data"] -= 1
    attempt["pagination"]["copy_rates_range_attempts_total"] += 1
    attempt["output_storage_resource_snapshot"].update(
        {"estimated_cell_bytes": 0, "sample_rows": 0}
    )

    descriptors = []
    for index, token in enumerate(("1" * 32, "2" * 32), start=1):
        record = deepcopy(attempt)
        record["acquisition_started_utc"] = f"2025-04-12T00:00:0{(index - 1) * 2}Z"
        record["acquisition_completed_utc"] = (
            f"2025-04-12T00:00:0{(index - 1) * 2 + 1}Z"
        )
        record["error"]["message"] += f":attempt-{index}"
        full_bytes = json.dumps(
            record,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
        ).encode("utf-8")
        envelope = {
            "schema_version": (
                request_contract.TERMINAL_TECHNICAL_ATTEMPT_RECEIPT_SCHEMA_VERSION
            ),
            "immutable_record": True,
            "nonfinal": True,
            "retryable": True,
            "attempt_id": token,
            "run_id": run_id,
            "symbol": SYMBOL,
            "timeframe": timeframe,
            "retention_status": "full",
            "full_record_sha256": hashlib.sha256(full_bytes).hexdigest(),
            "full_record_bytes": len(full_bytes),
            "record": record,
        }
        receipt_bytes = (
            json.dumps(
                envelope,
                sort_keys=True,
                separators=(",", ":"),
                ensure_ascii=False,
            ).encode("utf-8")
            + b"\n"
        )
        descriptors.append(
            {
                "relative_path": (
                    f"_attempts/{SYMBOL}/{timeframe}/attempt-{token}.json"
                ),
                "sha256": hashlib.sha256(receipt_bytes).hexdigest(),
                "bytes": len(receipt_bytes),
                "envelope": envelope,
            }
        )
    descriptors.sort(key=lambda value: (value["sha256"], value["relative_path"]))
    evidence = {
        "schema_version": request_contract.TERMINAL_TECHNICAL_EVIDENCE_SCHEMA_VERSION,
        "attempt_cap": request_contract.TERMINAL_TECHNICAL_ATTEMPT_CAP,
        "accepted_failure_status": "technical_history_gap",
        "receipts": descriptors,
    }
    latest = max(
        descriptors,
        key=lambda value: (
            pd.Timestamp(value["envelope"]["record"]["acquisition_completed_utc"]),
            value["sha256"],
        ),
    )["envelope"]["record"]
    terminal_provenance = {
        **deepcopy(latest),
        "status": "failed",
        "terminal_technical_evidence": evidence,
    }
    provenance_path.write_text(
        json.dumps(terminal_provenance, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    (leaf / "bars.csv").unlink()
    evidence_bytes = request_contract.canonical_json_bytes(evidence)
    result = next(
        item
        for item in manifest["results"]
        if item["symbol"] == SYMBOL and item["timeframe"] == timeframe
    )
    result.update(
        {
            "resume_disposition": "terminal_technical",
            "status": "failed",
            "failure_status": "technical_history_gap",
            "rows": 0,
            "sha256": None,
            "eligible_for_optimization": False,
            "eligible_for_downstream_validation": False,
            "terminal_evidence_sha256": hashlib.sha256(evidence_bytes).hexdigest(),
            "terminal_evidence_bytes": len(evidence_bytes),
        }
    )
    manifest["counts"] = {
        "success": sum(item["status"] == "success" for item in manifest["results"]),
        "partial": sum(item["status"] == "partial" for item in manifest["results"]),
        "failed": sum(item["status"] == "failed" for item in manifest["results"]),
        "total": len(manifest["results"]),
    }
    manifest_path.write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def _activate(native_root: Path, run_id: str, activation_id: str):
    """Exercise the production-like exact-roster pointer boundary."""

    return _activate_v5_fixture(
        native_root,
        run_id,
        activation_id,
        production_only_pointer=True,
    )


def test_atomic_byte_restore_retries_transient_windows_share_lock(
    tmp_path,
    monkeypatch,
):
    path = tmp_path / "ACTIVE.json"
    path.write_bytes(b"old")
    original_replace = activation_tool._atomic_replace_bytes
    attempts = 0

    def _transient_replace(target, payload):
        nonlocal attempts
        attempts += 1
        if attempts < 3:
            raise PermissionError(13, "simulated Windows sharing violation")
        return original_replace(target, payload)

    monkeypatch.setattr(activation_tool, "_atomic_replace_bytes", _transient_replace)
    activation_tool._atomic_replace_bytes_with_retry(
        path,
        b"last-good",
        initial_delay_seconds=0.0,
    )

    assert attempts == 3
    assert path.read_bytes() == b"last-good"


def test_atomic_audit_leaf_publish_retries_transient_windows_share_lock(tmp_path):
    staging = tmp_path / ".H4.transient.staging"
    final_dir = tmp_path / "H4"
    staging.mkdir()
    (staging / "parity.json").write_text("{}\n", encoding="utf-8")
    attempts = []
    delays = []

    def transient_replace(source, target):
        attempts.append((Path(source), Path(target)))
        if len(attempts) < 3:
            raise PermissionError(13, "simulated Windows sharing violation")
        os.replace(source, target)

    activation_tool._replace_directory_with_retry(
        staging,
        final_dir,
        replace_fn=transient_replace,
        sleep_fn=delays.append,
    )

    assert len(attempts) == 3
    assert delays == [0.025, 0.05]
    assert not staging.exists()
    assert (final_dir / "parity.json").read_text(encoding="utf-8") == "{}\n"


def test_atomic_audit_leaf_publish_never_retries_or_replaces_collision(tmp_path):
    staging = tmp_path / ".H4.collision.staging"
    final_dir = tmp_path / "H4"
    staging.mkdir()
    attempts = []

    def colliding_replace(source, target):
        attempts.append((Path(source), Path(target)))
        final_dir.mkdir()
        (final_dir / "owner.txt").write_text("other writer", encoding="utf-8")
        raise PermissionError(13, "simulated destination collision")

    with pytest.raises(FileExistsError, match="immutable audit leaf"):
        activation_tool._replace_directory_with_retry(
            staging,
            final_dir,
            replace_fn=colliding_replace,
            sleep_fn=lambda _delay: pytest.fail("collision must not be retried"),
        )

    assert len(attempts) == 1
    assert staging.is_dir()
    assert (final_dir / "owner.txt").read_text(encoding="utf-8") == "other writer"


def test_atomic_audit_leaf_retry_exhaustion_cleans_private_staging(
    tmp_path, monkeypatch
):
    audit_root = tmp_path / "audits" / "activation"
    audit_root.mkdir(parents=True)
    monkeypatch.setattr(
        activation_tool.os,
        "replace",
        lambda _source, _target: (_ for _ in ()).throw(
            PermissionError(13, "persistent simulated sharing violation")
        ),
    )
    monkeypatch.setattr(activation_tool.time, "sleep", lambda _delay: None)

    with pytest.raises(PermissionError, match="persistent simulated"):
        activation_tool._write_audit_leaf(
            audit_root,
            "USDCAD",
            "H4",
            {"status": "PENDING"},
            {"complete": False},
        )

    symbol_dir = audit_root / "USDCAD"
    assert not (symbol_dir / "H4").exists()
    assert list(symbol_dir.glob(".H4.*.staging")) == []


def test_v5_resolution_uses_scored_partitions_without_weakening_them():
    strategy_start = pd.Timestamp("2015-01-01T00:00:00Z")
    strategy_end = pd.Timestamp("2015-03-01T00:00:00Z")
    prefix_index = pd.date_range(
        "2014-01-01T00:00:00Z",
        periods=60,
        freq="D",
    )
    evidence_index = pd.date_range(
        strategy_start,
        strategy_end,
        freq="h",
        inclusive="left",
    )

    def _frame(index):
        values = pd.Series(range(len(index)), index=index, dtype=float)
        return pd.DataFrame(
            {
                "Open": values,
                "High": values + 0.2,
                "Low": values - 0.2,
                "Close": values + 0.1,
            },
            index=index,
        )

    source = {
        "provenance": {
            "request": {
                "cell_calendar_boundaries": {
                    "optimizer_train": {
                        "start_ts": strategy_start.isoformat(),
                        "end_ts": "2015-01-20T00:00:00+00:00",
                    },
                    "tuning_validation": {
                        "start_ts": "2015-01-20T00:00:00+00:00",
                        "end_ts": "2015-02-10T00:00:00+00:00",
                    },
                    "final_lockbox": {
                        "start_ts": "2015-02-10T00:00:00+00:00",
                        "end_ts": strategy_end.isoformat(),
                    },
                }
            }
        }
    }
    combined = _frame(prefix_index.append(evidence_index))
    full_span = activation_tool.audit_native_resolution(
        combined,
        symbol=SYMBOL,
        timeframe="H1",
    )
    assert full_span.status == "FAIL"
    assert any("DAILY_LIKE_YEARS:2014" in code for code in full_span.reason_codes)

    evidence, daily, error = activation_tool._v5_scored_resolution_frames(
        source,
        combined,
        None,
    )
    assert error is None
    assert daily is None
    assert evidence is not None
    assert evidence.index.min() == strategy_start
    assert evidence.index.max() < strategy_end
    scoped = activation_tool.audit_native_resolution(
        evidence,
        symbol=SYMBOL,
        timeframe="H1",
    )
    assert scoped.status == "PASS"

    daily_like_evidence = _frame(
        pd.date_range(strategy_start, periods=40, freq="D")
    )
    evidence, _, error = activation_tool._v5_scored_resolution_frames(
        source,
        daily_like_evidence,
        None,
    )
    assert error is None
    still_blocked = activation_tool.audit_native_resolution(
        evidence,
        symbol=SYMBOL,
        timeframe="H1",
    )
    assert still_blocked.status == "FAIL"
    assert any("DAILY_LIKE_YEARS:2015" in code for code in still_blocked.reason_codes)


def test_v5_authenticated_resolution_failure_is_terminal_not_global_veto(
    tmp_path,
    monkeypatch,
):
    data_dir = tmp_path / "data"
    native_root = data_dir / "native"
    native_root.mkdir(parents=True)
    _write_v5_run(native_root, "run-resolution-exclusion")
    original = activation_tool.audit_native_resolution
    original_pair = activation_tool.audit_native_cross_resolution

    def _one_scored_resolution_failure(*args, **kwargs):
        result = original(*args, **kwargs)
        if kwargs.get("timeframe") != "H4":
            return result
        return replace(
            result,
            status="FAIL",
            reason_codes=("INTRADAY_RESOLUTION_DAILY_LIKE_YEARS:2018",),
        )

    monkeypatch.setattr(
        activation_tool,
        "audit_native_resolution",
        _one_scored_resolution_failure,
    )

    def _excluded_reference_must_not_be_consumed(*args, **kwargs):
        target = kwargs.get("activation_target_timeframe")
        pair = {kwargs.get("fine_timeframe"), kwargs.get("coarse_timeframe")}
        if target != "H4" and "H4" in pair:
            raise AssertionError("excluded H4 was reused as consensus evidence")
        return original_pair(*args, **kwargs)

    monkeypatch.setattr(
        activation_tool,
        "audit_native_cross_resolution",
        _excluded_reference_must_not_be_consumed,
    )
    activated = _activate(
        native_root,
        "run-resolution-exclusion",
        "activation-resolution-exclusion",
    )

    assert activated["status"] == "active", activated
    assert activated["counts"][NATIVE_EVIDENCE_CANDIDATE] == 5
    assert activated["counts"][NATIVE_TECHNICAL_ERROR] == 1
    assert activated["counts"]["terminal"] == 6
    excluded = activated["entries"][SYMBOL]["H4"]
    assert excluded["status"] == NATIVE_TECHNICAL_ERROR
    assert excluded["audit_status"] == "FAIL"
    assert excluded["reason_codes"] == [
        "RESOLUTION:INTRADAY_RESOLUTION_DAILY_LIKE_YEARS:2018"
    ]
    assert not activated["active_pointer_updated"]


def test_v5_authenticated_terminal_prefix_shortfall_matches_released_contract():
    from tests.test_native_request_contract import _terminal_prefix_tail_fixture

    fixture = _terminal_prefix_tail_fixture(authenticated_inside_strategy=True)
    provenance = fixture["provenance"]
    # The frozen production run predates the optional derived boundary field,
    # but its failed/boundary-shortfall evidence remains fully validated by
    # the V5 page/artifact/source contract.
    provenance["pagination"].pop("available_history_boundary")

    boundary_errors = activation_tool._pagination_contract_errors(
        provenance,
        timeframe="H1",
        manifest_request=fixture["common_request"],
        manifest_source_environment=fixture["source_environment"],
        availability_class="success",
        failure_status="boundary_history_shortfall",
    )
    assert "SOURCE_TERMINAL_PREFIX_INTERSECTS_STRATEGY" not in boundary_errors

    nonboundary_errors = activation_tool._pagination_contract_errors(
        provenance,
        timeframe="H1",
        manifest_request=fixture["common_request"],
        manifest_source_environment=fixture["source_environment"],
        availability_class="success",
        failure_status="",
    )
    assert "SOURCE_TERMINAL_PREFIX_INTERSECTS_STRATEGY" in nonboundary_errors


def test_v5_all_twelve_edges_survive_sorted_json_and_production_recomputation(tmp_path):
    data_dir = tmp_path / "data"
    native_root = data_dir / "native"
    native_root.mkdir(parents=True)
    _write_v5_run(native_root, "run-clean")

    # The compact fixture is sparse but carries authenticated rows at every
    # requested page boundary. Acquisition may admit it only to downstream
    # validation; horizon/ESS/trade-power still decide statistical sufficiency.
    run_manifest = json.loads(
        (native_root / "runs" / "run-clean" / "run_manifest.json").read_text(
            encoding="utf-8"
        )
    )
    for result in run_manifest["results"]:
        assert result["status"] == "success"
        assert result["failure_status"] is None
        assert result["rows"] > 0
        assert result["eligible_for_optimization"] is False
        assert result["eligible_for_downstream_validation"] is True

    activated = _activate(native_root, "run-clean", "activation-clean")

    assert activated["status"] == "active", activated
    assert activated["counts"][NATIVE_EVIDENCE_CANDIDATE] == 6
    assert activated["counts"][NATIVE_SOURCE_UNAVAILABLE] == 0
    for timeframe in DEFAULT_TIMEFRAMES:
        entry = activated["entries"][SYMBOL][timeframe]
        assert entry["status"] == NATIVE_EVIDENCE_CANDIDATE
        assert entry["tradeable"] is False
        assert entry["statistical_sufficiency_receipt_required"] is True
        report = json.loads(
            (native_root / entry["parity_relative_path"]).read_text(encoding="utf-8")
        )
        assert (
            report["reference_timeframes"]
            == cross_resolution_reference_map(DEFAULT_TIMEFRAMES)[timeframe]
        )
        assert set(report["edges"]) == set(report["reference_timeframes"])
        assert report["windows"]["optimizer_train"]["status"] == "PASS"
        assert report["windows"]["strategy_full_context"]["status"] == "PASS"
        assert report["windows"]["final_lockbox"]["status"] == "PASS"
        assert report["windows"]["tuning_validation"]["status"] in {
            "PASS",
            "PENDING",
        }
    assert activated["audit_only"] is True
    assert activated["production_authoritative"] is False
    assert not activated["active_pointer_updated"]
    assert not (native_root / "ACTIVE.json").exists()


def test_v5_production_grid_accounts_for_all_600_cells(tmp_path):
    data_dir = tmp_path / "data"
    native_root = data_dir / "native"
    native_root.mkdir(parents=True)
    _write_v5_run(native_root, "run-production-grid", production_grid=True)

    activated = _activate(
        native_root,
        "run-production-grid",
        "activation-production-grid",
    )

    assert activated["status"] == "active", activated
    assert (
        tuple(activated["universe_eligibility"]["requested_symbols"])
        == PRODUCTION_SYMBOL_ORDER
    )
    assert len(activated["entries"]) == 100
    assert all(len(grid) == 6 for grid in activated["entries"].values())
    assert activated["counts"][NATIVE_EVIDENCE_CANDIDATE] == 600
    assert activated["counts"][NATIVE_SOURCE_UNAVAILABLE] == 0
    assert activated["counts"]["total"] == 600
    assert activated["counts"]["terminal"] == 600
    assert activated["production_authoritative"] is True
    assert activated["minimum_session_coverage"] is None
    assert activated["coverage_activation_threshold_removed"] is True
    authenticity = activated["universe_eligibility"]
    assert authenticity["coverage_is_diagnostic_only"] is True
    assert authenticity["fixed_coverage_activation_threshold"] is None
    assert "minimum_session_coverage" not in authenticity
    assert len(authenticity["symbols_with_evidence_candidates"]) == 100
    assert all(
        entry["status"] == NATIVE_EVIDENCE_CANDIDATE
        and entry["bars_relative_path"]
        and entry["bars_sha256"]
        and entry["sidecar_relative_path"]
        and entry["sidecar_sha256"]
        and entry["parity_relative_path"]
        and entry["parity_sha256"]
        for grid in activated["entries"].values()
        for entry in grid.values()
    )
    for entry in activated["entries"][SYMBOL].values():
        assert entry["evidence_metadata"]["coverage_is_diagnostic_only"] is True
        report = json.loads(
            (native_root / entry["parity_relative_path"]).read_text(encoding="utf-8")
        )
        assert "minimum_session_coverage" not in report

    pointer_path = native_root / "ACTIVE.json"
    original_pointer = pointer_path.read_bytes()
    assert json.loads(original_pointer)["production_authoritative"] is True
    universe = resolve_active_native_universe(
        data_dir,
        PRODUCTION_SYMBOL_ORDER,
        verify_exclusion_evidence=False,
        production_contract_required=True,
    )
    assert universe.valid, universe.errors
    assert universe.requested_symbols == PRODUCTION_SYMBOL_ORDER
    assert tuple(activated["entries"]) == PRODUCTION_SYMBOL_ORDER
    for broker_symbol in ("Platinum", "Palladium"):
        assert broker_symbol in activated["entries"]
        assert broker_symbol.upper() not in activated["entries"]
        for timeframe in DEFAULT_TIMEFRAMES:
            entry = activated["entries"][broker_symbol][timeframe]
            assert f"/{broker_symbol}/{timeframe}/" in (
                "/" + entry["bars_relative_path"]
            )

    wrong_case_config = tuple(
        "PLATINUM" if symbol == "Platinum" else symbol
        for symbol in PRODUCTION_SYMBOL_ORDER
    )
    wrong_config = resolve_active_native_universe(
        data_dir,
        wrong_case_config,
        verify_exclusion_evidence=False,
        production_contract_required=True,
    )
    assert not wrong_config.valid
    assert "NATIVE_UNIVERSE_CONFIG_SYMBOL_ORDER_IDENTITY_MISMATCH" in (
        wrong_config.errors
    )

    pointer = json.loads(original_pointer)
    pointer["manifest_relative_path"] = pointer["manifest_relative_path"].replace(
        "activations/", "Activations/", 1
    )
    pointer_path.write_text(
        json.dumps(pointer, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    wrong_pointer = resolve_active_native_universe(
        data_dir,
        PRODUCTION_SYMBOL_ORDER,
        verify_exclusion_evidence=False,
        production_contract_required=True,
    )
    assert not wrong_pointer.valid
    assert "NATIVE_ACTIVE_MANIFEST_PATH_IDENTITY_MISMATCH" in wrong_pointer.errors
    pointer_path.write_bytes(original_pointer)

    pointer = json.loads(original_pointer)
    pointer["production_authoritative"] = False
    pointer_path.write_text(
        json.dumps(pointer, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    forged_authority = resolve_active_native_universe(
        data_dir,
        PRODUCTION_SYMBOL_ORDER,
        verify_exclusion_evidence=False,
        production_contract_required=True,
    )
    assert not forged_authority.valid
    assert "NATIVE_ACTIVE_PRODUCTION_AUTHORITY_MARKER_MISMATCH" in (
        forged_authority.errors
    )
    pointer_path.write_bytes(original_pointer)

    manifest_path = Path(activated["manifest_path"])
    original_manifest = manifest_path.read_bytes()
    manifest = json.loads(original_manifest)
    eurusd_m5 = manifest["entries"][SYMBOL]["M5"]
    eurusd_m5["symbol"] = "eurusd"
    eurusd_m5["bars_relative_path"] = eurusd_m5["bars_relative_path"].replace(
        "/EURUSD/", "/eurusd/"
    )
    manifest_path.write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    pointer = json.loads(original_pointer)
    pointer["manifest_sha256"] = sha256_file(manifest_path)
    pointer_path.write_text(
        json.dumps(pointer, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    wrong_path = resolve_active_native_artifact(
        data_dir,
        SYMBOL,
        "M5",
        production_contract_required=True,
    )
    assert not wrong_path.valid
    assert "NATIVE_ACTIVE_CELL_SYMBOL_MISMATCH" in wrong_path.errors
    assert any("BARS_RELATIVE_PATH_IDENTITY_MISMATCH" in e for e in wrong_path.errors)
    manifest_path.write_bytes(original_manifest)
    pointer_path.write_bytes(original_pointer)

    run_manifest_path = (
        native_root / "runs" / "run-production-grid" / "run_manifest.json"
    )
    original_run_manifest = run_manifest_path.read_bytes()
    run_manifest = json.loads(original_run_manifest)
    run_manifest["request"]["timeframe_request_map_schema_version"] = (
        "fxpm.native_history.timeframe_request_map.v4"
    )
    run_manifest_path.write_text(
        json.dumps(run_manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    manifest = json.loads(original_manifest)
    manifest["run_manifest_sha256"] = sha256_file(run_manifest_path)
    manifest_path.write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    pointer = json.loads(original_pointer)
    pointer["manifest_sha256"] = sha256_file(manifest_path)
    pointer_path.write_text(
        json.dumps(pointer, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    downgraded = resolve_active_native_universe(
        data_dir,
        wrong_case_config,
        verify_exclusion_evidence=False,
        production_contract_required=False,
    )
    assert not downgraded.valid
    assert "NATIVE_AUTHENTICITY_V5_REQUEST_IDENTITY_REQUIRED" in downgraded.errors
    run_manifest_path.write_bytes(original_run_manifest)
    manifest_path.write_bytes(original_manifest)
    pointer_path.write_bytes(original_pointer)


def test_terminal_technical_classification_requires_exact_frozen_tuple():
    assert classify_native_availability_cell(
        "failed",
        "technical_history_gap",
        0,
        "terminal_technical",
    ) == ("terminal_technical", None)
    classification, reason = classify_native_availability_cell(
        "failed",
        "technical_history_gap",
        0,
    )
    assert classification == "invalid"
    assert reason == "NON_AVAILABILITY_FAILURE:technical_history_gap"
    assert classify_native_availability_cell(
        "partial",
        "technical_history_gap",
        0,
        "terminal_technical",
    ) == ("invalid", "TERMINAL_TECHNICAL_STATUS_MISMATCH")


def test_terminal_technical_only_symbol_remains_technical_not_no_trade():
    cells = {
        (SYMBOL, timeframe): {
            "status": "failed",
            "failure_status": "technical_history_gap",
            "rows": 0,
            "resume_disposition": "terminal_technical",
        }
        for timeframe in DEFAULT_TIMEFRAMES
    }
    decision = activation_tool._symbol_availability_decision(
        SYMBOL,
        DEFAULT_TIMEFRAMES,
        cells,
        (),
    )
    assert decision["status"] == NATIVE_TECHNICAL_ERROR
    assert all("TERMINAL_TECHNICAL" in reason for reason in decision["reason_codes"])


def test_terminal_technical_only_symbol_artifact_error_remains_cell_typed(tmp_path):
    data_dir = tmp_path / "data"
    native_root = data_dir / "native"
    native_root.mkdir(parents=True)
    run_id = "run-terminal-technical-only"
    _write_v5_run(native_root, run_id)
    for timeframe in DEFAULT_TIMEFRAMES:
        _rewrite_terminal_technical(native_root, run_id, timeframe)

    activated = _activate_v5_fixture(
        native_root,
        run_id,
        "activation-terminal-technical-only",
        production_only_pointer=True,
    )
    assert activated["status"] == "active", activated
    (native_root / "ACTIVE.json").write_text(
        json.dumps(
            {
                "schema_version": ACTIVE_POINTER_SCHEMA,
                "activation_id": activated["activation_id"],
                "manifest_relative_path": (
                    f"activations/{activated['activation_id']}.json"
                ),
                "manifest_sha256": activated["manifest_sha256"],
                "activated_at_utc": activated["created_at_utc"],
                "production_authoritative": False,
            },
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )

    terminal = resolve_active_native_artifact(data_dir, SYMBOL, "M15")
    assert not terminal.valid
    assert terminal.errors == ["NATIVE_ACTIVE_CELL_TERMINAL_TECHNICAL"]


def test_authenticated_terminal_technical_cell_preserves_valid_siblings(
    tmp_path,
):
    data_dir = tmp_path / "data"
    native_root = data_dir / "native"
    native_root.mkdir(parents=True)
    run_id = "run-terminal-technical"
    _write_v5_run(native_root, run_id)
    _rewrite_terminal_technical(native_root, run_id, "M15")

    activated = _activate_v5_fixture(
        native_root,
        run_id,
        "activation-terminal-technical",
        production_only_pointer=True,
    )

    assert activated["status"] == "active", activated
    assert activated["entries"][SYMBOL]["M15"]["status"] == NATIVE_TECHNICAL_ERROR
    assert activated["entries"][SYMBOL]["M15"]["audit_status"] == "PASS"
    assert activated["entries"][SYMBOL]["M15"]["tradeable"] is False
    assert (
        activated["entries"][SYMBOL]["M15"]["statistical_sufficiency_receipt_required"]
        is True
    )
    assert all(
        activated["entries"][SYMBOL][timeframe]["status"] == NATIVE_EVIDENCE_CANDIDATE
        for timeframe in DEFAULT_TIMEFRAMES
        if timeframe != "M15"
    )
    authenticity = activated["universe_eligibility"]
    assert authenticity["terminal_states"] == [
        NATIVE_EVIDENCE_CANDIDATE,
        NATIVE_SOURCE_UNAVAILABLE,
        NATIVE_TECHNICAL_ERROR,
    ]
    assert authenticity["nonterminal_states"] == ["pending"]
    assert authenticity["eligible_symbols"] == [SYMBOL]
    assert "M15" not in authenticity["evidence_candidate_timeframes_by_symbol"][SYMBOL]
    assert "M15" not in authenticity["source_unavailable_timeframes_by_symbol"][SYMBOL]

    (native_root / "ACTIVE.json").write_text(
        json.dumps(
            {
                "schema_version": ACTIVE_POINTER_SCHEMA,
                "activation_id": activated["activation_id"],
                "manifest_relative_path": (
                    f"activations/{activated['activation_id']}.json"
                ),
                "manifest_sha256": activated["manifest_sha256"],
                "activated_at_utc": activated["created_at_utc"],
                "production_authoritative": False,
            },
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )

    universe = resolve_active_native_universe(
        data_dir,
        [SYMBOL],
        verify_exclusion_evidence=True,
    )
    assert universe.valid, universe.errors
    terminal = resolve_active_native_artifact(data_dir, SYMBOL, "M15")
    assert not terminal.valid
    assert terminal.errors == ["NATIVE_ACTIVE_CELL_TERMINAL_TECHNICAL"]
    sibling = resolve_active_native_artifact(data_dir, SYMBOL, "H1")
    assert sibling.valid, sibling.errors

    provenance_path = native_root / "runs" / run_id / SYMBOL / "M15" / "provenance.json"
    tampered = json.loads(provenance_path.read_text(encoding="utf-8"))
    tampered["terminal_technical_evidence"]["attempt_cap"] = 3
    provenance_path.write_text(
        json.dumps(tampered, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    clear_unscoped_native_universe_memo()
    rejected = resolve_active_native_universe(
        data_dir,
        [SYMBOL],
        verify_exclusion_evidence=True,
    )
    assert not rejected.valid
    assert any(
        "PROVENANCE" in error or "TERMINAL_TECHNICAL" in error
        for error in rejected.errors
    )


def test_v5_wrong_case_request_result_provenance_and_leaf_path_fail_closed(tmp_path):
    native_root = tmp_path / "data" / "native"
    native_root.mkdir(parents=True)
    run_id = "run-wrong-case"
    _write_v5_run(native_root, run_id)
    manifest_path = native_root / "runs" / run_id / "run_manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["request"]["symbols"] = ["eurusd"]
    m5_result = next(item for item in manifest["results"] if item["timeframe"] == "M5")
    m5_result["symbol"] = "eurusd"
    m5_result["relative_leaf"] = "eurusd/m5"
    manifest_path.write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    provenance_path = native_root / "runs" / run_id / SYMBOL / "M5" / "provenance.json"
    provenance = json.loads(provenance_path.read_text(encoding="utf-8"))
    provenance["symbol"] = "eurusd"
    provenance["canonical_symbol"] = "eurusd"
    provenance_path.write_text(
        json.dumps(provenance, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )

    result = activation_tool.audit_and_activate(
        native_root,
        run_id=run_id,
        symbols=[SYMBOL],
        timeframes=DEFAULT_TIMEFRAMES,
        activation_id="activation-wrong-case",
        thresholds=activation_tool.ParityThresholds(min_compared_native_bars=20),
        update_active_pointer=False,
    )

    assert result["status"] == "failed"
    assert not result["active_pointer_updated"]
    reasons = [
        str(reason)
        for entry in result["entries"][SYMBOL].values()
        for reason in entry["reason_codes"]
    ]
    assert any("SYMBOL_ORDER_IDENTITY_MISMATCH" in reason for reason in reasons)
    assert any("CELL_PATH_IDENTITY_MISMATCH" in reason for reason in reasons)
    assert any("SOURCE_SYMBOL_MISMATCH" in reason for reason in reasons)
    assert any("SOURCE_CANONICAL_SYMBOL_MISMATCH" in reason for reason in reasons)


@pytest.mark.parametrize("target", DEFAULT_TIMEFRAMES)
def test_v5_single_target_corruption_isolates_cell_without_global_veto(
    tmp_path, target
):
    native_root = tmp_path / "data" / "native"
    native_root.mkdir(parents=True)
    _write_v5_run(native_root, f"run-corrupt-{target}", corrupt_timeframe=target)

    activated = _activate(
        native_root,
        f"run-corrupt-{target}",
        f"activation-corrupt-{target}",
    )

    assert activated["status"] == "active", activated
    assert not activated["active_pointer_updated"]
    statuses = {
        timeframe: entry["status"]
        for timeframe, entry in activated["entries"][SYMBOL].items()
    }
    assert statuses[target] == NATIVE_TECHNICAL_ERROR
    affected = {
        key for key, value in statuses.items() if value != NATIVE_EVIDENCE_CANDIDATE
    }
    graph = cross_resolution_reference_map(DEFAULT_TIMEFRAMES)
    possible_dependants = {
        timeframe for timeframe, references in graph.items() if target in references
    }
    assert target in affected
    assert affected <= ({target} | possible_dependants)
    if possible_dependants:
        assert any(
            activated["entries"][SYMBOL][timeframe]["consensus_disagreement_count"] > 0
            for timeframe in possible_dependants
        )
    else:
        # D1 is a target-only leaf in the released reference graph. Its own
        # pair checks fail, but no sibling can legitimately cite it.
        assert affected == {target}
    assert not (native_root / "ACTIVE.json").exists()


def test_v5_strategy_context_corruption_is_cell_local_and_cannot_hide(
    tmp_path,
):
    native_root = tmp_path / "data" / "native"
    native_root.mkdir(parents=True)
    _write_v5_run(
        native_root,
        "run-context",
        corrupt_timeframe="H1",
        context_only=True,
    )

    activated = _activate(native_root, "run-context", "activation-context")

    h1 = activated["entries"][SYMBOL]["H1"]
    report = json.loads(
        (native_root / h1["parity_relative_path"]).read_text(encoding="utf-8")
    )
    assert activated["status"] == "active"
    assert not activated["active_pointer_updated"]
    assert h1["status"] == NATIVE_TECHNICAL_ERROR
    assert report["windows"]["strategy_full_context"]["status"] == "FAIL"
    assert report["windows"]["optimizer_train"]["status"] == "PASS"
    assert all(
        report["windows"][partition]["status"] == "PASS"
        for partition in ("tuning_validation", "final_lockbox")
    )


def test_v5_one_unproven_empty_reference_is_technical_and_blocks_publish(tmp_path):
    data_dir = tmp_path / "data"
    native_root = data_dir / "native"
    native_root.mkdir(parents=True)
    _write_v5_run(native_root, "run-one-unavailable")
    _rewrite_no_history(native_root, "run-one-unavailable", "M15")

    activated = _activate(
        native_root,
        "run-one-unavailable",
        "activation-one-unavailable",
    )

    assert activated["status"] == "failed", activated
    assert activated["entries"][SYMBOL]["M15"]["status"] == NATIVE_TECHNICAL_ERROR
    assert activated["counts"][NATIVE_SOURCE_UNAVAILABLE] == 0
    assert activated["audit_only"] is True
    assert not (native_root / "ACTIVE.json").exists()


def test_v5_both_unproven_empty_references_are_integrity_error_and_block_publish(
    tmp_path,
):
    native_root = tmp_path / "data" / "native"
    native_root.mkdir(parents=True)
    _write_v5_run(native_root, "run-both-unavailable")
    _rewrite_no_history(native_root, "run-both-unavailable", "M15")
    _rewrite_no_history(native_root, "run-both-unavailable", "M30")

    activated = _activate(
        native_root,
        "run-both-unavailable",
        "activation-both-unavailable",
    )

    assert activated["status"] == "failed", activated
    assert not activated["active_pointer_updated"]
    assert activated["entries"][SYMBOL]["M5"]["status"] == NATIVE_TECHNICAL_ERROR
    report_path = (
        native_root / activated["entries"][SYMBOL]["M5"]["parity_relative_path"]
    )
    report = json.loads(report_path.read_text(encoding="utf-8"))
    assert report["status"] == "ERROR"
    assert all(detail["status"] == "ERROR" for detail in report["windows"].values())


def test_v5_unproven_all_empty_symbol_is_technical_not_no_trade(
    tmp_path,
):
    data_dir = tmp_path / "data"
    native_root = data_dir / "native"
    native_root.mkdir(parents=True)
    _write_v5_run(native_root, "run-all-unavailable")
    for timeframe in DEFAULT_TIMEFRAMES:
        _rewrite_no_history(
            native_root,
            "run-all-unavailable",
            timeframe,
        )

    activated = _activate(
        native_root,
        "run-all-unavailable",
        "activation-all-unavailable",
    )

    assert activated["status"] == "failed", activated
    assert activated["counts"][NATIVE_EVIDENCE_CANDIDATE] == 0
    assert activated["counts"][NATIVE_SOURCE_UNAVAILABLE] == 0
    assert activated["counts"][NATIVE_TECHNICAL_ERROR] == 6
    contract = activated["universe_eligibility"]
    assert contract["cell_statuses"][SYMBOL] == {
        timeframe: NATIVE_TECHNICAL_ERROR for timeframe in DEFAULT_TIMEFRAMES
    }
    assert contract["symbols_with_evidence_candidates"] == []
    assert not (native_root / "ACTIVE.json").exists()


@pytest.mark.parametrize("attack", ["valid_hex_hashes", "future_first_date"])
def test_v5_caller_asserted_absence_receipt_cannot_promote_terminal_empty(
    tmp_path,
    attack,
):
    native_root = tmp_path / "data" / "native"
    native_root.mkdir(parents=True)
    run_id = f"run-forged-absence-{attack}"
    _write_v5_run(native_root, run_id)
    _rewrite_no_history(native_root, run_id, "M15")
    provenance_path = native_root / "runs" / run_id / SYMBOL / "M15" / "provenance.json"
    provenance = json.loads(provenance_path.read_text(encoding="utf-8"))
    request = provenance["request"]
    requested_end = request["native_fetch_end_exclusive_utc"]
    proof = {
        "schema_version": "fxpm-native-source-unavailable-proof-v1",
        "symbol": SYMBOL,
        "timeframe": "M15",
        "attestation_kind": "broker_server_first_date_and_exhaustive_page_probe",
        "conclusion": "NO_SERVER_HISTORY_IN_REQUESTED_INTERVAL",
        "source_identity_sha256": "a" * 64,
        "probe_ledger_sha256": "b" * 64,
        "requested_start_utc": request["native_fetch_start_utc"],
        "requested_end_exclusive_utc": requested_end,
        "queried_at_utc": requested_end,
        "broker_server_first_open_utc": requested_end,
        "all_requested_pages_terminal_empty": True,
        "tradeable": False,
    }
    if attack == "valid_hex_hashes":
        proof["source_identity_sha256"] = "0" * 64
        proof["probe_ledger_sha256"] = "f" * 64
    else:
        proof["broker_server_first_open_utc"] = "2099-01-01T00:00:00Z"
    provenance["source_unavailable_proof"] = proof
    provenance_path.write_text(
        json.dumps(provenance, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )

    activated = _activate(
        native_root,
        run_id,
        f"activation-forged-absence-{attack}",
    )

    entry = activated["entries"][SYMBOL]["M15"]
    assert activated["status"] == "failed"
    assert not activated["active_pointer_updated"]
    assert entry["status"] == NATIVE_TECHNICAL_ERROR
    assert activated["counts"][NATIVE_SOURCE_UNAVAILABLE] == 0
    assert any(
        "NON_AVAILABILITY_FAILURE:terminal_history_empty_unresolved" in reason
        for reason in entry["reason_codes"]
    )


def test_v5_repinning_fabricated_consensus_does_not_defeat_raw_recomputation(tmp_path):
    data_dir = tmp_path / "data"
    native_root = data_dir / "native"
    native_root.mkdir(parents=True)
    _write_v5_run(native_root, "run-repin", production_grid=True)
    activated = _activate(native_root, "run-repin", "activation-repin")
    manifest_path = Path(activated["manifest_path"])
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    entry = manifest["entries"][SYMBOL]["D1"]
    consensus_path = native_root / entry["parity_relative_path"]
    consensus = json.loads(consensus_path.read_text(encoding="utf-8"))
    reference = consensus["reference_timeframes"][0]
    edge = consensus["edges"][reference]
    edge["report"]["windows"]["optimizer_train"]["pair_report_sha256"] = "0" * 64
    edge["report_sha256"] = canonical_sha256(edge["report"])
    consensus["consensus_sha256"] = canonical_sha256(
        {key: value for key, value in consensus.items() if key != "consensus_sha256"}
    )
    consensus_path.write_text(
        json.dumps(consensus, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    entry["parity_sha256"] = sha256_file(consensus_path)
    manifest_path.write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    pointer_path = native_root / "ACTIVE.json"
    pointer = json.loads(pointer_path.read_text(encoding="utf-8"))
    pointer["manifest_sha256"] = sha256_file(manifest_path)
    pointer_path.write_text(
        json.dumps(pointer, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )

    universe = resolve_active_native_universe(
        data_dir,
        PRODUCTION_SYMBOL_ORDER,
        verify_exclusion_evidence=False,
        production_contract_required=True,
    )

    assert not universe.valid
    assert any(
        "RAW_PAIR_RECOMPUTATION_MISMATCH" in error or "EDGE_PAIR_HASH_INVALID" in error
        for error in universe.errors
    )
    artifact = resolve_active_native_artifact(
        data_dir,
        SYMBOL,
        "D1",
        production_contract_required=True,
    )
    assert not artifact.valid
    assert any("RAW_PAIR_RECOMPUTATION_MISMATCH" in error for error in artifact.errors)


def test_v5_failed_audit_never_advances_existing_active_pointer(tmp_path):
    native_root = tmp_path / "data" / "native"
    native_root.mkdir(parents=True)
    _write_v5_run(native_root, "run-first", production_grid=True)
    first = _activate(native_root, "run-first", "activation-first")
    pointer_path = native_root / "ACTIVE.json"
    first_pointer = pointer_path.read_bytes()

    _write_v5_run(native_root, "run-bad-contract")
    run_path = native_root / "runs" / "run-bad-contract" / "run_manifest.json"
    run = json.loads(run_path.read_text(encoding="utf-8"))
    run["request"]["timeframe_consensus_policy"]["evaluate_all_references"] = False
    run_path.write_text(
        json.dumps(run, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    failed = _activate(native_root, "run-bad-contract", "activation-bad-contract")

    assert first["status"] == "active"
    assert failed["status"] == "failed"
    assert not failed["active_pointer_updated"]
    assert pointer_path.read_bytes() == first_pointer


def test_v5_missing_pinned_source_is_integrity_error_not_semantic_exclusion(tmp_path):
    native_root = tmp_path / "data" / "native"
    native_root.mkdir(parents=True)
    _write_v5_run(native_root, "run-missing-bars")
    (native_root / "runs" / "run-missing-bars" / SYMBOL / "H1" / "bars.csv").unlink()

    result = _activate(
        native_root,
        "run-missing-bars",
        "activation-missing-bars",
    )

    assert result["status"] == "failed"
    assert not result["active_pointer_updated"]
    assert result["entries"][SYMBOL]["H1"]["status"] == NATIVE_TECHNICAL_ERROR
    assert any(
        "SOURCE_BARS_MISSING" in reason
        for reason in result["entries"][SYMBOL]["H1"]["reason_codes"]
    )


def test_v5_fail_plus_pending_is_unresolved_technical_and_pointer_is_unchanged(
    tmp_path,
    monkeypatch,
):
    native_root = tmp_path / "data" / "native"
    native_root.mkdir(parents=True)
    _write_v5_run(native_root, "run-pending-base", production_grid=True)
    base = _activate(
        native_root,
        "run-pending-base",
        "activation-pending-base",
    )
    pointer_path = native_root / "ACTIVE.json"
    original_pointer = pointer_path.read_bytes()
    _write_v5_run(native_root, "run-pending-candidate")
    original_audit = activation_tool.audit_native_cross_resolution

    def _mixed_edge_states(*args, **kwargs):
        report = original_audit(*args, **kwargs)
        if kwargs.get("activation_target_timeframe") != "H1":
            return report
        report = json.loads(json.dumps(report))
        window = report["partitions"]["optimizer_train"]
        if kwargs.get("fine_timeframe") == "M30":
            window["status"] = "FAIL"
            window["reason_codes"] = ["SYNTHETIC_SEMANTIC_FAIL"]
            report["status"] = "FAIL"
        elif kwargs.get("coarse_timeframe") == "H4":
            window["status"] = "PENDING"
            window["reason_codes"] = ["SYNTHETIC_PENDING"]
            report["status"] = "PENDING"
        return report

    monkeypatch.setattr(
        activation_tool,
        "audit_native_cross_resolution",
        _mixed_edge_states,
    )
    candidate = _activate(
        native_root,
        "run-pending-candidate",
        "activation-pending-candidate",
    )

    assert base["status"] == "active"
    assert candidate["status"] == "failed"
    assert candidate["entries"][SYMBOL]["H1"]["status"] == NATIVE_TECHNICAL_ERROR
    parity = json.loads(
        (
            native_root
            / candidate["entries"][SYMBOL]["H1"]["parity_relative_path"]
        ).read_text(encoding="utf-8")
    )
    assert _v5_has_unopposed_semantic_failure(parity)
    assert (
        _v5_derive_recomputed_authenticity_state(
            parity,
            resolution_status=candidate["entries"][SYMBOL]["H1"][
                "resolution_status"
            ],
        )
        == NATIVE_TECHNICAL_ERROR
    )
    assert not candidate["active_pointer_updated"]
    assert pointer_path.read_bytes() == original_pointer


def test_v5_unknown_pure_pending_cannot_become_evidence_candidate(
    tmp_path,
    monkeypatch,
):
    native_root = tmp_path / "data" / "native"
    native_root.mkdir(parents=True)
    _write_v5_run(native_root, "run-unknown-pending")
    original_audit = activation_tool.audit_native_cross_resolution

    def _unknown_pending(*args, **kwargs):
        report = original_audit(*args, **kwargs)
        if kwargs.get("activation_target_timeframe") != "H1":
            return report
        report = json.loads(json.dumps(report))
        window = report["partitions"]["optimizer_train"]
        window["status"] = "PENDING"
        window["reason_codes"] = ["SYNTHETIC_PENDING"]
        report["status"] = "PENDING"
        return report

    monkeypatch.setattr(
        activation_tool,
        "audit_native_cross_resolution",
        _unknown_pending,
    )
    result = _activate(
        native_root,
        "run-unknown-pending",
        "activation-unknown-pending",
    )

    assert result["status"] == "pending"
    assert result["entries"][SYMBOL]["H1"]["status"] == "pending"
    assert result["entries"][SYMBOL]["H1"]["audit_status"] == "PENDING"
    assert not result["active_pointer_updated"]


def test_v5_authenticated_terminal_pending_remains_evidence_candidate(
    tmp_path,
    monkeypatch,
):
    native_root = tmp_path / "data" / "native"
    native_root.mkdir(parents=True)
    _write_v5_run(native_root, "run-authenticated-pending")
    original_audit = activation_tool.audit_native_cross_resolution

    def _authenticated_pending(*args, **kwargs):
        report = original_audit(*args, **kwargs)
        if kwargs.get("activation_target_timeframe") != "H1":
            return report
        report = json.loads(json.dumps(report))
        window = report["partitions"]["optimizer_train"]
        window["status"] = "PENDING"
        window["reason_codes"] = [
            "PARTITION_OVERLAP_TOO_SMALL:optimizer_train:19<20"
        ]
        report["status"] = "PENDING"
        return report

    monkeypatch.setattr(
        activation_tool,
        "audit_native_cross_resolution",
        _authenticated_pending,
    )
    result = _activate(
        native_root,
        "run-authenticated-pending",
        "activation-authenticated-pending",
    )

    assert result["status"] == "active", result
    assert result["entries"][SYMBOL]["H1"]["status"] == NATIVE_EVIDENCE_CANDIDATE
    assert result["entries"][SYMBOL]["H1"]["audit_status"] == "PASS"
    assert result["entries"][SYMBOL]["H1"]["consensus_status"] == "PENDING"
    parity = json.loads(
        (
            native_root
            / result["entries"][SYMBOL]["H1"]["parity_relative_path"]
        ).read_text(encoding="utf-8")
    )
    assert not _v5_has_unopposed_semantic_failure(parity)
    assert (
        _v5_derive_recomputed_authenticity_state(
            parity,
            resolution_status=result["entries"][SYMBOL]["H1"][
                "resolution_status"
            ],
        )
        == NATIVE_EVIDENCE_CANDIDATE
    )


def test_v5_runtime_derivation_keeps_semantic_failure_with_pass_witness_candidate():
    envelope = {
        "status": "PENDING",
        "windows": {
            window: {
                "edge_states": {
                    "H1": {
                        "integrity_state": "PASS",
                        "evidence_state": "SEMANTIC_FAIL",
                    },
                    "D1": {
                        "integrity_state": "PASS",
                        "evidence_state": "PASS",
                    },
                }
            }
            for window in activation_tool.REQUIRED_AUDIT_WINDOWS
        },
    }

    assert not _v5_has_unopposed_semantic_failure(envelope)
    assert (
        _v5_derive_recomputed_authenticity_state(
            envelope,
            resolution_status="PASS",
        )
        == NATIVE_EVIDENCE_CANDIDATE
    )


def test_terminal_promotion_candidate_reopens_compact_evidence_and_rejects_splices(
    tmp_path,
):
    native_root = tmp_path / "data" / "native"
    native_root.mkdir(parents=True)
    run_id = "run-promotion-candidate-evidence"
    activation_id = "activation-promotion-candidate-evidence"
    _write_v5_run(native_root, run_id)
    activated = _activate(native_root, run_id, activation_id)
    entry = activated["entries"][SYMBOL]["D1"]
    kwargs = {
        "native_root": native_root,
        "run_id": run_id,
        "activation_id": activation_id,
        "symbol": SYMBOL,
        "timeframe": "D1",
        "run_manifest_relative_path": activated["run_manifest_relative_path"],
        "run_manifest_sha256": activated["run_manifest_sha256"],
    }

    assert activation_tool._terminal_promotion_evidence_errors(entry, **kwargs) == []

    consensus_error = deepcopy(entry)
    consensus_error["consensus_status"] = "ERROR"
    assert any(
        "CANDIDATE_CONSENSUS_INVALID" in error
        for error in activation_tool._terminal_promotion_evidence_errors(
            consensus_error, **kwargs
        )
    )

    source_error = deepcopy(entry)
    source_error["reason_codes"] = ["SOURCE:SYNTHETIC_PROVENANCE_FAILURE"]
    assert any(
        "CANDIDATE_INTEGRITY_ERROR" in error
        for error in activation_tool._terminal_promotion_evidence_errors(
            source_error, **kwargs
        )
    )

    parity_hash_error = deepcopy(entry)
    parity_hash_error["parity_sha256"] = "0" * 64
    assert any(
        "PARITY_SHA256_MISMATCH" in error
        for error in activation_tool._terminal_promotion_evidence_errors(
            parity_hash_error, **kwargs
        )
    )

    pending_splice = deepcopy(entry)
    pending_splice["consensus_status"] = "PENDING"
    assert any(
        "PENDING_UNAUTHENTICATED" in error
        for error in activation_tool._terminal_promotion_evidence_errors(
            pending_splice, **kwargs
        )
    )


def test_terminal_promotion_uses_consensus_symbol_canonicalization_for_mixed_case(
    tmp_path,
):
    native_root = tmp_path / "data" / "native"
    run_id = "run-mixed-case-promotion"
    activation_id = "activation-mixed-case-promotion"
    symbol = "Platinum"
    timeframe = "D1"
    run_manifest_relative_path = f"runs/{run_id}/run_manifest.json"
    run_manifest_path = native_root / run_manifest_relative_path
    run_manifest_path.parent.mkdir(parents=True)
    run_manifest_path.write_text("{}\n", encoding="utf-8")
    run_manifest_sha256 = sha256_file(run_manifest_path)

    leaf = native_root / "runs" / run_id / symbol / timeframe
    leaf.mkdir(parents=True)
    provenance = {
        "schema_version": activation_tool.SCHEMA_VERSION,
        "acquisition_contract_version": activation_tool.ACQUISITION_CONTRACT_VERSION,
        "immutable_record": True,
        "run_id": run_id,
        "symbol": symbol,
        "canonical_symbol": symbol,
        "timeframe": timeframe,
        "sha256": None,
    }
    provenance_path = leaf / "provenance.json"
    provenance_path.write_text(
        json.dumps(provenance, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    provenance_sha256 = sha256_file(provenance_path)

    audit_leaf = native_root / "audits" / activation_id / symbol / timeframe
    audit_leaf.mkdir(parents=True)
    sidecar = {
        "schema_version": activation_tool.NATIVE_BARS_SCHEMA,
        "audited_sidecar_contract": activation_tool.AUDITED_SIDECAR_CONTRACT,
        "symbol": symbol,
        "timeframe": timeframe,
        "run_id": run_id,
        "activation_id": activation_id,
        "run_manifest_relative_path": run_manifest_relative_path,
        "run_manifest_sha256": run_manifest_sha256,
        "activation_status": "FAIL",
        "activation_reason_codes": [],
        "parity_status": "FAIL",
        "sha256": None,
        "source_provenance_relative_path": str(
            provenance_path.relative_to(native_root)
        ).replace("\\", "/"),
        "source_provenance_sha256": provenance_sha256,
        "cross_resolution_reference_bindings": {},
    }
    parity = {
        "symbol": symbol.upper(),
        "timeframe": timeframe,
        "run_id": run_id,
        "activation_id": activation_id,
        "run_manifest_relative_path": run_manifest_relative_path,
        "run_manifest_sha256": run_manifest_sha256,
        "cell_activation_status": "FAIL",
        "cell_reason_codes": [],
        "status": "FAIL",
        "source_bars_sha256": None,
        "source_provenance_sha256": provenance_sha256,
        "source_binding": {
            "availability_state": "TECHNICAL_TERMINAL",
            "bars_sha256": None,
            "provenance_sha256": provenance_sha256,
        },
        "reference_bindings": {},
        "cross_resolution_reference_bindings": {},
    }
    sidecar_path = audit_leaf / "bars.meta.json"
    parity_path = audit_leaf / "parity.json"
    sidecar_path.write_text(
        json.dumps(sidecar, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    parity_path.write_text(
        json.dumps(parity, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    entry = {
        "status": NATIVE_TECHNICAL_ERROR,
        "audit_status": "FAIL",
        "resolution_status": "FAIL",
        "consensus_status": "FAIL",
        "reason_codes": [],
        "bars_relative_path": str((leaf / "bars.csv").relative_to(native_root)).replace(
            "\\", "/"
        ),
        "bars_sha256": None,
        "sidecar_relative_path": str(sidecar_path.relative_to(native_root)).replace(
            "\\", "/"
        ),
        "sidecar_sha256": sha256_file(sidecar_path),
        "parity_relative_path": str(parity_path.relative_to(native_root)).replace(
            "\\", "/"
        ),
        "parity_sha256": sha256_file(parity_path),
        "cross_resolution_reference_bindings": {},
    }
    kwargs = {
        "native_root": native_root,
        "run_id": run_id,
        "activation_id": activation_id,
        "symbol": symbol,
        "timeframe": timeframe,
        "run_manifest_relative_path": run_manifest_relative_path,
        "run_manifest_sha256": run_manifest_sha256,
    }

    errors = activation_tool._terminal_promotion_evidence_errors(entry, **kwargs)
    assert not any("SIDECAR_CELL_MISMATCH" in error for error in errors)
    assert not any("PARITY_CELL_MISMATCH" in error for error in errors)
    assert not any("PROVENANCE_CELL_MISMATCH" in error for error in errors)

    parity["symbol"] = "PALLADIUM"
    parity_path.write_text(
        json.dumps(parity, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    entry["parity_sha256"] = sha256_file(parity_path)
    errors = activation_tool._terminal_promotion_evidence_errors(entry, **kwargs)
    assert any("PARITY_CELL_MISMATCH" in error for error in errors)
