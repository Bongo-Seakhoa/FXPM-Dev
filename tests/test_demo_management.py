from __future__ import annotations

import hashlib
import json
import os
import shutil
from contextlib import contextmanager
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

import pm_demo_management as management
from pm_demo_readiness import (
    DEMO_EXECUTION_BINDING_SCHEMA,
    DemoProbationReadinessReport,
    _DemoExecutionBinding,
    demo_position_config_sha256,
)
from pm_position import PositionConfig
from pm_live_readiness import ReadinessFinding


def _bytes(value: Any) -> bytes:
    return json.dumps(
        value,
        allow_nan=False,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


def _sha(value: Any) -> str:
    return hashlib.sha256(_bytes(value)).hexdigest()


def _file_sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(_bytes(value))


def _account(**changes: Any) -> SimpleNamespace:
    values = {
        "login": 101,
        "server": "Demo-Server",
        "currency": "USD",
        "trade_mode": 0,
        "margin_mode": 2,
        "trade_allowed": True,
        "trade_expert": True,
        "hedge_allowed": True,
    }
    values.update(changes)
    return SimpleNamespace(**values)


def _case(
    root: Path,
    *,
    seed: str = "a",
    valid_delta: timedelta = timedelta(days=2),
    absolute_data_dir: bool = False,
) -> SimpleNamespace:
    source = root / f"source-{seed}"
    source.mkdir(parents=True)
    config_path = source / "config.json"
    ledger_path = source / "pm_configs.json"
    manifest_path = source / "pm_configs.generation.json"
    data_dir = str(source.resolve() / "data") if absolute_data_dir else "data"
    _write_json(config_path, {"pipeline": {"data_dir": data_dir}})
    _write_json(ledger_path, {"schema_version": "fixture", "seed": seed})
    config_sha = _file_sha(config_path)
    ledger_sha = _file_sha(ledger_path)

    generation_id = f"generation-{seed}"
    activation_id = f"activation-{seed}"
    native_root = source / "data" / "native"
    bars_path = native_root / "runs" / seed / "EURUSD" / "H1" / "bars.csv"
    sidecar_path = (
        native_root / "audits" / activation_id / "EURUSD" / "H1" / "bars.meta.json"
    )
    bars_path.parent.mkdir(parents=True)
    bars_path.write_bytes(f"time,open\n{seed},1\n".encode())
    _write_json(sidecar_path, {"symbol": "EURUSD", "timeframe": "H1", "seed": seed})
    activation_manifest_path = native_root / "activations" / f"{activation_id}.json"
    activation_manifest = {
        "activation_id": activation_id,
        "entries": {
            "EURUSD": {
                "H1": {
                    "bars_relative_path": bars_path.relative_to(native_root).as_posix(),
                    "bars_sha256": _file_sha(bars_path),
                    "sidecar_relative_path": sidecar_path.relative_to(
                        native_root
                    ).as_posix(),
                    "sidecar_sha256": _file_sha(sidecar_path),
                }
            }
        },
    }
    _write_json(activation_manifest_path, activation_manifest)
    activation_sha = _file_sha(activation_manifest_path)

    raw_artifact_path = source / "refit" / "artifacts" / "input.json"
    _write_json(raw_artifact_path, {"raw": seed})
    artifact_body = {
        "relative_path": "input.json",
        "size_bytes": raw_artifact_path.stat().st_size,
        "content_sha256": _file_sha(raw_artifact_path),
        "semantic_sha256": _sha(f"semantic-{seed}"),
        "format_version": "fixture.v1",
    }
    artifact_descriptor = {**artifact_body, "artifact_id": _sha(artifact_body)}
    raw_manifest_body = {
        "schema_version": "fxpm.raw-current-refit-manifest.v2",
        "executor_contract_version": "fxpm.pipeline-current-refit-raw-executor.v2",
        "current_refit_receipt_sha256": _sha(f"receipt-{seed}"),
        "current_refit_execution_sha256": _sha(f"execution-{seed}"),
        "current_cutoff_utc": "2026-07-14T00:00:00+00:00",
        "bundle_artifact_ids": {},
        "artifact_descriptors": [artifact_descriptor],
        "cell_requests": [],
    }
    raw_manifest = {
        **raw_manifest_body,
        "manifest_sha256": _sha(raw_manifest_body),
    }
    raw_manifest_path = source / "refit" / "raw-manifest.json"
    _write_json(raw_manifest_path, raw_manifest)
    replay_root = source / "capital-replay"
    replay_root.mkdir()
    shard_body = {
        "schema_version": "fxpm.capital-envelope-attempt-shard.v1",
        "shard_index": 0,
        "attempts": [{"attempt_id": f"attempt-{seed}"}],
    }
    shard_raw = _bytes(shard_body) + b"\n"
    shard_sha = hashlib.sha256(shard_raw).hexdigest()
    shard_relative = f"attempt_shards/attempts_0000_{shard_sha}.json"
    shard_path = replay_root / shard_relative
    shard_path.parent.mkdir()
    shard_path.write_bytes(shard_raw)
    replay_body = {
        "schema_version": "fxpm.capital-envelope-causal-replay-ledger.v8",
        "storage_mode": "CONTENT_ADDRESSED_ATTEMPT_SHARDS_V1",
        "attempt_count": 1,
        "daily_observation_count": 0,
        "attempt_shards": [
            {
                "shard_index": 0,
                "relative_path": shard_relative,
                "file_sha256": shard_sha,
                "byte_size": len(shard_raw),
                "attempt_count": 1,
                "daily_observation_count": 0,
                "first_attempt_id": f"attempt-{seed}",
                "last_attempt_id": f"attempt-{seed}",
            }
        ],
    }

    artifacts: dict[str, dict[str, Any]] = {}
    artifact_hashes: dict[str, str] = {}
    for name in management.AUTHORITY_ARTIFACTS:
        path = source / "authority" / f"{name}.json"
        value: dict[str, Any] = {"artifact": name, "seed": seed}
        if name == "current_refit_execution":
            value = {
                "schema_version": ("fxpm.current-refit-execution-authority-index.v1"),
                "current_refit_receipt": {},
                "raw_manifest_relative_path": raw_manifest_path.relative_to(
                    source
                ).as_posix(),
                "artifact_root_relative_path": raw_artifact_path.parent.relative_to(
                    source
                ).as_posix(),
            }
        elif name == "capital_calibration":
            value = {
                "schema_version": "fxpm.capital-calibration-authority-index.v1",
                "replay_evidence_root_relative_path": replay_root.relative_to(
                    source
                ).as_posix(),
                "replay_evidence_receipt": {
                    "body": replay_body,
                },
            }
        _write_json(path, value)
        digest = _file_sha(path)
        artifact_hashes[name] = digest
        artifacts[name] = {
            "relative_path": path.relative_to(source).as_posix(),
            "sha256": digest,
            "size_bytes": path.stat().st_size,
            "schema_version": f"fxpm.{name.replace('_', '-')}-authority-index.v1",
        }

    selected = ["EURUSD|H1|TREND"]
    requested = ["EURUSD"]
    receipt_body = {
        "schema_version": "fxpm.optimization-authority-chain-receipt.v1",
        "generation_id": generation_id,
        "authorization_scope": "DEMO_PROBATION",
        "config_relative_path": config_path.relative_to(source).as_posix(),
        "config_sha256": config_sha,
        "ledger_sha256": ledger_sha,
        "native_activation_id": activation_id,
        "native_activation_manifest_sha256": activation_sha,
        "requested_symbols": requested,
        "authority_artifact_sha256_by_name": artifact_hashes,
    }
    receipt = {**receipt_body, "receipt_sha256": _sha(receipt_body)}
    manifest = {
        "schema_version": "fxpm.optimization-generation.v2",
        "generation_id": generation_id,
        "generation_status": "PROMOTED",
        "technical_failures": {},
        "ledger_sha256": ledger_sha,
        "authority_artifacts": artifacts,
        "authority_chain_receipt": receipt,
    }
    _write_json(manifest_path, manifest)

    names = (
        "historical_process_decision_sha256",
        "accepted_book_generation_receipt_sha256",
        "current_refit_cell_id",
        "winner_decision_sha256",
        "winner_book_entry_sha256",
        "source_snapshot_sha256",
        "parameter_sha256",
        "output_artifact_sha256",
        "regime_fit_receipt_sha256",
        "regime_detector_record_sha256",
        "regime_detector_parameters_sha256",
        "regime_detector_code_sha256",
    )
    current_binding = {name: _sha(f"{seed}:{name}") for name in names}
    expires = datetime.now(timezone.utc) + valid_delta
    cell = {
        "strategy_name": "SpecialistAlpha",
        "promotion_status": "PROCESS_VALIDATED_CURRENT_REFIT",
        "parameters": {"period": 17},
        "artifact_meta": {"current_refit_binding": current_binding},
    }
    projection = {
        "EURUSD": {
            "valid_until": expires.isoformat(),
            "regime_configs": {"H1": {"TREND": cell}},
        }
    }
    detector = {
        "schema_version": "fxpm.detector-projection.fixture.v1",
        "records": {"EURUSD|H1": {"seed": seed}},
    }
    envelope = {
        "max_combined_risk_pct": 33.33,
        "risk_per_trade_pct": 0.25,
    }
    raw = {name: True for name in management.RAW_COMPONENTS}
    authority = {
        "schema_version": "fxpm.live-readiness-authority-result.v2",
        "status": "DEMO_PROBATION_AUTHORIZED",
        "errors": [],
        "raw_reverification": raw,
        "selected_cell_ids": selected,
        "possible_slot_count": 1,
        "source_unavailable_cell_ids": [],
        "regime_unobserved_cell_ids": [],
        "no_local_winner_cell_ids": [],
        "insufficient_evidence_cell_ids": [],
        "technical_cell_ids": [],
        "unsearched_cell_ids": [],
        "missing_observed_cell_ids": [],
        "native_activation_id": activation_id,
        "native_activation_manifest_sha256": activation_sha,
        "ledger_sha256": ledger_sha,
        "selected_capital_envelope": envelope,
        "capital_config_matches": True,
        "capital_execution_enforcement_reverified": True,
        "funded_live_authorized": False,
    }
    position = PositionConfig()
    binding = _DemoExecutionBinding(
        schema_version=DEMO_EXECUTION_BINDING_SCHEMA,
        ledger_path=str(ledger_path),
        ledger_sha256=ledger_sha,
        projection_sha256=_sha(projection),
        detector_projection_sha256=_sha(detector),
        detector_projection_json=_bytes(detector).decode("utf-8"),
        config_path=str(config_path),
        config_sha256=config_sha,
        capital_config_sha256=_sha(f"capital-{seed}"),
        position_config_sha256=demo_position_config_sha256(position),
        selected_capital_envelope=tuple(sorted(envelope.items())),
        native_activation_id=activation_id,
        native_activation_manifest_sha256=activation_sha,
        authority_result_sha256=_sha(authority),
        requested_symbols=tuple(requested),
        selected_cell_ids=tuple(selected),
        authority_result_json=_bytes(authority).decode("utf-8"),
    )
    report = DemoProbationReadinessReport(
        ok=True,
        findings=[],
        _execution_binding=binding,
    )
    native = SimpleNamespace(
        activation_id=activation_id,
        activation_manifest_sha256=activation_sha,
        activation_manifest_path=activation_manifest_path,
    )
    return SimpleNamespace(
        report=report,
        authority=authority,
        projection=projection,
        position=position,
        account=_account(),
        native=native,
        source=source,
        config_path=config_path,
        external_path=raw_artifact_path,
        bars_path=bars_path,
        sidecar_path=sidecar_path,
        generation_id=generation_id,
        activation_id=activation_id,
        activation_sha=activation_sha,
        candidate_id=current_binding["current_refit_cell_id"],
    )


def _create(
    store: Path,
    case: SimpleNamespace,
    *,
    limits: management.ManagementLimits = management.ManagementLimits(),
    authorized_at: datetime | None = None,
) -> dict[str, Any]:
    return management.create_demo_management_capsule(
        store,
        authorization_report=case.report,
        authority_result=case.authority,
        execution_projection=case.projection,
        position_config=case.position,
        account_info=case.account,
        native_universe=case.native,
        authorized_at_utc=authorized_at or datetime.now(timezone.utc),
        limits=limits,
    )


def _native(case: SimpleNamespace) -> SimpleNamespace:
    return case.native


def _install_management_verifier(
    monkeypatch: pytest.MonkeyPatch,
    case: SimpleNamespace,
    captured: dict[str, Any],
) -> None:
    import pm_optimization_authority

    def verifier(
        *, management_only_context: dict[str, Any], **kwargs: Any
    ) -> dict[str, Any]:
        captured["context"] = management_only_context
        captured["kwargs"] = kwargs
        return {**case.authority, "status": management.MANAGEMENT_ONLY}

    verifier.__module__ = "pm_optimization_authority"
    monkeypatch.setattr(
        pm_optimization_authority,
        "verify_live_readiness_authority_chain",
        verifier,
    )


def test_capsule_snapshots_exact_closure_and_exposes_no_entry_configs(
    tmp_path: Path,
) -> None:
    case = _case(tmp_path)
    store = tmp_path / "store"
    result = _create(store, case)
    assert result["status"] == management.CAPSULE_WRITTEN
    assert result["permissions"] == management.NO_MANAGEMENT_PERMISSIONS
    assert "entry_configs" not in result
    case.config_path.write_bytes(b"changed-after-capture")
    loaded = management.load_demo_management_capsule(store, result["capsule_id"])
    assert loaded["status"] == management.CAPSULE_VALID
    assert loaded["permissions"] == management.NO_MANAGEMENT_PERMISSIONS
    assert loaded["managed_candidate_ids"] == [case.candidate_id]
    assert "entry_configs" not in loaded
    document = json.loads(
        (store / result["capsule_id"] / "capsule.json").read_text(encoding="utf-8")
    )
    roles = {item["role"] for item in document["external_closure"].values()}
    assert roles == {
        "current_refit_artifact",
        "capital_replay_artifact",
        "native_bars",
        "native_sidecar",
    }
    assert document["source_snapshots"]["native_activation_manifest"]["sha256"] == (
        case.activation_sha
    )


def _block_interval_entries(case: SimpleNamespace) -> None:
    case.report.ok = False
    case.report.findings = [ReadinessFinding(
        "error", "interval_cell_qualification_policy_unadopted",
        "Synthetic fixture: interval entry qualification remains unadopted.",
        "evidence",
    )]


def test_entry_policy_block_can_capture_but_cannot_grant_management(tmp_path: Path) -> None:
    case = _case(tmp_path)
    _block_interval_entries(case)
    store = tmp_path / "store"
    created = _create(store, case)
    assert created["status"] == management.CAPSULE_WRITTEN
    assert created["permissions"] == management.NO_MANAGEMENT_PERMISSIONS
    assert case.report.ok is False
    assert case.report.management_only is False
    assert "entry_configs" not in created
    loaded = management.load_demo_management_capsule(store, created["capsule_id"])
    assert loaded["status"] == management.CAPSULE_VALID
    assert loaded["permissions"] == management.NO_MANAGEMENT_PERMISSIONS


def test_entry_policy_exception_preserves_the_same_captured_authority(tmp_path: Path) -> None:
    case = _case(tmp_path)
    authorized_at = datetime.now(timezone.utc)
    ordinary_store, blocked_store = tmp_path / "ordinary", tmp_path / "entry-blocked"
    ordinary = _create(ordinary_store, case, authorized_at=authorized_at)
    _block_interval_entries(case)
    blocked = _create(blocked_store, case, authorized_at=authorized_at)
    captured = []
    verified = []
    validation_time = datetime.now(timezone.utc)
    for store, result in ((ordinary_store, ordinary), (blocked_store, blocked)):
        assert result["status"] == management.CAPSULE_WRITTEN
        assert result["permissions"] == management.NO_MANAGEMENT_PERMISSIONS
        document = json.loads((store / result["capsule_id"] / "capsule.json").read_text(encoding="utf-8"))
        body = dict(document)
        assert body.pop("capsule_id") == result["capsule_id"] == _sha(body)
        # Only capture time and its content-addressed ID may differ.
        body.pop("created_at_utc")
        captured.append(body)
        # Raw authority is deliberately fictional in this fixture, so both
        # real reverifications must reject it without granting permissions.
        response = management.reverify_demo_management_capsule(
            store, result["capsule_id"], account_info=case.account,
            native_universe=_native(case), validation_time_utc=validation_time,
        )
        assert response["status"] == management.BLOCKED
        assert response["permissions"] == management.NO_MANAGEMENT_PERMISSIONS
        assert response.pop("capsule_id") == result["capsule_id"]
        verified.append(response)
    assert captured[0] == captured[1]
    assert verified[0] == verified[1]
    assert case.report.ok is False
    assert case.report.management_only is False


@pytest.mark.parametrize("fault", [
    "wrong_error", "wrong_scope", "extra_error", "missing_error", "missing_binding",
    "invalid_binding", "mismatched_binding", "duck_report", "subclass_report", "false_like",
])
def test_entry_policy_capture_exception_is_exact(tmp_path: Path, fault: str) -> None:
    case = _case(tmp_path)
    _block_interval_entries(case)
    if fault == "wrong_error":
        case.report.findings[0].code = "unrelated_policy_unadopted"
    elif fault == "wrong_scope":
        case.report.findings[0].scope = "runtime"
    elif fault == "extra_error":
        case.report.findings.append(ReadinessFinding("error", "extra", "Synthetic blocker"))
    elif fault == "missing_error":
        case.report.findings = []
    elif fault == "missing_binding":
        case.report._execution_binding = None
    elif fault == "invalid_binding":
        case.report._execution_binding = replace(case.report._execution_binding, schema_version="wrong")
    elif fault == "mismatched_binding":
        case.report._execution_binding = replace(case.report._execution_binding, authority_result_sha256="0" * 64)
    elif fault == "duck_report":
        case.report = SimpleNamespace(**vars(case.report))
    elif fault == "subclass_report":
        class OtherReport(DemoProbationReadinessReport):
            pass
        case.report = OtherReport(**vars(case.report))
    else:
        case.report.ok = 0
    store = tmp_path / "store"
    result = _create(store, case)
    assert result["status"] == management.BLOCKED
    assert result["permissions"] == management.NO_MANAGEMENT_PERMISSIONS
    assert not store.exists()


def test_entry_blocked_capsule_still_requires_real_raw_reverification(tmp_path: Path) -> None:
    case = _case(tmp_path)
    _block_interval_entries(case)
    store = tmp_path / "store"
    created = _create(store, case)
    assert created["status"] == management.CAPSULE_WRITTEN
    result = management.reverify_demo_management_capsule(
        store, created["capsule_id"], account_info=case.account, native_universe=_native(case),
    )
    assert result["status"] == management.BLOCKED
    assert result["permissions"] == management.NO_MANAGEMENT_PERMISSIONS


@pytest.mark.parametrize("replacement", [b"x", b""])
def test_capsule_rejects_tamper_and_truncation(
    tmp_path: Path,
    replacement: bytes,
) -> None:
    case = _case(tmp_path)
    store = tmp_path / "store"
    created = _create(store, case)
    capsule = store / created["capsule_id"]
    document = json.loads((capsule / "capsule.json").read_text(encoding="utf-8"))
    target = capsule / document["source_snapshots"]["ledger"]["capsule_relative_path"]
    target.write_bytes(replacement)
    loaded = management.load_demo_management_capsule(store, created["capsule_id"])
    assert loaded["status"] == management.BLOCKED


@pytest.mark.parametrize("invalid_path", ["../escape", "."])
def test_capsule_rejects_rehashed_path_traversal(
    tmp_path: Path, invalid_path: str
) -> None:
    case = _case(tmp_path)
    store = tmp_path / "store"
    created = _create(store, case)
    old_dir = store / created["capsule_id"]
    document = json.loads((old_dir / "capsule.json").read_text(encoding="utf-8"))
    document["source_snapshots"]["config"]["capsule_relative_path"] = invalid_path
    body = dict(document)
    body.pop("capsule_id")
    new_id = _sha(body)
    document["capsule_id"] = new_id
    (old_dir / "capsule.json").write_bytes(_bytes(document))
    new_dir = store / new_id
    old_dir.rename(new_dir)
    loaded = management.load_demo_management_capsule(store, new_id)
    assert loaded["status"] == management.BLOCKED


def test_capsule_rejects_symlinked_snapshot(tmp_path: Path) -> None:
    case = _case(tmp_path)
    store = tmp_path / "store"
    created = _create(store, case)
    capsule = store / created["capsule_id"]
    document = json.loads((capsule / "capsule.json").read_text(encoding="utf-8"))
    target = capsule / document["source_snapshots"]["config"]["capsule_relative_path"]
    other = capsule / "other"
    other.write_bytes(target.read_bytes())
    target.unlink()
    try:
        os.symlink(other, target)
    except OSError:
        pytest.skip("symlink creation unavailable")
    loaded = management.load_demo_management_capsule(store, created["capsule_id"])
    assert loaded["status"] == management.BLOCKED


@pytest.mark.parametrize(
    "account",
    [
        _account(login=202),
        _account(trade_mode=2),
        _account(margin_mode=0),
    ],
)
def test_reverify_rejects_account_swap_real_and_netting(
    tmp_path: Path,
    account: SimpleNamespace,
) -> None:
    case = _case(tmp_path)
    store = tmp_path / "store"
    created = _create(store, case)
    result = management.reverify_demo_management_capsule(
        store,
        created["capsule_id"],
        account_info=account,
        native_universe=_native(case),
    )
    assert result["status"] == management.BLOCKED
    assert result["permissions"] == management.NO_MANAGEMENT_PERMISSIONS


def test_real_raw_verifier_fails_closed_on_fictional_authority(tmp_path: Path) -> None:
    case = _case(tmp_path)
    store = tmp_path / "store"
    created = _create(store, case)
    result = management.reverify_demo_management_capsule(
        store,
        created["capsule_id"],
        account_info=case.account,
        native_universe=_native(case),
    )
    assert result["status"] == management.BLOCKED


def test_readiness_orchestration_fails_closed_without_real_reverification(
    tmp_path: Path,
) -> None:
    import pm_demo_readiness as readiness

    case = _case(tmp_path)
    with pytest.raises(ValueError):
        readiness._capture_demo_management_authority(
            report=case.report,
            binding=case.report._execution_binding,
            projection=case.projection,
            position_config=case.position,
            account_info=case.account,
            native_universe=case.native,
        )


def test_invalid_explicit_validation_time_fails_closed(tmp_path: Path) -> None:
    case = _case(tmp_path)
    store = tmp_path / "store"
    created = _create(store, case)
    result = management.reverify_demo_management_capsule(
        store,
        created["capsule_id"],
        account_info=case.account,
        native_universe=_native(case),
        validation_time_utc="",
    )
    assert result["status"] == management.BLOCKED
    assert result["errors"] == ["MANAGEMENT_VALIDATION_TIME_INVALID"]


def test_supported_raw_verifier_returns_management_only_after_expiry(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    case = _case(tmp_path, valid_delta=timedelta(minutes=30))
    store = tmp_path / "store"
    created = _create(store, case)
    captured: dict[str, Any] = {}
    _install_management_verifier(monkeypatch, case, captured)
    result = management.reverify_demo_management_capsule(
        store,
        created["capsule_id"],
        account_info=case.account,
        native_universe=_native(case),
        validation_time_utc=datetime.now(timezone.utc) + timedelta(days=1),
    )
    assert result["status"] == management.MANAGEMENT_ONLY
    assert result["permissions"] == management.MANAGEMENT_PERMISSIONS
    assert captured["context"]["allow_expired_entry_management"] is True
    assert captured["context"]["allow_inactive_generation_pointer"] is True
    assert captured["context"]["allow_new_entries"] is False
    assert captured["context"]["scope"] == management.MANAGEMENT_ONLY
    assert captured["context"]["captured_source_root"] == str(case.source.resolve())
    assert set(captured["context"]["external_closure"])
    assert set(captured["context"]["selected_native_source_bindings"]) == {"EURUSD|H1"}
    assert str(store.resolve()) in str(captured["kwargs"]["config_path"])
    assert "entry_configs" not in result
    assert result["management_context"]["status"] == management.MANAGEMENT_ONLY
    assert result["management_context"]["execution_projection"] == case.projection


def test_capsule_load_survives_source_removal_but_reverify_blocks(
    tmp_path: Path,
) -> None:
    case = _case(tmp_path)
    store = tmp_path / "store"
    created = _create(store, case)
    shutil.rmtree(case.source)
    loaded = management.load_demo_management_capsule(store, created["capsule_id"])
    assert loaded["status"] == management.CAPSULE_VALID
    result = management.reverify_demo_management_capsule(
        store,
        created["capsule_id"],
        account_info=case.account,
        native_universe=_native(case),
    )
    assert result["status"] == management.BLOCKED


def test_capsule_load_supports_captured_absolute_data_root(tmp_path: Path) -> None:
    case = _case(tmp_path, absolute_data_dir=True)
    store = tmp_path / "store"
    created = _create(store, case)
    assert created["status"] == management.CAPSULE_WRITTEN
    shutil.rmtree(case.source)
    loaded = management.load_demo_management_capsule(store, created["capsule_id"])
    assert loaded["status"] == management.CAPSULE_VALID


def test_external_reference_change_blocks_reverification(tmp_path: Path) -> None:
    case = _case(tmp_path)
    store = tmp_path / "store"
    created = _create(store, case)
    case.external_path.write_bytes(b"changed-native-data")
    result = management.reverify_demo_management_capsule(
        store,
        created["capsule_id"],
        account_info=case.account,
        native_universe=_native(case),
    )
    assert result["status"] == management.BLOCKED


def test_invalid_authorization_chronology_is_rejected(tmp_path: Path) -> None:
    expired = _case(tmp_path / "expired", valid_delta=timedelta(days=-1))
    assert _create(tmp_path / "store-a", expired)["status"] == management.BLOCKED
    valid = _case(tmp_path / "future", seed="b")
    future = datetime.now(timezone.utc) + timedelta(minutes=10)
    assert (
        _create(tmp_path / "store-b", valid, authorized_at=future)["status"]
        == management.BLOCKED
    )


def test_retention_rejects_at_bound_and_prune_respects_references(
    tmp_path: Path,
) -> None:
    limits = management.ManagementLimits(max_capsules=2)
    store = tmp_path / "store"
    first = _case(tmp_path / "a", seed="a")
    second = _case(tmp_path / "b", seed="b")
    third = _case(tmp_path / "c", seed="c")
    one = _create(store, first, limits=limits)
    two = _create(store, second, limits=limits)
    assert one["status"] == management.CAPSULE_WRITTEN
    assert two["status"] == management.CAPSULE_WRITTEN
    assert _create(store, third, limits=limits)["status"] == management.BLOCKED

    generation_ref = management.prune_demo_management_capsules(
        store,
        [one["capsule_id"]],
        referenced_generation_ids=[first.generation_id],
        limits=limits,
    )
    assert generation_ref["referenced_capsule_ids"] == [one["capsule_id"]]
    candidate_ref = management.prune_demo_management_capsules(
        store,
        [one["capsule_id"]],
        referenced_candidate_ids=[first.candidate_id],
        limits=limits,
    )
    assert candidate_ref["referenced_capsule_ids"] == [one["capsule_id"]]
    removed = management.prune_demo_management_capsules(
        store,
        [two["capsule_id"]],
        limits=limits,
    )
    assert removed["removed_capsule_ids"] == [two["capsule_id"]]
    assert _create(store, third, limits=limits)["status"] == management.CAPSULE_WRITTEN


def test_external_pins_are_deduplicated_and_hash_bound(tmp_path: Path) -> None:
    case = _case(tmp_path)
    store = tmp_path / "store"
    first = _create(store, case)
    second = _create(store, case)
    assert first["capsule_id"] != second["capsule_id"]
    result = management.list_demo_management_external_pins(store)
    assert result["status"] == management.MANAGEMENT_ONLY
    assert result["capsule_ids"] == sorted([first["capsule_id"], second["capsule_id"]])
    assert len(result["external_pins"]) == 4
    assert result["external_pins_sha256"] == _sha(result["external_pins"])
    assert all(
        item["capsule_ids"] == result["capsule_ids"]
        for item in result["external_pins"].values()
    )


def test_external_pin_listing_fails_closed_on_malformed_capsule(tmp_path: Path) -> None:
    case = _case(tmp_path)
    store = tmp_path / "store"
    created = _create(store, case)
    capsule = store / created["capsule_id"]
    document = json.loads((capsule / "capsule.json").read_text(encoding="utf-8"))
    target = capsule / document["source_snapshots"]["ledger"]["capsule_relative_path"]
    target.write_bytes(b"tampered")
    result = management.list_demo_management_external_pins(store)
    assert result["status"] == management.BLOCKED
    assert result["external_pins"] == {}


def test_external_pin_listing_obeys_record_bound(tmp_path: Path) -> None:
    case = _case(tmp_path)
    store = tmp_path / "store"
    assert _create(store, case)["status"] == management.CAPSULE_WRITTEN
    limits = management.ManagementLimits(max_pin_records=1)
    result = management.list_demo_management_external_pins(store, limits=limits)
    assert result["status"] == management.BLOCKED
    assert result["external_pins"] == {}


def test_retention_guard_holds_exact_pins_and_store(tmp_path: Path) -> None:
    case = _case(tmp_path)
    store = tmp_path / "store"
    created = _create(store, case)
    assert created["status"] == management.CAPSULE_WRITTEN

    with management.hold_demo_management_retention_guard(
        store, [case.external_path]
    ) as guarded:
        assert guarded["status"] == management.BLOCKED
        assert guarded["authorized_paths"] == []
        assert str(case.external_path.resolve()) in guarded["blocked_paths"]

    unrelated = tmp_path / "unrelated.json"
    unrelated.write_text("{}", encoding="utf-8")
    with management.hold_demo_management_retention_guard(store, [unrelated]) as guarded:
        assert guarded["status"] == management.RETENTION_AUTHORIZED
        assert guarded["authorized_paths"] == [str(unrelated.resolve())]

    with management.hold_demo_management_retention_guard(store, [store]) as guarded:
        assert guarded["status"] == management.BLOCKED
        assert "CAPSULE_STORE" in guarded["blocked_paths"][str(store.resolve())]


def test_retention_guard_is_bounded_and_fail_closed(tmp_path: Path) -> None:
    case = _case(tmp_path)
    store = tmp_path / "store"
    _create(store, case)
    limits = management.ManagementLimits(max_retention_candidates=1)
    with management.hold_demo_management_retention_guard(
        store, [tmp_path / "a", tmp_path / "b"], limits=limits
    ) as guarded:
        assert guarded["status"] == management.BLOCKED
        assert guarded["errors"] == ["MANAGEMENT_RETENTION_REQUEST_INVALID"]


def test_readiness_retention_api_uses_deterministic_store(tmp_path: Path) -> None:
    import pm_demo_readiness as readiness

    case = _case(tmp_path)
    ledger = case.source / "pm_configs.json"
    store = readiness.demo_management_store_root(ledger)
    assert _create(store, case)["status"] == management.CAPSULE_WRITTEN
    pins = readiness.list_demo_management_retention_pins(ledger)
    assert pins["status"] == management.MANAGEMENT_ONLY
    with readiness.hold_demo_management_retention_for_ledger(
        ledger, [case.external_path]
    ) as guarded:
        assert guarded["status"] == management.BLOCKED


def test_capsule_rechecks_external_closure_after_lock(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    case = _case(tmp_path)
    store = tmp_path / "store"
    original_lock = management._store_lock

    @contextmanager
    def mutate_before_locked_write(root: Path):
        with original_lock(root):
            case.external_path.write_bytes(b"changed after preflight")
            yield

    monkeypatch.setattr(management, "_store_lock", mutate_before_locked_write)
    created = _create(store, case)
    assert created["status"] == management.BLOCKED
    assert created["errors"] == ["MANAGEMENT_EXTERNAL_REFERENCE_BINDING_MISMATCH"]
    assert not any(path.is_dir() for path in store.iterdir())


@pytest.mark.parametrize(
    "limits",
    [
        management.ManagementLimits(max_capsule_bytes=10),
        management.ManagementLimits(max_store_files=14),
    ],
)
def test_byte_and_file_capacity_bounds_are_fail_closed(
    tmp_path: Path,
    limits: management.ManagementLimits,
) -> None:
    case = _case(tmp_path)
    result = _create(tmp_path / "store", case, limits=limits)
    assert result["status"] == management.BLOCKED
