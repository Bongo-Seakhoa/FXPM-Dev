from __future__ import annotations

from pathlib import Path
import threading

import pytest

import pm_main
import pm_production_generation_entrypoint as entrypoint
import pm_production_generation_supervisor as supervisor


def _paths(tmp_path: Path) -> tuple[Path, Path, Path]:
    config = tmp_path / "config.json"
    ledger = tmp_path / "pm_configs.json"
    config.write_text("{}", encoding="utf-8")
    ledger.write_bytes(b'{"EURUSD":{}}\n')
    resolved_ledger = ledger.resolve()
    return (
        config.resolve(),
        resolved_ledger,
        entrypoint._supervision_root(resolved_ledger),  # noqa: SLF001
    )


def _argv(config: Path) -> list[str]:
    return ["--config", str(config), "--optimize", "--overwrite"]


def test_child_dispatch_reuses_exact_generation(
    tmp_path: Path,
    monkeypatch,
) -> None:
    config, ledger, _root = _paths(tmp_path)
    observed = {}

    def fake_main(argv, **kwargs):
        observed["argv"] = list(argv)
        observed.update(kwargs)
        return 2

    monkeypatch.setattr(pm_main, "main", fake_main)
    result = entrypoint.dispatch_optimization_cli(
        {
            "schema_version": entrypoint.ENTRYPOINT_REQUEST_SCHEMA,
            "argv": _argv(config),
            "generation_id": "a" * 64,
            "active_config_path": str(config),
            "winner_ledger_path": str(ledger),
        },
    )

    assert observed["argv"] == _argv(config)
    assert observed["_supervised_generation_id"] == "a" * 64
    assert set(observed) == {"argv", "_supervised_generation_id"}
    assert result["generation_id"] == "a" * 64
    assert result["cli_exit_code"] == 2


def test_forged_run_lease_is_rejected_before_supervision_state_creation(
    tmp_path: Path,
) -> None:
    config, ledger, root = _paths(tmp_path)

    with pytest.raises(
        entrypoint.ProductionGenerationEntrypointError,
        match="PRODUCTION_SUPERVISOR_RUN_LEASE_INVALID",
    ):
        entrypoint.run_supervised_optimization_cli(
            argv=_argv(config),
            active_config_path=config,
            winner_ledger_path=ledger,
            recovery_callback=lambda _value: "BAD",
            generation_id="d" * 64,
            run_lease=object(),
        )

    assert not root.exists()


def test_parent_supervision_binds_request_and_reopens_live_identities(
    tmp_path: Path,
    monkeypatch,
) -> None:
    config, ledger, root = _paths(tmp_path)
    generation_id = "b" * 64
    observed = {}
    recovered: list[str] = []

    def fake_supervise(payload, **kwargs):
        observed["payload"] = dict(payload)
        observed.update(kwargs)
        child = {
            "schema_version": entrypoint.ENTRYPOINT_RESULT_SCHEMA,
            "generation_id": generation_id,
            "active_config_path": str(config),
            "winner_ledger_path": str(ledger),
            "cli_exit_code": 0,
            "winner_ledger_sha256": entrypoint._file_sha256(ledger),  # noqa: SLF001
            "active_manifest_sha256": entrypoint._file_sha256(  # noqa: SLF001
                ledger.with_name("pm_configs.generation.json")
            ),
        }
        return supervisor.SupervisedDispatchResult(
            request_id=generation_id,
            request_sha256="c" * 64,
            status="SUCCEEDED",
            payload=child,
            error=None,
            worker_pid=123,
            process_exit_code=0,
            termination_complete=True,
            worker_result_path=tmp_path / "worker.json",
            result_path=tmp_path / "result.json",
        )

    monkeypatch.setattr(
        entrypoint.generation_supervisor,
        "run_supervised_dispatch",
        fake_supervise,
    )
    result = entrypoint.run_supervised_optimization_cli(
        argv=_argv(config),
        active_config_path=config,
        winner_ledger_path=ledger,
        recovery_callback=lambda value: recovered.append(value)
        or "REFERENCED_ACTIVE",
        generation_id=generation_id,
    )

    assert result.succeeded
    assert result.cli_exit_code == 0
    assert observed["payload"]["generation_id"] == generation_id
    assert set(observed) == {
        "payload",
        "dispatch_path",
        "request_path",
        "result_path",
        "request_id",
    }
    assert observed["request_id"] == generation_id
    assert observed["dispatch_path"].endswith(":dispatch_optimization_cli")
    assert recovered == [generation_id]
    assert result.generation_recovery_status == "REFERENCED_ACTIVE"
    assert result.request_directory_retained is False
    assert root.is_dir()
    assert list(root.iterdir()) == []


def test_parent_rejects_child_live_identity_substitution(
    tmp_path: Path,
    monkeypatch,
) -> None:
    config, ledger, root = _paths(tmp_path)
    generation_id = "d" * 64

    def fake_supervise(_payload, **_kwargs):
        return supervisor.SupervisedDispatchResult(
            request_id=generation_id,
            request_sha256="e" * 64,
            status="SUCCEEDED",
            payload={
                "schema_version": entrypoint.ENTRYPOINT_RESULT_SCHEMA,
                "generation_id": generation_id,
                "active_config_path": str(config),
                "winner_ledger_path": str(ledger),
                "cli_exit_code": 0,
                "winner_ledger_sha256": "0" * 64,
                "active_manifest_sha256": entrypoint._file_sha256(  # noqa: SLF001
                    ledger.with_name("pm_configs.generation.json")
                ),
            },
            error=None,
            worker_pid=456,
            process_exit_code=0,
            termination_complete=True,
            worker_result_path=tmp_path / "worker.json",
            result_path=tmp_path / "result.json",
        )

    monkeypatch.setattr(
        entrypoint.generation_supervisor,
        "run_supervised_dispatch",
        fake_supervise,
    )
    with pytest.raises(
        entrypoint.ProductionGenerationEntrypointError,
        match="PRODUCTION_SUPERVISOR_CHILD_LIVE_IDENTITY_MISMATCH",
    ):
        entrypoint.run_supervised_optimization_cli(
            argv=_argv(config),
            active_config_path=config,
            winner_ledger_path=ledger,
            recovery_callback=lambda _value: pytest.fail(
                "tampered child result reached recovery"
            ),
            generation_id=generation_id,
        )


def test_child_failure_has_no_payload_and_opens_only_reconciliation_gate(
    tmp_path: Path,
    monkeypatch,
) -> None:
    config, ledger, root = _paths(tmp_path)
    generation_id = "f" * 64
    recovered: list[str] = []

    def fake_supervise(_payload, **_kwargs):
        return supervisor.SupervisedDispatchResult(
            request_id=generation_id,
            request_sha256="1" * 64,
            status="FAILED",
            payload={},
            error={
                "code": "SUPERVISOR_DISPATCH_FAILED",
                "type": "RuntimeError",
                "message": "child failed",
            },
            worker_pid=789,
            process_exit_code=-1,
            termination_complete=True,
            worker_result_path=tmp_path / "worker.json",
            result_path=tmp_path / "result.json",
        )

    monkeypatch.setattr(
        entrypoint.generation_supervisor,
        "run_supervised_dispatch",
        fake_supervise,
    )
    result = entrypoint.run_supervised_optimization_cli(
        argv=_argv(config),
        active_config_path=config,
        winner_ledger_path=ledger,
        recovery_callback=lambda value: recovered.append(value)
        or "REMOVED",
        generation_id=generation_id,
    )

    assert not result.succeeded
    assert result.cli_exit_code is None
    assert result.safe_to_reconcile
    assert recovered == [generation_id]
    assert result.generation_recovery_status == "REMOVED"
    assert result.request_directory_retained is False
    assert list(root.iterdir()) == []

def test_abandoned_prepublication_owner_temporary_is_self_healing(
    tmp_path: Path,
) -> None:
    config, ledger, root = _paths(tmp_path)
    root.mkdir()
    temporary = root / (
        f".{entrypoint.OWNER_RECORD_NAME}.1234."
        f"{'a' * (entrypoint.OWNER_TEMP_TOKEN_BYTES * 2)}.tmp"
    )
    temporary.write_bytes(b"partial")

    result = entrypoint._recover_stale_request(  # noqa: SLF001
        root=root,
        active_config_path=config,
        winner_ledger_path=ledger,
        recovery_callback=lambda _value: pytest.fail(
            "an unpublished owner cannot own generation artifacts"
        ),
    )

    assert result is None
    assert list(root.iterdir()) == []


def test_authenticated_stale_request_with_legacy_extension_recovers(
    tmp_path: Path,
    monkeypatch,
) -> None:
    config, ledger, root = _paths(tmp_path)
    root.mkdir()
    generation_id = "8" * 64
    owner_pid = 424243
    owner = {
        "schema_version": entrypoint.OWNER_RECORD_SCHEMA,
        "generation_id": generation_id,
        "owner_pid": owner_pid,
        "owner_creation_ticks": 1,
        "active_config_path": str(config),
        "winner_ledger_path": str(ledger),
    }
    owner["content_sha256"] = entrypoint._owner_digest(owner)  # noqa: SLF001
    entrypoint._write_owner_record(  # noqa: SLF001
        root / entrypoint.OWNER_RECORD_NAME,
        owner,
    )
    request_directory = root / f"generation-{generation_id}"
    request_directory.mkdir()
    request = supervisor._build_request(  # noqa: SLF001
        request_id=generation_id,
        dispatch_path=(
            "pm_production_generation_entrypoint:"
            "dispatch_optimization_cli"
        ),
        payload=entrypoint._request_payload(  # noqa: SLF001
            argv=_argv(config),
            generation_id=generation_id,
            active_config_path=config,
            winner_ledger_path=ledger,
        ),
    )
    request_body = dict(request)
    request_body.pop("content_sha256")
    request_body["legacy_extension"] = {
        "opaque": ["preserved", 17, {"enabled": True}]
    }
    extended_request = supervisor._with_content_sha(  # noqa: SLF001
        request_body,
        maximum=supervisor.MAX_REQUEST_BYTES,
    )
    (request_directory / "request.json").write_bytes(
        supervisor._canonical_bytes(  # noqa: SLF001
            extended_request,
            maximum=supervisor.MAX_REQUEST_BYTES,
        )
    )
    original_identity = entrypoint._process_creation_ticks  # noqa: SLF001
    monkeypatch.setattr(
        entrypoint,
        "_process_creation_ticks",
        lambda pid: None if pid == owner_pid else original_identity(pid),
    )
    recovered: list[str] = []

    status = entrypoint._recover_stale_request(  # noqa: SLF001
        root=root,
        active_config_path=config,
        winner_ledger_path=ledger,
        recovery_callback=lambda value: recovered.append(value) or "REMOVED",
    )

    assert status == "REMOVED"
    assert recovered == [generation_id]
    assert list(root.iterdir()) == []


def test_unproven_process_tree_retains_request_and_never_recovers(
    tmp_path: Path,
    monkeypatch,
) -> None:
    config, ledger, root = _paths(tmp_path)
    generation_id = "2" * 64
    recovered: list[str] = []

    monkeypatch.setattr(
        entrypoint.generation_supervisor,
        "run_supervised_dispatch",
        lambda _payload, **_kwargs: supervisor.SupervisedDispatchResult(
            request_id=generation_id,
            request_sha256="3" * 64,
            status="TERMINATION_FAILED",
            payload={},
            error={
                "code": "SUPERVISOR_PROCESS_TREE_TERMINATION_FAILED",
                "type": "RuntimeError",
                "message": "not proven empty",
            },
            worker_pid=321,
            process_exit_code=None,
            termination_complete=False,
            worker_result_path=tmp_path / "worker.json",
            result_path=tmp_path / "result.json",
        ),
    )

    result = entrypoint.run_supervised_optimization_cli(
        argv=_argv(config),
        active_config_path=config,
        winner_ledger_path=ledger,
        recovery_callback=lambda value: recovered.append(value) or "BAD",
        generation_id=generation_id,
    )

    assert recovered == []
    assert result.request_directory_retained is True
    assert (root / entrypoint.OWNER_RECORD_NAME).is_file()
    assert result.request_directory.is_dir()


def test_stale_dead_owner_is_recovered_before_new_generation(
    tmp_path: Path,
    monkeypatch,
) -> None:
    config, ledger, root = _paths(tmp_path)
    root.mkdir()
    stale_id = "4" * 64
    fresh_id = "5" * 64
    stale_pid = 424242
    owner = {
        "schema_version": entrypoint.OWNER_RECORD_SCHEMA,
        "generation_id": stale_id,
        "owner_pid": stale_pid,
        "owner_creation_ticks": 1,
        "active_config_path": str(config),
        "winner_ledger_path": str(ledger),
    }
    owner["content_sha256"] = entrypoint._owner_digest(owner)  # noqa: SLF001
    entrypoint._write_owner_record(  # noqa: SLF001
        root / entrypoint.OWNER_RECORD_NAME,
        owner,
    )
    (root / f"generation-{stale_id}").mkdir()
    original_identity = entrypoint._process_creation_ticks  # noqa: SLF001
    monkeypatch.setattr(
        entrypoint,
        "_process_creation_ticks",
        lambda pid: None if pid == stale_pid else original_identity(pid),
    )

    def fake_supervise(_payload, **_kwargs):
        return supervisor.SupervisedDispatchResult(
            request_id=fresh_id,
            request_sha256="6" * 64,
            status="FAILED",
            payload={},
            error={
                "code": "SUPERVISOR_DISPATCH_FAILED",
                "type": "RuntimeError",
                "message": "child failed",
            },
            worker_pid=654,
            process_exit_code=-1,
            termination_complete=True,
            worker_result_path=tmp_path / "worker.json",
            result_path=tmp_path / "result.json",
        )

    monkeypatch.setattr(
        entrypoint.generation_supervisor,
        "run_supervised_dispatch",
        fake_supervise,
    )
    recovered: list[str] = []

    result = entrypoint.run_supervised_optimization_cli(
        argv=_argv(config),
        active_config_path=config,
        winner_ledger_path=ledger,
        recovery_callback=lambda value: recovered.append(value)
        or ("ABSENT" if value == stale_id else "REMOVED"),
        generation_id=fresh_id,
    )

    assert recovered == [stale_id, fresh_id]
    assert result.stale_recovery_status == "ABSENT"
    assert result.generation_recovery_status == "REMOVED"
    assert list(root.iterdir()) == []


def test_second_parent_fails_before_spawn_while_run_lease_is_owned(
    tmp_path: Path,
    monkeypatch,
) -> None:
    config, ledger, _root = _paths(tmp_path)
    entered = threading.Event()
    release = threading.Event()

    def hold() -> None:
        with entrypoint._exclusive_file_lock(  # noqa: SLF001
            entrypoint._run_lock_path(ledger)  # noqa: SLF001
        ):
            entered.set()
            release.wait(timeout=5)

    thread = threading.Thread(target=hold, daemon=True)
    thread.start()
    assert entered.wait(timeout=2)
    monkeypatch.setattr(
        entrypoint.generation_supervisor,
        "run_supervised_dispatch",
        lambda *_args, **_kwargs: pytest.fail(
            "contending parent spawned a child"
        ),
    )
    try:
        with pytest.raises(
            entrypoint.ProductionGenerationEntrypointError,
            match="PRODUCTION_SUPERVISOR_ALREADY_RUNNING",
        ):
            entrypoint.run_supervised_optimization_cli(
                argv=_argv(config),
                active_config_path=config,
                winner_ledger_path=ledger,
                recovery_callback=lambda _value: "BAD",
                generation_id="7" * 64,
            )
    finally:
        release.set()
        thread.join(timeout=2)
    assert not thread.is_alive()
