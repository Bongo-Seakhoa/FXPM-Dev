from __future__ import annotations

import ctypes
import json
import os
from pathlib import Path
import subprocess
import sys
from typing import Any

import pytest

import pm_production_generation_supervisor as supervisor


def _echo_dispatch(payload: dict[str, Any]) -> dict:
    return {
        "echo": payload,
        "pid": os.getpid(),
    }


def _spawn_lingering_descendant_dispatch(
    payload: dict[str, Any],
) -> dict:
    child = subprocess.Popen(
        [sys.executable, "-c", "import time; time.sleep(60)"],
        shell=False,
    )
    Path(payload["pid_path"]).write_text(str(child.pid), encoding="ascii")
    return {"descendant_pid": child.pid}


def _child_environment() -> dict[str, str]:
    tests_dir = str(Path(__file__).resolve().parent)
    repository = str(Path(__file__).resolve().parent.parent)
    existing = os.environ.get("PYTHONPATH", "")
    return {
        "PYTHONPATH": os.pathsep.join(
            value for value in (tests_dir, repository, existing) if value
        )
    }


def _paths(tmp_path: Path, stem: str = "run") -> tuple[Path, Path]:
    return (
        (tmp_path / f"{stem}.request.json").resolve(),
        (tmp_path / f"{stem}.result.json").resolve(),
    )


def test_real_spawn_roundtrip_uses_plain_json(
    tmp_path: Path,
) -> None:
    request_path, result_path = _paths(tmp_path)

    result = supervisor.run_supervised_dispatch(
        {"symbol": "EURUSD", "count": 3},
        dispatch_path="test_production_generation_supervisor:_echo_dispatch",
        request_path=request_path,
        result_path=result_path,
        environment=_child_environment(),
    )

    assert result.succeeded
    assert result.safe_to_reconcile
    assert result.payload["echo"] == {"symbol": "EURUSD", "count": 3}
    assert result.payload["pid"] == result.worker_pid
    assert request_path.stat().st_size <= supervisor.MAX_REQUEST_BYTES
    assert result_path.stat().st_size <= supervisor.MAX_RESULT_BYTES
    assert supervisor.read_supervisor_result(result_path) == result


def test_request_protocol_is_canonical_bounded_and_duplicate_safe(
    tmp_path: Path,
) -> None:
    request = supervisor._build_request(  # noqa: SLF001
        request_id="a" * 64,
        dispatch_path="module_name:function_name",
        payload={"b": 2, "a": 1},
    )
    raw = supervisor._canonical_bytes(  # noqa: SLF001
        request,
        maximum=supervisor.MAX_REQUEST_BYTES,
    )
    assert raw.startswith(b'{"content_sha256":')
    assert len(raw) <= supervisor.MAX_REQUEST_BYTES

    duplicate = tmp_path / "duplicate.json"
    duplicate.write_bytes(b'{"a":1,"a":2}')
    with pytest.raises(
        supervisor.ProductionGenerationSupervisorError,
        match="SUPERVISOR_JSON_DUPLICATE_KEY",
    ):
        supervisor._decode_canonical(  # noqa: SLF001
            duplicate.read_bytes(),
            maximum=supervisor.MAX_REQUEST_BYTES,
        )

    with pytest.raises(
        supervisor.ProductionGenerationSupervisorError,
        match="SUPERVISOR_JSON_STRING_UNBOUNDED",
    ):
        supervisor._build_request(  # noqa: SLF001
            request_id="b" * 64,
            dispatch_path="module_name:function_name",
            payload={"value": "x" * (supervisor.MAX_STRING_BYTES + 1)},
        )


def test_exclusive_publication_never_exposes_partial_final_path(
    tmp_path: Path,
    monkeypatch,
) -> None:
    target = (tmp_path / "request.json").resolve()
    raw = b'{"complete":true}'
    observed: list[Path] = []
    original_rename = supervisor.os.rename

    def inspect_then_rename(source, destination) -> None:
        source_path = Path(source)
        destination_path = Path(destination)
        assert destination_path == target
        assert not target.exists()
        assert source_path.read_bytes() == raw
        observed.append(source_path)
        original_rename(source_path, destination_path)

    monkeypatch.setattr(supervisor.os, "rename", inspect_then_rename)

    supervisor._exclusive_write(target, raw)  # noqa: SLF001

    assert len(observed) == 1
    assert target.read_bytes() == raw
    assert not observed[0].exists()


class _FakeStdin:
    def __init__(self, events: list[str], on_close=None) -> None:
        self.events = events
        self.on_close = on_close
        self.buffer = bytearray()

    def write(self, value: bytes) -> int:
        self.events.append("release")
        self.buffer.extend(value)
        return len(value)

    def flush(self) -> None:
        self.events.append("flush")

    def close(self) -> None:
        self.events.append("stdin_close")
        if self.on_close is not None:
            self.on_close()


class _FakeProcess:
    def __init__(
        self,
        *,
        events: list[str],
        pid: int = 4321,
        finish_on_release: bool = True,
    ) -> None:
        self.events = events
        self.pid = pid
        self.exit_code: int | None = None
        self.finish_on_release = finish_on_release
        self.stdin = _FakeStdin(events)

    def poll(self):
        return self.exit_code

    def wait(self, timeout=None):
        self.events.append("wait")
        if self.exit_code is None:
            raise subprocess.TimeoutExpired(["fake"], timeout)
        return self.exit_code

    def terminate(self) -> None:
        self.events.append("direct_terminate")
        self.exit_code = -15

    def kill(self) -> None:
        self.events.append("direct_kill")
        self.exit_code = -9


class _FakeGuard:
    def __init__(
        self,
        events: list[str],
        process: _FakeProcess,
        *,
        active_after_exit: int = 0,
        assignment_error: Exception | None = None,
        terminate_clears: bool = True,
    ) -> None:
        self.events = events
        self.process = process
        self.active = active_after_exit
        self.assignment_error = assignment_error
        self.terminate_clears = terminate_clears

    def assign(self, pid: int) -> None:
        assert pid == self.process.pid
        self.events.append("assign")
        if self.assignment_error is not None:
            raise self.assignment_error

    def terminate(self, exit_code: int) -> None:
        assert exit_code in {0xE0, 0xE1, 0xE2}
        self.events.append("job_terminate")
        self.process.exit_code = -1
        if self.terminate_clears:
            self.active = 0

    def active_processes(self) -> int:
        self.events.append("active_query")
        if self.process.exit_code is None:
            return 1
        return self.active

    def close(self) -> None:
        self.events.append("job_close")


def _install_success_fakes(
    monkeypatch,
    *,
    events: list[str],
    active_after_exit: int = 0,
    terminate_clears: bool = True,
) -> tuple[_FakeProcess, _FakeGuard]:
    process = _FakeProcess(events=events)
    guard = _FakeGuard(
        events,
        process,
        active_after_exit=active_after_exit,
        terminate_clears=terminate_clears,
    )

    def spawn(command, *, environment):
        del environment
        request_path = Path(command[command.index("--request") + 1])
        request_sha = command[command.index("--request-sha256") + 1]
        worker_path = Path(command[command.index("--worker-result") + 1])

        def publish() -> None:
            request = supervisor._read_request(  # noqa: SLF001
                request_path,
                expected_file_sha256=request_sha,
            )
            worker = supervisor._build_worker_result(  # noqa: SLF001
                request_id=request["request_id"],
                request_sha256=request_sha,
                status="SUCCEEDED",
                payload={"verified": True},
                error=None,
                worker_pid=process.pid,
            )
            supervisor._exclusive_write(  # noqa: SLF001
                worker_path,
                supervisor._canonical_bytes(  # noqa: SLF001
                    worker,
                    maximum=supervisor.MAX_WORKER_RESULT_BYTES,
                ),
            )
            process.exit_code = 0

        process.stdin.on_close = publish
        return process

    monkeypatch.setattr(supervisor, "_spawn_worker", spawn)
    monkeypatch.setattr(supervisor, "_create_process_tree_guard", lambda: guard)
    return process, guard


def test_windows_job_assignment_precedes_worker_release(
    tmp_path: Path,
    monkeypatch,
) -> None:
    events: list[str] = []
    _install_success_fakes(monkeypatch, events=events)
    request_path, result_path = _paths(tmp_path, "ordered")

    result = supervisor.run_supervised_dispatch(
        {"generation_id": "g"},
        dispatch_path="module_name:function_name",
        request_path=request_path,
        result_path=result_path,
    )

    assert result.succeeded
    assert events.index("assign") < events.index("release")
    assert events.index("release") < events.index("stdin_close")


def test_job_assignment_failure_never_releases_worker(
    tmp_path: Path,
    monkeypatch,
) -> None:
    events: list[str] = []
    process = _FakeProcess(events=events)
    guard = _FakeGuard(
        events,
        process,
        assignment_error=RuntimeError("nested job denied"),
    )
    monkeypatch.setattr(
        supervisor,
        "_spawn_worker",
        lambda command, *, environment: process,
    )
    monkeypatch.setattr(supervisor, "_create_process_tree_guard", lambda: guard)
    request_path, result_path = _paths(tmp_path, "assign-fail")

    result = supervisor.run_supervised_dispatch(
        {},
        dispatch_path="module_name:function_name",
        request_path=request_path,
        result_path=result_path,
        shutdown_timeout_seconds=0.1,
    )

    assert result.status == "FAILED"
    assert result.safe_to_reconcile
    assert result.error["code"] == "SUPERVISOR_JOB_ASSIGN_FAILED"
    assert "assign" in events
    assert "release" not in events
    assert "direct_terminate" in events


def test_process_wait_polls_until_terminal_without_elapsed_cutoff(
    monkeypatch,
) -> None:
    class _EventuallyDone:
        def __init__(self) -> None:
            self.results = iter((None, None, 0))

        def poll(self):
            return next(self.results)

    sleeps: list[float] = []
    monkeypatch.setattr(supervisor.time, "sleep", sleeps.append)

    exit_code = supervisor._wait_for_process(  # noqa: SLF001
        _EventuallyDone()
    )

    assert exit_code == 0
    assert sleeps == [supervisor.POLL_SECONDS, supervisor.POLL_SECONDS]


def test_operator_interrupt_terminates_tree_and_returns_reconcile_gate(
    tmp_path: Path,
    monkeypatch,
) -> None:
    events: list[str] = []
    process = _FakeProcess(events=events, finish_on_release=False)
    guard = _FakeGuard(events, process)
    monkeypatch.setattr(
        supervisor,
        "_spawn_worker",
        lambda command, *, environment: process,
    )
    monkeypatch.setattr(supervisor, "_create_process_tree_guard", lambda: guard)
    monkeypatch.setattr(
        supervisor,
        "_wait_for_process",
        lambda _process: (_ for _ in ()).throw(KeyboardInterrupt()),
    )
    request_path, result_path = _paths(tmp_path, "interrupted")

    result = supervisor.run_supervised_dispatch(
        {"generation_id": "known-before-spawn"},
        dispatch_path="module_name:function_name",
        request_path=request_path,
        result_path=result_path,
        shutdown_timeout_seconds=0.1,
    )

    assert result.status == "FAILED"
    assert result.safe_to_reconcile
    assert result.error["code"] == "SUPERVISOR_RUNTIME_FAILED"
    assert result.error["type"] == "KeyboardInterrupt"
    assert events.index("assign") < events.index("release")
    assert "job_terminate" in events
    assert events.index("job_terminate") < events.index("job_close")


def test_unproved_tree_shutdown_blocks_reconciliation(
    tmp_path: Path,
    monkeypatch,
) -> None:
    events: list[str] = []
    _process, _guard = _install_success_fakes(
        monkeypatch,
        events=events,
        active_after_exit=2,
        terminate_clears=False,
    )
    request_path, result_path = _paths(tmp_path, "termination-fail")

    result = supervisor.run_supervised_dispatch(
        {},
        dispatch_path="module_name:function_name",
        request_path=request_path,
        result_path=result_path,
        shutdown_timeout_seconds=0.05,
    )

    assert result.status == "TERMINATION_FAILED"
    assert not result.safe_to_reconcile
    assert result.error["code"] == "SUPERVISOR_DESCENDANTS_REMAINED"


def test_worker_protocol_tamper_is_not_returned_as_success(
    tmp_path: Path,
    monkeypatch,
) -> None:
    events: list[str] = []
    process, _guard = _install_success_fakes(monkeypatch, events=events)
    original_close = process.stdin.close

    def tampered_close() -> None:
        original_close()
        worker_paths = list(tmp_path.glob(".*.worker-*.json"))
        assert len(worker_paths) == 1
        raw = json.loads(worker_paths[0].read_text(encoding="ascii"))
        raw["payload"] = {"tampered": True}
        worker_paths[0].write_text(
            json.dumps(raw, sort_keys=True, separators=(",", ":")),
            encoding="ascii",
        )

    process.stdin.close = tampered_close
    request_path, result_path = _paths(tmp_path, "tamper")

    result = supervisor.run_supervised_dispatch(
        {},
        dispatch_path="module_name:function_name",
        request_path=request_path,
        result_path=result_path,
    )

    assert result.status == "CHILD_PROTOCOL_FAILED"
    assert result.safe_to_reconcile
    assert result.error["code"] == "SUPERVISOR_WORKER_RESULT_INVALID"


@pytest.mark.skipif(os.name != "nt", reason="Windows Job Object descendant proof")
def test_windows_job_kills_real_lingering_worker_descendant(
    tmp_path: Path,
) -> None:
    request_path, result_path = _paths(tmp_path, "real-tree")
    pid_path = (tmp_path / "grandchild.pid").resolve()

    result = supervisor.run_supervised_dispatch(
        {"pid_path": str(pid_path)},
        dispatch_path=(
            "test_production_generation_supervisor:"
            "_spawn_lingering_descendant_dispatch"
        ),
        request_path=request_path,
        result_path=result_path,
        shutdown_timeout_seconds=5,
        environment=_child_environment(),
    )

    assert result.status == "FAILED"
    assert result.safe_to_reconcile
    assert result.error["code"] == "SUPERVISOR_DESCENDANTS_REMAINED"
    grandchild_pid = int(pid_path.read_text(encoding="ascii"))
    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    kernel32.OpenProcess.argtypes = [
        ctypes.c_uint32,
        ctypes.c_int,
        ctypes.c_uint32,
    ]
    kernel32.OpenProcess.restype = ctypes.c_void_p
    kernel32.WaitForSingleObject.argtypes = [ctypes.c_void_p, ctypes.c_uint32]
    kernel32.WaitForSingleObject.restype = ctypes.c_uint32
    kernel32.CloseHandle.argtypes = [ctypes.c_void_p]
    synchronize = 0x00100000
    handle = kernel32.OpenProcess(synchronize, False, grandchild_pid)
    if handle:
        try:
            assert kernel32.WaitForSingleObject(handle, 0) == 0
        finally:
            kernel32.CloseHandle(handle)
