from __future__ import annotations

import errno
import json
from pathlib import Path

import pytest

import pm_authority_spool_quota as quota


class _FaultingStream:
    def __init__(self, stream, step: str, error: OSError) -> None:
        self.stream = stream
        self.step = step
        self.error = error

    def __enter__(self):
        return self

    def __exit__(self, error_type, error, traceback):
        self.stream.close()
        if self.step == "close" and error is None:
            raise self.error
        return False

    def write(self, value):
        if self.step == "write":
            raise self.error
        return self.stream.write(value)

    def flush(self):
        if self.step == "flush":
            raise self.error
        return self.stream.flush()

    def fileno(self):
        return self.stream.fileno()


@pytest.mark.parametrize("step", ["open", "write", "flush", "fsync", "close", "replace"])
def test_state_write_reports_os_step_without_sensitive_details(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    step: str,
) -> None:
    root = tmp_path / "spool"
    root.mkdir()
    target = root / quota.QUOTA_STATE_NAME
    prior = b"synthetic-prior-state"
    target.write_bytes(prior)
    value = quota._state(
        max_generation_bytes=100,
        min_free_bytes=0,
        committed_bytes=0,
    )
    failure = OSError(errno.ENOSPC, "synthetic-private-message", "private-path")
    failure.winerror = 1455
    real_open = Path.open
    real_fsync = quota.os.fsync
    real_replace = quota.os.replace

    def open_with_fault(path: Path, *args, **kwargs):
        stream_is_temporary = (
            path.name.startswith(quota.QUOTA_TEMP_PREFIX) and path.suffix == ".tmp"
        )
        if stream_is_temporary and step == "open":
            raise failure
        stream = real_open(path, *args, **kwargs)
        if stream_is_temporary:
            return _FaultingStream(stream, step, failure)
        return stream

    def fsync_with_fault(descriptor):
        if step == "fsync":
            raise failure
        return real_fsync(descriptor)

    def replace_with_fault(source, destination):
        if step == "replace":
            raise failure
        return real_replace(source, destination)

    monkeypatch.setattr(Path, "open", open_with_fault)
    monkeypatch.setattr(quota.os, "fsync", fsync_with_fault)
    monkeypatch.setattr(quota.os, "replace", replace_with_fault)

    with pytest.raises(quota.AuthoritySpoolQuotaError) as captured:
        quota._write_state(root, value)

    error = captured.value
    assert error.code == "SPOOL_QUOTA_STATE_WRITE_FAILED"
    assert error.details[0] == "OSError"
    assert str(error).startswith("SPOOL_QUOTA_STATE_WRITE_FAILED:OSError|")
    assert error.__cause__ is failure
    assert error.diagnostics == {
        "step": step,
        "exception_type": "OSError",
        "errno": errno.ENOSPC,
        "winerror": 1455,
    }
    assert f"step={step}|errno={errno.ENOSPC}|winerror=1455" in str(error)
    serialized = json.dumps(error.diagnostics)
    assert "synthetic-private-message" not in str(error) + serialized
    assert "private-path" not in str(error) + serialized
    assert target.read_bytes() == prior
    assert not list(root.glob(f"{quota.QUOTA_TEMP_PREFIX}*.tmp"))


def test_exhausted_replace_keeps_legacy_prefix_and_original_permission_cause(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "spool"
    root.mkdir()
    value = quota._state(
        max_generation_bytes=100,
        min_free_bytes=0,
        committed_bytes=0,
    )
    failure = PermissionError(errno.EACCES, "synthetic-private-message")
    failure.winerror = 32
    attempts = []
    sleeps = []

    def replace_with_denial(source, target):
        attempts.append((source, target))
        raise failure

    monkeypatch.setattr(quota.os, "replace", replace_with_denial)
    monkeypatch.setattr(quota.time, "sleep", sleeps.append)

    with pytest.raises(
        quota.AuthoritySpoolQuotaError,
        match="SPOOL_QUOTA_STATE_WRITE_FAILED:PermissionError",
    ) as captured:
        quota._write_state(root, value)

    error = captured.value
    assert error.__cause__ is failure
    assert error.diagnostics["step"] == "replace"
    assert error.diagnostics["errno"] == errno.EACCES
    assert error.diagnostics["winerror"] == 32
    assert len(attempts) == quota.STATE_REPLACE_ATTEMPTS
    assert len(sleeps) == quota.STATE_REPLACE_ATTEMPTS - 1
    assert not list(root.glob(f"{quota.QUOTA_TEMP_PREFIX}*.tmp"))


def test_close_error_reports_close_even_after_an_earlier_write_error(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    value = quota._state(
        max_generation_bytes=100,
        min_free_bytes=0,
        committed_bytes=0,
    )
    write_failure = OSError(errno.ENOSPC, "synthetic-write-failure")
    close_failure = OSError(errno.EIO, "synthetic-close-failure")
    real_open = Path.open

    class DoubleFaultStream(_FaultingStream):
        def __exit__(self, error_type, error, traceback):
            self.stream.close()
            raise close_failure

    def open_with_fault(path: Path, *args, **kwargs):
        stream = real_open(path, *args, **kwargs)
        if path.name.startswith(quota.QUOTA_TEMP_PREFIX) and path.suffix == ".tmp":
            return DoubleFaultStream(stream, "write", write_failure)
        return stream

    monkeypatch.setattr(Path, "open", open_with_fault)
    with pytest.raises(quota.AuthoritySpoolQuotaError) as captured:
        quota._write_state(tmp_path, value)

    assert captured.value.__cause__ is close_failure
    assert captured.value.diagnostics["step"] == "close"
    assert captured.value.diagnostics["errno"] == errno.EIO
    assert not list(tmp_path.glob(f"{quota.QUOTA_TEMP_PREFIX}*.tmp"))


def test_guard_write_sibling_preserves_cause_and_reports_fsync(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    failure = OSError(errno.EIO, "synthetic-private-message", "private-path")

    def failed_fsync(_descriptor):
        raise failure

    monkeypatch.setattr(quota.os, "fsync", failed_fsync)
    token = "1" * 32
    with pytest.raises(quota.AuthoritySpoolQuotaError) as captured:
        quota._ReservationGuard.create(tmp_path, token)

    error = captured.value
    assert error.code == "SPOOL_QUOTA_RESERVATION_GUARD_CREATE_FAILED"
    assert error.__cause__ is failure
    assert error.diagnostics == {
        "step": "guard_fsync",
        "exception_type": "OSError",
        "errno": errno.EIO,
        "winerror": None,
    }
    assert "synthetic-private-message" not in str(error)
    assert "private-path" not in str(error)
    assert not quota._guard_path(tmp_path, token).exists()


def test_state_read_sibling_reports_read_step_and_preserves_prior_bytes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    value = quota._state(
        max_generation_bytes=100,
        min_free_bytes=0,
        committed_bytes=0,
    )
    quota._write_state(tmp_path, value)
    target = tmp_path / quota.QUOTA_STATE_NAME
    prior = target.read_bytes()
    failure = OSError(errno.EIO, "synthetic-private-message", "private-path")
    real_read = Path.read_bytes

    def failed_read(path: Path):
        if path == target:
            raise failure
        return real_read(path)

    monkeypatch.setattr(Path, "read_bytes", failed_read)
    with pytest.raises(quota.AuthoritySpoolQuotaError) as captured:
        quota._read_state(tmp_path)

    error = captured.value
    assert error.code == "SPOOL_QUOTA_STATE_READ_FAILED"
    assert error.__cause__ is failure
    assert error.diagnostics["step"] == "read"
    assert error.diagnostics["errno"] == errno.EIO
    assert real_read(target) == prior


def test_nonnumeric_or_oversized_os_codes_are_not_serialized() -> None:
    failure = OSError("synthetic-private-message")
    failure.errno = "private-errno-value"
    failure.winerror = 2**100

    error = quota._io_error(
        "SPOOL_QUOTA_STATE_WRITE_FAILED", failure, step="replace"
    )

    assert error.diagnostics["errno"] is None
    assert error.diagnostics["winerror"] is None
    assert "private-errno-value" not in str(error) + json.dumps(error.diagnostics)
    assert str(2**100) not in str(error)


def test_semantic_quota_errors_keep_their_existing_message() -> None:
    error = quota.AuthoritySpoolQuotaError("SPOOL_QUOTA_LEASE_ALREADY_FINISHED")

    assert str(error) == "SPOOL_QUOTA_LEASE_ALREADY_FINISHED"
    assert error.details == ()
    assert error.diagnostics == {}


def test_memory_failure_retains_existing_passthrough_behavior(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    value = quota._state(
        max_generation_bytes=100,
        min_free_bytes=0,
        committed_bytes=0,
    )
    failure = MemoryError("synthetic-memory-failure")

    def failed_fsync(_descriptor):
        raise failure

    monkeypatch.setattr(quota.os, "fsync", failed_fsync)
    with pytest.raises(MemoryError) as captured:
        quota._write_state(tmp_path, value)

    assert captured.value is failure
    assert not list(tmp_path.glob(f"{quota.QUOTA_TEMP_PREFIX}*.tmp"))
