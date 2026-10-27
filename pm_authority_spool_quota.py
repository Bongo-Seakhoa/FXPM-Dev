"""Cross-process aggregate byte governance for one authority spool root.

The quota is operational routing state, not evidence.  Writers durably reserve
bytes under a short root lock, then compute and stage outside that lock.  A
kernel-held per-reservation guard distinguishes live writers from crashed
writers without trusting reusable process identifiers.  Once no writer is
live, abandoned reservations are reconciled by one exact inode-deduplicated
rescan before more bytes are admitted.
"""

from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
import hashlib
import json
import os
from pathlib import Path
import shutil
import stat
import threading
import time
from typing import Any, BinaryIO, Final, Iterator, Optional
import uuid

from pm_storage import _exclusive_file_lock


QUOTA_STATE_SCHEMA: Final = "fxpm.authority-spool-quota-state.v2"
QUOTA_STATE_NAME: Final = ".generation-quota.json"
QUOTA_LOCK_NAME: Final = ".generation-quota.lock"
QUOTA_TEMP_PREFIX: Final = ".generation-quota."
QUOTA_RESERVATION_PREFIX: Final = ".generation-quota.reservation."
MAX_STATE_BYTES: Final = 32 * 1024
MAX_RESCAN_ENTRIES: Final = 1_000_000
STATE_REPLACE_ATTEMPTS: Final = 6
STATE_REPLACE_INITIAL_DELAY_SECONDS: Final = 0.05

_STATE_FIELDS: Final = frozenset(
    {
        "schema_version",
        "status",
        "max_generation_bytes",
        "min_free_bytes",
        "committed_bytes",
        "reservations",
        "content_sha256",
    }
)
_RESERVATION_FIELDS: Final = frozenset(
    {
        "token",
        "owner_pid",
        "reserved_bytes",
        "materialized_bytes",
    }
)
_LIVE_GUARDS: set[str] = set()
_LIVE_GUARDS_LOCK = threading.Lock()


class AuthoritySpoolQuotaError(RuntimeError):
    """Typed operational storage failure."""

    def __init__(self, code: str, *details: Any) -> None:
        self.code = str(code)
        self.details = tuple(str(value)[:512] for value in details[:12])
        suffix = ":" + "|".join(self.details) if self.details else ""
        super().__init__(self.code + suffix)


def _fail(code: str, *details: Any) -> None:
    raise AuthoritySpoolQuotaError(code, *details)


def _bound(value: Any, code: str, *, allow_zero: bool) -> int:
    if (
        type(value) is not int
        or type(value) is bool
        or value < (0 if allow_zero else 1)
    ):
        _fail(code)
    return int(value)


def _is_reparse(path: Path) -> bool:
    try:
        info = path.lstat()
    except OSError:
        return True
    attributes = int(getattr(info, "st_file_attributes", 0) or 0)
    marker = int(getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0) or 0)
    return path.is_symlink() or bool(marker and attributes & marker)


def _root(path: Path) -> Path:
    target = Path(os.path.abspath(path))
    try:
        info = target.lstat()
    except OSError as exc:
        raise AuthoritySpoolQuotaError(
            "SPOOL_QUOTA_ROOT_UNAVAILABLE",
            type(exc).__name__,
            str(target),
        ) from exc
    if _is_reparse(target) or not stat.S_ISDIR(info.st_mode):
        _fail("SPOOL_QUOTA_ROOT_INVALID", target)
    return target


def _canonical_without_digest(value: dict[str, Any]) -> bytes:
    body = dict(value)
    body.pop("content_sha256", None)
    try:
        return json.dumps(
            body,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
            allow_nan=False,
        ).encode("ascii")
    except (TypeError, ValueError, UnicodeError) as exc:
        raise AuthoritySpoolQuotaError(
            "SPOOL_QUOTA_STATE_CANONICAL_INVALID",
            type(exc).__name__,
        ) from exc


def _reservation(value: Any) -> dict[str, Any]:
    if type(value) is not dict or set(value) != _RESERVATION_FIELDS:
        _fail("SPOOL_QUOTA_RESERVATION_INVALID")
    token = value.get("token")
    if (
        type(token) is not str
        or len(token) != 32
        or any(character not in "0123456789abcdef" for character in token)
    ):
        _fail("SPOOL_QUOTA_RESERVATION_TOKEN_INVALID")
    owner_pid = _bound(
        value.get("owner_pid"),
        "SPOOL_QUOTA_RESERVATION_OWNER_INVALID",
        allow_zero=False,
    )
    reserved = _bound(
        value.get("reserved_bytes"),
        "SPOOL_QUOTA_RESERVED_BYTES_INVALID",
        allow_zero=True,
    )
    materialized = _bound(
        value.get("materialized_bytes"),
        "SPOOL_QUOTA_MATERIALIZED_BYTES_INVALID",
        allow_zero=True,
    )
    if materialized > reserved:
        _fail("SPOOL_QUOTA_MATERIALIZED_EXCEEDS_RESERVED")
    return {
        "token": token,
        "owner_pid": owner_pid,
        "reserved_bytes": reserved,
        "materialized_bytes": materialized,
    }


def _state(
    *,
    max_generation_bytes: int,
    min_free_bytes: int,
    committed_bytes: int,
    reservations: Optional[list[dict[str, Any]]] = None,
) -> dict[str, Any]:
    maximum = _bound(
        max_generation_bytes,
        "SPOOL_QUOTA_MAX_BYTES_INVALID",
        allow_zero=False,
    )
    committed = _bound(
        committed_bytes,
        "SPOOL_QUOTA_COMMITTED_BYTES_INVALID",
        allow_zero=True,
    )
    records = [_reservation(value) for value in (reservations or [])]
    records.sort(key=lambda value: value["token"])
    if len({value["token"] for value in records}) != len(records):
        _fail("SPOOL_QUOTA_RESERVATION_DUPLICATE")
    projected = committed + sum(
        int(value["reserved_bytes"]) for value in records
    )
    if projected > maximum:
        _fail(
            "SPOOL_QUOTA_GENERATION_BYTES_EXCEEDED",
            projected,
            maximum,
        )
    result: dict[str, Any] = {
        "schema_version": QUOTA_STATE_SCHEMA,
        "status": "DIRTY" if records else "CLEAN",
        "max_generation_bytes": maximum,
        "min_free_bytes": _bound(
            min_free_bytes,
            "SPOOL_QUOTA_MIN_FREE_INVALID",
            allow_zero=True,
        ),
        "committed_bytes": committed,
        "reservations": records,
    }
    result["content_sha256"] = hashlib.sha256(
        _canonical_without_digest(result)
    ).hexdigest()
    encoded = json.dumps(
        result,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("ascii")
    if not 0 < len(encoded) <= MAX_STATE_BYTES:
        _fail("SPOOL_QUOTA_STATE_SIZE_INVALID")
    return result


def _write_state(root: Path, value: dict[str, Any]) -> None:
    encoded = json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("ascii")
    target = root / QUOTA_STATE_NAME
    temporary = root / f"{QUOTA_TEMP_PREFIX}{os.getpid()}.{uuid.uuid4().hex}.tmp"
    try:
        with temporary.open("xb") as stream:
            stream.write(encoded)
            stream.flush()
            os.fsync(stream.fileno())
        for attempt in range(STATE_REPLACE_ATTEMPTS):
            try:
                os.replace(temporary, target)
                break
            except PermissionError:
                if attempt + 1 >= STATE_REPLACE_ATTEMPTS:
                    raise
                time.sleep(
                    STATE_REPLACE_INITIAL_DELAY_SECONDS * (2**attempt)
                )
    except OSError as exc:
        raise AuthoritySpoolQuotaError(
            "SPOOL_QUOTA_STATE_WRITE_FAILED",
            type(exc).__name__,
        ) from exc
    finally:
        try:
            temporary.unlink(missing_ok=True)
        except OSError:
            pass


def _read_state(root: Path) -> Optional[dict[str, Any]]:
    path = root / QUOTA_STATE_NAME
    if not path.exists():
        return None
    try:
        before = path.lstat()
        if (
            _is_reparse(path)
            or not stat.S_ISREG(before.st_mode)
            or not 0 < before.st_size <= MAX_STATE_BYTES
        ):
            _fail("SPOOL_QUOTA_STATE_FILE_INVALID")
        raw = path.read_bytes()
        after = path.lstat()
        value = json.loads(
            raw.decode("ascii"),
            object_pairs_hook=_unique_pairs,
            parse_constant=_reject_constant,
        )
    except AuthoritySpoolQuotaError:
        raise
    except (OSError, UnicodeError, ValueError) as exc:
        raise AuthoritySpoolQuotaError(
            "SPOOL_QUOTA_STATE_READ_FAILED",
            type(exc).__name__,
        ) from exc
    if (
        before.st_dev != after.st_dev
        or before.st_ino != after.st_ino
        or before.st_size != after.st_size
        or before.st_mtime_ns != after.st_mtime_ns
        or len(raw) != before.st_size
        or type(value) is not dict
        or set(value) != _STATE_FIELDS
        or value.get("schema_version") != QUOTA_STATE_SCHEMA
        or value.get("status") not in {"CLEAN", "DIRTY"}
    ):
        _fail("SPOOL_QUOTA_STATE_INVALID")
    rebuilt = _state(
        max_generation_bytes=_bound(
            value.get("max_generation_bytes"),
            "SPOOL_QUOTA_MAX_BYTES_INVALID",
            allow_zero=False,
        ),
        min_free_bytes=_bound(
            value.get("min_free_bytes"),
            "SPOOL_QUOTA_MIN_FREE_INVALID",
            allow_zero=True,
        ),
        committed_bytes=_bound(
            value.get("committed_bytes"),
            "SPOOL_QUOTA_COMMITTED_BYTES_INVALID",
            allow_zero=True,
        ),
        reservations=value.get("reservations"),
    )
    if rebuilt != value:
        _fail("SPOOL_QUOTA_STATE_DIGEST_INVALID")
    return rebuilt


def _unique_pairs(values: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in values:
        if key in result:
            _fail("SPOOL_QUOTA_STATE_DUPLICATE_KEY", key)
        result[key] = value
    return result


def _reject_constant(value: str) -> None:
    _fail("SPOOL_QUOTA_STATE_NONFINITE", value)


def _excluded_root_metadata(path: Path, root: Path) -> bool:
    return path.parent == root and (
        path.name in {QUOTA_STATE_NAME, QUOTA_LOCK_NAME}
        or path.name.startswith(QUOTA_TEMP_PREFIX)
    )


def _scan_committed_bytes(root: Path) -> int:
    """Count exact unique regular-file inodes below one non-reparse root."""

    total = 0
    entry_count = 0
    inodes: set[tuple[int, int]] = set()
    stack = [root]
    while stack:
        directory = stack.pop()
        try:
            entries = os.scandir(directory)
        except OSError as exc:
            raise AuthoritySpoolQuotaError(
                "SPOOL_QUOTA_RESCAN_FAILED",
                type(exc).__name__,
                str(directory),
            ) from exc
        try:
            with entries:
                for entry in entries:
                    entry_count += 1
                    if entry_count > MAX_RESCAN_ENTRIES:
                        _fail("SPOOL_QUOTA_RESCAN_ENTRY_BOUND_EXCEEDED")
                    path = Path(entry.path)
                    if _excluded_root_metadata(path, root):
                        continue
                    try:
                        info = entry.stat(follow_symlinks=False)
                    except OSError as exc:
                        raise AuthoritySpoolQuotaError(
                            "SPOOL_QUOTA_RESCAN_FAILED",
                            type(exc).__name__,
                            str(path),
                        ) from exc
                    if entry.is_symlink() or _is_reparse(path):
                        _fail("SPOOL_QUOTA_REPARSE_ENTRY_FORBIDDEN", path)
                    if stat.S_ISDIR(info.st_mode):
                        stack.append(path)
                        continue
                    if not stat.S_ISREG(info.st_mode):
                        _fail("SPOOL_QUOTA_NONREGULAR_ENTRY_FORBIDDEN", path)
                    try:
                        file_info = os.stat(path, follow_symlinks=False)
                    except OSError as exc:
                        raise AuthoritySpoolQuotaError(
                            "SPOOL_QUOTA_RESCAN_FAILED",
                            type(exc).__name__,
                            str(path),
                        ) from exc
                    reparse_marker = int(
                        getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0) or 0
                    )
                    file_attributes = int(
                        getattr(file_info, "st_file_attributes", 0) or 0
                    )
                    if not stat.S_ISREG(file_info.st_mode) or (
                        reparse_marker and file_attributes & reparse_marker
                    ):
                        _fail("SPOOL_QUOTA_RESCAN_FAILED", "FILE_TYPE_CHANGED", path)
                    inode = (int(file_info.st_dev), int(file_info.st_ino))
                    if inode == (0, 0):
                        _fail("SPOOL_QUOTA_RESCAN_FAILED", "FILE_IDENTITY_UNAVAILABLE", path)
                    if inode not in inodes:
                        inodes.add(inode)
                        total += int(file_info.st_size)
        except AuthoritySpoolQuotaError:
            raise
        except OSError as exc:
            raise AuthoritySpoolQuotaError(
                "SPOOL_QUOTA_RESCAN_FAILED",
                type(exc).__name__,
                str(directory),
            ) from exc
    return total


def _guard_path(root: Path, token: str) -> Path:
    return root / f"{QUOTA_RESERVATION_PREFIX}{token}.lock"


def _try_lock_guard(handle: BinaryIO) -> bool:
    handle.seek(0)
    if os.name == "nt":
        import msvcrt

        try:
            msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
        except OSError:
            return False
        return True
    import fcntl

    try:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        return False
    return True


def _unlock_guard(handle: BinaryIO) -> None:
    handle.seek(0)
    if os.name == "nt":
        import msvcrt

        msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
        return
    import fcntl

    fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


@dataclass
class _ReservationGuard:
    root: Path
    token: str
    handle: BinaryIO
    closed: bool = False

    @classmethod
    def create(cls, root: Path, token: str) -> "_ReservationGuard":
        path = _guard_path(root, token)
        try:
            handle = path.open("x+b")
            handle.write(b"\0")
            handle.flush()
            os.fsync(handle.fileno())
            os.set_inheritable(handle.fileno(), False)
            if not _try_lock_guard(handle):
                _fail("SPOOL_QUOTA_RESERVATION_GUARD_CONFLICT")
        except AuthoritySpoolQuotaError:
            try:
                handle.close()
            except (OSError, UnboundLocalError):
                pass
            try:
                path.unlink(missing_ok=True)
            except OSError:
                pass
            raise
        except OSError as exc:
            try:
                handle.close()
            except (OSError, UnboundLocalError):
                pass
            try:
                path.unlink(missing_ok=True)
            except OSError:
                pass
            raise AuthoritySpoolQuotaError(
                "SPOOL_QUOTA_RESERVATION_GUARD_CREATE_FAILED",
                type(exc).__name__,
            ) from exc
        with _LIVE_GUARDS_LOCK:
            _LIVE_GUARDS.add(token)
        return cls(root=root, token=token, handle=handle)

    def close(self, *, remove: bool) -> None:
        if self.closed:
            return
        with _LIVE_GUARDS_LOCK:
            _LIVE_GUARDS.discard(self.token)
        try:
            _unlock_guard(self.handle)
        except OSError:
            pass
        try:
            self.handle.close()
        finally:
            self.closed = True
        if remove:
            try:
                _guard_path(self.root, self.token).unlink(missing_ok=True)
            except OSError:
                pass


def _guard_is_live(root: Path, token: str) -> bool:
    with _LIVE_GUARDS_LOCK:
        if token in _LIVE_GUARDS:
            return True
    path = _guard_path(root, token)
    if not path.exists():
        return False
    try:
        before = path.lstat()
        if (
            _is_reparse(path)
            or not stat.S_ISREG(before.st_mode)
            or before.st_size != 1
        ):
            _fail("SPOOL_QUOTA_RESERVATION_GUARD_INVALID", token)
        with path.open("r+b") as handle:
            os.set_inheritable(handle.fileno(), False)
            acquired = _try_lock_guard(handle)
            if acquired:
                _unlock_guard(handle)
                return False
            return True
    except AuthoritySpoolQuotaError:
        raise
    except OSError as exc:
        raise AuthoritySpoolQuotaError(
            "SPOOL_QUOTA_RESERVATION_GUARD_READ_FAILED",
            type(exc).__name__,
            token,
        ) from exc


def _disk_free(root: Path) -> int:
    try:
        return int(shutil.disk_usage(root).free)
    except OSError as exc:
        raise AuthoritySpoolQuotaError(
            "SPOOL_QUOTA_FREE_SPACE_UNAVAILABLE",
            type(exc).__name__,
        ) from exc


def _load_state_locked(
    root: Path,
    *,
    maximum: int,
    reserve: int,
) -> dict[str, Any]:
    current = _read_state(root)
    if current is None:
        committed = _scan_committed_bytes(root)
        current = _state(
            max_generation_bytes=maximum,
            min_free_bytes=reserve,
            committed_bytes=committed,
        )
    elif (
        current["max_generation_bytes"] != maximum
        or current["min_free_bytes"] != reserve
    ):
        _fail("SPOOL_QUOTA_LIMITS_CHANGED")
    records = list(current["reservations"])
    if records and not any(
        _guard_is_live(root, str(record["token"])) for record in records
    ):
        committed = _scan_committed_bytes(root)
        if committed > maximum:
            _fail(
                "SPOOL_QUOTA_GENERATION_BYTES_EXCEEDED",
                committed,
                maximum,
            )
        current = _state(
            max_generation_bytes=maximum,
            min_free_bytes=reserve,
            committed_bytes=committed,
        )
        _write_state(root, current)
        for record in records:
            try:
                _guard_path(root, str(record["token"])).unlink(
                    missing_ok=True
                )
            except OSError:
                pass
    if int(current["committed_bytes"]) > maximum:
        _fail(
            "SPOOL_QUOTA_GENERATION_BYTES_EXCEEDED",
            current["committed_bytes"],
            maximum,
        )
    return current


def _record_for(
    state: dict[str, Any],
    token: str,
) -> tuple[int, dict[str, Any]]:
    for index, record in enumerate(state["reservations"]):
        if record["token"] == token:
            return index, dict(record)
    _fail("SPOOL_QUOTA_RESERVATION_NOT_ACTIVE", token)
    raise AssertionError("unreachable")


def _unmaterialized(records: list[dict[str, Any]]) -> int:
    return sum(
        int(record["reserved_bytes"]) - int(record["materialized_bytes"])
        for record in records
    )


@dataclass
class PublicationLease:
    """One durable publication reservation; no root lock is held by callers."""

    _root: Path
    _max_generation_bytes: int
    _min_free_bytes: int
    _committed_bytes: int
    _token: str
    _guard: _ReservationGuard
    _in_progress_bytes: int = 0
    _materialized_bytes: int = 0
    _finished: bool = False

    @property
    def committed_bytes(self) -> int:
        return self._committed_bytes

    @property
    def in_progress_bytes(self) -> int:
        return self._in_progress_bytes

    @property
    def token(self) -> str:
        return self._token

    def _assert_open(self) -> None:
        if self._finished:
            _fail("SPOOL_QUOTA_LEASE_ALREADY_FINISHED")

    def consume(self, byte_count: int) -> None:
        """Durably reserve bytes before writing them to any transient file."""

        self._assert_open()
        count = _bound(
            byte_count,
            "SPOOL_QUOTA_CONSUME_BYTES_INVALID",
            allow_zero=True,
        )
        with _exclusive_file_lock(self._root / QUOTA_LOCK_NAME):
            current = _load_state_locked(
                self._root,
                maximum=self._max_generation_bytes,
                reserve=self._min_free_bytes,
            )
            index, record = _record_for(current, self._token)
            records = [dict(value) for value in current["reservations"]]
            projected = (
                int(current["committed_bytes"])
                + sum(int(value["reserved_bytes"]) for value in records)
                + count
            )
            if projected > self._max_generation_bytes:
                _fail(
                    "SPOOL_QUOTA_GENERATION_BYTES_EXCEEDED",
                    projected,
                    self._max_generation_bytes,
                )
            free_bytes = _disk_free(self._root)
            promised = _unmaterialized(records) + count
            if free_bytes - promised < self._min_free_bytes:
                _fail(
                    "SPOOL_QUOTA_FREE_SPACE_RESERVE_BREACHED",
                    free_bytes,
                    promised,
                    self._min_free_bytes,
                )
            if count:
                record["reserved_bytes"] = int(record["reserved_bytes"]) + count
                records[index] = record
                _write_state(
                    self._root,
                    _state(
                        max_generation_bytes=self._max_generation_bytes,
                        min_free_bytes=self._min_free_bytes,
                        committed_bytes=int(current["committed_bytes"]),
                        reservations=records,
                    ),
                )
            self._committed_bytes = int(current["committed_bytes"])
            self._in_progress_bytes = int(record["reserved_bytes"])
            self._materialized_bytes = int(record["materialized_bytes"])

    def materialize(self, byte_count: int) -> None:
        """Record bytes written after a successful physical write."""

        self._assert_open()
        count = _bound(
            byte_count,
            "SPOOL_QUOTA_MATERIALIZE_BYTES_INVALID",
            allow_zero=True,
        )
        if count == 0:
            return
        with _exclusive_file_lock(self._root / QUOTA_LOCK_NAME):
            current = _load_state_locked(
                self._root,
                maximum=self._max_generation_bytes,
                reserve=self._min_free_bytes,
            )
            index, record = _record_for(current, self._token)
            materialized = int(record["materialized_bytes"]) + count
            if materialized > int(record["reserved_bytes"]):
                _fail("SPOOL_QUOTA_MATERIALIZED_EXCEEDS_RESERVED")
            records = [dict(value) for value in current["reservations"]]
            record["materialized_bytes"] = materialized
            records[index] = record
            _write_state(
                self._root,
                _state(
                    max_generation_bytes=self._max_generation_bytes,
                    min_free_bytes=self._min_free_bytes,
                    committed_bytes=int(current["committed_bytes"]),
                    reservations=records,
                ),
            )
            self._committed_bytes = int(current["committed_bytes"])
            self._in_progress_bytes = int(record["reserved_bytes"])
            self._materialized_bytes = materialized

    def cancel_after_cleanup(self) -> None:
        """Release a reservation only after all of its physical bytes are gone."""

        self._assert_open()
        with _exclusive_file_lock(self._root / QUOTA_LOCK_NAME):
            current = _load_state_locked(
                self._root,
                maximum=self._max_generation_bytes,
                reserve=self._min_free_bytes,
            )
            index, _record = _record_for(current, self._token)
            records = [dict(value) for value in current["reservations"]]
            del records[index]
            _write_state(
                self._root,
                _state(
                    max_generation_bytes=self._max_generation_bytes,
                    min_free_bytes=self._min_free_bytes,
                    committed_bytes=int(current["committed_bytes"]),
                    reservations=records,
                ),
            )
            self._committed_bytes = int(current["committed_bytes"])
            self._in_progress_bytes = 0
            self._materialized_bytes = 0
            self._finished = True

    def commit(self, new_unique_bytes: int) -> None:
        self._assert_open()
        unique = _bound(
            new_unique_bytes,
            "SPOOL_QUOTA_COMMIT_BYTES_INVALID",
            allow_zero=True,
        )
        with _exclusive_file_lock(self._root / QUOTA_LOCK_NAME):
            current = _load_state_locked(
                self._root,
                maximum=self._max_generation_bytes,
                reserve=self._min_free_bytes,
            )
            index, record = _record_for(current, self._token)
            if unique > int(record["reserved_bytes"]):
                _fail("SPOOL_QUOTA_COMMIT_EXCEEDS_CONSUMED")
            records = [dict(value) for value in current["reservations"]]
            del records[index]
            committed = int(current["committed_bytes"]) + unique
            projected = committed + sum(
                int(value["reserved_bytes"]) for value in records
            )
            if projected > self._max_generation_bytes:
                _fail(
                    "SPOOL_QUOTA_GENERATION_BYTES_EXCEEDED",
                    projected,
                    self._max_generation_bytes,
                )
            free_bytes = _disk_free(self._root)
            promised = _unmaterialized(records)
            if free_bytes - promised < self._min_free_bytes:
                _fail(
                    "SPOOL_QUOTA_FREE_SPACE_RESERVE_BREACHED",
                    free_bytes,
                    promised,
                    self._min_free_bytes,
                )
            _write_state(
                self._root,
                _state(
                    max_generation_bytes=self._max_generation_bytes,
                    min_free_bytes=self._min_free_bytes,
                    committed_bytes=committed,
                    reservations=records,
                ),
            )
            self._committed_bytes = committed
            self._in_progress_bytes = 0
            self._materialized_bytes = 0
            self._finished = True


@contextmanager
def publication_lease(
    root: Path,
    *,
    max_generation_bytes: int,
    min_free_bytes: int,
) -> Iterator[PublicationLease]:
    """Create one durable reservation while holding only short state locks."""

    safe_root = _root(Path(root))
    maximum = _bound(
        max_generation_bytes,
        "SPOOL_QUOTA_MAX_BYTES_INVALID",
        allow_zero=False,
    )
    reserve = _bound(
        min_free_bytes,
        "SPOOL_QUOTA_MIN_FREE_INVALID",
        allow_zero=True,
    )
    token = uuid.uuid4().hex
    guard = _ReservationGuard.create(safe_root, token)
    lease: Optional[PublicationLease] = None
    state_recorded = False
    try:
        with _exclusive_file_lock(safe_root / QUOTA_LOCK_NAME):
            current = _load_state_locked(
                safe_root,
                maximum=maximum,
                reserve=reserve,
            )
            records = [dict(value) for value in current["reservations"]]
            records.append(
                {
                    "token": token,
                    "owner_pid": os.getpid(),
                    "reserved_bytes": 0,
                    "materialized_bytes": 0,
                }
            )
            _write_state(
                safe_root,
                _state(
                    max_generation_bytes=maximum,
                    min_free_bytes=reserve,
                    committed_bytes=int(current["committed_bytes"]),
                    reservations=records,
                ),
            )
            state_recorded = True
            lease = PublicationLease(
                _root=safe_root,
                _max_generation_bytes=maximum,
                _min_free_bytes=reserve,
                _committed_bytes=int(current["committed_bytes"]),
                _token=token,
                _guard=guard,
            )
        yield lease
    finally:
        remove_guard = (
            not state_recorded
            or (lease is not None and lease._finished)
        )
        guard.close(remove=remove_guard)


__all__ = [
    "AuthoritySpoolQuotaError",
    "PublicationLease",
    "QUOTA_LOCK_NAME",
    "QUOTA_RESERVATION_PREFIX",
    "QUOTA_STATE_NAME",
    "publication_lease",
]
