"""
Storage management helpers for FxPM.

This module keeps PM-owned storage bounded, records actionable signal history,
and discovers MetaQuotes cleanup candidates under explicit config control.
"""

from __future__ import annotations

import gzip
import hashlib
import json
import logging
import math
import os
import shutil
import threading
import time
from contextlib import contextmanager
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional, Sequence


_PROCESS_FILE_LOCK = threading.RLock()


@contextmanager
def _exclusive_file_lock(
    lock_path: Path,
    *,
    timeout_seconds: Optional[float] = None,
) -> Iterator[None]:
    """Serialize a file transaction across threads and OS processes.

    POSIX advisory locks are process-scoped, so the process-local RLock is
    required as well as the OS lock.  The one-byte lock file is persistent and
    bounded; the kernel releases its lock automatically if a writer exits.
    """
    if timeout_seconds is None:
        deadline = None
    else:
        if (
            isinstance(timeout_seconds, bool)
            or not isinstance(timeout_seconds, (int, float))
            or not math.isfinite(float(timeout_seconds))
            or float(timeout_seconds) < 0.0
        ):
            raise ValueError("FILE_LOCK_TIMEOUT_INVALID")
        deadline = time.monotonic() + float(timeout_seconds)
    lock_path = Path(lock_path)
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    # A single process-local lock is intentionally constant-space.  Per-path
    # lock registries grow forever as monthly ledgers roll over.
    if deadline is None:
        process_lock_acquired = _PROCESS_FILE_LOCK.acquire()
    else:
        process_lock_acquired = _PROCESS_FILE_LOCK.acquire(
            blocking=False
        )
        if not process_lock_acquired and deadline > time.monotonic():
            process_lock_acquired = _PROCESS_FILE_LOCK.acquire(
                timeout=max(0.0, deadline - time.monotonic())
            )
    if not process_lock_acquired:
        raise TimeoutError(f"FXPM_FILE_LOCK_TIMEOUT:{lock_path}")
    try:
        with open(lock_path, "a+b") as handle:
            handle.seek(0, os.SEEK_END)
            if handle.tell() == 0:
                handle.write(b"\0")
                handle.flush()
                os.fsync(handle.fileno())
            handle.seek(0)
            if os.name == "nt":
                import msvcrt

                while True:
                    try:
                        handle.seek(0)
                        msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
                        break
                    except OSError:
                        if deadline is not None and time.monotonic() >= deadline:
                            raise TimeoutError(
                                f"FXPM_FILE_LOCK_TIMEOUT:{lock_path}"
                            )
                        wait_seconds = 0.01
                        if deadline is not None:
                            wait_seconds = min(
                                wait_seconds,
                                max(0.0, deadline - time.monotonic()),
                            )
                        time.sleep(wait_seconds)
                try:
                    yield
                finally:
                    handle.seek(0)
                    msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                import fcntl

                if deadline is None:
                    fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
                else:
                    while True:
                        try:
                            fcntl.flock(
                                handle.fileno(),
                                fcntl.LOCK_EX | fcntl.LOCK_NB,
                            )
                            break
                        except BlockingIOError:
                            if time.monotonic() >= deadline:
                                raise TimeoutError(
                                    f"FXPM_FILE_LOCK_TIMEOUT:{lock_path}"
                                )
                            time.sleep(
                                min(
                                    0.01,
                                    max(0.0, deadline - time.monotonic()),
                                )
                            )
                try:
                    yield
                finally:
                    fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
    finally:
        _PROCESS_FILE_LOCK.release()


def _now() -> datetime:
    return datetime.now()


def _dt_from_iso(value: Any) -> Optional[datetime]:
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value))
    except Exception:
        return None


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return default


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except Exception:
        return default


def _safe_size(path: Path) -> int:
    try:
        return int(path.stat().st_size)
    except OSError:
        return 0


def _directory_size_bytes(path: Path) -> int:
    if not path.exists():
        return 0
    total = 0
    try:
        for child in path.rglob("*"):
            if child.is_file():
                total += _safe_size(child)
    except Exception:
        return total
    return total


def _path_modified_at(path: Path) -> Optional[datetime]:
    try:
        return datetime.fromtimestamp(path.stat().st_mtime)
    except OSError:
        return None


def _path_is_recent(path: Path, now: datetime, protect_minutes: int) -> bool:
    modified = _path_modified_at(path)
    if modified is None:
        return True
    return modified >= (now - timedelta(minutes=max(0, protect_minutes)))


def _dir_has_recent_writes(path: Path, now: datetime, protect_minutes: int) -> bool:
    if not path.exists():
        return False
    cutoff = now - timedelta(minutes=max(0, protect_minutes))
    try:
        root_modified = datetime.fromtimestamp(path.stat().st_mtime)
        if root_modified >= cutoff:
            return True
    except OSError:
        return True
    try:
        for child in path.rglob("*"):
            modified = _path_modified_at(child)
            if modified is None or modified >= cutoff:
                return True
    except Exception:
        return True
    return False


def _path_is_quiescent(path: Path, now: datetime, protect_minutes: int) -> bool:
    if path.is_dir():
        return not _dir_has_recent_writes(path, now, protect_minutes)
    return not _path_is_recent(path, now, protect_minutes)


def _delete_path(path: Path) -> None:
    if path.is_dir():
        shutil.rmtree(path)
    else:
        path.unlink()


def _append_jsonl(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, ensure_ascii=True, default=str))
        handle.write("\n")
        handle.flush()
        os.fsync(handle.fileno())


def _atomic_write_json(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_name(f"{path.name}.tmp.{os.getpid()}.{threading.get_ident()}")
    with open(tmp_path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, sort_keys=True)
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(tmp_path, path)


class SignalLedger:
    """Append-only monthly JSONL ledger for actionable signal outcomes."""

    def __init__(self, output_dir: Path, enabled: bool = True, filename: str = "signal_ledger.jsonl") -> None:
        self.output_dir = Path(output_dir)
        self.enabled = bool(enabled)
        self.filename = str(filename or "signal_ledger.jsonl").strip() or "signal_ledger.jsonl"

    def _ledger_path(self, when: datetime) -> Path:
        stem = Path(self.filename).stem or "signal_ledger"
        suffix = Path(self.filename).suffix or ".jsonl"
        return self.output_dir / f"{stem}_{when.strftime('%Y%m')}{suffix}"

    def append(self, record: Dict[str, Any], when: Optional[datetime] = None) -> Optional[Path]:
        if not self.enabled or not isinstance(record, dict):
            return None
        timestamp = when or _now()
        payload = dict(record)
        payload.setdefault("ledger_recorded_at", timestamp.isoformat())
        path = self._ledger_path(timestamp)
        stem = Path(self.filename).stem or "signal_ledger"
        lock_path = self.output_dir / f".{stem}.append.lock"
        with _exclusive_file_lock(lock_path):
            _append_jsonl(path, payload)
        return path


class StorageManager:
    """Config-driven storage governance for PM-owned files and safe external discovery."""

    def __init__(
        self,
        pipeline_config: Any,
        data_dir: Path,
        output_dir: Path,
        log_dir: Path,
        logger: Optional[logging.Logger] = None,
        active_servers: Optional[Sequence[str]] = None,
        active_symbols: Optional[Sequence[str]] = None,
    ) -> None:
        self.config = pipeline_config
        self.data_dir = Path(data_dir)
        self.output_dir = Path(output_dir)
        self.log_dir = Path(log_dir)
        self.logger = logger or logging.getLogger(__name__)
        state_filename = str(getattr(self.config, "state_filename", "storage_state.json") or "storage_state.json").strip()
        manifest_filename = str(getattr(self.config, "manifest_filename", "storage_manifest.jsonl") or "storage_manifest.jsonl").strip()
        ledger_filename = str(getattr(self.config, "ledger_filename", "signal_ledger.jsonl") or "signal_ledger.jsonl").strip()
        self.state_path = self.output_dir / state_filename
        self.manifest_path = self.output_dir / manifest_filename
        self._manifest_lock_path = self.manifest_path.with_name(
            f"{self.manifest_path.name}.lock"
        )
        self.signal_ledger = SignalLedger(
            self.output_dir,
            enabled=bool(getattr(self.config, "storage_signal_ledger_enabled", True)),
            filename=ledger_filename,
        )
        self._active_servers = {
            str(item).strip().lower()
            for item in (active_servers or [])
            if str(item).strip()
        }
        self._active_servers.update(
            str(item).strip().lower()
            for item in (getattr(self.config, "storage_metaquotes_active_root_allowlist", []) or [])
            if str(item).strip()
        )
        self._active_symbols = {
            str(item).strip().upper()
            for item in ((active_symbols or getattr(self.config, "symbols", [])) or [])
            if str(item).strip()
        }
        self._state: Dict[str, Any] = self._load_state()
        self._last_pressure_level = str(self._state.get("pressure_level") or "normal").lower()

    def record_actionable(self, record: Dict[str, Any], now: Optional[datetime] = None) -> None:
        if not isinstance(record, dict):
            return
        action = str(record.get("action") or "").strip().upper()
        if not action or action.startswith("NO_ACTIONABLE"):
            return
        try:
            self.signal_ledger.append(record, when=now)
        except Exception as exc:
            self.logger.debug(f"Signal ledger append failed: {exc}")

    def _order_governance_bucket(self) -> Dict[str, Any]:
        bucket = self._state.get("order_governance")
        if not isinstance(bucket, dict):
            bucket = {}
            self._state["order_governance"] = bucket
        return bucket

    def get_order_governance_state(self, ticket: int) -> Dict[str, Any]:
        if int(ticket or 0) <= 0:
            return {}
        bucket = self._order_governance_bucket()
        value = bucket.get(str(int(ticket)), {})
        return dict(value) if isinstance(value, dict) else {}

    def set_order_governance_state(self, ticket: int, state: Dict[str, Any]) -> None:
        ticket_id = int(ticket or 0)
        if ticket_id <= 0 or not isinstance(state, dict):
            return
        bucket = self._order_governance_bucket()
        bucket[str(ticket_id)] = dict(state)
        self._persist_state()

    def prune_order_governance_state(self, open_tickets: Sequence[int]) -> None:
        bucket = self._order_governance_bucket()
        keep = {str(int(ticket)) for ticket in (open_tickets or []) if int(ticket or 0) > 0}
        stale = [ticket for ticket in bucket.keys() if ticket not in keep]
        if not stale:
            return
        for ticket in stale:
            bucket.pop(ticket, None)
        self._persist_state()

    def add_active_server(self, server_name: str) -> None:
        normalized = str(server_name or "").strip().lower()
        if normalized:
            self._active_servers.add(normalized)

    def set_active_symbols(self, symbols: Sequence[str]) -> None:
        self._active_symbols = {
            str(item).strip().upper()
            for item in (symbols or [])
            if str(item).strip()
        }

    def is_due(self, now: Optional[datetime] = None) -> bool:
        ts = now or _now()
        for key in ("next_measure_at", "next_housekeep_at", "next_metaquotes_review_at"):
            due_at = _dt_from_iso(self._state.get(key))
            if due_at is None or ts >= due_at:
                return True
        return False

    def mark_run(self, now: Optional[datetime] = None) -> Dict[str, Any]:
        ts = now or _now()
        self._state["next_measure_at"] = (
            ts + timedelta(seconds=_safe_int(getattr(self.config, "storage_measure_interval_seconds", 300), 300))
        ).isoformat()
        self._state["next_housekeep_at"] = (
            ts + timedelta(seconds=_safe_int(getattr(self.config, "storage_housekeeping_interval_seconds", 900), 900))
        ).isoformat()
        self._state["next_metaquotes_review_at"] = (
            ts + timedelta(seconds=_safe_int(getattr(self.config, "storage_metaquotes_review_interval_seconds", 21600), 21600))
        ).isoformat()
        self._persist_state()
        return dict(self._state)

    def prune_path(
        self,
        path: Path | str,
        *,
        reason: str = "",
        dry_run: bool = True,
        min_age_seconds: Optional[int] = None,
        action: str = "delete_path",
        trigger: str = "manual",
        extra_protected_roots: Optional[Sequence[Path | str]] = None,
    ) -> Optional[Dict[str, Any]]:
        target = Path(path)
        if not target.exists():
            return None
        if self._is_path_protected(target, extra_roots=extra_protected_roots):
            return None

        now = _now()
        protect_minutes = _safe_int(getattr(self.config, "storage_write_protect_minutes", 5), 5)
        if min_age_seconds is not None:
            protect_minutes = max(protect_minutes, max(0, int(min_age_seconds) + 59) // 60)
        size_bytes = _directory_size_bytes(target) if target.is_dir() else _safe_size(target)

        if not _path_is_quiescent(target, now, protect_minutes):
            self._append_manifest(
                trigger=trigger,
                action=action,
                path=target,
                reason=reason or "protected_recent_write",
                size_bytes=size_bytes,
                result="protected_recent_write",
                now=now,
            )
            return None

        if bool(getattr(self.config, "storage_observe_only", True)) or dry_run:
            self._append_manifest(
                trigger=trigger,
                action=action,
                path=target,
                reason=reason or "candidate",
                size_bytes=size_bytes,
                result="candidate",
                now=now,
            )
            return {
                "path": str(target),
                "reason": reason,
                "size_bytes": size_bytes,
                "result": "candidate",
                "dry_run": True,
            }

        try:
            _delete_path(target)
            result = "deleted"
        except Exception as exc:
            result = f"failed:{exc}"
        self._append_manifest(
            trigger=trigger,
            action=action,
            path=target,
            reason=reason or "manual_prune",
            size_bytes=size_bytes,
            result=result,
            now=now,
        )
        return {
            "path": str(target),
            "reason": reason,
            "size_bytes": size_bytes,
            "result": result,
            "dry_run": False,
        }

    def should_pause_new_entries(self, now: Optional[datetime] = None) -> bool:
        threshold = getattr(self.config, "storage_pause_entries_below_free_gb", None)
        if threshold in (None, "", False):
            return False
        free_gb = self._measure_free_gb()
        return free_gb < _safe_float(threshold, default=-1.0)

    def on_sweep_complete(
        self,
        *,
        symbol_count: int,
        open_positions: int,
        sweep_duration: float,
        live_equity: float,
        now: Optional[datetime] = None,
    ) -> None:
        if not bool(getattr(self.config, "storage_enabled", True)):
            return
        ts = now or _now()
        free_gb = self._measure_free_gb()
        self._state["last_sweep"] = {
            "at": ts.isoformat(),
            "symbol_count": int(symbol_count),
            "open_positions": int(open_positions),
            "sweep_duration_seconds": round(float(sweep_duration), 3),
            "live_equity": round(float(live_equity), 2),
            "free_gb": round(free_gb, 2),
        }
        self._update_pressure_level(free_gb)

        run_measure = self._is_due(
            "next_measure_at",
            ts,
            _safe_int(getattr(self.config, "storage_measure_interval_seconds", 300), 300),
        )
        run_housekeep = self._is_due(
            "next_housekeep_at",
            ts,
            _safe_int(getattr(self.config, "storage_housekeeping_interval_seconds", 900), 900),
        )
        run_metaquotes_review = self._is_due(
            "next_metaquotes_review_at",
            ts,
            _safe_int(getattr(self.config, "storage_metaquotes_review_interval_seconds", 21600), 21600),
        )

        if free_gb <= _safe_float(getattr(self.config, "storage_critical_free_gb", 10.0), 10.0):
            run_housekeep = True

        if run_measure or run_housekeep or run_metaquotes_review:
            self._run_housekeeping(
                ts,
                trigger="sweep",
                collect_metrics=True,
                cleanup_pm_owned=run_housekeep,
                review_metaquotes=run_metaquotes_review,
            )
        else:
            self._persist_state()

    def on_shutdown(self, now: Optional[datetime] = None) -> None:
        if not bool(getattr(self.config, "storage_enabled", True)):
            return
        ts = now or _now()
        self._run_housekeeping(
            ts,
            trigger="shutdown",
            collect_metrics=True,
            cleanup_pm_owned=True,
            review_metaquotes=False,
        )

    def on_optimization_complete(self, now: Optional[datetime] = None) -> None:
        if not bool(getattr(self.config, "storage_enabled", True)):
            return
        ts = now or _now()
        self._state["last_optimization_at"] = ts.isoformat()
        self._run_housekeeping(
            ts,
            trigger="post_optimization",
            collect_metrics=True,
            cleanup_pm_owned=True,
            review_metaquotes=True,
        )

    def prune_metaquotes_demo_roots(
        self,
        roots: Sequence[Path | str],
        *,
        dry_run: bool = True,
        min_age_seconds: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        if not bool(getattr(self.config, "storage_metaquotes_cleanup_enabled", False)):
            return []
        if bool(getattr(self.config, "storage_observe_only", True)):
            return []

        now = _now()
        protect_minutes = _safe_int(getattr(self.config, "storage_write_protect_minutes", 5), 5)
        if min_age_seconds is not None:
            protect_minutes = max(protect_minutes, int(max(0, min_age_seconds + 59) // 60))
        demo_names = {
            str(item).strip().lower()
            for item in (getattr(self.config, "storage_metaquotes_demo_servers", ["FBS-Demo", "MetaQuotes-Demo"]) or [])
            if str(item).strip()
        }
        results: List[Dict[str, Any]] = []
        for item in roots:
            root = Path(item)
            lowered = root.name.lower()
            if lowered in self._active_servers:
                continue
            if lowered not in demo_names and "demo" not in lowered:
                continue
            if not root.exists() or not _path_is_quiescent(root, now, protect_minutes):
                continue
            size_bytes = _directory_size_bytes(root)
            result = self.prune_path(
                root,
                reason="confirmed_or_detected_demo_root",
                dry_run=dry_run,
                min_age_seconds=min_age_seconds,
                action="delete_metaquotes_demo_root",
                trigger="manual",
                extra_protected_roots=[root],
            )
            if result is None:
                continue
            result["size_bytes"] = size_bytes
            results.append(result)
        return results

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _is_path_protected(self, path: Path, extra_roots: Optional[Sequence[Path | str]] = None) -> bool:
        allowed_roots = [
            self.output_dir,
            self.log_dir,
            self.data_dir / ".cache",
        ]
        for item in extra_roots or []:
            allowed_roots.append(Path(item))

        try:
            resolved_target = path.resolve(strict=False)
        except Exception:
            resolved_target = path

        for root in allowed_roots:
            try:
                resolved_root = root.resolve(strict=False)
            except Exception:
                resolved_root = root
            if resolved_target == resolved_root or resolved_root in resolved_target.parents:
                return False
        return True

    def _load_state(self) -> Dict[str, Any]:
        try:
            with open(self.state_path, "r", encoding="utf-8") as handle:
                payload = json.load(handle)
            if isinstance(payload, dict):
                return payload
        except FileNotFoundError:
            pass
        except Exception as exc:
            self.logger.debug(f"Storage state load failed: {exc}")
        return {}

    def _state_freshness_context(self, now: datetime) -> Dict[str, Any]:
        last_sweep = self._state.get("last_sweep")
        last_housekeeping = self._state.get("last_housekeeping")
        sweep_at = _dt_from_iso(last_sweep.get("at")) if isinstance(last_sweep, dict) else None
        housekeeping_at = (
            _dt_from_iso(last_housekeeping.get("ran_at"))
            if isinstance(last_housekeeping, dict) else None
        )

        def _age_seconds(stamp: Optional[datetime]) -> Optional[int]:
            if stamp is None:
                return None
            return int(max(0.0, (now - stamp).total_seconds()))

        measure_interval = max(1, _safe_int(getattr(self.config, "storage_measure_interval_seconds", 300), 300))
        housekeeping_interval = max(
            1,
            _safe_int(getattr(self.config, "storage_housekeeping_interval_seconds", 900), 900),
        )
        sweep_age = _age_seconds(sweep_at)
        housekeeping_age = _age_seconds(housekeeping_at)
        return {
            "as_of": now.isoformat(),
            "last_sweep_at": sweep_at.isoformat() if sweep_at else None,
            "last_sweep_age_seconds": sweep_age,
            "last_sweep_is_fresh": bool(sweep_age is not None and sweep_age <= measure_interval * 2),
            "last_housekeeping_at": housekeeping_at.isoformat() if housekeeping_at else None,
            "last_housekeeping_age_seconds": housekeeping_age,
            "last_housekeeping_is_fresh": bool(
                housekeeping_age is not None and housekeeping_age <= housekeeping_interval * 2
            ),
        }

    def _persist_state(self) -> None:
        try:
            now = _now()
            self._state["state_updated_at"] = now.isoformat()
            self._state["freshness"] = self._state_freshness_context(now)
            _atomic_write_json(self.state_path, self._state)
        except Exception as exc:
            self.logger.debug(f"Storage state save failed: {exc}")

    # Non-terminal journal results may repeat every sweep in observe-only
    # mode; terminal outcomes (deleted/failed) must always be journaled.
    _MANIFEST_DEDUPE_RESULTS = frozenset({"candidate", "protected_recent_write", "missing"})
    _MANIFEST_DEFAULT_MAX_MB = 16.0
    _MANIFEST_ABSOLUTE_MAX_MB = 1024.0
    _MANIFEST_MAX_ROTATIONS = 16
    _MANIFEST_MAX_DEDUPE_HOURS = 24.0 * 365.0

    def _manifest_limits(self) -> tuple[int, int]:
        max_mb = _safe_float(
            getattr(self.config, "storage_manifest_max_mb", self._MANIFEST_DEFAULT_MAX_MB),
            self._MANIFEST_DEFAULT_MAX_MB,
        )
        if not math.isfinite(max_mb) or max_mb <= 0:
            max_mb = self._MANIFEST_DEFAULT_MAX_MB
        max_mb = min(self._MANIFEST_ABSOLUTE_MAX_MB, max_mb)
        keep = _safe_int(getattr(self.config, "storage_manifest_keep_rotations", 2), 2)
        keep = min(self._MANIFEST_MAX_ROTATIONS, max(0, keep))
        return max(1, int(max_mb * 1024 * 1024)), keep

    def _remove_excess_manifest_rotations(self, keep: int) -> None:
        prefix = f"{self.manifest_path.name}."
        for candidate in self.manifest_path.parent.glob(f"{self.manifest_path.name}.*"):
            token = candidate.name[len(prefix):]
            if token.isdigit() and int(token) > keep:
                candidate.unlink(missing_ok=True)

    def _rotate_manifest_if_needed_locked(self, incoming_bytes: int = 0) -> None:
        cap_bytes, keep = self._manifest_limits()
        self._remove_excess_manifest_rotations(keep)
        current_size = _safe_size(self.manifest_path)
        if current_size <= 0 or current_size + max(0, int(incoming_bytes)) <= cap_bytes:
            return
        if keep == 0:
            self.manifest_path.unlink(missing_ok=True)
            return
        oldest = self.manifest_path.with_name(f"{self.manifest_path.name}.{keep}")
        oldest.unlink(missing_ok=True)
        for index in range(keep - 1, 0, -1):
            source = self.manifest_path.with_name(f"{self.manifest_path.name}.{index}")
            if source.exists():
                os.replace(
                    source,
                    self.manifest_path.with_name(f"{self.manifest_path.name}.{index + 1}"),
                )
        os.replace(
            self.manifest_path,
            self.manifest_path.with_name(f"{self.manifest_path.name}.1"),
        )
        self._remove_excess_manifest_rotations(keep)

    def _rotate_manifest_if_needed(self, incoming_bytes: int = 0) -> None:
        """Rotate under the same cross-process lock used by manifest append."""
        with _exclusive_file_lock(self._manifest_lock_path):
            self._rotate_manifest_if_needed_locked(incoming_bytes=incoming_bytes)

    def _should_journal(
        self,
        path: Path,
        result: str,
        size_bytes: int,
        now: datetime,
        *,
        trigger: str = "",
        action: str = "",
        reason: str = "",
    ) -> bool:
        if result not in self._MANIFEST_DEDUPE_RESULTS:
            return True
        window_hours = _safe_float(getattr(self.config, "storage_manifest_dedupe_hours", 24.0), 24.0)
        if not math.isfinite(window_hours):
            window_hours = 24.0
        window_hours = min(self._MANIFEST_MAX_DEDUPE_HOURS, window_hours)
        if window_hours <= 0:
            return True
        bucket = self._state.get("manifest_dedupe")
        if not isinstance(bucket, dict):
            bucket = {}
            self._state["manifest_dedupe"] = bucket
        key = str(path)
        prior = bucket.get(key)
        if isinstance(prior, dict):
            prior_at = _dt_from_iso(prior.get("at"))
            try:
                within_window = bool(
                    prior_at is not None
                    and prior_at >= now - timedelta(hours=window_hours)
                )
            except (TypeError, OverflowError, ValueError):
                within_window = False
            if (
                str(prior.get("result")) == str(result)
                and _safe_int(prior.get("size_bytes"), -1) == int(size_bytes)
                and str(prior.get("trigger") or "") == str(trigger)
                and str(prior.get("action") or "") == str(action)
                and str(prior.get("reason") or "") == str(reason)
                and within_window
            ):
                return False
        bucket[key] = {
            "result": str(result),
            "size_bytes": int(size_bytes),
            "trigger": str(trigger),
            "action": str(action),
            "reason": str(reason),
            "at": now.isoformat(),
        }
        if len(bucket) > 512:
            ordered = sorted(
                bucket.items(),
                key=lambda item: str((item[1] or {}).get("at") or ""),
            )
            for stale_key, _payload in ordered[: len(bucket) - 512]:
                bucket.pop(stale_key, None)
        return True

    def _append_manifest(
        self,
        *,
        trigger: str,
        action: str,
        path: Path,
        reason: str,
        size_bytes: int,
        result: str,
        now: datetime,
    ) -> None:
        payload = {
            "time": now.isoformat(),
            "trigger": trigger,
            "action": action,
            "path": str(path),
            "reason": reason,
            "size_bytes": int(size_bytes),
            "result": result,
        }
        encoded_size = len(
            (json.dumps(payload, ensure_ascii=True, default=str) + "\n").encode("utf-8")
        )
        with _exclusive_file_lock(self._manifest_lock_path):
            if not self._should_journal(
                path,
                result,
                int(size_bytes),
                now,
                trigger=trigger,
                action=action,
                reason=reason,
            ):
                return
            try:
                self._rotate_manifest_if_needed_locked(incoming_bytes=encoded_size)
            except Exception as exc:
                # Rotation is housekeeping.  A rotation failure must never
                # suppress the event it was meant to make room for.
                self.logger.debug(f"Storage manifest rotation failed: {exc}")
            try:
                _append_jsonl(self.manifest_path, payload)
            except Exception as exc:
                self.logger.debug(f"Storage manifest append failed: {exc}")

    def _is_due(self, key: str, now: datetime, interval_seconds: int) -> bool:
        interval = max(1, int(interval_seconds))
        due_at = _dt_from_iso(self._state.get(key))
        if due_at is None or now >= due_at:
            self._state[key] = (now + timedelta(seconds=interval)).isoformat()
            return True
        return False

    def _measure_free_gb(self) -> float:
        anchor = self.data_dir.anchor or str(self.data_dir)
        usage = shutil.disk_usage(anchor)
        return round(float(usage.free) / float(1024 ** 3), 2)

    def _update_pressure_level(self, free_gb: float) -> None:
        warn_gb = _safe_float(getattr(self.config, "storage_warn_free_gb", 15.0), 15.0)
        critical_gb = _safe_float(getattr(self.config, "storage_critical_free_gb", 10.0), 10.0)
        if free_gb <= critical_gb:
            level = "critical"
        elif free_gb <= warn_gb:
            level = "warning"
        else:
            level = "normal"

        self._state["pressure_level"] = level
        self._state["last_free_gb"] = round(free_gb, 2)
        if level != self._last_pressure_level:
            if level == "critical":
                self.logger.warning(
                    f"Storage pressure CRITICAL: free space {free_gb:.2f} GB <= {critical_gb:.2f} GB"
                )
            elif level == "warning":
                self.logger.warning(
                    f"Storage pressure warning: free space {free_gb:.2f} GB <= {warn_gb:.2f} GB"
                )
            else:
                self.logger.info(f"Storage pressure normalized: free space {free_gb:.2f} GB")
            self._last_pressure_level = level

    def _run_housekeeping(
        self,
        now: datetime,
        *,
        trigger: str,
        collect_metrics: bool,
        cleanup_pm_owned: bool,
        review_metaquotes: bool,
    ) -> None:
        summary: Dict[str, Any] = {
            "trigger": trigger,
            "ran_at": now.isoformat(),
            "observe_only": bool(getattr(self.config, "storage_observe_only", True)),
            "pm_owned_cleanup": {},
            "metaquotes": {},
        }

        if collect_metrics:
            summary["pm_sizes"] = self._collect_pm_sizes()
            summary["free_gb"] = self._measure_free_gb()

        if cleanup_pm_owned:
            summary["pm_owned_cleanup"] = self._cleanup_pm_owned(now, trigger)

        if review_metaquotes:
            summary["metaquotes"] = self._review_metaquotes(now, trigger)

        self._state["last_housekeeping"] = summary
        self._persist_state()

    def _collect_pm_sizes(self) -> Dict[str, int]:
        cache_dir = self.data_dir / ".cache"
        live_dir = self.data_dir / ".live"
        return {
            "data_dir_bytes": _directory_size_bytes(self.data_dir),
            "cache_dir_bytes": _directory_size_bytes(cache_dir),
            "live_dir_bytes": _directory_size_bytes(live_dir),
            "output_dir_bytes": _directory_size_bytes(self.output_dir),
            "log_dir_bytes": _directory_size_bytes(self.log_dir),
        }

    def _cleanup_pm_owned(self, now: datetime, trigger: str) -> Dict[str, Any]:
        observe_only = bool(getattr(self.config, "storage_observe_only", True))
        candidates: List[Dict[str, Any]] = []
        candidates.extend(self._log_cleanup_candidates(now))
        candidates.extend(self._trade_snapshot_cleanup_candidates(now))
        candidates.extend(self._cache_cleanup_candidates(now))
        candidates.extend(self._live_cache_cleanup_candidates(now))
        # signal_ledger_YYYYMM.jsonl is never age-deleted.  Closed months are
        # losslessly archived to .jsonl.gz only after the compressed payload
        # decompresses sha256-identical, and every dashboard/audit reader
        # consumes the archive transparently, so canonical history survives
        # while storage stays bounded.
        candidates.extend(self._signal_ledger_compaction_candidates(now))

        reclaimed_bytes = 0
        deleted = 0
        compressed = 0
        skipped = 0
        protect_minutes = _safe_int(getattr(self.config, "storage_write_protect_minutes", 5), 5)

        for candidate in candidates:
            path = candidate["path"]
            size_bytes = int(candidate.get("size_bytes", 0))
            reason = str(candidate.get("reason") or "")
            action = str(candidate.get("action") or "delete")
            if not path.exists():
                self._append_manifest(
                    trigger=trigger,
                    action=action,
                    path=path,
                    reason=reason,
                    size_bytes=size_bytes,
                    result="missing",
                    now=now,
                )
                skipped += 1
                continue
            if not _path_is_quiescent(path, now, protect_minutes):
                self._append_manifest(
                    trigger=trigger,
                    action=action,
                    path=path,
                    reason=reason,
                    size_bytes=size_bytes,
                    result="protected_recent_write",
                    now=now,
                )
                skipped += 1
                continue
            if observe_only:
                self._append_manifest(
                    trigger=trigger,
                    action=action,
                    path=path,
                    reason=reason,
                    size_bytes=size_bytes,
                    result="candidate",
                    now=now,
                )
                reclaimed_bytes += size_bytes
                continue
            try:
                if action == "compress_signal_ledger_month":
                    reclaimed_bytes += self._compress_ledger_month(path)
                    compressed += 1
                    result = "compressed"
                else:
                    _delete_path(path)
                    reclaimed_bytes += size_bytes
                    deleted += 1
                    result = "deleted"
                self._append_manifest(
                    trigger=trigger,
                    action=action,
                    path=path,
                    reason=reason,
                    size_bytes=size_bytes,
                    result=result,
                    now=now,
                )
            except Exception as exc:
                skipped += 1
                self._append_manifest(
                    trigger=trigger,
                    action=action,
                    path=path,
                    reason=reason,
                    size_bytes=size_bytes,
                    result=f"failed:{exc}",
                    now=now,
                )

        return {
            "candidates": len(candidates),
            "deleted": deleted,
            "compressed": compressed,
            "skipped": skipped,
            "reclaimable_bytes": int(reclaimed_bytes),
        }

    def _log_cleanup_candidates(self, now: datetime) -> List[Dict[str, Any]]:
        keep_days = max(0, _safe_int(getattr(self.config, "storage_logs_keep_days", 14), 14))
        cutoff = now - timedelta(days=keep_days)
        candidates: List[Dict[str, Any]] = []
        if not self.log_dir.exists():
            return candidates
        for path in sorted(self.log_dir.glob("*.log")):
            modified = _path_modified_at(path)
            if modified is None or modified >= cutoff:
                continue
            candidates.append(
                {
                    "path": path,
                    "size_bytes": _safe_size(path),
                    "reason": f"log_retention>{keep_days}d",
                    "action": "delete_log",
                }
            )
        return candidates

    def _trade_snapshot_cleanup_candidates(self, now: datetime) -> List[Dict[str, Any]]:
        keep_days = max(0, _safe_int(getattr(self.config, "storage_pm_outputs_keep_days", 14), 14))
        keep_count = max(0, _safe_int(getattr(self.config, "storage_pm_outputs_keep_count", 30), 30))
        cutoff = now - timedelta(days=keep_days)
        files: List[tuple[Path, float]] = []
        for path in sorted(self.output_dir.glob("trades_*.json")):
            try:
                files.append((path, path.stat().st_mtime))
            except OSError:
                continue
        files.sort(key=lambda item: item[1], reverse=True)
        protected = {path for path, _mtime in files[:keep_count]}
        candidates: List[Dict[str, Any]] = []
        for path, mtime in files:
            modified = datetime.fromtimestamp(mtime)
            if path in protected and modified >= cutoff:
                continue
            if modified >= cutoff and len(files) <= keep_count:
                continue
            candidates.append(
                {
                    "path": path,
                    "size_bytes": _safe_size(path),
                    "reason": f"trade_snapshot_retention>{keep_days}d_or_keep_count>{keep_count}",
                    "action": "delete_trade_snapshot",
                }
            )
        return candidates

    def _cache_cleanup_candidates(self, now: datetime) -> List[Dict[str, Any]]:
        cache_dir = self.data_dir / ".cache"
        if not cache_dir.exists():
            return []
        keep_days = max(0, _safe_int(getattr(self.config, "storage_resample_cache_max_age_days", 7), 7))
        quota_bytes = int(max(0.0, _safe_float(getattr(self.config, "storage_resample_cache_max_gb", 1.0), 1.0)) * (1024 ** 3))
        cutoff = now - timedelta(days=keep_days)
        files: List[Dict[str, Any]] = []
        for path in sorted(cache_dir.rglob("*.pkl")):
            modified = _path_modified_at(path)
            if modified is None:
                continue
            files.append(
                {
                    "path": path,
                    "size_bytes": _safe_size(path),
                    "mtime": modified,
                }
            )
        if not files:
            return []

        candidates: List[Dict[str, Any]] = []
        marked: set[str] = set()
        for item in files:
            if item["mtime"] < cutoff:
                marked.add(str(item["path"]))
                candidates.append(
                    {
                        "path": item["path"],
                        "size_bytes": item["size_bytes"],
                        "reason": f"cache_age>{keep_days}d",
                        "action": "delete_cache_file",
                    }
                )

        remaining = [item for item in files if str(item["path"]) not in marked]
        total_bytes = sum(int(item["size_bytes"]) for item in remaining)
        if quota_bytes > 0 and total_bytes > quota_bytes:
            overflow = total_bytes - quota_bytes
            for item in sorted(remaining, key=lambda payload: payload["mtime"]):
                key = str(item["path"])
                if key in marked:
                    continue
                marked.add(key)
                candidates.append(
                    {
                        "path": item["path"],
                        "size_bytes": item["size_bytes"],
                        "reason": f"cache_quota>{quota_bytes}",
                        "action": "delete_cache_file",
                    }
                )
                overflow -= int(item["size_bytes"])
                if overflow <= 0:
                    break
        return candidates

    def _ledger_stem_suffix(self) -> tuple[str, str]:
        stem = Path(self.signal_ledger.filename).stem or "signal_ledger"
        suffix = Path(self.signal_ledger.filename).suffix or ".jsonl"
        return stem, suffix

    def _signal_ledger_compaction_candidates(self, now: datetime) -> List[Dict[str, Any]]:
        """Nominate closed ledger months for lossless gzip archiving.

        Only whole months older than the configured window qualify; the
        current and recent months always stay raw for hot readers/monitors.
        A month whose raw file reappeared after archiving (late append) is
        re-nominated so the merge path folds it into the existing archive.
        """
        compress_after = _safe_int(
            getattr(self.config, "storage_signal_ledger_compress_after_months", 2), 2
        )
        if compress_after <= 0:
            return []
        compress_after = min(compress_after, 120)
        stem, suffix = self._ledger_stem_suffix()
        candidates: List[Dict[str, Any]] = []
        for path in sorted(self.output_dir.glob(f"{stem}_*{suffix}")):
            token = path.stem[len(stem) + 1:]
            if len(token) != 6 or not token.isdigit():
                continue
            year, month = int(token[:4]), int(token[4:6])
            if not 1 <= month <= 12:
                continue
            age_months = (now.year - year) * 12 + (now.month - month)
            if age_months < compress_after:
                continue
            candidates.append(
                {
                    "path": path,
                    "size_bytes": _safe_size(path),
                    "reason": f"signal_ledger_lossless_archive>{compress_after}m",
                    "action": "compress_signal_ledger_month",
                }
            )
        return candidates

    def _compress_ledger_month(self, path: Path) -> int:
        """Losslessly archive one closed ledger month; return bytes reclaimed.

        The raw file is removed only after the fully written archive
        decompresses to a sha256-identical payload. If an archive already
        exists (a late append recreated the raw file), the raw records are
        merged after the archived ones so no canonical event is ever lost.

        Crash idempotency: a durable transaction sidecar records the raw and
        combined hashes before the archive is replaced. If a crash strands
        both the merged archive and the raw file, the retry recognizes the
        already-merged raw by hash and completes the unlink instead of
        concatenating the same records a second time.
        Runs under the same OS lock as ledger appends.
        """
        stem, _suffix = self._ledger_stem_suffix()
        lock_path = self.output_dir / f".{stem}.append.lock"
        with _exclusive_file_lock(lock_path):
            raw_bytes = path.read_bytes()
            raw_sha = hashlib.sha256(raw_bytes).hexdigest()
            gz_path = path.with_name(f"{path.name}.gz")
            txn_path = gz_path.with_name(f"{gz_path.name}.txn")
            prior_gz_size = _safe_size(gz_path)
            prior_bytes = b""
            if gz_path.exists():
                prior_bytes = gzip.decompress(gz_path.read_bytes())

            # Recovery: if a prior transaction already merged this raw payload
            # (or a prefix of it, when a late append landed on the stranded
            # file) into the current archive, only the unmerged remainder may
            # be merged again - otherwise canonical records duplicate.
            if txn_path.exists():
                try:
                    txn = json.loads(txn_path.read_text(encoding="utf-8"))
                except (OSError, json.JSONDecodeError):
                    txn = {}
                archive_sha = hashlib.sha256(prior_bytes).hexdigest()
                merged_len = _safe_int((txn or {}).get("raw_length"), -1)
                if (
                    isinstance(txn, dict)
                    # A recovery marker is honored only when it provably
                    # belongs to this schema generation and this month file;
                    # a foreign/renamed marker must never satisfy recovery.
                    and txn.get("schema") == "fxpm-ledger-compaction-txn-v1"
                    and txn.get("month_file") == path.name
                    and txn.get("combined_sha256") == archive_sha
                    and 0 <= merged_len <= len(raw_bytes)
                    and hashlib.sha256(raw_bytes[:merged_len]).hexdigest()
                    == txn.get("raw_sha256")
                ):
                    remainder = raw_bytes[merged_len:]
                    if not remainder:
                        path.unlink()
                        txn_path.unlink(missing_ok=True)
                        return max(0, len(raw_bytes))
                    # Atomically truncate the merged prefix off the raw file
                    # first, so every subsequent state (including another
                    # crash) is an ordinary merge of not-yet-archived bytes.
                    truncate_tmp = path.with_name(
                        f"{path.name}.truncate.{os.getpid()}.{threading.get_ident()}"
                    )
                    truncate_tmp.write_bytes(remainder)
                    os.replace(truncate_tmp, path)
                    txn_path.unlink(missing_ok=True)
                    raw_bytes = remainder
                    raw_sha = hashlib.sha256(raw_bytes).hexdigest()
                # Stale/foreign transaction marker: fall through and redo the
                # merge from current state, then overwrite the marker.

            combined = prior_bytes + raw_bytes
            expected_sha = hashlib.sha256(combined).hexdigest()
            _atomic_write_json(
                txn_path,
                {
                    "schema": "fxpm-ledger-compaction-txn-v1",
                    "month_file": path.name,
                    "prior_archive_sha256": hashlib.sha256(prior_bytes).hexdigest(),
                    "raw_sha256": raw_sha,
                    "raw_length": len(raw_bytes),
                    "combined_sha256": expected_sha,
                },
            )
            tmp_path = gz_path.with_name(
                f"{gz_path.name}.tmp.{os.getpid()}.{threading.get_ident()}"
            )
            try:
                with open(tmp_path, "wb") as handle:
                    # mtime=0 keeps the archive byte-deterministic for audits.
                    with gzip.GzipFile(
                        fileobj=handle, mode="wb", filename="", mtime=0
                    ) as gz_handle:
                        gz_handle.write(combined)
                    handle.flush()
                    os.fsync(handle.fileno())
                verified = gzip.decompress(tmp_path.read_bytes())
                if hashlib.sha256(verified).hexdigest() != expected_sha:
                    raise RuntimeError(
                        "ledger archive roundtrip does not match canonical bytes"
                    )
                os.replace(tmp_path, gz_path)
            except BaseException:
                # The archive was not replaced; the transaction never happened.
                txn_path.unlink(missing_ok=True)
                raise
            finally:
                if tmp_path.exists():
                    try:
                        tmp_path.unlink()
                    except OSError:
                        pass
            path.unlink()
            txn_path.unlink(missing_ok=True)
            return max(0, len(raw_bytes) + prior_gz_size - _safe_size(gz_path))

    def _live_cache_cleanup_candidates(self, now: datetime) -> List[Dict[str, Any]]:
        live_dir = self.data_dir / ".live"
        if not live_dir.exists():
            return []
        keep_days = max(0, _safe_int(getattr(self.config, "storage_live_cache_max_age_days", 7), 7))
        cutoff = now - timedelta(days=keep_days)
        candidates: List[Dict[str, Any]] = []
        for path in sorted(live_dir.glob("*.csv")):
            modified = _path_modified_at(path)
            if modified is None:
                continue
            stem = path.stem
            symbol = stem.rsplit("_", 1)[0].upper() if "_" in stem else stem.upper()
            inactive_symbol = bool(self._active_symbols) and symbol not in self._active_symbols
            stale_file = modified < cutoff
            if not (inactive_symbol or stale_file):
                continue
            reason = "inactive_live_cache_symbol" if inactive_symbol else f"live_cache_age>{keep_days}d"
            candidates.append(
                {
                    "path": path,
                    "size_bytes": _safe_size(path),
                    "reason": reason,
                    "action": "delete_live_cache",
                }
            )
        return candidates

    def _review_metaquotes(self, now: datetime, trigger: str) -> Dict[str, Any]:
        root = self._resolve_metaquotes_root()
        if root is None or not root.exists():
            return {"root": None, "available": False}

        observe_only = bool(getattr(self.config, "storage_observe_only", True))
        cleanup_enabled = bool(getattr(self.config, "storage_metaquotes_cleanup_enabled", False))
        allow_cleanup = cleanup_enabled and not observe_only
        protect_minutes = _safe_int(getattr(self.config, "storage_write_protect_minutes", 5), 5)
        stale_days = max(1, _safe_int(getattr(self.config, "storage_metaquotes_stale_tester_days", 14), 14))
        demo_names = {
            str(item).strip().lower()
            for item in (getattr(self.config, "storage_metaquotes_demo_servers", ["FBS-Demo", "MetaQuotes-Demo"]) or [])
            if str(item).strip()
        }
        summary: Dict[str, Any] = {
            "root": str(root),
            "available": True,
            "demo_candidates": [],
            "tester_base_candidates": [],
        }

        def _server_allowed(name: str) -> bool:
            return str(name or "").strip().lower() in self._active_servers

        terminal_bases = list(root.glob("Terminal/*/bases/*"))
        tester_bases = list(root.glob("Tester/*/bases/*"))
        summary["terminal_bytes"] = _directory_size_bytes(root / "Terminal")
        summary["tester_bytes"] = _directory_size_bytes(root / "Tester")

        for base_path in terminal_bases:
            if not base_path.is_dir():
                continue
            server_name = base_path.name
            lowered = server_name.lower()
            if _server_allowed(server_name):
                continue
            is_demo = lowered in demo_names or "demo" in lowered
            if not is_demo:
                continue
            size_bytes = _directory_size_bytes(base_path)
            summary["demo_candidates"].append(
                {
                    "path": str(base_path),
                    "server": server_name,
                    "size_bytes": size_bytes,
                    "reason": "confirmed_or_detected_demo_root",
                }
            )
            if allow_cleanup and _path_is_quiescent(base_path, now, protect_minutes):
                try:
                    _delete_path(base_path)
                    result = "deleted"
                except Exception as exc:
                    result = f"failed:{exc}"
                self._append_manifest(
                    trigger=trigger,
                    action="delete_metaquotes_demo_root",
                    path=base_path,
                    reason="confirmed_or_detected_demo_root",
                    size_bytes=size_bytes,
                    result=result,
                    now=now,
                )

        cutoff = now - timedelta(days=stale_days)
        for base_path in tester_bases:
            if not base_path.is_dir():
                continue
            server_name = base_path.name
            if _server_allowed(server_name):
                continue
            modified = _path_modified_at(base_path)
            if modified is None:
                continue
            lowered = server_name.lower()
            is_demo = lowered in demo_names or "demo" in lowered
            is_stale = modified < cutoff
            if not (is_demo or is_stale):
                continue
            size_bytes = _directory_size_bytes(base_path)
            reason = "demo_tester_base" if is_demo else f"stale_tester_base>{stale_days}d"
            summary["tester_base_candidates"].append(
                {
                    "path": str(base_path),
                    "server": server_name,
                    "size_bytes": size_bytes,
                    "reason": reason,
                }
            )
            if allow_cleanup and _path_is_quiescent(base_path, now, protect_minutes):
                try:
                    _delete_path(base_path)
                    result = "deleted"
                except Exception as exc:
                    result = f"failed:{exc}"
                self._append_manifest(
                    trigger=trigger,
                    action="delete_metaquotes_tester_base",
                    path=base_path,
                    reason=reason,
                    size_bytes=size_bytes,
                    result=result,
                    now=now,
                )

        return summary

    def _resolve_metaquotes_root(self) -> Optional[Path]:
        configured = getattr(self.config, "storage_metaquotes_root", None)
        if configured:
            return Path(configured)
        appdata = os.environ.get("APPDATA")
        if not appdata:
            return None
        return Path(appdata) / "MetaQuotes"
