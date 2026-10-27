"""Process-isolated spawn scheduling for historical symbol work.

This module owns routing and resource bounds only.  It never interprets
historical evidence and never grants authority.  Work continues until every
symbol reaches a terminal result or the operator stops the enclosing run.
"""

from __future__ import annotations

from typing import Any, Callable, Final, Mapping, Sequence

import pm_memory_governor as memory_governor


TERMINATION_GRACE_SECONDS: Final = 5.0


class HistoricalSchedulerError(RuntimeError):
    """Typed technical scheduler failure; never a market verdict."""

    def __init__(self, code: str, details: Sequence[Any] = ()) -> None:
        self.code = str(code)
        self.details = tuple(str(value)[:512] for value in tuple(details)[:16])
        suffix = ":" + "|".join(self.details) if self.details else ""
        super().__init__(self.code + suffix)


def _fail(code: str, *details: Any) -> None:
    raise HistoricalSchedulerError(code, details)


def run_symbol_scheduler(
    worker: Callable[[Mapping[str, Any], str], Mapping[str, Any]],
    *,
    contract: Mapping[str, Any],
    symbols: Sequence[str],
    requested_workers: int,
    phase: str,
) -> dict[str, dict[str, Any]]:
    """Run each exact symbol once and return results in caller order."""

    if not callable(worker):
        _fail("HISTORICAL_SCHEDULER_WORKER_INVALID")
    if type(contract) is not dict:
        _fail("HISTORICAL_SCHEDULER_CONTRACT_INVALID")
    if type(symbols) not in {list, tuple} or not symbols:
        _fail("HISTORICAL_SCHEDULER_SYMBOLS_INVALID")
    ordered = tuple(symbols)
    if (
        any(type(symbol) is not str or not symbol for symbol in ordered)
        or len(set(ordered)) != len(ordered)
        or type(requested_workers) is not int
        or type(requested_workers) is bool
        or requested_workers <= 0
    ):
        _fail("HISTORICAL_SCHEDULER_INPUT_INVALID")
    worker_limit = min(requested_workers, len(ordered))
    phase_name = str(phase or "").strip().upper()

    results: dict[str, dict[str, Any]] = {}
    try:
        governor = memory_governor.MemoryGovernor(
            requested_workers=worker_limit,
            termination_grace_seconds=TERMINATION_GRACE_SECONDS,
        )
        tasks = (
            memory_governor.MemoryTask(
                key=symbol,
                args=(contract, symbol),
            )
            for symbol in ordered
        )
        outcomes = governor.iter_completed(worker, tasks)
        try:
            for outcome in outcomes:
                value = outcome.value
                if (
                    outcome.status != "COMPLETED"
                    or outcome.technical_failure
                    or type(value) is not dict
                    or outcome.task_key not in ordered
                    or outcome.task_key in results
                    or value.get("symbol") != outcome.task_key
                ):
                    _fail(
                        "HISTORICAL_SYMBOL_WORKER_FAILED",
                        phase_name,
                        outcome.task_key,
                        outcome.status,
                        outcome.error_type,
                        outcome.error_message,
                    )
                results[outcome.task_key] = dict(value)
        finally:
            outcomes.close()
    except HistoricalSchedulerError:
        raise
    except Exception as exc:
        raise HistoricalSchedulerError(
            "HISTORICAL_SYMBOL_SCHEDULER_INFRASTRUCTURE_FAILED",
            (phase_name, type(exc).__name__, str(exc)),
        ) from exc

    if set(results) != set(ordered):
        missing = [symbol for symbol in ordered if symbol not in results]
        _fail("HISTORICAL_SYMBOL_ROSTER_INCOMPLETE", *missing[:8])
    return {symbol: results[symbol] for symbol in ordered}


__all__ = [
    "HistoricalSchedulerError",
    "TERMINATION_GRACE_SECONDS",
    "run_symbol_scheduler",
]


DECLARED_SCHEDULER_SCHEMA: Final = "fxpm.declared-symbol-scheduler.v1"
DECLARED_CELL_MIN_FREE_BYTES: Final = 3 * 1024**3 // 2
_DECLARED_CELL_STATUSES: Final = frozenset({
    "COMPLETED", "REOPENED", "TERMINAL_STOP", "FAILED", "NOT_RUN", "UNRESOLVED_CRASH",
})


class DeclaredSymbolSchedulerError(HistoricalSchedulerError):
    """A drained failed dispatch, retaining every cell's reported state."""

    def __init__(self, code: str, details: Sequence[Any], report: Mapping[str, Any]):
        super().__init__(code, details)
        self.report = dict(report)


def _declared_json_copy(value: Any) -> Any:
    import json
    encoded = json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)
    if len(encoded.encode("utf-8")) > 32 * 1024**2:
        _fail("DECLARED_SCHEDULER_DESCRIPTOR_BOUND_EXCEEDED")
    return json.loads(encoded)


def _declared_digest(value: Any) -> bool:
    return type(value) is str and len(value) == 64 and all(c in "0123456789abcdef" for c in value)


def _declared_contract(contract: Mapping[str, Any]) -> tuple[dict[str, Any], dict[str, list[dict[str, Any]]]]:
    from pathlib import Path
    if type(contract) is not dict:
        _fail("DECLARED_SCHEDULER_CONTRACT_INVALID")
    owned = _declared_json_copy(contract)
    if (owned.get("schema_version") != DECLARED_SCHEDULER_SCHEMA
            or not all(_declared_digest(owned.get(k)) for k in ("native_universe_sha256", "config_sha256", "profile_sha256"))
            or type(owned.get("active_config_path")) is not str
            or not owned["active_config_path"]
            or type(owned.get("cells")) is not list or not owned["cells"]):
        _fail("DECLARED_SCHEDULER_CONTRACT_INVALID")
    groups: dict[str, list[dict[str, Any]]] = {}
    roots: list[Path] = []
    runs: set[str] = set()
    for index, row in enumerate(owned["cells"]):
        if (type(row) is not dict or type(row.get("root")) is not str or not row["root"]
                or type(row.get("run_id")) is not str or not row["run_id"]
                or type(row.get("cell")) is not dict
                or set(row["cell"]) != {"cell_id", "symbol", "timeframe", "regime"}
                or not _declared_digest(row["cell"].get("cell_id"))
                or any(type(row["cell"].get(k)) is not str or not row["cell"][k] for k in ("symbol", "timeframe", "regime"))
                or type(row.get("launch_activation_utc")) is not str
                or type(row.get("frozen_refit_clock")) is not dict
                or type(row.get("protected_end_exclusive_utc")) is not str
                or type(row.get("extend_frozen_clock_backward")) is not bool):
            _fail("DECLARED_SCHEDULER_CELL_DESCRIPTOR_INVALID", index)
        policy = row.get("source_access_window_policy")
        if (type(policy) is not dict or set(policy) != {"lead_seconds", "validity_seconds"}
                or type(policy["lead_seconds"]) is not int or policy["lead_seconds"] < 0
                or type(policy["validity_seconds"]) is not int or policy["validity_seconds"] <= 0):
            _fail("DECLARED_SCHEDULER_WINDOW_POLICY_REQUIRED", index)
        root = Path(row["root"]).resolve()
        if row["run_id"] in runs or any(root == prior or root in prior.parents or prior in root.parents for prior in roots):
            _fail("DECLARED_SCHEDULER_CELL_NAMESPACE_COLLISION", index)
        runs.add(row["run_id"])
        roots.append(root)
        row["root"] = str(root)
        row["descriptor_index"] = index
        groups.setdefault(row["cell"]["symbol"], []).append(row)
    return owned, groups


def _declared_cell_row(row: Mapping[str, Any], status: str, reason: str = "", *, pid: int | None = None) -> dict[str, Any]:
    return {"descriptor_index": row["descriptor_index"], "root": row["root"],
        "run_id": row["run_id"], "cell_id": row["cell"]["cell_id"], "symbol": row["cell"]["symbol"],
        "status": status, "reason_code": str(reason)[:256], "worker_pid": pid,
        "precommit_sha256": None, "run_receipt": None, "account_descriptor": None,
        "recorded_before_dispatch": False, "qualification_authority": False,
        "capital_authority": False, "live_authority": False, "order_authority": False}


def _declared_utc_now():
    from datetime import datetime, timezone
    return datetime.now(timezone.utc).replace(microsecond=0)


def _declared_run_descriptor(precommit: Any, row: Mapping[str, Any], body: Mapping[str, Any]) -> dict[str, Any]:
    import hashlib
    import json
    import pm_historical_search_producer as producer
    encoded = json.dumps(body, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False).encode("ascii")
    return {"relative_path": producer._declared_run_path(precommit, row["cell"]["cell_id"]),
        "sha256": hashlib.sha256(encoded).hexdigest(), "size_bytes": len(encoded)}


def _declared_account(context, precommit, process, frozen, config):
    from pathlib import Path
    import pm_forward_oos_replay_producer as forward
    import pm_native_prefix_production as native
    existing = forward.discover_declared_cell_account_artifact(context, precommit=precommit,
        process=process, frozen_pipeline_config=frozen)
    if existing is not None:
        return existing, True
    request = forward.declared_cell_account_native_request(context, precommit=precommit,
        process=process, frozen_pipeline_config=frozen)
    accessed = _declared_utc_now().strftime("%Y-%m-%dT%H:%M:%SZ")
    execution = native.produce_activated_native_prefix(data_dir=Path(config.data_dir), context=context,
        symbol=request["symbol"], timeframe=request["execution_timeframe"], cutoff_utc=request["cutoff_utc"],
        source_accessed_at_utc=accessed, production_contract_required=request["production_contract_required"])
    execution = native.reverify_activated_native_prefix_product(execution, context, require_current_activation=True)
    resolved = forward.declared_cell_account_native_request(context, precommit=precommit,
        process=process, frozen_pipeline_config=frozen, execution_product=execution)
    if (resolved["symbol"] != request["symbol"] or resolved["cutoff_utc"] != request["cutoff_utc"]
            or not resolved["spread_source_timeframe"]):
        _fail("DECLARED_ACCOUNT_NATIVE_REQUEST_CHANGED")
    fine = native.produce_activated_native_prefix(data_dir=Path(config.data_dir), context=context,
        symbol=resolved["symbol"], timeframe=resolved["spread_source_timeframe"], cutoff_utc=resolved["cutoff_utc"],
        source_accessed_at_utc=accessed, production_contract_required=resolved["production_contract_required"])
    fine = native.reverify_activated_native_prefix_product(fine, context, require_current_activation=True)
    artifact = forward.produce_declared_cell_account_artifact(context, precommit=precommit, process=process,
        frozen_pipeline_config=frozen, execution_product=execution, spread_source_product=fine,
        frozen_profile_path=Path(resolved["frozen_profile_path"]))
    return artifact, False


def _declared_progress_or_busy(context, precommit, process, frozen, cell_id):
    import os
    import pm_authority_spool as spool
    import pm_historical_search_producer as producer
    import pm_native_storage as storage
    path = spool._local_path(context.root, f"declared-slots/{precommit.receipt_sha256}/{cell_id}/run.lock")
    # Do not create a lock merely to classify a known lapse or completed cell.
    try:
        spool._reject_reparse_chain(path)
        descriptor = os.open(path, os.O_RDWR | getattr(os, "O_NOFOLLOW", 0))
    except FileNotFoundError:
        return producer.reopen_declared_cell_owner_progress(context, precommit=precommit,
            process=process, cell_id=cell_id, frozen_pipeline_config=frozen), False
    with os.fdopen(descriptor, "r+b", buffering=0) as handle:
        spool._reject_reparse_chain(path)
        current, opened = os.stat(path, follow_symlinks=False), os.fstat(handle.fileno())
        if (current.st_dev,current.st_ino) != (opened.st_dev,opened.st_ino) or opened.st_size != 1:
            _fail("DECLARED_SLOT_RUN_LOCK_INVALID")
        if not storage._try_kernel_lock(handle):
            return None, True
        try:
            return producer.reopen_declared_cell_owner_progress(context, precommit=precommit,
                process=process, cell_id=cell_id, frozen_pipeline_config=frozen), False
        finally:
            storage._release_kernel_lock(handle)


def _declared_dispatch_lease(context):
    """Cooperative scheduler dispatch lease; never ownership of producer callers."""
    from contextlib import contextmanager
    import os
    import stat
    import pm_authority_spool as spool
    import pm_native_storage as storage

    @contextmanager
    def acquired():
        path = spool._local_path(context.root, "declared-scheduler.lock")
        spool._reject_reparse_chain(path)
        descriptor = os.open(path, os.O_RDWR | os.O_CREAT | getattr(os, "O_NOFOLLOW", 0), 0o600)
        with os.fdopen(descriptor, "r+b", buffering=0) as handle:
            spool._reject_reparse_chain(path)
            opened = os.fstat(handle.fileno())
            current = os.stat(path, follow_symlinks=False)
            identity = (opened.st_dev, opened.st_ino)
            if (not stat.S_ISREG(current.st_mode) or not stat.S_ISREG(opened.st_mode)
                    or identity != (current.st_dev, current.st_ino)):
                _fail("DECLARED_SCHEDULER_DISPATCH_LOCK_INVALID")
            if not storage._try_kernel_lock(handle):
                yield False
                return
            try:
                # Acquire before initialising: simultaneous fresh dispatches must
                # not write the byte while another dispatch already owns it.
                spool._reject_reparse_chain(path)
                pinned = os.stat(path, follow_symlinks=False)
                actual = os.fstat(handle.fileno())
                if (not stat.S_ISREG(pinned.st_mode)
                        or identity != (pinned.st_dev, pinned.st_ino)
                        or identity != (actual.st_dev, actual.st_ino)
                        or actual.st_size not in (0, 1)):
                    _fail("DECLARED_SCHEDULER_DISPATCH_LOCK_INVALID")
                if actual.st_size == 0:
                    handle.seek(0)
                    if handle.write(b"\0") != 1:
                        _fail("DECLARED_SCHEDULER_DISPATCH_LOCK_INVALID")
                    handle.flush()
                    os.fsync(handle.fileno())
                spool._reject_reparse_chain(path)
                pinned = os.stat(path, follow_symlinks=False)
                actual = os.fstat(handle.fileno())
                if (identity != (pinned.st_dev, pinned.st_ino)
                        or identity != (actual.st_dev, actual.st_ino)
                        or pinned.st_size != 1 or actual.st_size != 1):
                    _fail("DECLARED_SCHEDULER_DISPATCH_LOCK_INVALID")
                yield True
            finally:
                storage._release_kernel_lock(handle)
    return acquired()


def _declared_disk_free_bytes(root) -> int:
    """Measure the target volume without creating the prospective cell root."""
    from pathlib import Path
    import shutil
    ancestor = Path(root)
    while not ancestor.exists():
        parent = ancestor.parent
        if parent == ancestor:
            _fail("DECLARED_SCHEDULER_DISK_PROBE_TARGET_MISSING")
        ancestor = parent
    if not ancestor.is_dir():
        ancestor = ancestor.parent
    free = shutil.disk_usage(ancestor).free
    if type(free) is not int or free < 0:
        _fail("DECLARED_SCHEDULER_DISK_PROBE_INVALID")
    return free


def _declared_disk_requirement(contract, reserve: int) -> int:
    configured = contract.get("scheduler_disk_min_free_bytes", 0)
    headroom = contract.get("scheduler_disk_headroom_bytes", 0)
    if any(type(value) is not int or value < 0 for value in (configured, headroom, reserve)):
        _fail("DECLARED_SCHEDULER_DISK_POLICY_INVALID")
    return max(configured, reserve) + headroom


def _declared_disk_ready(root, contract, reserve: int) -> bool:
    return _declared_disk_free_bytes(root) >= _declared_disk_requirement(contract, reserve)


def _declared_reopen_process(context, precommit_path):
    """Use bounded stored hints only through the existing public verifier."""
    import pm_authority_spool as spool
    import pm_historical_search_producer as producer
    import pm_production_search_planner as planner
    path = str(precommit_path.relative_to(context.root)).replace("\\", "/")
    record = producer._read_declared_record(context, path)
    if (record is None or type(record[0]) is not dict or record[1] != precommit_path.parent.name):
        _fail("DECLARED_SCHEDULER_PRECOMMIT_RECORD_INVALID")
    hint = record[0].get("search_process_receipt_sha256")
    if not _declared_digest(hint):
        _fail("DECLARED_SCHEDULER_PROCESS_HINT_INVALID")
    manifest_path = spool._manifest_relative_path(planner.PROCESS_SPOOL_SYMBOL, spool.GENERAL_MANIFEST_DOMAIN)
    manifest = producer._read_declared_record(context, manifest_path,
        maximum_bytes=context.limits.max_manifest_bytes)
    if manifest is None:
        _fail("DECLARED_SCHEDULER_PROCESS_RECORD_MISSING")
    receipt = producer._declared_symbol_receipt(context, planner.PROCESS_SPOOL_SYMBOL, manifest[2])
    return planner.reopen_production_search_process(context, spool_receipt=receipt,
        expected_receipt_sha256=hint)


def _declared_run_one_cell(contract: Mapping[str, Any], row: Mapping[str, Any]) -> dict[str, Any]:
    from datetime import datetime, timedelta
    from pathlib import Path
    import os
    import pm_authority_spool as spool
    import pm_book_multiplicity as book
    import pm_historical_anchor_orchestration as orchestration
    import pm_historical_search_producer as producer
    import pm_portfolio_replay as portfolio
    import pm_production_anchor_plan as anchors
    import pm_production_config_binding as binding
    import pm_production_search_planner as planner
    import pm_spread_profile as spread
    result = _declared_cell_row(row, "FAILED", pid=os.getpid())
    try:
        context_root = Path(row["root"])
        if (context_root / spool.CONTEXT_RELATIVE_PATH).exists():
            context = spool.reopen_generation_context(context_root, expected_generation_id=row["run_id"],
                expected_native_universe_sha256=contract["native_universe_sha256"], expected_config_sha256=contract["config_sha256"])
        else:
            if not _declared_disk_ready(context_root, contract, DECLARED_CELL_MIN_FREE_BYTES):
                result.update(status="NOT_RUN", reason_code="DISK_LOW")
                return result
            context = spool.freeze_generation_context(context_root, generation_id=row["run_id"],
                native_universe_sha256=contract["native_universe_sha256"], config_sha256=contract["config_sha256"],
                limits=spool.SpoolLimits(min_free_bytes=DECLARED_CELL_MIN_FREE_BYTES))
        with _declared_dispatch_lease(context) as acquired:
            if not acquired:
                result.update(status="NOT_RUN", reason_code="BUSY")
                return result
            payload, supplied = binding.load_effective_pipeline_config(Path(contract["active_config_path"]))
            frozen = binding.freeze_production_pipeline_config(context, active_config_path=Path(contract["active_config_path"]),
                config_payload=payload, supplied_config=supplied)
            config = frozen.materialize(context)
            records = sorted((context.root / "declared-slots").glob("*/precommit.json"))
            if len(records) > 1:
                _fail("DECLARED_SCHEDULER_MULTIPLE_PRECOMMITS")
            if records:
                process = _declared_reopen_process(context, records[0])
                precommit = orchestration.reopen_declared_slot_owner_run_precommit(context,
                    receipt_sha256=records[0].parent.name, process=process, frozen_pipeline_config=frozen)
            else:
                if not _declared_disk_ready(context.root, contract, context.limits.min_free_bytes):
                    result.update(status="NOT_RUN", reason_code="DISK_LOW")
                    return result
                process = planner.freeze_production_search_process(context, run_id=row["run_id"])
                roster = anchors.derive_declared_owner_slot_roster_v2(launch_activation_utc=row["launch_activation_utc"],
                    frozen_refit_clock=row["frozen_refit_clock"], config_identity_refs={"config_sha256":context.config_sha256,
                        "effective_config_sha256":frozen.effective_config_sha256},
                    extend_frozen_clock_backward=row["extend_frozen_clock_backward"], protected_end_exclusive_utc=row["protected_end_exclusive_utc"])
                profile = spread.load_spread_profile(Path(config.data_dir) / "native" / "terms" / "spread_profile.json")
                if profile.sha256 != contract["profile_sha256"]:
                    _fail("DECLARED_SCHEDULER_PROFILE_CHANGED")
                if not _declared_disk_ready(context.root, contract, context.limits.min_free_bytes):
                    result.update(status="NOT_RUN", reason_code="DISK_LOW")
                    return result
                now = _declared_utc_now()
                policy = row["source_access_window_policy"]
                window = {"start_utc":(now-timedelta(seconds=policy["lead_seconds"])).strftime("%Y-%m-%dT%H:%M:%SZ"),
                    "end_exclusive_utc":(now+timedelta(seconds=policy["validity_seconds"])).strftime("%Y-%m-%dT%H:%M:%SZ")}
                precommit = orchestration.precommit_declared_slot_owner_run_v2(context, run_id=row["run_id"], process=process,
                    slot_roster=roster, cell_roster=[row["cell"]], frozen_pipeline_config=frozen,
                    spread_profile_identity={"profile_sha256":profile.sha256}, source_access_window_utc=window,
                    launch_activation_utc=row["launch_activation_utc"])
            parent = precommit.to_dict()
            if (parent.get("schema_version") != orchestration.DECLARED_SLOT_PRECOMMIT_SCHEMA_V2
                    or type(parent.get("slot_roster")) is not dict
                    or parent["slot_roster"].get("schema_version") != anchors.DECLARED_OWNER_SLOT_ROSTER_V2_SCHEMA):
                _fail("DECLARED_SCHEDULER_PRECOMMIT_V2_REQUIRED")
            anchors.verify_declared_owner_slot_roster(parent["slot_roster"],
                launch_activation_utc=row["launch_activation_utc"],
                frozen_refit_clock=row["frozen_refit_clock"],
                config_identity_refs={"config_sha256":context.config_sha256,
                    "effective_config_sha256":frozen.effective_config_sha256},
                extend_frozen_clock_backward=row["extend_frozen_clock_backward"],
                protected_end_exclusive_utc=row["protected_end_exclusive_utc"])
            if (parent["cells"] != [row["cell"]] or parent["run_id"] != row["run_id"]
                    or parent["spread_profile_identity"]["profile_sha256"] != contract["profile_sha256"]):
                _fail("DECLARED_SCHEDULER_PRECOMMIT_BINDING_MISMATCH")
            result["precommit_sha256"] = precommit.receipt_sha256
            progress, busy = _declared_progress_or_busy(context, precommit, process, frozen, row["cell"]["cell_id"])
            if busy:
                result.update(status="NOT_RUN", reason_code="BUSY")
                return result
            recorded = progress.run_receipt is not None or progress.terminal_stop or progress.first_uncommitted_slot is None
            result["recorded_before_dispatch"] = recorded
            if progress.run_receipt is not None:
                result["run_receipt"] = _declared_run_descriptor(precommit, row, progress.run_receipt)
            if progress.terminal_stop:
                result.update(status="TERMINAL_STOP", reason_code="RECORDED_TERMINAL_STOP")
                return result
            existing = None
            if recorded:
                import pm_forward_oos_replay_producer as forward
                existing = forward.discover_declared_cell_account_artifact(context, precommit=precommit,
                    process=process, frozen_pipeline_config=frozen)
                if existing is not None:
                    result.update(status="REOPENED", account_descriptor=dict(existing.descriptor))
                    return result
            if not _declared_disk_ready(context.root, contract, context.limits.min_free_bytes):
                result.update(status="NOT_RUN", reason_code="DISK_LOW")
                return result
            # Any unfinished work, including account production after all slots,
            # needs the existing effective access window and native snapshot.
            access_slot = progress.first_uncommitted_slot or parent["slot_roster"]["slots"][-1]
            window, _ = orchestration._declared_slot_effective_access(context, precommit, parent, access_slot)
            now = _declared_utc_now()
            if not datetime.fromisoformat(window["start_utc"].replace("Z", "+00:00")) <= now < datetime.fromisoformat(window["end_exclusive_utc"].replace("Z", "+00:00")):
                result.update(status="NOT_RUN", reason_code="WINDOW_EXPIRED")
                return result
            snapshot, errors = portfolio.active_native_portfolio_contract(Path(config.data_dir), book.PINNED_SYMBOL_ORDER, timeframes=book.TIMEFRAME_ORDER)
            if orchestration._declared_slot_native_snapshot_status(context, snapshot, errors) != "MATCH":
                result.update(status="NOT_RUN", reason_code="NATIVE_DRIFT")
                return result
            if progress.run_receipt is None:
                if not _declared_disk_ready(context.root, contract, context.limits.min_free_bytes):
                    result.update(status="NOT_RUN", reason_code="DISK_LOW")
                    return result
                try:
                    run = producer.execute_declared_cell_owner_slots(context=context, precommit=precommit, process=process,
                        frozen_pipeline_config=frozen, cell_id=row["cell"]["cell_id"])
                except Exception as exc:
                    if getattr(exc, "code", "") == "DECLARED_SLOT_RUN_ACTIVE":
                        result.update(status="NOT_RUN", reason_code="BUSY")
                        return result
                    raise
                run_body = run.to_dict()
                result["run_receipt"] = _declared_run_descriptor(precommit, row, run_body)
                if run_body["terminal_stop"]:
                    result.update(status="TERMINAL_STOP", reason_code="NEW_TERMINAL_STOP", recorded_before_dispatch=False)
                    return result
            if not _declared_disk_ready(context.root, contract, context.limits.min_free_bytes):
                result.update(status="NOT_RUN", reason_code="DISK_LOW")
                return result
            artifact, reopened = _declared_account(context, precommit, process, frozen, config)
            result.update(status="REOPENED" if reopened else "COMPLETED", account_descriptor=dict(artifact.descriptor))
            return result
    except Exception as exc:
        if spool.is_resource_capacity_error(exc):
            result.update(status="NOT_RUN", reason_code="DISK_LOW")
        elif getattr(exc,"code","") == "DECLARED_SLOT_RUN_ACTIVE":
            result.update(status="NOT_RUN", reason_code="BUSY")
        else:
            result.update(status="FAILED", reason_code=str(getattr(exc,"code",type(exc).__name__))[:256])
        return result


def declared_symbol_worker(contract: Mapping[str, Any], symbol: str) -> dict[str, Any]:
    """One spawned symbol job; every capability is reconstructed in this child."""
    import gc
    import os
    from pm_core import FeatureComputer
    rows: list[dict[str, Any]] = []
    stopped = False
    stop_reason = "PRIOR_CELL_FAILURE"
    for cell in contract["cells"]:
        if stopped:
            rows.append(_declared_cell_row(cell, "NOT_RUN", stop_reason))
            continue
        if cell["cell"]["symbol"] != symbol:
            _fail("DECLARED_SCHEDULER_SYMBOL_BINDING_MISMATCH")
        try:
            row = _declared_run_one_cell(contract, cell)
        except Exception as exc:
            row = _declared_cell_row(cell, "FAILED", str(getattr(exc, "code", type(exc).__name__)), pid=os.getpid())
            stopped = True
        rows.append(row)
        if row["status"] == "NOT_RUN" and row["reason_code"] == "DISK_LOW":
            stop_reason = "DISK_LOW"
            stopped = True
        stopped = stopped or row["status"] in {"FAILED", "UNRESOLVED_CRASH"} or (row["status"] == "TERMINAL_STOP" and not row["recorded_before_dispatch"])
        if not stopped:
            try:
                FeatureComputer.clear_cache()
                gc.collect()
            except Exception as exc:
                row["cleanup_failure"] = type(exc).__name__
                stopped = True
    return {"schema_version":DECLARED_SCHEDULER_SCHEMA, "symbol":symbol, "cells":rows, "closes_admission":stopped}


def _declared_validate_result(value: Any, cells: Sequence[Mapping[str, Any]], symbol: str, *, worker_pid: int) -> dict[str, Any]:
    import pm_authority_spool as spool
    owned = _declared_json_copy(value)
    if (type(owned) is not dict or owned.get("schema_version") != DECLARED_SCHEDULER_SCHEMA
            or owned.get("symbol") != symbol or type(owned.get("closes_admission")) is not bool
            or type(owned.get("cells")) is not list or len(owned["cells"]) != len(cells)
            or type(worker_pid) is not int or worker_pid <= 0):
        _fail("DECLARED_SCHEDULER_WORKER_RESULT_INVALID")
    for row, expected in zip(owned["cells"], cells, strict=True):
        if (type(row) is not dict or row.get("status") not in _DECLARED_CELL_STATUSES
                or any(row.get(k) != _declared_cell_row(expected,"NOT_RUN")[k] for k in ("descriptor_index","root","run_id","cell_id","symbol"))
                or type(row.get("recorded_before_dispatch")) is not bool
                or type(row.get("reason_code")) is not str or len(row["reason_code"]) > 256
                or any(row.get(k) is not False for k in ("qualification_authority","capital_authority","live_authority","order_authority"))):
            _fail("DECLARED_SCHEDULER_CELL_RESULT_INVALID")
        pid = row.get("worker_pid")
        if not (type(pid) is int and pid == worker_pid) and not (row["status"] == "NOT_RUN" and pid is None):
            _fail("DECLARED_SCHEDULER_CELL_PID_INVALID")
        precommit = row.get("precommit_sha256")
        if precommit is not None and not _declared_digest(precommit):
            _fail("DECLARED_SCHEDULER_CELL_PRECOMMIT_INVALID")
        receipt = row.get("run_receipt")
        if receipt is not None:
            if (type(receipt) is not dict or set(receipt) != {"relative_path","sha256","size_bytes"}
                    or not _declared_digest(precommit) or not _declared_digest(receipt.get("sha256"))
                    or type(receipt.get("size_bytes")) is not int or receipt["size_bytes"] <= 0
                    or receipt["relative_path"] != f"declared-slots/{precommit}/{row['cell_id']}/run-receipt.json"):
                _fail("DECLARED_SCHEDULER_RUN_RECEIPT_INVALID")
        account = row.get("account_descriptor")
        if account is not None:
            account = spool._validate_descriptor(account, content_addressed=True)
            if account["format"] != spool.CANONICAL_JSON_FORMAT:
                _fail("DECLARED_SCHEDULER_ACCOUNT_DESCRIPTOR_INVALID")
        if row["status"] in {"COMPLETED","REOPENED"} and (not _declared_digest(precommit) or receipt is None or account is None):
            _fail("DECLARED_SCHEDULER_COMPLETED_BINDINGS_MISSING")
        if "cleanup_failure" in row and (type(row["cleanup_failure"]) is not str or not row["cleanup_failure"] or len(row["cleanup_failure"]) > 256):
            _fail("DECLARED_SCHEDULER_CLEANUP_RESULT_INVALID")
    return owned


def _declared_confirm_receipt(contract: Mapping[str, Any], row: Mapping[str, Any]) -> bool | None:
    from pathlib import Path
    import pm_authority_spool as spool
    descriptor = row.get("run_receipt")
    if descriptor is None:
        return None
    if (type(descriptor) is not dict or set(descriptor) != {"relative_path","sha256","size_bytes"}
            or not _declared_digest(descriptor["sha256"]) or not _declared_digest(row.get("precommit_sha256"))
            or descriptor["relative_path"] != f"declared-slots/{row['precommit_sha256']}/{row['cell_id']}/run-receipt.json"):
        _fail("DECLARED_SCHEDULER_RUN_RECEIPT_INVALID")
    context = spool.reopen_generation_context(Path(row["root"]), expected_generation_id=row["run_id"],
        expected_native_universe_sha256=contract["native_universe_sha256"], expected_config_sha256=contract["config_sha256"])
    digest, _, _ = spool._stream_file(context.root, descriptor["relative_path"], maximum_bytes=context.limits.max_json_bytes,
        expected_size=descriptor["size_bytes"], collect=False)
    if digest != descriptor["sha256"]:
        _fail("DECLARED_SCHEDULER_STORED_RUN_RECEIPT_CHANGED")
    return True


def run_declared_symbol_scheduler(
    worker: Callable[[Mapping[str, Any], str], Mapping[str, Any]] = declared_symbol_worker,
    *, contract: Mapping[str, Any], telemetry_path: Any, requested_workers: int = 0,
    soft_ram_hold: bool = True, soft_ram_target_fraction: float = 0.8,
    telemetry_interval_seconds: float = 1.0, telemetry_max_bytes: int = 1024**2,
    disk_min_free_bytes: int = 0, disk_headroom_bytes: int = 0,
) -> dict[str, Any]:
    """Drain one symbol-grouped dispatch, retaining exactly one row per cell."""
    from dataclasses import asdict, is_dataclass
    import json
    from pathlib import Path
    if not callable(worker):
        _fail("DECLARED_SCHEDULER_WORKER_INVALID")
    owned, groups = _declared_contract(contract)
    owned.update(scheduler_disk_min_free_bytes=disk_min_free_bytes,
        scheduler_disk_headroom_bytes=disk_headroom_bytes)
    _declared_disk_requirement(owned, DECLARED_CELL_MIN_FREE_BYTES)
    if type(telemetry_max_bytes) is not int or telemetry_max_bytes < 4096:
        _fail("DECLARED_SCHEDULER_TELEMETRY_BOUND_INVALID")
    destination = Path(telemetry_path)
    if destination.suffix != ".jsonl":
        _fail("DECLARED_SCHEDULER_TELEMETRY_PATH_INVALID")
    worker_limit = memory_governor.recommended_worker_count(requested_workers, len(groups))
    governor = memory_governor.MemoryGovernor(requested_workers=worker_limit, termination_grace_seconds=TERMINATION_GRACE_SECONDS)
    policy = memory_governor.GovernorResourcePolicy(soft_ram_hold=soft_ram_hold,
        soft_ram_target_fraction=soft_ram_target_fraction, sample_interval_seconds=telemetry_interval_seconds, disk_path=str(destination.parent))
    gate = {"open":True, "reason":"ADMISSION_CLOSED"}
    pending_group = {"cells":None}
    errors: list[str] = []
    rows: dict[int,dict[str,Any]] = {}
    seen: set[str] = set()
    def retain_error(message):
        gate["open"] = False
        if len(errors) < 16:
            errors.append(str(message)[:512])
    def telemetry(sample):
        try:
            body = asdict(sample) if is_dataclass(sample) else dict(sample)
            line = (json.dumps(body, sort_keys=True, separators=(",", ":"), allow_nan=False)+"\n").encode("utf-8")
            if len(line) > telemetry_max_bytes:
                raise ValueError("telemetry sample exceeds bound")
            destination.parent.mkdir(parents=True, exist_ok=True)
            mode = "ab" if destination.exists() and destination.stat().st_size+len(line) <= telemetry_max_bytes else "wb"
            with destination.open(mode) as stream:
                stream.write(line)
        except Exception as exc:
            retain_error("TELEMETRY:"+type(exc).__name__)
    def tasks():
        for symbol, cells in groups.items():
            pending_group["cells"] = cells
            yield memory_governor.MemoryTask(key=symbol, args=({**owned,"cells":cells},symbol))
        pending_group["cells"] = None
    def admission_open():
        if not gate["open"]:
            return False
        try:
            if pending_group["cells"] is not None and any(
                    not _declared_disk_ready(cell["root"], owned, DECLARED_CELL_MIN_FREE_BYTES)
                    for cell in pending_group["cells"]):
                gate.update(open=False, reason="DISK_LOW")
        except Exception as exc:
            gate["reason"] = "DISK_PROBE_ERROR"
            retain_error("DISK_PROBE:"+type(exc).__name__)
        return gate["open"]
    outcomes = governor.iter_completed(worker, tasks(), admission_gate=admission_open,
        resource_policy=policy, telemetry_callback=telemetry)
    try:
        for outcome in outcomes:
            symbol = outcome.task_key
            try:
                if symbol not in groups or symbol in seen:
                    _fail("DECLARED_SCHEDULER_OUTCOME_IDENTITY_INVALID")
                seen.add(symbol)
                if outcome.status == "NOT_RUN":
                    result_rows = [_declared_cell_row(cell,"NOT_RUN",gate["reason"]) for cell in groups[symbol]]
                elif outcome.status != "COMPLETED" or outcome.technical_failure:
                    retain_error("WORKER:"+symbol+":"+outcome.status)
                    result_rows = [_declared_cell_row(cell,"UNRESOLVED_CRASH",outcome.error_type or outcome.status,pid=outcome.worker_pid or None) for cell in groups[symbol]]
                else:
                    value = _declared_validate_result(outcome.value, groups[symbol], symbol, worker_pid=outcome.worker_pid)
                    result_rows = value["cells"]
                    disk_deferred = any(row["status"] == "NOT_RUN" and row["reason_code"] == "DISK_LOW"
                        for row in result_rows)
                    failed = any(row["status"] in {"FAILED","UNRESOLVED_CRASH"}
                        or row.get("cleanup_failure") or (row["status"]=="TERMINAL_STOP" and not row["recorded_before_dispatch"])
                        for row in result_rows)
                    if failed or (value["closes_admission"] and not disk_deferred):
                        retain_error("CELL_FAILURE:"+symbol)
                    elif disk_deferred:
                        gate.update(open=False, reason="DISK_LOW")
                for row in result_rows:
                    rows[row["descriptor_index"]] = row
            except Exception as exc:
                retain_error("RESULT:"+symbol+":"+type(exc).__name__)
                if symbol in groups:
                    for cell in groups[symbol]:
                        rows.setdefault(cell["descriptor_index"],_declared_cell_row(cell,"UNRESOLVED_CRASH","INVALID_WORKER_RESULT",pid=outcome.worker_pid or None))
    except Exception as exc:
        retain_error("GOVERNOR:"+type(exc).__name__)
    except BaseException:
        outcomes.close()
        raise
    # The iterator is exhausted: no in-flight process can still change a receipt.
    for cell in owned["cells"]:
        index=cell["descriptor_index"]
        if index not in rows:
            retain_error("MISSING_CELL:"+str(index))
            rows[index]=_declared_cell_row(cell,"UNRESOLVED_CRASH","MISSING_WORKER_OUTCOME")
        try:
            rows[index]["run_receipt_confirmed"]=_declared_confirm_receipt(owned,rows[index])
        except Exception as exc:
            rows[index]["run_receipt_confirmed"]=False
            retain_error("RECEIPT:"+str(index)+":"+type(exc).__name__)
    stats=governor.resource_stats
    report={"schema_version":DECLARED_SCHEDULER_SCHEMA,"status":"FAILED" if errors else "COMPLETED",
        "cells":[rows[index] for index in range(len(owned["cells"]))],"requested_workers":requested_workers,
        "worker_limit":worker_limit,"errors":errors,"admission_deferral_reason":gate["reason"] if not gate["open"] else None,"resource_stats":asdict(stats) if is_dataclass(stats) else dict(stats),
        "qualification_authority":False,"capital_authority":False,"live_authority":False,"order_authority":False}
    if errors:
        raise DeclaredSymbolSchedulerError("DECLARED_SYMBOL_DISPATCH_FAILED",errors,report)
    return report


__all__ += ["DECLARED_SCHEDULER_SCHEMA", "DECLARED_CELL_MIN_FREE_BYTES", "DeclaredSymbolSchedulerError",
    "declared_symbol_worker", "run_declared_symbol_scheduler"]
