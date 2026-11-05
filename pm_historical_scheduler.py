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
DECLARED_CELL_MIN_FREE_BYTES: Final = 5 * 1024**3
# E4193 binds these limits in full13_research_bindings_002.draft.json.
_DECLARED_NATIVE_FINALIZATION_LIMITS: Final = {
    "max_artifact_bytes": 1024**3, "max_symbol_bytes": 8 * 1024**3,
    "max_generation_bytes": 8 * 1024**3, "max_artifacts_per_symbol": 100000,
    "max_manifest_bytes": 128 * 1024**2, "max_json_bytes": 64 * 1024**2,
    "stream_chunk_bytes": 1024**2, "min_free_bytes": 16 * 1024**3,
}
DECLARED_M5_DEVELOPMENT_SCHEDULER_SCHEMA: Final = "fxpm.declared-m5-development-symbol-scheduler.v1"
_DECLARED_CELL_STATUSES: Final = frozenset({
    "COMPLETED", "REOPENED", "TERMINAL_STOP", "FAILED", "NOT_RUN", "UNRESOLVED_CRASH", "ARCHIVED",
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
    native_outputs = []
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
        supplied_root = Path(row["root"])
        if "native_finalization" in row:
            import pm_authority_spool as spool

            if ("m5_development" in row or not supplied_root.is_absolute() or ".." in supplied_root.parts
                    or any(part in {"real_control", "STOP", ".git"} for part in supplied_root.parts)):
                _fail("DECLARED_NATIVE_FINALIZATION_PATH_INVALID")
            spool._reject_reparse_chain(supplied_root)
        root = supplied_root.resolve()
        if row["run_id"] in runs or any(root == prior or root in prior.parents or prior in root.parents for prior in roots):
            _fail("DECLARED_SCHEDULER_CELL_NAMESPACE_COLLISION", index)
        runs.add(row["run_id"])
        roots.append(root)
        row["root"] = str(root)
        row["descriptor_index"] = index
        if "native_finalization" in row:
            policy = row["native_finalization"]
            grid_root, fence_path, receipt_path = _declared_native_finalization_paths(policy)
            policy.update(end_grid_root=str(grid_root), source_fence_path=str(fence_path))
            policy["retirement"]["receipt_path"] = str(receipt_path)
            native_outputs.extend((grid_root, fence_path, receipt_path))
        groups.setdefault(row["cell"]["symbol"], []).append(row)
    for ordinal, path in enumerate(native_outputs):
        if any(path == other or path in other.parents or other in path.parents
               for other in (*roots, *native_outputs[:ordinal])):
            _fail("DECLARED_NATIVE_FINALIZATION_PATH_OVERLAP")
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
    return max(configured, reserve, DECLARED_CELL_MIN_FREE_BYTES) + headroom


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


def _declared_native_finalization_paths(policy):
    from pathlib import Path
    import pm_authority_spool as spool

    if (type(policy) is not dict or set(policy) != {"end_grid_root", "source_fence_path", "retirement"}
            or type(policy["retirement"]) is not dict
            or set(policy["retirement"]) != {"receipt_path", "max_paths", "max_nodes", "max_bytes"}
            or any(type(policy["retirement"][key]) is not int or policy["retirement"][key] <= 0
                   for key in ("max_paths", "max_nodes", "max_bytes"))):
        _fail("DECLARED_NATIVE_FINALIZATION_POLICY_INVALID")
    paths = []
    for value in (policy["end_grid_root"], policy["source_fence_path"], policy["retirement"]["receipt_path"]):
        if (type(value) is not str or not Path(value).is_absolute() or ".." in Path(value).parts
                or any(part in {"real_control", "STOP", ".git"} for part in Path(value).parts)):
            _fail("DECLARED_NATIVE_FINALIZATION_PATH_INVALID")
        path = Path(value)
        spool._reject_reparse_chain(path)
        paths.append(path.resolve())
    return tuple(paths)


def _declared_native_cell_contract_sha256(contract, row, limits):
    from dataclasses import asdict

    return _declared_m5_hash({"schema_version": contract["schema_version"],
        "native_universe_sha256": contract["native_universe_sha256"], "config_sha256": contract["config_sha256"],
        "profile_sha256": contract["profile_sha256"], "active_config_path": contract["active_config_path"],
        "spool_limits": asdict(limits),
        "cell": {key: value for key, value in row.items() if key != "descriptor_index"}})


def _declared_native_finalization_preflight(contract, row, config, limits, *, fresh):
    from pathlib import Path
    import sys
    import pm_authority_spool as spool
    import pm_end_grid as grid

    policy = row["native_finalization"]
    grid_root, fence_path, receipt_path = _declared_native_finalization_paths(policy)
    source_modules = ("pm_authority_spool_quota", "pm_historical_search_producer", "pm_historical_production_adapter",
        "pm_authoritative_search_execution", "pm_forward_oos_replay_producer", "pm_causal_evidence_producer",
        "pm_historical_anchor_orchestration", "pm_production_search_planner", "pm_production_config_binding",
        "pm_training_horizon", "pm_winner_raw_evidence", "pm_core", "pm_native_prefix_production", "pm_spread_profile")
    source_paths = tuple(Path(file).resolve() for name in source_modules
        if type(file := getattr(sys.modules.get(name), "__file__", None)) is str)
    protected = (Path(config.data_dir).resolve(), Path(contract["active_config_path"]).resolve(),
                 Path(__file__).resolve(), Path(spool.__file__).resolve(), Path(grid.__file__).resolve(), *source_paths)
    root = Path(row["root"])
    if (any(part in {"real_control", "STOP", ".git"} for part in root.parts)
            or any(path == held or path in held.parents or held in path.parents
                   for path in (root, grid_root, fence_path, receipt_path) for held in protected)
            or policy["retirement"]["max_bytes"] > limits.max_generation_bytes):
        _fail("DECLARED_NATIVE_FINALIZATION_PATH_OVERLAP")
    spool._reject_reparse_chain(root)
    grid._grid_root(grid_root)
    if not fence_path.parent.is_dir() or not receipt_path.parent.is_dir():
        _fail("DECLARED_NATIVE_FINALIZATION_PARENT_REQUIRED")
    if fresh and ((root.exists() and (not root.is_dir() or any(root.iterdir())))
                  or any(grid_root.iterdir()) or fence_path.exists() or receipt_path.exists()):
        _fail("DECLARED_NATIVE_FINALIZATION_FRESH_PATH_REQUIRED")
    return (*protected, grid_root, fence_path)


def _declared_native_archive_binding(record, row, digest):
    policy = row["native_finalization"]
    if (type(record) is not dict or record.get("status") != "ARCHIVED_RECORD_ONLY"
            or record.get("generation_id") != row["run_id"] or record.get("cell_id") != row["cell"]["cell_id"]
            or record.get("cell_contract_sha256") != digest
            or record.get("retirement_receipt_path") != policy["retirement"]["receipt_path"]
            or type(record.get("source_fence")) is not dict
            or record["source_fence"].get("path") != policy["source_fence_path"]):
        _fail("DECLARED_NATIVE_ARCHIVE_CONTRACT_CHANGED")


def _declared_native_completed_records(context, *, row, precommit, process, artifact, run_body, profile_sha):
    """Join original saved bytes after the native ACCOUNT producer or verifier returns."""
    import pm_authority_spool as spool
    import pm_causal_evidence_producer as causal
    import pm_historical_anchor_orchestration as orchestration
    import pm_historical_search_producer as producer

    parent = precommit.to_dict()
    slots = orchestration.declared_slot_selected_slots(parent)
    cell_id, symbol = row["cell"]["cell_id"], row["cell"]["symbol"]
    if (len(slots) != 13 or type(run_body) is not dict
            or run_body.get("schema_version") != producer.DECLARED_CELL_RUN_SCHEMA
            or run_body.get("terminal_stop") is not False or "terminal_failure" in run_body
            or run_body.get("completed_slots") != len(slots) or run_body.get("declared_slots") != len(slots)
            or run_body.get("generation_context_sha256") != context.context_sha256
            or run_body.get("precommit_sha256") != precommit.receipt_sha256
            or run_body.get("cell_id") != cell_id):
        _fail("DECLARED_NATIVE_FINALIZATION_SAVED_RUN_REQUIRED")
    run = _declared_run_descriptor(precommit, row, run_body)
    saved = producer._read_declared_record(context, run["relative_path"])
    if saved is None or saved[0] != run_body or saved[1] != run["sha256"] or saved[2]["size_bytes"] != run["size_bytes"]:
        _fail("DECLARED_NATIVE_FINALIZATION_SAVED_RUN_REQUIRED")
    account = causal._read_descriptor_json(context, artifact.descriptor, code="DECLARED_CELL_RAW_ACCOUNT")
    causal._verify_declared_cell_account_manifest(context, account, artifact.descriptor)
    if (account.get("progress_status") != "COMPLETE" or type(account.get("account")) is not dict
            or account.get("generation_context_sha256") != context.context_sha256
            or account.get("declared_precommit_sha256") != precommit.receipt_sha256
            or account.get("search_process_receipt_sha256") != process.receipt_sha256
            or account.get("cell") != row["cell"]
            or any(account.get(key) is not False for key in
                   ("qualification_authority", "capital_authority", "live_authority", "order_authority"))):
        _fail("DECLARED_NATIVE_FINALIZATION_ACCOUNT_BINDING_INVALID")
    entries, summaries = account["declared_commit_artifacts"], account["slot_summaries"]
    manifest = producer._read_declared_record(context,
        spool._manifest_relative_path(symbol, spool.GENERAL_MANIFEST_DOMAIN),
        maximum_bytes=context.limits.max_manifest_bytes)
    if (type(entries) is not list or type(summaries) is not list or len(entries) != len(slots)
            or len(summaries) != len(slots) or manifest is None
            or producer._declared_symbol_receipt(context, symbol, manifest[2]) != run_body.get("symbol_receipt")
            or manifest[0].get("artifacts") != entries):
        _fail("DECLARED_NATIVE_FINALIZATION_RUN_MANIFEST_INVALID")
    proofs, schedule, outcomes, owners = {}, [], [], []
    previous = None
    for ordinal, (slot, entry, summary) in enumerate(zip(slots, entries, summaries, strict=True), 1):
        stored = producer._read_declared_commit(context, producer._declared_commit_path(precommit, cell_id, ordinal))
        if stored is None:
            _fail("DECLARED_NATIVE_FINALIZATION_SLOT_PROOF_REQUIRED")
        commit, commit_sha, descriptor = stored
        key = slot["slot_id"]
        if (slot["ordinal"] != ordinal or key in proofs
                or entry != producer._declared_commit_artifact(slot, descriptor)
                or summary != producer._declared_slot_summary(commit, commit_sha)
                or commit.get("schema_version") != producer.DECLARED_SLOT_COMMIT_SCHEMA
                or commit.get("generation_context_sha256") != context.context_sha256
                or commit.get("precommit_sha256") != precommit.receipt_sha256
                or commit.get("previous_commit_sha256") != previous
                or commit.get("cell_id") != cell_id or commit.get("slot_id") != key or commit.get("ordinal") != ordinal):
            _fail("DECLARED_NATIVE_FINALIZATION_SLOT_PROOF_REQUIRED")
        completion = commit["completion"]
        status, owner = completion["status"], completion.get("owner")
        if ((status == "OWNER" and (type(owner) is not dict or owner.get("schema_version") != "fxpm.declared-slot-owner.v1"
                                   or owner.get("cell_id") != cell_id or owner.get("slot_id") != key))
                or (status == "NO_TRADE" and owner is not None) or status not in ("OWNER", "NO_TRADE")):
            _fail("DECLARED_NATIVE_FINALIZATION_SLOT_PROOF_REQUIRED")
        if owner is not None:
            owners.append(owner)
        outcomes.append({"slot_id": key, "status": status})
        schedule.append({"slot_id": key, "ordinal": ordinal, "start_utc": slot["activation_utc"],
            "end_exclusive_utc": slot["end_exclusive_utc"], "status": status,
            "owner_id": None if owner is None else owner["owner_id"]})
        proofs[key] = _declared_native_grid_proof(context, row=row, slot=slot, commit=commit, profile_sha=profile_sha)
        previous = commit_sha
    if (run_body.get("last_commit_sha256") != previous or run_body.get("slot_commits") != outcomes
            or account.get("slot_schedule") != schedule
            or [entry["owner"] for entry in account["owner_records"]] != owners):
        _fail("DECLARED_NATIVE_FINALIZATION_RUN_BINDING_INVALID")
    return parent, slots, proofs, summaries, run


def _declared_native_grid_proof(context, *, row, slot, commit, profile_sha):
    import hashlib
    import pm_historical_production_adapter as adapter

    cell, completion = row["cell"], commit["completion"]
    if completion.get("frozen_plan_receipt") is None:
        _fail("DECLARED_NATIVE_FINALIZATION_TIER_PROOF_REQUIRED")
    request = completion["request_body"]
    cutoff = request["anchor"]["selection_cutoff_utc"]
    source_entry = completion["source_session_entry"]
    entries = {entry["artifact_id"]: entry for entry in commit["artifact_roster"]}
    if (len(entries) != len(commit["artifact_roster"]) or source_entry not in entries.values()
            or _declared_m5_utc(cutoff) != _declared_m5_utc(slot["native_cutoff_utc"])):
        _fail("DECLARED_NATIVE_FINALIZATION_SOURCE_PROOF_REQUIRED")
    session = adapter._reopen_ascii_json_entry(context, source_entry,
        expected_type="declared_slot_source_session", code="DECLARED_NATIVE_FINALIZATION_SOURCE_PROOF_REQUIRED")
    index_descriptor = session["native_prefix_index_descriptor"]
    index = adapter._reopen_worker_json_descriptor(context, index_descriptor,
        maximum_bytes=context.limits.max_json_bytes, code="DECLARED_NATIVE_FINALIZATION_SOURCE_PROOF_REQUIRED")
    source = adapter._rebuild_source_decision(completion["source_group"])
    source_body = source.to_dict()
    activation, native, continuity = index["activation"], index["source"], index["continuity"]
    view = {"schema_version": "fxpm.broker-native-end-grid-view.v1", "symbol": cell["symbol"],
        "timeframe": cell["timeframe"], "cutoff_utc": cutoff, "activation_id": activation["activation_id"],
        "activation_manifest_sha256": activation["manifest_sha256"], "activation_entry_sha256": activation["entry_sha256"],
        "native_bars_sha256": native["bars_sha256"], "continuity_policy_sha256": continuity["policy"]["sha256"],
        "continuity_geometry_sha256": continuity["geometry_sha256"]}
    if (session.get("request_sha256") != completion["request_sha256"]
            or session.get("selection_cutoff_utc") != cutoff or index["cutoff_utc"] != cutoff
            or session.get("source_state") != "AVAILABLE" or index["source_state"] != "AVAILABLE"
            or session.get("source_accessed_at_utc") != index["source_accessed_at_utc"]
            or session.get("native_prefix_index_sha256") != index_descriptor["sha256"]
            or session.get("source_group_decision_sha256") != source.receipt_sha256
            or source_body.get("source_group_evidence_sha256") != index_descriptor["sha256"]
            or any(index[key] != cell[key] or source_body[key] != cell[key] for key in ("symbol", "timeframe"))
            or any(source_body[key] != view[key] for key in
                   ("activation_id", "activation_manifest_sha256", "activation_entry_sha256"))
            or source_body["native_source_sha256"] != view["native_bars_sha256"]
            or not all(_declared_digest(view[key]) for key in view if key.endswith("_sha256"))):
        _fail("DECLARED_NATIVE_FINALIZATION_SOURCE_PROOF_REQUIRED")
    prefix = f"{request['artifact_namespace']}/winner-search/{cell['timeframe']}/{cell['regime']}/frozen/"
    receipt, pair_entry, commitment_entry = (entries.get(prefix + name) for name in
        ("receipt", "frozen_tier_pair_contract", "precommitment"))
    if (receipt is None or receipt["artifact_type"] != "winner_frozen_search_plan_receipt"
            or receipt["descriptor"] != completion.get("frozen_plan_receipt")
            or pair_entry is None or pair_entry["artifact_type"] != "winner_frozen_tier_pair_contract"
            or commitment_entry is None or commitment_entry["artifact_type"] != "winner_precommitment"):
        _fail("DECLARED_NATIVE_FINALIZATION_TIER_PROOF_REQUIRED")
    code = "DECLARED_NATIVE_FINALIZATION_TIER_PROOF_REQUIRED"
    pair, pair_bytes = adapter._reopen_worker_ascii_json_descriptor(context, pair_entry["descriptor"],
        maximum_bytes=context.limits.max_json_bytes, code=code)
    commitment, _ = adapter._reopen_worker_ascii_json_descriptor(context, commitment_entry["descriptor"],
        maximum_bytes=context.limits.max_json_bytes, code=code)
    source_contract = source_body["source_contract_sha256"]
    levels = pair["tiers"]
    if (not _declared_digest(source_contract) or commitment.get("source_contract_sha256") != source_contract
            or commitment.get("frozen_tier_pair_contract") != pair
            or commitment.get("frozen_tier_pair_contract_sha256") != hashlib.sha256(pair_bytes).hexdigest()
            or [level["tier"] for level in levels] != ["P75", "P95"]
            or any(level["frozen_spread_tier"]["profile_sha256"] != profile_sha for level in levels)):
        _fail(code)
    owner = completion.get("owner")
    if owner is None:
        outcome, reason, tier = "NO_TRADE", completion["reason_code"], "P75"
        evidence = {"semantics": "BOUND_BASELINE_DESCRIPTOR_NO_SELECTED_WINNER", "tier": tier,
            "bound_baseline_descriptor": levels[0]["frozen_spread_tier"]}
        family = configuration = None
    else:
        outcome, reason, tier = "WINNER", "DECLARED_NATIVE_VERIFIED_OWNER", owner["selected_tier"]
        selected, artifacts = owner["selected_attempt"], owner["selected_artifacts"]
        trial, winner_process = artifacts["trial_definition"], artifacts["winner_process"]
        if (selected["trial_definition_sha256"] != _declared_m5_hash(trial)
                or selected["encoded_parameters"] != trial["parameters"]
                or selected["process_sha256"] != _declared_m5_hash(winner_process)
                or selected["strategy_id"] != winner_process["strategy_id"]
                or _declared_m5_utc(owner["selection_cutoff_utc"]) != _declared_m5_utc(cutoff)
                or tier != owner["selected_marginal_receipt"]["validated_cost_ladder_tolerance"]["tier"]
                or [level for level in levels if level["tier"] == tier] != [owner["selected_tier_contract"]]):
            _fail("DECLARED_NATIVE_FINALIZATION_OWNER_PROOF_REQUIRED")
        evidence = {"semantics": "VERIFIED_SELECTED_OWNER_TIER", "tier": tier,
            "selected_tier_contract": owner["selected_tier_contract"]}
        family, configuration = winner_process["strategy_family"], selected["trial_definition_sha256"]
    return view, evidence, source_entry["descriptor"]["sha256"], index_descriptor["sha256"], session["source_accessed_at_utc"], {
        "outcome": outcome, "reason_code": reason, "cost_tier": tier, "source_contract_sha256": source_contract,
        "strategy_family": family, "configuration_sha256": configuration}


def _declared_native_retirement_preflight(context, workspace_sha, *, protected, direct, locks,
        fence_path, fence_sha, retirement):
    """Check retirement bounds without publishing intent or taking write locks."""
    from pathlib import Path, PurePosixPath
    import shutil
    import pm_authority_spool as spool

    fence_path = Path(fence_path)
    receipt_path = Path(retirement["receipt_path"])
    spool._relative_parts(fence_path.name)
    spool._relative_parts(receipt_path.name)
    owned = frozenset(direct)
    max_paths, max_nodes, max_bytes = (retirement[key] for key in ("max_paths", "max_nodes", "max_bytes"))
    if len(owned) != len(direct) or len(owned) > max_paths:
        _fail("RETIREMENT_DIRECT_PATH_INVALID")
    for relative in owned:
        if len(relative.encode("utf-8")) > 4096:
            _fail("RETIREMENT_DIRECT_PATH_INVALID")
        spool._relative_parts(relative)
    expected_locks = {relative for relative in owned if PurePosixPath(relative).name.casefold() == "units.lock"}
    if len(locks) > max_paths or len(set(locks)) != len(locks) or set(locks) != expected_locks:
        _fail("RETIREMENT_UNIT_LOCK_PATHS_INVALID")
    for relative in locks:
        _, size, value = spool._stream_file(context.root, relative, maximum_bytes=1, collect=True)
        if size != 1 or value != b"0":
            _fail("RETIREMENT_UNIT_LOCK_UNVERIFIED")
    quota = spool.spool_quota._read_state(context.root)
    if quota is not None and quota["reservations"]:
        _fail("SPOOL_RETIREMENT_RESERVATIONS_PRESENT")
    if receipt_path.exists():
        _fail("RETIREMENT_RECEIPT_ALREADY_EXISTS")
    records, _, root_bytes, nodes = spool._retirement_inventory(context.root, owned,
        max_paths=max_paths, max_nodes=max_nodes, max_bytes=max_bytes, retained_unit_locks=locks)
    # Match the serialized intent that the locked retirement will validate again.
    arguments = {"source_fence_path": str(fence_path), "source_fence_sha256": fence_sha,
        "retirement_receipt_path": str(receipt_path), "workspace_owner_sha256": workspace_sha,
        "protected_paths": [str(Path(path).resolve()) for path in protected],
        "owned_direct_files": sorted(owned), "unit_lock_paths": list(locks),
        "max_paths": max_paths, "max_nodes": max_nodes, "max_bytes": max_bytes}
    intent = {"schema_version": spool.RETIREMENT_SCHEMA, "generation_context_sha256": context.context_sha256,
        "arguments": arguments, "files": records}
    maximum = min(context.limits.max_json_bytes, 64 * 1024**2)
    control_bytes = len(spool._canonical_json_bytes(intent, maximum)) + 2 * spool.MAX_CONTEXT_BYTES
    if (nodes + 3 > max_nodes or root_bytes + control_bytes > min(max_bytes, context.limits.max_generation_bytes)
            or shutil.disk_usage(context.root).free - control_bytes < context.limits.min_free_bytes):
        _fail("RETIREMENT_CONTROL_CAPACITY")


def _declared_native_finalize_account(result, *, context, contract, row, config, precommit, process,
        artifact, run_body, workspace_sha, cell_contract_sha256):
    import hashlib
    import os
    from pathlib import Path
    import pm_authority_spool as spool
    import pm_end_grid as grid

    writing = False
    policy = row["native_finalization"]
    try:
        if result["run_receipt"] is None:
            _fail("DECLARED_NATIVE_FINALIZATION_SAVED_RUN_REQUIRED")
        if workspace_sha is None:
            _fail("DECLARED_NATIVE_FINALIZATION_OWNED_WORKSPACE_REQUIRED")
        protected = _declared_native_finalization_preflight(contract, row, config, context.limits, fresh=False)
        renewal = context.root / "declared-slots" / precommit.receipt_sha256 / "source-access-renewal"
        if os.path.lexists(renewal):
            _fail("DECLARED_NATIVE_FINALIZATION_RENEWAL_NOT_OWNED")
        parent, slots, proofs, summaries, run = _declared_native_completed_records(context, row=row,
            precommit=precommit, process=process, artifact=artifact, run_body=run_body,
            profile_sha=contract["profile_sha256"])
        if result["run_receipt"] != run:
            _fail("DECLARED_NATIVE_FINALIZATION_SAVED_RUN_REQUIRED")
        grid_root = policy["end_grid_root"]
        symbol = row["cell"]["symbol"]
        retained = grid.read_end_grid(grid_root, symbol)
        code = dict(parent["source_identity"])
        for file in (__file__, spool.__file__, grid.__file__):
            path = Path(file)
            code[path.name] = hashlib.sha256(path.read_bytes()).hexdigest()
        planned, contracts, selected_slots = [], {}, []
        produced = _declared_m5_grid_utc(_declared_utc_now().strftime("%Y-%m-%dT%H:%M:%SZ"))
        for slot, summary in zip(slots, summaries, strict=True):
            key = slot["slot_id"]
            view, evidence, session_sha, index_sha, accessed, selection = proofs[key]
            start, end, cutoff = (_declared_m5_grid_utc(slot[name]) for name in
                ("activation_utc", "end_exclusive_utc", "native_cutoff_utc"))
            reconstruction = {"schema_version": "fxpm.broker-native-end-grid-reconstruction.v1", "view": view,
                "result": {"precommit_sha256": precommit.receipt_sha256, "cell_id": row["cell"]["cell_id"],
                    "slot_id": key, "ordinal": slot["ordinal"], "cycle_start_utc": start, "cycle_end_utc": end,
                    "completion_status": summary["status"], "reason_code": selection["reason_code"],
                    "request_sha256": summary["request_sha256"], "slot_commit_sha256": summary["commit_sha256"],
                    "source_session_sha256": session_sha, "native_prefix_index_sha256": index_sha,
                    "source_accessed_at_utc": accessed,
                    "saved_cell_run_receipt_sha256": run["sha256"]}, "cost_tier_evidence": evidence}
            encoded = grid.canonical_bytes(reconstruction)
            if len(encoded) > grid.MAX_CONTRACT_BYTES:
                _fail("DECLARED_NATIVE_FINALIZATION_VIEW_CONTRACT_BOUND")
            contract_sha = hashlib.sha256(encoded).hexdigest()
            contracts[contract_sha] = reconstruction
            prior = [item for item in retained if item.cell_id == row["cell"]["cell_id"]
                and item.cycle_start_utc == start and item.process_edition_sha256 == process.receipt_sha256]
            if len(prior) > 1:
                _fail("DECLARED_NATIVE_FINALIZATION_GRID_CONFLICT")
            record = grid.build_end_grid_row(symbol=symbol, timeframe=row["cell"]["timeframe"],
                regime=row["cell"]["regime"], cycle_start_utc=start, cycle_end_utc=end, information_cutoff_utc=cutoff,
                source_kind="broker_native", view_key=hashlib.sha256(grid.canonical_bytes(view)).hexdigest(),
                view_contract_sha256=contract_sha,
                source_identity={name: view[name] for name in
                    ("activation_id", "activation_manifest_sha256", "activation_entry_sha256", "native_bars_sha256")},
                no_trade_run_receipt_sha256=run["sha256"] if selection["outcome"] == "NO_TRADE" else None,
                process_edition_sha256=process.receipt_sha256, code_sha256=code, metrics={},
                produced_at_utc=prior[0].produced_at_utc if prior else produced, **selection)
            if prior and prior[0].to_bytes() != record.to_bytes():
                _fail("DECLARED_NATIVE_FINALIZATION_GRID_CONFLICT")
            planned.append(record)
            selected_slots.append({"slot_id": key, "cycle_start_utc": start, "cycle_end_utc": end,
                "information_cutoff_utc": cutoff, "outcome": selection["outcome"]})
        retirement = policy["retirement"]
        fence_path = policy["source_fence_path"]
        symbol_file = grid._symbol_file(grid._grid_root(grid_root), symbol)
        rows = {slot["slot_id"]: {"path": str(symbol_file), "row_sha256": record.row_sha256}
            for slot, record in zip(slots, planned, strict=True)}
        fence_body = {"schema_version": spool.NATIVE_COMPLETION_FENCE_SCHEMA,
            "workspace_owner_sha256": workspace_sha,
            "binding": {"generation_context_sha256": context.context_sha256, "run_id": row["run_id"],
                "cell_id": row["cell"]["cell_id"], "cell_contract_sha256": cell_contract_sha256,
                "search_process_receipt_sha256": process.receipt_sha256, "precommit_sha256": precommit.receipt_sha256},
            "completion": {"cell_run_receipt_sha256": run["sha256"], "account_artifact_sha256": artifact.descriptor["sha256"]},
            "selected_slots": selected_slots, "end_grid_rows": rows,
            "qualification_authority": False, "capital_authority": False,
            "live_authority": False, "order_authority": False}
        fence_maximum = 4 * 1024**2
        fence_sha = hashlib.sha256(spool._canonical_json_bytes(fence_body, fence_maximum)).hexdigest()
        if fence_sha != _declared_m5_hash(fence_body):
            _fail("DECLARED_NATIVE_FINALIZATION_FENCE_ENCODING_UNSUPPORTED")
        direct, locks, _ = _declared_owned_direct_files(context, precommit, row, max_paths=retirement["max_paths"])
        _declared_native_retirement_preflight(context, workspace_sha, protected=protected,
            direct=direct, locks=locks, fence_path=fence_path, fence_sha=fence_sha, retirement=retirement)
        if not _declared_disk_ready(context.root, contract, context.limits.min_free_bytes):
            _fail("DECLARED_NATIVE_FINALIZATION_DISK_LOW")
        writing = True
        for contract_sha, body in contracts.items():
            if grid.store_view_contract(grid_root, body) != contract_sha or grid.load_view_contract(grid_root, contract_sha) != body:
                _fail("DECLARED_NATIVE_FINALIZATION_VIEW_READBACK_MISMATCH")
        receipts = grid.EndGridWriter(grid_root).append_many_same_symbol(planned)
        observed = grid.read_end_grid(grid_root, symbol)
        readback = {item.row_sha256: item.to_bytes() for item in observed}
        if len(readback) != len(observed):
            _fail("DECLARED_NATIVE_FINALIZATION_GRID_READBACK_MISMATCH")
        for slot, record, receipt in zip(slots, planned, receipts, strict=True):
            if ({name: receipt[name] for name in ("path", "row_sha256")} != rows[slot["slot_id"]]
                    or readback.get(record.row_sha256) != record.to_bytes()):
                _fail("DECLARED_NATIVE_FINALIZATION_GRID_READBACK_MISMATCH")
        fence = _declared_m5_external_record(fence_path, fence_body, maximum_bytes=fence_maximum)
        if fence["sha256"] != fence_sha:
            _fail("DECLARED_NATIVE_FINALIZATION_FENCE_READBACK_MISMATCH")
        record = spool.retire_completed_generation(context.root, expected_context_sha256=context.context_sha256,
            workspace_owner_sha256=workspace_sha, source_fence_path=Path(fence_path),
            source_fence_sha256=fence_sha, retirement_receipt_path=Path(retirement["receipt_path"]),
            protected_paths=protected, owned_direct_files=direct, unit_lock_paths=locks,
            max_paths=retirement["max_paths"], max_nodes=retirement["max_nodes"], max_bytes=retirement["max_bytes"])
        _declared_native_archive_binding(record, row, cell_contract_sha256)
        result["source_kind"] = "broker_native"
        return _declared_m5_archived_row(result, record)
    except Exception as exc:
        if (context.root / spool.RETIREMENT_RELATIVE_PATH).exists():
            result.update(status="NOT_RUN", reason_code="RETIREMENT_INCOMPLETE", run_receipt=None, account_descriptor=None,
                descriptor_assurance="IDENTITY_ONLY_RETIREMENT_INCOMPLETE", genuine_cold_reopen_available=False)
        else:
            reason, diagnostics = _declared_m5_exception_reason(exc)
            result.update(native_finalization_status="INCOMPLETE_RETAINED" if writing else "REFUSED_RETAINED",
                native_finalization_reason_code=reason,
                native_finalization_error_type=diagnostics["exception_type"])
            if result["run_receipt"] is None:
                result.update(status="NOT_RUN", reason_code=reason, native_account_status=result["status"])
        return result


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
    finalize_native = row.get("native_finalization") is not None
    native_workspace_sha = None
    loaded_config = None
    try:
        context_root = Path(row["root"])
        limits = None
        if finalize_native:
            limits = spool.validate_spool_limits(spool.SpoolLimits(**_DECLARED_NATIVE_FINALIZATION_LIMITS))
            digest = _declared_native_cell_contract_sha256(contract, row, limits)
            archived = spool.read_generation_retirement(context_root,
                expected_completed_fence_schema=spool.NATIVE_COMPLETION_FENCE_SCHEMA)
            if archived is not None:
                if archived["status"] != "ARCHIVED_RECORD_ONLY":
                    result.update(status="NOT_RUN", reason_code="RETIREMENT_INCOMPLETE")
                    return result
                _declared_native_archive_binding(archived, row, digest)
                result.update(source_kind="broker_native", recorded_before_dispatch=True)
                return _declared_m5_archived_row(result, archived)
        if (context_root / spool.CONTEXT_RELATIVE_PATH).exists():
            context = spool.reopen_generation_context(context_root, expected_generation_id=row["run_id"],
                expected_native_universe_sha256=contract["native_universe_sha256"], expected_config_sha256=contract["config_sha256"])
        else:
            reserve = limits.min_free_bytes if finalize_native else DECLARED_CELL_MIN_FREE_BYTES
            if not _declared_disk_ready(context_root, contract, reserve):
                result.update(status="NOT_RUN", reason_code="DISK_LOW")
                return result
            ownership = {}
            if finalize_native:
                loaded_config = binding.load_effective_pipeline_config(Path(contract["active_config_path"]))
                _declared_native_finalization_preflight(contract, row, loaded_config[1], limits, fresh=True)
                ownership["owned_workspace"] = {"cell_id": row["cell"]["cell_id"], "cell_contract_sha256": digest}
            context = spool.freeze_generation_context(context_root, generation_id=row["run_id"],
                native_universe_sha256=contract["native_universe_sha256"], config_sha256=contract["config_sha256"],
                limits=limits if finalize_native else spool.SpoolLimits(min_free_bytes=DECLARED_CELL_MIN_FREE_BYTES),
                **ownership)
        if finalize_native:
            if not (context.root / spool.WORKSPACE_OWNER_RELATIVE_PATH).exists():
                _fail("DECLARED_NATIVE_FINALIZATION_OWNED_WORKSPACE_REQUIRED")
            workspace, native_workspace_sha = spool.read_owned_generation_workspace(context)
            if (context.limits != limits or workspace["cell_id"] != row["cell"]["cell_id"]
                    or workspace["cell_contract_sha256"] != digest):
                _fail("DECLARED_NATIVE_WORKSPACE_OWNER_CHANGED")
        with _declared_dispatch_lease(context) as acquired:
            if not acquired:
                result.update(status="NOT_RUN", reason_code="BUSY")
                return result
            payload, supplied = (loaded_config if loaded_config is not None
                else binding.load_effective_pipeline_config(Path(contract["active_config_path"])))
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
            run_body = progress.run_receipt
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
                    if finalize_native:
                        return _declared_native_finalize_account(result, context=context, contract=contract, row=row,
                            config=config, precommit=precommit, process=process, artifact=existing, run_body=run_body,
                            workspace_sha=native_workspace_sha, cell_contract_sha256=digest)
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
            if finalize_native:
                return _declared_native_finalize_account(result, context=context, contract=contract, row=row,
                    config=config, precommit=precommit, process=process, artifact=artifact, run_body=run_body,
                    workspace_sha=native_workspace_sha, cell_contract_sha256=digest)
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
            row = (_declared_m5_run_one_cell(contract, cell)
                if contract.get("schema_version") == DECLARED_M5_DEVELOPMENT_SCHEDULER_SCHEMA
                else _declared_run_one_cell(contract, cell))
        except Exception as exc:
            row = _declared_cell_row(cell, "FAILED", str(getattr(exc, "code", type(exc).__name__)), pid=os.getpid())
            stopped = True
        rows.append(row)
        if row["status"] == "NOT_RUN" and row["reason_code"] == "DISK_LOW":
            stop_reason = "DISK_LOW"
            stopped = True
        if ((contract.get("schema_version") == DECLARED_M5_DEVELOPMENT_SCHEDULER_SCHEMA
                or "native_finalization" in cell)
                and row["status"] == "NOT_RUN" and row["reason_code"].startswith(
                    ("RETIREMENT_", "SPOOL_RETIREMENT_", "WORKSPACE_ROOT_"))):
            stop_reason = row["reason_code"]
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
    from pathlib import Path
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
        if row["status"] == "ARCHIVED":
            archive = row.get("archive_record")
            source_kind = ("m5_derived" if "m5_development" in expected else
                "broker_native" if "native_finalization" in expected else None)
            if (source_kind is None or row.get("source_kind") != source_kind
                    or row.get("descriptor_assurance") != "IDENTITY_ONLY_AFTER_WORKSPACE_RELEASE"
                    or row.get("genuine_cold_reopen_available") is not False
                    or not _declared_digest(row.get("generation_context_sha256"))
                    or type(archive) is not dict or set(archive) != {"path", "sha256"}
                    or type(archive["path"]) is not str or not _declared_digest(archive["sha256"])
                    or not Path(archive["path"]).is_absolute()
                    or row.get("run_receipt") is not None or row.get("account_descriptor") is not None):
                _fail("DECLARED_NATIVE_ARCHIVED_RESULT_INVALID" if source_kind == "broker_native"
                    else "DECLARED_M5_ARCHIVED_RESULT_INVALID")
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
    if row.get("status") == "ARCHIVED":
        if contract.get("schema_version") == DECLARED_SCHEDULER_SCHEMA and row.get("source_kind") == "broker_native":
            expected = next(cell for cell in contract["cells"] if cell["descriptor_index"] == row["descriptor_index"])
            if "native_finalization" not in expected:
                _fail("DECLARED_NATIVE_ARCHIVED_RESULT_INVALID")
            limits = spool.validate_spool_limits(spool.SpoolLimits(**_DECLARED_NATIVE_FINALIZATION_LIMITS))
            record = spool.read_generation_retirement(Path(row["root"]),
                expected_context_sha256=row["generation_context_sha256"],
                expected_completed_fence_schema=spool.NATIVE_COMPLETION_FENCE_SCHEMA)
            _declared_native_archive_binding(record, expected, _declared_native_cell_contract_sha256(contract, expected, limits))
            if (record["retirement_receipt_sha256"] != row["archive_record"]["sha256"]
                    or record["retirement_receipt_path"] != row["archive_record"]["path"]
                    or record["source_fence"] != row["archived_result_reference"]
                    or record["end_grid_rows"] != row["retained_end_grid_rows"]
                    or record["precommit_sha256"] != row["precommit_sha256"]):
                _fail("DECLARED_NATIVE_ARCHIVE_RECORD_CHANGED")
            if type(row) is dict:
                row["archive_record_confirmed"] = True
            return None
        if contract.get("schema_version") != DECLARED_M5_DEVELOPMENT_SCHEDULER_SCHEMA:
            _fail("DECLARED_M5_ARCHIVED_RESULT_INVALID")
        record = spool.read_generation_retirement(Path(row["root"]),
            expected_context_sha256=row["generation_context_sha256"])
        expected = next(cell for cell in contract["cells"] if cell["descriptor_index"] == row["descriptor_index"])
        if (type(record) is not dict or record.get("status") != "ARCHIVED_RECORD_ONLY"
                or record["retirement_receipt_sha256"] != row["archive_record"]["sha256"]
                or record["retirement_receipt_path"] != row["archive_record"]["path"]
                or record["retirement_receipt_path"] != expected["m5_development"]["retirement"]["receipt_path"]
                or record["cell_contract_sha256"] != _declared_m5_cell_contract_sha256(contract, expected)):
            _fail("DECLARED_M5_ARCHIVE_RECORD_CHANGED")
        if type(row) is dict:
            row["archive_record_confirmed"] = True
        return None
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
    owned, groups = (_declared_m5_contract(contract)
        if type(contract) is dict and contract.get("schema_version") == DECLARED_M5_DEVELOPMENT_SCHEDULER_SCHEMA
        else _declared_contract(contract))
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


_DECLARED_M5_LIMITS: Final = {
    "max_artifact_bytes": 1024**3, "max_symbol_bytes": 8 * 1024**3,
    "max_artifacts_per_symbol": 100000, "max_manifest_bytes": 128 * 1024**2,
    "max_json_bytes": 64 * 1024**2, "stream_chunk_bytes": 1024**2,
    "max_generation_bytes": 8 * 1024**3, "min_free_bytes": DECLARED_CELL_MIN_FREE_BYTES,
}
_DECLARED_M5_PROFILE: Final = "9fe54fbffa760bf6568cff4915c65af06dcb8c80f9a9d48263ebec0324dc99a4"
_DECLARED_M5_RECIPE: Final = "85e321bc794c59cf26e725bbb7772b2590a7b0256e2f6e6086ee3bf52189c1f4"
_DECLARED_M5_ADMISSION_SHA: Final = "f2a6ffdf02aff5d84c9d0a743cf1ea5db30f45448112a81368acd02829b167a2"
_DECLARED_M5_COUNCIL_EVENT_SHA: Final = "23f30ed8404ab3cf63660767ffcaba0fbc64ffe76a26fb31758cf8fada40a216"
_DECLARED_M5_LINEAGE_SHA: Final = "957663a6114d370fcda30b46eaf2105396a07b22a50d586e60fa6ccbf2db07d2"
_DECLARED_M5_PATTERN_SHA: Final = "24daacb73b79e598719bd7c90f06b72577d2ecd3984f5d953f65d984a33086c3"
_DECLARED_M5_BUDGET_KEYS: Final = frozenset(("max_source_rows", "max_source_bytes",
    "max_view_rows", "max_view_bytes", "max_cache_rows", "max_cache_bytes"))
_DECLARED_M5_SCOPE: Final = {
    "source_kind": "m5_derived", "result_kind": "DEVELOPMENT_PARTIAL",
    "design_contaminated": True, "pristine_validation": False,
    "qualification_authority": False, "capital_authority": False,
    "live_authority": False, "order_authority": False,
}


def _declared_m5_hash(value) -> str:
    import hashlib
    import json
    digest = hashlib.sha256()
    for part in json.JSONEncoder(sort_keys=True, separators=(",", ":"),
            ensure_ascii=True, allow_nan=False).iterencode(value):
        digest.update(part.encode("ascii"))
    return digest.hexdigest()


def _declared_m5_utc(value):
    from datetime import datetime, timedelta
    if type(value) is not str or value != value.strip():
        _fail("DECLARED_M5_TIMESTAMP_INVALID")
    try:
        stamp = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        _fail("DECLARED_M5_TIMESTAMP_INVALID")
    if stamp.tzinfo is None or stamp.utcoffset() != timedelta(0) or stamp.microsecond:
        _fail("DECLARED_M5_TIMESTAMP_INVALID")
    return stamp


def _declared_m5_grid_utc(value):
    """Normalize only the end-grid boundary; recorded source clocks stay intact."""
    import re
    if type(value) is not str or re.fullmatch(r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(?:Z|\+00:00)", value) is None:
        _fail("DECLARED_M5_GRID_TIMESTAMP_INVALID")
    try:
        return _declared_m5_utc(value).isoformat(timespec="seconds")
    except HistoricalSchedulerError as error:
        raise HistoricalSchedulerError("DECLARED_M5_GRID_TIMESTAMP_INVALID") from error


def _declared_m5_contract(contract):
    """Validate declared axes and routing metadata without opening an edition."""
    from pathlib import Path
    import pm_m5_job_view as views
    import pm_m5_derived_costs as costs
    import pm_explicit_derived_publication as publication
    import pm_historical_anchor_orchestration as orchestration
    required = {"schema_version", "native_universe_sha256", "config_sha256", "profile_sha256",
        "active_config_path", "cells", "spool_limits", "view_budgets", "cost_edition_route", "scope"}
    optional = {"scheduler_disk_min_free_bytes", "scheduler_disk_headroom_bytes"}
    if (type(contract) is not dict or not required.issubset(contract)
            or not set(contract).issubset(required | optional)
            or contract.get("schema_version") != DECLARED_M5_DEVELOPMENT_SCHEDULER_SCHEMA):
        _fail("DECLARED_M5_CONTRACT_INVALID")
    owned = _declared_json_copy(contract)
    if (owned["scope"] != _DECLARED_M5_SCOPE
            or any(type(owned["scope"].get(k)) is not type(v) for k, v in _DECLARED_M5_SCOPE.items())
            or owned["profile_sha256"] != _DECLARED_M5_PROFILE
            or type(owned["spool_limits"]) is not dict or owned["spool_limits"] != _DECLARED_M5_LIMITS
            or any(type(value) is not int for value in owned["spool_limits"].values())
            or type(owned["view_budgets"]) is not dict or set(owned["view_budgets"]) != _DECLARED_M5_BUDGET_KEYS
            or any(type(value) is not int or value <= 0 for value in owned["view_budgets"].values())):
        _fail("DECLARED_M5_LIMITS_OR_SCOPE_INVALID")
    # This decoder validates ordinary recorded identity/routing; its eager data
    # reader is intentionally deferred until after the original precommit.
    pins = costs._decode_cost_reopen_route(owned["cost_edition_route"])
    if pins.profile_sha256 != _DECLARED_M5_PROFILE:
        _fail("DECLARED_M5_COST_EDITION_MISMATCH")
    native_shape = dict(owned, schema_version=DECLARED_SCHEDULER_SCHEMA)
    checked, groups = _declared_contract(native_shape)
    checked["schema_version"] = DECLARED_M5_DEVELOPMENT_SCHEDULER_SCHEMA
    if len(checked["cells"]) != 1:
        _fail("DECLARED_M5_DEVELOPMENT_CELL_NOT_ADOPTED")
    row = checked["cells"][0]
    if tuple(row["cell"][key] for key in ("symbol", "timeframe", "regime")) != ("XAGUSD", "H1", "TREND"):
        _fail("DECLARED_M5_DEVELOPMENT_CELL_NOT_ADOPTED")
    shared = {"root", "run_id", "cell", "launch_activation_utc", "frozen_refit_clock",
        "protected_end_exclusive_utc", "extend_frozen_clock_backward", "source_access_window_policy",
        "descriptor_index", "m5_development"}
    if set(row) != shared:
        _fail("DECLARED_M5_CELL_DESCRIPTOR_INVALID")
    policy = row["source_access_window_policy"]
    if (any(type(policy[name]) is not int for name in ("lead_seconds", "validity_seconds"))
            or policy != {"lead_seconds": 0, "validity_seconds": 172800}):
        _fail("DECLARED_M5_WINDOW_POLICY_INVALID")
    dev = row["m5_development"]
    fields = {"development_slot_ids", "view_axes", "slot_view_ids", "calibration_view_id",
        "account_view_id", "required_result_keys", "terminal_result_key", "m5_source_pins",
        "cost_group_metadata", "gap_provenance", "end_grid_root", "source_fence_path", "retirement"}
    if type(dev) is not dict or set(dev) != fields:
        _fail("DECLARED_M5_CELL_DESCRIPTOR_INVALID")
    selected = dev["development_slot_ids"]
    if (type(selected) is not list or not 1 <= len(selected) <= 3
            or any(type(item) is not str or not item for item in selected)
            or len(set(selected)) != len(selected)
            or type(dev["slot_view_ids"]) is not dict or set(dev["slot_view_ids"]) != set(selected)
            or any(type(value) is not str or not value for value in dev["slot_view_ids"].values())):
        _fail("DECLARED_M5_DEVELOPMENT_PREFIX_INVALID")
    ids = list(dev["slot_view_ids"].values()) + [dev["calibration_view_id"], dev["account_view_id"]]
    terminal = dev["terminal_result_key"]
    if (any(type(value) is not str or not value for value in ids) or len(set(ids)) != len(ids)
            or type(terminal) is not str or not terminal or terminal in selected
            or dev["required_result_keys"] != selected + [terminal]):
        _fail("DECLARED_M5_VIEW_ROSTER_INVALID")
    axes = dev["view_axes"]
    if type(axes) is not list or len(axes) != len(ids):
        _fail("DECLARED_M5_VIEW_AXES_INVALID")
    plan_sha = views.declared_view_plan_sha256(tuple(axes))
    axes_by_id = {axis["view_id"]: axis for axis in axes}
    if (set(axes_by_id) != set(ids) or any(axis["symbol"] != "XAGUSD"
            or axis["timeframe"] != "H1" or axis["source_kind"] != "m5_derived"
            or axis["clock_template_sha256"] is not None or axis["clock_axes_sha256"] is not None
            or axis["recipe_sha256"] != _DECLARED_M5_RECIPE for axis in axes)):
        _fail("DECLARED_M5_VIEW_AXES_INVALID")
    for view_id, axis in axes_by_id.items():
        purpose = "COST_EDITION_REPRODUCTION_ONLY" if view_id == dev["calibration_view_id"] else "FINANCIAL_INPUT"
        if axis["purpose"] != purpose or not _declared_digest(axis["spread_identification_sha256"]):
            _fail("DECLARED_M5_VIEW_AXES_INVALID")
    orchestration._validate_declared_m5_source_pins(dev["m5_source_pins"], row["cell"])
    if (dev["m5_source_pins"]["cost_edition_identity"] != pins.identity()
            or dev["m5_source_pins"]["view_plan_sha256"] != plan_sha):
        _fail("DECLARED_M5_SOURCE_PINS_MISMATCH")
    if any(axis["constructor_implementation_sha256"] != dev["m5_source_pins"]["constructor_identity"].get(
            "pm_explicit_derived_history.py") for axis in axes):
        _fail("DECLARED_M5_SOURCE_PINS_MISMATCH")
    group = dev["cost_group_metadata"]
    if (type(group) is not dict or group.get("symbol") != "XAGUSD" or group.get("timeframe") != "H1"
            or group.get("cost_status") != publication.COST_READY or group.get("source_kind") != "m5_derived"
            or type(group.get("fine_source")) is not dict
            or group.get("spread_identification_sha256") != axes_by_id[dev["calibration_view_id"]]["spread_identification_sha256"]
            or group.get("available_interval") != axes_by_id[dev["calibration_view_id"]]["interval"]):
        _fail("DECLARED_M5_RECORDED_CELL_METADATA_INVALID")
    if _declared_m5_utc(axes_by_id[dev["calibration_view_id"]]["cutoff_utc"]) != _declared_m5_utc(
            "2024-07-14T08:05:00+00:00"):
        _fail("DECLARED_M5_RECORDED_CELL_METADATA_INVALID")
    _declared_m5_admission(dev)
    if any(axis["gap_findings_sha256"] != _DECLARED_M5_LINEAGE_SHA for axis in axes):
        _fail("DECLARED_M5_GAP_PROVENANCE_INVALID")
    if dev["m5_source_pins"]["gap_findings_sha256"] != _DECLARED_M5_LINEAGE_SHA:
        _fail("DECLARED_M5_GAP_PROVENANCE_INVALID")
    retirement = dev["retirement"]
    if (type(retirement) is not dict or set(retirement) != {"receipt_path", "max_paths", "max_nodes", "max_bytes"}
            or any(type(retirement[key]) is not int or retirement[key] <= 0
                for key in ("max_paths", "max_nodes", "max_bytes"))
            or retirement["max_bytes"] > checked["spool_limits"]["max_generation_bytes"]):
        _fail("DECLARED_M5_RETIREMENT_POLICY_REQUIRED")
    external = []
    root = Path(row["root"])
    for value in (dev["end_grid_root"], dev["source_fence_path"], retirement["receipt_path"]):
        if type(value) is not str or not Path(value).is_absolute() or ".." in Path(value).parts:
            _fail("DECLARED_M5_FINAL_PATH_INVALID")
        path = Path(value).resolve()
        if path == root or path.is_relative_to(root) or root.is_relative_to(path):
            _fail("DECLARED_M5_FINAL_PATH_INVALID")
        external.append(path)
    if len(set(external)) != len(external):
        _fail("DECLARED_M5_FINAL_PATH_INVALID")
    return checked, groups


def _declared_m5_admission(dev):
    import json
    from pathlib import Path
    import pm_authority_spool as spool
    gap = dev["gap_provenance"]
    if (type(gap) is not dict or set(gap) != {"status", "gap_status", "admission_record", "lineage",
            "gap_pattern_observations", "control_identification_sha256"}
            or gap["status"] != "AVAILABLE_DATA_ONLY_RESEARCH_ADMISSION" or gap["gap_status"] != "UNCLASSIFIED"
            or gap["control_identification_sha256"] != dev["cost_group_metadata"]["spread_identification_sha256"]):
        _fail("DECLARED_M5_GAP_PROVENANCE_INVALID")
    reference = gap["admission_record"]
    if (type(reference) is not dict or set(reference) != {"path", "bytes", "sha256"}
            or reference["bytes"] != 2016 or type(reference["bytes"]) is not int
            or reference["sha256"] != _DECLARED_M5_ADMISSION_SHA
            or type(reference["path"]) is not str or not Path(reference["path"]).is_absolute()):
        _fail("DECLARED_M5_RESEARCH_ADMISSION_REQUIRED")
    path = Path(reference["path"])
    digest, size, encoded = spool._stream_file(path.parent, path.name, maximum_bytes=2016,
        expected_size=2016, collect=True)
    if digest != _DECLARED_M5_ADMISSION_SHA or size != 2016:
        _fail("DECLARED_M5_RESEARCH_ADMISSION_CHANGED")
    record = json.loads(encoded)
    if (record.get("schema_version") != "fxpm.available-data-research-admission.v1"
            or record.get("status") != gap["status"] or record.get("gap_status") != "UNCLASSIFIED"
            or record.get("decision_event") != "E-00004594"
            or record.get("decision_event_canonical_sha256") != _DECLARED_M5_COUNCIL_EVENT_SHA
            or record.get("cell") != {"symbol": "XAGUSD", "timeframe": "H1", "regime": "TREND"}
            or record.get("selected_periods") != len(dev["development_slot_ids"])
            or gap["lineage"] != record.get("lineage") or gap["gap_pattern_observations"] != record.get("gap_pattern_observations")
            or gap["lineage"].get("sha256") != _DECLARED_M5_LINEAGE_SHA or gap["lineage"].get("bytes") != 4717
            or gap["gap_pattern_observations"].get("sha256") != _DECLARED_M5_PATTERN_SHA
            or gap["gap_pattern_observations"].get("bytes") != 4263
            or gap["gap_pattern_observations"].get("calendar_causal_classifications_adopted") is not False
            or record.get("design_contaminated") is not True or record.get("pristine_validation") is not False
            or any(record.get(key) is not False for key in ("financial_consumer_ready", "full_native_context_eligibility",
                "qualification_authority", "capital_authority", "live_authority", "order_authority"))):
        _fail("DECLARED_M5_RESEARCH_ADMISSION_CHANGED")
    return record


def _declared_m5_dependencies():
    """Refuse an incomplete installed source batch before issuing anything."""
    import inspect
    import pm_forward_oos_replay_producer as forward
    import pm_historical_search_producer as producer
    import pm_authority_spool as spool
    import pm_end_grid as end_grid
    required = (
        (producer, "execute_declared_cell_owner_slots", {"m5_owner", "slot_view_ids", "calibration_view_id", "cost_edition", "cell_contract_sha256"}),
        (producer, "reopen_declared_cell_owner_progress", {"m5_owner", "slot_view_ids", "calibration_view_id", "cost_edition", "cell_contract_sha256"}),
        (producer, "publish_declared_m5_cell_source_context", {"m5_owner", "slot_view_ids", "calibration_view_id", "cost_edition", "cell_contract_sha256"}),
        (producer, "reopen_declared_m5_cell_source_context", {"cell_contract_sha256"}),
        (forward, "produce_declared_m5_development_cell_account_artifact", {"m5_owner", "account_view_id", "cost_edition"}),
        (forward, "discover_declared_m5_development_cell_account_artifact", {"precommit", "process", "frozen_pipeline_config"}),
        (forward, "declared_m5_development_cell_account_request", {"precommit", "process", "frozen_pipeline_config"}),
        (forward, "verify_declared_m5_development_cell_account_transport", {"m5_owner", "cost_edition"}),
        (spool, "freeze_generation_context", {"owned_workspace"}),
        (spool, "read_generation_retirement", {"expected_context_sha256"}),
        (spool, "read_owned_generation_workspace", {"context"}),
        (spool, "retire_completed_generation", {"expected_context_sha256", "workspace_owner_sha256", "unit_lock_paths"}),
        (end_grid, "build_end_grid_row", {"no_trade_run_receipt_sha256"}),
    )
    for module, name, names in required:
        function = getattr(module, name, None)
        if not callable(function) or not names.issubset(inspect.signature(function).parameters):
            _fail("DECLARED_M5_SOURCE_BATCH_NOT_IMPLEMENTED", name)


def _declared_m5_cell_contract_sha256(contract, row):
    return _declared_m5_hash({"schema_version": contract["schema_version"],
        "native_universe_sha256": contract["native_universe_sha256"], "config_sha256": contract["config_sha256"],
        "profile_sha256": contract["profile_sha256"], "active_config_path": contract["active_config_path"],
        "spool_limits": contract["spool_limits"], "view_budgets": contract["view_budgets"],
        "cost_edition_route": contract["cost_edition_route"], "scope": contract["scope"],
        "cell": {key: value for key, value in row.items() if key != "descriptor_index"}})


def _declared_m5_check_roster(row, parent, context, frozen):
    import pm_historical_anchor_orchestration as orchestration
    import pm_production_anchor_plan as anchors
    dev = row["m5_development"]
    if (parent.get("schema_version") != orchestration.DECLARED_M5_DEVELOPMENT_SLOT_PRECOMMIT_SCHEMA
            or orchestration.declared_slot_source_kind(parent) != "m5_derived"
            or parent.get("cells") != [row["cell"]] or parent.get("run_id") != row["run_id"]
            or parent.get("m5_source_pins") != dev["m5_source_pins"]
            or parent.get("spread_profile_identity") != {"profile_sha256": _DECLARED_M5_PROFILE}):
        _fail("DECLARED_M5_PRECOMMIT_BINDING_MISMATCH")
    anchors.verify_declared_owner_slot_roster(parent["slot_roster"],
        launch_activation_utc=row["launch_activation_utc"], frozen_refit_clock=row["frozen_refit_clock"],
        config_identity_refs={"config_sha256": context.config_sha256,
            "effective_config_sha256": frozen.effective_config_sha256},
        extend_frozen_clock_backward=row["extend_frozen_clock_backward"],
        protected_end_exclusive_utc=row["protected_end_exclusive_utc"])
    slots = orchestration.declared_slot_selected_slots(parent)
    if [slot["slot_id"] for slot in slots] != dev["development_slot_ids"]:
        _fail("DECLARED_M5_DEVELOPMENT_PREFIX_INVALID")
    axes = {axis["view_id"]: axis for axis in dev["view_axes"]}
    for slot in slots:
        cutoff = axes[dev["slot_view_ids"][slot["slot_id"]]]["cutoff_utc"]
        if _declared_m5_utc(cutoff) != _declared_m5_utc(slot["native_cutoff_utc"]):
            _fail("DECLARED_M5_SELECTION_AXIS_MISMATCH")
    return slots


def _declared_m5_new_owner(context, *, precommit, process, frozen, config, contract, row, edition):
    """Acquire only the finite cutoffs already bound by the original precommit."""
    from dataclasses import asdict
    from pathlib import Path
    import pm_m5_job_view as views
    import pm_native_prefix_production as native
    from pm_explicit_derived_history import Interval
    dev = row["m5_development"]
    group = edition.cell_metadata(row["cell"]["symbol"], row["cell"]["timeframe"])
    identification = edition.identification(row["cell"]["symbol"], row["cell"]["timeframe"])
    if (group != dev["cost_group_metadata"] or edition.sha256 != contract["profile_sha256"]
            or any(axis["stratum"] != asdict(identification.stratum)
                or axis["spread_identification_sha256"] != identification.evidence_sha256
                for axis in dev["view_axes"])):
        _fail("DECLARED_M5_RECORDED_AXES_MISMATCH")
    cutoffs = sorted({axis["cutoff_utc"] for axis in dev["view_axes"]}, key=_declared_m5_utc)
    if len({_declared_m5_utc(value) for value in cutoffs}) != len(cutoffs):
        _fail("DECLARED_M5_DUPLICATE_CUTOFF_SPELLING")
    accessed = _declared_utc_now().strftime("%Y-%m-%dT%H:%M:%SZ")
    products = native.produce_activated_native_prefixes(data_dir=Path(config.data_dir), context=context,
        symbol=row["cell"]["symbol"], timeframe="M5", requests=tuple({"cutoff_utc": _declared_m5_utc(value).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "source_accessed_at_utc": accessed} for value in cutoffs), production_contract_required=True)
    source = views.source_identity_from_m5_product(products[-1], context,
        canonical_data_dir=Path(config.data_dir))
    if asdict(source) != group["fine_source"]:
        _fail("DECLARED_M5_FINE_SOURCE_CHANGED")
    keys = {value: slot for slot, value in dev["slot_view_ids"].items()}
    requests = []
    for axis in dev["view_axes"]:
        requests.append(views.ViewRequest(view_id=axis["view_id"], source=source,
            timeframe=axis["timeframe"], interval=Interval(**axis["interval"]), cutoff_utc=axis["cutoff_utc"],
            constructor_implementation_sha256=axis["constructor_implementation_sha256"],
            gap_findings_sha256=axis["gap_findings_sha256"], result_key=keys.get(axis["view_id"], dev["terminal_result_key"]),
            recipe_sha256=axis["recipe_sha256"], consumer=(views.CalibrationViewBinding(edition.sha256)
                if axis["view_id"] == dev["calibration_view_id"] else None), stratum=identification.stratum,
            spread_identification=identification, execution_required=False, source_kind="m5_derived",
            purpose=axis["purpose"]))
    binding = views.DerivedJobBinding(
        m5_view_contract_sha256=_declared_m5_hash(dev["m5_source_pins"]),
        generation_context_sha256=context.context_sha256, config_sha256=context.config_sha256,
        effective_config_sha256=frozen.effective_config_sha256, search_process_receipt_sha256=process.receipt_sha256,
        precommit_sha256=precommit.receipt_sha256, run_id=row["run_id"], cell_id=row["cell"]["cell_id"],
        view_budgets_sha256=_declared_m5_hash(contract["view_budgets"]),
        view_plan_sha256=dev["m5_source_pins"]["view_plan_sha256"],
        required_result_keys=tuple(dev["required_result_keys"]), terminal_result_key=dev["terminal_result_key"])
    return views.M5JobViews(context, canonical_data_dir=Path(config.data_dir), binding=binding,
        fine_products=tuple(products), requests=tuple(requests), budgets=views.ViewBudgets(**contract["view_budgets"]))


def _declared_m5_external_record(path, body, *, maximum_bytes):
    """Write one exclusive final record, or verify the exact retained bytes."""
    import hashlib
    import json
    import os
    import stat
    from pathlib import Path
    import pm_authority_spool as spool
    path = Path(path)
    if not path.is_absolute() or not path.parent.is_dir():
        _fail("DECLARED_M5_FINAL_PARENT_REQUIRED")
    encoded = json.dumps(body, sort_keys=True, separators=(",", ":"), ensure_ascii=True,
        allow_nan=False).encode("ascii")
    if len(encoded) > maximum_bytes:
        _fail("DECLARED_M5_FINAL_RECORD_BOUND")
    spool._reject_reparse_chain(path)
    created = False
    try:
        descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0), 0o600)
    except FileExistsError:
        descriptor = None
    if descriptor is not None:
        created = True
        with os.fdopen(descriptor, "wb") as stream:
            if stream.write(encoded) != len(encoded):
                _fail("DECLARED_M5_FINAL_SHORT_WRITE")
            stream.flush()
            os.fsync(stream.fileno())
    spool._reject_reparse_chain(path)
    descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
    with os.fdopen(descriptor, "rb") as stream:
        before, opened = path.stat(follow_symlinks=False), os.fstat(stream.fileno())
        if (not stat.S_ISREG(opened.st_mode) or before.st_size != len(encoded)
                or (before.st_dev, before.st_ino) != (opened.st_dev, opened.st_ino)
                or stream.read(maximum_bytes + 1) != encoded):
            _fail("DECLARED_M5_FINAL_RECORD_CHANGED")
        after = path.stat(follow_symlinks=False)
        if (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns) != (
                before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns):
            _fail("DECLARED_M5_FINAL_RECORD_CHANGED")
    return {"path": str(path), "sha256": hashlib.sha256(encoded).hexdigest(),
        "size_bytes": len(encoded), "written": created, "readback_sha256": hashlib.sha256(encoded).hexdigest()}


def _declared_m5_finalize(context, *, precommit, process, frozen, contract, row, owner,
        edition, run_body, artifact):
    from dataclasses import asdict
    import pm_causal_evidence_producer as causal_producer
    import pm_authority_spool as spool
    import pm_forward_oos_replay_producer as forward
    import pm_historical_search_producer as producer
    import pm_m5_job_view as views
    dev = row["m5_development"]
    body = causal_producer._read_descriptor_json(context, artifact.descriptor,
        code="DECLARED_CELL_RAW_ACCOUNT")
    causal_producer._verify_declared_cell_account_manifest(context, body, artifact.descriptor,
        development=True)
    forward.verify_declared_m5_development_cell_account_transport(context, body, process=process,
        frozen_pipeline_config=frozen, m5_owner=owner, cost_edition=edition)
    if (body.get("progress_status") != "DEVELOPMENT_COMPLETE"
            or tuple(body.get("terminal_consumer_binding_sha256", ())) != owner.consumer_bindings_for(dev["terminal_result_key"])):
        _fail("DECLARED_M5_ACCOUNT_RESULT_NOT_COMPLETE")
    # Derive every selected result from an existing original commit; no future
    # request or synthetic calibration result is inserted into the ledger.
    slot_results, commits = {}, {}
    import pm_historical_anchor_orchestration as orchestration
    for slot in orchestration.declared_slot_selected_slots(precommit.to_dict()):
        record = producer._read_declared_commit(context,
            producer._declared_commit_path(precommit, row["cell"]["cell_id"], slot["ordinal"]))
        if record is None or record[0].get("slot_id") != slot["slot_id"]:
            _fail("DECLARED_M5_FINAL_COMMIT_MISSING")
        slot_results[slot["slot_id"]] = record[1]
        commits[slot["slot_id"]] = record[0]
    if set(slot_results) != set(dev["development_slot_ids"]):
        _fail("DECLARED_M5_FINAL_COMMIT_MISSING")
    run_descriptor = _declared_run_descriptor(precommit, row, run_body)
    if body.get("saved_cell_run_receipt_sha256") != run_descriptor["sha256"]:
        _fail("DECLARED_M5_ACCOUNT_RUN_BINDING_MISMATCH")
    if owner.pending_view_ids:
        _fail("DECLARED_M5_FINAL_CONSUMERS_INCOMPLETE", *owner.pending_view_ids)
    direct = _declared_m5_owned_direct_files(context, precommit, row, process=process, frozen=frozen,
        m5_owner=owner, cost_edition=edition, commits=commits)
    if not _declared_disk_ready(context.root, contract, context.limits.min_free_bytes):
        _fail("DECLARED_M5_DISK_LOW")
    grids = _declared_m5_append_grid(context, precommit=precommit, process=process, row=row,
        owner=owner, edition=edition, account_body=body, account_descriptor=artifact.descriptor,
        run_descriptor=run_descriptor, commits=commits)
    results = {key: views.CallerValidatedResultBinding(value, owner.job_binding_sha256,
        owner.consumer_bindings_for(key), result_descriptor_sha256=grids[key]["row_sha256"])
        for key, value in slot_results.items()}
    terminal = dev["terminal_result_key"]
    last = dev["development_slot_ids"][-1]
    results[terminal] = views.CallerValidatedResultBinding(artifact.descriptor["sha256"],
        owner.job_binding_sha256, owner.consumer_bindings_for(terminal),
        result_descriptor_sha256=grids[last]["row_sha256"])
    end_grid = {key: views.EndGridResultBinding(value["path"], value["row_sha256"], value["appended"],
        value["readback_row_sha256"]) for key, value in grids.items()}
    end_grid[terminal] = end_grid[last]
    summary = owner.finish(results, end_grid_rows=end_grid)
    if not _declared_disk_ready(context.root, contract, context.limits.min_free_bytes):
        _fail("DECLARED_M5_DISK_LOW")
    compact = asdict(summary)
    # The current invocation's append/readback assertion has been checked by
    # finish. Keep the immutable row identity instead of an invocation flag.
    del compact["end_grid_rows"]
    for view in compact["views"]:
        coverage = view["coverage"]
        if coverage is not None:
            gaps = coverage.pop("observed_gaps")
            coverage["observed_gap_count"] = None if gaps is None else len(gaps)
            coverage["observed_gaps_sha256"] = None if gaps is None else _declared_m5_hash(gaps)
    fence = _declared_m5_external_record(dev["source_fence_path"], {
        "schema_version": "fxpm.declared-m5-development-source-fence.v1",
        "workspace_owner_sha256": spool.read_owned_generation_workspace(context)[1],
        "terminal_end_grid_reference_semantics": "FINAL_SELECTED_SLOT_ROW_WITH_SEPARATE_ACCOUNT_BINDING",
        "retirement_ownership": direct[2],
        "end_grid_rows": {key: {name: value[name] for name in ("path", "row_sha256")}
            for key, value in grids.items()}, "source_summary": compact,
        **_DECLARED_M5_SCOPE}, maximum_bytes=4 * 1024**2)
    return grids, fence, direct


def _declared_m5_append_grid(context, *, precommit, process, row, owner, edition, account_body,
        account_descriptor, run_descriptor, commits):
    """Use the original project writer; never coerce a run hash to trade evidence."""
    import hashlib
    from pathlib import Path
    import pm_end_grid as grid
    import pm_historical_anchor_orchestration as orchestration
    import pm_m5_job_view as views
    dev = row["m5_development"]
    slots = orchestration.declared_slot_selected_slots(precommit.to_dict())
    statuses = {slot["slot_id"]: commits[slot["slot_id"]]["completion"]["status"] for slot in slots}
    if any(value not in {"OWNER", "NO_TRADE"} for value in statuses.values()):
        _fail("GRID_EVIDENCE_KIND_UNSUPPORTED")
    owners = {record["owner"]["slot_id"]: record["owner"] for record in account_body["owner_records"]}
    windows = {record["slot_id"]: record["selection_view_window"]
        for record in account_body["m5_selected_tier_views"]}
    owner_slots = {key for key, value in statuses.items() if value == "OWNER"}
    if set(owners) != owner_slots or set(windows) != owner_slots:
        _fail("DECLARED_M5_GRID_OWNER_BINDING_MISMATCH")
    metadata = owner.reopen_metadata()
    requests = {request["view_id"]: request for request in metadata["requests"]}
    source = owner.source_identity
    code = dict(dev["m5_source_pins"]["constructor_identity"])
    code["pm_historical_scheduler.py"] = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
    code["pm_end_grid.py"] = hashlib.sha256(Path(grid.__file__).read_bytes()).hexdigest()
    writer, written = grid.EndGridWriter(dev["end_grid_root"]), {}
    retained = grid.read_end_grid(dev["end_grid_root"], row["cell"]["symbol"])
    # Resolve all genuine owner/window inputs before publishing the first row.
    planned = []
    for slot in slots:
        key = slot["slot_id"]
        actual = owners.get(key)
        if actual is not None:
            window = windows[key]
            selected = actual["selected_attempt"]
            tier_contract = actual["selected_tier_contract"]
            source_contract = actual.get("source_contract_sha256")
            outcome, reason, tier = "WINNER", "DECLARED_DEVELOPMENT_VERIFIED_OWNER", actual["selected_tier"]
            family, configuration = selected["strategy_id"], selected["trial_definition_sha256"]
            baseline = None
        else:
            request_sha = commits[key]["completion"]["request_sha256"]
            request_lease = owner.acquire(dev["slot_view_ids"][key], consumer=views.SlotViewBinding(request_sha, key), result_key=key)
            with request_lease:
                calibration_lease = owner.acquire(dev["calibration_view_id"], consumer=views.CalibrationViewBinding(edition.sha256),
                    result_key=owner.binding.terminal_result_key)
                with calibration_lease:
                    live_view = owner.view(request_lease)
                    bound = edition.bind_view(owner, context=context, calibration_lease=calibration_lease,
                        request_lease=request_lease, point=owner.source_identity.point)
                    identity = owner.route_at(live_view.cutoff_utc)["source_identity_sha256"]
                    required = bound.resolve(row["cell"]["symbol"], row["cell"]["timeframe"], "P75",
                        point=owner.source_identity.point, source_identity_sha256=identity)
                    baseline = required.descriptor()
                    bound.from_descriptor(baseline, symbol=row["cell"]["symbol"], timeframe=row["cell"]["timeframe"],
                        point=owner.source_identity.point, source_identity_sha256=identity)
                    window = baseline["m5_execution_view_binding"]
                    if window["view_key"] != live_view.view_key:
                        _fail("DECLARED_M5_GRID_BASELINE_VIEW_CHANGED")
            selected, tier_contract, source_contract = None, None, None
            outcome, reason, tier = "NO_TRADE", "DECLARED_DEVELOPMENT_VERIFIED_NO_TRADE", "P75"
            family, configuration = None, None
        consumers = owner.consumer_bindings_for(key)
        if ((actual is not None and (actual.get("schema_version") != "fxpm.declared-m5-slot-owner.v1"
                    or not _declared_digest(source_contract)
                    or _declared_m5_utc(actual["selection_cutoff_utc"]) != _declared_m5_utc(slot["native_cutoff_utc"])))
                or len(consumers) != 1 or window["source_kind"] != "m5_derived"
                or window["m5_view_contract_sha256"] != owner.binding.m5_view_contract_sha256):
            _fail("DECLARED_M5_GRID_OWNER_BINDING_MISMATCH")
        view_id = dev["slot_view_ids"][key]
        contract_body = {"schema_version": "fxpm.m5-end-grid-reconstruction.v1",
            "view_request": requests[view_id], "m5_source_pins": dev["m5_source_pins"],
            "job_binding": metadata["binding"], "view_budgets": metadata["budgets"],
            "actual_slot_consumer": {"request_sha256": commits[key]["completion"]["request_sha256"], "slot_id": key},
            "cost_group_metadata": dev["cost_group_metadata"], "selected_tier_contract": tier_contract,
            "selected_attempt": selected, "target_view_window": window,
            "cost_tier_semantics": ("SELECTED_OWNER_TIER" if actual is not None
                else "BOUND_BASELINE_DESCRIPTOR_NO_SELECTED_WINNER"),
            "bound_baseline_descriptor_sha256": None if baseline is None else _declared_m5_hash(baseline),
            "bound_baseline_descriptor": baseline,
            "development_scope": orchestration.declared_slot_development_scope(precommit.to_dict()),
            "selected_slots": len(slots), "accepted_roster_slots": 13,
            "actual_result_identity": {"slot_commit_sha256": _declared_m5_hash(commits[key]),
                "cell_run_receipt_sha256": run_descriptor["sha256"], "account_artifact_sha256": account_descriptor["sha256"]},
            **_DECLARED_M5_SCOPE}
        if key == dev["development_slot_ids"][-1]:
            contract_body["terminal_account_reference"] = {
                "semantics": "FINAL_SELECTED_SLOT_ROW_WITH_SEPARATE_ACCOUNT_BINDING",
                "account_view_window": account_body["m5_account_view_window"],
                "account_consumer_bindings": account_body["m5_account_consumer_bindings"],
                "actual_terminal_consumer_sha256": account_body["terminal_consumer_binding_sha256"],
                "account_artifact_sha256": account_descriptor["sha256"], "cell_run_receipt_sha256": run_descriptor["sha256"]}
        contract_sha = grid.store_view_contract(dev["end_grid_root"], contract_body)
        prior = [value for value in retained if value.cell_id == row["cell"]["cell_id"]
            and _declared_m5_utc(value.cycle_start_utc) == _declared_m5_utc(slot["activation_utc"])
            and value.process_edition_sha256 == process.receipt_sha256]
        if len(prior) > 1:
            _fail("DECLARED_M5_GRID_READBACK_MISMATCH")
        produced = prior[0].produced_at_utc if prior else _declared_utc_now().strftime("%Y-%m-%dT%H:%M:%SZ")
        record = grid.build_end_grid_row(symbol=row["cell"]["symbol"], timeframe=row["cell"]["timeframe"],
            regime=row["cell"]["regime"], cycle_start_utc=_declared_m5_grid_utc(slot["activation_utc"]),
            cycle_end_utc=_declared_m5_grid_utc(slot["end_exclusive_utc"]), information_cutoff_utc=_declared_m5_grid_utc(slot["native_cutoff_utc"]),
            outcome=outcome, reason_code=reason, source_kind="m5_derived",
            source_contract_sha256=source_contract,
            no_trade_run_receipt_sha256=run_descriptor["sha256"] if outcome == "NO_TRADE" else None,
            view_key=window["view_key"],
            view_contract_sha256=contract_sha, source_identity={"activation_id": source.activation_id,
                "m5_source_sha256": source.bars_sha256,
                "constructor_implementation_sha256": requests[view_id]["constructor_implementation_sha256"],
                "gap_findings_sha256": requests[view_id]["gap_findings_sha256"]},
            cost_tier=tier, process_edition_sha256=process.receipt_sha256,
            code_sha256=code, metrics={}, strategy_family=family,
            configuration_sha256=configuration,
            job_binding_sha256=owner.job_binding_sha256, consumer_binding_sha256=consumers[0], produced_at_utc=_declared_m5_grid_utc(produced))
        planned.append((key, record))
    for key, record in planned:
        receipt = writer.append(record)
        matching = [value for value in grid.read_end_grid(dev["end_grid_root"], row["cell"]["symbol"])
            if value.row_sha256 == record.row_sha256]
        if len(matching) != 1 or matching[0].to_bytes() != record.to_bytes():
            _fail("DECLARED_M5_GRID_READBACK_MISMATCH")
        written[key] = {**receipt, "readback_row_sha256": matching[0].row_sha256}
    return written


def _declared_m5_refusal_eligible(*, resumed, source_attempted, owner_created, owner_present):
    facts = (resumed, source_attempted, owner_created, owner_present)
    if any(type(value) is not bool for value in facts):
        _fail("DECLARED_M5_REFUSAL_FACTS_INVALID")
    return not any(facts)


def _declared_m5_refusal_slot(slots, *, phase, run_body, resumed):
    """Use an earned cycle only; an unresolved in-flight slot is not guessed."""
    if type(slots) not in {tuple, list} or not slots or type(resumed) is not bool:
        _fail("DECLARED_M5_REFUSAL_SLOT_INVALID")
    if phase in {"PRE_SOURCE", "SOURCE_ACQUISITION", "SOURCE_HEADER"}:
        return None if resumed else slots[0]
    if type(run_body) is not dict:
        return None
    commits = run_body.get("slot_commits")
    if (type(commits) is not list or len(commits) > len(slots)
            or any(type(value) is not dict for value in commits)
            or [value.get("slot_id") for value in commits] != [slot["slot_id"] for slot in slots[:len(commits)]]):
        _fail("DECLARED_M5_REFUSAL_SLOT_INVALID")
    stops = [value["slot_id"] for value in commits if value.get("status") == "STOP"]
    if run_body.get("terminal_stop") is True and len(stops) == 1 and stops[0] == commits[-1]["slot_id"]:
        return slots[len(commits) - 1]
    if (run_body.get("terminal_stop") is False and phase in {"ACCOUNT", "FINALIZATION"}
            and len(commits) == len(slots)
            and all(value.get("status") in {"OWNER", "NO_TRADE"} for value in commits)):
        return slots[-1]
    return None


def _declared_m5_refusal_body(*, binding, workspace_owner_sha256, result, slot, cell,
        code_sha256, recorded_at_utc, resumed, source_attempted, owner_created, owner_present):
    """Ordinary refusal metadata; it is not a source/job completion summary."""
    keys = {"generation_context_sha256", "run_id", "cell_id", "cell_contract_sha256",
        "search_process_receipt_sha256", "precommit_sha256"}
    if (type(binding) is not dict or set(binding) != keys
            or any(not _declared_digest(binding[key]) for key in keys - {"run_id"})
            or type(binding["run_id"]) is not str or not binding["run_id"]
            or not _declared_digest(workspace_owner_sha256)
            or result.get("status") not in {"NOT_RUN", "FAILED", "TERMINAL_STOP"}
            or type(result.get("reason_code")) is not str or not 1 <= len(result["reason_code"]) <= 256
            or type(code_sha256) is not dict
            or set(code_sha256) != {"pm_historical_scheduler.py", "pm_end_grid.py", "pm_authority_spool.py"}
            or any(not _declared_digest(value) for value in code_sha256.values())):
        _fail("DECLARED_M5_REFUSAL_BINDING_INVALID")
    eligible = _declared_m5_refusal_eligible(resumed=resumed, source_attempted=source_attempted,
        owner_created=owner_created, owner_present=owner_present)
    eligible = eligible and result["status"] in {"NOT_RUN", "FAILED"}
    if binding["cell_id"] != cell["cell_id"]:
        _fail("DECLARED_M5_REFUSAL_BINDING_INVALID")
    disposition = {"stage": ("FRESH_PRE_SOURCE_NO_ACQUISITION" if eligible
        else "RESUMED_OR_SOURCE_WORK_RETAINED"), "fresh_workspace": not resumed, "resumed": resumed,
        "source_acquisition_attempted": source_attempted, "source_owner_created": owner_created,
        "source_products_acquired": (True if owner_created else False if not resumed and not source_attempted else None)}
    return {"schema_version": ("fxpm.declared-m5-terminal-refusal-fence.v1" if eligible
        else "fxpm.declared-m5-terminal-refusal-result.v1"),
        "workspace_owner_sha256": workspace_owner_sha256, "binding": dict(binding),
        "refusal": {"status": ("REFUSED_SOURCE_NOT_COMPLETED" if eligible else "REFUSED_CELL_NOT_COMPLETED"),
            "scheduler_status": result["status"], "reason_code": result["reason_code"],
            "recorded_at_utc": _declared_m5_grid_utc(recorded_at_utc), "slot_id": slot["slot_id"],
            "symbol": cell["symbol"], "timeframe": cell["timeframe"], "regime": cell["regime"],
            "cycle_start_utc": _declared_m5_grid_utc(slot["activation_utc"]),
            "cycle_end_utc": _declared_m5_grid_utc(slot["end_exclusive_utc"]),
            "information_cutoff_utc": _declared_m5_grid_utc(slot["native_cutoff_utc"])},
        "source_disposition": disposition, "code_sha256": dict(code_sha256), "end_grid_rows": {},
        "qualification_authority": False, "capital_authority": False, "live_authority": False,
        "order_authority": False, "full_native_context_eligibility": False, "financial_consumer_ready": False}



def _declared_m5_source_abort_eligible(*, resumed, source_attempted, owner_created,
        owner_present, controller_started, dispatch_lease_held, phase):
    facts = (resumed, source_attempted, owner_created, owner_present,
        controller_started, dispatch_lease_held)
    if any(type(value) is not bool for value in facts):
        _fail("DECLARED_M5_ABORT_FACTS_INVALID")
    return (not resumed and source_attempted and owner_created and owner_present
        and not controller_started and dispatch_lease_held
        and phase in {"SOURCE_ACQUISITION", "SOURCE_HEADER"})


def _declared_m5_source_abort_paths_clear(context, precommit, cell_id):
    """Reuse the original pre-controller namespaces; any present header retains."""
    import pm_authority_spool as spool
    import pm_historical_search_producer as producer
    base = f"declared-slots/{precommit.receipt_sha256}/{cell_id}"
    prior = (base + "/attempts", base + "/commits", base + "/run.lock",
        producer._declared_run_path(precommit, cell_id),
        producer._declared_terminal_path(precommit, cell_id),
        f"declared-slots/{precommit.receipt_sha256}/requests/{cell_id}",
        producer._declared_m5_cell_source_path(precommit, cell_id))
    for name in prior:
        path = spool._local_path(context.root, name)
        spool._reject_reparse_chain(path)
        try:
            path.lstat()
        except FileNotFoundError:
            continue
        return False
    return True


def _declared_m5_exception_reason(error):
    """Keep diagnostic exception text outside the canonical STOP reason."""
    import re
    import pm_authority_spool as spool
    import pm_end_grid as grid
    diagnostics = {"exception_type": type(error).__name__[:256]}
    try:
        candidate = getattr(error, "code", None)
        if candidate is None and isinstance(error, (spool.AuthoritySpoolError, grid.EndGridError)):
            candidate = str(error).split(":", 1)[0]
    except Exception:
        candidate = None
    if type(candidate) is str and re.fullmatch(r"[A-Z0-9][A-Z0-9_:.\-]{0,127}", candidate) is not None:
        return candidate, diagnostics
    diagnostics["rejected_reason_candidate"] = candidate[:256] if type(candidate) is str else None
    diagnostics["rejected_reason_candidate_type"] = type(candidate).__name__[:256]
    return "UNEXPECTED_EXCEPTION", diagnostics


def _declared_m5_capture_source_abort(*, owner, context, precommit, process, cell_id,
        source_error, resumed, source_attempted, owner_created, controller_started,
        dispatch_lease_held, phase):
    """Called inside the original held lease, before the ordinary owner close."""
    if not _declared_m5_source_abort_eligible(resumed=resumed, source_attempted=source_attempted,
            owner_created=owner_created, owner_present=owner is not None,
            controller_started=controller_started, dispatch_lease_held=dispatch_lease_held, phase=phase):
        return None
    import pm_authority_spool as spool
    import pm_m5_job_view as views
    if type(owner) is not views.M5JobViews:
        _fail("DECLARED_M5_ABORT_ORIGINAL_OWNER_REQUIRED")
    if not _declared_m5_source_abort_paths_clear(context, precommit, cell_id):
        _fail("DECLARED_M5_ABORT_PROGRESS_OR_HEADER_PRESENT")
    code, _ = _declared_m5_exception_reason(source_error)
    reason = ("DISK_LOW" if spool.is_resource_capacity_error(source_error)
        or code == "DECLARED_M5_DISK_LOW" else "BUSY" if code == "DECLARED_SLOT_RUN_ACTIVE" else code)
    if reason == "BUSY":
        return None
    report = owner.abort_before_consumption(reason)
    binding = report.get("binding") if type(report) is dict else None
    expected = {"generation_context_sha256": context.context_sha256,
        "precommit_sha256": precommit.receipt_sha256,
        "search_process_receipt_sha256": process.receipt_sha256, "cell_id": cell_id}
    if (type(binding) is not dict or any(binding.get(key) != value for key, value in expected.items())
            or report.get("reason") != reason):
        _fail("DECLARED_M5_ABORT_REPORT_BINDING_INVALID")
    return report


def _declared_m5_source_abort_body(*, abort_report, binding, workspace_owner_sha256,
        result, slot, cell, code_sha256, recorded_at_utc):
    """Project the actual unused-owner report; no completion or consumer is issued."""
    if (type(abort_report) is not dict
            or abort_report.get("schema_version") != "fxpm.m5-abort-before-consumption.v1"
            or abort_report.get("status") != "ABORTED_SOURCE_BEFORE_CONSUMPTION"
            or abort_report.get("reason") != result.get("reason_code")
            or result.get("status") not in {"NOT_RUN", "FAILED"}
            or abort_report.get("owner_closed") is not True
            or abort_report.get("source_complete") is not False
            or abort_report.get("job_complete") is not False
            or abort_report.get("retirement_authority") is not False):
        _fail("DECLARED_M5_ABORT_REPORT_BINDING_INVALID")
    job = abort_report.get("binding")
    fields = {"generation_context_sha256", "run_id", "cell_id",
        "search_process_receipt_sha256", "precommit_sha256"}
    if type(job) is not dict or any(job.get(key) != binding.get(key) for key in fields):
        _fail("DECLARED_M5_ABORT_REPORT_BINDING_INVALID")
    body = _declared_m5_refusal_body(binding=binding, workspace_owner_sha256=workspace_owner_sha256,
        result=result, slot=slot, cell=cell, code_sha256=code_sha256,
        recorded_at_utc=recorded_at_utc, resumed=False, source_attempted=True,
        owner_created=True, owner_present=True)
    body.update(schema_version="fxpm.declared-m5-post-source-abort-fence.v1",
        source_disposition={"stage": "FRESH_POST_SOURCE_PRE_CONTROLLER_ABORT",
            "fresh_workspace": True, "resumed": False, "source_acquisition_attempted": True,
            "source_owner_created": True, "source_products_acquired": True, "controller_started": False},
        abort_report=abort_report, bound_fitted_header=None)
    body["refusal"]["status"] = "REFUSED_SOURCE_NOT_COMPLETED"
    return body


def _declared_m5_abort_grid_identity(abort_report):
    import pm_authority_spool as spool
    fields = spool.POST_SOURCE_ABORT_GRID_SOURCE_FIELDS
    source = abort_report.get("source_identity") if type(abort_report) is dict else None
    if (type(source) is not dict
            or any(type(source.get(key)) is not str or not source[key] for key in fields)):
        _fail("DECLARED_M5_ABORT_REPORT_BINDING_INVALID")
    return {key: source[key] for key in fields}


def _declared_m5_refusal_archived_row(result, record):
    refusal = record.get("terminal_refusal")
    if (record.get("result_kind") not in {
            "TERMINAL_REFUSAL_SOURCE_NOT_COMPLETED", "POST_SOURCE_ABORT_SOURCE_NOT_COMPLETED"}
            or type(refusal) is not dict or refusal.get("scheduler_status") not in {"NOT_RUN", "FAILED"}
            or type(refusal.get("reason_code")) is not str or not refusal["reason_code"]):
        _fail("DECLARED_M5_REFUSAL_ARCHIVE_INVALID")
    result.update(status=refusal["scheduler_status"], reason_code=refusal["reason_code"],
        generation_context_sha256=record["generation_context_sha256"],
        archive_record={"path": record["retirement_receipt_path"], "sha256": record["retirement_receipt_sha256"]},
        archived_result_reference=record["source_fence"], retained_end_grid_rows=record["end_grid_rows"],
        terminal_refusal=dict(refusal), terminal_refusal_workspace_status="RELEASED_RECORDING_ONLY",
        descriptor_assurance="IDENTITY_ONLY_AFTER_WORKSPACE_RELEASE", genuine_cold_reopen_available=False,
        precommit_sha256=record["precommit_sha256"], run_receipt=None, account_descriptor=None)
    if record["result_kind"] == "POST_SOURCE_ABORT_SOURCE_NOT_COMPLETED":
        result["source_abort"] = dict(record["source_abort"])
    return result


def _declared_m5_existing_refusal_record(path):
    """Observe existing canonical bytes only; never create or complete a fence."""
    from pathlib import Path
    import pm_authority_spool as spool
    path = Path(path)
    if not path.is_absolute():
        _fail("DECLARED_M5_FINAL_PARENT_REQUIRED")
    spool._reject_reparse_chain(path)
    if not path.exists():
        return {"status": "ABSENT", "path": str(path)}
    body, digest = spool._retirement_control(path.parent, path.name, 4 * 1024**2)
    if body.get("schema_version") not in {
            "fxpm.declared-m5-development-source-fence.v1",
            "fxpm.declared-m5-terminal-refusal-fence.v1",
            "fxpm.declared-m5-terminal-refusal-result.v1",
            "fxpm.declared-m5-post-source-abort-fence.v1"}:
        _fail("DECLARED_M5_EXISTING_RECORD_SCHEMA_INVALID")
    return {"status": "DURABLE_EXISTING_RECORD", "schema_version": body["schema_version"],
        "reference": {"path": str(path), "sha256": digest,
            "size_bytes": len(spool._canonical_json_bytes(body, 4 * 1024**2)),
            "readback_sha256": digest, "written": False, "read_only_existing": True},
        "assurance": "EXISTING_BYTES_ONLY_NOT_NEW_SOURCE_VERIFICATION"}


def _declared_m5_reuse_recorded_rows(result, *, end_grid_root, source_fence_path,
        cell, slots, process_sha256):
    """Any retained cell/process row stops all new refusal writes."""
    import pm_end_grid as grid
    if not _declared_digest(process_sha256):
        _fail("DECLARED_M5_REFUSAL_BINDING_INVALID")
    by_start = {}
    for candidate in slots:
        start = _declared_m5_grid_utc(candidate["activation_utc"])
        if start in by_start:
            _fail("DECLARED_M5_REFUSAL_SLOT_INVALID")
        by_start[start] = candidate
    retained_cell = [item for item in grid.read_end_grid(end_grid_root, cell["symbol"])
        if item.cell_id == cell["cell_id"]]
    if any(item.cycle_start_utc in by_start and item.process_edition_sha256 != process_sha256
            for item in retained_cell):
        _fail("DECLARED_M5_GRID_PROCESS_CONFLICT")
    retained = [item for item in retained_cell if item.process_edition_sha256 == process_sha256]
    if not retained:
        return False
    path = str(grid._symbol_file(grid._grid_root(end_grid_root), cell["symbol"]))
    matching_stops = {}
    for recorded in retained:
        actual_slot = by_start.get(recorded.cycle_start_utc)
        if recorded.outcome != "STOP" or actual_slot is None:
            continue
        if (recorded.source_kind != "m5_derived" or recorded.timeframe != cell["timeframe"]
                or recorded.regime != cell["regime"]
                or recorded.cycle_end_utc != _declared_m5_grid_utc(actual_slot["end_exclusive_utc"])
                or recorded.information_cutoff_utc != _declared_m5_grid_utc(actual_slot["native_cutoff_utc"])
                or actual_slot["slot_id"] in matching_stops):
            _fail("DECLARED_M5_GRID_READBACK_MISMATCH")
        matching_stops[actual_slot["slot_id"]] = {"path": path, "row_sha256": recorded.row_sha256,
            "readback_row_sha256": recorded.row_sha256, "appended": False, "read_only_existing": True,
            "reason_code": recorded.reason_code}
    result.update(terminal_refusal_retained_rows=[{"path": path, "row_sha256": item.row_sha256,
            "outcome": item.outcome, "cycle_start_utc": item.cycle_start_utc} for item in retained],
        terminal_refusal_record_status=("EXISTING_DURABLE_STOP" if matching_stops
            and all(item.outcome == "STOP" for item in retained) else "EXISTING_CELL_PROCESS_ROWS"),
        terminal_refusal_workspace_status="RETAINED_EXISTING_RECORD_ONLY")
    if matching_stops:
        result["terminal_refusal_end_grid"] = matching_stops
        result["terminal_refusal_reason_codes"] = {key: value["reason_code"] for key, value in matching_stops.items()}
        if len(matching_stops) == 1:
            result["terminal_refusal_reason_code"] = next(iter(matching_stops.values()))["reason_code"]
    observed = _declared_m5_existing_refusal_record(source_fence_path)
    result["terminal_refusal_existing_fence_status"] = observed["status"]
    if observed["status"] == "DURABLE_EXISTING_RECORD":
        result["terminal_refusal_record"] = observed["reference"]
        result["terminal_refusal_existing_record_assurance"] = observed["assurance"]
    return True


def _declared_m5_stop_commit_reason(commit, *, context_sha256, precommit_sha256, cell_id, slot):
    """The durable original STOP completion supplies the technical reason."""
    expected = {"generation_context_sha256": context_sha256, "precommit_sha256": precommit_sha256,
        "cell_id": cell_id, "slot_id": slot["slot_id"], "ordinal": slot["ordinal"]}
    completion = commit.get("completion") if type(commit) is dict else None
    if (type(commit) is not dict or any(commit.get(key) != value for key, value in expected.items())
            or type(completion) is not dict or completion.get("status") != "STOP"
            or completion.get("cell_id") != cell_id or completion.get("slot_id") != slot["slot_id"]
            or type(completion.get("reason_code")) is not str or not completion["reason_code"]):
        _fail("DECLARED_M5_REFUSAL_STOP_COMMIT_INVALID")
    return completion["reason_code"]


def _declared_m5_stop_grid_identity(source_session, *, symbol):
    """Project recorded session DATA; no fresh Native assurance is supplied."""
    if source_session is None:
        return {}
    if type(source_session) is not dict:
        _fail("DECLARED_M5_STOP_SOURCE_IDENTITY_INVALID")
    if (source_session.get("failure") is not None or source_session.get("source_state") != "AVAILABLE"
            or source_session.get("cost_status") != "COST_READY"):
        return {}
    import pm_authority_spool as spool
    if (source_session.get("source_kind") != "m5_derived"
            or source_session.get("unverified_caller_source_or_fit_accepted") is not False
            or any(source_session.get(key) is not False for key in
                ("qualification_authority", "capital_authority", "live_authority", "order_authority"))):
        _fail("DECLARED_M5_STOP_SOURCE_IDENTITY_INVALID")
    source = source_session.get("m5_source_identity")
    fields = spool.POST_SOURCE_ABORT_GRID_SOURCE_FIELDS
    if (type(source) is not dict or any(type(source.get(key)) is not str for key in fields)
            or source["symbol"] != symbol or source["timeframe"] != "M5" or source["spread_unit"] != "points"
            or any(not _declared_digest(source[key]) for key in fields if key.endswith("sha256"))):
        _fail("DECLARED_M5_STOP_SOURCE_IDENTITY_INVALID")
    return {key: source[key] for key in fields}


def _declared_m5_recorded_stop_grid_identity(context, *, commit, precommit, cell, slot):
    """Read committed provenance, not a fresh source/fit completion check."""
    _declared_m5_stop_commit_reason(commit, context_sha256=context.context_sha256,
        precommit_sha256=precommit.receipt_sha256, cell_id=cell["cell_id"], slot=slot)
    completion = commit["completion"]
    entry = completion.get("source_session_entry")
    if entry is None:
        return {}
    import pm_authority_spool as spool
    import pm_historical_production_adapter as adapter
    body = completion.get("request_body")
    if (type(body) is not dict or type(entry) is not dict
            or type(commit.get("artifact_roster")) is not list or entry not in commit["artifact_roster"]
            or type(body.get("artifact_namespace")) is not str or not body["artifact_namespace"]
            or type(entry.get("artifact_id")) is not str
            or not entry["artifact_id"].startswith(body["artifact_namespace"] + "/")
            or type(entry.get("descriptor")) is not dict
            or entry["descriptor"].get("format") != spool.CANONICAL_JSON_FORMAT):
        _fail("DECLARED_M5_STOP_SOURCE_IDENTITY_INVALID")
    source = adapter._reopen_ascii_json_entry(context, entry,
        expected_type="declared_slot_source_session", code="DECLARED_SLOT_SOURCE_INVALID")
    source = adapter._validate_declared_m5_source_session(context, body=body,
        source_session=source, request_sha256=completion["request_sha256"], parent=precommit.to_dict(),
        completed_at_utc=completion["completed_at_utc"])
    return _declared_m5_stop_grid_identity(source, symbol=cell["symbol"])


def _declared_m5_record_terminal_refusal(result, *, context, workspace_sha, precommit, process,
        frozen, config, contract, row, cell_contract_sha256, resumed, source_attempted, owner_created,
        owner, phase, run_body, abort_report=None):
    """Append honest STOP metadata; retire only an earned explicit slice."""
    if (result.get("status") not in {"NOT_RUN", "FAILED", "TERMINAL_STOP"}
            or not result.get("reason_code") or result.get("reason_code") == "BUSY"
            or result.get("terminal_refusal_workspace_status") == "RELEASED_RECORDING_ONLY"
            or result.get("reason_code") == "RETIREMENT_INCOMPLETE"
            or result.get("end_grid_descriptor") is not None):
        return
    if any(value is None for value in (context, workspace_sha, precommit, process, frozen, config, cell_contract_sha256)):
        result["terminal_refusal_record_status"] = "ADMISSION_ONLY_RETAINED"
        return
    import hashlib
    from pathlib import Path
    import pm_authority_spool as spool
    import pm_end_grid as grid
    import pm_historical_anchor_orchestration as orchestration
    slots = orchestration.declared_slot_selected_slots(precommit.to_dict())
    slot = _declared_m5_refusal_slot(slots, phase=phase, run_body=run_body, resumed=resumed)
    with _declared_dispatch_lease(context) as acquired:
        if not acquired:
            result["terminal_refusal_record_status"] = "BUSY_RETAINED"
            return
        workspace, actual_workspace_sha = spool.read_owned_generation_workspace(context)
        if (actual_workspace_sha != workspace_sha or workspace["cell_contract_sha256"] != cell_contract_sha256
                or workspace["cell_id"] != row["cell"]["cell_id"]):
            _fail("DECLARED_M5_WORKSPACE_OWNER_CHANGED")
        if not _declared_disk_ready(context.root, contract, context.limits.min_free_bytes):
            result["terminal_refusal_record_status"] = "DISK_LOW_RETAINED"
            return
        if _declared_m5_reuse_recorded_rows(result, end_grid_root=row["m5_development"]["end_grid_root"],
                source_fence_path=row["m5_development"]["source_fence_path"],
                cell=row["cell"], slots=slots, process_sha256=process.receipt_sha256):
            return
        if slot is None:
            result["terminal_refusal_record_status"] = "CYCLE_UNRESOLVED_RETAINED"
            return
        refusal_result = dict(result)
        if result["status"] == "TERMINAL_STOP":
            import pm_historical_search_producer as producer
            saved = producer._read_declared_commit(context,
                producer._declared_commit_path(precommit, row["cell"]["cell_id"], slot["ordinal"]))
            refusal_result["reason_code"] = _declared_m5_stop_commit_reason(
                None if saved is None else saved[0], context_sha256=context.context_sha256,
                precommit_sha256=precommit.receipt_sha256, cell_id=row["cell"]["cell_id"], slot=slot)
        code = {"pm_historical_scheduler.py": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
            "pm_end_grid.py": hashlib.sha256(Path(grid.__file__).read_bytes()).hexdigest(),
            "pm_authority_spool.py": hashlib.sha256(Path(spool.__file__).read_bytes()).hexdigest()}
        binding = {"generation_context_sha256": context.context_sha256, "run_id": row["run_id"],
            "cell_id": row["cell"]["cell_id"], "cell_contract_sha256": cell_contract_sha256,
            "search_process_receipt_sha256": process.receipt_sha256, "precommit_sha256": precommit.receipt_sha256}
        produced = _declared_m5_grid_utc(_declared_utc_now().strftime("%Y-%m-%dT%H:%M:%SZ"))
        body = _declared_m5_refusal_body(binding=binding, workspace_owner_sha256=workspace_sha,
            result=refusal_result, slot=slot, cell=row["cell"], code_sha256=code, recorded_at_utc=produced,
            resumed=resumed, source_attempted=source_attempted, owner_created=owner_created,
            owner_present=owner is not None)
        if abort_report is not None:
            body = _declared_m5_source_abort_body(abort_report=abort_report,
                binding=binding, workspace_owner_sha256=workspace_sha, result=refusal_result,
                slot=slot, cell=row["cell"], code_sha256=code, recorded_at_utc=produced)
        refusal = body["refusal"]
        grid_identity = {} if abort_report is None else _declared_m5_abort_grid_identity(abort_report)
        if abort_report is None and result["status"] == "TERMINAL_STOP":
            try:
                grid_identity = _declared_m5_recorded_stop_grid_identity(context, commit=saved[0],
                    precommit=precommit, cell=row["cell"], slot=slot)
            except Exception as identity_error:
                code_value = getattr(identity_error, "code", type(identity_error).__name__)
                result["terminal_refusal_source_identity_error"] = (code_value[:256]
                    if type(code_value) is str else type(identity_error).__name__[:256])
        stopped = grid.build_end_grid_row(symbol=row["cell"]["symbol"], timeframe=row["cell"]["timeframe"],
            regime=row["cell"]["regime"], cycle_start_utc=refusal["cycle_start_utc"],
            cycle_end_utc=refusal["cycle_end_utc"], information_cutoff_utc=refusal["information_cutoff_utc"],
            outcome="STOP", reason_code=refusal["reason_code"], source_kind="m5_derived",
            source_contract_sha256=None, no_trade_run_receipt_sha256=None, view_key=None,
            view_contract_sha256=None, source_identity=grid_identity, cost_tier="NONE",
            process_edition_sha256=process.receipt_sha256, code_sha256=code, metrics={},
            strategy_family=None, configuration_sha256=None,
            job_binding_sha256=None if abort_report is None else abort_report["job_binding_sha256"],
            consumer_binding_sha256=None, produced_at_utc=produced)
        appended = grid.EndGridWriter(row["m5_development"]["end_grid_root"]).append(stopped)
        matched = [item for item in grid.read_end_grid(row["m5_development"]["end_grid_root"], row["cell"]["symbol"])
            if item.row_sha256 == stopped.row_sha256]
        if len(matched) != 1 or matched[0].to_bytes() != stopped.to_bytes():
            _fail("DECLARED_M5_GRID_READBACK_MISMATCH")
        reference = {**appended, "readback_row_sha256": matched[0].row_sha256}
        result["terminal_refusal_end_grid"] = {slot["slot_id"]: reference}
        body["end_grid_rows"] = {slot["slot_id"]: {key: reference[key] for key in ("path", "row_sha256")}}
        dev = row["m5_development"]
        saved = _declared_m5_external_record(dev["source_fence_path"], body, maximum_bytes=4 * 1024**2)
        result.update(terminal_refusal_record=saved, terminal_refusal=dict(refusal),
            terminal_refusal_record_status="DURABLE_STOP", terminal_refusal_workspace_status="RETAINED")
        if body["schema_version"] not in {
                "fxpm.declared-m5-terminal-refusal-fence.v1", "fxpm.declared-m5-post-source-abort-fence.v1"}:
            return
        protected = _declared_m5_protected_paths(contract, row, config)
        policy = dev["retirement"]
        archived = spool.retire_completed_generation(context.root, expected_context_sha256=context.context_sha256,
            workspace_owner_sha256=workspace_sha, source_fence_path=Path(dev["source_fence_path"]),
            source_fence_sha256=saved["sha256"], retirement_receipt_path=Path(policy["receipt_path"]),
            protected_paths=protected,
            owned_direct_files=(f"declared-slots/{precommit.receipt_sha256}/precommit.json",), unit_lock_paths=(),
            max_paths=policy["max_paths"], max_nodes=policy["max_nodes"], max_bytes=policy["max_bytes"])
        _declared_m5_refusal_archived_row(result, archived)


def _declared_m5_run_one_cell(contract, row):
    """Existing worker, explicit development source; no implicit run allocation."""
    from dataclasses import asdict
    from datetime import timedelta
    from pathlib import Path
    import os
    import pm_authority_spool as spool
    result = _declared_cell_row(row, "FAILED", pid=os.getpid())
    result.update(source_kind="m5_derived", result_kind="DEVELOPMENT_PARTIAL",
        design_contaminated=True, pristine_validation=False)
    owner, root = None, None
    context = workspace_sha = precommit = process = frozen = config = digest = None
    resumed, source_attempted, owner_created = False, False, False
    controller_started, abort_report = False, None
    phase, run_body = "PRE_SOURCE", None
    try:
        checked, _ = _declared_m5_contract(contract)
        if row != checked["cells"][0]:
            _fail("DECLARED_M5_CELL_DESCRIPTOR_INVALID")
        dev = row["m5_development"]
        _declared_m5_dependencies()
        import pm_book_multiplicity as book
        import pm_forward_oos_replay_producer as forward
        import pm_historical_anchor_orchestration as orchestration
        import pm_historical_search_producer as producer
        import pm_m5_derived_costs as costs
        import pm_portfolio_replay as portfolio
        import pm_production_anchor_plan as anchors
        import pm_production_config_binding as binding
        import pm_production_search_planner as planner
        root = Path(row["root"])
        digest = _declared_m5_cell_contract_sha256(contract, row)
        archived = spool.read_generation_retirement(root)
        if archived is not None:
            if archived["status"] == "ARCHIVED_RECORD_ONLY":
                if archived["cell_contract_sha256"] != digest:
                    _fail("DECLARED_M5_ARCHIVE_CONTRACT_CHANGED")
                if archived.get("result_kind") in {
                        "TERMINAL_REFUSAL_SOURCE_NOT_COMPLETED", "POST_SOURCE_ABORT_SOURCE_NOT_COMPLETED"}:
                    return _declared_m5_refusal_archived_row(result, archived)
                return _declared_m5_archived_row(result, archived)
            result.update(status="NOT_RUN", reason_code="RETIREMENT_INCOMPLETE")
            return result
        if not _declared_disk_ready(root, contract, contract["spool_limits"]["min_free_bytes"]):
            result.update(status="NOT_RUN", reason_code="DISK_LOW")
            return result
        resumed = (root / spool.CONTEXT_RELATIVE_PATH).exists()
        if resumed:
            context = spool.reopen_generation_context(root, expected_generation_id=row["run_id"],
                expected_native_universe_sha256=contract["native_universe_sha256"],
                expected_config_sha256=contract["config_sha256"])
            if asdict(context.limits) != contract["spool_limits"]:
                _fail("DECLARED_M5_CONTEXT_LIMITS_CHANGED")
        else:
            if root.exists() and any(root.iterdir()):
                _fail("DECLARED_M5_FRESH_ROOT_NOT_EMPTY")
            context = spool.freeze_generation_context(root, generation_id=row["run_id"],
                native_universe_sha256=contract["native_universe_sha256"], config_sha256=contract["config_sha256"],
                limits=spool.SpoolLimits(**contract["spool_limits"]),
                owned_workspace={"cell_id": row["cell"]["cell_id"], "cell_contract_sha256": digest})
        workspace, workspace_sha = spool.read_owned_generation_workspace(context)
        if workspace["cell_contract_sha256"] != digest or workspace["cell_id"] != row["cell"]["cell_id"]:
            _fail("DECLARED_M5_WORKSPACE_OWNER_CHANGED")
        with _declared_dispatch_lease(context) as acquired:
            if not acquired:
                result.update(status="NOT_RUN", reason_code="BUSY")
                return result
            try:
                payload, supplied = binding.load_effective_pipeline_config(Path(contract["active_config_path"]))
                frozen = binding.freeze_production_pipeline_config(context,
                    active_config_path=Path(contract["active_config_path"]), config_payload=payload, supplied_config=supplied)
                config = frozen.materialize(context)
                records = sorted((context.root / "declared-slots").glob("*/precommit.json"))
                if len(records) > 1:
                    _fail("DECLARED_SCHEDULER_MULTIPLE_PRECOMMITS")
                if resumed:
                    if len(records) != 1:
                        _fail("DECLARED_M5_RESUME_PRECOMMIT_MISSING")
                    process = _declared_reopen_process(context, records[0])
                    precommit = orchestration.reopen_declared_slot_owner_run_precommit(context,
                        receipt_sha256=records[0].parent.name, process=process, frozen_pipeline_config=frozen)
                else:
                    if records:
                        _fail("DECLARED_M5_FRESH_PRECOMMIT_ALREADY_EXISTS")
                    process = planner.freeze_production_search_process(context, run_id=row["run_id"])
                    roster = anchors.derive_declared_owner_slot_roster_v2(launch_activation_utc=row["launch_activation_utc"],
                        frozen_refit_clock=row["frozen_refit_clock"], config_identity_refs={
                            "config_sha256": context.config_sha256, "effective_config_sha256": frozen.effective_config_sha256},
                        extend_frozen_clock_backward=row["extend_frozen_clock_backward"],
                        protected_end_exclusive_utc=row["protected_end_exclusive_utc"])
                    if [slot["slot_id"] for slot in roster["slots"][:len(dev["development_slot_ids"])]] != dev["development_slot_ids"]:
                        _fail("DECLARED_M5_DEVELOPMENT_PREFIX_INVALID")
                    now, policy = _declared_utc_now(), row["source_access_window_policy"]
                    window = {"start_utc": (now - timedelta(seconds=policy["lead_seconds"])).strftime("%Y-%m-%dT%H:%M:%SZ"),
                        "end_exclusive_utc": (now + timedelta(seconds=policy["validity_seconds"])).strftime("%Y-%m-%dT%H:%M:%SZ")}
                    precommit = orchestration.precommit_declared_development_m5_slot_owner_run(context,
                        run_id=row["run_id"], process=process, slot_roster=roster, cell_roster=[row["cell"]],
                        frozen_pipeline_config=frozen, spread_profile_identity={"profile_sha256": contract["profile_sha256"]},
                        source_access_window_utc=window, launch_activation_utc=row["launch_activation_utc"],
                        m5_source_pins=dev["m5_source_pins"], development_slot_ids=dev["development_slot_ids"])
                parent = precommit.to_dict()
                slots = _declared_m5_check_roster(row, parent, context, frozen)
                result["precommit_sha256"] = precommit.receipt_sha256
                account_request = forward.declared_m5_development_cell_account_request(context, precommit=precommit,
                    process=process, frozen_pipeline_config=frozen)
                account_axis = next(axis for axis in dev["view_axes"] if axis["view_id"] == dev["account_view_id"])
                if _declared_m5_utc(account_request["cutoff_utc"]) != _declared_m5_utc(account_axis["cutoff_utc"]):
                    _fail("DECLARED_M5_ACCOUNT_AXIS_MISMATCH")
                window, _ = orchestration._declared_slot_effective_access(context, precommit, parent, slots[0])
                window_open = _declared_m5_utc(window["start_utc"]) <= _declared_utc_now() < _declared_m5_utc(window["end_exclusive_utc"])
                if not window_open and not resumed:
                    result.update(status="NOT_RUN", reason_code="WINDOW_EXPIRED")
                    return result
                snapshot, errors = portfolio.active_native_portfolio_contract(Path(config.data_dir),
                    book.PINNED_SYMBOL_ORDER, timeframes=book.TIMEFRAME_ORDER)
                if orchestration._declared_slot_native_snapshot_status(context, snapshot, errors) != "MATCH":
                    result.update(status="NOT_RUN", reason_code="NATIVE_DRIFT")
                    return result
                if resumed:
                    source_attempted, phase = True, "SOURCE_ACQUISITION"
                    source = producer.reopen_declared_m5_cell_source_context(context, precommit=precommit,
                        process=process, frozen_pipeline_config=frozen, cell_id=row["cell"]["cell_id"],
                        cell_contract_sha256=digest)
                    owner, edition = source["owner"], source["cost_edition"]
                    owner_created = True
                    if (source["slot_view_ids"] != dev["slot_view_ids"]
                            or source["calibration_view_id"] != dev["calibration_view_id"]
                            or owner.view_plan_sha256 != dev["m5_source_pins"]["view_plan_sha256"]
                            or edition.reopen_route() != contract["cost_edition_route"]):
                        _fail("DECLARED_M5_SOURCE_HEADER_BINDING_MISMATCH")
                else:
                    # First payload access: the fixed source window already exists.
                    source_attempted, phase = True, "SOURCE_ACQUISITION"
                    edition = costs.open_recorded_derived_costs_route(contract["cost_edition_route"])
                    if not _declared_disk_ready(root, contract, context.limits.min_free_bytes):
                        _fail("DECLARED_M5_DISK_LOW")
                    owner = _declared_m5_new_owner(context, precommit=precommit, process=process,
                        frozen=frozen, config=config, contract=contract, row=row, edition=edition)
                    owner_created, phase = True, "SOURCE_HEADER"
                    producer.publish_declared_m5_cell_source_context(context, precommit=precommit, process=process,
                        frozen_pipeline_config=frozen, cell_id=row["cell"]["cell_id"], cell_contract_sha256=digest,
                        m5_owner=owner, slot_view_ids=dev["slot_view_ids"],
                        calibration_view_id=dev["calibration_view_id"], cost_edition=edition)
                if not _declared_disk_ready(root, contract, context.limits.min_free_bytes):
                    _fail("DECLARED_M5_DISK_LOW")
                result["recorded_before_dispatch"] = (context.root / producer._declared_run_path(precommit, row["cell"]["cell_id"])).exists()
                controller_args = {"precommit": precommit, "process": process, "frozen_pipeline_config": frozen,
                    "cell_id": row["cell"]["cell_id"], "m5_owner": owner, "slot_view_ids": dev["slot_view_ids"],
                    "calibration_view_id": dev["calibration_view_id"], "cost_edition": edition,
                    "cell_contract_sha256": digest}
                controller_started = True
                phase = "PRODUCER_CONTROLLER"
                if window_open:
                    run = producer.execute_declared_cell_owner_slots(context=context, **controller_args)
                    run_body = run.to_dict()
                else:
                    progress = producer.reopen_declared_cell_owner_progress(context, **controller_args)
                    if progress.run_receipt is None:
                        result.update(status="NOT_RUN", reason_code="WINDOW_EXPIRED")
                        return result
                    run_body = progress.run_receipt
                result["run_receipt"] = _declared_run_descriptor(precommit, row, run_body)
                if run_body["terminal_stop"]:
                    result.update(status="TERMINAL_STOP", reason_code="RECORDED_TERMINAL_STOP"
                        if result["recorded_before_dispatch"] else "NEW_TERMINAL_STOP")
                    return result
                phase = "ACCOUNT"
                if not _declared_disk_ready(root, contract, context.limits.min_free_bytes):
                    _fail("DECLARED_M5_DISK_LOW")
                if window_open:
                    artifact = forward.produce_declared_m5_development_cell_account_artifact(context,
                        precommit=precommit, process=process, frozen_pipeline_config=frozen,
                        m5_owner=owner, account_view_id=dev["account_view_id"], cost_edition=edition)
                else:
                    artifact = forward.discover_declared_m5_development_cell_account_artifact(context,
                        precommit=precommit, process=process, frozen_pipeline_config=frozen)
                    if artifact is None:
                        result.update(status="NOT_RUN", reason_code="WINDOW_EXPIRED")
                        return result
                result["account_descriptor"] = dict(artifact.descriptor)
                phase = "FINALIZATION"
                grid, fence, direct = _declared_m5_finalize(context, precommit=precommit, process=process,
                    frozen=frozen, contract=contract, row=row, owner=owner, edition=edition,
                    run_body=run_body, artifact=artifact)
                result.update(status="REOPENED" if resumed else "COMPLETED", account_descriptor=dict(artifact.descriptor),
                    end_grid_descriptor=grid, source_fence_descriptor=fence)
                protected = _declared_m5_protected_paths(contract, row, config)
                policy = dev["retirement"]
                archived = spool.retire_completed_generation(context.root, expected_context_sha256=context.context_sha256,
                    workspace_owner_sha256=workspace_sha, source_fence_path=Path(dev["source_fence_path"]),
                    source_fence_sha256=fence["sha256"], retirement_receipt_path=Path(policy["receipt_path"]),
                    protected_paths=protected, owned_direct_files=direct[0], unit_lock_paths=direct[1],
                    max_paths=policy["max_paths"], max_nodes=policy["max_nodes"], max_bytes=policy["max_bytes"])
                return _declared_m5_archived_row(result, archived)
            except Exception as source_error:
                try:
                    abort_report = _declared_m5_capture_source_abort(owner=owner, context=context,
                        precommit=precommit, process=process, cell_id=row["cell"]["cell_id"],
                        source_error=source_error, resumed=resumed, source_attempted=source_attempted,
                        owner_created=owner_created, controller_started=controller_started,
                        dispatch_lease_held=acquired, phase=phase)
                except Exception as abort_error:
                    result["source_abort_error"] = str(getattr(abort_error, "code", str(abort_error)))[:256]
                raise
    except Exception as exc:
        code, diagnostics = _declared_m5_exception_reason(exc)
        result["exception_diagnostics"] = diagnostics
        if spool.is_resource_capacity_error(exc) or code == "DECLARED_M5_DISK_LOW":
            result.update(status="NOT_RUN", reason_code="DISK_LOW")
        elif code == "DECLARED_SLOT_RUN_ACTIVE":
            result.update(status="NOT_RUN", reason_code="BUSY")
        elif code == "GRID_EVIDENCE_KIND_UNSUPPORTED":
            result.update(status="NOT_RUN", reason_code=code)
        elif code.startswith(("RETIREMENT_", "SPOOL_RETIREMENT_", "WORKSPACE_ROOT_")):
            result.update(status="NOT_RUN", reason_code=code)
        else:
            result.update(status="FAILED", reason_code=code)
        if root is not None and (root / getattr(spool, "RETIREMENT_RELATIVE_PATH", "generation-retirement.json")).exists():
            result.update(run_receipt=None, account_descriptor=None,
                descriptor_assurance="IDENTITY_ONLY_RETIREMENT_INCOMPLETE", genuine_cold_reopen_available=False)
        return result
    finally:
        try:
            _declared_m5_record_terminal_refusal(result, context=context, workspace_sha=workspace_sha,
                precommit=precommit, process=process, frozen=frozen, config=config, contract=contract,
                row=row, cell_contract_sha256=digest, resumed=resumed, source_attempted=source_attempted,
                owner_created=owner_created, owner=owner, phase=phase, run_body=run_body,
                abort_report=abort_report)
        except Exception as record_error:
            result["terminal_refusal_record_error"] = str(getattr(record_error, "code", str(record_error)))[:256]
            if root is not None and (root / getattr(spool, "RETIREMENT_RELATIVE_PATH", "generation-retirement.json")).exists():
                result.update(descriptor_assurance="IDENTITY_ONLY_RETIREMENT_INCOMPLETE",
                    terminal_refusal_workspace_status="RETIREMENT_INCOMPLETE",
                    genuine_cold_reopen_available=False, run_receipt=None, account_descriptor=None)
        finally:
            if owner is not None:
                owner.close()


def _declared_m5_archived_row(result, record):
    result.update(status="ARCHIVED", reason_code="WORKSPACE_RELEASED_RECORDING_ONLY",
        generation_context_sha256=record["generation_context_sha256"],
        archive_record={"path": record["retirement_receipt_path"], "sha256": record["retirement_receipt_sha256"]},
        archived_result_reference=record["source_fence"], retained_end_grid_rows=record["end_grid_rows"],
        descriptor_assurance="IDENTITY_ONLY_AFTER_WORKSPACE_RELEASE", genuine_cold_reopen_available=False,
        precommit_sha256=record.get("precommit_sha256"),
        run_receipt=None, account_descriptor=None)
    return result


def _declared_m5_protected_paths(contract, row, config):
    from pathlib import Path
    import sys
    names = ("pm_historical_scheduler", "pm_authority_spool", "pm_authority_spool_quota",
        "pm_historical_search_producer", "pm_historical_production_adapter", "pm_authoritative_search_execution",
        "pm_forward_oos_replay_producer", "pm_causal_evidence_producer", "pm_m5_job_view", "pm_m5_derived_costs",
        "pm_explicit_derived_history", "pm_historical_anchor_orchestration", "pm_production_search_planner",
        "pm_training_horizon", "pm_winner_raw_evidence", "pm_core", "pm_end_grid", "pm_native_prefix_production",
        "pm_explicit_derived_native", "pm_explicit_derived_publication", "pm_explicit_observed_clock")
    files = []
    for name in names:
        module = sys.modules.get(name)
        file = getattr(module, "__file__", None)
        if type(file) is not str:
            _fail("DECLARED_M5_RETIREMENT_CODE_ORIGIN_UNAVAILABLE", name)
        files.append(Path(file).resolve())
    dev = row["m5_development"]
    return tuple(files) + (Path(config.data_dir).resolve(), Path(contract["cost_edition_route"]["root"]).resolve(),
        Path(contract["active_config_path"]).resolve(), Path(dev["end_grid_root"]).resolve(),
        Path(dev["source_fence_path"]).resolve())


def _declared_m5_no_plan_shape(verified):
    """Route metadata only; the original public verifier must earn acceptance."""
    return (type(verified) is dict and {"status", "reason_code", "plan_transport", "horizon_plan", "owner"}.issubset(verified)
        and verified.get("status") == "NO_TRADE"
        and verified.get("reason_code") == "REGIME_UNOBSERVED"
        and verified.get("plan_transport") is None and verified.get("horizon_plan") is None
        and verified.get("owner") is None)


def _declared_m5_owned_direct_files(context, precommit, row, *, process, frozen, m5_owner,
        cost_edition, commits):
    import pm_historical_search_producer as producer
    import pm_historical_production_adapter as adapter

    def verified_no_plan(slot):
        commit = commits[slot["slot_id"]]
        completion = commit["completion"]
        if not _declared_m5_no_plan_shape(completion):
            return None
        verified = adapter.verify_declared_slot_completion(context, precommit=precommit, process=process,
            completion=completion, artifact_roster=commit["artifact_roster"], frozen_pipeline_config=frozen,
            m5_owner=m5_owner, m5_cost_edition=cost_edition)
        if not _declared_m5_no_plan_shape(verified):
            _fail("RETIREMENT_NO_PLAN_VERIFICATION_REQUIRED")
        return verified

    cell = row["cell"]["cell_id"]
    return _declared_owned_direct_files(context, precommit, row,
        max_paths=row["m5_development"]["retirement"]["max_paths"], verified_no_plan=verified_no_plan,
        additional_direct=(producer._declared_terminal_path(precommit, cell),
                           producer._declared_m5_cell_source_path(precommit, cell)))


def _declared_owned_direct_files(context, precommit, row, *, max_paths, verified_no_plan=None, additional_direct=()):
    """Classify original request/unit metadata without opening Raw payloads."""
    from pathlib import Path
    import re
    import os
    import pm_authority_spool as spool
    import pm_authoritative_search_execution as execution
    import pm_historical_anchor_orchestration as orchestration
    import pm_historical_search_producer as producer
    cell = row["cell"]["cell_id"]
    base = f"declared-slots/{precommit.receipt_sha256}/{cell}"
    direct = {f"declared-slots/{precommit.receipt_sha256}/precommit.json", base + "/run.lock",
        producer._declared_run_path(precommit, cell), *additional_direct}
    unit_locks, observations = set(), []
    for slot in orchestration.declared_slot_selected_slots(precommit.to_dict()):
        verified = None if verified_no_plan is None else verified_no_plan(slot)
        direct.add(producer._declared_commit_path(precommit, cell, slot["ordinal"]))
        starts = producer._declared_prior_attempt_starts(context, precommit, cell, slot)
        if not starts:
            _fail("RETIREMENT_REQUEST_OWNERSHIP_UNRESOLVED")
        for start_ref in starts:
            relative = start_ref["relative_path"]
            start = execution._read_declared_unit_json(context, relative)
            attempt = Path(relative).stem
            request_path = f"declared-slots/{precommit.receipt_sha256}/requests/{cell}/{slot['slot_id']}/{attempt}.json"
            request = execution._read_declared_unit_json(context, request_path)
            if (type(request) is not dict or type(start) is not dict
                    or any(request.get(key) != value for key, value in (
                        ("precommit_sha256", precommit.receipt_sha256), ("cell_id", cell),
                        ("slot_id", slot["slot_id"]), ("attempt_id", attempt)))
                    or start.get("request_sha256") != _declared_m5_hash(request)):
                _fail("RETIREMENT_REQUEST_OWNERSHIP_UNRESOLVED")
            unit_base = str(Path(execution._declared_unit_relative(request, "header.json")).parent).replace("\\", "/")
            directory = spool._local_path(context.root, unit_base)
            if verified is not None:
                if _declared_m5_hash(request) != verified.get("request_sha256"):
                    _fail("RETIREMENT_NO_PLAN_VERIFICATION_REQUIRED")
                try:
                    directory.lstat()
                except FileNotFoundError:
                    pass
                else:
                    _fail("RETIREMENT_UNEXPECTED_UNIT_NAMESPACE")
                direct.update((relative, request_path))
                observations.append({"slot_id": slot["slot_id"], "request_sha256": _declared_m5_hash(request),
                    "route": "ORIGINAL_VERIFIER_REGIME_UNOBSERVED_NO_PLAN", "unit_namespace": "ABSENT"})
                continue
            header = execution._read_declared_unit_header(context, request)
            end_path = execution._declared_unit_relative(request, "evidence-completed.json")
            end = execution._read_declared_unit_json(context, end_path)
            if (type(header) is not dict or type(end) is not dict
                    or set(end) != {"binding_sha256", "unit_count", "unit_receipt_sha256", "evidence_access_completed_at_utc"}
                    or end["binding_sha256"] != _declared_m5_hash(header["binding"])
                    or type(end["unit_count"]) is not int or not 0 <= end["unit_count"] <= context.limits.max_artifacts_per_symbol
                    or type(end["unit_receipt_sha256"]) is not list
                    or len(end["unit_receipt_sha256"]) != end["unit_count"]
                    or any(not _declared_digest(value) for value in end["unit_receipt_sha256"])):
                _fail("RETIREMENT_UNIT_OWNERSHIP_UNRESOLVED")
            spool._reject_reparse_chain(directory)
            names = []
            with os.scandir(directory) as stream:
                for item in stream:
                    if len(names) >= max_paths:
                        _fail("RETIREMENT_UNIT_OWNERSHIP_BOUND")
                    names.append(item.name)
            fixed = {"header.json", "evidence-completed.json", "raw-completion.json", "units.lock"}
            ordinals = sorted(name for name in names if re.fullmatch(r"[0-9]{6}\.json", name))
            if any(name not in fixed and name not in ordinals for name in names) or len(ordinals) != end["unit_count"]:
                _fail("RETIREMENT_FOREIGN_UNIT_FILE")
            for name, expected in zip(ordinals, end["unit_receipt_sha256"], strict=True):
                unit_path = unit_base + "/" + name
                digest, _, encoded = spool._stream_file(context.root, unit_path,
                    maximum_bytes=spool.MAX_CONTEXT_BYTES, collect=True)
                unit = spool._strict_json_bytes(encoded, spool.MAX_CONTEXT_BYTES)
                if (digest != expected or type(unit) is not dict
                        or set(unit) != {"schema_version", "binding_sha256", "unit", "payload", "completed_at_utc"}
                        or unit["schema_version"] != execution._DECLARED_UNIT_SCHEMA
                        or unit["binding_sha256"] != end["binding_sha256"]
                        or type(unit["unit"]) is not dict or type(unit["unit"].get("ordinal")) is not int
                        or unit["unit"].get("ordinal") != int(name[:6])):
                    _fail("RETIREMENT_UNIT_OWNERSHIP_UNRESOLVED")
                spool._validate_descriptor(unit["payload"], content_addressed=True)
                direct.add(unit_path)
            lock = execution._declared_unit_relative(request, "units.lock")
            if "units.lock" not in names:
                _fail("RETIREMENT_UNIT_LOCK_UNVERIFIED")
            unit_locks.add(lock)
            direct.update((relative, request_path))
            direct.update(execution._declared_unit_relative(request, name) for name in fixed)
            observations.append({"slot_id": slot["slot_id"], "request_sha256": _declared_m5_hash(request),
                "route": "ORIGINAL_OBSERVED_UNIT_METADATA", "unit_count": end["unit_count"]})
    return tuple(sorted(direct)), tuple(sorted(unit_locks)), observations


__all__ += ["DECLARED_M5_DEVELOPMENT_SCHEDULER_SCHEMA"]
