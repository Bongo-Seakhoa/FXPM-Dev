"""Exact, invented unit-writer precommitment for the isolated Source340 witness."""

from datetime import datetime, timedelta, timezone
import hashlib
import os
from pathlib import Path
import sys

import pm_authority_spool as spool
import pm_book_multiplicity as book
from pm_end_grid import canonical_bytes
import pm_historical_anchor_orchestration as historical
import pm_production_anchor_plan as anchors
import pm_production_config_binding as config_binding
import pm_production_search_planner as planner
import pm_winner_selection as winner


UNIT_WRITER_PRECOMMIT_PATH = "unit-writer-precommit.json"
UNIT_WRITER_PRECOMMIT_SCHEMA = "fxpm.invented-historical-unit-writer-precommit.v1"
_BINDING_SCHEMA = "fxpm.invented-historical-unit-writer-binding.v1"
_CONSUMER_FIELDS = {"cell_id", "symbol", "timeframe", "regime", "root"}
_RUNTIME_MODULES = (
    "pm_historical_exec_cohort_stream",
    "pm_historical_persisted_exec",
    "pm_historical_cohort_consumer",
    "pm_historical_unit_writer",
    "pm_historical_unit_writer_issuer",
)


class UnitWriterIssuerError(ValueError):
    """The invented writer declaration cannot be issued or reopened."""


def _runtime_source_sha256():
    root = Path(__file__).resolve().parent
    sources = {}
    for name in _RUNTIME_MODULES:
        path = root / f"{name}.py"
        module = sys.modules.get(name)
        loaded_from = getattr(module, "__file__", None)
        if module is not None and (loaded_from is None or Path(loaded_from).resolve() != path):
            raise UnitWriterIssuerError("UNIT_WRITER_RUNTIME_SOURCE_CHANGED")
        try:
            with path.open("rb") as source:
                sources[name] = hashlib.file_digest(source, "sha256").hexdigest()
        except OSError as exc:
            raise UnitWriterIssuerError("UNIT_WRITER_RUNTIME_SOURCE_UNAVAILABLE") from exc
    return sources


def _timestamp(now):
    value = datetime.now(timezone.utc) if now is None else now
    if not isinstance(value, datetime) or value.tzinfo is None:
        raise UnitWriterIssuerError("UNIT_WRITER_CLOCK_INVALID")
    return value.astimezone(timezone.utc).replace(microsecond=0)


def _utc_text(value):
    return value.strftime("%Y-%m-%dT%H:%M:%SZ")


def _ordered_consumers(rows):
    if type(rows) not in (list, tuple) or not rows:
        raise UnitWriterIssuerError("UNIT_WRITER_ROSTER_INVALID")
    ordered, identities, roots = [], set(), set()
    for row in rows:
        if type(row) is not dict or set(row) != _CONSUMER_FIELDS:
            raise UnitWriterIssuerError("UNIT_WRITER_ROSTER_INVALID")
        symbol, timeframe, regime = (row[key] for key in ("symbol", "timeframe", "regime"))
        if (symbol not in historical.DECLARED_DEVELOPMENT_SYMBOLS or timeframe not in book.TIMEFRAME_ORDER
                or regime not in book.REGIME_ORDER or row["cell_id"] != winner.compute_cell_id(symbol, timeframe, regime)
                or not isinstance(row["root"], (str, Path))):
            raise UnitWriterIssuerError("UNIT_WRITER_ROSTER_INVALID")
        root = Path(row["root"])
        if not root.is_absolute():
            raise UnitWriterIssuerError("UNIT_WRITER_ROSTER_INVALID")
        spool._reject_reparse_chain(root)
        root = root.resolve()
        if row["cell_id"] in identities or any(
                root.is_relative_to(prior) or prior.is_relative_to(root) for prior in roots):
            raise UnitWriterIssuerError("UNIT_WRITER_ROSTER_DUPLICATE_OR_OVERLAP")
        if ordered and (symbol != ordered[0]["symbol"] or timeframe != ordered[0]["timeframe"]):
            raise UnitWriterIssuerError("UNIT_WRITER_ROSTER_AXES_CHANGED")
        identities.add(row["cell_id"])
        roots.add(root)
        ordered.append({**row, "root": str(root)})
    return ordered


def _unit_writer_binding(*, unit_id, unit_contract_sha256, consumer_cells, slot_roster,
        prerequisite_context, process, frozen_pipeline_config, activation_manifest_sha256,
        spread_profile_sha256, source_access_window_policy, cohort_roster_sha256, max_rows, max_bytes, now=None):
    """Prepare exact metadata for the owner declaration, without issuing anything."""
    if not all(spool._is_sha256(value) for value in (unit_id, unit_contract_sha256,
            activation_manifest_sha256, spread_profile_sha256, cohort_roster_sha256)):
        raise UnitWriterIssuerError("UNIT_WRITER_DIGEST_INVALID")
    if (type(prerequisite_context) is not spool.GenerationSpoolContext
            or type(process) is not planner.FrozenProductionSearchProcess
            or type(frozen_pipeline_config) is not config_binding.FrozenProductionPipelineConfig):
        raise UnitWriterIssuerError("UNIT_WRITER_PREREQUISITE_INVALID")
    if (type(max_rows) is not int or max_rows <= 0 or type(max_bytes) is not int or max_bytes <= 0):
        raise UnitWriterIssuerError("UNIT_WRITER_BOUNDS_INVALID")
    policy = source_access_window_policy
    if (type(policy) is not dict or set(policy) != {"lead_seconds", "validity_seconds"}
            or type(policy["lead_seconds"]) is not int or policy["lead_seconds"] < 0
            or type(policy["validity_seconds"]) is not int or policy["validity_seconds"] <= 0):
        raise UnitWriterIssuerError("UNIT_WRITER_WINDOW_POLICY_INVALID")
    cells = _ordered_consumers(consumer_cells)
    if type(slot_roster) is not dict or slot_roster.get("schema_version") != anchors.DECLARED_OWNER_SLOT_ROSTER_V2_SCHEMA:
        raise UnitWriterIssuerError("UNIT_WRITER_V2_ROSTER_REQUIRED")
    roster = historical._declared_roster(slot_roster, launch_activation_utc=slot_roster.get("start_utc"),
        protected_end_exclusive_utc=historical._DECLARED_PROTECTED_END_EXCLUSIVE_UTC)
    config_refs = {"config_sha256": prerequisite_context.config_sha256,
        "effective_config_sha256": frozen_pipeline_config.effective_config_sha256}
    if roster["config_identity_refs"] != config_refs or frozen_pipeline_config.config_sha256 != config_refs["config_sha256"]:
        raise UnitWriterIssuerError("UNIT_WRITER_CONFIG_ROSTER_CHANGED")
    selection = historical.declared_ordinary_period_selection(roster, mode="one_period", symbols=(cells[0]["symbol"],))
    process_body = process.to_dict()
    if (process_body.get("schema_version") != planner.INDEPENDENT_R_PROCESS_SCHEMA
            or process_body.get("execution_edition") != planner.INDEPENDENT_R_EXECUTION_EDITION):
        raise UnitWriterIssuerError("UNIT_WRITER_V19_REQUIRED")
    observed = _timestamp(now)
    window = {"start_utc": _utc_text(observed - timedelta(seconds=policy["lead_seconds"])),
        "end_exclusive_utc": _utc_text(observed + timedelta(seconds=policy["validity_seconds"]))}
    return {"schema_version": _BINDING_SCHEMA, "authority_scope": "INVENTED_SOFTWARE_ONLY", "unit_id": unit_id,
        "unit_contract_sha256": unit_contract_sha256, "consumer_cells": cells, "slot_roster": roster,
        "runtime_source_sha256": _runtime_source_sha256(),
        "slot_roster_sha256": hashlib.sha256(historical._declared_json(roster)).hexdigest(),
        "period_selection": selection, "selected_slot": dict(roster["slots"][0]),
        "prerequisite": {"root": str(prerequisite_context.root), "generation_id": prerequisite_context.generation_id,
            "generation_context_sha256": prerequisite_context.context_sha256,
            "native_universe_sha256": prerequisite_context.native_universe_sha256, **config_refs,
            "active_config_path": str(frozen_pipeline_config.active_config_path),
            "search_process_receipt_sha256": process.receipt_sha256},
        "activation_manifest_sha256": activation_manifest_sha256, "spread_profile_sha256": spread_profile_sha256,
        "source_access_window_policy": dict(policy), "window_observed_at_utc": _utc_text(observed),
        "source_access_window_utc": window, "cohort_roster_sha256": cohort_roster_sha256,
        "max_rows": max_rows, "max_bytes": max_bytes}


def _unit_writer_binding_sha256(binding):
    return hashlib.sha256(canonical_bytes(binding)).hexdigest()


def _bound_unit_precommit_inputs(context, binding, prerequisite_context, process, frozen_pipeline_config):
    if type(binding) is not dict:
        raise UnitWriterIssuerError("UNIT_WRITER_BINDING_INVALID")
    try:
        expected = _unit_writer_binding(unit_id=binding["unit_id"], unit_contract_sha256=binding["unit_contract_sha256"],
            consumer_cells=binding["consumer_cells"], slot_roster=binding["slot_roster"],
            prerequisite_context=prerequisite_context, process=process, frozen_pipeline_config=frozen_pipeline_config,
            activation_manifest_sha256=binding["activation_manifest_sha256"], spread_profile_sha256=binding["spread_profile_sha256"],
            source_access_window_policy=binding["source_access_window_policy"], cohort_roster_sha256=binding["cohort_roster_sha256"],
            max_rows=binding["max_rows"], max_bytes=binding["max_bytes"],
            now=historical._utc(binding["window_observed_at_utc"], "UNIT_WRITER_CLOCK_INVALID"))
    except KeyError as exc:
        raise UnitWriterIssuerError("UNIT_WRITER_BINDING_INVALID") from exc
    if canonical_bytes(binding) != canonical_bytes(expected):
        raise UnitWriterIssuerError("UNIT_WRITER_BINDING_CHANGED")
    cell_roots = {row["cell_id"]: Path(row["root"]) for row in expected["consumer_cells"]}
    prerequisite_root = prerequisite_context.root
    if any(prerequisite_root.is_relative_to(root) or root.is_relative_to(prerequisite_root)
            for root in (context.root, *cell_roots.values())) or frozen_pipeline_config.active_config_path.is_relative_to(context.root):
        raise UnitWriterIssuerError("UNIT_WRITER_PREREQUISITE_ROOT_OVERLAP")
    if (context.config_sha256 != prerequisite_context.config_sha256
            or context.native_universe_sha256 != prerequisite_context.native_universe_sha256
            or expected["max_bytes"] > context.limits.max_artifact_bytes):
        raise UnitWriterIssuerError("UNIT_WRITER_CONTEXT_BINDING_CHANGED")
    owner, owner_sha = spool.require_unit_owner(context, unit_id=expected["unit_id"],
        unit_contract_sha256=expected["unit_contract_sha256"], cell_roots=cell_roots)
    binding_sha = _unit_writer_binding_sha256(expected)
    if owner.get("unit_writer_binding_sha256") != binding_sha:
        raise UnitWriterIssuerError("UNIT_WRITER_OWNER_BINDING_CHANGED")
    frozen_pipeline_config.verify_current(prerequisite_context)
    planner.verify_frozen_production_search_process(prerequisite_context, process)
    planner._require_current_process(process)
    return {"schema_version": UNIT_WRITER_PRECOMMIT_SCHEMA, "authority_scope": "INVENTED_SOFTWARE_ONLY",
        "unit_id": expected["unit_id"], "unit_contract_sha256": expected["unit_contract_sha256"],
        "workspace_owner_sha256": owner_sha, "unit_generation_context_sha256": context.context_sha256,
        "unit_writer_binding_sha256": binding_sha, "binding": expected,
        "source_access_window_utc": expected["source_access_window_utc"]}


def _valid_issuance_time(body, stamp):
    return (body["binding"]["window_observed_at_utc"] <= stamp
        and body["source_access_window_utc"]["start_utc"] <= stamp < body["source_access_window_utc"]["end_exclusive_utc"])


def _issue_unit_precommit_under_lease(context, *, binding, prerequisite_context, process, frozen_pipeline_config, now=None):
    """Publish once. The caller already holds the unit dispatch lease."""
    if os.path.lexists(context.root / UNIT_WRITER_PRECOMMIT_PATH):
        raise UnitWriterIssuerError("UNIT_WRITER_PRECOMMIT_ALREADY_ISSUED")
    if any(os.path.lexists(context.root / relative) for relative in (spool.UNIT_LEDGER_RECEIPT_PATH,
            spool.UNIT_LEDGER_PENDING_PATH, spool.UNIT_LEDGER_PUBLICATION_INTENT_PATH)):
        raise UnitWriterIssuerError("UNIT_WRITER_ORIGINAL_PRECOMMIT_MISSING")
    body = _bound_unit_precommit_inputs(context, binding, prerequisite_context, process, frozen_pipeline_config)
    stamp = _utc_text(_timestamp(now))
    if not _valid_issuance_time(body, stamp):
        raise UnitWriterIssuerError("UNIT_WRITER_ORIGINAL_WINDOW_CLOSED")
    body["precommitted_at_utc"] = stamp
    control = spool._publish_exact_json(context, UNIT_WRITER_PRECOMMIT_PATH, body, context.limits.max_json_bytes)
    return body, control["sha256"]


def _reopen_unit_precommit_under_lease(context, *, binding, prerequisite_context, process, frozen_pipeline_config,
        expected_precommit_sha256):
    """Reopen the original receipt without refreshing its clock or publishing."""
    if not spool._is_sha256(expected_precommit_sha256):
        raise UnitWriterIssuerError("UNIT_WRITER_ORIGINAL_PRECOMMIT_IDENTITY_REQUIRED")
    if not os.path.lexists(context.root / UNIT_WRITER_PRECOMMIT_PATH):
        raise UnitWriterIssuerError("UNIT_WRITER_ORIGINAL_PRECOMMIT_MISSING")
    body, digest = spool._retirement_control(context.root, UNIT_WRITER_PRECOMMIT_PATH, context.limits.max_json_bytes)
    if digest != expected_precommit_sha256:
        raise UnitWriterIssuerError("UNIT_WRITER_ORIGINAL_PRECOMMIT_CHANGED")
    expected = _bound_unit_precommit_inputs(context, binding, prerequisite_context, process, frozen_pipeline_config)
    stamp = body.get("precommitted_at_utc")
    if (type(stamp) is not str or _utc_text(historical._utc(stamp, "UNIT_WRITER_CLOCK_INVALID")) != stamp
            or not _valid_issuance_time(expected, stamp)
            or canonical_bytes(body) != canonical_bytes({**expected, "precommitted_at_utc": stamp})):
        raise UnitWriterIssuerError("UNIT_WRITER_ORIGINAL_PRECOMMIT_CHANGED")
    return body, digest
