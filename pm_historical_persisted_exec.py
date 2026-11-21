"""Invented software proof of a persisted historical Exec unit projection."""

from dataclasses import asdict
import hashlib

import pandas as pd

import pm_authority_spool as spool
import pm_historical_exec_cohort_stream as stream
import pm_signal_opportunities as opportunities
from pm_core import Backtester
from pm_end_grid import canonical_bytes
from pm_historical_exec_unit_projection import (
    HistoricalUnitBatchProjection,
    _compatibility_key,
    _project_batch,
    _uses_base_stops_without_session,
)


RECEIPT = "ledger-receipt.json"
INTENT = "ledger-publication-intent.json"
PENDING = "ledger-pending.json"


class PersistedCohortError(ValueError):
    """The owned cohort publication or its expected view is incomplete."""


def _sha(body):
    return hashlib.sha256(canonical_bytes(body)).hexdigest()


def _compatibility_sha(view):
    key = _compatibility_key(view["partition"], view["tier"], view["parameters"],
        view["source_reference"], view["cost_probe"], view["evaluation_kwargs"])
    encoded = []
    for value in key:
        if type(value) is bytes:
            value = value.decode("ascii")
        elif isinstance(value, pd.Timestamp):
            value = value.isoformat()
        elif isinstance(value, opportunities.OpportunityProvenance):
            value = asdict(value)
        elif isinstance(value, type):
            value = [value.__module__, value.__qualname__]
        encoded.append(value)
    return _sha(encoded)


class HotCohortCapture(HistoricalUnitBatchProjection):
    """Capture actual inputs alongside the proved V19 union batches."""

    def __init__(self):
        super().__init__()
        self.expected = {}

    @property
    def batches(self):
        return {slot: batch for slot, (_key, batch) in self._batches.items()}

    def project(self, **view):
        batch = super().project(**view)
        slot = view["view_slot"]
        if batch is not None and slot not in self.expected:
            self.expected[slot] = {"geometry": dict(view["partition"].geometry_receipt),
                "close_axis": view["features"]["_BAR_CLOSE_TS"], "compatibility_sha256": _compatibility_sha(view)}
        return batch


class PersistedCohortProjection:
    """Serve exact verified cohorts to Exec's optional partition projection hook."""

    def __init__(self, expected, batches):
        if expected.keys() != batches.keys():
            raise PersistedCohortError("COHORT_SET_CHANGED")
        self._expected = dict(expected)
        self._batches = dict(batches)
        self.projected_partitions = 0
        self.direct_partitions = 0

    def project(self, **view):
        inputs = view["evaluation_kwargs"]
        slot = view["view_slot"]
        declaration = self._expected.get(slot)
        try:
            compatible = (declaration is not None
                and view["evaluate"] is opportunities.evaluate_independent_signal_r
                and type(inputs["pricing"]) is Backtester
                and _uses_base_stops_without_session(inputs["strategy"], inputs["spec"])
                and declaration["compatibility_sha256"] == _compatibility_sha(view))
        except (TypeError, ValueError):
            compatible = False
        result = None
        if compatible:
            result = _project_batch(self._batches[slot], view["features"], view["masked_signals"],
                view["regime"], cutoff=inputs["cutoff_utc"], start=inputs["signal_start_utc"])
        if result is None:
            self.direct_partitions += 1
        else:
            self.projected_partitions += 1
        return result


def _identity(context, *, unit_id, unit_contract_sha256, cell_roots, expected, max_rows, max_bytes,
        _generic_write=False):
    _, owner_sha = spool.require_unit_owner(context, unit_id=unit_id,
        unit_contract_sha256=unit_contract_sha256, cell_roots=cell_roots, _generic_write=_generic_write)
    if (type(max_rows) is not int or max_rows <= 0 or type(max_bytes) is not int
            or not 0 < max_bytes <= context.limits.max_artifact_bytes):
        raise PersistedCohortError("COHORT_LIMITS_INVALID")
    roster = [[slot, entry["geometry"]["receipt_sha256"], entry["compatibility_sha256"]]
        for slot, entry in sorted(expected.items())]
    binding = {"unit_id": unit_id, "unit_contract_sha256": unit_contract_sha256,
        "workspace_owner_sha256": owner_sha, "generation_context_sha256": context.context_sha256,
        "authority_scope": "INVENTED_SOFTWARE_ONLY", "max_rows": max_rows, "max_bytes": max_bytes,
        "expected_roster_sha256": _sha(roster)}
    return {**binding, "ledger_sha256": _sha({"schema_version": stream.RECEIPT_SCHEMA, **binding})}


def _intent(identity):
    return {"schema_version": "fxpm.historical-exec-cohort-publication-intent.v1", "identity": identity}


def _control(context, path):
    return spool._retirement_control(context.root, path, context.limits.max_json_bytes)


def _publish(context, path, body):
    return spool._publish_exact_json(context, path, body, context.limits.max_json_bytes)


def publish_cohorts(context, *, unit_id, unit_contract_sha256, cell_roots, expected, batches, max_rows, max_bytes):
    """Publish one bounded stream and its exact pending-to-final receipt."""
    identity = _identity(context, unit_id=unit_id, unit_contract_sha256=unit_contract_sha256,
        cell_roots=cell_roots, expected=expected, max_rows=max_rows, max_bytes=max_bytes,
        _generic_write=True)
    if any((context.root / name).exists() for name in (INTENT, PENDING, RECEIPT)):
        raise PersistedCohortError("COHORT_PUBLICATION_ALREADY_STARTED")
    chunks, receipt_for_descriptor = stream.prepare_write(identity=identity, expected=expected,
        batches=batches, max_rows=max_rows, max_bytes=max_bytes, max_line_bytes=context.limits.max_json_bytes)
    _publish(context, INTENT, _intent(identity))
    receipt = None

    def pending(descriptor):
        nonlocal receipt
        receipt = receipt_for_descriptor(descriptor)
        _publish(context, PENDING, receipt)

    descriptor = spool.write_opaque_artifact(context, chunks, format=stream.FORMAT, _before_link=pending)
    if receipt is None or receipt["descriptor"] != descriptor:
        raise PersistedCohortError("COHORT_PENDING_DESCRIPTOR_CHANGED")
    return receipt, _publish(context, RECEIPT, receipt)


def read_cohorts(context, *, unit_id, unit_contract_sha256, cell_roots, expected, expected_receipt_sha256,
        max_rows, max_bytes):
    """Expose typed cohorts after the visible receipt and entire stream verify."""
    identity = _identity(context, unit_id=unit_id, unit_contract_sha256=unit_contract_sha256,
        cell_roots=cell_roots, expected=expected, max_rows=max_rows, max_bytes=max_bytes)
    receipt, digest = _control(context, RECEIPT)
    if digest != expected_receipt_sha256:
        raise PersistedCohortError("COHORT_RECEIPT_SHA_MISMATCH")
    return stream.verify_read(context, identity=identity, expected=expected, receipt=receipt,
        max_rows=max_rows, max_bytes=max_bytes)


def recover_cohort_publication(context, *, unit_id, unit_contract_sha256, cell_roots, expected, max_rows, max_bytes):
    """Verify the uniquely bound complete pending object, then expose its same receipt."""
    identity = _identity(context, unit_id=unit_id, unit_contract_sha256=unit_contract_sha256,
        cell_roots=cell_roots, expected=expected, max_rows=max_rows, max_bytes=max_bytes,
        _generic_write=True)
    pending, pending_sha, visible = _read_exact_pending_cohort(context, identity=identity,
        expected=expected, max_rows=max_rows, max_bytes=max_bytes)
    if visible:
        return pending, pending_sha
    control = _publish(context, RECEIPT, pending)
    if control["sha256"] != pending_sha:
        raise PersistedCohortError("COHORT_RECOVERED_RECEIPT_CHANGED")
    return pending, pending_sha


def _read_exact_pending_cohort(context, *, identity, expected, max_rows, max_bytes):
    """Verify the original intent, pending receipt, and linked object without writing."""
    if not all((context.root / name).is_file() for name in (INTENT, PENDING)):
        raise PersistedCohortError("COHORT_EXACT_PENDING_REQUIRED")
    intent, _ = _control(context, INTENT)
    if intent != _intent(identity):
        raise PersistedCohortError("COHORT_INTENT_BINDING_CHANGED")
    pending, pending_sha = _control(context, PENDING)
    visible = (context.root / RECEIPT).exists()
    if visible:
        receipt, digest = _control(context, RECEIPT)
        if receipt != pending or digest != pending_sha:
            raise PersistedCohortError("COHORT_VISIBLE_RECEIPT_CONFLICT")
    stream.verify_read(context, identity=identity, expected=expected, receipt=pending,
        max_rows=max_rows, max_bytes=max_bytes)
    return pending, pending_sha, visible
