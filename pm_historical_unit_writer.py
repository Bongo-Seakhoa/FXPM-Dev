"""Authored unit publication under one original historical writer permit."""

from datetime import datetime, timezone

import pm_authority_spool as spool
import pm_historical_persisted_exec as persisted
from pm_historical_scheduler import _declared_dispatch_lease


class UnitWriterError(ValueError):
    """The original unit writer cannot proceed in the observed state."""


def _now_utc_seconds():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _require_open(window):
    if not window["start_utc"] <= _now_utc_seconds() < window["end_exclusive_utc"]:
        raise UnitWriterError("UNIT_WRITER_WINDOW_CLOSED")


def _control_present(context, relative):
    try:
        (context.root / relative).lstat()
    except FileNotFoundError:
        return False
    return True


def _publish_fresh(context, *, identity, expected, build_batches, window, symbol, timeframe, max_rows, max_bytes):
    _require_open(window)
    if not callable(build_batches):
        raise UnitWriterError("UNIT_WRITER_BATCH_BUILDER_REQUIRED")
    persisted._publish(context, persisted.INTENT, persisted._intent(identity))
    batches = build_batches()
    _require_open(window)
    if type(batches) is not dict or any((getattr(batch, "symbol", None), getattr(batch, "timeframe", None))
            != (symbol, timeframe) for batch in batches.values()):
        raise UnitWriterError("UNIT_WRITER_COHORT_AXES_CHANGED")
    chunks, receipt_for_descriptor = persisted.stream.prepare_write(identity=identity, expected=expected,
        batches=batches, max_rows=max_rows, max_bytes=max_bytes, max_line_bytes=context.limits.max_json_bytes)
    receipt = None

    def pending(descriptor):
        nonlocal receipt
        _require_open(window)
        receipt = receipt_for_descriptor(descriptor)
        persisted._publish(context, persisted.PENDING, receipt)
        _require_open(window)

    descriptor = spool.write_opaque_artifact(context, chunks, format=persisted.stream.FORMAT, _before_link=pending)
    if receipt is None or descriptor != receipt["descriptor"]:
        raise UnitWriterError("UNIT_WRITER_PENDING_DESCRIPTOR_CHANGED")
    _require_open(window)
    control = persisted._publish(context, persisted.RECEIPT, receipt)
    return receipt, control["sha256"]


def run_unit_writer(context, *, binding, prerequisite_context, process, frozen_pipeline_config,
        expected, build_batches=None, expected_precommit_sha256=None):
    """Issue once or reopen exactly, then expose the unit's one verified receipt."""
    with _declared_dispatch_lease(context) as acquired:
        if not acquired:
            raise UnitWriterError("UNIT_WRITER_BUSY")
        from pm_historical_unit_writer_issuer import (
            _issue_unit_precommit_under_lease,
            _reopen_unit_precommit_under_lease,
        )

        intent, pending, visible = (_control_present(context, path)
            for path in (persisted.INTENT, persisted.PENDING, persisted.RECEIPT))
        started = intent or pending or visible
        issuer_arguments = {"binding": binding, "prerequisite_context": prerequisite_context,
            "process": process, "frozen_pipeline_config": frozen_pipeline_config}
        if expected_precommit_sha256 is None:
            if started:
                raise UnitWriterError("UNIT_WRITER_ORIGINAL_PRECOMMIT_REQUIRED")
            permit, _ = _issue_unit_precommit_under_lease(context, **issuer_arguments)
        else:
            permit, _ = _reopen_unit_precommit_under_lease(context,
                expected_precommit_sha256=expected_precommit_sha256, **issuer_arguments)

        window = permit["source_access_window_utc"]
        if started:
            if not (intent and pending):
                raise UnitWriterError("UNIT_WRITER_INCOMPLETE")
            if _now_utc_seconds() < window["start_utc"]:
                raise UnitWriterError("UNIT_WRITER_WINDOW_CLOSED")
        else:
            _require_open(window)

        authorized = permit["binding"]
        owners = {cell["cell_id"]: cell["root"] for cell in authorized["consumer_cells"]}
        limits = {"max_rows": authorized["max_rows"], "max_bytes": authorized["max_bytes"]}
        identity = persisted._identity(context, unit_id=authorized["unit_id"],
            unit_contract_sha256=authorized["unit_contract_sha256"], cell_roots=owners,
            expected=expected, **limits)
        if identity["expected_roster_sha256"] != authorized["cohort_roster_sha256"]:
            raise UnitWriterError("UNIT_WRITER_COHORT_BINDING_CHANGED")
        if started:
            receipt, pending_sha, visible = persisted._read_exact_pending_cohort(
                context, identity=identity, expected=expected, **limits)
            if not visible:
                control = persisted._publish(context, persisted.RECEIPT, receipt)
                if control["sha256"] != pending_sha:
                    raise persisted.PersistedCohortError("COHORT_RECOVERED_RECEIPT_CHANGED")
            return receipt, pending_sha
        owner_axis = authorized["consumer_cells"][0]
        return _publish_fresh(context, identity=identity, expected=expected, build_batches=build_batches,
            window=window, symbol=owner_axis["symbol"], timeframe=owner_axis["timeframe"], **limits)
