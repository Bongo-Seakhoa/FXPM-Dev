"""INVENTED tiny publication metadata; no model, source or financial authority."""
from datetime import datetime, timedelta, timezone
import hashlib

import pm_authoritative_search_execution as execution
import pm_authority_spool as spool


def test_real_declared_unit_resume_has_no_aggregate_byte_admission(tmp_path, monkeypatch):
    # A small retired allowance exercises the same branch without allocating GiB.
    monkeypatch.setattr(execution, "MAX_CELL_EVIDENCE_BYTES", 1, raising=False)
    context = spool.freeze_generation_context(tmp_path / "spool",
        generation_id="INVENTED-NO-AGGREGATE-EVIDENCE-CAP",
        native_universe_sha256="a" * 64, config_sha256="b" * 64)
    now = datetime.now(timezone.utc).replace(microsecond=0)
    body = {"precommit_sha256": "c" * 64, "cell_id": "INVENTED-cell",
        "slot_id": "INVENTED-slot", "attempt_id": "INVENTED-owner-attempt",
        "source_access_window_utc": {
            "start_utc": (now-timedelta(minutes=1)).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "end_exclusive_utc": (now+timedelta(hours=1)).strftime("%Y-%m-%dT%H:%M:%SZ")}}
    units = [{"ordinal": ordinal, "raw_attempt_id": "INVENTED-" + str(ordinal),
        "plan_attempt_sha256": "d" * 64, "status": "PLANNED"} for ordinal in range(2)]
    values = [{"attempt_id": unit["raw_attempt_id"],
        "fixture_provenance": "INVENTED_SMALL_PUBLICATION_NOT_ECONOMIC_EVIDENCE"} for unit in units]
    binding_sha = "e" * 64
    first = execution._publish_declared_completed_unit(context, body, binding_sha, units[0], values[0])
    first_bytes = (context.root / first["payload"]["relative_path"]).read_bytes()
    assert first["payload"]["size_bytes"] == len(first_bytes) > 1
    produced = []

    def produce(unit):
        produced.append(unit["ordinal"])
        return values[unit["ordinal"]]

    rows, ended = execution._continue_declared_attempt_units(context, body, binding_sha, units, produce)
    assert produced == [1]
    assert len(rows) == 2 and rows[0] == first
    assert sum(row["payload"]["size_bytes"] for row in rows) > 1
    for unit, value, row in zip(units, values, rows, strict=True):
        encoded = (context.root / row["payload"]["relative_path"]).read_bytes()
        assert len(encoded) == row["payload"]["size_bytes"]
        assert hashlib.sha256(encoded).hexdigest() == row["payload"]["sha256"]
        reopened_record, reopened = execution._read_declared_completed_unit(context, body, binding_sha, unit)
        assert reopened_record == row and reopened == value
    again, ended_again = execution._continue_declared_attempt_units(
        context, body, binding_sha, units, produce)
    assert again == rows and ended_again == ended and produced == [1]
    assert (context.root / first["payload"]["relative_path"]).read_bytes() == first_bytes
