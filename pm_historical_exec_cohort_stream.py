"""Isolated transport for exact, locally evaluated V19 cohorts."""

from collections.abc import Mapping
from copy import deepcopy
from dataclasses import asdict, dataclass, fields
import hashlib
import math

import numpy as np

import pm_authority_spool as spool
import pm_one_pass_historical_ledger as settlement
import pm_signal_opportunities as opportunities
import pm_signal_opportunity_tape as tape


FORMAT = "fxpm-historical-exec-cohort-jsonl-v2"
RECEIPT_SCHEMA = "fxpm.historical-exec-cohort-receipt.v2"
SCHEMA = "fxpm.historical-exec-cohort.v2"
_GEOMETRY_SCHEMA = "fxpm.winner-partition-geometry.v1"
_GEOMETRY_FIELDS = frozenset({"schema_version", "role", "frame_start_utc", "scored_start_utc",
    "end_exclusive_utc", "warmup_bars", "scored_bars", "frame_rows", "frame_sha256", "receipt_sha256"})
_ROLE_ORDER = {"optimizer_train": 0, "tuning_validation": 1, "final_lockbox": 2}
_TIER_ORDER = {"P75": 0, "P95": 1}
_BATCH = opportunities.IndependentOpportunityBatch
_TRADE = opportunities.IndependentSignalTrade
_PROVENANCE = opportunities.OpportunityProvenance
_BATCH_FIELDS = tuple(field for field in fields(_BATCH) if field.name != "records")
_BATCH_NAMES = frozenset(field.name for field in _BATCH_FIELDS)
_BATCH_INIT = tuple(field.name for field in _BATCH_FIELDS if field.init)
_BATCH_CONSTANTS = {field.name: field.default for field in _BATCH_FIELDS if not field.init}
_PROVENANCE_FIELDS = frozenset(field.name for field in fields(_PROVENANCE))
_TRADE_FIELDS = frozenset(field.name for field in fields(_TRADE))
_COUNTS = ("raw_nonzero_count", "excluded_before_start_count", "window_nonzero_count",
    "unavailable_regime_count")


class CohortStreamError(ValueError):
    """The cohort stream or its expected local evaluation is inconsistent."""


@dataclass(frozen=True)
class _ExpectedCohort:
    slot: tuple
    geometry: dict
    axis: tuple
    local_axis_sha256: str
    compatibility_sha256: str


def _limits(max_rows, max_bytes):
    if type(max_rows) is not int or max_rows <= 0 or type(max_bytes) is not int or max_bytes <= 0:
        raise CohortStreamError("COHORT_LIMITS_INVALID")


def _slot_key(slot):
    if (type(slot) is not tuple or len(slot) != 4
            or type(slot[0]) is not int or slot[0] < 0 or type(slot[1]) is not int or slot[1] < 0
            or type(slot[2]) is not str or slot[2] not in _ROLE_ORDER
            or type(slot[3]) is not str or slot[3] not in _TIER_ORDER):
        raise CohortStreamError("COHORT_SLOT_INVALID")
    return slot[0], slot[1], _ROLE_ORDER[slot[2]], _TIER_ORDER[slot[3]]


def _geometry(value):
    if not isinstance(value, Mapping) or set(value) != _GEOMETRY_FIELDS:
        raise CohortStreamError("COHORT_GEOMETRY_INVALID")
    payload = dict(value)
    digest = payload.pop("receipt_sha256")
    spool._bounded_json_value(payload)
    if (payload["schema_version"] != _GEOMETRY_SCHEMA or not spool._is_sha256(payload["frame_sha256"])
            or not spool._is_sha256(digest) or tape._sha(payload) != digest):
        raise CohortStreamError("COHORT_GEOMETRY_INVALID")
    return {**payload, "receipt_sha256": digest}


def _local_axis(close_axis):
    try:
        axis = settlement._evaluation_axis(close_axis)
        digest = hashlib.sha256(b"fxpm.historical-exec-cohort-local-axis.v2\0")
        for index in axis:
            ns = index.as_unit("ns").asi8.astype(">i8", copy=False)
            digest.update(memoryview(ns).cast("B"))
    except (ValueError, OverflowError) as exc:
        raise CohortStreamError("COHORT_EVALUATION_AXIS_INVALID") from exc
    return axis, digest.hexdigest()


def _expectations(expected):
    if not isinstance(expected, Mapping) or not expected:
        raise CohortStreamError("COHORT_EXPECTATIONS_INVALID")
    axis_cache = {}
    cohorts = []
    for slot in sorted(expected, key=_slot_key):
        value = expected[slot]
        if (not isinstance(value, Mapping) or set(value) != {"geometry", "close_axis", "compatibility_sha256"}
                or not spool._is_sha256(value["compatibility_sha256"])):
            raise CohortStreamError("COHORT_EXPECTATION_INVALID")
        geometry = _geometry(value["geometry"])
        close_axis = value["close_axis"]
        axis_key = id(close_axis)
        if axis_key not in axis_cache:
            axis_cache[axis_key] = _local_axis(close_axis)
        axis, digest = axis_cache[axis_key]
        opens, closes = axis
        warmup = geometry["warmup_bars"]
        try:
            frame_start = settlement._utc(geometry["frame_start_utc"], "COHORT_GEOMETRY_INVALID")
        except ValueError as exc:
            raise CohortStreamError("COHORT_GEOMETRY_INVALID") from exc
        if (geometry["role"] != slot[2] or type(warmup) is not int or not 0 <= warmup < len(opens)
                or type(geometry["frame_rows"]) is not int or geometry["frame_rows"] != len(opens)
                or type(geometry["scored_bars"]) is not int or geometry["scored_bars"] != len(opens) - warmup
                or opens[0] < frame_start
                or geometry["scored_start_utc"] != opportunities._text(opens[warmup])
                or geometry["end_exclusive_utc"] != opportunities._text(closes[-1])):
            raise CohortStreamError("COHORT_GEOMETRY_AXIS_MISMATCH")
        cohorts.append(_ExpectedCohort(slot, geometry, axis, digest, value["compatibility_sha256"]))
    return cohorts


def _header(identity, cohort_count):
    if type(identity) is not dict or not identity:
        raise CohortStreamError("COHORT_IDENTITY_INVALID")
    return {"record_type": "HEADER", "schema_version": SCHEMA,
        "identity": identity, "cohort_count": cohort_count}


def _batch_meta(batch):
    if type(batch) is not _BATCH or type(batch.records) is not tuple or type(batch.provenance) is not _PROVENANCE:
        raise CohortStreamError("COHORT_BATCH_INVALID")
    metadata = {name: getattr(batch, name) for name in _BATCH_NAMES}
    metadata["provenance"] = asdict(batch.provenance)
    return metadata


def _batch_inputs(metadata, expected):
    if (type(metadata) is not dict or set(metadata) != _BATCH_NAMES
            or any(type(metadata[key]) is not type(value) or metadata[key] != value
                for key, value in _BATCH_CONSTANTS.items())
            or type(metadata["provenance"]) is not dict or set(metadata["provenance"]) != _PROVENANCE_FIELDS
            or any(type(metadata[field]) is not int or metadata[field] < 0 for field in _COUNTS)
            or metadata["raw_nonzero_count"] != metadata["excluded_before_start_count"] + metadata["window_nonzero_count"]
            or metadata["unavailable_regime_count"] > metadata["window_nonzero_count"]
            or type(metadata["cost_model"]) is not dict or type(metadata["episode_diagnostics"]) is not dict
            or type(metadata["symbol"]) is not str or not metadata["symbol"]
            or type(metadata["timeframe"]) is not str or metadata["timeframe"] not in tape._SECONDS
            or type(metadata["pip_size"]) is not float or not math.isfinite(metadata["pip_size"])
            or metadata["pip_size"] <= 0):
        raise CohortStreamError("COHORT_BATCH_METADATA_INVALID")
    provenance = _PROVENANCE(**metadata["provenance"])
    try:
        opportunities._provenance(provenance)
    except opportunities.OpportunityLabelError as exc:
        raise CohortStreamError("COHORT_BATCH_PROVENANCE_INVALID") from exc
    closes = expected.axis[1]
    warmup = expected.geometry["warmup_bars"]
    if (provenance.spread_tier != expected.slot[3]
            or metadata["cutoff_utc"] != expected.geometry["end_exclusive_utc"]
            or metadata["signal_start_utc"] != opportunities._text(closes[max(1, warmup) - 1])):
        raise CohortStreamError("COHORT_BATCH_GEOMETRY_MISMATCH")
    return {**{name: metadata[name] for name in _BATCH_INIT}, "provenance": provenance}


def _cohort(expected, metadata, row_count):
    return {"record_type": "COHORT", "slot": list(expected.slot), "geometry": expected.geometry,
        "local_axis_sha256": expected.local_axis_sha256, "compatibility_sha256": expected.compatibility_sha256,
        "metadata": metadata, "row_count": row_count}


def _cohort_index(expected, first_row, row_count, offset, end):
    return {"slot": list(expected.slot), "first_row": first_row, "row_count": row_count,
        "byte_offset": offset, "byte_count": end - offset, "local_axis_sha256": expected.local_axis_sha256,
        "compatibility_sha256": expected.compatibility_sha256}


def _row_clock(trade, expected, previous_signal):
    if (type(trade) is not dict or set(trade) != _TRADE_FIELDS
            or type(trade["signal_bar"]) is not int or trade["signal_bar"] <= previous_signal
            or type(trade["direction"]) is not int or trade["direction"] not in (-1, 1)
            or type(trade["eligible"]) is not bool
            or type(trade["status"]) is not str or trade["status"] not in settlement._STATUSES):
        raise CohortStreamError("COHORT_TRADE_INVALID")
    try:
        settled = settlement._settled_trade(trade, expected.axis)
    except settlement.UnitLedgerError as exc:
        raise CohortStreamError("COHORT_TRADE_CLOCK_INVALID") from exc
    return None if settled is None else opportunities._text(settled)


def _footer(cohort_count, row_count):
    return {"record_type": "COMPLETE", "cohort_count": cohort_count, "row_count": row_count}


def _receipt(identity, descriptor, row_count, cohorts):
    return {"schema_version": RECEIPT_SCHEMA, "identity": identity, "descriptor": descriptor,
        "cohort_count": len(cohorts), "row_count": row_count, "cohorts": cohorts}


def _descriptor(value, max_bytes):
    descriptor = spool._validate_descriptor(value, content_addressed=True)
    if descriptor["format"] != FORMAT or descriptor["size_bytes"] > max_bytes:
        raise CohortStreamError("COHORT_DESCRIPTOR_INVALID")
    return descriptor


def prepare_write(*, identity, expected, batches, max_rows, max_bytes, max_line_bytes=tape.MAX_LINE_BYTES):
    """Return one stream and its receipt builder for the completed opaque object."""
    _limits(max_rows, max_bytes)
    if type(max_line_bytes) is not int or max_line_bytes <= 0:
        raise CohortStreamError("COHORT_LIMITS_INVALID")
    line_bound = min(max_line_bytes, tape.MAX_LINE_BYTES)
    ordered = _expectations(expected)
    if not isinstance(batches, Mapping) or set(batches) != set(expected):
        raise CohortStreamError("COHORT_BATCH_SLOTS_CHANGED")
    header = _header(identity, len(ordered))
    identity = deepcopy(identity)
    header["identity"] = identity
    size, row_total, complete = 0, 0, False
    index = []

    def line(value):
        nonlocal size
        spool._bounded_json_value(value)
        result = tape._canonical(value) + b"\n"
        if len(result) > line_bound or size + len(result) > max_bytes:
            raise CohortStreamError("COHORT_BYTE_BOUND_EXCEEDED")
        size += len(result)
        return result

    def chunks():
        nonlocal row_total, complete
        yield line(header)
        for cohort in ordered:
            batch = batches[cohort.slot]
            metadata = _batch_meta(batch)
            _batch_inputs(metadata, cohort)
            count = len(batch.records)
            if count != metadata["window_nonzero_count"]:
                raise CohortStreamError("COHORT_ROW_COUNT_MISMATCH")
            if row_total + count > max_rows:
                raise CohortStreamError("COHORT_ROW_BOUND_EXCEEDED")
            start = size
            yield line(_cohort(cohort, metadata, count))
            previous = -1
            for record in batch.records:
                if type(record) is not _TRADE:
                    raise CohortStreamError("COHORT_TRADE_INVALID")
                trade = asdict(record)
                # Keep the plain JSON forms already used by Exec for these NumPy scalars.
                for name, value in trade.items():
                    if type(value) is np.str_:
                        trade[name] = str(value)
                    elif type(value) is np.float64:
                        trade[name] = float(value)
                settled = _row_clock(trade, cohort, previous)
                yield line({"record_type": "ROW", "settled_at_utc": settled, "trade": trade})
                previous = trade["signal_bar"]
            index.append(_cohort_index(cohort, row_total, count, start, size))
            row_total += count
        yield line(_footer(len(ordered), row_total))
        complete = True

    def receipt_for_descriptor(value):
        if not complete:
            raise CohortStreamError("COHORT_STREAM_INCOMPLETE")
        descriptor = _descriptor(value, max_bytes)
        if descriptor["size_bytes"] != size:
            raise CohortStreamError("COHORT_DESCRIPTOR_SIZE_MISMATCH")
        return _receipt(identity, descriptor, row_total, index)

    return chunks(), receipt_for_descriptor


def verify_read(context, *, identity, expected, receipt, max_rows, max_bytes):
    """Verify one complete opaque read before exposing reconstructed V19 batches."""
    _limits(max_rows, max_bytes)
    ordered = _expectations(expected)
    header = _header(identity, len(ordered))
    if type(receipt) is not dict:
        raise CohortStreamError("COHORT_RECEIPT_INVALID")
    descriptor = _descriptor(receipt.get("descriptor"), min(max_bytes, context.limits.max_artifact_bytes))
    claimed_rows, claimed_index = receipt.get("row_count"), receipt.get("cohorts")
    control_identity = receipt.get("identity")
    if (type(control_identity) is not dict or tape._canonical(control_identity) != tape._canonical(identity)
            or type(claimed_rows) is not int or not 0 <= claimed_rows <= max_rows
            or type(claimed_index) is not list or len(claimed_index) != len(ordered)
            or type(receipt.get("cohort_count")) is not int
            or receipt != _receipt(identity, descriptor, claimed_rows, claimed_index)):
        raise CohortStreamError("COHORT_RECEIPT_INVALID")

    row_total, index, batches = 0, [], {}
    with tape._Stream(context, descriptor) as stream:
        record, _ = stream.next()
        if record != header or tape._canonical(record) != tape._canonical(header):
            raise CohortStreamError("COHORT_HEADER_MISMATCH")
        for cohort in ordered:
            record, offset = stream.next()
            if type(record) is not dict:
                raise CohortStreamError("COHORT_BINDING_MISMATCH")
            metadata, count = record.get("metadata"), record.get("row_count")
            if (type(count) is not int or not 0 <= count <= claimed_rows - row_total
                    or record != _cohort(cohort, metadata, count)
                    or any(type(ordinal) is not int for ordinal in record["slot"][:2])
                    or any(type(record["geometry"][name]) is not int
                        for name in ("warmup_bars", "scored_bars", "frame_rows"))):
                raise CohortStreamError("COHORT_BINDING_MISMATCH")
            inputs = _batch_inputs(metadata, cohort)
            if count != metadata["window_nonzero_count"]:
                raise CohortStreamError("COHORT_ROW_COUNT_MISMATCH")
            rows, previous = [], -1
            for _ in range(count):
                row, _ = stream.next()
                if (type(row) is not dict or set(row) != {"record_type", "settled_at_utc", "trade"}
                        or row["record_type"] != "ROW"):
                    raise CohortStreamError("COHORT_ROW_INVALID")
                trade = row["trade"]
                settled = _row_clock(trade, cohort, previous)
                if row["settled_at_utc"] != settled:
                    raise CohortStreamError("COHORT_SETTLED_CLOCK_MISMATCH")
                rows.append(_TRADE(**trade))
                previous = trade["signal_bar"]
            index.append(_cohort_index(cohort, row_total, count, offset, stream.total))
            row_total += count
            batches[cohort.slot] = _BATCH(**inputs, records=tuple(rows))
        footer, _ = stream.next()
        if (row_total != claimed_rows or footer != _footer(len(ordered), row_total)
                or type(footer["cohort_count"]) is not int or type(footer["row_count"]) is not int):
            raise CohortStreamError("COHORT_FOOTER_MISMATCH")
        trailing, _ = stream.next()
        if trailing is not None:
            raise CohortStreamError("COHORT_TRAILING_RECORD")
        stream.finish()
    if tape._canonical(index) != tape._canonical(claimed_index):
        raise CohortStreamError("COHORT_INDEX_MISMATCH")
    return batches
