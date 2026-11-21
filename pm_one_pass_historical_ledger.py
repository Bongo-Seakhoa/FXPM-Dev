"""Isolated historical unit-ledger transport for invented software evidence."""

from dataclasses import fields
import hashlib

import pandas as pd

import pm_authority_spool as spool
import pm_signal_opportunities as opportunities
import pm_signal_opportunity_tape as tape


LEDGER_RECEIPT = "ledger-receipt.json"
LEDGER_RECEIPT_SCHEMA = "fxpm.historical-unit-ledger-receipt.v2"
LEDGER_FORMAT = "fxpm-historical-unit-ledger-jsonl-v2"
LEDGER_SCHEMA = "fxpm.historical-unit-ledger.v2"
_AUTHORITY_SCOPE = "INVENTED_SOFTWARE_ONLY"
_BINDING_HASHES = frozenset({
    "roster_sha256", "fit_sha256", "source_extent_sha256", "source_release_sha256",
    "spread_sha256", "swap_sha256",
})
_TIERS = {"P75": 0, "P95": 1}
_TRADE_FIELDS = frozenset(field.name for field in fields(opportunities.IndependentSignalTrade))
_STATUSES = frozenset({"CLOSED", "RIGHT_CENSORED", "NOT_ELIGIBLE", "NO_NEXT_OBSERVATION"})


class UnitLedgerError(ValueError):
    """The invented ledger is incomplete or does not match its owner and receipt."""


def _limits(context, max_rows, max_bytes):
    if (type(max_rows) is not int or max_rows <= 0 or type(max_bytes) is not int
            or not 0 < max_bytes <= context.limits.max_artifact_bytes):
        raise UnitLedgerError("LEDGER_LIMITS_INVALID")


def _identity_fields(context, *, unit_id, unit_contract_sha256, cell_roots, stream_binding,
        _generic_write=False):
    _, owner_sha = spool.require_unit_owner(context, unit_id=unit_id,
        unit_contract_sha256=unit_contract_sha256, cell_roots=cell_roots, _generic_write=_generic_write)
    if (type(stream_binding) is not dict
            or set(stream_binding) != _BINDING_HASHES | {"symbol", "timeframe"}
            or type(stream_binding["symbol"]) is not str or not stream_binding["symbol"]
            or type(stream_binding["timeframe"]) is not str
            or stream_binding["timeframe"] not in tape._SECONDS):
        raise UnitLedgerError("LEDGER_BINDING_INVALID")
    for name in _BINDING_HASHES:
        spool._sha256(stream_binding[name], name)
    identity = {"schema_version": LEDGER_SCHEMA, "unit_id": unit_id,
        "unit_contract_sha256": unit_contract_sha256, "binding": dict(stream_binding)}
    return {"unit_id": unit_id, "unit_contract_sha256": unit_contract_sha256,
        "workspace_owner_sha256": owner_sha, "generation_context_sha256": context.context_sha256,
        "ledger_sha256": hashlib.sha256(tape._canonical(identity)).hexdigest(),
        "authority_scope": _AUTHORITY_SCOPE, "binding": dict(stream_binding)}


def _utc(value, code):
    if type(value) is not str:
        raise UnitLedgerError(code)
    stamp = opportunities._utc(value, code)
    if opportunities._text(stamp) != value:
        raise UnitLedgerError(code)
    return stamp


def _row_key(row):
    if (type(row) is not dict
            or set(row) != {"record_type", "design_ordinal", "design_sha256", "tier", "trade", "settled_at_utc"}
            or row["record_type"] != "ROW" or type(row["design_ordinal"]) is not int
            or row["design_ordinal"] < 0 or type(row["tier"]) is not str or row["tier"] not in _TIERS):
        raise UnitLedgerError("LEDGER_ROW_INVALID")
    spool._sha256(row["design_sha256"], "LEDGER_DESIGN_SHA256")
    trade = row["trade"]
    if (type(trade) is not dict or set(trade) != _TRADE_FIELDS
            or type(trade["signal_bar"]) is not int or trade["signal_bar"] < 0
            or type(trade["status"]) is not str or trade["status"] not in _STATUSES
            or (trade["signal_regime"] is not None and type(trade["signal_regime"]) is not str)):
        raise UnitLedgerError("LEDGER_TRADE_INVALID")
    return row["design_ordinal"], _TIERS[row["tier"]], trade["signal_bar"]


def _evaluation_axis(close_axis):
    code = "LEDGER_EVALUATION_AXIS_INVALID"
    if not isinstance(close_axis, pd.Series) or close_axis.name != "_BAR_CLOSE_TS":
        raise UnitLedgerError(code)
    try:
        opens = opportunities._axis(close_axis.index, code)
        closes = opportunities._axis(close_axis, code)
    except opportunities.OpportunityLabelError as exc:
        raise UnitLedgerError(code) from exc
    if (closes <= opens).any() or (closes[:-1] > opens[1:]).any():
        raise UnitLedgerError(code)
    return opens, closes


def _settled_trade(trade, axis):
    opens, closes = axis
    signal = trade["signal_bar"]
    if (signal >= len(closes) or trade["signal_open_utc"] != opportunities._text(opens[signal])
            or trade["signal_close_utc"] != opportunities._text(closes[signal])):
        raise UnitLedgerError("LEDGER_SIGNAL_CLOCK_INVALID")
    observed = trade["exit_observed_at_utc"]
    if trade["status"] != "CLOSED":
        if observed is not None:
            raise UnitLedgerError("LEDGER_EXIT_CLOCK_INVALID")
        return None
    entry, exit_bar = trade["entry_bar"], trade["exit_bar"]
    if (trade["eligible"] is not True or type(entry) is not int or type(exit_bar) is not int
            or not signal + 1 == entry <= exit_bar < len(closes)
            or trade["entry_open_utc"] != opportunities._text(opens[entry])
            or trade["exit_bar_open_utc"] != opportunities._text(opens[exit_bar])):
        raise UnitLedgerError("LEDGER_EXIT_CLOCK_INVALID")
    session = trade["exit_reason"] == "SESSION"
    if ((trade["exit_fill"] == "SESSION_OPEN") != session
            or observed != opportunities._text(opens[exit_bar] if session else closes[exit_bar])):
        raise UnitLedgerError("LEDGER_EXIT_CLOCK_INVALID")
    # V19 requires the exit bar to be visible even for an open-observed session exit.
    return closes[exit_bar]


def _record_group(groups, previous, row, key, row_index, offset, end):
    if previous is not None and key <= previous:
        raise UnitLedgerError("LEDGER_ROW_ORDER_INVALID")
    if (previous is not None and key[0] == previous[0]
            and row["design_sha256"] != groups[-1]["design_sha256"]):
        raise UnitLedgerError("LEDGER_DESIGN_ID_CHANGED")
    if previous is None or key[:2] != previous[:2]:
        groups.append({"design_ordinal": row["design_ordinal"], "design_sha256": row["design_sha256"],
            "tier": row["tier"], "first_row": row_index, "row_count": 0,
            "byte_offset": offset, "byte_count": 0})
    group = groups[-1]
    group["row_count"] += 1
    group["byte_count"] = end - group["byte_offset"]
    return key


def _header(identity):
    return {"record_type": "HEADER", "schema_version": LEDGER_SCHEMA, **identity}


def _footer(row_count, groups):
    return {"record_type": "COMPLETE", "row_count": row_count, "group_count": len(groups)}


def _receipt(identity, descriptor, row_count, groups):
    return {"schema_version": LEDGER_RECEIPT_SCHEMA, **identity, "descriptor": descriptor,
        "row_count": row_count, "groups": groups}


def publish_unit_ledger(context, *, unit_id, unit_contract_sha256, cell_roots,
        stream_binding, close_axis, rows, max_rows, max_bytes):
    """Write one owned opaque stream and publish its exact receipt after completion."""
    _limits(context, max_rows, max_bytes)
    identity = _identity_fields(context, unit_id=unit_id, unit_contract_sha256=unit_contract_sha256,
        cell_roots=cell_roots, stream_binding=stream_binding, _generic_write=True)
    if (context.root / LEDGER_RECEIPT).exists():
        raise UnitLedgerError("LEDGER_ALREADY_PUBLISHED")
    axis = _evaluation_axis(close_axis)
    groups, row_count, size = [], 0, 0
    line_bound = min(context.limits.max_json_bytes, tape.MAX_LINE_BYTES)

    def line(record):
        nonlocal size
        spool._bounded_json_value(record)
        encoded = tape._canonical(record) + b"\n"
        if len(encoded) > line_bound or size + len(encoded) > max_bytes:
            raise UnitLedgerError("LEDGER_BYTE_BOUND_EXCEEDED")
        size += len(encoded)
        return encoded

    def chunks():
        nonlocal row_count
        yield line(_header(identity))
        previous = None
        for raw in rows:
            if row_count >= max_rows:
                raise UnitLedgerError("LEDGER_ROW_BOUND_EXCEEDED")
            if type(raw) is not dict or "record_type" in raw or "settled_at_utc" in raw:
                raise UnitLedgerError("LEDGER_ROW_INVALID")
            row = {"record_type": "ROW", "settled_at_utc": None, **raw}
            key = _row_key(row)
            settled = _settled_trade(row["trade"], axis)
            if settled is not None:
                row["settled_at_utc"] = opportunities._text(settled)
            offset = size
            encoded = line(row)
            previous = _record_group(groups, previous, row, key, row_count, offset, size)
            row_count += 1
            yield encoded
        yield line(_footer(row_count, groups))

    descriptor = spool.write_opaque_artifact(context, chunks(), format=LEDGER_FORMAT)
    if descriptor["size_bytes"] != size:
        raise UnitLedgerError("LEDGER_DESCRIPTOR_SIZE_MISMATCH")
    receipt = _receipt(identity, descriptor, row_count, groups)
    control = spool._publish_exact_json(context, LEDGER_RECEIPT, receipt, context.limits.max_json_bytes)
    return receipt, control


def read_unit_projections(context, *, unit_id, unit_contract_sha256, cell_roots,
        stream_binding, close_axis, expected_receipt_sha256, projections, max_rows, max_bytes):
    """Return settled projections only after one complete, verified read of the stream."""
    _limits(context, max_rows, max_bytes)
    identity = _identity_fields(context, unit_id=unit_id, unit_contract_sha256=unit_contract_sha256,
        cell_roots=cell_roots, stream_binding=stream_binding)
    spool._sha256(expected_receipt_sha256, "LEDGER_RECEIPT_SHA256")
    receipt, receipt_sha = spool._retirement_control(context.root, LEDGER_RECEIPT,
        context.limits.max_json_bytes)
    if receipt_sha != expected_receipt_sha256:
        raise UnitLedgerError("LEDGER_RECEIPT_SHA_MISMATCH")
    descriptor = spool._validate_descriptor(receipt.get("descriptor"), content_addressed=True)
    row_count, offsets = receipt.get("row_count"), receipt.get("groups")
    if (descriptor["format"] != LEDGER_FORMAT or descriptor["size_bytes"] > max_bytes
            or type(row_count) is not int or not 0 <= row_count <= max_rows
            or type(offsets) is not list or len(offsets) > row_count
            or receipt != _receipt(identity, descriptor, row_count, offsets)):
        raise UnitLedgerError("LEDGER_RECEIPT_INVALID")

    by_regime, results = {}, {}
    for query in projections:
        if (type(query) is not tuple or len(query) != 2 or type(query[0]) is not str
                or query[0] not in opportunities._REGIMES or type(query[1]) is not str
                or query in results):
            raise UnitLedgerError("LEDGER_PROJECTION_INVALID")
        regime, cutoff = query
        by_regime.setdefault(regime, []).append((_utc(cutoff, "LEDGER_CUTOFF_INVALID"), query))
        results[query] = []
    if not results:
        raise UnitLedgerError("LEDGER_PROJECTION_INVALID")

    axis = _evaluation_axis(close_axis)
    groups, previous = [], None
    with tape._Stream(context, descriptor) as stream:
        header, _ = stream.next()
        if header != _header(identity):
            raise UnitLedgerError("LEDGER_HEADER_INVALID")
        for row_index in range(row_count):
            row, offset = stream.next()
            key = _row_key(row)
            previous = _record_group(groups, previous, row, key, row_index, offset, stream.total)
            trade = row["trade"]
            settled = _settled_trade(trade, axis)
            if row["settled_at_utc"] != (None if settled is None else opportunities._text(settled)):
                raise UnitLedgerError("LEDGER_SETTLED_CLOCK_INVALID")
            if settled is not None:
                for cutoff, query in by_regime.get(trade["signal_regime"], ()):
                    if settled <= cutoff:
                        results[query].append(row)
        footer, _ = stream.next()
        if (footer != _footer(row_count, groups) or type(footer["row_count"]) is not int
                or type(footer["group_count"]) is not int):
            raise UnitLedgerError("LEDGER_FOOTER_INVALID")
        trailing, _ = stream.next()
        if trailing is not None:
            raise UnitLedgerError("LEDGER_TRAILING_RECORD")
        stream.finish()
    if tape._canonical(groups) != tape._canonical(offsets):
        raise UnitLedgerError("LEDGER_GROUP_OFFSETS_MISMATCH")
    return {query: tuple(rows) for query, rows in results.items()}
