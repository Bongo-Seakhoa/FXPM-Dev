"""Deterministic two-reference consensus for broker-native history evidence.

This module deliberately contains no file or MT5 access.  It normalizes the
result of two independently evaluated target/reference edges, recomputes every
window and top-level consensus state, and authenticates the resulting envelope.
Production callers must still recompute each pair report from the pinned bar
files; a hash proves byte identity, not mathematical correctness.
"""

from __future__ import annotations

import hashlib
import json
from typing import Any, Dict, Mapping, Optional, Sequence, Tuple


CONSENSUS_SCHEMA_VERSION = "fxpm-native-cross-resolution-consensus-v1"
EDGE_SCHEMA_VERSION = "fxpm-native-cross-resolution-consensus-edge-v1"
CONSENSUS_POLICY_SCHEMA_VERSION = "fxpm-native-consensus-policy-v1"
PAIR_REPORT_SCHEMA_VERSION = "fxpm-native-cross-resolution-parity-v2"

PRODUCTION_TIMEFRAMES: Tuple[str, ...] = ("M5", "M15", "M30", "H1", "H4", "D1")
TIMEFRAME_SECONDS: Dict[str, int] = {
    "M5": 300,
    "M15": 900,
    "M30": 1800,
    "H1": 3600,
    "H4": 14_400,
    "D1": 86_400,
}

# One finer and one coarser witness for every interior target.  The endpoints
# use two witnesses on the only available side.  Values are serialization
# authority only; their order is never a fallback priority.
REFERENCE_GRAPH: Dict[str, Tuple[str, str]] = {
    "M5": ("M15", "M30"),
    "M15": ("M5", "M30"),
    "M30": ("M15", "H1"),
    "H1": ("M30", "H4"),
    "H4": ("H1", "D1"),
    "D1": ("H4", "H1"),
}

REQUIRED_AUDIT_WINDOWS: Tuple[str, ...] = (
    "strategy_full_context",
    "optimizer_train",
    "tuning_validation",
    "final_lockbox",
)

INTEGRITY_PASS = "PASS"
INTEGRITY_ERROR = "ERROR"
EVIDENCE_PASS = "PASS"
EVIDENCE_SEMANTIC_FAIL = "SEMANTIC_FAIL"
EVIDENCE_TERMINAL_UNAVAILABLE = "TERMINAL_UNAVAILABLE"
EVIDENCE_PENDING = "PENDING"
TECHNICAL_TERMINAL = "TECHNICAL_TERMINAL"

WINDOW_PASS = "PASS"
WINDOW_FAIL = "FAIL"
WINDOW_PENDING = "PENDING"
WINDOW_ERROR = "ERROR"

VALID_INTEGRITY_STATES = frozenset({INTEGRITY_PASS, INTEGRITY_ERROR})
VALID_EVIDENCE_STATES = frozenset(
    {
        EVIDENCE_PASS,
        EVIDENCE_SEMANTIC_FAIL,
        EVIDENCE_TERMINAL_UNAVAILABLE,
        EVIDENCE_PENDING,
    }
)
VALID_WINDOW_STATES = frozenset(
    {WINDOW_PASS, WINDOW_FAIL, WINDOW_PENDING, WINDOW_ERROR}
)

CONSENSUS_POLICY: Dict[str, Any] = {
    "schema_version": CONSENSUS_POLICY_SCHEMA_VERSION,
    "required_reference_count": 2,
    "minimum_passing_references_per_window": 1,
    "evaluate_all_references": True,
    "integrity_error_dominates": True,
    "window_aggregation_precedence": [
        WINDOW_ERROR,
        WINDOW_FAIL,
        WINDOW_PENDING,
        WINDOW_PASS,
    ],
    "fault_model": "one_non_common_native_resolution_fault",
}


def canonical_json_bytes(value: Any) -> bytes:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")


def canonical_sha256(value: Any) -> str:
    return hashlib.sha256(canonical_json_bytes(value)).hexdigest()


CONSENSUS_POLICY_SHA256 = canonical_sha256(CONSENSUS_POLICY)
REFERENCE_GRAPH_SHA256 = canonical_sha256(
    {timeframe: list(REFERENCE_GRAPH[timeframe]) for timeframe in PRODUCTION_TIMEFRAMES}
)


def _json_copy(value: Any) -> Any:
    return json.loads(canonical_json_bytes(value).decode("utf-8"))


def _is_sha256(value: Any) -> bool:
    text = str(value or "").strip().lower()
    return len(text) == 64 and all(
        character in "0123456789abcdef" for character in text
    )


def expected_reference_timeframes(target_timeframe: str) -> Tuple[str, str]:
    target = str(target_timeframe or "").strip().upper()
    if target not in REFERENCE_GRAPH:
        raise ValueError(f"unsupported production target timeframe: {target!r}")
    return REFERENCE_GRAPH[target]


def validate_reference_graph(
    graph: Mapping[str, Sequence[str]],
) -> list[str]:
    errors: list[str] = []
    # JSON objects are unordered by contract and every persisted FXPM artifact
    # is written with ``sort_keys=True``.  Target order therefore cannot be an
    # authenticated property of this mapping.  The ordered reference *arrays*
    # below are semantic; the mapping itself must contain the exact target set.
    if len(graph) != len(PRODUCTION_TIMEFRAMES) or set(graph) != set(
        PRODUCTION_TIMEFRAMES
    ):
        errors.append("CONSENSUS_REFERENCE_GRAPH_TARGET_ROSTER_INVALID")
    for target in PRODUCTION_TIMEFRAMES:
        raw = graph.get(target)
        references = tuple(str(value or "").strip().upper() for value in (raw or ()))
        if references != REFERENCE_GRAPH[target]:
            errors.append(f"CONSENSUS_REFERENCE_GRAPH_MISMATCH:{target}")
            continue
        if len(set(references)) != 2 or target in references:
            errors.append(f"CONSENSUS_REFERENCE_GRAPH_DISTINCTNESS_INVALID:{target}")
        for reference in references:
            target_seconds = TIMEFRAME_SECONDS[target]
            reference_seconds = TIMEFRAME_SECONDS[reference]
            larger = max(target_seconds, reference_seconds)
            smaller = min(target_seconds, reference_seconds)
            if larger % smaller:
                errors.append(
                    f"CONSENSUS_REFERENCE_GRAPH_NONINTEGRAL_RATIO:{target}:{reference}"
                )
    return list(dict.fromkeys(errors))


def _normalized_edge_state(value: Mapping[str, Any]) -> Tuple[str, str]:
    integrity = str(value.get("integrity_state") or "").strip().upper()
    evidence = str(value.get("evidence_state") or "").strip().upper()
    return integrity, evidence


def consensus_window_state(edge_states: Sequence[Mapping[str, Any]]) -> str:
    """Return the 1-of-2 support decision for one authenticated audit window."""

    if len(edge_states) != 2:
        return WINDOW_ERROR
    normalized = [_normalized_edge_state(value) for value in edge_states]
    if any(
        integrity not in VALID_INTEGRITY_STATES or evidence not in VALID_EVIDENCE_STATES
        for integrity, evidence in normalized
    ):
        return WINDOW_ERROR
    if any(integrity == INTEGRITY_ERROR for integrity, _evidence in normalized):
        return WINDOW_ERROR
    evidence_states = [evidence for _integrity, evidence in normalized]
    if EVIDENCE_PASS in evidence_states:
        return WINDOW_PASS
    if EVIDENCE_PENDING in evidence_states:
        return WINDOW_PENDING
    if all(
        evidence in {EVIDENCE_SEMANTIC_FAIL, EVIDENCE_TERMINAL_UNAVAILABLE}
        for evidence in evidence_states
    ):
        return WINDOW_FAIL
    return WINDOW_ERROR


def aggregate_window_states(states: Mapping[str, Any]) -> str:
    if set(states) != set(REQUIRED_AUDIT_WINDOWS):
        return WINDOW_ERROR
    normalized = [
        str(states[name] or "").strip().upper() for name in REQUIRED_AUDIT_WINDOWS
    ]
    if any(value not in VALID_WINDOW_STATES for value in normalized):
        return WINDOW_ERROR
    for candidate in (WINDOW_ERROR, WINDOW_FAIL, WINDOW_PENDING):
        if candidate in normalized:
            return candidate
    return WINDOW_PASS


def _binding_errors(
    binding: Any, label: str, *, unavailable_ok: bool = False
) -> list[str]:
    if not isinstance(binding, Mapping):
        return [f"CONSENSUS_{label}_BINDING_INVALID"]
    availability = str(binding.get("availability_state") or "AVAILABLE").strip().upper()
    if availability == EVIDENCE_TERMINAL_UNAVAILABLE and unavailable_ok:
        if any(
            binding.get(field) not in {None, ""}
            for field in ("bars_sha256", "provenance_sha256")
        ):
            return [f"CONSENSUS_{label}_UNAVAILABLE_HASH_PRESENT"]
        return []
    if availability == TECHNICAL_TERMINAL:
        if binding.get("bars_sha256") is not None:
            return [f"CONSENSUS_{label}_TECHNICAL_BAR_HASH_PRESENT"]
        if not _is_sha256(binding.get("provenance_sha256")):
            return [f"CONSENSUS_{label}_TECHNICAL_PROVENANCE_INVALID"]
        return []
    if availability != "AVAILABLE":
        return [f"CONSENSUS_{label}_AVAILABILITY_INVALID"]
    errors = []
    for field in ("bars_sha256", "provenance_sha256"):
        if not _is_sha256(binding.get(field)):
            errors.append(f"CONSENSUS_{label}_{field.upper()}_INVALID")
    return errors


def build_edge_report(
    *,
    symbol: str,
    target_timeframe: str,
    reference_timeframe: str,
    source_binding: Mapping[str, Any],
    reference_binding: Mapping[str, Any],
    window_states: Mapping[str, Mapping[str, Any]],
    pair_reports: Optional[Mapping[str, Mapping[str, Any]]] = None,
) -> Dict[str, Any]:
    target = str(target_timeframe or "").strip().upper()
    reference = str(reference_timeframe or "").strip().upper()
    if reference not in expected_reference_timeframes(target):
        raise ValueError(f"{reference} is not an authenticated reference for {target}")
    if set(window_states) != set(REQUIRED_AUDIT_WINDOWS):
        raise ValueError("edge window roster is incomplete")
    fine = (
        target
        if TIMEFRAME_SECONDS[target] < TIMEFRAME_SECONDS[reference]
        else reference
    )
    coarse = reference if fine == target else target
    normalized_windows: Dict[str, Dict[str, Any]] = {}
    pair_reports = pair_reports or {}
    for window in REQUIRED_AUDIT_WINDOWS:
        state = dict(window_states[window])
        integrity, evidence = _normalized_edge_state(state)
        if (
            integrity not in VALID_INTEGRITY_STATES
            or evidence not in VALID_EVIDENCE_STATES
        ):
            raise ValueError(f"invalid edge state for {window}")
        pair_report = pair_reports.get(window)
        pair_digest = (
            canonical_sha256(pair_report) if isinstance(pair_report, Mapping) else None
        )
        normalized_windows[window] = {
            "integrity_state": integrity,
            "evidence_state": evidence,
            "reason_codes": [str(value) for value in (state.get("reason_codes") or [])],
            "pair_report_sha256": pair_digest,
        }
    edge_statuses = {
        window: (
            WINDOW_ERROR
            if value["integrity_state"] == INTEGRITY_ERROR
            else (
                WINDOW_PASS
                if value["evidence_state"] == EVIDENCE_PASS
                else (
                    WINDOW_PENDING
                    if value["evidence_state"] == EVIDENCE_PENDING
                    else WINDOW_FAIL
                )
            )
        )
        for window, value in normalized_windows.items()
    }
    return {
        "schema_version": EDGE_SCHEMA_VERSION,
        "symbol": str(symbol or "").strip().upper(),
        "activation_target_timeframe": target,
        "reference_timeframe": reference,
        "fine_timeframe": fine,
        "coarse_timeframe": coarse,
        "status": aggregate_window_states(edge_statuses),
        "source_binding": _json_copy(source_binding),
        "reference_binding": _json_copy(reference_binding),
        "windows": normalized_windows,
    }


def validate_edge_report(
    report: Any,
    *,
    symbol: str,
    target_timeframe: str,
    reference_timeframe: str,
    source_binding: Mapping[str, Any],
    reference_binding: Mapping[str, Any],
) -> list[str]:
    label = f"{str(target_timeframe).upper()}:{str(reference_timeframe).upper()}"
    if not isinstance(report, Mapping):
        return [f"CONSENSUS_EDGE_REPORT_INVALID:{label}"]
    errors: list[str] = []
    target = str(target_timeframe).upper()
    reference = str(reference_timeframe).upper()
    if str(report.get("schema_version") or "") != EDGE_SCHEMA_VERSION:
        errors.append(f"CONSENSUS_EDGE_SCHEMA_INVALID:{label}")
    if str(report.get("symbol") or "").upper() != str(symbol).upper():
        errors.append(f"CONSENSUS_EDGE_SYMBOL_MISMATCH:{label}")
    if str(report.get("activation_target_timeframe") or "").upper() != target:
        errors.append(f"CONSENSUS_EDGE_TARGET_MISMATCH:{label}")
    if str(report.get("reference_timeframe") or "").upper() != reference:
        errors.append(f"CONSENSUS_EDGE_REFERENCE_MISMATCH:{label}")
    fine = (
        target
        if TIMEFRAME_SECONDS[target] < TIMEFRAME_SECONDS[reference]
        else reference
    )
    coarse = reference if fine == target else target
    if str(report.get("fine_timeframe") or "").upper() != fine:
        errors.append(f"CONSENSUS_EDGE_FINE_ORIENTATION_INVALID:{label}")
    if str(report.get("coarse_timeframe") or "").upper() != coarse:
        errors.append(f"CONSENSUS_EDGE_COARSE_ORIENTATION_INVALID:{label}")
    if report.get("source_binding") != source_binding:
        errors.append(f"CONSENSUS_EDGE_SOURCE_BINDING_MISMATCH:{label}")
    if report.get("reference_binding") != reference_binding:
        errors.append(f"CONSENSUS_EDGE_REFERENCE_BINDING_MISMATCH:{label}")
    errors.extend(_binding_errors(source_binding, f"EDGE_SOURCE:{label}"))
    errors.extend(
        _binding_errors(
            reference_binding, f"EDGE_REFERENCE:{label}", unavailable_ok=True
        )
    )
    windows = report.get("windows")
    if not isinstance(windows, Mapping) or set(windows) != set(REQUIRED_AUDIT_WINDOWS):
        errors.append(f"CONSENSUS_EDGE_WINDOW_ROSTER_INVALID:{label}")
        return list(dict.fromkeys(errors))
    statuses: Dict[str, str] = {}
    for window in REQUIRED_AUDIT_WINDOWS:
        state = windows.get(window)
        if not isinstance(state, Mapping):
            errors.append(f"CONSENSUS_EDGE_WINDOW_INVALID:{label}:{window}")
            statuses[window] = WINDOW_ERROR
            continue
        integrity, evidence = _normalized_edge_state(state)
        if (
            integrity not in VALID_INTEGRITY_STATES
            or evidence not in VALID_EVIDENCE_STATES
        ):
            errors.append(f"CONSENSUS_EDGE_STATE_INVALID:{label}:{window}")
        pair_digest = state.get("pair_report_sha256")
        if pair_digest is not None and not _is_sha256(pair_digest):
            errors.append(f"CONSENSUS_EDGE_PAIR_HASH_INVALID:{label}:{window}")
        statuses[window] = (
            WINDOW_ERROR
            if integrity == INTEGRITY_ERROR
            else WINDOW_PASS
            if evidence == EVIDENCE_PASS
            else WINDOW_PENDING
            if evidence == EVIDENCE_PENDING
            else WINDOW_FAIL
        )
    if str(report.get("status") or "").upper() != aggregate_window_states(statuses):
        errors.append(f"CONSENSUS_EDGE_STATUS_DERIVATION_MISMATCH:{label}")
    return list(dict.fromkeys(errors))


def build_consensus_envelope(
    *,
    symbol: str,
    target_timeframe: str,
    source_binding: Mapping[str, Any],
    reference_bindings: Mapping[str, Mapping[str, Any]],
    edge_reports: Mapping[str, Mapping[str, Any]],
) -> Dict[str, Any]:
    target = str(target_timeframe or "").strip().upper()
    references = expected_reference_timeframes(target)
    if (
        len(reference_bindings) != len(references)
        or set(reference_bindings) != set(references)
        or len(edge_reports) != len(references)
        or set(edge_reports) != set(references)
    ):
        raise ValueError("consensus references must match the exact graph roster")
    errors: list[str] = []
    for reference in references:
        errors.extend(
            validate_edge_report(
                edge_reports[reference],
                symbol=symbol,
                target_timeframe=target,
                reference_timeframe=reference,
                source_binding=source_binding,
                reference_binding=reference_bindings[reference],
            )
        )
    if errors:
        raise ValueError("; ".join(errors))
    windows: Dict[str, Dict[str, Any]] = {}
    for window in REQUIRED_AUDIT_WINDOWS:
        states = [
            edge_reports[reference]["windows"][window] for reference in references
        ]
        windows[window] = {
            "status": consensus_window_state(states),
            "edge_states": {
                reference: {
                    "integrity_state": states[index]["integrity_state"],
                    "evidence_state": states[index]["evidence_state"],
                }
                for index, reference in enumerate(references)
            },
        }
    status = aggregate_window_states(
        {window: value["status"] for window, value in windows.items()}
    )
    envelope = {
        "schema_version": CONSENSUS_SCHEMA_VERSION,
        "symbol": str(symbol or "").strip().upper(),
        "timeframe": target,
        "activation_target_timeframe": target,
        "status": status,
        "reference_timeframes": list(references),
        "reference_graph_sha256": REFERENCE_GRAPH_SHA256,
        "policy": _json_copy(CONSENSUS_POLICY),
        "policy_sha256": CONSENSUS_POLICY_SHA256,
        "source_binding": _json_copy(source_binding),
        "reference_bindings": _json_copy(reference_bindings),
        "edges": {
            reference: {
                "report_sha256": canonical_sha256(edge_reports[reference]),
                "report": _json_copy(edge_reports[reference]),
            }
            for reference in references
        },
        "windows": windows,
        "diagnostics": {
            "all_references_evaluated": True,
            "fault_model": CONSENSUS_POLICY["fault_model"],
            "common_omission_caveat": (
                "An interval omitted identically by the target and both broker-native "
                "references requires an external source or authenticated calendar to detect."
            ),
        },
    }
    envelope["consensus_sha256"] = canonical_sha256(
        {key: value for key, value in envelope.items() if key != "consensus_sha256"}
    )
    return envelope


def validate_consensus_envelope(
    envelope: Any,
    *,
    symbol: str,
    target_timeframe: str,
    source_binding: Mapping[str, Any],
    reference_bindings: Mapping[str, Mapping[str, Any]],
) -> list[str]:
    if not isinstance(envelope, Mapping):
        return ["CONSENSUS_ENVELOPE_INVALID"]
    errors: list[str] = []
    target = str(target_timeframe or "").strip().upper()
    try:
        references = expected_reference_timeframes(target)
    except ValueError:
        return ["CONSENSUS_TARGET_TIMEFRAME_INVALID"]
    if str(envelope.get("schema_version") or "") != CONSENSUS_SCHEMA_VERSION:
        errors.append("CONSENSUS_SCHEMA_INVALID")
    if str(envelope.get("symbol") or "").upper() != str(symbol).upper():
        errors.append("CONSENSUS_SYMBOL_MISMATCH")
    for field in ("timeframe", "activation_target_timeframe"):
        if str(envelope.get(field) or "").upper() != target:
            errors.append(f"CONSENSUS_{field.upper()}_MISMATCH")
    if tuple(envelope.get("reference_timeframes") or ()) != references:
        errors.append("CONSENSUS_REFERENCE_ORDER_INVALID")
    if envelope.get("reference_graph_sha256") != REFERENCE_GRAPH_SHA256:
        errors.append("CONSENSUS_REFERENCE_GRAPH_HASH_INVALID")
    if envelope.get("policy") != CONSENSUS_POLICY:
        errors.append("CONSENSUS_POLICY_MISMATCH")
    if envelope.get("policy_sha256") != CONSENSUS_POLICY_SHA256:
        errors.append("CONSENSUS_POLICY_HASH_INVALID")
    if envelope.get("source_binding") != source_binding:
        errors.append("CONSENSUS_SOURCE_BINDING_MISMATCH")
    if envelope.get("reference_bindings") != reference_bindings:
        errors.append("CONSENSUS_REFERENCE_BINDINGS_MISMATCH")
    errors.extend(_binding_errors(source_binding, "SOURCE"))
    if len(reference_bindings) != len(references) or set(reference_bindings) != set(
        references
    ):
        errors.append("CONSENSUS_REFERENCE_BINDING_ROSTER_INVALID")
    for reference in references:
        errors.extend(
            _binding_errors(
                reference_bindings.get(reference),
                f"REFERENCE:{reference}",
                unavailable_ok=True,
            )
        )
    edges = envelope.get("edges")
    if (
        not isinstance(edges, Mapping)
        or len(edges) != len(references)
        or set(edges) != set(references)
    ):
        errors.append("CONSENSUS_EDGE_ROSTER_INVALID")
        return list(dict.fromkeys(errors))
    edge_reports: Dict[str, Mapping[str, Any]] = {}
    for reference in references:
        edge = edges.get(reference)
        if not isinstance(edge, Mapping) or not isinstance(edge.get("report"), Mapping):
            errors.append(f"CONSENSUS_EDGE_INVALID:{reference}")
            continue
        report = edge["report"]
        edge_reports[reference] = report
        if str(edge.get("report_sha256") or "") != canonical_sha256(report):
            errors.append(f"CONSENSUS_EDGE_HASH_INVALID:{reference}")
        errors.extend(
            validate_edge_report(
                report,
                symbol=symbol,
                target_timeframe=target,
                reference_timeframe=reference,
                source_binding=source_binding,
                reference_binding=reference_bindings.get(reference, {}),
            )
        )
    windows = envelope.get("windows")
    if not isinstance(windows, Mapping) or set(windows) != set(REQUIRED_AUDIT_WINDOWS):
        errors.append("CONSENSUS_WINDOW_ROSTER_INVALID")
    elif len(edge_reports) == 2:
        derived_states: Dict[str, str] = {}
        for window in REQUIRED_AUDIT_WINDOWS:
            detail = windows.get(window)
            if not isinstance(detail, Mapping):
                errors.append(f"CONSENSUS_WINDOW_INVALID:{window}")
                continue
            edge_states = [
                edge_reports[reference]["windows"][window] for reference in references
            ]
            expected = consensus_window_state(edge_states)
            derived_states[window] = expected
            if str(detail.get("status") or "").upper() != expected:
                errors.append(f"CONSENSUS_WINDOW_STATUS_DERIVATION_MISMATCH:{window}")
            expected_projection = {
                reference: {
                    "integrity_state": edge_states[index]["integrity_state"],
                    "evidence_state": edge_states[index]["evidence_state"],
                }
                for index, reference in enumerate(references)
            }
            if detail.get("edge_states") != expected_projection:
                errors.append(f"CONSENSUS_WINDOW_EDGE_PROJECTION_MISMATCH:{window}")
        if len(derived_states) == len(REQUIRED_AUDIT_WINDOWS):
            expected_status = aggregate_window_states(derived_states)
            if str(envelope.get("status") or "").upper() != expected_status:
                errors.append("CONSENSUS_STATUS_DERIVATION_MISMATCH")
    declared_digest = str(envelope.get("consensus_sha256") or "")
    observed_digest = canonical_sha256(
        {key: value for key, value in envelope.items() if key != "consensus_sha256"}
    )
    if declared_digest != observed_digest:
        errors.append("CONSENSUS_ENVELOPE_HASH_INVALID")
    diagnostics = envelope.get("diagnostics")
    if (
        not isinstance(diagnostics, Mapping)
        or diagnostics.get("all_references_evaluated") is not True
    ):
        errors.append("CONSENSUS_ALL_REFERENCES_NOT_PROVEN")
    return list(dict.fromkeys(errors))


def production_consensus_schema_valid(value: Any) -> bool:
    return str(value or "") == CONSENSUS_SCHEMA_VERSION
