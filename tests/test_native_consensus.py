import copy
import itertools

import pytest

from pm_native_consensus import (
    CONSENSUS_POLICY,
    CONSENSUS_POLICY_SHA256,
    CONSENSUS_SCHEMA_VERSION,
    EDGE_SCHEMA_VERSION,
    EVIDENCE_PASS,
    EVIDENCE_PENDING,
    EVIDENCE_SEMANTIC_FAIL,
    EVIDENCE_TERMINAL_UNAVAILABLE,
    INTEGRITY_ERROR,
    INTEGRITY_PASS,
    PRODUCTION_TIMEFRAMES,
    REFERENCE_GRAPH,
    REFERENCE_GRAPH_SHA256,
    REQUIRED_AUDIT_WINDOWS,
    WINDOW_ERROR,
    WINDOW_FAIL,
    WINDOW_PASS,
    WINDOW_PENDING,
    aggregate_window_states,
    build_consensus_envelope,
    build_edge_report,
    canonical_sha256,
    consensus_window_state,
    expected_reference_timeframes,
    production_consensus_schema_valid,
    validate_consensus_envelope,
    validate_reference_graph,
)


def _binding(seed: str, *, unavailable: bool = False):
    if unavailable:
        return {
            "availability_state": EVIDENCE_TERMINAL_UNAVAILABLE,
            "bars_sha256": None,
            "provenance_sha256": None,
        }
    character = seed[0].lower()
    return {
        "availability_state": "AVAILABLE",
        "bars_sha256": character * 64,
        "provenance_sha256": chr(ord(character) + 1) * 64,
    }


def _states(evidence=EVIDENCE_PASS, integrity=INTEGRITY_PASS):
    return {
        window: {
            "integrity_state": integrity,
            "evidence_state": evidence,
            "reason_codes": [],
        }
        for window in REQUIRED_AUDIT_WINDOWS
    }


def _envelope(target="H1", overrides=None, unavailable=()):
    references = expected_reference_timeframes(target)
    source = _binding("a")
    reference_bindings = {
        reference: _binding(chr(ord("c") + index), unavailable=reference in unavailable)
        for index, reference in enumerate(references)
    }
    edge_reports = {}
    for reference in references:
        states = _states(
            EVIDENCE_TERMINAL_UNAVAILABLE
            if reference in unavailable
            else EVIDENCE_PASS
        )
        for window, state in ((overrides or {}).get(reference, {}) or {}).items():
            states[window] = dict(state)
        edge_reports[reference] = build_edge_report(
            symbol="EURUSD",
            target_timeframe=target,
            reference_timeframe=reference,
            source_binding=source,
            reference_binding=reference_bindings[reference],
            window_states=states,
            pair_reports={
                window: {"schema_version": "pair", "window": window}
                for window in REQUIRED_AUDIT_WINDOWS
            },
        )
    envelope = build_consensus_envelope(
        symbol="EURUSD",
        target_timeframe=target,
        source_binding=source,
        reference_bindings=reference_bindings,
        edge_reports=edge_reports,
    )
    return envelope, source, reference_bindings


def _rehash(envelope):
    envelope["consensus_sha256"] = canonical_sha256(
        {key: value for key, value in envelope.items() if key != "consensus_sha256"}
    )


def test_reference_graph_is_exact_integral_and_deterministic():
    assert tuple(REFERENCE_GRAPH) == PRODUCTION_TIMEFRAMES
    assert validate_reference_graph(REFERENCE_GRAPH) == []
    assert REFERENCE_GRAPH == {
        "M5": ("M15", "M30"),
        "M15": ("M5", "M30"),
        "M30": ("M15", "H1"),
        "H1": ("M30", "H4"),
        "H4": ("H1", "D1"),
        "D1": ("H4", "H1"),
    }
    assert REFERENCE_GRAPH_SHA256 == canonical_sha256(
        {target: list(REFERENCE_GRAPH[target]) for target in PRODUCTION_TIMEFRAMES}
    )


@pytest.mark.parametrize(
    ("left", "right", "expected"),
    [
        (left, right, expected)
        for left, right in itertools.product(
            (
                EVIDENCE_PASS,
                EVIDENCE_SEMANTIC_FAIL,
                EVIDENCE_TERMINAL_UNAVAILABLE,
                EVIDENCE_PENDING,
            ),
            repeat=2,
        )
        for expected in [
            (
                WINDOW_PASS
                if EVIDENCE_PASS in {left, right}
                else WINDOW_PENDING
                if EVIDENCE_PENDING in {left, right}
                else WINDOW_FAIL
            )
        ]
    ],
)
def test_window_consensus_truth_table(left, right, expected):
    observed = consensus_window_state(
        [
            {"integrity_state": INTEGRITY_PASS, "evidence_state": left},
            {"integrity_state": INTEGRITY_PASS, "evidence_state": right},
        ]
    )
    assert observed == expected


def test_integrity_error_dominates_even_when_other_reference_passes():
    assert consensus_window_state(
        [
            {"integrity_state": INTEGRITY_ERROR, "evidence_state": EVIDENCE_PENDING},
            {"integrity_state": INTEGRITY_PASS, "evidence_state": EVIDENCE_PASS},
        ]
    ) == WINDOW_ERROR
    assert consensus_window_state([]) == WINDOW_ERROR


def test_window_aggregation_precedence_is_error_fail_pending_pass():
    all_pass = dict.fromkeys(REQUIRED_AUDIT_WINDOWS, WINDOW_PASS)
    assert aggregate_window_states(all_pass) == WINDOW_PASS
    for state in (WINDOW_PENDING, WINDOW_FAIL, WINDOW_ERROR):
        values = dict(all_pass)
        values["optimizer_train"] = state
        assert aggregate_window_states(values) == state
    values = dict(all_pass)
    values["optimizer_train"] = WINDOW_PENDING
    values["tuning_validation"] = WINDOW_FAIL
    values["final_lockbox"] = WINDOW_ERROR
    assert aggregate_window_states(values) == WINDOW_ERROR


def test_clean_envelope_binds_both_edges_and_recomputes_validly():
    envelope, source, references = _envelope()
    assert envelope["schema_version"] == CONSENSUS_SCHEMA_VERSION
    assert envelope["status"] == WINDOW_PASS
    assert list(envelope["edges"]) == ["M30", "H4"]
    assert envelope["policy"] == CONSENSUS_POLICY
    assert envelope["policy_sha256"] == CONSENSUS_POLICY_SHA256
    assert envelope["reference_graph_sha256"] == REFERENCE_GRAPH_SHA256
    assert all(
        envelope["windows"][window]["status"] == WINDOW_PASS
        for window in REQUIRED_AUDIT_WINDOWS
    )
    assert validate_consensus_envelope(
        envelope,
        symbol="EURUSD",
        target_timeframe="H1",
        source_binding=source,
        reference_bindings=references,
    ) == []


def test_partition_mosaic_passes_when_each_window_has_one_supporting_edge():
    fail = {
        "integrity_state": INTEGRITY_PASS,
        "evidence_state": EVIDENCE_SEMANTIC_FAIL,
        "reason_codes": ["TEST_FAIL"],
    }
    overrides = {
        "M30": {"tuning_validation": fail},
        "H4": {"optimizer_train": fail},
    }
    envelope, source, references = _envelope(overrides=overrides)
    assert envelope["status"] == WINDOW_PASS
    assert envelope["windows"]["optimizer_train"]["status"] == WINDOW_PASS
    assert envelope["windows"]["tuning_validation"]["status"] == WINDOW_PASS
    assert validate_consensus_envelope(
        envelope,
        symbol="EURUSD",
        target_timeframe="H1",
        source_binding=source,
        reference_bindings=references,
    ) == []


def test_window_fails_only_when_both_references_are_terminal():
    fail = {
        "integrity_state": INTEGRITY_PASS,
        "evidence_state": EVIDENCE_SEMANTIC_FAIL,
        "reason_codes": ["TEST_FAIL"],
    }
    unavailable = {
        "integrity_state": INTEGRITY_PASS,
        "evidence_state": EVIDENCE_TERMINAL_UNAVAILABLE,
        "reason_codes": ["REFERENCE_UNAVAILABLE"],
    }
    overrides = {
        "M30": {"final_lockbox": fail},
        "H4": {"final_lockbox": unavailable},
    }
    envelope, _source, _references = _envelope(overrides=overrides)
    assert envelope["windows"]["final_lockbox"]["status"] == WINDOW_FAIL
    assert envelope["status"] == WINDOW_FAIL


def test_fail_plus_pending_is_pending_without_a_passing_witness():
    overrides = {
        "M30": {
            "strategy_full_context": {
                "integrity_state": INTEGRITY_PASS,
                "evidence_state": EVIDENCE_SEMANTIC_FAIL,
            }
        },
        "H4": {
            "strategy_full_context": {
                "integrity_state": INTEGRITY_PASS,
                "evidence_state": EVIDENCE_PENDING,
            }
        },
    }
    envelope, _source, _references = _envelope(overrides=overrides)
    assert envelope["windows"]["strategy_full_context"]["status"] == WINDOW_PENDING
    assert envelope["status"] == WINDOW_PENDING


def test_one_unavailable_reference_plus_one_pass_still_passes():
    envelope, source, references = _envelope(unavailable=("M30",))
    assert envelope["status"] == WINDOW_PASS
    assert validate_consensus_envelope(
        envelope,
        symbol="EURUSD",
        target_timeframe="H1",
        source_binding=source,
        reference_bindings=references,
    ) == []


@pytest.mark.parametrize(
    ("tamper", "expected_code"),
    [
        ("top_status", "CONSENSUS_STATUS_DERIVATION_MISMATCH"),
        ("edge_hash", "CONSENSUS_EDGE_HASH_INVALID:M30"),
        ("orientation", "CONSENSUS_EDGE_TARGET_MISMATCH:H1:M30"),
        ("window_projection", "CONSENSUS_WINDOW_EDGE_PROJECTION_MISMATCH:optimizer_train"),
        ("drop_edge", "CONSENSUS_EDGE_ROSTER_INVALID"),
    ],
)
def test_repinning_derived_fields_cannot_bypass_recomputation(tamper, expected_code):
    envelope, source, references = _envelope()
    changed = copy.deepcopy(envelope)
    if tamper == "top_status":
        changed["status"] = WINDOW_FAIL
    elif tamper == "edge_hash":
        changed["edges"]["M30"]["report_sha256"] = "f" * 64
    elif tamper == "orientation":
        changed["edges"]["M30"]["report"]["activation_target_timeframe"] = "M30"
        changed["edges"]["M30"]["report_sha256"] = canonical_sha256(
            changed["edges"]["M30"]["report"]
        )
    elif tamper == "window_projection":
        changed["windows"]["optimizer_train"]["edge_states"]["M30"][
            "evidence_state"
        ] = EVIDENCE_SEMANTIC_FAIL
    else:
        changed["edges"].pop("H4")
    _rehash(changed)
    errors = validate_consensus_envelope(
        changed,
        symbol="EURUSD",
        target_timeframe="H1",
        source_binding=source,
        reference_bindings=references,
    )
    assert expected_code in errors


def test_swapped_reference_order_and_singular_v4_schema_are_not_production_valid():
    envelope, source, references = _envelope()
    changed = copy.deepcopy(envelope)
    changed["reference_timeframes"] = list(reversed(changed["reference_timeframes"]))
    _rehash(changed)
    assert "CONSENSUS_REFERENCE_ORDER_INVALID" in validate_consensus_envelope(
        changed,
        symbol="EURUSD",
        target_timeframe="H1",
        source_binding=source,
        reference_bindings=references,
    )
    assert production_consensus_schema_valid(CONSENSUS_SCHEMA_VERSION)
    assert not production_consensus_schema_valid(
        "fxpm-native-cross-resolution-parity-v2"
    )


def test_edge_schema_and_envelope_serialization_are_deterministic():
    first, _source, _references = _envelope(target="D1")
    second, _source_2, _references_2 = _envelope(target="D1")
    assert first == second
    assert first["consensus_sha256"] == second["consensus_sha256"]
    assert all(
        edge["report"]["schema_version"] == EDGE_SCHEMA_VERSION
        for edge in first["edges"].values()
    )
