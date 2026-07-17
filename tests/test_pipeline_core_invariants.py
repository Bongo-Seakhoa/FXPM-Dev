"""Core invariant regressions for the seq937-concurred pm_pipeline unblock.

Covers: (1) NON_SEARCH tuner-sidecar consumption in
``validate_regime_params_provenance`` (seq935/936) — typed authorization
outcome, never a gate failure, never an economic verdict; (2) the seq936
counts identity; (3) seq932 trial-telemetry aggregation semantics.  The
seq931 run_for_symbol invariant (technical evidence failure -> unsuccessful,
ledger untouched) is asserted end-to-end in tests/test_native_evidence.py,
patched in lockstep by its claim owner.
"""

from __future__ import annotations

import hashlib
import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from pm_pipeline import _aggregate_search_trial_telemetry  # noqa: E402
from pm_regime import (  # noqa: E402
    REGIME_NON_SEARCH_SCHEMA_VERSION,
    REGIME_PROVENANCE_SCHEMA_VERSION,
    REGIME_TRAIN_SUPPORT_SCHEMA_VERSION,
    _tuner_canonical_sha256,
    _validated_train_regime_support,
    validate_regime_params_provenance,
)

_ACTIVATION_ID = "activation-test-0001"
_ACTIVATION_MANIFEST = "ab" * 32


def _non_search_record(symbol: str, timeframe: str, **overrides) -> dict:
    record = {
        "schema_version": REGIME_NON_SEARCH_SCHEMA_VERSION,
        "status": "NON_SEARCH",
        "symbol": symbol.upper(),
        "timeframe": timeframe.upper(),
        "fit_scope": "optimizer_train",
        "generated_at": "2026-07-14T00:00:00+00:00",
        "decision_kind": "INSUFFICIENT_TRAIN_HISTORY",
        "reason_code": "INSUFFICIENT_OPTIMIZER_TRAIN_ROWS",
        "reason": "optimizer-train partition below detector floor",
        "activation_id": _ACTIVATION_ID,
        "activation_manifest_sha256": _ACTIVATION_MANIFEST,
        "window_contract_version": "test-window/v1",
        "economic_evidence_used": False,
        "economic_trade_verdict_emitted": False,
        "trade_verdict": False,
        "no_trade": False,
        "strategy_search_authorized": False,
        "generation_terminal": True,
        "retryable": False,
    }
    record.update(overrides)
    canonical = json.dumps(record, sort_keys=True, separators=(",", ":"))
    record["decision_sha256"] = hashlib.sha256(canonical.encode("utf-8")).hexdigest()
    return record


def _write_sidecar_pair(
    tmp_path: Path,
    symbol: str,
    timeframe: str,
    record: dict,
    *,
    counts: tuple[int, int, int, int] = (0, 1, 1, 1),
) -> tuple[Path, Path]:
    params_path = tmp_path / "regime_params.json"
    params_path.write_text(json.dumps({}), encoding="utf-8")
    params_sha = hashlib.sha256(params_path.read_bytes()).hexdigest()
    fitted, non_search, resolved, required = counts
    sidecar = {
        "schema_version": REGIME_PROVENANCE_SCHEMA_VERSION,
        "generation_status": "COMPLETE",
        "params_file_sha256": params_sha,
        "activation_id": _ACTIVATION_ID,
        "activation_manifest_sha256": _ACTIVATION_MANIFEST,
        "fitted_active_cell_count": fitted,
        "non_search_active_cell_count": non_search,
        "resolved_active_cell_count": resolved,
        "required_evidence_candidate_cell_count": required,
        "cells": {},
        "non_search_cells": {symbol.upper(): {timeframe.upper(): record}},
    }
    sidecar_path = tmp_path / "regime_params.provenance.json"
    sidecar_path.write_text(json.dumps(sidecar), encoding="utf-8")
    return params_path, sidecar_path


def _validate(tmp_path: Path, record: dict, **kwargs):
    params_path, sidecar_path = _write_sidecar_pair(
        tmp_path, "EURUSD", "H4", record, **kwargs
    )
    return validate_regime_params_provenance(
        "EURUSD",
        "H4",
        str(params_path),
        provenance_filepath=str(sidecar_path),
        expected_activation_id=_ACTIVATION_ID,
        expected_activation_manifest_sha256=_ACTIVATION_MANIFEST,
        require_complete_generation=True,
    )


def test_non_search_cell_is_valid_typed_and_never_a_gate_failure(tmp_path) -> None:
    check = _validate(tmp_path, _non_search_record("EURUSD", "H4"))
    assert check["valid"] is True
    assert check["status"] == "NON_SEARCH"
    assert check["decision_kind"] == "INSUFFICIENT_TRAIN_HISTORY"
    assert check["reason"] == "INSUFFICIENT_OPTIMIZER_TRAIN_ROWS"
    # A NON_SEARCH decision must never look like an economic verdict.
    assert check["record"]["no_trade"] is False
    assert check["record"]["strategy_search_authorized"] is False


def test_non_search_record_smuggling_a_verdict_fails_closed(tmp_path) -> None:
    for poison in (
        {"no_trade": True},
        {"trade_verdict": True},
        {"strategy_search_authorized": True},
        {"status": "FIT"},
    ):
        check = _validate(tmp_path, _non_search_record("EURUSD", "H4", **poison))
        assert check["valid"] is False, poison
        assert check.get("status") != "NON_SEARCH", poison


def test_non_search_record_with_tampered_hash_fails_closed(tmp_path) -> None:
    record = _non_search_record("EURUSD", "H4")
    record["decision_sha256"] = "0" * 64
    check = _validate(tmp_path, record)
    assert check["valid"] is False


def test_non_search_record_bound_to_wrong_activation_fails_closed(tmp_path) -> None:
    record = _non_search_record(
        "EURUSD", "H4", activation_id="some-other-activation"
    )
    check = _validate(tmp_path, record)
    assert check["valid"] is False


def test_counts_identity_is_enforced_on_complete_generations(tmp_path) -> None:
    broken = _validate(
        tmp_path, _non_search_record("EURUSD", "H4"), counts=(1, 1, 1, 1)
    )
    assert broken["valid"] is False
    assert "counts identity" in broken["reason"]
    missing = _validate(
        tmp_path, _non_search_record("EURUSD", "H4"), counts=(0, 1, 1, 2)
    )
    assert missing["valid"] is False


def test_missing_cell_without_non_search_record_still_rejects(tmp_path) -> None:
    params_path = tmp_path / "regime_params.json"
    params_path.write_text(json.dumps({}), encoding="utf-8")
    sidecar = {
        "schema_version": REGIME_PROVENANCE_SCHEMA_VERSION,
        "generation_status": "COMPLETE",
        "params_file_sha256": hashlib.sha256(
            params_path.read_bytes()
        ).hexdigest(),
        "activation_id": _ACTIVATION_ID,
        "activation_manifest_sha256": _ACTIVATION_MANIFEST,
        "fitted_active_cell_count": 0,
        "non_search_active_cell_count": 0,
        "resolved_active_cell_count": 0,
        "required_evidence_candidate_cell_count": 0,
        "cells": {},
        "non_search_cells": {},
    }
    sidecar_path = tmp_path / "regime_params.provenance.json"
    sidecar_path.write_text(json.dumps(sidecar), encoding="utf-8")
    check = validate_regime_params_provenance(
        "EURUSD",
        "H4",
        str(params_path),
        provenance_filepath=str(sidecar_path),
        require_complete_generation=True,
    )
    assert check["valid"] is False
    assert check["reason"] == "regime provenance cell is missing"


def _outcome(planned: int, completed: int, eligible: int = 0) -> dict:
    return {
        "phase": "family_probe_b0",
        "strategy_family": "test-family",
        "regime": "TREND",
        "search_trials": planned,
        "completed_trials": completed,
        "eligible_train_trials": eligible,
        "frozen_candidate": False,
    }


def test_trial_telemetry_sums_outcome_ledger_per_timeframe() -> None:
    telemetry = _aggregate_search_trial_telemetry(
        {
            "M5": [_outcome(500, 480, 90), _outcome(400, 400, 60)],
            "H4": [_outcome(300, 290, 40)],
        },
        [
            {"timeframe": "M5", "local_search_trials_total": 900, "search_trials": 40},
            {"timeframe": "H4", "local_search_trials_total": 300, "search_trials": 10},
        ],
    )
    assert telemetry["planned_search_trials_total"] == 1200
    assert telemetry["completed_search_trials_total"] == 1170
    assert telemetry["eligible_train_trials_total"] == 190
    assert telemetry["completed_search_trials_by_timeframe"] == {
        "H4": 290,
        "M5": 880,
    }
    assert telemetry["telemetry_anomalies"] == []
    assert telemetry["surviving_candidate_search_trials_sum"] == 50
    assert telemetry["survivor_local_search_trials_by_timeframe"] == {
        "H4": 300,
        "M5": 900,
    }


def test_trial_telemetry_all_rejected_still_reports_completed_trials() -> None:
    # seq950: every candidate gated out before OOS evidence exists — the
    # outcome ledger still proves real completed trials; never a silent 0.
    telemetry = _aggregate_search_trial_telemetry(
        {"M15": [_outcome(328, 328, 0)]},
        [],
    )
    assert telemetry["completed_search_trials_total"] == 328
    assert telemetry["planned_search_trials_total"] == 328
    assert telemetry["oos_candidate_evidence_records"] == 0
    assert telemetry["telemetry_anomalies"] == []


def test_trial_telemetry_searched_but_zero_completed_is_typed_anomaly() -> None:
    telemetry = _aggregate_search_trial_telemetry(
        {"H1": [_outcome(200, 0, 0)]},
        [],
    )
    assert telemetry["completed_search_trials_total"] == 0
    assert telemetry["telemetry_anomalies"] == [
        "SEARCHED_BUT_ZERO_COMPLETED_TRIALS:H1:planned=200"
    ]


def test_trial_telemetry_nothing_searched_reports_zero_without_anomaly() -> None:
    telemetry = _aggregate_search_trial_telemetry({}, [])
    assert telemetry["planned_search_trials_total"] == 0
    assert telemetry["completed_search_trials_total"] == 0
    assert telemetry["rejected_train_trials_total"] == 0
    assert telemetry["telemetry_anomalies"] == []


def test_trial_telemetry_seq960_zero_survivor_rejected_counts() -> None:
    # seq960 pinned case: planned=10, completed=8, eligible=0
    # => completed 8, rejected 8, never 0.
    telemetry = _aggregate_search_trial_telemetry(
        {"M5": [_outcome(10, 8, 0)]}, []
    )
    assert telemetry["completed_search_trials_total"] == 8
    assert telemetry["rejected_train_trials_total"] == 8
    assert telemetry["rejected_train_trials_by_timeframe"] == {"M5": 8}
    assert telemetry["eligible_train_trials_by_timeframe"] == {"M5": 0}
    assert telemetry["telemetry_anomalies"] == []


def test_trial_telemetry_eligible_exceeding_completed_is_typed() -> None:
    telemetry = _aggregate_search_trial_telemetry(
        {"M5": [_outcome(10, 8, 9)]}, []
    )
    assert (
        "ELIGIBLE_EXCEEDS_COMPLETED:M5:eligible=9:completed=8"
        in telemetry["fatal_telemetry_errors"]
    )
    # An impossible ordering never manufactures a negative rejected count.
    assert telemetry["rejected_train_trials_by_timeframe"] == {"M5": 0}


def test_trial_telemetry_completed_exceeding_planned_is_typed() -> None:
    telemetry = _aggregate_search_trial_telemetry(
        {"H4": [_outcome(10, 12, 4)]}, []
    )
    assert (
        "COMPLETED_EXCEEDS_PLANNED:H4:completed=12:planned=10"
        in telemetry["fatal_telemetry_errors"]
    )


def test_trial_telemetry_negative_counts_are_typed_and_never_summed() -> None:
    telemetry = _aggregate_search_trial_telemetry(
        {"H1": [_outcome(-2, -3, -1), _outcome(5, 5, 2)]}, []
    )
    assert any(
        anomaly.startswith("INVALID_SEARCH_CELL_COUNTS:H1:cell=0")
        for anomaly in telemetry["fatal_telemetry_errors"]
    )
    # The garbage cell contributes nothing; the valid cell still counts.
    assert telemetry["planned_search_trials_total"] == 5
    assert telemetry["completed_search_trials_total"] == 5
    assert telemetry["rejected_train_trials_total"] == 3


def test_trial_telemetry_non_integer_counts_are_typed() -> None:
    bad = _outcome(5, 5, 1)
    bad["completed_trials"] = "5"
    telemetry = _aggregate_search_trial_telemetry({"D1": [bad]}, [])
    assert any(
        anomaly.startswith("INVALID_SEARCH_CELL_COUNTS:D1:cell=0")
        for anomaly in telemetry["fatal_telemetry_errors"]
    )
    assert telemetry["planned_search_trials_total"] == 0


def test_evidence_no_trade_markers_helper_is_deleted() -> None:
    import pm_pipeline

    assert not hasattr(
        pm_pipeline.OptimizationPipeline, "_evidence_no_trade_markers"
    )


def test_fatal_errors_are_split_from_nonfatal_anomalies() -> None:
    # seq979: impossible counts are fatal; zero-completed is advisory.
    fatal = _aggregate_search_trial_telemetry(
        {"M5": [_outcome(10, 12, 4)], "H1": [_outcome(-2, -3, -1)]}, []
    )
    assert any(
        e.startswith("COMPLETED_EXCEEDS_PLANNED:M5")
        for e in fatal["fatal_telemetry_errors"]
    )
    assert any(
        e.startswith("INVALID_SEARCH_CELL_COUNTS:H1")
        for e in fatal["fatal_telemetry_errors"]
    )
    assert all(
        a.startswith("SEARCHED_BUT_ZERO") for a in fatal["telemetry_anomalies"]
    )
    nonfatal = _aggregate_search_trial_telemetry({"H4": [_outcome(9, 0, 0)]}, [])
    assert nonfatal["fatal_telemetry_errors"] == []
    assert nonfatal["telemetry_anomalies"] == [
        "SEARCHED_BUT_ZERO_COMPLETED_TRIALS:H4:planned=9"
    ]
    clean = _aggregate_search_trial_telemetry({"M5": [_outcome(10, 8, 0)]}, [])
    assert clean["fatal_telemetry_errors"] == []
    assert clean["rejected_train_trials_total"] == 8


def _support_receipt(counts: dict, **overrides) -> dict:
    order = ["TREND", "RANGE", "BREAKOUT", "CHOP"]
    receipt = {
        "schema_version": REGIME_TRAIN_SUPPORT_SCHEMA_VERSION,
        "symbol": "EURUSD",
        "timeframe": "H4",
        "surface": "REGIME_LIVE",
        "warmup_policy": "CURRENT_AND_SOURCE_BAR_NOT_WARMUP",
        "label_order": order,
        "eligible_rows": sum(counts.values()),
        "counts": dict(counts),
        "observed_regimes": [lab for lab in order if counts[lab] > 0],
        "unobserved_regimes": [lab for lab in order if counts[lab] == 0],
        "support_state_by_regime": {
            lab: "EMPIRICALLY_OBSERVED" if counts[lab] > 0 else "UNOBSERVED"
            for lab in order
        },
        "first_ts": "2020-01-01T00:00:00+00:00",
        "last_ts": "2021-12-31T00:00:00+00:00",
        "vector_encoding": "fxpm.regime-live-labels-nul/v1",
        "vector_sha256": "e" * 64,
        "bindings": {
            "symbol": "EURUSD",
            "timeframe": "H4",
            "activation_id": _ACTIVATION_ID,
            "activation_manifest_sha256": _ACTIVATION_MANIFEST,
            "source_sha256": "f" * 64,
            "train_window_sha256": "1" * 64,
            "params_sha256": "2" * 64,
            "detector_code_sha256": "3" * 64,
        },
    }
    receipt.update(overrides)
    receipt["receipt_sha256"] = _tuner_canonical_sha256(receipt)
    return receipt


def _check_support(receipt, params_cell=None):
    return _validated_train_regime_support(
        receipt, "EURUSD", "H4", params_cell, _ACTIVATION_ID, _ACTIVATION_MANIFEST
    )


def test_support_receipt_valid_sparse_observed() -> None:
    r = _support_receipt({"TREND": 900, "RANGE": 0, "BREAKOUT": 40, "CHOP": 0})
    out = _check_support(r)
    assert out is not None
    assert out["observed_regimes"] == ["TREND", "BREAKOUT"]
    assert out["unobserved_regimes"] == ["RANGE", "CHOP"]


def test_support_receipt_tampered_hash_rejected() -> None:
    r = _support_receipt({"TREND": 10, "RANGE": 1, "BREAKOUT": 1, "CHOP": 1})
    r["receipt_sha256"] = "0" * 64
    assert _check_support(r) is None


def test_support_receipt_omission_partition_mismatch_rejected() -> None:
    # observed list omitting a nonzero-count regime cannot self-authorize.
    r = _support_receipt({"TREND": 10, "RANGE": 5, "BREAKOUT": 0, "CHOP": 0})
    r["observed_regimes"] = ["TREND"]
    r["receipt_sha256"] = _tuner_canonical_sha256(
        {k: v for k, v in r.items() if k != "receipt_sha256"}
    )
    assert _check_support(r) is None


def test_support_receipt_zero_eligible_rejected() -> None:
    r = _support_receipt({"TREND": 0, "RANGE": 0, "BREAKOUT": 0, "CHOP": 0})
    assert _check_support(r) is None


def test_support_receipt_counts_sum_mismatch_rejected() -> None:
    r = _support_receipt({"TREND": 10, "RANGE": 1, "BREAKOUT": 1, "CHOP": 1})
    r["eligible_rows"] = 99
    r["receipt_sha256"] = _tuner_canonical_sha256(
        {k: v for k, v in r.items() if k != "receipt_sha256"}
    )
    assert _check_support(r) is None


def test_support_receipt_wrong_activation_rejected() -> None:
    r = _support_receipt({"TREND": 5, "RANGE": 5, "BREAKOUT": 5, "CHOP": 5})
    assert (
        _validated_train_regime_support(
            r, "EURUSD", "H4", None, "other-activation", _ACTIVATION_MANIFEST
        )
        is None
    )


def test_support_receipt_wrong_warmup_or_encoding_rejected() -> None:
    r = _support_receipt({"TREND": 5, "RANGE": 5, "BREAKOUT": 5, "CHOP": 5})
    for poison in (
        {"warmup_policy": "SOMETHING_ELSE"},
        {"vector_encoding": "raw"},
        {"first_ts": ""},
    ):
        bad = dict(r)
        bad.update(poison)
        bad["receipt_sha256"] = _tuner_canonical_sha256(
            {k: v for k, v in bad.items() if k != "receipt_sha256"}
        )
        assert _check_support(bad) is None, poison


def test_support_receipt_binding_keyset_and_hex_rejected() -> None:
    r = _support_receipt({"TREND": 5, "RANGE": 5, "BREAKOUT": 5, "CHOP": 5})
    extra = dict(r)
    extra["bindings"] = dict(extra["bindings"], rogue="x")
    extra["receipt_sha256"] = _tuner_canonical_sha256(
        {k: v for k, v in extra.items() if k != "receipt_sha256"}
    )
    assert _check_support(extra) is None
    short = dict(r)
    short["bindings"] = dict(short["bindings"], detector_code_sha256="abc")
    short["receipt_sha256"] = _tuner_canonical_sha256(
        {k: v for k, v in short.items() if k != "receipt_sha256"}
    )
    assert _check_support(short) is None


def test_non_search_kind_vocabulary_and_terminal_coherence(tmp_path) -> None:
    # seq1012/seq1028: kind-bound reasons; terminal/retryable must cohere.
    for poison in (
        {"decision_kind": "MADE_UP_KIND"},
        {"reason_code": "NOT_IN_VOCAB"},
        {"generation_terminal": False},
        {"retryable": True},
        {"fit_scope": "validation"},
        {"window_contract_version": ""},
    ):
        check = _validate(tmp_path, _non_search_record("EURUSD", "H4", **poison))
        assert check["valid"] is False, poison
    retryable = _non_search_record(
        "EURUSD",
        "H4",
        decision_kind="TECHNICAL_EXCLUSION",
        reason_code="REGIME_FIT_RAISED",
        generation_terminal=False,
        retryable=True,
    )
    check = _validate(tmp_path, retryable)
    assert check["valid"] is True and check["status"] == "NON_SEARCH"
    assert check["record"]["generation_terminal"] is False


def test_symbol_data_state_strict_validator() -> None:
    from pm_pipeline import _validated_symbol_data_state

    def _state(**over):
        base = {
            "kind": "ALL_TIMEFRAMES_NON_SEARCH",
            "timeframes": ["D1"],
            "decisions": {
                "D1": {
                    "decision_kind": "INSUFFICIENT_TRAIN_HISTORY",
                    "decision_sha256": "a" * 64,
                    "reason_code": "INSUFFICIENT_OPTIMIZER_TRAIN_ROWS",
                    "generation_terminal": True,
                }
            },
            "generation_terminal": True,
        }
        base.update(over)
        return base

    assert _validated_symbol_data_state(_state()) is not None
    # seq1035 poisons: each must fail closed.
    poisons = [
        _state(kind="OTHER"),
        _state(timeframes=["H4"]),  # keys mismatch
        _state(generation_terminal=False),
        _state(extra=True),
    ]
    bad_decisions = [
        {"decision_kind": "TECHNICAL_EXCLUSION",
         "decision_sha256": "a" * 64,
         "reason_code": "REGIME_FIT_RAISED",
         "generation_terminal": True},  # retryable kind not admissible
        {"decision_kind": "INSUFFICIENT_TRAIN_HISTORY",
         "decision_sha256": "zz",
         "reason_code": "INSUFFICIENT_OPTIMIZER_TRAIN_ROWS",
         "generation_terminal": True},  # bad hash
        {"decision_kind": "INSUFFICIENT_TRAIN_HISTORY",
         "decision_sha256": "a" * 64,
         "reason_code": "NOT_IN_VOCAB",
         "generation_terminal": True},
        {"decision_kind": "INSUFFICIENT_TRAIN_HISTORY",
         "decision_sha256": "a" * 64,
         "reason_code": "INSUFFICIENT_OPTIMIZER_TRAIN_ROWS",
         "generation_terminal": False},
        {"decision_kind": "INSUFFICIENT_TRAIN_HISTORY",
         "decision_sha256": "a" * 64,
         "reason_code": "INSUFFICIENT_OPTIMIZER_TRAIN_ROWS"},  # missing key
    ]
    poisons.extend(_state(decisions={"D1": bad}) for bad in bad_decisions)
    for poison in poisons:
        assert _validated_symbol_data_state(poison) is None, poison
