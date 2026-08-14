import json
from copy import deepcopy
from dataclasses import replace
from pathlib import Path

import numpy as np
import pandas as pd
import pm_native_activation as native_activation
import pytest
from pm_native_activation import (
    ACTIVE_POINTER_SCHEMA,
    AUDITED_SIDECAR_CONTRACT,
    NATIVE_BARS_SCHEMA,
    NATIVE_EVIDENCE_CANDIDATE,
    NATIVE_TECHNICAL_ERROR,
    _v5_authenticated_reference_resolution_audit,
    _v5_select_compatible_edge_receipt,
    classify_native_availability_cell,
    resolve_active_native_universe,
    sha256_file,
)
from pm_native_consensus import (
    CONSENSUS_SCHEMA_VERSION,
    EVIDENCE_PASS,
    EVIDENCE_SEMANTIC_FAIL,
    INTEGRITY_PASS,
    REQUIRED_AUDIT_WINDOWS,
    canonical_sha256,
)
from pm_native_resolution import RESOLUTION_AUDIT_SCHEMA_VERSION
from pm_native_parity import (
    CROSS_RESOLUTION_COMPATIBILITY_CURRENT,
    CROSS_RESOLUTION_COMPATIBILITY_FROZEN_PRE_SEQ2307,
    ParityThresholds,
    audit_native_cross_resolution,
)
from tests.native_v5_support import SYMBOL, activate_v5_run, write_v5_run
import tools.audit_activate_native_history as activation_tool


def _edge(evidence_state: str, *, pair_report_sha256: str) -> dict:
    return {
        "status": "PASS" if evidence_state == EVIDENCE_PASS else "FAIL",
        "pair_report_sha256": pair_report_sha256,
        "windows": {
            window: {
                "integrity_state": INTEGRITY_PASS,
                "evidence_state": evidence_state,
                "reason_codes": [],
            }
            for window in REQUIRED_AUDIT_WINDOWS
        },
    }


def _m5_history(*, origin: pd.Timestamp, rows: int) -> pd.DataFrame:
    index = pd.date_range(origin, periods=rows, freq="5min")
    close = 100.0 + np.arange(rows, dtype=float) * 0.0001
    return pd.DataFrame(
        {
            "Open": close,
            "High": close + 0.0002,
            "Low": close - 0.0002,
            "Close": close + 0.00005,
            "Volume": 1,
        },
        index=index,
    )


def _resample_m15(frame: pd.DataFrame) -> pd.DataFrame:
    return (
        frame.resample("15min")
        .agg(
            {
                "Open": "first",
                "High": "max",
                "Low": "min",
                "Close": "last",
                "Volume": "sum",
            }
        )
        .dropna()
    )


def _canonical_file_bytes(value: dict) -> bytes:
    return (
        json.dumps(value, indent=2, sort_keys=True, ensure_ascii=False) + "\n"
    ).encode("utf-8")


def _publish_subset_active_pointer(native_root: Path, activated: dict) -> None:
    activation_id = activated["activation_id"]
    (native_root / "ACTIVE.json").write_bytes(
        _canonical_file_bytes(
            {
                "schema_version": ACTIVE_POINTER_SCHEMA,
                "activation_id": activation_id,
                "manifest_relative_path": f"activations/{activation_id}.json",
                "manifest_sha256": activated["manifest_sha256"],
                "activated_at_utc": activated["created_at_utc"],
                "production_authoritative": False,
            }
        )
    )


def _repin_parity_window_digest(
    native_root: Path,
    *,
    activation_id: str,
    target_timeframe: str,
    reference_timeframe: str,
    replacement: str | None,
) -> None:
    manifest_path = native_root / "activations" / f"{activation_id}.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    entry = manifest["entries"][SYMBOL][target_timeframe]
    parity_path = native_root / entry["parity_relative_path"]
    parity = json.loads(parity_path.read_text(encoding="utf-8"))
    edge = parity["edges"][reference_timeframe]
    edge["report"]["windows"]["optimizer_train"][
        "pair_report_sha256"
    ] = replacement
    edge["report_sha256"] = canonical_sha256(edge["report"])
    parity["consensus_sha256"] = canonical_sha256(
        {
            key: value
            for key, value in parity.items()
            if key != "consensus_sha256"
        }
    )
    parity_path.write_bytes(_canonical_file_bytes(parity))
    entry["parity_sha256"] = sha256_file(parity_path)
    manifest_path.write_bytes(_canonical_file_bytes(manifest))

    pointer_path = native_root / "ACTIVE.json"
    pointer = json.loads(pointer_path.read_text(encoding="utf-8"))
    pointer["manifest_sha256"] = sha256_file(manifest_path)
    pointer_path.write_bytes(_canonical_file_bytes(pointer))


def test_reference_resolution_audit_accepts_consensus_canonical_symbol_case(
    tmp_path: Path,
):
    activation_id = "activation-mixed-case-reference"
    run_id = "run-mixed-case-reference"
    symbol = "Platinum"
    canonical_symbol = symbol.upper()
    timeframe = "M15"
    run_manifest_relative = f"runs/{run_id}/manifest.json"
    run_manifest_sha = "1" * 64
    bars_sha = "2" * 64
    provenance_sha = "3" * 64
    reason = "INTRADAY_RESOLUTION_DAILY_LIKE_YEARS:2019"
    resolution = {
        "schema_version": RESOLUTION_AUDIT_SCHEMA_VERSION,
        "symbol": canonical_symbol,
        "timeframe": timeframe,
        "status": "FAIL",
        "reason_codes": [reason],
        "rows": 10,
        "active_sessions": 2,
        "median_bars_per_active_session": 1.0,
        "nominal_or_faster_spacing_ratio": 0.0,
        "daily_identity_overlap_rows": 2,
        "daily_timestamp_identity_ratio": 1.0,
        "daily_ohlc_identity_ratio": 1.0,
        "yearly_diagnostics": {"2019": {"rows": 10}},
    }
    cell_reasons = [f"RESOLUTION:{reason}"]
    source_binding = {
        "availability_state": "AVAILABLE",
        "bars_sha256": bars_sha,
        "provenance_sha256": provenance_sha,
    }
    sidecar = {
        "schema_version": NATIVE_BARS_SCHEMA,
        "activation_id": activation_id,
        "run_id": run_id,
        "symbol": symbol,
        "timeframe": timeframe,
        "run_manifest_relative_path": run_manifest_relative,
        "run_manifest_sha256": run_manifest_sha,
        "audited_sidecar_contract": AUDITED_SIDECAR_CONTRACT,
        "sha256": bars_sha,
        "source_provenance_relative_path": (
            f"runs/{run_id}/{symbol}/{timeframe}/provenance.json"
        ),
        "source_provenance_sha256": provenance_sha,
        "resolution_audit": resolution,
        "activation_reason_codes": cell_reasons,
    }
    parity = {
        "schema_version": CONSENSUS_SCHEMA_VERSION,
        "activation_id": activation_id,
        "run_id": run_id,
        "symbol": canonical_symbol,
        "timeframe": timeframe,
        "run_manifest_relative_path": run_manifest_relative,
        "run_manifest_sha256": run_manifest_sha,
        "source_bars_sha256": bars_sha,
        "source_provenance_sha256": provenance_sha,
        "source_binding": source_binding,
        "cell_reason_codes": cell_reasons,
    }
    parity["consensus_sha256"] = canonical_sha256(parity)

    sidecar_relative = f"audits/{activation_id}/{symbol}/{timeframe}/bars.meta.json"
    parity_relative = f"audits/{activation_id}/{symbol}/{timeframe}/parity.json"
    sidecar_path = tmp_path / sidecar_relative
    parity_path = tmp_path / parity_relative
    sidecar_path.parent.mkdir(parents=True)
    sidecar_path.write_bytes(_canonical_file_bytes(sidecar))
    parity_path.write_bytes(_canonical_file_bytes(parity))
    reference_entry = {
        "symbol": symbol,
        "timeframe": timeframe,
        "run_id": run_id,
        "bars_relative_path": f"runs/{run_id}/{symbol}/{timeframe}/bars.csv",
        "bars_sha256": bars_sha,
        "run_manifest_sha256": run_manifest_sha,
        "sidecar_relative_path": sidecar_relative,
        "sidecar_sha256": sha256_file(sidecar_path),
        "parity_relative_path": parity_relative,
        "parity_sha256": sha256_file(parity_path),
        "resolution_status": "FAIL",
        "resolution_reason_codes": [reason],
    }

    observed, errors = _v5_authenticated_reference_resolution_audit(
        tmp_path,
        activation_id=activation_id,
        run_id=run_id,
        symbol=symbol,
        timeframe=timeframe,
        reference_entry=reference_entry,
        run_manifest_relative_path=run_manifest_relative,
        run_manifest_sha256=run_manifest_sha,
        bars_sha256=bars_sha,
        provenance_sha256=provenance_sha,
    )

    assert errors == []
    assert observed == resolution

    strict_identity = deepcopy(reference_entry)
    strict_identity["run_id"] = run_id.upper()
    _observed, strict_errors = _v5_authenticated_reference_resolution_audit(
        tmp_path,
        activation_id=activation_id,
        run_id=run_id,
        symbol=symbol,
        timeframe=timeframe,
        reference_entry=strict_identity,
        run_manifest_relative_path=run_manifest_relative,
        run_manifest_sha256=run_manifest_sha,
        bars_sha256=bars_sha,
        provenance_sha256=provenance_sha,
    )
    assert strict_errors == [
        "NATIVE_V5_REFERENCE_AUDIT:Platinum|M15:ENTRY_RUN_ID_MISMATCH"
    ]


def test_reference_resolution_exclusion_is_rederived_for_candidate_and_technical(
    tmp_path: Path,
    monkeypatch,
):
    data_dir = tmp_path / "data"
    native_root = data_dir / "native"
    native_root.mkdir(parents=True)
    run_id = "run-reference-resolution-exclusions"
    activation_id = "activation-reference-resolution-exclusions"
    write_v5_run(native_root, run_id)

    original_resolution_audit = activation_tool.audit_native_resolution
    failed_timeframes = {"M5", "M15", "M30", "H1", "H4"}

    def _forced_resolution_failures(*args, **kwargs):
        result = original_resolution_audit(*args, **kwargs)
        timeframe = str(kwargs.get("timeframe") or "")
        if timeframe not in failed_timeframes:
            return result
        return replace(
            result,
            status="FAIL",
            reason_codes=(
                f"INTRADAY_RESOLUTION_DAILY_LIKE_YEARS:{timeframe}:2018",
            ),
        )

    monkeypatch.setattr(
        activation_tool,
        "audit_native_resolution",
        _forced_resolution_failures,
    )
    monkeypatch.setattr(
        native_activation,
        "audit_native_resolution",
        _forced_resolution_failures,
    )
    activated = activate_v5_run(
        native_root,
        run_id,
        activation_id,
        production_only_pointer=True,
    )

    assert activated["status"] == "active", activated
    assert activated["active_pointer_updated"] is False
    _publish_subset_active_pointer(native_root, activated)
    assert activated["entries"][SYMBOL]["D1"]["status"] == (
        NATIVE_EVIDENCE_CANDIDATE
    )
    assert activated["entries"][SYMBOL]["M5"]["status"] == NATIVE_TECHNICAL_ERROR
    for target, references in (("D1", ("H4", "H1")), ("M5", ("M15", "M30"))):
        target_entry = activated["entries"][SYMBOL][target]
        parity = json.loads(
            (native_root / target_entry["parity_relative_path"]).read_text(
                encoding="utf-8"
            )
        )
        for reference in references:
            report = parity["edges"][reference]["report"]
            for window in REQUIRED_AUDIT_WINDOWS:
                assert report["windows"][window]["pair_report_sha256"] is None
                assert (
                    f"AUDIT_REFERENCE_RESOLUTION_EXCLUDED:{reference}"
                    in report["windows"][window]["reason_codes"]
                )

    verified = resolve_active_native_universe(
        data_dir,
        [SYMBOL],
        verify_exclusion_evidence=True,
        production_contract_required=False,
    )
    assert verified.valid, verified.errors

    def _selected_references_are_not_resolution_failures(*args, **kwargs):
        if str(kwargs.get("timeframe") or "") in {"H1", "M15"}:
            return original_resolution_audit(*args, **kwargs)
        return _forced_resolution_failures(*args, **kwargs)

    monkeypatch.setattr(
        native_activation,
        "audit_native_resolution",
        _selected_references_are_not_resolution_failures,
    )
    native_activation.clear_unscoped_native_universe_memo()
    rejected = resolve_active_native_universe(
        data_dir,
        [SYMBOL],
        verify_exclusion_evidence=True,
        production_contract_required=False,
    )

    assert not rejected.valid
    assert (
        "NATIVE_V5_REFERENCE_RESOLUTION_RECOMPUTATION_MISMATCH:EURUSD|D1:H1"
        in rejected.errors
    )
    assert (
        "NATIVE_V5_REFERENCE_RESOLUTION_RECOMPUTATION_MISMATCH:EURUSD|M5:M15"
        in rejected.errors
    )


@pytest.mark.parametrize(
    ("target_timeframe", "reference_timeframe", "expected_status"),
    [
        pytest.param("D1", "H4", NATIVE_EVIDENCE_CANDIDATE, id="candidate"),
        pytest.param("M5", "M15", NATIVE_TECHNICAL_ERROR, id="technical"),
    ],
)
@pytest.mark.parametrize(
    "replacement",
    [
        pytest.param(None, id="unexplained-null"),
        pytest.param("0" * 64, id="present-mismatch"),
    ],
)
def test_reference_pair_receipt_tamper_is_rejected_for_every_cell_status(
    tmp_path: Path,
    monkeypatch,
    target_timeframe: str,
    reference_timeframe: str,
    expected_status: str,
    replacement: str | None,
):
    data_dir = tmp_path / "data"
    native_root = data_dir / "native"
    native_root.mkdir(parents=True)
    run_id = "run-reference-pair-tamper"
    activation_id = "activation-reference-pair-tamper"
    write_v5_run(native_root, run_id)

    original_resolution_audit = activation_tool.audit_native_resolution

    def _m5_resolution_failure(*args, **kwargs):
        result = original_resolution_audit(*args, **kwargs)
        if str(kwargs.get("timeframe") or "") != "M5":
            return result
        return replace(
            result,
            status="FAIL",
            reason_codes=("INTRADAY_RESOLUTION_DAILY_LIKE_YEARS:2018",),
        )

    monkeypatch.setattr(
        activation_tool,
        "audit_native_resolution",
        _m5_resolution_failure,
    )
    monkeypatch.setattr(
        native_activation,
        "audit_native_resolution",
        _m5_resolution_failure,
    )
    activated = activate_v5_run(
        native_root,
        run_id,
        activation_id,
        production_only_pointer=True,
    )

    assert activated["status"] == "active", activated
    assert activated["entries"][SYMBOL][target_timeframe]["status"] == (
        expected_status
    )
    _publish_subset_active_pointer(native_root, activated)
    _repin_parity_window_digest(
        native_root,
        activation_id=activation_id,
        target_timeframe=target_timeframe,
        reference_timeframe=reference_timeframe,
        replacement=replacement,
    )

    rejected = resolve_active_native_universe(
        data_dir,
        [SYMBOL],
        verify_exclusion_evidence=True,
        production_contract_required=False,
    )

    assert not rejected.valid
    assert (
        "NATIVE_V5_RAW_PAIR_RECOMPUTATION_MISMATCH:"
        f"{SYMBOL}|{target_timeframe}:{reference_timeframe}"
        in rejected.errors
    )


def test_current_receipt_uses_current_profile_even_if_frozen_candidate_is_supplied():
    current = _edge(EVIDENCE_PASS, pair_report_sha256="1" * 64)
    frozen = deepcopy(current)
    stored = deepcopy(current)

    selected, profile = _v5_select_compatible_edge_receipt(
        stored,
        current,
        frozen_pre_seq2307_edge=frozen,
    )

    assert selected is current
    assert profile == CROSS_RESOLUTION_COMPATIBILITY_CURRENT


def test_exact_frozen_receipt_is_accepted_only_when_conservative():
    current = _edge(EVIDENCE_PASS, pair_report_sha256="2" * 64)
    frozen = _edge(EVIDENCE_SEMANTIC_FAIL, pair_report_sha256="3" * 64)
    stored = deepcopy(frozen)
    before = deepcopy(stored)

    selected, profile = _v5_select_compatible_edge_receipt(
        stored,
        current,
        frozen_pre_seq2307_edge=frozen,
    )

    assert selected is frozen
    assert profile == CROSS_RESOLUTION_COMPATIBILITY_FROZEN_PRE_SEQ2307
    assert stored == before


def test_one_byte_equivalent_receipt_change_is_rejected():
    current = _edge(EVIDENCE_PASS, pair_report_sha256="4" * 64)
    frozen = _edge(EVIDENCE_SEMANTIC_FAIL, pair_report_sha256="5" * 64)
    stored = deepcopy(frozen)
    stored["pair_report_sha256"] = "5" * 63 + "6"

    selected, profile = _v5_select_compatible_edge_receipt(
        stored,
        current,
        frozen_pre_seq2307_edge=frozen,
    )

    assert selected is None
    assert profile is None


def test_legacy_pass_is_rejected_when_current_semantics_fail():
    current = _edge(EVIDENCE_SEMANTIC_FAIL, pair_report_sha256="7" * 64)
    frozen = _edge(EVIDENCE_PASS, pair_report_sha256="8" * 64)

    selected, profile = _v5_select_compatible_edge_receipt(
        deepcopy(frozen),
        current,
        frozen_pre_seq2307_edge=frozen,
    )

    assert selected is None
    assert profile is None


def test_frozen_profile_reproduces_pre_seq2307_trailing_row_treatment():
    origin = pd.Timestamp("2025-01-01T00:00:00Z")
    fine = _m5_history(origin=origin, rows=289)
    coarse = _resample_m15(fine.iloc[:-1])
    cutoff = origin + pd.Timedelta(days=1, minutes=5)
    arguments = {
        "symbol": "EURUSD",
        "fine_timeframe": "M5",
        "coarse_timeframe": "M15",
        "activation_target_timeframe": "M5",
        "tick_tolerance": 0.0001,
        "partition_windows": {"final_lockbox": (origin, cutoff)},
        "thresholds": ParityThresholds(min_compared_native_bars=1),
    }

    current = audit_native_cross_resolution(fine, coarse, **arguments)
    frozen = audit_native_cross_resolution(
        fine,
        coarse,
        **arguments,
        compatibility_profile=CROSS_RESOLUTION_COMPATIBILITY_FROZEN_PRE_SEQ2307,
    )

    current_final = current["partitions"]["final_lockbox"]
    frozen_final = frozen["partitions"]["final_lockbox"]
    assert current["status"] == "PASS"
    assert current_final["fine_rows"] == 288
    assert current_final["trailing_incomplete_coarse_fine_rows_deferred"] == 1
    assert frozen["status"] == "FAIL"
    assert frozen_final["fine_rows"] == 289
    assert frozen_final["assigned_fine_rows"] == 288
    assert frozen_final["unassigned_fine_rows"] == 1
    assert "UNASSIGNED_FINE_ROWS:final_lockbox:1" in frozen_final["reason_codes"]
    assert "trailing_incomplete_coarse_fine_rows_deferred" not in frozen_final
    assert "trailing_incomplete_coarse_interval" not in frozen_final
    assert "trailing_incomplete_coarse_policy" not in frozen["diagnostics"]


def test_terminal_technical_compatibility_does_not_reclassify_as_unavailable():
    classification, error = classify_native_availability_cell(
        "failed",
        "technical_history_gap",
        0,
        "terminal_technical",
    )

    assert classification == "terminal_technical"
    assert error is None
