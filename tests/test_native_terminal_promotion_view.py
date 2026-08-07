from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
from typing import Any

import pytest

import pm_native_activation as native
import pm_production_anchor_plan as anchor_plan
from pm_native_request_contract import (
    PRODUCTION_SYMBOL_ORDER,
    PRODUCTION_TIMEFRAME_ORDER,
    build_v5_manifest_request,
    build_v5_request_contract,
)


UTC = timezone.utc
CUTOFF = datetime(2026, 7, 13, 9, 3, 17, tzinfo=UTC)


def _write_json(path: Path, value: Any) -> str:
    raw = (
        json.dumps(value, indent=2, sort_keys=True, ensure_ascii=False) + "\n"
    ).encode("utf-8")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(raw)
    return hashlib.sha256(raw).hexdigest()


def _fixture(
    tmp_path: Path,
    *,
    promoted: bool,
) -> tuple[Path, native.ActiveNativeUniverse, dict[str, Path], dict[str, Any]]:
    data_dir = tmp_path / "data"
    native_root = data_dir / "native"
    activation_id = "activation-terminal-view"
    run_id = "run-terminal-view"
    common = build_v5_request_contract(
        requested_information_cutoff_utc=CUTOFF,
        warmup_days=500,
    )
    request = build_v5_manifest_request(
        common,
        symbols=PRODUCTION_SYMBOL_ORDER,
        timeframes=PRODUCTION_TIMEFRAME_ORDER,
    )
    resolver_calendar, resolver_errors = native._validated_calendar_contract(request)
    assert not resolver_errors
    run_manifest = {
        "schema_version": native.NATIVE_RUN_MANIFEST_SCHEMA,
        "run_id": run_id,
        "immutable_record": True,
        "complete": True,
        "request": request,
    }
    run_path = native_root / "runs" / run_id / "run_manifest.json"
    run_sha = _write_json(run_path, run_manifest)
    counts = {
        native.NATIVE_EVIDENCE_CANDIDATE: len(PRODUCTION_SYMBOL_ORDER)
        * len(PRODUCTION_TIMEFRAME_ORDER),
        native.NATIVE_SOURCE_UNAVAILABLE: 0,
        native.NATIVE_TECHNICAL_ERROR: 0,
        native.NATIVE_PENDING: 0,
        "total": len(PRODUCTION_SYMBOL_ORDER) * len(PRODUCTION_TIMEFRAME_ORDER),
        "terminal": len(PRODUCTION_SYMBOL_ORDER) * len(PRODUCTION_TIMEFRAME_ORDER),
        "symbols_with_evidence_candidates": len(PRODUCTION_SYMBOL_ORDER),
    }
    source = {
        "schema_version": native.ACTIVATION_MANIFEST_SCHEMA,
        "activation_id": activation_id,
        "run_id": run_id,
        "status": "failed" if promoted else "active",
        "production_authoritative": True,
        "run_manifest_relative_path": f"runs/{run_id}/run_manifest.json",
        "run_manifest_sha256": run_sha,
        "previous_active_pointer_sha256": "a" * 64,
        "counts": counts,
    }
    source_path = native_root / "activations" / f"{activation_id}.json"
    source_sha = _write_json(source_path, source)
    receipt_path = native_root / "transition_receipts" / f"{activation_id}.json"
    if promoted:
        view = {**source, "status": "active"}
        manifest_path = (
            native_root / "activations" / "promoted" / f"{activation_id}.json"
        )
        manifest_sha = _write_json(manifest_path, view)
        receipt = {
            "schema_version": native.TERMINAL_PROMOTION_RECEIPT_SCHEMA,
            "activation_id": activation_id,
            "run_id": run_id,
            "source_manifest_relative_path": f"activations/{activation_id}.json",
            "source_manifest_sha256": source_sha,
            "promoted_manifest_relative_path": (
                f"activations/promoted/{activation_id}.json"
            ),
            "promoted_manifest_sha256": manifest_sha,
            "previous_active_pointer_sha256": "a" * 64,
            "counts": counts,
            "transition": "failed_complete_terminal_grid_to_active_view",
            "data_reacquired": False,
            "cell_evidence_replayed": False,
        }
        _write_json(receipt_path, receipt)
    else:
        manifest_path = source_path
        manifest_sha = source_sha
        receipt = {}
    manifest_relative = manifest_path.relative_to(native_root).as_posix()
    pointer_path = native_root / "ACTIVE.json"
    _write_json(
        pointer_path,
        {
            "schema_version": native.ACTIVE_POINTER_SCHEMA,
            "activation_id": activation_id,
            "manifest_relative_path": manifest_relative,
            "manifest_sha256": manifest_sha,
            "activated_at_utc": "2026-07-19T19:00:00Z",
            "production_authoritative": True,
        },
    )
    cell_statuses = {
        symbol: {
            timeframe: native.NATIVE_EVIDENCE_CANDIDATE
            for timeframe in PRODUCTION_TIMEFRAME_ORDER
        }
        for symbol in PRODUCTION_SYMBOL_ORDER
    }
    universe = native.ActiveNativeUniverse(
        activation_id=activation_id,
        activation_manifest_path=manifest_path.resolve(),
        activation_manifest_sha256=manifest_sha,
        requested_symbols=tuple(PRODUCTION_SYMBOL_ORDER),
        required_timeframes=tuple(PRODUCTION_TIMEFRAME_ORDER),
        eligible_symbols=tuple(PRODUCTION_SYMBOL_ORDER),
        excluded_symbols={},
        cell_statuses=cell_statuses,
        symbols_with_active_cells=tuple(PRODUCTION_SYMBOL_ORDER),
        fully_excluded_symbols=(),
        calendar_contract=resolver_calendar,
        eligibility_schema=native.UNIVERSE_AUTHENTICITY_SCHEMA,
        decision_basis=native.NATIVE_AUTHENTICITY_DECISION_BASIS,
        explicit_contract=True,
        exact_symbol_identity=True,
        errors=[],
    )
    paths = {
        "native_root": native_root,
        "source": source_path,
        "manifest": manifest_path,
        "pointer": pointer_path,
        "receipt": receipt_path,
    }
    payloads = {"source": source, "receipt": receipt}
    return data_dir, universe, paths, payloads


def _artifact_fixture(
    tmp_path: Path,
    *,
    promoted: bool,
) -> tuple[Path, native.ActiveNativeUniverse, dict[str, Path], dict[str, Any]]:
    data_dir, universe, paths, payloads = _fixture(tmp_path, promoted=promoted)
    native_root = paths["native_root"]
    symbol = PRODUCTION_SYMBOL_ORDER[0]
    timeframe = PRODUCTION_TIMEFRAME_ORDER[0]
    bars_path = native_root / "cells" / symbol / timeframe / "bars.csv"
    sidecar_path = native_root / "cells" / symbol / timeframe / "sidecar.json"
    bars_path.parent.mkdir(parents=True, exist_ok=True)
    bars_path.write_bytes(b"timestamp,open,high,low,close\n")
    sidecar_sha = _write_json(sidecar_path, {"cell": f"{symbol}|{timeframe}"})
    bars_sha = hashlib.sha256(bars_path.read_bytes()).hexdigest()
    entry = {
        "status": native.NATIVE_EVIDENCE_CANDIDATE,
        "symbol": symbol,
        "timeframe": timeframe,
        "bars_relative_path": bars_path.relative_to(native_root).as_posix(),
        "bars_sha256": bars_sha,
        "sidecar_relative_path": sidecar_path.relative_to(native_root).as_posix(),
        "sidecar_sha256": sidecar_sha,
    }
    source = json.loads(paths["source"].read_text(encoding="utf-8"))
    source.update(
        {
            "native_is_signal_authority": True,
            "silent_fallback_allowed": False,
            "universe_eligibility": {
                "schema_version": native.UNIVERSE_AUTHENTICITY_SCHEMA,
                "required_timeframes": list(PRODUCTION_TIMEFRAME_ORDER),
                "session_coverage_method": native.NATIVE_SESSION_COVERAGE_METHOD,
            },
            "entries": {symbol: {timeframe: entry}},
        }
    )
    source_sha = _write_json(paths["source"], source)
    if promoted:
        view = {**source, "status": "active"}
        manifest_sha = _write_json(paths["manifest"], view)
        receipt = json.loads(paths["receipt"].read_text(encoding="utf-8"))
        receipt["source_manifest_sha256"] = source_sha
        receipt["promoted_manifest_sha256"] = manifest_sha
        _write_json(paths["receipt"], receipt)
        payloads["receipt"] = receipt
    else:
        manifest_sha = source_sha
    pointer = json.loads(paths["pointer"].read_text(encoding="utf-8"))
    pointer["manifest_sha256"] = manifest_sha
    _write_json(paths["pointer"], pointer)
    universe.activation_manifest_path = paths["manifest"].resolve()
    universe.activation_manifest_sha256 = manifest_sha
    universe.entries = {symbol: {timeframe: entry}}
    payloads["source"] = source
    paths.update({"bars": bars_path, "sidecar": sidecar_path})
    return data_dir, universe, paths, payloads


def _stub_runtime_cell_evidence(
    monkeypatch: pytest.MonkeyPatch,
    universe: native.ActiveNativeUniverse,
) -> None:
    monkeypatch.setattr(
        native,
        "resolve_active_native_universe",
        lambda *args, **kwargs: deepcopy(universe),
    )
    monkeypatch.setattr(
        native,
        "_verify_activation_cell_evidence",
        lambda *args, **kwargs: [],
    )


@pytest.mark.parametrize("promoted", [False, True])
def test_per_cell_resolver_accepts_canonical_and_authenticated_promoted_manifest(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    promoted: bool,
) -> None:
    data_dir, universe, paths, _payloads = _artifact_fixture(
        tmp_path,
        promoted=promoted,
    )
    _stub_runtime_cell_evidence(monkeypatch, universe)

    result = native.resolve_active_native_artifact(
        data_dir,
        PRODUCTION_SYMBOL_ORDER[0],
        PRODUCTION_TIMEFRAME_ORDER[0],
        production_contract_required=True,
    )

    assert result.valid, result.errors
    assert result.activation_manifest_path == paths["manifest"].resolve()
    assert result.activation_manifest_sha256 == universe.activation_manifest_sha256
    assert result.data_path == paths["bars"].resolve()
    assert result.sidecar_path == paths["sidecar"].resolve()


def test_per_cell_resolver_rejects_promoted_manifest_with_tampered_receipt(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    data_dir, universe, paths, payloads = _artifact_fixture(
        tmp_path,
        promoted=True,
    )
    _stub_runtime_cell_evidence(monkeypatch, universe)
    receipt = deepcopy(payloads["receipt"])
    receipt["counts"] = {**receipt["counts"], "total": 599}
    _write_json(paths["receipt"], receipt)

    result = native.resolve_active_native_artifact(
        data_dir,
        PRODUCTION_SYMBOL_ORDER[0],
        PRODUCTION_TIMEFRAME_ORDER[0],
        production_contract_required=True,
    )

    assert not result.valid
    assert "NATIVE_TERMINAL_PROMOTION_COUNTS_MISMATCH" in result.errors


def test_anchor_loader_retains_canonical_active_manifest_support(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    data_dir, universe, _paths, _payloads = _fixture(tmp_path, promoted=False)

    def _unexpected(*args: Any, **kwargs: Any) -> list[str]:
        raise AssertionError("canonical activation must not invoke promotion verifier")

    monkeypatch.setattr(anchor_plan, "validate_terminal_promotion_view", _unexpected)

    _common, _run_sha, activation_id, activation_sha = (
        anchor_plan._load_verified_v5_common(data_dir, universe)
    )

    assert activation_id == universe.activation_id
    assert activation_sha == universe.activation_manifest_sha256


def test_anchor_loader_accepts_exact_receipt_validated_promoted_view(
    tmp_path: Path,
) -> None:
    data_dir, universe, paths, _payloads = _fixture(tmp_path, promoted=True)

    common, _run_sha, activation_id, activation_sha = (
        anchor_plan._load_verified_v5_common(data_dir, universe)
    )

    assert common["common_as_of_utc"] == "2026-07-13T09:00:00Z"
    assert activation_id == universe.activation_id
    assert activation_sha == universe.activation_manifest_sha256
    assert paths["manifest"].parent.name == "promoted"


def test_anchor_loader_rejects_alternate_promoted_view_path(tmp_path: Path) -> None:
    data_dir, universe, paths, _payloads = _fixture(tmp_path, promoted=True)
    native_root = paths["native_root"]
    alternate = native_root / "activations" / "promoted-copy" / paths["manifest"].name
    alternate_sha = _write_json(
        alternate,
        json.loads(paths["manifest"].read_text(encoding="utf-8")),
    )
    pointer = json.loads(paths["pointer"].read_text(encoding="utf-8"))
    pointer["manifest_relative_path"] = alternate.relative_to(native_root).as_posix()
    pointer["manifest_sha256"] = alternate_sha
    _write_json(paths["pointer"], pointer)
    universe.activation_manifest_path = alternate.resolve()
    universe.activation_manifest_sha256 = alternate_sha

    with pytest.raises(anchor_plan.ProductionAnchorPlanError) as caught:
        anchor_plan._load_verified_v5_common(data_dir, universe)

    assert caught.value.code == "ACTIVE_POINTER_AUTHORITY_MISMATCH"


def test_anchor_loader_rejects_noncanonical_promoted_path_separator(
    tmp_path: Path,
) -> None:
    data_dir, universe, paths, _payloads = _fixture(tmp_path, promoted=True)
    pointer = json.loads(paths["pointer"].read_text(encoding="utf-8"))
    pointer["manifest_relative_path"] = pointer["manifest_relative_path"].replace(
        "/", "\\"
    )
    _write_json(paths["pointer"], pointer)

    with pytest.raises(anchor_plan.ProductionAnchorPlanError) as caught:
        anchor_plan._load_verified_v5_common(data_dir, universe)

    assert caught.value.code == "ACTIVE_POINTER_AUTHORITY_MISMATCH"


def test_anchor_loader_rejects_non_string_promoted_path_fail_closed(
    tmp_path: Path,
) -> None:
    data_dir, universe, paths, _payloads = _fixture(tmp_path, promoted=True)
    pointer = json.loads(paths["pointer"].read_text(encoding="utf-8"))
    pointer["manifest_relative_path"] = ["activations", "promoted"]
    _write_json(paths["pointer"], pointer)

    with pytest.raises(anchor_plan.ProductionAnchorPlanError) as caught:
        anchor_plan._load_verified_v5_common(data_dir, universe)

    assert caught.value.code == "ACTIVE_POINTER_AUTHORITY_MISMATCH"


def test_anchor_loader_rejects_tampered_promotion_receipt(tmp_path: Path) -> None:
    data_dir, universe, paths, payloads = _fixture(tmp_path, promoted=True)
    receipt = deepcopy(payloads["receipt"])
    receipt["counts"] = {**receipt["counts"], "total": 599}
    _write_json(paths["receipt"], receipt)

    with pytest.raises(anchor_plan.ProductionAnchorPlanError) as caught:
        anchor_plan._load_verified_v5_common(data_dir, universe)

    assert caught.value.code == "TERMINAL_PROMOTION_VIEW_INVALID"
    assert caught.value.details


def test_anchor_loader_rejects_receipt_predecessor_mismatch(tmp_path: Path) -> None:
    data_dir, universe, paths, payloads = _fixture(tmp_path, promoted=True)
    receipt = deepcopy(payloads["receipt"])
    receipt["previous_active_pointer_sha256"] = "b" * 64
    _write_json(paths["receipt"], receipt)

    with pytest.raises(anchor_plan.ProductionAnchorPlanError) as caught:
        anchor_plan._load_verified_v5_common(data_dir, universe)

    assert caught.value.code == "TERMINAL_PROMOTION_VIEW_INVALID"
    assert "NATIVE_TERMINAL_PROMOTION_PREDECESSOR_POINTER_MISMATCH" in (
        caught.value.details
    )


def test_anchor_loader_rejects_promoted_source_status_not_failed(
    tmp_path: Path,
) -> None:
    data_dir, universe, paths, payloads = _fixture(tmp_path, promoted=True)
    source = deepcopy(payloads["source"])
    source["status"] = "active"
    source_sha = _write_json(paths["source"], source)
    receipt = deepcopy(payloads["receipt"])
    receipt["source_manifest_sha256"] = source_sha
    _write_json(paths["receipt"], receipt)

    with pytest.raises(anchor_plan.ProductionAnchorPlanError) as caught:
        anchor_plan._load_verified_v5_common(data_dir, universe)

    assert caught.value.code == "TERMINAL_PROMOTION_VIEW_INVALID"
    assert "NATIVE_TERMINAL_PROMOTION_SOURCE_STATUS_INVALID" in caught.value.details


def test_anchor_loader_rejects_rehashed_source_authority_mutation(
    tmp_path: Path,
) -> None:
    data_dir, universe, paths, payloads = _fixture(tmp_path, promoted=True)
    source = deepcopy(payloads["source"])
    source["production_authoritative"] = False
    source_sha = _write_json(paths["source"], source)
    view = {**source, "status": "active"}
    view_sha = _write_json(paths["manifest"], view)
    receipt = deepcopy(payloads["receipt"])
    receipt["source_manifest_sha256"] = source_sha
    receipt["promoted_manifest_sha256"] = view_sha
    _write_json(paths["receipt"], receipt)
    pointer = json.loads(paths["pointer"].read_text(encoding="utf-8"))
    pointer["manifest_sha256"] = view_sha
    _write_json(paths["pointer"], pointer)
    universe.activation_manifest_sha256 = view_sha

    with pytest.raises(anchor_plan.ProductionAnchorPlanError) as caught:
        anchor_plan._load_verified_v5_common(data_dir, universe)

    assert caught.value.code == "ACTIVATION_MANIFEST_AUTHORITY_INVALID"


def test_anchor_loader_rejects_rehashed_non_status_view_mutation(tmp_path: Path) -> None:
    data_dir, universe, paths, payloads = _fixture(tmp_path, promoted=True)
    view = json.loads(paths["manifest"].read_text(encoding="utf-8"))
    view["unexpected_authority_claim"] = True
    view_sha = _write_json(paths["manifest"], view)
    receipt = deepcopy(payloads["receipt"])
    receipt["promoted_manifest_sha256"] = view_sha
    _write_json(paths["receipt"], receipt)
    pointer = json.loads(paths["pointer"].read_text(encoding="utf-8"))
    pointer["manifest_sha256"] = view_sha
    _write_json(paths["pointer"], pointer)
    universe.activation_manifest_sha256 = view_sha

    with pytest.raises(anchor_plan.ProductionAnchorPlanError) as caught:
        anchor_plan._load_verified_v5_common(data_dir, universe)

    assert caught.value.code == "TERMINAL_PROMOTION_VIEW_INVALID"
    assert caught.value.details
