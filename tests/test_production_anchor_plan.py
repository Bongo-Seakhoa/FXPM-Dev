from __future__ import annotations

from copy import deepcopy
from dataclasses import replace
from datetime import datetime, timedelta, timezone
import hashlib
import json
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Callable

import pytest

import pm_causal_process as causal
import pm_native_activation as native
import pm_production_capital_bootstrap as capital_bootstrap
import pm_production_anchor_plan as anchor_plan
from pm_cap_calibration import CapCalibrationConstraints
from pm_native_activation import ActiveNativeUniverse
from pm_native_request_contract import (
    PRODUCTION_SYMBOL_ORDER,
    PRODUCTION_SYMBOL_ORDER_SHA256,
    PRODUCTION_TIMEFRAME_ORDER,
    PRODUCTION_TIMEFRAME_ORDER_SHA256,
    build_v5_manifest_request,
    build_v5_request_contract,
    canonical_sha256,
    timeframe_request_hash_payload,
)


UTC = timezone.utc
CUTOFF = datetime(2026, 7, 13, 9, 3, 17, tzinfo=UTC)


def _write_json(path: Path, value: Any) -> str:
    raw = json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    ).encode("ascii")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(raw)
    return hashlib.sha256(raw).hexdigest()


def _fixture(
    tmp_path: Path,
    *,
    common: dict[str, Any] | None = None,
    mutate_run_request: Callable[[dict[str, Any]], None] | None = None,
) -> tuple[Path, ActiveNativeUniverse, dict[str, Any]]:
    data_dir = tmp_path / "data"
    native_root = data_dir / "native"
    activation_id = "activation-anchor-plan"
    run_id = "run-anchor-plan"
    common = common or build_v5_request_contract(
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
    if mutate_run_request is not None:
        mutate_run_request(request)
    run_manifest = {
        "schema_version": native.NATIVE_RUN_MANIFEST_SCHEMA,
        "run_id": run_id,
        "immutable_record": True,
        "complete": True,
        "request": request,
    }
    run_path = native_root / "runs" / run_id / "run_manifest.json"
    run_sha = _write_json(run_path, run_manifest)
    activation = {
        "schema_version": native.ACTIVATION_MANIFEST_SCHEMA,
        "activation_id": activation_id,
        "run_id": run_id,
        "status": "active",
        "production_authoritative": True,
        "run_manifest_relative_path": f"runs/{run_id}/run_manifest.json",
        "run_manifest_sha256": run_sha,
    }
    activation_path = native_root / "activations" / f"{activation_id}.json"
    activation_sha = _write_json(activation_path, activation)
    _write_json(
        native_root / "ACTIVE.json",
        {
            "schema_version": native.ACTIVE_POINTER_SCHEMA,
            "activation_id": activation_id,
            "manifest_relative_path": f"activations/{activation_id}.json",
            "manifest_sha256": activation_sha,
            "activated_at_utc": "2026-07-15T00:00:00Z",
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
    universe = ActiveNativeUniverse(
        activation_id=activation_id,
        activation_manifest_path=activation_path.resolve(),
        activation_manifest_sha256=activation_sha,
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
    return data_dir, universe, common


def _install_resolver(
    monkeypatch: pytest.MonkeyPatch, universe: Any
) -> list[tuple[tuple[Any, ...], dict[str, Any]]]:
    calls: list[tuple[tuple[Any, ...], dict[str, Any]]] = []

    def _resolve(*args: Any, **kwargs: Any) -> Any:
        calls.append((args, kwargs))
        return universe

    monkeypatch.setattr(anchor_plan, "resolve_active_native_universe", _resolve)
    return calls


def test_universe_keeps_valid_sibling_when_one_cell_is_terminal_technical(
    tmp_path: Path,
) -> None:
    _data_dir, universe, _common = _fixture(tmp_path)
    universe.cell_statuses["AUDUSD"]["M5"] = native.NATIVE_TECHNICAL_ERROR

    assert anchor_plan._validate_universe(universe) is universe


def test_universe_excludes_only_a_technical_only_symbol_from_candidates(
    tmp_path: Path,
) -> None:
    _data_dir, universe, _common = _fixture(tmp_path)
    for timeframe in PRODUCTION_TIMEFRAME_ORDER:
        universe.cell_statuses["AUDUSD"][timeframe] = native.NATIVE_TECHNICAL_ERROR
    expected = tuple(symbol for symbol in PRODUCTION_SYMBOL_ORDER if symbol != "AUDUSD")
    universe.eligible_symbols = expected
    universe.symbols_with_active_cells = expected

    assert anchor_plan._validate_universe(universe) is universe


def test_factory_resolves_exact_production_universe_and_derives_causal_plan(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    data_dir, universe, common = _fixture(tmp_path)
    calls = _install_resolver(monkeypatch, universe)

    receipt = anchor_plan.derive_production_anchor_plan(data_dir)
    reopened = anchor_plan.ProductionAnchorPlanReceipt(
        _body_json=receipt.canonical_body_bytes,
        _receipt_sha256=receipt.receipt_sha256,
    )
    verified = anchor_plan.verify_production_anchor_plan_receipt(data_dir, reopened)

    assert calls == [
        (
            (data_dir, tuple(PRODUCTION_SYMBOL_ORDER)),
            {
                "required_timeframes": tuple(PRODUCTION_TIMEFRAME_ORDER),
                "verify_exclusion_evidence": True,
                "production_contract_required": True,
            },
        ),
        (
            (data_dir, tuple(PRODUCTION_SYMBOL_ORDER)),
            {
                "required_timeframes": tuple(PRODUCTION_TIMEFRAME_ORDER),
                "verify_exclusion_evidence": True,
                "production_contract_required": True,
            },
        ),
    ]
    assert verified["activation_id"] == universe.activation_id
    assert verified["activation_manifest_sha256"] == universe.activation_manifest_sha256
    assert verified["symbol_roster"] == list(PRODUCTION_SYMBOL_ORDER)
    assert verified["symbol_roster_sha256"] == PRODUCTION_SYMBOL_ORDER_SHA256
    assert verified["timeframe_roster"] == list(PRODUCTION_TIMEFRAME_ORDER)
    assert verified["timeframe_roster_sha256"] == PRODUCTION_TIMEFRAME_ORDER_SHA256
    assert verified["current_refit_cutoff_utc"] == "2026-07-13T00:00:00Z"
    assert verified["latest_fully_closed_common_date"] == "2026-07-12"
    assert verified["latest_historical_outer_end_date"] == "2025-07-12"
    assert verified["anchor_count"] == 4
    assert verified["minimum_purge_calendar_days"] == 35
    assert verified["forward_oos_calendar_days_per_anchor"] == 365
    assert verified["inter_anchor_oos_gap_calendar_days"] == 36
    assert verified["capital_inner_selection_calendar_days"] == 365
    assert verified["capital_current_selection_calendar_days"] == 365
    assert len(verified["anchor_plan"]) == 4
    assert (
        causal._validate_anchor_plan(deepcopy(verified["anchor_plan"]))
        == (verified["anchor_plan"])
    )
    for anchor in verified["anchor_plan"]:
        start = datetime.fromisoformat(anchor["forward_start_date"]).date()
        end = datetime.fromisoformat(anchor["forward_end_date"]).date()
        assert (end - start).days + 1 == 365
    for prior, later in zip(verified["anchor_plan"], verified["anchor_plan"][1:]):
        prior_end_exclusive = datetime.fromisoformat(prior["forward_end_date"]).replace(
            tzinfo=UTC
        ) + timedelta(days=1)
        later_cutoff = datetime.fromisoformat(
            later["selection_cutoff_utc"].replace("Z", "+00:00")
        )
        assert later_cutoff > prior_end_exclusive
    assert verified["current_selection_window"] == {
        "start_date": "2025-07-13",
        "end_date": "2026-07-13",
    }
    inner_windows = verified["inner_selection_windows_by_anchor"]
    assert set(inner_windows) == {
        anchor["anchor_id"] for anchor in verified["anchor_plan"]
    }
    for anchor in verified["anchor_plan"]:
        inner = inner_windows[anchor["anchor_id"]]
        inner_start = datetime.fromisoformat(inner["start_date"]).date()
        inner_end = datetime.fromisoformat(inner["end_date"]).date()
        cutoff = datetime.fromisoformat(
            anchor["selection_cutoff_utc"].replace("Z", "+00:00")
        ).date()
        assert (inner_end - inner_start).days == 365
        assert inner_end == cutoff
    capital_anchors = capital_bootstrap._capital_anchor_plan(
        verified["anchor_plan"], inner_windows
    )
    constraints = CapCalibrationConstraints()
    assert all(
        (
            datetime.fromisoformat(row["inner_window"]["end_date"]).date()
            - datetime.fromisoformat(row["inner_window"]["start_date"]).date()
        ).days
        >= constraints.minimum_inner_calendar_days
        for row in capital_anchors
    )
    assert (
        capital_anchors[-1]["outer_window"]["end_date"]
        == verified["current_selection_window"]["start_date"]
    )
    proofs = verified["optimizer_train_history_proof_by_timeframe"]
    assert list(sorted(proofs)) == list(sorted(PRODUCTION_TIMEFRAME_ORDER))
    assert proofs["M5"]["optimizer_train_years"] == 5
    assert proofs["D1"]["optimizer_train_years"] == 12
    assert all(
        proof["authenticated_optimizer_train_depth_retained"] is True
        for proof in proofs.values()
    )
    assert verified["minimum_additional_history_calendar_days_by_timeframe"] == {
        timeframe: 0 for timeframe in PRODUCTION_TIMEFRAME_ORDER
    }
    flag = (
        "confers_search_process_current_refit_capital_readiness_publication_"
        "or_trading_authority"
    )
    assert verified["scope"] == anchor_plan.PLAN_SCOPE
    assert verified[flag] is False
    assert (
        receipt.receipt_sha256
        == hashlib.sha256(receipt.canonical_body_bytes).hexdigest()
    )
    assert verified["native_calendar"]["common_as_of_utc"] == common["common_as_of_utc"]
    assert verified["native_v5_common_request"] == common
    assert verified["native_v5_common_request_sha256"] == anchor_plan._sha(common)

    repeated = anchor_plan.derive_production_anchor_plan(data_dir)
    assert repeated.canonical_body_bytes == receipt.canonical_body_bytes
    assert repeated.receipt_sha256 == receipt.receipt_sha256


def test_non_midnight_and_native_end_mismatch_use_minimum_closed_midnight(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    data_dir, universe, common = _fixture(tmp_path)
    _install_resolver(monkeypatch, universe)
    endpoints = {
        row["end_exclusive_utc"]
        for row in common["timeframe_strategy_evidence_intervals"].values()
    }
    assert endpoints == {"2026-07-13T09:00:00Z", "2026-07-13T09:00:01Z"}

    body = anchor_plan.derive_production_anchor_plan(data_dir).to_dict()

    assert body["current_refit_cutoff_utc"] == "2026-07-13T00:00:00Z"
    assert body["anchor_plan"][-1]["forward_end_date"] == "2025-07-12"
    assert body["current_selection_window"] == {
        "start_date": "2025-07-13",
        "end_date": "2026-07-13",
    }


def test_fake_universe_object_cannot_impersonate_resolver_authority(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    fake = SimpleNamespace(
        valid=True,
        errors=[],
        activation_id="activation-anchor-plan",
        requested_symbols=tuple(PRODUCTION_SYMBOL_ORDER),
        required_timeframes=tuple(PRODUCTION_TIMEFRAME_ORDER),
    )
    _install_resolver(monkeypatch, fake)

    with pytest.raises(
        anchor_plan.ProductionAnchorPlanError,
        match="NATIVE_UNIVERSE_EXACT_TYPE_REQUIRED",
    ):
        anchor_plan.derive_production_anchor_plan(tmp_path / "data")


def test_resolver_errors_fail_closed_before_calendar_access(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    data_dir, universe, _common = _fixture(tmp_path)
    universe.errors.append("NATIVE_INFRASTRUCTURE_FAILURE")
    _install_resolver(monkeypatch, universe)

    with pytest.raises(anchor_plan.ProductionAnchorPlanError) as caught:
        anchor_plan.derive_production_anchor_plan(data_dir)
    assert caught.value.code == "NATIVE_UNIVERSE_RESOLUTION_FAILED"
    assert "NATIVE_INFRASTRUCTURE_FAILURE" in caught.value.details


@pytest.mark.parametrize(
    ("mutation", "expected"),
    [
        ("legacy_schema", "NATIVE_UNIVERSE_CURRENT_AUTHENTICITY_REQUIRED"),
        ("implicit_contract", "NATIVE_UNIVERSE_CURRENT_AUTHENTICITY_REQUIRED"),
        (
            "excluded_projection",
            "NATIVE_UNIVERSE_EXCLUDED_NO_TRADE_AUTHORITY_FORBIDDEN",
        ),
        (
            "excluded_cell",
            "NATIVE_UNIVERSE_EXCLUDED_NO_TRADE_AUTHORITY_FORBIDDEN",
        ),
    ],
)
def test_legacy_or_excluded_no_trade_universe_authority_is_rejected(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    mutation: str,
    expected: str,
) -> None:
    data_dir, universe, _common = _fixture(tmp_path)
    if mutation == "legacy_schema":
        universe.eligibility_schema = native.LEGACY_UNIVERSE_ELIGIBILITY_SCHEMA
    elif mutation == "implicit_contract":
        universe.explicit_contract = False
    elif mutation == "excluded_projection":
        universe.excluded_symbols = {"AUDUSD": {"reason": "caller verdict"}}
    elif mutation == "excluded_cell":
        universe.cell_statuses["AUDUSD"]["M5"] = "excluded_no_trade"
    else:  # pragma: no cover - parametrization is closed.
        raise AssertionError(mutation)
    _install_resolver(monkeypatch, universe)

    with pytest.raises(anchor_plan.ProductionAnchorPlanError, match=expected):
        anchor_plan.derive_production_anchor_plan(data_dir)


def test_public_factory_has_no_caller_date_or_geometry_override(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    calls = _install_resolver(monkeypatch, object())

    with pytest.raises(TypeError):
        anchor_plan.derive_production_anchor_plan(
            tmp_path / "data",
            current_refit_cutoff_utc="2030-01-01T00:00:00Z",  # type: ignore[call-arg]
        )
    with pytest.raises(TypeError):
        anchor_plan.derive_production_anchor_plan(  # type: ignore[call-arg]
            tmp_path / "data", "2030-01-01T00:00:00Z"
        )
    assert calls == []


def test_legacy_native_calendar_schema_is_not_anchor_authority(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def _mutate(request: dict[str, Any]) -> None:
        request["timeframe_request_map_schema_version"] = (
            native.FORENSIC_TIMEFRAME_REQUEST_MAP_SCHEMA_VERSION
        )
        request["timeframe_request_map_sha256"] = canonical_sha256(
            timeframe_request_hash_payload(request)
        )

    data_dir, universe, _common = _fixture(tmp_path, mutate_run_request=_mutate)
    _install_resolver(monkeypatch, universe)

    with pytest.raises(
        anchor_plan.ProductionAnchorPlanError, match="NATIVE_CALENDAR_V5_REQUIRED"
    ):
        anchor_plan.derive_production_anchor_plan(data_dir)


def test_fixed_window_history_shortfall_is_diagnostic_and_not_global_veto(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    short_common = build_v5_request_contract(requested_information_cutoff_utc=CUTOFF)
    data_dir, universe, _common = _fixture(tmp_path, common=short_common)
    _install_resolver(monkeypatch, universe)

    receipt = anchor_plan.derive_production_anchor_plan(data_dir)
    verified = anchor_plan.verify_production_anchor_plan_receipt(data_dir, receipt)

    assert verified["minimum_additional_history_calendar_days_by_timeframe"] == {
        "M5": 81,
        "M15": 81,
        "M30": 81,
        "H1": 80,
        "H4": 80,
        "D1": 78,
    }
    assert all(
        proof["authenticated_optimizer_train_depth_retained"] is False
        for proof in verified["optimizer_train_history_proof_by_timeframe"].values()
    )
    flag = (
        "confers_search_process_current_refit_capital_readiness_publication_"
        "or_trading_authority"
    )
    assert verified[flag] is False


def test_rehashed_native_end_splice_is_rejected_by_v5_semantics(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def _mutate(request: dict[str, Any]) -> None:
        changed_end = "2026-07-14T09:00:00Z"
        request["timeframe_strategy_evidence_intervals"]["M30"]["end_exclusive_utc"] = (
            changed_end
        )
        request["timeframe_source_request_intervals"]["M30"]["end_exclusive_utc"] = (
            changed_end
        )
        request["timeframe_request_map_sha256"] = canonical_sha256(
            timeframe_request_hash_payload(request)
        )

    data_dir, universe, _common = _fixture(tmp_path, mutate_run_request=_mutate)
    _install_resolver(monkeypatch, universe)

    with pytest.raises(anchor_plan.ProductionAnchorPlanError) as caught:
        anchor_plan.derive_production_anchor_plan(data_dir)
    assert caught.value.code == "NATIVE_CALENDAR_V5_VALIDATION_FAILED"
    assert any("V5_COMMON_FIELD_MISMATCH" in value for value in caught.value.details)


@pytest.mark.parametrize(
    ("mutation", "expected"),
    [
        ("current_length", "CAPITAL_CURRENT_SELECTION_WINDOW_LENGTH_MISMATCH"),
        (
            "current_leakage",
            "CAPITAL_CURRENT_SELECTION_REUSES_OR_GAPS_OUTER_EVIDENCE",
        ),
        ("inner_length", "CAPITAL_INNER_SELECTION_WINDOW_LENGTH_MISMATCH"),
        ("inner_leakage", "CAPITAL_INNER_SELECTION_WINDOW_LEAKAGE"),
    ],
)
def test_capital_window_length_and_leakage_reseal_attacks_fail_closed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    mutation: str,
    expected: str,
) -> None:
    data_dir, universe, _common = _fixture(tmp_path)
    _install_resolver(monkeypatch, universe)
    receipt = anchor_plan.derive_production_anchor_plan(data_dir)
    body = json.loads(receipt.canonical_body_bytes.decode("ascii"))
    if mutation == "current_length":
        body["current_selection_window"]["end_date"] = "2026-07-12"
        body["current_selection_window_sha256"] = anchor_plan._sha(
            body["current_selection_window"]
        )
    elif mutation == "current_leakage":
        body["current_selection_window"] = {
            "start_date": "2025-07-12",
            "end_date": "2026-07-12",
        }
        body["current_selection_window_sha256"] = anchor_plan._sha(
            body["current_selection_window"]
        )
    else:
        anchor = body["anchor_plan"][0]
        anchor_id = anchor["anchor_id"]
        window = body["inner_selection_windows_by_anchor"][anchor_id]
        if mutation == "inner_length":
            window["start_date"] = (
                datetime.fromisoformat(window["start_date"]).date() + timedelta(days=1)
            ).isoformat()
        elif mutation == "inner_leakage":
            leaking_end = datetime.fromisoformat(
                anchor["forward_start_date"]
            ).date() + timedelta(days=1)
            window["end_date"] = leaking_end.isoformat()
            window["start_date"] = (leaking_end - timedelta(days=365)).isoformat()
        else:  # pragma: no cover - parametrization is closed.
            raise AssertionError(mutation)
        body["inner_selection_windows_by_anchor_sha256"] = anchor_plan._sha(
            body["inner_selection_windows_by_anchor"]
        )
    raw = anchor_plan._canonical_bytes(body)
    resealed = anchor_plan.ProductionAnchorPlanReceipt(
        _body_json=raw,
        _receipt_sha256=hashlib.sha256(raw).hexdigest(),
    )

    with pytest.raises(anchor_plan.ProductionAnchorPlanError, match=expected):
        anchor_plan.verify_production_anchor_plan_receipt(data_dir, resealed)


def test_resealed_calendar_cannot_launder_earlier_history_than_v5_preimage(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    data_dir, universe, _common = _fixture(tmp_path)
    _install_resolver(monkeypatch, universe)
    receipt = anchor_plan.derive_production_anchor_plan(data_dir)
    body = json.loads(receipt.canonical_body_bytes.decode("ascii"))
    body["native_calendar"]["timeframe_strategy_evidence_intervals"]["M5"][
        "start_utc"
    ] = "2010-01-01T00:00:00Z"
    body["native_calendar"]["timeframe_source_request_intervals"]["M5"]["start_utc"] = (
        "2009-01-01T00:00:00Z"
    )
    body["native_calendar_sha256"] = anchor_plan._sha(body["native_calendar"])
    proofs, additional = anchor_plan._history_proofs(
        body["native_calendar"], body["anchor_plan"]
    )
    body["optimizer_train_history_proof_by_timeframe"] = proofs
    body["minimum_additional_history_calendar_days_by_timeframe"] = additional
    raw = anchor_plan._canonical_bytes(body)
    resealed = anchor_plan.ProductionAnchorPlanReceipt(
        _body_json=raw,
        _receipt_sha256=hashlib.sha256(raw).hexdigest(),
    )

    with pytest.raises(anchor_plan.ProductionAnchorPlanError) as caught:
        anchor_plan.verify_production_anchor_plan_receipt(data_dir, resealed)
    assert caught.value.code == "NATIVE_CALENDAR_COMMON_REQUEST_PROJECTION_MISMATCH"


def test_fully_resealed_authority_identity_fails_current_authority_reopen(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    data_dir, universe, _common = _fixture(tmp_path)
    _install_resolver(monkeypatch, universe)
    receipt = anchor_plan.derive_production_anchor_plan(data_dir)
    body = json.loads(receipt.canonical_body_bytes.decode("ascii"))
    body["activation_id"] = "forged-activation"
    body["activation_manifest_sha256"] = "a" * 64
    body["native_run_manifest_sha256"] = "b" * 64
    raw = anchor_plan._canonical_bytes(body)
    resealed = anchor_plan.ProductionAnchorPlanReceipt(
        _body_json=raw,
        _receipt_sha256=hashlib.sha256(raw).hexdigest(),
    )

    with pytest.raises(anchor_plan.ProductionAnchorPlanError) as caught:
        anchor_plan.verify_production_anchor_plan_receipt(data_dir, resealed)
    assert caught.value.code == "ANCHOR_PLAN_CURRENT_NATIVE_AUTHORITY_MISMATCH"


def test_fully_self_consistent_future_calendar_reseal_fails_current_authority_reopen(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    data_dir, universe, _common = _fixture(tmp_path)
    _install_resolver(monkeypatch, universe)
    receipt = anchor_plan.derive_production_anchor_plan(data_dir)
    body = json.loads(receipt.canonical_body_bytes.decode("ascii"))
    forged_common = build_v5_request_contract(
        requested_information_cutoff_utc=datetime(2030, 1, 15, 9, 3, 17, tzinfo=UTC),
        warmup_days=500,
    )
    forged_calendar = anchor_plan._calendar_projection(forged_common)
    _validated_calendar, forged_cutoff = anchor_plan._validate_calendar_projection(
        forged_calendar
    )
    forged_anchors = anchor_plan._derive_anchor_plan(forged_cutoff)
    forged_inner = anchor_plan._derive_inner_selection_windows(forged_anchors)
    forged_current = anchor_plan._derive_current_selection_window(forged_cutoff)
    forged_proofs, forged_additional = anchor_plan._history_proofs(
        forged_calendar, forged_anchors
    )
    body.update(
        {
            "native_v5_common_request": forged_common,
            "native_v5_common_request_sha256": anchor_plan._sha(forged_common),
            "native_calendar": forged_calendar,
            "native_calendar_sha256": anchor_plan._sha(forged_calendar),
            "current_refit_cutoff_utc": anchor_plan._iso(forged_cutoff),
            "latest_fully_closed_common_date": (
                forged_cutoff.date() - timedelta(days=1)
            ).isoformat(),
            "latest_historical_outer_end_date": forged_anchors[-1]["forward_end_date"],
            "anchor_plan": forged_anchors,
            "anchor_plan_sha256": anchor_plan._sha(forged_anchors),
            "inner_selection_windows_by_anchor": forged_inner,
            "inner_selection_windows_by_anchor_sha256": anchor_plan._sha(forged_inner),
            "current_selection_window": forged_current,
            "current_selection_window_sha256": anchor_plan._sha(forged_current),
            "optimizer_train_history_proof_by_timeframe": forged_proofs,
            "minimum_additional_history_calendar_days_by_timeframe": (
                forged_additional
            ),
        }
    )
    raw = anchor_plan._canonical_bytes(body)
    resealed = anchor_plan.ProductionAnchorPlanReceipt(
        _body_json=raw,
        _receipt_sha256=hashlib.sha256(raw).hexdigest(),
    )

    with pytest.raises(anchor_plan.ProductionAnchorPlanError) as caught:
        anchor_plan.verify_production_anchor_plan_receipt(data_dir, resealed)
    assert caught.value.code == "ANCHOR_PLAN_CURRENT_NATIVE_AUTHORITY_MISMATCH"


def test_loader_returns_locally_pinned_activation_identity(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    data_dir, universe, _common = _fixture(tmp_path)
    original_activation_id = universe.activation_id
    original_activation_sha = universe.activation_manifest_sha256
    _install_resolver(monkeypatch, universe)
    original_loader = anchor_plan._load_verified_v5_common

    def _mutate_universe_after_reopen(*args: Any, **kwargs: Any) -> Any:
        result = original_loader(*args, **kwargs)
        universe.activation_id = "concurrently-mutated-activation"
        universe.activation_manifest_sha256 = "f" * 64
        return result

    monkeypatch.setattr(
        anchor_plan, "_load_verified_v5_common", _mutate_universe_after_reopen
    )

    receipt = anchor_plan.derive_production_anchor_plan(data_dir)
    produced = receipt.to_dict()
    assert produced["activation_id"] == original_activation_id
    assert produced["activation_manifest_sha256"] == original_activation_sha
    with pytest.raises(
        anchor_plan.ProductionAnchorPlanError,
        match="ACTIVE_POINTER_AUTHORITY_MISMATCH",
    ):
        anchor_plan.verify_production_anchor_plan_receipt(data_dir, receipt)


def test_receipt_hash_and_geometry_cannot_be_resealed_after_mutation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    data_dir, universe, _common = _fixture(tmp_path)
    _install_resolver(monkeypatch, universe)
    receipt = anchor_plan.derive_production_anchor_plan(data_dir)
    body = json.loads(receipt.canonical_body_bytes.decode("ascii"))
    body["anchor_plan"][0]["selection_cutoff_utc"] = "2021-02-20T00:00:00Z"
    mutated = anchor_plan._canonical_bytes(body)

    with pytest.raises(
        anchor_plan.ProductionAnchorPlanError,
        match="ANCHOR_PLAN_RECEIPT_SHA256_MISMATCH",
    ):
        anchor_plan.verify_production_anchor_plan_receipt(
            data_dir, replace(receipt, _body_json=mutated)
        )

    resealed = anchor_plan.ProductionAnchorPlanReceipt(
        _body_json=mutated,
        _receipt_sha256=hashlib.sha256(mutated).hexdigest(),
    )
    with pytest.raises(
        anchor_plan.ProductionAnchorPlanError,
        match="ANCHOR_PLAN_GEOMETRY_DERIVATION_MISMATCH",
    ):
        anchor_plan.verify_production_anchor_plan_receipt(data_dir, resealed)
