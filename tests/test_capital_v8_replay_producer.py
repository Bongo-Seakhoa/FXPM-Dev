from __future__ import annotations

import base64
from copy import deepcopy
from datetime import date, datetime, timedelta
from pathlib import Path

import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

import pm_cap_calibration as cap
from pm_cap_replay_producer import (
    build_capital_v8_attempt_from_synchronized_output,
    capital_policy_for_point,
    produce_unsigned_capital_v8_replay_body,
)
from pm_portfolio_replay import (
    LEGACY_PORTFOLIO_REPLAY_SCHEMA_VERSION,
    PORTFOLIO_REPLAY_CANDIDATE_SCHEMA_VERSION,
    CandidateTrade,
    ExitLeg,
    MarkPoint,
    PortfolioPolicy,
    SynchronizedPortfolioSimulator,
    portfolio_replay_candidate_roster_receipt,
)


def _sha(label: str) -> str:
    return cap.canonical_json_sha256({"label": label})


def _point(policy_id: str, total: float, factor: float, symbol: float) -> dict:
    return {
        "policy_id": policy_id,
        "max_total_stop_risk_pct": total,
        "max_factor_stop_risk_pct": factor,
        "max_same_symbol_stop_risk_pct": symbol,
        "max_margin_utilization_pct": 50.0,
        "min_margin_level_pct": 200.0,
        "min_free_margin_pct": 50.0,
        "gap_stress_multiple": 2.5,
    }


def _base_policy(*, current: bool = False) -> PortfolioPolicy:
    return PortfolioPolicy(
        starting_balance=10_000.0,
        target_risk_per_trade_pct=1.0,
        max_risk_per_trade_pct=1.0,
        max_open_risk_pct=20.0,
        max_concurrent_positions=10,
        max_same_symbol_open_risk_pct=8.0,
        max_factor_group_open_risk_pct=12.0,
        max_correlated_cluster_risk_pct=12.0,
        max_margin_utilization_pct=50.0,
        min_margin_level_pct=200.0,
        min_free_margin_pct=50.0,
        allowed_promotion_statuses=(
            ("PROCESS_VALIDATED_CURRENT_REFIT",)
            if current
            else ("PROMOTED",)
        ),
    )


def _candidate(phase_id: str, start: date, *, current: bool = False) -> CandidateTrade:
    entry = datetime.combine(start, datetime.min.time()) + timedelta(hours=1)
    exit_time = entry + timedelta(days=1)
    return CandidateTrade(
        candidate_id=f"{phase_id}|candidate",
        cell_id="EURUSD|H1|TREND",
        symbol="EURUSD",
        timeframe="H1",
        regime="TREND",
        strategy_name="TestStrategy",
        promotion_status=(
            "PROCESS_VALIDATED_CURRENT_REFIT" if current else "PROMOTED"
        ),
        asset_class="fx",
        risk_groups=(),
        entry_time=entry.isoformat(),
        direction=1,
        stop_risk_per_lot=100.0,
        margin_per_lot=100.0,
        volume_min=0.01,
        volume_max=100.0,
        volume_step=0.01,
        priority_score=1.0,
        legs=(ExitLeg(exit_time.isoformat(), 1.0, 50.0),),
        marks=(MarkPoint((entry + timedelta(hours=12)).isoformat(), 25.0),),
        source_schema=PORTFOLIO_REPLAY_CANDIDATE_SCHEMA_VERSION,
        margin_model="test-margin-v1",
    ).normalized()


def _contract() -> dict[str, str]:
    return {
        "book_contract_version": "book-v11",
        "refit_contract_version": "refit-v2",
        "cost_contract_sha256": _sha("cost"),
        "native_source_contract_sha256": _sha("native"),
        "replay_contract_sha256": _sha("replay"),
        "margin_contract_sha256": _sha("margin"),
    }


def _lineage(phase_id: str, *, current: bool = False) -> dict:
    anchor_id, phase = phase_id.split("|", 1)
    stable = "CURRENT" if current else anchor_id
    return {
        **_contract(),
        "book_role": (
            "PROCESS_VALIDATED_CURRENT_REFIT" if current else "ANCHOR_LOCAL_REFIT"
        ),
        "book_decision_sha256": _sha(f"book-decision:{stable}"),
        "book_generation_receipt_sha256": _sha(f"generation:{stable}"),
        "refit_receipt_sha256": _sha(f"refit:{stable}"),
        "refit_execution_sha256": _sha(f"execution:{stable}"),
        "output_book_sha256": _sha(f"book:{stable}"),
        "winner_roster_sha256": _sha(f"winners:{stable}"),
        "native_activation_id": f"activation-{stable}",
        "native_activation_manifest_sha256": _sha(f"manifest:{stable}"),
        "native_activation_roster_sha256": _sha(f"activation-roster:{stable}"),
        "native_bars_sha256_by_source": {
            "EURUSD|H1": _sha(f"bars:{phase_id}")
        },
        "native_sidecar_sha256_by_source": {
            "EURUSD|H1": _sha(f"sidecar:{phase_id}")
        },
        "margin_terms_sha256": _sha(f"margin-terms:{stable}"),
        "broker_terms_sha256": _sha(f"broker-terms:{stable}"),
    }


def _run(
    candidate: CandidateTrade,
    point: dict,
    *,
    current: bool = False,
) -> dict:
    simulator = SynchronizedPortfolioSimulator(
        capital_policy_for_point(_base_policy(current=current), point)
    )
    if current:
        return simulator._run([candidate], allow_current_refit=True)
    return simulator.run([candidate])


def _window(start: date, days: int = 3) -> dict[str, str]:
    return {
        "start_date": start.isoformat(),
        "end_date": (start + timedelta(days=days)).isoformat(),
    }


def test_actual_replay_output_builds_complete_daily_v8_attempt(tmp_path: Path) -> None:
    point = _point("low", 10.0, 6.0, 4.0)
    window = _window(date(2025, 1, 1))
    candidate = _candidate("A1|INNER_SELECTION", date(2025, 1, 1))
    frozen = portfolio_replay_candidate_roster_receipt([candidate])
    result = _run(candidate, point)

    attempt = build_capital_v8_attempt_from_synchronized_output(
        result,
        output_root=tmp_path,
        anchor_id="A1",
        phase="INNER_SELECTION",
        window=window,
        policy_point=point,
        lineage=_lineage("A1|INNER_SELECTION"),
        frozen_candidate_roster=frozen,
    )

    assert attempt["status"] == "COMPLETE"
    assert attempt["accepted_positions"] == 1
    assert [row["date"] for row in attempt["daily_observations"]] == [
        "2025-01-01",
        "2025-01-02",
        "2025-01-03",
    ]
    first = attempt["daily_observations"][0]
    assert first["same_symbol_stop_risk_pct"] <= first["factor_stop_risk_pct"]
    assert first["factor_stop_risk_pct"] <= first["total_stop_risk_pct"]
    assert first["joint_gap_loss_pct"] == pytest.approx(
        first["total_stop_risk_pct"] * 2.5
    )
    assert first["margin_level_pct"] == pytest.approx(
        10_000.0 / first["margin_utilization_pct"]
    )
    assert sum(row["admission_events"] for row in attempt["daily_observations"]) == 1
    assert len(list((tmp_path / "replay_output_receipts").iterdir())) == 1


@pytest.mark.parametrize(
    "mutation,match",
    [
        (
            lambda result: result["summary"].__setitem__(
                "schema_version", LEGACY_PORTFOLIO_REPLAY_SCHEMA_VERSION
            ),
            "LEGACY_V7_CAPITAL_REPLAY_DISABLED",
        ),
        (
            lambda result: result["path"][0].__setitem__(
                "factor_stop_risk_pct",
                result["path"][0]["total_stop_risk_pct"] + 1.0,
            ),
            "stop-risk hierarchy",
        ),
        (
            lambda result: result["closed_positions"][0].__setitem__(
                "candidate_id", "forged"
            ),
            "disposition roster mismatch",
        ),
    ],
)
def test_hostile_replay_output_cannot_be_adapted(
    tmp_path: Path,
    mutation,
    match: str,
) -> None:
    point = _point("low", 10.0, 6.0, 4.0)
    candidate = _candidate("A1|INNER_SELECTION", date(2025, 1, 1))
    frozen = portfolio_replay_candidate_roster_receipt([candidate])
    result = _run(candidate, point)
    mutation(result)

    with pytest.raises(ValueError, match=match):
        build_capital_v8_attempt_from_synchronized_output(
            result,
            output_root=tmp_path,
            anchor_id="A1",
            phase="INNER_SELECTION",
            window=_window(date(2025, 1, 1)),
            policy_point=point,
            lineage=_lineage("A1|INNER_SELECTION"),
            frozen_candidate_roster=frozen,
        )


def test_different_candidate_roster_and_unverified_current_mode_fail(
    tmp_path: Path,
) -> None:
    point = _point("low", 10.0, 6.0, 4.0)
    candidate = _candidate("A1|INNER_SELECTION", date(2025, 1, 1))
    result = _run(candidate, point)
    other = deepcopy(candidate)
    object.__setattr__(other, "candidate_id", "other")
    with pytest.raises(ValueError, match="was not frozen"):
        build_capital_v8_attempt_from_synchronized_output(
            result,
            output_root=tmp_path,
            anchor_id="A1",
            phase="INNER_SELECTION",
            window=_window(date(2025, 1, 1)),
            policy_point=point,
            lineage=_lineage("A1|INNER_SELECTION"),
            frozen_candidate_roster=portfolio_replay_candidate_roster_receipt(
                [other]
            ),
        )

    with pytest.raises(ValueError, match="execution authority mode"):
        build_capital_v8_attempt_from_synchronized_output(
            result,
            output_root=tmp_path,
            anchor_id="CURRENT",
            phase="CURRENT_SELECTION",
            window=_window(date(2025, 1, 1)),
            policy_point=point,
            lineage=_lineage("CURRENT|CURRENT_SELECTION", current=True),
            frozen_candidate_roster=portfolio_replay_candidate_roster_receipt(
                [candidate]
            ),
        )


def _constraints() -> cap.CapCalibrationConstraints:
    return cap.CapCalibrationConstraints(
        minimum_accepted_positions=1,
        maximum_mtm_drawdown_pct=50.0,
        maximum_initial_stop_stress_drawdown_pct=50.0,
        minimum_cap_utilization_fraction=0.0,
        maximum_joint_gap_stress_loss_pct=50.0,
        maximum_factor_gap_stress_loss_pct=50.0,
        maximum_margin_utilization_pct=90.0,
        minimum_outer_anchors=2,
        maximum_outer_anchors=4,
        maximum_candidate_policies=4,
        minimum_exposure_effective_observations=0.0,
        minimum_near_cap_observations=0,
        minimum_paired_policy_observations=1,
        minimum_profitable_anchor_fraction=0.0,
        familywise_alpha=0.10,
        minimum_inner_calendar_days=3,
        minimum_outer_calendar_days=3,
        minimum_current_calendar_days=3,
        minimum_hac_effective_observations=0.0,
        hac_lag_days=1,
        minimum_margin_level_pct=150.0,
        minimum_free_margin_pct=25.0,
    )


def _signed_precommit(constraints: cap.CapCalibrationConstraints) -> tuple[dict, dict]:
    cursor = date(2025, 1, 1)
    anchors = []
    for index in range(2):
        inner = _window(cursor)
        cursor += timedelta(days=3)
        outer = _window(cursor)
        cursor += timedelta(days=3)
        anchors.append(
            {
                "anchor_id": f"A{index + 1}",
                "inner_window": inner,
                "outer_window": outer,
            }
        )
    body = {
        "schema_version": cap.CAP_PRECOMMIT_SCHEMA_VERSION,
        "precommitment_id": "capital-v8-producer-test",
        "created_at_utc": "2026-01-01T00:00:00Z",
        "evidence_access_not_before_utc": "2026-01-02T00:00:00Z",
        "candidate_policies": [
            _point("high", 20.0, 12.0, 8.0),
            _point("low", 10.0, 6.0, 4.0),
        ],
        "anchor_plan": anchors,
        "current_selection_window": _window(cursor),
        "selection_rule": deepcopy(cap._FROZEN_SELECTION_RULE),
        "constraints_sha256": cap.canonical_json_sha256(constraints.__dict__),
        "contract_lineage": _contract(),
    }
    private = Ed25519PrivateKey.generate()
    public = private.public_key().public_bytes(
        serialization.Encoding.Raw,
        serialization.PublicFormat.Raw,
    )
    signature = private.sign(
        cap.capital_authority_signature_payload(
            cap.PRECOMMITMENT_SIGNATURE_KIND, "pre-key", body
        )
    )
    envelope = {
        "schema_version": cap.CAP_SIGNED_ENVELOPE_SCHEMA_VERSION,
        "key_id": "pre-key",
        "body": body,
        "signature_base64": base64.b64encode(signature).decode("ascii"),
    }
    return envelope, {"pre-key": public}


def test_signed_precommit_produces_exact_bounded_sharded_body(tmp_path: Path) -> None:
    constraints = _constraints()
    precommit, keys = _signed_precommit(constraints)
    phase_windows: dict[str, dict] = {}
    for anchor in precommit["body"]["anchor_plan"]:
        phase_windows[f"{anchor['anchor_id']}|INNER_SELECTION"] = anchor[
            "inner_window"
        ]
        phase_windows[f"{anchor['anchor_id']}|OUTER_EVALUATION"] = anchor[
            "outer_window"
        ]
    phase_windows["CURRENT|CURRENT_SELECTION"] = precommit["body"][
        "current_selection_window"
    ]
    candidates: dict[str, CandidateTrade] = {}
    frozen = {}
    lineages = {}
    for phase_id, window in phase_windows.items():
        current = phase_id.startswith("CURRENT|")
        candidate = _candidate(
            phase_id,
            date.fromisoformat(window["start_date"]),
            current=current,
        )
        candidates[phase_id] = candidate
        frozen[phase_id] = portfolio_replay_candidate_roster_receipt([candidate])
        lineages[phase_id] = _lineage(phase_id, current=current)

    def loader(spec: dict) -> dict:
        phase_id = f"{spec['anchor_id']}|{spec['phase']}"
        return _run(
            candidates[phase_id],
            spec["policy"],
            current=phase_id.startswith("CURRENT|"),
        )

    body = produce_unsigned_capital_v8_replay_body(
        output_root=tmp_path,
        precommitment_receipt=precommit,
        trusted_precommitment_public_keys=keys,
        constraints=constraints,
        frozen_candidate_rosters_by_phase=frozen,
        lineage_by_phase=lineages,
        replay_output_loader=loader,
        generated_at_utc="2026-01-03T00:00:00Z",
        shard_attempt_limit=2,
    )

    assert body["schema_version"] == cap.CAP_REPLAY_SCHEMA_VERSION
    assert body["attempt_count"] == 10
    assert body["daily_observation_count"] == 30
    assert body["storage_mode"] == "CONTENT_ADDRESSED_ATTEMPT_SHARDS_V1"
    attempts = cap._materialize_attempt_shards(body, tmp_path)
    assert len({item["attempt_id"] for item in attempts}) == 10
    assert all(item["status"] == "COMPLETE" for item in attempts)
    assert len(list((tmp_path / "replay_output_receipts").iterdir())) == 10


def test_batch_rejects_tampered_signature_and_incomplete_phase_roster(
    tmp_path: Path,
) -> None:
    constraints = _constraints()
    precommit, keys = _signed_precommit(constraints)
    precommit["body"]["precommitment_id"] = "tampered"
    with pytest.raises(cap._Blocked, match="SIGNATURE_INVALID"):
        produce_unsigned_capital_v8_replay_body(
            output_root=tmp_path,
            precommitment_receipt=precommit,
            trusted_precommitment_public_keys=keys,
            constraints=constraints,
            frozen_candidate_rosters_by_phase={},
            lineage_by_phase={},
            replay_output_loader=lambda _spec: {},
            generated_at_utc="2026-01-03T00:00:00Z",
        )

    precommit, keys = _signed_precommit(constraints)
    with pytest.raises(ValueError, match="frozen phase roster is incomplete"):
        produce_unsigned_capital_v8_replay_body(
            output_root=tmp_path,
            precommitment_receipt=precommit,
            trusted_precommitment_public_keys=keys,
            constraints=constraints,
            frozen_candidate_rosters_by_phase={},
            lineage_by_phase={},
            replay_output_loader=lambda _spec: {},
            generated_at_utc="2026-01-03T00:00:00Z",
        )
