from __future__ import annotations

import copy
import hashlib
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

import pm_book_multiplicity as book
import pm_optimization_authority as authority
from pm_book_multiplicity import (
    AVAILABLE,
    REGIME_FITTED,
    REGIME_ORDER,
    BookMultiplicityDecision,
    create_historical_process_state_decision,
    create_historical_source_group_decision,
    create_regime_detector_record,
    create_regime_fit_decision,
    evaluate_historical_book_multiplicity,
    process_state_cell_evidence,
    verify_historical_book_multiplicity_decision,
)
from pm_current_refit_execution import build_current_regime_params_bundle
from tests.test_book_multiplicity import (
    _available_state_cell,
    _family,
    _terminal_family,
    _verified_winner,
)
from tests.test_model_lifecycle import _process_refit_receipts
from tests.test_model_lifecycle import (
    _detector_parameters,
    _digest,
    _train_support,
)
from pm_model_lifecycle import canonical_receipt_sha256


def _write(path: Path, value) -> bytes:
    raw = json.dumps(value, allow_nan=False, sort_keys=True).encode("utf-8")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(raw)
    return raw


def _fixture(tmp_path: Path):
    book = evaluate_historical_book_multiplicity(_terminal_family()).to_dict()

    def add_detector_bindings(rows):
        for index, row in enumerate(rows):
            for name in (
                "regime_fit_receipt_sha256",
                "regime_detector_record_sha256",
                "regime_detector_parameters_sha256",
                "regime_detector_code_sha256",
            ):
                row[name] = hashlib.sha256(f"{name}:{index}".encode()).hexdigest()

    _process, accepted_book, refit_receipt = _process_refit_receipts(
        roster_change=add_detector_bindings
    )
    refit_root = tmp_path / "refit"
    _write(refit_root / "raw-manifest.json", {})
    (tmp_path / "capital-replay").mkdir()

    indexes = {
        "regime_support": {
            "schema_version": authority.INDEX_SCHEMAS["regime_support"],
            "current_book_multiplicity_receipt_sha256": book["receipt_sha256"],
            "native_activation_id": "activation-test",
            "native_activation_manifest_sha256": "a" * 64,
            "detector_ledger_records": [],
        },
        "causal_process": {
            "schema_version": authority.INDEX_SCHEMAS["causal_process"],
            "historical_process_decision": {},
            "precommit_payload": {},
            "evidence_payload": {},
            "reconciliation_payload": {},
            "historical_book_receipt_sha256": book["receipt_sha256"],
        },
        "historical_book": {
            "schema_version": authority.INDEX_SCHEMAS["historical_book"],
            "book_decision": book,
        },
        "current_process_book": {
            "schema_version": authority.INDEX_SCHEMAS["current_process_book"],
            "accepted_book_generation_receipt": accepted_book,
            "current_book_decision": book,
            "historical_process_decision_sha256": "b" * 64,
        },
        "current_refit_execution": {
            "schema_version": authority.INDEX_SCHEMAS["current_refit_execution"],
            "current_refit_receipt": refit_receipt,
            "raw_manifest_relative_path": "refit/raw-manifest.json",
            "artifact_root_relative_path": "refit",
        },
        "synchronized_portfolio_replay": {
            "schema_version": authority.INDEX_SCHEMAS["synchronized_portfolio_replay"],
            "evidence": {},
            "expected_lineage": {},
            "expected_policies": {"10000": {}},
            "validation_policy": {
                "minimum_calendar_days": 730.0,
                "minimum_accepted_positions": 100,
                "minimum_profit_factor": 1.05,
                "minimum_return_pct": 0.0,
                "maximum_drawdown_pct": 20.0,
                "require_mtm": True,
                "maximum_bootstrap_ruin_upper_95pct": 0.05,
                "maximum_bootstrap_p95_drawdown_pct": 20.0,
                "minimum_bootstrap_daily_return_observations": 252,
            },
        },
        "capital_calibration": {
            "schema_version": authority.INDEX_SCHEMAS["capital_calibration"],
            "artifact": {},
            "expected_lineage": {},
            "constraints": {},
            "expected_selected_policy_point_id": None,
            "replay_evidence_root_relative_path": "capital-replay",
            "trusted_authority_public_keys": {},
            "trusted_replay_public_keys": {},
            "trusted_precommitment_public_keys": {},
            "precommitment_receipt": None,
            "replay_evidence_receipt": None,
        },
    }
    descriptors = {}
    paths = {}
    hashes = {}
    for name, value in indexes.items():
        path = tmp_path / f"{name}.json"
        raw = _write(path, value)
        digest = hashlib.sha256(raw).hexdigest()
        hashes[name] = digest
        paths[name] = path
        descriptors[name] = {
            "relative_path": path.name,
            "sha256": digest,
            "size_bytes": len(raw),
            "schema_version": authority.INDEX_SCHEMAS[name],
        }

    ledger = {}
    ledger_path = tmp_path / "pm_configs.json"
    ledger_raw = _write(ledger_path, ledger)
    config_payload = {
        "symbols": ["EURUSD"],
        "pipeline": {
            "data_dir": "data",
            "max_total_open_risk_pct": 8.0,
            "max_factor_group_open_risk_pct": 2.0,
            "max_combined_risk_pct": 1.0,
            "margin_entry_block_level": 200.0,
            "production_portfolio_replay_min_calendar_days": 730,
            "production_portfolio_replay_min_positions": 100,
            "production_portfolio_replay_min_profit_factor": 1.05,
            "production_portfolio_replay_min_return_pct": 0.0,
            "production_portfolio_replay_max_drawdown_pct": 20.0,
            "production_portfolio_replay_require_mtm": True,
        },
    }
    config_path = tmp_path / "config.json"
    config_raw = _write(config_path, config_payload)
    receipt_body = {
        "schema_version": authority.CHAIN_RECEIPT_SCHEMA,
        "generation_id": "generation-test",
        "authorization_scope": "DEMO_PROBATION",
        "config_relative_path": "config.json",
        "config_sha256": hashlib.sha256(config_raw).hexdigest(),
        "ledger_sha256": hashlib.sha256(ledger_raw).hexdigest(),
        "native_activation_id": "activation-test",
        "native_activation_manifest_sha256": "a" * 64,
        "requested_symbols": ["EURUSD"],
        "authority_artifact_sha256_by_name": hashes,
    }
    receipt = {
        **receipt_body,
        "receipt_sha256": authority._canonical_sha(receipt_body),
    }
    manifest = {
        "schema_version": authority.GENERATION_SCHEMA,
        "generation_id": "generation-test",
        "generation_status": "PROMOTED",
        "ledger_sha256": hashlib.sha256(ledger_raw).hexdigest(),
        "technical_failures": {},
        "authority_chain_receipt": receipt,
        "authority_artifacts": descriptors,
    }
    manifest_path = tmp_path / "pm_configs.generation.json"
    _write(manifest_path, manifest)
    config = SimpleNamespace(**config_payload["pipeline"])
    native = SimpleNamespace(
        activation_id="activation-test",
        activation_manifest_sha256="a" * 64,
        cell_status=lambda *_: "source_unavailable",
        entries={},
    )
    return {
        "manifest": manifest,
        "manifest_path": manifest_path,
        "authority_index_paths": paths,
        "ledger": ledger,
        "ledger_path": ledger_path,
        "config": config,
        "config_payload": config_payload,
        "config_path": config_path,
        "requested_symbols": ("EURUSD",),
        "native_universe": native,
        "validation_time_utc": "2026-07-14T00:00:00Z",
        "require_funded_authorization": False,
    }


def _management_fixture(tmp_path: Path, *, technical_timeframe: str | None = None):
    source = (tmp_path / "source").resolve()
    native = source / "data" / "native"
    activation_id = "activation-captured"
    selected_tf = "H1"
    selected_root = native / "runs" / "run-1" / "EURUSD" / selected_tf
    bars = selected_root / "bars.csv"
    sidecar = selected_root / "provenance.json"
    bars.parent.mkdir(parents=True)
    bars.write_bytes(b"time,open,high,low,close\n")
    sidecar.write_bytes(b'{"status":"captured"}')
    bars_sha = hashlib.sha256(bars.read_bytes()).hexdigest()
    sidecar_sha = hashlib.sha256(sidecar.read_bytes()).hexdigest()
    entries = {}
    statuses = {}
    for timeframe in authority.TIMEFRAMES:
        selected = timeframe == selected_tf
        technical = timeframe == technical_timeframe
        status = "technical_error" if technical else "evidence_candidate"
        entries[timeframe] = {
            "symbol": "EURUSD",
            "timeframe": timeframe,
            "status": status,
            "audit_status": "PASS",
            "universe_eligibility_status": status,
            "tradeable": False,
            "statistical_sufficiency_receipt_required": True,
            "bars_relative_path": f"runs/run-1/EURUSD/{timeframe}/bars.csv",
            "sidecar_relative_path": (f"runs/run-1/EURUSD/{timeframe}/provenance.json"),
            "bars_sha256": None
            if technical
            else bars_sha
            if selected
            else hashlib.sha256(f"bars:{timeframe}".encode()).hexdigest(),
            "sidecar_sha256": sidecar_sha
            if selected
            else hashlib.sha256(f"sidecar:{timeframe}".encode()).hexdigest(),
            "consensus_status": "PENDING" if technical else "PASS",
            "resolution_status": "PENDING" if technical else "PASS",
        }
        statuses[timeframe] = status
    candidate_timeframes = [
        timeframe
        for timeframe in authority.TIMEFRAMES
        if timeframe != technical_timeframe
    ]
    activation = {
        "schema_version": "fxpm-native-activation-v1",
        "status": "active",
        "activation_id": activation_id,
        "production_authoritative": True,
        "native_is_signal_authority": True,
        "silent_fallback_allowed": False,
        "coverage_activation_threshold_removed": True,
        "entries": {"EURUSD": entries},
        "universe_eligibility": {
            "schema_version": "fxpm-native-universe-authenticity-v4",
            "decision_basis": "retrieval_authenticity_cadence_and_availability_only",
            "performance_inputs_used": False,
            "trading_verdicts_emitted": False,
            "coverage_is_diagnostic_only": True,
            "fixed_coverage_activation_threshold": None,
            "session_coverage_method": "broker_native_cross_resolution_partition_v1",
            "session_coverage_partitions": [
                "optimizer_train",
                "tuning_validation",
                "final_lockbox",
            ],
            "requested_symbols": ["EURUSD"],
            "required_timeframes": list(authority.TIMEFRAMES),
            "cell_statuses": {"EURUSD": statuses},
            "terminal_states": [
                "evidence_candidate",
                "source_unavailable",
                *(["technical_error"] if technical_timeframe is not None else []),
            ],
            "nonterminal_states": (
                ["pending"]
                if technical_timeframe is not None
                else ["technical_error", "pending"]
            ),
            "evidence_candidate_timeframes_by_symbol": {"EURUSD": candidate_timeframes},
            "source_unavailable_timeframes_by_symbol": {"EURUSD": []},
            "symbols_with_evidence_candidates": ["EURUSD"],
            "eligible_symbols": ["EURUSD"],
            "excluded_symbols": {},
            "statistical_sufficiency_authority": (
                "authenticated_horizon_ess_trade_power_and_multiplicity_v1"
            ),
        },
    }
    activation_raw = json.dumps(activation, sort_keys=True).encode()
    activation_sha = hashlib.sha256(activation_raw).hexdigest()
    activation_logical = f"data/native/activations/{activation_id}.json"
    closure_values = {}
    for role, logical, path, digest in (
        (
            "native_bars",
            "data/native/runs/run-1/EURUSD/H1/bars.csv",
            bars,
            bars_sha,
        ),
        (
            "native_sidecar",
            "data/native/runs/run-1/EURUSD/H1/provenance.json",
            sidecar,
            sidecar_sha,
        ),
    ):
        body = {
            "role": role,
            "logical_relative_path": logical,
            "size_bytes": path.stat().st_size,
            "sha256": digest,
        }
        closure_values[authority._canonical_sha(body)] = body
    native_binding = {
        "activation_id": activation_id,
        "activation_manifest_sha256": activation_sha,
        "bars_logical_relative_path": ("data/native/runs/run-1/EURUSD/H1/bars.csv"),
        "bars_size_bytes": bars.stat().st_size,
        "bars_sha256": bars_sha,
        "sidecar_logical_relative_path": (
            "data/native/runs/run-1/EURUSD/H1/provenance.json"
        ),
        "sidecar_size_bytes": sidecar.stat().st_size,
        "sidecar_sha256": sidecar_sha,
    }
    native_bindings = {"EURUSD|H1": native_binding}
    activation_descriptor = {
        "role": "native_activation_manifest",
        "source_relative_path": activation_logical,
        "capsule_relative_path": f"closure/{activation_logical}",
        "size_bytes": len(activation_raw),
        "sha256": activation_sha,
    }
    capsule_body = {
        "schema_version": "fxpm.demo-management-authority.v1",
        "generation_id": "generation-captured",
        "source_root": str(source),
        "requested_symbols": ["EURUSD"],
        "selected_cell_ids": ["EURUSD|H1|TREND"],
        "native_activation_id": activation_id,
        "native_activation_manifest_sha256": activation_sha,
        "selected_native_source_bindings": native_bindings,
        "selected_native_source_bindings_sha256": authority._canonical_sha(
            native_bindings
        ),
        "source_snapshots": {
            "native_activation_manifest": activation_descriptor,
        },
        "external_closure": closure_values,
        "external_closure_sha256": authority._canonical_sha(closure_values),
        "permissions": {
            "open_new_positions": False,
            "increase_positions": False,
            "widen_stops": False,
            "tighten_stops": True,
            "reduce_positions": True,
            "close_positions": True,
        },
    }
    capsule_id = authority._canonical_sha(capsule_body)
    capsule_root = tmp_path / "store" / capsule_id
    activation_path = capsule_root / "closure" / activation_logical
    activation_path.parent.mkdir(parents=True)
    activation_path.write_bytes(activation_raw)
    manifest_path = capsule_root / "closure" / "pm_configs.generation.json"
    manifest_path.write_text("{}", encoding="utf-8")
    _write(capsule_root / "capsule.json", {**capsule_body, "capsule_id": capsule_id})
    closure_context = {
        closure_id: {
            **body,
            "absolute_path": str(source / body["logical_relative_path"]),
        }
        for closure_id, body in closure_values.items()
    }
    context = {
        "schema_version": authority.MANAGEMENT_CONTEXT_SCHEMA,
        "capsule_id": capsule_id,
        "generation_id": "generation-captured",
        "scope": authority.MANAGEMENT_STATUS,
        "allow_expired_entry_management": True,
        "allow_inactive_generation_pointer": True,
        "allow_new_entries": False,
        "allow_position_increase": False,
        "allow_stop_widening": False,
        "captured_source_root": str(source),
        "captured_activation_manifest_path": str(activation_path),
        "captured_activation_manifest_sha256": activation_sha,
        "external_closure": closure_context,
        "selected_native_source_bindings": native_bindings,
    }
    return context, manifest_path, bars, activation_id, activation_sha


def _management_regime_bundle(
    *,
    cutoff: str,
    activation_id: str,
    activation_sha: str,
    bar_digests: dict[str, str],
):
    sources = {}
    fits = {}
    envelopes = []
    for timeframe in ("M5", "D1"):
        source = create_historical_source_group_decision(
            symbol="EURUSD",
            timeframe=timeframe,
            availability=AVAILABLE,
            reason_code="NATIVE_SOURCE_AVAILABLE",
            source_contract_sha256=_digest(f"source-contract-EURUSD-{timeframe}"),
            source_group_evidence_sha256=_digest(f"source-evidence-EURUSD-{timeframe}"),
            acquisition_attempts_exact=1,
            activation_id=activation_id,
            activation_manifest_sha256=activation_sha,
            native_source_sha256=bar_digests[timeframe],
        )
        assert source is not None
        code_sha = _digest(f"detector-code-EURUSD-{timeframe}")
        record = create_regime_detector_record(
            source_group_decision=source,
            fit_cutoff_utc=cutoff,
            detector_version="fxpm-regime-live-v1",
            detector_code_sha256=code_sha,
            parameters=_detector_parameters(),
        )
        assert record is not None
        support = _train_support(
            "EURUSD",
            timeframe,
            activation_id=activation_id,
            manifest_sha=activation_sha,
            source_sha=bar_digests[timeframe],
            params_sha=record["parameters_sha256"],
            code_sha=code_sha,
        )
        fit = create_regime_fit_decision(
            source_group_decision=source,
            fit_state=REGIME_FITTED,
            reason_code="CURRENT_SHARED_REGIME_FIT_COMPLETE",
            regime_fit_policy_sha256=_digest(f"fit-policy-EURUSD-{timeframe}"),
            detector_process_sha256=code_sha,
            regime_fit_evidence_sha256=_digest(f"fit-evidence-EURUSD-{timeframe}"),
            four_regime_mapping_sha256=_digest(f"regime-map-EURUSD-{timeframe}"),
            regime_params_sha256=record["parameters_sha256"],
            regime_detector_record=record,
            train_regime_support=support,
        )
        assert fit is not None
        fit_value = fit.to_dict()
        fit_sha = fit_value.pop("receipt_sha256")
        envelopes.append({"payload": fit_value, "receipt_sha256": fit_sha})
        sources[timeframe] = source
        fits[timeframe] = fit
    return (
        build_current_regime_params_bundle(
            current_cutoff_utc=cutoff,
            regime_fits=envelopes,
        ),
        sources,
        fits,
    )


def _management_book_decision(
    book_entries: dict[str, dict],
    sources: dict,
    fits: dict,
) -> dict:
    body = evaluate_historical_book_multiplicity(_terminal_family()).to_dict()
    body.pop("receipt_sha256")
    winner_ordinals = set()
    for timeframe in ("M5", "D1"):
        group = book.PINNED_SYMBOL_ORDER.index("EURUSD") * len(book.TIMEFRAME_ORDER)
        group += book.TIMEFRAME_ORDER.index(timeframe)
        source = sources[timeframe]
        fit = fits[timeframe]
        source_value = source.to_dict()
        source_sha = source_value.pop("receipt_sha256")
        fit_value = fit.to_dict()
        fit_sha = fit_value.pop("receipt_sha256")
        body["source_groups"][group] = {
            "payload": source_value,
            "receipt_sha256": source_sha,
        }
        body["regime_fits"][group] = {
            "payload": fit_value,
            "receipt_sha256": fit_sha,
        }
        start = group * len(REGIME_ORDER)
        for offset, regime in enumerate(REGIME_ORDER):
            ordinal = start + offset
            if regime == "TREND":
                body["cells"][ordinal] = copy.deepcopy(book_entries[timeframe])
                winner_ordinals.add(ordinal)
                continue
            state = create_historical_process_state_decision(
                symbol="EURUSD",
                timeframe=timeframe,
                regime=regime,
                state_kind=book.REGIME_UNOBSERVED,
                reason_code=book.REGIME_UNOBSERVED_REASON,
                source_group_decision=source,
                regime_fit_decision=fit,
                attempted_trials_exact=0,
            )
            assert state is not None
            evidence = process_state_cell_evidence(
                process_state_decision=state,
                source_group_decision=source,
                regime_fit_decision=fit,
            )
            assert evidence is not None
            specific = book._state_record(evidence, ordinal, source, fit)
            record = {
                "ordinal": ordinal,
                "cell_id": book.compute_cell_id("EURUSD", timeframe, regime),
                "symbol": "EURUSD",
                "timeframe": timeframe,
                "regime": regime,
                "source_group_id": source_value["source_group_id"],
                "source_group_decision_sha256": source_sha,
                "regime_receipt_sha256": fit_sha,
                "regime_detector_record_sha256": fit_value[
                    "regime_detector_record_sha256"
                ],
                "train_regime_support_receipt_sha256": fit_value[
                    "train_regime_support"
                ]["receipt_sha256"],
                **specific,
            }
            body["cells"][ordinal] = {
                **record,
                "adjusted_q_value": 1.0,
                "by_qualified": False,
                "historical_local_candidate_pending_causal": False,
                "historical_book_entry_sha256": book._entry_identity(record),
            }

    q_values = book.adjust_p_values_by(
        [cell["marginal_p_value"] for cell in body["cells"]]
    )
    assert q_values is not None
    for ordinal, (cell, q_value) in enumerate(
        zip(body["cells"], q_values, strict=True)
    ):
        winner = ordinal in winner_ordinals
        cell["adjusted_q_value"] = q_value
        cell["by_qualified"] = winner and q_value <= book.TARGET_FDR
        cell["historical_local_candidate_pending_causal"] = winner
    candidate_ids = [
        body["cells"][ordinal]["cell_id"] for ordinal in sorted(winner_ordinals)
    ]
    body.update(
        status="HISTORICAL_LOCAL_CANDIDATES_AVAILABLE",
        valid=True,
        reason_codes=["LOCAL_WINNERS_REQUIRE_SEPARATE_CAUSAL_PROCESS_VALIDATION"],
        optimization_run_complete=True,
        counts={
            state: sum(cell["historical_state"] == state for cell in body["cells"])
            for state in body["counts"]
        },
        source_group_counts={
            state: sum(
                item["payload"]["availability"] == state
                for item in body["source_groups"]
            )
            for state in body["source_group_counts"]
        },
        regime_fit_counts={
            state: sum(
                item["payload"]["fit_state"] == state for item in body["regime_fits"]
            )
            for state in body["regime_fit_counts"]
        },
        diagnostic_by_qualified_count=sum(
            cell["by_qualified"] for cell in body["cells"]
        ),
        historical_local_candidate_count=len(candidate_ids),
        historical_local_candidate_cell_ids=candidate_ids,
    )
    body["book_generation_sha256"] = authority._canonical_sha(
        {
            "family_precommitment_sha256": body["family_precommitment_sha256"],
            "method": body["method"],
            "selection_rule": body["selection_rule"],
            "target_fdr": body["target_fdr"],
            "source_group_receipts": [
                item["receipt_sha256"] for item in body["source_groups"]
            ],
            "regime_fit_receipts": [
                item["receipt_sha256"] for item in body["regime_fits"]
            ],
            "historical_book_entry_sha256": [
                cell["historical_book_entry_sha256"] for cell in body["cells"]
            ],
        }
    )
    raw = json.dumps(
        body,
        allow_nan=False,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("ascii")
    decision = BookMultiplicityDecision(raw, hashlib.sha256(raw).hexdigest())
    assert verify_historical_book_multiplicity_decision(decision)
    return decision.to_dict()


def test_unmocked_chain_invokes_real_verifiers_and_fails_closed(tmp_path: Path):
    result = authority.verify_live_readiness_authority_chain(**_fixture(tmp_path))

    assert set(result) == authority._RESULT_FIELDS
    assert result["status"] == "BLOCKED"
    assert result["funded_live_authorized"] is False
    assert result["raw_reverification"]["historical_book"] is True
    assert result["raw_reverification"]["causal_process"] is False
    assert result["raw_reverification"]["current_refit_execution"] is False
    assert "RAW_CURRENT_REFIT_MANIFEST_FIELDS_INVALID" in result["errors"]
    assert any("PORTFOLIO" in error for error in result["errors"])


@pytest.mark.parametrize(
    "raw",
    [b'{"x":1,"x":2}', b'{"x":NaN}', b'{"x":1e999}'],
)
def test_strict_snapshot_rejects_duplicate_and_nonfinite_json(
    tmp_path: Path, raw: bytes
):
    path = tmp_path / "bad.json"
    path.write_bytes(raw)

    with pytest.raises(authority._Invalid):
        authority._snapshot(path, 1024, root=tmp_path)


def test_checksum_receipt_cannot_mint_demo_authority(tmp_path: Path):
    fixture = _fixture(tmp_path)
    result = authority.verify_live_readiness_authority_chain(**fixture)

    assert result["errors"]
    assert not all(result["raw_reverification"].values())
    assert result["status"] != "DEMO_PROBATION_AUTHORIZED"


def test_authority_relative_paths_reject_traversal(tmp_path: Path):
    outside = tmp_path.parent / "outside.json"
    outside.write_text("{}", encoding="utf-8")

    with pytest.raises(authority._Invalid):
        authority._relative_path(tmp_path, "../outside.json", "TEST")


def test_snapshot_rejects_symlink_or_reparse(tmp_path: Path):
    target = tmp_path / "target.json"
    target.write_text("{}", encoding="utf-8")
    link = tmp_path / "link.json"
    try:
        link.symlink_to(target)
    except OSError:
        pytest.skip("symlink creation unavailable")

    with pytest.raises(authority._Invalid):
        authority._snapshot(link, 1024, root=tmp_path)


def test_snapshot_rejects_pre_open_file_swap(tmp_path: Path, monkeypatch):
    target = tmp_path / "target.json"
    replacement = tmp_path / "replacement.json"
    target.write_text('{"version":1}', encoding="utf-8")
    replacement.write_text('{"version":200}', encoding="utf-8")
    original_open = Path.open
    swapped = False

    def swapping_open(self, *args, **kwargs):
        nonlocal swapped
        if self == target and not swapped:
            swapped = True
            replacement.replace(target)
        return original_open(self, *args, **kwargs)

    monkeypatch.setattr(Path, "open", swapping_open)
    with pytest.raises(authority._Invalid, match="FILE_CHANGED_DURING_READ"):
        authority._snapshot(target, 1024, root=tmp_path)


def test_management_context_rehashes_capsule_manifest_and_selected_native_files(
    tmp_path: Path,
):
    context, manifest_path, _bars, activation_id, activation_sha = _management_fixture(
        tmp_path
    )
    state, errors = authority._management_context(
        context,
        manifest_path=manifest_path,
        requested_symbols=("EURUSD",),
    )
    assert errors == []
    universe, errors = authority._management_native_reverify(
        state=state,
        requested_symbols=("EURUSD",),
        supplied_universe=SimpleNamespace(
            activation_id=activation_id,
            activation_manifest_sha256=activation_sha,
        ),
    )
    assert errors == []
    assert universe.cell_status("EURUSD", "H1") == "evidence_candidate"
    state["closure"].require_complete()
    state["closure"].verify_all()


def test_management_context_preserves_terminal_technical_without_laundering(
    tmp_path: Path,
):
    context, manifest_path, _bars, activation_id, activation_sha = _management_fixture(
        tmp_path, technical_timeframe="M5"
    )
    state, errors = authority._management_context(
        context,
        manifest_path=manifest_path,
        requested_symbols=("EURUSD",),
    )
    assert errors == []

    universe, errors = authority._management_native_reverify(
        state=state,
        requested_symbols=("EURUSD",),
        supplied_universe=SimpleNamespace(
            activation_id=activation_id,
            activation_manifest_sha256=activation_sha,
        ),
    )

    assert errors == []
    assert universe.cell_status("EURUSD", "M5") == "technical_error"
    assert universe.cell_status("EURUSD", "H1") == "evidence_candidate"
    assert universe.eligible_symbols == ("EURUSD",)
    state["closure"].require_complete()
    state["closure"].verify_all()


def test_regime_support_does_not_require_detector_for_terminal_technical_cell():
    native = SimpleNamespace(
        activation_id="activation-terminal",
        activation_manifest_sha256="a" * 64,
        entries={"EURUSD": {}},
        cell_status=lambda _symbol, timeframe: (
            "technical_error" if timeframe == "M5" else "source_unavailable"
        ),
    )
    bindings, errors = authority._regime_support_reverify(
        index_value={
            "schema_version": authority.INDEX_SCHEMAS["regime_support"],
            "current_book_multiplicity_receipt_sha256": "b" * 64,
            "native_activation_id": native.activation_id,
            "native_activation_manifest_sha256": (native.activation_manifest_sha256),
            "detector_ledger_records": [],
        },
        current_book_body={"regime_fits": []},
        current_book_receipt="b" * 64,
        requested_symbols=("EURUSD",),
        native_universe=native,
    )

    assert errors == []
    assert bindings == {}


def test_book_partition_accepts_only_distinct_terminal_technical_state():
    cells = [
        {
            "symbol": "EURUSD",
            "timeframe": timeframe,
            "regime": regime,
            "historical_state": (
                book.TERMINAL_TECHNICAL
                if timeframe == "M5"
                else book.INSUFFICIENT_EVIDENCE
                if timeframe == "M15"
                else "SOURCE_UNAVAILABLE"
            ),
        }
        for timeframe in authority.TIMEFRAMES
        for regime in authority.REGIMES
    ]
    partition, errors = authority._book_partition(  # noqa: SLF001
        {
            "valid": True,
            "optimization_run_complete": True,
            "cells": cells,
        },
        ("EURUSD",),
    )
    assert errors == []
    assert partition["technical_cell_ids"] == {
        f"EURUSD|M5|{regime}" for regime in authority.REGIMES
    }
    assert partition["insufficient_evidence_cell_ids"] == {
        f"EURUSD|M15|{regime}" for regime in authority.REGIMES
    }

    cells[0]["historical_state"] = book.TECHNICAL_FAILURE
    _, errors = authority._book_partition(  # noqa: SLF001
        {
            "valid": True,
            "optimization_run_complete": True,
            "cells": cells,
        },
        ("EURUSD",),
    )
    assert any("CURRENT_BOOK_CELL_STATE_INVALID" in error for error in errors)


def test_incomplete_current_book_with_winner_and_unsearched_cannot_authorize():
    unsearched = _available_state_cell(1, book.UNSEARCHED, attempts=0)
    decision = evaluate_historical_book_multiplicity(
        _family(
            available_groups=(0,),
            overrides={0: _verified_winner(), 1: unsearched},
        )
    )
    assert verify_historical_book_multiplicity_decision(decision)
    body = decision.to_dict()
    assert body["valid"] is False
    assert body["optimization_run_complete"] is False

    partition, errors = authority._book_partition(  # noqa: SLF001
        body,
        ("AUDUSD",),
    )
    assert partition["selected_cell_ids"] == {"AUDUSD|M5|TREND"}
    assert partition["unsearched_cell_ids"] == {"AUDUSD|M5|RANGE"}
    assert "CURRENT_BOOK_NOT_COMPLETE_VALID" in errors
    assert "CURRENT_BOOK_UNSEARCHED_CELLS_PRESENT" in errors


def test_current_book_raw_reverify_rejects_self_consistent_incomplete_book():
    unsearched = _available_state_cell(1, book.UNSEARCHED, attempts=0)
    decision = evaluate_historical_book_multiplicity(
        _family(
            available_groups=(0,),
            overrides={0: _verified_winner(), 1: unsearched},
        )
    )
    body = decision.to_dict()

    def bind_book(payload):
        payload["book_multiplicity_receipt_sha256"] = body["receipt_sha256"]
        payload["full_cell_outcome_roster_sha256"] = body["book_generation_sha256"]
        payload["selected_winner_count"] = sum(
            cell["historical_state"] == "WINNER" for cell in body["cells"]
        )
        payload["precommitted_cell_count"] = len(body["cells"])

    process, accepted, _ = _process_refit_receipts(book_change=bind_book)
    _, verified_body, _, errors = authority._current_book_reverify(  # noqa: SLF001
        {
            "schema_version": authority.INDEX_SCHEMAS["current_process_book"],
            "accepted_book_generation_receipt": accepted,
            "current_book_decision": body,
            "historical_process_decision_sha256": canonical_receipt_sha256(process),
        },
        causal_decision=process,
    )

    assert verified_body["valid"] is False
    assert verified_body["optimization_run_complete"] is False
    assert "CURRENT_BOOK_NOT_COMPLETE_VALID" in errors


def test_management_context_rejects_external_file_change(tmp_path: Path):
    context, manifest_path, bars, _activation_id, _activation_sha = _management_fixture(
        tmp_path
    )
    bars.write_bytes(b"changed")

    state, errors = authority._management_context(
        context,
        manifest_path=manifest_path,
        requested_symbols=("EURUSD",),
    )

    assert state is None
    assert "MANAGEMENT_EXTERNAL_CLOSURE_HASH_MISMATCH" in errors


def test_management_context_rejects_extra_or_escaped_closure(tmp_path: Path):
    context, manifest_path, _bars, _activation_id, _activation_sha = (
        _management_fixture(tmp_path)
    )
    escaped = {
        "role": "native_bars",
        "logical_relative_path": "../outside.csv",
        "absolute_path": str(tmp_path / "outside.csv"),
        "size_bytes": 1,
        "sha256": "a" * 64,
    }
    context["external_closure"][
        authority._canonical_sha(
            {
                key: escaped[key]
                for key in ("role", "logical_relative_path", "size_bytes", "sha256")
            }
        )
    ] = escaped

    state, errors = authority._management_context(
        context,
        manifest_path=manifest_path,
        requested_symbols=("EURUSD",),
    )

    assert state is None
    assert any("MANAGEMENT_" in error for error in errors)


def test_invalid_management_context_never_falls_back_to_entry_authority(
    tmp_path: Path,
):
    fixture = _fixture(tmp_path)
    fixture["management_only_context"] = {}

    result = authority.verify_live_readiness_authority_chain(**fixture)

    assert result["status"] == "BLOCKED"
    assert result["funded_live_authorized"] is False
    assert result["raw_reverification"]["native_data_state"] is False
    assert "MANAGEMENT_NATIVE_CONTEXT_UNAVAILABLE" in result["errors"]
