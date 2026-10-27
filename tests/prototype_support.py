"""Finite synthetic prototype fixtures; no market, OWNER or trading authority.

Only the moved prototype fixtures live here. The legacy native generator stays
byte-pinned; normal imports reuse its existing constant instead of copying it.
"""
from __future__ import annotations
from copy import deepcopy
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
from types import SimpleNamespace
import numpy as np
import pandas as pd
from native_v5_support import SYMBOL
import pm_core
import pm_authority_spool as spool
import pm_authority_spool as authority_spool
import pm_causal_evidence_producer as causal
import pm_forward_oos_replay_producer as forward
import pm_native_prefix_production as native_prefix
import pm_historical_anchor_orchestration as orchestration
import pm_historical_search_producer as producer
import pm_production_anchor_plan as anchor_plan
import pm_production_search_planner as planner
from pm_current_refit_parameters import canonicalize_parameter_mapping
from pm_spread_profile import FrozenSpreadProfile


CELL_ID = "8a8d94e173d45705d415b48e6cafe39d068988884ad769401534cf19868fed38"

LAUNCH = "2023-03-05T00:01:00Z"

PROTECTED_END = "2024-07-14T08:05:00Z"

def _roster_arguments(config_sha="a" * 64, effective_sha="b" * 64):
    return {
        "launch_activation_utc": LAUNCH,
        "frozen_refit_clock": {
            "anchor_activation_utc": "2026-03-29T00:01:00Z",
            "interval_calendar_days": 28,
        },
        "config_identity_refs": {
            "config_sha256": config_sha,
            "effective_config_sha256": effective_sha,
        },
        "extend_frozen_clock_backward": True,
        "protected_end_exclusive_utc": PROTECTED_END,
    }

class _DeclaredClock(datetime):
    value = datetime(2026, 10, 22, 12, tzinfo=timezone.utc)

    @classmethod
    def now(cls, tz=None):
        assert tz is timezone.utc
        return cls.value

def _declared_environment(tmp_path, monkeypatch, *, candidate_risk=1.0, issue=True, native_snapshot=None):
    """Real metadata issuers on invented config; no native bars or Winner."""
    import pm_production_config_binding as binding
    import pm_authority_spool as spool
    from test_production_config_binding import _context, _payload, _write

    payload = _payload(tmp_path)
    payload["symbols"] = ["AUDUSD"]
    payload["pipeline"].update(
        research_mode=True, initial_capital=10000.0,
        discovery_risk_per_trade_pct=candidate_risk, risk_per_trade_pct=0.25,
    )
    path = tmp_path / "declared-synthetic-config.json"
    _write(path, payload)
    owned, effective = binding.load_effective_pipeline_config(path)
    if native_snapshot is None:
        context = _context(tmp_path, payload)
    else:
        import pm_portfolio_replay as portfolio_replay
        context = spool.freeze_generation_context(
            (tmp_path / "spool").resolve(), generation_id="production-config-binding-test",
            native_universe_sha256=portfolio_replay.canonical_json_sha256(native_snapshot),
            config_sha256=portfolio_replay.canonical_json_sha256(payload),
        )
    frozen = binding.freeze_production_pipeline_config(
        context, active_config_path=path, config_payload=owned,
        supplied_config=effective,
    )
    run_id = "declared-slot-qa-run"
    process = planner.freeze_production_search_process(context, run_id=run_id)
    roster = anchor_plan.derive_declared_owner_slot_roster(**_roster_arguments(
        context.config_sha256, frozen.effective_config_sha256,
    ))
    monkeypatch.setattr(_DeclaredClock, "value", datetime(2026, 10, 22, 12, tzinfo=timezone.utc))
    monkeypatch.setattr(orchestration, "datetime", _DeclaredClock)
    monkeypatch.setattr(producer, "datetime", _DeclaredClock)
    arguments = {
        "run_id": run_id, "process": process, "slot_roster": roster,
        "launch_activation_utc": LAUNCH,
        "cell_roster": [{"cell_id": CELL_ID, "symbol": "AUDUSD", "timeframe": "H1", "regime": "TREND"}],
        "frozen_pipeline_config": frozen,
        "spread_profile_identity": {"profile_sha256": "e" * 64},
        "source_access_window_utc": {
            "start_utc": "2026-10-22T11:00:00Z",
            "end_exclusive_utc": "2026-10-22T13:00:00Z",
        },
    }
    precommit = orchestration.precommit_declared_slot_owner_run(context, **arguments) if issue else None
    return SimpleNamespace(
        context=context, process=process, frozen=frozen, roster=roster,
        precommit=precommit, arguments=arguments, config_path=path,
        sink=spool.open_symbol_sink(context, "AUDUSD"),
    )

def _sha(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()

def _write_json(path: Path, value: object) -> str:
    path.parent.mkdir(parents=True, exist_ok=True)
    raw = (json.dumps(value, indent=2, sort_keys=True) + "\n").encode("utf-8")
    path.write_bytes(raw)
    return _sha(raw)

def _execution_config(
    tmp_path: Path,
    *,
    symbol: str = SYMBOL,
    tick_size: float = 0.00001,
    broker_overrides: dict[str, object] | None = None,
) -> Path:
    broker = {
        symbol: {
            "symbol": symbol,
            "broker_symbol": symbol,
            "asset_class": "fx",
            "path": f"Forex\\Main\\{symbol}",
            "digits": 5,
            "point": 0.00001,
            "tick_size": tick_size,
            "tick_value": 1.25,
            "contract_size": 100_000.0,
            "volume_min": 0.01,
            "volume_max": 500.0,
            "volume_step": 0.01,
            "spread": 25,
            "swap_long": -3.0,
            "swap_short": 1.0,
            "swap_mode": 1,
            "swap_rollover3days": 3,
            "stops_level": 0,
            "pip_size": 0.0001,
        }
    }
    broker[symbol].update(broker_overrides or {})
    _write_json(tmp_path / "broker_specs.json", broker)
    config = {
        "_meta": {"scope": "funded_live_v1"},
        "broker_specs_path": "broker_specs.json",
    }
    path = tmp_path / "config.json"
    _write_json(path, config)
    return path

def _sealed_identity_only_product(
    *, symbol: str, timeframe: str, cutoff_utc: str
) -> native_prefix.NativePrefixProduct:
    def descriptor(format_name: str, seed: str, size: int = 10) -> dict:
        return {
            "schema_version": authority_spool.ARTIFACT_DESCRIPTOR_SCHEMA,
            "relative_path": f"objects/{seed}",
            "sha256": forward._sha({"seed": seed}),
            "size_bytes": size,
            "format": format_name,
        }

    content = descriptor("csv", "prefix-content", 100)
    index = {
        "schema_version": native_prefix.PREFIX_INDEX_SCHEMA,
        "authority_scope": native_prefix.PREFIX_AUTHORITY_SCOPE,
        "source_state": native_prefix.AVAILABLE,
        "source_reason": None,
        "symbol": symbol,
        "timeframe": timeframe,
        "cutoff_utc": cutoff_utc,
        "source_accessed_at_utc": "2026-02-01T00:00:01Z",
        "activation": {
            "activation_id": "identity-only-activation",
            "manifest_relative_path": "activations/identity-only.json",
            "manifest_sha256": "1" * 64,
            "entry_sha256": "2" * 64,
            "run_manifest_relative_path": "runs/identity-only/run.json",
            "run_manifest_sha256": "3" * 64,
        },
        "source": {
            "bars_relative_path": "runs/identity-only/bars.csv",
            "bars_sha256": "4" * 64,
            "bars_bytes": 100,
            "bars_rows": 2,
            "audit_sidecar_relative_path": "audits/identity-only.json",
            "audit_sidecar_sha256": "5" * 64,
            "audit_sidecar_bytes": 10,
            "provenance_relative_path": "runs/identity-only/provenance.json",
            "provenance_sha256": "6" * 64,
            "provenance_bytes": 10,
            "request_map_sha256": "7" * 64,
            "source_identity_sha256": "8" * 64,
            "source_identity": {"account": "frozen"},
            "instrument_snapshot": {},
            "request_start_utc": "2026-01-01T00:00:00Z",
            "request_end_exclusive_utc": cutoff_utc,
        },
        "prefix": {
            "schema": descriptor("json", "prefix-schema"),
            "content": content,
            "segments": descriptor("jsonl", "prefix-segments"),
            "prefix_rows": 2,
            "prefix_start_utc": "2026-01-01T00:00:00Z",
            "prefix_end_exclusive_utc": cutoff_utc,
            "prefix_first_open_utc": "2026-01-01T00:00:00Z",
            "prefix_last_open_utc": "2026-01-31T23:55:00Z",
            "prefix_last_close_utc": cutoff_utc,
        },
        "continuity": {
            "policy": descriptor("json", "continuity-policy"),
            "receipt": descriptor("json", "continuity-receipt"),
            "geometry_sha256": "9" * 64,
            "internal_status": "CONTINUOUS",
            "boundary_status": "COMPLETE",
            "unexplained_discontinuity_count": 0,
            "segment_count": 1,
            "gap_count": 0,
            "requires_preserved_segment_ledger": True,
        },
        "terminal_receipt": None,
        "grants_search_authority": False,
        "grants_trading_authority": False,
    }
    raw = forward._canonical_bytes(index)
    index_sha = forward.hashlib.sha256(raw).hexdigest()
    index_descriptor = {
        "schema_version": authority_spool.ARTIFACT_DESCRIPTOR_SCHEMA,
        "relative_path": "objects/prefix-index",
        "sha256": index_sha,
        "size_bytes": len(raw),
        "format": authority_spool.CANONICAL_JSON_FORMAT,
    }
    return native_prefix.NativePrefixProduct(
        source_state=native_prefix.AVAILABLE,
        index=index,
        index_descriptor=index_descriptor,
        index_sha256=index_sha,
        native_prefix=native_prefix._native_prefix_from_index(index, index_sha),
        instrument_snapshot={},
        prefix_path=None,
        prefix_descriptor=content,
        artifact_descriptors=(index_descriptor, content),
        _seal=native_prefix._PRODUCT_SEAL,
    )

def _synthetic_profile(tmp_path, *, symbol="EURUSD", timeframe="H1", source_timeframe="M5", identity=None):
    """Invented thresholds for execution conformance, not market estimates."""
    identity = identity or {"source_identity_sha256": "1" * 64, "source_bars_sha256": "2" * 64,
        "source_audit_sidecar_sha256": "3" * 64, "source_provenance_sha256": "4" * 64,
        "activation_id": "synthetic-cost-conformance", "activation_manifest_sha256": "5" * 64}
    body = {"schema_version": "fxpm.frozen-spread-profile.v1",
        "status": "COMPLETE_CALIBRATION_WITH_EXPLICIT_UNAVAILABLE_GROUPS",
        "calculation_policy": "ONE_TIME_OFFLINE_NO_RUNTIME_RECALCULATION",
        "source_convention": "FINEST_ACTIVATED_CANDIDATE_FIXED_PER_SYMBOL_EXACT_PRIOR_CLOSE_AT_TARGET_OPEN",
        "production_authority": False, "protected_cutoff_utc": "2026-02-01T00:00:00Z",
        "activation": {"id": identity["activation_id"], "manifest_sha256": identity["activation_manifest_sha256"]},
        "cells": [{"symbol": symbol, "timeframe": timeframe, "status": "CALCULATED", "point_price": 0.00001,
            "source_timeframe": source_timeframe, "source_identity": {"identity_sha256": identity["source_identity_sha256"]},
            "source_binding": {"bars_relative_path": "synthetic/bars.csv", "bars_sha256": identity["source_bars_sha256"],
                "sidecar_sha256": identity["source_audit_sidecar_sha256"], "provenance_sha256": identity["source_provenance_sha256"],
                "target_bars_relative_path": "synthetic/bars.csv", "target_bars_sha256": identity["source_bars_sha256"]},
            "tiers": {"P75": {"spread_points": 9, "full_spread_price": 9 * 0.00001,
                              "half_spread_price": 9 * 0.00001 / 2, "usability": "POSITIVE_QUANTILE"},
                      "P95": {"spread_points": 16, "full_spread_price": 16 * 0.00001,
                              "half_spread_price": 16 * 0.00001 / 2, "usability": "POSITIVE_QUANTILE"}}}]}
    path = tmp_path / "synthetic_frozen_profile.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(body), encoding="utf-8")
    profile = FrozenSpreadProfile(path)
    return {level: profile.resolve(symbol, timeframe, level, point=0.00001,
            source_identity_sha256=identity["source_identity_sha256"]) for level in ("P75", "P95")}

def _synthetic_reopened_declared_prefix(tmp_path, monkeypatch, edition, *, missing_only=False):
    """Physical commits with an exact, labelled synthetic raw-authentication seam.

    The real metadata issuer, request validator, edition gate, commit writer,
    cold progress, account input reopener and account transport stay in use.
    Upstream raw OWNER and native/spec authentication are invented. The MISSING
    mode also emits only the thirteen predeclared absences through the actual
    driver. These finite seams claim no genuine OWNER or real native absence.
    Newly invented :01 bars support this structural writer projection only:
    real :00 bars fail the old roster's terminal guard. No clock policy follows.
    """
    from pathlib import Path
    import test_production_config_binding as config_fixture
    import pm_historical_anchor_orchestration as orchestration
    import pm_historical_production_adapter as adapter
    import pm_historical_search_producer as producer
    import pm_native_prefix_production as native
    from pm_regime import RegimeParams, regime_params_sha256
    from prototype_support import _declared_environment
    from prototype_support import _sealed_identity_only_product, _synthetic_profile
    from prototype_support import _execution_config

    assert edition in ("v15", "v16")
    classification = "QA142_SYNTHETIC_COMMIT_AUTH_NOT_RAW_OWNER_PROOF"
    root = tmp_path / ("synthetic-reopened-" + edition)
    root.mkdir(parents=True)
    _execution_config(root, symbol="AUDUSD")  # Existing invented broker bytes.
    original_payload = config_fixture._payload

    def payload(path):
        value = original_payload(path)
        value["broker_specs_path"] = "broker_specs.json"
        value["pipeline"]["swap_model_enabled"] = False
        return value

    # Build the normal real issuer on a declared test payload, not a config stub.
    with monkeypatch.context() as payload_patch:
        payload_patch.setattr(config_fixture, "_payload", payload)
        env = _declared_environment(root, monkeypatch, issue=False)
    config = env.frozen.materialize(env.context)
    slots, cell = env.roster["slots"], env.arguments["cell_roster"][0]
    start = pd.Timestamp(slots[0]["activation_utc"])
    end = pd.Timestamp(slots[-1]["end_exclusive_utc"])
    cutoff = end.strftime("%Y-%m-%dT%H:%M:%SZ")
    # Newly invented bars: no existing bars are shifted to this nominal :01 grid.
    selection_cutoff = pd.Timestamp(slots[0]["native_cutoff_utc"])
    prefix_index = pd.date_range(end=selection_cutoff - pd.Timedelta(hours=1), periods=220, freq="h")
    index = prefix_index.append(pd.DatetimeIndex([
        start, start + pd.Timedelta(hours=1), end - pd.Timedelta(hours=2), end - pd.Timedelta(hours=1)]))
    frame = pd.DataFrame({"Open": 1.0, "High": 1.0002, "Low": 0.9998, "Close": 1.0,
        "Volume": 1.0, "Spread": 3.0, "_BAR_CLOSE_TS": index + pd.Timedelta(hours=1)}, index=index)
    fine_index = (index - pd.Timedelta(minutes=5)).append(pd.DatetimeIndex([end - pd.Timedelta(minutes=5)]))
    fine_frame = pd.DataFrame({"Open": 1.0, "High": 1.0002, "Low": 0.9998, "Close": 1.0,
        "Spread": 3.0, "_BAR_CLOSE_TS": fine_index + pd.Timedelta(minutes=5)}, index=fine_index)
    products = {tf: _sealed_identity_only_product(symbol=cell["symbol"], timeframe=tf, cutoff_utc=cutoff)
        for tf in ("H1", "M5")}
    identities = {tf: forward._native_product_identity(product, expected_symbol=cell["symbol"],
        expected_timeframe=tf, expected_cutoff_utc=cutoff,
        role="DECLARED_CELL_EXECUTION" if tf == "H1" else "DECLARED_CELL_SPREAD")
        for tf, product in products.items()}
    terms = Path(config.data_dir) / "native" / "terms"
    tiers = _synthetic_profile(terms, symbol=cell["symbol"], identity=identities["M5"])
    profile_path = terms / "spread_profile.json"
    (terms / "synthetic_frozen_profile.json").rename(profile_path)
    env.arguments["spread_profile_identity"] = {"profile_sha256": tiers["P75"].descriptor()["profile_sha256"]}
    issuer = (orchestration.precommit_declared_slot_owner_run_v2 if edition == "v16"
        else orchestration.precommit_declared_slot_owner_run)
    env.precommit = issuer(env.context, **env.arguments)
    parameters = RegimeParams().to_dict()
    winner_process = {"strategy_id": "EMACrossoverStrategy", "strategy_class": "EMACrossoverStrategy",
        "strategy_module": "pm_strategies"}
    selected = {"strategy_id": winner_process["strategy_id"], "process_candidate_id": "QA142_SYNTHETIC_FIXED_PROCESS",
        "process_sha256": forward._sha(winner_process), "encoded_parameters": canonicalize_parameter_mapping(
            forward.StrategyRegistry.get(winner_process["strategy_id"]).get_default_params())}
    owner = {"classification": classification,
        "owner_id": orchestration.declared_slot_owner_id(cell_id=cell["cell_id"], slot_id=slots[0]["slot_id"],
            winner_decision_sha256="f" * 64, process_receipt_sha256=env.process.receipt_sha256),
        "cell_id": cell["cell_id"], "slot_id": slots[0]["slot_id"], "selected_tier": "P75",
        "selected_tier_contract": {"frozen_spread_tier": tiers["P75"].descriptor()},
        "selected_artifacts": {"winner_process": winner_process}, "selected_attempt": selected,
        "regime_detector_record": {"parameters_canonical_json": json.dumps(parameters, sort_keys=True, separators=(",", ":")),
            "parameters_sha256": regime_params_sha256(parameters)}}
    count = {**cell, "fit_start_utc": prefix_index[0].isoformat(),
        "fit_end_exclusive_utc": selection_cutoff.isoformat(), "source_bar_count": len(prefix_index),
        "fit_frame_sha256": forward._frame_sha256(frame.loc[prefix_index]),
        "latest_closed_bar_utc": selection_cutoff.isoformat()}
    count["receipt_sha256"] = forward._sha(count)
    known, authentication_calls, commit_entries = {}, [], []

    def synthetic_completion_auth(context, *, precommit, process, completion, artifact_roster, frozen_pipeline_config):
        """Finite invented raw-auth corpus; reject unknown, extra or changed bytes."""
        assert context is env.context and process is env.process and frozen_pipeline_config is env.frozen
        row = known.get(completion.get("request_sha256"))
        assert row is not None and forward._canonical_bytes(completion) == row["completion_bytes"]
        assert list(artifact_roster) == row["artifacts"]
        assert completion["classification"] == classification
        sink = spool.open_symbol_sink(context, cell["symbol"])
        request = orchestration.DeclaredSlotProducerRequest(
            orchestration._declared_json(completion["request_body"]), precommit, sink, orchestration._DECLARED_SLOT_SEAL)
        orchestration._register_declared_request(request, context, live=False)
        try:
            body = orchestration.verify_declared_slot_producer_request(context, request,
                precommit=precommit, process=process)
            assert request.receipt_sha256 == completion["request_sha256"]
            assert body["cell_id"] == cell["cell_id"] and body["slot_id"] == completion["slot_id"]
            for entry in artifact_roster:
                descriptor = entry["descriptor"]
                digest, size, _ = spool._stream_file(context.root, descriptor["relative_path"],
                    maximum_bytes=descriptor["size_bytes"], expected_size=descriptor["size_bytes"], collect=False)
                assert digest == descriptor["sha256"] and size == descriptor["size_bytes"]
        finally:
            orchestration._release_declared_slot_producer_request(request, context)
        authentication_calls.append(completion["request_sha256"])
        return deepcopy(completion)

    monkeypatch.setattr(adapter, "verify_declared_slot_completion", synthetic_completion_auth)

    def prepare_completion(request, slot, status):
        body = orchestration.verify_declared_slot_producer_request(env.context, request,
            precommit=env.precommit, process=env.process)
        assert body["slot_id"] == slot["slot_id"] and body["anchor"]["ordinal"] == slot["ordinal"]
        assert body["cell_id"] == cell["cell_id"] and body["precommit_sha256"] == env.precommit.receipt_sha256
        assert request.receipt_sha256 not in known
        sink = request.symbol_sink(cell["symbol"])
        completion = producer._declared_base_completion(request, status=status,
            reason_code="SYNTHETIC_ABSENCE" if status == "MISSING" else None if status == "OWNER" else "SYNTHETIC_NO_TRADE",
            source_entry=None)
        completion["classification"] = classification
        completion["owner"] = owner if status == "OWNER" else None
        artifacts = []
        if status == "OWNER":
            namespace = request.to_dict()["artifact_namespace"]
            schema = adapter.raw_winner.V16_FROZEN_PLAN_SCHEMA if edition == "v16" else adapter.raw_winner.CURRENT_FROZEN_PLAN_SCHEMA
            frozen_entry = sink.put_opaque(namespace + "/invented-frozen-receipt", "synthetic_frozen_schema_only",
                (orchestration._declared_json({"schema_version": schema, "classification": classification}),),
                format=adapter.raw_winner.ASCII_JSON_FORMAT)
            cross_entry = sink.put_json(namespace + "/invented-cross", "synthetic_owner_mapping_not_raw_cross_proof",
                {"classification": classification, "mapping": [{"winner_process_candidate_id": selected["process_candidate_id"],
                    "process_sha256": selected["process_sha256"], "source_count_receipt": count}]})
            completion["frozen_plan_receipt"] = frozen_entry["descriptor"]
            completion["horizon_winner_cross_binding"] = cross_entry["descriptor"]
            artifacts = [frozen_entry, cross_entry]
        known[request.receipt_sha256] = {"completion_bytes": forward._canonical_bytes(completion),
            "completion": deepcopy(completion), "artifacts": deepcopy(artifacts), "slot": slot}
        return completion, artifacts

    driver_calls = []
    if missing_only:
        def emit_declared_missing(context, *, request, process, frozen_pipeline_config):
            """Invented source absence only, keyed to this driver's real issued requests."""
            assert context is env.context and process is env.process and frozen_pipeline_config is env.frozen
            body = orchestration.verify_declared_slot_producer_request(context, request,
                precommit=env.precommit, process=process)
            orchestration._verify_declared_request_identity(request, context, require_live=True)
            ordinal = len(driver_calls) + 1
            assert 1 <= ordinal <= 13 and body["anchor"]["ordinal"] == ordinal
            assert body["slot_id"] == slots[ordinal - 1]["slot_id"]
            completion, artifacts = prepare_completion(request, slots[ordinal - 1], "MISSING")
            assert artifacts == []
            driver_calls.append(body["slot_id"])
            return completion

        monkeypatch.setattr(producer, "execute_declared_slot_owner_search", emit_declared_missing)
        run_receipt = producer.execute_declared_cell_owner_slots(context=env.context, precommit=env.precommit,
            process=env.process, frozen_pipeline_config=env.frozen, cell_id=cell["cell_id"]).to_dict()
        assert driver_calls == [slot["slot_id"] for slot in slots]
        assert run_receipt["completed_slots"] == 13 and run_receipt["terminal_stop"] is False
        progress = producer.reopen_declared_cell_owner_progress(env.context, precommit=env.precommit,
            process=env.process, cell_id=cell["cell_id"], frozen_pipeline_config=env.frozen)
        commit_entries.extend(progress.slot_commit_artifacts)
    else:
        previous = None
        for slot in slots:
            sink = spool.open_symbol_sink(env.context, cell["symbol"])
            request = orchestration.declared_slot_producer_request(env.context, precommit=env.precommit,
                process=env.process, cell_id=cell["cell_id"], slot_id=slot["slot_id"],
                attempt_id="synthetic-carrier", sink=sink)
            completion, artifacts = prepare_completion(request, slot, "OWNER" if slot["ordinal"] == 1 else "NO_TRADE")
            try:
                _, descriptor = producer._commit_declared_slot_completion(env.context,
                    precommit=env.precommit, process=env.process, cell_id=cell["cell_id"], slot=slot,
                    completion=completion, artifact_roster=artifacts, previous_commit_sha256=previous,
                    frozen_pipeline_config=env.frozen)
                previous = descriptor["sha256"]
                commit_entries.append(producer._declared_commit_artifact(slot, descriptor))
            finally:
                orchestration._release_declared_slot_producer_request(request, env.context)
            if slot["ordinal"] == 1:
                # Read the actual one-OWNER prefix before any later commits exist.
                env.one_owner_prefix = forward._reopen_declared_cell_inputs(env.context,
                    precommit=env.precommit, process=env.process, frozen_pipeline_config=env.frozen)
                assert env.one_owner_prefix["progress_status"] == "INCOMPLETE"
                assert len(env.one_owner_prefix["commits"]) == len(env.one_owner_prefix["owner_records"]) == 1
                assert [row["status"] for row in env.one_owner_prefix["slot_schedule"]] == ["OWNER"] + ["MISSING"] * 12
        sink = spool.open_symbol_sink(env.context, cell["symbol"])
        for entry in commit_entries:
            sink.register_artifact(entry["artifact_id"], entry["artifact_type"], entry["descriptor"])
        sink.seal()
    spec = pm_core.InstrumentSpec(cell["symbol"], 5, point=0.00001, tick_size=0.00001,
        tick_value=1.25, pip_value=12.5, min_lot=0.01, max_lot=500.0, volume_step=0.01, asset_class="fx")

    native_calls = []

    def verify_native(**kwargs):
        assert kwargs["context"] is env.context and kwargs["expected_symbol"] == cell["symbol"]
        product = products[kwargs["expected_timeframe"]]
        assert kwargs["expected_index_sha256"] == product.index_sha256
        assert kwargs["index_descriptor"] == product.index_descriptor and kwargs["expected_cutoff_utc"] == cutoff
        assert kwargs["pinned_activation_manifest_sha256"] == identities["M5"]["activation_manifest_sha256"]
        assert kwargs["expected_activation_manifest_sha256"] == identities["M5"]["activation_manifest_sha256"]
        assert kwargs["production_contract_required"] is False and kwargs["require_current_activation"] is False
        assert Path(kwargs["data_dir"]) == Path(config.data_dir)
        native_calls.append(kwargs["expected_timeframe"])
        return product

    def open_native(product):
        assert any(product is value for value in products.values())
        return (frame if product is products["H1"] else fine_frame).copy(deep=True)

    def freeze_spec(*, product, config):
        assert any(product is value for value in products.values())
        assert config._deployment_config_path == str(env.config_path.resolve())
        deployment = forward._deployment_config(config)
        receipt = {"classification": classification, "deployment_config_sha256": deployment.config_sha256,
            "deployment_config_size_bytes": deployment.config_size_bytes,
            "broker_specs_relative_path": deployment.broker_relative_path,
            "broker_specs_sha256": deployment.broker_sha256, "broker_specs_size_bytes": deployment.broker_size_bytes,
            "native_prefix_index_sha256": product.index_sha256,
            "grants_search_authority": False, "grants_trading_authority": False}
        receipt["receipt_sha256"] = forward._sha(receipt)
        return spec, receipt

    monkeypatch.setattr(native, "verify_activated_native_prefix", verify_native)
    monkeypatch.setattr(native, "open_verified_prefix_frame", open_native)
    monkeypatch.setattr(forward.search_execution, "_freeze_bound_execution_spec", freeze_spec)
    monkeypatch.setattr(pm_core, "NUMBA_AVAILABLE", False)
    env.known, env.authentication_calls, env.commit_entries = known, authentication_calls, commit_entries
    env.products, env.profile_path, env.classification = products, profile_path, classification
    env.native_calls = native_calls
    env.driver_calls = driver_calls
    return env
