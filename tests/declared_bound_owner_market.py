"""Owned synthetic market for a proposed declared OWNER software witness.

SYNTHETIC_SOFTWARE_WITNESS_NOT_QUALIFICATION. The builder is source-only until
independent exact-source review and separate Root QA authorization. Imports
and file I/O occur only when build_owned_market is explicitly invoked.
"""
from __future__ import annotations


PREDECLARATION = {
    "label": "SYNTHETIC_SOFTWARE_WITNESS_NOT_QUALIFICATION",
    "edition": "IMPLEMENTATION_CANDIDATE_002",
    "status": "PROPOSED_UNRUN_PENDING_EXACT_REVIEW_AND_ROOT_QA",
    "fixture_id": "declared-bound-owner-witness-001",
    "market": {
        "symbol": "EURUSD",
        "timeframe": "H4",
        "regime": "TREND",
        "generator": "unchanged native_v5_support._bars_for_index/_resample via write_v5_run",
        "oscillation_period_days": 28.0,
        "price_offset": 0.0,
        "spread_points": 3,
        "dense_band_utc": ["2020-09-27T00:00:00Z", "2023-06-24T00:00:00Z"],
        "information_cutoff_utc": "2025-04-11T00:00:00Z",
        "random_seed": "NO_RANDOM_DRAWS_DETERMINISTIC_TIME_BUCKET_FORMULA",
        "retry_or_market_seed_sweep": False,
        "geometry_assertions": "Assert the actual four frozen OOS folds and all300 cross-binding fit spans lie inside the fixed dense band. The source-count schema does not publish all twelve partition spans; original planner and partition gates remain in force. No OWNER forecast.",
    },
    "process": {
        "public_builder": "pm_production_search_planner.freeze_production_search_process",
        "attempt_count": 300,
        "count_override_or_private_seal": False,
        "parameter_design_seed": "unchanged canonical public process design; actual seed fields are bound by its original process receipt",
        "refit_seed": "unchanged public planner _cell_hash(strategy, parameters, refit-seed), never selected by a test sweep",
        "first_slot_only": True,
        "transport": "v16 public precommit and actual BOUND_FIT_V16/v7 plan; no alternate edition run",
        "launch_activation_utc": "2023-06-24T00:00:00Z",
        "frozen_refit_clock": {"anchor_activation_utc": "2026-03-29T00:01:00Z", "interval_calendar_days": 28},
        "protected_end_exclusive_utc": "2024-07-14T08:05:00Z",
        "roster_geometry": "original derive_declared_owner_slot_roster_v2; retain all13 unmaterialized slots; exercise and commit only slot1",
        "statistical_gates": "all existing Raw/Winner/trade-power thresholds and selection/refit policies unchanged",
    },
    "proposed_owned_inputs_only": {
        "native": "native_v5_support.write_v5_run(production_grid=True), then original activate_v5_run/audit_and_activate over that OWNED synthetic root",
        "synthetic_series": 600,
        "series_geometry": "100 symbols x6 timeframes; one dense target symbol, other99 physically backed sparse page-boundary series",
        "physical_files": "NOT assumed600. write_v5_run writes per-series CSV/provenance plus run manifest; actual audit writes sidecars/parity/semantic receipt/activation/pointer files. Count must come from that builder/actual retained inventory.",
        "source_receipts": "existing helper emits labelled invented connector/page diagnostics. Real fixture audit checks those bytes; none are represented as observed broker/native acquisition or source-qualified real history.",
        "broker_and_config": "explicit owned invented broker specification and complete declared deployment config, source-bound to the same owned instrument snapshot; canonical config/data never read",
        "spread_profile": "owned fixed P75=9/P95=16 point software cost scenario inherited from unchanged prototype_support._synthetic_profile, with actual owned fine/target/activation identity and paths; not empirical calibration or a threshold search",
        "terms_status": "PUBLIC IMMUTABLE STRESS INPUT PROPOSAL; EXACT SOURCE REVIEW AND QA STILL PENDING",
        "terms_recipe": [
            "pm_broker_terms.write_capture_request: fresh owned request and nonce",
            "explicitly invented CSV with EXPORT_COLUMNS, all100 rows, invented broker identity, coherent owned D1 source anchors and seven weekday factors",
            "original parse_terms_export: compute actual owned export/content hashes; validate nonce, roster, identity and freshness",
            "original build_explicit_stress_snapshot: immutable built-in all_sessions_triple_debit_v1 transform, never hand-edit status/provenance/rates/flags/policy hash",
            "original activate_broker_terms: actual owned audited price scope, roster and schedule derivation; owned snapshots/schedules/activation/ACTIVE/lock files only",
        ],
        "terms_truth": "Synthetic software-input CSV/activation only; no observed broker export, historical-rate authentication, financial qualification or genuine authority claim. No old broker-term test imports or mocked price authority.",
        "all_IO_roots": "one future Root-owned pytest tmp_path only; native fixture inputs, config, terms, profile, logs, outputs and spool all stay there",
        "excluded_inputs": "real native207, real broker/profile600 audit, canonical native/config/terms, retained audit cases, parked real_control/STOP, old full-chain/owner test fixtures",
    },
    "required_original_functions": [
        "pm_historical_production_adapter.verify_declared_slot_completion",
        "pm_historical_production_adapter._declared_owner_from_verified_winner",
        "pm_historical_search_producer._commit_declared_slot_completion",
        "pm_historical_search_producer.reopen_declared_cell_owner_progress",
    ],
    "required_assertions": [
        "four functions and their subordinate verification gates remain module originals at call time",
        "actual capture complete_observed_cell takes OWNER from original selector under BOUND_FIT_V15/V16",
        "original verify_historical_cell_winner_decision returns true; real owner equals original adapter construction, committed owner and cold-reopened owner/summary",
        "qualification/capital/live/order authority flags all false",
        "one copied owner-field mutation refuses exactly DECLARED_SLOT_OWNER_BINDING_MISMATCH",
        "legacy forward._selected_artifacts must fail if called; only this negative guard may be patched",
    ],
    "commit_bounds": {"maximum_bytes": 67108864, "maximum_JSON_nodes": 2000000},
    "bound_scope": "commit bound only; NOT an aggregate-temp/RSS/I/O estimate or admission forecast",
    "stopping_condition": "one declared fixture/process only; if selector abstains or a technical/capacity/source gate refuses, stop and retain actual failure, no parameter/seed/threshold/market redesign without a new predeclaration",
    "proven_source_blocker": {
        "without_terms": "TradePower requires six binding digests: pm_trade_power.py758-762",
        "plain_current_terms": "core8116-8117 historical_rates_authenticated=False; TradePower790-799 rejects SOURCE_CONTRACT_SWAP_PROVENANCE_INVALID",
        "existing_software_alternative": "AVAILABLE/STRESS_APPROVED with exact immutable approved policy accepted by TradePower813-827; public builder exists at broker_terms945",
        "not_a_workaround": "ResearchSwapContext conflicts with authenticated native binding and will not be used; do not disable swaps or rewrite provenance",
    },
    "required_before_runtime": [
        "original exact-source disposition must explicitly cover proposed owned100x6 audit/profile/config/terms I/O and immutable stress input recipe",
        "review the implemented CSV nonce/identity/owned-D1-anchor binding and actual frozen fold/cross-fit span assertions without borrowed objects or patches",
        "obtain coherent two-file exact-source acceptance and separate one-run Root QA disposition; no automatic run or market redesign",
    ],
    "expected_source_cutoff_utc": "2023-06-23T00:00:00Z",
    "source_access_duration_seconds": 172800,
    "source_access_scope": "fresh actual builder issuance plus48h, policy allowance only; no runtime forecast or renewal",
    "persistent_spool_limits": {"artifact_bytes": 1073741824, "symbol_and_generation_bytes": 8589934592, "artifacts_per_symbol": 100000, "min_free_bytes": 17179869184},
    "pytest_scheduling_opt_in": "FXPM_RUN_DECLARED_BOUND_OWNER_WITNESS=1; test-local scheduling only, not runtime authority; approved QA must report zero skips",
    "regime_parameter_source": "authoritative fit uses inline DEFAULT_PARAMS_BY_TIMEFRAME; optional config file is owned and deliberately absent, never a claimed tuned model",
    "path_defaults": "data/output/log/cache/winner-ledger/regime-file paths bind inside the owned root; optional production/live/calendar/manual paths retain empty built-in defaults",
    "runnable_test_written": True,
    "product_or_existing_fixture_edits": False,
    "source_preparation_imports_materialization_audit_tests_fit_or_models_run": False,
    "genuine_NO_TRADE_case_changed": False,
}


def _owned_json(path, root):
    import json
    from pathlib import Path

    path = Path(path).resolve(strict=True)
    path.relative_to(root)
    if path.is_symlink() or path.stat().st_size > 64 * 1024**2:
        raise ValueError("SYNTHETIC_METADATA_PATH_OR_SIZE_INVALID")
    with path.open("r", encoding="utf-8") as stream:
        body = json.load(stream)
    if not isinstance(body, dict):
        raise ValueError("SYNTHETIC_METADATA_NOT_OBJECT")
    return body


def _utc_text(value):
    from datetime import timezone

    return value.astimezone(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def assert_planned_geometry(frozen):
    from datetime import datetime, timedelta

    market = PREDECLARATION["market"]
    start, end = (datetime.fromisoformat(value.replace("Z", "+00:00"))
                  for value in market["dense_band_utc"])
    cutoff = datetime.fromisoformat(PREDECLARATION["expected_source_cutoff_utc"].replace("Z", "+00:00"))
    folds = frozen.precommitment["causal_fold_plan"]
    assert len(folds) == 4
    prior_end = None
    for ordinal, fold in enumerate(folds):
        assert fold["fold_ordinal"] == ordinal
        opened = datetime.fromisoformat(fold["oos_start_date_utc"] + "T00:00:00+00:00")
        closed = datetime.fromisoformat(fold["oos_end_date_utc"] + "T00:00:00+00:00") + timedelta(days=1)
        assert datetime.fromisoformat(fold["anchor_as_of_utc"].replace("Z", "+00:00")) == opened
        assert start <= opened < closed <= min(end, cutoff)
        assert prior_end is None or prior_end <= opened
        prior_end = closed
    assert len(frozen.attempt_ledger["attempts"]) == 300


def assert_source_count_geometry(cross):
    from datetime import datetime

    start, end = (datetime.fromisoformat(value.replace("Z", "+00:00"))
                  for value in PREDECLARATION["market"]["dense_band_utc"])
    cutoff = datetime.fromisoformat(PREDECLARATION["expected_source_cutoff_utc"].replace("Z", "+00:00"))
    assert len(cross["mapping"]) == 300
    for row in cross["mapping"]:
        count = row["source_count_receipt"]
        fit_start = datetime.fromisoformat(count["fit_start_utc"].replace("Z", "+00:00"))
        fit_end = datetime.fromisoformat(count["fit_end_exclusive_utc"].replace("Z", "+00:00"))
        latest = datetime.fromisoformat(count["latest_closed_bar_utc"].replace("Z", "+00:00"))
        assert start <= fit_start < fit_end <= min(end, cutoff)
        assert fit_start <= latest <= fit_end
        assert type(count["source_bar_count"]) is int and count["source_bar_count"] > 0
    # The source-count schema has fit spans, not all twelve partition spans.


def _write_software_profile(native_root, manifest, manifest_sha):
    import json
    from pm_native_request_contract import canonical_source_identity
    from pm_spread_profile import FrozenSpreadProfile

    symbol = PREDECLARATION["market"]["symbol"]
    timeframe = PREDECLARATION["market"]["timeframe"]
    target = manifest["entries"][symbol][timeframe]
    fine = manifest["entries"][symbol]["M5"]
    if target["status"] != "evidence_candidate" or fine["status"] != "evidence_candidate":
        raise ValueError("SYNTHETIC_TARGET_OR_FINE_NOT_ADMISSIBLE")
    fine_sidecar = _owned_json(native_root / fine["sidecar_relative_path"], native_root)
    provenance = _owned_json(native_root / fine_sidecar["source_provenance_relative_path"], native_root)
    identity = canonical_source_identity(provenance["source_environment"])
    point = 0.00001
    tiers = {
        tier: {"spread_points": points, "full_spread_price": points * point,
               "half_spread_price": points * point / 2, "usability": "POSITIVE_QUANTILE"}
        for tier, points in (("P75", 9), ("P95", 16))
    }
    body = {
        "schema_version": "fxpm.frozen-spread-profile.v1",
        "status": "COMPLETE_CALIBRATION_WITH_EXPLICIT_UNAVAILABLE_GROUPS",
        "calculation_policy": "ONE_TIME_OFFLINE_NO_RUNTIME_RECALCULATION",
        "source_convention": "FINEST_ACTIVATED_CANDIDATE_FIXED_PER_SYMBOL_EXACT_PRIOR_CLOSE_AT_TARGET_OPEN",
        "production_authority": False,
        "fixture_provenance": "SYNTHETIC_FIXED_SOFTWARE_COST_SCENARIO_NOT_EMPIRICAL_CALIBRATION",
        "protected_cutoff_utc": PREDECLARATION["process"]["protected_end_exclusive_utc"],
        "activation": {"id": manifest["activation_id"], "manifest_sha256": manifest_sha},
        "cells": [{
            "symbol": symbol, "timeframe": timeframe, "status": "CALCULATED",
            "point_price": point, "source_timeframe": "M5",
            "source_identity": {"identity_sha256": identity["identity_sha256"]},
            "source_binding": {
                "bars_relative_path": fine["bars_relative_path"],
                "bars_sha256": fine["bars_sha256"],
                "sidecar_sha256": fine["sidecar_sha256"],
                "provenance_sha256": fine_sidecar["source_provenance_sha256"],
                "target_bars_relative_path": target["bars_relative_path"],
                "target_bars_sha256": target["bars_sha256"],
            },
            "tiers": tiers,
        }],
    }
    path = native_root / "terms" / "spread_profile.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("x", encoding="utf-8", newline="\n") as stream:
        json.dump(body, stream, sort_keys=True)
        stream.write("\n")
    profile = FrozenSpreadProfile(path)
    for tier in ("P75", "P95"):
        profile.resolve(symbol, timeframe, tier, point=point,
                        source_identity_sha256=identity["identity_sha256"])
    return path, profile


def _activate_software_terms(native_root, manifest):
    import csv
    from datetime import datetime, timezone
    import pandas as pd
    import pm_broker_terms as terms
    from native_v5_support import SOURCE_ENVIRONMENT

    captured = datetime.now(timezone.utc).replace(microsecond=0)
    identity = {
        "broker_server": SOURCE_ENVIRONMENT["account"]["server"],
        "terminal_company": SOURCE_ENVIRONMENT["terminal"]["company"],
        "account_currency": SOURCE_ENVIRONMENT["account"]["currency"],
        "terminal_build": SOURCE_ENVIRONMENT["terminal"]["build"],
    }
    roster = {symbol.upper(): symbol for symbol in manifest["entries"]}
    request = terms.write_capture_request(
        native_root / "terms" / "SYNTHETIC-request.csv", roster,
        requested_at_utc=captured, expected_identity=identity,
    )
    rows = []
    factors = {"sunday": 0, "monday": 1, "tuesday": 1, "wednesday": 3,
               "thursday": 1, "friday": 1, "saturday": 0}
    for canonical, broker in roster.items():
        d1 = manifest["entries"][broker]["D1"]
        axis_path = (native_root / d1["bars_relative_path"]).resolve(strict=True)
        axis_path.relative_to(native_root)
        opens = pd.to_datetime(pd.read_csv(axis_path, usecols=["time"], nrows=2)["time"], utc=True)
        if opens.empty or opens.isna().any():
            raise ValueError("SYNTHETIC_D1_ANCHOR_MISSING")
        anchors = "|".join(f"{int(value.timestamp())}:{(value.weekday() + 1) % 7}" for value in opens)
        rows.append({
            "schema_version": terms.TERMS_EXPORT_SCHEMA, "nonce": request["nonce"],
            "captured_at_utc": _utc_text(captured), "canonical_symbol": canonical,
            "broker_symbol": broker, **identity, "swap_mode": 1,
            "swap_long": -3.0, "swap_short": 1.0, "swap_rollover3days": 3,
            **{f"swap_{day}": value for day, value in factors.items()}, "d1_anchors": anchors,
        })
    export = native_root / "terms" / "SYNTHETIC-invented-response.csv"
    with export.open("x", encoding="utf-8", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=terms.EXPORT_COLUMNS, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)
    parsed = terms.parse_terms_export(
        export, expected_nonce=request["nonce"], expected_roster=roster,
        expected_identity=identity, requested_at_utc=request["requested_at_utc"],
        now_utc=captured,
    )
    stressed = terms.build_explicit_stress_snapshot(parsed)
    if parsed["provenance_status"] != terms.AUTHENTICATED or stressed["provenance_status"] != terms.STRESS_APPROVED:
        raise ValueError("SYNTHETIC_PUBLIC_STRESS_TRANSFORM_FAILED")
    activated = terms.activate_broker_terms(
        native_root, stressed, activation_id="synthetic-owner-terms-001",
        now_utc=captured,
    )
    return activated, {"request": request, "export_path": export,
                       "provenance": PREDECLARATION["label"],
                       "observed_broker_export": False}


def build_owned_market(tmp_path):
    """Materialize this proposal only under a separately authorized QA invocation.

    Every file stays under a fresh pytest-owned child. Native audit and terms
    activation are originals over invented inputs; none are market observations.
    """
    import copy
    import json
    from datetime import datetime, timedelta, timezone
    from pathlib import Path
    from types import SimpleNamespace

    from native_v5_support import write_v5_run, activate_v5_run
    from prototype_support import _execution_config
    from pm_native_request_contract import PRODUCTION_SYMBOL_ORDER
    import pm_authority_spool as spool
    import pm_portfolio_replay as portfolio
    import pm_production_config_binding as config_binding
    import pm_production_search_planner as planner
    import pm_production_anchor_plan as anchor_plan
    import pm_historical_anchor_orchestration as orchestration
    import pm_book_multiplicity as book

    root = Path(tmp_path).resolve(strict=True) / PREDECLARATION["fixture_id"]
    root.mkdir(exist_ok=False)
    data_dir = root / "data"
    native_root = data_dir / "native"
    native_root.mkdir(parents=True)
    config_path = _execution_config(
        root, symbol=PREDECLARATION["market"]["symbol"],
        broker_overrides={"parity_tick_tolerance": 0.0001},
    )
    broker = _owned_json(root / "broker_specs.json", root)[PREDECLARATION["market"]["symbol"]]
    # Project the written broker values without changing their units or config.
    instrument_snapshot = {
        "asset_class": broker["asset_class"],
        "digits": broker["digits"],
        "parity_tick_tolerance": broker["parity_tick_tolerance"],
        "path": broker["path"],
        "point": broker["point"],
        "status": "available",
        "symbol": broker["symbol"],
        "trade_contract_size": broker["contract_size"],
        "trade_tick_size": broker["tick_size"],
        "trade_tick_value": broker["tick_value"],
    }
    payload = {
        "_meta": {"scope": "funded_live_v1", "fixture_provenance": PREDECLARATION["label"]},
        "symbols": list(PRODUCTION_SYMBOL_ORDER),
        "broker_specs_path": "broker_specs.json",
        "pipeline": {
            "data_dir": str(data_dir), "output_dir": str(root / "outputs"),
            "log_dir": str(root / "logs"), "research_mode": True,
            "feature_disk_cache_dir": str(root / "feature-cache"),
            "winner_ledger_path": str(root / "unused-winner-ledger.json"),
            "regime_params_file": str(root / "unused-inline-regime-defaults.json"),
            "initial_capital": 10000.0, "discovery_risk_per_trade_pct": 1.0,
            "risk_per_trade_pct": 0.25, "use_spread": True,
            "backtest_spread_mode": "conservative_max",
        },
    }
    with config_path.open("w", encoding="utf-8", newline="\n") as stream:
        json.dump(payload, stream, sort_keys=True)
        stream.write("\n")
    owned_payload, effective = config_binding.load_effective_pipeline_config(config_path)
    market = PREDECLARATION["market"]
    write_v5_run(
        native_root, "synthetic-owner-native-001", production_grid=True,
        dense_symbol=market["symbol"], dense_band_utc=tuple(market["dense_band_utc"]),
        information_cutoff_utc=market["information_cutoff_utc"],
        oscillation_period_days=market["oscillation_period_days"],
        price_offset=market["price_offset"], spread_points=market["spread_points"],
        canonical_broker_symbols=True, instrument_snapshot=instrument_snapshot,
    )
    activation = activate_v5_run(
        native_root, "synthetic-owner-native-001", "synthetic-owner-native-001",
        production_only_pointer=True, now_utc=datetime.now(timezone.utc),
    )
    pointer = _owned_json(native_root / "ACTIVE.json", native_root)
    manifest_path = (native_root / pointer["manifest_relative_path"]).resolve(strict=True)
    manifest_path.relative_to(native_root)
    manifest = _owned_json(manifest_path, native_root)
    profile_path, profile = _write_software_profile(native_root, manifest, pointer["manifest_sha256"])
    terms_activation, terms_provenance = _activate_software_terms(native_root, manifest)
    native_snapshot, errors = portfolio.active_native_portfolio_contract(
        data_dir, book.PINNED_SYMBOL_ORDER, timeframes=book.TIMEFRAME_ORDER,
    )
    if errors:
        raise ValueError("SYNTHETIC_NATIVE_PORTFOLIO_CONTRACT_FAILED:" + ",".join(errors))
    limits = spool.SpoolLimits(
        max_artifact_bytes=1024**3, max_symbol_bytes=8 * 1024**3,
        max_artifacts_per_symbol=100000, max_manifest_bytes=128 * 1024**2,
        max_json_bytes=64 * 1024**2, stream_chunk_bytes=1024**2,
        max_generation_bytes=8 * 1024**3, min_free_bytes=16 * 1024**3,
    )
    context = spool.freeze_generation_context(
        root / "spool", generation_id=PREDECLARATION["fixture_id"],
        native_universe_sha256=portfolio.canonical_json_sha256(native_snapshot),
        config_sha256=portfolio.canonical_json_sha256(owned_payload), limits=limits,
    )
    frozen = config_binding.freeze_production_pipeline_config(
        context, active_config_path=config_path, config_payload=owned_payload,
        supplied_config=effective,
    )
    config = frozen.materialize(context)
    params_path = Path(config.regime_params_file)
    if not params_path.is_absolute() or params_path.resolve().parent != root or params_path.exists():
        raise ValueError("SYNTHETIC_REGIME_DEFAULT_PATH_MUST_BE_OWNED_AND_ABSENT")
    if config.manual_kill_switch_path != "":
        raise ValueError("SYNTHETIC_MANUAL_PATH_MUST_RETAIN_EMPTY_DEFAULT")
    if (config.discovery_risk_per_trade_pct != 1.0 or not config.swap_model_enabled
            or config.false_discovery_symbol_count != 100):
        raise ValueError("SYNTHETIC_DEFAULT_POLICY_BINDING_FAILED")
    run_id = "synthetic-owner-software-run-001"
    process = planner.freeze_production_search_process(context, run_id=run_id)
    roster = anchor_plan.derive_declared_owner_slot_roster_v2(
        launch_activation_utc=PREDECLARATION["process"]["launch_activation_utc"],
        frozen_refit_clock=copy.deepcopy(PREDECLARATION["process"]["frozen_refit_clock"]),
        config_identity_refs={"config_sha256": context.config_sha256,
                              "effective_config_sha256": frozen.effective_config_sha256},
        extend_frozen_clock_backward=True,
        protected_end_exclusive_utc=PREDECLARATION["process"]["protected_end_exclusive_utc"],
    )
    cell = {key: market[key] for key in ("symbol", "timeframe", "regime")}
    cell["cell_id"] = portfolio.canonical_json_sha256(cell)
    issued = datetime.now(timezone.utc).replace(microsecond=0)
    window = {"start_utc": _utc_text(issued),
              "end_exclusive_utc": _utc_text(issued + timedelta(seconds=PREDECLARATION["source_access_duration_seconds"]))}
    precommit = orchestration.precommit_declared_slot_owner_run_v2(
        context, run_id=run_id, process=process, slot_roster=roster,
        cell_roster=[cell], frozen_pipeline_config=frozen,
        spread_profile_identity={"profile_sha256": profile.sha256},
        source_access_window_utc=window,
        launch_activation_utc=PREDECLARATION["process"]["launch_activation_utc"],
    )
    parent = precommit.to_dict()
    first_slot = parent["slot_roster"]["slots"][0]
    if first_slot["native_cutoff_utc"] != PREDECLARATION["expected_source_cutoff_utc"]:
        raise ValueError("SYNTHETIC_DECLARED_SOURCE_CUTOFF_CHANGED")
    return SimpleNamespace(
        root=root, data_dir=data_dir, native_root=native_root, config_path=config_path,
        config_payload=owned_payload, config=config, frozen_pipeline_config=frozen,
        frozen=frozen, context=context, process=process, precommit=precommit, parent=parent,
        cell=parent["cells"][0], slot=first_slot, profile_path=profile_path,
        native_snapshot=native_snapshot, activation=activation, terms_activation=terms_activation,
        terms_provenance=terms_provenance, declaration=copy.deepcopy(PREDECLARATION),
    )
