"""Real metadata issuers on invented owned inputs: no native, fit, Raw or financial proof.

Root must supply a coherent flat source image. This test neither assembles code
nor substitutes verifier results; original public APIs create every capability.
"""
from datetime import datetime, timedelta, timezone
import hashlib
import importlib
import json
import os
from pathlib import Path
import stat
from types import SimpleNamespace

import pytest


_SOURCE_ENV = "FXPM_M5_PUBLIC_SOURCE_ROOT"
_NOTICE = "SYNTHETIC_PUBLIC_METADATA_ONLY_NO_NATIVE_OR_FINANCIAL_VERIFICATION"


def _sha(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":"),
                                     ensure_ascii=True, allow_nan=False).encode("ascii")).hexdigest()


def _regular_file(path):
    info = path.lstat()
    if (not stat.S_ISREG(info.st_mode) or path.is_symlink()
            or getattr(info, "st_file_attributes", 0) & getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0)):
        raise RuntimeError("PUBLIC_METADATA_SOURCE_NOT_REGULAR")
    return path.read_bytes()


def _modules():
    configured = os.environ.get(_SOURCE_ENV)
    if configured is None:
        pytest.skip("Root-owned coherent public-metadata QA source image is not configured")
    source_root = Path(configured)
    if not configured or not source_root.is_absolute():
        raise RuntimeError("PUBLIC_METADATA_SOURCE_ROOT_INVALID")
    info = source_root.lstat()
    if (not stat.S_ISDIR(info.st_mode) or source_root.is_symlink()
            or getattr(info, "st_file_attributes", 0) & getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0)
            or source_root.resolve(strict=True) != source_root):
        raise RuntimeError("PUBLIC_METADATA_SOURCE_ROOT_INVALID")
    # The QA launcher supplies sys.path. A stale cached module must fail, not be replaced.
    o = importlib.import_module("pm_historical_anchor_orchestration")
    if Path(o.__file__).resolve() != source_root / "pm_historical_anchor_orchestration.py":
        raise RuntimeError("PUBLIC_METADATA_ORCHESTRATION_ORIGIN_MISMATCH")
    closure = tuple(sorted(set(o._DECLARED_SOURCE_FILES) | set(o._DECLARED_M5_CONSTRUCTORS)))
    physical = {name: hashlib.sha256(_regular_file(source_root / name)).hexdigest() for name in closure}
    loaded = {}
    for filename in closure:
        module = importlib.import_module(Path(filename).stem)
        if Path(module.__file__).resolve() != source_root / filename:
            raise RuntimeError("PUBLIC_METADATA_CODE_CLOSURE_ORIGIN_MISMATCH:" + filename)
        loaded[filename] = module
    # These unchanged original metadata APIs may reside in the coherent image or canonical checkout.
    names = ("pm_authority_spool", "pm_production_config_binding", "pm_production_anchor_plan",
             "pm_cap_calibration", "pm_book_multiplicity")
    support = {name: importlib.import_module(name) for name in names}
    repo = Path.cwd().resolve()
    for name, module in support.items():
        if Path(module.__file__).resolve() not in (source_root / (name + ".py"), repo / (name + ".py")):
            raise RuntimeError("PUBLIC_METADATA_ORIGINAL_API_ORIGIN_MISMATCH:" + name)
    return SimpleNamespace(o=o, planner=loaded["pm_production_search_planner.py"],
        spool=support["pm_authority_spool"], config=support["pm_production_config_binding"],
        anchors=support["pm_production_anchor_plan"], cap=support["pm_cap_calibration"],
        book=support["pm_book_multiplicity"], source_root=source_root, physical=physical)


@pytest.fixture(scope="module")
def public_development_environment(tmp_path_factory):
    modules = _modules()  # Missing opt-in skips before any owned fixture files exist.
    root = tmp_path_factory.mktemp("public-development-metadata").resolve()
    notice = {"classification": _NOTICE, "native_verification_performed": False,
              "model_or_fit_performed": False, "qualification_authority": False,
              "capital_authority": False, "live_authority": False, "order_authority": False}
    (root / "fixture_notice.json").write_text(json.dumps(notice, indent=2) + "\n", encoding="utf-8")
    payload = {"_meta": {"scope": "UNISSUED_TECHNICAL_BENCHMARK", "fixture_classification": _NOTICE},
        "pipeline": {"data_dir": str(root / "data"), "output_dir": str(root / "outputs"),
            "log_dir": str(root / "logs"), "manual_kill_switch_path": str(root / "unused-owned-stop.json"),
            "regime_params_file": str(root / "absent-owned-regime-params.json"),
            "research_mode": True, "initial_capital": 10000.0,
            "discovery_risk_per_trade_pct": 1.0, "risk_per_trade_pct": 0.25,
            "backtest_spread_mode": "conservative_max", "oos_validation_mode": "fixed_holdout"},
        "symbols": ["XAGUSD"], "position": {}, "storage": {}}
    config_path = root / "synthetic-public-config.json"
    config_path.write_text(json.dumps(payload, allow_nan=False, indent=2) + "\n", encoding="utf-8")
    owned, effective = modules.config.load_effective_pipeline_config(config_path)
    limits = modules.spool.SpoolLimits(max_artifact_bytes=1024**3,
        max_symbol_bytes=8*1024**3, max_generation_bytes=8*1024**3,
        max_artifacts_per_symbol=100000, max_manifest_bytes=128*1024**2,
        max_json_bytes=64*1024**2, stream_chunk_bytes=1024**2, min_free_bytes=5*1024**3)
    context = modules.spool.freeze_generation_context(root / "spool",
        generation_id="synthetic-public-development-001",
        native_universe_sha256=_sha({"classification": _NOTICE, "identity_role": "invented-universe-label"}),
        config_sha256=modules.cap.canonical_json_sha256(owned), limits=limits)
    frozen = modules.config.freeze_production_pipeline_config(context, active_config_path=config_path,
        config_payload=owned, supplied_config=effective)
    run_id = "synthetic-public-development-001"
    process = modules.planner.freeze_production_search_process(context, run_id=run_id)
    launch = "2023-06-24T00:00:00Z"
    roster = modules.anchors.derive_declared_owner_slot_roster_v2(launch_activation_utc=launch,
        frozen_refit_clock={"anchor_activation_utc": "2026-03-29T00:01:00Z", "interval_calendar_days": 28},
        config_identity_refs={"config_sha256": context.config_sha256,
                              "effective_config_sha256": frozen.effective_config_sha256},
        extend_frozen_clock_backward=True, protected_end_exclusive_utc="2024-07-14T08:05:00Z")
    cell = {"cell_id": modules.book.compute_cell_id("XAGUSD", "H1", "TREND"),
            "symbol": "XAGUSD", "timeframe": "H1", "regime": "TREND"}
    # These are invented documentary pins, not a RecordedDerivedCosts object or verified profile.
    profile_sha = _sha({"classification": _NOTICE, "identity_role": "unverified-profile"})
    cost_identity = {"receipt_sha256": _sha({"classification": _NOTICE, "identity_role": "unverified-cost-receipt"}),
        "generation_id": "synthetic-unverified-cost-metadata", "native_universe_sha256": context.native_universe_sha256,
        "plan_sha256": _sha({"classification": _NOTICE, "identity_role": "unverified-cost-plan"}),
        "implementation_sha256": _sha({"classification": _NOTICE, "identity_role": "unverified-cost-implementation"}),
        "profile_sha256": profile_sha}
    pins = {"schema_version": modules.o.DECLARED_M5_SOURCE_PINS_SCHEMA,
        "source_kind": "m5_derived", "base_timeframe": "M5", "view_timeframe": "H1",
        "recipe_sha256": "85e321bc794c59cf26e725bbb7772b2590a7b0256e2f6e6086ee3bf52189c1f4",
        "constructor_identity": {name: modules.physical[name] for name in modules.o._DECLARED_M5_CONSTRUCTORS},
        "view_plan_sha256": _sha({"classification": _NOTICE, "identity_role": "unissued-view-plan"}),
        "gap_findings_sha256": _sha({"classification": _NOTICE, "identity_role": "unexamined-gap-label"}),
        "cost_edition_identity": cost_identity}
    now = datetime.now(timezone.utc).replace(microsecond=0)
    window = {"start_utc": (now-timedelta(minutes=1)).strftime("%Y-%m-%dT%H:%M:%SZ"),
              "end_exclusive_utc": (now+timedelta(hours=1)).strftime("%Y-%m-%dT%H:%M:%SZ")}
    precommit = modules.o.precommit_declared_development_m5_slot_owner_run(context,
        run_id=run_id, process=process, slot_roster=roster, cell_roster=[cell],
        frozen_pipeline_config=frozen, spread_profile_identity={"profile_sha256": profile_sha},
        source_access_window_utc=window, launch_activation_utc=launch, m5_source_pins=pins,
        development_slot_ids=[roster["slots"][0]["slot_id"]])
    return SimpleNamespace(modules=modules, root=root, context=context, frozen=frozen,
        process=process, roster=roster, cell=cell, pins=pins, precommit=precommit, window=window)


def test_genuine_public_development_precommit_request_and_reopen_scope(public_development_environment):
    e = public_development_environment
    o, context = e.modules.o, e.context
    parent = o.verify_declared_slot_owner_run_precommit(context, e.precommit, process=e.process)
    assert parent["schema_version"] == o.DECLARED_M5_DEVELOPMENT_SLOT_PRECOMMIT_SCHEMA
    assert parent["slot_roster"] == e.roster and len(parent["slot_roster"]["slots"]) == 13
    assert parent["source_identity"] == {name: e.modules.physical[name] for name in o._DECLARED_SOURCE_FILES}
    assert parent["m5_source_pins"] == e.pins
    assert parent["m5_view_contract_sha256"] == _sha(e.pins)
    assert parent["development_scope_sha256"] == _sha(parent["development_scope"])
    assert parent["development_scope"]["selected_slot_ids"] == [e.roster["slots"][0]["slot_id"]]
    assert parent["development_scope"]["design_contaminated"] is True
    assert parent["development_scope"]["pristine_validation"] is False
    for key in ("qualification_authority", "capital_authority", "live_authority", "order_authority"):
        assert parent[key] is False and parent["development_scope"][key] is False
    reopened = o.reopen_declared_slot_owner_run_precommit(context, receipt_sha256=e.precommit.receipt_sha256,
        process=e.process, frozen_pipeline_config=e.frozen)
    assert reopened.receipt_sha256 == e.precommit.receipt_sha256
    assert reopened.to_dict() == parent
    sink = e.modules.spool.open_symbol_sink(context, e.cell["symbol"])
    request = o.declared_slot_producer_request(context, precommit=reopened, process=e.process,
        cell_id=e.cell["cell_id"], slot_id=e.roster["slots"][0]["slot_id"],
        attempt_id="synthetic-public-attempt-001", sink=sink)
    try:
        body = o.verify_declared_slot_producer_request(context, request, precommit=reopened, process=e.process)
        assert body["schema_version"] == o.DECLARED_M5_DEVELOPMENT_SLOT_REQUEST_SCHEMA
        assert body["development_scope"] == parent["development_scope"]
        assert body["development_scope_sha256"] == parent["development_scope_sha256"]
        assert body["precommit_sha256"] == reopened.receipt_sha256
        assert body["anchor"]["process_precommit_receipt_sha256"] == reopened.receipt_sha256
        assert body["anchor"]["ordinal"] == 1
        assert body["anchor"]["selection_cutoff_utc"] == e.roster["slots"][0]["native_cutoff_utc"]
        assert body["source_access_window_utc"] == e.window
        assert body["confers_causal_or_current_authority"] is False
        assert _sha(body) == request.receipt_sha256
        # Actual public request header metadata, not a fabricated fitted/Raw unit header.
        stored = context.root / o._declared_request_path(body)
        assert json.loads(stored.read_text(encoding="ascii")) == body
    finally:
        o._release_declared_slot_producer_request(request, context)


def test_public_request_refuses_a_real_but_unselected_roster_slot(public_development_environment):
    e = public_development_environment
    sink = e.modules.spool.open_symbol_sink(e.context, e.cell["symbol"])
    with pytest.raises(e.modules.o.HistoricalAnchorOrchestrationError) as caught:
        e.modules.o.declared_slot_producer_request(e.context, precommit=e.precommit, process=e.process,
            cell_id=e.cell["cell_id"], slot_id=e.roster["slots"][1]["slot_id"],
            attempt_id="synthetic-forbidden-attempt", sink=sink)
    assert str(caught.value) == "DECLARED_SLOT_SLOT_INVALID"
    assert len(e.precommit.to_dict()["slot_roster"]["slots"]) == 13
