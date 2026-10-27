"""Invented transport witnesses, not market evidence or a raw account replay."""
from copy import deepcopy
from dataclasses import FrozenInstanceError, replace
from datetime import date, datetime, timedelta, timezone
import hashlib
import importlib.util
import inspect
import json
import os
from pathlib import Path
import shutil
import sys
import time

import pytest

import pm_authority_spool as spool
import pm_winner_raw_evidence as raw
import pm_winner_selection as winner
from tests import test_winner_selection as fixtures


def _bytes(value):
    return json.dumps(value, ensure_ascii=True, allow_nan=False, sort_keys=True, separators=(",", ":")).encode("ascii")


def synthetic_pair(symbol="AUDUSD", timeframe="M5"):
    """Accepted 154 cost shape with wholly invented provenance, no probes."""
    from pm_core import (BACKTEST_FROZEN_TIER_PAIR_CONTRACT_SCHEMA_VERSION,
        build_backtest_spread_mode, validate_backtest_frozen_tier_pair_contract)
    from pm_spread_profile import ADMISSION_SCHEMA, ADMISSION_POLICY, TIER_SCHEMA
    from pm_trade_power import build_numerical_execution_cost_policy
    digest = lambda label: hashlib.sha256(("SYNTHETIC_TRANSPORT_ONLY:" + label).encode("ascii")).hexdigest()
    tiers = []
    for tier, points in (("P75", 21), ("P95", 29)):
        descriptor = dict(schema_version=TIER_SCHEMA, profile_sha256=digest("PROFILE"), activation="SYNTHETIC_TRANSPORT_ONLY",
            protected_cutoff_utc="2020-01-01T00:00:00Z", symbol=symbol, timeframe=timeframe, tier=tier,
            source_timeframe="M1", point_price=.00001, spread_points=points, full_spread_price=points * .00001,
            source_identity_sha256=digest("SOURCE"), source_binding={"classification": "SYNTHETIC_TRANSPORT_ONLY"},
            cost_policy="MAX_FROZEN_TIER_AND_RECORDED_SPREAD_BOTH_SIDES",
            admission_policy="EXACT_PRIOR_FINE_CLOSE_THEN_LIVE_FIRST_AND_FINAL_TICK_CAP", qualification_authority=False)
        model = dict(mode="conservative_max", frozen_spread_tier=descriptor,
            entry_admission_schema=ADMISSION_SCHEMA, entry_admission_policy=ADMISSION_POLICY)
        policy = build_numerical_execution_cost_policy(use_spread=True, spread_mode=build_backtest_spread_mode(model),
            instrument_spec_sha256=digest("INSTRUMENT"))
        tiers.append(dict(tier=tier, frozen_spread_tier=descriptor, numerical_execution_cost_policy=policy))
    return validate_backtest_frozen_tier_pair_contract(dict(schema_version=BACKTEST_FROZEN_TIER_PAIR_CONTRACT_SCHEMA_VERSION,
        admission_schema=ADMISSION_SCHEMA, admission_policy=ADMISSION_POLICY, tiers=tiers, qualification_authority=False))


def build_v16_payload(current_payload=None, **current_kwargs):
    """Construct the explicit transport edition from invented current evidence."""
    if current_payload is None:
        current_kwargs.setdefault("pair_contract", synthetic_pair(current_kwargs.get("symbol", "AUDUSD"), current_kwargs.get("timeframe", "M5")))
    value = (fixtures._current_payload_request(**current_kwargs) if current_payload is None else deepcopy(current_payload))
    value["precommitment_payload"]["contract_version"] = winner.V16_GENERATION.precommitment_version
    value["evidence_ledger_payload"]["contract_version"] = winner.V16_GENERATION.evidence_version
    for axis in value["evidence_ledger_payload"]["shared_fold_axis"]:
        axis["shared_oos_partition"]["schema_version"] = winner.V16_GENERATION.shared_partition_version
    fixtures._refresh_current_request(value)
    return value


def _capacity_counts(half_total, evaluated, concentrated):
    minimum = int(winner.CURRENT_SELECTION_POLICY["min_completed_trades_per_fold"])
    remaining = half_total - evaluated * 4 * minimum
    assert remaining >= 0
    counts = [minimum] * evaluated
    if concentrated:
        for ordinal in range(evaluated):
            extra = min(remaining, winner.MAX_TRADES_PER_FOLD - minimum)
            counts[ordinal] += extra
            remaining -= extra
        assert remaining == 0
    else:
        quotient, remainder = divmod(remaining, evaluated)
        counts = [minimum + quotient + int(i < remainder) for i in range(evaluated)]
    return minimum, counts


def build_v16_capacity_payload(total_completed_trades, *, evaluated_attempt_count=300, concentrated=False):
    """Full 300-roster/four-fold/two-account witness; no producer is invoked."""
    half = (total_completed_trades + 1) // 2
    minimum, counts = _capacity_counts(half, evaluated_attempt_count, concentrated)
    plan = fixtures._capacity_fold_plan()
    if concentrated:
        long_days = 63
    else:
        trade_work = 2 * sum(winner._dependence_operation_count([minimum] * 3 + [count]) for count in counts)
        long_days = min(winner.MAX_DAYS_PER_FOLD, winner.MAX_TOTAL_DAILY_POINTS // evaluated_attempt_count - 189)
        while long_days > 63 and (trade_work + 2 * evaluated_attempt_count *
                winner._dependence_operation_count([63, 63, 63, long_days]) > winner.MAX_DEPENDENCE_BLOCK_OPERATIONS):
            long_days -= 1
    start = date.fromisoformat(plan[-1]["oos_start_date_utc"])
    plan[-1]["oos_end_date_utc"] = (start + timedelta(days=long_days - 1)).isoformat()
    archived = fixtures._capacity_request(half, evaluated_attempt_count=evaluated_attempt_count,
        long_fold_trade_counts=counts, fold_plan_override=plan)
    identity = archived["precommitment_payload"]
    current = fixtures._current_payload_request(base_request=archived, p95_trade_step=1,
        pair_contract=synthetic_pair(identity["symbol"], identity["timeframe"]))
    if total_completed_trades % 2:
        row = current["evidence_ledger_payload"]["entries"][0]["folds"][-1]["tier_evidence"]["P95"]
        count = len(row["completed_trade_close_offset_seconds"])
        values = winner._decode_r_e12_i64be_base64(row["completed_trade_net_mtm_r_e12_i64be_base64"],
            expected_count=count, maximum_abs_r=winner.MAX_ABS_TRADE_R, code="SYNTHETIC_CAPACITY_R")
        positions = winner._decode_position_id_digests(row["completed_trade_position_id_digests_base64"], count, "SYNTHETIC_CAPACITY_POSITIONS")
        for key in ("completed_trade_open_offset_seconds", "completed_trade_close_offset_seconds",
                "completed_trade_open_event_order_offsets", "completed_trade_close_event_order_offsets"):
            row[key].pop()
        last_r = values.pop()
        values[-1] += last_r
        row["completed_trade_net_mtm_r_e12_i64be_base64"] = winner._encode_r_e12_i64be_base64(values, winner.MAX_ABS_TRADE_R, "SYNTHETIC_CAPACITY_R")
        row["completed_trade_position_id_digests_base64"] = winner._encode_position_id_digests(positions[:-1], "SYNTHETIC_CAPACITY_POSITIONS")
    return build_v16_payload(current)


def _typed(module, value):
    return module.create_winner_selection_request(value["precommitment_payload"],
        value["attempt_ledger_payload"], value["evidence_ledger_payload"])


def validate_v16_transport(value, *, request=None):
    request = _typed(winner, value) if request is None else request
    policy, policy_sha, attempts, attempt_sha, evidence, _, request_sha = winner._request_payloads(request)
    generation = winner.winner_generation_for_precommitment(policy)
    policy_time, cell, planned, folds, boundary = winner._validate_precommitment(policy, _generation=generation)
    frozen_at, attempts = winner._validate_attempt_ledger(attempts, policy, policy_sha, policy_time, cell, planned,
        _generation=generation)
    winner._validate_evidence_ledger(evidence, policy, policy_sha, attempt_sha, frozen_at, cell, attempts,
        folds, boundary, datetime.now(timezone.utc), request_sha, _generation=generation)
    return request


def _shape(value, request):
    body = request.to_dict()
    entries = value["evidence_ledger_payload"]["entries"]
    days = sum(len(axis["daily_dates_utc"]) for axis in value["evidence_ledger_payload"]["shared_fold_axis"])
    counts = [len(tier["completed_trade_close_offset_seconds"]) for entry in entries for fold in entry["folds"]
        for tier in fold["tier_evidence"].values()]
    return {"request_bytes": len(_bytes(body)), "request_nodes": fixtures._json_node_count(body),
        "evidence_payload_nodes": fixtures._json_node_count(body["evidence_ledger"]["payload"]),
        "daily_points_per_attempt": days, "total_evaluated_daily_points": days * sum(bool(e["folds"]) for e in entries),
        "maximum_per_fold_trade_events": max(counts), "completed_trade_events": sum(counts)}


def _memory():
    if os.name != "nt":
        return {"available": False}
    import ctypes
    from ctypes import wintypes
    class Counters(ctypes.Structure):
        _fields_ = [("cb", wintypes.DWORD), ("PageFaultCount", wintypes.DWORD),
            *[(name, ctypes.c_size_t) for name in ("PeakWorkingSetSize", "WorkingSetSize", "QuotaPeakPagedPoolUsage",
                "QuotaPagedPoolUsage", "QuotaPeakNonPagedPoolUsage", "QuotaNonPagedPoolUsage", "PagefileUsage", "PeakPagefileUsage")]]
    counters = Counters()
    counters.cb = ctypes.sizeof(counters)
    ctypes.windll.kernel32.GetCurrentProcess.restype = wintypes.HANDLE
    ctypes.windll.psapi.GetProcessMemoryInfo.argtypes = (wintypes.HANDLE, ctypes.POINTER(Counters), wintypes.DWORD)
    process = ctypes.windll.kernel32.GetCurrentProcess()
    if not ctypes.windll.psapi.GetProcessMemoryInfo(process, ctypes.byref(counters), counters.cb):
        return {"available": False}
    return {"available": True, "working_set_bytes": counters.WorkingSetSize,
        "process_lifetime_peak_working_set_bytes": counters.PeakWorkingSetSize,
        "private_commit_bytes": counters.PagefileUsage, "process_lifetime_peak_private_commit_bytes": counters.PeakPagefileUsage}


def _record(name, body):
    destination = os.environ.get("FXPM_WINNER_QA_OUTPUT")
    if destination:
        root = Path(destination).resolve()
        root.mkdir(parents=True, exist_ok=True)
        with (root / (name + ".json")).open("x", encoding="ascii") as handle:
            json.dump({"provenance": "INVENTED_TRANSPORT_ONLY_NO_MARKET_OR_RAW_ACCOUNT_REPLAY", **body}, handle,
                sort_keys=True, indent=2, allow_nan=False)


def test_transport_editions_preserve_legacy_constants_and_policy():
    assert winner.CURRENT_GENERATION.selection_version.endswith("v15")
    assert winner.MAX_REQUEST_BYTES == 16 * 1024 * 1024
    assert winner.MAX_JSON_NODES == 1_000_000
    assert winner.MAX_TOTAL_COMPLETED_TRADES == 65_536
    assert winner.MAX_TOTAL_DAILY_POINTS == 262_144
    assert winner.MAX_TRADES_PER_FOLD == 4096
    assert winner.MAX_DEPENDENCE_BLOCK_OPERATIONS == 50_000_000
    assert winner.V16_GENERATION.policy_sha256 == winner.CURRENT_GENERATION.policy_sha256
    assert winner.V16_GENERATION.transport.max_request_bytes == 64 * 1024 * 1024
    assert winner.V16_GENERATION.transport.max_json_nodes == 2_000_000
    assert winner.V16_GENERATION.transport.max_total_completed_trades == 262_144
    with pytest.raises(FrozenInstanceError): winner.V16_GENERATION.transport.max_json_nodes = 3_000_000


@pytest.mark.parametrize("generation", [winner.ARCHIVED_GENERATION, winner.CURRENT_GENERATION, winner.V16_GENERATION], ids=["v14", "v15", "v16"])
def test_transport_count_budget_is_independent_of_power(generation):
    winner.check_completed_trade_transport_count(generation.transport.max_total_completed_trades, generation=generation)
    with pytest.raises(ValueError, match=generation.transport.error_prefix + "GLOBAL_COMPLETED_TRADE_LIMIT_EXCEEDED"):
        winner.check_completed_trade_transport_count(generation.transport.max_total_completed_trades + 1, generation=generation)
    assert winner.MAX_TOTAL_COMPLETED_TRADES + 1 == 65_537
    power = winner._completed_trade_power_account({"folds": []}, 300, _generation=generation)
    assert power["derived_required"] == power["required"] == 65_537


def test_unrecognised_resource_generation_cannot_override_budget():
    forged = replace(winner.V16_GENERATION, transport=replace(winner.V16_TRANSPORT_ENVELOPE, max_total_completed_trades=999_999))
    with pytest.raises(ValueError, match="WINNER_TRANSPORT_GENERATION_INVALID"):
        winner.check_completed_trade_transport_count(262_145, generation=forged)


def test_raw_successor_interfaces_preserve_legacy_metadata_defaults():
    assert raw.CURRENT_FROZEN_PLAN_SCHEMA.endswith("v4") and raw.CURRENT_PRODUCTION_SCHEMA.endswith("v5")
    assert raw.V16_FROZEN_PLAN_SCHEMA.endswith("v5") and raw.V16_PRODUCTION_SCHEMA.endswith("v6")
    for old, new in ((raw.freeze_winner_search_plan_v4, raw.freeze_winner_search_plan_v5),
            (raw.build_frozen_winner_search_plan_v4_receipt, raw.build_frozen_winner_search_plan_v5_receipt),
            (raw.restore_frozen_winner_search_plan_v4, raw.restore_frozen_winner_search_plan_v5),
            (raw.produce_winner_v15_raw_evidence, raw.produce_winner_v16_raw_evidence)):
        assert inspect.signature(old).parameters == inspect.signature(new).parameters
    for name in ("freeze_winner_search_plan_v3", "produce_winner_v14_raw_evidence"):
        with pytest.raises(raw.WinnerRawEvidenceError, match="ARCHIVED_WINNER_V14_WRITER_RETIRED_FROZEN_TIER_PAIR_REQUIRED"):
            getattr(raw, name)()


@pytest.mark.parametrize("generation", [winner.CURRENT_GENERATION, winner.V16_GENERATION], ids=["v15", "v16"])
def test_raw_producer_uses_the_same_independent_event_guard(generation):
    limit = generation.transport.max_total_completed_trades
    raw._check_cumulative_completed_trades(limit, generation)
    with pytest.raises(raw.WinnerRawEvidenceError, match=generation.transport.error_prefix + "GLOBAL_COMPLETED_TRADE_LIMIT_EXCEEDED"):
        raw._check_cumulative_completed_trades(limit + 1, generation)
    if generation is winner.CURRENT_GENERATION:
        refusal = winner._rejected("DEPENDENCE_OPERATION_LIMIT_EXCEEDED", None, _generation=generation)
        with pytest.raises(raw.WinnerRawEvidenceError) as caught:
            raw._verify_produced_winner_decision(refusal, None, generation)
        assert caught.value.code == str(caught.value) == "WINNER_V15_DECISION_VERIFICATION_FAILED"


def test_explicit_v16_builder_rejects_old_precommitment():
    value = fixtures._current_payload_request(pair_contract=synthetic_pair())
    with pytest.raises(ValueError, match="V16_PRECOMMITMENT_VERSION_REQUIRED"):
        winner.create_winner_selection_request_v16(value["precommitment_payload"], value["attempt_ledger_payload"], value["evidence_ledger_payload"])


def test_v16_request_byte_budget_is_not_legacy_global():
    chunk = "X" * (1024 * 1024)
    old = {"contract_version": winner.CURRENT_GENERATION.precommitment_version}
    evidence = {"synthetic_padding": [chunk] * 17}
    assert winner.create_winner_selection_request(old, {}, evidence) is None
    new = dict(old, contract_version=winner.V16_GENERATION.precommitment_version)
    assert winner.create_winner_selection_request_v16(new, {}, evidence) is not None
    evidence["synthetic_padding"] = [chunk] * 65
    with pytest.raises(ValueError, match="V16_INPUT_STRING_BUDGET_EXCEEDED"):
        winner.create_winner_selection_request_v16(new, {}, evidence)


def test_node_envelope_is_edition_specific_with_unchanged_width():
    value = [[0] * 3999 for _ in range(250)]
    assert fixtures._json_node_count(value) == 1_000_001
    with pytest.raises(ValueError, match="INPUT_STRUCTURE_LIMIT_EXCEEDED"): winner._owned_json(value)
    winner._owned_json(value, winner.MAX_SUPPORTED_WINNER_REQUEST_BYTES, _envelope=winner.V16_TRANSPORT_ENVELOPE)
    value.extend([[0] * 3999 for _ in range(250)])
    assert fixtures._json_node_count(value) == 2_000_001
    with pytest.raises(ValueError, match="V16_INPUT_STRUCTURE_LIMIT_EXCEEDED"):
        winner._owned_json(value, winner.MAX_SUPPORTED_WINNER_REQUEST_BYTES, _envelope=winner.V16_TRANSPORT_ENVELOPE)


@pytest.mark.parametrize("edition", ["v14", "v15"])
def test_same_environment_preimage_semantic_parity(edition):
    path = os.environ.get("FXPM_WINNER_PREIMAGE_SELECTION")
    if not path: pytest.skip("QA-owned exact preimage source not supplied")
    name = "_fxpm_transport_preimage_winner_selection"
    spec = importlib.util.spec_from_file_location(name, path)
    previous = importlib.util.module_from_spec(spec)
    sys.modules[name] = previous
    try:
        spec.loader.exec_module(previous)
        value = fixtures._payload_request() if edition == "v14" else fixtures._current_payload_request(pair_contract=synthetic_pair())
        before, after = _typed(previous, value), _typed(winner, value)
        assert _bytes(before.to_dict()) == _bytes(after.to_dict()) and before.request_sha256 == after.request_sha256
        old_decision, new_decision = previous.evaluate_historical_cell_winner(before), winner.evaluate_historical_cell_winner(after)
        assert old_decision._body_json == new_decision._body_json and old_decision.receipt_sha256 == new_decision.receipt_sha256
        bad = deepcopy(value)
        bad["evidence_ledger_payload"]["entries_sha256"] = "0" * 64
        old_refusal = previous.evaluate_historical_cell_winner(_typed(previous, bad))
        new_refusal = winner.evaluate_historical_cell_winner(_typed(winner, bad))
        assert old_refusal._body_json == new_refusal._body_json and old_refusal.receipt_sha256 == new_refusal.receipt_sha256
        _record("legacy_parity_" + edition, {"edition": edition, "request_sha256": after.request_sha256,
            "canonical_request_bytes_equal": True, "valid_decision_sha256": new_decision.receipt_sha256,
            "refusal_decision_sha256": new_refusal.receipt_sha256, "valid_and_refusal_bytes_equal": True, "selector_calls": 4,
            "complete_semantic_closes": 2, "refused_selector_calls": 2})
    finally:
        sys.modules.pop(name, None)


@pytest.mark.parametrize("evaluated,total", [(297, 262_144), (297, 262_145), (300, 262_145)])
def test_full_roster_cumulative_event_boundary(evaluated, total):
    payload = build_v16_capacity_payload(total, evaluated_attempt_count=evaluated)
    request = _typed(winner, payload)
    if total > 262_144:
        with pytest.raises(ValueError, match="V16_GLOBAL_COMPLETED_TRADE_LIMIT_EXCEEDED"): validate_v16_transport(payload, request=request)
    else:
        assert validate_v16_transport(payload, request=request) is not None
    _record(f"boundary_{evaluated}_{total}", {"evaluated_attempts": evaluated, "rejected_attempts": 300 - evaluated,
        **_shape(payload, request), "selector_inference_calls": 0})


def test_concentrated_transport_keeps_existing_work_stop(monkeypatch):
    selector_calls = []
    original_evaluate = winner._evaluate_body

    def counted_evaluate(*args, **kwargs):
        selector_calls.append("evaluate_body")
        return original_evaluate(*args, **kwargs)

    monkeypatch.setattr(winner, "_evaluate_body", counted_evaluate)
    payload = build_v16_capacity_payload(262_144, concentrated=True)
    request = _typed(winner, payload)
    with pytest.raises(ValueError, match="DEPENDENCE_OPERATION_LIMIT_EXCEEDED"): validate_v16_transport(payload, request=request)
    decision = winner.evaluate_historical_cell_winner(request)
    assert decision.reason_codes == ("DEPENDENCE_OPERATION_LIMIT_EXCEEDED",)
    with pytest.raises(raw.WinnerRawEvidenceError, match="WINNER_V16_DECISION_VERIFICATION_FAILED: DEPENDENCE_OPERATION_LIMIT_EXCEEDED"):
        raw._verify_produced_winner_decision(decision, request, winner.V16_GENERATION)
    assert len(selector_calls) == 2
    _record("concentrated_stop", {**_shape(payload, request), "expected_refusal": "DEPENDENCE_OPERATION_LIMIT_EXCEEDED",
        "raw_failure_code": "WINNER_V16_DECISION_VERIFICATION_FAILED", "work_limit": 50_000_000,
        "selector_inference_calls": len(selector_calls), "selector_stages": ["evaluate_rejected", "raw_rejected_decision_verification"]})


@pytest.mark.parametrize("profile", ["baseline", "legacy_ceiling", "v16_ceiling"])
def test_complete_selector_close_and_cold_spool(profile, tmp_path, monkeypatch):
    start, stages = time.perf_counter(), {}
    selector_calls, selector_stage = {}, {"name": "synthetic_assembly"}
    original_evaluate = winner._evaluate_body

    def counted_evaluate(*args, **kwargs):
        stage = selector_stage["name"]
        selector_calls[stage] = selector_calls.get(stage, 0) + 1
        return original_evaluate(*args, **kwargs)

    monkeypatch.setattr(winner, "_evaluate_body", counted_evaluate)
    memory_before = _memory()
    assert memory_before["available"] is True
    memory_stages = {"before_assembly": memory_before}
    disk_before = shutil.disk_usage(tmp_path.anchor).free
    value = (build_v16_payload() if profile == "baseline" else
        build_v16_capacity_payload(65_536 if profile == "legacy_ceiling" else 262_144))
    stages["synthetic_assembly_seconds"] = time.perf_counter() - start
    memory_stages["after_assembly"] = _memory()
    at = time.perf_counter()
    selector_stage["name"] = "request_creation"
    request = _typed(winner, value)
    stages["request_creation_seconds"] = time.perf_counter() - at
    memory_stages["after_request_creation"] = _memory()
    at = time.perf_counter()
    selector_stage["name"] = "selector_evaluate"
    decision = winner.evaluate_historical_cell_winner(request)
    stages["selector_evaluate_seconds"] = time.perf_counter() - at
    assert decision.to_dict()["validated"] is True
    memory_stages["after_selector_evaluate"] = _memory()
    at = time.perf_counter()
    selector_stage["name"] = "selector_marginal"
    marginal = winner.historical_book_marginal_receipt(decision, request)
    stages["selector_marginal_seconds"] = time.perf_counter() - at
    assert marginal is not None
    if profile != "baseline":
        assert decision.status == "NO_PROCESS_WINNER" and decision.reason_codes == ("NO_P75_ADMISSIBLE_PROCESS_CANDIDATE",)
        assert marginal["marginal_p_value"] == 1.0 and marginal["local_admissibility_passed"] is False
    memory_stages["after_selector_marginal"] = _memory()
    context = spool.freeze_generation_context(tmp_path / "spool", generation_id="SYNTHETIC_TRANSPORT_" + profile,
        native_universe_sha256="1" * 64, config_sha256="2" * 64)
    sink = spool.open_symbol_sink(context, value["precommitment_payload"]["symbol"])
    at = time.perf_counter()
    selector_stage["name"] = "publication"
    namespace = "SYNTHETIC_TRANSPORT_INGRESS"
    request_id = f"{namespace}/winner-search/{value['precommitment_payload']['timeframe']}/{value['precommitment_payload']['regime']}/authority/winner-request"
    entries = {name: raw._put_ascii_json(sink, request_id if name == "request" else name, kind, body) for name, kind, body in (
        ("request", "winner_v16_request", request.to_dict()),
        ("ledger", "winner_evidence_ledger", value["evidence_ledger_payload"]),
        ("decision", "winner_v16_decision", decision.to_dict()))}
    if profile == "v16_ceiling":
        for name, kind in (("production_receipt", "winner_raw_evidence_production_receipt"),
                ("cross_binding", "current_horizon_winner_cross_binding")):
            entries[name] = raw._put_ascii_json(sink, name, kind, {})
    receipt = sink.seal()
    stages["publication_seconds"] = time.perf_counter() - at
    memory_stages["after_publication"] = _memory()
    at = time.perf_counter()
    selector_stage["name"] = "cold_reopen"
    manifest = spool.verify_symbol_receipt(context.root, receipt, expected_generation_id=context.generation_id,
        expected_symbol=sink.symbol, expected_native_universe_sha256=context.native_universe_sha256,
        expected_config_sha256=context.config_sha256, expected_manifest_domain=spool.GENERAL_MANIFEST_DOMAIN)
    assert len(manifest["artifacts"]) == (5 if profile == "v16_ceiling" else 3)
    decoded = {}
    for name in ("request", "ledger", "decision"):
        descriptor = entries[name]["descriptor"]
        limit = winner.MAX_DECISION_BYTES if name == "decision" else winner.MAX_SUPPORTED_WINNER_REQUEST_BYTES
        digest, _, encoded = spool._stream_file(context.root, descriptor["relative_path"], maximum_bytes=limit,
            expected_size=descriptor["size_bytes"], collect=True)
        assert digest == descriptor["sha256"]
        decoded[name] = json.loads(encoded.decode("ascii"))
    cold = decoded["request"]
    reopened = winner.create_winner_selection_request_v16(*[cold[key]["payload"] for key in ("precommitment", "attempt_ledger", "evidence_ledger")])
    assert reopened.request_sha256 == request.request_sha256
    assert hashlib.sha256(_bytes(cold["evidence_ledger"]["payload"])).hexdigest() == entries["ledger"]["descriptor"]["sha256"]
    assert _bytes(decoded["ledger"]) == _bytes(cold["evidence_ledger"]["payload"])
    del decoded["ledger"]
    assert decoded["decision"] == decision.to_dict()
    cold_decision_body = dict(decoded["decision"])
    cold_decision_receipt = cold_decision_body.pop("receipt_sha256")
    cold_decision_raw = _bytes(cold_decision_body)
    assert cold_decision_raw == decision._body_json
    assert hashlib.sha256(cold_decision_raw).hexdigest() == cold_decision_receipt == decision.receipt_sha256
    cold_decision = winner.WinnerSelectionDecision(cold_decision_raw, cold_decision_receipt)
    stages["cold_reopen_seconds"] = time.perf_counter() - at
    memory_stages["after_cold_reopen"] = _memory()
    at = time.perf_counter()
    selector_stage["name"] = "selector_cold_verify"
    assert winner.verify_historical_cell_winner_decision(cold_decision, reopened)
    stages["selector_cold_verify_seconds"] = time.perf_counter() - at
    memory_stages["after_selector_cold_verify"] = _memory()
    ingress = {"executed": False}
    if profile == "v16_ceiling":
        at = time.perf_counter()
        ingress = _large_reader_ingress(context, manifest, entries, request, decision, namespace, selector_stage)
        stages["actual_reader_ingress_seconds"] = time.perf_counter() - at
        memory_stages["after_actual_reader_ingress"] = _memory()
    tiers = [tier for entry in value["evidence_ledger_payload"]["entries"] for fold in entry["folds"] for tier in fold["tier_evidence"].values()]
    request_nodes = fixtures._json_node_count(cold)
    evidence_nodes = fixtures._json_node_count(cold["evidence_ledger"]["payload"])
    slot_bytes = sum(path.stat().st_size for path in context.root.rglob("*") if path.is_file())
    profile_stages = ("selector_evaluate", "selector_marginal", "selector_cold_verify")
    assert all(selector_calls.get(stage, 0) == 1 for stage in profile_stages)
    reader_calls = {stage: count for stage, count in selector_calls.items() if stage not in profile_stages}
    assert reader_calls == ({"adapter_rebuild": 1, "current_worker_rebuild": 2} if profile == "v16_ceiling" else {})
    ingress["observed_additional_selector_evaluations"] = sum(reader_calls.values())
    _record("control_" + profile, {"stages": stages, "total_seconds": time.perf_counter() - start,
        "selector_inference_calls": sum(selector_calls[stage] for stage in profile_stages),
        "observed_selector_call_census": selector_calls, "observed_all_selector_evaluation_calls": sum(selector_calls.values()),
        "timing_instrumentation": "TINY_NAMED_STAGE_COUNTER_AROUND_UNCHANGED_EVALUATE_BODY_INCLUDED_IN_TIMING",
        "request_bytes": entries["request"]["descriptor"]["size_bytes"], "request_nodes": request_nodes,
        "evidence_payload_nodes": evidence_nodes, "artifact_bytes": {key: entry["descriptor"]["size_bytes"] for key, entry in entries.items()},
        "attempts": len(value["attempt_ledger_payload"]["attempts"]), "completed_trade_events": sum(len(t["completed_trade_close_offset_seconds"]) for t in tiers),
        "maximum_per_fold_trade_events": max(len(t["completed_trade_close_offset_seconds"]) for t in tiers),
        "daily_points_per_attempt": sum(len(axis["daily_dates_utc"]) for axis in value["evidence_ledger_payload"]["shared_fold_axis"]),
        "total_evaluated_daily_points": sum(len(axis["daily_dates_utc"]) for axis in value["evidence_ledger_payload"]["shared_fold_axis"]) *
            sum(bool(entry["folds"]) for entry in value["evidence_ledger_payload"]["entries"]),
        "memory_before": memory_before, "memory_after": _memory(), "memory_stages": memory_stages,
        "memory_peak_scope": "FRESH_PROCESS_LIFETIME_INCLUDING_IMPORTS_AND_THIS_SINGLE_CONTROL",
        "slot_retained_regular_file_bytes": slot_bytes, "thirteen_slot_retained_byte_projection": 13 * slot_bytes,
        "disk_projection_scope": "THIRTEEN_IDENTICAL_SYNTHETIC_SPOOLS_ONLY_NOT_PRODUCTION_CAPACITY",
        "c_free_bytes_before_control": disk_before, "c_free_bytes_after_control": shutil.disk_usage(tmp_path.anchor).free,
        "marginal_available": True, "reader_ingress": ingress,
        "real_spool_cold_transport": True, "authentic_raw_producer_close": False,
        "remaining_gap": "NO_NATIVE_ACCOUNT_OR_AUTHENTIC_RAW_PRODUCER_CHAIN_EXECUTED"})


def _large_reader_ingress(context, manifest, entries, request, decision, namespace, selector_stage):
    """Real descriptor reads; deliberately incomplete synthetic outer custody."""
    import pm_book_multiplicity as book
    import pm_current_refit_search_producer as current
    import pm_forward_oos_replay_producer as forward
    import pm_historical_production_adapter as adapter

    assert winner.MAX_REQUEST_BYTES < entries["request"]["descriptor"]["size_bytes"] <= winner.MAX_SUPPORTED_WINNER_REQUEST_BYTES
    cell = request.precommitment.to_dict()
    identity = {key: cell[key] for key in ("symbol", "timeframe", "regime")}
    selector_stage["name"] = "adapter_rebuild"
    rebuilt_request, rebuilt_decision = adapter._rebuild_winner_pair(context,
        {"request": entries["request"]["descriptor"], "request_sha256": request.request_sha256,
         "decision": entries["decision"]["descriptor"], "decision_sha256": decision.receipt_sha256}, **identity)
    assert _bytes(rebuilt_request.to_dict()) == _bytes(request.to_dict())
    assert rebuilt_decision._body_json == decision._body_json
    del rebuilt_request, rebuilt_decision
    bindings = {name: current._artifact_binding(entries[alias]) for name, alias in (
        ("winner_request", "request"), ("winner_decision", "decision"),
        ("production_receipt", "production_receipt"), ("cross_binding", "cross_binding"))}
    selector_stage["name"] = "current_worker_rebuild"
    with pytest.raises(current.CurrentRefitSearchProducerError) as caught:
        current._rebuild_raw_winner_artifacts(context=context, manifest=manifest, bindings=bindings,
            expected_decision_sha256=decision.receipt_sha256, **identity)
    assert caught.value.code == "CURRENT_WORKER_CROSS_BINDING_INVALID"
    book_cell = {**identity, "cell_id": cell["cell_id"], "historical_state": "WINNER",
        "winner_request_sha256": request.request_sha256, "winner_decision_sha256": decision.receipt_sha256}
    record = {**identity, "historical_state": "WINNER_DECISION", "raw_artifact_namespace": namespace,
        "winner_request_sha256": request.request_sha256, "winner_decision_sha256": decision.receipt_sha256}
    selector_stage["name"] = "forward_request_ingress"
    with pytest.raises(forward.ForwardOOSReplayProductionError) as caught:
        forward._selected_artifacts(context=context,
            entries={entries["request"]["artifact_id"]: entries["request"]}, anchor={},
            book_body={"contract_version": book.V16_GENERATION.version}, book_cell=book_cell,
            index={"cell_records": [record]})
    assert caught.value.code == "WINNER_DECISION_MISSING"
    return {"executed": True, "request_bytes": entries["request"]["descriptor"]["size_bytes"],
        "adapter": "REQUEST_AND_DECISION_REOPENED_BYTE_EXACT",
        "current_worker": "REAL_REQUEST_AND_DECISION_READ_THEN_INTENTIONAL_CROSS_BINDING_REFUSAL",
        "forward": "REAL_REQUEST_READ_THEN_INTENTIONAL_MISSING_DECISION_REFUSAL",
        "forward_book_metadata": "INTENTIONALLY_INCOMPLETE_UNVERIFIED_SYNTHETIC_READER_INPUT",
        "authentic_outer_custody_or_raw_producer": False}
