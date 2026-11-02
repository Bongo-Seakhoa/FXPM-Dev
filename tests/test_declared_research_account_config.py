"""Owned synthetic regression inputs for declared research account config binding.

SOURCE-ONLY PREPARATION: no fixture has been materialized or test run. These
normal imports must resolve the separately reviewed product edition at QA time;
this file supplies no alternate module, verifier, Winner or account producer.
All future fixture writes stay under pytest tmp_path. No native/terms/profile
input is needed. Invented document flags grant no authority.
"""
from __future__ import annotations

from dataclasses import asdict
from datetime import datetime, timedelta, timezone
import hashlib
import json
import os
from pathlib import Path
from types import SimpleNamespace

import pytest

import pm_forward_oos_replay_producer as producer
import pm_production_config_binding as config_binding


LABEL = "SYNTHETIC_SOFTWARE_CONFIG_BINDING_REGRESSION_NOT_QUALIFICATION"
TECHNICAL_SCOPE = "UNISSUED_TECHNICAL_BENCHMARK"
ABSENT = object()
AUTHORITY = ("qualification_authority", "capital_authority", "live_authority", "order_authority")
POLICY_MISSING = "DEPLOYMENT_RISK_OR_SCOPE_POLICY_MISSING"
CHANGED = "DEPLOYMENT_OR_BROKER_CONFIG_CHANGED_DURING_REPLAY"


def _write_json(path, value):
    # Deliberate formatting/newline makes raw-byte custody distinct from the
    # canonical semantic body; binding must retain the actual written hash.
    encoded = (json.dumps(value, ensure_ascii=True, indent=2) + "\n").encode("utf-8")
    path.write_bytes(encoded)
    return encoded


def _case(tmp_path, *, position=ABSENT, research=True, scope=TECHNICAL_SCOPE):
    root = tmp_path.resolve(strict=True)
    broker_path = root / "broker_specs.json"
    broker_raw = _write_json(broker_path, {
        "EURUSD": {"symbol": "EURUSD", "fixture_marker": "broker-A",
                   "fixture_provenance": LABEL},
    })
    payload = {
        "_meta": {"scope": scope, "fixture_provenance": LABEL,
                  **{name: False for name in AUTHORITY}, "fixture_marker": "raw-A"},
        "symbols": ["EURUSD"],
        "broker_specs_path": "broker_specs.json",
        "pipeline": {
            "data_dir": str(root / "data"), "output_dir": str(root / "outputs"),
            "log_dir": str(root / "logs"), "research_mode": research,
            "feature_disk_cache_dir": str(root / "unused-feature-cache"),
            "winner_ledger_path": str(root / "unused-ledger.json"),
            "regime_params_file": str(root / "unused-regime-defaults.json"),
            "initial_capital": 10000.0, "discovery_risk_per_trade_pct": 1.0,
            "risk_per_trade_pct": 0.25, "use_spread": True,
            "backtest_spread_mode": "conservative_max",
        },
    }
    if position is not ABSENT:
        payload["position"] = position
    path = root / "config.json"
    raw = _write_json(path, payload)
    loaded, config = config_binding.load_effective_pipeline_config(path)
    assert loaded == payload
    assert type(config) is producer.PipelineConfig
    assert config.research_mode is research
    assert config._funded_live_execution is False
    for field in ("data_dir", "output_dir", "log_dir", "feature_disk_cache_dir",
                  "winner_ledger_path", "regime_params_file"):
        assert Path(getattr(config, field)).resolve().is_relative_to(root)
    assert Path(config._deployment_config_path).resolve() == path.resolve()
    return SimpleNamespace(root=root, path=path, raw=raw, payload=payload, config=config,
                           broker_path=broker_path, broker_raw=broker_raw)


def _assert_raw_binding(case, binding):
    assert case.path.read_bytes() == case.raw
    assert case.broker_path.read_bytes() == case.broker_raw
    assert binding.config_body == case.payload
    assert binding.config_path == case.path.resolve()
    assert binding.config_sha256 == hashlib.sha256(case.raw).hexdigest()
    assert binding.config_size_bytes == len(case.raw)
    assert binding.broker_path == case.broker_path.resolve()
    assert binding.broker_relative_path == "broker_specs.json"
    assert binding.broker_sha256 == hashlib.sha256(case.broker_raw).hexdigest()
    assert binding.broker_size_bytes == len(case.broker_raw)
    assert binding.pipeline_config_identity == producer._pipeline_config_identity(case.config)
    assert binding.pipeline_config_identity_sha256 == producer._sha(binding.pipeline_config_identity)
    receipt = binding.receipt_body()
    assert receipt["deployment_config_sha256"] == binding.config_sha256
    assert receipt["deployment_config_size_bytes"] == len(case.raw)
    assert receipt["broker_specs_sha256"] == binding.broker_sha256
    assert receipt["broker_specs_size_bytes"] == len(case.broker_raw)
    assert all(binding.config_body["_meta"][key] is False for key in AUTHORITY)
    return receipt


def test_missing_position_is_strictly_refused_but_declared_research_binds_raw_unchanged(tmp_path):
    case = _case(tmp_path)
    initial = asdict(case.config)
    with pytest.raises(producer.ForwardOOSReplayProductionError, match="^" + POLICY_MISSING + "$"):
        producer._deployment_config(case.config)
    binding = producer._deployment_config(case.config, declared_research_account=True)
    receipt = _assert_raw_binding(case, binding)
    assert "position" not in binding.config_body
    assert "position" not in json.loads(case.path.read_bytes())
    assert producer._deployment_config(case.config, declared_research_account=True).receipt_body() == receipt
    producer._assert_deployment_config_unchanged(binding, case.config, declared_research_account=True)
    with pytest.raises(producer.ForwardOOSReplayProductionError, match="^" + POLICY_MISSING + "$"):
        producer._assert_deployment_config_unchanged(binding, case.config)
    assert asdict(case.config) == initial
    assert case.config._funded_live_execution is False


@pytest.mark.parametrize("scope", [TECHNICAL_SCOPE, "funded_live_v1"], ids=["technical", "funded"])
def test_explicit_empty_position_has_identical_strict_and_declared_receipts(tmp_path, scope):
    case = _case(tmp_path, position={}, scope=scope)
    initial = asdict(case.config)
    strict = producer._deployment_config(case.config)
    declared = producer._deployment_config(case.config, declared_research_account=True)
    assert strict.receipt_body() == declared.receipt_body()
    assert strict.config_body == declared.config_body == case.payload
    assert declared.config_body["position"] == {}
    assert _assert_raw_binding(case, strict) == _assert_raw_binding(case, declared)
    producer._assert_deployment_config_unchanged(strict, case.config)
    producer._assert_deployment_config_unchanged(declared, case.config, declared_research_account=True)
    assert asdict(case.config) == initial


@pytest.mark.parametrize("position", [None, [], 7, "explicit-scalar", False],
                         ids=["null", "list", "integer", "string", "boolean"])
@pytest.mark.parametrize("declared", [False, True], ids=["strict", "declared"])
def test_explicit_malformed_position_is_never_treated_as_absent(tmp_path, position, declared):
    case = _case(tmp_path, position={})
    # Start with a real public-loader config, then exercise the reader's fresh
    # invalid-document refusal; no fake config object or successful stub exists.
    changed = dict(case.payload)
    changed["position"] = position
    invalid_raw = _write_json(case.path, changed)
    with pytest.raises(producer.ForwardOOSReplayProductionError, match="^" + POLICY_MISSING + "$"):
        producer._deployment_config(case.config, declared_research_account=declared)
    assert case.path.read_bytes() == invalid_raw


@pytest.mark.parametrize("mode", ["nonresearch", "funded", "funded-scope"],
                         ids=["nonresearch", "funded-carrier", "funded-scope"])
def test_missing_position_exception_excludes_nonresearch_and_funded_carriers(tmp_path, mode):
    scope = "  FuNdEd_LiVe_V1  " if mode == "funded-scope" else TECHNICAL_SCOPE
    case = _case(tmp_path, research=mode != "nonresearch", scope=scope)
    if mode == "funded":
        # Negative carrier only: the real loader deliberately returns False.
        # This sets the input exclusion flag; it creates no funded capability.
        case.config._funded_live_execution = True
    with pytest.raises(producer.ForwardOOSReplayProductionError, match="^" + POLICY_MISSING + "$"):
        producer._deployment_config(case.config, declared_research_account=True)
    assert case.path.read_bytes() == case.raw
    assert case.broker_path.read_bytes() == case.broker_raw


def test_declared_refresh_rejects_same_length_same_mtime_raw_mutation(tmp_path):
    case = _case(tmp_path)
    binding = producer._deployment_config(case.config, declared_research_account=True)
    before = binding.receipt_body()
    metadata = case.path.stat()
    changed = case.raw.replace(b'"raw-A"', b'"raw-B"')
    assert changed != case.raw and len(changed) == len(case.raw)
    case.path.write_bytes(changed)
    os.utime(case.path, ns=(metadata.st_atime_ns, metadata.st_mtime_ns))
    with pytest.raises(producer.ForwardOOSReplayProductionError, match="^" + CHANGED + "$"):
        producer._assert_deployment_config_unchanged(binding, case.config, declared_research_account=True)
    assert case.path.read_bytes() == changed
    assert binding.receipt_body() == before


def test_declared_refresh_rejects_same_length_broker_mutation(tmp_path):
    case = _case(tmp_path)
    binding = producer._deployment_config(case.config, declared_research_account=True)
    before = binding.receipt_body()
    changed = case.broker_raw.replace(b'"broker-A"', b'"broker-B"')
    assert changed != case.broker_raw and len(changed) == len(case.broker_raw)
    case.broker_path.write_bytes(changed)
    with pytest.raises(producer.ForwardOOSReplayProductionError, match="^" + CHANGED + "$"):
        producer._assert_deployment_config_unchanged(binding, case.config, declared_research_account=True)
    assert case.broker_path.read_bytes() == changed
    assert case.path.read_bytes() == case.raw
    assert binding.receipt_body() == before


def test_declared_exception_keeps_initial_capital_drift_refusal(tmp_path):
    case = _case(tmp_path)
    changed = json.loads(case.raw)
    changed["pipeline"]["initial_capital"] = 9000.0
    _write_json(case.path, changed)
    with pytest.raises(producer.ForwardOOSReplayProductionError, match="^PIPELINE_CONFIG_DEPLOYMENT_DRIFT$"):
        producer._deployment_config(case.config, declared_research_account=True)


def test_declared_binding_preserves_risk_h364_and_false_authority_surface(tmp_path):
    case = _case(tmp_path)
    initial = asdict(case.config)
    binding = producer._deployment_config(case.config, declared_research_account=True)
    assert asdict(case.config) == initial
    assert case.config.initial_capital == 10000.0
    assert case.config.discovery_risk_per_trade_pct == 1.0
    assert case.config.risk_per_trade_pct == 0.25
    measured = producer._declared_cell_measured_config(case.config)
    assert measured.initial_capital == 10000.0
    assert measured.discovery_risk_per_trade_pct == 0.25
    assert case.config.discovery_risk_per_trade_pct == 1.0
    start = datetime(2023, 6, 24, tzinfo=timezone.utc)
    parent = {"slot_roster": {"slots": [
        {"ordinal": ordinal + 1,
         "activation_utc": (start + timedelta(days=28 * ordinal)).isoformat(),
         "end_exclusive_utc": (start + timedelta(days=28 * (ordinal + 1))).isoformat()}
        for ordinal in range(13)]}}
    measurement = producer._declared_cell_measurement(parent)
    assert (measurement["calendar_days"], measurement["refit_cycles"], measurement["cycle_days"]) == (364, 13, 28)
    assert measurement["risk_per_trade_pct"] == 0.25
    assert measurement["terminal_policy"] == "MARK_OPEN"
    assert all(binding.config_body["_meta"][key] is False for key in AUTHORITY)
    assert asdict(case.config) == initial
    assert case.config._funded_live_execution is False
    _assert_raw_binding(case, binding)
