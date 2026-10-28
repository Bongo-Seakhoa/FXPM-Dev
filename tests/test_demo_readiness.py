import hashlib
import inspect
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest

import pm_demo_readiness as demo
import pm_live_readiness as live
from pm_position import PositionConfig
from pm_regime import RegimeParams, detector_code_sha256


SHA = "a" * 64
VALID_UNTIL = "2099-01-01T00:00:00+00:00"


def _config():
    return SimpleNamespace(
        max_total_open_risk_pct=8.0,
        max_factor_group_open_risk_pct=2.0,
        max_combined_risk_pct=1.0,
        margin_entry_block_level=200.0,
    )


def _full_config(tmp_path, ledger_path):
    return SimpleNamespace(
        **vars(_config()),
        production_symbol_pool_target_count=1,
        timeframes=["M5", "M15", "M30", "H1", "H4", "D1"],
        research_mode=False,
        production_split_mode="calendar_lockbox_v2",
        evidence_sufficiency_enabled=True,
        evidence_require_native_timeframes=True,
        evidence_require_native_provenance=True,
        evidence_require_session_coverage=True,
        use_spread=True,
        swap_model_enabled=True,
        backtest_spread_mode="conservative_max",
        winner_ledger_path=str(ledger_path),
        data_dir=tmp_path,
        live_require_tuned_regime_params=False,
        manual_new_entries_enabled=True,
        manual_kill_switch_path="",
    )


def _position_config():
    return PositionConfig(
        risk_per_trade_pct=0.5,
        max_risk_pct=1.0,
        risk_tolerance_pct=10.0,
    )


def _write_config(path: Path):
    path.write_text(
        json.dumps(
            {
                "_meta": {"scope": "funded_live_v1"},
                "position": vars(_position_config()),
            }
        ),
        encoding="utf-8",
    )


def test_demo_cost_prerequisites_are_spread_and_swap_only():
    source = inspect.getsource(demo.validate_demo_probation_readiness)
    assert "use_commission" not in source
    assert "use_slippage" not in source
    assert "slippage_pips" not in source
    assert '"use_spread"' in source
    assert '"swap_model_enabled"' in source


def test_demo_position_slippage_buffer_is_independent_of_pipeline_fields():
    position = PositionConfig(
        risk_per_trade_pct=0.5,
        max_risk_pct=1.0,
        risk_tolerance_pct=10.0,
        use_slippage=True,
        slippage_pips=0.75,
    )
    config_payload = {
        "_meta": {"scope": "funded_live_v1"},
        "position": vars(position),
    }
    config = SimpleNamespace(
        max_combined_risk_pct=1.0,
        use_spread=True,
        use_slippage=False,
        slippage_pips=99.0,
    )

    expected = demo._expected_position_policy(config_payload, config)

    assert expected["use_slippage"] is True
    assert expected["slippage_pips"] == 0.75
    assert demo._position_policy_errors(expected, config) == []


def test_demo_position_slippage_buffer_fails_closed_when_disabled_or_negative():
    payload = demo._position_policy_payload(_position_config())
    config = SimpleNamespace(max_combined_risk_pct=1.0, use_spread=True)

    disabled = dict(payload, use_slippage=False)
    assert "DEMO_POSITION_SLIPPAGE_BUFFER_DISABLED" in (
        demo._position_policy_errors(disabled, config)
    )

    negative = dict(payload, slippage_pips=-0.01)
    assert "DEMO_POSITION_SLIPPAGE_BUFFER_INVALID" in (
        demo._position_policy_errors(negative, config)
    )


def _ledger():
    binding = {name: SHA for name in live._CURRENT_REFIT_BINDING_FIELDS}
    params = RegimeParams().to_dict()
    params_text = json.dumps(params, sort_keys=True, separators=(",", ":"))
    record = {
        "schema_version": "fxpm.regime-detector-record/v1",
        "symbol": "EURUSD",
        "timeframe": "H1",
        "source_group_id": demo._canonical_json_sha256(
            {
                "identity_contract": "FXPM_SYMBOL_TIMEFRAME_SOURCE_GROUP_V1",
                "symbol": "EURUSD",
                "timeframe": "H1",
            }
        ),
        "source_group_decision_sha256": SHA,
        "source_contract_sha256": SHA,
        "source_group_evidence_sha256": SHA,
        "activation_id": "activation-1",
        "activation_manifest_sha256": SHA,
        "native_source_sha256": SHA,
        "fit_cutoff_utc": "2026-07-01T00:00:00+00:00",
        "detector_version": "market-regime-detector-v1",
        "detector_code_sha256": detector_code_sha256(),
        "parameters_canonical_json": params_text,
        "parameters_sha256": hashlib.sha256(params_text.encode("ascii")).hexdigest(),
    }
    record["record_sha256"] = demo._canonical_json_sha256(record)
    binding["regime_detector_record_sha256"] = record["record_sha256"]
    binding["regime_detector_parameters_sha256"] = record["parameters_sha256"]
    binding["regime_detector_code_sha256"] = record["detector_code_sha256"]
    return {
        "EURUSD": {
            "symbol": "EURUSD",
            "valid_until": VALID_UNTIL,
            "strategy_name": "LegacyFallback",
            "timeframe": "M5",
            "parameters": {"legacy": True},
            "default_config": {
                "strategy_name": "LegacyFallback",
                "parameters": {},
            },
            "edge_challenger_book": {"self_asserted": True},
            "regime_detector_records": {"H1": record},
            "regime_configs": {
                "H1": {
                    "TREND": {
                        "strategy_name": "ExpertA",
                        "parameters": {},
                        "promotion_status": "PROCESS_VALIDATED_CURRENT_REFIT",
                        "artifact_meta": {"current_refit_binding": binding},
                        "valid_until": VALID_UNTIL,
                    }
                }
            },
        }
    }


@pytest.mark.parametrize("mutation", ["missing", "mismatch", "naive"])
def test_live_expiry_rejects_selected_cell_deadline_defects(mutation):
    ledger = _ledger()
    cell = ledger["EURUSD"]["regime_configs"]["H1"]["TREND"]
    if mutation == "missing":
        cell.pop("valid_until")
        expected = "no expiry date"
    elif mutation == "mismatch":
        cell["valid_until"] = "2098-01-01T00:00:00+00:00"
        expected = "does not match symbol deadline"
    else:
        cell["valid_until"] = "2099-01-01T00:00:00"
        expected = "not aware UTC"

    issues = live._ledger_live_expiry_issues(  # noqa: SLF001
        ledger, datetime(2026, 7, 22, tzinfo=timezone.utc)
    )

    assert any(expected in issue for issue in issues)


def _write_manifest(root: Path, ledger_path: Path):
    artifacts = {}
    for name in live.SPARSE_AUTHORITY_ARTIFACTS:
        artifact = root / f"{name}.json"
        payload = json.dumps({"component": name}).encode()
        artifact.write_bytes(payload)
        artifacts[name] = {
            "relative_path": artifact.name,
            "sha256": hashlib.sha256(payload).hexdigest(),
            "size_bytes": len(payload),
            "schema_version": live.SPARSE_AUTHORITY_ARTIFACT_SCHEMAS[name],
        }
    manifest = {
        "schema_version": "fxpm.optimization-generation.v2",
        "generation_status": "PROMOTED",
        "ledger_sha256": hashlib.sha256(ledger_path.read_bytes()).hexdigest(),
        "technical_failures": {},
        "authority_chain_receipt": {"present": True},
        "authority_artifacts": artifacts,
    }
    path = root / f"{ledger_path.stem}.generation.json"
    path.write_text(json.dumps(manifest), encoding="utf-8")


def _authority_result(ledger_path: Path):
    selected = {"EURUSD|H1|TREND"}
    possible = {
        f"EURUSD|{timeframe}|{regime}"
        for timeframe in ("M5", "M15", "M30", "H1", "H4", "D1")
        for regime in ("TREND", "RANGE", "BREAKOUT", "CHOP")
    }
    unobserved = {"EURUSD|H1|CHOP"}
    return {
        "schema_version": live.SPARSE_AUTHORITY_RESULT_SCHEMA,
        "status": demo.DEMO_PROBATION_AUTHORITY_STATUS,
        "errors": [],
        "raw_reverification": {
            name: True for name in live.SPARSE_AUTHORITY_RAW_REVERIFICATION
        },
        "selected_cell_ids": sorted(selected),
        "source_unavailable_cell_ids": [],
        "regime_unobserved_cell_ids": sorted(unobserved),
        "no_local_winner_cell_ids": sorted(possible - selected - unobserved),
        "insufficient_evidence_cell_ids": [],
        "technical_cell_ids": [],
        "unsearched_cell_ids": [],
        "missing_observed_cell_ids": [],
        "possible_slot_count": 24,
        "native_activation_id": "activation-1",
        "native_activation_manifest_sha256": SHA,
        "ledger_sha256": hashlib.sha256(ledger_path.read_bytes()).hexdigest(),
        "selected_capital_envelope": {
            "max_total_stop_risk_pct": 8.0,
            "max_factor_stop_risk_pct": 2.0,
            "max_same_symbol_stop_risk_pct": 1.0,
            "max_margin_utilization_pct": 50.0,
            "min_margin_level_pct": 200.0,
            "min_free_margin_pct": 50.0,
            "gap_stress_multiple": 2.0,
        },
        "capital_config_matches": True,
        "capital_execution_enforcement_reverified": True,
        "funded_live_authorized": False,
    }


def _verify(tmp_path, monkeypatch, result, *, terminal_timeframes=()):
    ledger = _ledger()
    ledger_path = tmp_path / "probation.json"
    ledger_path.write_text(json.dumps(ledger), encoding="utf-8")
    _write_manifest(tmp_path, ledger_path)
    result["ledger_sha256"] = hashlib.sha256(ledger_path.read_bytes()).hexdigest()
    calls = []
    module = SimpleNamespace(
        verify_live_readiness_authority_chain=lambda **kwargs: (
            calls.append(kwargs) or result
        )
    )
    monkeypatch.setitem(sys.modules, "pm_optimization_authority", module)
    native = SimpleNamespace(
        activation_id="activation-1",
        activation_manifest_sha256=SHA,
        entries={"EURUSD": {"H1": {"bars_sha256": SHA}}},
        cell_status=lambda _symbol, timeframe: (
            "technical_error" if timeframe in terminal_timeframes else "active"
        ),
    )
    verified, errors = demo.verify_demo_probation_authority(
        config=_config(),
        config_path=str(tmp_path / "config.json"),
        config_payload={},
        ledger=ledger,
        ledger_path=ledger_path,
        requested_symbols=["EURUSD"],
        selected_cells={"EURUSD|H1|TREND"},
        native_universe=native,
        validation_time_utc="2026-07-14T00:00:00Z",
    )
    return verified, errors, calls


def _install_native(monkeypatch, resolver):
    monkeypatch.setitem(
        sys.modules,
        "pm_native_activation",
        SimpleNamespace(
            NATIVE_EVIDENCE_CANDIDATE="evidence_candidate",
            NATIVE_SOURCE_UNAVAILABLE="source_unavailable",
            resolve_active_native_universe=resolver,
            resolve_active_native_artifact=lambda *_args, **_kwargs: SimpleNamespace(
                valid=True
            ),
        ),
    )


def _install_management(monkeypatch, *, block_reverify: bool = False):
    import pm_demo_management as management

    capsule_id = "b" * 64
    generation_id = "generation-test"
    calls = []

    def create(store, **kwargs):
        Path(store).mkdir(parents=True, exist_ok=True)
        calls.append(("create", kwargs))
        return {
            "status": management.CAPSULE_WRITTEN,
            "errors": [],
            "capsule_id": capsule_id,
            "generation_id": generation_id,
        }

    def reverify(store, supplied_capsule_id, **kwargs):
        calls.append(("reverify", kwargs))
        if block_reverify:
            return {"status": management.BLOCKED, "errors": ["RAW_BLOCKED"]}
        context = {
            "schema_version": management.POSITION_MANAGEMENT_CONTEXT_SCHEMA,
            "status": management.MANAGEMENT_ONLY,
            "capsule_id": supplied_capsule_id,
            "generation_id": generation_id,
            "permissions": dict(management.MANAGEMENT_PERMISSIONS),
        }
        return {
            "status": management.MANAGEMENT_ONLY,
            "errors": [],
            "capsule_id": supplied_capsule_id,
            "generation_id": generation_id,
            "permissions": dict(management.MANAGEMENT_PERMISSIONS),
            "management_context": context,
        }

    def pins(_store):
        return {
            "status": management.MANAGEMENT_ONLY,
            "capsule_ids": [capsule_id],
            "generation_ids": [generation_id],
        }

    monkeypatch.setattr(management, "create_demo_management_capsule", create)
    monkeypatch.setattr(management, "reverify_demo_management_capsule", reverify)
    monkeypatch.setattr(management, "list_demo_management_external_pins", pins)
    return calls


def _demo_account():
    return SimpleNamespace(
        login=demo.AUTHORIZED_DEMO_ACCOUNT_IDENTITY.login,
        server=demo.AUTHORIZED_DEMO_ACCOUNT_IDENTITY.server,
        currency=demo.AUTHORIZED_DEMO_ACCOUNT_IDENTITY.currency,
        trade_mode=demo.AUTHORIZED_DEMO_ACCOUNT_IDENTITY.trade_mode,
        trade_allowed=True,
        trade_expert=True,
        equity=10_000.0,
        margin_mode=2,
        hedge_allowed=True,
    )


def test_demo_authority_raw_reverifies_without_funded_authorization(
    tmp_path, monkeypatch
):
    ledger_path = tmp_path / "probation.json"
    ledger_path.write_text(json.dumps(_ledger()), encoding="utf-8")
    result = _authority_result(ledger_path)
    verified, errors, calls = _verify(tmp_path, monkeypatch, result)
    assert errors == []
    assert verified["funded_live_authorized"] is False
    assert calls[0]["require_funded_authorization"] is False


def test_demo_authority_rejects_funded_scope_leakage(tmp_path, monkeypatch):
    ledger_path = tmp_path / "probation.json"
    ledger_path.write_text(json.dumps(_ledger()), encoding="utf-8")
    result = _authority_result(ledger_path)
    result["status"] = "FUNDED_AUTHORIZED"
    result["funded_live_authorized"] = True
    _verified, errors, _calls = _verify(tmp_path, monkeypatch, result)
    assert "DEMO_PROBATION_AUTHORITY_NOT_GRANTED" in errors
    assert "DEMO_AUTHORITY_FUNDED_SCOPE_LEAKAGE" in errors


def test_demo_authority_rejects_self_asserted_cap_evidence(tmp_path, monkeypatch):
    ledger_path = tmp_path / "probation.json"
    ledger_path.write_text(json.dumps(_ledger()), encoding="utf-8")
    result = _authority_result(ledger_path)
    result["raw_reverification"]["capital_calibration"] = False
    _verified, errors, _calls = _verify(tmp_path, monkeypatch, result)
    assert "DEMO_AUTHORITY_RAW_REVERIFICATION_INCOMPLETE" in errors


def test_demo_authority_rejects_incomplete_slot_partition(tmp_path, monkeypatch):
    ledger_path = tmp_path / "probation.json"
    ledger_path.write_text(json.dumps(_ledger()), encoding="utf-8")
    result = _authority_result(ledger_path)
    result["no_local_winner_cell_ids"].pop()
    _verified, errors, _calls = _verify(tmp_path, monkeypatch, result)
    assert "DEMO_AUTHORITY_CELL_PARTITION_INCOMPLETE" in errors


def test_demo_authority_accepts_explicit_insufficient_evidence_abstention(
    tmp_path, monkeypatch
):
    ledger_path = tmp_path / "probation.json"
    ledger_path.write_text(json.dumps(_ledger()), encoding="utf-8")
    result = _authority_result(ledger_path)
    cell = result["no_local_winner_cell_ids"].pop()
    result["insufficient_evidence_cell_ids"] = [cell]
    _verified, errors, _calls = _verify(tmp_path, monkeypatch, result)
    assert errors == []


def test_demo_authority_accepts_authenticated_terminal_technical_abstention(
    tmp_path, monkeypatch
):
    ledger_path = tmp_path / "probation.json"
    ledger_path.write_text(json.dumps(_ledger()), encoding="utf-8")
    result = _authority_result(ledger_path)
    terminal_cells = {
        f"EURUSD|M5|{regime}" for regime in ("TREND", "RANGE", "BREAKOUT", "CHOP")
    }
    result["no_local_winner_cell_ids"] = sorted(
        set(result["no_local_winner_cell_ids"]) - terminal_cells
    )
    result["technical_cell_ids"] = sorted(terminal_cells)
    verified, errors, _calls = _verify(
        tmp_path,
        monkeypatch,
        result,
        terminal_timeframes={"M5"},
    )
    assert verified is not None
    assert errors == []


def test_demo_authority_rejects_unmatched_terminal_technical_roster(
    tmp_path, monkeypatch
):
    ledger_path = tmp_path / "probation.json"
    ledger_path.write_text(json.dumps(_ledger()), encoding="utf-8")
    result = _authority_result(ledger_path)
    cell = result["no_local_winner_cell_ids"].pop()
    result["technical_cell_ids"] = [cell]
    _verified, errors, _calls = _verify(tmp_path, monkeypatch, result)
    assert "DEMO_AUTHORITY_NATIVE_TERMINAL_TECHNICAL_ROSTER_MISMATCH" in errors


def test_demo_authority_rejects_unverified_cap_enforcement(tmp_path, monkeypatch):
    ledger_path = tmp_path / "probation.json"
    ledger_path.write_text(json.dumps(_ledger()), encoding="utf-8")
    result = _authority_result(ledger_path)
    result["capital_execution_enforcement_reverified"] = False
    _verified, errors, _calls = _verify(tmp_path, monkeypatch, result)
    assert "DEMO_AUTHORITY_CAPITAL_ENFORCEMENT_UNVERIFIED" in errors


def test_broker_demo_hedging_account_is_required():
    good = _demo_account()
    assert demo.demo_account_errors(good) == []
    real = _demo_account()
    real.trade_mode = 2
    assert "DEMO_ACCOUNT_NOT_BROKER_DEMO" in demo.demo_account_errors(real)
    missing_identity = _demo_account()
    missing_identity.login = 0
    missing_identity.server = " "
    assert {
        "DEMO_ACCOUNT_LOGIN_INVALID",
        "DEMO_ACCOUNT_SERVER_INVALID",
    }.issubset(demo.demo_account_errors(missing_identity))


@pytest.mark.parametrize(
    ("field", "value", "expected_error"),
    [
        ("login", 106165988, "DEMO_ACCOUNT_LOGIN_NOT_AUTHORIZED"),
        ("server", "Other-Demo", "DEMO_ACCOUNT_SERVER_NOT_AUTHORIZED"),
        ("currency", "EUR", "DEMO_ACCOUNT_CURRENCY_NOT_AUTHORIZED"),
        ("trade_mode", 2, "DEMO_ACCOUNT_NOT_BROKER_DEMO"),
    ],
)
def test_demo_account_requires_exact_authorized_identity(field, value, expected_error):
    account = _demo_account()
    setattr(account, field, value)
    assert expected_error in demo.demo_account_errors(account)
    assert demo.demo_account_errors(_demo_account()) == []


def test_demo_contract_replaces_legacy_paper_projection():
    source = inspect.getsource(demo.validate_demo_probation_readiness)
    assert "EDGE_CHALLENGER_DEMO" not in source
    assert "PAPER_ONLY" not in source
    assert demo.DEMO_PROBATION_CALLER_CONTRACT["ledger_role"] == (
        "PROCESS_VALIDATED_CURRENT_REFIT"
    )
    assert demo.DEMO_PROBATION_CALLER_CONTRACT["account_login"] == 106165989
    assert demo.DEMO_PROBATION_CALLER_CONTRACT["account_server"] == "FBS-Demo"
    assert demo.DEMO_PROBATION_CALLER_CONTRACT["account_currency"] == "USD"
    assert demo.DEMO_PROBATION_CALLER_CONTRACT["account_trade_mode"] == "DEMO"
    assert demo.DEMO_PROBATION_CALLER_CONTRACT["funded_live_eligible"] is False


@pytest.mark.parametrize(
    "mutator",
    [
        lambda ledger: ledger["EURUSD"].pop("regime_detector_records"),
        lambda ledger: ledger["EURUSD"]["regime_detector_records"]["H1"].__setitem__(
            "activation_id", "other"
        ),
        lambda ledger: ledger["EURUSD"]["regime_detector_records"]["H1"].__setitem__(
            "parameters_sha256", "f" * 64
        ),
        lambda ledger: ledger["EURUSD"]["regime_detector_records"].__setitem__(
            "M5", ledger["EURUSD"]["regime_detector_records"]["H1"]
        ),
    ],
)
def test_detector_projection_rejects_record_tampering(mutator):
    ledger = _ledger()
    mutator(ledger)
    native = SimpleNamespace(
        activation_id="activation-1",
        activation_manifest_sha256=SHA,
        entries={"EURUSD": {"H1": {"bars_sha256": SHA}}},
    )
    with pytest.raises(ValueError, match="DEMO_DETECTOR"):
        demo._execution_detector_projection(
            ledger,
            ("EURUSD|H1|TREND",),
            native,
        )


def test_detector_projection_rejects_runtime_code_drift(monkeypatch):
    import pm_regime

    monkeypatch.setattr(pm_regime, "detector_code_sha256", lambda: "f" * 64)
    native = SimpleNamespace(
        activation_id="activation-1",
        activation_manifest_sha256=SHA,
        entries={"EURUSD": {"H1": {"bars_sha256": SHA}}},
    )
    with pytest.raises(ValueError, match="DEMO_DETECTOR_CODE_DRIFT"):
        demo._execution_detector_projection(
            _ledger(),
            ("EURUSD|H1|TREND",),
            native,
        )


def test_missing_raw_verifier_blocks_demo(tmp_path, monkeypatch):
    ledger = _ledger()
    ledger_path = tmp_path / "probation.json"
    ledger_path.write_text(json.dumps(ledger), encoding="utf-8")
    _write_manifest(tmp_path, ledger_path)
    monkeypatch.setitem(sys.modules, "pm_optimization_authority", None)
    native = SimpleNamespace(
        activation_id="activation-1", activation_manifest_sha256=SHA
    )
    _verified, errors = demo.verify_demo_probation_authority(
        config=_config(),
        config_path=str(tmp_path / "config.json"),
        config_payload={},
        ledger=ledger,
        ledger_path=ledger_path,
        requested_symbols=["EURUSD"],
        selected_cells={"EURUSD|H1|TREND"},
        native_universe=native,
        validation_time_utc="2026-07-14T00:00:00Z",
    )
    assert "DEMO_RAW_AUTHORITY_VERIFIER_UNAVAILABLE" in errors


@pytest.mark.parametrize("spread_mode", ["conservative_max", "fixed_spec"])
def test_demo_entry_policy_block_preserves_verified_management_for_demo_only(tmp_path, monkeypatch, spread_mode):
    from pm_pipeline import SymbolConfig

    config_path = tmp_path / "config.json"
    _write_config(config_path)
    ledger = _ledger()
    ledger["EURUSD"]["valid_until"] = "2099-01-01T00:00:00+00:00"
    ledger_path = tmp_path / "probation.json"
    ledger_path.write_text(json.dumps(ledger), encoding="utf-8")
    _write_manifest(tmp_path, ledger_path)
    result = _authority_result(ledger_path)
    terminal_cells = {
        f"EURUSD|M5|{regime}" for regime in ("TREND", "RANGE", "BREAKOUT", "CHOP")
    }
    result["no_local_winner_cell_ids"] = sorted(
        set(result["no_local_winner_cell_ids"]) - terminal_cells
    )
    result["technical_cell_ids"] = sorted(terminal_cells)
    monkeypatch.setitem(
        sys.modules,
        "pm_optimization_authority",
        SimpleNamespace(verify_live_readiness_authority_chain=lambda **_kwargs: result),
    )
    management_calls = _install_management(monkeypatch)
    native = SimpleNamespace(
        valid=True,
        explicit_contract=True,
        activation_id="activation-1",
        activation_manifest_sha256=SHA,
        entries={"EURUSD": {"H1": {"bars_sha256": SHA}}},
        cell_status=lambda _symbol, timeframe: (
            "technical_error" if timeframe == "M5" else "active"
        ),
    )
    monkeypatch.setitem(
        sys.modules,
        "pm_native_activation",
        SimpleNamespace(
            NATIVE_EVIDENCE_CANDIDATE="evidence_candidate",
            NATIVE_SOURCE_UNAVAILABLE="source_unavailable",
            resolve_active_native_universe=lambda *_args, **_kwargs: native,
            resolve_active_native_artifact=lambda *_args, **_kwargs: SimpleNamespace(
                valid=True
            ),
        ),
    )
    config = _full_config(tmp_path, ledger_path)
    config.backtest_spread_mode = spread_mode
    config.live_require_tuned_regime_params = True
    config.regime_params_file = str(tmp_path / "retired-regime-params.json")
    account = _demo_account()
    report, projection = demo.authorize_demo_probation_execution(
        config,
        symbols=["EURUSD"],
        config_path=str(config_path),
        account_info=account,
        position_config=_position_config(),
    )
    assert not report.ok
    if spread_mode != "conservative_max":
        assert [(item.code, item.scope) for item in report.errors] == [
            ("demo_cost_contract_invalid", "evidence"),
            ("interval_cell_qualification_policy_unadopted", "evidence"),
        ]
        assert projection is None
        assert not report.management_only
        assert report.management_projection == {}
        assert report.management_context == {}
        assert management_calls == []
        return
    assert [(item.code, item.scope) for item in report.errors] == [
        ("interval_cell_qualification_policy_unadopted", "evidence")
    ]
    assert projection is None
    assert report.management_only
    assert [name for name, _kwargs in management_calls] == ["create", "reverify"]
    assert report.management_capsule_id == "b" * 64
    assert report.management_generation_id == "generation-test"
    assert report.management_context["status"] == "MANAGEMENT_ONLY"
    assert report.management_context_sha256 is not None
    assert report.management_store_root == str(
        (tmp_path / demo.DEMO_MANAGEMENT_STORE_RELATIVE).resolve()
    )
    context_copy = report.management_context
    context_copy.clear()
    assert report.management_context["status"] == "MANAGEMENT_ONLY"
    assert report.execution_ledger_sha256 is not None
    projection = report.management_projection
    assert set(projection) == {"EURUSD"}
    assert set(projection["EURUSD"]["regime_configs"]) == {"H1"}
    assert set(projection["EURUSD"]["regime_configs"]["H1"]) == {"TREND"}
    for forbidden in (
        "default_config",
        "edge_challenger_book",
        "strategy_name",
        "timeframe",
        "parameters",
        "regime_detector_records",
    ):
        assert forbidden not in projection["EURUSD"]
    parsed = SymbolConfig.from_dict(projection["EURUSD"])
    assert parsed.default_config is None
    assert parsed.edge_challenger_book == {}
    assert parsed.count_regime_winners() == 1
    ledger_sha = report.execution_ledger_sha256
    selected_ids = report.execution_selected_cell_ids
    detector_projection = report.execution_detector_projection
    assert set(detector_projection) == {"EURUSD|H1"}
    assert report.execution_detector_projection_sha256 is not None
    projection["EURUSD"]["regime_configs"].clear()
    detector_projection.clear()
    assert set(report.management_projection["EURUSD"]["regime_configs"]) == {"H1"}
    assert report.execution_ledger_sha256 == ledger_sha
    assert report.execution_selected_cell_ids == selected_ids
    assert set(report.execution_detector_projection) == {"EURUSD|H1"}

    account.trade_mode = 2
    report = demo.validate_demo_probation_readiness(
        config,
        symbols=["EURUSD"],
        config_path=str(config_path),
        account_info=account,
        position_config=_position_config(),
    )
    assert not report.ok
    assert "demo_account_not_broker_demo" in {
        finding.code for finding in report.runtime_errors
    }


def test_execution_projection_rejects_validation_to_load_mutation(
    tmp_path, monkeypatch
):
    config_path = tmp_path / "config.json"
    _write_config(config_path)
    ledger = _ledger()
    ledger["EURUSD"]["valid_until"] = "2099-01-01T00:00:00+00:00"
    ledger_path = tmp_path / "probation.json"
    ledger_path.write_text(json.dumps(ledger), encoding="utf-8")
    _write_manifest(tmp_path, ledger_path)
    result = _authority_result(ledger_path)
    monkeypatch.setitem(
        sys.modules,
        "pm_optimization_authority",
        SimpleNamespace(verify_live_readiness_authority_chain=lambda **_kwargs: result),
    )
    native = SimpleNamespace(
        valid=True,
        explicit_contract=True,
        activation_id="activation-1",
        activation_manifest_sha256=SHA,
        entries={"EURUSD": {"H1": {"bars_sha256": SHA}}},
        cell_status=lambda _symbol, _timeframe: "active",
    )
    monkeypatch.setitem(
        sys.modules,
        "pm_native_activation",
        SimpleNamespace(
            NATIVE_EVIDENCE_CANDIDATE="evidence_candidate",
            NATIVE_SOURCE_UNAVAILABLE="source_unavailable",
            resolve_active_native_universe=lambda *_args, **_kwargs: native,
            resolve_active_native_artifact=lambda *_args, **_kwargs: SimpleNamespace(
                valid=True
            ),
        ),
    )
    original_validate = demo.validate_demo_probation_readiness
    validation_calls = 0

    def mutate_after_second_validation(*args, **kwargs):
        nonlocal validation_calls
        report = original_validate(*args, **kwargs)
        validation_calls += 1
        if validation_calls == 2:
            ledger["EURUSD"]["regime_configs"]["H1"]["TREND"]["strategy_name"] = (
                "Mutated"
            )
            ledger_path.write_text(json.dumps(ledger), encoding="utf-8")
        return report

    monkeypatch.setattr(
        demo, "validate_demo_probation_readiness", mutate_after_second_validation
    )
    report, projection = demo.authorize_demo_probation_execution(
        _full_config(tmp_path, ledger_path),
        symbols=["EURUSD"],
        config_path=str(config_path),
        account_info=_demo_account(),
        position_config=_position_config(),
    )
    assert not report.ok
    assert projection is None
    assert any(
        "DEMO_EXECUTION_LEDGER_SHA256_MISMATCH" in finding.message
        for finding in report.errors
    )


def test_authority_rejects_ledger_change_during_reverification(tmp_path, monkeypatch):
    ledger = _ledger()
    ledger_path = tmp_path / "probation.json"
    ledger_path.write_text(json.dumps(ledger), encoding="utf-8")
    _write_manifest(tmp_path, ledger_path)
    result = _authority_result(ledger_path)

    def mutate_while_verifying(**_kwargs):
        ledger_path.write_text(json.dumps({"changed": {}}), encoding="utf-8")
        return result

    monkeypatch.setitem(
        sys.modules,
        "pm_optimization_authority",
        SimpleNamespace(verify_live_readiness_authority_chain=mutate_while_verifying),
    )
    verified, errors = demo.verify_demo_probation_authority(
        config=_config(),
        config_path=str(tmp_path / "config.json"),
        config_payload={},
        ledger=ledger,
        ledger_path=ledger_path,
        requested_symbols=["EURUSD"],
        selected_cells={"EURUSD|H1|TREND"},
        native_universe=SimpleNamespace(
            activation_id="activation-1", activation_manifest_sha256=SHA
        ),
        validation_time_utc="2026-07-14T00:00:00Z",
    )
    assert verified is None
    assert errors == ["DEMO_AUTHORITY_LEDGER_CHANGED_DURING_REVERIFY"]


def test_public_execution_api_cannot_consume_a_forged_report(tmp_path):
    forged = demo.DemoProbationReadinessReport(ok=True, findings=[])
    config_path = tmp_path / "config.json"
    _write_config(config_path)
    report, projection = demo.authorize_demo_probation_execution(
        forged,
        symbols=["EURUSD"],
        config_path=str(config_path),
        winner_ledger_path=str(tmp_path / "self-asserted.json"),
        position_config=_position_config(),
    )
    assert not report.ok
    assert projection is None
    assert not hasattr(demo, "load_verified_demo_execution_projection")


def test_execution_projection_rejects_capital_config_mutation(tmp_path, monkeypatch):
    config_path = tmp_path / "config.json"
    _write_config(config_path)
    ledger = _ledger()
    ledger["EURUSD"]["valid_until"] = "2099-01-01T00:00:00+00:00"
    ledger_path = tmp_path / "probation.json"
    ledger_path.write_text(json.dumps(ledger), encoding="utf-8")
    _write_manifest(tmp_path, ledger_path)
    result = _authority_result(ledger_path)
    monkeypatch.setitem(
        sys.modules,
        "pm_optimization_authority",
        SimpleNamespace(verify_live_readiness_authority_chain=lambda **_kwargs: result),
    )
    native = SimpleNamespace(
        valid=True,
        explicit_contract=True,
        activation_id="activation-1",
        activation_manifest_sha256=SHA,
        entries={"EURUSD": {"H1": {"bars_sha256": SHA}}},
        cell_status=lambda _symbol, _timeframe: "active",
    )
    monkeypatch.setitem(
        sys.modules,
        "pm_native_activation",
        SimpleNamespace(
            NATIVE_EVIDENCE_CANDIDATE="evidence_candidate",
            NATIVE_SOURCE_UNAVAILABLE="source_unavailable",
            resolve_active_native_universe=lambda *_args, **_kwargs: native,
            resolve_active_native_artifact=lambda *_args, **_kwargs: SimpleNamespace(
                valid=True
            ),
        ),
    )
    config = _full_config(tmp_path, ledger_path)
    original_validate = demo.validate_demo_probation_readiness
    validation_calls = 0

    def mutate_cap_after_second_validation(*args, **kwargs):
        nonlocal validation_calls
        report = original_validate(*args, **kwargs)
        validation_calls += 1
        if validation_calls == 2:
            config.max_total_open_risk_pct = 40.0
        return report

    monkeypatch.setattr(
        demo, "validate_demo_probation_readiness", mutate_cap_after_second_validation
    )
    report, projection = demo.authorize_demo_probation_execution(
        config,
        symbols=["EURUSD"],
        config_path=str(config_path),
        account_info=_demo_account(),
        position_config=_position_config(),
    )
    assert not report.ok
    assert projection is None
    assert any(
        "DEMO_EXECUTION_CAPITAL_CONFIG_CHANGED" in finding.message
        for finding in report.errors
    )


def test_execution_authority_is_reverified_before_projection(tmp_path, monkeypatch):
    config_path = tmp_path / "config.json"
    _write_config(config_path)
    ledger = _ledger()
    ledger["EURUSD"]["valid_until"] = "2099-01-01T00:00:00+00:00"
    ledger_path = tmp_path / "probation.json"
    ledger_path.write_text(json.dumps(ledger), encoding="utf-8")
    _write_manifest(tmp_path, ledger_path)
    valid = _authority_result(ledger_path)
    calls = 0

    def changing_authority(**_kwargs):
        nonlocal calls
        calls += 1
        if calls == 1:
            return valid
        changed = json.loads(json.dumps(valid))
        changed["selected_capital_envelope"]["gap_stress_multiple"] = 3.0
        return changed

    monkeypatch.setitem(
        sys.modules,
        "pm_optimization_authority",
        SimpleNamespace(verify_live_readiness_authority_chain=changing_authority),
    )
    native = SimpleNamespace(
        valid=True,
        explicit_contract=True,
        activation_id="activation-1",
        activation_manifest_sha256=SHA,
        entries={"EURUSD": {"H1": {"bars_sha256": SHA}}},
        cell_status=lambda _symbol, _timeframe: "active",
    )
    _install_native(monkeypatch, lambda *_args, **_kwargs: native)
    report, projection = demo.authorize_demo_probation_execution(
        _full_config(tmp_path, ledger_path),
        symbols=["EURUSD"],
        config_path=str(config_path),
        account_info=_demo_account(),
        position_config=_position_config(),
    )
    assert calls == 2
    assert not report.ok
    assert projection is None
    assert "demo_execution_authority_changed_during_reverification" in {
        finding.code for finding in report.errors
    }


def test_native_authority_is_reverified_before_projection(tmp_path, monkeypatch):
    config_path = tmp_path / "config.json"
    _write_config(config_path)
    ledger = _ledger()
    ledger["EURUSD"]["valid_until"] = "2099-01-01T00:00:00+00:00"
    ledger_path = tmp_path / "probation.json"
    ledger_path.write_text(json.dumps(ledger), encoding="utf-8")
    _write_manifest(tmp_path, ledger_path)
    result = _authority_result(ledger_path)

    def current_native_authority(**kwargs):
        current = json.loads(json.dumps(result))
        universe = kwargs["native_universe"]
        current["native_activation_id"] = universe.activation_id
        current["native_activation_manifest_sha256"] = (
            universe.activation_manifest_sha256
        )
        return current

    monkeypatch.setitem(
        sys.modules,
        "pm_optimization_authority",
        SimpleNamespace(verify_live_readiness_authority_chain=current_native_authority),
    )
    calls = 0

    def changing_native(*_args, **_kwargs):
        nonlocal calls
        calls += 1
        activation_id = "activation-1" if calls == 1 else "activation-2"
        return SimpleNamespace(
            valid=True,
            explicit_contract=True,
            activation_id=activation_id,
            activation_manifest_sha256=SHA,
            entries={"EURUSD": {"H1": {"bars_sha256": SHA}}},
            cell_status=lambda _symbol, _timeframe: "active",
        )

    _install_native(monkeypatch, changing_native)
    report, projection = demo.authorize_demo_probation_execution(
        _full_config(tmp_path, ledger_path),
        symbols=["EURUSD"],
        config_path=str(config_path),
        account_info=_demo_account(),
        position_config=_position_config(),
    )
    assert calls == 2
    assert not report.ok
    assert projection is None
    assert "demo_native_authority_failed" in {finding.code for finding in report.errors}


def test_empty_projection_is_never_authorized(tmp_path, monkeypatch):
    config_path = tmp_path / "config.json"
    _write_config(config_path)
    ledger_path = tmp_path / "ledger.json"
    ledger_path.write_text(json.dumps(_ledger()), encoding="utf-8")
    config = _config()
    envelope = {
        "max_total_stop_risk_pct": 8.0,
        "max_factor_stop_risk_pct": 2.0,
        "max_same_symbol_stop_risk_pct": 1.0,
        "max_margin_utilization_pct": 50.0,
        "min_margin_level_pct": 200.0,
        "min_free_margin_pct": 50.0,
        "gap_stress_multiple": 2.0,
    }
    binding = demo._DemoExecutionBinding(
        schema_version=demo.DEMO_EXECUTION_BINDING_SCHEMA,
        ledger_path=str(ledger_path.resolve()),
        ledger_sha256=hashlib.sha256(ledger_path.read_bytes()).hexdigest(),
        projection_sha256=SHA,
        detector_projection_sha256=SHA,
        detector_projection_json="{}",
        config_path=str(config_path.resolve()),
        config_sha256=hashlib.sha256(config_path.read_bytes()).hexdigest(),
        capital_config_sha256=demo._capital_config_sha256(config),
        position_config_sha256=demo.demo_position_config_sha256(_position_config()),
        selected_capital_envelope=tuple(sorted(envelope.items())),
        native_activation_id="activation-1",
        native_activation_manifest_sha256=SHA,
        authority_result_sha256=SHA,
        requested_symbols=("EURUSD",),
        selected_cell_ids=("EURUSD|H1|TREND",),
    )

    def successful_report(*_args, **_kwargs):
        return demo.DemoProbationReadinessReport(
            ok=True, findings=[], _execution_binding=binding
        )

    monkeypatch.setattr(demo, "validate_demo_probation_readiness", successful_report)
    monkeypatch.setattr(demo, "_execution_symbol_payloads", lambda *_args: {})
    report, projection = demo.authorize_demo_probation_execution(
        config, symbols=["EURUSD"], position_config=_position_config()
    )
    assert not report.ok
    assert projection is None
    assert any(
        "DEMO_EXECUTION_PROJECTION_EMPTY" in item.message for item in report.errors
    )


@pytest.mark.parametrize("extra,expected", [
    ((), True),
    ((("warning", "unrelated_warning", "evidence"),), True),
    ((("error", "runtime_account_invalid", "runtime"),), False),
    ((("error", "unrelated_evidence_error", "evidence"),), False),
    ((("error", "interval_cell_qualification_policy_unadopted", "evidence"),), False),
    ((("error", "interval_cell_qualification_policy_unadopted", "runtime"),), False),
], ids=("only-entry-policy", "warning-does-not-grant-entry", "runtime-error",
         "other-evidence-error", "duplicate-policy-error", "wrong-scope-policy-error"))
def test_management_exception_requires_exact_single_entry_policy_error(
    extra, expected, monkeypatch,
):
    policy = live.ReadinessFinding(
        "error", "interval_cell_qualification_policy_unadopted",
        "Synthetic entry-policy block", "evidence",
    )
    findings = [policy, *(
        live.ReadinessFinding(severity, code, "Synthetic additional finding", scope)
        for severity, code, scope in extra
    )]
    report = demo.DemoProbationReadinessReport(
        ok=False, findings=findings, _execution_binding=object(),
    )
    assert demo._only_interval_entry_policy_block(findings) is expected
    assert demo._demo_management_binding_checks_pass(report) is expected
    assert not report.ok
    # Eligibility alone cannot produce a verified management context or projection.
    assert not report.management_only
    assert report.management_projection == {}
    if not expected:
        monkeypatch.setattr(
            demo, "validate_demo_probation_readiness", lambda *_args, **_kwargs: report,
        )
        monkeypatch.setattr(
            demo, "_capture_demo_management_authority",
            lambda **_kwargs: pytest.fail("invalid exception reached capsule capture"),
        )
        returned, projection = demo.authorize_demo_probation_execution(
            _config(), symbols=["EURUSD"], position_config=_position_config(),
        )
        assert returned is report
        assert not returned.ok
        assert projection is None


def test_management_exception_rejects_policy_code_in_runtime_scope():
    findings = [live.ReadinessFinding(
        "error", "interval_cell_qualification_policy_unadopted",
        "Synthetic runtime-scope impostor", "runtime",
    )]
    report = demo.DemoProbationReadinessReport(
        ok=False, findings=findings, _execution_binding=object(),
    )
    assert not demo._only_interval_entry_policy_block(findings)
    assert not demo._demo_management_binding_checks_pass(report)


def test_management_exception_requires_reverified_execution_binding():
    report = demo.DemoProbationReadinessReport(
        ok=False, findings=[live.ReadinessFinding(
            "error", "interval_cell_qualification_policy_unadopted",
            "Synthetic entry-policy block", "evidence",
        )],
    )
    assert not demo._demo_management_binding_checks_pass(report)
    assert not report.management_only
    assert report.management_projection == {}
