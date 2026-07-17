import inspect
import copy
import sys
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
import pandas as pd

import pm_demo_readiness
import pm_main
from pm_model_lifecycle import PROCESS_VALIDATED_CURRENT_REFIT_ROLE
from pm_pipeline import SymbolConfig
from pm_position import TradeTagEncoder
from pm_regime import RegimeParams, detector_code_sha256, regime_params_sha256


def _binding(seed: str = "a", detector_seed: str | None = None):
    detector_seed = detector_seed or seed
    binding = {
        name: seed * 64 for name in pm_main.LiveTrader._CURRENT_REFIT_BINDING_FIELDS
    }
    binding["regime_fit_receipt_sha256"] = detector_seed * 64
    binding["regime_detector_record_sha256"] = detector_seed * 64
    binding["regime_detector_parameters_sha256"] = regime_params_sha256(
        RegimeParams().to_dict()
    )
    binding["regime_detector_code_sha256"] = detector_code_sha256()
    return binding


def _cell(
    seed: str = "a",
    *,
    status=PROCESS_VALIDATED_CURRENT_REFIT_ROLE,
    detector_seed: str | None = None,
):
    return {
        "strategy_name": "TrendExpert",
        "parameters": {"lookback": 20},
        "quality_score": 0.8,
        "train_metrics": {"total_trades": 40},
        "val_metrics": {},
        "holdout_metrics": {},
        "promotion_status": status,
        "artifact_meta": {
            "current_refit_binding": _binding(seed, detector_seed)
        },
        "valid_until": (datetime.now(timezone.utc) + timedelta(days=1)).isoformat(),
    }


def _projection(*, two_symbols=False):
    result = {
        "EURUSD": {
            "symbol": "EURUSD",
            "valid_until": (datetime.now(timezone.utc) + timedelta(days=1)).isoformat(),
            "regime_configs": {"H1": {"TREND": _cell("a")}},
        }
    }
    if two_symbols:
        result["GBPUSD"] = {
            "symbol": "GBPUSD",
            "valid_until": (datetime.now(timezone.utc) + timedelta(days=1)).isoformat(),
            "regime_configs": {"M15": {"RANGE": _cell("b")}},
        }
    return result


def _detector_projection(projection):
    params = RegimeParams().to_dict()
    records = {}
    for symbol, payload in projection.items():
        for timeframe, regimes in payload["regime_configs"].items():
            binding = next(iter(regimes.values()))["artifact_meta"][
                "current_refit_binding"
            ]
            records[f"{symbol}|{timeframe}"] = {
                "regime_fit_receipt_sha256": binding[
                    "regime_fit_receipt_sha256"
                ],
                "record_sha256": binding["regime_detector_record_sha256"],
                "parameters_sha256": binding[
                    "regime_detector_parameters_sha256"
                ],
                "detector_code_sha256": binding["regime_detector_code_sha256"],
                "parameters": params,
            }
    return records


def _isolated_trader(projection=None):
    projection = projection or _projection()
    configs, bindings, fingerprints = (
        pm_main.LiveTrader._parse_current_refit_demo_projection(projection)
    )
    trader = pm_main.LiveTrader.__new__(pm_main.LiveTrader)
    trader._is_current_refit_demo = True
    trader.execution_book = pm_main.CURRENT_REFIT_DEMO_EXECUTION_BOOK
    trader._current_refit_demo_configs = configs
    trader._current_refit_demo_cell_bindings = bindings
    trader._current_refit_demo_cell_fingerprints = fingerprints
    detectors, detector_fingerprint = (
        pm_main.LiveTrader._parse_current_refit_demo_detectors(
            _detector_projection(projection), configs
        )
    )
    trader._current_refit_demo_detectors = detectors
    trader._current_refit_demo_detector_fingerprint = detector_fingerprint
    trader._current_refit_demo_detector_roster = tuple(sorted(detectors))
    trader._current_refit_demo_detector_record_fingerprints = {
        key: trader._canonical_payload_sha256(value)
        for key, value in detectors.items()
    }
    trader._current_refit_demo_detector_authority_sha256 = (
        trader._canonical_payload_sha256(
            {
                "schema_version": "fxpm.demo-regime-detector-projection.v1",
                "records": detectors,
            }
        )
    )
    trader.pipeline_config = SimpleNamespace(live_config_expiry_grace_minutes=0)
    trader.pm = Mock()
    trader.logger = Mock()
    trader.mt5 = Mock()
    trader.mt5.find_broker_symbol.side_effect = lambda symbol: symbol
    return trader


def _demo_account(**changes):
    values = {
        "login": pm_demo_readiness.AUTHORIZED_DEMO_ACCOUNT_IDENTITY.login,
        "server": pm_demo_readiness.AUTHORIZED_DEMO_ACCOUNT_IDENTITY.server,
        "currency": pm_demo_readiness.AUTHORIZED_DEMO_ACCOUNT_IDENTITY.currency,
        "trade_mode": pm_demo_readiness.AUTHORIZED_DEMO_ACCOUNT_IDENTITY.trade_mode,
        "trade_allowed": True,
        "trade_expert": True,
        "equity": 10_000.0,
        "margin_mode": 2,
        "hedge_allowed": True,
    }
    values.update(changes)
    return SimpleNamespace(**values)


def test_projection_is_sparse_and_independent_from_caller_and_manager():
    raw = _projection()
    trader = _isolated_trader(raw)
    raw["EURUSD"]["regime_configs"]["H1"]["CHOP"] = _cell("c")
    raw["USDJPY"] = {"symbol": "USDJPY", "regime_configs": {}}

    selected = trader._get_live_configs()

    assert set(selected) == {"EURUSD"}
    assert set(selected["EURUSD"].regime_configs["H1"]) == {"TREND"}
    trader.pm.get_edge_challenger_configs.assert_not_called()
    trader.pm.get_live_eligible_configs.assert_not_called()


@pytest.mark.parametrize("field", ["default_config", "edge_challenger_book"])
def test_projection_rejects_legacy_fallbacks(field):
    projection = _projection()
    projection["EURUSD"][field] = {"strategy_name": "Fallback"}
    with pytest.raises(ValueError, match="fallback"):
        pm_main.LiveTrader._parse_current_refit_demo_projection(projection)


@pytest.mark.parametrize("status", ["PAPER_ONLY", "PROMOTED", "BLOCKED"])
def test_projection_rejects_non_current_refit_status(status):
    projection = _projection()
    projection["EURUSD"]["regime_configs"]["H1"]["TREND"]["promotion_status"] = status
    with pytest.raises(ValueError, match="not executable"):
        pm_main.LiveTrader._parse_current_refit_demo_projection(projection)


def test_detector_projection_rejects_missing_and_hash_splice():
    projection = _projection()
    configs, _bindings, _fingerprints = (
        pm_main.LiveTrader._parse_current_refit_demo_projection(projection)
    )
    detectors = _detector_projection(projection)
    with pytest.raises(ValueError, match="projection is empty"):
        pm_main.LiveTrader._parse_current_refit_demo_detectors({}, configs)
    spliced = copy.deepcopy(detectors)
    spliced["EURUSD|H1"]["record_sha256"] = "f" * 64
    with pytest.raises(ValueError, match="cell splice"):
        pm_main.LiveTrader._parse_current_refit_demo_detectors(spliced, configs)


def test_selected_siblings_must_share_one_detector():
    projection = _projection()
    projection["EURUSD"]["regime_configs"]["H1"]["RANGE"] = _cell("b")
    configs, _bindings, _fingerprints = (
        pm_main.LiveTrader._parse_current_refit_demo_projection(projection)
    )
    with pytest.raises(ValueError, match="cell splice"):
        pm_main.LiveTrader._parse_current_refit_demo_detectors(
            _detector_projection(projection), configs
        )


def test_entry_and_governance_use_authorized_detector_override(monkeypatch):
    trader = _isolated_trader()
    config = trader._get_live_configs()["EURUSD"]
    trader.pipeline_config = SimpleNamespace(
        live_config_expiry_grace_minutes=0,
        live_require_tuned_regime_params=True,
        regime_params_file="missing-sidecar.json",
        live_bars_count=4,
        live_min_bars=2,
        regime_boundary_audit_enabled=False,
        local_governance_live_mode="active",
    )
    trader._last_bar_times = {}
    trader._candidate_cache = {}
    trader._cache_hits = 0
    trader._cache_misses = 0
    trader._last_governance_bar_times = {}
    trader._uses_scheduled_trigger = lambda: False
    trader._prune_cache = lambda: None
    index = pd.date_range("2026-01-01", periods=4, freq="h", tz="UTC")
    bars = pd.DataFrame(
        {
            "open": [1.0] * 4,
            "high": [1.1] * 4,
            "low": [0.9] * 4,
            "close": [1.0] * 4,
            "volume": [1.0] * 4,
        },
        index=index,
    )
    trader._get_live_bars = lambda *_args, **_kwargs: bars
    calls = []

    def capture(frame, **kwargs):
        calls.append(kwargs)
        result = frame.copy()
        result["REGIME_LIVE"] = "CHOP"
        result["REGIME_STRENGTH_LIVE"] = 0.5
        return result

    monkeypatch.setattr(pm_main.FeatureComputer, "compute_all", capture)
    monkeypatch.setattr(pm_main, "policy_name_from_artifact", lambda _value: "trail_atr")
    import pm_regime

    monkeypatch.setattr(
        pm_regime,
        "has_tuned_regime_params",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("sidecar")),
    )
    trader._evaluate_regime_candidates("EURUSD", "EURUSD", config)

    magic = trader._encode_specialist_magic("EURUSD", "H1", "TREND")
    trader._resolve_symbol_from_broker = lambda *_args: "EURUSD"
    trader._infer_position_timeframe = lambda *_args: "H1"
    trader._build_magic_lookup = lambda *_args: ({}, {magic: "TREND"})
    config.regime_configs["H1"]["TREND"].governance_policy = {"name": "trail_atr"}
    position = SimpleNamespace(
        ticket=1,
        symbol="EURUSD",
        magic=magic,
        time=index[0],
    )
    trader._run_order_governance_cycle({"EURUSD": config}, [position])

    assert len(calls) == 2
    for kwargs in calls:
        assert kwargs["regime_params_override"] == RegimeParams().to_dict()
        assert kwargs["regime_params_sha256"] == regime_params_sha256(
            RegimeParams().to_dict()
        )


def test_direct_demo_construction_without_raw_authority_fails():
    with pytest.raises(ValueError, match="authority inputs are incomplete"):
        pm_main.LiveTrader(
            mt5_connector=Mock(),
            portfolio_manager=Mock(),
            position_config=Mock(),
            pipeline_config=Mock(),
            execution_book=pm_main.CURRENT_REFIT_DEMO_EXECUTION_BOOK,
        )


def test_direct_demo_construction_rejects_forged_projection_keyword():
    with pytest.raises(TypeError, match="_current_refit_demo_projection"):
        pm_main.LiveTrader(
            mt5_connector=Mock(),
            portfolio_manager=Mock(),
            position_config=Mock(),
            pipeline_config=Mock(),
            execution_book=pm_main.CURRENT_REFIT_DEMO_EXECUTION_BOOK,
            _current_refit_demo_projection=_projection(),
        )


def test_expiry_blocks_entries_but_preserves_management_projection():
    trader = _isolated_trader()
    trader._current_refit_demo_configs["EURUSD"].valid_until = datetime.now(
        timezone.utc
    ) - timedelta(seconds=1)
    assert trader._get_live_configs() == {}
    assert set(trader._get_management_configs()) == {"EURUSD"}


def test_current_refit_comment_and_magic_are_disjoint():
    trader = _isolated_trader()
    comment = TradeTagEncoder.encode_comment(
        "EURUSD",
        "H1",
        "TrendExpert",
        "LONG",
        risk_pct=0.25,
        book_role=PROCESS_VALIDATED_CURRENT_REFIT_ROLE,
    )
    decoded = TradeTagEncoder.decode_comment(comment)
    assert comment.startswith("PMR2:")
    assert decoded["book_role"] == PROCESS_VALIDATED_CURRENT_REFIT_ROLE
    assert decoded["risk_pct"] == 0.25
    assert trader._encode_specialist_magic("EURUSD", "H1", "TREND") != (
        TradeTagEncoder.encode_magic("EURUSD", "H1", "TREND")
    )


@pytest.mark.parametrize("risk", [0.05, 0.15, 0.25, 0.35, 0.45])
def test_current_refit_comment_never_understates_risk(risk):
    comment = TradeTagEncoder.encode_comment(
        "EURUSD",
        "H1",
        "TrendExpert",
        "LONG",
        risk_pct=risk,
        book_role=PROCESS_VALIDATED_CURRENT_REFIT_ROLE,
    )
    decoded = TradeTagEncoder.decode_comment(comment)
    assert decoded["risk_pct"] >= risk


def test_legacy_current_refit_comment_decodes_conservatively():
    decoded = TradeTagEncoder.decode_comment("PMR:EURUSD:H1:abcde:L:2")
    assert decoded["risk_pct"] == pytest.approx(0.25)


def test_position_gate_accepts_other_projected_symbol_and_rejects_bad_identity():
    trader = _isolated_trader(_projection(two_symbols=True))
    configs = trader._get_management_configs()
    magic = trader._encode_specialist_magic("GBPUSD", "M15", "RANGE")
    comment = TradeTagEncoder.encode_comment(
        "GBPUSD",
        "M15",
        "TrendExpert",
        "LONG",
        risk_pct=0.25,
        book_role=PROCESS_VALIDATED_CURRENT_REFIT_ROLE,
    )
    position = SimpleNamespace(ticket=1, symbol="GBPUSD", magic=magic, comment=comment)
    assert trader._current_refit_demo_position_block_reason([position], configs) is None

    position.comment = TradeTagEncoder.encode_comment(
        "GBPUSD",
        "M15",
        "WrongExpert",
        "LONG",
        risk_pct=0.25,
        book_role=PROCESS_VALIDATED_CURRENT_REFIT_ROLE,
    )
    assert trader._current_refit_demo_position_block_reason([position], configs)


def test_position_gate_rejects_any_selected_magic_collision(monkeypatch):
    projection = _projection()
    projection["EURUSD"]["regime_configs"]["H1"]["RANGE"] = _cell(
        "b", detector_seed="a"
    )
    trader = _isolated_trader(projection)
    monkeypatch.setattr(trader, "_encode_specialist_magic", lambda *_args: 7)
    reason = trader._current_refit_demo_position_block_reason(
        [], trader._get_management_configs()
    )
    assert reason and "magic collision" in reason


@pytest.mark.parametrize(
    "changes",
    [
        {"trade_mode": 2},
        {"trade_allowed": False},
        {"trade_expert": False},
        {"equity": 0.0},
        {"equity": float("nan")},
        {"margin_mode": 0},
        {"hedge_allowed": False},
    ],
)
def test_demo_account_boundary_fails_closed(changes):
    assert pm_main.LiveTrader._current_refit_demo_account_block_reason(
        _demo_account(**changes)
    )


@pytest.mark.parametrize(
    "changes",
    [
        {"login": 106165988},
        {"server": "Other-Demo"},
        {"currency": "EUR"},
        {"trade_mode": 2},
    ],
)
def test_demo_startup_boundary_requires_exact_authorized_identity(changes):
    assert pm_main.LiveTrader._current_refit_demo_account_block_reason(
        _demo_account(**changes)
    )
    assert (
        pm_main.LiveTrader._current_refit_demo_account_block_reason(_demo_account())
        is None
    )


def test_demo_pre_send_uses_authorized_identity_not_data_account_pin():
    trader = _isolated_trader()
    trader.pipeline_config = SimpleNamespace(
        live_min_account_equity=0.0,
        live_min_account_equity_currency="USD",
    )
    trader.mt5.config = SimpleNamespace(
        expected_login=28829971,
        expected_server="FBS-Real",
        expected_currency="USD",
    )
    account = _demo_account()
    trader.mt5.get_account_info.return_value = account

    fresh, error = trader._fresh_entry_account_validation(account)

    assert fresh is account
    assert error is None


def test_paper_preflight_is_demo_readiness_only_and_never_starts_trading(
    tmp_path, monkeypatch
):
    observed = {}

    class _Connector:
        def __init__(self, config):
            observed["connector_config"] = config

        @staticmethod
        def connect():
            observed["connected"] = True
            return True

        @staticmethod
        def get_account_info():
            return _demo_account()

        @staticmethod
        def disconnect():
            observed["disconnected"] = True

    report = SimpleNamespace(ok=True, findings=[], errors=[], warnings=[])

    def _demo_readiness(*_args, **kwargs):
        observed["demo_kwargs"] = kwargs
        return report

    config_path = tmp_path / "config.json"
    monkeypatch.setattr(
        pm_main,
        "load_config_json",
        lambda _path: {
            "symbols": ["EURUSD"],
            "pipeline": {
                "data_dir": str(tmp_path / "data"),
                "output_dir": str(tmp_path / "out"),
                "log_dir": str(tmp_path / "logs"),
                "winner_ledger_path": str(tmp_path / "pm_configs.json"),
            },
        },
    )
    monkeypatch.setattr(pm_main, "setup_logging", lambda **_kwargs: None)
    monkeypatch.setattr(pm_main, "log_resolved_config_summary", lambda *_args: None)
    monkeypatch.setattr(pm_main, "log_readiness_report", lambda *_args: None)
    monkeypatch.setattr(pm_main.signal, "signal", lambda *_args: None)
    monkeypatch.setattr(pm_main, "set_instrument_specs", lambda **_kwargs: None)
    monkeypatch.setattr(pm_main, "MT5_AVAILABLE", True)
    monkeypatch.setattr(pm_main, "MT5Connector", _Connector)
    monkeypatch.setattr(
        pm_main,
        "FXPortfolioManagerApp",
        lambda **_kwargs: (_ for _ in ()).throw(
            AssertionError("DEMO preflight must not construct the trading application")
        ),
    )
    monkeypatch.setattr(
        pm_main,
        "validate_live_readiness",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("funded readiness must not run for --preflight --paper")
        ),
    )
    monkeypatch.setattr(
        pm_demo_readiness, "validate_demo_probation_readiness", _demo_readiness
    )
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "pm_main.py",
            "--config",
            str(config_path),
            "--preflight",
            "--paper",
        ],
    )

    assert pm_main.main() == 0
    assert observed["demo_kwargs"]["account_info"].login == 106165989
    assert observed["connected"] is True
    assert observed["disconnected"] is True
    assert observed["connector_config"].login == 0
    assert observed["connector_config"].password == ""
    assert observed["connector_config"].server == ""
    assert (
        pm_main.LiveTrader._current_refit_demo_account_block_reason(_demo_account())
        is None
    )


def test_bounded_paper_preflight_fails_closed_and_disconnects_on_probe_error(
    monkeypatch,
):
    observed = {}

    class _Connector:
        def __init__(self, config):
            observed["connector_config"] = config

        @staticmethod
        def connect():
            raise RuntimeError("terminal unavailable")

        @staticmethod
        def disconnect():
            observed["disconnected"] = True

    monkeypatch.setattr(pm_main, "MT5_AVAILABLE", True)
    monkeypatch.setattr(pm_main, "MT5Connector", _Connector)
    monkeypatch.setattr(
        pm_demo_readiness,
        "validate_demo_probation_readiness",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("full evidence validation must not run after identity failure")
        ),
    )
    monkeypatch.setattr(
        pm_main,
        "log_readiness_report",
        lambda _logger, report: observed.update(report=report),
    )

    result = pm_main._run_bounded_demo_preflight(
        logger=Mock(),
        pipeline_config=SimpleNamespace(winner_ledger_path="ledger.json"),
        position_config=SimpleNamespace(),
        mt5_config=pm_main.MT5Config(
            login=28829971,
            password="must-not-be-forwarded",
            server="FBS-Real",
        ),
        symbols=["EURUSD"],
        config_path="config.json",
    )

    assert result == 2
    assert observed["disconnected"] is True
    assert observed["connector_config"].login == 0
    assert observed["connector_config"].password == ""
    assert observed["connector_config"].server == ""
    assert {finding.code for finding in observed["report"].errors} == {
        "demo_account_unavailable"
    }


def test_bounded_paper_preflight_fails_closed_when_disconnect_fails(monkeypatch):
    observed = {}

    class _Connector:
        def __init__(self, config):
            observed["connector_config"] = config

        @staticmethod
        def connect():
            return True

        @staticmethod
        def get_account_info():
            return _demo_account()

        @staticmethod
        def disconnect():
            raise RuntimeError("shutdown failed")

    monkeypatch.setattr(pm_main, "MT5_AVAILABLE", True)
    monkeypatch.setattr(pm_main, "MT5Connector", _Connector)
    monkeypatch.setattr(
        pm_demo_readiness,
        "validate_demo_probation_readiness",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("cleanup failure must stop before evidence validation")
        ),
    )
    monkeypatch.setattr(
        pm_main,
        "log_readiness_report",
        lambda _logger, report: observed.update(report=report),
    )

    result = pm_main._run_bounded_demo_preflight(
        logger=Mock(),
        pipeline_config=SimpleNamespace(winner_ledger_path="ledger.json"),
        position_config=SimpleNamespace(),
        mt5_config=pm_main.MT5Config(),
        symbols=["EURUSD"],
        config_path="config.json",
    )

    assert result == 2
    assert observed["connector_config"].login == 0
    assert {finding.code for finding in observed["report"].errors} == {
        "demo_preflight_mt5_disconnect_failed"
    }


def test_demo_constructor_raw_authority_failure_blocks(monkeypatch):
    mt5 = Mock()
    mt5.get_account_info.return_value = _demo_account()
    pipeline_config = SimpleNamespace(
        _deployment_config_path="config.json",
    )
    finding = SimpleNamespace(severity="error", code="raw_block", message="blocked")
    monkeypatch.setattr(
        pm_demo_readiness,
        "authorize_demo_probation_execution",
        lambda *_args, **_kwargs: (SimpleNamespace(ok=False, findings=[finding]), None),
    )
    with pytest.raises(ValueError, match="raw_block"):
        pm_main.LiveTrader(
            mt5_connector=mt5,
            portfolio_manager=Mock(),
            position_config=Mock(),
            pipeline_config=pipeline_config,
            execution_book=pm_main.CURRENT_REFIT_DEMO_EXECUTION_BOOK,
            demo_symbols=["EURUSD"],
            demo_winner_ledger_path="pm_configs.json",
        )


def test_disconnected_mt5_returns_explicit_trading_failure():
    app = pm_main.FXPortfolioManagerApp.__new__(pm_main.FXPortfolioManagerApp)
    app.mt5 = Mock()
    app.mt5.is_connected.return_value = False
    app.logger = Mock()
    assert app.run_trading() is False


def test_scheduled_timeframes_come_from_isolated_projection():
    app = pm_main.FXPortfolioManagerApp.__new__(pm_main.FXPortfolioManagerApp)
    app.trader = Mock()
    app.trader._get_live_configs.return_value = {
        "EURUSD": SymbolConfig.from_dict(_projection()["EURUSD"])
    }
    app.portfolio_manager = Mock()
    app.pipeline_config = SimpleNamespace(timeframes=["D1"])
    assert app._get_active_live_timeframes() == ["H1"]
    app.portfolio_manager.get_live_eligible_configs.assert_not_called()


def test_demo_loop_cannot_auto_retrain_or_use_legacy_selector():
    source = inspect.getsource(pm_main.FXPortfolioManagerApp.run_trading)
    constructor_source = inspect.getsource(pm_main.LiveTrader.__init__)
    assert "not is_current_refit_demo and now >= next_retrain_check_at" in source
    assert "get_edge_challenger_configs" not in source
    assert "authorize_demo_probation_execution" not in source
    assert "authorize_demo_probation_execution" in constructor_source


def test_pre_send_uses_full_management_projection():
    source = inspect.getsource(pm_main.LiveTrader._execute_entry)
    assert "self._get_management_configs()" in source
    assert "PROCESS_VALIDATED_CURRENT_REFIT_ROLE" in source


def test_demo_capital_config_mutation_blocks_final_authorization():
    trader = _isolated_trader()
    trader.pipeline_config = SimpleNamespace(
        max_total_open_risk_pct=20.0,
        max_factor_group_open_risk_pct=10.0,
        max_combined_risk_pct=5.0,
        margin_entry_block_level=200.0,
    )
    trader._current_refit_demo_capital_config_sha256 = (
        pm_demo_readiness.demo_capital_config_sha256(trader.pipeline_config)
    )
    trader.position_config = pm_main.PositionConfig(
        risk_per_trade_pct=0.25,
        max_risk_pct=0.5,
        risk_tolerance_pct=10.0,
    )
    trader._current_refit_demo_position_config_sha256 = (
        pm_demo_readiness.demo_position_config_sha256(trader.position_config)
    )
    trader._current_refit_demo_capital_envelope = {
        "max_total_stop_risk_pct": 20.0,
        "max_factor_stop_risk_pct": 10.0,
        "max_same_symbol_stop_risk_pct": 5.0,
        "max_margin_utilization_pct": 50.0,
        "min_margin_level_pct": 200.0,
        "min_free_margin_pct": 50.0,
        "gap_stress_multiple": 1.5,
    }
    assert trader._current_refit_demo_capital_block_reason() is None
    trader.pipeline_config.max_total_open_risk_pct = 90.0
    assert "capital config changed" in trader._current_refit_demo_capital_block_reason()


def test_demo_detector_mutation_blocks_final_authorization():
    trader = _isolated_trader()
    trader.pipeline_config = SimpleNamespace(
        max_total_open_risk_pct=20.0,
        max_factor_group_open_risk_pct=10.0,
        max_combined_risk_pct=5.0,
        margin_entry_block_level=200.0,
    )
    trader._current_refit_demo_capital_config_sha256 = (
        pm_demo_readiness.demo_capital_config_sha256(trader.pipeline_config)
    )
    trader.position_config = pm_main.PositionConfig(
        risk_per_trade_pct=0.25,
        max_risk_pct=0.5,
        risk_tolerance_pct=10.0,
    )
    trader._current_refit_demo_position_config_sha256 = (
        pm_demo_readiness.demo_position_config_sha256(trader.position_config)
    )
    trader._current_refit_demo_capital_envelope = {
        "max_total_stop_risk_pct": 20.0,
        "max_factor_stop_risk_pct": 10.0,
        "max_same_symbol_stop_risk_pct": 5.0,
        "max_margin_utilization_pct": 50.0,
        "min_margin_level_pct": 200.0,
        "min_free_margin_pct": 50.0,
        "gap_stress_multiple": 1.5,
    }
    assert trader._current_refit_demo_capital_block_reason() is None
    trader._current_refit_demo_detectors["EURUSD|H1"]["parameters"][
        "adx_period"
    ] = 15
    assert "detector authority changed" in (
        trader._current_refit_demo_capital_block_reason()
    )


def test_demo_position_config_mutation_blocks_final_authorization():
    trader = _isolated_trader()
    trader.pipeline_config = SimpleNamespace(
        max_total_open_risk_pct=20.0,
        max_factor_group_open_risk_pct=10.0,
        max_combined_risk_pct=5.0,
        margin_entry_block_level=200.0,
    )
    trader._current_refit_demo_capital_config_sha256 = (
        pm_demo_readiness.demo_capital_config_sha256(trader.pipeline_config)
    )
    trader._current_refit_demo_capital_envelope = {
        "max_total_stop_risk_pct": 20.0,
        "max_factor_stop_risk_pct": 10.0,
        "max_same_symbol_stop_risk_pct": 5.0,
        "max_margin_utilization_pct": 50.0,
        "min_margin_level_pct": 200.0,
        "min_free_margin_pct": 50.0,
        "gap_stress_multiple": 1.5,
    }
    trader.position_config = pm_main.PositionConfig(
        risk_per_trade_pct=0.25,
        max_risk_pct=0.5,
        risk_tolerance_pct=10.0,
    )
    trader._current_refit_demo_position_config_sha256 = (
        pm_demo_readiness.demo_position_config_sha256(trader.position_config)
    )
    assert trader._current_refit_demo_capital_block_reason() is None
    trader.position_config.risk_per_trade_pct = 4.0
    assert "position config changed" in trader._current_refit_demo_capital_block_reason()


@pytest.mark.parametrize("style", ["z", "offset", "naive"])
def test_probation_timestamp_forms_do_not_crash_or_change_age(style):
    old = datetime.now(timezone.utc) - timedelta(days=30)
    if style == "z":
        stamp = old.isoformat().replace("+00:00", "Z")
    elif style == "offset":
        stamp = old.astimezone(timezone(timedelta(hours=2))).isoformat()
    else:
        stamp = old.replace(tzinfo=None).isoformat()
    trader = pm_main.LiveTrader.__new__(pm_main.LiveTrader)
    trader.pipeline_config = SimpleNamespace(
        probation_enabled=True,
        probation_days=14,
        probation_risk_multiplier=0.5,
        probation_min_live_outcomes=1,
        probation_min_mean_r=0.0,
        probation_min_profit_factor=1.0,
    )
    trader._live_outcome_ledger = Mock()
    trader._live_outcome_ledger.exact_cell_summary.return_value = {
        "exact_closed_positions": 1,
        "r_observations": 1,
        "mean_r": 0.1,
        "profit_factor_r": 1.1,
    }
    cell = SimpleNamespace(trained_at=stamp, strategy_name="TrendExpert")
    assert trader._probation_multiplier(
        cell,
        SimpleNamespace(optimized_at=None),
        symbol="EURUSD",
        timeframe="H1",
        regime="TREND",
    ) == (1.0, False)


@pytest.mark.parametrize(
    "stamp",
    ["invalid", (datetime.now(timezone.utc) + timedelta(days=1)).isoformat()],
)
def test_invalid_or_future_probation_timestamp_stays_probationary(stamp):
    trader = pm_main.LiveTrader.__new__(pm_main.LiveTrader)
    trader.pipeline_config = SimpleNamespace(
        probation_enabled=True,
        probation_days=14,
        probation_risk_multiplier=0.5,
    )
    trader._live_outcome_ledger = Mock()
    cell = SimpleNamespace(trained_at=stamp, strategy_name="TrendExpert")
    assert trader._probation_multiplier(
        cell,
        SimpleNamespace(optimized_at=None),
        symbol="EURUSD",
        timeframe="H1",
        regime="TREND",
    ) == (0.5, True)
