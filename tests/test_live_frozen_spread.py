"""Synthetic live admission and paper-submit conformance, not profit evidence."""

from types import SimpleNamespace
from unittest.mock import Mock
import sys
import threading
import json
from dataclasses import replace
from datetime import datetime, timedelta, timezone

import numpy as np
import pandas as pd
import pytest

import pm_main
from pm_main import LiveTrader
from pm_spread_profile import load_spread_profile, spread_admission


SOURCE_ID = "0be6d0831806c1a62ab723193a87a362772a14d8bf4ccbe2d3a8774962eefe09"
OPEN = pd.Timestamp("2020-01-01 01:00", tz="UTC")


@pytest.fixture(scope="module")
def tier():
    return load_spread_profile().resolve(
        "EURUSD", "H1", "P75", point=0.00001, source_identity_sha256=SOURCE_ID
    )


def prior_bars(spread=9):
    return pd.DataFrame(
        {"Spread": [1, spread, 1000]},
        index=pd.date_range(OPEN - pd.Timedelta(minutes=10), periods=3, freq="5min"),
    )


def tick(points=9):
    return SimpleNamespace(bid=1.0, ask=1.0 + points / 100000)


def test_live_binding_uses_runtime_identity_and_exact_cell_descriptor(monkeypatch, tier):
    import pm_mt5
    import pm_native_request_contract
    from tools import acquire_native_history

    snapshot = Mock(return_value={"synthetic_metadata": True})
    identity = Mock(return_value={"identity_sha256": SOURCE_ID})
    monkeypatch.setattr(acquire_native_history, "snapshot_read_only_runtime_metadata", snapshot)
    monkeypatch.setattr(pm_native_request_contract, "canonical_source_identity", identity)
    trader = LiveTrader.__new__(LiveTrader)
    trader.mt5 = Mock()
    cell = SimpleNamespace(artifact_meta={"validated_spread_tier": tier.descriptor()})
    resolved = trader._resolve_live_spread_tier(
        "EURUSD", "EURUSD", "H1", SimpleNamespace(point=0.00001), cell
    )
    assert resolved == tier
    snapshot.assert_called_once_with(trader.mt5, terminal_api=pm_mt5.mt5)
    identity.assert_called_once_with({"synthetic_metadata": True})

    for changes in ({"tier": "P95"}, {"profile_sha256": "0" * 64}, {"timeframe": "D1"}):
        cell.artifact_meta["validated_spread_tier"] = {**tier.descriptor(), **changes}
        with pytest.raises(ValueError):
            trader._resolve_live_spread_tier(
                "EURUSD", "EURUSD", "H1", SimpleNamespace(point=0.00001), cell
            )


@pytest.mark.parametrize("cell", [None, SimpleNamespace(artifact_meta={})])
def test_missing_legacy_descriptor_cannot_get_a_live_tier(cell):
    trader = LiveTrader.__new__(LiveTrader)
    with pytest.raises(ValueError, match="DESCRIPTOR_UNAVAILABLE"):
        trader._resolve_live_spread_tier(
            "EURUSD", "EURUSD", "H1", SimpleNamespace(point=0.00001), cell
        )


def test_runtime_identity_failure_has_no_invented_fallback(monkeypatch, tier):
    from tools import acquire_native_history

    monkeypatch.setattr(
        acquire_native_history, "snapshot_read_only_runtime_metadata",
        Mock(side_effect=RuntimeError("synthetic terminal unavailable")),
    )
    trader = LiveTrader.__new__(LiveTrader)
    trader.mt5 = Mock()
    cell = SimpleNamespace(artifact_meta={"validated_spread_tier": tier.descriptor()})
    with pytest.raises(ValueError, match="SOURCE_IDENTITY_UNAVAILABLE"):
        trader._resolve_live_spread_tier(
            "EURUSD", "EURUSD", "H1", SimpleNamespace(point=0.00001), cell
        )


@pytest.mark.parametrize("spread,state", [(0, "ADMIT"), (9, "ADMIT"), (10, "REFUSE")])
def test_native_prior_fine_bar_closes_exactly_at_target_open(tier, spread, state):
    trader = LiveTrader.__new__(LiveTrader)
    trader.mt5 = Mock()
    bars = prior_bars(spread)
    trader.mt5.get_bars.return_value = bars
    observation = trader._live_prior_spread_observation("EURUSD", tier, OPEN)
    assert observation == spread
    assert spread_admission(tier, observation) == state
    trader.mt5.get_bars.assert_called_once_with("EURUSD", "M5", count=14)
    assert "_BAR_CLOSE_TS" not in bars  # no source mutation


def test_prior_fine_gap_is_missing_without_inbar_or_ffill(tier):
    trader = LiveTrader.__new__(LiveTrader)
    trader.mt5 = Mock()
    trader.mt5.get_bars.return_value = prior_bars().drop(OPEN - pd.Timedelta(minutes=5))
    assert np.isnan(trader._live_prior_spread_observation("EURUSD", tier, OPEN))


def test_fractional_prior_bar_spread_is_invalid(tier):
    trader = LiveTrader.__new__(LiveTrader)
    trader.mt5 = Mock()
    trader.mt5.get_bars.return_value = prior_bars(8.5)
    with pytest.raises(ValueError, match="POINTS_INVALID"):
        trader._live_prior_spread_observation("EURUSD", tier, OPEN)


@pytest.mark.parametrize("quote,status", [
    (tick(0), "ADMIT"), (tick(9), "ADMIT"), (tick(10), "REFUSE"),
    (SimpleNamespace(bid=1.0, ask=0.9), "INVALID"),
    (SimpleNamespace(bid=0, ask=0), "INVALID"),
    (SimpleNamespace(bid=1.0, ask=np.inf), "INVALID"), (None, "INVALID"),
])
def test_first_and_final_quote_rule_shares_exact_cap(tier, quote, status):
    assert LiveTrader._live_tick_spread_admission(tier, quote)[0] == status


def paper_trader(tier, quotes, prior=9):
    trader = LiveTrader.__new__(LiveTrader)
    trader.mt5 = Mock()
    trader.logger = Mock()
    trader._decision_throttle = Mock()
    trader._actionable_log = Mock()
    trader._last_order_times = {}
    trader.enable_trading = False
    trader.position_config = SimpleNamespace(
        risk_per_trade_pct=0.5, max_risk_pct=2.0, risk_tolerance_pct=2.0,
        min_position_size=0.01, max_position_size=0.0, risk_basis="equity",
        auto_widen_sl=False,
    )
    trader.pipeline_config = SimpleNamespace(
        manual_new_entries_enabled=True, min_trade_risk_pct=0.01,
    )
    trader.position_calc = Mock()
    trader.position_calc.calculate_stop_prices.return_value = (0.99, 1.02)
    trader._enhancement_seams = SimpleNamespace(
        risk_scalar_stack=SimpleNamespace(overlays=[]),
        execution_quality_overlay=SimpleNamespace(enabled=False),
    )
    trader._margin_state_persistence_block_reason = Mock(return_value=None)
    trader._bound_capital_admission_envelope = Mock(return_value={
        "min_margin_level_pct": 200.0, "max_total_stop_risk_pct": 8.0,
    })
    trader._margin_cooldown_remaining_minutes = Mock(return_value=0.0)
    trader._same_symbol_specialist_position_block = Mock(return_value=None)
    trader._estimate_stop_loss_amount = Mock(
        side_effect=lambda _type, _symbol, volume, *_args, **_kwargs: (100 * volume, "synthetic")
    )
    trader._check_symbol_combined_risk_cap = Mock(return_value=(True, "synthetic capacity"))
    trader._estimate_total_open_risk_pct = Mock(return_value=0.0)
    trader._estimate_realized_loss_pct = Mock(return_value=None)
    trader._estimate_open_drawdown_pct = Mock(return_value=0.0)
    allow = SimpleNamespace(
        allow_trade=True, risk_multiplier=1.0, observed_only=False, reasons=[], action="allow",
    )
    trader._cycle_drift_monitor = Mock()
    trader._cycle_drift_monitor.evaluate.return_value = allow
    trader._institutional_supervisor = Mock()
    trader._institutional_supervisor.evaluate.return_value = allow
    trader._resolve_live_spread_tier = Mock(return_value=tier)
    trader._log_trade = Mock()
    trader.mt5.get_position_by_symbol_magic.return_value = None
    trader.mt5.get_positions.return_value = []
    trader.mt5.get_symbol_info.return_value = SimpleNamespace(
        visible=True, trade_mode=2, volume_min=0.01, volume_max=100.0,
        trade_stops_level=0, point=0.00001,
    )
    trader.mt5.get_symbol_tick.side_effect = quotes
    trader.mt5.get_bars.return_value = prior_bars(prior)
    trader.mt5.normalize_volume.side_effect = lambda volume, _info: volume
    trader.mt5.calc_margin_required.return_value = 100.0
    return trader


def execute_paper(trader):
    trader._execute_entry(
        symbol="EURUSD", broker_symbol="EURUSD", signal=1, strategy=Mock(),
        features=pd.DataFrame({"ATR_14": [0.01, 0.01]}, index=[OPEN - pd.Timedelta(hours=1), OPEN]),
        spec=SimpleNamespace(pip_size=0.0001, point=0.00001, spread_avg=0.9),
        magic=123, config=SimpleNamespace(symbol="EURUSD"),
        decision_key="synthetic-entry", bar_time_iso=str(OPEN - pd.Timedelta(hours=1)),
        best_candidate={"timeframe": "H1", "regime": "TREND", "strategy_name": "Synthetic"},
        trade_intent=SimpleNamespace(stop_loss_pips=100, take_profit_pips=200),
        positions_snapshot=[], account_info=SimpleNamespace(
            balance=10000.0, equity=10000.0, margin=0.0, margin_level=0.0, margin_free=10000.0,
        ),
    )
    trader.mt5.send_market_order.assert_not_called()
    return trader._actionable_log.record.call_args.args[1]


@pytest.mark.parametrize("quotes,prior,stage", [
    ([tick(10)], 9, "initial_quote"),
    ([tick(9), tick(10)], 9, "final_quote"),
    ([tick(9)], 10, "prior_admission"),
])
def test_entry_blocks_known_cap_failure_at_each_stage(tier, quotes, prior, stage):
    trader = paper_trader(tier, quotes, prior)
    payload = execute_paper(trader)
    assert payload["action"] == "BLOCKED_SPREAD_FILTER"
    audit = payload["context"]["spread_profile"]
    assert (audit[stage] if stage == "prior_admission" else audit[stage]["admission"]) == "REFUSE"
    assert audit["target_open"] == str(OPEN)
    trader._log_trade.assert_not_called()


@pytest.mark.parametrize("missing_prior", [False, True])
def test_paper_entry_requires_prior_observation_despite_available_quotes(tier, missing_prior):
    quotes = [tick(5), tick(9)]
    assert all(LiveTrader._live_tick_spread_admission(tier, quote)[0] == "ADMIT" for quote in quotes)
    trader = paper_trader(tier, quotes)
    if missing_prior:
        trader.mt5.get_bars.return_value = prior_bars().drop(OPEN - pd.Timedelta(minutes=5))
    payload = execute_paper(trader)
    audit = payload["context"]["spread_profile"]
    if missing_prior:
        assert payload["action"] == "BLOCKED_SPREAD_FILTER"
        assert audit["prior_admission"] == "MISSING"
        trader._log_trade.assert_not_called()
    else:
        assert payload["action"] == "PAPER"
        assert audit["prior_admission"] == "ADMIT"
        assert audit["initial_quote"]["admission"] == "ADMIT"
        assert audit["final_quote"]["admission"] == "ADMIT"
        assert audit["initial_quote"]["spread_points"] == 5
        assert audit["final_quote"]["spread_points"] == 9


def test_paper_path_audits_first_and_final_quotes_and_uses_final_width_for_drift(tier):
    trader = paper_trader(tier, [tick(5), tick(9)])
    payload = execute_paper(trader)
    assert payload["action"] == "PAPER"
    audit = payload["context"]["spread_profile"]
    assert audit["descriptor"] == tier.descriptor()
    assert audit["initial_quote"]["spread_points"] == 5
    assert audit["final_quote"]["spread_points"] == 9
    contexts = [call.args[0] for call in trader._cycle_drift_monitor.evaluate.call_args_list]
    assert contexts[0].spread_degradation_mult == pytest.approx(5 / 9)
    assert contexts[-1].spread_degradation_mult == pytest.approx(1.0)


def test_fixed_cost_descriptor_does_not_override_operator_kill_switch(tier):
    trader = paper_trader(tier, [tick(9), tick(9)])
    trader.pipeline_config.manual_new_entries_enabled = False
    payload = execute_paper(trader)
    assert payload["action"] == "BLOCKED_MANUAL_KILL_SWITCH"
    trader._resolve_live_spread_tier.assert_not_called()
    trader._log_trade.assert_not_called()


def test_position_exit_does_not_depend_on_entry_cost_admission():
    trader = LiveTrader.__new__(LiveTrader)
    trader.enable_trading = True
    trader.logger = Mock()
    trader.mt5 = Mock()
    # Explicit synthetic identity, not a broker record or grant.
    trader.mt5.config = SimpleNamespace(
        expected_login=123, expected_server="Broker-Test", expected_currency="USD"
    )
    trader.mt5.get_account_info.return_value = SimpleNamespace(
        login=123, server="Broker-Test", currency="USD"
    )
    trader.mt5.close_position.return_value = SimpleNamespace(success=True)
    trader._resolve_live_spread_tier = Mock(side_effect=ValueError("unavailable"))
    position = SimpleNamespace(symbol="EURUSD")
    trader._close_position_on_signal(position, pd.DataFrame(), None)
    trader.mt5.close_position.assert_called_once_with(
        position=position, deviation=30, comment="PM_SignalExit"
    )
    trader._resolve_live_spread_tier.assert_not_called()
    trader.mt5.get_symbol_tick.assert_not_called()


def test_startup_evidence_block_cannot_be_bypassed_by_direct_entry(tier):
    trader = paper_trader(tier, [tick(9), tick(9)])
    trader._startup_entry_block_reason = "interval_cell_qualification_policy_unadopted"
    payload = execute_paper(trader)
    assert payload["action"] == "BLOCKED_ENTRY_READINESS"
    trader._resolve_live_spread_tier.assert_not_called()
    trader._log_trade.assert_not_called()


@pytest.mark.parametrize("book", ["funded", "demo"])
@pytest.mark.parametrize("mode", ["conservative_max", "fixed_spec", "historical_bar", None])
def test_actual_readiness_retains_cost_mode_error_beside_interval_policy(tmp_path, book, mode):
    import pm_demo_readiness as demo
    import pm_live_readiness as live

    # Source authority is deliberately absent; only the cost predicates are isolated here.
    config = SimpleNamespace(
        use_spread=True, swap_model_enabled=True,
        data_dir=tmp_path / "data", log_dir=tmp_path / "logs",
        winner_ledger_path=str(tmp_path / "synthetic_ledger.json"),
    )
    if mode is not None:
        config.backtest_spread_mode = mode
    validator = (
        live.validate_live_readiness if book == "funded"
        else demo.validate_demo_probation_readiness
    )
    report = validator(
        config, symbols=["EURUSD"], config_path=str(tmp_path / "synthetic_config.json")
    )
    codes = {(finding.code, finding.scope) for finding in report.errors}
    mode_error = (
        "backtest_spread_mode_not_conservative" if book == "funded"
        else "demo_cost_contract_invalid"
    )
    assert ("interval_cell_qualification_policy_unadopted", "evidence") in codes
    assert ((mode_error, "evidence") in codes) is (mode != "conservative_max")
    assert not report.ok


@pytest.mark.parametrize("case,expected_exit", [
    ("interval_only", 0), ("interval_with_warning", 0),
    ("other_evidence", 2), ("runtime", 2), ("interval_runtime_scope", 2),
    ("interval_unknown_scope", 2), ("interval_extra_evidence", 2),
    ("interval_extra_runtime", 2), ("interval_duplicate", 2), ("missing_error", 2),
    ("nonconservative_spread_mode", 2),
])
def test_funded_main_preserves_management_only_for_unadopted_interval_policy(monkeypatch, tmp_path, case, expected_exit):
    """Exercise main -> real startup loop -> real management dispatch, with no broker orders."""
    from pm_live_readiness import ReadinessFinding, ReadinessReport

    app_type = pm_main.FXPortfolioManagerApp
    trader_type = LiveTrader
    shutdown_event = threading.Event()
    positions = [SimpleNamespace(symbol="EURUSD", ticket=1)]
    management = {"EURUSD": SimpleNamespace(symbol="EURUSD")}
    # Explicit synthetic identity, not a broker record or grant.
    account = SimpleNamespace(
        login=123, server="Broker-Test", currency="USD",
        equity=10000, trade_allowed=True, trade_expert=True,
    )
    observed = {}

    class SyntheticApp(app_type):
        def __init__(self, **kwargs):
            self.pipeline_config = kwargs["pipeline_config"]
            self.position_config = kwargs["position_config"]
            self.symbols = ["EURUSD"]
            self.config_file = str(tmp_path / "synthetic_ledger.json")
            self.storage_manager = None
            self.logger = Mock()
            self.mt5 = Mock()
            self.mt5.config = SimpleNamespace(
                expected_login=123, expected_server="Broker-Test", expected_currency="USD"
            )
            self.mt5.is_connected.return_value = True
            self.mt5.get_positions.return_value = positions
            self.mt5.get_account_info.return_value = account
            self.mt5.get_recent_closing_deals.return_value = []
            self.portfolio_manager = Mock()
            self.portfolio_manager.get_validated_configs.return_value = management
            self.portfolio_manager.get_live_eligible_configs.return_value = management
            self._check_production_retrain_schedule = Mock()
            self._wait_for_next_due = lambda *_args, **_kwargs: shutdown_event.set()
            self.shutdown = Mock()
            observed["app"] = self

        def initialize(self):
            return True

    def construct_trader(**kwargs):
        trader = trader_type.__new__(trader_type)
        trader.pm = kwargs["portfolio_manager"]
        trader.mt5 = kwargs["mt5_connector"]
        trader.pipeline_config = kwargs["pipeline_config"]
        trader.logger = Mock()
        trader.storage_manager = None
        trader._startup_entry_block_reason = kwargs["startup_entry_block_reason"]
        trader._is_current_refit_demo = False
        trader._shutdown_event = shutdown_event
        trader._sweep_recent_m5_windows = {}
        trader._equity_peak = 0.0
        trader.get_symbols_with_new_bars = Mock(return_value={})
        trader.commit_bar_probe_times = Mock()
        trader._run_margin_protection_cycle = Mock(return_value=False)
        trader._run_order_governance_cycle = Mock()
        trader._realized_loss_hard_block_reasons = Mock(return_value=[])
        trader._sync_drift_monitor = Mock()
        trader._evaluate_daily_loss_advisory = Mock()
        trader._run_portfolio_observatory = Mock()
        trader._process_symbol = Mock()
        observed["trader"] = trader
        return trader

    monkeypatch.setattr(pm_main, "FXPortfolioManagerApp", SyntheticApp)
    monkeypatch.setattr(pm_main, "LiveTrader", construct_trader)
    monkeypatch.setattr(pm_main, "_app_instance", None)
    monkeypatch.setattr(pm_main, "load_config_json", lambda _path: {
        "_meta": {"scope": "funded_live_v1"},
        "symbols": ["EURUSD"], "pipeline": {
            "data_dir": str(tmp_path / "data"), "output_dir": str(tmp_path / "out"),
            "log_dir": str(tmp_path / "logs"), "winner_ledger_path": str(tmp_path / "ledger.json"),
        },
    })
    monkeypatch.setattr(pm_main, "setup_logging", lambda **_kwargs: None)
    monkeypatch.setattr(pm_main, "log_resolved_config_summary", lambda *_args: None)
    monkeypatch.setattr(pm_main, "log_readiness_report", lambda *_args: None)
    monkeypatch.setattr(pm_main.signal, "signal", lambda *_args: None)
    monkeypatch.setattr(pm_main, "set_instrument_specs", lambda **_kwargs: None)
    monkeypatch.setattr(pm_main, "_production_hard_supervision_required", lambda *_args, **_kwargs: False)
    interval_error = ReadinessFinding(
        "error", "interval_cell_qualification_policy_unadopted", "synthetic", "evidence"
    )
    findings_by_case = {
        "interval_only": [interval_error],
        "interval_with_warning": [interval_error, ReadinessFinding(
            "warning", "synthetic_warning", "synthetic", "evidence"
        )],
        "other_evidence": [ReadinessFinding(
            "error", "synthetic_evidence_failure", "synthetic", "evidence"
        )],
        "runtime": [ReadinessFinding(
            "error", "synthetic_runtime_failure", "synthetic", "runtime"
        )],
        "interval_runtime_scope": [ReadinessFinding(
            "error", interval_error.code, "synthetic", "runtime"
        )],
        "interval_unknown_scope": [ReadinessFinding(
            "error", interval_error.code, "synthetic", "unknown"
        )],
        "interval_extra_evidence": [interval_error, ReadinessFinding(
            "error", "synthetic_evidence_failure", "synthetic", "evidence"
        )],
        "interval_extra_runtime": [interval_error, ReadinessFinding(
            "error", "synthetic_runtime_failure", "synthetic", "runtime"
        )],
        "interval_duplicate": [interval_error, interval_error],
        "missing_error": [],
        "nonconservative_spread_mode": [interval_error, ReadinessFinding(
            "error", "backtest_spread_mode_not_conservative", "synthetic", "evidence"
        )],
    }
    monkeypatch.setattr(pm_main, "validate_live_readiness", lambda *_args, **_kwargs: ReadinessReport(
        False, findings_by_case[case],
    ))
    monkeypatch.setattr(sys, "argv", ["pm_main.py", "--config", str(tmp_path / "synthetic.json"), "--trade"])

    assert pm_main.main() == expected_exit
    app = observed["app"]
    app.mt5.send_market_order.assert_not_called()
    if expected_exit == 2:
        assert "trader" not in observed
    else:
        trader = observed["trader"]
        assert trader._startup_entry_block_reason == interval_error.code
        assert trader._get_live_configs() == {}
        assert trader._get_management_configs() == management
        trader._run_margin_protection_cycle.assert_called_once_with(
            account_info=account, positions_snapshot=positions
        )
        trader._run_order_governance_cycle.assert_called_once_with(management, positions)
        trader._process_symbol.assert_not_called()
        app.portfolio_manager.get_live_eligible_configs.assert_not_called()


def synthetic_demo_management_report():
    """A producer-seam fixture; it is not broker or statistical authority."""
    import pm_demo_readiness as demo
    from pm_live_readiness import ReadinessFinding
    from pm_regime import RegimeParams, detector_code_sha256, regime_params_sha256

    params = RegimeParams().to_dict()
    binding = {name: "a" * 64 for name in LiveTrader._CURRENT_REFIT_BINDING_FIELDS}
    binding["regime_detector_parameters_sha256"] = regime_params_sha256(params)
    binding["regime_detector_code_sha256"] = detector_code_sha256()
    deadline = (datetime.now(timezone.utc) + timedelta(days=1)).isoformat()
    projection = {"EURUSD": {
        "symbol": "EURUSD", "valid_until": deadline,
        "regime_configs": {"H1": {"TREND": {
            "strategy_name": "TrendExpert", "parameters": {}, "quality_score": 0.8,
            "promotion_status": pm_main.PROCESS_VALIDATED_CURRENT_REFIT_ROLE,
            "artifact_meta": {"current_refit_binding": binding}, "valid_until": deadline,
        }}},
    }}
    detectors = {"records": {"EURUSD|H1": {
        "regime_fit_receipt_sha256": binding["regime_fit_receipt_sha256"],
        "record_sha256": binding["regime_detector_record_sha256"],
        "parameters_sha256": binding["regime_detector_parameters_sha256"],
        "detector_code_sha256": binding["regime_detector_code_sha256"], "parameters": params,
    }}}
    execution = demo._DemoExecutionBinding(
        schema_version=demo.DEMO_EXECUTION_BINDING_SCHEMA,
        ledger_path="synthetic_ledger.json", ledger_sha256="a" * 64,
        projection_sha256=LiveTrader._canonical_payload_sha256(projection),
        detector_projection_sha256=LiveTrader._canonical_payload_sha256(detectors),
        detector_projection_json=json.dumps(detectors), config_path="synthetic_config.json",
        config_sha256="a" * 64, capital_config_sha256="a" * 64,
        position_config_sha256="a" * 64,
        selected_capital_envelope=(("max_total_stop_risk_pct", 8.0),),
        native_activation_id="synthetic", native_activation_manifest_sha256="a" * 64,
        authority_result_sha256="a" * 64, requested_symbols=("EURUSD",),
        selected_cell_ids=("EURUSD|H1|TREND",),
    )
    management = demo._DemoManagementBinding(
        schema_version=demo.DEMO_MANAGEMENT_BINDING_SCHEMA, store_root="synthetic",
        capsule_id="b" * 64, generation_id="synthetic-generation",
        management_context_sha256="c" * 64, management_context_json="{}",
    )
    return demo.DemoProbationReadinessReport(
        ok=False, findings=[ReadinessFinding(
            "error", "interval_cell_qualification_policy_unadopted", "synthetic", "evidence"
        )], _execution_binding=execution, _management_binding=management,
        _management_projection_json=json.dumps(projection),
    )


@pytest.mark.parametrize("case", [
    "verified", "other_error", "missing_capsule", "bad_capsule", "untyped",
    "nonconservative_spread_mode",
])
def test_demo_startup_management_requires_typed_verified_management_only_report(monkeypatch, tmp_path, case):
    import pm_demo_readiness as demo
    from pm_live_readiness import ReadinessFinding
    from pm_position import TradeTagEncoder

    report = synthetic_demo_management_report()
    if case == "other_error":
        report.findings.append(ReadinessFinding("error", "raw_authority_failed", "synthetic", "evidence"))
    elif case == "nonconservative_spread_mode":
        report.findings.append(ReadinessFinding(
            "error", "demo_cost_contract_invalid", "synthetic", "evidence"
        ))
    elif case == "missing_capsule":
        report._management_binding = None
    elif case == "bad_capsule":
        report._management_binding = replace(report._management_binding, capsule_id="")
    elif case == "untyped":
        report = SimpleNamespace(ok=False, management_only=True, findings=report.findings)
    authorize = Mock(return_value=(report, None))
    monkeypatch.setattr(demo, "authorize_demo_probation_execution", authorize)
    for name in ("RuntimeObservabilityStore", "DecisionThrottle", "ActionableDecisionLog", "PositionCalculator"):
        monkeypatch.setattr(pm_main, name, Mock())
    monkeypatch.setattr(LiveTrader, "_restore_margin_protection_state", Mock())

    original_init = LiveTrader.__init__

    def initialize(trader, **kwargs):
        original_init(trader, **kwargs)
        trader.get_symbols_with_new_bars = Mock(return_value={})
        trader.commit_bar_probe_times = Mock()
        trader._run_margin_protection_cycle = Mock(return_value=False)
        trader._run_order_governance_cycle = Mock()
        trader._realized_loss_hard_block_reasons = Mock(return_value=[])
        trader._sync_drift_monitor = Mock()
        trader._evaluate_daily_loss_advisory = Mock()
        trader._run_portfolio_observatory = Mock()
        trader._process_symbol = Mock()

    monkeypatch.setattr(LiveTrader, "__init__", initialize)
    identity = demo.AUTHORIZED_DEMO_ACCOUNT_IDENTITY
    account = SimpleNamespace(
        login=identity.login, server=identity.server, currency=identity.currency,
        trade_mode=identity.trade_mode, trade_allowed=True, trade_expert=True,
        equity=10000.0, margin_mode=2, hedge_allowed=True,
    )
    position = SimpleNamespace(
        symbol="EURUSD", ticket=1,
        magic=TradeTagEncoder.encode_magic("EURUSD", "H1", "CURRENT_REFIT::TREND"),
        comment=TradeTagEncoder.encode_comment(
            "EURUSD", "H1", "TrendExpert", "LONG", risk_pct=0.5,
            book_role=pm_main.PROCESS_VALIDATED_CURRENT_REFIT_ROLE,
        ),
    )
    app = pm_main.FXPortfolioManagerApp.__new__(pm_main.FXPortfolioManagerApp)
    app.logger = Mock()
    app.mt5 = Mock()
    app.mt5.is_connected.return_value = True
    app.mt5.get_account_info.return_value = account
    app.mt5.get_positions.return_value = [position]
    app.mt5.find_broker_symbol.side_effect = lambda symbol: symbol
    app.mt5.get_recent_closing_deals.return_value = []
    app.portfolio_manager = Mock(symbols=[])
    app.symbols = ["EURUSD"]
    app.config_file = str(tmp_path / "synthetic_ledger.json")
    app.storage_manager = None
    app.position_config = SimpleNamespace()
    app.pipeline_config = SimpleNamespace(
        _deployment_config_path=str(tmp_path / "synthetic_config.json"),
        output_dir=str(tmp_path / "out"), live_loop_trigger_mode="bar",
        production_retrain_mode="disabled", describe_retrain_schedule=lambda: "disabled",
    )
    app.shutdown = Mock()
    app._check_demo_refit_expiry_notice = Mock()
    app._wait_for_next_due = lambda event, *_args, **_kwargs: event.set()

    result = app.run_trading(execution_book=pm_main.CURRENT_REFIT_DEMO_EXECUTION_BOOK)
    authorize.assert_called_once()
    app.mt5.send_market_order.assert_not_called()
    app.portfolio_manager.get_live_eligible_configs.assert_not_called()
    if case != "verified":
        assert result is False
        assert app.trader is None
    else:
        assert result is not False
        trader = app.trader
        assert report.ok is False and report.management_only
        assert trader._startup_entry_block_reason == "interval_cell_qualification_policy_unadopted"
        assert trader._demo_management_capsule_id == "b" * 64
        assert trader._get_live_configs() == {}
        management = trader._get_management_configs()
        assert set(management) == {"EURUSD"}
        trader._run_margin_protection_cycle.assert_called_once_with(
            account_info=account, positions_snapshot=[position]
        )
        trader._run_order_governance_cycle.assert_called_once_with(management, [position])
        trader._process_symbol.assert_not_called()
