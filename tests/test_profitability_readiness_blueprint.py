import json
import tempfile
import unittest
from datetime import datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import pandas as pd

from pm_core import InstrumentSpec, PipelineConfig, Backtester
from pm_cycle_drift import CycleDriftContext, CycleDriftMonitor
from pm_institutional_supervisor import InstitutionalSupervisor, SupervisorContext
from pm_live_readiness import validate_live_readiness
from pm_main import LiveTrader
from pm_order_governance import GovernanceContext, candidate_policy_names, evaluate_policy, normalize_policy_name
from pm_pipeline import RegimeConfig
from pm_strategies import (
    LiquiditySweepReversalStrategy,
    RegimeTransitionBreakoutStrategy,
    SessionVWAPReversionStrategy,
    StrategyRegistry,
)
from pm_event_context import EventCalendar
from pm_regime_audit import audit_regime_profitability


class ProfitabilityReadinessBlueprintTests(unittest.TestCase):
    def test_institutional_supervisor_blocks_only_hard_breakpoints(self):
        cfg = PipelineConfig(max_total_open_risk_pct=3.0, open_drawdown_hard_block_pct=5.0)
        supervisor = InstitutionalSupervisor(cfg)

        decision = supervisor.evaluate(
            SupervisorContext(
                symbol="EURUSD",
                total_open_risk_pct=2.5,
                new_trade_risk_pct=1.0,
                open_drawdown_pct=1.0,
            )
        )

        self.assertFalse(decision.allow_trade)
        self.assertEqual(decision.action, "block_new_entry")
        self.assertTrue(any("total_open_risk" in reason for reason in decision.reasons))

    def test_institutional_supervisor_latches_until_next_cycle(self):
        with tempfile.TemporaryDirectory() as tmp:
            cfg = PipelineConfig(max_total_open_risk_pct=1.0, hard_block_reopen_mode="manual_or_next_cycle")
            supervisor = InstitutionalSupervisor(cfg, state_path=Path(tmp) / "supervisor.json")
            blocked = supervisor.evaluate(
                SupervisorContext(
                    symbol="EURUSD",
                    total_open_risk_pct=1.0,
                    new_trade_risk_pct=1.0,
                    metadata={"cycle_id": "cycle-a"},
                )
            )
            latched = supervisor.evaluate(
                SupervisorContext(symbol="EURUSD", metadata={"cycle_id": "cycle-a"})
            )
            reopened = supervisor.evaluate(
                SupervisorContext(symbol="EURUSD", metadata={"cycle_id": "cycle-b"})
            )
            self.assertFalse(blocked.allow_trade)
            self.assertFalse(latched.allow_trade)
            self.assertTrue(reopened.allow_trade)

    def test_cycle_drift_pauses_after_severe_local_loss_cluster(self):
        with tempfile.TemporaryDirectory() as tmp:
            cfg = PipelineConfig(cycle_drift_min_trades=3, cycle_drift_consecutive_losses=3)
            state_path = Path(tmp) / "cycle.json"
            monitor = CycleDriftMonitor(cfg, state_path=state_path)
            for _ in range(3):
                monitor.record_trade("EURUSD", "H1", "TREND", "Demo", pnl_dollars=-10.0)
            monitor = CycleDriftMonitor(cfg, state_path=state_path)

            decision = monitor.evaluate(CycleDriftContext("EURUSD", "H1", "TREND", "Demo"))

            self.assertFalse(decision.allow_trade)
            self.assertEqual(decision.action, "pause_context")

    def test_realized_loss_pct_uses_recent_closing_deals(self):
        trader = LiveTrader.__new__(LiveTrader)
        trader.mt5 = None
        trader.logger = SimpleNamespace(debug=lambda *_args, **_kwargs: None)
        now = datetime.now()
        trader._live_session_started_at = now - timedelta(hours=2)
        trader._recent_closing_deals = [
            {"time": now - timedelta(hours=1), "profit": -250.0},
            {"time": now - timedelta(hours=3), "profit": -500.0},
        ]

        loss_pct = trader._estimate_realized_loss_pct(
            "session",
            SimpleNamespace(balance=10000.0),
        )

        self.assertAlmostEqual(loss_pct, 2.5, places=6)

    def test_weekly_realized_loss_refetches_beyond_recent_cache(self):
        trader = LiveTrader.__new__(LiveTrader)
        now = datetime.now()
        trader._live_session_started_at = now - timedelta(hours=2)
        trader._recent_closing_deals = [{"time": now - timedelta(hours=2), "profit": 0.0}]
        trader._recent_closing_deals_lookback_hours = 48
        trader.logger = SimpleNamespace(debug=lambda *_args, **_kwargs: None)
        trader.mt5 = Mock()
        trader.mt5.get_recent_closing_deals.return_value = [
            {"time": now - timedelta(days=3), "profit": -500.0},
            {"time": now - timedelta(hours=2), "profit": 0.0},
        ]

        loss_pct = trader._estimate_realized_loss_pct(
            "weekly",
            SimpleNamespace(balance=10000.0),
        )

        self.assertAlmostEqual(loss_pct, 5.0, places=6)
        trader.mt5.get_recent_closing_deals.assert_called_once_with(lookback_hours=24 * 8)

    def test_live_readiness_blocks_non_blocking_artifact_policy(self):
        with tempfile.TemporaryDirectory() as tmp:
            ledger = Path(tmp) / "pm_configs.json"
            ledger.write_text(
                json.dumps(
                    {
                        "EURUSD": {
                            "symbol": "EURUSD",
                            "regime_configs": {
                                "H1": {"TREND": {"strategy_name": "EMACrossoverStrategy"}}
                            },
                        }
                    }
                ),
                encoding="utf-8",
            )
            cfg = PipelineConfig(
                live_artifact_drift_policy="warn",
                winner_ledger_path=str(ledger),
                regime_params_file="",
            )

            report = validate_live_readiness(cfg, symbols=["EURUSD"], config_path=str(Path(tmp) / "config.json"))

            self.assertFalse(report.ok)
            self.assertTrue(any(item.code == "artifact_drift_not_blocking" for item in report.errors))

    def test_live_readiness_rejects_paper_only_winners(self):
        with tempfile.TemporaryDirectory() as tmp:
            ledger = Path(tmp) / "pm_configs.json"
            ledger.write_text(
                json.dumps(
                    {
                        "EURUSD": {
                            "symbol": "EURUSD",
                            "regime_configs": {
                                "H1": {
                                    "TREND": {
                                        "strategy_name": "EMACrossoverStrategy",
                                        "promotion_status": "PAPER_ONLY",
                                    }
                                }
                            },
                        }
                    }
                ),
                encoding="utf-8",
            )
            cfg = PipelineConfig(winner_ledger_path=str(ledger), regime_params_file="")

            report = validate_live_readiness(cfg, symbols=["EURUSD"], config_path=str(Path(tmp) / "config.json"))

            self.assertFalse(report.ok)
            self.assertTrue(any(item.code == "winner_ledger_no_tradeable_winners" for item in report.errors))

    def test_live_readiness_blocks_symbols_missing_from_broker_specs(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "config.json").write_text(json.dumps({"broker_specs_path": "broker_specs.json"}), encoding="utf-8")
            (root / "broker_specs.json").write_text(
                json.dumps(
                    {
                        "EURUSD": {
                            "digits": 5,
                            "point": 0.00001,
                            "tick_size": 0.00001,
                            "tick_value": 1.0,
                            "contract_size": 100000.0,
                            "volume_min": 0.01,
                            "volume_max": 100.0,
                            "volume_step": 0.01,
                            "spread": 20,
                        }
                    }
                ),
                encoding="utf-8",
            )
            ledger = root / "pm_configs.json"
            ledger.write_text(
                json.dumps(
                    {
                        "EURUSD": {"symbol": "EURUSD", "strategy_name": "EMACrossoverStrategy"},
                        "AAPL": {"symbol": "AAPL", "strategy_name": "EMACrossoverStrategy"},
                    }
                ),
                encoding="utf-8",
            )
            cfg = PipelineConfig(
                live_artifact_drift_policy="block",
                winner_ledger_path=str(ledger),
                regime_params_file="",
                walk_forward_audit_enabled=True,
                log_dir=str(root / "logs"),
            )

            report = validate_live_readiness(cfg, symbols=["EURUSD", "AAPL"], config_path=str(root / "config.json"))

            self.assertFalse(report.ok)
            self.assertTrue(any(item.code == "broker_specs_missing_symbols" for item in report.errors))

    def test_live_readiness_blocks_invalid_broker_spec_execution_fields(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "config.json").write_text(json.dumps({"broker_specs_path": "broker_specs.json"}), encoding="utf-8")
            (root / "broker_specs.json").write_text(
                json.dumps(
                    {
                        "EURUSD": {
                            "digits": 5,
                            "point": 0.00001,
                            "tick_size": 0.00001,
                            "tick_value": 0.0,
                            "contract_size": 100000.0,
                            "volume_min": 0.01,
                            "volume_max": 100.0,
                            "volume_step": 0.01,
                            "spread": 20,
                        }
                    }
                ),
                encoding="utf-8",
            )
            ledger = root / "pm_configs.json"
            ledger.write_text(
                json.dumps({"EURUSD": {"symbol": "EURUSD", "strategy_name": "EMACrossoverStrategy"}}),
                encoding="utf-8",
            )
            cfg = PipelineConfig(
                live_artifact_drift_policy="block",
                winner_ledger_path=str(ledger),
                regime_params_file="",
                walk_forward_audit_enabled=True,
                log_dir=str(root / "logs"),
            )

            report = validate_live_readiness(cfg, symbols=["EURUSD"], config_path=str(root / "config.json"))

            self.assertFalse(report.ok)
            self.assertTrue(any(item.code == "broker_specs_invalid_fields" for item in report.errors))

    def test_live_readiness_accepts_broker_symbol_alias_in_specs(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "config.json").write_text(json.dumps({"broker_specs_path": "broker_specs.json"}), encoding="utf-8")
            (root / "broker_specs.json").write_text(
                json.dumps(
                    {
                        "EURUSD": {
                            "symbol": "EURUSD",
                            "broker_symbol": "EURUSD.a",
                            "digits": 5,
                            "point": 0.00001,
                            "tick_size": 0.00001,
                            "tick_value": 1.0,
                            "contract_size": 100000.0,
                            "volume_min": 0.01,
                            "volume_max": 100.0,
                            "volume_step": 0.01,
                            "spread": 20,
                        }
                    }
                ),
                encoding="utf-8",
            )
            ledger = root / "pm_configs.json"
            ledger.write_text(
                json.dumps({"EURUSD.a": {"symbol": "EURUSD.a", "strategy_name": "EMACrossoverStrategy"}}),
                encoding="utf-8",
            )
            cfg = PipelineConfig(
                live_artifact_drift_policy="block",
                winner_ledger_path=str(ledger),
                regime_params_file="",
                walk_forward_audit_enabled=True,
                log_dir=str(root / "logs"),
            )

            report = validate_live_readiness(cfg, symbols=["EURUSD.a"], config_path=str(root / "config.json"))

            self.assertTrue(report.ok)
            self.assertFalse(any(item.code == "broker_specs_missing_symbols" for item in report.errors))

    def test_event_calendar_matches_symbol_currencies_and_minutes(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "events.csv"
            event_time = datetime.now() + timedelta(minutes=45)
            path.write_text(
                "event_time,currency,severity,title\n"
                f"{event_time.isoformat()},USD,high,FOMC\n",
                encoding="utf-8",
            )
            calendar = EventCalendar.from_csv(path)
            events = calendar.nearby_events("EURUSD", at=datetime.now(), window_minutes=60)
            self.assertEqual(len(events), 1)
            self.assertAlmostEqual(events[0].minutes_to_event(datetime.now()), 45, delta=1)

    def test_live_entry_actionable_context_includes_event_calendar(self):
        trader = LiveTrader.__new__(LiveTrader)
        event_time = datetime.now() + timedelta(minutes=30)
        trader.pipeline_config = PipelineConfig(event_calendar_enabled=True, event_calendar_mode="observe")
        trader._event_calendar = EventCalendar()
        trader._event_calendar.events = [
            SimpleNamespace(
                event_time=event_time,
                currency="USD",
                symbol="",
                severity="high",
                title="FOMC",
                metadata={},
                to_dict=lambda at: {
                    "event_time": event_time.isoformat(),
                    "currency": "USD",
                    "symbol": "",
                    "severity": "high",
                    "title": "FOMC",
                    "metadata": {},
                    "minutes_to_event": 30.0,
                },
            )
        ]
        trader.logger = SimpleNamespace(debug=lambda *_args, **_kwargs: None, warning=lambda *_args, **_kwargs: None)
        trader.storage_manager = Mock()
        trader.storage_manager.should_pause_new_entries.return_value = True
        trader._decision_throttle = Mock()
        trader._actionable_log = Mock()

        trader._execute_entry(
            symbol="EURUSD",
            signal=1,
            strategy=Mock(),
            features=pd.DataFrame(),
            spec=SimpleNamespace(),
            magic=1,
            config=SimpleNamespace(symbol="EURUSD"),
            decision_key="event-test",
            bar_time_iso=datetime.now().isoformat(),
            best_candidate={"timeframe": "H1", "regime": "TREND", "strategy_name": "Demo"},
        )

        payload = trader._actionable_log.record.call_args[0][1]
        self.assertEqual(payload["action"], "SKIPPED_STORAGE_PRESSURE")
        self.assertIn("event_calendar", payload["context"])
        self.assertEqual(payload["context"]["event_calendar"]["nearby_events"][0]["title"], "FOMC")

    def test_governance_candidate_surface_includes_blueprint_policies(self):
        names = candidate_policy_names()
        for expected in {
            "stepped_breakeven",
            "chandelier_atr",
            "structure_trail",
            "partial_tp_runner",
            "time_decay_exit",
            "volatility_contraction_exit",
            "regime_flip_reduce",
        }:
            self.assertIn(expected, names)

    def test_stepped_breakeven_locks_profit_at_two_r(self):
        decision = evaluate_policy(
            {"name": "stepped_breakeven", "step2_r": 2.0, "step2_stop_r": 1.0},
            GovernanceContext(
                symbol="EURUSD",
                timeframe="H1",
                regime="TREND",
                direction=1,
                entry_price=1.1000,
                current_stop_loss=1.0990,
                current_take_profit=1.1050,
                initial_stop_loss=1.0990,
                initial_take_profit=1.1050,
                current_price=1.1025,
                current_atr=0.0005,
                highest_since_entry=1.1025,
                lowest_since_entry=1.1000,
                pip_size=0.0001,
            ),
        )
        self.assertIsNotNone(decision.stop_loss)
        self.assertAlmostEqual(decision.stop_loss, 1.1010, places=8)

    def test_regime_config_persists_promotion_status(self):
        cfg = RegimeConfig(
            strategy_name="EMACrossoverStrategy",
            parameters={},
            quality_score=0.5,
            promotion_status="PROMOTE_SMALL",
            promotion_reasons=["holdout caution"],
        )
        restored = RegimeConfig.from_dict(cfg.to_dict())
        self.assertEqual(restored.promotion_status, "PROMOTE_SMALL")
        self.assertEqual(restored.promotion_reasons, ["holdout caution"])

    def test_swap_model_adjusts_trade_and_equity_curve(self):
        cfg = PipelineConfig(swap_model_enabled=True, swap_model_min_holding_hours=0)
        backtester = Backtester(cfg)
        trades = [
            {
                "entry_time": pd.Timestamp("2026-04-06"),
                "exit_time": pd.Timestamp("2026-04-09"),
                "entry_bar": 0,
                "exit_bar": 3,
                "direction": "LONG",
                "position_size": 1.0,
                "pnl_dollars": 100.0,
                "risk_amount": 100.0,
            }
        ]
        spec = InstrumentSpec("EURUSD", pip_position=4, swap_long=-5.0, swap_short=1.0)

        adjusted, final_equity, _, equity_curve = backtester._apply_swap_model(
            trades,
            final_equity=10100.0,
            equity_curve=[10000.0, 10000.0, 10000.0, 10100.0],
            max_drawdown=0.0,
            features=None,
            spec=spec,
            timeframe="D1",
        )

        self.assertEqual(adjusted[0]["swap_rollover_units"], 5)
        self.assertAlmostEqual(adjusted[0]["swap_dollars"], -25.0)
        self.assertAlmostEqual(adjusted[0]["pnl_dollars"], 75.0)
        self.assertAlmostEqual(final_equity, 10075.0)
        self.assertAlmostEqual(equity_curve[-1], 10075.0)

    def test_swap_model_does_not_charge_same_day_without_rollover(self):
        cfg = PipelineConfig(swap_model_enabled=True, swap_model_min_holding_hours=0)
        backtester = Backtester(cfg)
        trade = {
            "entry_time": pd.Timestamp("2026-04-06 00:00:00"),
            "exit_time": pd.Timestamp("2026-04-06 22:00:00"),
            "entry_bar": 0,
            "exit_bar": 22,
        }

        self.assertEqual(backtester._swap_rollover_units(trade, None, timeframe="H1"), 0)

    def test_cycle_drift_persists_execution_degradation_evidence(self):
        with tempfile.TemporaryDirectory() as tmp:
            cfg = PipelineConfig(cycle_drift_mode="observe")
            state_path = Path(tmp) / "cycle.json"
            monitor = CycleDriftMonitor(cfg, state_path=state_path)
            monitor.evaluate(
                CycleDriftContext(
                    "EURUSD",
                    "H1",
                    "TREND",
                    "Demo",
                    execution_slippage_mult=3.0,
                    spread_degradation_mult=2.5,
                )
            )
            reloaded = CycleDriftMonitor(cfg, state_path=state_path)
            snapshot = reloaded.snapshot("EURUSD", "H1", "TREND", "Demo")

            self.assertAlmostEqual(snapshot["max_execution_slippage_mult"], 3.0)
            self.assertAlmostEqual(snapshot["max_spread_degradation_mult"], 2.5)

    def test_cycle_drift_groups_partial_and_final_deals_by_position(self):
        trader = LiveTrader.__new__(LiveTrader)
        trader.mt5 = Mock()
        trader.mt5.get_positions.return_value = []
        trader.mt5.find_broker_symbol.side_effect = lambda symbol: symbol
        trader.logger = SimpleNamespace(debug=lambda *_args, **_kwargs: None)
        trader.pm = Mock()
        trader.drift_monitor = Mock()
        trader.drift_monitor.get_summary.return_value = {}
        trader._cycle_drift_monitor = Mock()
        trader._seen_closing_deals = set()
        trader._seen_cycle_closure_keys = set()
        regime_cfg = SimpleNamespace(strategy_name="Demo", live_observability={}, val_metrics={})
        symbol_cfg = SimpleNamespace(
            has_regime_configs=lambda: False,
            default_config=regime_cfg,
            timeframe="H1",
            val_metrics={},
        )
        now = datetime.now()

        trader._sync_drift_monitor(
            {"EURUSD": symbol_cfg},
            recent_deals=[
                {"ticket": 1, "position_id": 77, "symbol": "EURUSD", "profit": 40.0, "time": now},
                {"ticket": 2, "position_id": 77, "symbol": "EURUSD", "profit": -10.0, "time": now},
            ],
        )

        trader._cycle_drift_monitor.record_trade.assert_called_once()
        kwargs = trader._cycle_drift_monitor.record_trade.call_args.kwargs
        self.assertEqual(kwargs["symbol"], "EURUSD")
        self.assertAlmostEqual(kwargs["pnl_dollars"], 30.0)

    def test_regime_audit_records_transition_pnl(self):
        features = pd.DataFrame(
            {
                "REGIME_LIVE": ["CHOP", "BREAKOUT", "BREAKOUT"],
                "REGIME_GAP": [0.10, 0.04, 0.08],
            }
        )
        report = audit_regime_profitability(
            {"BREAKOUT": {"total_trades": 1, "profit_factor": 1.5, "total_return_pct": 2.0}},
            features=features,
            trades=[{"entry_bar": 1, "pnl_dollars": 25.0}],
            min_trades=1,
            min_profit_factor=1.0,
            low_gap_warn=0.05,
        ).to_dict()

        coverage = report["metrics_by_regime"]["_coverage"]
        self.assertEqual(coverage["pnl_by_regime_transition"]["CHOP->BREAKOUT"]["trades"], 1)
        self.assertAlmostEqual(coverage["pnl_by_regime_transition"]["CHOP->BREAKOUT"]["pnl_dollars"], 25.0)

    def test_regime_transition_breakout_requires_gap_and_source_transition(self):
        idx = pd.date_range("2026-04-01", periods=40, freq="h")
        close = pd.Series([1.10] * 30 + [1.12] + [1.13] * 9, index=idx)
        features = pd.DataFrame(
            {
                "Open": close,
                "High": close + 0.0002,
                "Low": close - 0.0002,
                "Close": close,
                "ADX": [25.0] * 40,
                "REGIME_LIVE": ["CHOP"] * 30 + ["BREAKOUT"] * 10,
                "REGIME_GAP": [0.01] * 31 + [0.08] * 9,
            },
            index=idx,
        )
        strategy = RegimeTransitionBreakoutStrategy(lookback=5, transition_window=3, min_regime_gap=0.05, ema_filter_period=0)
        signals = strategy.generate_signals(features, "EURUSD")
        self.assertEqual(int(signals.iloc[30]), 0)
        self.assertIn(1, set(signals.iloc[31:].tolist()))

    def test_new_profitability_strategies_registered_and_emit_series(self):
        idx = pd.date_range("2026-04-01", periods=80, freq="h")
        close = pd.Series([1.10 + i * 0.0001 for i in range(80)], index=idx)
        features = pd.DataFrame(
            {
                "Open": close,
                "High": close + 0.0005,
                "Low": close - 0.0005,
                "Close": close,
                "Volume": [1000.0] * 80,
                "ATR_14": [0.0006] * 80,
                "ADX": [25.0] * 80,
                "REGIME_LIVE": ["RANGE"] * 30 + ["BREAKOUT"] * 50,
            },
            index=idx,
        )
        for strategy_cls in (
            SessionVWAPReversionStrategy,
            LiquiditySweepReversalStrategy,
            RegimeTransitionBreakoutStrategy,
        ):
            strategy = strategy_cls()
            self.assertIn(strategy.name, StrategyRegistry.list_all())
            signals = strategy.generate_signals(features.copy(), "EURUSD")
            self.assertEqual(len(signals), len(features))
            self.assertTrue(set(signals.unique()).issubset({-1, 0, 1}))

    # ---------- audit fix regressions ----------

    def test_swing_trail_alias_resolves_to_structure_trail(self):
        self.assertEqual(normalize_policy_name("swing_trail"), "structure_trail")

    def test_promote_small_risk_multiplier_is_config_driven_and_clamped(self):
        cfg = PipelineConfig(promote_small_risk_multiplier=0.25)
        self.assertAlmostEqual(cfg.promote_small_risk_multiplier, 0.25)
        # Out-of-range values clamp to [0, 1].
        self.assertAlmostEqual(PipelineConfig(promote_small_risk_multiplier=1.5).promote_small_risk_multiplier, 1.0)
        self.assertAlmostEqual(PipelineConfig(promote_small_risk_multiplier=-1.0).promote_small_risk_multiplier, 0.0)

    def test_live_readiness_blocks_when_supervisor_latched(self):
        with tempfile.TemporaryDirectory() as tmp:
            log_dir = Path(tmp) / "logs"
            log_dir.mkdir()
            (log_dir / "institutional_supervisor_state.json").write_text(
                json.dumps(
                    {
                        "active_block": True,
                        "block_reasons": ["daily_loss 4.2% >= 3.0%"],
                        "cycle_id": "cycle-x",
                    }
                ),
                encoding="utf-8",
            )
            ledger = Path(tmp) / "pm_configs.json"
            ledger.write_text(
                json.dumps(
                    {
                        "EURUSD": {
                            "symbol": "EURUSD",
                            "regime_configs": {
                                "H1": {"TREND": {"strategy_name": "EMACrossoverStrategy"}}
                            },
                        }
                    }
                ),
                encoding="utf-8",
            )
            cfg = PipelineConfig(
                live_artifact_drift_policy="block",
                winner_ledger_path=str(ledger),
                regime_params_file="",
                log_dir=str(log_dir),
                hard_block_reopen_mode="manual",
            )
            report = validate_live_readiness(cfg, symbols=["EURUSD"], config_path=str(Path(tmp) / "config.json"))
            self.assertFalse(report.ok)
            self.assertTrue(any(item.code == "supervisor_latched_from_previous_run" for item in report.errors))

    def test_live_readiness_allows_supervisor_latch_from_previous_cycle(self):
        with tempfile.TemporaryDirectory() as tmp:
            log_dir = Path(tmp) / "logs"
            log_dir.mkdir()
            (log_dir / "institutional_supervisor_state.json").write_text(
                json.dumps(
                    {
                        "active_block": True,
                        "block_reasons": ["daily_loss 4.2% >= 3.0%"],
                        "cycle_id": "2026-04-01T00:00:00",
                    }
                ),
                encoding="utf-8",
            )
            ledger = Path(tmp) / "pm_configs.json"
            ledger.write_text(
                json.dumps(
                    {
                        "EURUSD": {
                            "symbol": "EURUSD",
                            "regime_configs": {
                                "H1": {"TREND": {"strategy_name": "EMACrossoverStrategy"}}
                            },
                        }
                    }
                ),
                encoding="utf-8",
            )
            cfg = PipelineConfig(
                live_artifact_drift_policy="block",
                winner_ledger_path=str(ledger),
                regime_params_file="",
                log_dir=str(log_dir),
                hard_block_reopen_mode="manual_or_next_cycle",
                walk_forward_audit_enabled=True,
            )
            cfg.get_last_retrain_slot = Mock(return_value=datetime(2026, 4, 15))
            report = validate_live_readiness(cfg, symbols=["EURUSD"], config_path=str(Path(tmp) / "config.json"))
            self.assertTrue(report.ok)
            self.assertTrue(any(item.code == "supervisor_latch_from_previous_cycle" for item in report.warnings))

    def test_cycle_drift_record_trade_captures_execution_quality(self):
        with tempfile.TemporaryDirectory() as tmp:
            cfg = PipelineConfig()
            state_path = Path(tmp) / "cycle.json"
            monitor = CycleDriftMonitor(cfg, state_path=state_path)
            monitor.record_trade(
                "EURUSD",
                "H1",
                "TREND",
                "Demo",
                pnl_dollars=-25.0,
                execution_slippage_mult=2.7,
                spread_degradation_mult=2.4,
            )
            reloaded = CycleDriftMonitor(cfg, state_path=state_path)
            snapshot = reloaded.snapshot("EURUSD", "H1", "TREND", "Demo")
            self.assertAlmostEqual(snapshot["max_execution_slippage_mult"], 2.7)
            self.assertAlmostEqual(snapshot["max_spread_degradation_mult"], 2.4)


if __name__ == "__main__":
    unittest.main()
