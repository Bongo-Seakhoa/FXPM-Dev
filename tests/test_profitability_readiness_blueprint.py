import ast
import inspect
import json
import tempfile
import textwrap
import unittest
from dataclasses import fields
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

import pandas as pd

from pm_core import InstrumentSpec, PipelineConfig, Backtester
from pm_cycle_drift import CycleDriftContext, CycleDriftMonitor
from pm_institutional_supervisor import InstitutionalSupervisor, SupervisorContext
from pm_live_readiness import validate_live_readiness
from pm_main import LiveTrader
from pm_order_governance import GovernanceContext, candidate_policy_names, evaluate_policy, normalize_policy_name
from pm_pipeline import PortfolioManager, RegimeConfig, build_artifact_meta
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
        self.assertEqual(decision.action, "block_new_entry_transient")
        self.assertTrue(any("total_open_risk" in reason for reason in decision.reasons))

    def test_institutional_supervisor_uses_selected_capital_total_axis(self):
        cfg = PipelineConfig(
            max_total_open_risk_pct=8.0,
            max_new_entries_when_total_open_risk_above_pct=5.0,
            open_risk_action="pause_new_entries",
        )
        with tempfile.TemporaryDirectory() as tmp:
            supervisor = InstitutionalSupervisor(
                cfg, state_path=Path(tmp) / "supervisor.json"
            )
            admitted = supervisor.evaluate(
                SupervisorContext(
                    symbol="EURUSD",
                    total_open_risk_pct=10.0,
                    new_trade_risk_pct=1.0,
                    max_total_stop_risk_pct=20.0,
                )
            )
            capped = supervisor.evaluate(
                SupervisorContext(
                    symbol="EURUSD",
                    total_open_risk_pct=10.0,
                    new_trade_risk_pct=1.0,
                    max_total_stop_risk_pct=10.0,
                )
            )
            invalid = supervisor.evaluate(
                SupervisorContext(
                    symbol="EURUSD",
                    max_total_stop_risk_pct=float("nan"),
                )
            )

        self.assertTrue(admitted.allow_trade)
        self.assertFalse(capped.allow_trade)
        self.assertEqual(capped.action, "block_new_entry_transient")
        self.assertTrue(any("> 10.00%" in reason for reason in capped.reasons))
        self.assertFalse(invalid.allow_trade)
        self.assertEqual(invalid.action, "block_invalid_capital_envelope")
        self.assertTrue(any("is invalid" in reason for reason in invalid.reasons))

        observe_cfg = PipelineConfig(institutional_supervisor_mode="observe")
        for invalid_value in (float("nan"), float("inf"), 0.0, 100.0, True, "20"):
            with self.subTest(invalid_selected_total=invalid_value):
                with tempfile.TemporaryDirectory() as tmp:
                    invalid_observe = InstitutionalSupervisor(
                        observe_cfg, state_path=Path(tmp) / "supervisor.json"
                    ).evaluate(
                        SupervisorContext(
                            symbol="EURUSD",
                            max_total_stop_risk_pct=invalid_value,
                        )
                    )
                self.assertFalse(invalid_observe.allow_trade)
                self.assertFalse(invalid_observe.observed_only)
                self.assertEqual(
                    invalid_observe.action, "block_invalid_capital_envelope"
                )

        derate_cfg = PipelineConfig(
            max_total_open_risk_pct=8.0,
            max_new_entries_when_total_open_risk_above_pct=5.0,
            open_risk_action="derate",
        )
        with tempfile.TemporaryDirectory() as tmp:
            selected_derate = InstitutionalSupervisor(
                derate_cfg, state_path=Path(tmp) / "supervisor.json"
            ).evaluate(
                SupervisorContext(
                    symbol="EURUSD",
                    total_open_risk_pct=10.0,
                    new_trade_risk_pct=1.0,
                    max_total_stop_risk_pct=20.0,
                )
            )
        self.assertTrue(selected_derate.allow_trade)
        self.assertEqual(selected_derate.action, "allow")
        self.assertEqual(selected_derate.risk_multiplier, 1.0)

    def test_institutional_supervisor_preserves_standalone_legacy_pause(self):
        cfg = PipelineConfig(
            max_total_open_risk_pct=20.0,
            max_new_entries_when_total_open_risk_above_pct=5.0,
            open_risk_action="pause_new_entries",
        )
        with tempfile.TemporaryDirectory() as tmp:
            decision = InstitutionalSupervisor(
                cfg, state_path=Path(tmp) / "supervisor.json"
            ).evaluate(
                SupervisorContext(
                    symbol="EURUSD",
                    total_open_risk_pct=10.0,
                    new_trade_risk_pct=1.0,
                )
            )

        self.assertFalse(decision.allow_trade)
        self.assertEqual(decision.action, "block_new_entry_transient")
        self.assertTrue(
            any("existing_open_risk" in reason for reason in decision.reasons)
        )

    def test_selected_capital_total_axis_does_not_weaken_account_breakers(self):
        cfg = PipelineConfig(
            max_total_open_risk_pct=8.0,
            max_new_entries_when_total_open_risk_above_pct=0.0,
            daily_loss_hard_block_pct=2.0,
        )
        with tempfile.TemporaryDirectory() as tmp:
            decision = InstitutionalSupervisor(
                cfg, state_path=Path(tmp) / "supervisor.json"
            ).evaluate(
                SupervisorContext(
                    symbol="EURUSD",
                    total_open_risk_pct=10.0,
                    new_trade_risk_pct=1.0,
                    max_total_stop_risk_pct=20.0,
                    daily_loss_pct=2.1,
                )
            )

        self.assertFalse(decision.allow_trade)
        self.assertEqual(decision.action, "block_new_entry")
        self.assertTrue(any("daily_loss" in reason for reason in decision.reasons))

    def test_execute_entry_binds_selected_total_axis_at_both_supervisor_checks(self):
        source = textwrap.dedent(inspect.getsource(LiveTrader._execute_entry))
        tree = ast.parse(source)
        supervisor_contexts = [
            node
            for node in ast.walk(tree)
            if isinstance(node, ast.Call)
            and isinstance(node.func, ast.Name)
            and node.func.id == "SupervisorContext"
        ]

        self.assertEqual(len(supervisor_contexts), 2)
        for context_call in supervisor_contexts:
            bound_values = [
                keyword.value
                for keyword in context_call.keywords
                if keyword.arg == "max_total_stop_risk_pct"
            ]
            self.assertEqual(len(bound_values), 1)
            value = bound_values[0]
            self.assertIsInstance(value, ast.Subscript)
            self.assertIsInstance(value.value, ast.Name)
            self.assertEqual(value.value.id, "_cap_env")
            self.assertIsInstance(value.slice, ast.Constant)
            self.assertEqual(value.slice.value, "max_total_stop_risk_pct")

    def test_institutional_supervisor_capacity_block_does_not_latch(self):
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
            self.assertEqual(blocked.action, "block_new_entry_transient")
            self.assertTrue(latched.allow_trade)
            self.assertTrue(reopened.allow_trade)

    def test_account_breaker_latches_until_clear_in_production_mode(self):
        with tempfile.TemporaryDirectory() as tmp:
            cfg = PipelineConfig(
                daily_loss_hard_block_pct=2.0,
                max_new_entries_when_total_open_risk_above_pct=0.0,
                hard_block_reopen_mode="when_clear",
            )
            supervisor = InstitutionalSupervisor(cfg, state_path=Path(tmp) / "supervisor.json")
            blocked = supervisor.evaluate(
                SupervisorContext(symbol="EURUSD", daily_loss_pct=2.1)
            )
            still_blocked = supervisor.evaluate(
                SupervisorContext(symbol="GBPUSD", daily_loss_pct=2.1)
            )
            reopened = supervisor.evaluate(
                SupervisorContext(symbol="GBPUSD", daily_loss_pct=0.0)
            )
            self.assertFalse(blocked.allow_trade)
            self.assertEqual(blocked.action, "block_new_entry")
            self.assertFalse(still_blocked.allow_trade)
            self.assertEqual(still_blocked.action, "block_latched")
            self.assertTrue(reopened.allow_trade)
            self.assertFalse(supervisor._state.get("active_block"))

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

    def test_realized_loss_pct_includes_all_closing_deal_cash_costs(self):
        trader = LiveTrader.__new__(LiveTrader)
        trader.mt5 = None
        trader.logger = SimpleNamespace(
            debug=lambda *_args, **_kwargs: None,
            warning=lambda *_args, **_kwargs: None,
        )
        now = datetime.now()
        trader._live_session_started_at = now - timedelta(hours=2)
        trader._recent_closing_deals = [
            {
                "ticket": 42,
                "time": now - timedelta(hours=1),
                "profit": -100.0,
                "commission": -7.0,
                "swap": -3.0,
                "fee": -2.0,
            }
        ]

        loss_pct = trader._estimate_realized_loss_pct(
            "session",
            SimpleNamespace(balance=10000.0),
        )

        self.assertAlmostEqual(loss_pct, 1.12, places=6)

    def test_realized_loss_pct_distinguishes_history_failure_from_empty_history(self):
        trader = LiveTrader.__new__(LiveTrader)
        trader._recent_closing_deals = []
        trader._recent_closing_deals_lookback_hours = 0
        trader._recent_closing_deals_available = False
        trader._live_session_started_at = datetime.now() - timedelta(hours=1)
        trader.logger = SimpleNamespace(
            debug=lambda *_args, **_kwargs: None,
            warning=lambda *_args, **_kwargs: None,
        )
        trader.mt5 = Mock()
        trader.mt5.get_recent_closing_deals.return_value = None

        unavailable = trader._estimate_realized_loss_pct(
            "daily",
            SimpleNamespace(balance=10000.0),
        )
        self.assertIsNone(unavailable)

        trader.mt5.get_recent_closing_deals.return_value = []
        empty = trader._estimate_realized_loss_pct(
            "daily",
            SimpleNamespace(balance=10000.0),
        )
        self.assertEqual(empty, 0.0)

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

    def test_realized_loss_hard_block_fails_closed_when_deals_unavailable(self):
        trader = LiveTrader.__new__(LiveTrader)
        trader.pipeline_config = PipelineConfig(
            daily_loss_hard_block_pct=1.0,
            session_loss_hard_block_pct=0.0,
            weekly_loss_hard_block_pct=0.0,
        )
        trader._recent_closing_deals = []
        trader._recent_closing_deals_lookback_hours = 0
        trader._live_session_started_at = datetime.now() - timedelta(hours=1)
        trader.logger = SimpleNamespace(debug=lambda *_args, **_kwargs: None, warning=lambda *_args, **_kwargs: None)
        trader.mt5 = Mock()
        trader.mt5.get_recent_closing_deals.side_effect = RuntimeError("broker history unavailable")

        reasons = trader._realized_loss_hard_block_reasons(SimpleNamespace(balance=10000.0))

        self.assertIn("daily_loss unavailable; fail-closed", reasons)

    def test_realized_loss_pct_accepts_timezone_aware_deal_times(self):
        trader = LiveTrader.__new__(LiveTrader)
        trader.mt5 = None
        trader.logger = SimpleNamespace(debug=lambda *_args, **_kwargs: None)
        now = datetime.now(timezone.utc)
        trader._live_session_started_at = (now - timedelta(hours=2)).replace(tzinfo=None)
        trader._recent_closing_deals = [
            {"time": now - timedelta(hours=1), "profit": -125.0},
        ]

        loss_pct = trader._estimate_realized_loss_pct(
            "session",
            SimpleNamespace(balance=10000.0),
        )

        self.assertAlmostEqual(loss_pct, 1.25, places=6)

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

    def test_live_readiness_blocks_winner_ledger_artifact_drift(self):
        with tempfile.TemporaryDirectory() as tmp:
            ledger = Path(tmp) / "pm_configs.json"
            cfg = PipelineConfig(
                live_artifact_drift_policy="block",
                winner_ledger_path=str(ledger),
                regime_params_file="",
                log_dir=Path(tmp) / "logs",
            )
            stale_artifact = build_artifact_meta(cfg)
            stale_artifact.pop("backtest_spread_mode")
            ledger.write_text(
                json.dumps(
                    {
                        "EURUSD": {
                            "symbol": "EURUSD",
                            "strategy_name": "EMACrossoverStrategy",
                            "valid_until": (datetime.now() + timedelta(days=2)).isoformat(),
                            "artifact_meta": stale_artifact,
                            "regime_configs": {
                                "H1": {
                                    "TREND": {
                                        "strategy_name": "EMACrossoverStrategy",
                                        "parameters": {},
                                        "promotion_status": "PROMOTED",
                                    }
                                }
                            },
                        }
                    }
                ),
                encoding="utf-8",
            )

            report = validate_live_readiness(cfg, symbols=["EURUSD"], config_path=str(Path(tmp) / "config.json"))

            self.assertFalse(report.ok)
            self.assertTrue(any(item.code == "winner_ledger_artifact_drift" for item in report.errors))

    def test_live_readiness_blocks_required_full_symbol_ledger_gap(self):
        with tempfile.TemporaryDirectory() as tmp:
            ledger = Path(tmp) / "pm_configs.json"
            cfg = PipelineConfig(
                live_artifact_drift_policy="block",
                winner_ledger_path=str(ledger),
                regime_params_file="",
                production_require_full_symbol_ledger=True,
                production_symbol_pool_target_count=2,
                log_dir=Path(tmp) / "logs",
            )
            ledger.write_text(
                json.dumps(
                    {
                        "EURUSD": {
                            "symbol": "EURUSD",
                            "strategy_name": "EMACrossoverStrategy",
                            "valid_until": (datetime.now() + timedelta(days=7)).isoformat(),
                            "artifact_meta": build_artifact_meta(cfg),
                        }
                    }
                ),
                encoding="utf-8",
            )

            report = validate_live_readiness(
                cfg,
                symbols=["EURUSD", "GBPUSD"],
                config_path=str(Path(tmp) / "config.json"),
            )

            self.assertFalse(report.ok)
            self.assertTrue(any(item.code == "production_symbol_ledger_incomplete" for item in report.errors))

    def test_live_readiness_blocks_optional_symbol_ledger_mode(self):
        with tempfile.TemporaryDirectory() as tmp:
            ledger = Path(tmp) / "pm_configs.json"
            cfg = PipelineConfig(
                live_artifact_drift_policy="block",
                winner_ledger_path=str(ledger),
                regime_params_file="",
                production_require_full_symbol_ledger=False,
                production_symbol_pool_target_count=2,
                log_dir=Path(tmp) / "logs",
            )
            ledger.write_text(
                json.dumps(
                    {
                        "EURUSD": {
                            "symbol": "EURUSD",
                            "strategy_name": "EMACrossoverStrategy",
                            "valid_until": (datetime.now() + timedelta(days=7)).isoformat(),
                            "artifact_meta": build_artifact_meta(cfg),
                        }
                    }
                ),
                encoding="utf-8",
            )

            report = validate_live_readiness(
                cfg,
                symbols=["EURUSD", "GBPUSD"],
                config_path=str(Path(tmp) / "config.json"),
            )

            self.assertFalse(report.ok)
            self.assertTrue(any(item.code == "full_symbol_ledger_required" for item in report.errors))
            self.assertTrue(any(item.code == "symbols_missing_from_ledger" for item in report.warnings))

    def test_live_readiness_blocks_partial_ledger_mode(self):
        with tempfile.TemporaryDirectory() as tmp:
            ledger = Path(tmp) / "pm_configs.json"
            cfg = PipelineConfig(
                live_artifact_drift_policy="block",
                winner_ledger_path=str(ledger),
                regime_params_file="",
                production_require_full_symbol_ledger=True,
                live_allow_partial_symbol_ledger=True,
                production_symbol_pool_target_count=2,
                log_dir=Path(tmp) / "logs",
            )
            ledger.write_text(
                json.dumps(
                    {
                        "EURUSD": {
                            "symbol": "EURUSD",
                            "strategy_name": "EMACrossoverStrategy",
                            "valid_until": (datetime.now() + timedelta(days=7)).isoformat(),
                            "artifact_meta": build_artifact_meta(cfg),
                        }
                    }
                ),
                encoding="utf-8",
            )

            report = validate_live_readiness(
                cfg,
                symbols=["EURUSD", "GBPUSD"],
                config_path=str(Path(tmp) / "config.json"),
            )

            self.assertFalse(report.ok)
            self.assertTrue(any(item.code == "partial_symbol_ledger_not_live" for item in report.errors))
            self.assertTrue(any(item.code == "symbols_missing_from_ledger" for item in report.warnings))
            self.assertFalse(any(item.code == "production_symbol_ledger_incomplete" for item in report.errors))

    def test_live_readiness_blocks_account_below_min_equity(self):
        cfg = PipelineConfig(
            live_artifact_drift_policy="block",
            regime_params_file="",
            live_min_account_equity=20.0,
            live_min_account_equity_currency="USD",
        )
        report = validate_live_readiness(
            cfg,
            symbols=[],
            account_info=SimpleNamespace(equity=19.99, currency="USD"),
        )

        self.assertFalse(report.ok)
        self.assertTrue(any(item.code == "account_equity_below_live_minimum" for item in report.errors))

    def test_live_readiness_blocks_risk_pct_above_hard_cap(self):
        cfg = PipelineConfig(live_artifact_drift_policy="block", regime_params_file="")
        report = validate_live_readiness(
            cfg,
            symbols=[],
            account_info=SimpleNamespace(equity=20.0, currency="USD"),
            position_config=SimpleNamespace(risk_per_trade_pct=0.5, max_risk_pct=0.35),
        )

        self.assertFalse(report.ok)
        self.assertTrue(any(item.code == "risk_pct_exceeds_hard_cap" for item in report.errors))

    def test_live_readiness_blocks_when_min_lot_exceeds_target_risk(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "config.json").write_text(json.dumps({"broker_specs_path": "broker_specs.json"}), encoding="utf-8")
            (root / "broker_specs.json").write_text(
                json.dumps(
                    {
                        "EURUSD": {
                            "digits": 5,
                            "point": 0.00001,
                            "tick_size": 0.0001,
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
                        "EURUSD": {
                            "symbol": "EURUSD",
                            "valid_until": (datetime.now() + timedelta(days=2)).isoformat(),
                            "regime_configs": {
                                "H1": {
                                    "TREND": {
                                        "strategy_name": "EMACrossoverStrategy",
                                        "parameters": {"sl_pips": 50.0},
                                        "promotion_status": "PROMOTED",
                                    }
                                }
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
                walk_forward_audit_enabled=True,
                log_dir=str(root / "logs"),
            )

            report = validate_live_readiness(
                cfg,
                symbols=["EURUSD"],
                config_path=str(root / "config.json"),
                account_info=SimpleNamespace(equity=22.0, currency="USD"),
                position_config=SimpleNamespace(risk_per_trade_pct=0.5, max_risk_pct=25.0),
            )

            self.assertTrue(any(item.code == "min_lot_risk_exceeds_target" for item in report.errors))
            self.assertFalse(any(item.code == "min_lot_risk_exceeds_hard_cap" for item in report.errors))

    def test_live_readiness_rejects_weakened_walk_forward_policy(self):
        with tempfile.TemporaryDirectory() as tmp:
            cfg = PipelineConfig(
                walk_forward_audit_enabled=True,
                walk_forward_promotion_policy="observe_only",
                walk_forward_live_blocking_enabled=False,
                winner_ledger_path=str(Path(tmp) / "missing-ledger.json"),
                regime_params_file="",
                log_dir=str(Path(tmp) / "logs"),
            )

            report = validate_live_readiness(
                cfg,
                symbols=[],
                config_path=str(Path(tmp) / "missing-config.json"),
            )

        codes = {item.code for item in report.errors}
        self.assertIn("walk_forward_promotion_policy_weakened", codes)
        self.assertIn("walk_forward_live_blocking_disabled", codes)

    def test_live_readiness_blocks_when_min_lot_exceeds_hard_cap(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "config.json").write_text(json.dumps({"broker_specs_path": "broker_specs.json"}), encoding="utf-8")
            (root / "broker_specs.json").write_text(
                json.dumps(
                    {
                        "EURUSD": {
                            "digits": 5,
                            "point": 0.00001,
                            "tick_size": 0.0001,
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
                        "EURUSD": {
                            "symbol": "EURUSD",
                            "valid_until": (datetime.now() + timedelta(days=2)).isoformat(),
                            "regime_configs": {
                                "H1": {
                                    "TREND": {
                                        "strategy_name": "EMACrossoverStrategy",
                                        "parameters": {"sl_pips": 50.0},
                                        "promotion_status": "PROMOTED",
                                    }
                                }
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
                walk_forward_audit_enabled=True,
                log_dir=str(root / "logs"),
            )

            report = validate_live_readiness(
                cfg,
                symbols=["EURUSD"],
                config_path=str(root / "config.json"),
                account_info=SimpleNamespace(equity=22.0, currency="USD"),
                position_config=SimpleNamespace(risk_per_trade_pct=0.5, max_risk_pct=1.0),
            )

            self.assertFalse(report.ok)
            self.assertTrue(any(item.code == "min_lot_risk_exceeds_hard_cap" for item in report.errors))

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

    def test_live_readiness_ignores_legacy_top_level_when_nested_candidates_blocked(self):
        with tempfile.TemporaryDirectory() as tmp:
            ledger = Path(tmp) / "pm_configs.json"
            blocked = {
                "strategy_name": "EMACrossoverStrategy",
                "promotion_status": "BLOCKED",
                "promotion_reasons": ["holdout failed"],
            }
            ledger.write_text(
                json.dumps(
                    {
                        "EURUSD": {
                            "symbol": "EURUSD",
                            "strategy_name": "EMACrossoverStrategy",
                            "default_config": blocked,
                            "regime_configs": {"H1": {"TREND": blocked}},
                        }
                    }
                ),
                encoding="utf-8",
            )
            cfg = PipelineConfig(winner_ledger_path=str(ledger), regime_params_file="")

            report = validate_live_readiness(cfg, symbols=["EURUSD"], config_path=str(Path(tmp) / "config.json"))

            self.assertFalse(report.ok)
            self.assertTrue(any(item.code == "winner_ledger_no_tradeable_winners" for item in report.errors))

    def test_data_failure_cannot_mutate_winner_ledger(self):
        with tempfile.TemporaryDirectory() as tmp:
            ledger = Path(tmp) / "pm_configs.json"
            cfg = PipelineConfig(
                data_dir=str(Path(tmp) / "data"),
                timeframes=["M5", "H1"],
                winner_ledger_path=str(ledger),
                regime_params_file="",
            )
            manager = PortfolioManager(cfg, symbols=["EURCHF"], config_file=str(ledger))
            before = ledger.read_bytes() if ledger.exists() else None

            with self.assertRaisesRegex(
                RuntimeError,
                "ACQUISITION_DATA_STATE_WINNER_LEDGER_MUTATION_FORBIDDEN",
            ):
                manager.save_data_no_trade_config(
                    "EURCHF", "Insufficient M5 history after retry"
                )

            after = ledger.read_bytes() if ledger.exists() else None
            self.assertEqual(after, before)

    def test_optimization_failure_no_trade_marker_completes_symbol_ledger(self):
        with tempfile.TemporaryDirectory() as tmp:
            ledger = Path(tmp) / "pm_configs.json"
            cfg = PipelineConfig(
                data_dir=str(Path(tmp) / "data"),
                timeframes=["M5"],
                winner_ledger_path=str(ledger),
                regime_params_file="",
            )
            manager = PortfolioManager(cfg, symbols=["AU200"], config_file=str(ledger))

            manager.save_optimization_no_trade_config("AU200", "No validated strategy config produced")

            payload = json.loads(ledger.read_text(encoding="utf-8"))
            symbol_cfg = payload["AU200"]
            self.assertEqual(symbol_cfg["strategy_name"], "NO_TRADE")
            self.assertTrue(symbol_cfg["artifact_meta"]["optimization_failure"])
            self.assertFalse(symbol_cfg["artifact_meta"].get("data_failure", False))

    def test_live_readiness_blocks_expired_winner_ledger(self):
        with tempfile.TemporaryDirectory() as tmp:
            ledger = Path(tmp) / "pm_configs.json"
            cfg = PipelineConfig(
                winner_ledger_path=str(ledger),
                regime_params_file="",
                log_dir=Path(tmp) / "logs",
            )
            ledger.write_text(
                json.dumps(
                    {
                        "EURUSD": {
                            "symbol": "EURUSD",
                            "strategy_name": "EMACrossoverStrategy",
                            "promotion_status": "PROMOTED",
                            "valid_until": (datetime.now() - timedelta(minutes=1)).isoformat(),
                            "artifact_meta": build_artifact_meta(cfg),
                        }
                    }
                ),
                encoding="utf-8",
            )

            report = validate_live_readiness(cfg, symbols=["EURUSD"], config_path=str(Path(tmp) / "config.json"))

            self.assertFalse(report.ok)
            self.assertTrue(any(item.code == "winner_ledger_not_live_current" for item in report.errors))

    def test_live_readiness_warns_storage_below_advisory_floor(self):
        with tempfile.TemporaryDirectory() as tmp:
            ledger = Path(tmp) / "pm_configs.json"
            cfg = PipelineConfig(
                winner_ledger_path=str(ledger),
                regime_params_file="",
                log_dir=str(Path(tmp) / "logs"),
                institutional_supervisor_enabled=False,
                storage_pause_entries_below_free_gb=999999999.0,
            )
            ledger.write_text(
                json.dumps(
                    {
                        "EURUSD": {
                            "symbol": "EURUSD",
                            "strategy_name": "EMACrossoverStrategy",
                            "valid_until": (datetime.now() + timedelta(days=1)).isoformat(),
                            "artifact_meta": build_artifact_meta(cfg),
                        }
                    }
                ),
                encoding="utf-8",
            )

            report = validate_live_readiness(cfg, symbols=["EURUSD"], config_path=str(Path(tmp) / "config.json"))

            self.assertFalse(report.ok)
            self.assertTrue(any(item.code == "supervisor_disabled" for item in report.errors))
            self.assertTrue(
                any(item.code == "storage_free_space_below_advisory_floor" for item in report.warnings)
            )

    def test_live_readiness_accepts_missing_regime_params_when_artifact_contract_matches(self):
        with tempfile.TemporaryDirectory() as tmp:
            ledger = Path(tmp) / "pm_configs.json"
            cfg = PipelineConfig(
                winner_ledger_path=str(ledger),
                regime_params_file=str(Path(tmp) / "missing_regime_params.json"),
                live_require_tuned_regime_params=False,
                log_dir=Path(tmp) / "logs",
            )
            ledger.write_text(
                json.dumps(
                    {
                        "EURUSD": {
                            "symbol": "EURUSD",
                            "strategy_name": "EMACrossoverStrategy",
                            "valid_until": (datetime.now() + timedelta(days=1)).isoformat(),
                            "artifact_meta": build_artifact_meta(cfg),
                        }
                    }
                ),
                encoding="utf-8",
            )

            report = validate_live_readiness(cfg, symbols=["EURUSD"], config_path=str(Path(tmp) / "config.json"))

            self.assertFalse(report.ok)
            self.assertFalse(any(item.code == "regime_params_missing" for item in report.warnings))

    def test_live_readiness_blocks_promoted_regime_cell_without_explicit_detector_row(self):
        with tempfile.TemporaryDirectory() as tmp:
            ledger = Path(tmp) / "pm_configs.json"
            cfg = PipelineConfig(
                winner_ledger_path=str(ledger),
                regime_params_file=str(Path(tmp) / "missing_regime_params.json"),
                live_require_tuned_regime_params=True,
                log_dir=Path(tmp) / "logs",
            )
            ledger.write_text(
                json.dumps(
                    {
                        "EURUSD": {
                            "symbol": "EURUSD",
                            "valid_until": (datetime.now() + timedelta(days=1)).isoformat(),
                            "artifact_meta": build_artifact_meta(cfg),
                            "regime_configs": {
                                "H1": {
                                    "TREND": {
                                        "strategy_name": "EMACrossoverStrategy",
                                        "parameters": {},
                                        "quality_score": 1.0,
                                        "promotion_status": "PROMOTED",
                                    }
                                }
                            },
                        }
                    }
                ),
                encoding="utf-8",
            )

            report = validate_live_readiness(cfg, symbols=["EURUSD"], config_path=str(Path(tmp) / "config.json"))

            self.assertFalse(report.ok)
            self.assertTrue(any(item.code == "live_tuned_regime_params_missing" for item in report.errors))

    def test_live_readiness_requires_pinned_mt5_identity_when_mt5_config_supplied(self):
        cfg = PipelineConfig(regime_params_file="")

        report = validate_live_readiness(
            cfg,
            symbols=[],
            config_path="config.json",
            mt5_config=SimpleNamespace(expected_login=0, expected_server="", expected_currency=""),
            account_info=None,
        )

        self.assertFalse(report.ok)
        self.assertTrue(any(item.code == "mt5_account_identity_not_pinned" for item in report.errors))

    def test_live_readiness_rejects_each_partial_mt5_identity_pin(self):
        cfg = PipelineConfig(regime_params_file="")
        partial_pins = (
            SimpleNamespace(
                expected_login=0,
                expected_server="Broker-Live",
                expected_currency="USD",
            ),
            SimpleNamespace(
                expected_login=123,
                expected_server="",
                expected_currency="USD",
            ),
            SimpleNamespace(
                expected_login=123,
                expected_server="Broker-Live",
                expected_currency="",
            ),
        )

        for mt5_config in partial_pins:
            with self.subTest(mt5_config=mt5_config):
                report = validate_live_readiness(
                    cfg,
                    symbols=[],
                    config_path="config.json",
                    mt5_config=mt5_config,
                    account_info=SimpleNamespace(
                        login=123,
                        server="Broker-Live",
                        currency="USD",
                    ),
                )
                self.assertTrue(
                    any(
                        item.code == "mt5_account_identity_not_pinned"
                        for item in report.errors
                    )
                )

    def test_live_readiness_blocks_mt5_identity_mismatch(self):
        cfg = PipelineConfig(regime_params_file="")

        report = validate_live_readiness(
            cfg,
            symbols=[],
            config_path="config.json",
            mt5_config=SimpleNamespace(expected_login=123, expected_server="Broker-Live", expected_currency="USD"),
            account_info=SimpleNamespace(login=456, server="Broker-Demo", currency="EUR"),
        )

        self.assertFalse(report.ok)
        codes = {item.code for item in report.errors}
        self.assertIn("mt5_account_login_mismatch", codes)
        self.assertIn("mt5_account_server_mismatch", codes)
        self.assertIn("mt5_account_currency_mismatch", codes)

    def test_live_readiness_fails_closed_on_runtime_promotion_mode_mutation(self):
        cfg = PipelineConfig(regime_params_file="")
        cfg.production_promotion_gate_mode = "ledger_status"

        report = validate_live_readiness(
            cfg,
            symbols=[],
            config_path="config.json",
        )

        self.assertFalse(report.ok)
        self.assertTrue(
            any(
                item.code == "production_promotion_gate_not_immutable_exact"
                for item in report.errors
            )
        )

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
            cfg = PipelineConfig(
                live_artifact_drift_policy="block",
                winner_ledger_path=str(ledger),
                regime_params_file="",
                walk_forward_audit_enabled=True,
                log_dir=str(root / "logs"),
            )
            ledger.write_text(
                json.dumps(
                    {
                        "EURUSD.a": {
                            "symbol": "EURUSD.a",
                            "strategy_name": "EMACrossoverStrategy",
                            "valid_until": (datetime.now() + timedelta(days=1)).isoformat(),
                            "artifact_meta": build_artifact_meta(cfg),
                        }
                    }
                ),
                encoding="utf-8",
            )

            report = validate_live_readiness(cfg, symbols=["EURUSD.a"], config_path=str(root / "config.json"))

            self.assertFalse(report.ok)
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

    def test_storage_pressure_is_advisory_and_does_not_skip_live_entry(self):
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
        trader.logger = Mock()
        trader.storage_manager = Mock()
        trader.storage_manager.should_pause_new_entries.return_value = True
        trader._decision_throttle = Mock()
        trader._actionable_log = Mock()
        trader._manual_new_entries_block_reason = Mock(return_value="")
        trader._ambiguous_order_block_until = {}
        trader._last_order_times = {}
        trader.mt5 = Mock()
        trader.mt5.get_position_by_symbol_magic.return_value = None
        trader.mt5.get_positions.return_value = []
        trader.mt5.get_symbol_info.return_value = None
        strategy = Mock()
        strategy.build_trade_intent.return_value = SimpleNamespace(
            stop_loss_pips=10.0,
            take_profit_pips=20.0,
            governance_hint={},
        )

        trader._execute_entry(
            symbol="EURUSD",
            signal=1,
            strategy=strategy,
            features=pd.DataFrame(),
            spec=SimpleNamespace(),
            magic=1,
            config=SimpleNamespace(symbol="EURUSD"),
            decision_key="event-test",
            bar_time_iso=datetime.now().isoformat(),
            best_candidate={"timeframe": "H1", "regime": "TREND", "strategy_name": "Demo"},
        )

        payload = trader._actionable_log.record.call_args[0][1]
        self.assertEqual(payload["action"], "SKIPPED_NO_SYMBOL_INFO")
        self.assertIn("event_calendar", payload["context"])
        self.assertEqual(payload["context"]["event_calendar"]["nearby_events"][0]["title"], "FOMC")
        self.assertTrue(
            any(
                "Storage pressure advisory active; continuing" in str(call.args[0])
                for call in trader.logger.warning.call_args_list
                if call.args
            )
        )

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
        trader.logger = SimpleNamespace(
            debug=lambda *_args, **_kwargs: None,
            info=lambda *_args, **_kwargs: None,
            warning=lambda *_args, **_kwargs: None,
            error=lambda *_args, **_kwargs: None,
        )
        trader.pm = Mock()
        # _record_runtime_observability merges pm.update_live_observability's
        # result and returns dict(merged); an empty dict keeps that a real dict.
        trader.pm.update_live_observability.return_value = {}
        trader.drift_monitor = Mock()
        trader.drift_monitor.get_summary.return_value = {}
        trader._cycle_drift_monitor = Mock()
        # _sync_drift_monitor now routes runtime state through
        # _record_runtime_observability; the double needs the observability
        # store it reads/writes. An empty get() is the clean "no existing
        # badge" state so consecutive-loss accounting starts from zero.
        trader._runtime_observability = Mock()
        trader._runtime_observability.get.return_value = {}
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

    def test_live_readiness_fails_closed_when_supervisor_state_check_raises(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            log_dir = root / "logs"
            log_dir.mkdir()
            (log_dir / "institutional_supervisor_state.json").write_text(
                "{}", encoding="utf-8"
            )
            cfg = PipelineConfig(log_dir=str(log_dir))
            def _state_only_failure(path, *_args, **_kwargs):
                if Path(path).name == "institutional_supervisor_state.json":
                    raise RuntimeError("state read failed")
                return None
            with patch(
                "pm_live_readiness._load_bounded_json_file",
                side_effect=_state_only_failure,
            ):
                report = validate_live_readiness(
                    cfg,
                    symbols=["EURUSD"],
                    config_path=str(root / "config.json"),
                )
            self.assertTrue(
                any(
                    item.code == "supervisor_state_check_failed"
                    for item in report.errors
                )
            )

    def test_live_readiness_defers_aligned_cap_selection_to_capital_authority(self):
        with tempfile.TemporaryDirectory() as tmp:
            cfg = PipelineConfig(
                max_total_open_risk_pct=90.0,
                max_factor_group_open_risk_pct=50.0,
            )
            report = validate_live_readiness(
                cfg,
                symbols=["EURUSD"],
                config_path=str(Path(tmp) / "config.json"),
            )
            self.assertFalse(
                any(
                    item.code == "total_stop_risk_cap_contract_mismatch"
                    for item in report.errors
                )
            )
            self.assertTrue(
                any(
                    item.code == "funded_sparse_authority_unproven"
                    for item in report.errors
                )
            )

            nominally_insolvent = PipelineConfig(
                max_total_open_risk_pct=100.0,
                max_factor_group_open_risk_pct=50.0,
            )
            rejected = validate_live_readiness(
                nominally_insolvent,
                symbols=["EURUSD"],
                config_path=str(Path(tmp) / "config.json"),
            )
            self.assertTrue(
                any(
                    item.code == "total_stop_risk_cap_contract_mismatch"
                    for item in rejected.errors
                )
            )

    def test_live_readiness_rejects_non_enforcing_supervisor_modes(self):
        funded_breakers = {
            "session_loss_hard_block_pct": 1.5,
            "daily_loss_hard_block_pct": 2.0,
            "weekly_loss_hard_block_pct": 5.0,
            "open_drawdown_hard_block_pct": 3.0,
        }
        with tempfile.TemporaryDirectory() as tmp:
            for mode in ("off", "observe"):
                with self.subTest(mode=mode):
                    cfg = PipelineConfig(
                        institutional_supervisor_mode=mode,
                        portfolio_control_policy="hard_breakpoints_only",
                        **funded_breakers,
                    )
                    report = validate_live_readiness(
                        cfg,
                        symbols=["EURUSD"],
                        config_path=str(Path(tmp) / "config.json"),
                    )
                    self.assertTrue(
                        any(
                            item.code == "supervisor_mode_not_circuit_breaker"
                            for item in report.errors
                        )
                    )

    def test_live_readiness_rejects_disabled_breakpoint_policy(self):
        with tempfile.TemporaryDirectory() as tmp:
            cfg = PipelineConfig(
                institutional_supervisor_mode="circuit_breaker",
                portfolio_control_policy="off",
                session_loss_hard_block_pct=1.5,
                daily_loss_hard_block_pct=2.0,
                weekly_loss_hard_block_pct=5.0,
                open_drawdown_hard_block_pct=3.0,
            )
            report = validate_live_readiness(
                cfg,
                symbols=["EURUSD"],
                config_path=str(Path(tmp) / "config.json"),
            )
            self.assertTrue(
                any(
                    item.code == "portfolio_control_policy_not_hard_breakpoints"
                    for item in report.errors
                )
            )

    def test_live_readiness_rejects_each_wrong_breaker_threshold(self):
        funded_breakers = {
            "session_loss_hard_block_pct": 1.5,
            "daily_loss_hard_block_pct": 2.0,
            "weekly_loss_hard_block_pct": 5.0,
            "open_drawdown_hard_block_pct": 3.0,
        }
        with tempfile.TemporaryDirectory() as tmp:
            for field_name, expected in funded_breakers.items():
                with self.subTest(field_name=field_name):
                    thresholds = dict(funded_breakers)
                    thresholds[field_name] = expected + 0.25
                    cfg = PipelineConfig(
                        institutional_supervisor_mode="circuit_breaker",
                        portfolio_control_policy="hard_breakpoints_only",
                        **thresholds,
                    )
                    report = validate_live_readiness(
                        cfg,
                        symbols=["EURUSD"],
                        config_path=str(Path(tmp) / "config.json"),
                    )
                    self.assertTrue(
                        any(
                            item.code == "account_breaker_threshold_contract_mismatch"
                            for item in report.errors
                        )
                    )

    def test_shipped_config_passes_funded_breaker_contract(self):
        repo_root = Path(__file__).resolve().parents[1]
        payload = json.loads((repo_root / "config.json").read_text(encoding="utf-8"))
        # Production filters config.json to PipelineConfig dataclass fields
        # (pm_main.py:250-257); coordinator-only raw keys such as
        # production_capital_authority are read from the raw dict, not the
        # dataclass. Mirror that filtering so the shipped-config contract is
        # validated exactly as production loads it.
        allowed = {f.name for f in fields(PipelineConfig)}
        cfg = PipelineConfig(
            **{k: v for k, v in payload["pipeline"].items() if k in allowed}
        )

        report = validate_live_readiness(
            cfg,
            symbols=payload["symbols"],
            config_path=str(repo_root / "config.json"),
        )

        breaker_contract_codes = {
            "supervisor_disabled",
            "supervisor_mode_not_circuit_breaker",
            "portfolio_control_policy_not_hard_breakpoints",
            "account_breaker_threshold_contract_mismatch",
            "automatic_safety_reopen_not_live",
        }
        self.assertFalse(
            breaker_contract_codes.intersection(item.code for item in report.errors)
        )

    def test_live_readiness_blocks_automatic_supervisor_reopen_even_on_new_cycle(self):
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
                live_require_tuned_regime_params=False,
                log_dir=str(log_dir),
                hard_block_reopen_mode="manual_or_next_cycle",
                walk_forward_audit_enabled=True,
            )
            cfg.get_last_retrain_slot = Mock(return_value=datetime(2026, 4, 15))
            ledger.write_text(
                json.dumps(
                    {
                        "EURUSD": {
                            "symbol": "EURUSD",
                            "valid_until": (datetime.now() + timedelta(days=1)).isoformat(),
                            "artifact_meta": build_artifact_meta(cfg),
                            "regime_configs": {
                                "H1": {"TREND": {"strategy_name": "EMACrossoverStrategy"}}
                            },
                        }
                    }
                ),
                encoding="utf-8",
            )
            report = validate_live_readiness(cfg, symbols=["EURUSD"], config_path=str(Path(tmp) / "config.json"))
            self.assertFalse(report.ok)
            self.assertTrue(any(item.code == "automatic_safety_reopen_not_live" for item in report.errors))
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
