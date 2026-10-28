"""Test position sizing edge cases: volume=0, min_lot, max_lot boundaries.

Also covers the lot-normalization risk-drift classifier (findings.html §6/§8.1):
INFO-when-within-tolerance / WARN-when-above / BLOCK-when-over-hard-cap.
"""
import logging
import unittest
from types import SimpleNamespace
from unittest.mock import MagicMock

import pandas as pd

from pm_core import InstrumentSpec, PipelineConfig
from pm_main import LiveTrader, classify_lot_normalization_drift
from pm_position import PositionCalculator, PositionConfig
from pm_risk_admission import StopRiskExposure
from tests.test_config_source_of_truth import _execute_with_frozen_spread_fixture


class PositionSizingEdgeCaseTests(unittest.TestCase):

    def setUp(self):
        self.config = PositionConfig(risk_per_trade_pct=1.0)
        self.calc = PositionCalculator(self.config)

    def test_volume_below_min_lot_returns_zero(self):
        """When risk is too small for even min lot, volume should be 0."""
        spec = InstrumentSpec(
            symbol="EURUSD", pip_position=4, pip_value=10.0, min_lot=0.01,
            max_lot=100.0, volume_step=0.01,
        )
        # Very small equity + very large SL → volume rounds to 0
        result = self.calc.calculate_position_size(
            equity=10.0, sl_pips=500.0, spec=spec
        )
        self.assertEqual(result.volume, 0.0)

    def test_normal_sizing_returns_positive(self):
        """Standard sizing should return a positive volume."""
        spec = InstrumentSpec(
            symbol="EURUSD", pip_position=4, pip_value=10.0, min_lot=0.01,
            max_lot=100.0, volume_step=0.01,
        )
        result = self.calc.calculate_position_size(
            equity=10000.0, sl_pips=20.0, spec=spec
        )
        self.assertGreater(result.volume, 0.0)

    def test_optional_min_lot_clamp_rejects_risk_above_target_tolerance(self):
        """A loose hard cap must not authorize a target-risk overshoot."""
        calc = PositionCalculator(
            PositionConfig(
                risk_per_trade_pct=1.0,
                max_risk_pct=3.0,
                allow_min_lot_risk_clamp=True,
            )
        )
        spec = InstrumentSpec(
            symbol="EURUSD", pip_position=4, pip_value=10.0, min_lot=0.01,
            max_lot=100.0, volume_step=0.01,
        )
        result = calc.calculate_position_size(
            equity=1000.0, sl_pips=200.0, spec=spec
        )
        self.assertEqual(result.volume, 0.0)

    def test_optional_min_lot_clamp_allows_volume_inside_target_tolerance(self):
        calc = PositionCalculator(
            PositionConfig(
                risk_per_trade_pct=1.0,
                max_risk_pct=25.0,
                risk_tolerance_pct=2.0,
                allow_min_lot_risk_clamp=True,
            )
        )
        spec = InstrumentSpec(
            symbol="EURUSD", pip_position=4, pip_value=10.0, min_lot=0.01,
            max_lot=100.0, volume_step=0.01,
        )
        result = calc.calculate_position_size(equity=1000.0, sl_pips=101.0, spec=spec)
        self.assertEqual(result.volume, 0.01)

    def test_min_lot_clamp_rejects_historical_live_overshoots(self):
        calc = PositionCalculator(
            PositionConfig(
                risk_per_trade_pct=0.5,
                max_risk_pct=25.0,
                risk_tolerance_pct=2.0,
                allow_min_lot_risk_clamp=True,
            )
        )
        spec = InstrumentSpec(
            symbol="EURUSD", pip_position=4, pip_value=10.0, min_lot=0.01,
            max_lot=100.0, volume_step=0.01,
        )
        for sl_pips in (350.0, 2467.0):
            with self.subTest(sl_pips=sl_pips):
                self.assertEqual(
                    calc.calculate_position_size(1000.0, sl_pips, spec).volume,
                    0.0,
                )

    def test_max_lot_respected(self):
        """Volume should never exceed max_lot."""
        spec = InstrumentSpec(
            symbol="EURUSD", pip_position=4, pip_value=10.0, min_lot=0.01,
            max_lot=0.5, volume_step=0.01,
        )
        result = self.calc.calculate_position_size(
            equity=1_000_000.0, sl_pips=5.0, spec=spec
        )
        self.assertLessEqual(result.volume, 0.5)

    def test_below_min_lot_emits_single_warning(self):
        """D6 — only the live early-return branch may log the min_lot skip.

        The dead duplicate `if volume < spec.min_lot` block at pm_position.py
        was removed; if it ever returns, two identical WARN lines would fire
        for one trade. Lock that to exactly one.
        """
        spec = InstrumentSpec(
            symbol="EURUSD", pip_position=4, pip_value=10.0, min_lot=0.01,
            max_lot=100.0, volume_step=0.01,
        )
        with self.assertLogs("pm_position", level=logging.WARNING) as captured:
            result = self.calc.calculate_position_size(
                equity=10.0, sl_pips=500.0, spec=spec
            )
        self.assertEqual(result.volume, 0.0)
        skip_lines = [
            r for r in captured.records
            if "below min_lot" in r.getMessage() and "skipping trade" in r.getMessage()
        ]
        self.assertEqual(
            len(skip_lines), 1,
            f"Expected exactly one min_lot skip WARN, got {len(skip_lines)}: "
            f"{[r.getMessage() for r in skip_lines]}",
        )


class LiveOrderRiskEnvelopeTests(unittest.TestCase):
    def _trader(self, observed_risk_pct):
        trader = LiveTrader.__new__(LiveTrader)
        trader.mt5 = MagicMock()
        trader._decision_throttle = MagicMock()
        trader._actionable_log = MagicMock()
        trader._last_order_times = {}
        trader.position_config = SimpleNamespace(
            risk_per_trade_pct=0.5,
            max_risk_pct=25.0,
            risk_tolerance_pct=2.0,
            min_position_size=0.01,
            max_position_size=0.0,
            risk_basis="equity",
            auto_widen_sl=True,
        )
        trader.pipeline_config = SimpleNamespace(
            min_trade_risk_pct=0.01,
            d1_secondary_risk_multiplier=1.0,
            secondary_trade_max_risk_pct=0.5,
            max_combined_risk_pct=25.0,
            margin_entry_block_level=100.0,
            target_annual_vol=0.10,
        )
        trader._funded_capital_envelope = {
            "max_total_stop_risk_pct": 8.0,
            "max_factor_stop_risk_pct": 2.0,
            "max_same_symbol_stop_risk_pct": 1.0,
            "max_margin_utilization_pct": 50.0,
            "min_margin_level_pct": 200.0,
            "min_free_margin_pct": 50.0,
            "gap_stress_multiple": 1.5,
        }
        trader.position_calc = MagicMock()
        trader.position_calc.calculate_stop_prices.return_value = (1.1900, 1.2200)
        trader.enable_trading = True
        trader.logger = MagicMock()
        trader._enhancement_seams = SimpleNamespace(
            risk_scalar_stack=SimpleNamespace(overlays=[]),
            execution_quality_overlay=SimpleNamespace(enabled=False),
        )
        trader._equity_peak = 0.0
        trader._check_symbol_combined_risk_cap = MagicMock(return_value=(True, "ok"))
        trader._log_trade = MagicMock()
        trader._estimate_total_open_risk_pct = MagicMock(return_value=0.0)
        trader._estimate_realized_loss_pct = MagicMock(return_value=None)
        trader._estimate_open_drawdown_pct = MagicMock(return_value=0.0)
        allow = SimpleNamespace(
            reasons=[], allow_trade=True, risk_multiplier=1.0,
            observed_only=False, action="allow",
        )
        trader._cycle_drift_monitor = MagicMock()
        trader._cycle_drift_monitor.evaluate.return_value = allow
        trader._institutional_supervisor = MagicMock()
        trader._institutional_supervisor.evaluate.return_value = allow
        trader.mt5.get_position_by_symbol_magic.return_value = None
        trader.mt5.get_positions.return_value = []
        trader.mt5.get_symbol_info.return_value = SimpleNamespace(
            visible=True,
            trade_mode=2,
            volume_min=0.01,
            volume_max=100.0,
            trade_stops_level=0,
            point=0.01,
            trade_tick_size=0.0,
            trade_tick_value=0.0,
        )
        trader.mt5.get_symbol_tick.return_value = SimpleNamespace(ask=1.2000, bid=1.1998)
        trader.mt5.normalize_volume.side_effect = lambda volume, _info: volume
        loss_per_lot = observed_risk_pct / 100.0 * 10000.0 / 0.01
        trader.mt5.calc_loss_amount.side_effect = (
            lambda _typ, _sym, volume, _entry, _stop: loss_per_lot * float(volume)
        )
        trader.mt5.calc_margin_required.return_value = None
        trader.mt5.send_market_order = MagicMock()
        return trader

    def test_observed_overshoots_never_reach_order_send(self):
        features = pd.DataFrame(
            {
                "Open": [1.0, 1.0],
                "High": [1.0, 1.0],
                "Low": [1.0, 1.0],
                "Close": [1.0, 1.0],
            }
        )
        for actual in (3.5, 24.67):
            with self.subTest(actual=actual):
                trader = self._trader(actual)
                _execute_with_frozen_spread_fixture(trader,
                    symbol="XAUUSD",
                    broker_symbol="XAUUSD",
                    signal=1,
                    strategy=MagicMock(),
                    features=features,
                    spec=SimpleNamespace(
                        pip_size=0.01, pip_value=1.0, spread_avg=3.0, point=0.01,
                    ),
                    magic=12345,
                    config=SimpleNamespace(symbol="XAUUSD"),
                    decision_key=f"overshoot-{actual}",
                    bar_time_iso="2026-07-10T12:00:00",
                    best_candidate={
                        "timeframe": "M15",
                        "regime": "CHOP",
                        "strategy_name": "MomentumBurstStrategy",
                        "signal": 1,
                    },
                    trade_intent=SimpleNamespace(
                        stop_loss_pips=10.0, take_profit_pips=20.0,
                    ),
                    positions_snapshot=[],
                    account_info=SimpleNamespace(
                        balance=10000.0,
                        equity=10000.0,
                        margin_level=0.0,
                        margin_free=10000.0,
                    ),
                )
                trader.mt5.send_market_order.assert_not_called()

    def test_final_shared_factor_gate_blocks_order_send(self):
        trader = self._trader(0.25)
        trader.pipeline_config = PipelineConfig(
            manual_new_entries_enabled=True,
            production_require_promotion_gate=False,
            max_total_open_risk_pct=8.0,
            max_factor_group_open_risk_pct=2.0,
            max_new_entries_when_total_open_risk_above_pct=0.0,
            open_risk_action="observe",
            margin_entry_block_level=200.0,
            margin_recovery_start_level=150.0,
            margin_panic_level=120.0,
            margin_reopen_level=300.0,
        )
        trader._fresh_cell_authorization_failure = MagicMock(return_value=None)
        fresh_account = SimpleNamespace(
            balance=10_000.0,
            equity=10_000.0,
            margin=0.0,
            margin_level=0.0,
            margin_free=10_000.0,
            trade_allowed=True,
            trade_expert=True,
            currency="USD",
        )
        trader._fresh_entry_account_validation = MagicMock(
            return_value=(fresh_account, None)
        )
        trader._build_live_stop_risk_exposures = MagicMock(
            return_value=(
                [StopRiskExposure("EURUSD", 1.75, "fx", "existing")],
                10_000.0,
                "",
            )
        )
        trader.mt5.calc_margin_required.return_value = 100.0
        trader.mt5.get_account_info.return_value = fresh_account
        features = pd.DataFrame(
            {
                "Open": [1.0, 1.0],
                "High": [1.0, 1.0],
                "Low": [1.0, 1.0],
                "Close": [1.0, 1.0],
            }
        )
        _execute_with_frozen_spread_fixture(trader,
            symbol="GBPUSD",
            broker_symbol="GBPUSD",
            signal=1,
            strategy=MagicMock(),
            features=features,
            spec=SimpleNamespace(
                pip_size=0.01,
                pip_value=1.0,
                spread_avg=3.0,
                point=0.01,
                asset_class="fx",
            ),
            magic=12345,
            config=SimpleNamespace(symbol="GBPUSD"),
            decision_key="factor-cap",
            bar_time_iso="2026-07-11T12:00:00",
            best_candidate={
                "timeframe": "M15",
                "regime": "TREND",
                "strategy_name": "MomentumBurstStrategy",
                "signal": 1,
            },
            trade_intent=SimpleNamespace(
                stop_loss_pips=10.0,
                take_profit_pips=20.0,
            ),
            positions_snapshot=[],
            account_info=fresh_account,
        )
        trader.mt5.send_market_order.assert_not_called()
        actions = [call.args[1]["action"] for call in trader._actionable_log.record.call_args_list]
        self.assertIn("BLOCKED_GROSS_STOP_RISK_ADMISSION", actions)

        # With factor capacity restored, the distinct projected-margin gate
        # must independently stop a candidate that would exceed 50% use.
        trader._actionable_log.reset_mock()
        trader._build_live_stop_risk_exposures.return_value = ([], 10_000.0, "")
        fresh_account.margin = 4_900.0
        fresh_account.margin_level = fresh_account.equity / fresh_account.margin * 100.0
        fresh_account.margin_free = 5_100.0
        trader.mt5.calc_margin_required.return_value = 200.0
        _execute_with_frozen_spread_fixture(trader,
            symbol="GBPUSD",
            broker_symbol="GBPUSD",
            signal=1,
            strategy=MagicMock(),
            features=features,
            spec=SimpleNamespace(
                pip_size=0.01,
                pip_value=1.0,
                spread_avg=3.0,
                point=0.01,
                asset_class="fx",
            ),
            magic=12345,
            config=SimpleNamespace(symbol="GBPUSD"),
            decision_key="margin-cap",
            bar_time_iso="2026-07-11T12:05:00",
            best_candidate={
                "timeframe": "M15",
                "regime": "TREND",
                "strategy_name": "MomentumBurstStrategy",
                "signal": 1,
            },
            trade_intent=SimpleNamespace(
                stop_loss_pips=10.0,
                take_profit_pips=20.0,
            ),
            positions_snapshot=[],
            account_info=fresh_account,
        )
        trader.mt5.send_market_order.assert_not_called()
        actions = [call.args[1]["action"] for call in trader._actionable_log.record.call_args_list]
        self.assertIn(
            "BLOCKED_PROJECTED_MARGIN_ADMISSION",
            actions,
            [call.args[1] for call in trader._actionable_log.record.call_args_list],
        )

        # Replay sizes new risk from min(MTM equity, balance - open stop loss).
        # A lower reconstructable capital snapshot must therefore reduce the
        # final broker volume before the order/margin decision, not merely
        # relabel the original equity-sized percentage.
        trader._actionable_log.reset_mock()
        trader.mt5.calc_margin_required.reset_mock()
        trader._build_live_stop_risk_exposures.return_value = ([], 5_000.0, "")
        fresh_account.margin = 0.0
        fresh_account.margin_level = 0.0
        fresh_account.margin_free = 10_000.0
        trader.mt5.calc_margin_required.return_value = 100.0
        final_gate_order = []
        trader._promotion_authorization_block_reason = MagicMock(
            side_effect=lambda *_args, **_kwargs: (
                final_gate_order.append("artifact_authorization") or None
            )
        )
        trader._fresh_entry_account_validation = MagicMock(
            side_effect=lambda _baseline: (
                final_gate_order.append("fresh_broker_snapshot")
                or (fresh_account, None)
            )
        )
        trader.mt5.send_market_order.return_value = SimpleNamespace(
            success=False,
            retcode=10013,
            retcode_description="test terminal rejection after sizing",
            error_message="",
        )
        _execute_with_frozen_spread_fixture(trader,
            symbol="GBPUSD",
            broker_symbol="GBPUSD",
            signal=1,
            strategy=MagicMock(),
            features=features,
            spec=SimpleNamespace(
                pip_size=0.01,
                pip_value=1.0,
                spread_avg=3.0,
                point=0.01,
                asset_class="fx",
            ),
            magic=12345,
            config=SimpleNamespace(symbol="GBPUSD"),
            decision_key="conservative-sizing",
            bar_time_iso="2026-07-11T12:10:00",
            best_candidate={
                "timeframe": "M15",
                "regime": "TREND",
                "strategy_name": "MomentumBurstStrategy",
                "signal": 1,
            },
            trade_intent=SimpleNamespace(
                stop_loss_pips=10.0,
                take_profit_pips=20.0,
            ),
            positions_snapshot=[],
            account_info=fresh_account,
        )
        self.assertAlmostEqual(
            trader.mt5.calc_margin_required.call_args.args[2],
            0.01,
            places=8,
        )
        self.assertEqual(
            final_gate_order[:2],
            ["artifact_authorization", "fresh_broker_snapshot"],
        )

    def test_conservative_snapshot_none_volume_blocks_without_type_error(self):
        trader = self._trader(0.25)
        trader.pipeline_config = PipelineConfig(
            manual_new_entries_enabled=True,
            max_total_open_risk_pct=8.0,
            max_factor_group_open_risk_pct=2.0,
            max_new_entries_when_total_open_risk_above_pct=0.0,
            open_risk_action="observe",
            margin_entry_block_level=200.0,
            margin_recovery_start_level=150.0,
            margin_panic_level=120.0,
            margin_reopen_level=300.0,
        )
        trader._fresh_cell_authorization_failure = MagicMock(return_value=None)
        trader._promotion_authorization_block_reason = MagicMock(return_value=None)
        fresh_account = SimpleNamespace(
            balance=10_000.0,
            equity=10_000.0,
            margin=0.0,
            margin_level=0.0,
            margin_free=10_000.0,
            trade_allowed=True,
            trade_expert=True,
            currency="USD",
        )
        trader._fresh_entry_account_validation = MagicMock(
            return_value=(fresh_account, None)
        )
        trader._build_live_stop_risk_exposures = MagicMock(
            return_value=([], 5_000.0, "")
        )
        trader.mt5.get_account_info.return_value = fresh_account
        trader.mt5.calc_margin_required.return_value = 100.0
        normalization_calls = []

        def _normalize(volume, _symbol_info):
            normalization_calls.append(volume)
            return volume if len(normalization_calls) == 1 else None

        trader.mt5.normalize_volume.side_effect = _normalize
        features = pd.DataFrame(
            {
                "Open": [1.0, 1.0],
                "High": [1.0, 1.0],
                "Low": [1.0, 1.0],
                "Close": [1.0, 1.0],
            }
        )

        _execute_with_frozen_spread_fixture(trader,
            symbol="GBPUSD",
            broker_symbol="GBPUSD",
            signal=1,
            strategy=MagicMock(),
            features=features,
            spec=SimpleNamespace(
                pip_size=0.01,
                pip_value=1.0,
                spread_avg=3.0,
                point=0.01,
                asset_class="fx",
            ),
            magic=12345,
            config=SimpleNamespace(symbol="GBPUSD"),
            decision_key="conservative-normalize-none",
            bar_time_iso="2026-07-13T01:00:00",
            best_candidate={
                "timeframe": "M15",
                "regime": "TREND",
                "strategy_name": "MomentumBurstStrategy",
                "signal": 1,
            },
            trade_intent=SimpleNamespace(
                stop_loss_pips=10.0,
                take_profit_pips=20.0,
            ),
            positions_snapshot=[],
            account_info=fresh_account,
        )

        self.assertEqual(len(normalization_calls), 2)
        self.assertAlmostEqual(normalization_calls[0], 0.02, places=8)
        self.assertAlmostEqual(normalization_calls[1], 0.01, places=8)
        trader.mt5.send_market_order.assert_not_called()
        actionable_payload = trader._actionable_log.record.call_args.args[1]
        self.assertEqual(
            actionable_payload["action"],
            "BLOCKED_MIN_LOT_EXCEEDS_CAP",
        )
        self.assertIsNone(
            actionable_payload["context"]["normalized_conservative_volume"]
        )


class MT5RiskFallbackWarnTests(unittest.TestCase):
    """D7 — `_estimate_position_risk_pct` must surface a one-time-per-(symbol,
    fallback) WARN when the MT5 contract path silently returns None/0 and a
    less-accurate fallback (tick or pip) was used to rescue the calculation.

    Pip/tick fallbacks can mis-state risk on cross pairs and synthetic CFDs
    where pip_value is hard-coded; operators need to see when the accurate
    path stops working even if the trade still gets a usable estimate.
    """

    def _bare_trader(self) -> LiveTrader:
        trader = LiveTrader.__new__(LiveTrader)
        trader.mt5 = MagicMock()
        trader.logger = MagicMock()
        return trader

    def _position(self):
        return SimpleNamespace(
            symbol="EURUSD",
            comment="",
            price_open=1.1000,
            sl=1.0950,
            volume=0.10,
            type=0,
            ticket=42,
        )

    def test_warns_once_when_mt5_path_returns_none_and_tick_fallback_rescues(self):
        trader = self._bare_trader()
        trader.mt5.calc_loss_amount.return_value = None
        # Tick-value fallback succeeds: 50 pips → 500 ticks of $0.10 each on 0.1 lot = $5
        trader.mt5.get_symbol_info.return_value = SimpleNamespace(
            trade_tick_size=0.0001, trade_tick_value=1.0,
        )
        account = SimpleNamespace(equity=10_000.0)

        risk_pct = trader._estimate_position_risk_pct(
            self._position(), account_info=account,
            canonical_symbol="EURUSD", broker_symbol="EURUSD",
        )

        self.assertIsNotNone(risk_pct, "tick fallback should produce a usable estimate")
        warn_calls = [c for c in trader.logger.warning.call_args_list
                      if "calc_loss_amount" in c[0][0]]
        self.assertEqual(len(warn_calls), 1)
        self.assertIn("tick_value", warn_calls[0][0][0])
        self.assertIn("EURUSD", warn_calls[0][0][0])

    def test_warns_once_when_mt5_path_returns_zero_and_pip_fallback_rescues(self):
        trader = self._bare_trader()
        trader.mt5.calc_loss_amount.return_value = 0.0  # also counts as failed
        # Force tick fallback to fail (zero tick metadata) → falls through to pip fallback
        trader.mt5.get_symbol_info.return_value = SimpleNamespace(
            trade_tick_size=0.0, trade_tick_value=0.0,
        )
        account = SimpleNamespace(equity=10_000.0)

        risk_pct = trader._estimate_position_risk_pct(
            self._position(), account_info=account,
            canonical_symbol="EURUSD", broker_symbol="EURUSD",
        )

        self.assertIsNotNone(risk_pct, "pip fallback should produce a usable estimate for EURUSD")
        warn_calls = [c for c in trader.logger.warning.call_args_list
                      if "calc_loss_amount" in c[0][0]]
        self.assertEqual(len(warn_calls), 1)
        self.assertIn("pip_value", warn_calls[0][0][0])

    def test_silent_when_mt5_path_succeeds(self):
        trader = self._bare_trader()
        trader.mt5.calc_loss_amount.return_value = 50.0  # primary path returns a usable value

        risk_pct = trader._estimate_position_risk_pct(
            self._position(), account_info=SimpleNamespace(equity=10_000.0),
            canonical_symbol="EURUSD", broker_symbol="EURUSD",
        )

        self.assertAlmostEqual(risk_pct, 0.5, places=6)
        warn_calls = [c for c in trader.logger.warning.call_args_list
                      if "calc_loss_amount" in c[0][0]]
        self.assertEqual(warn_calls, [],
                         "no fallback WARN when MT5 contract path is healthy")

    def test_dedup_per_symbol_and_path(self):
        trader = self._bare_trader()
        trader.mt5.calc_loss_amount.return_value = None
        trader.mt5.get_symbol_info.return_value = SimpleNamespace(
            trade_tick_size=0.0001, trade_tick_value=1.0,
        )
        account = SimpleNamespace(equity=10_000.0)

        for _ in range(3):
            trader._estimate_position_risk_pct(
                self._position(), account_info=account,
                canonical_symbol="EURUSD", broker_symbol="EURUSD",
            )

        warn_calls = [c for c in trader.logger.warning.call_args_list
                      if "calc_loss_amount" in c[0][0]]
        self.assertEqual(len(warn_calls), 1,
                         "WARN must dedupe per (symbol, fallback path) — got "
                         f"{len(warn_calls)} for 3 identical calls")

    def test_distinct_symbols_each_warn_once(self):
        trader = self._bare_trader()
        trader.mt5.calc_loss_amount.return_value = None
        trader.mt5.get_symbol_info.return_value = SimpleNamespace(
            trade_tick_size=0.0001, trade_tick_value=1.0,
        )
        account = SimpleNamespace(equity=10_000.0)

        for sym in ("EURUSD", "GBPUSD"):
            pos = SimpleNamespace(
                symbol=sym, comment="", price_open=1.1, sl=1.095,
                volume=0.10, type=0, ticket=1,
            )
            trader._estimate_position_risk_pct(
                pos, account_info=account,
                canonical_symbol=sym, broker_symbol=sym,
            )

        warn_calls = [c for c in trader.logger.warning.call_args_list
                      if "calc_loss_amount" in c[0][0]]
        self.assertEqual(len(warn_calls), 2)
        symbols_warned = {sym for sym in ("EURUSD", "GBPUSD")
                          if any(sym in c[0][0] for c in warn_calls)}
        self.assertEqual(symbols_warned, {"EURUSD", "GBPUSD"})


class PromotionStatusEntryGateTests(unittest.TestCase):
    def _trader(self, enable_trading=True, statuses=None):
        trader = LiveTrader.__new__(LiveTrader)
        trader.enable_trading = enable_trading
        trader.pipeline_config = SimpleNamespace(
            live_allowed_promotion_statuses=statuses or ["PROMOTED"]
        )
        return trader

    def test_live_default_accepts_only_promoted(self):
        trader = self._trader()
        self.assertTrue(trader._promotion_status_is_entry_eligible("PROMOTED"))
        self.assertFalse(trader._promotion_status_is_entry_eligible("PROMOTE_SMALL"))

    def test_live_can_explicitly_opt_into_promote_small(self):
        trader = self._trader(statuses=["PROMOTED", "PROMOTE_SMALL"])
        self.assertTrue(trader._promotion_status_is_entry_eligible("PROMOTE_SMALL"))
        self.assertFalse(trader._promotion_status_is_entry_eligible("PAPER_ONLY"))

    def test_paper_accepts_promote_small_for_observation(self):
        trader = self._trader(enable_trading=False)
        self.assertTrue(trader._promotion_status_is_entry_eligible("PROMOTE_SMALL"))


class PreSendAccountRevalidationTests(unittest.TestCase):
    @staticmethod
    def _account(**overrides):
        values = dict(
            login=123,
            server="Broker-Live",
            currency="USD",
            balance=2000.0,
            equity=2000.0,
            margin_free=1800.0,
            trade_allowed=True,
            trade_expert=True,
        )
        values.update(overrides)
        return SimpleNamespace(**values)

    def _trader(self):
        trader = LiveTrader.__new__(LiveTrader)
        trader.pipeline_config = SimpleNamespace(
            live_min_account_equity=1000.0,
            live_min_account_equity_currency="USD",
        )
        trader.mt5 = MagicMock()
        trader.mt5.config = SimpleNamespace(
            expected_login=123,
            expected_server="Broker-Live",
            expected_currency="USD",
        )
        return trader

    def test_stable_safe_snapshot_passes(self):
        trader = self._trader()
        fresh = self._account()
        trader.mt5.get_account_info.return_value = fresh
        result, error = trader._fresh_entry_account_validation(self._account())
        self.assertIs(result, fresh)
        self.assertIsNone(error)

    def test_equity_drop_below_minimum_blocks(self):
        trader = self._trader()
        trader.mt5.get_account_info.return_value = self._account(equity=999.99)
        result, error = trader._fresh_entry_account_validation(self._account())
        self.assertIsNone(result)
        self.assertIn("below live minimum", error)

    def test_identity_change_blocks(self):
        trader = self._trader()
        trader.mt5.get_account_info.return_value = self._account(login=999)
        result, error = trader._fresh_entry_account_validation(self._account())
        self.assertIsNone(result)
        self.assertIn("expected", error)

    def test_unpinned_funded_identity_blocks(self):
        trader = self._trader()
        trader.mt5.config = SimpleNamespace(
            expected_login=0, expected_server="", expected_currency=""
        )
        trader.mt5.get_account_info.return_value = self._account()
        result, error = trader._fresh_entry_account_validation(self._account())
        self.assertIsNone(result)
        self.assertIn("expected_login", error)

    def test_partial_funded_identity_pins_block(self):
        cases = (
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
        for connector_config in cases:
            with self.subTest(connector_config=connector_config):
                trader = self._trader()
                trader.mt5.config = connector_config
                trader.mt5.get_account_info.return_value = self._account()
                result, error = trader._fresh_entry_account_validation(
                    self._account()
                )
                self.assertIsNone(result)
                self.assertIn("requires", error)


class LotNormalizationDriftClassifierTests(unittest.TestCase):
    """findings.html §6/§8.1 — lot-normalization drift severity boundaries."""

    def test_within_tolerance_above_target_returns_warn(self):
        # target=1.0, tolerance=10% → warn boundary at 1.10
        self.assertEqual(
            classify_lot_normalization_drift(
                actual_risk_pct=1.05, target_risk_pct=1.0,
                max_risk_pct=2.0, tolerance_pct=10.0
            ),
            "warn",
        )

    def test_actual_equals_target_returns_ok(self):
        self.assertEqual(
            classify_lot_normalization_drift(
                actual_risk_pct=1.0, target_risk_pct=1.0,
                max_risk_pct=2.0, tolerance_pct=10.0
            ),
            "ok",
        )

    def test_above_tolerance_under_hard_cap_returns_block(self):
        # 1.15 is above 1.10 boundary, still under 2.0 hard cap
        self.assertEqual(
            classify_lot_normalization_drift(
                actual_risk_pct=1.15, target_risk_pct=1.0,
                max_risk_pct=2.0, tolerance_pct=10.0
            ),
            "block",
        )

    def test_above_hard_cap_returns_block(self):
        self.assertEqual(
            classify_lot_normalization_drift(
                actual_risk_pct=2.5, target_risk_pct=1.0,
                max_risk_pct=2.0, tolerance_pct=10.0
            ),
            "block",
        )

    def test_block_takes_priority_over_warn(self):
        # Even with absurd tolerance the hard cap still blocks.
        self.assertEqual(
            classify_lot_normalization_drift(
                actual_risk_pct=3.0, target_risk_pct=1.0,
                max_risk_pct=2.0, tolerance_pct=10000.0
            ),
            "block",
        )

    def test_zero_tolerance_blocks_any_drift(self):
        self.assertEqual(
            classify_lot_normalization_drift(
                actual_risk_pct=1.001, target_risk_pct=1.0,
                max_risk_pct=2.0, tolerance_pct=0.0
            ),
            "block",
        )

    def test_negative_tolerance_clamps_to_zero_and_blocks_drift(self):
        # Bad config shouldn't crash — negative tolerance behaves as 0.
        self.assertEqual(
            classify_lot_normalization_drift(
                actual_risk_pct=1.001, target_risk_pct=1.0,
                max_risk_pct=2.0, tolerance_pct=-50.0
            ),
            "block",
        )

    def test_actual_at_hard_cap_above_tolerance_is_block(self):
        # findings.html: "actual ≤ max_risk_pct" stays as WARN.
        self.assertEqual(
            classify_lot_normalization_drift(
                actual_risk_pct=2.0, target_risk_pct=1.0,
                max_risk_pct=2.0, tolerance_pct=10.0
            ),
            "block",
        )

    def test_historical_live_risk_overshoots_are_blocked(self):
        for actual in (3.5, 24.67):
            with self.subTest(actual=actual):
                self.assertEqual(
                    classify_lot_normalization_drift(actual, 0.5, 25.0, 2.0),
                    "block",
                )

    def test_nonfinite_or_nonpositive_inputs_fail_closed(self):
        cases = [
            (float("nan"), 1.0, 2.0, 2.0),
            (1.0, float("inf"), 2.0, 2.0),
            (1.0, 1.0, float("nan"), 2.0),
            (1.0, 1.0, 2.0, float("inf")),
            (0.0, 1.0, 2.0, 2.0),
        ]
        for values in cases:
            with self.subTest(values=values):
                self.assertEqual(classify_lot_normalization_drift(*values), "block")


if __name__ == "__main__":
    unittest.main()
