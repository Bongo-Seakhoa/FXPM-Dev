"""Test position sizing edge cases: volume=0, min_lot, max_lot boundaries.

Also covers the lot-normalization risk-drift classifier (findings.html §6/§8.1):
INFO-when-within-tolerance / WARN-when-above / BLOCK-when-over-hard-cap.
"""
import logging
import unittest
from types import SimpleNamespace
from unittest.mock import MagicMock

from pm_core import InstrumentSpec
from pm_main import LiveTrader, classify_lot_normalization_drift
from pm_position import PositionCalculator, PositionConfig


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

    def test_optional_min_lot_clamp_respects_hard_cap(self):
        """When enabled, min-lot clamping is allowed only if the hard cap still permits it."""
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
        self.assertEqual(result.volume, 0.01)

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


class LotNormalizationDriftClassifierTests(unittest.TestCase):
    """findings.html §6/§8.1 — lot-normalization drift severity boundaries."""

    def test_within_tolerance_returns_ok(self):
        # target=1.0, tolerance=10% → warn boundary at 1.10
        self.assertEqual(
            classify_lot_normalization_drift(
                actual_risk_pct=1.05, target_risk_pct=1.0,
                max_risk_pct=2.0, tolerance_pct=10.0
            ),
            "ok",
        )

    def test_actual_equals_target_returns_ok(self):
        self.assertEqual(
            classify_lot_normalization_drift(
                actual_risk_pct=1.0, target_risk_pct=1.0,
                max_risk_pct=2.0, tolerance_pct=10.0
            ),
            "ok",
        )

    def test_above_tolerance_under_hard_cap_returns_warn(self):
        # 1.15 is above 1.10 boundary, still under 2.0 hard cap
        self.assertEqual(
            classify_lot_normalization_drift(
                actual_risk_pct=1.15, target_risk_pct=1.0,
                max_risk_pct=2.0, tolerance_pct=10.0
            ),
            "warn",
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

    def test_zero_tolerance_warns_on_any_drift(self):
        self.assertEqual(
            classify_lot_normalization_drift(
                actual_risk_pct=1.001, target_risk_pct=1.0,
                max_risk_pct=2.0, tolerance_pct=0.0
            ),
            "warn",
        )

    def test_negative_tolerance_clamps_to_zero(self):
        # Bad config shouldn't crash — negative tolerance behaves as 0.
        self.assertEqual(
            classify_lot_normalization_drift(
                actual_risk_pct=1.001, target_risk_pct=1.0,
                max_risk_pct=2.0, tolerance_pct=-50.0
            ),
            "warn",
        )

    def test_actual_at_hard_cap_is_warn_not_block(self):
        # findings.html: "actual ≤ max_risk_pct" stays as WARN.
        self.assertEqual(
            classify_lot_normalization_drift(
                actual_risk_pct=2.0, target_risk_pct=1.0,
                max_risk_pct=2.0, tolerance_pct=10.0
            ),
            "warn",
        )


if __name__ == "__main__":
    unittest.main()
