import unittest
from types import SimpleNamespace
from unittest.mock import MagicMock

from pm_main import LiveTrader
from pm_pipeline import RegimeConfig, SymbolConfig
from pm_position import TradeTagEncoder


def make_symbol_config(symbol: str) -> SymbolConfig:
    cfg_d1 = RegimeConfig(strategy_name="MomentumBurstStrategy", parameters={}, quality_score=0.4)
    cfg_h1 = RegimeConfig(strategy_name="KeltnerBreakoutStrategy", parameters={}, quality_score=0.4)
    return SymbolConfig(
        symbol=symbol,
        regime_configs={
            "D1": {"TREND": cfg_d1},
            "H1": {"CHOP": cfg_h1},
        },
        strategy_name="LegacyStrategy",
        timeframe="H4",
    )


def make_legacy_tag_config(symbol: str) -> SymbolConfig:
    cfg_d1 = RegimeConfig(strategy_name="PivotBreakoutStrategy", parameters={}, quality_score=0.4)
    cfg_h1 = RegimeConfig(strategy_name="EMACrossoverStrategy", parameters={}, quality_score=0.4)
    return SymbolConfig(
        symbol=symbol,
        regime_configs={
            "D1": {"TREND": cfg_d1},
            "H1": {"TREND": cfg_h1},
        },
        strategy_name="LegacyStrategy",
        timeframe="H1",
    )


class SecondaryPositionInferenceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.trader = LiveTrader.__new__(LiveTrader)

    def test_decode_comment_pm2_short(self) -> None:
        decoded = TradeTagEncoder.decode_comment("PM2:GBPUSD:D1:7t")
        self.assertIsNotNone(decoded)
        self.assertEqual(decoded.get("symbol"), "GBPUSD")
        self.assertEqual(decoded.get("timeframe"), "D1")
        self.assertEqual(decoded.get("strategy_code"), "7t")

    def test_decode_comment_pm3_short(self) -> None:
        decoded = TradeTagEncoder.decode_comment("PM3:EURJPY:H4:o1")
        self.assertIsNotNone(decoded)
        self.assertEqual(decoded.get("symbol"), "EURJPY")
        self.assertEqual(decoded.get("timeframe"), "H4")
        self.assertEqual(decoded.get("strategy_code"), "o1")

    def test_encode_comment_without_tier_uses_pm3_with_risk(self) -> None:
        comment = TradeTagEncoder.encode_comment(
            symbol="EURUSD",
            timeframe="H1",
            strategy_name="MomentumBurstStrategy",
            direction="LONG",
            risk_pct=1.2,
        )
        self.assertTrue(comment.startswith("PM3:"))
        decoded = TradeTagEncoder.decode_comment(comment)
        self.assertIsNotNone(decoded)
        self.assertAlmostEqual(decoded.get("risk_pct"), 1.2, places=6)

    def test_decode_comment_pm_legacy_strategy_only(self) -> None:
        decoded = TradeTagEncoder.decode_comment("PM_Stochastic")
        self.assertIsNotNone(decoded)
        self.assertEqual(decoded.get("strategy_tag"), "Stochastic")
        self.assertIsNone(decoded.get("timeframe"))

    def test_infer_timeframe_from_comment(self) -> None:
        config = make_symbol_config("USDSEK")
        comment = TradeTagEncoder.encode_comment(
            symbol="USDSEK",
            timeframe="D1",
            strategy_name="MomentumBurstStrategy",
            direction="LONG",
            risk_pct=1.0,
        )
        position = SimpleNamespace(comment=comment, magic=123)
        self.assertEqual(self.trader._infer_position_timeframe("USDSEK", config, position), "D1")

    def test_infer_timeframe_from_pm3_short_comment(self) -> None:
        config = make_symbol_config("EURJPY")
        position = SimpleNamespace(comment="PM3:EURJPY:D1:o1", magic=123)
        self.assertEqual(self.trader._infer_position_timeframe("EURJPY", config, position), "D1")

    def test_infer_timeframe_from_legacy_strategy_tag_when_unique(self) -> None:
        config = make_legacy_tag_config("AUDNZD")
        position = SimpleNamespace(comment="PM_PivotBreak", magic=123)
        self.assertEqual(self.trader._infer_position_timeframe("AUDNZD", config, position), "D1")

    def test_infer_timeframe_from_magic_when_comment_missing(self) -> None:
        config = make_symbol_config("USDSEK")
        magic = TradeTagEncoder.encode_magic("USDSEK", "D1", "TREND")
        position = SimpleNamespace(comment="", magic=magic)
        self.assertEqual(self.trader._infer_position_timeframe("USDSEK", config, position), "D1")

    def test_infer_timeframe_from_manual_override_by_magic(self) -> None:
        config = make_symbol_config("USDSEK")
        self.trader.pipeline_config = SimpleNamespace(
            position_timeframe_overrides={"magic:987654321": "H4"}
        )
        position = SimpleNamespace(comment="PM_Stochastic", magic=987654321, ticket=1234)
        self.assertEqual(self.trader._infer_position_timeframe("USDSEK", config, position), "H4")

    def test_infer_timeframe_from_history_metadata(self) -> None:
        config = make_symbol_config("USDSEK")
        self.trader.pipeline_config = SimpleNamespace(position_timeframe_overrides={})
        self.trader.mt5 = MagicMock()
        self.trader.mt5.get_position_opening_metadata.return_value = {
            "comment": "PM3:USDSEK:H1:abc12",
            "magic": 0,
            "reason": 3,
        }
        position = SimpleNamespace(comment="", magic=0, ticket=1234, identifier=555)

        self.assertEqual(self.trader._infer_position_timeframe("USDSEK", config, position), "H1")
        self.trader.mt5.get_position_opening_metadata.assert_called_once_with(555)

    def test_infer_timeframe_negative_caches_failed_history_lookup(self) -> None:
        config = make_symbol_config("USDSEK")
        self.trader.pipeline_config = SimpleNamespace(position_timeframe_overrides={})
        self.trader.mt5 = MagicMock()
        self.trader.mt5.get_position_opening_metadata.return_value = None
        position = SimpleNamespace(comment="", magic=0, ticket=1234, identifier=777)

        self.assertIsNone(self.trader._infer_position_timeframe("USDSEK", config, position))
        self.assertIsNone(self.trader._infer_position_timeframe("USDSEK", config, position))
        self.trader.mt5.get_position_opening_metadata.assert_called_once_with(777)

    def test_infer_timeframe_unknown_magic_returns_none(self) -> None:
        config = make_symbol_config("USDSEK")
        position = SimpleNamespace(comment="", magic=987654321)
        self.assertIsNone(self.trader._infer_position_timeframe("USDSEK", config, position))

    def test_estimate_position_risk_warns_when_geometry_and_comment_fail(self) -> None:
        trader = LiveTrader.__new__(LiveTrader)
        trader.mt5 = MagicMock()
        trader.logger = MagicMock()
        trader.mt5.calc_loss_amount.return_value = None
        trader.mt5.get_symbol_info.return_value = None

        position = SimpleNamespace(
            symbol="EURUSD",
            comment="GARBAGE",
            price_open=0.0,
            sl=0.0,
            volume=0.0,
            type=0,
            ticket=42,
        )
        account = SimpleNamespace(equity=10000.0)

        result = trader._estimate_position_risk_pct(
            position,
            account_info=account,
            canonical_symbol="EURUSD",
            broker_symbol="EURUSD",
            default_risk_pct=0.5,
        )

        self.assertAlmostEqual(result, 0.5, places=6)
        trader.logger.warning.assert_called_once()
        message = trader.logger.warning.call_args[0][0]
        self.assertIn("EURUSD", message)
        self.assertIn("0.500%", message)
        self.assertIn("ticket=42", message)
        self.assertIn("geometry_unavailable", message)

    def test_estimate_position_risk_uses_geometry_silently_when_available(self) -> None:
        trader = LiveTrader.__new__(LiveTrader)
        trader.mt5 = MagicMock()
        trader.logger = MagicMock()
        trader.mt5.calc_loss_amount.return_value = 50.0

        position = SimpleNamespace(
            symbol="EURUSD",
            comment="",
            price_open=1.1000,
            sl=1.0950,
            volume=0.10,
            type=0,
            ticket=99,
        )
        account = SimpleNamespace(equity=10000.0)

        result = trader._estimate_position_risk_pct(
            position,
            account_info=account,
            canonical_symbol="EURUSD",
            broker_symbol="EURUSD",
            default_risk_pct=1.0,
        )

        self.assertAlmostEqual(result, 0.5, places=6)
        trader.logger.warning.assert_not_called()

    def test_estimate_position_risk_warns_when_geometry_zero_and_no_comment(self) -> None:
        trader = LiveTrader.__new__(LiveTrader)
        trader.mt5 = MagicMock()
        trader.logger = MagicMock()
        trader.mt5.calc_loss_amount.return_value = 0.0
        trader.mt5.get_symbol_info.return_value = SimpleNamespace(
            trade_tick_size=0.0, trade_tick_value=0.0
        )

        position = SimpleNamespace(
            symbol="",
            comment="",
            price_open=1.1000,
            sl=1.0950,
            volume=0.10,
            type=0,
            ticket=7,
        )
        account = SimpleNamespace(equity=10000.0)

        result = trader._estimate_position_risk_pct(
            position,
            account_info=account,
            canonical_symbol="",
            broker_symbol="",
            default_risk_pct=0.25,
        )

        self.assertAlmostEqual(result, 0.25, places=6)
        trader.logger.warning.assert_called_once()
        message = trader.logger.warning.call_args[0][0]
        self.assertIn("geometry_returned_zero", message)
        self.assertIn("comment_empty", message)

    def test_execute_entry_records_skipped_position_exists_for_dashboard(self) -> None:
        trader = LiveTrader.__new__(LiveTrader)
        trader.mt5 = MagicMock()
        trader._decision_throttle = MagicMock()
        trader._actionable_log = MagicMock()
        trader._last_order_times = {}
        trader.position_config = SimpleNamespace()
        trader.pipeline_config = SimpleNamespace()
        trader.position_calc = MagicMock()
        trader.enable_trading = True
        trader.logger = MagicMock()

        existing_position = SimpleNamespace(
            comment=TradeTagEncoder.encode_comment(
                symbol="USDSEK",
                timeframe="D1",
                strategy_name="MomentumBurstStrategy",
                direction="SHORT",
                risk_pct=1.0,
            ),
            ticket=321,
        )
        trader.mt5.get_position_by_symbol_magic.return_value = existing_position

        trader._execute_entry(
            symbol="USDSEK",
            signal=-1,
            strategy=MagicMock(),
            features=None,
            spec=None,
            magic=TradeTagEncoder.encode_magic("USDSEK", "D1", "TREND"),
            config=SimpleNamespace(symbol="USDSEK"),
            decision_key="test-key",
            bar_time_iso="2026-02-10T00:22:06",
            best_candidate={
                "timeframe": "D1",
                "regime": "TREND",
                "strategy_name": "MomentumBurstStrategy",
                "signal": -1,
            },
            is_secondary_trade=True,
        )

        trader._decision_throttle.record_decision.assert_called_once()
        trader._actionable_log.record.assert_called_once()
        call_args = trader._actionable_log.record.call_args[0]
        self.assertEqual(call_args[0], "USDSEK")
        self.assertEqual(call_args[1]["action"], "SKIPPED_POSITION_EXISTS")

    def test_execute_entry_uses_fresh_position_read_after_snapshot(self) -> None:
        trader = LiveTrader.__new__(LiveTrader)
        trader.mt5 = MagicMock()
        trader._decision_throttle = MagicMock()
        trader._actionable_log = MagicMock()
        trader._last_order_times = {}
        trader.position_config = SimpleNamespace()
        trader.pipeline_config = SimpleNamespace()
        trader.position_calc = MagicMock()
        trader.enable_trading = True
        trader.logger = MagicMock()

        magic = TradeTagEncoder.encode_magic("USDSEK", "D1", "TREND")
        existing_position = SimpleNamespace(symbol="USDSEK", magic=magic, ticket=654)
        trader.mt5.get_position_by_symbol_magic.return_value = None
        trader.mt5.get_positions.return_value = [existing_position]

        trader._execute_entry(
            symbol="USDSEK",
            signal=-1,
            strategy=MagicMock(),
            features=None,
            spec=None,
            magic=magic,
            config=SimpleNamespace(symbol="USDSEK"),
            decision_key="test-key",
            bar_time_iso="2026-02-10T00:22:06",
            best_candidate={
                "timeframe": "D1",
                "regime": "TREND",
                "strategy_name": "MomentumBurstStrategy",
                "signal": -1,
            },
            positions_snapshot=[],
        )

        trader.mt5.get_positions.assert_called_with(symbol="USDSEK")
        trader._actionable_log.record.assert_called_once()
        self.assertEqual(trader._actionable_log.record.call_args[0][1]["action"], "SKIPPED_POSITION_EXISTS")


if __name__ == "__main__":
    unittest.main()
