import unittest
from dataclasses import dataclass
from unittest.mock import Mock

from pm_position import TradeTagEncoder


@dataclass
class MockPosition:
    ticket: int
    symbol: str
    comment: str
    sl: float = 0.0
    price_open: float = 0.0
    volume: float = 0.0


@dataclass
class MockAccountInfo:
    equity: float
    balance: float


class PortfolioRiskCapTests(unittest.TestCase):
    def _make_trader(self, positions, max_combined=3.0, pipeline_risk_per_trade=1.0):
        from pm_main import LiveTrader
        from pm_position import PositionConfig

        mt5 = Mock()
        mt5.is_connected.return_value = False
        mt5.get_account_info.return_value = MockAccountInfo(equity=10000.0, balance=10000.0)
        mt5.get_positions.return_value = positions

        pm = Mock()
        pm.symbols = []

        pipeline_config = Mock()
        pipeline_config.max_combined_risk_pct = max_combined
        pipeline_config.risk_per_trade_pct = pipeline_risk_per_trade

        return LiveTrader(
            mt5_connector=mt5,
            portfolio_manager=pm,
            position_config=PositionConfig(),
            pipeline_config=pipeline_config,
            enable_trading=False,
        )

    def test_allows_when_under_cap(self):
        # Existing 1.2%, new 1.5% -> 2.7% under 3.0%.
        comment = TradeTagEncoder.encode_comment(
            symbol="EURUSD",
            timeframe="D1",
            strategy_name="TestStrategy",
            direction="LONG",
            risk_pct=1.2,
        )
        pos = MockPosition(ticket=1, symbol="EURUSD", comment=comment)
        trader = self._make_trader([pos], max_combined=3.0)
        can_trade, reason = trader._check_portfolio_risk_cap("EURUSD", 1.5, "EURUSD")
        self.assertTrue(can_trade)
        self.assertIn("risk OK", reason)

    def test_blocks_when_exceeding_cap(self):
        # Existing positions fallback to 1.0% each (no explicit risk tag), new 1.5% -> 3.5% over 3.0%.
        pos1 = MockPosition(
            ticket=1,
            symbol="EURUSD",
            comment=TradeTagEncoder.encode_comment(
                symbol="EURUSD",
                timeframe="D1",
                strategy_name="TestStrategy",
                direction="LONG",
                risk_pct=1.5,
            ),
        )
        pos2 = MockPosition(
            ticket=2,
            symbol="EURUSD",
            comment=TradeTagEncoder.encode_comment(
                symbol="EURUSD",
                timeframe="H4",
                strategy_name="TestStrategy",
                direction="SHORT",
                risk_pct=1.0,
            ),
        )
        trader = self._make_trader([pos1, pos2], max_combined=3.0)
        can_trade, reason = trader._check_portfolio_risk_cap("EURUSD", 1.5, "EURUSD")
        self.assertFalse(can_trade)
        self.assertIn("exceeded", reason)

    def test_allows_at_exact_cap(self):
        # Existing 1.5%, new 1.5% -> exactly 3.0%.
        pos = MockPosition(
            ticket=1,
            symbol="EURUSD",
            comment=TradeTagEncoder.encode_comment(
                symbol="EURUSD",
                timeframe="D1",
                strategy_name="TestStrategy",
                direction="LONG",
                risk_pct=1.5,
            ),
        )
        trader = self._make_trader([pos], max_combined=3.0)
        can_trade, _ = trader._check_portfolio_risk_cap("EURUSD", 1.5, "EURUSD")
        self.assertTrue(can_trade)

    def test_fallback_risk_estimate_uses_pipeline_risk_source(self):
        # Position has no parseable risk tag and no SL info, so fallback estimate is used.
        # Pipeline risk should be used as the source-of-truth for this estimate.
        pos = MockPosition(ticket=1, symbol="EURUSD", comment="")
        trader = self._make_trader([pos], max_combined=3.0, pipeline_risk_per_trade=2.5)

        # Existing fallback risk 2.5% + new 1.0% -> 3.5% (must be blocked).
        can_trade, reason = trader._check_portfolio_risk_cap("EURUSD", 1.0, "EURUSD")
        self.assertFalse(can_trade)
        self.assertIn("exceeded", reason)


if __name__ == "__main__":
    unittest.main()
