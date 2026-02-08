"""
Test portfolio risk cap enforcement for multi-timeframe trading.

Validates that max_combined_risk_pct is properly enforced when opening
multiple positions on same symbol across different timeframes (D1 + lower TF scenario).
"""
import pytest
from unittest.mock import Mock, MagicMock, patch
from datetime import datetime
from dataclasses import dataclass

# Mock dependencies
@dataclass
class MockPosition:
    """Mock MT5 position."""
    ticket: int
    symbol: str
    comment: str
    sl: float = 0.0
    price_open: float = 0.0
    volume: float = 0.0


@dataclass
class MockAccountInfo:
    """Mock account info."""
    equity: float
    balance: float


@dataclass
class MockInstrumentSpec:
    """Mock instrument specification."""
    pip_size: float
    pip_value: float


class TestPortfolioRiskCap:
    """Test portfolio risk cap enforcement."""

    def setup_method(self):
        """Setup test fixtures."""
        # Import here to avoid circular dependencies
        from pm_position import TradeTagEncoder
        self.encoder = TradeTagEncoder

    def test_encode_decode_comment_with_risk(self):
        """Test that trade comments encode/decode correctly with risk percentage."""
        comment = self.encoder.encode_comment(
            symbol="EURUSD",
            timeframe="D1",
            strategy_name="MomentumStrategy",
            direction="LONG",
            risk_pct=1.5
        )

        # Should use PM3 format
        assert comment.startswith("PM3:")

        # Decode and verify
        metadata = self.encoder.decode_comment(comment)
        assert metadata is not None
        assert metadata['symbol'] == "EURUSD"
        assert metadata['timeframe'] == "D1"
        assert metadata['direction'] == "LONG"
        assert metadata['risk_pct'] == 1.5

    def test_decode_multiple_comment_formats(self):
        """Test backward compatibility with v1, v2, and v3 comment formats."""
        # v3 (current - winners-only)
        v3_comment = "PM3:EURUSD:H4:abc12:L:15"
        v3_meta = self.encoder.decode_comment(v3_comment)
        assert v3_meta['symbol'] == "EURUSD"
        assert v3_meta['timeframe'] == "H4"
        assert v3_meta['risk_pct'] == 1.5

        # v2 (deprecated with tier)
        v2_comment = "PM2:GBPUSD:H1:xyz78:S:2:20"
        v2_meta = self.encoder.decode_comment(v2_comment)
        assert v2_meta['symbol'] == "GBPUSD"
        assert v2_meta['timeframe'] == "H1"
        assert v2_meta['risk_pct'] == 2.0

        # v1 (legacy - no risk)
        v1_comment = "PM:AUDUSD:D1:TrendStrategy:SHORT"
        v1_meta = self.encoder.decode_comment(v1_comment)
        assert v1_meta['symbol'] == "AUDUSD"
        assert v1_meta['timeframe'] == "D1"
        assert v1_meta['direction'] == "SHORT"
        assert 'risk_pct' not in v1_meta

    def test_check_portfolio_risk_cap_no_positions(self):
        """Test risk cap check with no existing positions."""
        # Mock MT5Connector
        mock_mt5 = Mock()
        mock_mt5.get_account_info.return_value = MockAccountInfo(equity=10000, balance=10000)
        mock_mt5.is_connected.return_value = False  # Skip MT5 sync
        mock_mt5.get_positions.return_value = []
        mock_mt5.is_connected.return_value = False  # Skip MT5 sync

        # Mock PortfolioManager
        mock_pm = Mock()
        mock_pm.symbols = []  # Empty symbols list to skip sync
        mock_pm.symbols = []  # Empty symbols list to skip sync

        # Mock configs
        mock_pipeline_config = Mock()
        mock_pipeline_config.max_combined_risk_pct = 3.0

        # Create minimal LiveTrader mock
        from pm_main import LiveTrader
        from pm_position import PositionConfig
        import logging
        logging.disable(logging.CRITICAL)  # Suppress log messages

        trader = LiveTrader(
            mt5_connector=mock_mt5,
            portfolio_manager=mock_pm,
            position_config=PositionConfig(),
            pipeline_config=mock_pipeline_config,
            enable_trading=False
        )

        # Test: no positions, should allow trade
        can_trade, reason = trader._check_portfolio_risk_cap(
            symbol="EURUSD",
            new_trade_risk_pct=1.5,
            broker_symbol="EURUSD"
        )

        assert can_trade is True
        assert "Symbol risk OK" in reason
        assert "0 open positions" in reason
        logging.disable(logging.NOTSET)  # Re-enable logging

    def test_check_portfolio_risk_cap_under_limit(self):
        """Test risk cap check with existing positions under limit."""
        # Create position with 1.2% risk
        pos1 = MockPosition(
            ticket=12345,
            symbol="EURUSD",
            comment="PM3:EURUSD:D1:abc12:L:12"  # 1.2% risk
        )

        # Mock MT5Connector
        mock_mt5 = Mock()
        mock_mt5.get_account_info.return_value = MockAccountInfo(equity=10000, balance=10000)
        mock_mt5.is_connected.return_value = False  # Skip MT5 sync
        mock_mt5.get_positions.return_value = [pos1]

        # Mock PortfolioManager
        mock_pm = Mock()
        mock_pm.symbols = []  # Empty symbols list to skip sync

        # Mock configs
        mock_pipeline_config = Mock()
        mock_pipeline_config.max_combined_risk_pct = 3.0

        # Create LiveTrader
        from pm_main import LiveTrader
        from pm_position import PositionConfig

        trader = LiveTrader(
            mt5_connector=mock_mt5,
            portfolio_manager=mock_pm,
            position_config=PositionConfig(),
            pipeline_config=mock_pipeline_config,
            enable_trading=False
        )

        # Test: 1.2% existing + 1.5% new = 2.7% < 3.0% cap
        can_trade, reason = trader._check_portfolio_risk_cap(
            symbol="EURUSD",
            new_trade_risk_pct=1.5,
            broker_symbol="EURUSD"
        )

        assert can_trade is True
        assert "Symbol risk OK" in reason
        assert "2.70%" in reason  # Total risk
        assert "1 open positions" in reason

    def test_check_portfolio_risk_cap_exceeds_limit(self):
        """Test risk cap check when adding trade would exceed limit."""
        # Create two existing positions
        pos1 = MockPosition(
            ticket=12345,
            symbol="EURUSD",
            comment="PM3:EURUSD:D1:abc12:L:15"  # 1.5% risk
        )
        pos2 = MockPosition(
            ticket=67890,
            symbol="EURUSD",
            comment="PM3:EURUSD:H4:xyz78:L:10"  # 1.0% risk
        )

        # Mock MT5Connector
        mock_mt5 = Mock()
        mock_mt5.get_account_info.return_value = MockAccountInfo(equity=10000, balance=10000)
        mock_mt5.is_connected.return_value = False  # Skip MT5 sync
        mock_mt5.get_positions.return_value = [pos1, pos2]

        # Mock PortfolioManager
        mock_pm = Mock()
        mock_pm.symbols = []  # Empty symbols list to skip sync

        # Mock configs
        mock_pipeline_config = Mock()
        mock_pipeline_config.max_combined_risk_pct = 3.0

        # Create LiveTrader
        from pm_main import LiveTrader
        from pm_position import PositionConfig

        trader = LiveTrader(
            mt5_connector=mock_mt5,
            portfolio_manager=mock_pm,
            position_config=PositionConfig(),
            pipeline_config=mock_pipeline_config,
            enable_trading=False
        )

        # Test: 1.5% + 1.0% + 1.0% = 3.5% > 3.0% cap
        can_trade, reason = trader._check_portfolio_risk_cap(
            symbol="EURUSD",
            new_trade_risk_pct=1.0,
            broker_symbol="EURUSD"
        )

        assert can_trade is False
        assert "Symbol risk cap exceeded" in reason
        assert "existing 2.50%" in reason
        assert "new 1.00%" in reason
        assert "3.50% > max 3.00%" in reason
        assert "2 positions" in reason

    def test_check_portfolio_risk_cap_different_symbol_ignored(self):
        """Test that positions on different symbols are not counted."""
        # Position on GBPUSD should not affect EURUSD risk
        pos1 = MockPosition(
            ticket=12345,
            symbol="GBPUSD",
            comment="PM3:GBPUSD:D1:abc12:L:20"  # 2.0% risk on GBPUSD
        )

        # Mock MT5Connector
        mock_mt5 = Mock()
        mock_mt5.get_account_info.return_value = MockAccountInfo(equity=10000, balance=10000)
        mock_mt5.is_connected.return_value = False  # Skip MT5 sync
        mock_mt5.get_positions.return_value = [pos1]

        # Mock PortfolioManager
        mock_pm = Mock()
        mock_pm.symbols = []  # Empty symbols list to skip sync

        # Mock configs
        mock_pipeline_config = Mock()
        mock_pipeline_config.max_combined_risk_pct = 3.0

        # Create LiveTrader
        from pm_main import LiveTrader
        from pm_position import PositionConfig

        trader = LiveTrader(
            mt5_connector=mock_mt5,
            portfolio_manager=mock_pm,
            position_config=PositionConfig(),
            pipeline_config=mock_pipeline_config,
            enable_trading=False
        )

        # Test: checking EURUSD should not count GBPUSD position
        can_trade, reason = trader._check_portfolio_risk_cap(
            symbol="EURUSD",
            new_trade_risk_pct=2.5,
            broker_symbol="EURUSD"
        )

        assert can_trade is True
        assert "0 open positions" in reason  # No EURUSD positions

    def test_check_portfolio_risk_cap_fallback_estimation(self):
        """Test risk estimation fallback when comment cannot be decoded."""
        # Position with unparseable comment
        pos1 = MockPosition(
            ticket=12345,
            symbol="EURUSD",
            comment="Some external comment",
            sl=1.0950,
            price_open=1.1000,
            volume=0.1
        )

        # Mock MT5Connector
        mock_mt5 = Mock()
        mock_mt5.get_account_info.return_value = MockAccountInfo(equity=10000, balance=10000)
        mock_mt5.is_connected.return_value = False  # Skip MT5 sync
        mock_mt5.get_positions.return_value = [pos1]

        # Mock PortfolioManager
        mock_pm = Mock()
        mock_pm.symbols = []  # Empty symbols list to skip sync
        mock_pm.get_instrument_spec.return_value = MockInstrumentSpec(
            pip_size=0.0001,
            pip_value=1.0
        )

        # Mock configs
        mock_pipeline_config = Mock()
        mock_pipeline_config.max_combined_risk_pct = 3.0

        # Create LiveTrader
        from pm_main import LiveTrader
        from pm_position import PositionConfig

        trader = LiveTrader(
            mt5_connector=mock_mt5,
            portfolio_manager=mock_pm,
            position_config=PositionConfig(),
            pipeline_config=mock_pipeline_config,
            enable_trading=False
        )

        # Test: should estimate risk from SL distance
        can_trade, reason = trader._check_portfolio_risk_cap(
            symbol="EURUSD",
            new_trade_risk_pct=1.0,
            broker_symbol="EURUSD"
        )

        # Should succeed (existing risk estimated as 0.5%, new 1.0% = 1.5% < 3.0%)
        # Calculation: 50 pips * 1.0 pip_value * 0.1 lots = $5 = 0.05% of $10k
        assert can_trade is True

    def test_check_portfolio_risk_cap_exact_limit(self):
        """Test risk cap check at exact limit (should allow)."""
        # Existing position with 1.5% risk
        pos1 = MockPosition(
            ticket=12345,
            symbol="EURUSD",
            comment="PM3:EURUSD:D1:abc12:L:15"
        )

        # Mock MT5Connector
        mock_mt5 = Mock()
        mock_mt5.get_account_info.return_value = MockAccountInfo(equity=10000, balance=10000)
        mock_mt5.is_connected.return_value = False  # Skip MT5 sync
        mock_mt5.get_positions.return_value = [pos1]

        # Mock PortfolioManager
        mock_pm = Mock()
        mock_pm.symbols = []  # Empty symbols list to skip sync

        # Mock configs
        mock_pipeline_config = Mock()
        mock_pipeline_config.max_combined_risk_pct = 3.0

        # Create LiveTrader
        from pm_main import LiveTrader
        from pm_position import PositionConfig

        trader = LiveTrader(
            mt5_connector=mock_mt5,
            portfolio_manager=mock_pm,
            position_config=PositionConfig(),
            pipeline_config=mock_pipeline_config,
            enable_trading=False
        )

        # Test: 1.5% existing + 1.5% new = 3.0% == 3.0% cap (should allow)
        can_trade, reason = trader._check_portfolio_risk_cap(
            symbol="EURUSD",
            new_trade_risk_pct=1.5,
            broker_symbol="EURUSD"
        )

        assert can_trade is True
        assert "3.00% / 3.00%" in reason


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
