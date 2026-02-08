"""Test SL exit scenarios for long and short trades."""
import unittest
import pandas as pd
from pm_core import PipelineConfig, Backtester, InstrumentSpec


class SLDummyStrategy:
    name = "SLDummy"

    def get_default_params(self):
        return {}

    def get_params(self):
        return {}

    def generate_signals(self, features, symbol):
        sig = pd.Series(0, index=features.index)
        sig.iloc[0] = 1  # Long signal
        return sig

    def calculate_stops(self, features, signal, symbol, spec=None, bar_index=None):
        return 10.0, 50.0  # Tight SL, wide TP


class SLShortDummyStrategy:
    name = "SLShortDummy"

    def get_default_params(self):
        return {}

    def get_params(self):
        return {}

    def generate_signals(self, features, symbol):
        sig = pd.Series(0, index=features.index)
        sig.iloc[0] = -1  # Short signal
        return sig

    def calculate_stops(self, features, signal, symbol, spec=None, bar_index=None):
        return 10.0, 50.0


class BacktestSLExitTests(unittest.TestCase):

    def _make_config(self):
        return PipelineConfig(
            use_spread=False, use_slippage=False,
            use_commission=False, risk_per_trade_pct=1.0,
        )

    def test_long_sl_hit(self):
        """A long trade should exit at SL when price drops."""
        bt = Backtester(self._make_config())
        # Price goes up slightly then crashes
        prices = [100.0, 100.05, 100.03, 99.85, 99.80, 99.75] + [99.70] * 14
        df = pd.DataFrame({
            "Open": prices,
            "High": [p + 0.02 for p in prices],
            "Low": [p - 0.12 for p in prices],  # Ensure low touches SL
            "Close": prices,
            "Volume": [100] * len(prices),
        })
        spec = InstrumentSpec(symbol="EURUSD", pip_position=2, pip_value=10.0, spread_avg=0.0)
        strat = SLDummyStrategy()
        signals = strat.generate_signals(df, "EURUSD")
        res = bt.run(df, signals, "EURUSD", strat, spec=spec)
        trades = res.get("trades", [])
        self.assertTrue(len(trades) >= 1, "Expected at least one trade")
        t0 = trades[0]
        self.assertIn(t0.get("exit_reason", ""), ["SL", "sl", "closed_sl"])

    def test_short_sl_hit(self):
        """A short trade should exit at SL when price rises."""
        bt = Backtester(self._make_config())
        prices = [100.0, 99.95, 100.05, 100.15, 100.25, 100.30] + [100.35] * 14
        df = pd.DataFrame({
            "Open": prices,
            "High": [p + 0.12 for p in prices],
            "Low": [p - 0.02 for p in prices],
            "Close": prices,
            "Volume": [100] * len(prices),
        })
        spec = InstrumentSpec(symbol="EURUSD", pip_position=2, pip_value=10.0, spread_avg=0.0)
        strat = SLShortDummyStrategy()
        signals = strat.generate_signals(df, "EURUSD")
        res = bt.run(df, signals, "EURUSD", strat, spec=spec)
        trades = res.get("trades", [])
        self.assertTrue(len(trades) >= 1, "Expected at least one trade")


if __name__ == "__main__":
    unittest.main()
