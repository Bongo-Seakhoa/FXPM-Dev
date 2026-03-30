import unittest
import pandas as pd

from pm_core import PipelineConfig, Backtester, InstrumentSpec


class DummyStrategy:
    name = "Dummy"

    def get_default_params(self):
        return {}

    def get_params(self):
        return {}

    def generate_signals(self, features, symbol):
        # One long signal on the first bar only
        sig = pd.Series(0, index=features.index)
        sig.iloc[0] = 1
        return sig

    def calculate_stops(self, features, signal, symbol, spec=None, bar_index=None):
        # Fixed 10 pip stop and 10 pip TP
        return 10.0, 10.0


class BacktesterTests(unittest.TestCase):
    def test_entry_timing_and_exit(self):
        cfg = PipelineConfig(
            use_spread=False,
            use_slippage=False,
            use_commission=False,
            risk_per_trade_pct=1.0,
        )
        bt = Backtester(cfg)

        # Construct data with clear TP hit and no SL hit
        prices = [100.0 + i * 0.01 for i in range(20)]
        df = pd.DataFrame({
            "Open": prices,
            "High": [p + 0.2 for p in prices],
            "Low": [p - 0.05 for p in prices],
            "Close": prices,
            "Volume": [100] * len(prices),
        })

        strat = DummyStrategy()
        signals = strat.generate_signals(df, "EURUSD")
        spec = InstrumentSpec(symbol="EURUSD", pip_position=2, pip_value=10.0, spread_avg=0.0)
        res = bt.run(df, signals, "EURUSD", strat, spec=spec)

        trades = res.get("trades", [])
        self.assertTrue(len(trades) >= 1)
        t0 = trades[0]
        self.assertEqual(t0["signal_bar"], 0)
        self.assertEqual(t0["entry_bar"], 1)
        # Ensure entry_time is after signal_time
        self.assertTrue(t0["entry_time"] > t0["signal_time"])

    def test_same_bar_exit_is_possible_after_entry(self):
        cfg = PipelineConfig(
            use_spread=False,
            use_slippage=False,
            use_commission=False,
            risk_per_trade_pct=1.0,
        )
        bt = Backtester(cfg)
        df = pd.DataFrame({
            "Open": [100.0, 100.0, 100.0],
            "High": [100.0, 100.2, 100.0],
            "Low": [100.0, 99.95, 100.0],
            "Close": [100.0, 100.1, 100.0],
            "Volume": [100, 100, 100],
        })
        strat = DummyStrategy()
        signals = strat.generate_signals(df, "EURUSD")
        spec = InstrumentSpec(symbol="EURUSD", pip_position=2, pip_value=10.0, spread_avg=0.0)
        res = bt.run(df, signals, "EURUSD", strat, spec=spec)
        trade = res["trades"][0]
        self.assertEqual(trade["entry_bar"], 1)
        self.assertEqual(trade["exit_bar"], 1)

    def test_signal_index_mismatch_raises(self):
        cfg = PipelineConfig(use_spread=False, use_slippage=False, use_commission=False)
        bt = Backtester(cfg)
        df = pd.DataFrame({
            "Open": [100.0, 100.1, 100.2],
            "High": [100.2, 100.3, 100.4],
            "Low": [99.8, 99.9, 100.0],
            "Close": [100.0, 100.1, 100.2],
            "Volume": [100, 100, 100],
        }, index=pd.date_range("2024-01-01", periods=3, freq="h"))
        spec = InstrumentSpec(symbol="EURUSD", pip_position=2, pip_value=10.0, spread_avg=0.0)
        strat = DummyStrategy()
        bad_signals = pd.Series([1, 0, 0], index=pd.date_range("2024-02-01", periods=3, freq="h"))
        with self.assertRaises(ValueError):
            bt.run(df, bad_signals, "EURUSD", strat, spec=spec)


if __name__ == "__main__":
    unittest.main()
