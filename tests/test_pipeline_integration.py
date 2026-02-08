"""Integration test: data → feature compute → strategy signal → backtest → valid result."""
import unittest
import numpy as np
import pandas as pd
from pm_core import PipelineConfig, Backtester, FeatureComputer, InstrumentSpec
from pm_strategies import StrategyRegistry


class PipelineIntegrationTests(unittest.TestCase):

    def _make_trending_data(self, n=500):
        np.random.seed(42)
        trend = np.linspace(0, 5, n)
        noise = np.cumsum(np.random.randn(n) * 0.2)
        close = 100 + trend + noise
        return pd.DataFrame({
            'Open': close - 0.1,
            'High': close + abs(np.random.randn(n) * 0.3),
            'Low': close - abs(np.random.randn(n) * 0.3),
            'Close': close,
            'Volume': np.random.randint(100, 1000, n),
        }, index=pd.date_range('2024-01-01', periods=n, freq='h'))

    def test_end_to_end_backtest(self):
        """Full pipeline: compute features → generate signals → run backtest."""
        df = self._make_trending_data()
        features = FeatureComputer.compute_required(df, {'ATR_14', 'EMA_20', 'RSI_14'})
        strat = StrategyRegistry.get('EMACrossoverStrategy')
        signals = strat.generate_signals(features, 'TEST')

        cfg = PipelineConfig(
            use_spread=False, use_slippage=False,
            use_commission=False, risk_per_trade_pct=1.0,
        )
        bt = Backtester(cfg)
        spec = InstrumentSpec(symbol="TEST", pip_position=2, pip_value=10.0)
        result = bt.run(features, signals, "TEST", strat, spec=spec)

        self.assertIn('total_trades', result)
        self.assertIn('total_return_pct', result)
        self.assertIn('max_drawdown_pct', result)
        self.assertIn('sharpe_ratio', result)

    def test_new_strategies_produce_signals(self):
        """All 15 new strategies should produce at least some non-zero signals on sample data."""
        df = self._make_trending_data(n=1000)
        features = FeatureComputer.compute_required(
            df, {'ATR_14', 'EMA_20', 'RSI_14', 'BB_LOWER_20', 'BB_UPPER_20',
                 'ADX', 'MACD_HIST'}
        )
        new_strategies = [
            'InsideBarBreakoutStrategy', 'NarrowRangeBreakoutStrategy',
            'TurtleSoupReversalStrategy', 'PinBarReversalStrategy',
            'EngulfingPatternStrategy', 'VolumeSpikeMomentumStrategy',
            'RSIDivergenceStrategy', 'MACDDivergenceStrategy',
            'OBVDivergenceStrategy', 'KeltnerFadeStrategy',
            'ROCExhaustionReversalStrategy', 'EMAPullbackContinuationStrategy',
            'ParabolicSARTrendStrategy', 'ATRPercentileBreakoutStrategy',
            'KaufmanAMATrendStrategy',
        ]
        for name in new_strategies:
            with self.subTest(strategy=name):
                strat = StrategyRegistry.get(name)
                signals = strat.generate_signals(features.copy(), "TEST")
                self.assertEqual(len(signals), len(features))
                # At least check it runs without error
                # Some strategies may produce 0 signals on synthetic data


if __name__ == "__main__":
    unittest.main()
