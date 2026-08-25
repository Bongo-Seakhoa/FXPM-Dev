import unittest
import numpy as np
import pandas as pd

import pm_core
from pm_core import PipelineConfig, Backtester, InstrumentSpec, StrategyCategory
from pm_strategies import BaseStrategy


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


class WideStopStrategy(DummyStrategy):
    name = "WideStop"

    def calculate_stops(self, features, signal, symbol, spec=None, bar_index=None):
        return 100.0, 100.0


class InheritedAtrStopStrategy(BaseStrategy):
    @property
    def name(self):
        return "InheritedAtrStop"

    @property
    def category(self):
        return StrategyCategory.TREND

    def get_default_params(self):
        return {
            "sl_atr_mult": 1.0,
            "tp_atr_mult": 2.0,
            "use_structural_sl": False,
        }

    def generate_signals(self, features, symbol):
        signals = pd.Series(0, index=features.index, dtype=int)
        signals.iloc[10::11] = 1
        signals.iloc[16::13] = -1
        return signals


class ForcedSlowAtrStopStrategy(InheritedAtrStopStrategy):
    def build_trade_intent(self, *args, **kwargs):
        return super().build_trade_intent(*args, **kwargs)


class BacktesterTests(unittest.TestCase):
    def test_python_loop_invalid_entry_sizing_keeps_equity_curve_aligned(self):
        cfg = PipelineConfig(
            initial_capital=10000.0,
            use_spread=False,
        )
        bt = Backtester(cfg)
        n = 12
        index = pd.date_range("2024-01-01", periods=n, freq="h")
        features = pd.DataFrame({
            "Open": [1.10] * n,
            "High": [1.11] * n,
            "Low": [1.09] * n,
            "Close": [1.10] * n,
            "Volume": [100] * n,
        }, index=index)
        sig_arr = np.ones(n, dtype=np.int32)
        entry_prices = np.full(n, 1.10, dtype=np.float64)
        sl_prices = np.full(n, 1.09, dtype=np.float64)
        tp_prices = np.full(n, 1.11, dtype=np.float64)
        spec = InstrumentSpec(symbol="EURUSD", pip_position=4, pip_value=0.0, spread_avg=0.0)

        trades, final_equity, max_dd, equity_curve = bt._run_python_loop(
            features,
            features["Open"].to_numpy(dtype=np.float64),
            features["High"].to_numpy(dtype=np.float64),
            features["Low"].to_numpy(dtype=np.float64),
            features["Close"].to_numpy(dtype=np.float64),
            sig_arr,
            sl_prices,
            tp_prices,
            entry_prices,
            np.zeros(n, dtype=np.float64),
            spec,
        )

        self.assertEqual(trades, [])
        self.assertEqual(final_equity, cfg.initial_capital)
        self.assertEqual(max_dd, 0.0)
        self.assertEqual(len(equity_curve), n)
        self.assertTrue(all(value == cfg.initial_capital for value in equity_curve))

    def test_numba_loop_invalid_entry_sizing_does_not_leave_phantom_zero(self):
        if not pm_core.NUMBA_AVAILABLE:
            self.skipTest("Numba not available")

        n = 12
        open_arr = np.full(n, 1.10, dtype=np.float64)
        high_arr = np.full(n, 1.11, dtype=np.float64)
        low_arr = np.full(n, 1.09, dtype=np.float64)
        close_arr = np.full(n, 1.10, dtype=np.float64)
        sig_arr = np.ones(n, dtype=np.int32)
        entry_prices = np.full(n, 1.10, dtype=np.float64)
        sl_prices = np.full(n, 1.09, dtype=np.float64)
        tp_prices = np.full(n, 1.11, dtype=np.float64)

        result = pm_core._backtest_loop_numba(
            open_arr,
            high_arr,
            low_arr,
            close_arr,
            sig_arr,
            sl_prices,
            tp_prices,
            entry_prices,
            10000.0,
            1.0,
            np.zeros(n, dtype=np.float64),
            False,
            0.00001,
            0.0,
            0.0001,
            0.0,
            0.01,
            100.0,
            0.01,
            0,
        )
        equity_curve = result[13]
        final_equity = result[14]
        max_dd = result[15]

        self.assertEqual(len(equity_curve), n)
        self.assertEqual(final_equity, 10000.0)
        self.assertEqual(max_dd, 0.0)
        self.assertTrue(np.all(equity_curve == 10000.0))

    def test_trade_excursion_fields_match_python_and_numba(self):
        cfg = PipelineConfig(
            initial_capital=10000.0,
            use_spread=False,
        )
        bt = Backtester(cfg)
        index = pd.date_range("2024-01-01", periods=5, freq="h")
        features = pd.DataFrame({
            "Open": [1.1000, 1.1000, 1.1000, 1.1000, 1.1010],
            "High": [1.1000, 1.1005, 1.1008, 1.1010, 1.1010],
            "Low": [1.1000, 1.0996, 1.0997, 1.1000, 1.1010],
            "Close": [1.1000, 1.0998, 1.1002, 1.1010, 1.1010],
            "Volume": [100] * 5,
        }, index=index)
        open_arr = features["Open"].to_numpy(dtype=np.float64)
        high_arr = features["High"].to_numpy(dtype=np.float64)
        low_arr = features["Low"].to_numpy(dtype=np.float64)
        close_arr = features["Close"].to_numpy(dtype=np.float64)
        sig_arr = np.zeros(5, dtype=np.int32)
        sig_arr[0] = 1
        entry_prices = np.full(5, np.nan, dtype=np.float64)
        sl_prices = np.full(5, np.nan, dtype=np.float64)
        tp_prices = np.full(5, np.nan, dtype=np.float64)
        entry_prices[1] = 1.1000
        sl_prices[1] = 1.0990
        tp_prices[1] = 1.1010
        spec = InstrumentSpec(symbol="EURUSD", pip_position=4, pip_value=10.0, spread_avg=0.0)

        trades, *_ = bt._run_python_loop(
            features,
            open_arr,
            high_arr,
            low_arr,
            close_arr,
            sig_arr,
            sl_prices,
            tp_prices,
            entry_prices,
            np.zeros(5, dtype=np.float64),
            spec,
        )

        self.assertEqual(len(trades), 1)
        py_trade = trades[0]
        self.assertEqual(py_trade["trade_record_schema_version"], pm_core.BACKTEST_TRADE_RECORD_SCHEMA_VERSION)
        self.assertAlmostEqual(py_trade["mae_pips"], 4.0, places=2)
        self.assertAlmostEqual(py_trade["mfe_pips"], 10.0, places=2)
        self.assertAlmostEqual(py_trade["mae_r"], 0.4, places=3)
        self.assertAlmostEqual(py_trade["mfe_r"], 1.0, places=3)
        self.assertEqual(py_trade["time_to_mae_bars"], 0)
        self.assertEqual(py_trade["time_to_mfe_bars"], 2)
        self.assertEqual(py_trade["time_underwater_bars"], 1)
        self.assertAlmostEqual(py_trade["entry_to_stop_pips"], 10.0, places=2)
        self.assertAlmostEqual(py_trade["entry_to_target_pips"], 10.0, places=2)

        if not pm_core.NUMBA_AVAILABLE:
            return
        result = pm_core._backtest_loop_numba(
            open_arr,
            high_arr,
            low_arr,
            close_arr,
            sig_arr,
            sl_prices,
            tp_prices,
            entry_prices,
            10000.0,
            1.0,
            np.zeros(5, dtype=np.float64),
            False,
            0.00001,
            1.0,
            0.0001,
            10.0,
            0.01,
            100.0,
            0.01,
            0,
        )
        self.assertEqual(len(result[0]), 1)
        self.assertAlmostEqual(float(result[16][0]), 4.0, places=2)
        self.assertAlmostEqual(float(result[17][0]), 10.0, places=2)
        self.assertAlmostEqual(float(result[18][0]), 0.4, places=3)
        self.assertAlmostEqual(float(result[19][0]), 1.0, places=3)
        self.assertEqual(int(result[20][0]), 0)
        self.assertEqual(int(result[21][0]), 2)
        self.assertEqual(int(result[22][0]), 1)
        self.assertAlmostEqual(float(result[23][0]), 10.0, places=2)
        self.assertAlmostEqual(float(result[24][0]), 10.0, places=2)

    def test_warmup_prefix_is_excluded_from_bar_return_risk_metrics(self):
        cfg = PipelineConfig(initial_capital=100.0)
        bt = Backtester(cfg)
        trades = [{
            "pnl_dollars": 3.0,
            "pnl_pips": 3.0,
            "r_multiple": 1.0,
            "exit_reason": "tp",
            "direction": "LONG",
            "entry_bar": 50,
            "exit_bar": 55,
        }]
        compact_curve = [100.0, 101.0, 99.0, 102.0, 101.0, 103.0]
        warm_curve = [100.0] * 50 + [101.0, 99.0, 102.0, 101.0, 103.0]

        compact = bt._calculate_metrics(
            trades,
            103.0,
            compact_curve,
            2.0,
            timeframe="H1",
            warmup_bars=0,
        )
        warmed = bt._calculate_metrics(
            trades,
            103.0,
            warm_curve,
            2.0,
            timeframe="H1",
            warmup_bars=50,
        )

        self.assertEqual(warmed["sharpe_ratio"], compact["sharpe_ratio"])
        self.assertEqual(warmed["sortino_ratio"], compact["sortino_ratio"])

    def test_r_multiple_confidence_metrics_are_reported(self):
        cfg = PipelineConfig(initial_capital=100.0)
        bt = Backtester(cfg)
        trades = []
        for idx, r in enumerate([1.0, -1.0, 2.0]):
            trades.append({
                "pnl_dollars": r,
                "pnl_pips": r,
                "r_multiple": r,
                "exit_reason": "tp" if r > 0 else "sl",
                "direction": "LONG",
                "entry_bar": idx * 10,
                "exit_bar": idx * 10 + 3,
            })

        metrics = bt._calculate_metrics(
            trades,
            102.0,
            [100.0, 101.0, 100.0, 102.0],
            1.0,
            timeframe="H1",
        )
        r_arr = np.asarray([1.0, -1.0, 2.0], dtype=np.float64)
        expected_std = float(np.std(r_arr, ddof=1))
        expected_se = expected_std / np.sqrt(3.0)
        expected_t = float(np.mean(r_arr) / expected_se)

        self.assertEqual(metrics["r_count"], 3)
        self.assertAlmostEqual(metrics["std_r"], expected_std, places=6)
        self.assertAlmostEqual(metrics["se_r"], expected_se, places=6)
        self.assertAlmostEqual(metrics["edge_t_stat_r"], expected_t, places=6)

    def test_default_atr_stop_fast_path_matches_slow_trade_intent_path(self):
        cfg = PipelineConfig(
            use_spread=True,
            risk_per_trade_pct=1.0,
            backtest_spread_mode="fixed_spec",
        )
        bt = Backtester(cfg)
        n = 240
        index = pd.date_range("2024-01-01", periods=n, freq="h")
        base = 1.1000 + np.sin(np.arange(n) / 8.0) * 0.002
        df = pd.DataFrame({
            "Open": base,
            "High": base + 0.0020,
            "Low": base - 0.0020,
            "Close": base + np.cos(np.arange(n) / 6.0) * 0.0004,
            "Volume": [100] * n,
            "ATR_14": [0.0015] * n,
        }, index=index)
        spec = InstrumentSpec(symbol="EURUSD", pip_position=4, pip_value=10.0, spread_avg=0.8)

        fast_strategy = InheritedAtrStopStrategy()
        slow_strategy = ForcedSlowAtrStopStrategy()
        signals = fast_strategy.generate_signals(df, "EURUSD")

        fast = bt.run(df, signals, "EURUSD", fast_strategy, spec=spec)
        slow = bt.run(df, signals, "EURUSD", slow_strategy, spec=spec)

        self.assertEqual(fast["trades"], slow["trades"])
        self.assertEqual(fast["equity_curve"], slow["equity_curve"])
        self.assertEqual(fast["final_equity"], slow["final_equity"])
        fast_summary = {k: v for k, v in fast.items() if k not in {"trades", "equity_curve"}}
        slow_summary = {k: v for k, v in slow.items() if k not in {"trades", "equity_curve"}}
        self.assertEqual(fast_summary, slow_summary)

    def test_entry_timing_and_exit(self):
        cfg = PipelineConfig(
            use_spread=False,
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
        self.assertEqual(t0["trade_record_schema_version"], pm_core.BACKTEST_TRADE_RECORD_SCHEMA_VERSION)
        # Ensure entry_time is after signal_time
        self.assertTrue(t0["entry_time"] > t0["signal_time"])

    def test_same_bar_exit_is_possible_after_entry(self):
        cfg = PipelineConfig(
            use_spread=False,
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

    def test_discovery_risk_is_independent_from_pipeline_risk(self):
        cfg = PipelineConfig(
            use_spread=False,
            risk_per_trade_pct=5.0,
            discovery_risk_per_trade_pct=1.0,
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

        self.assertAlmostEqual(res["trades"][0]["risk_amount"], 100.0)

    def test_signal_index_mismatch_raises(self):
        cfg = PipelineConfig(use_spread=False)
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

    def test_historical_spread_mode_uses_bar_spread_column(self):
        idx = pd.date_range("2024-01-01", periods=3, freq="h")
        df = pd.DataFrame({
            "Open": [100.0, 100.0, 100.0],
            "High": [100.0, 100.05, 100.05],
            "Low": [100.0, 99.95, 99.95],
            "Close": [100.0, 100.10, 100.20],
            "Volume": [100, 100, 100],
            # MT5 spread points. With point=0.01 and pip_size=0.01 this is 10 pips.
            "Spread": [10.0, 10.0, 10.0],
        }, index=idx)
        spec = InstrumentSpec(
            symbol="TEST",
            pip_position=2,
            pip_value=10.0,
            spread_avg=0.0,
            point=0.01,
            tick_size=0.01,
            tick_value=10.0,
        )
        strat = WideStopStrategy()
        signals = strat.generate_signals(df, "TEST")

        fixed = Backtester(PipelineConfig(
            use_spread=True,
            backtest_spread_mode="fixed_spec",
        )).run(df, signals, "TEST", strat, spec=spec)
        historical = Backtester(PipelineConfig(
            use_spread=True,
            backtest_spread_mode="historical_bar",
        )).run(df, signals, "TEST", strat, spec=spec)

        self.assertGreater(fixed["trades"][0]["pnl_pips"], historical["trades"][0]["pnl_pips"])
        self.assertAlmostEqual(fixed["trades"][0]["pnl_pips"], 20.0, places=6)
        self.assertAlmostEqual(historical["trades"][0]["pnl_pips"], 10.0, places=6)
        self.assertEqual(historical["spread_model"]["source"], "historical_spread_points")
        self.assertAlmostEqual(historical["spread_model"]["mean_effective_spread_price"], 0.10, places=8)
        self.assertAlmostEqual(historical["spread_model"]["mean_effective_spread_cash_per_lot"], 100.0, places=8)

    def test_stress_max_spread_mode_charges_max_spread_at_entry(self):
        idx = pd.date_range("2024-01-01", periods=3, freq="h")
        df = pd.DataFrame({
            "Open": [100.0, 100.0, 100.0],
            "High": [100.0, 100.05, 100.05],
            "Low": [100.0, 99.95, 99.95],
            "Close": [100.0, 100.10, 100.20],
            "Volume": [100, 100, 100],
            "Spread": [2.0, 2.0, 2.0],
            "SpreadOpen": [2.0, 2.0, 2.0],
            "SpreadP90": [6.0, 6.0, 6.0],
            "SpreadP95": [8.0, 8.0, 8.0],
            "SpreadMax": [10.0, 10.0, 10.0],
        }, index=idx)
        spec = InstrumentSpec(
            symbol="TEST",
            pip_position=2,
            pip_value=10.0,
            spread_avg=0.0,
            point=0.01,
            tick_size=0.01,
            tick_value=10.0,
        )

        conservative_entry, conservative_bar, conservative_meta = Backtester(
            PipelineConfig(use_spread=True, backtest_spread_mode="conservative_max")
        )._build_half_spread_arrays(df, spec)
        stress_p90_entry, stress_p90_bar, stress_p90_meta = Backtester(
            PipelineConfig(use_spread=True, backtest_spread_mode="stress_p90")
        )._build_half_spread_arrays(df, spec)
        stress_p95_entry, stress_p95_bar, stress_p95_meta = Backtester(
            PipelineConfig(use_spread=True, backtest_spread_mode="stress_p95")
        )._build_half_spread_arrays(df, spec)
        stress_entry, stress_bar, stress_meta = Backtester(
            PipelineConfig(use_spread=True, backtest_spread_mode="stress_max")
        )._build_half_spread_arrays(df, spec)

        self.assertAlmostEqual(conservative_entry[1], 0.01, places=8)
        self.assertAlmostEqual(conservative_bar[1], 0.05, places=8)
        self.assertEqual(conservative_meta["entry_column"], "SpreadOpen")
        self.assertAlmostEqual(stress_p90_entry[1], 0.03, places=8)
        self.assertAlmostEqual(stress_p90_bar[1], 0.05, places=8)
        self.assertEqual(stress_p90_meta["entry_column"], "SpreadP90")
        self.assertEqual(stress_p90_meta["entry_model"], "p90_spread_stress")
        self.assertAlmostEqual(stress_p95_entry[1], 0.04, places=8)
        self.assertAlmostEqual(stress_p95_bar[1], 0.05, places=8)
        self.assertEqual(stress_p95_meta["entry_column"], "SpreadP95")
        self.assertEqual(stress_p95_meta["entry_model"], "p95_spread_stress")
        self.assertAlmostEqual(stress_entry[1], 0.05, places=8)
        self.assertAlmostEqual(stress_bar[1], 0.05, places=8)
        self.assertEqual(stress_meta["entry_column"], "SpreadMax")
        self.assertEqual(stress_meta["entry_model"], "max_spread_stress")

    def test_percentile_stress_falls_back_to_max_not_open(self):
        idx = pd.date_range("2024-01-01", periods=3, freq="h")
        df = pd.DataFrame({
            "Open": [100.0, 100.0, 100.0],
            "High": [100.0, 100.05, 100.05],
            "Low": [100.0, 99.95, 99.95],
            "Close": [100.0, 100.10, 100.20],
            "Volume": [100, 100, 100],
            "Spread": [2.0, 2.0, 2.0],
            "SpreadOpen": [2.0, 2.0, 2.0],
            "SpreadMax": [10.0, 10.0, 10.0],
        }, index=idx)
        spec = InstrumentSpec(
            symbol="TEST",
            pip_position=2,
            pip_value=10.0,
            spread_avg=0.0,
            point=0.01,
            tick_size=0.01,
            tick_value=10.0,
        )

        stress_entry, stress_bar, stress_meta = Backtester(
            PipelineConfig(use_spread=True, backtest_spread_mode="stress_p95")
        )._build_half_spread_arrays(df, spec)

        self.assertAlmostEqual(stress_entry[1], 0.05, places=8)
        self.assertAlmostEqual(stress_bar[1], 0.05, places=8)
        self.assertEqual(stress_meta["entry_column"], "SpreadMax")
        self.assertEqual(stress_meta["entry_model"], "max_spread_stress")
        self.assertTrue(stress_meta["stress_entry_fallback"])

    def test_non_fx_spread_outlier_is_flagged_for_provenance_review(self):
        idx = pd.date_range("2024-01-01", periods=3, freq="h")
        df = pd.DataFrame({
            "Open": [100.0, 100.0, 100.0],
            "High": [100.0, 100.05, 100.05],
            "Low": [100.0, 99.95, 99.95],
            "Close": [100.0, 100.10, 100.20],
            "Volume": [100, 100, 100],
            "Spread": [200.0, 200.0, 200.0],
            "SpreadOpen": [200.0, 200.0, 200.0],
            "SpreadMax": [200.0, 200.0, 200.0],
        }, index=idx)
        spec = InstrumentSpec(
            symbol="DOGUSD",
            pip_position=2,
            pip_value=10.0,
            spread_avg=2.0,
            point=0.01,
            tick_size=0.01,
            tick_value=1.0,
            asset_class="crypto",
        )

        _, _, meta = Backtester(PipelineConfig(
            use_spread=True,
            backtest_spread_mode="conservative_max",
        ))._build_half_spread_arrays(df, spec)

        self.assertEqual(meta["asset_class"], "crypto")
        self.assertGreaterEqual(meta["mean_to_broker_spread_ratio"], 5.0)
        self.assertEqual(meta["spread_provenance_status"], "non_fx_spread_outlier_review")


if __name__ == "__main__":
    unittest.main()
