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
            self.skipTest("Numba not available")
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

    def test_spread_on_gap_and_intrabar_exits_match_python_and_numba(self):
        if not pm_core.NUMBA_AVAILABLE:
            self.skipTest("Numba not available")

        cfg = PipelineConfig(initial_capital=10000.0, use_spread=True)
        bt = Backtester(cfg)
        index = pd.date_range("2024-01-01", periods=7, freq="h")
        features = pd.DataFrame(
            {
                "Open": [1.1000, 1.1000, 1.0988, 1.1000, 1.1000, 1.1000, 1.1000],
                "High": [1.1000, 1.1005, 1.0990, 1.1000, 1.1013, 1.1000, 1.1000],
                "Low": [1.1000, 1.0995, 1.0985, 1.1000, 1.0997, 1.1000, 1.1000],
                "Close": [1.1000, 1.1000, 1.0988, 1.1000, 1.1010, 1.1000, 1.1000],
                "Volume": [100] * 7,
            },
            index=index,
        )
        arrays = [
            features[name].to_numpy(dtype=np.float64)
            for name in ("Open", "High", "Low", "Close")
        ]
        signals = np.zeros(7, dtype=np.int32)
        signals[[0, 3]] = 1
        entry_prices = np.full(7, np.nan, dtype=np.float64)
        stop_prices = np.full(7, np.nan, dtype=np.float64)
        target_prices = np.full(7, np.nan, dtype=np.float64)
        entry_prices[[1, 4]] = 1.1001
        stop_prices[[1, 4]] = 1.0991
        target_prices[[1, 4]] = 1.1011
        spreads = np.full(7, 0.0001, dtype=np.float64)
        spec = InstrumentSpec(
            symbol="EURUSD",
            pip_position=4,
            pip_value=10.0,
            spread_avg=2.0,
            tick_size=0.0001,
            tick_value=10.0,
        )

        python_trades, python_final, python_drawdown, python_curve = (
            bt._run_python_loop(
                features,
                *arrays,
                signals,
                stop_prices,
                target_prices,
                entry_prices,
                spreads,
                spec,
            )
        )
        numba = pm_core._backtest_loop_numba(
            *arrays,
            signals,
            stop_prices,
            target_prices,
            entry_prices,
            10000.0,
            1.0,
            spreads,
            True,
            spec.tick_size,
            spec.tick_value,
            spec.pip_size,
            spec.pip_value,
            spec.min_lot,
            spec.max_lot,
            spec.volume_step,
            0,
        )

        self.assertEqual(
            [trade["exit_reason"] for trade in python_trades],
            ["closed_sl", "closed_tp"],
        )
        self.assertEqual(list(numba[11]), [pm_core.EXIT_SL, pm_core.EXIT_TP])
        self.assertTrue(
            np.allclose(
                [trade["exit_price"] for trade in python_trades],
                numba[5],
                rtol=0.0,
                atol=1e-12,
            )
        )
        self.assertTrue(
            np.allclose(
                [trade["pnl_pips"] for trade in python_trades],
                np.round(numba[10], 2),
                rtol=0.0,
                atol=1e-12,
            )
        )
        self.assertTrue(
            np.allclose(
                [trade["pnl_dollars"] for trade in python_trades],
                np.round(numba[9], 2),
                rtol=0.0,
                atol=1e-12,
            )
        )
        self.assertAlmostEqual(python_final, float(numba[14]), places=10)
        self.assertAlmostEqual(python_drawdown, float(numba[15]), places=10)
        self.assertTrue(np.allclose(python_curve, numba[13], rtol=0.0, atol=1e-10))

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
            # Synthetic outlier metadata remains a non-authorising diagnostic.
            backtest_spread_mode="historical_bar",
        ))._build_half_spread_arrays(df, spec)

        self.assertEqual(meta["asset_class"], "crypto")
        self.assertGreaterEqual(meta["mean_to_broker_spread_ratio"], 5.0)
        self.assertEqual(meta["spread_provenance_status"], "non_fx_spread_outlier_review")


if __name__ == "__main__":
    unittest.main()


# INVENTED admission metadata and prices; no native, model or financial evidence.
def _invented_admission_tier(*, tier="P75", points=12):
    import hashlib
    import json
    from pm_spread_profile import FrozenSpreadTier, TIER_SCHEMA
    label = "INVENTED_ENTRY_ADMISSION_SOFTWARE_ONLY"
    descriptor = {"schema_version": TIER_SCHEMA, "symbol": "EURUSD", "timeframe": "H1",
                  "tier": tier, "source_timeframe": "H1", "point_price": 0.00001,
                  "spread_points": points, "full_spread_price": points * 0.00001,
                  "profile_sha256": hashlib.sha256((label + ":profile").encode()).hexdigest(),
                  "source_identity_sha256": hashlib.sha256((label + ":source").encode()).hexdigest(),
                  "source_binding": {"fixture_provenance": label}, "qualification_authority": False}
    return FrozenSpreadTier(json.dumps(descriptor, sort_keys=True, separators=(",", ":")))


def _admission_scalar_counter(callee=None):
    from contextlib import contextmanager
    import sys
    original = pm_core.spread_admission if callee is None else callee
    code = original.__code__

    @contextmanager
    def count():
        calls = []
        previous = sys.getprofile()

        def observe(frame, event, arg):
            if event == "call" and frame.f_code is code:
                calls.append(True)

        sys.setprofile(observe)
        try:
            yield calls
        finally:
            sys.setprofile(previous)
    return count()


def _admission_call(observations, tier, preparation=None):
    frame = pd.DataFrame(index=observations.index)
    if preparation is None:
        return Backtester._entry_spread_admission_inputs(frame, tier, observations)
    return Backtester._entry_spread_admission_inputs(frame, tier, observations, _preparation=preparation)


def _invented_admission_run_inputs():
    index = pd.date_range("2024-01-01T00:00:00Z", periods=7, freq="h")
    frame = pd.DataFrame({"Open": [1.1000, 1.1000, 1.0988, 1.1000, 1.1000, 1.1000, 1.1000],
                          "High": [1.1000, 1.1005, 1.0990, 1.1000, 1.1013, 1.1000, 1.1000],
                          "Low": [1.1000, 1.0995, 1.0985, 1.1000, 1.0997, 1.1000, 1.1000],
                          "Close": [1.1000, 1.1000, 1.0988, 1.1000, 1.1010, 1.1000, 1.1000],
                          "Volume": 100.0, "ATR_14": 0.0005, "SpreadOpen": 9, "SpreadMax": 12,
                          "_BAR_CLOSE_TS": index + pd.Timedelta(hours=1)}, index=index)
    frame.attrs["fixture_provenance"] = "INVENTED_ENTRY_ADMISSION_RUN_SOFTWARE_ONLY"
    signals = pd.Series([1, 0, 0, 1, 0, 0, 0], index=index, dtype=np.int32)
    observations = pd.Series(9.0, index=index)
    config = PipelineConfig(initial_capital=10000.0, risk_per_trade_pct=1.0,
                            discovery_risk_per_trade_pct=1.0, use_spread=True,
                            backtest_spread_mode="conservative_max", swap_model_enabled=True,
                            research_mode=True)
    spec = InstrumentSpec("EURUSD", pip_position=4, pip_value=10.0, point=0.00001,
                          tick_size=0.00001, tick_value=1.0, swap_long=-2.0, swap_short=1.0)
    return frame, signals, observations, config, spec, _invented_admission_tier()


class EntryAdmissionPreparationTests(unittest.TestCase):
    def assert_admission_equal(self, actual, expected):
        self.assertEqual(actual[0].shape, expected[0].shape)
        self.assertEqual(actual[0].dtype, expected[0].dtype)
        self.assertEqual(actual[0].tobytes(), expected[0].tobytes())
        self.assertEqual(actual[1], expected[1])

    def test_lazy_overlap_counts_original_scalar_frames_only(self):
        index = pd.date_range("2024-01-01", periods=8, freq="h", tz="UTC")
        observations = pd.Series([1.0, 2.0, np.nan, 12.0, 13.0, -1.0, 12.5, np.inf], index=index)
        tier = _invented_admission_tier()
        first_expected = _admission_call(observations.iloc[:2], tier)
        overlap_expected = _admission_call(observations.iloc[1:5], tier)
        with _admission_scalar_counter() as calls:
            with pm_core._EntryAdmissionPreparation(observations) as preparation:
                self.assertEqual(len(calls), 0)
                self.assert_admission_equal(_admission_call(observations.iloc[:2], tier, preparation), first_expected)
                self.assertEqual(len(calls), 2)
                self.assert_admission_equal(_admission_call(observations.iloc[:2], tier, preparation), first_expected)
                self.assertEqual(len(calls), 2)
                self.assert_admission_equal(_admission_call(observations.iloc[1:5], tier, preparation), overlap_expected)
                self.assertEqual(len(calls), 5)
                self.assert_admission_equal(_admission_call(observations.iloc[1:5], tier, preparation), overlap_expected)
        self.assertEqual(preparation._labels_by_tier, {})

    def test_slice_dtype_bytes_and_complete_binding_match_static_oracle(self):
        index = pd.date_range("2024-01-01", periods=5, freq="h", tz="UTC")
        tier = _invented_admission_tier()
        for dtype, values in ((np.float64, [1, 2, np.nan, 12, 13]),
                              (np.float32, [1, 2, np.nan, 12, 13]), (np.int64, [1, 2, 3, 12, 13])):
            observations = pd.Series(values, index=index, dtype=dtype)
            with pm_core._EntryAdmissionPreparation(observations) as preparation:
                for positions in ([0, 1], [4], [2], [0, 2, 4], []):
                    with self.subTest(dtype=str(dtype), positions=positions):
                        subset = observations.iloc[positions]
                        expected = _admission_call(subset, tier)
                        self.assert_admission_equal(_admission_call(subset, tier, preparation), expected)

    def test_invalid_rows_outside_slice_do_not_refuse_and_slice_order_is_preserved(self):
        index = pd.date_range("2024-01-01", periods=5, freq="h", tz="UTC")
        observations = pd.Series([1.0, 2.0, 12.5, -1.0, np.inf], index=index)
        tier = _invented_admission_tier()
        with pm_core._EntryAdmissionPreparation(observations) as preparation:
            self.assert_admission_equal(_admission_call(observations.iloc[:2], tier, preparation),
                                        _admission_call(observations.iloc[:2], tier))
            for position in (2, 3, 4):
                subset = observations.iloc[position:position + 1]
                with self.assertRaises(ValueError) as expected:
                    _admission_call(subset, tier)
                with _admission_scalar_counter() as calls:
                    with self.assertRaises(ValueError) as actual:
                        _admission_call(subset, tier, preparation)
                self.assertEqual(str(actual.exception), str(expected.exception))
                if position == 2:
                    self.assertEqual(calls, [])  # Fractional validation precedes state creation.

    def test_unsupported_inputs_and_closed_pool_use_original_scalar_path(self):
        index = pd.date_range("2024-01-01", periods=2, freq="h", tz="UTC")
        tier = _invented_admission_tier()
        supported = pd.Series([1.0, 2.0], index=index)
        for full in (None, [1.0, 2.0], pd.Series([1, 2], index=index, dtype=object),
                     pd.Series([1, 2], index=index, dtype="Int64"), pd.Series([1, 2], index=pd.RangeIndex(2))):
            with self.subTest(full_type=type(full).__name__):
                with pm_core._EntryAdmissionPreparation(full) as preparation:
                    with _admission_scalar_counter() as calls:
                        actual = _admission_call(supported, tier, preparation)
                        again = _admission_call(supported, tier, preparation)
                    self.assertEqual(len(calls), 4)
                    self.assert_admission_equal(actual, _admission_call(supported, tier))
                    self.assert_admission_equal(again, actual)
        with pm_core._EntryAdmissionPreparation(supported) as closed:
            _admission_call(supported, tier, closed)
        with _admission_scalar_counter() as calls:
            _admission_call(supported, tier, closed)
        self.assertEqual(len(calls), 2)

    def test_selected_mutation_dtype_and_nan_bits_fall_back(self):
        index = pd.date_range("2024-01-01", periods=3, freq="h", tz="UTC")
        full = pd.Series([0.0, np.nan, 2.0], index=index)
        tier = _invented_admission_tier()
        with pm_core._EntryAdmissionPreparation(full) as preparation:
            _admission_call(full, tier, preparation)
            negative_zero = full.copy(deep=True)
            negative_zero.iloc[0] = -0.0
            changed_nan = full.copy(deep=True)
            changed_nan.to_numpy().view(np.uint64)[1] = np.uint64(0x7ff8000000000001)
            changed_value = full.copy(deep=True)
            changed_value.iloc[2] = 3.0
            for subset in (negative_zero, changed_nan, changed_value, full.astype(np.float32)):
                with self.subTest(dtype=str(subset.dtype), bits=subset.to_numpy().tobytes().hex()):
                    expected = _admission_call(subset, tier)
                    with _admission_scalar_counter() as calls:
                        actual = _admission_call(subset, tier, preparation)
                    self.assertEqual(len(calls), len(subset))
                    self.assert_admission_equal(actual, expected)

    def test_returned_state_and_binding_mutation_does_not_mutate_pool(self):
        index = pd.date_range("2024-01-01", periods=3, freq="h", tz="UTC")
        full = pd.Series([1.0, 2.0, 3.0], index=index)
        tier = _invented_admission_tier()
        expected = _admission_call(full.iloc[:2], tier)
        with pm_core._EntryAdmissionPreparation(full) as preparation:
            states, binding = _admission_call(full.iloc[:2], tier, preparation)
            states[:] = "XXXXX"
            binding["entry_observations_sha256"] = "changed"
            full.iloc[2] = 0.5  # An unrequested changed row must not poison this slice.
            with _admission_scalar_counter() as calls:
                actual = _admission_call(full.iloc[:2], tier, preparation)
            self.assertEqual(calls, [])
            self.assert_admission_equal(actual, expected)

    def test_exact_tier_identity_separates_equal_caps_and_wrapped_scalar_bypasses(self):
        from unittest.mock import patch
        index = pd.date_range("2024-01-01", periods=2, freq="h", tz="UTC")
        observations = pd.Series([1.0, 2.0], index=index)
        p75, p95 = _invented_admission_tier(), _invented_admission_tier(tier="P95")
        original = pm_core.spread_admission
        with pm_core._EntryAdmissionPreparation(observations) as preparation:
            with _admission_scalar_counter() as calls:
                _admission_call(observations, p75, preparation)
                _admission_call(observations, p95, preparation)
            self.assertEqual(len(calls), 4)
            self.assertEqual(len(preparation._labels_by_tier), 2)
            wrapped_calls = []

            def wrapped(tier, value):
                wrapped_calls.append(True)
                return original(tier, value)

            with patch.object(pm_core, "spread_admission", wrapped):
                first = _admission_call(observations, p75, preparation)
                second = _admission_call(observations, p75, preparation)
            self.assertEqual(len(wrapped_calls), 4)
            self.assert_admission_equal(first, second)

    def test_attached_pool_preserves_real_callbacks_and_fresh_run_evidence(self):
        frame, signals, observations, config, spec, tier = _invented_admission_run_inputs()

        class CountingStrategy(DummyStrategy):
            def __init__(self):
                self.stop_calls = 0

            def calculate_stops(self, *args, **kwargs):
                self.stop_calls += 1
                return super().calculate_stops(*args, **kwargs)

        strategy = CountingStrategy()
        backtester = Backtester(config)
        with pm_core._EntryAdmissionPreparation(observations) as preparation:
            with backtester._partition_entry_admission_reuse(preparation):
                with _admission_scalar_counter() as calls:
                    first = backtester.run(frame, signals, "EURUSD", strategy, spec=spec, timeframe="H1",
                                           spread_tier=tier, entry_spread_observations=observations)
                    first_calls = strategy.stop_calls
                    second = backtester.run(frame, signals, "EURUSD", strategy, spec=spec, timeframe="H1",
                                            spread_tier=tier, entry_spread_observations=observations)
                self.assertEqual(len(calls), 1)  # Seven identical raw observation patterns.
                self.assertGreater(first_calls, 0)
                self.assertEqual(strategy.stop_calls, 2 * first_calls)
            self.assertTrue(preparation._labels_by_tier)  # Binding exit does not own pool cleanup.
        for key in ("trades", "equity_curve", "position_events", "spread_admission",
                    "numerical_execution_cost_attestation", "swap_accrual_contract"):
            self.assertEqual(first[key], second[key])
        self.assertIsNot(first["numerical_execution_cost_attestation"], second["numerical_execution_cost_attestation"])

    def test_replaced_real_three_arg_helper_works_with_attached_pool(self):
        from unittest.mock import patch
        frame, signals, observations, config, spec, tier = _invented_admission_run_inputs()
        original = Backtester._entry_spread_admission_inputs
        replacement_calls = []

        def only_three_args(features, spread_tier, observations):
            replacement_calls.append(True)
            return original(features, spread_tier, observations)

        backtester = Backtester(config)
        with pm_core._EntryAdmissionPreparation(observations) as preparation:
            with backtester._partition_entry_admission_reuse(preparation):
                with patch.object(Backtester, "_entry_spread_admission_inputs", staticmethod(only_three_args)):
                    result = backtester.run(frame, signals, "EURUSD", DummyStrategy(), spec=spec, timeframe="H1",
                                            spread_tier=tier, entry_spread_observations=observations)
            self.assertEqual(replacement_calls, [True])
            self.assertEqual(preparation._labels_by_tier, {})
        self.assertIn("entry_admission_mask_sha256", result["spread_admission"])

    def test_static_default_path_does_not_inherit_instance_preparation(self):
        index = pd.date_range("2024-01-01", periods=2, freq="h", tz="UTC")
        observations = pd.Series([1.0, 2.0], index=index)
        tier = _invented_admission_tier()
        backtester = Backtester(PipelineConfig())
        with pm_core._EntryAdmissionPreparation(observations) as preparation:
            with backtester._partition_entry_admission_reuse(preparation):
                with _admission_scalar_counter() as calls:
                    first = _admission_call(observations, tier)
                    second = _admission_call(observations, tier)
            self.assertEqual(len(calls), 4)
            self.assertEqual(preparation._labels_by_tier, {})
        self.assert_admission_equal(first, second)

    def test_nested_binding_restores_absence_and_outer_without_clearing_shared_pool(self):
        index = pd.date_range("2024-01-01", periods=2, freq="h", tz="UTC")
        observations = pd.Series([1.0, 2.0], index=index)
        tier = _invented_admission_tier()
        first, second = Backtester(PipelineConfig()), Backtester(PipelineConfig())
        self.assertNotIn("_entry_admission_preparation", first.__dict__)
        with pm_core._EntryAdmissionPreparation(observations) as outer:
            with pm_core._EntryAdmissionPreparation(observations) as inner:
                with first._partition_entry_admission_reuse(outer):
                    self.assertIs(first._entry_admission_preparation, outer)
                    _admission_call(observations, tier, outer)
                    with self.assertRaisesRegex(RuntimeError, "INVENTED_EXCEPTION"):
                        with first._partition_entry_admission_reuse(inner):
                            self.assertIs(first._entry_admission_preparation, inner)
                            raise RuntimeError("INVENTED_EXCEPTION")
                    self.assertIs(first._entry_admission_preparation, outer)
                    with first._partition_entry_admission_reuse(None):
                        self.assertIsNone(first._entry_admission_preparation)
                    self.assertIs(first._entry_admission_preparation, outer)
                self.assertNotIn("_entry_admission_preparation", first.__dict__)
                self.assertTrue(outer._labels_by_tier)
                with second._partition_entry_admission_reuse(outer):
                    with _admission_scalar_counter() as calls:
                        _admission_call(observations, tier, second._entry_admission_preparation)
                self.assertEqual(calls, [])
                self.assertTrue(outer._labels_by_tier)
            self.assertTrue(inner._closed)
        self.assertTrue(outer._closed)
        self.assertEqual(outer._labels_by_tier, {})

    def test_axis_refusal_precedes_lookup_and_foreign_tier_falls_back(self):
        from types import SimpleNamespace
        index = pd.date_range("2024-01-01", periods=2, freq="h", tz="UTC")
        observations = pd.Series([1.0, 2.0], index=index)
        tier = _invented_admission_tier()
        mismatched = pd.DataFrame(index=index + pd.Timedelta(hours=1))
        with pm_core._EntryAdmissionPreparation(observations) as preparation:
            with _admission_scalar_counter() as calls:
                with self.assertRaisesRegex(ValueError, "ENTRY_SPREAD_OBSERVATIONS_AXIS_MISMATCH"):
                    Backtester._entry_spread_admission_inputs(mismatched, tier, observations,
                                                             _preparation=preparation)
            self.assertEqual(calls, [])
            foreign_tier = SimpleNamespace(spread_points=12)
            with _admission_scalar_counter() as calls:
                actual = _admission_call(observations, foreign_tier, preparation)
                repeated = _admission_call(observations, foreign_tier, preparation)
            self.assertEqual(len(calls), 4)
            self.assertEqual(preparation._labels_by_tier, {})
            self.assert_admission_equal(actual, repeated)

    def test_reordered_duplicate_slices_reuse_only_unique_new_positions(self):
        index = pd.date_range("2024-01-01", periods=6, freq="h", tz="UTC")
        full = pd.Series([1.0, np.nan, 13.0, 2.0, 12.0, 14.0], index=index)
        self.assertTrue(full.index.is_unique)
        tier = _invented_admission_tier()
        first = full.iloc[[4, 0, 4, 2]]
        overlap = full.iloc[[2, 5, 0, 5]]
        missing = full.iloc[[1, 4, 1]]
        expected_first = _admission_call(first, tier)
        expected_overlap = _admission_call(overlap, tier)
        expected_missing = _admission_call(missing, tier)
        with pm_core._EntryAdmissionPreparation(full) as preparation:
            with _admission_scalar_counter() as calls:
                self.assert_admission_equal(_admission_call(first, tier, preparation), expected_first)
                self.assertEqual(len(calls), 3)  # Three unique requested positions, four output rows.
                self.assert_admission_equal(_admission_call(first, tier, preparation), expected_first)
                self.assertEqual(len(calls), 3)
                self.assert_admission_equal(_admission_call(overlap, tier, preparation), expected_overlap)
                self.assertEqual(len(calls), 4)  # Only snapshot position 5 is newly requested.
                self.assert_admission_equal(_admission_call(overlap, tier, preparation), expected_overlap)
                self.assertEqual(len(calls), 4)
                self.assert_admission_equal(_admission_call(missing, tier, preparation), expected_missing)
                self.assertEqual(len(calls), 5)  # MISSING width is inferred from this slice only.

    def test_new_rows_group_only_identical_bits_preserving_zero_and_nan_payloads(self):
        import sys
        index = pd.date_range("2024-01-01", periods=7, freq="h", tz="UTC")
        # INVENTED native float64 patterns: equal rows, distinct zeros and NaN payloads.
        raw_bits = np.array([0x3ff0000000000000, 0x3ff0000000000000,
                             0x8000000000000000, 0x0000000000000000,
                             0x7ff8000000000001, 0x7ff8000000000001,
                             0x7ff8000000000002], dtype=np.uint64)
        observations = pd.Series(raw_bits.view(np.float64), index=index, copy=True)
        np.testing.assert_array_equal(observations.to_numpy().view(np.uint64), raw_bits)
        tier = _invented_admission_tier()
        expected = _admission_call(observations, tier)
        distinct_patterns = list(dict.fromkeys(int(value) for value in raw_bits))
        self.assertEqual(len(distinct_patterns), 5)
        original_code = pm_core.spread_admission.__code__
        observed_patterns = []
        previous = sys.getprofile()

        def observe(frame, event, arg):
            if event == "call" and frame.f_code is original_code:
                value = frame.f_locals["observation"]
                observed_patterns.append(int(np.asarray([value], dtype=np.float64).view(np.uint64)[0]))

        sys.setprofile(observe)
        try:
            with pm_core._EntryAdmissionPreparation(observations) as preparation:
                self.assertEqual(observed_patterns, [])
                actual = _admission_call(observations, tier, preparation)
                self.assert_admission_equal(actual, expected)
                self.assertCountEqual(observed_patterns, distinct_patterns)
                self.assertEqual(len(observed_patterns), 5)
                repeated = _admission_call(observations, tier, preparation)
                self.assert_admission_equal(repeated, expected)
                self.assertEqual(len(observed_patterns), 5)
        finally:
            sys.setprofile(previous)
