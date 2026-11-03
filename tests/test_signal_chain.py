"""Chain builder against the unchanged account kernel on an invented price path.

The path is designed so every occupancy case the kernel distinguishes occurs
at least once: a signal during an open trade, a gap-open stop that frees the
slot for an entry at the same open, a same-bar exit after entry, an intrabar
stop that blocks an entry at that bar's open, a right-censored tail, and a
signal whose next open is past the cutoff.  Nothing here is market data and
no outcome here is evidence about any strategy.
"""

import hashlib
import math
import unittest

import numpy as np
import pandas as pd

from pm_core import Backtester, InstrumentSpec, PipelineConfig
import pm_signal_chain as chain
from pm_signal_opportunities import OpportunityProvenance, label_signal_opportunities


class FixedStops:
    name = "SYNTHETIC_FIXED_STOPS"
    params = {}

    def calculate_stops(self, features, signal, symbol, *, spec, bar_index):
        return 5.0, 5.0


ROWS = [
    (1.0000, 1.0001, 0.9999, 1.0000),  # 0  signal +1 TREND -> A enters at 1
    (1.0000, 1.0003, 0.9999, 1.0002),  # 1  signal +1 blocked (A open)
    (1.0002, 1.0006, 1.0001, 1.0005),  # 2  A target; signal +1 -> A2 enters at 3
    (1.0005, 1.0006, 1.0004, 1.0005),  # 3
    (1.0005, 1.0006, 1.0004, 1.0005),  # 4  signal +1 -> B enters at 5 (gap-open frees the slot)
    (0.9995, 0.9998, 0.9993, 0.9996),  # 5  A2 gap-open stop; B enters; signal +1 blocked
    (0.9996, 1.0001, 0.9995, 1.0000),  # 6  B target; signal +1 -> C enters at 7
    (1.0000, 1.0001, 0.9994, 0.9995),  # 7  C same-bar stop; signal +1 -> D enters at 8
    (0.9995, 0.9997, 0.9994, 0.9996),  # 8  signal +1 -> entry at 9 blocked by D's intrabar stop
    (0.9996, 0.9997, 0.9989, 0.9990),  # 9  D intrabar stop; signal +1 -> E enters at 10
    (0.9990, 0.9992, 0.9989, 0.9991),  # 10 signal -1 RANGE -> F (short) enters at 11
    (0.9991, 0.9993, 0.9990, 0.9992),  # 11 signal +1 TREND blocked (E open)
    (0.9992, 0.9994, 0.9991, 0.9993),  # 12 signal +1 RANGE blocked (F open)
    (0.9993, 0.9994, 0.9992, 0.9993),  # 13 signal +1 TREND -> no next observation
]
SIGNALS = [1, 1, 1, 0, 1, 1, 1, 1, 1, 1, -1, 1, 1, 1]
REGIMES = ["TREND"] * 14
REGIMES[10] = "RANGE"
REGIMES[12] = "RANGE"


def _sha(text):
    return hashlib.sha256(text.encode("ascii")).hexdigest()


def _spec():
    return InstrumentSpec("SYNTHETIC", pip_position=4, spread_avg=0.4, pip_value=10.0,
                          asset_class="fx", min_lot=0.01, max_lot=100.0, volume_step=0.01,
                          swap_long=0.0, swap_short=0.0)


def _frame():
    index = pd.date_range("2024-01-01", periods=len(ROWS), freq="h", tz="UTC")
    frame = pd.DataFrame(ROWS, columns=["Open", "High", "Low", "Close"], index=index)
    frame["_BAR_CLOSE_TS"] = index + pd.Timedelta(hours=1)
    frame["REGIME_LIVE"] = REGIMES
    frame["ATR_14"] = 0.0005
    frame["Spread"] = 0
    return frame


def _batch(frame, spec, strategy):
    half = spec.get_half_spread_price()
    provenance = OpportunityProvenance(
        strategy_id=strategy.name, configuration_sha256=_sha("SYNTHETIC_CONFIG"),
        source_surface_sha256=_sha("SYNTHETIC_SURFACE"), spread_tier="P75",
        spread_model_sha256=_sha("SYNTHETIC_HALF_SPREAD"),
        admission_policy_sha256=_sha("SYNTHETIC_ADMISSION"))
    return label_signal_opportunities(
        frame, pd.Series(SIGNALS, index=frame.index), strategy=strategy, spec=spec,
        symbol=spec.symbol, timeframe="H1", cutoff_utc=frame["_BAR_CLOSE_TS"].iloc[-1],
        entry_half_spreads=np.full(len(frame), half), exit_half_spreads=np.full(len(frame), half),
        entry_admission=["ADMIT"] * len(frame), provenance=provenance)


def _kernel_trades(frame, spec, strategy, regime):
    signals = pd.Series(SIGNALS, index=frame.index)
    if regime is not None:
        signals = signals.where(frame["REGIME_LIVE"] == regime, 0).astype(int)
    config = PipelineConfig(initial_capital=10000.0, risk_per_trade_pct=1.0,
                            discovery_risk_per_trade_pct=1.0, use_spread=True,
                            backtest_spread_mode="fixed_spec", swap_model_enabled=False,
                            research_mode=True)
    result = Backtester(config).run(frame, signals, spec.symbol, strategy, spec=spec, timeframe="H1")
    rows = []
    for trade in result["trades"]:
        reason = str(trade["exit_reason"]).upper()
        if "SL" in reason:
            reason = "SL"
        elif "TP" in reason:
            reason = "TP"
        elif "EOD" in reason or "END_OF_DATA" in reason:
            reason = "EOD"
        rows.append({
            "signal_bar": int(trade["signal_bar"]), "entry_bar": int(trade["entry_bar"]),
            "exit_bar": int(trade["exit_bar"]),
            "direction": 1 if trade["direction"] == "LONG" else -1,
            "entry_price": float(trade["entry_price"]), "exit_price": float(trade["exit_price"]),
            "exit_reason": reason,
        })
    return rows


def _comparable(chain_rows, kernel_rows):
    """Closed trades match exactly; a censored chain trade matches the kernel's EOD liquidation."""
    chain_closed = [dict(row, status=None) for row in chain_rows if row["status"] == "CLOSED"]
    kernel_closed = [row for row in kernel_rows if row["exit_reason"] != "EOD"]
    for row in chain_closed:
        row.pop("status")
    chain_open = [row for row in chain_rows if row["status"] == "RIGHT_CENSORED"]
    kernel_open = [row for row in kernel_rows if row["exit_reason"] == "EOD"]
    open_pairs = ([(r["signal_bar"], r["entry_bar"], r["direction"], r["entry_price"]) for r in chain_open],
                  [(r["signal_bar"], r["entry_bar"], r["direction"], r["entry_price"]) for r in kernel_open])
    return chain_closed, kernel_closed, open_pairs


class ChainAgainstKernelTests(unittest.TestCase):
    def setUp(self):
        self.spec = _spec()
        self.strategy = FixedStops()
        self.frame = _frame()
        self.batch = _batch(self.frame, self.spec, self.strategy)

    def test_fixture_exercises_every_case(self):
        statuses = {record.signal_bar: record.status for record in self.batch.records}
        fills = {record.signal_bar: record.exit_fill for record in self.batch.records}
        self.assertEqual(statuses[13], "NO_NEXT_OBSERVATION")
        self.assertEqual(fills[2], "GAP_OPEN")       # A2 (signal 2) stopped by the gap at bar 5
        self.assertEqual(fills[6], "STOP_LEVEL")     # C (signal 6) same-bar stop at bar 7
        self.assertEqual(fills[7], "STOP_LEVEL")     # D (signal 7) intrabar stop at bar 9
        self.assertEqual(statuses[9], "RIGHT_CENSORED")
        self.assertEqual(statuses[10], "RIGHT_CENSORED")

    def test_trend_chain_matches_gated_kernel_byte_for_byte(self):
        trend = chain.build_regime_chain(self.batch, "TREND")
        chain_closed, kernel_closed, open_pairs = _comparable(trend.trade_list(),
                                                              _kernel_trades(self.frame, self.spec, self.strategy, "TREND"))
        self.assertEqual(chain.canonical_bytes(chain_closed), chain.canonical_bytes(kernel_closed))
        self.assertEqual(open_pairs[0], open_pairs[1])
        self.assertEqual([t.signal_bar for t in trend.taken], [0, 2, 4, 6, 7, 9])
        self.assertEqual({(s.signal_bar, s.reason) for s in trend.skipped},
                         {(1, "BLOCKED_BY_OPEN_TRADE"), (5, "BLOCKED_BY_OPEN_TRADE"),
                          (8, "BLOCKED_BY_OPEN_TRADE"), (11, "BLOCKED_BY_OPEN_TRADE"),
                          (13, "NO_NEXT_OBSERVATION")})

    def test_range_chain_matches_gated_kernel(self):
        rng = chain.build_regime_chain(self.batch, "RANGE")
        chain_closed, kernel_closed, open_pairs = _comparable(rng.trade_list(),
                                                              _kernel_trades(self.frame, self.spec, self.strategy, "RANGE"))
        self.assertEqual(chain_closed, kernel_closed, )
        self.assertEqual(open_pairs[0], open_pairs[1])
        self.assertEqual([t.signal_bar for t in rng.taken], [10])
        self.assertEqual(rng.taken[0].direction, -1)
        self.assertEqual({(s.signal_bar, s.reason) for s in rng.skipped}, {(12, "BLOCKED_BY_OPEN_TRADE")})

    def test_ungated_chain_matches_ungated_kernel(self):
        every = chain.build_regime_chain(self.batch, None)
        chain_closed, kernel_closed, open_pairs = _comparable(every.trade_list(),
                                                              _kernel_trades(self.frame, self.spec, self.strategy, None))
        self.assertEqual(chain.canonical_bytes(chain_closed), chain.canonical_bytes(kernel_closed))
        self.assertEqual(open_pairs[0], open_pairs[1])
        self.assertEqual(every.chain_statistics["n_skipped_blocked"], 6)

    def test_empty_regimes_and_all_regime_builder(self):
        chains = chain.build_all_regime_chains(self.batch)
        self.assertEqual(set(chains), set(chain.REGIMES))
        for regime in ("CHOP", "BREAKOUT"):
            self.assertEqual(chains[regime].taken, ())
            self.assertIsNone(chains[regime].chain_statistics["mean_r"])
            self.assertEqual(chains[regime].chain_statistics["n_closed"], 0)


class ChainStatisticsTests(unittest.TestCase):
    def setUp(self):
        self.spec = _spec()
        self.strategy = FixedStops()
        self.frame = _frame()
        self.batch = _batch(self.frame, self.spec, self.strategy)
        self.trend = chain.build_regime_chain(self.batch, "TREND")

    def test_r_multiples_and_lower_bound(self):
        closed = [t for t in self.trend.taken if t.status == "CLOSED"]
        values = [t.r_multiple for t in closed]
        self.assertEqual(len(values), 5)
        for trade in closed:
            self.assertAlmostEqual(trade.r_multiple, trade.signed_pips / trade.stop_loss_pips)
        stats = self.trend.chain_statistics
        n = len(values)
        mean = sum(values) / n
        sd = math.sqrt(sum((v - mean) ** 2 for v in values) / (n - 1))
        self.assertAlmostEqual(stats["total_r"], sum(values))
        self.assertAlmostEqual(stats["lcb_total_r"], sum(values) - chain.DEFAULT_Z * sd * math.sqrt(n))
        self.assertEqual(stats["n_taken"], 6)
        self.assertEqual(stats["n_censored"], 1)
        self.assertLessEqual(stats["worst_drawdown_r"], 0.0)
        self.assertEqual(stats["hit_rate"], sum(1 for v in values if v > 0) / n)

    def test_all_opportunity_statistics_count_blocked_signals(self):
        every = self.trend.all_opportunity_statistics
        # Every closed TREND opportunity counts, taken or blocked; the chain only counts the taken.
        self.assertGreater(every["n_closed"], self.trend.chain_statistics["n_closed"])
        self.assertIsNotNone(every["masking_gap_mean_r"])
        self.assertAlmostEqual(every["masking_gap_mean_r"],
                               every["mean_r"] - self.trend.chain_statistics["mean_r"])

    def test_deterministic_bytes_and_refusals(self):
        again = chain.build_regime_chain(self.batch, "TREND")
        self.assertEqual(again.to_bytes(), self.trend.to_bytes())
        self.assertEqual(again.sha256, self.trend.sha256)
        self.assertEqual(self.trend.lcb_method, "NORMAL_APPROX_ONE_SIDED_Z")
        self.assertEqual(self.trend.statistics_scope, chain.STATISTICS_SCOPE)
        self.assertEqual(self.trend.terminal_policy_disclosure, chain.TERMINAL_POLICY_DISCLOSURE)
        self.assertIn("statistics_scope", self.trend.to_dict())
        with self.assertRaises(chain.SignalChainError) as caught:
            chain.build_regime_chain(self.batch, "SIDEWAYS")
        self.assertEqual(str(caught.exception), "REGIME_INVALID")
        with self.assertRaises(chain.SignalChainError) as caught:
            chain.build_regime_chain(self.batch, "TREND", z=float("nan"))
        self.assertEqual(str(caught.exception), "Z_INVALID")
        with self.assertRaises(chain.SignalChainError) as caught:
            chain.build_regime_chain(object(), "TREND")
        self.assertEqual(str(caught.exception), "BATCH_TYPE_INVALID")


if __name__ == "__main__":
    unittest.main()
