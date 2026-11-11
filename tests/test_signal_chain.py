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


# These envelopes are invented contract fixtures, never backtest-run evidence.
def _reconciliation_source_probe(result):
    from pm_trade_power import build_trade_sample_evidence
    import pm_winner_raw_evidence as raw
    source = raw.build_trade_source_contract_from_backtester_result(result)
    return build_trade_sample_evidence(
        [], source_contract=source,
        minimum_observations=raw.PRODUCTION_TRADE_POWER_MINIMUM_OBSERVATIONS,
    )


def _invented_reconciliation_fixture(*, tier="P75", end_utc="2024-01-13T00:00:00+00:00"):
    from copy import deepcopy
    from pm_core import build_backtest_partition_axis, build_backtest_spread_mode
    from pm_trade_power import build_numerical_execution_cost_attestation

    label = "INVENTED_CHAIN_ACCOUNT_CONTRACT_FIXTURE_NOT_MARKET_OR_RUN_EVIDENCE"
    end = pd.Timestamp(end_utc)
    index = pd.date_range(end=end - pd.Timedelta(days=1), periods=12, freq="D")
    features = pd.DataFrame({"_BAR_CLOSE_TS": index + pd.Timedelta(days=1)}, index=index)
    axis = build_backtest_partition_axis(features, timeframe="D1", warmup_bars=0)
    # signal, entry, exit, direction, entry, stop, target, exit price, stop pips, target pips
    designs = [
        (0, 1, 2, 1, 1.1000, 1.0990, 1.1010, 1.1010, 10.0, 10.0),
        (3, 4, 5, -1, 1.1020, 1.1040, 1.1000, 1.1040, 20.0, 20.0),
        (6, 7, 8, 1, 1.1000, 1.0995, 1.1015, 1.1015, 5.0, 15.0),
        (9, 10, None, -1, 1.1008, 1.1015, 1.0998, None, 7.0, 10.0),
    ]
    signed = (10.0, -20.0, 15.0, None)
    reasons = ("TP", "SL", "TP", None)
    taken = tuple(chain.ChainTrade(
        opportunity_id=f"INVENTED_OPPORTUNITY_{ordinal}", signal_bar=signal,
        entry_bar=entry_bar, direction=direction, entry_regime="TREND", run_start=True,
        status="RIGHT_CENSORED" if exit_bar is None else "CLOSED",
        entry_price=entry_price, stop_loss_price=stop, take_profit_price=target,
        stop_loss_pips=stop_pips, take_profit_pips=target_pips, exit_bar=exit_bar,
        exit_price=exit_price, exit_reason=reasons[ordinal],
        exit_fill=None if exit_bar is None else ("STOP_LEVEL" if ordinal == 1 else "TARGET_LEVEL"),
        signed_pips=signed[ordinal],
        r_multiple=None if exit_bar is None else signed[ordinal] / stop_pips,
    ) for ordinal, (signal, entry_bar, exit_bar, direction, entry_price, stop,
                    target, exit_price, stop_pips, target_pips) in enumerate(designs))
    diagnostic_chain = chain.RegimeChain(
        symbol="INVENTED", timeframe="D1", regime="TREND", strategy_id=label,
        configuration_sha256=_sha(label + ":configuration"), spread_tier=tier,
        cutoff_utc=axis["end_utc"], taken=taken, skipped=(),
        chain_statistics={"provenance": label}, all_opportunity_statistics={"provenance": label},
    )
    risks = (125.0, 250.0, 80.0, 160.0)
    gross = (120.0, -190.0, 240.0, 42.5)
    swaps = (-3.25, -2.5, -1.75, -1.25)
    trades = []
    for ordinal, trade in enumerate(taken):
        exit_bar = trade.exit_bar if trade.exit_bar is not None else 11
        trades.append({
            "fixture_provenance": label, "signal_bar": trade.signal_bar,
            "entry_bar": trade.entry_bar, "exit_bar": exit_bar,
            "signal_time": index[trade.signal_bar], "entry_time": index[trade.entry_bar],
            "exit_time": index[exit_bar], "direction": "LONG" if trade.direction == 1 else "SHORT",
            "entry_price": trade.entry_price,
            "exit_price": trade.exit_price if trade.exit_price is not None else 1.1006,
            "stop_loss": trade.stop_loss_price, "take_profit": trade.take_profit_price,
            "position_size": 0.25 + ordinal * 0.1, "risk_amount": risks[ordinal],
            "gross_pnl_dollars": gross[ordinal], "swap_dollars": swaps[ordinal],
            "swap_rollover_units": 1, "pnl_dollars": gross[ordinal] + swaps[ordinal],
            "r_multiple": 999.0,
            "exit_reason": "end_of_data" if trade.exit_bar is None else (
                "closed_sl" if trade.exit_reason == "SL" else "closed_tp"),
            "trade_record_schema_version": "2026-07-14-position-events-v2",
        })
    spread_model = {"mode": "conservative_max", "frozen_spread_tier": {"tier": tier}}
    attestation = build_numerical_execution_cost_attestation(
        use_spread=True, spread_mode=build_backtest_spread_mode(spread_model),
        entry_half_spread_values_sha256=_sha(label + ":entry_spread:" + tier),
        intrabar_half_spread_values_sha256=_sha(label + ":intrabar_spread:" + tier),
        instrument_spec_sha256=_sha(label + ":instrument"),
    )
    swap_contract = {
        "fixture_provenance": label, "contract_version": "causal_swap_accrual_v1",
        "status": "AVAILABLE", "provenance_status": "AUTHENTICATED",
        "execution_state_compounding": True, "source_provenance_authenticated": True,
        "historical_rates_authenticated": True, "timestamp_source": "_BAR_CLOSE_TS",
        "projection_contract": "fxpm-swap-preopen-close-projection-v1",
        "projection_sha256": _sha(label + ":projection"),
    }
    for name in ("price_activation_manifest_sha256", "terms_activation_manifest_sha256",
                 "terms_snapshot_sha256", "schedule_sha256", "target_bars_sha256",
                 "target_sidecar_sha256", "historical_rates_sha256"):
        swap_contract[name] = _sha(label + ":" + name)
    result = {
        "fixture_provenance": label, "trades": trades, "spread_model": spread_model,
        "trade_power_partition_axis": axis,
        "execution_cost_timing_contract": {"swap_accrual": "causal_swap_accrual_v1"},
        "numerical_execution_cost_attestation": attestation,
        "swap_accrual_contract": swap_contract,
    }
    probe = _reconciliation_source_probe(result)
    if probe.source_contract is None:
        raise AssertionError("invented source envelope violates the unchanged public contract")
    binding = {
        "symbol": diagnostic_chain.symbol, "timeframe": diagnostic_chain.timeframe,
        "strategy_id": diagnostic_chain.strategy_id,
        "configuration_sha256": diagnostic_chain.configuration_sha256,
        "regime": diagnostic_chain.regime, "spread_tier": tier,
        "source_surface_sha256": _sha(label + ":caller_bound_surface"),
        "partition_source_contract_sha256": probe.source_contract.partition_source_contract_sha256,
        "partition_axis": deepcopy(axis), "comparison_scope": "SAME_WINDOW_TRADE_RECONCILIATION",
        "partition_role": "optimizer_train",
    }
    return diagnostic_chain, result, binding, probe


class ChainAccountReconciliationTests(unittest.TestCase):
    """Invented envelopes test diagnostic behavior, never financial evidence."""

    def setUp(self):
        self.diagnostic_chain, self.result, self.binding, self.probe = _invented_reconciliation_fixture()

    def reconcile(self, *, diagnostic_chain=None, result=None, binding=None, digest=None):
        return chain.reconcile_chain_account(
            self.diagnostic_chain if diagnostic_chain is None else diagnostic_chain,
            backtester_result=self.result if result is None else result,
            expected_source_contract_sha256=(self.probe.source_contract.source_contract_sha256
                                             if digest is None else digest),
            source_window_binding=self.binding if binding is None else binding,
        )

    def assert_refused(self, code, **kwargs):
        with self.assertRaises(chain.SignalChainError) as caught:
            self.reconcile(**kwargs)
        self.assertEqual(str(caught.exception), "CHAIN_ACCOUNT_" + code)

    def test_public_source_probe_has_distinct_stable_and_partition_identity(self):
        import pm_winner_raw_evidence as raw
        source = raw.build_trade_source_contract_from_backtester_result(self.result)
        identity = self.probe.source_contract
        self.assertIn("INVENTED", self.result["fixture_provenance"])
        self.assertIn("INVENTED", self.result["swap_accrual_contract"]["fixture_provenance"])
        self.assertEqual(self.probe.reason_codes, ("NO_TRADES",))
        self.assertNotEqual(identity.source_contract_sha256, identity.partition_source_contract_sha256)
        whole_dict_sha = hashlib.sha256(chain.canonical_bytes(source)).hexdigest()
        self.assertNotEqual(identity.source_contract_sha256, whole_dict_sha)
        self.assert_refused("SOURCE_IDENTITY_MISMATCH", digest=whole_dict_sha)

    def test_nonzero_swaps_varying_stops_and_risk_use_actual_account_net_r(self):
        from pm_core import canonical_backtest_winner_r
        from pm_winner_raw_evidence import build_precommitted_risk_unit_contract
        outcome = self.reconcile()
        self.assertEqual(outcome["status"], "RECONCILED")
        self.assertEqual(len(outcome["closed_pairs"]), 3)
        self.assertEqual(len(outcome["censored_pairs"]), 1)
        self.assertEqual(outcome["source_contract_sha256"], self.probe.source_contract.source_contract_sha256)
        self.assertEqual(outcome["partition_source_contract_sha256"],
                         self.probe.source_contract.partition_source_contract_sha256)
        self.assertEqual(outcome["source_window_binding"], self.binding)
        self.assertEqual(outcome["risk_unit_contract"], build_precommitted_risk_unit_contract())
        self.assertEqual(outcome["counts"], {"chain_closed": 3, "chain_censored": 1,
                                           "account_trades": 4, "closed_pairs": 3, "censored_pairs": 1})
        self.assertEqual({pair["direction"] for pair in outcome["closed_pairs"]}, {-1, 1})
        self.assertEqual({trade.stop_loss_pips for trade in self.diagnostic_chain.taken},
                         {5.0, 7.0, 10.0, 20.0})
        for pair, account, price in zip(outcome["closed_pairs"], self.result["trades"],
                                        self.diagnostic_chain.taken):
            self.assertNotEqual(account["swap_dollars"], 0.0)
            self.assertEqual(account["pnl_dollars"], account["gross_pnl_dollars"] + account["swap_dollars"])
            self.assertEqual(pair["pnl_dollars"], account["pnl_dollars"])
            self.assertEqual(pair["risk_amount"], account["risk_amount"])
            self.assertEqual(pair["chain_price_r"], price.signed_pips / price.stop_loss_pips)
            self.assertEqual(pair["account_net_r"], canonical_backtest_winner_r(
                account["pnl_dollars"], account["risk_amount"]))
            self.assertNotEqual(pair["account_net_r"], account["r_multiple"])
            self.assertNotEqual(pair["account_net_r"], pair["chain_price_r"])
        self.assertEqual(len({pair["risk_amount"] for pair in outcome["closed_pairs"]}), 3)

    def test_censored_pair_retains_eod_account_r_without_chain_r(self):
        outcome = self.reconcile()
        pair = outcome["censored_pairs"][0]
        self.assertIsNone(pair["chain_price_r"])
        self.assertIsInstance(pair["account_net_r"], float)
        self.assertEqual(pair["signal_bar"], 9)
        self.assertEqual(pair["exit_bar"], 11)
        self.assertEqual(len(outcome["closed_pairs"]), 3)

    def test_inputs_and_existing_chain_bytes_are_unchanged(self):
        from copy import deepcopy
        result_before, binding_before = deepcopy(self.result), deepcopy(self.binding)
        chain_before = self.diagnostic_chain.to_bytes()
        self.reconcile()
        self.assertEqual(self.result, result_before)
        self.assertEqual(self.binding, binding_before)
        self.assertEqual(self.diagnostic_chain.to_bytes(), chain_before)

    def test_no_trade_is_unavailable_without_rank_or_authority(self):
        from copy import deepcopy
        from dataclasses import replace
        empty = replace(self.diagnostic_chain, taken=())
        result = deepcopy(self.result)
        result["trades"] = []
        outcome = self.reconcile(diagnostic_chain=empty, result=result)
        self.assertEqual(outcome["status"], "UNAVAILABLE")
        self.assertEqual(outcome["reason_code"], "NO_CLOSED_TRADE_PAIRS")
        self.assertEqual(outcome["closed_pairs"], [])
        self.assertEqual(outcome["censored_pairs"], [])
        self.assertEqual(outcome["counts"], {"chain_closed": 0, "chain_censored": 0,
                                           "account_trades": 0, "closed_pairs": 0, "censored_pairs": 0})
        for name in ("qualification_authority", "capital_authority", "live_authority", "order_authority"):
            self.assertIs(outcome[name], False)

    def test_config_disabled_swap_is_not_broker_disabled_evidence(self):
        from copy import deepcopy
        result = deepcopy(self.result)
        result["fixture_configuration"] = {"swap_model_enabled": False}
        result["swap_accrual_contract"].update({
            "status": "DISABLED", "mode": "disabled", "reason": "swap model disabled",
            "source_provenance_authenticated": False, "historical_rates_authenticated": False,
        })
        self.assertNotIn("broker_disabled_proof_sha256", result["swap_accrual_contract"])
        self.assert_refused("SOURCE_CONTRACT_INVALID", result=result)

    def test_missing_cost_conversion_and_nonpositive_risk_refuse(self):
        from copy import deepcopy
        for field, value in (("pnl_dollars", None), ("pnl_dollars", float("inf")),
                             ("risk_amount", 0.0), ("risk_amount", -1.0), ("risk_amount", True)):
            with self.subTest(field=field, value=value):
                result = deepcopy(self.result)
                result["trades"][0][field] = value
                self.assert_refused("TRADE_INVALID", result=result)
        result = deepcopy(self.result)
        del result["trades"][0]["pnl_dollars"]
        self.assert_refused("TRADE_INVALID", result=result)

    def test_missing_and_tampered_source_and_partition_identities_refuse(self):
        from copy import deepcopy
        self.assert_refused("BINDING_INVALID", digest="")
        self.assert_refused("SOURCE_IDENTITY_MISMATCH", digest="f" * 64)
        for value in ("", "f" * 64):
            with self.subTest(partition_digest=value):
                binding = deepcopy(self.binding)
                binding["partition_source_contract_sha256"] = value
                self.assert_refused("BINDING_INVALID" if not value else "PARTITION_IDENTITY_MISMATCH",
                                    binding=binding)
        result = deepcopy(self.result)
        result["numerical_execution_cost_attestation"]["attestation_sha256"] = "f" * 64
        self.assert_refused("SOURCE_CONTRACT_INVALID", result=result)

    def test_missing_or_tampered_exact_partition_axis_refuses(self):
        from copy import deepcopy
        for field in ("event_axis_sha256", "horizon_receipt_sha256", "scored_bars", "end_utc"):
            with self.subTest(field=field):
                binding = deepcopy(self.binding)
                binding["partition_axis"][field] = ("f" * 64 if field.endswith("sha256") else
                                                     (13 if field == "scored_bars" else "2024-01-12T00:00:00+00:00"))
                self.assert_refused("WINDOW_MISMATCH", binding=binding)
        result = deepcopy(self.result)
        del result["trade_power_partition_axis"]
        self.assert_refused("WINDOW_MISMATCH", result=result)

    def test_candidate_metadata_must_match_exactly(self):
        from copy import deepcopy
        alternatives = {"symbol": "OTHER", "timeframe": "H1", "strategy_id": "OTHER",
                        "configuration_sha256": "f" * 64, "regime": "RANGE", "spread_tier": "P95"}
        for field, value in alternatives.items():
            with self.subTest(field=field):
                binding = deepcopy(self.binding)
                binding[field] = value
                self.assert_refused("CANDIDATE_MISMATCH", binding=binding)

    def test_p95_and_tier_dependent_trade_sets_are_not_base_evidence(self):
        diagnostic_chain, result, binding, probe = _invented_reconciliation_fixture(tier="P95")
        result["trades"] = result["trades"][1:]
        outcome = self.reconcile(diagnostic_chain=diagnostic_chain, result=result, binding=binding,
                                 digest=probe.source_contract.source_contract_sha256)
        self.assertEqual((outcome["status"], outcome["reason_code"]),
                         ("UNAVAILABLE", "BASE_TIER_REQUIRED"))
        self.assertEqual(outcome["closed_pairs"], [])
        self.assertEqual(outcome["censored_pairs"], [])

    def test_p95_attestation_cannot_be_relabelled_as_p75(self):
        diagnostic_chain, result, binding, probe = _invented_reconciliation_fixture(tier="P95")
        result["spread_model"]["frozen_spread_tier"]["tier"] = "P75"
        self.assert_refused("SOURCE_CONTRACT_INVALID", diagnostic_chain=diagnostic_chain,
                            result=result, binding=binding,
                            digest=probe.source_contract.source_contract_sha256)

    def test_missing_actual_tier_is_unavailable(self):
        from copy import deepcopy
        from pm_core import build_backtest_spread_mode
        from pm_trade_power import build_numerical_execution_cost_attestation
        result = deepcopy(self.result)
        result["spread_model"].pop("frozen_spread_tier")
        old = result["numerical_execution_cost_attestation"]
        result["numerical_execution_cost_attestation"] = build_numerical_execution_cost_attestation(
            use_spread=True, spread_mode=build_backtest_spread_mode(result["spread_model"]),
            entry_half_spread_values_sha256=old["entry_half_spread_values_sha256"],
            intrabar_half_spread_values_sha256=old["intrabar_half_spread_values_sha256"],
            instrument_spec_sha256=old["instrument_spec_sha256"],
        )
        probe = _reconciliation_source_probe(result)
        binding = deepcopy(self.binding)
        binding["partition_source_contract_sha256"] = probe.source_contract.partition_source_contract_sha256
        outcome = self.reconcile(result=result, binding=binding,
                                 digest=probe.source_contract.source_contract_sha256)
        self.assertEqual((outcome["status"], outcome["reason_code"]),
                         ("UNAVAILABLE", "BASE_TIER_REQUIRED"))
        self.assertEqual(outcome["closed_pairs"], [])

    def test_missing_extra_duplicate_and_reordered_trade_rows_refuse(self):
        from copy import deepcopy
        from dataclasses import replace
        missing = deepcopy(self.result)
        missing["trades"].pop(1)
        self.assert_refused("TRADE_SEQUENCE_MISMATCH", result=missing)
        extra = deepcopy(self.result)
        row = deepcopy(extra["trades"][-1])
        row.update({"signal_bar": 10, "entry_bar": 11, "exit_bar": 11})
        extra["trades"].append(row)
        self.assert_refused("TRADE_SEQUENCE_MISMATCH", result=extra)
        duplicate = deepcopy(self.result)
        duplicate["trades"].append(deepcopy(duplicate["trades"][0]))
        self.assert_refused("DUPLICATE_TRADE", result=duplicate)
        repeated = (self.diagnostic_chain.taken[0], self.diagnostic_chain.taken[0],
                    *self.diagnostic_chain.taken[2:])
        self.assert_refused("DUPLICATE_TRADE",
                            diagnostic_chain=replace(self.diagnostic_chain, taken=repeated))
        reordered = deepcopy(self.result)
        reordered["trades"][0], reordered["trades"][1] = reordered["trades"][1], reordered["trades"][0]
        self.assert_refused("TRADE_SEQUENCE_MISMATCH", result=reordered)

    def test_exact_signal_entry_exit_identity_and_prices_are_required(self):
        from copy import deepcopy
        for field in ("signal_bar", "entry_bar", "exit_bar"):
            with self.subTest(field=field):
                result = deepcopy(self.result)
                if field == "signal_bar":
                    result["trades"][1][field] -= 1
                else:
                    result["trades"][0][field] += 1
                self.assert_refused("TRADE_SEQUENCE_MISMATCH", result=result)
        for field in ("entry_price", "exit_price", "stop_loss", "take_profit"):
            with self.subTest(field=field):
                result = deepcopy(self.result)
                result["trades"][0][field] += 0.000000001
                self.assert_refused("PRICE_MISMATCH", result=result)

    def test_unknown_exit_and_censored_non_eod_are_never_fuzzy_matched(self):
        from copy import deepcopy
        for ordinal, reason, code in ((0, "closed_tp_extra", "EXIT_REASON_UNSUPPORTED"),
                                      (0, "end_of_data", "TRADE_SEQUENCE_MISMATCH"),
                                      (3, "closed_tp", "EXIT_REASON_UNSUPPORTED")):
            with self.subTest(ordinal=ordinal, reason=reason):
                result = deepcopy(self.result)
                result["trades"][ordinal]["exit_reason"] = reason
                self.assert_refused(code, result=result)

    def test_non_string_or_unhashable_role_and_scope_are_typed_refusals(self):
        from copy import deepcopy
        from dataclasses import replace
        for field in ("partition_role", "comparison_scope"):
            for value in (None, 1, [], {}, "UNKNOWN"):
                with self.subTest(field=field, value=value):
                    binding = deepcopy(self.binding)
                    binding[field] = value
                    self.assert_refused("BINDING_INVALID", binding=binding)
        invalid_chain = replace(self.diagnostic_chain, regime=[])
        self.assert_refused("BINDING_INVALID", diagnostic_chain=invalid_chain)

    def test_cutoff_and_axis_require_native_utc_whole_second_strings(self):
        from copy import deepcopy
        from dataclasses import replace

        def invalid_forms(utc_value):
            stamp = pd.Timestamp(utc_value)
            text = stamp.strftime("%Y-%m-%dT%H:%M:%S")
            offset = (stamp + pd.Timedelta(hours=1)).strftime("%Y-%m-%dT%H:%M:%S") + "+01:00"
            return text, offset, text + ".000000001Z", stamp

        for value in invalid_forms(self.diagnostic_chain.cutoff_utc):
            with self.subTest(cutoff=value):
                self.assert_refused("BINDING_INVALID",
                                    diagnostic_chain=replace(self.diagnostic_chain, cutoff_utc=value))
        for field in ("start_utc", "end_utc"):
            for value in invalid_forms(self.binding["partition_axis"][field]):
                with self.subTest(axis_field=field, value=value):
                    result, binding = deepcopy(self.result), deepcopy(self.binding)
                    axis = result["trade_power_partition_axis"]
                    axis[field] = value
                    if isinstance(value, str):
                        body = {key: item for key, item in axis.items() if key != "horizon_receipt_sha256"}
                        axis["horizon_receipt_sha256"] = hashlib.sha256(chain.canonical_bytes(body)).hexdigest()
                    binding["partition_axis"] = deepcopy(axis)
                    self.assert_refused("WINDOW_MISMATCH", result=result, binding=binding)

    def test_trade_times_reject_naive_non_utc_and_subsecond_values(self):
        from copy import deepcopy
        invalid = ("2024-01-02T00:00:00", "2024-01-02T01:00:00+01:00",
                   "2024-01-02T00:00:00.000000001Z",
                   pd.Timestamp("2024-01-02"), pd.Timestamp("2024-01-02", tz="Europe/London"),
                   pd.Timestamp("2024-01-02T00:00:00.000000001Z"))
        for value in invalid:
            with self.subTest(value=value):
                result = deepcopy(self.result)
                result["trades"][0]["entry_time"] = value
                self.assert_refused("TRADE_INVALID", result=result)

    def test_different_window_comparison_returns_no_trade_figures(self):
        from copy import deepcopy
        binding = deepcopy(self.binding)
        binding["comparison_scope"] = "TRAIN_HELDOUT_CANDIDATE_COMPARISON"
        result = deepcopy(self.result)
        result["trades"] = [{"pnl_dollars": "not a convertible account trade"}]
        outcome = self.reconcile(binding=binding, result=result)
        self.assertEqual((outcome["status"], outcome["reason_code"]),
                         ("UNAVAILABLE", "TRAIN_HELDOUT_COMPARISON_UNAVAILABLE"))
        self.assertEqual(outcome["closed_pairs"], [])
        self.assertEqual(outcome["censored_pairs"], [])

    def test_final_lockbox_is_refused_before_trade_calculation(self):
        from copy import deepcopy
        binding = deepcopy(self.binding)
        binding["partition_role"] = "final_lockbox"
        result = deepcopy(self.result)
        result["trades"] = [{"pnl_dollars": "not a convertible account trade"}]
        self.assert_refused("LOCKBOX_REFUSED", binding=binding, result=result)

    def test_exact_protected_boundary_is_allowed_and_later_window_refuses(self):
        from dataclasses import replace
        cutoff = "2024-07-14T08:05:00+00:00"
        diagnostic_chain, result, binding, probe = _invented_reconciliation_fixture(end_utc=cutoff)
        outcome = self.reconcile(diagnostic_chain=diagnostic_chain, result=result, binding=binding,
                                 digest=probe.source_contract.source_contract_sha256)
        self.assertEqual(outcome["status"], "RECONCILED")
        for later in ("2024-07-14T08:05:01+00:00", "2024-07-15T08:05:00+00:00"):
            with self.subTest(later=later):
                later_chain, later_result, later_binding, later_probe = _invented_reconciliation_fixture(end_utc=later)
                later_result["trades"] = [{"pnl_dollars": "not a convertible account trade"}]
                self.assert_refused("PROTECTED_CUTOFF_EXCEEDED", diagnostic_chain=later_chain,
                                    result=later_result, binding=later_binding,
                                    digest=later_probe.source_contract.source_contract_sha256)
        later_chain = replace(diagnostic_chain, cutoff_utc="2024-07-14T08:05:01+00:00")
        self.assert_refused("PROTECTED_CUTOFF_EXCEEDED", diagnostic_chain=later_chain,
                            result=result, binding=binding, digest=probe.source_contract.source_contract_sha256)

    def test_validation_partition_has_same_diagnostic_only_authorities(self):
        from copy import deepcopy
        binding = deepcopy(self.binding)
        binding["partition_role"] = "tuning_validation"
        outcome = self.reconcile(binding=binding)
        self.assertEqual(outcome["status"], "RECONCILED")
        self.assertEqual(outcome["scope"], "DESCRIPTIVE_DIAGNOSTIC_ONLY")
        self.assertEqual(outcome["provenance_scope"], "CALLER_BOUND_INPUTS_NOT_SOURCE_AUTHENTICATION")
        self.assertEqual(outcome["stress_cost"]["status"], "UNAVAILABLE")
        self.assertEqual(outcome["stress_cost"]["reason_code"], "STRESS_ON_BASE_COST_EVIDENCE_UNAVAILABLE")
        for name in ("qualification_authority", "capital_authority", "live_authority", "order_authority"):
            self.assertIs(outcome[name], False)


class ChainAccountWindowBoundaryTests(unittest.TestCase):
    """INVENTED window fixtures test refusals, never market or run evidence."""

    def setUp(self):
        self.diagnostic_chain, self.result, self.binding, self.probe = _invented_reconciliation_fixture()
        self.assertEqual(self.reconcile()["status"], "RECONCILED")

    def reconcile(self, *, diagnostic_chain=None, result=None, binding=None, probe=None):
        identity = (self.probe if probe is None else probe).source_contract
        return chain.reconcile_chain_account(
            self.diagnostic_chain if diagnostic_chain is None else diagnostic_chain,
            backtester_result=self.result if result is None else result,
            expected_source_contract_sha256=identity.source_contract_sha256,
            source_window_binding=self.binding if binding is None else binding,
        )

    def assert_refused(self, code, **kwargs):
        with self.assertRaises(chain.SignalChainError) as caught:
            self.reconcile(**kwargs)
        self.assertEqual(str(caught.exception), "CHAIN_ACCOUNT_" + code)

    def test_valid_chain_cutoff_before_axis_end_refuses(self):
        """Published guards 389-390 reject a valid one-bar-earlier cutoff."""
        from dataclasses import replace

        earlier = pd.Timestamp(self.diagnostic_chain.cutoff_utc) - pd.Timedelta(days=1)
        diagnostic_chain = replace(self.diagnostic_chain, cutoff_utc=earlier.isoformat())
        self.assert_refused("WINDOW_MISMATCH", diagnostic_chain=diagnostic_chain)

    def test_valid_utc_trade_times_outside_axis_refuse(self):
        """Published guards 475-476 reject valid UTC times outside the axis."""
        from copy import deepcopy
        from pm_core import build_backtest_trade_identity_pnl_evidence

        axis = self.binding["partition_axis"]
        start, end = pd.Timestamp(axis["start_utc"]), pd.Timestamp(axis["end_utc"])
        cases = (
            ("entry_before_start", 0, "entry_time", start - pd.Timedelta(seconds=1)),
            ("exit_at_end", -1, "exit_time", end),
            ("exit_after_protected", -1, "exit_time", pd.Timestamp("2024-07-14T08:05:01+00:00")),
        )
        for label, ordinal, field, value in cases:
            with self.subTest(boundary=label):
                result = deepcopy(self.result)
                result["trades"][ordinal][field] = value
                if field == "entry_time":
                    result["trades"][ordinal]["signal_time"] = start - pd.Timedelta(days=1)
                projected = build_backtest_trade_identity_pnl_evidence(result["trades"])
                self.assertEqual(len(projected), len(result["trades"]))
                self.assert_refused("WINDOW_MISMATCH", result=result)

    def test_valid_different_partition_axes_refuse(self):
        """Published guards 386-387 reject distinct public-builder axes."""
        from copy import deepcopy

        other_chain, other_result, other_binding, other_probe = _invented_reconciliation_fixture(
            end_utc="2024-01-14T00:00:00+00:00")
        self.assertEqual(self.reconcile(diagnostic_chain=other_chain, result=other_result,
                                       binding=other_binding, probe=other_probe)["status"], "RECONCILED")
        binding = deepcopy(self.binding)
        binding["partition_axis"] = deepcopy(other_binding["partition_axis"])
        self.assertNotEqual(binding["partition_axis"], self.result["trade_power_partition_axis"])
        self.assert_refused("WINDOW_MISMATCH", binding=binding)

    def test_train_heldout_future_expected_axis_refuses(self):
        """Published guards 369-370 reject the future expected axis first."""
        from copy import deepcopy

        protected = pd.Timestamp("2024-07-14T08:05:00+00:00")
        _, _, future_binding, _ = _invented_reconciliation_fixture(
            end_utc="2024-07-14T08:05:01+00:00")
        binding = deepcopy(self.binding)
        binding["comparison_scope"] = "TRAIN_HELDOUT_CANDIDATE_COMPARISON"
        binding["partition_axis"] = deepcopy(future_binding["partition_axis"])
        self.assertLess(pd.Timestamp(self.diagnostic_chain.cutoff_utc), protected)
        self.assertGreater(pd.Timestamp(binding["partition_axis"]["end_utc"]), protected)
        self.assert_refused("PROTECTED_CUTOFF_EXCEEDED", binding=binding)

    def test_train_heldout_future_observed_axis_refuses(self):
        """Published guards 369-370 reject the future observed axis second."""
        from copy import deepcopy

        protected = pd.Timestamp("2024-07-14T08:05:00+00:00")
        _, future_result, _, future_probe = _invented_reconciliation_fixture(
            end_utc="2024-07-14T08:05:01+00:00")
        binding = deepcopy(self.binding)
        binding["comparison_scope"] = "TRAIN_HELDOUT_CANDIDATE_COMPARISON"
        binding["partition_source_contract_sha256"] = future_probe.source_contract.partition_source_contract_sha256
        self.assertLess(pd.Timestamp(self.diagnostic_chain.cutoff_utc), protected)
        self.assertLess(pd.Timestamp(binding["partition_axis"]["end_utc"]), protected)
        self.assertGreater(pd.Timestamp(future_result["trade_power_partition_axis"]["end_utc"]), protected)
        self.assert_refused("PROTECTED_CUTOFF_EXCEEDED", result=future_result,
                            binding=binding, probe=future_probe)


if __name__ == "__main__":
    unittest.main()
