import unittest
from unittest.mock import Mock
import logging
import math
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pm_mt5
from pm_mt5 import MT5Config, MT5Connector, OrderType


class _FakeMT5:
    ORDER_TYPE_BUY = 0
    ORDER_TYPE_SELL = 1
    ORDER_FILLING_FOK = 0
    ORDER_FILLING_IOC = 1
    ORDER_FILLING_RETURN = 2
    ORDER_TIME_GTC = 0
    TRADE_ACTION_DEAL = 1
    TRADE_ACTION_SLTP = 2
    SYMBOL_FILLING_FOK = 1
    SYMBOL_FILLING_IOC = 2
    SYMBOL_TRADE_EXECUTION_MARKET = 2

    def __init__(self):
        self.order_check_request = None
        self.order_send_request = None
        self.tick_calls = 0
        self.account_trade_mode = 0
        self.account_trade_mode_present = True

    def symbol_select(self, symbol, enable):
        return symbol == "EURUSD.a"

    def symbols_get(self):
        return [SimpleNamespace(name="EURUSD.a", custom=False)]

    def symbol_info(self, symbol):
        if symbol != "EURUSD.a":
            return None
        return SimpleNamespace(
            name="EURUSD.a",
            digits=5,
            point=0.00001,
            trade_tick_value=10.0,
            trade_tick_size=0.00001,
            trade_contract_size=100000.0,
            volume_min=0.01,
            volume_max=100.0,
            volume_step=0.01,
            spread=12,
            spread_float=True,
            swap_long=-1.0,
            swap_short=0.5,
            trade_stops_level=10,
            visible=True,
            trade_mode=2,
            filling_mode=self.SYMBOL_FILLING_FOK,
            trade_exemode=1,
            trade_freeze_level=0,
        )

    def symbol_info_tick(self, symbol):
        self.tick_calls += 1
        if symbol != "EURUSD.a":
            return None
        return SimpleNamespace(
            time=1711843260,
            bid=1.10000,
            ask=1.10020,
            last=1.10010,
            volume=100,
        )

    def copy_rates_range(self, symbol, timeframe, start_date, end_date):
        if symbol != "EURUSD.a":
            return []
        return [
            {
                "time": 1711843200,
                "open": 1.10000,
                "high": 1.10100,
                "low": 1.09900,
                "close": 1.10050,
                "tick_volume": 100,
                "spread": 17,
                "real_volume": 3,
            }
        ]

    def order_check(self, request):
        self.order_check_request = dict(request)
        return SimpleNamespace(retcode=0, comment="check passed")

    def order_send(self, request):
        self.order_send_request = dict(request)
        return SimpleNamespace(
            retcode=10009,
            deal=12345,
            order=67890,
            volume=request["volume"],
            price=request["price"],
        )

    def last_error(self):
        return (0, "ok")

    def account_info(self):
        values = dict(
            login=106165989,
            server="FBS-Demo",
            company="Broker",
            balance=10000.0,
            equity=10000.0,
            margin=0.0,
            margin_free=10000.0,
            margin_level=0.0,
            profit=0.0,
            leverage=100,
            currency="USD",
            trade_allowed=True,
            trade_expert=True,
            margin_mode=2,
        )
        if self.account_trade_mode_present:
            values["trade_mode"] = self.account_trade_mode
        return SimpleNamespace(**values)


class MT5ConnectorTests(unittest.TestCase):
    def setUp(self):
        self._orig_available = pm_mt5.MT5_AVAILABLE
        self._orig_mt5 = pm_mt5.mt5
        self.fake_mt5 = _FakeMT5()
        pm_mt5.mt5 = self.fake_mt5
        pm_mt5.MT5_AVAILABLE = True

    def tearDown(self):
        pm_mt5.mt5 = self._orig_mt5
        pm_mt5.MT5_AVAILABLE = self._orig_available

    def test_send_market_order_uses_resolved_broker_symbol_and_preflight(self):
        connector = MT5Connector(MT5Config())
        connector._connected = True

        result = connector.send_market_order(
            symbol="EURUSD",
            order_type=OrderType.BUY,
            volume=0.17,
            sl=1.09800,
            tp=1.10300,
        )

        self.assertTrue(result.success)
        self.assertIsNotNone(self.fake_mt5.order_check_request)
        self.assertIsNotNone(self.fake_mt5.order_send_request)
        self.assertEqual(self.fake_mt5.order_check_request["symbol"], "EURUSD.a")
        self.assertEqual(self.fake_mt5.order_send_request["symbol"], "EURUSD.a")
        self.assertEqual(
            self.fake_mt5.order_send_request["type_filling"],
            self.fake_mt5.ORDER_FILLING_FOK,
        )

    def test_send_market_order_uses_prechecked_price_without_refetching_tick(self):
        connector = MT5Connector(MT5Config())
        connector._connected = True

        result = connector.send_market_order(
            symbol="EURUSD",
            order_type=OrderType.BUY,
            volume=0.17,
            sl=1.09800,
            tp=1.10300,
            price=1.10123,
        )

        self.assertTrue(result.success)
        self.assertEqual(self.fake_mt5.tick_calls, 0)
        self.assertEqual(self.fake_mt5.order_send_request["price"], 1.10123)

    def test_order_send_none_is_marked_ambiguous(self):
        connector = MT5Connector(MT5Config())
        connector._connected = True

        def _none_order_send(request):
            self.fake_mt5.order_send_request = dict(request)
            return None

        self.fake_mt5.order_send = _none_order_send

        result = connector.send_market_order(
            symbol="EURUSD",
            order_type=OrderType.BUY,
            volume=0.17,
            sl=1.09800,
            tp=1.10300,
        )

        self.assertFalse(result.success)
        self.assertTrue(result.ambiguous)
        self.assertIsNotNone(self.fake_mt5.order_send_request)

    def test_normalize_volume_preserves_broker_step_precision(self):
        connector = MT5Connector(MT5Config())
        symbol_info = SimpleNamespace(
            volume_min=0.001,
            volume_max=100.0,
            volume_step=0.001,
        )

        volume = connector.normalize_volume(0.1239, symbol_info)

        self.assertEqual(volume, 0.123)

    def test_invalid_or_subminimum_volume_never_reaches_mt5(self):
        connector = MT5Connector(MT5Config())
        connector._connected = True

        for requested in (0.0, -0.01, float("nan"), 0.0001):
            with self.subTest(requested=requested):
                self.fake_mt5.order_check_request = None
                self.fake_mt5.order_send_request = None
                self.fake_mt5.tick_calls = 0

                result = connector.send_market_order(
                    symbol="EURUSD",
                    order_type=OrderType.BUY,
                    volume=requested,
                    sl=1.09800,
                    tp=1.10300,
                )

                self.assertFalse(result.success)
                self.assertEqual(result.retcode, 10014)
                self.assertIn("Invalid volume", result.retcode_description)
                self.assertIsNone(self.fake_mt5.order_check_request)
                self.assertIsNone(self.fake_mt5.order_send_request)
                self.assertEqual(self.fake_mt5.tick_calls, 0)

    def test_normalize_volume_floors_without_increasing_exposure(self):
        connector = MT5Connector(MT5Config())
        symbol_info = SimpleNamespace(
            volume_min=0.01,
            volume_max=100.0,
            volume_step=0.01,
        )

        self.assertIsNone(connector.normalize_volume(0.0, symbol_info))
        self.assertIsNone(connector.normalize_volume(-0.01, symbol_info))
        self.assertIsNone(connector.normalize_volume(float("nan"), symbol_info))
        self.assertIsNone(connector.normalize_volume(0.0001, symbol_info))
        self.assertEqual(connector.normalize_volume(0.019, symbol_info), 0.01)
        self.assertEqual(connector.normalize_volume(0.01, symbol_info), 0.01)
        self.assertEqual(connector.normalize_volume(101.0, symbol_info), 100.0)

    def test_normalize_volume_supports_millilot_precision(self):
        connector = MT5Connector(MT5Config())
        symbol_info = SimpleNamespace(
            volume_min=0.001,
            volume_max=2.0,
            volume_step=0.001,
        )

        self.assertEqual(connector.normalize_volume(0.0199, symbol_info), 0.019)
        self.assertEqual(connector.normalize_volume(0.001, symbol_info), 0.001)

    def test_normalize_volume_rejects_invalid_broker_step_metadata(self):
        connector = MT5Connector(MT5Config())

        for step in (0.0, -0.01, float("nan"), float("inf")):
            with self.subTest(step=step):
                symbol_info = SimpleNamespace(
                    volume_min=0.01,
                    volume_max=100.0,
                    volume_step=step,
                )
                self.assertIsNone(connector.normalize_volume(0.10, symbol_info))

    def test_return_fill_is_rejected_for_market_execution(self):
        connector = MT5Connector(MT5Config(preferred_filling_type="return"))
        connector._connected = True

        symbol_info = SimpleNamespace(
            filling_mode=0,
            trade_exemode=self.fake_mt5.SYMBOL_TRADE_EXECUTION_MARKET,
        )

        filling = connector._get_filling_type("EURUSD", symbol_info=symbol_info)

        self.assertEqual(filling, self.fake_mt5.ORDER_FILLING_FOK)

    def test_get_symbol_info_preserves_tradability_metadata(self):
        connector = MT5Connector(MT5Config())
        connector._connected = True

        info = connector.get_symbol_info("EURUSD")

        self.assertIsNotNone(info)
        self.assertTrue(info.visible)
        self.assertEqual(info.trade_mode, 2)

    def test_get_account_info_preserves_hedging_account_mode(self):
        from pm_demo_readiness import demo_account_errors

        connector = MT5Connector(MT5Config())
        connector._connected = True

        info = connector.get_account_info()

        self.assertIsNotNone(info)
        self.assertEqual(info.trade_mode, 0)
        self.assertEqual(info.margin_mode, 2)
        # Python's account_info tuple may omit ACCOUNT_HEDGE_ALLOWED; retail
        # hedging mode is then the authoritative permission source.
        self.assertTrue(info.hedge_allowed)
        self.assertEqual(demo_account_errors(info), [])

    def test_get_account_info_missing_or_non_demo_mode_fails_closed(self):
        from pm_demo_readiness import demo_account_errors

        connector = MT5Connector(MT5Config())
        connector._connected = True

        self.fake_mt5.account_trade_mode_present = False
        missing = connector.get_account_info()
        self.assertEqual(missing.trade_mode, -1)
        self.assertIn("DEMO_ACCOUNT_NOT_BROKER_DEMO", demo_account_errors(missing))

        self.fake_mt5.account_trade_mode_present = True
        self.fake_mt5.account_trade_mode = 2
        real = connector.get_account_info()
        self.assertEqual(real.trade_mode, 2)
        self.assertIn("DEMO_ACCOUNT_NOT_BROKER_DEMO", demo_account_errors(real))

    def test_missing_symbol_get_bars_is_not_terminal_noise(self):
        connector = MT5Connector(MT5Config())
        connector._connected = True
        records = []

        class _ListHandler(logging.Handler):
            def emit(self, record):
                records.append(record)

        handler = _ListHandler()
        handler.setLevel(logging.DEBUG)
        previous_level = pm_mt5.logger.level
        pm_mt5.logger.setLevel(logging.DEBUG)
        pm_mt5.logger.addHandler(handler)
        try:
            bars = connector.get_bars("BCHUSD", "M5", count=3)
        finally:
            pm_mt5.logger.removeHandler(handler)
            pm_mt5.logger.setLevel(previous_level)

        self.assertIsNone(bars)
        self.assertFalse(
            [record for record in records if record.levelno >= logging.WARNING],
            "Missing broker-universe symbols should stay below terminal log levels.",
        )

    def test_get_bars_range_preserves_spread_and_real_volume(self):
        connector = MT5Connector(MT5Config())
        connector._connected = True

        bars = connector.get_bars_range(
            "EURUSD",
            "M5",
            start_date=datetime(2024, 3, 30, tzinfo=timezone.utc),
            end_date=datetime(2024, 4, 1, tzinfo=timezone.utc),
        )

        self.assertIsNotNone(bars)
        self.assertIn("Spread", bars.columns)
        self.assertIn("RealVolume", bars.columns)
        self.assertEqual(float(bars["Spread"].iloc[0]), 17.0)
        self.assertEqual(float(bars["RealVolume"].iloc[0]), 3.0)


class _SyncingMT5:
    """Fake terminal where a freshly selected symbol has an empty local cache.

    ``copy_rates_from_pos`` (local cache) returns nothing until the terminal has
    "downloaded" enough history; a date-anchored ``copy_rates_from`` is what
    forces/awaits that download. This reproduces stock-CFD / exotic-metal
    symbols that are not in the default Market Watch.
    """

    def __init__(self, sync_after: int = 2):
        self.sync_after = int(sync_after)
        self.from_pos_calls = 0
        self.from_calls = 0
        self.range_calls = 0
        self.selected = []

    def _bar(self):
        return {
            "time": 1711843200,
            "open": 1.0,
            "high": 1.1,
            "low": 0.9,
            "close": 1.05,
            "tick_volume": 100,
            "spread": 3,
            "real_volume": 0,
        }

    def symbol_select(self, symbol, enable):
        self.selected.append(symbol)
        return symbol == "PLATINUM"

    def symbols_get(self):
        return [SimpleNamespace(name="PLATINUM", custom=False)]

    def copy_rates_from_pos(self, symbol, timeframe, start_pos, count):
        self.from_pos_calls += 1
        return []  # local cache is empty for a freshly selected symbol

    def copy_rates_from(self, symbol, timeframe, date_from, count):
        self.from_calls += 1
        if self.from_calls >= self.sync_after:
            return [self._bar()]
        return []

    def copy_rates_range(self, symbol, timeframe, start_date, end_date):
        self.range_calls += 1
        if self.range_calls >= self.sync_after:
            return [self._bar()]
        return []

    def last_error(self):
        return (0, "ok")


class _ImmediateMT5(_SyncingMT5):
    """Local cache already has data: first copy_rates_from_pos returns bars."""

    def copy_rates_from_pos(self, symbol, timeframe, start_pos, count):
        self.from_pos_calls += 1
        return [self._bar()]


class _PagedMT5(_SyncingMT5):
    """A symbol with ``total`` bars served at most ``cap`` per call (the terminal
    'Max bars in chart' limit), modelled date-anchored like real MT5.

    A single copy_rates_from(now, 300000) errors with (-2) because 300000 > cap,
    but walking the anchor date backwards accumulates the full ``total`` depth.
    """

    def __init__(self, total: int = 300000, maxbars: int = 10000, report_maxbars=None):
        super().__init__()
        self.total = int(total)
        self.cap = int(maxbars)
        self.report_maxbars = (
            self.cap if report_maxbars is None else int(report_maxbars)
        )
        self.base = 1_700_000_000  # newest bar in the past so anchor=now includes it
        self._err = (1, "Success")

    def terminal_info(self):
        return SimpleNamespace(maxbars=self.report_maxbars, build=5833, connected=True)

    def copy_rates_from(self, symbol, timeframe, date_from, count):
        self.from_calls += 1
        if count > self.cap:
            self._err = (-2, "Terminal: Invalid params")
            return []
        ts = (
            int(date_from.timestamp())
            if hasattr(date_from, "timestamp")
            else int(date_from)
        )
        i_start = max(0, math.ceil((self.base - ts) / 300))
        if i_start >= self.total:
            self._err = (1, "Success")
            return []
        idxs = range(i_start, min(self.total, i_start + int(count)))
        self._err = (1, "Success")
        return [
            dict(self._bar(), time=self.base - i * 300)
            for i in sorted(idxs, reverse=True)
        ]

    def last_error(self):
        return self._err


class HistorySyncTests(unittest.TestCase):
    def setUp(self):
        self._orig_available = pm_mt5.MT5_AVAILABLE
        self._orig_mt5 = pm_mt5.mt5
        pm_mt5.MT5_AVAILABLE = True

    def tearDown(self):
        pm_mt5.mt5 = self._orig_mt5
        pm_mt5.MT5_AVAILABLE = self._orig_available

    def _connector(self, fake):
        pm_mt5.mt5 = fake
        # sleep=0 keeps the test fast; attempts high enough to cross sync_after
        connector = MT5Connector(
            MT5Config(history_sync_max_attempts=6, history_sync_sleep_seconds=0.0)
        )
        connector._connected = True
        return connector

    def test_get_bars_forces_history_sync_for_fresh_symbol(self):
        fake = _SyncingMT5(sync_after=2)
        connector = self._connector(fake)

        bars = connector.get_bars("PLATINUM", "M5", count=300000)

        self.assertIsNotNone(bars, "fresh symbol should fetch after history sync")
        self.assertEqual(len(bars), 1)
        self.assertIn("Close", bars.columns)
        # The date-anchored copy_rates_from is what actually returned data.
        self.assertGreaterEqual(fake.from_calls, 2)

    def test_get_bars_no_extra_calls_when_cache_ready(self):
        fake = _ImmediateMT5()
        connector = self._connector(fake)

        bars = connector.get_bars("PLATINUM", "M5", count=1000)

        self.assertIsNotNone(bars)
        self.assertEqual(
            fake.from_pos_calls, 1, "no sync retries when data is already cached"
        )
        self.assertEqual(fake.from_calls, 0)

    def test_get_bars_returns_none_when_truly_empty(self):
        fake = _SyncingMT5(sync_after=999)  # never syncs within budget
        connector = self._connector(fake)

        bars = connector.get_bars("PLATINUM", "M5", count=1000)

        self.assertIsNone(bars)

    def test_get_bars_force_full_beats_maxbars_cap(self):
        # Broker has 35k but the terminal serves only 10k/call; date-walk gets all 35k.
        fake = _PagedMT5(total=35000, maxbars=10000)
        connector = self._connector(fake)

        df = connector.get_bars("PLATINUM", "M5", count=300000, force_full=True)
        self.assertIsNotNone(df)
        self.assertEqual(len(df), 35000)
        self.assertGreaterEqual(
            fake.from_calls, 4
        )  # multiple windows, past the 10k cap

    def test_get_bars_force_full_stops_at_true_broker_depth(self):
        # Broker genuinely holds 25k; date-walk returns exactly that, not more.
        fake = _PagedMT5(total=25000, maxbars=10000)
        connector = self._connector(fake)

        df = connector.get_bars("PLATINUM", "M5", count=300000, force_full=True)
        self.assertIsNotNone(df)
        self.assertEqual(len(df), 25000)

    def test_get_bars_force_full_shrinks_when_call_cap_below_page(self):
        # terminal_info under-reports (0); the real per-call cap forces an adaptive shrink.
        fake = _PagedMT5(total=50000, maxbars=40000, report_maxbars=0)
        connector = self._connector(fake)

        df = connector.get_bars("PLATINUM", "M5", count=300000, force_full=True)
        self.assertIsNotNone(df)
        self.assertEqual(len(df), 50000)
        self.assertGreaterEqual(fake.from_calls, 2)

    def test_get_bars_range_survives_copy_rates_exception(self):
        class _Boom(_SyncingMT5):
            def copy_rates_range(self, *a, **k):
                raise OSError(22, "Invalid argument")  # pre-1970 date in real MT5

        connector = self._connector(_Boom())
        out = connector.get_bars_range(
            "PLATINUM", "M5", datetime(1937, 5, 23), datetime(2026, 1, 1)
        )
        self.assertIsNone(out)  # degrades to None instead of crashing

    def test_get_bars_range_sync_retry_opt_in(self):
        class _EmptyThenWarmMT5(_SyncingMT5):
            def __init__(self):
                super().__init__()
                self.warmed = False

            def copy_rates_from(self, symbol, timeframe, date_from, count):
                self.from_calls += 1
                self.warmed = True
                return []

            def copy_rates_range(self, symbol, timeframe, start_date, end_date):
                self.range_calls += 1
                if not self.warmed:
                    return []
                return [dict(self._bar(), time=int(start_date.timestamp()))]

        fake = _EmptyThenWarmMT5()
        connector = self._connector(fake)

        # Default: no retry -> empty first call returns None.
        none_bars = connector.get_bars_range(
            "PLATINUM", "M5", datetime(2024, 1, 1), datetime(2024, 1, 2)
        )
        self.assertIsNone(none_bars)
        self.assertEqual(fake.range_calls, 1)

        # Opt-in: one bounded warm plus one exact reread can complete the sync.
        fake = _EmptyThenWarmMT5()
        connector = self._connector(fake)
        ok_bars = connector.get_bars_range(
            "PLATINUM",
            "M5",
            datetime(2024, 1, 1),
            datetime(2024, 1, 2),
            sync_retry=True,
        )
        self.assertIsNotNone(ok_bars)
        self.assertEqual(len(ok_bars), 1)
        self.assertEqual(fake.range_calls, 2)
        self.assertEqual(fake.from_calls, 1)

    def test_get_bars_range_sync_retry_waits_for_nonempty_history_to_expand(self):
        class _GrowingRangeMT5(_SyncingMT5):
            def copy_rates_range(self, symbol, timeframe, start_date, end_date):
                self.range_calls += 1
                recent = int((end_date - timedelta(minutes=5)).timestamp())
                if self.range_calls == 1:
                    # MT5 can expose the locally prepared tail before the older
                    # requested history has finished downloading/building.
                    return [dict(self._bar(), time=recent)]
                return [
                    dict(self._bar(), time=int(start_date.timestamp())),
                    dict(self._bar(), time=recent),
                ]

        fake = _GrowingRangeMT5()
        connector = self._connector(fake)
        bars = connector.get_bars_range(
            "PLATINUM",
            "M5",
            datetime(2024, 1, 1, tzinfo=timezone.utc),
            datetime(2024, 1, 10, tzinfo=timezone.utc),
            sync_retry=True,
        )

        self.assertIsNotNone(bars)
        self.assertEqual(len(bars), 2)
        self.assertGreaterEqual(fake.range_calls, 2)

    def test_get_bars_range_does_not_spin_on_repeated_incomplete_cache_extent(self):
        class _DelayedExpansionMT5(_SyncingMT5):
            def copy_rates_range(self, symbol, timeframe, start_date, end_date):
                self.range_calls += 1
                recent = int((end_date - timedelta(minutes=5)).timestamp())
                if self.range_calls < 4:
                    return [dict(self._bar(), time=recent)]
                return [
                    dict(self._bar(), time=int(start_date.timestamp())),
                    dict(self._bar(), time=recent),
                ]

        fake = _DelayedExpansionMT5()
        connector = self._connector(fake)
        bars = connector.get_bars_range(
            "PLATINUM",
            "M5",
            datetime(2024, 1, 1, tzinfo=timezone.utc),
            datetime(2024, 1, 10, tzinfo=timezone.utc),
            sync_retry=True,
        )

        self.assertIsNotNone(bars)
        self.assertEqual(len(bars), 1)
        self.assertEqual(fake.range_calls, 2)
        self.assertEqual(fake.from_calls, 1)
        diagnostics = connector.get_last_history_range_diagnostics()
        self.assertEqual(diagnostics["copy_rates_range_attempts"], 2)
        self.assertEqual(diagnostics["copy_rates_range_max_attempts"], 2)
        self.assertFalse(diagnostics["boundary_complete"])
        self.assertEqual(diagnostics["sync_status"], "boundary_incomplete")
        self.assertEqual(
            bars.attrs["mt5_history_range_diagnostics"],
            diagnostics,
        )

    def test_get_bars_range_uses_bounded_download_trigger_then_exact_range(self):
        class _TriggerOnlyMT5(_SyncingMT5):
            def __init__(self):
                super().__init__()
                self.warmed = False
                self.trigger_requests = []

            def copy_rates_from(self, symbol, timeframe, date_from, count):
                self.from_calls += 1
                self.trigger_requests.append((symbol, timeframe, date_from, count))
                self.warmed = True
                # This deliberately distinctive row is a synchronization
                # response, not admissible range evidence.
                return [dict(self._bar(), time=946684800, close=99.0)]

            def copy_rates_range(self, symbol, timeframe, start_date, end_date):
                self.range_calls += 1
                recent = int((end_date - timedelta(minutes=5)).timestamp())
                if not self.warmed:
                    return [dict(self._bar(), time=recent, close=1.01)]
                return [
                    dict(self._bar(), time=int(start_date.timestamp()), close=1.02),
                    dict(self._bar(), time=recent, close=1.03),
                ]

        fake = _TriggerOnlyMT5()
        connector = self._connector(fake)
        start = datetime(2024, 1, 1, tzinfo=timezone.utc)
        end = datetime(2024, 1, 10, tzinfo=timezone.utc)

        bars = connector.get_bars_range("PLATINUM", "M5", start, end, sync_retry=True)

        self.assertIsNotNone(bars)
        self.assertEqual(len(bars), 2)
        self.assertEqual(list(bars["Close"]), [1.02, 1.03])
        self.assertEqual(fake.range_calls, 2)
        self.assertEqual(len(fake.trigger_requests), 1)
        self.assertEqual(fake.trigger_requests[0][2], end)
        self.assertEqual(fake.trigger_requests[0][3], 2594)
        diagnostics = connector.get_last_history_range_diagnostics()
        self.assertEqual(diagnostics["history_download_trigger_attempts"], 1)
        self.assertEqual(diagnostics["history_download_trigger_nonempty"], 1)
        self.assertEqual(diagnostics["history_download_trigger_max_bars"], 2594)
        self.assertEqual(diagnostics["history_download_trigger_errors"], [])
        self.assertTrue(diagnostics["boundary_complete"])

    def test_observed_right_disjoint_sentinel_is_confirmed_once_and_never_admitted(
        self,
    ):
        class _ObservedSentinelMT5(_SyncingMT5):
            def __init__(self):
                super().__init__()
                self.events = []

            def symbol_select(self, symbol, enable):
                self.selected.append(symbol)
                return symbol == "ALCOA"

            def copy_rates_range(self, symbol, timeframe, start_date, end_date):
                self.range_calls += 1
                self.events.append("range")
                # Exact production observation: an old ALCOA request exposed a
                # single terminal-cache sentinel on 2019-11-01.
                sentinel = int(datetime(2019, 11, 1, tzinfo=timezone.utc).timestamp())
                return [dict(self._bar(), time=sentinel, close=99.0)]

            def copy_rates_from(self, symbol, timeframe, date_from, count):
                self.from_calls += 1
                self.events.append("warm")
                return []

            def last_error(self):
                self.events.append("last_error")
                return (1, "Success")

        start = datetime(2016, 5, 11, tzinfo=timezone.utc)
        end = datetime(2016, 6, 10, tzinfo=timezone.utc)
        sentinel = int(datetime(2019, 11, 1, tzinfo=timezone.utc).timestamp())

        historical = _ObservedSentinelMT5()
        connector = self._connector(historical)
        bars = connector.get_bars_range("ALCOA", "M5", start, end, sync_retry=True)

        self.assertIsNone(bars)
        self.assertEqual(historical.range_calls, 2)
        self.assertEqual(historical.from_calls, 1)
        self.assertEqual(
            historical.events,
            [
                "range",
                "last_error",
                "warm",
                "last_error",
                "range",
                "last_error",
            ],
        )
        diagnostics = connector.get_last_history_range_diagnostics()
        self.assertEqual(diagnostics["sync_status"], "terminal_prefix_unsynchronized")
        self.assertTrue(diagnostics["terminal_confirmation_consumed"])
        self.assertEqual(diagnostics["initial_raw_relation"], "RIGHT_DISJOINT_PREFIX")
        self.assertEqual(
            diagnostics["confirmation_raw_relation"],
            "RIGHT_DISJOINT_PREFIX",
        )
        self.assertEqual(diagnostics["raw_relation"], "RIGHT_DISJOINT_PREFIX")
        self.assertEqual(
            diagnostics["raw_extent"],
            {
                "rows": 1,
                "first_epoch_second": sentinel,
                "last_epoch_second": sentinel,
            },
        )
        self.assertEqual(diagnostics["admitted_count"], 0)
        self.assertIsNone(diagnostics["admitted_extent"])
        self.assertIsNone(diagnostics["best_extent"])
        self.assertEqual(len(diagnostics["call_ledger"]), 3)

        # The live/delta path remains isolated from synchronization calls; its
        # only behavior change is strict rejection of the sentinel.
        live = _ObservedSentinelMT5()
        connector = self._connector(live)
        bars = connector.get_bars_range("ALCOA", "M5", start, end, sync_retry=False)
        self.assertIsNone(bars)
        self.assertEqual(live.range_calls, 1)
        self.assertEqual(live.from_calls, 0)
        self.assertEqual(live.events, ["range", "last_error"])
        diagnostics = connector.get_last_history_range_diagnostics()
        self.assertFalse(diagnostics["terminal_confirmation_consumed"])
        self.assertEqual(len(diagnostics["call_ledger"]), 1)

    def test_mixed_raw_response_filters_both_edges_and_reports_admission(self):
        class _MixedRangeMT5(_SyncingMT5):
            def copy_rates_range(self, symbol, timeframe, start_date, end_date):
                self.range_calls += 1
                return [
                    dict(
                        self._bar(),
                        time=int((start_date - timedelta(minutes=5)).timestamp()),
                        close=10.0,
                    ),
                    dict(self._bar(), time=int(start_date.timestamp()), close=20.0),
                    dict(
                        self._bar(),
                        time=int((end_date - timedelta(minutes=5)).timestamp()),
                        close=30.0,
                    ),
                    # The upper bound is exclusive.
                    dict(self._bar(), time=int(end_date.timestamp()), close=40.0),
                ]

        fake = _MixedRangeMT5()
        connector = self._connector(fake)
        start = datetime(2024, 1, 1, tzinfo=timezone.utc)
        end = datetime(2024, 1, 2, tzinfo=timezone.utc)

        bars = connector.get_bars_range("PLATINUM", "M5", start, end, sync_retry=False)

        self.assertIsNotNone(bars)
        self.assertEqual(list(bars["Close"]), [20.0, 30.0])
        returned_epochs = bars.index.astype("int64") // 1_000_000_000
        self.assertTrue((returned_epochs >= int(start.timestamp())).all())
        self.assertTrue((returned_epochs < int(end.timestamp())).all())
        diagnostics = connector.get_last_history_range_diagnostics()
        self.assertEqual(diagnostics["raw_relation"], "MIXED_INVALID")
        self.assertEqual(diagnostics["raw_count"], 4)
        self.assertEqual(diagnostics["admitted_count"], 2)
        self.assertEqual(diagnostics["admitted_extent"]["rows"], 2)
        self.assertEqual(diagnostics["best_extent"], diagnostics["admitted_extent"])
        self.assertEqual(diagnostics["call_ledger"][0]["raw_extent"]["rows"], 4)
        self.assertEqual(diagnostics["call_ledger"][0]["admitted_extent"]["rows"], 2)

    def test_left_disjoint_suffix_uses_only_one_bounded_confirmation(self):
        class _LeftSuffixMT5(_SyncingMT5):
            def copy_rates_range(self, symbol, timeframe, start_date, end_date):
                self.range_calls += 1
                return [
                    dict(
                        self._bar(),
                        time=int((start_date - timedelta(minutes=5)).timestamp()),
                    )
                ]

            def copy_rates_from(self, symbol, timeframe, date_from, count):
                self.from_calls += 1
                return []

        fake = _LeftSuffixMT5()
        connector = self._connector(fake)
        connector.config.history_sync_max_attempts = 999
        bars = connector.get_bars_range(
            "PLATINUM",
            "M5",
            datetime(2024, 1, 1, tzinfo=timezone.utc),
            datetime(2024, 1, 2, tzinfo=timezone.utc),
            sync_retry=True,
        )

        self.assertIsNone(bars)
        self.assertEqual(fake.range_calls, 2)
        self.assertEqual(fake.from_calls, 1)
        diagnostics = connector.get_last_history_range_diagnostics()
        self.assertEqual(diagnostics["sync_status"], "terminal_suffix_unsynchronized")
        self.assertEqual(diagnostics["raw_relation"], "LEFT_DISJOINT_SUFFIX")
        self.assertEqual(diagnostics["copy_rates_range_attempts"], 2)
        self.assertEqual(diagnostics["copy_rates_range_max_attempts"], 2)
        self.assertEqual(len(diagnostics["call_ledger"]), 3)

    def test_hard_mt5_errors_stop_before_warm_and_are_captured_immediately(self):
        class _HardErrorMT5(_SyncingMT5):
            def __init__(self, code, message):
                super().__init__()
                self.code = code
                self.message = message
                self.events = []

            def copy_rates_range(self, symbol, timeframe, start_date, end_date):
                self.range_calls += 1
                self.events.append("range")
                return []

            def copy_rates_from(self, symbol, timeframe, date_from, count):
                raise AssertionError("hard failure must stop before warm")

            def last_error(self):
                self.events.append("last_error")
                return (self.code, self.message)

        for code, message in (
            (-1, "generic failure"),
            (-2, "invalid params"),
            (-3, "no memory"),
            (-10001, "internal IPC send failure"),
        ):
            with self.subTest(code=code):
                fake = _HardErrorMT5(code, message)
                connector = self._connector(fake)
                bars = connector.get_bars_range(
                    "PLATINUM",
                    "M5",
                    datetime(2024, 1, 1, tzinfo=timezone.utc),
                    datetime(2024, 1, 2, tzinfo=timezone.utc),
                    sync_retry=True,
                )

                self.assertIsNone(bars)
                self.assertEqual(fake.range_calls, 1)
                self.assertEqual(fake.events, ["range", "last_error"])
                diagnostics = connector.get_last_history_range_diagnostics()
                self.assertEqual(diagnostics["sync_status"], "hard_mt5_failure")
                self.assertTrue(diagnostics["hard_failure"])
                self.assertEqual(
                    diagnostics["hard_failure_call"],
                    "copy_rates_range_initial",
                )
                self.assertEqual(diagnostics["last_error"]["code"], code)
                self.assertEqual(len(diagnostics["call_ledger"]), 1)
                self.assertGreaterEqual(
                    diagnostics["call_ledger"][0]["elapsed_ms"], 0.0
                )

    def test_hard_warm_error_stops_before_exact_reread_and_retains_safe_prefix(self):
        class _HardWarmMT5(_SyncingMT5):
            def __init__(self):
                super().__init__()
                self._error = (1, "Success")

            def copy_rates_range(self, symbol, timeframe, start_date, end_date):
                self.range_calls += 1
                self._error = (1, "Success")
                return [dict(self._bar(), time=int(start_date.timestamp()))]

            def copy_rates_from(self, symbol, timeframe, date_from, count):
                self.from_calls += 1
                self._error = (-3, "No memory")
                return []

            def last_error(self):
                return self._error

        fake = _HardWarmMT5()
        connector = self._connector(fake)
        bars = connector.get_bars_range(
            "PLATINUM",
            "M5",
            datetime(2024, 1, 1, tzinfo=timezone.utc),
            datetime(2024, 1, 10, tzinfo=timezone.utc),
            sync_retry=True,
        )

        self.assertIsNotNone(bars)
        self.assertEqual(len(bars), 1)
        self.assertEqual(fake.range_calls, 1)
        self.assertEqual(fake.from_calls, 1)
        diagnostics = connector.get_last_history_range_diagnostics()
        self.assertEqual(diagnostics["sync_status"], "hard_mt5_failure")
        self.assertEqual(diagnostics["hard_failure_call"], "copy_rates_from_warm")
        self.assertEqual(len(diagnostics["call_ledger"]), 2)
        self.assertEqual(diagnostics["call_ledger"][1]["last_error"]["code"], -3)

    def test_short_m5_range_cannot_complete_from_one_cached_end_bar(self):
        class _ShortRangeTriggerMT5(_SyncingMT5):
            def __init__(self):
                super().__init__()
                self.warmed = False

            def copy_rates_from(self, symbol, timeframe, date_from, count):
                self.from_calls += 1
                self.warmed = True
                return [self._bar()]

            def copy_rates_range(self, symbol, timeframe, start_date, end_date):
                self.range_calls += 1
                recent = int((end_date - timedelta(minutes=5)).timestamp())
                if not self.warmed:
                    return [dict(self._bar(), time=recent)]
                return [
                    dict(self._bar(), time=int(start_date.timestamp())),
                    dict(self._bar(), time=recent),
                ]

        fake = _ShortRangeTriggerMT5()
        connector = self._connector(fake)
        start = datetime(2024, 1, 2, tzinfo=timezone.utc)
        end = start + timedelta(days=1)

        bars = connector.get_bars_range("PLATINUM", "M5", start, end, sync_retry=True)

        self.assertIsNotNone(bars)
        self.assertEqual(len(bars), 2)
        self.assertEqual(fake.range_calls, 2)
        self.assertEqual(fake.from_calls, 1)
        diagnostics = connector.get_last_history_range_diagnostics()
        self.assertTrue(diagnostics["boundary_complete"])
        self.assertEqual(diagnostics["boundary_span_seconds"], 86400)
        self.assertLess(
            2 * diagnostics["boundary_slack_seconds"],
            diagnostics["boundary_span_seconds"] - 300,
        )

    def test_d1_weekday_range_allows_weekend_boundary_without_overlap(self):
        class _WeekdayD1MT5(_SyncingMT5):
            def copy_rates_range(self, symbol, timeframe, start_date, end_date):
                self.range_calls += 1
                return [
                    dict(self._bar(), time=int(start_date.timestamp())),
                    dict(
                        self._bar(),
                        time=int((end_date - timedelta(days=3)).timestamp()),
                    ),
                ]

        fake = _WeekdayD1MT5()
        connector = self._connector(fake)
        start = datetime(2024, 1, 1, tzinfo=timezone.utc)  # Monday
        end = datetime(2024, 1, 8, tzinfo=timezone.utc)  # following Monday

        bars = connector.get_bars_range("PLATINUM", "D1", start, end, sync_retry=True)

        self.assertIsNotNone(bars)
        self.assertEqual(fake.range_calls, 1)
        self.assertEqual(fake.from_calls, 0)
        diagnostics = connector.get_last_history_range_diagnostics()
        self.assertTrue(diagnostics["boundary_complete"])
        self.assertLess(
            2 * diagnostics["boundary_slack_seconds"],
            diagnostics["boundary_span_seconds"] - 86400,
        )

    def test_fixed_offset_datetimes_use_absolute_boundary_geometry(self):
        class _FixedOffsetCompleteMT5(_SyncingMT5):
            def copy_rates_range(self, symbol, timeframe, start_date, end_date):
                self.range_calls += 1
                return [
                    dict(self._bar(), time=int(start_date.timestamp())),
                    dict(
                        self._bar(),
                        time=int((end_date - timedelta(minutes=5)).timestamp()),
                    ),
                ]

        fake = _FixedOffsetCompleteMT5()
        connector = self._connector(fake)
        fixed_offset = timezone(timedelta(hours=5, minutes=30))
        start = datetime(2024, 1, 2, tzinfo=fixed_offset)
        end = start + timedelta(days=1)

        bars = connector.get_bars_range("PLATINUM", "M5", start, end, sync_retry=True)

        self.assertIsNotNone(bars)
        self.assertEqual(fake.range_calls, 1)
        self.assertEqual(fake.from_calls, 0)
        diagnostics = connector.get_last_history_range_diagnostics()
        self.assertTrue(diagnostics["boundary_complete"])
        self.assertEqual(diagnostics["boundary_span_seconds"], 86400)

    def test_invalid_equal_or_reversed_range_returns_before_any_mt5_call(self):
        start = datetime(2024, 1, 2, tzinfo=timezone.utc)
        for end in (start, start - timedelta(days=1)):
            with self.subTest(end=end):
                fake = _ImmediateMT5()
                connector = self._connector(fake)
                connector._check_connection = Mock(
                    side_effect=AssertionError("MT5 must not be called")
                )

                bars = connector.get_bars_range(
                    "PLATINUM", "M5", start, end, sync_retry=True
                )

                self.assertIsNone(bars)
                connector._check_connection.assert_not_called()
                self.assertEqual(fake.range_calls, 0)
                self.assertEqual(fake.from_calls, 0)
                diagnostics = connector.get_last_history_range_diagnostics()
                self.assertEqual(diagnostics["copy_rates_range_attempts"], 0)
                self.assertEqual(diagnostics["history_download_trigger_attempts"], 0)
                self.assertEqual(diagnostics["boundary_span_seconds"], 0)
                self.assertFalse(diagnostics["boundary_complete"])

    def test_get_bars_range_complete_first_read_needs_no_download_trigger(self):
        class _CompleteRangeMT5(_SyncingMT5):
            def copy_rates_range(self, symbol, timeframe, start_date, end_date):
                self.range_calls += 1
                return [
                    dict(self._bar(), time=int(start_date.timestamp())),
                    dict(
                        self._bar(),
                        time=int((end_date - timedelta(minutes=5)).timestamp()),
                    ),
                ]

        fake = _CompleteRangeMT5()
        connector = self._connector(fake)
        bars = connector.get_bars_range(
            "PLATINUM",
            "M5",
            datetime(2024, 1, 1, tzinfo=timezone.utc),
            datetime(2024, 1, 10, tzinfo=timezone.utc),
            sync_retry=True,
        )

        self.assertIsNotNone(bars)
        self.assertEqual(fake.range_calls, 1)
        self.assertEqual(fake.from_calls, 0)
        diagnostics = connector.get_last_history_range_diagnostics()
        self.assertEqual(diagnostics["history_download_trigger_attempts"], 0)
        self.assertTrue(diagnostics["boundary_complete"])

    def test_get_bars_range_trigger_failure_is_finite_and_retains_best_extent(self):
        class _FailingTriggerMT5(_SyncingMT5):
            def copy_rates_from(self, symbol, timeframe, date_from, count):
                self.from_calls += 1
                raise RuntimeError("x" * 1000)

            def copy_rates_range(self, symbol, timeframe, start_date, end_date):
                self.range_calls += 1
                return [
                    dict(
                        self._bar(),
                        time=int((end_date - timedelta(minutes=5)).timestamp()),
                    )
                ]

        fake = _FailingTriggerMT5()
        connector = self._connector(fake)
        connector.config.history_sync_max_attempts = 3
        bars = connector.get_bars_range(
            "PLATINUM",
            "M5",
            datetime(2024, 1, 1, tzinfo=timezone.utc),
            datetime(2024, 1, 10, tzinfo=timezone.utc),
            sync_retry=True,
        )

        self.assertIsNotNone(bars)
        self.assertEqual(len(bars), 1)
        self.assertEqual(fake.range_calls, 1)
        self.assertEqual(fake.from_calls, 1)
        diagnostics = connector.get_last_history_range_diagnostics()
        self.assertEqual(diagnostics["copy_rates_range_attempts"], 1)
        self.assertEqual(diagnostics["history_download_trigger_attempts"], 1)
        self.assertEqual(len(diagnostics["history_download_trigger_errors"]), 1)
        self.assertEqual(
            len(diagnostics["history_download_trigger_errors"][0]["message"]),
            512,
        )
        self.assertFalse(diagnostics["boundary_complete"])
        self.assertTrue(diagnostics["hard_failure"])
        self.assertEqual(diagnostics["hard_failure_call"], "copy_rates_from_warm")
        self.assertEqual(len(diagnostics["call_ledger"]), 2)

    def test_get_bars_range_download_trigger_has_hard_allocation_ceiling(self):
        class _RecordingTriggerMT5(_SyncingMT5):
            def __init__(self):
                super().__init__()
                self.trigger_counts = []

            def copy_rates_from(self, symbol, timeframe, date_from, count):
                self.from_calls += 1
                self.trigger_counts.append(int(count))
                return []

            def copy_rates_range(self, symbol, timeframe, start_date, end_date):
                self.range_calls += 1
                return []

        fake = _RecordingTriggerMT5()
        connector = self._connector(fake)
        connector.config.history_sync_max_attempts = 2
        connector.config.history_page_size = 10_000_000

        bars = connector.get_bars_range(
            "PLATINUM",
            "M5",
            datetime(2000, 1, 1, tzinfo=timezone.utc),
            datetime(2026, 1, 1, tzinfo=timezone.utc),
            sync_retry=True,
        )

        self.assertIsNone(bars)
        self.assertEqual(fake.trigger_counts, [50_000])
        diagnostics = connector.get_last_history_range_diagnostics()
        self.assertEqual(diagnostics["history_download_trigger_max_bars"], 50_000)
        self.assertEqual(diagnostics["history_download_trigger_attempts"], 1)

    def test_range_diagnostics_do_not_break_get_bars_and_reset_on_early_return(self):
        connector = self._connector(_ImmediateMT5())
        self.assertIsNotNone(connector.get_bars("PLATINUM", "M5", count=1))
        connector._last_history_range_diagnostics = {"copy_rates_range_attempts": 99}

        result = connector.get_bars_range(
            "PLATINUM",
            "UNKNOWN",
            datetime(2024, 1, 1, tzinfo=timezone.utc),
            datetime(2024, 1, 2, tzinfo=timezone.utc),
            sync_retry=True,
        )

        self.assertIsNone(result)
        diagnostics = connector.get_last_history_range_diagnostics()
        self.assertEqual(diagnostics["copy_rates_range_attempts"], 0)
        self.assertEqual(diagnostics["history_download_trigger_attempts"], 0)
        self.assertEqual(diagnostics["history_download_trigger_nonempty"], 0)
        self.assertEqual(diagnostics["history_download_trigger_max_bars"], 0)
        self.assertEqual(diagnostics["history_download_trigger_errors"], [])
        self.assertEqual(diagnostics["boundary_span_seconds"], 0)
        self.assertEqual(diagnostics["boundary_slack_seconds"], 0)
        self.assertFalse(diagnostics["boundary_complete"])


if __name__ == "__main__":
    unittest.main()
