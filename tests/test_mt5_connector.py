import unittest
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
        if symbol != "EURUSD.a":
            return None
        return SimpleNamespace(
            time=1711843260,
            bid=1.10000,
            ask=1.10020,
            last=1.10010,
            volume=100,
        )

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


if __name__ == "__main__":
    unittest.main()
