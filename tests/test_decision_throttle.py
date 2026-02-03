import os
import tempfile
import unittest

from pm_main import DecisionThrottle


class DecisionThrottleTests(unittest.TestCase):
    def test_per_bar_key_suppression(self):
        with tempfile.TemporaryDirectory() as tmp:
            log_path = os.path.join(tmp, "last_trade_log.json")
            throttle = DecisionThrottle(log_path=log_path)

            bar_time = "2024-01-01T00:00:00"
            dk1 = DecisionThrottle.make_decision_key(
                "EURUSD", "StratA", "H1", "TREND", 1, bar_time
            )
            dk2 = DecisionThrottle.make_decision_key(
                "EURUSD", "StratB", "H1", "TREND", 1, bar_time
            )

            self.assertFalse(throttle.should_suppress("EURUSD", dk1, bar_time))
            throttle.record_decision(
                symbol="EURUSD", decision_key=dk1, bar_time_iso=bar_time,
                timeframe="H1", regime="TREND", strategy_name="StratA",
                direction=1, action="EXECUTED"
            )
            self.assertTrue(throttle.should_suppress("EURUSD", dk1, bar_time))
            # Different decision key on same bar should NOT suppress
            self.assertFalse(throttle.should_suppress("EURUSD", dk2, bar_time))

            throttle.record_decision(
                symbol="EURUSD", decision_key=dk2, bar_time_iso=bar_time,
                timeframe="H1", regime="TREND", strategy_name="StratB",
                direction=1, action="EXECUTED"
            )
            # Both keys now suppressed on same bar
            self.assertTrue(throttle.should_suppress("EURUSD", dk1, bar_time))
            self.assertTrue(throttle.should_suppress("EURUSD", dk2, bar_time))

            # New bar should allow again
            next_bar = "2024-01-01T01:00:00"
            self.assertFalse(throttle.should_suppress("EURUSD", dk1, next_bar))

    def test_persistence_roundtrip(self):
        with tempfile.TemporaryDirectory() as tmp:
            log_path = os.path.join(tmp, "last_trade_log.json")
            bar_time = "2024-01-01T00:00:00"
            dk1 = DecisionThrottle.make_decision_key(
                "GBPUSD", "StratA", "M15", "RANGE", -1, bar_time
            )

            throttle = DecisionThrottle(log_path=log_path)
            throttle.record_decision(
                symbol="GBPUSD", decision_key=dk1, bar_time_iso=bar_time,
                timeframe="M15", regime="RANGE", strategy_name="StratA",
                direction=-1, action="EXECUTED"
            )

            # Reload from disk
            throttle2 = DecisionThrottle(log_path=log_path)
            self.assertTrue(throttle2.should_suppress("GBPUSD", dk1, bar_time))


if __name__ == "__main__":
    unittest.main()
