import json
import os
import unittest
from datetime import datetime, timedelta

from pm_main import DecisionThrottle


class DecisionThrottleTests(unittest.TestCase):
    def test_per_bar_key_suppression(self):
        tmp_root = os.path.join(os.getcwd(), "_test_tmp", "decision_throttle", "per_bar")
        os.makedirs(tmp_root, exist_ok=True)
        log_path = os.path.join(tmp_root, "last_trade_log.json")
        if os.path.exists(log_path):
            os.remove(log_path)
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
        tmp_root = os.path.join(os.getcwd(), "_test_tmp", "decision_throttle", "roundtrip")
        os.makedirs(tmp_root, exist_ok=True)
        log_path = os.path.join(tmp_root, "last_trade_log.json")
        if os.path.exists(log_path):
            os.remove(log_path)
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


class DecisionThrottleStaleEntryPurgeTests(unittest.TestCase):
    """findings.html §6/§8.1: stale (>24h) cache entries must be purged on load.

    A long restart can otherwise resurrect a suppression keyed on a `bar_time`
    that the current run can no longer reason about, blocking real signals.
    """

    def _seed_log_with_action_time(self, log_path: str, *, action_time_iso: str,
                                    symbol: str = "EURUSD") -> None:
        payload = {
            symbol: {
                "symbol": symbol,
                "decision_key": "abcd1234",
                "decision_keys": ["abcd1234"],
                "bar_time": "2020-01-01T00:00:00",
                "timeframe": "H1",
                "regime": "TREND",
                "strategy_name": "StratA",
                "direction": 1,
                "action": "EXECUTED",
                "action_time": action_time_iso,
                "context": {},
            }
        }
        os.makedirs(os.path.dirname(log_path), exist_ok=True)
        with open(log_path, "w", encoding="utf-8") as f:
            json.dump(payload, f)

    def _read_log_keys(self, log_path: str):
        with open(log_path, "r", encoding="utf-8") as f:
            return set(json.load(f).keys())

    def test_stale_entry_purged_on_load(self):
        tmp_root = os.path.join(os.getcwd(), "_test_tmp", "decision_throttle", "stale_purge")
        log_path = os.path.join(tmp_root, "last_trade_log.json")
        if os.path.exists(log_path):
            os.remove(log_path)
        # 48h old → purged
        stale_iso = (datetime.now() - timedelta(hours=48)).isoformat()
        self._seed_log_with_action_time(log_path, action_time_iso=stale_iso)

        throttle = DecisionThrottle(log_path=log_path, max_age_hours=24.0)
        # Cache must be empty: stale entry was dropped on load.
        self.assertFalse(
            throttle.should_suppress("EURUSD", "abcd1234", "2020-01-01T00:00:00")
        )
        # And the on-disk file was rewritten without the stale entry.
        self.assertNotIn("EURUSD", self._read_log_keys(log_path))

    def test_fresh_entry_kept_on_load(self):
        tmp_root = os.path.join(os.getcwd(), "_test_tmp", "decision_throttle", "fresh_keep")
        log_path = os.path.join(tmp_root, "last_trade_log.json")
        if os.path.exists(log_path):
            os.remove(log_path)
        # 1h old → kept
        fresh_iso = (datetime.now() - timedelta(hours=1)).isoformat()
        self._seed_log_with_action_time(log_path, action_time_iso=fresh_iso)

        throttle = DecisionThrottle(log_path=log_path, max_age_hours=24.0)
        self.assertTrue(
            throttle.should_suppress("EURUSD", "abcd1234", "2020-01-01T00:00:00")
        )

    def test_max_age_zero_disables_purge(self):
        tmp_root = os.path.join(os.getcwd(), "_test_tmp", "decision_throttle", "no_purge")
        log_path = os.path.join(tmp_root, "last_trade_log.json")
        if os.path.exists(log_path):
            os.remove(log_path)
        ancient_iso = (datetime.now() - timedelta(days=400)).isoformat()
        self._seed_log_with_action_time(log_path, action_time_iso=ancient_iso)

        throttle = DecisionThrottle(log_path=log_path, max_age_hours=0.0)
        # With purging disabled the ancient entry survives.
        self.assertTrue(
            throttle.should_suppress("EURUSD", "abcd1234", "2020-01-01T00:00:00")
        )

    def test_unparseable_action_time_is_kept(self):
        """Defensive: if action_time can't be parsed, leave the entry alone
        (don't drop it silently — that would be the worst-of-both)."""
        tmp_root = os.path.join(os.getcwd(), "_test_tmp", "decision_throttle", "bad_iso")
        log_path = os.path.join(tmp_root, "last_trade_log.json")
        if os.path.exists(log_path):
            os.remove(log_path)
        self._seed_log_with_action_time(log_path, action_time_iso="not-an-iso-string")

        throttle = DecisionThrottle(log_path=log_path, max_age_hours=24.0)
        self.assertTrue(
            throttle.should_suppress("EURUSD", "abcd1234", "2020-01-01T00:00:00")
        )


if __name__ == "__main__":
    unittest.main()
