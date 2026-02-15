import unittest
from copy import deepcopy

from pm_dashboard.models import SignalEntry
from pm_dashboard.parsers import parse_pm_execution_log
from pm_dashboard.utils import DEFAULT_CONFIG
from pm_dashboard.watcher import (
    enrich_entries,
    merge_actionable_with_log_executions,
    normalize_action_flags,
    should_display_entry,
)


class TestDashboardSignalDesk(unittest.TestCase):
    def test_parse_pm_execution_log(self) -> None:
        log_text = (
            "2026-02-04 11:52:15 [INFO] __main__: [EURGBP] Selected: "
            "VolatilityBreakoutStrategy @ M5/TREND (strength=0.37, quality=0.44)\n"
            "2026-02-04 11:52:15 [INFO] __main__: [EURGBP] SELL | basis=1453.35 (balance) | "
            "target_risk=1.00% ($14.53) | actual_risk=0.95% ($13.74) | vol_raw=0.1481 | "
            "vol=0.1400 | entry=0.86145 | sl=0.86217 | tp=0.86045\n"
            "2026-02-04 11:52:15 [INFO] __main__: [OK] [EURGBP] SHORT executed: 0.14 lots @ 0.86145\n"
        )

        config = deepcopy(DEFAULT_CONFIG)
        entries = parse_pm_execution_log(log_text, "pm.log", config, {})
        self.assertEqual(len(entries), 1)
        entry = entries[0]
        self.assertEqual(entry.symbol, "EURGBP")
        self.assertEqual(entry.timeframe, "M5")
        self.assertEqual(entry.regime, "TREND")
        self.assertEqual(entry.strategy_name, "VolatilityBreakoutStrategy")
        self.assertEqual(entry.signal_direction, "sell")
        self.assertAlmostEqual(entry.entry_price, 0.86145, places=5)
        self.assertAlmostEqual(entry.stop_loss_price, 0.86217, places=5)
        self.assertAlmostEqual(entry.take_profit_price, 0.86045, places=5)
        self.assertEqual(entry.reason, "EXECUTED")

    def test_parse_pm_execution_log_secondary_and_skips(self) -> None:
        log_text = (
            "2026-02-10 00:22:06 [INFO] __main__: [USDSEK] [SECONDARY] Selected: "
            "MomentumBurstStrategy @ D1/TREND (strength=0.32, quality=0.44, freshness=1.00, score=0.141)\n"
            "2026-02-10 00:22:06 [INFO] __main__: [USDSEK] Skipping trade; position already exists for magic 123456 "
            "(ticket=789101, tf=D1)\n"
            "2026-02-10 00:21:40 [INFO] __main__: [XAGUSD] Selected: "
            "KeltnerBreakoutStrategy @ M30/TREND (strength=0.42, quality=0.44, freshness=1.00, score=0.183)\n"
            "2026-02-10 00:21:40 [WARNING] __main__: [XAGUSD] Skipping trade; risk 8.97% exceeds cap 5.00% "
            "(vol=0.0100, sl=79.32350)\n"
        )
        config = deepcopy(DEFAULT_CONFIG)
        entries = parse_pm_execution_log(log_text, "pm.log", config, {})
        self.assertEqual(len(entries), 2)

        by_key = {(e.symbol, e.reason): e for e in entries}
        self.assertIn(("USDSEK", "SKIPPED_POSITION_EXISTS"), by_key)
        self.assertIn(("XAGUSD", "SKIPPED_RISK_CAP"), by_key)
        self.assertEqual(by_key[("USDSEK", "SKIPPED_POSITION_EXISTS")].timeframe, "D1")
        self.assertEqual(by_key[("XAGUSD", "SKIPPED_RISK_CAP")].timeframe, "M30")
        self.assertAlmostEqual(by_key[("XAGUSD", "SKIPPED_RISK_CAP")].stop_loss_price, 79.3235, places=4)

    def test_parse_pm_execution_log_failed_order(self) -> None:
        log_text = (
            "2026-02-10 00:21:47 [INFO] __main__: [EU50] [SECONDARY] Selected: "
            "MomentumBurstStrategy @ D1/CHOP (strength=0.32, quality=0.39, freshness=1.00, score=0.126)\n"
            "2026-02-10 00:21:47 [INFO] __main__: [EU50] BUY | basis=1412.46 (balance) | "
            "target_risk=1.00% ($14.12) | actual_risk=1.56% ($22.08) | vol_raw=0.0100 | "
            "vol=0.0100 | entry=6066.95000 | sl=5881.52143 | tp=6345.09286\n"
            "2026-02-10 00:21:47 [WARNING] __main__: [FAIL] [EU50] Order failed: 10018 - Market closed\n"
        )
        config = deepcopy(DEFAULT_CONFIG)
        entries = parse_pm_execution_log(log_text, "pm.log", config, {})
        self.assertEqual(len(entries), 1)

        entry = entries[0]
        self.assertEqual(entry.symbol, "EU50")
        self.assertEqual(entry.reason, "FAILED_10018")
        self.assertEqual(entry.signal_direction, "buy")
        self.assertTrue(entry.secondary_trade)

    def test_display_filters_respect_actions_and_fields(self) -> None:
        config = deepcopy(DEFAULT_CONFIG)
        config["display_actions"] = ["EXECUTED", "SKIPPED_RISK_CAP", "SKIPPED_POSITION_EXISTS"]
        config["display_require_fields"] = [
            "signal_direction",
            "entry_price",
            "stop_loss_price",
            "take_profit_price",
        ]
        config["display_allow_if_actions"] = ["SKIPPED_RISK_CAP", "SKIPPED_POSITION_EXISTS"]

        executed_missing = SignalEntry(
            symbol="EURGBP",
            signal_direction="sell",
            entry_price=None,
            stop_loss_price=None,
            take_profit_price=None,
            reason="EXECUTED",
            raw={"action": "EXECUTED"},
        )
        self.assertFalse(should_display_entry(executed_missing, config))

        risk_cap_missing = SignalEntry(
            symbol="XAGUSD",
            signal_direction="buy",
            entry_price=None,
            stop_loss_price=None,
            take_profit_price=None,
            reason="SKIPPED_RISK_CAP",
            raw={"action": "SKIPPED_RISK_CAP"},
        )
        self.assertTrue(should_display_entry(risk_cap_missing, config))

        skip_exists_missing = SignalEntry(
            symbol="USDSEK",
            signal_direction="sell",
            entry_price=None,
            stop_loss_price=None,
            take_profit_price=None,
            reason="SKIPPED_POSITION_EXISTS",
            raw={"action": "SKIPPED_POSITION_EXISTS"},
        )
        self.assertTrue(should_display_entry(skip_exists_missing, config))

    def test_display_filters_support_action_prefixes(self) -> None:
        config = deepcopy(DEFAULT_CONFIG)
        config["display_actions"] = []
        config["display_action_prefixes"] = ["FAILED_"]
        config["display_require_fields"] = [
            "signal_direction",
            "entry_price",
            "stop_loss_price",
            "take_profit_price",
        ]
        config["display_allow_if_actions"] = []
        config["display_allow_if_action_prefixes"] = ["FAILED_"]

        failed_missing = SignalEntry(
            symbol="EU50",
            signal_direction="buy",
            entry_price=None,
            stop_loss_price=None,
            take_profit_price=None,
            reason="FAILED_10018",
            raw={"action": "FAILED_10018"},
        )
        self.assertTrue(should_display_entry(failed_missing, config))

    def test_validity_respects_age(self) -> None:
        config = deepcopy(DEFAULT_CONFIG)
        config["valid_actions"] = ["EXECUTED"]
        config["max_signal_age_minutes"] = 1

        entry = SignalEntry(
            symbol="EURGBP",
            signal_direction="sell",
            entry_price=1.0,
            stop_loss_price=1.1,
            take_profit_price=0.9,
            timestamp="2000-01-01T00:00:00",
            raw={"action": "EXECUTED"},
        )
        normalize_action_flags([entry], config)
        self.assertFalse(entry.valid_now)

    def test_enrich_entries_respects_direction_and_freshness(self) -> None:
        config = deepcopy(DEFAULT_CONFIG)
        config["trade_map_max_age_minutes"] = 30

        entry = SignalEntry(
            symbol="XAUUSD",
            signal_direction="sell",
            entry_price=None,
            stop_loss_price=None,
            take_profit_price=None,
            timestamp="2026-02-04T12:30:00",
            raw={"action": "EXECUTED"},
        )

        trade_map = {
            "XAUUSD": {
                "price": 5091.05,
                "sl": 5024.05,
                "tp": 5135.71,
                "direction": "BUY",  # mismatch
                "timestamp": "2026-02-04T12:30:10",
                "status": "EXECUTED",
            }
        }

        enriched = enrich_entries([entry], {}, trade_map, config)
        self.assertIsNone(enriched[0].entry_price)

        # Now with matching direction but stale timestamp (beyond max age)
        trade_map["XAUUSD"]["direction"] = "SELL"
        trade_map["XAUUSD"]["timestamp"] = "2026-02-04T10:00:00"
        enriched = enrich_entries([entry], {}, trade_map, config)
        self.assertIsNone(enriched[0].entry_price)

    def test_merge_actionable_with_log_executions_prefers_newer_log_outcome(self) -> None:
        primary = SignalEntry(
            symbol="EU50",
            timeframe="D1",
            regime="CHOP",
            strategy_name="MomentumBurstStrategy",
            signal_direction="buy",
            entry_price=6066.95,
            stop_loss_price=5881.52,
            take_profit_price=6345.09,
            timestamp="2026-02-10T00:21:47",
            reason="EXECUTED",
            raw={"action": "EXECUTED"},
        )
        failed_log = SignalEntry(
            symbol="EU50",
            timeframe="D1",
            regime="CHOP",
            strategy_name="MomentumBurstStrategy",
            signal_direction="buy",
            entry_price=6066.95,
            stop_loss_price=5881.52,
            take_profit_price=6345.09,
            timestamp="2026-02-10T00:22:47",
            reason="FAILED_10018",
            raw={"action": "FAILED_10018"},
        )

        merged = merge_actionable_with_log_executions([primary], [failed_log])
        actions = [item.reason for item in merged]
        self.assertIn("EXECUTED", actions)
        self.assertIn("FAILED_10018", actions)


if __name__ == "__main__":
    unittest.main()
