import unittest
from copy import deepcopy
from datetime import datetime

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
            "2026-02-04 11:52:15 [INFO] __main__: [EURGBP.A] [SECONDARY] Selected: "
            "VolatilityBreakoutStrategy @ M5/TREND (strength=0.37, quality=0.44)\n"
            "2026-02-04 11:52:15 [INFO] __main__: [EURGBP.A] SELL | basis=1453.35 (balance) | "
            "target_risk=1.00% ($14.53) | actual_risk=0.95% ($13.74) | vol_raw=0.1481 | "
            "vol=0.1400 | entry=0.86145 | sl=0.86217 | tp=0.86045\n"
            "2026-02-04 11:52:15 [INFO] __main__: [OK] [EURGBP.A] SHORT executed: 0.14 lots @ 0.86145\n"
        )

        config = deepcopy(DEFAULT_CONFIG)
        entries = parse_pm_execution_log(log_text, "pm.log", config, {})
        self.assertEqual(len(entries), 1)
        entry = entries[0]
        self.assertEqual(entry.symbol, "EURGBP.A")
        self.assertEqual(entry.timeframe, "M5")
        self.assertEqual(entry.regime, "TREND")
        self.assertEqual(entry.strategy_name, "VolatilityBreakoutStrategy")
        self.assertEqual(entry.signal_direction, "sell")
        self.assertAlmostEqual(entry.entry_price, 0.86145, places=5)
        self.assertAlmostEqual(entry.stop_loss_price, 0.86217, places=5)
        self.assertAlmostEqual(entry.take_profit_price, 0.86045, places=5)
        self.assertEqual(entry.reason, "EXECUTED")
        self.assertTrue(entry.secondary_trade)
        self.assertEqual(entry.secondary_reason, "log_tag")

    def test_display_filters_respect_actions_and_fields(self) -> None:
        config = deepcopy(DEFAULT_CONFIG)
        config["display_actions"] = ["EXECUTED", "SKIPPED_RISK_CAP"]
        config["display_require_fields"] = [
            "signal_direction",
            "entry_price",
            "stop_loss_price",
            "take_profit_price",
        ]
        config["display_allow_if_actions"] = ["SKIPPED_RISK_CAP"]

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

    def test_risk_cap_actions_are_valid_when_configured(self) -> None:
        config = deepcopy(DEFAULT_CONFIG)
        config["valid_actions"] = ["EXECUTED", "SKIPPED_RISK_CAP", "BLOCKED_RISK_CAP"]
        config["valid_action_prefixes"] = ["EXECUTED", "SKIPPED_RISK_CAP", "BLOCKED_RISK_CAP"]
        config["max_signal_age_minutes"] = 1440

        entry = SignalEntry(
            symbol="XAUUSD",
            signal_direction="buy",
            entry_price=2900.0,
            stop_loss_price=2890.0,
            take_profit_price=2920.0,
            timestamp=datetime.now().isoformat(),
            reason="SKIPPED_RISK_CAP",
            raw={"action": "SKIPPED_RISK_CAP"},
        )

        normalize_action_flags([entry], config)
        self.assertTrue(entry.valid_now)

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
            "XAUUSD": [
                {
                    "price": 5091.05,
                    "sl": 5024.05,
                    "tp": 5135.71,
                    "direction": "BUY",  # mismatch
                    "timestamp": "2026-02-04T12:30:10",
                    "status": "EXECUTED",
                }
            ]
        }

        enriched = enrich_entries([entry], {}, trade_map, config)
        self.assertIsNone(enriched[0].entry_price)

        # Now with matching direction but stale timestamp (beyond max age)
        trade_map["XAUUSD"][0]["direction"] = "SELL"
        trade_map["XAUUSD"][0]["timestamp"] = "2026-02-04T10:00:00"
        enriched = enrich_entries([entry], {}, trade_map, config)
        self.assertIsNone(enriched[0].entry_price)

    def test_merge_actionable_with_log_executions_keeps_hidden_exec(self) -> None:
        primary_entries = [
            SignalEntry(
                symbol="US100",
                signal_direction="sell",
                entry_price=25123.8,
                stop_loss_price=26774.96785714286,
                take_profit_price=23197.4375,
                timestamp="2026-02-09T00:01:37.818636",
                reason="SKIPPED_RISK_CAP",
                raw={"action": "SKIPPED_RISK_CAP"},
            ),
            SignalEntry(
                symbol="USDMXN",
                signal_direction="sell",
                entry_price=17.2349,
                stop_loss_price=17.60647,
                take_profit_price=16.58465,
                timestamp="2026-02-08T23:59:43.447424",
                reason="EXECUTED",
                raw={"action": "EXECUTED"},
            ),
        ]
        log_entries = [
            SignalEntry(
                symbol="US100",
                signal_direction="buy",
                entry_price=25122.82,
                stop_loss_price=24893.35643,
                take_profit_price=25237.55179,
                timestamp="2026-02-09T00:00:00",
                reason="EXECUTED",
                raw={"action": "EXECUTED"},
            ),
            SignalEntry(
                symbol="USDMXN",
                signal_direction="sell",
                entry_price=17.2349,
                stop_loss_price=17.60647,
                take_profit_price=16.58465,
                timestamp="2026-02-08T23:59:43",
                reason="EXECUTED",
                raw={"action": "EXECUTED"},
            ),
        ]

        merged = merge_actionable_with_log_executions(primary_entries, log_entries)
        self.assertTrue(
            any(
                entry.symbol == "US100" and str(entry.reason).upper() == "EXECUTED"
                for entry in merged
            )
        )
        self.assertEqual(
            len(
                [
                    entry
                    for entry in merged
                    if entry.symbol == "USDMXN" and str(entry.reason).upper() == "EXECUTED"
                ]
            ),
            1,
        )


if __name__ == "__main__":
    unittest.main()
