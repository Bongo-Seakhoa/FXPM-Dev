import unittest
from copy import deepcopy

from pm_dashboard.models import SignalEntry
from pm_dashboard.parsers import parse_pm_execution_log
from pm_dashboard.utils import DEFAULT_CONFIG
from pm_dashboard.watcher import normalize_action_flags, should_display_entry


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


if __name__ == "__main__":
    unittest.main()
