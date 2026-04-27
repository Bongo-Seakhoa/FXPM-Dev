import json
import os
import shutil
import unittest
import uuid
from copy import deepcopy
from datetime import datetime

from pm_dashboard.models import SignalEntry
from pm_dashboard.analytics import load_trade_history
from pm_dashboard.parsers import parse_entries_from_file, parse_pm_execution_log
from pm_dashboard.utils import DEFAULT_CONFIG
from pm_dashboard.watcher import (
    build_telegram_message,
    entry_alert_key,
    enrich_entries,
    filter_telegram_entries,
    find_primary_file,
    merge_actionable_with_log_executions,
    normalize_action_flags,
    select_trade_candidate,
    should_display_entry,
)


class TestDashboardSignalDesk(unittest.TestCase):
    def _make_temp_dir(self, prefix: str) -> str:
        root = os.path.join(os.getcwd(), ".tmp_dashboard_tests", f"{prefix}_{uuid.uuid4().hex}")
        os.makedirs(root, exist_ok=True)
        return root

    def test_parse_signal_ledger_jsonl(self) -> None:
        config = deepcopy(DEFAULT_CONFIG)
        text = (
            '{"symbol":"XAUUSD","action":"EXECUTED","direction":"buy","timeframe":"M15","regime":"CHOP",'
            '"strategy_name":"MomentumBurstStrategy","entry_price":3021.5,"stop_loss_price":3018.0,'
            '"take_profit_price":3028.0,"action_time":"2026-04-02T15:00:00"}\n'
        )
        entries = parse_entries_from_file("signal_ledger_202604.jsonl", text, config, {})
        self.assertEqual(len(entries), 1)
        entry = entries[0]
        self.assertEqual(entry.symbol, "XAUUSD")
        self.assertEqual(entry.reason, "EXECUTED")
        self.assertEqual(entry.timeframe, "M15")
        self.assertEqual(entry.regime, "CHOP")
        self.assertEqual(entry.signal_direction, "buy")

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

    def test_parse_signal_ledger_jsonl(self) -> None:
        tmp = self._make_temp_dir("ledger_jsonl")
        try:
            path = os.path.join(tmp, "signal_ledger.jsonl")
            with open(path, "w", encoding="utf-8") as handle:
                handle.write(
                    json.dumps(
                        {
                            "symbol": "EURUSD",
                            "timeframe": "H1",
                            "regime": "TREND",
                            "strategy_name": "MomentumBurstStrategy",
                            "signal_direction": "buy",
                            "entry_price": 1.0834,
                            "stop_loss_price": 1.0801,
                            "take_profit_price": 1.0902,
                            "signal_strength": 0.72,
                            "timestamp": "2026-04-02T12:00:00",
                            "action": "EXECUTED",
                        }
                    )
                    + "\n"
                )

            from pm_dashboard.parsers import parse_entries_from_file

            with open(path, "r", encoding="utf-8") as handle:
                parsed = parse_entries_from_file(path, handle.read(), deepcopy(DEFAULT_CONFIG), {}, None)

            self.assertEqual(len(parsed), 1)
            entry = parsed[0]
            self.assertEqual(entry.symbol, "EURUSD")
            self.assertEqual(entry.timeframe, "H1")
            self.assertEqual(entry.signal_direction, "buy")
            self.assertEqual(entry.reason, "EXECUTED")
        finally:
            shutil.rmtree(tmp, ignore_errors=True)

    def test_find_primary_file_prefers_signal_ledger(self) -> None:
        tmp = self._make_temp_dir("primary_file")
        try:
            outputs = os.path.join(tmp, "pm_outputs")
            os.makedirs(outputs, exist_ok=True)
            ledger = os.path.join(outputs, "signal_ledger.jsonl")
            actionable = os.path.join(tmp, "last_actionable_log.json")
            with open(ledger, "w", encoding="utf-8") as handle:
                handle.write("{}\n")
            with open(actionable, "w", encoding="utf-8") as handle:
                handle.write("{}\n")

            primary = find_primary_file(tmp, ["**/signal_ledger*.jsonl", "last_actionable_log.json", "last_trade_log.json"])
            self.assertEqual(os.path.normpath(primary), os.path.normpath(ledger))
        finally:
            shutil.rmtree(tmp, ignore_errors=True)

    def test_load_trade_history_reads_ledger_and_dedupes_legacy_snapshots(self) -> None:
        tmp = self._make_temp_dir("trade_history")
        try:
            outputs = os.path.join(tmp, "pm_outputs")
            os.makedirs(outputs, exist_ok=True)
            ledger_path = os.path.join(outputs, "signal_ledger.jsonl")
            legacy_path = os.path.join(outputs, "trades_20260402_120000.json")

            record = {
                "symbol": "EURUSD",
                "timeframe": "H1",
                "regime": "TREND",
                "strategy_name": "MomentumBurstStrategy",
                "direction": "buy",
                "entry_price": 1.0834,
                "stop_loss_price": 1.0801,
                "take_profit_price": 1.0902,
                "timestamp": "2026-04-02T12:00:00",
                "action": "EXECUTED",
            }

            with open(ledger_path, "w", encoding="utf-8") as handle:
                handle.write(json.dumps(record) + "\n")
            with open(legacy_path, "w", encoding="utf-8") as handle:
                json.dump([record], handle)

            trades = load_trade_history(tmp, max_files=10)
            self.assertEqual(len(trades), 1)
            self.assertEqual(trades[0]["symbol"], "EURUSD")
            self.assertEqual(trades[0]["timeframe"], "H1")
            self.assertEqual(trades[0]["action"], "EXECUTED")
        finally:
            shutil.rmtree(tmp, ignore_errors=True)

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

    def test_default_display_filters_surface_margin_and_min_lot_actions(self) -> None:
        config = deepcopy(DEFAULT_CONFIG)
        margin_entry = SignalEntry(
            symbol="XAUUSD",
            signal_direction="buy",
            entry_price=None,
            stop_loss_price=None,
            take_profit_price=None,
            reason="SKIPPED_MARGIN_REQUIRED",
            raw={"action": "SKIPPED_MARGIN_REQUIRED"},
        )
        min_lot_entry = SignalEntry(
            symbol="XAGUSD",
            signal_direction="buy",
            entry_price=None,
            stop_loss_price=None,
            take_profit_price=None,
            reason="BLOCKED_MIN_LOT_EXCEEDS_CAP",
            raw={"action": "BLOCKED_MIN_LOT_EXCEEDS_CAP"},
        )

        self.assertTrue(should_display_entry(margin_entry, config))
        self.assertTrue(should_display_entry(min_lot_entry, config))

        margin_entry.timestamp = datetime.now().isoformat()
        min_lot_entry.timestamp = datetime.now().isoformat()
        normalize_action_flags([margin_entry, min_lot_entry], config)
        self.assertTrue(margin_entry.valid_now)
        self.assertTrue(min_lot_entry.valid_now)

    def test_telegram_filter_only_allows_recent_configured_valid_signals(self) -> None:
        cfg = {
            "actions": ["EXECUTED"],
            "action_prefixes": [],
            "min_strength": 0.50,
            "max_signal_age_minutes": 60,
        }
        valid = SignalEntry(
            symbol="EURUSD",
            timeframe="H1",
            regime="TREND",
            strategy_name="HiddenStrategy",
            signal_direction="buy",
            entry_price=1.1,
            stop_loss_price=1.09,
            take_profit_price=1.12,
            signal_strength=0.72,
            timestamp=datetime.now().isoformat(),
            valid_now=True,
            reason="EXECUTED",
            raw={"action": "EXECUTED"},
        )
        blocked = SignalEntry(
            symbol="XAUUSD",
            signal_direction="sell",
            entry_price=3000,
            stop_loss_price=3010,
            take_profit_price=2980,
            signal_strength=0.90,
            timestamp=datetime.now().isoformat(),
            valid_now=True,
            reason="SKIPPED_MARGIN_REQUIRED",
            raw={"action": "SKIPPED_MARGIN_REQUIRED"},
        )

        selected = filter_telegram_entries([blocked, valid], cfg)

        self.assertEqual(selected, [valid])

    def test_telegram_message_hides_strategy_by_default_but_keeps_trade_layout(self) -> None:
        entry = SignalEntry(
            symbol="EURUSD",
            timeframe="H1",
            regime="TREND",
            strategy_name="DoNotExpose",
            signal_direction="buy",
            entry_price=1.10001,
            stop_loss_price=1.09501,
            take_profit_price=1.11001,
            signal_strength=0.72,
            timestamp="2026-04-02T12:00:00",
            valid_now=True,
            reason="EXECUTED",
            raw={"action": "EXECUTED"},
        )

        message = build_telegram_message(entry, {"include_strategy": False, "include_regime": True})

        self.assertIn("<b>FXPM Signal</b>", message)
        self.assertIn("EURUSD BUY", message)
        self.assertIn("Entry: <code>1.10001</code>", message)
        self.assertIn("Context: H1 / TREND", message)
        self.assertNotIn("DoNotExpose", message)

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

    def test_select_trade_candidate_does_not_fallback_to_mismatched_direction(self) -> None:
        entry = SignalEntry(
            symbol="EURUSD",
            timeframe="M5",
            regime="TREND",
            strategy_name="MomentumBurstStrategy",
            signal_direction="sell",
            timestamp="2026-02-09T00:00:00",
            raw={"action": "EXECUTED"},
        )
        candidate = {
            "symbol": "EURUSD",
            "timeframe": "M5",
            "regime": "TREND",
            "strategy": "MomentumBurstStrategy",
            "direction": "BUY",
            "timestamp": "2026-02-09T00:00:05",
            "_ts": datetime.fromisoformat("2026-02-09T00:00:05").timestamp(),
        }

        selected = select_trade_candidate(entry, [candidate], deepcopy(DEFAULT_CONFIG))
        self.assertIsNone(selected)

    def test_entry_alert_key_distinguishes_different_actions(self) -> None:
        base = SignalEntry(
            symbol="EURUSD",
            timeframe="M5",
            regime="TREND",
            strategy_name="MomentumBurstStrategy",
            signal_direction="buy",
            entry_price=1.1,
            stop_loss_price=1.09,
            take_profit_price=1.12,
            timestamp="2026-02-09T00:00:00",
            reason="EXECUTED",
            raw={"action": "EXECUTED"},
        )
        blocked = SignalEntry(
            symbol=base.symbol,
            timeframe=base.timeframe,
            regime=base.regime,
            strategy_name=base.strategy_name,
            signal_direction=base.signal_direction,
            entry_price=base.entry_price,
            stop_loss_price=base.stop_loss_price,
            take_profit_price=base.take_profit_price,
            timestamp=base.timestamp,
            reason="BLOCKED_RISK_CAP",
            raw={"action": "BLOCKED_RISK_CAP"},
        )

        self.assertNotEqual(entry_alert_key(base), entry_alert_key(blocked))


if __name__ == "__main__":
    unittest.main()
