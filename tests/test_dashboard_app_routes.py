import json
import os
import shutil
import unittest
import uuid
from unittest.mock import patch

from pm_dashboard.app import create_app


class DashboardAppRouteTests(unittest.TestCase):
    def setUp(self) -> None:
        self.pm_root = os.path.join(os.getcwd(), f".tmp_dashboard_app_{uuid.uuid4().hex}")
        os.makedirs(self.pm_root, exist_ok=True)
        self.config_path = os.path.join(self.pm_root, "dashboard_config.json")
        self._write_dashboard_config({"pm_root": self.pm_root, "refresh_interval_sec": 5})

    def tearDown(self) -> None:
        shutil.rmtree(self.pm_root, ignore_errors=True)

    def _write_dashboard_config(self, payload) -> None:
        with open(self.config_path, "w", encoding="utf-8") as handle:
            json.dump(payload, handle)

    def _write_trade_file(self, rows) -> None:
        outputs_dir = os.path.join(self.pm_root, "pm_outputs")
        os.makedirs(outputs_dir, exist_ok=True)
        path = os.path.join(outputs_dir, "trades_20260209_000000.json")
        with open(path, "w", encoding="utf-8") as handle:
            json.dump(rows, handle)

    def _write_root_config(self, payload) -> None:
        with open(os.path.join(self.pm_root, "config.json"), "w", encoding="utf-8") as handle:
            json.dump(payload, handle)

    def _write_pm_configs(self, filename, payload) -> None:
        with open(os.path.join(self.pm_root, filename), "w", encoding="utf-8") as handle:
            json.dump(payload, handle)

    def test_api_config_rejects_invalid_json_and_persists_atomically(self) -> None:
        app = create_app(self.config_path, start_background_workers=False)
        client = app.test_client()

        bad = client.post(
            "/api/config",
            data="{invalid",
            content_type="application/json",
        )
        self.assertEqual(bad.status_code, 400)

        good = client.post("/api/config", json={"refresh_interval_sec": 11})
        self.assertEqual(good.status_code, 200)
        with open(self.config_path, "r", encoding="utf-8") as handle:
            saved = json.load(handle)
        self.assertEqual(saved["refresh_interval_sec"], 11)

    def test_api_simulate_respects_end_date_without_start_date(self) -> None:
        self._write_trade_file(
            [
                {
                    "timestamp": "2026-02-01T10:00:00",
                    "symbol": "EURUSD",
                    "direction": "LONG",
                    "price": 1.1,
                    "sl": 1.09,
                    "tp": 1.11,
                    "status": "CLOSED",
                    "close_reason": "TP_HIT",
                    "pnl": 12.5,
                },
                {
                    "timestamp": "2026-02-03T10:00:00",
                    "symbol": "EURUSD",
                    "direction": "LONG",
                    "price": 1.2,
                    "sl": 1.19,
                    "tp": 1.21,
                    "status": "CLOSED",
                    "close_reason": "SL_HIT",
                    "pnl": -8.0,
                },
            ]
        )

        app = create_app(self.config_path, start_background_workers=False)
        client = app.test_client()
        response = client.post(
            "/api/simulate",
            json={
                "end_date": "2026-02-02T23:59:59",
                "max_trades": 10,
                "initial_capital": 10000,
            },
        )

        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertTrue(payload["success"])
        self.assertEqual(payload["total_trades"], 1)
        self.assertEqual(len(payload["trades"]), 1)
        self.assertAlmostEqual(payload["metrics"]["total_pnl"], 12.5, places=2)

    def test_api_config_rejects_non_object_payload(self) -> None:
        app = create_app(self.config_path, start_background_workers=False)
        client = app.test_client()

        response = client.post("/api/config", json=["not", "an", "object"])
        self.assertEqual(response.status_code, 400)
        payload = response.get_json()
        self.assertFalse(payload["success"])
        self.assertIn("payload", payload["error"].lower())

    def test_remote_config_write_requires_token(self) -> None:
        with patch.dict(os.environ, {"PM_DASHBOARD_WRITE_TOKEN": ""}):
            app = create_app(self.config_path, start_background_workers=False)
            client = app.test_client()

            response = client.post(
                "/api/config",
                json={"refresh_interval_sec": 7},
                environ_base={"REMOTE_ADDR": "192.0.2.10"},
            )

        self.assertEqual(response.status_code, 403)
        payload = response.get_json()
        self.assertFalse(payload["success"])
        self.assertIn("remote dashboard writes", payload["error"].lower())

    def test_loopback_config_write_rejects_cross_origin_without_token(self) -> None:
        with patch.dict(os.environ, {"PM_DASHBOARD_WRITE_TOKEN": ""}):
            app = create_app(self.config_path, start_background_workers=False)
            client = app.test_client()

            response = client.post(
                "/api/config",
                json={"refresh_interval_sec": 8},
                headers={"Origin": "http://example.test"},
            )

        self.assertEqual(response.status_code, 403)
        payload = response.get_json()
        self.assertFalse(payload["success"])
        self.assertIn("cross-origin", payload["error"].lower())

    def test_config_write_accepts_token_when_configured(self) -> None:
        with patch.dict(os.environ, {"PM_DASHBOARD_WRITE_TOKEN": "secret-token"}):
            app = create_app(self.config_path, start_background_workers=False)
            client = app.test_client()

            unauth = client.post("/api/config", json={"refresh_interval_sec": 8})
            authed = client.post(
                "/api/config",
                json={"refresh_interval_sec": 9},
                headers={"X-PM-Dashboard-Token": "secret-token"},
            )

        self.assertEqual(unauth.status_code, 401)
        self.assertEqual(authed.status_code, 200)
        with open(self.config_path, "r", encoding="utf-8") as handle:
            saved = json.load(handle)
        self.assertEqual(saved["refresh_interval_sec"], 9)

    def test_strategies_follow_pipeline_winner_ledger_when_auto(self) -> None:
        self._write_root_config({"pipeline": {"winner_ledger_path": "pm_configs_high_risk.json"}})
        self._write_pm_configs(
            "pm_configs.json",
            {
                "EURUSD": {
                    "symbol": "EURUSD",
                    "timeframe": "M5",
                    "default_config": {"strategy_name": "LegacyStrategy"},
                }
            },
        )
        self._write_pm_configs(
            "pm_configs_high_risk.json",
            {
                "XAUUSD": {
                    "symbol": "XAUUSD",
                    "timeframe": "H1",
                    "default_config": {"strategy_name": "HighRiskStrategy"},
                }
            },
        )

        app = create_app(self.config_path, start_background_workers=False)
        client = app.test_client()
        response = client.get("/api/strategies")

        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        symbols = {row["symbol"] for row in payload["rows"]}
        self.assertEqual(symbols, {"XAUUSD"})

    def test_strategies_preserve_zero_scores_and_trade_counts(self) -> None:
        self._write_root_config({"pipeline": {"winner_ledger_path": "pm_configs_high_risk.json"}})
        self._write_pm_configs(
            "pm_configs_high_risk.json",
            {
                "EURUSD": {
                    "symbol": "EURUSD",
                    "is_validated": True,
                    "regime_configs": {
                        "M5": {
                            "RANGE": {
                                "strategy_name": "NoTradeWinner",
                                "quality_score": "0.0",
                                "regime_train_trades": "0",
                                "regime_val_trades": 0,
                            }
                        }
                    },
                }
            },
        )

        app = create_app(self.config_path, start_background_workers=False)
        client = app.test_client()
        response = client.get("/api/strategies")

        self.assertEqual(response.status_code, 200)
        row = response.get_json()["rows"][0]
        self.assertEqual(row["quality_score"], 0.0)
        self.assertEqual(row["regime_train_trades"], 0)
        self.assertEqual(row["regime_val_trades"], 0)

    def test_trade_history_uses_pipeline_winner_ledger_for_enrichment(self) -> None:
        self._write_root_config({"pipeline": {"winner_ledger_path": "pm_configs_high_risk.json"}})
        self._write_pm_configs(
            "pm_configs_high_risk.json",
            {
                "GBPUSD": {
                    "symbol": "GBPUSD",
                    "timeframe": "H4",
                    "regime_configs": {
                        "H4": {
                            "TREND": {"strategy_name": "HighRiskTrend"}
                        }
                    },
                }
            },
        )
        self._write_trade_file(
            [
                {
                    "timestamp": "2026-02-09T01:00:00",
                    "symbol": "GBPUSD",
                    "direction": "SHORT",
                    "volume": 0.01,
                    "price": 1.25,
                    "sl": 1.26,
                    "tp": 1.24,
                    "status": "EXECUTED",
                }
            ]
        )

        app = create_app(self.config_path, start_background_workers=False)
        client = app.test_client()
        response = client.get("/api/trades")

        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertEqual(payload["trades"][0]["timeframe"], "H4")
        self.assertEqual(payload["trades"][0]["strategy"], "HighRiskTrend")

    def test_live_command_reports_ledger_readiness_from_active_winner_file(self) -> None:
        self._write_root_config({
            "symbols": ["EURUSD", "GBPUSD"],
            "pipeline": {
                "winner_ledger_path": "pm_configs_high_risk.json",
                "initial_capital": 5000,
            },
        })
        self._write_pm_configs(
            "pm_configs_high_risk.json",
            {
                "EURUSD": {
                    "symbol": "EURUSD",
                    "is_validated": True,
                    "valid_until": "2099-01-01T00:00:00",
                    "optimized_at": "2026-04-01T00:00:00",
                    "regime_configs": {
                        "H1": {
                            "TREND": {
                                "strategy_name": "TrendWinner",
                                "quality_score": 0.8,
                                "validation_status": "validated",
                            }
                        }
                    },
                }
            },
        )

        app = create_app(self.config_path, start_background_workers=False)
        client = app.test_client()
        response = client.get("/api/live-command")

        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertEqual(payload["active_pm_configs_path"], "pm_configs_high_risk.json")
        self.assertEqual(payload["ledger"]["configured_symbol_count"], 2)
        self.assertEqual(payload["ledger"]["optimized_symbol_count"], 1)
        self.assertEqual(payload["ledger"]["missing_symbol_count"], 1)
        self.assertEqual(payload["ledger"]["tradeable_winners"], 1)
        self.assertEqual(payload["trades"]["equity_estimate"], 5000.0)
        self.assertEqual(payload["readiness"]["tone"], "warning")

    def test_config_round_trip_includes_telegram_settings_without_token_value(self) -> None:
        app = create_app(self.config_path, start_background_workers=False)
        client = app.test_client()

        response = client.post(
            "/api/config",
            json={
                "telegram": {
                    "enabled": True,
                    "chat_id": "-100123",
                    "bot_token_env": "PM_DASHBOARD_TEST_TOKEN",
                    "min_strength": 0.42,
                    "max_signal_age_minutes": 12,
                    "include_strategy": True,
                    "include_regime": False,
                }
            },
        )
        self.assertEqual(response.status_code, 200)

        payload = client.get("/api/config").get_json()
        self.assertTrue(payload["telegram"]["enabled"])
        self.assertEqual(payload["telegram"]["chat_id"], "-100123")
        self.assertEqual(payload["telegram"]["bot_token_env"], "PM_DASHBOARD_TEST_TOKEN")
        self.assertFalse(payload["telegram"]["bot_token_configured"])
        self.assertNotIn("bot_token", payload["telegram"])


if __name__ == "__main__":
    unittest.main()
