import json
import os
import shutil
import unittest
import uuid

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


if __name__ == "__main__":
    unittest.main()
