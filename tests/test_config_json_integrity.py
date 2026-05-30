import json
import unittest
from dataclasses import fields
from pathlib import Path

from pm_core import PipelineConfig


class ConfigJsonIntegrityTests(unittest.TestCase):
    repo_root = Path(__file__).resolve().parents[1]

    def _assert_no_duplicate_object_keys(self, config_path: Path) -> None:
        duplicates = []

        def _hook(pairs):
            seen = {}
            for key, value in pairs:
                if key in seen:
                    duplicates.append(key)
                seen[key] = value
            return seen

        json.loads(config_path.read_text(encoding="utf-8"), object_pairs_hook=_hook)
        self.assertEqual(duplicates, [], str(config_path))

    def test_config_json_has_no_duplicate_object_keys(self) -> None:
        self._assert_no_duplicate_object_keys(self.repo_root / "config.json")

    def test_discovery_honest_config_has_no_duplicate_object_keys(self) -> None:
        self._assert_no_duplicate_object_keys(self.repo_root / "config.discovery_honest.json")

    def test_discovery_honest_config_locks_reopt_contract(self) -> None:
        config_path = self.repo_root / "config.discovery_honest.json"
        data = json.loads(config_path.read_text(encoding="utf-8"))
        pipeline = data["pipeline"]

        self.assertEqual(pipeline["backtest_spread_mode"], "conservative_max")
        self.assertEqual(pipeline["clean_holdout_pct"], 15.0)
        self.assertEqual(data["_meta"]["strategy_roster_count"], 76)
        self.assertEqual(data["_meta"]["holdout_unit"], "percent_of_stage2_selection")
        self.assertEqual(data["_meta"]["scope"], "research_discovery_only_not_live_trading")
        self.assertNotIn("position", data)
        self.assertNotIn("mt5", data)
        self.assertEqual(data["broker_specs_path"], "broker_specs.json")
        self.assertTrue((self.repo_root / data["broker_specs_path"]).exists())
        self.assertEqual(pipeline["output_dir"], "./pm_outputs/discovery_honest")
        self.assertEqual(
            pipeline["winner_ledger_path"],
            "pm_outputs/discovery_honest/pm_configs_discovery_honest.json",
        )
        self.assertGreaterEqual(len(data["symbols"]), 60)

        allowed = {field.name for field in fields(PipelineConfig)}
        cfg = PipelineConfig(**{k: v for k, v in pipeline.items() if k in allowed})
        self.assertEqual(cfg.backtest_spread_mode, "conservative_max")
        self.assertAlmostEqual(cfg.clean_holdout_pct, 15.0, places=6)
        self.assertGreater(cfg.clean_holdout_pct, 1.0)


if __name__ == "__main__":
    unittest.main()
