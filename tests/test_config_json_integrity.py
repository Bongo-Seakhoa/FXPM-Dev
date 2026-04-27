import json
import unittest
from pathlib import Path


class ConfigJsonIntegrityTests(unittest.TestCase):
    def test_config_json_has_no_duplicate_object_keys(self) -> None:
        config_path = Path(__file__).resolve().parents[1] / "config.json"
        duplicates = []

        def _hook(pairs):
            seen = {}
            for key, value in pairs:
                if key in seen:
                    duplicates.append(key)
                seen[key] = value
            return seen

        json.loads(config_path.read_text(encoding="utf-8"), object_pairs_hook=_hook)
        self.assertEqual(duplicates, [])


if __name__ == "__main__":
    unittest.main()
