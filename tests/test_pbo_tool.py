import unittest
import hashlib
import json
import tempfile
from pathlib import Path

import numpy as np

from tools.pbo import load_oos_manifest_matrix, pbo_cscv


class PboToolTests(unittest.TestCase):
    def test_pbo_rejects_insufficient_matrix(self):
        result = pbo_cscv(np.ones((4, 1), dtype=np.float32), n_blocks=4)

        self.assertIsNone(result["pbo"])
        self.assertEqual(result["n_combos"], 0)

    def test_pbo_accepts_exported_matrix_shape(self):
        matrix = np.asarray(
            [
                [0.01, 0.00, -0.01],
                [0.02, -0.01, 0.00],
                [0.01, 0.01, -0.02],
                [-0.01, 0.00, 0.01],
                [0.02, 0.01, -0.01],
                [0.01, -0.02, 0.00],
                [0.00, 0.01, 0.01],
                [0.02, -0.01, -0.01],
            ],
            dtype=np.float32,
        )

        result = pbo_cscv(matrix, n_blocks=4, candidate_ids=["a", "b", "c"])

        self.assertEqual(result["n_configs"], 3)
        self.assertEqual(result["n_obs"], 8)
        self.assertIsNotNone(result["pbo"])
        self.assertEqual([stat["candidate_id"] for stat in result["candidate_stats"]], ["a", "b", "c"])
        self.assertEqual(
            sum(stat["selected_count"] for stat in result["candidate_stats"]),
            result["n_combos"],
        )
        self.assertTrue(
            any(stat["selected_oos_pbo"] is not None for stat in result["candidate_stats"])
        )

    def test_load_oos_manifest_matrix_verifies_engine_sidecar(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp = Path(tmp_dir)
            returns = np.asarray(
                [
                    [0.01, 0.00],
                    [0.02, -0.01],
                    [-0.01, 0.01],
                    [0.00, 0.02],
                    [0.01, -0.02],
                    [0.02, 0.01],
                    [0.00, 0.00],
                    [0.01, -0.01],
                ],
                dtype=np.float32,
            )
            candidate_ids = np.asarray(["a", "b"], dtype=str)
            digest = hashlib.sha1(
                returns.tobytes()
                + json.dumps(candidate_ids.tolist(), separators=(",", ":")).encode("utf-8")
            ).hexdigest()
            matrix_path = tmp / f"EURUSD_oos_returns_{digest[:12]}.npz"
            np.savez_compressed(matrix_path, returns=returns, candidate_ids=candidate_ids)
            manifest_path = tmp / "EURUSD_oos_manifest.json"
            manifest_path.write_text(
                json.dumps(
                    {
                        "format_version": "oos_candidate_matrix_v1",
                        "symbol": "EURUSD",
                        "record_count": 2,
                        "matrix_path": str(matrix_path),
                        "matrix_digest_sha1": digest,
                        "matrix_contract": {"returns_shape": [8, 2]},
                    }
                ),
                encoding="utf-8",
            )

            loaded, manifest, loaded_ids = load_oos_manifest_matrix(manifest_path)

            self.assertEqual(loaded.shape, (8, 2))
            self.assertEqual(manifest["symbol"], "EURUSD")
            self.assertEqual(loaded_ids, ["a", "b"])

    def test_load_oos_manifest_matrix_accepts_empty_completion_sidecar(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp = Path(tmp_dir)
            returns = np.zeros((0, 0), dtype=np.float32)
            candidate_ids = np.asarray([], dtype=str)
            digest = hashlib.sha1(
                returns.tobytes()
                + json.dumps(candidate_ids.tolist(), separators=(",", ":")).encode("utf-8")
            ).hexdigest()
            matrix_path = tmp / f"BTCETH_oos_returns_{digest[:12]}.npz"
            np.savez_compressed(matrix_path, returns=returns, candidate_ids=candidate_ids)
            manifest_path = tmp / "BTCETH_oos_manifest.json"
            manifest_path.write_text(
                json.dumps(
                    {
                        "format_version": "oos_candidate_matrix_v1",
                        "symbol": "BTCETH",
                        "record_count": 0,
                        "completion_status": "NO_CANDIDATE_RECORDS",
                        "matrix_path": str(matrix_path),
                        "matrix_digest_sha1": digest,
                        "matrix_contract": {"returns_shape": [0, 0]},
                    }
                ),
                encoding="utf-8",
            )

            loaded, manifest, loaded_ids = load_oos_manifest_matrix(manifest_path)

            self.assertEqual(loaded.shape, (0, 0))
            self.assertEqual(manifest["symbol"], "BTCETH")
            self.assertEqual(loaded_ids, [])


if __name__ == "__main__":
    unittest.main()
