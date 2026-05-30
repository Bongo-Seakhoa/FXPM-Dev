import unittest

import numpy as np

from tools.pbo import pbo_cscv


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

        result = pbo_cscv(matrix, n_blocks=4)

        self.assertEqual(result["n_configs"], 3)
        self.assertEqual(result["n_obs"], 8)
        self.assertIsNotNone(result["pbo"])


if __name__ == "__main__":
    unittest.main()
