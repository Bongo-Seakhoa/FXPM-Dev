import unittest
import pandas as pd

from pm_core import PipelineConfig, DataSplitter


class DataSplitterTests(unittest.TestCase):
    def test_split_indices(self):
        cfg = PipelineConfig(train_pct=80.0, overlap_pct=10.0, holdout_pct=10.0)
        splitter = DataSplitter(cfg)
        df = pd.DataFrame({"Open": range(100), "High": range(100), "Low": range(100), "Close": range(100)})
        split = splitter.split(df)
        indices = splitter.get_split_indices(len(df))

        self.assertEqual(indices["train"], (0, 80))
        self.assertEqual(indices["warmup"], (70, 80))
        self.assertEqual(indices["validation"], (80, 90))
        self.assertEqual(indices["validation_with_warmup"], (70, 90))
        self.assertEqual(indices["holdout"], (90, 100))
        self.assertEqual(indices["holdout_with_warmup"], (80, 100))

        self.assertEqual(len(split["train"]), 80)
        self.assertEqual(len(split["validation_with_warmup"]), 20)
        self.assertEqual(len(split["holdout_with_warmup"]), 20)


if __name__ == "__main__":
    unittest.main()
