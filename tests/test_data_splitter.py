import unittest
import pandas as pd

from pm_core import PipelineConfig, DataSplitter


class DataSplitterTests(unittest.TestCase):
    def test_split_indices(self):
        cfg = PipelineConfig(train_pct=80.0, overlap_pct=10.0)
        splitter = DataSplitter(cfg)
        df = pd.DataFrame({"Open": range(100), "High": range(100), "Low": range(100), "Close": range(100)})
        train_df, val_df = splitter.split(df)

        self.assertEqual(len(train_df), 80)
        self.assertEqual(len(val_df), 30)
        # Overlap starts at 70
        self.assertEqual(train_df.index[-1], 79)
        self.assertEqual(val_df.index[0], 70)


if __name__ == "__main__":
    unittest.main()
