import os
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

from server import forex
from server.etl import dgcis


class DGCISTest(unittest.TestCase):
    def setUp(self):
        forex.reset_cache()
        os.environ["FX_RATES_FILE"] = str(Path("data/fx_rates.csv").resolve())

    def tearDown(self):
        forex.reset_cache()

    def test_load_csv_parses_and_converts(self):
        records = dgcis.load_csv(Path("data/dgcis_sample.csv"))
        self.assertTrue(records)
        sample = records[0]
        self.assertIsNotNone(sample.value_inr)
        self.assertIsNotNone(sample.value_usd)
        self.assertIsNotNone(sample.fx_rate)

    @patch("server.etl.dgcis.db.insert_monthly")
    @patch("server.etl.dgcis.db.upsert_product")
    def test_load_inserts_into_database(self, mock_upsert, mock_insert):
        records = dgcis.load_csv(Path("data/dgcis_sample.csv"))
        conn = MagicMock()
        products, rows = dgcis.load(conn, records)
        self.assertGreater(products, 0)
        self.assertEqual(rows, len(records))
        self.assertEqual(mock_upsert.call_count, products)
        self.assertEqual(mock_insert.call_count, rows)

    def test_missing_file_raises(self):
        with self.assertRaises(RuntimeError):
            dgcis.run(MagicMock(), source=Path("data/missing.csv"))


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
