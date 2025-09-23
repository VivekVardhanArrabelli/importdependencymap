import os
import unittest
from pathlib import Path

from server import forex


class ForexTest(unittest.TestCase):
    def setUp(self):
        forex.reset_cache()
        os.environ["FX_RATES_FILE"] = str(Path("data/fx_rates.csv").resolve())

    def tearDown(self):
        forex.reset_cache()

    def test_monthly_rate_returns_value(self):
        rate = forex.monthly_rate(2024, 1)
        self.assertAlmostEqual(rate, 82.95, places=2)

    def test_missing_month_raises(self):
        with self.assertRaises(RuntimeError):
            forex.monthly_rate(2025, 1)


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
