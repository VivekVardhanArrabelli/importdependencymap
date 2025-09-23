import os
import unittest

from server import main


class SeedAndListingTest(unittest.TestCase):
    def setUp(self):
        os.environ.pop("DATABASE_URL", None)

    def test_seed_fallback_and_detail(self):
        payload = main.seed_database(None)
        self.assertEqual(payload.get("source"), "csv_fallback")
        self.assertGreaterEqual(payload.get("items", 0), 1)

        listing = main.list_products(
            sectors=None,
            combine="OR",
            min_capex=None,
            max_capex=None,
            sort="opportunity",
            limit=10,
        )
        items = listing.get("items", [])
        self.assertTrue(items)
        detail = main.product_detail(items[0]["hs_code"])
        self.assertGreaterEqual(len(detail.get("timeseries", [])), 12)


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
