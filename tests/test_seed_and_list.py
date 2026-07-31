import os
import unittest
from unittest.mock import patch

from fastapi import HTTPException

from server import main


class SeedAndListingTest(unittest.TestCase):
    def setUp(self):
        os.environ.pop("DATABASE_URL", None)

    def test_seed_requires_database(self):
        with patch("server.main._verify_admin"):
            with self.assertRaises(HTTPException) as ctx:
                main.seed_database(None)
        self.assertEqual(ctx.exception.status_code, 500)

    def test_listing_requires_database(self):
        with self.assertRaises(HTTPException) as ctx:
            main.list_products(
                sectors=None,
                combine="OR",
                min_capex=None,
                max_capex=None,
                sort="opportunity",
                limit=10,
            )
        self.assertEqual(ctx.exception.status_code, 500)

    def test_detail_requires_database(self):
        with self.assertRaises(HTTPException) as ctx:
            main.product_detail("123456")
        self.assertEqual(ctx.exception.status_code, 500)

    def test_seed_rows_use_canonical_hs_sector_taxonomy(self):
        row = main._parse_csv_row({
            "hs_code": "848180",
            "title": "Industrial valves",
            "description": "General purpose industrial valves",
            "sectors": "{industrial,metals}",
            "capex_min": "",
            "capex_max": "",
            "seed_month_value": "",
            "top_country": "",
        })
        self.assertEqual(row["hs_code"], "848180")
        self.assertEqual(row["sectors"], ["machinery"])


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
