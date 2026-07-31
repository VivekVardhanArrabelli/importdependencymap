"""Postgres integration coverage for schema migration and full-catalog logic."""
from __future__ import annotations

import os
import unittest

import psycopg2

from server import db, jobs, main


TEST_DATABASE_URL = os.getenv("TEST_DATABASE_URL")


@unittest.skipUnless(TEST_DATABASE_URL, "TEST_DATABASE_URL is not configured")
class PostgresIntegrationTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        os.environ["DATABASE_URL"] = str(TEST_DATABASE_URL)
        cls._reset_to_legacy_schema()

    @classmethod
    def tearDownClass(cls):
        cls._drop_application_tables()

    @classmethod
    def _drop_application_tables(cls):
        with psycopg2.connect(str(TEST_DATABASE_URL)) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    DROP TABLE IF EXISTS domestic_capability CASCADE;
                    DROP TABLE IF EXISTS import_progress CASCADE;
                    DROP TABLE IF EXISTS baseline_imports CASCADE;
                    DROP TABLE IF EXISTS monthly_imports CASCADE;
                    DROP TABLE IF EXISTS products CASCADE;
                    """
                )

    @classmethod
    def _reset_to_legacy_schema(cls):
        cls._drop_application_tables()
        with psycopg2.connect(str(TEST_DATABASE_URL)) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    CREATE TABLE products (
                        hs_code TEXT PRIMARY KEY,
                        title TEXT,
                        description TEXT,
                        sectors TEXT[] DEFAULT ARRAY[]::TEXT[],
                        granularity INT DEFAULT 6,
                        capex_min NUMERIC,
                        capex_max NUMERIC,
                        created_at timestamptz DEFAULT now(),
                        updated_at timestamptz DEFAULT now()
                    );
                    CREATE TABLE monthly_imports (
                        id SERIAL PRIMARY KEY,
                        hs_code TEXT REFERENCES products(hs_code),
                        year INT NOT NULL,
                        month INT NOT NULL,
                        value_usd NUMERIC,
                        value_inr NUMERIC,
                        fx_rate NUMERIC,
                        qty NUMERIC,
                        partner_country TEXT
                    );
                    CREATE UNIQUE INDEX uq_monthly_imports_hs_ym_partner
                      ON monthly_imports (hs_code, year, month, partner_country);
                    INSERT INTO products (hs_code, title, description, sectors)
                    VALUES ('850760', 'Lithium-ion accumulators', 'Legacy row',
                            ARRAY['industrial']);
                    INSERT INTO monthly_imports (
                        hs_code, year, month, value_usd, partner_country
                    )
                    VALUES
                        ('850760', 2024, 1, 10, NULL),
                        ('850760', 2024, 1, 20, NULL);
                    """
                )

    def test_migrate_replace_recompute_and_read(self):
        with db.connect() as conn:
            db.init_db(conn)
        with db.connect() as conn:
            db.init_db(conn)
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT COUNT(*), MIN(partner_country), MIN(source)
                    FROM monthly_imports
                    WHERE hs_code = '850760' AND year = 2024 AND month = 1
                    """
                )
                count, partner, source = cur.fetchone()
                self.assertEqual(count, 1)
                self.assertEqual(partner, "UNKNOWN")
                self.assertEqual(source, "legacy")

            db.bulk_upsert_products(
                conn,
                [
                    (
                        "850760",
                        "Lithium-ion accumulators",
                        "Lithium-ion accumulators",
                        ["industrial"],
                        None,
                        None,
                    ),
                    (
                        "999999",
                        "No reported imports",
                        "Completeness zero-fill check",
                        [],
                        None,
                        None,
                    ),
                ],
            )

            aggregate_rows = []
            partner_rows = []
            for month in range(1, 13):
                aggregate_rows.append(
                    (
                        "850760",
                        2025,
                        month,
                        float(month * 100),
                        None,
                        None,
                        None,
                        None,
                        None,
                        None,
                        "W00",
                        "comtrade",
                        True,
                    )
                )
                for partner, value in (("CHN", 80.0), ("KOR", 20.0)):
                    partner_rows.append(
                        (
                            "850760",
                            2025,
                            month,
                            value,
                            None,
                            None,
                            None,
                            None,
                            None,
                            None,
                            partner,
                            "comtrade_partner",
                            False,
                        )
                    )
            db.bulk_insert_monthly(conn, [*aggregate_rows, *partner_rows])
            deleted = db.delete_comtrade_aggregate_periods(conn, ["202501"])
            self.assertEqual(deleted, 1)
            replacement = list(aggregate_rows[0])
            replacement[3] = 999.0
            db.bulk_insert_monthly(conn, [tuple(replacement)])

            baseline = jobs.recompute_baseline(conn)
            progress = jobs.recompute_progress(conn)
            self.assertEqual(baseline["processed"], 2)
            self.assertEqual(progress["processed"], 2)
            self.assertEqual(progress["scored"], 1)

            imported_series = db.fetch_last_36m(conn, "850760")
            zero_series = db.fetch_last_36m(conn, "999999")
            self.assertEqual(len(imported_series), 12)
            self.assertEqual(float(imported_series[0]["value_usd"]), 999.0)
            self.assertEqual(len(zero_series), 12)
            self.assertTrue(
                all(float(row["value_usd"]) == 0 for row in zero_series)
            )

        listing = main.list_products(
            sectors=None,
            combine="OR",
            min_capex=None,
            max_capex=None,
            sort="opportunity",
            limit=10,
            offset=0,
            q=None,
        )
        self.assertEqual(listing["total"], 2)
        self.assertEqual(listing["latest_period"], "2025-12")
        self.assertEqual(listing["items"][0]["hs_code"], "850760")
        self.assertIsNotNone(listing["items"][0]["opportunity_score"])
        self.assertIsNone(listing["items"][1]["opportunity_score"])

        detail = main.product_detail("850760")
        self.assertEqual(len(detail["timeseries"]), 12)
        self.assertEqual(len(detail["partners"]), 2)
        self.assertEqual(detail["partner_period"], "202512")
        self.assertAlmostEqual(detail["progress"]["hhi_current"], 0.68)


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
