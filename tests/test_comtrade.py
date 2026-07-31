"""Contract tests for the UN Comtrade v1 ingestion path.

The response fixture was captured from the public v1 preview endpoint on
2026-07-31 for India's January 2024 imports of HS 850760 from China.  Network
access is deliberately mocked in this suite so upstream availability does not
make CI flaky.
"""
from __future__ import annotations

import copy
import json
import os
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

from server.etl import comtrade


FIXTURES = Path(__file__).parent / "fixtures"


def load_fixture(name: str) -> dict:
    return json.loads((FIXTURES / name).read_text(encoding="utf-8"))


class ComtradeEndpointContractTest(unittest.TestCase):
    def test_default_reporter_is_current_india_code(self):
        with patch.dict(os.environ, {}, clear=True):
            self.assertEqual(comtrade._normalize_reporter_code(None), "699")
            self.assertEqual(comtrade._normalize_reporter_code("India"), "699")

    def test_numeric_reporter_override_is_preserved(self):
        with patch.dict(os.environ, {}, clear=True):
            self.assertEqual(comtrade._normalize_reporter_code("840"), "840")

    def test_retired_india_reporter_is_rejected(self):
        with patch.dict(os.environ, {}, clear=True):
            with self.assertRaisesRegex(RuntimeError, "historical India"):
                comtrade._normalize_reporter_code("356")

    def test_retired_india_reporter_environment_is_migrated(self):
        with patch.dict(os.environ, {"COMTRADE_REPORTER": "356"}, clear=True):
            self.assertEqual(comtrade._normalize_reporter_code(None), "699")

    def test_unkeyed_endpoint_is_public_preview(self):
        with patch.dict(os.environ, {}, clear=True):
            parts = comtrade._build_path_segments("C", "M", "HS")
            endpoint = comtrade._resolve_endpoint(*parts)

        self.assertEqual(
            endpoint,
            "https://comtradeapi.un.org/public/v1/preview/C/M/HS",
        )

    def test_keyed_endpoint_is_authenticated_get(self):
        with patch.dict(os.environ, {"COMTRADE_KEY": "test-key"}, clear=True):
            parts = comtrade._build_path_segments("C", "M", "HS")
            endpoint = comtrade._resolve_endpoint(*parts)

        self.assertEqual(
            endpoint,
            "https://comtradeapi.un.org/data/v1/get/C/M/HS",
        )

    def test_keyed_endpoint_ignores_legacy_public_base(self):
        with patch.dict(
            os.environ,
            {
                "COMTRADE_KEY": "test-key",
                "COMTRADE_BASE": "https://comtradeapi.un.org/public/v1",
            },
            clear=True,
        ):
            parts = comtrade._build_path_segments("C", "M", "HS")
            endpoint = comtrade._resolve_endpoint(*parts)

        self.assertEqual(
            endpoint,
            "https://comtradeapi.un.org/data/v1/get/C/M/HS",
        )


class ComtradeCatalogContractTest(unittest.TestCase):
    def setUp(self):
        self.payload = load_fixture("comtrade_h6_reference_minimal.json")

    def test_catalog_parser_keeps_only_six_digit_leaf_products(self):
        products = comtrade._parse_catalog(self.payload)

        self.assertEqual(
            [product.hs_code for product in products],
            ["850760", "850780"],
        )
        self.assertEqual(
            products[0].title,
            "Electric accumulators; lithium-ion, including separators, whether or not rectangular (including square)",
        )

    def test_catalog_parser_accepts_string_aggregation_levels_and_leaf_flags(self):
        payload = copy.deepcopy(self.payload)
        for row in payload["results"]:
            row["aggrlevel"] = str(row["aggrlevel"])

        products = comtrade._parse_catalog(payload)

        self.assertEqual(
            [product.hs_code for product in products],
            ["850760", "850780"],
        )

    def test_catalog_parser_rejects_six_digit_non_leaf_rows(self):
        payload = copy.deepcopy(self.payload)
        payload["results"].extend(
            [
                {
                    "id": "850700",
                    "text": "850700 - Invented aggregate",
                    "parent": "8507",
                    "isLeaf": "0",
                    "aggrlevel": "6",
                },
                {
                    "id": "850799",
                    "text": "850799 - Wrong aggregation level",
                    "parent": "8507",
                    "isLeaf": "1",
                    "aggrlevel": "4",
                },
            ]
        )

        products = comtrade._parse_catalog(payload)

        self.assertEqual(
            [product.hs_code for product in products],
            ["850760", "850780"],
        )

    def test_sync_catalog_upserts_every_leaf_product(self):
        products = comtrade._parse_catalog(self.payload)
        conn = MagicMock()

        with patch(
            "server.etl.comtrade.db.bulk_upsert_products",
            return_value=2,
        ) as mock_upsert, patch(
            "server.etl.comtrade.db.replace_product_catalog_membership",
        ) as mock_membership:
            synced = comtrade.sync_product_catalog(conn, products)

        self.assertEqual(synced, 2)
        mock_upsert.assert_called_once()
        rows = mock_upsert.call_args.args[1]
        self.assertEqual(
            [row[0] for row in rows],
            ["850760", "850780"],
        )
        self.assertEqual(
            rows[0][1],
            "Electric accumulators; lithium-ion, including separators, whether or not rectangular (including square)",
        )
        mock_membership.assert_called_once_with(conn, ["850760", "850780"])

    @patch("server.etl.comtrade.db.replace_product_catalog_membership")
    @patch(
        "server.etl.comtrade.db.bulk_upsert_products",
        return_value=2,
    )
    @patch("server.etl.comtrade.fetch_product_catalog")
    def test_sync_catalog_fetches_reference_when_products_are_not_supplied(
        self,
        mock_fetch,
        mock_upsert,
        mock_membership,
    ):
        mock_fetch.return_value = comtrade._parse_catalog(self.payload)

        synced = comtrade.sync_product_catalog(MagicMock())

        self.assertEqual(synced, 2)
        mock_fetch.assert_called_once_with()
        mock_upsert.assert_called_once()
        self.assertEqual(len(mock_upsert.call_args.args[1]), 2)
        mock_membership.assert_called_once()


class ComtradeCurrentSchemaContractTest(unittest.TestCase):
    def setUp(self):
        self.payload = load_fixture("comtrade_v1_current.json")
        self.row = self.payload["data"][0]

    def test_captured_v1_fixture_parses_current_field_names(self):
        records = comtrade._parse_dataset(self.payload["data"])

        self.assertEqual(len(records), 1)
        record = records[0]
        self.assertEqual(record.hs_code, "850760")
        self.assertEqual(
            record.title,
            "Electric accumulators; lithium-ion, including separators, whether or not rectangular (including square)",
        )
        self.assertEqual(record.year, 2024)
        self.assertEqual(record.month, 1)
        self.assertAlmostEqual(record.value_usd, 211283955.693)
        self.assertAlmostEqual(record.net_weight, 3448845.818)
        self.assertAlmostEqual(record.qty, 74174565.0)
        self.assertEqual(record.qty_unit_code, 5)
        self.assertEqual(record.qty_unit, "u")
        self.assertEqual(record.partner_country, "CHN")
        self.assertFalse(record.is_aggregate)
        self.assertEqual(record.source, "comtrade")

    def test_world_total_flag_is_derived_from_partner_not_api_provenance(self):
        row = copy.deepcopy(self.row)
        row.update(
            {
                "partnerCode": 0,
                "partnerISO": "W00",
                "partnerDesc": "World",
                "isAggregate": False,
            }
        )

        record = comtrade._parse_dataset([row])[0]

        self.assertEqual(record.partner_country, "W00")
        self.assertTrue(record.is_aggregate)

    def test_parser_preserves_zero_current_values(self):
        row = copy.deepcopy(self.row)
        row.update({"primaryValue": 0, "netWgt": 0, "qty": 0})

        record = comtrade._parse_dataset([row])[0]

        self.assertEqual(record.value_usd, 0)
        self.assertEqual(record.net_weight, 0)
        self.assertEqual(record.qty, 0)

    def test_parser_filters_every_non_hs6_leaf_shape(self):
        non_leaf_rows = []
        for cmd_code, aggregation_level, is_leaf in [
            ("TOTAL", 0, False),
            ("85", 2, False),
            ("8507", 4, False),
            ("850700", 6, False),
            ("850799", 4, True),
            ("8507600", 6, True),
        ]:
            row = copy.deepcopy(self.row)
            row.update(
                {
                    "cmdCode": cmd_code,
                    "aggrLevel": aggregation_level,
                    "isLeaf": is_leaf,
                }
            )
            non_leaf_rows.append(row)

        records = comtrade._parse_dataset([self.row, *non_leaf_rows])

        self.assertEqual([record.hs_code for record in records], ["850760"])

    def test_parser_accepts_string_leaf_metadata_without_padding_codes(self):
        valid = copy.deepcopy(self.row)
        valid.update({"cmdCode": "010121", "aggrLevel": "6", "isLeaf": "1"})
        invalid = copy.deepcopy(valid)
        invalid.update({"cmdCode": "10121"})

        records = comtrade._parse_dataset([valid, invalid])

        self.assertEqual([record.hs_code for record in records], ["010121"])


class ComtradeRequestPlanningContractTest(unittest.TestCase):
    def test_preview_batches_exact_codes_at_350_and_one_period(self):
        codes = [f"{100000 + offset:06d}" for offset in range(351)]
        periods = ["202401", "202402"]

        batches = comtrade._build_request_batches(
            periods,
            codes,
            keyed=False,
        )

        self.assertEqual(len(batches), 4)
        for period in periods:
            period_batches = [
                batch for batch in batches if batch.params["period"] == period
            ]
            self.assertEqual(len(period_batches), 2)
            requested_codes = []
            for batch in period_batches:
                self.assertNotIn(",", batch.params["period"])
                batch_codes = batch.params["cmdCode"].split(",")
                self.assertLessEqual(len(batch_codes), 350)
                self.assertTrue(
                    all(
                        len(code) == 6 and code.isdigit()
                        for code in batch_codes
                    )
                )
                self.assertNotIn("*", batch.params["cmdCode"])
                self.assertNotEqual(batch.params["cmdCode"], "AG6")
                requested_codes.extend(batch_codes)
            self.assertEqual(requested_codes, codes)

    def test_keyed_batches_use_ag6_and_high_record_limit(self):
        periods = ["202401", "202402"]
        batches = comtrade._build_request_batches(
            periods,
            ["010121", "850760"],
            keyed=True,
        )

        requested_periods = []
        for batch in batches:
            self.assertEqual(batch.params["cmdCode"], "AG6")
            self.assertEqual(int(batch.params["maxRecords"]), 100000)
            requested_periods.extend(batch.params["period"].split(","))
        self.assertEqual(requested_periods, periods)

    @patch("server.etl.comtrade._request")
    @patch("server.etl.comtrade.fetch_product_catalog")
    def test_unkeyed_fetch_range_uses_catalog_codes_and_single_period_calls(
        self,
        mock_catalog,
        mock_request,
    ):
        catalog = comtrade._parse_catalog(
            load_fixture("comtrade_h6_reference_minimal.json")
        )
        fixture = load_fixture("comtrade_v1_current.json")
        january = copy.deepcopy(fixture)
        february = copy.deepcopy(fixture)
        february["data"][0].update(
            {"period": "202402", "refPeriodId": 20240201, "refMonth": 2}
        )
        mock_catalog.return_value = catalog
        mock_request.side_effect = [january, february]

        with patch.dict(os.environ, {}, clear=True):
            records = comtrade.fetch_range("2024-01", "2024-02")

        self.assertEqual(
            [(record.year, record.month) for record in records],
            [(2024, 1), (2024, 2)],
        )
        self.assertEqual(mock_request.call_count, 2)
        for expected_period, call in zip(
            ["202401", "202402"],
            mock_request.call_args_list,
        ):
            params = call.args[0]
            self.assertEqual(params["reporterCode"], "699")
            self.assertEqual(params["flowCode"], "M")
            self.assertEqual(params["period"], expected_period)
            self.assertEqual(params["partnerCode"], "0")
            self.assertEqual(params["breakdownMode"], "classic")
            self.assertEqual(params["cmdCode"], "850760,850780")
            endpoint = comtrade._resolve_endpoint(*call.kwargs["path_parts"])
            self.assertEqual(
                endpoint,
                "https://comtradeapi.un.org/public/v1/preview/C/M/HS",
            )


class ComtradePayloadValidationContractTest(unittest.TestCase):
    def setUp(self):
        self.payload = load_fixture("comtrade_v1_current.json")

    def test_captured_payload_is_complete(self):
        data = comtrade._validate_payload(
            self.payload,
            max_records=500,
            context="captured fixture",
        )

        self.assertEqual(data, self.payload["data"])

    def test_json_error_fails_instead_of_skipping_partition(self):
        payload = {"count": 0, "data": [], "error": "Upstream partition failed"}

        with self.assertRaisesRegex(RuntimeError, "Upstream partition failed"):
            comtrade._validate_payload(
                payload,
                max_records=500,
                context="202401 batch 1",
            )

    def test_validation_error_fails(self):
        payload = {
            "count": 0,
            "data": [],
            "error": "",
            "validation": {"status": "ERROR", "message": "Invalid period"},
        }

        with self.assertRaisesRegex(RuntimeError, "Invalid period"):
            comtrade._validate_payload(
                payload,
                max_records=500,
                context="202413 batch 1",
            )

    def test_malformed_data_shape_fails(self):
        payload = {"count": 1, "data": {"cmdCode": "850760"}, "error": ""}

        with self.assertRaises(RuntimeError):
            comtrade._validate_payload(
                payload,
                max_records=500,
                context="malformed",
            )

    def test_count_mismatch_fails_as_incomplete(self):
        payload = copy.deepcopy(self.payload)
        payload["count"] = 2

        with self.assertRaises(RuntimeError):
            comtrade._validate_payload(
                payload,
                max_records=500,
                context="incomplete",
            )

    def test_response_at_record_cap_fails_as_potentially_truncated(self):
        payload = copy.deepcopy(self.payload)
        payload["data"] = [
            copy.deepcopy(self.payload["data"][0])
            for _ in range(3)
        ]
        payload["count"] = 3

        with self.assertRaisesRegex(RuntimeError, "(?i)(truncat|limit|cap)"):
            comtrade._validate_payload(
                payload,
                max_records=3,
                context="capped",
            )


class ComtradePartnerSnapshotContractTest(unittest.TestCase):
    @patch("server.etl.comtrade._request_json_url")
    def test_public_detail_snapshot_never_consumes_subscription_key(
        self,
        mock_request,
    ):
        base = load_fixture("comtrade_v1_current.json")["data"][0]
        world = copy.deepcopy(base)
        world.update(
            {
                "partnerCode": 0,
                "partnerISO": "W00",
                "partnerDesc": "World",
                "primaryValue": 100,
            }
        )
        china = copy.deepcopy(base)
        china.update({"primaryValue": 80})
        korea = copy.deepcopy(base)
        korea.update(
            {
                "partnerCode": 410,
                "partnerISO": "KOR",
                "partnerDesc": "Rep. of Korea",
                "primaryValue": 20,
            }
        )
        mock_request.return_value = {
            "count": 3,
            "data": [world, china, korea],
            "error": "",
        }
        comtrade.fetch_partner_snapshot.cache_clear()

        with patch.dict(
            os.environ,
            {"COMTRADE_KEY": "must-not-be-used"},
            clear=True,
        ):
            snapshot = comtrade.fetch_partner_snapshot("850760", "202401")

        url = mock_request.call_args.args[0]
        self.assertIn("/public/v1/preview/C/M/HS?", url)
        self.assertNotIn("subscription-key", url)
        self.assertEqual(mock_request.call_args.kwargs["timeout_seconds"], 8)
        self.assertEqual(mock_request.call_args.kwargs["max_retries"], 1)
        self.assertEqual(len(snapshot["partners"]), 2)
        self.assertAlmostEqual(snapshot["hhi"], 0.68)


class ComtradeRunContractTest(unittest.TestCase):
    @patch("server.etl.comtrade.db.delete_comtrade_aggregate_periods")
    @patch("server.etl.comtrade.load", return_value=(100, 100))
    @patch("server.etl.comtrade.fetch_range")
    @patch("server.etl.comtrade.sync_product_catalog", return_value=2)
    def test_run_syncs_full_catalog_before_loading_trade_rows(
        self,
        mock_sync,
        mock_fetch,
        mock_load,
        mock_delete,
    ):
        conn = MagicMock()
        record = comtrade._parse_dataset(
            load_fixture("comtrade_v1_current.json")["data"]
        )[0]
        mock_fetch.return_value = [record]
        catalog = [
            comtrade.CatalogProduct(
                hs_code=record.hs_code,
                title=record.title,
                description=record.description,
                sectors=record.sectors,
            )
        ]

        with patch.dict(
            os.environ,
            {"COMTRADE_MIN_PRODUCTS_PER_PERIOD": "1"},
            clear=True,
        ):
            comtrade.run(
                conn,
                from_period="2024-01",
                to_period="2024-01",
                catalog_products=catalog,
                cap_to_available=False,
            )

        mock_sync.assert_called_once_with(conn, catalog)
        mock_fetch.assert_called_once_with(
            "2024-01",
            "2024-01",
            catalog_products=catalog,
        )
        mock_load.assert_called_once_with(conn, [record])
        mock_delete.assert_called_once_with(conn, ["202401"])

    @patch("server.etl.comtrade.fetch_range")
    @patch("server.etl.comtrade.sync_product_catalog", return_value=2)
    def test_run_rejects_implausibly_partial_month(self, mock_sync, mock_fetch):
        record = comtrade._parse_dataset(
            load_fixture("comtrade_v1_current.json")["data"]
        )[0]
        mock_fetch.return_value = [record]
        catalog = [
            comtrade.CatalogProduct(
                hs_code=record.hs_code,
                title=record.title,
                description=record.description,
                sectors=record.sectors,
            )
        ]

        with patch.dict(
            os.environ,
            {"COMTRADE_MIN_PRODUCTS_PER_PERIOD": "2"},
            clear=True,
        ):
            with self.assertRaisesRegex(RuntimeError, "completeness floor"):
                comtrade.run(
                    MagicMock(),
                    from_period="2024-01",
                    to_period="2024-01",
                    catalog_products=catalog,
                    cap_to_available=False,
                )


class ComtradeLoadTest(unittest.TestCase):
    @patch(
        "server.etl.comtrade.db.bulk_insert_monthly",
        return_value=1,
    )
    @patch(
        "server.etl.comtrade.db.bulk_upsert_products",
        return_value=1,
    )
    def test_load_handles_missing_fx(self, mock_upsert, mock_insert):
        record = comtrade._parse_dataset(
            load_fixture("comtrade_v1_current.json")["data"]
        )[0]

        with patch(
            "server.etl.comtrade.forex.monthly_rate",
            side_effect=RuntimeError("missing"),
        ):
            products, rows = comtrade.load(MagicMock(), [record])

        self.assertEqual(products, 1)
        self.assertEqual(rows, 1)
        mock_upsert.assert_called_once()
        mock_insert.assert_called_once()
        monthly_row = mock_insert.call_args.args[1][0]
        self.assertEqual(monthly_row[0], "850760")
        self.assertAlmostEqual(monthly_row[3], 211283955.693)
        self.assertIsNone(monthly_row[4])
        self.assertIsNone(monthly_row[5])
        self.assertEqual(monthly_row[10], "CHN")


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
