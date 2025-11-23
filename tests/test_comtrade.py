import os
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

from server.etl import comtrade


class ComtradeETLTest(unittest.TestCase):
    def setUp(self):
        os.environ["FX_RATES_FILE"] = str(Path("data/fx_rates.csv").resolve())
        self._orig_chapters = comtrade.HS_CHAPTERS
        comtrade.HS_CHAPTERS = (1,)

    def tearDown(self):
        if "FX_RATES_FILE" in os.environ:
            del os.environ["FX_RATES_FILE"]
        comtrade.HS_CHAPTERS = self._orig_chapters

    @patch("server.etl.comtrade._request")
    def test_fetch_range_uses_defaults(self, mock_request):
        mock_request.return_value = {"dataset": []}
        os.environ["COMTRADE_REPORTER"] = "India"
        os.environ["COMTRADE_FLOW"] = "import"


        comtrade.fetch_range("2024-01", "2024-02")


        called_params = mock_request.call_args.args[0]
        path_parts = mock_request.call_args.kwargs["path_parts"]

        self.assertEqual(called_params["reporterCode"], "356")
        self.assertEqual(called_params["flowCode"], "M")
        self.assertEqual(called_params["period"], "202401,202402")
        self.assertEqual(called_params["partnerCode"], "0")
        self.assertEqual(called_params["includeDesc"], "false")
        self.assertEqual(path_parts, ["get", "C", "M", "HS"])


        os.environ.pop("COMTRADE_REPORTER", None)
        os.environ.pop("COMTRADE_FLOW", None)


    @patch("server.etl.comtrade._request")
    def test_fetch_range_handles_pagination_and_validation(self, mock_request):
        first_payload = {
            "data": [{
                "cmdCode": "850760",
                "period": "202401",
                "cmdDescE": "Lithium batteries",
                "TradeValue": "1000",
            }],
            "links": {"next": "https://example.com/api?cursor=abc"},
        }
        second_payload = {
            "dataset": [{
                "cmdCode": "850780",
                "period": "202402",
                "cmdDescE": "Nickel batteries",
                "TradeValue": "2000",
            }],
        }
        mock_request.side_effect = [first_payload, second_payload]

        records = comtrade.fetch_range("2024-01", "2024-02")

        self.assertEqual(len(records), 2)
        self.assertEqual(mock_request.call_count, 2)
        first_call_params = mock_request.call_args_list[0].args[0]
        second_call_params = mock_request.call_args_list[1].args[0]
        self.assertNotIn("cursor", first_call_params)
        self.assertEqual(second_call_params.get("cursor"), "abc")

    @patch("server.etl.comtrade._request")
    def test_fetch_range_raises_on_validation_error(self, mock_request):
        mock_request.return_value = {"validation": {"status": "ERROR", "message": "Invalid"}}

        with self.assertRaises(RuntimeError):
            comtrade.fetch_range("2024-01", "2024-01")


    @patch("server.etl.comtrade.db.insert_monthly")
    @patch("server.etl.comtrade.db.upsert_product")
    def test_load_handles_missing_fx(self, mock_upsert, mock_insert):
        record = comtrade.Record(
            hs_code="850760",
            title="Lithium-ion batteries",
            description="",
            sectors=["energy"],
            capex_min=None,
            capex_max=None,
            year=2024,
            month=5,
            value_usd=1250000.0,
            value_inr=None,
            qty=1000.0,
            partner_country="China",
        )

        with patch("server.etl.comtrade.forex.monthly_rate", side_effect=RuntimeError("missing")):
            products, rows = comtrade.load(MagicMock(), [record])

        self.assertEqual(products, 1)
        self.assertEqual(rows, 1)
        mock_upsert.assert_called_once()
        mock_insert.assert_called_once()
        kwargs = mock_insert.call_args.kwargs
        self.assertEqual(kwargs["hs_code"], "850760")
        self.assertIsNone(kwargs["fx_rate"])
        self.assertAlmostEqual(kwargs["value_usd"], 1250000.0)
        self.assertIsNone(kwargs["value_inr"])


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
