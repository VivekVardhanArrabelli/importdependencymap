import os
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

from server.etl import comtrade


class ComtradeETLTest(unittest.TestCase):
    def setUp(self):
        os.environ["FX_RATES_FILE"] = str(Path("data/fx_rates.csv").resolve())

    def tearDown(self):
        if "FX_RATES_FILE" in os.environ:
            del os.environ["FX_RATES_FILE"]

    @patch("server.etl.comtrade._request")
    def test_fetch_range_uses_defaults(self, mock_request):
        mock_request.return_value = {"dataset": []}
        os.environ["COMTRADE_REPORTER"] = "India"
        os.environ["COMTRADE_FLOW"] = "import"

        comtrade.fetch_range("202401", "202402")

        called_params = mock_request.call_args[0][0]
        self.assertEqual(called_params["reporter"], "India")
        self.assertEqual(called_params["flow"], "import")
        self.assertEqual(called_params["time_period"], "202401:202402")

        os.environ.pop("COMTRADE_REPORTER", None)
        os.environ.pop("COMTRADE_FLOW", None)

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
