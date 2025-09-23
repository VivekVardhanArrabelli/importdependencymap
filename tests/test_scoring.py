import unittest

from server import util


class ScoringMathTest(unittest.TestCase):
    def test_norm_log_monotonic(self):
        values = {"low": 10, "mid": 1000, "high": 100000}
        norm = util.norm_log(values)
        self.assertTrue(0.0 <= norm["low"] < norm["mid"] < norm["high"] <= 1.0)

    def test_hhi_from_shares(self):
        shares = [0.5, 0.3, 0.2]
        hhi = util.hhi_from_shares(shares)
        self.assertTrue(0 < hhi < 1)
        self.assertIsNone(util.hhi_from_shares([]))

    def test_tech_feasibility_prefers_highest_sector(self):
        score = util.tech_feasibility_for(["industrial", "electronics"])
        self.assertEqual(score, util.SECTOR_TECH_FEASIBILITY["electronics"])
        self.assertEqual(util.tech_feasibility_for(None), 0.6)


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
