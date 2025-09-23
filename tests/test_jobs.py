import unittest

from server import jobs


class WindowingTest(unittest.TestCase):
    def test_window_of_12_fills_gaps_and_prefers_latest(self):
        monthly = [
            jobs.MonthlyTotal(2022, 12, 5.0),
            jobs.MonthlyTotal(2023, 1, 10.0),
            jobs.MonthlyTotal(2023, 3, 30.0),
            jobs.MonthlyTotal(2023, 6, 60.0),
            jobs.MonthlyTotal(2023, 12, 120.0),
            jobs.MonthlyTotal(2024, 2, 200.0),
        ]

        baseline_window = jobs._window_of_12(monthly)
        self.assertIsNotNone(baseline_window)
        self.assertEqual((baseline_window[0].year, baseline_window[0].month), (2022, 12))
        self.assertEqual((baseline_window[-1].year, baseline_window[-1].month), (2023, 11))

        latest_window = jobs._window_of_12(monthly, latest=True)
        self.assertIsNotNone(latest_window)
        self.assertEqual(len(latest_window), 12)
        self.assertEqual((latest_window[0].year, latest_window[0].month), (2023, 3))
        self.assertEqual((latest_window[-1].year, latest_window[-1].month), (2024, 2))

        april_entry = next(row for row in latest_window if (row.year, row.month) == (2023, 4))
        self.assertEqual(april_entry.total, 0.0)


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
