import os
import unittest

from server import main


class HealthTest(unittest.TestCase):
    def test_health_endpoint(self):
        os.environ.pop("DATABASE_URL", None)
        self.assertEqual(main.health(), {"ok": True})


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
