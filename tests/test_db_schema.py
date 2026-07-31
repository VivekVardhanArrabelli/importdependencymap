"""Unit-level regression checks for idempotent database migrations."""
from __future__ import annotations

import unittest

from server import db


class _RecordingCursor:
    def __init__(self, statements):
        self.statements = statements

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, traceback):
        return False

    def execute(self, statement, parameters=None):
        self.statements.append(str(statement))


class _RecordingConnection:
    def __init__(self):
        self.statements = []

    def cursor(self, *args, **kwargs):
        return _RecordingCursor(self.statements)


class DatabaseSchemaMigrationTest(unittest.TestCase):
    def test_domestic_capability_is_deduplicated_before_unique_index(self):
        conn = _RecordingConnection()

        db.init_db(conn)

        normalized = [" ".join(statement.split()) for statement in conn.statements]
        dedupe_index = next(
            index
            for index, statement in enumerate(normalized)
            if statement.startswith("DELETE FROM domestic_capability older")
        )
        unique_index = next(
            index
            for index, statement in enumerate(normalized)
            if "CREATE UNIQUE INDEX IF NOT EXISTS uq_domestic_capability_hs_code"
            in statement
        )
        self.assertLess(dedupe_index, unique_index)
        self.assertIn(
            "ON domestic_capability (hs_code)", normalized[unique_index]
        )


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
