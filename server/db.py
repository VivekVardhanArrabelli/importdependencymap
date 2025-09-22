"""Database helpers for the Build for India service."""
from __future__ import annotations

import logging
import os
from contextlib import contextmanager
from typing import Dict, List, Optional, Sequence, Tuple

import psycopg2
from psycopg2.extras import RealDictCursor

LOGGER = logging.getLogger(__name__)


class DatabaseError(RuntimeError):
    """Raised when the database configuration is missing."""


def _database_url() -> str:
    url = os.getenv("DATABASE_URL")
    if not url:
        raise DatabaseError("DATABASE_URL environment variable is required")
    return url


@contextmanager
def connect():
    """Yield a psycopg2 connection configured for manual transactions."""

    conn = psycopg2.connect(_database_url())
    try:
        yield conn
        conn.commit()
    except Exception:  # pragma: no cover - defensive rollback
        conn.rollback()
        raise
    finally:
        conn.close()


def init_db(conn) -> None:
    """Create tables if they do not already exist."""

    statements = [
        """
        CREATE TABLE IF NOT EXISTS products (
            hs_code TEXT PRIMARY KEY,
            title TEXT NOT NULL,
            description TEXT,
            sectors TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[],
            granularity INT DEFAULT 6,
            capex_min NUMERIC,
            capex_max NUMERIC
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS monthly_imports (
            id SERIAL PRIMARY KEY,
            hs_code TEXT REFERENCES products(hs_code),
            year INT NOT NULL,
            month INT NOT NULL,
            value_usd NUMERIC,
            qty NUMERIC,
            partner_country TEXT,
            UNIQUE (hs_code, year, month, partner_country)
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS baseline_imports (
            hs_code TEXT PRIMARY KEY REFERENCES products(hs_code),
            baseline_12m_usd NUMERIC,
            baseline_period TEXT,
            updated_at timestamptz DEFAULT now()
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS import_progress (
            hs_code TEXT PRIMARY KEY REFERENCES products(hs_code),
            baseline_12m_usd NUMERIC,
            current_12m_usd NUMERIC,
            reduction_abs NUMERIC,
            reduction_pct NUMERIC,
            hhi_baseline NUMERIC,
            hhi_current NUMERIC,
            concentration_shift NUMERIC,
            opportunity_score NUMERIC,
            last_updated timestamptz DEFAULT now()
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS domestic_capability (
            id SERIAL PRIMARY KEY,
            hs_code TEXT REFERENCES products(hs_code),
            capex_min NUMERIC,
            capex_max NUMERIC,
            machines JSONB,
            skills JSONB,
            notes TEXT,
            source TEXT,
            verified BOOLEAN DEFAULT false,
            UNIQUE (hs_code)
        )
        """,
    ]
    with conn.cursor() as cur:
        for statement in statements:
            cur.execute(statement)
    LOGGER.info("Database schema ensured")


def upsert_product(conn, *, hs_code: str, title: str, description: str, sectors: Sequence[str],
                   capex_min, capex_max) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO products (hs_code, title, description, sectors, capex_min, capex_max)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (hs_code) DO UPDATE
            SET title = EXCLUDED.title,
                description = EXCLUDED.description,
                sectors = EXCLUDED.sectors,
                capex_min = EXCLUDED.capex_min,
                capex_max = EXCLUDED.capex_max
            """,
            (hs_code, title, description, list(sectors), capex_min, capex_max),
        )


def insert_monthly(conn, *, hs_code: str, year: int, month: int, value_usd, qty, partner: str) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO monthly_imports (hs_code, year, month, value_usd, qty, partner_country)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (hs_code, year, month, partner_country) DO UPDATE
            SET value_usd = EXCLUDED.value_usd,
                qty = EXCLUDED.qty
            """,
            (hs_code, year, month, value_usd, qty, partner),
        )


def fetch_last_36m(conn, hs_code: str) -> List[Dict]:
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            SELECT year, month, value_usd, qty, partner_country
            FROM monthly_imports
            WHERE hs_code = %s
            ORDER BY year DESC, month DESC
            LIMIT 36
            """,
            (hs_code,),
        )
        rows = cur.fetchall()
    return list(reversed(rows))


def aggregate_last_12m(conn, hs_code: Optional[str] = None) -> List[Dict]:
    query = """
        SELECT hs_code, SUM(value_usd) AS total
        FROM monthly_imports
        WHERE make_date(year, month, 1) >= (
            SELECT make_date(max(year), max(month), 1) - INTERVAL '11 month'
            FROM monthly_imports
        )
    """
    params: Tuple = tuple()
    if hs_code:
        query += " AND hs_code = %s"
        params = (hs_code,)
    query += " GROUP BY hs_code"
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(query, params)
        return cur.fetchall()


def partner_shares(conn, hs_code: str, *, period: str, start: Optional[Tuple[int, int]] = None,
                   end: Optional[Tuple[int, int]] = None) -> Dict[str, float]:
    """Return partner shares for the requested period."""

    if period == "current":
        if start and end:
            query = """
                SELECT partner_country, SUM(value_usd) AS total
                FROM monthly_imports
                WHERE hs_code = %s
                  AND make_date(year, month, 1) BETWEEN make_date(%s, %s, 1) AND make_date(%s, %s, 1)
                GROUP BY partner_country
            """
            params = (hs_code, start[0], start[1], end[0], end[1])
        else:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT max(year), max(month) FROM monthly_imports WHERE hs_code = %s
                    """,
                    (hs_code,),
                )
                latest = cur.fetchone()
            if not latest or latest[0] is None:
                return {}
            last_year, last_month = latest
            query = """
                SELECT partner_country, SUM(value_usd) AS total
                FROM monthly_imports
                WHERE hs_code = %s
                  AND make_date(year, month, 1) BETWEEN
                      make_date(%s, %s, 1) - INTERVAL '11 month' AND make_date(%s, %s, 1)
                GROUP BY partner_country
            """
            params = (hs_code, last_year, last_month, last_year, last_month)
    else:
        if not (start and end):
            return {}
        query = """
            SELECT partner_country, SUM(value_usd) AS total
            FROM monthly_imports
            WHERE hs_code = %s
              AND make_date(year, month, 1) BETWEEN make_date(%s, %s, 1) AND make_date(%s, %s, 1)
            GROUP BY partner_country
        """
        params = (hs_code, start[0], start[1], end[0], end[1])
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(query, params)
        rows = cur.fetchall()
    total = sum(float(row["total"] or 0) for row in rows)
    if total == 0:
        return {}
    return {row["partner_country"]: float(row["total"]) / total for row in rows}
