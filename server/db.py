"""Database helpers for the Build for India service."""
from __future__ import annotations

import logging
import os
from contextlib import contextmanager
from datetime import datetime
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

import psycopg2
from psycopg2 import sql
from psycopg2.extras import Json, RealDictCursor, execute_values

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
    """Create tables, indexes, and schemas if they do not already exist."""

    statements = [
        """
        CREATE TABLE IF NOT EXISTS products (
            hs_code TEXT PRIMARY KEY,
            title TEXT,
            description TEXT,
            sectors TEXT[] DEFAULT ARRAY[]::TEXT[],
            granularity INT DEFAULT 6,
            capex_min NUMERIC,
            capex_max NUMERIC,
            created_at timestamptz DEFAULT now(),
            updated_at timestamptz DEFAULT now()
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS monthly_imports (
            id SERIAL PRIMARY KEY,
            hs_code TEXT REFERENCES products(hs_code),
            year INT NOT NULL,
            month INT NOT NULL,
            value_usd NUMERIC,
            value_inr NUMERIC,
            fx_rate NUMERIC,
            qty NUMERIC,
            partner_country TEXT,
            created_at timestamptz DEFAULT now()
        )
        """,
        """
        CREATE INDEX IF NOT EXISTS ix_monthly_imports_hs_ym
          ON monthly_imports (hs_code, year, month)
        """,
        """
        CREATE UNIQUE INDEX IF NOT EXISTS uq_monthly_imports_hs_ym_partner
          ON monthly_imports (hs_code, year, month, partner_country)
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
            created_at timestamptz DEFAULT now()
        )
        """,
    ]
    with conn.cursor() as cur:
        for statement in statements:
            cur.execute(statement)
        cur.execute(
            "ALTER TABLE monthly_imports ADD COLUMN IF NOT EXISTS value_inr NUMERIC"
        )
        cur.execute(
            "ALTER TABLE monthly_imports ADD COLUMN IF NOT EXISTS fx_rate NUMERIC"
        )
    LOGGER.info("Database schema ensured")


def upsert_product(
    conn,
    *,
    hs_code: str,
    title: str,
    description: str,
    sectors: Sequence[str],
    capex_min,
    capex_max,
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO products (hs_code, title, description, sectors, capex_min, capex_max, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s, now())
            ON CONFLICT (hs_code) DO UPDATE
            SET title = EXCLUDED.title,
                description = EXCLUDED.description,
                sectors = EXCLUDED.sectors,
                capex_min = COALESCE(EXCLUDED.capex_min, products.capex_min),
                capex_max = COALESCE(EXCLUDED.capex_max, products.capex_max),
                updated_at = now()
            """,
            (hs_code, title, description, list(sectors or []), capex_min, capex_max),
        )


def insert_monthly(
    conn,
    *,
    hs_code: str,
    year: int,
    month: int,
    value_usd,
    value_inr,
    fx_rate,
    qty,
    partner: Optional[str],
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO monthly_imports (
                hs_code, year, month, value_usd, value_inr, fx_rate, qty, partner_country
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (hs_code, year, month, partner_country) DO UPDATE
                SET value_usd = EXCLUDED.value_usd,
                    value_inr = EXCLUDED.value_inr,
                    fx_rate = EXCLUDED.fx_rate,
                    qty = EXCLUDED.qty
            """,
            (hs_code, year, month, value_usd, value_inr, fx_rate, qty, partner),
        )


def bulk_insert_monthly(
    conn,
    rows: Iterable[Tuple[str, int, int, float, Optional[float], Optional[float], Optional[str]]],
) -> int:
    """Insert many monthly rows with upsert semantics."""

    rows = list(rows)
    if not rows:
        return 0
    with conn.cursor() as cur:
        execute_values(
            cur,
            """
            INSERT INTO monthly_imports (hs_code, year, month, value_usd, value_inr, fx_rate, qty, partner_country)
            VALUES %s
            ON CONFLICT (hs_code, year, month, partner_country) DO UPDATE
              SET value_usd = EXCLUDED.value_usd,
                  value_inr = EXCLUDED.value_inr,
                  fx_rate = EXCLUDED.fx_rate,
                  qty = EXCLUDED.qty
            """,
            rows,
        )
    return len(rows)


def upsert_baseline(
    conn,
    *,
    hs_code: str,
    baseline_value,
    baseline_period: Optional[str],
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO baseline_imports (hs_code, baseline_12m_usd, baseline_period, updated_at)
            VALUES (%s, %s, %s, now())
            ON CONFLICT (hs_code) DO UPDATE
              SET baseline_12m_usd = EXCLUDED.baseline_12m_usd,
                  baseline_period = EXCLUDED.baseline_period,
                  updated_at = now()
            """,
            (hs_code, baseline_value, baseline_period),
        )


def upsert_progress(
    conn,
    *,
    hs_code: str,
    baseline_value,
    current_value,
    reduction_abs,
    reduction_pct,
    hhi_baseline,
    hhi_current,
    concentration_shift,
    opportunity_score,
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO import_progress (
                hs_code, baseline_12m_usd, current_12m_usd, reduction_abs, reduction_pct,
                hhi_baseline, hhi_current, concentration_shift, opportunity_score, last_updated
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, now())
            ON CONFLICT (hs_code) DO UPDATE
              SET baseline_12m_usd = EXCLUDED.baseline_12m_usd,
                  current_12m_usd = EXCLUDED.current_12m_usd,
                  reduction_abs = EXCLUDED.reduction_abs,
                  reduction_pct = EXCLUDED.reduction_pct,
                  hhi_baseline = EXCLUDED.hhi_baseline,
                  hhi_current = EXCLUDED.hhi_current,
                  concentration_shift = EXCLUDED.concentration_shift,
                  opportunity_score = EXCLUDED.opportunity_score,
                  last_updated = now()
            """,
            (
                hs_code,
                baseline_value,
                current_value,
                reduction_abs,
                reduction_pct,
                hhi_baseline,
                hhi_current,
                concentration_shift,
                opportunity_score,
            ),
        )


def fetch_last_36m(conn, hs_code: str) -> List[Dict]:
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            SELECT year, month, value_usd, value_inr, fx_rate, qty, partner_country
            FROM monthly_imports
            WHERE hs_code = %s
            ORDER BY year DESC, month DESC
            LIMIT 36
            """,
            (hs_code,),
        )
        rows = cur.fetchall()
    return list(reversed(rows))


def partner_shares(
    conn,
    hs_code: str,
    *,
    start: Tuple[int, int],
    end: Tuple[int, int],
) -> Dict[str, float]:
    """Return partner share fractions for a period inclusive."""

    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            SELECT partner_country, SUM(value_usd) AS total
            FROM monthly_imports
            WHERE hs_code = %s
              AND (
                    (year > %s OR (year = %s AND month >= %s))
                AND (year < %s OR (year = %s AND month <= %s))
              )
            GROUP BY partner_country
            HAVING SUM(value_usd) IS NOT NULL
            """,
            (hs_code, start[0], start[0], start[1], end[0], end[0], end[1]),
        )
        rows = cur.fetchall()
    total = sum(float(row["total"]) for row in rows if row["total"] is not None)
    if total <= 0:
        return {}
    return {row["partner_country"]: float(row["total"]) / total for row in rows if row["total"] is not None}


def fetch_monthly_series(
    conn,
    hs_code: str,
    *,
    start: Optional[Tuple[int, int]] = None,
    end: Optional[Tuple[int, int]] = None,
) -> List[Dict]:
    query = [
        "SELECT year, month, SUM(value_usd) AS total"
        " FROM monthly_imports",
        " WHERE hs_code = %s",
    ]
    params: List = [hs_code]
    if start and end:
        query.append("   AND make_date(year, month, 1) BETWEEN make_date(%s, %s, 1) AND make_date(%s, %s, 1)")
        params.extend([start[0], start[1], end[0], end[1]])
    query.append(" GROUP BY year, month ORDER BY year, month")
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("\n".join(query), params)
        return cur.fetchall()


def latest_year_month(conn) -> Optional[Tuple[int, int]]:
    with conn.cursor() as cur:
        cur.execute("SELECT max(year), max(month) FROM monthly_imports")
        result = cur.fetchone()
    if not result or result[0] is None or result[1] is None:
        return None
    return result[0], result[1]


def upsert_domestic_capability(
    conn,
    *,
    hs_code: str,
    capex_min,
    capex_max,
    machines,
    skills,
    notes,
    source,
) -> int:
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            INSERT INTO domestic_capability (hs_code, capex_min, capex_max, machines, skills, notes, source, verified, created_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, false, now())
            ON CONFLICT (hs_code) DO UPDATE
              SET capex_min = EXCLUDED.capex_min,
                  capex_max = EXCLUDED.capex_max,
                  machines = EXCLUDED.machines,
                  skills = EXCLUDED.skills,
                  notes = EXCLUDED.notes,
                  source = EXCLUDED.source,
                  verified = false
            RETURNING id
            """,
            (
                hs_code,
                capex_min,
                capex_max,
                Json(machines) if machines is not None else None,
                Json(skills) if skills is not None else None,
                notes,
                source,
            ),
        )
        row = cur.fetchone()
    return int(row["id"]) if row and row.get("id") is not None else 0


def fetch_verified_capability(conn, hs_code: str) -> List[Dict]:
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            SELECT id, hs_code, capex_min, capex_max, machines, skills, notes, source, verified
            FROM domestic_capability
            WHERE hs_code = %s AND verified = true
            ORDER BY created_at DESC
            """,
            (hs_code,),
        )
        return cur.fetchall()


def ensure_extensions(conn) -> None:
    """Optional hook to ensure extensions (placeholder for future)."""

    try:
        with conn.cursor() as cur:
            cur.execute(sql.SQL("SELECT 1"))
    except Exception:  # pragma: no cover - ext creation is optional
        LOGGER.debug("Extension check failed", exc_info=True)
