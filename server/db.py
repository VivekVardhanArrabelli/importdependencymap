"""Database helpers for the Build for India service."""
from __future__ import annotations

import logging
import os
from contextlib import contextmanager
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

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
            catalog_active BOOLEAN NOT NULL DEFAULT true,
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
            net_weight NUMERIC,
            qty_unit_code INT,
            qty_unit TEXT,
            partner_country TEXT NOT NULL DEFAULT 'UNKNOWN',
            source TEXT NOT NULL DEFAULT 'legacy',
            is_aggregate BOOLEAN NOT NULL DEFAULT false,
            created_at timestamptz DEFAULT now()
        )
        """,
        """
        CREATE INDEX IF NOT EXISTS ix_monthly_imports_hs_ym
          ON monthly_imports (hs_code, year, month)
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
        cur.execute(
            "ALTER TABLE monthly_imports ADD COLUMN IF NOT EXISTS net_weight NUMERIC"
        )
        cur.execute(
            "ALTER TABLE monthly_imports ADD COLUMN IF NOT EXISTS qty_unit_code INT"
        )
        cur.execute(
            "ALTER TABLE monthly_imports ADD COLUMN IF NOT EXISTS qty_unit TEXT"
        )
        cur.execute(
            "ALTER TABLE monthly_imports ADD COLUMN IF NOT EXISTS source TEXT DEFAULT 'legacy'"
        )
        cur.execute(
            "ALTER TABLE monthly_imports ADD COLUMN IF NOT EXISTS is_aggregate BOOLEAN DEFAULT false"
        )
        cur.execute(
            "ALTER TABLE monthly_imports ADD COLUMN IF NOT EXISTS created_at timestamptz DEFAULT now()"
        )
        cur.execute("DROP INDEX IF EXISTS uq_monthly_imports_hs_ym_partner")
        cur.execute(
            """
            DELETE FROM monthly_imports older
            USING monthly_imports newer
            WHERE older.id < newer.id
              AND older.hs_code = newer.hs_code
              AND older.year = newer.year
              AND older.month = newer.month
              AND COALESCE(older.partner_country, 'UNKNOWN')
                    = COALESCE(newer.partner_country, 'UNKNOWN')
              AND COALESCE(older.source, 'legacy')
                    = COALESCE(newer.source, 'legacy')
            """
        )
        cur.execute("UPDATE monthly_imports SET partner_country = 'UNKNOWN' WHERE partner_country IS NULL")
        cur.execute("UPDATE monthly_imports SET source = 'legacy' WHERE source IS NULL")
        cur.execute("UPDATE monthly_imports SET is_aggregate = false WHERE is_aggregate IS NULL")
        cur.execute("ALTER TABLE monthly_imports ALTER COLUMN partner_country SET DEFAULT 'UNKNOWN'")
        cur.execute("ALTER TABLE monthly_imports ALTER COLUMN partner_country SET NOT NULL")
        cur.execute("ALTER TABLE monthly_imports ALTER COLUMN source SET DEFAULT 'legacy'")
        cur.execute("ALTER TABLE monthly_imports ALTER COLUMN source SET NOT NULL")
        cur.execute("ALTER TABLE monthly_imports ALTER COLUMN is_aggregate SET DEFAULT false")
        cur.execute("ALTER TABLE monthly_imports ALTER COLUMN is_aggregate SET NOT NULL")
        cur.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS uq_monthly_imports_hs_ym_partner_source
              ON monthly_imports (hs_code, year, month, partner_country, source)
            """
        )
        # Ensure timestamp columns on products (for idempotency across schema evolutions)
        cur.execute("ALTER TABLE products ADD COLUMN IF NOT EXISTS created_at timestamptz DEFAULT now()")
        cur.execute("ALTER TABLE products ADD COLUMN IF NOT EXISTS updated_at timestamptz DEFAULT now()")
        cur.execute(
            "ALTER TABLE products ADD COLUMN IF NOT EXISTS catalog_active BOOLEAN DEFAULT true"
        )
        cur.execute("UPDATE products SET catalog_active = true WHERE catalog_active IS NULL")
        cur.execute("ALTER TABLE products ALTER COLUMN catalog_active SET DEFAULT true")
        cur.execute("ALTER TABLE products ALTER COLUMN catalog_active SET NOT NULL")
        # Similarly for other tables if needed (e.g., baseline_imports already has updated_at in CREATE)
        cur.execute("ALTER TABLE baseline_imports ADD COLUMN IF NOT EXISTS updated_at timestamptz DEFAULT now()")
        cur.execute("ALTER TABLE import_progress ADD COLUMN IF NOT EXISTS last_updated timestamptz DEFAULT now()")
        cur.execute("ALTER TABLE domestic_capability ADD COLUMN IF NOT EXISTS created_at timestamptz DEFAULT now()")
        # Older deployments created this table without the uniqueness required
        # by upsert_domestic_capability's ON CONFLICT target. Keep the newest
        # record deterministically before adding the idempotent index.
        cur.execute(
            """
            DELETE FROM domestic_capability older
            USING domestic_capability newer
            WHERE older.hs_code IS NOT NULL
              AND older.hs_code = newer.hs_code
              AND older.id < newer.id
            """
        )
        cur.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS uq_domestic_capability_hs_code
              ON domestic_capability (hs_code)
            """
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


def bulk_upsert_products(
    conn,
    rows: Iterable[
        Tuple[str, str, str, Sequence[str], Optional[float], Optional[float]]
    ],
) -> int:
    """Upsert product metadata efficiently while preserving richer existing values."""

    values = [
        (
            hs_code,
            title,
            description,
            list(sectors or []),
            capex_min,
            capex_max,
        )
        for hs_code, title, description, sectors, capex_min, capex_max in rows
    ]
    if not values:
        return 0
    with conn.cursor() as cur:
        execute_values(
            cur,
            """
            INSERT INTO products (
                hs_code, title, description, sectors, capex_min, capex_max, updated_at
            )
            VALUES %s
            ON CONFLICT (hs_code) DO UPDATE
            SET title = CASE
                    WHEN EXCLUDED.title IS NULL
                      OR btrim(EXCLUDED.title) = ''
                      OR EXCLUDED.title LIKE 'HS %%'
                    THEN products.title
                    ELSE EXCLUDED.title
                END,
                description = CASE
                    WHEN EXCLUDED.description IS NULL OR btrim(EXCLUDED.description) = ''
                    THEN products.description
                    ELSE EXCLUDED.description
                END,
                sectors = CASE
                    WHEN cardinality(EXCLUDED.sectors) = 0 THEN products.sectors
                    ELSE EXCLUDED.sectors
                END,
                capex_min = COALESCE(EXCLUDED.capex_min, products.capex_min),
                capex_max = COALESCE(EXCLUDED.capex_max, products.capex_max),
                updated_at = now()
            """,
            [
                (
                    hs_code,
                    title,
                    description,
                    sectors,
                    capex_min,
                    capex_max,
                    datetime.now(),
                )
                for hs_code, title, description, sectors, capex_min, capex_max in values
            ],
            page_size=1_000,
        )
    return len(values)


def replace_product_catalog_membership(conn, hs_codes: Sequence[str]) -> int:
    """Mark only the supplied HS6 codes as members of the current catalogue."""

    codes = sorted({str(code).strip() for code in hs_codes if str(code).strip()})
    if not codes:
        raise ValueError("Current product catalogue cannot be empty")
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE products
            SET catalog_active = (hs_code = ANY(%s))
            WHERE granularity = 6
              AND hs_code ~ '^[0-9]{6}$'
            """,
            (codes,),
        )
        return int(cur.rowcount)


def fetch_product_catalog(conn) -> List[Dict[str, Any]]:
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            SELECT hs_code, title, description, sectors
            FROM products
            WHERE granularity = 6
              AND catalog_active = true
              AND hs_code ~ '^[0-9]{6}$'
            ORDER BY hs_code
            """
        )
        return [dict(row) for row in cur.fetchall()]


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
    net_weight=None,
    qty_unit_code: Optional[int] = None,
    qty_unit: Optional[str] = None,
    source: str = "legacy",
    is_aggregate: bool = False,
) -> None:
    partner_key = partner or "UNKNOWN"
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO monthly_imports (
                hs_code, year, month, value_usd, value_inr, fx_rate, qty,
                net_weight, qty_unit_code, qty_unit, partner_country, source, is_aggregate
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (hs_code, year, month, partner_country, source) DO UPDATE
                SET value_usd = EXCLUDED.value_usd,
                    value_inr = EXCLUDED.value_inr,
                    fx_rate = EXCLUDED.fx_rate,
                    qty = EXCLUDED.qty,
                    net_weight = EXCLUDED.net_weight,
                    qty_unit_code = EXCLUDED.qty_unit_code,
                    qty_unit = EXCLUDED.qty_unit,
                    is_aggregate = EXCLUDED.is_aggregate,
                    created_at = now()
            """,
            (
                hs_code,
                year,
                month,
                value_usd,
                value_inr,
                fx_rate,
                qty,
                net_weight,
                qty_unit_code,
                qty_unit,
                partner_key,
                source,
                is_aggregate,
            ),
        )


def bulk_insert_monthly(
    conn,
    rows: Iterable[
        Tuple[
            str,
            int,
            int,
            Optional[float],
            Optional[float],
            Optional[float],
            Optional[float],
            Optional[float],
            Optional[int],
            Optional[str],
            str,
            str,
            bool,
        ]
    ],
) -> int:
    """Insert many monthly rows with upsert semantics."""

    rows = list(rows)
    if not rows:
        return 0
    with conn.cursor() as cur:
        execute_values(
            cur,
            """
            INSERT INTO monthly_imports (
                hs_code, year, month, value_usd, value_inr, fx_rate, qty,
                net_weight, qty_unit_code, qty_unit, partner_country, source, is_aggregate
            )
            VALUES %s
            ON CONFLICT (hs_code, year, month, partner_country, source) DO UPDATE
              SET value_usd = EXCLUDED.value_usd,
                  value_inr = EXCLUDED.value_inr,
                  fx_rate = EXCLUDED.fx_rate,
                  qty = EXCLUDED.qty,
                  net_weight = EXCLUDED.net_weight,
                  qty_unit_code = EXCLUDED.qty_unit_code,
                  qty_unit = EXCLUDED.qty_unit,
                  is_aggregate = EXCLUDED.is_aggregate,
                  created_at = now()
            """,
            rows,
            page_size=1_000,
        )
    return len(rows)


def delete_comtrade_aggregate_periods(conn, periods: Iterable[str]) -> int:
    values = [int(str(period).replace("-", "")) for period in periods]
    if not values:
        return 0
    with conn.cursor() as cur:
        cur.execute(
            """
            DELETE FROM monthly_imports
            WHERE source = 'comtrade'
              AND is_aggregate = true
              AND (year * 100 + month) = ANY(%s)
            """,
            (values,),
        )
        return int(cur.rowcount)


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


def bulk_upsert_baselines(
    conn,
    rows: Iterable[Tuple[str, Optional[float], Optional[str]]],
) -> int:
    values = list(rows)
    if not values:
        return 0
    with conn.cursor() as cur:
        execute_values(
            cur,
            """
            INSERT INTO baseline_imports (
                hs_code, baseline_12m_usd, baseline_period
            )
            VALUES %s
            ON CONFLICT (hs_code) DO UPDATE
              SET baseline_12m_usd = EXCLUDED.baseline_12m_usd,
                  baseline_period = EXCLUDED.baseline_period,
                  updated_at = now()
            """,
            values,
            page_size=1_000,
        )
    return len(values)


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


def bulk_upsert_progress(
    conn,
    rows: Iterable[
        Tuple[
            str,
            Optional[float],
            Optional[float],
            Optional[float],
            Optional[float],
            Optional[float],
            Optional[float],
            Optional[float],
            Optional[float],
        ]
    ],
) -> int:
    values = list(rows)
    if not values:
        return 0
    with conn.cursor() as cur:
        execute_values(
            cur,
            """
            INSERT INTO import_progress (
                hs_code, baseline_12m_usd, current_12m_usd, reduction_abs,
                reduction_pct, hhi_baseline, hhi_current,
                concentration_shift, opportunity_score
            )
            VALUES %s
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
            values,
            page_size=1_000,
        )
    return len(values)


def fetch_last_36m(conn, hs_code: str) -> List[Dict]:
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            SELECT MIN(year * 100 + month) AS first_period,
                   MAX(year * 100 + month) AS last_period,
                   COUNT(DISTINCT year * 100 + month) AS period_count
            FROM monthly_imports
            WHERE source = 'comtrade' AND is_aggregate = true
            """
        )
        coverage = cur.fetchone() or {}

    first_period = coverage.get("first_period")
    last_period = coverage.get("last_period")
    period_count = int(coverage.get("period_count") or 0)
    complete_bounds: Optional[Tuple[Tuple[int, int], Tuple[int, int]]] = None
    if first_period is not None and last_period is not None:
        first_value = int(first_period)
        last_value = int(last_period)
        first = (first_value // 100, first_value % 100)
        last = (last_value // 100, last_value % 100)
        expected = (last[0] - first[0]) * 12 + last[1] - first[1] + 1
        if expected >= 12 and period_count == expected:
            complete_bounds = (first, last)

    if complete_bounds is not None:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT year, month,
                       SUM(value_usd) AS value_usd,
                       SUM(value_inr) AS value_inr,
                       MAX(fx_rate) AS fx_rate,
                       SUM(qty) AS qty,
                       SUM(net_weight) AS net_weight
                FROM monthly_imports
                WHERE hs_code = %s
                  AND source = 'comtrade'
                  AND is_aggregate = true
                GROUP BY year, month
                ORDER BY year, month
                """,
                (hs_code,),
            )
            values = {
                (int(row["year"]), int(row["month"])): dict(row)
                for row in cur.fetchall()
            }

        start, end = complete_bounds
        months: List[Tuple[int, int]] = []
        year, month = start
        while (year, month) <= end:
            months.append((year, month))
            if month == 12:
                year, month = year + 1, 1
            else:
                month += 1
        result: List[Dict[str, Any]] = []
        for year, month in months[-36:]:
            row = values.get((year, month))
            result.append(
                {
                    "year": year,
                    "month": month,
                    "value_usd": row.get("value_usd") if row else 0,
                    "value_inr": row.get("value_inr") if row else None,
                    "fx_rate": row.get("fx_rate") if row else None,
                    "qty": row.get("qty") if row else None,
                    "net_weight": row.get("net_weight") if row else None,
                    "partner_country": None,
                }
            )
        return result

    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            WITH preferred AS (
                SELECT year, month,
                       SUM(value_usd) AS value_usd,
                       SUM(value_inr) AS value_inr,
                       MAX(fx_rate) AS fx_rate,
                       SUM(qty) AS qty,
                       SUM(net_weight) AS net_weight
                FROM monthly_imports
                WHERE hs_code = %s
                  AND (
                        is_aggregate = true
                     OR NOT EXISTS (
                            SELECT 1
                            FROM monthly_imports aggregate_rows
                            WHERE aggregate_rows.hs_code = %s
                              AND aggregate_rows.year = monthly_imports.year
                              AND aggregate_rows.month = monthly_imports.month
                              AND aggregate_rows.is_aggregate = true
                        )
                  )
                GROUP BY year, month
            ),
            latest AS (
                SELECT *
                FROM preferred
                ORDER BY year DESC, month DESC
                LIMIT 36
            )
            SELECT year, month, value_usd, value_inr, fx_rate, qty, net_weight,
                   NULL::TEXT AS partner_country
            FROM latest
            ORDER BY year, month
            """,
            (hs_code, hs_code),
        )
        return cur.fetchall()


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
              AND is_aggregate = false
              AND source IN ('dgcis', 'comtrade_partner')
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
        "SELECT year, month, SUM(value_usd) AS total FROM monthly_imports",
        " WHERE hs_code = %s",
        "   AND (is_aggregate = true OR NOT EXISTS (",
        "       SELECT 1 FROM monthly_imports aggregate_rows",
        "       WHERE aggregate_rows.hs_code = %s AND aggregate_rows.is_aggregate = true",
        "         AND aggregate_rows.year = monthly_imports.year",
        "         AND aggregate_rows.month = monthly_imports.month",
        "   ))",
    ]
    params: List[Any] = [hs_code, hs_code]
    if start and end:
        query.append("   AND make_date(year, month, 1) BETWEEN make_date(%s, %s, 1) AND make_date(%s, %s, 1)")
        params.extend([start[0], start[1], end[0], end[1]])
    query.append(" GROUP BY year, month ORDER BY year, month")
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("\n".join(query), params)
        return cur.fetchall()


def latest_data_status(conn) -> Optional[Dict[str, Any]]:
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            SELECT year, month, source, is_aggregate, MAX(created_at) AS loaded_at
            FROM monthly_imports
            GROUP BY year, month, source, is_aggregate
            ORDER BY year DESC, month DESC, is_aggregate DESC, loaded_at DESC
            LIMIT 1
            """
        )
        result = cur.fetchone()
    if not result:
        return None
    return dict(result)


def latest_year_month(conn) -> Optional[Tuple[int, int]]:
    status = latest_data_status(conn)
    if not status:
        return None
    return int(status["year"]), int(status["month"])


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
