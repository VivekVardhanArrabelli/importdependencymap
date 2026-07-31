"""Batch jobs that recompute analytics tables."""
from __future__ import annotations

import logging
from collections import defaultdict
from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

from psycopg2.extras import RealDictCursor

from . import db, util

LOGGER = logging.getLogger(__name__)

Period = Tuple[int, int]


@dataclass
class MonthlyTotal:
    year: int
    month: int
    total: float


def _next_month(year: int, month: int) -> Period:
    if month == 12:
        return year + 1, 1
    return year, month + 1


def _periods_between(start: Period, end: Period) -> List[Period]:
    if start > end:
        return []
    periods: List[Period] = []
    current = start
    while current <= end:
        periods.append(current)
        current = _next_month(*current)
    return periods


def _fill_missing_months(
    monthly: Sequence[MonthlyTotal],
    *,
    bounds: Optional[Tuple[Period, Period]] = None,
) -> List[MonthlyTotal]:
    """Return a contiguous series, inserting zero totals for absent months.

    Comtrade omits a commodity/month row when its value is zero. Once a
    complete aggregate ETL window exists, ``bounds`` lets those omitted rows be
    represented as genuine zeros instead of looking like missing data.
    """

    if bounds is not None:
        start, end = bounds
    elif monthly:
        start = (monthly[0].year, monthly[0].month)
        end = (monthly[-1].year, monthly[-1].month)
    else:
        return []

    totals = {(row.year, row.month): float(row.total) for row in monthly}
    return [
        MonthlyTotal(year, month, totals.get((year, month), 0.0))
        for year, month in _periods_between(start, end)
    ]


def _window_of_12(
    monthly: Sequence[MonthlyTotal],
    *,
    latest: bool = False,
    bounds: Optional[Tuple[Period, Period]] = None,
) -> Optional[List[MonthlyTotal]]:
    filled = _fill_missing_months(monthly, bounds=bounds)
    if len(filled) < 12:
        return None
    return filled[-12:] if latest else filled[:12]


def _load_monthly_totals(
    conn,
) -> Tuple[Dict[str, List[MonthlyTotal]], Optional[Tuple[Period, Period]]]:
    """Load every product's authoritative monthly totals in one query.

    When at least 12 Comtrade World-total periods exist, those rows form a
    complete reporting window and are used exclusively. Otherwise the legacy
    per-month preference (World aggregate, else country-row sum) is retained.
    """

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

    bounds: Optional[Tuple[Period, Period]] = None
    first_period = coverage.get("first_period")
    last_period = coverage.get("last_period")
    if first_period is not None and last_period is not None:
        first_value = int(first_period)
        last_value = int(last_period)
        candidate_bounds = (
            (first_value // 100, first_value % 100),
            (last_value // 100, last_value % 100),
        )
        expected_periods = len(_periods_between(*candidate_bounds))
        if (
            expected_periods >= 12
            and int(coverage.get("period_count") or 0) == expected_periods
        ):
            bounds = candidate_bounds

    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        if bounds is not None:
            cur.execute(
                """
                SELECT hs_code, year, month, SUM(value_usd) AS total
                FROM monthly_imports
                WHERE source = 'comtrade' AND is_aggregate = true
                GROUP BY hs_code, year, month
                ORDER BY hs_code, year, month
                """
            )
        else:
            cur.execute(
                """
                SELECT mi.hs_code, mi.year, mi.month, SUM(mi.value_usd) AS total
                FROM monthly_imports mi
                WHERE mi.is_aggregate = true
                   OR NOT EXISTS (
                        SELECT 1
                        FROM monthly_imports aggregate_rows
                        WHERE aggregate_rows.hs_code = mi.hs_code
                          AND aggregate_rows.year = mi.year
                          AND aggregate_rows.month = mi.month
                          AND aggregate_rows.is_aggregate = true
                   )
                GROUP BY mi.hs_code, mi.year, mi.month
                ORDER BY mi.hs_code, mi.year, mi.month
                """
            )
        rows = cur.fetchall()

    totals: Dict[str, List[MonthlyTotal]] = defaultdict(list)
    for row in rows:
        totals[str(row["hs_code"])].append(
            MonthlyTotal(
                int(row["year"]),
                int(row["month"]),
                float(row["total"] or 0),
            )
        )
    return dict(totals), bounds


def _load_partner_monthly(conn) -> Dict[str, Dict[Period, Dict[str, float]]]:
    """Load all stored country-detail totals in one query."""

    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            SELECT hs_code, year, month, partner_country, SUM(value_usd) AS total
            FROM monthly_imports
            WHERE is_aggregate = false
              AND source IN ('dgcis', 'comtrade_partner')
            GROUP BY hs_code, year, month, partner_country
            HAVING SUM(value_usd) IS NOT NULL
            ORDER BY hs_code, year, month
            """
        )
        rows = cur.fetchall()

    result: Dict[str, Dict[Period, Dict[str, float]]] = defaultdict(
        lambda: defaultdict(dict)
    )
    for row in rows:
        result[str(row["hs_code"])][
            (int(row["year"]), int(row["month"]))
        ][str(row["partner_country"])] = float(row["total"])
    return {
        hs_code: {period: dict(values) for period, values in periods.items()}
        for hs_code, periods in result.items()
    }


def _partner_shares_for_periods(
    monthly: Dict[Period, Dict[str, float]],
    periods: Iterable[Period],
) -> List[float]:
    totals: Dict[str, float] = defaultdict(float)
    for period in periods:
        for partner, value in monthly.get(period, {}).items():
            totals[partner] += value
    grand_total = sum(totals.values())
    if grand_total <= 0:
        return []
    return [value / grand_total for value in totals.values()]


def _opportunity_score(
    normalized_import_value: float,
    hhi_current: Optional[float],
    sectors: Optional[Sequence[str]],
) -> Optional[float]:
    """Return a score only when supplier concentration is actually known."""

    if hhi_current is None:
        return None
    tech_score = util.tech_feasibility_for(sectors)
    return normalized_import_value * (1 - hhi_current) * tech_score


def recompute_baseline(conn=None) -> Dict[str, int]:
    """Populate baselines with the earliest complete 12-month window."""

    if conn is None:
        with db.connect() as managed:
            return recompute_baseline(managed)

    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            "SELECT hs_code FROM products WHERE catalog_active = true ORDER BY hs_code"
        )
        products = [str(row["hs_code"]) for row in cur.fetchall()]

    totals_by_product, bounds = _load_monthly_totals(conn)
    baseline_rows: List[Tuple[str, Optional[float], Optional[str]]] = []
    with_baseline = 0
    for hs_code in products:
        window = _window_of_12(
            totals_by_product.get(hs_code, []),
            bounds=bounds,
        )
        if window is None:
            baseline_rows.append((hs_code, None, "insufficient_data"))
            continue
        value = sum(item.total for item in window)
        start = window[0]
        end = window[-1]
        period = (
            f"{start.year:04d}-{start.month:02d}_to_"
            f"{end.year:04d}-{end.month:02d}"
        )
        baseline_rows.append((hs_code, value, period))
        with_baseline += 1

    db.bulk_upsert_baselines(conn, baseline_rows)
    LOGGER.info(
        "Baseline recompute complete: %s processed, %s with baseline",
        len(products),
        with_baseline,
    )
    return {"processed": len(products), "with_baseline": with_baseline}


def recompute_progress(conn=None) -> Dict[str, int]:
    """Bulk recompute rolling imports, partner HHI, and opportunity scores."""

    if conn is None:
        with db.connect() as managed:
            return recompute_progress(managed)

    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            SELECT hs_code, sectors
            FROM products
            WHERE catalog_active = true
            ORDER BY hs_code
            """
        )
        products = [
            (str(row["hs_code"]), list(row.get("sectors") or []))
            for row in cur.fetchall()
        ]
        cur.execute(
            "SELECT hs_code, baseline_12m_usd, baseline_period FROM baseline_imports"
        )
        baseline_map = {
            str(row["hs_code"]): {
                "baseline": (
                    float(row["baseline_12m_usd"])
                    if row["baseline_12m_usd"] is not None
                    else None
                ),
                "period": row.get("baseline_period"),
            }
            for row in cur.fetchall()
        }

    totals_by_product, bounds = _load_monthly_totals(conn)
    partner_monthly = _load_partner_monthly(conn)
    current_totals: Dict[str, float] = {}
    metrics: Dict[str, Dict[str, object]] = {}

    for hs_code, sectors in products:
        window = _window_of_12(
            totals_by_product.get(hs_code, []),
            latest=True,
            bounds=bounds,
        )
        if window is None:
            metrics[hs_code] = {
                "current": None,
                "reduction_abs": None,
                "reduction_pct": None,
                "hhi_current": None,
                "hhi_baseline": None,
                "concentration_shift": None,
                "sectors": sectors,
            }
            continue

        current_total = sum(item.total for item in window)
        current_totals[hs_code] = current_total
        baseline_info = baseline_map.get(hs_code, {})
        baseline_value = baseline_info.get("baseline")
        baseline_number = (
            float(baseline_value) if baseline_value is not None else None
        )
        reduction_abs = (
            baseline_number - current_total
            if baseline_number is not None
            else None
        )
        reduction_pct = (
            reduction_abs / baseline_number
            if reduction_abs is not None and baseline_number not in (None, 0)
            else None
        )

        product_partners = partner_monthly.get(hs_code, {})
        current_periods = [(item.year, item.month) for item in window]
        hhi_current = util.hhi_from_shares(
            _partner_shares_for_periods(product_partners, current_periods)
        )

        hhi_baseline: Optional[float] = None
        period_text = str(baseline_info.get("period") or "")
        if "_to_" in period_text:
            start_text, end_text = period_text.split("_to_", 1)
            baseline_periods = _periods_between(
                (int(start_text[:4]), int(start_text[5:7])),
                (int(end_text[:4]), int(end_text[5:7])),
            )
            hhi_baseline = util.hhi_from_shares(
                _partner_shares_for_periods(product_partners, baseline_periods)
            )

        concentration_shift = (
            hhi_baseline - hhi_current
            if hhi_baseline is not None and hhi_current is not None
            else None
        )
        metrics[hs_code] = {
            "current": current_total,
            "reduction_abs": reduction_abs,
            "reduction_pct": reduction_pct,
            "hhi_current": hhi_current,
            "hhi_baseline": hhi_baseline,
            "concentration_shift": concentration_shift,
            "sectors": sectors,
        }

    normalized = util.norm_log(current_totals)
    progress_rows = []
    scored = 0
    for hs_code, sectors in products:
        metric = metrics[hs_code]
        score = (
            _opportunity_score(
                normalized.get(hs_code, 0.0),
                metric["hhi_current"],  # type: ignore[arg-type]
                sectors,
            )
            if metric["current"] is not None
            else None
        )
        if score is not None:
            scored += 1
        baseline_value = baseline_map.get(hs_code, {}).get("baseline")
        progress_rows.append(
            (
                hs_code,
                baseline_value,
                metric["current"],
                metric["reduction_abs"],
                metric["reduction_pct"],
                metric["hhi_baseline"],
                metric["hhi_current"],
                metric["concentration_shift"],
                score,
            )
        )

    db.bulk_upsert_progress(conn, progress_rows)
    LOGGER.info(
        "Progress recompute complete: %s products, %s scored",
        len(products),
        scored,
    )
    return {"processed": len(products), "scored": scored}
