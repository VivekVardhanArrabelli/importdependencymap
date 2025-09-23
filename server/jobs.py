"""Batch jobs that recompute analytics tables."""
from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

from psycopg2.extras import RealDictCursor

from . import db, util

LOGGER = logging.getLogger(__name__)


@dataclass
class MonthlyTotal:
    year: int
    month: int
    total: float


def _next_month(year: int, month: int) -> Tuple[int, int]:
    if month == 12:
        return year + 1, 1
    return year, month + 1


def _fill_missing_months(monthly: List[MonthlyTotal]) -> List[MonthlyTotal]:
    """Return a contiguous series, inserting zero totals for gaps."""

    if not monthly:
        return []

    totals = {(row.year, row.month): float(row.total) for row in monthly}
    start_year, start_month = monthly[0].year, monthly[0].month
    end_year, end_month = monthly[-1].year, monthly[-1].month

    filled: List[MonthlyTotal] = []
    year, month = start_year, start_month
    while True:
        filled.append(MonthlyTotal(year, month, totals.get((year, month), 0.0)))
        if (year, month) == (end_year, end_month):
            break
        year, month = _next_month(year, month)
    return filled


def _window_of_12(monthly: List[MonthlyTotal], *, latest: bool = False) -> Optional[List[MonthlyTotal]]:
    filled = _fill_missing_months(monthly)
    if len(filled) < 12:
        return None
    if latest:
        return filled[-12:]
    return filled[:12]


def _monthly_totals(conn, hs_code: str) -> List[MonthlyTotal]:
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            SELECT year, month, SUM(value_usd) AS total
            FROM monthly_imports
            WHERE hs_code = %s
            GROUP BY year, month
            ORDER BY year, month
            """,
            (hs_code,),
        )
        rows = cur.fetchall()
    return [MonthlyTotal(int(row["year"]), int(row["month"]), float(row["total"] or 0)) for row in rows]


def recompute_baseline(conn=None) -> Dict[str, int]:
    """Populate baseline_imports with the earliest contiguous 12-month window."""

    own_connection = conn is None
    if own_connection:
        with db.connect() as managed:
            return recompute_baseline(managed)

    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("SELECT hs_code FROM products ORDER BY hs_code")
        products = [row["hs_code"] for row in cur.fetchall()]

    processed = 0
    with_baseline = 0
    for code in products:
        monthly = _monthly_totals(conn, code)
        window = _window_of_12(monthly)
        if not window:
            db.upsert_baseline(conn, hs_code=code, baseline_value=None, baseline_period="insufficient_data")
            processed += 1
            continue

        baseline_value = sum(item.total for item in window)
        start = window[0]
        end = window[-1]
        baseline_period = f"{start.year:04d}-{start.month:02d}_to_{end.year:04d}-{end.month:02d}"
        db.upsert_baseline(conn, hs_code=code, baseline_value=baseline_value, baseline_period=baseline_period)
        processed += 1
        with_baseline += 1

    LOGGER.info("Baseline recompute complete: %s processed, %s with baseline", processed, with_baseline)
    return {"processed": processed, "with_baseline": with_baseline}


def recompute_progress(conn=None) -> Dict[str, int]:
    """Recompute rolling 12-month metrics, HHI, and opportunity scores."""

    own_connection = conn is None
    if own_connection:
        with db.connect() as managed:
            return recompute_progress(managed)

    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("SELECT hs_code, sectors FROM products ORDER BY hs_code")
        products = cur.fetchall()

    baseline_map: Dict[str, Dict[str, Optional[float]]] = {}
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("SELECT hs_code, baseline_12m_usd, baseline_period FROM baseline_imports")
        for row in cur.fetchall():
            baseline_map[row["hs_code"]] = {
                "baseline": float(row["baseline_12m_usd"]) if row["baseline_12m_usd"] is not None else None,
                "period": row.get("baseline_period"),
            }

    current_totals: Dict[str, float] = {}
    metrics: Dict[str, Dict[str, Optional[float]]] = {}

    for row in products:
        hs_code = row["hs_code"]
        sectors = row.get("sectors") or []
        monthly = _monthly_totals(conn, hs_code)
        window = _window_of_12(monthly, latest=True)
        if not window:
            metrics[hs_code] = {
                "current": None,
                "reduction_abs": None,
                "reduction_pct": None,
                "hhi_current": None,
                "hhi_baseline": None,
                "concentration_shift": None,
                "opportunity_score": None,
                "sectors": sectors,
            }
            continue

        current_total = sum(item.total for item in window)
        current_totals[hs_code] = current_total
        start = window[0]
        end = window[-1]
        baseline_info = baseline_map.get(hs_code, {})
        baseline_value = baseline_info.get("baseline")
        reduction_abs = (baseline_value - current_total) if baseline_value is not None else None
        if baseline_value in (None, 0) or reduction_abs is None:
            reduction_pct = None
        else:
            reduction_pct = reduction_abs / baseline_value

        period_str = baseline_info.get("period")
        hhi_baseline = None
        if period_str and "_to_" in period_str:
            start_str, end_str = period_str.split("_to_")
            b_start = (int(start_str[:4]), int(start_str[5:7]))
            b_end = (int(end_str[:4]), int(end_str[5:7]))
            shares = db.partner_shares(conn, hs_code, start=b_start, end=b_end).values()
            hhi_baseline = util.hhi_from_shares(list(shares))

        current_shares = db.partner_shares(conn, hs_code, start=(start.year, start.month), end=(end.year, end.month))
        hhi_current = util.hhi_from_shares(list(current_shares.values()))
        concentration_shift = None
        if hhi_baseline is not None or hhi_current is not None:
            concentration_shift = (hhi_baseline or 0.0) - (hhi_current or 0.0)

        metrics[hs_code] = {
            "current": current_total,
            "reduction_abs": reduction_abs,
            "reduction_pct": reduction_pct,
            "hhi_current": hhi_current,
            "hhi_baseline": hhi_baseline,
            "concentration_shift": concentration_shift,
            "opportunity_score": None,
            "sectors": sectors,
        }

    norm = util.norm_log({code: value for code, value in current_totals.items()})

    for hs_code, metric in metrics.items():
        current_value = metric["current"]
        if current_value is None:
            opportunity = None
        else:
            tech_score = util.tech_feasibility_for(metric.get("sectors"))
            import_value = norm.get(hs_code, 0.0)
            hhi_current = metric["hhi_current"] or 0.0
            opportunity = import_value * (1 - hhi_current) * tech_score * 1.0
        metric["opportunity_score"] = opportunity

        baseline_info = baseline_map.get(hs_code, {})
        db.upsert_progress(
            conn,
            hs_code=hs_code,
            baseline_value=baseline_info.get("baseline"),
            current_value=current_value,
            reduction_abs=metric["reduction_abs"],
            reduction_pct=metric["reduction_pct"],
            hhi_baseline=metric["hhi_baseline"],
            hhi_current=metric["hhi_current"],
            concentration_shift=metric["concentration_shift"],
            opportunity_score=metric["opportunity_score"],
        )

    LOGGER.info("Progress recompute complete for %s products", len(metrics))
    return {"processed": len(metrics)}
