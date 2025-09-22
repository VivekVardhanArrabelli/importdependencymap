"""Batch jobs that recompute analytics tables."""
from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

from psycopg2.extras import RealDictCursor

from . import db, util

LOGGER = logging.getLogger(__name__)


@dataclass
class BaselineResult:
    hs_code: str
    baseline_value: Optional[float]
    baseline_period: Optional[str]
    start: Optional[Tuple[int, int]]
    end: Optional[Tuple[int, int]]
    method: str


def _month_key(year: int, month: int) -> int:
    return year * 12 + (month - 1)


def _ensure_contiguous(months: List[Tuple[int, int]]) -> bool:
    if len(months) < 2:
        return True
    start_key = _month_key(*months[0])
    for idx, (year, month) in enumerate(months):
        if _month_key(year, month) != start_key + idx:
            return False
    return True


def _parse_period(period: Optional[str]) -> Optional[Tuple[Tuple[int, int], Tuple[int, int]]]:
    if not period or "_to_" not in period:
        return None
    start, end = period.split("_to_")
    start_year, start_month = map(int, start.split("-"))
    end_year, end_month = map(int, end.split("-"))
    return (start_year, start_month), (end_year, end_month)


def _shift_month(year: int, month: int, delta: int) -> Tuple[int, int]:
    key = _month_key(year, month) + delta
    if key < 0:
        raise ValueError("Month delta produced negative value")
    quotient, remainder = divmod(key, 12)
    return quotient, remainder + 1


def _collect_window(
    monthly_map: Dict[Tuple[int, int], float],
    end_year: int,
    end_month: int,
    length: int = 12,
) -> Optional[List[Tuple[Tuple[int, int], float]]]:
    window: List[Tuple[Tuple[int, int], float]] = []
    for offset in range(length - 1, -1, -1):
        year, month = _shift_month(end_year, end_month, -offset)
        value = monthly_map.get((year, month))
        if value is None:
            return None
        window.append(((year, month), float(value)))
    return window


def _calendar_year_window(
    monthly_map: Dict[Tuple[int, int], float], year: int
) -> Optional[List[Tuple[Tuple[int, int], float]]]:
    months = []
    for month in range(1, 13):
        value = monthly_map.get((year, month))
        if value is None:
            return None
        months.append(((year, month), float(value)))
    return months


def recompute_baseline(conn=None, period: str = "last_24m") -> Dict[str, int]:
    """Recompute the baseline_imports table for all products."""

    own_connection = conn is None
    if own_connection:
        with db.connect() as managed:
            return recompute_baseline(managed, period=period)

    results: List[BaselineResult] = []
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("SELECT hs_code FROM products ORDER BY hs_code")
        products = [row["hs_code"] for row in cur.fetchall()]

    for code in products:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT year, month, SUM(value_usd) AS total
                FROM monthly_imports
                WHERE hs_code = %s
                GROUP BY year, month
                ORDER BY year, month
                """,
                (code,),
            )
            monthly = cur.fetchall()

        if not monthly:
            results.append(BaselineResult(code, None, None, None, None, "none"))
            continue

        monthly_map = {
            (row["year"], row["month"]): float(row["total"] or 0) for row in monthly
        }

        latest_year, latest_month = max(monthly_map, key=lambda item: _month_key(*item))

        window: Optional[List[Tuple[Tuple[int, int], float]]] = None
        method = "none"

        if period == "last_24m":
            window = _collect_window(monthly_map, latest_year, latest_month, 12)
            if window:
                method = "rolling"

        if window is None:
            # fallback to most recent full calendar year
            candidate_years = sorted({year for year, _ in monthly_map.keys()}, reverse=True)
            for candidate in candidate_years:
                calendar_window = _calendar_year_window(monthly_map, candidate)
                if calendar_window:
                    window = calendar_window
                    method = "calendar_year"
                    break

        if window is None:
            results.append(BaselineResult(code, None, None, None, None, method))
            continue

        start_year, start_month = window[0][0]
        end_year, end_month = window[-1][0]
        baseline_value = sum(value for _, value in window)
        baseline_period = f"{start_year:04d}-{start_month:02d}_to_{end_year:04d}-{end_month:02d}"

        results.append(
            BaselineResult(
                code,
                baseline_value,
                baseline_period,
                (start_year, start_month),
                (end_year, end_month),
                method,
            )
        )

    with conn.cursor() as cur:
        for item in results:
            cur.execute(
                """
                INSERT INTO baseline_imports (hs_code, baseline_12m_usd, baseline_period, updated_at)
                VALUES (%s, %s, %s, now())
                ON CONFLICT (hs_code) DO UPDATE
                SET baseline_12m_usd = EXCLUDED.baseline_12m_usd,
                    baseline_period = EXCLUDED.baseline_period,
                    updated_at = now()
                """,
                (item.hs_code, item.baseline_value, item.baseline_period),
            )
    LOGGER.info("Baseline recompute complete for %s products", len(results))

    with_count = sum(1 for item in results if item.baseline_value is not None)
    rolling_count = sum(1 for item in results if item.method == "rolling")
    calendar_count = sum(1 for item in results if item.method == "calendar_year")
    return {
        "processed": len(results),
        "with_baseline": with_count,
        "rolling": rolling_count,
        "calendar_year": calendar_count,
    }


def recompute_progress(conn=None) -> Dict[str, int]:
    """Recompute import_progress metrics based on baseline and current imports."""

    own_connection = conn is None
    if own_connection:
        with db.connect() as managed:
            return recompute_progress(managed)

    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("SELECT hs_code, sectors FROM products ORDER BY hs_code")
        products = cur.fetchall()
    sectors_map = {row["hs_code"]: row["sectors"] for row in products}

    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("SELECT hs_code, baseline_12m_usd, baseline_period FROM baseline_imports")
        baseline_rows = cur.fetchall()
    baseline_map = {
        row["hs_code"]: {
            "baseline": float(row["baseline_12m_usd"]) if row["baseline_12m_usd"] is not None else None,
            "period": row["baseline_period"],
        }
        for row in baseline_rows
    }

    metrics: Dict[str, Dict[str, Optional[float]]] = {}
    current_totals: Dict[str, float] = {}
    for code in sectors_map:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT year, month, SUM(value_usd) AS total
                FROM monthly_imports
                WHERE hs_code = %s
                GROUP BY year, month
                ORDER BY year, month
                """,
                (code,),
            )
            monthly = cur.fetchall()

        if len(monthly) < 12:
            metrics[code] = {
                "current": None,
                "reduction_abs": None,
                "reduction_pct": None,
                "hhi_current": None,
                "hhi_baseline": None,
                "concentration_shift": None,
                "opportunity_score": None,
            }
            continue

        window = monthly[-12:]
        months = [(row["year"], row["month"]) for row in window]
        if not _ensure_contiguous(months):
            metrics[code] = {
                "current": None,
                "reduction_abs": None,
                "reduction_pct": None,
                "hhi_current": None,
                "hhi_baseline": None,
                "concentration_shift": None,
                "opportunity_score": None,
            }
            continue

        current_total = sum(float(row["total"] or 0) for row in window)
        current_totals[code] = current_total

        baseline_info = baseline_map.get(code)
        baseline_value = baseline_info.get("baseline") if baseline_info else None
        reduction_abs = baseline_value - current_total if baseline_value is not None else None
        if baseline_value in (None, 0) or reduction_abs is None:
            reduction_pct = None
        else:
            reduction_pct = reduction_abs / baseline_value

        baseline_period = baseline_info.get("period") if baseline_info else None
        baseline_bounds = _parse_period(baseline_period) if baseline_period else None
        hhi_current = util.hhi_from_shares(
            list(
                db.partner_shares(
                    conn,
                    code,
                    period="current",
                    start=months[0],
                    end=months[-1],
                ).values()
            )
        )
        hhi_baseline = None
        if baseline_bounds:
            hhi_baseline = util.hhi_from_shares(
                list(
                    db.partner_shares(
                        conn,
                        code,
                        period="baseline",
                        start=baseline_bounds[0],
                        end=baseline_bounds[1],
                    ).values()
                )
            )
        concentration_shift = None
        if hhi_baseline is not None or hhi_current is not None:
            concentration_shift = (hhi_baseline or 0) - (hhi_current or 0)

        metrics[code] = {
            "current": current_total,
            "reduction_abs": reduction_abs,
            "reduction_pct": reduction_pct,
            "hhi_current": hhi_current,
            "hhi_baseline": hhi_baseline,
            "concentration_shift": concentration_shift,
            "opportunity_score": None,
        }

    norm = util.norm_log(current_totals)

    for code, metric in metrics.items():
        current_total = metric["current"]
        if current_total is None:
            metric["opportunity_score"] = None
            continue
        hhi_current = metric["hhi_current"] or 0.0
        tech_score = util.tech_feasibility_for(sectors_map.get(code))
        import_value = norm.get(code, 0.0)
        opportunity = import_value * (1 - hhi_current) * tech_score * 1.0
        metric["opportunity_score"] = opportunity

    with conn.cursor() as cur:
        for code, metric in metrics.items():
            baseline_info = baseline_map.get(code, {})
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
                    code,
                    baseline_info.get("baseline"),
                    metric["current"],
                    metric["reduction_abs"],
                    metric["reduction_pct"],
                    metric["hhi_baseline"],
                    metric["hhi_current"],
                    metric["concentration_shift"],
                    metric["opportunity_score"],
                ),
            )
    LOGGER.info("Progress recompute complete for %s products", len(metrics))
    return {"processed": len(metrics)}
