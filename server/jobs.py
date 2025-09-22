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


def recompute_baseline(conn=None) -> Dict[str, int]:
    """Recompute the baseline_imports table for all products."""

    own_connection = conn is None
    if own_connection:
        with db.connect() as managed:
            return recompute_baseline(managed)

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

        if len(monthly) < 12:
            results.append(BaselineResult(code, None, "insufficient_data", None, None))
            continue

        baseline_value: Optional[float] = None
        baseline_period: Optional[str] = None
        baseline_months: Optional[List[Tuple[int, int]]] = None

        for idx in range(0, len(monthly) - 11):
            window = monthly[idx : idx + 12]
            months = [(row["year"], row["month"]) for row in window]
            if not _ensure_contiguous(months):
                continue
            baseline_value = sum(float(row["total"] or 0) for row in window)
            start_year, start_month = months[0]
            end_year, end_month = months[-1]
            baseline_period = f"{start_year:04d}-{start_month:02d}_to_{end_year:04d}-{end_month:02d}"
            baseline_months = months
            break

        if baseline_months is None:
            results.append(BaselineResult(code, None, "insufficient_data", None, None))
            continue

        results.append(
            BaselineResult(
                code,
                baseline_value,
                baseline_period,
                baseline_months[0],
                baseline_months[-1],
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
    return {"processed": len(results), "with_baseline": with_count}


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
