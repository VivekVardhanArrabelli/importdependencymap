"""FastAPI entrypoint for Build for India."""
from __future__ import annotations

import csv
import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from dotenv import load_dotenv
from fastapi import Depends, FastAPI, Header, HTTPException, Query, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, HTMLResponse
from psycopg2.extras import RealDictCursor

from . import db, jobs
from .etl import comtrade, normalize
from .schemas import DomesticCapabilityPayload, ProductCard

load_dotenv()

LOGGER = logging.getLogger(__name__)
logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))

app = FastAPI(title="Build for India")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

DATA_PATH = Path("data/top100_hs.csv")
DEFAULT_SOURCE = "database"
CSV_SOURCE = "csv_fallback"
ADMIN_SOURCE = "admin"
MANUAL_SOURCE = "manual"

DEFAULT_SEED = """hs_code,title,description,sectors,capex_min,capex_max,seed_month_value,top_country
850440,Static converters,Power converters,"{electronics}",50000,200000,120000,China
840721,Reactors and vessels,Industrial pressure vessels,"{industrial}",70000,300000,80000,Germany
854231,Integrated circuits,ICs general,"{electronics}",1000000,5000000,500000,China
870323,Motor vehicles,Passenger vehicles,"{automotive}",2000000,8000000,1500000,Germany
730799,Tube/pipe fittings,Steel fittings,"{metals}",25000,120000,45000,China
841459,Industrial fans,Axial fans,"{industrial}",30000,200000,60000,Thailand
848180,Valves (other),General-purpose valves,"{metals}",40000,250000,90000,China
850760,Lithium-ion batteries,Rechargeable cells/packs,"{energy,electronics}",200000,1500000,300000,China
902710,Gas/smoke analyzers,Environmental instruments,"{instruments}",60000,350000,70000,Japan
940540,LED lamps,LED bulbs and lamps,"{electronics}",50000,300000,85000,Vietnam
"""


class AdminGuard:
    """Dependency to guard admin endpoints."""

    def __call__(self, authorization: Optional[str] = Header(default=None)) -> None:
        admin_key = os.getenv("ADMIN_KEY")
        if not admin_key:
            LOGGER.warning("ADMIN_KEY not configured; denying admin access")
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Unauthorized")
        if not authorization or not authorization.startswith("Bearer "):
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Unauthorized")
        token = authorization.split(" ", 1)[1].strip()
        if token != admin_key:
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Unauthorized")


admin_required = AdminGuard()


def _database_available() -> bool:
    return bool(os.getenv("DATABASE_URL"))


def _ensure_seed_file() -> None:
    DATA_PATH.parent.mkdir(parents=True, exist_ok=True)
    if not DATA_PATH.exists():
        DATA_PATH.write_text(DEFAULT_SEED, encoding="utf-8")
        LOGGER.info("Seed CSV created at %s", DATA_PATH)


def _read_seed_rows() -> List[Dict[str, str]]:
    _ensure_seed_file()
    with DATA_PATH.open("r", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def _parse_csv_row(row: Dict[str, str]) -> Dict[str, Any]:
    sectors = normalize.parse_csv_sectors(row.get("sectors", ""))
    capex_min = row.get("capex_min")
    capex_max = row.get("capex_max")
    seed_value = row.get("seed_month_value")
    return {
        "hs_code": (row.get("hs_code") or "").strip(),
        "title": (row.get("title") or "").strip(),
        "description": (row.get("description") or "").strip(),
        "sectors": sectors,
        "capex_min": float(capex_min) if capex_min else None,
        "capex_max": float(capex_max) if capex_max else None,
        "seed_month_value": float(seed_value) if seed_value else None,
        "top_country": (row.get("top_country") or "").strip() or None,
    }


@app.on_event("startup")
def ensure_schema() -> None:
    if not _database_available():
        LOGGER.info("DATABASE_URL not configured; running in CSV-only mode")
        return
    try:
        with db.connect() as conn:
            db.ensure_extensions(conn)
            db.init_db(conn)
    except Exception as exc:  # pragma: no cover - best effort
        LOGGER.warning("Database init skipped: %s", exc)


@app.get("/", include_in_schema=False)
def serve_index():
    index_path = Path("client/index.html")
    if index_path.exists():
        return FileResponse(index_path)
    return HTMLResponse("<h1>Build for India</h1><p>Client not found.</p>")


@app.get("/health")
def health() -> Dict[str, bool]:
    return {"ok": True}


@app.post("/admin/seed")
def seed_database(_: None = Depends(admin_required)) -> Dict[str, Any]:
    rows = [_parse_csv_row(row) for row in _read_seed_rows()]
    if not rows:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Seed CSV is empty")

    if not _database_available():
        return {
            "seeded": False,
            "message": "DATABASE_URL not configured; CSV fallback only",
            "items": len(rows),
            "source": CSV_SOURCE,
            "last_updated": datetime.now(timezone.utc).isoformat(),
        }

    now_iso = datetime.now(timezone.utc).isoformat()
    with db.connect() as conn:
        db.init_db(conn)
        product_count = 0
        monthly_count = 0
        for row in rows:
            db.upsert_product(
                conn,
                hs_code=row["hs_code"],
                title=row["title"],
                description=row["description"],
                sectors=row["sectors"],
                capex_min=row["capex_min"],
                capex_max=row["capex_max"],
            )
            product_count += 1
            for month in range(1, 13):
                db.insert_monthly(
                    conn,
                    hs_code=row["hs_code"],
                    year=datetime.now(timezone.utc).year,
                    month=month,
                    value_usd=normalize.ensure_usd(row["seed_month_value"]),
                    qty=None,
                    partner=row["top_country"],
                )
                monthly_count += 1

        baseline_summary = jobs.recompute_baseline(conn)
        progress_summary = jobs.recompute_progress(conn)

    return {
        "seeded": True,
        "products": product_count,
        "monthly_rows": monthly_count,
        "baseline": baseline_summary,
        "progress": progress_summary,
        "source": ADMIN_SOURCE,
        "last_updated": now_iso,
    }


def _parse_period(period: str) -> str:
    try:
        parsed = datetime.strptime(period, "%Y-%m")
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Invalid period: {period}") from exc
    return parsed.strftime("%Y%m")


@app.post("/admin/etl/comtrade")
def trigger_comtrade(
    _: None = Depends(admin_required),
    from_period: str = Query(..., alias="from", description="YYYY-MM inclusive start"),
    to_period: str = Query(..., alias="to", description="YYYY-MM inclusive end"),
) -> Dict[str, Any]:
    if not _database_available():
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="DATABASE_URL not configured")

    start_key = _parse_period(from_period)
    end_key = _parse_period(to_period)
    if start_key > end_key:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="from must be <= to")

    with db.connect() as conn:
        summary = comtrade.run(conn, from_period=start_key, to_period=end_key)
        baseline_summary = jobs.recompute_baseline(conn)
        progress_summary = jobs.recompute_progress(conn)

        summary.update(
        {
            "baseline": baseline_summary,
            "progress": progress_summary,
            "last_updated": datetime.now(timezone.utc).isoformat(),
        }
    )
    return summary


@app.post("/admin/recompute")
def trigger_recompute(_: None = Depends(admin_required)) -> Dict[str, Any]:
    if not _database_available():
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="DATABASE_URL not configured")
    with db.connect() as conn:
        baseline_summary = jobs.recompute_baseline(conn)
        progress_summary = jobs.recompute_progress(conn)
    return {
        "baseline": baseline_summary,
        "progress": progress_summary,
        "source": ADMIN_SOURCE,
        "last_updated": datetime.now(timezone.utc).isoformat(),
    }


def _products_from_csv(
    *,
    sectors: Optional[str],
    combine: str,
    min_capex: Optional[float],
    max_capex: Optional[float],
    sort: str,
    limit: int,
) -> Dict[str, Any]:
    rows = [_parse_csv_row(row) for row in _read_seed_rows()]
    sector_list = [s.strip().lower() for s in sectors.split(",") if s.strip()] if sectors else []
    combine = combine.upper()
    items: List[Dict[str, Any]] = []
    for row in rows:
        row_sectors = [s.lower() for s in row["sectors"]]
        if sector_list:
            if combine == "AND":
                if not all(s in row_sectors for s in sector_list):
                    continue
            else:
                if not any(s in row_sectors for s in sector_list):
                    continue
        if min_capex is not None and row["capex_max"] is not None and row["capex_max"] < min_capex:
            continue
        if max_capex is not None and row["capex_min"] is not None and row["capex_min"] > max_capex:
            continue
        seed_value = row["seed_month_value"]
        items.append(
            {
                "hs_code": row["hs_code"],
                "title": row["title"],
                "sectors": row["sectors"],
                "capex_min": row["capex_min"],
                "capex_max": row["capex_max"],
                "last_12m_value_usd": (seed_value * 12) if seed_value else None,
                "reduction_pct": None,
                "opportunity_score": None,
                "last_updated": None,
            }
        )

    key = sort.lower()
    if key == "value":
        items.sort(key=lambda x: (x["last_12m_value_usd"] or 0), reverse=True)
    elif key == "progress":
        items.sort(key=lambda x: (x["reduction_pct"] or 0), reverse=True)
    else:
        items.sort(key=lambda x: (x["opportunity_score"] or 0), reverse=True)
    items = items[:limit]
    return {"items": items, "count": len(items), "source": CSV_SOURCE, "last_updated": None}


@app.get("/api/products")
def list_products(
    sectors: Optional[str] = Query(default=None, description="Comma-separated sectors"),
    combine: str = Query(default="OR"),
    min_capex: Optional[float] = Query(default=None, ge=0),
    max_capex: Optional[float] = Query(default=None, ge=0),
    sort: str = Query(default="opportunity"),
    limit: int = Query(default=100, ge=1, le=200),
) -> Dict[str, Any]:
    if not _database_available():
        return _products_from_csv(
            sectors=sectors,
            combine=combine,
            min_capex=min_capex,
            max_capex=max_capex,
            sort=sort,
            limit=limit,
        )

    combine = combine.upper()
    if combine not in {"AND", "OR"}:
        combine = "OR"
    sort_map = {
        "opportunity": "COALESCE(ip.opportunity_score, 0) DESC",
        "progress": "COALESCE(ip.reduction_pct, 0) DESC",
        "value": "COALESCE(ip.current_12m_usd, 0) DESC",
    }
    order_clause = sort_map.get(sort.lower(), sort_map["opportunity"])

    query_parts = [
        "SELECT p.hs_code, p.title, p.sectors, p.capex_min, p.capex_max,",
        "       ip.current_12m_usd, ip.reduction_pct, ip.opportunity_score, ip.last_updated",
        "FROM products p",
        "LEFT JOIN import_progress ip ON ip.hs_code = p.hs_code",
    ]
    params: List[Any] = []
    conditions: List[str] = []

    if sectors:
        sector_list = [s.strip() for s in sectors.split(",") if s.strip()]
        if sector_list:
            if combine == "AND":
                conditions.append("p.sectors @> %s")
            else:
                conditions.append("p.sectors && %s")
            params.append(sector_list)
    if min_capex is not None:
        conditions.append("(p.capex_max IS NULL OR p.capex_max >= %s)")
        params.append(min_capex)
    if max_capex is not None:
        conditions.append("(p.capex_min IS NULL OR p.capex_min <= %s)")
        params.append(max_capex)

    if conditions:
        query_parts.append("WHERE " + " AND ".join(conditions))
    query_parts.append(f"ORDER BY {order_clause}")
    query_parts.append("LIMIT %s")
    params.append(limit)

    with db.connect() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("\n".join(query_parts), params)
            rows = cur.fetchall()

    items: List[Dict[str, Any]] = []
    last_updated: Optional[str] = None
    for row in rows:
        item = {
            "hs_code": row["hs_code"],
            "title": row["title"],
            "sectors": row.get("sectors") or [],
            "capex_min": float(row["capex_min"]) if row.get("capex_min") is not None else None,
            "capex_max": float(row["capex_max"]) if row.get("capex_max") is not None else None,
            "last_12m_value_usd": float(row["current_12m_usd"]) if row.get("current_12m_usd") is not None else None,
            "reduction_pct": float(row["reduction_pct"]) if row.get("reduction_pct") is not None else None,
            "opportunity_score": float(row["opportunity_score"]) if row.get("opportunity_score") is not None else None,
            "last_updated": row["last_updated"].isoformat() if row.get("last_updated") else None,
        }
        if item["last_updated"]:
            last_updated = max(last_updated or item["last_updated"], item["last_updated"])
        items.append(item)

    return {
        "items": items,
        "count": len(items),
        "source": DEFAULT_SOURCE,
        "last_updated": last_updated,
    }


def _product_detail_from_csv(hs_code: str) -> Dict[str, Any]:
    rows = [_parse_csv_row(row) for row in _read_seed_rows()]
    row = next((item for item in rows if item["hs_code"] == hs_code), None)
    if not row:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Product not found")
    seed_value = row["seed_month_value"] or 0
    product_card = ProductCard(
        hs_code=row["hs_code"],
        title=row["title"],
        sectors=row["sectors"],
        capex_min=row["capex_min"],
        capex_max=row["capex_max"],
        last_12m_value_usd=seed_value * 12 if seed_value else None,
        reduction_pct=None,
        opportunity_score=None,
        last_updated=None,
    )
    timeseries = [
        {
            "year": 2024,
            "month": month,
            "value_usd": seed_value,
            "qty": None,
            "partner_country": row["top_country"],
        }
        for month in range(1, 13)
    ]
    return {
        "product": product_card.model_dump(),
        "description": row["description"],
        "baseline_period": None,
        "timeseries": timeseries,
        "partners": [
            {"partner_country": row["top_country"], "value_usd": seed_value * 12}
        ],
        "progress": {
            "reduction_abs": None,
            "reduction_pct": None,
            "hhi_current": None,
            "hhi_baseline": None,
            "concentration_shift": None,
            "opportunity_score": None,
            "baseline_12m_usd": None,
        },
        "source": CSV_SOURCE,
        "last_updated": None,
    }


@app.get("/api/products/{hs_code}")
def product_detail(hs_code: str) -> Dict[str, Any]:
    if not _database_available():
        return _product_detail_from_csv(hs_code)

    with db.connect() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT p.hs_code, p.title, p.description, p.sectors, p.capex_min, p.capex_max,
                       ip.current_12m_usd, ip.reduction_pct, ip.opportunity_score, ip.last_updated,
                       ip.reduction_abs, ip.hhi_current, ip.hhi_baseline, ip.concentration_shift,
                       b.baseline_period, b.baseline_12m_usd, b.updated_at
                FROM products p
                LEFT JOIN import_progress ip ON ip.hs_code = p.hs_code
                LEFT JOIN baseline_imports b ON b.hs_code = p.hs_code
                WHERE p.hs_code = %s
                """,
                (hs_code,),
            )
            row = cur.fetchone()
        if not row:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Product not found")

        timeseries = db.fetch_last_36m(conn, hs_code)
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT partner_country, SUM(value_usd) AS total
                FROM monthly_imports
                WHERE hs_code = %s
                GROUP BY partner_country
                ORDER BY total DESC
                LIMIT 5
                """,
                (hs_code,),
            )
            partners = cur.fetchall()

    product_card = ProductCard(
        hs_code=row["hs_code"],
        title=row["title"],
        sectors=row.get("sectors") or [],
        capex_min=float(row["capex_min"]) if row.get("capex_min") is not None else None,
        capex_max=float(row["capex_max"]) if row.get("capex_max") is not None else None,
        last_12m_value_usd=float(row["current_12m_usd"]) if row.get("current_12m_usd") is not None else None,
        reduction_pct=float(row["reduction_pct"]) if row.get("reduction_pct") is not None else None,
        opportunity_score=float(row["opportunity_score"]) if row.get("opportunity_score") is not None else None,
        last_updated=row["last_updated"].isoformat() if row.get("last_updated") else None,
    )

    timeseries_payload = [
        {
            "year": entry["year"],
            "month": entry["month"],
            "value_usd": float(entry["value_usd"]) if entry.get("value_usd") is not None else None,
            "qty": float(entry["qty"]) if entry.get("qty") is not None else None,
            "partner_country": entry.get("partner_country"),
        }
        for entry in timeseries
    ]

    partner_payload = [
        {
            "partner_country": item.get("partner_country"),
            "value_usd": float(item.get("total") or 0),
        }
        for item in partners
    ]

    progress_payload = {
        "reduction_abs": float(row["reduction_abs"]) if row.get("reduction_abs") is not None else None,
        "reduction_pct": float(row["reduction_pct"]) if row.get("reduction_pct") is not None else None,
        "hhi_current": float(row["hhi_current"]) if row.get("hhi_current") is not None else None,
        "hhi_baseline": float(row["hhi_baseline"]) if row.get("hhi_baseline") is not None else None,
        "concentration_shift": float(row["concentration_shift"]) if row.get("concentration_shift") is not None else None,
        "opportunity_score": product_card.opportunity_score,
        "baseline_12m_usd": float(row["baseline_12m_usd"]) if row.get("baseline_12m_usd") is not None else None,
    }

    return {
        "product": product_card.model_dump(),
        "description": row.get("description"),
        "baseline_period": row.get("baseline_period"),
        "timeseries": timeseries_payload,
        "partners": partner_payload,
        "progress": progress_payload,
        "source": DEFAULT_SOURCE,
        "last_updated": product_card.last_updated or (row.get("updated_at").isoformat() if row.get("updated_at") else None),
    }


@app.get("/api/leaderboard")
def leaderboard(
    metric: str = Query(default="opportunity"),
    limit: int = Query(default=50, ge=1, le=200),
) -> Dict[str, Any]:
    if not _database_available():
        data = _products_from_csv(
            sectors=None,
            combine="OR",
            min_capex=None,
            max_capex=None,
            sort=metric,
            limit=limit,
        )
        return {"items": data["items"], "source": CSV_SOURCE, "last_updated": data.get("last_updated")}

    sort_map = {
        "opportunity": "COALESCE(ip.opportunity_score, 0) DESC",
        "progress": "COALESCE(ip.reduction_pct, 0) DESC",
        "value": "COALESCE(ip.current_12m_usd, 0) DESC",
    }
    order_clause = sort_map.get(metric.lower(), sort_map["opportunity"])

    with db.connect() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                f"""
                SELECT p.hs_code, p.title, p.sectors,
                       ip.current_12m_usd, ip.reduction_pct, ip.opportunity_score, ip.last_updated
                FROM products p
                LEFT JOIN import_progress ip ON ip.hs_code = p.hs_code
                ORDER BY {order_clause}
                LIMIT %s
                """,
                (limit,),
            )
            rows = cur.fetchall()

    items = [
        {
            "hs_code": row["hs_code"],
            "title": row["title"],
            "sectors": row.get("sectors") or [],
            "last_12m_value_usd": float(row["current_12m_usd"]) if row.get("current_12m_usd") is not None else None,
            "reduction_pct": float(row["reduction_pct"]) if row.get("reduction_pct") is not None else None,
            "opportunity_score": float(row["opportunity_score"]) if row.get("opportunity_score") is not None else None,
            "last_updated": row["last_updated"].isoformat() if row.get("last_updated") else None,
        }
        for row in rows
    ]
    last_updated = None
    for item in items:
        if item["last_updated"]:
            last_updated = max(last_updated or item["last_updated"], item["last_updated"])
    return {"items": items, "source": DEFAULT_SOURCE, "last_updated": last_updated}


@app.post("/api/domestic_capability")
def upsert_domestic_capability(
    payload: DomesticCapabilityPayload,
    _: None = Depends(admin_required),
) -> Dict[str, Any]:
    if not _database_available():
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="DATABASE_URL not configured")
    with db.connect() as conn:
        record_id = db.upsert_domestic_capability(
            conn,
            hs_code=payload.hs_code,
            capex_min=payload.capex_min,
            capex_max=payload.capex_max,
            machines=payload.machines,
            skills=payload.skills,
            notes=payload.notes,
            source=payload.source,
        )
    return {
        "id": record_id,
        "hs_code": payload.hs_code,
        "source": MANUAL_SOURCE,
        "last_updated": datetime.now(timezone.utc).isoformat(),
    }


@app.get("/api/domestic_capability/{hs_code}")
def list_domestic_capability(hs_code: str) -> Dict[str, Any]:
    if not _database_available():
        return {"items": [], "source": CSV_SOURCE, "last_updated": None}
    with db.connect() as conn:
        rows = db.fetch_verified_capability(conn, hs_code)
    items: List[Dict[str, Any]] = []
    for row in rows:
        items.append(
            {
                "id": row["id"],
                "hs_code": row["hs_code"],
                "capex_min": float(row["capex_min"]) if row.get("capex_min") is not None else None,
                "capex_max": float(row["capex_max"]) if row.get("capex_max") is not None else None,
                "machines": row.get("machines"),
                "skills": row.get("skills"),
                "notes": row.get("notes"),
                "source": row.get("source"),
                "verified": row.get("verified", False),
            }
        )
    last_updated = datetime.now(timezone.utc).isoformat() if items else None
    return {"items": items, "source": DEFAULT_SOURCE, "last_updated": last_updated}
