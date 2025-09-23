"""FastAPI entrypoint for Build for India."""
from __future__ import annotations

import csv
import logging
import os
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from dotenv import load_dotenv
from fastapi import Depends, FastAPI, Header, HTTPException, Query, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, FileResponse
from psycopg2.extras import Json, RealDictCursor

from . import db, jobs
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
SEED_SOURCE = "seed"
MANUAL_SOURCE = "manual"


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


@app.get("/", include_in_schema=False)
def serve_index():
    """Serve the static client index if present.

    This avoids the need for a separate static server/proxy so the
    website and API can share the same origin.
    """
    index_path = Path("client/index.html")
    if index_path.exists():
        return FileResponse(index_path)
    return HTMLResponse("<h1>Build for India</h1><p>Client not found.</p>")


@app.on_event("startup")
def ensure_schema() -> None:
    try:
        with db.connect() as conn:
            db.init_db(conn)
    except Exception as exc:  # pragma: no cover - best effort on startup
        LOGGER.warning("Database init skipped: %s", exc)


def _ensure_seed_file() -> None:
    DATA_PATH.parent.mkdir(parents=True, exist_ok=True)
    if DATA_PATH.exists():
        return
    DATA_PATH.write_text(
        """hs_code,title,description,sectors,capex_min,capex_max,seed_month_value,top_country
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
""",
        encoding="utf-8",
    )
    LOGGER.info("Seed CSV created at %s", DATA_PATH)


def _parse_sectors(raw: str) -> List[str]:
    if not raw:
        return []
    cleaned = raw.strip()
    if cleaned.startswith("{") and cleaned.endswith("}"):
        cleaned = cleaned[1:-1]
    return [segment.strip() for segment in cleaned.split(",") if segment.strip()]


def _csv_rows() -> List[Dict[str, str]]:
    _ensure_seed_file()
    with DATA_PATH.open("r", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        return list(reader)


@app.get("/health")
def health() -> Dict[str, bool]:
    return {"ok": True}


@app.post("/admin/seed")
def seed_database(_: None = Depends(admin_required)) -> Dict[str, Any]:
    rows = _csv_rows()
    if not rows:
        return {"seeded": False, "items": 0}

    now = datetime.utcnow().isoformat()
    with db.connect() as conn:
        db.init_db(conn)
        product_count = 0
        monthly_count = 0
        for row in rows:
            hs_code = row["hs_code"].strip()
            sectors = _parse_sectors(row.get("sectors", ""))
            capex_min = row.get("capex_min")
            capex_max = row.get("capex_max")
            capex_min_val = float(capex_min) if capex_min not in (None, "") else None
            capex_max_val = float(capex_max) if capex_max not in (None, "") else None
            db.upsert_product(
                conn,
                hs_code=hs_code,
                title=row.get("title", "").strip(),
                description=row.get("description", "").strip(),
                sectors=sectors,
                capex_min=capex_min_val,
                capex_max=capex_max_val,
            )
            product_count += 1
            seed_value = float(row.get("seed_month_value") or 0)
            top_partner = row.get("top_country") or "Unknown"
            for month in range(1, 13):
                db.insert_monthly(
                    conn,
                    hs_code=hs_code,
                    year=2024,
                    month=month,
                    value_usd=seed_value,
                    qty=None,
                    partner=top_partner,
                )
                monthly_count += 1

        baseline_summary = jobs.recompute_baseline(conn)
        progress_summary = jobs.recompute_progress(conn)

    response = {
        "seeded": True,
        "products": product_count,
        "monthly_rows": monthly_count,
        "baseline": baseline_summary,
        "progress": progress_summary,
        "source": SEED_SOURCE,
        "last_updated": now,
    }
    return response


@app.post("/admin/recompute")
def trigger_recompute(_: None = Depends(admin_required)) -> Dict[str, Any]:
    with db.connect() as conn:
        baseline_summary = jobs.recompute_baseline(conn)
        progress_summary = jobs.recompute_progress(conn)
    return {
        "baseline": baseline_summary,
        "progress": progress_summary,
        "source": SEED_SOURCE,
        "last_updated": datetime.utcnow().isoformat(),
    }


@app.get("/api/products")
def list_products(
    sectors: Optional[str] = Query(default=None, description="Comma separated sectors"),
    combine: str = Query(default="OR"),
    min_capex: Optional[float] = Query(default=None, ge=0),
    max_capex: Optional[float] = Query(default=None, ge=0),
    sort: str = Query(default="opportunity"),
    limit: int = Query(default=100, ge=1, le=200),
) -> Dict[str, Any]:
    def _fallback_from_csv() -> Dict[str, Any]:
        rows = _csv_rows()
        sector_list = [s.strip().lower() for s in sectors.split(",") if s.strip()] if sectors else []
        items: List[Dict[str, Any]] = []
        for row in rows:
            row_sectors = [s.lower() for s in _parse_sectors(row.get("sectors", ""))]
            # Filter by sectors
            if sector_list:
                if combine.upper() == "AND":
                    if not all(s in row_sectors for s in sector_list):
                        continue
                else:
                    if not any(s in row_sectors for s in sector_list):
                        continue
            capex_min_val = float(row.get("capex_min") or 0) if row.get("capex_min") not in (None, "") else None
            capex_max_val = float(row.get("capex_max") or 0) if row.get("capex_max") not in (None, "") else None
            # Filter by capex
            if min_capex is not None and capex_max_val is not None and capex_max_val < min_capex:
                continue
            if max_capex is not None and capex_min_val is not None and capex_min_val > max_capex:
                continue
            seed_value = float(row.get("seed_month_value") or 0)
            items.append(
                {
                    "hs_code": row["hs_code"].strip(),
                    "title": row.get("title", "").strip(),
                    "sectors": row_sectors,
                    "capex_min": capex_min_val,
                    "capex_max": capex_max_val,
                    "last_12m_value_usd": seed_value * 12 if seed_value else None,
                    "reduction_pct": None,
                    "opportunity_score": None,
                    "last_updated": None,
                }
            )
        sort_key = sort.lower()
        if sort_key == "value":
            items.sort(key=lambda x: (x["last_12m_value_usd"] or 0), reverse=True)
        elif sort_key == "progress":
            items.sort(key=lambda x: (x["reduction_pct"] or 0), reverse=True)
        else:
            items.sort(key=lambda x: (x["opportunity_score"] or 0), reverse=True)
        items = items[:limit]
        return {"items": items, "count": len(items), "source": SEED_SOURCE, "last_updated": None}

    # If no DATABASE_URL is configured, serve from CSV immediately
    if not os.getenv("DATABASE_URL"):
        return _fallback_from_csv()

    sector_list = [s.strip() for s in sectors.split(",") if s.strip()] if sectors else []
    combine = combine.upper()
    if combine not in {"AND", "OR"}:
        combine = "OR"
    sort = sort.lower()
    sort_map = {
        "opportunity": "COALESCE(ip.opportunity_score, 0) DESC",
        "progress": "COALESCE(ip.reduction_pct, 0) DESC",
        "value": "COALESCE(ip.current_12m_usd, 0) DESC",
    }
    order_clause = sort_map.get(sort, sort_map["opportunity"])

    query = [
        "SELECT p.hs_code, p.title, p.sectors, p.capex_min, p.capex_max,",
        "       ip.current_12m_usd, ip.reduction_pct, ip.opportunity_score, ip.last_updated",
        "FROM products p",
        "LEFT JOIN import_progress ip ON ip.hs_code = p.hs_code",
    ]
    conditions: List[str] = []
    params: List[Any] = []
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
        query.append("WHERE " + " AND ".join(conditions))
    query.append(f"ORDER BY {order_clause}")
    query.append("LIMIT %s")
    params.append(limit)

    try:
        items: List[Dict[str, Any]] = []
        last_updated: Optional[str] = None
        with db.connect() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("\n".join(query), params)
                rows = cur.fetchall()
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
            "source": SEED_SOURCE,
            "last_updated": last_updated,
        }
    except Exception as exc:  # pragma: no cover - fallback for local/demo
        LOGGER.warning("Falling back to CSV for /api/products: %s", exc)
        return _fallback_from_csv()


@app.get("/api/products/{hs_code}")
def product_detail(hs_code: str) -> Dict[str, Any]:
    try:
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
            "product": product_card.dict(),
            "description": row.get("description"),
            "baseline_period": row.get("baseline_period"),
            "timeseries": timeseries_payload,
            "partners": partner_payload,
            "progress": progress_payload,
            "source": SEED_SOURCE,
            "last_updated": product_card.last_updated or (row.get("updated_at").isoformat() if row.get("updated_at") else None),
        }
    except Exception as exc:  # pragma: no cover - fallback for local/demo
        LOGGER.warning("Falling back to CSV for /api/products/{hs_code}: %s", exc)
        rows = _csv_rows()
        row = next((r for r in rows if r.get("hs_code", "").strip() == hs_code), None)
        if not row:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Product not found")
        seed_value = float(row.get("seed_month_value") or 0)
        row_sectors = _parse_sectors(row.get("sectors", ""))
        product_card = ProductCard(
            hs_code=row["hs_code"].strip(),
            title=row.get("title", "").strip(),
            sectors=row_sectors,
            capex_min=float(row.get("capex_min")) if row.get("capex_min") not in (None, "") else None,
            capex_max=float(row.get("capex_max")) if row.get("capex_max") not in (None, "") else None,
            last_12m_value_usd=seed_value * 12 if seed_value else None,
            reduction_pct=None,
            opportunity_score=None,
            last_updated=None,
        )
        # Build 12-month timeseries for 2024 as demo
        timeseries_payload = [
            {"year": 2024, "month": m, "value_usd": seed_value, "qty": None, "partner_country": row.get("top_country")}
            for m in range(1, 13)
        ]
        partner_payload = [
            {"partner_country": row.get("top_country") or "Unknown", "value_usd": seed_value * 12 if seed_value else 0}
        ]
        progress_payload = {
            "reduction_abs": None,
            "reduction_pct": None,
            "hhi_current": None,
            "hhi_baseline": None,
            "concentration_shift": None,
            "opportunity_score": None,
            "baseline_12m_usd": None,
        }
        return {
            "product": product_card.dict(),
            "description": row.get("description"),
            "baseline_period": None,
            "timeseries": timeseries_payload,
            "partners": partner_payload,
            "progress": progress_payload,
            "source": SEED_SOURCE,
            "last_updated": None,
        }


@app.get("/api/leaderboard")
def leaderboard(
    metric: str = Query(default="opportunity"),
    limit: int = Query(default=50, ge=1, le=200),
) -> Dict[str, Any]:
    def _fallback_from_csv() -> Dict[str, Any]:
        rows = _csv_rows()
        items = []
        for row in rows:
            seed_value = float(row.get("seed_month_value") or 0)
            items.append(
                {
                    "hs_code": row.get("hs_code"),
                    "title": row.get("title"),
                    "sectors": _parse_sectors(row.get("sectors", "")),
                    "last_12m_value_usd": seed_value * 12 if seed_value else None,
                    "reduction_pct": None,
                    "opportunity_score": None,
                    "last_updated": None,
                }
            )
        key = (metric or "value").lower()
        if key == "value":
            items.sort(key=lambda x: (x["last_12m_value_usd"] or 0), reverse=True)
        elif key == "progress":
            items.sort(key=lambda x: (x["reduction_pct"] or 0), reverse=True)
        else:
            items.sort(key=lambda x: (x["opportunity_score"] or 0), reverse=True)
        return {"items": items[:limit], "source": SEED_SOURCE, "last_updated": None}

    metric = metric.lower()
    sort_map = {
        "opportunity": "COALESCE(ip.opportunity_score, 0) DESC",
        "progress": "COALESCE(ip.reduction_pct, 0) DESC",
        "value": "COALESCE(ip.current_12m_usd, 0) DESC",
    }
    order_clause = sort_map.get(metric, sort_map["opportunity"])
    try:
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
        return {"items": items, "source": SEED_SOURCE, "last_updated": last_updated}
    except Exception as exc:  # pragma: no cover - fallback
        LOGGER.warning("Falling back to CSV for /api/leaderboard: %s", exc)
        return _fallback_from_csv()


@app.post("/api/domestic_capability")
def upsert_domestic_capability(
    payload: DomesticCapabilityPayload,
    _: None = Depends(admin_required),
) -> Dict[str, Any]:
    with db.connect() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                INSERT INTO domestic_capability (hs_code, capex_min, capex_max, machines, skills, notes, source, verified)
                VALUES (%s, %s, %s, %s, %s, %s, %s, false)
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
                    payload.hs_code,
                    payload.capex_min,
                    payload.capex_max,
                    Json(payload.machines) if payload.machines is not None else None,
                    Json(payload.skills) if payload.skills is not None else None,
                    payload.notes,
                    payload.source,
                ),
            )
            record = cur.fetchone()
    return {
        "id": record["id"] if record else None,
        "hs_code": payload.hs_code,
        "source": MANUAL_SOURCE,
        "last_updated": datetime.utcnow().isoformat(),
    }


@app.get("/api/domestic_capability/{hs_code}")
def list_domestic_capability(hs_code: str) -> Dict[str, Any]:
    with db.connect() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT id, hs_code, capex_min, capex_max, machines, skills, notes, source, verified
                FROM domestic_capability
                WHERE hs_code = %s AND verified = true
                ORDER BY id
                """,
                (hs_code,),
            )
            rows = cur.fetchall()
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
    last_updated = datetime.utcnow().isoformat() if items else None
    return {"items": items, "source": MANUAL_SOURCE, "last_updated": last_updated}
