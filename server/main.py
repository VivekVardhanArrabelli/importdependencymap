"""FastAPI entrypoint for Build for India."""
from __future__ import annotations

import csv
import logging
import os
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from dotenv import load_dotenv
from fastapi import Depends, FastAPI, HTTPException, Query, Request, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles
from psycopg2.extras import RealDictCursor

from . import db, jobs
from .etl import comtrade, dgcis, normalize
from dateutil.relativedelta import relativedelta
from .schemas import DomesticCapabilityPayload, ProductCard
from urllib.parse import urlencode

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

BASE_DIR = Path(__file__).resolve().parent.parent
CLIENT_DIR = BASE_DIR / "client"


try:
    app.mount("/static", StaticFiles(directory=CLIENT_DIR), name="client")
except RuntimeError as exc:  # pragma: no cover - optional dependency missing
    if "aiofiles" in str(exc):
        LOGGER.warning("Static files mount skipped: %s", exc)
    else:
        raise

SEED_CSV_PATH = BASE_DIR / "data" / "top100_hs.csv"

DEFAULT_SOURCE = "database"
ADMIN_SOURCE = "admin"
MANUAL_SOURCE = "manual"


def admin_guard(request: Request) -> None:
    """Dependency to guard admin endpoints (supports header or ?key=)."""
    admin_key = (os.getenv("ADMIN_KEY") or "").strip()
    if not admin_key:
        LOGGER.warning("ADMIN_KEY not configured; denying admin access")
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Unauthorized")

    auth_header = request.headers.get("authorization") or request.headers.get("Authorization")
    x_admin_key = request.headers.get("x-admin-key") or request.headers.get("X-Admin-Key")
    query_key = request.query_params.get("key")

    token: Optional[str] = None
    if isinstance(auth_header, str) and auth_header.startswith("Bearer "):
        token = auth_header.split(" ", 1)[1]
    elif isinstance(x_admin_key, str):
        token = x_admin_key
    elif isinstance(query_key, str):
        token = query_key

    if not token or token.strip() != admin_key:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Unauthorized")


admin_required = admin_guard

def _verify_admin(request: Request) -> None:
    admin_guard(request)


def _require_database_url() -> str:
    url = os.getenv("DATABASE_URL")
    if not url:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="DATABASE_URL not configured")
    return url
def _read_seed_rows() -> List[Dict[str, str]]:
    """Load curated seed rows; invoked only by the admin seed endpoint."""

    if not SEED_CSV_PATH.exists():
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Seed CSV not found at {SEED_CSV_PATH}",
        )
    with SEED_CSV_PATH.open("r", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    if not rows:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Seed CSV is empty")
    return rows


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


def _env_enabled(name: str, *, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() not in {"0", "false", "no", "off"}


def _sync_catalog_after_startup() -> None:
    try:
        catalogue = comtrade.fetch_product_catalog(
            timeout_seconds=12,
            max_retries=2,
        )
        with db.connect() as conn:
            synced = comtrade.sync_product_catalog(conn, catalogue)
        LOGGER.info("Comtrade HS6 catalogue ready: %s products", synced)
    except Exception as exc:  # upstream catalogue is best effort on startup
        LOGGER.warning("Comtrade catalogue background sync skipped: %s", exc)


@app.on_event("startup")
def ensure_schema() -> None:
    try:
        _require_database_url()
        with db.connect() as conn:
            db.ensure_extensions(conn)
            db.init_db(conn)
    except Exception as exc:  # pragma: no cover - best effort
        LOGGER.warning("Database init skipped: %s", exc)
        return

    if not _env_enabled("COMTRADE_SYNC_CATALOG_ON_STARTUP", default=True):
        return

    threading.Thread(
        target=_sync_catalog_after_startup,
        name="comtrade-catalog-sync",
        daemon=True,
    ).start()


@app.get("/", include_in_schema=False)
def serve_index():
    index_path = CLIENT_DIR / "index.html"
    if index_path.exists():
        return FileResponse(index_path)
    return HTMLResponse("<h1>Build for India</h1><p>Client not found.</p>")


@app.get("/health")
def health() -> Dict[str, bool]:
    return {"ok": True}


@app.post("/admin/seed")
def seed_database(request: Request) -> Dict[str, Any]:
    _verify_admin(request)
    rows = [_parse_csv_row(row) for row in _read_seed_rows()]
    _require_database_url()

    now_iso = datetime.now(timezone.utc).isoformat()
    try:
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
                        value_inr=None,
                        fx_rate=None,
                        qty=None,
                        partner=row["top_country"],
                        source="seed",
                        is_aggregate=False,
                    )
                    monthly_count += 1

            baseline_summary = jobs.recompute_baseline(conn)
            progress_summary = jobs.recompute_progress(conn)

    except Exception as exc:
        LOGGER.exception("Error during seed_database execution")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Seeding failed: {str(exc)}")

    return {
        "seeded": True,
        "products": product_count,
        "monthly_rows": monthly_count,
        "baseline": baseline_summary,
        "progress": progress_summary,
        "source": ADMIN_SOURCE,
        "last_updated": now_iso,
    }


def _parse_period(period: str) -> datetime:
    try:
        return datetime.strptime(period, "%Y-%m")
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Invalid period: {period}") from exc


def _refresh_comtrade(
    *,
    from_period: str,
    to_period: str,
    sync_catalog: bool = True,
    recompute: bool = True,
) -> Dict[str, Any]:
    """Persist the catalogue first, then atomically refresh facts and metrics.

    The separate catalogue transaction ensures all HS6 products remain
    discoverable even if the upstream fact refresh subsequently fails.
    """

    if sync_catalog:
        catalogue = comtrade.fetch_product_catalog()
        with db.connect() as conn:
            catalog_count = comtrade.sync_product_catalog(conn, catalogue)
    else:
        with db.connect() as conn:
            stored = db.fetch_product_catalog(conn)
        catalogue = [
            comtrade.CatalogProduct(
                hs_code=str(item["hs_code"]),
                title=str(item.get("title") or f"HS {item['hs_code']}"),
                description=str(item.get("description") or item.get("title") or ""),
                sectors=list(item.get("sectors") or []),
            )
            for item in stored
        ]
        if len(catalogue) < 1_000:
            raise comtrade.ComtradeError(
                "Stored HS6 catalogue is incomplete; synchronize the catalogue first"
            )
        catalog_count = len(catalogue)

    with db.connect() as conn:
        summary = comtrade.run(
            conn,
            from_period=from_period,
            to_period=to_period,
            catalog_products=catalogue,
            sync_catalog=False,
        )
        baseline_summary = jobs.recompute_baseline(conn) if recompute else None
        progress_summary = jobs.recompute_progress(conn) if recompute else None

    summary.update(
        {
            "catalog_products": catalog_count,
            "baseline": baseline_summary,
            "progress": progress_summary,
            "last_updated": datetime.now(timezone.utc).isoformat(),
        }
    )
    return summary


@app.post("/admin/etl/comtrade")
def trigger_comtrade(
    request: Request,
    from_period: str = Query(..., alias="from", description="YYYY-MM inclusive start"),
    to_period: str = Query(..., alias="to", description="YYYY-MM inclusive end"),
    sync_catalog: bool = Query(
        default=True,
        description="Refresh the official HS6 catalogue before loading facts",
    ),
    recompute: bool = Query(
        default=True,
        description="Recompute all analytics after loading facts",
    ),
) -> Dict[str, Any]:
    _verify_admin(request)
    _require_database_url()

    start_dt = _parse_period(from_period)
    end_dt = _parse_period(to_period)
    if start_dt > end_dt:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="from must be <= to")

    return _refresh_comtrade(
        from_period=start_dt.strftime("%Y-%m"),
        to_period=end_dt.strftime("%Y-%m"),
        sync_catalog=sync_catalog,
        recompute=recompute,
    )


@app.post("/admin/etl/comtrade/catalog")
def trigger_comtrade_catalog(request: Request) -> Dict[str, Any]:
    _verify_admin(request)
    _require_database_url()
    catalogue = comtrade.fetch_product_catalog()
    with db.connect() as conn:
        synced = comtrade.sync_product_catalog(conn, catalogue)
    return {
        "catalog_products": synced,
        "source": "comtrade_reference",
        "last_updated": datetime.now(timezone.utc).isoformat(),
    }


@app.get("/admin/etl/comtrade/availability")
def comtrade_availability(request: Request) -> Dict[str, Any]:
    _verify_admin(request)
    result = comtrade.latest_available_period()
    return {
        **result,
        "source": "comtrade",
        "last_updated": datetime.now(timezone.utc).isoformat(),
    }


@app.post("/admin/etl/dgcis")
def trigger_dgcis(
    request: Request,
    file_path: Optional[str] = Query(
        default=None,
        description="Path to DGCI&S CSV export (defaults to data/dgcis_latest.csv)",
    ),
) -> Dict[str, Any]:
    _verify_admin(request)
    _require_database_url()
    source = Path(file_path or os.getenv("DGCIS_DEFAULT_PATH", "data/dgcis_latest.csv"))
    if not source.exists():
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"DGCI&S file not found at {source}",
        )

    with db.connect() as conn:
        summary = dgcis.run(conn, source=source)
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
def trigger_recompute(request: Request) -> Dict[str, Any]:
    _verify_admin(request)
    _require_database_url()
    with db.connect() as conn:
        baseline_summary = jobs.recompute_baseline(conn)
        progress_summary = jobs.recompute_progress(conn)
    return {
        "baseline": baseline_summary,
        "progress": progress_summary,
        "source": ADMIN_SOURCE,
        "last_updated": datetime.now(timezone.utc).isoformat(),
    }


@app.get("/admin/diag")
def admin_diagnostics(request: Request) -> Dict[str, Any]:
    _verify_admin(request)
    """Return environment, filesystem, and DB diagnostics for troubleshooting."""
    # Environment presence (no secrets leaked)
    env = {
        "DATABASE_URL": bool(os.getenv("DATABASE_URL")),
        "COMTRADE_BASE": bool(os.getenv("COMTRADE_BASE")),
        "COMTRADE_PATH": bool(os.getenv("COMTRADE_PATH")),
        "COMTRADE_FLOW": bool(os.getenv("COMTRADE_FLOW")),
        "COMTRADE_REPORTER": bool(os.getenv("COMTRADE_REPORTER")),
        "COMTRADE_FREQ": bool(os.getenv("COMTRADE_FREQ")),
        "COMTRADE_KEY": bool(os.getenv("COMTRADE_KEY")),
    }

    # Filesystem checks
    seed_exists = SEED_CSV_PATH.exists()
    seed_info: Dict[str, Any] = {
        "path": str(SEED_CSV_PATH),
        "exists": seed_exists,
        "size_bytes": SEED_CSV_PATH.stat().st_size if seed_exists else None,
        "rows": None,
    }
    if seed_exists:
        try:
            with SEED_CSV_PATH.open("r", encoding="utf-8") as handle:
                rows = list(csv.DictReader(handle))
            seed_info["rows"] = len(rows)
        except Exception as exc:  # pragma: no cover - diagnostics only
            seed_info["error"] = str(exc)

    # Resolve Comtrade endpoint (best effort)
    try:
        resolved_endpoint = comtrade._resolve_endpoint()  # type: ignore[attr-defined]
    except Exception as exc:  # pragma: no cover - diagnostics only
        resolved_endpoint = f"error: {exc}"
    try:
        resolved_reporter = comtrade._normalize_reporter_code(None)  # type: ignore[attr-defined]
    except Exception as exc:  # pragma: no cover - diagnostics only
        resolved_reporter = f"error: {exc}"

    # DB checks
    db_ok = False
    counts: Dict[str, Any] = {
        "products": None,
        "monthly_imports": None,
        "aggregate_monthly_imports": None,
        "latest_period": None,
    }
    try:
        _require_database_url()
        with db.connect() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("SELECT COUNT(*) AS c FROM products")
                counts["products"] = int(cur.fetchone()["c"])  # type: ignore[index]
                cur.execute("SELECT COUNT(*) AS c FROM monthly_imports")
                counts["monthly_imports"] = int(cur.fetchone()["c"])  # type: ignore[index]
                cur.execute(
                    "SELECT COUNT(*) AS c FROM monthly_imports WHERE is_aggregate = true"
                )
                counts["aggregate_monthly_imports"] = int(cur.fetchone()["c"])  # type: ignore[index]
                latest = db.latest_year_month(conn)
                counts["latest_period"] = (
                    f"{latest[0]:04d}-{latest[1]:02d}" if latest else None
                )
        db_ok = True
    except Exception as exc:  # pragma: no cover - diagnostics only
        counts["error"] = str(exc)

    return {
        "env": env,
        "seed_csv": seed_info,
        "comtrade_endpoint": resolved_endpoint,
        "comtrade_reporter": resolved_reporter,
        "db_ok": db_ok,
        "counts": counts,
        "source": ADMIN_SOURCE,
        "last_updated": datetime.now(timezone.utc).isoformat(),
    }


@app.post("/admin/etl/nightly")
def trigger_nightly(request: Request) -> Dict[str, Any]:
    """Convenience endpoint for Railway HTTP Cron.

    Requests enough history to retain a 12-month analytics window even when
    India's latest Comtrade release lags the current calendar month.
    """
    _verify_admin(request)
    _require_database_url()
    if not (os.getenv("COMTRADE_KEY") or "").strip():
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=(
                "A single-request nightly refresh requires COMTRADE_KEY. "
                "Use the partitioned GitHub workflow for anonymous preview mode."
            ),
        )

    # Compute the month range in UTC
    today_utc = datetime.now(timezone.utc)
    lookback_months = max(int(os.getenv("COMTRADE_LOOKBACK_MONTHS", "18")), 13)
    start_month = (
        today_utc.replace(day=1) - relativedelta(months=lookback_months)
    ).strftime("%Y-%m")
    end_month = (today_utc.replace(day=1) - relativedelta(months=1)).strftime("%Y-%m")

    summary = _refresh_comtrade(from_period=start_month, to_period=end_month)
    summary.update({"from": start_month, "to": end_month})
    return summary


@app.get("/api/products")
def list_products(
    sectors: Optional[str] = Query(default=None, description="Comma-separated sectors"),
    combine: str = Query(default="OR"),
    min_capex: Optional[float] = Query(default=None, ge=0),
    max_capex: Optional[float] = Query(default=None, ge=0),
    sort: str = Query(default="opportunity"),
    limit: int = Query(default=100, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
    q: Optional[str] = Query(default=None, alias="q", description="Search by HS code or title"),
) -> Dict[str, Any]:
    _require_database_url()

    combine = combine.upper()
    if combine not in {"AND", "OR"}:
        combine = "OR"
    sort_map = {
        "opportunity": "COALESCE(ip.opportunity_score, 0) DESC, p.hs_code ASC",
        "progress": "COALESCE(ip.reduction_pct, 0) DESC, p.hs_code ASC",
        "value": "COALESCE(ip.current_12m_usd, 0) DESC, p.hs_code ASC",
        "catalog": "p.hs_code ASC",
    }
    order_clause = sort_map.get(sort.lower(), sort_map["opportunity"])

    from_parts = [
        "FROM products p",
        "LEFT JOIN import_progress ip ON ip.hs_code = p.hs_code",
    ]
    query_parts = [
        "SELECT p.hs_code, p.title, p.description, p.sectors, p.capex_min, p.capex_max,",
        "       ip.current_12m_usd, ip.reduction_pct, ip.opportunity_score, ip.last_updated",
        *from_parts,
    ]
    params: List[Any] = []
    conditions: List[str] = ["p.catalog_active = true"]

    search_term = (q or "").strip()
    if search_term:
        like = f"%{search_term}%"
        conditions.append("(p.title ILIKE %s OR p.description ILIKE %s OR p.hs_code ILIKE %s)")
        params.extend([like, like, like])
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
    query_parts.append("OFFSET %s")
    params.append(offset)

    with db.connect() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("\n".join(query_parts), params)
            rows = cur.fetchall()
            count_query = ["SELECT COUNT(*) AS total", *from_parts]
            count_params = params[:-2]
            if conditions:
                count_query.append("WHERE " + " AND ".join(conditions))
            cur.execute("\n".join(count_query), count_params)
            total = int(cur.fetchone()["total"])
        data_status = db.latest_data_status(conn)
        latest = (
            (int(data_status["year"]), int(data_status["month"]))
            if data_status
            else None
        )

    items: List[Dict[str, Any]] = []
    last_updated: Optional[str] = None
    for row in rows:
        item = {
            "hs_code": row["hs_code"],
            "title": row["title"],
            "description": row.get("description"),
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
        "total": total,
        "offset": offset,
        "limit": limit,
        "source": data_status.get("source", DEFAULT_SOURCE) if data_status else DEFAULT_SOURCE,
        "last_updated": last_updated,
        "latest_period": f"{latest[0]:04d}-{latest[1]:02d}" if latest else None,
    }


@app.get("/api/products/{hs_code}")
def product_detail(hs_code: str) -> Dict[str, Any]:
    _require_database_url()

    with db.connect() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT p.hs_code, p.title, p.description, p.sectors, p.capex_min, p.capex_max,
                       ip.current_12m_usd, ip.reduction_pct, ip.opportunity_score, ip.last_updated,
                       ip.reduction_abs, ip.hhi_current, ip.hhi_baseline, ip.concentration_shift,
                       b.baseline_period, b.baseline_12m_usd,
                       b.updated_at AS baseline_updated_at,
                       p.updated_at AS product_updated_at
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
                WITH latest_partner_period AS (
                    SELECT year, month
                    FROM monthly_imports
                    WHERE hs_code = %s
                      AND is_aggregate = false
                      AND source IN ('dgcis', 'comtrade_partner')
                    ORDER BY year DESC, month DESC
                    LIMIT 1
                )
                SELECT mi.partner_country, SUM(mi.value_usd) AS total,
                       SUM(SUM(mi.value_usd)) OVER () AS grand_total,
                       mi.year, mi.month, MIN(mi.source) AS source
                FROM monthly_imports mi
                JOIN latest_partner_period latest
                  ON latest.year = mi.year AND latest.month = mi.month
                WHERE mi.hs_code = %s
                  AND mi.is_aggregate = false
                  AND mi.source IN ('dgcis', 'comtrade_partner')
                GROUP BY mi.partner_country, mi.year, mi.month
                ORDER BY total DESC
                LIMIT 10
                """,
                (hs_code, hs_code),
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
            "value_inr": float(entry["value_inr"]) if entry.get("value_inr") is not None else None,
            "fx_rate": float(entry["fx_rate"]) if entry.get("fx_rate") is not None else None,
            "qty": float(entry["qty"]) if entry.get("qty") is not None else None,
            "partner_country": entry.get("partner_country"),
        }
        for entry in timeseries
    ]

    partner_payload = [
        {
            "partner_country": item.get("partner_country"),
            "value_usd": float(item.get("total") or 0),
            "share": (
                float(item.get("total") or 0) / float(item["grand_total"])
                if item.get("grand_total") not in (None, 0)
                else None
            ),
        }
        for item in partners
    ]
    partner_period: Optional[str] = None
    partner_source: Optional[str] = None
    snapshot_hhi: Optional[float] = None
    if partner_payload:
        partner_period = f"{int(partners[0]['year']):04d}{int(partners[0]['month']):02d}"
        partner_source = str(partners[0].get("source") or "database")
    elif timeseries_payload:
        partner_period = (
            f"{timeseries_payload[-1]['year']:04d}"
            f"{timeseries_payload[-1]['month']:02d}"
        )
        try:
            snapshot = comtrade.fetch_partner_snapshot(hs_code, partner_period)
            partner_payload = list(snapshot.get("partners") or [])[:10]
            snapshot_hhi = (
                float(snapshot["hhi"]) if snapshot.get("hhi") is not None else None
            )
            partner_source = str(snapshot.get("source") or "comtrade")
        except Exception as exc:  # upstream detail is best-effort
            LOGGER.warning(
                "Partner snapshot unavailable for %s %s: %s",
                hs_code,
                partner_period,
                exc,
            )

    progress_payload = {
        "reduction_abs": float(row["reduction_abs"]) if row.get("reduction_abs") is not None else None,
        "reduction_pct": float(row["reduction_pct"]) if row.get("reduction_pct") is not None else None,
        "hhi_current": (
            float(row["hhi_current"])
            if row.get("hhi_current") is not None
            else snapshot_hhi
        ),
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
        "partner_period": partner_period,
        "partner_source": partner_source,
        "progress": progress_payload,
        "source": DEFAULT_SOURCE,
        "last_updated": product_card.last_updated
        or (
            row["baseline_updated_at"].isoformat()
            if row.get("baseline_updated_at")
            else (
                row["product_updated_at"].isoformat()
                if row.get("product_updated_at")
                else None
            )
        ),
        "latest_period": (
            f"{timeseries_payload[-1]['year']:04d}-{timeseries_payload[-1]['month']:02d}"
            if timeseries_payload
            else None
        ),
    }


@app.get("/api/leaderboard")
def leaderboard(
    metric: str = Query(default="opportunity"),
    limit: int = Query(default=50, ge=1, le=200),
) -> Dict[str, Any]:
    _require_database_url()

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
    _require_database_url()
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
    _require_database_url()
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


@app.get("/admin/etl/comtrade/probe")
def comtrade_probe(
    request: Request,
    period: str = Query(..., description="YYYY-MM or YYYYMM"),
    freq: str = Query(default="M", description="M=monthly, A=annual"),
    cmd: str = Query(default="TOTAL", description="HS code or TOTAL"),
    reporter: Optional[str] = Query(default=None, description="Override reporterCode (India is 699)"),
    flow: Optional[str] = Query(default=None, description="Override flowCode (M=Import, X=Export)"),
    partner: Optional[str] = Query(default=None, description="Optional partnerCode (0 for World)"),
) -> Dict[str, Any]:
    _verify_admin(request)
    # Build params following Comtrade v1 /data
    reporter_code = comtrade._normalize_reporter_code(reporter)  # type: ignore[attr-defined]
    flow_code = comtrade._normalize_flow_code(flow)  # type: ignore[attr-defined]
    freq_code = comtrade._resolve_freq_code(freq)  # type: ignore[attr-defined]
    type_code = comtrade._resolve_type_code(None)  # type: ignore[attr-defined]
    classification = comtrade._resolve_classification()  # type: ignore[attr-defined]
    path_segments = comtrade._build_path_segments(type_code, freq_code, classification)  # type: ignore[attr-defined]

    # Normalize period to YYYYMM or YYYY
    p = period.strip()
    if "-" in p and len(p) >= 7:
        p = f"{p[:4]}{p[5:7]}"

    params = {
        "reporterCode": reporter_code,
        "flowCode": flow_code,
        "period": p,
        "cmdCode": cmd,
        "includeDesc": "true",
        "breakdownMode": "classic",
        "partner2Code": "0",
        "customsCode": "C00",
        "motCode": "0",
        "maxRecords": "500",
    }
    if partner:
        params["partnerCode"] = partner

    # Mask key in debug URL, but include it in real request via comtrade._request
    endpoint = comtrade._resolve_endpoint(*path_segments)  # type: ignore[attr-defined]
    debug_params = dict(params)
    if os.getenv("COMTRADE_KEY"):
        debug_params["subscription-key"] = "***"
    debug_url = f"{endpoint}?{urlencode(debug_params)}"

    try:
        payload = comtrade._request(dict(params), path_parts=path_segments)  # type: ignore[attr-defined]
    except Exception as exc:
        return {
            "endpoint": endpoint,
            "debug_url": debug_url,
            "params": params,
            "has_key": bool(os.getenv("COMTRADE_KEY")),
            "error": str(exc),
            "source": ADMIN_SOURCE,
            "last_updated": datetime.now(timezone.utc).isoformat(),
        }

    data = payload.get("data") if isinstance(payload, dict) else None
    count = payload.get("count") if isinstance(payload, dict) else None
    if count is None and isinstance(data, list):
        count = len(data)
    response = {
        "endpoint": endpoint,
        "debug_url": debug_url,
        "params": params,
        "has_key": bool(os.getenv("COMTRADE_KEY")),
        "statusCode": payload.get("statusCode") if isinstance(payload, dict) else None,
        "count": count,
        "validation": payload.get("validation") if isinstance(payload, dict) else None,
        "error": payload.get("error") if isinstance(payload, dict) else None,
        "data_head": (data[:3] if isinstance(data, list) else None),
        "source": ADMIN_SOURCE,
        "last_updated": datetime.now(timezone.utc).isoformat(),
    }
    return response
