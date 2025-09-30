"""ETL pipeline for UN Comtrade monthly imports."""
from __future__ import annotations

import json
import logging
import os
import time
from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional, Tuple

from urllib import error, parse, request

from .. import db, forex
from . import normalize

LOGGER = logging.getLogger(__name__)

MAX_RETRIES = 4
RETRY_STATUS = {429, 500, 502, 503, 504}

@dataclass
class Record:
    hs_code: str
    title: str
    description: str
    sectors: List[str]
    capex_min: Optional[float]
    capex_max: Optional[float]
    year: int
    month: int
    value_usd: Optional[float]
    value_inr: Optional[float] = None
    qty: Optional[float] = None
    partner_country: Optional[str] = None

def _base_url() -> str:
    base = os.getenv("COMTRADE_BASE")
    if not base:
        raise RuntimeError("COMTRADE_BASE environment variable is required")
    return base.rstrip("/")

def _resolve_endpoint() -> str:
    """Return the fully qualified /data endpoint for preview API."""
    base = _base_url()
    if "/preview" in base:
        return f"{base}/data"
    else:
        # Fallback for legacy, but prefer preview
        return f"{base}/v1/preview/data"

def _request(params: Dict[str, str]) -> Dict:
    # Add subscription key if available
    key = os.getenv("COMTRADE_KEY")
    if key:
        params["subscription-key"] = key
    
    query = parse.urlencode(params)
    url = f"{_resolve_endpoint()}?{query}"
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            req = request.Request(url)
            # Alternative: Add as header if preferred by docs
            # if key:
            #     req.add_header("Ocp-Apim-Subscription-Key", key)
            
            with request.urlopen(req, timeout=45) as resp:
                status = resp.status
                if status in RETRY_STATUS:
                    raise error.HTTPError(url, status, "retryable", hdrs=None, fp=None)
                payload = resp.read()
                return json.loads(payload.decode("utf-8"))
        except error.HTTPError as exc:
            if exc.code in RETRY_STATUS and attempt < MAX_RETRIES:
                LOGGER.warning("Comtrade HTTP %s (%s/%s); backing off", exc.code, attempt, MAX_RETRIES)
                time.sleep(min(2 ** attempt, 30))
                continue
            raise
        except error.URLError as exc:
            LOGGER.warning("Comtrade request failed (%s/%s): %s", attempt, MAX_RETRIES, exc)
            if attempt == MAX_RETRIES:
                raise
            time.sleep(min(2 ** attempt, 30))
    raise RuntimeError("Comtrade API request failed after retries")

def _extract_dataset(payload: Dict) -> List[Dict]:
    data = payload.get("data", [])
    if isinstance(data, list):
        return data
    return []

def _next_cursor(payload: Dict) -> Optional[str]:
    links = payload.get("links", {})
    if isinstance(links, dict):
        next_link = links.get("next")
        if isinstance(next_link, dict):
            next_link = next_link.get("href")
        if isinstance(next_link, str) and "?" in next_link:
            parsed = parse.urlparse(next_link)
            cursor = dict(parse.parse_qsl(parsed.query)).get("cursor")
            if cursor:
                return cursor
    return None

def _parse_dataset(dataset: Iterable[Dict]) -> List[Record]:
    records: List[Record] = []
    for row in dataset:
        hs_code = normalize.canonical_hs_code(row.get("cmdCode"))
        if not hs_code:
            continue
        period = str(row.get("period") or "")
        if len(period) != 6:
            continue
        year, month = int(period[:4]), int(period[4:6])
        title = (row.get("cmdDescE") or row.get("cmdDescription") or "").strip()
        description = (row.get("mainCategory") or "").strip()
        partner = row.get("pt3ISO") or row.get("ptTitle") or None
        value_usd = float(row.get("TradeValue", 0)) if row.get("TradeValue") is not None else None
        qty_raw = row.get("NetWeight") or row.get("primaryValue")
        qty = float(qty_raw) if qty_raw is not None else None
        sectors = normalize.infer_sectors(title, description)
        records.append(
            Record(
                hs_code=hs_code,
                title=title or f"HS {hs_code}",
                description=description,
                sectors=sectors,
                capex_min=None,
                capex_max=None,
                year=year,
                month=month,
                value_usd=normalize.ensure_usd(value_usd),
                value_inr=None,
                qty=qty,
                partner_country=partner,
            )
        )
    return records

def _build_periods(from_period: str, to_period: str) -> List[str]:
    """Generate comma-separated YYYYMM periods for preview API."""
    from_parts = [int(from_period[:4]), int(from_period[4:])]
    to_parts = [int(to_period[:4]), int(to_period[4:])]
    periods = []
    year, month = from_parts
    while year < to_parts[0] or (year == to_parts[0] and month <= to_parts[1]):
        periods.append(f"{year:04d}{month:02d}")
        month += 1
        if month > 12:
            month = 1
            year += 1
    return periods

def fetch_range(
    from_period: str,
    to_period: str,
    *,
    reporter_code: Optional[str] = None,
    flow_code: Optional[str] = None,
    frequency: Optional[str] = None,
) -> List[Record]:
    reporter_code = reporter_code or os.getenv("COMTRADE_REPORTER", "356")  # India
    flow_code = flow_code or os.getenv("COMTRADE_FLOW", "2")  # Imports
    frequency = frequency or os.getenv("COMTRADE_FREQ", "M")
    
    # Base params for preview API
    base_params = {
        "reporterCode": reporter_code,
        "flowCode": flow_code,
        "freqCode": frequency,
        "typeCode": "C",  # Customs value
        "period": ",".join(_build_periods(from_period, to_period)),
    }
    
    all_records = []
    for chapter in range(1, 100):  # HS chapters 01-99
        params = dict(base_params)
        params["cmdCode"] = f"{chapter:02d}*"  # Chapter wildcard (preview supports limited)
        
        dataset = []
        cursor = None
        while True:
            if cursor:
                params["cursor"] = cursor
            payload = _request(params)
            
            if payload.get("statusCode") == 404:
                LOGGER.debug("No data for HS chapter %02d", chapter)
                break
            
            dataset.extend(_extract_dataset(payload))
            cursor = _next_cursor(payload)
            if not cursor:
                break
        
        records = _parse_dataset(dataset)
        all_records.extend(records)
        LOGGER.info("Fetched %d records for HS chapter %02d", len(records), chapter)
    
    LOGGER.info("Total fetched %s rows from Comtrade", len(all_records))
    return all_records

def load(
    conn,
    records: Iterable[Record],
) -> Tuple[int, int]:
    products_seen: Dict[str, bool] = {}
    monthly_rows = 0
    for record in records:
        products_seen.setdefault(record.hs_code, False)
        if not products_seen[record.hs_code]:
            db.upsert_product(
                conn,
                hs_code=record.hs_code,
                title=record.title,
                description=record.description,
                sectors=record.sectors,
                capex_min=record.capex_min,
                capex_max=record.capex_max,
            )
            products_seen[record.hs_code] = True
        fx_rate: Optional[float]
        try:
            fx_rate = forex.monthly_rate(record.year, record.month)
        except RuntimeError:
            LOGGER.warning(
                "Missing FX rate for %s %s-%02d; storing without INR conversion",
                record.hs_code,
                record.year,
                record.month,
            )
            fx_rate = None

        value_usd = record.value_usd
        value_inr = record.value_inr
        if value_usd is None and value_inr is None:
            LOGGER.debug("Skipping record %s due to missing monetary values", record)
            continue
        if fx_rate is not None and value_inr is None and value_usd is not None:
            value_inr = value_usd * fx_rate
        if fx_rate is not None and value_usd is None and value_inr is not None:
            value_usd = value_inr / fx_rate

        db.insert_monthly(
            conn,
            hs_code=record.hs_code,
            year=record.year,
            month=record.month,
            value_usd=value_usd,
            value_inr=value_inr,
            fx_rate=fx_rate,
            qty=record.qty,
            partner=record.partner_country,
        )
        monthly_rows += 1
    return len(products_seen), monthly_rows

def run(
    conn,
    *,
    from_period: str,
    to_period: str,
) -> Dict[str, object]:
    records = fetch_range(from_period, to_period)
    if not records:
        raise RuntimeError("Comtrade returned no records for the requested range")

    products, monthly_rows = load(conn, records)
    return {
        "products_upserted": products,
        "monthly_imports_upserted": monthly_rows,
        "source": "comtrade",
    }
