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
    return base



def _resolve_endpoint() -> str:
    """Return the fully qualified endpoint for Comtrade requests."""

    base = _base_url().rstrip("/")
    path = os.getenv("COMTRADE_PATH")
    if path:
        endpoint = parse.urljoin(base + "/", path.lstrip("/"))
    else:
        # If COMTRADE_BASE already points at a versioned API path (e.g. /public/v1/preview),
        # treat it as the full endpoint. Otherwise, fall back to legacy v1/get/HS.
        if "/v1/" in base or "/public/" in base:
            endpoint = base
        else:
            endpoint = parse.urljoin(base + "/", "v1/get/HS")
    return endpoint


def _request(params: Dict[str, str]) -> Dict:
    query = parse.urlencode(params)
    url = f"{_resolve_endpoint()}?{query}"
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            with request.urlopen(url, timeout=45) as resp:
                status = resp.status
                if status in RETRY_STATUS:
                    raise error.HTTPError(url, status, "retryable", hdrs=None, fp=None)
                payload = resp.read()
                return json.loads(payload.decode("utf-8"))
        except error.HTTPError as exc:  # pragma: no cover - integration scenario
            if exc.code in RETRY_STATUS and attempt < MAX_RETRIES:
                LOGGER.warning("Comtrade HTTP %s (%s/%s); backing off", exc.code, attempt, MAX_RETRIES)
                time.sleep(min(2 ** attempt, 30))
                continue
            raise
        except error.URLError as exc:  # pragma: no cover - integration scenario
            LOGGER.warning("Comtrade request failed (%s/%s): %s", attempt, MAX_RETRIES, exc)
            if attempt == MAX_RETRIES:
                raise
            time.sleep(min(2 ** attempt, 30))
    raise RuntimeError("Comtrade API request failed after retries")


def _extract_dataset(payload: Dict) -> List[Dict]:
    dataset = payload.get("dataset")
    if isinstance(dataset, list):
        return dataset
    data = payload.get("data")
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        nested = data.get("dataset")
        if isinstance(nested, list):
            return nested
    return []


def _next_cursor(payload: Dict) -> Optional[str]:
    links = payload.get("links") or {}
    if isinstance(links, dict):
        next_link = links.get("next")
        if isinstance(next_link, dict):
            next_link = next_link.get("href") or next_link.get("url")
        if isinstance(next_link, str):
            parsed = parse.urlparse(next_link)
            if parsed.query:
                query = dict(parse.parse_qsl(parsed.query))
                cursor = query.get("cursor")
                if cursor:
                    return cursor
            if next_link.startswith("cursor="):
                return next_link.split("cursor=", 1)[1]
            if next_link:
                return next_link
    meta = payload.get("meta")
    if isinstance(meta, dict):
        for key in ("next", "cursor"):
            cursor = meta.get(key)
            if isinstance(cursor, str) and cursor:
                return cursor
    for key in ("next", "cursor"):
        cursor = payload.get(key)
        if isinstance(cursor, str) and cursor:
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
        title = (row.get("cmdDescE") or "").strip()
        description = (row.get("mainCategory") or row.get("aggLevel")) or ""
        partner = (row.get("ptTitle") or row.get("pt3ISO") or "").strip() or None
        try:
            value_usd = float(row.get("TradeValue")) if row.get("TradeValue") is not None else None
        except (ValueError, TypeError):
            value_usd = None
        qty_raw = row.get("NetWeight") or row.get("qty")
        try:
            qty = float(qty_raw) if qty_raw is not None else None
        except (ValueError, TypeError):
            qty = None
        sectors = normalize.infer_sectors(title, description)
        records.append(
            Record(
                hs_code=hs_code,
                title=title or f"HS {hs_code}",
                description=description if isinstance(description, str) else "",
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


def fetch_range(
    from_period: str,
    to_period: str,
    *,
    reporter: Optional[str] = None,
    flow: Optional[str] = None,
    frequency: Optional[str] = None,
    reporter_code: Optional[str] = None,
) -> List[Record]:
    reporter = reporter or os.getenv("COMTRADE_REPORTER", "India")

    reporter_code_env = os.getenv("COMTRADE_REPORTER_CODE")
    reporter_code = reporter_code or (reporter_code_env.strip() if reporter_code_env else "699")
    flow = flow or os.getenv("COMTRADE_FLOW", "import")
    frequency = frequency or os.getenv("COMTRADE_FREQ", "M")
    partner = os.getenv("COMTRADE_PARTNER")
    partner_code_env = os.getenv("COMTRADE_PARTNER_CODE")
    partner_code = partner_code_env.strip() if partner_code_env else "0"


    base_params = {
        "reporter": reporter,
        "flow": flow,
        "time_period": f"{from_period}:{to_period}",
        "frequency": frequency,
        "type": "C",
        "classification": "HS",
    }
    if reporter_code:

        base_params["reporterCode"] = reporter_code
    if partner_code:
        base_params["partnerCode"] = partner_code
    elif partner:
        base_params["partner"] = partner

    dataset: List[Dict] = []
    cursor: Optional[str] = None
    while True:
        params = dict(base_params)
        if cursor:
            params["cursor"] = cursor
        payload = _request(params)

        validation = payload.get("validation")
        if isinstance(validation, dict) and validation.get("status", "").lower() == "error":
            message = validation.get("message") or validation.get("description") or "Unknown validation error"
            raise RuntimeError(f"Comtrade request rejected: {message}")

        dataset.extend(_extract_dataset(payload))
        cursor = _next_cursor(payload)
        if not cursor:
            break


    LOGGER.info("Fetched %s rows from Comtrade", len(dataset))
    return _parse_dataset(dataset)


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
        "products": products,
        "monthly_rows": monthly_rows,
        "source": "comtrade",
    }
