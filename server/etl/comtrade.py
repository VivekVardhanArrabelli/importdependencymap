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

DEFAULT_BASE = "https://comtradeapi.un.org/public/v1/preview"
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
    return os.getenv("COMTRADE_BASE", DEFAULT_BASE)


def _request(params: Dict[str, str]) -> Dict:
    query = parse.urlencode(params)
    url = f"{_base_url().rstrip('/')}/v1/get/HS?{query}"
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
) -> List[Record]:
    reporter = reporter or os.getenv("COMTRADE_REPORTER", "India")
    flow = flow or os.getenv("COMTRADE_FLOW", "import")
    frequency = frequency or os.getenv("COMTRADE_FREQ", "M")

    params = {
        "reporter": reporter,
        "flow": flow,
        "time_period": f"{from_period}:{to_period}",
        "frequency": frequency,
        "type": "C",
        "classification": "HS",
    }
    payload = _request(params)
    dataset = payload.get("dataset") or []
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
        try:
            fx_rate = forex.monthly_rate(record.year, record.month)
        except RuntimeError as exc:
            raise RuntimeError(
                f"Missing FX rate for {record.year}-{record.month:02d} while processing {record.hs_code}"
            ) from exc

        value_usd = record.value_usd
        value_inr = record.value_inr
        if value_usd is None and value_inr is None:
            LOGGER.debug("Skipping record %s due to missing monetary values", record)
            continue
        if value_inr is None and value_usd is not None:
            value_inr = value_usd * fx_rate
        if value_usd is None and value_inr is not None:
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
