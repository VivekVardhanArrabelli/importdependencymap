"""ETL pipeline for UN Comtrade monthly imports."""
from __future__ import annotations

import csv
import json
import logging
import os
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple
from urllib import error, parse, request

from .. import db
from . import normalize

LOGGER = logging.getLogger(__name__)

DEFAULT_BASE = "https://comtradeapi.un.org/public/v1/preview"
MAX_RETRIES = 4
RETRY_STATUS = {429, 500, 502, 503, 504}
DATA_FALLBACK = Path("data/top100_hs.csv")


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
    qty: Optional[float]
    partner_country: Optional[str]


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


def _load_csv_fallback() -> List[Record]:
    if not DATA_FALLBACK.exists():
        return []
    records: List[Record] = []
    with DATA_FALLBACK.open("r", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            hs_code = normalize.canonical_hs_code(row.get("hs_code"))
            if not hs_code:
                continue
            title = (row.get("title") or "").strip()
            description = (row.get("description") or "").strip()
            sectors = normalize.parse_csv_sectors(row.get("sectors", "")) or normalize.infer_sectors(title, description)
            try:
                capex_min = float(row.get("capex_min")) if row.get("capex_min") else None
                capex_max = float(row.get("capex_max")) if row.get("capex_max") else None
                seed_value = float(row.get("seed_month_value")) if row.get("seed_month_value") else None
            except (ValueError, TypeError):
                capex_min = capex_max = seed_value = None
            partner = (row.get("top_country") or "").strip() or None
            for month in range(1, 13):
                records.append(
                    Record(
                        hs_code=hs_code,
                        title=title or f"HS {hs_code}",
                        description=description,
                        sectors=sectors,
                        capex_min=capex_min,
                        capex_max=capex_max,
                        year=2024,
                        month=month,
                        value_usd=normalize.ensure_usd(seed_value),
                        qty=None,
                        partner_country=partner,
                    )
                )
    LOGGER.info("Loaded %s fallback rows from CSV", len(records))
    return records


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
        db.insert_monthly(
            conn,
            hs_code=record.hs_code,
            year=record.year,
            month=record.month,
            value_usd=record.value_usd,
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
    try:
        records = fetch_range(from_period, to_period)
        source = "comtrade"
    except Exception as exc:  # pragma: no cover - triggered in integration tests
        LOGGER.warning("Falling back to CSV due to error: %s", exc)
        records = _load_csv_fallback()
        source = "csv_fallback"
    if not records:
        return {"products": 0, "monthly_rows": 0, "source": source}

    products, monthly_rows = load(conn, records)
    return {
        "products": products,
        "monthly_rows": monthly_rows,
        "source": source,
    }
