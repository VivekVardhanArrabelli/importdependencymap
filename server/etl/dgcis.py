"""ETL routines for DGCI&S (Tradestat) data exports."""
from __future__ import annotations

import csv
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, List, Optional, Tuple

from .. import db, forex
from . import normalize

LOGGER = logging.getLogger(__name__)


@dataclass
class Record:
    hs_code: str
    title: str
    description: str
    sectors: List[str]
    year: int
    month: int
    value_inr: Optional[float]
    value_usd: Optional[float]
    fx_rate: Optional[float]
    qty: Optional[float]
    partner_country: Optional[str]


REQUIRED_COLUMNS = {
    "hs_code",
    "year",
    "month",
    "value_inr",
}


def _read_csv(path: Path) -> Iterable[dict]:
    with path.open("r", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        missing = REQUIRED_COLUMNS - set(reader.fieldnames or [])
        if missing:
            raise RuntimeError(f"DGCI&S file missing required columns: {', '.join(sorted(missing))}")
        yield from reader


def _parse_row(row: dict) -> Optional[Record]:
    hs_code = normalize.canonical_hs_code(row.get("hs_code"))
    if not hs_code:
        return None
    try:
        year = int(row["year"])
        month = int(row["month"])
    except (TypeError, ValueError):
        return None

    def _to_float(key: str) -> Optional[float]:
        value = row.get(key)
        if value in (None, ""):
            return None
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    value_inr = _to_float("value_inr")
    value_usd = _to_float("value_usd")
    qty = _to_float("qty")
    partner = row.get("partner_country") or None
    title = (row.get("title") or "").strip()
    description = (row.get("description") or "").strip()
    sectors = normalize.parse_csv_sectors(row.get("sectors", "")) or normalize.infer_sectors(title, description)

    try:
        fx_rate = forex.monthly_rate(year, month)
    except RuntimeError:
        fx_rate = None

    if value_inr is None and value_usd is None:
        return None
    if fx_rate is not None:
        if value_inr is None and value_usd is not None:
            value_inr = value_usd * fx_rate
        if value_usd is None and value_inr is not None:
            value_usd = value_inr / fx_rate

    return Record(
        hs_code=hs_code,
        title=title or f"HS {hs_code}",
        description=description,
        sectors=sectors,
        year=year,
        month=month,
        value_inr=value_inr,
        value_usd=value_usd,
        fx_rate=fx_rate,
        qty=qty,
        partner_country=partner,
    )


def load_csv(path: Path) -> List[Record]:
    records: List[Record] = []
    for raw in _read_csv(path):
        record = _parse_row(raw)
        if record is None:
            continue
        records.append(record)
    if not records:
        raise RuntimeError(f"DGCI&S file {path} did not yield any valid rows")
    LOGGER.info("Parsed %s DGCI&S records from %s", len(records), path)
    return records


def load(conn, records: Iterable[Record]) -> Tuple[int, int]:
    products_seen: dict[str, bool] = {}
    monthly_rows = 0
    for record in records:
        if record.value_usd is None or record.value_inr is None or record.fx_rate is None:
            raise RuntimeError(
                f"Incomplete monetary data for {record.hs_code} {record.year}-{record.month:02d}"
            )
        if record.hs_code not in products_seen:
            db.upsert_product(
                conn,
                hs_code=record.hs_code,
                title=record.title,
                description=record.description,
                sectors=record.sectors,
                capex_min=None,
                capex_max=None,
            )
            products_seen[record.hs_code] = True
        db.insert_monthly(
            conn,
            hs_code=record.hs_code,
            year=record.year,
            month=record.month,
            value_usd=record.value_usd,
            value_inr=record.value_inr,
            fx_rate=record.fx_rate,
            qty=record.qty,
            partner=record.partner_country,
        )
        monthly_rows += 1
    return len(products_seen), monthly_rows


def run(conn, *, source: Path) -> dict:
    if not source.exists():
        raise RuntimeError(f"DGCI&S source file not found: {source}")
    records = load_csv(source)
    products, monthly_rows = load(conn, records)
    return {
        "products": products,
        "monthly_rows": monthly_rows,
        "source": "dgcis",
        "file": str(source),
    }
