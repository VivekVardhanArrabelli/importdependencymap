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

DEFAULT_BASE = "https://comtradeapi.un.org/data/v1"
DEFAULT_OPERATION = "get"
DEFAULT_TYPE_CODE = "C"
DEFAULT_FREQ_CODE = "M"
DEFAULT_CLASSIFICATION = "HS"
DEFAULT_REPORTER_CODE = "356"  # India
DEFAULT_FLOW_CODE = "M"  # Import
DEFAULT_PARTNER_CODE = "0"  # World aggregate

REPORTER_NAME_MAP = {
    "india": "356",
}

FLOW_CODE_MAP = {
    "m": "M",
    "import": "M",
    "imports": "M",
    "1": "M",
    "x": "X",
    "export": "X",
    "exports": "X",
    "2": "X",
    "r": "R",
    "re-export": "R",
    "reexport": "R",
    "3": "R",
    "i": "I",
    "re-import": "I",
    "reimport": "I",
    "4": "I",
}

HS_CHAPTERS: Tuple[int, ...] = tuple(range(1, 100))

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
    base = os.getenv("COMTRADE_BASE", DEFAULT_BASE)
    return base.rstrip("/")


def _resolve_endpoint(*path_parts: str) -> str:
    """Return the fully qualified endpoint for Comtrade requests."""
    parts = [_base_url().rstrip("/")]
    parts.extend(part.strip("/") for part in path_parts if part)
    return "/".join(parts)

def _request(
    params: Dict[str, str],
    *,
    path_parts: Optional[Iterable[str]] = None,
) -> Dict:
    # Add subscription key if available
    key = os.getenv("COMTRADE_KEY")
    if key:
        params["subscription-key"] = key
    
    query = parse.urlencode(params)
    path = list(path_parts or [])
    url = f"{_resolve_endpoint(*path)}?{query}"
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            req = request.Request(url)
            if key:
                req.add_header("Ocp-Apim-Subscription-Key", key)
            
            with request.urlopen(req, timeout=45) as resp:
                status = resp.status
                if status in RETRY_STATUS:
                    raise error.HTTPError(url, status, "retryable", hdrs=None, fp=None)
                payload = resp.read()
                return json.loads(payload.decode("utf-8"))
        except error.HTTPError as exc:
            body = ""
            try:
                body = exc.read().decode("utf-8")
            except Exception:
                body = ""
            if exc.code in RETRY_STATUS and attempt < MAX_RETRIES:
                LOGGER.warning("Comtrade HTTP %s (%s/%s); backing off", exc.code, attempt, MAX_RETRIES)
                time.sleep(min(2 ** attempt, 30))
                continue
            detail = body or exc.reason or "Unknown HTTP error"
            raise RuntimeError(f"Comtrade request failed ({exc.code}): {detail}") from exc
        except error.URLError as exc:
            LOGGER.warning("Comtrade request failed (%s/%s): %s", attempt, MAX_RETRIES, exc)
            if attempt == MAX_RETRIES:
                raise
            time.sleep(min(2 ** attempt, 30))
    raise RuntimeError("Comtrade API request failed after retries")

def _extract_dataset(payload: Dict) -> List[Dict]:
    data = payload.get("data")
    if isinstance(data, list):
        return data
    dataset = payload.get("dataset")
    if isinstance(dataset, list):
        return dataset
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
    """Generate comma-separated YYYYMM periods from inclusive YYYY-MM or YYYYMM inputs."""
    def parse_period(p: str) -> Tuple[int, int]:
        p = (p or "").strip()
        if "-" in p and len(p) >= 7:
            return int(p[:4]), int(p[5:7])
        return int(p[:4]), int(p[4:6])

    from_y, from_m = parse_period(from_period)
    to_y, to_m = parse_period(to_period)

    periods: List[str] = []
    y, m = from_y, from_m
    while y < to_y or (y == to_y and m <= to_m):
        periods.append(f"{y:04d}{m:02d}")
        m += 1
        if m > 12:
            m = 1
            y += 1
    return periods


def _normalize_reporter_code(value: Optional[str]) -> str:
    candidate = value.strip() if value else None
    if candidate:
        if candidate.isdigit():
            return candidate
        mapped = REPORTER_NAME_MAP.get(candidate.lower())
        if mapped:
            return mapped
    env_code = os.getenv("COMTRADE_REPORTER_CODE")
    if env_code:
        env_code = env_code.strip()
        if env_code:
            return env_code
    env_name = os.getenv("COMTRADE_REPORTER")
    if env_name:
        env_name = env_name.strip()
        if env_name.isdigit():
            return env_name
        mapped = REPORTER_NAME_MAP.get(env_name.lower())
        if mapped:
            return mapped
    return DEFAULT_REPORTER_CODE


def _normalize_flow_code(value: Optional[str]) -> str:
    def resolve(token: Optional[str]) -> Optional[str]:
        if not token:
            return None
        normalized = token.strip()
        if not normalized:
            return None
        key = normalized.lower()
        if normalized.upper() in {"M", "X", "R", "I"}:
            return normalized.upper()
        if key in FLOW_CODE_MAP:
            return FLOW_CODE_MAP[key]
        return None

    for candidate in (
        value,
        os.getenv("COMTRADE_FLOW_CODE"),
        os.getenv("COMTRADE_FLOW"),
    ):
        resolved = resolve(candidate)
        if resolved:
            return resolved

    return DEFAULT_FLOW_CODE


def _normalize_partner_code(value: Optional[str]) -> Optional[str]:
    candidate = value.strip() if value else None
    if not candidate:
        candidate = os.getenv("COMTRADE_PARTNER_CODE") or os.getenv("COMTRADE_PARTNER")
        candidate = candidate.strip() if candidate else DEFAULT_PARTNER_CODE
    lowered = candidate.lower()
    if lowered in {"", "all"}:
        return None
    if lowered == "world":
        return "0"
    return candidate


def _resolve_type_code(value: Optional[str]) -> str:
    candidate = value or os.getenv("COMTRADE_TYPE_CODE") or os.getenv("COMTRADE_TYPE")
    candidate = (candidate or DEFAULT_TYPE_CODE).strip()
    return candidate.upper() or DEFAULT_TYPE_CODE


def _resolve_freq_code(value: Optional[str]) -> str:
    candidate = value or os.getenv("COMTRADE_FREQ_CODE") or os.getenv("COMTRADE_FREQ")
    candidate = (candidate or DEFAULT_FREQ_CODE).strip()
    return candidate.upper() or DEFAULT_FREQ_CODE


def _resolve_classification(value: Optional[str] = None) -> str:
    candidate = value or os.getenv("COMTRADE_CL_CODE") or os.getenv("COMTRADE_CLASSIFICATION")
    candidate = (candidate or DEFAULT_CLASSIFICATION).strip()
    return candidate.upper() or DEFAULT_CLASSIFICATION


def _build_path_segments(type_code: str, freq_code: str, classification: str) -> List[str]:
    operation = os.getenv("COMTRADE_PATH", DEFAULT_OPERATION).strip().strip("/")
    operation = operation or DEFAULT_OPERATION
    return [operation, type_code.upper(), freq_code.upper(), classification.upper()]

def fetch_range(
    from_period: str,
    to_period: str,
    *,
    reporter_code: Optional[str] = None,
    flow_code: Optional[str] = None,
    frequency: Optional[str] = None,
) -> List[Record]:
    reporter_code = _normalize_reporter_code(reporter_code)
    flow_code = _normalize_flow_code(flow_code)
    freq_code = _resolve_freq_code(frequency)
    type_code = _resolve_type_code(None)
    classification = _resolve_classification()
    partner_code = _normalize_partner_code(None)
    include_desc = "true" if os.getenv("COMTRADE_INCLUDE_DESC", "false").lower() in {"1", "true", "yes"} else "false"
    path_segments = _build_path_segments(type_code, freq_code, classification)

    base_params = {
        "reporterCode": reporter_code,
        "flowCode": flow_code,
        "period": ",".join(_build_periods(from_period, to_period)),
        "includeDesc": include_desc,
    }
    if partner_code:
        base_params["partnerCode"] = partner_code
    
    all_records = []
    for chapter in HS_CHAPTERS:  # HS chapters 01-99
        params = dict(base_params)
        params["cmdCode"] = f"{chapter:02d}*"  # Chapter wildcard
        
        dataset = []
        cursor = None
        while True:
            query_params = dict(params)
            if cursor:
                query_params["cursor"] = cursor
            payload = _request(query_params, path_parts=path_segments)

            validation = payload.get("validation") if isinstance(payload, dict) else None
            if isinstance(validation, dict) and str(validation.get("status", "")).upper() == "ERROR":
                message = validation.get("message") or validation.get("description") or "Comtrade validation error"
                raise RuntimeError(message)

            error_message = payload.get("error") if isinstance(payload, dict) else None
            if error_message:
                LOGGER.warning("Comtrade error for HS chapter %02d: %s", chapter, error_message)
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
