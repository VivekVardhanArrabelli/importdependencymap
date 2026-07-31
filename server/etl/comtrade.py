"""ETL pipeline for current UN Comtrade monthly imports.

The product dimension and trade facts are deliberately fetched separately:

* the public HS 2022 reference always supplies the complete HS6 catalogue;
* a keyed data request is the fast path for monthly facts;
* the anonymous preview API remains a complete (but slower) fallback by
  requesting at most 350 explicit HS6 codes for one period at a time.

This avoids treating Comtrade's 500-row preview as a complete response and
keeps an upstream fact outage from emptying the product catalogue.
"""
from __future__ import annotations

import json
import logging
import os
import re
import threading
import time
from dataclasses import dataclass
from functools import lru_cache
from typing import Dict, Iterable, List, Optional, Sequence, Tuple
from urllib import error, parse, request

from .. import db, forex
from . import normalize

LOGGER = logging.getLogger(__name__)

MAX_RETRIES = 5
RETRY_STATUS = {429, 500, 502, 503, 504}

KEYED_BASE = "https://comtradeapi.un.org/data/v1"
PREVIEW_BASE = "https://comtradeapi.un.org/public/v1/preview"
AVAILABILITY_BASE = "https://comtradeapi.un.org/public/v1/getDA"
DEFAULT_REFERENCE_URL = "https://comtradeapi.un.org/files/v1/app/reference/H6.json"

DEFAULT_TYPE_CODE = "C"
DEFAULT_FREQ_CODE = "M"
DEFAULT_CLASSIFICATION = "HS"
DEFAULT_REPORTER_CODE = "699"  # Current India reporter (356 expired in 1974).
DEFAULT_FLOW_CODE = "M"  # Imports.
DEFAULT_PARTNER_CODE = "0"  # World aggregate.
DEFAULT_KEYED_MAX_RECORDS = 100_000
DEFAULT_KEYED_PERIOD_BATCH_SIZE = 12
DEFAULT_PREVIEW_MAX_RECORDS = 500
DEFAULT_PREVIEW_CODE_BATCH_SIZE = 350
PREVIEW_MIN_INTERVAL_SECONDS = 1.05

REPORTER_NAME_MAP = {
    "india": DEFAULT_REPORTER_CODE,
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

_HS6_PATTERN = re.compile(r"^\d{6}$")
_CATALOG_TITLE_PREFIX = re.compile(r"^\s*\d{6}\s*-\s*")
_PREVIEW_LOCK = threading.Lock()
_LAST_PREVIEW_CALL = 0.0


class ComtradeError(RuntimeError):
    """Base class for actionable Comtrade failures."""


class ComtradeConfigurationError(ComtradeError):
    """Raised for an invalid local Comtrade configuration."""


class ComtradeTruncationError(ComtradeError):
    """Raised when a response may have reached an API record ceiling."""


@dataclass(frozen=True)
class CatalogProduct:
    hs_code: str
    title: str
    description: str
    sectors: List[str]


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
    net_weight: Optional[float] = None
    qty_unit_code: Optional[int] = None
    qty_unit: Optional[str] = None
    partner_country: Optional[str] = None
    is_aggregate: bool = False
    source: str = "comtrade"


@dataclass(frozen=True)
class QueryBatch:
    periods: Tuple[str, ...]
    params: Dict[str, str]
    max_records: int
    keyed: bool


def _api_key() -> Optional[str]:
    value = (os.getenv("COMTRADE_KEY") or "").strip()
    return value or None


def _base_url() -> str:
    configured = (os.getenv("COMTRADE_BASE") or "").strip()
    if configured:
        lowered = configured.lower()
        if _api_key() and "/public/v1/preview" in lowered:
            LOGGER.info(
                "COMTRADE_KEY is configured; using the keyed data endpoint instead of preview"
            )
            return KEYED_BASE
        if not _api_key() and "/data/v1" in lowered:
            LOGGER.info(
                "COMTRADE_KEY is absent; using the complete batched preview fallback"
            )
            return PREVIEW_BASE
        return configured.rstrip("/")
    return KEYED_BASE if _api_key() else PREVIEW_BASE


def _resolve_endpoint(*path_parts: str) -> str:
    """Return a fully qualified endpoint without duplicating path segments."""

    parts = [_base_url().rstrip("/")]
    parts.extend(part.strip("/") for part in path_parts if part and part.strip("/"))
    return "/".join(parts)


def _endpoint_has_dimension_path(
    base: str,
    type_code: str,
    freq_code: str,
    classification: str,
) -> bool:
    suffix = f"/{type_code}/{freq_code}/{classification}".lower()
    return base.lower().rstrip("/").endswith(suffix)


def _build_path_segments(type_code: str, freq_code: str, classification: str) -> List[str]:
    """Return the path segments missing from the configured base URL.

    Supported configurations include ``.../data/v1``, ``.../data/v1/get``,
    ``.../public/v1/preview``, and a full endpoint ending in ``/C/M/HS``.
    """

    base = _base_url()
    if _endpoint_has_dimension_path(base, type_code, freq_code, classification):
        return []

    segments: List[str] = []
    lowered = base.lower().rstrip("/")
    if not (lowered.endswith("/get") or lowered.endswith("/preview")):
        segments.append("preview" if "/public/" in lowered else "get")
    segments.extend([type_code.upper(), freq_code.upper(), classification.upper()])
    return segments


def _throttle_preview() -> None:
    global _LAST_PREVIEW_CALL

    with _PREVIEW_LOCK:
        now = time.monotonic()
        wait_seconds = PREVIEW_MIN_INTERVAL_SECONDS - (now - _LAST_PREVIEW_CALL)
        if wait_seconds > 0:
            time.sleep(wait_seconds)
        _LAST_PREVIEW_CALL = time.monotonic()


def _retry_delay(exc: error.HTTPError, attempt: int) -> float:
    retry_after = None
    if exc.headers:
        retry_after = exc.headers.get("Retry-After")
    if retry_after:
        try:
            return max(float(retry_after), 0.0)
        except (TypeError, ValueError):
            pass
    return float(min(2 ** attempt, 30))


def _request_json_url(
    url: str,
    *,
    headers: Optional[Dict[str, str]] = None,
    is_preview: bool = False,
    timeout_seconds: float = 90,
    max_retries: int = MAX_RETRIES,
) -> Dict:
    attempts = max(int(max_retries), 1)
    for attempt in range(1, attempts + 1):
        if is_preview:
            _throttle_preview()
        try:
            req = request.Request(url, headers=headers or {})
            with request.urlopen(req, timeout=max(float(timeout_seconds), 1.0)) as resp:
                payload = resp.read()
            decoded = json.loads(payload.decode("utf-8"))
            if not isinstance(decoded, dict):
                raise ComtradeError("Comtrade returned a non-object JSON response")
            return decoded
        except error.HTTPError as exc:
            body = ""
            try:
                body = exc.read().decode("utf-8")
            except Exception:
                body = ""
            if exc.code in RETRY_STATUS and attempt < attempts:
                delay = _retry_delay(exc, attempt)
                LOGGER.warning(
                    "Comtrade HTTP %s (%s/%s); retrying in %.1fs",
                    exc.code,
                    attempt,
                    attempts,
                    delay,
                )
                time.sleep(delay)
                continue
            detail = body or str(exc.reason or "Unknown HTTP error")
            if exc.code == 401:
                detail = (
                    f"{detail}. Configure COMTRADE_KEY for the keyed data API, "
                    "or remove a keyed COMTRADE_BASE to use the preview fallback."
                )
            raise ComtradeError(f"Comtrade request failed ({exc.code}): {detail}") from exc
        except error.URLError as exc:
            if attempt == attempts:
                raise ComtradeError(f"Comtrade network request failed: {exc}") from exc
            delay = float(min(2 ** attempt, 30))
            LOGGER.warning(
                "Comtrade network failure (%s/%s); retrying in %.1fs: %s",
                attempt,
                attempts,
                delay,
                exc,
            )
            time.sleep(delay)
        except json.JSONDecodeError as exc:
            raise ComtradeError("Comtrade returned invalid JSON") from exc
    raise ComtradeError("Comtrade API request failed after retries")


def _request(
    params: Dict[str, str],
    *,
    path_parts: Optional[Iterable[str]] = None,
) -> Dict:
    request_params = dict(params)
    key = _api_key()
    endpoint = _resolve_endpoint(*(path_parts or []))
    is_preview = "/public/v1/preview/" in f"{endpoint}/"

    headers = {"Accept": "application/json"}
    if key and not is_preview:
        request_params["subscription-key"] = key
        headers["Ocp-Apim-Subscription-Key"] = key

    query = parse.urlencode(request_params)
    return _request_json_url(
        f"{endpoint}?{query}",
        headers=headers,
        is_preview=is_preview,
    )


def _extract_dataset(payload: Dict) -> List[Dict]:
    for key in ("data", "dataset"):
        value = payload.get(key)
        if isinstance(value, list):
            return value
    return []


def _next_cursor(payload: Dict) -> Optional[str]:
    """Backward-compatible cursor reader for any future paginated endpoint."""

    links = payload.get("links", {})
    if isinstance(links, dict):
        next_link = links.get("next")
        if isinstance(next_link, dict):
            next_link = next_link.get("href")
        if isinstance(next_link, str) and "?" in next_link:
            parsed = parse.urlparse(next_link)
            return dict(parse.parse_qsl(parsed.query)).get("cursor")
    return None


def _validation_error(payload: Dict) -> Optional[str]:
    validation = payload.get("validation")
    if isinstance(validation, dict):
        status_value = str(validation.get("status", "")).upper()
        if status_value == "ERROR":
            return str(
                validation.get("message")
                or validation.get("description")
                or "Comtrade validation error"
            )
    error_message = payload.get("error")
    if error_message:
        return str(error_message)
    status_code = payload.get("statusCode")
    if isinstance(status_code, int) and status_code >= 400:
        return str(payload.get("message") or f"Comtrade status {status_code}")
    return None


def _validate_payload(
    payload: Dict,
    *,
    max_records: int,
    context: str,
) -> List[Dict]:
    message = _validation_error(payload)
    if message:
        raise ComtradeError(f"{context}: {message}")

    dataset = _extract_dataset(payload)
    count = payload.get("count")
    if isinstance(count, str) and count.isdigit():
        count = int(count)
    if isinstance(count, int) and count > len(dataset):
        raise ComtradeTruncationError(
            f"{context}: API reported {count} rows but returned {len(dataset)}"
        )
    if len(dataset) >= max_records:
        raise ComtradeTruncationError(
            f"{context}: response reached the {max_records}-row ceiling"
        )
    return dataset


def _truthy(value: object) -> bool:
    if isinstance(value, bool):
        return value
    return str(value or "").strip().lower() in {"1", "true", "yes", "y"}


def _parse_catalog(payload: Dict) -> List[CatalogProduct]:
    raw_results = payload.get("results")
    if not isinstance(raw_results, list):
        raise ComtradeError("HS reference response is missing its results array")

    products: List[CatalogProduct] = []
    seen: set[str] = set()
    for row in raw_results:
        if not isinstance(row, dict):
            continue
        raw_code = str(row.get("id") or "").strip()
        if not _HS6_PATTERN.fullmatch(raw_code):
            continue
        try:
            level = int(row.get("aggrlevel", row.get("aggrLevel", 0)))
        except (TypeError, ValueError):
            continue
        if level != 6 or not _truthy(row.get("isLeaf")):
            continue
        if raw_code in seen:
            continue

        raw_title = str(row.get("text") or "").strip()
        title = _CATALOG_TITLE_PREFIX.sub("", raw_title).strip() or f"HS {raw_code}"
        products.append(
            CatalogProduct(
                hs_code=raw_code,
                title=title,
                description=title,
                sectors=normalize.infer_sectors(title),
            )
        )
        seen.add(raw_code)

    products.sort(key=lambda product: product.hs_code)
    return products


def fetch_product_catalog(
    *,
    timeout_seconds: float = 90,
    max_retries: int = MAX_RETRIES,
) -> List[CatalogProduct]:
    url = (os.getenv("COMTRADE_HS_REFERENCE") or DEFAULT_REFERENCE_URL).strip()
    payload = _request_json_url(
        url,
        headers={"Accept": "application/json"},
        timeout_seconds=timeout_seconds,
        max_retries=max_retries,
    )
    products = _parse_catalog(payload)
    if len(products) < 1_000:
        raise ComtradeError(
            f"HS reference returned only {len(products)} HS6 leaf products; "
            "refusing incomplete catalogue"
        )
    return products


def sync_product_catalog(
    conn,
    products: Optional[Sequence[CatalogProduct]] = None,
) -> int:
    catalogue = list(products or fetch_product_catalog())
    rows = [
        (
            item.hs_code,
            item.title,
            item.description,
            item.sectors,
            None,
            None,
        )
        for item in catalogue
    ]
    return db.bulk_upsert_products(conn, rows)


def _leaf_row(row: Dict) -> bool:
    raw_code = str(row.get("cmdCode") or "").strip()
    if not _HS6_PATTERN.fullmatch(raw_code):
        return False
    level_raw = row.get("aggrLevel", row.get("aggrlevel"))
    if level_raw not in (None, ""):
        try:
            if int(level_raw) != 6:
                return False
        except (TypeError, ValueError):
            return False
    leaf_raw = row.get("isLeaf")
    if leaf_raw is not None and not _truthy(leaf_raw):
        return False
    return True


def _as_float(value: object) -> Optional[float]:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _as_int(value: object) -> Optional[int]:
    if value in (None, ""):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _first_present(row: Dict, *keys: str) -> object:
    for key in keys:
        value = row.get(key)
        if value not in (None, ""):
            return value
    return None


def _parse_dataset(dataset: Iterable[Dict]) -> List[Record]:
    records: List[Record] = []
    for row in dataset:
        if not isinstance(row, dict) or not _leaf_row(row):
            continue

        hs_code = str(row.get("cmdCode")).strip()
        period = str(
            _first_present(row, "period")
            or (
                f"{int(row['refYear']):04d}{int(row['refMonth']):02d}"
                if row.get("refYear") and row.get("refMonth")
                else ""
            )
        )
        if len(period) != 6 or not period.isdigit():
            continue
        year, month = int(period[:4]), int(period[4:6])
        if month < 1 or month > 12:
            continue

        title = str(
            _first_present(row, "cmdDesc", "cmdDescE", "cmdDescription") or ""
        ).strip()
        description = str(
            _first_present(row, "mainCategory", "cmdDesc", "cmdDescE") or ""
        ).strip()
        partner_code = str(
            _first_present(row, "partnerISO", "pt3ISO", "partnerDesc", "ptTitle")
            or ""
        ).strip()
        partner_numeric = str(row.get("partnerCode") or "").strip()
        is_aggregate = partner_numeric == "0" or partner_code.upper() in {
            "W00",
            "WORLD",
        }
        if not partner_code and is_aggregate:
            partner_code = "W00"

        value_usd = _as_float(
            _first_present(row, "primaryValue", "TradeValue", "tradeValue", "cifvalue")
        )
        qty = _as_float(_first_present(row, "qty", "altQty"))
        net_weight = _as_float(_first_present(row, "netWgt", "NetWeight", "netWeight"))
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
                net_weight=net_weight,
                qty_unit_code=_as_int(row.get("qtyUnitCode")),
                qty_unit=str(row.get("qtyUnitAbbr") or "").strip() or None,
                partner_country=partner_code or None,
                is_aggregate=is_aggregate,
            )
        )
    return records


def _parse_period(value: str) -> Tuple[int, int]:
    cleaned = (value or "").strip()
    match = re.fullmatch(r"(\d{4})-?(\d{2})", cleaned)
    if not match:
        raise ComtradeConfigurationError(
            f"Invalid monthly period {value!r}; expected YYYY-MM or YYYYMM"
        )
    year, month = int(match.group(1)), int(match.group(2))
    if month < 1 or month > 12:
        raise ComtradeConfigurationError(f"Invalid month in period {value!r}")
    return year, month


def _build_periods(from_period: str, to_period: str) -> List[str]:
    """Generate inclusive YYYYMM periods from YYYY-MM or YYYYMM inputs."""

    from_y, from_m = _parse_period(from_period)
    to_y, to_m = _parse_period(to_period)
    if (from_y, from_m) > (to_y, to_m):
        raise ComtradeConfigurationError("from_period must be <= to_period")

    periods: List[str] = []
    year, month = from_y, from_m
    while (year, month) <= (to_y, to_m):
        periods.append(f"{year:04d}{month:02d}")
        if month == 12:
            year, month = year + 1, 1
        else:
            month += 1
    return periods


def _normalize_reporter_code(value: Optional[str]) -> str:
    for candidate in (
        value,
        os.getenv("COMTRADE_REPORTER_CODE"),
        os.getenv("COMTRADE_REPORTER"),
    ):
        normalized = (candidate or "").strip()
        if not normalized:
            continue
        if normalized.isdigit():
            if normalized == "356":
                raise ComtradeConfigurationError(
                    "Reporter code 356 is historical India and has no current "
                    "monthly data; remove the override or use 699"
                )
            return normalized
        mapped = REPORTER_NAME_MAP.get(normalized.lower())
        if mapped:
            return mapped
        raise ComtradeConfigurationError(f"Unknown Comtrade reporter {candidate!r}")
    return DEFAULT_REPORTER_CODE


def _normalize_flow_code(value: Optional[str]) -> str:
    for candidate in (
        value,
        os.getenv("COMTRADE_FLOW_CODE"),
        os.getenv("COMTRADE_FLOW"),
    ):
        normalized = (candidate or "").strip()
        if not normalized:
            continue
        resolved = FLOW_CODE_MAP.get(normalized.lower())
        if resolved:
            return resolved
        raise ComtradeConfigurationError(f"Unknown Comtrade flow {candidate!r}")
    return DEFAULT_FLOW_CODE


def _normalize_partner_code(value: Optional[str]) -> Optional[str]:
    candidate = (
        value
        or os.getenv("COMTRADE_PARTNER_CODE")
        or os.getenv("COMTRADE_PARTNER")
        or DEFAULT_PARTNER_CODE
    ).strip()
    if candidate.lower() in {"", "all"}:
        return None
    if candidate.lower() == "world":
        return DEFAULT_PARTNER_CODE
    return candidate


def _resolve_type_code(value: Optional[str]) -> str:
    candidate = value or os.getenv("COMTRADE_TYPE_CODE") or os.getenv("COMTRADE_TYPE")
    return (candidate or DEFAULT_TYPE_CODE).strip().upper() or DEFAULT_TYPE_CODE


def _resolve_freq_code(value: Optional[str]) -> str:
    candidate = value or os.getenv("COMTRADE_FREQ_CODE") or os.getenv("COMTRADE_FREQ")
    return (candidate or DEFAULT_FREQ_CODE).strip().upper() or DEFAULT_FREQ_CODE


def _resolve_classification(value: Optional[str] = None) -> str:
    candidate = value or os.getenv("COMTRADE_CL_CODE") or os.getenv(
        "COMTRADE_CLASSIFICATION"
    )
    return (candidate or DEFAULT_CLASSIFICATION).strip().upper() or DEFAULT_CLASSIFICATION


def _chunks(values: Sequence[str], size: int) -> Iterable[Sequence[str]]:
    if size < 1:
        raise ComtradeConfigurationError("Comtrade batch size must be positive")
    for index in range(0, len(values), size):
        yield values[index : index + size]


def _build_request_batches(
    periods: Sequence[str],
    catalog_codes: Optional[Sequence[str]],
    keyed: bool,
    *,
    reporter_code: str = DEFAULT_REPORTER_CODE,
    flow_code: str = DEFAULT_FLOW_CODE,
    partner_code: Optional[str] = DEFAULT_PARTNER_CODE,
) -> List[QueryBatch]:
    common = {
        "reporterCode": reporter_code,
        "flowCode": flow_code,
        "includeDesc": "true",
        "breakdownMode": "classic",
        "partner2Code": "0",
        "customsCode": "C00",
        "motCode": "0",
    }
    if partner_code is not None:
        common["partnerCode"] = partner_code

    batches: List[QueryBatch] = []
    if keyed:
        period_batch_size = min(
            max(int(os.getenv("COMTRADE_PERIOD_BATCH_SIZE", DEFAULT_KEYED_PERIOD_BATCH_SIZE)), 1),
            DEFAULT_KEYED_PERIOD_BATCH_SIZE,
        )
        max_records = min(
            max(int(os.getenv("COMTRADE_MAX_RECORDS", DEFAULT_KEYED_MAX_RECORDS)), 1),
            DEFAULT_KEYED_MAX_RECORDS,
        )
        cmd_code = (os.getenv("COMTRADE_CMD_CODE") or "AG6").strip() or "AG6"
        for period_batch in _chunks(list(periods), period_batch_size):
            params = dict(common)
            params.update(
                {
                    "period": ",".join(period_batch),
                    "cmdCode": cmd_code,
                    "maxRecords": str(max_records),
                }
            )
            batches.append(
                QueryBatch(
                    periods=tuple(period_batch),
                    params=params,
                    max_records=max_records,
                    keyed=True,
                )
            )
        return batches

    codes = sorted(
        {
            str(code).strip()
            for code in (catalog_codes or [])
            if _HS6_PATTERN.fullmatch(str(code).strip())
        }
    )
    if not codes:
        raise ComtradeConfigurationError(
            "The preview fallback requires the authoritative HS6 catalogue"
        )
    code_batch_size = min(
        max(int(os.getenv("COMTRADE_PREVIEW_CODE_BATCH_SIZE", DEFAULT_PREVIEW_CODE_BATCH_SIZE)), 1),
        DEFAULT_PREVIEW_CODE_BATCH_SIZE,
    )
    for period in periods:
        for code_batch in _chunks(codes, code_batch_size):
            params = dict(common)
            params.update(
                {
                    "period": period,
                    "cmdCode": ",".join(code_batch),
                    "maxRecords": str(DEFAULT_PREVIEW_MAX_RECORDS),
                }
            )
            batches.append(
                QueryBatch(
                    periods=(period,),
                    params=params,
                    max_records=DEFAULT_PREVIEW_MAX_RECORDS,
                    keyed=False,
                )
            )
    return batches


def _fetch_batch(
    batch: QueryBatch,
    *,
    path_segments: Sequence[str],
) -> List[Dict]:
    context = (
        f"Comtrade periods {','.join(batch.periods)} "
        f"products {batch.params.get('cmdCode', '')[:40]}"
    )
    payload = _request(batch.params, path_parts=path_segments)
    try:
        return _validate_payload(
            payload,
            max_records=batch.max_records,
            context=context,
        )
    except ComtradeTruncationError:
        if not batch.keyed or len(batch.periods) <= 1:
            raise
        midpoint = len(batch.periods) // 2
        rows: List[Dict] = []
        for period_slice in (batch.periods[:midpoint], batch.periods[midpoint:]):
            params = dict(batch.params)
            params["period"] = ",".join(period_slice)
            rows.extend(
                _fetch_batch(
                    QueryBatch(
                        periods=tuple(period_slice),
                        params=params,
                        max_records=batch.max_records,
                        keyed=True,
                    ),
                    path_segments=path_segments,
                )
            )
        return rows


def fetch_range(
    from_period: str,
    to_period: str,
    *,
    reporter_code: Optional[str] = None,
    flow_code: Optional[str] = None,
    frequency: Optional[str] = None,
    catalog_products: Optional[Sequence[CatalogProduct]] = None,
) -> List[Record]:
    reporter = _normalize_reporter_code(reporter_code)
    flow = _normalize_flow_code(flow_code)
    freq_code = _resolve_freq_code(frequency)
    if freq_code != "M":
        raise ComtradeConfigurationError("This ETL currently supports monthly data only")
    type_code = _resolve_type_code(None)
    classification = _resolve_classification()
    partner = _normalize_partner_code(None)
    periods = _build_periods(from_period, to_period)
    keyed = bool(_api_key())

    catalogue = list(catalog_products or [])
    if not keyed and not catalogue:
        catalogue = fetch_product_catalog()
    batches = _build_request_batches(
        periods,
        [item.hs_code for item in catalogue],
        keyed,
        reporter_code=reporter,
        flow_code=flow,
        partner_code=partner,
    )
    path_segments = _build_path_segments(type_code, freq_code, classification)

    records_by_key: Dict[Tuple[str, int, int, Optional[str]], Record] = {}
    for index, batch in enumerate(batches, start=1):
        dataset = _fetch_batch(batch, path_segments=path_segments)
        records = _parse_dataset(dataset)
        for item in records:
            records_by_key[
                (item.hs_code, item.year, item.month, item.partner_country)
            ] = item
        LOGGER.info(
            "Comtrade batch %s/%s returned %s HS6 rows",
            index,
            len(batches),
            len(records),
        )

    ordered = sorted(
        records_by_key.values(),
        key=lambda item: (
            item.year,
            item.month,
            item.hs_code,
            item.partner_country or "",
        ),
    )
    LOGGER.info(
        "Fetched %s unique Comtrade rows across %s periods using %s mode",
        len(ordered),
        len(periods),
        "keyed" if keyed else "preview",
    )
    return ordered


@lru_cache(maxsize=2_048)
def fetch_partner_snapshot(
    hs_code: str,
    period: str,
    *,
    reporter_code: Optional[str] = None,
    flow_code: Optional[str] = None,
) -> Dict[str, object]:
    """Fetch one product's partner breakdown for a single monthly period.

    Partner detail is intentionally kept separate from World-total fact rows so
    rolling imports cannot be double counted. The result is cached per process
    because product detail pages may be opened repeatedly.
    """

    code = str(hs_code or "").strip()
    if not _HS6_PATTERN.fullmatch(code):
        raise ComtradeConfigurationError(f"Invalid HS6 code {hs_code!r}")
    period_year, period_month = _parse_period(period)
    normalized_period = f"{period_year:04d}{period_month:02d}"
    reporter = _normalize_reporter_code(reporter_code)
    flow = _normalize_flow_code(flow_code)
    freq_code = _resolve_freq_code(None)
    type_code = _resolve_type_code(None)
    classification = _resolve_classification()
    keyed = bool(_api_key())
    max_records = (
        DEFAULT_KEYED_MAX_RECORDS if keyed else DEFAULT_PREVIEW_MAX_RECORDS
    )
    params = {
        "reporterCode": reporter,
        "flowCode": flow,
        "period": normalized_period,
        "cmdCode": code,
        "includeDesc": "true",
        "breakdownMode": "classic",
        "partner2Code": "0",
        "customsCode": "C00",
        "motCode": "0",
        "maxRecords": str(max_records),
    }
    endpoint = "/".join(
        [
            PREVIEW_BASE.rstrip("/"),
            type_code,
            freq_code,
            classification,
        ]
    )
    query = parse.urlencode(params)
    payload = _request_json_url(
        f"{endpoint}?{query}",
        headers={"Accept": "application/json"},
        is_preview=True,
        timeout_seconds=8,
        max_retries=1,
    )
    dataset = _validate_payload(
        payload,
        max_records=max_records,
        context=f"Comtrade partner snapshot {code} {normalized_period}",
    )

    partner_totals: Dict[str, Dict[str, object]] = {}
    world_total: Optional[float] = None
    for row in dataset:
        if not isinstance(row, dict) or not _leaf_row(row):
            continue
        value = _as_float(
            _first_present(row, "primaryValue", "TradeValue", "tradeValue", "cifvalue")
        )
        if value is None:
            continue
        partner_numeric = str(row.get("partnerCode") or "").strip()
        partner_code = str(
            _first_present(row, "partnerISO", "pt3ISO") or ""
        ).strip()
        partner_name = str(
            _first_present(row, "partnerDesc", "ptTitle", "partnerISO") or ""
        ).strip()
        if partner_numeric == "0" or partner_code.upper() == "W00":
            world_total = value
            continue
        if not partner_code:
            partner_code = partner_numeric or partner_name
        if not partner_code:
            continue
        item = partner_totals.setdefault(
            partner_code,
            {
                "partner_code": partner_code,
                "partner_country": partner_name or partner_code,
                "value_usd": 0.0,
            },
        )
        item["value_usd"] = float(item["value_usd"]) + value

    partners = sorted(
        partner_totals.values(),
        key=lambda item: float(item["value_usd"]),
        reverse=True,
    )
    partner_sum = sum(float(item["value_usd"]) for item in partners)
    denominator = world_total if world_total and world_total > 0 else partner_sum
    for item in partners:
        item["share"] = (
            float(item["value_usd"]) / denominator if denominator > 0 else None
        )
    hhi = (
        sum((float(item["value_usd"]) / partner_sum) ** 2 for item in partners)
        if partner_sum > 0
        else None
    )
    return {
        "period": normalized_period,
        "partners": partners,
        "hhi": hhi,
        "world_total_usd": world_total,
        "source": "comtrade",
    }


def latest_available_period(
    *,
    reporter_code: Optional[str] = None,
    frequency: Optional[str] = None,
) -> Dict[str, object]:
    reporter = _normalize_reporter_code(reporter_code)
    freq_code = _resolve_freq_code(frequency)
    type_code = _resolve_type_code(None)
    classification = _resolve_classification()
    endpoint = "/".join(
        [
            AVAILABILITY_BASE.rstrip("/"),
            type_code,
            freq_code,
            classification,
        ]
    )
    query = parse.urlencode({"reporterCode": reporter})
    payload = _request_json_url(
        f"{endpoint}?{query}",
        headers={"Accept": "application/json"},
    )
    message = _validation_error(payload)
    if message:
        raise ComtradeError(f"Comtrade data availability: {message}")
    data = _extract_dataset(payload)
    candidates = [
        row
        for row in data
        if isinstance(row, dict)
        and str(row.get("reporterCode") or "") == reporter
        and str(row.get("period") or "").isdigit()
    ]
    if not candidates:
        raise ComtradeError(
            f"Comtrade reports no {freq_code} data availability for reporter {reporter}"
        )
    latest = max(candidates, key=lambda row: int(row["period"]))
    return {
        "period": str(latest["period"]),
        "last_released": latest.get("lastReleased"),
        "classification": latest.get("classificationCode"),
        "total_records": latest.get("totalRecords"),
    }


def _cap_to_available(
    from_period: str,
    to_period: str,
    availability: Dict[str, object],
) -> Tuple[str, str]:
    requested = _build_periods(from_period, to_period)
    latest = str(availability["period"])
    effective = [period for period in requested if period <= latest]
    if not effective:
        raise ComtradeError(
            f"Requested range starts after latest available Comtrade period {latest}"
        )
    return effective[0], effective[-1]


def load(
    conn,
    records: Iterable[Record],
) -> Tuple[int, int]:
    rows = list(records)
    if not rows:
        return 0, 0

    products: Dict[str, Record] = {}
    for item in rows:
        products.setdefault(item.hs_code, item)
    db.bulk_upsert_products(
        conn,
        [
            (
                item.hs_code,
                item.title,
                item.description,
                item.sectors,
                item.capex_min,
                item.capex_max,
            )
            for item in products.values()
        ],
    )

    rates: Dict[Tuple[int, int], Optional[float]] = {}
    for period in sorted({(item.year, item.month) for item in rows}):
        try:
            rates[period] = forex.monthly_rate(*period)
        except RuntimeError:
            rates[period] = None
            LOGGER.info(
                "No USD/INR rate for %s-%02d; storing Comtrade USD without INR",
                period[0],
                period[1],
            )

    monthly_rows = []
    for item in rows:
        if item.value_usd is None and item.value_inr is None:
            continue
        rate = rates[(item.year, item.month)]
        value_usd = item.value_usd
        value_inr = item.value_inr
        if rate is not None and value_inr is None and value_usd is not None:
            value_inr = value_usd * rate
        if rate is not None and value_usd is None and value_inr is not None:
            value_usd = value_inr / rate
        monthly_rows.append(
            (
                item.hs_code,
                item.year,
                item.month,
                value_usd,
                value_inr,
                rate,
                item.qty,
                item.net_weight,
                item.qty_unit_code,
                item.qty_unit,
                item.partner_country or ("W00" if item.is_aggregate else "UNKNOWN"),
                item.source,
                item.is_aggregate,
            )
        )

    inserted = db.bulk_insert_monthly(conn, monthly_rows)
    return len(products), inserted


def run(
    conn,
    *,
    from_period: str,
    to_period: str,
    catalog_products: Optional[Sequence[CatalogProduct]] = None,
    sync_catalog: bool = True,
    cap_to_available: bool = True,
) -> Dict[str, object]:
    catalogue = list(catalog_products or fetch_product_catalog())
    catalog_count = sync_product_catalog(conn, catalogue) if sync_catalog else len(catalogue)

    availability: Optional[Dict[str, object]] = None
    effective_from, effective_to = from_period, to_period
    if cap_to_available:
        availability = latest_available_period()
        effective_from, effective_to = _cap_to_available(
            from_period,
            to_period,
            availability,
        )

    records = fetch_range(
        effective_from,
        effective_to,
        catalog_products=catalogue,
    )
    if not records:
        raise ComtradeError(
            f"Comtrade returned no HS6 facts for {effective_from} through {effective_to}"
        )
    minimum_products = max(
        int(os.getenv("COMTRADE_MIN_PRODUCTS_PER_PERIOD", "1000")),
        1,
    )
    products_by_period: Dict[str, set[str]] = {}
    for record in records:
        period = f"{record.year:04d}{record.month:02d}"
        products_by_period.setdefault(period, set()).add(record.hs_code)
    incomplete = {
        period: len(products_by_period.get(period, set()))
        for period in _build_periods(effective_from, effective_to)
        if len(products_by_period.get(period, set())) < minimum_products
    }
    if incomplete:
        detail = ", ".join(
            f"{period}={count}" for period, count in incomplete.items()
        )
        raise ComtradeError(
            "Comtrade fact load failed the per-period completeness floor "
            f"({minimum_products} products): {detail}"
        )
    db.delete_comtrade_aggregate_periods(
        conn,
        _build_periods(effective_from, effective_to),
    )
    products, monthly_rows = load(conn, records)
    return {
        "catalog_products": catalog_count,
        "products_upserted": products,
        "monthly_imports_upserted": monthly_rows,
        "requested_from": from_period,
        "requested_to": to_period,
        "effective_from": effective_from,
        "effective_to": effective_to,
        "latest_available": availability,
        "mode": "keyed" if _api_key() else "preview",
        "source": "comtrade",
    }
