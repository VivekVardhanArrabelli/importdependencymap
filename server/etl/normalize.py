"""Normalization utilities shared across ETL jobs."""
from __future__ import annotations

import re
from typing import Iterable, List, Optional, Sequence

HS_PATTERN = re.compile(r"\d+")


def canonical_hs_code(raw: str, granularity: int = 6) -> Optional[str]:
    """Normalize HS codes to the requested granularity (default HS6)."""

    if not raw:
        return None
    digits = "".join(HS_PATTERN.findall(str(raw)))
    if not digits:
        return None
    digits = digits.ljust(granularity, "0")
    return digits[:granularity]


_SECTOR_KEYWORDS = {
    "electronics": ["semiconductor", "circuit", "chip", "battery", "converter", "led"],
    "industrial": ["machinery", "reactor", "pump", "compressor", "industrial"],
    "automotive": ["vehicle", "automotive", "motor", "engine"],
    "metals": ["steel", "valve", "fitting", "aluminium", "metal"],
    "energy": ["energy", "solar", "battery", "power"] ,
    "instruments": ["analyzer", "instrument", "meter", "sensor"],
}


def infer_sectors(*text_blocks: Sequence[str]) -> List[str]:
    """Return a deduplicated sector list based on keyword heuristics."""

    snippets: List[str] = []
    for block in text_blocks:
        if block is None:
            continue
        if isinstance(block, (list, tuple, set)):
            snippets.extend(str(item) for item in block if item)
        else:
            snippets.append(str(block))
    haystack = " ".join(snippets).lower()
    sectors: List[str] = []
    for sector, keywords in _SECTOR_KEYWORDS.items():
        if any(keyword in haystack for keyword in keywords):
            sectors.append(sector)
    if not sectors:
        sectors.append("industrial")
    return sectors


def parse_csv_sectors(raw: str) -> List[str]:
    if not raw:
        return []
    cleaned = raw.strip()
    if cleaned.startswith("{") and cleaned.endswith("}"):
        cleaned = cleaned[1:-1]
    return [segment.strip() for segment in cleaned.split(",") if segment.strip()]


def ensure_usd(value: Optional[float]) -> Optional[float]:
    """Placeholder for currency conversion when data already in USD."""

    return value
