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
    "agrifood": [r"\bfood\b", r"\bfruit\b", r"\bvegetable\b", r"\bgrain\b"],
    "chemicals": [r"\bchemical\b", r"\bpharmaceutical\b", r"\bmedicament\b"],
    "materials": [r"\bplastic\b", r"\brubber\b", r"\bpaper\b", r"\bglass\b"],
    "textiles": [r"\btextile\b", r"\bfabric\b", r"\bgarment\b"],
    "metals": [r"\bsteel\b", r"\baluminium\b", r"\bmetal\b"],
    "machinery": [r"\bmachinery\b", r"\breactor\b", r"\bpump\b", r"\bcompressor\b"],
    "electronics": [r"\bsemiconductor\b", r"\bcircuit\b", r"\bchip\b", r"\bconverter\b", r"\bled\b"],
    "mobility": [r"\bvehicle\b", r"\bautomotive\b", r"\baircraft\b", r"\bvessel\b"],
    "energy": [
        r"\bsolar\b",
        r"\bphotovoltaic\b",
        r"\bwind turbine\b",
        r"\bpower generation\b",
        r"\bbatter(?:y|ies)\b",
        r"\baccumulators?\b",
    ],
    "instruments": [r"\banaly[sz]er\b", r"\binstrument\b", r"\bmeter\b", r"\bsensor\b"],
    "consumer": [r"\bfurniture\b", r"\bfootwear\b", r"\btoy\b"],
}


def _chapter_sectors(hs_code: Optional[str]) -> List[str]:
    """Return stable sector groups from the official HS chapter structure."""

    canonical = canonical_hs_code(hs_code or "")
    if not canonical:
        return []
    chapter = int(canonical[:2])
    if 1 <= chapter <= 24:
        return ["agrifood"]
    if 25 <= chapter <= 26:
        return ["materials"]
    if chapter == 27:
        return ["energy"]
    if 28 <= chapter <= 38:
        return ["chemicals"]
    if 39 <= chapter <= 49:
        return ["materials"]
    if 50 <= chapter <= 63:
        return ["textiles"]
    if 64 <= chapter <= 67:
        return ["consumer"]
    if 68 <= chapter <= 70:
        return ["materials"]
    if chapter == 71:
        return ["metals"]
    if 72 <= chapter <= 83:
        return ["metals"]
    if chapter == 84:
        if canonical.startswith("8471"):
            return ["electronics"]
        if canonical.startswith("8486"):
            return ["machinery", "electronics"]
        return ["machinery"]
    if chapter == 85:
        return ["electronics"]
    if 86 <= chapter <= 89:
        return ["mobility"]
    if 90 <= chapter <= 92:
        return ["instruments"]
    if chapter == 93:
        return ["controlled"]
    if 94 <= chapter <= 99:
        return ["consumer"]
    return []


def infer_sectors(
    *text_blocks: Sequence[str],
    hs_code: Optional[str] = None,
) -> List[str]:
    """Return coarse, deduplicated sectors without substring false positives.

    HS chapter groups are the primary taxonomy when a code is available.
    Whole-word keyword patterns are a fallback and can add a cross-cutting
    energy tag for products such as batteries in the electrical chapter.
    """

    snippets: List[str] = []
    for block in text_blocks:
        if block is None:
            continue
        if isinstance(block, (list, tuple, set)):
            snippets.extend(str(item) for item in block if item)
        else:
            snippets.append(str(block))
    haystack = " ".join(snippets).lower()
    sectors = _chapter_sectors(hs_code)
    if sectors == ["metals"] and hs_code and str(hs_code).startswith("71"):
        if re.search(r"\b(jewellery|goldsmiths' wares|silversmiths' wares|ornament)\b", haystack):
            sectors = ["consumer"]
    for sector, patterns in _SECTOR_KEYWORDS.items():
        if sectors and sector != "energy":
            continue
        if any(re.search(pattern, haystack) for pattern in patterns):
            if sector not in sectors:
                sectors.append(sector)
    if not sectors:
        sectors.append("materials")
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
