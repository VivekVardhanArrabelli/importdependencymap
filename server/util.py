"""Utility helpers for analytics math and normalization."""
from __future__ import annotations

import math
from typing import Dict, Iterable, List, Tuple


def norm_log(values: Dict[str, float] | Iterable[Tuple[str, float]]) -> Dict[str, float]:
    """Return log1p normalized values between 0 and 1.

    The function accepts either a mapping of identifiers to numeric values or
    an iterable of ``(identifier, value)`` tuples. ``log1p`` is used to compress
    the range while keeping ordering. When the set is empty or the range is
    constant the values default to ``0.0`` to avoid division errors.
    """

    if isinstance(values, dict):
        items = list(values.items())
    else:
        items = list(values)
    if not items:
        return {}

    logs = [(key, math.log1p(float(val)) if val is not None else 0.0) for key, val in items]
    log_values = [val for _, val in logs]
    min_val = min(log_values)
    max_val = max(log_values)
    span = max_val - min_val
    if span == 0:
        return {key: 0.0 for key, _ in logs}
    return {key: (val - min_val) / span for key, val in logs}


def hhi_from_shares(shares: List[float]) -> float | None:
    """Calculate the Herfindahl-Hirschman Index from partner shares.

    Empty inputs return ``None``. The function expects fractional shares that
    sum to roughly 1.0. The index is the sum of the squared shares.
    """

    if not shares:
        return None
    return sum((float(share) if share is not None else 0.0) ** 2 for share in shares)


SECTOR_TECH_FEASIBILITY = {
    "electronics": 0.7,
    "industrial": 0.6,
    "automotive": 0.5,
    "metals": 0.65,
    "energy": 0.6,
    "instruments": 0.65,
}


def tech_feasibility_for(sectors: Iterable[str] | None) -> float:
    """Return a heuristic feasibility score for a product's sectors."""

    if not sectors:
        return 0.6
    best = 0.0
    for sector in sectors:
        score = SECTOR_TECH_FEASIBILITY.get(sector.lower(), 0.6)
        best = max(best, score)
    return best or 0.6
