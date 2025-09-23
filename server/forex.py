"""Utilities for working with monthly foreign exchange rates."""
from __future__ import annotations

import csv
import os
from functools import lru_cache
from pathlib import Path
from typing import Dict, Tuple

LOGGER_NAME = "buildforindia.forex"


def _rates_file() -> Path:
    path = Path(os.getenv("FX_RATES_FILE", "data/fx_rates.csv"))
    if not path.exists():
        raise RuntimeError(f"FX rates file not found: {path}")
    return path


@lru_cache(maxsize=8)
def _load_rates(resolved_path: str) -> Dict[Tuple[int, int], float]:
    table: Dict[Tuple[int, int], float] = {}
    with Path(resolved_path).open("r", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        required = {"year", "month", "usd_to_inr"}
        missing = required - set(reader.fieldnames or [])
        if missing:
            raise RuntimeError(f"FX rates file missing columns: {', '.join(sorted(missing))}")
        for row in reader:
            try:
                year = int(row["year"])
                month = int(row["month"])
                rate = float(row["usd_to_inr"])
            except (TypeError, ValueError) as exc:
                raise RuntimeError(f"Invalid FX rate row: {row}") from exc
            table[(year, month)] = rate
    if not table:
        raise RuntimeError("FX rates file is empty")
    return table


def monthly_rate(year: int, month: int) -> float:
    """Return the USD → INR rate for the given year/month."""

    if not (1 <= int(month) <= 12):
        raise RuntimeError(f"Invalid month for FX rate: {month}")
    resolved = str(_rates_file().resolve())
    rates = _load_rates(resolved)
    key = (int(year), int(month))
    if key not in rates:
        raise RuntimeError(f"FX rate missing for {year}-{int(month):02d}")
    return rates[key]


def reset_cache() -> None:
    """Clear cached FX data (useful for tests)."""

    _load_rates.cache_clear()
