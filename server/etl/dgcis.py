"""Placeholders for DGCI&S data ingestion."""
from __future__ import annotations

from typing import Iterable, Mapping


def ingest_csv(_: Iterable[Mapping[str, str]]) -> None:
    """Placeholder for authenticated DGCI&S CSV ingestion.

    TODO: Implement once access flow is defined. Keep the function so that
    agents may plug in later without touching API routes.
    """

    raise NotImplementedError("DGCI&S ingestion is not implemented yet")


def configure_auth(_: str) -> None:
    """Placeholder for setting up auth credentials for DGCI&S."""

    raise NotImplementedError("DGCI&S auth configuration is not implemented yet")
