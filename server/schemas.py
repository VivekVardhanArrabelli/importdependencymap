"""Pydantic schemas used for request and response validation."""
from __future__ import annotations

from typing import List, Optional

from pydantic import BaseModel, Field


class ProductFilter(BaseModel):
    sectors: Optional[List[str]] = Field(default=None)
    combine: str = Field(default="OR")
    min_capex: Optional[float] = Field(default=None, ge=0)
    max_capex: Optional[float] = Field(default=None, ge=0)
    sort: str = Field(default="opportunity")
    limit: int = Field(default=100, ge=1, le=500)


class DomesticCapabilityPayload(BaseModel):
    hs_code: str
    capex_min: Optional[float] = None
    capex_max: Optional[float] = None
    machines: Optional[dict] = None
    skills: Optional[dict] = None
    notes: Optional[str] = None
    source: Optional[str] = None


class CapabilityResponse(BaseModel):
    id: int
    hs_code: str
    capex_min: Optional[float] = None
    capex_max: Optional[float] = None
    machines: Optional[dict] = None
    skills: Optional[dict] = None
    notes: Optional[str] = None
    source: Optional[str] = None
    verified: bool


class ProductCard(BaseModel):
    hs_code: str
    title: str
    sectors: List[str]
    capex_min: Optional[float] = None
    capex_max: Optional[float] = None
    last_12m_value_usd: Optional[float] = None
    reduction_pct: Optional[float] = None
    opportunity_score: Optional[float] = None
    last_updated: Optional[str] = None


class ProductDetail(BaseModel):
    product: ProductCard
    baseline_period: Optional[str] = None
    timeseries: List[dict]
    partners: List[dict]
    progress: Optional[dict] = None
