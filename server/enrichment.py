"""Validated loader for the curated opportunity-route registry."""
from __future__ import annotations

import json
import re
from collections import Counter
from datetime import date
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Set


DEFAULT_REGISTRY_PATH = (
    Path(__file__).resolve().parent.parent
    / "data"
    / "opportunity_routes.v1.json"
)

MAX_REGISTRY_ROUTES = 400
MAX_ROUTES_PER_HS_CODE = 2

ROUTE_FIELDS: Set[str] = {
    "route_id",
    "hs_code",
    "business_name",
    "entry_model",
    "wedge",
    "includes",
    "excludes",
    "capital_min_inr",
    "capital_max_inr",
    "months_min",
    "months_max",
    "buyer",
    "first_proof",
    "critical_unknowns",
    "required_strengths",
    "buyer_types",
    "complexity",
    "impact_type",
    "confidence",
    "source_note",
    "reviewed_at",
}

ENTRY_MODE_VALUES = {"make", "assemble", "contract", "enable"}
REQUIRED_STRENGTH_VALUES = {
    "buyer_access",
    "engineering",
    "operations",
    "distribution",
    "sourcing",
    "quality",
    "workshop",
    "land_power",
}
BUYER_TYPE_VALUES = {"factories", "oems", "government", "consumer", "export"}
IMPACT_TYPE_VALUES = {"direct", "enabling", "research"}
CONFIDENCE_VALUES = {"curated_model", "research_only"}
COMPLEXITY_FIELDS = {
    "capital",
    "technical",
    "regulatory",
    "supply_chain",
    "buyer_qualification",
}

_HS_CODE_PATTERN = re.compile(r"^[0-9]{6}$")
_ROUTE_ID_PATTERN = re.compile(r"^[a-z0-9][a-z0-9:_-]{2,100}$")


class RouteRegistryError(RuntimeError):
    """Raised when the curated opportunity-route registry is invalid."""


def _require_nonempty_string(value: Any, field: str, route_label: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise RouteRegistryError(
            f"{route_label}: {field} must be a non-empty string"
        )
    return value.strip()


def _require_integer(value: Any, field: str, route_label: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise RouteRegistryError(f"{route_label}: {field} must be an integer")
    return value


def _validate_string_list(
    value: Any,
    field: str,
    route_label: str,
    *,
    min_items: int = 1,
    max_items: Optional[int] = None,
    allowed_values: Optional[Set[str]] = None,
) -> List[str]:
    if not isinstance(value, list):
        raise RouteRegistryError(f"{route_label}: {field} must be a list")
    if len(value) < min_items or (max_items is not None and len(value) > max_items):
        expected = (
            f"between {min_items} and {max_items}"
            if max_items is not None
            else f"at least {min_items}"
        )
        raise RouteRegistryError(
            f"{route_label}: {field} must contain {expected} items"
        )

    normalized = [
        _require_nonempty_string(item, field, route_label) for item in value
    ]
    if len(set(normalized)) != len(normalized):
        raise RouteRegistryError(f"{route_label}: {field} contains duplicates")
    if allowed_values is not None:
        invalid = sorted(set(normalized) - allowed_values)
        if invalid:
            raise RouteRegistryError(
                f"{route_label}: {field} contains invalid values: {invalid}"
            )
    return normalized


def _validate_route(raw_route: Any, index: int) -> Dict[str, Any]:
    route_label = f"route[{index}]"
    if not isinstance(raw_route, Mapping):
        raise RouteRegistryError(f"{route_label} must be an object")

    actual_fields = set(raw_route.keys())
    missing = sorted(ROUTE_FIELDS - actual_fields)
    unexpected = sorted(actual_fields - ROUTE_FIELDS)
    if missing or unexpected:
        raise RouteRegistryError(
            f"{route_label} has invalid fields; missing={missing}, unexpected={unexpected}"
        )

    route_id = _require_nonempty_string(
        raw_route["route_id"], "route_id", route_label
    )
    route_label = f"route {route_id!r}"
    if not _ROUTE_ID_PATTERN.fullmatch(route_id):
        raise RouteRegistryError(
            f"{route_label}: route_id must use lowercase letters, digits, colon, dash, or underscore"
        )

    hs_code = _require_nonempty_string(
        raw_route["hs_code"], "hs_code", route_label
    )
    if not _HS_CODE_PATTERN.fullmatch(hs_code):
        raise RouteRegistryError(f"{route_label}: hs_code must be exactly six digits")

    entry_model = _require_nonempty_string(
        raw_route["entry_model"], "entry_model", route_label
    )
    if entry_model not in ENTRY_MODE_VALUES:
        raise RouteRegistryError(
            f"{route_label}: entry_model must be one of {sorted(ENTRY_MODE_VALUES)}"
        )

    capital_min = _require_integer(
        raw_route["capital_min_inr"], "capital_min_inr", route_label
    )
    capital_max = _require_integer(
        raw_route["capital_max_inr"], "capital_max_inr", route_label
    )
    if capital_min < 0 or capital_max < capital_min:
        raise RouteRegistryError(
            f"{route_label}: capital values must satisfy 0 <= minimum <= maximum"
        )

    months_min = _require_integer(
        raw_route["months_min"], "months_min", route_label
    )
    months_max = _require_integer(
        raw_route["months_max"], "months_max", route_label
    )
    if months_min < 1 or months_max < months_min or months_max > 120:
        raise RouteRegistryError(
            f"{route_label}: months values must satisfy 1 <= minimum <= maximum <= 120"
        )

    complexity = raw_route["complexity"]
    if not isinstance(complexity, Mapping):
        raise RouteRegistryError(f"{route_label}: complexity must be an object")
    complexity_fields = set(complexity.keys())
    if complexity_fields != COMPLEXITY_FIELDS:
        raise RouteRegistryError(
            f"{route_label}: complexity must contain exactly {sorted(COMPLEXITY_FIELDS)}"
        )
    normalized_complexity: Dict[str, int] = {}
    for field in sorted(COMPLEXITY_FIELDS):
        score = _require_integer(complexity[field], f"complexity.{field}", route_label)
        if score < 1 or score > 5:
            raise RouteRegistryError(
                f"{route_label}: complexity.{field} must be between 1 and 5"
            )
        normalized_complexity[field] = score

    impact_type = _require_nonempty_string(
        raw_route["impact_type"], "impact_type", route_label
    )
    if impact_type not in IMPACT_TYPE_VALUES:
        raise RouteRegistryError(
            f"{route_label}: impact_type must be one of {sorted(IMPACT_TYPE_VALUES)}"
        )
    confidence = _require_nonempty_string(
        raw_route["confidence"], "confidence", route_label
    )
    if confidence not in CONFIDENCE_VALUES:
        raise RouteRegistryError(
            f"{route_label}: confidence must be one of {sorted(CONFIDENCE_VALUES)}"
        )

    reviewed_at = _require_nonempty_string(
        raw_route["reviewed_at"], "reviewed_at", route_label
    )
    try:
        parsed_reviewed_at = date.fromisoformat(reviewed_at)
    except ValueError as exc:
        raise RouteRegistryError(
            f"{route_label}: reviewed_at must be an ISO date"
        ) from exc
    if parsed_reviewed_at.isoformat() != reviewed_at:
        raise RouteRegistryError(
            f"{route_label}: reviewed_at must use YYYY-MM-DD format"
        )

    source_note = _require_nonempty_string(
        raw_route["source_note"], "source_note", route_label
    )
    normalized_source_note = " ".join(source_note.lower().split())
    required_disclaimers = (
        "modeled validation/entry envelope",
        "never verified plant capex",
    )
    if not all(phrase in normalized_source_note for phrase in required_disclaimers):
        raise RouteRegistryError(
            f"{route_label}: source_note must identify the modeled envelope and state that it is never verified plant CAPEX"
        )

    normalized: Dict[str, Any] = {
        "route_id": route_id,
        "hs_code": hs_code,
        "business_name": _require_nonempty_string(
            raw_route["business_name"], "business_name", route_label
        ),
        "entry_model": entry_model,
        "wedge": _require_nonempty_string(raw_route["wedge"], "wedge", route_label),
        "includes": _require_nonempty_string(
            raw_route["includes"], "includes", route_label
        ),
        "excludes": _require_nonempty_string(
            raw_route["excludes"], "excludes", route_label
        ),
        "capital_min_inr": capital_min,
        "capital_max_inr": capital_max,
        "months_min": months_min,
        "months_max": months_max,
        "buyer": _require_nonempty_string(raw_route["buyer"], "buyer", route_label),
        "first_proof": _require_nonempty_string(
            raw_route["first_proof"], "first_proof", route_label
        ),
        "critical_unknowns": _validate_string_list(
            raw_route["critical_unknowns"],
            "critical_unknowns",
            route_label,
            min_items=3,
            max_items=6,
        ),
        "required_strengths": _validate_string_list(
            raw_route["required_strengths"],
            "required_strengths",
            route_label,
            allowed_values=REQUIRED_STRENGTH_VALUES,
        ),
        "buyer_types": _validate_string_list(
            raw_route["buyer_types"],
            "buyer_types",
            route_label,
            allowed_values=BUYER_TYPE_VALUES,
        ),
        "complexity": normalized_complexity,
        "impact_type": impact_type,
        "confidence": confidence,
        "source_note": source_note,
        "reviewed_at": reviewed_at,
    }
    return normalized


def validate_opportunity_registry(payload: Any) -> Dict[str, Any]:
    """Validate and normalize an in-memory registry payload."""

    if not isinstance(payload, Mapping):
        raise RouteRegistryError("registry must be a JSON object")
    actual_fields = set(payload.keys())
    if actual_fields != {"version", "routes"}:
        raise RouteRegistryError(
            "registry must contain exactly the top-level fields 'version' and 'routes'"
        )

    version = _require_nonempty_string(payload["version"], "version", "registry")
    raw_routes = payload["routes"]
    if not isinstance(raw_routes, list):
        raise RouteRegistryError("registry routes must be a list")
    if not raw_routes:
        raise RouteRegistryError("registry routes must not be empty")
    if len(raw_routes) > MAX_REGISTRY_ROUTES:
        raise RouteRegistryError(
            f"registry cannot contain more than {MAX_REGISTRY_ROUTES} routes"
        )

    routes = [_validate_route(route, index) for index, route in enumerate(raw_routes)]
    route_ids = [route["route_id"] for route in routes]
    if len(set(route_ids)) != len(route_ids):
        duplicates = sorted(
            route_id for route_id, count in Counter(route_ids).items() if count > 1
        )
        raise RouteRegistryError(f"duplicate route_id values: {duplicates}")

    hs_counts = Counter(route["hs_code"] for route in routes)
    over_limit = sorted(
        hs_code
        for hs_code, count in hs_counts.items()
        if count > MAX_ROUTES_PER_HS_CODE
    )
    if over_limit:
        raise RouteRegistryError(
            f"at most {MAX_ROUTES_PER_HS_CODE} routes are allowed per HS code: {over_limit}"
        )

    return {"version": version, "routes": routes, "count": len(routes)}


def load_opportunity_routes(path: Optional[Path] = None) -> Dict[str, Any]:
    """Load the versioned registry from disk and return its API representation."""

    registry_path = path or DEFAULT_REGISTRY_PATH
    try:
        with registry_path.open("r", encoding="utf-8") as handle:
            payload = json.load(handle)
    except FileNotFoundError as exc:
        raise RouteRegistryError(
            f"opportunity-route registry not found: {registry_path}"
        ) from exc
    except (OSError, json.JSONDecodeError) as exc:
        raise RouteRegistryError(
            f"opportunity-route registry could not be read: {registry_path}"
        ) from exc
    return validate_opportunity_registry(payload)
