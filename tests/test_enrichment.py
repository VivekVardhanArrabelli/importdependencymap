"""Tests for the curated opportunity-route registry and API."""
from __future__ import annotations

import copy
import os
import unittest
from collections import Counter
from unittest import mock

from fastapi import HTTPException

from server import enrichment, main


REQUIRED_HS_CODES = {
    "853710",
    "853690",
    "842139",
    "847150",
    "847149",
    "850760",
    "850440",
    "850300",
    "401693",
    "401699",
    "940320",
    "200819",
    "190590",
    "230990",
    "640411",
    "640399",
    "470710",
    "470790",
    "440890",
    "392010",
    "400510",
    "691490",
    "854370",
    "841989",
    "700719",
    "711319",
    "152000",
    "843351",
}


class OpportunityRouteRegistryTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.registry = enrichment.load_opportunity_routes()

    def test_default_registry_is_complete_and_bounded(self):
        self.assertEqual(set(self.registry), {"version", "routes", "count"})
        self.assertEqual(self.registry["version"], "1.0.0")
        self.assertEqual(self.registry["count"], len(self.registry["routes"]))
        self.assertGreaterEqual(self.registry["count"], 25)
        self.assertLessEqual(self.registry["count"], 35)

        hs_counts = Counter(route["hs_code"] for route in self.registry["routes"])
        self.assertTrue(REQUIRED_HS_CODES.issubset(hs_counts))
        self.assertLessEqual(max(hs_counts.values()), 2)
        self.assertEqual(hs_counts["842139"], 2)
        self.assertEqual(hs_counts["847150"], 2)
        self.assertEqual(hs_counts["850760"], 2)

    def test_research_and_enabling_routes_are_labeled_conservatively(self):
        glass_routes = [
            route
            for route in self.registry["routes"]
            if route["hs_code"] == "700719"
        ]
        jewelry_routes = [
            route
            for route in self.registry["routes"]
            if route["hs_code"] == "711319"
        ]
        self.assertTrue(glass_routes)
        self.assertTrue(jewelry_routes)
        self.assertTrue(
            all(route["impact_type"] == "enabling" for route in glass_routes)
        )
        self.assertTrue(
            all(route["confidence"] == "research_only" for route in glass_routes)
        )
        self.assertTrue(
            all(route["confidence"] == "research_only" for route in jewelry_routes)
        )
        self.assertTrue(
            all(
                "missing net trade" in route["source_note"].lower().replace("-", " ")
                for route in jewelry_routes
            )
        )

    def test_capital_disclaimer_is_present_on_every_route(self):
        for route in self.registry["routes"]:
            note = " ".join(route["source_note"].lower().split())
            self.assertIn("modeled validation/entry envelope", note)
            self.assertIn("never verified plant capex", note)

    def test_validation_rejects_more_than_two_routes_for_an_hs_code(self):
        base = copy.deepcopy(self.registry["routes"][0])
        routes = []
        for index in range(3):
            route = copy.deepcopy(base)
            route["route_id"] = f"duplicate-limit-{index}"
            routes.append(route)

        with self.assertRaisesRegex(enrichment.RouteRegistryError, "at most 2"):
            enrichment.validate_opportunity_registry(
                {"version": "test", "routes": routes}
            )

    def test_validation_rejects_invalid_route_values(self):
        mutations = {
            "boolean capital": lambda route: route.update(capital_min_inr=True),
            "unknown strength": lambda route: route.update(
                required_strengths=["marketing"]
            ),
            "too few unknowns": lambda route: route.update(
                critical_unknowns=["one", "two"]
            ),
            "out-of-range complexity": lambda route: route["complexity"].update(
                capital=6
            ),
            "unexpected field": lambda route: route.update(typo="invalid"),
        }
        for label, mutate in mutations.items():
            with self.subTest(label=label):
                route = copy.deepcopy(self.registry["routes"][0])
                mutate(route)
                with self.assertRaises(enrichment.RouteRegistryError):
                    enrichment.validate_opportunity_registry(
                        {"version": "test", "routes": [route]}
                    )

    def test_endpoint_works_without_database_configuration(self):
        with mock.patch.dict(os.environ):
            os.environ.pop("DATABASE_URL", None)
            result = main.opportunity_routes()
        self.assertEqual(result, self.registry)

    def test_endpoint_hides_registry_read_errors(self):
        with mock.patch.object(
            enrichment,
            "load_opportunity_routes",
            side_effect=enrichment.RouteRegistryError("private path"),
        ):
            with self.assertRaises(HTTPException) as caught:
                main.opportunity_routes()
        self.assertEqual(caught.exception.status_code, 500)
        self.assertEqual(
            caught.exception.detail,
            "Opportunity route registry unavailable",
        )


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
