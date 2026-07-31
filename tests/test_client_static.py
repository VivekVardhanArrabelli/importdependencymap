"""Static guardrails for the no-build browser client."""
from __future__ import annotations

import re
from html.parser import HTMLParser
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
INDEX = (ROOT / "client" / "index.html").read_text(encoding="utf-8")
APP = (ROOT / "client" / "app.js").read_text(encoding="utf-8")


class _IdCollector(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.ids: list[str] = []

    def handle_starttag(self, _tag: str, attrs: list[tuple[str, str | None]]) -> None:
        for name, value in attrs:
            if name == "id" and value:
                self.ids.append(value)


def test_client_has_accessible_operating_views_and_dossier() -> None:
    assert 'id="view-discover"' in INDEX
    assert 'id="view-portfolio"' in INDEX
    assert 'id="view-method"' in INDEX
    assert 'role="dialog"' in INDEX
    assert 'aria-modal="true"' in INDEX
    assert 'aria-labelledby="dossierTitle"' in INDEX
    assert 'aria-describedby="dossierDescription"' in INDEX


def test_client_uses_delegated_events_instead_of_inline_handlers() -> None:
    assert re.search(r"\son[a-z]+\s*=", INDEX, flags=re.IGNORECASE) is None
    assert "data-action" in INDEX
    assert "handleDocumentClick" in APP


def test_local_model_loads_before_the_application() -> None:
    model_position = INDEX.index("/static/opportunity-model.js")
    app_position = INDEX.index("/static/app.js")
    assert model_position < app_position
    assert (ROOT / "client" / "opportunity-model.js").exists()


def test_client_has_no_remote_runtime_script_or_font_dependency() -> None:
    assert re.search(r'<script[^>]+src="https?://', INDEX, flags=re.IGNORECASE) is None
    assert "fonts.googleapis.com" not in INDEX
    assert "Chart.js" not in INDEX
    assert "new Chart" not in APP


def test_static_document_ids_are_unique() -> None:
    parser = _IdCollector()
    parser.feed(INDEX)
    assert len(parser.ids) == len(set(parser.ids))


def test_screening_guardrails_are_visible() -> None:
    assert "Not investment advice" in INDEX
    assert "not projected revenue" in INDEX
    assert "Gross imports do not" in INDEX
    assert "Controlled goods stay out of default founder results" in INDEX
    assert "Aircraft and vessels carry a strategic-asset gate" in INDEX
    assert "excluded from ranking because project CAPEX coverage is not yet systematic" in INDEX
    assert "CAPEX fit unverified" in APP
    assert "not achievable market share, revenue, or expected return" in APP


def test_csv_exports_neutralize_spreadsheet_formula_prefixes() -> None:
    assert "Model.spreadsheetSafeText(value)" in APP
