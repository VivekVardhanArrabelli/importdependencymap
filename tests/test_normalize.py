"""Tests for HS and sector normalization."""
from server.etl import normalize


def test_chapter_taxonomy_prevents_led_substring_false_positives() -> None:
    assert normalize.infer_sectors(
        "Areca nuts, shelled or peeled",
        hs_code="080280",
    ) == ["agrifood"]
    assert normalize.infer_sectors(
        "Iron or non-alloy steel, in coils",
        hs_code="720836",
    ) == ["metals"]


def test_energy_is_a_cross_cutting_tag_for_electrical_batteries() -> None:
    assert normalize.infer_sectors(
        "Electric accumulators; lithium-ion batteries",
        hs_code="850760",
    ) == ["electronics", "energy"]

    assert normalize.infer_sectors(
        "Wind turbine generating sets",
        hs_code="841280",
    ) == ["machinery", "energy"]


def test_generic_power_wording_does_not_create_false_energy_tags() -> None:
    assert normalize.infer_sectors(
        "Dies for drawing or extruding metal, whether or not power-operated",
        hs_code="820720",
    ) == ["metals"]
    assert normalize.infer_sectors(
        "Agricultural tractors; engine power exceeding 18kW",
        hs_code="870191",
    ) == ["mobility"]


def test_hs_subheadings_refine_broad_chapters() -> None:
    assert normalize.infer_sectors(
        "Automatic data processing machines",
        hs_code="847150",
    ) == ["electronics"]
    assert normalize.infer_sectors(
        "Silver, semi-manufactured",
        hs_code="710692",
    ) == ["metals"]
    assert normalize.infer_sectors(
        "Jewellery of precious metal",
        hs_code="711319",
    ) == ["consumer"]
    assert normalize.infer_sectors(
        "Parts and accessories of revolvers or pistols",
        hs_code="930510",
    ) == ["controlled"]


def test_keyword_fallback_uses_word_boundaries() -> None:
    assert normalize.infer_sectors("LED converters") == ["electronics"]
    assert normalize.infer_sectors("Shelled nuts") == ["materials"]
