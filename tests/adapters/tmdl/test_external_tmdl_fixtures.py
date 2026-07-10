from __future__ import annotations

import json
from collections import Counter
from pathlib import Path

import pytest

from sidemantic.adapters.tmdl import TMDLAdapter
from sidemantic.core.introspection import describe_graph

ROOT = Path(__file__).resolve().parents[2]
FIXTURE_ROOT = ROOT / "fixtures" / "external_powerbi"

# Per fixture: models, metrics, relationships, inactive rels, always-present warnings,
# dax_parser_unavailable count (when the parser is absent), dax_not_translated count (when the
# parser is present and a measure's DAX cannot reduce to a native aggregation).
TMDL_FIXTURES = [
    pytest.param("microsoft-analysis-services-sales", 11, 29, 5, 1, {}, 30, 22, id="analysis-services"),
    pytest.param("microsoft-fabric-samples-bank-customer-churn", 1, 4, 0, 0, {}, 4, 2, id="fabric-samples"),
    pytest.param("pbi-tools-adventureworks-dw2020", 7, 0, 8, 2, {}, 0, 0, id="adventureworks"),
    pytest.param("pbip-lineage-explorer-sample", 6, 7, 3, 0, {}, 8, 3, id="pbip-lineage"),
    pytest.param("ruiromano-pbip-demo-agentic-model01", 4, 15, 4, 1, {}, 15, 9, id="pbip-demo-agentic"),
]


def _dax_parser_available() -> bool:
    try:
        from sidemantic_dax import parse_expression
    except Exception:
        return False

    try:
        parse_expression("SUM(Sales[Amount])")
    except Exception:
        return False

    return True


@pytest.mark.parametrize(
    (
        "fixture_name",
        "expected_models",
        "expected_metrics",
        "expected_relationships",
        "expected_inactive",
        "warnings",
        "expected_dax_unavailable_warnings",
        "expected_dax_not_translated",
    ),
    TMDL_FIXTURES,
)
def test_external_powerbi_tmdl_fixtures_parse(
    fixture_name: str,
    expected_models: int,
    expected_metrics: int,
    expected_relationships: int,
    expected_inactive: int,
    warnings: dict[str, int],
    expected_dax_unavailable_warnings: int,
    expected_dax_not_translated: int,
):
    graph = TMDLAdapter().parse(FIXTURE_ROOT / fixture_name)

    assert len(graph.models) == expected_models
    assert sum(len(model.metrics) for model in graph.models.values()) == expected_metrics
    assert sum(len(model.relationships) for model in graph.models.values()) == expected_relationships
    assert (
        sum(1 for model in graph.models.values() for rel in model.relationships if not rel.active) == expected_inactive
    )
    expected_warnings = Counter(warnings)
    if _dax_parser_available():
        if expected_dax_not_translated:
            expected_warnings["dax_not_translated"] = expected_dax_not_translated
    elif expected_dax_unavailable_warnings:
        expected_warnings["dax_parser_unavailable"] = expected_dax_unavailable_warnings
    assert Counter(warning["code"] for warning in getattr(graph, "import_warnings", [])) == expected_warnings

    description = describe_graph(graph)
    json.dumps(description)
    assert {model["source_format"] for model in description["models"]} == {"TMDL"}


@pytest.mark.parametrize("fixture_name", [fixture.values[0] for fixture in TMDL_FIXTURES])
def test_external_powerbi_fixtures_include_upstream_license(fixture_name: str):
    license_text = (FIXTURE_ROOT / fixture_name / "LICENSE.upstream").read_text()
    assert "MIT License" in license_text


def test_adventureworks_roundtrip_preserves_cardinality_and_keys(tmp_path):
    """Regression for the P0 phantom-key and P1 cardinality fixes against a real Power BI model.

    Importing AdventureWorks, then exporting and re-importing it, must:
    (a) resolve a real primary-key column for one-side dimension tables, and never fabricate a key
        (no phantom "id", no many-side foreign key promoted to a primary key) (the phantom-key P0);
    (b) preserve the raw cardinality text -- a cardinality omitted in the source must stay omitted
        on re-export rather than being synthesized (the cardinality P1 / export corruption);
    (c) re-import cleanly with stable relationship types (the export must produce valid TMDL even
        for models with inlined tab-indented culture blocks).
    """
    fixture = FIXTURE_ROOT / "pbi-tools-adventureworks-dw2020"
    graph = TMDLAdapter().parse(fixture)

    # (a) Dimension (one-side) join targets resolve a real key column from their relationships.
    dimension_join_targets = {"Customer", "Date", "Product", "Reseller", "Sales Territory"}
    for name in dimension_join_targets:
        model = graph.models[name]
        columns = {dim.name for dim in model.dimensions}
        assert model.primary_key in columns, f"{name} should resolve a real key, got {model.primary_key!r}"
    # No table is left with a fabricated key: a resolved primary key is always a real column, and a
    # table with no inferable key stays None rather than a phantom "id" or a many-side foreign key
    # promoted to a primary key. "Sales Order" is only ever a relationship's many side, so it has no
    # primary key.
    for model in graph.models.values():
        if model.primary_key is not None:
            columns = {dim.name for dim in model.dimensions}
            assert model.primary_key in columns, f"{model.name} has phantom primary_key {model.primary_key!r}"
    assert graph.models["Sales Order"].primary_key is None

    types_before = sorted((m.name, r.name, r.type) for m in graph.models.values() for r in m.relationships)

    export_dir = tmp_path / "roundtrip"
    TMDLAdapter().export(graph, export_dir)

    # (b) The source declares fromCardinality on one relationship and omits toCardinality entirely;
    # the re-export must not synthesize the omitted cardinality. Compare raw declaration counts.
    src_rel = (fixture / "relationships.tmdl").read_text()
    out_rel = (export_dir / "definition" / "relationships.tmdl").read_text()
    assert out_rel.count("fromCardinality:") == src_rel.count("fromCardinality:")
    assert out_rel.count("toCardinality:") == src_rel.count("toCardinality:")

    # (c) The export must re-import cleanly with stable relationship types.
    reparsed = TMDLAdapter().parse(export_dir)
    types_after = sorted((m.name, r.name, r.type) for m in reparsed.models.values() for r in m.relationships)
    assert types_after == types_before
