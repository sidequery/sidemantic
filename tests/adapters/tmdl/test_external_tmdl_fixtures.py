from __future__ import annotations

import json
from collections import Counter
from pathlib import Path

import pytest

from sidemantic.adapters.tmdl import TMDLAdapter
from sidemantic.core.introspection import describe_graph

ROOT = Path(__file__).resolve().parents[2]
FIXTURE_ROOT = ROOT / "fixtures" / "external_powerbi"

TMDL_FIXTURES = [
    pytest.param("microsoft-analysis-services-sales", 11, 29, 5, 1, {}, id="analysis-services"),
    pytest.param("microsoft-fabric-samples-bank-customer-churn", 1, 4, 0, 0, {}, id="fabric-samples"),
    pytest.param("pbi-tools-adventureworks-dw2020", 7, 0, 8, 2, {}, id="adventureworks"),
    pytest.param("pbip-lineage-explorer-sample", 6, 7, 3, 0, {}, id="pbip-lineage"),
    pytest.param("ruiromano-pbip-demo-agentic-model01", 4, 15, 4, 1, {}, id="pbip-demo-agentic"),
]


@pytest.mark.parametrize(
    ("fixture_name", "expected_models", "expected_metrics", "expected_relationships", "expected_inactive", "warnings"),
    TMDL_FIXTURES,
)
def test_external_powerbi_tmdl_fixtures_parse(
    fixture_name: str,
    expected_models: int,
    expected_metrics: int,
    expected_relationships: int,
    expected_inactive: int,
    warnings: dict[str, int],
):
    graph = TMDLAdapter().parse(FIXTURE_ROOT / fixture_name)

    assert len(graph.models) == expected_models
    assert sum(len(model.metrics) for model in graph.models.values()) == expected_metrics
    assert sum(len(model.relationships) for model in graph.models.values()) == expected_relationships
    assert (
        sum(1 for model in graph.models.values() for rel in model.relationships if not rel.active) == expected_inactive
    )
    assert Counter(warning["code"] for warning in getattr(graph, "import_warnings", [])) == warnings

    description = describe_graph(graph)
    json.dumps(description)
    assert {model["source_format"] for model in description["models"]} == {"TMDL"}


@pytest.mark.parametrize("fixture_name", [fixture.values[0] for fixture in TMDL_FIXTURES])
def test_external_powerbi_fixtures_include_upstream_license(fixture_name: str):
    license_text = (FIXTURE_ROOT / fixture_name / "LICENSE.upstream").read_text()
    assert "MIT License" in license_text
