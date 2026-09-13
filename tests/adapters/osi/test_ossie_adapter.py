from __future__ import annotations

import json
from pathlib import Path

import pytest
import yaml

from sidemantic.adapters.osi import OSIAdapter
from sidemantic.adapters.ossie import OssieAdapter, OssieImportError
from sidemantic.core.dimension import Dimension
from sidemantic.core.metric import Metric
from sidemantic.core.model import Model
from sidemantic.core.relationship import Relationship
from sidemantic.core.semantic_graph import SemanticGraph
from sidemantic.interchange.ossie import OssieConsumerProfile, OssieSynthesisError

VALID_DOCUMENT = """version: 0.2.0.dev0
semantic_model:
  - name: commerce
    datasets:
      - name: orders
        source: analytics.orders
        primary_key: [id]
        fields:
          - name: id
            expression: {dialects: [{dialect: ANSI_SQL, expression: id}]}
"""


def test_historical_osi_class_spelling_routes_to_canonical_adapter(tmp_path: Path) -> None:
    assert issubclass(OSIAdapter, OssieAdapter)
    assert OSIAdapter.SUPPORTED_EXPORT_DIALECTS == ["ANSI_SQL", "BIGQUERY", "SNOWFLAKE", "DATABRICKS"]
    invalid = tmp_path / "invalid.yaml"
    invalid.write_text("version: 0.2.0.dev0\nsemantic_model: {}\n")
    with pytest.raises(OssieImportError):
        OSIAdapter().parse(invalid)


ONTOLOGY_DOCUMENT = """# ontology source
version: 0.2.0.dev0
name: business_terms
description: Shared business vocabulary
ai_context:
  instructions: Use the business vocabulary when answering questions.
ontology:
  - concept: Customer
    type: EntityType
  - concept: Order
    type: EntityType
ontology_mappings:
  - semantic_model:
      name: commerce
      datasets:
        - name: customers
          source: analytics.customers
    concept_mappings:
      - concept: Customer
"""


def test_parse_document_returns_preserved_source_and_scoped_catalog(tmp_path: Path) -> None:
    source = tmp_path / "commerce.yaml"
    source.write_text(VALID_DOCUMENT)

    result = OssieAdapter(preserve_source=True).parse_document(source)

    assert result.document.source.original_bytes == source.read_bytes()
    assert result.catalog.scope_ids == ("commerce",)
    assert result.catalog["commerce"].graph.get_model("orders").primary_key == "id"


def test_legacy_parse_surface_requires_scope_selection_when_ambiguous(tmp_path: Path) -> None:
    source = tmp_path / "multiple.yaml"
    source.write_text(
        VALID_DOCUMENT
        + """  - name: operations
    datasets:
      - name: orders
        source: operations.orders
        fields: []
"""
    )

    with pytest.raises(OssieImportError, match="Select one explicitly"):
        OssieAdapter().parse(source)

    graph = OssieAdapter(scope_id="operations").parse(source)
    assert graph.get_model("orders").table == "operations.orders"


def test_directory_scopes_are_qualified_and_generated_directories_are_skipped(tmp_path: Path) -> None:
    first = tmp_path / "one.yaml"
    second = tmp_path / "nested" / "two.json"
    generated = tmp_path / "target" / "generated.yaml"
    first.write_text(VALID_DOCUMENT)
    second.parent.mkdir()
    second.write_text(
        '{"version":"0.2.0.dev0","semantic_model":[{"name":"commerce","datasets":'
        '[{"name":"orders","source":"nested.orders","fields":[]}]}]}'
    )
    generated.parent.mkdir()
    generated.write_text(VALID_DOCUMENT)

    project = OssieAdapter().parse_catalog(tmp_path)

    assert project.catalog.scope_ids == ("nested/two.json::commerce", "one.yaml::commerce")


def test_validation_failure_is_not_silently_lowered_through_legacy_surface(tmp_path: Path) -> None:
    source = tmp_path / "invalid.yaml"
    source.write_text("version: 0.2.0.dev0\nsemantic_model: {}\n")

    with pytest.raises(OssieImportError) as error:
        OssieAdapter().parse(source)

    assert any(diagnostic.code == "ossie.schema.type" for diagnostic in error.value.diagnostics)


def test_ontology_only_document_reports_no_executable_scope(tmp_path: Path) -> None:
    source = tmp_path / "ontology.yaml"
    source.write_text(ONTOLOGY_DOCUMENT)

    with pytest.raises(OssieImportError, match="contains no scopes"):
        OssieAdapter().parse(source)


def test_ontology_document_canonical_serialization_preserves_ontology_data(tmp_path: Path) -> None:
    source = tmp_path / "ontology.yaml"
    output = tmp_path / "ontology.json"
    source.write_text(ONTOLOGY_DOCUMENT)
    expected = yaml.safe_load(ONTOLOGY_DOCUMENT)

    lowered = OssieAdapter(preserve_source=True).parse_document(source)
    result = OssieAdapter().export_document(lowered, output, serialization="json")

    assert not lowered.executable
    assert lowered.catalog.scope_ids == ()
    assert result.serialization.value == "json"
    assert not result.exact_source_reused
    canonical = json.loads(result.data)
    assert canonical["ontology"] == expected["ontology"]
    assert canonical["ontology_mappings"] == expected["ontology_mappings"]
    assert canonical == expected


def test_ontology_document_exact_source_round_trip_reuses_original_bytes(tmp_path: Path) -> None:
    source = tmp_path / "ontology.yaml"
    output = tmp_path / "ontology-copy.yaml"
    source_bytes = ONTOLOGY_DOCUMENT.encode()
    source.write_bytes(source_bytes)

    lowered = OssieAdapter(preserve_source=True).parse_document(source)
    result = OssieAdapter().export_document(lowered, output, exact_source=True)

    assert not lowered.executable
    assert lowered.catalog.scope_ids == ()
    assert lowered.document.ontology is not None
    assert lowered.document.ontology_mappings is not None
    assert result.exact_source_reused
    assert result.data == source_bytes
    assert output.read_bytes() == source_bytes


def test_export_document_reuses_exact_validated_source_bytes(tmp_path: Path) -> None:
    source = tmp_path / "source.yaml"
    output = tmp_path / "output.yaml"
    source.write_text("# retained\n" + VALID_DOCUMENT)
    lowered = OssieAdapter(preserve_source=True).parse_document(source)

    result = OssieAdapter().export_document(lowered, output, exact_source=True)

    assert result.exact_source_reused
    assert output.read_bytes() == source.read_bytes()


def test_dbt_alias_profile_survives_parse_lower_and_document_export(tmp_path: Path) -> None:
    source = tmp_path / "dbt.json"
    exact_output = tmp_path / "exact.json"
    canonical_output = tmp_path / "canonical.json"
    source.write_bytes(b'{"version":"0.1.0","semantic_model":[]}\n')
    adapter = OssieAdapter(consumer_profile=OssieConsumerProfile.DBT_1_12, preserve_source=True)

    lowered = adapter.parse_document(source)
    exact = adapter.export_document(lowered, exact_output, exact_source=True)
    canonical = adapter.export_document(lowered, canonical_output)

    assert lowered.valid
    assert exact.exact_source_reused
    assert exact_output.read_bytes() == source.read_bytes()
    assert yaml.safe_load(canonical.data)["version"] == "0.1.0"


def test_graph_export_requires_explicit_scope_and_dialect(tmp_path: Path) -> None:
    graph = SemanticGraph()
    graph.add_model(
        Model(
            name="orders",
            table="analytics.orders",
            dimensions=[Dimension(name="id", type="numeric", sql="id")],
        )
    )

    with pytest.raises(ValueError, match="scope_name"):
        OssieAdapter().export(graph, tmp_path / "model.yaml")

    output = tmp_path / "model.yaml"
    OssieAdapter(export_scope_name="commerce", expression_dialect="BIGQUERY").export(graph, output)
    data = yaml.safe_load(output.read_text())
    dialect = data["semantic_model"][0]["datasets"][0]["fields"][0]["expression"]["dialects"][0]
    assert dialect == {"dialect": "BIGQUERY", "expression": "id"}


def test_graph_export_refuses_unidentified_relationships(tmp_path: Path) -> None:
    graph = SemanticGraph()
    graph.add_model(Model(name="orders", table="orders"))
    graph.add_model(Model(name="customers", table="customers", primary_key="id"))
    graph.models["orders"].relationships.append(
        Relationship(name="customers", type="many_to_one", foreign_key="customer_id", primary_key="id")
    )

    with pytest.raises(OssieSynthesisError):
        OssieAdapter(export_scope_name="commerce", expression_dialect="ANSI_SQL").export(graph, tmp_path / "model.yaml")


@pytest.mark.parametrize("adapter_class", [OssieAdapter, OSIAdapter])
def test_filtered_metric_export_refusal_preserves_existing_output(tmp_path: Path, adapter_class) -> None:
    graph = SemanticGraph()
    graph.add_model(
        Model(
            name="orders",
            table="orders",
            metrics=[Metric(name="paid_revenue", agg="sum", sql="amount", filters=["status = 'paid'"])],
        )
    )
    output = tmp_path / "model.yaml"
    output.write_text("existing document\n")

    with pytest.raises(OssieSynthesisError) as error:
        adapter_class(export_scope_name="commerce", expression_dialect="ANSI_SQL").export(graph, output)

    assert any(d.code == "ossie.synthesis.metric_semantics_unsupported" for d in error.value.diagnostics)
    assert output.read_text() == "existing document\n"
