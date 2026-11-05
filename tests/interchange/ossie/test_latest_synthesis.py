"""Latest-profile exports preserve native results without metric references."""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pytest
import sqlglot
from sqlglot import exp

from sidemantic import Dimension, Metric, Model, SemanticLayer
from sidemantic.core.semantic_graph import SemanticGraph
from sidemantic.interchange.ossie import (
    OssieParseOptions,
    lower_ossie_document,
    parse_ossie_document,
    synthesize_ossie_document,
)
from sidemantic.interchange.ossie.profiles import CURRENT_OSSIE_SCHEMA_COMMIT, LATEST_OSSIE_SCHEMA_COMMIT


def _graph(*metrics: Metric) -> SemanticGraph:
    graph = SemanticGraph()
    graph.add_model(
        Model(
            name="orders",
            sql="SELECT * FROM (VALUES (10, 1), (5, 2), (NULL, 3)) AS t(amount, quantity)",
            primary_key=None,
            dimensions=[Dimension(name="amount", type="numeric", sql="amount * 100")],
            metrics=list(metrics),
        )
    )
    return graph


@pytest.mark.parametrize("reference", ["total", "orders.total"])
def test_latest_export_expands_derived_dependencies_and_preserves_native_inputs(reference):
    graph = _graph(
        Metric(name="total", agg="sum", sql="amount", fill_nulls_with=0),
        Metric(name="double_total", type="derived", sql=f"{reference} * 2"),
        Metric(name="ratio", type="ratio", numerator="double_total", denominator="total"),
    )
    result = synthesize_ossie_document(graph, scope_name="sales", expression_dialect="ANSI_SQL", portable_only=True)
    assert result.valid, result.diagnostics
    assert result.document.schema_revision == LATEST_OSSIE_SCHEMA_COMMIT
    data = result.document.to_parsed_data()
    fields = {field["name"] for field in data["datasets"][0]["fields"]}
    for metric in data["metrics"]:
        tree = sqlglot.parse_one(metric["expression"]["dialects"][0]["expression"])
        assert all(column.table == "orders" and column.name in fields for column in tree.find_all(exp.Column))
    parsed = parse_ossie_document(
        json.dumps(data).encode(),
        options=OssieParseOptions(schema_revision=LATEST_OSSIE_SCHEMA_COMMIT, target_dialect="duckdb"),
    )
    lowered = lower_ossie_document(parsed)
    assert lowered.valid, lowered.diagnostics
    layer = SemanticLayer.from_catalog(lowered.catalog, engine="python", fallback=False, auto_register=False)
    try:
        assert layer.query(metrics=["total", "double_total", "ratio"]).fetchone() == (15, 30, 2)
    finally:
        layer.adapter.close()


def test_latest_export_declares_source_columns_missing_from_native_dimensions():
    result = synthesize_ossie_document(
        _graph(Metric(name="quantity_total", agg="sum", sql="quantity")),
        scope_name="sales",
        expression_dialect="ANSI_SQL",
        portable_only=True,
    )
    assert result.valid, result.diagnostics
    data = result.document.to_parsed_data()
    fields = {field["name"]: field for field in data["datasets"][0]["fields"]}
    assert fields["quantity"]["expression"]["dialects"][0]["expression"] == "quantity"
    assert data["metrics"][0]["expression"]["dialects"][0]["expression"] == "SUM(orders.quantity)"


@pytest.mark.parametrize("portable_only", [False, True])
def test_latest_export_rejects_metric_cycles_even_with_runtime_extension_enabled(portable_only):
    graph = _graph(
        Metric(name="first", type="derived", sql="second + 1"),
        Metric(name="second", type="derived", sql="first + 1"),
    )
    result = synthesize_ossie_document(
        graph, scope_name="sales", expression_dialect="ANSI_SQL", portable_only=portable_only
    )
    assert not result.valid
    assert any(
        diagnostic.code == "ossie.synthesis.metric_reference_invalid" and "Cyclic" in diagnostic.message
        for diagnostic in result.diagnostics
    )


def test_latest_export_rejects_unknown_derived_references():
    result = synthesize_ossie_document(
        _graph(Metric(name="broken", type="derived", sql="missing_metric * 2")),
        scope_name="sales",
        expression_dialect="ANSI_SQL",
        portable_only=True,
    )
    assert not result.valid
    assert result.document is None


def test_explicit_historical_export_keeps_its_metric_reference_contract():
    graph = _graph(
        Metric(name="total", agg="sum", sql="amount"),
        Metric(name="double_total", type="derived", sql="total * 2"),
    )
    result = synthesize_ossie_document(
        graph,
        scope_name="sales",
        expression_dialect="ANSI_SQL",
        schema_revision=CURRENT_OSSIE_SCHEMA_COMMIT,
        portable_only=True,
    )
    assert result.valid, result.diagnostics
    assert result.document.schema_revision == CURRENT_OSSIE_SCHEMA_COMMIT
    assert result.document.to_parsed_data()["metrics"][1]["expression"]["dialects"][0]["expression"] == "total * 2"


def test_latest_imported_dataset_metric_scopes_survive_graph_synthesis():
    data = {
        "version": "0.2.0.dev0",
        "name": "sales",
        "datasets": [
            {
                "name": name,
                "source": f"SELECT {amount} AS amount",
                "fields": [{"name": "amount", "expression": {"sql": "amount * 2"}}],
                "metrics": [
                    {"name": "total", "expression": {"sql": "SUM(amount)"}},
                    {"name": "raw", "expression": {"sql": "SUM(source.amount)"}},
                ],
            }
            for name, amount in [("orders", 10), ("returns", 3)]
        ],
    }
    options = OssieParseOptions(target_dialect="duckdb", schema_revision=LATEST_OSSIE_SCHEMA_COMMIT)
    original = lower_ossie_document(parse_ossie_document(json.dumps(data).encode(), options=options))
    assert original.valid, original.diagnostics
    graph = original.catalog["sales"].graph
    result = synthesize_ossie_document(graph, scope_name="sales", expression_dialect="ANSI_SQL", portable_only=True)
    assert result.valid, result.diagnostics
    exported = result.document.to_parsed_data()
    assert "metrics" not in exported
    for dataset in exported["datasets"]:
        assert [metric["name"] for metric in dataset["metrics"]] == ["total", "raw"]
    lowered = lower_ossie_document(parse_ossie_document(json.dumps(exported).encode(), options=options))
    assert lowered.valid, lowered.diagnostics
    layer = SemanticLayer.from_catalog(lowered.catalog, engine="python", fallback=False, auto_register=False)
    try:
        assert layer.query(metrics=["orders.total", "orders.raw"]).fetchone() == (20, 10)
        assert layer.query(metrics=["returns.total", "returns.raw"]).fetchone() == (6, 3)
    finally:
        layer.adapter.close()


def test_latest_export_preserves_quoted_physical_inputs_in_declared_fields():
    graph = SemanticGraph()
    graph.add_model(
        Model(
            name="orders",
            sql='SELECT 10 AS "Amount", 2 AS "Quantity"',
            primary_key=None,
            metrics=[Metric(name="total", agg="sum", sql='COALESCE("Amount", 0) * orders."Quantity"')],
        )
    )
    result = synthesize_ossie_document(graph, scope_name="sales", expression_dialect="ANSI_SQL", portable_only=True)
    assert result.valid, result.diagnostics
    data = result.document.to_parsed_data()
    assert {field["expression"]["dialects"][0]["expression"] for field in data["datasets"][0]["fields"]} == {
        '"Amount"',
        '"Quantity"',
    }
    lowered = lower_ossie_document(
        parse_ossie_document(
            json.dumps(data).encode(),
            options=OssieParseOptions(target_dialect="duckdb", schema_revision=LATEST_OSSIE_SCHEMA_COMMIT),
        )
    )
    assert lowered.valid, lowered.diagnostics
    layer = SemanticLayer.from_catalog(lowered.catalog, engine="python", fallback=False, auto_register=False)
    try:
        assert layer.query(metrics=["total"]).fetchone() == (20,)
    finally:
        layer.adapter.close()


def test_latest_synthesized_derived_metrics_pass_official_validator_without_reference_warnings(tmp_path):
    result = synthesize_ossie_document(
        _graph(
            Metric(name="total", agg="sum", sql="amount"),
            Metric(name="double_total", type="derived", sql="total * 2"),
        ),
        scope_name="sales",
        expression_dialect="ANSI_SQL",
        portable_only=True,
    )
    assert result.valid, result.diagnostics
    document_path = tmp_path / "export.json"
    document_path.write_text(json.dumps(result.document.to_parsed_data()))
    upstream = Path(__file__).parents[2] / "ossie-fixtures" / "upstream" / "172a3e5"
    completed = subprocess.run(
        [sys.executable, str(upstream / "validation" / "validate.py"), str(document_path)],
        capture_output=True,
        text=True,
        check=False,
    )
    assert completed.returncode == 0, completed.stdout + completed.stderr
    assert "Validation PASSED" in completed.stdout
    # The upstream validator may warn that a generated pass-through deliberately
    # reads the physical column shadowed by the calculated dimension. It must
    # not warn about any metric referring to another metric or undeclared field.
    assert "[Reference] Warning: Metric" not in completed.stdout


@pytest.mark.parametrize("location", ["graph", "model"])
@pytest.mark.parametrize(
    "metadata",
    [
        {"ossie_schema_revision": "unknown"},
        {"ossie_schema_revision": {"not": "a revision"}},
        {"ossie_schema_revision": LATEST_OSSIE_SCHEMA_COMMIT, "user_state": True},
    ],
)
def test_source_revision_whitelist_does_not_discard_other_metadata(location, metadata):
    graph = _graph(Metric(name="total", agg="sum", sql="amount"))
    target = graph if location == "graph" else graph.models["orders"]
    target.metadata = metadata
    result = synthesize_ossie_document(graph, scope_name="sales", expression_dialect="ANSI_SQL", portable_only=True)
    assert not result.valid
    assert any(diagnostic.code == "ossie.synthesis.native_state_unrepresented" for diagnostic in result.diagnostics)


@pytest.mark.parametrize("index", [-1, True, "0", {"value": 0}])
def test_metric_declaration_index_provenance_rejects_invalid_values(index):
    graph = _graph(Metric(name="total", agg="sum", sql="amount", metadata={"ossie_dataset_metric_index": index}))
    result = synthesize_ossie_document(graph, scope_name="sales", expression_dialect="ANSI_SQL", portable_only=True)
    assert not result.valid
    assert any(diagnostic.code == "ossie.synthesis.native_state_unrepresented" for diagnostic in result.diagnostics)
