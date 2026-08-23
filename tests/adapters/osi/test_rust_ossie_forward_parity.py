from __future__ import annotations

import json
from pathlib import Path

import pytest
import yaml

from sidemantic.adapters.ossie import OssieAdapter
from sidemantic.interchange.ossie import validation
from tests.rust_layer_adapter import rust_ossie_select_scope, rust_ossie_validate

FIXTURES = Path(__file__).resolve().parents[2] / "ossie-fixtures"


def test_rust_and_python_match_shared_fixture_outcomes() -> None:
    manifest = yaml.safe_load((FIXTURES / "manifest.yaml").read_text())

    for case in manifest["cases"]:
        source = FIXTURES / case["input"]
        expected = case["expected"]
        content = source.read_text()
        document = json.loads(content) if case["serialization"] == "json" else yaml.safe_load(content)
        rust = rust_ossie_validate(content, case["serialization"])
        python = validation.validate_ossie_schema(document, profile=case["profile"])

        assert rust["valid"] is expected["valid"], case["id"]
        assert python.valid is expected["valid"], case["id"]
        rust_diagnostics = {(item["code"], item["instance_path"]) for item in rust["diagnostics"]}
        python_diagnostics = {(item.code, item.json_pointer) for item in python.diagnostics}
        for diagnostic in expected["diagnostics"]:
            key = (diagnostic["code"], diagnostic["instance_path"])
            assert key in rust_diagnostics, case["id"]
            assert key in python_diagnostics, case["id"]
        assert rust_diagnostics == python_diagnostics, case["id"]


def test_selected_scope_shape_preserves_parity_fields_and_target_selection(tmp_path: Path) -> None:
    content = """version: 0.2.0.dev0
semantic_model:
  - name: commerce
    datasets:
      - name: orders
        source: analytics.orders
        fields:
          - name: created_at
            datatype: DateTime
            dimension: {is_time: false}
            expression:
              dialects:
                - {dialect: ANSI_SQL, expression: created_at}
                - {dialect: BIGQUERY, expression: SAFE_CAST(created_at AS DATETIME)}
                - {dialect: MDX, expression: "[Orders].[Created At]"}
          - name: customer_id
            datatype: Integer
            expression: {dialects: [{dialect: ANSI_SQL, expression: customer_id}]}
      - name: customers
        source: analytics.customers
        primary_key: [id]
        fields:
          - name: id
            datatype: Integer
            expression: {dialects: [{dialect: ANSI_SQL, expression: id}]}
    relationships:
      - name: orders_customer
        from: orders
        to: customers
        from_columns: [customer_id]
        to_columns: [id]
    metrics:
      - name: gross_amount
        datatype: Decimal
        expression:
          dialects:
            - {dialect: ANSI_SQL, expression: SUM(amount)}
            - {dialect: BIGQUERY, expression: SUM(SAFE_CAST(amount AS NUMERIC))}
  - name: operations
    datasets:
      - name: orders
        source: operations.orders
"""
    source = tmp_path / "scopes.yaml"
    source.write_text(content)

    rust = rust_ossie_select_scope(content, "yaml", "commerce", target="BIGQUERY")
    python = OssieAdapter(scope_id="commerce", target_dialect="bigquery").parse_document(source)
    python_graph = python.catalog["commerce"].graph

    assert rust["scope_id"] == "commerce"
    assert [model["name"] for model in rust["models"]] == list(python_graph.models)
    assert rust["models"][0]["table"] == python_graph.get_model("orders").table
    rust_dimension = rust["models"][0]["dimensions"][0]
    python_dimension = python_graph.get_model("orders").dimensions[0]
    assert rust_dimension["sql"] == python_dimension.sql == "SAFE_CAST(created_at AS DATETIME)"
    assert rust_dimension["logical_data_type"] == python_dimension.logical_data_type == "DateTime"
    assert rust_dimension["declared_is_time"] is python_dimension.declared_is_time is False
    assert "declared_is_time" not in rust["models"][0]["dimensions"][1]
    assert rust["metrics"][0]["logical_data_type"] == "Decimal"
    assert rust["metrics"][0]["agg"] == "sum"
    assert rust["metrics"][0]["sql"] == "SAFE_CAST(amount AS NUMERIC)"
    assert rust["models"][0]["relationships"][0]["edge_id"] == "orders_customer"
    assert python_graph.get_model("orders").relationships[0].edge_id == "orders_customer"
    assert rust["models"][0]["primary_key"] == ""
    assert rust["models"][0]["primary_key_columns"] == []

    with pytest.raises(ValueError, match="ambiguous"):
        rust_ossie_select_scope(content, "yaml", target="BIGQUERY")
