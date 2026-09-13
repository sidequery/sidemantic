from __future__ import annotations

import json
from pathlib import Path

import pytest

from sidemantic.core.semantic_layer import SemanticLayer
from sidemantic.interchange.ossie import (
    OssieImportPolicy,
    OssieParseOptions,
    lower_ossie_document,
    parse_ossie_document,
)


def _parse(text: str, *, policy: OssieImportPolicy = OssieImportPolicy.STRICT):
    return parse_ossie_document(
        text.encode(),
        source_identifier="model.ossie.yaml",
        options=OssieParseOptions(import_policy=policy, validate_schema=True),
    )


@pytest.mark.parametrize("key_kind", ["primary_key", "unique_keys"])
def test_reordered_composite_unique_key_preserves_join_pairs(key_kind: str) -> None:
    source = json.loads(
        (Path(__file__).parents[2] / "ossie-fixtures/cases/logical-composite-key-reordered.json").read_text()
    )
    target = source["semantic_model"][0]["datasets"][1]
    if key_kind == "unique_keys":
        target["unique_keys"] = [target.pop("primary_key")]
    lowered = lower_ossie_document(_parse(json.dumps(source)), target_dialect="duckdb")

    assert lowered.valid, lowered.diagnostics
    relationship = lowered.catalog["commerce"].graph.get_model("orders").relationships[0]
    assert relationship.foreign_key_columns == ["customer_id", "tenant_id"]
    assert relationship.primary_key_columns == ["id", "tenant_id"]
    layer = SemanticLayer.from_catalog(lowered.catalog, auto_register=False)
    assert layer.query(dimensions=["orders.customer_id", "customers.label"]).fetchall() == [(10, "matched")]


def test_vendor_expression_alternatives_preserved_while_ansi_sql_is_selected() -> None:
    source = (
        Path(__file__).parents[2] / "ossie-fixtures/cases/logical-0.2-current-dialects-vendors/document.json"
    ).read_text()
    parsed = _parse(source)
    lowered = lower_ossie_document(parsed, target_dialect="duckdb")

    assert lowered.valid, lowered.diagnostics
    assert lowered.catalog["commerce"].graph.get_model("orders").get_dimension("amount").sql == "amount"
    assert lowered.document.to_parsed_data() == json.loads(source)


def test_lowers_multiple_scopes_without_flattening_duplicate_model_names() -> None:
    parsed = _parse(
        """version: 0.2.0.dev0
semantic_model:
  - name: finance
    datasets:
      - name: orders
        source: finance.orders
        fields:
          - name: id
            expression: {dialects: [{dialect: ANSI_SQL, expression: id}]}
  - name: marketing
    datasets:
      - name: orders
        source: marketing.orders
        fields:
          - name: id
            expression: {dialects: [{dialect: ANSI_SQL, expression: id}]}
"""
    )

    lowered = lower_ossie_document(parsed, target_dialect="duckdb")

    assert lowered.catalog.scope_ids == ("finance", "marketing")
    assert lowered.catalog["finance"].graph.get_model("orders").table == "finance.orders"
    assert lowered.catalog["marketing"].graph.get_model("orders").table == "marketing.orders"


def test_classifies_query_source_and_preserves_unknown_primary_key() -> None:
    parsed = _parse(
        """version: 0.2.0.dev0
semantic_model:
  - name: finance
    datasets:
      - name: orders
        source: WITH recent AS (SELECT * FROM raw.orders) SELECT * FROM recent
        fields:
          - name: id
            expression: {dialects: [{dialect: ANSI_SQL, expression: id}]}
"""
    )

    model = lower_ossie_document(parsed, target_dialect="duckdb").catalog["finance"].graph.get_model("orders")

    assert model.table is None
    assert model.sql.startswith("WITH recent")
    assert model.primary_key is None
    assert model.default_time_dimension is None


@pytest.mark.parametrize(
    ("source", "expected_kind"),
    [
        ("analytics.orders", "table"),
        ('"analytics"."orders"', "table"),
        ("SELECT * FROM raw.orders", "query"),
        ("(SELECT * FROM raw.orders)", "query"),
        ("WITH recent AS (SELECT * FROM raw.orders) SELECT * FROM recent", "query"),
    ],
)
def test_source_kind_matrix_is_classified_without_rewriting(source: str, expected_kind: str) -> None:
    parsed = _parse(
        f"""version: 0.2.0.dev0
semantic_model:
  - name: scope
    datasets:
      - name: orders
        source: '{source}'
"""
    )

    model = lower_ossie_document(parsed, target_dialect="duckdb").catalog["scope"].graph.get_model("orders")

    assert model.metadata["ossie_source_kind"] == expected_kind
    assert (model.table or model.sql) == source


def test_query_source_is_executable_as_a_derived_table() -> None:
    parsed = _parse(
        """version: 0.2.0.dev0
semantic_model:
  - name: scope
    datasets:
      - name: rows
        source: SELECT 1 AS id UNION ALL SELECT 2 AS id
        fields:
          - name: id
            datatype: Integer
            expression: {dialects: [{dialect: ANSI_SQL, expression: id}]}
"""
    )
    lowered = lower_ossie_document(parsed, target_dialect="duckdb")

    layer = SemanticLayer.from_catalog(lowered.catalog, auto_register=False)

    assert layer.query(dimensions=["rows.id"]).fetchall() == [(1,), (2,)]


def test_preserves_datatype_declared_time_and_derived_time_role_without_day_granularity() -> None:
    parsed = _parse(
        """version: 0.2.0.dev0
semantic_model:
  - name: finance
    datasets:
      - name: events
        source: analytics.events
        fields:
          - name: occurred_at
            datatype: DateTime
            expression: {dialects: [{dialect: ANSI_SQL, expression: occurred_at}]}
          - name: loaded_at
            datatype: DateTime
            dimension: {is_time: false}
            expression: {dialects: [{dialect: ANSI_SQL, expression: loaded_at}]}
          - name: fiscal_year
            datatype: Integer
            dimension: {is_time: true}
            expression: {dialects: [{dialect: ANSI_SQL, expression: fiscal_year}]}
"""
    )

    model = lower_ossie_document(parsed, target_dialect="duckdb").catalog["finance"].graph.get_model("events")
    occurred = model.get_dimension("occurred_at")
    loaded = model.get_dimension("loaded_at")
    fiscal = model.get_dimension("fiscal_year")

    assert (occurred.type, occurred.logical_data_type, occurred.declared_is_time, occurred.granularity) == (
        "time",
        "DateTime",
        None,
        None,
    )
    assert (loaded.type, loaded.declared_is_time) == ("categorical", False)
    assert (fiscal.type, fiscal.logical_data_type, fiscal.declared_is_time) == ("time", "Integer", True)


def test_selects_exact_bigquery_expression_before_ansi_fallback() -> None:
    parsed = _parse(
        """version: 0.2.0.dev0
semantic_model:
  - name: warehouse
    datasets:
      - name: events
        source: analytics.events
        fields:
          - name: occurred_on
            expression:
              dialects:
                - {dialect: ANSI_SQL, expression: CAST(occurred_at AS DATE)}
                - {dialect: BIGQUERY, expression: DATE(occurred_at)}
"""
    )

    graph = lower_ossie_document(parsed, target_dialect="bigquery").catalog["warehouse"].graph
    assert graph.get_model("events").get_dimension("occurred_on").sql == "DATE(occurred_at)"


def test_lowers_named_relationship_without_fabricating_keys() -> None:
    parsed = _parse(
        """version: 0.2.0.dev0
semantic_model:
  - name: commerce
    datasets:
      - name: orders
        source: analytics.orders
        primary_key: [id]
        fields:
          - {name: id, expression: {dialects: [{dialect: ANSI_SQL, expression: id}]}}
          - {name: customer_id, expression: {dialects: [{dialect: ANSI_SQL, expression: customer_id}]}}
      - name: customers
        source: analytics.customers
        primary_key: [id]
        fields:
          - {name: id, expression: {dialects: [{dialect: ANSI_SQL, expression: id}]}}
    relationships:
      - name: order_customer
        from: orders
        to: customers
        from_columns: [customer_id]
        to_columns: [id]
"""
    )

    graph = lower_ossie_document(parsed, target_dialect="duckdb").catalog["commerce"].graph
    relationship = graph.get_model("orders").relationships[0]
    path = graph.find_relationship_path("orders", "customers")

    assert relationship.edge_id == "order_customer"
    assert relationship.foreign_key == "customer_id"
    assert relationship.primary_key == "id"
    assert path[0].edge_id == "order_customer"


def test_case_varied_regular_references_bind_to_canonical_runtime_names() -> None:
    source = {
        "version": "0.2.0.dev0",
        "semantic_model": [
            {
                "name": "Commerce",
                "datasets": [
                    {
                        "name": "Orders",
                        "source": "analytics.orders",
                        "primary_key": ["iD"],
                        "fields": [
                            {"name": "Id", "expression": {"dialects": [{"dialect": "ANSI_SQL", "expression": "id"}]}},
                            {
                                "name": "Customer_Id",
                                "expression": {"dialects": [{"dialect": "ANSI_SQL", "expression": "customer_id"}]},
                            },
                        ],
                    },
                    {
                        "name": "Customers",
                        "source": "analytics.customers",
                        "primary_key": ["id"],
                        "unique_keys": [["ID"]],
                        "fields": [
                            {"name": "ID", "expression": {"dialects": [{"dialect": "ANSI_SQL", "expression": "id"}]}}
                        ],
                    },
                ],
                "relationships": [
                    {
                        "name": "Order_Customer",
                        "from": "ORDERS",
                        "to": "customers",
                        "from_columns": ["CUSTOMER_ID"],
                        "to_columns": ["Id"],
                    }
                ],
            }
        ],
    }
    original = json.loads(json.dumps(source))
    parsed = parse_ossie_document(
        json.dumps(source).encode(),
        source_identifier="normalized.ossie.json",
        options=OssieParseOptions(import_policy=OssieImportPolicy.STRICT, validate_schema=True),
    )

    lowered = lower_ossie_document(parsed, target_dialect="duckdb")
    graph = lowered.catalog["Commerce"].graph
    orders = graph.get_model("Orders")
    relationship = orders.relationships[0]

    assert lowered.valid
    assert orders.primary_key == "Id"
    assert graph.get_model("Customers").primary_key == "ID"
    assert graph.get_model("Customers").unique_keys == [["ID"]]
    assert relationship.name == "Customers"
    assert relationship.foreign_key == "Customer_Id"
    assert relationship.primary_key == "ID"
    assert parsed.document.to_parsed_data() == original


def test_quoted_lowercase_reference_does_not_bind_to_regular_lowercase_declaration() -> None:
    parsed = _parse(
        """version: 0.2.0.dev0
semantic_model:
  - name: commerce
    datasets:
      - name: orders
        source: analytics.orders
        fields:
          - {name: customer_id, expression: {dialects: [{dialect: ANSI_SQL, expression: customer_id}]}}
      - name: customers
        source: analytics.customers
        primary_key: [id]
        fields:
          - {name: id, expression: {dialects: [{dialect: ANSI_SQL, expression: id}]}}
    relationships:
      - {name: bad_case, from: orders, to: '"customers"', from_columns: [customer_id], to_columns: ['"id"']}
""",
        policy=OssieImportPolicy.PERMISSIVE,
    )

    lowered = lower_ossie_document(parsed, target_dialect="duckdb")

    assert lowered.executable
    assert lowered.catalog["commerce"].graph.get_model("orders").relationships == []
    assert any(
        diagnostic.code == "ossie.semantic.relationship.to_dataset_unknown" for diagnostic in lowered.diagnostics
    )


@pytest.mark.parametrize("policy", [OssieImportPolicy.STRICT, OssieImportPolicy.PERMISSIVE])
def test_overlong_identifier_is_never_lowered(policy: OssieImportPolicy) -> None:
    too_long = "x" * 129
    parsed = parse_ossie_document(
        json.dumps(
            {
                "version": "0.2.0.dev0",
                "semantic_model": [
                    {
                        "name": "commerce",
                        "datasets": [{"name": too_long, "source": "analytics.rows", "fields": []}],
                    }
                ],
            }
        ).encode(),
        options=OssieParseOptions(import_policy=policy, validate_schema=True),
    )

    lowered = lower_ossie_document(parsed, target_dialect="duckdb")

    assert any(diagnostic.code == "ossie.semantic.identifier.length_exceeded" for diagnostic in lowered.diagnostics)
    if policy is OssieImportPolicy.STRICT:
        assert not lowered.executable
    else:
        assert lowered.catalog["commerce"].graph.models == {}


def test_strict_validation_errors_block_all_executable_scopes() -> None:
    parsed = _parse(
        """version: 0.2.0.dev0
semantic_model:
  - name: commerce
    datasets:
      - name: orders
        source: analytics.orders
        fields: []
    relationships:
      - {name: unsafe, from: orders, to: missing, from_columns: [], to_columns: []}
"""
    )

    lowered = lower_ossie_document(parsed, target_dialect="duckdb")

    assert not lowered.executable
    assert len(lowered.catalog) == 0
    assert any(diagnostic.code.startswith("ossie.semantic.relationship") for diagnostic in lowered.diagnostics)


def test_lowering_runs_schema_validation_when_parser_did_not_request_it() -> None:
    parsed = parse_ossie_document(
        b'{"version":"0.2.0.dev0","semantic_model":{}}',
        options=OssieParseOptions(import_policy=OssieImportPolicy.STRICT, validate_schema=False),
    )

    lowered = lower_ossie_document(parsed, target_dialect="duckdb")

    assert not lowered.executable
    assert lowered.schema_validation is not None
    assert any(diagnostic.code == "ossie.schema.type" for diagnostic in lowered.diagnostics)


def test_permissive_policy_preserves_invalid_relationship_but_excludes_its_edge() -> None:
    parsed = _parse(
        """version: 0.2.0.dev0
semantic_model:
  - name: commerce
    datasets:
      - name: orders
        source: analytics.orders
        fields:
          - {name: id, expression: {dialects: [{dialect: ANSI_SQL, expression: id}]}}
    relationships:
      - {name: unsafe, from: orders, to: missing, from_columns: [], to_columns: []}
""",
        policy=OssieImportPolicy.PERMISSIVE,
    )

    lowered = lower_ossie_document(parsed, target_dialect="duckdb")

    assert lowered.executable
    assert lowered.catalog["commerce"].graph.get_model("orders").relationships == []
    assert any(diagnostic.code == "ossie.lowering.relationship_unsafe" for diagnostic in lowered.diagnostics)


def test_permissive_policy_never_passes_malformed_unique_keys_to_runtime_models() -> None:
    parsed = _parse(
        """version: 0.2.0.dev0
semantic_model:
  - name: commerce
    datasets:
      - name: orders
        source: analytics.orders
        unique_keys: [[id], [42]]
        fields:
          - {name: id, expression: {dialects: [{dialect: ANSI_SQL, expression: id}]}}
""",
        policy=OssieImportPolicy.PERMISSIVE,
    )

    lowered = lower_ossie_document(parsed, target_dialect="duckdb")

    assert lowered.executable
    scope = lowered.catalog["commerce"]
    assert scope.graph.get_model("orders").unique_keys == [["id"]]
    assert not scope.valid
    assert any(diagnostic.code.startswith("ossie.schema") for diagnostic in scope.diagnostics)
    assert any(diagnostic.code.startswith("ossie.schema") for diagnostic in lowered.diagnostics)


def test_permissive_policy_diagnoses_and_drops_unresolvable_declared_keys() -> None:
    parsed = _parse(
        """version: 0.2.0.dev0
semantic_model:
  - name: commerce
    datasets:
      - name: orders
        source: analytics.orders
        primary_key: [missing]
        unique_keys: [[id, ID]]
        fields:
          - {name: id, expression: {dialects: [{dialect: ANSI_SQL, expression: id}]}}
""",
        policy=OssieImportPolicy.PERMISSIVE,
    )

    lowered = lower_ossie_document(parsed, target_dialect="duckdb")
    model = lowered.catalog["commerce"].graph.get_model("orders")

    assert lowered.executable
    assert not lowered.valid
    assert model.primary_key is None
    assert model.unique_keys is None
    assert {diagnostic.code for diagnostic in lowered.diagnostics} >= {
        "ossie.semantic.dataset.key_field_unknown",
        "ossie.semantic.dataset.key_column_duplicate",
    }


def test_ontology_is_preserved_but_not_projected_as_an_executable_graph() -> None:
    parsed = _parse(
        """version: 0.2.0.dev0
name: business
ontology: []
"""
    )

    lowered = lower_ossie_document(parsed, target_dialect="duckdb")

    assert lowered.document is parsed.document
    assert not lowered.executable
    assert len(lowered.catalog) == 0


def test_target_dialect_is_required_for_executable_lowering() -> None:
    parsed = _parse(
        """version: 0.2.0.dev0
semantic_model:
  - name: empty
    datasets: []
"""
    )

    lowered = lower_ossie_document(parsed)

    assert not lowered.executable
    assert lowered.diagnostics[-1].code == "ossie.lowering.target_dialect_required"


def test_source_dialect_partitions_compilation_identity() -> None:
    source = b"""version: 0.2.0.dev0
semantic_model:
  - name: scope
    datasets:
      - name: t
        source: a-b
"""
    default = parse_ossie_document(
        source,
        options=OssieParseOptions(import_policy=OssieImportPolicy.PERMISSIVE, validate_schema=True),
    )
    bigquery = parse_ossie_document(
        source,
        options=OssieParseOptions(
            import_policy=OssieImportPolicy.PERMISSIVE,
            validate_schema=True,
            source_dialect="bigquery",
        ),
    )

    default_scope = lower_ossie_document(default, target_dialect="duckdb").catalog["scope"]
    bigquery_scope = lower_ossie_document(bigquery, target_dialect="duckdb").catalog["scope"]

    assert default_scope.content_id == bigquery_scope.content_id
    assert default_scope.compilation_id != bigquery_scope.compilation_id
    assert default_scope.cache_key != bigquery_scope.cache_key


def test_strict_lowering_rejects_malformed_selected_sql_expression() -> None:
    parsed = _parse(
        """version: 0.2.0.dev0
semantic_model:
  - name: commerce
    datasets:
      - name: orders
        source: analytics.orders
        fields:
          - name: broken
            expression: {dialects: [{dialect: ANSI_SQL, expression: "not ("}]}
"""
    )

    lowered = lower_ossie_document(parsed, target_dialect="duckdb")

    assert not lowered.executable
    assert not lowered.valid
    assert any(diagnostic.code == "ossie.lowering.expression_invalid" for diagnostic in lowered.diagnostics)


def test_permissive_lowering_rejects_multiple_statements_but_keeps_safe_scope() -> None:
    parsed = _parse(
        """version: 0.2.0.dev0
semantic_model:
  - name: commerce
    datasets:
      - name: orders
        source: analytics.orders
        fields:
          - name: unsafe
            expression: {dialects: [{dialect: ANSI_SQL, expression: "id; DROP TABLE orders"}]}
          - name: safe
            expression: {dialects: [{dialect: ANSI_SQL, expression: id}]}
""",
        policy=OssieImportPolicy.PERMISSIVE,
    )

    lowered = lower_ossie_document(parsed, target_dialect="duckdb")

    assert lowered.executable
    model = lowered.catalog["commerce"].graph.get_model("orders")
    assert [dimension.name for dimension in model.dimensions] == ["safe"]
    assert any(diagnostic.code == "ossie.lowering.expression_invalid" for diagnostic in lowered.diagnostics)
