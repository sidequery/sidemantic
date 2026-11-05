"""Compiler-boundary checks complementing the shared wide-table oracle corpus."""

import json

import duckdb
import pytest
import sqlglot

from sidemantic.interchange.ossie import OssieParseOptions, lower_ossie_document, parse_ossie_document
from sidemantic.sql.ossie_wide import OssieWideQueryError, compile_ossie_wide_query
from tests.interchange.ossie.test_latest_logical import document, field, lower


def graph(data):
    lowered = lower(data)
    assert lowered.valid, lowered.diagnostics
    return lowered.catalog.scopes["sales"].graph


def execute(graph, query):
    sql = compile_ossie_wide_query(graph, query)
    with duckdb.connect() as connection:
        connection.execute("CREATE TABLE orders(amount INT); INSERT INTO orders VALUES (10),(20),(NULL)")
        return connection.execute(sql).fetchall()


def test_quoted_identity_is_case_sensitive_and_output_preserves_query_spelling():
    data = document()
    dataset = data["datasets"][0]
    dataset["name"] = '"Orders"'
    dataset["fields"] = [field('"Amount"', "amount * 2")]
    dataset["metrics"] = [field('"Total"', 'SUM("Amount")')]
    semantic_graph = graph(data)
    assert execute(semantic_graph, {"metrics": ['"Orders"."Total"']}) == [(60,)]
    with pytest.raises(OssieWideQueryError, match="E_NAME_NOT_FOUND"):
        compile_ossie_wide_query(semantic_graph, {"metrics": ["orders.total"]})
    with pytest.raises(OssieWideQueryError, match="E_NAME_COLLISION"):
        compile_ossie_wide_query(
            semantic_graph,
            {
                "metrics": [
                    {"metric": '"Orders"."Total"', "alias": "total"},
                    {"metric": '"Orders"."Total"', "alias": '"TOTAL"'},
                ]
            },
        )


def test_count_only_does_not_bind_unrelated_declared_field_expression():
    data = document()
    data["datasets"][0]["fields"] = [field("missing", "absent_physical_column")]
    data["datasets"][0]["metrics"] = [field("count", "COUNT(*)")]
    assert execute(graph(data), {"metrics": ["orders.count"]}) == [(3,)]


@pytest.mark.parametrize("predicate", ["1", "'yes'", "1+2", "DATE '2026-01-01'"])
def test_provably_nonboolean_filter_rejected_before_name_resolution(predicate):
    with pytest.raises(OssieWideQueryError, match="E_INVALID_QUERY"):
        compile_ossie_wide_query(graph(document()), {"metrics": ["missing"], "filter": predicate})


def test_model_ordered_set_arguments_determine_home_in_within_group_dialect():
    data = document()
    data["metrics"] = [field("median_amount", "PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY orders.amount)")]
    parsed = parse_ossie_document(
        json.dumps(data).encode(), options=OssieParseOptions(validate_schema=True, target_dialect="snowflake")
    )
    lowered = lower_ossie_document(parsed)
    assert lowered.valid, lowered.diagnostics
    sql = compile_ossie_wide_query(lowered.catalog.scopes["sales"].graph, {"metrics": ["median_amount"]}, "snowflake")
    with duckdb.connect() as connection:
        connection.execute("CREATE TABLE orders(amount INT); INSERT INTO orders VALUES (10),(20),(NULL)")
        assert connection.execute(sqlglot.transpile(sql, read="snowflake", write="duckdb")[0]).fetchall() == [(30.0,)]


@pytest.mark.parametrize("reference", ["orders.count; SELECT 1", "x" * 129])
def test_reference_must_be_exactly_one_valid_length_identifier(reference):
    with pytest.raises(OssieWideQueryError, match="E_INVALID_QUERY"):
        compile_ossie_wide_query(graph(document()), {"metrics": [reference]})


@pytest.mark.parametrize(
    "expression", ["SUM(amount) OVER ()", "SUM(amount) + amount", "SUM(SUM(amount))", "amount", "1"]
)
def test_nonaggregate_dataset_measure_retains_source_but_cannot_execute(expression):
    data = document()
    data["datasets"][0]["metrics"] = [field("invalid_measure", expression)]
    semantic_graph = graph(data)
    with pytest.raises(OssieWideQueryError, match="E_INVALID_QUERY"):
        compile_ossie_wide_query(semantic_graph, {"metrics": ["orders.invalid_measure"]})


def test_strict_binding_does_not_reinterpret_native_graph():
    from sidemantic.core.semantic_graph import SemanticGraph

    with pytest.raises(OssieWideQueryError, match="E_INVALID_QUERY"):
        compile_ossie_wide_query(SemanticGraph(), {"metrics": ["x"]})


@pytest.mark.parametrize("index_location", ["field", "metric"])
def test_cross_output_dialect_preserves_stored_array_index_and_metric_inputs(index_location):
    from sidemantic import SemanticLayer

    data = document()
    dataset = data["datasets"][0]
    dataset["source"] = "SELECT [10, 20] AS amounts"
    dataset["fields"] = [
        {"name": "first_amount", "expression": {"dialects": [{"dialect": "ANSI_SQL", "expression": "amounts[1]"}]}}
    ]
    dataset["metrics"] = [field("first_sum", "SUM(first_amount)")]
    if index_location == "metric":
        dataset["fields"] = [field("amounts", "amounts")]
        dataset["metrics"] = [
            {
                "name": "first_sum",
                "expression": {"dialects": [{"dialect": "ANSI_SQL", "expression": "SUM(amounts[1])"}]},
            }
        ]
    lowered = lower(data)
    assert lowered.valid, lowered.diagnostics
    layer = SemanticLayer.from_catalog(lowered.catalog, engine="python", fallback=False)
    try:
        sql = layer.compile_ossie_wide({"metrics": ["orders.first_sum"]}, dialect="snowflake")
        assert "[0]" in sql
        duckdb_sql = sqlglot.transpile(sql, read="snowflake", write="duckdb")[0]
        assert layer.adapter.execute(duckdb_sql).fetchall() == [(10,)]
    finally:
        layer.adapter.close()


def test_model_nested_aggregate_retains_source_but_rejects_invalid_measure():
    data = document()
    data["metrics"] = [field("nested", "SUM(SUM(orders.amount))")]
    with pytest.raises(OssieWideQueryError, match="E_INVALID_QUERY"):
        compile_ossie_wide_query(graph(data), {"metrics": ["nested"]})
